import { domainDigest } from '../server/events/domain-events.ts';
import { parseRelationalFact } from '../server/events/relational-fact-contracts.ts';
import { readableResultFile } from '../server/storage/result-files.ts';

function conflict() {
  throw new Error('Legacy artifact metadata contradicts its proven source relationships.');
}

function date(value) {
  // 与唯一离线 CLI 同用原保存 ms/日期字符串；未知不借 header/当前时间补齐。
  if (value === null) {
    return null;
  }
  let timestamp = value;
  if (typeof value === 'string' && value.trim()) {
    timestamp = Date.parse(value);
  }
  // 不能把已存但不可解释的 removedAt 转成 NULL，进而复活原删除状态。
  if (!Number.isSafeInteger(timestamp) || timestamp < 0) {
    conflict();
  }
  return timestamp;
}

// 唯一离线 CLI 已核只读 source 与 audit ExportProof；这里只消费其描述符，
// 不重读旧正文/凭据、追加提交或做 IO。findings 中的原 ID 只能留在私有审计产物。
export function mapLegacyArtifacts(_source, refs, metadata) {
  const facts = [],
    findings = [],
    targets = new Map(),
    emittedTargets = new Set(),
    artifacts = new Set(),
    materializations = metadata.localMaterializationsByArtifact ?? new Map(),
    materialized = new Set();
  if (!(materializations instanceof Map)) {
    conflict();
  }
  const finding = (originalId, code) => findings.push({ originalId, code });
  const fact = (eventType, payload, memberId, accountId, originalId) => {
    const parsed = parseRelationalFact({ eventType, payload });
    facts.push({
      ...parsed,
      eventId: refs.eventId(eventType, originalId),
      memberId,
      accountId,
    });
  };
  for (const target of metadata.storageTargets ?? []) {
    if (targets.has(target.kind) || typeof target.originalId !== 'string') {
      conflict();
    }
    targets.set(target.kind, {
      storageTargetId: `${target.kind}:${domainDigest(target.namespace)}`,
      kind: target.kind,
      namespace: target.namespace,
      credentialRef: target.kind === 'local' ? null : `deployment:${target.kind}`,
      origin: refs.origin(target.originalId),
    });
  }
  const replica = (artifact, kind, location, originalId = artifact.artifactId) => {
    const target = targets.get(kind);
    if (!target) {
      finding(artifact.artifactId, `${kind.toUpperCase()}_NAMESPACE_UNPROVEN`);
      return;
    }
    if (!emittedTargets.has(target.storageTargetId)) {
      fact('OpeningStorageTargetRecorded', target, null, null, target.storageTargetId);
      emittedTargets.add(target.storageTargetId);
    }
    fact(
      'OpeningReplicaRecorded',
      {
        artifactId: artifact.artifactId,
        storageTargetId: target.storageTargetId,
        ...location,
        lastErrorCode: null,
        origin: refs.origin(originalId),
      },
      artifact.memberId,
      null,
      domainDigest([artifact.artifactId, target.storageTargetId]),
    );
  };
  for (const original of metadata.artifacts) {
    if (artifacts.has(original.id)) {
      conflict();
    }
    artifacts.add(original.id);
    const input = refs.inputsByArtifact.get(original.id);
    if (!input || !refs.memberIds.has(input.memberId)) {
      finding(original.id, 'ARTIFACT_INPUT_OWNER_UNPROVEN');
      continue;
    }
    if (
      (original.memberId !== null && original.memberId !== input.memberId) ||
      (original.operation !== null &&
        input.operation != null &&
        original.operation !== input.operation)
    ) {
      conflict();
    }
    if (original.mimeType === null || original.byteLength === null || original.sha256 === null) {
      finding(original.id, 'ARTIFACT_DESCRIPTOR_INCOMPLETE');
      continue;
    }
    const attempt = refs.artifactAttemptById?.get(original.id);
    if (
      attempt &&
      (!refs.attemptIds.has(attempt.attemptId) ||
        !refs.accountIds.has(attempt.accountId) ||
        attempt.memberId !== input.memberId ||
        attempt.inputId !== input.inputId ||
        attempt.artifactId !== original.id)
    ) {
      conflict();
    }
    const history = refs.artifactHistoryById?.get(original.id);
    if (
      history &&
      (history.memberId !== input.memberId ||
        history.inputId !== input.inputId ||
        history.sha256 !== original.sha256 ||
        history.byteLength !== original.byteLength)
    ) {
      conflict();
    }
    const artifact = {
      artifactId: original.id,
      memberId: input.memberId,
      inputId: input.inputId,
      originalAttemptId: attempt?.attemptId ?? null,
      operation: original.operation ?? input.operation ?? null,
      mimeType: original.mimeType,
      byteLength: original.byteLength,
      sha256: original.sha256,
      // image.createdAt 不是原 publication 决定；只有独立完整来源可提供该时点。
      publishedAt: history?.publishedAt ?? null,
      retiredAt: history?.retiredAt ?? null,
      origin: refs.origin(original.id),
    };
    let canonicalFile;
    try {
      canonicalFile = readableResultFile({ imageId: original.id, ...artifact });
    } catch {
      finding(original.id, 'ARTIFACT_ONLINE_DESCRIPTOR_UNSUPPORTED');
      continue;
    }
    let materialization = null;
    if (materializations.has(original.id)) {
      materialization = materializations.get(original.id);
      const fields = [
        'artifactId',
        'memberId',
        'inputId',
        'sha256',
        'byteLength',
        'file',
        'verifiedAt',
        'proofRef',
      ];
      if (
        !materialization ||
        typeof materialization !== 'object' ||
        ![Object.prototype, null].includes(Object.getPrototypeOf(materialization)) ||
        Object.keys(materialization).length !== fields.length ||
        fields.some((field) => !Object.hasOwn(materialization, field)) ||
        materialization.artifactId !== original.id ||
        materialization.memberId !== input.memberId ||
        materialization.inputId !== input.inputId ||
        materialization.sha256 !== original.sha256 ||
        materialization.byteLength !== original.byteLength ||
        materialization.file !== canonicalFile ||
        !Number.isSafeInteger(materialization.verifiedAt) ||
        materialization.verifiedAt < 0 ||
        typeof materialization.proofRef !== 'string' ||
        !materialization.proofRef ||
        !(refs.boundLocalMaterializationProofs instanceof Map) ||
        refs.boundLocalMaterializationProofs.get(materialization.proofRef) !==
          domainDigest(materialization) ||
        targets.get('local')?.namespace !== 'images'
      ) {
        conflict();
      }
      materialized.add(original.id);
    }
    fact(
      'OpeningArtifactRecorded',
      artifact,
      input.memberId,
      attempt?.accountId ?? null,
      original.id,
    );
    if (materialization) {
      // 这是 B 新目标复制并实读后的当前 baseline；不先导入旧 tombstone 再复活。
      // 原 Local 删除来源仍在 archive，产物 publishedAt/retiredAt 不随复制改变。
      replica(
        artifact,
        'local',
        {
          objectKey: materialization.file.slice('images/'.length),
          encoding: 'identity',
          uploadedAt: null,
          verifiedAt: materialization.verifiedAt,
          deletedAt: null,
        },
        materialization.proofRef,
      );
    } else if (original.file !== null) {
      if (!original.file.startsWith('images/') || original.file === 'images/') {
        finding(original.id, 'LOCAL_LOCATOR_UNPROVEN');
      } else {
        if (original.file !== canonicalFile) {
          finding(original.id, 'LOCAL_LOCATOR_NOT_CANONICAL');
        }
        const deletedAt = original.r2 === null ? null : date(original.r2.localRemovedAt);
        replica(artifact, 'local', {
          objectKey: original.file.slice('images/'.length),
          encoding: 'identity',
          uploadedAt: null,
          verifiedAt: null,
          deletedAt,
        });
        if (deletedAt === null) {
          finding(original.id, 'LOCAL_BYTES_NOT_VERIFIED');
        }
      }
    }
    if (original.r2 !== null) {
      const r2 = original.r2;
      if (r2.sha256 !== original.sha256 || r2.byteLength !== original.byteLength) {
        conflict();
      }
      const target = targets.get('r2');
      if (target && new URL(target.namespace).pathname !== '/' + r2.bucket) {
        conflict();
      }
      replica(artifact, 'r2', {
        objectKey: r2.key,
        // 固定旧 owner 的 image R2 object 保存原字节；gzip 只属于其备份容器。
        encoding: 'identity',
        uploadedAt: null,
        verifiedAt: date(r2.verifiedAt),
        deletedAt: date(r2.removedAt),
      });
    }
    if (original.pikpak !== null) {
      const pik = original.pikpak,
        target = targets.get('pikpak');
      if (original.r2 === null || pik.encoding !== 'brotli') {
        conflict();
      }
      if (target && target.namespace !== pik.url) {
        conflict();
      }
      replica(artifact, 'pikpak', {
        objectKey: original.sha256 + '.br',
        encoding: 'br',
        uploadedAt: date(pik.uploadedAt),
        verifiedAt: null,
        deletedAt: null,
      });
    }
  }
  if (materialized.size !== materializations.size) {
    conflict();
  }
  return {
    facts,
    findings,
    expected: {
      artifacts: facts.filter((entry) => entry.eventType === 'OpeningArtifactRecorded').length,
      storageTargets: emittedTargets.size,
      replicas: facts.filter((entry) => entry.eventType === 'OpeningReplicaRecorded').length,
    },
  };
}
