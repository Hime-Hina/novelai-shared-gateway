import path from 'node:path';
import type Database from 'better-sqlite3';
import {
  assertRetainedArtifactReleasePermit,
  type RetainedArtifactReleaseClaim,
  type RetainedArtifactReleasePermit,
} from './retained-sources.ts';
import { canonicalJson, domainDigest } from '../events/domain-events.ts';
import {
  appendRelationalCommit,
  readRelationalCommit,
  readRelationalReceipt,
  replayRelationalCommit,
  assertRelationalOpeningSource,
  type StoredRelationalCommit,
} from '../events/relational-event-store.ts';
import type {
  RelationalCommand,
  RelationalEvent,
  RelationalPayloads,
  RelationalFact,
} from '../events/relational-events.ts';
import { assertRelationalStorageFormat } from './relational-storage.ts';
import {
  assertStoredResultDescriptor,
  assertReadableStoredResultDescriptor,
  readableResultFile,
  type ReadableStoredResultDescriptor,
  type describeStoredResult,
} from './result-files.ts';
import { r2Config, R2_PIKPAK_THRESHOLD_BYTES, type R2Storage } from './r2-storage.ts';
import { pikpakConfig, type PikPakStorage } from './pikpak-storage.ts';
import { RelationalCommitReplayer } from './relational-replay.ts';
import { assertCurrentOwnerCommand, type MemberUser } from '../members/authentication.ts';
import {
  assertHistoryRetirementControls,
  assertRetirableArtifact,
  assertHistoryRetirementCommit,
  historyRetirementIds,
  historyRetirementFingerprint,
  type HistoryRetirementControls,
} from './history-retirement.ts';

type Metadata = Omit<RelationalCommand, 'events' | 'expectedVersions' | 'receipt'> & {
  policyVersion: string;
  pricingVersion: string;
};
type Kind = RelationalPayloads['StorageTargetRegistered']['kind'];
type Target = RelationalPayloads['StorageTargetRegistered'];
type Replica = RelationalPayloads['ArtifactReplicaRecorded'];
type Publication = ReturnType<typeof describeStoredResult>;
// 原verified承诺证明副本曾存在；未知uploaded时点不等于从未上传。
export function hasArtifactReplicaPresence(
  replica: Pick<Replica, 'uploadedAt' | 'verifiedAt' | 'deletedAt'> | null | undefined,
) {
  return (
    !!replica &&
    replica.deletedAt === null &&
    (replica.uploadedAt != null || replica.verifiedAt != null)
  );
}
type ReplicaFact = Extract<
  RelationalFact,
  { eventType: 'StorageTargetRegistered' | 'ArtifactReplicaRecorded' }
>;
export interface RelationalReplicaReadConfiguration {
  dataDir: string;
  r2?: R2Storage | null;
  pikpak?: PikPakStorage | null;
}
export interface RelationalReplicaConfiguration extends RelationalReplicaReadConfiguration {
  idle(): boolean;
  retainedSources?: string;
}
type Ticket = Readonly<{ artifactId: string; storageTargetId: string }>;
type ReleaseEvidence = {
  sourceEventId: string;
  destinationEventId: string;
  destinationStorageTargetId: string;
  registryId: string;
  revision: number;
  proofSha256: string;
  claimSha256: string;
};
function publicReleaseClaim(claim: RetainedArtifactReleaseClaim) {
  const { dataDir: _dataDir, ...source } = claim.source;
  return { ...claim, source };
}
type Phase = 'reserved' | 'uploaded' | 'verified' | 'local_verified' | 'failed';
export const relationalReplicaFacts = new Set<RelationalEvent['eventType']>([
  'StorageTargetRegistered',
  'OpeningStorageTargetRecorded',
  'OpeningReplicaRecorded',
  'ArtifactReplicaRecorded',
  'ArtifactHistoryRetired',
]);
function fail(message: string): never {
  throw Object.assign(new Error(message), { statusCode: 409, code: 'REPLICA_STATE_CONFLICT' });
}
const LOCAL_NAMESPACE = 'images';
function targetIdentity(kind: Kind, namespace: string): Target {
  return {
    storageTargetId: `${kind}:${domainDigest(namespace)}`,
    kind,
    namespace,
    credentialRef: kind === 'local' ? null : `deployment:${kind}`,
  };
}
function replicaLocation(
  publication: ReadableStoredResultDescriptor,
  kind: Kind,
): Pick<Replica, 'objectKey' | 'encoding'> {
  if (kind === 'local') {
    return { objectKey: path.posix.basename(publication.file), encoding: 'identity' };
  }
  if (kind === 'r2') {
    return {
      objectKey: `images/${publication.imageId}/${publication.sha256}`,
      encoding: 'identity',
    };
  }
  return { objectKey: `${publication.sha256}.br`, encoding: 'br' };
}
function assertTarget(target: Target) {
  let valid =
    target.kind === 'local' &&
    target.namespace === LOCAL_NAMESPACE &&
    target.credentialRef === null;
  if (target.kind === 'r2') {
    valid =
      /^r2:\/\/[a-f0-9]{32}\/[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/.test(target.namespace) &&
      target.credentialRef === 'deployment:r2';
  } else if (target.kind === 'pikpak') {
    try {
      const url = new URL(target.namespace);
      valid =
        url.protocol === 'https:' &&
        !url.username &&
        !url.password &&
        !url.search &&
        !url.hash &&
        url.href === target.namespace &&
        url.pathname.endsWith('/') &&
        target.credentialRef === 'deployment:pikpak';
    } catch {
      valid = false;
    }
  }
  if (!valid || target.storageTargetId !== `${target.kind}:${domainDigest(target.namespace)}`) {
    fail('副本目标缺少规范的来源身份');
  }
}

// 只拥有目标及副本事实；部署秘密、网络、文件及物理删除不属于业务 replay。
export class RelationalReplicaStore {
  private sqlite: Database.Database;
  private configuration?: RelationalReplicaReadConfiguration;
  private env: NodeJS.ProcessEnv;
  private tickets = new WeakMap<
    Ticket,
    {
      configuration: string;
      artifactSource: string;
      targetSource: string;
      replicaSource: string | null;
      background: boolean;
    }
  >();
  private releaseTickets = new WeakMap<
    Ticket,
    {
      source: Ticket;
      destination: Ticket;
      claim: RetainedArtifactReleaseClaim;
    }
  >();
  constructor(
    sqlite: Database.Database,
    configuration?: RelationalReplicaReadConfiguration,
    env: NodeJS.ProcessEnv = process.env,
  ) {
    assertRelationalStorageFormat(sqlite);
    this.sqlite = sqlite;
    this.configuration = configuration;
    this.env = env;
  }
  private owned<T>(run: () => T): T {
    if (!this.sqlite.inTransaction) {
      throw new Error('Replica writes require the owning command transaction.');
    }
    return this.sqlite.transaction(run)();
  }
  private active() {
    if (
      !this.configuration ||
      !('idle' in this.configuration) ||
      typeof this.configuration.idle !== 'function' ||
      !this.configuration.idle()
    ) {
      fail('副本后台当前不可执行');
    }
    return this.configuration;
  }
  private authorize(metadata: Metadata) {
    if (metadata.actorKind !== 'system' || metadata.actorId !== null) {
      fail('副本命令只接受执行器后台');
    }
    this.active();
  }
  private configuredTarget(kind: Kind) {
    const config = this.configuration;
    if (!config) {
      fail('副本目标当前未配置');
    }
    let namespace: string, secret: unknown;
    if (kind === 'local') {
      if (typeof config.dataDir !== 'string' || !config.dataDir) {
        fail('缺少本地副本目录');
      }
      namespace = LOCAL_NAMESPACE;
      secret = path.resolve(config.dataDir);
    } else if (kind === 'r2' && config.r2) {
      const current = config.r2.config;
      r2Config({
        R2_ACCOUNT_ID: current.accountId,
        R2_BUCKET: current.bucket,
        R2_ACCESS_KEY_ID: current.accessKeyId,
        R2_SECRET_ACCESS_KEY: current.secretAccessKey,
      });
      namespace = `r2://${current.accountId}/${current.bucket}`;
      secret = current;
    } else if (kind === 'pikpak' && config.pikpak) {
      const current = config.pikpak.config;
      const normalized = pikpakConfig({
        PIKPAK_WEBDAV_URL: current.url,
        PIKPAK_WEBDAV_USERNAME: current.username,
        PIKPAK_WEBDAV_PASSWORD: current.password,
      })!;
      if (normalized.url !== current.url) {
        fail('PikPak 目录尚未规范化');
      }
      namespace = current.url;
      secret = current;
    } else {
      fail('副本目标当前未配置');
    }
    return {
      target: targetIdentity(kind, namespace),
      fingerprint: domainDigest(secret),
    };
  }
  readConfiguredStorageTarget(kind: Kind) {
    this.active();
    return this.configuredTarget(kind).target;
  }
  planLocalArtifactPublication(publication: Publication, at: number) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Local publication planning requires the owning command transaction.');
    }
    assertStoredResultDescriptor(publication);
    if (!Number.isSafeInteger(at) || at < 0) {
      fail('Local 发布缺少有效的原提交时间');
    }
    const target = targetIdentity('local', LOCAL_NAMESPACE);
    const exists = this.isStorageTargetRegistered(target.storageTargetId);
    return {
      target: exists
        ? null
        : ({ eventType: 'StorageTargetRegistered', payload: target } satisfies ReplicaFact),
      replica: {
        eventType: 'ArtifactReplicaRecorded',
        payload: {
          artifactId: publication.imageId,
          storageTargetId: target.storageTargetId,
          ...replicaLocation(publication, 'local'),
          uploadedAt: at,
          verifiedAt: at,
          deletedAt: null,
          lastErrorCode: null,
        },
      } satisfies ReplicaFact,
    };
  }
  private source(eventId: string) {
    const row = this.sqlite
      .prepare<[string], { seq: string }>(
        'SELECT CAST(commit_seq AS TEXT) AS seq FROM domain_events WHERE event_id=?',
      )
      .get(eventId);
    if (!row) {
      fail('副本缺少原事实');
    }
    return readRelationalCommit(this.sqlite, row.seq).events.find((e) => e.eventId === eventId)!;
  }
  readArtifact(artifactId: string) {
    const row = this.sqlite
      .prepare<
        [string],
        {
          id: string;
          memberId: string;
          inputId: string;
          operation: string | null;
          publishedAt: number | null;
          mimeType: string;
          byteLength: number;
          sha256: string;
          retiredAt: number | null;
          sourceEventId: string;
        }
      >(
        'SELECT id,member_id AS memberId,input_id AS inputId,operation,mime_type AS mimeType,byte_length AS byteLength,sha256,published_at AS publishedAt,retired_at AS retiredAt,source_event_id AS sourceEventId FROM artifacts WHERE id=?',
      )
      .get(artifactId);
    if (!row) {
      fail('副本产物不存在');
    }
    let source = this.source(row.sourceEventId);
    if (source.eventType === 'ArtifactHistoryRetired') {
      if (
        source.payload.artifactId !== row.id ||
        source.memberId !== row.memberId ||
        source.accountId !== null ||
        row.retiredAt !== source.occurredAt
      ) {
        fail('退休产物与原退休事实不一致');
      }
      const original = this.sqlite
        .prepare<[string], { eventId: string }>(
          "SELECT event_id AS eventId FROM domain_events WHERE stream_id=? AND event_type IN ('ArtifactPublished','OpeningArtifactRecorded')",
        )
        .all(`artifact:${row.id}`);
      if (original.length !== 1) {
        fail('退休产物缺少唯一的原发布事实');
      }
      source = this.source(original[0].eventId);
    }
    if (
      (source.eventType !== 'ArtifactPublished' &&
        source.eventType !== 'OpeningArtifactRecorded') ||
      source.payload.artifactId !== row.id ||
      source.payload.memberId !== row.memberId ||
      source.payload.inputId !== row.inputId ||
      source.payload.operation !== row.operation ||
      (source.eventType === 'ArtifactPublished'
        ? source.occurredAt
        : source.payload.publishedAt) !== row.publishedAt ||
      source.payload.mimeType !== row.mimeType ||
      source.payload.byteLength !== row.byteLength ||
      source.payload.sha256 !== row.sha256
    ) {
      fail('副本产物与原发布事实不一致');
    }
    const descriptor = {
      imageId: row.id,
      file: readableResultFile({
        imageId: row.id,
        operation: row.operation,
        mimeType: row.mimeType,
      }),
      operation: row.operation,
      mimeType: row.mimeType,
      byteLength: row.byteLength,
      sha256: row.sha256,
    };
    assertReadableStoredResultDescriptor(descriptor);
    return { ...row, descriptor, source };
  }
  readRecoverableArtifacts(afterId: string | null, limit: number) {
    if (
      (afterId !== null && (typeof afterId !== 'string' || !afterId || afterId.length > 128)) ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > 100
    ) {
      fail('产物枚举需要有效游标及 1–100 的上界');
    }
    return this.sqlite
      .prepare<[string, number], { id: string }>(
        'SELECT id FROM artifacts WHERE retired_at IS NULL AND id>? ORDER BY id LIMIT ?',
      )
      .all(afterId ?? '', limit)
      .map(({ id }) => ({ id, descriptor: this.readArtifact(id).descriptor }));
  }
  // 保留源枚举不能用在线可恢复列表：退休行的原副本承诺仍属于该封存源。
  readRetainedArtifactDependencies(afterId = '') {
    if (typeof afterId !== 'string' || afterId.length > 128) {
      fail('保留源枚举需要原产物游标');
    }
    return this.sqlite
      .prepare<[string], { id: string }>(
        'SELECT id FROM artifacts WHERE id>? ORDER BY id LIMIT 100',
      )
      .all(afterId)
      .map(({ id }) => {
        const artifact = this.readArtifact(id);
        const replicas = this.sqlite
          .prepare<[string], { id: string }>(
            'SELECT storage_target_id AS id FROM artifact_replicas WHERE artifact_id=? ORDER BY storage_target_id',
          )
          .all(id)
          .map(({ id: targetId }) => ({
            target: this.readStorageTarget(targetId),
            replica: this.readArtifactReplica(id, targetId)!,
          }));
        return {
          id,
          descriptor: artifact.descriptor,
          sourceEventId: artifact.sourceEventId,
          replicas,
        };
      });
  }
  private r2Bytes(storageTargetId: string) {
    return this.sqlite
      .prepare<[string], { bytes: bigint }>(
        `SELECT COALESCE(SUM(bytes),0) AS bytes FROM (
          SELECT MAX(a.byte_length) AS bytes FROM artifact_replicas r JOIN artifacts a ON a.id=r.artifact_id
          WHERE r.storage_target_id=? AND r.deleted_at IS NULL AND r.encoding='identity' GROUP BY r.object_key)`,
      )
      .safeIntegers()
      .get(storageTargetId)!.bytes;
  }
  readDueArtifactReplica(now: number): {
    artifactId: string;
    kind: 'r2' | 'pikpak';
    sourceEventId: string | null;
  } | null {
    if (!Number.isSafeInteger(now) || now < 0) {
      fail('副本调度缺少有效当前时间');
    }
    const configuration = this.active();
    if (!configuration.r2) {
      return null;
    }
    const hot = this.configuredTarget('r2').target.storageTargetId,
      local = this.configuredTarget('local').target.storageTargetId,
      retry = now - 3600000;
    if (configuration.pikpak) {
      // 本站当前目标的 identity 登记/预留按物理对象去重；不是全云或未知压缩长度的容量。
      const bytes = this.r2Bytes(hot);
      const cold = this.configuredTarget('pikpak').target.storageTargetId,
        candidate = this.sqlite
          .prepare<
            [string, string, number, number, number],
            { artifactId: string; sourceEventId: string | null }
          >(
            `SELECT a.id AS artifactId,d.source_event_id AS sourceEventId FROM artifacts a
            JOIN artifact_replicas r ON r.artifact_id=a.id AND r.storage_target_id=?
            LEFT JOIN artifact_replicas d ON d.artifact_id=a.id AND d.storage_target_id=?
            LEFT JOIN domain_events e ON e.event_id=d.source_event_id
            WHERE a.retired_at IS NULL AND a.mime_type IN ('image/png','image/jpeg','image/webp')
              AND r.deleted_at IS NULL AND r.verified_at IS NOT NULL AND r.last_error_code IS NULL
              AND r.encoding='identity' AND r.object_key='images/'||a.id||'/'||a.sha256
              AND (r.verified_at<? OR ?)
              AND (d.artifact_id IS NULL OR (d.deleted_at IS NULL AND d.uploaded_at IS NULL AND d.verified_at IS NULL
                AND d.encoding='br' AND d.object_key=a.sha256||'.br'
                AND e.event_type='ArtifactReplicaRecorded' AND e.occurred_at<?))
            ORDER BY r.verified_at,a.published_at,a.id LIMIT 1`,
          )
          .get(
            hot,
            cold,
            now - 7 * 86400000,
            bytes >= BigInt(R2_PIKPAK_THRESHOLD_BYTES) ? 1 : 0,
            retry,
          );
      if (candidate) {
        return { ...candidate, kind: 'pikpak' };
      }
    }
    // Opening 没有原 mock/失败尝试时刻；不以开账 header 时间补造自动复制或重试资格。
    const candidate = this.sqlite
      .prepare<
        [string, string, number, number, number],
        { artifactId: string; sourceEventId: string | null }
      >(
        `SELECT a.id AS artifactId,d.source_event_id AS sourceEventId FROM artifacts a
        JOIN domain_events origin ON origin.event_id=a.source_event_id
        LEFT JOIN artifact_replicas l ON l.artifact_id=a.id AND l.storage_target_id=?
        LEFT JOIN artifact_replicas d ON d.artifact_id=a.id AND d.storage_target_id=?
        LEFT JOIN domain_events e ON e.event_id=d.source_event_id
        WHERE a.retired_at IS NULL AND a.mime_type IN ('image/png','image/jpeg','image/webp')
          AND (d.artifact_id IS NULL OR (d.deleted_at IS NULL AND d.encoding='identity'
            AND d.object_key='images/'||a.id||'/'||a.sha256
            AND (d.last_error_code IS NULL OR (d.last_error_code!='REPLICA_RELEASE_FAILED' AND e.event_type='ArtifactReplicaRecorded' AND e.occurred_at<?))))
          AND (((d.uploaded_at IS NOT NULL OR d.verified_at IS NOT NULL) AND (d.verified_at IS NULL OR d.last_error_code IS NOT NULL))
            OR (origin.event_type='ArtifactPublished' AND a.published_at<?
              AND l.deleted_at IS NULL AND l.verified_at IS NOT NULL
              AND (d.artifact_id IS NULL OR (d.uploaded_at IS NULL AND d.verified_at IS NULL
                AND e.event_type='ArtifactReplicaRecorded' AND e.occurred_at<?))))
        ORDER BY a.published_at,a.id LIMIT 1`,
      )
      .get(local, hot, retry, now - 86400000, retry);
    return candidate ? { ...candidate, kind: 'r2' } : null;
  }
  readDueArtifactRelease(
    now: number,
    afterId = '',
    afterKind?: 'local' | 'r2',
  ): {
    artifactId: string;
    kind: 'local' | 'r2';
    sourceEventId: string;
  } | null {
    if (
      !Number.isSafeInteger(now) ||
      now < 0 ||
      typeof afterId !== 'string' ||
      afterId.length > 128 ||
      (afterKind !== undefined && afterKind !== 'local' && afterKind !== 'r2')
    ) {
      fail('副本删除调度缺少有效时间或原游标');
    }
    const configuration = this.active();
    if (!configuration.r2) {
      return null;
    }
    const local = this.configuredTarget('local').target.storageTargetId,
      hot = this.configuredTarget('r2').target.storageTargetId,
      cold = configuration.pikpak ? this.configuredTarget('pikpak').target.storageTargetId : '',
      pressure = this.r2Bytes(hot) >= BigInt(R2_PIKPAK_THRESHOLD_BYTES) ? 1 : 0;
    return (
      this.sqlite
        .prepare<unknown[], { artifactId: string; kind: 'local' | 'r2'; sourceEventId: string }>(
          `SELECT * FROM (
        SELECT a.id AS artifactId,'local' AS kind,s.source_event_id AS sourceEventId
        FROM artifacts a JOIN artifact_replicas s ON s.artifact_id=a.id AND s.storage_target_id=?
        JOIN artifact_replicas d ON d.artifact_id=a.id AND d.storage_target_id=?
        JOIN domain_events e ON e.event_id=s.source_event_id
        WHERE a.retired_at IS NULL AND a.published_at<?
          AND a.mime_type IN ('image/png','image/jpeg','image/webp')
          AND s.deleted_at IS NULL AND s.verified_at IS NOT NULL AND s.encoding='identity'
          AND (s.last_error_code IS NULL OR (s.last_error_code='REPLICA_RELEASE_FAILED'
            AND e.event_type='ArtifactReplicaRecorded' AND e.occurred_at<?))
          AND d.deleted_at IS NULL AND d.verified_at IS NOT NULL AND d.last_error_code IS NULL
          AND d.encoding='identity' AND d.object_key='images/'||a.id||'/'||a.sha256
        UNION ALL
        SELECT a.id AS artifactId,'r2' AS kind,s.source_event_id AS sourceEventId
        FROM artifacts a JOIN artifact_replicas s ON s.artifact_id=a.id AND s.storage_target_id=?
        JOIN artifact_replicas d ON d.artifact_id=a.id AND d.storage_target_id=?
        JOIN domain_events e ON e.event_id=s.source_event_id
        WHERE a.retired_at IS NULL AND a.mime_type IN ('image/png','image/jpeg','image/webp')
          AND s.verified_at IS NOT NULL AND (s.verified_at<? OR ?)
          AND s.deleted_at IS NULL AND s.encoding='identity' AND s.object_key='images/'||a.id||'/'||a.sha256
          AND (s.last_error_code IS NULL OR (s.last_error_code='REPLICA_RELEASE_FAILED'
            AND e.event_type='ArtifactReplicaRecorded' AND e.occurred_at<?))
          AND d.deleted_at IS NULL AND d.uploaded_at IS NOT NULL AND d.last_error_code IS NULL
          AND d.encoding='br' AND d.object_key=a.sha256||'.br')
        WHERE artifactId>? OR (artifactId=? AND kind>?) ORDER BY artifactId,kind LIMIT 1`,
        )
        .get(
          local,
          hot,
          now - 86400000,
          now - 3600000,
          hot,
          cold,
          now - 7 * 86400000,
          pressure,
          now - 3600000,
          afterId,
          afterId,
          afterKind ?? 'r2',
        ) ?? null
    );
  }
  readStorageTarget(storageTargetId: string) {
    const row = this.sqlite
      .prepare<[string], Target & { sourceEventId: string }>(
        'SELECT id AS storageTargetId,kind,namespace,credential_ref AS credentialRef,source_event_id AS sourceEventId FROM storage_targets WHERE id=?',
      )
      .get(storageTargetId);
    if (!row) {
      fail('副本目标不存在');
    }
    const source = this.source(row.sourceEventId);
    let payload: Target;
    if (source.eventType === 'StorageTargetRegistered') {
      payload = source.payload;
    } else if (source.eventType === 'OpeningStorageTargetRecorded') {
      assertRelationalOpeningSource(this.sqlite, source);
      const { origin: _origin, ...original } = source.payload;
      payload = original;
    } else {
      fail('副本目标缺少原登记事实');
    }
    if (
      canonicalJson(payload) !==
      canonicalJson({
        storageTargetId: row.storageTargetId,
        kind: row.kind,
        namespace: row.namespace,
        credentialRef: row.credentialRef,
      })
    ) {
      fail('副本目标与原登记事实不一致');
    }
    assertTarget(payload);
    return row;
  }
  isStorageTargetRegistered(storageTargetId: string) {
    if (!this.sqlite.prepare('SELECT 1 FROM storage_targets WHERE id=?').get(storageTargetId)) {
      return false;
    }
    this.readStorageTarget(storageTargetId);
    return true;
  }
  readArtifactReplica(artifactId: string, storageTargetId: string) {
    const row =
      this.sqlite
        .prepare<[string, string], Replica & { sourceEventId: string }>(
          'SELECT artifact_id AS artifactId,storage_target_id AS storageTargetId,object_key AS objectKey,encoding,uploaded_at AS uploadedAt,verified_at AS verifiedAt,deleted_at AS deletedAt,last_error_code AS lastErrorCode,source_event_id AS sourceEventId FROM artifact_replicas WHERE artifact_id=? AND storage_target_id=?',
        )
        .get(artifactId, storageTargetId) || null;
    if (row) {
      const { sourceEventId, ...payload } = row;
      const source = this.source(sourceEventId);
      let original: Replica;
      if (source.eventType === 'ArtifactReplicaRecorded') {
        original = source.payload;
      } else if (source.eventType === 'OpeningReplicaRecorded') {
        assertRelationalOpeningSource(this.sqlite, source);
        const { origin: _origin, ...replica } = source.payload;
        original = replica;
      } else {
        fail('副本缺少原登记事实');
      }
      if (
        canonicalJson(original) !== canonicalJson(payload) ||
        source.memberId !== this.readArtifact(artifactId).memberId
      ) {
        fail('副本当前行与原事实不一致');
      }
    }
    return row;
  }
  private append(
    metadata: Metadata,
    fact: ReplicaFact,
    memberId: string | null,
    release?: ReleaseEvidence,
  ) {
    const streamId =
      fact.eventType === 'StorageTargetRegistered'
        ? `storage:${fact.payload.storageTargetId}`
        : `artifact:${fact.payload.artifactId}`;
    const version =
      this.sqlite
        .prepare<[string], { value: string }>(
          'SELECT CAST(stream_version AS TEXT) AS value FROM stream_heads WHERE stream_id=?',
        )
        .get(streamId)?.value || '0';
    const { policyVersion, pricingVersion, ...command } = metadata;
    const result = appendRelationalCommit(this.sqlite, {
      ...command,
      expectedVersions: { [streamId]: version },
      receipt: {
        storageTargetId: fact.payload.storageTargetId,
        ...(release ? { release } : {}),
        ...(fact.eventType === 'ArtifactReplicaRecorded'
          ? { artifactId: fact.payload.artifactId }
          : {}),
      },
      events: [
        {
          ...fact,
          streamId,
          memberId,
          accountId: null,
          occurredAt: metadata.recordedAt,
          policyVersion,
          pricingVersion,
        },
      ],
    });
    this.applyReplicaEvents(result.events);
    return result;
  }
  registerStorageTarget(metadata: Metadata, kind: Kind) {
    return this.owned(() => {
      this.authorize(metadata);
      const { target } = this.configuredTarget(kind);
      const command = { ...metadata, fingerprint: domainDigest([metadata.fingerprint, target]) };
      const prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return prior;
      }
      return this.append(command, { eventType: 'StorageTargetRegistered', payload: target }, null);
    });
  }
  retireArtifactHistory(
    metadata: Omit<Metadata, 'fingerprint'>,
    artifactIds: string[],
    actor: MemberUser | null,
    controls: HistoryRetirementControls,
  ) {
    return this.owned(() => {
      const current =
        actor &&
        this.sqlite
          .prepare<[string], { status: string; authEpoch: string | null }>(
            'SELECT status,auth_epoch AS authEpoch FROM members WHERE id=?',
          )
          .get(actor.id);
      assertCurrentOwnerCommand(metadata, actor, current, this.env);
      const ids = historyRetirementIds(artifactIds),
        command = { ...metadata, fingerprint: historyRetirementFingerprint('artifacts', ids) },
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return prior;
      }
      assertHistoryRetirementControls(controls);
      const { policyVersion, pricingVersion, ...envelope } = command;
      const events = ids.map((artifactId) => {
        const artifact = this.readArtifact(artifactId);
        if (artifact.retiredAt !== null) {
          fail('产物历史已经退休');
        }
        assertRetirableArtifact(this.sqlite, artifactId, controls);
        return {
          eventType: 'ArtifactHistoryRetired' as const,
          payload: { artifactId, reason: 'owner_cleared_cache' },
          streamId: `artifact:${artifactId}`,
          memberId: artifact.memberId,
          accountId: null,
          occurredAt: metadata.recordedAt,
          policyVersion,
          pricingVersion,
        };
      });
      return new RelationalCommitReplayer(this.sqlite).appendCommit({
        ...envelope,
        events,
        expectedVersions: Object.fromEntries(
          events.map((event) => [
            event.streamId,
            this.sqlite
              .prepare<[string], { value: string }>(
                'SELECT CAST(stream_version AS TEXT) AS value FROM stream_heads WHERE stream_id=?',
              )
              .get(event.streamId)?.value ?? '0',
          ]),
        ),
        receipt: { retiredArtifactIds: ids },
      });
    });
  }
  beginArtifactReplica(artifactId: string, storageTargetId: string): Ticket {
    this.active();
    return this.beginTicket(artifactId, storageTargetId, true).ticket;
  }
  beginArtifactRead(artifactId: string, kind: Kind) {
    const current = this.configuredTarget(kind);
    if (!this.isStorageTargetRegistered(current.target.storageTargetId)) {
      return null;
    }
    const replica = this.readArtifactReplica(artifactId, current.target.storageTargetId);
    // PikPak 沿第 5.1 节按需实读核验；上传回执不在此提升为持久 verified。
    if (
      !replica ||
      !hasArtifactReplicaPresence(replica) ||
      (kind !== 'pikpak' && replica.verifiedAt === null) ||
      replica.deletedAt !== null
    ) {
      return null;
    }
    const { artifact, target, ticket } = this.beginTicket(
      artifactId,
      current.target.storageTargetId,
      false,
    );
    return {
      artifact,
      target,
      replica,
      ticket,
    };
  }
  readArtifactCacheReference(artifactId: string) {
    const artifact = this.readArtifact(artifactId);
    if (artifact.retiredAt !== null) {
      fail('退休产物不能用于新缓存引用');
    }
    const rows = this.sqlite
      .prepare<[string], { id: string; kind: Kind }>(
        `SELECT s.id,s.kind FROM artifact_replicas r JOIN storage_targets s ON s.id=r.storage_target_id
       WHERE r.artifact_id=? ORDER BY s.id`,
      )
      .all(artifactId);
    let local = false;
    for (const row of rows) {
      this.readStorageTarget(row.id);
      const replica = this.readArtifactReplica(artifactId, row.id)!;
      if (
        row.kind === 'local' &&
        hasArtifactReplicaPresence(replica) &&
        replica.verifiedAt !== null
      ) {
        local = true;
      }
      if (
        hasArtifactReplicaPresence(replica) &&
        ((row.kind === 'r2' && replica.verifiedAt !== null) || row.kind === 'pikpak')
      ) {
        return { artifact, persistent: true, local };
      }
    }
    return { artifact, persistent: false, local };
  }
  private beginTicket(artifactId: string, storageTargetId: string, background: boolean) {
    const artifact = this.readArtifact(artifactId),
      target = this.readStorageTarget(storageTargetId),
      current = this.configuredTarget(target.kind);
    if (
      artifact.retiredAt !== null ||
      canonicalJson(current.target) !==
        canonicalJson({
          storageTargetId,
          kind: target.kind,
          namespace: target.namespace,
          credentialRef: target.credentialRef,
        })
    ) {
      fail('副本目标已不属于当前配置');
    }
    const replica = this.readTicketReplica(artifactId, storageTargetId),
      location = replicaLocation(artifact.descriptor, target.kind);
    // 历史定位仍是持久引用承诺，但不能让当前 IO 或后续 UPSERT 冒充改写了它。
    if (
      replica &&
      (replica.objectKey !== location.objectKey || replica.encoding !== location.encoding)
    ) {
      fail('副本读取定位或编码不属于原产物');
    }
    const ticket = Object.freeze({ artifactId, storageTargetId });
    this.tickets.set(ticket, {
      configuration: current.fingerprint,
      artifactSource: artifact.sourceEventId,
      targetSource: target.sourceEventId,
      replicaSource: replica?.sourceEventId || null,
      background,
    });
    // 同步建票时复用已核事实；IO 后仍由 checkedTicket 完整复核。
    return { ticket, artifact, target };
  }
  private readTicketReplica(artifactId: string, storageTargetId: string) {
    const replica = this.readArtifactReplica(artifactId, storageTargetId);
    if (replica?.deletedAt != null) {
      fail('已删除副本不能领取读取或复制票据');
    }
    return replica;
  }
  private checkedTicket(ticket: Ticket, allowDeleted = false) {
    const issued = this.tickets.get(ticket);
    if (!issued) {
      fail('副本票据不属于当前后台');
    }
    if (issued.background) {
      this.active();
    }
    const artifact = this.readArtifact(ticket.artifactId),
      target = this.readStorageTarget(ticket.storageTargetId),
      configured = this.configuredTarget(target.kind);
    if (
      artifact.retiredAt !== null ||
      configured.target.storageTargetId !== target.storageTargetId ||
      issued.configuration !== configured.fingerprint ||
      artifact.sourceEventId !== issued.artifactSource ||
      target.sourceEventId !== issued.targetSource
    ) {
      fail('副本票据的来源或配置已变化');
    }
    return {
      issued,
      artifact,
      target,
      replica: allowDeleted
        ? this.readArtifactReplica(ticket.artifactId, ticket.storageTargetId)
        : this.readTicketReplica(ticket.artifactId, ticket.storageTargetId),
    };
  }
  assertArtifactReplicaTicket(ticket: Ticket) {
    const { issued, replica } = this.checkedTicket(ticket);
    if ((replica?.sourceEventId || null) !== issued.replicaSource) {
      fail('副本票据已被另一提交替换');
    }
  }
  private releasePair(
    artifactId: string,
    sourceId: string,
    destinationId: string,
    allowDeleted = false,
  ) {
    const artifact = this.readArtifact(artifactId),
      sourceTarget = this.readStorageTarget(sourceId),
      destinationTarget = this.readStorageTarget(destinationId),
      source = this.readArtifactReplica(artifactId, sourceId),
      destination = this.readArtifactReplica(artifactId, destinationId);
    if (
      artifact.retiredAt !== null ||
      (sourceTarget.kind !== 'local' && sourceTarget.kind !== 'r2') ||
      destinationTarget.kind === 'local' ||
      (sourceTarget.kind === 'local'
        ? destinationTarget.kind !== 'r2'
        : destinationTarget.kind !== 'pikpak') ||
      !source ||
      (!allowDeleted && !hasArtifactReplicaPresence(source)) ||
      source.verifiedAt === null ||
      (source.lastErrorCode !== null && source.lastErrorCode !== 'REPLICA_RELEASE_FAILED') ||
      !destination ||
      !hasArtifactReplicaPresence(destination) ||
      destination.lastErrorCode !== null ||
      (sourceTarget.kind === 'local'
        ? destination.verifiedAt === null
        : destination.uploadedAt === null) ||
      source.objectKey !== replicaLocation(artifact.descriptor, sourceTarget.kind).objectKey ||
      source.encoding !== 'identity' ||
      destination.objectKey !==
        replicaLocation(artifact.descriptor, destinationTarget.kind).objectKey ||
      destination.encoding !== (sourceTarget.kind === 'local' ? 'identity' : 'br')
    ) {
      fail('删除副本缺少同产物的持久目的或原定位');
    }
    const claim: RetainedArtifactReleaseClaim = {
      artifactId,
      sha256: artifact.sha256,
      byteLength: artifact.byteLength,
      source: {
        kind: sourceTarget.kind,
        namespace: sourceTarget.namespace,
        objectKey: source.objectKey,
        encoding: 'identity',
      },
      destination: {
        kind: destinationTarget.kind,
        namespace: destinationTarget.namespace,
        objectKey: destination.objectKey,
        encoding: sourceTarget.kind === 'local' ? 'identity' : 'br',
      },
    };
    return {
      artifact,
      source: { target: sourceTarget, replica: source },
      destination: { target: destinationTarget, replica: destination },
      claim,
    };
  }
  private assertReleasePolicy(
    pair: ReturnType<RelationalReplicaStore['releasePair']>,
    now: number,
  ) {
    if (!Number.isSafeInteger(now) || now < 0) {
      fail('副本删除缺少有效确认时间');
    }
    const { artifact, source } = pair,
      failure =
        source.replica.lastErrorCode === 'REPLICA_RELEASE_FAILED'
          ? this.source(source.replica.sourceEventId)
          : null;
    if (
      !['image/png', 'image/jpeg', 'image/webp'].includes(artifact.mimeType) ||
      (source.target.kind === 'local' &&
        (artifact.publishedAt === null || artifact.publishedAt >= now - 86400000)) ||
      (source.target.kind === 'r2' &&
        (source.replica.verifiedAt === null ||
          (source.replica.verifiedAt >= now - 7 * 86400000 &&
            this.r2Bytes(source.target.storageTargetId) < BigInt(R2_PIKPAK_THRESHOLD_BYTES)))) ||
      (failure &&
        (failure.eventType !== 'ArtifactReplicaRecorded' || failure.occurredAt >= now - 3600000))
    ) {
      fail('副本尚未达到原保留期或删除重试期限');
    }
  }
  beginArtifactRelease(artifactId: string, kind: 'local' | 'r2', now: number) {
    this.active();
    if (kind !== 'local' && kind !== 'r2') {
      fail('只有Local或R2副本可以释放');
    }
    const source = this.configuredTarget(kind),
      destination = this.configuredTarget(kind === 'local' ? 'r2' : 'pikpak'),
      pair = this.releasePair(
        artifactId,
        source.target.storageTargetId,
        destination.target.storageTargetId,
      ),
      sourceTicket = this.beginTicket(artifactId, source.target.storageTargetId, true).ticket,
      destinationTicket = this.beginTicket(
        artifactId,
        destination.target.storageTargetId,
        true,
      ).ticket;
    this.assertReleasePolicy(pair, now);
    if (kind === 'local') {
      pair.claim.source.dataDir = path.resolve(this.configuration!.dataDir);
    }
    const ticket = Object.freeze({ artifactId, storageTargetId: source.target.storageTargetId });
    this.releaseTickets.set(ticket, {
      source: sourceTicket,
      destination: destinationTicket,
      claim: structuredClone(pair.claim),
    });
    return {
      ...pair,
      source: { ...pair.source, ticket: sourceTicket },
      destination: { ...pair.destination, ticket: destinationTicket },
      ticket,
    };
  }
  private checkedReleaseTicket(ticket: Ticket, allowDeleted = false) {
    const issued = this.releaseTickets.get(ticket);
    if (!issued) {
      fail('删除票据不属于当前后台');
    }
    const source = this.checkedTicket(issued.source, allowDeleted),
      destination = this.checkedTicket(issued.destination),
      pair = this.releasePair(
        ticket.artifactId,
        issued.source.storageTargetId,
        issued.destination.storageTargetId,
        allowDeleted,
      );
    if (
      destination.replica?.sourceEventId !== destination.issued.replicaSource ||
      (!allowDeleted && source.replica?.sourceEventId !== source.issued.replicaSource)
    ) {
      fail('删除票据的源或目的已被另一提交替换');
    }
    if (pair.source.target.kind === 'local') {
      pair.claim.source.dataDir = path.resolve(this.configuration!.dataDir);
    }
    if (canonicalJson(pair.claim) !== canonicalJson(issued.claim)) {
      fail('删除票据的原对象或物理位置已变化');
    }
    return { ...pair, issued, sourceIssued: source.issued };
  }
  assertArtifactReleaseTicket(ticket: Ticket, now: number) {
    this.assertReleasePolicy(this.checkedReleaseTicket(ticket), now);
  }
  recordArtifactRelease(metadata: Metadata, ticket: Ticket, permit: RetainedArtifactReleasePermit) {
    return this.owned(() => {
      this.authorize(metadata);
      const pair = this.checkedReleaseTicket(ticket, true),
        evidence = assertRetainedArtifactReleasePermit(permit, pair.issued.claim),
        claimSha256 = domainDigest(publicReleaseClaim(pair.claim)),
        release: ReleaseEvidence = {
          sourceEventId: pair.sourceIssued.replicaSource!,
          destinationEventId: pair.destination.replica.sourceEventId,
          destinationStorageTargetId: pair.destination.target.storageTargetId,
          ...evidence,
          claimSha256,
        },
        command = {
          ...metadata,
          fingerprint: domainDigest([
            metadata.fingerprint,
            release.sourceEventId,
            release.destinationEventId,
            claimSha256,
          ]),
        },
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        if (
          pair.source.replica.deletedAt === null ||
          this.source(pair.source.replica.sourceEventId).commitSeq !== prior.commitSeq
        ) {
          fail('原删除回执与当前来源不符');
        }
        return prior;
      }
      if (
        pair.source.replica.deletedAt !== null ||
        pair.source.replica.sourceEventId !== release.sourceEventId
      ) {
        fail('删除票据已被另一提交替换');
      }
      this.assertReleasePolicy(pair, metadata.recordedAt);
      const { sourceEventId: _eventId, ...original } = pair.source.replica;
      return this.append(
        command,
        {
          eventType: 'ArtifactReplicaRecorded',
          payload: {
            ...original,
            deletedAt: metadata.recordedAt,
            lastErrorCode: null,
          },
        },
        pair.artifact.memberId,
        release,
      );
    });
  }
  private assertReleaseCommit(event: RelationalEvent, stored: StoredRelationalCommit) {
    if (event.eventType !== 'ArtifactReplicaRecorded') {
      fail('删除确认必须保存原副本事实');
    }
    const p = event.payload,
      receipt = stored.command.receipt;
    if (
      !receipt ||
      typeof receipt !== 'object' ||
      Array.isArray(receipt) ||
      receipt.artifactId !== p.artifactId ||
      receipt.storageTargetId !== p.storageTargetId ||
      !receipt.release ||
      typeof receipt.release !== 'object' ||
      Array.isArray(receipt.release) ||
      stored.events.length !== 1 ||
      stored.events[0].eventId !== event.eventId ||
      stored.command.recordedAt !== event.occurredAt
    ) {
      fail('副本删除缺少唯一完整原回执');
    }
    const proof = receipt.release;
    if (
      Object.keys(receipt).sort().join(',') !== 'artifactId,release,storageTargetId' ||
      Object.keys(proof).sort().join(',') !==
        'claimSha256,destinationEventId,destinationStorageTargetId,proofSha256,registryId,revision,sourceEventId' ||
      typeof proof.destinationStorageTargetId !== 'string' ||
      typeof proof.registryId !== 'string' ||
      !/^[a-f0-9-]{36}$/.test(proof.registryId) ||
      typeof proof.revision !== 'number' ||
      !Number.isSafeInteger(proof.revision) ||
      proof.revision < 0 ||
      typeof proof.proofSha256 !== 'string' ||
      !/^[a-f0-9]{64}$/.test(proof.proofSha256)
    ) {
      fail('副本删除保留源证据无效');
    }
    const pair = this.releasePair(
      p.artifactId,
      p.storageTargetId,
      proof.destinationStorageTargetId,
    );
    if (
      pair.source.replica.sourceEventId !== proof.sourceEventId ||
      pair.destination.replica.sourceEventId !== proof.destinationEventId ||
      domainDigest(publicReleaseClaim(pair.claim)) !== proof.claimSha256 ||
      p.uploadedAt !== pair.source.replica.uploadedAt ||
      p.verifiedAt !== pair.source.replica.verifiedAt ||
      p.deletedAt !== event.occurredAt ||
      p.lastErrorCode !== null
    ) {
      fail('副本删除与原来源、目的或确认不一致');
    }
    this.assertReleasePolicy(pair, event.occurredAt);
  }
  recordArtifactReplica(
    metadata: Metadata,
    ticket: Ticket,
    phase: Phase,
    errorCode: string | null = null,
  ) {
    return this.owned(() => {
      this.authorize(metadata);
      const { issued, artifact, target } = this.checkedTicket(ticket);
      if (!issued.background) {
        fail('只读副本票据不能提交状态');
      }
      if (
        !['reserved', 'uploaded', 'verified', 'local_verified', 'failed'].includes(phase) ||
        (phase === 'failed'
          ? !errorCode || !/^[A-Z0-9_]{1,64}$/.test(errorCode)
          : errorCode !== null)
      ) {
        fail('副本阶段或故障分类无效');
      }
      const command = {
        ...metadata,
        fingerprint: domainDigest([metadata.fingerprint, ticket, phase, errorCode]),
      };
      const prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return prior;
      }
      const previous = this.readArtifactReplica(ticket.artifactId, ticket.storageTargetId);
      if ((previous?.sourceEventId || null) !== issued.replicaSource) {
        fail('副本票据已被另一提交替换');
      }
      if (
        previous?.lastErrorCode === 'REPLICA_RELEASE_FAILED' &&
        (phase !== 'failed' || errorCode !== 'REPLICA_RELEASE_FAILED')
      ) {
        fail('副本删除未决，不能以复制或验证覆盖');
      }
      const row: Replica = {
        artifactId: ticket.artifactId,
        storageTargetId: ticket.storageTargetId,
        ...replicaLocation(artifact.descriptor, target.kind),
        uploadedAt: previous?.uploadedAt ?? null,
        verifiedAt: previous?.verifiedAt ?? null,
        deletedAt: null,
        lastErrorCode: errorCode,
      };
      if (phase === 'local_verified') {
        if (
          target.kind !== 'local' ||
          previous ||
          artifact.source.eventType !== 'ArtifactPublished'
        ) {
          fail('本地副本已经登记或目标不符');
        }
        assertStoredResultDescriptor(artifact.descriptor);
        row.uploadedAt = row.verifiedAt = metadata.recordedAt;
      } else if (phase === 'uploaded') {
        if (!previous || hasArtifactReplicaPresence(previous)) {
          fail('副本没有待确认的上传');
        }
        row.uploadedAt = metadata.recordedAt;
      } else if (phase === 'verified') {
        if (
          !previous ||
          !hasArtifactReplicaPresence(previous) ||
          (previous.verifiedAt !== null && previous.lastErrorCode === null)
        ) {
          fail('副本没有待核验的上传');
        }
        row.verifiedAt ??= metadata.recordedAt;
      } else if (phase === 'reserved' && (target.kind === 'local' || previous)) {
        fail('副本已经预留或目标不符');
      } else if (phase === 'failed' && !previous) {
        fail('故障缺少原副本');
      }
      return this.append(
        command,
        { eventType: 'ArtifactReplicaRecorded', payload: row },
        artifact.memberId,
      );
    });
  }
  applyReplicaEvents(events: RelationalEvent[]) {
    return this.owned(() => {
      for (const event of events) {
        const original = readRelationalCommit(this.sqlite, event.commitSeq),
          source = original.events.find((entry) => entry.eventId === event.eventId);
        if (
          (event.eventType !== 'ArtifactHistoryRetired' &&
            (original.command.actorKind !== 'system' || original.command.actorId !== null)) ||
          !source ||
          canonicalJson(source) !== canonicalJson(event)
        ) {
          fail('副本投影与原完整提交不符');
        }
        const receipt = original.command.receipt;
        if (
          receipt &&
          typeof receipt === 'object' &&
          !Array.isArray(receipt) &&
          Object.hasOwn(receipt, 'release') &&
          (event.eventType !== 'ArtifactReplicaRecorded' || event.payload.deletedAt === null)
        ) {
          fail('副本删除回执缺少原删除决定');
        }
        if (event.eventType === 'ArtifactHistoryRetired') {
          assertHistoryRetirementCommit(this.sqlite, event, 'artifacts');
          const artifact = this.readArtifact(event.payload.artifactId);
          if (artifact.retiredAt !== null) {
            fail('产物历史已经退休');
          }
          assertRetirableArtifact(this.sqlite, artifact.id);
          this.sqlite
            .prepare('UPDATE artifacts SET retired_at=?,source_event_id=? WHERE id=?')
            .run(event.occurredAt, event.eventId, artifact.id);
        } else if (
          event.eventType === 'StorageTargetRegistered' ||
          event.eventType === 'OpeningStorageTargetRecorded'
        ) {
          const p = event.payload;
          if (event.eventType === 'OpeningStorageTargetRecorded') {
            assertRelationalOpeningSource(this.sqlite, event);
          }
          assertTarget(p);
          if (event.memberId !== null || event.accountId !== null) {
            fail('目标登记不能冒充成员或上游账户');
          }
          this.sqlite
            .prepare(
              'INSERT INTO storage_targets(id,kind,namespace,credential_ref,source_event_id) VALUES(?,?,?,?,?)',
            )
            .run(p.storageTargetId, p.kind, p.namespace, p.credentialRef, event.eventId);
        } else if (
          event.eventType === 'ArtifactReplicaRecorded' ||
          event.eventType === 'OpeningReplicaRecorded'
        ) {
          const p = event.payload,
            artifact = this.readArtifact(p.artifactId),
            target = this.readStorageTarget(p.storageTargetId),
            previous = this.readArtifactReplica(p.artifactId, p.storageTargetId);
          const opening = event.eventType === 'OpeningReplicaRecorded';
          if (opening) {
            assertRelationalOpeningSource(this.sqlite, event);
          }
          const location = replicaLocation(artifact.descriptor, target.kind);
          const firstReservation =
            !previous &&
            target.kind !== 'local' &&
            p.uploadedAt === null &&
            p.verifiedAt === null &&
            p.lastErrorCode === null;
          const firstLocal =
            !previous &&
            artifact.source.eventType === 'ArtifactPublished' &&
            target.kind === 'local' &&
            p.uploadedAt === event.occurredAt &&
            p.verifiedAt === event.occurredAt &&
            p.lastErrorCode === null;
          const upload =
            previous &&
            !hasArtifactReplicaPresence(previous) &&
            p.uploadedAt === event.occurredAt &&
            p.verifiedAt === null &&
            p.lastErrorCode === null;
          const verification =
            previous &&
            hasArtifactReplicaPresence(previous) &&
            p.uploadedAt === previous.uploadedAt &&
            p.lastErrorCode === null &&
            ((previous.verifiedAt === null && p.verifiedAt === event.occurredAt) ||
              (previous.verifiedAt !== null &&
                previous.lastErrorCode !== null &&
                previous.lastErrorCode !== 'REPLICA_RELEASE_FAILED' &&
                p.verifiedAt === previous.verifiedAt));
          const failed =
            previous &&
            p.uploadedAt === previous.uploadedAt &&
            p.verifiedAt === previous.verifiedAt &&
            p.lastErrorCode !== null &&
            (previous.lastErrorCode !== 'REPLICA_RELEASE_FAILED' ||
              p.lastErrorCode === 'REPLICA_RELEASE_FAILED') &&
            /^[A-Z0-9_]{1,64}$/.test(p.lastErrorCode);
          const release = !opening && p.deletedAt !== null;
          if (release) {
            this.assertReleaseCommit(event, original);
          }
          if (
            event.memberId !== artifact.memberId ||
            event.accountId !== null ||
            (opening
              ? previous !== null
              : artifact.retiredAt !== null ||
                (!release && p.deletedAt !== null) ||
                p.objectKey !== location.objectKey ||
                p.encoding !== location.encoding ||
                (previous !== null &&
                  (previous.objectKey !== p.objectKey || previous.encoding !== p.encoding)) ||
                previous?.deletedAt != null ||
                !(release || firstReservation || firstLocal || upload || verification || failed))
          ) {
            fail('副本来源、编码或状态迁移不一致');
          }
          const conflict = this.sqlite
            .prepare(
              'SELECT 1 FROM artifact_replicas r JOIN artifacts a ON a.id=r.artifact_id WHERE r.storage_target_id=? AND r.object_key=? AND r.deleted_at IS NULL AND (a.sha256!=? OR a.byte_length!=? OR r.encoding!=?) LIMIT 1',
            )
            .get(p.storageTargetId, p.objectKey, artifact.sha256, artifact.byteLength, p.encoding);
          if (p.deletedAt === null && conflict) {
            fail('同一物理对象的内容或编码冲突');
          }
          this.sqlite
            .prepare(
              `INSERT INTO artifact_replicas(artifact_id,storage_target_id,object_key,encoding,uploaded_at,verified_at,deleted_at,last_error_code,source_event_id) VALUES(?,?,?,?,?,?,?,?,?)
          ON CONFLICT(artifact_id,storage_target_id) DO UPDATE SET uploaded_at=excluded.uploaded_at,verified_at=excluded.verified_at,deleted_at=excluded.deleted_at,last_error_code=excluded.last_error_code,source_event_id=excluded.source_event_id`,
            )
            .run(
              p.artifactId,
              p.storageTargetId,
              p.objectKey,
              p.encoding,
              p.uploadedAt,
              p.verifiedAt,
              p.deletedAt,
              p.lastErrorCode,
              event.eventId,
            );
        } else {
          fail('副本投影不能应用另一 owner 的事实');
        }
      }
    });
  }
  replayReplicaCommit(stored: StoredRelationalCommit) {
    return this.owned(() => {
      if (stored.events.some((e) => !relationalReplicaFacts.has(e.eventType))) {
        fail('副本回放必须保留原完整提交');
      }
      const result = replayRelationalCommit(this.sqlite, stored);
      if (!result.replayed) {
        this.applyReplicaEvents(result.events);
      }
      return result;
    });
  }
}
