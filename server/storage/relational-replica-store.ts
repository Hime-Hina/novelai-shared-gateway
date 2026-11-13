import path from 'node:path';
import type Database from 'better-sqlite3';
import { canonicalJson, domainDigest } from '../events/domain-events.ts';
import {
  appendRelationalCommit,
  readRelationalCommit,
  readRelationalReceipt,
  replayRelationalCommit,
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
  resultFile,
  type describeStoredResult,
} from './result-files.ts';
import { r2Config, type R2Storage } from './r2-storage.ts';
import { pikpakConfig, type PikPakStorage } from './pikpak-storage.ts';

type Metadata = Omit<RelationalCommand, 'events' | 'expectedVersions' | 'receipt'> & {
  policyVersion: string;
  pricingVersion: string;
};
type Kind = RelationalPayloads['StorageTargetRegistered']['kind'];
type Target = RelationalPayloads['StorageTargetRegistered'];
type Replica = RelationalPayloads['ArtifactReplicaRecorded'];
type Publication = ReturnType<typeof describeStoredResult>;
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
}
type Ticket = Readonly<{ artifactId: string; storageTargetId: string }>;
type Phase = 'reserved' | 'uploaded' | 'verified' | 'local_verified' | 'failed';
export const relationalReplicaFacts = new Set<RelationalEvent['eventType']>([
  'StorageTargetRegistered',
  'ArtifactReplicaRecorded',
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
  publication: Publication,
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

// 只拥有目标及副本事实；部署秘密、网络、文件及源副本删除不属于业务 replay。
export class RelationalReplicaStore {
  private sqlite: Database.Database;
  private configuration?: RelationalReplicaReadConfiguration;
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
  constructor(sqlite: Database.Database, configuration?: RelationalReplicaReadConfiguration) {
    assertRelationalStorageFormat(sqlite);
    this.sqlite = sqlite;
    this.configuration = configuration;
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
          operation: string;
          mimeType: string;
          byteLength: number;
          sha256: string;
          retiredAt: number | null;
          sourceEventId: string;
        }
      >(
        'SELECT id,member_id AS memberId,input_id AS inputId,operation,mime_type AS mimeType,byte_length AS byteLength,sha256,retired_at AS retiredAt,source_event_id AS sourceEventId FROM artifacts WHERE id=?',
      )
      .get(artifactId);
    if (!row) {
      fail('副本产物不存在');
    }
    const source = this.source(row.sourceEventId);
    if (
      (source.eventType !== 'ArtifactPublished' &&
        source.eventType !== 'OpeningArtifactRecorded') ||
      source.payload.artifactId !== row.id ||
      source.payload.memberId !== row.memberId ||
      source.payload.inputId !== row.inputId ||
      source.payload.operation !== row.operation ||
      source.payload.mimeType !== row.mimeType ||
      source.payload.byteLength !== row.byteLength ||
      source.payload.sha256 !== row.sha256
    ) {
      fail('副本产物与原发布事实不一致');
    }
    const descriptor = {
      imageId: row.id,
      file: resultFile({ imageId: row.id, operation: row.operation, mimeType: row.mimeType }),
      operation: row.operation,
      mimeType: row.mimeType,
      byteLength: row.byteLength,
      sha256: row.sha256,
    };
    assertStoredResultDescriptor(descriptor);
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
    if (
      source.eventType !== 'StorageTargetRegistered' ||
      canonicalJson(source.payload) !==
        canonicalJson({
          storageTargetId: row.storageTargetId,
          kind: row.kind,
          namespace: row.namespace,
          credentialRef: row.credentialRef,
        })
    ) {
      fail('副本目标与原登记事实不一致');
    }
    assertTarget(source.payload);
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
      if (
        source.eventType !== 'ArtifactReplicaRecorded' ||
        canonicalJson(source.payload) !== canonicalJson(payload) ||
        source.memberId !== this.readArtifact(artifactId).memberId
      ) {
        fail('副本当前行与原事实不一致');
      }
    }
    return row;
  }
  private append(metadata: Metadata, fact: ReplicaFact, memberId: string | null) {
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
  beginArtifactReplica(artifactId: string, storageTargetId: string): Ticket {
    this.active();
    return this.beginTicket(artifactId, storageTargetId, true);
  }
  beginArtifactRead(artifactId: string, kind: Kind) {
    const current = this.configuredTarget(kind);
    if (!this.isStorageTargetRegistered(current.target.storageTargetId)) {
      return null;
    }
    const replica = this.readArtifactReplica(artifactId, current.target.storageTargetId);
    if (
      !replica ||
      replica.uploadedAt === null ||
      replica.verifiedAt === null ||
      replica.deletedAt !== null
    ) {
      return null;
    }
    const artifact = this.readArtifact(artifactId),
      target = this.readStorageTarget(current.target.storageTargetId),
      location = replicaLocation(artifact.descriptor, kind);
    if (replica.objectKey !== location.objectKey || replica.encoding !== location.encoding) {
      fail('副本读取定位或编码不属于原产物');
    }
    return {
      artifact,
      target,
      replica,
      ticket: this.beginTicket(artifactId, target.storageTargetId, false),
    };
  }
  private beginTicket(artifactId: string, storageTargetId: string, background: boolean): Ticket {
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
    const ticket = Object.freeze({ artifactId, storageTargetId });
    this.tickets.set(ticket, {
      configuration: current.fingerprint,
      artifactSource: artifact.sourceEventId,
      targetSource: target.sourceEventId,
      replicaSource: this.readArtifactReplica(artifactId, storageTargetId)?.sourceEventId || null,
      background,
    });
    return ticket;
  }
  private checkedTicket(ticket: Ticket) {
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
    return { issued, artifact, target };
  }
  assertArtifactReplicaTicket(ticket: Ticket) {
    const { issued } = this.checkedTicket(ticket);
    if (
      (this.readArtifactReplica(ticket.artifactId, ticket.storageTargetId)?.sourceEventId ||
        null) !== issued.replicaSource
    ) {
      fail('副本票据已被另一提交替换');
    }
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
        if (target.kind !== 'local' || previous) {
          fail('本地副本已经登记或目标不符');
        }
        row.uploadedAt = row.verifiedAt = metadata.recordedAt;
      } else if (phase === 'uploaded') {
        if (!previous || previous.uploadedAt !== null) {
          fail('副本没有待确认的上传');
        }
        row.uploadedAt = metadata.recordedAt;
      } else if (phase === 'verified') {
        if (
          !previous ||
          previous.uploadedAt === null ||
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
          original.command.actorKind !== 'system' ||
          original.command.actorId !== null ||
          !source ||
          canonicalJson(source) !== canonicalJson(event)
        ) {
          fail('副本投影与原完整提交不符');
        }
        if (event.eventType === 'StorageTargetRegistered') {
          const p = event.payload;
          assertTarget(p);
          if (event.memberId !== null || event.accountId !== null) {
            fail('目标登记不能冒充成员或上游账户');
          }
          this.sqlite
            .prepare(
              'INSERT INTO storage_targets(id,kind,namespace,credential_ref,source_event_id) VALUES(?,?,?,?,?)',
            )
            .run(p.storageTargetId, p.kind, p.namespace, p.credentialRef, event.eventId);
        } else if (event.eventType === 'ArtifactReplicaRecorded') {
          const p = event.payload,
            artifact = this.readArtifact(p.artifactId),
            target = this.readStorageTarget(p.storageTargetId),
            previous = this.readArtifactReplica(p.artifactId, p.storageTargetId);
          const location = replicaLocation(artifact.descriptor, target.kind);
          const firstReservation =
            !previous &&
            target.kind !== 'local' &&
            p.uploadedAt === null &&
            p.verifiedAt === null &&
            p.lastErrorCode === null;
          const firstLocal =
            !previous &&
            target.kind === 'local' &&
            p.uploadedAt === event.occurredAt &&
            p.verifiedAt === event.occurredAt &&
            p.lastErrorCode === null;
          const upload =
            previous &&
            previous.uploadedAt === null &&
            p.uploadedAt === event.occurredAt &&
            p.verifiedAt === null &&
            p.lastErrorCode === null;
          const verification =
            previous &&
            previous.uploadedAt !== null &&
            p.uploadedAt === previous.uploadedAt &&
            p.lastErrorCode === null &&
            ((previous.verifiedAt === null && p.verifiedAt === event.occurredAt) ||
              (previous.verifiedAt !== null &&
                previous.lastErrorCode !== null &&
                p.verifiedAt === previous.verifiedAt));
          const failed =
            previous &&
            p.uploadedAt === previous.uploadedAt &&
            p.verifiedAt === previous.verifiedAt &&
            p.lastErrorCode !== null &&
            /^[A-Z0-9_]{1,64}$/.test(p.lastErrorCode);
          if (
            event.memberId !== artifact.memberId ||
            event.accountId !== null ||
            p.deletedAt !== null ||
            p.objectKey !== location.objectKey ||
            p.encoding !== location.encoding ||
            previous?.deletedAt != null ||
            !(firstReservation || firstLocal || upload || verification || failed)
          ) {
            fail('副本来源、编码或状态迁移不一致');
          }
          const conflict = this.sqlite
            .prepare(
              'SELECT 1 FROM artifact_replicas r JOIN artifacts a ON a.id=r.artifact_id WHERE r.storage_target_id=? AND r.object_key=? AND r.deleted_at IS NULL AND (a.sha256!=? OR a.byte_length!=? OR r.encoding!=?) LIMIT 1',
            )
            .get(p.storageTargetId, p.objectKey, artifact.sha256, artifact.byteLength, p.encoding);
          if (conflict) {
            fail('同一物理对象的内容或编码冲突');
          }
          this.sqlite
            .prepare(
              `INSERT INTO artifact_replicas(artifact_id,storage_target_id,object_key,encoding,uploaded_at,verified_at,deleted_at,last_error_code,source_event_id) VALUES(?,?,?,?,?,?,?,?,?)
          ON CONFLICT(artifact_id,storage_target_id) DO UPDATE SET uploaded_at=excluded.uploaded_at,verified_at=excluded.verified_at,last_error_code=excluded.last_error_code,source_event_id=excluded.source_event_id`,
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
