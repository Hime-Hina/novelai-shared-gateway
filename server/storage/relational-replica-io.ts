import type Database from 'better-sqlite3';
import { domainDigest } from '../events/domain-events.ts';
import {
  publishStoredResult,
  readStoredResultContent,
  assertStoredResultContent,
} from './result-files.ts';
import {
  RelationalReplicaStore,
  type RelationalReplicaConfiguration,
  type RelationalReplicaReadConfiguration,
} from './relational-replica-store.ts';

// 领取只验证承诺副本，不借用后台确认命令，也不修复或删除任何来源。
export async function readRelationalArtifactContent(
  sqlite: Database.Database,
  artifactId: string,
  configuration: RelationalReplicaReadConfiguration,
) {
  if (sqlite.inTransaction) {
    throw new Error('Artifact reads cannot hold a transaction across network or file IO.');
  }
  const store = new RelationalReplicaStore(sqlite, configuration),
    original = store.readArtifact(artifactId);
  for (const kind of ['local', 'r2', 'pikpak'] as const) {
    try {
      const source = store.beginArtifactRead(artifactId, kind);
      if (!source || source.artifact.sourceEventId !== original.sourceEventId) {
        continue;
      }
      const { artifact, target, replica } = source;
      let buffer: Buffer;
      if (kind === 'local') {
        const content = await readStoredResultContent(configuration.dataDir, artifact.descriptor);
        if (!content) {
          continue;
        }
        buffer = content.buffer;
      } else if (kind === 'r2' && configuration.r2) {
        const client = configuration.r2;
        buffer = await client.get({
          bucket: client.config.bucket,
          key: replica.objectKey,
          sha256: artifact.sha256,
          byteLength: artifact.byteLength,
        });
      } else if (kind === 'pikpak' && configuration.pikpak) {
        const client = configuration.pikpak;
        buffer = await client.get({
          key: `images/${artifact.id}/${artifact.sha256}`,
          sha256: artifact.sha256,
          byteLength: artifact.byteLength,
          pikpak: {
            url: target.namespace,
            uploadedAt: new Date(replica.uploadedAt!).toISOString(),
          },
        });
      } else {
        continue;
      }
      assertStoredResultContent(artifact.descriptor, buffer);
      store.assertArtifactReplicaTicket(source.ticket);
      return { descriptor: artifact.descriptor, buffer, storageTargetId: target.storageTargetId };
    } catch {
      // 某承诺副本不可用时只尝试其他原来源；不把失败写回或泄漏私有 IO 错误。
    }
  }
  throw Object.assign(new Error('No verified artifact replica has readable original bytes.'), {
    statusCode: 503,
    code: 'ARTIFACT_CONTENT_UNAVAILABLE',
  });
}

type Ticket = ReturnType<RelationalReplicaStore['beginArtifactReplica']>;
type Phase = Parameters<RelationalReplicaStore['recordArtifactReplica']>[2];
function metadata(
  store: RelationalReplicaStore,
  artifactId: string,
  action: string,
  source: string | null,
  now: () => number,
) {
  const artifact = store.readArtifact(artifactId);
  const intent = { artifactId, action, source };
  return {
    commandId: `replica:${domainDigest(intent)}`,
    fingerprint: domainDigest(intent),
    actorKind: 'system' as const,
    actorId: null,
    recordedAt: now(),
    correlationId: artifact.source.correlationId,
    causationId: artifact.source.eventId,
    policyVersion: artifact.source.policyVersion,
    pricingVersion: artifact.source.pricingVersion,
  };
}
function target(
  sqlite: Database.Database,
  store: RelationalReplicaStore,
  artifactId: string,
  kind: 'local' | 'r2' | 'pikpak',
  now: () => number,
) {
  const value = store.readConfiguredStorageTarget(kind);
  // Local 目标可已随原发布提交登记；不再用另一命令重复建立同一来源。
  if (store.isStorageTargetRegistered(value.storageTargetId)) {
    return value;
  }
  // 全部署的目标登记命令不随某张产物重复建立；原回执仍先重核当前后台/配置。
  const command = metadata(store, artifactId, `target:${value.storageTargetId}`, null, now);
  command.commandId = `storage-target:${value.storageTargetId}`;
  command.fingerprint = domainDigest(value);
  sqlite.transaction(() => store.registerStorageTarget(command, kind)).immediate();
  return value;
}
function record(
  sqlite: Database.Database,
  store: RelationalReplicaStore,
  ticket: Ticket,
  phase: Phase,
  now: () => number,
  errorCode: string | null = null,
) {
  const previous = store.readArtifactReplica(ticket.artifactId, ticket.storageTargetId);
  return sqlite
    .transaction(() =>
      store.recordArtifactReplica(
        metadata(
          store,
          ticket.artifactId,
          `${ticket.storageTargetId}:${phase}`,
          previous?.sourceEventId || null,
          now,
        ),
        ticket,
        phase,
        errorCode,
      ),
    )
    .immediate();
}
function failed(
  sqlite: Database.Database,
  store: RelationalReplicaStore,
  ticket: Ticket,
  now: () => number,
) {
  // 旧配置/并发提交使票据失效时，不用旧 IO 结果覆盖新状态；外部副本留给后续核对。
  try {
    record(sqlite, store, ticket, 'failed', now, 'REPLICA_IO_FAILED');
  } catch {
    // 保留原异常，失败状态也必须通过同一票据及保存点守卫。
  }
}

// 当前只复制和确认，保留所有源副本；归档引用闭包完成前没有删除入口。
export async function copyRelationalArtifactReplica(
  sqlite: Database.Database,
  artifactId: string,
  kind: 'r2' | 'pikpak',
  configuration: RelationalReplicaConfiguration,
  now = Date.now,
) {
  if (sqlite.inTransaction) {
    throw new Error('Replica IO cannot hold a transaction across network or file IO.');
  }
  const store = new RelationalReplicaStore(sqlite, configuration),
    artifact = store.readArtifact(artifactId);
  const destination = target(sqlite, store, artifactId, kind, now);
  let row = store.readArtifactReplica(artifactId, destination.storageTargetId);
  let ticket = store.beginArtifactReplica(artifactId, destination.storageTargetId);
  if (row && row.uploadedAt !== null) {
    if (kind === 'r2' && (row.verifiedAt === null || row.lastErrorCode !== null)) {
      await verifyRelationalArtifactReplica(
        sqlite,
        artifactId,
        destination.storageTargetId,
        configuration,
        now,
      );
    }
    return store.readArtifactReplica(artifactId, destination.storageTargetId)!;
  }
  if (!row) {
    record(sqlite, store, ticket, 'reserved', now);
    row = store.readArtifactReplica(artifactId, destination.storageTargetId)!;
  }
  ticket = store.beginArtifactReplica(artifactId, destination.storageTargetId);
  try {
    if (kind === 'r2') {
      const local = target(sqlite, store, artifactId, 'local', now),
        localTicket = store.beginArtifactReplica(artifactId, local.storageTargetId);
      const content = await publishStoredResult(configuration.dataDir, artifact.descriptor);
      if (!content) {
        throw new Error('Local artifact bytes are unavailable.');
      }
      store.assertArtifactReplicaTicket(localTicket);
      const localReplica = store.readArtifactReplica(artifactId, local.storageTargetId);
      if (!localReplica) {
        record(sqlite, store, localTicket, 'local_verified', now);
      } else if (localReplica.lastErrorCode !== null) {
        record(sqlite, store, localTicket, 'verified', now);
      }
      store.assertArtifactReplicaTicket(ticket);
      await configuration.r2!.put(
        {
          bucket: configuration.r2!.config.bucket,
          key: row.objectKey,
          sha256: artifact.sha256,
          byteLength: artifact.byteLength,
        },
        content.buffer,
        artifact.mimeType,
      );
    } else {
      const source = target(sqlite, store, artifactId, 'r2', now),
        sourceReplica = store.readArtifactReplica(artifactId, source.storageTargetId);
      if (sourceReplica?.verifiedAt == null) {
        throw new Error('PikPak requires the original verified R2 source.');
      }
      const sourceTicket = store.beginArtifactReplica(artifactId, source.storageTargetId);
      const object = {
        bucket: configuration.r2!.config.bucket,
        key: sourceReplica.objectKey,
        sha256: artifact.sha256,
        byteLength: artifact.byteLength,
      };
      const bytes = await configuration.r2!.get(object);
      store.assertArtifactReplicaTicket(sourceTicket);
      store.assertArtifactReplicaTicket(ticket);
      await configuration.pikpak!.put(object, bytes);
    }
    record(sqlite, store, ticket, 'uploaded', now);
  } catch (error) {
    failed(sqlite, store, ticket, now);
    throw error;
  }
  if (kind === 'r2') {
    await verifyRelationalArtifactReplica(
      sqlite,
      artifactId,
      destination.storageTargetId,
      configuration,
      now,
    );
  }
  return store.readArtifactReplica(artifactId, destination.storageTargetId)!;
}

export async function verifyRelationalArtifactReplica(
  sqlite: Database.Database,
  artifactId: string,
  storageTargetId: string,
  configuration: RelationalReplicaConfiguration,
  now = Date.now,
) {
  if (sqlite.inTransaction) {
    throw new Error('Replica IO cannot hold a transaction across network or file IO.');
  }
  const store = new RelationalReplicaStore(sqlite, configuration),
    artifact = store.readArtifact(artifactId),
    destination = store.readStorageTarget(storageTargetId),
    ticket = store.beginArtifactReplica(artifactId, storageTargetId),
    replica = store.readArtifactReplica(artifactId, storageTargetId);
  if (!replica || replica.uploadedAt === null || replica.deletedAt !== null) {
    throw new Error('Artifact replica has no confirmed upload.');
  }
  try {
    let bytes: Buffer;
    const object = {
      key: `images/${artifact.id}/${artifact.sha256}`,
      sha256: artifact.sha256,
      byteLength: artifact.byteLength,
    };
    if (destination.kind === 'r2' && replica.encoding === 'identity') {
      bytes = await configuration.r2!.get({ ...object, bucket: configuration.r2!.config.bucket });
    } else if (destination.kind === 'pikpak' && replica.encoding === 'br') {
      bytes = await configuration.pikpak!.get({
        ...object,
        pikpak: {
          url: destination.namespace,
          uploadedAt: new Date(replica.uploadedAt).toISOString(),
        },
      });
    } else {
      throw new Error('Replica encoding is not supported by this IO owner.');
    }
    store.assertArtifactReplicaTicket(ticket);
    if (replica.verifiedAt === null || replica.lastErrorCode !== null) {
      record(sqlite, store, ticket, 'verified', now);
    }
    return bytes;
  } catch (error) {
    failed(sqlite, store, ticket, now);
    throw error;
  }
}
