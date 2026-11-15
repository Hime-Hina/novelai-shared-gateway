import type Database from 'better-sqlite3';
import { lstat, open, unlink } from 'node:fs/promises';
import path from 'node:path';
import { domainDigest } from '../events/domain-events.ts';
import {
  publishStoredResult,
  readStoredResultContent,
  assertStoredResultContent,
  storedResultReferenceAvailable,
} from './result-files.ts';
import {
  RelationalReplicaStore,
  hasArtifactReplicaPresence,
  type RelationalReplicaConfiguration,
  type RelationalReplicaReadConfiguration,
} from './relational-replica-store.ts';
import {
  withRetainedArtifactRelease,
  assertRetainedArtifactReleasePermit,
  type RetainedArtifactReleasePermit,
} from './retained-sources.ts';

export async function relationalArtifactReferenceAvailable(
  sqlite: Database.Database,
  artifactId: string,
  configuration: RelationalReplicaReadConfiguration,
) {
  if (sqlite.inTransaction) {
    throw new Error('Cache reference IO cannot hold a SQL transaction.');
  }
  const store = new RelationalReplicaStore(sqlite, configuration);
  if (store.readArtifactCacheReference(artifactId).persistent) {
    return true;
  }
  const local = store.beginArtifactRead(artifactId, 'local');
  if (!local) {
    return false;
  }
  const available = await storedResultReferenceAvailable(
    configuration.dataDir,
    local.artifact.descriptor,
  );
  store.assertArtifactReplicaTicket(local.ticket);
  return available;
}

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
  throw Object.assign(new Error('No confirmed artifact replica has readable original bytes.'), {
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
  errorCode = 'REPLICA_IO_FAILED',
) {
  // 旧配置/并发提交使票据失效时，不用旧 IO 结果覆盖新状态；外部副本留给后续核对。
  try {
    record(sqlite, store, ticket, 'failed', now, errorCode);
  } catch {
    // 保留原异常，失败状态也必须通过同一票据及保存点守卫。
  }
}

// 本命令只复制和确认；源释放由下方持保留源 lease 的独立命令完成。
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
  if (artifact.retiredAt !== null) {
    throw new Error('Retired artifacts cannot start replica copies.');
  }
  const planned = store.readConfiguredStorageTarget(kind);
  let row = store.readArtifactReplica(artifactId, planned.storageTargetId);
  let source: { ticket: Ticket; key: string } | undefined;
  // 来源资格先于目的预留；没有开始 IO 的拒绝不冒充目的副本故障。
  // 已上传的 PikPak 重领只核原目的，不重新要求已经不参与本次 IO 的 R2。
  if (kind === 'pikpak' && !hasArtifactReplicaPresence(row)) {
    const sourceTarget = store.readConfiguredStorageTarget('r2'),
      sourceReplica = store.readArtifactReplica(artifactId, sourceTarget.storageTargetId);
    if (sourceReplica?.verifiedAt == null) {
      throw new Error('PikPak requires the original verified R2 source.');
    }
    source = {
      ticket: store.beginArtifactReplica(artifactId, sourceTarget.storageTargetId),
      key: sourceReplica.objectKey,
    };
  }
  const destination = target(sqlite, store, artifactId, kind, now);
  let ticket = store.beginArtifactReplica(artifactId, destination.storageTargetId);
  if (row && hasArtifactReplicaPresence(row)) {
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
      // Opening只读取已存在原图；不能借publisher补成新的Local发布/上传历史。
      const historical = artifact.source.eventType === 'OpeningArtifactRecorded';
      const content = historical
        ? await readStoredResultContent(configuration.dataDir, artifact.descriptor)
        : await publishStoredResult(configuration.dataDir, artifact.descriptor);
      if (!content) {
        throw new Error('Local artifact bytes are unavailable.');
      }
      store.assertArtifactReplicaTicket(localTicket);
      const localReplica = store.readArtifactReplica(artifactId, local.storageTargetId);
      if (!historical) {
        if (!localReplica) {
          record(sqlite, store, localTicket, 'local_verified', now);
        } else if (localReplica.lastErrorCode !== null) {
          record(sqlite, store, localTicket, 'verified', now);
        }
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
      const object = {
        bucket: configuration.r2!.config.bucket,
        key: source!.key,
        sha256: artifact.sha256,
        byteLength: artifact.byteLength,
      };
      const bytes = await configuration.r2!.get(object);
      assertStoredResultContent(artifact.descriptor, bytes);
      store.assertArtifactReplicaTicket(source!.ticket);
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
  if (!replica || !hasArtifactReplicaPresence(replica)) {
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
        },
      });
    } else {
      throw new Error('Replica encoding is not supported by this IO owner.');
    }
    assertStoredResultContent(artifact.descriptor, bytes);
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

type Release = ReturnType<RelationalReplicaStore['beginArtifactRelease']>;
async function localRelease(
  sqlite: Database.Database,
  store: RelationalReplicaStore,
  release: Release,
  permit: RetainedArtifactReleasePermit,
  now: () => number,
) {
  const source = release.claim.source;
  if (source.kind !== 'local' || !source.dataDir || source.namespace !== 'images') {
    throw new Error('Local source release requires its original physical root.');
  }
  const directory = path.resolve(source.dataDir, source.namespace),
    filename = path.resolve(directory, source.objectKey),
    originalDirectory = await lstat(directory, { bigint: true });
  if (
    path.dirname(filename) !== directory ||
    !originalDirectory.isDirectory() ||
    originalDirectory.isSymbolicLink()
  ) {
    throw new Error('Local source release requires a regular original image directory.');
  }
  const stat = () =>
    lstat(filename, { bigint: true }).catch((error: NodeJS.ErrnoException) => {
      if (error.code === 'ENOENT') {
        return null;
      }
      throw error;
    });
  const original = await stat();
  if (original) {
    if (!original.isFile() || original.isSymbolicLink()) {
      throw new Error('Local source release requires a regular original file.');
    }
    // A retry may observe absence after DELETE/unlink succeeded but its SQL acknowledgement failed.
    // Existing bytes still have to match the original descriptor before they can be removed.
    if (!(await readStoredResultContent(source.dataDir, release.artifact.descriptor))) {
      throw new Error('Local source changed before release.');
    }
  }
  const current = await stat(),
    currentDirectory = await lstat(directory, { bigint: true });
  if (
    originalDirectory.dev !== currentDirectory.dev ||
    originalDirectory.ino !== currentDirectory.ino ||
    currentDirectory.isSymbolicLink() ||
    (original === null
      ? current !== null
      : !current ||
        current.dev !== original.dev ||
        current.ino !== original.ino ||
        current.size !== original.size ||
        current.mtimeNs !== original.mtimeNs ||
        current.ctimeNs !== original.ctimeNs)
  ) {
    throw new Error('Local source identity changed before release.');
  }
  store.assertArtifactReleaseTicket(release.ticket, now());
  assertRetainedArtifactReleasePermit(permit, release.claim);
  if (current) {
    try {
      await unlink(filename);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
        failed(sqlite, store, release.source.ticket, now, 'REPLICA_RELEASE_FAILED');
        throw error;
      }
    }
  }
  if (process.platform !== 'win32') {
    const handle = await open(directory, 'r');
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
  }
}

// The registry lease spans the physical operation and its acknowledgement. No SQL transaction spans
// IO. A failed acknowledgement cannot roll back external deletion; retry confirms absence instead.
export async function releaseRelationalArtifactReplica(
  sqlite: Database.Database,
  artifactId: string,
  kind: 'local' | 'r2',
  configuration: RelationalReplicaConfiguration,
  now = Date.now,
) {
  if (sqlite.inTransaction) {
    throw new Error('Replica release IO cannot hold a SQL transaction.');
  }
  if (!configuration.retainedSources) {
    return null;
  }
  if (!configuration.idle()) {
    throw new Error('Replica release requires the current idle background.');
  }
  const store = new RelationalReplicaStore(sqlite, configuration),
    source = store.readConfiguredStorageTarget(kind),
    current = store.readArtifactReplica(artifactId, source.storageTargetId);
  if (current?.deletedAt != null) {
    return current;
  }
  const release = store.beginArtifactRelease(artifactId, kind, now());
  return withRetainedArtifactRelease(
    configuration.retainedSources,
    release.claim,
    async (permit) => {
      store.assertArtifactReleaseTicket(release.ticket, now());
      assertRetainedArtifactReleasePermit(permit, release.claim);
      if (kind === 'local') {
        await localRelease(sqlite, store, release, permit, now);
      } else {
        try {
          await configuration.r2!.removeImage({
            bucket: configuration.r2!.config.bucket,
            key: release.source.replica.objectKey,
            sha256: release.artifact.sha256,
            byteLength: release.artifact.byteLength,
            verifiedAt: new Date(release.source.replica.verifiedAt!).toISOString(),
            pikpak: {
              url: release.destination.target.namespace,
              uploadedAt: new Date(release.destination.replica.uploadedAt!).toISOString(),
            },
          });
        } catch (error) {
          // The remote may have deleted the object before its reply was lost. Preserve that unknown
          // attempt and retry DELETE after the original delay; do not PUT or re-verify it as a new copy.
          failed(sqlite, store, release.source.ticket, now, 'REPLICA_RELEASE_FAILED');
          throw error;
        }
      }
      sqlite
        .transaction(() =>
          store.recordArtifactRelease(
            metadata(
              store,
              artifactId,
              `release:${release.source.target.storageTargetId}:${release.destination.target.storageTargetId}`,
              `${release.source.replica.sourceEventId}:${release.destination.replica.sourceEventId}`,
              now,
            ),
            release.ticket,
            permit,
          ),
        )
        .immediate();
      return store.readArtifactReplica(artifactId, source.storageTargetId)!;
    },
    { r2: configuration.r2 ?? undefined },
  );
}
