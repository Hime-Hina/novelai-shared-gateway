import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { rename, readFile } from 'node:fs/promises';
import { getPatcher } from 'webdav';
import {
  fixture,
  meta,
  NOW,
  keys,
  image,
  databaseState,
} from '../helpers/relational-generation.mjs';
import { initializeFixtureConfiguration } from '../helpers/relational-configuration.mjs';
import { domainDigest } from '../../server/events/domain-events.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { relationalFactStream } from '../../server/events/relational-fact-contracts.ts';
import {
  RelationalCommitReplayer,
  relationalReplayFacts,
} from '../../server/storage/relational-replay.ts';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import {
  relationalArtifactReferenceAvailable,
  readRelationalArtifactContent,
  copyRelationalArtifactReplica,
  verifyRelationalArtifactReplica,
} from '../../server/storage/relational-replica-io.ts';
import { describeStoredResult, publishStoredResult } from '../../server/storage/result-files.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { relationalSnapshotSha256 } from '../../server/storage/relational-private-restore.ts';
import { R2Storage } from '../../server/storage/r2-storage.ts';
import { PikPakStorage } from '../../server/storage/pikpak-storage.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import { nativeNormalizationContext } from '../../server/protocols/native-image.ts';
import { normalizeNovelAiRequest } from '../../server/protocols/novelai-client.ts';
import {
  requestNormalizationDefaults,
  generationRequestFingerprint,
} from '../../server/generation/request-fingerprint.ts';
import { generationIdempotencyKeyHash } from '../../server/generation/admission.ts';

const origin = { sourceId: 'synthetic-opening', originalId: 'old-replica' };
function event(eventType, payload, memberId = null) {
  const kind = relationalFactStream(eventType);
  return {
    eventType,
    payload,
    memberId,
    accountId: null,
    streamId: `${kind}:${payload.artifactId ?? payload.storageTargetId ?? payload.sourceId}`,
    occurredAt: NOW,
    policyVersion: 'opening-policy',
    pricingVersion: 'opening-price',
  };
}
function command(db, id, events) {
  const { policyVersion: _policy, pricingVersion: _pricing, ...metadata } = meta(id);
  return {
    ...metadata,
    expectedVersions: Object.fromEntries(
      events.map((e) => [
        e.streamId,
        db
          .prepare(
            'SELECT CAST(stream_version AS TEXT) AS version FROM stream_heads WHERE stream_id=?',
          )
          .get(e.streamId)?.version ?? '0',
      ]),
    ),
    receipt: { opened: id },
    events,
  };
}
function append(f, id, events) {
  return f.db.transaction(() =>
    new RelationalCommitReplayer(f.db).appendCommit(command(f.db, id, events)),
  )();
}
function target(name = 'historical-images') {
  const namespace = `r2://${'a'.repeat(32)}/${name}`;
  return {
    storageTargetId: `r2:${domainDigest(namespace)}`,
    kind: 'r2',
    namespace,
    credentialRef: 'deployment:r2',
    origin: { ...origin, originalId: name },
  };
}
function opening(artifactId, storageTargetId, changes = {}, memberId = 'm1') {
  return event(
    'OpeningReplicaRecorded',
    {
      artifactId,
      storageTargetId,
      objectKey: 'old-layout/original-image.gz',
      encoding: 'gzip',
      uploadedAt: 0,
      verifiedAt: null,
      deletedAt: null,
      lastErrorCode: null,
      origin,
      ...changes,
    },
    memberId,
  );
}
async function published(t, second = false) {
  const f = await fixture(t);
  await f.accept('replica');
  f.dispatch('replica-0');
  await f.publish('replica-0');
  if (second) {
    await f.accept('second', { memberId: 'm2', now: NOW + 20000 });
    f.dispatch('second-0', NOW + 20001);
    await f.publish('second-0', NOW + 20003);
  }
  return { ...f, artifactId: 'artifact-replica-0' };
}
function empty(t) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  return db;
}
function rebuild(t, source) {
  const db = empty(t),
    replayer = new RelationalCommitReplayer(db);
  for (const { seq } of source.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    db.transaction(() =>
      replayer.replayCommit(readRelationalCommit(source.db, seq), source.inputs),
    )();
  }
  return db;
}

test('OpeningReplica 由原 owner 保存已知零、未知和原 locator；完整回放、重开与重复零写不做 IO/恢复私密', async (t) => {
  const f = await published(t),
    a = target(),
    b = target('uploaded-images'),
    c = target('deleted-images');
  const result = append(
    f,
    'opening-replicas',
    [a, b, c].flatMap((p, i) => [
      event('OpeningStorageTargetRecorded', p),
      opening(
        f.artifactId,
        p.storageTargetId,
        [
          { verifiedAt: 0 },
          { uploadedAt: null },
          { deletedAt: 0, lastErrorCode: 'historical-error' },
        ][i],
      ),
    ]),
  );
  assert.equal(relationalReplayFacts().has('OpeningReplicaRecorded'), true);
  const store = new RelationalReplicaStore(f.db);
  for (const p of [a, b, c]) {
    const row = store.readArtifactReplica(f.artifactId, p.storageTargetId);
    assert.equal(row.objectKey, 'old-layout/original-image.gz');
    assert.equal(row.encoding, 'gzip');
  }
  assert.equal(store.readArtifactReplica(f.artifactId, a.storageTargetId).verifiedAt, 0);
  assert.equal(store.readArtifactReplica(f.artifactId, b.storageTargetId).uploadedAt, null);
  assert.equal(store.readArtifactReplica(f.artifactId, c.storageTargetId).deletedAt, 0);
  const noNetwork = t.mock.method(globalThis, 'fetch', () => assert.fail('replay must not do IO'));
  const source = databaseState(f.db),
    targetDb = rebuild(t, f);
  assert.deepEqual(
    targetDb.prepare('SELECT * FROM artifact_replicas ORDER BY rowid').all(),
    f.db.prepare('SELECT * FROM artifact_replicas ORDER BY rowid').all(),
  );
  const before = databaseState(targetDb),
    original = readRelationalCommit(f.db, result.commitSeq);
  assert.equal(
    targetDb.transaction(() => new RelationalCommitReplayer(targetDb).replayCommit(original))()
      .replayed,
    true,
  );
  assert.equal(
    targetDb.transaction(() => new RelationalReplicaStore(targetDb).replayReplicaCommit(original))()
      .replayed,
    true,
  );
  assert.equal(databaseState(targetDb), before);
  const reopened = new Database(targetDb.serialize());
  t.after(() => reopened.close());
  assert.equal(
    new RelationalReplicaStore(reopened).readArtifactReplica(f.artifactId, a.storageTargetId)
      .verifiedAt,
    0,
  );
  for (const table of ['api_keys', 'member_sessions', 'upstream_credentials']) {
    assert.equal(targetDb.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0);
  }
  assert.equal(noNetwork.mock.callCount(), 0);
  assert.equal(databaseState(f.db), source);
  assert.throws(() => assertRelationalStorageReady(targetDb), /incomplete/);
});

test('完整 Opening envelope 拒绝错 source/member/account/target/顺序和覆盖，外层捕获仍原子且可合法重试', async (t) => {
  const f = await published(t),
    p = target(),
    replica = opening(f.artifactId, p.storageTargetId);
  const source = event('ImportSourceRegistered', {
    sourceId: 'new-source',
    sourceSha256: 'a'.repeat(64),
    sourceCommit: 'known-cut',
    cutoffSeq: '7',
    cutoffAt: 0,
    manifestSha256: 'b'.repeat(64),
    route: 'A',
  });
  const cases = [
    [
      'missing-source',
      [
        event('OpeningStorageTargetRecorded', p),
        opening(f.artifactId, p.storageTargetId, { origin: { ...origin, sourceId: 'missing' } }),
      ],
    ],
    [
      'late-source',
      [
        event('OpeningStorageTargetRecorded', p),
        opening(f.artifactId, p.storageTargetId, { origin: { ...origin, sourceId: 'new-source' } }),
        source,
      ],
    ],
    [
      'different-source',
      [
        source,
        event('OpeningStorageTargetRecorded', p),
        opening(f.artifactId, p.storageTargetId, { origin: { ...origin, sourceId: 'missing' } }),
      ],
    ],
    ['wrong-member', [event('OpeningStorageTargetRecorded', p), { ...replica, memberId: 'm2' }]],
    ['wrong-account', [event('OpeningStorageTargetRecorded', p), { ...replica, accountId: 'a' }]],
    [
      'wrong-target',
      [event('OpeningStorageTargetRecorded', p), opening(f.artifactId, 'missing-target')],
    ],
    ['late-target', [replica, event('OpeningStorageTargetRecorded', p)]],
    [
      'missing-artifact',
      [event('OpeningStorageTargetRecorded', p), opening('missing-artifact', p.storageTargetId)],
    ],
    [
      'forged-namespace',
      [
        event('OpeningStorageTargetRecorded', {
          ...p,
          namespace: `r2://${'b'.repeat(32)}/historical-images`,
        }),
        replica,
      ],
    ],
    ['duplicate-pair', [event('OpeningStorageTargetRecorded', p), replica, replica]],
    [
      'verified-without-upload',
      [
        event('OpeningStorageTargetRecorded', p),
        opening(f.artifactId, p.storageTargetId, { uploadedAt: null, verifiedAt: 0 }),
      ],
    ],
  ];
  for (const [name, events] of cases) {
    await t.test(name, () => {
      const before = databaseState(f.db);
      f.db.transaction(() =>
        assert.throws(() =>
          new RelationalCommitReplayer(f.db).appendCommit(command(f.db, name, events)),
        ),
      )();
      assert.equal(databaseState(f.db), before);
    });
  }
  const good = append(f, 'valid-opening', [event('OpeningStorageTargetRecorded', p), replica]);
  const once = databaseState(f.db);
  assert.throws(() => append(f, 'new-command-cannot-cover', [replica]), /状态迁移/);
  assert.equal(databaseState(f.db), once);
  assert.equal(
    f.db.transaction(() =>
      new RelationalCommitReplayer(f.db).replayCommit(readRelationalCommit(f.db, good.commitSeq)),
    )().replayed,
    true,
  );
});

test('第二行 SQL 故障在真实 INSERT 后回滚 facts/heads/receipts/replicas，外层继续而原合法完整提交可重试', async (t) => {
  const f = await published(t),
    a = target(),
    b = target('second-images');
  const events = [a, b].flatMap((p) => [
    event('OpeningStorageTargetRecorded', p),
    opening(f.artifactId, p.storageTargetId),
  ]);
  let inserted = 0;
  f.db.function('observe_replica_insert', () => ++inserted);
  f.db.exec(
    `CREATE TEMP TRIGGER fail_second_replica AFTER INSERT ON artifact_replicas BEGIN SELECT observe_replica_insert(); SELECT CASE WHEN NEW.storage_target_id='${b.storageTargetId}' THEN RAISE(ABORT,'synthetic second replica failure') END; END`,
  );
  const before = databaseState(f.db);
  f.db.transaction(() => {
    assert.throws(
      () => new RelationalCommitReplayer(f.db).appendCommit(command(f.db, 'sql-failure', events)),
      /second replica failure/,
    );
    assert.equal(inserted, 2);
  })();
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER fail_second_replica');
  assert.equal(append(f, 'sql-failure', events).replayed, false);
});

test('原完整 envelope 的窄 replay 首次投影与重复零写，截断拒绝后仍可恢复；成员不能冒充系统导入', async (t) => {
  const f = await published(t),
    p = target();
  const targetDb = new Database(f.db.serialize());
  t.after(() => targetDb.close());
  const events = [
    event('OpeningStorageTargetRecorded', p),
    opening(f.artifactId, p.storageTargetId),
  ];
  const beforeSource = databaseState(f.db);
  f.db.transaction(() =>
    assert.throws(
      () =>
        new RelationalCommitReplayer(f.db).appendCommit({
          ...command(f.db, 'member-import', events),
          actorKind: 'member',
          actorId: 'm1',
        }),
      /offline system importer/,
    ),
  )();
  assert.equal(databaseState(f.db), beforeSource);
  const result = append(f, 'complete-source', events),
    stored = readRelationalCommit(f.db, result.commitSeq);
  const truncated = structuredClone(stored);
  truncated.command.events.pop();
  truncated.events.pop();
  const store = new RelationalReplicaStore(targetDb),
    before = databaseState(targetDb);
  targetDb.transaction(() =>
    assert.throws(() => store.replayReplicaCommit(truncated), /complete|完整/i),
  )();
  assert.equal(databaseState(targetDb), before);
  assert.equal(targetDb.transaction(() => store.replayReplicaCommit(stored))().replayed, false);
  const once = databaseState(targetDb);
  assert.equal(targetDb.transaction(() => store.replayReplicaCommit(stored))().replayed, true);
  assert.equal(databaseState(targetDb), once);
  assert.deepEqual(
    targetDb.prepare('SELECT * FROM artifact_replicas ORDER BY rowid').all(),
    f.db.prepare('SELECT * FROM artifact_replicas ORDER BY rowid').all(),
  );
});

test('同物理对象可由相同内容跨成员引用，编码冲突拒绝；deleted 历史来源不占活对象承诺', async (t) => {
  const f = await published(t, true),
    p = target();
  append(f, 'shared-object', [
    event('OpeningStorageTargetRecorded', p),
    opening(f.artifactId, p.storageTargetId),
  ]);
  const before = databaseState(f.db);
  assert.throws(
    () =>
      append(f, 'wrong-encoding', [
        opening('artifact-second-0', p.storageTargetId, { encoding: 'identity' }, 'm2'),
      ]),
    /物理对象/,
  );
  assert.equal(databaseState(f.db), before);
  append(f, 'same-content', [opening('artifact-second-0', p.storageTargetId, {}, 'm2')]);
  const tombstone = target('deleted-source-images');
  append(f, 'deleted-source', [
    event('OpeningStorageTargetRecorded', tombstone),
    opening(f.artifactId, tombstone.storageTargetId, { deletedAt: 0 }),
  ]);
  append(f, 'live-new-encoding', [
    opening('artifact-second-0', tombstone.storageTargetId, { encoding: 'identity' }, 'm2'),
  ]);
  assert.equal(
    new RelationalReplicaStore(f.db).readArtifactReplica(f.artifactId, tombstone.storageTargetId)
      .deletedAt,
    0,
  );
  // 另一真实正常生成的 PNG 有独立内容/长度，不靠改 artifact SQL 造冲突。
  await f.accept('different', { now: NOW + 40000 });
  f.dispatch('different-0', NOW + 40001);
  const png = Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=',
    'base64',
  );
  const descriptor = describeStoredResult('artifact-different-0', 'generate', {
    mimeType: 'image/png',
    buffer: png,
  });
  f.db.transaction(() =>
    f.generations.prepareGenerationResult(
      meta('prepare-different', NOW + 40003),
      'different-0',
      descriptor,
      f.response(NOW + 40003),
    ),
  )();
  await publishStoredResult(f.dir, descriptor, png);
  f.db.transaction(() =>
    f.generations.completeGeneration(
      meta('complete-different', NOW + 40004),
      'different-0',
      descriptor,
    ),
  )();
  const once = databaseState(f.db);
  assert.throws(
    () => append(f, 'wrong-content', [opening(descriptor.imageId, p.storageTargetId)]),
    /物理对象/,
  );
  assert.equal(databaseState(f.db), once);
  // 墓碑本身不声称对象仍承诺这些字节，允许与另一活来源不同。
  append(f, 'different-content-tombstone', [
    opening(descriptor.imageId, p.storageTargetId, { deletedAt: NOW - 1 }),
  ]);
});

test('历史 locator 不允许后台票据或普通 UPSERT 假称移位，cache 持久资格仍 true，实际领取 503 零 IO/零写', async (t) => {
  const f = await published(t),
    r2 = new R2Storage({
      accountId: 'a'.repeat(32),
      bucket: 'historical-images',
      accessKeyId: 'synthetic',
      secretAccessKey: 'synthetic',
    }),
    p = target();
  append(f, 'legacy-locator', [
    event('OpeningStorageTargetRecorded', p),
    opening(f.artifactId, p.storageTargetId, { verifiedAt: 0 }),
  ]);
  const configuration = { dataDir: f.dir, r2, idle: () => true },
    store = new RelationalReplicaStore(f.db, configuration);
  const get = t.mock.method(r2, 'get', () => assert.fail('unsupported locator cannot GET')),
    put = t.mock.method(r2, 'put', () => assert.fail('unsupported locator cannot PUT'));
  await rename(
    path.join(f.dir, 'images', `${f.artifactId}.webp`),
    path.join(f.dir, 'images', `${f.artifactId}.saved`),
  );
  const before = databaseState(f.db);
  assert.equal(await relationalArtifactReferenceAvailable(f.db, f.artifactId, configuration), true);
  await assert.rejects(readRelationalArtifactContent(f.db, f.artifactId, configuration), {
    statusCode: 503,
  });
  assert.throws(() => store.beginArtifactReplica(f.artifactId, p.storageTargetId), /定位或编码/);
  await assert.rejects(
    copyRelationalArtifactReplica(f.db, f.artifactId, 'r2', configuration),
    /定位或编码/,
  );
  const row = opening(f.artifactId, p.storageTargetId, {
    objectKey: `images/${f.artifactId}/${store.readArtifact(f.artifactId).sha256}`,
    encoding: 'identity',
    verifiedAt: 0,
    lastErrorCode: 'REPLICA_IO_FAILED',
  });
  const { origin: _origin, ...normal } = row.payload;
  assert.throws(
    () => append(f, 'false-relocation', [event('ArtifactReplicaRecorded', normal, 'm1')]),
    /来源、编码或状态/,
  );
  assert.equal(databaseState(f.db), before);
  assert.equal(get.mock.callCount(), 0);
  assert.equal(put.mock.callCount(), 0);
  assert.deepEqual(await readFile(path.join(f.dir, 'images', `${f.artifactId}.saved`)), image);
});

test('canonical Opening 上传可沿原 owner 实际回读后 verified，旧来源行篡改被读核对拒绝', async (t) => {
  const f = await published(t),
    p = target(),
    r2 = new R2Storage({
      accountId: 'a'.repeat(32),
      bucket: 'historical-images',
      accessKeyId: 'synthetic',
      secretAccessKey: 'synthetic',
    });
  const store = new RelationalReplicaStore(f.db, { dataDir: f.dir, r2, idle: () => true });
  append(f, 'canonical-upload', [
    event('OpeningStorageTargetRecorded', p),
    opening(f.artifactId, p.storageTargetId, {
      objectKey: `images/${f.artifactId}/${store.readArtifact(f.artifactId).sha256}`,
      encoding: 'identity',
    }),
  ]);
  const get = t.mock.method(r2, 'get', async (object) => {
    assert.equal(f.db.inTransaction, false);
    assert.equal(object.key, `images/${f.artifactId}/${store.readArtifact(f.artifactId).sha256}`);
    return image;
  });
  await verifyRelationalArtifactReplica(
    f.db,
    f.artifactId,
    p.storageTargetId,
    { dataDir: f.dir, r2, idle: () => true },
    () => NOW + 30000,
  );
  assert.equal(get.mock.callCount(), 1);
  assert.equal(store.readArtifactReplica(f.artifactId, p.storageTargetId).verifiedAt, NOW + 30000);
  const before = databaseState(f.db);
  assert.throws(
    () =>
      f.db.transaction(() => {
        f.db
          .prepare(
            'UPDATE artifact_replicas SET object_key=? WHERE artifact_id=? AND storage_target_id=?',
          )
          .run('forged-current-row', f.artifactId, p.storageTargetId);
        store.readArtifactReplica(f.artifactId, p.storageTargetId);
      })(),
    /当前行与原事实/,
  );
  assert.equal(databaseState(f.db), before);
  assert.equal(
    rebuild(t, f)
      .prepare('SELECT verified_at FROM artifact_replicas WHERE storage_target_id=?')
      .get(p.storageTargetId).verified_at,
    NOW + 30000,
  );
});

test('canonical Opening tombstone 在共同票据入口拒绝源/目的复制和核验，真实客户端零 HTTP/零 SQL', async (t) => {
  for (const role of ['destination', 'source', 'completed']) {
    await t.test(role, async (t) => {
      const f = await published(t),
        p = target();
      const configuration = {
        dataDir: f.dir,
        idle: () => true,
        r2: new R2Storage({
          accountId: 'a'.repeat(32),
          bucket: 'historical-images',
          accessKeyId: 'synthetic',
          secretAccessKey: 'synthetic',
        }),
        pikpak: new PikPakStorage({
          url: 'https://dav.pikpak.test/archive/',
          username: 'synthetic',
          password: 'synthetic',
        }),
      };
      const store = new RelationalReplicaStore(f.db, configuration),
        artifact = store.readArtifact(f.artifactId),
        calls = [],
        objects = new Map();
      getPatcher().patch('fetch', (...args) => globalThis.fetch(...args));
      t.after(() => getPatcher().restore('fetch'));
      t.mock.method(globalThis, 'fetch', async (address, options) => {
        assert.equal(f.db.inTransaction, false);
        calls.push(options.method);
        if (options.method === 'PUT') {
          objects.set(String(address), Buffer.from(options.body));
          return new Response(null, { status: 204 });
        }
        assert.equal(options.method, 'GET');
        return new Response(image, { headers: { 'content-length': String(image.length) } });
      });
      const deleted = opening(f.artifactId, p.storageTargetId, {
        objectKey: `images/${f.artifactId}/${artifact.sha256}`,
        encoding: 'identity',
        uploadedAt: role !== 'destination' ? 0 : null,
        verifiedAt: role !== 'destination' ? 0 : null,
        deletedAt: 0,
      });
      const events = [event('OpeningStorageTargetRecorded', p), deleted];
      if (role !== 'destination') {
        const cold = store.readConfiguredStorageTarget('pikpak');
        events.push(
          event('OpeningStorageTargetRecorded', { ...cold, origin }),
          opening(f.artifactId, cold.storageTargetId, {
            objectKey: `${artifact.sha256}.br`,
            encoding: 'br',
            uploadedAt: role === 'completed' ? 0 : null,
          }),
        );
      }
      append(f, `deleted-${role}`, events);
      const before = databaseState(f.db);
      if (role === 'completed') {
        configuration.r2 = null;
        const copied = await copyRelationalArtifactReplica(
          f.db,
          f.artifactId,
          'pikpak',
          configuration,
        );
        assert.equal(copied.uploadedAt, 0);
        assert.equal(copied.verifiedAt, null);
        assert.equal(store.readArtifactCacheReference(f.artifactId).persistent, true);
        assert.deepEqual(calls, []);
        assert.equal(databaseState(f.db), before);
        return;
      }
      assert.equal(store.readArtifactCacheReference(f.artifactId).persistent, false);
      assert.throws(
        () => store.beginArtifactReplica(f.artifactId, p.storageTargetId),
        /已删除副本/,
      );
      await assert.rejects(
        copyRelationalArtifactReplica(
          f.db,
          f.artifactId,
          role === 'source' ? 'pikpak' : 'r2',
          configuration,
          () => NOW + 30000,
        ),
        /已删除副本/,
      );
      await assert.rejects(
        verifyRelationalArtifactReplica(
          f.db,
          f.artifactId,
          p.storageTargetId,
          configuration,
          () => NOW + 30000,
        ),
        /已删除副本/,
      );
      assert.deepEqual(calls, []);
      assert.equal(objects.size, 0);
      assert.equal(databaseState(f.db), before);
      assert.deepEqual(await readFile(path.join(f.dir, 'images', `${f.artifactId}.webp`)), image);
    });
  }
});

test('Pik 来源预检票据在真实 R2 GET 后仍复核原配置，源换凭据不上传且真实 IO 失败仍归目的', async (t) => {
  const f = await published(t),
    p = target();
  const configuration = {
    dataDir: f.dir,
    idle: () => true,
    r2: new R2Storage({
      accountId: 'a'.repeat(32),
      bucket: 'historical-images',
      accessKeyId: 'synthetic',
      secretAccessKey: 'synthetic',
    }),
    pikpak: new PikPakStorage({
      url: 'https://dav.pikpak.test/archive/',
      username: 'synthetic',
      password: 'synthetic',
    }),
  };
  const store = new RelationalReplicaStore(f.db, configuration),
    artifact = store.readArtifact(f.artifactId),
    cold = store.readConfiguredStorageTarget('pikpak');
  append(f, 'live-source-before-get', [
    event('OpeningStorageTargetRecorded', p),
    opening(f.artifactId, p.storageTargetId, {
      objectKey: `images/${f.artifactId}/${artifact.sha256}`,
      encoding: 'identity',
      uploadedAt: 0,
      verifiedAt: 0,
    }),
    event('OpeningStorageTargetRecorded', { ...cold, origin }),
    opening(f.artifactId, cold.storageTargetId, {
      objectKey: `${artifact.sha256}.br`,
      encoding: 'br',
      uploadedAt: null,
    }),
  ]);
  const beforeSource = store.readArtifactReplica(f.artifactId, p.storageTargetId),
    calls = [];
  getPatcher().patch('fetch', (...args) => globalThis.fetch(...args));
  t.after(() => getPatcher().restore('fetch'));
  t.mock.method(globalThis, 'fetch', async (_address, options) => {
    assert.equal(f.db.inTransaction, false);
    calls.push(options.method);
    assert.equal(options.method, 'GET');
    configuration.r2 = new R2Storage({ ...configuration.r2.config, accessKeyId: 'rotated-access' });
    return new Response(image, { headers: { 'content-length': String(image.length) } });
  });
  await assert.rejects(
    copyRelationalArtifactReplica(f.db, f.artifactId, 'pikpak', configuration, () => NOW + 30000),
    /来源或配置已变化/,
  );
  assert.deepEqual(calls, ['GET']);
  assert.deepEqual(store.readArtifactReplica(f.artifactId, p.storageTargetId), beforeSource);
  const destination = store.readArtifactReplica(f.artifactId, cold.storageTargetId);
  assert.equal(destination.uploadedAt, null);
  assert.equal(destination.verifiedAt, null);
  assert.equal(destination.lastErrorCode, 'REPLICA_IO_FAILED');
});

test('退休产物的 Opening 副本只保全历史，不复活缓存/读取或删原件', async (t) => {
  const f = await published(t),
    env = { OWNER_USER_ID: 'm1' },
    p = target();
  withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    (actor) =>
      new RelationalReplicaStore(f.db, undefined, env).retireArtifactHistory(
        meta('retire-original', NOW + 90000, 'm1'),
        [f.artifactId],
        actor,
        new Map(),
      ),
    env,
  );
  append(f, 'retired-replica', [
    event('OpeningStorageTargetRecorded', p),
    opening(f.artifactId, p.storageTargetId, { verifiedAt: 0 }),
  ]);
  const store = new RelationalReplicaStore(f.db);
  assert.equal(store.readArtifactReplica(f.artifactId, p.storageTargetId).verifiedAt, 0);
  assert.throws(() => store.readArtifactCacheReference(f.artifactId), /退休/);
  await assert.rejects(readRelationalArtifactContent(f.db, f.artifactId, { dataDir: f.dir }), {
    statusCode: 503,
  });
  assert.deepEqual(await readFile(path.join(f.dir, 'images', `${f.artifactId}.webp`)), image);
  assert.equal(
    rebuild(t, f).prepare('SELECT retired_at FROM artifacts WHERE id=?').get(f.artifactId)
      .retired_at,
    NOW + 90000,
  );
});

test('旧 locator 持久承诺使真实 R08 新旧 key 继续引用同结果；领取 503 不生成新 attempt、不改旧回执', async (t) => {
  const f = await fixture(t, { seconds: 0 });
  const context = {
    kind: 'direct',
    native: nativeNormalizationContext(),
    defaults: requestNormalizationDefaults({}),
  };
  const request = normalizeNovelAiRequest(
    {
      tag: 'cache promise',
      model: 'nai-diffusion-4-5-full',
      width: 128,
      height: 128,
      steps: 30,
      seed: 42,
    },
    context.defaults,
  );
  const authorize = (run) => withAuthenticatedMemberApiKey(f.members, keys.m1, run, {});
  authorize(() => {
    const intent = f.inputs.generationInputIntent('m1', request, context, 'fixed', true);
    f.generations.acceptGeneration(
      {
        ...meta('accept-cache-source', NOW, 'm1'),
        fingerprint: generationRequestFingerprint(request, { randomSeed: false, noCache: true }),
      },
      {
        memberId: 'm1',
        inputId: 'input-cache-source',
        requestId: 'cache-source',
        source: 'direct',
        request,
        context,
        seedMode: 'fixed',
        confirmation: null,
        idempotency: { keyHash: generationIdempotencyKeyHash('original-producer'), noCache: true },
        jobs: [
          {
            jobId: 'cache-source-0',
            status: 'pending',
            queueSeq: null,
            queueExpiresAt: null,
            deadlineAt: null,
            admissionEpoch: 0,
            cacheKey: intent.cacheKey,
          },
        ],
      },
    );
  });
  f.dispatch('cache-source-0');
  await f.publish('cache-source-0');
  const artifactId = 'artifact-cache-source-0',
    p = target();
  append(f, 'cache-legacy-replica', [
    event('OpeningStorageTargetRecorded', p),
    opening(artifactId, p.storageTargetId, { verifiedAt: 0 }),
  ]);
  await rename(
    path.join(f.dir, 'images', `${artifactId}.webp`),
    path.join(f.dir, 'images', `${artifactId}.saved`),
  );
  const noNetwork = t.mock.method(globalThis, 'fetch', () =>
    assert.fail('cache acceptance must not do network'),
  );
  const configuration = { dataDir: f.dir },
    lookup = {
      memberId: 'm1',
      request,
      context,
      seedMode: 'fixed',
      noCache: false,
      allowActive: false,
    };
  const reuse = (id) =>
    authorize(() =>
      f.generations.reuseGeneration(
        {
          ...meta(`reuse-${id}`, NOW, 'm1'),
          fingerprint: generationRequestFingerprint(request, { randomSeed: false, noCache: false }),
        },
        {
          memberId: 'm1',
          request,
          context,
          seedMode: 'fixed',
          noCache: false,
          sourceJobId: 'cache-source-0',
          idempotency: { keyHash: generationIdempotencyKeyHash(id), noCache: false },
          cached: {
            requestId: id,
            jobId: `${id}-0`,
            inputId: `input-${id}`,
            source: 'direct',
            artifactId,
          },
        },
      ),
    );
  const beforeLookup = databaseState(f.db);
  assert.equal(
    (await f.generations.findReusableGeneration(lookup, configuration)).artifactId,
    artifactId,
  );
  assert.equal(databaseState(f.db), beforeLookup);
  const first = await reuse('first-cache');
  assert.equal(first.receipt.cacheHit, true);
  const once = databaseState(f.db);
  assert.equal((await reuse('first-cache')).replayed, true);
  await assert.rejects(readRelationalArtifactContent(f.db, artifactId, configuration), {
    statusCode: 503,
  });
  assert.equal(databaseState(f.db), once);
  assert.equal(
    (await f.generations.findReusableGeneration(lookup, configuration)).artifactId,
    artifactId,
  );
  assert.equal((await reuse('second-cache')).receipt.cacheHit, true);
  const final = databaseState(f.db);
  await assert.rejects(readRelationalArtifactContent(f.db, artifactId, configuration), {
    statusCode: 503,
  });
  assert.equal(databaseState(f.db), final);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_attempts').get().n, 1);
  assert.equal(noNetwork.mock.callCount(), 0);
  rebuild(t, f);
});

test('sole ready 必须核对真实可读副本；仅旧 locator 承诺失败保留源/目标，另有原 Local 才可安装', async (t) => {
  const f = await published(t),
    p = target(),
    r2 = new R2Storage({
      accountId: 'a'.repeat(32),
      bucket: 'historical-images',
      accessKeyId: 'synthetic',
      secretAccessKey: 'synthetic',
    });
  const get = t.mock.method(r2, 'get', () =>
    assert.fail('unsupported historical locator cannot GET'),
  );
  append(f, 'ready-old-locator', [
    event('OpeningStorageTargetRecorded', p),
    opening(f.artifactId, p.storageTargetId, { verifiedAt: 0 }),
  ]);
  initializeFixtureConfiguration(f.db);
  const sourcePath = path.join(f.dir, 'selected-opening-replica.sqlite');
  await f.db.backup(sourcePath);
  const sealed = new Database(sourcePath);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  const selected = {
    sourcePath,
    sourceSha256: relationalSnapshotSha256(sourcePath),
    historyId: 'generation-history',
    finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get()
      .seq,
    replicas: { dataDir: path.join(f.dir, 'no-local-source'), r2 },
  };
  const targetDb = rebuild(t, f),
    before = databaseState(targetDb);
  await assert.rejects(
    verifyAndInstallRelationalStorage(targetDb, selected),
    /readable original bytes/,
  );
  assert.equal(databaseState(targetDb), before);
  assert.equal(relationalSnapshotSha256(sourcePath), selected.sourceSha256);
  assert.throws(() => assertRelationalStorageReady(targetDb), /incomplete/);
  const installed = await verifyAndInstallRelationalStorage(targetDb, {
    ...selected,
    replicas: { dataDir: f.dir, r2 },
  });
  assert.equal(installed.artifactsVerified, 1);
  assertRelationalStorageReady(targetDb);
  assert.equal(get.mock.callCount(), 0);
  assert.deepEqual(await readFile(path.join(f.dir, 'images', `${f.artifactId}.webp`)), image);
});
