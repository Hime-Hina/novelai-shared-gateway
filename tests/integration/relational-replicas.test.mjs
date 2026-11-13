import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { readFile } from 'node:fs/promises';
import { brotliDecompressSync } from 'node:zlib';
import { getPatcher } from 'webdav';
import { fixture, meta, NOW, image, databaseState } from '../helpers/relational-generation.mjs';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import {
  copyRelationalArtifactReplica,
  verifyRelationalArtifactReplica,
} from '../../server/storage/relational-replica-io.ts';
import { R2Storage } from '../../server/storage/r2-storage.ts';
import { PikPakStorage } from '../../server/storage/pikpak-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';

async function setup(t, second = false) {
  const f = await fixture(t);
  await f.accept('replica');
  f.dispatch('replica-0');
  await f.publish('replica-0');
  if (second) {
    await f.accept('second', { memberId: 'm2', now: NOW + 20000 });
    f.dispatch('second-0', NOW + 20001);
    await f.publish('second-0', NOW + 20003);
  }
  const r2 = new R2Storage({
      accountId: 'a'.repeat(32),
      bucket: 'replica-images',
      accessKeyId: 'private-access',
      secretAccessKey: 'private-secret',
    }),
    pikpak = new PikPakStorage({
      url: 'https://dav.pikpak.test/archive/',
      username: 'private-user',
      password: 'private-password',
    }),
    controls = {},
    objects = new Map(),
    calls = [];
  const configuration = { dataDir: f.dir, r2, pikpak, idle: () => !controls.busy };
  const store = new RelationalReplicaStore(f.db, configuration);
  getPatcher().patch('fetch', (...args) => globalThis.fetch(...args));
  t.after(() => getPatcher().restore('fetch'));
  t.mock.method(globalThis, 'fetch', async (address, options) => {
    const url = new URL(address),
      method = options.method,
      cold = url.hostname === 'dav.pikpak.test';
    assert.equal(f.db.inTransaction, false);
    assert.ok(options.signal);
    assert.notEqual(method, 'DELETE', '当前副本片没有源删除能力');
    if (cold) {
      const current = configuration.pikpak.config;
      assert.equal(
        new Headers(options.headers).get('authorization'),
        'Basic ' + Buffer.from(`${current.username}:${current.password}`).toString('base64'),
      );
    }
    calls.push(`${cold ? 'pikpak' : 'r2'}:${method}`);
    if (method === 'PUT') {
      if (controls.failUpload) {
        return new Response(null, { status: 503 });
      }
      const exists = objects.has(url.href);
      if (cold || !exists) {
        objects.set(url.href, Buffer.from(options.body));
      }
      controls.afterUpload?.(cold);
      if (controls.loseUploadReply) {
        controls.loseUploadReply = false;
        throw new Error('synthetic lost upload acknowledgement');
      }
      return new Response(null, { status: !cold && exists ? 412 : 204 });
    }
    assert.equal(method, 'GET');
    if (controls.failRead) {
      return new Response(null, { status: 503 });
    }
    if (!objects.has(url.href)) {
      return new Response(null, { status: 404 });
    }
    const bytes = controls.corruptRead ? Buffer.from('corrupted') : objects.get(url.href);
    return new Response(bytes, { headers: { 'content-length': String(bytes.length) } });
  });
  return {
    ...f,
    store,
    configuration,
    controls,
    objects,
    calls,
    copy: (kind, artifactId = 'artifact-replica-0') =>
      copyRelationalArtifactReplica(f.db, artifactId, kind, configuration, () => NOW + 30000),
    verify: (targetId, artifactId = 'artifact-replica-0') =>
      verifyRelationalArtifactReplica(f.db, artifactId, targetId, configuration, () => NOW + 40000),
    target: (kind) => store.readConfiguredStorageTarget(kind).storageTargetId,
  };
}
function record(f, id, ticket, phase, error = null) {
  return f.db.transaction(() =>
    f.store.recordArtifactReplica(meta(id, NOW + 30000), ticket, phase, error),
  )();
}
function register(f, kind, id = `target-${kind}`) {
  return f.db.transaction(() => f.store.registerStorageTarget(meta(id), kind))();
}

test('Local→R2 保留原图并分别确认上传/回读，PikPak 实际使用 br 且上传不冒充 verified', async (t) => {
  const f = await setup(t);
  const hot = await f.copy('r2');
  assert.equal(hot.encoding, 'identity');
  assert.equal(hot.uploadedAt, NOW + 30000);
  assert.equal(hot.verifiedAt, NOW + 30000);
  assert.deepEqual(f.calls, ['r2:PUT', 'r2:GET']);
  const cold = await f.copy('pikpak');
  assert.equal(cold.encoding, 'br');
  assert.match(cold.objectKey, /^[a-f0-9]{64}\.br$/);
  assert.equal(cold.uploadedAt, NOW + 30000);
  assert.equal(cold.verifiedAt, null);
  assert.deepEqual(f.calls, ['r2:PUT', 'r2:GET', 'r2:GET', 'pikpak:PUT']);
  const compressed = [...f.objects.entries()].find(([url]) =>
    url.startsWith(f.configuration.pikpak.config.url),
  )[1];
  assert.deepEqual(brotliDecompressSync(compressed), image);
  assert.deepEqual(await readFile(path.join(f.dir, 'images/artifact-replica-0.webp')), image);
  assert.equal(
    [...f.objects.keys()].filter((key) => key.includes('.r2.cloudflarestorage.com/')).length,
    1,
  );
  const before = databaseState(f.db),
    calls = f.calls.length;
  await f.copy('r2');
  await f.copy('pikpak');
  assert.equal(databaseState(f.db), before);
  assert.equal(f.calls.length, calls, '已确认的命令不重复网络上传');
  assert.deepEqual(await f.verify(cold.storageTargetId), image);
  assert.equal(
    f.store.readArtifactReplica(cold.artifactId, cold.storageTargetId).verifiedAt,
    NOW + 40000,
  );
  const text =
    JSON.stringify(f.db.prepare('SELECT payload FROM domain_events').all()) +
    JSON.stringify(f.db.prepare('SELECT receipt FROM commits').all());
  for (const secret of [
    'private-secret',
    'private-access',
    'private-password',
    'private-user',
    'synthetic-upstream-pst',
    'private generation prompt',
  ]) {
    assert.equal(text.includes(secret), false);
  }
  assert.deepEqual(f.db.pragma('foreign_key_check'), []);
});

test('跨成员相同内容的独立产物可引用同一 PikPak 对象，不改归属或新增持久引用计数', async (t) => {
  const f = await setup(t, true);
  for (const id of ['artifact-replica-0', 'artifact-second-0']) {
    await f.copy('r2', id);
    await f.copy('pikpak', id);
  }
  assert.deepEqual(
    f.db
      .prepare(
        "SELECT a.member_id,r.object_key,r.encoding FROM artifact_replicas r JOIN artifacts a ON a.id=r.artifact_id JOIN storage_targets s ON s.id=r.storage_target_id WHERE s.kind='pikpak' ORDER BY a.member_id",
      )
      .all()
      .map((r) => ({ ...r, object_key: r.object_key.slice(64) })),
    [
      { member_id: 'm1', object_key: '.br', encoding: 'br' },
      { member_id: 'm2', object_key: '.br', encoding: 'br' },
    ],
  );
  assert.equal(
    f.db
      .prepare(
        "SELECT COUNT(DISTINCT r.object_key) AS n FROM artifact_replicas r JOIN storage_targets s ON s.id=r.storage_target_id WHERE s.kind='pikpak'",
      )
      .get().n,
    1,
  );
  assert.equal(
    [...f.objects.keys()].filter((url) => url.startsWith(f.configuration.pikpak.config.url)).length,
    1,
  );
});

test('IO owner 固定实际配置快照，调用方别名/属性不能把 A 客户端宣称为 B；轮换使用新实例', async (t) => {
  const f = await setup(t),
    r2Input = { ...f.configuration.r2.config },
    pikpakInput = { ...f.configuration.pikpak.config };
  f.configuration.r2 = new R2Storage(r2Input);
  f.configuration.pikpak = new PikPakStorage(pikpakInput);
  const boundR2 = f.configuration.r2,
    boundPikPak = f.configuration.pikpak;
  r2Input.bucket = 'wrong-images';
  r2Input.secretAccessKey = 'wrong-secret';
  pikpakInput.url = 'https://dav.pikpak.test/archive-b/';
  pikpakInput.password = 'wrong-password';
  assert.equal(boundR2.config.bucket, 'replica-images');
  assert.equal(boundPikPak.config.url, 'https://dav.pikpak.test/archive/');
  assert.throws(() => {
    boundPikPak.config.url = pikpakInput.url;
  }, TypeError);
  assert.throws(() => {
    boundPikPak.config.password = 'other';
  }, TypeError);
  assert.throws(() => {
    boundPikPak.config = pikpakInput;
  }, TypeError);
  assert.throws(() => {
    boundPikPak.client = new PikPakStorage(pikpakInput).client;
  }, TypeError);
  assert.throws(() => {
    boundR2.config.secretAccessKey = 'other';
  }, TypeError);
  assert.throws(() => {
    boundR2.config = r2Input;
  }, TypeError);
  await f.copy('r2');
  const a = await f.copy('pikpak');
  await f.verify(a.storageTargetId);
  assert.ok(f.calls.includes('pikpak:GET'));
  assert.ok([...f.objects.keys()].some((url) => url.startsWith(boundPikPak.config.url)));
  assert.equal(
    [...f.objects.keys()].some((url) => url.startsWith(pikpakInput.url)),
    false,
  );
  const oldTicket = f.store.beginArtifactReplica(a.artifactId, a.storageTargetId),
    before = databaseState(f.db);
  f.configuration.pikpak = new PikPakStorage({
    ...boundPikPak.config,
    url: pikpakInput.url,
    password: 'replacement-password',
  });
  assert.throws(() => record(f, 'old-instance', oldTicket, 'failed', 'REPLICA_IO_FAILED'), /配置/);
  assert.equal(databaseState(f.db), before);
  const b = await f.copy('pikpak');
  assert.notEqual(b.storageTargetId, a.storageTargetId);
  assert.deepEqual(await f.verify(b.storageTargetId), image);
  assert.ok([...f.objects.keys()].some((url) => url.startsWith(pikpakInput.url)));
});

test('PikPak 上传等待时替换目录或凭据实例，旧客户端结果不能确认到新配置', async (t) => {
  for (const change of ['namespace', 'credentials']) {
    await t.test(change, async (t) => {
      const f = await setup(t);
      await f.copy('r2');
      const originalTarget = f.target('pikpak');
      f.controls.afterUpload = (cold) => {
        if (cold) {
          f.configuration.pikpak = new PikPakStorage({
            ...f.configuration.pikpak.config,
            ...(change === 'namespace'
              ? { url: 'https://dav.pikpak.test/archive-b/' }
              : { password: 'rotated-password' }),
          });
        }
      };
      await assert.rejects(f.copy('pikpak'), /配置/);
      const row = f.store.readArtifactReplica('artifact-replica-0', originalTarget);
      assert.equal(row.uploadedAt, null);
      assert.equal(row.verifiedAt, null);
      assert.equal(
        [...f.objects.keys()].filter((url) => url.includes('.r2.cloudflarestorage.com/')).length,
        1,
      );
    });
  }
});

test('同一对象既有引用的错误编码会阻止新引用，错误原长度也不能被另一合法来源掩盖', async (t) => {
  const f = await setup(t, true);
  await f.copy('r2');
  await f.copy('r2', 'artifact-second-0');
  await f.copy('pikpak');
  const targetId = f.target('pikpak');
  f.db
    .prepare("UPDATE artifact_replicas SET encoding='identity' WHERE storage_target_id=?")
    .run(targetId);
  const before = databaseState(f.db);
  await assert.rejects(f.copy('pikpak', 'artifact-second-0'), /物理对象/);
  assert.equal(databaseState(f.db), before);
  f.db
    .prepare("UPDATE artifact_replicas SET encoding='br' WHERE storage_target_id=?")
    .run(targetId);
  f.db
    .prepare("UPDATE artifacts SET byte_length=byte_length+1 WHERE id='artifact-replica-0'")
    .run();
  const lengthConflict = databaseState(f.db);
  await assert.rejects(f.copy('pikpak', 'artifact-second-0'), /物理对象/);
  assert.equal(databaseState(f.db), lengthConflict);
  f.db
    .prepare("UPDATE artifacts SET byte_length=byte_length-1 WHERE id='artifact-replica-0'")
    .run();
  await f.copy('pikpak', 'artifact-second-0');
});

test('系统资格、配置和不可伪造票据先于回执；同键不同意图与旧票据均零写入', async (t) => {
  const f = await setup(t);
  const before = databaseState(f.db);
  f.db.transaction(() =>
    assert.throws(() => f.store.registerStorageTarget(meta('not-system', NOW, 'm1'), 'r2'), /后台/),
  )();
  assert.equal(databaseState(f.db), before);
  register(f, 'r2');
  const id = f.target('r2'),
    ticket = f.store.beginArtifactReplica('artifact-replica-0', id);
  let state = databaseState(f.db);
  f.db.transaction(() =>
    assert.throws(
      () => f.store.recordArtifactReplica(meta('forged'), { ...ticket }, 'reserved'),
      /票据/,
    ),
  )();
  assert.equal(databaseState(f.db), state);
  record(f, 'reserved', ticket, 'reserved');
  const bytes = f.db.serialize();
  assert.equal(record(f, 'reserved', ticket, 'reserved').replayed, true);
  assert.ok(f.db.serialize().equals(bytes));
  assert.throws(() => record(f, 'reserved', ticket, 'uploaded'), /idempotency/);
  const stale = f.store.beginArtifactReplica('artifact-replica-0', id);
  record(f, 'upload', f.store.beginArtifactReplica('artifact-replica-0', id), 'uploaded');
  state = databaseState(f.db);
  assert.throws(() => record(f, 'stale-failure', stale, 'failed', 'REPLICA_IO_FAILED'), /另一提交/);
  assert.equal(databaseState(f.db), state);
  f.controls.busy = true;
  assert.throws(() => register(f, 'r2'), /不可执行/);
  f.controls.busy = false;
  const current = f.store.beginArtifactReplica('artifact-replica-0', id);
  f.configuration.r2 = new R2Storage({
    ...f.configuration.r2.config,
    secretAccessKey: 'rotated-private-secret',
  });
  assert.throws(() => record(f, 'verify-old-config', current, 'verified'), /配置/);
  assert.equal(databaseState(f.db), state);
});

test('副本投影 SQL 故障在事实已写入后发生，外层捕获仍全表回滚且同命令可重试', async (t) => {
  const f = await setup(t);
  const targetBefore = databaseState(f.db);
  f.db.exec(
    "CREATE TEMP TRIGGER fail_target BEFORE INSERT ON storage_targets BEGIN SELECT RAISE(ABORT,'target fault'); END",
  );
  f.db.transaction(() => assert.throws(() => register(f, 'r2'), /target fault/))();
  assert.equal(databaseState(f.db), targetBefore);
  f.db.exec('DROP TRIGGER fail_target');
  register(f, 'r2');
  const ticket = f.store.beginArtifactReplica('artifact-replica-0', f.target('r2')),
    before = databaseState(f.db),
    eventCount = f.db.prepare('SELECT COUNT(*) AS n FROM domain_events').get().n;
  let reached = 0;
  f.db.function('saw_replica_fact', (count) => {
    assert.equal(count, eventCount + 1);
    reached++;
    return 1;
  });
  f.db.exec(
    "CREATE TEMP TRIGGER fail_replica BEFORE INSERT ON artifact_replicas BEGIN SELECT saw_replica_fact((SELECT COUNT(*) FROM domain_events)); SELECT RAISE(ABORT,'replica fault'); END",
  );
  f.db.transaction(() =>
    assert.throws(() => record(f, 'atomic-reserve', ticket, 'reserved'), /replica fault/),
  )();
  assert.equal(reached, 1);
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER fail_replica');
  record(f, 'atomic-reserve', ticket, 'reserved');
  const bytes = f.db.serialize();
  assert.equal(record(f, 'atomic-reserve', ticket, 'reserved').replayed, true);
  assert.ok(f.db.serialize().equals(bytes));
});

test('上传已落远端但确认 SQL 失败时不伪造回执，重试原对象后源和收费保持原值', async (t) => {
  const f = await setup(t);
  const charges = JSON.stringify(f.db.prepare('SELECT * FROM charges').all());
  f.db.exec(`CREATE TEMP TRIGGER fail_upload_receipt BEFORE INSERT ON domain_events
    WHEN NEW.event_type='ArtifactReplicaRecorded' AND json_extract(NEW.payload,'$.uploadedAt') IS NOT NULL AND json_extract(NEW.payload,'$.verifiedAt') IS NULL
    BEGIN SELECT RAISE(ABORT,'upload receipt fault'); END`);
  await assert.rejects(f.copy('r2'), /upload receipt fault/);
  const row = f.store.readArtifactReplica('artifact-replica-0', f.target('r2'));
  assert.equal(row.uploadedAt, null);
  assert.equal(row.verifiedAt, null);
  assert.equal(row.lastErrorCode, 'REPLICA_IO_FAILED');
  assert.equal(f.objects.size, 1);
  assert.deepEqual(await readFile(path.join(f.dir, 'images/artifact-replica-0.webp')), image);
  assert.equal(JSON.stringify(f.db.prepare('SELECT * FROM charges').all()), charges);
  f.db.exec('DROP TRIGGER fail_upload_receipt');
  assert.notEqual((await f.copy('r2')).verifiedAt, null);
});

test('关闭重开后由相同私有配置重领确认，不复制原票据、不再上传或冒充 PikPak 已核验', async (t) => {
  let reopened;
  t.after(() => {
    if (reopened?.open) {
      reopened.close();
    }
  });
  const f = await setup(t);
  await f.copy('r2');
  const cold = await f.copy('pikpak');
  const before = databaseState(f.db),
    calls = f.calls.length;
  f.db.close();
  reopened = new Database(path.join(f.dir, 'library.sqlite'));
  reopened.pragma('foreign_keys=ON');
  const configuration = {
    ...f.configuration,
    r2: new R2Storage({ ...f.configuration.r2.config }),
    pikpak: new PikPakStorage({ ...f.configuration.pikpak.config }),
  };
  await copyRelationalArtifactReplica(
    reopened,
    'artifact-replica-0',
    'r2',
    configuration,
    () => NOW + 50000,
  );
  const repeated = await copyRelationalArtifactReplica(
    reopened,
    'artifact-replica-0',
    'pikpak',
    configuration,
    () => NOW + 50000,
  );
  assert.equal(repeated.verifiedAt, null);
  assert.equal(repeated.uploadedAt, cold.uploadedAt);
  assert.equal(databaseState(reopened), before);
  assert.equal(f.calls.length, calls);
});

test('丢上传回执后按原哈希对象重试，R2 回读失败不标 verified 且从未删本地源', async (t) => {
  const f = await setup(t);
  f.controls.loseUploadReply = true;
  await assert.rejects(f.copy('r2'), /lost upload/);
  let row = f.store.readArtifactReplica('artifact-replica-0', f.target('r2'));
  assert.equal(row.uploadedAt, null);
  assert.equal(row.verifiedAt, null);
  assert.equal(row.lastErrorCode, 'REPLICA_IO_FAILED');
  f.controls.corruptRead = true;
  await assert.rejects(f.copy('r2'), /length mismatch|checksum/);
  row = f.store.readArtifactReplica('artifact-replica-0', f.target('r2'));
  assert.equal(row.uploadedAt, NOW + 30000);
  assert.equal(row.verifiedAt, null);
  assert.deepEqual(await readFile(path.join(f.dir, 'images/artifact-replica-0.webp')), image);
  f.controls.corruptRead = false;
  const puts = f.calls.filter((c) => c === 'r2:PUT').length;
  row = await f.copy('r2');
  assert.equal(row.verifiedAt, NOW + 30000);
  assert.equal(row.lastErrorCode, null);
  assert.equal(f.calls.filter((c) => c === 'r2:PUT').length, puts);
});

test('上传等待期间凭据或空闲资格变化，旧结果不写确认且源副本保留', async (t) => {
  for (const change of ['credentials', 'namespace', 'busy']) {
    await t.test(change, async (t) => {
      const f = await setup(t);
      const originalTarget = f.target('r2');
      f.controls.afterUpload = () => {
        if (change === 'credentials') {
          f.configuration.r2 = new R2Storage({
            ...f.configuration.r2.config,
            secretAccessKey: 'new-private-secret',
          });
        } else if (change === 'namespace') {
          f.configuration.r2 = new R2Storage({
            ...f.configuration.r2.config,
            bucket: 'other-images',
          });
        } else {
          f.controls.busy = true;
        }
      };
      await assert.rejects(f.copy('r2'), /配置|不可执行/);
      f.controls.busy = false;
      const row = f.store.readArtifactReplica('artifact-replica-0', originalTarget);
      assert.equal(row.uploadedAt, null);
      assert.equal(row.verifiedAt, null);
      assert.deepEqual(await readFile(path.join(f.dir, 'images/artifact-replica-0.webp')), image);
    });
  }
});

test('PikPak 上传或回读失败只保留确认级别，R2 原件和故障证据始终保留', async (t) => {
  const f = await setup(t);
  await f.copy('r2');
  f.controls.failUpload = true;
  await assert.rejects(f.copy('pikpak'), /503/);
  let cold = f.store.readArtifactReplica('artifact-replica-0', f.target('pikpak'));
  assert.equal(cold.uploadedAt, null);
  f.controls.failUpload = false;
  cold = await f.copy('pikpak');
  f.controls.corruptRead = true;
  await assert.rejects(f.verify(cold.storageTargetId));
  cold = f.store.readArtifactReplica(cold.artifactId, cold.storageTargetId);
  assert.equal(cold.uploadedAt, NOW + 30000);
  assert.equal(cold.verifiedAt, null);
  assert.equal(cold.lastErrorCode, 'REPLICA_IO_FAILED');
  f.controls.corruptRead = false;
  assert.deepEqual(await f.verify(cold.storageTargetId), image);
  assert.equal(
    [...f.objects.keys()].filter((key) => key.includes('.r2.cloudflarestorage.com/')).length,
    1,
  );
});

test('完整副本提交回放不访问 IO/私有配置，重复零写入；篡改核验顺序或编码仍整笔拒绝', async (t) => {
  const f = await setup(t);
  const checkpoint = f.db.serialize(),
    cutoff = f.db.prepare('SELECT MAX(commit_seq) AS n FROM commits').get().n;
  await f.copy('r2');
  await f.copy('pikpak');
  const sequences = f.db
    .prepare(
      'SELECT CAST(commit_seq AS TEXT) AS n FROM commits WHERE commit_seq>? ORDER BY commit_seq',
    )
    .all(cutoff)
    .map((r) => r.n);
  const target = new Database(checkpoint);
  t.after(() => target.close());
  target.pragma('foreign_keys=ON');
  const store = new RelationalReplicaStore(target),
    calls = f.calls.length;
  for (const seq of sequences) {
    const stored = readRelationalCommit(f.db, seq);
    const wrongActor = structuredClone(stored),
      actorBefore = databaseState(target);
    wrongActor.command.actorKind = 'member';
    wrongActor.command.actorId = 'm1';
    target.transaction(() =>
      assert.throws(() => store.replayReplicaCommit(wrongActor), /完整提交/),
    )();
    assert.equal(databaseState(target), actorBefore);
    if (
      stored.events[0].eventType === 'ArtifactReplicaRecorded' &&
      stored.events[0].payload.uploadedAt === null
    ) {
      for (const change of ['premature-verification', 'wrong-encoding']) {
        const bad = structuredClone(stored),
          before = databaseState(target);
        for (const event of [bad.events[0], bad.command.events[0]]) {
          if (change === 'premature-verification') {
            event.payload.uploadedAt = event.payload.verifiedAt = event.occurredAt;
          } else {
            event.payload.encoding = 'gzip';
          }
        }
        target.transaction(() =>
          assert.throws(() => store.replayReplicaCommit(bad), /来源|编码|状态/),
        )();
        assert.equal(databaseState(target), before);
      }
    }
    target.transaction(() => store.replayReplicaCommit(stored))();
  }
  assert.equal(databaseState(target), databaseState(f.db));
  const bytes = target.serialize();
  for (const seq of sequences) {
    target.transaction(() =>
      assert.equal(store.replayReplicaCommit(readRelationalCommit(f.db, seq)).replayed, true),
    )();
  }
  assert.ok(target.serialize().equals(bytes));
  assert.equal(f.calls.length, calls);
  target.transaction(() =>
    assert.throws(
      () => store.replayReplicaCommit(readRelationalCommit(f.db, String(cutoff))),
      /完整提交/,
    ),
  )();
});
