import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { rename, readFile, writeFile, stat } from 'node:fs/promises';
import { getPatcher } from 'webdav';
import { fixture, meta, NOW, image, databaseState } from '../helpers/relational-generation.mjs';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import {
  copyRelationalArtifactReplica,
  verifyRelationalArtifactReplica,
  readRelationalArtifactContent,
} from '../../server/storage/relational-replica-io.ts';
import { R2Storage } from '../../server/storage/r2-storage.ts';
import { PikPakStorage } from '../../server/storage/pikpak-storage.ts';

const artifactId = 'artifact-read-0',
  unavailable = { code: 'ARTIFACT_CONTENT_UNAVAILABLE', statusCode: 503 };
async function setup(t, hot = false, cold = false) {
  const f = await fixture(t);
  await f.accept('read');
  f.dispatch('read-0');
  await f.publish('read-0');
  const configuration = {
      dataDir: f.dir,
      r2: new R2Storage({
        accountId: 'a'.repeat(32),
        bucket: 'reader-images',
        accessKeyId: 'private-access',
        secretAccessKey: 'private-secret',
      }),
      pikpak: new PikPakStorage({
        url: 'https://dav.pikpak.test/reader/',
        username: 'private-user',
        password: 'private-password',
      }),
      idle: () => !controls.busy,
    },
    controls = {},
    objects = new Map(),
    calls = [];
  getPatcher().patch('fetch', (...args) => globalThis.fetch(...args));
  t.after(() => getPatcher().restore('fetch'));
  t.mock.method(globalThis, 'fetch', async (address, options) => {
    const url = new URL(address),
      cold = url.hostname === 'dav.pikpak.test';
    assert.equal(f.db.inTransaction, false);
    assert.ok(options.signal, '沿用真实 IO owner 的有界超时');
    calls.push(`${cold ? 'pikpak' : 'r2'}:${options.method}`);
    if (options.method === 'PUT') {
      objects.set(url.href, Buffer.from(options.body));
      return new Response(null, { status: 204 });
    }
    assert.equal(options.method, 'GET', '只读领取从未 DELETE');
    controls.onRead?.(cold);
    if ((cold ? controls.coldFailed : controls.hotFailed) || !objects.has(url.href)) {
      return new Response(null, { status: 404 });
    }
    const bytes = (cold ? controls.corruptCold : controls.corruptHot)
      ? Buffer.alloc(image.length)
      : objects.get(url.href);
    return new Response(bytes, { headers: { 'content-length': String(bytes.length) } });
  });
  if (hot) {
    await copyRelationalArtifactReplica(f.db, artifactId, 'r2', configuration, () => NOW + 30000);
  }
  if (cold) {
    await copyRelationalArtifactReplica(
      f.db,
      artifactId,
      'pikpak',
      configuration,
      () => NOW + 40000,
    );
  }
  const store = new RelationalReplicaStore(f.db, configuration),
    file = path.join(f.dir, `images/${artifactId}.webp`);
  return {
    ...f,
    store,
    configuration,
    objects,
    controls,
    calls,
    file,
    read: () => readRelationalArtifactContent(f.db, artifactId, configuration),
    hideLocal: () => rename(file, `${file}.saved`),
    target: (kind) => store.readConfiguredStorageTarget(kind).storageTargetId,
  };
}

test('封存只读 SQLite 和繁忙执行器仍可领取真实 Local，数据库与原文件不变', async (t) => {
  const f = await setup(t),
    readonly = new Database(path.join(f.dir, 'library.sqlite'), {
      readonly: true,
      fileMustExist: true,
    });
  f.controls.busy = true;
  const before = databaseState(f.db),
    file = await stat(f.file),
    configuration = { dataDir: f.dir };
  let result;
  try {
    result = await readRelationalArtifactContent(readonly, artifactId, configuration);
  } finally {
    readonly.close();
  }
  assert.deepEqual(result.buffer, image);
  assert.equal(result.descriptor.imageId, artifactId);
  assert.equal(f.store.readStorageTarget(result.storageTargetId).kind, 'local');
  assert.equal(databaseState(f.db), before);
  assert.equal((await stat(f.file)).mtimeMs, file.mtimeMs);
  assert.deepEqual(await readFile(f.file), image);
  assert.deepEqual(f.calls, []);
  let pending;
  f.db.transaction(() => {
    pending = f.read();
  })();
  await assert.rejects(pending, /cannot hold a transaction/);
});

test('缺失或坏 Local 回退原 R2，R2 不可读再回退实际 br PikPak；全部读取零写入', async (t) => {
  const f = await setup(t, true, true),
    cold = f.target('pikpak');
  await verifyRelationalArtifactReplica(f.db, artifactId, cold, f.configuration, () => NOW + 50000);
  await writeFile(f.file, Buffer.alloc(image.length));
  const before = databaseState(f.db),
    remote = JSON.stringify([...f.objects].map(([key, bytes]) => [key, bytes.toString('hex')]));
  f.calls.length = 0;
  const hot = await f.read();
  assert.deepEqual(hot.buffer, image);
  assert.equal(hot.storageTargetId, f.target('r2'));
  await f.hideLocal();
  f.controls.corruptHot = true;
  const recovered = await f.read();
  assert.deepEqual(recovered.buffer, image);
  assert.equal(recovered.storageTargetId, cold);
  assert.deepEqual(f.calls, ['r2:GET', 'r2:GET', 'pikpak:GET']);
  assert.equal(databaseState(f.db), before);
  assert.equal(
    JSON.stringify([...f.objects].map(([key, bytes]) => [key, bytes.toString('hex')])),
    remote,
  );
  assert.deepEqual(await readFile(`${f.file}.saved`), Buffer.alloc(image.length));
  f.controls.corruptCold = true;
  await assert.rejects(f.read(), unavailable, '真实 R2 校验和/Brotli 拒绝后没有其他合法承诺');
  assert.equal(databaseState(f.db), before);
});

test('只有 uploaded 的实际 PikPak 字节不冒充承诺，真实验证后才可只读领取', async (t) => {
  const f = await setup(t, true, true);
  await f.hideLocal();
  f.controls.hotFailed = true;
  const before = databaseState(f.db);
  f.calls.length = 0;
  await assert.rejects(f.read(), unavailable);
  assert.deepEqual(f.calls, ['r2:GET']);
  assert.equal(databaseState(f.db), before);
  f.controls.hotFailed = false;
  await verifyRelationalArtifactReplica(
    f.db,
    artifactId,
    f.target('pikpak'),
    f.configuration,
    () => NOW + 50000,
  );
  f.controls.hotFailed = true;
  assert.deepEqual((await f.read()).buffer, image);
});

test('错误编码/来源及配置目录不匹配拒绝，不按当前默认位置猜测或改写事实', async (t) => {
  const f = await setup(t, true);
  await f.hideLocal();
  const target = f.target('r2'),
    original = f.configuration.r2;
  f.configuration.r2 = new R2Storage({ ...original.config, bucket: 'other-images' });
  f.calls.length = 0;
  const changed = databaseState(f.db);
  await assert.rejects(f.read(), unavailable);
  assert.deepEqual(f.calls, []);
  assert.equal(databaseState(f.db), changed);
  f.configuration.r2 = original;
  f.db
    .prepare("UPDATE artifact_replicas SET encoding='gzip' WHERE storage_target_id=?")
    .run(target);
  const bad = databaseState(f.db);
  await assert.rejects(f.read(), unavailable);
  assert.deepEqual(f.calls, []);
  assert.equal(databaseState(f.db), bad);
  f.db
    .prepare("UPDATE artifact_replicas SET encoding='identity' WHERE storage_target_id=?")
    .run(target);
  assert.deepEqual((await f.read()).buffer, image);
});

test('IO 等待中 namespace/凭据、原副本来源或退休变化不能返回旧成功', async (t) => {
  for (const change of ['namespace', 'credentials', 'replica', 'target', 'retired']) {
    await t.test(change, async (t) => {
      const f = await setup(t, true);
      await f.hideLocal();
      f.controls.onRead = () => {
        if (change === 'namespace' || change === 'credentials') {
          f.configuration.r2 = new R2Storage({
            ...f.configuration.r2.config,
            ...(change === 'namespace'
              ? { bucket: 'other-images' }
              : { secretAccessKey: 'rotated' }),
          });
        } else if (change === 'retired') {
          f.db.prepare('UPDATE artifacts SET retired_at=? WHERE id=?').run(NOW + 50000, artifactId);
        } else {
          const published = f.db
            .prepare('SELECT source_event_id AS id FROM artifacts WHERE id=?')
            .get(artifactId).id;
          if (change === 'replica') {
            f.db
              .prepare(
                "UPDATE artifact_replicas SET source_event_id=? WHERE storage_target_id LIKE 'r2:%'",
              )
              .run(published);
          } else {
            f.db
              .prepare("UPDATE storage_targets SET source_event_id=? WHERE kind='r2'")
              .run(published);
          }
        }
      };
      await assert.rejects(f.read(), unavailable);
      const after = databaseState(f.db);
      f.controls.onRead = null;
      if (change === 'credentials') {
        assert.deepEqual((await f.read()).buffer, image, '新请求可使用当前合法凭据读取同一来源');
      } else {
        await assert.rejects(f.read(), unavailable);
      }
      assert.equal(databaseState(f.db), after, '读取没有额外业务事实或故障写入');
    });
  }
});

test('只读票据不能提交状态，后台写资格仍拒绝繁忙或未提供 idle 的配置', async (t) => {
  const f = await setup(t),
    source = f.store.beginArtifactRead(artifactId, 'local'),
    before = databaseState(f.db);
  f.db.transaction(() =>
    assert.throws(
      () =>
        f.store.recordArtifactReplica(
          meta('read-ticket', NOW + 50000),
          source.ticket,
          'failed',
          'READ_FAILED',
        ),
      /只读/,
    ),
  )();
  assert.equal(databaseState(f.db), before);
  f.controls.busy = true;
  assert.throws(
    () => f.store.beginArtifactReplica(artifactId, source.target.storageTargetId),
    /后台/,
  );
  assert.deepEqual((await f.read()).buffer, image);
  const reader = new RelationalReplicaStore(f.db, { dataDir: f.dir });
  f.db.transaction(() =>
    assert.throws(() => reader.registerStorageTarget(meta('no-idle'), 'local'), /后台/),
  )();
  assert.equal(databaseState(f.db), before);
});

test('产物枚举严格按 ID 游标有界、排除退休且不读取输入正文', async (t) => {
  const f = await setup(t);
  await f.accept('second', { now: NOW + 20000 });
  f.dispatch('second-0', NOW + 20001);
  await f.publish('second-0', NOW + 20003);
  const before = databaseState(f.db),
    first = f.store.readRecoverableArtifacts(null, 1);
  assert.deepEqual(
    first.map((row) => row.id),
    [artifactId],
  );
  assert.deepEqual(
    f.store.readRecoverableArtifacts(artifactId, 1).map((row) => row.id),
    ['artifact-second-0'],
  );
  assert.deepEqual(f.store.readRecoverableArtifacts('artifact-second-0', 100), []);
  for (const bound of [0, 101, 1.5, NaN]) {
    assert.throws(() => f.store.readRecoverableArtifacts(null, bound), /上界/);
  }
  assert.equal(databaseState(f.db), before);
  f.db.prepare('UPDATE artifacts SET retired_at=? WHERE id=?').run(NOW + 50000, artifactId);
  assert.deepEqual(
    f.store.readRecoverableArtifacts(null, 100).map((row) => row.id),
    ['artifact-second-0'],
  );
  await assert.rejects(f.read(), unavailable);
});
