import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, realpath, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { fixture, NOW, image, databaseState } from '../helpers/relational-generation.mjs';
import { createRetainedReleaseFixture } from '../helpers/retained-release.mjs';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import * as io from '../../server/storage/relational-replica-io.ts';
import { R2Storage } from '../../server/storage/r2-storage.ts';
import { PikPakStorage } from '../../server/storage/pikpak-storage.ts';

// The fixed old IO can run the preceding actual copy lifecycle for the baseline counter. The current
// implementation exports the release command; no product fallback or test option is introduced.
const release =
  io.releaseRelationalArtifactReplica ??
  ((db, id, kind, configuration, now) =>
    io.copyRelationalArtifactReplica(
      db,
      id,
      kind === 'local' ? 'r2' : 'pikpak',
      configuration,
      now,
    ));
const LATER = NOW + 30 * 86400000;

async function setup(t) {
  let f;
  f = await fixture({
    after(fn) {
      t.after(async () => {
        const directory = await realpath(f.dir);
        assert.ok(
          directory.toLowerCase().startsWith((await realpath(tmpdir())).toLowerCase() + path.sep),
        );
        await fn();
      });
    },
  });
  await f.accept('release');
  f.dispatch('release-0');
  await f.publish('release-0');
  const r2 = new R2Storage({
    accountId: 'a'.repeat(32),
    bucket: 'release-images',
    accessKeyId: 'synthetic-access',
    secretAccessKey: 'synthetic-secret',
  });
  const pikpak = new PikPakStorage({
    url: 'https://dav.pikpak.test/release/',
    username: 'synthetic-user',
    password: 'synthetic-pass',
  });
  const calls = [];
  const objects = new Map();
  const controls = {};
  const configuration = { dataDir: f.dir, r2, pikpak, idle: () => !controls.busy };
  const store = new RelationalReplicaStore(f.db, configuration);
  t.mock.method(r2, 'listBackups', async () => []);
  t.mock.method(r2, 'put', async (object, bytes) => {
    assert.equal(f.db.inTransaction, false);
    calls.push('r2:PUT');
    objects.set(object.key, Buffer.from(bytes));
  });
  t.mock.method(r2, 'get', async (object) => {
    assert.equal(f.db.inTransaction, false);
    calls.push('r2:GET');
    assert.ok(objects.has(object.key));
    return objects.get(object.key);
  });
  t.mock.method(pikpak, 'put', async (object, bytes) => {
    assert.equal(f.db.inTransaction, false);
    calls.push('pikpak:PUT');
    objects.set(object.sha256 + '.br', Buffer.from(bytes));
    return bytes.length;
  });
  t.mock.method(pikpak, 'get', async (object) => {
    assert.equal(f.db.inTransaction, false);
    calls.push('pikpak:GET');
    return objects.get(object.sha256 + '.br');
  });
  t.mock.method(globalThis, 'fetch', async (address, options) => {
    assert.equal(f.db.inTransaction, false);
    assert.equal(options.method, 'DELETE');
    calls.push('r2:DELETE');
    if (controls.failDelete) {
      return new Response(null, { status: 503 });
    }
    const key = decodeURIComponent(new URL(address).pathname).slice('/release-images/'.length);
    const existed = objects.delete(key);
    if (controls.loseDeleteReply) {
      controls.loseDeleteReply = false;
      throw new Error('synthetic reply lost after actual deletion');
    }
    return new Response(null, { status: existed ? 204 : 404 });
  });
  const artifactId = 'artifact-release-0';
  const copy = (kind) =>
    io.copyRelationalArtifactReplica(f.db, artifactId, kind, configuration, () => NOW + 30000);
  const seal = async () => {
    const retained = await createRetainedReleaseFixture(t, f.db, f.dir, { r2 });
    configuration.retainedSources = retained.registry;
    return retained;
  };
  return {
    ...f,
    artifactId,
    configuration,
    store,
    calls,
    controls,
    objects,
    copy,
    seal,
    target: (kind) => store.readConfiguredStorageTarget(kind).storageTargetId,
    release: (kind, now = LATER) => release(f.db, artifactId, kind, configuration, () => now),
  };
}

test('原24h Local→R2只在持久核验和受保护inventory后释放，重复零IO/零写', async (t) => {
  const f = await setup(t);
  await f.copy('r2');
  await f.seal();
  assert.equal(f.store.readArtifactReplica(f.artifactId, f.target('r2')).verifiedAt, NOW + 30000);
  const before = [...f.calls];
  const deleted = await f.release('local');
  await assert.rejects(readFile(path.join(f.dir, 'images', f.artifactId + '.webp')), {
    code: 'ENOENT',
  });
  assert.equal(deleted.deletedAt, LATER);
  assert.deepEqual(
    f.calls,
    before,
    'release does not PUT or re-download a verified R2 destination',
  );
  const state = databaseState(f.db);
  assert.deepEqual(await f.release('local'), deleted);
  assert.equal(databaseState(f.db), state);
  assert.deepEqual(f.calls, before);
  f.controls.busy = true;
  await assert.rejects(f.release('local'));
});

test('原7d R2→Pik持久uploaded后DELETE，不增加立即PikGET；最后按原字节冷读', async (t) => {
  const f = await setup(t);
  await f.copy('r2');
  await f.copy('pikpak');
  await f.seal();
  await f.release('local');
  const before = [...f.calls];
  const deleted = await f.release('r2');
  assert.equal(deleted.deletedAt, LATER);
  assert.deepEqual(f.calls, [...before, 'r2:DELETE']);
  assert.equal(f.store.readArtifactReplica(f.artifactId, f.target('pikpak')).verifiedAt, null);
  assert.deepEqual(
    (await io.readRelationalArtifactContent(f.db, f.artifactId, f.configuration)).buffer,
    image,
  );
  assert.equal(f.calls.at(-1), 'pikpak:GET');
});

test('原R2-only快照不借current新增Pik解pin；后续Source捕获必须重新核全集', async (t) => {
  const f = await setup(t);
  await f.copy('r2');
  const retained = await f.seal();
  await f.copy('pikpak');
  await retained.addSnapshot();
  const before = [...f.calls];
  assert.equal(await f.release('r2'), null);
  assert.equal(f.store.readArtifactReplica(f.artifactId, f.target('r2')).deletedAt, null);
  assert.deepEqual(f.calls, before);
});

test('实际unlink后SQL尾错不伪造物理回滚；原命令整笔回滚，缺文件重试完成确认', async (t) => {
  const f = await setup(t);
  await f.copy('r2');
  await f.seal();
  const before = databaseState(f.db);
  const original = f.db.prepare;
  let reached = false;
  const hook = t.mock.method(f.db, 'prepare', function (sql) {
    const statement = original.call(this, sql);
    if (!/^\s*INSERT INTO artifact_replicas\(/.test(sql)) {
      return statement;
    }
    return new Proxy(statement, {
      get(target, key) {
        if (key === 'run') {
          return (...parameters) => {
            if (parameters[6] !== null) {
              reached = true;
              assert.equal(
                original
                  .call(
                    f.db,
                    "SELECT COUNT(*) AS n FROM domain_events WHERE event_type=? AND json_extract(payload,'$.deletedAt') IS NOT NULL",
                  )
                  .get('ArtifactReplicaRecorded').n,
                1,
              );
              throw new Error('release-delete-projection-tail');
            }
            return target.run(...parameters);
          };
        }
        const value = Reflect.get(target, key);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
  });
  try {
    await assert.rejects(f.release('local'), /release-delete-projection-tail/);
  } finally {
    hook.mock.restore();
  }
  assert.equal(reached, true);
  assert.equal(databaseState(f.db), before, 'outer catch sees the whole SQL command rolled back');
  await assert.rejects(readFile(path.join(f.dir, 'images', f.artifactId + '.webp')), {
    code: 'ENOENT',
  });
  assert.equal((await f.release('local', LATER + 1)).deletedAt, LATER + 1);
  assert.equal(f.calls.filter((call) => call === 'r2:PUT').length, 1);
});

test('DELETE实际完成但回执丢失，1h后只重试404确认，不回读/重PUT已移走的R2', async (t) => {
  const f = await setup(t);
  await f.copy('r2');
  await f.copy('pikpak');
  await f.seal();
  f.controls.loseDeleteReply = true;
  await assert.rejects(f.release('r2'), /synthetic reply lost/);
  assert.equal(
    f.store.readArtifactReplica(f.artifactId, f.target('r2')).lastErrorCode,
    'REPLICA_RELEASE_FAILED',
  );
  assert.equal(f.store.readDueArtifactReplica(LATER + 3600001), null);
  await assert.rejects(f.release('r2', LATER + 3600000));
  const before = [...f.calls];
  assert.equal((await f.release('r2', LATER + 3600001)).deletedAt, LATER + 3600001);
  assert.deepEqual(f.calls, [...before, 'r2:DELETE']);
});

test('原Local文件被同长其他内容替换时不unlink，不用目的存在掩盖源内容冲突', async (t) => {
  const f = await setup(t);
  await f.copy('r2');
  await f.seal();
  const filename = path.join(f.dir, 'images', f.artifactId + '.webp');
  const foreign = Buffer.from(image);
  foreign[foreign.length - 1] ^= 1;
  await writeFile(filename, foreign);
  const before = databaseState(f.db);
  const calls = [...f.calls];
  await assert.rejects(f.release('local'));
  assert.deepEqual(await readFile(filename), foreign);
  assert.equal(databaseState(f.db), before);
  assert.deepEqual(f.calls, calls);
});
