import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { rename, writeFile } from 'node:fs/promises';
import { fixture, NOW, image, databaseState } from '../helpers/relational-generation.mjs';
import {
  relationalArtifactReferenceAvailable,
  readRelationalArtifactContent,
  copyRelationalArtifactReplica,
} from '../../server/storage/relational-replica-io.ts';
import { R2Storage } from '../../server/storage/r2-storage.ts';

async function published(t) {
  const f = await fixture(t, { seconds: 0 });
  await f.accept('reference');
  f.dispatch('reference-0');
  await f.publish('reference-0');
  const artifactId = f.db.prepare('SELECT id FROM artifacts').get().id;
  return { ...f, artifactId, file: path.join(f.dir, 'images', `${artifactId}.webp`) };
}

test('引用资格沿安全非空Local，坏内容留待领取核验；真缺失无持久来源返回false且SQL零写', async (t) => {
  const f = await published(t),
    before = databaseState(f.db);
  assert.equal(
    await relationalArtifactReferenceAvailable(f.db, f.artifactId, { dataDir: f.dir }),
    true,
  );
  assert.deepEqual(
    (await readRelationalArtifactContent(f.db, f.artifactId, { dataDir: f.dir })).buffer,
    image,
  );
  await writeFile(f.file, Buffer.from('bad but nonempty'));
  assert.equal(
    await relationalArtifactReferenceAvailable(f.db, f.artifactId, { dataDir: f.dir }),
    true,
  );
  await assert.rejects(readRelationalArtifactContent(f.db, f.artifactId, { dataDir: f.dir }), {
    statusCode: 503,
  });
  await rename(f.file, f.file + '.missing');
  assert.equal(
    await relationalArtifactReferenceAvailable(f.db, f.artifactId, { dataDir: f.dir }),
    false,
  );
  assert.equal(databaseState(f.db), before);
  let pending;
  f.db.transaction(() => {
    pending = relationalArtifactReferenceAvailable(f.db, f.artifactId, { dataDir: f.dir });
  })();
  await assert.rejects(pending, /cannot hold a SQL transaction/);
});

test('已确认R2承诺在无配置/错桶时仍可引用，资格不GET，实际领取503且不借用错误目标', async (t) => {
  const f = await published(t),
    r2 = new R2Storage({
      accountId: 'a'.repeat(32),
      bucket: 'reference-images',
      accessKeyId: 'synthetic',
      secretAccessKey: 'synthetic-secret',
    });
  const put = t.mock.method(r2, 'put', async () => {}),
    get = t.mock.method(r2, 'get', async () => Buffer.from(image));
  await copyRelationalArtifactReplica(
    f.db,
    f.artifactId,
    'r2',
    { dataDir: f.dir, r2, idle: () => true },
    () => NOW + 30000,
  );
  assert.equal(put.mock.callCount(), 1);
  const oldReads = get.mock.callCount();
  await rename(f.file, f.file + '.missing');
  const wrong = new R2Storage({ ...r2.config, bucket: 'wrong-images' }),
    wrongGet = t.mock.method(wrong, 'get', async () => assert.fail('wrong target cannot be read')),
    before = databaseState(f.db);
  for (const client of [null, wrong]) {
    const configuration = { dataDir: f.dir, r2: client };
    assert.equal(
      await relationalArtifactReferenceAvailable(f.db, f.artifactId, configuration),
      true,
    );
    await assert.rejects(readRelationalArtifactContent(f.db, f.artifactId, configuration), {
      statusCode: 503,
    });
    assert.equal(databaseState(f.db), before);
  }
  assert.equal(get.mock.callCount(), oldReads);
  assert.equal(wrongGet.mock.callCount(), 0);
  assert.deepEqual(
    (await readRelationalArtifactContent(f.db, f.artifactId, { dataDir: f.dir, r2 })).buffer,
    image,
  );
  assert.equal(databaseState(f.db), before);
});
