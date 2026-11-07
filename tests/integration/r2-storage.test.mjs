import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { brotliCompressSync } from 'node:zlib';
import { JsonStore } from '../../server/storage/sqlite-store.ts';
import { archiveColdImage } from '../../server/storage/cold-images.ts';
import { PikPakStorage } from '../../server/storage/pikpak-storage.ts';
import {
  ImageStorage,
  MAX_STORED_IMAGE_BYTES,
  R2Storage,
  R2_PIKPAK_THRESHOLD_BYTES,
  r2Config,
  signedR2Request,
} from '../../server/storage/r2-storage.ts';

const png = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=',
  'base64',
);
const digest = (bytes) => createHash('sha256').update(bytes).digest('hex');
const config = {
  accountId: 'a'.repeat(32),
  bucket: 'synthetic-images',
  accessKeyId: 'synthetic-access',
  secretAccessKey: 'synthetic-secret',
};
const now = Date.now(),
  cold = new Date(now - 86400001).toISOString();

async function fixture(t) {
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-r2-'));
  await mkdir(path.join(dataDir, 'images'));
  const store = new JsonStore(dataDir);
  await store.init();
  t.after(() => store.close());
  const objects = new Map(),
    calls = [],
    controls = { corrupt: false, fail: false, timeoutAfterPut: false };
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    assert.equal(url.hostname, `${config.accountId}.r2.cloudflarestorage.com`);
    assert.equal(options.redirect, 'error');
    assert.ok(options.signal);
    assert.match(
      options.headers.authorization,
      /AWS4-HMAC-SHA256 Credential=synthetic-access\/\d{8}\/auto\/s3\/aws4_request/,
    );
    calls.push(options.method);
    const key = decodeURIComponent(url.pathname);
    if (controls.fail) {
      return new Response('unavailable', { status: 503 });
    }
    if (options.method === 'PUT') {
      assert.equal(options.headers['if-none-match'], '*');
      assert.equal(options.headers['x-amz-content-sha256'], digest(options.body));
      if (objects.has(key)) {
        return new Response('', { status: 412 });
      }
      objects.set(key, Buffer.from(options.body));
      if (controls.timeoutAfterPut) {
        throw new Error('synthetic lost response');
      }
      return new Response(null, { status: 200 });
    }
    if (!objects.has(key)) {
      return new Response('', { status: 404 });
    }
    const bytes = Buffer.from(objects.get(key));
    if (controls.corrupt) {
      bytes[bytes.length - 1] ^= 1;
    }
    return new Response(bytes, { headers: { 'content-length': String(bytes.length) } });
  });
  async function seed(id, values = {}) {
    const image = {
      id,
      file: `images/${id}.png`,
      mimeType: 'image/png',
      createdAt: cold,
      fullPrompt: 'private generation parameters',
      model: 'nai-diffusion-5-full',
      sha256: digest(png),
      byteLength: png.length,
      ...values,
    };
    await writeFile(path.join(dataDir, image.file), png);
    await store.update((db) => db.images.push(image), {
      immediate: true,
      dirtyRows: { images: [id] },
    });
    return image;
  }
  const remote = new R2Storage(config),
    storage = new ImageStorage(dataDir, remote, (id) => store.findImage(id));
  return { dataDir, store, remote, storage, objects, calls, controls, seed };
}

test('R2 配置与签名限定账户、S3作用域及条件写，不输出凭据', () => {
  assert.equal(r2Config({}), null);
  assert.throws(() => r2Config({ R2_BUCKET: 'images' }), /configuration/);
  assert.throws(
    () =>
      r2Config({
        R2_ACCOUNT_ID: 'https://evil',
        R2_BUCKET: 'images',
        R2_ACCESS_KEY_ID: 'a',
        R2_SECRET_ACCESS_KEY: 'b',
      }),
    /configuration/,
  );
  const request = signedR2Request(
    config,
    'PUT',
    'images/img_a/' + digest(png),
    png,
    'image/png',
    new Date('2026-10-21T00:00:00Z'),
  );
  assert.equal(request.headers['x-amz-date'], '20261021T000000Z');
  assert.equal(request.headers['content-md5'], createHash('md5').update(png).digest('base64'));
  assert.equal(request.headers['content-type'], 'image/png');
  assert.ok(!request.url.toString().includes(config.secretAccessKey));
  assert.ok(!request.headers.authorization.includes(config.secretAccessKey));
});

test('超过一天的图片原字节转存，参数保留；陈旧记录、历史下载与重启仍可读取', async (t) => {
  const f = await fixture(t),
    image = await f.seed('img_cold');
  await f.seed('img_warm', { createdAt: new Date(now).toISOString() });
  await f.seed('img_pending');
  await f.store.update(
    (db) =>
      db.jobs.push({
        id: 'unfinished',
        imageId: 'img_pending',
        status: 'failed',
        resultPending: { imageId: 'img_pending' },
      }),
    { immediate: true },
  );
  assert.equal(await archiveColdImage(f.store, f.remote, () => true, now), true);
  const archived = await f.store.findImage(image.id);
  assert.ok(archived.r2.verifiedAt);
  assert.ok(archived.r2.localRemovedAt);
  assert.equal(archived.fullPrompt, image.fullPrompt);
  assert.equal(archived.sha256, image.sha256);
  assert.equal(archived.mimeType, 'image/png');
  await assert.rejects(readFile(path.join(f.dataDir, image.file)), { code: 'ENOENT' });
  assert.deepEqual(await f.storage.read(image), png, '读取旧行时重新找到云端位置');
  const count = f.calls.length;
  assert.equal(await f.storage.available(archived), true);
  assert.equal(f.calls.length, count, '历史领取检查不额外产生 HEAD/GET');
  assert.equal(await archiveColdImage(f.store, f.remote, () => true, now), false);
  assert.deepEqual(await f.storage.read(await f.store.findImage('img_warm')), png);
  await f.store.close();
  await f.store.init();
  assert.deepEqual(await f.storage.read(await f.store.findImage(image.id)), png);
  assert.deepEqual(f.store.readR2Usage(), {
    bytes: png.length,
    images: 1,
    pending: 0,
    errors: 0,
    archiveAtBytes: R2_PIKPAK_THRESHOLD_BYTES,
  });
});

for (const failure of ['upload', 'checksum', 'database']) {
  test(`R2 ${failure} 失败保留本地，容量预留持久化；重试不会覆盖已存在的云端原图`, async (t) => {
    const f = await fixture(t),
      image = await f.seed('img_failure');
    if (failure === 'upload') {
      f.controls.fail = true;
    }
    if (failure === 'checksum') {
      f.controls.corrupt = true;
    }
    if (failure === 'database') {
      f.store.sqlite.exec(`CREATE TEMP TRIGGER fail_verified BEFORE INSERT ON images
      WHEN json_extract(NEW.data, '$.r2.verifiedAt') IS NOT NULL BEGIN SELECT RAISE(ABORT, 'synthetic write failure'); END`);
    }
    await assert.rejects(archiveColdImage(f.store, f.remote, () => true, now));
    assert.deepEqual(await readFile(path.join(f.dataDir, image.file)), png);
    assert.equal(f.store.readR2Usage().bytes, png.length);
    assert.equal(f.store.readR2Usage().images, 0);
    assert.equal(f.store.readR2Usage().errors, 1);
    f.controls.fail = false;
    f.controls.corrupt = false;
    if (failure === 'database') {
      f.store.sqlite.exec('DROP TRIGGER fail_verified');
    }
    assert.equal(await archiveColdImage(f.store, f.remote, () => true, now + 3600001), true);
    assert.equal(f.objects.size, 1);
    assert.deepEqual(await f.storage.read(image), png);
  });
}

test('上传结果未知时保留原图；重新启动后复用同一预留 key 并校验远端', async (t) => {
  const f = await fixture(t),
    image = await f.seed('img_timeout');
  f.controls.timeoutAfterPut = true;
  await assert.rejects(archiveColdImage(f.store, f.remote, () => true, now));
  assert.deepEqual(await readFile(path.join(f.dataDir, image.file)), png);
  assert.equal(f.objects.size, 1);
  await f.store.close();
  await f.store.init();
  f.controls.timeoutAfterPut = false;
  assert.equal(await archiveColdImage(f.store, f.remote, () => true, now + 3600001), true);
  assert.equal(f.objects.size, 1);
  f.controls.fail = true;
  await assert.rejects(f.storage.read(image), { statusCode: 503 });
  assert.ok((await f.store.findImage(image.id)).r2.verifiedAt, 'R2 暂时故障不删除历史记录');
});

test('R2 水位只触发转冷，不阻止本地图片上传；忙时不上传', async (t) => {
  const f = await fixture(t);
  await f.seed('img_limit');
  const existingBytes = 10_000_000_000;
  const objects = Math.ceil(existingBytes / MAX_STORED_IMAGE_BYTES);
  const createdAt = new Date(now).toISOString();
  await f.store.update(
    (db) => {
      for (let i = 0; i < objects; i++) {
        db.images.push({
          id: `img_reserved_${i}`,
          createdAt,
          r2: {
            bucket: config.bucket,
            key: `images/img_reserved_${i}/${digest(png)}`,
            byteLength: Math.min(
              MAX_STORED_IMAGE_BYTES,
              existingBytes - i * MAX_STORED_IMAGE_BYTES,
            ),
            sha256: digest(png),
            verifiedAt: createdAt,
            localRemovedAt: createdAt,
          },
        });
      }
    },
    { immediate: true },
  );
  assert.equal(await archiveColdImage(f.store, f.remote, () => false, now), false);
  assert.equal(f.calls.length, 0);
  assert.equal(await archiveColdImage(f.store, f.remote, () => true, now), true);
  assert.equal(f.store.readR2Usage().bytes, existingBytes + png.length);
});

test('已校验并移除本地但收尾未提交的情况可恢复，文件路径越界拒绝', async (t) => {
  const f = await fixture(t),
    image = await f.seed('img_finish');
  await archiveColdImage(f.store, f.remote, () => true, now);
  await f.store.update(
    () => {
      delete f.store.loadItemForUpdate('images', image.id).r2.localRemovedAt;
    },
    { immediate: true, dirtyRows: { images: [image.id] } },
  );
  assert.equal(await archiveColdImage(f.store, f.remote, () => true, now), true);
  await assert.rejects(f.storage.read({ file: '../library.sqlite' }), { statusCode: 403 });
  await assert.rejects(
    f.remote.get({ ...(await f.store.findImage(image.id)).r2, bucket: 'another-bucket' }),
    /descriptor/,
  );
  await assert.rejects(
    f.storage.read({
      r2: {
        ...(await f.store.findImage(image.id)).r2,
        key: 'backups/daily-20261021T000000Z/' + digest(png),
      },
    }),
    { statusCode: 403 },
  );
});

test('备份列表签名包含查询参数；截断、错误内容拒绝，保留策略不能删除图片', async (t) => {
  let xml =
    '<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>backups/daily-20261021T000000Z/' +
    digest(png) +
    '</Key><Size>68</Size></Contents></ListBucketResult>';
  const calls = [];
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    calls.push(options.method);
    if (options.method === 'GET') {
      assert.equal(url.search, '?list-type=2&max-keys=1000&prefix=backups%2F');
      return new Response(xml);
    }
    return new Response(null, { status: 204 });
  });
  const remote = new R2Storage(config),
    rows = await remote.listBackups();
  assert.equal(rows[0].bytes, 68);
  await remote.removeBackup({
    bucket: config.bucket,
    key: rows[0].key,
    byteLength: 68,
    sha256: digest(png),
  });
  await assert.rejects(
    remote.removeBackup({
      bucket: config.bucket,
      key: 'images/img_a/' + digest(png),
      byteLength: 68,
      sha256: digest(png),
    }),
    /Only daily/,
  );
  assert.deepEqual(calls, ['GET', 'DELETE']);
  for (xml of [
    '<Error/>',
    '<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>',
    '<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>backups/bad</Key></Contents></ListBucketResult>',
    '<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>backups/bad</Key><Size/></Contents></ListBucketResult>',
  ]) {
    await assert.rejects(remote.listBackups());
  }
});

test('R2 单次备份上传采用精确平台上限，图片限额仍为 64 MiB', async (t) => {
  t.mock.method(globalThis, 'fetch', async () =>
    assert.fail('Must reject before sending requests'),
  );
  const remote = new R2Storage(config);
  const object = {
    bucket: config.bucket,
    key: 'backups/daily-20261021T000000Z/' + digest(png),
    sha256: digest(png),
  };
  const limit = 5 * 1024 ** 3 - 5 * 1024 ** 2;
  await assert.rejects(remote.putFile({ ...object, byteLength: limit }, ''), { code: 'ENOENT' });
  await assert.rejects(remote.putFile({ ...object, byteLength: limit + 1 }, ''), /descriptor/);
  await assert.rejects(
    remote.get({
      ...object,
      key: 'images/img_a/' + digest(png),
      byteLength: MAX_STORED_IMAGE_BYTES + 1,
    }),
    /descriptor/,
  );
});

test('冷图与旧快照从 PikPak 领取时校验原图，无回执的 R2 损坏不会触发缺失回退', async (t) => {
  const f = await fixture(t);
  const image = await f.seed('img_archived');
  assert.equal(await archiveColdImage(f.store, f.remote, () => true, now), true);
  const object = (await f.store.findImage(image.id)).r2;
  const pikpak = new PikPakStorage({
    url: 'https://example.invalid/archive/',
    username: 'synthetic',
    password: 'synthetic',
  });
  const storage = new ImageStorage(f.dataDir, f.remote, async () => null, pikpak);
  const receipt = { url: pikpak.config.url, uploadedAt: cold };
  const removed = { ...object, pikpak: receipt, removedAt: cold };
  let compressed = brotliCompressSync(png);
  let unauthorized = false;
  const get = t.mock.method(pikpak.client, 'customRequest', async (filename, options) => {
    assert.equal(filename, object.sha256 + '.br');
    assert.equal(options.method, 'GET');
    assert.ok(options.signal);
    if (unauthorized) {
      throw new Error('synthetic unauthorized');
    }
    return new Response(compressed);
  });
  const count = f.calls.length;
  assert.equal(await storage.available({ r2: removed }), true);
  assert.equal(await f.storage.available({ r2: removed }), false);
  assert.equal(
    f.calls.length,
    count,
    'availability checks configured paths without a network read',
  );
  assert.deepEqual(await storage.read({ r2: removed }), png);
  assert.equal(f.calls.length, count, '已有移除回执时直接读取 PikPak');
  f.controls.corrupt = true;
  await assert.rejects(storage.read({ r2: object }), { statusCode: 503 });
  assert.equal(get.mock.callCount(), 1);
  await assert.rejects(storage.read({ r2: { ...object, bucket: 'unrelated-bucket' } }), {
    statusCode: 503,
  });
  assert.equal(get.mock.callCount(), 1);
  assert.deepEqual(
    await storage.read({ r2: { ...object, pikpak: receipt } }),
    png,
    'a committed PikPak copy can be verified independently while R2 is unhealthy',
  );
  assert.equal(get.mock.callCount(), 2);
  f.objects.clear();
  assert.deepEqual(await storage.read({ r2: object }), png, '无 PikPak 回执的旧快照按 SHA 查找');
  await assert.rejects(
    storage.read({
      r2: { ...removed, pikpak: { ...receipt, url: 'https://example.invalid/other/' } },
    }),
    { statusCode: 503 },
  );
  assert.equal(get.mock.callCount(), 3);
  unauthorized = true;
  await assert.rejects(storage.read({ r2: object }), { statusCode: 503 });
  unauthorized = false;
  for (compressed of [
    compressed.subarray(0, -1),
    brotliCompressSync(Buffer.alloc(png.length)),
    brotliCompressSync(Buffer.alloc(png.length + 1)),
  ]) {
    await assert.rejects(storage.read({ r2: object }), { statusCode: 503 });
  }
});

test('已退出内存缓存的冷图仍可转存，持久顺序与其他图片保持不变', async (t) => {
  const f = await fixture(t);
  await f.seed('img_old');
  await f.seed('img_new', { createdAt: new Date(now).toISOString() });
  const before = f.store.sqlite
    .prepare('SELECT id, order_value FROM images ORDER BY order_value')
    .all();
  f.store.trimRuntimeCollection('images', 0);
  f.store.partialCollections.add('images');
  assert.equal(f.store.db.images.length, 0);
  assert.equal(await archiveColdImage(f.store, f.remote, () => true, now), true);
  assert.deepEqual(
    f.store.sqlite.prepare('SELECT id, order_value FROM images ORDER BY order_value').all(),
    before,
  );
  assert.deepEqual(await f.storage.read(await f.store.findImage('img_old')), png);
  assert.deepEqual(await f.storage.read(await f.store.findImage('img_new')), png);
});

test('备份对象列表完整翻页，XML 转义的续页令牌正确签名；重复令牌拒绝', async (t) => {
  let calls = 0,
    repeat = false;
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    assert.equal(options.method, 'GET');
    calls++;
    const token = url.searchParams.get('continuation-token');
    if (token) assert.equal(token, 'next&token');
    return new Response(
      '<ListBucketResult><IsTruncated>' +
        (!token || repeat ? 'true' : 'false') +
        '</IsTruncated>' +
        (!token || repeat ? '<NextContinuationToken>next&amp;token</NextContinuationToken>' : '') +
        '<Contents><Key>backups/daily-20261021T000000Z/' +
        digest(png) +
        '</Key><Size>68</Size></Contents></ListBucketResult>',
    );
  });
  const remote = new R2Storage(config);
  assert.equal((await remote.listBackups()).length, 2);
  assert.equal(calls, 2);
  repeat = true;
  calls = 0;
  await assert.rejects(remote.listBackups(), /incomplete/);
  assert.equal(calls, 2);
});

test('冷图筛选复用任务及图片索引，不对每张图片重复扫描全部任务；待恢复两种关联都保留', async (t) => {
  const f = await fixture(t);
  for (const id of ['img_active', 'img_pending_ref', 'img_pending_result', 'img_ready']) {
    await f.seed(id);
  }
  await f.store.update(
    (db) =>
      db.jobs.push(
        { id: 'active', status: 'running', imageId: 'img_active' },
        {
          id: 'pending',
          status: 'failed',
          imageId: 'img_pending_ref',
          resultPending: { imageId: 'img_pending_result' },
        },
        { id: 'no_image', status: 'running' },
      ),
    { immediate: true },
  );
  const select = f.store.selectItems.bind(f.store);
  let plan;
  t.mock.method(f.store, 'selectItems', (collection, where, parameters) => {
    plan = f.store.sqlite
      .prepare(`EXPLAIN QUERY PLAN SELECT id, data FROM ${collection} ${where}`)
      .all(parameters)
      .map((row) => row.detail)
      .join('\n');
    return select(collection, where, parameters);
  });
  assert.equal(f.store.nextColdImage(now).id, 'img_ready');
  assert.doesNotMatch(plan, /CORRELATED|SCAN jobs(?:\n|$)/);
  assert.match(plan, /idx_jobs_result_pending/);
  assert.match(plan, /idx_images_order/);
});
