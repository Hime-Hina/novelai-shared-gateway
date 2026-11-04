import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { brotliDecompressSync, brotliCompressSync } from 'node:zlib';
import { getPatcher } from 'webdav';
import { JsonStore } from '../../server/store.ts';
import { archivePikPakImage } from '../../server/cold-images.ts';
import { ImageStorage, R2Storage, R2_PIKPAK_THRESHOLD_BYTES } from '../../server/r2-storage.ts';
import { PikPakStorage, pikpakConfig } from '../../server/pikpak-storage.ts';

const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
const config = { accountId: 'a'.repeat(32), bucket: 'synthetic-images', accessKeyId: 'access', secretAccessKey: 'secret' };
const dav = { url: 'https://dav.pikpak.test/archive/', username: 'synthetic-user', password: 'synthetic-password' };
const now = Date.now(), day = 86400000;

async function fixture(t) {
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-pikpak-'));
  await mkdir(path.join(dataDir, 'images'));
  const store = new JsonStore(dataDir); await store.init(); t.after(() => store.close());
  const objects = new Map(), calls = [], controls = {};
  getPatcher().patch('fetch', (...args) => globalThis.fetch(...args));
  t.after(() => getPatcher().restore('fetch'));
  t.mock.method(globalThis, 'fetch', async (address, options) => {
    const url = new URL(address), isPikPak = url.hostname === 'dav.pikpak.test';
    assert.ok(isPikPak || url.hostname === `${config.accountId}.r2.cloudflarestorage.com`);
    const method = options.method, key = url.href;
    assert.ok(options.signal);
    calls.push(`${isPikPak ? 'pikpak' : 'r2'}:${method}`);
    if (isPikPak) assert.equal(new Headers(options.headers).get('authorization'), 'Basic ' + Buffer.from(`${dav.username}:${dav.password}`).toString('base64'));
    if (method === 'PUT') {
      assert.ok(isPikPak);
      if (controls.failUpload) return new Response(null, { status: 503 });
      objects.set(key, Buffer.from(options.body));
      if (controls.loseUploadReply) throw new Error('synthetic lost upload response');
      return new Response(null, { status: 204 });
    }
    if (method === 'DELETE') {
      assert.equal(isPikPak, false, 'cold copies are never deleted by retention');
      if (controls.failDelete) return new Response(null, { status: 503 });
      objects.delete(key);
      if (controls.loseDeleteReply) throw new Error('synthetic lost delete response');
      return new Response(null, { status: 204 });
    }
    if (!objects.has(key)) return new Response(null, { status: 404 });
    let bytes = Buffer.from(objects.get(key));
    if (isPikPak && controls.download) bytes = controls.download;
    return new Response(bytes, { headers: { 'content-length': String(bytes.length) } });
  });
  const remote = new R2Storage(config), pikpak = new PikPakStorage(dav);
  const storage = new ImageStorage(dataDir, remote, id => store.findImage(id), pikpak);
  async function seed(id, storedAt = now - 7 * day - 1, values = {}) {
    const image = { id, file: `images/${id}.png`, mimeType: 'image/png', createdAt: new Date(now - 10 * day).toISOString(),
      fullPrompt: 'private original parameters', sha256: digest(png), byteLength: png.length,
      r2: { bucket: config.bucket, key: `images/${id}/${digest(png)}`, sha256: digest(png), byteLength: png.length,
        verifiedAt: new Date(storedAt).toISOString(), localRemovedAt: new Date(storedAt).toISOString() }, ...values };
    objects.set(`https://${config.accountId}.r2.cloudflarestorage.com/${config.bucket}/${image.r2.key}`, png);
    await store.update(db => db.images.push(image), { immediate: true, dirtyRows: { images: [id] } });
    return image;
  }
  return { dataDir, store, remote, pikpak, storage, objects, calls, controls, seed,
    move: (at = now, idle = () => true) => archivePikPakImage(store, remote, pikpak, idle, at) };
}

test('PikPak 配置只接受完整 HTTPS 凭据；URL 不携带凭据或查询参数', () => {
  assert.equal(pikpakConfig({}), null);
  for (const values of [{ PIKPAK_WEBDAV_URL: dav.url, PIKPAK_WEBDAV_PASSWORD: '' }, { PIKPAK_WEBDAV_URL: 'http://dav.pikpak.test/' },
    { PIKPAK_WEBDAV_URL: 'https://user:secret@dav.pikpak.test/' }, { PIKPAK_WEBDAV_URL: dav.url + '?secret=value' }]) {
    assert.throws(() => pikpakConfig({ PIKPAK_WEBDAV_USERNAME: dav.username, PIKPAK_WEBDAV_PASSWORD: dav.password, ...values }), /configuration/);
  }
  assert.deepEqual(pikpakConfig({ PIKPAK_WEBDAV_URL: dav.url.slice(0, -1), PIKPAK_WEBDAV_USERNAME: dav.username, PIKPAK_WEBDAV_PASSWORD: dav.password }), dav);
});

test('R2 超过七天的图片无损压缩上传，不回读网盘；确认落库后释放 R2，按需下载才解压校验', async t => {
  const f = await fixture(t), image = await f.seed('img_old');
  assert.equal(await f.move(), true);
  assert.deepEqual(f.calls, ['r2:GET', 'pikpak:PUT', 'r2:DELETE']);
  const archived = await f.store.findImage(image.id), compressed = f.objects.get(dav.url + image.sha256 + '.br');
  assert.deepEqual(brotliDecompressSync(compressed), png);
  assert.ok(archived.r2.pikpak.uploadedAt); assert.ok(archived.r2.removedAt);
  assert.deepEqual(f.store.readPikPakUsage(), { originalBytes: png.length, images: 1, pending: 0, errors: 0 });
  assert.equal(archived.fullPrompt, image.fullPrompt);
  assert.equal(f.store.readR2Usage().bytes, 0); assert.equal(f.store.readR2Usage().images, 0);
  assert.equal(await f.storage.available(image), true); assert.equal(f.calls.length, 3);
  await f.store.close(); await f.store.init();
  assert.deepEqual(await f.storage.read(image), png);
  assert.equal(f.calls.at(-1), 'pikpak:GET');
  assert.equal(await f.move(), false);
});

test('低于 90% 只迁移 R2 超七天的图；触及阈值按 R2 存放时间从早到晚，忙时跳过', async t => {
  const f = await fixture(t);
  await f.seed('young', now - 1000); await f.seed('older', now - day); await f.seed('boundary', now - 7 * day);
  let usage = R2_PIKPAK_THRESHOLD_BYTES - 1;
  t.mock.method(f.store, 'readR2Usage', () => ({ bytes: usage }));
  assert.equal(f.store.nextPikPakImage(now), null, '生成十天但 R2 未超七天的图片不提前迁移');
  usage++;
  assert.equal(f.store.nextPikPakImage(now).id, 'boundary');
  assert.equal(await f.move(now, () => false), false); assert.equal(f.calls.length, 0);
  assert.equal(await f.move(), true);
  assert.equal(f.store.nextPikPakImage(now).id, 'older');
  usage--;
  assert.equal(f.store.nextPikPakImage(now), null, '降到阈值以下即停止容量迁移');
});

for (const failure of ['upload', 'upload-unknown', 'receipt', 'delete']) {
  test(`PikPak ${failure} 失败保留副本、容量保守计入 R2，一小时后重试且不回读网盘`, async t => {
    const f = await fixture(t), image = await f.seed('img_retry');
    f.controls.failUpload = failure === 'upload'; f.controls.loseUploadReply = failure === 'upload-unknown'; f.controls.failDelete = failure === 'delete';
    if (failure === 'receipt') f.store.sqlite.exec(`CREATE TEMP TRIGGER fail_receipt BEFORE INSERT ON images
      WHEN json_extract(NEW.data, '$.r2.pikpak.uploadedAt') IS NOT NULL BEGIN SELECT RAISE(ABORT, 'synthetic receipt failure'); END`);
    await assert.rejects(f.move());
    assert.equal(f.store.readR2Usage().bytes, png.length); assert.equal(f.store.readPikPakUsage().errors, 1);
    assert.deepEqual(await f.remote.get(image.r2), png);
    assert.equal(await f.move(), false);
    if (failure === 'receipt') f.store.sqlite.exec('DROP TRIGGER fail_receipt');
    f.controls.failUpload = false; f.controls.loseUploadReply = false; f.controls.failDelete = false;
    assert.equal(await f.move(now + 3600001), true);
    assert.equal(f.store.readR2Usage().bytes, 0); assert.equal(f.store.readPikPakUsage().errors, 0);
    assert.ok(!f.calls.includes('pikpak:GET'));
  });
}

for (const failure of ['delete-unknown', 'removed-receipt']) {
  test(`R2 ${failure} 可在重启后收尾，不重新上传或回读；旧备份仍凭原 SHA-256 找回压缩原图`, async t => {
    const f = await fixture(t), image = await f.seed('img_recover');
    f.controls.loseDeleteReply = failure === 'delete-unknown';
    if (failure === 'removed-receipt') f.store.sqlite.exec(`CREATE TEMP TRIGGER fail_removed BEFORE INSERT ON images
      WHEN json_extract(NEW.data, '$.r2.removedAt') IS NOT NULL BEGIN SELECT RAISE(ABORT, 'synthetic removal receipt failure'); END`);
    await assert.rejects(f.move());
    await f.store.close(); await f.store.init(); f.controls.loseDeleteReply = false;
    assert.equal(await f.move(now + 3600001), true);
    assert.equal(f.calls.filter(call => call === 'pikpak:PUT').length, 1); assert.ok(!f.calls.includes('pikpak:GET'));
    const restored = new ImageStorage(f.dataDir, f.remote, async () => image, f.pikpak);
    assert.deepEqual(await restored.read(image), png);
  });
}

test('下载时拒绝截断、超长、解压膨胀或损坏的冷图；失败保留归档记录', async t => {
  const f = await fixture(t), image = await f.seed('img_corrupt'); await f.move();
  const archived = await f.store.findImage(image.id), length = f.objects.get(dav.url + image.sha256 + '.br').length;
  for (const bytes of [Buffer.alloc(length - 1), Buffer.alloc(length + 1), Buffer.alloc(length)]) {
    f.controls.download = bytes;
    await assert.rejects(f.storage.read(image), { statusCode: 503 });
  }
  f.controls.download = brotliCompressSync(Buffer.alloc(png.length + 1));
  await assert.rejects(f.pikpak.get(archived.r2));
  f.controls.download = Buffer.alloc(65 * 1024 * 1024 + 1);
  await assert.rejects(f.pikpak.get(archived.r2), /maximum length/);
  assert.ok((await f.store.findImage(image.id)).r2.pikpak.uploadedAt);
});

test('上传前校验源字节，拒绝越界描述符；归档目录改变后不删除 R2 原件', async t => {
  const f = await fixture(t), image = await f.seed('img_safe');
  await assert.rejects(f.pikpak.put(image.r2, Buffer.from('wrong')), /checksum/);
  await assert.rejects(f.pikpak.put({ ...image.r2, key: '../secret' }, png), /descriptor/);
  await assert.rejects(f.remote.removeImage(image.r2), /Confirmed/);
  await f.store.update(() => { f.store.loadItemForUpdate('images', image.id).r2.pikpak = { url: 'https://other.test/', uploadedAt: new Date(now).toISOString() }; }, { immediate: true, dirtyRows: { images: [image.id] } });
  await assert.rejects(f.move(), /directory changed/);
  assert.deepEqual(await f.remote.get(image.r2), png);
  assert.equal(f.calls.filter(call => call === 'r2:DELETE').length, 0);
});
