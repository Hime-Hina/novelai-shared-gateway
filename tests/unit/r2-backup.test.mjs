import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, readFile, readdir, symlink, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { encryptBackup, decryptBackup } from '../../scripts/r2-backup.mjs';

test('业务备份 AES-256-GCM 可还原，密钥/密文/认证标签损坏必须拒绝', () => {
  const original = Buffer.from('synthetic credentials and database bytes'), key = randomBytes(32);
  const encrypted = encryptBackup(original, key);
  assert.deepEqual(decryptBackup(encrypted, key), original);
  assert.ok(!encrypted.includes(original));
  assert.notDeepEqual(encryptBackup(original, key), encrypted);
  assert.throws(() => decryptBackup(encrypted, randomBytes(32)));
  for (const position of [0, 8, 20, encrypted.length - 1]) {
    const changed = Buffer.from(encrypted); changed[position] ^= 1;
    assert.throws(() => decryptBackup(changed, key));
  }
  assert.throws(() => encryptBackup(original, Buffer.alloc(1)));
});

test('超过 64 MiB 的备份流式上传与恢复，超过 500 MB 总量仍只在验证成功后保留七份', async t => {
  const root = await mkdtemp(path.join(tmpdir(), 'nai-r2-backup-'));
  t.after(async () => { assert.equal(path.dirname(root), path.resolve(tmpdir())); await rm(root, { recursive: true, force: true }); });
  const cloud = path.join(root, 'cloud'); await mkdir(cloud);
  const encode = key => path.join(cloud, encodeURIComponent(key));
  for (let i = 1; i <= 7; i++) await writeFile(encode(`backups/daily-2026100${i}T000000Z/${'a'.repeat(64)}`), 'old');
  const archive = path.join(root, 'business.tar.gz'), receipt = path.join(root, 'receipt.json');
  const envFile = path.join(root, 'r2.env'), keyFile = path.join(root, 'backup.key'), preload = path.join(root, 'preload.mjs');
  await writeFile(archive, Buffer.alloc(70 * 1024 ** 2, 0xa5));
  await writeFile(keyFile, randomBytes(32));
  await writeFile(envFile, `R2_ACCOUNT_ID=${'a'.repeat(32)}\nR2_BUCKET=synthetic-images\nR2_ACCESS_KEY_ID=synthetic-access\nR2_SECRET_ACCESS_KEY=synthetic-secret\n`);
  await writeFile(preload, `
    import { readFile, writeFile, readdir, stat, unlink } from 'node:fs/promises';
    import { createReadStream } from 'node:fs';
    import { Readable } from 'node:stream';
    import path from 'node:path';
    const root = ${JSON.stringify(cloud)};
    globalThis.fetch = async (url, options) => {
      if (!url.hostname.endsWith('.r2.cloudflarestorage.com')) throw new Error('Unexpected host');
      const key = decodeURIComponent(url.pathname.split('/').slice(2).join('/'));
      const file = path.join(root, encodeURIComponent(key));
      if (url.search) {
        const rows = await Promise.all((await readdir(root)).map(async name => '<Contents><Key>' + decodeURIComponent(name)
          + '</Key><Size>' + (decodeURIComponent(name).includes('/${'a'.repeat(64)}') ? 90_000_000 : (await stat(path.join(root, name))).size) + '</Size></Contents>'));
        return new Response('<ListBucketResult><IsTruncated>false</IsTruncated>' + rows.join('') + '</ListBucketResult>');
      }
      if (options.method === 'PUT') { await writeFile(file, options.body, { flag: 'wx' }); return new Response(null); }
      if (options.method === 'DELETE') { await unlink(file); return new Response(null, { status: 204 }); }
      if (process.env.R2_TEST_FAIL_READ === '1') return new Response(null, { status: 503 });
      return new Response(Readable.toWeb(createReadStream(file)), { headers: { 'content-length': String((await stat(file)).size) } });
    };`);
  const run = promisify(execFile);
  const scriptsLink = path.join(root, 'scripts-link');
  await symlink(fileURLToPath(new URL('../../scripts/', import.meta.url)), scriptsLink, 'junction');
  const cli = path.join(scriptsLink, 'r2-backup.mjs');
  const call = (args, failRead = false) => run(process.execPath, ['--import', pathToFileURL(preload).href, cli, ...args], {
    windowsHide: true, env: { ...process.env, R2_TEST_FAIL_READ: failRead ? '1' : '0' }
  });
  const legacy = encryptBackup(Buffer.from('previous backup format'), await readFile(keyFile));
  const legacyHash = createHash('sha256').update(legacy).digest('hex');
  const legacyObject = { bucket: 'synthetic-images', key: `backups/daily-20260901T000000Z/${legacyHash}`, byteLength: legacy.length, sha256: legacyHash };
  const legacyReceipt = path.join(root, 'legacy.json'), legacyRestored = path.join(root, 'legacy-restored.tar.gz');
  await writeFile(encode(legacyObject.key), legacy); await writeFile(legacyReceipt, JSON.stringify(legacyObject));
  await call(['download', legacyReceipt, legacyRestored, envFile, keyFile]);
  assert.equal((await readFile(legacyRestored)).toString(), 'previous backup format');
  await call(['upload', archive, receipt, envFile, keyFile, 'daily-20261021T000000Z']);
  assert.equal((await readdir(cloud)).length, 7);
  assert.ok(!(await readdir(cloud)).some(name => decodeURIComponent(name).includes('20261001')));
  const restored = path.join(root, 'restored.tar.gz');
  await call(['download', receipt, restored, envFile, keyFile]);
  assert.deepEqual(await readFile(restored), await readFile(archive));
  const failedReceipt = path.join(root, 'failed-receipt.json');
  await assert.rejects(call(['upload', archive, failedReceipt, envFile, keyFile, 'daily-20261022T000000Z'], true));
  await assert.rejects(readFile(failedReceipt), { code: 'ENOENT' });
  assert.equal((await readdir(cloud)).length, 8, 'unverified upload must not delete older backups');
  assert.equal((await readFile(archive)).length, 70 * 1024 ** 2);
  await assert.rejects(call(['download', receipt, restored, envFile, keyFile]), 'restore never overwrites existing files');
  assert.ok(!(await readdir(root)).some(name => name.startsWith('.r2-transfer-')));
});
