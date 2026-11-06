import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { writeFile, readFile, readdir, open, stat } from 'node:fs/promises';
import path from 'node:path';
import { backupFixture } from '../helpers/r2-backup-fixture.mjs';
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

test('备份 CLI 上传、回读解密及下载还原，验证后才清理到七份', async t => {
  const { root, cloud, envFile, keyFile, call, encode } = await backupFixture(t);
  for (let i = 1; i <= 7; i++) await writeFile(encode(`backups/daily-2026100${i}T000000Z/${'a'.repeat(64)}`), 'old');
  const archive = path.join(root, 'business.tar.gz'), receipt = path.join(root, 'receipt.json');
  await writeFile(archive, 'synthetic private archive');
  await call(['upload', archive, receipt, envFile, keyFile, 'daily-20261021T000000Z']);
  assert.equal((await readdir(cloud)).length, 7);
  assert.ok(!(await readdir(cloud)).some(name => decodeURIComponent(name).includes('20261001')));
  const restored = path.join(root, 'restored.tar.gz');
  await call(['download', receipt, restored, envFile, keyFile]);
  assert.deepEqual(await readFile(restored), await readFile(archive));
  const failedReceipt = path.join(root, 'failed-receipt.json');
  await assert.rejects(call(['upload', archive, failedReceipt, envFile, keyFile, 'daily-20261022T000000Z'], { R2_TEST_FAIL_READ: '1' }));
  await assert.rejects(readFile(failedReceipt), { code: 'ENOENT' });
  assert.equal((await readdir(cloud)).length, 8, 'unverified upload must not delete older backups');
  assert.equal((await readFile(archive)).toString(), 'synthetic private archive');
});

test('50 MiB archive limit includes its boundary and rejects the next byte before upload', async t => {
  const { root, cloud, envFile, keyFile, call } = await backupFixture(t);
  const archive = path.join(root, 'boundary.tar.gz'), receipt = path.join(root, 'receipt.json');
  const file = await open(archive, 'w'); await file.truncate(50 * 1024 * 1024); await file.close();
  await call(['upload', archive, receipt, envFile, keyFile, 'daily-20261101T000000Z']);
  assert.equal(JSON.parse(await readFile(receipt)).byteLength, 50 * 1024 * 1024 + 36);
  const larger = await open(archive, 'r+'); await larger.truncate(50 * 1024 * 1024 + 1); await larger.close();
  const failedReceipt = path.join(root, 'failed.json');
  await assert.rejects(call(['upload', archive, failedReceipt, envFile, keyFile, 'daily-20261102T000000Z']));
  await assert.rejects(readFile(failedReceipt), { code: 'ENOENT' });
  assert.equal((await readdir(cloud)).length, 1);
  assert.equal((await stat(archive)).size, 50 * 1024 * 1024 + 1);
});

test('500,000,000 byte allowance includes encryption overhead and fails before deleting old backups', async t => {
  const { root, cloud, envFile, keyFile, call, encode } = await backupFixture(t);
  const archive = path.join(root, 'business.tar.gz'); await writeFile(archive, 'capacity fixture');
  const encryptedBytes = (await stat(archive)).size + 36;
  await writeFile(encode('backups/pinned-legacy'), 'old');
  const rejected = path.join(root, 'rejected.json');
  await assert.rejects(call(['upload', archive, rejected, envFile, keyFile, 'daily-20261101T000000Z'],
    { R2_TEST_LIST_BYTES: String(500_000_000 - encryptedBytes + 1) }));
  await assert.rejects(readFile(rejected), { code: 'ENOENT' });
  assert.equal((await readdir(cloud)).length, 1);
  await call(['upload', archive, path.join(root, 'accepted.json'), envFile, keyFile, 'daily-20261101T000000Z'],
    { R2_TEST_LIST_BYTES: String(500_000_000 - encryptedBytes) });
  assert.equal((await readdir(cloud)).length, 2);
  assert.equal((await readFile(archive)).toString(), 'capacity fixture');
});
