import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { writeFile, readFile, readdir, open, stat } from 'node:fs/promises';
import path from 'node:path';
import { backupFixture } from '../helpers/r2-backup-fixture.mjs';
import { encryptBackup, decryptBackup } from '../../scripts/r2-backup.mjs';
import { fileChecksums } from '../../server/storage/r2-storage.ts';

function dailyKey(day, encrypted) {
  const sha256 = createHash('sha256').update(encrypted).digest('hex');
  return `backups/daily-202610${String(day).padStart(2, '0')}T000000Z/${sha256}`;
}

test('业务备份 AES-256-GCM 可还原，密钥/密文/认证标签损坏必须拒绝', () => {
  const original = Buffer.from('synthetic credentials and database bytes'),
    key = randomBytes(32);
  const encrypted = encryptBackup(original, key);
  assert.deepEqual(decryptBackup(encrypted, key), original);
  assert.ok(!encrypted.includes(original));
  assert.notDeepEqual(encryptBackup(original, key), encrypted);
  assert.throws(() => decryptBackup(encrypted, randomBytes(32)));
  for (const position of [0, 8, 20, encrypted.length - 1]) {
    const changed = Buffer.from(encrypted);
    changed[position] ^= 1;
    assert.throws(() => decryptBackup(changed, key));
  }
  assert.throws(() => encryptBackup(original, Buffer.alloc(1)));
});

test('70 MiB 备份跨过旧单份与累计上限，验证后才清理到七份 daily', async (t) => {
  const { root, cloud, envFile, keyFile, call, encode } = await backupFixture(t);
  const pinned = 'backups/cutover-20260901T000000Z/' + 'a'.repeat(64);
  await writeFile(encode(pinned), 'pinned');
  const old = encryptBackup(Buffer.from('previous valid backup'), await readFile(keyFile));
  for (let i = 1; i <= 7; i++) {
    await writeFile(encode(dailyKey(i, old)), old);
  }
  const archive = path.join(root, 'business.tar.gz'),
    receipt = path.join(root, 'receipt.json'),
    trace = path.join(root, 'rotation.log'),
    bytes = 70 * 1024 ** 2,
    file = await open(archive, 'wx');
  try {
    await file.truncate(bytes);
    await file.write('start', 0);
    await file.write('end', bytes - 3);
  } finally {
    await file.close();
  }
  await call(['upload', archive, receipt, envFile, keyFile, 'daily-20261021T000000Z'], {
    R2_TEST_CUTOVER_BYTES: '500000000',
    R2_TEST_REPEAT_NEWEST: '1',
    R2_TEST_TRACE: trace,
  });
  const retained = await readdir(cloud);
  assert.equal(retained.length, 8);
  assert.ok(!retained.some((name) => decodeURIComponent(name).includes('20261001')));
  assert.equal(await readFile(encode(pinned), 'utf8'), 'pinned');
  const descriptor = JSON.parse(await readFile(receipt));
  assert.equal(descriptor.byteLength, bytes + 36);
  assert.deepEqual((await readFile(trace, 'utf8')).trimEnd().split('\n'), [
    'GET ',
    'PUT ' + descriptor.key,
    'GET ' + descriptor.key,
    ...[7, 6, 5, 4, 3, 2].map((day) => 'GET ' + dailyKey(day, old)),
    'DELETE ' + dailyKey(1, old),
  ]);
  const restored = path.join(root, 'restored.tar.gz');
  await call(['download', receipt, restored, envFile, keyFile]);
  const original = await fileChecksums(archive);
  assert.deepEqual(await fileChecksums(restored), original);
  await assert.rejects(
    call(['download', receipt, restored, envFile, keyFile]),
    'restore must preserve existing output',
  );
  assert.deepEqual(await fileChecksums(restored), original);
  const failedReceipt = path.join(root, 'failed-receipt.json');
  await assert.rejects(
    call(['upload', archive, failedReceipt, envFile, keyFile, 'daily-20261022T000000Z'], {
      R2_TEST_FAIL_READ: '1',
    }),
  );
  await assert.rejects(readFile(failedReceipt), { code: 'ENOENT' });
  const afterFailure = await readdir(cloud);
  assert.equal(afterFailure.length, 9);
  assert.ok(retained.every((name) => afterFailure.includes(name)));
  assert.deepEqual(await fileChecksums(archive), original);
  assert.ok(!(await readdir(root)).some((name) => name.startsWith('.r2-transfer-')));
});

test('下次上传成功后，失败残留也不能挤掉七份已验证备份', async (t) => {
  const { root, cloud, envFile, keyFile, call, encode } = await backupFixture(t);
  const archive = path.join(root, 'business.tar.gz'),
    original = Buffer.from('synthetic backup bytes');
  const snapshot = (day) => 'daily-202610' + String(day).padStart(2, '0') + 'T000000Z';
  const receipt = (day) => path.join(root, snapshot(day) + '.json');
  const upload = (day, env) =>
    call(['upload', archive, receipt(day), envFile, keyFile, snapshot(day)], env);
  await writeFile(archive, original);
  const verified = [];
  for (let day = 1; day <= 7; day++) {
    await upload(day);
    verified.push(JSON.parse(await readFile(receipt(day))).key);
  }
  for (let day = 8; day <= 10; day++) {
    await assert.rejects(upload(day, { R2_TEST_CORRUPT_UPLOAD: '1' }), { code: 1 });
    await assert.rejects(stat(receipt(day)), { code: 'ENOENT' });
  }
  const previous = await readdir(cloud);
  assert.equal(previous.length, 10);
  const trace = path.join(root, 'rotation.log');
  const failure = await upload(11, { R2_TEST_TRACE: trace }).then(
    () => null,
    (error) => error,
  );
  const latest = JSON.parse(await readFile(receipt(11)));
  const retained = await readdir(cloud);
  const key = await readFile(keyFile);
  let recoverable = 0;
  for (const filename of retained) {
    const encrypted = await readFile(path.join(cloud, filename));
    if (
      decodeURIComponent(filename).endsWith(createHash('sha256').update(encrypted).digest('hex'))
    ) {
      assert.deepEqual(decryptBackup(encrypted, key), original);
      recoverable++;
    }
  }
  assert.equal(recoverable, 8, '失败残留不能淘汰已验证的旧备份');
  assert.deepEqual(retained.sort(), [...previous, path.basename(encode(latest.key))].sort());
  assert.ok(verified.every((objectKey) => retained.includes(path.basename(encode(objectKey)))));
  assert.doesNotMatch(await readFile(trace, 'utf8'), /^DELETE /m);
  assert.equal(failure?.code, 2);
  assert.match(failure.stderr, /retention/i);
});

test('回填旧日期时也验证第七份保留对象的 GCM，失败不开始删除', async (t) => {
  const { root, cloud, envFile, keyFile, call, encode } = await backupFixture(t);
  const archive = path.join(root, 'business.tar.gz'),
    receipt = path.join(root, 'backdated.json'),
    trace = path.join(root, 'rotation.log');
  await writeFile(archive, 'synthetic archive');
  const healthy = encryptBackup(Buffer.from('valid backup'), await readFile(keyFile));
  const wrongKey = encryptBackup(Buffer.from('object hash matches but wrong key'), randomBytes(32));
  await writeFile(encode(dailyKey(8, wrongKey)), wrongKey);
  for (let day = 9; day <= 14; day++) {
    await writeFile(encode(dailyKey(day, healthy)), healthy);
  }
  await assert.rejects(
    call(['upload', archive, receipt, envFile, keyFile, 'daily-20261001T000000Z'], {
      R2_TEST_TRACE: trace,
    }),
    { code: 2 },
  );
  const descriptor = JSON.parse(await readFile(receipt));
  assert.equal((await readdir(cloud)).length, 8);
  assert.deepEqual((await readFile(trace, 'utf8')).trimEnd().split('\n'), [
    'GET ',
    'PUT ' + descriptor.key,
    'GET ' + descriptor.key,
    ...[14, 13, 12, 11, 10, 9].map((day) => 'GET ' + dailyKey(day, healthy)),
    'GET ' + dailyKey(8, wrongKey),
  ]);
});

test('流式下载读取旧备份封套，认证失败时不发布明文', async (t) => {
  const { root, envFile, keyFile, call, encode } = await backupFixture(t);
  const original = Buffer.from('previous backup format');
  const legacy = encryptBackup(original, await readFile(keyFile));
  const receipt = path.join(root, 'legacy.json');
  const restore = async (encrypted, output) => {
    const sha256 = createHash('sha256').update(encrypted).digest('hex');
    const key = `backups/daily-20260901T000000Z/${sha256}`;
    await writeFile(encode(key), encrypted);
    await writeFile(
      receipt,
      JSON.stringify({ bucket: 'synthetic-images', key, byteLength: encrypted.length, sha256 }),
    );
    await call(['download', receipt, output, envFile, keyFile]);
  };
  const restored = path.join(root, 'legacy-restored.tar.gz');
  await restore(legacy, restored);
  assert.deepEqual(await readFile(restored), original);

  // R2 checksum verification passes because the synthetic descriptor matches the tampered object.
  legacy[20] ^= 1;
  const tampered = path.join(root, 'tampered.tar.gz');
  await assert.rejects(restore(legacy, tampered));
  await assert.rejects(stat(tampered), { code: 'ENOENT' });
  assert.ok(!(await readdir(root)).some((name) => name.startsWith('.r2-transfer-')));
});

test('备份 CLI 不上传 cutover，也不覆盖已有回执', async (t) => {
  const { root, cloud, envFile, keyFile, call } = await backupFixture(t);
  const archive = path.join(root, 'business.tar.gz'),
    receipt = path.join(root, 'receipt.json');
  await writeFile(archive, 'synthetic private archive');
  await assert.rejects(
    call(['upload', archive, receipt, envFile, keyFile, 'cutover-20261101T000000Z']),
  );
  assert.deepEqual(await readdir(cloud), []);
  await assert.rejects(stat(receipt), { code: 'ENOENT' });
  await writeFile(receipt, 'existing receipt');
  await assert.rejects(
    call(['upload', archive, receipt, envFile, keyFile, 'daily-20261101T000000Z']),
  );
  assert.equal(await readFile(receipt, 'utf8'), 'existing receipt');
});
