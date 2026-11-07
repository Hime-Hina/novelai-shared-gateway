import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';
import { constants, createReadStream, createWriteStream, existsSync, realpathSync } from 'node:fs';
import { copyFile, mkdtemp, open, readFile, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import { parseEnv } from 'node:util';
import { pathToFileURL } from 'node:url';

const magic = Buffer.from('NAIR2B1\0');
export function encryptBackup(archive, key) {
  if (key.length !== 32) {
    throw new Error('Backup key must contain 32 bytes');
  }
  const iv = randomBytes(12),
    cipher = createCipheriv('aes-256-gcm', key, iv);
  const encrypted = Buffer.concat([cipher.update(archive), cipher.final()]);
  return Buffer.concat([magic, iv, cipher.getAuthTag(), encrypted]);
}
export function decryptBackup(buffer, key) {
  if (!buffer.subarray(0, 8).equals(magic) || buffer.length < 36) {
    throw new Error('Invalid encrypted backup');
  }
  const decipher = createDecipheriv('aes-256-gcm', key, buffer.subarray(8, 20));
  decipher.setAuthTag(buffer.subarray(20, 36));
  return Buffer.concat([decipher.update(buffer.subarray(36)), decipher.final()]);
}

async function encryptBackupFile(input, output, key) {
  if (key.length !== 32) {
    throw new Error('Backup key must contain 32 bytes');
  }
  const iv = randomBytes(12),
    cipher = createCipheriv('aes-256-gcm', key, iv);
  // 认证标签在流结束后回填，保持旧备份的封套布局。
  await writeFile(output, Buffer.concat([magic, iv, Buffer.alloc(16)]), {
    flag: 'wx',
    mode: 0o600,
  });
  await pipeline(createReadStream(input), cipher, createWriteStream(output, { flags: 'a' }));
  const handle = await open(output, 'r+');
  try {
    if ((await handle.write(cipher.getAuthTag(), 0, 16, 20)).bytesWritten !== 16) {
      throw new Error('Backup authentication tag write failed');
    }
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function decryptBackupFile(input, output, key) {
  if (key.length !== 32) {
    throw new Error('Backup key must contain 32 bytes');
  }
  const handle = await open(input, 'r'),
    header = Buffer.alloc(36);
  try {
    if (
      (await handle.read(header, 0, header.length, 0)).bytesRead !== 36 ||
      !header.subarray(0, 8).equals(magic)
    ) {
      throw new Error('Invalid encrypted backup');
    }
  } finally {
    await handle.close();
  }
  const decipher = createDecipheriv('aes-256-gcm', key, header.subarray(8, 20));
  decipher.setAuthTag(header.subarray(20, 36));
  await pipeline(
    createReadStream(input, { start: 36 }),
    decipher,
    createWriteStream(output, { flags: 'wx', mode: 0o600 }),
  );
}

async function main() {
  const [mode, input, output, envFile, keyFile, snapshotName] = process.argv.slice(2);
  if (!['upload', 'download'].includes(mode) || !input || !output || !envFile || !keyFile) {
    throw new Error('Invalid backup arguments');
  }
  const runtime = existsSync(new URL('../server/storage/r2-storage.js', import.meta.url))
    ? '../server/storage/r2-storage.js'
    : '../server/storage/r2-storage.ts';
  const { R2Storage, r2Config, fileChecksums } = await import(runtime);
  const config = r2Config(parseEnv(await readFile(envFile, 'utf8')));
  if (!config) {
    throw new Error('R2 configuration is required');
  }
  const remote = new R2Storage(config),
    key = await readFile(keyFile);
  const parent = path.resolve(path.dirname(output)),
    work = await mkdtemp(path.join(parent, '.r2-transfer-'));
  try {
    const encrypted = path.join(work, 'encrypted'),
      downloaded = path.join(work, 'downloaded'),
      restored = path.join(work, 'restored');
    if (mode === 'download') {
      const receipt = JSON.parse(await readFile(input, 'utf8'));
      await remote.getFile(receipt, downloaded);
      await decryptBackupFile(downloaded, restored, key);
      await copyFile(restored, output, constants.COPYFILE_EXCL);
      return;
    }
    if (!/^daily-[0-9TZ.]+$/.test(snapshotName || '')) {
      throw new Error('Only daily business backups are uploaded');
    }
    const original = await fileChecksums(input);
    await encryptBackupFile(input, encrypted, key);
    const { byteLength, sha256 } = await fileChecksums(encrypted);
    const object = {
      bucket: config.bucket,
      key: `backups/${snapshotName}/${sha256}`,
      byteLength,
      sha256,
    };
    const existing = await remote.listBackups();
    await remote.putFile(object, encrypted);
    await remote.getFile(object, downloaded);
    await decryptBackupFile(downloaded, restored, key);
    const roundTrip = await fileChecksums(restored);
    if (roundTrip.byteLength !== original.byteLength || roundTrip.sha256 !== original.sha256) {
      throw new Error('Encrypted backup round-trip mismatch');
    }
    const receipt = await open(output, 'wx', 0o600);
    try {
      await receipt.writeFile(JSON.stringify(object, null, 2) + '\n');
      await receipt.sync();
    } finally {
      await receipt.close();
    }
    const daily = new Map();
    for (const row of existing) {
      const match = /^backups\/daily-[0-9TZ.]+\/([a-f0-9]{64})$/.exec(row.key);
      if (match) {
        daily.set(row.key, {
          bucket: config.bucket,
          key: row.key,
          byteLength: row.bytes,
          sha256: match[1],
        });
      }
    }
    daily.set(object.key, object);
    const backups = [...daily.values()].sort((a, b) => b.key.localeCompare(a.key));
    const expired = backups.slice(7);
    if (!expired.length) {
      return;
    }
    // 失败尝试也可能留在列表里。所有拟保留对象通过验证后才能删旧份。
    try {
      await rm(encrypted);
      for (const retained of backups.slice(0, 7)) {
        if (retained.key === object.key) {
          continue;
        }
        await rm(downloaded);
        await rm(restored);
        await remote.getFile(retained, downloaded);
        await decryptBackupFile(downloaded, restored, key);
      }
    } catch {
      console.error('R2 backup retention aborted; existing backups retained.');
      process.exitCode = 2;
      return;
    }
    for (const old of expired) {
      await remote.removeBackup(old);
    }
  } finally {
    if (path.dirname(work) !== parent) {
      throw new Error('Unsafe backup temporary directory');
    }
    await rm(work, { recursive: true, force: true });
  }
}
if (process.argv[1] && pathToFileURL(realpathSync(process.argv[1])).href === import.meta.url) {
  main().catch(() => {
    console.error('R2 business backup failed; local snapshot retained.');
    process.exitCode = 1;
  });
}
