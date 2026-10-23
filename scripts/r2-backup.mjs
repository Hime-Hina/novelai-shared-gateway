import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';
import { existsSync, realpathSync } from 'node:fs';
import { readFile, writeFile, stat } from 'node:fs/promises';
import { parseEnv } from 'node:util';
import { pathToFileURL } from 'node:url';

const magic = Buffer.from('NAIR2B1\0');
const hash = value => createHash('sha256').update(value).digest('hex');
export function encryptBackup(archive, key) {
  if (key.length !== 32) throw new Error('Backup key must contain 32 bytes');
  const iv = randomBytes(12), cipher = createCipheriv('aes-256-gcm', key, iv);
  const encrypted = Buffer.concat([cipher.update(archive), cipher.final()]);
  return Buffer.concat([magic, iv, cipher.getAuthTag(), encrypted]);
}
export function decryptBackup(buffer, key) {
  if (!buffer.subarray(0, 8).equals(magic) || buffer.length < 36) throw new Error('Invalid encrypted backup');
  const decipher = createDecipheriv('aes-256-gcm', key, buffer.subarray(8, 20));
  decipher.setAuthTag(buffer.subarray(20, 36));
  return Buffer.concat([decipher.update(buffer.subarray(36)), decipher.final()]);
}

async function main() {
  const [mode, input, output, envFile, keyFile, snapshotName] = process.argv.slice(2);
  if (!['upload', 'download'].includes(mode) || !input || !output || !envFile || !keyFile) throw new Error('Invalid backup arguments');
  const runtime = existsSync(new URL('../server/r2-storage.js', import.meta.url)) ? '../server/r2-storage.js' : '../server/r2-storage.ts';
  const { R2Storage, r2Config } = await import(runtime);
  const config = r2Config(parseEnv(await readFile(envFile, 'utf8')));
  if (!config) throw new Error('R2 configuration is required');
  const remote = new R2Storage(config), key = await readFile(keyFile);
  if (mode === 'download') {
    const receipt = JSON.parse(await readFile(input, 'utf8'));
    const decrypted = decryptBackup(await remote.get(receipt), key);
    await writeFile(output, decrypted, { flag: 'wx', mode: 0o600 });
    return;
  }
  if (!/^daily-[0-9TZ.]+$/.test(snapshotName || '')) throw new Error('Only daily business backups are uploaded');
  // Keep seven compressed backups below the 0.5 GB reserved alongside the 9.5 GB image allowance.
  if ((await stat(input)).size > 50 * 1024 * 1024) throw new Error('Compressed business backup exceeds 50 MiB; local backup retained');
  const encrypted = encryptBackup(await readFile(input), key);
  const object = { bucket: config.bucket, key: `backups/${snapshotName}/${hash(encrypted)}`,
    byteLength: encrypted.length, sha256: hash(encrypted) };
  const existing = await remote.listBackups();
  if (existing.reduce((sum, row) => sum + row.bytes, 0) + encrypted.length > 500_000_000) throw new Error('R2 business backup allowance reached; local backup retained');
  await remote.put(object, encrypted, 'application/octet-stream');
  if (!decryptBackup(await remote.get(object), key).equals(await readFile(input))) throw new Error('Encrypted backup round-trip mismatch');
  await writeFile(output, JSON.stringify(object, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  const backups = [...existing, { key: object.key, bytes: object.byteLength }]
    .filter(row => /^backups\/daily-[0-9TZ.]+\/[a-f0-9]{64}$/.test(row.key)).sort((a, b) => b.key.localeCompare(a.key));
  for (const old of backups.slice(7)) {
    await remote.removeBackup({ bucket: config.bucket, key: old.key, byteLength: old.bytes, sha256: old.key.split('/').at(-1) });
  }
}
if (process.argv[1] && pathToFileURL(realpathSync(process.argv[1])).href === import.meta.url) {
  main().catch(() => { console.error('R2 business backup failed; local snapshot retained.'); process.exitCode = 1; });
}
