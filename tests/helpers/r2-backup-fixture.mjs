import { randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, symlink, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

export async function backupFixture(t, root) {
  const preserve = Boolean(root);
  root ??= await mkdtemp(path.join(tmpdir(), 'nai-r2-backup-'));
  if (!preserve) {
    t.after(() => rm(root, { recursive: true, force: true }));
  }
  const cloud = path.join(root, 'cloud');
  await mkdir(cloud);
  const envFile = path.join(root, 'r2.env'),
    keyFile = path.join(root, 'backup.key'),
    preload = path.join(root, 'preload.mjs');
  await writeFile(keyFile, randomBytes(32));
  await writeFile(
    envFile,
    `R2_ACCOUNT_ID=${'a'.repeat(32)}\nR2_BUCKET=synthetic-images\nR2_ACCESS_KEY_ID=synthetic-access\nR2_SECRET_ACCESS_KEY=synthetic-secret\n`,
  );
  await writeFile(
    preload,
    `
    import { readdir, stat, unlink } from 'node:fs/promises';
    import path from 'node:path';
    import { appendFileSync, createReadStream, createWriteStream, readFileSync, writeFileSync, statSync, lstatSync, readdirSync } from 'node:fs';
    import { Readable } from 'node:stream';
    import { pipeline } from 'node:stream/promises';
    const root = ${JSON.stringify(cloud)};
    globalThis.fetch = async (url, options) => {
      if (!url.hostname.endsWith('.r2.cloudflarestorage.com')) throw new Error('Unexpected host');
      const key = decodeURIComponent(url.pathname.split('/').slice(2).join('/'));
      const file = path.join(root, encodeURIComponent(key));
      if (process.env.R2_TEST_TRACE) {
        appendFileSync(process.env.R2_TEST_TRACE, options.method + ' ' + key + '\\n');
      }
      if (url.search) {
        const names = (await readdir(root)).filter(name => decodeURIComponent(name).startsWith('backups/')).sort();
        const rows = await Promise.all(names.map(async name => {
          const listedKey = decodeURIComponent(name);
          const size = listedKey.startsWith('backups/cutover-') && process.env.R2_TEST_CUTOVER_BYTES
            ? process.env.R2_TEST_CUTOVER_BYTES
            : (await stat(path.join(root, name))).size;
          return '<Contents><Key>' + listedKey + '</Key><Size>' + size + '</Size></Contents>';
        }));
        if (process.env.R2_TEST_REPEAT_NEWEST === '1' && rows.length) {
          rows.push(rows.at(-1));
        }
        return new Response('<ListBucketResult><IsTruncated>false</IsTruncated>' + rows.join('') + '</ListBucketResult>');
      }
      if (options.method === 'PUT') {
        if (Buffer.isBuffer(options.body)) {
          throw new Error('Expected a file stream for backup uploads');
        }
        await pipeline(options.body, createWriteStream(file, { flags: 'wx' }));
        if (process.env.R2_TEST_CORRUPT_UPLOAD === '1') {
          const stored = readFileSync(file);
          stored[stored.length - 1] ^= 1;
          writeFileSync(file, stored);
        }
        return new Response(null);
      }
      if (options.method === 'DELETE') { await unlink(file); return new Response(null, { status: 204 }); }
      if (process.env.R2_TEST_FAIL_READ === '1') return new Response(null, { status: 503 });
      const length = (await stat(file)).size;
      const body = Readable.toWeb(createReadStream(file), { strategy: { highWaterMark: 1 } });
      return new Response(body, { headers: { 'content-length': String(length) } });
    };
    process.on('exit', () => {
      if (process.env.R2_TEST_METRICS) {
        const bytes = dir => readdirSync(dir).reduce((sum, name) => {
          const file = path.join(dir, name), info = lstatSync(file);
          return sum + (info.isSymbolicLink() ? 0 : info.isDirectory() ? bytes(file) : info.size);
        }, 0);
        const archive = process.argv[2] === 'upload' ? process.argv[3] : process.argv[4];
        // getrusage can include the parent's RSS before exec; Linux VmHWM measures this process image.
        const status = readFileSync('/proc/self/status', 'utf8');
        const peakRssBytes = Number(/^VmHWM:\\s+(\\d+) kB$/m.exec(status)[1]) * 1024;
        writeFileSync(process.env.R2_TEST_METRICS, JSON.stringify({ peakRssBytes,
          archiveBytes: statSync(archive).size, retainedLogicalBytes: bytes(process.env.R2_TEST_ROOT) }));
      }
    });`,
  );
  const scriptsLink = path.join(root, 'scripts-link');
  await symlink(fileURLToPath(new URL('../../scripts/', import.meta.url)), scriptsLink, 'junction');
  const cli = path.join(scriptsLink, 'r2-backup.mjs'),
    run = promisify(execFile);
  const call = (args, env = {}) =>
    run(process.execPath, ['--import', pathToFileURL(preload).href, cli, ...args], {
      windowsHide: true,
      env: { ...process.env, R2_TEST_FAIL_READ: '0', ...env },
    });
  return {
    root,
    cloud,
    envFile,
    keyFile,
    preload,
    call,
    encode: (key) => path.join(cloud, encodeURIComponent(key)),
  };
}
