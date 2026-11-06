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
  if (!preserve) t.after(() => rm(root, { recursive: true, force: true }));
  const cloud = path.join(root, 'cloud'); await mkdir(cloud);
  const envFile = path.join(root, 'r2.env'), keyFile = path.join(root, 'backup.key'), preload = path.join(root, 'preload.mjs');
  await writeFile(keyFile, randomBytes(32));
  await writeFile(envFile, `R2_ACCOUNT_ID=${'a'.repeat(32)}\nR2_BUCKET=synthetic-images\nR2_ACCESS_KEY_ID=synthetic-access\nR2_SECRET_ACCESS_KEY=synthetic-secret\n`);
  await writeFile(preload, `
    import { readFile, writeFile, readdir, stat, unlink } from 'node:fs/promises';
    import path from 'node:path';
    import { writeFileSync, statSync, lstatSync, readdirSync } from 'node:fs';
    const root = ${JSON.stringify(cloud)};
    globalThis.fetch = async (url, options) => {
      if (!url.hostname.endsWith('.r2.cloudflarestorage.com')) throw new Error('Unexpected host');
      const key = decodeURIComponent(url.pathname.split('/').slice(2).join('/'));
      const file = path.join(root, encodeURIComponent(key));
      if (url.search) {
        const rows = await Promise.all((await readdir(root)).map(async name => '<Contents><Key>' + decodeURIComponent(name)
          + '</Key><Size>' + (process.env.R2_TEST_LIST_BYTES || (await stat(path.join(root, name))).size) + '</Size></Contents>'));
        return new Response('<ListBucketResult><IsTruncated>false</IsTruncated>' + rows.join('') + '</ListBucketResult>');
      }
      if (options.method === 'PUT') { await writeFile(file, options.body, { flag: 'wx' }); return new Response(null); }
      if (options.method === 'DELETE') { await unlink(file); return new Response(null, { status: 204 }); }
      if (process.env.R2_TEST_FAIL_READ === '1') return new Response(null, { status: 503 });
      return new Response(await readFile(file));
    };
    process.on('exit', () => {
      if (process.env.R2_TEST_METRICS) {
        const bytes = dir => readdirSync(dir).reduce((sum, name) => {
          const file = path.join(dir, name), info = lstatSync(file);
          return sum + (info.isSymbolicLink() ? 0 : info.isDirectory() ? bytes(file) : info.size);
        }, 0);
        writeFileSync(process.env.R2_TEST_METRICS, JSON.stringify({ peakRssBytes: process.resourceUsage().maxRSS * 1024,
          archiveBytes: statSync(process.argv[3]).size, retainedLogicalBytes: bytes(process.env.R2_TEST_ROOT) }));
      }
    });`);
  const scriptsLink = path.join(root, 'scripts-link');
  await symlink(fileURLToPath(new URL('../../scripts/', import.meta.url)), scriptsLink, 'junction');
  const cli = path.join(scriptsLink, 'r2-backup.mjs'), run = promisify(execFile);
  const call = (args, env = {}) => run(process.execPath, ['--import', pathToFileURL(preload).href, cli, ...args], {
    windowsHide: true, env: { ...process.env, R2_TEST_FAIL_READ: '0', R2_TEST_LIST_BYTES: '', ...env }
  });
  return { root, cloud, envFile, keyFile, call, encode: key => path.join(cloud, encodeURIComponent(key)) };
}
