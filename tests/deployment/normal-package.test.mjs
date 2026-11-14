import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { createServer, request } from 'node:http';
import { createRequire } from 'node:module';
import {
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  realpath,
  rm,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = path.resolve(
    process.env.NORMAL_PACKAGE_ROOT ||
      fileURLToPath(new URL('../../.output/backend', import.meta.url)),
  ),
  load = (name) => import(pathToFileURL(path.join(root, name)).href),
  Database = createRequire(path.join(root, 'package.json'))('better-sqlite3'),
  sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex'),
  delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// The same checks run against the local build and the extracted Linux release, without source imports.
async function ownedFiles(directory = root, prefix = '') {
  const files = {};
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    if (entry.name === 'node_modules') continue;
    const filename = path.join(directory, entry.name),
      name = prefix + entry.name;
    if (entry.isDirectory()) Object.assign(files, await ownedFiles(filename, name + '/'));
    else {
      assert.ok(entry.isFile(), `Unexpected runtime link: ${name}`);
      files[name] = sha256(await readFile(filename));
    }
  }
  return files;
}

async function fixture(t) {
  const directory = await mkdtemp(path.join(tmpdir(), 'nai-normal-package-')),
    children = [];
  t.after(async () => {
    for (const state of children) {
      if (!state.exit) {
        state.child.kill('SIGKILL');
        await state.finished;
      }
    }
    await rm(directory, { recursive: true, force: true });
  });
  const marker = path.join(directory, 'denied-network'),
    preload = path.join(directory, 'network-guard.mjs');
  await writeFile(
    preload,
    `
import { registerHooks, syncBuiltinESMExports } from 'node:module';
import { appendFileSync } from 'node:fs';
import http from 'node:http'; import https from 'node:https'; import net from 'node:net'; import tls from 'node:tls';
const deny=()=>{appendFileSync(${JSON.stringify(marker)}, 'denied\\n');throw new Error('Non-local or offline package IO rejected');};
registerHooks({load(url,context,next){if(/\\.(?:ts|tsx|mts|cts)$/.test(url))throw new Error('Owned TypeScript is forbidden in normal package');return next(url,context);}});
const fetch=globalThis.fetch;
globalThis.fetch=(input,...args)=>{
  if(!process.env.PACKAGE_LOCAL_UPSTREAM||new URL(typeof input==='string'||input instanceof URL?input:input.url).origin!==process.env.PACKAGE_LOCAL_UPSTREAM)deny();
  return fetch(input,...args);
};
for(const owner of [http,https]){owner.request=deny;owner.get=deny;}
for(const [owner,key] of [[net.Socket.prototype,'connect'],[tls,'connect']]){
  const original=owner[key];owner[key]=function(...args){
    let first=Array.isArray(args[0])?args[0][0]:args[0];
    const host=typeof first==='object'?first?.host:typeof args[1]==='string'?args[1]:null;
    if(!process.env.PACKAGE_LOCAL_UPSTREAM||!['127.0.0.1','::1','localhost'].includes(host))deny();
    return original.apply(this,args);
  };
}
if(!process.env.PACKAGE_LOCAL_UPSTREAM)net.Server.prototype.listen=deny;
syncBuiltinESMExports();
if(process.send)process.on('message',message=>{if(message==='shutdown')process.emit('SIGTERM');});
`,
  );
  const env = Object.fromEntries(
    ['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE']
      .filter((key) => process.env[key])
      .map((key) => [key, process.env[key]]),
  );
  return {
    directory,
    children,
    marker,
    preload,
    env: {
      ...env,
      ADMIN_TOKEN: randomUUID() + randomUUID(),
      NOVELAI_TOKEN: 'pst-synthetic-normal-package-0123456789',
      NODE_NO_WARNINGS: '1',
      DATA_DIR: path.join(directory, 'data'),
      HOST: '127.0.0.1',
      PORT: '0',
    },
  };
}

function run(f, entry, args = [], patch = {}) {
  const child = spawn(
    process.execPath,
    [
      '--no-experimental-strip-types',
      '--import',
      pathToFileURL(f.preload).href,
      path.join(root, entry),
      ...args,
    ],
    {
      cwd: root,
      env: { ...f.env, ...patch },
      windowsHide: true,
      stdio:
        entry === 'server/index.js'
          ? ['ignore', 'pipe', 'pipe', 'ipc']
          : ['ignore', 'pipe', 'pipe'],
    },
  );
  const state = { child, stdout: '', stderr: '', exit: null, origin: null };
  child.stdout.on('data', (bytes) => {
    state.stdout += bytes;
    const match = state.stdout.match(/Nai2API listening on http:\/\/127\.0\.0\.1:(\d+)/);
    if (match) state.origin = 'http://127.0.0.1:' + match[1];
  });
  child.stderr.on('data', (bytes) => (state.stderr += bytes));
  state.finished = once(child, 'exit').then(([code, signal]) => (state.exit = { code, signal }));
  f.children.push(state);
  return state;
}

async function waitFor(predicate, state) {
  const until = Date.now() + 15000;
  while (!predicate()) {
    assert.ok(Date.now() < until, state.stderr || state.stdout || 'Packaged child did not finish');
    await delay(10);
  }
}

async function start(t, f, patch) {
  const state = run(f, 'server/index.js', [], patch);
  t.after(async () => {
    if (!state.exit) {
      state.child.kill('SIGKILL');
      await state.finished;
    }
  });
  await waitFor(() => state.origin || state.exit, state);
  assert.ok(state.origin, state.stderr);
  state.stop = async () => {
    state.child.send('shutdown');
    await waitFor(() => state.exit, state);
    assert.deepEqual(state.exit, { code: 0, signal: null }, state.stderr);
  };
  return state;
}

async function call(server, route, { method = 'GET', headers = {}, body } = {}) {
  return new Promise((resolve, reject) => {
    const outgoing = request(
      server.origin + route,
      {
        method,
        headers: {
          origin: server.origin,
          ...headers,
          ...(body ? { 'content-type': 'application/json' } : {}),
        },
        signal: AbortSignal.timeout(15000),
      },
      async (incoming) => {
        const chunks = [];
        try {
          for await (const bytes of incoming) chunks.push(bytes);
          resolve({ status: incoming.statusCode, text: Buffer.concat(chunks).toString() });
        } catch (error) {
          reject(error);
        }
      },
    );
    outgoing.on('error', reject);
    outgoing.end(body ? JSON.stringify(body) : undefined);
  });
}

test(
  '普通 JS 发布包：离线 fresh bootstrap、真实暂停入口/管理员启用、公共及私有联合恢复、重启',
  { timeout: 90000 },
  async (t) => {
    const before = await ownedFiles(),
      manifest = JSON.parse(await readFile(path.join(root, 'package.json'), 'utf8'));
    assert.equal(manifest.main, 'server/index.js');
    assert.deepEqual(Object.keys(manifest.scripts).sort(), ['bootstrap', 'restore', 'start']);
    assert.equal(manifest.devDependencies, undefined);
    for (const cli of [
      'bootstrap.mjs',
      'restore-relational.mjs',
      'r2-backup.mjs',
      'mock-novelai.mjs',
    ]) {
      assert.ok(before['scripts/' + cli], cli);
    }
    const { encryptBackup, decryptBackup } = await load('scripts/r2-backup.mjs'),
      backupBytes = Buffer.from('synthetic normal-package business backup'),
      backupKey = Buffer.alloc(32, 17);
    assert.deepEqual(decryptBackup(encryptBackup(backupBytes, backupKey), backupKey), backupBytes);
    if (process.env.NORMAL_PACKAGE_DEPENDENCIES) {
      assert.ok(
        createRequire(path.join(root, 'package.json'))
          .resolve('better-sqlite3')
          .startsWith((await realpath(process.env.NORMAL_PACKAGE_DEPENDENCIES)) + path.sep),
      );
    }
    assert.ok(
      Object.keys(before).every(
        (name) =>
          /\.(?:[cm]?js|json|md)$/.test(name) || ['LICENSE', 'UPSTREAM-LICENSE'].includes(name),
      ),
    );
    for (const retired of [
      'server/storage/sqlite-store.js',
      'server/storage/sqlite-migrations.js',
      'server/storage/sqlite-read-worker.js',
      'scripts/replay-events.mjs',
      'scripts/create-test-copy.mjs',
    ]) {
      assert.equal(before[retired], undefined, retired);
    }
    assert.ok(!Object.keys(before).some((name) => name.startsWith('server/migrations/')));
    const f = await fixture(t),
      boot = run(f, 'scripts/bootstrap.mjs');
    await boot.finished;
    assert.deepEqual(boot.exit, { code: 0, signal: null }, boot.stderr);
    const mockCli = run(f, 'scripts/mock-novelai.mjs', ['--unknown', '1']);
    await mockCli.finished;
    assert.equal(mockCli.exit.code, 1);
    assert.match(mockCli.stderr, /Usage: mock-novelai\.mjs/);
    const credentials = JSON.parse(
        await readFile(path.join(f.env.DATA_DIR, 'bootstrap-credentials.json'), 'utf8'),
      ),
      sourcePath = path.join(f.directory, 'selected.sqlite');
    let publicNetworkCalls = 0;
    t.mock.method(globalThis, 'fetch', () => {
      publicNetworkCalls++;
      throw new Error('Public replay and selected private installation must stay offline');
    });
    await copyFile(path.join(f.env.DATA_DIR, 'library.sqlite'), sourcePath);
    const source = new Database(sourcePath, { readonly: true }),
      { relationalSnapshotSha256, sameRelationalPrivateState } = await load(
        'server/storage/relational-private-restore.js',
      ),
      { initializeRelationalStorage, assertRelationalStorageReady } = await load(
        'server/storage/relational-storage.js',
      ),
      { RelationalCommitReplayer } = await load('server/storage/relational-replay.js'),
      { RelationalInputStore } = await load('server/storage/relational-input-store.js'),
      { readRelationalCommit } = await load('server/events/relational-event-store.js'),
      { verifyAndInstallRelationalStorage } = await load('server/storage/relational-ready.js'),
      selected = {
        sourcePath,
        sourceSha256: relationalSnapshotSha256(sourcePath),
        historyId: source.prepare('SELECT history_id id FROM projection_state').get().id,
        finalCommitSeq: source
          .prepare('SELECT CAST(MAX(commit_seq) AS TEXT) seq FROM commits')
          .get().seq,
        replicas: { dataDir: f.env.DATA_DIR },
      },
      accountId = source.prepare('SELECT id FROM upstream_accounts').get().id;
    source.pragma('foreign_keys=ON');
    assert.deepEqual(source.prepare('SELECT enabled FROM upstream_accounts').get(), { enabled: 0 });
    assert.equal(
      source.prepare("SELECT COUNT(*) n FROM domain_events WHERE event_type='QuotaObserved'").get()
        .n,
      0,
    );
    const publicOnly = new Database(path.join(f.directory, 'public.sqlite'));
    try {
      initializeRelationalStorage(publicOnly, selected.historyId);
      const replayer = new RelationalCommitReplayer(publicOnly),
        inputs = new RelationalInputStore(source);
      for (const { seq } of source
        .prepare('SELECT CAST(commit_seq AS TEXT) seq FROM commits ORDER BY commit_seq')
        .all()) {
        publicOnly.transaction(() =>
          replayer.replayCommit(readRelationalCommit(source, seq), inputs),
        )();
      }
      for (const table of [
        'api_keys',
        'password_credentials',
        'upstream_credentials',
        'member_sessions',
      ]) {
        assert.equal(publicOnly.prepare(`SELECT COUNT(*) n FROM ${table}`).get().n, 0);
      }
      assert.throws(() => assertRelationalStorageReady(publicOnly), /incomplete/);
      const unchanged = publicOnly.serialize();
      await assert.rejects(
        verifyAndInstallRelationalStorage(publicOnly, {
          ...selected,
          finalCommitSeq: String(BigInt(selected.finalCommitSeq) - 1n),
        }),
        /cut/,
      );
      assert.deepEqual(publicOnly.serialize(), unchanged);
      await verifyAndInstallRelationalStorage(publicOnly, selected);
      assertRelationalStorageReady(publicOnly);
      assert.equal(sameRelationalPrivateState(source, publicOnly), true);
      assert.equal(publicNetworkCalls, 0);
    } finally {
      publicOnly.close();
      source.close();
    }
    const restoredDir = path.join(f.directory, 'restored');
    await mkdir(restoredDir);
    const restore = run(f, 'scripts/restore-relational.mjs', [
      '--source',
      selected.sourcePath,
      '--sha256',
      selected.sourceSha256,
      '--history',
      selected.historyId,
      '--cut',
      selected.finalCommitSeq,
      '--output',
      path.join(restoredDir, 'library.sqlite'),
      '--data-dir',
      f.env.DATA_DIR,
    ]);
    await restore.finished;
    assert.deepEqual(restore.exit, { code: 0, signal: null }, restore.stderr);
    assert.equal(JSON.parse(restore.stdout).restored, true);
    assert.equal(relationalSnapshotSha256(sourcePath), selected.sourceSha256);
    const rejected = run(f, 'scripts/restore-relational.mjs', [
      '--source',
      selected.sourcePath,
      '--sha256',
      selected.sourceSha256,
      '--history',
      selected.historyId,
      '--cut',
      selected.finalCommitSeq,
      '--output',
      path.join(restoredDir, 'library.sqlite'),
      '--data-dir',
      f.env.DATA_DIR,
    ]);
    await rejected.finished;
    assert.equal(rejected.exit.code, 1);
    assert.equal(JSON.parse(rejected.stderr).phase, 'paths');
    assert.equal((await readdir(f.directory)).includes('denied-network'), false);

    const upstream = { quota: 0, generation: 0 },
      mock = createServer(async (req, res) => {
        for await (const _ of req) {
        }
        if (req.url !== '/user/data') {
          upstream.generation++;
          res.writeHead(500).end();
          return;
        }
        upstream.quota++;
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(
          JSON.stringify({
            subscription: {
              active: true,
              tier: 3,
              trainingStepsLeft: { fixedTrainingStepsLeft: 1000, purchasedTrainingSteps: 0 },
              usage: { percent: 0, isNegative: false },
            },
          }),
        );
      });
    mock.listen(0, '127.0.0.1');
    await once(mock, 'listening');
    t.after(async () => {
      mock.closeAllConnections();
      await new Promise((resolve) => mock.close(resolve));
    });
    const origin = `http://127.0.0.1:${mock.address().port}`,
      online = {
        PACKAGE_LOCAL_UPSTREAM: origin,
        NOVELAI_API_URL: origin,
        NOVELAI_ACCOUNT_API_URL: origin,
      };
    for (const dataDir of [f.env.DATA_DIR, restoredDir]) {
      const server = await start(t, f, { ...online, DATA_DIR: dataDir }),
        member = credentials.accounts[0],
        count = upstream.quota;
      assert.equal((await call(server, '/api/health')).status, 200);
      assert.equal((await call(server, '/api/admin/ping')).status, 403);
      await delay(100);
      assert.equal(
        upstream.quota,
        count,
        'bootstrap and public/private restore retain the paused account',
      );
      const enabled = await call(server, `/api/admin/accounts/${accountId}`, {
        method: 'PATCH',
        headers: { 'x-admin-token': f.env.ADMIN_TOKEN, 'idempotency-key': 'package-enable' },
        body: { enabled: true },
      });
      assert.equal(enabled.status, 200, enabled.text);
      assert.equal(JSON.parse(enabled.text).enabled, true);
      assert.equal(upstream.quota, count + 1);
      for (const route of ['/api/me/quota', '/user/subscription']) {
        assert.equal(
          (await call(server, route, { headers: { authorization: `Bearer ${member.api_key}` } }))
            .status,
          200,
          route,
        );
      }
      await server.stop();
    }
    const restarted = await start(t, f, { ...online, DATA_DIR: restoredDir });
    assert.equal(
      (
        await call(restarted, '/api/me/quota', {
          headers: { authorization: `Bearer ${credentials.accounts[0].api_key}` },
        })
      ).status,
      200,
    );
    await restarted.stop();
    assert.equal(upstream.generation, 0);
    assert.equal((await readdir(f.directory)).includes('denied-network'), false);
    for (const secret of [
      f.env.ADMIN_TOKEN,
      f.env.NOVELAI_TOKEN,
      ...credentials.accounts.flatMap((m) => [m.password, m.api_key]),
    ]) {
      assert.ok(!boot.stdout.includes(secret));
      assert.ok(!boot.stderr.includes(secret));
      for (const name of Object.keys(before))
        assert.ok(!(await readFile(path.join(root, name), 'utf8')).includes(secret), name);
    }
    assert.deepEqual(await ownedFiles(), before);
  },
);

test(
  '普通包拒绝 missing/旧格式/building/hash漂移，不创建或改写数据库、不监听或网络IO',
  { timeout: 45000 },
  async (t) => {
    const { initializeRelationalStorage } = await load('server/storage/relational-storage.js');
    for (const kind of ['missing', 'old', 'building', 'hash']) {
      await t.test(kind, async (t) => {
        const f = await fixture(t);
        await mkdir(f.env.DATA_DIR);
        const filename = path.join(f.env.DATA_DIR, 'library.sqlite');
        if (kind !== 'missing') {
          const db = new Database(filename);
          if (kind === 'old')
            db.exec(
              'CREATE TABLE __drizzle_migrations(id INTEGER PRIMARY KEY, hash TEXT, created_at INTEGER)',
            );
          else {
            initializeRelationalStorage(db, 'synthetic-invalid-package-history');
            if (kind === 'hash')
              db.prepare("UPDATE app_meta SET value=? WHERE key='schema_sha256'").run(
                '0'.repeat(64),
              );
          }
          db.close();
        }
        const before = await ownedFiles(f.env.DATA_DIR),
          child = run(f, 'server/index.js');
        await child.finished;
        assert.equal(child.exit.code, 1, child.stderr);
        assert.equal(child.origin, null);
        assert.deepEqual(await ownedFiles(f.env.DATA_DIR), before);
        assert.equal((await readdir(f.directory)).includes('denied-network'), false);
      });
    }
  },
);
