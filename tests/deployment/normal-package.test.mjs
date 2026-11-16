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
      'scripts/resolve-legacy-retained-dependencies.mjs',
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

function packageReleaseHook(f, hold) {
  const require = createRequire(path.join(root, 'package.json'));
  return `
import { mock } from 'node:test';
import Database from ${JSON.stringify(pathToFileURL(require.resolve('better-sqlite3')).href)};
import { getPatcher } from ${JSON.stringify(pathToFileURL(require.resolve('webdav')).href)};
import { R2Storage } from ${JSON.stringify(pathToFileURL(path.join(root, 'server/storage/r2-storage.js')).href)};
import { RelationalReplicaStore } from ${JSON.stringify(pathToFileURL(path.join(root, 'server/storage/relational-replica-store.js')).href)};
mock.timers.enable({apis:['setTimeout','Date'],now:${Date.now() + 2 * 86400000}});
registerHooks({load(url,context,next){
  const result=next(url,context);
  if(url!==${JSON.stringify(pathToFileURL(path.join(root, 'server/gateway-runtime.js')).href)})return result;
  let source=String(result.source);
  for(const [marker,observation] of [
    ['replicaCopy = (async () => {','process.send?.({replicaRoundStarted:true});'],
    ['scheduleReplicaCopy(progress ? 1000 : 60000);','process.send?.({replicaRoundDone:true,replicaProgress:progress});'],
  ]){
    if(source.split(marker).length!==2)throw Error('compiled replica round observation marker missing');
    source=source.replace(marker,marker+observation);
  }
  process.send?.({compiledReplicaHook:true});
  return {...result,source};
}});
const select=RelationalReplicaStore.prototype.readDueArtifactRelease;
RelationalReplicaStore.prototype.readDueArtifactRelease=function(...args){
  const row=select.apply(this,args);
  process.send?.({selector:{afterId:args[1],afterKind:args[2],id:row?.artifactId,kind:row?.kind}});
  return row;
};
const remove=R2Storage.prototype.removeImage;
R2Storage.prototype.removeImage=async function(...args){
  process.send?.({removeImageEntered:true});
  const result=await remove.apply(this,args);
  process.send?.({removeImageCompleted:true});return result;
};
const close=Database.prototype.close;
Database.prototype.close=function(...args){
  const main=this.name===${JSON.stringify(f.filename)},result=close.apply(this,args);
  if(main)process.send?.({replica:'dbclosed'});return result;
};
let release;const objects=new Map(),actualFetch=globalThis.fetch;
process.on('message',message=>{
  if(message?.tick){mock.timers.tick(message.tick);process.send?.({tick:message.tick});}
  if(message==='release-replica')release?.();
});
getPatcher().patch('fetch',(...args)=>globalThis.fetch(...args));
globalThis.fetch=async(address,options)=>{
  const url=new URL(address),cold=url.hostname==='dav.pikpak.test';
  if(url.hostname==='127.0.0.1')return actualFetch(address,options);
  if(!cold&&!url.hostname.endsWith('.r2.cloudflarestorage.com'))throw Error('Unexpected package endpoint');
  if(!cold&&url.searchParams.get('list-type')==='2')
    return new Response('<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>',{headers:{'content-type':'application/xml'}});
  process.send?.({replica:(cold?'pikpak:':'')+options.method});
  if(options.method==='PUT'){objects.set(url.href,Buffer.from(options.body));return new Response(null,{status:204});}
  if(options.method==='DELETE'){
    if(cold)throw Error('No PikPak source deletion');
    if(${hold})await new Promise(resolve=>release=resolve);
    objects.delete(url.href);return new Response(null,{status:204});
  }
  if(options.method!=='GET'||!objects.has(url.href))throw Error('Missing package replica');
  const bytes=objects.get(url.href);return new Response(bytes,{headers:{'content-length':String(bytes.length)}});
};
`;
}

async function packageReleaseRuntime(t, hold = false) {
  const f = await fixture(t),
    boot = run(f, 'scripts/bootstrap.mjs');
  await boot.finished;
  assert.deepEqual(boot.exit, { code: 0, signal: null }, boot.stderr);
  const image = Buffer.from('UklGRhoAAABXRUJQVlA4TA4AAAAvAUAAAJj/+Z///ofCAw==', 'base64'),
    upstream = createServer(async (req, res) => {
      for await (const _ of req) {
      }
      res.setHeader('content-type', req.url === '/user/data' ? 'application/json' : 'image/webp');
      res.end(
        req.url === '/user/data'
          ? JSON.stringify({
              subscription: {
                active: true,
                tier: 3,
                trainingStepsLeft: { fixedTrainingStepsLeft: 1000, purchasedTrainingSteps: 0 },
                usage: { percent: 0, isNegative: false },
              },
            })
          : image,
      );
    });
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  t.after(async () => {
    upstream.closeAllConnections();
    await new Promise((resolve) => upstream.close(resolve));
  });
  const origin = `http://127.0.0.1:${upstream.address().port}`,
    online = {
      PACKAGE_LOCAL_UPSTREAM: origin,
      NOVELAI_API_URL: origin,
      NOVELAI_ACCOUNT_API_URL: origin,
      NOVELAI_STREAM_IMAGE: 'false',
      SHUTDOWN_GRACE_MS: '10000',
    },
    credentials = JSON.parse(
      await readFile(path.join(f.env.DATA_DIR, 'bootstrap-credentials.json'), 'utf8'),
    );
  f.filename = path.join(f.env.DATA_DIR, 'library.sqlite');
  const query = (sql, ...args) => {
    const db = new Database(f.filename, { readonly: true, fileMustExist: true });
    try {
      return db.prepare(sql).all(...args);
    } finally {
      db.close();
    }
  };
  const accountId = query('SELECT id FROM upstream_accounts')[0].id,
    warm = await start(t, f, online);
  const enabled = await call(warm, `/api/admin/accounts/${accountId}`, {
    method: 'PATCH',
    headers: { 'x-admin-token': f.env.ADMIN_TOKEN, 'idempotency-key': 'package-release-enable' },
    body: { enabled: true },
  });
  assert.equal(enabled.status, 200, enabled.text);
  const generated = await call(warm, '/ai/generate-image', {
    method: 'POST',
    headers: { authorization: `Bearer ${credentials.accounts[0].api_key}` },
    body: {
      input: 'synthetic package release',
      model: 'nai-diffusion-4-5-full',
      action: 'generate',
      parameters: { width: 128, height: 128, steps: 23, seed: 42 },
    },
  });
  assert.equal(generated.status, 200, generated.text);
  const paused = await call(warm, `/api/admin/accounts/${accountId}`, {
    method: 'PATCH',
    headers: { 'x-admin-token': f.env.ADMIN_TOKEN, 'idempotency-key': 'package-release-pause' },
    body: { enabled: false },
  });
  assert.equal(paused.status, 200, paused.text);
  await warm.stop();
  const { artifact_id: artifactId, object_key: localPath } = query(
      "SELECT r.artifact_id,r.object_key FROM artifact_replicas r JOIN storage_targets s ON s.id=r.storage_target_id WHERE s.kind='local'",
    )[0],
    { R2Storage, r2Config } = await load('server/storage/r2-storage.js'),
    storage = {
      R2_ACCOUNT_ID: 'a'.repeat(32),
      R2_BUCKET: 'package-replicas',
      R2_ACCESS_KEY_ID: 'synthetic-access',
      R2_SECRET_ACCESS_KEY: 'synthetic-secret',
      PIKPAK_WEBDAV_URL: 'https://dav.pikpak.test/archive/',
      PIKPAK_WEBDAV_USERNAME: 'synthetic-user',
      PIKPAK_WEBDAV_PASSWORD: 'synthetic-password',
    },
    r2 = new R2Storage(r2Config(storage));
  r2.listBackups = async () => [];
  const retained = await load('server/storage/retained-sources.js'),
    { describeRetainedSnapshot } = await load('scripts/retained-sources.mjs'),
    registry = path.join(f.directory, 'registry'),
    backups = path.join(f.directory, 'backups'),
    initialized = await retained.initializeRetainedSources(registry);
  await mkdir(backups);
  // Same capture -> describe -> immutable binding -> checkpoint -> adopt chain as the source fixture.
  // The Linux release check mounts only this test file, so every owner comes from the actual package.
  async function captureSnapshot() {
    const snapshot = path.join(backups, randomUUID()),
      capture = await retained.beginRetainedCapture(registry, 'daily', snapshot);
    await mkdir(path.join(snapshot, 'data'), { recursive: true });
    await mkdir(path.join(snapshot, 'config'));
    const filename = path.join(snapshot, 'data/library.sqlite'),
      current = new Database(f.filename, { readonly: true, fileMustExist: true });
    try {
      await current.backup(filename);
    } finally {
      current.close();
    }
    const sealed = new Database(filename);
    try {
      sealed.pragma('wal_checkpoint(TRUNCATE)');
      sealed.pragma('journal_mode=DELETE');
    } finally {
      sealed.close();
    }
    const scope = path.join(snapshot, 'config/retained-inventory-scope.json');
    await writeFile(
      scope,
      JSON.stringify({
        format: 'novelai-retained-inventory-scope-v1',
        registryId: initialized.registryId,
        localRoots: [backups],
        archiveRoots: [],
        r2Namespaces: [`r2://${r2.config.accountId}/${r2.config.bucket}`],
      }),
      { flag: 'wx', mode: 0o600 },
    );
    const dependencies = path.join(snapshot, 'retained-dependencies.jsonl'),
      binding = await describeRetainedSnapshot(filename, dependencies, f.env.DATA_DIR),
      manifest = path.join(snapshot, 'manifest.json');
    await writeFile(
      manifest,
      JSON.stringify({
        files: {
          'data/library.sqlite': binding.sourceSha256,
          'retained-dependencies.jsonl': binding.dependenciesSha256,
          'config/retained-inventory-scope.json': await retained.retainedFileSha256(scope),
        },
        retainedSource: { ...capture, binding },
      }),
      { flag: 'wx', mode: 0o600 },
    );
    await retained.bindRetainedCapture(
      registry,
      capture.sourceId,
      manifest,
      { ...binding, manifestSha256: await retained.retainedFileSha256(manifest) },
      dependencies,
    );
    await retained.publishRetainedLocal(registry, capture.sourceId, snapshot);
    const checkpoint = path.join(f.directory, 'checkpoint-' + randomUUID()),
      proof = await retained.checkpointRetainedSources(registry, checkpoint);
    await retained.adoptRetainedInventory(
      registry,
      {
        checkpoint,
        checkpointSha256: proof.sha256,
        scope,
        scopeSha256: await retained.retainedFileSha256(scope),
      },
      { r2 },
    );
  }
  await captureSnapshot();
  await writeFile(f.preload, (await readFile(f.preload, 'utf8')) + packageReleaseHook(f, hold));
  const server = await start(t, f, {
      ...online,
      ...storage,
      RETAINED_SOURCES_DIR: registry,
    }),
    messages = [];
  server.child.on('message', (message) => messages.push(message));
  const tick = async (n, round = true) => {
    const previous = messages.filter((message) => message.replicaRoundDone).length,
      acknowledgements = messages.filter((message) => message.tick === n).length;
    server.child.send({ tick: n });
    await waitFor(
      () => messages.filter((message) => message.tick === n).length > acknowledgements,
      server,
    );
    if (round)
      await waitFor(
        () => messages.filter((message) => message.replicaRoundDone).length > previous,
        server,
      );
  };
  assert.equal(
    messages.some((message) => message.replicaRoundStarted),
    false,
  );
  await tick(60000);
  assert.equal(messages.at(-1).replicaProgress, true);
  await tick(1000);
  assert.equal(messages.at(-1).replicaProgress, false, 'Local-only retained source pins Local');
  const previous = messages.filter((message) => message.replicaRoundStarted).length;
  await tick(1000, false);
  assert.equal(messages.filter((message) => message.replicaRoundStarted).length, previous);
  await tick(7 * 86400000 + 1);
  assert.equal(messages.filter((message) => message.replica === 'pikpak:PUT').length, 1);
  await captureSnapshot();
  await tick(1000);
  assert.equal(
    messages.at(-1).replicaProgress,
    false,
    'new capture does not erase the older Local hold',
  );
  return { f, server, messages, tick, query, artifactId, localPath, image };
}

test(
  '普通 JS 发布包：retained pin不饿死copy，同artifact按(id,kind)继续R2释放',
  {
    timeout: 90000,
    skip:
      process.platform === 'win32'
        ? 'compiled source release requires Linux directory durability'
        : false,
  },
  async (t) => {
    const { f, server, messages, tick, query, artifactId, localPath, image } =
        await packageReleaseRuntime(t),
      previous = messages.filter((message) => message.replicaRoundStarted).length;
    await tick(1000, false);
    assert.equal(messages.filter((message) => message.replicaRoundStarted).length, previous);
    await tick(59000);
    assert.equal(messages.at(-1).replicaProgress, true);
    assert.equal(messages.filter((message) => message.replica === 'DELETE').length, 1);
    assert.ok(messages.some((message) => message.removeImageCompleted));
    assert.ok(
      messages.some(
        (message) =>
          message.selector?.afterId === artifactId &&
          message.selector.afterKind === 'local' &&
          message.selector.id === artifactId &&
          message.selector.kind === 'r2',
      ),
    );
    const rows = query(
      'SELECT s.kind,r.deleted_at FROM artifact_replicas r JOIN storage_targets s ON s.id=r.storage_target_id WHERE r.artifact_id=?',
      artifactId,
    );
    assert.equal(rows.find((row) => row.kind === 'local').deleted_at, null);
    assert.equal(typeof rows.find((row) => row.kind === 'r2').deleted_at, 'number');
    assert.equal(rows.find((row) => row.kind === 'pikpak').deleted_at, null);
    assert.equal(
      messages.some((message) => message.replica === 'pikpak:DELETE'),
      false,
    );
    assert.deepEqual(await readFile(path.join(f.env.DATA_DIR, localPath)), image);
    await server.stop();
    assert.ok(messages.some((message) => message.replica === 'dbclosed'));
  },
);

test(
  '普通 JS 发布包：shutdown等待真实held R2 DELETE后才确认释放并关闭主库',
  {
    timeout: 90000,
    skip:
      process.platform === 'win32'
        ? 'compiled source release requires Linux directory durability'
        : false,
  },
  async (t) => {
    const { f, server, messages, tick, query, artifactId, localPath, image } =
      await packageReleaseRuntime(t, true);
    await tick(60000, false);
    await waitFor(() => messages.some((message) => message.replica === 'DELETE'), server);
    assert.ok(messages.some((message) => message.removeImageEntered));
    const releasedAt = () =>
      query(
        "SELECT r.deleted_at FROM artifact_replicas r JOIN storage_targets s ON s.id=r.storage_target_id WHERE r.artifact_id=? AND s.kind='r2'",
        artifactId,
      )[0].deleted_at;
    assert.equal(releasedAt(), null);
    server.child.send('shutdown');
    await waitFor(() => server.stdout.includes('SIGTERM received'), server);
    await delay(50);
    assert.equal(server.exit, null);
    assert.equal(
      messages.some((message) => message.replica === 'dbclosed'),
      false,
    );
    assert.equal(
      messages.some((message) => message.removeImageCompleted),
      false,
    );
    assert.equal(releasedAt(), null);
    server.child.send('release-replica');
    await server.finished;
    assert.deepEqual(server.exit, { code: 0, signal: null }, server.stderr);
    assert.ok(messages.some((message) => message.removeImageCompleted));
    assert.ok(messages.some((message) => message.replica === 'dbclosed'));
    assert.equal(typeof releasedAt(), 'number');
    assert.deepEqual(await readFile(path.join(f.env.DATA_DIR, localPath)), image);
  },
);
