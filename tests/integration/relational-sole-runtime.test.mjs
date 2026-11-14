import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createHash, randomUUID } from 'node:crypto';
import { mkdtemp, readFile, readdir, writeFile, mkdir, rm } from 'node:fs/promises';
import { createServer, request } from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';
import { bootstrap } from '../../scripts/bootstrap.mjs';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { createRelationalNativeAdmission } from '../../server/generation/relational-native-admission.ts';
import { createGenerationPayloadBudget } from '../../server/generation/payload-budget.ts';
import { describeStoredResult } from '../../server/storage/result-files.ts';
import { image, meta } from '../helpers/relational-generation.mjs';
import { authenticateMemberApiKey } from '../../server/members/authentication.ts';

const root = fileURLToPath(new URL('../../', import.meta.url)),
  delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function waitFor(predicate, message = 'sole condition', timeout = 15000) {
  const until = Date.now() + timeout;
  while (!(await predicate())) {
    assert.ok(Date.now() < until, message);
    await delay(10);
  }
}
const nativeInput = () => ({
  input: 'synthetic sole native input',
  model: 'nai-diffusion-4-5-full',
  action: 'generate',
  parameters: { width: 128, height: 128, steps: 23, seed: 42 },
});
const openAiInput = () => ({
  model: 'nai-diffusion-4-5-full:k_euler_ancestral',
  messages: [
    {
      role: 'user',
      content:
        '提示词: sole OpenAI image\n画师串:\n尺寸: 方图\n提示词引导值: 5\n缩放引导值: 0\n负面提示词:',
    },
  ],
  nai: { width: 128, height: 128 },
});
async function fresh(t) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'nai-relational-sole-')),
    dataDir = path.join(dir, 'data'),
    env = {
      DATA_DIR: dataDir,
      ADMIN_TOKEN: randomUUID() + randomUUID(),
      NOVELAI_TOKEN: 'pst-sole-synthetic-credential-0123456789',
    };
  const children = [];
  t.after(async () => {
    for (const state of children) {
      if (!state.exit) {
        state.child.kill('SIGKILL');
        await state.finished;
      }
    }
    await rm(dir, { recursive: true, force: true });
  });
  const result = await bootstrap({ env, log() {} }),
    credentials = JSON.parse(await readFile(result.credentialPath, 'utf8')),
    filename = path.join(dataDir, 'library.sqlite');
  const db = new Database(filename, { readonly: true });
  const accountId = db.prepare('SELECT id FROM upstream_accounts').get().id;
  db.close();
  return { dir, dataDir, env, credentials, filename, accountId, children };
}
async function upstream(t) {
  const state = { quota: 0, generation: 0, hold: null, active: 0 },
    server = createServer(async (req, res) => {
      for await (const _chunk of req) {
      }
      if (req.url === '/user/data') {
        state.quota++;
      } else {
        state.generation++;
      }
      state.active++;
      try {
        await state.hold?.(req);
        if (req.url === '/user/data') {
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
        } else {
          res.writeHead(200, { 'content-type': 'image/webp' });
          res.end(image);
        }
      } finally {
        state.active--;
      }
    });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  });
  state.origin = `http://127.0.0.1:${server.address().port}`;
  return state;
}
async function launch(t, f, { env = {}, hook = '', port = 0 } = {}) {
  // Windows 测试用 IPC 触发现有 SIGTERM handler；不是声称 Windows 原生发送了 POSIX signal。
  const preload = String.raw`
    import { registerHooks } from 'node:module';
    registerHooks({load(url, context, next) {
      if (/\/server\/storage\/(?:sqlite-store|sqlite-migrations)\.(?:ts|js)$/.test(url))
        throw new Error('sole must not load legacy SQLite owner');
      return next(url, context);
    }});
    process.on('message', message => {if (message === 'shutdown') process.emit('SIGTERM');});
    ${hook}
  `;
  const child = spawn(
    process.execPath,
    ['--import', `data:text/javascript,${encodeURIComponent(preload)}`, 'server/index.ts'],
    {
      cwd: root,
      env: {
        PATH: process.env.PATH,
        SystemRoot: process.env.SystemRoot,
        TEMP: process.env.TEMP,
        TMP: process.env.TMP,
        HOME: process.env.HOME,
        ...f.env,
        HOST: '127.0.0.1',
        PORT: String(port),
        SHUTDOWN_GRACE_MS: '10000',
        NOVELAI_STREAM_IMAGE: 'false',
        ...(f.upstream
          ? {
              NOVELAI_API_URL: f.upstream.origin,
              NOVELAI_ACCOUNT_API_URL: f.upstream.origin,
            }
          : {}),
        ...env,
      },
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    },
  );
  const state = { child, stdout: '', stderr: '', exit: null, origin: null };
  child.stdout.on('data', (bytes) => {
    state.stdout += bytes;
    const match = state.stdout.match(/Nai2API listening on http:\/\/127\.0\.0\.1:(\d+)/);
    if (match) {
      state.origin = `http://127.0.0.1:${match[1]}`;
    }
  });
  child.stderr.on('data', (bytes) => (state.stderr += bytes));
  state.finished = once(child, 'exit').then(([code, signal]) => (state.exit = { code, signal }));
  f.children.push(state);
  state.ready = async () => {
    await waitFor(() => state.origin || state.exit, 'actual sole listener must start or reject');
    assert.ok(state.origin, state.stderr);
    return state;
  };
  state.stop = async () => {
    if (!state.exit) {
      child.send('shutdown');
    }
    await waitFor(() => state.exit, 'sole graceful shutdown did not finish', 12000);
    assert.equal(state.exit.code, 0, state.stderr);
  };
  t.after(async () => {
    if (!state.exit) {
      child.kill('SIGKILL');
      await state.finished;
    }
  });
  return state;
}
async function call(server, route, { body, token, headers, method, ...options } = {}) {
  const response = await new Promise((resolve, reject) => {
    const outgoing = request(
      server.origin + route,
      {
        ...options,
        method: method ?? (body ? 'POST' : 'GET'),
        headers: {
          origin: server.origin,
          accept: 'application/json',
          ...(body ? { 'content-type': 'application/json' } : {}),
          ...(token ? { authorization: `Bearer ${token}` } : {}),
          ...headers,
        },
        signal: AbortSignal.timeout(40000),
      },
      async (incoming) => {
        try {
          const chunks = [];
          for await (const chunk of incoming) {
            chunks.push(chunk);
          }
          const bytes = Buffer.concat(chunks);
          resolve({
            status: incoming.statusCode,
            headers: new Headers(
              Object.entries(incoming.headers).flatMap(([key, value]) =>
                value === undefined ? [] : [[key, Array.isArray(value) ? value.join(', ') : value]],
              ),
            ),
            bytes,
            json: () => JSON.parse(bytes.toString()),
          });
        } catch (error) {
          reject(error);
        }
      },
    );
    outgoing.on('error', reject);
    outgoing.end(body ? JSON.stringify(body) : undefined);
  });
  return response;
}
async function enable(f, server) {
  const response = await call(server, `/api/admin/accounts/${f.accountId}`, {
    method: 'PATCH',
    headers: { 'x-admin-token': f.env.ADMIN_TOKEN, 'idempotency-key': 'sole-enable' },
    body: { enabled: true },
  });
  assert.equal(response.status, 200, response.bytes.toString());
  assert.equal(response.json().enabled, true);
}
async function files(f) {
  const result = {};
  for (const name of await readdir(f.dataDir)) {
    if (/^library\.sqlite(?:-wal|-shm|-journal)?$/.test(name)) {
      result[name] = createHash('sha256')
        .update(await readFile(path.join(f.dataDir, name)))
        .digest('hex');
    }
  }
  return result;
}
function query(f, sql, ...args) {
  const db = new Database(f.filename, { readonly: true });
  try {
    return db.prepare(sql).all(...args);
  } finally {
    db.close();
  }
}
async function prepareEnabled(t, f) {
  f.upstream = await upstream(t);
  const server = await (await launch(t, f)).ready();
  await enable(f, server);
  await server.stop();
}
async function acceptQueued(f) {
  const db = new Database(f.filename);
  db.pragma('foreign_keys=ON');
  const members = new RelationalMemberStore(db, f.env),
    inputs = new RelationalInputStore(db, f.env),
    generations = new RelationalGenerationStore(db, f.env),
    budget = createGenerationPayloadBudget(() => 0),
    member = members.readMember(f.credentials.accounts[0].user_id),
    admit = createRelationalNativeAdmission({
      sqlite: db,
      members,
      inputs,
      generations,
      payloadBudget: budget,
      replicas: { dataDir: f.dataDir },
      env: f.env,
    });
  const result = await admit({
    token: f.credentials.accounts[0].api_key,
    body: nativeInput(),
    operation: 'generate',
    key: randomUUID(),
    diagnosticId: 'sole-test-queued',
    admissionVersion: member.admissionEpoch,
  });
  return { db, generations, result };
}
async function leaveRecoveryFiles(t, f, kind) {
  const code = `
    import Database from 'better-sqlite3';
    import {RelationalMemberStore} from ${JSON.stringify(pathToFileURL(path.join(root, 'server/storage/relational-member-store.ts')).href)};
    const db = new Database(${JSON.stringify(f.filename)});
    db.pragma('foreign_keys=ON');
    ${
      kind.includes('wal')
        ? `
      db.pragma('journal_mode=WAL'); db.pragma('wal_autocheckpoint=0');
      db.transaction(() => new RelationalMemberStore(db).setMemberStatus(${JSON.stringify(meta('sole-wal-source', Date.now()))}, ${JSON.stringify(f.credentials.accounts[0].user_id)}, 'disabled'))();
      ${kind.startsWith('bad') ? `db.prepare("UPDATE app_meta SET value='wrong' WHERE key='schema_sha256'").run();` : ''}
    `
        : `
      db.pragma('journal_mode=DELETE'); db.pragma('cache_size=2');
      ${kind.startsWith('bad') ? `db.prepare("UPDATE app_meta SET value='wrong' WHERE key='schema_sha256'").run();` : ''}
      db.exec('BEGIN IMMEDIATE; CREATE TABLE uncommitted_spill(x); INSERT INTO uncommitted_spill VALUES(randomblob(1048576))');
    `
    }
    console.log('recovery-files-written'); setInterval(()=>{void db.inTransaction;},1000);
  `;
  const child = spawn(process.execPath, ['--input-type=module', '-e', code], {
      cwd: root,
      stdio: ['ignore', 'pipe', 'pipe'],
    }),
    exit = once(child, 'exit');
  let output = '',
    error = '';
  child.stdout.on('data', (bytes) => (output += bytes));
  child.stderr.on('data', (bytes) => (error += bytes));
  t.after(() => {
    if (child.exitCode === null) {
      child.kill('SIGKILL');
    }
  });
  await waitFor(() => output.includes('recovery-files-written') || child.exitCode !== null, error);
  assert.ok(output.includes('recovery-files-written'), error);
  child.kill('SIGKILL');
  await exit;
  const recovery = await readFile(f.filename + (kind.includes('wal') ? '-wal' : '-journal'));
  assert.ok(recovery.length > 4096);
  assert.ok(
    recovery.subarray(0, 8).some((value) => value !== 0),
    'actual native journal/WAL header',
  );
}

test('实际sole新入口仅新owner；暂停bootstrap零自动查询，真实Cookie/Key/Host/Origin/admin门禁', async (t) => {
  const f = await fresh(t);
  const db = new Database(f.filename),
    members = new RelationalMemberStore(db, f.env),
    nonPrefixedKey = 'synthetic-valid-registered-without-prefix';
  db.transaction(() =>
    members.replaceMemberApiKey(f.credentials.accounts[1].user_id, nonPrefixedKey),
  )();
  assert.ok(await authenticateMemberApiKey(members, nonPrefixedKey));
  db.close();
  f.upstream = await upstream(t);
  const server = await (await launch(t, f, { env: { PUBLIC_HOST: 'sole.example' } })).ready(),
    member = f.credentials.accounts[0];
  assert.equal((await call(server, '/api/health')).status, 200);
  assert.equal((await call(server, '/api/health', { method: 'HEAD' })).bytes.length, 0);
  assert.equal(
    (await call(server, '/api/health', { headers: { host: 'attacker.example' } })).status,
    403,
  );
  assert.equal(
    (await call(server, '/api/health', { headers: { origin: 'https://attacker.example' } })).status,
    403,
  );
  assert.equal((await call(server, '/api/admin/ping')).status, 403);
  assert.equal((await call(server, `/api/admin/ping?adminToken=${f.env.ADMIN_TOKEN}`)).status, 401);
  assert.equal(
    (await call(server, '/api/admin/ping', { headers: { 'x-admin-token': f.env.ADMIN_TOKEN } }))
      .status,
    200,
  );
  assert.equal(
    (
      await call(server, '/api/admin/ping', {
        headers: { host: 'sole.example', 'x-admin-token': f.env.ADMIN_TOKEN },
      })
    ).status,
    403,
  );
  const login = await call(server, '/api/member/login', {
    body: { username: member.username, password: member.password },
  });
  assert.equal(login.status, 200, login.bytes.toString());
  const cookie = login.headers.get('set-cookie').split(';')[0];
  for (const route of [
    '/api/member/me',
    '/api/member/key',
    '/api/member/history',
    '/api/member/heatmap',
    '/api/member/shared-status',
  ]) {
    assert.equal((await call(server, route, { headers: { cookie } })).status, 200, route);
  }
  assert.equal(
    (await call(server, '/api/member/owner/member-management', { headers: { cookie } })).status,
    403,
  );
  const management = await call(server, '/api/admin/member-management', {
    headers: { 'x-admin-token': f.env.ADMIN_TOKEN },
  });
  assert.equal(management.status, 200, management.bytes.toString());
  for (const secret of [member.password, member.api_key, f.env.NOVELAI_TOKEN, f.env.ADMIN_TOKEN]) {
    assert.ok(!management.bytes.toString().includes(secret));
  }
  assert.equal((await call(server, '/v1/models', { token: member.api_key })).status, 200);
  assert.equal((await call(server, '/api/me/quota', { token: member.api_key })).status, 200);
  assert.equal((await call(server, '/api/me/quota', { token: f.env.NOVELAI_TOKEN })).status, 401);
  for (const route of ['/api/me/quota', '/api/requests/not-owned']) {
    assert.equal((await call(server, route, { token: nonPrefixedKey })).status, 401, route);
  }
  assert.equal((await call(server, '/memory')).status, 404);
  await delay(100);
  assert.equal(f.upstream.quota, 0);
  assert.equal(f.upstream.generation, 0);
  await server.stop();
});

test(
  'sole真实PATCH启用共享query→Native/Raw/Local领取→OpenAI绝对签名→Direct，重启签名失效而原Key仍可读',
  { timeout: 100000 },
  async (t) => {
    const f = await fresh(t);
    f.upstream = await upstream(t);
    let server = await (await launch(t, f)).ready();
    const token = f.credentials.accounts[0].api_key;
    await enable(f, server);
    assert.equal(f.upstream.quota, 1);
    const native = await call(server, '/ai/generate-image', {
      token,
      body: nativeInput(),
      headers: { 'idempotency-key': 'sole-native' },
    });
    assert.equal(native.status, 200, native.bytes.toString());
    assert.deepEqual(Buffer.from(native.json().images[0].image, 'base64'), image);
    const requestId = native.headers.get('x-nai-request-id'),
      before = query(f, 'SELECT COUNT(*) AS n FROM commits')[0].n,
      repeated = await call(server, '/ai/generate-image', {
        token,
        body: nativeInput(),
        headers: { 'idempotency-key': 'sole-native' },
      });
    assert.equal(repeated.status, 200);
    assert.deepEqual(repeated.bytes, native.bytes);
    assert.equal(query(f, 'SELECT COUNT(*) AS n FROM commits')[0].n, before);
    const raw = await call(server, `/api/requests/${requestId}`, { token });
    assert.equal(raw.status, 200, raw.bytes.toString());
    const artifactId = query(f, 'SELECT id FROM artifacts')[0].id,
      imageRoute = `/api/images/${artifactId}/content`;
    assert.deepEqual((await call(server, imageRoute, { token })).bytes, image);
    assert.equal((await call(server, imageRoute)).status, 401);
    const openai = await call(server, '/v1/chat/completions', {
      token,
      body: openAiInput(),
    });
    assert.equal(openai.status, 200, openai.bytes.toString());
    const signed = openai.json().choices[0].message.content.match(/!\[image\]\(([^)]+)\)/)[1];
    assert.ok(signed.startsWith(server.origin + '/api/images/'));
    assert.deepEqual(Buffer.from(await (await fetch(signed)).arrayBuffer()), image);
    const direct = await call(
      server,
      `/generate?token=${token}&tag=synthetic&width=128&height=128&steps=23&seed=12`,
    );
    assert.equal(direct.status, 200, direct.bytes.toString());
    assert.deepEqual(direct.bytes, image);
    assert.equal(f.upstream.generation, 3);
    await server.stop();
    server = await (await launch(t, f)).ready();
    assert.equal(
      (await call(server, new URL(signed).pathname + new URL(signed).search)).status,
      401,
    );
    assert.deepEqual((await call(server, imageRoute, { token })).bytes, image);
    await server.stop();
  },
);

test('missing/旧正确格式/hash错/building在原DB及sidecar完全不变时拒绝，无listener和images', async (t) => {
  for (const kind of ['missing', 'old', 'hash', 'building']) {
    await t.test(kind, async (t) => {
      const f = await fresh(t);
      if (kind === 'missing') {
        await rm(f.filename);
      } else if (kind === 'old') {
        await rm(f.filename);
        const fixture = JSON.parse(
            await readFile(
              new URL('../fixtures/relational-schema-v5.json', import.meta.url),
              'utf8',
            ),
          ),
          db = new Database(f.filename);
        db.exec(fixture.sql);
        const insert = db.prepare('INSERT INTO app_meta(key,value) VALUES(?,?)');
        insert.run('database_format', 'novelai-relational-v1');
        insert.run('schema_sha256', fixture.sha256);
        insert.run('event_authority', JSON.stringify({ version: 5, minimumWriterVersion: 5 }));
        assert.equal(createHash('sha256').update(fixture.sql).digest('hex'), fixture.sha256);
        db.close();
      } else {
        const db = new Database(f.filename);
        if (kind === 'hash') {
          db.prepare("UPDATE app_meta SET value='wrong' WHERE key='schema_sha256'").run();
        } else {
          db.prepare("UPDATE projection_state SET status='building'").run();
        }
        db.close();
      }
      const before = await files(f),
        server = await launch(t, f);
      await server.finished;
      assert.notEqual(server.exit.code, 0);
      assert.equal(server.origin, null);
      assert.deepEqual(await files(f), before);
      assert.ok(
        !(await readdir(f.dataDir)).some(
          (name) => name === 'images' || name.startsWith('.gateway-preflight-'),
        ),
      );
    });
  }
  await t.test('missing directory', async (t) => {
    const f = await fresh(t),
      missing = path.join(f.dir, 'not-created');
    f.env.DATA_DIR = missing;
    const server = await launch(t, f);
    await server.finished;
    assert.notEqual(server.exit.code, 0);
    assert.equal(server.origin, null);
    await assert.rejects(readdir(missing), { code: 'ENOENT' });
  });
});

test('实际入口核WAL-only最新提交并恢复真正hotjournal；错hash仅在日志也拒绝且原字节不变', async (t) => {
  for (const kind of ['wal', 'hot', 'bad-wal', 'bad-hot']) {
    await t.test(kind, async (t) => {
      const f = await fresh(t);
      await leaveRecoveryFiles(t, f, kind);
      const before = await files(f),
        server = await launch(t, f);
      if (kind.startsWith('bad')) {
        await server.finished;
        assert.notEqual(server.exit.code, 0);
        assert.equal(server.origin, null);
        assert.deepEqual(await files(f), before);
      } else {
        await server.ready();
        const member = f.credentials.accounts[0],
          response = await call(server, '/api/member/login', {
            body: { username: member.username, password: member.password },
          });
        assert.equal(response.status, kind === 'wal' ? 401 : 200, response.bytes.toString());
        await server.stop();
        assert.equal(
          query(f, "SELECT COUNT(*) n FROM sqlite_schema WHERE name='uncommitted_spill'")[0].n,
          0,
        );
        assert.equal(
          query(f, 'SELECT status FROM members WHERE id=?', member.user_id)[0].status,
          kind === 'wal' ? 'disabled' : 'active',
        );
      }
      assert.ok(!(await readdir(f.dataDir)).some((name) => name.startsWith('.gateway-preflight-')));
    });
  }
});

test('真正EADDRINUSE发生前不activate已合法受理的queued任务', async (t) => {
  const f = await fresh(t);
  await prepareEnabled(t, f);
  const prepared = await acceptQueued(f);
  prepared.db.close();
  const taken = createServer();
  taken.listen(0, '127.0.0.1');
  await once(taken, 'listening');
  t.after(() => new Promise((resolve) => taken.close(resolve)));
  const before = query(
      f,
      "SELECT COUNT(*) n FROM domain_events WHERE event_type='DispatchAuthorized'",
    )[0].n,
    server = await launch(t, f, {
      port: taken.address().port,
      // 仍调用原真实listen，放大错误activate→listen实现会先派发的窗口。
      hook: `import {Server} from 'node:http'; const listen=Server.prototype.listen;
        Server.prototype.listen=function(...args){setTimeout(()=>listen.apply(this,args),100);return this;};`,
    });
  await server.finished;
  assert.notEqual(server.exit.code, 0);
  assert.match(server.stderr, /EADDRINUSE/);
  assert.equal(server.origin, null);
  assert.equal(f.upstream.generation, 0);
  assert.equal(
    query(f, "SELECT COUNT(*) n FROM domain_events WHERE event_type='DispatchAuthorized'")[0].n,
    before,
  );
  const changed = await launch(t, f, {
    hook: `import Database from 'better-sqlite3'; import {Server} from 'node:http';
      const listen = Server.prototype.listen;
      Server.prototype.listen=function(...args){const callback=args.pop();return listen.call(this,...args,()=>{
        const db=new Database(${JSON.stringify(f.filename)});db.prepare("UPDATE projection_state SET status='building'").run();db.close();callback();
      });};`,
  });
  await changed.finished;
  assert.notEqual(changed.exit.code, 0);
  assert.equal(changed.origin, null);
  assert.equal(
    f.upstream.generation,
    0,
    'listen success must still recheck current ready before activation',
  );
  assert.equal(
    query(f, "SELECT COUNT(*) n FROM domain_events WHERE event_type='DispatchAuthorized'")[0].n,
    before,
  );
});

test('真实prepared损坏使prepare拒绝，未监听且未发新上游请求', async (t) => {
  const f = await fresh(t);
  await prepareEnabled(t, f);
  const { db, generations, result } = await acceptQueued(f),
    jobId = result.id,
    at = Date.now(),
    descriptor = describeStoredResult('sole-prepared', 'generate', {
      buffer: image,
      mimeType: 'image/webp',
    });
  db.transaction(() =>
    generations.dispatchGeneration(
      meta('sole-dispatch', at),
      {
        jobId,
        accountId: f.accountId,
        attemptId: 'sole-attempt',
      },
      { dispatchEnabled: () => true, physicalAccountLoad: () => 0 },
    ),
  ).immediate();
  db.transaction(() =>
    generations.prepareGenerationResult(meta('sole-prepared', at + 1), jobId, descriptor, {
      dispatchState: 'sent',
      upstreamStartedAt: at,
      upstreamHeadersAt: at,
      upstreamCompletedAt: at + 1,
      upstreamHttpStatus: 200,
    }),
  ).immediate();
  db.close();
  await mkdir(path.join(f.dataDir, 'images'), { recursive: true });
  await writeFile(path.join(f.dataDir, descriptor.file), Buffer.from('not the promised image'));
  const server = await launch(t, f);
  await server.finished;
  assert.notEqual(server.exit.code, 0);
  assert.equal(server.origin, null);
  assert.equal(f.upstream.generation, 0);
  assert.match(server.stderr, /GENERATION_RUNTIME_FAILED/);
  assert.equal(
    query(f, 'SELECT status FROM generation_jobs WHERE id=?', jobId)[0].status,
    'running',
  );
});

test('关闭等待真实quota网络、已派发generation与HTTP响应，再关DB；重启保留结果', async (t) => {
  for (const kind of ['quota', 'generation']) {
    await t.test(kind, async (t) => {
      const f = await fresh(t);
      f.upstream = await upstream(t);
      const server = await (await launch(t, f)).ready();
      if (kind === 'generation') {
        await enable(f, server);
      }
      let release;
      const gate = new Promise((resolve) => (release = resolve));
      f.upstream.hold = () => gate;
      t.after(() => release());
      const pending =
        kind === 'quota'
          ? enable(f, server)
          : call(server, '/ai/generate-image', {
              token: f.credentials.accounts[0].api_key,
              body: nativeInput(),
            });
      await waitFor(() => f.upstream.active === 1);
      let waiting;
      if (kind === 'generation') {
        const body = nativeInput();
        body.parameters.seed++;
        waiting = call(server, '/ai/generate-image', {
          token: f.credentials.accounts[0].api_key,
          body,
        });
        await waitFor(
          () => query(f, "SELECT COUNT(*) n FROM generation_jobs WHERE status='queued'")[0].n === 1,
        );
      }
      server.child.send('shutdown');
      await delay(100);
      assert.equal(server.exit, null, 'physical IO must retain the owning SQLite handle');
      release();
      const response = await pending;
      if (kind === 'generation') {
        assert.equal(response.status, 200, response.bytes.toString());
        assert.deepEqual(Buffer.from(response.json().images[0].image, 'base64'), image);
        assert.equal(
          (await waiting).status,
          503,
          'only HTTP waiting ends; queued business survives',
        );
      }
      await server.finished;
      assert.equal(server.exit.code, 0, server.stderr);
      if (kind === 'generation') {
        assert.equal(
          query(f, "SELECT COUNT(*) AS n FROM generation_jobs WHERE status='done'")[0].n,
          1,
        );
        assert.equal(
          query(f, "SELECT COUNT(*) AS n FROM generation_jobs WHERE status='queued'")[0].n,
          1,
        );
      }
      const restarted = await (await launch(t, f)).ready();
      assert.equal((await call(restarted, '/api/health')).status, 200);
      if (kind === 'generation') {
        await waitFor(
          () => query(f, "SELECT COUNT(*) n FROM generation_jobs WHERE status='done'")[0].n === 2,
        );
        assert.equal(f.upstream.generation, 2);
      }
      await restarted.stop();
    });
  }
});

test('sole沿原Direct和Native/OpenAI领取期限，不把HTTP超时当物理上游结束', async (t) => {
  for (const kind of ['direct', 'native', 'openai']) {
    await t.test(kind, async (t) => {
      const f = await fresh(t);
      f.upstream = await upstream(t);
      const server = await (
        await launch(t, f, {
          env: {
            DIRECT_GENERATE_TIMEOUT_MS: '100',
            OPENAI_CHAT_TIMEOUT_MS: '100',
            NOVELAI_GENERATE_TIMEOUT_MS: '5000',
          },
        })
      ).ready();
      await enable(f, server);
      let release;
      const gate = new Promise((resolve) => {
        release = resolve;
      });
      f.upstream.hold = () => gate;
      t.after(() => release());
      const token = f.credentials.accounts[0].api_key,
        pending =
          kind === 'direct'
            ? call(
                server,
                `/generate?token=${token}&tag=timeout-proof&width=128&height=128&steps=23&seed=12`,
              )
            : call(server, kind === 'native' ? '/ai/generate-image' : '/v1/chat/completions', {
                token,
                body: kind === 'native' ? nativeInput() : openAiInput(),
              });
      await waitFor(() => f.upstream.active === 1);
      // 错用物理5000ms的变体会在500ms收到真产物，精确误返200，不靠测试timeout假红。
      const timer = setTimeout(release, 500);
      t.after(() => clearTimeout(timer));
      const response = await pending;
      clearTimeout(timer);
      if (kind === 'direct') {
        assert.equal(response.status, 200);
        assert.equal(response.headers.get('x-timeout'), '1');
        assert.notDeepEqual(response.bytes, image);
      } else {
        assert.equal(response.status, 504, response.bytes.toString());
      }
      assert.equal(f.upstream.active, 1, 'HTTP deadline must not release actual physical work');
      assert.equal(
        query(f, "SELECT COUNT(*) n FROM generation_jobs WHERE status='running'")[0].n,
        1,
      );
      release();
      await waitFor(
        () => query(f, "SELECT COUNT(*) n FROM generation_jobs WHERE status='done'")[0].n === 1,
      );
      assert.equal(f.upstream.generation, 1);
      await server.stop();
    });
  }
});
