import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { mkdtemp, readFile, writeFile, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createTestContext, loadFixture, buildFixture } from '@nuxt/test-utils/e2e';

const webRoot = fileURLToPath(new URL('../../', import.meta.url));
const executorEntry = fileURLToPath(new URL('../../../executor/.output/server/start.mjs', import.meta.url));
const handler = fileURLToPath(new URL('../fixtures/executor-gateway.ts', import.meta.url)).replaceAll('\\', '/');
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
const publicHost = 'gateway.example', publicOrigin = 'https://' + publicHost;
const envBase = () => Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE']
  .filter(key => process.env[key]).map(key => [key, process.env[key]]));
const jobBody = prompt => ({ prompt, model: 'nai-diffusion-4-5-full', width: 1024, height: 1024, steps: 23, seed: 42 });
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
async function listen(server) {
  server.listen(0, '127.0.0.1'); await once(server, 'listening'); return server.address().port;
}
async function unusedPort() {
  const reservation = http.createServer(), port = await listen(reservation);
  await new Promise(resolve => reservation.close(resolve)); return port;
}
async function until(predicate, timeout, label) {
  const deadline = Date.now() + timeout;
  while (!await predicate()) {
    if (Date.now() >= deadline) throw new Error(label);
    await new Promise(resolve => setTimeout(resolve, 20));
  }
}
function call(base, route, { method = 'GET', body, headers = {} } = {}) {
  const target = new URL(route, base);
  assert.equal(target.origin, base, 'fixture calls must stay on the local process');
  return new Promise((resolve, reject) => {
    const request = http.request(target, { method: body === undefined ? method : 'POST', signal: AbortSignal.timeout(15000),
      headers: { host: publicHost, origin: publicOrigin, ...(body === undefined ? {} : { 'content-type': 'application/json' }), ...headers } }, response => {
      const chunks = [];
      response.on('data', chunk => chunks.push(chunk)); response.on('error', reject);
      response.on('end', () => resolve({ status: response.statusCode, headers: response.headers, body: Buffer.concat(chunks) }));
    });
    request.on('error', reject); request.end(body === undefined ? undefined : JSON.stringify(body));
  });
}
function json(response, status = 200) {
  assert.equal(response.status, status, response.body.toString().slice(0, 500));
  assert.match(response.headers['content-type'], /application\/json/);
  return JSON.parse(response.body);
}

async function fixture(t, nuxtEntry, { holdFirst = false, holdFileSync = false } = {}) {
  await stat(executorEntry);
  const runtimeRequire = createRequire(executorEntry);
  const packagedRoot = path.join(path.dirname(executorEntry), 'node_modules', 'novelai-shared-gateway');
  const packaged = relative => {
    const filename = runtimeRequire.resolve('novelai-shared-gateway/' + relative), inside = path.relative(packagedRoot, filename);
    assert.ok(!inside.startsWith('..') && !path.isAbsolute(inside), 'executor fixture must use the packaged runtime');
    return pathToFileURL(filename).href;
  };
  const { bootstrap } = await import(packaged('scripts/bootstrap.mjs'));
  const { JsonStore } = await import(packaged('server/store.js'));
  const { syncFairQuota, quoteFairRequest } = await import(packaged('server/fair-quota.js'));
  const { nativeImagesMsgpack } = await import(packaged('server/native-stream.js'));
  const Database = runtimeRequire('better-sqlite3');
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-nuxt-executor-'));
  const adminToken = 'synthetic-nuxt-executor-admin-token-only';
  const internalToken = 'synthetic-nuxt-executor-internal-token-only';
  const upstreamToken = 'pst-synthetic-nuxt-executor-upstream-only';
  const initialized = await bootstrap({ env: { ADMIN_TOKEN: adminToken, NOVELAI_TOKEN: upstreamToken, DATA_DIR: dataDir }, log() {} });
  const members = JSON.parse(await readFile(initialized.credentialPath, 'utf8')).accounts;
  const seeded = new JsonStore(dataDir); await seeded.init();
  try {
    await seeded.update(db => {
      Object.assign(db.accounts[0], { quotaTier: 3, subscriptionActive: true, subscriptionExpiresAt: Date.now() + 86400000,
        quotaFixed: 10000, quotaPurchased: 0, v5UsagePercent: 100, quotaCheckedAt: new Date().toISOString() });
      syncFairQuota(db);
    }, { immediate: true });
  } finally { await seeded.close(); }

  // After seeding, only the independently launched executor writes the business database.
  const audit = new Database(path.join(dataDir, 'library.sqlite'), { readonly: true, fileMustExist: true });
  const rows = table => audit.prepare(`SELECT data FROM ${table}`).all().map(row => JSON.parse(row.data));
  const jobs = () => rows('jobs');
  const releaseFirst = deferred();
  let fileSynced = false;
  let generations = 0, fixedRemaining = 10000, unexpectedUpstream = 0, leakedInternalHeader = false, wrongUpstreamAuth = false;
  const upstream = http.createServer(async (req, res) => {
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    leakedInternalHeader ||= Object.hasOwn(req.headers, 'x-executor-token');
    wrongUpstreamAuth ||= req.headers.authorization !== 'Bearer ' + upstreamToken;
    if (req.url === '/user/data') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ subscription: { tier: 3, active: true, expiresAt: Math.floor(Date.now() / 1000) + 86400,
        trainingStepsLeft: { fixedTrainingStepsLeft: fixedRemaining, purchasedTrainingSteps: 0 },
        usage: { percent: 100, isNegative: false, timeUntilNextPercent: 0 } } }));
    } else if (['/ai/generate-image', '/ai/generate-image-stream'].includes(req.url)) {
      generations++;
      const generated = JSON.parse(Buffer.concat(chunks));
      const quote = quoteFairRequest({ ...generated.parameters, model: generated.model });
      if (quote.resource === 'anlas') fixedRemaining -= quote.amount;
      if (generations === 1 && holdFirst) await releaseFirst.promise;
      const stream = req.url.endsWith('-stream');
      res.writeHead(200, { 'content-type': stream ? 'application/octet-stream' : 'image/png' });
      res.end(stream ? nativeImagesMsgpack([png]) : png);
    } else { unexpectedUpstream++; res.writeHead(500); res.end('unexpected synthetic upstream route'); }
  });
  const upstreamPort = await listen(upstream), executorPort = await unusedPort(), nuxtPort = await unusedPort();
  const executorBase = `http://127.0.0.1:${executorPort}`, nuxtBase = `http://127.0.0.1:${nuxtPort}`;
  const observations = [], droppedReceipts = [];
  let dropNextReceipt = false;
  // This relay injects a lost internal acknowledgement; it is not the Nuxt gateway under test.
  const relay = http.createServer(async (req, res) => {
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    const body = Buffer.concat(chunks);
    observations.push({ path: req.url, method: req.method, headers: { ...req.headers }, body });
    const forwarded = http.request(new URL(req.url, executorBase), { method: req.method, headers: req.headers }, response => {
      const parts = [];
      response.on('data', chunk => parts.push(chunk));
      response.on('error', () => res.destroy());
      response.on('end', () => {
        const received = Buffer.concat(parts);
        if (dropNextReceipt && req.method === 'POST' && response.statusCode === 202) {
          dropNextReceipt = false; droppedReceipts.push(JSON.parse(received)); res.destroy(); return;
        }
        res.writeHead(response.statusCode, response.headers); res.end(received);
      });
    });
    forwarded.on('error', () => { if (!res.destroyed) { res.writeHead(502); res.end('synthetic relay transport failure'); } });
    forwarded.end(body);
  });
  const relayPort = await listen(relay);
  const preload = path.join(dataDir, 'deny-external-fetch.mjs');
  await writeFile(preload, `const original = globalThis.fetch;
globalThis.fetch = (input, options) => {
  const url = new URL(typeof input === 'string' || input instanceof URL ? input : input.url);
  if (url.protocol !== 'http:' || !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)) throw new Error('External fetch blocked by Nuxt executor fixture');
  return original(input, { ...options, redirect: 'error' });
};\n`);
  const secrets = [adminToken, internalToken, upstreamToken, ...members.flatMap(member => [member.api_key, member.password])];
  const children = [];
  function processHandle(label, entry, env, imports = []) {
    let child, exited, output = '';
    const redacted = () => secrets.reduce((text, secret) => text.replaceAll(secret, '[redacted]'), output);
    const stop = async () => {
      if (child?.exitCode === null && child?.signalCode === null) child.kill('SIGKILL');
      if (exited) await exited;
    };
    const start = async (ready, envChanges = {}) => {
      output = '';
      child = spawn(process.execPath, ['--import', pathToFileURL(preload).href, ...imports.flatMap(file => ['--import', file]), entry], {
        cwd: path.dirname(entry), env: { ...env, ...envChanges }, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
      exited = once(child, 'exit');
      child.on('message', message => { if (message.point === 'file_sync') fileSynced = true; });
      const record = chunk => { output = (output + chunk.toString()).slice(-12000); };
      child.stdout.on('data', record); child.stderr.on('data', record);
      await until(async () => {
        if (child.exitCode !== null || child.signalCode !== null) throw new Error(label + ' exited before ready: ' + redacted());
        return ready(output);
      }, 20000, label + ' did not become ready');
    };
    const handle = { start, stop, child: () => child, output: redacted }; children.push(handle); return handle;
  }
  const executor = processHandle('built executor', executorEntry, { ...envBase(), ADMIN_TOKEN: adminToken,
    EXECUTOR_TOKEN: internalToken, DATA_DIR: dataDir, HOST: '127.0.0.1', PORT: String(executorPort),
    PUBLIC_HOST: publicHost, PUBLIC_BASE_URL: publicOrigin, TRUSTED_PROXY_IPS: '127.0.0.1',
    RESULT_RECOVERY_TEST_STAGE: holdFileSync ? 'file_sync' : '',
    NOVELAI_API_URL: `http://127.0.0.1:${upstreamPort}`, NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${upstreamPort}` },
  [new URL('../fixtures/executor-file-sync.mjs', import.meta.url).href]);
  const nuxt = processHandle('built Nuxt', nuxtEntry, { ...envBase(), NODE_ENV: 'production', HOST: '127.0.0.1', PORT: String(nuxtPort),
    NUXT_EXECUTOR_BASE_URL: `http://127.0.0.1:${relayPort}`, NUXT_EXECUTOR_TOKEN: internalToken });
  const startNuxt = () => nuxt.start(async () => {
    try { return (await call(nuxtBase, '/')).status === 200; } catch { return false; }
  });
  const startExecutor = () => executor.start(output => output.includes('Nai2API listening'), { RESULT_RECOVERY_TEST_STAGE: '' });
  t.after(async () => {
    releaseFirst.resolve();
    for (const child of children.slice().reverse()) await child.stop();
    for (const server of [relay, upstream]) { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
    audit.close();
    if (!t.passed) for (const child of children) t.diagnostic(child.output());
  });
  await executor.start(output => output.includes('Nai2API listening')); await startNuxt();
  const render = await call(nuxtBase, '/');
  assert.match(render.body.toString(), /__NUXT_DATA__/);
  for (const secret of secrets) assert.ok(!render.body.includes(Buffer.from(secret)), 'SSR must not serialize internal credentials');
  const request = (route, { member = 0, headers = {}, ...options } = {}) => call(nuxtBase, route, {
    ...options, headers: { authorization: 'Bearer ' + members[member].api_key, ...headers } });
  const checkStorage = () => {
    assert.equal(audit.pragma('integrity_check', { simple: true }), 'ok');
    assert.equal(unexpectedUpstream, 0); assert.equal(leakedInternalHeader, false); assert.equal(wrongUpstreamAuth, false);
    for (const child of children) assert.doesNotMatch(child.output(), /External fetch blocked/);
  };
  return { request, nuxt, executor, startNuxt, startExecutor, dataDir, audit, jobs, members, observations, droppedReceipts, internalToken,
    fileSynced: () => fileSynced,
    releaseFirst: releaseFirst.resolve, generations: () => generations,
    dropReceipt() { dropNextReceipt = true; }, checkStorage };
}

test('真实 Nuxt 与执行器的生命周期隔离和内部受理重试', { timeout: 240000 }, async t => {
  await stat(executorEntry); // A missing build is a failure; never substitute the legacy Node server.
  const ctx = createTestContext({ rootDir: webRoot, dev: false, build: true, browser: false, server: false,
    nuxtConfig: { compatibilityDate: '2026-10-17',
      nitro: { handlers: ['/api/jobs', '/api/jobs/**', '/api/requests/**'].map(route => ({ route, handler })) } } });
  t.after(async () => {
    await ctx.nuxt?.close();
    const testRoot = path.resolve(webRoot, '.nuxt', 'test');
    const buildDir = path.resolve(ctx.nuxt?.options.buildDir || ctx.options.nuxtConfig.buildDir);
    const inside = path.relative(testRoot, buildDir);
    assert.ok(inside && !inside.startsWith('..') && !path.isAbsolute(inside), 'only an isolated Nuxt test build may be removed');
    for (const teardown of ctx.teardown || []) await teardown();
  });
  await loadFixture(); await buildFixture();
  const nuxtEntry = path.join(ctx.nuxt.options.nitro.output.dir, 'server', 'index.mjs'); await stat(nuxtEntry);

  await t.test('只强杀 Nuxt 不取消在途或排队任务，恢复后仅原成员可查询领取', { timeout: 60000 }, async sub => {
    const f = await fixture(sub, nuxtEntry, { holdFirst: true });
    const first = json(await f.request('/api/jobs', { body: jobBody('synthetic running after Nuxt kill') }), 202);
    await until(() => f.generations() === 1, 10000, 'first accepted task did not reach the synthetic upstream');
    const second = json(await f.request('/api/jobs', { body: jobBody('synthetic queued after Nuxt kill') }), 202);
    assert.notEqual(first.id, second.id); assert.ok(first.requestId); assert.ok(second.requestId);
    assert.equal(f.jobs().find(job => job.id === first.id).status, 'running');
    assert.equal(f.jobs().find(job => job.id === second.id).status, 'queued');
    const executorPid = f.executor.child().pid;
    await f.nuxt.stop();
    assert.equal(f.executor.child().pid, executorPid); assert.equal(f.executor.child().exitCode, null);
    f.releaseFirst();
    await until(() => f.jobs().length === 2 && f.jobs().every(job => job.status === 'done'), 35000, 'executor failed to finish while Nuxt was stopped');
    assert.equal(f.generations(), 2); assert.equal(f.executor.child().pid, executorPid);
    for (const job of f.jobs()) {
      const image = JSON.parse(f.audit.prepare('SELECT data FROM images WHERE id = ?').get(job.imageId).data);
      assert.deepEqual(await readFile(path.join(f.dataDir, image.file)), png);
    }
    await f.startNuxt();
    for (const accepted of [first, second]) {
      const job = json(await f.request('/api/jobs/' + accepted.id)); assert.equal(job.status, 'done');
      assert.equal(json(await f.request('/api/requests/' + accepted.requestId)).id, accepted.requestId);
      const image = await f.request('/api/jobs/' + accepted.id + '/content'); assert.equal(image.status, 200); assert.deepEqual(image.body, png);
      assert.equal((await f.request('/api/jobs/' + accepted.id, { member: 1 })).status, 403);
      assert.equal((await f.request('/api/jobs/' + accepted.id + '/content', { member: 1 })).status, 403);
      assert.equal((await f.request('/api/requests/' + accepted.requestId, { member: 1 })).status, 404);
    }
    assert.equal(f.generations(), 2); f.checkStorage();
  });

  await t.test('丢失202后内部复用同键和正文，身份/Host/Origin/代理链保留，业务错误不重试', { timeout: 45000 }, async sub => {
    const f = await fixture(sub, nuxtEntry);
    f.dropReceipt();
    const cookie = 'nai_member_session=synthetic-observable-cookie';
    const body = { ...jobBody('synthetic paid operation with lost acknowledgement'), width: 1536 };
    const accepted = json(await f.request('/api/jobs', { body, headers: { cookie, 'x-executor-token': 'client-forged-internal-token',
      'x-forwarded-for': '198.51.100.10, 203.0.113.20' } }), 202);
    const submissions = f.observations.filter(item => item.method === 'POST');
    assert.equal(submissions.length, 2); assert.equal(f.droppedReceipts.length, 1);
    assert.equal(f.droppedReceipts[0].id, accepted.id); assert.equal(f.droppedReceipts[0].requestId, accepted.requestId);
    const key = submissions[0].headers['idempotency-key']; assert.match(key, /^[\x21-\x7e]{1,128}$/);
    assert.equal(submissions[1].headers['idempotency-key'], key); assert.deepEqual(submissions[0].body, submissions[1].body);
    for (const submission of submissions) {
      assert.equal(submission.headers.host, publicHost); assert.equal(submission.headers.origin, publicOrigin);
      assert.equal(submission.headers.authorization, 'Bearer ' + f.members[0].api_key); assert.equal(submission.headers.cookie, cookie);
      assert.equal(submission.headers['x-executor-token'], f.internalToken);
      assert.match(submission.headers['x-forwarded-for'], /^198\.51\.100\.10, 203\.0\.113\.20, (?:::ffff:)?127\.0\.0\.1$/);
    }
    await until(() => f.jobs().length === 1 && f.jobs()[0].status === 'done', 15000, 'retried operation did not finish');
    assert.equal(f.generations(), 1);
    for (const table of ['jobs', 'images', 'generation_requests', 'generation_idempotency', 'fair_quota_settlements']) {
      assert.equal(f.audit.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 1, table + ' must contain only the original operation');
    }
    const charge = JSON.parse(f.audit.prepare('SELECT charge FROM fair_quota_settlements WHERE job_id = ?').get(accepted.id).charge);
    assert.equal(charge.status, 'settled'); assert.equal(charge.resource, 'anlas'); assert.ok(charge.fixed > 0); assert.equal(charge.purchased, 0);
    const ledger = f.audit.prepare('SELECT data FROM ledger').all().map(row => JSON.parse(row.data));
    assert.equal(ledger.filter(entry => entry.jobId === accepted.id && entry.type === 'generation_reserved').length, 1);
    const before = f.observations.length;
    const conflict = await f.request('/api/jobs', { body: { ...body, prompt: 'different intent' }, headers: { 'idempotency-key': key } });
    assert.equal(conflict.status, 409); assert.equal(f.observations.length, before + 1);
    assert.equal((await f.request('/api/jobs/' + accepted.id, { headers: { host: 'unconfigured.example' } })).status, 403);
    assert.equal((await f.request('/api/jobs/' + accepted.id, { headers: { host: 'localhost:1234', origin: 'https://untrusted.example' } })).status, 403);
    assert.equal(f.jobs().length, 1); assert.equal(f.generations(), 1); f.checkStorage();
  });

  await t.test('执行器在完整产物sync后强杀，两次启动恢复同一结果且不重复结算，Nuxt保持运行', { timeout: 45000 }, async sub => {
    const f = await fixture(sub, nuxtEntry, { holdFileSync: true });
    const accepted = json(await f.request('/api/jobs', { body: { ...jobBody('synthetic durable result before settlement'), width: 1536 } }), 202);
    await until(f.fileSynced, 10000, 'complete result did not reach the file_sync checkpoint');
    const original = f.jobs()[0], imageId = original.resultPending?.imageId;
    assert.ok(imageId); assert.equal(original.status, 'running'); assert.equal(f.generations(), 1);
    assert.deepEqual(await readFile(path.join(f.dataDir, original.resultPending.file + '.part')), png);
    assert.equal(f.audit.prepare('SELECT COUNT(*) AS n FROM fair_quota_settlements').get().n, 0);
    const nuxtPid = f.nuxt.child().pid;
    await f.executor.stop();
    let settledState;
    for (let restart = 0; restart < 2; restart++) {
      await f.startExecutor();
      assert.equal(f.nuxt.child().pid, nuxtPid); assert.equal(f.nuxt.child().exitCode, null);
      const job = json(await f.request('/api/jobs/' + accepted.id));
      assert.equal(job.status, 'done'); assert.equal(job.imageId, imageId);
      assert.deepEqual((await f.request('/api/jobs/' + accepted.id + '/content')).body, png);
      assert.equal(json(await f.request('/api/requests/' + accepted.requestId)).id, accepted.requestId);
      const saved = f.jobs()[0]; assert.equal(saved.resultPending, undefined); assert.equal(saved.imageId, imageId);
      const charge = JSON.parse(f.audit.prepare('SELECT charge FROM fair_quota_settlements WHERE job_id = ?').get(accepted.id).charge);
      assert.equal(charge.status, 'settled'); assert.ok(charge.fixed > 0);
      const counts = ['jobs', 'images', 'generation_requests', 'generation_idempotency', 'fair_quota_settlements', 'ledger']
        .map(table => f.audit.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n);
      const current = { counts, charge, imageId: saved.imageId };
      if (settledState) assert.deepEqual(current, settledState); else settledState = current;
      assert.equal(f.generations(), 1); f.checkStorage();
      await f.executor.stop();
    }
  });
});
