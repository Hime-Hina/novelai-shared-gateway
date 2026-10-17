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

// CI mounts only this test directory into the final image. Never fall back to source modules.
const entry = process.env.EXECUTOR_TEST_ENTRY ? path.resolve(process.env.EXECUTOR_TEST_ENTRY)
  : fileURLToPath(new URL('../../.output/server/start.mjs', import.meta.url));
await stat(entry);
const runtimeRequire = createRequire(entry);
const packagedRoot = path.join(path.dirname(entry), 'node_modules', 'novelai-shared-gateway');
function packaged(relative) {
  const filename = runtimeRequire.resolve('novelai-shared-gateway/' + relative);
  const resolved = path.relative(packagedRoot, filename);
  assert.ok(!resolved.startsWith('..') && !path.isAbsolute(resolved), 'business module must resolve inside the built output');
  return filename;
}
const { bootstrap } = await import(pathToFileURL(packaged('scripts/bootstrap.mjs')).href);
const { JsonStore } = await import(pathToFileURL(packaged('server/store.js')).href);
const { syncFairQuota } = await import(pathToFileURL(packaged('server/fair-quota.js')).href);
const { createMemberSession } = await import(pathToFileURL(packaged('server/member-auth.js')).href);
const Database = runtimeRequire('better-sqlite3');
const migrationJournal = JSON.parse(await readFile(packaged('server/migrations/meta/_journal.json'), 'utf8'));
for (const name of ['server/gateway-runtime.js', 'server/sqlite-read-worker.js', 'public/generation-pricing.js',
  ...migrationJournal.entries.map(item => `server/migrations/${item.tag}.sql`)]) await stat(packaged(name));

const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
const nativeBody = { input: 'synthetic built executor image', model: 'nai-diffusion-4-5-full', action: 'generate',
  parameters: { width: 1024, height: 1024, steps: 23, n_samples: 1, seed: 7 } };
const envBase = () => Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE']
  .filter(key => process.env[key]).map(key => [key, process.env[key]]));
async function listen(server) {
  server.listen(0, '127.0.0.1'); await once(server, 'listening'); return server.address().port;
}
function bounded(promise, timeout, label) {
  let timer;
  return Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(label)), timeout); })])
    .finally(() => clearTimeout(timer));
}

async function fixture(t, { owner = false } = {}) {
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-executor-artifact-'));
  const adminToken = 'synthetic-executor-admin-token-only';
  const internalToken = 'synthetic-executor-internal-token-only';
  const upstreamToken = 'pst-synthetic-executor-upstream-only';
  const initialized = await bootstrap({ env: { ADMIN_TOKEN: adminToken, NOVELAI_TOKEN: upstreamToken, DATA_DIR: dataDir }, log() {} });
  const members = JSON.parse(await readFile(initialized.credentialPath, 'utf8')).accounts;
  const env = { ...envBase(), ADMIN_TOKEN: adminToken, EXECUTOR_TOKEN: internalToken, DATA_DIR: dataDir,
    HOST: '127.0.0.1', SHUTDOWN_GRACE_MS: '20000' };
  if (owner) Object.assign(env, { OWNER_USER_ID: members[0].user_id, DISCORD_GUILD_ID: '1134557553011998840',
    DISCORD_CLIENT_ID: '123456789012345678', DISCORD_CLIENT_SECRET: 'synthetic-executor-discord-secret', PUBLIC_BASE_URL: 'https://executor.example' });
  const seeded = new JsonStore(dataDir), sessions = [];
  await seeded.init();
  try {
    await seeded.update(db => {
      Object.assign(db.accounts[0], { quotaTier: 3, subscriptionActive: true, subscriptionExpiresAt: Date.now() + 86400000,
        quotaFixed: 10000, quotaPurchased: 0, v5UsagePercent: 100, quotaCheckedAt: new Date().toISOString() });
      syncFairQuota(db);
      if (owner) db.users.forEach((user, index) => {
        user.discord = { id: String(222222222222222220n + BigInt(index)), guildId: env.DISCORD_GUILD_ID,
          member: true, checkedAt: Date.now(), accessToken: 'synthetic-discord-access', refreshToken: 'synthetic-discord-refresh',
          expiresAt: Date.now() + 3600000 };
      });
      db.jobs = [0, 1, 1].map((member, index) => ({ id: `synthetic-retained-${index}`, status: 'done', source: 'novelai-native',
        createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(), userToken: db.users[member].token,
        generationAttempt: { id: `synthetic-history-attempt-${index}`, memberId: db.users[member].id },
        request: { operation: 'generate', model: nativeBody.model, prompt: 'synthetic-private-history' } }));
    }, { immediate: true });
    if (owner) for (const user of seeded.db.users.slice(0, 2)) sessions.push(await createMemberSession(seeded, user, env));
  } finally { await seeded.close(); }

  // From this point only the child writes SQLite; parent assertions use a read-only connection.
  const audit = new Database(path.join(dataDir, 'library.sqlite'), { readonly: true, fileMustExist: true });
  let generations = 0, unexpectedUpstream = 0, leakedInternalHeader = false, wrongUpstreamAuth = false;
  const upstream = http.createServer(async (req, res) => {
    for await (const _chunk of req) { /* Consume synthetic bodies without logging credentials or prompts. */ }
    leakedInternalHeader ||= Object.hasOwn(req.headers, 'x-executor-token');
    wrongUpstreamAuth ||= req.headers.authorization !== 'Bearer ' + upstreamToken;
    if (req.url === '/user/data') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ subscription: { tier: 3, active: true, expiresAt: Math.floor(Date.now() / 1000) + 86400,
        trainingStepsLeft: { fixedTrainingStepsLeft: 10000, purchasedTrainingSteps: 0 },
        usage: { percent: 100, isNegative: false, timeUntilNextPercent: 0 } } }));
    } else if (req.url === '/ai/generate-image') {
      generations++; res.writeHead(200, { 'content-type': 'image/png' }); res.end(png);
    } else { unexpectedUpstream++; res.writeHead(500); res.end('unexpected synthetic upstream route'); }
  });
  const upstreamPort = await listen(upstream), reservation = http.createServer(), port = await listen(reservation);
  await new Promise(resolve => reservation.close(resolve));
  const base = `http://127.0.0.1:${port}`;
  Object.assign(env, { PORT: String(port), NOVELAI_API_URL: `http://127.0.0.1:${upstreamPort}`,
    NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${upstreamPort}` });
  if (!owner) env.PUBLIC_BASE_URL = base;
  const preload = path.join(dataDir, 'deny-external-fetch.mjs');
  await writeFile(preload, `const original = globalThis.fetch;
globalThis.fetch = (input, options) => {
  const url = new URL(typeof input === 'string' || input instanceof URL ? input : input.url);
  if (url.protocol !== 'http:' || !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)) throw new Error('External fetch blocked by executor artifact test');
  return original(input, { ...options, redirect: 'error' });
};\n`);
  let child, exited, output = '';
  const secrets = [adminToken, internalToken, upstreamToken, env.DISCORD_CLIENT_SECRET, ...members.flatMap(member => [member.api_key, member.password]),
    ...sessions.map(session => session.token)].filter(Boolean);
  const safeOutput = () => secrets.reduce((text, secret) => text.replaceAll(secret, '[redacted]'), output);
  const record = chunk => { output = (output + chunk.toString()).slice(-12000); };
  const stop = async () => {
    if (child?.exitCode === null && child?.signalCode === null) child.kill('SIGKILL');
    if (exited) await exited;
  };
  t.after(async () => {
    await stop(); audit.close(); upstream.closeAllConnections(); await new Promise(resolve => upstream.close(resolve));
    if (!t.passed && output) t.diagnostic(safeOutput());
  });
  const start = async () => {
    child = spawn(process.execPath, ['--import', pathToFileURL(preload).href, entry], {
      cwd: path.dirname(entry), env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    exited = once(child, 'exit'); child.stdout.on('data', record); child.stderr.on('data', record);
    await new Promise((resolve, reject) => {
      const cleanup = () => { clearTimeout(timer); child.stdout.off('data', ready); child.off('exit', failed); child.off('error', failed); };
      const ready = () => { if (output.includes('Nai2API listening')) { cleanup(); resolve(); } };
      const failed = () => { cleanup(); reject(new Error('Built executor failed to start: ' + safeOutput())); };
      const timer = setTimeout(failed, 15000);
      child.stdout.on('data', ready); child.once('exit', failed); child.once('error', failed);
    });
  };
  const call = (route, { body, cookie, member, internal = internalToken, headers = {} } = {}) => {
    const url = new URL(route, base); assert.equal(url.origin, base, 'test requests must remain on the local executor');
    return fetch(url, { method: body === undefined ? 'GET' : 'POST', redirect: 'error', signal: AbortSignal.timeout(15000),
      headers: { origin: base, ...(internal === null ? {} : { 'x-executor-token': internal }), ...(cookie ? { cookie } : {}),
        ...(member === undefined ? {} : { authorization: 'Bearer ' + members[member].api_key }),
        ...(body === undefined ? {} : { 'content-type': 'application/json' }), ...headers },
      body: body === undefined ? undefined : JSON.stringify(body) });
  };
  const checkStorage = () => {
    assert.equal(audit.pragma('integrity_check', { simple: true }), 'ok');
    assert.equal(audit.prepare('SELECT COUNT(*) AS n FROM __drizzle_migrations').get().n, migrationJournal.entries.length);
    assert.equal(unexpectedUpstream, 0); assert.equal(leakedInternalHeader, false); assert.equal(wrongUpstreamAuth, false);
    assert.doesNotMatch(output, /External fetch blocked/);
  };
  return { start, stop, call, audit, dataDir, members, sessions, secrets, adminToken,
    child: () => child, exited: () => exited, generations: () => generations, checkStorage };
}

async function json(response, status = 200) {
  assert.equal(response.status, status); assert.match(response.headers.get('content-type'), /application\/json/);
  return response.json();
}
async function subscribe(gateway, route, options, firstFrame) {
  const response = await gateway.call(route, options);
  assert.equal(response.status, 200); assert.match(response.headers.get('content-type'), /text\/event-stream/);
  const reader = response.body.getReader(), decoder = new TextDecoder();
  let text = '';
  while (!text.includes(firstFrame)) {
    const chunk = await reader.read(); assert.equal(chunk.done, false); text += decoder.decode(chunk.value, { stream: true });
  }
  const ended = (async () => {
    for (;;) { const chunk = await reader.read(); if (chunk.done) return { text }; text += decoder.decode(chunk.value, { stream: true }); }
  })().catch(error => ({ error }));
  return { ended, cancel: () => reader.cancel() };
}
function assertSafeHeatmap(value, members, total) {
  assert.equal(value.coverage, 'retained-records'); assert.equal(value.timezone, 'Asia/Taipei');
  assert.equal(value.days.reduce((sum, day) => sum + day.total, 0), total);
  assert.equal(Object.hasOwn(value, 'jobs'), false);
  const text = JSON.stringify(value);
  for (const secret of ['synthetic-private-history', 'synthetic-retained-', ...members.flatMap(member => [member.password, member.api_key])]) {
    assert.equal(text.includes(secret), false, 'heatmap must expose aggregate counts only');
  }
}

test('实际 Nitro 产物保留鉴权、SQLite worker、Native 二进制与跨格式幂等，并正常结束 SSE', { timeout: 60000 }, async t => {
  // Relative paths must be rejected before importing the runtime or opening any database/listener.
  const invalid = spawn(process.execPath, [entry], { cwd: tmpdir(), env: { ...envBase(), DATA_DIR: 'relative-data' },
    windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  let invalidOutput = ''; invalid.stdout.on('data', chunk => { invalidOutput += chunk; }); invalid.stderr.on('data', chunk => { invalidOutput += chunk; });
  t.after(() => { if (invalid.exitCode === null && invalid.signalCode === null) invalid.kill('SIGKILL'); });
  const [invalidCode] = await bounded(once(invalid, 'exit'), 5000, 'relative DATA_DIR startup did not exit');
  assert.notEqual(invalidCode, 0); assert.match(invalidOutput, /requires an absolute DATA_DIR/); assert.doesNotMatch(invalidOutput, /Nai2API listening/);

  const gateway = await fixture(t); await gateway.start();
  for (const internal of [null, 'synthetic-wrong-internal-token']) {
    const rejected = await gateway.call('/api/health', { internal });
    assert.equal(rejected.status, 401); assert.equal(rejected.headers.get('cache-control'), 'no-store');
    assert.deepEqual(await rejected.json(), { error: 'Invalid executor credentials.' });
  }
  await json(await gateway.call('/api/health'));
  assert.equal((await gateway.call('/api/member/me')).status, 401);
  assert.equal((await gateway.call('/api/member/me', { member: 0 })).status, 401);
  assert.equal((await gateway.call('/api/member/owner/member-management')).status, 401);
  assert.equal((await gateway.call('/api/admin/member-management')).status, 403);
  await json(await gateway.call('/api/admin/member-management?view=heatmap', { headers: { 'x-admin-token': gateway.adminToken } }));

  const beforeLogin = Date.now();
  const loginResponse = await gateway.call('/api/member/login', { body: { username: gateway.members[0].username, password: gateway.members[0].password } });
  const login = await json(loginResponse), setCookie = loginResponse.headers.get('set-cookie');
  assert.equal(login.ok, true); assert.ok(login.expiresAt >= beforeLogin + 7 * 86400000);
  assert.match(setCookie, /^nai_member_session=[^;]+; Path=\/api\/member; HttpOnly; SameSite=Strict;/);
  assert.match(setCookie, /Max-Age=60480[0-9]|Max-Age=60479[89]/); assert.match(setCookie, /Expires=/);
  const cookie = setCookie.split(';')[0]; gateway.secrets.push(cookie.slice(cookie.indexOf('=') + 1));
  const profileResponse = await gateway.call('/api/member/me', { cookie });
  assert.equal(profileResponse.headers.get('set-cookie'), null);
  assert.equal((await json(profileResponse)).sessionExpiresAt, login.expiresAt);
  assert.equal((await gateway.call('/api/member/owner/member-management', { cookie })).status, 403);
  assert.equal((await gateway.call('/ai/generate-image', { cookie, body: nativeBody })).status, 401);
  assert.equal((await gateway.call('/api/member/heatmap?memberId=' + gateway.members[1].user_id, { cookie })).status, 400);
  assertSafeHeatmap(await json(await gateway.call('/api/member/heatmap', { cookie })), gateway.members, 1);

  const headers = { 'Idempotency-Key': 'synthetic-built-output-one-generation' };
  const generated = await gateway.call('/ai/generate-image', { body: nativeBody, member: 0, headers });
  assert.equal(generated.status, 201); assert.match(generated.headers.get('content-type'), /application\/zip/);
  assert.match(generated.headers.get('vary'), /Accept/i);
  const requestId = generated.headers.get('x-nai-request-id'); assert.ok(requestId);
  const zip = Buffer.from(await generated.arrayBuffer());
  assert.equal(zip.readUInt32LE(0), 0x04034b50); assert.equal(zip.readUInt16LE(8), 0);
  const imageAt = 30 + zip.readUInt16LE(26) + zip.readUInt16LE(28);
  assert.deepEqual(zip.subarray(imageAt, imageAt + zip.readUInt32LE(18)), png);
  const eocd = zip.length - 22; assert.equal(zip.readUInt32LE(eocd), 0x06054b50);
  assert.equal(zip.readUInt16LE(eocd + 20), 0); assert.equal(zip.readUInt16LE(eocd + 10), 1);
  assert.equal(zip.readUInt32LE(eocd + 12) + zip.readUInt32LE(eocd + 16), eocd);
  const jobs = gateway.audit.prepare('SELECT data FROM jobs').all().map(row => JSON.parse(row.data));
  const job = jobs.find(item => item.requestId === requestId); assert.ok(job); assert.equal(job.status, 'done');
  const parent = await json(await gateway.call('/api/requests/' + requestId, { member: 0 }));
  assert.equal(parent.id, requestId);
  assert.equal((await gateway.call('/api/requests/' + requestId, { member: 1 })).status, 404);
  const publicJob = await json(await gateway.call('/api/jobs/' + job.id, { member: 0 }));
  assert.equal(publicJob.status, 'done'); assert.equal(publicJob.imageId, job.imageId); assert.ok(publicJob.imageUrl);
  for (const route of [publicJob.imageUrl, '/api/jobs/' + job.id + '/content']) {
    const image = await gateway.call(route, { member: 0 }); assert.equal(image.status, 200);
    assert.deepEqual(Buffer.from(await image.arrayBuffer()), png);
  }
  const image = JSON.parse(gateway.audit.prepare('SELECT data FROM images WHERE id = ?').get(job.imageId).data);
  assert.match(image.file, /^images\/[A-Za-z0-9_-]+\.png$/);
  assert.deepEqual(await readFile(path.join(gateway.dataDir, image.file)), png);
  const counts = () => ['jobs', 'images', 'generation_requests', 'generation_idempotency', 'fair_quota_settlements', 'ledger']
    .map(table => gateway.audit.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n);
  const originalCounts = counts();
  const charge = JSON.parse(gateway.audit.prepare('SELECT charge FROM fair_quota_settlements WHERE job_id = ?').get(job.id).charge);
  assert.equal(charge.resource, 'free'); assert.equal(charge.status, 'settled'); assert.equal(charge.amount, 0);
  const asJson = await gateway.call('/v1/ai/generate-image', { body: nativeBody, member: 0,
    headers: { ...headers, accept: 'application/json; charset=utf-8' } });
  assert.equal(asJson.headers.get('x-nai-request-id'), requestId); assert.match(asJson.headers.get('vary'), /Accept/i);
  assert.deepEqual((await json(asJson, 201)).images, [{ image: png.toString('base64'), index: 0, seed: job.request.seed }]);
  assert.equal(job.request.seed, nativeBody.parameters.seed);
  const unacceptable = await gateway.call('/ai/generate-image', { body: nativeBody, member: 0,
    headers: { 'Idempotency-Key': 'synthetic-unacceptable-output', accept: 'application/xml' } });
  assert.equal(unacceptable.status, 406); await unacceptable.text();
  assert.deepEqual(counts(), originalCounts); assert.equal(gateway.generations(), 1);
  const replay = await gateway.call('/v1/ai/generate-image-stream', { body: nativeBody, member: 0, headers });
  assert.equal(replay.status, 200); assert.match(replay.headers.get('content-type'), /application\/octet-stream/);
  assert.equal(replay.headers.get('x-nai-request-id'), requestId);
  const frames = Buffer.from(await replay.arrayBuffer()); assert.ok(frames.includes(png), 'MessagePack must retain original PNG bytes');
  assert.ok(frames.includes(Buffer.from('event_type'))); assert.ok(frames.includes(Buffer.from('image')));
  const conflict = await gateway.call('/ai/generate-image', { body: { ...nativeBody, input: 'different synthetic operation' }, member: 0, headers });
  assert.equal(conflict.status, 409); await conflict.text();
  assert.deepEqual(counts(), originalCounts); assert.equal(gateway.generations(), 1);
  assertSafeHeatmap(await json(await gateway.call('/api/member/heatmap', { cookie })), gateway.members, 2);
  gateway.checkStorage();

  const shared = await subscribe(gateway, '/api/member/shared-events', { cookie }, 'event: shared-status');
  const previews = await subscribe(gateway, '/api/jobs/events', { member: 0 }, ': connected');
  t.after(async () => { await shared.cancel().catch(() => {}); await previews.cancel().catch(() => {}); });
  await t.test('Linux SIGTERM 使两类 SSE 普通 EOF，及时正常退出', { skip: process.platform === 'win32' }, async () => {
    const before = counts(), started = Date.now(); gateway.child().kill('SIGTERM');
    const [sharedEnd, previewEnd, exit] = await bounded(Promise.all([shared.ended, previews.ended, gateway.exited()]), 5000,
      'SSE subscriptions held executor shutdown past five seconds');
    assert.equal(sharedEnd.error, undefined); assert.equal(previewEnd.error, undefined);
    assert.doesNotMatch(sharedEnd.text, /auth-expired/); assert.doesNotMatch(previewEnd.text, /auth-expired/);
    assert.deepEqual(exit, [0, null]); assert.ok(Date.now() - started < 5000);
    assert.deepEqual(counts(), before); assert.equal(gateway.generations(), 1); gateway.checkStorage();
  });
});

test('实际 Nitro 产物的内部认证不替代 owner 与成员范围认证', { timeout: 30000 }, async t => {
  const gateway = await fixture(t, { owner: true }); await gateway.start();
  const cookie = index => 'nai_member_session=' + gateway.sessions[index].token;
  const route = '/api/member/owner/member-management?view=heatmap';
  assert.equal((await gateway.call(route)).status, 401);
  assert.equal((await gateway.call(route, { cookie: cookie(1), headers: { 'x-admin-token': gateway.adminToken } })).status, 403);
  assertSafeHeatmap(await json(await gateway.call(route, { cookie: cookie(0) })), gateway.members, 3);
  const other = await json(await gateway.call(route + '&memberId=' + gateway.members[1].user_id, { cookie: cookie(0) }));
  assert.equal(other.memberId, gateway.members[1].user_id); assertSafeHeatmap(other, gateway.members, 2);
  const profile = await json(await gateway.call('/api/member/me', { cookie: cookie(0) }));
  assert.equal(profile.owner, true); assert.equal(profile.sessionExpiresAt, gateway.sessions[0].expiresAt);
  assert.equal(gateway.generations(), 0); gateway.checkStorage();
});
