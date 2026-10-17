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
import { readNativeEvents } from '../../../tests/helpers/native-stream-client.mjs';

const webRoot = fileURLToPath(new URL('../../', import.meta.url));
const executorEntry = fileURLToPath(new URL('../../../executor/.output/server/start.mjs', import.meta.url));
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
// The same valid 128×128 PNG as native-image tests; reference pricing rejects tiny (<100 character) encodings.
const referencePng = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAIAAAACACAYAAADDPmHLAAAAVklEQVR4nO3BMQEAAADCoPVPbQwfoAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAOBvAI8AAT4ZY7sAAAAASUVORK5CYII=', 'base64');
const vibe = Buffer.from([0, 255, 128, 3, 13, 10, 0, 42]);
const previewPayload = Buffer.concat([Buffer.from([0x82, 0xaa]), Buffer.from('event_type'), Buffer.from([0xac]),
  Buffer.from('intermediate'), Buffer.from([0xa5]), Buffer.from('image'), Buffer.from([0xc4, png.length]), png]);
const previewLength = Buffer.alloc(4); previewLength.writeUInt32BE(previewPayload.length);
const previewFrame = Buffer.concat([previewLength, previewPayload]);
const publicHost = 'gateway.example', publicOrigin = 'https://' + publicHost;
const envBase = () => Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE']
  .filter(key => process.env[key]).map(key => [key, process.env[key]]));
const jobBody = prompt => ({ prompt, model: 'nai-diffusion-4-5-full', width: 1024, height: 1024, steps: 23, seed: 42 });
const nativeBody = input => ({ input, model: 'nai-diffusion-4-5-full', action: 'generate',
  parameters: { width: 1024, height: 1024, steps: 23, seed: 42, n_samples: 1 } });
const chatBody = (label, stream = false) => ({ model: 'nai-diffusion-4-5-full:k_euler_ancestral', stream,
  messages: [{ role: 'user', content: `提示词: ${label}\n画师串:\n尺寸: 方图\n提示词引导值: 5\n缩放引导值: 0\n负面提示词:` }] });
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
function call(base, route, { method, body, headers = {}, timeout = 15000, agent } = {}) {
  const target = new URL(route, base);
  assert.equal(target.origin, base, 'fixture calls must stay on the local process');
  return new Promise((resolve, reject) => {
    const request = http.request(target, { method: method || (body === undefined ? 'GET' : 'POST'), signal: AbortSignal.timeout(timeout), agent,
      headers: { host: publicHost, origin: publicOrigin, ...(body === undefined ? {} : { 'content-type': 'application/json' }), ...headers } }, response => {
      const chunks = [];
      response.on('data', chunk => chunks.push(chunk)); response.on('error', reject);
      response.on('end', () => resolve({ status: response.statusCode, headers: response.headers, body: Buffer.concat(chunks) }));
    });
    request.on('error', reject);
    const bytes = body === undefined ? undefined : typeof body === 'string' || Buffer.isBuffer(body) ? body : JSON.stringify(body);
    if (headers['transfer-encoding'] === 'chunked') { request.write(bytes.subarray(0, 13)); request.end(bytes.subarray(13)); }
    else request.end(bytes);
  });
}
function json(response, status = 200) {
  assert.equal(response.status, status, response.body.toString().slice(0, 500));
  assert.match(response.headers['content-type'], /application\/json/);
  return JSON.parse(response.body);
}

async function subscribe(t, base, cookie, { route = '/api/member/shared-events', headers = {}, body } = {}) {
  const frames = [];
  let response, ended = false, failure, buffer = '';
  const request = http.request(base + route, { method: body ? 'POST' : 'GET', headers: { host: publicHost, origin: publicOrigin,
    ...(cookie ? { cookie } : {}), ...(body ? { 'content-type': 'application/json' } : {}), ...headers } }, incoming => {
    response = incoming;
    incoming.setEncoding('utf8');
    incoming.on('data', chunk => {
      buffer += chunk;
      for (let at; (at = buffer.indexOf('\n\n')) !== -1;) {
        const frame = buffer.slice(0, at); buffer = buffer.slice(at + 2);
        if (frame.startsWith(':')) frames.push({ comment: frame });
        else {
          const event = /^event: ?(.+)$/m.exec(frame)?.[1], data = /^data: ?(.+)$/m.exec(frame)?.[1];
          if (data) frames.push({ event: event || 'message', data: data === '[DONE]' ? data : JSON.parse(data) });
        }
      }
    });
    incoming.on('end', () => { ended = true; });
    incoming.on('error', error => { failure = error; ended = true; });
  });
  request.on('error', error => { failure = error; ended = true; });
  request.end(body ? JSON.stringify(body) : undefined);
  t.after(() => request.destroy());
  await until(() => response || failure, 10000, 'Nuxt SSE response did not start');
  if (failure) throw failure;
  assert.equal(response.statusCode, 200); assert.match(response.headers['content-type'], /text\/event-stream/);
  return { frames, request, response, ended: () => ended, failure: () => failure };
}

async function fixture(t, nuxtEntry, { holdFirst = false, holdFileSync = false, discord = false, directExecutor = false, directTimeout } = {}) {
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
  const { createMemberSession } = await import(packaged('server/member-auth.js'));
  const Database = runtimeRequire('better-sqlite3');
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-nuxt-executor-'));
  const adminToken = 'synthetic-nuxt-executor-admin-token-only';
  const internalToken = 'synthetic-nuxt-executor-internal-token-only';
  const upstreamToken = 'pst-synthetic-nuxt-executor-upstream-only';
  const initialized = await bootstrap({ env: { ADMIN_TOKEN: adminToken, NOVELAI_TOKEN: upstreamToken, DATA_DIR: dataDir }, log() {} });
  const members = JSON.parse(await readFile(initialized.credentialPath, 'utf8')).accounts;
  const discordEnv = discord ? { DISCORD_CLIENT_ID: '123456789012345678', DISCORD_GUILD_ID: '1134557553011998840',
    DISCORD_CLIENT_SECRET: 'synthetic-query-discord-client-secret', OWNER_USER_ID: members[1].user_id, PUBLIC_BASE_URL: publicOrigin } : {};
  const sessions = [];
  const seeded = new JsonStore(dataDir); await seeded.init();
  try {
    await seeded.update(db => {
      Object.assign(db.accounts[0], { quotaTier: 3, subscriptionActive: true, subscriptionExpiresAt: Date.now() + 86400000,
        quotaFixed: 10000, quotaPurchased: 0, v5UsagePercent: 100, quotaCheckedAt: new Date().toISOString() });
      syncFairQuota(db);
      if (discord) {
        db.users.forEach((user, index) => {
          user.discord = { id: String(222222222222222220n + BigInt(index)), guildId: discordEnv.DISCORD_GUILD_ID,
            member: true, checkedAt: index ? Date.now() : 0, expiresAt: index ? Date.now() + 3600000 : 0,
            accessToken: `synthetic-discord-access-${index}-0`, refreshToken: `synthetic-discord-refresh-${index}-0` };
        });
        db.jobs = [0, 1, 1].map((member, index) => ({ id: `synthetic-query-history-${index}`, status: 'done', source: 'novelai-native',
          createdAt: new Date().toISOString(), userToken: db.users[member].token,
          generationAttempt: { id: `synthetic-query-attempt-${index}`, memberId: db.users[member].id },
          request: { operation: 'generate', model: 'nai-diffusion-4-5-full', prompt: 'synthetic-private-query-history' } }));
      }
    }, { immediate: true });
    if (discord) for (const user of seeded.db.users) sessions.push(await createMemberSession(seeded, user, discordEnv));
  } finally { await seeded.close(); }

  // After seeding, only the independently launched executor writes the business database.
  const audit = new Database(path.join(dataDir, 'library.sqlite'), { readonly: true, fileMustExist: true });
  const rows = table => audit.prepare(`SELECT data FROM ${table}`).all().map(row => JSON.parse(row.data));
  const jobs = () => rows('jobs');
  const releaseFirst = deferred();
  const heldGenerations = [];
  let nextGeneration;
  const upstreamCalls = [];
  const discordState = { refreshTokens: [], authorizationCodes: [], membershipTokens: [], membershipCalls: [], status: 200, gate: null, persistedBeforeMembership: false };
  let fileSynced = false;
  let generations = 0, quotaCalls = 0, fixedRemaining = 10000, unexpectedUpstream = 0, leakedInternalHeader = false, wrongUpstreamAuth = false;
  const upstream = http.createServer(async (req, res) => {
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    if (discord && req.url.startsWith('/discord/')) {
      if (req.url === '/discord/oauth2/token' && req.method === 'POST') {
        const form = new URLSearchParams(Buffer.concat(chunks).toString());
        assert.equal(form.get('client_secret'), discordEnv.DISCORD_CLIENT_SECRET);
        const login = form.get('grant_type') === 'authorization_code';
        if (login) {
          assert.equal(form.get('redirect_uri'), publicOrigin + '/api/member/discord/callback');
          discordState.authorizationCodes.push(form.get('code'));
        } else {
          assert.equal(form.get('grant_type'), 'refresh_token');
          discordState.refreshTokens.push(form.get('refresh_token'));
        }
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ access_token: login ? 'synthetic-discord-login-access-1' : 'synthetic-discord-access-0-1',
          refresh_token: login ? 'synthetic-discord-login-refresh-1' : 'synthetic-discord-refresh-0-1',
          expires_in: 3600, token_type: 'Bearer', scope: 'identify guilds.members.read' }));
      } else if (req.url === '/discord/users/@me') {
        assert.equal(req.headers.authorization, 'Bearer synthetic-discord-login-access-1');
        res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify({ id: '222222222222222221', bot: false }));
      } else if (req.url === `/discord/users/@me/guilds/${discordEnv.DISCORD_GUILD_ID}/member`) {
        discordState.membershipTokens.push(req.headers.authorization);
        const verification = { startedAt: performance.now(), respondedAt: null };
        discordState.membershipCalls.push(verification);
        const user = rows('users').find(user => user.id === members[0].user_id);
        discordState.persistedBeforeMembership = user.discord.refreshToken === 'synthetic-discord-refresh-0-1';
        if (discordState.gate) await discordState.gate.promise;
        verification.respondedAt = performance.now();
        res.writeHead(discordState.status, { 'content-type': 'application/json' });
        res.end(JSON.stringify(discordState.status === 200 ? { pending: false, user: {
          id: req.headers.authorization === 'Bearer synthetic-discord-login-access-1' ? '222222222222222221' : '222222222222222220'
        } } : { message: 'synthetic membership unavailable' }));
      } else { unexpectedUpstream++; res.writeHead(500); res.end('unexpected synthetic Discord route'); }
      return;
    }
    leakedInternalHeader ||= Object.hasOwn(req.headers, 'x-executor-token');
    wrongUpstreamAuth ||= req.headers.authorization !== 'Bearer ' + upstreamToken;
    if (req.url === '/user/data') {
      quotaCalls++;
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ subscription: { tier: 3, active: true, expiresAt: Math.floor(Date.now() / 1000) + 86400,
        trainingStepsLeft: { fixedTrainingStepsLeft: fixedRemaining, purchasedTrainingSteps: 0 },
        usage: { percent: 100, isNegative: false, timeUntilNextPercent: 0 } } }));
    } else if (['/ai/generate-image', '/ai/generate-image-stream', '/ai/encode-vibe', '/ai/upscale'].includes(req.url)) {
      generations++;
      const generated = JSON.parse(Buffer.concat(chunks));
      upstreamCalls.push({ path: req.url, body: generated });
      if (req.url.includes('generate-image')) {
        const quote = quoteFairRequest({ ...generated.parameters, model: generated.model });
        if (quote.resource === 'anlas') fixedRemaining -= quote.amount;
      }
      if (generations === 1 && holdFirst) await releaseFirst.promise;
      const stream = req.url.endsWith('-stream');
      const held = nextGeneration; nextGeneration = undefined;
      if (held) {
        held.started = true;
        if (held.preview) {
          assert.equal(stream, true, 'preview fixture must exercise an actual upstream MessagePack stream');
          res.writeHead(200, { 'content-type': 'application/octet-stream' }); res.write(previewFrame);
        }
        await held.promise;
      }
      const encoded = req.url === '/ai/encode-vibe';
      if (!res.headersSent) res.writeHead(200, { 'content-type': stream || encoded ? 'application/octet-stream' : 'image/png' });
      res.end(encoded ? vibe : stream ? nativeImagesMsgpack([png]) : png);
    } else { unexpectedUpstream++; res.writeHead(500); res.end('unexpected synthetic upstream route'); }
  });
  const upstreamPort = await listen(upstream), executorPort = await unusedPort(), nuxtPort = await unusedPort();
  const executorBase = `http://127.0.0.1:${executorPort}`, nuxtBase = `http://127.0.0.1:${nuxtPort}`;
  const observations = [], droppedReceipts = [];
  let dropNextReceipt = false, dropMutationPath, queryBatch;
  // This relay injects a lost internal acknowledgement; it is not the Nuxt gateway under test.
  const relay = http.createServer(async (req, res) => {
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    const body = Buffer.concat(chunks);
    const observation = { path: req.url, method: req.method, headers: { ...req.headers }, body, forwardedAt: null, respondedAt: null };
    observations.push(observation);
    const batch = req.method === 'GET' && queryBatch?.paths.has(req.url) ? queryBatch : null;
    if (batch) {
      batch.arrived++;
      if (batch.arrived === batch.paths.size) batch.open.resolve();
      await batch.open.promise;
    }
    const forwarded = http.request(new URL(req.url, executorBase), { method: req.method, headers: req.headers }, response => {
      observation.respondedAt = performance.now();
      if (response.headers['content-type']?.startsWith('text/event-stream')) {
        res.writeHead(response.statusCode, response.headers); res.flushHeaders();
        response.on('error', () => res.destroy()); res.once('close', () => response.destroy());
        response.pipe(res); return;
      }
      const parts = [];
      response.on('data', chunk => parts.push(chunk));
      response.on('error', () => res.destroy());
      response.on('end', () => {
        const received = Buffer.concat(parts);
        observation.response = received;
        if (dropNextReceipt && req.method === 'POST' && response.statusCode === 202) {
          dropNextReceipt = false; droppedReceipts.push(JSON.parse(received)); res.destroy(); return;
        }
        if (dropMutationPath === req.url && ['POST', 'PATCH'].includes(req.method) && response.statusCode === 200) {
          dropMutationPath = undefined; res.destroy(); return;
        }
        res.writeHead(response.statusCode, response.headers); res.end(received);
      });
    });
    forwarded.on('error', () => { if (!res.destroyed && !res.headersSent) { res.writeHead(502); res.end('synthetic relay transport failure'); } });
    forwarded.once('finish', () => {
      observation.forwardedAt = performance.now();
      if (batch && ++batch.forwarded === batch.paths.size) queryBatch = null;
    });
    res.once('close', () => forwarded.destroy());
    forwarded.end(body);
  });
  const relayPort = await listen(relay);
  const preload = path.join(dataDir, 'deny-external-fetch.mjs');
  await writeFile(preload, `const original = globalThis.fetch;
globalThis.fetch = (input, options) => {
  const url = new URL(typeof input === 'string' || input instanceof URL ? input : input.url);
  if (process.env.DATA_DIR && process.env.DISCORD_CLIENT_ID && url.origin === 'https://discord.com' && url.pathname.startsWith('/api/v10/')) {
    return original('http://127.0.0.1:${upstreamPort}/discord/' + url.pathname.slice('/api/v10/'.length) + url.search, { ...options, redirect: 'error' });
  }
  if (url.protocol !== 'http:' || !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)) throw new Error('External fetch blocked by Nuxt executor fixture');
  return original(input, { ...options, redirect: 'error' });
};\n`);
  const discordSecrets = discord ? [discordEnv.DISCORD_CLIENT_SECRET, 'synthetic-discord-access-0-1', 'synthetic-discord-refresh-0-1',
    'synthetic-discord-login-access-1', 'synthetic-discord-login-refresh-1',
    ...members.flatMap((_, index) => [`synthetic-discord-access-${index}-0`, `synthetic-discord-refresh-${index}-0`])] : [];
  const secrets = [adminToken, internalToken, upstreamToken, ...discordSecrets, ...members.flatMap(member => [member.api_key, member.password]),
    ...sessions.map(session => session.token)];
  const children = [];
  function processHandle(label, entry, env, imports = []) {
    let child, exited, output = '';
    const redacted = () => secrets.reduce((text, secret) => text.replaceAll(secret, '[redacted]'), output);
    const stop = async (signal = 'SIGKILL') => {
      if (child?.exitCode === null && child?.signalCode === null) child.kill(signal);
      if (exited) return await exited;
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
    ...(directTimeout === undefined ? {} : { DIRECT_GENERATE_TIMEOUT_MS: String(directTimeout) }),
    NOVELAI_API_URL: `http://127.0.0.1:${upstreamPort}`, NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${upstreamPort}`, ...discordEnv },
  [new URL('../fixtures/executor-file-sync.mjs', import.meta.url).href]);
  const nuxt = processHandle('built Nuxt', nuxtEntry, { ...envBase(), NODE_ENV: 'production', HOST: '127.0.0.1', PORT: String(nuxtPort),
    NUXT_EXECUTOR_BASE_URL: directExecutor ? executorBase : `http://127.0.0.1:${relayPort}`, NUXT_EXECUTOR_TOKEN: internalToken });
  const startNuxt = () => nuxt.start(async () => {
    try { return (await call(nuxtBase, '/')).status === 200; } catch { return false; }
  });
  const startExecutor = () => executor.start(output => output.includes('Nai2API listening'), { RESULT_RECOVERY_TEST_STAGE: '' });
  t.after(async () => {
    releaseFirst.resolve();
    for (const held of heldGenerations) held.resolve();
    discordState.gate?.resolve();
    queryBatch?.open.resolve();
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
  const cookie = member => 'nai_member_session=' + sessions[member].token;
  const revoke = member => call(executorBase, '/api/admin/member-management', { body: { action: 'member-status', userId: members[member].user_id, enabled: false },
    headers: { host: 'localhost:18082', origin: 'http://localhost:18082', 'x-admin-token': adminToken, 'x-executor-token': internalToken } });
  return { request, nuxt, executor, startNuxt, startExecutor, dataDir, audit, jobs, members, observations, droppedReceipts, internalToken,
    nuxtBase, executorBase, adminToken, upstreamToken, cookie, revoke, discordState, forbiddenResponseSecrets: [upstreamToken, internalToken, adminToken, ...discordSecrets],
    fileSynced: () => fileSynced, upstreamCalls,
    holdNext(preview = false) { assert.equal(nextGeneration, undefined); const held = { ...deferred(), preview, started: false };
      heldGenerations.push(held); nextGeneration = held; return held; },
    releaseFirst: releaseFirst.resolve, generations: () => generations, quotaCalls: () => quotaCalls,
    batchQueries(routes) { queryBatch = { paths: new Set(routes), arrived: 0, forwarded: 0, open: deferred() }; return queryBatch; },
    dropReceipt() { dropNextReceipt = true; }, dropMutation(route) { dropMutationPath = route; }, checkStorage };
}

test('真实 Nuxt 与执行器的生命周期隔离和内部受理重试', { timeout: 240000 }, async t => {
  await stat(executorEntry); // A missing build is a failure; never substitute the legacy Node server.
  const ctx = createTestContext({ rootDir: webRoot, dev: false, build: true, browser: false, server: false,
    nuxtConfig: { compatibilityDate: '2026-10-17' } });
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

  await t.test('正式 Nuxt GET 保留成员范围，Discord并发刷新先落盘，资格503与执行器重启不丢新令牌', { timeout: 45000 }, async sub => {
    const f = await fixture(sub, nuxtEntry, { discord: true });
    const memberCall = (route, member = 0, headers = {}) => f.request(route, { member, headers: { cookie: f.cookie(member), ...headers } });
    const ledger = () => f.audit.prepare('SELECT id, data FROM ledger ORDER BY id').all();
    const user = () => JSON.parse(f.audit.prepare('SELECT data FROM users WHERE id = ?').get(f.members[0].user_id).data);
    const originalLedger = ledger(), originalVersion = user().memberAuth.version;
    const originalSessions = f.audit.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n;
    const options = json(await f.request('/api/member/login-options')); assert.equal(options.discord, true); assert.equal(options.ready, true);
    const routes = ['/api/member/me', '/api/member/heatmap?operation=generate', '/api/member/shared-status'];
    f.discordState.status = 503; f.discordState.gate = deferred();
    const failureBatch = f.batchQueries(routes);
    const failedReads = routes.map(route => memberCall(route));
    await until(() => f.discordState.membershipTokens.length === 1 && failureBatch.forwarded === 3,
      10000, 'concurrent queries did not reach Discord membership verification');
    assert.deepEqual(f.discordState.refreshTokens, ['synthetic-discord-refresh-0-0']);
    assert.equal(f.discordState.persistedBeforeMembership, true);
    assert.equal(user().discord.refreshToken, 'synthetic-discord-refresh-0-1');
    f.discordState.gate.resolve();
    const failed = await Promise.all(failedReads); assert.ok(failed.every(response => response.status === 503));
    assert.equal(f.discordState.membershipTokens.length, 1, JSON.stringify({
      requests: f.observations.filter(item => routes.includes(item.path)).map(({ path, forwardedAt, respondedAt }) => ({ path, forwardedAt, respondedAt })),
      verifications: f.discordState.membershipCalls
    }));
    assert.deepEqual(f.discordState.refreshTokens, ['synthetic-discord-refresh-0-0']);
    assert.equal(user().memberAuth.version, originalVersion);
    assert.equal(f.audit.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n, originalSessions);
    assert.deepEqual(ledger(), originalLedger);

    const nuxtPid = f.nuxt.child().pid;
    await f.executor.stop(); await f.startExecutor();
    f.discordState.status = 200; f.discordState.gate = deferred();
    const successBatch = f.batchQueries(routes);
    const reads = routes.map(route => memberCall(route));
    await until(() => f.discordState.membershipTokens.length === 2 && successBatch.forwarded === 3,
      10000, 'restarted executor did not verify saved grant');
    assert.deepEqual(f.discordState.refreshTokens, ['synthetic-discord-refresh-0-0']);
    assert.deepEqual(f.discordState.membershipTokens, ['Bearer synthetic-discord-access-0-1', 'Bearer synthetic-discord-access-0-1']);
    f.discordState.gate.resolve();
    const results = await Promise.all(reads), [me, heatmap, shared] = results.map(response => json(response));
    assert.equal(f.discordState.membershipTokens.length, 2, 'each overlapping query batch must share one membership verification');
    assert.equal(me.username, f.members[0].username); assert.equal(me.owner, false);
    assert.equal(heatmap.days.reduce((sum, day) => sum + day.total, 0), 1);
    assert.equal(shared.subscriptionActive, true); assert.equal(f.nuxt.child().pid, nuxtPid);
    assert.deepEqual(ledger(), originalLedger);
    assert.equal((await f.request('/api/member/me')).status, 401, 'Bearer must not substitute for a browser session');
    assert.equal((await memberCall('/api/member/heatmap?memberId=' + f.members[1].user_id)).status, 400);
    assert.equal((await memberCall('/api/member/owner/member-management')).status, 403);
    assert.equal(json(await memberCall('/api/member/key')).apiKey, f.members[0].api_key);
    const owner = json(await memberCall('/api/member/owner/member-management', 1)); assert.equal(owner.users.length, 4);
    assert.equal(json(await memberCall('/api/member/owner/statistics?days=7', 1)).totals.total, 3);
    assert.equal(json(await memberCall('/api/member/owner/member-management?view=heatmap', 1)).days.reduce((sum, day) => sum + day.total, 0), 3);
    assert.ok(json(await memberCall('/api/member/owner/member-management?view=losses', 1)).policy);
    assert.equal(json(await memberCall('/api/member/owner/summary', 1)).accounts.length, 1);
    const local = { host: 'localhost:18082', origin: 'http://localhost:18082', 'x-admin-token': f.adminToken };
    assert.equal((await memberCall('/api/admin/member-management', 1, { 'x-admin-token': f.adminToken })).status, 404);
    assert.equal((await f.request('/api/admin/ping', { headers: { host: local.host, origin: local.origin } })).status, 403);
    assert.equal((await f.request('/api/admin/ping', { headers: { ...local, 'x-admin-token': 'synthetic-wrong-admin' } })).status, 401);
    for (const route of ['/api/admin/member-management', '/api/admin/member-management?view=losses', '/api/admin/summary', '/api/admin/ping']) {
      json(await f.request(route, { headers: local }));
    }
    assert.equal((await memberCall('/api/member/me', 0, { host: 'unconfigured.example' })).status, 403);
    assert.equal((await memberCall('/api/member/me', 0, { origin: 'https://untrusted.example' })).status, 403);
    assert.deepEqual(ledger(), originalLedger);

    const accepted = json(await f.request('/api/jobs', { body: jobBody('synthetic query ownership') }), 202);
    assert.equal(json(await memberCall('/api/member/requests/' + accepted.requestId)).id, accepted.requestId);
    assert.equal((await memberCall('/api/member/requests/' + accepted.requestId, 2)).status, 404);
    await until(() => f.jobs().find(job => job.id === accepted.id)?.status === 'done', 10000, 'query ownership sample did not finish');
    for (const route of [...routes, '/api/member/key']) assert.ok(f.observations.some(item => item.path === route), 'formal Nuxt route must reach executor: ' + route);
    for (const observation of f.observations) {
      if (!observation.response) continue;
      for (const secret of f.forbiddenResponseSecrets) assert.ok(!observation.response.includes(Buffer.from(secret)), 'executor response must not expose upstream/Discord credentials');
    }
    assert.equal(f.discordState.refreshTokens.length, 1); assert.equal(f.generations(), 1); f.checkStorage();
  });

  await t.test('正式密码登录保持七天固定期限，Nuxt与执行器重启不丢会话，注销持久生效', { timeout: 30000 }, async sub => {
    const f = await fixture(sub, nuxtEntry);
    const credentials = { username: f.members[0].username, password: f.members[0].password };
    const rawLogin = JSON.stringify(credentials, null, 2) + '\n';
    assert.equal((await f.request('/api/member/login', { body: rawLogin, headers: { origin: 'https://untrusted.example', 'sec-fetch-site': 'cross-site' } })).status, 403);
    assert.equal(f.audit.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n, 0);
    const loginResponse = await f.request('/api/member/login', { body: rawLogin });
    const login = json(loginResponse), setCookie = loginResponse.headers['set-cookie'];
    assert.equal(setCookie.length, 1); assert.match(setCookie[0], /^nai_member_session=[^;]+; Path=\/api\/member; HttpOnly; SameSite=Strict;/);
    assert.match(setCookie[0], /; Secure$/); assert.match(setCookie[0], /Max-Age=60480[0-9]|Max-Age=60479[89]/);
    const cookie = setCookie[0].split(';')[0], headers = { cookie };
    const saved = f.audit.prepare('SELECT created_at, expires_at FROM member_sessions WHERE user_id = ?').get(f.members[0].user_id);
    assert.equal(saved.expires_at - saved.created_at, 7 * 86400000); assert.equal(login.expiresAt, saved.expires_at);
    const forwarded = f.observations.findLast(item => item.path === '/api/member/login');
    assert.deepEqual(forwarded.body, Buffer.from(rawLogin)); assert.equal(forwarded.headers.host, publicHost); assert.equal(forwarded.headers.origin, publicOrigin);
    assert.equal(forwarded.headers['idempotency-key'], undefined, 'login must not be turned into a retried generation command');
    for (const restart of [async () => { await f.nuxt.stop(); await f.startNuxt(); }, async () => { await f.executor.stop(); await f.startExecutor(); }]) {
      await restart();
      const response = await f.request('/api/member/me', { headers });
      assert.equal(json(response).sessionExpiresAt, login.expiresAt); assert.equal(response.headers['set-cookie'], undefined);
    }
    const loggedOut = await f.request('/api/member/logout', { body: {}, headers }); json(loggedOut);
    assert.match(loggedOut.headers['set-cookie'][0], /^nai_member_session=;/); assert.match(loggedOut.headers['set-cookie'][0], /Max-Age=0/);
    assert.equal(f.audit.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n, 0);
    await f.executor.stop(); await f.startExecutor();
    assert.equal((await f.request('/api/member/me', { headers })).status, 401);
    assert.equal(f.generations(), 0); f.checkStorage();
  });

  await t.test('正式Discord回跳跨Nuxt重启保留state，原303与两个Cookie逐项保留，跨站仅限合法回跳', { timeout: 30000 }, async sub => {
    const f = await fixture(sub, nuxtEntry, { discord: true });
    const initialSessions = f.audit.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n;
    assert.equal((await f.request('/api/member/discord/start', { body: {}, headers: { origin: 'https://untrusted.example', 'sec-fetch-site': 'cross-site' } })).status, 403);
    const started = await f.request('/api/member/discord/start', { body: {} });
    const authorization = new URL(json(started).authorizationUrl);
    assert.equal(authorization.origin, 'https://discord.com'); assert.equal(authorization.pathname, '/oauth2/authorize');
    assert.equal(authorization.searchParams.get('redirect_uri'), publicOrigin + '/api/member/discord/callback');
    const state = authorization.searchParams.get('state'); assert.ok(state);
    assert.equal(started.headers['set-cookie'].length, 1);
    assert.match(started.headers['set-cookie'][0], /Path=\/api\/member\/discord; HttpOnly; Secure; SameSite=Lax/);
    const stateCookie = started.headers['set-cookie'][0].split(';')[0];
    const callback = '/api/member/discord/callback?' + new URLSearchParams({ state, code: 'synthetic-owner-code' });
    assert.equal((await f.request(callback, { headers: { cookie: 'nai_discord_state=wrong', origin: 'https://discord.com', 'sec-fetch-site': 'cross-site' } })).status, 400);
    assert.equal(f.discordState.authorizationCodes.length, 0);
    assert.equal(f.audit.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n, initialSessions);
    const executorPid = f.executor.child().pid;
    await f.nuxt.stop(); await f.startNuxt();
    const response = await f.request(callback, { headers: { cookie: stateCookie, origin: 'https://discord.com', 'sec-fetch-site': 'cross-site' } });
    assert.equal(response.status, 303); assert.equal(response.headers.location, publicOrigin + '/');
    assert.equal(response.headers['cache-control'], 'no-store'); assert.equal(f.executor.child().pid, executorPid);
    const cookies = response.headers['set-cookie']; assert.equal(cookies.length, 2);
    const stateCleared = cookies.find(value => value.startsWith('nai_discord_state='));
    const sessionCookie = cookies.find(value => value.startsWith('nai_member_session='));
    assert.match(stateCleared, /^nai_discord_state=;/); assert.match(stateCleared, /Max-Age=0/);
    assert.match(sessionCookie, /Path=\/api\/member; HttpOnly; SameSite=Strict;/); assert.match(sessionCookie, /; Secure$/);
    assert.match(sessionCookie, /Max-Age=4320[0-9]|Max-Age=43199/);
    const ownerCookie = sessionCookie.split(';')[0], me = json(await f.request('/api/member/me', { headers: { cookie: ownerCookie } }));
    assert.equal(me.owner, true); assert.equal(me.username, f.members[1].username);
    const ownerSession = f.audit.prepare('SELECT created_at, expires_at FROM member_sessions WHERE user_id = ? ORDER BY created_at DESC LIMIT 1').get(f.members[1].user_id);
    assert.equal(ownerSession.expires_at - ownerSession.created_at, 12 * 3600000); assert.equal(me.sessionExpiresAt, ownerSession.expires_at);
    assert.equal((await f.request(callback, { headers: { cookie: stateCookie } })).status, 400, 'OAuth state is single-use');
    assert.deepEqual(f.discordState.authorizationCodes, ['synthetic-owner-code']);
    assert.equal((await f.request('/api/member/logout', { body: {}, headers: { cookie: ownerCookie, origin: 'https://untrusted.example', 'sec-fetch-site': 'cross-site' } })).status, 403);
    assert.equal(json(await f.request('/api/member/me', { headers: { cookie: ownerCookie } })).owner, true);
    json(await f.request('/api/member/logout', { body: {}, headers: { cookie: ownerCookie } }));
    assert.equal((await f.request('/api/member/me', { headers: { cookie: ownerCookie } })).status, 401);
    for (const observation of f.observations) if (observation.response) {
      for (const secret of f.forbiddenResponseSecrets) assert.ok(!observation.response.includes(Buffer.from(secret)), 'OAuth response must not disclose grant credentials');
    }
    assert.equal(f.generations(), 0); f.checkStorage();
  });

  await t.test('正式管理写入保留权限、版本与32KiB校验，丢失管理回执不重发已提交命令', { timeout: 30000 }, async sub => {
    const f = await fixture(sub, nuxtEntry, { discord: true });
    const ownerHeaders = { cookie: f.cookie(1) }, memberHeaders = { cookie: f.cookie(2) };
    const adminHeaders = { host: 'localhost:18082', origin: 'http://localhost:18082', 'x-admin-token': f.adminToken };
    const ownerPath = '/api/member/owner/member-management', adminPath = '/api/admin/member-management';
    const fair = () => JSON.parse(f.audit.prepare("SELECT value FROM app_settings WHERE key = 'settings'").get().value).fairQuota;
    const ledger = () => f.audit.prepare('SELECT id, data FROM ledger ORDER BY id').all();
    const initialFair = fair(), initialLedger = ledger();
    const policy = { action: 'fallback-policy', enabled: true };
    assert.equal((await f.request(ownerPath, { body: policy })).status, 401);
    assert.equal((await f.request(ownerPath, { body: policy, headers: memberHeaders })).status, 403);
    assert.equal((await f.request(ownerPath, { body: policy, headers: { ...ownerHeaders, origin: 'https://untrusted.example' } })).status, 403);
    assert.equal((await f.request(adminPath, { body: policy, headers: { ...ownerHeaders, 'x-admin-token': f.adminToken } })).status, 404);
    assert.equal((await f.request(adminPath, { body: policy, headers: { host: adminHeaders.host, origin: adminHeaders.origin } })).status, 403);
    assert.equal((await f.request(adminPath, { body: policy, headers: { ...adminHeaders, 'x-admin-token': 'synthetic-wrong-admin' } })).status, 401);
    assert.equal((await f.request(ownerPath, { body: '{', headers: ownerHeaders })).status, 400);
    assert.equal((await f.request(ownerPath, { body: JSON.stringify({ ...policy, filler: 'x'.repeat(32768) }), headers: ownerHeaders })).status, 413);
    const staleGroups = { action: 'groups', expectedConfigVersion: initialFair.configVersion - 1, groups: initialFair.groups };
    assert.equal((await f.request(ownerPath, { body: staleGroups, headers: ownerHeaders })).status, 409);
    assert.deepEqual(fair(), initialFair); assert.deepEqual(ledger(), initialLedger);

    const rawPolicy = '{\n  "action": "fallback-policy",\n  "enabled": true\n}\n';
    assert.equal(json(await f.request(ownerPath, { body: rawPolicy, headers: ownerHeaders })).autoAnlasFallback, true);
    assert.equal(fair().autoAnlasFallback, true);
    assert.equal(fair().policyHistory.length, (initialFair.policyHistory?.length || 0) + 1);
    const policyObservation = f.observations.findLast(item => item.path === ownerPath);
    assert.deepEqual(policyObservation.body, Buffer.from(rawPolicy)); assert.equal(policyObservation.headers.cookie, ownerHeaders.cookie);
    assert.equal(policyObservation.headers['idempotency-key'], undefined);

    const countBeforeLoss = f.observations.filter(item => item.path === adminPath && item.method === 'POST').length;
    const policyCount = fair().policyHistory.length;
    f.dropMutation(adminPath);
    const lost = await f.request(adminPath, { body: { action: 'fallback-policy', enabled: false }, headers: adminHeaders });
    json(lost, 503); assert.equal(lost.headers['cache-control'], 'no-store');
    assert.equal(f.observations.filter(item => item.path === adminPath && item.method === 'POST').length, countBeforeLoss + 1);
    assert.equal(fair().autoAnlasFallback, false); assert.equal(fair().policyHistory.length, policyCount + 1);
    assert.equal(json(await f.request(adminPath, { body: policy, headers: adminHeaders })).autoAnlasFallback, true);

    const accountId = initialFair.boundAccountId;
    const account = () => JSON.parse(f.audit.prepare('SELECT data FROM accounts WHERE id = ?').get(accountId).data);
    const quotaCalls = f.quotaCalls();
    assert.equal(json(await f.request('/api/member/owner/accounts/quota', { body: { ids: [accountId] }, headers: ownerHeaders })).ok, 1);
    assert.equal(f.quotaCalls(), quotaCalls + 1);
    assert.equal(json(await f.request('/api/member/owner/accounts/' + accountId, { method: 'PATCH', body: { enabled: false }, headers: ownerHeaders })).enabled, false);
    assert.equal(account().enabled, false);
    assert.equal(json(await f.request('/api/member/owner/accounts/' + accountId, { method: 'PATCH', body: { enabled: true }, headers: ownerHeaders })).enabled, true);
    const privateQuota = json(await f.request('/api/admin/accounts/quota', { body: { ids: [accountId] }, headers: adminHeaders }));
    assert.equal(privateQuota.ok, 1); assert.equal(privateQuota.accounts[0].token, f.upstreamToken);
    const bulk = json(await f.request('/api/admin/accounts', { method: 'PATCH', body: { ids: [accountId], weight: 2 }, headers: adminHeaders }));
    assert.equal(bulk.accounts[0].weight, 2); assert.equal(account().weight, 2);
    const name = 'Synthetic private management account';
    assert.equal(json(await f.request('/api/admin/accounts/' + accountId, { method: 'PATCH', body: { name }, headers: adminHeaders })).name, name);
    assert.equal(account().name, name);
    const configured = json(await f.request('/api/admin/fair-quota', { body: { memberIds: initialFair.members }, headers: adminHeaders }));
    assert.deepEqual(configured.members, initialFair.members);
    const disabledToken = 'pst-synthetic-disabled-management-account';
    f.forbiddenResponseSecrets.push(disabledToken);
    const added = json(await f.request('/api/admin/accounts', { body: { token: disabledToken, enabled: false, name: 'Synthetic disabled account' }, headers: adminHeaders }), 201);
    assert.equal(added.enabled, false); assert.notEqual(added.id, accountId);
    assert.equal(f.audit.prepare('SELECT COUNT(*) AS n FROM accounts').get().n, 2);
    for (const observation of f.observations) if (observation.response) {
      // This existing private admin response explicitly reveals PST; owner and public DTOs never do.
      const permittedPrivateQuota = observation.path === '/api/admin/accounts/quota' && observation.method === 'POST'
        && observation.headers.host === adminHeaders.host && observation.headers['x-admin-token'] === f.adminToken;
      for (const secret of f.forbiddenResponseSecrets) {
        if (permittedPrivateQuota && secret === f.upstreamToken) continue;
        assert.ok(!observation.response.includes(Buffer.from(secret)), 'management response must not leak internal/upstream credentials');
      }
    }
    assert.equal(f.generations(), 0); f.checkStorage();
  });

  await t.test('正式 Native 别名共享幂等结果，multipart真实附件保留32MiB边界，Vibe和放大字节可领取', { timeout: 45000 }, async sub => {
    const f = await fixture(sub, nuxtEntry, { directExecutor: true });
    for (const [method, route] of [
      ...['api', 'ai', 'v1', 'user'].map(namespace => ['GET', `/${namespace}/unsupported`]),
      ['GET', '/ai/generate-image'], ['DELETE', '/api/jobs'], ['PATCH', '/v1/chat/completions'], ['POST', '/api/web/jobs'],
      ['OPTIONS', '/ai/generate-image'], ['HEAD', '/api/jobs']
    ]) {
      const proxied = await f.request(route, { method });
      const direct = await call(f.executorBase, route, { method, headers: {
        authorization: 'Bearer ' + f.members[0].api_key, 'x-executor-token': f.internalToken } });
      assert.equal(proxied.status, direct.status, `${method} ${route}`); assert.equal(proxied.body.toString(), direct.body.toString(), `${method} ${route}`);
      for (const name of ['content-type', 'access-control-allow-origin', 'access-control-allow-methods', 'access-control-allow-headers', 'access-control-expose-headers']) {
        assert.equal(proxied.headers[name], direct.headers[name], `${method} ${route}: ${name}`);
      }
    }
    const assertZip = (response, status = 201) => {
      assert.equal(response.status, status, response.body.toString().slice(0, 300));
      assert.match(response.headers['content-type'], /application\/zip/);
      assert.match(response.headers.vary, /(?:^|,\s*)Accept(?:\s*,|$)/i);
      assert.equal(response.body.readUInt32LE(0), 0x04034b50);
      assert.equal(response.body.readUInt16LE(8), 0);
      const offset = 30 + response.body.readUInt16LE(26) + response.body.readUInt16LE(28);
      assert.deepEqual(response.body.subarray(offset, offset + response.body.readUInt32LE(18)), png);
    };
    const counts = () => ['jobs', 'generation_requests', 'generation_idempotency', 'fair_quota_settlements', 'ledger']
      .map(table => f.audit.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n);
    const beforeUnsupported = counts();
    for (const [route, body] of [['/ai/generate-image', nativeBody('unacceptable response type')], ['/ai/upscale', { image: png.toString('base64') }]]) {
      for (const accept of ['application/xml', 'application/zip;q=0, application/json;q=0']) {
        const response = await f.request(route, { body, headers: { accept, 'idempotency-key': 'unacceptable-operation' } });
        assert.equal(response.status, 406); assert.match(response.headers.vary, /Accept/i);
      }
    }
    assert.deepEqual(counts(), beforeUnsupported); assert.equal(f.generations(), 0);
    const aliasBody = nativeBody('synthetic alias replay'); aliasBody.parameters.seed = -1;
    let nativeId;
    for (const route of ['/ai/generate-image', '/v1/ai/generate-image', '/ai/generate-image-stream', '/v1/ai/generate-image-stream']) {
      const response = await f.request(route, { body: aliasBody, headers: { 'idempotency-key': 'native-alias-replay' } });
      assert.ok(response.headers['x-nai-request-id']);
      nativeId ||= response.headers['x-nai-request-id']; assert.equal(response.headers['x-nai-request-id'], nativeId);
      if (route.endsWith('-stream')) {
        assert.equal(response.status, 200);
        assert.match(response.headers['content-type'], /application\/octet-stream/);
        const events = readNativeEvents(response.body); assert.equal(events.length, 1);
        assert.equal(events[0].event_type, 'final'); assert.equal(events[0].samp_ix, 0); assert.deepEqual(Buffer.from(events[0].image), png);
      } else assertZip(response);
    }
    assert.equal(f.generations(), 1); assert.equal(f.jobs().length, 1);
    const originalCounts = counts(), seed = f.upstreamCalls[0].body.parameters.seed;
    assert.ok(Number.isInteger(seed) && seed >= 0); assert.equal(f.jobs()[0].request.seed, seed);
    for (const [accept, asJson] of [
      ['application/json', true], ['application/zip;q=0.2, application/json; charset=utf-8;q=0.9', true],
      ['application/json;q=0.2, application/zip;q=0.9', false], ['*/*', false]
    ]) {
      const response = await f.request('/v1/ai/generate-image', { body: aliasBody, headers: { accept, 'idempotency-key': 'native-alias-replay' } });
      assert.equal(response.headers['x-nai-request-id'], nativeId); assert.equal(response.headers['x-nai-image-count'], '1');
      assert.equal(response.headers['x-nai-partial'], 'false'); assert.match(response.headers.vary, /Accept/i);
      if (asJson) assert.deepEqual(json(response, 201).images, [{ image: png.toString('base64'), index: 0, seed }]);
      else assertZip(response);
    }
    assert.deepEqual(counts(), originalCounts); assert.equal(f.generations(), 1);

    const input = { ...nativeBody('synthetic multipart reference'), model: 'nai-diffusion-3' };
    Object.assign(input.parameters, { reference_image_multiple: ['ref_multiple_0'], reference_strength_multiple: [0.6], reference_information_extracted_multiple: [1] });
    const boundary = 'synthetic-nuxt-native-boundary';
    const prefix = Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="request"; filename="request.json"\r\nContent-Type: application/json\r\n\r\n${JSON.stringify(input)}`);
    const suffix = Buffer.concat([Buffer.from(`\r\n--${boundary}\r\nContent-Disposition: form-data; name="ref_multiple_0"; filename="reference.png"\r\nContent-Type: image/png\r\n\r\n`), referencePng, Buffer.from(`\r\n--${boundary}--\r\n`)]);
    const exactLimit = 32 * 1024 * 1024;
    const body = Buffer.concat([prefix, Buffer.alloc(exactLimit - prefix.length - suffix.length, 32), suffix]);
    assert.equal(body.length, exactLimit);
    const headers = { 'content-type': `multipart/form-data; boundary=${boundary}`, 'idempotency-key': 'multipart-size-boundary' };
    for (const framing of [{ 'content-length': String(body.length) }, { 'transfer-encoding': 'chunked' }]) {
      assertZip(await f.request('/v1/ai/generate-image', { body, headers: { ...headers, ...framing }, timeout: 30000 }));
    }
    assert.equal(f.generations(), 2); assert.equal(f.upstreamCalls.at(-1).body.parameters.reference_image_multiple[0], referencePng.toString('base64'));
    const beforeRejected = counts();
    const oversize = Buffer.concat([body, Buffer.from(' ')]);
    for (const options of [
      // An early-rejected incomplete upload cannot reuse its socket for another HTTP request.
      { body: Buffer.from('{}'), agent: false, headers: { ...headers, 'content-length': String(exactLimit + 1) } },
      { body: oversize, headers: { ...headers, 'content-length': String(exactLimit + 1) } },
      { body: oversize, headers: { ...headers, 'transfer-encoding': 'chunked' } }
    ]) {
      const rejected = await f.request('/ai/generate-image', options); assert.equal(rejected.status, 413,
        JSON.stringify({ framing: options.headers['transfer-encoding'] || 'content-length', headers: rejected.headers, body: rejected.body.toString().slice(0, 300) }));
    }
    assert.deepEqual(counts(), beforeRejected); assert.equal(f.generations(), 2);

    let vibeId;
    for (const route of ['/ai/encode-vibe', '/v1/ai/encode-vibe']) {
      const response = await f.request(route, { body: { model: 'nai-diffusion-4-5-full', image: png.toString('base64'), information_extracted: 1 },
        headers: { 'idempotency-key': 'opaque-vibe-replay' } });
      assert.equal(response.status, 201); assert.match(response.headers['content-type'], /application\/binary/); assert.deepEqual(response.body, vibe);
      vibeId ||= response.headers['x-nai-request-id']; assert.equal(response.headers['x-nai-request-id'], vibeId);
    }
    const vibeJob = f.jobs().find(job => job.requestId === vibeId); assert.ok(vibeJob);
    const claim = await f.request(`/api/jobs/${vibeJob.id}/content`); assert.equal(claim.status, 200); assert.deepEqual(claim.body, vibe);
    assert.match(claim.headers['content-type'], /application\/octet-stream/);
    assert.equal((await f.request(`/api/jobs/${vibeJob.id}/content`, { member: 1 })).status, 403);
    for (const route of ['/ai/upscale', '/v1/ai/upscale']) {
      assertZip(await f.request(route, { body: { image: png.toString('base64') }, headers: { 'idempotency-key': 'upscale-alias-replay' } }), 200);
    }
    const beforeUpscaleReplay = counts();
    const upscaled = await f.request('/v1/ai/upscale', { body: { image: png.toString('base64') },
      headers: { accept: 'application/json; charset=utf-8', 'idempotency-key': 'upscale-alias-replay' } });
    assert.deepEqual(json(upscaled).images.map(({ image, index }) => ({ image, index })), [{ image: png.toString('base64'), index: 0 }]);
    assert.match(upscaled.headers.vary, /Accept/i); assert.deepEqual(counts(), beforeUpscaleReplay);
    assert.equal(f.generations(), 4); assert.equal(f.jobs().length, 4);
    assert.equal(f.observations.length, 0, 'stream and binary tests must bypass the buffering fault relay'); f.checkStorage();
  });

  await t.test('正式OpenAI首帧与任务预览实时到达，长于45秒仍完成，任务取消和签名图保留成员边界', { timeout: 90000 }, async sub => {
    const f = await fixture(sub, nuxtEntry, { directExecutor: true, discord: true, directTimeout: 1000 });
    const authorization = 'Bearer ' + f.members[1].api_key;
    const previews = await subscribe(sub, f.nuxtBase, undefined, { route: '/api/jobs/events', headers: { authorization } });
    const foreignPreviews = await subscribe(sub, f.nuxtBase, undefined, { route: '/api/jobs/events', headers: { authorization: 'Bearer ' + f.members[2].api_key } });
    const held = f.holdNext(true), started = Date.now();
    const streamed = await subscribe(sub, f.nuxtBase, undefined, { route: '/v1/chat/completions', headers: { authorization }, body: chatBody('synthetic slow stream', true) });
    await until(() => held.started && previews.frames.some(frame => frame.data?.preview) && streamed.frames.some(frame => frame.data?.object === 'chat.completion.chunk'),
      10000, 'OpenAI first frame and actual upstream preview must arrive before the held final image');
    const preview = previews.frames.find(frame => frame.data?.preview).data;
    assert.deepEqual(Buffer.from(preview.preview.split(',')[1], 'base64'), png);
    assert.ok(preview.progress.percent > 0 && preview.progress.percent < 100);
    assert.equal(f.jobs().find(job => job.id === preview.jobId).status, 'running');
    assert.equal(foreignPreviews.frames.some(frame => frame.data?.preview), false);
    assert.equal(streamed.ended(), false); assert.equal(f.generations(), 1);

    const queued = json(await f.request('/api/jobs', { member: 1, body: jobBody('synthetic cancellable task') }), 202);
    assert.equal(json(await f.request('/api/jobs/' + queued.id, { member: 1 })).status, 'queued');
    assert.equal(json(await f.request('/api/requests/' + queued.requestId, { member: 1 })).id, queued.requestId);
    const memberRoute = '/api/member/requests/' + queued.requestId;
    assert.equal(json(await call(f.nuxtBase, memberRoute, { headers: { cookie: f.cookie(1) } })).id, queued.requestId);
    for (const [route, options] of [
      ['/api/requests/' + queued.requestId, { member: 2 }],
      ['/api/requests/' + queued.requestId + '/cancel', { member: 2, body: {} }]
    ]) assert.equal((await f.request(route, options)).status, 404);
    assert.equal((await call(f.nuxtBase, memberRoute + '/cancel', { body: {}, headers: { cookie: f.cookie(2) } })).status, 404);
    assert.equal(json(await call(f.nuxtBase, memberRoute + '/cancel', { body: {}, headers: { cookie: f.cookie(1) } })).cancelledCount, 1);
    assert.equal(json(await f.request('/api/requests/' + queued.requestId + '/cancel', { member: 1, body: {} })).cancelledCount, 0);

    const direct = '/generate?' + new URLSearchParams({ token: f.members[1].api_key, prompt: 'synthetic original timeout image',
      artist: '', model: 'nai-diffusion-4-5-full', width: '1024', height: '1024', steps: '23', seed: '63', nocache: '1' });
    const timedOut = await call(f.nuxtBase, direct);
    assert.equal(timedOut.status, 200); assert.match(timedOut.headers['content-type'], /image\//);
    assert.equal(timedOut.headers['x-error'], '1'); assert.equal(timedOut.headers['x-timeout'], '1');
    assert.ok(timedOut.body.length); assert.ok(timedOut.headers['x-nai-request-id']);
    assert.equal(json(await f.request('/api/requests/' + timedOut.headers['x-nai-request-id'] + '/cancel', { member: 1, body: {} })).cancelledCount, 1);

    // This wait crosses the old finite proxy deadline; the upstream final frame remains explicitly blocked.
    await new Promise(resolve => setTimeout(resolve, Math.max(0, 46000 - (Date.now() - started))));
    assert.equal(streamed.ended(), false, 'long generation must not inherit a 45 second proxy deadline');
    assert.equal(streamed.failure(), undefined); assert.equal(f.jobs().find(job => job.id === preview.jobId).status, 'running');
    held.resolve();
    await until(streamed.ended, 10000, 'OpenAI stream did not deliver its final frame');
    assert.equal(streamed.failure(), undefined);
    const messages = streamed.frames.filter(frame => frame.event === 'message').map(frame => frame.data);
    assert.equal(messages.at(-1), '[DONE]');
    const chunks = messages.slice(0, -1); assert.ok(chunks.every(chunk => chunk.object === 'chat.completion.chunk'));
    assert.ok(chunks.every(chunk => chunk.id === chunks[0].id));
    assert.deepEqual(chunks.at(-1).choices, [{ index: 0, delta: {}, finish_reason: 'stop' }]);
    const markdown = chunks.map(chunk => chunk.choices[0].delta.content || '').join('');
    assert.match(markdown, /!\[image\]\(https?:\/\//);
    previews.request.destroy(); foreignPreviews.request.destroy();

    const completion = json(await f.request('/v1/chat/completions', { member: 1, body: chatBody('synthetic JSON completion') }));
    assert.equal(completion.object, 'chat.completion'); assert.equal(completion.choices[0].finish_reason, 'stop');
    const imageUrl = new URL(completion.choices[0].message.content.match(/!\[image\]\((https?:\/\/[^)]+)\)/)[1]);
    assert.equal(imageUrl.hostname, publicHost);
    const imagePath = imageUrl.pathname + imageUrl.search;
    const signed = await call(f.nuxtBase, imagePath); assert.equal(signed.status, 200); assert.deepEqual(signed.body, png);
    assert.equal((await f.request(imagePath, { member: 2 })).status, 200, 'a valid limited-lifetime share URL remains usable');
    const forged = new URL(imageUrl); forged.searchParams.set('signature', '0'.repeat(64));
    for (const pathname of [imageUrl.pathname, forged.pathname + forged.search]) {
      assert.equal((await call(f.nuxtBase, pathname)).status, 401);
      assert.equal((await f.request(pathname, { member: 2 })).status, 403);
      assert.deepEqual((await f.request(pathname, { member: 1 })).body, png);
    }
    await until(() => {
      const account = JSON.parse(f.audit.prepare('SELECT data FROM accounts LIMIT 1').get().data);
      return !account.cooldownUntil || Date.parse(account.cooldownUntil) <= Date.now();
    }, 15000, 'successful-generation cooldown did not elapse before the direct success sample');
    const success = await call(f.nuxtBase, direct.replace('synthetic+original+timeout+image', 'synthetic+direct+success'));
    assert.equal(success.status, 200); assert.deepEqual(success.body, png); assert.equal(success.headers['x-error'], undefined);
    assert.equal(f.generations(), 3); assert.equal(f.observations.length, 0); f.checkStorage();
  });

  await t.test('正式同步生图在Nuxt强杀或客户端断开后继续，同键重试领取而不再次派发', { timeout: 45000 }, async sub => {
    const f = await fixture(sub, nuxtEntry, { directExecutor: true });
    const executorPid = f.executor.child().pid;
    for (const [index, interrupt] of ['nuxt-kill', 'client-close'].entries()) {
      const input = nativeBody('synthetic lost synchronous response ' + interrupt), held = f.holdNext();
      let received = false;
      const pending = http.request(f.nuxtBase + '/ai/generate-image', { method: 'POST', headers: { host: publicHost, origin: publicOrigin,
        authorization: 'Bearer ' + f.members[0].api_key, 'content-type': 'application/json', 'idempotency-key': interrupt } }, response => { received = true; response.resume(); });
      pending.on('error', () => {}); pending.end(JSON.stringify(input)); sub.after(() => pending.destroy());
      await until(() => held.started && f.jobs().length === index + 1, 15000, 'synchronous request did not reach the held upstream');
      const accepted = f.jobs().find(job => job.status === 'running'); assert.ok(accepted?.requestId); assert.equal(received, false);
      if (interrupt === 'nuxt-kill') await f.nuxt.stop(); else pending.destroy();
      assert.equal(f.executor.child().pid, executorPid); assert.equal(f.executor.child().exitCode, null);
      held.resolve();
      await until(() => f.jobs().find(job => job.id === accepted.id)?.status === 'done', 10000, 'executor cancelled an accepted task after downstream disconnection');
      if (interrupt === 'nuxt-kill') await f.startNuxt();
      const replay = await f.request('/v1/ai/generate-image-stream', { body: input, headers: { 'idempotency-key': interrupt } });
      assert.equal(replay.status, 200); assert.equal(replay.headers['x-nai-request-id'], accepted.requestId);
      assert.deepEqual(Buffer.from(readNativeEvents(replay.body)[0].image), png);
      assert.deepEqual((await f.request(`/api/jobs/${accepted.id}/content`)).body, png);
      assert.equal(json(await f.request('/api/requests/' + accepted.requestId)).id, accepted.requestId);
      assert.equal((await f.request('/api/requests/' + accepted.requestId, { member: 1 })).status, 404);
      assert.equal(f.generations(), index + 1); assert.equal(f.jobs().length, index + 1);
      assert.equal(f.audit.prepare('SELECT COUNT(*) AS n FROM fair_quota_settlements').get().n, index + 1);
    }
    assert.equal(f.observations.length, 0); f.checkStorage();
  });

  await t.test('正式 SSE 保持同一客户端连接跨执行器重启，校准新boot后仍接收授权失效', { timeout: 45000 }, async sub => {
    const f = await fixture(sub, nuxtEntry, { discord: true });
    const stream = await subscribe(sub, f.nuxtBase, f.cookie(2));
    const invalidations = () => stream.frames.filter(frame => frame.event === 'state-invalidated');
    await until(() => invalidations().some(frame => frame.data.resync), 10000, 'initial SSE calibration missing');
    const initial = invalidations()[0].data, socket = stream.response.socket, nuxtPid = f.nuxt.child().pid;
    // Waiting for the actual upstream heartbeat catches accidental reuse of the finite 15-second deadline.
    await until(() => stream.frames.some(frame => frame.comment?.includes('keepalive')), 20000, 'SSE did not survive until the upstream heartbeat');
    assert.equal(stream.ended(), false);
    await f.executor.stop();
    await until(() => stream.frames.some(frame => frame.event === 'connection-status' && frame.data.state === 'retrying'), 5000, 'internal disconnect was not reported');
    assert.equal(stream.ended(), false); assert.equal(stream.response.socket, socket);
    await f.startExecutor();
    await until(() => invalidations().some(frame => frame.data.bootId !== initial.bootId && frame.data.resync), 12000, 'same client connection did not receive restarted executor calibration');
    assert.equal(stream.ended(), false); assert.equal(stream.response.socket, socket); assert.equal(f.nuxt.child().pid, nuxtPid);
    assert.ok(f.observations.filter(item => item.path === '/api/member/shared-events').length >= 2, 'Nuxt must reopen the internal stream');
    assert.equal(f.discordState.refreshTokens.length, 0);
    json(await f.revoke(2));
    await until(() => stream.frames.some(frame => frame.event === 'auth-expired') && stream.ended(), 10000, 'revoked member must receive auth-expired and EOF');
    assert.equal(stream.failure(), undefined); assert.equal(f.generations(), 0);
    await sub.test('Linux SIGTERM结束Nuxt桥为普通EOF，执行器继续运行', { skip: process.platform === 'win32' }, async signalTest => {
      const ownerStream = await subscribe(signalTest, f.nuxtBase, f.cookie(1));
      await until(() => ownerStream.frames.some(frame => frame.event === 'state-invalidated' && frame.data.resync), 5000, 'owner stream did not open');
      const previewStream = await subscribe(signalTest, f.nuxtBase, undefined, { route: '/api/jobs/events',
        headers: { authorization: 'Bearer ' + f.members[1].api_key } });
      await until(() => previewStream.frames.some(frame => frame.comment?.includes('connected')), 5000, 'preview stream did not open');
      const executorPid = f.executor.child().pid, started = Date.now();
      let exit;
      const stopping = f.nuxt.stop('SIGTERM').then(value => { exit = value; });
      await until(() => ownerStream.ended() && previewStream.ended() && exit, 5000, 'Nuxt SIGTERM did not close both streams and exit within five seconds');
      await stopping;
      assert.equal(ownerStream.failure(), undefined); assert.deepEqual(exit, [0, null]);
      assert.equal(previewStream.failure(), undefined); assert.equal(previewStream.frames.some(frame => frame.event === 'auth-expired'), false);
      assert.ok(Date.now() - started < 5000);
      assert.equal(ownerStream.frames.some(frame => frame.event === 'auth-expired'), false);
      assert.equal(f.executor.child().pid, executorPid); assert.equal(f.executor.child().exitCode, null); assert.equal(f.executor.child().signalCode, null);
      json(await call(f.executorBase, '/api/health', { headers: { host: 'localhost:18082', origin: 'http://localhost:18082', 'x-executor-token': f.internalToken } }));
    });
    f.checkStorage();
  });
});
