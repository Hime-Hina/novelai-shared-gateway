import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { bootstrap } from '../../scripts/bootstrap.mjs';
import { JsonStore } from '../../server/store.ts';
import { syncFairQuota } from '../../server/fair-quota.ts';

const root = fileURLToPath(new URL('../../', import.meta.url));
const model = 'nai-diffusion-4-5-full';
async function listen(server) {
  server.listen(0, '127.0.0.1'); await once(server, 'listening'); return server.address().port;
}
async function until(predicate, timeout = 10000) {
  const deadline = Date.now() + timeout;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error('synthetic cooldown wait timed out');
    await new Promise(resolve => setTimeout(resolve, 20));
  }
}

async function fixture(t, { hold = false } = {}) {
  // 只使用新建数据库和 localhost 模拟上游；不加载 .env 或继承外部服务配置。
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-shared-cooldown-http-'));
  const adminToken = 'synthetic-admin-cooldown-http-only';
  const initialized = await bootstrap({ env: { ADMIN_TOKEN: adminToken, NOVELAI_TOKEN: 'pst-synthetic-cooldown-only', DATA_DIR: dataDir }, log() {} });
  const member = JSON.parse(await readFile(initialized.credentialPath, 'utf8')).accounts[0];
  const seeded = new JsonStore(dataDir); await seeded.init();
  try {
    await seeded.update(db => {
      Object.assign(db.accounts[0], { quotaTier: 3, subscriptionActive: true, subscriptionExpiresAt: Date.now() + 86400000,
        quotaPoints: 10000, quotaFixed: 10000, quotaPurchased: 0, v5UsagePercent: 100,
        v5UsageIsNegative: false, v5UsageTimeUntilNextPercent: 0, quotaCheckedAt: new Date().toISOString(), upstreamStatus: 'active' });
      syncFairQuota(db);
    }, { immediate: true });
  } finally { await seeded.close(); }
  const audit = new Database(path.join(dataDir, 'library.sqlite'), { readonly: true, fileMustExist: true });
  const jobs = () => audit.prepare('SELECT data FROM jobs').all().map(row => JSON.parse(row.data));
  const account = () => JSON.parse(audit.prepare('SELECT data FROM accounts').get().data);
  let release;
  const released = new Promise(resolve => { release = resolve; });
  const calls = [];
  const upstream = http.createServer(async (req, res) => {
    if (req.url === '/user/data') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ subscription: { tier: 3, active: true, expiresAt: Math.floor(Date.now() / 1000) + 86400,
        trainingStepsLeft: { fixedTrainingStepsLeft: 10000, purchasedTrainingSteps: 0 },
        usage: { percent: 100, isNegative: false, timeUntilNextPercent: 0 } } })); return;
    }
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    calls.push({ path: req.url, body: JSON.parse(Buffer.concat(chunks).toString('utf8')) });
    if (hold) await released;
    res.writeHead(429, { 'content-type': 'application/json', 'Retry-After': '300' });
    res.end(JSON.stringify({ message: 'Concurrent generation is locked' }));
  });
  const upstreamPort = await listen(upstream), reservation = http.createServer(), port = await listen(reservation);
  await new Promise(resolve => reservation.close(resolve));
  const base = `http://127.0.0.1:${port}`;
  const env = Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE'].filter(key => process.env[key]).map(key => [key, process.env[key]]));
  Object.assign(env, { ADMIN_TOKEN: adminToken, DATA_DIR: dataDir, HOST: '127.0.0.1', PORT: String(port),
    PUBLIC_BASE_URL: base, PUBLIC_HOST: 'gateway.example', OPENAI_CHAT_TIMEOUT_MS: '15000',
    NOVELAI_API_URL: `http://127.0.0.1:${upstreamPort}`, NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${upstreamPort}` });
  let child, exited, output = '';
  const record = chunk => { output = (output + chunk.toString()).slice(-16000); };
  const start = async () => {
    child = spawn(process.execPath, ['server/index.js'], { cwd: root, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    exited = once(child, 'exit'); child.stdout.on('data', record); child.stderr.on('data', record);
    await new Promise((resolve, reject) => {
      const cleanup = () => { clearTimeout(timer); child.stdout.off('data', ready); child.off('exit', failed); child.off('error', failed); };
      const ready = chunk => { if (chunk.toString().includes('Nai2API listening')) { cleanup(); resolve(); } };
      const failed = cause => { cleanup(); reject(new Error(`synthetic gateway failed ${cause}: ${output}`)); };
      const timer = setTimeout(() => failed('startup timeout'), 15000);
      child.stdout.on('data', ready); child.once('exit', failed); child.once('error', failed);
    });
  };
  const stop = async (signal = 'SIGTERM') => {
    if (child?.exitCode === null && child?.signalCode === null) child.kill(signal);
    if (exited) await exited;
  };
  t.after(async () => {
    release(); await stop(); audit.close(); upstream.closeAllConnections(); await new Promise(resolve => upstream.close(resolve));
  });
  const call = (endpoint, { body, key, headers = {} } = {}) => fetch(base + endpoint, {
    method: body === undefined ? 'GET' : 'POST', signal: AbortSignal.timeout(20000),
    headers: { authorization: `Bearer ${member.api_key}`, ...(key === undefined ? {} : { 'Idempotency-Key': key }),
      ...(body === undefined ? {} : { 'content-type': 'application/json' }), ...headers },
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  const sharedStatus = async () => {
    const login = await call('/api/member/login', { body: { username: member.username, password: member.password }, headers: { origin: base } });
    assert.equal(login.status, 200); await login.json();
    const response = await call('/api/member/shared-status', { headers: { cookie: login.headers.get('set-cookie').split(';')[0] } });
    assert.equal(response.status, 200); return response.json();
  };
  await start();
  return { start, stop, call, sharedStatus, jobs, account, calls, release,
    assertAlive() { assert.equal(child.exitCode, null); assert.equal(child.signalCode, null); assert.doesNotMatch(output, /ERR_HTTP_HEADERS_SENT/); } };
}

function assertRetryAfter(response, deadline, before, after) {
  const text = response.headers.get('retry-after');
  assert.match(text, /^\d+$/);
  const seconds = Number(text), until = Date.parse(deadline);
  assert.ok(seconds >= Math.ceil((until - after) / 1000) && seconds <= Math.ceil((until - before) / 1000),
    `Retry-After ${seconds} must describe remaining time until ${deadline}`);
  return seconds;
}

test('真实 Native 429、幂等失败重放和强制重启共用持久冷却截止时间', { timeout: 45000 }, async t => {
  const gateway = await fixture(t);
  const body = { input: 'synthetic cooldown check', model, action: 'generate',
    parameters: { width: 1024, height: 1024, steps: 23, seed: 7, n_samples: 1 } };
  const call = () => gateway.call('/ai/generate-image', { body, key: 'capacity-operation' });
  const before = Date.now(), first = await call(), after = Date.now();
  assert.equal(first.status, 429); assert.match((await first.json()).message, /并发/);
  const requestId = first.headers.get('x-nai-request-id'); assert.ok(requestId);
  const saved = gateway.account(), [job] = gateway.jobs(), deadline = saved.cooldownUntil;
  const firstRemaining = assertRetryAfter(first, deadline, before, after);
  assert.ok(firstRemaining > 120 && firstRemaining <= 300);
  assert.equal(saved.cooldownReason, 'upstream_retry_after'); assert.equal(saved.capacityStrikeCount, 1);
  assert.equal(saved.failures, 1); assert.equal(saved.inFlight, 0);
  assert.equal(job.status, 'failed'); assert.equal(job.errorStatus, 429); assert.equal(job.requestId, requestId);
  assert.equal(job.upstreamHttpStatus, 429); assert.equal(job.upstreamRetryAfterUntil, deadline);
  assert.equal(job.sharedCooldown.until, deadline); assert.equal(job.sharedCooldown.kind, 'capacity');
  assert.equal(job.fairCharge.status, 'refunded'); assert.equal(gateway.calls.length, 1);
  const checkShared = async () => {
    const status = await gateway.sharedStatus();
    assert.equal(status.status, 'cooldown'); assert.equal(status.cooldownUntil, deadline);
    assert.equal(status.cooldownReason, 'upstream_retry_after'); assert.equal(status.recentFailures, 1);
    assert.equal(status.running, 0); assert.equal(status.queued, 0); assert.equal(status.pending, 0);
  };
  await checkShared();
  // 跨过一个秒边界，证明重放返回剩余时间，而非重新开始 300 秒。
  await until(() => Date.now() >= Date.parse(deadline) - (firstRemaining - 1) * 1000);
  const replay = async () => {
    const started = Date.now(), response = await call(), ended = Date.now();
    assert.equal(response.status, 429); await response.json();
    assert.equal(response.headers.get('x-nai-request-id'), requestId);
    assert.ok(assertRetryAfter(response, deadline, started, ended) < firstRemaining);
    assert.equal(gateway.account().cooldownUntil, deadline);
    assert.equal(gateway.account().capacityStrikeCount, 1); assert.equal(gateway.account().failures, 1);
    assert.equal(gateway.jobs().length, 1); assert.deepEqual(gateway.jobs()[0], job);
    assert.equal(gateway.calls.length, 1);
  };
  await replay();
  await gateway.stop('SIGKILL'); await gateway.start();
  await checkShared(); await replay();
  gateway.assertAlive();
});

test('OpenAI 已发出 SSE 响应头后的容量失败在流内结束，网关继续服务', { timeout: 35000 }, async t => {
  const gateway = await fixture(t, { hold: true });
  const response = await gateway.call('/v1/chat/completions', { body: { model: `${model}:k_euler_ancestral`, stream: true,
    messages: [{ role: 'user', content: '提示词: synthetic SSE capacity\n画师串:\n尺寸: 方图\n提示词引导值: 5\n缩放引导值: 0\n负面提示词:' }] } });
  assert.equal(response.status, 200); assert.match(response.headers.get('content-type'), /text\/event-stream/);
  await until(() => gateway.calls.length === 1);
  assert.equal(gateway.jobs()[0].status, 'running');
  gateway.release();
  const events = (await response.text()).split('\n').filter(line => line.startsWith('data: ')).map(line => line.slice(6));
  assert.equal(events.at(-1), '[DONE]');
  const chunks = events.slice(0, -1).map(value => JSON.parse(value));
  assert.equal(chunks.at(-1).choices[0].finish_reason, 'stop');
  const content = chunks.map(chunk => chunk.choices[0].delta.content || '').join('');
  assert.match(content, /NAI 暂时限制此共享账户的并发/); assert.match(content, /<\/think>/);
  assert.equal(gateway.jobs()[0].errorStatus, 429); assert.equal(gateway.account().capacityStrikeCount, 1);
  assert.equal(gateway.calls.length, 1);
  const healthy = await gateway.call('/api/member/login-options');
  assert.equal(healthy.status, 200); await healthy.json();
  gateway.assertAlive();
});
