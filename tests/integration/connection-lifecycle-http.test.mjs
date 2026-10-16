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
import { JsonStore } from '../../server/store.js';
import { syncFairQuota } from '../../server/fair-quota.js';
import { nativeImagesMsgpack } from '../../server/native-stream.js';

const root = fileURLToPath(new URL('../../', import.meta.url));
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}
async function until(predicate, timeout = 100000) {
  const end = Date.now() + timeout;
  while (!predicate()) {
    if (Date.now() >= end) throw new Error('synthetic connection lifecycle wait timed out');
    await new Promise(resolve => setTimeout(resolve, 20));
  }
}
const openAiBody = (label, stream = false) => ({ model: 'nai-diffusion-4-5-full:k_euler_ancestral', stream,
  messages: [{ role: 'user', content: `提示词: ${label}\n画师串:\n尺寸: 方图\n提示词引导值: 5\n缩放引导值: 0\n负面提示词:` }] });

test('direct/OpenAI 超时与断线只结束连接，完整父请求和旧 inline 队列在强杀后继续，缓存响应不变', { timeout: 150000 }, async t => {
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-connection-http-'));
  const adminToken = 'synthetic-admin-for-connection-lifecycle';
  const initialized = await bootstrap({ env: { ADMIN_TOKEN: adminToken, NOVELAI_TOKEN: 'pst-synthetic-offline-connection-only', DATA_DIR: dataDir }, log() {} });
  const credentials = JSON.parse(await readFile(initialized.credentialPath, 'utf8')).accounts;
  const store = new JsonStore(dataDir);
  await store.init();
  await store.update(db => {
    Object.assign(db.accounts[0], { quotaTier: 3, subscriptionActive: true, subscriptionExpiresAt: Date.now() + 86400000,
      quotaFixed: 10000, quotaPurchased: 0, v5UsagePercent: 100, quotaCheckedAt: new Date().toISOString(),
      cooldownUntil: new Date(Date.now() + 30000).toISOString() });
    syncFairQuota(db);
  }, { immediate: true });
  await store.close();
  const audit = new Database(path.join(dataDir, 'library.sqlite'), { readonly: true });
  t.after(() => audit.close());
  const calls = [];
  const upstream = http.createServer(async (req, res) => {
    if (req.url === '/user/data') {
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ subscription: { tier: 3, active: true, expiresAt: Math.floor(Date.now() / 1000) + 86400,
        trainingStepsLeft: { fixedTrainingStepsLeft: 10000, purchasedTrainingSteps: 0 },
        usage: { percent: 100, isNegative: false, timeUntilNextPercent: 0 } } }));
      return;
    }
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const payload = JSON.parse(Buffer.concat(chunks));
    calls.push(payload.input);
    res.writeHead(200, { 'content-type': req.url.endsWith('-stream') ? 'application/octet-stream' : 'image/png' });
    res.end(req.url.endsWith('-stream') ? nativeImagesMsgpack([png]) : png);
  });
  const upstreamPort = await listen(upstream), reservation = http.createServer();
  const port = await listen(reservation);
  await new Promise(resolve => reservation.close(resolve));
  const base = `http://127.0.0.1:${port}`;
  const env = Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE'].filter(key => process.env[key]).map(key => [key, process.env[key]]));
  Object.assign(env, { ADMIN_TOKEN: adminToken, DATA_DIR: dataDir, HOST: '127.0.0.1', PORT: String(port),
    DIRECT_GENERATE_TIMEOUT_MS: '100', OPENAI_CHAT_TIMEOUT_MS: '100', QUEUE_MAX_PAYLOAD_BYTES: String(512 * 1024),
    NOVELAI_API_URL: `http://127.0.0.1:${upstreamPort}`, NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${upstreamPort}` });
  let child, exited, output = '';
  const start = async () => {
    child = spawn(process.execPath, ['server/index.js'], { cwd: root, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    exited = once(child, 'exit');
    child.stderr.on('data', chunk => { output += chunk.toString(); });
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('synthetic gateway startup timeout: ' + output)), 15000);
      child.stdout.on('data', chunk => { output += chunk.toString(); if (chunk.toString().includes('Nai2API listening')) { clearTimeout(timer); resolve(); } });
      child.once('exit', code => { clearTimeout(timer); reject(new Error(`synthetic gateway exited ${code}: ${output}`)); });
    });
  };
  const stop = async (signal = 'SIGTERM') => {
    if (child?.exitCode === null && child?.signalCode === null) child.kill(signal);
    if (exited) await exited;
  };
  t.after(async () => { await stop(); upstream.closeAllConnections(); await new Promise(resolve => upstream.close(resolve)); });
  const rows = () => audit.prepare('SELECT data FROM jobs').all().map(row => JSON.parse(row.data));
  const post = (body, member = 0) => fetch(base + '/v1/chat/completions', { method: 'POST', signal: AbortSignal.timeout(5000),
    headers: { authorization: `Bearer ${credentials[member].api_key}`, 'content-type': 'application/json' }, body: typeof body === 'string' ? body : JSON.stringify(body) });
  const direct = (label, seed, member = 0) => '/generate?' + new URLSearchParams({ token: credentials[member].api_key,
    prompt: label, model: 'nai-diffusion-4-5-full', width: '1024', height: '1024', steps: '23', seed: String(seed) });
  await start();

  assert.equal((await post('{')).status, 400);
  assert.equal((await post('x'.repeat(2 * 1024 * 1024 + 1))).status, 413);
  assert.equal((await post('x'.repeat(512 * 1024 + 1))).status, 429);
  // 中断未读完的 OpenAI 正文；其临时预算不能泄漏到随后的正常请求。
  const aborted = http.request(base + '/v1/chat/completions', { method: 'POST', headers: {
    authorization: `Bearer ${credentials[0].api_key}`, 'content-type': 'application/json', 'content-length': String(400000) } });
  aborted.on('error', () => {});
  aborted.write('{"messages":"' + 'x'.repeat(200000));
  await new Promise(resolve => setTimeout(resolve, 30));
  aborted.destroy();

  const cachedPath = direct('direct timeout', 501);
  const directTimeout = await fetch(base + cachedPath);
  assert.equal(directTimeout.status, 200);
  assert.match(directTimeout.headers.get('content-type'), /image\//);
  assert.equal(directTimeout.headers.get('x-error'), '1');
  assert.equal(directTimeout.headers.get('x-timeout'), '1');
  await directTimeout.arrayBuffer();
  const prompt = 'large JSON timeout ' + 'x'.repeat(64000);
  const jsonTimeout = await post(openAiBody(prompt));
  assert.equal(jsonTimeout.status, 504);
  assert.equal((await jsonTimeout.json()).error.type, 'server_error');
  const streamTimeout = await post(openAiBody('SSE timeout', true), 1);
  assert.equal(streamTimeout.status, 200);
  const events = await streamTimeout.text();
  assert.match(events, /连接超时/);
  assert.match(events, /data: \[DONE\]/);
  assert.equal(rows().length, 3);
  assert.ok(rows().every(job => job.status === 'queued' && !job.deadlineAt && job.requestId && !job.request.prompt && !job.request.tag));
  assert.ok(rows().every(job => Date.parse(job.queueExpiresAt) - Date.parse(job.createdAt) > 590000));
  const parents = audit.prepare('SELECT * FROM generation_requests').all();
  assert.equal(parents.length, 3);
  assert.ok(parents.some(parent => JSON.parse(parent.payload).tag === prompt));
  assert.ok(parents.every(parent => !parent.payload.includes(credentials[0].api_key) && !parent.payload.includes('/generate?')));
  assert.equal(parents.filter(parent => parent.source === 'openai').length, 2);

  for (const [index, endpoint, value] of [[0, direct('direct disconnected', 502, 1), null],
    [1, '/v1/chat/completions', openAiBody('JSON disconnected')], [2, '/v1/chat/completions', openAiBody('SSE disconnected', true)]]) {
    const before = rows().length;
    const pending = http.request(base + endpoint, { method: value ? 'POST' : 'GET', headers: {
      authorization: `Bearer ${credentials[2].api_key}`, 'content-type': 'application/json' } }, response => response.resume());
    pending.on('error', () => {});
    pending.end(value ? JSON.stringify(value) : undefined);
    await until(() => rows().length === before + 1, 5000);
    pending.destroy();
    assert.equal(rows().length, 4 + index);
  }
  const acceptedIds = rows().map(job => job.id);
  assert.equal(calls.length, 0);
  await stop('SIGKILL');
  const legacy = new JsonStore(dataDir);
  await legacy.init();
  await legacy.update(db => {
    db.accounts[0].cooldownUntil = '';
    for (const [index, source] of ['direct', 'openai'].entries()) {
      const request = { model: 'nai-diffusion-4-5-full', width: 1024, height: 1024, steps: 23, seed: 801 + index,
        prompt: `legacy ${source}`, tag: `legacy ${source}`, sampler: 'k_euler_ancestral' };
      db.jobs.push({ id: `legacy-${source}`, source, userToken: credentials[3].api_key, status: 'queued', request,
        queueSeq: legacy.nextQueueSequence(), queueExpiresAt: new Date(Date.now() + 600000).toISOString(), createdAt: new Date().toISOString() });
    }
  }, { immediate: true, dirtyRows: { jobs: ['legacy-direct', 'legacy-openai'], accounts: [legacy.db.accounts[0].id] } });
  await legacy.close();
  await start();
  await until(() => rows().length === 8 && rows().every(job => job.status === 'done'));
  assert.equal(calls.length, 8);
  assert.ok(acceptedIds.every(id => rows().find(job => job.id === id)?.imageId));
  assert.equal(rows().find(job => job.id === 'legacy-direct').requestId, undefined);
  assert.ok(rows().filter(job => job.id.startsWith('legacy-')).every(job => job.payloadBytes > 0));
  const hit = await fetch(base + cachedPath);
  assert.equal(hit.status, 200);
  assert.equal(hit.headers.get('x-cache'), 'hit');
  assert.deepEqual(Buffer.from(await hit.arrayBuffer()), png);
  assert.equal(calls.length, 8);
  assert.equal(rows().length, 9);
  assert.equal(rows().filter(job => job.source === 'direct' && job.status === 'done').length, 4);
});
