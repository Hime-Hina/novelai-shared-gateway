import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, rename } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { bootstrap } from '../../scripts/bootstrap.mjs';
import { JsonStore } from '../../server/store.js';
import { syncFairQuota } from '../../server/fair-quota.ts';
import { nativeImagesMsgpack } from '../../server/native-stream.ts';

const root = fileURLToPath(new URL('../../', import.meta.url));
const model = 'nai-diffusion-4-5-full';
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
async function listen(server) {
  server.listen(0, '127.0.0.1'); await once(server, 'listening'); return server.address().port;
}
async function until(predicate, timeout = 5000) {
  const deadline = Date.now() + timeout;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error('synthetic idempotency wait timed out');
    await new Promise(resolve => setTimeout(resolve, 20));
  }
}

test('HTTP 幂等跨并发、断线、重启、默认值变化和 Key 轮换保留同一任务，丢失结果不重生', { timeout: 60000 }, async t => {
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-idempotency-http-'));
  const adminToken = 'synthetic-admin-idempotency-http-only';
  const initialized = await bootstrap({ env: { ADMIN_TOKEN: adminToken, NOVELAI_TOKEN: 'pst-synthetic-idempotency-only', DATA_DIR: dataDir }, log() {} });
  const members = JSON.parse(await readFile(initialized.credentialPath, 'utf8')).accounts;
  const editOffline = async mutate => {
    const store = new JsonStore(dataDir); await store.init();
    try { await store.update(mutate, { immediate: true }); } finally { await store.close(); }
  };
  await editOffline(db => {
    Object.assign(db.accounts[0], { quotaTier: 3, subscriptionActive: true, subscriptionExpiresAt: Date.now() + 86400000,
      quotaFixed: 10000, quotaPurchased: 0, v5UsagePercent: 100, quotaCheckedAt: new Date().toISOString(),
      cooldownUntil: new Date(Date.now() + 900000).toISOString() });
    db.settings.defaultArtist = 'first default artist'; syncFairQuota(db);
  });
  const audit = new Database(path.join(dataDir, 'library.sqlite'), { readonly: true });
  t.after(() => audit.close());
  const jobs = () => audit.prepare('SELECT data FROM jobs').all().map(row => JSON.parse(row.data));
  const calls = [];
  const upstream = http.createServer(async (req, res) => {
    if (req.url === '/user/data') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ subscription: { tier: 3, active: true, expiresAt: Math.floor(Date.now() / 1000) + 86400,
        trainingStepsLeft: { fixedTrainingStepsLeft: 10000, purchasedTrainingSteps: 0 },
        usage: { percent: 100, isNegative: false, timeUntilNextPercent: 0 } } })); return;
    }
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    calls.push({ path: req.url, body: JSON.parse(Buffer.concat(chunks)) });
    res.writeHead(200, { 'content-type': req.url.endsWith('-stream') ? 'application/octet-stream' : 'image/png' });
    res.end(req.url.endsWith('-stream') ? nativeImagesMsgpack([png]) : png);
  });
  const upstreamPort = await listen(upstream), reservation = http.createServer(), port = await listen(reservation);
  await new Promise(resolve => reservation.close(resolve));
  const base = `http://127.0.0.1:${port}`;
  const env = Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE'].filter(key => process.env[key]).map(key => [key, process.env[key]]));
  Object.assign(env, { ADMIN_TOKEN: adminToken, DATA_DIR: dataDir, HOST: '127.0.0.1', PORT: String(port),
    DIRECT_GENERATE_TIMEOUT_MS: '100', OPENAI_CHAT_TIMEOUT_MS: '500',
    NOVELAI_API_URL: `http://127.0.0.1:${upstreamPort}`, NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${upstreamPort}` });
  let child, exited, output = '';
  const start = async () => {
    child = spawn(process.execPath, ['server/index.js'], { cwd: root, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    exited = once(child, 'exit'); child.stderr.on('data', chunk => { output += chunk.toString(); });
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(output)), 15000);
      child.stdout.on('data', chunk => { output += chunk.toString(); if (chunk.toString().includes('Nai2API listening')) { clearTimeout(timer); resolve(); } });
      child.once('exit', code => { clearTimeout(timer); reject(new Error(`synthetic gateway exited ${code}: ${output}`)); });
    });
  };
  const stop = async (signal = 'SIGTERM') => {
    if (child?.exitCode === null && child?.signalCode === null) child.kill(signal);
    if (exited) await exited;
  };
  t.after(async () => { await stop(); upstream.closeAllConnections(); await new Promise(resolve => upstream.close(resolve)); });
  const call = (endpoint, { body, key, member = 0, token = members[member].api_key } = {}) => fetch(base + endpoint, {
    method: body === undefined ? 'GET' : 'POST', signal: AbortSignal.timeout(5000),
    headers: { authorization: `Bearer ${token}`, ...(key === undefined ? {} : { 'Idempotency-Key': key }),
      ...(body === undefined ? {} : { 'content-type': 'application/json' }) }, body: body === undefined ? undefined : JSON.stringify(body)
  });
  const cancel = async (id, member = 0) => {
    const response = await call(`/api/requests/${id}/cancel`, { body: {}, member });
    assert.equal(response.status, 200); return response.json();
  };
  await start();
  for (const key of [undefined, 'null-input']) {
    assert.equal((await call('/ai/generate-image', { body: null, key })).status, 400);
  }
  assert.equal(jobs().length, 0);
  const webBody = { prompt: 'stable random web operation', model, width: 1024, height: 1024, steps: 23 };
  const webResponses = await Promise.all(Array.from({ length: 3 }, () => call('/api/jobs', { body: webBody, key: 'web-operation' })));
  assert.ok(webResponses.every(response => response.status === 202));
  const accepted = await Promise.all(webResponses.map(response => response.json())), first = accepted[0];
  assert.ok(accepted.every(job => job.id === first.id && job.requestId === first.requestId));
  assert.equal(jobs().length, 1);
  const alias = await call('/api/jobs', { body: webBody, key: 'web-cache-alias' });
  assert.equal(alias.status, 202); assert.equal((await alias.json()).id, first.id);
  assert.equal(jobs().length, 1);
  assert.equal((await call('/api/jobs', { body: { ...webBody, prompt: 'changed intent' }, key: 'web-operation' })).status, 409);
  const otherMember = await call('/api/jobs', { body: webBody, key: 'web-operation', member: 1 });
  assert.equal(otherMember.status, 202); const other = await otherMember.json();
  assert.notEqual(other.id, first.id); await cancel(other.requestId, 1);
  const initialPayload = JSON.parse(audit.prepare('SELECT payload FROM generation_requests WHERE id = ?').get(first.requestId).payload);
  await stop('SIGKILL');
  await editOffline(db => { db.settings.defaultArtist = 'changed default artist'; db.settings.defaults.scale = 8; });
  await start();
  const afterRestart = await call('/api/jobs', { body: webBody, key: 'web-operation' });
  assert.equal(afterRestart.status, 202); assert.equal((await afterRestart.json()).id, first.id);
  assert.deepEqual(JSON.parse(audit.prepare('SELECT payload FROM generation_requests WHERE id = ?').get(first.requestId).payload), initialPayload);
  assert.equal(jobs().length, 2);

  // Native 两种响应形态共享同一次操作；一次受理两张后，重复提交不能再占两张容量。
  const nativeBody = { input: 'native random retry', model, parameters: { width: 1024, height: 1024, steps: 23, seed: -1, n_samples: 2 } };
  const nativeFirst = await call('/ai/generate-image', { body: nativeBody, key: 'native-operation', member: 1 });
  assert.equal(nativeFirst.status, 504); const nativeId = nativeFirst.headers.get('x-nai-request-id');
  const nativeAgain = await call('/v1/ai/generate-image-stream', { body: nativeBody, key: 'native-operation', member: 1 });
  assert.equal(nativeAgain.status, 504); assert.equal(nativeAgain.headers.get('x-nai-request-id'), nativeId);
  assert.equal(jobs().filter(job => job.requestId === nativeId).length, 2);
  assert.equal((await call('/ai/generate-image', { body: { ...nativeBody, parameters: { ...nativeBody.parameters, n_samples: 3 } }, key: 'native-operation', member: 1 })).status, 409);
  assert.equal((await cancel(nativeId, 1)).cancelledCount, 2);
  const failedReplay = await call('/ai/generate-image', { body: nativeBody, key: 'native-operation', member: 1 });
  assert.equal(failedReplay.status, 409); assert.equal(failedReplay.headers.get('x-nai-request-id'), nativeId);

  for (const [operation, body] of [
    ['encode-vibe', { model, image: png.toString('base64'), information_extracted: 1 }],
    ['upscale', { image: png.toString('base64') }]
  ]) {
    const a = await call(`/ai/${operation}`, { body, key: 'same-key-different-operation', member: 1 });
    const b = await call(`/ai/${operation}`, { body, key: 'same-key-different-operation', member: 1 });
    assert.equal(a.status, 504); assert.equal(b.status, 504);
    assert.ok(a.headers.get('x-nai-request-id')); assert.equal(a.headers.get('x-nai-request-id'), b.headers.get('x-nai-request-id'));
    await cancel(a.headers.get('x-nai-request-id'), 1);
  }

  const direct = '/generate?' + new URLSearchParams({ token: members[1].api_key, prompt: 'direct retry', model, seed: '29', nocache: '1' });
  const directA = await call(direct, { key: 'direct-operation', member: 1 }), directB = await call(direct, { key: 'direct-operation', member: 1 });
  assert.equal(directA.status, 200); assert.equal(directB.status, 200);
  assert.equal(directA.headers.get('x-error'), '1'); assert.equal(directB.headers.get('x-error'), '1');
  assert.ok(directA.headers.get('x-nai-request-id')); assert.equal(directA.headers.get('x-nai-request-id'), directB.headers.get('x-nai-request-id'));
  await cancel(directA.headers.get('x-nai-request-id'), 1);

  const chat = { model: model + ':k_euler_ancestral', messages: [{ role: 'user', content: '提示词: chat retry\n画师串:\n尺寸: 方图\n提示词引导值: 5\n缩放引导值: 0\n负面提示词:' }] };
  const chatA = await call('/v1/chat/completions', { body: chat, key: 'chat-operation', member: 1 });
  const chatB = await call('/v1/chat/completions', { body: { ...chat, stream: true }, key: 'chat-operation', member: 1 });
  assert.equal(chatA.status, 504); assert.equal(chatB.status, 200);
  assert.ok(chatA.headers.get('x-nai-request-id')); assert.equal(chatA.headers.get('x-nai-request-id'), chatB.headers.get('x-nai-request-id'));
  assert.match(await chatB.text(), /data: \[DONE\]/); await cancel(chatA.headers.get('x-nai-request-id'), 1);
  assert.equal(calls.length, 0);

  // 在受理提交后直接断开连接，再用同一 key 找回；不能依赖第一次 HTTP 回包。
  const beforeDisconnect = jobs().length;
  const disconnectedBody = { ...nativeBody, input: 'accepted before connection close', parameters: { ...nativeBody.parameters, n_samples: 1 } };
  let responded = false;
  const disconnected = http.request(base + '/ai/generate-image', { method: 'POST', headers: { authorization: `Bearer ${members[1].api_key}`,
    'content-type': 'application/json', 'Idempotency-Key': 'disconnected-operation' } }, response => { responded = true; response.resume(); });
  disconnected.on('error', () => {}); disconnected.end(JSON.stringify(disconnectedBody));
  await until(() => jobs().length === beforeDisconnect + 1); disconnected.destroy(); assert.equal(responded, false);
  const recovered = await call('/ai/generate-image', { body: disconnectedBody, key: 'disconnected-operation', member: 1 });
  assert.equal(recovered.status, 504);
  assert.equal(jobs().length, beforeDisconnect + 1); await cancel(recovered.headers.get('x-nai-request-id'), 1);

  await stop('SIGKILL');
  await editOffline(db => { db.accounts[0].cooldownUntil = ''; });
  await start();
  await until(() => jobs().find(job => job.id === first.id)?.status === 'done');
  assert.equal(calls.length, 1); assert.equal(calls[0].body.parameters.seed, initialPayload.seed);
  const done = await call('/api/jobs', { body: webBody, key: 'web-operation' });
  assert.equal(done.status, 202); const result = await done.json();
  assert.equal(result.id, first.id); assert.equal(result.status, 'done');
  const image = await fetch(base + result.imageUrl);
  assert.equal(image.status, 200); assert.deepEqual(Buffer.from(await image.arrayBuffer()), png);
  const cachedBody = { ...webBody, artist: initialPayload.artist, negative: initialPayload.negative,
    size: initialPayload.size, scale: initialPayload.scale, cfg: initialPayload.cfg,
    sampler: initialPayload.sampler, noise_schedule: initialPayload.noiseSchedule };
  const cached = await call('/api/jobs', { body: cachedBody, key: 'completed-cache-operation' });
  assert.equal(cached.status, 202); const cachedJob = await cached.json();
  assert.equal(cachedJob.status, 'done'); assert.equal(cachedJob.imageId, result.imageId);
  assert.notEqual(cachedJob.id, first.id);
  const cachedAgain = await call('/api/jobs', { body: cachedBody, key: 'completed-cache-operation' });
  assert.equal(cachedAgain.status, 202); assert.equal((await cachedAgain.json()).id, cachedJob.id);
  assert.equal(calls.length, 1);
  const replacementKey = 'STA1N-synthetic-idempotency-rotated-key';
  await stop('SIGKILL');
  await editOffline(db => {
    const user = db.users.find(user => user.id === members[0].user_id); user.token = replacementKey; user.acceptingJobs = false;
    db.accounts[0].enabled = false;
  });
  await start();
  const rotated = await call('/api/jobs', { body: webBody, key: 'web-operation', token: replacementKey });
  assert.equal(rotated.status, 202); assert.equal((await rotated.json()).id, first.id);
  assert.equal(calls.length, 1);
  await stop('SIGKILL');
  const stored = JSON.parse(audit.prepare('SELECT data FROM images WHERE id = ?').get(result.imageId).data);
  const artifact = path.resolve(dataDir, stored.file);
  assert.ok(artifact.startsWith(path.join(dataDir, 'images') + path.sep));
  await rename(artifact, artifact + '.missing-for-test');
  await start();
  assert.equal((await call('/api/jobs', { body: webBody, key: 'web-operation', token: replacementKey })).status, 410);
  assert.equal(calls.length, 1);
  await stop('SIGKILL');
  await editOffline(db => { db.jobs = db.jobs.filter(job => job.id !== first.id); });
  await start();
  assert.equal((await call('/api/jobs', { body: webBody, key: 'web-operation', token: replacementKey })).status, 410);
  assert.equal(calls.length, 1);
});
