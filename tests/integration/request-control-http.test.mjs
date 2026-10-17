import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { bootstrap } from '../../scripts/bootstrap.mjs';
import { JsonStore } from '../../server/store.ts';
import { syncFairQuota } from '../../server/fair-quota.ts';

const root = fileURLToPath(new URL('../../', import.meta.url));
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
async function listen(server) { server.listen(0, '127.0.0.1'); await once(server, 'listening'); return server.address().port; }

test('请求 API 的 Bearer/Cookie 同归属查询、CSRF 取消、签名领取与受理请求 ID', { timeout: 20000 }, async t => {
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-request-api-'));
  const adminToken = 'synthetic-admin-for-request-api-test';
  const initialized = await bootstrap({ env: { ADMIN_TOKEN: adminToken, NOVELAI_TOKEN: 'pst-synthetic-offline-request-api-only', DATA_DIR: dataDir }, log() {} });
  const credentials = JSON.parse(await readFile(initialized.credentialPath, 'utf8')).accounts;
  const newKey = 'STA1N-synthetic-replacement-key';
  const store = new JsonStore(dataDir);
  await store.init();
  await store.update(db => {
    Object.assign(db.accounts[0], { quotaTier: 3, subscriptionActive: true, subscriptionExpiresAt: Date.now() + 86400000,
      quotaFixed: 10000, quotaPurchased: 0, v5UsagePercent: 100, quotaCheckedAt: new Date().toISOString(),
      cooldownUntil: new Date(Date.now() + 900000).toISOString() });
    syncFairQuota(db);
    db.users.find(user => user.id === credentials[0].user_id).token = newKey;
    db.images.push({ id: 'owned-image', token: credentials[0].api_key, mimeType: 'image/png', base64: png.toString('base64') },
      { id: 'foreign-image', token: credentials[1].api_key, mimeType: 'image/png', base64: png.toString('base64') });
    db.jobs = ['done', 'queued', 'pending'].map((status, sampleIndex) => ({ id: `old-child-${sampleIndex}`, requestId: 'owned-parent', sampleIndex,
      source: 'novelai-native', userToken: credentials[0].api_key, status, request: { model: 'nai-diffusion-4-5-full' },
      imageId: sampleIndex === 0 ? 'owned-image' : 'foreign-image', createdAt: new Date().toISOString(), queueExpiresAt: new Date(Date.now() + 600000).toISOString() }));
    db.jobs.push({ id: 'inline', userToken: newKey, status: 'done', createdAt: new Date().toISOString() });
  }, { immediate: true, generationRequest: { id: 'owned-parent', member_id: credentials[0].user_id, source: 'novelai-native', operation: 'generate',
    created_at: new Date().toISOString(), payload_bytes: 100, payload: 'not-json-must-not-be-loaded-by-query' } });
  await store.close();
  let generationCalls = 0;
  const upstream = http.createServer((req, res) => {
    if (req.url !== '/user/data') generationCalls++;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ subscription: { tier: 3, active: true, expiresAt: Math.floor(Date.now() / 1000) + 86400,
      trainingStepsLeft: { fixedTrainingStepsLeft: 10000, purchasedTrainingSteps: 0 }, usage: { percent: 100, isNegative: false, timeUntilNextPercent: 0 } } }));
  });
  const upstreamPort = await listen(upstream), reservation = http.createServer(), port = await listen(reservation);
  await new Promise(resolve => reservation.close(resolve));
  const base = `http://127.0.0.1:${port}`;
  const env = Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE'].filter(key => process.env[key]).map(key => [key, process.env[key]]));
  Object.assign(env, { ADMIN_TOKEN: adminToken, DATA_DIR: dataDir, HOST: '127.0.0.1', PORT: String(port), OPENAI_CHAT_TIMEOUT_MS: '100',
    NOVELAI_API_URL: `http://127.0.0.1:${upstreamPort}`, NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${upstreamPort}` });
  const child = spawn(process.execPath, ['server/index.js'], { cwd: root, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  const exited = once(child, 'exit');
  let output = '';
  child.stderr.on('data', chunk => { output += chunk.toString(); });
  t.after(async () => { if (child.exitCode === null && child.signalCode === null) child.kill(); await exited;
    upstream.closeAllConnections(); await new Promise(resolve => upstream.close(resolve)); });
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(output)), 15000);
    child.stdout.on('data', chunk => { output += chunk.toString(); if (chunk.toString().includes('Nai2API listening')) { clearTimeout(timer); resolve(); } });
    child.once('exit', code => { clearTimeout(timer); reject(new Error(`synthetic gateway exited ${code}: ${output}`)); });
  });
  const call = (path, { key, cookie, body, origin } = {}) => fetch(base + path, { method: body === undefined ? 'GET' : 'POST',
    headers: { ...(key ? { authorization: `Bearer ${key}` } : {}), ...(cookie ? { cookie } : {}),
      ...(origin ? { origin } : {}), ...(body === undefined ? {} : { 'content-type': 'application/json' }) },
    body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(5000) });
  const login = await call('/api/member/login', { body: { username: credentials[0].username, password: credentials[0].password }, origin: base });
  assert.equal(login.status, 200);
  const cookie = login.headers.get('set-cookie').split(';')[0];
  const bearer = await call('/api/requests/owned-parent', { key: newKey });
  const member = await call('/api/member/requests/owned-parent', { cookie });
  assert.equal(bearer.status, 200); assert.equal(member.status, 200);
  const expected = await bearer.json(), actual = await member.json();
  assert.deepEqual(actual, expected);
  assert.equal(actual.jobs[1].imageUrl, ''); assert.equal(actual.jobs[2].imageUrl, '');
  const image = await fetch(base + actual.jobs[0].imageUrl);
  assert.equal(image.status, 200); assert.deepEqual(Buffer.from(await image.arrayBuffer()), png);
  assert.equal((await call('/api/requests/owned-parent', { key: credentials[1].api_key })).status, 404);
  assert.equal((await call('/api/requests/owned-parent/cancel', { key: credentials[1].api_key, body: {} })).status, 404);
  assert.equal((await call('/api/requests/owned-parent', { key: credentials[0].api_key })).status, 401);
  for (const origin of [undefined, 'https://evil.invalid']) {
    assert.equal((await call('/api/member/requests/owned-parent/cancel', { cookie, body: {}, origin })).status, 403);
  }
  const cancelled = await call('/api/member/requests/owned-parent/cancel', { cookie, body: {}, origin: base });
  assert.equal(cancelled.status, 200);
  assert.equal((await cancelled.json()).cancelledCount, 2);
  const repeated = await call('/api/requests/owned-parent/cancel', { key: newKey, body: {} });
  assert.equal((await repeated.json()).cancelledCount, 0);
  assert.equal((await call('/api/requests/inline', { key: newKey })).status, 404);
  assert.equal((await call('/api/jobs/inline', { key: newKey })).status, 200);

  const web = await call('/api/jobs', { key: newKey, body: { prompt: 'new web request', model: 'nai-diffusion-4-5-full', seed: 42, nocache: true } });
  assert.equal(web.status, 202);
  const webJob = await web.json();
  assert.ok(webJob.requestId);
  const me = await (await call('/api/member/me', { cookie })).json();
  assert.equal(me.jobs.find(job => job.id === webJob.id).requestId, webJob.requestId);
  assert.equal((await (await call(`/api/requests/${webJob.requestId}/cancel`, { key: newKey, body: {} })).json()).cancelledCount, 1);
  for (const [endpoint, body] of [
    ['/ai/generate-image', { input: 'native request ID', model: 'nai-diffusion-4-5-full', parameters: { width: 1024, height: 1024, steps: 23, seed: 43 } }],
    ['/ai/encode-vibe', { model: 'nai-diffusion-4-5-full', image: png.toString('base64'), information_extracted: 1 }]
  ]) {
    const accepted = await call(endpoint, { key: newKey, body });
    assert.equal(accepted.status, 504);
    const id = accepted.headers.get('x-nai-request-id');
    assert.ok(id);
    assert.match(accepted.headers.get('access-control-expose-headers'), /X-Nai-Request-Id/);
    assert.equal((await call(`/api/requests/${id}`, { key: newKey })).status, 200);
    assert.equal((await (await call(`/api/requests/${id}/cancel`, { key: newKey, body: {} })).json()).cancelledCount, 1);
  }
  assert.equal(generationCalls, 0);
});
