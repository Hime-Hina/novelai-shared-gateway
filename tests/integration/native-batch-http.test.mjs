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
const body = (seed, count) => ({ input: 'synthetic batch', model: 'nai-diffusion-4-5-full', action: 'generate',
  parameters: { width: 1024, height: 1024, steps: 23, seed, n_samples: count } });
async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}
async function until(predicate, timeout = 45000) {
  const end = Date.now() + timeout;
  while (!predicate()) {
    if (Date.now() >= end) throw new Error('synthetic state wait timed out');
    await new Promise(resolve => setTimeout(resolve, 25));
  }
}
function zipEntries(buffer) {
  const result = new Map();
  let offset = 0;
  while (buffer.readUInt32LE(offset) === 0x04034b50) {
    const size = buffer.readUInt32LE(offset + 18), nameLength = buffer.readUInt16LE(offset + 26), extraLength = buffer.readUInt16LE(offset + 28);
    const start = offset + 30 + nameLength + extraLength;
    result.set(buffer.subarray(offset + 30, offset + 30 + nameLength).toString(), buffer.subarray(start, start + size));
    offset = start + size;
  }
  return result;
}

test('真实 Native HTTP：A1→B→A2、公平激活、部分 ZIP、断线和重启不丢已受理批次', { timeout: 150000 }, async t => {
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-batch-http-'));
  const adminToken = 'synthetic-admin-token-for-offline-batch-test';
  const initialized = await bootstrap({ env: { ADMIN_TOKEN: adminToken, NOVELAI_TOKEN: 'pst-synthetic-offline-batch-fixture-only', DATA_DIR: dataDir }, log() {} });
  const credentials = JSON.parse(await readFile(initialized.credentialPath, 'utf8')).accounts;
  const store = new JsonStore(dataDir);
  await store.init();
  await store.update(db => {
    Object.assign(db.accounts[0], { quotaTier: 3, subscriptionActive: true, subscriptionExpiresAt: Date.now() + 86400000,
      quotaFixed: 10000, quotaPurchased: 0, v5UsagePercent: 100, quotaCheckedAt: new Date().toISOString() });
    syncFairQuota(db);
  }, { immediate: true });
  await store.close();
  const audit = new Database(path.join(dataDir, 'library.sqlite'), { readonly: true });
  t.after(() => audit.close());
  const calls = [], releases = new Map();
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
    const seed = payload.parameters.seed;
    calls.push(seed);
    if ([100, 300, 400].includes(seed)) await new Promise(resolve => releases.set(seed, resolve));
    if (seed === 101) {
      res.writeHead(500, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ message: 'synthetic second image failure' }));
      return;
    }
    res.writeHead(200, { 'content-type': req.url.endsWith('-stream') ? 'application/octet-stream' : 'image/png' });
    res.end(req.url.endsWith('-stream') ? nativeImagesMsgpack([png]) : png);
  });
  const upstreamPort = await listen(upstream);
  const portReservation = http.createServer();
  const port = await listen(portReservation);
  await new Promise(resolve => portReservation.close(resolve));
  const base = `http://127.0.0.1:${port}`;
  const env = Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE'].filter(key => process.env[key]).map(key => [key, process.env[key]]));
  Object.assign(env, { ADMIN_TOKEN: adminToken, DATA_DIR: dataDir, HOST: '127.0.0.1', PORT: String(port),
    NOVELAI_API_URL: `http://127.0.0.1:${upstreamPort}`, NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${upstreamPort}` });
  let child, exited, output = '';
  const start = async () => {
    child = spawn(process.execPath, ['server/index.js'], { cwd: root, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    exited = once(child, 'exit');
    child.stderr.on('data', chunk => { output += chunk.toString(); });
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('synthetic gateway startup timeout: ' + output)), 15000);
      child.stdout.on('data', chunk => {
        output += chunk.toString();
        if (chunk.toString().includes('Nai2API listening')) { clearTimeout(timer); resolve(); }
      });
      child.once('exit', code => { clearTimeout(timer); reject(new Error(`synthetic gateway exited ${code}: ${output}`)); });
    });
  };
  const stop = async (signal = 'SIGTERM') => {
    if (child?.exitCode === null && child?.signalCode === null) child.kill(signal);
    if (exited) await exited;
  };
  t.after(async () => {
    for (const release of releases.values()) release();
    await stop();
    upstream.closeAllConnections();
    await new Promise(resolve => upstream.close(resolve));
  });
  await start();
  const rows = () => audit.prepare('SELECT data FROM jobs').all().map(row => JSON.parse(row.data));
  const post = (value, member = 0) => fetch(base + '/ai/generate-image', { method: 'POST',
    headers: { authorization: `Bearer ${credentials[member].api_key}`, 'content-type': 'application/json' },
    body: JSON.stringify(value), signal: AbortSignal.timeout(90000) });

  const a = post(body(100, 4));
  await until(() => calls.includes(100));
  const first = rows().filter(job => job.request.seed === 100)[0];
  const aRows = rows().filter(job => job.requestId === first.requestId);
  assert.equal(aRows.length, 4);
  assert.equal(aRows.filter(job => job.status === 'pending').length, 3);
  assert.ok(aRows.every(job => !job.request.prompt && !job.request.nativeParameters));
  assert.equal(audit.prepare('SELECT COUNT(*) AS n FROM generation_requests').get().n, 1);
  const b = post(body(200, 1), 1);
  await until(() => rows().some(job => job.request.seed === 200 && job.status === 'queued'));
  releases.get(100)();
  const [aResponse, bResponse] = await Promise.all([a, b]);
  assert.equal(aResponse.status, 200);
  assert.equal(bResponse.status, 200);
  await bResponse.arrayBuffer();
  assert.deepEqual(calls, [100, 200, 101]);
  assert.equal(aResponse.headers.get('x-nai-partial'), 'true');
  const entries = zipEntries(Buffer.from(await aResponse.arrayBuffer()));
  assert.deepEqual(entries.get('image_0.png'), png);
  const manifest = JSON.parse(entries.get('results.json'));
  assert.equal(manifest.completed, 1);
  assert.equal(manifest.remaining, 3);
  const ended = rows().filter(job => job.requestId === first.requestId).sort((a, b) => a.sampleIndex - b.sampleIndex);
  assert.deepEqual(ended.map(job => job.status), ['done', 'failed', 'failed', 'failed']);
  assert.ok(ended.slice(2).every(job => !job.fairCharge && !job.generationAttempt));
  assert.equal(new Set(ended.map(job => job.queueExpiresAt)).size, 1);
  assert.ok(ended[1].queueSeq > rows().find(job => job.request.seed === 200).queueSeq);

  // 断开同步 HTTP 连接后，已受理的两张仍继续；首张完成后停进程，重启恢复已排队的第二张。
  const disconnected = http.request(base + '/ai/generate-image-stream', { method: 'POST', headers: {
    authorization: `Bearer ${credentials[0].api_key}`, 'content-type': 'application/json' } });
  disconnected.on('error', () => {});
  disconnected.end(JSON.stringify(body(300, 2)));
  await until(() => calls.includes(300));
  disconnected.destroy();
  releases.get(300)();
  await until(() => rows().some(job => job.request.seed === 300 && job.status === 'done'));
  const queued = rows().find(job => job.request.seed === 301);
  assert.equal(queued.status, 'queued');
  await stop('SIGKILL');
  await start();
  await until(() => rows().some(job => job.request.seed === 301 && job.status === 'done'));
  assert.equal(calls.filter(seed => seed === 300).length, 1);
  assert.equal(calls.filter(seed => seed === 301).length, 1);
  assert.equal(rows().find(job => job.id === queued.id).queueSeq, queued.queueSeq);
  assert.ok(rows().filter(job => [300, 301].includes(job.request.seed)).every(job => job.imageId));

  // 缩短的只是同步等待期限，不能把 504 当成任务取消或业务排队过期。
  await stop();
  env.OPENAI_CHAT_TIMEOUT_MS = '100';
  await start();
  const timedOut = await post(body(400, 1));
  assert.equal(timedOut.status, 504);
  await timedOut.json();
  assert.ok(['queued', 'running'].includes(rows().find(job => job.request.seed === 400).status));
  await until(() => calls.includes(400));
  releases.get(400)();
  await until(() => rows().some(job => job.request.seed === 400 && job.status === 'done'));
  assert.equal(calls.filter(seed => seed === 400).length, 1);
});
