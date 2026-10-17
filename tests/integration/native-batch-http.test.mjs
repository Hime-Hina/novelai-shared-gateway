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
import { JsonStore } from '../../server/store.ts';
import { syncFairQuota } from '../../server/fair-quota.ts';
import { nativeImagesMsgpack } from '../../server/native-stream.ts';
import { readNativeEvents, readNativeSseEvents } from '../helpers/native-stream-client.mjs';

const root = fileURLToPath(new URL('../../', import.meta.url));
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
const webp = await readFile(new URL('../fixtures/synthetic.webp', import.meta.url));
const body = (seed, count) => ({ input: 'synthetic batch', model: 'nai-diffusion-4-5-full', action: 'generate',
  parameters: { width: 1024, height: 1024, steps: 23, seed, n_samples: count } });
async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
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

test('真实 Native HTTP：公平批次、ZIP/JSON 协商、幂等冷重取与断线恢复', { timeout: 150000 }, async t => {
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
  const calls = [], upstreamEvents = [], releases = new Map();
  const upstreamEvent = (seed, event, details = {}) => {
    upstreamEvents.push({ seed, event, at: new Date().toISOString(), ...details });
    if (upstreamEvents.length > 40) upstreamEvents.shift();
  };
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
    upstreamEvent(seed, 'received', { imageFormat: payload.parameters.image_format,
      redundantPrompt: Object.hasOwn(payload.parameters, 'prompt'), originalInput: payload.input === '  synthetic batch  ' });
    res.once('close', () => upstreamEvent(seed, 'closed', { finished: res.writableFinished }));
    if ([100, 300, 400].includes(seed)) {
      await new Promise(resolve => releases.set(seed, resolve));
      upstreamEvent(seed, 'released');
    }
    if (seed === 101) {
      upstreamEvent(seed, 'response', { status: 500 });
      res.writeHead(500, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ message: 'synthetic second image failure' }));
      return;
    }
    upstreamEvent(seed, 'response', { status: 200 });
    const image = payload.parameters.image_format === 'webp' ? webp : png;
    res.writeHead(200, { 'content-type': req.url.endsWith('-stream') ? 'application/octet-stream' : image === webp ? 'image/webp' : 'image/png' });
    res.end(req.url.endsWith('-stream') ? nativeImagesMsgpack([image]) : image);
  });
  const upstreamPort = await listen(upstream);
  const portReservation = http.createServer();
  const port = await listen(portReservation);
  await new Promise(resolve => portReservation.close(resolve));
  const base = `http://127.0.0.1:${port}`;
  const env = Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE'].filter(key => process.env[key]).map(key => [key, process.env[key]]));
  Object.assign(env, { ADMIN_TOKEN: adminToken, DATA_DIR: dataDir, HOST: '127.0.0.1', PORT: String(port),
    NOVELAI_API_URL: `http://127.0.0.1:${upstreamPort}`, NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${upstreamPort}` });
  let child, exited, output = '', fetchFailure;
  const rows = () => audit.prepare('SELECT data FROM jobs').all().map(row => JSON.parse(row.data));
  const redact = value => {
    let text = String(value);
    for (const secret of [adminToken, 'pst-synthetic-offline-batch-fixture-only', ...credentials.flatMap(member => [member.api_key, member.password])]) {
      if (secret) text = text.replaceAll(secret, '[redacted]');
    }
    return text.replace(/\b(?:STA1N-|pst-)[A-Za-z0-9_-]+/g, '[redacted]');
  };
  const diagnosticError = (phase, error) => {
    let state;
    try {
      state = {
        at: new Date().toISOString(), phase, calls, upstreamEvents,
        child: { exitCode: child?.exitCode, signalCode: child?.signalCode },
        fetchFailure: fetchFailure && { seed: fetchFailure.seed, at: fetchFailure.at },
        jobs: rows().map(job => Object.fromEntries([
          'id', 'requestId', 'sampleIndex', 'status', 'queueSeq', 'createdAt', 'startedAt', 'updatedAt', 'completedAt',
          'queueExpiresAt', 'deadlineAt', 'upstreamStartedAt', 'upstreamHeadersAt', 'upstreamCompletedAt', 'upstreamHttpStatus',
          'errorStatus', 'sharedCooldown', 'imageId'
        ].map(key => [key, job[key]]).concat([['seed', job.request?.seed], ['chargeStatus', job.fairCharge?.status],
          ['resultPending', Boolean(job.resultPending)], ['error', String(job.error || '').slice(0, 300)]]))),
        accounts: audit.prepare('SELECT data FROM accounts').all().map(row => {
          const account = JSON.parse(row.data);
          return Object.fromEntries(['id', 'enabled', 'inFlight', 'cooldownUntil', 'cooldownReason', 'ordinaryStrikeCount',
            'capacityStrikeCount', 'quotaCheckedAt', 'upstreamStatus'].map(key => [key, account[key]]));
        })
      };
    } catch (readError) { state = { phase, readError: String(readError), calls, upstreamEvents }; }
    const failure = { name: error?.name, message: error?.message || String(error), code: error?.code,
      cause: error?.cause && { name: error.cause.name, message: error.cause.message, code: error.cause.code } };
    return new Error(redact(`[native-batch-state] ${JSON.stringify({ failure, state })}\nchild tail:\n${output.slice(-10000)}`), { cause: error });
  };
  const checkWaitingState = () => {
    if (fetchFailure) throw fetchFailure.error;
    if (child?.exitCode !== null || child?.signalCode !== null) throw new Error('synthetic gateway exited while waiting');
    // seed 101 的模拟失败及其未派发余图是本例预期；其他失败应立即保留现场。
    const failed = rows().find(job => job.status === 'failed' && ![101, 102, 103].includes(job.request?.seed));
    if (failed) throw new Error(`unexpected terminal job: ${failed.id}`);
  };
  const until = async (predicate, phase, timeout = 45000) => {
    const end = Date.now() + timeout;
    try {
      while (!predicate()) {
        checkWaitingState();
        if (Date.now() >= end) throw new Error('synthetic state wait timed out');
        await new Promise(resolve => setTimeout(resolve, 25));
      }
    } catch (error) { throw diagnosticError(phase, error); }
  };
  const observed = async (promise, phase) => {
    let timer;
    try {
      return await Promise.race([promise, new Promise((_, reject) => {
        timer = setInterval(() => {
          try { checkWaitingState(); } catch (error) { reject(error); }
        }, 100);
      })]);
    } catch (error) { throw diagnosticError(phase, error); }
    finally { clearInterval(timer); }
  };
  const start = async () => {
    child = spawn(process.execPath, ['server/index.js'], { cwd: root, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    exited = once(child, 'exit');
    child.stderr.on('data', chunk => { output = (output + chunk.toString()).slice(-16000); });
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(diagnosticError('startup', new Error('synthetic gateway startup timeout'))), 15000);
      child.stdout.on('data', chunk => {
        output = (output + chunk.toString()).slice(-16000);
        if (chunk.toString().includes('Nai2API listening')) { clearTimeout(timer); resolve(); }
      });
      child.once('exit', code => { clearTimeout(timer); reject(diagnosticError('startup', new Error(`synthetic gateway exited ${code}`))); });
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
  const post = (value, member = 0, { accept, key, route = '/ai/generate-image' } = {}) => {
    const pending = fetch(base + route, { method: 'POST',
      headers: { authorization: `Bearer ${credentials[member].api_key}`, 'content-type': 'application/json',
        ...(accept ? { accept } : {}), ...(key ? { 'Idempotency-Key': key } : {}) },
      body: JSON.stringify(value), signal: AbortSignal.timeout(90000) });
    // A 的响应在 B 入队后才一起等待，提前失败也不能成为无现场的未处理拒绝。
    pending.catch(error => { fetchFailure = { seed: value.parameters.seed, at: new Date().toISOString(), error }; });
    return pending;
  };

  const rejected = await post(body(50, 1), 0, { accept: 'text/html', key: 'rejected-format' });
  assert.equal(rejected.status, 406); await rejected.json();
  assert.equal(rows().length, 0); assert.equal(calls.length, 0);
  assert.equal(audit.prepare('SELECT COUNT(*) AS n FROM generation_requests').get().n, 0);
  const invalidFormat = body(50, 1); invalidFormat.parameters.image_format = 'avif';
  const invalidResponse = await post(invalidFormat); assert.equal(invalidResponse.status, 422); await invalidResponse.json();
  assert.equal(rows().length, 0); assert.equal(calls.length, 0);
  const beforeInvalidPrompt = ['jobs', 'generation_requests', 'generation_idempotency', 'fair_quota_settlements', 'ledger']
    .map(table => audit.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n);
  for (const prompt of ['synthetic-conflicting-private-prompt', null, 42]) {
    const invalidPrompt = body(50, 1); invalidPrompt.parameters.prompt = prompt;
    const response = await post(invalidPrompt, 0, { key: 'rejected-redundant-prompt' });
    assert.equal(response.status, 422); assert.doesNotMatch(await response.text(), /synthetic-conflicting-private-prompt|synthetic batch/);
  }
  assert.deepEqual(['jobs', 'generation_requests', 'generation_idempotency', 'fair_quota_settlements', 'ledger']
    .map(table => audit.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n), beforeInvalidPrompt);
  assert.equal(calls.length, 0);

  const aBody = body(100, 4), bBody = body(4294967295, 2);
  bBody.input = '  synthetic batch  ';
  Object.assign(bBody.parameters, { image_format: 'webp', prompt: bBody.input });
  const aKey = 'partial-batch-format-replay', bKey = 'complete-batch-format-replay';
  const a = post(aBody, 0, { key: aKey });
  await until(() => calls.includes(100), 'A1 reaches mock upstream');
  const first = rows().filter(job => job.request.seed === 100)[0];
  const aRows = rows().filter(job => job.requestId === first.requestId);
  assert.equal(aRows.length, 4);
  assert.equal(aRows.filter(job => job.status === 'pending').length, 3);
  assert.ok(aRows.every(job => !job.request.prompt && !job.request.nativeParameters));
  assert.equal(audit.prepare('SELECT COUNT(*) AS n FROM generation_requests').get().n, 1);
  const b = post(bBody, 1, { accept: 'application/json', key: bKey });
  await until(() => rows().some(job => job.request.seed === 4294967295 && job.status === 'queued'), 'B queued behind A1');
  releases.get(100)();
  const [aResponse, bResponse] = await observed(Promise.all([a, b]), 'A/B synchronous responses');
  assert.equal(aResponse.status, 200);
  assert.equal(bResponse.status, 200); assert.match(bResponse.headers.get('content-type'), /application\/json/);
  const bResult = await observed(bResponse.json(), 'B JSON response body');
  assert.deepEqual(bResult.images, [{ image: webp.toString('base64'), index: 0, seed: 4294967295 }, { image: webp.toString('base64'), index: 1, seed: 0 }]);
  assert.equal(bResult.requested, 2); assert.equal(bResult.completed, 2); assert.equal(bResult.remaining, 0);
  // A1 → B1 → A2 仍先于 B2，不能让任一多图批次独占队列。
  assert.deepEqual(calls, [100, 4294967295, 101, 0]);
  const bUpstream = upstreamEvents.filter(event => event.event === 'received' && [4294967295, 0].includes(event.seed));
  assert.deepEqual(bUpstream.map(event => event.imageFormat), ['webp', 'webp']);
  assert.ok(bUpstream.every(event => event.redundantPrompt === false && event.originalInput === true));
  assert.equal(aResponse.headers.get('x-nai-partial'), 'true');
  const entries = zipEntries(Buffer.from(await observed(aResponse.arrayBuffer(), 'A partial ZIP body')));
  assert.deepEqual(entries.get('image_0.png'), png);
  const manifest = JSON.parse(entries.get('results.json'));
  assert.equal(manifest.completed, 1);
  assert.equal(manifest.remaining, 3);
  const ended = rows().filter(job => job.requestId === first.requestId).sort((a, b) => a.sampleIndex - b.sampleIndex);
  assert.deepEqual(ended.map(job => job.status), ['done', 'failed', 'failed', 'failed']);
  assert.ok(ended.slice(2).every(job => !job.fairCharge && !job.generationAttempt));
  assert.equal(new Set(ended.map(job => job.queueExpiresAt)).size, 1);
  assert.ok(ended[1].queueSeq > rows().find(job => job.request.seed === 4294967295).queueSeq);
  const partialReplay = await post(aBody, 0, { accept: 'application/json', key: aKey });
  assert.equal(partialReplay.status, 200); assert.equal(partialReplay.headers.get('x-nai-partial'), 'true');
  const { images: partialImages, ...partialSummary } = await partialReplay.json();
  assert.deepEqual(partialImages, [{ image: png.toString('base64'), index: 0, seed: 100 }]);
  assert.deepEqual(partialSummary, manifest);
  assert.equal(calls.length, 4); assert.equal(rows().length, 6);
  const bId = bResponse.headers.get('x-nai-request-id');
  const persistedB = JSON.parse(audit.prepare('SELECT payload FROM generation_requests WHERE id = ?').get(bId).payload);
  assert.equal(persistedB.prompt, bBody.input); assert.equal(Object.hasOwn(persistedB.nativeParameters, 'prompt'), false);
  const bRows = rows().filter(job => job.requestId === bId).sort((a, b) => a.sampleIndex - b.sampleIndex);
  const bSettlements = () => audit.prepare('SELECT job_id, charge FROM fair_quota_settlements WHERE job_id IN (?, ?) ORDER BY job_id').all(...bRows.map(job => job.id));
  const settledB = bSettlements();
  assert.equal(settledB.length, 2); assert.ok(settledB.every(row => JSON.parse(row.charge).status === 'settled'));

  // 断开同步 HTTP 连接后，已受理的两张仍继续；首张完成后停进程，重启恢复已排队的第二张。
  const disconnected = http.request(base + '/ai/generate-image-stream', { method: 'POST', headers: {
    authorization: `Bearer ${credentials[0].api_key}`, 'content-type': 'application/json' } });
  disconnected.on('error', () => {});
  disconnected.end(JSON.stringify(body(300, 2)));
  await until(() => calls.includes(300), 'disconnected batch starts');
  disconnected.destroy();
  releases.get(300)();
  await until(() => rows().some(job => job.request.seed === 300 && job.status === 'done'), 'disconnected first image completes');
  const queued = rows().find(job => job.request.seed === 301);
  assert.equal(queued.status, 'queued');
  await stop('SIGKILL');
  env.RUNTIME_JOBS_CACHE_LIMIT = '0'; env.RUNTIME_IMAGES_CACHE_LIMIT = '0';
  await start();
  await until(() => rows().some(job => job.request.seed === 301 && job.status === 'done'), 'queued second image resumes');
  assert.equal(calls.filter(seed => seed === 300).length, 1);
  assert.equal(calls.filter(seed => seed === 301).length, 1);
  assert.equal(rows().find(job => job.id === queued.id).queueSeq, queued.queueSeq);
  assert.ok(rows().filter(job => [300, 301].includes(job.request.seed)).every(job => job.imageId));

  // 已完成任务/图片未装入重启热缓存；两种格式均从 SQLite 和原文件重新领取。
  const beforeReplay = calls.length;
  const replayBody = { ...bBody, parameters: { ...bBody.parameters } }; delete replayBody.parameters.prompt;
  const coldZip = await post(replayBody, 1, { accept: 'application/zip', key: bKey });
  assert.equal(coldZip.status, 200); assert.equal(coldZip.headers.get('x-nai-request-id'), bId);
  const coldEntries = zipEntries(Buffer.from(await coldZip.arrayBuffer()));
  assert.deepEqual(coldEntries.get('image_0.webp'), webp); assert.deepEqual(coldEntries.get('image_1.webp'), webp);
  const { images: _images, ...bSummary } = bResult;
  assert.deepEqual(JSON.parse(coldEntries.get('results.json')), bSummary);
  // 真实客户端在收到 JSON 响应头后断开，不取消已完成任务或退还已结算额度。
  await new Promise((resolve, reject) => {
    const req = http.request(base + '/ai/generate-image', { method: 'POST', headers: {
      authorization: `Bearer ${credentials[1].api_key}`, 'content-type': 'application/json',
      accept: 'application/json', 'Idempotency-Key': bKey } }, response => {
      try { assert.equal(response.statusCode, 200); response.destroy(); resolve(); } catch (error) { reject(error); }
    });
    req.on('error', reject); req.setTimeout(5000, () => req.destroy(new Error('synthetic JSON replay timeout')));
    req.end(JSON.stringify(replayBody));
  });
  const coldJson = await post(replayBody, 1, { accept: 'application/json', key: bKey });
  assert.equal(coldJson.status, 200); assert.deepEqual(await coldJson.json(), bResult);
  const coldStream = await post({ ...replayBody, parameters: { ...replayBody.parameters, stream: 'msgpack' } }, 1,
    { key: bKey, route: '/v1/ai/generate-image-stream', accept: 'text/event-stream' });
  assert.equal(coldStream.status, 200); assert.match(coldStream.headers.get('content-type'), /^application\/msgpack(?:;|$)/);
  const coldFrames = readNativeEvents(Buffer.from(await coldStream.arrayBuffer()));
  assert.deepEqual(coldFrames.map(frame => [frame.event_type, frame.samp_ix]), [['final', 0], ['final', 1]]);
  for (const frame of coldFrames) assert.deepEqual(Buffer.from(frame.image), webp);
  for (const [stream, accept] of [['sse', 'text/event-stream'], [undefined, 'application/msgpack']]) {
    const response = await post({ ...replayBody, parameters: { ...replayBody.parameters, ...(stream ? { stream } : {}) } }, 1,
      { key: bKey, route: '/ai/generate-image-stream', accept });
    assert.equal(response.status, 200); assert.match(response.headers.get('content-type'), /^text\/event-stream(?:;|$)/);
    assert.equal(response.headers.get('x-nai-request-id'), bId);
    const frames = readNativeSseEvents(Buffer.from(await response.arrayBuffer()));
    assert.deepEqual(frames.map(frame => [frame.event_type, frame.samp_ix]), [['final', 0], ['final', 1]]);
    for (const frame of frames) assert.deepEqual(Buffer.from(frame.image, 'base64'), webp);
  }
  for (const job of bRows) {
    const headers = { authorization: `Bearer ${credentials[1].api_key}` };
    const snapshot = await (await fetch(base + '/api/jobs/' + job.id, { headers })).json();
    for (const [route, options] of [[snapshot.imageUrl, {}], ['/api/jobs/' + job.id + '/content', { headers }]]) {
      const image = await fetch(new URL(route, base), options); assert.equal(image.status, 200);
      assert.match(image.headers.get('content-type'), /image\/webp/); assert.deepEqual(Buffer.from(await image.arrayBuffer()), webp);
    }
  }
  const beforeConflict = ['jobs', 'generation_requests', 'generation_idempotency', 'fair_quota_settlements', 'ledger']
    .map(table => audit.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n);
  const conflict = await post({ ...bBody, parameters: { ...bBody.parameters, image_format: 'png' } }, 1, { key: bKey });
  assert.equal(conflict.status, 409); await conflict.json();
  assert.deepEqual(['jobs', 'generation_requests', 'generation_idempotency', 'fair_quota_settlements', 'ledger']
    .map(table => audit.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n), beforeConflict);
  assert.equal(calls.length, beforeReplay); assert.deepEqual(bSettlements(), settledB);

  // 缩短的只是同步等待期限，不能把 504 当成任务取消或业务排队过期。
  await stop();
  env.OPENAI_CHAT_TIMEOUT_MS = '100';
  await start();
  const timedOut = await observed(post(body(400, 1)), 'HTTP deadline response');
  assert.equal(timedOut.status, 504);
  await timedOut.json();
  assert.ok(['queued', 'running'].includes(rows().find(job => job.request.seed === 400).status));
  await until(() => calls.includes(400), 'accepted job starts after HTTP deadline');
  releases.get(400)();
  await until(() => rows().some(job => job.request.seed === 400 && job.status === 'done'), 'accepted job completes after HTTP deadline');
  assert.equal(calls.filter(seed => seed === 400).length, 1);

  const stored = JSON.parse(audit.prepare('SELECT data FROM images WHERE id = ?').get(bRows[0].imageId).data);
  assert.equal(stored.mimeType, 'image/webp'); assert.match(stored.file, /\.webp$/);
  const artifact = path.resolve(dataDir, stored.file);
  assert.ok(artifact.startsWith(path.join(dataDir, 'images') + path.sep));
  await rename(artifact, artifact + '.missing-for-test');
  const beforeMissing = calls.length, jobCount = rows().length;
  const unavailable = await post(bBody, 1, { accept: 'application/json', key: bKey });
  assert.equal(unavailable.status, 410); await unavailable.json();
  assert.equal(calls.length, beforeMissing); assert.equal(rows().length, jobCount);
  assert.deepEqual(bSettlements(), settledB);
});
