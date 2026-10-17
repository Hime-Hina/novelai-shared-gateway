import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes } from 'node:crypto';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import Database from 'better-sqlite3';
import { bootstrap } from '../../scripts/bootstrap.mjs';
import { JsonStore } from '../../server/store.js';
import { syncFairQuota, quoteFairRequest } from '../../server/fair-quota.js';
import { parseNativeImageRequest } from '../../server/native-image.js';
import { nativeImageMsgpack, nativeImagesMsgpack } from '../../server/native-stream.js';
import { readNativeEvents } from '../helpers/native-stream-client.mjs';

const root = fileURLToPath(new URL('../../', import.meta.url));
// 合成图片和新建临时数据库；测试不会读取 .env、生产数据或请求真实 NAI。
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
const body = (model = 'nai-diffusion-5-full', extra = {}) => ({
  input: 'a small blue paper boat', model, action: 'generate',
  parameters: { width: 1024, height: 1024, steps: 23, n_samples: 1, seed: 7, ...extra }
});
const near = (actual, expected) => assert.ok(Math.abs(actual - expected) < 1e-8, `${actual} != ${expected}`);

async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}

async function* streamEvents(response) {
  const reader = response.body.getReader();
  let pending = Buffer.alloc(0);
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) { assert.equal(pending.length, 0, '结束时不能遗留半帧'); return; }
      pending = Buffer.concat([pending, Buffer.from(value)]);
      while (pending.length >= 4) {
        const length = pending.readUInt32BE(0);
        assert.ok(length > 0 && length <= 64 * 1024 * 1024, '无效帧长度');
        if (pending.length < length + 4) break;
        const [event] = readNativeEvents(pending.subarray(0, length + 4));
        pending = pending.subarray(length + 4);
        yield event;
      }
    }
  } finally { await reader.cancel(); reader.releaseLock(); }
}

test('独立公开发行版从初始化到真实 HTTP 返回、扣费、退款与重启', { timeout: 240000 }, async t => {
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-gateway-compat-'));
  const adminToken = randomBytes(32).toString('hex');
  const upstreamToken = 'pst-synthetic-offline-fixture-only';
  const initialized = await bootstrap({ env: { ADMIN_TOKEN: adminToken, NOVELAI_TOKEN: upstreamToken, DATA_DIR: dataDir }, log() {} });
  const credentials = JSON.parse(await readFile(initialized.credentialPath, 'utf8')).accounts;
  const seeded = new JsonStore(dataDir);
  await seeded.init();
  try {
    await seeded.update(db => {
      Object.assign(db.accounts[0], { quotaTier: 3, subscriptionActive: true, subscriptionExpiresAt: Date.now() + 86400000,
        quotaPoints: 10000, quotaFixed: 10000, quotaPurchased: 0, v5UsagePercent: 100,
        v5UsageIsNegative: false, v5UsageTimeUntilNextPercent: 0, quotaCheckedAt: new Date().toISOString(), upstreamStatus: 'active' });
      syncFairQuota(db);
      db.images.push({ id: 'img-access-owner', token: credentials[0].api_key, mimeType: 'image/png', base64: png.toString('base64') });
      db.jobs.push({ id: 'job-access-owner', userToken: credentials[0].api_key, status: 'done', imageId: 'img-access-owner', createdAt: new Date().toISOString() });
    }, { collections: ['accounts', 'settings', 'images', 'jobs'], includeSettings: true, immediate: true });
  } finally { await seeded.close(); }

  const calls = [];
  let scenario = 'initial HTTP compatibility checks';
  let failAt = -1, failStatus = 500, officialFixed = 10000, officialV5 = 100;
  let activeGenerations = 0, maxActiveGenerations = 0, holdNextMs = 0;
  let malformedNext = false;
  const previewGates = new Map();
  const previewGate = (seed, step, disconnect = false) => {
    let release;
    const gate = { step, disconnect, written: false, released: false, wait: new Promise(resolve => { release = resolve; }) };
    gate.release = () => { gate.released = true; release(); };
    previewGates.set(seed, gate);
    return gate;
  };
  const auditReader = new Database(path.join(dataDir, 'library.sqlite'), { readonly: true, fileMustExist: true });
  t.after(() => auditReader.close());
  const dispatchEvidence = [];
  const upstream = http.createServer(async (req, res) => {
    assert.equal(req.headers.authorization, `Bearer ${upstreamToken}`);
    if (req.url === '/user/data') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ subscription: { tier: 3, active: true, expiresAt: Math.floor(Date.now() / 1000) + 86400,
        trainingStepsLeft: { fixedTrainingStepsLeft: officialFixed, purchasedTrainingSteps: 0 },
        usage: { percent: officialV5, isNegative: false, timeUntilNextPercent: 0 } } }));
      return;
    }
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const payload = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    // 真正收到上游调用时就检查派发证据已提交，不能只在失败后补写一个看似完整的历史。
    const running = auditReader.prepare("SELECT data FROM jobs WHERE status = 'running'").all().map(row => JSON.parse(row.data));
    dispatchEvidence.push(running.map(job => ({ queueSeq: job.queueSeq, attempt: job.generationAttempt,
      reservation: JSON.parse(auditReader.prepare("SELECT value FROM app_settings WHERE key = 'settings'").get().value).fairQuota.reservations[job.id],
      ledger: auditReader.prepare('SELECT data FROM ledger WHERE id = ?').get(`${job.generationAttempt?.id}:generation_reserved`)?.data
    })));
    const call = { pathname: req.url, payload, startedAt: Date.now(), scenario, failAtOnReceive: failAt }; calls.push(call);
    activeGenerations++; maxActiveGenerations = Math.max(maxActiveGenerations, activeGenerations);
    let counted = true;
    const inactive = () => { if (counted) { activeGenerations--; counted = false; } };
    res.once('finish', () => { inactive(); call.finishedAt = Date.now(); });
    res.once('close', () => { inactive(); call.closedAt = Date.now(); call.responseFinished = res.writableFinished; });
    if (activeGenerations > 1) {
      call.responseStatus = 429;
      res.writeHead(429, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ message: 'Concurrent generation is locked' }));
      return;
    }
    const delay = holdNextMs; holdNextMs = 0; call.holdMs = delay;
    if (delay) await new Promise(resolve => setTimeout(resolve, delay));
    call.failAtOnResponse = failAt;
    if (calls.length === failAt) {
      call.responseStatus = failStatus;
      res.writeHead(failStatus, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ message: failStatus === 429 ? 'Concurrent generation limit reached' : 'synthetic generation failure' }));
      return;
    }
    if (malformedNext) {
      malformedNext = false;
      call.responseStatus = 200; call.malformed = true;
      res.writeHead(200, { 'content-type': 'text/html' });
      res.end('<html>synthetic upstream error</html>');
      return;
    }
    const stream = req.url.endsWith('-stream'), gate = previewGates.get(payload.parameters.seed);
    if (stream && gate) {
      res.writeHead(200, { 'content-type': 'application/octet-stream' });
      res.write(nativeImageMsgpack({ event_type: 'intermediate', samp_ix: 0, image: png, step_ix: gate.step }));
      gate.written = true;
      await gate.wait;
      if (gate.disconnect) { call.responseStatus = 'disconnected'; res.destroy(); return; }
    }
    const cost = quoteFairRequest(parseNativeImageRequest({ input: payload.input, action: payload.action, model: payload.model, parameters: payload.parameters }));
    if (cost.resource === 'anlas') officialFixed -= cost.amount;
    if (cost.resource === 'v5') officialV5 -= cost.amount;
    call.responseStatus = 200;
    if (!res.headersSent) res.writeHead(200, { 'content-type': stream ? 'application/octet-stream' : 'image/png' });
    res.end(stream ? nativeImagesMsgpack([png]) : png);
  });
  const upstreamPort = await listen(upstream);
  const reservation = http.createServer();
  const gatewayPort = await listen(reservation);
  await new Promise(resolve => reservation.close(resolve));
  const baseUrl = `http://127.0.0.1:${gatewayPort}`;
  const env = Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE'].filter(name => process.env[name]).map(name => [name, process.env[name]]));
  Object.assign(env, { ADMIN_TOKEN: adminToken, DATA_DIR: dataDir, HOST: '127.0.0.1', PORT: String(gatewayPort),
    NOVELAI_API_URL: `http://127.0.0.1:${upstreamPort}`, NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${upstreamPort}`,
    PUBLIC_BASE_URL: baseUrl, PUBLIC_HOST: 'gateway.example', QUEUE_MAX_PAYLOAD_BYTES: String(1024 * 1024) });
  const child = spawn(process.execPath, ['server/index.js'], { cwd: root, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  let runtimeOutput = '';
  const record = chunk => { runtimeOutput = (runtimeOutput + chunk.toString('utf8')).slice(-8000); };
  child.stdout.on('data', record); child.stderr.on('data', record);
  const stopped = once(child, 'exit');
  const stop = async () => { if (child.exitCode === null && child.signalCode === null) child.kill(); await stopped; };
  t.after(async () => {
    for (const gate of previewGates.values()) gate.release();
    await stop(); upstream.closeAllConnections(); await new Promise(resolve => upstream.close(resolve));
    // 保留这次隔离生成的临时文件，便于复核，不执行批量删除。
  });
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('公开版启动超时：' + runtimeOutput)), 15000);
    const ready = chunk => { if (chunk.toString().includes('Nai2API listening')) { clearTimeout(timer); child.stdout.off('data', ready); resolve(); } };
    child.stdout.on('data', ready);
    child.once('error', error => { clearTimeout(timer); reject(error); });
    child.once('exit', code => { clearTimeout(timer); reject(new Error(`公开版提前退出 ${code}：${runtimeOutput}`)); });
  });
  const redact = value => {
    let text = String(value);
    for (const secret of [adminToken, upstreamToken, ...credentials.flatMap(member => [member.api_key, member.password])]) {
      if (secret) text = text.replaceAll(secret, '[redacted]');
    }
    return text.replace(/\b(?:STA1N-|pst-)[A-Za-z0-9_-]+/g, '[redacted]')
      .replace(/nai_member_session=[A-Za-z0-9_-]+/g, 'nai_member_session=[redacted]');
  };
  const fetchFailure = (error, request) => {
    let state;
    try {
      state = {
        scenario, observedAt: Date.now(), request, callsAfter: calls.length,
        mock: { failAt, failStatus, activeGenerations, maxActiveGenerations, holdNextMs, malformedNext },
        calls: calls.slice(-16).map(call => ({ pathname: call.pathname, seed: call.payload.parameters.seed,
          scenario: call.scenario, startedAt: call.startedAt, finishedAt: call.finishedAt, closedAt: call.closedAt,
          responseFinished: call.responseFinished, responseStatus: call.responseStatus, holdMs: call.holdMs,
          failAtOnReceive: call.failAtOnReceive, failAtOnResponse: call.failAtOnResponse, malformed: call.malformed })),
        jobs: auditReader.prepare('SELECT data FROM jobs ORDER BY order_value DESC LIMIT 24').all().map(row => {
          const job = JSON.parse(row.data);
          return Object.fromEntries(['id', 'requestId', 'sampleIndex', 'status', 'queueSeq', 'createdAt', 'startedAt',
            'updatedAt', 'completedAt', 'queueExpiresAt', 'upstreamStartedAt', 'upstreamHeadersAt', 'upstreamCompletedAt',
            'upstreamHttpStatus', 'errorStatus', 'sharedCooldown', 'imageId'].map(key => [key, job[key]]).concat([
            ['model', job.request?.model], ['seed', job.request?.seed], ['chargeStatus', job.fairCharge?.status],
            ['resultPending', Boolean(job.resultPending)], ['error', String(job.error || '').slice(0, 300)]]));
        }),
        accounts: auditReader.prepare('SELECT data FROM accounts').all().map(row => {
          const account = JSON.parse(row.data);
          return Object.fromEntries(['id', 'enabled', 'inFlight', 'cooldownUntil', 'cooldownReason', 'ordinaryStrikeCount',
            'capacityStrikeCount', 'quotaCheckedAt', 'upstreamStatus'].map(key => [key, account[key]]));
        }),
        child: { exitCode: child.exitCode, signalCode: child.signalCode }
      };
    } catch (readError) { state = { scenario, request, callsAfter: calls.length, readError: String(readError) }; }
    const failure = { name: error?.name, message: error?.message || String(error), code: error?.code,
      cause: error?.cause && { name: error.cause.name, message: error.cause.message, code: error.cause.code } };
    return new Error(redact(`[native-http-state] ${JSON.stringify({ failure, state })}\nchild tail:\n${runtimeOutput}`), { cause: error });
  };
  const fetchLocal = async (pathname, options = {}) => {
    const request = { pathname: new URL(pathname, baseUrl).pathname, method: options.method || 'GET',
      startedAt: Date.now(), callsBefore: calls.length };
    try { return await fetch(baseUrl + pathname, { ...options, signal: AbortSignal.timeout(45000) }); }
    catch (error) { throw fetchFailure(error, request); }
  };
  const post = (pathname, request, key = credentials[0].api_key, headers = {}) => fetchLocal(pathname, {
    method: 'POST', headers: { authorization: `Bearer ${key}`, 'content-type': 'application/json', ...headers }, body: JSON.stringify(request)
  });
  const cookies = [];
  for (const member of credentials.slice(0, 2)) {
    const response = await fetchLocal('/api/member/login', { method: 'POST', headers: { 'content-type': 'application/json', origin: baseUrl },
      body: JSON.stringify({ username: member.username, password: member.password }) });
    assert.equal(response.status, 200);
    cookies.push(response.headers.get('set-cookie').split(';')[0]);
  }
  const quota = async (member = 0) => {
    const response = await fetchLocal('/api/member/me', { headers: { cookie: cookies[member] } });
    assert.equal(response.status, 200); return (await response.json()).quota;
  };

  await t.test('图片读取保留有效签名、本人及管理员访问，拒绝其他成员绕过过期签名', async () => {
    const ownHeaders = { authorization: `Bearer ${credentials[0].api_key}` };
    const otherHeaders = { authorization: `Bearer ${credentials[1].api_key}` };
    const jobResponse = await fetchLocal('/api/jobs/job-access-owner', { headers: ownHeaders });
    assert.equal(jobResponse.status, 200);
    const signed = (await jobResponse.json()).imageUrl;
    const unsigned = signed.split('?')[0];
    const expired = new URL(signed, baseUrl);
    expired.searchParams.set('expires', String(Math.floor(Date.now() / 1000) - 1));
    const forged = new URL(signed, baseUrl);
    forged.searchParams.set('signature', '0'.repeat(64));

    for (const headers of [undefined, otherHeaders]) {
      const response = await fetchLocal(signed, { headers });
      assert.equal(response.status, 200);
      assert.deepEqual(Buffer.from(await response.arrayBuffer()), png);
    }
    for (const pathname of [unsigned, expired.pathname + expired.search, forged.pathname + forged.search]) {
      assert.equal((await fetchLocal(pathname)).status, 401);
      assert.equal((await fetchLocal(pathname, { headers: otherHeaders })).status, 403);
      assert.equal((await fetchLocal(pathname, { headers: ownHeaders })).status, 200);
      assert.equal((await fetchLocal(pathname, { headers: { 'x-admin-token': adminToken } })).status, 200);
    }
    assert.equal((await fetchLocal('/api/images/img-access-owner/anything', { headers: ownHeaders })).status, 404);
  });

  await t.test('路径、鉴权与跨域校验在生图前完成，私有站点业务不存在', async () => {
    for (const pathname of ['/ai/generate-image-stream', '/v1/ai/generate-image-stream']) {
      assert.equal((await post(pathname, body(), 'invalid')).status, 401);
      assert.equal((await post(pathname, body('invalid-model'))).status, 422);
    }
    assert.equal(calls.length, 0);
    const ownerPage=await fetchLocal('/owner');assert.equal(ownerPage.status,200);assert.match(await ownerPage.text(),/management\.js/);
    assert.equal((await fetchLocal('/management.js')).status,200);
    assert.equal((await fetchLocal('/api/member/owner/summary')).status,401);
    for (const pathname of ['/discord', '/auth/discord/callback', '/api/carpool/teams']) assert.equal((await fetchLocal(pathname)).status, 404);
    const cors = await new Promise((resolve, reject) => {
      const request = http.request(new URL('/ai/generate-image-stream', baseUrl), { method: 'OPTIONS', headers: { host: 'gateway.example', origin: 'https://client.example', 'access-control-request-method': 'POST', 'access-control-request-headers': 'authorization,idempotency-key' } }, response => {
        response.resume(); response.on('end', () => resolve(response));
      });
      request.on('error', reject); request.end();
    });
    assert.equal(cors.statusCode, 204); assert.equal(cors.headers['access-control-allow-origin'], '*');
    assert.match(cors.headers['access-control-allow-headers'], /idempotency-key/);
  });

  await t.test('网页请求原子冷存完整大提示词，202/查询/取图契约不变，正文仍限 2 MiB', async () => {
    const prompt = '合成大提示词'.repeat(7000);
    const before = calls.length;
    await new Promise(resolve => {
      const request = http.request({ hostname: '127.0.0.1', port: gatewayPort, path: '/api/jobs', method: 'POST',
        headers: { authorization: `Bearer ${credentials[0].api_key}`, 'content-type': 'application/json' } });
      request.on('error', resolve);
      request.write('{"prompt":"' + 'x'.repeat(900 * 1024), () => setTimeout(() => request.destroy(), 50));
    });
    await new Promise(resolve => setTimeout(resolve, 50));
    const malformed = await fetchLocal('/api/jobs', { method: 'POST',
      headers: { authorization: `Bearer ${credentials[0].api_key}`, 'content-type': 'application/json' },
      body: '{"prompt":"' + 'x'.repeat(900 * 1024) });
    assert.equal(malformed.status, 400);
    // 原始点段路径不能绕过 route 的规范化路径所对应的载荷预算和持久父记录。
    const response = await new Promise((resolve, reject) => {
      const request = http.request({ hostname: '127.0.0.1', port: gatewayPort, path: '/x/../api/jobs', method: 'POST',
        headers: { authorization: `Bearer ${credentials[0].api_key}`, 'content-type': 'application/json' } }, response => {
        const chunks = [];
        response.on('data', chunk => chunks.push(chunk));
        response.on('end', () => resolve({ status: response.statusCode, json: () => JSON.parse(Buffer.concat(chunks).toString('utf8')) }));
      });
      request.on('error', reject);
      request.end(JSON.stringify({ prompt, artist: 'synthetic artist', negative: 'no text',
        model: 'nai-diffusion-4-5-full', width: 1024, height: 1024, steps: 28, seed: 891, nocache: true }));
    });
    assert.equal(response.status, 202);
    const accepted = await response.json();
    assert.equal(accepted.prompt, prompt);
    const stored = JSON.parse(auditReader.prepare('SELECT data FROM jobs WHERE id = ?').get(accepted.id).data);
    assert.ok(stored.requestId);
    assert.ok(Number.isSafeInteger(stored.queueSeq) && stored.queueSeq > 0);
    assert.equal(stored.request.tag, undefined);
    assert.equal(stored.request.prompt, undefined);
    assert.equal(stored.request.negative, undefined);
    const parent = auditReader.prepare('SELECT * FROM generation_requests WHERE id = ?').get(stored.requestId);
    assert.equal(parent.member_id, credentials[0].id || JSON.parse(auditReader.prepare('SELECT data FROM users WHERE token = ?').get(credentials[0].api_key).data).id);
    assert.equal(parent.payload_bytes, Buffer.byteLength(parent.payload));
    assert.equal(JSON.parse(parent.payload).prompt, 'synthetic artist\n' + prompt);
    assert.ok(!parent.payload.includes(credentials[0].api_key));
    let result;
    for (let attempt = 0; attempt < 200; attempt++) {
      result = await (await fetchLocal('/api/jobs/' + accepted.id, { headers: { authorization: `Bearer ${credentials[0].api_key}` } })).json();
      if (['done', 'failed'].includes(result.status)) break;
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.equal(result.status, 'done');
    assert.equal(result.prompt, prompt);
    assert.equal(calls[before].payload.input, 'synthetic artist\n' + prompt);
    const content = await fetchLocal('/api/jobs/' + accepted.id + '/content', { headers: { authorization: `Bearer ${credentials[0].api_key}` } });
    assert.deepEqual(Buffer.from(await content.arrayBuffer()), png);
    const rejected = await post('/api/jobs', { prompt: 'x'.repeat(2 * 1024 * 1024) });
    assert.equal(rejected.status, 413);
    assert.equal(calls.length, before + 1);
  });

  await t.test('上游 200 返回 HTML 也按失败退款，不保存为成功图片', async () => {
    const before = await quota();
    const beforeCalls = calls.length;
    malformedNext = true;
    const response = await post('/ai/generate-image', body());
    assert.ok(response.status >= 400);
    assert.equal((await quota()).v5Available, before.v5Available);
    assert.equal(calls.length, beforeCalls + 1);
  });

  await t.test('direct 生图仍返回原图片，实际派发使用持久排队序号', async () => {
    const before = calls.length;
    const query = new URLSearchParams({ token: credentials[0].api_key, prompt: 'direct queue sequence',
      artist: '', model: 'nai-diffusion-4-5-full', width: '1024', height: '1024', steps: '28', seed: '893', nocache: '1' });
    const response = await fetchLocal('/generate?' + query);
    assert.equal(response.status, 200);
    assert.deepEqual(Buffer.from(await response.arrayBuffer()), png);
    assert.equal(calls.length, before + 1);
    const row = auditReader.prepare("SELECT data, queue_seq FROM jobs WHERE source = 'direct' ORDER BY order_value DESC LIMIT 1").get();
    assert.ok(Number.isSafeInteger(row.queue_seq) && row.queue_seq > 0);
    assert.equal(JSON.parse(row.data).queueSeq, row.queue_seq);
  });

  await t.test('Full、5C 与 Medium 从两个流式路径返回可解码最终图，按实际参数只扣本人', async subtest => {
    for (const [model, pathname] of [['nai-diffusion-5-full', '/ai/generate-image-stream'], ['nai-diffusion-5-curated', '/v1/ai/generate-image-stream'], ['nai-diffusion-5-full-medium', '/ai/generate-image-stream']]) {
      const live = model === 'nai-diffusion-5-full';
      const input = body(model, live ? { seed: 707 } : {}), before = await quota(), other = await quota(1);
      const gate = live ? previewGate(707, 17) : null;
      if (gate) subtest.after(() => gate.release());
      const response = await post(pathname, input);
      assert.equal(response.status, 200);
      assert.match(response.headers.get('content-type'), /application\/octet-stream/);
      assert.equal(response.headers.get('x-nai-image-count'), null); assert.equal(response.headers.get('x-nai-partial'), null);
      const reader = streamEvents(response);
      subtest.after(() => reader.return());
      if (gate) {
        const { value: preview, done } = await reader.next();
        assert.equal(done, false); assert.equal(preview.event_type, 'intermediate');
        assert.equal(preview.step_ix, 17); assert.equal(preview.samp_ix, 0); assert.deepEqual(Buffer.from(preview.image), png);
        assert.equal(gate.written, true); assert.equal(gate.released, false);
        assert.equal(calls.at(-1).finishedAt, undefined, '预览必须在上游完成前到达');
        gate.release();
      }
      const { value: event, done } = await reader.next(); assert.equal(done, false);
      assert.equal(event.event_type, 'final'); assert.equal(event.samp_ix, 0); assert.deepEqual(Buffer.from(event.image), png);
      assert.equal((await reader.next()).done, true);
      const cost = quoteFairRequest(parseNativeImageRequest(input));
      near(before.v5Available - (await quota()).v5Available, cost.amount);
      assert.equal((await quota()).anlasFixedAvailable, before.anlasFixedAvailable);
      const unchanged = await quota(1);
      for (const field of ['v5Available', 'v5Capacity', 'v5Reserved', 'anlasFixedAvailable', 'anlasPurchasedAvailable', 'anlasReserved', 'share']) assert.equal(unchanged[field], other[field], field);
      assert.equal(calls.at(-1).payload.model, model);
      assert.equal(calls.at(-1).payload.parameters.steps, model.endsWith('-medium') ? 14 : 23);
    }
  });

  await t.test('桌面客户端 multipart 别名与普通 ZIP 接口兼容', async () => {
    const input = body('nai-diffusion-4-5-full', { uc_preset: 0, quality_toggle: false });
    const form = new FormData(); form.append('request', new Blob([JSON.stringify(input)], { type: 'application/json' }), 'request.json');
    const before = await quota();
    const response = await fetchLocal('/v1/ai/generate-image', { method: 'POST', headers: { authorization: `Bearer ${credentials[0].api_key}` }, body: form });
    assert.equal(response.status, 201); assert.match(response.headers.get('content-type'), /application\/zip/);
    assert.equal(Buffer.from(await response.arrayBuffer()).readUInt32LE(0), 0x04034b50);
    assert.equal(calls.at(-1).payload.parameters.ucPreset, 0); assert.equal(calls.at(-1).payload.parameters.qualityToggle, false);
    assert.equal(calls.at(-1).payload.parameters.uc_preset, undefined); assert.equal((await quota()).anlasFixedAvailable, before.anlasFixedAvailable);
  });

  await t.test('OpenAI 模型目录、非流式图片领取与流式结束契约通过真实 HTTP 验证', async () => {
    const model = 'nai-diffusion-4-5-full:k_euler_ancestral';
    const content = '提示词: a blue paper boat\n画师串:\n尺寸: 方图\n提示词引导值: 5\n缩放引导值: 0\n负面提示词:';
    const request = { model, messages: [{ role: 'user', content }] };
    const startCount = calls.length, before = await quota();
    for (const response of [await fetchLocal('/v1/models'), await post('/v1/chat/completions', request, 'invalid')]) {
      assert.equal(response.status, 401);
      const failure = await response.json();
      assert.equal(failure.error.type, 'authentication_error');
      assert.equal(failure.error.code, 'authentication_error');
      assert.equal(failure.error.param, null);
    }
    assert.equal(calls.length, startCount);
    const models = await fetchLocal('/v1/models', { headers: { authorization: `Bearer ${credentials[0].api_key}` } });
    assert.equal(models.status, 200);
    const catalog = await models.json();
    assert.equal(catalog.object, 'list');
    assert.ok(catalog.data.some(item => item.id === model && item.object === 'model'));

    const completion = await post('/v1/chat/completions', request);
    assert.equal(completion.status, 200, await completion.clone().text());
    assert.match(completion.headers.get('content-type'), /application\/json/);
    const message = await completion.json();
    assert.equal(message.object, 'chat.completion'); assert.equal(message.model, model);
    assert.equal(message.choices[0].message.role, 'assistant');
    assert.equal(message.choices[0].finish_reason, 'stop');
    const claimImage = async markdown => {
      const match = markdown.match(/!\[image\]\((https?:\/\/[^)]+)\)/);
      assert.ok(match, '生成结果必须包含可领取的 Markdown 图片链接');
      const imageUrl = new URL(match[1]);
      assert.equal(imageUrl.origin, baseUrl);
      const image = await fetchLocal(imageUrl.pathname + imageUrl.search);
      assert.equal(image.status, 200); assert.match(image.headers.get('content-type'), /image\/png/);
      assert.deepEqual(Buffer.from(await image.arrayBuffer()), png);
    };
    await claimImage(message.choices[0].message.content);

    const streamed = await post('/v1/chat/completions', { ...request, stream: true });
    assert.equal(streamed.status, 200);
    assert.match(streamed.headers.get('content-type'), /text\/event-stream/);
    const events = (await streamed.text()).split('\n\n').filter(event => event.startsWith('data: ')).map(event => event.slice(6));
    assert.equal(events.at(-1), '[DONE]');
    const chunks = events.slice(0, -1).map(event => JSON.parse(event));
    assert.ok(chunks.length >= 3);
    assert.ok(chunks.every(chunk => chunk.object === 'chat.completion.chunk' && chunk.model === model));
    assert.ok(chunks.every(chunk => chunk.id === chunks[0].id));
    assert.deepEqual(chunks.at(-1).choices, [{ index: 0, delta: {}, finish_reason: 'stop' }]);
    assert.ok(chunks.slice(0, -1).every(chunk => chunk.choices[0].finish_reason === null));
    await claimImage(chunks.map(chunk => chunk.choices[0].delta.content || '').join(''));
    assert.equal(calls.length - startCount, 2);
    for (const call of calls.slice(startCount)) {
      assert.equal(call.payload.model, 'nai-diffusion-4-5-full');
      assert.equal(call.payload.parameters.steps, 28);
      assert.equal(call.payload.parameters.n_samples, 1);
      assert.equal(call.payload.parameters.width, 1024); assert.equal(call.payload.parameters.height, 1024);
    }
    const after = await quota();
    for (const field of ['v5Available', 'anlasFixedAvailable', 'anlasPurchasedAvailable']) assert.equal(after[field], before[field]);
  });

  await t.test('首图失败保留HTTP错误；批次final先结算，次图预览后断流返回error并退款', async subtest => {
    scenario = 'single failure then partial batch';
    let before = await quota(); failAt = calls.length + 1;
    const failed = await post('/ai/generate-image', body(), credentials[0].api_key, { accept: 'application/json' });
    assert.equal(failed.status, 500); assert.match(failed.headers.get('content-type'), /application\/json/);
    const failure = await failed.json(); assert.ok(failure.error); assert.equal(failure.images, undefined);
    near((await quota()).v5Available, before.v5Available);
    before = await quota(); failAt = -1;
    const beforeCalls = calls.length, gate = previewGate(801, 9, true);
    subtest.after(() => gate.release());
    const response = await post('/v1/ai/generate-image-stream', body('nai-diffusion-5-curated', { steps: 29, n_samples: 3, seed: 800 }));
    assert.equal(response.status, 200); assert.equal(response.headers.get('x-nai-partial'), null); assert.equal(response.headers.get('x-nai-image-count'), null);
    const reader = streamEvents(response), first = (await reader.next()).value;
    subtest.after(() => reader.return());
    assert.equal(first.event_type, 'final'); assert.equal(first.samp_ix, 0); assert.deepEqual(Buffer.from(first.image), png);
    const batchRows = () => auditReader.prepare('SELECT data FROM jobs WHERE request_id = ?').all(response.headers.get('x-nai-request-id'))
      .map(row => JSON.parse(row.data)).sort((a, b) => a.sampleIndex - b.sampleIndex);
    let jobs = batchRows();
    assert.equal(jobs[0].status, 'done'); assert.ok(jobs[0].imageId);
    const charge = id => JSON.parse(auditReader.prepare('SELECT charge FROM fair_quota_settlements WHERE job_id = ?').get(id).charge);
    assert.equal(charge(jobs[0].id).status, 'settled');
    assert.notEqual(jobs[1].status, 'done'); assert.equal(calls.length, beforeCalls + 1, '首个final不能等第二张派发才发送');
    const preview = (await reader.next()).value;
    assert.equal(preview.event_type, 'intermediate'); assert.equal(preview.samp_ix, 1); assert.equal(preview.step_ix, 9);
    assert.deepEqual(Buffer.from(preview.image), png); assert.equal(gate.released, false); assert.equal(gate.written, true);
    gate.release();
    const failureFrame = (await reader.next()).value;
    assert.equal(failureFrame.event_type, 'error'); assert.equal(failureFrame.samp_ix, 1); assert.ok(failureFrame.message);
    assert.equal((await reader.next()).done, true);
    jobs = batchRows();
    assert.deepEqual(jobs.map(job => job.status), ['done', 'failed', 'failed']);
    assert.equal(charge(jobs[1].id).status, 'refunded'); assert.equal(jobs[2].generationAttempt, undefined);
    assert.equal(before.anlasFixedAvailable - (await quota()).anlasFixedAvailable, 32);
    assert.equal(calls.length, beforeCalls + 2);
  });

  await t.test('两位成员同时生图，第一张超过十秒时第二张仍排队，完整结束后才接续', async subtest => {
    scenario = 'two members with held first generation';
    const before = [await quota(), await quota(1)], startCount = calls.length;
    holdNextMs = 11600; maxActiveGenerations = 0;
    const firstBody = body('nai-diffusion-5-full', { seed: 9001 }), key = 'live-preview-disconnect-replay';
    const gate = previewGate(9001, 11);
    subtest.after(() => gate.release());
    const firstPending = post('/ai/generate-image-stream', firstBody, credentials[0].api_key, { 'Idempotency-Key': key });
    const secondPending = post('/v1/ai/generate-image-stream', body(), credentials[1].api_key);
    secondPending.catch(() => {}); // 首请求的断言先失败时，仍避免并发请求产生未处理拒绝；下方 await 会继续传播错误。
    const firstResponse = await firstPending, firstReader = streamEvents(firstResponse);
    subtest.after(() => firstReader.return());
    assert.equal(firstResponse.status, 200);
    const preview = (await firstReader.next()).value;
    assert.equal(preview.event_type, 'intermediate'); assert.equal(preview.step_ix, 11); assert.equal(gate.released, false);
    await firstReader.return(); // 关闭浏览器连接，随后才让上游完成。
    gate.release();
    const secondResponse = await secondPending;
    const replay = await post('/ai/generate-image-stream', firstBody, credentials[0].api_key, { 'Idempotency-Key': key });
    assert.equal(replay.headers.get('x-nai-request-id'), firstResponse.headers.get('x-nai-request-id'));
    for (const response of [secondResponse, replay]) {
      assert.equal(response.status, 200, await response.clone().text());
      const final = readNativeEvents(Buffer.from(await response.arrayBuffer())).find(event => event.event_type === 'final');
      assert.ok(final); assert.deepEqual(Buffer.from(final.image), png);
    }
    assert.equal(maxActiveGenerations, 1); assert.equal(calls.length - startCount, 2);
    assert.ok(calls[startCount + 1].startedAt - calls[startCount].finishedAt >= 10000);
    const cost = quoteFairRequest(parseNativeImageRequest(body()));
    for (let member = 0; member < 2; member++) near(before[member].v5Available - (await quota(member)).v5Available, cost.amount);
  });

  await t.test('成员上限与原子停止接单：排队取消，在途结算，恢复不续旧批次', async () => {
    scenario = 'admission limit and member stop';
    const manage = value => fetchLocal('/api/admin/member-management', {
      method: 'POST', headers: { 'x-admin-token': adminToken, 'content-type': 'application/json', origin: baseUrl },
      body: JSON.stringify(value)
    });
    const memberId = credentials[0].user_id;
    const startCount = calls.length, before = await quota();
    holdNextMs = 3000;
    const batch = post('/ai/generate-image-stream', body('nai-diffusion-5-full', { n_samples: 3 }));
    const startedDeadline = Date.now() + 20000;
    while (calls.length === startCount && Date.now() < startedDeadline) await new Promise(resolve => setTimeout(resolve, 20));
    assert.equal(calls.length, startCount + 1);
    const queued = [];
    const enqueue = seed => post('/api/jobs', { prompt: 'synthetic queue admission', model: 'nai-diffusion-5-full', width: 1024,
      height: 1024, steps: 23, seed, nocache: true });
    for (let index = 0; index < 1; index++) {
      const response = await enqueue(900 + index);
      assert.equal(response.status, 202, await response.clone().text());
      queued.push((await response.json()).id);
    }
    const rejected = await enqueue(904);
    assert.equal(rejected.status, 429);
    assert.equal(typeof (await rejected.json()).error, 'string');
    const stopped = await manage({ action: 'member-admission', userId: memberId, acceptingJobs: false });
    assert.equal(stopped.status, 200);
    assert.equal((await stopped.json()).cancelledCount, 3);
    const me = await fetchLocal('/api/member/me', { headers: { cookie: cookies[0] } });
    assert.equal(me.status, 200);
    assert.equal((await me.json()).acceptingJobs, false);
    const denied = await post('/ai/generate-image', body());
    assert.equal(denied.status, 403);
    assert.equal(typeof (await denied.json()).message, 'string');
    assert.equal((await manage({ action: 'member-admission', userId: memberId, acceptingJobs: true })).status, 200);
    const result = await batch;
    assert.equal(result.status, 200);
    assert.equal(result.headers.get('x-nai-image-count'), null);
    assert.equal(result.headers.get('x-nai-partial'), null);
    assert.deepEqual(readNativeEvents(Buffer.from(await result.arrayBuffer())).map(event => event.event_type), ['final', 'error']);
    assert.equal(calls.length, startCount + 1);
    near(before.v5Available - (await quota()).v5Available, quoteFairRequest(parseNativeImageRequest(body())).amount);
    for (const id of queued) {
      const response = await fetchLocal('/api/jobs/' + id, { headers: { authorization: `Bearer ${credentials[0].api_key}` } });
      assert.equal(response.status, 200);
      assert.equal((await response.json()).status, 'failed');
    }
    // 恢复只接受新请求；立即再停止清理这个新排队任务，避免发出额外上游生成。
    assert.equal((await enqueue(905)).status, 202);
    const cleaned = await manage({ action: 'member-admission', userId: memberId, acceptingJobs: false });
    assert.equal((await cleaned.json()).cancelledCount, 1);
    assert.equal((await manage({ action: 'member-admission', userId: memberId, acceptingJobs: true })).status, 200);
  });

  await t.test('上游并发锁返回 429 与重试时间，预扣退回且不自动重发', async () => {
    scenario = 'upstream capacity rejection';
    const before = await quota(); failStatus = 429; failAt = calls.length + 1;
    const response = await post('/ai/generate-image-stream', body());
    assert.equal(response.status, 429); assert.equal(response.headers.get('retry-after'), '30');
    near((await quota()).v5Available, before.v5Available);
    assert.equal(calls.length, failAt);
  });

  await stop();
  const persisted = new JsonStore(dataDir);
  await persisted.init();
  try {
    const snapshot = await persisted.readCollections(['accounts', 'jobs', 'settings']);
    assert.equal(snapshot.accounts[0].capacityConcurrencyLimit, 1);
    assert.deepEqual(snapshot.settings.fairQuota.reservations, {});
    const reservations = persisted.sqlite.prepare('SELECT charge FROM fair_quota_settlements').all().map(row => JSON.parse(row.charge));
    assert.ok(reservations.some(charge => charge.status === 'settled'));
    assert.ok(reservations.some(charge => charge.status === 'refunded'));
    assert.ok(reservations.every(charge => charge.status !== 'reserved'));
    assert.ok(snapshot.jobs.every(job => ['done', 'failed'].includes(job.status)));
    assert.equal(dispatchEvidence.length, calls.length);
    for (const dispatched of dispatchEvidence) {
      assert.equal(dispatched.length, 1);
      assert.ok(Number.isSafeInteger(dispatched[0].queueSeq) && dispatched[0].queueSeq > 0);
      assert.equal(dispatched[0].attempt.id, dispatched[0].reservation.attemptId);
      assert.equal(dispatched[0].reservation.status, 'reserved');
      assert.deepEqual(JSON.parse(dispatched[0].ledger).attempt, dispatched[0].attempt);
    }
    const audit = auditReader.prepare("SELECT data FROM ledger WHERE source LIKE 'generation_%'").all().map(row => JSON.parse(row.data));
    for (const job of snapshot.jobs) {
      const refunds = audit.filter(event => event.jobId === job.id && event.type === 'generation_refunded');
      const losses = audit.filter(event => event.jobId === job.id && event.type === 'generation_loss_suspected');
      assert.equal(refunds.length, job.fairCharge?.status === 'refunded' ? 1 : 0);
      if (!job.fairCharge || job.status === 'done') assert.equal(losses.length, 0);
      if (losses.length) {
        assert.equal(losses.length, 1); assert.equal(losses[0].actualConsumed, null);
        assert.equal(losses[0].response.upstreamHttpStatus, job.upstreamHttpStatus);
      }
    }
    assert.ok(audit.some(event => event.type === 'generation_loss_suspected' && event.response.upstreamHttpStatus === 200));
    const batch = snapshot.jobs.filter(job => job.sampleIndex === 1 && job.fairCharge?.status === 'refunded')[0];
    assert.ok(batch?.batchId);
    assert.ok(snapshot.jobs.some(job => job.batchId === batch.batchId && job.sampleIndex === 0 && job.status === 'done'));
    const request = { tag: 'restart queued request', prompt: 'restart queued request', artist: '', negative: '',
      model: 'nai-diffusion-4-5-full', width: 1024, height: 1024, steps: 28, seed: 901,
      sampler: 'k_euler_ancestral', scale: 6, cfg: 0, noiseSchedule: 'karras' };
    const payload = JSON.stringify(request), createdAt = new Date().toISOString();
    let parent;
    await persisted.update(db => {
      const user = db.users.find(user => user.token === credentials[0].api_key);
      parent = { id: 'request-restart', member_id: user.id, source: 'web', operation: 'generate',
        created_at: createdAt, payload_bytes: Buffer.byteLength(payload), payload };
      db.accounts[0].cooldownUntil = '';
      const common = { source: 'web', status: 'queued', userToken: user.token, createdAt, updatedAt: createdAt,
        jobAdmissionVersion: user.jobAdmissionVersion || 0, cost: 0, imageId: '',
        queueExpiresAt: new Date(Date.now() + 60000).toISOString() };
      db.jobs.unshift({ ...common, id: 'job-restart-cold', requestId: parent.id, queueSeq: persisted.nextQueueSequence(),
        request: { model: request.model, width: 1024, height: 1024, steps: 28, seed: 901 } });
      db.jobs.unshift({ ...common, id: 'job-restart-legacy', queueSeq: persisted.nextQueueSequence(), request: { ...request, seed: 902 } });
    }, { immediate: true, generationRequest: () => parent,
      dirtyRows: { accounts: [snapshot.accounts[0].id], jobs: ['job-restart-cold', 'job-restart-legacy'] } });
    // 故意令历史顺序与排队顺序相反；重启后的真实派发仍须使用持久 queueSeq。
    persisted.sqlite.exec("UPDATE jobs SET order_value = 1000 WHERE id = 'job-restart-cold'; UPDATE jobs SET order_value = -1000 WHERE id = 'job-restart-legacy';");
  } finally { await persisted.close(); }

  await t.test('独立进程重启后继续冷存网页任务，并兼容旧无 requestId 的 queued 记录', async () => {
    const before = calls.length;
    const restarted = spawn(process.execPath, ['server/index.js'], { cwd: root, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    const exited = once(restarted, 'exit');
    try {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('重启超时')), 15000);
        restarted.stdout.on('data', chunk => {
          if (chunk.toString().includes('Nai2API listening')) { clearTimeout(timer); resolve(); }
        });
        restarted.stderr.on('data', record);
        restarted.once('error', error => { clearTimeout(timer); reject(error); });
      });
      for (const id of ['job-restart-cold', 'job-restart-legacy']) {
        let job;
        for (let attempt = 0; attempt < 400; attempt++) {
          job = await (await fetchLocal('/api/jobs/' + id, { headers: { authorization: `Bearer ${credentials[0].api_key}` } })).json();
          if (['done', 'failed'].includes(job.status)) break;
          await new Promise(resolve => setTimeout(resolve, 50));
        }
        assert.equal(job.status, 'done', JSON.stringify(job));
        assert.equal(job.prompt, 'restart queued request');
      }
      assert.deepEqual(calls.slice(before).map(call => call.payload.parameters.seed), [901, 902]);
    } finally {
      restarted.kill();
      await exited;
    }
  });
});
