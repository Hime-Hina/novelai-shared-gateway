import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { rename } from 'node:fs/promises';
import path from 'node:path';
import { once } from 'node:events';
import Database from 'better-sqlite3';
import { quoteFairRequest } from '../../server/quota/fair-share.ts';
import { crc32, parseNativeImageRequest } from '../../server/protocols/native-image.ts';
import { nativeImageMsgpack, nativeImagesMsgpack } from '../../server/protocols/native-stream.ts';
import { readNativeEvents, readNativeSseEvents } from '../helpers/native-stream-client.mjs';
import {
  childFixture,
  launchGateway,
  enableAccount,
  query,
  assertChildPublicReplay,
} from '../helpers/relational-child.mjs';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { sharedAccountConcurrency } from '../../server/quota/shared-capacity.ts';
import { relationalAccountConnectionSource } from '../../server/storage/relational-account-connection.ts';

// 合成图片和新建临时数据库；测试不会读取 .env、生产数据或请求真实 NAI。
const png = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=',
  'base64',
);
// 合法PNG元数据将已有像素样本扩为8MiB，实际socket必须处理多个SSE块。
const metadataChunk = Buffer.alloc(8 * 1024 * 1024 + 12, 0x78);
metadataChunk.writeUInt32BE(metadataChunk.length - 12, 0);
metadataChunk.write('tEXt', 4);
metadataChunk.write('Comment\0', 8);
metadataChunk.writeUInt32BE(crc32(metadataChunk.subarray(4, -4)), metadataChunk.length - 4);
const largePng = Buffer.concat([png.subarray(0, -12), metadataChunk, png.subarray(-12)]);
const body = (model = 'nai-diffusion-5-full', extra = {}) => ({
  input: 'a small blue paper boat',
  model,
  action: 'generate',
  parameters: {
    width: 1024,
    height: 1024,
    steps: 23,
    n_samples: 1,
    seed: 7,
    stream: 'msgpack',
    ...extra,
  },
});
const privateMarker = 'synthetic-private-generation-diagnostic';
const safeFailure = '生成失败，请稍后重试或联系管理员。';
const legacyKey = 'synthetic-legacy-private-error';
const near = (actual, expected) =>
  assert.ok(Math.abs(actual - expected) < 1e-8, `${actual} != ${expected}`);

async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}

async function* streamEvents(response) {
  const reader = response.body.getReader();
  const sse = response.headers.get('content-type') === 'text/event-stream';
  let pending = Buffer.alloc(0);
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        assert.equal(pending.length, 0, '结束时不能遗留半帧');
        return;
      }
      pending = Buffer.concat([pending, Buffer.from(value)]);
      while (pending.length >= 4) {
        if (sse) {
          const end = pending.indexOf('\n\n');
          if (end < 0) {
            break;
          }
          const [event] = readNativeSseEvents(pending.subarray(0, end + 2));
          pending = pending.subarray(end + 2);
          if (event.image) {
            event.image = Buffer.from(event.image, 'base64');
          }
          yield event;
          continue;
        }
        const length = pending.readUInt32BE(0);
        assert.ok(length > 0 && length <= 64 * 1024 * 1024, '无效帧长度');
        if (pending.length < length + 4) {
          break;
        }
        const [event] = readNativeEvents(pending.subarray(0, length + 4));
        pending = pending.subarray(length + 4);
        yield event;
      }
    }
  } finally {
    await reader.cancel();
    reader.releaseLock();
  }
}

async function nativeFixture(t) {
  const state = await childFixture(t, 'native-http'),
    dataDir = state.dataDir,
    credentials = state.members,
    adminToken = state.env.ADMIN_TOKEN,
    upstreamToken = state.env.NOVELAI_TOKEN,
    privatePathError = `ENOENT: no such file or directory, open '${path.join(state.directory, privateMarker)}'`;
  const read = (consume) => {
    const sqlite = new Database(state.filename, { readonly: true });
    try {
      return consume(sqlite);
    } finally {
      sqlite.close();
    }
  };
  const jobs = () =>
    query(
      state,
      `SELECT j.*,r.source,r.model,r.member_id,a.id AS attempt_id,
    a.account_id,a.upstream_http_status,a.actual_consumption,a.loss_event_id,c.status AS charge_status
    FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id
    LEFT JOIN generation_attempts a ON a.job_id=j.id LEFT JOIN charges c ON c.job_id=j.id ORDER BY j.rowid`,
    );
  const events = () =>
    read((sqlite) =>
      sqlite
        .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
        .all()
        .flatMap(({ seq }) => readRelationalCommit(sqlite, seq).events),
    );
  const charge = (jobId) => query(state, 'SELECT * FROM charges WHERE job_id=?', jobId)[0];
  const diagnostic = (jobId) => {
    const row = query(state, 'SELECT id FROM generation_diagnostics WHERE job_id=?', jobId)[0];
    assert.ok(row, 'actual failed dispatch must retain its original protected diagnostic');
    return read((sqlite) =>
      new RelationalInputStore(sqlite).readGenerationDiagnostic(row.id, jobId),
    );
  };
  const inputRequest = (jobId) =>
    read((sqlite) => new RelationalInputStore(sqlite).readErrorLogRequest(jobId));
  const memberInput = (requestId, memberId) =>
    read((sqlite) => new RelationalInputStore(sqlite).readMemberInput(requestId, memberId));
  const localArtifact = (artifactId) =>
    read((sqlite) => {
      const replicas = new RelationalReplicaStore(sqlite, { dataDir }),
        source = replicas.beginArtifactRead(artifactId, 'local');
      assert.ok(source);
      replicas.assertArtifactReplicaTicket(source.ticket);
      return source.artifact.descriptor;
    });
  const calls = [];
  const mock = {
    scenario: t.name,
    failAt: -1,
    failStatus: 500,
    maxActiveGenerations: 0,
    holdNextMs: 0,
    malformedNext: false,
  };
  let officialFixed = 10000,
    officialV5 = 100,
    activeGenerations = 0;
  const previewGates = new Map();
  const previewGate = (seed, step, disconnect = false) => {
    let release, receive;
    const gate = {
      step,
      disconnect,
      written: false,
      released: false,
      received: new Promise((resolve) => {
        receive = resolve;
      }),
      wait: new Promise((resolve) => {
        release = resolve;
      }),
    };
    gate.receive = receive;
    gate.release = () => {
      gate.released = true;
      release();
    };
    previewGates.set(seed, gate);
    return gate;
  };
  const dispatchEvidence = [];
  const upstream = http.createServer(async (req, res) => {
    assert.equal(req.headers.authorization, `Bearer ${upstreamToken}`);
    if (req.url === '/user/data') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(
        JSON.stringify({
          subscription: {
            tier: 3,
            active: true,
            expiresAt: Math.floor(Date.now() / 1000) + 86400,
            trainingStepsLeft: { fixedTrainingStepsLeft: officialFixed, purchasedTrainingSteps: 0 },
            usage: { percent: officialV5, isNegative: false, timeUntilNextPercent: 0 },
          },
        }),
      );
      return;
    }
    const chunks = [];
    for await (const chunk of req) {
      chunks.push(chunk);
    }
    const payload = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    // 真正收到上游调用时就检查派发证据已提交，不能只在失败后补写一个看似完整的历史。
    const running = query(
      state,
      `SELECT j.id AS job_id,j.request_id,j.queue_seq,r.member_id,
      a.id AS attempt_id,a.account_id,a.credential_epoch,c.attempt_id AS charge_attempt_id,
      c.status AS charge_status,c.wallet_member_id,c.wallet_account_id,c.reservation_event_id,
      account_state.credential_epoch AS current_credential_epoch
      FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id
      JOIN generation_attempts a ON a.job_id=j.id JOIN charges c ON c.job_id=j.id
      JOIN upstream_accounts account_state ON account_state.id=a.account_id WHERE j.status='running'`,
    );
    dispatchEvidence.push(
      running.map((row) => ({
        ...row,
        connectionSourceEventId: read((sqlite) =>
          relationalAccountConnectionSource(sqlite, row.account_id),
        ),
        original: read((sqlite) => {
          const { seq } = sqlite
            .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM domain_events WHERE event_id=?')
            .get(row.reservation_event_id);
          return readRelationalCommit(sqlite, seq);
        }),
      })),
    );
    const call = {
      pathname: req.url,
      payload,
      startedAt: Date.now(),
      scenario: mock.scenario,
      failAtOnReceive: mock.failAt,
    };
    calls.push(call);
    activeGenerations++;
    mock.maxActiveGenerations = Math.max(mock.maxActiveGenerations, activeGenerations);
    let counted = true;
    const inactive = () => {
      if (counted) {
        activeGenerations--;
        counted = false;
      }
    };
    res.once('finish', () => {
      inactive();
      call.finishedAt = Date.now();
    });
    res.once('close', () => {
      inactive();
      call.closedAt = Date.now();
      call.responseFinished = res.writableFinished;
    });
    if (activeGenerations > 1) {
      call.responseStatus = 429;
      res.writeHead(429, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ message: 'Concurrent generation is locked' }));
      return;
    }
    previewGates.get(payload.parameters.seed)?.receive();
    const delay = mock.holdNextMs;
    mock.holdNextMs = 0;
    call.holdMs = delay;
    if (delay) {
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
    call.failAtOnResponse = mock.failAt;
    if (calls.length === mock.failAt) {
      call.responseStatus = mock.failStatus;
      res.writeHead(mock.failStatus, { 'content-type': 'application/json' });
      res.end(
        JSON.stringify({
          message:
            mock.failStatus === 429
              ? 'Concurrent generation limit reached'
              : 'synthetic generation failure',
          diagnostic: privateMarker + ' ' + privatePathError,
          credential: upstreamToken,
        }),
      );
      return;
    }
    if (mock.malformedNext) {
      mock.malformedNext = false;
      call.responseStatus = 200;
      call.malformed = true;
      res.writeHead(200, { 'content-type': 'text/html' });
      res.end('<html>synthetic upstream error</html>');
      return;
    }
    const stream = req.url.endsWith('-stream'),
      gate = previewGates.get(payload.parameters.seed);
    if (stream && gate) {
      res.writeHead(200, { 'content-type': 'application/octet-stream' });
      res.write(
        nativeImageMsgpack({
          event_type: 'intermediate',
          samp_ix: 0,
          image: png,
          step_ix: gate.step,
        }),
      );
      gate.written = true;
      await gate.wait;
      if (gate.errorMessage) {
        call.responseStatus = 200;
        res.end(
          nativeImageMsgpack({ event_type: 'error', samp_ix: 0, message: gate.errorMessage }),
        );
        return;
      }
      if (gate.disconnect) {
        call.responseStatus = 'disconnected';
        res.destroy();
        return;
      }
    }
    const cost = quoteFairRequest(
      parseNativeImageRequest({
        input: payload.input,
        action: payload.action,
        model: payload.model,
        parameters: payload.parameters,
      }),
    );
    if (cost.resource === 'anlas') {
      officialFixed -= cost.amount;
    }
    if (cost.resource === 'v5') {
      officialV5 -= cost.amount;
    }
    call.responseStatus = 200;
    if (!res.headersSent) {
      res.writeHead(200, { 'content-type': stream ? 'application/octet-stream' : 'image/png' });
    }
    const image = payload.parameters.seed === 9001 ? largePng : png;
    res.end(stream ? nativeImagesMsgpack([image]) : image);
  });
  const upstreamPort = await listen(upstream);
  const env = { PUBLIC_HOST: 'gateway.example', QUEUE_MAX_PAYLOAD_BYTES: String(1024 * 1024) };
  let server, baseUrl, gatewayPort;
  const start = async () => {
    server = await launchGateway(state, `http://127.0.0.1:${upstreamPort}`, env);
    baseUrl = server.origin;
    gatewayPort = Number(new URL(baseUrl).port);
  };
  const stop = () => server.stop();
  t.after(async () => {
    for (const gate of previewGates.values()) {
      gate.release();
    }
    upstream.closeAllConnections();
    await new Promise((resolve) => upstream.close(resolve));
  });
  await start();
  await enableAccount(state, server);
  const redact = (value) => {
    let text = String(value);
    for (const secret of [
      adminToken,
      upstreamToken,
      ...credentials.flatMap((member) => [member.api_key, member.password]),
    ]) {
      if (secret) {
        text = text.replaceAll(secret, '[redacted]');
      }
    }
    return text
      .replace(/\b(?:STA1N-|pst-)[A-Za-z0-9_-]+/g, '[redacted]')
      .replace(/nai_member_session=[A-Za-z0-9_-]+/g, 'nai_member_session=[redacted]');
  };
  const fetchFailure = (error, request) => {
    let observed;
    try {
      observed = {
        scenario: mock.scenario,
        observedAt: Date.now(),
        request,
        callsAfter: calls.length,
        mock: {
          failAt: mock.failAt,
          failStatus: mock.failStatus,
          activeGenerations,
          maxActiveGenerations: mock.maxActiveGenerations,
          holdNextMs: mock.holdNextMs,
          malformedNext: mock.malformedNext,
        },
        calls: calls.slice(-16).map((call) => ({
          pathname: call.pathname,
          seed: call.payload.parameters.seed,
          scenario: call.scenario,
          startedAt: call.startedAt,
          finishedAt: call.finishedAt,
          closedAt: call.closedAt,
          responseFinished: call.responseFinished,
          responseStatus: call.responseStatus,
          holdMs: call.holdMs,
          failAtOnReceive: call.failAtOnReceive,
          failAtOnResponse: call.failAtOnResponse,
          malformed: call.malformed,
        })),
        jobs: jobs(),
        accounts: query(
          state,
          'SELECT id,enabled,cooldown_until,cooldown_reason,ordinary_strikes,capacity_strikes,credential_epoch FROM upstream_accounts',
        ),
        child: server.exit,
      };
    } catch (readError) {
      observed = {
        scenario: mock.scenario,
        request,
        callsAfter: calls.length,
        readError: String(readError),
      };
    }
    const failure = {
      name: error?.name,
      message: error?.message || String(error),
      code: error?.code,
      cause: error?.cause && {
        name: error.cause.name,
        message: error.cause.message,
        code: error.cause.code,
      },
    };
    return new Error(
      redact(
        `[native-http-state] ${JSON.stringify({ failure, state: observed })}\nchild tail:\n${server.output}`,
      ),
      { cause: error },
    );
  };
  const fetchLocal = async (pathname, options = {}) => {
    const request = {
      pathname: new URL(pathname, baseUrl).pathname,
      method: options.method || 'GET',
      startedAt: Date.now(),
      callsBefore: calls.length,
    };
    try {
      return await fetch(baseUrl + pathname, { ...options, signal: AbortSignal.timeout(45000) });
    } catch (error) {
      throw fetchFailure(error, request);
    }
  };
  const post = (pathname, request, key = credentials[0].api_key, headers = {}) =>
    fetchLocal(pathname, {
      method: 'POST',
      headers: { authorization: `Bearer ${key}`, 'content-type': 'application/json', ...headers },
      body: JSON.stringify(request),
    });
  const cookies = [];
  for (const member of credentials.slice(0, 2)) {
    const response = await fetchLocal('/api/member/login', {
      method: 'POST',
      headers: { 'content-type': 'application/json', origin: baseUrl },
      body: JSON.stringify({ username: member.username, password: member.password }),
    });
    assert.equal(response.status, 200);
    cookies.push(response.headers.get('set-cookie').split(';')[0]);
  }
  const quota = async (member = 0) => {
    const response = await fetchLocal('/api/member/me', { headers: { cookie: cookies[member] } });
    assert.equal(response.status, 200);
    return (await response.json()).quota;
  };

  return {
    ...state,
    dataDir,
    adminToken,
    upstreamToken,
    credentials,
    calls,
    mock,
    previewGate,
    dispatchEvidence,
    stop,
    reopen: start,
    fetchLocal,
    post,
    cookies,
    quota,
    jobs,
    events,
    charge,
    diagnostic,
    inputRequest,
    memberInput,
    localArtifact,
    get baseUrl() {
      return baseUrl;
    },
    get gatewayPort() {
      return gatewayPort;
    },
  };
}

function checkAccounting(fixture, jobs) {
  const { dispatchEvidence, calls } = fixture;
  const settlements = query(fixture, 'SELECT * FROM charges ORDER BY job_id');
  assert.ok(settlements.every((charge) => charge.status !== 'reserved'));
  assert.ok(jobs.every((job) => ['done', 'failed'].includes(job.status)));
  assert.equal(dispatchEvidence.length, calls.length);
  for (const dispatched of dispatchEvidence) {
    assert.equal(dispatched.length, 1);
    const row = dispatched[0];
    assert.ok(Number.isSafeInteger(row.queue_seq) && row.queue_seq > 0);
    assert.equal(row.attempt_id, row.charge_attempt_id);
    assert.equal(row.charge_status, 'reserved');
    assert.equal(row.wallet_member_id, row.member_id);
    assert.equal(row.wallet_account_id, row.account_id);
    assert.equal(row.credential_epoch, row.current_credential_epoch);
    const reserved = row.original.events.find(
        (event) => event.eventId === row.reservation_event_id,
      ),
      authorized = row.original.events.find((event) => event.eventType === 'DispatchAuthorized');
    assert.equal(reserved.eventType, 'QuotaReserved');
    assert.equal(reserved.payload.jobId, row.job_id);
    assert.equal(reserved.payload.memberId, row.member_id);
    assert.equal(reserved.payload.accountId, row.account_id);
    assert.equal(authorized.payload.attemptId, row.attempt_id);
    assert.equal(authorized.payload.jobId, row.job_id);
    assert.equal(authorized.payload.accountId, row.account_id);
    assert.equal(authorized.payload.credentialEpoch, row.credential_epoch);
    assert.equal(authorized.payload.connectionSourceEventId, row.connectionSourceEventId);
    assert.equal(authorized.memberId, row.member_id);
    assert.equal(reserved.memberId, row.member_id);
    assert.equal(authorized.commitSeq, reserved.commitSeq);
    assert.ok(row.original.events.indexOf(reserved) < row.original.events.indexOf(authorized));
  }
  const audit = fixture.events();
  for (const job of jobs) {
    const refunds = audit.filter(
        (event) => event.eventType === 'GenerationRefunded' && event.payload.jobId === job.id,
      ),
      losses = audit.filter(
        (event) => event.eventType === 'ReviewOpened' && event.payload.attemptId === job.attempt_id,
      );
    assert.equal(refunds.length, job.charge_status === 'refunded' ? 1 : 0);
    if (!job.charge_status || job.status === 'done') {
      assert.equal(losses.length, 0);
    }
    if (losses.length) {
      assert.equal(losses.length, 1);
      assert.equal(job.actual_consumption, null);
      assert.equal(losses[0].payload.response.upstreamHttpStatus, job.upstream_http_status);
      assert.equal(losses[0].eventId, job.loss_event_id);
    }
  }
  return { audit, settlements };
}
function checkSingleGeneration(fixture, status, settlementStatus) {
  const jobs = fixture.jobs();
  assert.equal(fixture.calls.length, 1);
  assert.equal(jobs.length, 1);
  const [job] = jobs;
  assert.equal(job.status, status);
  const { audit, settlements } = checkAccounting(fixture, jobs);
  assert.deepEqual(
    settlements.map((charge) => charge.status),
    [settlementStatus],
  );
  return { job, audit };
}

test('独立公开发行版从初始化到真实 HTTP 返回、扣费、退款与重启', { timeout: 240000 }, async (t) => {
  const fixture = await nativeFixture(t);
  const {
    dataDir,
    adminToken,
    upstreamToken,
    credentials,
    calls,
    mock,
    previewGate,
    jobs,
    events,
    charge,
    diagnostic,
    inputRequest,
    memberInput,
    localArtifact,
    baseUrl,
    gatewayPort,
    stop,
    fetchLocal,
    post,
    cookies,
    quota,
  } = fixture;
  let savedImageJobId, failedReplayJobId;

  await t.test('后端旧门户及静态资源的 GET/HEAD 均为 404，无重定向或成功回退', async () => {
    for (const pathname of [
      '/',
      '/index.html',
      '/account.html',
      '/account.js',
      '/members.css',
      '/shared-status.js',
      '/member-events.js',
      '/generation-pricing.js',
      '/v5-medium.js',
      '/admin',
      '/admin/',
      '/admin.html',
      '/admin/members.html',
      '/admin/members.js',
      '/owner',
      '/owner/',
      '/owner.html',
      '/management.js',
      '/generate.html',
      '/generate.js',
      '/generate.css',
      '/unknown.html',
      '/unknown.js',
      '/unknown.css',
    ]) {
      for (const method of ['GET', 'HEAD']) {
        const response = await fetchLocal(pathname, { method, redirect: 'manual' });
        assert.equal(response.status, 404, `${method} ${pathname}`);
        assert.equal(response.headers.get('location'), null, `${method} ${pathname}`);
        assert.match(response.headers.get('content-type'), /application\/json/);
        if (method === 'HEAD') {
          assert.equal(await response.text(), '');
        } else {
          assert.deepEqual(await response.json(), { error: 'not found.' });
        }
      }
    }
    for (const method of ['GET', 'HEAD']) {
      const health = await fetchLocal('/api/health', { method });
      assert.equal(health.status, 200);
      assert.match(health.headers.get('content-type'), /application\/json/);
      if (method === 'HEAD') {
        assert.equal(await health.text(), '');
      } else {
        assert.equal((await health.json()).ok, true);
      }
    }
    assert.equal(calls.length, 0);
  });

  await t.test('路径、鉴权与跨域校验在生图前完成，私有站点业务不存在', async () => {
    for (const prefix of ['/ai/', '/v1/ai/']) {
      for (const operation of [
        'generate-image',
        'generate-image-stream',
        'encode-vibe',
        'upscale',
      ]) {
        const pathname = `${prefix}${operation}?contract=error`;
        const invalid = operation.startsWith('generate-image')
          ? body('invalid-model')
          : { image: 'not-an-image' };
        for (const [response, status] of [
          [await post(pathname, body(), 'invalid'), 401],
          [await post(pathname, invalid), 422],
        ]) {
          assert.equal(response.status, status);
          const error = await response.json();
          assert.equal(typeof error.message, 'string');
          assert.ok(error.message);
          assert.deepEqual(error, {
            message: error.message,
            statusCode: status,
            error: error.message,
          });
        }
      }
    }
    for (const pathname of ['/ai/generate-image-stream', '/v1/ai/generate-image-stream']) {
      const invalidStream = await post(
        pathname,
        body('nai-diffusion-4-5-full', { stream: 'invalid' }),
      );
      assert.equal(invalidStream.status, 422);
      assert.equal((await invalidStream.json()).statusCode, 422);
    }
    assert.equal(calls.length, 0);
    assert.equal((await fetchLocal('/api/member/owner/summary')).status, 401);
    for (const pathname of ['/ai/generate-image', '/v1/ai/generate-image-stream']) {
      const denied = await post(pathname, body(), upstreamToken);
      assert.equal(denied.status, 401, '上游 PST 不能作为普通成员 Key 直连生成');
      await denied.json();
    }
    assert.equal(calls.length, 0);
    for (const pathname of ['/discord', '/auth/discord/callback', '/api/carpool/teams']) {
      assert.equal((await fetchLocal(pathname)).status, 404);
    }
    const cors = await new Promise((resolve, reject) => {
      const request = http.request(
        new URL('/ai/generate-image-stream', baseUrl),
        {
          method: 'OPTIONS',
          headers: {
            host: 'gateway.example',
            origin: 'https://client.example',
            'access-control-request-method': 'POST',
            'access-control-request-headers': 'authorization,idempotency-key',
          },
        },
        (response) => {
          response.resume();
          response.on('end', () => resolve(response));
        },
      );
      request.on('error', reject);
      request.end();
    });
    assert.equal(cors.statusCode, 204);
    assert.equal(cors.headers['access-control-allow-origin'], '*');
    assert.match(cors.headers['access-control-allow-headers'], /idempotency-key/);
  });

  await t.test('网页请求原子冷存完整大提示词，202/查询/取图契约不变，正文仍限 2 MiB', async () => {
    const prompt = '合成大提示词'.repeat(7000);
    const before = calls.length;
    await new Promise((resolve) => {
      const request = http.request({
        hostname: '127.0.0.1',
        port: gatewayPort,
        path: '/api/jobs',
        method: 'POST',
        headers: {
          authorization: `Bearer ${credentials[0].api_key}`,
          'content-type': 'application/json',
        },
      });
      request.on('error', resolve);
      request.write('{"prompt":"' + 'x'.repeat(900 * 1024), () =>
        setTimeout(() => request.destroy(), 50),
      );
    });
    await new Promise((resolve) => setTimeout(resolve, 50));
    const malformed = await fetchLocal('/api/jobs', {
      method: 'POST',
      headers: {
        authorization: `Bearer ${credentials[0].api_key}`,
        'content-type': 'application/json',
      },
      body: '{"prompt":"' + 'x'.repeat(900 * 1024),
    });
    assert.equal(malformed.status, 400);
    // 原始点段路径不能绕过 route 的规范化路径所对应的载荷预算和持久父记录。
    const response = await new Promise((resolve, reject) => {
      const request = http.request(
        {
          hostname: '127.0.0.1',
          port: gatewayPort,
          path: '/x/../api/jobs',
          method: 'POST',
          headers: {
            authorization: `Bearer ${credentials[0].api_key}`,
            'content-type': 'application/json',
          },
        },
        (response) => {
          const chunks = [];
          response.on('data', (chunk) => chunks.push(chunk));
          response.on('end', () =>
            resolve({
              status: response.statusCode,
              json: () => JSON.parse(Buffer.concat(chunks).toString('utf8')),
            }),
          );
        },
      );
      request.on('error', reject);
      request.end(
        JSON.stringify({
          prompt,
          artist: 'synthetic artist',
          negative: 'no text',
          model: 'nai-diffusion-4-5-full',
          width: 1024,
          height: 1024,
          steps: 28,
          seed: 891,
          nocache: true,
        }),
      );
    });
    assert.equal(response.status, 202);
    const accepted = await response.json();
    assert.equal(accepted.prompt, prompt);
    savedImageJobId = accepted.id;
    const stored = jobs().find((job) => job.id === accepted.id);
    assert.ok(stored.request_id);
    assert.ok(Number.isSafeInteger(stored.queue_seq) && stored.queue_seq > 0);
    assert.equal(Object.hasOwn(stored, 'request'), false);
    const parent = query(
        fixture,
        'SELECT * FROM generation_requests WHERE id=?',
        stored.request_id,
      )[0],
      input = query(fixture, 'SELECT * FROM generation_inputs WHERE id=?', parent.input_id)[0];
    assert.equal(parent.member_id, credentials[0].user_id);
    const registered = events().find(
      (event) => event.eventType === 'InputRegistered' && event.payload.inputId === input.id,
    );
    assert.ok(registered);
    assert.equal(input.logical_bytes, registered.payload.logicalBytes);
    assert.equal(input.sha256, registered.payload.inputSha256);
    assert.equal(
      memberInput(parent.id, parent.member_id).input.input,
      'synthetic artist\n' + prompt,
    );
    assert.ok(!input.body.includes(credentials[0].api_key));
    let result;
    for (let attempt = 0; attempt < 200; attempt++) {
      result = await (
        await fetchLocal('/api/jobs/' + accepted.id, {
          headers: { authorization: `Bearer ${credentials[0].api_key}` },
        })
      ).json();
      if (['done', 'failed'].includes(result.status)) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    assert.equal(result.status, 'done');
    assert.equal(result.prompt, prompt);
    assert.equal(calls[before].payload.input, 'synthetic artist\n' + prompt);
    const content = await fetchLocal('/api/jobs/' + accepted.id + '/content', {
      headers: { authorization: `Bearer ${credentials[0].api_key}` },
    });
    assert.deepEqual(Buffer.from(await content.arrayBuffer()), png);
    const rejected = await post('/api/jobs', { prompt: 'x'.repeat(2 * 1024 * 1024) });
    assert.equal(rejected.status, 413);
    assert.equal(calls.length, before + 1);
  });

  await t.test('已保存图片读取失败不在成员响应中公开 filesystem 路径', async () => {
    const job = jobs().find((job) => job.id === savedImageJobId);
    assert.ok(job);
    const image = localArtifact(job.artifact_id),
      file = path.join(dataDir, image.file),
      held = file + '.held';
    await rename(file, held);
    try {
      const response = await fetchLocal('/api/jobs/' + job.id + '/content', {
        headers: { authorization: `Bearer ${credentials[0].api_key}` },
      });
      // 保留原500断言；新reader若返回503须先取证并报告，不在夹具里消红。
      assert.equal(response.status, 500);
      assert.deepEqual(await response.json(), { error: safeFailure });
    } finally {
      await rename(held, file);
    }
  });

  await t.test('图片读取保留有效签名、本人及管理员访问，拒绝其他成员绕过过期签名', async () => {
    const ownHeaders = { authorization: `Bearer ${credentials[0].api_key}` };
    const otherHeaders = { authorization: `Bearer ${credentials[1].api_key}` };
    const jobResponse = await fetchLocal('/api/jobs/' + savedImageJobId, { headers: ownHeaders });
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
    for (const pathname of [
      unsigned,
      expired.pathname + expired.search,
      forged.pathname + forged.search,
    ]) {
      assert.equal((await fetchLocal(pathname)).status, 401);
      assert.equal((await fetchLocal(pathname, { headers: otherHeaders })).status, 403);
      assert.equal((await fetchLocal(pathname, { headers: ownHeaders })).status, 200);
      assert.equal(
        (await fetchLocal(pathname, { headers: { 'x-admin-token': adminToken } })).status,
        200,
      );
    }
    assert.equal(
      (
        await fetchLocal(
          '/api/images/' +
            jobs().find((job) => job.id === savedImageJobId).artifact_id +
            '/anything',
          { headers: ownHeaders },
        )
      ).status,
      404,
    );
  });

  await t.test(
    'Full、5C 与 Medium 从两个流式路径返回可解码最终图，按实际参数只扣本人',
    async (subtest) => {
      for (const [model, pathname] of [
        ['nai-diffusion-5-full', '/ai/generate-image-stream'],
        ['nai-diffusion-5-curated', '/v1/ai/generate-image-stream'],
        ['nai-diffusion-5-full-medium', '/ai/generate-image-stream'],
      ]) {
        const live = model === 'nai-diffusion-5-full';
        let parameters = {};
        if (live) {
          parameters = { seed: 707, stream: undefined };
        } else if (model.endsWith('-curated')) {
          parameters = { stream: 'sse' };
        }
        const input = body(model, parameters),
          before = await quota(),
          other = await quota(1);
        const gate = live ? previewGate(707, 17) : null;
        if (gate) {
          subtest.after(() => gate.release());
        }
        const sse = input.parameters.stream !== 'msgpack';
        const response = await post(pathname, input, credentials[0].api_key, {
          accept: sse ? 'application/msgpack' : 'text/event-stream',
        });
        assert.equal(response.status, 200);
        assert.equal(
          response.headers.get('content-type'),
          sse ? 'text/event-stream' : 'application/msgpack',
        );
        assert.equal(response.headers.get('x-nai-image-count'), null);
        assert.equal(response.headers.get('x-nai-partial'), null);
        const reader = streamEvents(response);
        subtest.after(() => reader.return());
        if (gate) {
          const { value: preview, done } = await reader.next();
          assert.equal(done, false);
          assert.equal(preview.event_type, 'intermediate');
          assert.equal(preview.step_ix, 17);
          assert.equal(preview.samp_ix, 0);
          assert.deepEqual(Buffer.from(preview.image), png);
          assert.equal(gate.written, true);
          assert.equal(gate.released, false);
          assert.equal(calls.at(-1).finishedAt, undefined, '预览必须在上游完成前到达');
          gate.release();
        }
        const { value: event, done } = await reader.next();
        assert.equal(done, false);
        assert.equal(event.event_type, 'final');
        assert.equal(event.samp_ix, 0);
        assert.deepEqual(Buffer.from(event.image), png);
        assert.equal((await reader.next()).done, true);
        const cost = quoteFairRequest(parseNativeImageRequest(input));
        near(before.v5Available - (await quota()).v5Available, cost.amount);
        assert.equal((await quota()).anlasFixedAvailable, before.anlasFixedAvailable);
        const unchanged = await quota(1);
        for (const field of [
          'v5Available',
          'v5Capacity',
          'v5Reserved',
          'anlasFixedAvailable',
          'anlasPurchasedAvailable',
          'anlasReserved',
          'share',
        ]) {
          assert.equal(unchanged[field], other[field], field);
        }
        assert.equal(calls.at(-1).payload.model, model);
        assert.equal(calls.at(-1).payload.parameters.steps, model.endsWith('-medium') ? 14 : 23);
      }
    },
  );

  await t.test('OpenAI 模型目录、非流式图片领取与流式结束契约通过真实 HTTP 验证', async () => {
    const model = 'nai-diffusion-4-5-full:k_euler_ancestral';
    const content =
      '提示词: a blue paper boat\n画师串:\n尺寸: 方图\n提示词引导值: 5\n缩放引导值: 0\n负面提示词:';
    const request = { model, messages: [{ role: 'user', content }] };
    const startCount = calls.length,
      before = await quota();
    for (const response of [
      await fetchLocal('/v1/models'),
      await post('/v1/chat/completions', request, 'invalid'),
    ]) {
      assert.equal(response.status, 401);
      const failure = await response.json();
      assert.equal(failure.error.type, 'authentication_error');
      assert.equal(failure.error.code, 'authentication_error');
      assert.equal(failure.error.param, null);
    }
    assert.equal(calls.length, startCount);
    const models = await fetchLocal('/v1/models', {
      headers: { authorization: `Bearer ${credentials[0].api_key}` },
    });
    assert.equal(models.status, 200);
    const catalog = await models.json();
    assert.equal(catalog.object, 'list');
    assert.ok(catalog.data.some((item) => item.id === model && item.object === 'model'));

    const completion = await post('/v1/chat/completions', request);
    assert.equal(completion.status, 200, await completion.clone().text());
    assert.match(completion.headers.get('content-type'), /application\/json/);
    const message = await completion.json();
    assert.equal(message.object, 'chat.completion');
    assert.equal(message.model, model);
    assert.equal(message.choices[0].message.role, 'assistant');
    assert.equal(message.choices[0].finish_reason, 'stop');
    const claimImage = async (markdown) => {
      const match = markdown.match(/!\[image\]\((https?:\/\/[^)]+)\)/);
      assert.ok(match, '生成结果必须包含可领取的 Markdown 图片链接');
      const imageUrl = new URL(match[1]);
      assert.equal(imageUrl.origin, baseUrl);
      const image = await fetchLocal(imageUrl.pathname + imageUrl.search);
      assert.equal(image.status, 200);
      assert.match(image.headers.get('content-type'), /image\/png/);
      assert.deepEqual(Buffer.from(await image.arrayBuffer()), png);
    };
    await claimImage(message.choices[0].message.content);

    const streamed = await post('/v1/chat/completions', { ...request, stream: true });
    assert.equal(streamed.status, 200);
    assert.match(streamed.headers.get('content-type'), /text\/event-stream/);
    const events = (await streamed.text())
      .split('\n\n')
      .filter((event) => event.startsWith('data: '))
      .map((event) => event.slice(6));
    assert.equal(events.at(-1), '[DONE]');
    const chunks = events.slice(0, -1).map((event) => JSON.parse(event));
    assert.ok(chunks.length >= 3);
    assert.ok(
      chunks.every((chunk) => chunk.object === 'chat.completion.chunk' && chunk.model === model),
    );
    assert.ok(chunks.every((chunk) => chunk.id === chunks[0].id));
    assert.deepEqual(chunks.at(-1).choices, [{ index: 0, delta: {}, finish_reason: 'stop' }]);
    assert.ok(chunks.slice(0, -1).every((chunk) => chunk.choices[0].finish_reason === null));
    await claimImage(chunks.map((chunk) => chunk.choices[0].delta.content || '').join(''));
    assert.equal(calls.length - startCount, 2);
    for (const call of calls.slice(startCount)) {
      assert.equal(call.payload.model, 'nai-diffusion-4-5-full');
      assert.equal(call.payload.parameters.steps, 28);
      assert.equal(call.payload.parameters.n_samples, 1);
      assert.equal(call.payload.parameters.width, 1024);
      assert.equal(call.payload.parameters.height, 1024);
    }
    const after = await quota();
    for (const field of ['v5Available', 'anlasFixedAvailable', 'anlasPurchasedAvailable']) {
      assert.equal(after[field], before[field]);
    }
  });

  await t.test(
    '首图失败保留HTTP错误；批次final先结算，次图预览后断流返回error并退款',
    async (subtest) => {
      mock.scenario = 'single failure then partial batch';
      let before = await quota();
      mock.failAt = calls.length + 1;
      const failed = await post('/ai/generate-image', body(), credentials[0].api_key, {
        accept: 'application/json',
        'Idempotency-Key': legacyKey,
      });
      assert.equal(failed.status, 500);
      assert.match(failed.headers.get('content-type'), /application\/json/);
      const failure = await failed.json();
      assert.ok(failure.error);
      assert.equal(failure.images, undefined);
      assert.deepEqual(failure, { message: failure.error, statusCode: 500, error: failure.error });
      assert.equal(failure.message, safeFailure);
      const failedJob = jobs().find(
        (job) => job.request_id === failed.headers.get('x-nai-request-id'),
      );
      assert.ok(failedJob);
      failedReplayJobId = failedJob.id;
      assert.ok(diagnostic(failedJob.id).includes(privateMarker));
      const failedRequest = await (
        await fetchLocal('/api/requests/' + failedJob.request_id, {
          headers: { authorization: `Bearer ${credentials[0].api_key}` },
        })
      ).json();
      assert.equal(failedRequest.jobs[0].error, safeFailure);
      near((await quota()).v5Available, before.v5Available);
      before = await quota();
      mock.failAt = -1;
      const beforeCalls = calls.length,
        gate = previewGate(801, 9, true);
      subtest.after(() => gate.release());
      const response = await post(
        '/v1/ai/generate-image-stream',
        body('nai-diffusion-5-curated', { steps: 29, n_samples: 3, seed: 800, stream: 'sse' }),
      );
      assert.equal(response.status, 200);
      assert.equal(response.headers.get('x-nai-partial'), null);
      assert.equal(response.headers.get('x-nai-image-count'), null);
      const reader = streamEvents(response),
        first = (await reader.next()).value;
      subtest.after(() => reader.return());
      assert.equal(first.event_type, 'final');
      assert.equal(first.samp_ix, 0);
      assert.deepEqual(Buffer.from(first.image), png);
      const batchRows = () =>
        fixture
          .jobs()
          .filter((job) => job.request_id === response.headers.get('x-nai-request-id'))
          .sort((a, b) => a.sample_index - b.sample_index);
      let batchJobs = batchRows();
      assert.equal(batchJobs[0].status, 'done');
      assert.ok(batchJobs[0].artifact_id);
      assert.equal(charge(batchJobs[0].id).status, 'settled');
      assert.notEqual(batchJobs[1].status, 'done');
      assert.equal(calls.length, beforeCalls + 1, '首个final不能等第二张派发才发送');
      const preview = (await reader.next()).value;
      assert.equal(preview.event_type, 'intermediate');
      assert.equal(preview.samp_ix, 1);
      assert.equal(preview.step_ix, 9);
      assert.deepEqual(Buffer.from(preview.image), png);
      assert.equal(gate.released, false);
      assert.equal(gate.written, true);
      gate.release();
      const failureFrame = (await reader.next()).value;
      assert.equal(failureFrame.event_type, 'error');
      assert.equal(failureFrame.samp_ix, 1);
      assert.ok(failureFrame.message);
      assert.equal(failureFrame.code, '500');
      assert.equal((await reader.next()).done, true);
      batchJobs = batchRows();
      assert.deepEqual(
        batchJobs.map((job) => job.status),
        ['done', 'failed', 'failed'],
      );
      assert.equal(charge(batchJobs[1].id).status, 'refunded');
      assert.equal(batchJobs[2].attempt_id, null);
      assert.equal(before.anlasFixedAvailable - (await quota()).anlasFixedAvailable, 32);
      assert.equal(calls.length, beforeCalls + 2);
    },
  );

  await t.test('实际原失败在查询、错误图片和幂等重放时不公开内部路径，管理员仍能诊断', async () => {
    const before = calls.length,
      jobId = failedReplayJobId,
      headers = { authorization: `Bearer ${credentials[0].api_key}` },
      originalDetail = diagnostic(jobId);
    const snapshot = await (await fetchLocal('/api/jobs/' + jobId, { headers })).json();
    assert.equal(snapshot.error, safeFailure);
    const image = await fetchLocal('/api/jobs/' + jobId + '/content', { headers });
    assert.equal(image.status, 200);
    assert.equal(image.headers.get('x-error'), '1');
    const svg = await image.text();
    assert.ok(!svg.includes(privateMarker));
    assert.ok(!svg.includes(dataDir));
    const replay = await post('/ai/generate-image-stream', body(), credentials[0].api_key, {
      'Idempotency-Key': legacyKey,
    });
    assert.equal(replay.status, 500);
    assert.deepEqual(await replay.json(), {
      message: safeFailure,
      statusCode: 500,
      error: safeFailure,
    });
    assert.equal(calls.length, before);
    assert.equal(diagnostic(jobId), originalDetail, '公共读取不能重写原保护诊断');
    const persisted = jobs().find((job) => job.id === jobId),
      summary = await (
        await fetchLocal('/api/admin/summary', { headers: { 'x-admin-token': adminToken } })
      ).json(),
      item = summary.errorLogs.find((entry) => entry.id === jobId);
    assert.ok(item?.errorDetail.includes(privateMarker));
    for (const field of ['retryCount', 'cost', 'accountCost', 'queueTotal']) {
      assert.equal(persisted[field], undefined, '该存储角色没有原字段');
      assert.equal(item[field], null, `${field}不能在HTTP读取时补零`);
    }
    const input = inputRequest(jobId);
    assert.equal(item.request.requestedSteps, input.requestedSteps ?? null);
    assert.equal(item.request.routedSteps, input.steps);
    assert.equal(item.completedAt, new Date(persisted.completed_at).toISOString());
  });

  await t.test(
    'SSE 部分失败和同键 JSON 重放只公开分类文案，原错误帧留作管理员诊断',
    async (subtest) => {
      mock.scenario = 'private upstream stream error';
      const before = await quota(),
        startCount = calls.length,
        key = 'private-stream-error';
      const input = body('nai-diffusion-5-curated', {
        steps: 29,
        n_samples: 2,
        seed: 810,
        stream: 'sse',
      });
      const gate = previewGate(811, 3);
      gate.errorMessage = JSON.stringify({ diagnostic: privateMarker, credential: upstreamToken });
      subtest.after(() => gate.release());
      const response = await post('/ai/generate-image-stream', input, credentials[0].api_key, {
        'Idempotency-Key': key,
      });
      assert.equal(response.status, 200);
      const reader = streamEvents(response);
      subtest.after(() => reader.return());
      assert.equal((await reader.next()).value.event_type, 'final');
      assert.equal((await reader.next()).value.event_type, 'intermediate');
      gate.release();
      const error = (await reader.next()).value;
      assert.deepEqual(error, {
        event_type: 'error',
        samp_ix: 1,
        message: safeFailure,
        code: '500',
      });
      assert.equal((await reader.next()).done, true);
      const jobs = fixture
        .jobs()
        .filter((job) => job.request_id === response.headers.get('x-nai-request-id'))
        .sort((a, b) => a.sample_index - b.sample_index);
      assert.deepEqual(
        jobs.map((job) => job.status),
        ['done', 'failed'],
      );
      assert.ok(diagnostic(jobs[1].id).includes(privateMarker));
      const headers = { authorization: `Bearer ${credentials[0].api_key}` };
      const snapshot = await (await fetchLocal('/api/jobs/' + jobs[1].id, { headers })).json();
      assert.equal(snapshot.error, safeFailure);
      const batch = await (
        await fetchLocal('/api/requests/' + jobs[1].request_id, { headers })
      ).json();
      assert.equal(batch.jobs[1].error, safeFailure);
      const replay = await post('/v1/ai/generate-image', input, credentials[0].api_key, {
        'Idempotency-Key': key,
        accept: 'application/json',
      });
      assert.equal(replay.status, 200);
      const result = await replay.json();
      assert.equal(result.completed, 1);
      assert.equal(result.results[1].message, safeFailure);
      assert.deepEqual(Buffer.from(result.images[0].image, 'base64'), png);
      assert.equal(calls.length, startCount + 2);
      assert.equal(before.anlasFixedAvailable - (await quota()).anlasFixedAvailable, 32);
      const charges = jobs.map((job) => charge(job.id));
      assert.deepEqual(
        charges.map((charge) => charge.status),
        ['settled', 'refunded'],
      );
    },
  );

  await t.test(
    '两位成员同时生图，第一张超过十秒时第二张仍排队，完整结束后才接续',
    async (subtest) => {
      mock.scenario = 'two members with held first generation';
      const before = [await quota(), await quota(1)],
        startCount = calls.length;
      mock.holdNextMs = 11600;
      mock.maxActiveGenerations = 0;
      const firstBody = body('nai-diffusion-5-full', { seed: 9001, stream: 'sse' }),
        key = 'live-preview-disconnect-replay';
      const gate = previewGate(9001, 11);
      subtest.after(() => gate.release());
      const firstPending = post('/ai/generate-image-stream', firstBody, credentials[0].api_key, {
        'Idempotency-Key': key,
      });
      // HTTP 到达顺序不由调用顺序保证；先确认长请求占槽，再验证第二位成员排队。
      await gate.received;
      const secondPending = post('/v1/ai/generate-image-stream', body(), credentials[1].api_key);
      secondPending.catch(() => {}); // 首请求的断言先失败时，仍避免并发请求产生未处理拒绝；下方 await 会继续传播错误。
      const firstResponse = await firstPending,
        firstReader = streamEvents(firstResponse);
      subtest.after(() => firstReader.return());
      assert.equal(firstResponse.status, 200);
      const preview = (await firstReader.next()).value;
      assert.equal(preview.event_type, 'intermediate');
      assert.equal(preview.step_ix, 11);
      assert.equal(gate.released, false);
      assert.equal(calls.length, startCount + 1);
      await firstReader.return(); // 关闭浏览器连接，随后才让上游完成。
      gate.release();
      const secondResponse = await secondPending;
      // 复用已结算结果：暂停真实客户端读取，再在大final中途断开，不新增生成调用。
      await new Promise((resolve, reject) => {
        const request = http.request(
          baseUrl + '/ai/generate-image-stream',
          {
            method: 'POST',
            headers: {
              authorization: `Bearer ${credentials[0].api_key}`,
              'content-type': 'application/json',
              'Idempotency-Key': key,
            },
          },
          (response) => {
            try {
              assert.equal(response.statusCode, 200);
              assert.equal(response.headers['content-type'], 'text/event-stream');
            } catch (error) {
              response.destroy();
              reject(error);
              return;
            }
            response.on('error', () => {});
            response.once('data', (chunk) => {
              response.pause();
              assert.ok(chunk.length < largePng.length);
              setTimeout(() => {
                response.destroy();
                resolve();
              }, 100);
            });
          },
        );
        request.on('error', reject);
        request.setTimeout(5000, () => request.destroy(new Error('synthetic slow SSE timeout')));
        request.end(JSON.stringify(firstBody));
      });
      const completeSse = await post(
        '/v1/ai/generate-image-stream',
        firstBody,
        credentials[0].api_key,
        { 'Idempotency-Key': key },
      );
      const completeFrames = readNativeSseEvents(Buffer.from(await completeSse.arrayBuffer()));
      assert.equal(completeFrames.length, 1);
      assert.equal(completeFrames[0].event_type, 'final');
      assert.deepEqual(Buffer.from(completeFrames[0].image, 'base64'), largePng);
      const replay = await post(
        '/ai/generate-image-stream',
        { ...firstBody, parameters: { ...firstBody.parameters, stream: 'msgpack' } },
        credentials[0].api_key,
        { 'Idempotency-Key': key },
      );
      assert.equal(
        replay.headers.get('x-nai-request-id'),
        firstResponse.headers.get('x-nai-request-id'),
      );
      for (const response of [secondResponse, replay]) {
        assert.equal(response.status, 200, await response.clone().text());
        const final = readNativeEvents(Buffer.from(await response.arrayBuffer())).find(
          (event) => event.event_type === 'final',
        );
        assert.ok(final);
        assert.deepEqual(Buffer.from(final.image), response === replay ? largePng : png);
      }
      assert.equal(mock.maxActiveGenerations, 1);
      assert.equal(calls.length - startCount, 2);
      assert.ok(calls[startCount + 1].startedAt - calls[startCount].finishedAt >= 10000);
      const cost = quoteFairRequest(parseNativeImageRequest(body()));
      for (let member = 0; member < 2; member++) {
        near(before[member].v5Available - (await quota(member)).v5Available, cost.amount);
      }
    },
  );

  await t.test('成员上限与原子停止接单：排队取消，在途结算，恢复不续旧批次', async () => {
    mock.scenario = 'admission limit and member stop';
    const manage = (value) =>
      fetchLocal('/api/admin/member-management', {
        method: 'POST',
        headers: {
          'x-admin-token': adminToken,
          'content-type': 'application/json',
          origin: baseUrl,
        },
        body: JSON.stringify(value),
      });
    const memberId = credentials[0].user_id;
    const startCount = calls.length,
      before = await quota();
    mock.holdNextMs = 3000;
    const batch = post('/ai/generate-image-stream', body('nai-diffusion-5-full', { n_samples: 3 }));
    const startedDeadline = Date.now() + 20000;
    while (calls.length === startCount && Date.now() < startedDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    assert.equal(calls.length, startCount + 1);
    const queued = [];
    const enqueue = (seed) =>
      post('/api/jobs', {
        prompt: 'synthetic queue admission',
        model: 'nai-diffusion-5-full',
        width: 1024,
        height: 1024,
        steps: 23,
        seed,
        nocache: true,
      });
    for (let index = 0; index < 1; index++) {
      const response = await enqueue(900 + index);
      assert.equal(response.status, 202, await response.clone().text());
      queued.push((await response.json()).id);
    }
    const rejected = await enqueue(904);
    assert.equal(rejected.status, 429);
    assert.equal(typeof (await rejected.json()).error, 'string');
    const stopped = await manage({
      action: 'member-admission',
      userId: memberId,
      acceptingJobs: false,
    });
    assert.equal(stopped.status, 200);
    assert.equal((await stopped.json()).cancelledCount, 3);
    const me = await fetchLocal('/api/member/me', { headers: { cookie: cookies[0] } });
    assert.equal(me.status, 200);
    assert.equal((await me.json()).acceptingJobs, false);
    const denied = await post('/ai/generate-image', body());
    assert.equal(denied.status, 403);
    assert.equal(typeof (await denied.json()).message, 'string');
    assert.equal(
      (await manage({ action: 'member-admission', userId: memberId, acceptingJobs: true })).status,
      200,
    );
    const result = await batch;
    assert.equal(result.status, 200);
    assert.equal(result.headers.get('x-nai-image-count'), null);
    assert.equal(result.headers.get('x-nai-partial'), null);
    assert.deepEqual(
      readNativeEvents(Buffer.from(await result.arrayBuffer())).map((event) => event.event_type),
      ['final', 'error'],
    );
    assert.equal(calls.length, startCount + 1);
    near(
      before.v5Available - (await quota()).v5Available,
      quoteFairRequest(parseNativeImageRequest(body())).amount,
    );
    for (const id of queued) {
      const response = await fetchLocal('/api/jobs/' + id, {
        headers: { authorization: `Bearer ${credentials[0].api_key}` },
      });
      assert.equal(response.status, 200);
      assert.equal((await response.json()).status, 'failed');
    }
    // 恢复只接受新请求；立即再停止清理这个新排队任务，避免发出额外上游生成。
    assert.equal((await enqueue(905)).status, 202);
    const cleaned = await manage({
      action: 'member-admission',
      userId: memberId,
      acceptingJobs: false,
    });
    assert.equal((await cleaned.json()).cancelledCount, 1);
    assert.equal(
      (await manage({ action: 'member-admission', userId: memberId, acceptingJobs: true })).status,
      200,
    );
  });

  await t.test('上游并发锁返回 429 与重试时间，预扣退回且不自动重发', async () => {
    mock.scenario = 'upstream capacity rejection';
    const before = await quota();
    mock.failStatus = 429;
    mock.failAt = calls.length + 1;
    const response = await post('/ai/generate-image-stream', body());
    assert.equal(response.status, 429);
    assert.equal(response.headers.get('retry-after'), '30');
    const error = await response.json();
    assert.deepEqual(error, { message: error.error, statusCode: 429, error: error.error });
    near((await quota()).v5Available, before.v5Available);
    assert.equal(calls.length, mock.failAt);
  });

  assert.equal(sharedAccountConcurrency(), 1);
  assert.equal(mock.maxActiveGenerations, 1);
  const snapshot = jobs(),
    { audit, settlements } = checkAccounting(fixture, snapshot);
  assert.ok(settlements.some((item) => item.status === 'settled'));
  assert.ok(settlements.some((item) => item.status === 'refunded'));
  assert.ok(
    audit.some(
      (event) =>
        event.eventType === 'ReviewOpened' && event.payload.response.upstreamHttpStatus === 200,
    ),
  );
  const batch = snapshot.find((job) => job.sample_index === 1 && job.charge_status === 'refunded');
  assert.ok(batch?.request_id);
  assert.ok(
    snapshot.some(
      (job) =>
        job.request_id === batch.request_id && job.sample_index === 0 && job.status === 'done',
    ),
  );

  // 两个正常请求真实受理于持久容量冷却期间；旧parentless/逆order_value源另归档，不能手造等价新父。
  const request = {
    tag: 'restart queued request',
    prompt: 'restart queued request',
    artist: '',
    negative: '',
    model: 'nai-diffusion-4-5-full',
    width: 1024,
    height: 1024,
    steps: 28,
    seed: 901,
    sampler: 'k_euler_ancestral',
    scale: 6,
    cfg: 0,
    noiseSchedule: 'karras',
    nocache: true,
  };
  const restarting = [];
  for (const seed of [901, 902]) {
    const response = await post('/api/jobs', { ...request, seed });
    assert.equal(response.status, 202, await response.clone().text());
    const accepted = await response.json(),
      job = jobs().find((row) => row.id === accepted.id);
    assert.equal(job.status, 'queued');
    assert.equal(job.source, 'web');
    assert.equal(job.member_id, credentials[0].user_id);
    assert.ok(Number.isSafeInteger(job.queue_seq) && job.queue_seq > 0);
    assert.equal(memberInput(job.request_id, job.member_id).input.input, 'restart queued request');
    restarting.push(job);
  }
  assert.ok(restarting[0].queue_seq < restarting[1].queue_seq);
  await stop();
  assertChildPublicReplay(fixture);
  await t.test('独立进程重启后继续两个正常冷存网页任务，旧parentless源另归档', async () => {
    const before = calls.length;
    const { cooldown_until: cooldownUntil } = query(
      fixture,
      'SELECT cooldown_until FROM upstream_accounts WHERE id=?',
      fixture.accountId,
    )[0];
    assert.ok(Number.isSafeInteger(cooldownUntil));
    // 原容量锁的真实30s冷却自然到期；不清投影、不伪clock，仍在原240s总限内准备重启。
    await new Promise((resolve) => setTimeout(resolve, Math.max(0, cooldownUntil - Date.now())));
    await fixture.reopen();
    try {
      for (const original of restarting) {
        let job;
        for (let attempt = 0; attempt < 400; attempt++) {
          job = await (
            await fetchLocal('/api/jobs/' + original.id, {
              headers: { authorization: `Bearer ${credentials[0].api_key}` },
            })
          ).json();
          if (['done', 'failed'].includes(job.status)) {
            break;
          }
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
        assert.equal(job.status, 'done', JSON.stringify(job));
        assert.equal(job.prompt, 'restart queued request');
        assert.equal(jobs().find((row) => row.id === original.id).queue_seq, original.queue_seq);
      }
      assert.deepEqual(
        calls.slice(before).map((call) => call.payload.parameters.seed),
        [901, 902],
      );
      assert.equal(calls.length, 17, '正常主流程保持原17次上游生成；归档与读取不能额外生成');
    } finally {
      await stop();
    }
    assertChildPublicReplay(fixture);
  });
});

test('上游 200 返回 HTML 也按失败退款，不保存为成功图片', async (subtest) => {
  const fixture = await nativeFixture(subtest);
  const { mock, post, quota } = fixture;
  const before = await quota();
  mock.malformedNext = true;
  const response = await post('/ai/generate-image', body());
  assert.ok(response.status >= 400);
  assert.equal((await quota()).v5Available, before.v5Available);
  const { job, audit } = checkSingleGeneration(fixture, 'failed', 'refunded');
  assert.ok(!job.artifact_id);
  assert.ok(
    audit.some(
      (event) =>
        event.eventType === 'ReviewOpened' && event.payload.response.upstreamHttpStatus === 200,
    ),
  );
  await fixture.stop();
  assertChildPublicReplay(fixture);
});

test('direct 生图仍返回原图片，实际派发使用持久排队序号', async (subtest) => {
  const fixture = await nativeFixture(subtest);
  const { credentials, fetchLocal } = fixture;
  const query = new URLSearchParams({
    token: credentials[0].api_key,
    prompt: 'direct queue sequence',
    artist: '',
    model: 'nai-diffusion-4-5-full',
    width: '1024',
    height: '1024',
    steps: '28',
    seed: '893',
    nocache: '1',
  });
  const response = await fetchLocal('/generate?' + query);
  assert.equal(response.status, 200);
  assert.deepEqual(Buffer.from(await response.arrayBuffer()), png);
  checkSingleGeneration(fixture, 'done', 'settled');
  const row = fixture.jobs().find((job) => job.source === 'direct');
  assert.ok(Number.isSafeInteger(row.queue_seq) && row.queue_seq > 0);
  assert.equal(
    fixture
      .events()
      .find((event) => event.eventType === 'JobAccepted' && event.payload.jobId === row.id).payload
      .queueSeq,
    String(row.queue_seq),
  );
  await fixture.stop();
  assertChildPublicReplay(fixture);
});

test('桌面客户端 multipart 别名与普通 ZIP 接口兼容', async (subtest) => {
  const fixture = await nativeFixture(subtest);
  const { credentials, fetchLocal, calls, quota } = fixture;
  const input = body('nai-diffusion-4-5-full', { uc_preset: 0, quality_toggle: false });
  const form = new FormData();
  form.append(
    'request',
    new Blob([JSON.stringify(input)], { type: 'application/json' }),
    'request.json',
  );
  const before = await quota();
  const response = await fetchLocal('/v1/ai/generate-image', {
    method: 'POST',
    headers: { authorization: `Bearer ${credentials[0].api_key}` },
    body: form,
  });
  assert.equal(response.status, 200);
  assert.match(response.headers.get('content-type'), /application\/zip/);
  assert.equal(Buffer.from(await response.arrayBuffer()).readUInt32LE(0), 0x04034b50);
  assert.equal(calls.at(-1).payload.parameters.ucPreset, 0);
  assert.equal(calls.at(-1).payload.parameters.qualityToggle, false);
  assert.equal(calls.at(-1).payload.parameters.uc_preset, undefined);
  assert.equal((await quota()).anlasFixedAvailable, before.anlasFixedAvailable);
  checkSingleGeneration(fixture, 'done', 'settled');
  await fixture.stop();
  assertChildPublicReplay(fixture);
});
