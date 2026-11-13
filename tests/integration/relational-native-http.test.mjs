import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createServer, request as httpRequest } from 'node:http';
import { EventEmitter, once } from 'node:events';
import path from 'node:path';
import { encode } from '@msgpack/msgpack';
import { fixture, NOW, image, databaseState } from '../helpers/relational-generation.mjs';
import { readNativeEvents, readNativeSseEvents } from '../helpers/native-stream-client.mjs';
import { createRelationalNativeHttp } from '../../server/generation/relational-native-http.ts';
import { createRelationalQueueRuntime } from '../../server/generation/relational-queue-runtime.ts';
import { createGenerationPayloadBudget } from '../../server/generation/payload-budget.ts';
import { nativeGenerationError } from '../../server/protocols/native-http.ts';
import { JobPreviews } from '../../server/generation/job-previews.ts';
import { cooldownRetryAfter } from '../../server/quota/shared-capacity.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { relationalSnapshotSha256 } from '../../server/storage/relational-private-restore.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import {
  authenticateMemberApiKey,
  createMemberSession,
  authenticateMemberSession,
} from '../../server/members/authentication.ts';
import { NATIVE_DEFAULTS } from '../../server/protocols/native-image.ts';
import { R2Storage } from '../../server/storage/r2-storage.ts';
import { copyRelationalArtifactReplica } from '../../server/storage/relational-replica-io.ts';
import { rename } from 'node:fs/promises';

const keys = { m1: 'STA1N-relational-native-m1', m2: 'STA1N-relational-native-m2' };
const input = (parameters = {}, root = {}) => ({
  input: 'synthetic private native prompt',
  model: 'nai-diffusion-4-5-full',
  action: 'generate',
  parameters: { width: 128, height: 128, steps: 30, seed: -1, ...parameters },
  ...root,
});
async function waitFor(predicate) {
  const deadline = Date.now() + 5000;
  while (!predicate()) {
    assert.ok(Date.now() < deadline, 'real Native HTTP/queue condition did not complete');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}
async function setup(t, options = {}) {
  const cleanup = [],
    f = await fixture({ after: (fn) => cleanup.push(fn) }, options);
  await f.accept('existing', { steps: options.fixed === '0' ? 23 : 30 });
  f.dispatch('existing-0', NOW);
  await f.publish('existing-0');
  f.db.transaction(() => {
    for (const [id, key] of Object.entries(keys)) {
      f.members.replaceMemberApiKey(id, key);
    }
  })();
  const sourcePath = path.join(f.dir, 'native-source.sqlite');
  await f.db.backup(sourcePath);
  const source = new Database(sourcePath);
  source.pragma('journal_mode=DELETE');
  source.close();
  await verifyAndInstallRelationalStorage(f.db, {
    sourcePath,
    sourceSha256: relationalSnapshotSha256(sourcePath),
    historyId: 'generation-history',
    finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get()
      .seq,
    replicas: { dataDir: f.dir },
  });
  let clock = NOW + 10010;
  const upstream = { calls: [], handler: null, release: null };
  const upstreamServer = createServer(async (req, res) => {
    assert.equal(f.db.inTransaction, false);
    const chunks = [];
    for await (const chunk of req) {
      chunks.push(chunk);
    }
    const body = JSON.parse(Buffer.concat(chunks).toString());
    upstream.calls.push({ url: req.url, body, authorization: req.headers.authorization });
    if (upstream.handler) {
      await upstream.handler(req, res, body);
      return;
    }
    if (req.url.includes('encode-vibe')) {
      res.writeHead(200, { 'content-type': 'application/octet-stream' });
      res.end(Buffer.alloc(200, 0x80));
    } else {
      res.writeHead(200, { 'content-type': 'image/webp' });
      res.end(image);
    }
  });
  upstreamServer.listen(0, '127.0.0.1');
  await once(upstreamServer, 'listening');
  const env = {
    NOVELAI_API_URL: `http://127.0.0.1:${upstreamServer.address().port}`,
    NOVELAI_STREAM_IMAGE: options.stream ? 'true' : 'false',
    MOCK_WHEN_NO_ACCOUNT: 'false',
  };
  const events = new EventEmitter(),
    previews = new JobPreviews(),
    controls = new Map();
  const budget = createGenerationPayloadBudget(() =>
    f.inputs.activePayloadBytes(controls.values()),
  );
  const errors = [],
    runtime = createRelationalQueueRuntime({
      sqlite: f.db,
      generations: f.generations,
      inputs: f.inputs,
      dataDir: f.dir,
      controls,
      events,
      previews,
      env,
      now: () => clock,
      onError: (event) => errors.push(event),
    });
  const replicas = { dataDir: f.dir };
  const handle = createRelationalNativeHttp({
    sqlite: f.db,
    events,
    previews,
    payloadBudget: budget,
    replicas,
    env,
    now: () => clock,
    schedule: runtime.schedule,
    timeoutMs: options.timeoutMs ?? 60000,
    logDiagnostic() {},
    readJson: async (req, maxBytes) => {
      const chunks = [];
      let size = 0;
      for await (const chunk of req) {
        size += chunk.length;
        if (size > maxBytes) {
          throw Object.assign(new Error('请求体超过大小限制'), { statusCode: 413 });
        }
        req.generationPayloadClaim?.resize(size);
        chunks.push(chunk);
      }
      try {
        return JSON.parse(Buffer.concat(chunks).toString());
      } catch {
        throw Object.assign(new Error('invalid JSON body.'), { statusCode: 400 });
      }
    },
    sendImage: (res, status, mimeType, buffer, headers) => {
      res.writeHead(status, {
        'content-type': mimeType,
        'content-length': buffer.length,
        ...headers,
      });
      res.end(buffer);
    },
  });
  await runtime.start();
  const server = createServer(async (req, res) => {
    try {
      if (!(await handle(req, res, new URL(req.url, `http://${req.headers.host}`)))) {
        res.writeHead(404);
        res.end();
      }
    } catch (error) {
      if (res.destroyed || res.writableEnded) {
        return;
      }
      if (res.headersSent) {
        res.destroy();
        return;
      }
      const payload = nativeGenerationError(error),
        retryAfter = cooldownRetryAfter(error.cooldownUntil, clock);
      res.writeHead(payload.statusCode, {
        'content-type': 'application/json; charset=utf-8',
        ...(retryAfter ? { 'retry-after': String(retryAfter) } : {}),
        ...(error.requestId ? { 'x-nai-request-id': error.requestId } : {}),
      });
      res.end(JSON.stringify(payload.body));
    }
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const origin = `http://127.0.0.1:${server.address().port}`;
  t.after(async () => {
    upstream.release?.();
    server.closeAllConnections();
    upstreamServer.closeAllConnections();
    await runtime.stop();
    await Promise.all([
      new Promise((resolve) => server.close(resolve)),
      new Promise((resolve) => upstreamServer.close(resolve)),
    ]);
    for (const run of cleanup) {
      await run();
    }
  });
  const call = async (
    body = input(),
    {
      endpoint = '/ai/generate-image',
      member = 'm1',
      key,
      accept = 'application/json',
      headers = {},
      ...rest
    } = {},
  ) => {
    const response = await fetch(origin + endpoint, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${keys[member]}`,
        ...(body instanceof FormData ? {} : { 'content-type': 'application/json' }),
        accept,
        ...(key === undefined ? {} : { 'idempotency-key': key }),
        ...headers,
      },
      body: body instanceof FormData ? body : JSON.stringify(body),
      ...rest,
    });
    return {
      status: response.status,
      headers: response.headers,
      buffer: Buffer.from(await response.arrayBuffer()),
    };
  };
  return {
    ...f,
    server,
    origin,
    runtime,
    events,
    controls,
    budget,
    previews,
    replicas,
    upstream,
    errors,
    call,
    advance(ms = 10001) {
      clock += ms;
      runtime.tick();
    },
    get clock() {
      return clock;
    },
  };
}
function json(response) {
  return JSON.parse(response.buffer.toString());
}
function zipFiles(bytes) {
  const files = new Map();
  let offset = 0;
  while (bytes.readUInt32LE(offset) === 0x04034b50) {
    const length = bytes.readUInt32LE(offset + 18),
      nameLength = bytes.readUInt16LE(offset + 26),
      extraLength = bytes.readUInt16LE(offset + 28),
      start = offset + 30 + nameLength + extraLength;
    files.set(
      bytes.toString('utf8', offset + 30, offset + 30 + nameLength),
      bytes.subarray(start, start + length),
    );
    offset = start + length;
  }
  return files;
}

test('合法ready Native HTTP 真Key受理、实际队列/文件领取与同键随机重放，无键每次新意图', async (t) => {
  const f = await setup(t),
    body = input();
  const [first, concurrent] = await Promise.all([
    f.call(body, { key: 'same' }),
    f.call(body, { key: 'same' }),
  ]);
  assert.equal(first.status, 200, first.buffer.toString());
  assert.equal(concurrent.status, 200, concurrent.buffer.toString());
  assert.equal(concurrent.headers.get('x-nai-request-id'), first.headers.get('x-nai-request-id'));
  assert.deepEqual(concurrent.buffer, first.buffer);
  const id = first.headers.get('x-nai-request-id'),
    value = json(first);
  assert.deepEqual(Buffer.from(value.images[0].image, 'base64'), image);
  assert.ok(Number.isInteger(value.images[0].seed));
  const before = databaseState(f.db),
    replay = await f.call(body, { key: 'same', accept: 'application/zip' });
  assert.equal(replay.status, 200);
  assert.equal(replay.headers.get('x-nai-request-id'), id);
  assert.deepEqual(zipFiles(replay.buffer).get('image_0.webp'), image);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.upstream.calls.length, 1);
  const conflict = await f.call(input({}, { input: 'changed' }), { key: 'same' });
  assert.equal(conflict.status, 409);
  assert.equal(databaseState(f.db), before);
  f.advance();
  const second = await f.call(body);
  f.advance();
  const third = await f.call(body);
  assert.equal(second.status, 200);
  assert.equal(third.status, 200);
  assert.notEqual(second.headers.get('x-nai-request-id'), third.headers.get('x-nai-request-id'));
  assert.equal(f.upstream.calls.length, 3);
  f.advance();
  const other = await f.call(body, { key: 'same', member: 'm2' });
  assert.equal(other.status, 200, other.buffer.toString());
  assert.notEqual(other.headers.get('x-nai-request-id'), id);
  assert.equal(f.upstream.calls.length, 4);
  assert.ok(f.upstream.calls.every(({ body }) => body.parameters.n_samples === 1));
  assert.deepEqual(f.errors, []);
});

test('Native网关诊断header绑定原受理事实和command，原键新交付不改写原关联', async (t) => {
  const f = await setup(t),
    clientDiagnostic = 'client-cannot-own-local-diagnostic',
    upstreamDiagnostic = 'upstream-cannot-own-local-diagnostic';
  f.upstream.handler = async (_req, res) => {
    res.writeHead(200, {
      'content-type': 'image/webp',
      'x-nai-diagnostic-id': upstreamDiagnostic,
      'x-correlation-id': upstreamDiagnostic,
    });
    res.end(image);
  };
  const first = await f.call(input(), {
    key: 'diagnostic',
    headers: { 'x-nai-diagnostic-id': clientDiagnostic, 'x-correlation-id': clientDiagnostic },
  });
  assert.equal(first.status, 200, first.buffer.toString());
  const diagnosticId = first.headers.get('x-nai-diagnostic-id'),
    requestId = first.headers.get('x-nai-request-id');
  assert.match(diagnosticId, /^[a-f0-9]{16}$/);
  assert.notEqual(diagnosticId, clientDiagnostic);
  assert.notEqual(diagnosticId, upstreamDiagnostic);
  const accepted = f.db
    .prepare(
      `SELECT CAST(commit_seq AS TEXT) AS seq,correlation_id AS correlationId FROM domain_events
      WHERE event_type='RequestAccepted' AND json_extract(payload,'$.requestId')=?`,
    )
    .get(requestId);
  assert.equal(accepted.correlationId, diagnosticId);
  assert.equal(readRelationalCommit(f.db, accepted.seq).command.correlationId, diagnosticId);
  const before = databaseState(f.db),
    replay = await f.call(input(), { key: 'diagnostic' });
  assert.equal(replay.status, 200, replay.buffer.toString());
  assert.equal(replay.headers.get('x-nai-request-id'), requestId);
  assert.match(replay.headers.get('x-nai-diagnostic-id'), /^[a-f0-9]{16}$/);
  assert.notEqual(replay.headers.get('x-nai-diagnostic-id'), diagnosticId);
  assert.deepEqual(replay.buffer, first.buffer);
  assert.equal(databaseState(f.db), before);
  assert.equal(readRelationalCommit(f.db, accepted.seq).command.correlationId, diagnosticId);
  assert.equal(f.upstream.calls.length, 1);
});

test('Native多child沿真实队列逐图收费，JSON/ZIP完整且共同提交回放保留输入/alias/结果', async (t) => {
  const f = await setup(t),
    pending = f.call(input({ n_samples: 2, seed: 42 }), {
      key: 'batch',
      accept: 'application/zip',
    });
  await waitFor(() => f.upstream.calls.length === 1 && f.controls.size === 0);
  f.advance();
  const response = await pending;
  assert.equal(response.status, 200, response.buffer.toString());
  assert.equal(response.headers.get('x-nai-image-count'), '2');
  const files = zipFiles(response.buffer),
    summary = JSON.parse(files.get('results.json'));
  assert.equal(summary.completed, 2);
  assert.deepEqual(
    f.upstream.calls.map(({ body }) => body.parameters.seed),
    [42, 43],
  );
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replayer = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), inputs))();
  }
  for (const table of [
    'generation_requests',
    'generation_inputs',
    'input_assets',
    'normalization_contexts',
    'generation_jobs',
    'generation_attempts',
    'charges',
    'wallets',
    'account_quota_state',
    'artifacts',
    'artifact_replicas',
    'idempotency_keys',
    'commits',
    'domain_events',
  ]) {
    assert.deepEqual(
      target.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
      f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
      table,
    );
  }
  assert.equal(target.prepare('SELECT status FROM projection_state').get().status, 'building');
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
});

test('Native辅助操作复用JSON/multipart和真实BLOB/产物，维持编码201与放大200', async (t) => {
  const f = await setup(t),
    body = {
      model: 'nai-diffusion-4-5-full',
      image: image.toString('base64'),
      information_extracted: 0.5,
    },
    form = new FormData();
  form.set('request', JSON.stringify({ ...body, image: 'image' }));
  form.set('image', new Blob([image], { type: 'image/webp' }), 'input.webp');
  const encoded = await f.call(form, { endpoint: '/v1/ai/encode-vibe', key: 'vibe' });
  assert.equal(encoded.status, 201, encoded.buffer.toString());
  assert.equal(encoded.headers.get('content-type'), 'application/binary');
  assert.deepEqual(encoded.buffer, Buffer.alloc(200, 0x80));
  const before = f.db.serialize(),
    replay = await f.call(body, { endpoint: '/ai/encode-vibe', key: 'vibe' });
  assert.equal(replay.status, 201);
  assert.deepEqual(replay.buffer, encoded.buffer);
  assert.deepEqual(f.db.serialize(), before);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM input_assets').get().n, 1);
  f.advance();
  const upscaled = await f.call(
    { image: body.image, scale: 2 },
    { endpoint: '/ai/upscale', key: 'upscale' },
  );
  assert.equal(upscaled.status, 200, upscaled.buffer.toString());
  assert.deepEqual(Buffer.from(json(upscaled).images[0].image, 'base64'), image);
  assert.equal(f.upstream.calls.length, 2);
});

test('Native复用MessagePack/SSE预览与final协议，真实上游流经过同一队列和文件发布', async (t) => {
  for (const stream of ['msgpack', 'sse']) {
    await t.test(stream, async (t) => {
      const f = await setup(t, { stream: true });
      const frame = (value) => {
        const bytes = Buffer.from(encode(value)),
          header = Buffer.alloc(4);
        header.writeUInt32BE(bytes.length);
        return Buffer.concat([header, bytes]);
      };
      f.upstream.handler = async (_req, res) => {
        res.writeHead(200, { 'content-type': 'application/msgpack' });
        res.write(frame({ event_type: 'intermediate', samp_ix: 0, step_ix: 1, image }));
        await new Promise((resolve) => setTimeout(resolve, 10));
        res.end(frame({ event_type: 'final', samp_ix: 0, image }));
      };
      const response = await f.call(input({ stream }), {
        endpoint: '/v1/ai/generate-image-stream',
        key: stream,
      });
      assert.equal(response.status, 200, response.buffer.toString());
      const events =
        stream === 'sse' ? readNativeSseEvents(response.buffer) : readNativeEvents(response.buffer);
      assert.deepEqual(
        events.map((event) => event.event_type),
        ['intermediate', 'final'],
      );
      assert.deepEqual(
        Buffer.from(events[1].image, stream === 'sse' ? 'base64' : undefined),
        image,
      );
      assert.equal(f.upstream.calls.length, 1);
      assert.equal(f.previews.watchers.size, 0);
    });
  }
});

test('Native等待/免费准入保留旧拒绝边界，免费整批仍逐child复核而非提前耗尽整批额度', async (t) => {
  const f = await setup(t, { freeLimit: 1 }),
    body = input({ steps: 23, n_samples: 2 });
  const pending = f.call(body, { key: 'free-batch' });
  await waitFor(() => f.upstream.calls.length === 1 && f.controls.size === 0);
  f.advance();
  const response = await pending;
  assert.equal(response.status, 200, response.buffer.toString());
  assert.equal(json(response).completed, 1);
  assert.equal(json(response).remaining, 1);
  assert.equal(f.upstream.calls.length, 1);
  let before = databaseState(f.db);
  assert.equal((await f.call(input({ steps: 23 }), { key: 'free-over-limit' })).status, 429);
  assert.equal(databaseState(f.db), before);
  f.db.prepare("UPDATE upstream_accounts SET enabled=0 WHERE id='a'").run();
  before = databaseState(f.db);
  assert.equal((await f.call(input(), { key: 'paused' })).status, 503);
  assert.equal(databaseState(f.db), before);
  f.db.prepare("UPDATE upstream_accounts SET enabled=1 WHERE id='a'").run();
  f.db.prepare("DELETE FROM group_members WHERE member_id='m1'").run();
  before = databaseState(f.db);
  assert.equal((await f.call(input(), { key: 'ungrouped' })).status, 403);
  assert.equal(databaseState(f.db), before);
});

test('Native受理SQL失败连恢复全回滚；原键先复核当前Key，慢正文凭据/epoch变化不留新意图', async (t) => {
  const f = await setup(t),
    body = input();
  f.db.exec(
    "CREATE TEMP TRIGGER reject_input AFTER INSERT ON generation_inputs BEGIN SELECT RAISE(ABORT,'native input rollback'); END",
  );
  const before = databaseState(f.db),
    failed = await f.call(body, { key: 'rollback' });
  assert.equal(failed.status, 500);
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER reject_input');
  assert.equal((await f.call(body, { key: 'rollback' })).status, 200);
  const renewedKey = 'STA1N-renewed-native-key';
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', renewedKey))();
  const rotated = databaseState(f.db);
  assert.equal((await f.call(body, { key: 'rollback' })).status, 401);
  assert.equal(
    (await f.call(body, { key: 'rollback', headers: { authorization: `Bearer ${renewedKey}` } }))
      .status,
    200,
  );
  assert.equal(databaseState(f.db), rotated);
  const slow = async (key, name, mutate) => {
    const incoming = once(f.server, 'request'),
      text = JSON.stringify(body);
    const outgoing = httpRequest(f.origin + '/ai/generate-image', {
      method: 'POST',
      headers: {
        authorization: `Bearer ${key}`,
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(text),
        'idempotency-key': name,
      },
    });
    const response = new Promise((resolve, reject) => {
      outgoing.on('response', async (res) => {
        for await (const _chunk of res) {
        }
        resolve(res.statusCode);
      });
      outgoing.on('error', reject);
    });
    outgoing.write(text.slice(0, 20));
    const [upload] = await incoming;
    await once(upload, 'data');
    mutate();
    const state = databaseState(f.db);
    outgoing.end(text.slice(20));
    const status = await response;
    assert.equal(databaseState(f.db), state);
    return status;
  };
  const latestKey = 'STA1N-latest-native-key';
  assert.equal(
    await slow(renewedKey, 'revoked-during-body', () =>
      f.db.transaction(() => f.members.replaceMemberApiKey('m1', latestKey))(),
    ),
    401,
  );
  // 负向注入模拟读取正文期间的旧接单epoch；真正受理不能把它改成当前值后继续。
  assert.equal(
    await slow(latestKey, 'stale-epoch', () =>
      f.db.prepare("UPDATE members SET admission_epoch=admission_epoch+2 WHERE id='m1'").run(),
    ),
    409,
  );
  assert.equal(f.upstream.calls.length, 1);
});

test('Native断线只释放领取等待，真实生成继续；同键重领不重发，原bytes缺失为410', async (t) => {
  const f = await setup(t);
  const gate = new Promise((resolve) => {
    f.upstream.release = resolve;
  });
  f.upstream.handler = async (_req, res) => {
    await gate;
    res.writeHead(200, { 'content-type': 'image/webp' });
    res.end(image);
  };
  const controller = new AbortController();
  const pending = f.call(input(), { key: 'disconnect', signal: controller.signal });
  await waitFor(() => f.upstream.calls.length === 1);
  controller.abort();
  await assert.rejects(pending, { name: 'AbortError' });
  await waitFor(() => f.events.listenerCount('commit') === 0);
  const request = f.db
    .prepare("SELECT id FROM generation_requests WHERE source='novelai-native'")
    .get();
  assert.equal(
    f.db.prepare('SELECT status FROM generation_jobs WHERE request_id=?').get(request.id).status,
    'running',
  );
  f.upstream.release();
  await waitFor(() => f.controls.size === 0);
  assert.equal((await f.call(input(), { key: 'disconnect' })).status, 200);
  assert.equal(f.upstream.calls.length, 1);
  const artifact = f.db
    .prepare(
      'SELECT a.id,a.source_event_id FROM artifacts a JOIN generation_jobs j ON j.artifact_id=a.id WHERE j.request_id=?',
    )
    .get(request.id);
  const row = f.db
    .prepare('SELECT object_key FROM artifact_replicas WHERE artifact_id=?')
    .get(artifact.id);
  const { unlink } = await import('node:fs/promises');
  await unlink(path.join(f.dir, 'images', row.object_key));
  const before = databaseState(f.db),
    unavailable = await f.call(input(), { key: 'disconnect' });
  assert.equal(unavailable.status, 410);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.upstream.calls.length, 1);
});

test('Native原键沿首次context重领，当前defaults变化仅作用于新操作键', async (t) => {
  const f = await setup(t),
    scale = NATIVE_DEFAULTS.scale;
  t.after(() => {
    NATIVE_DEFAULTS.scale = scale;
  });
  assert.equal((await f.call(input(), { key: 'context' })).status, 200);
  NATIVE_DEFAULTS.scale = scale + 1;
  const before = databaseState(f.db);
  assert.equal((await f.call(input(), { key: 'context' })).status, 200);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.upstream.calls.length, 1);
  f.advance();
  assert.equal((await f.call(input(), { key: 'new-context' })).status, 200);
  assert.deepEqual(
    f.upstream.calls.map(({ body }) => body.parameters.scale),
    [scale, scale + 1],
  );
});

test('Native权限和Accept/头格式在受理前拒绝，合法Cookie不能替代STA1N Bearer', async (t) => {
  const building = new Database(':memory:');
  initializeRelationalStorage(building, 'native-building');
  assert.throws(() => createRelationalNativeHttp({ sqlite: building }), /incomplete/);
  building.close();
  const f = await setup(t),
    member = await authenticateMemberApiKey(f.members, keys.m1),
    session = await createMemberSession(f.members, member, {}, f.clock);
  assert.equal(
    (await authenticateMemberSession(f.members, session.token, {}, f.clock)).user.id,
    'm1',
  );
  const before = databaseState(f.db);
  const cookie = await f.call(input(), {
    headers: { authorization: '', cookie: `nai_member_session=${session.token}` },
  });
  assert.equal(cookie.status, 401);
  const invalidKey = await f.call(input(), { key: 'invalid key' });
  assert.equal(invalidKey.status, 400);
  const unacceptable = await f.call(input(), { accept: 'text/plain' });
  assert.equal(unacceptable.status, 406);
  for (const response of [cookie, invalidKey, unacceptable]) {
    const value = json(response);
    assert.deepEqual(Object.keys(value).sort(), ['error', 'message', 'statusCode']);
    assert.equal(value.statusCode, response.status);
    assert.equal(value.message, value.error);
    assert.equal(response.buffer.includes(Buffer.from(keys.m1)), false);
  }
  assert.equal(databaseState(f.db), before);
  assert.equal(f.upstream.calls.length, 0);
});

test('Native准入不提前预扣或拒绝派发期余额，真实dispatch拒绝仍保留原受理与失败回执', async (t) => {
  const f = await setup(t, {
    fixed: '0',
    purchased: '0',
    officialFixed: '0',
    officialPurchased: '0',
  });
  const response = await f.call(input(), { key: 'no-anlas' });
  assert.equal(response.status, 402, response.buffer.toString());
  const request = f.db
    .prepare("SELECT id FROM generation_requests WHERE source='novelai-native'")
    .get();
  assert.ok(request);
  const job = f.db.prepare('SELECT status FROM generation_jobs WHERE request_id=?').get(request.id);
  assert.equal(job.status, 'failed');
  assert.equal(
    f.db.prepare('SELECT COUNT(*) AS n FROM idempotency_keys WHERE request_id=?').get(request.id).n,
    1,
  );
  assert.equal(
    f.db
      .prepare(
        'SELECT COUNT(*) AS n FROM charges WHERE job_id IN (SELECT id FROM generation_jobs WHERE request_id=?)',
      )
      .get(request.id).n,
    0,
  );
  assert.equal(f.upstream.calls.length, 0);
});

test('Native原键保留持久R2承诺：缺配置/错目录/模拟GET失败503，恢复后原图零写领取', async (t) => {
  const f = await setup(t),
    originalFetch = globalThis.fetch,
    cloudCalls = [],
    objects = new Map(),
    r2 = new R2Storage({
      accountId: 'a'.repeat(32),
      bucket: 'native-reference-images',
      accessKeyId: 'synthetic-private-access',
      secretAccessKey: 'synthetic-private-secret',
    });
  let cloudFailed = false;
  // 仅模拟云传输；真实R2 client/签名、复制确认owner和字节摘要仍执行，不访问真实云。
  t.mock.method(globalThis, 'fetch', async (address, options) => {
    const url = new URL(address);
    if (!url.hostname.endsWith('.r2.cloudflarestorage.com')) {
      return originalFetch(address, options);
    }
    assert.equal(f.db.inTransaction, false);
    assert.ok(options.signal);
    cloudCalls.push(options.method);
    if (options.method === 'PUT') {
      objects.set(url.href, Buffer.from(options.body));
      return new Response(null, { status: 204 });
    }
    assert.equal(options.method, 'GET');
    if (cloudFailed || !objects.has(url.href)) {
      return new Response(null, { status: 503 });
    }
    const bytes = objects.get(url.href);
    return new Response(bytes, { headers: { 'content-length': String(bytes.length) } });
  });
  const first = await f.call(input(), { key: 'cloud-reference' });
  assert.equal(first.status, 200, first.buffer.toString());
  const requestId = first.headers.get('x-nai-request-id'),
    job = f.db
      .prepare('SELECT artifact_id AS artifactId FROM generation_jobs WHERE request_id=?')
      .get(requestId);
  f.replicas.r2 = r2;
  await copyRelationalArtifactReplica(
    f.db,
    job.artifactId,
    'r2',
    {
      ...f.replicas,
      idle: () => f.controls.size === 0,
    },
    () => f.clock,
  );
  assert.deepEqual(cloudCalls, ['PUT', 'GET']);
  const file = path.join(f.dir, `images/${job.artifactId}.webp`);
  await rename(file, `${file}.saved`);
  const before = databaseState(f.db);
  const unavailable = async () => {
    const response = await f.call(input(), { key: 'cloud-reference' });
    assert.equal(response.status, 503, response.buffer.toString());
    assert.equal(response.headers.get('x-nai-request-id'), requestId);
    assert.equal(databaseState(f.db), before);
    assert.equal(f.upstream.calls.length, 1);
  };
  delete f.replicas.r2;
  await unavailable();
  assert.deepEqual(cloudCalls, ['PUT', 'GET']);
  f.replicas.r2 = new R2Storage({ ...r2.config, bucket: 'wrong-native-images' });
  await unavailable();
  assert.deepEqual(cloudCalls, ['PUT', 'GET']);
  f.replicas.r2 = r2;
  cloudFailed = true;
  await unavailable();
  assert.deepEqual(cloudCalls, ['PUT', 'GET', 'GET']);
  cloudFailed = false;
  const recovered = await f.call(input(), { key: 'cloud-reference' });
  assert.equal(recovered.status, 200, recovered.buffer.toString());
  assert.equal(recovered.headers.get('x-nai-request-id'), requestId);
  assert.deepEqual(recovered.buffer, first.buffer);
  assert.equal(databaseState(f.db), before);
  assert.deepEqual(cloudCalls, ['PUT', 'GET', 'GET', 'GET']);
  assert.equal(f.upstream.calls.length, 1);
  const currentKey = 'STA1N-native-cloud-current';
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', currentKey))();
  const rotated = databaseState(f.db);
  assert.equal((await f.call(input(), { key: 'cloud-reference' })).status, 401);
  assert.deepEqual(cloudCalls, ['PUT', 'GET', 'GET', 'GET']);
  cloudFailed = true;
  const currentFailure = await f.call(input(), {
    key: 'cloud-reference',
    headers: { authorization: `Bearer ${currentKey}` },
  });
  assert.equal(currentFailure.status, 503);
  assert.equal(currentFailure.headers.get('x-nai-request-id'), requestId);
  assert.equal(databaseState(f.db), rotated);
  assert.deepEqual(cloudCalls, ['PUT', 'GET', 'GET', 'GET', 'GET']);
  assert.equal(f.upstream.calls.length, 1);
});

test('独立：多child旧键先核全体引用，真缺失410不GET，冷承诺暂不可读503不重发', async (t) => {
  const f = await setup(t),
    originalFetch = globalThis.fetch;
  const r2 = new R2Storage({
    accountId: 'c'.repeat(32),
    bucket: 'independent-native',
    accessKeyId: 'synthetic-access',
    secretAccessKey: 'synthetic-secret',
  });
  const objects = new Map(),
    cloudCalls = [];
  t.mock.method(globalThis, 'fetch', async (address, options) => {
    const url = new URL(address);
    if (!url.hostname.endsWith('.r2.cloudflarestorage.com')) return originalFetch(address, options);
    cloudCalls.push(options.method);
    assert.equal(f.db.inTransaction, false);
    if (options.method === 'PUT') {
      objects.set(url.href, Buffer.from(options.body));
      return new Response(null, { status: 204 });
    }
    assert.equal(options.method, 'GET');
    const bytes = objects.get(url.href);
    assert.ok(bytes);
    return new Response(bytes, { headers: { 'content-length': String(bytes.length) } });
  });
  const body = input({ n_samples: 2, seed: 52 });
  const pending = f.call(body, { key: 'independent-batch-reference' });
  await waitFor(() => f.upstream.calls.length === 1 && f.controls.size === 0);
  f.advance();
  const first = await pending;
  assert.equal(first.status, 200, first.buffer.toString());
  const requestId = first.headers.get('x-nai-request-id');
  const jobs = f.db
    .prepare(
      'SELECT artifact_id AS id FROM generation_jobs WHERE request_id=? ORDER BY sample_index',
    )
    .all(requestId);
  assert.equal(jobs.length, 2);
  f.replicas.r2 = r2;
  await copyRelationalArtifactReplica(
    f.db,
    jobs[0].id,
    'r2',
    { ...f.replicas, idle: () => f.controls.size === 0 },
    () => f.clock,
  );
  assert.deepEqual(cloudCalls, ['PUT', 'GET']);
  for (const { id } of jobs) {
    const file = path.join(f.dir, `images/${id}.webp`);
    await rename(file, file + '.saved');
  }
  delete f.replicas.r2;
  const before = databaseState(f.db);
  const missing = await f.call(body, {
    endpoint: '/v1/ai/generate-image',
    key: 'independent-batch-reference',
  });
  assert.equal(missing.status, 410, missing.buffer.toString());
  assert.equal(databaseState(f.db), before);
  assert.deepEqual(cloudCalls, ['PUT', 'GET']);
  assert.equal(f.upstream.calls.length, 2);
  const second = path.join(f.dir, `images/${jobs[1].id}.webp`);
  await rename(second + '.saved', second);
  const temporary = await f.call(body, { key: 'independent-batch-reference' });
  assert.equal(temporary.status, 503, temporary.buffer.toString());
  assert.equal(temporary.headers.get('x-nai-request-id'), requestId);
  assert.equal(databaseState(f.db), before);
  assert.deepEqual(cloudCalls, ['PUT', 'GET']);
  assert.equal(f.upstream.calls.length, 2);
});
