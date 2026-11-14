import { initializeFixtureConfiguration } from '../helpers/relational-configuration.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createServer, request as httpRequest } from 'node:http';
import { EventEmitter, once } from 'node:events';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import fs from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import path from 'node:path';
import { fixture, keys, NOW, image, databaseState } from '../helpers/relational-generation.mjs';
import {
  authenticateMemberApiKey,
  createMemberSession,
  logoutMemberSession,
} from '../../server/members/authentication.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import {
  assertRelationalStorageReady,
  initializeRelationalStorage,
} from '../../server/storage/relational-storage.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { createRelationalMemberGenerationHttp } from '../../server/generation/relational-member-http.ts';
import { closeMemberGenerationStreams } from '../../server/generation/member-http.ts';
import { createGenerationPayloadBudget } from '../../server/generation/payload-budget.ts';
import { JobPreviews } from '../../server/generation/job-previews.ts';

const input = (parameters = {}, root = {}) => ({
  operation: 'generate',
  model: 'nai-diffusion-4-5-full',
  input: 'synthetic HTTP prompt',
  parameters: { width: 128, height: 128, steps: 30, seed: -1, ...parameters },
  ...root,
});
const confirmation = (quote) => ({
  requestFingerprint: quote.requestFingerprint,
  versions: quote.versions,
  maxPerJob: quote.charges.perJob[0].resources,
  maxTotal: quote.charges.total,
});
async function setup(t, options = {}) {
  const f = await fixture(t, options),
    sessions = {};
  for (const id of ['m1', 'm2']) {
    sessions[id] = await createMemberSession(
      f.members,
      await authenticateMemberApiKey(f.members, keys[id]),
      options.env ?? {},
      NOW,
    );
  }
  // 使用真实 owner 发布字节，再经唯一完整 shadow/33表/private/bytes 安装入口进入 ready。
  await f.accept('existing');
  f.dispatch('existing-0', NOW);
  await f.publish('existing-0');
  const sourcePath = path.join(f.dir, 'selected.sqlite');
  initializeFixtureConfiguration(f.db);
  await f.db.backup(sourcePath);
  const sealed = new Database(sourcePath);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  await verifyAndInstallRelationalStorage(f.db, {
    sourcePath,
    sourceSha256: createHash('sha256')
      .update(await readFile(sourcePath))
      .digest('hex'),
    historyId: 'generation-history',
    finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get()
      .seq,
    replicas: { dataDir: f.dir },
  });
  assertRelationalStorageReady(f.db);
  const events = new EventEmitter(),
    previews = new JobPreviews();
  let drains = 0;
  const handle = createRelationalMemberGenerationHttp({
    sqlite: f.db,
    payloadBudget: createGenerationPayloadBudget(() => f.inputs.activePayloadBytes()),
    events,
    previews,
    replicas: { dataDir: f.dir },
    env: options.env ?? {},
    now: () => NOW + 10010,
    scheduleQueueDrain: () => {
      drains++;
    },
  });
  const server = createServer(async (req, res) => {
    if (!(await handle(req, res, new URL(req.url, `http://${req.headers.host}`)))) {
      res.writeHead(404);
      res.end();
    }
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const origin = `http://127.0.0.1:${server.address().port}`;
  const controllers = [];
  t.after(async () => {
    for (const controller of controllers) {
      controller.abort();
    }
    closeMemberGenerationStreams(f.db);
    events.emit('close');
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  });
  const fetchMember = (url, { member = 'm1', method = 'GET', body, headers = {}, ...rest } = {}) =>
    fetch(origin + url, {
      method,
      headers: {
        cookie: `nai_member_session=${sessions[member].token}`,
        origin,
        ...(body === undefined ? {} : { 'content-type': 'application/json' }),
        ...headers,
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      ...rest,
    });
  const json = async (url, options) => {
    const response = await fetchMember(url, options);
    return { status: response.status, headers: response.headers, body: await response.json() };
  };
  const submit = async (body = input(), key = 'intent') => {
    const quote = await json('/api/member/generation/quote', { method: 'POST', body });
    assert.equal(quote.status, 200);
    return json('/api/member/generation/requests', {
      method: 'POST',
      body: { ...body, confirmation: confirmation(quote.body) },
      headers: { 'idempotency-key': key },
    });
  };
  const stream = async (url) => {
    const controller = new AbortController();
    controllers.push(controller);
    const response = await fetchMember(url, { signal: controller.signal });
    assert.equal(response.status, 200);
    const reader = response.body.getReader(),
      decoder = new TextDecoder();
    let buffer = '';
    return {
      abort: () => controller.abort(),
      async until(event) {
        const deadline = AbortSignal.timeout(3000);
        while (!buffer.includes(`event: ${event}\n`)) {
          const next = await Promise.race([
            reader.read(),
            new Promise((_, reject) =>
              deadline.addEventListener(
                'abort',
                () => reject(new Error(`Missing SSE ${event}: ${buffer}`)),
                { once: true },
              ),
            ),
          ]);
          if (next.done) {
            throw new Error(`SSE ended before ${event}: ${buffer}`);
          }
          buffer += decoder.decode(next.value, { stream: true });
        }
        const result = buffer;
        buffer = '';
        return result;
      },
    };
  };
  return {
    ...f,
    events,
    previews,
    origin,
    server,
    sessions,
    fetchMember,
    json,
    submit,
    stream,
    drains: () => drains,
  };
}

test('关系 HTTP 在 listen 前拒绝 building；Cookie/Origin 隔离且报价只读，真实 Local 结果可领取', async (t) => {
  const building = new Database(':memory:');
  initializeRelationalStorage(building, 'unready');
  assert.throws(() => createRelationalMemberGenerationHttp({ sqlite: building }), /incomplete/);
  building.close();
  const f = await setup(t, { env: { OWNER_USER_ID: 'm1' } });
  await f.accept('other-member', { memberId: 'm2' });
  assert.equal(
    (
      await fetch(f.origin + '/api/member/generation/capabilities', {
        headers: { authorization: `Bearer ${keys.m1}` },
      })
    ).status,
    401,
  );
  assert.equal(
    (
      await f.json('/api/member/generation/capabilities', {
        headers: { origin: 'https://elsewhere.invalid' },
      })
    ).status,
    403,
  );
  assert.equal(
    (
      await f.json('/api/member/generation/quote', {
        method: 'POST',
        body: input(),
        headers: { origin: '' },
      })
    ).status,
    403,
  );
  const before = databaseState(f.db),
    capability = await f.json('/api/member/generation/capabilities'),
    quote = await f.json('/api/member/generation/quote', { method: 'POST', body: input() });
  assert.equal(capability.status, 200);
  assert.equal(quote.status, 200);
  assert.equal(quote.body.admission.allowed, true);
  assert.equal(quote.headers.get('cache-control'), 'private, no-store');
  assert.equal((await f.json('/api/member/requests/other-member/input')).status, 404);
  assert.equal(databaseState(f.db), before);
  const content = await f.fetchMember('/api/member/jobs/existing-0/content');
  assert.equal(content.status, 200);
  assert.deepEqual(Buffer.from(await content.arrayBuffer()), image);
  assert.equal((await f.json('/api/member/jobs/existing-0/content', { member: 'm2' })).status, 404);
  assert.equal((await f.json('/api/member/requests/existing/input', { member: 'm2' })).status, 404);
  assert.equal(
    (
      await f.json('/api/member/generation/quote', {
        method: 'POST',
        body: input({}, { memberId: 'm2' }),
      })
    ).status,
    422,
  );
  assert.equal(databaseState(f.db), before);
});

test('关系 HTTP 完整受理同键重放保留首次 seed/context，拒绝确认篡改且完整提交可重建', async (t) => {
  const f = await setup(t),
    body = input({ n_samples: 2 }),
    quote = (await f.json('/api/member/generation/quote', { method: 'POST', body })).body,
    confirmed = { ...body, confirmation: confirmation(quote) },
    options = { method: 'POST', body: confirmed, headers: { 'idempotency-key': 'same' } };
  const results = await Promise.all([
    f.json('/api/member/generation/requests', options),
    f.json('/api/member/generation/requests', options),
  ]);
  assert.deepEqual(
    results.map((r) => r.status),
    [202, 202],
  );
  assert.equal(results[0].body.requestId, results[1].body.requestId);
  assert.deepEqual(results.map((r) => r.body.replayed).sort(), [false, true]);
  const id = results[0].body.requestId,
    original = (await f.json(`/api/member/requests/${id}/input`)).body,
    state = (await f.json(`/api/member/requests/${id}`)).body;
  assert.equal(original.recoverable, true);
  assert.ok(Number.isInteger(original.input.parameters.seed));
  assert.notEqual(original.input.parameters.seed, -1);
  assert.deepEqual(
    state.jobs.map((j) => j.status),
    ['queued', 'pending'],
  );
  assert.equal(
    f.db
      .prepare('SELECT COUNT(*) AS n FROM charges WHERE job_id IN (?,?)')
      .get(...results[0].body.jobIds).n,
    0,
  );
  const before = databaseState(f.db);
  assert.equal(
    (
      await f.json('/api/member/generation/requests', {
        ...options,
        body: { ...confirmed, input: 'changed' },
      })
    ).body.code,
    'idempotency_conflict',
  );
  assert.equal(
    (
      await f.json('/api/member/generation/requests', {
        ...options,
        body: {
          ...confirmed,
          confirmation: {
            ...confirmed.confirmation,
            maxTotal: { v5: '0', anlasFixed: '0', anlasPurchased: '0' },
          },
        },
        headers: { 'idempotency-key': 'cap-too-small' },
      })
    ).body.code,
    'cost_limit_exceeded',
  );
  assert.equal(databaseState(f.db), before);
  const restored = new Database(':memory:');
  t.after(() => restored.close());
  initializeRelationalStorage(restored, 'generation-history');
  const replayer = new RelationalCommitReplayer(restored);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    restored.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), f.inputs))();
  }
  assert.deepEqual(
    new RelationalInputStore(restored).readJobInput(results[0].body.jobIds[0]),
    f.inputs.readJobInput(results[0].body.jobIds[0]),
  );
  for (const table of [
    'generation_requests',
    'generation_inputs',
    'normalization_contexts',
    'generation_jobs',
    'wallets',
    'account_quota_state',
    'commits',
    'domain_events',
    'idempotency_keys',
  ]) {
    assert.deepEqual(
      restored.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
      f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
      table,
    );
  }
  assert.equal(restored.prepare('SELECT COUNT(*) AS n FROM generation_jobs').get().n, 3);
  assert.equal(restored.prepare('SELECT status FROM projection_state').get().status, 'building');
  assert.equal(restored.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n, 0);
});

test('关系 HTTP multipart/JSON 同报价并经唯一 input BLOB 恢复附件，取消仅改变等待 child', async (t) => {
  const f = await setup(t),
    body = {
      operation: 'encode-vibe',
      model: 'nai-diffusion-4-5-full',
      image: image.toString('base64'),
      information_extracted: 0.5,
    };
  const quote = (await f.json('/api/member/generation/quote', { method: 'POST', body })).body,
    form = new FormData();
  form.set('request', JSON.stringify({ ...body, image: 'image' }));
  form.set('image', new Blob([image], { type: 'image/webp' }), 'input.webp');
  const response = await fetch(f.origin + '/api/member/generation/quote', {
    method: 'POST',
    headers: { cookie: `nai_member_session=${f.sessions.m1.token}`, origin: f.origin },
    body: form,
  });
  assert.equal(response.status, 200);
  assert.equal((await response.json()).requestFingerprint, quote.requestFingerprint);
  const accepted = await f.submit(body, 'asset');
  assert.equal(accepted.status, 202);
  const restored = (await f.json(accepted.body.links.input)).body;
  assert.equal(restored.assets.length, 1);
  const asset = await f.fetchMember(restored.assets[0].url);
  assert.deepEqual(Buffer.from(await asset.arrayBuffer()), image);
  assert.equal((await f.json(restored.assets[0].url, { member: 'm2' })).status, 404);
  const cancel = await f.json(accepted.body.links.cancel, { method: 'POST', body: {} });
  assert.equal(cancel.status, 200);
  assert.equal(cancel.body.cancelledCount, 1);
  assert.equal(cancel.body.status, 'cancelled');
  assert.equal(
    (await f.json(accepted.body.links.cancel, { method: 'POST', body: {} })).body.cancelledCount,
    0,
  );
});

test('关系 HTTP SSE 随真实 owner 提交发送状态与预览，撤销与断线不取消任务', async (t) => {
  const f = await setup(t),
    accepted = await f.submit(input(), 'sse'),
    id = accepted.body.jobIds[0],
    stream = await f.stream(accepted.body.links.events);
  assert.match(await stream.until('snapshot'), /queued/);
  assert.ok(f.dispatch(id, NOW + 10011));
  f.events.emit('commit');
  assert.match(await stream.until('state'), /running/);
  f.previews.update(
    { id, userToken: 'm1', status: 'running' },
    { previewBuffer: image, previewMimeType: 'image/webp', percent: 50 },
  );
  assert.match(await stream.until('preview'), /data:image\/webp;base64/);
  await f.publish(id, NOW + 10013);
  f.events.emit('commit');
  assert.match(await stream.until('complete'), /done/);
  const waiting = await f.submit(input(), 'revoke'),
    revoked = await f.stream(waiting.body.links.events);
  await revoked.until('snapshot');
  const disconnected = await f.stream(waiting.body.links.events);
  await disconnected.until('snapshot');
  disconnected.abort();
  assert.equal((await f.json(waiting.body.links.status)).body.status, 'queued');
  await logoutMemberSession(f.members, f.sessions.m1.token);
  f.events.emit('commit');
  await revoked.until('auth-expired');
  assert.equal(
    f.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get(waiting.body.jobIds[0])
      .status,
    'queued',
  );
  assert.equal((await f.json(waiting.body.links.input)).status, 401);
});

test('关系 HTTP 正文等待中撤销凭据与受理中途失败均不留下输入、映射或事实', async (t) => {
  const f = await setup(t),
    body = input(),
    quote = (await f.json('/api/member/generation/quote', { method: 'POST', body })).body,
    text = JSON.stringify({ ...body, confirmation: confirmation(quote) });
  f.db.exec(
    "CREATE TEMP TRIGGER reject_input AFTER INSERT ON generation_inputs BEGIN SELECT RAISE(ABORT,'synthetic rollback'); END",
  );
  const before = databaseState(f.db);
  const failed = await f.json('/api/member/generation/requests', {
    method: 'POST',
    body: JSON.parse(text),
    headers: { 'idempotency-key': 'rollback' },
  });
  assert.equal(failed.status, 500);
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER reject_input');
  assert.equal(
    (
      await f.json('/api/member/generation/requests', {
        method: 'POST',
        body: JSON.parse(text),
        headers: { 'idempotency-key': 'rollback' },
      })
    ).status,
    202,
  );
  const incoming = once(f.server, 'request');
  const outgoing = httpRequest(f.origin + '/api/member/generation/requests', {
    method: 'POST',
    headers: {
      cookie: `nai_member_session=${f.sessions.m1.token}`,
      origin: f.origin,
      'content-type': 'application/json',
      'content-length': Buffer.byteLength(text),
      'idempotency-key': 'revoked-upload',
    },
  });
  const response = new Promise((resolve, reject) => {
    outgoing.on('response', async (res) => {
      const chunks = [];
      for await (const chunk of res) {
        chunks.push(chunk);
      }
      resolve({ status: res.statusCode, body: JSON.parse(Buffer.concat(chunks).toString()) });
    });
    outgoing.on('error', reject);
  });
  outgoing.write(text.slice(0, 20));
  const [upload] = await incoming;
  await once(upload, 'data');
  await logoutMemberSession(f.members, f.sessions.m1.token);
  const revoked = databaseState(f.db);
  outgoing.end(text.slice(20));
  assert.equal((await response).status, 401);
  assert.equal(databaseState(f.db), revoked);
});

test('关系报价在恢复时间边界估算独立钱包，提交与派发在同一点落实相同恢复', async (t) => {
  const f = await setup(t, { v5: '0', secondV5: '40', seconds: 1 }),
    body = input({ steps: 23 }, { model: 'nai-diffusion-5-full' }),
    before = databaseState(f.db),
    quote = await f.json('/api/member/generation/quote', { method: 'POST', body });
  assert.equal(quote.status, 200);
  assert.equal(quote.body.admission.allowed, true, JSON.stringify(quote.body.admission));
  assert.equal(databaseState(f.db), before);
  assert.equal(f.generations.readWallet('a', 'm1').v5, '0');
  const accepted = await f.json('/api/member/generation/requests', {
    method: 'POST',
    body: { ...body, confirmation: confirmation(quote.body) },
    headers: { 'idempotency-key': 'recovered' },
  });
  assert.equal(accepted.status, 202, JSON.stringify(accepted.body));
  assert.ok(f.dispatch(accepted.body.jobIds[0], NOW + 10010));
  assert.equal(
    f.db.prepare("SELECT COUNT(*) AS n FROM domain_events WHERE event_type='RefillApplied'").get()
      .n,
    1,
  );
});

test('关系 HTTP 额度暂不可用时仍返回原分池报价与拒绝原因，查询不改变事实', async (t) => {
  const f = await setup(t),
    body = input(),
    current = (await f.json('/api/member/generation/quote', { method: 'POST', body })).body;
  // 负向故障注入只改变资格；不能把已知的成员权益误报成另一资源池的报价。
  f.db.prepare("UPDATE upstream_accounts SET enabled=0 WHERE id='a'").run();
  const before = databaseState(f.db),
    unavailable = await f.json('/api/member/generation/quote', { method: 'POST', body });
  assert.equal(unavailable.status, 200);
  assert.equal(unavailable.body.admission.allowed, false);
  assert.equal(unavailable.body.admission.reasons[0].code, 'upstream_unavailable');
  assert.deepEqual(unavailable.body.charges, current.charges);
  assert.equal(databaseState(f.db), before);
});

test('成员HTTP实际正文等待中撤下ready，当前Cookie仍有效也不受理', async (t) => {
  const f = await setup(t),
    body = input(),
    quote = (await f.json('/api/member/generation/quote', { method: 'POST', body })).body,
    text = JSON.stringify({ ...body, confirmation: confirmation(quote) }),
    incoming = once(f.server, 'request');
  const outgoing = httpRequest(f.origin + '/api/member/generation/requests', {
    method: 'POST',
    headers: {
      cookie: `nai_member_session=${f.sessions.m1.token}`,
      origin: f.origin,
      'content-type': 'application/json',
      'content-length': Buffer.byteLength(text),
      'idempotency-key': 'withdrawn-ready-upload',
    },
  });
  const response = new Promise((resolve, reject) => {
    outgoing.on('response', async (res) => {
      for await (const chunk of res) {
        /* 完整消费响应，避免把提前断线误判为拒绝。 */
      }
      resolve(res.statusCode);
    });
    outgoing.on('error', reject);
  });
  outgoing.write(text.slice(0, 20));
  const [upload] = await incoming;
  await once(upload, 'data');
  f.db.prepare("UPDATE projection_state SET status='building'").run();
  const before = databaseState(f.db);
  outgoing.end(text.slice(20));
  assert.equal(await response, 500);
  assert.equal(databaseState(f.db), before);
});

test('成员结果实际文件IO后撤下ready，不返回已读取的原bytes', async (t) => {
  const f = await setup(t);
  let before;
  const originalOpen = fs.open;
  const mocked = t.mock.method(fs, 'open', async (...args) => {
    const handle = await originalOpen(...args);
    if (String(args[0]).startsWith(path.join(f.dir, 'images') + path.sep)) {
      const read = handle.readFile.bind(handle);
      handle.readFile = async (...values) => {
        const bytes = await read(...values);
        if (!before) {
          f.db.prepare("UPDATE projection_state SET status='building'").run();
          before = databaseState(f.db);
        }
        return bytes;
      };
    }
    return handle;
  });
  syncBuiltinESMExports();
  t.after(() => {
    mocked.mock.restore();
    syncBuiltinESMExports();
  });
  const response = await f.fetchMember('/api/member/jobs/existing-0/content'),
    bytes = Buffer.from(await response.arrayBuffer());
  assert.ok(before);
  assert.equal(response.status, 500, bytes.toString());
  assert.notDeepEqual(bytes, image);
  assert.equal(databaseState(f.db), before);
});
