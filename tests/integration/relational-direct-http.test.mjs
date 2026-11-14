import test from 'node:test';
import assert from 'node:assert/strict';
import { setup, keys, waitFor, replicateMockR2 } from '../helpers/relational-api-http.mjs';
import { image, databaseState, meta } from '../helpers/relational-generation.mjs';
import { createRelationalDirectHttp } from '../../server/generation/relational-direct-http.ts';
import { createPublicImageAccess } from '../../server/http/public-image.ts';
import { publicErrorMessage } from '../../server/http/public-errors.ts';
import Database from 'better-sqlite3';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import fs from 'node:fs/promises';
import path from 'node:path';
import { syncBuiltinESMExports } from 'node:module';
import { encode } from '@msgpack/msgpack';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';

const input = (values = {}) => ({
  tag: 'synthetic private Direct prompt',
  model: 'nai-diffusion-4-5-full',
  width: 128,
  height: 128,
  steps: 23,
  seed: 7,
  ...values,
});
async function result(response) {
  return {
    status: response.status,
    headers: response.headers,
    buffer: Buffer.from(await response.arrayBuffer()),
  };
}
const json = (response) => JSON.parse(response.buffer.toString());
function assertReplay(t, f) {
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replayer = new RelationalCommitReplayer(target),
    source = new RelationalInputStore(f.db);
  t.mock.method(globalThis, 'fetch', () => assert.fail('完整业务回放不能访问网络'));
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), source))();
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
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
  assert.ok(
    target
      .prepare('SELECT auth_epoch FROM members')
      .all()
      .every((m) => m.auth_epoch === null),
  );
}
async function fixture(t, options = {}) {
  let f,
    defaultReads = 0;
  const defaults = { defaultModel: 'nai-diffusion-4-5-full', defaults: {} },
    signer = createPublicImageAccess({ PUBLIC_MODE: 'true' }, () => f.clock);
  f = await setup(t, {
    ...options,
    env: { PUBLIC_MODE: 'true', ...options.env },
    createHttp: createRelationalDirectHttp,
    renderError: (error) => ({
      statusCode: error.statusCode || 500,
      body: { error: publicErrorMessage(error.message || 'Internal server error') },
    }),
    httpDependencies: {
      readNormalizationDefaults: () => {
        defaultReads++;
        return defaults;
      },
      ...signer,
      validImageSignature: signer.validSignature,
      isAdminRead: (req) => req.headers['x-admin-token'] === 'synthetic-trusted-read-admin',
      sendJson(res, status, payload) {
        res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify(payload));
      },
      ...options.httpDependencies,
    },
  });
  return {
    ...f,
    defaults,
    signer,
    get defaultReads() {
      return defaultReads;
    },
    get: (path, options) => fetch(f.origin + path, options).then(result),
    direct: (values = input(), { token = keys.m1, key, headers = {}, ...options } = {}) =>
      fetch(f.origin + '/generate?' + new URLSearchParams({ token, ...values }), {
        headers: { ...(key === undefined ? {} : { 'idempotency-key': key }), ...headers },
        ...options,
      }).then(result),
    post: (body = input(), options = {}) => f.call(body, { endpoint: '/api/jobs', ...options }),
  };
}

test('合法ready Direct原图片与POST 202/detail/content复用真实受理/队列/文件，关系不伪造balance', async (t) => {
  const f = await fixture(t),
    first = await f.direct(input(), { key: 'direct-original' });
  assert.equal(first.status, 200, first.buffer.toString());
  assert.deepEqual(first.buffer, image);
  assert.equal(first.headers.get('x-cache'), 'miss');
  assert.equal(first.headers.has('x-balance'), false);
  const requestId = first.headers.get('x-nai-request-id'),
    job = f.db.prepare('SELECT id FROM generation_jobs WHERE request_id=?').get(requestId),
    before = databaseState(f.db),
    replay = await f.direct(input(), { key: 'direct-original' });
  assert.equal(replay.status, 200, replay.buffer.toString());
  assert.deepEqual(replay.buffer, image);
  assert.equal(replay.headers.get('x-nai-request-id'), requestId);
  assert.equal(databaseState(f.db), before);
  const detail = await f.get(`/api/jobs/${job.id}?token=${keys.m1}`);
  assert.equal(detail.status, 200, detail.buffer.toString());
  const value = json(detail);
  assert.equal(value.prompt, input().tag);
  assert.equal(value.requestedSteps, 23);
  assert.equal(value.routedSteps, 23);
  assert.equal(value.accountId, 'a');
  assert.equal(value.accountRouteId, null);
  assert.equal(value.accountCost, null);
  assert.equal(value.cost, 0);
  assert.equal(value.status, 'done');
  assert.equal((await f.get(`/api/jobs/${job.id}/content?token=${keys.m1}`)).status, 200);
  assert.equal(f.upstream.calls.length, 1);
  const cached = await f.direct(input());
  assert.equal(cached.status, 200, cached.buffer.toString());
  assert.equal(cached.headers.get('x-cache'), 'hit');
  assert.equal(f.upstream.calls.length, 1);
  f.advance();
  const accepted = await f.post(input({ token: keys.m1, nocache: 1 }), { key: 'post-new' });
  assert.equal(accepted.status, 202, accepted.buffer.toString());
  const posted = json(accepted);
  assert.equal(posted.source, 'web');
  assert.equal(posted.prompt, input().tag);
  await waitFor(
    () =>
      f.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get(posted.id).status ===
      'done',
  );
  assert.deepEqual((await f.get(`/api/jobs/${posted.id}/content?token=${keys.m1}`)).buffer, image);
});

test('Direct只用query.token，POST先限量正文再用body.token优先，PST/Cookie/admin都不能写', async (t) => {
  const f = await fixture(t);
  for (const headers of [
    { authorization: `Bearer ${keys.m1}` },
    { 'x-user-token': keys.m1 },
    { cookie: 'nai_member_session=synthetic' },
    { 'x-admin-token': 'synthetic-trusted-read-admin' },
  ]) {
    assert.equal(
      (await f.get('/generate?' + new URLSearchParams(input()), { headers })).status,
      401,
    );
  }
  assert.equal((await f.direct(input(), { token: 'pst-private' })).status, 401);
  const accepted = await f.post(input({ token: keys.m2, nocache: 1 }), {
    headers: { authorization: 'Bearer STA1N-wrong-header' },
    key: 'body-priority',
  });
  assert.equal(accepted.status, 202, accepted.buffer.toString());
  assert.equal(
    f.db
      .prepare('SELECT member_id FROM generation_requests WHERE id=?')
      .get(json(accepted).requestId).member_id,
    'm2',
  );
  await waitFor(
    () =>
      f.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get(json(accepted).id)
        .status === 'done' && f.controls.size === 0,
  );
  const before = databaseState(f.db);
  assert.equal((await f.post(input({ token: 'STA1N-wrong-body' }))).status, 401);
  assert.equal((await f.post(input({ token: keys.m1, edit: true }))).status, 400);
  assert.equal((await f.get('/api/web/jobs', { method: 'POST' })).status, 404);
  const large = await fetch(f.origin + '/api/jobs', {
    method: 'POST',
    body: 'x'.repeat(2 * 1024 * 1024 + 1),
    headers: { authorization: 'Bearer STA1N-wrong' },
  });
  assert.equal(large.status, 413);
  await large.arrayBuffer();
  assert.equal(databaseState(f.db), before);
});

test('job跨成员403/缺失404，真实15分钟签名可转交且不被Key撤销绑死，admin仅只读', async (t) => {
  const f = await fixture(t),
    first = await f.direct(input(), { key: 'signed-source' }),
    job = f.db
      .prepare('SELECT id,artifact_id FROM generation_jobs WHERE request_id=?')
      .get(first.headers.get('x-nai-request-id')),
    signed = f.signer.imageUrl(job.artifact_id);
  assert.equal((await f.get(`/api/jobs/${job.id}?token=${keys.m2}`)).status, 403);
  assert.equal((await f.get(`/api/jobs/missing?token=${keys.m1}`)).status, 404);
  assert.equal((await f.get(`/api/jobs/${job.id}/content?token=${keys.m2}`)).status, 403);
  assert.deepEqual((await f.get(signed)).buffer, image);
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'STA1N-direct-new-key'))();
  const before = databaseState(f.db);
  assert.equal((await f.get(`/api/jobs/${job.id}?token=${keys.m1}`)).status, 401);
  assert.deepEqual(
    (await f.get(signed, { headers: { authorization: 'Bearer STA1N-invalid' } })).buffer,
    image,
  );
  assert.equal(
    (
      await f.get(`/api/jobs/${job.id}`, {
        headers: { 'x-admin-token': 'synthetic-trusted-read-admin' },
      })
    ).status,
    200,
  );
  const altered = new URL(signed, f.origin);
  altered.searchParams.set('expires', String(Number(altered.searchParams.get('expires')) + 1));
  assert.equal((await f.get(altered.pathname + altered.search)).status, 401);
  assert.equal(databaseState(f.db), before);
  f.advance(901000);
  assert.equal((await f.get(signed)).status, 401);
  assert.equal(
    (await f.get(`/api/images/${job.artifact_id}/content?token=STA1N-direct-new-key`)).status,
    200,
  );
});

test('Direct无键不合并活动任务，有键可合并；POST无键与新键均按原strict cache owner合并', async (t) => {
  for (const protocol of ['direct', 'web']) {
    await t.test(protocol, async (t) => {
      const f = await fixture(t);
      let release;
      const gate = new Promise((resolve) => {
        release = resolve;
      });
      t.after(() => release());
      f.upstream.release = release;
      f.upstream.handler = async (_req, res) => {
        if (f.upstream.calls.length === 1) {
          await gate;
        }
        res.writeHead(200, { 'content-type': 'image/webp' });
        res.end(image);
      };
      const body = input({ nocache: 0 }),
        call = (options) =>
          protocol === 'direct' ? f.direct(body, options) : f.post(body, options),
        first = call();
      if (protocol === 'web') {
        assert.equal((await first).status, 202);
      }
      await waitFor(() => f.upstream.calls.length === 1);
      const second = call();
      if (protocol === 'web') {
        assert.equal((await second).status, 202);
      }
      await waitFor(
        () =>
          f.db.prepare('SELECT COUNT(*) AS n FROM generation_requests WHERE source=?').get(protocol)
            .n === (protocol === 'direct' ? 2 : 1),
      );
      const aliased = call({ key: 'active-alias' });
      if (protocol === 'web') {
        assert.equal((await aliased).status, 202);
      }
      await waitFor(() => f.db.prepare('SELECT COUNT(*) AS n FROM idempotency_keys').get().n === 1);
      assert.equal(
        f.db.prepare('SELECT COUNT(*) AS n FROM generation_requests WHERE source=?').get(protocol)
          .n,
        protocol === 'direct' ? 2 : 1,
      );
      release();
      if (protocol === 'direct') {
        assert.equal((await first).status, 200);
        await waitFor(() => f.controls.size === 0);
        f.advance();
        assert.equal((await second).status, 200);
        assert.equal((await aliased).status, 200);
      }
      await waitFor(
        () =>
          f.controls.size === 0 &&
          f.db
            .prepare(
              "SELECT COUNT(*) AS n FROM generation_jobs WHERE status IN ('queued','pending','running')",
            )
            .get().n === 0,
      );
      assert.equal(f.upstream.calls.length, protocol === 'direct' ? 2 : 1);
      assertReplay(t, f);
    });
  }
});

test('POST最后alias SQL失败连真实refill/input全回滚，当前Key先于原receipt且全提交可重放', async (t) => {
  const f = await fixture(t, { v5: '0', secondV5: '40', seconds: 1 }),
    body = input({ model: 'nai-diffusion-5-full', nocache: 1 });
  f.db.exec(
    "CREATE TEMP TRIGGER reject_direct_alias BEFORE INSERT ON idempotency_keys BEGIN SELECT RAISE(ABORT,'direct alias rollback'); END",
  );
  const before = databaseState(f.db);
  assert.equal((await f.post(body, { key: 'rollback' })).status, 500);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.upstream.calls.length, 0);
  f.db.exec('DROP TRIGGER reject_direct_alias');
  const healthy = await f.post(body, { key: 'rollback' });
  assert.equal(healthy.status, 202, healthy.buffer.toString());
  const job = json(healthy);
  await waitFor(
    () =>
      f.controls.size === 0 &&
      f.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get(job.id).status === 'done',
  );
  assert.equal(
    f.db.prepare("SELECT COUNT(*) AS n FROM domain_events WHERE event_type='RefillApplied'").get()
      .n,
    1,
  );
  f.defaults.defaultArtist = 'changed current artist';
  const reads = f.defaultReads,
    saved = databaseState(f.db),
    replay = await f.post(body, { key: 'rollback' });
  assert.equal(replay.status, 202);
  assert.equal(json(replay).id, job.id);
  assert.equal(f.defaultReads, reads);
  assert.equal(databaseState(f.db), saved);
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'STA1N-direct-after-rollback'))();
  const revoked = databaseState(f.db);
  assert.equal((await f.post(body, { key: 'rollback' })).status, 401);
  assert.equal(databaseState(f.db), revoked);
  const current = await f.post(body, {
    key: 'rollback',
    headers: { authorization: 'Bearer STA1N-direct-after-rollback' },
  });
  assert.equal(current.status, 202, current.buffer.toString());
  assert.equal(json(current).id, job.id);
  assert.equal(databaseState(f.db), revoked);
  assert.equal(f.upstream.calls.length, 1);
  assertReplay(t, f);
});

test('Direct源finite seed原样经过真实HTTP/执行/冷读/回放，非finite仍SVG拒绝且零受理', async (t) => {
  const f = await fixture(t);
  for (const seed of [0, -1, -2, 1.5, 4294967296]) {
    const response = await f.direct(input({ seed, nocache: 1 }), { key: `seed-${seed}` });
    assert.equal(response.status, 200, response.buffer.toString());
    assert.deepEqual(response.buffer, image);
    assert.equal(f.upstream.calls.at(-1).body.parameters.seed, seed);
    const job = f.db
      .prepare('SELECT id,actual_seed FROM generation_jobs WHERE request_id=?')
      .get(response.headers.get('x-nai-request-id'));
    assert.equal(job.actual_seed, seed);
    assert.equal(f.inputs.readJobInput(job.id).seed, seed);
    const before = databaseState(f.db);
    assert.deepEqual(
      (await f.direct(input({ seed, nocache: 1 }), { key: `seed-${seed}` })).buffer,
      image,
    );
    assert.equal(databaseState(f.db), before);
    f.advance();
  }
  const before = databaseState(f.db);
  for (const seed of ['NaN', 'Infinity', '-Infinity']) {
    const response = await f.direct(input({ seed, nocache: 1 }), { key: `seed-${seed}` });
    assert.equal(response.status, 200);
    assert.match(response.headers.get('content-type'), /image\/svg\+xml/);
    assert.equal(response.headers.get('x-error'), '1');
    assert.equal(databaseState(f.db), before);
  }
  assert.equal(f.upstream.calls.length, 5);
  const random = input({ nocache: 1 });
  delete random.seed;
  const original = await f.direct(random, { key: 'random-first' });
  assert.equal(original.status, 200, original.buffer.toString());
  const saved = databaseState(f.db),
    actual = f.upstream.calls.at(-1).body.parameters.seed,
    replay = await f.direct(random, { key: 'random-first' });
  assert.equal(replay.headers.get('x-nai-request-id'), original.headers.get('x-nai-request-id'));
  assert.equal(
    (await f.direct({ ...random, seed: actual }, { key: 'random-first' })).headers.get('x-error'),
    '1',
  );
  assert.equal(databaseState(f.db), saved);
  assert.equal(f.upstream.calls.length, 6);
  assertReplay(t, f);
});

test('job/image真实文件读取后重核原Key，真实签名也不能跨越ready失效', async (t) => {
  for (const mode of ['job-key', 'image-key', 'signed-ready']) {
    await t.test(mode, async (t) => {
      const f = await fixture(t),
        first = await f.direct(input(), { key: 'io-source' }),
        job = f.db
          .prepare('SELECT id,artifact_id FROM generation_jobs WHERE request_id=?')
          .get(first.headers.get('x-nai-request-id'));
      let changed;
      const originalOpen = fs.open;
      const mocked = t.mock.method(fs, 'open', async (...args) => {
        const handle = await originalOpen(...args);
        if (String(args[0]).startsWith(path.join(f.dir, 'images') + path.sep)) {
          const read = handle.readFile.bind(handle);
          handle.readFile = async (...values) => {
            const bytes = await read(...values);
            if (!changed) {
              if (mode === 'signed-ready') {
                f.db.prepare("UPDATE projection_state SET status='building'").run();
              } else {
                f.db.transaction(() =>
                  f.members.replaceMemberApiKey('m1', 'STA1N-direct-after-io'),
                )();
              }
              changed = databaseState(f.db);
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
      const route =
        mode === 'job-key'
          ? `/api/jobs/${job.id}/content?token=${keys.m1}`
          : mode === 'image-key'
            ? `/api/images/${job.artifact_id}/content?token=${keys.m1}`
            : f.signer.imageUrl(job.artifact_id);
      const response = await f.get(route);
      assert.ok(changed, '必须先实际读取原文件');
      assert.equal(
        response.status,
        mode === 'signed-ready' ? 500 : 401,
        response.buffer.toString(),
      );
      assert.notDeepEqual(response.buffer, image);
      assert.equal(databaseState(f.db), changed);
      assert.equal(f.upstream.calls.length, 1);
    });
  }
});

test('preview真实上游帧沿同一memberID实例，实际send在当前Key事务内，撤销后旧订阅不能拿新帧', async (t) => {
  const f = await fixture(t, { stream: true });
  f.server.on('request', (req, res) => {
    if (!req.url.startsWith('/api/jobs/events')) {
      return;
    }
    const write = res.write.bind(res);
    res.write = (chunk, ...args) => {
      if (String(chunk).startsWith('data: ')) {
        assert.equal(f.db.inTransaction, true);
      }
      return write(chunk, ...args);
    };
  });
  let second, finish;
  const secondGate = new Promise((resolve) => {
      second = resolve;
    }),
    finalGate = new Promise((resolve) => {
      finish = resolve;
    });
  f.upstream.release = () => {
    second();
    finish();
  };
  t.after(f.upstream.release);
  const frame = (value) => {
    const bytes = Buffer.from(encode(value)),
      header = Buffer.alloc(4);
    header.writeUInt32BE(bytes.length);
    return Buffer.concat([header, bytes]);
  };
  f.upstream.handler = async (_req, res) => {
    res.writeHead(200, { 'content-type': 'application/msgpack' });
    res.write(frame({ event_type: 'intermediate', samp_ix: 0, step_ix: 1, image }));
    await secondGate;
    await new Promise((resolve) => setTimeout(resolve, 160));
    res.write(frame({ event_type: 'intermediate', samp_ix: 0, step_ix: 3, image }));
    await finalGate;
    res.end(frame({ event_type: 'final', samp_ix: 0, image }));
  };
  const subscribe = async (token) => {
    const controller = new AbortController();
    t.after(() => controller.abort());
    const response = await fetch(f.origin + `/api/jobs/events?token=${token}`, {
      signal: controller.signal,
    });
    assert.equal(response.status, 200);
    const reader = response.body.getReader(),
      decoder = new TextDecoder();
    let buffer = '';
    return async () => {
      while (true) {
        const boundary = buffer.indexOf('\n\n');
        if (boundary !== -1) {
          const chunk = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          if (chunk.startsWith('data: ')) {
            return JSON.parse(chunk.slice(6));
          }
          continue;
        }
        const next = await reader.read();
        if (next.done) {
          return null;
        }
        buffer += decoder.decode(next.value, { stream: true });
      }
    };
  };
  const old = await subscribe(keys.m1),
    accepted = await f.post(input({ nocache: 1 })),
    job = json(accepted);
  assert.equal(accepted.status, 202, accepted.buffer.toString());
  const preview = await old();
  assert.equal(preview.jobId, job.id);
  assert.match(preview.preview, /^data:image\/webp;base64,/);
  assert.equal(f.previews.listeners.has('m1'), true);
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'STA1N-direct-preview-current'))();
  second();
  assert.equal(await old(), null);
  const current = await subscribe('STA1N-direct-preview-current'),
    newest = await current();
  assert.equal(newest.jobId, job.id);
  assert.ok(newest.progress.step > preview.progress.step);
  assert.equal(f.previews.listeners.get('m1').size, 1);
  finish();
  await waitFor(
    () =>
      f.controls.size === 0 &&
      f.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get(job.id).status === 'done',
  );
  assert.equal((await f.get(`/api/jobs/${job.id}?token=STA1N-direct-preview-current`)).status, 200);
  assert.equal(f.upstream.calls.length, 1);
});

test('原失败job content始终200错误SVG，真实迟到产物只从image原领取且不补扣', async (t) => {
  const f = await fixture(t);
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  f.upstream.release = release;
  t.after(() => release());
  f.upstream.handler = async (_req, res) => {
    await gate;
    res.writeHead(200, { 'content-type': 'image/webp' });
    res.end(image);
  };
  const accepted = await f.post(input({ nocache: 1 })),
    job = json(accepted);
  assert.equal(accepted.status, 202);
  await waitFor(() => f.upstream.calls.length === 1);
  f.fail(job.id, { errorCode: 'upstream_unavailable', errorStatus: 502, aborted: false }, f.clock);
  const failedAt = f.db
      .prepare('SELECT completed_at FROM generation_jobs WHERE id=?')
      .get(job.id).completed_at,
    refunded = f.generations.readCharge(job.id),
    wallet = f.generations.readWallet('a', 'm1');
  assert.equal(refunded.status, 'refunded');
  const failed = await f.get(`/api/jobs/${job.id}/content?token=${keys.m1}`);
  assert.equal(failed.status, 200);
  assert.equal(failed.headers.get('x-error'), '1');
  assert.match(failed.headers.get('content-type'), /image\/svg\+xml/);
  release();
  await waitFor(
    () =>
      f.controls.size === 0 &&
      Boolean(
        f.db.prepare('SELECT artifact_id FROM generation_jobs WHERE id=?').get(job.id).artifact_id,
      ),
  );
  const detail = json(await f.get(`/api/jobs/${job.id}?token=${keys.m1}`));
  assert.equal(detail.status, 'failed');
  assert.equal(detail.lateResultAvailable, true);
  assert.equal(
    f.db.prepare('SELECT completed_at FROM generation_jobs WHERE id=?').get(job.id).completed_at,
    failedAt,
  );
  assert.deepEqual(f.generations.readCharge(job.id), refunded);
  assert.deepEqual(f.generations.readWallet('a', 'm1'), wallet);
  assert.equal(
    (await f.get(`/api/jobs/${job.id}/content?token=${keys.m1}`)).headers.get('x-error'),
    '1',
  );
  assert.deepEqual((await f.get(detail.imageUrl)).buffer, image);
  assert.equal(f.upstream.calls.length, 1);
  assertReplay(t, f);
});

test('Direct超时/断线只结束领取，保留原request/charge/物理输入，随后原键零写领取', async (t) => {
  for (const mode of ['timeout', 'disconnect']) {
    await t.test(mode, async (t) => {
      const f = await fixture(t, { timeoutMs: 100 });
      let release;
      const gate = new Promise((resolve) => {
        release = resolve;
      });
      f.upstream.release = release;
      t.after(() => release());
      f.upstream.handler = async (_req, res) => {
        await gate;
        res.writeHead(200, { 'content-type': 'image/webp' });
        res.end(image);
      };
      const body = input({ nocache: 1 }),
        controller = new AbortController(),
        pending = f.direct(body, { key: 'connection', signal: controller.signal });
      await waitFor(() => f.upstream.calls.length === 1);
      const job = f.db
        .prepare(
          "SELECT j.id,r.id AS requestId FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id WHERE r.source='direct'",
        )
        .get();
      if (mode === 'timeout') {
        f.advance(101);
        const response = await pending;
        assert.equal(response.status, 200);
        assert.equal(response.headers.get('x-error'), '1');
        assert.equal(response.headers.get('x-timeout'), '1');
      } else {
        controller.abort();
        await assert.rejects(pending, (e) => e.name === 'AbortError');
      }
      await waitFor(() => f.events.listenerCount('commit') === 0);
      assert.equal(
        f.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get(job.id).status,
        'running',
      );
      assert.equal(f.generations.readCharge(job.id).status, 'reserved');
      assert.equal(f.controls.size, 1);
      assert.ok(f.budget.activeBytes() > 0);
      assert.equal((await f.get(`/api/jobs/${job.id}/content?token=${keys.m1}`)).status, 409);
      release();
      await waitFor(
        () =>
          f.controls.size === 0 &&
          f.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get(job.id).status ===
            'done',
      );
      const before = databaseState(f.db),
        response = await f.direct(body, { key: 'connection' });
      assert.deepEqual(response.buffer, image);
      assert.equal(response.headers.get('x-nai-request-id'), job.requestId);
      assert.equal(databaseState(f.db), before);
      assert.equal(f.upstream.calls.length, 1);
      assertReplay(t, f);
    });
  }
});

test('Direct实际上游429保持原200 busy错误SVG和requestID，不泄漏上游正文', async (t) => {
  const f = await fixture(t);
  f.upstream.handler = async (_req, res) => {
    res.writeHead(429, { 'content-type': 'application/json' });
    res.end(
      JSON.stringify({
        message: 'Concurrent generation limit exceeded. synthetic-private-upstream',
      }),
    );
  };
  const response = await f.direct(input({ nocache: 1 }), { key: 'busy' });
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('x-busy'), '1');
  assert.equal(response.headers.get('x-error'), '1');
  assert.ok(response.headers.get('x-nai-request-id'));
  assert.match(response.headers.get('content-type'), /image\/svg\+xml/);
  assert.doesNotMatch(response.buffer.toString(), /synthetic-private-upstream/);
  assert.equal(f.upstream.calls.length, 1);
});

test('POST实际正文读取期间换Key或撤ready，body身份仅在限量读完后核验且零受理', async (t) => {
  for (const change of ['key', 'ready']) {
    await t.test(change, async (t) => {
      let f, before;
      f = await fixture(t, {
        onBodyChunk() {
          if (!before) {
            if (change === 'key') {
              f.db.transaction(() =>
                f.members.replaceMemberApiKey('m1', 'STA1N-direct-body-current'),
              )();
            } else {
              f.db.prepare("UPDATE projection_state SET status='building'").run();
            }
            before = databaseState(f.db);
          }
        },
      });
      const response = await f.post(input({ token: keys.m1 }), { key: 'body-auth' });
      assert.ok(before);
      assert.equal(response.status, change === 'key' ? 401 : 500);
      assert.equal(databaseState(f.db), before);
      assert.equal(f.upstream.calls.length, 0);
    });
  }
});

test('Direct缓存实际资格IO中stop/resume不能借旧admission epoch追加回执，原键仍零写重领', async (t) => {
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { env }),
    source = await f.direct(input(), { key: 'epoch-source' });
  assert.equal(source.status, 200);
  const file = f.db
      .prepare(
        "SELECT p.object_key FROM artifact_replicas p JOIN generation_jobs j ON j.artifact_id=p.artifact_id JOIN generation_requests r ON r.id=j.request_id JOIN storage_targets s ON s.id=p.storage_target_id WHERE r.source='direct' AND s.kind='local'",
      )
      .get(),
    members = new RelationalMemberStore(f.db, env),
    originalStat = fs.lstat;
  let changed;
  const mocked = t.mock.method(fs, 'lstat', async (...args) => {
    const stat = await originalStat(...args);
    if (!changed && String(args[0]) === path.join(f.dir, 'images', file.object_key)) {
      await withAuthenticatedMemberApiKey(
        members,
        keys.m1,
        (actor) => {
          members.changeMemberAdmission(
            meta('direct-cache-stop', f.clock, 'm1'),
            { memberId: 'm1', acceptingJobs: false },
            actor,
          );
          members.changeMemberAdmission(
            meta('direct-cache-resume', f.clock, 'm1'),
            { memberId: 'm1', acceptingJobs: true },
            actor,
          );
        },
        env,
      );
      changed = databaseState(f.db);
    }
    return stat;
  });
  syncBuiltinESMExports();
  t.after(() => {
    mocked.mock.restore();
    syncBuiltinESMExports();
  });
  const stale = await f.direct(input(), { key: 'epoch-stale' });
  assert.ok(changed);
  assert.equal(stale.status, 200);
  assert.equal(stale.headers.get('x-error'), '1');
  assert.equal(databaseState(f.db), changed);
  assert.equal(f.upstream.calls.length, 1);
  assert.deepEqual((await f.direct(input(), { key: 'epoch-source' })).buffer, image);
  assert.equal(databaseState(f.db), changed);
  const healthy = await f.direct(input(), { key: 'epoch-stale' });
  assert.deepEqual(healthy.buffer, image);
  assert.equal(healthy.headers.get('x-cache'), 'hit');
  assert.equal(f.upstream.calls.length, 1);
  assertReplay(t, f);
});

test('POST原键Local真丢失410且不重生，正常及恢复原字节仍202零新事实', async (t) => {
  const f = await fixture(t),
    body = input({ nocache: true }),
    key = 'post-local-original';
  const accepted = await f.post(body, { key });
  assert.equal(accepted.status, 202, accepted.buffer.toString());
  const original = json(accepted);
  await waitFor(
    () =>
      f.controls.size === 0 &&
      f.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get(original.id).status ===
        'done',
  );
  const before = databaseState(f.db),
    healthy = await f.post(body, { key });
  assert.equal(healthy.status, 202, healthy.buffer.toString());
  const done = json(healthy);
  assert.equal(done.id, original.id);
  assert.equal(done.status, 'done');
  assert.equal(databaseState(f.db), before);
  const file = path.join(f.dir, `images/${done.imageId}.webp`);
  await fs.rename(file, file + '.saved');
  const missing = await f.post(body, { key });
  assert.equal(missing.status, 410, missing.buffer.toString());
  assert.equal(databaseState(f.db), before);
  assert.equal(f.upstream.calls.length, 1);
  await fs.rename(file + '.saved', file);
  const restored = await f.post(body, { key });
  assert.equal(restored.status, 202, restored.buffer.toString());
  assert.equal(json(restored).id, original.id);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.upstream.calls.length, 1);
  assertReplay(t, f);
});

test('Direct原键云承诺缺配置不重生，原图片错误SVG与job/image503保持同一原结果', async (t) => {
  const f = await fixture(t),
    body = input(),
    first = await f.direct(body, { key: 'cloud-original' }),
    requestId = first.headers.get('x-nai-request-id'),
    job = f.db
      .prepare('SELECT id,artifact_id FROM generation_jobs WHERE request_id=?')
      .get(requestId),
    cloud = await replicateMockR2(t, f, job.artifact_id),
    file = path.join(f.dir, `images/${job.artifact_id}.webp`);
  await fs.rename(file, `${file}.saved`);
  const before = databaseState(f.db);
  delete f.replicas.r2;
  const unavailable = await f.direct(body, { key: 'cloud-original' });
  assert.equal(unavailable.status, 200);
  assert.equal(unavailable.headers.get('x-error'), '1');
  assert.equal(unavailable.headers.get('x-nai-request-id'), requestId);
  assert.equal((await f.get(`/api/jobs/${job.id}/content?token=${keys.m1}`)).status, 503);
  assert.equal((await f.get(f.signer.imageUrl(job.artifact_id))).status, 503);
  assert.deepEqual(cloud.calls, ['PUT', 'GET']);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.upstream.calls.length, 1);
  f.replicas.r2 = cloud.r2;
  assert.deepEqual((await f.direct(body, { key: 'cloud-original' })).buffer, image);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.upstream.calls.length, 1);
});

test('Direct实际Discord撤销在原事务完成后通知同一runtime，不受理或广播重复提交', async (t) => {
  const env = {
    DISCORD_CLIENT_ID: '123456789012345678',
    DISCORD_CLIENT_SECRET: 'synthetic-discord-private-secret',
    DISCORD_GUILD_ID: '1134557553011998840',
    PUBLIC_BASE_URL: 'https://synthetic.example',
  };
  let checks = 0;
  const f = await fixture(t, {
    env,
    httpDependencies: {
      async discordFetch(url) {
        assert.equal(f.db.inTransaction, false);
        assert.match(String(url), /users\/@me\/guilds\/.+\/member$/);
        checks++;
        return Response.json({ user: { id: '123456789012345679' }, pending: true });
      },
    },
  });
  await f.members.discordAuthenticationTransaction((tx) => {
    const member = tx.readMember('m1');
    member.discord = {
      id: '123456789012345679',
      guildId: env.DISCORD_GUILD_ID,
      accessToken: 'synthetic-private-discord-access',
      refreshToken: 'synthetic-private-discord-refresh',
      expiresAt: Date.now() + 86400000,
      checkedAt: 0,
      member: true,
    };
    tx.saveAuthentication(member);
  });
  const commits = [],
    cut = f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n,
    wallets = f.db.prepare('SELECT * FROM wallets ORDER BY rowid').all(),
    jobs = f.db.prepare('SELECT * FROM generation_jobs ORDER BY rowid').all(),
    charges = f.db.prepare('SELECT * FROM charges ORDER BY rowid').all();
  f.events.on('commit', (event) => {
    assert.equal(f.db.inTransaction, false);
    commits.push(event);
  });
  const response = await f.direct();
  assert.equal(response.status, 403);
  assert.equal(checks, 1);
  assert.equal(f.upstream.calls.length, 0);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n, cut + 1);
  assert.deepEqual(
    readRelationalCommit(f.db, String(cut + 1)).events.map((event) => event.eventType),
    ['MemberEligibilityChanged'],
  );
  assert.deepEqual(f.db.prepare('SELECT * FROM wallets ORDER BY rowid').all(), wallets);
  assert.deepEqual(f.db.prepare('SELECT * FROM generation_jobs ORDER BY rowid').all(), jobs);
  assert.deepEqual(f.db.prepare('SELECT * FROM charges ORDER BY rowid').all(), charges);
  assert.equal(commits.length, 1);
  assert.deepEqual(commits[0].memberIds, ['m1']);
  assert.ok(commits[0].collections.includes('sessions'));
  assert.equal(commits[0].allMembers, false);
});
