import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import Database from 'better-sqlite3';
import {
  childFixture,
  launchGateway,
  enableAccount,
  query,
  assertChildPublicReplay,
} from '../helpers/relational-child.mjs';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';

const model = 'nai-diffusion-4-5-full';
async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}
async function until(predicate, timeout = 10000) {
  const deadline = Date.now() + timeout;
  while (!predicate()) {
    if (Date.now() >= deadline) {
      throw new Error('synthetic cooldown wait timed out');
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

async function fixture(t, { hold = false } = {}) {
  const fixture = await childFixture(t, 'shared-cooldown-http'),
    member = fixture.members[0];
  const jobs = () =>
    query(
      fixture,
      `SELECT j.*,r.member_id AS member_id FROM generation_jobs j
    JOIN generation_requests r ON r.id=j.request_id ORDER BY j.queue_seq`,
    );
  const account = () =>
    query(
      fixture,
      `SELECT a.*,q.observed_v5,q.observed_anlas_fixed,q.observed_anlas_purchased,
    q.seconds_per_percent FROM upstream_accounts a JOIN account_quota_state q ON q.account_id=a.id WHERE a.id=?`,
      fixture.accountId,
    )[0];
  let release;
  const released = new Promise((resolve) => {
    release = resolve;
  });
  const calls = [];
  let quotaFailure = false,
    missingQuotaField = null,
    wire = {};
  const upstream = http.createServer(async (req, res) => {
    if (req.url === '/user/data') {
      if (quotaFailure) {
        res.writeHead(503, { 'content-type': 'text/plain' });
        res.end('SYNTHETIC_PRIVATE_QUOTA_ROOT');
        return;
      }
      const subscription = {
        tier: 3,
        active: true,
        expiresAt: Math.floor(Date.now() / 1000) + 86400,
        trainingStepsLeft: {
          fixedTrainingStepsLeft: 10000,
          purchasedTrainingSteps: 0,
          ...wire.trainingStepsLeft,
        },
        usage: { percent: 100, isNegative: false, timeUntilNextPercent: 0, ...wire.usage },
      };
      if (missingQuotaField === 'v5') {
        delete subscription.usage.percent;
      }
      if (missingQuotaField === 'fixed') {
        delete subscription.trainingStepsLeft.fixedTrainingStepsLeft;
      }
      if (missingQuotaField === 'purchased') {
        delete subscription.trainingStepsLeft.purchasedTrainingSteps;
      }
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ subscription }));
      return;
    }
    const chunks = [];
    for await (const chunk of req) {
      chunks.push(chunk);
    }
    calls.push({ path: req.url, body: JSON.parse(Buffer.concat(chunks).toString('utf8')) });
    if (hold) {
      await released;
    }
    res.writeHead(429, { 'content-type': 'application/json', 'Retry-After': '300' });
    res.end(JSON.stringify({ message: 'Concurrent generation is locked' }));
  });
  const upstreamPort = await listen(upstream);
  let server, base;
  const start = async () => {
    server = await launchGateway(fixture, `http://127.0.0.1:${upstreamPort}`, {
      PUBLIC_HOST: 'gateway.example',
      OPENAI_CHAT_TIMEOUT_MS: '15000',
    });
    base = server.origin;
  };
  const stop = async (signal) => (signal === 'SIGKILL' ? server.kill() : server.stop());
  t.after(async () => {
    release();
    upstream.closeAllConnections();
    await new Promise((resolve) => upstream.close(resolve));
  });
  const call = (endpoint, { body, key, headers = {} } = {}) =>
    fetch(base + endpoint, {
      method: body === undefined ? 'GET' : 'POST',
      signal: AbortSignal.timeout(20000),
      headers: {
        authorization: `Bearer ${member.api_key}`,
        ...(key === undefined ? {} : { 'Idempotency-Key': key }),
        ...(body === undefined ? {} : { 'content-type': 'application/json' }),
        ...headers,
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  const sharedStatus = async () => {
    const login = await call('/api/member/login', {
      body: { username: member.username, password: member.password },
      headers: { origin: base },
    });
    assert.equal(login.status, 200);
    await login.json();
    const response = await call('/api/member/shared-status', {
      headers: { cookie: login.headers.get('set-cookie').split(';')[0] },
    });
    assert.equal(response.status, 200);
    return response.json();
  };
  await start();
  await enableAccount(fixture, server);
  const quotaSummary = () => {
    const db = new Database(fixture.filename, { readonly: true });
    try {
      return new RelationalQuotaStore(db, fixture.env).readMemberQuotaSummary(
        member.user_id,
        Date.now(),
      );
    } finally {
      db.close();
    }
  };
  const financialState = () => ({
    quota: query(
      fixture,
      `SELECT baseline_event_id,refill_cursor_at,settlement_cursor_at,unallocated_v5,
      fallback_fixed,fallback_purchased,shared_v5_debt,pending_v5_debit,pending_fixed_debit,pending_purchased_debit,
      pending_fallback_v5,pending_fallback_fixed,pending_fallback_purchased FROM account_quota_state WHERE account_id=?`,
      fixture.accountId,
    ),
    records: Object.fromEntries(
      [
        'wallets',
        'charges',
        'operating_state',
        'generation_jobs',
        'generation_attempts',
        'generation_requests',
        'idempotency_keys',
      ].map((table) => [table, query(fixture, `SELECT * FROM ${table} ORDER BY rowid`)]),
    ),
  });
  return {
    start,
    stop,
    call,
    sharedStatus,
    jobs,
    account,
    quotaSummary,
    calls,
    release,
    financialState,
    memberKey: member.api_key,
    async observeQuota({ failure = false, missing = null, overrides = {} } = {}) {
      quotaFailure = failure;
      missingQuotaField = missing;
      wire = overrides;
      const response = await call('/api/admin/accounts/quota', {
        body: { ids: [account().id] },
        headers: { origin: base, 'x-admin-token': fixture.env.ADMIN_TOKEN },
      });
      assert.equal(response.status, 200, await response.clone().text());
      return response.json();
    },
    privateEvidence() {
      return {
        log: server.output,
        events: query(
          fixture,
          "SELECT payload FROM domain_events WHERE event_type='QuotaObserved' ORDER BY commit_seq,event_index",
        ),
      };
    },
    verifyReplay: () => assertChildPublicReplay(fixture),
    attempts: () => query(fixture, 'SELECT * FROM generation_attempts ORDER BY rowid'),
    charges: () => query(fixture, 'SELECT * FROM charges ORDER BY rowid'),
    cooldownFacts: () =>
      query(
        fixture,
        "SELECT payload FROM domain_events WHERE event_type='CooldownChanged' ORDER BY commit_seq,event_index",
      ).map((row) => JSON.parse(row.payload)),
    assertAlive() {
      assert.equal(server.exit, null);
      assert.doesNotMatch(server.output, /ERR_HTTP_HEADERS_SENT/);
    },
  };
}

function assertRetryAfter(response, deadline, before, after) {
  const text = response.headers.get('retry-after');
  assert.match(text, /^\d+$/);
  const seconds = Number(text),
    until = Date.parse(deadline);
  assert.ok(
    seconds >= Math.ceil((until - after) / 1000) && seconds <= Math.ceil((until - before) / 1000),
    `Retry-After ${seconds} must describe remaining time until ${deadline}`,
  );
  return seconds;
}

test(
  '真实 Native 429、幂等失败重放和强制重启共用持久冷却截止时间',
  { timeout: 45000 },
  async (t) => {
    const gateway = await fixture(t);
    const body = {
      input: 'synthetic cooldown check',
      model,
      action: 'generate',
      parameters: { width: 1024, height: 1024, steps: 23, seed: 7, n_samples: 1 },
    };
    const call = () => gateway.call('/ai/generate-image', { body, key: 'capacity-operation' });
    const before = Date.now(),
      first = await call(),
      after = Date.now();
    assert.equal(first.status, 429);
    assert.match((await first.json()).message, /并发/);
    const requestId = first.headers.get('x-nai-request-id');
    assert.ok(requestId);
    const saved = gateway.account(),
      [job] = gateway.jobs(),
      deadline = new Date(saved.cooldown_until).toISOString();
    const firstRemaining = assertRetryAfter(first, deadline, before, after);
    assert.ok(firstRemaining > 120 && firstRemaining <= 300);
    assert.equal(saved.cooldown_reason, 'upstream_retry_after');
    assert.equal(saved.capacity_strikes, 1);
    assert.equal(gateway.jobs().filter((job) => job.status === 'failed').length, 1);
    assert.equal(gateway.jobs().filter((job) => job.status === 'running').length, 0);
    assert.equal(job.status, 'failed');
    assert.equal(job.error_status, 429);
    assert.equal(job.request_id, requestId);
    assert.equal(gateway.attempts()[0].upstream_http_status, 429);
    assert.equal(gateway.attempts()[0].job_id, job.id);
    assert.equal(gateway.charges()[0].status, 'refunded');
    const cooldown = gateway.cooldownFacts().at(-1);
    assert.equal(new Date(cooldown.cooldownUntil).toISOString(), deadline);
    assert.equal(cooldown.completionKind, 'capacity');
    assert.equal(cooldown.attemptId, gateway.attempts()[0].id);
    assert.equal(gateway.calls.length, 1);
    const checkShared = async () => {
      const status = await gateway.sharedStatus();
      assert.equal(status.status, 'cooldown');
      assert.equal(status.cooldownUntil, deadline);
      assert.equal(status.cooldownReason, 'upstream_retry_after');
      assert.equal(status.recentFailures, 1);
      assert.equal(status.running, 0);
      assert.equal(status.queued, 0);
      assert.equal(status.pending, 0);
    };
    await checkShared();
    // 跨过一个秒边界，证明重放返回剩余时间，而非重新开始 300 秒。
    await until(() => Date.now() >= Date.parse(deadline) - (firstRemaining - 1) * 1000);
    const replay = async () => {
      const started = Date.now(),
        response = await call(),
        ended = Date.now();
      assert.equal(response.status, 429);
      await response.json();
      assert.equal(response.headers.get('x-nai-request-id'), requestId);
      assert.ok(assertRetryAfter(response, deadline, started, ended) < firstRemaining);
      assert.equal(new Date(gateway.account().cooldown_until).toISOString(), deadline);
      assert.equal(gateway.account().capacity_strikes, 1);
      assert.equal(gateway.jobs().filter((job) => job.status === 'failed').length, 1);
      assert.equal(gateway.jobs().length, 1);
      assert.deepEqual(gateway.jobs()[0], job);
      assert.equal(gateway.calls.length, 1);
    };
    await replay();
    await gateway.stop('SIGKILL');
    await gateway.start();
    await checkShared();
    await replay();
    gateway.assertAlive();
    await gateway.stop();
    gateway.verifyReplay();
  },
);

test(
  '刷新失败及不完整余额优先于冷却，所有生成协议拒绝新意图并保留原受理重放',
  { timeout: 45000 },
  async (t) => {
    const gateway = await fixture(t);
    const body = {
      input: 'synthetic quota admission',
      model,
      action: 'generate',
      parameters: { width: 1024, height: 1024, steps: 23, seed: 7, n_samples: 1 },
    };
    const original = await gateway.call('/ai/generate-image', {
      body,
      key: 'before-quota-failure',
    });
    assert.equal(original.status, 429);
    await original.json();
    const requestId = original.headers.get('x-nai-request-id');
    const history = gateway.privateEvidence().events;
    for (const observation of [
      { failure: true },
      ...['v5', 'fixed', 'purchased'].map((missing) => ({ missing })),
      { overrides: { usage: { percent: null } } },
    ]) {
      await gateway.observeQuota(observation);
      const expected = observation.failure ? 'quota_sync_error' : 'unknown_balance';
      assert.equal((await gateway.sharedStatus()).status, expected);
      const before = gateway.financialState();
      const denied = await gateway.call('/ai/generate-image', {
        body,
        key: `blocked-${expected}-${observation.missing}`,
      });
      assert.equal(denied.status, 503);
      assert.match(
        (await denied.json()).message,
        observation.failure ? /额度同步失败/ : /额度不完整/,
      );
      const openAi = await gateway.call('/v1/chat/completions', {
        body: {
          model: `${model}:k_euler_ancestral`,
          stream: false,
          messages: [
            {
              role: 'user',
              content:
                '提示词: synthetic quota admission\n画师串:\n尺寸: 方图\n提示词引导值: 5\n缩放引导值: 0\n负面提示词:',
            },
          ],
        },
      });
      assert.equal(openAi.status, 503);
      assert.doesNotMatch(
        await openAi.text(),
        /SYNTHETIC_PRIVATE|quota_sync_error|unknown_balance/,
      );
      const query = new URLSearchParams({
        token: gateway.memberKey,
        prompt: 'synthetic quota admission',
        model,
        seed: '7',
        nocache: '1',
      });
      const direct = await gateway.call('/generate?' + query);
      assert.equal(direct.status, 200);
      assert.match(direct.headers.get('content-type'), /^image\//);
      assert.doesNotMatch(
        await direct.text(),
        /SYNTHETIC_PRIVATE|quota_sync_error|unknown_balance/,
      );
      const replay = await gateway.call('/ai/generate-image', {
        body,
        key: 'before-quota-failure',
      });
      assert.equal(replay.status, 429);
      await replay.json();
      assert.equal(replay.headers.get('x-nai-request-id'), requestId);
      assert.deepEqual(gateway.financialState(), before);
      assert.equal(gateway.calls.length, 1);
      assert.doesNotMatch(
        JSON.stringify(gateway.privateEvidence()),
        /SYNTHETIC_PRIVATE_QUOTA_ROOT/,
      );
    }
    assert.deepEqual(gateway.privateEvidence().events.slice(0, history.length), history);
    assert.equal(gateway.account().observed_v5, null);
    await gateway.stop();
    gateway.verifyReplay();
    await gateway.start();
    assert.equal((await gateway.sharedStatus()).status, 'unknown_balance');
    const reopened = await gateway.call('/ai/generate-image', {
      body,
      key: 'before-quota-failure',
    });
    assert.equal(reopened.status, 429);
    await reopened.json();
    assert.equal(reopened.headers.get('x-nai-request-id'), requestId);
    assert.equal(gateway.calls.length, 1);
    await gateway.observeQuota();
    assert.equal((await gateway.sharedStatus()).status, 'cooldown');
    gateway.assertAlive();
    await gateway.stop();
    gateway.verifyReplay();
  },
);

test('无效上游余额分别写为未知且禁止 V5 HTTP 预留，有效零及可选间隔不妨碍恢复', async (t) => {
  const body = {
    input: 'synthetic wire quota admission',
    model: 'nai-diffusion-5-full',
    action: 'generate',
    parameters: { width: 1024, height: 1024, steps: 23, seed: 7, n_samples: 1 },
  };
  const fields = [
    ['observed_v5', { usage: { percent: null } }],
    ['observed_anlas_fixed', { trainingStepsLeft: { fixedTrainingStepsLeft: false } }],
    ['observed_anlas_purchased', { trainingStepsLeft: { purchasedTrainingSteps: [] } }],
  ];
  for (const [field, overrides] of fields) {
    await t.test(field, { timeout: 25000 }, async (t) => {
      const gateway = await fixture(t);
      await gateway.observeQuota();
      const before = gateway.financialState();
      await gateway.observeQuota({ overrides });
      const saved = gateway.account(),
        fair = gateway.quotaSummary(),
        shared = await gateway.sharedStatus();
      const denied = await gateway.call('/ai/generate-image', { body, key: field });
      const error = await denied.json();
      assert.deepEqual(
        [denied.status, saved[field], fair.status, shared.status],
        [503, null, 'unknown_balance', 'unknown_balance'],
      );
      assert.match(error.message, /额度不完整/);
      assert.equal(denied.headers.get('x-nai-request-id'), null);
      assert.deepEqual(gateway.financialState(), before);
      assert.equal(gateway.calls.length, 0);

      await gateway.observeQuota({
        overrides: { usage: { percent: 0, timeUntilNextPercent: [] } },
      });
      const recovered = gateway.account();
      assert.equal(recovered.observed_v5, '0');
      assert.equal(recovered.observed_anlas_purchased, '0');
      assert.equal(recovered.seconds_per_percent, null);
      assert.equal(gateway.quotaSummary().status, 'active');
      assert.equal((await gateway.sharedStatus()).status, 'active');
      const depleted = await gateway.call('/ai/generate-image', { body, key: 'known-zero' });
      assert.equal(depleted.status, 402);
      await depleted.json();
      assert.equal(gateway.calls.length, 0);
      await gateway.stop();
      gateway.verifyReplay();
    });
  }
});

test(
  'OpenAI 已发出 SSE 响应头后的容量失败在流内结束，网关继续服务',
  { timeout: 35000 },
  async (t) => {
    const gateway = await fixture(t, { hold: true });
    const response = await gateway.call('/v1/chat/completions', {
      body: {
        model: `${model}:k_euler_ancestral`,
        stream: true,
        messages: [
          {
            role: 'user',
            content:
              '提示词: synthetic SSE capacity\n画师串:\n尺寸: 方图\n提示词引导值: 5\n缩放引导值: 0\n负面提示词:',
          },
        ],
      },
    });
    assert.equal(response.status, 200);
    assert.match(response.headers.get('content-type'), /text\/event-stream/);
    await until(() => gateway.calls.length === 1);
    assert.equal(gateway.jobs()[0].status, 'running');
    const held = gateway.financialState();
    assert.equal(held.records.charges[0].status, 'reserved');
    await gateway.observeQuota({
      overrides: { trainingStepsLeft: { fixedTrainingStepsLeft: false } },
    });
    assert.equal(gateway.quotaSummary().status, 'unknown_balance');
    assert.equal((await gateway.sharedStatus()).status, 'unknown_balance');
    assert.deepEqual(gateway.financialState(), held);
    gateway.release();
    const events = (await response.text())
      .split('\n')
      .filter((line) => line.startsWith('data: '))
      .map((line) => line.slice(6));
    assert.equal(events.at(-1), '[DONE]');
    const chunks = events.slice(0, -1).map((value) => JSON.parse(value));
    assert.equal(chunks.at(-1).choices[0].finish_reason, 'stop');
    const content = chunks.map((chunk) => chunk.choices[0].delta.content || '').join('');
    assert.match(content, /NAI 暂时限制此共享账户的并发/);
    assert.match(content, /<\/think>/);
    assert.equal(gateway.jobs()[0].error_status, 429);
    assert.equal(gateway.account().capacity_strikes, 1);
    assert.equal(gateway.calls.length, 1);
    const healthy = await gateway.call('/api/member/login-options');
    assert.equal(healthy.status, 200);
    await healthy.json();
    gateway.assertAlive();
    await gateway.stop();
    gateway.verifyReplay();
  },
);
