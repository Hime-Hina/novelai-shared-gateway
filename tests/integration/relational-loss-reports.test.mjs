import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { JsonStore } from '../../server/storage/sqlite-store.ts';
import { fixture, NOW, keys, meta, databaseState } from '../helpers/relational-generation.mjs';
import { RelationalReportStore } from '../../server/storage/relational-report-store.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { accountQuotaResult } from '../../server/quota/account-refresh.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import {
  summarizeRelationalFailureRisk,
  failureAccountingAlerts,
} from '../../server/quota/failure-accounting.ts';

const day = 86400000,
  env = { OWNER_USER_ID: 'm1' };
function trace(db) {
  const queries = [],
    prepare = db.prepare.bind(db);
  db.prepare = (sql) => {
    queries.push(sql);
    return prepare(sql);
  };
  return { queries, prepare };
}
async function setup(t, options = {}) {
  const f = await fixture(t, { seconds: 0, ...options, env }),
    quotas = new RelationalQuotaStore(f.db, env),
    reports = new RelationalReportStore(f.db);
  const observe = (id, changes, at = NOW + 10) => {
    const current = quotas.readAccountQuota('a'),
      ticket = quotas.beginQuotaObservation('a', at),
      result = accountQuotaResult(
        'a',
        {
          tier: 3,
          subscriptionActive: true,
          subscriptionExpiresAt: null,
          points: 0,
          fixed: current.observedAnlasFixed,
          purchased: current.observedAnlasPurchased,
          v5UsagePercent: current.observedV5,
          v5UsageTimeUntilNextPercent: current.secondsPerPercent,
          ...changes,
        },
        new Date(at).toISOString(),
      );
    return f.db.transaction(() =>
      quotas.recordQuotaObservation(meta(id, at + 1), ticket, result),
    )();
  };
  const failed = async (id, memberId = 'm1', at = NOW + 2) => {
    await f.accept(id, { memberId });
    const dispatch = f.dispatch(`${id}-0`).dispatch;
    return f.fail(
      `${id}-0`,
      {
        aborted: false,
        evidence: { requestStarted: true },
        diagnostic: { dispatch, error: `private-stack ${dispatch.pst}` },
      },
      at,
    );
  };
  return { ...f, quotas, reports, observe, failed };
}

test('真实退款/核对/结案/迟到仅按原尝试计风险，白名单不读输入或诊断', async (t) => {
  const f = await setup(t);
  await f.failed('one');
  await f.failed('two', 'm2');
  const closed = f.db.transaction(() =>
    f.generations.closeGenerationReview(meta('close-one', NOW + 300002), 'one-0'),
  )();
  await f.publish('one-0', NOW + 300004);
  const before = databaseState(f.db),
    captured = trace(f.db),
    result = f.reports.readFailureAccounting(null, NOW + 300010);
  assert.equal(result.coverage, 'retained-facts');
  assert.equal(result.risk.last24h.attempts, 2);
  assert.equal(result.risk.last24h.unknownAmounts, 0);
  assert.equal(result.risk.last24h.possible.anlasFixed, 4);
  assert.equal(result.risk.unknownTimeAttempts, 0);
  assert.equal(result.events.filter((e) => e.type === 'generation_loss_suspected').length, 2);
  assert.equal(result.events.filter((e) => e.type === 'generation_loss_estimated').length, 1);
  const estimated = result.events.find((e) => e.type === 'generation_loss_estimated');
  assert.equal(estimated.estimateAmountStatus, 'quoted_candidates');
  assert.equal(estimated.firstUnknownAt, new Date(NOW + 2).toISOString());
  assert.equal(estimated.deadlineAt, new Date(NOW + 300002).toISOString());
  const recovered = result.events.find((e) => e.type === 'generation_result_recovered');
  assert.equal(recovered.refundRetained, true);
  assert.ok(recovered.lossEventId);
  assert.equal(
    recovered.closureEventId,
    closed.events.find((e) => e.eventType === 'ReviewClosed').eventId,
  );
  assert.notEqual(
    recovered.closureEventId,
    f.db
      .prepare("SELECT closure_event_id AS id FROM generation_attempts WHERE job_id='one-0'")
      .get().id,
  );
  assert.equal(result.events.find((e) => e.type === 'generation_reserved').resources.anlasFixed, 2);
  for (const secret of [
    'private-stack',
    'private generation prompt',
    'synthetic-upstream-pst',
    keys.m1,
    keys.m2,
  ]) {
    assert.ok(!JSON.stringify(result).includes(secret));
  }
  assert.ok(
    captured.queries.every(
      (sql) =>
        !/generation_inputs|input_assets|generation_diagnostics|normalization_contexts/i.test(sql),
    ),
  );
  assert.equal(databaseState(f.db), before);
});

test('实际下降只累计本次observedAmount，容量只累计原正delta，历史累计未知不冒充当前账', async (t) => {
  const f = await setup(t);
  f.observe('gap-one', { fixed: '7800', v5UsagePercent: '50.000000000001' }, NOW + 10);
  f.observe('gap-two', { fixed: '7750', v5UsagePercent: '60.000000000001' }, NOW + 20);
  f.observe('same', { fixed: '7750', v5UsagePercent: '60.000000000001' }, NOW + 30);
  const before = databaseState(f.db),
    result = f.reports.readFailureAccounting(null, NOW + 40),
    gaps = result.events.filter((e) => e.type === 'resource_gap_observed'),
    capacities = result.events.filter((e) => e.type === 'v5_capacity_observed');
  assert.deepEqual(
    gaps.map((e) => e.observedAmount),
    [50, 200],
  );
  assert.deepEqual(
    gaps.map((e) => e.amount),
    [null, null],
  );
  assert.equal(result.risk.aggregate.last24h.anlasFixed, 250);
  assert.equal(result.operating.unfunded.anlasFixed, 250);
  assert.equal(result.risk.aggregate.last24h.v5, 29.999999999999);
  assert.deepEqual(
    capacities.map((e) => e.delta),
    [0, -10, 29.999999999999],
  );
  assert.equal(result.operating.v5CapacityGap, 19.999999999999);
  assert.equal(result.risk.last24h.possible.v5, 0);
  assert.deepEqual(
    result.alerts.map((a) => a.id),
    ['v5-24h', 'fixed-24h'],
  );
  assert.equal(databaseState(f.db), before);
});

test('未曾估计的迟到结果关联同笔实际结案，不借其他请求或后来当前投影的结案', async (t) => {
  const f = await setup(t);
  await f.failed('pending-recovery');
  await f.failed('other-estimate');
  const other = f.db.transaction(() =>
      f.generations.closeGenerationReview(meta('other-close', NOW + 300002), 'other-estimate-0'),
    )(),
    published = await f.publish('pending-recovery-0', NOW + 300004),
    original = published.events.find((e) => e.eventType === 'ReviewClosed'),
    before = databaseState(f.db),
    view = f.reports.readFailureAccounting(null, NOW + 300010),
    recovered = view.events.find((e) => e.type === 'generation_result_recovered');
  assert.equal(original.payload.status, 'result_recovered');
  assert.equal(recovered.closureEventId, original.eventId);
  assert.notEqual(
    recovered.closureEventId,
    other.events.find((e) => e.eventType === 'ReviewClosed').eventId,
  );
  assert.equal(
    view.events.filter(
      (e) => e.type === 'generation_loss_estimated' && e.jobId === 'pending-recovery-0',
    ).length,
    0,
  );
  assert.equal(databaseState(f.db), before);
  const sql = trace(f.db);
  f.reports.readFailureAccounting(null, NOW + 300010);
  const query = sql.queries.find((q) => q.includes('WITH page AS MATERIALIZED')),
    plan = sql.prepare(`EXPLAIN QUERY PLAN ${query}`).all();
  assert.ok(
    plan.some((row) => /SEARCH closed USING INDEX .*stream_id=/.test(row.detail)),
    JSON.stringify(plan),
  );
  t.diagnostic(
    `original-closure: ${JSON.stringify(plan.filter((row) => /closed|ORDER BY/.test(row.detail)))}`,
  );
});

test('当前政策与原政策审计分开，资金回执/运营余额复用实际owner且不写派生状态', async (t) => {
  const f = await setup(t);
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    (member) =>
      f.quotas.configureQuotaPolicy(
        meta('policy-first', NOW + 10, 'm1'),
        {
          action: 'loss-alert-policy',
          expectedVersion: 1,
          policy: {
            v5Points24h: 1e-13,
            fixedAnlas24h: 12,
            fixedAnlas30d: 34,
            memberAttempts24h: 2,
          },
        },
        member,
      ),
    env,
  );
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    () =>
      f.quotas.manageQuotaFunding(meta('contribute', NOW + 11, 'm1'), 'a', {
        action: 'operating-contribute',
        receipt: 'original-receipt',
        resource: 'anlasFixed',
        amount: 7,
      }),
    env,
  );
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    () =>
      f.quotas.manageQuotaFunding(meta('register', NOW + 12, 'm1'), 'a', {
        action: 'operating-register-funding',
        receipt: 'pending-receipt',
        resource: 'anlasFixed',
        amount: 5,
        destination: 'operating',
      }),
    env,
  );
  const before = databaseState(f.db),
    result = f.reports.readFailureAccounting(null, NOW + 20),
    policy = result.events.find((e) => e.type === 'loss_alert_policy_updated');
  assert.equal(result.policy.version, 2);
  assert.equal(result.policy.v5Points24h, 1e-13);
  assert.equal(policy.actorId, 'm1');
  assert.deepEqual(policy.before, {
    version: 1,
    v5Points24h: 1,
    fixedAnlas24h: 100,
    fixedAnlas30d: 500,
    memberAttempts24h: 3,
  });
  assert.deepEqual(policy.after, result.policy);
  assert.equal(result.operating.available.anlasFixed, 7);
  assert.equal(
    result.events.find((e) => e.type === 'operating_funding_received').receipt,
    'original-receipt',
  );
  assert.equal(result.events.find((e) => e.type === 'operating_funding_registered').amount, 5);
  assert.equal(result.operating.pendingExternalFunding[0].remaining, 5);
  assert.equal(result.operating.pendingExternalFunding[0].receipt, 'pending-receipt');
  assert.equal(databaseState(f.db), before);
});

test('实际返回/纠正/核销/扣款确认按原资源事实展示，缺累计来源保留NULL', async (t) => {
  const f = await setup(t),
    fund = (id, body, at) =>
      withAuthenticatedMemberApiKey(
        f.members,
        keys.m1,
        () => f.quotas.manageQuotaFunding(meta(id, at, 'm1'), 'a', body),
        env,
      );
  f.observe('return-gap', { fixed: '7900' }, NOW + 10);
  const gap = f.db.prepare('SELECT fixed_gap_event_id AS id FROM operating_state').get().id;
  f.observe('return-up', { fixed: '8000' }, NOW + 20);
  await fund(
    'cover-source',
    {
      action: 'operating-contribute',
      receipt: 'cover-source-receipt',
      resource: 'anlasFixed',
      amount: 10,
    },
    NOW + 21,
  );
  await fund(
    'cover',
    {
      action: 'operating-cover-loss',
      receipt: 'cover-receipt',
      resource: 'anlasFixed',
      amount: 10,
      lossEventId: gap,
    },
    NOW + 22,
  );
  await fund(
    'correct',
    {
      action: 'operating-reconcile-return',
      receipt: 'correct-receipt',
      resource: 'anlasFixed',
      amount: 20,
      lossEventId: gap,
    },
    NOW + 23,
  );
  await f.accept('debit', { now: NOW + 30 });
  f.dispatch('debit-0', NOW + 31);
  await f.publish('debit-0', NOW + 33);
  f.observe('debit-snapshot', { fixed: '8000' }, NOW + 40);
  await fund(
    'confirm',
    {
      action: 'operating-confirm-debits',
      receipt: 'confirm-receipt',
      resource: 'anlasFixed',
      amount: 2,
    },
    NOW + 41,
  );
  const before = databaseState(f.db),
    result = f.reports.readFailureAccounting(null, NOW + 50),
    returned = result.events.find((e) => e.type === 'resource_return_unattributed'),
    corrected = result.events.find((e) => e.type === 'resource_gap_reconciled'),
    covered = result.events.find((e) => e.type === 'operating_loss_covered'),
    confirmed = result.events.find((e) => e.type === 'operating_debits_confirmed');
  assert.equal(returned.amount, 100);
  assert.equal(corrected.amount, null);
  assert.equal(covered.amount, 10);
  assert.equal(covered.lossEventId, gap);
  assert.equal(confirmed.amount, 2);
  assert.equal(confirmed.evidenceLevel, 'operator_attested_debit_reconciliation');
  assert.equal(result.operating.pendingAnlasDebits.anlasFixed, 0);
  assert.equal(result.operating.unfunded.anlasFixed, 70);
  assert.equal(databaseState(f.db), before);
});

test('实际V5恢复只展示原容量恢复金额，软告警不把历史正变化减成当前缺口', async (t) => {
  const f = await setup(t, { seconds: 600 });
  f.observe('capacity-source', { v5UsagePercent: '79.5' }, NOW + 10);
  const initial = f.db.prepare('SELECT v5_capacity_gap AS gap FROM operating_state').get().gap,
    result = f.db.transaction(() =>
      f.quotas.refillQuota(meta('recover-capacity', NOW + 600000), 'a'),
    )(),
    fact = result.events.find((e) => e.eventType === 'RefillApplied');
  assert.ok(Number(fact.payload.capacityRecovered) > 0);
  const view = f.reports.readFailureAccounting(null, NOW + 600001),
    event = view.events.find((e) => e.type === 'v5_capacity_recovered');
  assert.equal(event.amount, Number(fact.payload.capacityRecovered));
  assert.equal(view.risk.aggregate.last24h.v5, Number(initial));
  assert.equal(event.evidenceLevel, 'recovery_rate_estimate');
});

test('风险owner的窗口端点、互斥候选、稳定同名成员、未知金额/时间与超2^53精度', () => {
  const candidate = (resources) => ({ resources, amountSource: 'original_quote' }),
    attempts = [
      {
        memberId: 'm1',
        memberName: 'same',
        firstUnknownAt: NOW - day,
        upstreamCandidates: [
          candidate({ v5: '0.1' }),
          candidate({ anlasFixed: '9007199254740993.999999999999' }),
        ],
      },
      {
        memberId: 'm2',
        memberName: 'same',
        firstUnknownAt: NOW,
        upstreamCandidates: [candidate({ v5: '0.2' })],
      },
      {
        memberId: 'm1',
        memberName: 'same',
        firstUnknownAt: NOW - 30 * day,
        upstreamCandidates: [],
      },
      {
        memberId: 'm1',
        memberName: 'same',
        firstUnknownAt: NOW - 30 * day - 1,
        upstreamCandidates: [candidate({ v5: '9' })],
      },
      {
        memberId: 'm1',
        memberName: 'same',
        firstUnknownAt: NOW + 1,
        upstreamCandidates: [candidate({ v5: '9' })],
      },
      {
        memberId: 'm1',
        memberName: 'same',
        firstUnknownAt: null,
        upstreamCandidates: [candidate({ v5: '9' })],
      },
    ],
    before = structuredClone(attempts),
    summary = summarizeRelationalFailureRisk(
      attempts,
      [
        { at: NOW - day, resource: 'v5', amount: '0.4' },
        { at: NOW, resource: 'v5', amount: '-0.3' },
        { at: NOW + 1, resource: 'v5', amount: '8' },
        { at: NOW - 30 * day, resource: 'anlasPurchased', amount: '1' },
      ],
      NOW,
    );
  assert.equal(summary.risk.last24h.attempts, 2);
  assert.equal(summary.risk.last30d.attempts, 3);
  assert.equal(summary.risk.last30d.unknownAmounts, 1);
  assert.equal(summary.unknownTimeAttempts, 1);
  assert.deepEqual(summary.risk.last24h.possible, {
    v5: '0.3',
    anlasFixed: '9007199254740993.999999999999',
    anlasPurchased: '0',
  });
  assert.equal(summary.risk.aggregate.last24h.v5, '0.4');
  assert.equal(summary.risk.aggregate.last30d.anlasPurchased, '1');
  assert.deepEqual(
    summary.members.map((m) => [m.memberId, m.attempts]),
    [
      ['m1', 1],
      ['m2', 1],
    ],
  );
  const alerts = failureAccountingAlerts(
    {
      version: 1,
      v5Points24h: 0.5,
      fixedAnlas24h: 9007199254740994,
      fixedAnlas30d: 9007199254740994,
      memberAttempts24h: 1,
    },
    summary.risk,
    summary.members,
  );
  assert.deepEqual(
    alerts.map((a) => a.id),
    ['purchased-30d', 'amount-unknown', 'member-24h:m1', 'member-24h:m2'],
  );
  assert.deepEqual(attempts, before);
});

test('合法原 JsonStore 风险/告警与同来源精确关系汇总对照，不读取旧库作为新路径fallback', async (t) => {
  const dir = await mkdtemp(path.join(tmpdir(), 'nai-loss-owner-comparison-')),
    store = new JsonStore(dir);
  await store.init();
  t.after(async () => {
    await store.close();
    await rm(dir, { recursive: true, force: true });
  });
  const candidate = (resources) => ({ resources, amountSource: 'original_quote' }),
    attempts = [
      {
        memberId: 'm1',
        memberName: 'same',
        firstUnknownAt: NOW - day,
        upstreamCandidates: [
          candidate({ v5: '0.1' }),
          candidate({ anlasFixed: '26', anlasPurchased: '4' }),
        ],
      },
      {
        memberId: 'm2',
        memberName: 'same',
        firstUnknownAt: NOW,
        upstreamCandidates: [candidate({ v5: '0.2' })],
      },
      {
        memberId: 'm1',
        memberName: 'same',
        firstUnknownAt: NOW - 30 * day,
        upstreamCandidates: [candidate({ v5: '0' })],
      },
    ],
    aggregate = [
      { at: NOW, resource: 'v5', amount: '0.4' },
      { at: NOW - day, resource: 'anlasFixed', amount: '11' },
    ],
    policy = {
      version: 1,
      v5Points24h: 0.5,
      fixedAnlas24h: 30,
      fixedAnlas30d: 50,
      memberAttempts24h: 1,
    },
    db = structuredClone(store.db);
  db.users = ['m1', 'm2'].map((id) => ({
    id,
    token: `synthetic-${id}`,
    enabled: true,
    balance: 0,
    memberAuth: { username: 'same', version: id },
  }));
  db.settings.fairQuota = { lossAlertPolicy: policy };
  db.ledger = [
    ...attempts.map((a, i) => ({
      id: `loss-${i}`,
      attemptId: `attempt-${i}`,
      type: 'generation_loss_suspected',
      at: new Date(a.firstUnknownAt).toISOString(),
      memberId: a.memberId,
      upstreamCandidates: a.upstreamCandidates,
    })),
    ...aggregate.map((a, i) => ({
      id: `gap-${i}`,
      type: a.resource === 'v5' ? 'v5_capacity_observed' : 'resource_gap_observed',
      at: new Date(a.at).toISOString(),
      resource: a.resource,
      ...(a.resource === 'v5' ? { delta: a.amount } : { observedAmount: a.amount }),
    })),
  ];
  await store.write(db);
  const old = await store.readFailureAccounting(null, NOW),
    summary = summarizeRelationalFailureRisk(attempts, aggregate, NOW),
    numeric = (value) => Object.fromEntries(Object.entries(value).map(([r, a]) => [r, Number(a)]));
  for (const window of ['last24h', 'last30d']) {
    assert.equal(old.risk[window].attempts, summary.risk[window].attempts);
    assert.equal(old.risk[window].unknownAmounts, summary.risk[window].unknownAmounts);
    for (const resource of ['v5', 'anlasFixed', 'anlasPurchased']) {
      assert.ok(
        Math.abs(
          old.risk[window].possible[resource] - Number(summary.risk[window].possible[resource]),
        ) < 1e-12,
      );
    }
    assert.deepEqual(old.risk.aggregate[window], numeric(summary.risk.aggregate[window]));
  }
  assert.deepEqual(old.alerts, failureAccountingAlerts(policy, summary.risk, summary.members));
});

test('多页冷事实按完整提交/事件序号稳定读取，cursor绑定history且大序号按BigInt进入SQL', async (t) => {
  const f = await setup(t);
  for (let i = 0; i < 19; i++) {
    await f.failed(`page-${i}`);
  }
  const before = databaseState(f.db),
    seen = new Set();
  let cursor = null,
    pages = 0;
  do {
    const result = f.reports.readFailureAccounting(cursor, NOW + 3);
    assert.ok(result.events.length <= 50);
    for (const event of result.events) {
      assert.ok(!seen.has(event.id));
      seen.add(event.id);
    }
    cursor = result.nextCursor;
    pages++;
  } while (cursor);
  assert.equal(seen.size, 57);
  assert.equal(pages, 2);
  const history = f.db.prepare('SELECT history_id AS id FROM projection_state').get().id,
    encode = (point) => Buffer.from(JSON.stringify(point)).toString('base64url');
  for (const value of [
    '',
    'not-a-cursor',
    encode(['other-history', '1', 0]),
    encode([history, '9223372036854775808', 0]),
    encode([history, '1', -1]),
  ]) {
    assert.throws(() => f.reports.readFailureAccounting(value, NOW), { statusCode: 400 });
  }
  const prepare = f.db.prepare.bind(f.db);
  let actual;
  f.db.prepare = (sql) => {
    const statement = prepare(sql);
    if (sql.includes('WITH page AS MATERIALIZED')) {
      const all = statement.all.bind(statement);
      statement.all = (...args) => {
        actual = args;
        return all(...args);
      };
    }
    return statement;
  };
  f.reports.readFailureAccounting(encode([history, '9007199254740993', 0]), NOW);
  assert.equal(actual[0], 9007199254740993n);
  assert.equal(databaseState(f.db), before);
});

test('重开和完整提交回放后的损耗读取一致，不恢复认证或回填输入；当前baseline缺失不冒充零账', async (t) => {
  const f = await setup(t);
  await f.failed('recover');
  f.observe('recover-observation', { fixed: '7900' }, NOW + 10);
  const expected = f.reports.readFailureAccounting(null, NOW + 20),
    reopened = new Database(path.join(f.dir, 'library.sqlite'));
  try {
    assert.deepEqual(
      new RelationalReportStore(reopened).readFailureAccounting(null, NOW + 20),
      expected,
    );
  } finally {
    reopened.close();
  }
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replay = new RelationalCommitReplayer(target);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replay.replayCommit(readRelationalCommit(f.db, seq), f.inputs))();
  }
  const before = databaseState(target);
  assert.deepEqual(
    new RelationalReportStore(target).readFailureAccounting(null, NOW + 20),
    expected,
  );
  assert.equal(databaseState(target), before);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM generation_inputs').get().n, 1);
  const blank = new Database(':memory:');
  try {
    initializeRelationalStorage(blank, 'blank-history');
    assert.throws(() => new RelationalReportStore(blank).readFailureAccounting(null, NOW), {
      statusCode: 503,
    });
  } finally {
    blank.close();
  }
});

test('实际损耗SQL计划按原窗口/提交分页，查询只装配薄字段', async (t) => {
  const f = await setup(t);
  await f.failed('plans');
  f.observe('plan-observation', { fixed: '7900' }, NOW + 10);
  const captured = trace(f.db);
  f.reports.readFailureAccounting(null, NOW + 20);
  const attempt = captured.queries.find((q) => q.includes('a.first_unknown_at AS firstUnknownAt')),
    aggregate = captured.queries.find((q) =>
      q.includes("CASE event_type WHEN 'V5CapacityObserved'"),
    ),
    page = captured.queries.find((q) => q.includes('WITH page AS MATERIALIZED'));
  const attemptPlan = captured
      .prepare(`EXPLAIN QUERY PLAN ${attempt}`)
      .all(NOW - 30 * day, NOW + 20),
    aggregatePlan = captured
      .prepare(`EXPLAIN QUERY PLAN ${aggregate}`)
      .all(NOW - 30 * day, NOW + 20);
  assert.ok(
    attemptPlan.some((row) =>
      /SEARCH generation_attempts USING INDEX idx_attempt_loss_time/.test(row.detail),
    ),
    JSON.stringify(attemptPlan),
  );
  assert.ok(
    attemptPlan.every((row) => !/SCAN a\b/.test(row.detail)),
    JSON.stringify(attemptPlan),
  );
  assert.ok(
    aggregatePlan.some((row) =>
      /SEARCH domain_events USING INDEX idx_loss_aggregate_time/.test(row.detail),
    ),
    JSON.stringify(aggregatePlan),
  );
  t.diagnostic(`loss-attempt: ${JSON.stringify(attemptPlan)}`);
  t.diagnostic(`loss-aggregate: ${JSON.stringify(aggregatePlan)}`);
  t.diagnostic(
    `loss-page: ${JSON.stringify(captured.prepare(`EXPLAIN QUERY PLAN ${page}`).all())}`,
  );
});
