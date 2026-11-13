import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { fixture, meta, NOW, keys, databaseState } from '../helpers/relational-generation.mjs';
import {
  RelationalQuotaStore,
  relationalQuotaFacts,
} from '../../server/storage/relational-quota-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import {
  createAccountQuotaQuery,
  accountQuotaResult,
  accountQuotaErrorResult,
} from '../../server/quota/account-refresh.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import {
  configureFairGroups,
  syncFairQuota,
  manageOperatingFunding,
} from '../../server/quota/fair-share.ts';
import { parseAmount, formatAmount } from '../../server/quota/resource-amounts.ts';

async function setup(t, options = {}) {
  const f = await fixture(t, { seconds: 0, ...options }),
    { db } = f;
  const quotas = new RelationalQuotaStore(db, { OWNER_USER_ID: 'm1' });
  let sequence = 0;
  const raw = (changes = {}) => {
    const q = quotas.readAccountQuota('a');
    return {
      tier: 3,
      subscriptionActive: true,
      subscriptionExpiresAt: null,
      points: 0,
      fixed: q.observedAnlasFixed,
      purchased: q.observedAnlasPurchased,
      v5UsagePercent: q.observedV5,
      v5UsageTimeUntilNextPercent: q.secondsPerPercent,
      ...changes,
    };
  };
  const observe = async (changes = {}, at = NOW + ++sequence, failure = null) => {
    const ticket = quotas.beginQuotaObservation('a', at);
    const query = createAccountQuotaQuery({
      now: () => at,
      logger: { log() {}, error() {} },
      fetchQuota: async (pst) => {
        assert.equal(db.inTransaction, false);
        assert.equal(pst, 'synthetic-upstream-pst');
        if (failure) {
          throw failure;
        }
        return raw(changes);
      },
    });
    let result;
    try {
      result = accountQuotaResult(
        'a',
        await query(ticket.pst, '', 'a'),
        new Date(at).toISOString(),
      );
    } catch (error) {
      result = accountQuotaErrorResult('a', error, new Date(at).toISOString());
    }
    return db.transaction(() =>
      quotas.recordQuotaObservation(meta(`observe-${ticket.sequence}`, at + 1), ticket, result),
    )();
  };
  const fund = (id, body, at = NOW + 10, memberId = 'm1') =>
    withAuthenticatedMemberApiKey(
      f.members,
      keys[memberId],
      () => quotas.manageQuotaFunding(meta(id, at, memberId), 'a', body),
      {},
    );
  const refill = (at, id = `refill-${at}`) =>
    db.transaction(() => quotas.refillQuota(meta(id, at), 'a'))();
  return { ...f, quotas, observe, fund, refill, raw };
}
const contribute = (receipt, resource, amount) => ({
  action: 'operating-contribute',
  receipt,
  resource,
  amount,
});
const register = (
  receipt,
  resource,
  amount,
  destination = 'operating',
  fromUnattributed = false,
) => ({
  action: 'operating-register-funding',
  receipt,
  resource,
  amount,
  destination,
  fromUnattributed,
});
const wallet = (f, id = 'm1') => f.generations.readWallet('a', id);

test('人工刷新复用当前 owner 身份，普通成员不能提交观察或恢复命令', async (t) => {
  const f = await setup(t),
    { db, quotas: q } = f;
  const ticket = q.beginQuotaObservation('a', NOW + 10),
    result = accountQuotaResult('a', f.raw({ fixed: '8100' }), new Date(NOW + 10).toISOString());
  const before = db.serialize();
  await assert.rejects(
    withAuthenticatedMemberApiKey(
      f.members,
      keys.m2,
      () => q.recordQuotaObservation(meta('member-observe', NOW + 11, 'm2'), ticket, result),
      {},
    ),
    /执行 owner/,
  );
  await assert.rejects(
    withAuthenticatedMemberApiKey(
      f.members,
      keys.m2,
      () => q.refillQuota(meta('member-refill', NOW + 11, 'm2'), 'a'),
      {},
    ),
    /执行 owner/,
  );
  assert.ok(db.serialize().equals(before));
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    () => q.recordQuotaObservation(meta('owner-observe', NOW + 11, 'm1'), ticket, result),
    {},
  );
  assert.equal(wallet(f).anlasFixed, '4050');
  assert.equal(
    readRelationalCommit(
      db,
      db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS n FROM commits').get().n,
    ).command.actorId,
    'm1',
  );
});

test('同毫秒资金登记按原提交顺序匹配，收据字典序不能改变资金归属', async (t) => {
  const f = await setup(t);
  await f.fund('fifo-first', register('z-members-receipt', 'anlasFixed', 100, 'members'), NOW + 10);
  await f.fund(
    'fifo-second',
    register('a-operating-receipt', 'anlasFixed', 100, 'operating'),
    NOW + 10,
  );
  await f.observe({ fixed: '8050' }, NOW + 20);
  assert.equal(wallet(f).anlasFixed, '4025');
  assert.equal(wallet(f, 'm2').anlasFixed, '4025');
  assert.equal(f.db.prepare('SELECT anlas_fixed_available AS n FROM operating_state').get().n, '0');
  assert.deepEqual(
    f.db.prepare('SELECT receipt,remaining FROM pending_funding ORDER BY receipt').all(),
    [
      { receipt: 'a-operating-receipt', remaining: '100' },
      { receipt: 'z-members-receipt', remaining: '50' },
    ],
  );
});

test('真实查询 owner 在事务外取额度，关系事务同时保存观察、来源分配与新基线；重领零写入', async (t) => {
  const f = await setup(t),
    { db, quotas: q } = f;
  const result = await f.observe({ fixed: '9000' }, NOW + 10);
  assert.deepEqual(wallet(f), { v5: '40', anlasFixed: '4500', anlasPurchased: '0' });
  const latest = q.readAccountQuota('a');
  assert.equal(latest.observedSequence, '9007199254740994');
  assert.equal(latest.baselineEventId, latest.observationEventId);
  assert.ok(result.events.some((e) => e.eventType === 'AllocationApplied'));
  const before = db.serialize();
  // 已提交回执无需重新发起查询或获得可再次发送的凭据。
  const replay = db.transaction(() =>
    q.recordQuotaObservation(
      meta('observe-9007199254740994', NOW + 11),
      {
        accountId: 'a',
        credentialEpoch: 1,
        sequence: '9007199254740994',
        startedAt: NOW + 10,
        pst: 'synthetic-upstream-pst',
      },
      accountQuotaResult('a', f.raw({ fixed: '9000' }), new Date(NOW + 10).toISOString()),
    ),
  )();
  assert.equal(replay.replayed, true);
  assert.ok(db.serialize().equals(before));
  const events =
    JSON.stringify(db.prepare('SELECT payload FROM domain_events').all()) +
    JSON.stringify(db.prepare('SELECT receipt FROM commits').all());
  for (const secret of ['synthetic-upstream-pst', keys.m1, 'private generation prompt']) {
    assert.equal(events.includes(secret), false);
  }
  assert.deepEqual(db.pragma('foreign_key_check'), []);
  assert.throws(() => assertRelationalStorageReady(db), /incomplete/);
});

test('最新发起票优先；已轮换凭据、旧响应和旧序号不能覆盖关系观察', async (t) => {
  const f = await setup(t),
    { db, quotas: q } = f;
  const first = q.beginQuotaObservation('a', NOW + 1),
    second = q.beginQuotaObservation('a', NOW + 2);
  const before = db.serialize();
  const apply = (ticket, fixed) =>
    db.transaction(() =>
      q.recordQuotaObservation(
        meta(`ticket-${ticket.sequence}`, ticket.startedAt + 1),
        ticket,
        accountQuotaResult('a', f.raw({ fixed }), new Date(ticket.startedAt).toISOString()),
      ),
    )();
  assert.equal(apply(first, '10000').ignored, true);
  assert.ok(db.serialize().equals(before));
  apply(second, '9000');
  assert.equal(q.readAccountQuota('a').observedAnlasFixed, '9000');
  const stale = q.beginQuotaObservation('a', NOW + 3);
  db.prepare('UPDATE upstream_accounts SET credential_epoch=2').run();
  const rotated = db.serialize();
  assert.equal(apply(stale, '10000').ignored, true);
  assert.ok(db.serialize().equals(rotated));
  const fresh = q.beginQuotaObservation('a', NOW + 4);
  const beforeInvalid = db.serialize();
  assert.throws(
    () =>
      db.transaction(() =>
        q.recordQuotaObservation(
          meta('wrong-start-time', NOW + 5),
          fresh,
          accountQuotaResult('a', f.raw(), new Date(NOW + 5).toISOString()),
        ),
      )(),
    /账户不符/,
  );
  assert.ok(db.serialize().equals(beforeInvalid));
  apply(fresh, '9000');
  assert.equal(q.readAccountQuota('a').observedCredentialEpoch, 2);
  assert.equal(fresh.sequence, '9007199254740996');
});

test('失败与未知余额不抹成员权益或旧基线；明确封禁按原 owner 停用', async (t) => {
  const f = await setup(t),
    { db, quotas: q } = f;
  const original = q.readAccountQuota('a'),
    balance = wallet(f);
  await f.observe({}, NOW + 20, new Error('raw private upstream body'));
  let current = q.readAccountQuota('a');
  assert.equal(current.observedAt, original.observedAt);
  assert.equal(current.observedAnlasFixed, original.observedAnlasFixed);
  assert.equal(current.baselineEventId, original.baselineEventId);
  assert.equal(current.status, 'quota_sync_error');
  assert.equal(current.quotaErrorCode, 'sync_error');
  assert.deepEqual(wallet(f), balance);
  await f.observe({ fixed: null }, NOW + 30);
  current = q.readAccountQuota('a');
  assert.equal(current.observedAnlasFixed, null);
  assert.equal(current.status, 'unknown_balance');
  assert.equal(current.baselineEventId, original.baselineEventId);
  assert.deepEqual(wallet(f), balance);
  await f.observe({}, NOW + 40, new Error('Account ban'));
  assert.equal(db.prepare('SELECT enabled FROM upstream_accounts').get().enabled, 0);
  assert.deepEqual(wallet(f), balance);
});

test('观察在途只更新安全余额；结清且取得新观察后才抵消原扣款，不重复分配净充值', async (t) => {
  const f = await setup(t),
    { quotas: q } = f;
  await f.accept('held');
  f.dispatch('held-0');
  const before = wallet(f),
    baseline = q.readAccountQuota('a').baselineEventId;
  await f.observe({ fixed: '7998' }, NOW + 2);
  assert.equal(q.readAccountQuota('a').baselineEventId, baseline);
  assert.deepEqual(wallet(f), before);
  await f.publish('held-0', NOW + 3);
  assert.equal(q.readAccountQuota('a').pendingFixedDebit, '2');
  await f.observe({ fixed: '7998' }, NOW + 5);
  assert.equal(q.readAccountQuota('a').pendingFixedDebit, '0');
  assert.deepEqual(wallet(f), before);
  await f.observe({ fixed: '7998' }, NOW + 6);
  assert.deepEqual(wallet(f), before);
});

test('恢复只推进原窗口一次，先偿还债务再抵消容量缺口，运营 V5 占用也不重复发放', async (t) => {
  const f = await setup(t, { seconds: 600 }),
    { db, quotas: q } = f;
  db.prepare("UPDATE account_quota_state SET shared_v5_debt='0.5'").run();
  db.prepare("UPDATE operating_state SET v5_capacity_gap='0.2'").run();
  const result = f.refill(NOW + 600000);
  const refill = result.events.find((e) => e.eventType === 'RefillApplied').payload;
  assert.equal(refill.budget, '1');
  assert.equal(refill.debtRepaid, '0.5');
  assert.equal(refill.capacityRecovered, '0.2');
  assert.equal(wallet(f).v5, '40.15');
  assert.equal(q.readAccountQuota('a').sharedV5Debt, '0');
  assert.equal(db.prepare('SELECT v5_capacity_gap AS n FROM operating_state').get().n, '0');
  const bytes = db.serialize();
  assert.equal(f.refill(NOW + 600000).replayed, true);
  assert.ok(db.serialize().equals(bytes));
  f.refill(NOW + 600001, 'stale-refill');
  assert.equal(wallet(f).v5, '40.15');
  assert.equal(q.readAccountQuota('a').status, 'stale');
});

test('运营者本人划入、登记与部分到账保持原资源，登记本身不增加可用额', async (t) => {
  const f = await setup(t),
    { db } = f;
  await f.fund('contribute-fixed', contribute('fixed-receipt', 'anlasFixed', 100));
  await f.fund('contribute-v5', contribute('v5-receipt', 'v5', 0.123456789012));
  assert.equal(wallet(f).anlasFixed, '3900');
  assert.equal(wallet(f).v5, '39.876543210988');
  await f.fund('register-operating', register('registered-receipt', 'anlasFixed', 200), NOW + 20);
  assert.equal(db.prepare('SELECT anlas_fixed_available AS n FROM operating_state').get().n, '100');
  assert.equal(db.prepare('SELECT remaining FROM pending_funding').get().remaining, '200');
  await f.observe({ fixed: '8050' }, NOW + 30);
  assert.equal(db.prepare('SELECT remaining FROM pending_funding').get().remaining, '150');
  assert.equal(db.prepare('SELECT anlas_fixed_available AS n FROM operating_state').get().n, '150');
  assert.equal(wallet(f).anlasFixed, '3900');
  await f.observe({ fixed: '8300' }, NOW + 40);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM pending_funding').get().n, 0);
  assert.equal(db.prepare('SELECT anlas_fixed_available AS n FROM operating_state').get().n, '300');
  assert.equal(wallet(f).anlasFixed, '3950');
  assert.equal(wallet(f, 'm2').anlasFixed, '4050');
});

test('聚合下降与回跳不削减成员或自动核销，返回再次消失不重复计提差额', async (t) => {
  const f = await setup(t),
    { db } = f;
  const before = wallet(f);
  await f.observe({ fixed: '0' }, NOW + 10);
  const gap = db
    .prepare('SELECT unfunded_fixed AS n,fixed_gap_event_id AS id FROM operating_state')
    .get();
  assert.equal(gap.n, '8000');
  await f.observe({ fixed: '8000' }, NOW + 20);
  assert.deepEqual(wallet(f), before);
  assert.equal(db.prepare('SELECT unattributed_fixed AS n FROM operating_state').get().n, '8000');
  const fallen = await f.observe({ fixed: '0' }, NOW + 30);
  assert.ok(
    fallen.events.some((e) => e.eventType === 'ReturnClassified' && e.payload.amount === '-8000'),
  );
  assert.equal(db.prepare('SELECT unfunded_fixed AS n FROM operating_state').get().n, '8000');
  assert.equal(db.prepare('SELECT unattributed_fixed AS n FROM operating_state').get().n, '0');
  assert.deepEqual(wallet(f), before);
  assert.equal(
    fallen.events.some((e) => e.eventType === 'ResourceGapObserved'),
    false,
  );
});

test('资金命令复用当前认证与部署 owner，本人来源不可由正文指定；同收据不同决定被拒绝', async (t) => {
  const f = await setup(t),
    { db } = f,
    body = contribute('identity-receipt', 'anlasFixed', 10);
  let before = db.serialize();
  await assert.rejects(f.fund('other-member', body, NOW + 10, 'm2'), /owner|授权/);
  assert.ok(db.serialize().equals(before));
  await f.fund('owner-contribution', { ...body, fromUserId: 'm2' });
  assert.equal(wallet(f).anlasFixed, '3990');
  assert.equal(wallet(f, 'm2').anlasFixed, '4000');
  before = db.serialize();
  assert.equal((await f.fund('same-receipt-new-command', body)).replayed, true);
  assert.ok(db.serialize().equals(before));
  await assert.rejects(f.fund('changed-receipt', { ...body, amount: 11 }), /不同操作/);
  assert.ok(db.serialize().equals(before));
  await assert.rejects(f.fund('v5-external', register('v5-external-receipt', 'v5', 1)), /V5/);
  assert.ok(db.serialize().equals(before));
});

test('净充值中确认原普通扣款后才匹配已登记资金，剩余按份额分配且不重复确认', async (t) => {
  const f = await setup(t),
    { db, quotas: q } = f;
  await f.accept('net');
  f.dispatch('net-0');
  const before = db.serialize();
  await assert.rejects(
    f.fund('held-confirm', {
      action: 'operating-confirm-debits',
      receipt: 'held-confirm-receipt',
      resource: 'anlasFixed',
      amount: 1,
    }),
    /结清|确认/,
  );
  assert.ok(db.serialize().equals(before));
  await f.publish('net-0');
  await f.observe({ fixed: '8000' }, NOW + 5);
  assert.equal(q.readAccountQuota('a').pendingFixedDebit, '2');
  await f.fund('pending-net', register('net-funding-receipt', 'anlasFixed', 1), NOW + 6);
  await f.observe({ fixed: '8000' }, NOW + 7);
  const body = {
    action: 'operating-confirm-debits',
    receipt: 'net-confirm-receipt',
    resource: 'anlasFixed',
    amount: 2,
  };
  await f.fund('confirm-net', body, NOW + 9);
  assert.equal(q.readAccountQuota('a').pendingFixedDebit, '0');
  assert.equal(wallet(f).anlasFixed, '3998.5');
  assert.equal(wallet(f, 'm2').anlasFixed, '4000.5');
  assert.equal(db.prepare('SELECT anlas_fixed_available AS n FROM operating_state').get().n, '1');
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM pending_funding').get().n, 0);
  const saved = db.serialize();
  assert.equal((await f.fund('same-net-receipt', body, NOW + 10)).replayed, true);
  assert.ok(db.serialize().equals(saved));
});

test('待归属新增与快照纠正分别处理，只用同池运营可用核销当前差额', async (t) => {
  const f = await setup(t),
    { db } = f;
  await f.observe({ fixed: '0' }, NOW + 10);
  const gap = db.prepare('SELECT fixed_gap_event_id AS id FROM operating_state').get().id;
  await f.observe({ fixed: '8000' }, NOW + 20);
  await f.fund(
    'declare-new',
    register('declared-members-receipt', 'anlasFixed', 100, 'members', true),
    NOW + 30,
  );
  assert.equal(wallet(f).anlasFixed, '4050');
  assert.equal(db.prepare('SELECT unfunded_fixed AS n FROM operating_state').get().n, '8000');
  await f.fund(
    'correct-return',
    {
      action: 'operating-reconcile-return',
      receipt: 'correct-return-receipt',
      resource: 'anlasFixed',
      amount: 200,
      lossEventId: gap,
    },
    NOW + 31,
  );
  const current = db
    .prepare(
      'SELECT unfunded_fixed AS gap,unattributed_fixed AS returned,fixed_gap_event_id AS id FROM operating_state',
    )
    .get();
  assert.deepEqual(current, {
    gap: '7800',
    returned: '7700',
    id: 'operating-receipt:correct-return-receipt',
  });
  await f.fund('cover-source', contribute('cover-source-receipt', 'anlasFixed', 100), NOW + 32);
  const before = db.serialize();
  await assert.rejects(
    f.fund(
      'too-large-cover',
      {
        action: 'operating-cover-loss',
        receipt: 'large-cover-receipt',
        resource: 'anlasFixed',
        amount: 101,
        lossEventId: current.id,
      },
      NOW + 33,
    ),
    /可用余额/,
  );
  await assert.rejects(
    f.fund(
      'wrong-pool-cover',
      {
        action: 'operating-cover-loss',
        receipt: 'wrong-pool-receipt',
        resource: 'anlasPurchased',
        amount: 1,
        lossEventId: current.id,
      },
      NOW + 33,
    ),
    /资源|不存在/,
  );
  await assert.rejects(
    f.fund(
      'stale-gap-cover',
      {
        action: 'operating-cover-loss',
        receipt: 'stale-gap-receipt',
        resource: 'anlasFixed',
        amount: 1,
        lossEventId: gap,
      },
      NOW + 33,
    ),
    /已更新/,
  );
  assert.ok(db.serialize().equals(before));
  await f.fund(
    'cover',
    {
      action: 'operating-cover-loss',
      receipt: 'cover-loss-receipt',
      resource: 'anlasFixed',
      amount: 100,
      lossEventId: current.id,
    },
    NOW + 34,
  );
  assert.equal(db.prepare('SELECT unfunded_fixed AS n FROM operating_state').get().n, '7700');
  assert.equal(db.prepare('SELECT anlas_fixed_available AS n FROM operating_state').get().n, '0');
  assert.equal(wallet(f).anlasFixed, '3950');
});

test('累计差额当前点可核销前次缺口，覆盖金额从持久事实索引读取且每笔只承担一次', async (t) => {
  const f = await setup(t),
    { db } = f;
  await f.observe({ fixed: '7900' }, NOW + 10);
  await f.observe({ fixed: '7800' }, NOW + 20);
  const lossEventId = db.prepare('SELECT fixed_gap_event_id AS id FROM operating_state').get().id;
  assert.equal(
    JSON.parse(
      db.prepare('SELECT payload FROM domain_events WHERE event_id=?').get(lossEventId).payload,
    ).amount,
    '100',
  );
  await f.fund(
    'cover-200-source',
    contribute('cover-200-source-receipt', 'anlasFixed', 200),
    NOW + 21,
  );
  for (const [receipt, amount] of [
    ['first-cover-receipt', 150],
    ['second-cover-receipt', 50],
  ]) {
    await f.fund(
      receipt,
      { action: 'operating-cover-loss', receipt, resource: 'anlasFixed', amount, lossEventId },
      NOW + 22,
    );
  }
  assert.equal(db.prepare('SELECT unfunded_fixed AS n FROM operating_state').get().n, '0');
  const before = db.serialize();
  await assert.rejects(
    f.fund(
      'extra-cover',
      {
        action: 'operating-cover-loss',
        receipt: 'extra-cover-receipt',
        resource: 'anlasFixed',
        amount: 1,
        lossEventId,
      },
      NOW + 23,
    ),
    /核销超过/,
  );
  assert.ok(db.serialize().equals(before));
  const plan = db
    .prepare(
      "EXPLAIN QUERY PLAN SELECT json_extract(payload,'$.amount') AS amount FROM domain_events WHERE event_type='OperatingLossCovered' AND json_extract(payload,'$.lossEventId')=?",
    )
    .all(lossEventId);
  assert.match(JSON.stringify(plan), /idx_quota_loss_coverage/);
});

test('观察、恢复与资金 SQL 故障被外层捕获仍整笔回滚，原票和收据可安全重试', async (t) => {
  const f = await setup(t, { seconds: 600 }),
    { db, quotas: q } = f;
  const ticket = q.beginQuotaObservation('a', NOW + 10),
    result = accountQuotaResult('a', f.raw({ fixed: '9000' }), new Date(NOW + 10).toISOString());
  db.exec(
    "CREATE TEMP TRIGGER fail_alloc BEFORE UPDATE ON wallets WHEN NEW.member_id='m2' BEGIN SELECT RAISE(ABORT,'allocation fault'); END",
  );
  const before = databaseState(db);
  db.transaction(() =>
    assert.throws(
      () => q.recordQuotaObservation(meta('atomic-observation', NOW + 11), ticket, result),
      /allocation fault/,
    ),
  )();
  assert.equal(databaseState(db), before);
  db.transaction(() =>
    assert.throws(() => q.refillQuota(meta('atomic-refill', NOW + 1000), 'a'), /allocation fault/),
  )();
  assert.equal(databaseState(db), before);
  db.exec('DROP TRIGGER fail_alloc');
  db.transaction(() =>
    q.recordQuotaObservation(meta('atomic-observation', NOW + 11), ticket, result),
  )();
  db.exec(
    "CREATE TEMP TRIGGER fail_funding BEFORE INSERT ON pending_funding BEGIN SELECT RAISE(ABORT,'funding fault'); END",
  );
  const saved = databaseState(db),
    body = register('atomic-funding-receipt', 'anlasFixed', 100);
  db.transaction(() =>
    assert.throws(
      () => q.manageQuotaFunding(meta('atomic-funding', NOW + 12, 'm1'), 'a', body),
      /funding fault/,
    ),
  )();
  assert.equal(databaseState(db), saved);
  db.exec('DROP TRIGGER fail_funding');
  await f.fund('atomic-funding', body, NOW + 12);
  assert.equal(db.prepare('SELECT remaining FROM pending_funding').get().remaining, '100');
  assert.deepEqual(db.pragma('foreign_key_check'), []);
});

test('收据在 SQLite 关闭重开后仍按原规范请求去重，已完成匹配不需要在线 pending 行', async (t) => {
  const f = await setup(t),
    body = register('cold-funding-receipt', 'anlasFixed', 100);
  await f.fund('cold-register', body, NOW + 10);
  await f.observe({ fixed: '8100' }, NOW + 20);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM pending_funding').get().n, 0);
  f.db.close();
  const reopened = new Database(path.join(f.dir, 'library.sqlite'));
  try {
    reopened.pragma('foreign_keys=ON');
    const q = new RelationalQuotaStore(reopened, { OWNER_USER_ID: 'm1' }),
      members = new RelationalMemberStore(reopened),
      before = reopened.serialize();
    const duplicate = await withAuthenticatedMemberApiKey(
      members,
      keys.m1,
      () => q.manageQuotaFunding(meta('cold-new-command', NOW + 30, 'm1'), 'a', body),
      {},
    );
    assert.equal(duplicate.replayed, true);
    assert.ok(reopened.serialize().equals(before));
    await assert.rejects(
      withAuthenticatedMemberApiKey(
        members,
        keys.m1,
        () =>
          q.manageQuotaFunding(meta('cold-changed', NOW + 30, 'm1'), 'a', { ...body, amount: 101 }),
        {},
      ),
      /不同操作/,
    );
    assert.ok(reopened.serialize().equals(before));
  } finally {
    reopened.close();
  }
});

test('官方大额文本以 12 位精度分配，序号、预算与 Anlas 不通过 number 求和', async (t) => {
  const f = await setup(t),
    amount = '9007199254740993.123456789012';
  await f.observe({ fixed: amount }, NOW + 10);
  const total = parseAmount(wallet(f).anlasFixed) + parseAmount(wallet(f, 'm2').anlasFixed);
  assert.equal(formatAmount(total), amount);
  assert.equal(f.quotas.readAccountQuota('a').observedAnlasFixed, amount);
  assert.equal(f.quotas.readAccountQuota('a').observedSequence, '9007199254740994');
});

test('已有运营 V5 也占来源容量，满额期间的恢复不会变成另一份成员权益', async (t) => {
  const f = await setup(t, { v5: '50', officialV5: '100', seconds: 600 });
  await f.fund('capacity-contribute', contribute('capacity-contribute-receipt', 'v5', 1), NOW);
  const saved = wallet(f);
  f.refill(NOW + 600000);
  assert.equal(saved.v5, '49');
  assert.deepEqual(wallet(f), saved);
  assert.equal(wallet(f, 'm2').v5, '50');
  assert.equal(f.db.prepare('SELECT v5_available AS n FROM operating_state').get().n, '1');
});

test('fallback 待核对与缓冲分开：同值观察不退款，延迟下降只匹配原缓冲一次', async (t) => {
  const f = await setup(t, { v5: '0', secondV5: '40', fallback: true }),
    { quotas: q } = f;
  await f.accept('fallback', { model: 'nai-diffusion-5-full', steps: 23 });
  f.dispatch('fallback-0');
  await f.publish('fallback-0');
  const charge = f.generations.readCharge('fallback-0'),
    before = wallet(f);
  assert.equal(charge.billingMode, 'anlas_fallback');
  await f.observe({ fixed: '8000' }, NOW + 10);
  assert.equal(q.readAccountQuota('a').pendingFallbackFixed, '0');
  assert.equal(q.readAccountQuota('a').fallbackFixed, charge.anlasFixed);
  assert.equal(q.readAccountQuota('a').sharedV5Debt, charge.sharedV5Reserved);
  const observed = await f.observe(
    { fixed: formatAmount(8000n * 10n ** 12n - parseAmount(charge.anlasFixed)) },
    NOW + 20,
  );
  assert.equal(q.readAccountQuota('a').fallbackFixed, '0');
  assert.equal(
    observed.events.some((e) => e.eventType === 'ResourceGapObserved'),
    false,
  );
  assert.deepEqual(wallet(f), before);
});

test('人工资金命令在同一事务核对当前资格；旧 active 标记不能给过期观察登记未来资金', async (t) => {
  const f = await setup(t),
    before = f.db.serialize();
  await assert.rejects(
    f.fund('stale-register', register('stale-register-receipt', 'anlasFixed', 1), NOW + 600001),
    /有效的官方/,
  );
  assert.ok(f.db.serialize().equals(before));
});

test('空金融基线只为真正空钱包开账；已有运营资产不能被再次分配', async (t) => {
  const empty = await setup(t, { v5: '0', fixed: '0', baseline: false });
  await empty.observe({ fixed: '8000', v5UsagePercent: '80' }, NOW + 10);
  assert.equal(wallet(empty).anlasFixed, '4000');
  assert.equal(wallet(empty).v5, '40');
  assert.equal(
    empty.quotas.readAccountQuota('a').baselineEventId,
    empty.quotas.readAccountQuota('a').observationEventId,
  );
  const existing = await setup(t, { v5: '0', fixed: '0', baseline: false });
  existing.db.prepare("UPDATE operating_state SET anlas_fixed_available='100'").run();
  const before = existing.db.serialize();
  await assert.rejects(existing.observe({ fixed: '8000' }, NOW + 10), /金融基线/);
  assert.ok(existing.db.serialize().equals(before));
});

test('额度、资金与生成混合历史按原完整提交回放，金额和收据一致，私有认证仍不恢复', async (t) => {
  const f = await setup(t),
    { db } = f;
  await f.fund('replay-register', register('replay-register-receipt', 'anlasFixed', 100), NOW + 10);
  await f.observe({ fixed: '8050' }, NOW + 20);
  await f.accept('replay-job');
  f.dispatch('replay-job-0', NOW + 21);
  await f.publish('replay-job-0', NOW + 23);
  await f.observe({ fixed: '8098' }, NOW + 30);
  await f.fund(
    'replay-contribute',
    contribute('replay-contribute-receipt', 'anlasFixed', 10),
    NOW + 31,
  );
  await f.observe({ fixed: '7998' }, NOW + 40);
  const lossEventId = db.prepare('SELECT fixed_gap_event_id AS id FROM operating_state').get().id;
  await f.fund(
    'replay-cover',
    {
      action: 'operating-cover-loss',
      receipt: 'replay-cover-receipt',
      resource: 'anlasFixed',
      amount: 10,
      lossEventId,
    },
    NOW + 41,
  );
  const target = new Database(':memory:');
  try {
    initializeRelationalStorage(target, 'generation-history');
    const members = new RelationalMemberStore(target),
      inputs = new RelationalInputStore(target),
      generations = new RelationalGenerationStore(target, {}),
      quotas = new RelationalQuotaStore(target, {});
    const commits = db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS n FROM commits ORDER BY commit_seq')
      .all()
      .map((r) => readRelationalCommit(db, r.n));
    const replay = (stored) =>
      target.transaction(() => {
        if (BigInt(stored.events[0].commitSeq) <= 2n) {
          return members.replayMemberCommit(stored);
        }
        if (stored.events.every((e) => relationalQuotaFacts.has(e.eventType))) {
          return quotas.replayQuotaCommit(stored);
        }
        if (stored.events.some((e) => e.eventType === 'RequestAccepted')) {
          return inputs.replayAcceptedInput(f.inputs, stored.events[0].commitSeq);
        }
        return generations.replayGenerationCommit(stored);
      })();
    for (const stored of commits.slice(0, 7)) {
      replay(stored);
    }
    const checkpoint = target.serialize();
    for (const stored of commits.slice(0, 7)) {
      assert.equal(replay(stored).replayed, true);
    }
    assert.ok(target.serialize().equals(checkpoint));
    for (const stored of commits.slice(7)) {
      replay(stored);
    }
    for (const table of [
      'commits',
      'domain_events',
      'stream_heads',
      'projection_state',
      'account_quota_state',
      'operating_state',
      'wallets',
      'charges',
      'pending_funding',
      'generation_jobs',
      'generation_attempts',
      'artifacts',
    ]) {
      assert.deepEqual(
        target.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
        db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
        table,
      );
    }
    const saved = target.serialize();
    for (const stored of commits) {
      assert.equal(replay(stored).replayed, true);
    }
    assert.ok(target.serialize().equals(saved));
    assert.equal(target.prepare('SELECT status FROM projection_state').get().status, 'building');
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
    assert.deepEqual(target.prepare('SELECT auth_epoch FROM members').all(), [
      { auth_epoch: null },
      { auth_epoch: null },
    ]);
    assert.deepEqual(target.pragma('foreign_key_check'), []);
  } finally {
    target.close();
  }
});

test('恢复回放拒绝错误来源、重复成员和超预算，截短两个列表也不能留下半笔写入', async (t) => {
  const f = await setup(t, { seconds: 600 });
  f.refill(NOW + 600001, 'mark-stale');
  const snapshot = f.db.serialize();
  const result = f.refill(NOW + 600000),
    stored = readRelationalCommit(f.db, result.commitSeq);
  for (const change of [
    (e) => {
      e.payload.budget = '0';
    },
    (e) => {
      e.payload.observationEventId = 'missing-observation';
    },
    (e) => {
      e.payload.allocations.push({ ...e.payload.allocations[0] });
    },
    (e) => {
      e.payload.from = NOW - 1;
    },
  ]) {
    const target = new Database(snapshot);
    try {
      target.pragma('foreign_keys=ON');
      const q = new RelationalQuotaStore(target, {}),
        bad = structuredClone(stored),
        before = databaseState(target);
      const index = bad.events.findIndex((e) => e.eventType === 'RefillApplied');
      for (const e of [bad.events[index], bad.command.events[index]]) {
        change(e);
      }
      target.transaction(() =>
        assert.throws(() => q.replayQuotaCommit(bad), /预算|来源|观察|游标|身份/),
      )();
      assert.equal(databaseState(target), before);
      const partial = structuredClone(stored);
      partial.events.pop();
      partial.command.events.pop();
      target.transaction(() => assert.throws(() => q.replayQuotaCommit(partial), /complete/))();
      assert.equal(databaseState(target), before);
      target.transaction(() => q.replayQuotaCommit(stored))();
      assert.equal(
        target.prepare('SELECT v5_available AS n FROM wallets WHERE member_id=?').get('m1').n,
        '40.5',
      );
    } finally {
      target.close();
    }
  }
});

test('关系命令与原实际同步/资金 owner 在相同观察和确认序列下保持逐资源一致', async (t) => {
  const f = await setup(t);
  const legacy = {
    settings: {},
    users: ['m1', 'm2'].map((id) => ({ id, enabled: true })),
    jobs: [],
    ledger: [],
    accounts: [
      {
        id: 'a',
        enabled: true,
        quotaTier: 3,
        subscriptionActive: true,
        quotaFixed: '8000',
        quotaPurchased: '0',
        v5UsagePercent: '80',
        v5UsageTimeUntilNextPercent: 0,
        quotaCheckedAt: NOW,
      },
    ],
  };
  configureFairGroups(
    legacy,
    [
      { id: 'one', members: ['m1'], share: 0.5 },
      { id: 'two', members: ['m2'], share: 0.5 },
      { id: 'three', members: [], share: 0 },
      { id: 'four', members: [], share: 0 },
    ],
    { now: NOW },
  );
  syncFairQuota(legacy, NOW);
  const compare = () => {
    const s = legacy.settings.fairQuota,
      q = f.quotas.readAccountQuota('a'),
      o = f.db.prepare('SELECT * FROM operating_state').get();
    for (const id of ['m1', 'm2']) {
      assert.deepEqual(wallet(f, id), {
        v5: s.balances[id].v5Available,
        anlasFixed: s.balances[id].anlasFixedAvailable,
        anlasPurchased: s.balances[id].anlasPurchasedAvailable,
      });
    }
    assert.equal(q.pendingFixedDebit, s.spent.anlasFixed);
    assert.equal(q.fallbackFixed, s.fallbackBuffer.fixed);
    assert.equal(q.sharedV5Debt, s.sharedV5Debt);
    assert.equal(o.unfunded_fixed, s.operating.unfunded.anlasFixed);
    assert.equal(o.unattributed_fixed, s.operating.unattributedReturns.anlasFixed);
    assert.equal(o.anlas_fixed_available, s.operating.available.anlasFixed);
  };
  const observe = async (fixed, at) => {
    await f.observe({ fixed }, at);
    Object.assign(legacy.accounts[0], { quotaFixed: fixed, quotaCheckedAt: at });
    syncFairQuota(legacy, at + 1);
    compare();
  };
  const fund = async (id, body, at) => {
    await f.fund(id, body, at);
    syncFairQuota(legacy, at);
    manageOperatingFunding(
      legacy,
      body,
      {
        actorId: 'm1',
        findEvent: (id) => legacy.ledger.find((e) => e.id === id),
        coveredAmount: (id) =>
          formatAmount(
            legacy.ledger
              .filter((e) => e.type === 'operating_loss_covered' && e.lossEventId === id)
              .reduce((n, e) => n + parseAmount(e.amount), 0n),
          ),
      },
      at,
    );
    compare();
  };
  compare();
  await observe('7900', NOW + 10);
  await observe('8000', NOW + 20);
  await fund(
    'compare-declared',
    register('compare-declared-receipt', 'anlasFixed', 40, 'members', true),
    NOW + 30,
  );
  await fund(
    'compare-contribute',
    contribute('compare-contribute-receipt', 'anlasFixed', 10),
    NOW + 31,
  );
  let lossEventId = legacy.settings.fairQuota.operating.gapEventIds.anlasFixed;
  await fund(
    'compare-cover',
    {
      action: 'operating-cover-loss',
      receipt: 'compare-cover-receipt',
      resource: 'anlasFixed',
      amount: 10,
      lossEventId,
    },
    NOW + 32,
  );
  await fund(
    'compare-registered',
    register('compare-registered-receipt', 'anlasFixed', 30, 'members'),
    NOW + 33,
  );
  await observe('8060', NOW + 40);
  await observe('7990', NOW + 50);
  lossEventId = legacy.settings.fairQuota.operating.gapEventIds.anlasFixed;
  await fund(
    'compare-correct',
    {
      action: 'operating-reconcile-return',
      receipt: 'compare-correct-receipt',
      resource: 'anlasFixed',
      amount: 20,
      lossEventId,
    },
    NOW + 60,
  );
});
