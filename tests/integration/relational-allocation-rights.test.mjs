import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { fixture, meta, NOW, keys, databaseState } from '../helpers/relational-generation.mjs';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { accountQuotaResult } from '../../server/quota/account-refresh.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import { parseAmount, formatAmount } from '../../server/quota/resource-amounts.ts';

const resources = ['v5', 'anlasFixed', 'anlasPurchased'];
const zero = () => Object.fromEntries(resources.map((resource) => [resource, '0']));
async function setup(t, options = {}) {
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { seconds: 0, ...options, env }),
    quotas = new RelationalQuotaStore(f.db, env);
  const configure = (weights, at = NOW + 10) =>
    withAuthenticatedMemberApiKey(f.members, keys.m1, (member) =>
      quotas.configureGroups(
        meta(`weights-${at}`, at, member.id),
        {
          expectedConfigVersion: f.db
            .prepare('SELECT config_version AS n FROM sharing_policy')
            .get().n,
          groups: [
            { id: 'g', share: 1, members: ['m1', 'm2'], memberWeights: weights },
            ...['g2', 'g3', 'g4'].map((id) => ({ id, share: 0, members: [] })),
          ],
        },
        member,
      ),
    );
  const fund = (receipt, resource, amount, destination = 'members', at = NOW + 20, extra = {}) =>
    withAuthenticatedMemberApiKey(f.members, keys.m1, (member) =>
      quotas.manageQuotaFunding(
        meta(receipt, at, 'm1'),
        'a',
        {
          action: 'operating-register-funding',
          receipt,
          resource,
          amount,
          destination,
          ...extra,
        },
        member,
      ),
    );
  const observe = (changes, at = NOW + 30) => {
    const q = quotas.readAccountQuota('a'),
      ticket = quotas.beginQuotaObservation('a', at),
      result = accountQuotaResult(
        'a',
        {
          tier: 3,
          subscriptionActive: true,
          subscriptionExpiresAt: null,
          fixed: q.observedAnlasFixed,
          purchased: q.observedAnlasPurchased,
          v5UsagePercent: q.observedV5,
          v5UsageTimeUntilNextPercent: q.secondsPerPercent,
          ...changes,
        },
        new Date(at).toISOString(),
      );
    return f.db.transaction(() =>
      quotas.recordQuotaObservation(meta(`observe-${at}`, at + 1), ticket, result),
    )();
  };
  const refill = (at) =>
    f.db.transaction(() => quotas.refillQuota(meta(`refill-${at}`, at), 'a'))();
  return { ...f, quotas, configure, fund, observe, refill };
}
// 反例保持完整合法 envelope；不把结构/版本错误误算为权益检查通过。
function resequence(stored, db) {
  const versions = new Map();
  stored.command.expectedVersions = {};
  for (const [eventIndex, event] of stored.events.entries()) {
    if (!versions.has(event.streamId)) {
      const version =
        db
          .prepare('SELECT CAST(stream_version AS TEXT) AS n FROM stream_heads WHERE stream_id=?')
          .get(event.streamId)?.n ?? '0';
      versions.set(event.streamId, BigInt(version));
      stored.command.expectedVersions[event.streamId] = version;
    }
    versions.set(event.streamId, versions.get(event.streamId) + 1n);
    Object.assign(event, { eventIndex, streamVersion: String(versions.get(event.streamId)) });
  }
  stored.eventCount = stored.events.length;
  stored.command.events = stored.events.map(
    ({
      eventType,
      payload,
      streamId,
      occurredAt,
      accountId,
      memberId,
      policyVersion,
      pricingVersion,
    }) => ({
      eventType,
      payload,
      streamId,
      occurredAt,
      accountId,
      memberId,
      policyVersion,
      pricingVersion,
    }),
  );
  return stored;
}
function rejectedMutation(t, snapshot, stored, mutate) {
  const source = new Database(snapshot),
    db = new Database(':memory:');
  t.after(() => source.close());
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const inputs = new RelationalInputStore(source),
    replayer = new RelationalCommitReplayer(db);
  for (const { seq } of source
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    db.transaction(() => replayer.replayCommit(readRelationalCommit(source, seq), inputs))();
  }
  const owner = new RelationalCommitReplayer(db),
    bad = structuredClone(stored),
    before = databaseState(db);
  mutate(bad);
  resequence(bad, db);
  db.transaction(() =>
    assert.throws(
      () => owner.replayCommit(bad),
      (error) => error.statusCode === 409,
    ),
  )();
  assert.equal(databaseState(db), before, '外层捕获后事实、回执、游标及所有投影不变');
  db.transaction(() => owner.replayCommit(stored))();
  const once = db.serialize();
  db.transaction(() => assert.equal(owner.replayCommit(stored).replayed, true))();
  assert.deepEqual(db.serialize(), once);
}
function redirect(allocation, memberId) {
  allocation.payload.allocations = [
    {
      memberId,
      resources: Object.fromEntries(
        resources.map((resource) => [
          resource,
          formatAmount(
            allocation.payload.allocations.reduce(
              (sum, entry) => sum + parseAmount(entry.resources[resource]),
              0n,
            ),
          ),
        ]),
      ),
    },
  ];
}

test('恢复按原未满额权重与速率决定；零权、非零比例、预算和最小尾数不能伪造', async (t) => {
  for (const [label, weights, options, at] of [
    ['zero', { m1: 1, m2: 0 }, { v5: '10', secondV5: '10', seconds: 600 }, NOW + 60000],
    ['ratio', { m1: 3, m2: 1 }, { v5: '10', secondV5: '10', seconds: 600 }, NOW + 60000],
    [
      'quantum',
      { m1: 3, m2: 1 },
      { v5: '0', secondV5: '0', officialV5: '0', seconds: 1000000000 },
      NOW + 11,
    ],
  ]) {
    await t.test(label, async (t) => {
      const f = await setup(t, options);
      await f.configure(weights);
      const snapshot = f.db.serialize(),
        result = f.refill(at),
        stored = readRelationalCommit(f.db, result.commitSeq),
        refill = stored.events.find((event) => event.eventType === 'RefillApplied'),
        total = refill.payload.allocations.reduce(
          (sum, entry) => sum + parseAmount(entry.amount),
          0n,
        );
      assert.ok(total > 0n);
      if (label === 'zero')
        assert.deepEqual(
          refill.payload.allocations.map((entry) => entry.memberId),
          ['m1'],
        );
      if (label === 'ratio')
        assert.ok(
          parseAmount(refill.payload.allocations[0].amount) >
            parseAmount(refill.payload.allocations[1].amount),
        );
      if (label === 'quantum')
        assert.deepEqual(refill.payload.allocations, [
          { memberId: 'm1', amount: '0.000000000001' },
        ]);
      for (const mutation of ['recipient', 'budget', 'cursor', 'debt', 'capacity']) {
        await t.test(mutation, (t) =>
          rejectedMutation(t, snapshot, stored, (bad) => {
            const p = bad.events.find((event) => event.eventType === 'RefillApplied').payload;
            if (mutation === 'recipient')
              p.allocations = [{ memberId: 'm2', amount: formatAmount(total) }];
            if (mutation === 'budget') {
              p.budget = formatAmount(parseAmount(p.budget) + total);
              p.allocations[0].amount = formatAmount(parseAmount(p.allocations[0].amount) + total);
            }
            if (mutation === 'cursor') p.until--;
            if (mutation === 'debt') {
              p.debtRepaid = '0.000000000001';
              p.allocations[0].amount = formatAmount(parseAmount(p.allocations[0].amount) - 1n);
            }
            if (mutation === 'capacity') {
              p.capacityRecovered = '0.000000000001';
              p.allocations[0].amount = formatAmount(parseAmount(p.allocations[0].amount) - 1n);
            }
          }),
        );
      }
    });
  }
});

test('观察净额与原收据角色共同决定入账，守恒仍不能换受益人或放大预算', async (t) => {
  for (const weights of [
    { m1: 1, m2: 0 },
    { m1: 3, m2: 1 },
  ]) {
    await t.test(JSON.stringify(weights), async (t) => {
      const f = await setup(t);
      await f.configure(weights);
      await f.fund('rights-member-fund', 'anlasFixed', 60);
      await f.fund('rights-operator-fund', 'anlasFixed', 40, 'operating', NOW + 21);
      const snapshot = f.db.serialize(),
        result = f.observe({ fixed: '8100' }),
        stored = readRelationalCommit(f.db, result.commitSeq),
        allocation = stored.events.find((event) => event.eventType === 'AllocationApplied');
      assert.equal(allocation.payload.sourceResources.anlasFixed, '60');
      assert.deepEqual(
        allocation.payload.allocations,
        weights.m2 === 0
          ? [{ memberId: 'm1', resources: { ...zero(), anlasFixed: '60' } }]
          : [
              { memberId: 'm1', resources: { ...zero(), anlasFixed: '45' } },
              { memberId: 'm2', resources: { ...zero(), anlasFixed: '15' } },
            ],
      );
      for (const mutation of [
        'recipient',
        'budget',
        'role',
        'receipt',
        'missing-funding',
        'missing-allocation',
        'no-sync-facts',
        'late-funding',
        'reason',
      ]) {
        await t.test(mutation, (t) =>
          rejectedMutation(t, snapshot, stored, (bad) => {
            const allocation = bad.events.find((event) => event.eventType === 'AllocationApplied'),
              funding = bad.events.find((event) => event.eventType === 'FundingReceived');
            if (mutation === 'recipient') redirect(allocation, 'm2');
            if (mutation === 'budget') {
              allocation.payload.sourceResources.anlasFixed = '1000';
              allocation.payload.allocations = [
                { memberId: 'm1', resources: { ...zero(), anlasFixed: '1000' } },
              ];
            }
            if (mutation === 'role')
              funding.payload.destination =
                funding.payload.destination === 'members' ? 'operating' : 'members';
            if (mutation === 'receipt')
              funding.payload.receiptEventId = 'operating-receipt:rights-operator-fund';
            if (mutation === 'missing-funding')
              bad.events = bad.events.filter((event) => event !== funding);
            if (mutation === 'missing-allocation')
              bad.events = bad.events.filter((event) => event !== allocation);
            if (mutation === 'no-sync-facts')
              bad.events = bad.events.filter((event) =>
                ['QuotaObserved', 'QuotaStatusChanged', 'AvailabilityChanged'].includes(
                  event.eventType,
                ),
              );
            if (mutation === 'late-funding') {
              bad.events = bad.events.filter((event) => event !== funding);
              bad.events.push(funding);
            }
            if (mutation === 'reason') allocation.payload.reason = 'opening';
          }),
        );
      }
    });
  }
});

test('配置或人工资金剩余入口仍核原同步，删光前置恢复不能留下成功回执', async (t) => {
  for (const command of ['groups', 'funding']) {
    await t.test(command, async (t) => {
      const f = await setup(t, { v5: '10', secondV5: '10', seconds: 600 }),
        snapshot = f.db.serialize(),
        result =
          command === 'groups'
            ? await f.configure({ m1: 3, m2: 1 })
            : await f.fund('omitted-prefill-funding', 'anlasFixed', 100),
        stored = readRelationalCommit(f.db, result.commitSeq);
      assert.ok(stored.events.some((event) => event.eventType === 'RefillApplied'));
      rejectedMutation(t, snapshot, stored, (bad) => {
        bad.events = bad.events.filter(
          (event) =>
            event.eventType === (command === 'groups' ? 'SharingConfigured' : 'FundingRegistered'),
        );
      });
    });
  }
});

test('三资源首次开账的原观察预算不能自报增发，采纳先于分配的原顺序仍合法', async (t) => {
  const f = await setup(t, { v5: '0', secondV5: '0', fixed: '0', baseline: false }),
    snapshot = f.db.serialize(),
    result = f.observe({ fixed: '8000', purchased: '100' }),
    stored = readRelationalCommit(f.db, result.commitSeq),
    adoption = stored.events.find((event) => event.eventType === 'QuotaBaselineAdopted');
  for (const resource of resources) {
    await t.test(resource, (t) => {
      const allocation = stored.events.find(
        (event) =>
          event.eventType === 'AllocationApplied' &&
          event.payload.sourceResources[resource] !== '0',
      );
      assert.ok(adoption.eventIndex < allocation.eventIndex);
      assert.equal(allocation.payload.reason, 'opening');
      rejectedMutation(t, snapshot, stored, (bad) => {
        const p = bad.events.find((event) => event.eventId === allocation.eventId).payload;
        p.sourceResources[resource] = formatAmount(parseAmount(p.sourceResources[resource]) * 2n);
        for (const entry of p.allocations)
          entry.resources[resource] = formatAmount(parseAmount(entry.resources[resource]) * 2n);
      });
    });
  }
});

test('待归属返回不是普通注资；净观察预算扣除原返回后才按历史权重入账', async (t) => {
  const f = await setup(t);
  await f.configure({ m1: 3, m2: 1 });
  f.observe({ fixed: '7900' }, NOW + 20);
  const snapshot = f.db.serialize(),
    result = f.observe({ fixed: '8050' }, NOW + 30),
    stored = readRelationalCommit(f.db, result.commitSeq),
    allocation = stored.events.find((event) => event.eventType === 'AllocationApplied');
  assert.equal(allocation.payload.sourceResources.anlasFixed, '50');
  assert.equal(
    stored.events.find((event) => event.eventType === 'ReturnClassified').payload.amount,
    '100',
  );
  for (const mutation of ['return', 'gap-id', 'omit-return', 'budget']) {
    await t.test(mutation, (t) =>
      rejectedMutation(t, snapshot, stored, (bad) => {
        const returned = bad.events.find((event) => event.eventType === 'ReturnClassified'),
          allocation = bad.events.find((event) => event.eventType === 'AllocationApplied');
        if (mutation === 'return') returned.payload.amount = '0';
        if (mutation === 'gap-id') returned.payload.gapEventId = null;
        if (mutation === 'omit-return')
          bad.events = bad.events.filter((event) => event !== returned);
        if (mutation === 'budget') {
          allocation.payload.sourceResources.anlasFixed = '150';
          allocation.payload.allocations = [
            { memberId: 'm1', resources: { ...zero(), anlasFixed: '112.5' } },
            { memberId: 'm2', resources: { ...zero(), anlasFixed: '37.5' } },
          ];
        }
      }),
    );
  }
});

test('直接凭据和人工扣款确认仍复用原加权分配，运营到账不变成成员权利', async (t) => {
  for (const kind of ['declared', 'confirmed']) {
    await t.test(kind, async (t) => {
      const f = await setup(t);
      await f.configure({ m1: 3, m2: 1 });
      if (kind === 'declared') {
        f.observe({ fixed: '7900' }, NOW + 20);
        f.observe({ fixed: '8000' }, NOW + 30);
      } else {
        await f.fund('confirm-operator', 'anlasFixed', 1, 'operating', NOW + 20);
        await f.accept('confirm-job', { now: NOW + 30 });
        f.dispatch('confirm-job-0', NOW + 31);
        await f.publish('confirm-job-0', NOW + 32);
        f.observe({ fixed: '8000' }, NOW + 40);
      }
      const snapshot = f.db.serialize(),
        result =
          kind === 'declared'
            ? await f.fund('direct-rights', 'anlasFixed', 3, 'members', NOW + 50, {
                fromUnattributed: true,
              })
            : await withAuthenticatedMemberApiKey(f.members, keys.m1, (member) =>
                f.quotas.manageQuotaFunding(
                  meta('confirmed-rights', NOW + 50, 'm1'),
                  'a',
                  {
                    action: 'operating-confirm-debits',
                    receipt: 'confirmed-rights',
                    resource: 'anlasFixed',
                    amount: 2,
                  },
                  member,
                ),
              ),
        stored = readRelationalCommit(f.db, result.commitSeq),
        allocation = stored.events.find((event) => event.eventType === 'AllocationApplied');
      assert.equal(allocation.payload.sourceResources.anlasFixed, kind === 'declared' ? '3' : '1');
      rejectedMutation(t, snapshot, stored, (bad) =>
        redirect(
          bad.events.find((event) => event.eventType === 'AllocationApplied'),
          'm2',
        ),
      );
      rejectedMutation(t, snapshot, stored, (bad) => {
        bad.events = bad.events.filter((event) => event.eventType !== 'AllocationApplied');
      });
      if (kind === 'confirmed') {
        rejectedMutation(t, snapshot, stored, (bad) => {
          bad.events = bad.events.filter((event) => event.eventType !== 'FundingReceived');
        });
        rejectedMutation(t, snapshot, stored, (bad) => {
          const source = bad.events.find((event) => event.eventType === 'DebitsConfirmed'),
            allocation = bad.events.find((event) => event.eventType === 'AllocationApplied');
          source.payload.allocatedToMembers = '2';
          allocation.payload.sourceResources.anlasFixed = '2';
          allocation.payload.allocations = [
            { memberId: 'm1', resources: { ...zero(), anlasFixed: '1.5' } },
            { memberId: 'm2', resources: { ...zero(), anlasFixed: '0.5' } },
          ];
        });
      }
    });
  }
});

test('原恢复先偿还共享负债和运营容量，不得把同一预算改发个人', async (t) => {
  for (const priority of ['debtRepaid', 'capacityRecovered']) {
    await t.test(priority, async (t) => {
      const f = await setup(
        t,
        priority === 'debtRepaid'
          ? { v5: '0', secondV5: '40', fallback: true, seconds: 600 }
          : { seconds: 600 },
      );
      if (priority === 'debtRepaid') {
        await f.accept('priority-fallback', { model: 'nai-diffusion-5-full', steps: 23 });
        f.dispatch('priority-fallback-0');
        await f.publish('priority-fallback-0');
        f.observe({ fixed: '8000' }, NOW + 10);
        assert.ok(parseAmount(f.quotas.readAccountQuota('a').sharedV5Debt) > 0n);
      } else {
        f.observe({ v5UsagePercent: '50' }, NOW + 10);
        assert.ok(
          parseAmount(f.db.prepare('SELECT v5_capacity_gap AS n FROM operating_state').get().n) >
            0n,
        );
      }
      const snapshot = f.db.serialize(),
        result = f.refill(NOW + 12),
        stored = readRelationalCommit(f.db, result.commitSeq),
        original = stored.events.find((event) => event.eventType === 'RefillApplied').payload;
      assert.ok(parseAmount(original[priority]) > 0n);
      assert.deepEqual(original.allocations, []);
      rejectedMutation(t, snapshot, stored, (bad) => {
        const p = bad.events.find((event) => event.eventType === 'RefillApplied').payload;
        p.allocations = [{ memberId: 'm1', amount: p[priority] }];
        p[priority] = '0';
      });
    });
  }
});

test('观察原待扣和 fallback 抵消组成不能省略后留作另一份可确认资金', async (t) => {
  for (const kind of ['debit', 'fallback']) {
    await t.test(kind, async (t) => {
      const f = await setup(
        t,
        kind === 'fallback' ? { v5: '0', secondV5: '40', fallback: true } : {},
      );
      await f.accept(
        `offset-${kind}`,
        kind === 'fallback' ? { model: 'nai-diffusion-5-full', steps: 23 } : {},
      );
      f.dispatch(`offset-${kind}-0`);
      await f.publish(`offset-${kind}-0`);
      const charge = f.generations.readCharge(`offset-${kind}-0`);
      if (kind === 'fallback') f.observe({ fixed: '8000' }, NOW + 10);
      const snapshot = f.db.serialize(),
        result = f.observe(
          { fixed: formatAmount(parseAmount('8000') - parseAmount(charge.anlasFixed)) },
          NOW + 20,
        ),
        stored = readRelationalCommit(f.db, result.commitSeq),
        adoption = stored.events.find((event) => event.eventType === 'QuotaBaselineAdopted');
      assert.equal(
        kind === 'fallback'
          ? adoption.payload.matchedFallbackFixed
          : adoption.payload.matchedDebits.anlasFixed,
        charge.anlasFixed,
      );
      rejectedMutation(t, snapshot, stored, (bad) => {
        const p = bad.events.find((event) => event.eventType === 'QuotaBaselineAdopted').payload;
        if (kind === 'fallback') p.matchedFallbackFixed = '0';
        else p.matchedDebits.anlasFixed = '0';
      });
    });
  }
});

test('合法第二钱包 SQL 故障确实发生在首钱包之后，外层捕获仍整笔回滚', async (t) => {
  const f = await setup(t),
    ticket = f.quotas.beginQuotaObservation('a', NOW + 20),
    result = accountQuotaResult(
      'a',
      {
        tier: 3,
        subscriptionActive: true,
        subscriptionExpiresAt: null,
        fixed: '9000',
        purchased: '0',
        v5UsagePercent: '80',
        v5UsageTimeUntilNextPercent: 0,
      },
      new Date(NOW + 20).toISOString(),
    );
  let writes = 0;
  f.db.function('wallet_was_written', () => {
    writes++;
  });
  f.db
    .exec(`CREATE TEMP TRIGGER actual_second_wallet_failure AFTER UPDATE OF anlas_fixed_available ON wallets
    BEGIN SELECT wallet_was_written(); SELECT CASE WHEN NEW.member_id='m2' THEN RAISE(ABORT,'actual second wallet failure') END; END;`);
  const before = databaseState(f.db);
  f.db.transaction(() =>
    assert.throws(
      () => f.quotas.recordQuotaObservation(meta('real-write-failure', NOW + 21), ticket, result),
      /actual second wallet failure/,
    ),
  )();
  assert.equal(writes, 2);
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER actual_second_wallet_failure');
  f.db.transaction(() =>
    f.quotas.recordQuotaObservation(meta('real-write-failure', NOW + 21), ticket, result),
  )();
  assert.equal(f.generations.readWallet('a', 'm1').anlasFixed, '4500');
});

test('后来的权重变更不重算旧注资/恢复；完整共同回放保存原角色且重复零写', async (t) => {
  const f = await setup(t, { v5: '10', secondV5: '10', seconds: 600 });
  await f.configure({ m1: 3, m2: 1 });
  await f.fund('historical-funding', 'anlasFixed', 100);
  const original = f.observe({ fixed: '8100' });
  f.refill(NOW + 10000);
  await f.configure({ m1: 0, m2: 1 }, NOW + 10001);
  f.refill(NOW + 20000);
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const owner = new RelationalCommitReplayer(db);
  let upstreamCalls = 0;
  t.mock.method(globalThis, 'fetch', async () => {
    upstreamCalls++;
    throw new Error('replay must not query an upstream');
  });
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    db.transaction(() => owner.replayCommit(readRelationalCommit(f.db, seq), f.inputs))();
  }
  for (const table of [
    'wallets',
    'account_quota_state',
    'operating_state',
    'pending_funding',
    'sharing_groups',
    'group_members',
    'sharing_policy',
    'commits',
    'domain_events',
    'stream_heads',
  ]) {
    assert.deepEqual(
      db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
      f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
      table,
    );
  }
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
  assert.equal(upstreamCalls, 0);
  const before = db.serialize();
  db.transaction(() =>
    assert.equal(owner.replayCommit(readRelationalCommit(f.db, original.commitSeq)).replayed, true),
  )();
  assert.deepEqual(db.serialize(), before);
});
