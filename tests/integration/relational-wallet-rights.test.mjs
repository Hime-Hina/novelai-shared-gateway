import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { meta, NOW, keys, openingFacts, databaseState } from '../helpers/relational-generation.mjs';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import { accountQuotaResult } from '../../server/quota/account-refresh.ts';
import {
  normalizeMemberGeneration,
  materializeMemberGeneration,
} from '../../server/generation/member-contract.ts';
import { parseAmount, formatAmount } from '../../server/quota/resource-amounts.ts';

function setup(t, options = {}) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const env = { OWNER_USER_ID: 'm1' },
    members = new RelationalMemberStore(db),
    quotas = new RelationalQuotaStore(db, env),
    generations = new RelationalGenerationStore(db, env);
  for (const id of ['m1', 'm2'])
    db.transaction(() =>
      members.registerMember(meta(`register-${id}`), {
        memberId: id,
        displayName: id,
        apiKey: keys[id],
      }),
    )();
  const events = openingFacts({
    v5: '50',
    secondV5: '50',
    officialV5: '100',
    seconds: 600,
    ...options,
  }).filter((e) => e.eventType !== 'OpeningGroupMemberRecorded' || e.payload.memberId !== 'm2');
  if (options.zeroFormerMember) {
    const wallet = events.find(
      (e) => e.eventType === 'OpeningWalletRecorded' && e.payload.memberId === 'm2',
    );
    wallet.payload.available = { v5: '0', anlasFixed: '0', anlasPurchased: '0' };
  }
  if (options.otherPool) {
    for (const type of [
      'OpeningAccountRecorded',
      'OpeningWalletRecorded',
      'OpeningQuotaRecorded',
      'OpeningOperatingRecorded',
    ]) {
      const e = structuredClone(events.find((e) => e.eventType === type));
      e.eventId = `other-${type}`;
      e.streamId = 'account:b';
      e.accountId = 'b';
      e.payload.accountId = 'b';
      e.payload.origin.originalId = `b-${type}`;
      if (type === 'OpeningAccountRecorded') e.payload.enabled = false;
      if (type === 'OpeningQuotaRecorded')
        Object.assign(e.payload, { observationEventId: e.eventId, baselineEventId: e.eventId });
      events.push(e);
    }
  }
  const { policyVersion: _p, pricingVersion: _r, ...command } = meta('opening');
  db.transaction(() =>
    new RelationalCommitReplayer(db).appendCommit({
      ...command,
      expectedVersions: Object.fromEntries(events.map((e) => [e.streamId, '0'])),
      events,
      receipt: { opened: true },
    }),
  )();
  db.prepare('INSERT INTO upstream_credentials(account_id,pst) VALUES(?,?)').run(
    'a',
    'synthetic-pst',
  );
  const refill = (at = NOW + 60000) =>
    db.transaction(() => quotas.refillQuota(meta(`refill-${at}`, at), 'a'))();
  const observe = (changes, at = NOW + 60001) => {
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
    return db.transaction(() =>
      quotas.recordQuotaObservation(meta(`observe-${at}`, at + 1), ticket, result),
    )();
  };
  return { db, members, quotas, generations, refill, observe };
}
function total(db, accountId = 'a') {
  return formatAmount(
    db
      .prepare('SELECT v5_available AS n FROM wallets WHERE account_id=?')
      .all(accountId)
      .reduce((sum, row) => sum + parseAmount(row.n), 0n),
  );
}
function replay(t, source) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const owner = new RelationalCommitReplayer(db),
    inputs = new RelationalInputStore(source);
  for (const { seq } of source
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all())
    db.transaction(() => owner.replayCommit(readRelationalCommit(source, seq), inputs))();
  return { db, owner, inputs };
}
function sameFinancial(source, target) {
  for (const table of [
    'wallets',
    'account_quota_state',
    'operating_state',
    'pending_funding',
    'sharing_groups',
    'group_members',
    'sharing_policy',
  ]) {
    assert.deepEqual(
      target.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
      source.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
      table,
    );
  }
}

test('未归组正额钱包仍占原账户物理预算，不能重复恢复；估算/原观察不改其权益', async (t) => {
  const f = setup(t),
    before = databaseState(f.db),
    summary = f.quotas.readMemberQuotaSummary('m1', NOW + 60000);
  assert.equal(summary.v5Available, 50);
  assert.equal(summary.v5FullRefillSeconds, null);
  assert.equal(summary.v5RefillLimited, true);
  assert.equal(f.quotas.readMemberQuotaSummary('m2', NOW + 60000).member, false);
  assert.equal(databaseState(f.db), before);
  const result = f.refill();
  assert.equal(total(f.db), '100');
  assert.deepEqual(f.generations.readWallet('a', 'm2'), {
    v5: '50',
    anlasFixed: '4000',
    anlasPurchased: '0',
  });
  const refill = readRelationalCommit(f.db, result.commitSeq).events.find(
    (e) => e.eventType === 'RefillApplied',
  );
  assert.deepEqual(refill.payload.allocations, []);
  const settled = databaseState(f.db);
  assert.equal(f.refill().replayed, true);
  assert.equal(databaseState(f.db), settled);
  const observed = f.observe({ v5UsagePercent: '100' }),
    facts = readRelationalCommit(f.db, observed.commitSeq).events;
  assert.equal(
    facts.find((e) => e.eventType === 'V5CapacityObserved').payload.status,
    'within_precision',
  );
  assert.equal(total(f.db), '100');
  const restored = replay(t, f.db);
  sameFinancial(f.db, restored.db);
  const after = databaseState(restored.db);
  restored.db.transaction(() =>
    restored.owner.replayCommit(readRelationalCommit(f.db, observed.commitSeq), restored.inputs),
  )();
  assert.equal(databaseState(restored.db), after);
  assert.equal(restored.db.prepare('SELECT count(*) AS n FROM upstream_credentials').get().n, 0);
});

test('剩余真实room只给当前组，零旧钱包/别的原账户不会消耗本池room', async (t) => {
  for (const scenario of [
    { v5: '20', secondV5: '50', officialV5: '70', expected: '70.1' },
    { v5: '20', secondV5: '0', officialV5: '20', expected: '20.1' },
    { v5: '20', secondV5: '0', officialV5: '20', otherPool: true, expected: '20.1' },
  ])
    await t.test(JSON.stringify(scenario), () => {
      const f = setup(t, scenario),
        old = f.generations.readWallet('a', 'm2'),
        other = f.generations.readWallet('b', 'm1');
      f.refill();
      assert.equal(total(f.db), scenario.expected);
      assert.equal(f.generations.readWallet('a', 'm1').v5, '20.1');
      assert.deepEqual(f.generations.readWallet('a', 'm2'), old);
      assert.deepEqual(f.generations.readWallet('b', 'm1'), other);
      const restored = replay(t, f.db);
      sameFinancial(f.db, restored.db);
    });
});

test('拥有旧钱包不授予新的派发、恢复或转账资格，重新入组才可分配且保留原余额', async (t) => {
  const f = setup(t, { v5: '20', secondV5: '50', officialV5: '70', seconds: 0 }),
    before = databaseState(f.db);
  assert.throws(
    () => f.quotas.estimateGenerationQuota('a', 'm2', NOW),
    (e) => e.statusCode === 503,
  );
  await assert.rejects(
    () =>
      withAuthenticatedMemberApiKey(f.members, keys.m1, (actor) =>
        f.quotas.manageMemberQuota(
          meta('old-source', NOW + 1, actor.id),
          {
            action: 'transfer-quota',
            expectedConfigVersion: 1,
            fromUserId: 'm2',
            toUserId: 'm1',
            resource: 'anlasFixed',
            amount: 1,
          },
          actor,
          new Map(),
        ),
      ),
    /转出成员未加入/,
  );
  const normalized = normalizeMemberGeneration({
    operation: 'generate',
    model: 'nai-diffusion-4-5-full',
    input: 'synthetic',
    parameters: { width: 128, height: 128, steps: 30, seed: 42, n_samples: 1 },
  });
  const input = {
    inputId: 'off-input',
    requestId: 'off-request',
    memberId: 'm2',
    source: 'native',
    seedMode: 'fixed',
    confirmation: null,
    request: materializeMemberGeneration(normalized),
    context: { kind: 'native', native: normalized.normalizationContext.native },
    jobs: [
      {
        jobId: 'off-job',
        status: 'pending',
        queueSeq: null,
        queueExpiresAt: null,
        deadlineAt: null,
        admissionEpoch: 0,
        cacheKey: null,
      },
    ],
  };
  assert.equal(databaseState(f.db), before);
  const pending = setup(t, { v5: '20', secondV5: '50', officialV5: '70', seconds: 0 });
  await withAuthenticatedMemberApiKey(pending.members, keys.m2, () =>
    pending.generations.acceptGeneration(meta('off-accept', NOW + 1, 'm2'), input),
  );
  const queued = databaseState(pending.db);
  pending.db.transaction(() =>
    assert.throws(
      () =>
        pending.generations.dispatchGeneration(
          meta('off-dispatch', NOW + 2),
          { jobId: 'off-job', accountId: 'a', attemptId: 'off-attempt' },
          { dispatchEnabled: () => true, physicalAccountLoad: () => 0 },
        ),
      /当前分组/,
    ),
  )();
  assert.equal(databaseState(pending.db), queued);
  const configured = [
    { id: 'g', name: 'restored', share: 1, members: ['m1', 'm2'], memberWeights: { m1: 1, m2: 1 } },
    ...['g2', 'g3', 'g4'].map((id) => ({ id, share: 0, members: [] })),
  ];
  const result = await withAuthenticatedMemberApiKey(f.members, keys.m1, (actor) =>
    f.quotas.configureGroups(
      meta('restore-group', NOW + 1, actor.id),
      { groups: configured, expectedConfigVersion: 1 },
      actor,
    ),
  );
  assert.equal(result.receipt.configVersion, 2);
  assert.equal(f.generations.readWallet('a', 'm2').v5, '50');
  assert.equal(f.quotas.readMemberQuotaSummary('m2', NOW + 1).member, true);
  const restored = replay(t, f.db);
  sameFinancial(f.db, restored.db);
});

test('新Anlas净到账只归当前份额，组命令不能静默留下或清除非零旧权利', async (t) => {
  const f = setup(t, { seconds: 0 }),
    old = f.generations.readWallet('a', 'm2');
  f.observe({ fixed: '8100' }, NOW + 10);
  assert.equal(f.generations.readWallet('a', 'm1').anlasFixed, '4100');
  assert.deepEqual(f.generations.readWallet('a', 'm2'), old);
  const before = databaseState(f.db);
  await assert.rejects(
    () =>
      withAuthenticatedMemberApiKey(f.members, keys.m1, (actor) =>
        f.quotas.configureGroups(
          meta('omit-old-wallet', NOW + 11, actor.id),
          {
            expectedConfigVersion: 1,
            groups: [
              { id: 'g', share: 1, members: ['m1'], memberWeights: { m1: 1 } },
              ...['g2', 'g3', 'g4'].map((id) => ({ id, share: 0, members: [] })),
            ],
          },
          actor,
        ),
      ),
    /不能移除仍持有额度/,
  );
  assert.equal(databaseState(f.db), before);
  const restored = replay(t, f.db);
  sameFinancial(f.db, restored.db);
});

test('完整合法envelope不能把原零room恢复决定伪造为新成员余额', async (t) => {
  const f = setup(t),
    before = f.db.serialize(),
    result = f.refill(),
    stored = structuredClone(readRelationalCommit(f.db, result.commitSeq));
  stored.events.find((e) => e.eventType === 'RefillApplied').payload.allocations = [
    { memberId: 'm1', amount: '0.1' },
  ];
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
  const source = new Database(before);
  t.after(() => source.close());
  const { db, owner } = replay(t, source),
    snapshot = databaseState(db);
  db.transaction(() => assert.throws(() => owner.replayCommit(stored), /同步金融事实与原观察/))();
  assert.equal(databaseState(db), snapshot);
  assert.equal(total(db), '100');
});

test('原生后段SQL失败即使被外层捕获，游标/事实/全部旧钱包仍原子回滚', async (t) => {
  const f = setup(t),
    before = databaseState(f.db);
  let writes = 0;
  f.db.function('count_cursor_write', () => {
    writes++;
    return 0;
  });
  f.db.exec(
    "CREATE TEMP TRIGGER fail_cursor AFTER UPDATE ON account_quota_state BEGIN SELECT count_cursor_write();SELECT RAISE(ABORT,'synthetic cursor failure');END",
  );
  f.db.transaction(() =>
    assert.throws(
      () => f.quotas.refillQuota(meta('fail-refill', NOW + 60000), 'a'),
      /synthetic cursor failure/,
    ),
  )();
  assert.equal(writes, 1);
  assert.equal(databaseState(f.db), before);
  assert.equal(total(f.db), '100');
  f.db.exec('DROP TRIGGER fail_cursor');
  f.refill();
  assert.equal(total(f.db), '100');
});

test('现存零钱包不是当前成员，原移除仍让其继承源slot/weight且不清除身份/钱包行', async (t) => {
  const f = setup(t, { zeroFormerMember: true, seconds: 0 });
  const result = f.db.transaction(() =>
    f.quotas.manageMemberQuota(
      { ...meta('inherit-former-member', NOW + 1), actorKind: 'admin' },
      { action: 'delete-member', expectedConfigVersion: 1, userId: 'm1', transferToUserId: 'm2' },
      null,
      new Map(),
    ),
  )();
  assert.equal(result.receipt.autoJoined.userId, 'm2');
  assert.equal(result.receipt.autoJoined.groupId, 'g');
  assert.deepEqual(f.db.prepare('SELECT member_id FROM group_members').all(), [
    { member_id: 'm2' },
  ]);
  assert.equal(f.generations.readWallet('a', 'm2').v5, '50');
  assert.equal(f.generations.readWallet('a', 'm2').anlasFixed, '4000');
  assert.deepEqual(f.generations.readWallet('a', 'm1'), {
    v5: '0',
    anlasFixed: '0',
    anlasPurchased: '0',
  });
  assert.equal(f.db.prepare("SELECT status FROM members WHERE id='m1'").get().status, 'removed');
  const restored = replay(t, f.db);
  sameFinancial(f.db, restored.db);
});

test('旧钱包不能代替运营者当前分组资格，拒绝决定前后全表相同', async (t) => {
  const f = setup(t, { seconds: 0 }),
    quotas = new RelationalQuotaStore(f.db, { OWNER_USER_ID: 'm2' }),
    before = databaseState(f.db);
  await assert.rejects(
    () =>
      withAuthenticatedMemberApiKey(f.members, keys.m2, () =>
        quotas.manageQuotaFunding(meta('former-contribute', NOW + 1, 'm2'), 'a', {
          action: 'operating-contribute',
          resource: 'anlasFixed',
          amount: 1,
        }),
      ),
    /需要已分组的运营者/,
  );
  assert.equal(databaseState(f.db), before);
});
