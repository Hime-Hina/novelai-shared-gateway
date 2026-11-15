import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { fixture, meta, NOW, keys, databaseState } from '../helpers/relational-generation.mjs';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalAccountStore } from '../../server/storage/relational-account-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { accountQuotaResult } from '../../server/quota/account-refresh.ts';
import { launcherSubscription } from '../../server/quota/launcher-subscription.ts';
import { configureFairGroups, syncFairQuota } from '../../server/quota/fair-share.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import { ownerAccountCommand } from '../../server/members/owner-operations.ts';

const env = { OWNER_USER_ID: 'm1' };
const groups = [
  { id: 'g', name: 'shared', share: 1, members: ['m1', 'm2'], memberWeights: { m1: 1, m2: 1 } },
  ...['g2', 'g3', 'g4'].map((id) => ({ id, name: id, share: 0, members: [], memberWeights: {} })),
];
function observe(f, expiresAt, patch = {}) {
  const quotas = new RelationalQuotaStore(f.db),
    ticket = quotas.beginQuotaObservation('a', NOW),
    result = accountQuotaResult(
      'a',
      {
        tier: 3,
        subscriptionActive: true,
        subscriptionExpiresAt: expiresAt,
        fixed: '8000',
        purchased: '0',
        v5UsagePercent: 80,
        v5UsageIsNegative: false,
        v5UsageTimeUntilNextPercent: 600,
        ...patch,
      },
      new Date(NOW).toISOString(),
    );
  f.db.transaction(() =>
    quotas.recordQuotaObservation(meta(`observe-${ticket.sequence}`, NOW), ticket, result),
  )();
  return quotas;
}
function readonly(db, read) {
  const before = databaseState(db);
  db.pragma('query_only=ON');
  try {
    const result = read();
    assert.equal(databaseState(db), before);
    return result;
  } finally {
    db.pragma('query_only=OFF');
  }
}
function legacy(expiresAt) {
  const db = {
    users: ['m1', 'm2'].map((id) => ({ id, enabled: true })),
    settings: {},
    accounts: [
      {
        id: 'a',
        enabled: true,
        quotaTier: 3,
        subscriptionActive: true,
        subscriptionExpiresAt: expiresAt,
        quotaCheckedAt: NOW,
        v5UsagePercent: 80,
        v5UsageIsNegative: false,
        v5UsageTimeUntilNextPercent: 600,
        quotaFixed: 8000,
        quotaPurchased: 0,
      },
    ],
  };
  configureFairGroups(db, groups, { now: NOW });
  syncFairQuota(db, NOW);
  return db;
}
function replay(t, source) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const owner = new RelationalCommitReplayer(db);
  for (const { seq } of source
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    db.transaction(() => owner.replayCommit(readRelationalCommit(source, seq)))();
  }
  return db;
}

test('启动器窄读逐字段复用原个人订阅 DTO、恢复估算与保守整数化，query_only 零写且不泄漏身份', async (t) => {
  const f = await fixture(t),
    expiry = NOW + 86400000,
    quotas = observe(f, expiry),
    old = legacy(expiry),
    result = readonly(f.db, () => quotas.readLauncherSubscription('m1', NOW + 60000));
  assert.deepEqual(result, launcherSubscription(old, old.users[0], NOW + 60000));
  assert.equal(result.active, true);
  assert.equal(result.usage.percent, 40.05);
  assert.equal(result.trainingStepsLeft.fixedTrainingStepsLeft, 4000);
  assert.deepEqual(Object.keys(result), [
    'tier',
    'active',
    'expiresAt',
    'trainingStepsLeft',
    'perks',
    'usage',
  ]);
  for (const secret of ['synthetic-upstream-pst', keys.m1, keys.m2, 'm1', 'm2', 'boundAccountId']) {
    assert.ok(!JSON.stringify(result).includes(secret));
  }
  // 公开展示事实可完整回放，私有认证不会恢复；DTO 不是调度或 ready 授权。
  const restored = replay(t, f.db),
    restoredQuotas = new RelationalQuotaStore(restored);
  assert.deepEqual(restoredQuotas.readQuotaConfiguration(), quotas.readQuotaConfiguration());
  assert.deepEqual(
    restoredQuotas.readLauncherSubscription('m1', NOW),
    quotas.readLauncherSubscription('m1', NOW),
  );
  assert.equal(restored.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
});

test('未知/过期到期时间不借另一账户期限，保留个人余额；停用与非成员仍先拒绝', async (t) => {
  const f = await fixture(t),
    quotas = new RelationalQuotaStore(f.db),
    opening = readRelationalCommit(f.db, '3');
  const events = opening.events
    .filter((event) => ['OpeningAccountRecorded', 'OpeningQuotaRecorded'].includes(event.eventType))
    .map((event) => {
      const payload = structuredClone(event.payload);
      payload.accountId = 'another-pool';
      payload.origin.originalId = 'another-pool';
      if (event.eventType === 'OpeningQuotaRecorded') {
        payload.observationEventId = 'another-quota';
        payload.baselineEventId = 'another-quota';
        payload.subscriptionExpiresAt = NOW + 86400000;
      }
      return {
        eventType: event.eventType,
        eventId: event.eventType === 'OpeningQuotaRecorded' ? 'another-quota' : 'another-account',
        streamId: 'account:another-pool',
        accountId: 'another-pool',
        memberId: null,
        occurredAt: NOW,
        policyVersion: event.policyVersion,
        pricingVersion: event.pricingVersion,
        payload,
      };
    });
  const { policyVersion: _policy, pricingVersion: _pricing, ...metadata } = meta('another-pool');
  f.db.transaction(() =>
    new RelationalCommitReplayer(f.db).appendCommit({
      ...metadata,
      expectedVersions: { 'account:another-pool': '0' },
      receipt: {},
      events,
    }),
  )();
  const unknown = readonly(f.db, () => quotas.readLauncherSubscription('m1', NOW));
  assert.equal(unknown.active, false);
  assert.equal(unknown.tier, 0);
  assert.equal(unknown.expiresAt, 0);
  assert.equal(unknown.trainingStepsLeft.fixedTrainingStepsLeft, 4000);
  observe(f, NOW - 1);
  assert.equal(readonly(f.db, () => quotas.readLauncherSubscription('m1', NOW)).active, false);
  f.db.transaction(() =>
    f.members.registerMember(meta('register-outsider'), {
      memberId: 'outsider',
      displayName: 'outsider',
      apiKey: 'secret-outsider',
    }),
  )();
  f.db.transaction(() => f.members.setMemberStatus(meta('disable-m2'), 'm2', 'disabled'))();
  for (const member of ['m2', 'outsider', 'missing']) {
    readonly(f.db, () =>
      assert.throws(() => quotas.readLauncherSubscription(member, NOW), { statusCode: 403 }),
    );
  }
  const personal = await fixture(t, {
      v5: '0',
      secondV5: '40',
      fixed: '3329.999999999999',
      officialFixed: '6659.999999999998',
      purchased: '8.9',
      officialPurchased: '17.8',
      seconds: 0,
    }),
    onlyOwn = readonly(personal.db, () =>
      new RelationalQuotaStore(personal.db).readLauncherSubscription('m1', NOW),
    );
  assert.equal(onlyOwn.usage.percent, 0);
  assert.equal(onlyOwn.usage.isNegative, true);
  assert.equal(onlyOwn.trainingStepsLeft.fixedTrainingStepsLeft, 3330);
  assert.equal(onlyOwn.trainingStepsLeft.purchasedTrainingSteps, 8);
});

test('真实预扣期间允许窄读，暂停账户不伪造 active，金融基线缺失仍为503', async (t) => {
  const f = await fixture(t, { env }),
    quotas = observe(f, NOW + 86400000);
  await f.accept('held-subscription');
  f.dispatch('held-subscription-0');
  const held = readonly(f.db, () => quotas.readLauncherSubscription('m1', NOW + 2));
  assert.equal(held.active, true);
  assert.equal(held.trainingStepsLeft.fixedTrainingStepsLeft, 3998);
  assert.equal(readonly(f.db, () => quotas.readQuotaConfiguration()).configVersion, 1);
  const accounts = new RelationalAccountStore(f.db, quotas, new Map(), env);
  await ownerAccountCommand(
    accounts,
    (write) => withAuthenticatedMemberApiKey(f.members, keys.m1, write, env),
    meta('pause-read', NOW + 3, 'm1'),
    { kind: 'pause', accountId: 'a' },
  );
  assert.equal(readonly(f.db, () => quotas.readLauncherSubscription('m1', NOW + 4)).active, false);
  const missing = await fixture(t, { baseline: false }),
    missingQuotas = new RelationalQuotaStore(missing.db);
  readonly(missing.db, () =>
    assert.throws(() => missingQuotas.readLauncherSubscription('m1', NOW), { statusCode: 503 }),
  );
  assert.equal(readonly(missing.db, () => missingQuotas.readQuotaConfiguration()).configVersion, 1);
});

test('政策窄读只映射原版本/NULL/精度，未绑定初始化可读且未初始化不补默认值', async (t) => {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const members = new RelationalMemberStore(db),
    quotas = new RelationalQuotaStore(db, env);
  readonly(db, () => assert.throws(() => quotas.readQuotaConfiguration(), { statusCode: 503 }));
  for (const id of ['m1', 'm2']) {
    db.transaction(() =>
      members.registerMember(meta(`register-${id}`), {
        memberId: id,
        displayName: id,
        apiKey: keys[id],
      }),
    )();
  }
  const write = (fn) => withAuthenticatedMemberApiKey(members, keys.m1, fn, env);
  await write((member) =>
    quotas.configureGroups(
      meta('unbound-groups', NOW, 'm1'),
      { groups, expectedConfigVersion: 1 },
      member,
    ),
  );
  const fallback = await write((member) =>
    quotas.configureQuotaPolicy(
      meta('read-fallback', NOW, 'm1'),
      { action: 'fallback-policy', enabled: true },
      member,
    ),
  );
  const free = await write((member) =>
    quotas.configureQuotaPolicy(
      meta('read-free', NOW, 'm1'),
      {
        action: 'free-image-policy',
        expectedVersion: 1,
        policy: { enabled: true, warningThreshold: null, hardLimit: 9 },
      },
      member,
    ),
  );
  const loss = await write((member) =>
    quotas.configureQuotaPolicy(
      meta('read-loss', NOW, 'm1'),
      {
        action: 'loss-alert-policy',
        expectedVersion: 1,
        policy: { v5Points24h: 1e-15, fixedAnlas24h: 0, fixedAnlas30d: 13, memberAttempts24h: 0 },
      },
      member,
    ),
  );
  const result = readonly(db, () => quotas.readQuotaConfiguration());
  assert.deepEqual(result, {
    groups,
    configVersion: 2,
    autoAnlasFallback: fallback.receipt.autoAnlasFallback,
    freeImagePolicy: free.receipt.policy,
    lossAlertPolicy: loss.receipt.policy,
  });
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM wallets').get().n, 0);
  assert.equal(result.lossAlertPolicy.v5Points24h, 1e-15);
  assert.equal(result.freeImagePolicy.warningThreshold, null);
  assert.deepEqual(Object.keys(result), [
    'groups',
    'configVersion',
    'autoAnlasFallback',
    'freeImagePolicy',
    'lossAlertPolicy',
  ]);
  const restored = replay(t, db);
  assert.deepEqual(
    readonly(restored, () => new RelationalQuotaStore(restored).readQuotaConfiguration()),
    result,
  );
  const unbound = readonly(db, () => quotas.readLauncherSubscription('m1', NOW));
  assert.equal(unbound.active, false);
  assert.equal(unbound.expiresAt, 0);
  assert.equal(unbound.usage.percent, 0);
});
