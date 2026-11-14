import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import {
  fixture,
  meta,
  NOW,
  keys,
  databaseState,
  openingFacts,
} from '../helpers/relational-generation.mjs';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalAccountStore } from '../../server/storage/relational-account-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { accountQuotaResult } from '../../server/quota/account-refresh.ts';
import {
  authenticateMemberApiKey,
  withAuthenticatedMemberApiKey,
} from '../../server/members/authentication.ts';

const at = NOW + 10000,
  groups = (weights = { m1: 3, m2: 1 }) => [
    { id: 'g', name: 'shared', share: 1, members: ['m1', 'm2'], memberWeights: weights },
    ...['g2', 'g3', 'g4'].map((id) => ({ id, name: id, share: 0, members: [], memberWeights: {} })),
  ],
  input = (redistributeRemaining = false) => ({
    groups: groups(),
    expectedConfigVersion: 1,
    redistributeRemaining,
  }),
  actorMetadata = (id = 'm1') => ({ actorKind: 'member', actorId: id, recordedAt: at });
async function setup(t, options = {}) {
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { ...options, env });
  return { ...f, env, quotas: new RelationalQuotaStore(f.db, env) };
}
async function preview(f, change, key = keys.m1) {
  return withAuthenticatedMemberApiKey(
    f.members,
    key,
    (member) => f.quotas.previewGroups(actorMetadata(member.id), change, member),
    f.env,
  );
}
async function readOnlyPreview(f, change) {
  const before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() AS n').get().n;
  const value = await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    (member) => {
      // 原认证先取得自己的 IMMEDIATE 事务；预览的每条 SQL 仍受 query_only 约束。
      f.db.pragma('query_only=ON');
      try {
        return f.quotas.previewGroups(actorMetadata(member.id), change, member);
      } finally {
        f.db.pragma('query_only=OFF');
      }
    },
    f.env,
  );
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
  return value;
}
async function sameAsCommit(f, change) {
  const first = await readOnlyPreview(f, change),
    again = await readOnlyPreview(f, change);
  assert.deepEqual(again, first);
  assert.equal(first.configVersion, change.expectedConfigVersion);
  assert.equal(first.redistributeRemaining, change.redistributeRemaining === true);
  const result = await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    (member) => f.quotas.configureGroups(meta('apply-preview', at, member.id), change, member),
    f.env,
  );
  assert.deepEqual(first.groups, result.receipt.groups);
  assert.equal(result.receipt.configVersion, first.configVersion + 1);
  for (const user of first.users)
    assert.deepEqual(user.quota, f.quotas.readMemberQuotaSummary(user.id, at), user.id);
  const saved = databaseState(f.db);
  assert.equal(
    (
      await withAuthenticatedMemberApiKey(
        f.members,
        keys.m1,
        (member) => f.quotas.configureGroups(meta('apply-preview', at, member.id), change, member),
        f.env,
      )
    ).replayed,
    true,
  );
  assert.equal(databaseState(f.db), saved);
  await assert.rejects(preview(f, change), { statusCode: 409 });
  assert.equal(databaseState(f.db), saved);
  return first;
}

test('query_only组预览零写且与真实配置后DTO一致，保持原旧组恢复/三池/暂停/未知和0', async (t) => {
  for (const [name, options, redistribute] of [
    ['preserve', { v5: '60', secondV5: '20', fixed: '4000', purchased: '100', seconds: 0 }, false],
    [
      'redistribute',
      { v5: '60', secondV5: '20', fixed: '4000', purchased: '100', seconds: 0 },
      true,
    ],
    ['old-weight-recovery', { v5: '0', secondV5: '0', officialV5: '0', seconds: 10 }, false],
    ['unknown-seconds', { seconds: null }, false],
    ['paused', { seconds: 0 }, false],
    [
      'settled-fallback',
      { v5: '0', secondV5: '0', officialV5: '0', seconds: 0, fallback: true },
      false,
    ],
    [
      'actual-status',
      { v5: '40', secondV5: '40', officialV5: '79.999999999999', seconds: 0 },
      false,
    ],
    [
      'zero',
      { v5: '0', secondV5: '0', fixed: '0', officialFixed: '0', officialV5: '0', seconds: 0 },
      true,
    ],
  ])
    await t.test(name, async (t) => {
      const f = await setup(t, options);
      if (name === 'actual-status') {
        const ticket = f.quotas.beginQuotaObservation('a', NOW + 1),
          observation = accountQuotaResult(
            'a',
            {
              tier: 3,
              subscriptionActive: true,
              subscriptionExpiresAt: null,
              fixed: '8000',
              purchased: '0',
              v5UsagePercent: 79.999999999999,
              v5UsageIsNegative: false,
              v5UsageTimeUntilNextPercent: 0,
            },
            new Date(NOW + 1).toISOString(),
          );
        f.db.transaction(() =>
          f.quotas.recordQuotaObservation(meta('original-check', NOW + 2), ticket, observation),
        )();
        const previous = JSON.parse(
          f.db
            .prepare(
              "SELECT payload FROM domain_events WHERE event_type='V5CapacityObserved' ORDER BY commit_seq DESC,event_index DESC LIMIT 1",
            )
            .get().payload,
        );
        assert.equal(previous.status, 'source_below_local');
      }
      if (name === 'settled-fallback') {
        await f.accept('actual-spend', { model: 'nai-diffusion-5-full', steps: 23 });
        f.dispatch('actual-spend-0');
        await f.publish('actual-spend-0');
      }
      if (name === 'paused') {
        const accounts = new RelationalAccountStore(f.db, f.quotas, new Map(), f.env);
        await withAuthenticatedMemberApiKey(
          f.members,
          keys.m1,
          (member) =>
            accounts.prepareAccountCommand(
              meta('pause', NOW, 'm1'),
              { kind: 'pause', accountId: 'a' },
              member,
            ),
          f.env,
        );
      }
      const result = await sameAsCommit(f, input(redistribute));
      assert.deepEqual(
        result.users.map((u) => u.id),
        ['m1', 'm2'],
      );
      if (name === 'unknown-seconds')
        assert.ok(result.users.every((u) => u.quota.refillSecondsPerPercent === null));
      if (name === 'zero')
        assert.ok(
          result.users.every(
            (u) => u.quota.v5Available === 0 && u.quota.v5FullRefillSeconds === null,
          ),
        );
      if (name === 'old-weight-recovery')
        assert.equal(result.users[0].quota.v5Available, result.users[1].quota.v5Available);
      if (name === 'settled-fallback') {
        assert.ok(result.users[0].quota.localAnlasFallbackFixedSpent > 0);
        assert.equal(result.users[1].quota.localAnlasFallbackFixedSpent, 0);
      }
      if (name === 'actual-status')
        assert.equal(result.users[0].quota.v5SharedBalanceStatus, 'source_below_local');
    });
});

test('预览先核当前owner/active/epoch和输入版本，原空闲与跨组重分规则不放宽', async (t) => {
  const f = await setup(t, { seconds: 0 }),
    member = await authenticateMemberApiKey(f.members, keys.m1),
    before = databaseState(f.db);
  for (const bad of [
    { ...input(), expectedConfigVersion: 0 },
    { ...input(), expectedConfigVersion: 1.5 },
    { ...input(), redistributeRemaining: 'true' },
    { ...input(), extra: 'ignored' },
    { ...input(), groups: [] },
    { ...input(), groups: groups({ m1: -1, m2: 1 }) },
  ]) {
    await assert.rejects(preview(f, bad), (e) => [400, 409].includes(e.statusCode));
    assert.equal(databaseState(f.db), before);
  }
  await assert.rejects(preview(f, input(), keys.m2), { statusCode: 403 });
  f.db.transaction(() =>
    assert.throws(() => f.quotas.previewGroups(actorMetadata(), input(), null), {
      statusCode: 403,
    }),
  )();
  f.env.OWNER_USER_ID = 'm2';
  f.db.transaction(() =>
    assert.throws(() => f.quotas.previewGroups(actorMetadata(), input(), member), {
      statusCode: 403,
    }),
  )();
  f.env.OWNER_USER_ID = 'm1';
  f.db.transaction(() => f.members.setMemberStatus(meta('disable'), 'm1', 'disabled'))();
  const disabled = databaseState(f.db);
  f.db.transaction(() =>
    assert.throws(() => f.quotas.previewGroups(actorMetadata(), input(), member), {
      statusCode: 403,
    }),
  )();
  assert.equal(databaseState(f.db), disabled);
  f.db.transaction(() => f.members.setMemberStatus(meta('enable'), 'm1', 'active'))();
  const enabled = databaseState(f.db);
  f.db.transaction(() =>
    assert.throws(() => f.quotas.previewGroups(actorMetadata(), input(), member), {
      statusCode: 403,
    }),
  )();
  assert.equal(databaseState(f.db), enabled);
  const split = groups();
  split[0] = { ...split[0], share: 0.5, members: ['m1'], memberWeights: { m1: 1 } };
  split[1] = { ...split[1], share: 0.5, members: ['m2'], memberWeights: { m2: 1 } };
  await assert.rejects(
    preview(f, { groups: split, expectedConfigVersion: 1, redistributeRemaining: true }),
    /必须分两次/,
  );
  assert.equal(databaseState(f.db), enabled);
  await f.accept('held');
  f.dispatch('held-0');
  const held = databaseState(f.db);
  await assert.rejects(preview(f, input()), /排队|运行|预留/);
  assert.equal(databaseState(f.db), held);
});

test('原未绑定首次预览不造账户或政策，admin null合法且不需commandId/receipt', async (t) => {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'preview');
  const env = { OWNER_USER_ID: 'm1' },
    members = new RelationalMemberStore(db),
    quotas = new RelationalQuotaStore(db, env);
  for (const id of ['m1', 'm2'])
    db.transaction(() =>
      members.registerMember(meta('register-' + id), {
        memberId: id,
        displayName: 'same',
        apiKey: keys[id],
      }),
    )();
  const f = { db, members, quotas, env };
  const first = await readOnlyPreview(f, input());
  assert.ok(first.users.every((u) => u.quota.v5Available === 0 && u.quota.boundAccountId === null));
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM sharing_policy').get().n, 0);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM upstream_accounts').get().n, 0);
  db.pragma('query_only=ON');
  const admin = db.transaction(() =>
    quotas.previewGroups(
      { actorKind: 'admin', actorId: 'local-admin', recordedAt: at },
      input(),
      null,
    ),
  )();
  db.pragma('query_only=OFF');
  assert.deepEqual(admin, first);
  await sameAsCommit(f, input());
});

test('未归组正额钱包仍占同池预算，预览/提交不吸收其余额或假造恢复room', async (t) => {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'preview');
  const env = { OWNER_USER_ID: 'm1' },
    members = new RelationalMemberStore(db),
    quotas = new RelationalQuotaStore(db, env);
  for (const id of ['m1', 'm2'])
    db.transaction(() =>
      members.registerMember(meta('register-' + id), {
        memberId: id,
        displayName: 'same',
        apiKey: keys[id],
      }),
    )();
  const events = openingFacts({ v5: '50', secondV5: '50', officialV5: '100', seconds: 600 }).filter(
      (e) => e.eventType !== 'OpeningGroupMemberRecorded' || e.payload.memberId !== 'm2',
    ),
    { policyVersion: _p, pricingVersion: _r, ...command } = meta('opening');
  db.transaction(() =>
    new RelationalCommitReplayer(db).appendCommit({
      ...command,
      expectedVersions: Object.fromEntries(events.map((e) => [e.streamId, '0'])),
      events,
      receipt: { opened: true },
    }),
  )();
  const f = { db, members, quotas, env },
    excluded = input();
  excluded.groups[0] = { ...excluded.groups[0], members: ['m1'], memberWeights: { m1: 1 } };
  const before = databaseState(db);
  await assert.rejects(readOnlyPreview(f, excluded), /不能移除仍持有额度/);
  await assert.rejects(
    withAuthenticatedMemberApiKey(
      members,
      keys.m1,
      (member) => quotas.configureGroups(meta('invalid-remove', at, 'm1'), excluded, member),
      env,
    ),
    /不能移除仍持有额度/,
  );
  assert.equal(databaseState(db), before);
  const change = input();
  change.groups[0].memberWeights = { m1: 1, m2: 0 };
  const wallet = db.prepare("SELECT * FROM wallets WHERE member_id='m2'").get(),
    result = await sameAsCommit(f, change),
    m1 = result.users.find((u) => u.id === 'm1').quota;
  assert.equal(m1.v5Available, 50);
  assert.equal(m1.v5FullRefillSeconds, null);
  assert.equal(m1.v5RefillLimited, true);
  assert.equal(result.users.find((u) => u.id === 'm2').quota.member, true);
  assert.equal(result.users.find((u) => u.id === 'm2').quota.share, 0);
  assert.deepEqual(db.prepare("SELECT * FROM wallets WHERE member_id='m2'").get(), wallet);
});
