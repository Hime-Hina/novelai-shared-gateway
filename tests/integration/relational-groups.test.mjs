import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { fixture, meta, NOW, keys, databaseState } from '../helpers/relational-generation.mjs';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalAccountStore } from '../../server/storage/relational-account-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import {
  withAuthenticatedMemberApiKey,
  authenticateMemberApiKey,
  createMemberSession,
  withAuthenticatedMemberSession,
  logoutMemberSession,
} from '../../server/members/authentication.ts';
import { parseRelationalFact } from '../../server/events/relational-fact-contracts.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { ownerAccountCommand } from '../../server/members/owner-operations.ts';
import { parseAmount, formatAmount } from '../../server/quota/resource-amounts.ts';

const groups = (weights = { m1: 1, m2: 1 }) => [
  { id: 'g', name: 'shared', share: 1, members: ['m1', 'm2'], memberWeights: weights },
  ...['g2', 'g3', 'g4'].map((id) => ({ id, name: id, share: 0, members: [], memberWeights: {} })),
];
async function setup(t, options = {}) {
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { ...options, env }),
    quotas = new RelationalQuotaStore(f.db, env);
  const configure = (
    id,
    configured = groups(),
    redistributeRemaining = false,
    expectedConfigVersion = 1,
  ) =>
    withAuthenticatedMemberApiKey(
      f.members,
      keys.m1,
      (member) =>
        quotas.configureGroups(
          meta(id, NOW + 10, member.id),
          { groups: configured, redistributeRemaining, expectedConfigVersion },
          member,
        ),
      env,
    );
  return { ...f, env, quotas, configure };
}
function replay(t, source, inputs, until = Number.MAX_SAFE_INTEGER) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const owner = new RelationalCommitReplayer(db);
  for (const { seq } of source
    .prepare(
      'SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE commit_seq<=? ORDER BY commit_seq',
    )
    .all(until)) {
    db.transaction(() => owner.replayCommit(readRelationalCommit(source, seq), inputs))();
  }
  return { db, owner };
}
const wallets = (db) => db.prepare('SELECT * FROM wallets ORDER BY account_id,member_id').all();

function resequence(stored, db) {
  const versions = new Map();
  stored.command.expectedVersions = {};
  stored.events = stored.events.map((event, eventIndex) => {
    if (!versions.has(event.streamId)) {
      const version =
        db
          .prepare('SELECT CAST(stream_version AS TEXT) AS v FROM stream_heads WHERE stream_id=?')
          .get(event.streamId)?.v || '0';
      versions.set(event.streamId, BigInt(version));
      stored.command.expectedVersions[event.streamId] = version;
    }
    versions.set(event.streamId, versions.get(event.streamId) + 1n);
    return { ...event, eventIndex, streamVersion: String(versions.get(event.streamId)) };
  });
  stored.eventCount = stored.events.length;
  stored.command.events = stored.events.map((event) =>
    Object.fromEntries(
      [
        'eventType',
        'payload',
        'streamId',
        'occurredAt',
        'accountId',
        'memberId',
        'policyVersion',
        'pricingVersion',
      ].map((key) => [key, event[key]]),
    ),
  );
  return stored;
}

test('真实在线首次配置显式保存原政策，随后账户首绑和官方观察开账，不借opening初始化', async (t) => {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const env = { OWNER_USER_ID: 'm1' },
    members = new RelationalMemberStore(db, env),
    quotas = new RelationalQuotaStore(db, env),
    accounts = new RelationalAccountStore(db, quotas, new Map(), env);
  for (const id of ['m1', 'm2']) {
    db.transaction(() =>
      members.registerMember(meta(`register-${id}`), {
        memberId: id,
        displayName: 'same display name',
        apiKey: keys[id],
      }),
    )();
  }
  const authorize = (write) => withAuthenticatedMemberApiKey(members, keys.m1, write, env),
    configured = await authorize((member) =>
      quotas.configureGroups(
        meta('initial-groups', NOW, 'm1'),
        { groups: groups(), expectedConfigVersion: 1 },
        member,
      ),
    );
  assert.equal(configured.events.length, 1);
  assert.equal(configured.events[0].eventType, 'SharingConfigured');
  assert.equal(configured.receipt.configVersion, 2);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM wallets').get().n, 0);
  assert.deepEqual(
    db
      .prepare(
        'SELECT auto_anlas_fallback,free_policy_version,free_enabled,loss_policy_version,loss_v5_24h,loss_fixed_24h FROM sharing_policy',
      )
      .get(),
    {
      auto_anlas_fallback: 0,
      free_policy_version: 1,
      free_enabled: 0,
      loss_policy_version: 1,
      loss_v5_24h: 1,
      loss_fixed_24h: '100',
    },
  );
  await authorize((member) =>
    quotas.configureGroups(
      meta('before-bind', NOW + 1, 'm1'),
      { groups: groups({ m1: 3, m2: 1 }), expectedConfigVersion: 2 },
      member,
    ),
  );
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM wallets').get().n, 0);
  await ownerAccountCommand(accounts, authorize, meta('register-account', NOW + 2, 'm1'), {
    kind: 'register',
    accountId: 'a',
    displayName: 'shared',
    token: 'pst-synthetic',
    configVersion: 3,
  });
  t.mock.method(Date, 'now', () => NOW + 3);
  await ownerAccountCommand(
    accounts,
    authorize,
    meta('enable', NOW + 3, 'm1'),
    { kind: 'enable', accountId: 'a' },
    async (_pst, _proxy, _id, start) => {
      await start?.();
      return {
        tier: 3,
        subscriptionActive: true,
        subscriptionExpiresAt: null,
        points: 8000,
        fixed: '8000',
        purchased: '0',
        v5UsagePercent: '80',
        v5UsageTimeUntilNextPercent: 0,
      };
    },
  );
  assert.deepEqual(
    wallets(db).map((wallet) => wallet.v5_available),
    ['60', '20'],
  );
  assert.equal(
    db
      .prepare(
        "SELECT COUNT(*) AS n FROM domain_events WHERE event_type LIKE 'Opening%' OR event_type='ImportSourceRegistered'",
      )
      .get().n,
    0,
  );
  const r = replay(t, db);
  assert.deepEqual(wallets(r.db), wallets(db));
  assert.equal(r.db.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
  assert.throws(() => assertRelationalStorageReady(r.db), /incomplete/);
  const restored = databaseState(r.db);
  r.db.transaction(() => r.owner.replayCommit(readRelationalCommit(db, configured.commitSeq)))();
  assert.equal(databaseState(r.db), restored);
});

test('已有组配置保留钱包、绑定与独立政策，显式重分三资源只消费一次原组总额', async (t) => {
  const f = await setup(t, {
      v5: '60',
      secondV5: '20',
      fixed: '4000',
      purchased: '100',
      seconds: 0,
    }),
    before = wallets(f.db);
  const configured = await f.configure('groups', groups({ m1: 3, m2: 1 }));
  assert.deepEqual(wallets(f.db), before);
  assert.equal(
    configured.events.find((event) => event.eventType === 'SharingConfigured').payload.groups
      .length,
    4,
  );
  const changed = await f.configure('redistribute', groups(), true, 2);
  const redistributed = changed.events.filter(
    (event) => event.eventType === 'GroupBalancesRedistributed',
  );
  assert.equal(redistributed.length, 1);
  assert.equal(
    redistributed[0].payload.configurationEventId,
    changed.events.find((event) => event.eventType === 'SharingConfigured').eventId,
  );
  for (const resource of ['v5_available', 'anlas_fixed_available', 'anlas_purchased_available']) {
    assert.equal(
      wallets(f.db).reduce((sum, wallet) => sum + parseAmount(wallet[resource]), 0n),
      before.reduce((sum, wallet) => sum + parseAmount(wallet[resource]), 0n),
    );
  }
  assert.deepEqual(
    wallets(f.db).map((wallet) => wallet.v5_available),
    ['40', '40'],
  );
  const once = databaseState(f.db);
  assert.equal((await f.configure('redistribute', groups(), true, 2)).replayed, true);
  assert.equal(databaseState(f.db), once);
  const r = replay(t, f.db, f.inputs);
  assert.deepEqual(wallets(r.db), wallets(f.db));
  assert.deepEqual(
    r.db.prepare('SELECT * FROM sharing_policy').all(),
    f.db.prepare('SELECT * FROM sharing_policy').all(),
  );
});

test('按旧组先恢复再改权重，不能用新权重重写原恢复决定', async (t) => {
  const f = await setup(t, { v5: '0', secondV5: '0', officialV5: '0', seconds: 10 }),
    changed = await f.configure('recover-before-groups', groups({ m1: 3, m2: 1 })),
    refill = changed.events.find((event) => event.eventType === 'RefillApplied'),
    configuration = changed.events.find((event) => event.eventType === 'SharingConfigured');
  assert.ok(refill);
  assert.ok(refill.eventIndex < configuration.eventIndex);
  assert.equal(refill.payload.configVersion, 1);
  assert.equal(refill.payload.allocations[0].amount, refill.payload.allocations[1].amount);
  assert.ok(parseAmount(refill.payload.allocations[0].amount) > 0n);
  assert.equal(wallets(f.db)[0].v5_available, wallets(f.db)[1].v5_available);
  const r = replay(t, f.db, f.inputs);
  assert.deepEqual(wallets(r.db), wallets(f.db));
});

test('原显示名fallback与有限数值权重完整保留，回放不重新规范化原展示决定', async (t) => {
  for (const [name, id, weight] of [
    ['space', ' group ', 1e-15],
    ['long', 'x'.repeat(120), 0.1234567890123456],
    ['minimum', 'g', Number.MIN_VALUE],
  ]) {
    await t.test(name, async (t) => {
      const f = await setup(t, { seconds: 0 }),
        configured = groups({ m1: weight, m2: 1 });
      configured[0].id = id;
      delete configured[0].name;
      await f.configure('names', configured);
      assert.deepEqual(f.db.prepare('SELECT name FROM sharing_groups WHERE id=?').get(id), {
        name: id,
      });
      assert.equal(
        f.db.prepare("SELECT weight FROM group_members WHERE member_id='m1'").get().weight,
        weight,
      );
      const r = replay(t, f.db, f.inputs);
      assert.deepEqual(
        r.db.prepare('SELECT * FROM sharing_groups ORDER BY id').all(),
        f.db.prepare('SELECT * FROM sharing_groups ORDER BY id').all(),
      );
      assert.equal(
        r.db.prepare("SELECT weight FROM group_members WHERE member_id='m1'").get().weight,
        weight,
      );
    });
  }
});

test('停用成员只可保留原位置，有余额跨组与显式重分沿原owner分开执行', async (t) => {
  const f = await setup(t, { seconds: 0 }),
    split = groups();
  split[0] = { ...split[0], share: 0.5, members: ['m1'], memberWeights: { m1: 1 } };
  split[1] = { ...split[1], share: 0.5, members: ['m2'], memberWeights: { m2: 1 } };
  const before = databaseState(f.db);
  await assert.rejects(f.configure('move-and-redistribute', split, true), /必须分两次/);
  assert.equal(databaseState(f.db), before);
  const members = new RelationalMemberStore(f.db);
  f.db.transaction(() => members.setMemberStatus(meta('disable-m2'), 'm2', 'disabled'))();
  await f.configure('keep-disabled', groups({ m1: 2, m2: 1 }));
  const disabled = databaseState(f.db);
  await assert.rejects(f.configure('move-disabled', split, false, 2), /停用成员只能保留/);
  assert.equal(databaseState(f.db), disabled);
  assert.equal(members.readMember('m2').status, 'disabled');
});

test('当前 owner、Key、Cookie和认证epoch在旧回执之前重核，原回执不回滚后来政策', async (t) => {
  const f = await setup(t, { seconds: 0 });
  await f.configure('original', groups());
  await f.configure('later', groups({ m1: 3, m2: 1 }), false, 2);
  const once = databaseState(f.db);
  assert.equal((await f.configure('original', groups())).replayed, true);
  assert.equal(databaseState(f.db), once);
  await assert.rejects(f.configure('original', groups({ m1: 2, m2: 1 })), /conflict|fingerprint/i);
  const actor = await authenticateMemberApiKey(f.members, keys.m1),
    session = await createMemberSession(f.members, actor, {}, NOW + 10);
  const cookieResult = await withAuthenticatedMemberSession(
    f.members,
    session.token,
    {},
    (member) =>
      f.quotas.configureGroups(
        meta('cookie', NOW + 10, 'm1'),
        { groups: groups(), expectedConfigVersion: 3 },
        member,
      ),
    NOW + 11,
  );
  assert.equal(cookieResult.receipt.configVersion, 4);
  await logoutMemberSession(f.members, session.token);
  assert.equal(
    await withAuthenticatedMemberSession(
      f.members,
      session.token,
      {},
      () => assert.fail('revoked cookie'),
      NOW + 12,
    ),
    null,
  );
  f.db.prepare("UPDATE members SET auth_epoch='revoked-owner-epoch' WHERE id='m1'").run();
  assert.throws(
    () =>
      f.db.transaction(() =>
        f.quotas.configureGroups(
          meta('original', NOW + 10, 'm1'),
          { groups: groups(), expectedConfigVersion: 1, redistributeRemaining: false },
          actor,
        ),
      )(),
    /当前 owner/,
  );
  f.env.OWNER_USER_ID = 'm2';
  await assert.rejects(f.configure('original', groups()), /当前 owner/);
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'replaced-key'))();
  assert.equal(await f.configure('original', groups()), null);
});

test('全局等待和预扣拒绝调整，分组末代不能因number精度重用', async (t) => {
  const f = await setup(t, { seconds: 0 });
  await f.accept('waiting');
  const waiting = databaseState(f.db);
  await assert.rejects(f.configure('busy'), /排队|空闲/);
  assert.equal(databaseState(f.db), waiting);
  f.dispatch('waiting-0');
  const running = databaseState(f.db);
  await assert.rejects(f.configure('running'), /排队|预留|空闲/);
  assert.equal(databaseState(f.db), running);
  await f.fail('waiting-0', { aborted: false });
  f.db.prepare('UPDATE sharing_policy SET config_version=?').run(Number.MAX_SAFE_INTEGER);
  const boundary = databaseState(f.db);
  await assert.rejects(
    f.configure('overflow', groups(), false, Number.MAX_SAFE_INTEGER),
    /安全边界/,
  );
  assert.equal(databaseState(f.db), boundary);
});

test('独立政策非默认值完整保留，新增零钱包成员的零预算组也记录显式重分', async (t) => {
  const f = await setup(t, { seconds: 0 }),
    manage = (id, input) =>
      withAuthenticatedMemberApiKey(
        f.members,
        keys.m1,
        (member) => f.quotas.configureQuotaPolicy(meta(id, NOW + 1, 'm1'), input, member),
        f.env,
      );
  await manage('fallback', { action: 'fallback-policy', enabled: true });
  await manage('free', {
    action: 'free-image-policy',
    expectedVersion: 1,
    policy: { enabled: true, warningThreshold: 7, hardLimit: 9 },
  });
  await manage('loss', {
    action: 'loss-alert-policy',
    expectedVersion: 1,
    policy: {
      v5Points24h: Number.MIN_VALUE,
      fixedAnlas24h: 0,
      fixedAnlas30d: 17,
      memberAttempts24h: 2,
    },
  });
  f.db.transaction(() =>
    f.members.registerMember(meta('register-m3'), {
      memberId: 'm3',
      displayName: 'same display name',
      apiKey: 'synthetic-m3-key',
    }),
  )();
  const added = groups();
  added[0].members.push('m3');
  added[0].memberWeights.m3 = 1;
  await f.configure('add', added);
  assert.equal(
    f.db.prepare("SELECT v5_available FROM wallets WHERE member_id='m3'").get().v5_available,
    '0',
  );
  const split = groups();
  split[0].share = 0.75;
  split[1] = {
    id: 'g2',
    name: 'zero-budget-group',
    share: 0.25,
    members: ['m3'],
    memberWeights: { m3: 1 },
  };
  const changed = await f.configure('split-zero', split, true, 2),
    budgets = changed.events.filter((event) => event.eventType === 'GroupBalancesRedistributed');
  assert.equal(budgets.length, 2);
  assert.deepEqual(
    budgets.find((event) => event.payload.groupId === 'g2').payload.sourceResources,
    { v5: '0', anlasFixed: '0', anlasPurchased: '0' },
  );
  const zeroWeights = structuredClone(split);
  zeroWeights[0].share = 0;
  zeroWeights[0].memberWeights = { m1: 0, m2: 0 };
  zeroWeights[1].share = 1;
  const kept = await f.configure('zero-weight-group', zeroWeights, true, 3);
  assert.deepEqual(
    wallets(f.db).map((wallet) => wallet.v5_available),
    ['40', '40', '0'],
  );
  assert.deepEqual(
    kept.events
      .filter((event) => event.eventType === 'GroupBalancesRedistributed')
      .map((event) => event.payload.groupId),
    ['g2'],
  );
  assert.deepEqual(
    f.db
      .prepare(
        'SELECT auto_anlas_fallback,free_policy_version,free_hard_limit,loss_policy_version,loss_v5_24h,loss_fixed_30d FROM sharing_policy',
      )
      .get(),
    {
      auto_anlas_fallback: 1,
      free_policy_version: 2,
      free_hard_limit: 9,
      loss_policy_version: 2,
      loss_v5_24h: Number.MIN_VALUE,
      loss_fixed_30d: '17',
    },
  );
  const r = replay(t, f.db, f.inputs);
  assert.deepEqual(wallets(r.db), wallets(f.db));
});

test('新组命令不能由metadata自证，非owner不能重领，本地admin保留原事务入口', async (t) => {
  const f = await setup(t, { seconds: 0 }),
    input = { groups: groups(), expectedConfigVersion: 1 },
    actor = await authenticateMemberApiKey(f.members, keys.m1);
  assert.throws(
    () => f.quotas.configureGroups(meta('outside', NOW + 10, 'm1'), input, actor),
    /transaction/,
  );
  for (const bad of [
    { ...input, extra: true },
    { ...input, redistributeRemaining: 1 },
    { ...input, expectedConfigVersion: 0 },
  ]) {
    const before = databaseState(f.db);
    await assert.rejects(
      withAuthenticatedMemberApiKey(
        f.members,
        keys.m1,
        (member) => f.quotas.configureGroups(meta('invalid', NOW + 10, 'm1'), bad, member),
        f.env,
      ),
    );
    assert.equal(databaseState(f.db), before);
  }
  await f.configure('owner', groups());
  await assert.rejects(
    withAuthenticatedMemberApiKey(
      f.members,
      keys.m2,
      (member) =>
        f.quotas.configureGroups(
          meta('owner', NOW + 10, 'm2'),
          { ...input, redistributeRemaining: false },
          member,
        ),
      f.env,
    ),
    /当前 owner/,
  );
  assert.throws(
    () => f.db.transaction(() => f.quotas.configureGroups(meta('system'), input, null))(),
    /当前 owner/,
  );
  const result = f.db.transaction(() =>
    f.quotas.configureGroups(
      { ...meta('local', NOW + 10), actorKind: 'admin', actorId: 'local-admin' },
      { ...input, expectedConfigVersion: 2 },
      null,
    ),
  )();
  assert.equal(result.receipt.configVersion, 3);
});

test('第二钱包写入失败时配置、第一钱包、流头和回执整笔回滚，即使外层捕获', async (t) => {
  const f = await setup(t, { v5: '60', secondV5: '20', seconds: 0 }),
    actor = await authenticateMemberApiKey(f.members, keys.m1),
    before = databaseState(f.db),
    prepare = f.db.prepare;
  let writes = 0;
  f.db.prepare = function (sql) {
    const statement = prepare.call(this, sql);
    if (
      sql.startsWith(
        'UPDATE wallets SET v5_available=?,anlas_fixed_available=?,anlas_purchased_available=?,source_event_id=? WHERE account_id=? AND member_id=?',
      )
    ) {
      const run = statement.run.bind(statement);
      statement.run = (...args) => {
        if (++writes === 2) {
          assert.equal(
            prepare.call(f.db, 'SELECT config_version FROM sharing_policy').get().config_version,
            2,
          );
          assert.equal(
            prepare.call(f.db, "SELECT v5_available FROM wallets WHERE member_id='m1'").get()
              .v5_available,
            '40',
          );
          throw new Error('synthetic second group wallet');
        }
        return run(...args);
      };
    }
    return statement;
  };
  try {
    f.db.transaction(() =>
      assert.throws(
        () =>
          f.quotas.configureGroups(
            meta('rollback', NOW + 10, 'm1'),
            { groups: groups(), expectedConfigVersion: 1, redistributeRemaining: true },
            actor,
          ),
        /synthetic second/,
      ),
    )();
  } finally {
    f.db.prepare = prepare;
  }
  assert.equal(writes, 2);
  assert.equal(databaseState(f.db), before);
});

test('大额三池仍以原12位金额重分，不能经number丢失Anlas或尾数', async (t) => {
  const fixed = '9007199254740993.000000000003',
    purchased = '9007199254740993.000000000001',
    fixedTotal = formatAmount(parseAmount(fixed) * 2n),
    purchasedTotal = formatAmount(parseAmount(purchased) * 2n),
    f = await setup(t, {
      seconds: 0,
      fixed,
      purchased,
      officialFixed: fixedTotal,
      officialPurchased: purchasedTotal,
    });
  const changed = await f.configure('large-money', groups({ m1: 3, m2: 1 }), true),
    budget = changed.events.find((event) => event.eventType === 'GroupBalancesRedistributed');
  assert.equal(budget.payload.sourceResources.anlasFixed, fixedTotal);
  assert.equal(budget.payload.sourceResources.anlasPurchased, purchasedTotal);
  assert.equal(
    formatAmount(
      wallets(f.db).reduce((sum, wallet) => sum + parseAmount(wallet.anlas_fixed_available), 0n),
    ),
    fixedTotal,
  );
  assert.equal(
    formatAmount(
      wallets(f.db).reduce(
        (sum, wallet) => sum + parseAmount(wallet.anlas_purchased_available),
        0n,
      ),
    ),
    purchasedTotal,
  );
  const r = replay(t, f.db, f.inputs);
  assert.deepEqual(wallets(r.db), wallets(f.db));
});

test('真实受理派发与Local收尾后的分组保留原confirmation、结算和待核对责任', async (t) => {
  const f = await setup(t, { seconds: 0 });
  await f.accept('completed', { member: true });
  f.dispatch('completed-0');
  await f.publish('completed-0');
  const charges = f.db.prepare('SELECT * FROM charges ORDER BY original_job_id').all(),
    requests = f.db.prepare('SELECT id,confirmation FROM generation_requests ORDER BY id').all(),
    pending = (db) =>
      db
        .prepare(
          'SELECT pending_v5_debit,pending_fixed_debit,pending_purchased_debit,pending_fallback_v5,pending_fallback_fixed,pending_fallback_purchased FROM account_quota_state',
        )
        .get(),
    responsibilities = pending(f.db);
  await f.configure('after-settlement', groups({ m1: 3, m2: 1 }));
  assert.deepEqual(f.db.prepare('SELECT * FROM charges ORDER BY original_job_id').all(), charges);
  assert.deepEqual(
    f.db.prepare('SELECT id,confirmation FROM generation_requests ORDER BY id').all(),
    requests,
  );
  assert.deepEqual(pending(f.db), responsibilities);
  const r = replay(t, f.db, f.inputs);
  assert.deepEqual(pending(r.db), responsibilities);
  assert.deepEqual(r.db.prepare('SELECT * FROM charges ORDER BY original_job_id').all(), charges);
  assert.deepEqual(
    r.db.prepare('SELECT * FROM artifact_replicas ORDER BY artifact_id,storage_target_id').all(),
    f.db.prepare('SELECT * FROM artifact_replicas ORDER BY artifact_id,storage_target_id').all(),
  );
});

test('未用组占位事实已退出，导入OpeningGroup事实保留', () => {
  for (const eventType of ['GroupConfigured', 'GroupMemberAssigned', 'GroupMemberRemoved']) {
    assert.throws(() => parseRelationalFact({ eventType, payload: {} }), /Unsupported/);
  }
});

test('完整回放核对同次历史权重和原舍入，守恒不能授权零权重或低权重独吞', async (t) => {
  for (const weights of [
    { m1: 1, m2: 0 },
    { m1: 3, m2: 1 },
  ]) {
    await t.test(JSON.stringify(weights), async (t) => {
      const f = await setup(t, {
          v5: '60',
          secondV5: '20',
          fixed: '4000',
          purchased: '100',
          seconds: 0,
        }),
        changed = await f.configure('weights', groups(weights), true),
        stored = readRelationalCommit(f.db, changed.commitSeq);
      assert.deepEqual(
        Object.fromEntries(
          stored.events
            .find((event) => event.eventType === 'GroupBalancesRedistributed')
            .payload.allocations.map((allocation) => [allocation.memberId, allocation.resources]),
        ),
        weights.m2 === 0
          ? {
              m1: { v5: '80', anlasFixed: '8000', anlasPurchased: '200' },
              m2: { v5: '0', anlasFixed: '0', anlasPurchased: '0' },
            }
          : {
              m1: { v5: '60', anlasFixed: '6000', anlasPurchased: '150' },
              m2: { v5: '20', anlasFixed: '2000', anlasPurchased: '50' },
            },
      );
      for (const resource of ['v5', 'anlasFixed', 'anlasPurchased']) {
        await t.test(resource, (t) => {
          const r = replay(t, f.db, f.inputs, Number(changed.commitSeq) - 1),
            bad = structuredClone(stored),
            before = databaseState(r.db),
            budget = bad.events.find((event) => event.eventType === 'GroupBalancesRedistributed');
          for (const allocation of budget.payload.allocations) {
            allocation.resources[resource] =
              allocation.memberId === 'm2' ? budget.payload.sourceResources[resource] : '0';
          }
          resequence(bad, r.db);
          r.db.transaction(() =>
            assert.throws(() => r.owner.replayCommit(bad, f.inputs), /权重及舍入/),
          )();
          assert.equal(databaseState(r.db), before);
        });
      }
    });
  }
  await t.test('one-atomic-unit-rounding', async (t) => {
    const f = await setup(t, {
        v5: '0.000000000001',
        secondV5: '0',
        fixed: '0',
        officialFixed: '0',
        officialV5: '0.000000000001',
        seconds: 0,
      }),
      desired = groups();
    desired[0].members.reverse();
    const changed = await f.configure('rounding', desired, true),
      r = replay(t, f.db, f.inputs, Number(changed.commitSeq) - 1),
      bad = structuredClone(readRelationalCommit(f.db, changed.commitSeq)),
      before = databaseState(r.db),
      budget = bad.events.find((event) => event.eventType === 'GroupBalancesRedistributed'),
      winner = budget.payload.allocations.find(
        (allocation) => parseAmount(allocation.resources.v5) > 0n,
      ).memberId;
    assert.equal(winner, 'm1');
    for (const allocation of budget.payload.allocations) {
      allocation.resources.v5 = allocation.memberId === winner ? '0' : '0.000000000001';
    }
    resequence(bad, r.db);
    r.db.transaction(() =>
      assert.throws(() => r.owner.replayCommit(bad, f.inputs), /权重及舍入/),
    )();
    assert.equal(databaseState(r.db), before);
  });
});

test('分组与预算完整提交拒绝伪造来源、偷分、漏分和分叉回执，外层捕获后全表不变', async (t) => {
  const f = await setup(t, { v5: '60', secondV5: '20', seconds: 0 });
  const first = await f.configure('first', groups(), true),
    changed = await f.configure('redistribution', groups(), true, 2),
    stored = readRelationalCommit(f.db, changed.commitSeq),
    config = (s) => s.events.find((event) => event.eventType === 'SharingConfigured'),
    budget = (s) => s.events.find((event) => event.eventType === 'GroupBalancesRedistributed');
  const cases = {
    missingBudget: (s) => {
      s.events = s.events.filter((event) => event.eventType !== 'GroupBalancesRedistributed');
    },
    missingConfiguration: (s) => {
      s.events = s.events.filter((event) => event.eventType !== 'SharingConfigured');
    },
    duplicateBudget: (s) => {
      s.events.push({ ...structuredClone(budget(s)), eventId: 'duplicate-budget' });
    },
    falseFlag: (s) => {
      config(s).payload.redistributeRemaining = false;
    },
    previousConfiguration: (s) => {
      budget(s).payload.configurationEventId = first.events.find(
        (event) => event.eventType === 'SharingConfigured',
      ).eventId;
    },
    consumedPreviousBudget: (s) => {
      const original = budget(s);
      original.payload.configurationEventId = first.events.find(
        (event) => event.eventType === 'SharingConfigured',
      ).eventId;
      original.payload.configVersion = 2;
      s.events = [
        {
          ...structuredClone(original),
          eventId: 'ordinary-before-budget',
          eventType: 'QuotaStatusChanged',
          payload: { accountId: 'a', status: 'active' },
        },
        original,
      ];
      s.command.receipt = { attemptedReuse: true };
    },
    wrongGroup: (s) => {
      budget(s).payload.groupId = 'g2';
    },
    wrongVersion: (s) => {
      budget(s).payload.configVersion += 1;
    },
    wrongTime: (s) => {
      budget(s).occurredAt += 1;
    },
    wrongAccountPayload: (s) => {
      budget(s).payload.accountId = 'another-account';
    },
    wrongMemberReference: (s) => {
      budget(s).memberId = 'm1';
    },
    excessBudget: (s) => {
      const p = budget(s).payload;
      p.sourceResources.v5 = formatAmount(parseAmount(p.sourceResources.v5) + 1n);
      p.allocations[0].resources.v5 = formatAmount(parseAmount(p.allocations[0].resources.v5) + 1n);
    },
    resourceSwap: (s) => {
      const p = budget(s).payload;
      [p.sourceResources.v5, p.sourceResources.anlasFixed] = [
        p.sourceResources.anlasFixed,
        p.sourceResources.v5,
      ];
      for (const a of p.allocations) {
        [a.resources.v5, a.resources.anlasFixed] = [a.resources.anlasFixed, a.resources.v5];
      }
    },
    missingMember: (s) => {
      budget(s).payload.allocations.pop();
    },
    duplicateMember: (s) => {
      budget(s).payload.allocations.push(structuredClone(budget(s).payload.allocations[0]));
    },
    unknownMember: (s) => {
      budget(s).payload.allocations[0].memberId = 'unrelated-member';
    },
    shareSum: (s) => {
      config(s).payload.groups[0].share = 0.9;
    },
    emptyGroupShare: (s) => {
      config(s).payload.groups[1].share = 0.1;
    },
    excessiveShare: (s) => {
      config(s).payload.groups[0].share = 1.01;
    },
    excessiveWeight: (s) => {
      config(s).payload.groups[0].members[0].weight = 1000001;
    },
    negativeWeight: (s) => {
      config(s).payload.groups[0].members[0].weight = -1;
    },
    duplicateMembership: (s) => {
      config(s).payload.groups[1].members.push({ memberId: 'm1', weight: 1 });
    },
    missingGroup: (s) => {
      config(s).payload.groups.pop();
    },
    independentPolicy: (s) => {
      config(s).payload.freeHardLimit = 9;
    },
    configurationVersion: (s) => {
      config(s).payload.configVersion += 1;
    },
    order: (s) => {
      s.events.reverse();
    },
    receiptVersion: (s) => {
      s.command.receipt.configVersion += 1;
    },
    receiptGroups: (s) => {
      s.command.receipt.groups[0].members.pop();
    },
    receiptStatus: (s) => {
      s.command.receipt.status = 'banned';
    },
    ordinaryFactWithGroupReceipt: (s) => {
      const e = budget(s);
      s.events = [
        { ...e, eventType: 'QuotaStatusChanged', payload: { accountId: 'a', status: 'active' } },
      ];
    },
  };
  for (const [name, mutate] of Object.entries(cases)) {
    await t.test(name, (t) => {
      const r = replay(t, f.db, f.inputs, Number(changed.commitSeq) - 1),
        bad = structuredClone(stored),
        before = databaseState(r.db);
      mutate(bad);
      resequence(bad, r.db);
      r.db.transaction(() => assert.throws(() => r.owner.replayCommit(bad, f.inputs)))();
      assert.equal(databaseState(r.db), before);
    });
  }
});
