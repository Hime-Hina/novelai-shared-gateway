import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { fixture, meta, NOW, keys, databaseState } from '../helpers/relational-generation.mjs';
import {
  RelationalQuotaStore,
  relationalQuotaFacts,
} from '../../server/storage/relational-quota-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import {
  withAuthenticatedMemberApiKey,
  withAuthenticatedMemberSession,
  createMemberSession,
  logoutMemberSession,
} from '../../server/members/authentication.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { configureAnlasFallback } from '../../server/quota/fair-share.ts';
import { configureFreeImages } from '../../server/quota/free-image-policy.ts';
import { updateLossAlertPolicy } from '../../server/quota/failure-accounting.ts';

const free = (expectedVersion = 1, patch = {}) => ({
  action: 'free-image-policy',
  expectedVersion,
  policy: { enabled: true, warningThreshold: null, hardLimit: 5, ...patch },
});
const loss = (expectedVersion = 1, patch = {}) => ({
  action: 'loss-alert-policy',
  expectedVersion,
  policy: {
    v5Points24h: 0.1234567890123456,
    fixedAnlas24h: 0,
    fixedAnlas30d: 9,
    memberAttempts24h: 0,
    ...patch,
  },
});
const fallback = (enabled) => ({ action: 'fallback-policy', enabled });
async function setup(t, options = {}) {
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { ...options, env }),
    quotas = new RelationalQuotaStore(f.db, env);
  const manage = (id, input, memberId = 'm1') =>
    withAuthenticatedMemberApiKey(
      f.members,
      keys[memberId],
      (member) => quotas.configureQuotaPolicy(meta(id, NOW + 10, memberId), input, member),
      env,
    );
  return { ...f, env, quotas, manage };
}
const policyRow = (db) => db.prepare('SELECT * FROM sharing_policy WHERE id=1').get();

test('三个政策命令复用原 owner 的结果与版本，fallback 不增加组版本或改写钱包', async (t) => {
  const f = await setup(t),
    before = policyRow(f.db),
    financial = f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all();
  const reference = {
    settings: {
      fairQuota: {
        enabled: true,
        autoAnlasFallback: false,
        freeImagePolicy: { version: 1, enabled: false, warningThreshold: null, hardLimit: null },
        lossAlertPolicy: {
          version: 1,
          v5Points24h: 1,
          fixedAnlas24h: 100,
          fixedAnlas30d: 500,
          memberAttempts24h: 3,
        },
      },
    },
    ledger: [],
  };
  assert.deepEqual(
    (await f.manage('fallback', fallback(true))).receipt,
    configureAnlasFallback(reference, true, 'm1', NOW + 10),
  );
  assert.deepEqual((await f.manage('free', free())).receipt, {
    policy: configureFreeImages(reference, free(), 'm1', NOW + 10),
  });
  assert.deepEqual((await f.manage('loss', loss())).receipt, {
    policy: updateLossAlertPolicy(reference, loss(), 'm1', NOW + 10),
  });
  const row = policyRow(f.db);
  assert.equal(row.config_version, before.config_version);
  assert.equal(row.free_policy_version, 2);
  assert.equal(row.loss_policy_version, 2);
  assert.equal(row.auto_anlas_fallback, 1);
  assert.equal(row.loss_v5_24h, 0.1234567890123456);
  assert.equal(
    f.db.prepare('SELECT typeof(loss_v5_24h) AS type FROM sharing_policy').get().type,
    'real',
  );
  assert.deepEqual(f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(), financial);
  const facts = f.db
    .prepare(
      "SELECT event_type,stream_id,account_id,member_id FROM domain_events WHERE event_type IN ('FallbackPolicyChanged','FreePolicyChanged','LossPolicyChanged') ORDER BY commit_seq",
    )
    .all();
  assert.equal(facts.length, 3);
  assert.ok(
    facts.every(
      (fact) =>
        fact.stream_id === 'sharing:default' && fact.account_id === null && fact.member_id === null,
    ),
  );
});

test('原回执在当前授权后重领零写入，不重新增加政策版本或复活旧 fallback', async (t) => {
  const f = await setup(t);
  const first = await f.manage('fallback-A', fallback(true));
  await f.manage('fallback-B', fallback(false));
  const freeResult = await f.manage('free', free()),
    lossResult = await f.manage('loss', loss());
  const before = f.db.serialize();
  for (const [id, input, result] of [
    ['fallback-A', fallback(true), first],
    ['free', free(), freeResult],
    ['loss', loss(), lossResult],
  ]) {
    const replayed = await f.manage(id, input);
    assert.equal(replayed.replayed, true);
    assert.deepEqual(replayed.receipt, result.receipt);
    assert.ok(f.db.serialize().equals(before));
  }
  assert.equal(policyRow(f.db).auto_anlas_fallback, 0);
  for (const [id, input] of [
    ['fallback-A', fallback(false)],
    ['free', free(1, { hardLimit: 4 })],
    ['loss', loss(1, { fixedAnlas30d: 10 })],
  ]) {
    await assert.rejects(f.manage(id, input), /idempotency conflict/);
    assert.ok(f.db.serialize().equals(before));
  }
  const reordered = {
    policy: { hardLimit: 5, warningThreshold: null, enabled: true },
    expectedVersion: 1,
    action: 'free-image-policy',
  };
  assert.equal((await f.manage('free', reordered)).replayed, true);
  assert.ok(f.db.serialize().equals(before));
  await f.manage('same-value-new-key', fallback(false));
  assert.equal(
    f.db
      .prepare("SELECT COUNT(*) AS n FROM domain_events WHERE event_type='FallbackPolicyChanged'")
      .get().n,
    3,
  );
  assert.equal(policyRow(f.db).config_version, 1);
});

test('政策保留各自版本、NULL 和原 owner 的输入边界，拒绝不留下事实或回执', async (t) => {
  const f = await setup(t);
  await f.manage('valid-free', free());
  await f.manage('valid-loss', loss());
  for (const [name, input] of [
    ['unknown-action', { action: 'other' }],
    ['fallback-string', fallback('true')],
    ['free-stale', free(1)],
    ['free-version-type', free('2')],
    ['free-zero-warning', free(2, { warningThreshold: 0 })],
    ['free-negative-limit', free(2, { hardLimit: -1 })],
    ['free-fractional', free(2, { hardLimit: 0.5 })],
    ['free-extra', free(2, { extra: true })],
    ['loss-stale', loss(1)],
    ['loss-negative', loss(2, { v5Points24h: -1 })],
    ['loss-NaN', loss(2, { v5Points24h: NaN })],
    ['loss-infinity', loss(2, { v5Points24h: Infinity })],
    ['loss-fractional-fixed', loss(2, { fixedAnlas24h: 1.5 })],
    ['loss-unsafe-fixed', loss(2, { fixedAnlas30d: Number.MAX_SAFE_INTEGER + 1 })],
    ['loss-fractional-attempts', loss(2, { memberAttempts24h: 0.5 })],
  ]) {
    await t.test(name, async () => {
      const before = f.db.serialize();
      await assert.rejects(f.manage(name, input));
      assert.ok(f.db.serialize().equals(before));
    });
  }
  const disabled = await f.manage(
    'free-disabled',
    free(2, { enabled: false, warningThreshold: null, hardLimit: null }),
  );
  assert.deepEqual(disabled.receipt.policy, {
    version: 3,
    enabled: false,
    warningThreshold: null,
    hardLimit: null,
  });
});

test('V5 告警阈值保留任意有限非负 number，原金额与安全整数规则保持独立', async (t) => {
  const f = await setup(t);
  for (const [index, threshold] of [1e-15, 0.1234567890123456, 0, Number.MAX_VALUE].entries()) {
    const result = await f.manage(
      `threshold-${index}`,
      loss(index + 1, {
        v5Points24h: threshold,
        fixedAnlas24h: Number.MAX_SAFE_INTEGER,
        memberAttempts24h: Number.MAX_SAFE_INTEGER,
      }),
    );
    assert.equal(result.receipt.policy.v5Points24h, threshold);
    assert.equal(policyRow(f.db).loss_v5_24h, threshold);
    assert.equal(policyRow(f.db).loss_fixed_24h, String(Number.MAX_SAFE_INTEGER));
    assert.equal(policyRow(f.db).loss_member_attempts_24h, Number.MAX_SAFE_INTEGER);
  }
  for (const threshold of [-1, Infinity, NaN]) {
    assert.throws(
      () => f.db.prepare('UPDATE sharing_policy SET loss_v5_24h=?').run(threshold),
      /constraint|NOT NULL/i,
    );
  }
});

test('合成 opening 中的原 V5 告警数值保真恢复，重复不改历史或私有凭据', async (t) => {
  const f = await setup(t);
  for (const threshold of [Number.MIN_VALUE, 1e-15, 0.1234567890123456, 0, Number.MAX_VALUE]) {
    await t.test(String(threshold), () => {
      const target = new Database(':memory:');
      try {
        initializeRelationalStorage(target, 'generation-history');
        const members = new RelationalMemberStore(target),
          generations = new RelationalGenerationStore(target, {});
        for (const seq of ['1', '2']) {
          target.transaction(() => members.replayMemberCommit(readRelationalCommit(f.db, seq)))();
        }
        const opening = structuredClone(readRelationalCommit(f.db, '3'));
        for (const events of [opening.events, opening.command.events]) {
          events.find((event) => event.eventType === 'OpeningPolicyRecorded').payload.lossV5_24h =
            threshold;
        }
        target.transaction(() => generations.replayGenerationCommit(opening))();
        assert.equal(policyRow(target).loss_v5_24h, threshold);
        const checkpoint = target.serialize();
        target.transaction(() =>
          assert.equal(generations.replayGenerationCommit(opening).replayed, true),
        )();
        assert.ok(target.serialize().equals(checkpoint));
        assert.equal(target.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
        assert.throws(() => assertRelationalStorageReady(target), /incomplete/);
      } finally {
        target.close();
      }
    });
  }
});

test('当前 owner、Key、Cookie 和认证代次在回执前重核，元数据不能自证管理身份', async (t) => {
  for (const mode of ['owner', 'key', 'cookie', 'epoch']) {
    await t.test(mode, async (t) => {
      const f = await setup(t),
        input = free();
      await f.manage('original', input);
      const member = await withAuthenticatedMemberApiKey(f.members, keys.m1, (user) => user, f.env),
        session = await createMemberSession(f.members, member, f.env, NOW);
      if (mode === 'owner') {
        f.env.OWNER_USER_ID = 'm2';
      } else if (mode === 'key') {
        f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'synthetic-policy-new-key'))();
      } else if (mode === 'cookie') {
        await logoutMemberSession(f.members, session.token);
      } else {
        f.db.transaction(() =>
          f.members.setMemberStatus(meta('disable-owner'), 'm1', 'disabled'),
        )();
      }
      const before = databaseState(f.db);
      if (mode === 'key') {
        assert.equal(await f.manage('original', input), null);
      } else if (mode === 'cookie') {
        assert.equal(
          await withAuthenticatedMemberSession(
            f.members,
            session.token,
            f.env,
            (user) => f.quotas.configureQuotaPolicy(meta('original', NOW + 10, 'm1'), input, user),
            NOW,
          ),
          null,
        );
      } else if (mode === 'epoch') {
        f.db.transaction(() =>
          assert.throws(
            () => f.quotas.configureQuotaPolicy(meta('original', NOW + 10, 'm1'), input, member),
            /授权/,
          ),
        )();
      } else {
        await assert.rejects(f.manage('original', input), /owner/);
      }
      assert.equal(databaseState(f.db), before);
    });
  }
  const f = await setup(t);
  await assert.rejects(f.manage('non-owner', fallback(true), 'm2'), /owner/);
  assert.throws(
    () => f.quotas.configureQuotaPolicy(meta('outside', NOW, 'm1'), fallback(true), null),
    /owning/,
  );
  f.db.transaction(() =>
    assert.throws(
      () => f.quotas.configureQuotaPolicy(meta('forged', NOW, 'm1'), fallback(true), null),
      /授权/,
    ),
  )();
});

test('SQL 故障被外层捕获后政策、事实、回执、流头与游标全部回滚，原键可正常重试', async (t) => {
  for (const input of [fallback(true), free(), loss()]) {
    await t.test(input.action, async (t) => {
      const f = await setup(t);
      f.db.exec(
        "CREATE TEMP TRIGGER fail_policy BEFORE UPDATE ON sharing_policy BEGIN SELECT RAISE(ABORT,'policy fault'); END",
      );
      const before = databaseState(f.db);
      await withAuthenticatedMemberApiKey(
        f.members,
        keys.m1,
        (member) => {
          assert.throws(
            () => f.quotas.configureQuotaPolicy(meta('atomic', NOW + 10, 'm1'), input, member),
            /policy fault/,
          );
          assert.equal(databaseState(f.db), before);
        },
        f.env,
      );
      assert.equal(databaseState(f.db), before);
      f.db.exec('DROP TRIGGER fail_policy');
      assert.equal((await f.manage('atomic', input)).replayed, false);
      const after = f.db.serialize();
      assert.equal((await f.manage('atomic', input)).replayed, true);
      assert.ok(f.db.serialize().equals(after));
    });
  }
});

test('政策变化不改原确认或免费成功，当前 warning/损耗设置不使既有授权失效', async (t) => {
  const f = await setup(t, { freeLimit: 5 });
  await f.accept('prior-confirmation', { member: true, steps: 23 });
  const before = f.db.prepare('SELECT confirmation FROM generation_requests').get().confirmation;
  await f.manage('warning-only', free(1, { warningThreshold: 1 }));
  await f.manage('loss-only', loss());
  assert.equal(
    f.db.prepare('SELECT confirmation FROM generation_requests').get().confirmation,
    before,
  );
  assert.ok(f.dispatch('prior-confirmation-0').dispatch);
  assert.equal(f.generations.readCharge('prior-confirmation-0').billingMode, 'free');
  await f.publish('prior-confirmation-0');
  const successful = f.db
    .prepare('SELECT free_succeeded_at,free_warning_threshold FROM generation_jobs')
    .get();
  await f.manage('disable-free', free(2, { enabled: false }));
  assert.deepEqual(
    f.db.prepare('SELECT free_succeeded_at,free_warning_threshold FROM generation_jobs').get(),
    successful,
  );
  assert.equal(
    f.db.prepare('SELECT confirmation FROM generation_requests').get().confirmation,
    before,
  );
});

test('改变实际计价政策后派发仍复查原确认，拒绝不改输入、原确认或产生预扣', async (t) => {
  for (const input of [fallback(true), free(1, { hardLimit: 1 })]) {
    await t.test(input.action, async (t) => {
      const f = await setup(t, { freeLimit: 5 });
      await f.accept('waiting', { member: true, steps: 23 });
      const before = f.db
        .prepare('SELECT confirmation FROM generation_requests')
        .get().confirmation;
      await f.manage('new-policy', input);
      assert.throws(() => f.dispatch('waiting-0'), /生成规则已变化/);
      assert.equal(
        f.db.prepare('SELECT confirmation FROM generation_requests').get().confirmation,
        before,
      );
      assert.equal(f.generations.readCharge('waiting-0'), null);
    });
  }
});

test('三政策混合真实生成、退款历史从空库完整提交重建，重复零新增且不恢复私有认证', async (t) => {
  const f = await setup(t);
  await f.manage('fallback-enable', fallback(true));
  await f.manage('free-update', free());
  await f.manage('loss-precise', loss(1, { v5Points24h: 1e-15 }));
  await f.accept('generation');
  f.dispatch('generation-0');
  f.fail('generation-0');
  await f.manage('loss-zero', loss(2, { v5Points24h: 0 }));
  const target = new Database(':memory:');
  try {
    initializeRelationalStorage(target, 'generation-history');
    const members = new RelationalMemberStore(target),
      quotas = new RelationalQuotaStore(target, {}),
      inputs = new RelationalInputStore(target),
      generations = new RelationalGenerationStore(target, {});
    const commits = f.db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS n FROM commits ORDER BY commit_seq')
      .all()
      .map(({ n }) => readRelationalCommit(f.db, n));
    const apply = (stored) =>
      target.transaction(() => {
        if (stored.events[0].eventType === 'MemberRegistered') {
          return members.replayMemberCommit(stored);
        }
        if (stored.events.every((event) => relationalQuotaFacts.has(event.eventType))) {
          return quotas.replayQuotaCommit(stored);
        }
        if (stored.events.some((event) => event.eventType === 'RequestAccepted')) {
          return inputs.replayAcceptedInput(f.inputs, stored.events[0].commitSeq);
        }
        return generations.replayGenerationCommit(stored);
      })();
    for (const stored of commits) {
      apply(stored);
      const checkpoint = target.serialize();
      assert.equal(apply(stored).replayed, true);
      assert.ok(target.serialize().equals(checkpoint));
    }
    for (const table of [
      'sharing_policy',
      'commits',
      'domain_events',
      'stream_heads',
      'projection_state',
      'account_quota_state',
      'operating_state',
      'wallets',
      'charges',
      'generation_requests',
      'generation_jobs',
      'generation_attempts',
    ]) {
      assert.deepEqual(
        target.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
        f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
        table,
      );
    }
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
    assert.equal(
      target.prepare('SELECT COUNT(*) AS n FROM members WHERE auth_epoch IS NOT NULL').get().n,
      0,
    );
    assert.throws(() => assertRelationalStorageReady(target), /incomplete/);
  } finally {
    target.close();
  }
});

test('回放拒绝伪造政策来源流、错误版本和非法阈值，外层捕获仍保持原切点', async (t) => {
  for (const [name, input, mutate] of [
    [
      'fallback-version',
      fallback(true),
      (event) => {
        event.payload.configVersion++;
      },
    ],
    [
      'free-version',
      free(),
      (event) => {
        event.payload.version++;
      },
    ],
    [
      'loss-version',
      loss(),
      (event) => {
        event.payload.version++;
      },
    ],
    [
      'loss-precision',
      loss(),
      (event) => {
        event.payload.v5_24h = '0.1234567890123456';
      },
    ],
    [
      'loss-negative',
      loss(),
      (event) => {
        event.payload.v5_24h = -1;
      },
    ],
    [
      'loss-fractional-fixed',
      loss(),
      (event) => {
        event.payload.fixed24h = '1.5';
      },
    ],
    [
      'loss-unsafe-fixed',
      loss(),
      (event) => {
        event.payload.fixed24h = '9007199254740992';
      },
    ],
    [
      'free-zero-limit',
      free(),
      (event) => {
        event.payload.hardLimit = 0;
      },
    ],
  ]) {
    await t.test(name, async (t) => {
      const f = await setup(t),
        before = f.db.serialize(),
        result = await f.manage('original', input),
        target = new Database(before);
      try {
        target.pragma('foreign_keys=ON');
        const stored = readRelationalCommit(f.db, result.commitSeq),
          bad = structuredClone(stored),
          quotas = new RelationalQuotaStore(target, {});
        for (const events of [bad.events, bad.command.events]) {
          mutate(events[0]);
        }
        const checkpoint = databaseState(target);
        target.transaction(() => assert.throws(() => quotas.replayQuotaCommit(bad)))();
        assert.equal(databaseState(target), checkpoint);
        target.transaction(() => quotas.replayQuotaCommit(stored))();
        assert.equal(databaseState(target), databaseState(f.db));
      } finally {
        target.close();
      }
    });
  }
  const f = await setup(t),
    before = f.db.serialize(),
    result = await f.manage('original', free()),
    target = new Database(before);
  try {
    target.pragma('foreign_keys=ON');
    const bad = structuredClone(readRelationalCommit(f.db, result.commitSeq)),
      quotas = new RelationalQuotaStore(target, {});
    bad.command.expectedVersions = { 'sharing:other': '0' };
    bad.events[0].streamVersion = '1';
    for (const events of [bad.events, bad.command.events]) {
      events[0].streamId = 'sharing:other';
    }
    const checkpoint = databaseState(target);
    target.transaction(() => assert.throws(() => quotas.replayQuotaCommit(bad), /当前共享流/))();
    assert.equal(databaseState(target), checkpoint);
  } finally {
    target.close();
  }
});
