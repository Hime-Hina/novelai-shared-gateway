import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import {
  NOW,
  keys,
  meta,
  openingFacts,
  fixture,
  databaseState,
} from '../helpers/relational-generation.mjs';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import {
  RelationalQuotaStore,
  relationalQuotaFacts,
} from '../../server/storage/relational-quota-store.ts';
import { RelationalAccountStore } from '../../server/storage/relational-account-store.ts';
import {
  withAuthenticatedMemberApiKey,
  withAuthenticatedMemberSession,
  createMemberSession,
  logoutMemberSession,
} from '../../server/members/authentication.ts';
import { ownerAccountCommand } from '../../server/members/owner-operations.ts';
import { accountQuotaResult, accountQuotaErrorResult } from '../../server/quota/account-refresh.ts';
import {
  appendRelationalCommit,
  readRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import { domainDigest } from '../../server/events/domain-events.ts';

const rawQuota = (patch = {}) => ({
  tier: 3,
  subscriptionActive: true,
  subscriptionExpiresAt: null,
  points: 8000,
  fixed: '8000',
  purchased: '0',
  v5UsagePercent: '80',
  v5UsageTimeUntilNextPercent: 0,
  ...patch,
});
const registration = {
  kind: 'register',
  accountId: 'a',
  displayName: 'shared',
  token: 'pst-synthetic-A',
  configVersion: 1,
};
const mutation = (kind, patch = {}) => ({ kind, accountId: 'a', ...patch });
function account(db) {
  return db
    .prepare(
      'SELECT id,enabled,credential_epoch,cooldown_until,source_event_id FROM upstream_accounts WHERE id=?',
    )
    .get('a');
}
function commandFixture(
  t,
  db,
  members,
  env = { OWNER_USER_ID: 'm1' },
  quotas = new RelationalQuotaStore(db, env),
) {
  let clock = NOW;
  t.mock.method(Date, 'now', () => clock);
  const controls = new Map(),
    accounts = new RelationalAccountStore(db, quotas, controls, env);
  let queries = 0,
    onQuery = async () => {},
    response = rawQuota();
  const authorize = (write) =>
    withAuthenticatedMemberApiKey(members, keys.m1, (member) => write(member), env);
  const run = (id, input, overrides = {}) =>
    ownerAccountCommand(
      accounts,
      overrides.authorize ?? authorize,
      { ...meta(id, Date.now(), 'm1'), ...overrides.metadata },
      input,
      async (pst, _proxy, _id, start) => {
        await start?.();
        queries++;
        assert.equal(db.inTransaction, false);
        assert.equal(pst, db.prepare('SELECT pst FROM upstream_credentials').get().pst);
        await onQuery();
        return response;
      },
    );
  const prepare = async (id, input = mutation('enable')) => {
    const metadata = meta(id, Date.now(), 'm1'),
      prepared = await authorize((member) =>
        accounts.prepareAccountCommand(metadata, input, member),
      );
    // 此窄 owner 夹具明确模拟实际发起；单独准备不得推进观察顺序。
    return 'prepared' in prepared
      ? authorize((member) =>
          accounts.startAccountEnable(metadata, input, prepared.prepared, member),
        )
      : prepared;
  };
  const complete = (
    id,
    ticket,
    input = mutation('enable'),
    result = accountQuotaResult('a', rawQuota(), new Date(ticket.startedAt).toISOString()),
  ) =>
    authorize((member) =>
      accounts.completeAccountEnable(meta(id, Date.now(), 'm1'), input, ticket, result, member),
    );
  return {
    db,
    members,
    env,
    controls,
    accounts,
    quotas,
    run,
    prepare,
    complete,
    authorize,
    now: (value) => {
      clock = value;
    },
    queries: () => queries,
    query: (fn) => {
      onQuery = fn;
    },
    response: (value) => {
      response = value;
    },
  };
}
async function fresh(t) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'account-history');
  const members = new RelationalMemberStore(db);
  for (const id of ['m1', 'm2']) {
    db.transaction(() =>
      members.registerMember(meta(`member-${id}`), {
        memberId: id,
        displayName: id,
        apiKey: keys[id],
      }),
    )();
  }
  const source = new Database(db.serialize());
  try {
    source.pragma('foreign_keys=ON');
    const events = openingFacts().filter((event) =>
      [
        'ImportSourceRegistered',
        'OpeningGroupRecorded',
        'OpeningGroupMemberRecorded',
        'OpeningPolicyRecorded',
      ].includes(event.eventType),
    );
    events.find((e) => e.eventType === 'OpeningPolicyRecorded').payload.boundAccountId = null;
    const { policyVersion: _policy, pricingVersion: _price, ...metadata } = meta('unbound-policy');
    source.transaction(() =>
      appendRelationalCommit(source, {
        ...metadata,
        expectedVersions: Object.fromEntries(events.map((event) => [event.streamId, '0'])),
        events,
        receipt: { opened: true },
      }),
    )();
    db.transaction(() =>
      new RelationalGenerationStore(db, {}).replayGenerationCommit(
        readRelationalCommit(source, '3'),
      ),
    )();
  } finally {
    source.close();
  }
  return commandFixture(t, db, members);
}

test('账户真实首绑保持暂停与未知额度，核验一次分配并保存可回放的观察身份', async (t) => {
  const f = await fresh(t);
  const registered = await f.run('register-account', registration);
  assert.deepEqual(registered.receipt, { accountId: 'a', enabled: false });
  assert.equal(account(f.db).credential_epoch, 1);
  assert.equal(account(f.db).enabled, 0);
  assert.equal(f.quotas.readAccountQuota('a').observedAt, null);
  assert.deepEqual(
    f.db.prepare('SELECT anlas_fixed_available AS v FROM wallets ORDER BY member_id').all(),
    [{ v: '0' }, { v: '0' }],
  );
  assert.equal(f.queries(), 0);
  const enabled = await f.run('enable', mutation('enable'));
  assert.deepEqual(enabled.receipt, { accountId: 'a', enabled: true });
  assert.equal(account(f.db).enabled, 1);
  const stored = readRelationalCommit(f.db, enabled.commitSeq),
    observed = stored.events.find((e) => e.eventType === 'QuotaObserved');
  assert.equal(
    stored.events.find((e) => e.eventType === 'AvailabilityChanged').payload.observationEventId,
    observed.eventId,
  );
  assert.deepEqual(
    f.db.prepare('SELECT anlas_fixed_available AS v FROM wallets ORDER BY member_id').all(),
    [{ v: '4000' }, { v: '4000' }],
  );
  assert.equal(f.queries(), 1);
  const snapshot = f.db.serialize();
  assert.equal((await f.run('register-account', registration)).replayed, true);
  assert.equal((await f.run('enable', mutation('enable'))).replayed, true);
  assert.ok(f.db.serialize().equals(snapshot));
  assert.equal(f.queries(), 1);
  await assert.rejects(
    f.run('register-account', { ...registration, token: 'pst-different' }),
    /idempotency conflict/,
  );
  await assert.rejects(f.run('second-account', { ...registration, accountId: 'b' }), /首次注册/);
  assert.throws(() => assertRelationalStorageReady(f.db), /incomplete/);
});

test('账户命令拒绝未知操作和额外字段，授权与拥有者事务不能由元数据自证', async (t) => {
  const f = await fresh(t);
  for (const input of [
    null,
    { ...registration, token: 'invalid' },
    { ...registration, configVersion: 0 },
    mutation('unknown'),
    { ...mutation('pause'), ignored: true },
  ]) {
    const before = databaseState(f.db);
    await assert.rejects(f.run('invalid-input', input));
    assert.equal(databaseState(f.db), before);
  }
  assert.throws(
    () => f.accounts.prepareAccountCommand(meta('no-transaction', NOW, 'm1'), registration, null),
    /owning authenticated/,
  );
  f.db.transaction(() =>
    assert.throws(
      () => f.accounts.prepareAccountCommand(meta('forged', NOW, 'm1'), registration, null),
      /授权/,
    ),
  )();
  await assert.rejects(
    f.run('system-actor', registration, { metadata: { actorKind: 'system', actorId: null } }),
    /授权/,
  );
  await f.run('register', registration);
  await assert.rejects(f.run('other-account', { kind: 'pause', accountId: 'b' }), {
    statusCode: 404,
  });
});

test('暂停与轮换保留权益、代次单调，重领原启用回执不会重新启用或查询', async (t) => {
  const f = await fresh(t);
  await f.run('register', registration);
  await f.run('enable', mutation('enable'));
  const wallet = f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all();
  await f.run('pause', mutation('pause'));
  assert.equal(account(f.db).enabled, 0);
  await f.run('enable', mutation('enable'));
  assert.equal(account(f.db).enabled, 0);
  assert.equal(f.queries(), 1);
  for (const [id, token, epoch] of [
    ['rotate-B', 'pst-synthetic-B', 2],
    ['rotate-A', registration.token, 3],
    ['same-A', registration.token, 3],
  ]) {
    const result = await f.run(id, mutation('token', { token }));
    assert.equal(account(f.db).credential_epoch, epoch);
    assert.equal(account(f.db).enabled, 0);
    assert.ok(!JSON.stringify(result).includes(token));
    const snapshot = f.db.serialize();
    assert.equal((await f.run(id, mutation('token', { token }))).replayed, true);
    assert.ok(f.db.serialize().equals(snapshot));
  }
  assert.deepEqual(f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(), wallet);
  await f.run('enable-current', mutation('enable'));
  assert.equal(account(f.db).enabled, 1);
  assert.equal(f.quotas.readAccountQuota('a').observedCredentialEpoch, 3);
});

test('当前授权在原回执和网络等待后重核，旧 Key、Cookie、epoch、owner 均不能重领', async (t) => {
  for (const mode of ['key', 'cookie', 'epoch', 'owner', 'non-owner']) {
    await t.test(mode, async (t) => {
      const f = await fresh(t);
      await f.run('register', registration);
      const user = await withAuthenticatedMemberApiKey(
        f.members,
        keys.m1,
        (member) => member,
        f.env,
      );
      const session = await createMemberSession(f.members, user, f.env, NOW);
      let authorize = f.authorize;
      if (mode === 'cookie') {
        authorize = (write) =>
          withAuthenticatedMemberSession(
            f.members,
            session.token,
            f.env,
            (member) => write(member),
            NOW,
          );
      } else if (mode === 'non-owner') {
        authorize = (write) =>
          withAuthenticatedMemberApiKey(f.members, keys.m2, (member) => write(member), f.env);
      }
      if (mode === 'key') {
        f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'synthetic-new-member-key'))();
      } else if (mode === 'cookie') {
        await logoutMemberSession(f.members, session.token);
      } else if (mode === 'epoch') {
        f.db.transaction(() => f.members.setMemberStatus(meta('disable'), 'm1', 'disabled'))();
      } else if (mode === 'owner') {
        f.env.OWNER_USER_ID = 'm2';
      }
      const before = databaseState(f.db);
      await assert.rejects(f.run('register', registration, { authorize }), /会话|owner|账户|登录/);
      assert.equal(databaseState(f.db), before);
      assert.equal(f.queries(), 0);
    });
  }
  const f = await fresh(t);
  await f.run('register', registration);
  f.query(async () => {
    f.env.OWNER_USER_ID = 'm2';
  });
  await assert.rejects(f.run('enable', mutation('enable')), /owner/);
  assert.equal(account(f.db).enabled, 0);
  assert.equal(f.quotas.readAccountQuota('a').observationEventId, null);
});

test('暂停、凭据 A→B→A、配置变化或较新观察使等待中的核验失效', async (t) => {
  for (const mode of ['pause', 'rotate', 'new-ticket', 'new-observation', 'policy']) {
    await t.test(mode, async (t) => {
      const f = await fresh(t);
      await f.run('register', registration);
      f.query(async () => {
        if (mode === 'pause') {
          await f.run('pause-during-query', mutation('pause'));
        } else if (mode === 'rotate') {
          await f.run('rotate-B', mutation('token', { token: 'pst-synthetic-B' }));
          await f.run('rotate-A', mutation('token', { token: registration.token }));
        } else if (mode === 'policy') {
          // 模拟后续政策 owner 已完成的独立版本变化，不据此批准当前查询。
          f.db.prepare('UPDATE sharing_policy SET config_version=config_version+1').run();
        } else {
          const ticket = f.quotas.beginQuotaObservation('a', NOW);
          if (mode === 'new-observation') {
            f.db.transaction(() =>
              f.quotas.recordQuotaObservation(
                meta('new-observation'),
                ticket,
                accountQuotaResult('a', rawQuota(), new Date(NOW).toISOString()),
              ),
            )();
          }
        }
      });
      await assert.rejects(f.run('waiting-enable', mutation('enable')), /已变化|过期/);
      assert.equal(account(f.db).enabled, 0);
      assert.equal(
        f.db.prepare("SELECT COUNT(*) AS n FROM commits WHERE command_id='waiting-enable'").get().n,
        0,
      );
    });
  }
});

test('无效 Opus 与查询失败保留观察和失败回执，重领不查上游或发放权益', async (t) => {
  for (const patch of [
    { tier: 2 },
    { subscriptionActive: false },
    { subscriptionExpiresAt: new Date(NOW - 1).toISOString() },
    null,
  ]) {
    await t.test(JSON.stringify(patch), async (t) => {
      const f = await fresh(t);
      await f.run('register', registration);
      if (patch) {
        f.response(rawQuota(patch));
      } else {
        f.query(async () => {
          throw new Error('synthetic upstream failure');
        });
      }
      await assert.rejects(f.run('enable', mutation('enable')), { statusCode: 409 });
      assert.equal(account(f.db).enabled, 0);
      assert.notEqual(f.quotas.readAccountQuota('a').observationEventId, null);
      assert.deepEqual(f.db.prepare('SELECT anlas_fixed_available AS n FROM wallets').all(), [
        { n: '0' },
        { n: '0' },
      ]);
      const receipt = JSON.parse(
        f.db.prepare("SELECT receipt FROM commits WHERE command_id='enable'").get().receipt,
      );
      assert.equal(receipt.error.statusCode, 409);
      const before = databaseState(f.db);
      await assert.rejects(f.run('enable', mutation('enable')), { statusCode: 409 });
      assert.equal(databaseState(f.db), before);
      assert.equal(f.queries(), 1);
    });
  }
});

test('已启用账户重新核验遇到封禁后先提交失败事实再报错，原回执和回放均阻止派发', async (t) => {
  const original = await fixture(t, { env: { OWNER_USER_ID: 'm1' } }),
    f = commandFixture(t, original.db, original.members);
  await original.accept('banned');
  const before = f.db.serialize();
  f.query(async () => {
    throw new Error('account banned');
  });
  await assert.rejects(f.run('verify-banned', mutation('enable')), { statusCode: 409 });
  assert.equal(account(f.db).enabled, 0);
  assert.equal(f.quotas.readAccountQuota('a').quotaErrorCode, 'sync_error');
  assert.throws(() => original.dispatch('banned-0'), /基线不可用/);
  assert.equal(original.generations.readCharge('banned-0'), null);
  const snapshot = databaseState(f.db);
  await assert.rejects(f.run('verify-banned', mutation('enable')), /封禁/);
  assert.equal(databaseState(f.db), snapshot);
  assert.equal(f.queries(), 1);
  const seq = f.db
      .prepare(
        "SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE command_id='verify-banned'",
      )
      .get().seq,
    stored = readRelationalCommit(f.db, seq),
    target = new Database(before);
  try {
    target.pragma('foreign_keys=ON');
    const quotas = new RelationalQuotaStore(target, {}),
      accounts = new RelationalAccountStore(target, quotas, new Map(), {});
    target.transaction(() => accounts.replayAccountCommit(stored))();
    assert.equal(databaseState(target), databaseState(f.db));
    assert.throws(
      () =>
        target.transaction(() =>
          new RelationalGenerationStore(target, {}).dispatchGeneration(
            meta('blocked', NOW + 10),
            { jobId: 'banned-0', accountId: 'a', attemptId: 'blocked-attempt' },
            original.controls,
          ),
        )(),
      /基线不可用/,
    );
    const checkpoint = target.serialize();
    target.transaction(() => assert.equal(accounts.replayAccountCommit(stored).replayed, true))();
    assert.ok(target.serialize().equals(checkpoint));
  } finally {
    target.close();
  }
});

test('核验等待期间正常生成收尾不改变可用性代次，成功观察和封禁观察都能提交', async (t) => {
  for (const mode of ['valid', 'banned']) {
    await t.test(mode, async (t) => {
      const original = await fixture(t, { env: { OWNER_USER_ID: 'm1' } }),
        f = commandFixture(t, original.db, original.members);
      await original.accept('finishing');
      original.dispatch('finishing-0');
      const previousSource = account(f.db).source_event_id;
      f.now(NOW + 2);
      f.query(async () => {
        await original.publish('finishing-0', NOW + 5);
        assert.notEqual(account(f.db).source_event_id, previousSource);
        f.now(NOW + 10);
        if (mode === 'banned') {
          throw new Error('account banned');
        }
      });
      if (mode === 'banned') {
        await assert.rejects(f.run('verify-finishing', mutation('enable')), /封禁/);
        assert.equal(account(f.db).enabled, 0);
        assert.equal(f.quotas.readAccountQuota('a').quotaErrorCode, 'sync_error');
      } else {
        assert.equal((await f.run('verify-finishing', mutation('enable'))).receipt.enabled, true);
      }
      assert.equal(original.generations.readCharge('finishing-0').status, 'settled');
      assert.equal(
        f.db.prepare("SELECT COUNT(*) AS n FROM commits WHERE command_id='verify-finishing'").get()
          .n,
        1,
      );
      assert.equal(f.queries(), 1);
    });
  }
});

test('非法期限核验的失败资格在后续恢复仍阻止真实派发，合法空值和数字字符串 tier 保留', async (t) => {
  const original = await fixture(t, { env: { OWNER_USER_ID: 'm1' } }),
    f = commandFixture(t, original.db, original.members);
  await original.accept('invalid-expiry');
  f.response(rawQuota({ subscriptionExpiresAt: 'malformed-expiration' }));
  await assert.rejects(f.run('invalid-expiry', mutation('enable')), { statusCode: 409 });
  f.db.transaction(() => f.quotas.refillQuota(meta('no-expiry-revival', NOW + 20), 'a'))();
  assert.equal(f.quotas.readAccountQuota('a').quotaErrorCode, 'sync_error');
  assert.equal(f.quotas.readAccountQuota('a').status, 'quota_sync_error');
  assert.throws(() => original.dispatch('invalid-expiry-0', NOW + 30), /基线不可用/);
  for (const [index, expires] of [
    undefined,
    null,
    0,
    '',
    new Date(NOW + 100000).toISOString(),
  ].entries()) {
    f.now(NOW + 40 + index);
    f.response(rawQuota({ subscriptionExpiresAt: expires, tier: '3' }));
    assert.equal((await f.run(`valid-expiry-${index}`, mutation('enable'))).receipt.enabled, true);
    assert.equal(f.quotas.readAccountQuota('a').quotaTier, 3);
  }
  f.now(NOW + 50);
  f.response(rawQuota({ subscriptionExpiresAt: '0' }));
  await assert.rejects(f.run('expired-string-zero', mutation('enable')), { statusCode: 409 });
  f.db.transaction(() => f.quotas.refillQuota(meta('string-zero-refill', NOW + 51), 'a'))();
  assert.equal(f.quotas.readAccountQuota('a').status, 'inactive_opus');
});

test('超过观察有效期的网络结果和等待期间撤销的 Cookie 均不能启用', async (t) => {
  for (const mode of ['expired-query', 'revoked-cookie']) {
    await t.test(mode, async (t) => {
      const f = await fresh(t);
      await f.run('register', registration);
      const user = await withAuthenticatedMemberApiKey(
          f.members,
          keys.m1,
          (member) => member,
          f.env,
        ),
        session = await createMemberSession(f.members, user, f.env, NOW);
      const authorize = (write) =>
        withAuthenticatedMemberSession(
          f.members,
          session.token,
          f.env,
          (member) => write(member),
          Date.now(),
        );
      f.query(async () => {
        if (mode === 'expired-query') {
          f.now(NOW + 600001);
        } else {
          await logoutMemberSession(f.members, session.token);
        }
      });
      await assert.rejects(f.run('late-enable', mutation('enable'), { authorize }), /Opus|会话/);
      assert.equal(account(f.db).enabled, 0);
      assert.equal(
        f.quotas.readAccountQuota('a').observationEventId === null,
        mode === 'revoked-cookie',
      );
      assert.equal(
        f.db.prepare("SELECT COUNT(*) AS n FROM commits WHERE command_id='late-enable'").get().n,
        mode === 'revoked-cookie' ? 0 : 1,
      );
    });
  }
});

test('轮换同时检查真实物理槽和预扣，逻辑退款不提前释放占用', async (t) => {
  for (const mode of ['physical-only', 'reservation-only', 'refunded-physical']) {
    await t.test(mode, async (t) => {
      const original = await fixture(t, { env: { OWNER_USER_ID: 'm1' } }),
        f = commandFixture(t, original.db, original.members);
      if (mode !== 'physical-only') {
        await original.accept('active');
        original.dispatch('active-0');
        if (mode === 'refunded-physical') {
          original.fail('active-0', { aborted: false });
          assert.equal(original.generations.readCharge('active-0').status, 'refunded');
        }
      }
      if (mode !== 'reservation-only') {
        f.controls.set('physical-active', { accountId: 'a' });
      }
      const before = databaseState(f.db);
      await assert.rejects(
        f.run('rotate', mutation('token', { token: 'pst-replacement' })),
        /在途/,
      );
      assert.equal(databaseState(f.db), before);
      await f.run('pause-running', mutation('pause'));
      assert.equal(account(f.db).enabled, 0);
      assert.equal(f.controls.size, mode === 'reservation-only' ? 0 : 1);
    });
  }
});

test('私有凭据、后续成员钱包或启用事实故障在外层捕获后仍全表回滚', async (t) => {
  for (const mode of ['register-secret', 'rotate-secret', 'enable-wallet', 'enable-availability']) {
    await t.test(mode, async (t) => {
      const f = await fresh(t);
      if (mode !== 'register-secret') {
        await f.run('register', registration);
      }
      let input = mutation('enable');
      let trigger =
        "CREATE TEMP TRIGGER fail_account BEFORE UPDATE ON upstream_accounts WHEN NEW.enabled=1 BEGIN SELECT RAISE(ABORT,'availability fault'); END";
      if (mode === 'register-secret') {
        input = registration;
      } else if (mode === 'rotate-secret') {
        input = mutation('token', { token: 'pst-replacement' });
      }
      if (mode.endsWith('secret')) {
        trigger =
          "CREATE TEMP TRIGGER fail_account BEFORE INSERT ON upstream_credentials BEGIN SELECT RAISE(ABORT,'secret fault'); END";
      } else if (mode === 'enable-wallet') {
        trigger =
          "CREATE TEMP TRIGGER fail_account BEFORE UPDATE ON wallets WHEN NEW.member_id='m2' BEGIN SELECT RAISE(ABORT,'wallet fault'); END";
      }
      const ticket = input.kind === 'enable' ? (await f.prepare('atomic', input)).ticket : null;
      f.db.exec(trigger);
      const before = databaseState(f.db);
      await f.authorize((member) => {
        assert.throws(
          () =>
            input.kind === 'enable'
              ? f.accounts.completeAccountEnable(
                  meta('atomic', NOW, 'm1'),
                  input,
                  ticket,
                  accountQuotaResult('a', rawQuota(), new Date(NOW).toISOString()),
                  member,
                )
              : f.accounts.prepareAccountCommand(meta('atomic', NOW, 'm1'), input, member),
          /fault/,
        );
        assert.equal(databaseState(f.db), before);
      });
      assert.equal(databaseState(f.db), before);
      f.db.exec('DROP TRIGGER fail_account');
      if (ticket) {
        await f.complete('atomic', ticket);
      } else {
        await f.run('atomic', input);
      }
    });
  }
});

function businessState(db) {
  const excluded = new Set([
    'password_credentials',
    'api_keys',
    'discord_bindings',
    'discord_invites',
    'member_sessions',
    'member_auth_state',
    'upstream_credentials',
  ]);
  return domainDigest(
    db
      .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
      .all()
      .filter(({ name }) => !excluded.has(name))
      .map(({ name }) => [
        name,
        db
          .prepare(`SELECT * FROM "${name}" ORDER BY rowid`)
          .safeIntegers()
          .all()
          .map((row) =>
            Object.fromEntries(
              Object.entries(row)
                .filter(([key]) => name !== 'members' || key !== 'auth_epoch')
                .map(([key, value]) => [key, typeof value === 'bigint' ? String(value) : value]),
            ),
          ),
      ]),
  );
}

test('原完整提交从空库重建账户、观察和权益，保留原回执且不恢复私有认证', async (t) => {
  const f = await fresh(t);
  await f.run('register', registration);
  await f.run('enable', mutation('enable'));
  await f.run('pause', mutation('pause'));
  await f.run('rotate', mutation('token', { token: 'pst-second-secret' }));
  await f.run('enable-new', mutation('enable'));
  const target = new Database(':memory:');
  try {
    initializeRelationalStorage(target, 'account-history');
    const members = new RelationalMemberStore(target),
      quotas = new RelationalQuotaStore(target, {}),
      accounts = new RelationalAccountStore(target, quotas, new Map(), {}),
      generations = new RelationalGenerationStore(target, {});
    const commits = f.db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS n FROM commits ORDER BY commit_seq')
      .all()
      .map(({ n }) => readRelationalCommit(f.db, n));
    for (const stored of commits) {
      const apply = () =>
        target.transaction(() => {
          if (stored.events[0].eventType === 'MemberRegistered') {
            return members.replayMemberCommit(stored);
          }
          if (stored.events.some((e) => e.eventType === 'ImportSourceRegistered')) {
            return generations.replayGenerationCommit(stored);
          }
          return accounts.replayAccountCommit(stored);
        })();
      apply();
      const checkpoint = target.serialize();
      assert.equal(apply().replayed, true);
      assert.ok(target.serialize().equals(checkpoint));
    }
    assert.equal(businessState(target), businessState(f.db));
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
    assert.equal(
      target.prepare('SELECT COUNT(*) AS n FROM members WHERE auth_epoch IS NOT NULL').get().n,
      0,
    );
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
    assert.throws(() => assertRelationalStorageReady(target), /incomplete/);
    const exposed =
      JSON.stringify(f.db.prepare('SELECT * FROM commits').all()) +
      JSON.stringify(f.db.prepare('SELECT * FROM domain_events').all());
    for (const secret of [registration.token, 'pst-second-secret', keys.m1]) {
      assert.ok(!exposed.includes(secret));
    }
  } finally {
    target.close();
  }
});

test('回放拒绝错误观察身份、旧代次、旧序号、伪首绑和未知事实，外层捕获后保持原切点', async (t) => {
  const f = await fresh(t);
  const beforeRegister = f.db.serialize(),
    registered = await f.run('register', registration),
    beforeEnable = f.db.serialize(),
    enabled = await f.run('enable', mutation('enable'));
  for (const mode of [
    'wrong-observation',
    'wrong-epoch',
    'wrong-sequence',
    'registration-enabled',
    'binding-version',
    'unknown-owner',
  ]) {
    await t.test(mode, (t) => {
      const isRegister = mode.startsWith('registration') || mode.startsWith('binding'),
        target = new Database(isRegister ? beforeRegister : beforeEnable),
        stored = structuredClone(
          readRelationalCommit(f.db, isRegister ? registered.commitSeq : enabled.commitSeq),
        );
      try {
        target.pragma('foreign_keys=ON');
        for (const events of [stored.events, stored.command.events]) {
          if (mode === 'wrong-observation') {
            events.find((e) => e.eventType === 'AvailabilityChanged').payload.observationEventId =
              'not-this-observation';
          } else if (mode === 'wrong-epoch') {
            events.find((e) => e.eventType === 'QuotaObserved').payload.credentialEpoch = 2;
          } else if (mode === 'wrong-sequence') {
            events.find((e) => e.eventType === 'QuotaObserved').payload.sequence = '0';
          } else if (mode === 'registration-enabled') {
            events.find((e) => e.eventType === 'AccountRegistered').payload.enabled = true;
          } else if (mode === 'binding-version') {
            events.find((e) => e.eventType === 'AccountBound').payload.configVersion = 8;
          } else {
            events[0].eventType = 'CooldownChanged';
          }
        }
        const quotas = new RelationalQuotaStore(target, {}),
          accounts = new RelationalAccountStore(target, quotas, new Map(), {}),
          before = databaseState(target);
        target.transaction(() => assert.throws(() => accounts.replayAccountCommit(stored)))();
        assert.equal(databaseState(target), before);
        target.transaction(() =>
          accounts.replayAccountCommit(
            readRelationalCommit(f.db, isRegister ? registered.commitSeq : enabled.commitSeq),
          ),
        )();
      } finally {
        target.close();
      }
    });
  }
});

test('真实轮换隔离旧观察成功/失败和旧尝试冷却，混合提交可断点重复回放', async (t) => {
  const original = await fixture(t, { env: { OWNER_USER_ID: 'm1' } }),
    f = commandFixture(t, original.db, original.members),
    old = f.quotas.beginQuotaObservation('a', NOW);
  await original.accept('mixed', { samples: 2 });
  original.dispatch('mixed-0');
  original.fail('mixed-0', { aborted: false });
  f.now(NOW + 10);
  const beforeRotation = f.db.serialize();
  const rotated = await f.run('rotate-B', mutation('token', { token: 'pst-synthetic-B' }));
  await assert.rejects(
    f.run('invalid-token', mutation('token', { token: 'synthetic-upstream-pst' })),
    { statusCode: 400 },
  );
  await f.run('rotate-current', mutation('token', { token: 'pst-current-A' }));
  assert.equal(account(f.db).credential_epoch, 3);
  const beforeStale = databaseState(f.db);
  for (const result of [
    accountQuotaResult('a', rawQuota(), new Date(old.startedAt).toISOString()),
    accountQuotaErrorResult('a', new Error('old failure'), new Date(old.startedAt).toISOString()),
  ]) {
    const ignored = f.db.transaction(() =>
      f.quotas.recordQuotaObservation(meta(`old-${result.ok}`, NOW + 11), old, result),
    )();
    assert.equal(ignored.ignored, true);
    assert.equal(databaseState(f.db), beforeStale);
  }
  const cooldown = account(f.db).cooldown_until;
  await original.publish('mixed-0', NOW + 15);
  assert.equal(account(f.db).cooldown_until, cooldown);
  assert.equal(original.generations.readCharge('mixed-0').status, 'refunded');
  assert.deepEqual(f.db.prepare('SELECT status FROM generation_jobs ORDER BY sample_index').all(), [
    { status: 'failed' },
    { status: 'failed' },
  ]);
  f.now(NOW + 20);
  await f.run('enable-current', mutation('enable'));
  assert.ok(BigInt(f.quotas.readAccountQuota('a').observedSequence) > 9007199254740993n);
  const credentialTarget = new Database(beforeRotation);
  try {
    credentialTarget.pragma('foreign_keys=ON');
    const quotas = new RelationalQuotaStore(credentialTarget, {}),
      accounts = new RelationalAccountStore(credentialTarget, quotas, new Map(), {});
    credentialTarget.transaction(() =>
      accounts.replayAccountCommit(readRelationalCommit(f.db, rotated.commitSeq)),
    )();
    assert.equal(
      credentialTarget.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n,
      0,
    );
  } finally {
    credentialTarget.close();
  }
  const target = new Database(':memory:');
  try {
    initializeRelationalStorage(target, 'generation-history');
    const members = new RelationalMemberStore(target),
      quotas = new RelationalQuotaStore(target, {}),
      accounts = new RelationalAccountStore(target, quotas, new Map(), {}),
      generations = new RelationalGenerationStore(target, {}),
      inputs = new RelationalInputStore(target);
    const accountIds = new Set(['rotate-B', 'rotate-current', 'enable-current']);
    const commits = f.db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS n FROM commits ORDER BY commit_seq')
      .all()
      .map(({ n }) => readRelationalCommit(f.db, n));
    const replay = (stored) =>
      target.transaction(() => {
        if (stored.events[0].eventType === 'MemberRegistered') {
          return members.replayMemberCommit(stored);
        }
        if (accountIds.has(stored.command.commandId)) {
          return accounts.replayAccountCommit(stored);
        }
        if (stored.events.every((event) => relationalQuotaFacts.has(event.eventType))) {
          return quotas.replayQuotaCommit(stored);
        }
        if (stored.events.some((event) => event.eventType === 'RequestAccepted')) {
          return inputs.replayAcceptedInput(original.inputs, stored.events[0].commitSeq);
        }
        return generations.replayGenerationCommit(stored);
      })();
    for (const stored of commits) {
      replay(stored);
      const checkpoint = target.serialize();
      assert.equal(replay(stored).replayed, true);
      assert.ok(target.serialize().equals(checkpoint));
    }
    assert.equal(businessState(target), businessState(f.db));
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
    assert.deepEqual(target.pragma('foreign_key_check'), []);
  } finally {
    target.close();
  }
});

test('账户准备不领取票，实际开始前凭据或授权变化不发查询、不写观察，健康新调用可重试', async (t) => {
  for (const mode of ['credential', 'owner'])
    await t.test(mode, async (t) => {
      const f = await fresh(t);
      await f.run('start-register', registration);
      let calls = 0;
      const metadata = meta('before-start', NOW, 'm1'),
        input = mutation('enable');
      await assert.rejects(
        ownerAccountCommand(
          f.accounts,
          f.authorize,
          metadata,
          input,
          async (_pst, _proxy, _id, start) => {
            if (mode === 'credential')
              await f.run('change-before-start', mutation('token', { token: 'pst-start-new' }));
            else f.env.OWNER_USER_ID = 'm2';
            const before = databaseState(f.db);
            try {
              await start();
              calls++;
              return rawQuota();
            } finally {
              assert.equal(databaseState(f.db), before);
            }
          },
        ),
        { statusCode: mode === 'owner' ? 403 : 409 },
      );
      assert.equal(calls, 0);
      f.env.OWNER_USER_ID = 'm1';
      const before = f.quotas.readAccountQuota('a').observedSequence;
      const result = await f.run('healthy-actual-start', input);
      assert.equal(result.receipt.enabled, true);
      assert.equal(
        f.quotas.readAccountQuota('a').observedSequence,
        String(BigInt(before ?? '0') + 1n),
      );
    });
});

test('账户准备与实际start之间已有同键原回执时零raw IO返回，不按后来可用性重决定', async (t) => {
  const f = await fresh(t);
  await f.run('concurrent-register', registration);
  const input = mutation('enable');
  let raw = 0,
    committed;
  const result = await ownerAccountCommand(
    f.accounts,
    f.authorize,
    meta('same-command', NOW, 'm1'),
    input,
    async (_pst, _proxy, _id, start) => {
      committed = await f.run('same-command', input);
      const before = databaseState(f.db);
      try {
        await start();
        raw++;
        return rawQuota();
      } finally {
        assert.equal(databaseState(f.db), before);
      }
    },
  );
  assert.deepEqual(result.receipt, committed.receipt);
  assert.equal(result.replayed, true);
  assert.equal(raw, 0);
  assert.equal(f.queries(), 1, 'only the other actually started query ran');
});
