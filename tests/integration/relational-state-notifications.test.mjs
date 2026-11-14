import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { createHash } from 'node:crypto';
import { fixture, meta, NOW, keys, databaseState } from '../helpers/relational-generation.mjs';
import { relationalStateNotificationsFor } from '../../server/events/relational-state-notifications.ts';
import { stateEventsFor, stateInvalidation } from '../../server/events/state-notifications.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import {
  authenticateMemberApiKey,
  authenticateMemberSession,
  createMemberSession,
  renewMemberSession,
  logoutMemberSession,
} from '../../server/members/authentication.ts';
import { activityDate } from '../../server/reports/activity-heatmap.ts';

const midnight = NOW - ((NOW + 8 * 3600000) % 86400000);

function notifications(f) {
  const events = new EventEmitter(),
    commits = [];
  const notifier = relationalStateNotificationsFor(f.db, events);
  events.on('commit', (event) => commits.push({ event, inTransaction: f.db.inTransaction }));
  return { events, commits, notifier, members: new RelationalMemberStore(f.db, {}, notifier) };
}

test('真实最外层回滚与被catch的保存点：只通知最终存留成员和原child日期', async (t) => {
  const f = await fixture(t, { openingAt: midnight - 1 });
  await f.accept('rollback', { memberId: 'm1', now: midnight - 1 });
  await f.accept('kept', { memberId: 'm2', now: midnight + 1 });
  const { notifier, commits, members } = notifications(f),
    before = databaseState(f.db);
  assert.throws(
    () =>
      notifier.commit(() =>
        f.db.transaction(() => {
          f.generations.cancelWaitingGeneration(meta('outer-rollback', NOW + 1), 'rollback');
          members.replaceMemberApiKey('m1', 'synthetic-rolled-back-key');
          throw Error('outer rollback');
        })(),
      ),
    /outer rollback/,
  );
  assert.equal(databaseState(f.db), before);
  assert.deepEqual(commits, []);
  notifier.commit(() =>
    f.db.transaction(() => {
      try {
        f.db.transaction(() => {
          members.replaceMemberApiKey('m1', 'synthetic-savepoint-key');
          f.generations.cancelWaitingGeneration(meta('inner-rollback', NOW + 1), 'rollback');
          throw Error('savepoint rollback');
        })();
      } catch (error) {
        assert.match(error.message, /savepoint rollback/);
      }
      f.generations.cancelWaitingGeneration(meta('kept-cancel', NOW + 1), 'kept');
    })(),
  );
  assert.equal(commits.length, 1);
  assert.equal(commits[0].inTransaction, false);
  assert.deepEqual(commits[0].event.memberIds, ['m2']);
  assert.deepEqual(commits[0].event.activityChanges, [
    { memberId: 'm2', date: activityDate(new Date(midnight + 1).toISOString()) },
  ]);
  assert.ok(!commits[0].event.collections.includes('sessions'));
  assert.equal((await authenticateMemberApiKey(members, keys.m1)) !== null, true);
});

test('同一顶层多owner、原回执重领、保存点误用和监听器异常不伪装提交失败', async (t) => {
  const f = await fixture(t);
  await f.accept('first');
  const { notifier, events, commits, members } = notifications(f);
  const observed = [];
  events.prependListener('commit', () => {
    throw Error('private listener text');
  });
  events.on('commit', () => observed.push('later'));
  events.on('commit', async () => {
    throw Error('private async text');
  });
  const result = notifier.commit(() => f.dispatch('first-0'));
  assert.equal(result.replayed, false);
  assert.deepEqual(observed, ['later']);
  assert.equal(commits.length, 1);
  assert.ok(commits[0].event.collections.includes('jobs'));
  assert.ok(commits[0].event.collections.includes('accounts'));
  assert.ok(Object.isFrozen(commits[0].event));
  notifier.commit(() => f.dispatch('first-0'));
  assert.equal(commits.length, 1, '原回执没有新提交，不广播');
  assert.throws(() => f.db.transaction(() => notifier.commit(() => {}))(), /outer commit/);
  const callback = () => {
    throw Error('not called');
  };
  await assert.rejects(
    members.memberApiKeyTransaction('none', () =>
      f.db.transaction(() => notifier.commit(callback))(),
    ),
    /outer commit/,
  );
  assert.equal(commits.length, 1);
  assert.ok(!JSON.stringify(commits).includes('private'));
});

test('同一runtime/hub与两成员原日期配对，Key转归不改历史归属', async (t) => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: NOW });
  const f = await fixture(t, { openingAt: midnight - 1 });
  await f.accept('alice', { memberId: 'm1', now: midnight - 1 });
  await f.accept('bob', { memberId: 'm2', now: midnight + 1 });
  const { notifier, events, commits } = notifications(f);
  assert.equal(relationalStateNotificationsFor(f.db, events), notifier);
  assert.throws(() => relationalStateNotificationsFor(f.db, new EventEmitter()), /one runtime/);
  assert.throws(() => new RelationalMemberStore(f.seed, {}, notifier), /same runtime database/);
  const hub = stateEventsFor(notifier),
    delivered = [];
  assert.equal(stateEventsFor(notifier), hub);
  hub.subscribe((event) => delivered.push(event));
  t.after(() => events.emit('close'));
  // 当前Key已改归另一稳定成员；历史仍按原request/child归属。
  f.db.transaction(() => {
    f.members.replaceMemberApiKey('m1', 'new-m1');
    f.members.replaceMemberApiKey('m2', keys.m1);
  })();
  notifier.commit(() =>
    f.db.transaction(() => {
      f.generations.cancelWaitingGeneration(meta('alice-cancel', NOW + 1), 'alice');
      f.generations.cancelWaitingGeneration(meta('bob-cancel', NOW + 1), 'bob');
    })(),
  );
  assert.equal(commits.length, 1);
  assert.equal(delivered.length, 0);
  t.mock.timers.tick(200);
  assert.equal(delivered.length, 1);
  const a = activityDate(new Date(midnight - 1).toISOString()),
    b = activityDate(new Date(midnight + 1).toISOString());
  assert.deepEqual(stateInvalidation(delivered[0], 'm1', false).activityDates, [a]);
  assert.deepEqual(stateInvalidation(delivered[0], 'm2', false).activityDates, [b]);
  assert.deepEqual(stateInvalidation(delivered[0], 'owner', true).activityDates, [a, b]);
  assert.equal(stateInvalidation(delivered[0], 'stranger', false).activityDates, undefined);
  assert.ok(!JSON.stringify(delivered).includes(keys.m1));
});

test('纯签发/续期/鉴权不广播；只有私有logout也能唤醒hub且不产生业务scope', async (t) => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: NOW });
  const f = await fixture(t);
  const { notifier, events, commits, members } = notifications(f),
    delivered = [];
  stateEventsFor(notifier).subscribe((event) => delivered.push(event));
  t.after(() => events.emit('close'));
  const user = await authenticateMemberApiKey(members, keys.m1);
  const session = await createMemberSession(members, user, {}, NOW);
  await authenticateMemberSession(members, session.token, {}, NOW + 1);
  await renewMemberSession(members, session.token, {}, NOW + 2);
  assert.deepEqual(commits, []);
  const count = f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n;
  await logoutMemberSession(members, session.token);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n, count);
  assert.equal(commits.length, 1);
  assert.deepEqual(commits[0].event.collections, ['sessions']);
  assert.deepEqual(commits[0].event.memberIds, ['m1']);
  t.mock.timers.tick(200);
  assert.equal(delivered.length, 1);
  assert.equal(stateInvalidation(delivered[0], 'm1', true), null);
  assert.equal(await authenticateMemberSession(members, session.token, {}, NOW + 3), null);
  await logoutMemberSession(members, session.token);
  assert.equal(commits.length, 1, '重复logout不广播');
});

test('私有epoch/key变更外层rollback无唤醒，成功提交后重入鉴权安全', async (t) => {
  const f = await fixture(t);
  const { notifier, events, commits, members } = notifications(f);
  const user = await authenticateMemberApiKey(members, keys.m1),
    session = await createMemberSession(members, user, {}, NOW);
  const prepared = await members.prepareMemberCredentials('m1', {
    username: 'notification.member',
    password: 'synthetic-password-12345',
  });
  const before = databaseState(f.db);
  assert.throws(
    () =>
      notifier.commit(() =>
        f.db.transaction(() => {
          members.setMemberCredentials(prepared);
          throw Error('private rollback');
        })(),
      ),
    /private rollback/,
  );
  assert.equal(databaseState(f.db), before);
  assert.deepEqual(commits, []);
  const rechecks = [];
  events.on('commit', () =>
    rechecks.push(authenticateMemberSession(members, session.token, {}, NOW + 1)),
  );
  notifier.commit(() => f.db.transaction(() => members.setMemberCredentials(prepared))());
  assert.equal(commits.length, 1);
  assert.deepEqual(await Promise.all(rechecks), [null]);
  assert.ok(commits[0].event.collections.includes('sessions'));
  assert.ok(!JSON.stringify(commits).includes(prepared.credentials.hash));
});

test('单纯原响应证据更新不重算日期；原0时刻不替换成本次提交日', async (t) => {
  const f = await fixture(t, { openingAt: 0 });
  await f.accept('zero', { now: 0 });
  f.dispatch('zero-0', 1);
  const { notifier, commits } = notifications(f);
  notifier.commit(() =>
    f.db.transaction(() =>
      f.generations.recordGenerationResponse(meta('response-only', 4), 'zero-0', f.response(4)),
    )(),
  );
  assert.equal(commits.length, 1);
  assert.deepEqual(commits[0].event.activityChanges, []);
  notifier.commit(() =>
    f.db.transaction(() =>
      f.generations.failGeneration(meta('zero-failed', NOW), 'zero-0', {
        errorCode: 'upstream_unavailable',
        errorStatus: 503,
        aborted: false,
      }),
    )(),
  );
  assert.equal(commits.length, 2);
  assert.deepEqual(commits[1].event.activityChanges, [{ memberId: 'm1', date: '1970-01-01' }]);
});

test('通知阶段只读本笔公开facts和有界job来源，实际SQL计划不扫描全库或保护正文', async (t) => {
  const f = await fixture(t);
  await f.accept('sql');
  const { notifier, commits } = notifications(f),
    queries = [];
  const original = f.db.prepare;
  let notificationPhase = false;
  f.db.prepare = function (sql) {
    if (notificationPhase) queries.push(sql);
    return original.call(this, sql);
  };
  try {
    notifier.commit(() => {
      const result = f.dispatch('sql-0');
      notificationPhase = true;
      return result;
    });
  } finally {
    f.db.prepare = original;
  }
  assert.equal(commits.length, 1);
  assert.ok(queries.length > 0);
  assert.ok(
    queries.every(
      (sql) =>
        !/(?:FROM|JOIN)\s+(?:generation_inputs|input_assets|normalization_contexts|generation_diagnostics)\b/i.test(
          sql,
        ),
    ),
  );
  const job = queries.find((sql) => sql.includes('generation_job_origins'));
  assert.ok(job);
  const plan = f.db
    .prepare('EXPLAIN QUERY PLAN ' + job)
    .all('sql-0')
    .map((row) => row.detail)
    .join('\n');
  assert.match(plan, /SEARCH j.*\(id=\?\)/);
  assert.match(plan, /SEARCH domain_events.*\(stream_id=\?\)/);
  assert.doesNotMatch(plan, /SCAN (?:j|r|domain_events)\b/);
});

test('失败保存点的新注册/停用不能让提交后私有比较混入中间public状态', async (t) => {
  const f = await fixture(t),
    { notifier, members, commits } = notifications(f);
  notifier.commit(() =>
    f.db.transaction(() => {
      try {
        f.db.transaction(() => {
          members.registerMember(meta('transient-register', NOW), {
            memberId: 'transient',
            displayName: 'synthetic',
            apiKey: 'synthetic-transient-key',
          });
          members.setMemberStatus(meta('transient-disable', NOW), 'm1', 'disabled');
          throw Error('rollback both');
        })();
      } catch (error) {
        assert.match(error.message, /rollback both/);
      }
      members.replaceMemberApiKey('m2', 'synthetic-kept-key');
    })(),
  );
  assert.equal(commits.length, 1);
  assert.deepEqual(commits[0].event.memberIds, ['m2']);
  assert.deepEqual(commits[0].event.activityChanges, []);
  assert.equal(f.db.prepare('SELECT status FROM members WHERE id=?').get('m1').status, 'active');
  assert.equal(f.db.prepare('SELECT id FROM members WHERE id=?').get('transient'), undefined);
});

test('首次owner换绑只唤醒真实受影响成员，不让失败私有候选沾上全局owner变化', async (t) => {
  const f = await fixture(t),
    { members, commits } = notifications(f),
    sessions = {};
  for (const id of ['m1', 'm2']) {
    sessions[id] = await createMemberSession(
      members,
      await authenticateMemberApiKey(members, keys[id]),
      {},
      NOW,
    );
  }
  assert.deepEqual(commits, []);
  await members.memberSessionTransaction(
    () => {
      try {
        f.db.transaction(() => {
          members.replaceMemberApiKey('m2', 'synthetic-failed-owner-candidate');
          throw Error('private candidate rollback');
        })();
      } catch (error) {
        assert.match(error.message, /candidate rollback/);
      }
    },
    { memberSessionOwner: { ownerUserId: 'm1', guildId: '1134557553011998840' } },
  );
  assert.equal(commits.length, 1);
  assert.deepEqual(commits[0].event.collections, ['sessions']);
  assert.deepEqual(commits[0].event.memberIds, ['m1']);
  const env = { OWNER_USER_ID: 'm1', DISCORD_GUILD_ID: '1134557553011998840' };
  assert.equal(await authenticateMemberSession(members, sessions.m1.token, env, NOW + 1), null);
  assert.ok(await authenticateMemberSession(members, sessions.m2.token, env, NOW + 1));
});

test('仅私有Discord资格撤销也唤醒鉴权，不把token或checkedAt刷新当通知内容', async (t) => {
  const f = await fixture(t),
    { members, commits } = notifications(f);
  const before = f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n;
  await members.discordAuthenticationTransaction((transaction) => {
    const user = transaction.readMember('m1');
    user.discord = {
      id: '222222222222222222',
      guildId: '1134557553011998840',
      member: false,
      checkedAt: NOW,
      accessToken: 'synthetic-private-discord-token',
    };
    transaction.saveAuthentication(user);
  });
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n, before);
  assert.equal(commits.length, 1);
  assert.deepEqual(commits[0].event.collections, ['sessions']);
  assert.deepEqual(commits[0].event.memberIds, ['m1']);
  assert.ok(!JSON.stringify(commits).includes('synthetic-private'));
  await members.discordAuthenticationTransaction((transaction) => {
    const user = transaction.readMember('m1');
    user.discord.checkedAt = NOW + 1;
    user.discord.accessToken = 'synthetic-private-refreshed-token';
    transaction.saveAuthentication(user);
  });
  assert.equal(commits.length, 1, '只刷新私有token/checkedAt不广播');
});

test('失败私有候选后真实签发及两种原清理都保持安静', async (t) => {
  for (const kind of ['new', 'expired', 'obsolete']) {
    await t.test(kind, async (t) => {
      const f = await fixture(t),
        { members, commits, notifier } = notifications(f);
      const user = await authenticateMemberApiKey(members, keys.m1);
      let old = null;
      if (kind === 'expired') {
        old = await createMemberSession(members, user, {}, NOW);
      } else if (kind === 'obsolete') {
        // 现签发owner可清理的旧epoch私有行；不是新的业务受理或ready导入证明。
        old = { token: 's'.repeat(43), expiresAt: NOW + 60000 };
        const epoch = user.memberAuth.version;
        await f.members.memberSessionTransaction(() => {}, {
          memberSession: () => ({
            tokenHash: createHash('sha256').update(old.token).digest('hex'),
            memberId: 'm1',
            authEpoch: (epoch[0] === '0' ? '1' : '0') + epoch.slice(1),
            ownerBinding: null,
            createdAt: NOW,
            expiresAt: old.expiresAt,
          }),
        });
      }
      const at = kind === 'expired' ? old.expiresAt + 1 : NOW;
      // 实际factory会有多个MemberStore：候选来自A，签发/清理由B在共同context中完成。
      const signer = new RelationalMemberStore(f.db, {}, notifier);
      const original = signer.memberSessionTransaction.bind(signer);
      const sessionStore = {
        memberSessionTransaction: (mutate, options) =>
          original((transaction) => {
            try {
              f.db.transaction(() => {
                members.replaceMemberApiKey('m1', 'synthetic-rolled-back-before-issuance');
                throw Error('private rollback before issuance');
              })();
            } catch (error) {
              assert.match(error.message, /private rollback before issuance/);
            }
            return mutate(transaction);
          }, options),
      };
      const publicCount = f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n;
      const signed = await createMemberSession(sessionStore, user, {}, at);
      assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n, publicCount);
      assert.ok(await authenticateMemberApiKey(members, keys.m1));
      assert.ok(await authenticateMemberSession(members, signed.token, {}, at + 1));
      if (old) {
        assert.equal(await authenticateMemberSession(members, old.token, {}, at + 1), null);
      }
      assert.deepEqual(commits, []);
    });
  }
});

test('同一真实签发事务注销原Cookie仍准确唤醒本人，保留新Cookie和其他成员', async (t) => {
  const f = await fixture(t),
    { members, commits } = notifications(f);
  const user = await authenticateMemberApiKey(members, keys.m1);
  const old = await createMemberSession(members, user, {}, NOW);
  const other = await createMemberSession(
    members,
    await authenticateMemberApiKey(members, keys.m2),
    {},
    NOW,
  );
  const original = members.memberSessionTransaction.bind(members);
  const sessionStore = {
    memberSessionTransaction: (mutate, options) =>
      original(mutate, {
        ...options,
        memberSessionDelete: createHash('sha256').update(old.token).digest('hex'),
      }),
  };
  const signed = await createMemberSession(sessionStore, user, {}, NOW + 1);
  assert.equal(commits.length, 1);
  assert.deepEqual(commits[0].event.collections, ['sessions']);
  assert.deepEqual(commits[0].event.memberIds, ['m1']);
  assert.equal(await authenticateMemberSession(members, old.token, {}, NOW + 2), null);
  assert.ok(await authenticateMemberSession(members, signed.token, {}, NOW + 2));
  assert.ok(await authenticateMemberSession(members, other.token, {}, NOW + 2));
  assert.equal(commits.length, 1);
});
