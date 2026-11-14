import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fixture, meta, keys, NOW, databaseState } from '../helpers/relational-generation.mjs';
import {
  appendRelationalCommit,
  readRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import { relationalPayloadParsers } from '../../server/events/relational-fact-contracts.ts';
import {
  RelationalCommitReplayer,
  relationalReplayFacts,
} from '../../server/storage/relational-replay.ts';
import {
  RelationalMemberStore,
  relationalMemberFacts,
} from '../../server/storage/relational-member-store.ts';
import {
  RelationalInputStore,
  relationalInputFacts,
} from '../../server/storage/relational-input-store.ts';
import {
  RelationalGenerationStore,
  relationalGenerationFacts,
} from '../../server/storage/relational-generation-store.ts';
import {
  RelationalQuotaStore,
  relationalQuotaFacts,
} from '../../server/storage/relational-quota-store.ts';
import {
  RelationalAccountStore,
  relationalAccountFacts,
} from '../../server/storage/relational-account-store.ts';
import { relationalReplicaFacts } from '../../server/storage/relational-replica-store.ts';
import { ownerAccountCommand } from '../../server/members/owner-operations.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import {
  restoreRelationalPrivateSnapshot,
  relationalPrivateStateEmpty,
} from '../../server/storage/relational-private-restore.ts';
import {
  withAuthenticatedMemberApiKey,
  authenticateMemberApiKey,
  authenticateMemberSession,
  createMemberSession,
} from '../../server/members/authentication.ts';
import { accountQuotaResult } from '../../server/quota/account-refresh.ts';

function target(t, f) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const replayer = new RelationalCommitReplayer(db);
  const replay = (stored, source = f.inputs) =>
    db.transaction(() => replayer.replayCommit(stored, source))();
  const through = (until = f.db.prepare('SELECT MAX(commit_seq) AS n FROM commits').get().n) => {
    for (const { seq } of f.db
      .prepare(
        'SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE commit_seq<=? ORDER BY commit_seq',
      )
      .all(until)) {
      replay(readRelationalCommit(f.db, seq));
    }
  };
  return { db, replayer, replay, through };
}
function common(fact, streamId, memberId = null, accountId = null) {
  return {
    ...fact,
    streamId,
    memberId,
    accountId,
    occurredAt: NOW + 3,
    policyVersion: 'shared-quota-v1',
    pricingVersion: 'price1',
  };
}
function stopFact(id, epoch = 1) {
  return common(
    {
      eventType: 'AdmissionChanged',
      payload: { memberId: id, acceptingJobs: false, admissionEpoch: epoch },
    },
    `member:${id}`,
    id,
  );
}
function mixedStopCommand(f, memberId = 'm1') {
  const events = [
    stopFact('m1'),
    common(
      {
        eventType: 'WaitingJobsCancelled',
        payload: {
          jobIds: ['waiting-0', 'waiting-1'],
          reason: '管理员已停止此成员接单，未派发任务已取消',
          errorStatus: 409,
          cancellationReason: 'member_stopped',
        },
      },
      'request:waiting',
      'm1',
    ),
    common(
      {
        eventType: 'AvailabilityChanged',
        payload: {
          accountId: 'a',
          enabled: false,
          reason: 'paused',
          observationEventId: null,
        },
      },
      'account:a',
      null,
      'a',
    ),
    stopFact('m2'),
  ];
  const command = meta('mixed-stop', NOW + 3, memberId);
  delete command.policyVersion;
  delete command.pricingVersion;
  return {
    ...command,
    expectedVersions: Object.fromEntries(
      events.map((e) => [
        e.streamId,
        f.db
          .prepare(
            'SELECT CAST(stream_version AS TEXT) AS version FROM stream_heads WHERE stream_id=?',
          )
          .get(e.streamId).version,
      ]),
    ),
    receipt: { stopped: ['m1', 'm2'], cancelled: ['waiting-0', 'waiting-1'] },
    events,
  };
}
async function stopMixed(f) {
  return withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    (member) =>
      f.db.transaction(() =>
        new RelationalCommitReplayer(f.db, f.generations).appendCommit(
          mixedStopCommand(f, member.id),
        ),
      )(),
    {},
  );
}
function inputOf(event) {
  return Object.fromEntries(
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
  );
}
function resequence(stored, db, events = stored.events) {
  const versions = new Map();
  stored.command.expectedVersions = {};
  stored.events = events.map((event, eventIndex) => {
    if (!versions.has(event.streamId)) {
      const version =
        db
          .prepare(
            'SELECT CAST(stream_version AS TEXT) AS version FROM stream_heads WHERE stream_id=?',
          )
          .get(event.streamId)?.version || '0';
      versions.set(event.streamId, BigInt(version));
      stored.command.expectedVersions[event.streamId] = version;
    }
    versions.set(event.streamId, versions.get(event.streamId) + 1n);
    return { ...event, eventIndex, streamVersion: String(versions.get(event.streamId)) };
  });
  stored.eventCount = stored.events.length;
  stored.command.events = stored.events.map(inputOf);
  return stored;
}
async function privateSnapshot(f) {
  const filename = path.join(f.dir, 'selected-private.sqlite');
  await f.db.backup(filename);
  const work = new Database(filename);
  work.pragma('journal_mode=DELETE');
  work.close();
  return {
    filename,
    sha256: createHash('sha256')
      .update(await readFile(filename))
      .digest('hex'),
  };
}
function observe(f, q, id, fixed, at) {
  const ticket = q.beginQuotaObservation('a', at);
  const result = accountQuotaResult(
    'a',
    {
      tier: 3,
      subscriptionActive: true,
      subscriptionExpiresAt: null,
      fixed,
      purchased: '0',
      v5UsagePercent: '80',
      v5UsageTimeUntilNextPercent: 0,
    },
    new Date(at).toISOString(),
  );
  return f.db.transaction(() => q.recordQuotaObservation(meta(id, at), ticket, result))();
}

test('覆盖只来自六个实际 owner，未实现事实仍明确拒绝', () => {
  const owners = [
    relationalMemberFacts,
    relationalInputFacts,
    relationalGenerationFacts,
    relationalQuotaFacts,
    relationalAccountFacts,
    relationalReplicaFacts,
  ];
  assert.equal(
    owners.reduce((sum, facts) => sum + facts.size, 0),
    relationalReplayFacts().size,
  );
  assert.equal(relationalReplayFacts().size, 63);
  assert.deepEqual(
    Object.keys(relationalPayloadParsers)
      .filter((name) => !relationalReplayFacts().has(name))
      .sort(),
    [
      'FundsTransferred',
      'CachedResultLinked',
      'OpeningRequestRecorded',
      'OpeningJobRecorded',
      'OpeningAttemptRecorded',
      'OpeningArtifactRecorded',
      'OpeningReplicaRecorded',
      'OpeningInputRecorded',
      'OpeningIdempotencyRecorded',
    ].sort(),
  );
});

test('共同回放保留原政策 owner 的决定、独立版本与有限 number 阈值', async (t) => {
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { env }),
    quotas = new RelationalQuotaStore(f.db, env);
  const commands = [
    { action: 'fallback-policy', enabled: true },
    {
      action: 'free-image-policy',
      expectedVersion: 1,
      policy: { enabled: true, warningThreshold: null, hardLimit: 5 },
    },
    {
      action: 'loss-alert-policy',
      expectedVersion: 1,
      policy: { v5Points24h: 1e-15, fixedAnlas24h: 0, fixedAnlas30d: 9, memberAttempts24h: 0 },
    },
  ];
  for (const [index, command] of commands.entries()) {
    await withAuthenticatedMemberApiKey(
      f.members,
      keys.m1,
      (member) =>
        quotas.configureQuotaPolicy(meta(`policy-${index}`, NOW + 10, member.id), command, member),
      env,
    );
  }
  const r = target(t, f);
  r.through();
  const row = (db) => db.prepare('SELECT * FROM sharing_policy WHERE id=1').get();
  assert.deepEqual(row(r.db), row(f.db));
  assert.equal(row(r.db).loss_v5_24h, 1e-15);
  assert.equal(row(r.db).config_version, 1);
  const once = r.db.serialize();
  r.through();
  assert.deepEqual(r.db.serialize(), once);
});

test('真实鉴权内的混合提交按原顺序跨 member/generation/quota 应用，只保存一份回执，私有另行恢复', async (t) => {
  const network = t.mock.method(globalThis, 'fetch', () => {
    throw new Error('No network during replay');
  });
  const f = await fixture(t);
  await f.accept('running');
  f.dispatch('running-0');
  await f.accept('waiting', { samples: 2 });
  const beforeMixed = f.db.prepare('SELECT MAX(commit_seq) AS n FROM commits').get().n;
  const mixed = await stopMixed(f),
    stored = readRelationalCommit(f.db, mixed.commitSeq),
    r = target(t, f);
  r.through(beforeMixed);
  r.db.exec(`CREATE TEMP TABLE projection_order(event_id TEXT);
    CREATE TEMP TRIGGER member_order AFTER UPDATE ON members BEGIN INSERT INTO projection_order VALUES(NEW.source_event_id); END;
    CREATE TEMP TRIGGER job_order AFTER UPDATE ON generation_jobs BEGIN INSERT INTO projection_order VALUES(NEW.source_event_id); END;
    CREATE TEMP TRIGGER account_order AFTER UPDATE ON upstream_accounts BEGIN INSERT INTO projection_order VALUES(NEW.source_event_id); END;`);
  r.replay(stored);
  const order = r.db
    .prepare('SELECT event_id FROM projection_order')
    .all()
    .map((row) => row.event_id)
    .filter((id, index, ids) => index === 0 || id !== ids[index - 1]);
  assert.deepEqual(
    order,
    stored.events.map((event) => event.eventId),
  );
  assert.deepEqual(r.db.prepare('SELECT id,status FROM generation_jobs ORDER BY id').all(), [
    { id: 'running-0', status: 'running' },
    { id: 'waiting-0', status: 'failed' },
    { id: 'waiting-1', status: 'failed' },
  ]);
  assert.equal(
    r.db.prepare('SELECT COUNT(*) AS n FROM commits WHERE command_id=?').get('mixed-stop').n,
    1,
  );
  assert.deepEqual(readRelationalCommit(r.db, mixed.commitSeq), stored);
  assert.equal(relationalPrivateStateEmpty(r.db), true);
  const once = r.db.serialize();
  assert.equal(r.replay(stored).replayed, true);
  assert.deepEqual(r.db.serialize(), once);
  const saved = await privateSnapshot(f);
  r.db.transaction(() => restoreRelationalPrivateSnapshot(r.db, saved.filename, saved.sha256))();
  assert.equal(databaseState(r.db), databaseState(f.db));
  assert.equal(r.replay(stored).replayed, true);
  const withPrivate = r.db.serialize();
  f.db.transaction(() =>
    f.members.registerMember(meta('later-member'), {
      memberId: 'later',
      displayName: 'later',
      apiKey: 'private-later-key',
    }),
  )();
  const later = readRelationalCommit(f.db, String(BigInt(mixed.commitSeq) + 1n));
  assert.throws(() => r.replay(later), /without private/);
  assert.deepEqual(r.db.serialize(), withPrivate);
  assert.equal(network.mock.callCount(), 0);
  assert.throws(() => assertRelationalStorageReady(r.db), /incomplete/);
});

test('第二 owner 已写入后异常被外层捕获，完整提交、回执、游标与所有投影仍全表回滚', async (t) => {
  const f = await fixture(t);
  await f.accept('waiting', { samples: 2 });
  const through = f.db.prepare('SELECT MAX(commit_seq) AS n FROM commits').get().n;
  const result = await stopMixed(f),
    stored = readRelationalCommit(f.db, result.commitSeq),
    r = target(t, f);
  r.through(through);
  let reached = false;
  r.db.function('fail_second_owner', () => {
    reached = true;
    throw new Error('second owner failure');
  });
  r.db.exec(
    "CREATE TEMP TRIGGER fail_second AFTER UPDATE ON generation_jobs WHEN NEW.id='waiting-0' AND NEW.status='failed' AND (SELECT accepting_jobs FROM members WHERE id='m1')=0 BEGIN SELECT fail_second_owner(); END",
  );
  const before = databaseState(r.db);
  r.db.transaction(() =>
    assert.throws(() => r.replayer.replayCommit(stored, f.inputs), /second owner failure/),
  )();
  assert.equal(reached, true);
  assert.equal(databaseState(r.db), before);
  r.db.exec('DROP TRIGGER fail_second');
  assert.equal(r.replay(stored).replayed, false);
  const once = r.db.serialize();
  assert.equal(r.replay(stored).replayed, true);
  assert.deepEqual(r.db.serialize(), once);
});

test('在线入口不接受自报新写入或重领结果，资金不能重复扣除，新接单状态不能跳过', async (t) => {
  const f = await fixture(t);
  const quotas = new RelationalQuotaStore(f.db, { OWNER_USER_ID: 'm1' });
  const funding = await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    () =>
      quotas.manageQuotaFunding(meta('contribution-result', NOW, 'm1'), 'a', {
        action: 'operating-contribute',
        receipt: 'contribution-result',
        resource: 'anlasFixed',
        amount: 10,
      }),
    {},
  );
  const replayer = new RelationalCommitReplayer(f.db, f.generations),
    stored = readRelationalCommit(f.db, funding.commitSeq),
    before = databaseState(f.db),
    wallet = f.generations.readWallet('a', 'm1').anlasFixed;
  assert.throws(
    () =>
      f.db.transaction(() =>
        replayer.applyCommit({ ...funding, replayed: false, events: stored.events }),
      )(),
    /not a function/,
  );
  assert.equal(f.generations.readWallet('a', 'm1').anlasFixed, wallet);
  assert.equal(databaseState(f.db), before);
  f.db.transaction(() => {
    assert.throws(() => replayer.appendCommit(funding), /Invalid relational command/);
    assert.equal(replayer.appendCommit(stored.command).replayed, true);
  })();
  assert.equal(databaseState(f.db), before);

  assert.throws(
    () =>
      f.db.transaction(() => {
        const { policyVersion, pricingVersion, ...command } = meta('fake-replayed-stop', NOW + 3);
        const result = appendRelationalCommit(f.db, {
          ...command,
          expectedVersions: {
            'member:m1': f.db
              .prepare(
                "SELECT CAST(stream_version AS TEXT) AS v FROM stream_heads WHERE stream_id='member:m1'",
              )
              .get().v,
          },
          receipt: { stopped: 'm1' },
          events: [stopFact('m1')],
        });
        replayer.applyCommit({ ...result, replayed: true, events: [] });
      })(),
    /not a function/,
  );
  assert.equal(databaseState(f.db), before);
  assert.equal(replayer['#applyCommit'], undefined);
  await f.accept('waiting', { samples: 2 });
  assert.equal((await stopMixed(f)).replayed, false);
  const once = f.db.serialize();
  assert.equal((await stopMixed(f)).replayed, true);
  assert.deepEqual(f.db.serialize(), once);
});

test('在线受控追加与投影在同一保存点，第二 owner 失败被外层捕获也不留下原事实', async (t) => {
  const f = await fixture(t);
  await f.accept('waiting', { samples: 2 });
  f.db.exec(
    "CREATE TEMP TRIGGER fail_online_second AFTER UPDATE ON generation_jobs WHEN NEW.id='waiting-0' AND NEW.status='failed' AND (SELECT accepting_jobs FROM members WHERE id='m1')=0 BEGIN SELECT RAISE(ABORT,'online second owner failure'); END",
  );
  const before = databaseState(f.db);
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    (member) =>
      f.db.transaction(() => {
        assert.throws(
          () =>
            new RelationalCommitReplayer(f.db, f.generations).appendCommit(
              mixedStopCommand(f, member.id),
            ),
          /online second owner failure/,
        );
        assert.equal(databaseState(f.db), before);
      })(),
    {},
  );
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER fail_online_second');
  assert.equal((await stopMixed(f)).replayed, false);
  const once = f.db.serialize();
  assert.equal((await stopMixed(f)).replayed, true);
  assert.deepEqual(f.db.serialize(), once);
});

test('完整提交缺失、重排、不同历史、checkpoint 及未实现 owner 均拒绝且零部分写入', async (t) => {
  const f = await fixture(t);
  await f.accept('waiting', { samples: 2 });
  const through = f.db.prepare('SELECT MAX(commit_seq) AS n FROM commits').get().n,
    result = await stopMixed(f),
    stored = readRelationalCommit(f.db, result.commitSeq),
    r = target(t, f);
  r.through(through);
  for (const change of [
    (bad) => {
      bad.events.pop();
      bad.command.events.pop();
    },
    (bad) => {
      [bad.events[0], bad.events[1]] = [bad.events[1], bad.events[0]];
      [bad.command.events[0], bad.command.events[1]] = [
        bad.command.events[1],
        bad.command.events[0],
      ];
    },
    (bad) => {
      bad.historyId = 'different';
    },
    (bad) => {
      bad.events[2].eventType = 'FundsTransferred';
      bad.events[2].payload = {
        accountId: 'a',
        fromMemberId: 'm1',
        toMemberId: 'm2',
        resources: { v5: '1', anlasFixed: '0', anlasPurchased: '0' },
        reason: 'transfer',
      };
      resequence(bad, r.db);
    },
  ]) {
    const bad = structuredClone(stored),
      before = databaseState(r.db);
    change(bad);
    assert.throws(() => r.replay(bad));
    assert.equal(databaseState(r.db), before);
  }
  r.replay(stored);
  const conflict = structuredClone(stored),
    before = databaseState(r.db);
  conflict.command.receipt = { changed: true };
  assert.throws(() => r.replay(conflict), /checkpoint/);
  assert.equal(databaseState(r.db), before);
});

test('额度来源消费上下文不被跨 owner 分派重置，观察与有限资金重复消费均整笔拒绝', async (t) => {
  for (const kind of ['observation', 'finite']) {
    await t.test(kind, async (t) => {
      const f = await fixture(t, { seconds: 0 }),
        q = new RelationalQuotaStore(f.db, { OWNER_USER_ID: 'm1' });
      if (kind === 'finite') {
        observe(f, q, 'down', '7900', NOW + 10);
        observe(f, q, 'return', '8000', NOW + 20);
      }
      const through = f.db.prepare('SELECT MAX(commit_seq) AS n FROM commits').get().n;
      const result =
        kind === 'observation'
          ? observe(f, q, 'up', '9000', NOW + 30)
          : await withAuthenticatedMemberApiKey(
              f.members,
              keys.m1,
              () =>
                q.manageQuotaFunding(meta('finite', NOW + 30, 'm1'), 'a', {
                  action: 'operating-register-funding',
                  receipt: 'finite-receipt',
                  resource: 'anlasFixed',
                  amount: 100,
                  destination: 'members',
                  fromUnattributed: true,
                }),
              {},
            );
      const stored = readRelationalCommit(f.db, result.commitSeq),
        allocation = stored.events.find((event) => event.eventType === 'AllocationApplied'),
        r = target(t, f);
      assert.ok(allocation);
      r.through(through);
      const bad = structuredClone(stored),
        extra = { ...structuredClone(allocation), eventId: `again-${kind}` },
        member = {
          ...structuredClone(allocation),
          ...stopFact('m2'),
          eventId: `interleaved-member-${kind}`,
          streamKind: 'member',
        };
      resequence(bad, r.db, [...bad.events, member, extra]);
      let memberApplied = false;
      r.db.function('member_applied', () => {
        memberApplied = true;
        return 0;
      });
      r.db.exec(
        'CREATE TEMP TRIGGER member_applied AFTER UPDATE ON members BEGIN SELECT member_applied(); END',
      );
      const before = databaseState(r.db);
      r.db.transaction(() =>
        assert.throws(
          () => r.replayer.replayCommit(bad),
          /重复消费|仅消费一次|同步金融事实与原观察、净预算、配置及分配决定不符/,
        ),
      )();
      assert.equal(
        memberApplied,
        kind === 'finite',
        '观察决定先核对，有限资金仍验证跨 owner 消费上下文',
      );
      assert.equal(databaseState(r.db), before);
      assert.equal(r.replay(stored).replayed, false);
      const once = r.db.serialize();
      assert.equal(r.replay(stored).replayed, true);
      assert.deepEqual(r.db.serialize(), once);
    });
  }
});

test('退款仍可引用同一完整提交的 ReviewOpened；缺少原核对事实不能留下退款', async (t) => {
  const f = await fixture(t);
  await f.accept('unknown');
  f.dispatch('unknown-0');
  const through = f.db.prepare('SELECT MAX(commit_seq) AS n FROM commits').get().n,
    result = f.fail('unknown-0', { aborted: false }),
    stored = readRelationalCommit(f.db, result.commitSeq),
    r = target(t, f);
  r.through(through);
  assert.ok(stored.events.some((event) => event.eventType === 'ReviewOpened'));
  const bad = structuredClone(stored);
  resequence(
    bad,
    r.db,
    bad.events.filter((event) => event.eventType !== 'ReviewOpened'),
  );
  const before = databaseState(r.db);
  assert.throws(() => r.replay(bad), /同一原提交/);
  assert.equal(databaseState(r.db), before);
  r.replay(stored);
  assert.equal(r.db.prepare('SELECT status FROM charges').get().status, 'refunded');
  assert.equal(
    r.db.prepare('SELECT review_status FROM generation_attempts').get().review_status,
    'pending',
  );
});

test('输入来源必须匹配完整原提交，正文损坏或重复 checkpoint 缺少正文时拒绝且不修补', async (t) => {
  const f = await fixture(t),
    other = await fixture(t);
  await f.accept('input');
  await other.accept('input');
  const stored = readRelationalCommit(f.db, '4'),
    r = target(t, f);
  r.through(3);
  const before = databaseState(r.db);
  r.db.transaction(() =>
    assert.throws(() => r.replayer.replayCommit(stored), /matching protected source/),
  )();
  assert.equal(databaseState(r.db), before);
  assert.throws(() => r.replay(stored, other.inputs), /原输入|上下文/);
  assert.equal(databaseState(r.db), before);
  const originalBody = f.db.prepare('SELECT body FROM generation_inputs').get().body,
    sourceTrigger = f.db
      .prepare("SELECT sql FROM sqlite_schema WHERE name='generation_inputs_identity'")
      .get().sql;
  const changeSourceBody = (body) => {
    f.db.exec('DROP TRIGGER generation_inputs_identity');
    f.db.prepare('UPDATE generation_inputs SET body=?').run(body);
    f.db.exec(sourceTrigger);
  };
  const damagedBody = JSON.parse(originalBody);
  damagedBody.input = 'damaged source';
  changeSourceBody(JSON.stringify(damagedBody));
  assert.throws(() => r.replay(stored), /原输入|上下文/);
  assert.equal(databaseState(r.db), before);
  changeSourceBody(originalBody);
  r.replay(stored);
  const body = r.db.prepare('SELECT body FROM generation_inputs').get().body;
  const trigger = r.db
    .prepare("SELECT sql FROM sqlite_schema WHERE name='generation_inputs_identity'")
    .get().sql;
  r.db.exec('DROP TRIGGER generation_inputs_identity');
  r.db.prepare("UPDATE generation_inputs SET body=json_set(body,'$.input','damaged')").run();
  r.db.exec(trigger);
  const corrupted = databaseState(r.db);
  assert.throws(() => r.replay(stored), /原输入|上下文/);
  assert.equal(databaseState(r.db), corrupted);
  assert.notEqual(r.db.prepare('SELECT body FROM generation_inputs').get().body, body);
});

test('旧 member replay 的停用/资格失效只撤销，不会被后续 active/资格恢复复活旧 epoch 和 Cookie', async (t) => {
  for (const kind of ['status', 'eligibility']) {
    await t.test(kind, async (t) => {
      const f = await fixture(t),
        user = await authenticateMemberApiKey(f.members, keys.m1),
        login = await createMemberSession(f.members, user, {}, NOW);
      const checkpoint = new Database(f.db.serialize());
      t.after(() => checkpoint.close());
      checkpoint.pragma('foreign_keys=ON');
      const memberReplay = new RelationalMemberStore(checkpoint);
      if (kind === 'status') {
        f.db.transaction(() => f.members.setMemberStatus(meta('disable'), 'm1', 'disabled'))();
        f.db.transaction(() => f.members.setMemberStatus(meta('enable'), 'm1', 'active'))();
      } else {
        for (const [index, eligible] of [false, true].entries()) {
          const e = common(
            {
              eventType: 'MemberEligibilityChanged',
              payload: {
                memberId: 'm1',
                discordId: 'discord',
                guildId: 'guild',
                checkedAt: NOW + index,
                eligible,
              },
            },
            'member:m1',
            'm1',
          );
          f.db.transaction(() =>
            appendRelationalCommit(f.db, {
              ...meta(`eligibility-${index}`),
              expectedVersions: { 'member:m1': String(index + 1) },
              receipt: { eligible },
              events: [e],
            }),
          )();
        }
      }
      for (const seq of ['4', '5']) {
        checkpoint.transaction(() =>
          memberReplay.replayMemberCommit(readRelationalCommit(f.db, seq)),
        )();
      }
      assert.equal(
        checkpoint.prepare("SELECT auth_epoch FROM members WHERE id='m1'").get().auth_epoch,
        null,
      );
      assert.equal(await authenticateMemberApiKey(memberReplay, keys.m1), null);
      assert.equal(await authenticateMemberSession(memberReplay, login.token, {}, NOW + 10), null);
    });
  }
});

test('账户真实轮换命令也经共同回放保留原代次和暂停，不恢复新 PST', async (t) => {
  const f = await fixture(t),
    env = { OWNER_USER_ID: 'm1' },
    quotas = new RelationalQuotaStore(f.db, env),
    accounts = new RelationalAccountStore(f.db, quotas, new Map(), env);
  await ownerAccountCommand(
    accounts,
    (write) => withAuthenticatedMemberApiKey(f.members, keys.m1, write, env),
    meta('rotate-replay', NOW + 10, 'm1'),
    { kind: 'token', accountId: 'a', token: 'pst-replay-next' },
    async () => {
      throw new Error('Rotation must not query');
    },
  );
  const r = target(t, f);
  r.through();
  assert.deepEqual(r.db.prepare('SELECT enabled,credential_epoch FROM upstream_accounts').get(), {
    enabled: 0,
    credential_epoch: 2,
  });
  assert.equal(r.db.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
  const stored = readRelationalCommit(f.db, '4'),
    once = r.db.serialize();
  assert.equal(r.replay(stored).replayed, true);
  assert.deepEqual(r.db.serialize(), once);
});

test('单提交投影闭包离开所属事务后不能继续写入', async (t) => {
  const f = await fixture(t);
  await f.accept('waiting', { samples: 2 });
  const accepted = readRelationalCommit(f.db, '4'),
    result = await stopMixed(f),
    stored = readRelationalCommit(f.db, result.commitSeq),
    r = target(t, f);
  r.through(3);
  const quotas = new RelationalQuotaStore(r.db),
    generations = new RelationalGenerationStore(r.db),
    inputs = new RelationalInputStore(r.db);
  const stages = r.db.transaction(() => [
    [
      quotas.createQuotaProjector(stored.events),
      stored.events.find((event) => event.eventType === 'AvailabilityChanged'),
    ],
    [
      generations.createGenerationProjector(stored.events),
      stored.events.find((event) => event.eventType === 'WaitingJobsCancelled'),
    ],
    [inputs.createInputProjector(f.inputs, accepted, false), accepted.events[0]],
  ])();
  const before = databaseState(r.db);
  for (const [apply, event] of stages) {
    assert.throws(() => apply(event), /owning/);
  }
  assert.equal(databaseState(r.db), before);
});
