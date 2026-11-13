import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { fixture, meta, keys, NOW, databaseState } from '../helpers/relational-generation.mjs';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import { relationalPrivateStateEmpty } from '../../server/storage/relational-private-restore.ts';
import {
  setMemberAdmission,
  assertMemberAcceptsJobs,
} from '../../server/generation/queue-policy.ts';
import {
  withAuthenticatedMemberApiKey,
  withAuthenticatedMemberSession,
  authenticateMemberApiKey,
  createMemberSession,
  logoutMemberSession,
} from '../../server/members/authentication.ts';

async function setup(t) {
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { env }),
    members = new RelationalMemberStore(f.db, env);
  const change = (id, acceptingJobs, memberId = 'm1', key = keys.m1) =>
    withAuthenticatedMemberApiKey(
      members,
      key,
      (actor) =>
        members.changeMemberAdmission(
          meta(id, NOW + 10, actor.id),
          { memberId, acceptingJobs },
          actor,
        ),
      env,
    );
  return { ...f, members, env, change };
}
function target(t, f, until) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const owner = new RelationalCommitReplayer(db),
    replay = (stored) => db.transaction(() => owner.replayCommit(stored, f.inputs))();
  for (const { seq } of f.db
    .prepare(
      `SELECT CAST(commit_seq AS TEXT) AS seq FROM commits
    WHERE commit_seq<=? ORDER BY commit_seq`,
    )
    .all(until ?? f.db.prepare('SELECT MAX(commit_seq) AS n FROM commits').get().n)) {
    replay(readRelationalCommit(f.db, seq));
  }
  return { db, replay };
}
function resequence(stored, db, events = stored.events) {
  const versions = new Map();
  stored.command.expectedVersions = {};
  stored.events = events.map((event, eventIndex) => {
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

test('接单命令复用原 owner，完整取消所有本人等待意图并保留运行任务、预扣和认证', async (t) => {
  const f = await setup(t);
  f.db.prepare("UPDATE members SET display_name='same name'").run();
  await f.accept('running', { samples: 2 });
  f.dispatch('running-0');
  await f.accept('waiting', { samples: 2 });
  await f.accept('other', { memberId: 'm2' });
  const view = {
      users: [{ id: 'm1', acceptingJobs: true, jobAdmissionVersion: 0 }],
      jobs: f.db
        .prepare(
          `SELECT j.id,j.request_id AS requestId,r.member_id AS memberId,j.status FROM generation_jobs j
    JOIN generation_requests r ON r.id=j.request_id ORDER BY j.rowid`,
        )
        .all(),
    },
    expected = setMemberAdmission(view, 'm1', false, () => {}, NOW + 10),
    charges = f.db.prepare('SELECT * FROM charges').all(),
    wallets = f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(),
    confirmations = f.db
      .prepare('SELECT id,confirmation FROM generation_requests ORDER BY id')
      .all(),
    epoch = f.db.prepare("SELECT auth_epoch FROM members WHERE id='m1'").get().auth_epoch;
  const stopped = await f.change('stop', false);
  assert.deepEqual(stopped.receipt, expected);
  assert.deepEqual(
    stopped.events.map((e) => e.eventType),
    ['AdmissionChanged', ...expected.cancelledJobIds.map(() => 'WaitingJobsCancelled')],
  );
  assert.equal(f.members.readMember('m1').admissionEpoch, 1);
  assert.equal(f.members.readMember('m1').acceptingJobs, 0);
  for (const id of expected.cancelledJobIds) {
    const job = f.db
      .prepare(
        'SELECT status,error_code,error_status,cancellation_reason FROM generation_jobs WHERE id=?',
      )
      .get(id);
    assert.deepEqual(job, {
      status: 'failed',
      error_code: '管理员已停止此成员接单，未派发任务已取消',
      error_status: 409,
      cancellation_reason: 'member_stopped',
    });
  }
  assert.equal(
    f.db.prepare("SELECT status FROM generation_jobs WHERE id='running-0'").get().status,
    'running',
  );
  assert.equal(
    f.db.prepare("SELECT status FROM generation_jobs WHERE id='other-0'").get().status,
    'queued',
  );
  assert.deepEqual(f.db.prepare('SELECT * FROM charges').all(), charges);
  assert.deepEqual(f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(), wallets);
  assert.deepEqual(
    f.db.prepare('SELECT id,confirmation FROM generation_requests ORDER BY id').all(),
    confirmations,
  );
  assert.equal(
    f.db.prepare("SELECT auth_epoch FROM members WHERE id='m1'").get().auth_epoch,
    epoch,
  );
  assert.ok(await authenticateMemberApiKey(f.members, keys.m1));
  await assert.rejects(f.accept('stopped-new'), /停止此成员接单/);
  await f.publish('running-0', NOW + 11);
  assert.equal(
    f.db.prepare("SELECT status FROM generation_jobs WHERE id='running-1'").get().status,
    'failed',
  );
  await f.change('resume', true);
  assert.equal(f.members.readMember('m1').admissionEpoch, 1);
  assert.throws(
    () => assertMemberAcceptsJobs({ acceptingJobs: true, jobAdmissionVersion: 1 }, 0),
    /剩余任务/,
  );
  assert.throws(() => f.dispatch('waiting-0', NOW + 12));
  await f.accept('fresh', { now: NOW + 12 });
  assert.equal(
    f.db.prepare("SELECT admission_epoch FROM generation_jobs WHERE id='fresh-0'").get()
      .admission_epoch,
    1,
  );
  f.dispatch('fresh-0', NOW + 13);
});

test('同毫秒暂停恢复与同键回执保留原决定，重复停止不增加代次或重取消', async (t) => {
  const f = await setup(t);
  await f.accept('waiting');
  const stopped = await f.change('stop', false);
  await f.change('stop-again', false);
  assert.equal(f.members.readMember('m1').admissionEpoch, 1);
  await f.change('resume', true);
  await f.accept('fresh');
  const before = databaseState(f.db),
    retry = await f.change('stop', false);
  assert.equal(retry.replayed, true);
  assert.deepEqual(retry.receipt, stopped.receipt);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.members.readMember('m1').acceptingJobs, 1);
  await assert.rejects(f.change('stop', true), /conflict|fingerprint/i);
  await f.change('stop-next', false);
  assert.equal(f.members.readMember('m1').admissionEpoch, 2);
  assert.equal(
    f.db.prepare("SELECT status FROM generation_jobs WHERE id='fresh-0'").get().status,
    'failed',
  );
});

test('原回执前重核当前 owner、Key、Cookie 与认证代次，metadata 不能替代授权', async (t) => {
  for (const mode of ['owner', 'key', 'cookie', 'epoch', 'non-owner']) {
    await t.test(mode, async (t) => {
      const f = await setup(t);
      await f.change('stop', false);
      const actor = await authenticateMemberApiKey(f.members, keys.m1),
        session = await createMemberSession(f.members, actor, {}, NOW + 10);
      if (mode === 'owner') {
        f.env.OWNER_USER_ID = 'm2';
      }
      if (mode === 'key') {
        f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'new-key'))();
      }
      if (mode === 'cookie') {
        await logoutMemberSession(f.members, session.token);
      }
      if (mode === 'epoch') {
        f.db.prepare("UPDATE members SET auth_epoch='new-epoch' WHERE id='m1'").run();
      }
      const before = databaseState(f.db);
      if (mode === 'key') {
        assert.equal(await f.change('stop', false), null);
      } else if (mode === 'cookie') {
        assert.equal(
          await withAuthenticatedMemberSession(
            f.members,
            session.token,
            {},
            (member) =>
              f.members.changeMemberAdmission(
                meta('stop', NOW + 10, 'm1'),
                { memberId: 'm1', acceptingJobs: false },
                member,
              ),
            NOW + 11,
          ),
          null,
        );
      } else if (mode === 'epoch') {
        assert.throws(
          () =>
            f.db.transaction(() =>
              f.members.changeMemberAdmission(
                meta('stop', NOW + 10, 'm1'),
                { memberId: 'm1', acceptingJobs: false },
                actor,
              ),
            )(),
          /当前 owner/,
        );
      } else {
        await assert.rejects(
          f.change('stop', false, 'm1', mode === 'non-owner' ? keys.m2 : keys.m1),
          /当前 owner/,
        );
      }
      assert.equal(databaseState(f.db), before);
    });
  }
});

test('接单命令只接受完整布尔意图，移除成员不复活，本地管理仍使用原事务边界', async (t) => {
  const f = await setup(t),
    actor = await authenticateMemberApiKey(f.members, keys.m1);
  assert.throws(
    () =>
      f.members.changeMemberAdmission(
        meta('outside'),
        { memberId: 'm1', acceptingJobs: false },
        actor,
      ),
    /transaction/,
  );
  for (const input of [
    { memberId: 'm1', acceptingJobs: 1 },
    { memberId: 'm1', acceptingJobs: false, extra: true },
    { memberId: 'missing', acceptingJobs: true },
  ]) {
    const before = databaseState(f.db);
    assert.throws(() =>
      f.db.transaction(() =>
        f.members.changeMemberAdmission(meta('invalid', NOW + 10, 'm1'), input, actor),
      )(),
    );
    assert.equal(databaseState(f.db), before);
  }
  assert.throws(
    () =>
      f.db.transaction(() =>
        f.members.changeMemberAdmission(
          meta('system'),
          { memberId: 'm1', acceptingJobs: false },
          null,
        ),
      )(),
    /当前 owner/,
  );
  f.db.prepare("UPDATE members SET status='removed' WHERE id='m2'").run();
  await assert.rejects(f.change('removed', true, 'm2'), /成员不存在/);
  const result = f.db.transaction(() =>
    f.members.changeMemberAdmission(
      { ...meta('local'), actorKind: 'admin', actorId: 'local-admin' },
      { memberId: 'm1', acceptingJobs: false },
      null,
    ),
  )();
  assert.equal(result.receipt.acceptingJobs, false);
});

test('成员与第一任务已更新后第二任务故障，外层捕获仍整笔回滚；末代拒绝不留事实', async (t) => {
  const f = await setup(t);
  await f.accept('waiting', { samples: 2 });
  const before = databaseState(f.db),
    actor = await authenticateMemberApiKey(f.members, keys.m1);
  const prepare = f.db.prepare;
  let updates = 0;
  f.db.prepare = function (sql) {
    const statement = prepare.call(this, sql);
    if (sql.startsWith("UPDATE generation_jobs SET status='failed',completed_at=?")) {
      const run = statement.run.bind(statement);
      statement.run = (...args) => {
        if (++updates === 2) {
          assert.equal(f.members.readMember('m1').acceptingJobs, 0);
          assert.equal(
            prepare.call(f.db, "SELECT status FROM generation_jobs WHERE id='waiting-0'").get()
              .status,
            'failed',
          );
          throw new Error('synthetic second cancellation');
        }
        return run(...args);
      };
    }
    return statement;
  };
  try {
    f.db.transaction(() => {
      assert.throws(
        () =>
          f.members.changeMemberAdmission(
            meta('rollback', NOW + 10, 'm1'),
            { memberId: 'm1', acceptingJobs: false },
            actor,
          ),
        /synthetic second/,
      );
    })();
  } finally {
    f.db.prepare = prepare;
  }
  assert.equal(updates, 2);
  assert.equal(databaseState(f.db), before);
  f.db.prepare("UPDATE members SET admission_epoch=? WHERE id='m1'").run(Number.MAX_SAFE_INTEGER);
  const boundary = databaseState(f.db);
  await assert.rejects(f.change('overflow', false), /安全边界/);
  assert.equal(databaseState(f.db), boundary);
});

test('等待意图持有原预扣或尝试时拒绝停止，不补造退款；恢复接单不恢复 disabled 资格', async (t) => {
  const f = await setup(t);
  await f.accept('inconsistent');
  f.dispatch('inconsistent-0');
  f.db.prepare("UPDATE generation_jobs SET status='pending' WHERE id='inconsistent-0'").run();
  const before = databaseState(f.db);
  await assert.rejects(f.change('stop-inconsistent', false), /不能持有预扣或派发尝试/);
  assert.equal(databaseState(f.db), before);
  f.db.transaction(() => f.members.setMemberStatus(meta('disable'), 'm2', 'disabled'))();
  const epoch = f.db.prepare("SELECT auth_epoch FROM members WHERE id='m2'").get().auth_epoch;
  await f.change('stop-disabled', false, 'm2');
  await f.change('resume-disabled', true, 'm2');
  assert.equal(f.members.readMember('m2').status, 'disabled');
  assert.equal(
    f.db.prepare("SELECT auth_epoch FROM members WHERE id='m2'").get().auth_epoch,
    epoch,
  );
  assert.equal(await authenticateMemberApiKey(f.members, keys.m2), null);
});

test('接单完整提交与真实生成联合恢复，重复零写入且不恢复认证或 ready', async (t) => {
  const f = await setup(t);
  await f.accept('running', { samples: 2 });
  f.dispatch('running-0');
  await f.accept('waiting', { samples: 2 });
  await f.change('stop', false);
  await f.publish('running-0', NOW + 12);
  await f.change('resume', true);
  await f.accept('fresh', { now: NOW + 14 });
  f.dispatch('fresh-0', NOW + 15);
  await f.fail('fresh-0', { aborted: false });
  const r = target(t, f);
  for (const table of [
    'generation_requests',
    'generation_jobs',
    'generation_attempts',
    'charges',
    'wallets',
    'account_quota_state',
    'operating_state',
    'domain_events',
    'commits',
    'idempotency_keys',
    'stream_heads',
    'projection_state',
  ]) {
    assert.deepEqual(
      r.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
      f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
      table,
    );
  }
  assert.deepEqual(
    r.db
      .prepare(
        'SELECT id,status,accepting_jobs,admission_epoch,source_event_id FROM members ORDER BY id',
      )
      .all(),
    f.db
      .prepare(
        'SELECT id,status,accepting_jobs,admission_epoch,source_event_id FROM members ORDER BY id',
      )
      .all(),
  );
  assert.equal(relationalPrivateStateEmpty(r.db), true);
  assert.throws(() => assertRelationalStorageReady(r.db), /incomplete/);
  const once = databaseState(r.db);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    r.replay(readRelationalCommit(f.db, seq));
  }
  assert.equal(databaseState(r.db), once);
});

test('原批次收尾的 member_stopped 原因保留 batch_stopped 语义，不要求伪造接单决定', async (t) => {
  const f = await setup(t);
  await f.accept('batch', { samples: 2 });
  f.dispatch('batch-0');
  f.db.prepare("UPDATE members SET auth_epoch=NULL WHERE id='m1'").run();
  const completed = await f.publish('batch-0', NOW + 12),
    cancelled = completed.events.find((event) => event.eventType === 'WaitingJobsCancelled');
  assert.equal(cancelled.payload.reason, 'member_stopped');
  assert.equal(cancelled.payload.cancellationReason, 'batch_stopped');
  const r = target(t, f),
    before = databaseState(r.db);
  assert.equal(
    r.db.prepare("SELECT status FROM generation_jobs WHERE id='batch-1'").get().status,
    'failed',
  );
  r.replay(readRelationalCommit(f.db, completed.commitSeq));
  assert.equal(databaseState(r.db), before);
});

test('普通成员原请求取消的共享 cancelledJobIds 回执不属于接单变更，原序恢复与重领保持', async (t) => {
  const f = await setup(t);
  await f.accept('cancel', { memberId: 'm2', samples: 2 });
  const cancel = () =>
    withAuthenticatedMemberApiKey(
      f.members,
      keys.m2,
      (actor) =>
        f.generations.cancelWaitingGeneration(meta('request-cancel', NOW + 5, actor.id), 'cancel'),
      {},
    );
  const result = await cancel();
  assert.deepEqual(result.receipt, {
    requestId: 'cancel',
    cancelledJobIds: ['cancel-0', 'cancel-1'],
  });
  assert.equal(f.members.readMember('m2').acceptingJobs, 1);
  assert.equal(
    result.events.some((event) => event.eventType === 'AdmissionChanged'),
    false,
  );
  assert.equal(result.events[0].payload.cancellationReason, 'batch_stopped');
  const before = databaseState(f.db);
  assert.equal((await cancel()).replayed, true);
  assert.equal(databaseState(f.db), before);
  const r = target(t, f),
    replayed = readRelationalCommit(r.db, result.commitSeq),
    restored = databaseState(r.db);
  assert.deepEqual(replayed.command.receipt, result.receipt);
  assert.equal(replayed.command.actorKind, 'member');
  assert.equal(replayed.command.actorId, 'm2');
  r.replay(readRelationalCommit(f.db, result.commitSeq));
  assert.equal(databaseState(r.db), restored);
});

test('完整回放拒绝遗漏、伪造、跨成员与逆序取消，异常外层捕获不留半笔', async (t) => {
  const f = await setup(t);
  await f.accept('waiting', { samples: 2 });
  const stopped = await f.change('stop', false),
    stored = readRelationalCommit(f.db, stopped.commitSeq);
  const cases = {
    missing: (s) => s.events.pop(),
    epoch: (s) => {
      s.events[0].payload.admissionEpoch = 2;
    },
    reusedEpoch: (s) => {
      s.events[0].payload.admissionEpoch = 0;
    },
    member: (s) => {
      s.events[1].memberId = 'm2';
    },
    stream: (s) => {
      s.events[1].streamId = 'request:other';
    },
    duplicate: (s) => {
      s.events[1].payload.jobIds.push(s.events[1].payload.jobIds[0]);
    },
    reasonCode: (s) => {
      s.events[1].payload.cancellationReason = 'batch_stopped';
    },
    reason: (s) => {
      s.events[1].payload.reason = 'forged-not-the-original-stop-decision';
    },
    noStopEmpty: (s) => {
      s.events = [s.events[1]];
      s.events[0].payload.jobIds = [];
    },
    noStopNonempty: (s) => {
      s.events.shift();
    },
    noStopOrdinaryCancellation: (s) => {
      s.events.shift();
      for (const event of s.events) {
        event.payload.cancellationReason = 'batch_stopped';
      }
    },
    emptyLinkedCancellation: (s) => {
      s.events[1].payload.jobIds = [];
    },
    time: (s) => {
      s.events[1].occurredAt += 1;
    },
    receiptIds: (s) => {
      s.command.receipt.cancelledJobIds.pop();
    },
    receiptState: (s) => {
      s.command.receipt.acceptingJobs = true;
    },
    receiptMember: (s) => {
      s.command.receipt.id = 'm2';
    },
    order: (s) => {
      s.events.push(s.events.shift());
    },
    resumeBeforeCancel: (s) => {
      s.events.splice(1, 0, {
        ...structuredClone(s.events[0]),
        eventId: 'forged-resume',
        payload: { memberId: 'm1', acceptingJobs: true, admissionEpoch: 1 },
      });
    },
  };
  for (const [name, mutate] of Object.entries(cases)) {
    await t.test(name, (t) => {
      const r = target(t, f, Number(stopped.commitSeq) - 1),
        bad = structuredClone(stored),
        before = databaseState(r.db);
      mutate(bad);
      resequence(bad, r.db);
      r.db.transaction(() => {
        assert.throws(() => r.replay(bad));
      })();
      assert.equal(databaseState(r.db), before);
    });
  }
});
