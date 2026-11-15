import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { fixture, meta, keys, NOW, databaseState } from '../helpers/relational-generation.mjs';
import { RelationalRequestStore } from '../../server/storage/relational-request-store.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import {
  relationalSnapshotSha256,
  restoreRelationalPrivateSnapshot,
} from '../../server/storage/relational-private-restore.ts';

test('共享计数取当前稳定组成员和薄状态，Key转归不改变计数且query_only下全表零写', async (t) => {
  const f = await fixture(t, { seconds: 0 }),
    requests = new RelationalRequestStore(f.db);
  await f.accept('running');
  assert.ok(f.dispatch('running-0').dispatch);
  await f.accept('batch', { samples: 3 });
  await f.accept('second', { memberId: 'm2' });
  const expected = { running: 1, queued: 2, pending: 2, recentFailures: 0 };
  assert.deepEqual(requests.readSharedJobCounts('m1', NOW + 3), expected);
  f.db.transaction(() => {
    f.members.replaceMemberApiKey('m1', 'new-stable-member-key');
    f.members.replaceMemberApiKey('m2', keys.m1);
  })();
  const before = databaseState(f.db),
    statements = [],
    prepare = f.db.prepare;
  f.db.pragma('query_only=ON');
  f.db.prepare = function (sql) {
    statements.push(sql);
    return prepare.call(this, sql);
  };
  try {
    assert.deepEqual(requests.readSharedJobCounts('m1', NOW + 3), expected);
    assert.deepEqual(requests.readSharedJobCounts('m2', NOW + 3), expected);
  } finally {
    f.db.prepare = prepare;
    f.db.pragma('query_only=OFF');
  }
  assert.equal(databaseState(f.db), before);
  assert.equal(
    statements.some((sql) =>
      /api_keys|generation_inputs|input_assets|domain_events|\bbody\b|\bbytes\b/.test(sql),
    ),
    false,
  );
  const aggregate = statements.find((sql) => sql.includes('COUNT(*) FILTER'));
  const plan = f.db.prepare('EXPLAIN QUERY PLAN ' + aggregate).all(NOW - 3600000, NOW);
  assert.match(JSON.stringify(plan), /SEARCH r USING INDEX idx_requests_member/);
  // SQLite 可选原 request/sample 唯一索引；要求按 request_id 搜索，不强迫同功能索引名。
  assert.match(JSON.stringify(plan), /SEARCH j USING INDEX [^(]+ \(request_id=\?\)/);
  assert.equal(
    plan.some((step) => /SCAN (?:r|j)(?:\s|$)/.test(step.detail)),
    false,
  );
  assert.throws(() => requests.readSharedJobCounts('missing', NOW), { statusCode: 401 });
});

test('失败窗口包含真实时点0与两端，未知薄行不借header补0，退休不再进入可见统计', async (t) => {
  const f = await fixture(t, { openingAt: 0, seconds: 0 }),
    requests = new RelationalRequestStore(f.db);
  await f.accept('zero', { now: 0 });
  assert.ok(f.dispatch('zero-0', 0).dispatch);
  f.fail('zero-0', { aborted: false }, 0);
  assert.equal(requests.readSharedJobCounts('m1', 0).recentFailures, 1);
  assert.equal(requests.readSharedJobCounts('m1', 3600000).recentFailures, 1);
  assert.equal(requests.readSharedJobCounts('m1', 3600001).recentFailures, 0);
  const before = databaseState(f.db),
    stop = new Error('rollback thin-row fault');
  assert.throws(
    () =>
      f.db.transaction(() => {
        // 薄行缺来源的故障输入，仅核未知表示，不冒称已经通过 Opening/ready 导入。
        f.db.prepare("UPDATE generation_jobs SET completed_at=NULL WHERE id='zero-0'").run();
        assert.equal(requests.readSharedJobCounts('m1', 0).recentFailures, null);
        throw stop;
      })(),
    (error) => error === stop,
  );
  assert.equal(databaseState(f.db), before);
  f.db.transaction(() =>
    f.generations.closeGenerationReview(meta('close-zero-review', 300000), 'zero-0'),
  )();
  f.db.transaction(() =>
    f.generations.retireJobHistory(
      { ...meta('retire-zero', 300001), actorKind: 'admin' },
      ['zero-0'],
      null,
      new Map(),
    ),
  )();
  assert.equal(requests.readSharedJobCounts('m1', 3600000).recentFailures, 0);
});

test('当前组移除通过原权益命令后排除旧失败，不把未归组钱包或旧Key当成员集合', async (t) => {
  const f = await fixture(t, { seconds: 0 }),
    requests = new RelationalRequestStore(f.db),
    quotas = new RelationalQuotaStore(f.db);
  await f.accept('other-failure', { memberId: 'm2' });
  assert.ok(f.dispatch('other-failure-0').dispatch);
  f.fail('other-failure-0', { aborted: false });
  assert.equal(requests.readSharedJobCounts('m1', NOW + 3).recentFailures, 1);
  f.db.transaction(() =>
    f.generations.closeGenerationReview(
      meta('close-other-review', NOW + 300002),
      'other-failure-0',
    ),
  )();
  f.db.transaction(() =>
    quotas.manageMemberQuota(
      { ...meta('remove-group-member', NOW + 300003), actorKind: 'admin' },
      { action: 'delete-member', userId: 'm2', transferToUserId: 'm1', expectedConfigVersion: 1 },
      null,
      new Map(),
    ),
  )();
  const before = databaseState(f.db);
  assert.equal(requests.readSharedJobCounts('m1', NOW + 300004).recentFailures, 0);
  assert.equal(databaseState(f.db), before);
});

test('重开和原完整commit加选定private快照恢复保持共享薄统计，业务回放无网络或认证复活', async (t) => {
  const cleanup = [];
  let reopened, target;
  t.after(async () => {
    reopened?.close();
    target?.close();
    for (const run of cleanup.reverse()) {
      await run();
    }
  });
  const f = await fixture({ after: (run) => cleanup.push(run) }, { seconds: 0 });
  await f.accept('failed');
  assert.ok(f.dispatch('failed-0').dispatch);
  f.fail('failed-0', { aborted: false });
  const expected = new RelationalRequestStore(f.db).readSharedJobCounts('m1', NOW + 3),
    sourcePath = path.join(f.dir, 'shared-selected.sqlite');
  await f.db.backup(sourcePath);
  const sealed = new Database(sourcePath);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  reopened = new Database(f.db.name);
  target = new Database(':memory:');
  assert.deepEqual(
    new RelationalRequestStore(reopened).readSharedJobCounts('m1', NOW + 3),
    expected,
  );
  initializeRelationalStorage(target, 'generation-history');
  const replayer = new RelationalCommitReplayer(target),
    source = new RelationalInputStore(f.db);
  t.mock.method(globalThis, 'fetch', () => assert.fail('business replay cannot fetch'));
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), source))();
  }
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
  target.transaction(() =>
    restoreRelationalPrivateSnapshot(target, sourcePath, relationalSnapshotSha256(sourcePath)),
  )();
  const before = databaseState(target);
  assert.deepEqual(new RelationalRequestStore(target).readSharedJobCounts('m1', NOW + 3), expected);
  assert.equal(databaseState(target), before);
});
