import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import {
  appendRelationalCommit,
  readRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import { describeStoredResult, publishStoredResult } from '../../server/storage/result-files.ts';
import { parseAmount, formatAmount } from '../../server/quota/resource-amounts.ts';
import { quoteFairRequest } from '../../server/quota/fair-share.ts';

import {
  NOW,
  keys,
  image,
  meta,
  databaseState,
  fixture,
} from '../helpers/relational-generation.mjs';

test('T4 真实授权受理、派发预扣、文件发布及任务/收费终态在同一关系事务闭环', async (t) => {
  const f = await fixture(t),
    { db, generations: g } = f;
  await f.accept('first');
  assert.deepEqual(
    db.prepare('SELECT status,CAST(queue_seq AS TEXT) AS seq FROM generation_jobs').get(),
    { status: 'queued', seq: '1' },
  );
  const before = g.readWallet('a', 'm1');
  const started = f.dispatch('first-0');
  assert.equal(started.dispatch.pst, 'synthetic-upstream-pst');
  const charge = g.readCharge('first-0');
  assert.equal(charge.status, 'reserved');
  assert.equal(
    g.readWallet('a', 'm1').anlasFixed,
    formatAmount(parseAmount(before.anlasFixed) - parseAmount(charge.anlasFixed)),
  );
  const checkpoint = db.serialize();
  const retry = f.dispatch('first-0');
  assert.equal(retry.replayed, true);
  assert.equal(retry.dispatch, null);
  assert.deepEqual(db.serialize(), checkpoint);
  const complete = await f.publish('first-0');
  assert.equal(complete.receipt.artifactId, 'artifact-first-0');
  assert.equal(g.readCharge('first-0').status, 'settled');
  assert.equal(db.prepare('SELECT status,artifact_id FROM generation_jobs').get().status, 'done');
  assert.equal(
    db.prepare('SELECT pending_fixed_debit AS n FROM account_quota_state').get().n,
    charge.anlasFixed,
  );
  assert.equal(
    db.prepare('SELECT actual_consumption FROM generation_attempts').get().actual_consumption,
    null,
  );
  const after = db.serialize();
  const descriptor = describeStoredResult('artifact-first-0', 'generate', {
    mimeType: 'image/webp',
    buffer: image,
  });
  db.transaction(() =>
    assert.equal(
      g.completeGeneration(meta('complete-first-0', NOW + 4), 'first-0', descriptor).replayed,
      true,
    ),
  )();
  assert.deepEqual(db.serialize(), after);
  const publicText =
    JSON.stringify(db.prepare('SELECT payload FROM domain_events').all()) +
    JSON.stringify(db.prepare('SELECT receipt FROM commits').all());
  for (const secret of [
    'private generation prompt',
    keys.m1,
    'synthetic-upstream-pst',
    image.toString('base64'),
  ]) {
    assert.equal(publicText.includes(secret), false);
  }
  assert.deepEqual(db.pragma('foreign_key_check'), []);
  assert.throws(() => assertRelationalStorageReady(db), /incomplete/);
});

test('派发重新核对当前授权、观察代次、私有凭据与物理槽，拒绝时没有预扣或半笔回执', async (t) => {
  const f = await fixture(t),
    { db } = f;
  await f.accept('guarded');
  const unchanged = (run, pattern) => {
    const before = db.serialize();
    assert.throws(run, pattern);
    assert.deepEqual(db.serialize(), before);
  };
  for (const controls of [
    { dispatchEnabled: () => false, physicalAccountLoad: () => 0 },
    { dispatchEnabled: () => true, physicalAccountLoad: () => 1 },
  ]) {
    const before = db.serialize();
    assert.equal(f.dispatch('guarded-0', NOW + 1, controls).dispatch, null);
    assert.deepEqual(db.serialize(), before);
  }
  unchanged(
    () =>
      f.dispatch('guarded-0', NOW + 1, {
        dispatchEnabled: () => true,
        physicalAccountLoad: () => NaN,
      }),
    /物理执行槽/,
  );
  db.prepare('UPDATE members SET admission_epoch=1 WHERE id=?').run('m1');
  unchanged(() => f.dispatch('guarded-0'), /管理员取消/);
  db.prepare('UPDATE members SET admission_epoch=0,auth_epoch=NULL WHERE id=?').run('m1');
  unchanged(() => f.dispatch('guarded-0'), /授权/);
  db.prepare('UPDATE members SET auth_epoch=? WHERE id=?').run('restored-private-epoch', 'm1');
  db.prepare('UPDATE upstream_accounts SET credential_epoch=2').run();
  unchanged(() => f.dispatch('guarded-0'), /额度基线/);
  db.prepare('UPDATE upstream_accounts SET credential_epoch=1').run();
  db.prepare('DELETE FROM upstream_credentials').run();
  unchanged(() => f.dispatch('guarded-0'), /私有凭据/);
  db.prepare('INSERT INTO upstream_credentials(account_id,pst) VALUES(?,?)').run(
    'a',
    'synthetic-upstream-pst',
  );
  db.prepare('UPDATE account_quota_state SET observed_at=?').run(NOW - 600001);
  unchanged(() => f.dispatch('guarded-0'), /stale/);
  db.prepare('UPDATE account_quota_state SET observed_at=?').run(NOW);
  db.prepare('UPDATE upstream_accounts SET enabled=0').run();
  unchanged(() => f.dispatch('guarded-0'), /inactive_opus/);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM charges').get().n, 0);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM generation_attempts').get().n, 0);
});

test('真实受理复用原确认指纹、规则版本、当前单张与整批费用上限', async (t) => {
  const f = await fixture(t),
    { db } = f;
  const before = db.serialize();
  await assert.rejects(
    f.accept('wrong-intent', {
      member: true,
      editConfirmation: (c) => ({ ...c, requestFingerprint: 'a'.repeat(64) }),
    }),
    /确认|报价/,
  );
  assert.deepEqual(db.serialize(), before);
  await assert.rejects(
    f.accept('limited', {
      member: true,
      samples: 2,
      editConfirmation: (c) => ({ ...c, maxPerJob: { ...c.maxPerJob, anlasFixed: '0' } }),
    }),
    /费用超过/,
  );
  assert.ok(db.serialize().equals(before));
  await assert.rejects(
    f.accept('limited-batch', {
      member: true,
      samples: 2,
      editConfirmation: (c) => ({ ...c, maxTotal: { ...c.maxTotal, anlasFixed: '2' } }),
    }),
    /费用超过/,
  );
  assert.ok(db.serialize().equals(before));
  await f.accept('versioned', { member: true });
  db.prepare('UPDATE sharing_policy SET auto_anlas_fallback=1').run();
  const changed = db.serialize();
  assert.throws(() => f.dispatch('versioned-0'), /生成规则已变化/);
  assert.deepEqual(db.serialize(), changed);
});

test('预扣、尝试和终态 SQL 故障均回滚整笔事实、钱包、回执与任务，可用同一命令重试', async (t) => {
  const f = await fixture(t),
    { db, generations: g } = f;
  await f.accept('atomic');
  db.exec(
    `CREATE TEMP TRIGGER fail_attempt BEFORE INSERT ON generation_attempts BEGIN SELECT RAISE(ABORT,'attempt fault'); END`,
  );
  let before = databaseState(db);
  db.transaction(() => assert.throws(() => f.dispatch('atomic-0'), /attempt fault/))();
  assert.equal(databaseState(db), before);
  db.exec('DROP TRIGGER fail_attempt');
  f.dispatch('atomic-0');
  const descriptor = describeStoredResult('artifact-atomic-0', 'generate', {
    mimeType: 'image/webp',
    buffer: image,
  });
  db.transaction(() =>
    g.prepareGenerationResult(
      meta('prepare-atomic-0', NOW + 3),
      'atomic-0',
      descriptor,
      f.response(),
    ),
  )();
  await publishStoredResult(f.dir, descriptor, image);
  db.exec(
    `CREATE TEMP TRIGGER fail_terminal BEFORE UPDATE OF status ON generation_jobs WHEN NEW.status='done' BEGIN SELECT RAISE(ABORT,'terminal fault'); END`,
  );
  before = databaseState(db);
  db.transaction(() =>
    assert.throws(
      () => g.completeGeneration(meta('complete-atomic-0', NOW + 4), 'atomic-0', descriptor),
      /terminal fault/,
    ),
  )();
  assert.equal(databaseState(db), before);
  assert.equal(g.readCharge('atomic-0').status, 'reserved');
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM artifacts').get().n, 0);
  db.exec('DROP TRIGGER fail_terminal');
  db.transaction(() =>
    g.completeGeneration(meta('complete-atomic-0', NOW + 4), 'atomic-0', descriptor),
  )();
  assert.equal(g.readCharge('atomic-0').status, 'settled');
  assert.deepEqual(db.pragma('foreign_key_check'), []);
});

test('未知失败按原池退款一次并开启原五分钟窗口，关闭核对不虚构实际消费', async (t) => {
  const f = await fixture(t),
    { db, generations: g } = f;
  await f.accept('unknown', { samples: 3 });
  const wallet = g.readWallet('a', 'm1');
  f.dispatch('unknown-0');
  db.exec(
    `CREATE TEMP TRIGGER fail_refund BEFORE UPDATE OF status ON charges WHEN NEW.status='refunded' BEGIN SELECT RAISE(ABORT,'refund fault'); END`,
  );
  const before = databaseState(db);
  db.transaction(() => assert.throws(() => f.fail('unknown-0'), /refund fault/))();
  assert.equal(databaseState(db), before);
  db.exec('DROP TRIGGER fail_refund');
  const failed = f.fail('unknown-0');
  assert.deepEqual(g.readWallet('a', 'm1'), wallet);
  assert.equal(g.readCharge('unknown-0').status, 'refunded');
  const attempt = db.prepare('SELECT * FROM generation_attempts').get();
  assert.equal(attempt.review_status, 'pending');
  assert.equal(attempt.first_unknown_at, NOW + 2);
  assert.equal(attempt.review_deadline_at, NOW + 300002);
  assert.equal(attempt.actual_consumption, null);
  const refund = failed.events.find((e) => e.eventType === 'GenerationRefunded');
  const review = failed.events.find((e) => e.eventType === 'ReviewOpened');
  assert.equal(refund.payload.lossEventId, review.eventId);
  assert.equal(review.payload.lossEventId, review.eventId);
  assert.deepEqual(
    db
      .prepare('SELECT status FROM generation_jobs ORDER BY sample_index')
      .all()
      .map((j) => j.status),
    ['failed', 'failed', 'failed'],
  );
  const failedBytes = db.serialize();
  assert.equal(f.fail('unknown-0').replayed, true);
  assert.deepEqual(db.serialize(), failedBytes);
  assert.throws(
    () =>
      db.transaction(() => g.closeGenerationReview(meta('too-early', NOW + 300001), 'unknown-0'))(),
    /窗口/,
  );
  db.transaction(() => g.closeGenerationReview(meta('close-review', NOW + 300002), 'unknown-0'))();
  assert.deepEqual(
    db.prepare('SELECT review_status,actual_consumption FROM generation_attempts').get(),
    { review_status: 'estimated_closed', actual_consumption: null },
  );
  assert.deepEqual(g.readWallet('a', 'm1'), wallet);
  assert.equal(f.dispatch('unknown-0', NOW + 300003).dispatch, null);
  assert.throws(
    () =>
      db.transaction(() =>
        g.dispatchGeneration(
          meta('resend-unknown', NOW + 300003),
          { jobId: 'unknown-0', accountId: 'a', attemptId: 'second-attempt' },
          f.controls,
        ),
      )(),
    /已有收费|已派发/,
  );
});

test('明确未发送只退原预扣，无风险核对；等待取消不制造尝试且不能取消运行任务', async (t) => {
  const f = await fixture(t),
    { db, generations: g } = f;
  await f.accept('cancel', { samples: 3 });
  const wallet = g.readWallet('a', 'm1');
  f.dispatch('cancel-0');
  assert.throws(
    () =>
      db.transaction(() =>
        g.failGeneration(meta('member-fail-running', NOW + 2, 'm1'), 'cancel-0', {
          errorCode: 'cancel',
          errorStatus: 409,
        }),
      )(),
    /已经派发/,
  );
  assert.throws(
    () =>
      db.transaction(() =>
        g.cancelWaitingGeneration(meta('other-cancel', NOW + 2, 'm2'), 'cancel'),
      )(),
    /归属/,
  );
  db.transaction(() =>
    g.cancelWaitingGeneration(meta('cancel-waiting', NOW + 2, 'm1'), 'cancel'),
  )();
  assert.deepEqual(
    db
      .prepare('SELECT status FROM generation_jobs ORDER BY sample_index')
      .all()
      .map((j) => j.status),
    ['running', 'failed', 'failed'],
  );
  const failed = f.fail('cancel-0', { evidence: { requestStarted: false } });
  assert.equal(
    failed.events.some((e) => e.eventType === 'ReviewOpened'),
    false,
  );
  assert.equal(
    db.prepare('SELECT review_status FROM generation_attempts').get().review_status,
    null,
  );
  assert.deepEqual(g.readWallet('a', 'm1'), wallet);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM charges').get().n, 1);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM generation_attempts').get().n, 1);
});

test('批次 pending 无预扣或序号，完成后加入 B 后方，沿用原期限且不复活停止意图', async (t) => {
  const f = await fixture(t),
    { db, generations: g } = f;
  await f.accept('A', { samples: 2, deadlineAt: NOW + 50000 });
  await f.accept('B', { memberId: 'm2' });
  const jobs = () =>
    db
      .prepare(
        'SELECT id,status,CAST(queue_seq AS TEXT) AS seq,queue_expires_at AS expires FROM generation_jobs ORDER BY id',
      )
      .all();
  assert.deepEqual(
    jobs().map((j) => [j.id, j.status, j.seq]),
    [
      ['A-0', 'queued', '1'],
      ['A-1', 'pending', null],
      ['B-0', 'queued', '2'],
    ],
  );
  f.dispatch('A-0');
  await f.publish('A-0');
  assert.deepEqual(
    jobs()
      .filter((j) => j.status === 'queued')
      .sort((a, b) => Number(BigInt(a.seq) - BigInt(b.seq)))
      .map((j) => j.id),
    ['B-0', 'A-1'],
  );
  assert.equal(jobs().find((j) => j.id === 'A-1').expires, NOW + 50000);
  assert.equal(g.readCharge('A-1'), null);
  f.dispatch('B-0', NOW + 10005);
  db.prepare("UPDATE members SET accepting_jobs=0,admission_epoch=1 WHERE id='m1'").run();
  await f.publish('B-0', NOW + 10007);
  assert.throws(() => f.dispatch('A-1', NOW + 20009), /停止|取消/);
  db.prepare("UPDATE members SET accepting_jobs=1 WHERE id='m1'").run();
  assert.throws(() => f.dispatch('A-1', NOW + 20009), /取消/);
  db.transaction(() => g.cancelWaitingGeneration(meta('cancel-old-A', NOW + 20010, 'm1'), 'A'))();
  assert.equal(g.readCharge('A-1'), null);
});

test('物理结束和逻辑失败复用同一次退避，逻辑退款仍不能释放未结束物理槽', async (t) => {
  const f = await fixture(t),
    { db, generations: g } = f;
  await f.accept('running');
  await f.accept('waiting', { memberId: 'm2' });
  f.dispatch('running-0');
  f.fail('running-0');
  const before = db.serialize();
  assert.equal(
    f.dispatch('waiting-0', NOW + 3, { dispatchEnabled: () => true, physicalAccountLoad: () => 1 })
      .dispatch,
    null,
  );
  assert.deepEqual(db.serialize(), before);
  const response = { ...f.response(NOW + 4), upstreamHttpStatus: 503 };
  db.transaction(() =>
    g.recordGenerationResponse(meta('physical-end', NOW + 4), 'running-0', response, {
      kind: 'ordinary',
    }),
  )();
  assert.equal(
    db.prepare('SELECT ordinary_strikes FROM upstream_accounts').get().ordinary_strikes,
    1,
  );
  const until = db.prepare('SELECT cooldown_until AS n FROM upstream_accounts').get().n;
  db.transaction(() =>
    g.recordGenerationResponse(meta('repeat-physical-end', NOW + 5), 'running-0', response, {
      kind: 'ordinary',
    }),
  )();
  assert.equal(db.prepare('SELECT cooldown_until AS n FROM upstream_accounts').get().n, until);
  assert.equal(f.dispatch('waiting-0', NOW + 10003).dispatch, null);
  assert.ok(f.dispatch('waiting-0', NOW + 10004).dispatch);
  db.transaction(() =>
    g.recordGenerationResponse(
      meta('second-end', NOW + 10006),
      'waiting-0',
      f.response(NOW + 10006),
      { kind: 'ordinary' },
    ),
  )();
  f.fail('waiting-0', {}, NOW + 10007);
  assert.equal(
    db.prepare('SELECT ordinary_strikes FROM upstream_accounts').get().ordinary_strikes,
    2,
  );
  assert.equal(
    db.prepare('SELECT cooldown_until AS n FROM upstream_accounts').get().n,
    NOW + 30006,
  );
});

test('迟到结果保留失败与退款、不复活余图，旧凭据结果不能覆盖新凭据冷却', async (t) => {
  const f = await fixture(t),
    { db, generations: g } = f;
  await f.accept('late', { samples: 2 });
  const wallet = g.readWallet('a', 'm1');
  f.dispatch('late-0');
  f.fail('late-0');
  db.prepare(
    'UPDATE upstream_accounts SET credential_epoch=2,cooldown_until=?,ordinary_strikes=3',
  ).run(NOW + 60000);
  const result = await f.publish('late-0', NOW + 6);
  assert.equal(result.receipt.refundRetained, true);
  assert.equal(g.readCharge('late-0').status, 'refunded');
  assert.deepEqual(g.readWallet('a', 'm1'), wallet);
  assert.deepEqual(
    db
      .prepare(
        'SELECT status,completed_at,late_result_at FROM generation_jobs ORDER BY sample_index',
      )
      .all(),
    [
      { status: 'failed', completed_at: NOW + 2, late_result_at: NOW + 7 },
      { status: 'failed', completed_at: NOW + 2, late_result_at: null },
    ],
  );
  assert.deepEqual(
    db.prepare('SELECT review_status,actual_consumption FROM generation_attempts').get(),
    { review_status: 'result_recovered', actual_consumption: null },
  );
  assert.deepEqual(
    db
      .prepare('SELECT credential_epoch,cooldown_until,ordinary_strikes FROM upstream_accounts')
      .get(),
    { credential_epoch: 2, cooldown_until: NOW + 60000, ordinary_strikes: 3 },
  );
});

test('V5、fixed、purchased 与 fallback 都使用原资源池精确结算或退款', async (t) => {
  for (const [name, options, request, mode, pool] of [
    ['v5', {}, { model: 'nai-diffusion-5-full', steps: 23 }, 'v5', 'v5'],
    [
      'purchased',
      { fixed: '0', purchased: '100', officialFixed: '0', officialPurchased: '200' },
      { allowPurchased: true },
      'anlas',
      'anlasPurchased',
    ],
    [
      'fallback',
      { v5: '0', secondV5: '40', fallback: true },
      { model: 'nai-diffusion-5-full', steps: 23 },
      'anlas_fallback',
      'anlasFixed',
    ],
  ]) {
    await t.test(name, async (t) => {
      const f = await fixture(t, options),
        { db, generations: g } = f;
      if (name === 'purchased') {
        await f.accept('unauthorized');
        const before = db.serialize();
        assert.throws(() => f.dispatch('unauthorized-0'), /明确授权/);
        assert.ok(db.serialize().equals(before));
        db.transaction(() =>
          g.cancelWaitingGeneration(meta('cancel-unauthorized', NOW + 2, 'm1'), 'unauthorized'),
        )();
      }
      await f.accept(name, request);
      const wallet = g.readWallet('a', 'm1');
      f.dispatch(`${name}-0`);
      const c = g.readCharge(`${name}-0`);
      assert.equal(c.billingMode, mode);
      assert.ok(parseAmount(c[pool]) > 0n);
      for (const r of ['v5', 'anlasFixed', 'anlasPurchased']) {
        assert.equal(
          g.readWallet('a', 'm1')[r],
          formatAmount(parseAmount(wallet[r]) - parseAmount(c[r])),
        );
      }
      await f.publish(`${name}-0`);
      const q = db.prepare('SELECT * FROM account_quota_state').get();
      if (mode === 'anlas_fallback') {
        assert.ok(parseAmount(c.sharedV5Reserved) > 0n);
        assert.equal(q.fallback_fixed, c.anlasFixed);
        assert.equal(q.pending_fallback_fixed, c.anlasFixed);
        assert.equal(q.pending_fallback_v5, c.sharedV5Reserved);
        assert.equal(q.shared_v5_debt, c.sharedV5Reserved);
        assert.equal(q.pending_fixed_debit, '0');
        assert.equal(
          db
            .prepare('SELECT json_array_length(upstream_candidates) AS n FROM generation_attempts')
            .get().n,
          2,
        );
      } else {
        assert.equal(q.pending_v5_debit, c.v5);
        assert.equal(q.pending_purchased_debit, c.anlasPurchased);
      }
      assert.equal(
        db.prepare('SELECT CAST(observed_sequence AS TEXT) AS n FROM account_quota_state').get().n,
        '9007199254740993',
      );
      await f.accept(`${name}-refund`, request);
      const beforeRefund = g.readWallet('a', 'm1');
      f.dispatch(`${name}-refund-0`, NOW + 10005);
      f.fail(`${name}-refund-0`, { evidence: { requestStarted: false } }, NOW + 10006);
      assert.deepEqual(g.readWallet('a', 'm1'), beforeRefund);
      assert.equal(g.readCharge(`${name}-refund-0`).status, 'refunded');
      assert.deepEqual(db.pragma('foreign_key_check'), []);
    });
  }
});

test('免费迟到完整结果仍计滚动限额，重复结果不重复计数', async (t) => {
  const f = await fixture(t, { freeLimit: 1 }),
    { db, generations: g } = f;
  await f.accept('free', { steps: 23, samples: 2 });
  const wallet = g.readWallet('a', 'm1');
  f.dispatch('free-0');
  assert.equal(g.readCharge('free-0').billingMode, 'free');
  f.fail('free-0');
  await f.publish('free-0');
  assert.deepEqual(g.readWallet('a', 'm1'), wallet);
  assert.deepEqual(
    db
      .prepare(
        'SELECT free_succeeded_at,free_warning_threshold,status FROM generation_jobs ORDER BY sample_index',
      )
      .all(),
    [
      { free_succeeded_at: NOW + 4, free_warning_threshold: 1, status: 'failed' },
      { free_succeeded_at: null, free_warning_threshold: null, status: 'failed' },
    ],
  );
  await f.accept('over-free-limit', { steps: 23 });
  const before = db.serialize();
  assert.throws(() => f.dispatch('over-free-limit-0', NOW + 10005), /免费/);
  assert.ok(db.serialize().equals(before));
  const descriptor = describeStoredResult('artifact-free-0', 'generate', {
    mimeType: 'image/webp',
    buffer: image,
  });
  assert.throws(
    () =>
      db.transaction(() =>
        g.completeGeneration(meta('duplicate-late', NOW + 10006), 'free-0', descriptor),
      )(),
    /终结/,
  );
  assert.equal(
    db
      .prepare('SELECT COUNT(*) AS n FROM generation_jobs WHERE free_succeeded_at IS NOT NULL')
      .get().n,
    1,
  );
});

test('队列序号和大额 12 位金额穿过关系事务不经 number 舍入；原期限不因下一张激活延长', async (t) => {
  const amount = '9007199254740993.123456789012';
  const f = await fixture(t, { fixed: amount, officialFixed: '18014398509481986.246913578024' }),
    { db, generations: g } = f;
  await f.accept('big', { samples: 2, deadlineAt: NOW + 50000 });
  db.prepare('UPDATE generation_jobs SET queue_seq=? WHERE id=?').run(9007199254740993n, 'big-0');
  await f.accept('tail', { memberId: 'm2' });
  assert.equal(
    db.prepare('SELECT CAST(queue_seq AS TEXT) AS n FROM generation_jobs WHERE id=?').get('tail-0')
      .n,
    '9007199254740994',
  );
  f.dispatch('big-0');
  const c = g.readCharge('big-0');
  assert.equal(
    g.readWallet('a', 'm1').anlasFixed,
    formatAmount(parseAmount(amount) - parseAmount(c.anlasFixed)),
  );
  await f.publish('big-0', NOW + 50001);
  assert.equal(
    db.prepare('SELECT status FROM generation_jobs WHERE id=?').get('big-1').status,
    'failed',
  );
  assert.equal(
    db.prepare('SELECT queue_expires_at AS n FROM generation_jobs WHERE id=?').get('big-1').n,
    NOW + 50000,
  );
  assert.equal(g.readCharge('big-1'), null);
});

test('整批已确认总额计入前张已结算收费；成员停用不能重领受理回执', async (t) => {
  const f = await fixture(t, { v5: '0', secondV5: '40', fallback: true }),
    { db, generations: g } = f;
  const v5Limit = formatAmount(
    parseAmount(
      quoteFairRequest({ model: 'nai-diffusion-5-full', width: 128, height: 128, steps: 23 })
        .amount,
      'ceil',
    ),
  );
  await f.accept('budget', {
    samples: 2,
    member: true,
    model: 'nai-diffusion-5-full',
    steps: 23,
    editConfirmation: (c) => ({ ...c, maxTotal: { ...c.maxTotal, v5: v5Limit } }),
  });
  // 派发时资源路径可随真实余额变化；原累计上限仍约束已经结算的 child。
  db.prepare("UPDATE wallets SET v5_available='40' WHERE member_id='m1'").run();
  f.dispatch('budget-0');
  assert.equal(g.readCharge('budget-0').v5, v5Limit);
  await f.publish('budget-0');
  const before = db.serialize();
  assert.throws(() => f.dispatch('budget-1', NOW + 10005), /费用超过/);
  assert.ok(db.serialize().equals(before));
  db.prepare("UPDATE members SET status='disabled' WHERE id='m1'").run();
  assert.equal(await f.accept('budget', { samples: 2, member: true }), null);
});

test('按原提交有界回放开账、授权受理及账务/结果闭环，私有认证不随业务事实恢复', async (t) => {
  const f = await fixture(t),
    { db } = f;
  await f.accept('success', { samples: 2, member: true });
  f.dispatch('success-0');
  await f.publish('success-0');
  f.dispatch('success-1', NOW + 10005);
  f.fail('success-1', {}, NOW + 10006);
  db.transaction(() =>
    f.generations.closeGenerationReview(meta('review-done', NOW + 310006), 'success-1'),
  )();
  await f.publish('success-1', NOW + 310010);
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const members = new RelationalMemberStore(target),
    inputs = new RelationalInputStore(target),
    generations = new RelationalGenerationStore(target, {});
  const sequences = db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS n FROM commits ORDER BY commit_seq')
    .all()
    .map((r) => r.n);
  const replay = (seq) => {
    const stored = readRelationalCommit(db, seq);
    if (
      stored.events.every((e) => ['MemberRegistered', 'MemberApiKeyChanged'].includes(e.eventType))
    ) {
      return target.transaction(() => members.replayMemberCommit(stored))();
    }
    if (stored.events.some((e) => e.eventType === 'RequestAccepted')) {
      return target.transaction(() => inputs.replayAcceptedInput(f.inputs, seq))();
    }
    return target.transaction(() => generations.replayGenerationCommit(stored))();
  };
  for (const seq of sequences.slice(0, 6)) {
    replay(seq);
  }
  assert.equal(target.prepare('SELECT status FROM projection_state').get().status, 'building');
  for (const seq of sequences.slice(6)) {
    replay(seq);
  }
  for (const table of [
    'commits',
    'domain_events',
    'stream_heads',
    'projection_state',
    'sharing_groups',
    'group_members',
    'sharing_policy',
    'upstream_accounts',
    'wallets',
    'account_quota_state',
    'operating_state',
    'generation_requests',
    'generation_inputs',
    'normalization_contexts',
    'generation_jobs',
    'generation_attempts',
    'charges',
    'artifacts',
  ]) {
    assert.deepEqual(
      target.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
      db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
      table,
    );
  }
  for (const seq of sequences) {
    assert.deepEqual(readRelationalCommit(target, seq), readRelationalCommit(db, seq));
  }
  assert.deepEqual(target.prepare('SELECT auth_epoch FROM members').all(), [
    { auth_epoch: null },
    { auth_epoch: null },
  ]);
  for (const table of [
    'api_keys',
    'password_credentials',
    'member_sessions',
    'upstream_credentials',
  ]) {
    assert.equal(target.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0, table);
  }
  const bytes = target.serialize();
  for (const seq of sequences) {
    assert.equal(replay(seq).replayed, true);
  }
  assert.ok(target.serialize().equals(bytes));
  assert.deepEqual(target.pragma('foreign_key_check'), []);
  assert.throws(() => assertRelationalStorageReady(target), /incomplete/);
});

test('回放核对实际 job、attempt、原预扣、成员和账号；截短完整提交或混入其他 owner 都零写入', async (t) => {
  const f = await fixture(t),
    { db } = f;
  await f.accept('victim');
  f.dispatch('victim-0');
  const snapshot = db.serialize();
  const terminal = f.fail('victim-0');
  const original = readRelationalCommit(db, terminal.commitSeq);
  const changes = [
    (stored) => {
      stored.events.pop();
      stored.command.events.pop();
    },
    (stored) => {
      const i = stored.events.findIndex((e) => e.eventType === 'GenerationRefunded');
      for (const e of [stored.events[i], stored.command.events[i]]) {
        e.payload.reservationEventId = 'opening-quota';
      }
    },
    (stored) => {
      const i = stored.events.findIndex((e) => e.eventType === 'GenerationRefunded');
      for (const e of [stored.events[i], stored.command.events[i]]) {
        e.memberId = 'm2';
      }
    },
    (stored) => {
      const i = stored.events.findIndex((e) => e.eventType === 'GenerationRefunded');
      for (const e of [stored.events[i], stored.command.events[i]]) {
        e.accountId = 'other';
        e.streamId = 'account:other';
      }
      delete stored.command.expectedVersions['account:a'];
      stored.command.expectedVersions['account:other'] = '0';
      stored.events[i].streamVersion = '1';
    },
    (stored) => {
      for (const e of [stored.events[0], stored.command.events[0]]) {
        e.payload.attemptId = 'nonexistent-attempt';
      }
    },
    (stored) => {
      const i = stored.events.findIndex((e) => e.eventType === 'GenerationRefunded');
      for (const e of [stored.events[i], stored.command.events[i]]) {
        e.payload.jobId = 'nonexistent-job';
      }
    },
  ];
  for (const change of changes) {
    const target = new Database(snapshot);
    try {
      target.pragma('foreign_keys=ON');
      const g = new RelationalGenerationStore(target, {}),
        stored = structuredClone(original);
      change(stored);
      const before = databaseState(target);
      target.transaction(() =>
        assert.throws(
          () => g.replayGenerationCommit(stored),
          /完整|complete|不符|归属|原预扣|真实尝试|不存在/,
        ),
      )();
      assert.equal(databaseState(target), before);
      target.transaction(() =>
        assert.throws(
          () => g.replayGenerationCommit(readRelationalCommit(db, '1')),
          /complete generation/,
        ),
      )();
      assert.equal(databaseState(target), before);
      target.transaction(() => g.replayGenerationCommit(original))();
      assert.equal(g.readCharge('victim-0').status, 'refunded');
    } finally {
      target.close();
    }
  }
});

test('原目标已缺失的终态开账凭据独立保留，不能靠同 job ID 重新派发、退款或补造实体', async (t) => {
  const f = await fixture(t),
    { db, generations: g } = f;
  const source = new Database(db.serialize());
  t.after(() => source.close());
  source.pragma('foreign_keys=ON');
  const { policyVersion, pricingVersion, ...metadata } = meta('thin-opening');
  const streamVersion = source
    .prepare(
      "SELECT CAST(stream_version AS TEXT) AS n FROM stream_heads WHERE stream_id='account:a'",
    )
    .get().n;
  source.transaction(() =>
    appendRelationalCommit(source, {
      ...metadata,
      receipt: { imported: true },
      expectedVersions: { 'account:a': streamVersion },
      events: [
        {
          eventId: 'thin-charge-proof',
          eventType: 'OpeningChargeRecorded',
          streamId: 'account:a',
          accountId: 'a',
          memberId: 'retired-member',
          occurredAt: NOW,
          policyVersion,
          pricingVersion,
          payload: {
            originalJobId: 'archived-0',
            originalMemberId: 'retired-member',
            originalAccountId: 'a',
            jobId: null,
            walletAccountId: null,
            walletMemberId: null,
            attemptId: null,
            status: 'refunded',
            resolution: 'unavailable',
            originRef: 'old-job-archive',
            resources: { v5: '0', anlasFixed: '9.123456789012', anlasPurchased: '0' },
            billingMode: 'anlas',
            v5Equivalent: '0',
            sharedV5Reserved: '0',
            configVersion: 1,
            reservedAt: NOW - 10,
            terminalAt: NOW - 1,
            reservationEventId: 'thin-charge-proof',
            terminalEventId: 'thin-charge-proof',
            origin: { sourceId: 'synthetic-opening', originalId: 'old-charge' },
          },
        },
      ],
    }),
  )();
  db.transaction(() => g.replayGenerationCommit(readRelationalCommit(source, '4')))();
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM generation_jobs').get().n, 0);
  assert.equal(db.prepare("SELECT 1 FROM members WHERE id='retired-member'").get(), undefined);
  const proof = g.readCharge('archived-0');
  assert.equal(proof.resolution, 'unavailable');
  assert.equal(proof.jobId, null);
  const wallet = g.readWallet('a', 'm1');
  assert.throws(() => f.fail('archived-0'), /不存在/);
  await f.accept('archived');
  const before = db.serialize();
  assert.throws(() => f.dispatch('archived-0'), /已有收费/);
  assert.ok(db.serialize().equals(before));
  assert.deepEqual(g.readCharge('archived-0'), proof);
  assert.deepEqual(g.readWallet('a', 'm1'), wallet);
  assert.deepEqual(db.pragma('foreign_key_check'), []);
});

test('响应保留实际观察，墙钟回退不改时间，冲突证据不能覆盖已保存事实', async (t) => {
  const f = await fixture(t),
    { db, generations: g } = f;
  await f.accept('clock');
  f.dispatch('clock-0');
  const observed = {
    dispatchState: 'request_started',
    upstreamStartedAt: NOW + 5,
    upstreamHeadersAt: NOW + 4,
    upstreamCompletedAt: NOW + 3,
    upstreamHttpStatus: 200,
  };
  db.transaction(() =>
    g.recordGenerationResponse(meta('clock-response', NOW + 6), 'clock-0', observed),
  )();
  assert.deepEqual(
    db
      .prepare(
        'SELECT upstream_started_at AS started,upstream_headers_at AS headers,upstream_completed_at AS completed FROM generation_attempts',
      )
      .get(),
    { started: NOW + 5, headers: NOW + 4, completed: NOW + 3 },
  );
  const before = db.serialize();
  assert.throws(
    () =>
      db.transaction(() =>
        g.recordGenerationResponse(meta('conflicting-response', NOW + 7), 'clock-0', {
          ...observed,
          upstreamHttpStatus: 503,
        }),
      )(),
    /冲突/,
  );
  assert.ok(db.serialize().equals(before));
});
