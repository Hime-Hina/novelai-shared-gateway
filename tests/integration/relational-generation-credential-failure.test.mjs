import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { fixture, meta, NOW, databaseState } from '../helpers/relational-generation.mjs';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { relationalPrivateTables } from '../../server/storage/relational-private-restore.ts';
import { parseAmount, formatAmount } from '../../server/quota/resource-amounts.ts';
import {
  accountQuotaResult,
  generationCredentialError,
} from '../../server/quota/account-refresh.ts';

const observed = (q) =>
  Object.fromEntries(
    [
      'observationEventId',
      'observedAt',
      'observedCredentialEpoch',
      'observedSequence',
      'quotaTier',
      'subscriptionActive',
      'subscriptionExpiresAt',
      'observedV5',
      'observedAnlasFixed',
      'observedAnlasPurchased',
      'secondsPerPercent',
      'baselineEventId',
      'refillCursorAt',
    ].map((key) => [key, q[key]]),
  );

function clone(t, db) {
  const copy = new Database(db.serialize());
  t.after(() => copy.close());
  copy.pragma('foreign_keys=ON');
  return copy;
}
function response(f, status, at = NOW + 2) {
  return f.db.transaction(() =>
    f.generations.recordGenerationResponse(
      meta('response', at),
      'failed-0',
      {
        ...f.response(at),
        upstreamHttpStatus: status,
      },
      { kind: 'ordinary' },
    ),
  )();
}
async function running(t) {
  const f = await fixture(t, { seconds: 0 });
  await f.accept('failed');
  const wallet = f.generations.readWallet('a', 'm1');
  const dispatched = f.dispatch('failed-0');
  return { ...f, wallet, dispatched, quotas: new RelationalQuotaStore(f.db) };
}

test('原分类 owner 只将明确非 abort 的 401/403 判为凭据失效', () => {
  assert.equal(generationCredentialError(false, 401), 'invalid_key');
  assert.equal(generationCredentialError(false, 403), 'access_denied');
  for (const status of [null, 200, 429, 500]) {
    assert.equal(generationCredentialError(false, status), null);
  }
  assert.equal(generationCredentialError(true, 401), null);
  assert.equal(generationCredentialError(true, 403), null);
});

test('当前凭据 401/403 同笔失效并退款，abort/200 不停用；原观察余额、时间及序号不变', async (t) => {
  for (const [status, aborted] of [
    [401, false],
    [403, false],
    [401, true],
    [200, false],
  ]) {
    await t.test(`${status}/${aborted}`, async (t) => {
      const f = await running(t);
      response(f, status);
      const before = f.quotas.readAccountQuota('a'),
        result = f.fail('failed-0', {
          aborted,
          diagnostic: {
            dispatch: f.dispatched.dispatch,
            error: new Error(`private transport ${f.dispatched.dispatch.pst}`),
          },
        }),
        after = f.quotas.readAccountQuota('a'),
        stored = readRelationalCommit(f.db, result.commitSeq),
        invalidity = stored.events.find((e) => e.eventType === 'QuotaStatusChanged'),
        expected = generationCredentialError(aborted, status);
      assert.equal(after.quotaErrorCode, expected);
      assert.equal(after.status, expected ? 'quota_sync_error' : before.status);
      assert.deepEqual(observed(after), observed(before));
      assert.deepEqual(f.generations.readWallet('a', 'm1'), f.wallet);
      assert.equal(f.generations.readCharge('failed-0').status, 'refunded');
      assert.equal(f.db.prepare('SELECT enabled FROM upstream_accounts').get().enabled, 1);
      assert.equal(
        stored.events.some((e) => e.eventType === 'QuotaObserved'),
        false,
      );
      const failed = stored.events.find((e) => e.eventType === 'JobFailed');
      assert.equal(failed.payload.aborted, aborted);
      if (expected) {
        const source = invalidity.payload.generationFailure,
          recorded = stored.events.find((e) => e.eventId === source.responseEventId);
        assert.equal(source.errorCode, expected);
        assert.equal(
          source.dispatchEventId,
          f.dispatched.events.find((e) => e.eventType === 'DispatchAuthorized').eventId,
        );
        assert.equal(source.failureEventId, failed.eventId);
        assert.equal(recorded.eventType, 'ResponseEvidenceRecorded');
        assert.ok(recorded.eventIndex < invalidity.eventIndex);
        assert.ok(
          invalidity.eventIndex <
            stored.events.find((e) => e.eventType === 'GenerationRefunded').eventIndex,
        );
        assert.ok(invalidity.eventIndex < failed.eventIndex);
      } else {
        assert.equal(invalidity, undefined);
      }
      const once = f.db.serialize();
      assert.equal(f.fail('failed-0', { aborted }).replayed, true);
      assert.deepEqual(f.db.serialize(), once);
      await f.accept('next');
      if (expected) {
        assert.throws(() => f.dispatch('next-0', NOW + 11000), /额度基线不可用/);
      } else {
        assert.ok(f.dispatch('next-0', NOW + 11000).dispatch);
      }
    });
  }
});

test('失败 API 在原回执前也要求显式 aborted，不把旧未知或遗漏变成 false', async (t) => {
  const f = await running(t);
  response(f, 401);
  const before = databaseState(f.db);
  assert.throws(() => f.fail('failed-0'), /必须明确/);
  assert.equal(databaseState(f.db), before);
  f.fail('failed-0', { aborted: false });
  const once = f.db.serialize();
  assert.throws(() => f.fail('failed-0'), /必须明确/);
  assert.deepEqual(f.db.serialize(), once);
});

test('A1→B→A2 旧尝试错误按原 epoch 忽略失效，保留新观察且原池退款/窄回放仍合法', async (t) => {
  const f = await running(t),
    oldPst = f.dispatched.dispatch.pst;
  // 模拟迁移/恢复切点上保留的旧在途尝试；当前在线轮换命令本身仍禁止跨在途强换。
  for (const [epoch, pst] of [
    [2, 'synthetic-B'],
    [3, oldPst],
  ]) {
    f.db.prepare('UPDATE upstream_accounts SET credential_epoch=?').run(epoch);
    f.db.prepare('UPDATE upstream_credentials SET pst=?').run(pst);
  }
  const ticket = f.quotas.beginQuotaObservation('a', NOW + 20);
  f.db.transaction(() =>
    f.quotas.recordQuotaObservation(
      meta('new-observation', NOW + 20),
      ticket,
      accountQuotaResult(
        'a',
        {
          points: 0,
          fixed: 8000,
          purchased: 0,
          tier: 3,
          subscriptionActive: true,
          subscriptionExpiresAt: null,
          v5UsagePercent: 0,
          v5UsageTimeUntilNextPercent: 0,
        },
        new Date(NOW + 20).toISOString(),
      ),
    ),
  )();
  response(f, 401, NOW + 21);
  const walletBefore = f.generations.readWallet('a', 'm1'),
    charge = f.generations.readCharge('failed-0');
  const refundExpected = Object.fromEntries(
    ['v5', 'anlasFixed', 'anlasPurchased'].map((key) => [
      key,
      formatAmount(parseAmount(walletBefore[key]) + parseAmount(charge[key])),
    ]),
  );
  const before = f.quotas.readAccountQuota('a'),
    checkpoint = clone(t, f.db),
    result = f.fail('failed-0', { aborted: false }, NOW + 22),
    stored = readRelationalCommit(f.db, result.commitSeq);
  assert.equal(
    stored.events.some((e) => e.eventType === 'QuotaStatusChanged'),
    false,
  );
  assert.deepEqual(observed(f.quotas.readAccountQuota('a')), observed(before));
  assert.equal(f.quotas.readAccountQuota('a').quotaErrorCode, null);
  assert.deepEqual(f.generations.readWallet('a', 'm1'), refundExpected);
  const replay = new RelationalGenerationStore(checkpoint);
  checkpoint.transaction(() => replay.replayGenerationCommit(stored))();
  assert.equal(databaseState(checkpoint), databaseState(f.db));
  const once = checkpoint.serialize();
  assert.equal(
    checkpoint.transaction(() => replay.replayGenerationCommit(stored))().replayed,
    true,
  );
  assert.deepEqual(checkpoint.serialize(), once);
  await f.accept('current');
  assert.equal(f.dispatch('current-0', NOW + 11000).dispatch.credentialEpoch, 3);
});

test('Quota 第二 owner 已写后异常被外层捕获仍全表回滚，同一命令随后可重试', async (t) => {
  const f = await running(t);
  response(f, 401);
  let reached = false;
  f.db.function('quota_failure_written', () => {
    reached = true;
    throw new Error('quota failure fault');
  });
  f.db.exec(
    "CREATE TEMP TRIGGER quota_failure_fault AFTER UPDATE OF quota_error_code ON account_quota_state WHEN NEW.quota_error_code='invalid_key' BEGIN SELECT quota_failure_written(); END",
  );
  const before = databaseState(f.db);
  f.db.transaction(() =>
    assert.throws(() => f.fail('failed-0', { aborted: false }), /quota failure fault/),
  )();
  assert.equal(reached, true);
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER quota_failure_fault');
  assert.equal(f.fail('failed-0', { aborted: false }).replayed, false);
  assert.equal(f.quotas.readAccountQuota('a').quotaErrorCode, 'invalid_key');
});

function resequence(stored, db) {
  const versions = new Map();
  stored.command.expectedVersions = {};
  stored.events = stored.events.map((e, eventIndex) => {
    if (!versions.has(e.streamId)) {
      const current =
        db
          .prepare('SELECT CAST(stream_version AS TEXT) AS v FROM stream_heads WHERE stream_id=?')
          .get(e.streamId)?.v ?? '0';
      versions.set(e.streamId, BigInt(current));
      stored.command.expectedVersions[e.streamId] = current;
    }
    versions.set(e.streamId, versions.get(e.streamId) + 1n);
    return { ...e, eventIndex, streamVersion: String(versions.get(e.streamId)) };
  });
  stored.eventCount = stored.events.length;
  stored.command.events = stored.events.map((e) =>
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
      ].map((key) => [key, e[key]]),
    ),
  );
  return stored;
}

test('完整提交即使两个列表一致也不能错配来源、反转顺序、撤销 abort 或换当前 epoch', async (t) => {
  const f = await running(t);
  const recorded = response(f, 401);
  const checkpoint = clone(t, f.db),
    result = f.fail('failed-0', { aborted: false }),
    original = readRelationalCommit(f.db, result.commitSeq);
  for (const kind of [
    'dispatch',
    'old-response',
    'failure',
    'aborted',
    'classification',
    'order',
    'missing-response',
    'epoch',
  ]) {
    await t.test(kind, () => {
      const db = clone(t, checkpoint),
        bad = structuredClone(original),
        quota = bad.events.find((e) => e.eventType === 'QuotaStatusChanged'),
        failed = bad.events.find((e) => e.eventType === 'JobFailed');
      if (kind === 'dispatch') {
        quota.payload.generationFailure.dispatchEventId = bad.events[0].eventId;
      }
      if (kind === 'old-response') {
        quota.payload.generationFailure.responseEventId = recorded.events.find(
          (e) => e.eventType === 'ResponseEvidenceRecorded',
        ).eventId;
      }
      if (kind === 'failure') {
        quota.payload.generationFailure.failureEventId = bad.events[0].eventId;
      }
      if (kind === 'aborted') {
        failed.payload.aborted = true;
      }
      if (kind === 'classification') {
        quota.payload.generationFailure.errorCode = 'access_denied';
      }
      if (kind === 'order') {
        [bad.events[0], bad.events[1]] = [bad.events[1], bad.events[0]];
      }
      if (kind === 'missing-response') {
        bad.events = bad.events.filter((e) => e.eventType !== 'ResponseEvidenceRecorded');
      }
      if (kind === 'epoch') {
        db.prepare('UPDATE upstream_accounts SET credential_epoch=3').run();
      }
      resequence(bad, db);
      const before = databaseState(db);
      db.transaction(() =>
        assert.throws(
          () => new RelationalGenerationStore(db).replayGenerationCommit(bad),
          /凭据失效|上游响应|派发/,
        ),
      )();
      assert.equal(databaseState(db), before);
    });
  }
});

test('原派发事实本身必须属于当时当前账户代次，不能先伪造 epoch 再作为失效来源', async (t) => {
  const f = await fixture(t, { seconds: 0 });
  await f.accept('failed');
  const checkpoint = clone(t, f.db),
    result = f.dispatch('failed-0'),
    original = readRelationalCommit(f.db, result.commitSeq);
  const forged = structuredClone(original);
  forged.events.find((e) => e.eventType === 'DispatchAuthorized').payload.credentialEpoch = 2;
  resequence(forged, checkpoint);
  const before = databaseState(checkpoint),
    replay = new RelationalGenerationStore(checkpoint);
  checkpoint.transaction(() =>
    assert.throws(() => replay.replayGenerationCommit(forged), /派发与真实原预扣不一致/),
  )();
  assert.equal(databaseState(checkpoint), before);
  checkpoint.transaction(() => replay.replayGenerationCommit(original))();
  assert.equal(databaseState(checkpoint), databaseState(f.db));
});

test('当前失效与退款通过共同完整业务回放恢复，重复零写且不恢复 PST/成员认证或发网络', async (t) => {
  const f = await running(t);
  response(f, 403);
  f.fail('failed-0', {
    aborted: false,
    diagnostic: {
      dispatch: f.dispatched.dispatch,
      error: new Error(`private transport ${f.dispatched.dispatch.pst}`),
    },
  });
  const rebuilt = new Database(':memory:');
  t.after(() => rebuilt.close());
  initializeRelationalStorage(rebuilt, 'generation-history');
  const replayer = new RelationalCommitReplayer(rebuilt),
    expected = clone(t, f.db);
  let network = 0;
  t.mock.method(globalThis, 'fetch', () => {
    network++;
    throw new Error('no network');
  });
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    rebuilt.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), f.inputs))();
  }
  expected.prepare('UPDATE members SET auth_epoch=NULL').run();
  for (const [table] of relationalPrivateTables) {
    expected.prepare(`DELETE FROM ${table}`).run();
  }
  assert.equal(databaseState(rebuilt), databaseState(expected));
  const once = rebuilt.serialize();
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    assert.equal(
      rebuilt.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), f.inputs))()
        .replayed,
      true,
    );
  }
  assert.deepEqual(rebuilt.serialize(), once);
  assert.equal(network, 0);
});
