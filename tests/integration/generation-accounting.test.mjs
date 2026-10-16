import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { JsonStore } from '../../server/store.js';
import { configureFairQuota, syncFairQuota, reserveFairQuota, settleFairQuota } from '../../server/fair-quota.js';

const now = 1800000000000;
function fixture(mode = 'v5') {
  const db = {
    settings: {}, users: ['a', 'b', 'c', 'd'].map(id => ({ id, token: `secret-${id}`, enabled: true })),
    accounts: [{ id: 'opus', token: 'secret-upstream', enabled: true, quotaTier: 3, subscriptionActive: true,
      quotaFixed: mode === 'purchased' ? 0 : 10000, quotaPurchased: mode === 'purchased' ? 1000 : 0,
      v5UsagePercent: 80, v5UsageTimeUntilNextPercent: 600, quotaCheckedAt: now }],
    jobs: [{ id: 'job', batchId: 'batch', sampleIndex: 1, userToken: 'secret-a', status: 'queued', source: 'novelai-native',
      request: { model: mode === 'free' ? 'nai-diffusion-4-5-full' : 'nai-diffusion-5-full',
        width: 1024, height: 1024, steps: ['fixed', 'purchased'].includes(mode) ? 29 : 23,
        allowPurchasedAnlas: mode === 'purchased', prompt: 'private prompt' } }], ledger: []
  };
  configureFairQuota(db, ['a', 'b', 'c', 'd']);
  syncFairQuota(db, now);
  if (mode === 'fallback') {
    db.settings.fairQuota.autoAnlasFallback = true;
    db.settings.fairQuota.balances.a.v5Available = 0;
  }
  return db;
}
function reserve(db) {
  const charge = reserveFairQuota(db, db.users[0], db.jobs[0], db.accounts[0], now);
  db.jobs[0].status = 'running';
  return charge;
}
const events = (db, type) => db.ledger.filter(event => event.type === type);

test('派发意图与原预扣绑定稳定身份；证据不含令牌/提示词，不随任务或快照修改', () => {
  const db = fixture(), charge = reserve(db), attempt = structuredClone(db.jobs[0].generationAttempt);
  reserve(db);
  assert.equal(events(db, 'generation_reserved').length, 1);
  assert.equal(charge.attemptId, attempt.id);
  assert.equal(attempt.batchId, 'batch'); assert.equal(attempt.sampleIndex, 1);
  assert.equal(attempt.dispatchIntentAt, now); assert.equal(attempt.snapshot.at, now);
  assert.equal(attempt.localReserved.v5, charge.amount);
  db.jobs[0].generationAttempt.localReserved.v5 = 123;
  db.accounts[0].quotaFixed = 1;
  assert.deepEqual(events(db, 'generation_reserved')[0].attempt, attempt);
  assert.doesNotMatch(JSON.stringify(db.ledger), /secret-|private prompt/);
});

for (const mode of ['v5', 'fixed', 'purchased', 'fallback', 'free']) {
  test(`${mode} 只退原资源一次，候选损耗不作已确认实扣或运营资金门槛`, () => {
    const db = fixture(mode), before = structuredClone(db.settings.fairQuota.balances);
    const charge = reserve(db), job = db.jobs[0];
    const original = structuredClone(job.generationAttempt.localReserved);
    job.upstreamStartedAt = new Date(now + 1).toISOString(); job.upstreamHttpStatus = 200;
    settleFairQuota(db, job, false, now + 2);
    assert.deepEqual(db.settings.fairQuota.balances, before);
    assert.deepEqual(events(db, 'generation_refunded')[0].resources, original);
    const audit = structuredClone(db.ledger);
    settleFairQuota(db, job, false, now + 3); settleFairQuota(db, job, true, now + 4);
    assert.deepEqual(db.ledger, audit); assert.deepEqual(db.settings.fairQuota.balances, before);
    const loss = events(db, 'generation_loss_suspected')[0];
    if (mode === 'free') { assert.equal(loss, undefined); return; }
    assert.equal(loss.actualConsumed, null); assert.equal(loss.evidenceLevel, 'unconfirmed');
    assert.equal(loss.firstUnknownAt, now + 2); assert.equal(loss.response.upstreamHttpStatus, 200);
    assert.equal(loss.refundEventId, job.generationRefund.eventId);
    if (mode === 'fallback') {
      assert.equal(charge.amount, 26); assert.equal(original.anlasFixed, 26); assert.equal(original.v5, 0);
      assert.equal(loss.upstreamCandidates.length, 2); assert.equal(loss.candidateMode, 'alternatives');
      assert.ok(Math.abs(loss.upstreamCandidates[0].resources.v5 - 0.057803468208092484) < 1e-10);
      assert.equal(loss.upstreamCandidates[1].resources.anlasFixed, 26);
    } else if (mode === 'purchased') {
      assert.equal(loss.upstreamCandidates[0].resources.anlasFixed, 0);
      assert.equal(loss.upstreamCandidates[0].resources.anlasPurchased, charge.amount);
      assert.equal(db.settings.operatingFunds, undefined);
    }
  });
}

test('未派发取消和活进程发出前失败不记未知；旧 running 仅保留不确定意图', () => {
  const queued = fixture();
  settleFairQuota(queued, queued.jobs[0], false, now);
  assert.deepEqual(queued.ledger, []);
  const unsent = fixture(); reserve(unsent);
  settleFairQuota(unsent, unsent.jobs[0], false, now + 1, { requestStarted: false });
  assert.equal(events(unsent, 'generation_loss_suspected').length, 0);
  assert.equal(events(unsent, 'generation_refunded')[0].response.dispatchState, 'not_sent');
  const pending = fixture(); reserve(pending); pending.jobs[0].status = 'pending';
  settleFairQuota(pending, pending.jobs[0], false, now + 1);
  assert.equal(events(pending, 'generation_refunded')[0].response.dispatchState, 'not_sent');
  assert.equal(events(pending, 'generation_loss_suspected').length, 0);
  const legacy = fixture(); reserve(legacy);
  delete legacy.jobs[0].generationAttempt; delete legacy.settings.fairQuota.reservations.job.attemptId;
  legacy.ledger = [];
  settleFairQuota(legacy, legacy.jobs[0], false, now + 1);
  const attempt = events(legacy, 'generation_reserved')[0].attempt;
  assert.equal(attempt.evidenceOrigin, 'legacy_reservation'); assert.equal(attempt.snapshot, null);
  assert.equal(attempt.dispatchIntentAt, null);
  assert.equal(events(legacy, 'generation_loss_suspected')[0].response.dispatchState, 'unknown');
});

// 与生产 dirtyJobMutationRows 相同：只提交本任务、预扣设置及本任务追加的 ledger 行。
const persist = { immediate: true, includeSettings: true, dirtyRows: (_result, db) => ({
  jobs: ['job'], ledger: db.ledger.filter(event => event.jobId === 'job').map(event => event.id)
}) };

test('真实 SQLite 预扣/退款/证据同事务；写失败还原内存，重启及冷 ledger 重复退款均无副作用', async t => {
  const directory = await mkdtemp(path.join(tmpdir(), 'nai-generation-accounting-'));
  let store = new JsonStore(directory);
  t.after(() => store.close());
  await store.init();
  await store.update(db => Object.assign(db, fixture()), { immediate: true });
  const original = structuredClone(store.db.settings.fairQuota.balances);
  store.sqlite.exec(`CREATE TEMP TRIGGER reject_generation_evidence BEFORE INSERT ON ledger
    WHEN json_extract(NEW.data, '$.type') = 'generation_reserved'
    BEGIN SELECT RAISE(ABORT, 'synthetic evidence failure'); END;`);
  await assert.rejects(store.update(reserve, persist), /synthetic evidence failure/);
  assert.deepEqual(store.db.settings.fairQuota.balances, original);
  assert.equal(store.db.jobs[0].status, 'queued'); assert.equal(store.db.ledger.length, 0);
  store.sqlite.exec('DROP TRIGGER reject_generation_evidence');
  await store.update(reserve, persist);
  const reserved = structuredClone(store.db.settings.fairQuota.balances);
  const fail = db => {
    db.jobs[0].upstreamStartedAt = new Date(now + 1).toISOString();
    settleFairQuota(db, db.jobs[0], false, now + 2); db.jobs[0].status = 'failed';
  };
  store.sqlite.exec(`CREATE TEMP TRIGGER reject_generation_evidence BEFORE INSERT ON ledger
    WHEN json_extract(NEW.data, '$.type') = 'generation_loss_suspected'
    BEGIN SELECT RAISE(ABORT, 'synthetic loss failure'); END;`);
  await assert.rejects(store.update(fail, persist), /synthetic loss failure/);
  assert.deepEqual(store.db.settings.fairQuota.balances, reserved);
  assert.equal(store.db.jobs[0].status, 'running'); assert.equal(store.db.ledger.length, 1);
  assert.equal(store.db.settings.fairQuota.reservations.job.status, 'reserved');
  store.sqlite.exec('DROP TRIGGER reject_generation_evidence');
  await store.update(fail, persist);
  const audit = store.sqlite.prepare('SELECT id, data FROM ledger ORDER BY id').all();
  assert.equal(audit.length, 3);
  await store.close(); store = new JsonStore(directory); await store.init();
  assert.deepEqual(store.db.settings.fairQuota.balances, original);
  // 历史 ledger 不在热缓存时仍以持久预扣终态保证幂等，而非扫描有限缓存猜是否已退款。
  store.db.ledger = [];
  await store.update(db => settleFairQuota(db, db.jobs[0], false, now + 5000), persist);
  assert.deepEqual(store.db.settings.fairQuota.balances, original);
  assert.deepEqual(store.sqlite.prepare('SELECT id, data FROM ledger ORDER BY id').all(), audit);
  assert.equal(store.selectItemById('ledger', 'attempt:job:1:generation_loss_suspected').actualConsumed, null);
  assert.equal(store.sqlite.pragma('integrity_check', { simple: true }), 'ok');
});
