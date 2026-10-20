import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { JsonStore } from '../../server/store.ts';
import { configureFairQuota, syncFairQuota, reserveFairQuota, settleFairQuota, quoteFairRequest } from '../../server/fair-quota.ts';
import { recordRecoveredGeneration } from '../../server/generation-accounting.ts';
import { assertFreeImageAdmission, configureFreeImages, freeImagePolicy } from '../../server/free-image-policy.ts';

const now = Date.now(), day = 86400000;
const free = { model: 'nai-diffusion-4-5-full', width: 1024, height: 1024, steps: 23 };
const write = { immediate: true };
async function fixture(t, policy = { enabled: true, warningThreshold: 1, hardLimit: 2 }) {
  const store = new JsonStore(await mkdtemp(path.join(tmpdir(), 'nai-free-images-')));
  await store.init(); t.after(() => store.close());
  await store.update(db => {
    db.users = ['a', 'b', 'c', 'd'].map(id => ({ id, token: id, enabled: true }));
    db.accounts = [{ id: 'opus', enabled: true, quotaTier: 3, subscriptionActive: true,
      quotaFixed: 1000, quotaPurchased: 0, v5UsagePercent: 80, quotaCheckedAt: now }];
    configureFairQuota(db, ['a', 'b', 'c', 'd']); syncFairQuota(db, now);
    configureFreeImages(db, { expectedVersion: 1, policy }, 'a');
  }, write);
  return store;
}
function reserve(store, id, at = now, request = free) {
  return store.update(db => {
    Object.assign(db.accounts[0], { quotaCheckedAt: at }); syncFairQuota(db, at);
    if (quoteFairRequest(request).resource === 'free') assertFreeImageAdmission(store.readFreeImageUsage(db, 'a', at));
    const job = { id, userToken: 'a', status: 'running', request, createdAt: new Date(at).toISOString() };
    reserveFairQuota(db, db.users[0], job, db.accounts[0], at);
    db.jobs.push(job); return job;
  }, write);
}
function settle(store, id, success, at = now) {
  return store.update(db => {
    const job = db.jobs.find(j => j.id === id);
    settleFairQuota(db, job, success, at, {}, store.readFairQuotaSettlement(id));
    job.status = success ? 'done' : 'failed';
  }, write);
}
const usage = (store, at = now) => store.readFreeImageUsage(store.db, 'a', at);

test('最后一个免费名额事务内争抢；失败释放，成功按结算时间计数，窗口端点精确', async t => {
  const store = await fixture(t, { enabled: true, warningThreshold: 1, hardLimit: 1 });
  const contenders = await Promise.allSettled([reserve(store, 'one'), reserve(store, 'two')]);
  assert.equal(contenders.filter(r => r.status === 'fulfilled').length, 1);
  assert.equal(contenders.find(r => r.status === 'rejected').reason.statusCode, 429);
  assert.equal(usage(store).reserved, 1); assert.equal(usage(store).waitingForSettlement, true);
  await settle(store, 'one', false);
  assert.equal(usage(store).reserved, 0); assert.equal(usage(store).successful, 0);
  await reserve(store, 'success'); await settle(store, 'success', true, now + 100);
  assert.equal(usage(store, now + 100).successful, 1);
  assert.equal(usage(store, now + 100).warnedAt, now + 100);
  assert.equal(usage(store, now + 100).nextAvailableAt, now + 100 + day);
  assert.throws(() => assertFreeImageAdmission(usage(store, now + 100)), error => error.statusCode === 429 && error.cooldownUntil === new Date(now + 100 + day).toISOString());
  assert.equal(usage(store, now + day + 99).successful, 1);
  assert.equal(usage(store, now + day + 100).successful, 0);
});

test('重启和清理日志不清零，已退款的迟到完整图片计一次，缓存/预览/付费不计', async t => {
  let store = await fixture(t, { enabled: true, warningThreshold: 2, hardLimit: null });
  await reserve(store, 'late'); await settle(store, 'late', false);
  await store.update(db => {
    const job = db.jobs.find(j => j.id === 'late');
    assert.equal(recordRecoveredGeneration(db, job, 'image', now + 1), true);
    assert.equal(recordRecoveredGeneration(db, job, 'image', now + 2), false);
    db.jobs.push({ id: 'cache', status: 'done', imageId: 'image' }, { id: 'preview', status: 'running', generationProgress: { percent: 95 } });
  }, write);
  await reserve(store, 'paid', now + 3, { ...free, steps: 29 }); await settle(store, 'paid', true, now + 4);
  await reserve(store, 'v5', now + 5, { ...free, model: 'nai-diffusion-5-full' }); await settle(store, 'v5', true, now + 6);
  await reserve(store, 'second', now + 7); await settle(store, 'second', true, now + 8);
  await settle(store, 'second', true, now + 9);
  assert.equal(usage(store, now + 9).successful, 2);
  assert.equal(usage(store, now + 9).warnedAt, now + 8);
  await store.update(db => { db.jobs = []; }, { collections: ['jobs'], immediate: true });
  const directory = store.dataDir; await store.close();
  store = new JsonStore(directory); await store.init(); t.after(() => store.close());
  assert.equal(usage(store, now + 10).successful, 2);
  assert.equal(store.readFreeImageUsage(store.db, 'b', now + 10).successful, 0);
  const plan = store.sqlite.prepare('EXPLAIN QUERY PLAN SELECT COUNT(*) FROM free_image_usage WHERE member_id = ? AND succeeded_at > ? AND succeeded_at <= ?').all('a', now - day, now);
  assert.ok(plan.some(row => row.detail.includes('idx_free_image_member_time')));
});

test('成功写失败全笔回滚；关闭/仅提醒不阻止，告警越线去重并可重新触发', async t => {
  const store = await fixture(t, { enabled: true, warningThreshold: 1, hardLimit: null });
  await reserve(store, 'rollback');
  store.sqlite.exec("CREATE TEMP TRIGGER fail_success BEFORE INSERT ON free_image_usage BEGIN SELECT RAISE(ABORT, 'synthetic success failure'); END;");
  await assert.rejects(settle(store, 'rollback', true, now + 1), /synthetic success failure/);
  assert.equal(usage(store).reserved, 1); assert.equal(usage(store).successful, 0);
  store.sqlite.exec('DROP TRIGGER fail_success');
  await settle(store, 'rollback', true, now + 1);
  await reserve(store, 'more', now + 2); await settle(store, 'more', true, now + 3);
  assert.equal(usage(store, now + 3).warnedAt, now + 1);
  await reserve(store, 'new-window', now + day + 4); await settle(store, 'new-window', true, now + day + 5);
  assert.equal(usage(store, now + day + 5).warnedAt, now + day + 5);
  await store.update(db => configureFreeImages(db, { expectedVersion: 2, policy: { enabled: false, warningThreshold: 1, hardLimit: 1 } }, 'a'), write);
  assert.equal(usage(store, now + day + 5).warning, false);
  await reserve(store, 'disabled', now + day + 6);
  await assert.rejects(store.update(db => configureFreeImages(db, { expectedVersion: 2, policy: {} }, 'a'), write), error => error.statusCode === 409);
  for (const hardLimit of [0, -1, 1.5, '2', Infinity]) {
    await assert.rejects(store.update(db => configureFreeImages(db, { expectedVersion: 3, policy: { enabled: true, warningThreshold: null, hardLimit } }, 'a'), write), error => error.statusCode === 400);
  }
  assert.equal(freeImagePolicy(store.db).version, 3);
});

test('升级回填旧成功与迟到任务，重启保留在途名额且退款后释放', async t => {
  let store = await fixture(t, { enabled: false, warningThreshold: null, hardLimit: null });
  await reserve(store, 'old-success'); await settle(store, 'old-success', true, now + 1);
  await reserve(store, 'old-late', now + 2); await settle(store, 'old-late', false, now + 3);
  await store.update(db => recordRecoveredGeneration(db, db.jobs.find(j => j.id === 'old-late'), 'image', now + 4), write);
  await reserve(store, 'in-flight', now + 5);
  store.sqlite.exec(`DROP TRIGGER free_image_settled; DROP TRIGGER free_image_recovered_insert; DROP TRIGGER free_image_recovered_update;
    DROP TABLE free_image_usage; DELETE FROM __drizzle_migrations WHERE created_at = (SELECT MAX(created_at) FROM __drizzle_migrations);`);
  const directory = store.dataDir; await store.close();
  store = new JsonStore(directory); await store.init(); t.after(() => store.close());
  assert.equal(usage(store, now + 6).successful, 2);
  assert.equal(usage(store, now + 6).reserved, 1);
  await settle(store, 'in-flight', false, now + 7);
  assert.equal(usage(store, now + 7).successful, 2);
  assert.equal(usage(store, now + 7).reserved, 0);
});
