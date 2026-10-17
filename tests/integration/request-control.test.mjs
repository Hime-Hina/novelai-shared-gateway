import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import { JsonStore } from '../../server/store.ts';
import { configureFairQuota, syncFairQuota, reserveFairQuota, settleFairQuota } from '../../server/fair-quota.ts';
import { activateNextBatchJob, singleGenerationRequest, cancelWaitingBatchJobs } from '../../server/generation-request.ts';
import { assertMemberAcceptsJobs, queuedDeadlineTimestamp } from '../../server/queue-policy.ts';
import { sharedAccountConcurrency, sharedAccountReadyDelay } from '../../server/shared-capacity.ts';

const source = await readFile(new URL('../../server/gateway-runtime.js', import.meta.url), 'utf8');
const request = { model: 'nai-diffusion-5-full', width: 1024, height: 1024, steps: 23, seed: 12 };
async function fixture(t, statuses) {
  const store = new JsonStore(await mkdtemp(path.join(tmpdir(), 'nai-request-control-')));
  await store.init();
  t.after(() => store.close());
  await store.update(db => {
    db.users = ['a', 'b', 'c', 'd'].map(id => ({ id, token: `STA1N-${id}`, enabled: true }));
    db.accounts = [{ id: 'opus', enabled: true, quotaTier: 3, subscriptionActive: true,
      quotaFixed: 1000, quotaPurchased: 0, v5UsagePercent: 100, quotaCheckedAt: Date.now(), inFlight: 0 }];
    configureFairQuota(db, ['a', 'b', 'c', 'd']); syncFairQuota(db);
    db.jobs = statuses.map((status, sampleIndex) => ({ id: `child-${sampleIndex}`, requestId: 'parent', sampleIndex,
      status, source: 'novelai-native', userToken: 'STA1N-a', request, createdAt: new Date().toISOString() }));
  }, { immediate: true, generationRequest: { id: 'parent', member_id: 'a', source: 'novelai-native', operation: 'generate',
    created_at: new Date().toISOString(), payload_bytes: 100000, payload: JSON.stringify({ ...request, prompt: 'x'.repeat(100000) }) } });
  const update = store.update.bind(store);
  store.update = (fn, options) => update(fn, { ...options, immediate: true, includeSettings: true });
  const notifications = [];
  const ctx = vm.createContext({ store, fairShareMode: true, queueDispatchEnabled: true, settleFairQuota, syncFairQuota, reserveFairQuota,
    assertMemberAcceptsJobs, queuedDeadlineTimestamp, sharedAccountConcurrency, sharedAccountReadyDelay,
    singleGenerationRequest, cancelWaitingBatchJobs, runningJobControls: new Map(), jobAccountCost: () => 0,
    staleQueuedJobMs: () => 600000, staleRunningJobMs: () => 660000,
    httpError: (statusCode, message) => Object.assign(new Error(message), { statusCode }),
    publicErrorMessage: value => value, publicImageUrl: id => `/signed/${id}`, clearJobStreamProgress() {},
    notifyJobWaiters: id => notifications.push(id), scheduleQueueDrain() {}, createId: () => 'synthetic-refund' });
  for (const name of ['publicGenerationRequest', 'getGenerationRequest', 'cancelGenerationRequest', 'refundJob',
    'getUserOrThrow', 'uniqueIds', 'batchJobIds', 'dirtyJobMutationRows', 'dirtyResultJobRows',
    'physicalAccountLoad', 'jobDeadlineTimestamp', 'isExpiredJobAt', 'isStaleActiveJob', 'staleActiveJobDetail', 'reserveQueuedJob']) {
    const start = source.indexOf(`function ${name}(`), end = source.indexOf('\n}', start) + 2;
    vm.runInContext((source.slice(start - 6, start) === 'async ' ? 'async ' : '') + source.slice(start, end), ctx);
  }
  return { store, ctx, notifications };
}

test('请求查询不读大父载荷或热载冷历史，换 Key 后按稳定成员归属签发本人产物', async t => {
  const { store, ctx } = await fixture(t, ['done', 'failed', 'done']);
  await store.update(db => {
    db.users[0].token = 'STA1N-a-new';
    db.users[1].token = 'STA1N-a';
    db.images.push({ id: 'owned', token: 'STA1N-a' }, { id: 'foreign', token: 'STA1N-foreign' });
    db.jobs[0].imageId = 'owned';
    db.jobs[1].imageId = 'owned'; db.jobs[1].lateResultAvailable = true;
    db.jobs[2].imageId = 'foreign';
  }, { immediate: true });
  store.sqlite.prepare('UPDATE generation_requests SET payload = ? WHERE id = ?').run('not-json', 'parent');
  store.trimRuntimeCollection('jobs', 0);
  store.trimRuntimeCollection('images', 0);
  const state = await ctx.getGenerationRequest('parent', 'a');
  assert.equal(state.jobs[0].imageUrl, '/signed/owned');
  assert.equal(state.jobs[1].imageUrl, '/signed/owned');
  assert.equal(state.jobs[1].lateResultAvailable, true);
  assert.equal(state.jobs[2].imageUrl, '');
  assert.equal(state.status, 'partial');
  assert.equal(store.db.jobs.length, 0);
  assert.equal(store.db.images.length, 0);
  assert.ok(!JSON.stringify(state).includes('STA1N-'));
  assert.ok(!JSON.stringify(state).includes('prompt'));
  await assert.rejects(ctx.getGenerationRequest('parent', 'b'), error => error.statusCode === 404);
  await assert.rejects(ctx.cancelGenerationRequest('parent', 'b'), error => error.statusCode === 404);
  const plan = store.sqlite.prepare('EXPLAIN QUERY PLAN SELECT id FROM jobs WHERE request_id = ?').all('parent');
  assert.ok(plan.some(row => row.detail.includes('idx_jobs_request_status')));
});

test('取消、任务退款和账本同事务：写失败不通知，重复取消不再退款，running 继续且不复活余图', async t => {
  const { store, ctx, notifications } = await fixture(t, ['queued', 'pending', 'running']);
  await store.update(db => {
    for (const job of db.jobs.filter(job => job.status !== 'pending')) reserveFairQuota(db, db.users[0], job, db.accounts[0]);
    db.accounts[0].inFlight = 1;
    db.jobs.find(job => job.status === 'running').accountId = 'opus';
  }, { immediate: true });
  const before = await store.read();
  store.sqlite.exec("CREATE TEMP TRIGGER reject_cancel BEFORE INSERT ON jobs WHEN NEW.id='child-1' BEGIN SELECT RAISE(ABORT, 'synthetic cancel failure'); END");
  await assert.rejects(ctx.cancelGenerationRequest('parent', 'a'), /synthetic cancel failure/);
  assert.deepEqual(await store.read(), before);
  assert.equal(notifications.length, 0);
  store.sqlite.exec('DROP TRIGGER reject_cancel');
  const results = await Promise.all([ctx.cancelGenerationRequest('parent', 'a'), ctx.cancelGenerationRequest('parent', 'a')]);
  assert.deepEqual(results.map(result => result.cancelledCount), [2, 0]);
  assert.equal(notifications.length, 2);
  assert.equal(store.selectItemById('jobs', 'child-0').fairCharge.status, 'refunded');
  assert.equal(store.selectItemById('jobs', 'child-2').fairCharge.status, 'reserved');
  assert.equal(store.db.accounts[0].inFlight, 1);
  assert.equal(store.db.ledger.filter(event => event.type === 'generation_refunded').length, 1);
  assert.equal((await ctx.getGenerationRequest('parent', 'a')).cancellable, false);
  await store.update(db => {
    const running = db.jobs.find(job => job.id === 'child-2');
    settleFairQuota(db, running, true); running.status = 'done';
    activateNextBatchJob(db, running, store.nextQueueSequence());
  }, { immediate: true, includeSettings: true, dirtyRows: { jobs: ['child-2'] } });
  assert.equal(store.selectItemById('jobs', 'child-1').status, 'failed');
});

test('取消与派发按同一事务顺序竞争，已派发不撤销，未派发不越过取消', async t => {
  for (const dispatchFirst of [true, false]) {
    const { store, ctx } = await fixture(t, ['queued', 'pending']);
    const dispatch = () => ctx.reserveQueuedJob('child-0');
    const operations = dispatchFirst ? [dispatch(), ctx.cancelGenerationRequest('parent', 'a')]
      : [ctx.cancelGenerationRequest('parent', 'a'), dispatch()];
    await Promise.all(operations);
    assert.equal(store.selectItemById('jobs', 'child-0').status, dispatchFirst ? 'running' : 'failed');
    assert.equal(store.selectItemById('jobs', 'child-1').status, 'failed');
  }
});

test('部分冷历史下取消只持久化本父待执行任务，终态重复取消不触发全量覆盖', async t => {
  const { store, ctx } = await fixture(t, ['queued', 'pending']);
  await store.update(db => {
    for (let index = 0; index < 101; index++) db.jobs.unshift({ id: `history-${index}`, status: 'done', createdAt: new Date().toISOString() });
  }, { immediate: true });
  store.trimRuntimeCollection('jobs', 0);
  store.refreshPartialCollectionState('jobs');
  assert.equal(store.partialCollections.has('jobs'), true);
  assert.equal((await ctx.cancelGenerationRequest('parent', 'a')).cancelledCount, 2);
  assert.equal((await ctx.cancelGenerationRequest('parent', 'a')).cancelledCount, 0);
  assert.equal(store.countRecords('jobs'), 103);
  assert.ok(store.selectItemById('jobs', 'history-100'));
  assert.equal(store.activeGenerationPayloadBytes(), 0);
});
