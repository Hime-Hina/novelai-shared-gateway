import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import { JsonStore } from '../../server/store.ts';
import * as requests from '../../server/generation-request.ts';
import { assertQueueAdmission, queueDeadline, queuedDeadlineTimestamp } from '../../server/queue-policy.ts';
import { sharedAccountConcurrency, sharedAccountReadyDelay, recordSharedCompletion } from '../../server/shared-capacity.ts';
import { assertFairQuotaIdle, configureFairQuota, syncFairQuota, reserveFairQuota, settleFairQuota } from '../../server/fair-quota.ts';

const source = await readFile(new URL('../../server/gateway-runtime.js', import.meta.url), 'utf8');
const request = { model: 'nai-diffusion-4-5-full', action: 'generate', width: 1024, height: 1024, steps: 23,
  seed: 100, sampleCount: 4, prompt: 'synthetic large input', nativeParameters: { negative_prompt: 'x'.repeat(1024), seed: 100, n_samples: 4 } };
function functions(context, names) {
  const ctx = vm.createContext(context);
  for (const name of names) {
    const start = source.indexOf(`function ${name}(`), end = source.indexOf('\n}', start) + 2;
    vm.runInContext((source.slice(start - 6, start) === 'async ' ? 'async ' : '') + source.slice(start, end), ctx);
  }
  return ctx;
}
async function open(t, directory) {
  const store = new JsonStore(directory || await mkdtemp(path.join(tmpdir(), 'nai-native-batch-')));
  await store.init();
  t.after(() => store.close());
  return store;
}
function acceptance(store) {
  let id = 0;
  const budget = requests.createGenerationPayloadBudget(() => store.activeGenerationPayloadBytes());
  const ctx = functions({ store, Buffer, process, ...requests, assertQueueAdmission, queueDeadline,
    createId: prefix => `${prefix}-${++id}`, getUserOrThrow: (db, token) => db.users.find(user => user.token === token),
    assertFairMemberReady: async () => {}, cleanupStaleActiveJobs: async () => {},
    activeJobCount: jobs => jobs.filter(job => ['queued', 'running'].includes(job.status)).length
  }, ['resolveGenerationIdempotency', 'validateGenerationReplay', 'generationRequestRecord', 'createNativeBatch']);
  return (token, value = request) => {
    const claim = budget.claim();
    return ctx.createNativeBatch(token, value, 'synthetic-diagnostic', 0, claim).finally(() => claim.release());
  };
}

test('Native 全意图同事务受理：父正文仅一份，pending 计入上限、无预扣和队列序号，失败无半批记录', async t => {
  const store = await open(t);
  await store.update(db => { db.users = [{ id: 'a', token: 'a' }]; }, { immediate: true });
  const admit = acceptance(store);
  store.sqlite.exec("CREATE TEMP TRIGGER reject_child BEFORE INSERT ON jobs WHEN json_extract(NEW.data, '$.sampleIndex') = 2 BEGIN SELECT RAISE(ABORT, 'synthetic child failure'); END");
  await assert.rejects(admit('a'), /synthetic child failure/);
  assert.equal(store.countRecords('jobs'), 0);
  assert.equal(store.sqlite.prepare('SELECT COUNT(*) AS n FROM generation_requests').get().n, 0);
  assert.equal(store.activeGenerationPayloadBytes(), 0);
  store.sqlite.exec('DROP TRIGGER reject_child');
  const result = await admit('a');
  assert.equal(result.jobIds.length, 4);
  const jobs = result.jobIds.map(id => store.selectItemById('jobs', id));
  assert.deepEqual(jobs.map(job => job.status), ['queued', 'pending', 'pending', 'pending']);
  assert.equal(jobs[0].queueSeq, 1);
  assert.ok(jobs.slice(1).every(job => job.queueSeq === undefined && job.fairCharge === undefined));
  assert.ok(jobs.every(job => JSON.stringify(job).length < 1000 && !job.request.nativeParameters));
  assert.equal(new Set(jobs.map(job => job.queueExpiresAt)).size, 1);
  assert.equal(store.sqlite.prepare('SELECT COUNT(*) AS n FROM generation_requests').get().n, 1);
  assert.equal(store.activeGenerationPayloadBytes(), Buffer.byteLength(JSON.stringify(request)));
  await assert.rejects(admit('a', { ...request, sampleCount: 1 }), error => error.statusCode === 429);
  assert.throws(() => assertFairQuotaIdle(store.db), error => error.statusCode === 409);
});

test('重启保留全部 pending；成功让 A2 排在 B 后，原期限不变，失败/过期取消未启动意图', async t => {
  const store = await open(t);
  await store.update(db => { db.users = [{ id: 'a', token: 'a' }, { id: 'b', token: 'b' }]; }, { immediate: true });
  const admit = acceptance(store);
  const a = await admit('a'), b = await admit('b', { ...request, sampleCount: 1 });
  await store.close();
  const reopened = await open(t, store.dataDir);
  assert.equal(reopened.db.jobs.filter(job => job.status === 'pending').length, 3);
  const deadline = reopened.selectItemById('jobs', a.jobIds[1]).queueExpiresAt;
  await reopened.update(db => {
    const a1 = db.jobs.find(job => job.id === a.jobIds[0]);
    a1.status = 'done';
    requests.activateNextBatchJob(db, a1, reopened.nextQueueSequence());
  }, { immediate: true, dirtyRows: { jobs: a.jobIds } });
  assert.deepEqual(reopened.queuedJobIds(), [b.jobIds[0], a.jobIds[1]]);
  assert.equal(reopened.selectItemById('jobs', a.jobIds[1]).queueExpiresAt, deadline);
  await reopened.update(db => {
    const a2 = db.jobs.find(job => job.id === a.jobIds[1]);
    a2.status = 'failed';
    assert.equal(requests.cancelWaitingBatchJobs(db, a2, 'synthetic failure', 502).length, 2);
  }, { immediate: true, dirtyRows: { jobs: a.jobIds } });
  assert.deepEqual(a.jobIds.map(id => reopened.selectItemById('jobs', id).status), ['done', 'failed', 'failed', 'failed']);
  const expired = { jobs: [{ id: 'one', requestId: 'expired', status: 'done' },
    { id: 'two', requestId: 'expired', status: 'pending', sampleIndex: 1, queueExpiresAt: new Date(1).toISOString() }] };
  requests.activateNextBatchJob(expired, expired.jobs[0], 9);
  assert.equal(expired.jobs[1].status, 'failed');
  assert.equal(expired.jobs[1].queueSeq, undefined);
});

test('物理在途与 SQL 活动父去重：逻辑失败后仍占字节和上游槽，执行 finally 后才释放', async t => {
  const store = await open(t);
  await store.update(db => { db.users = [{ id: 'a', token: 'a' }]; }, { immediate: true });
  const a = await acceptance(store)('a');
  const job = store.db.jobs.find(job => job.id === a.jobIds[0]);
  job.accountId = 'opus';
  const runningJobControls = new Map();
  const ctx = functions({ runningJobControls, AbortController, setTimeout, clearTimeout,
    runningJobTimeoutDelay: () => 60000, abortRunningJob() {}, scheduleQueueDrain() {},
    fairShareMode: true, sharedAccountConcurrency, sharedAccountReadyDelay
  }, ['createRunningJobControl', 'finishRunningJobControl', 'physicalAccountLoad', 'availableAccountSlots']);
  const control = ctx.createRunningJobControl(job);
  t.after(() => ctx.finishRunningJobControl(job.id, control));
  const bytes = Buffer.byteLength(JSON.stringify(request));
  assert.equal(store.activeGenerationPayloadBytes([...runningJobControls.values()]), bytes);
  await store.update(db => {
    const first = db.jobs.find(item => item.id === job.id);
    first.status = 'failed';
    requests.cancelWaitingBatchJobs(db, first, 'synthetic timeout', 504);
  }, { immediate: true, dirtyRows: { jobs: a.jobIds } });
  assert.equal(store.activeGenerationPayloadBytes(), 0);
  assert.equal(store.activeGenerationPayloadBytes([...runningJobControls.values()]), bytes);
  assert.equal(ctx.availableAccountSlots([{ id: 'opus', enabled: true, inFlight: 0 }], { fairQuota: { boundAccountId: 'opus' } }), 0);
  ctx.finishRunningJobControl(job.id, control);
  assert.equal(store.activeGenerationPayloadBytes([...runningJobControls.values()]), 0);
  assert.equal(ctx.availableAccountSlots([{ id: 'opus', enabled: true, inFlight: 0 }], { fairQuota: { boundAccountId: 'opus' } }), 1);
});

test('错误日志按新父请求计算单张种子，旧 inline 已解析种子不重复偏移', () => {
  const ctx = functions({ store: { readGenerationRequest: () => request }, singleGenerationRequest: requests.singleGenerationRequest,
    maskToken: value => value, publicErrorMessage: value => value, errorDetailMessage: value => value,
    jobAccountCost: () => 0, jobDurationMs: () => 0, errorLogRequest: value => value, beijingDateKey: () => ''
  }, ['publicErrorLog']);
  assert.equal(ctx.publicErrorLog({ requestId: 'parent', sampleIndex: 1 }).request.seed, 101);
  assert.equal(ctx.publicErrorLog({ request: requests.singleGenerationRequest(request, 1), sampleIndex: 1 }).request.seed, 101);
});

test('启动未知在途和已过期父批次原子终止 pending；未过期的其他批次保留', async t => {
  const store = await open(t);
  await store.update(db => {
    db.users = ['a', 'b', 'c', 'd'].map(id => ({ id, token: id, enabled: true }));
    db.accounts = [{ id: 'opus', enabled: true, quotaTier: 3, subscriptionActive: true,
      quotaFixed: 1000, quotaPurchased: 0, v5UsagePercent: 100, quotaCheckedAt: Date.now(), inFlight: 0 }];
    configureFairQuota(db, ['a', 'b', 'c', 'd']);
    syncFairQuota(db);
  }, { immediate: true });
  const admit = acceptance(store);
  const a = await admit('a'), b = await admit('b', { ...request, sampleCount: 2 });
  await store.update(db => {
    const first = db.jobs.find(job => job.id === a.jobIds[0]);
    reserveFairQuota(db, db.users[0], first, db.accounts[0], Date.now(), requests.singleGenerationRequest(request));
    first.status = 'running';
    first.accountId = 'opus';
    first.upstreamStartedAt = new Date().toISOString();
    db.accounts[0].inFlight = 1;
  }, { immediate: true, includeSettings: true, dirtyRows: { jobs: a.jobIds, accounts: ['opus'] } });
  await store.close();
  const reopened = await open(t, store.dataDir);
  const update = reopened.update.bind(reopened);
  reopened.update = (fn, options) => update(fn, { ...options, includeSettings: true, immediate: true });
  const ctx = functions({ store: reopened, console, fairShareMode: true, queuedDeadlineTimestamp, recordSharedCompletion, notifyJobWaiters() {},
    cancelWaitingBatchJobs: requests.cancelWaitingBatchJobs, publicErrorMessage: value => value,
    refundJob: (db, job) => settleFairQuota(db, job, false)
  }, ['uniqueIds', 'mergeDirtyRows', 'batchJobIds', 'dirtyJobMutationRows', 'dirtyJobListRows', 'isExpiredJobAt', 'isNovelAiCapacityError', 'sharedFailureKind', 'cleanupInterruptedStartupJobs']);
  await ctx.cleanupInterruptedStartupJobs();
  assert.ok(a.jobIds.every(id => reopened.selectItemById('jobs', id).status === 'failed'));
  assert.equal(reopened.selectItemById('jobs', a.jobIds[0]).fairCharge.status, 'refunded');
  assert.deepEqual(Array.from(b.jobIds, id => reopened.selectItemById('jobs', id).status), ['queued', 'pending']);
  await reopened.update(db => {
    for (const job of db.jobs.filter(job => job.requestId === b.id)) job.queueExpiresAt = new Date(1).toISOString();
  }, { dirtyRows: { jobs: b.jobIds } });
  await ctx.cleanupInterruptedStartupJobs();
  assert.ok(b.jobIds.every(id => reopened.selectItemById('jobs', id).status === 'failed'));
  assert.equal(reopened.activeGenerationPayloadBytes(), 0);
  assert.ok(reopened.readGenerationRequest(a.id));
});
