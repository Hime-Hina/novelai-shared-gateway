import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import { JsonStore } from '../../server/store.js';
import { configureFairQuota, syncFairQuota, reserveFairQuota, settleFairQuota } from '../../server/fair-quota.js';
import { assertQueueAdmission, assertMemberAcceptsJobs, setMemberAdmission, queueDeadline } from '../../server/queue-policy.js';
import { createMemberSession, authenticateMemberSession } from '../../server/member-auth.js';

const request = { model: 'nai-diffusion-5-full', width: 1024, height: 1024, steps: 23 };
const writeOptions = { collections: ['users', 'jobs', 'settings', 'ledger'], immediate: true };
const refund = (db, job) => settleFairQuota(db, job, false);

async function fixture(t) {
  const directory = await mkdtemp(path.join(tmpdir(), 'nai-queue-admission-'));
  const store = new JsonStore(directory);
  await store.init();
  t.after(() => store.close());
  await store.update(db => {
    db.users = ['a', 'b', 'c', 'd'].map(id => ({ id, token: id, enabled: true, balance: 0, memberAuth: { username: id, version: 'synthetic-session' } }));
    db.accounts = [{ id: 'opus', enabled: true, quotaTier: 3, subscriptionActive: true, quotaFixed: 1000,
      quotaPurchased: 0, v5UsagePercent: 100, quotaCheckedAt: Date.now(), inFlight: 0 }];
    configureFairQuota(db, ['a', 'b', 'c', 'd']);
    syncFairQuota(db);
  }, { immediate: true });
  return { store, directory };
}

test('持续并发提交与停止串行提交，既不超上限也不留下停止后的排队任务', async t => {
  const { store } = await fixture(t);
  const admit = index => store.update(db => {
    const user = db.users[0];
    assertQueueAdmission(db, user, undefined, {});
    db.jobs.push({ id: `job-${index}`, userToken: user.token, status: 'queued', request,
      createdAt: new Date().toISOString(), queueExpiresAt: queueDeadline(Date.now(), '', {}) });
  }, { collections: ['jobs'], immediate: true });
  const first = await Promise.allSettled(Array.from({ length: 20 }, (_, index) => admit(index)));
  assert.equal(first.filter(result => result.status === 'fulfilled').length, 4);
  assert.ok(first.filter(result => result.status === 'rejected').every(result => result.reason.statusCode === 429));
  const stop = store.update(db => setMemberAdmission(db, 'a', false, refund), writeOptions);
  const later = await Promise.allSettled(Array.from({ length: 20 }, (_, index) => admit(index + 20)));
  assert.equal((await stop).cancelledJobIds.length, 4);
  assert.ok(later.every(result => result.status === 'rejected' && result.reason.statusCode === 403));
  const db = await store.read();
  assert.equal(db.jobs.filter(job => job.status === 'queued').length, 0);
  assert.equal(db.users[0].acceptingJobs, false);
});

test('停止事务写失败整体回滚，成功后取消预留只退一次且在途继续结算', async t => {
  const { store, directory } = await fixture(t);
  await store.update(db => {
    for (const status of ['queued', 'pending', 'running']) {
      const job = { id: status, userToken: 'a', status, request, createdAt: new Date().toISOString(), queueExpiresAt: queueDeadline(Date.now(), '') };
      db.jobs.push(job);
      reserveFairQuota(db, db.users[0], job, db.accounts[0]);
    }
    db.accounts[0].inFlight = 1;
  }, { immediate: true });
  const before = await store.read(), token = createMemberSession(before.users[0]);
  store.sqlite.exec("CREATE TEMP TRIGGER reject_stop BEFORE INSERT ON jobs BEGIN SELECT RAISE(ABORT, 'injected stop failure'); END");
  await assert.rejects(store.update(db => setMemberAdmission(db, 'a', false, refund), writeOptions), /injected stop failure/);
  assert.deepEqual(await store.read(), before);
  store.sqlite.exec('DROP TRIGGER reject_stop');
  assert.deepEqual((await store.update(db => setMemberAdmission(db, 'a', false, refund), writeOptions)).cancelledJobIds, ['queued', 'pending']);
  const stopped = await store.read();
  assert.equal(stopped.accounts[0].inFlight, 1);
  assert.equal(stopped.jobs.find(job => job.id === 'running').fairCharge.status, 'reserved');
  assert.equal(authenticateMemberSession(stopped, token)?.id, 'a');
  const balance = stopped.settings.fairQuota.balances.a.v5Available;
  await store.update(db => setMemberAdmission(db, 'a', false, refund), writeOptions);
  assert.equal((await store.read()).settings.fairQuota.balances.a.v5Available, balance);
  await store.close();
  const reopened = new JsonStore(directory);
  await reopened.init();
  t.after(() => reopened.close());
  assert.equal((await reopened.read()).users[0].acceptingJobs, false);
  assert.equal((await reopened.read()).jobs.find(job => job.id === 'queued').queueExpiresAt, before.jobs.find(job => job.id === 'queued').queueExpiresAt);
  for (const id of ['queued', 'pending']) {
    const refunded = reopened.selectItemById('ledger', `attempt:${id}:1:generation_refunded`);
    assert.equal(refunded.response.dispatchState, 'not_sent');
    assert.equal(reopened.selectItemById('ledger', `attempt:${id}:1:generation_loss_suspected`), null);
  }
  await reopened.update(db => {
    const running = db.jobs.find(job => job.id === 'running');
    settleFairQuota(db, running, true);
    running.status = 'done';
    db.accounts[0].inFlight = 0;
    setMemberAdmission(db, 'a', true, refund);
    assert.throws(() => assertMemberAcceptsJobs(db.users[0], 0), error => error.statusCode === 409);
    assert.equal(db.jobs.find(job => job.id === 'queued').status, 'failed');
  }, { immediate: true });
  assert.equal((await reopened.read()).jobs.find(job => job.id === 'running').fairCharge.status, 'settled');
});

test('等待者补查不通知未提交的取消，已经提交的取消也不会错过', async t => {
  const { store } = await fixture(t);
  const jobWaiters = new Map();
  const context = vm.createContext({ store, jobWaiters, setTimeout, clearTimeout, setMemberAdmission,
    refundJob: refund, clearJobStreamProgress() {}, scheduleQueueDrain() {} });
  const source = await readFile(new URL('../../server/index.js', import.meta.url), 'utf8');
  for (const name of ['waitForJobResult', 'removeJobWaiter', 'notifyJobWaiters', 'changeMemberAdmission']) {
    const start = source.indexOf(`function ${name}(`), end = source.indexOf('\n}', start) + 2;
    vm.runInContext((source.slice(start - 6, start) === 'async ' ? 'async ' : '') + source.slice(start, end), context);
  }
  await store.update(db => { db.jobs.push({ id: 'waiting', userToken: 'a', status: 'queued', request }); }, { immediate: true });
  let entered, release;
  const mutationStarted = new Promise(resolve => { entered = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  const failingStop = store.update(async db => {
    setMemberAdmission(db, 'a', false, refund);
    entered();
    await gate;
    throw new Error('stop transaction rolled back');
  }, writeOptions);
  const rejected = assert.rejects(failingStop, /rolled back/);
  await mutationStarted;
  const waiting = context.waitForJobResult('waiting', Date.now() + 3000);
  release();
  await rejected;
  await store.update(() => null, { persist: false });
  assert.equal(jobWaiters.get('waiting')?.size, 1);
  assert.equal((await store.read()).jobs[0].status, 'queued');
  await context.changeMemberAdmission('a', false);
  assert.equal((await waiting).statusCode, 409);
  // 注册发生在取消提交之后时，由串行补查立即返回，不等到请求超时。
  assert.equal((await context.waitForJobResult('waiting', Date.now() + 3000)).statusCode, 409);
});

test('图片缓存命中也保留原批次停止代次，不能给旧请求换成恢复后的身份', async t => {
  const { store } = await fixture(t);
  await store.update(db => {
    db.users[0].jobAdmissionVersion = 1;
    db.images.push({ id: 'cached', token: 'a', cacheKey: 'synthetic-cache', mimeType: 'image/png' });
  }, { immediate: true });
  const context = vm.createContext({ store, structuredClone, assertMemberAcceptsJobs,
    assertFairMemberReady: async () => {}, cleanupStaleActiveJobs: async () => {},
    isNoCache: () => false, requestCacheKey: () => 'synthetic-cache', dirtyResultJobRows: () => ({}),
    httpError: (statusCode, message) => Object.assign(new Error(message), { statusCode }) });
  const source = await readFile(new URL('../../server/index.js', import.meta.url), 'utf8');
  for (const name of ['createJob', 'createDirectJob', 'getUserOrThrow']) {
    const start = source.indexOf(`function ${name}(`), end = source.indexOf('\n}', start) + 2;
    vm.runInContext((source.slice(start - 6, start) === 'async ' ? 'async ' : '') + source.slice(start, end), context);
  }
  await assert.rejects(context.createJob('a', request, { native: true, admissionVersion: 0 }), error => error.statusCode === 409);
  assert.equal((await store.read()).jobs.length, 0);
});
