import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import { JsonStore } from '../../server/store.js';
import * as fair from '../../server/fair-quota.ts';
import * as capacity from '../../server/shared-capacity.ts';
import * as requests from '../../server/generation-request.ts';
import { queuedDeadlineTimestamp } from '../../server/queue-policy.ts';

const source = await readFile(new URL('../../server/gateway-runtime.js', import.meta.url), 'utf8');
const NOW = 1800000000000;
function context(values, names) {
  const ctx = vm.createContext({ ...fair, ...capacity, ...requests, queuedDeadlineTimestamp, ...values });
  for (const name of names) {
    const start = source.indexOf(`function ${name}(`), end = source.indexOf('\n}', start) + 2;
    vm.runInContext((source.slice(start - 6, start) === 'async ' ? 'async ' : '') + source.slice(start, end), ctx);
  }
  return ctx;
}
async function fixture(t, directory) {
  const store = new JsonStore(directory || await mkdtemp(path.join(tmpdir(), 'nai-cooldown-')));
  await store.init(); t.after(() => store.close());
  const update = store.update.bind(store);
  store.update = (fn, options = {}) => update(fn, { ...options, immediate: true, includeSettings: true });
  let now = NOW;
  class ClockDate extends Date { constructor(...args) { super(...(args.length ? args : [now])); } static now() { return now; } }
  const notifications = [], logs = [];
  const ctx = context({ store, fairShareMode: true, Date: ClockDate, runningJobControls: new Map(),
    console: { log: (...args) => logs.push(args) }, publicErrorMessage: value => value, errorDetailMessage: e => e.message,
    scheduleQueueDrain() {}, clearJobStreamProgress() {}, notifyJobWaiters: (id, value) => notifications.push({ id, ...value }),
    isStaleActiveJob: job => job.status === 'running', staleActiveJobDetail: () => 'synthetic deadline'
  }, ['uniqueIds', 'mergeDirtyRows', 'batchJobIds', 'dirtyJobMutationRows', 'dirtyJobListRows', 'dirtyReservationJobRows',
    'isNovelAiCapacityError', 'sharedFailureKind', 'refundJob', 'failGeneration', 'cancelReservedJob', 'cleanupStaleActiveJobs',
    'isExpiredJobAt', 'cleanupInterruptedStartupJobs', 'physicalAccountLoad', 'availableAccountSlots', 'nextAccountReadyDelay']);
  if (!directory) await store.update(db => {
    db.users = ['a', 'b', 'c', 'd'].map(id => ({ id, token: id, enabled: true, balance: 0 }));
    db.accounts = [{ id: 'opus', enabled: true, quotaTier: 3, subscriptionActive: true, quotaFixed: 1000,
      quotaPurchased: 0, v5UsagePercent: 100, quotaCheckedAt: now, inFlight: 1, ordinaryStrikeCount: 2, capacityStrikeCount: 1 }];
    fair.configureFairQuota(db, ['a', 'b', 'c', 'd']); fair.syncFairQuota(db, now);
    const job = { id: 'sent', status: 'running', accountId: 'opus', userToken: 'a', createdAt: new Date(now).toISOString(),
      request: { model: 'nai-diffusion-5-full', width: 1024, height: 1024, steps: 23 } };
    db.jobs.push(job); fair.reserveFairQuota(db, db.users[0], job, db.accounts[0], now);
  });
  const reservation = { job: store.db.jobs[0], account: store.db.accounts[0], token: 'a', failurePhase: 'upstream',
    upstreamTiming: { upstreamStartedAt: new Date(now).toISOString() } };
  return { store, ctx, reservation, notifications, logs, advance: ms => { now += ms; } };
}

test('失败分类区分HTTP200流内容量、网络、认证/额度、本地提交及未发送取消', () => {
  const ctx = context({}, ['isNovelAiCapacityError', 'sharedFailureKind']);
  const sent = { failurePhase: 'upstream', upstreamTiming: { upstreamStartedAt: new Date(NOW).toISOString() } };
  for (const [error, phase, expected] of [
    [new Error('network disconnected'), 'upstream', 'ordinary'],
    [Object.assign(new Error('Concurrent generation is locked'), { upstreamStatus: 200 }), 'upstream', 'capacity'],
    [Object.assign(new Error('unrelated body'), { upstreamStatus: 429 }), 'upstream', 'capacity'],
    [Object.assign(new Error('Concurrent generation is locked'), { upstreamStatus: 401 }), 'upstream', 'ended'],
    [Object.assign(new Error('insufficient quota'), { upstreamStatus: 402 }), 'upstream', 'ended'],
    [Object.assign(new Error('disk full'), { code: 'ENOSPC' }), 'upstream', 'ended'],
    [new Error('synthetic local write failure'), 'commit', 'ended'],
    [new Error('cancelled'), 'preflight', 'ended']
  ]) assert.equal(ctx.sharedFailureKind({ ...sent, failurePhase: phase }, error), expected);
  assert.equal(ctx.sharedFailureKind({ ...sent, upstreamTiming: { ...sent.upstreamTiming, upstreamHttpStatus: 429 } }, new Error('body aborted')), 'capacity');
});

test('失败冷却和退款同一SQLite事务：写失败恢复内存，重试/重启不重计或延长', async t => {
  const { store, ctx, reservation, notifications } = await fixture(t);
  const initial = structuredClone(store.db.accounts[0]);
  const until = new Date(NOW + 300000).toISOString();
  const error = Object.assign(new Error('network disconnected'), { upstreamRetryAfterUntil: until });
  store.sqlite.exec("CREATE TEMP TRIGGER reject_cooldown BEFORE UPDATE ON accounts BEGIN SELECT RAISE(ABORT, 'synthetic cooldown failure'); END");
  await assert.rejects(ctx.failGeneration(reservation, error), /synthetic cooldown failure/);
  assert.deepEqual(store.db.accounts[0], initial);
  assert.equal(store.selectItemById('jobs', 'sent').status, 'running');
  assert.equal(store.selectItemById('jobs', 'sent').sharedCooldown, undefined);
  assert.equal(notifications.length, 0);
  store.sqlite.exec('DROP TRIGGER reject_cooldown');
  await ctx.failGeneration(reservation, error);
  assert.equal(store.db.accounts[0].ordinaryStrikeCount, 3);
  assert.equal(store.db.accounts[0].capacityStrikeCount, 1);
  assert.equal(store.db.accounts[0].cooldownUntil, until);
  assert.equal(store.db.accounts[0].cooldownReason, 'upstream_retry_after');
  assert.equal(notifications[0].cooldownUntil, until);
  assert.equal(store.readFairQuotaSettlement('sent').status, 'refunded');
  const final = structuredClone(store.db.accounts[0]);
  await store.close();
  const reopened = await fixture(t, store.dataDir); reopened.advance(60000);
  await reopened.ctx.failGeneration(reservation, error);
  await reopened.ctx.cleanupInterruptedStartupJobs();
  assert.deepEqual(reopened.store.db.accounts[0], final);
  assert.equal(reopened.notifications.length, 0);
});

test('超时清理先完成仍按活执行证据计一次，物理执行未结束不释放槽；429取消通知带截止', async t => {
  const { store, ctx, reservation, notifications } = await fixture(t);
  ctx.runningJobControls.set('sent', { accountId: 'opus', reservation });
  await ctx.cleanupStaleActiveJobs();
  assert.equal(store.db.accounts[0].ordinaryStrikeCount, 3);
  assert.equal(store.db.accounts[0].cooldownUntil, new Date(NOW + 40000).toISOString());
  assert.equal(notifications[0].cooldownUntil, store.db.accounts[0].cooldownUntil);
  await ctx.cancelReservedJob(reservation, new Error('late timeout callback'));
  assert.equal(store.db.accounts[0].ordinaryStrikeCount, 3);
  assert.equal(ctx.availableAccountSlots(store.db.accounts, store.db.settings), 0);
  const other = await fixture(t);
  other.reservation.upstreamTiming.upstreamHttpStatus = 429;
  other.reservation.upstreamTiming.upstreamRetryAfterUntil = new Date(NOW + 3600000).toISOString();
  other.ctx.runningJobControls.set('sent', { accountId: 'opus', reservation: other.reservation });
  await other.ctx.cleanupStaleActiveJobs();
  await other.ctx.cancelReservedJob(other.reservation, new Error('body aborted'));
  assert.equal(other.store.db.accounts[0].capacityStrikeCount, 2);
  assert.equal(other.store.db.accounts[0].ordinaryStrikeCount, 2);
  assert.equal(other.notifications[0].statusCode, 429);
  assert.equal(other.notifications[0].cooldownUntil, new Date(NOW + 3600000).toISOString());
  assert.equal(other.notifications.length, 1);
});

test('启动中断无发送证据仅结束间隔；排队过期无故障；其他账户不改变绑定账户唤醒时间', async t => {
  const { store, ctx, advance } = await fixture(t);
  await ctx.cleanupInterruptedStartupJobs();
  assert.equal(store.db.accounts[0].ordinaryStrikeCount, 2);
  assert.equal(store.db.accounts[0].cooldownReason, 'completion_gap');
  store.db.accounts.push({ id: 'other', enabled: true, inFlight: 0 });
  assert.equal(ctx.availableAccountSlots(store.db.accounts, store.db.settings), 0);
  assert.equal(ctx.nextAccountReadyDelay(store.db.accounts, store.db.settings), 10000);
  advance(10000);
  assert.equal(ctx.availableAccountSlots(store.db.accounts, store.db.settings), 1);
  const before = structuredClone(store.db.accounts[0]);
  await store.update(db => { db.jobs.push({ id: 'queued', status: 'queued', accountId: 'opus', queueExpiresAt: new Date(1).toISOString() }); });
  await ctx.cleanupInterruptedStartupJobs();
  assert.deepEqual(store.db.accounts[0], before);
});

test('Retry-After仅来自尚未到期真实截止，direct忙碌图维持SVG响应', () => {
  let output;
  const ctx = context({ buildErrorImage: () => ({ mimeType: 'image/svg+xml', buffer: Buffer.from('<svg/>') }),
    cooldownRetryAfter: until => capacity.cooldownRetryAfter(until, NOW), sendImage: (...args) => { output = args; }
  }, ['sendBusyImage', 'sendTimeoutImage']);
  for (const [until, expected] of [[undefined, undefined], [new Date(NOW).toISOString(), undefined], [new Date(NOW + 1001).toISOString(), '2']]) {
    ctx.sendBusyImage({}, until);
    assert.equal(output[1], 200); assert.equal(output[2], 'image/svg+xml');
    assert.equal(output[4]['retry-after'], expected);
    ctx.sendTimeoutImage({}, until);
    assert.equal(output[4]['retry-after'], expected);
  }
});

test('统计重置和停用/重新启用不能绕过共享账户冷却或释放在途计数', async () => {
  const account = { id: 'opus', inFlight: 1, ordinaryStrikeCount: 3, capacityStrikeCount: 2,
    cooldownUntil: new Date(NOW + 300000).toISOString(), cooldownReason: 'upstream_retry_after', lastUsedAt: new Date(NOW).toISOString(), total: 5, failures: 4 };
  const db = { accounts: [account], ledger: [] };
  const ctx = context({ fairShareMode: true, store: { update: fn => fn(db) }, collectValues: value => value, createId: () => 'audit' },
    ['resetAccountStats', 'disableNovelAiAccount']);
  const scheduling = ({ total, failures, updatedAt, enabled, ...state }) => state;
  const before = scheduling(account);
  await ctx.resetAccountStats({ ids: ['opus'] });
  assert.deepEqual(scheduling(account), before); assert.equal(account.total, 0); assert.equal(account.failures, 0);
  ctx.disableNovelAiAccount(account); account.enabled = true;
  assert.deepEqual(scheduling(account), before);
});

test('队首等待诊断只在原因/截止变化时记录，保留计划唤醒和实际检查时间', async () => {
  let now = NOW;
  class ClockDate extends Date { static now() { return now; } }
  const lines = [], db = { settings: { fairQuota: { status: 'active', boundAccountId: 'opus' } },
    accounts: [{ id: 'opus', inFlight: 1 }], jobs: [{ id: 'head', status: 'queued' }] };
  const controls = new Map([['previous', { accountId: 'opus' }]]);
  const ctx = context({ Date: ClockDate, console: { log: (_prefix, line) => lines.push(JSON.parse(line)) },
    queueDispatchEnabled: true, queueDraining: false, queueDrainRequested: false, lastQueueWait: '', fairShareMode: true, runningJobControls: controls,
    cleanupStaleActiveJobs: async () => {}, store: { update: fn => fn(db), queuedJobIds: () => ['head'] },
    queueRetryDelay: async () => 1000, scheduleQueueDrain() {}, isQueueActiveJob: () => true,
    reserveQueuedJob: async () => ({ job: db.jobs[0] }), runReservedJob() {}
  }, ['physicalAccountLoad', 'availableAccountSlots', 'nextAccountReadyDelay', 'drainQueuedJobs']);
  await ctx.drainQueuedJobs(now - 250); await ctx.drainQueuedJobs(now);
  assert.equal(lines.length, 1); assert.equal(lines[0].reason, 'physical_inflight');
  assert.equal(lines[0].checkedAt - lines[0].scheduledAt, 250);
  controls.clear(); await ctx.drainQueuedJobs(now);
  assert.equal(lines.at(-1).reason, 'account_inflight');
  db.accounts[0].inFlight = 0; db.accounts[0].cooldownUntil = new Date(now + 10000).toISOString();
  db.accounts[0].cooldownReason = 'success_gap'; await ctx.drainQueuedJobs(now);
  assert.equal(lines.at(-1).reason, 'cooldown'); assert.equal(lines.at(-1).cooldownReason, 'success_gap');
  now += 10000; await ctx.drainQueuedJobs(now - 700);
  assert.equal(lines.at(-2).reason, 'ready'); assert.equal(lines.at(-1).event, 'dispatch');
  assert.equal(lines.at(-1).dispatchedAt - lines.at(-1).scheduledAt, 700);
  db.accounts[0].enabled = false; await ctx.drainQueuedJobs(now);
  assert.equal(lines.at(-1).reason, 'eligibility');
});

test('跨冷却截止读取零槽后即使等待已为零仍保留唤醒，下一轮只派发一次', async () => {
  let now = NOW + 10000, firstRead = true;
  class ClockDate extends Date {
    static now() {
      if (firstRead) { firstRead = false; return now - 1; }
      return now;
    }
  }
  const scheduled = [], dispatched = [], lines = [];
  const db = { settings: { fairQuota: { status: 'active', boundAccountId: 'opus' } },
    accounts: [{ id: 'opus', inFlight: 0, cooldownUntil: new Date(now).toISOString(), cooldownReason: 'success_gap' }],
    jobs: [{ id: 'last', status: 'queued', queueExpiresAt: new Date(NOW + 600000).toISOString() }] };
  const ctx = context({ Date: ClockDate, console: { log: (_prefix, line) => lines.push(JSON.parse(line)) },
    queueDispatchEnabled: true, queueDraining: false, queueDrainRequested: false, lastQueueWait: '', fairShareMode: true,
    runningJobControls: new Map(), cleanupStaleActiveJobs: async () => {},
    store: { update: fn => fn(db), queuedJobIds: () => db.jobs.filter(job => job.status === 'queued').map(job => job.id), readCollections: async () => db },
    scheduleQueueDrain: delay => scheduled.push(delay), isQueueActiveJob: job => job.status === 'queued',
    reserveQueuedJob: async () => { db.jobs[0].status = 'running'; return { job: db.jobs[0] }; },
    runReservedJob: reservation => dispatched.push(reservation.job.id)
  }, ['physicalAccountLoad', 'availableAccountSlots', 'nextAccountReadyDelay', 'queueRetryDelay', 'drainQueuedJobs']);

  await ctx.drainQueuedJobs(now);
  assert.equal(lines.at(-1).reason, 'ready');
  assert.equal(dispatched.length, 0);
  assert.deepEqual(scheduled, [1000], '冷却刚过期不能丢掉最后一条排队任务的唤醒');
  assert.equal(db.accounts[0].cooldownUntil, new Date(NOW + 10000).toISOString());
  now += scheduled[0];
  await ctx.drainQueuedJobs(now);
  await ctx.drainQueuedJobs(now);
  assert.deepEqual(dispatched, ['last']);
  assert.deepEqual(scheduled, [1000]);
});
