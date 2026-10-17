import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { JsonStore } from '../../server/store.ts';

function env(t, name, value) {
  const previous = process.env[name]; process.env[name] = value;
  t.after(() => { if (previous === undefined) delete process.env[name]; else process.env[name] = previous; });
}
async function open(t, directory) {
  env(t, 'NO_AUTO_DELETE', 'true');
  const store = new JsonStore(directory || await mkdtemp(path.join(tmpdir(), 'nai-activity-')));
  t.after(() => store.close()); await store.init();
  return store;
}
const window = { since: '2025-10-17T16:00:00.000Z', until: '2026-10-17T16:00:00.000Z' };
const submitted = '2026-10-15T16:00:00.000Z';
const job = (id, fields = {}) => ({ id, userToken: 'key-a', createdAt: submitted, status: 'done', cost: 0, ...fields });
const parent = (id, member = 'a', operation = 'generate') => ({ id, member_id: member, source: 'novelai-native',
  operation, created_at: submitted, payload_bytes: 2, payload: '{}' });
const empty = date => ({ date, total: 0, done: 0, failed: 0, cancelled: 0, queued: 0, pending: 0, running: 0, other: 0 });

test('逐张生图按提交日分类，免费/缓存计入、取消互斥、迟到仍失败，辅助操作独立', async t => {
  const store = await open(t);
  await store.update(db => {
    db.users = [{ id: 'a', token: 'key-a' }];
    db.jobs = ['done', 'failed', 'queued', 'pending'].map((status, index) => job(`child-${index}`, {
      requestId: 'batch', sampleIndex: index, status, ...(status === 'failed' ? { cancelledAt: submitted } : {}) }));
    db.jobs.push(job('free', { fairCharge: { resource: 'free', status: 'settled' } }), job('cache', { imageId: 'cached' }),
      job('late', { status: 'failed', errorStatus: 409, lateResultAvailable: true, imageId: 'late-image' }),
      job('running', { status: 'running' }), job('unknown', { status: 'future' }),
      job('img2img', { request: { action: 'img2img' } }), job('infill', { generationAttempt: { operation: 'infill' } }),
      job('upscale', { request: { operation: 'upscale' } }), job('vibe', { request: { operation: 'encode-vibe' } }));
  }, { immediate: true, generationRequest: parent('batch') });
  assert.deepEqual(await store.readActivityDays(window), [{ ...empty('2026-10-16'), total: 11, done: 5,
    cancelled: 1, failed: 1, queued: 1, pending: 1, running: 1, other: 1 }]);
  assert.deepEqual(await store.readActivityDays({ ...window, operation: 'upscale' }), [{ ...empty('2026-10-16'), total: 1, done: 1 }]);
  assert.deepEqual(await store.readActivityDays({ ...window, operation: 'encode-vibe' }), [{ ...empty('2026-10-16'), total: 1, done: 1 }]);
  await store.update(() => null, { collections: [], generationIdempotency: { member_id: 'a', operation: 'generate',
    key_hash: 'a'.repeat(64), fingerprint: 'b'.repeat(64), normalization_context: '{}', job_id: 'cache', request_id: null, created_at: submitted } });
  assert.equal((await store.readActivityDays(window))[0].total, 11);
  assert.deepEqual(await store.readActivityDays({ ...window, dates: ['2026-10-15'] }), []);
  assert.deepEqual(await store.readActivityDays({ ...window, dates: [] }), []);
});

test('成员归属父表优先，旧attempt/凭据其次；未知只进全局，Key轮换不挪走稳定归属', async t => {
  const store = await open(t);
  await store.update(db => {
    db.users = [{ id: 'a', token: 'key-a' }, { id: 'b', token: 'key-b' }];
    db.jobs = [job('parent', { requestId: 'parent', userToken: 'key-b', generationAttempt: { memberId: 'b' } }),
      job('attempt', { userToken: 'key-b', generationAttempt: { memberId: 'a' }, fairCharge: { userId: 'b' } }),
      job('receipt', { userToken: 'key-b', fairCharge: { userId: 'a' } }), job('token'), job('orphan', { userToken: 'old-secret' })];
  }, { immediate: true, generationRequest: parent('parent') });
  assert.equal((await store.readActivityDays({ ...window, memberId: 'a' }))[0].total, 4);
  assert.deepEqual(await store.readActivityDays({ ...window, memberId: 'b' }), []);
  assert.equal((await store.readActivityDays(window))[0].total, 5);
  await store.update(db => { db.users[0].token = 'rotated-key'; }, { immediate: true, dirtyRows: { users: ['a'] } });
  assert.equal((await store.readActivityDays({ ...window, memberId: 'a' }))[0].total, 3);
  const serialized = JSON.stringify(await store.readActivityDays(window));
  for (const secret of ['key-a', 'key-b', 'old-secret', 'rotated-key', 'memberId']) assert.ok(!serialized.includes(secret));
});

test('精确毫秒左闭右开、台北午夜、合法ISO偏移与非法日期，不用字符串边界误漏', async t => {
  const store = await open(t);
  const values = [
    '2026-10-15T16:00:00.000Z', '2026-10-16T00:00:00+08:00', '2026-10-15T00:01:00-23:59',
    '2026-10-17T15:59:59.999+23:59', '2026-10-16T15:59:59.999Z',
    '2026-10-15T15:59:59.999Z', '2026-10-16T16:00:00.000Z',
    '2026-02-30T00:00:00Z', '2026-10-16', '2026-10-16T00:00:00', '2026-10-16T24:00:00Z', 'invalid'
  ];
  await store.update(db => { db.jobs = values.map((createdAt, index) => job(String(index), { createdAt })); }, { immediate: true });
  const oneDay = { since: '2026-10-15T16:00:00.000Z', until: '2026-10-16T16:00:00.000Z' };
  assert.deepEqual(await store.readActivityDays(oneDay), [{ ...empty('2026-10-16'), total: 4, done: 4 }]);
  const all = await store.readActivityDays({ since: '2026-01-01T00:00:00.000Z', until: '2027-01-01T00:00:00.000Z' });
  assert.equal(all.reduce((sum, day) => sum + day.total, 0), 7);
  assert.deepEqual(await store.readActivityDays({ ...window, dates: ['2026-10-15'] }), [{ ...empty('2026-10-15'), total: 1, done: 1 }]);
});

test('全年SQL聚合覆盖大量冷历史与重启，不热载，现有日期索引缩小候选集', async t => {
  env(t, 'RUNTIME_JOBS_CACHE_LIMIT', '2');
  const store = await open(t);
  await store.update(db => {
    db.jobs = Array.from({ length: 12000 }, (_, index) => job(`cold-${index}`, {
      createdAt: new Date(Date.parse(window.since) + (index % 365) * 86400000).toISOString() }));
    db.jobs.push(job('before-window', { createdAt: '2024-01-01T00:00:00Z' }));
  }, { immediate: true });
  assert.equal(store.db.jobs.length, 2); assert.equal(store.hasPartialCollection('jobs'), true);
  const cachedIds = store.db.jobs.map(row => row.id), started = performance.now();
  const rows = await store.readActivityDays(window);
  t.diagnostic(`12000 retained tasks grouped in ${Math.round(performance.now() - started)} ms`);
  assert.equal(rows.length, 365); assert.equal(rows.reduce((sum, row) => sum + row.total, 0), 12000);
  assert.deepEqual(await store.readActivityDays({ ...window, dates: ['2026-01-01'] }), rows.filter(row => row.date === '2026-01-01'));
  assert.deepEqual(store.db.jobs.map(row => row.id), cachedIds);
  const plan = store.sqlite.prepare(`EXPLAIN QUERY PLAN SELECT j.id FROM jobs j INDEXED BY idx_jobs_created_stats
    LEFT JOIN generation_requests p ON p.id = j.request_id WHERE j.created_at >= ? AND j.created_at < ?`)
    .all('2025-10-15', '2026-10-19');
  assert.ok(plan.some(row => /SEARCH j USING INDEX idx_jobs_created_stats.*created_at>.*created_at</.test(row.detail)));
  await store.close(); const reopened = await open(t, store.dataDir);
  assert.deepEqual(await reopened.readActivityDays(window), rows);
  assert.equal(reopened.db.jobs.length, 2);
});

test('读取等待串行事务并flush延迟写，SQL失败不能进入已提交统计', async t => {
  env(t, 'SQLITE_BATCH_WRITES', 'true'); env(t, 'SQLITE_WRITE_DEBOUNCE_MS', '60000');
  const store = await open(t), events = [];
  store.events.on('commit', event => events.push(event));
  let enter, release; const entered = new Promise(resolve => { enter = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  const update = store.update(async db => { db.jobs.push(job('waiting')); enter(); await gate; }, { dirtyRows: { jobs: ['waiting'] } });
  await entered;
  let resolved = false; const reading = store.readActivityDays(window).then(rows => { resolved = true; return rows; });
  await new Promise(resolve => setImmediate(resolve)); assert.equal(resolved, false); assert.equal(events.length, 0);
  release(); await update;
  assert.equal((await reading)[0].total, 1); assert.equal(store.pendingPersistScope, null);
  assert.deepEqual(events[0].activityChanges, [{ memberId: null, date: '2026-10-16' }]);
  store.sqlite.exec("CREATE TEMP TRIGGER fail_activity BEFORE INSERT ON jobs BEGIN SELECT RAISE(ABORT, 'synthetic activity failure'); END");
  await assert.rejects(store.update(db => { db.jobs.push(job('failed-write')); }, { immediate: true, dirtyRows: { jobs: ['failed-write'] } }), /synthetic activity failure/);
  store.sqlite.exec('DROP TRIGGER fail_activity');
  assert.equal((await store.readActivityDays(window))[0].total, 1);
  assert.equal(events.length, 1);
});

for (const dirty of [false, true]) test(`日期通知只看统计投影，变更前后成员/日期成对去重，失败零通知（${dirty ? 'dirty' : '普通'}）`, async t => {
  const store = await open(t), events = [];
  await store.update(db => { db.users = [{ id: 'a', token: 'key-a' }]; }, { immediate: true });
  store.events.on('commit', event => events.push(event));
  const options = dirty ? { immediate: true, dirtyRows: { jobs: ['one', 'two', 'unknown'] } } : { immediate: true, collections: ['jobs'] };
  await store.update(db => { db.jobs = [job('one', { requestId: 'parent', status: 'queued' }), job('two', { requestId: 'parent' }), job('unknown', { userToken: 'old-key' })]; },
    { ...options, generationRequest: parent('parent') });
  assert.deepEqual(events.at(-1).activityChanges, [{ memberId: 'a', date: '2026-10-16' }, { memberId: null, date: '2026-10-16' }]);
  await store.update(db => { db.jobs[0].updatedAt = '2026-10-17T00:00:00Z'; db.jobs[1].imageId = 'new-image'; }, options);
  assert.deepEqual(events.at(-1).activityChanges, []);
  await store.update(db => { db.jobs[0].status = 'running'; }, options);
  assert.deepEqual(events.at(-1).activityChanges, [{ memberId: 'a', date: '2026-10-16' }]);
  await store.update(db => { db.jobs[0].status = 'failed'; }, options);
  await store.update(db => { db.jobs[0].cancelledAt = submitted; }, options);
  assert.deepEqual(events.at(-1).activityChanges, [{ memberId: 'a', date: '2026-10-16' }]);
  await store.update(db => { Object.assign(db.jobs[0], { requestId: null, generationAttempt: { memberId: 'b' }, createdAt: '2026-10-16T16:00:00Z' }); }, options);
  assert.deepEqual(events.at(-1).activityChanges, [{ memberId: 'a', date: '2026-10-16' }, { memberId: 'b', date: '2026-10-17' }]);
  assert.ok(Object.isFrozen(events.at(-1).activityChanges)); assert.ok(Object.isFrozen(events.at(-1).activityChanges[0]));
  const before = events.length;
  store.sqlite.exec("CREATE TEMP TRIGGER fail_activity BEFORE INSERT ON jobs BEGIN SELECT RAISE(ABORT, 'synthetic event failure'); END");
  await assert.rejects(store.update(db => { db.jobs[0].status = 'done'; }, options), /synthetic event failure/);
  assert.equal(events.length, before); store.sqlite.exec('DROP TRIGGER fail_activity');
});

test('清除冷历史与全量write删除保留日期归属', async t => {
  env(t, 'RUNTIME_JOBS_CACHE_LIMIT', '2');
  const store = await open(t), events = [];
  await store.update(db => {
    db.jobs = Array.from({ length: 210 }, (_, index) => job(`history-${index}`, { requestId: 'parent' }));
  }, { immediate: true, generationRequest: parent('parent') });
  store.events.on('commit', event => events.push(event));
  await store.clearRequestLogs();
  assert.ok(events.length >= 2);
  assert.ok(events.every(event => JSON.stringify(event.activityChanges) === JSON.stringify([{ memberId: 'a', date: '2026-10-16' }])));
  await store.update(db => { db.jobs = [job('replace', { requestId: 'parent' })]; }, { immediate: true, collections: ['jobs'] });
  const replacement = await store.read(); replacement.jobs = []; await store.write(replacement);
  assert.deepEqual(events.at(-1).activityChanges, [{ memberId: 'a', date: '2026-10-16' }]);
  assert.deepEqual(await store.readActivityDays(window), []);
});
