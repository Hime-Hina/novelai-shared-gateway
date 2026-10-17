import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { JsonStore } from '../../server/store.js';

function env(t, name, value) {
  const previous = process.env[name]; process.env[name] = value;
  t.after(() => { if (previous === undefined) delete process.env[name]; else process.env[name] = previous; });
}
async function open(t) {
  const store = new JsonStore(await mkdtemp(path.join(tmpdir(), 'nai-store-events-')));
  t.after(() => store.close()); await store.init();
  await store.update(db => { db.users = ['a', 'b'].map(id => ({ id, token: `STA1N-secret-${id}`, enabled: true, balance: 1 })); }, { immediate: true });
  return store;
}
// 日期配对由 activity-storage 专项覆盖；本文件继续验证原业务失效范围。
function observe(store) { const events = []; store.events.on('commit', ({ activityChanges, ...event }) => events.push(event)); return events; }
const parent = id => ({ id, member_id: 'a', source: 'web', operation: 'generate', created_at: new Date().toISOString(), payload_bytes: 2, payload: '{}' });
const job = (id, overrides = {}) => ({ id, status: 'done', userToken: 'STA1N-secret-a', createdAt: new Date().toISOString(), ...overrides });

for (const dirty of [false, true]) {
  test(`提交事件在SQL和缓存更新后发出，回滚/纯读/no-op零事件（${dirty ? 'dirty' : '普通'}路径）`, async t => {
    const store = await open(t), events = observe(store), visible = [];
    assert.ok(store.events instanceof EventEmitter);
    store.events.on('commit', () => visible.push({ sql: store.selectItemById('users', 'a').balance,
      cache: store.db.users[0].balance, row: JSON.parse(store.rowState.users.get('a').data).balance }));
    const options = dirty ? { dirtyRows: { users: ['a'] }, immediate: true } : { collections: ['users'], immediate: true };
    store.sqlite.exec("CREATE TEMP TRIGGER reject_user BEFORE INSERT ON users BEGIN SELECT RAISE(ABORT, 'synthetic user failure'); END");
    await assert.rejects(store.update(db => { db.users[0].balance = 2; }, options), /synthetic user failure/);
    assert.equal(events.length, 0);
    store.sqlite.exec('DROP TRIGGER reject_user');
    await store.update(db => { db.users[0].balance = 2; }, options);
    assert.deepEqual(events, [{ collections: ['users'], memberIds: ['a'], allMembers: false }]);
    assert.deepEqual(visible, [{ sql: 2, cache: 2, row: 2 }]);
    await store.update(db => db.users[0], { persist: false });
    await store.update(() => {}, options);
    await store.read(); await store.flush();
    assert.equal(events.length, 1);
  });
}

test('延迟写完成前不通知，flush只通知一次，延期SQL失败也不通知', async t => {
  env(t, 'SQLITE_BATCH_WRITES', 'true'); env(t, 'SQLITE_WRITE_DEBOUNCE_MS', '60000');
  const store = await open(t), events = observe(store);
  await store.update(db => { db.users[0].balance = 2; }, { dirtyRows: { users: ['a'] } });
  assert.equal(events.length, 0); assert.equal(store.selectItemById('users', 'a').balance, 1);
  store.sqlite.exec("CREATE TEMP TRIGGER reject_deferred BEFORE INSERT ON users BEGIN SELECT RAISE(ABORT, 'synthetic defer failure'); END");
  assert.throws(() => store.flushSync(), /synthetic defer failure/); assert.equal(events.length, 0);
  store.sqlite.exec('DROP TRIGGER reject_deferred');
  store.flushSync(); store.flushSync();
  assert.deepEqual(events, [{ collections: ['users'], memberIds: ['a'], allMembers: false }]);
});

test('新增父请求、冷任务和旧attempt稳定归属优先于已轮换Key，不泄漏原行', async t => {
  const store = await open(t), events = observe(store);
  await store.update(db => { db.jobs.push(job('parent-child', { requestId: 'parent', userToken: 'STA1N-secret-b', request: { prompt: 'private-prompt' } })); },
    { dirtyRows: { jobs: ['parent-child'] }, generationRequest: parent('parent'), immediate: true });
  assert.deepEqual(events.pop(), { collections: ['jobs'], memberIds: ['a'], allMembers: false });
  await store.update(db => { db.jobs.push(job('legacy-attempt', { userToken: 'old-unmatched-token', generationAttempt: { memberId: 'b' } })); },
    { dirtyRows: { jobs: ['legacy-attempt'] }, immediate: true });
  assert.deepEqual(events.pop(), { collections: ['jobs'], memberIds: ['b'], allMembers: false });
  await store.update(db => { db.ledger.push({ id: 'ledger', jobId: 'parent-child', type: 'generation_refunded', token: 'old-unmatched-token' }); },
    { dirtyRows: { ledger: ['ledger'] }, immediate: true });
  assert.deepEqual(events.pop(), { collections: ['ledger'], memberIds: ['a'], allMembers: false });
  await store.update(db => { db.jobs.push(job('unknown', { userToken: 'unmatched-token' })); }, { dirtyRows: { jobs: ['unknown'] }, immediate: true });
  assert.deepEqual(events.pop(), { collections: ['jobs'], memberIds: [], allMembers: true });
  assert.equal(events.length, 0);
});

test('清日志覆盖未热载历史，每批提交后可读且空清理不通知', async t => {
  env(t, 'RUNTIME_JOBS_CACHE_LIMIT', '100');
  const store = await open(t);
  await store.update(db => {
    db.jobs = Array.from({ length: 230 }, (_, i) => job(`history-${i}`, { requestId: 'parent', userToken: 'retired-token' }));
  }, { collections: ['jobs'], generationRequest: parent('parent'), immediate: true });
  assert.equal(store.hasPartialCollection('jobs'), true);
  const events = observe(store), counts = [];
  store.events.on('commit', () => counts.push({ sql: store.countRecords('jobs'), cache: store.db.jobs.length }));
  const result = await store.clearRequestLogs();
  assert.equal(result.removed, 230); assert.equal(result.remaining, 0);
  assert.ok(events.length >= 2);
  assert.ok(events.every(event => event.collections.join() === 'jobs' && event.memberIds.join() === 'a' && !event.allMembers));
  assert.ok(counts.every(count => count.cache <= count.sql));
  assert.deepEqual(counts.at(-1), { sql: 0, cache: 0 });
  const before = events.length; await store.clearRequestLogs(); assert.equal(events.length, before);
});

test('显式图片清理和已有缓存裁剪提交后通知图片及关联任务，失败不通知', async t => {
  env(t, 'NO_AUTO_DELETE', 'false');
  const store = await open(t);
  await store.update(db => {
    db.jobs = [job('image-job', { imageId: 'one' })];
    db.images = [{ id: 'one', token: 'STA1N-secret-a' }, { id: 'two', token: 'STA1N-secret-b' }];
  }, { immediate: true });
  const events = observe(store), visible = [];
  store.events.on('commit', () => visible.push({ one: store.selectItemById('images', 'one'), image: store.db.jobs[0].imageId }));
  store.sqlite.exec("CREATE TEMP TRIGGER reject_image_delete BEFORE DELETE ON images BEGIN SELECT RAISE(ABORT, 'synthetic image failure'); END");
  await assert.rejects(store.clearCachedImages({ ids: ['one'] }), /synthetic image failure/);
  assert.equal(events.length, 0);
  store.sqlite.exec('DROP TRIGGER reject_image_delete');
  await store.clearCachedImages({ ids: ['one'] });
  assert.deepEqual(new Set(events[0].collections), new Set(['images', 'jobs', 'ledger']));
  assert.deepEqual(events[0].memberIds, ['a']); assert.equal(events[0].allMembers, false);
  assert.deepEqual(visible[0], { one: null, image: '' });
  await store.clearCachedImages({ ids: ['missing'] }); assert.equal(events.length, 1);
  await store.trimImageCache(0, { force: true });
  assert.deepEqual(events[1], { collections: ['images'], memberIds: ['b'], allMembers: false });
});

test('全量替换先同步缓存再通知，等价重写不通知；删除冷历史不漏归属', async t => {
  env(t, 'RUNTIME_JOBS_CACHE_LIMIT', '100');
  const store = await open(t);
  await store.update(db => { db.jobs = Array.from({ length: 130 }, (_, i) => job(`history-${i}`, { requestId: 'parent', userToken: 'retired-token' })); },
    { collections: ['jobs'], generationRequest: parent('parent'), immediate: true });
  const events = observe(store), visible = [];
  store.events.on('commit', () => visible.push([store.countRecords('jobs'), store.db.jobs.length, store.db.users[0].balance]));
  const replacement = await store.read(); replacement.jobs = []; replacement.users[0].balance = 5;
  await store.write(replacement);
  assert.deepEqual(new Set(events[0].collections), new Set(['users', 'jobs']));
  assert.deepEqual(events[0].memberIds, ['a']); assert.equal(events[0].allMembers, false);
  assert.deepEqual(visible, [[0, 0, 5]]);
  await store.write(await store.read()); assert.equal(events.length, 1);
  store.sqlite.exec("CREATE TEMP TRIGGER reject_replace BEFORE INSERT ON users BEGIN SELECT RAISE(ABORT, 'synthetic replace failure'); END");
  const failed = await store.read(); failed.users[0].balance = 6;
  await assert.rejects(store.write(failed), /synthetic replace failure/);
  assert.equal(events.length, 1); assert.equal(store.db.users[0].balance, 5);
});

test('settings实际变化通知全成员；幂等别名和纯结算归档不通知', async t => {
  const store = await open(t), events = observe(store);
  await store.update(db => { db.settings.fairQuota = { reservations: {} }; }, { collections: ['settings'], immediate: true });
  assert.deepEqual(events.pop(), { collections: ['settings'], memberIds: [], allMembers: true });
  await store.update(db => { db.settings.fairQuota.reservations.terminal = { status: 'settled', userId: 'a', amount: 1 }; }, { collections: ['settings'], immediate: true });
  assert.equal(store.readFairQuotaSettlement('terminal').userId, 'a');
  await store.update(() => null, { collections: [], generationIdempotency: { member_id: 'a', operation: 'generate', key_hash: 'a'.repeat(64),
    fingerprint: 'b'.repeat(64), normalization_context: '{}', job_id: 'old-cache-job', request_id: null, created_at: new Date().toISOString() } });
  assert.equal(events.length, 0);
});

test('会话签发/相同owner配置不通知，真实退出或owner撤销只发sessions与稳定成员ID', async t => {
  const store = await open(t), events = observe(store), owner = { ownerUserId: 'a', guildId: 'guild' };
  const session = { token_hash: 'a'.repeat(64), user_id: 'a', auth_version: 'v1', created_at: 1000, expires_at: 2000, owner_binding: null };
  await store.update(() => null, { collections: [], memberSessionOwner: owner, memberSession: session });
  await store.update(() => null, { collections: [], memberSessionOwner: owner });
  assert.equal(events.length, 0);
  await store.update(() => null, { collections: [], memberSessionDelete: session.token_hash });
  assert.deepEqual(events.pop(), { collections: ['sessions'], memberIds: ['a'], allMembers: false });
  await store.update(() => null, { collections: [], memberSessionDelete: session.token_hash }); assert.equal(events.length, 0);
  await store.update(() => null, { collections: [], memberSession: session });
  await store.update(() => null, { collections: [], memberSessionOwner: { ...owner, ownerUserId: 'b' } });
  assert.deepEqual(events.pop(), { collections: ['sessions'], memberIds: ['a'], allMembers: false });
});

test('同步和异步监听器异常均不影响提交和后续监听器，close通知并释放监听器', async t => {
  const store = await open(t), errors = [];
  const original = console.error; console.error = (...args) => errors.push(args);
  t.after(() => { console.error = original; });
  let received = 0, once = 0, closed = 0;
  store.events.on('commit', () => { throw new Error('synthetic sync listener'); });
  store.events.on('commit', async () => { throw new Error('synthetic async listener'); });
  store.events.once('commit', () => { once++; });
  store.events.on('commit', event => { received++; assert.deepEqual(Object.keys(event).sort(), ['activityChanges', 'allMembers', 'collections', 'memberIds']); });
  store.events.on('close', () => { throw new Error('synthetic close listener'); });
  store.events.on('close', () => { closed++; });
  await store.update(db => { db.users[0].balance++; }, { collections: ['users'], immediate: true });
  await store.update(db => { db.users[0].balance++; }, { collections: ['users'], immediate: true });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(store.selectItemById('users', 'a').balance, 3);
  assert.equal(received, 2); assert.equal(once, 1); assert.equal(errors.length, 4);
  await store.close(); assert.equal(closed, 1); assert.equal(errors.length, 5);
  assert.equal(store.events.listenerCount('commit'), 0); assert.equal(store.events.listenerCount('close'), 0);
});
