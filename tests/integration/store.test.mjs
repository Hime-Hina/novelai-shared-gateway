import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { JsonStore } from '../../server/store.js';
import { configureFairQuota, reserveFairQuota, syncFairQuota } from '../../server/fair-quota.js';

function setEnv(t, name, value) {
  const previous = process.env[name];
  process.env[name] = value;
  t.after(() => { if (previous === undefined) delete process.env[name]; else process.env[name] = previous; });
}

async function fixture(t) {
  const store = new JsonStore(await mkdtemp(path.join(tmpdir(), 'nai-store-rollback-')));
  await store.init();
  t.after(() => store.close());
  await store.update(db => {
    db.users = ['a', 'b', 'c', 'd'].map(id => ({ id, token: `STA1N-${id}`, enabled: true, balance: 10 }));
    db.accounts = [{ id: 'opus', enabled: true, quotaTier: 3, subscriptionActive: true,
      quotaFixed: 10000, quotaPurchased: 0, v5UsagePercent: 80, quotaCheckedAt: Date.now() }];
    db.jobs = [{ id: 'queued', status: 'queued', userToken: 'STA1N-a', createdAt: new Date().toISOString(),
      request: { model: 'nai-diffusion-5-full', width: 1024, height: 1024, steps: 23 } }];
    configureFairQuota(db, ['a', 'b', 'c', 'd']);
    syncFairQuota(db);
  }, { immediate: true });
  return store;
}

function failJobWrites(store) {
  // SQLite 在 settings 写入之后拒绝任务行，验证真实事务回滚而非模拟整个 update 抛错。
  store.sqlite.exec(`CREATE TEMP TRIGGER fail_job_write BEFORE INSERT ON jobs
    BEGIN SELECT RAISE(ABORT, 'injected job write failure'); END`);
}

function persistedRows(store) {
  return Object.fromEntries(Object.entries(store.rowState).map(([collection, rows]) => [collection,
    new Map([...rows].map(([id, { data, order }]) => [id, { data, order }]))]));
}

for (const options of [
  { immediate: true },
  { collections: ['settings', 'accounts', 'jobs'], immediate: true },
  { includeSettings: true, dirtyRows: { accounts: ['opus'], jobs: ['queued'] }, immediate: true }
]) {
  test(`SQLite 写失败退回钱包、任务、账户及持久化索引：${JSON.stringify(options)}`, async t => {
    const store = await fixture(t);
    const before = await store.read();
    const rows = persistedRows(store);
    const orders = structuredClone(store.orderKeys);
    failJobWrites(store);
    await assert.rejects(store.update(db => {
      reserveFairQuota(db, db.users[0], db.jobs[0], db.accounts[0]);
      db.accounts[0].inFlight = 1;
      db.jobs[0].status = 'running';
    }, options), /injected job write failure/);
    assert.deepEqual(await store.read(), before);
    assert.deepEqual(persistedRows(store), rows);
    assert.deepEqual(store.orderKeys, orders);
    assert.equal(store.settingsState, JSON.stringify(before.settings));
    store.sqlite.exec('DROP TRIGGER fail_job_write');
    await store.update(db => { db.users[1].balance += 1; }, { collections: ['users'], immediate: true });
    await store.flush();
    const persisted = store.loadFromSqlite();
    assert.deepEqual(persisted.settings, before.settings);
    assert.deepEqual(persisted.jobs, before.jobs);
    assert.deepEqual(persisted.accounts, before.accounts);
    assert.equal(persisted.users[0].balance, 10);
    assert.equal(persisted.users[1].balance, 11);
  });
}

test('变更回调或持久化选项抛错后，下一次更新不夹带失败操作', async t => {
  const store = await fixture(t);
  const before = await store.read();
  await assert.rejects(store.update(async db => {
    db.users[0].balance = 0;
    db.settings.fairQuota.balances.a.v5Available = 0;
    await Promise.resolve();
    throw new Error('invalid operation');
  }, { collections: ['users', 'settings'], immediate: true }), /invalid operation/);
  assert.deepEqual(await store.read(), before);
  await assert.rejects(store.update(db => { db.jobs[0].status = 'running'; }, {
    dirtyRows() { throw new Error('invalid persistence scope'); }
  }), /invalid persistence scope/);
  assert.deepEqual(await store.read(), before);
  await store.update(db => { db.users[1].balance++; }, { collections: ['users'], immediate: true });
  assert.equal(store.loadFromSqlite().users[0].balance, 10);
});

test('先提交前一次延迟写，再执行可能失败的新变更，避免提前写入新变更', async t => {
  const store = await fixture(t);
  setEnv(t, 'SQLITE_BATCH_WRITES', 'true');
  setEnv(t, 'SQLITE_WRITE_DEBOUNCE_MS', '60000');
  await store.update(db => { db.users[0].balance = 11; }, { dirtyRows: { users: ['a'] } });
  assert.ok(store.pendingPersistScope);
  failJobWrites(store);
  await assert.rejects(store.update(db => {
    db.users[0].balance = 22;
    db.jobs[0].status = 'running';
  }, { collections: ['users', 'jobs'], immediate: true }), /injected job write failure/);
  assert.equal((await store.readUserByToken('STA1N-a')).balance, 11);
  assert.equal(store.loadFromSqlite().users[0].balance, 11);
  assert.equal(store.db.jobs[0].status, 'queued');
  assert.equal(store.pendingPersistScope, null);
  store.sqlite.exec('DROP TRIGGER fail_job_write');
});

test('延迟提交本身失败时保留待写范围，不执行后续变更，恢复后只提交一次', async t => {
  const store = await fixture(t);
  setEnv(t, 'SQLITE_BATCH_WRITES', 'true');
  setEnv(t, 'SQLITE_WRITE_DEBOUNCE_MS', '60000');
  await store.update(db => { db.jobs[0].status = 'running'; }, { dirtyRows: { jobs: ['queued'] } });
  failJobWrites(store);
  let mutated = false;
  await assert.rejects(store.update(() => { mutated = true; }), /injected job write failure/);
  assert.equal(mutated, false);
  assert.ok(store.pendingPersistScope);
  assert.equal(store.sqlite.prepare('SELECT status FROM jobs WHERE id = ?').get('queued').status, 'queued');
  store.sqlite.exec('DROP TRIGGER fail_job_write');
  await store.update(db => { db.users[0].balance++; }, { collections: ['users'], immediate: true });
  assert.equal(store.pendingPersistScope, null);
  assert.equal(store.loadFromSqlite().jobs[0].status, 'running');
});

test('部分历史缓存回滚后仍保留未加载的持久任务', async t => {
  const store = await fixture(t);
  setEnv(t, 'RUNTIME_JOBS_CACHE_LIMIT', '1');
  await store.update(db => {
    for (let i = 0; i < 5; i++) db.jobs.push({ id: `history-${i}`, status: 'done', createdAt: new Date().toISOString() });
  }, { collections: ['jobs'], immediate: true });
  assert.ok(store.partialCollections.has('jobs'));
  failJobWrites(store);
  await assert.rejects(store.update(db => { db.jobs.find(job => job.id === 'queued').status = 'running'; }, {
    dirtyRows: { jobs: ['queued'] }, immediate: true
  }), /injected job write failure/);
  assert.ok(store.partialCollections.has('jobs'));
  assert.equal(store.sqlite.prepare('SELECT COUNT(*) AS count FROM jobs').get().count, 6);
  assert.equal((await store.findJobContext('queued')).job.status, 'queued');
  assert.equal((await store.findJobContext('history-4')).job.status, 'done');
  const before = structuredClone(store.db);
  const replacement = structuredClone(before);
  replacement.users[0].balance = 0;
  await assert.rejects(store.write(replacement), /injected job write failure/);
  assert.deepEqual(store.db, before);
  assert.ok(store.partialCollections.has('jobs'));
  assert.equal(store.sqlite.prepare('SELECT COUNT(*) AS count FROM jobs').get().count, 6);
  store.sqlite.exec('DROP TRIGGER fail_job_write');
});

test('延迟写定时器失败保留待写变更，修复数据库后可继续提交', { timeout: 3000 }, async t => {
  const store = await fixture(t);
  setEnv(t, 'SQLITE_BATCH_WRITES', 'true');
  setEnv(t, 'SQLITE_WRITE_DEBOUNCE_MS', '5');
  const failure = new Promise(resolve => { t.mock.method(console, 'error', (...args) => resolve(args)); });
  failJobWrites(store);
  await store.update(db => { db.jobs[0].status = 'running'; }, { dirtyRows: { jobs: ['queued'] } });
  store.pendingPersistTimer.ref();
  const logged = await failure;
  assert.match(logged[0], /SQLite deferred persist failed/);
  assert.match(logged[1].message, /injected job write failure/);
  assert.ok(store.pendingPersistScope);
  assert.equal(store.sqlite.prepare('SELECT status FROM jobs WHERE id = ?').get('queued').status, 'queued');
  store.sqlite.exec('DROP TRIGGER fail_job_write');
  await store.update(db => { db.users[0].balance++; }, { collections: ['users'], immediate: true });
  assert.equal(store.pendingPersistScope, null);
  assert.equal(store.loadFromSqlite().jobs[0].status, 'running');
});
