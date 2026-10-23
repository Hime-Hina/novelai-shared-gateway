import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, cp, appendFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { JsonStore } from '../../server/store.ts';
import { migrateStorage, storageMigrationsDirectory } from '../../server/storage-migrations.ts';

function env(t, name, value) {
  const previous = process.env[name];
  process.env[name] = value;
  t.after(() => { if (previous === undefined) delete process.env[name]; else process.env[name] = previous; });
}
async function open(t, directory) {
  const store = new JsonStore(directory || await mkdtemp(path.join(tmpdir(), 'nai-pending-result-')));
  t.after(() => store.close());
  await store.init();
  return store;
}
const descriptor = id => ({ attemptId: `attempt:${id}:1`, imageId: `img_${id}`, file: `images/img_${id}.png`,
  operation: 'generate', mimeType: 'image/png', byteLength: 123, sha256: 'a'.repeat(64) });

test('写连接首次启动与重启都使用WAL和FULL同步', async t => {
  const store = await open(t);
  assert.equal(store.sqlite.pragma('journal_mode', { simple: true }), 'wal');
  assert.equal(store.sqlite.pragma('synchronous', { simple: true }), 2);
  await store.update(db => { db.jobs.push({ id: 'pending', status: 'failed', resultPending: descriptor('pending') }); }, { immediate: true });
  await store.close();
  const reopened = await open(t, store.dataDir);
  assert.equal(reopened.sqlite.pragma('journal_mode', { simple: true }), 'wal');
  assert.equal(reopened.sqlite.pragma('synchronous', { simple: true }), 2);
  assert.deepEqual(await reopened.readPendingResultJobIds(), ['pending']);
});

test('冷失败产物按ID游标遍历，未处理的前项不阻塞后页；查询不热载且恢复写保留排序', async t => {
  env(t, 'RUNTIME_JOBS_CACHE_LIMIT', '2');
  env(t, 'NO_AUTO_DELETE', 'false');
  const store = await open(t), recent = new Date().toISOString();
  await store.update(db => {
    db.jobs = Array.from({ length: 230 }, (_, index) => ({ id: `history-${index}`, status: 'done', createdAt: recent }));
    for (const [id, status] of [['pending-c', 'failed'], ['pending-a', 'failed'], ['pending-b', 'done']]) {
      db.jobs.push({ id, status, createdAt: new Date(1).toISOString(), resultPending: descriptor(id) });
    }
  }, { immediate: true });
  assert.equal(store.hasPartialCollection('jobs'), true);
  assert.ok(store.db.jobs.every(job => !job.resultPending));
  const cachedIds = store.db.jobs.map(job => job.id);
  assert.deepEqual(await store.readPendingResultJobIds('', 1), ['pending-a']);
  assert.deepEqual(await store.readPendingResultJobIds('pending-a', 1), ['pending-b']);
  assert.deepEqual(await store.readPendingResultJobIds('pending-b', 1), ['pending-c']);
  assert.deepEqual(await store.readPendingResultJobIds('pending-c', 1), []);
  assert.deepEqual(store.db.jobs.map(job => job.id), cachedIds);
  const plan = store.sqlite.prepare(`EXPLAIN QUERY PLAN SELECT id FROM jobs INDEXED BY idx_jobs_result_pending
    WHERE json_type(data, '$.resultPending') = 'object' AND id > @afterId ORDER BY id LIMIT @limit`).all({ afterId: '', limit: 100 });
  assert.ok(plan.some(row => /SEARCH.*idx_jobs_result_pending/.test(row.detail)));
  assert.ok(plan.every(row => !/TEMP B-TREE/.test(row.detail)));
  const before = store.sqlite.prepare("SELECT order_value FROM jobs WHERE id='pending-b'").get().order_value;
  await store.update(db => {
    const job = store.loadJobForUpdate('pending-b');
    delete job.resultPending;
    job.updatedAt = recent;
  }, { immediate: true, dirtyRows: { jobs: ['pending-b'] } });
  assert.equal(store.sqlite.prepare("SELECT order_value FROM jobs WHERE id='pending-b'").get().order_value, before);
  assert.deepEqual(await store.readPendingResultJobIds(), ['pending-a', 'pending-c']);
  const cleared = await store.clearRequestLogs();
  assert.equal(cleared.removed, 231);
  assert.equal(cleared.remaining, 2);
  assert.deepEqual(await store.readPendingResultJobIds(), ['pending-a', 'pending-c']);
  await store.close();
  const reopened = await open(t, store.dataDir);
  assert.deepEqual(await reopened.readPendingResultJobIds(), ['pending-a', 'pending-c']);
  assert.deepEqual(reopened.selectItemById('jobs', 'pending-a').resultPending, descriptor('pending-a'));
});

test('默认历史裁剪只移除普通旧终态，待恢复证据和活动任务保留', async t => {
  env(t, 'NO_AUTO_DELETE', 'false');
  const store = await open(t), old = new Date(1).toISOString();
  await store.update(db => {
    db.jobs = [
      { id: 'old-failed', status: 'failed', createdAt: old },
      { id: 'old-done', status: 'done', createdAt: old },
      { id: 'failed-result', status: 'failed', createdAt: old, resultPending: descriptor('failed-result') },
      { id: 'done-result', status: 'done', createdAt: old, resultPending: descriptor('done-result') },
      { id: 'active', status: 'running', createdAt: old }
    ];
  }, { immediate: true });
  assert.deepEqual(store.db.jobs.map(job => job.id), ['failed-result', 'done-result', 'active']);
  assert.deepEqual(await store.readPendingResultJobIds(), ['done-result', 'failed-result']);
  assert.equal((await store.clearRequestLogs()).removed, 0);
  assert.equal(store.countRecords('jobs'), 3);
});

test('冷查询等待串行写完成并刷出延迟写，不读未提交内存', async t => {
  env(t, 'SQLITE_BATCH_WRITES', 'true'); env(t, 'SQLITE_WRITE_DEBOUNCE_MS', '60000');
  const store = await open(t);
  await store.update(db => { db.jobs.push({ id: 'pending', status: 'failed', createdAt: new Date().toISOString() }); }, { immediate: true });
  let entered, release;
  const started = new Promise(resolve => { entered = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  const update = store.update(async db => { db.jobs[0].resultPending = descriptor('pending'); entered(); await gate; }, { dirtyRows: { jobs: ['pending'] } });
  await started;
  let resolved = false;
  const reading = store.readPendingResultJobIds().then(ids => { resolved = true; return ids; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(resolved, false);
  release(); await update;
  assert.deepEqual(await reading, ['pending']);
  assert.equal(store.pendingPersistScope, null);
  assert.ok(store.selectItemById('jobs', 'pending').resultPending);
});

test('记录或清除pending的SQL失败均恢复已提交描述，不遗失或补造恢复项', async t => {
  const store = await open(t);
  await store.update(db => { db.jobs.push({ id: 'pending', status: 'running', createdAt: new Date().toISOString() }); }, { immediate: true });
  const trigger = "CREATE TEMP TRIGGER reject_pending BEFORE INSERT ON jobs BEGIN SELECT RAISE(ABORT, 'synthetic pending write failure'); END";
  store.sqlite.exec(trigger);
  await assert.rejects(store.update(db => { db.jobs[0].resultPending = descriptor('pending'); },
    { immediate: true, dirtyRows: { jobs: ['pending'] } }), /synthetic pending write failure/);
  assert.deepEqual(await store.readPendingResultJobIds(), []);
  assert.equal(store.db.jobs[0].resultPending, undefined);
  store.sqlite.exec('DROP TRIGGER reject_pending');
  await store.update(db => { db.jobs[0].resultPending = descriptor('pending'); }, { immediate: true, dirtyRows: { jobs: ['pending'] } });
  store.sqlite.exec(trigger);
  await assert.rejects(store.update(db => { delete db.jobs[0].resultPending; db.jobs[0].status = 'done'; },
    { immediate: true, dirtyRows: { jobs: ['pending'] } }), /synthetic pending write failure/);
  assert.deepEqual(await store.readPendingResultJobIds(), ['pending']);
  assert.deepEqual(store.db.jobs[0].resultPending, descriptor('pending'));
  assert.equal(store.db.jobs[0].status, 'running');
});

test('0009只新增部分索引，重复迁移不改历史，改写已登记文件被拒绝', async t => {
  const store = await open(t);
  const columns = store.sqlite.prepare('PRAGMA table_info(jobs)').all();
  assert.equal(columns.some(column => column.name === 'result_pending'), false);
  assert.deepEqual(store.sqlite.prepare("PRAGMA index_info('idx_jobs_result_pending')").all().map(row => row.name), ['id']);
  const journal = store.sqlite.prepare('SELECT hash,created_at FROM __drizzle_migrations ORDER BY created_at').all();
  migrateStorage(store.sqlite);
  assert.deepEqual(store.sqlite.prepare('SELECT hash,created_at FROM __drizzle_migrations ORDER BY created_at').all(), journal);
  const copy = path.join(await mkdtemp(path.join(tmpdir(), 'nai-pending-migrations-')), 'migrations');
  await cp(storageMigrationsDirectory, copy, { recursive: true });
  await appendFile(path.join(copy, '0009_pending_generation_results.sql'), '\n-- changed applied migration\n');
  assert.throws(() => migrateStorage(store.sqlite, copy), /migration history differs/);
  assert.deepEqual(store.sqlite.prepare('SELECT hash,created_at FROM __drizzle_migrations ORDER BY created_at').all(), journal);
});
