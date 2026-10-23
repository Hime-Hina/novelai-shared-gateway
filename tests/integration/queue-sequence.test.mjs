import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { JsonStore } from '../../server/store.ts';

async function open(t, directory) {
  const store = new JsonStore(directory || await mkdtemp(path.join(tmpdir(), 'nai-queue-sequence-')));
  await store.init();
  t.after(() => store.close());
  return store;
}

function enqueue(store, id) {
  return store.update(db => {
    const job = { id, status: 'queued', queueSeq: store.nextQueueSequence(), createdAt: new Date().toISOString() };
    db.jobs.unshift(job);
    return { id, queueSeq: job.queueSeq };
  }, { immediate: true, dirtyRows: { jobs: [id] } });
}

test('旧活任务按升级前实际缓存反向顺序编号，历史顺序值保持不变，重启不重排', async t => {
  const directory = await mkdtemp(path.join(tmpdir(), 'nai-queue-sequence-legacy-'));
  const old = new Database(path.join(directory, 'library.sqlite'));
  try {
    old.exec(await readFile(new URL('../fixtures/sqlite-before-drizzle.sql', import.meta.url), 'utf8'));
    const insert = old.prepare('INSERT INTO jobs (id, order_value, data, status, created_at) VALUES (?, ?, ?, ?, ?)');
    for (const [id, order, createdAt] of [['a', 10, '2020-01-01'], ['b', 10, '2021-01-01'], ['c', 20, '2019-01-01']]) {
      insert.run(id, order, JSON.stringify({ id, status: 'queued', createdAt }), 'queued', createdAt);
    }
  } finally { old.close(); }
  const store = await open(t, directory);
  // 旧 active 索引枚举 c/a/b；稳定历史排序为 c/a/b；真正派发 reverse 得 b/a/c。
  assert.deepEqual(store.queuedJobIds(), ['b', 'a', 'c']);
  assert.deepEqual(store.sqlite.prepare('SELECT id, order_value FROM jobs ORDER BY id').all(),
    [{ id: 'a', order_value: 10 }, { id: 'b', order_value: 10 }, { id: 'c', order_value: 20 }]);
  const assigned = store.sqlite.prepare('SELECT id, queue_seq FROM jobs ORDER BY queue_seq').all();
  assert.deepEqual(assigned.map(row => row.queue_seq), [1, 2, 3]);
  await store.close();
  const reopened = await open(t, directory);
  assert.deepEqual(reopened.sqlite.prepare('SELECT id, queue_seq FROM jobs ORDER BY queue_seq').all(), assigned);
  assert.equal((await enqueue(reopened, 'new')).queueSeq, 4);
});

test('新序号与任务原子提交，失败不推进高水位，并发不重号，清空历史后不归零', async t => {
  const store = await open(t);
  assert.equal((await enqueue(store, 'first')).queueSeq, 1);
  store.sqlite.exec("CREATE TEMP TRIGGER reject_queue BEFORE INSERT ON jobs WHEN NEW.id = 'bad' BEGIN SELECT RAISE(ABORT, 'synthetic queue failure'); END");
  await assert.rejects(enqueue(store, 'bad'), /synthetic queue failure/);
  assert.equal(store.statements.readQueueSequence.get().value, '1');
  assert.deepEqual(store.queuedJobIds(), ['first']);
  store.sqlite.exec('DROP TRIGGER reject_queue');
  store.sqlite.exec("CREATE TEMP TRIGGER reject_counter BEFORE UPDATE ON app_meta WHEN NEW.key = 'queue_sequence' BEGIN SELECT RAISE(ABORT, 'synthetic counter failure'); END");
  await assert.rejects(enqueue(store, 'bad-counter'), /synthetic counter failure/);
  assert.equal(store.statements.readQueueSequence.get().value, '1');
  assert.deepEqual(store.queuedJobIds(), ['first']);
  store.sqlite.exec('DROP TRIGGER reject_counter');
  const accepted = await Promise.all([enqueue(store, 'second'), enqueue(store, 'third')]);
  assert.deepEqual(accepted.map(job => job.queueSeq), [2, 3]);
  const snapshot = await store.read();
  snapshot.jobs = [];
  await store.write(snapshot);
  await store.close();
  const reopened = await open(t, store.dataDir);
  assert.equal((await enqueue(reopened, 'after-replace')).queueSeq, 4);
});

test('SQL 派发与公开排队进度按 queueSeq，历史数组和 order_value 不决定派发', async t => {
  const store = await open(t);
  for (const id of ['running', 'b', 'a2']) await enqueue(store, id);
  await store.update(db => {
    db.jobs.find(job => job.id === 'running').status = 'running';
    db.jobs.forEach(job => { job.queueTotal = 3; });
    db.jobs.reverse();
  }, { immediate: true, collections: ['jobs'] });
  store.sqlite.prepare('UPDATE jobs SET order_value = -100 WHERE id = ?').run('a2');
  assert.deepEqual(store.queuedJobIds(), ['b', 'a2']);
  assert.equal((await store.findJobContext('b')).queue.progress, 2);
  assert.equal((await store.findJobContext('a2')).queue.progress, 1);
  const plan = store.sqlite.prepare(`EXPLAIN QUERY PLAN SELECT id FROM jobs INDEXED BY idx_jobs_queued_sequence
    WHERE status = 'queued' ORDER BY queue_seq ASC`).all();
  assert.ok(plan.some(row => row.detail.includes('idx_jobs_queued_sequence')));
  assert.ok(!plan.some(row => row.detail.includes('TEMP B-TREE')));
});
