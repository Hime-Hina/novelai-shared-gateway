import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { JsonStore } from '../../server/store.js';

async function open(t, directory) {
  const store = new JsonStore(directory || await mkdtemp(path.join(tmpdir(), 'nai-idempotency-store-')));
  await store.init();
  t.after(() => store.close());
  return store;
}
function mapping(overrides = {}) {
  return { member_id: 'member-a', operation: 'generate', key_hash: 'a'.repeat(64), fingerprint: 'b'.repeat(64),
    normalization_context: JSON.stringify({ defaultModel: 'nai-diffusion-4-5-full', defaults: { steps: 23 } }),
    job_id: 'job', request_id: 'parent', created_at: new Date().toISOString(), ...overrides };
}
function parent() {
  const payload = JSON.stringify({ model: 'nai-diffusion-4-5-full', prompt: 'synthetic request', seed: 123 });
  return { id: 'parent', member_id: 'member-a', source: 'web', operation: 'generate',
    created_at: new Date().toISOString(), payload_bytes: Buffer.byteLength(payload), payload };
}
function admission(store, options = {}) {
  const record = mapping();
  return store.update(db => {
    db.settings.syntheticAdmission = 'committed';
    db.jobs.push({ id: 'job', requestId: 'parent', status: 'queued', queueSeq: store.nextQueueSequence(), createdAt: new Date().toISOString() });
    db.ledger.push({ id: 'reserve', jobId: 'job', type: 'reserve', amount: 0 });
    return 'job';
  }, { ...options, generationRequest: parent(), generationIdempotency: () => record });
}

for (const dirty of [false, true]) {
  test(`映射写失败与父/子/账本/序号/设置整体回滚，${dirty ? 'dirty rows' : '全量增量'}路径一致`, async t => {
    const store = await open(t);
    const before = await store.read();
    store.sqlite.exec("CREATE TEMP TRIGGER reject_mapping BEFORE INSERT ON generation_idempotency BEGIN SELECT RAISE(ABORT, 'synthetic mapping failure'); END");
    const options = dirty ? { includeSettings: true, dirtyRows: { jobs: ['job'], ledger: ['reserve'] } } : {};
    await assert.rejects(admission(store, options), /synthetic mapping failure/);
    assert.deepEqual(await store.read(), before);
    assert.equal(store.readGenerationRequest('parent'), null);
    assert.equal(store.readGenerationIdempotency('member-a', 'generate', 'a'.repeat(64)), null);
    assert.equal(store.nextQueueSequence(), 1);
    assert.equal(store.activeGenerationPayloadBytes(), 0);
    store.sqlite.exec('DROP TRIGGER reject_mapping');
    await admission(store, options);
    assert.equal(store.selectItemById('jobs', 'job').queueSeq, 1);
    assert.equal(store.selectItemById('ledger', 'reserve').jobId, 'job');
    assert.equal(store.readGenerationIdempotency('member-a', 'generate', 'a'.repeat(64)).job_id, 'job');
    assert.equal(store.pendingPersistScope, null);
  });
}

test('同成员/操作/键唯一；不同键可指向同一缓存任务，单独映射写入不依赖新父或脏记录', async t => {
  const store = await open(t);
  await admission(store);
  const append = record => store.update(() => null, { collections: [], generationIdempotency: record });
  await append(mapping({ key_hash: 'c'.repeat(64), fingerprint: 'd'.repeat(64) }));
  await append(mapping({ member_id: 'member-b' }));
  await append(mapping({ operation: 'encode-vibe' }));
  await append(mapping({ key_hash: 'e'.repeat(64), request_id: null, job_id: 'legacy-inline' }));
  await assert.rejects(append(mapping({ fingerprint: 'f'.repeat(64) })), /UNIQUE constraint failed/);
  assert.equal(store.readGenerationIdempotency('member-a', 'generate', 'a'.repeat(64)).fingerprint, 'b'.repeat(64));
  assert.equal(store.readGenerationIdempotency('member-a', 'generate', 'c'.repeat(64)).job_id, 'job');
  assert.equal(store.readGenerationIdempotency('member-a', 'generate', 'e'.repeat(64)).request_id, null);
  assert.equal(store.sqlite.prepare('SELECT COUNT(*) AS n FROM generation_idempotency').get().n, 5);
  assert.equal(store.pendingPersistScope, null);
  assert.equal(store.db.generation_idempotency, undefined);
  assert.equal(store.rowState.generation_idempotency, undefined);
  const plan = store.sqlite.prepare('EXPLAIN QUERY PLAN SELECT job_id FROM generation_idempotency WHERE member_id=? AND operation=? AND key_hash=?')
    .all('member-a', 'generate', 'a'.repeat(64));
  assert.ok(plan.some(row => /SEARCH.*sqlite_autoindex_generation_idempotency/.test(row.detail)));
});

test('并发同键在原串行 update 中重查后只写一份映射、父子意图和序号', async t => {
  const store = await open(t);
  const submit = () => {
    let inserted = false;
    return store.update(db => {
      const existing = store.readGenerationIdempotency('member-a', 'generate', 'a'.repeat(64));
      if (existing) return existing.job_id;
      inserted = true;
      db.jobs.push({ id: 'job', requestId: 'parent', status: 'queued', queueSeq: store.nextQueueSequence(), createdAt: new Date().toISOString() });
      return 'job';
    }, { collections: ['jobs'], dirtyRows: { jobs: ['job'] },
      generationRequest: () => inserted ? parent() : undefined,
      generationIdempotency: () => inserted ? mapping() : undefined });
  };
  assert.deepEqual(await Promise.all(Array.from({ length: 10 }, submit)), Array(10).fill('job'));
  assert.equal(store.countRecords('jobs'), 1);
  assert.equal(store.sqlite.prepare('SELECT COUNT(*) AS n FROM generation_requests').get().n, 1);
  assert.equal(store.sqlite.prepare('SELECT COUNT(*) AS n FROM generation_idempotency').get().n, 1);
  assert.equal(store.nextQueueSequence(), 2);
});

test('日志清理删首张但保留活动余图，映射仍保留原父 ID；全量替换和重启也不删去重证据', async t => {
  const store = await open(t);
  await admission(store);
  await store.update(db => {
    db.jobs[0].status = 'done';
    db.jobs.push({ id: 'next-child', requestId: 'parent', status: 'pending', sampleIndex: 1, createdAt: new Date().toISOString() });
  }, { immediate: true, dirtyRows: { jobs: ['job', 'next-child'] } });
  const original = store.readGenerationIdempotency('member-a', 'generate', 'a'.repeat(64));
  await store.clearRequestLogs();
  assert.equal(store.selectItemById('jobs', 'job'), null);
  assert.equal(store.selectItemById('jobs', 'next-child').status, 'pending');
  assert.deepEqual(store.readGenerationIdempotency('member-a', 'generate', 'a'.repeat(64)), original);
  assert.ok(store.readGenerationRequest('parent'));
  const snapshot = await store.read();
  snapshot.jobs = [];
  await store.write(snapshot);
  assert.equal(store.readGenerationRequest('parent'), null);
  assert.deepEqual(store.readGenerationIdempotency('member-a', 'generate', 'a'.repeat(64)), original);
  await store.close();
  const reopened = await open(t, store.dataDir);
  assert.deepEqual(reopened.readGenerationIdempotency('member-a', 'generate', 'a'.repeat(64)), original);
  assert.equal(reopened.countRecords('jobs'), 0);
});
