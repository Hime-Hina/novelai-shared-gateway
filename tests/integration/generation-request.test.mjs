import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { JsonStore } from '../../server/store.js';
import { createGenerationPayloadBudget, generationPayloadLimits, webJobRequestSummary } from '../../server/generation-request.js';

async function fixture(t, dataDir) {
  const store = new JsonStore(dataDir || await mkdtemp(path.join(tmpdir(), 'nai-durable-request-')));
  await store.init();
  t.after(() => store.close());
  return store;
}

function record(id = 'request-a') {
  const request = { model: 'nai-diffusion-4-5-full', width: 1024, height: 1024, steps: 28,
    tag: '测试', prompt: 'artist\n测试', negative: 'synthetic private parameters', seed: 123 };
  const payload = JSON.stringify(request);
  return { id, member_id: 'member-a', source: 'web', operation: 'generate',
    created_at: new Date().toISOString(), payload_bytes: Buffer.byteLength(payload), payload };
}

async function admit(store, parent, status = 'queued', claim) {
  return store.update(db => {
    claim?.resize(parent.payload_bytes);
    db.jobs.unshift({ id: `job-${parent.id}`, requestId: parent.id, source: 'web',
      status, createdAt: parent.created_at, request: webJobRequestSummary(JSON.parse(parent.payload)) });
    db.ledger.unshift({ id: `reserve-${parent.id}`, type: 'reserve', amount: 0 });
    return { id: `job-${parent.id}` };
  }, { immediate: true, generationRequest: parent, generationPayloadClaim: claim,
    dirtyRows: { jobs: [`job-${parent.id}`], ledger: [`reserve-${parent.id}`] } });
}

test('父参数冷存、重启按活动任务求字节数，终态释放但不删除历史', async t => {
  const store = await fixture(t), parent = record();
  assert.deepEqual(await admit(store, parent), { id: 'job-request-a' });
  assert.equal(store.db.jobs[0].request.tag, undefined);
  assert.equal(store.db.jobs[0].request.prompt, undefined);
  assert.equal(store.db.jobs[0].request.negative, undefined);
  assert.equal(store.db.generation_requests, undefined);
  assert.equal(store.rowState.generation_requests, undefined);
  assert.deepEqual(store.readGenerationRequest(parent.id), JSON.parse(parent.payload));
  assert.equal(store.activeGenerationPayloadBytes(), parent.payload_bytes);
  await store.close();
  const reopened = await fixture(t, store.dataDir);
  assert.equal(reopened.activeGenerationPayloadBytes(), parent.payload_bytes);
  assert.equal(reopened.readGenerationRequest(parent.id).seed, 123);
  await reopened.update(db => { db.jobs[0].status = 'running'; }, { immediate: true, dirtyRows: { jobs: ['job-request-a'] } });
  assert.equal(reopened.activeGenerationPayloadBytes(), parent.payload_bytes);
  await reopened.update(db => { db.jobs[0].status = 'done'; }, { immediate: true, dirtyRows: { jobs: ['job-request-a'] } });
  assert.equal(reopened.activeGenerationPayloadBytes(), 0);
  assert.equal(reopened.readGenerationRequest(parent.id).negative, 'synthetic private parameters');
});

test('两个并发受理正好达到总字节预算，提交后先移交 claim 再开始下一笔', async t => {
  const store = await fixture(t), first = record('first'), second = record('other');
  const maximum = first.payload_bytes + second.payload_bytes;
  const budget = createGenerationPayloadBudget(() => store.activeGenerationPayloadBytes(), { request: maximum, active: maximum });
  const firstClaim = budget.claim(), secondClaim = budget.claim();
  firstClaim.resize(first.payload_bytes);
  secondClaim.resize(second.payload_bytes);
  await Promise.all([admit(store, first, 'queued', firstClaim), admit(store, second, 'queued', secondClaim)]);
  assert.equal(store.db.jobs.length, 2);
  assert.equal(store.activeGenerationPayloadBytes(), maximum);
  assert.throws(() => budget.claim().resize(1), error => error.statusCode === 429);
  // HTTP finally 可能更晚执行；重复释放不能扣掉其他请求的占用。
  firstClaim.release();
  secondClaim.release();
  assert.throws(() => budget.claim().resize(1), error => error.statusCode === 429);
});

test('父写入与 job/账本写入原子回滚，失败不会占用持久字节预算', async t => {
  const store = await fixture(t), parent = record();
  store.sqlite.exec("CREATE TEMP TRIGGER reject_job BEFORE INSERT ON jobs BEGIN SELECT RAISE(ABORT, 'synthetic job failure'); END");
  await assert.rejects(admit(store, parent), /synthetic job failure/);
  assert.equal(store.readGenerationRequest(parent.id), null);
  assert.equal(store.db.jobs.length, 0);
  assert.equal(store.db.ledger.length, 0);
  assert.equal(store.activeGenerationPayloadBytes(), 0);
  store.sqlite.exec('DROP TRIGGER reject_job');
  await admit(store, parent);
  await assert.rejects(admit(store, parent), /UNIQUE constraint failed/);
  assert.equal(store.db.jobs.length, 1);
  assert.equal(store.db.ledger.length, 1);
  assert.equal(store.activeGenerationPayloadBytes(), parent.payload_bytes);
});

test('全量替换保留仍被引用的父记录，并与新 jobs 一起回滚或移除孤立父记录', async t => {
  const store = await fixture(t), parent = record();
  await admit(store, parent);
  const snapshot = await store.read();
  await store.write(snapshot);
  assert.equal(store.readGenerationRequest(parent.id).seed, 123);
  const invalid = structuredClone(snapshot);
  invalid.jobs[0].requestId = 'missing-parent';
  await assert.rejects(store.write(invalid), /FOREIGN KEY constraint failed/);
  assert.equal(store.readGenerationRequest(parent.id).seed, 123);
  assert.equal(store.selectItemById('jobs', 'job-request-a').requestId, parent.id);
  snapshot.jobs = [];
  await store.write(snapshot);
  assert.equal(store.readGenerationRequest(parent.id), null);
});

test('容量 SQL 从小活动索引出发查父主键，不扫描全部历史 payload', async t => {
  const store = await fixture(t);
  await admit(store, record('active'));
  await admit(store, record('history'), 'done');
  // 历史 payload 即使不能解码，也不应被启动或容量统计触碰。
  store.sqlite.prepare('UPDATE generation_requests SET payload = ? WHERE id = ?').run('not-json', 'history');
  await store.close();
  const reopened = await fixture(t, store.dataDir);
  assert.equal(reopened.activeGenerationPayloadBytes(), record('active').payload_bytes);
  const plan = reopened.sqlite.prepare(`EXPLAIN QUERY PLAN
    SELECT COALESCE(SUM(payload_bytes), 0) FROM generation_requests
    WHERE id IN (SELECT request_id FROM jobs INDEXED BY idx_jobs_active_created
      WHERE status IN ('queued', 'pending', 'running') AND request_id IS NOT NULL)`).all();
  assert.ok(plan.some(row => /SEARCH generation_requests USING INDEX sqlite_autoindex_generation_requests_1/.test(row.detail)));
  assert.ok(plan.some(row => row.detail.includes('idx_jobs_active_created')));
  assert.ok(!plan.some(row => /SCAN generation_requests/.test(row.detail)));
});

test('接近网页正文上限的大 tag 只在父记录保留，热子对象与 SQL 影子列不复制正文', async t => {
  const store = await fixture(t), parent = record();
  const request = JSON.parse(parent.payload);
  request.tag = 'x'.repeat(2 * 1024 * 1024 - 4096);
  request.prompt = 'artist\n' + request.tag;
  parent.payload = JSON.stringify(request);
  parent.payload_bytes = Buffer.byteLength(parent.payload);
  await admit(store, parent);
  const row = store.sqlite.prepare('SELECT data, prompt, full_prompt FROM jobs').get();
  assert.ok(Buffer.byteLength(row.data) < 1024);
  assert.equal(row.prompt, null);
  assert.equal(row.full_prompt, null);
  assert.equal(store.db.jobs[0].request.tag, undefined);
  assert.equal(store.readGenerationRequest(parent.id).prompt, request.prompt);
});

test('正文临时占用按 UTF-8 字节计量，超额、解析失败与断线释放后可继续受理', () => {
  let activeBytes = 0;
  const budget = createGenerationPayloadBudget(() => activeBytes, { request: 8, active: 12 });
  const first = budget.claim(), second = budget.claim();
  first.resize(Buffer.byteLength('中文')); // 六字节而不是两个字符。
  second.resize(6);
  assert.throws(() => budget.claim().resize(1), error => error.statusCode === 429);
  assert.throws(() => first.resize(9), error => error.statusCode === 413);
  first.release();
  first.release();
  const retry = budget.claim();
  retry.resize(6);
  // 持久受理后释放临时 claim，后续请求仍受活动 SQL 总量约束。
  activeBytes = 6;
  retry.release();
  assert.throws(() => budget.claim().resize(1), error => error.statusCode === 429);
  second.release();
  const afterFailure = budget.claim();
  afterFailure.resize(6);
  afterFailure.release();
  assert.deepEqual(generationPayloadLimits({}), { request: 32 * 1024 ** 2, active: 64 * 1024 ** 2 });
});

test('inline 请求 claim 与活动行同步交接，延迟写模式也不放出尚未落盘的预算', async t => {
  const store = await fixture(t);
  const payload = { prompt: 'x'.repeat(100) }, bytes = Buffer.byteLength(JSON.stringify(payload));
  const budget = createGenerationPayloadBudget(() => store.activeGenerationPayloadBytes(), { request: bytes, active: bytes });
  const claim = budget.claim();
  claim.resize(bytes);
  await store.update(db => {
    db.jobs.push({ id: 'inline', status: 'queued', request: payload, payloadBytes: bytes });
  }, { dirtyRows: { jobs: ['inline'] }, generationPayloadClaim: claim });
  assert.equal(store.activeGenerationPayloadBytes(), bytes);
  assert.throws(() => budget.claim().resize(1), error => error.statusCode === 429);
  claim.release();
  await store.close();
  const reopened = await fixture(t, store.dataDir);
  assert.equal(reopened.activeGenerationPayloadBytes(), bytes);
});
