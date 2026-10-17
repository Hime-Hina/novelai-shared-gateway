import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile, mkdir, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';
import http from 'node:http';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import Database from 'better-sqlite3';
import { JsonStore } from '../../server/store.js';
import { describeStoredResult, publishStoredResult } from '../../server/result-files.js';
import { configureFairQuota, syncFairQuota, reserveFairQuota, settleFairQuota, resolveFairQuotaCharge } from '../../server/fair-quota.js';
import { closeGenerationReview, recordRecoveredGeneration, GENERATION_REVIEW_MS } from '../../server/generation-accounting.js';
import { activateNextBatchJob } from '../../server/generation-request.js';
import { recordSharedCompletion } from '../../server/shared-capacity.ts';

const source = await readFile(new URL('../../server/gateway-runtime.js', import.meta.url), 'utf8');
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
function fixture(now = Date.now(), { refund = true, fallback = false, id = 'review' } = {}) {
  const db = { settings: {}, users: ['a', 'b', 'c', 'd'].map(id => ({ id, token: `STA1N-${id}`, balance: 0, enabled: true })),
    accounts: [{ id: 'opus', enabled: true, quotaTier: 3, subscriptionActive: true, quotaFixed: 1000,
      quotaPurchased: 0, v5UsagePercent: 80, quotaCheckedAt: now, v5UsageTimeUntilNextPercent: 0, inFlight: 1 }],
    jobs: [{ id, userToken: 'STA1N-a', source: 'web', status: 'running', cost: 0,
      createdAt: new Date(now).toISOString(), accountId: 'opus',
      request: { model: 'nai-diffusion-5-full', width: 1024, height: 1024, steps: fallback ? 23 : 29 } }], ledger: [], images: [] };
  configureFairQuota(db, ['a', 'b', 'c', 'd']); syncFairQuota(db, now);
  if (fallback) { db.settings.fairQuota.autoAnlasFallback = true; db.settings.fairQuota.balances.a.v5Available = 0; }
  reserveFairQuota(db, db.users[0], db.jobs[0], db.accounts[0], now);
  if (refund) {
    db.jobs[0].upstreamStartedAt = new Date(now).toISOString();
    settleFairQuota(db, db.jobs[0], false, now + 1);
    db.jobs[0].status = 'failed'; db.jobs[0].error = 'synthetic interruption';
    db.jobs[0].completedAt = new Date(now + 1).toISOString();
    db.accounts[0].inFlight = 0;
  }
  return db;
}
const eventCount = (db, type) => db.ledger.filter(event => event.type === type).length;
async function open(t, seed) {
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-generation-review-'));
  const store = new JsonStore(dataDir); await store.init();
  t.after(() => store.close());
  await store.update(db => Object.assign(db, seed), { immediate: true });
  return store;
}
function loadFunctions(context, names) {
  const ctx = vm.createContext(context);
  for (const name of names) {
    const start = source.indexOf(`function ${name}(`), end = source.indexOf('\n}', start) + 2;
    vm.runInContext((source.slice(start - 6, start) === 'async ' ? 'async ' : '') + source.slice(start, end), ctx);
  }
  return ctx;
}

test('五分钟从首次未知固定计时，虚拟重启/重复关闭不退款、不叠加聚合缺口或运营核销', () => {
  const start = 1800000000000;
  let db = fixture(start, { fallback: true }), job = db.jobs[0];
  const deadline = start + 1 + GENERATION_REVIEW_MS;
  assert.equal(job.generationReview.deadlineAt, deadline);
  const balances = structuredClone(db.settings.fairQuota.balances), ops = structuredClone(db.settings.fairQuota.operating);
  assert.equal(closeGenerationReview(db, job, deadline - 1), false);
  db = JSON.parse(JSON.stringify(db)); job = db.jobs[0]; db.accounts[0].enabled = false;
  assert.equal(job.generationReview.firstUnknownAt, start + 1);
  assert.equal(closeGenerationReview(db, job, deadline), true);
  assert.equal(closeGenerationReview(db, job, deadline + 1000), false);
  const event = db.ledger.find(entry => entry.type === 'generation_loss_estimated');
  assert.equal(event.upstreamCandidates.length, 2); assert.equal(event.candidateMode, 'alternatives');
  assert.equal(event.actualConsumed, null); assert.equal(event.liability, 'operator_pending_attribution');
  assert.equal(event.aggregateSettlement, 'unmatched');
  assert.deepEqual(db.settings.fairQuota.balances, balances); assert.deepEqual(db.settings.fairQuota.operating, ops);
  assert.equal(eventCount(db, 'generation_refunded'), 1); assert.equal(eventCount(db, 'generation_loss_estimated'), 1);
});

test('无可信金额也结束核对；未派发与正常运行不因五分钟被失败结案', () => {
  const db = fixture(), job = db.jobs[0]; job.generationAttempt.upstreamCandidates = [];
  closeGenerationReview(db, job, job.generationReview.deadlineAt);
  const event = db.ledger.find(entry => entry.type === 'generation_loss_estimated');
  assert.equal(event.estimateAmountStatus, 'unknown'); assert.equal(event.upstreamCandidates, null); assert.equal(event.actualConsumed, null);
  const running = fixture(Date.now(), { refund: false });
  assert.equal(closeGenerationReview(running, running.jobs[0], Date.now() + 3600000), false);
  assert.equal(running.jobs[0].status, 'running');
  running.jobs[0].status = 'pending'; settleFairQuota(running, running.jobs[0], false);
  assert.equal(running.jobs[0].generationReview, undefined);
});

test('迟到证据追加关联调整，保留原结案、原退款及未知收费，不重复追加', () => {
  const db = fixture(), job = db.jobs[0]; closeGenerationReview(db, job, job.generationReview.deadlineAt);
  const closure = structuredClone(db.ledger[0]), balances = structuredClone(db.settings.fairQuota.balances);
  assert.equal(recordRecoveredGeneration(db, job, 'late-image'), true);
  assert.equal(recordRecoveredGeneration(db, job, 'late-image'), false);
  assert.equal(db.ledger[0].closureEventId, closure.id); assert.equal(db.ledger[0].refundRetained, true);
  assert.equal(db.ledger[0].actualConsumed, null);
  assert.deepEqual(db.ledger.find(event => event.id === closure.id), closure);
  assert.deepEqual(db.settings.fairQuota.balances, balances); assert.equal(job.status, 'failed');
  // 迟到的聚合余额下降仍不能证明这一次实际收费；只增加独立缺口，不把尝试估值再计一遍。
  const amount = job.fairCharge.fixed, at = job.generationReview.deadlineAt + 1;
  Object.assign(db.accounts[0], { quotaFixed: 1000 - amount, quotaCheckedAt: at }); syncFairQuota(db, at);
  assert.equal(db.settings.fairQuota.operating.unfunded.anlasFixed, amount);
  assert.equal(db.ledger.find(event => event.type === 'resource_gap_observed').actualTaskConsumption, null);
  assert.deepEqual(db.ledger.find(event => event.id === closure.id), closure);
  assert.deepEqual(db.settings.fairQuota.balances, balances);
});

test('升级沿用原首次未知，JSON 部分索引查冷历史，关闭写失败回滚并可重试', async t => {
  const seed = fixture(Date.now() - 400000), first = seed.jobs[0].generationReview.firstUnknownAt;
  delete seed.jobs[0].generationReview;
  const directory = await mkdtemp(path.join(tmpdir(), 'nai-generation-review-legacy-'));
  const old = new Database(path.join(directory, 'library.sqlite'));
  try {
    // 从冻结的真实旧 schema 建库；新增迁移不再要求测试猜测如何撤销最新结构。
    old.exec(await readFile(new URL('../fixtures/sqlite-before-drizzle.sql', import.meta.url), 'utf8'));
    old.prepare("INSERT INTO app_settings (key, value) VALUES ('settings', ?)").run(JSON.stringify(seed.settings));
    for (const collection of ['users', 'accounts', 'jobs', 'ledger']) {
      const insert = old.prepare(`INSERT INTO ${collection}
        (id, order_value, data, status, source, user_token, account_id, at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
      seed[collection].forEach((item, index) => insert.run(item.id,
        collection === 'jobs' ? -37.5 : seed[collection].length - index, JSON.stringify(item),
        item.status || null, item.source || null, item.userToken || null, item.accountId || null, item.at || null));
    }
  } finally { old.close(); }
  const reopened = new JsonStore(directory); t.after(() => reopened.close()); await reopened.init();
  reopened.trimRuntimeCollection('jobs', 0); reopened.trimRuntimeCollection('ledger', 0);
  const rows = await reopened.readDueGenerationReviews(Date.now());
  assert.equal(rows.length, 1); assert.equal(rows[0].generationReview.firstUnknownAt, first);
  assert.equal(rows[0].generationReview.deadlineAt, first + GENERATION_REVIEW_MS);
  const plan = reopened.sqlite.prepare(`EXPLAIN QUERY PLAN SELECT data FROM jobs
    WHERE json_extract(data, '$.generationReview.status') = 'pending'
      AND json_extract(data, '$.generationReview.deadlineAt') <= ?
    ORDER BY json_extract(data, '$.generationReview.deadlineAt') LIMIT 100`).all(Date.now());
  assert.ok(plan.some(row => row.detail.includes('idx_jobs_generation_review_due')));
  const review = loadFunctions({ store: reopened, closeGenerationReview, recordRecoveredGeneration,
    uniqueIds: values => [...new Set(values.filter(Boolean))] }, ['batchJobIds', 'dirtyJobMutationRows', 'reconcileGenerationReviews']);
  const originalOrder = reopened.sqlite.prepare("SELECT order_value FROM jobs WHERE id = 'review'").get().order_value;
  assert.equal(originalOrder, -37.5);
  reopened.sqlite.exec("CREATE TEMP TRIGGER reject_review BEFORE INSERT ON ledger WHEN json_extract(NEW.data, '$.type') = 'generation_loss_estimated' BEGIN SELECT RAISE(ABORT, 'synthetic review failure'); END;");
  await assert.rejects(review.reconcileGenerationReviews(), /synthetic review failure/);
  assert.equal((await reopened.readDueGenerationReviews(Date.now())).length, 1);
  reopened.sqlite.exec('DROP TRIGGER reject_review');
  reopened.trimRuntimeCollection('jobs', 0); reopened.trimRuntimeCollection('ledger', 0);
  await review.reconcileGenerationReviews();
  assert.equal((await reopened.readDueGenerationReviews(Date.now())).length, 0);
  assert.equal(reopened.sqlite.prepare("SELECT order_value FROM jobs WHERE id = 'review'").get().order_value, originalOrder);
  assert.equal(reopened.sqlite.pragma('integrity_check', { simple: true }), 'ok');
});

async function completionContext(store) {
  await mkdir(path.join(store.dataDir, 'images'), { recursive: true });
  let writes = 0, ids = 0;
  const update = store.update.bind(store);
  store.update = (fn, options = {}) => update(fn, { ...options, includeSettings: true, immediate: true });
  const ctx = loadFunctions({ store, dataDir: store.dataDir, fairShareMode: true, settleFairQuota, resolveFairQuotaCharge, recordRecoveredGeneration, recordSharedCompletion, activateNextBatchJob,
    describeStoredResult, publishStoredResult: async (...args) => { writes++; return publishStoredResult(...args); },
    createId: () => `image-${++ids}`, reservationAccountCost: () => 0, httpError: (statusCode, message) => Object.assign(new Error(message), { statusCode }),
    writeStoredImage: async (id, image) => { writes++; const file = `images/${id}.png`; await writeFile(path.join(store.dataDir, file), image.buffer); return file; },
    removeStoredImages: async () => {}, imageCacheTrimBuffer: () => 100,
    uniqueIds: values => [...new Set(values.filter(Boolean))], clearJobStreamProgress() {}, scheduleQueueDrain() {}, notifyJobWaiters() {}
  }, ['batchJobIds', 'completionJob', 'prepareGenerationResult', 'completeGeneration', 'recoverPendingGenerationResults']);
  return { ctx, writes: () => writes };
}
function reservation(db) { return { job: db.jobs[0], account: db.accounts[0], token: 'STA1N-a', cost: 0 }; }

test('真实完成路径：退款前成功结算；退款后保留失败与结果，重复迟到只写一次文件/调整且不释放别的在途槽', async t => {
  for (const refund of [false, true]) {
    const store = await open(t, fixture(Date.now(), { refund })), { ctx, writes } = await completionContext(store);
    // 迟到调用可能携带旧 reserved 快照；冷退款凭据仍是最终依据。
    if (refund) await store.update(db => { db.jobs[0].fairCharge.status = 'reserved'; }, { dirtyRows: { jobs: ['review'] } });
    store.db.accounts[0].inFlight = 1;
    store.db.accounts[0].ordinaryStrikeCount = 3;
    store.db.accounts[0].capacityStrikeCount = 2;
    const originalCooldown = new Date(Date.now() + 300000).toISOString();
    store.db.accounts[0].cooldownUntil = originalCooldown;
    const before = structuredClone(store.db.settings.fairQuota.balances), r = reservation(store.db);
    store.sqlite.prepare("UPDATE jobs SET order_value = -21.25 WHERE id = 'review'").run();
    const originalOrder = store.sqlite.prepare("SELECT order_value FROM jobs WHERE id = 'review'").get().order_value;
    store.trimRuntimeCollection('jobs', 0);
    const results = await Promise.all([1, 2].map(() => ctx.completeGeneration(r, r.job.request, { buffer: png, mimeType: 'image/png' }, { jobId: 'review' })));
    assert.equal(results[0].id, results[1].id); assert.equal(writes(), 1);
    assert.equal(store.sqlite.prepare("SELECT order_value FROM jobs WHERE id = 'review'").get().order_value, originalOrder);
    assert.equal((await readdir(path.join(store.dataDir, 'images'))).length, 1);
    assert.deepEqual(store.db.settings.fairQuota.balances, before);
    assert.equal(store.db.jobs[0].status, refund ? 'failed' : 'done');
    assert.equal(store.db.jobs[0].fairCharge.status, refund ? 'refunded' : 'settled');
    assert.equal(store.db.accounts[0].inFlight, refund ? 1 : 0);
    assert.equal(store.db.accounts[0].ordinaryStrikeCount, 0);
    assert.equal(store.db.accounts[0].capacityStrikeCount, 0);
    assert.equal(store.db.accounts[0].cooldownUntil, originalCooldown);
    await store.update(db => {
      recordSharedCompletion(db.accounts[0], Date.now(), { job: { id: 'later-task' }, kind: 'ordinary' });
    }, { dirtyRows: { accounts: ['opus'] } });
    await ctx.completeGeneration(r, r.job.request, { buffer: png, mimeType: 'image/png' }, { jobId: 'review' });
    assert.equal(store.db.accounts[0].ordinaryStrikeCount, 1, '旧成功重复不能重置后续失败');
    assert.equal(store.db.accounts[0].cooldownUntil, originalCooldown);
    assert.equal(eventCount(store.db, 'generation_result_recovered'), refund ? 1 : 0);
    assert.equal(eventCount(store.db, 'generation_refunded'), refund ? 1 : 0);
    await assert.rejects(ctx.completeGeneration({ ...r, token: 'STA1N-b' }, r.job.request, { buffer: png, mimeType: 'image/png' }, { jobId: 'review' }), /归属不符/);
    assert.equal(writes(), 1);
  }
});

test('迟到成功的计数清零和结果提交一起回滚，重试仅成功一次', async t => {
  const seed = fixture(); seed.accounts[0].ordinaryStrikeCount = 3; seed.accounts[0].capacityStrikeCount = 2;
  const store = await open(t, seed), { ctx } = await completionContext(store), r = reservation(store.db);
  store.sqlite.exec("CREATE TEMP TRIGGER reject_success BEFORE UPDATE ON accounts BEGIN SELECT RAISE(ABORT, 'synthetic success failure'); END");
  await assert.rejects(ctx.completeGeneration(r, r.job.request, { buffer: png, mimeType: 'image/png' }, { jobId: 'review' }), /synthetic success failure/);
  assert.equal(store.db.accounts[0].ordinaryStrikeCount, 3); assert.equal(store.db.accounts[0].capacityStrikeCount, 2);
  assert.equal(store.selectItemById('jobs', 'review').sharedCooldown, undefined);
  assert.equal(store.selectItemById('jobs', 'review').imageId, undefined);
  const pending = store.selectItemById('jobs', 'review').resultPending;
  assert.ok(pending?.sha256);
  assert.deepEqual(await readFile(path.join(store.dataDir, pending.file)), png);
  store.sqlite.exec('DROP TRIGGER reject_success');
  await ctx.completeGeneration(r, r.job.request, { buffer: png, mimeType: 'image/png' }, { jobId: 'review' });
  assert.equal(store.selectItemById('jobs', 'review').imageId, pending.imageId);
  assert.equal(store.selectItemById('jobs', 'review').resultPending, undefined);
  assert.equal((await readdir(path.join(store.dataDir, 'images'))).length, 1);
  assert.equal(store.db.accounts[0].ordinaryStrikeCount, 0); assert.equal(store.db.accounts[0].capacityStrikeCount, 0);
  assert.equal(store.selectItemById('jobs', 'review').sharedCooldown.kind, 'success');
  assert.equal(store.readFairQuotaSettlement('review').status, 'refunded');
});

test('产物描述提交失败不写文件；错误尝试和已占用结果ID不能保存或结算', async t => {
  const store = await open(t, fixture(Date.now(), { refund: false })), { ctx, writes } = await completionContext(store);
  const r = reservation(store.db), image = { buffer: png, mimeType: 'image/png' };
  store.sqlite.exec("CREATE TEMP TRIGGER reject_descriptor BEFORE UPDATE ON jobs WHEN json_type(NEW.data, '$.resultPending') = 'object' BEGIN SELECT RAISE(ABORT, 'synthetic descriptor failure'); END");
  await assert.rejects(ctx.completeGeneration(r, r.job.request, image, { jobId: 'review' }), /synthetic descriptor failure/);
  assert.equal(writes(), 0); assert.equal((await readdir(path.join(store.dataDir, 'images'))).length, 0);
  assert.equal(store.selectItemById('jobs', 'review').resultPending, undefined);
  assert.equal(store.db.jobs[0].fairCharge.status, 'reserved');
  store.sqlite.exec('DROP TRIGGER reject_descriptor');
  const wrongAttempt = { ...r, job: { ...r.job, generationAttempt: { ...r.job.generationAttempt, id: 'wrong-attempt' } } };
  await assert.rejects(ctx.completeGeneration(wrongAttempt, r.job.request, image, { jobId: 'review' }), /执行尝试不符/);
  await store.update(db => {
    db.jobs[0].resultPending = { ...describeStoredResult('occupied-result', 'generate', image), attemptId: db.jobs[0].generationAttempt.id };
    db.images.push({ id: 'occupied-result', token: 'STA1N-b' });
  }, { dirtyRows: { jobs: ['review'], images: ['occupied-result'] } });
  await assert.rejects(ctx.completeGeneration(r, r.job.request, image, { jobId: 'review' }), /ID 已被使用/);
  assert.equal(writes(), 0); assert.equal(store.selectItemById('images', 'occupied-result').token, 'STA1N-b');
  assert.equal(store.db.jobs[0].fairCharge.status, 'reserved');
});

test('启动核验缺失文件的待恢复冷任务不重新热载历史或丢弃证据', async t => {
  const store = await open(t, fixture()), { ctx } = await completionContext(store);
  const pending = { ...describeStoredResult('missing-result', 'generate', { buffer: png, mimeType: 'image/png' }),
    attemptId: store.db.jobs[0].generationAttempt.id };
  await store.update(db => { db.jobs[0].resultPending = pending; }, { dirtyRows: { jobs: ['review'] } });
  store.trimRuntimeCollection('jobs', 0);
  assert.equal(store.db.jobs.length, 0);
  await ctx.recoverPendingGenerationResults();
  assert.equal(store.db.jobs.length, 0);
  assert.deepEqual(store.selectItemById('jobs', 'review').resultPending, pending);
  assert.equal(store.readFairQuotaSettlement('review').status, 'refunded');
});

test('离线停用账户启动后自动关闭到期核对，真实 HTTP 可领取失败任务的迟到图片且不补扣', { timeout: 20000 }, async t => {
  const seed = fixture(Date.now() - 400000), store = await open(t, seed), { ctx } = await completionContext(store);
  const r = reservation(store.db), before = structuredClone(store.db.settings.fairQuota.balances);
  await ctx.completeGeneration(r, r.job.request, { buffer: png, mimeType: 'image/png' }, { jobId: 'review' });
  await store.update(db => {
    const offline = fixture(Date.now() - 400000, { id: 'offline-due' });
    db.jobs.push(offline.jobs[0]); db.ledger.unshift(...offline.ledger);
    db.settings.fairQuota.reservations['offline-due'] = offline.settings.fairQuota.reservations['offline-due'];
    db.accounts[0].enabled = false; db.accounts[0].inFlight = 0;
  }, { immediate: true });
  await store.close();
  const listener = http.createServer(); listener.listen(0, '127.0.0.1'); await once(listener, 'listening');
  const port = listener.address().port; await new Promise(resolve => listener.close(resolve));
  const env = Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE'].filter(key => process.env[key]).map(key => [key, process.env[key]]));
  Object.assign(env, { ADMIN_TOKEN: 'synthetic-admin-for-offline-review-12345', DATA_DIR: store.dataDir, HOST: '127.0.0.1', PORT: String(port),
    NOVELAI_API_URL: 'http://127.0.0.1:1', NOVELAI_ACCOUNT_API_URL: 'http://127.0.0.1:1', PUBLIC_HOST: 'gateway.example' });
  const child = spawn(process.execPath, ['server/index.js'], { cwd: fileURLToPath(new URL('../../', import.meta.url)), env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  t.after(async () => { if (child.exitCode === null) { child.kill(); await once(child, 'exit'); } });
  child.stderr.resume();
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('server startup timeout')), 10000);
    child.once('exit', code => { clearTimeout(timer); reject(new Error(`server exited ${code}`)); });
    child.stdout.on('data', chunk => { if (chunk.toString().includes('listening on')) { clearTimeout(timer); resolve(); } });
  });
  const base = `http://127.0.0.1:${port}`, headers = { host: 'gateway.example', authorization: 'Bearer STA1N-a' };
  const response = await fetch(`${base}/api/jobs/review`, { headers }); assert.equal(response.status, 200);
  const job = await response.json(); assert.equal(job.status, 'failed'); assert.equal(job.lateResultAvailable, true); assert.ok(job.imageUrl);
  const downloaded = await fetch(new URL(job.imageUrl, base), { headers }); assert.equal(downloaded.status, 200);
  assert.deepEqual(Buffer.from(await downloaded.arrayBuffer()), png);
  const reader = new Database(store.dbPath, { readonly: true }); t.after(() => reader.close());
  for (let i = 0; i < 50; i++) {
    const due = JSON.parse(reader.prepare("SELECT data FROM jobs WHERE id = 'offline-due'").get().data);
    if (due.generationReview.status === 'estimated_closed') break;
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  assert.equal(JSON.parse(reader.prepare("SELECT data FROM jobs WHERE id = 'offline-due'").get().data).generationReview.status, 'estimated_closed');
  const settings = JSON.parse(reader.prepare("SELECT value FROM app_settings WHERE key='settings'").get().value);
  assert.deepEqual(settings.fairQuota.balances, before);
});

test('已退款的批次首图迟到找回只保存结果，不激活或复活剩余图片', async t => {
  const seed = fixture();
  const store = await open(t, seed);
  const payload = JSON.stringify(seed.jobs[0].request);
  await store.update(db => {
    db.jobs[0].requestId = 'late-parent';
    db.jobs.push({ id: 'cancelled-child', requestId: 'late-parent', sampleIndex: 1, userToken: 'STA1N-a', status: 'failed',
      error: 'remaining batch cancelled', createdAt: new Date().toISOString() });
  }, { immediate: true, generationRequest: { id: 'late-parent', member_id: 'a', source: 'novelai-native', operation: 'generate',
    created_at: new Date().toISOString(), payload_bytes: Buffer.byteLength(payload), payload },
    dirtyRows: { jobs: ['review', 'cancelled-child'] } });
  const { ctx } = await completionContext(store);
  const r = reservation(store.db);
  await ctx.completeGeneration(r, r.job.request, { buffer: png, mimeType: 'image/png' }, { jobId: 'review' });
  assert.equal(store.selectItemById('jobs', 'review').status, 'failed');
  assert.ok(store.selectItemById('jobs', 'review').imageId);
  assert.equal(store.selectItemById('jobs', 'cancelled-child').status, 'failed');
  assert.equal(store.selectItemById('jobs', 'cancelled-child').queueSeq, undefined);
  assert.equal(store.queuedJobIds().length, 0);
});

test('父取消与实际完成/激活并发时，成功产物照常结算，剩余 child 无论顺序都取消', async t => {
  for (const cancelFirst of [true, false]) {
    const seed = fixture(Date.now(), { refund: false });
    const store = await open(t, seed), payload = JSON.stringify(seed.jobs[0].request);
    await store.update(db => {
      db.jobs[0].requestId = 'cancel-parent';
      db.jobs.push({ id: 'next-child', requestId: 'cancel-parent', sampleIndex: 1, userToken: 'STA1N-a', status: 'pending',
        request: seed.jobs[0].request, createdAt: new Date().toISOString(), queueExpiresAt: new Date(Date.now() + 600000).toISOString() });
    }, { immediate: true, generationRequest: { id: 'cancel-parent', member_id: 'a', source: 'novelai-native', operation: 'generate',
      created_at: new Date().toISOString(), payload_bytes: Buffer.byteLength(payload), payload }, dirtyRows: { jobs: ['review', 'next-child'] } });
    const { ctx } = await completionContext(store);
    ctx.publicErrorMessage = value => value;
    ctx.publicImageUrl = id => `/signed/${id}`;
    for (const name of ['publicGenerationRequest', 'getGenerationRequest', 'cancelGenerationRequest', 'refundJob']) {
      const start = source.indexOf(`function ${name}(`), end = source.indexOf('\n}', start) + 2;
      vm.runInContext((source.slice(start - 6, start) === 'async ' ? 'async ' : '') + source.slice(start, end), ctx);
    }
    const r = reservation(store.db);
    const complete = () => ctx.completeGeneration(r, r.job.request, { buffer: png, mimeType: 'image/png' }, { jobId: 'review' });
    const cancel = () => ctx.cancelGenerationRequest('cancel-parent', 'a');
    await Promise.all(cancelFirst ? [cancel(), complete()] : [complete(), cancel()]);
    assert.equal(store.selectItemById('jobs', 'review').status, 'done');
    assert.equal(store.selectItemById('jobs', 'review').fairCharge.status, 'settled');
    assert.equal(store.selectItemById('jobs', 'next-child').status, 'failed');
    assert.equal(store.selectItemById('jobs', 'next-child').cancellationReason, 'request_cancelled');
    assert.equal(store.db.ledger.filter(event => event.type === 'generation_refunded').length, 0);
    assert.equal((await ctx.getGenerationRequest('cancel-parent', 'a')).status, 'partial');
  }
});
