import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, mkdir, readdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { once } from 'node:events';
import Database from 'better-sqlite3';
import { JsonStore } from '../../server/storage/sqlite-store.ts';
import {
  describeStoredResult,
  publishStoredResult,
  readStoredResultContent,
} from '../../server/storage/result-files.ts';
import {
  configureFairQuota,
  syncFairQuota,
  reserveFairQuota,
  settleFairQuota,
} from '../../server/quota/fair-share.ts';
import {
  closeGenerationReview,
  recordRecoveredGeneration,
  generationChargeResources,
  generationResponseEvidence,
  GENERATION_REVIEW_MS,
} from '../../server/generation/accounting.ts';
import { recordSharedCompletion } from '../../server/quota/shared-capacity.ts';
import {
  completeGenerationCommand,
  recoverPendingGenerationResultsCommand,
  reconcileGenerationReviewsCommand,
} from '../../server/generation/execution.ts';
import { createRequestControls } from '../../server/generation/delivery.ts';
import {
  publicGenerationRequest,
  readOwnedGenerationRequest,
} from '../../server/generation/request-queries.ts';
import { recordDomainEvent, domainDigest } from '../../server/events/domain-events.ts';
import {
  childFixture,
  launchGateway,
  query,
  assertChildPublicReplay,
} from '../helpers/relational-child.mjs';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalAccountStore } from '../../server/storage/relational-account-store.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import {
  restoreRelationalPrivateSnapshot,
  relationalSnapshotSha256,
} from '../../server/storage/relational-private-restore.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { parseAmount, formatAmount } from '../../server/quota/resource-amounts.ts';

const png = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=',
  'base64',
);
function fixture(now = Date.now(), { refund = true, fallback = false, id = 'review' } = {}) {
  const db = {
    settings: {},
    users: ['a', 'b', 'c', 'd'].map((id) => ({
      id,
      token: `STA1N-${id}`,
      balance: 0,
      enabled: true,
    })),
    accounts: [
      {
        id: 'opus',
        enabled: true,
        quotaTier: 3,
        subscriptionActive: true,
        quotaFixed: 1000,
        quotaPurchased: 0,
        v5UsagePercent: 80,
        quotaCheckedAt: now,
        v5UsageTimeUntilNextPercent: 0,
        inFlight: 1,
      },
    ],
    jobs: [
      {
        id,
        userToken: 'STA1N-a',
        source: 'web',
        status: 'running',
        cost: 0,
        createdAt: new Date(now).toISOString(),
        accountId: 'opus',
        request: {
          model: 'nai-diffusion-5-full',
          width: 1024,
          height: 1024,
          steps: fallback ? 23 : 29,
        },
      },
    ],
    ledger: [],
    images: [],
  };
  configureFairQuota(db, ['a', 'b', 'c', 'd']);
  syncFairQuota(db, now);
  if (fallback) {
    db.settings.fairQuota.autoAnlasFallback = true;
    db.settings.fairQuota.balances.a.v5Available = 0;
  }
  reserveFairQuota(db, db.users[0], db.jobs[0], db.accounts[0], now);
  if (refund) {
    db.jobs[0].upstreamStartedAt = new Date(now).toISOString();
    settleFairQuota(db, db.jobs[0], false, now + 1);
    db.jobs[0].status = 'failed';
    db.jobs[0].error = 'synthetic interruption';
    db.jobs[0].completedAt = new Date(now + 1).toISOString();
    db.accounts[0].inFlight = 0;
  }
  return db;
}
const eventCount = (db, type) => db.ledger.filter((event) => event.type === type).length;
async function open(t, seed, requestId) {
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-generation-review-'));
  const store = new JsonStore(dataDir);
  await store.init();
  t.after(() => store.close());
  if (requestId) {
    const payload = JSON.stringify(seed.jobs[0].request);
    store.sqlite
      .prepare(
        'INSERT INTO generation_requests(id,member_id,source,operation,created_at,payload_bytes,payload) VALUES(?,?,?,?,?,?,?)',
      )
      .run(
        requestId,
        'a',
        'novelai-native',
        'generate',
        seed.jobs[0].createdAt,
        Buffer.byteLength(payload),
        payload,
      );
  }
  await store.write(seed);
  return store;
}

test('五分钟从首次未知固定计时，虚拟重启/重复关闭不退款、不叠加聚合缺口或运营核销', () => {
  const start = 1800000000000;
  let db = fixture(start, { fallback: true }),
    job = db.jobs[0];
  const deadline = start + 1 + GENERATION_REVIEW_MS;
  assert.equal(job.generationReview.deadlineAt, deadline);
  const balances = structuredClone(db.settings.fairQuota.balances),
    ops = structuredClone(db.settings.fairQuota.operating);
  assert.equal(closeGenerationReview(db, job, deadline - 1), false);
  db = JSON.parse(JSON.stringify(db));
  job = db.jobs[0];
  db.accounts[0].enabled = false;
  assert.equal(job.generationReview.firstUnknownAt, start + 1);
  assert.equal(closeGenerationReview(db, job, deadline), true);
  assert.equal(closeGenerationReview(db, job, deadline + 1000), false);
  const event = db.ledger.find((entry) => entry.type === 'generation_loss_estimated');
  assert.equal(event.upstreamCandidates.length, 2);
  assert.equal(event.candidateMode, 'alternatives');
  assert.equal(event.actualConsumed, null);
  assert.equal(event.liability, 'operator_pending_attribution');
  assert.equal(event.aggregateSettlement, 'unmatched');
  assert.deepEqual(db.settings.fairQuota.balances, balances);
  assert.deepEqual(db.settings.fairQuota.operating, ops);
  assert.equal(eventCount(db, 'generation_refunded'), 1);
  assert.equal(eventCount(db, 'generation_loss_estimated'), 1);
});

test('无可信金额也结束核对；未派发与正常运行不因五分钟被失败结案', () => {
  const db = fixture(),
    job = db.jobs[0];
  job.generationAttempt.upstreamCandidates = [];
  closeGenerationReview(db, job, job.generationReview.deadlineAt);
  const event = db.ledger.find((entry) => entry.type === 'generation_loss_estimated');
  assert.equal(event.estimateAmountStatus, 'unknown');
  assert.equal(event.upstreamCandidates, null);
  assert.equal(event.actualConsumed, null);
  const running = fixture(Date.now(), { refund: false });
  assert.equal(closeGenerationReview(running, running.jobs[0], Date.now() + 3600000), false);
  assert.equal(running.jobs[0].status, 'running');
  running.jobs[0].status = 'pending';
  settleFairQuota(running, running.jobs[0], false);
  assert.equal(running.jobs[0].generationReview, undefined);
});

test('迟到证据追加关联调整，保留原结案、原退款及未知收费，不重复追加', () => {
  const db = fixture(),
    job = db.jobs[0];
  closeGenerationReview(db, job, job.generationReview.deadlineAt);
  const closure = structuredClone(db.ledger[0]),
    balances = structuredClone(db.settings.fairQuota.balances);
  assert.equal(recordRecoveredGeneration(db, job, 'late-image'), true);
  assert.equal(recordRecoveredGeneration(db, job, 'late-image'), false);
  assert.equal(db.ledger[0].closureEventId, closure.id);
  assert.equal(db.ledger[0].refundRetained, true);
  assert.equal(db.ledger[0].actualConsumed, null);
  assert.deepEqual(
    db.ledger.find((event) => event.id === closure.id),
    closure,
  );
  assert.deepEqual(db.settings.fairQuota.balances, balances);
  assert.equal(job.status, 'failed');
  // 迟到的聚合余额下降仍不能证明这一次实际收费；只增加独立缺口，不把尝试估值再计一遍。
  const amount = job.fairCharge.fixed,
    at = job.generationReview.deadlineAt + 1;
  Object.assign(db.accounts[0], { quotaFixed: 1000 - amount, quotaCheckedAt: at });
  syncFairQuota(db, at);
  assert.equal(db.settings.fairQuota.operating.unfunded.anlasFixed, amount);
  assert.equal(
    db.ledger.find((event) => event.type === 'resource_gap_observed').actualTaskConsumption,
    null,
  );
  assert.deepEqual(
    db.ledger.find((event) => event.id === closure.id),
    closure,
  );
  assert.deepEqual(db.settings.fairQuota.balances, balances);
});

test('升级沿用原首次未知，JSON 部分索引查冷历史，关闭写失败回滚并可重试', async (t) => {
  const seed = fixture(Date.now() - 400000),
    first = seed.jobs[0].generationReview.firstUnknownAt;
  delete seed.jobs[0].generationReview;
  const directory = await mkdtemp(path.join(tmpdir(), 'nai-generation-review-legacy-'));
  const old = new Database(path.join(directory, 'library.sqlite'));
  try {
    // 从冻结的真实旧 schema 建库；新增迁移不再要求测试猜测如何撤销最新结构。
    old.exec(
      await readFile(new URL('../fixtures/sqlite-before-drizzle.sql', import.meta.url), 'utf8'),
    );
    old
      .prepare("INSERT INTO app_settings (key, value) VALUES ('settings', ?)")
      .run(JSON.stringify(seed.settings));
    for (const collection of ['users', 'accounts', 'jobs', 'ledger']) {
      const insert = old.prepare(`INSERT INTO ${collection}
        (id, order_value, data, status, source, user_token, account_id, at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
      seed[collection].forEach((item, index) =>
        insert.run(
          item.id,
          collection === 'jobs' ? -37.5 : seed[collection].length - index,
          JSON.stringify(item),
          item.status || null,
          item.source || null,
          item.userToken || null,
          item.accountId || null,
          item.at || null,
        ),
      );
    }
  } finally {
    old.close();
  }
  const reopened = new JsonStore(directory);
  t.after(() => reopened.close());
  await reopened.init();
  reopened.trimRuntimeCollection('jobs', 0);
  reopened.trimRuntimeCollection('ledger', 0);
  const rows = await reopened.readDueGenerationReviews(Date.now());
  assert.equal(rows.length, 1);
  assert.equal(rows[0].generationReview.firstUnknownAt, first);
  assert.equal(rows[0].generationReview.deadlineAt, first + GENERATION_REVIEW_MS);
  const plan = reopened.sqlite
    .prepare(
      `EXPLAIN QUERY PLAN SELECT data FROM jobs
    WHERE json_extract(data, '$.generationReview.status') = 'pending'
      AND json_extract(data, '$.generationReview.deadlineAt') <= ?
    ORDER BY json_extract(data, '$.generationReview.deadlineAt') LIMIT 100`,
    )
    .all(Date.now());
  assert.ok(plan.some((row) => row.detail.includes('idx_jobs_generation_review_due')));
  const reconcile = () =>
    reconcileGenerationReviewsCommand(reopened, {
      readStoredImage: (image) => readFile(path.join(reopened.dataDir, image.file)),
    });
  const originalOrder = reopened.sqlite
    .prepare("SELECT order_value FROM jobs WHERE id = 'review'")
    .get().order_value;
  assert.equal(originalOrder, -37.5);
  reopened.sqlite.exec(
    "CREATE TEMP TRIGGER reject_review BEFORE INSERT ON ledger WHEN json_extract(NEW.data, '$.type') = 'generation_loss_estimated' BEGIN SELECT RAISE(ABORT, 'synthetic review failure'); END;",
  );
  await assert.rejects(reconcile(), /synthetic review failure/);
  assert.equal((await reopened.readDueGenerationReviews(Date.now())).length, 1);
  reopened.sqlite.exec('DROP TRIGGER reject_review');
  reopened.trimRuntimeCollection('jobs', 0);
  reopened.trimRuntimeCollection('ledger', 0);
  await reconcile();
  assert.equal((await reopened.readDueGenerationReviews(Date.now())).length, 0);
  assert.equal(
    reopened.sqlite.prepare("SELECT order_value FROM jobs WHERE id = 'review'").get().order_value,
    originalOrder,
  );
  assert.equal(reopened.sqlite.pragma('integrity_check', { simple: true }), 'ok');
});

async function completionContext(store) {
  await mkdir(path.join(store.dataDir, 'images'), { recursive: true });
  let writes = 0,
    ids = 0;
  const update = store.update.bind(store);
  store.update = (fn, options = {}) =>
    update(fn, { ...options, includeSettings: true, immediate: true });
  const options = {
    store,
    dataDir: store.dataDir,
    createId: () => `image-${++ids}`,
    publish: async (...args) => {
      writes++;
      return publishStoredResult(...args);
    },
  };
  const getGenerationRequest = async (requestId, memberId) =>
    publicGenerationRequest(await readOwnedGenerationRequest(store, requestId, memberId), {
      imageUrl: (id) => `/signed/${id}`,
      errorMessage: (value) => value,
    });
  const ctx = {
    getGenerationRequest,
    ...createRequestControls(store, {
      clearProgress() {},
      notify() {},
      schedule() {},
      readRequest: getGenerationRequest,
    }),
    completeGeneration: (...args) => completeGenerationCommand(options, ...args),
    recoverPendingGenerationResults: () => recoverPendingGenerationResultsCommand(options),
  };
  return { ctx, writes: () => writes };
}
function reservation(db) {
  return { job: db.jobs[0], account: db.accounts[0], token: 'STA1N-a', cost: 0 };
}

test('真实完成路径：退款前成功结算；退款后保留失败与结果，重复迟到只写一次文件/调整且不释放别的在途槽', async (t) => {
  for (const refund of [false, true]) {
    const store = await open(t, fixture(Date.now(), { refund })),
      { ctx, writes } = await completionContext(store);
    // 迟到调用可能携带旧 reserved 快照；冷退款凭据仍是最终依据。
    if (refund) {
      await store.update(
        (db) => {
          db.jobs[0].fairCharge.status = 'reserved';
        },
        { dirtyRows: { jobs: ['review'] } },
      );
    }
    store.db.accounts[0].inFlight = 1;
    store.db.accounts[0].ordinaryStrikeCount = 3;
    store.db.accounts[0].capacityStrikeCount = 2;
    const originalCooldown = new Date(Date.now() + 300000).toISOString();
    store.db.accounts[0].cooldownUntil = originalCooldown;
    const before = structuredClone(store.db.settings.fairQuota.balances),
      r = reservation(store.db);
    store.sqlite.prepare("UPDATE jobs SET order_value = -21.25 WHERE id = 'review'").run();
    const originalOrder = store.sqlite
      .prepare("SELECT order_value FROM jobs WHERE id = 'review'")
      .get().order_value;
    store.trimRuntimeCollection('jobs', 0);
    const results = await Promise.all(
      [1, 2].map(() =>
        ctx.completeGeneration(
          r,
          r.job.request,
          { buffer: png, mimeType: 'image/png' },
          { jobId: 'review' },
        ),
      ),
    );
    assert.equal(results[0].id, results[1].id);
    assert.equal(writes(), 1);
    assert.equal(
      store.sqlite.prepare("SELECT order_value FROM jobs WHERE id = 'review'").get().order_value,
      originalOrder,
    );
    assert.equal((await readdir(path.join(store.dataDir, 'images'))).length, 1);
    assert.deepEqual(store.db.settings.fairQuota.balances, before);
    assert.equal(store.db.jobs[0].status, refund ? 'failed' : 'done');
    assert.equal(store.db.jobs[0].fairCharge.status, refund ? 'refunded' : 'settled');
    assert.equal(store.db.accounts[0].inFlight, refund ? 1 : 0);
    assert.equal(store.db.accounts[0].ordinaryStrikeCount, 0);
    assert.equal(store.db.accounts[0].capacityStrikeCount, 0);
    assert.equal(store.db.accounts[0].cooldownUntil, originalCooldown);
    await store.update(
      (db) => {
        recordSharedCompletion(db.accounts[0], Date.now(), {
          job: { id: 'later-task' },
          kind: 'ordinary',
        });
      },
      { dirtyRows: { accounts: ['opus'] } },
    );
    await ctx.completeGeneration(
      r,
      r.job.request,
      { buffer: png, mimeType: 'image/png' },
      { jobId: 'review' },
    );
    assert.equal(store.db.accounts[0].ordinaryStrikeCount, 1, '旧成功重复不能重置后续失败');
    assert.equal(store.db.accounts[0].cooldownUntil, originalCooldown);
    assert.equal(eventCount(store.db, 'generation_result_recovered'), refund ? 1 : 0);
    assert.equal(eventCount(store.db, 'generation_refunded'), refund ? 1 : 0);
    await assert.rejects(
      ctx.completeGeneration(
        { ...r, token: 'STA1N-b' },
        r.job.request,
        { buffer: png, mimeType: 'image/png' },
        { jobId: 'review' },
      ),
      /归属不符/,
    );
    assert.equal(writes(), 1);
  }
});

test('迟到成功的计数清零和结果提交一起回滚，重试仅成功一次', async (t) => {
  const seed = fixture();
  seed.accounts[0].ordinaryStrikeCount = 3;
  seed.accounts[0].capacityStrikeCount = 2;
  const store = await open(t, seed),
    { ctx } = await completionContext(store),
    r = reservation(store.db);
  store.sqlite.exec(
    "CREATE TEMP TRIGGER reject_success BEFORE UPDATE ON accounts BEGIN SELECT RAISE(ABORT, 'synthetic success failure'); END",
  );
  await assert.rejects(
    ctx.completeGeneration(
      r,
      r.job.request,
      { buffer: png, mimeType: 'image/png' },
      { jobId: 'review' },
    ),
    /synthetic success failure/,
  );
  assert.equal(store.db.accounts[0].ordinaryStrikeCount, 3);
  assert.equal(store.db.accounts[0].capacityStrikeCount, 2);
  assert.equal(store.selectItemById('jobs', 'review').sharedCooldown, undefined);
  assert.equal(store.selectItemById('jobs', 'review').imageId, undefined);
  const pending = store.selectItemById('jobs', 'review').resultPending;
  assert.ok(pending?.sha256);
  assert.deepEqual(await readFile(path.join(store.dataDir, pending.file)), png);
  store.sqlite.exec('DROP TRIGGER reject_success');
  await ctx.completeGeneration(
    r,
    r.job.request,
    { buffer: png, mimeType: 'image/png' },
    { jobId: 'review' },
  );
  assert.equal(store.selectItemById('jobs', 'review').imageId, pending.imageId);
  assert.equal(store.selectItemById('jobs', 'review').resultPending, undefined);
  assert.equal((await readdir(path.join(store.dataDir, 'images'))).length, 1);
  assert.equal(store.db.accounts[0].ordinaryStrikeCount, 0);
  assert.equal(store.db.accounts[0].capacityStrikeCount, 0);
  assert.equal(store.selectItemById('jobs', 'review').sharedCooldown.kind, 'success');
  assert.equal(store.readFairQuotaSettlement('review').status, 'refunded');
});

test('产物描述提交失败不写文件；错误尝试和已占用结果ID不能保存或结算', async (t) => {
  const store = await open(t, fixture(Date.now(), { refund: false })),
    { ctx, writes } = await completionContext(store);
  const r = reservation(store.db),
    image = { buffer: png, mimeType: 'image/png' };
  store.sqlite.exec(
    "CREATE TEMP TRIGGER reject_descriptor BEFORE UPDATE ON jobs WHEN json_type(NEW.data, '$.resultPending') = 'object' BEGIN SELECT RAISE(ABORT, 'synthetic descriptor failure'); END",
  );
  await assert.rejects(
    ctx.completeGeneration(r, r.job.request, image, { jobId: 'review' }),
    /synthetic descriptor failure/,
  );
  assert.equal(writes(), 0);
  assert.equal((await readdir(path.join(store.dataDir, 'images'))).length, 0);
  assert.equal(store.selectItemById('jobs', 'review').resultPending, undefined);
  assert.equal(store.db.jobs[0].fairCharge.status, 'reserved');
  store.sqlite.exec('DROP TRIGGER reject_descriptor');
  const wrongAttempt = {
    ...r,
    job: { ...r.job, generationAttempt: { ...r.job.generationAttempt, id: 'wrong-attempt' } },
  };
  await assert.rejects(
    ctx.completeGeneration(wrongAttempt, r.job.request, image, { jobId: 'review' }),
    /执行尝试不符/,
  );
  await store.update(
    (db) => {
      db.jobs[0].resultPending = {
        ...describeStoredResult('occupied-result', 'generate', image),
        attemptId: db.jobs[0].generationAttempt.id,
      };
      db.images.push({ id: 'occupied-result', token: 'STA1N-b' });
    },
    { dirtyRows: { jobs: ['review'], images: ['occupied-result'] } },
  );
  await assert.rejects(
    ctx.completeGeneration(r, r.job.request, image, { jobId: 'review' }),
    /ID 已被使用/,
  );
  assert.equal(writes(), 0);
  assert.equal(store.selectItemById('images', 'occupied-result').token, 'STA1N-b');
  assert.equal(store.db.jobs[0].fairCharge.status, 'reserved');
});

test('启动核验缺失文件的待恢复冷任务不重新热载历史或丢弃证据', async (t) => {
  const store = await open(t, fixture()),
    { ctx } = await completionContext(store);
  const pending = {
    ...describeStoredResult('missing-result', 'generate', { buffer: png, mimeType: 'image/png' }),
    attemptId: store.db.jobs[0].generationAttempt.id,
  };
  await store.update(
    (db) => {
      db.jobs[0].resultPending = pending;
    },
    { dirtyRows: { jobs: ['review'] } },
  );
  store.trimRuntimeCollection('jobs', 0);
  assert.equal(store.db.jobs.length, 0);
  await ctx.recoverPendingGenerationResults();
  assert.equal(store.db.jobs.length, 0);
  assert.deepEqual(store.selectItemById('jobs', 'review').resultPending, pending);
  assert.equal(store.readFairQuotaSettlement('review').status, 'refunded');
});

test(
  '离线停用账户启动后自动关闭到期核对，真实 HTTP 可领取失败任务的迟到图片且不补扣',
  { timeout: 20000 },
  async (t) => {
    const seed = fixture(Date.now() - 400000),
      offline = fixture(Date.now() - 400000, { id: 'offline-due' });
    seed.jobs.push(offline.jobs[0]);
    seed.ledger.unshift(...offline.ledger);
    seed.settings.fairQuota.reservations['offline-due'] =
      offline.settings.fairQuota.reservations['offline-due'];
    const store = await open(t, seed),
      { ctx } = await completionContext(store);
    const r = reservation(store.db),
      before = structuredClone(store.db.settings.fairQuota.balances);
    await ctx.completeGeneration(
      r,
      r.job.request,
      { buffer: png, mimeType: 'image/png' },
      { jobId: 'review' },
    );
    const disabledAt = new Date().toISOString();
    await store.update(
      (db) => {
        const account = db.accounts[0];
        account.enabled = false;
        account.inFlight = 0;
        account.updatedAt = disabledAt;
        recordDomainEvent(db, {
          eventType: 'AvailabilityChanged',
          streamId: `account:${account.id}`,
          accountId: account.id,
          occurredAt: disabledAt,
          payload: {
            accountId: account.id,
            enabled: account.enabled,
            updatedAt: disabledAt,
            reason: 'administrator',
          },
        });
      },
      {
        immediate: true,
        domainCommand: {
          actorId: 'synthetic-offline-review-admin',
          actorKind: 'admin',
          recordedAt: disabledAt,
        },
      },
    );
    // 原 reserve/settle/complete 的实际保存行先封存，不能用新正常请求替代原 parentless 源。
    const original = structuredClone(await store.read()),
      sourceCut = store.sqlite
        .prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM event_commits')
        .get().seq,
      originalSnapshot = path.join(store.dataDir, 'late-original-source.sqlite');
    await store.sqlite.backup(originalSnapshot);
    const originalSeal = new Database(originalSnapshot);
    originalSeal.pragma('journal_mode=DELETE');
    originalSeal.close();
    const originalSha256 = relationalSnapshotSha256(originalSnapshot);
    await store.close();

    const state = await childFixture(t, 'generation-review-late'),
      bootstrapSnapshot = path.join(state.directory, 'bootstrap-selected.sqlite');
    const bootstrapSource = new Database(state.filename, { readonly: true });
    await bootstrapSource.backup(bootstrapSnapshot);
    bootstrapSource.close();
    const bootstrapSeal = new Database(bootstrapSnapshot);
    bootstrapSeal.pragma('journal_mode=DELETE');
    bootstrapSeal.close();
    const bootstrapSha256 = relationalSnapshotSha256(bootstrapSnapshot);
    state.dataDir = path.join(state.directory, 'restored');
    state.filename = path.join(state.dataDir, 'library.sqlite');
    state.env.DATA_DIR = state.dataDir;
    await mkdir(state.dataDir);
    const db = new Database(state.filename);
    try {
      // Bootstrap 的同 cut 公共+所选私有原样恢复为 building，再经 owner 导入本例原行。
      const selected = new Database(bootstrapSnapshot, { readonly: true });
      try {
        initializeRelationalStorage(
          db,
          selected.prepare('SELECT history_id FROM projection_state WHERE id=1').get().history_id,
        );
        const replayer = new RelationalCommitReplayer(db),
          inputs = new RelationalInputStore(selected);
        for (const { seq } of selected
          .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
          .all()) {
          db.transaction(() =>
            replayer.replayCommit(readRelationalCommit(selected, seq), inputs),
          )();
        }
        db.transaction(() =>
          restoreRelationalPrivateSnapshot(db, bootstrapSnapshot, bootstrapSha256),
        )();
      } finally {
        selected.close();
      }
      const at = Date.now(),
        command = (name, actorKind = 'system') => ({
          commandId: `late-fixture:${name}`,
          fingerprint: domainDigest(name),
          actorKind,
          actorId: actorKind === 'admin' ? 'local-admin' : null,
          recordedAt: at,
          correlationId: null,
          causationId: null,
          policyVersion: 'retained-source',
          pricingVersion: 'retained-source',
        }),
        members = new RelationalMemberStore(db, state.env),
        quotas = new RelationalQuotaStore(db),
        accounts = new RelationalAccountStore(db, quotas, new Map(), state.env),
        inputs = new RelationalInputStore(db),
        replicas = new RelationalReplicaStore(db, { dataDir: state.dataDir });
      db.transaction(() => {
        // 这是当前测试入口的真实凭据注册，不把新 epoch/创建日当作旧尝试的元数据。
        for (const user of original.users) {
          members.registerMember(command(`member:${user.id}`), {
            memberId: user.id,
            displayName: user.id,
            apiKey: user.token,
          });
        }
        accounts.prepareAccountCommand(
          command('disabled-account', 'admin'),
          {
            kind: 'register',
            accountId: original.accounts[0].id,
            displayName: 'Current disabled retained-source account',
            token: state.env.NOVELAI_TOKEN,
            configVersion: db
              .prepare('SELECT config_version AS version FROM sharing_policy WHERE id=1')
              .get().version,
            bind: false,
            enabled: false,
          },
          null,
        );
      })();
      const oldImage = original.images.find(
          (image) => image.id === original.jobs.find((job) => job.id === 'review').imageId,
        ),
        descriptor = {
          imageId: oldImage.id,
          file: oldImage.file,
          operation: oldImage.operation,
          mimeType: oldImage.mimeType,
          byteLength: oldImage.byteLength,
          sha256: oldImage.sha256,
        },
        content = await readStoredResultContent(store.dataDir, descriptor);
      assert.ok(content);
      await mkdir(path.join(state.dataDir, 'images'));
      await writeFile(path.join(state.dataDir, descriptor.file), content.buffer);
      assert.deepEqual((await readStoredResultContent(state.dataDir, descriptor)).buffer, png);
      // 实际恢复读完成后的本次验证证据，不是旧图片 createdAt/publishedAt 的替身。
      const verifiedAt = Date.now(),
        sourceId = 'late-original-source',
        local = db.transaction(() =>
          replicas.planLocalArtifactPublication(descriptor, verifiedAt),
        )(),
        storageTargetId = local.replica.payload.storageTargetId,
        manifest = {
          originalSha256,
          sourceCut,
          localVerification: {
            artifactId: oldImage.id,
            storageTargetId,
            verifiedAt,
            sha256: descriptor.sha256,
          },
        },
        events = [],
        event = (eventType, streamId, payload, memberId = null, accountId = null) => ({
          eventType,
          streamId,
          payload,
          memberId,
          accountId,
          occurredAt: verifiedAt,
          policyVersion: 'retained-source',
          pricingVersion: 'retained-source',
        }),
        origin = (id) => ({ sourceId, originalId: id }),
        stamp = (value) => (value == null ? null : Date.parse(value));
      events.push(
        event('ImportSourceRegistered', `import:${sourceId}`, {
          sourceId,
          sourceSha256: originalSha256,
          sourceCommit: '3a15979fde2af494e7114f4a9f2192db72aad4e9',
          cutoffSeq: sourceCut,
          cutoffAt: at,
          manifestSha256: domainDigest(manifest),
          route: 'B',
        }),
      );
      if (local.target) {
        events.push(
          event(local.target.eventType, `storage:${storageTargetId}`, local.target.payload),
        );
      }
      db.transaction(() => {
        for (const [memberId, balance] of Object.entries(before)) {
          events.push(
            event(
              'OpeningWalletRecorded',
              `account:${original.accounts[0].id}`,
              {
                accountId: original.accounts[0].id,
                memberId,
                available: {
                  v5: balance.v5Available,
                  anlasFixed: balance.anlasFixedAvailable,
                  anlasPurchased: balance.anlasPurchasedAvailable,
                },
                origin: origin(`balance:${memberId}`),
              },
              memberId,
              original.accounts[0].id,
            ),
          );
        }
        for (const job of original.jobs) {
          const attempt = job.generationAttempt,
            charge = job.fairCharge,
            review = job.generationReview,
            requestId = `retained-container:${job.id}`,
            inputId = `retained-input:${job.id}`,
            body = {
              operation: attempt.operation,
              model: job.request.model,
              parameters: {
                width: job.request.width,
                height: job.request.height,
                steps: job.request.steps,
              },
            },
            response = generationResponseEvidence(job),
            attemptEventId = `opening:${attempt.id}`,
            chargeEventId = `opening-charge:${job.id}`;
          assert.equal(Object.hasOwn(job, 'requestId'), false);
          assert.equal(attempt.memberId, charge.userId);
          assert.equal(attempt.accountId, charge.accountId);
          assert.equal(review.deadlineAt, review.firstUnknownAt + GENERATION_REVIEW_MS);
          events.push(
            event(
              'OpeningInputRecorded',
              `input:${inputId}`,
              {
                ...inputs.stageOpeningInput({
                  inputId,
                  memberId: attempt.memberId,
                  completeness: 'partial',
                  body,
                  logicalBytes: Buffer.byteLength(JSON.stringify(body)),
                  assets: [],
                }),
                origin: origin(`input:${job.id}`),
              },
              attempt.memberId,
            ),
          );
          events.push(
            event(
              'OpeningRequestRecorded',
              `request:${requestId}`,
              {
                requestId,
                originalRequestId: null,
                memberId: attempt.memberId,
                inputId,
                contextId: null,
                source: job.source,
                operation: attempt.operation,
                model: job.request.model,
                sampleCount: null,
                seedMode: 'unknown',
                confirmation: null,
                acceptedAt: null,
                retiredAt: null,
                origin: origin(job.id),
              },
              attempt.memberId,
            ),
          );
          events.push(
            event(
              'OpeningJobRecorded',
              `request:${requestId}`,
              {
                jobId: job.id,
                requestId,
                createdAt: stamp(job.createdAt),
                sampleIndex: attempt.sampleIndex,
                actualSeed: null,
                status: job.status,
                queueSeq: null,
                queueExpiresAt: null,
                deadlineAt: null,
                admissionEpoch: null,
                artifactId: job.imageId ?? null,
                cacheKey: null,
                completedAt: stamp(job.completedAt),
                errorCode: null,
                errorStatus: null,
                cancellationReason: null,
                lateResultAt: stamp(job.lateResultAt),
                freeSucceededAt: null,
                freeWarningThreshold: null,
                retiredAt: null,
                origin: origin(job.id),
              },
              attempt.memberId,
            ),
          );
          events.push({
            ...event(
              'OpeningAttemptRecorded',
              `request:${requestId}`,
              {
                attemptId: attempt.id,
                jobId: job.id,
                accountId: attempt.accountId,
                credentialEpoch: null,
                connectionSourceEventId: null,
                authorizedAt: attempt.dispatchIntentAt,
                response: {
                  ...response,
                  upstreamStartedAt: stamp(response.upstreamStartedAt),
                  upstreamHeadersAt: stamp(response.upstreamHeadersAt),
                  upstreamCompletedAt: stamp(response.upstreamCompletedAt),
                },
                candidateMode: attempt.candidateMode,
                upstreamCandidates: attempt.upstreamCandidates,
                actualConsumption: null,
                lossCoverageRemaining: { v5: null, anlasFixed: null, anlasPurchased: null },
                reviewStatus: review.status,
                firstUnknownAt: review.firstUnknownAt,
                reviewDeadlineAt: review.deadlineAt,
                reviewClosedAt: review.closedAt,
                originalLossEventId: review.lossEventId,
                originalClosureEventId: review.closureEventId,
                preparedArtifactId: null,
                preparedFile: null,
                preparedMimeType: null,
                preparedByteLength: null,
                preparedSha256: null,
                origin: origin(attempt.id),
              },
              attempt.memberId,
              attempt.accountId,
            ),
            eventId: attemptEventId,
          });
          events.push({
            ...event(
              'OpeningChargeRecorded',
              `account:${charge.accountId}`,
              {
                originalJobId: job.id,
                originalMemberId: charge.userId,
                originalAccountId: charge.accountId,
                jobId: job.id,
                walletAccountId: charge.accountId,
                walletMemberId: charge.userId,
                attemptId: attempt.id,
                status: charge.status,
                resolution: 'live',
                originRef: chargeEventId,
                resources: generationChargeResources(charge),
                billingMode: charge.billingMode ?? charge.resource,
                estimated: charge.estimated ?? null,
                // 原非fallback资源不使用这两列；契约固定为0，不代表未知fallback估值。
                v5Equivalent: charge.billingMode === 'anlas_fallback' ? charge.v5Equivalent : '0',
                sharedV5Reserved:
                  charge.billingMode === 'anlas_fallback' ? charge.sharedV5Reserved : '0',
                configVersion: charge.configVersion,
                reservedAt: charge.reservedAt,
                terminalAt: job.generationRefund.refundedAt,
                reservationEventId: chargeEventId,
                terminalEventId: chargeEventId,
                origin: origin(`charge:${job.id}`),
              },
              charge.userId,
              charge.accountId,
            ),
            eventId: chargeEventId,
          });
          if (job.imageId) {
            assert.equal(job.imageId, oldImage.id);
            const recovered = original.ledger.find(
              (entry) => entry.type === 'generation_result_recovered' && entry.jobId === job.id,
            );
            assert.ok(recovered);
            assert.equal(recovered.attemptId, attempt.id);
            assert.equal(recovered.imageId, oldImage.id);
            assert.equal(recovered.memberId, oldImage.memberId);
            assert.equal(recovered.accountId, oldImage.accountId);
            events.push(
              event(
                'OpeningArtifactRecorded',
                `artifact:${oldImage.id}`,
                {
                  artifactId: oldImage.id,
                  memberId: attempt.memberId,
                  inputId,
                  originalAttemptId: attempt.id,
                  operation: oldImage.operation,
                  mimeType: oldImage.mimeType,
                  byteLength: oldImage.byteLength,
                  sha256: oldImage.sha256,
                  publishedAt: null,
                  retiredAt: null,
                  origin: origin(oldImage.id),
                },
                attempt.memberId,
                attempt.accountId,
              ),
            );
          }
        }
        events.push(
          event(
            'OpeningReplicaRecorded',
            `artifact:${oldImage.id}`,
            {
              artifactId: oldImage.id,
              storageTargetId,
              objectKey: path.basename(oldImage.file),
              encoding: 'identity',
              uploadedAt: null,
              verifiedAt,
              deletedAt: null,
              lastErrorCode: null,
              origin: origin(`capture:${oldImage.id}`),
            },
            'a',
          ),
        );
        new RelationalCommitReplayer(db).appendCommit(
          {
            ...command('opening'),
            recordedAt: verifiedAt,
            fingerprint: domainDigest(events),
            receipt: { originalRows: original.jobs.map((job) => job.id) },
            events,
            expectedVersions: Object.fromEntries(
              events.map((e) => [
                e.streamId,
                db
                  .prepare(
                    'SELECT CAST(stream_version AS TEXT) AS seq FROM stream_heads WHERE stream_id=?',
                  )
                  .get(e.streamId)?.seq ?? '0',
              ]),
            ),
          },
          inputs,
        );
      })();
      const sealed = path.join(state.directory, 'late-selected.sqlite');
      await db.backup(sealed);
      const seal = new Database(sealed);
      seal.pragma('journal_mode=DELETE');
      seal.close();
      await verifyAndInstallRelationalStorage(db, {
        sourcePath: sealed,
        sourceSha256: relationalSnapshotSha256(sealed),
        historyId: db.prepare('SELECT history_id FROM projection_state WHERE id=1').get()
          .history_id,
        finalCommitSeq: db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get()
          .seq,
        replicas: { dataDir: state.dataDir },
      });
    } finally {
      db.close();
    }
    const walletsBefore = query(state, 'SELECT * FROM wallets ORDER BY account_id,member_id');
    let upstreamCalls = 0;
    const upstream = http.createServer((_req, res) => {
      upstreamCalls++;
      res.writeHead(500);
      res.end();
    });
    upstream.listen(0, '127.0.0.1');
    await once(upstream, 'listening');
    t.after(() => new Promise((resolve) => upstream.close(resolve)));
    let startupTimer;
    const server = await Promise.race([
      launchGateway(state, `http://127.0.0.1:${upstream.address().port}`, {
        PUBLIC_HOST: 'gateway.example',
      }),
      new Promise((_resolve, reject) => {
        startupTimer = setTimeout(() => reject(new Error('server startup timeout')), 10000);
      }),
    ]).finally(() => clearTimeout(startupTimer));
    try {
      const base = server.origin,
        headers = { host: 'gateway.example', authorization: 'Bearer STA1N-a' };
      const response = await fetch(`${base}/api/jobs/review`, { headers });
      assert.equal(response.status, 200);
      const job = await response.json();
      assert.equal(job.status, 'failed');
      assert.equal(job.lateResultAvailable, true);
      assert.ok(job.imageUrl);
      const downloaded = await fetch(new URL(job.imageUrl, base), { headers });
      assert.equal(downloaded.status, 200);
      assert.deepEqual(Buffer.from(await downloaded.arrayBuffer()), png);
      const withoutCookie = await fetch(new URL(job.imageUrl, base), {
        headers: { host: 'gateway.example' },
      });
      assert.equal(withoutCookie.status, 200);
      assert.deepEqual(Buffer.from(await withoutCookie.arrayBuffer()), png);
      const other = await fetch(`${base}/api/jobs/review`, {
        headers: { ...headers, authorization: 'Bearer STA1N-b' },
      });
      assert.equal(other.status, 403);
      for (let i = 0; i < 50; i++) {
        const due = query(
          state,
          "SELECT review_status FROM generation_attempts WHERE job_id='offline-due'",
        )[0];
        if (due.review_status === 'estimated_closed') {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      assert.equal(
        query(state, "SELECT review_status FROM generation_attempts WHERE job_id='offline-due'")[0]
          .review_status,
        'estimated_closed',
      );
      assert.deepEqual(
        query(state, 'SELECT * FROM wallets ORDER BY account_id,member_id'),
        walletsBefore,
      );
      const balances = Object.fromEntries(
        query(
          state,
          'SELECT member_id AS id,v5_available AS v5Available,anlas_fixed_available AS anlasFixedAvailable,anlas_purchased_available AS anlasPurchasedAvailable FROM wallets WHERE account_id=? ORDER BY member_id',
          original.accounts[0].id,
        )
          .filter(({ id }) => Object.hasOwn(before, id))
          .map(({ id, ...balance }) => [
            id,
            {
              ...balance,
              anlasAvailable: formatAmount(
                parseAmount(balance.anlasFixedAvailable) +
                  parseAmount(balance.anlasPurchasedAvailable),
              ),
            },
          ]),
      );
      assert.deepEqual(balances, before);
      assert.equal(
        query(state, 'SELECT enabled FROM upstream_accounts WHERE id=?', original.accounts[0].id)[0]
          .enabled,
        0,
      );
      assert.equal(upstreamCalls, 0);
    } finally {
      await server.stop();
      assertChildPublicReplay(state);
    }
  },
);

test('已退款的批次首图迟到找回只保存结果，不激活或复活剩余图片', async (t) => {
  const seed = fixture();
  seed.jobs[0].requestId = 'late-parent';
  seed.jobs.push({
    id: 'cancelled-child',
    requestId: 'late-parent',
    sampleIndex: 1,
    userToken: 'STA1N-a',
    status: 'failed',
    error: 'remaining batch cancelled',
    createdAt: new Date().toISOString(),
  });
  const store = await open(t, seed, 'late-parent');
  const { ctx } = await completionContext(store);
  const r = reservation(store.db);
  await ctx.completeGeneration(
    r,
    r.job.request,
    { buffer: png, mimeType: 'image/png' },
    { jobId: 'review' },
  );
  assert.equal(store.selectItemById('jobs', 'review').status, 'failed');
  assert.ok(store.selectItemById('jobs', 'review').imageId);
  assert.equal(store.selectItemById('jobs', 'cancelled-child').status, 'failed');
  assert.equal(store.selectItemById('jobs', 'cancelled-child').queueSeq, undefined);
  assert.equal(store.queuedJobIds().length, 0);
});

test('父取消与实际完成/激活并发时，成功产物照常结算，剩余 child 无论顺序都取消', async (t) => {
  for (const cancelFirst of [true, false]) {
    const seed = fixture(Date.now(), { refund: false });
    seed.jobs[0].requestId = 'cancel-parent';
    seed.jobs.push({
      id: 'next-child',
      requestId: 'cancel-parent',
      sampleIndex: 1,
      userToken: 'STA1N-a',
      status: 'pending',
      request: seed.jobs[0].request,
      createdAt: new Date().toISOString(),
      queueExpiresAt: new Date(Date.now() + 600000).toISOString(),
    });
    const store = await open(t, seed, 'cancel-parent');
    const { ctx } = await completionContext(store);
    const r = reservation(store.db);
    const complete = () =>
      ctx.completeGeneration(
        r,
        r.job.request,
        { buffer: png, mimeType: 'image/png' },
        { jobId: 'review' },
      );
    const cancel = () => ctx.cancelGenerationRequest('cancel-parent', 'a');
    await Promise.all(cancelFirst ? [cancel(), complete()] : [complete(), cancel()]);
    assert.equal(store.selectItemById('jobs', 'review').status, 'done');
    assert.equal(store.selectItemById('jobs', 'review').fairCharge.status, 'settled');
    assert.equal(store.selectItemById('jobs', 'next-child').status, 'failed');
    assert.equal(
      store.selectItemById('jobs', 'next-child').cancellationReason,
      'request_cancelled',
    );
    assert.equal(store.db.ledger.filter((event) => event.type === 'generation_refunded').length, 0);
    assert.equal((await ctx.getGenerationRequest('cancel-parent', 'a')).status, 'partial');
  }
});
