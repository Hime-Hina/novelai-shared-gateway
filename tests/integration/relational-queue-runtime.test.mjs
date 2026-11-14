import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createServer } from 'node:http';
import { EventEmitter } from 'node:events';
import path from 'node:path';
import fsPromises from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import { encode } from '@msgpack/msgpack';
import {
  fixture,
  meta,
  NOW,
  image,
  databaseState,
  keys,
} from '../helpers/relational-generation.mjs';
import { createRelationalQueueRuntime } from '../../server/generation/relational-queue-runtime.ts';
import { createRelationalMemberGenerationAdmission } from '../../server/generation/relational-member-admission.ts';
import { createRelationalMemberGenerationHttp } from '../../server/generation/relational-member-http.ts';
import { closeMemberGenerationStreams } from '../../server/generation/member-http.ts';
import {
  normalizeMemberGeneration,
  materializeMemberGeneration,
} from '../../server/generation/member-contract.ts';
import { prepareRelationalInput } from '../../server/generation/relational-input.ts';
import { createGenerationPayloadBudget } from '../../server/generation/payload-budget.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { relationalSnapshotSha256 } from '../../server/storage/relational-private-restore.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalAccountStore } from '../../server/storage/relational-account-store.ts';
import {
  withAuthenticatedMemberApiKey,
  authenticateMemberApiKey,
  createMemberSession,
} from '../../server/members/authentication.ts';
import { readRelationalArtifactContent } from '../../server/storage/relational-replica-io.ts';
import { describeStoredResult, publishStoredResult } from '../../server/storage/result-files.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';

async function waitFor(predicate) {
  const until = Date.now() + 5000;
  while (!predicate()) {
    assert.ok(Date.now() < until, 'runtime condition did not complete');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}
async function setup(t, handler, options = {}) {
  const cleanup = [],
    beforeStop = [],
    runtimes = [];
  const f = await fixture({ after: (fn) => cleanup.push(fn) }, options);
  let clock = NOW,
    calls = 0;
  const events = new EventEmitter();
  const server = createServer(async (req, res) => {
    assert.equal(f.db.inTransaction, false);
    let body = '';
    for await (const chunk of req) {
      body += chunk;
    }
    calls++;
    if (handler) {
      await handler(req, res, JSON.parse(body));
    } else {
      res.writeHead(200, { 'content-type': 'image/webp' });
      res.end(image);
    }
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    for (const run of beforeStop.reverse()) {
      await run();
    }
    server.closeAllConnections();
    for (const runtime of runtimes) {
      await runtime.stop();
    }
    await new Promise((resolve) => server.close(resolve));
    for (const run of cleanup.reverse()) {
      await run();
    }
  });
  const env = {
    NOVELAI_API_URL: `http://127.0.0.1:${server.address().port}`,
    NOVELAI_STREAM_IMAGE: 'false',
    MOCK_WHEN_NO_ACCOUNT: 'false',
  };
  return {
    ...f,
    server,
    env,
    events,
    get calls() {
      return calls;
    },
    get clock() {
      return clock;
    },
    set clock(value) {
      clock = value;
    },
    beforeStop: (fn) => beforeStop.push(fn),
    afterStop: (fn) => cleanup.push(fn),
    runtime(extra = {}) {
      const runtime = createRelationalQueueRuntime({
        generations: f.generations,
        inputs: f.inputs,
        dataDir: f.dir,
        sqlite: f.db,
        controls: new Map(),
        events,
        env,
        now: () => clock,
        ...extra,
      });
      runtimes.push(runtime);
      return runtime;
    },
    async ready() {
      const sourcePath = path.join(f.dir, `ready-source-${Date.now()}.sqlite`);
      await f.db.backup(sourcePath);
      const source = new Database(sourcePath);
      source.pragma('journal_mode=DELETE');
      source.close();
      const selected = {
        sourcePath,
        sourceSha256: relationalSnapshotSha256(sourcePath),
        historyId: 'generation-history',
        finalCommitSeq: f.db
          .prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits')
          .get().seq,
        replicas: { dataDir: f.dir },
      };
      await verifyAndInstallRelationalStorage(f.db, selected);
      assertRelationalStorageReady(f.db);
      return selected;
    },
  };
}

function replayAll(t, f, source = f.db) {
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replayer = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(source);
  const before = source.serialize(),
    fetch = globalThis.fetch;
  let network = 0;
  globalThis.fetch = () => {
    network++;
    throw new Error('Business replay attempted network IO.');
  };
  try {
    const rows = source
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all();
    for (const { seq } of rows) {
      target.transaction(() => replayer.replayCommit(readRelationalCommit(source, seq), inputs))();
    }
    for (const table of [
      'generation_jobs',
      'generation_attempts',
      'charges',
      'wallets',
      'account_quota_state',
      'upstream_accounts',
      'artifacts',
      'artifact_replicas',
    ]) {
      assert.deepEqual(
        target.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
        source.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
      );
    }
    const complete = target.serialize();
    target.transaction(() =>
      replayer.replayCommit(readRelationalCommit(source, rows.at(-1).seq), inputs),
    )();
    assert.ok(target.serialize().equals(complete));
    assert.ok(source.serialize().equals(before));
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
    assert.throws(() => assertRelationalStorageReady(target), /incomplete/);
  } finally {
    globalThis.fetch = fetch;
  }
  assert.equal(network, 0);
  return target;
}

test('只有正式installer就绪库可启动，物理Map和关闭通知复用根core', async (t) => {
  const f = await setup(t),
    runtime = f.runtime();
  await assert.rejects(runtime.start(), /incomplete/);
  assert.equal(f.calls, 0);
  await runtime.stop();
  await f.ready();
  const shared = new Map(),
    live = f.runtime({ controls: shared });
  await live.start();
  assert.equal(live.controls, shared);
  assert.equal(live.start(), live.start());
  let closed = 0;
  f.events.on('close', () => {
    closed++;
  });
  assert.equal(f.calls, 0);
  await live.stop();
  await live.stop();
  assert.equal(closed, 1);
});

test('真实HTTP/PST与冷输入执行，SQL外发布本地字节、原子结算后才推进余图', async (t) => {
  const observed = [];
  const f = await setup(t, (req, res, body) => {
    assert.equal(req.headers.authorization, 'Bearer synthetic-upstream-pst');
    assert.equal(req.url, '/ai/generate-image');
    observed.push(body);
    res.writeHead(200, { 'content-type': 'image/webp' });
    res.end(image);
  });
  await f.accept('batch', { samples: 2, member: true, steps: 30 });
  await f.ready();
  f.events.on('commit', (...args) => {
    assert.equal(f.db.inTransaction, false);
    assert.deepEqual(args, []);
  });
  const errors = [],
    runtime = f.runtime({
      onError: (e) => errors.push(e),
    });
  await runtime.start();
  runtime.tick();
  await waitFor(
    () =>
      f.db.prepare("SELECT status FROM generation_jobs WHERE id='batch-0'").get().status === 'done',
  );
  assert.equal(f.calls, 1);
  assert.equal(f.generations.readCharge('batch-0').status, 'settled');
  assert.equal(
    f.db.prepare("SELECT status FROM generation_jobs WHERE id='batch-1'").get().status,
    'queued',
  );
  runtime.tick();
  assert.equal(f.calls, 1);
  f.clock += 10001;
  await waitFor(
    () =>
      f.db.prepare("SELECT status FROM generation_jobs WHERE id='batch-1'").get().status === 'done',
  );
  assert.equal(f.calls, 2);
  assert.deepEqual(
    observed.map((row) => row.parameters.seed),
    [42, 43],
  );
  assert.ok(observed.every((row) => row.parameters.n_samples === 1));
  for (const { id } of f.db.prepare('SELECT id FROM artifacts').all()) {
    assert.deepEqual(
      (await readRelationalArtifactContent(f.db, id, { dataDir: f.dir })).buffer,
      image,
    );
  }
  assert.deepEqual(errors, []);
  await runtime.stop();
  assert.equal(runtime.controls.size, 0);
  assert.equal(runtime.activePayloadBytes(), 0);
  assertRelationalStorageReady(f.db);
  replayAll(t, f);
});

test('真实HTTP断流保留未知责任及NULL上游完成时间，物理结束补原10秒gap', async (t) => {
  const f = await setup(t, (req, res) => {
    res.writeHead(200, { 'content-type': 'image/webp', 'content-length': image.length });
    res.write(image.subarray(0, 4));
    setTimeout(() => res.destroy(), 10);
  });
  await f.accept('broken', { steps: 30 });
  await f.accept('waiting', { memberId: 'm2', steps: 30 });
  await f.ready();
  const runtime = f.runtime();
  await runtime.start();
  runtime.tick();
  await waitFor(
    () =>
      f.db.prepare("SELECT status FROM generation_jobs WHERE id='broken-0'").get().status ===
        'failed' && runtime.controls.size === 0,
  );
  assert.equal(f.calls, 1);
  const attempt = f.db.prepare("SELECT * FROM generation_attempts WHERE job_id='broken-0'").get();
  assert.equal(attempt.upstream_completed_at, null);
  assert.equal(attempt.review_status, 'pending');
  assert.equal(
    f.db.prepare("SELECT error_status FROM generation_jobs WHERE id='broken-0'").get().error_status,
    500,
  );
  assert.equal(f.generations.readCharge('broken-0').status, 'refunded');
  assert.equal(
    f.db.prepare('SELECT cooldown_until AS at FROM upstream_accounts').get().at,
    NOW + 10000,
  );
  runtime.tick();
  assert.equal(f.calls, 1);
  f.clock += 10000;
  runtime.tick();
  await waitFor(() => f.calls === 2);
  await runtime.stop();
  replayAll(t, f);
});

test('实际上游401/403经协调器非abort失败同笔失效退款，后续派发拒绝且完整恢复不复活认证', async (t) => {
  for (const [status, errorCode] of [
    [401, 'invalid_key'],
    [403, 'access_denied'],
  ]) {
    await t.test(String(status), async (t) => {
      const f = await setup(
        t,
        (req, res) => {
          assert.equal(req.url, '/ai/generate-image');
          assert.equal(req.headers.authorization, 'Bearer synthetic-upstream-pst');
          res.writeHead(status, { 'content-type': 'application/json' });
          res.end(JSON.stringify({ error: `synthetic ${status}: synthetic-upstream-pst` }));
        },
        { seconds: 0 },
      );
      await f.accept('denied', { steps: 30 });
      await f.accept('next', { memberId: 'm2', steps: 30 });
      await f.ready();
      const quotas = new RelationalQuotaStore(f.db),
        beforeQuota = quotas.readAccountQuota('a'),
        wallets = f.db
          .prepare(
            'SELECT account_id,member_id,v5_available,anlas_fixed_available,anlas_purchased_available FROM wallets ORDER BY member_id',
          )
          .all();
      const officialFields = [
        'observationEventId',
        'observedAt',
        'observedCredentialEpoch',
        'observedSequence',
        'quotaTier',
        'subscriptionActive',
        'subscriptionExpiresAt',
        'observedV5',
        'observedAnlasFixed',
        'observedAnlasPurchased',
        'secondsPerPercent',
        'baselineEventId',
        'refillCursorAt',
      ];
      let failureNotifications = 0;
      const observe = (...args) => {
        assert.equal(f.db.inTransaction, false);
        assert.deepEqual(args, []);
        if (f.generations.readGenerationExecutionState('denied-0').status === 'failed') {
          failureNotifications++;
          assert.equal(quotas.readAccountQuota('a').quotaErrorCode, errorCode);
          assert.equal(f.generations.readCharge('denied-0').status, 'refunded');
          assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_diagnostics').get().n, 1);
        }
      };
      f.events.on('commit', observe);
      const runtime = f.runtime();
      await runtime.start();
      runtime.tick();
      await waitFor(
        () =>
          f.generations.readGenerationExecutionState('denied-0').status === 'failed' &&
          runtime.controls.size === 0,
      );
      f.events.off('commit', observe);
      assert.equal(failureNotifications, 1);
      assert.equal(runtime.status, 'running');
      assert.equal(f.calls, 1);
      const afterQuota = quotas.readAccountQuota('a');
      assert.equal(afterQuota.status, 'quota_sync_error');
      assert.equal(afterQuota.quotaErrorCode, errorCode);
      for (const key of officialFields) {
        assert.deepEqual(afterQuota[key], beforeQuota[key], key);
      }
      assert.equal(f.db.prepare('SELECT enabled FROM upstream_accounts').get().enabled, 1);
      assert.deepEqual(
        f.db
          .prepare(
            'SELECT account_id,member_id,v5_available,anlas_fixed_available,anlas_purchased_available FROM wallets ORDER BY member_id',
          )
          .all(),
        wallets,
      );
      const { seq } = f.db
        .prepare(
          "SELECT CAST(commit_seq AS TEXT) AS seq FROM domain_events WHERE event_type='JobFailed' AND json_extract(payload,'$.jobId')='denied-0'",
        )
        .get();
      const stored = readRelationalCommit(f.db, seq),
        invalidity = stored.events.find((event) => event.eventType === 'QuotaStatusChanged'),
        response = stored.events.find((event) => event.eventType === 'ResponseEvidenceRecorded'),
        failed = stored.events.find((event) => event.eventType === 'JobFailed'),
        refund = stored.events.find((event) => event.eventType === 'GenerationRefunded'),
        diagnostic = stored.events.find((event) => event.eventType === 'DiagnosticRecorded');
      assert.ok(invalidity);
      assert.ok(response);
      assert.ok(failed);
      assert.ok(refund);
      assert.ok(diagnostic);
      assert.equal(failed.payload.aborted, false);
      assert.equal(response.payload.response.upstreamHttpStatus, status);
      assert.notEqual(response.payload.response.upstreamStartedAt, null);
      assert.notEqual(response.payload.response.upstreamHeadersAt, null);
      assert.notEqual(response.payload.response.upstreamCompletedAt, null);
      const source = invalidity.payload.generationFailure,
        dispatch = JSON.parse(
          f.db
            .prepare('SELECT payload FROM domain_events WHERE event_id=?')
            .get(source.dispatchEventId).payload,
        );
      assert.equal(dispatch.jobId, 'denied-0');
      assert.equal(dispatch.credentialEpoch, 1);
      assert.equal(source.errorCode, errorCode);
      assert.equal(source.responseEventId, response.eventId);
      assert.equal(source.failureEventId, failed.eventId);
      assert.ok(response.eventIndex < invalidity.eventIndex);
      assert.ok(invalidity.eventIndex < refund.eventIndex);
      assert.ok(refund.eventIndex < diagnostic.eventIndex);
      assert.ok(diagnostic.eventIndex < failed.eventIndex);
      assert.equal(
        stored.events.some((event) => event.eventType === 'QuotaObserved'),
        false,
      );
      assert.equal(JSON.stringify(stored).includes('synthetic-upstream-pst'), false);
      const detail = f.inputs.readGenerationDiagnostic(diagnostic.payload.diagnosticId, 'denied-0');
      assert.match(detail, new RegExp(`synthetic ${status}: \\[REDACTED\\]`));
      assert.equal(detail.includes('synthetic-upstream-pst'), false);
      f.clock += 10001;
      runtime.tick();
      await waitFor(() => f.generations.readGenerationExecutionState('next-0').status === 'failed');
      assert.equal(f.calls, 1);
      assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_attempts').get().n, 1);
      assert.equal(f.generations.readCharge('next-0'), null);
      assert.equal(
        f.db.prepare("SELECT error_status FROM generation_jobs WHERE id='next-0'").get()
          .error_status,
        503,
      );
      await runtime.stop();
      const rebuilt = replayAll(t, f);
      assert.equal(
        new RelationalQuotaStore(rebuilt).readAccountQuota('a').quotaErrorCode,
        errorCode,
      );
      assert.equal(
        new RelationalInputStore(rebuilt).readGenerationDiagnostic(
          diagnostic.payload.diagnosticId,
          'denied-0',
        ),
        detail,
      );
      assert.equal(rebuilt.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n, 0);
      assert.equal(
        rebuilt.prepare('SELECT COUNT(*) AS n FROM members WHERE auth_epoch IS NOT NULL').get().n,
        0,
      );
    });
  }
});

test('本地发布仍悬挂时逻辑退款不释放物理槽/父输入，stop等待真实finally且迟到不补扣', async (t) => {
  const f = await setup(t);
  await f.accept('held', { samples: 2, steps: 30, deadlineAt: NOW + 100000 });
  await f.accept('other', { memberId: 'm2', steps: 30 });
  await f.ready();
  const originalRename = fsPromises.rename;
  let release,
    entered = false;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  fsPromises.rename = async (...args) => {
    if (String(args[0]).startsWith(f.dir) && String(args[0]).endsWith('.part')) {
      entered = true;
      await gate;
    }
    return originalRename(...args);
  };
  syncBuiltinESMExports();
  const restore = () => {
    fsPromises.rename = originalRename;
    syncBuiltinESMExports();
    release();
  };
  f.beforeStop(restore);
  const runtime = f.runtime();
  await runtime.start();
  runtime.tick();
  await waitFor(() => entered);
  assert.equal(f.calls, 1);
  assert.equal(runtime.controls.size, 1);
  f.clock = NOW + 100001;
  runtime.tick();
  assert.equal(f.generations.readCharge('held-0').status, 'refunded');
  assert.equal(
    f.db.prepare("SELECT status FROM generation_jobs WHERE id='held-1'").get().status,
    'failed',
  );
  const payload = f.db
    .prepare(
      "SELECT i.logical_bytes AS n FROM generation_inputs i JOIN generation_requests r ON r.input_id=i.id WHERE r.id='held'",
    )
    .get().n;
  assert.equal(runtime.activePayloadBytes(), f.inputs.activePayloadBytes() + payload);
  const body = {
    operation: 'generate',
    model: 'nai-diffusion-4-5-full',
    input: 'shared budget next request',
    parameters: { width: 128, height: 128, steps: 30, seed: 7, n_samples: 1 },
  };
  const normalized = normalizeMemberGeneration(body);
  const confirmation = {
    requestFingerprint: normalized.requestFingerprint,
    versions: f.generations.readMemberGenerationCapabilities('m1').versions,
    maxPerJob: { v5: '100', anlasFixed: '4000', anlasPurchased: '4000' },
    maxTotal: { v5: '100', anlasFixed: '4000', anlasPurchased: '4000' },
  };
  const nextBytes = prepareRelationalInput(
    {
      ...materializeMemberGeneration(normalized),
      clientContract: 'member-generation-v1',
      confirmation,
    },
    { kind: 'member', member: normalized.normalizationContext },
    {},
  ).logicalBytes;
  const payloadBudget = createGenerationPayloadBudget(
    () => f.inputs.activePayloadBytes(runtime.controls.values()),
    {
      request: nextBytes,
      active: f.inputs.activePayloadBytes() + payload + nextBytes - 1,
    },
  );
  const admit = createRelationalMemberGenerationAdmission({
    members: f.members,
    inputs: f.inputs,
    generations: f.generations,
    payloadBudget,
    env: {},
    now: () => f.clock,
  });
  const beforeAdmission = databaseState(f.db);
  await assert.rejects(
    admit({
      credential: { apiKey: keys.m1 },
      request: {
        headers: { 'idempotency-key': 'blocked-by-held-input' },
        rawHeaders: ['Idempotency-Key', 'blocked-by-held-input'],
      },
      body: { ...body, confirmation },
    }),
    (e) => e.statusCode === 429,
  );
  assert.equal(databaseState(f.db), beforeAdmission);
  f.clock += 20000;
  runtime.tick();
  assert.equal(f.calls, 1);
  let stopped = false;
  const stopping = runtime.stop().then(() => {
    stopped = true;
  });
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(stopped, false);
  assert.equal(runtime.controls.size, 1);
  restore();
  await stopping;
  assert.equal(runtime.controls.size, 0);
  assert.equal(runtime.activePayloadBytes(), f.inputs.activePayloadBytes());
  assert.equal(f.generations.readCharge('held-0').status, 'refunded');
  assert.equal(
    f.db
      .prepare("SELECT review_status AS status FROM generation_attempts WHERE job_id='held-0'")
      .get().status,
    'result_recovered',
  );
  assert.equal(
    f.db.prepare("SELECT status FROM generation_jobs WHERE id='held-0'").get().status,
    'failed',
  );
  assert.equal(f.calls, 1);
  const { id } = f.db.prepare('SELECT id FROM artifacts').get();
  assert.deepEqual(
    (await readRelationalArtifactContent(f.db, id, { dataDir: f.dir })).buffer,
    image,
  );
  replayAll(t, f);
});

test('真实发布后SQL结算故障保留原准备，重开先恢复原字节且不重发上游', async (t) => {
  const f = await setup(t);
  await f.accept('crash', { samples: 2, steps: 30 });
  await f.ready();
  f.db.exec(
    "CREATE TEMP TRIGGER fail_settlement BEFORE UPDATE OF status ON charges WHEN NEW.status='settled' BEGIN SELECT RAISE(ABORT,'synthetic commit failure'); END",
  );
  const errors = [],
    runtime = f.runtime({ onError: (e) => errors.push(e) });
  await runtime.start();
  runtime.tick();
  await waitFor(() => runtime.status === 'faulted' && runtime.controls.size === 0);
  assert.equal(f.calls, 1);
  assert.equal(f.generations.readCharge('crash-0').status, 'reserved');
  const { descriptor } = f.generations.readGenerationResultPreparation('crash-0');
  assert.deepEqual(await fsPromises.readFile(path.join(f.dir, descriptor.file)), image);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM artifacts').get().n, 0);
  assert.ok(errors.length > 0);
  assert.ok(!JSON.stringify(errors).includes('synthetic commit failure'));
  await assert.rejects(runtime.start(), /restarted/);
  f.db.exec('DROP TRIGGER fail_settlement');
  await runtime.stop();
  const file = f.db.name;
  f.db.close();
  const db = new Database(file);
  f.afterStop(() => db.close());
  const generations = new RelationalGenerationStore(db, {}),
    inputs = new RelationalInputStore(db);
  f.clock += 100;
  const restarted = f.runtime({ sqlite: db, generations, inputs });
  await restarted.start();
  assert.equal(f.calls, 1);
  assert.equal(generations.readCharge('crash-0').status, 'settled');
  assert.equal(
    db.prepare("SELECT status FROM generation_jobs WHERE id='crash-1'").get().status,
    'queued',
  );
  assert.deepEqual(
    (await readRelationalArtifactContent(db, descriptor.imageId, { dataDir: f.dir })).buffer,
    image,
  );
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM generation_attempts').get().n, 1);
  assertRelationalStorageReady(db);
  await restarted.stop();
  replayAll(t, f, db);
});

test('重启先收尾未完成原尝试和原期限，核对到期不虚构消费或重新发送', async (t) => {
  const f = await setup(t);
  await f.accept('interrupted', { samples: 2, steps: 30 });
  f.dispatch('interrupted-0');
  await f.accept('expired', { memberId: 'm2', queueExpiresAt: NOW + 50 });
  await f.ready();
  f.clock = NOW + 100;
  const runtime = f.runtime();
  await runtime.start();
  runtime.tick();
  assert.equal(f.calls, 0);
  assert.equal(f.generations.readCharge('interrupted-0').status, 'refunded');
  assert.ok(
    f.db.prepare("SELECT review_status FROM generation_attempts WHERE job_id='interrupted-0'").get()
      .review_status === 'pending',
  );
  assert.deepEqual(
    f.db
      .prepare('SELECT status FROM generation_jobs ORDER BY id')
      .all()
      .map((row) => row.status),
    ['failed', 'failed', 'failed'],
  );
  f.clock = f.db.prepare('SELECT review_deadline_at AS at FROM generation_attempts').get().at;
  runtime.tick();
  const closed = f.db
    .prepare(
      'SELECT review_status,actual_consumption,upstream_completed_at FROM generation_attempts',
    )
    .get();
  assert.equal(closed.review_status, 'estimated_closed');
  assert.equal(closed.actual_consumption, null);
  assert.equal(closed.upstream_completed_at, null);
  assert.equal(f.calls, 0);
  await runtime.stop();
  replayAll(t, f);
});

test('真实HTTP仍挂时先超时不启动NULL gap，真实finally补gap且旧epoch不能污染新账号', async (t) => {
  const f = await setup(t, (_req, res) => {
    res.writeHead(200, { 'content-type': 'image/webp' });
    res.flushHeaders();
  });
  await f.accept('hanging', { steps: 30, deadlineAt: NOW + 100000 });
  await f.accept('blocked', { memberId: 'm2', steps: 30 });
  await f.ready();
  const runtime = f.runtime();
  await runtime.start();
  runtime.tick();
  await waitFor(() => f.calls === 1);
  f.clock = NOW + 100001;
  runtime.tick();
  assert.equal(f.generations.readCharge('hanging-0').status, 'refunded');
  assert.equal(runtime.controls.size, 1);
  assert.equal(f.db.prepare('SELECT cooldown_until AS at FROM upstream_accounts').get().at, null);
  assert.ok(runtime.activePayloadBytes() > f.inputs.activePayloadBytes());
  runtime.tick();
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_attempts').get().n, 1);
  await waitFor(() => runtime.controls.size === 0);
  const attempt = f.db.prepare("SELECT * FROM generation_attempts WHERE job_id='hanging-0'").get();
  assert.equal(attempt.upstream_completed_at, null);
  assert.equal(attempt.review_status, 'pending');
  assert.equal(
    f.db.prepare('SELECT cooldown_until AS at FROM upstream_accounts').get().at,
    f.clock + 10000,
  );
  const ownerEnv = { OWNER_USER_ID: 'm1' },
    members = new RelationalMemberStore(f.db, ownerEnv);
  const accounts = new RelationalAccountStore(
    f.db,
    new RelationalQuotaStore(f.db, ownerEnv),
    runtime.controls,
    ownerEnv,
  );
  await withAuthenticatedMemberApiKey(
    members,
    keys.m1,
    (member) =>
      accounts.prepareAccountCommand(
        meta('rotate-after-end', f.clock, 'm1'),
        {
          kind: 'token',
          accountId: 'a',
          token: 'pst-synthetic-next',
        },
        member,
      ),
    ownerEnv,
  );
  const account = f.db.prepare('SELECT * FROM upstream_accounts').get();
  assert.equal(account.credential_epoch, 2);
  const observed = {
    dispatchState: attempt.dispatch_state,
    upstreamStartedAt: attempt.upstream_started_at,
    upstreamHeadersAt: attempt.upstream_headers_at,
    upstreamCompletedAt: null,
    upstreamHttpStatus: attempt.upstream_http_status,
  };
  for (const proof of [
    {},
    { attemptId: attempt.id, credentialEpoch: 2, kind: 'ordinary' },
    { attemptId: 'different-attempt', credentialEpoch: 1, kind: 'ordinary' },
  ]) {
    const before = databaseState(f.db);
    f.db.transaction(() =>
      assert.throws(
        () =>
          f.generations.recordGenerationResponse(
            meta('bad-physical-proof', f.clock + 1),
            'hanging-0',
            observed,
            proof,
          ),
        /物理结束/,
      ),
    )();
    assert.equal(databaseState(f.db), before);
  }
  f.db.transaction(() =>
    f.generations.recordGenerationResponse(
      meta('old-epoch-physical-end', f.clock + 2),
      'hanging-0',
      observed,
      {
        attemptId: attempt.id,
        credentialEpoch: 1,
        kind: 'capacity',
      },
    ),
  )();
  assert.deepEqual(f.db.prepare('SELECT * FROM upstream_accounts').get(), account);
  f.clock += 20000;
  runtime.tick();
  assert.equal(f.calls, 1);
  assert.equal(
    f.db.prepare("SELECT status FROM generation_jobs WHERE id='blocked-0'").get().status,
    'queued',
  );
  await runtime.stop();
});

test('实际Member接单暂停再恢复不复活原批次余图', async (t) => {
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  const f = await setup(t, async (_req, res) => {
    await gate;
    res.writeHead(200, { 'content-type': 'image/webp' });
    res.end(image);
  });
  f.beforeStop(() => release());
  await f.accept('stopped-batch', { member: true, samples: 3, steps: 30 });
  await f.ready();
  const runtime = f.runtime();
  await runtime.start();
  runtime.tick();
  await waitFor(() => f.calls === 1);
  const env = { OWNER_USER_ID: 'm1' },
    members = new RelationalMemberStore(f.db, env);
  for (const acceptingJobs of [false, true]) {
    await withAuthenticatedMemberApiKey(
      members,
      keys.m1,
      (member) =>
        members.changeMemberAdmission(
          meta(`admission-${acceptingJobs}`, f.clock, 'm1'),
          { memberId: 'm1', acceptingJobs },
          member,
        ),
      env,
    );
  }
  release();
  await waitFor(() => runtime.controls.size === 0);
  assert.equal(f.calls, 1);
  assert.equal(f.generations.readCharge('stopped-batch-0').status, 'settled');
  assert.deepEqual(
    f.db
      .prepare('SELECT status FROM generation_jobs ORDER BY sample_index')
      .all()
      .map((row) => row.status),
    ['done', 'failed', 'failed'],
  );
  assert.equal(f.generations.readCharge('stopped-batch-1'), null);
  f.clock += 20000;
  runtime.tick();
  assert.equal(f.calls, 1);
  await runtime.stop();
});

test('真实MessagePack上游预览复用瞬时owner，逐帧不写业务提交', async (t) => {
  let release, commitsAtDispatch;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  const frame = (event) => {
    const body = Buffer.from(encode(event)),
      length = Buffer.alloc(4);
    length.writeUInt32BE(body.length);
    return Buffer.concat([length, body]);
  };
  const f = await setup(t, async (req, res, body) => {
    assert.equal(req.url, '/ai/generate-image-stream');
    assert.equal(body.parameters.stream, 'msgpack');
    commitsAtDispatch = f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n;
    res.writeHead(200, { 'content-type': 'application/octet-stream' });
    res.write(
      frame({ event_type: 'intermediate', samp_ix: 0, step_ix: 2, total_steps: 30, image }),
    );
    await gate;
    res.end(frame({ event_type: 'final', samp_ix: 0, image }));
  });
  f.beforeStop(() => release());
  await f.accept('preview', { steps: 30 });
  await f.ready();
  const seen = [],
    runtime = f.runtime({ env: { ...f.env, NOVELAI_STREAM_IMAGE: 'true' } });
  const unwatch = runtime.previews.watch('preview-0', (frame) => seen.push(frame));
  f.beforeStop(unwatch);
  await runtime.start();
  runtime.tick();
  await waitFor(() => seen.length > 0);
  assert.equal(seen[0].token, 'm1');
  assert.deepEqual(seen[0].buffer, image);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n, commitsAtDispatch);
  release();
  await waitFor(() => runtime.controls.size === 0);
  assert.equal(f.generations.readCharge('preview-0').status, 'settled');
  assert.equal(runtime.previews.frames.size, 0);
  await runtime.stop();
});

test('失败命令外层SQL回滚时不发commit，不把SQL故障写成已退款的网络失败', async (t) => {
  const f = await setup(t, (_req, res) => {
    res.writeHead(503, { 'content-type': 'text/plain' });
    res.end('synthetic upstream failure');
  });
  await f.accept('atomic', { steps: 30 });
  await f.ready();
  f.db.exec(
    "CREATE TEMP TRIGGER fail_refund BEFORE UPDATE OF status ON charges WHEN NEW.status='refunded' BEGIN SELECT RAISE(ABORT,'synthetic refund failure'); END",
  );
  let notifications = 0,
    prefix;
  f.events.on('commit', () => {
    notifications++;
    prefix = databaseState(f.db);
    assert.equal(f.db.inTransaction, false);
  });
  const runtime = f.runtime();
  await runtime.start();
  runtime.tick();
  await waitFor(() => runtime.status === 'faulted' && runtime.controls.size === 0);
  assert.equal(f.calls, 1);
  assert.equal(notifications, 1);
  assert.equal(databaseState(f.db), prefix);
  assert.equal(f.generations.readCharge('atomic-0').status, 'reserved');
  assert.equal(
    f.db.prepare('SELECT dispatch_state AS state FROM generation_attempts').get().state,
    'unknown',
  );
  f.db.exec('DROP TRIGGER fail_refund');
  await runtime.stop();
  const restarted = f.runtime();
  await restarted.start();
  assert.equal(f.calls, 1);
  assert.equal(f.generations.readCharge('atomic-0').status, 'refunded');
  assert.equal(
    f.db.prepare('SELECT review_status AS status FROM generation_attempts').get().status,
    'pending',
  );
  await restarted.stop();
});

test('ready后准备字节丢失先阻止启动，保留原准备/预扣且不重发', async (t) => {
  const f = await setup(t);
  await f.accept('missing', { steps: 30 });
  f.dispatch('missing-0');
  const descriptor = describeStoredResult('missing-artifact', 'generate', {
    mimeType: 'image/webp',
    buffer: image,
  });
  f.db.transaction(() =>
    f.generations.prepareGenerationResult(
      meta('missing-prepare', NOW + 2),
      'missing-0',
      descriptor,
      f.response(NOW + 2),
    ),
  )();
  await publishStoredResult(f.dir, descriptor, image);
  await f.ready();
  await fsPromises.unlink(path.join(f.dir, descriptor.file));
  const before = databaseState(f.db),
    runtime = f.runtime();
  await assert.rejects(runtime.start(), /require recovery/);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.calls, 0);
  assert.equal(f.generations.readCharge('missing-0').status, 'reserved');
  assert.equal(
    f.db.prepare("SELECT status FROM generation_jobs WHERE id='missing-0'").get().status,
    'running',
  );
  await runtime.stop();
});

test('队列与恢复薄读有界只读，queue期限不冒充已运行请求的执行期限', async (t) => {
  const f = await setup(t);
  await f.accept('A', { samples: 4, queueExpiresAt: NOW + 50 });
  f.dispatch('A-0');
  await f.accept('B', { memberId: 'm2' });
  const before = databaseState(f.db);
  assert.deepEqual(f.generations.readQueuedGenerationIds(1), ['B-0']);
  assert.deepEqual(f.generations.readInterruptedGenerationIds('', 1), ['A-0']);
  assert.deepEqual(f.generations.readInterruptedGenerationIds('A-0', 1), []);
  assert.deepEqual(f.generations.readExpiredGenerationIds(NOW + 51), ['A-1', 'A-2', 'A-3']);
  assert.deepEqual(f.generations.readDueGenerationReviewIds(NOW), []);
  const state = f.generations.readGenerationExecutionState('A-0');
  assert.equal(Object.hasOwn(state, 'pst'), false);
  assert.equal(Object.hasOwn(state, 'request'), false);
  assert.ok(!JSON.stringify(state).includes('private generation prompt'));
  const account = f.generations.readBoundGenerationAccount();
  assert.equal(Object.hasOwn(account, 'pst'), false);
  for (const limit of [0, 101, NaN, 1.5]) {
    assert.throws(() => f.generations.readQueuedGenerationIds(limit), /page/);
    assert.throws(() => f.generations.readInterruptedGenerationIds('', limit), /page/);
    assert.throws(() => f.generations.readExpiredGenerationIds(NOW, limit), /page/);
    assert.throws(() => f.generations.readDueGenerationReviewIds(NOW, limit), /page/);
  }
  assert.equal(databaseState(f.db), before);
  assert.equal(f.calls, 0);
});

test('辅助操作由同一真实HTTP执行链发布原图与二进制编码，不走图片假结果', async (t) => {
  const binary = Buffer.from(Array.from({ length: 128 }, (_, i) => 128 + (i % 127))),
    seen = [];
  const f = await setup(t, (req, res, body) => {
    seen.push(req.url);
    assert.ok(body.image.length > 0);
    if (req.url === '/ai/encode-vibe') {
      res.writeHead(200, { 'content-type': 'application/octet-stream' });
      res.end(binary);
    } else {
      assert.equal(req.url, '/ai/upscale');
      res.writeHead(200, { 'content-type': 'image/webp' });
      res.end(image);
    }
  });
  const payloadBudget = createGenerationPayloadBudget(() => f.inputs.activePayloadBytes());
  const admit = createRelationalMemberGenerationAdmission({
    members: f.members,
    inputs: f.inputs,
    generations: f.generations,
    payloadBudget,
    env: {},
    now: () => f.clock,
  });
  const requests = [];
  for (const operation of ['upscale', 'encode-vibe']) {
    const body = {
      operation,
      image: image.toString('base64'),
      ...(operation === 'encode-vibe'
        ? { model: 'nai-diffusion-4-5-full', information_extracted: 0.5 }
        : {}),
    };
    const confirmation = {
      requestFingerprint: normalizeMemberGeneration(body).requestFingerprint,
      versions: f.generations.readMemberGenerationCapabilities('m1').versions,
      maxPerJob: { v5: '100', anlasFixed: '4000', anlasPurchased: '4000' },
      maxTotal: { v5: '100', anlasFixed: '4000', anlasPurchased: '4000' },
    };
    const accepted = await admit({
      credential: { apiKey: keys.m1 },
      request: {
        headers: { 'idempotency-key': operation },
        rawHeaders: ['Idempotency-Key', operation],
      },
      body: { ...body, confirmation },
    });
    requests.push(accepted.receipt);
  }
  await f.ready();
  const runtime = f.runtime();
  await runtime.start();
  runtime.tick();
  await waitFor(
    () =>
      f.db.prepare('SELECT COUNT(*) AS n FROM artifacts').get().n === 1 &&
      runtime.controls.size === 0,
  );
  f.clock += 10001;
  runtime.tick();
  await waitFor(() => runtime.controls.size === 0 && f.calls === 2);
  assert.deepEqual(seen, ['/ai/upscale', '/ai/encode-vibe']);
  for (const [index, { jobIds }] of requests.entries()) {
    const row = f.db
      .prepare('SELECT artifact_id AS id,status FROM generation_jobs WHERE id=?')
      .get(jobIds[0]);
    assert.equal(row.status, 'done');
    assert.equal(f.generations.readCharge(jobIds[0]).status, 'settled');
    const content = await readRelationalArtifactContent(f.db, row.id, { dataDir: f.dir });
    assert.deepEqual(content.buffer, index === 0 ? image : binary);
    assert.equal(content.descriptor.operation, index === 0 ? 'upscale' : 'encode-vibe');
  }
  await runtime.stop();
  replayAll(t, f);
});

test('真实成员HTTP与执行器共用Map/预算/事件，退款仍占物理输入且SSE领取原本地字节', async (t) => {
  const f = await setup(t, undefined, { seconds: 0 });
  const session = await createMemberSession(
    f.members,
    await authenticateMemberApiKey(f.members, keys.m1),
    {},
    NOW,
  );
  await f.ready();
  const controls = new Map(),
    runtime = f.runtime({ controls }),
    payloadBudget = createGenerationPayloadBudget(
      () => f.inputs.activePayloadBytes(controls.values()),
      { request: 32000, active: 20000 },
    );
  let commits = 0;
  f.events.on('commit', (...args) => {
    assert.equal(f.db.inTransaction, false);
    assert.deepEqual(args, []);
    commits++;
  });
  const handle = createRelationalMemberGenerationHttp({
    sqlite: f.db,
    payloadBudget,
    events: f.events,
    previews: runtime.previews,
    replicas: { dataDir: f.dir },
    scheduleQueueDrain: runtime.schedule,
    env: f.env,
    now: () => f.clock,
  });
  const memberServer = createServer(async (req, res) => {
    if (!(await handle(req, res, new URL(req.url, `http://${req.headers.host}`)))) {
      res.writeHead(404);
      res.end();
    }
  });
  await new Promise((resolve) => memberServer.listen(0, '127.0.0.1', resolve));
  const origin = `http://127.0.0.1:${memberServer.address().port}`,
    streamAbort = new AbortController();
  f.beforeStop(async () => {
    streamAbort.abort();
    closeMemberGenerationStreams(f.db);
    memberServer.closeAllConnections();
    await new Promise((resolve) => memberServer.close(resolve));
  });
  const memberFetch = (route, body, key, extra = {}) =>
    fetch(origin + route, {
      method: body ? 'POST' : 'GET',
      headers: {
        cookie: `nai_member_session=${session.token}`,
        origin,
        ...(body ? { 'content-type': 'application/json' } : {}),
        ...(key ? { 'idempotency-key': key } : {}),
      },
      ...(body ? { body: JSON.stringify(body) } : {}),
      ...extra,
    });
  const first = {
      operation: 'generate',
      model: 'nai-diffusion-4-5-full',
      input: 'a'.repeat(6000),
      parameters: { width: 128, height: 128, steps: 30, seed: -1 },
    },
    second = { ...first, input: 'b'.repeat(6000) };
  async function confirmed(body) {
    const result = await memberFetch('/api/member/generation/quote', body);
    assert.equal(result.status, 200);
    const quote = await result.json();
    return {
      ...body,
      confirmation: {
        requestFingerprint: quote.requestFingerprint,
        versions: quote.versions,
        maxPerJob: quote.charges.perJob[0].resources,
        maxTotal: quote.charges.total,
      },
    };
  }
  const a = await confirmed(first),
    b = await confirmed(second),
    originalRename = fsPromises.rename;
  let release,
    entered = false;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  fsPromises.rename = async (...args) => {
    if (String(args[0]).startsWith(f.dir) && String(args[0]).endsWith('.part')) {
      entered = true;
      await gate;
    }
    return originalRename(...args);
  };
  syncBuiltinESMExports();
  const restore = () => {
    fsPromises.rename = originalRename;
    syncBuiltinESMExports();
    release();
  };
  f.beforeStop(restore);
  await runtime.start();
  const accepted = await memberFetch('/api/member/generation/requests', a, 'http-physical-a');
  assert.equal(accepted.status, 202);
  const receipt = await accepted.json();
  await waitFor(() => entered);
  assert.equal(f.calls, 1);
  assert.equal(runtime.controls, controls);
  assert.equal(controls.size, 1);
  assert.equal(payloadBudget.activeBytes(), runtime.activePayloadBytes());
  assert.ok(payloadBudget.activeBytes() > 6000 && payloadBudget.activeBytes() < 20000);
  const sse = await memberFetch(`/api/member/requests/${receipt.requestId}/events`, null, null, {
    signal: streamAbort.signal,
  });
  assert.equal(sse.status, 200);
  let streamText = '';
  const reader = sse.body.getReader();
  const reading = (async () => {
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }
        streamText += Buffer.from(value).toString('utf8');
      }
    } catch (error) {
      if (!streamAbort.signal.aborted) {
        throw error;
      }
    }
  })();
  await waitFor(() => streamText.includes('"status":"running"'));
  controls.get(receipt.jobIds[0]).controller.abort(new Error('Synthetic physical timeout.'));
  runtime.tick();
  await waitFor(() => streamText.includes('"status":"failed"'));
  assert.equal(f.generations.readCharge(receipt.jobIds[0]).status, 'refunded');
  assert.equal(f.inputs.activePayloadBytes(), 0);
  assert.ok(payloadBudget.activeBytes() > 6000);
  const before = databaseState(f.db),
    priorCommits = commits;
  const blocked = await memberFetch('/api/member/generation/requests', b, 'http-physical-b');
  assert.equal(blocked.status, 429);
  assert.equal((await blocked.json()).code, 'queue_full');
  assert.equal(databaseState(f.db), before);
  assert.equal(commits, priorCommits);
  restore();
  await waitFor(() => controls.size === 0 && payloadBudget.activeBytes() === 0);
  await reading;
  assert.ok(streamText.includes('event: complete\n'));
  const late = await memberFetch(`/api/member/requests/${receipt.requestId}`);
  assert.equal(late.status, 200);
  assert.equal((await late.json()).jobs[0].lateResultAvailable, true);
  const content = await memberFetch(`/api/member/jobs/${receipt.jobIds[0]}/content`);
  assert.equal(content.status, 200);
  assert.deepEqual(Buffer.from(await content.arrayBuffer()), image);
  f.clock += 10001;
  const next = await memberFetch('/api/member/generation/requests', b, 'http-physical-b');
  assert.equal(next.status, 202);
  const nextReceipt = await next.json();
  await waitFor(
    () => f.generations.readGenerationExecutionState(nextReceipt.jobIds[0]).status === 'done',
  );
  assert.equal(f.calls, 2);
  assert.equal(f.generations.readCharge(nextReceipt.jobIds[0]).status, 'settled');
  assert.ok(commits >= 8);
  await runtime.stop();
  await reading;
  replayAll(t, f);
});
