import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { EventEmitter } from 'node:events';
import { createServer } from 'node:http';
import path from 'node:path';
import { readFile } from 'node:fs/promises';
import { initializeFixtureConfiguration } from '../helpers/relational-configuration.mjs';
import {
  fixture,
  meta,
  NOW,
  image,
  databaseState,
  installReady,
} from '../helpers/relational-generation.mjs';
import { domainDigest } from '../../server/events/domain-events.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { parseRelationalFact } from '../../server/events/relational-events.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalReportStore } from '../../server/storage/relational-report-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import {
  relationalPrivateTables,
  relationalSnapshotSha256,
} from '../../server/storage/relational-private-restore.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { describeStoredResult, publishStoredResult } from '../../server/storage/result-files.ts';
import { createRelationalQueueRuntime } from '../../server/generation/relational-queue-runtime.ts';
import { accountQuotaResult } from '../../server/quota/account-refresh.ts';

const zero = { v5: '0', anlasFixed: '0', anlasPurchased: '0' };
const unknown = { v5: null, anlasFixed: null, anlasPurchased: null };
const response = {
  dispatchState: 'unknown',
  upstreamStartedAt: null,
  upstreamHeadersAt: null,
  upstreamCompletedAt: null,
  upstreamHttpStatus: null,
};
async function source(t) {
  const f = await fixture(t);
  f.accept('definition');
  f.fail('definition-0', { aborted: true });
  return f;
}
function open(f, options = {}) {
  return f.db.transaction(() => {
    const name = options.name ?? 'old',
      requestId = `${name}-request`,
      jobId = `${name}-job`;
    const inputs = new RelationalInputStore(f.db);
    const definition = f.db
      .prepare("SELECT * FROM generation_requests WHERE id='definition'")
      .get();
    const original = f.db
      .prepare('SELECT * FROM generation_inputs WHERE id=?')
      .get(definition.input_id);
    const origin = (kind) => ({
      sourceId: 'synthetic-opening',
      originalId: `${name}-original-${kind}`,
    });
    const event = (eventType, payload, kind = 'request') => ({
      eventId: `${name}-${eventType}`,
      eventType,
      payload,
      streamId:
        kind === 'request'
          ? `request:${requestId}`
          : `${kind}:${kind === 'input' ? payload.inputId : 'a'}`,
      memberId: 'm1',
      accountId: kind === 'account' || eventType === 'OpeningAttemptRecorded' ? 'a' : null,
      occurredAt: NOW + 10,
      policyVersion: 'original-policy',
      pricingVersion: 'original-price',
    });
    const registered = event(
      'OpeningInputRecorded',
      {
        ...inputs.stageOpeningInput({
          inputId: `${name}-input`,
          memberId: 'm1',
          completeness: 'complete',
          body: JSON.parse(original.body),
          logicalBytes: original.logical_bytes,
          assets: [],
        }),
        origin: origin('input'),
      },
      'input',
    );
    const request = event('OpeningRequestRecorded', {
      requestId,
      memberId: 'm1',
      inputId: `${name}-input`,
      contextId: definition.context_id,
      source: 'historical-source',
      operation: 'generate',
      model: definition.model,
      sampleCount: 1,
      seedMode: 'fixed',
      confirmation: null,
      acceptedAt: 0,
      retiredAt: null,
      origin: origin('request'),
    });
    const job = event('OpeningJobRecorded', {
      jobId,
      requestId,
      createdAt: null,
      sampleIndex: 0,
      actualSeed: 42,
      status: 'failed',
      queueSeq: null,
      queueExpiresAt: null,
      deadlineAt: null,
      admissionEpoch: 0,
      artifactId: null,
      cacheKey: null,
      completedAt: 0,
      errorCode: 'old_failure',
      errorStatus: 504,
      cancellationReason: null,
      lateResultAt: null,
      freeSucceededAt: null,
      freeWarningThreshold: null,
      retiredAt: null,
      origin: origin('job'),
      ...(options.job ?? {}),
    });
    const descriptor = describeStoredResult(`${name}-artifact`, 'generate', {
      mimeType: 'image/webp',
      buffer: image,
    });
    const attempt = event('OpeningAttemptRecorded', {
      attemptId: `${name}-attempt`,
      jobId,
      accountId: 'a',
      credentialEpoch: null,
      connectionSourceEventId: null,
      authorizedAt: null,
      response,
      candidateMode: 'unknown',
      upstreamCandidates: [],
      actualConsumption: null,
      lossCoverageRemaining: unknown,
      reviewStatus: 'pending',
      firstUnknownAt: null,
      reviewDeadlineAt: null,
      reviewClosedAt: null,
      originalLossEventId: null,
      originalClosureEventId: null,
      preparedArtifactId: null,
      preparedFile: null,
      preparedMimeType: null,
      preparedByteLength: null,
      preparedSha256: null,
      origin: origin('attempt'),
      ...(options.prepared
        ? {
            preparedArtifactId: descriptor.imageId,
            preparedFile: descriptor.file,
            preparedMimeType: descriptor.mimeType,
            preparedByteLength: descriptor.byteLength,
            preparedSha256: descriptor.sha256,
          }
        : {}),
      ...(options.attempt ?? {}),
    });
    const charge = event(
      'OpeningChargeRecorded',
      {
        originalJobId: jobId,
        originalMemberId: 'm1',
        originalAccountId: 'a',
        jobId,
        walletAccountId: 'a',
        walletMemberId: 'm1',
        attemptId: attempt.payload.attemptId,
        status: job.payload.status === 'running' ? 'reserved' : 'refunded',
        resolution: 'live',
        originRef: `${name}-archive-charge`,
        resources: { ...zero, v5: '1' },
        billingMode: 'v5',
        estimated: true,
        v5Equivalent: '0',
        sharedV5Reserved: '0',
        configVersion: 1,
        reservedAt: 0,
        terminalAt: job.payload.status === 'running' ? null : 0,
        reservationEventId: `${name}-OpeningChargeRecorded`,
        terminalEventId: job.payload.status === 'running' ? null : `${name}-OpeningChargeRecorded`,
        origin: origin('charge'),
        ...(options.charge ?? {}),
      },
      'account',
    );
    let events = [registered, request, job, attempt, ...(options.noCharge ? [] : [charge])];
    if (options.mutate) {
      events = options.mutate(events) ?? events;
    }
    const { policyVersion: _p, pricingVersion: _v, ...metadata } = meta(`opening-${name}`);
    const command = {
      ...metadata,
      fingerprint: domainDigest(events),
      receipt: { imported: name },
      events,
      expectedVersions: Object.fromEntries(
        events.map((e) => [
          e.streamId,
          f.db
            .prepare('SELECT CAST(stream_version AS TEXT) AS v FROM stream_heads WHERE stream_id=?')
            .get(e.streamId)?.v ?? '0',
        ]),
      ),
    };
    const result = new RelationalCommitReplayer(f.db).appendCommit(command, inputs);
    return { ...result, descriptor, jobId, attemptId: attempt.payload.attemptId };
  })();
}
function publicState(db) {
  const privateNames = new Set(relationalPrivateTables.map(([name]) => name));
  return db
    .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
    .all()
    .filter(({ name }) => !privateNames.has(name) && name !== 'projection_state')
    .map(({ name }) => [
      name,
      db
        .prepare(`SELECT * FROM "${name}" ORDER BY rowid`)
        .safeIntegers()
        .all()
        .map((row) => (name === 'members' ? { ...row, auth_epoch: null } : row)),
    ]);
}
function rebuild(t, source) {
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replay = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(source);
  const fetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = () => {
    calls++;
    throw new Error('complete replay cannot fetch');
  };
  try {
    for (const { seq } of source
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all()) {
      target.transaction(() => replay.replayCommit(readRelationalCommit(source, seq), inputs))();
    }
    const before = databaseState(target),
      changes = target.prepare('SELECT total_changes() AS n').get().n;
    for (const { seq } of source
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all()) {
      target.transaction(() => replay.replayCommit(readRelationalCommit(source, seq), inputs))();
    }
    assert.equal(databaseState(target), before);
    assert.equal(target.prepare('SELECT total_changes() AS n').get().n, changes);
    assert.equal(calls, 0);
  } finally {
    globalThis.fetch = fetch;
  }
  assert.deepEqual(publicState(target), publicState(source));
  for (const [table] of relationalPrivateTables) {
    assert.equal(target.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0);
  }
  assert.equal(
    target.prepare('SELECT COUNT(*) AS n FROM members WHERE auth_epoch IS NOT NULL').get().n,
    0,
  );
  return target;
}

test('OpeningAttempt preserves known review states, original opaque identities, unknown dates and independent zero/NULL budgets', async (t) => {
  const f = await source(t);
  for (const [index, status] of ['pending', 'estimated_closed', 'result_recovered'].entries()) {
    const coverage = { ...unknown, v5: '0', anlasFixed: index === 1 ? '50' : null };
    const result = open(f, {
      name: `history-${index}`,
      attempt: {
        reviewStatus: status,
        originalLossEventId: 'opaque-old-loss',
        originalClosureEventId: status === 'pending' ? null : 'opaque-old-close',
        actualConsumption: { ...zero, anlasFixed: '100' },
        lossCoverageRemaining: coverage,
      },
    });
    const row = f.db.prepare('SELECT * FROM generation_attempts WHERE id=?').get(result.attemptId);
    assert.equal(row.review_status, status);
    assert.equal(row.loss_event_id, `history-${index}-OpeningAttemptRecorded`);
    assert.equal(row.closure_event_id, status === 'pending' ? null : row.loss_event_id);
    assert.equal(row.first_unknown_at, null);
    assert.equal(row.review_deadline_at, null);
    assert.equal(row.review_closed_at, null);
    assert.equal(
      f.db
        .prepare(
          "SELECT COUNT(*) AS n FROM domain_events WHERE event_id IN ('opaque-old-loss','opaque-old-close')",
        )
        .get().n,
      0,
    );
    const saved = readRelationalCommit(f.db, result.commitSeq).events.find(
      (e) => e.eventType === 'OpeningAttemptRecorded',
    ).payload;
    assert.deepEqual(saved.lossCoverageRemaining, coverage);
    assert.deepEqual(saved.actualConsumption, { ...zero, anlasFixed: '100' });
  }
  assert.deepEqual(f.generations.readDueGenerationReviewIds(NOW + 86400000), []);
  const report = new RelationalReportStore(f.db).readFailureAccounting(null, NOW + 20);
  assert.equal(report.risk.unknownTimeAttempts, 3);
  assert.equal(report.events.filter((e) => e.type === 'opening_attempt_recorded').length, 3);
  assert.deepEqual(report.events[0].lossCoverageRemaining, {
    v5: 0,
    anlasFixed: null,
    anlasPurchased: null,
  });
});

test('Opening running requires one actual Job→Attempt→Charge closure in original order and exact roles', async (t) => {
  const f = await source(t);
  const running = {
    job: { status: 'running', completedAt: null, errorCode: null, errorStatus: null },
    attempt: { reviewStatus: null },
  };
  const changes = [
    (e) => e.filter((x) => x.eventType !== 'OpeningAttemptRecorded'),
    (e) => e.filter((x) => x.eventType !== 'OpeningChargeRecorded'),
    (e) => {
      [e[2], e[3]] = [e[3], e[2]];
    },
    (e) => {
      [e[3], e[4]] = [e[4], e[3]];
    },
    (e) => {
      e[4].payload.attemptId = 'other-attempt';
    },
    (e) => {
      e[3].memberId = 'm2';
    },
    (e) => {
      e[4].payload.walletMemberId = 'm2';
    },
    (e) => {
      e[4].payload.reservationEventId = 'old-opaque-id';
    },
    (e) => {
      e[3].payload.candidateMode = 'known';
    },
    (e) => {
      e.push(structuredClone(e[3]));
      e.at(-1).eventId += '-duplicate';
    },
  ];
  for (const [index, mutate] of changes.entries()) {
    const before = databaseState(f.db);
    f.db.transaction(() =>
      assert.throws(() => open(f, { ...running, name: `bad-${index}`, mutate })),
    )();
    assert.equal(databaseState(f.db), before);
  }
  const before = f.generations.readWallet('a', 'm1');
  const result = open(f, { ...running, name: 'running' });
  assert.deepEqual(f.generations.readWallet('a', 'm1'), before);
  assert.equal(f.generations.readCharge(result.jobId).status, 'reserved');
  assert.equal(
    f.generations.readGenerationExecutionState(result.jobId).eventId,
    'running-OpeningAttemptRecorded',
  );
});

test('never-dispatched pending/queued Opening children cannot own attempts; done original attempts stay valid', async (t) => {
  const f = await source(t);
  for (const status of ['pending', 'queued']) {
    const before = databaseState(f.db);
    f.db.transaction(() =>
      assert.throws(
        () =>
          open(f, {
            name: `unstarted-${status}`,
            noCharge: true,
            job: {
              status,
              queueSeq: status === 'queued' ? '2' : null,
              completedAt: null,
              errorCode: null,
              errorStatus: null,
            },
            attempt: { reviewStatus: null, actualConsumption: { ...zero, anlasFixed: '100' } },
          }),
        /未派发开账任务不能已有尝试/,
      ),
    )();
    assert.equal(databaseState(f.db), before);
  }
  const descriptor = describeStoredResult('done-artifact', 'generate', {
    mimeType: 'image/webp',
    buffer: image,
  });
  const result = open(f, {
    name: 'done',
    job: {
      status: 'done',
      artifactId: descriptor.imageId,
      completedAt: 0,
      errorCode: null,
      errorStatus: null,
    },
    attempt: { reviewStatus: null },
    charge: { status: 'settled' },
    mutate: (events) => {
      events.push({
        eventId: 'done-OpeningArtifactRecorded',
        eventType: 'OpeningArtifactRecorded',
        streamId: 'artifact:done-artifact',
        memberId: 'm1',
        accountId: 'a',
        occurredAt: NOW + 10,
        policyVersion: 'original-policy',
        pricingVersion: 'original-price',
        payload: {
          artifactId: descriptor.imageId,
          memberId: 'm1',
          inputId: 'done-input',
          originalAttemptId: 'done-attempt',
          operation: 'generate',
          mimeType: descriptor.mimeType,
          byteLength: descriptor.byteLength,
          sha256: descriptor.sha256,
          publishedAt: 0,
          retiredAt: null,
          origin: { sourceId: 'synthetic-opening', originalId: 'done-original-artifact' },
        },
      });
    },
  });
  assert.equal(f.generations.readCharge(result.jobId).status, 'settled');
  assert.equal(
    f.db
      .prepare('SELECT original_attempt_id AS attempt FROM artifacts WHERE id=?')
      .get(descriptor.imageId).attempt,
    result.attemptId,
  );
  rebuild(t, f.db);
});

test('known not_sent cannot own an artifact or a recovered result; identical unknown history stays valid', async (t) => {
  const f = await source(t);
  for (const role of ['done-artifact', 'failed-artifact', 'result-recovered']) {
    for (const dispatchState of ['not_sent', 'unknown']) {
      const name = `${role}-${dispatchState}`,
        descriptor = describeStoredResult(`${name}-artifact`, 'generate', {
          mimeType: 'image/webp',
          buffer: image,
        });
      const before = databaseState(f.db);
      let job = {};
      if (role === 'done-artifact') {
        job = {
          status: 'done',
          artifactId: descriptor.imageId,
          errorCode: null,
          errorStatus: null,
        };
      } else if (role === 'failed-artifact') {
        job = { artifactId: descriptor.imageId };
      }
      const importRole = () =>
        open(f, {
          name,
          job,
          attempt: {
            reviewStatus: role === 'result-recovered' ? 'result_recovered' : null,
            response: { ...response, dispatchState },
          },
          charge: role === 'done-artifact' ? { status: 'settled' } : {},
          mutate: (events) => {
            if (role !== 'result-recovered') {
              events.push({
                eventId: `${name}-OpeningArtifactRecorded`,
                eventType: 'OpeningArtifactRecorded',
                streamId: `artifact:${descriptor.imageId}`,
                memberId: 'm1',
                accountId: 'a',
                occurredAt: NOW + 10,
                policyVersion: 'original-policy',
                pricingVersion: 'original-price',
                payload: {
                  artifactId: descriptor.imageId,
                  memberId: 'm1',
                  inputId: `${name}-input`,
                  originalAttemptId: `${name}-attempt`,
                  operation: 'generate',
                  mimeType: descriptor.mimeType,
                  byteLength: descriptor.byteLength,
                  sha256: descriptor.sha256,
                  publishedAt: 0,
                  retiredAt: null,
                  origin: {
                    sourceId: 'synthetic-opening',
                    originalId: `${name}-original-artifact`,
                  },
                },
              });
            }
          },
        });
      if (dispatchState === 'not_sent') {
        f.db.transaction(() => assert.throws(importRole, /未发送|原尝试\/任务\/输入角色不符/))();
        assert.equal(databaseState(f.db), before);
      } else {
        const result = importRole();
        assert.equal(
          f.db
            .prepare('SELECT dispatch_state AS state FROM generation_attempts WHERE id=?')
            .get(result.attemptId).state,
          'unknown',
        );
      }
    }
  }
  rebuild(t, f.db);
});

test('ready and preparation reader reject unexecutable archived prepared sources without changing source, target or Local bytes', async (t) => {
  for (const kind of [
    'missing-charge',
    'already-recovered',
    'archived-charge',
    'not-sent-evidence',
  ]) {
    const f = await source(t),
      result = open(f, {
        prepared: true,
        noCharge: kind === 'missing-charge',
        attempt: { reviewStatus: kind === 'already-recovered' ? 'result_recovered' : 'pending' },
        charge: kind === 'archived-charge' ? { resolution: 'archived' } : {},
      });
    if (kind === 'not-sent-evidence') {
      f.db.transaction(() =>
        f.generations.recordGenerationResponse(meta('original-not-sent-evidence'), result.jobId, {
          ...response,
          dispatchState: 'not_sent',
        }),
      )();
    }
    await publishStoredResult(f.dir, result.descriptor, image);
    initializeFixtureConfiguration(f.db);
    const selected = path.join(f.dir, 'unexecutable-source.sqlite');
    await f.db.backup(selected);
    const seal = new Database(selected);
    seal.pragma('journal_mode=DELETE');
    seal.close();
    const sha = relationalSnapshotSha256(selected),
      before = databaseState(f.db),
      targetBytes = f.db.serialize();
    const reason = /任务或收费已经终结|原核对状态不能再次恢复结果|实际任务、尝试与原预扣不一致/;
    assert.throws(() => f.generations.readGenerationResultPreparation(result.jobId), reason);
    f.db.transaction(() =>
      assert.throws(
        () =>
          f.generations.completeGeneration(meta(`reject-${kind}`), result.jobId, result.descriptor),
        reason,
      ),
    )();
    await assert.rejects(
      verifyAndInstallRelationalStorage(f.db, {
        sourcePath: selected,
        sourceSha256: sha,
        historyId: 'generation-history',
        finalCommitSeq: f.db
          .prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits')
          .get().seq,
        replicas: { dataDir: f.dir },
      }),
      reason,
    );
    assert.equal(f.db.prepare('SELECT status FROM projection_state').get().status, 'building');
    assert.equal(databaseState(f.db), before);
    assert.ok(f.db.serialize().equals(targetBytes));
    assert.equal(relationalSnapshotSha256(selected), sha);
    assert.deepEqual(await readFile(path.join(f.dir, result.descriptor.file)), image);
  }
});

test('ready shares the real running prepared completion closure and original coordinator settles only its original charge', async (t) => {
  const f = await source(t),
    result = open(f, {
      prepared: true,
      job: { status: 'running', completedAt: null, errorCode: null, errorStatus: null },
      attempt: { reviewStatus: null },
    });
  await publishStoredResult(f.dir, result.descriptor, image);
  const before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() AS n').get().n;
  assert.deepEqual(
    f.generations.readGenerationResultPreparation(result.jobId).descriptor,
    result.descriptor,
  );
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
  await installReady(f);
  const wallet = f.generations.readWallet('a', 'm1');
  const run = createRelationalQueueRuntime({
    sqlite: f.db,
    dataDir: f.dir,
    generations: f.generations,
    inputs: f.inputs,
    controls: new Map(),
    events: new EventEmitter(),
    now: () => NOW + 20,
    env: {},
  });
  const fetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = () => {
    calls++;
    assert.fail('prepared recovery must not resend');
  };
  try {
    await run.start();
    await run.stop();
    assert.equal(f.generations.readCharge(result.jobId).status, 'settled');
    assert.deepEqual(f.generations.readWallet('a', 'm1'), wallet);
    assert.equal(
      f.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get(result.jobId).status,
      'done',
    );
    assert.equal(calls, 0);
    rebuild(t, f.db);
  } finally {
    await run.stop();
    globalThis.fetch = fetch;
  }
});

test('OpeningAttempt SQL failure is atomic under an outer catch; retry and full original repeat write once', async (t) => {
  const f = await source(t),
    before = databaseState(f.db);
  const prepare = f.db.prepare;
  f.db.prepare = function (sql) {
    const statement = prepare.call(this, sql);
    if (!sql.includes('INSERT INTO generation_attempts')) {
      return statement;
    }
    const run = statement.run;
    statement.run = function (...args) {
      run.apply(this, args);
      throw new Error('injected opening attempt failure');
    };
    return statement;
  };
  try {
    f.db.transaction(() => assert.throws(() => open(f), /injected opening/))();
  } finally {
    f.db.prepare = prepare;
  }
  assert.equal(databaseState(f.db), before);
  const result = open(f),
    after = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() AS n').get().n;
  f.db.transaction(() =>
    new RelationalCommitReplayer(f.db).replayCommit(
      readRelationalCommit(f.db, result.commitSeq),
      f.inputs,
    ),
  )();
  assert.equal(databaseState(f.db), after);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
  rebuild(t, f.db);
});

test('known actual consumption survives deadline close, conflicting non-NULL claims roll back, and unknown candidates retain risk', async (t) => {
  const f = await source(t),
    consumed = { ...zero, anlasFixed: '100' };
  const result = open(f, {
    attempt: { actualConsumption: consumed, firstUnknownAt: NOW, reviewDeadlineAt: NOW + 1 },
  });
  const before = databaseState(f.db),
    { policyVersion, pricingVersion, ...metadata } = meta('conflicting-cost');
  f.db.transaction(() =>
    assert.throws(
      () =>
        new RelationalCommitReplayer(f.db).appendCommit({
          ...metadata,
          receipt: null,
          expectedVersions: {
            'request:old-request': f.db
              .prepare(
                "SELECT CAST(stream_version AS TEXT) AS v FROM stream_heads WHERE stream_id='request:old-request'",
              )
              .get().v,
          },
          events: [
            {
              eventType: 'ReviewClosed',
              streamId: 'request:old-request',
              accountId: 'a',
              memberId: 'm1',
              occurredAt: NOW + 2,
              policyVersion,
              pricingVersion,
              payload: {
                attemptId: result.attemptId,
                status: 'estimated_closed',
                lossEventId: 'old-OpeningAttemptRecorded',
                reason: 'bad',
                actualConsumption: zero,
              },
            },
          ],
        }),
      /已知原上游消费/,
    ),
  )();
  assert.equal(databaseState(f.db), before);
  f.db.transaction(() =>
    f.generations.closeGenerationReview(meta('close-opening', NOW + 1), result.jobId),
  )();
  assert.deepEqual(
    JSON.parse(
      f.db
        .prepare('SELECT actual_consumption FROM generation_attempts WHERE id=?')
        .get(result.attemptId).actual_consumption,
    ),
    consumed,
  );
  const report = new RelationalReportStore(f.db).readFailureAccounting(null, NOW + 20);
  assert.equal(report.risk.last24h.attempts, 1);
  assert.equal(report.risk.last24h.unknownAmounts, 1);
});

test('two costs 100 with aggregate uncovered 150 never infer per-loss budgets or become coverage sources', async (t) => {
  const f = await source(t);
  for (const name of ['one', 'two']) {
    open(f, { name, attempt: { actualConsumption: { ...zero, anlasFixed: '100' } } });
  }
  const quotas = new RelationalQuotaStore(f.db, { OWNER_USER_ID: 'm1' });
  const ticket = quotas.beginQuotaObservation('a', NOW + 15);
  f.db.transaction(() =>
    quotas.recordQuotaObservation(
      meta('aggregate-150', NOW + 16),
      ticket,
      accountQuotaResult(
        'a',
        {
          tier: 3,
          subscriptionActive: true,
          subscriptionExpiresAt: null,
          points: 0,
          fixed: '7850',
          purchased: '0',
          v5UsagePercent: '80',
          v5UsageTimeUntilNextPercent: 600,
        },
        new Date(NOW + 15).toISOString(),
      ),
    ),
  )();
  assert.equal(
    new RelationalReportStore(f.db).readFailureAccounting(null, NOW + 20).operating.unfunded
      .anlasFixed,
    150,
  );
  const before = databaseState(f.db);
  for (const name of ['one', 'two']) {
    const event = f.db
      .prepare('SELECT payload FROM domain_events WHERE event_id=?')
      .get(`${name}-OpeningAttemptRecorded`);
    assert.deepEqual(JSON.parse(event.payload).lossCoverageRemaining, unknown);
    f.db.transaction(() =>
      assert.throws(
        () =>
          quotas.manageQuotaFunding(meta(`cover-${name}`, NOW + 20, 'm1'), 'a', {
            action: 'operating-cover-loss',
            receipt: `cover-${name}`,
            resource: 'anlasFixed',
            amount: 1,
            lossEventId: `${name}-OpeningAttemptRecorded`,
          }),
        /损耗记录不存在|资源不符/,
      ),
    )();
  }
  assert.equal(databaseState(f.db), before);
});

test('legal ready reopen uses original coordinator to refund imported running exactly once, without resending unknown risk', async (t) => {
  const cleanup = [],
    f = await source({ after: (fn) => cleanup.push(fn) });
  let db, upstream, first;
  t.after(async () => {
    await first?.stop();
    if (upstream) {
      await new Promise((resolve) => upstream.close(resolve));
    }
    db?.close();
    for (const run of cleanup.reverse()) {
      await run();
    }
  });
  const result = open(f, {
    job: { status: 'running', completedAt: null, errorCode: null, errorStatus: null },
    attempt: { reviewStatus: null },
  });
  const wallet = f.generations.readWallet('a', 'm1');
  await installReady(f);
  f.db.close();
  db = new Database(path.join(f.dir, 'library.sqlite'));
  db.pragma('foreign_keys=ON');
  let calls = 0;
  upstream = createServer((_req, res) => {
    calls++;
    res.writeHead(200, { 'content-type': 'image/webp' });
    res.end(image);
  });
  await new Promise((resolve) => upstream.listen(0, '127.0.0.1', resolve));
  const store = new RelationalGenerationStore(db),
    events = new EventEmitter(),
    controls = new Map();
  const runtime = () =>
    createRelationalQueueRuntime({
      sqlite: db,
      dataDir: f.dir,
      generations: store,
      inputs: new RelationalInputStore(db),
      controls,
      events,
      now: () => NOW + 20,
      env: {
        NOVELAI_API_URL: `http://127.0.0.1:${upstream.address().port}`,
        MOCK_WHEN_NO_ACCOUNT: 'false',
      },
    });
  first = runtime();
  await first.start();
  await first.stop();
  assert.equal(store.readCharge(result.jobId).status, 'refunded');
  assert.equal(Number(store.readWallet('a', 'm1').v5), Number(wallet.v5) + 1);
  assert.equal(
    db
      .prepare('SELECT review_status AS status FROM generation_attempts WHERE id=?')
      .get(result.attemptId).status,
    'pending',
  );
  const after = databaseState(db),
    second = runtime();
  await second.start();
  await second.stop();
  assert.equal(databaseState(db), after);
  assert.equal(calls, 0);
  rebuild(t, db);
});

test('legal ready original Local prepared snapshot recovers once and retains original known cost and refund', async (t) => {
  const f = await source(t),
    consumed = { ...zero, anlasFixed: '100' };
  const result = open(f, {
    prepared: true,
    attempt: {
      actualConsumption: consumed,
      reviewStatus: 'estimated_closed',
      originalClosureEventId: 'original-estimated-close',
      reviewClosedAt: 0,
    },
  });
  await publishStoredResult(f.dir, result.descriptor, image);
  await installReady(f);
  const wallet = f.generations.readWallet('a', 'm1');
  f.db.close();
  const db = new Database(path.join(f.dir, 'library.sqlite'));
  db.pragma('foreign_keys=ON');
  const store = new RelationalGenerationStore(db);
  let calls = 0;
  const fetch = globalThis.fetch;
  globalThis.fetch = () => {
    calls++;
    throw new Error('recovery cannot fetch');
  };
  const run = createRelationalQueueRuntime({
    sqlite: db,
    dataDir: f.dir,
    generations: store,
    inputs: new RelationalInputStore(db),
    controls: new Map(),
    events: new EventEmitter(),
    now: () => NOW + 20,
    env: {},
  });
  try {
    await run.start();
    await run.stop();
    const a = db.prepare('SELECT * FROM generation_attempts WHERE id=?').get(result.attemptId);
    assert.equal(a.review_status, 'result_recovered');
    assert.deepEqual(JSON.parse(a.actual_consumption), consumed);
    assert.equal(store.readCharge(result.jobId).status, 'refunded');
    assert.deepEqual(store.readWallet('a', 'm1'), wallet);
    const late = new RelationalReportStore(db)
      .readFailureAccounting(null, NOW + 20)
      .events.find((e) => e.type === 'generation_result_recovered');
    assert.equal(late.closureEventId, 'old-OpeningAttemptRecorded');
    assert.equal(calls, 0);
    rebuild(t, db);
  } finally {
    await run.stop();
    globalThis.fetch = fetch;
    db.close();
  }
});

test('OpeningAttempt required nullable coverage, original identities and pending closure fields reject fabricated combinations', () => {
  const base = {
    attemptId: 'a',
    jobId: 'j',
    accountId: 'u',
    credentialEpoch: null,
    connectionSourceEventId: null,
    authorizedAt: null,
    response,
    candidateMode: 'unknown',
    upstreamCandidates: [],
    actualConsumption: null,
    lossCoverageRemaining: unknown,
    reviewStatus: 'pending',
    firstUnknownAt: null,
    reviewDeadlineAt: null,
    reviewClosedAt: null,
    originalLossEventId: null,
    originalClosureEventId: null,
    preparedArtifactId: null,
    preparedFile: null,
    preparedMimeType: null,
    preparedByteLength: null,
    preparedSha256: null,
    origin: { sourceId: 's', originalId: 'a' },
  };
  assert.equal(
    parseRelationalFact({ eventType: 'OpeningAttemptRecorded', payload: base }).payload
      .reviewStatus,
    'pending',
  );
  for (const payload of [
    { ...base, lossCoverageRemaining: undefined },
    { ...base, lossCoverageRemaining: { ...unknown, v5: '-1' } },
    { ...base, reviewClosedAt: 0 },
    { ...base, originalClosureEventId: 'fake-close' },
  ]) {
    assert.throws(() => parseRelationalFact({ eventType: 'OpeningAttemptRecorded', payload }));
  }
});

test('Opening response credential effects require real original authorization and the exact current epoch; public replay restores no auth', async (t) => {
  for (const [
    name,
    credentialEpoch,
    authorizedAt,
    expected,
    connectionSourceEventId = 'opening-account',
    cooldownExpected = expected,
  ] of [
    ['known', 1, 0, true],
    ['unknown-authorization', 1, null, false],
    ['unknown-epoch', null, 0, false],
    ['different-epoch', 2, 0, false],
    ['unknown-connection', 1, 0, false, null, true],
  ]) {
    await t.test(name, async (t) => {
      const f = await source(t);
      const result = open(f, {
        name,
        job: { status: 'running', completedAt: null, errorCode: null, errorStatus: null },
        attempt: {
          reviewStatus: null,
          credentialEpoch,
          connectionSourceEventId,
          authorizedAt,
          response: {
            ...response,
            dispatchState: 'request_started',
            upstreamStartedAt: 0,
            upstreamCompletedAt: 0,
            upstreamHttpStatus: 401,
          },
        },
      });
      const before = f.db
        .prepare("SELECT pst FROM upstream_credentials WHERE account_id='a'")
        .get().pst;
      if (credentialEpoch === 1 && authorizedAt === null) {
        const state = databaseState(f.db),
          { policyVersion, pricingVersion, ...command } = meta('unproved-opening-cooldown');
        f.db.transaction(() =>
          assert.throws(
            () =>
              new RelationalCommitReplayer(f.db).appendCommit({
                ...command,
                receipt: null,
                expectedVersions: {
                  'account:a': f.db
                    .prepare(
                      "SELECT CAST(stream_version AS TEXT) AS v FROM stream_heads WHERE stream_id='account:a'",
                    )
                    .get().v,
                },
                events: [
                  {
                    eventType: 'CooldownChanged',
                    streamId: 'account:a',
                    memberId: 'm1',
                    accountId: 'a',
                    occurredAt: NOW + 20,
                    policyVersion,
                    pricingVersion,
                    payload: {
                      accountId: 'a',
                      attemptId: result.attemptId,
                      completionKind: 'ended',
                      cooldownUntil: NOW + 1000,
                      cooldownReason: 'minimum_gap',
                      ordinaryStrikes: 0,
                      capacityStrikes: 0,
                      capacityLimitedUntil: null,
                      capacityLastErrorAt: null,
                    },
                  },
                ],
              }),
            /开账冷却缺少原授权证明/,
          ),
        )();
        assert.equal(databaseState(f.db), state);
      }
      const failed = f.fail(result.jobId, { aborted: false });
      const claims = readRelationalCommit(f.db, failed.commitSeq).events.filter(
        (e) => e.eventType === 'QuotaStatusChanged',
      );
      assert.equal(claims.length, Number(expected));
      if (expected) {
        assert.equal(
          claims[0].payload.generationFailure.dispatchEventId,
          `${name}-OpeningAttemptRecorded`,
        );
      }
      const cooldowns = readRelationalCommit(f.db, failed.commitSeq).events.filter(
        (e) => e.eventType === 'CooldownChanged',
      );
      assert.equal(cooldowns.length, Number(cooldownExpected));
      assert.equal(
        f.db.prepare("SELECT pst FROM upstream_credentials WHERE account_id='a'").get().pst,
        before,
      );
      const target = rebuild(t, f.db);
      assert.equal(target.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
      assert.equal(
        target.prepare('SELECT COUNT(*) AS n FROM members WHERE auth_epoch IS NOT NULL').get().n,
        0,
      );
    });
  }
});

test('unknown original charge preserves archived pending liability but has no automatic deadline or recovery authority', async (t) => {
  const f = await source(t),
    result = open(f, {
      noCharge: true,
      attempt: { firstUnknownAt: NOW, reviewDeadlineAt: NOW + 1 },
    });
  const before = databaseState(f.db);
  assert.deepEqual(f.generations.readDueGenerationReviewIds(NOW + 2), []);
  assert.equal(f.generations.readNextGenerationWakeAt(), null);
  f.db.transaction(() =>
    assert.throws(
      () =>
        f.generations.closeGenerationReview(meta('missing-original-charge', NOW + 2), result.jobId),
      /原退款/,
    ),
  )();
  assert.equal(databaseState(f.db), before);
  const report = new RelationalReportStore(f.db).readFailureAccounting(null, NOW + 20);
  assert.equal(report.risk.last24h.attempts, 1);
  assert.equal(report.risk.last24h.unknownAmounts, 1);
});

test('missing or corrupt real Opening prepared bytes block the original coordinator before any dispatch', async (t) => {
  const { writeFile, unlink } = await import('node:fs/promises');
  for (const corrupt of [false, true]) {
    await t.test(String(corrupt), async (t) => {
      const f = await source(t),
        result = open(f, { prepared: true });
      await publishStoredResult(f.dir, result.descriptor, image);
      await installReady(f);
      if (corrupt) {
        await writeFile(path.join(f.dir, result.descriptor.file), Buffer.from('bad actual bytes'));
      } else {
        await unlink(path.join(f.dir, result.descriptor.file));
      }
      const before = databaseState(f.db);
      const run = createRelationalQueueRuntime({
        sqlite: f.db,
        dataDir: f.dir,
        generations: f.generations,
        inputs: f.inputs,
        controls: new Map(),
        events: new EventEmitter(),
        now: () => NOW + 20,
        env: {},
      });
      try {
        await assert.rejects(run.start(), /require recovery/);
      } finally {
        await run.stop();
      }
      assert.equal(databaseState(f.db), before);
    });
  }
});

test('a never-dispatched Opening queued job uses its own request metadata and executes once through localhost', async (t) => {
  const cleanup = [],
    f = await source({ after: (fn) => cleanup.push(fn) });
  let run, upstream;
  t.after(async () => {
    await run?.stop();
    if (upstream) {
      await new Promise((resolve) => upstream.close(resolve));
    }
    for (const close of cleanup.reverse()) {
      await close();
    }
  });
  const result = open(f, {
    job: {
      status: 'queued',
      queueSeq: '2',
      queueExpiresAt: NOW + 60000,
      deadlineAt: NOW + 60000,
      completedAt: null,
      errorCode: null,
      errorStatus: null,
    },
    mutate: (events) =>
      events.filter(
        (e) => !['OpeningAttemptRecorded', 'OpeningChargeRecorded'].includes(e.eventType),
      ),
  });
  assert.equal(
    f.generations.readGenerationExecutionState(result.jobId).eventId,
    'old-OpeningRequestRecorded',
  );
  await installReady(f);
  let calls = 0,
    body;
  upstream = createServer(async (req, res) => {
    let raw = '';
    for await (const chunk of req) {
      raw += chunk;
    }
    body = JSON.parse(raw);
    calls++;
    res.writeHead(200, { 'content-type': 'image/webp' });
    res.end(image);
  });
  await new Promise((resolve) => upstream.listen(0, '127.0.0.1', resolve));
  run = createRelationalQueueRuntime({
    sqlite: f.db,
    dataDir: f.dir,
    generations: f.generations,
    inputs: f.inputs,
    controls: new Map(),
    events: new EventEmitter(),
    now: () => NOW + 20,
    env: {
      NOVELAI_API_URL: `http://127.0.0.1:${upstream.address().port}`,
      NOVELAI_STREAM_IMAGE: 'false',
      MOCK_WHEN_NO_ACCOUNT: 'false',
    },
  });
  await run.start();
  const deadline = Date.now() + 5000;
  while (
    f.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get(result.jobId).status !==
    'done'
  ) {
    assert.ok(Date.now() < deadline, 'original queued runtime did not complete');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  await run.stop();
  assert.equal(calls, 1);
  assert.equal(body.parameters.seed, 42);
  assert.equal(f.generations.readCharge(result.jobId).status, 'settled');
  rebuild(t, f.db);
});
