import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { fixture, meta, keys, NOW, databaseState } from '../helpers/relational-generation.mjs';
import { initializeFixtureConfiguration } from '../helpers/relational-configuration.mjs';
import { domainDigest } from '../../server/events/domain-events.ts';
import { relationalFactStream } from '../../server/events/relational-fact-contracts.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalReportStore } from '../../server/storage/relational-report-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import {
  relationalPrivateStateEmpty,
  relationalSnapshotSha256,
} from '../../server/storage/relational-private-restore.ts';
import { authenticateMemberApiKey } from '../../server/members/authentication.ts';
import { errorDetailMessage } from '../../server/http/public-errors.ts';

const sha = (text) => createHash('sha256').update(text).digest('hex');
const origin = (id, sourceId = 'synthetic-opening') => ({ sourceId, originalId: id });
const detail = `  原诊断 ${'x'.repeat(5000)} \u0000 end  `;
function event(eventType, payload) {
  const kind = relationalFactStream(eventType);
  return {
    eventType,
    payload,
    streamId: `${kind}:${kind === 'input' ? payload.inputId : (payload.requestId ?? 'original-request')}`,
    memberId: 'm1',
    accountId: eventType === 'OpeningAttemptRecorded' ? 'a' : null,
    occurredAt: NOW + 10,
    policyVersion: 'original',
    pricingVersion: 'original',
  };
}
function open(f, options = {}) {
  return f.db.transaction(() => {
    const inputs = new RelationalInputStore(f.db),
      input = event('OpeningInputRecorded', {
        ...inputs.stageOpeningInput({
          inputId: 'original-input',
          memberId: 'm1',
          completeness: 'unavailable',
          body: null,
          logicalBytes: 0,
          assets: [],
        }),
        origin: origin('input'),
      }),
      request = event('OpeningRequestRecorded', {
        requestId: 'original-request',
        originalRequestId: null,
        memberId: 'm1',
        inputId: 'original-input',
        contextId: null,
        source: null,
        operation: null,
        model: null,
        sampleCount: null,
        seedMode: 'unknown',
        confirmation: null,
        acceptedAt: null,
        retiredAt: null,
        origin: origin('request'),
      }),
      job = event('OpeningJobRecorded', {
        jobId: 'original-job',
        requestId: 'original-request',
        diagnosticId: 'original-diagnostic',
        sourceOrder: null,
        displayCost: null,
        createdAt: 0,
        sampleIndex: null,
        actualSeed: null,
        status: 'failed',
        queueSeq: null,
        queueExpiresAt: null,
        deadlineAt: null,
        admissionEpoch: null,
        artifactId: null,
        cacheKey: null,
        completedAt: 0,
        errorCode: 'original failure',
        errorStatus: 500,
        cancellationReason: null,
        lateResultAt: null,
        freeSucceededAt: null,
        freeWarningThreshold: null,
        retiredAt: null,
        origin: origin('job'),
      }),
      diagnostic = event('OpeningDiagnosticRecorded', {
        diagnosticId: 'original-diagnostic',
        jobId: 'original-job',
        sha256: sha(options.detail ?? detail),
        byteLength: Buffer.byteLength(options.detail ?? detail),
        createdAt: options.createdAt ?? null,
        origin: origin('diagnostic'),
      }),
      attempt = event('OpeningAttemptRecorded', {
        attemptId: 'original-attempt',
        jobId: 'original-job',
        accountId: 'a',
        credentialEpoch: null,
        connectionSourceEventId: null,
        authorizedAt: null,
        response: {
          dispatchState: 'unknown',
          upstreamStartedAt: null,
          upstreamHeadersAt: null,
          upstreamCompletedAt: null,
          upstreamHttpStatus: null,
        },
        candidateMode: 'unknown',
        upstreamCandidates: [],
        actualConsumption: null,
        lossCoverageRemaining: { v5: null, anlasFixed: null, anlasPurchased: null },
        reviewStatus: null,
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
      });
    let events = [input, request, job, diagnostic, attempt];
    if (options.mutate) events = options.mutate(events) ?? events;
    const record = events.find((e) => e.eventType === 'OpeningDiagnosticRecorded');
    if (record && !options.missingBody) {
      inputs.stageOpeningDiagnostic({ ...record.payload, detail: options.detail ?? detail });
    }
    const {
      policyVersion: _p,
      pricingVersion: _v,
      ...metadata
    } = meta('original-diagnostic', NOW + 10);
    return new RelationalCommitReplayer(f.db).appendCommit(
      {
        ...metadata,
        fingerprint: domainDigest(events),
        receipt: { opened: true },
        events,
        expectedVersions: Object.fromEntries(events.map((e) => [e.streamId, '0'])),
      },
      inputs,
    );
  })();
}
function rebuild(t, f, through = null) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const replayer = new RelationalCommitReplayer(db);
  for (const { seq } of f.db
    .prepare(
      'SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE ? IS NULL OR commit_seq<=? ORDER BY commit_seq',
    )
    .all(through, through))
    db.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), f.inputs))();
  return { db, inputs: new RelationalInputStore(db), replayer };
}
function anotherSource(f) {
  const { policyVersion: _p, pricingVersion: _v, ...metadata } = meta('other-diagnostic-source');
  f.db.transaction(() =>
    new RelationalCommitReplayer(f.db).appendCommit({
      ...metadata,
      fingerprint: domainDigest('other-source'),
      receipt: { registered: true },
      expectedVersions: { 'import:other-source': '0' },
      events: [
        {
          eventType: 'ImportSourceRegistered',
          streamId: 'import:other-source',
          memberId: null,
          accountId: null,
          occurredAt: NOW,
          policyVersion: 'original',
          pricingVersion: 'original',
          payload: {
            sourceId: 'other-source',
            sourceSha256: 'd'.repeat(64),
            sourceCommit: 'original',
            cutoffSeq: '1',
            cutoffAt: NOW,
            manifestSha256: 'e'.repeat(64),
            route: 'B',
          },
        },
      ],
    }),
  )();
}

test('Opening诊断原长正文/NULL与0只作保护内容；全回放零auth且唯一samecut安装后仍可读', async (t) => {
  for (const createdAt of [null, 0]) {
    const f = await fixture(t, { seconds: 0 }),
      wallet = f.generations.readWallet('a', 'm1');
    initializeFixtureConfiguration(f.db);
    const result = open(f, { createdAt });
    assert.ok(detail.length > 4000 && Buffer.byteLength(detail) < 16000);
    assert.equal(f.inputs.readGenerationDiagnostic('original-diagnostic', 'original-job'), detail);
    assert.equal(
      f.db.prepare('SELECT created_at FROM generation_diagnostics').get().created_at,
      createdAt,
    );
    assert.deepEqual(f.generations.readWallet('a', 'm1'), wallet);
    assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM charges').get().n, 0);
    const stored = readRelationalCommit(f.db, result.commitSeq);
    assert.equal(JSON.stringify(stored).includes(detail), false);
    assert.equal(
      stored.events.some(
        (e) => e.eventType === 'JobFailed' || e.eventType === 'GenerationRefunded',
      ),
      false,
    );
    const logs = new RelationalReportStore(f.db).readErrorLogs(0);
    assert.equal(logs[0].errorDetail, errorDetailMessage(detail));
    const target = rebuild(t, f);
    assert.equal(relationalPrivateStateEmpty(target.db), true);
    assert.equal(
      await authenticateMemberApiKey(new RelationalMemberStore(target.db), keys.m1),
      null,
    );
    assert.equal(
      target.inputs.readGenerationDiagnostic('original-diagnostic', 'original-job'),
      detail,
    );
    const before = databaseState(target.db);
    target.db.transaction(() => target.replayer.replayCommit(stored, f.inputs))();
    assert.equal(databaseState(target.db), before);
    const sourcePath = path.join(f.dir, 'diagnostic-source.sqlite');
    await f.db.backup(sourcePath);
    await verifyAndInstallRelationalStorage(target.db, {
      sourcePath,
      sourceSha256: relationalSnapshotSha256(sourcePath),
      historyId: 'generation-history',
      finalCommitSeq: result.commitSeq,
      replicas: { dataDir: f.dir },
    });
    assert.equal(
      target.inputs.readGenerationDiagnostic('original-diagnostic', 'original-job'),
      detail,
    );
  }
});

test('Opening诊断与Job双向唯一/原序/同成员来源；漏保护正文和错SHA不能半导入', async (t) => {
  for (const change of [
    {
      mutate: (e) => {
        e[3].payload.jobId = 'another-job';
      },
    },
    {
      mutate: (e) => {
        e[3].memberId = 'm2';
      },
    },
    {
      otherSource: true,
      mutate: (e) => {
        e[3].payload.origin.sourceId = 'other-source';
      },
    },
    {
      mutate: (e) => {
        [e[2], e[3]] = [e[3], e[2]];
      },
    },
    { mutate: (e) => e.filter((v) => v.eventType !== 'OpeningDiagnosticRecorded') },
    { mutate: (e) => e.filter((v) => v.eventType !== 'OpeningJobRecorded') },
    { mutate: (e) => [...e, structuredClone(e[3])] },
    { missingBody: true },
    {
      mutate: (e) => {
        e[3].payload.sha256 = '0'.repeat(64);
      },
    },
    { detail: 'x'.repeat(16001) },
  ]) {
    const f = await fixture(t);
    if (change.otherSource) anotherSource(f);
    const before = databaseState(f.db);
    assert.throws(() => open(f, change));
    assert.equal(databaseState(f.db), before);
    open(f);
    assert.equal(f.inputs.readGenerationDiagnostic('original-diagnostic', 'original-job'), detail);
  }
});

test('真实Opening诊断暂存与Job后尾SQL错在外catch前全回滚；原提交重试可读', async (t) => {
  const f = await fixture(t),
    before = databaseState(f.db);
  f.db.exec(`CREATE TEMP TABLE sentinel(value TEXT);
    CREATE TEMP TRIGGER diagnostic_tail BEFORE INSERT ON main.generation_attempts
    WHEN NEW.id='original-attempt' BEGIN SELECT RAISE(ABORT,'diagnostic actual tail'); END;`);
  f.db.transaction(() => {
    assert.throws(() => open(f), /diagnostic actual tail/);
    f.db.prepare('INSERT INTO temp.sentinel VALUES(?)').run('outer survived');
  })();
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT value FROM temp.sentinel').get().value, 'outer survived');
  f.db.exec('DROP TRIGGER temp.diagnostic_tail');
  open(f);
  assert.equal(f.inputs.readGenerationDiagnostic('original-diagnostic', 'original-job'), detail);
});

test('完整源诊断缺失/同长改字节时公共回放拒绝且目标全表不变，恢复原内容可重试', async (t) => {
  const f = await fixture(t),
    prior = f.db.prepare('SELECT MAX(commit_seq) AS n FROM commits').get().n;
  const result = open(f),
    stored = readRelationalCommit(f.db, result.commitSeq),
    target = rebuild(t, f, prior);
  const body = f.db.prepare('SELECT * FROM generation_diagnostics').get(),
    before = databaseState(target.db);
  for (const corrupt of [null, detail.replace('x', 'y')]) {
    if (corrupt === null) f.db.prepare('DELETE FROM generation_diagnostics').run();
    else
      f.db
        .prepare(
          'INSERT INTO generation_diagnostics(id,job_id,detail,sha256,created_at) VALUES(?,?,?,?,?)',
        )
        .run(body.id, body.job_id, corrupt, body.sha256, body.created_at);
    assert.throws(
      () => target.db.transaction(() => target.replayer.replayCommit(stored, f.inputs))(),
      { code: 'diagnostic_unavailable' },
    );
    assert.equal(databaseState(target.db), before);
  }
  f.db.prepare('UPDATE generation_diagnostics SET detail=?').run(detail);
  target.db.transaction(() => target.replayer.replayCommit(stored, f.inputs))();
  assert.equal(
    target.inputs.readGenerationDiagnostic('original-diagnostic', 'original-job'),
    detail,
  );
});

test('正常诊断仍要求已存在Job/4000字符；孤立Opening暂存不能借deferred FK提交', async (t) => {
  const f = await fixture(t),
    before = databaseState(f.db);
  assert.throws(
    () => f.db.transaction(() => f.inputs.stageGenerationDiagnostic('missing', 'ordinary', NOW))(),
    { code: 'diagnostic_unavailable' },
  );
  assert.throws(
    () => f.db.transaction(() => f.inputs.stageGenerationDiagnostic('missing', detail, NOW))(),
    { code: 'diagnostic_unavailable' },
  );
  assert.throws(
    () =>
      f.db.transaction(() =>
        f.inputs.stageOpeningDiagnostic({
          diagnosticId: 'orphan',
          jobId: 'missing',
          detail,
          sha256: sha(detail),
          createdAt: null,
          origin: origin('orphan'),
        }),
      )(),
    /FOREIGN KEY constraint failed/,
  );
  assert.equal(databaseState(f.db), before);
});
