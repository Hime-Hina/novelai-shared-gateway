import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fixture, meta, NOW, databaseState } from '../helpers/relational-generation.mjs';
import { domainDigest } from '../../server/events/domain-events.ts';
import { relationalFactStream } from '../../server/events/relational-fact-contracts.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageFormat,
  assertRelationalStorageReady,
  RELATIONAL_DATABASE_FORMAT,
  RELATIONAL_WRITER_VERSION,
} from '../../server/storage/relational-storage.ts';
import { RELATIONAL_SCHEMA_SQL } from '../../server/storage/relational-schema.ts';
import { RELATIONAL_EVENT_SCHEMA_VERSION } from '../../server/events/relational-events.ts';

// 与旧503夹具相同的原行粒度：只有child创建日和文件创建日，没有原parent或发布/完成时点。
// 此处的稳定member映射是选定来源的显式证明，不从文件MIME/ID/日期推断业务字段。
const original = Object.freeze({
  job: {
    id: 'historical-job',
    status: 'done',
    imageId: 'historical-image',
    createdAt: '1970-01-01T00:00:00.000Z',
  },
  image: {
    id: 'historical-image',
    mimeType: 'image/webp',
    file: 'images/historical-image.webp',
    createdAt: '1970-01-01T00:00:00.000Z',
  },
  stableMemberId: 'm1',
});
const sourceSha256 = createHash('sha256').update(JSON.stringify(original)).digest('hex');
const bytes = readFileSync(new URL('../fixtures/synthetic.webp', import.meta.url));
const hash = createHash('sha256').update(bytes).digest('hex');
const origin = (originalId) => ({ sourceId: 'precision-source', originalId });
function event(eventType, payload) {
  const kind = relationalFactStream(eventType),
    field = { import: 'sourceId', input: 'inputId', request: 'requestId', artifact: 'artifactId' }[
      kind
    ];
  return {
    eventType,
    payload,
    streamId: `${kind}:${payload[field]}`,
    memberId: ['import'].includes(kind) ? null : (payload.memberId ?? 'm1'),
    accountId: null,
    occurredAt: NOW + 20,
    policyVersion: 'original-policy',
    pricingVersion: 'original-price',
  };
}
function appendOpening(f, options = {}) {
  return f.db.transaction(() => {
    const definition = options.complete
      ? f.db
          .prepare(
            "SELECT i.* FROM generation_requests r JOIN generation_inputs i ON i.id=r.input_id WHERE r.id='definition'",
          )
          .get()
      : null;
    const input = f.inputs.stageOpeningInput({
      inputId: 'precision-input',
      memberId: 'm1',
      completeness: definition ? 'complete' : 'unavailable',
      body: definition ? JSON.parse(definition.body) : null,
      logicalBytes: definition?.logical_bytes ?? 0,
      assets: [],
    });
    const events = [
      event('ImportSourceRegistered', {
        sourceId: 'precision-source',
        sourceSha256,
        sourceCommit: '5e629689b0ed41c2df94b52db1dcb3646ffa7314',
        cutoffSeq: '0',
        cutoffAt: NOW,
        manifestSha256: sourceSha256,
        route: 'B',
      }),
      event('OpeningInputRecorded', { ...input, origin: origin(original.job.id) }),
      event('OpeningRequestRecorded', {
        requestId: 'structural-container',
        originalRequestId: null,
        memberId: original.stableMemberId,
        inputId: 'precision-input',
        contextId: null,
        source: null,
        operation: null,
        model: null,
        sampleCount: null,
        seedMode: 'unknown',
        confirmation: null,
        acceptedAt: null,
        retiredAt: null,
        origin: origin(original.job.id),
        ...(options.request ?? {}),
      }),
      event('OpeningJobRecorded', {
        jobId: original.job.id,
        requestId: 'structural-container',
        createdAt: Date.parse(original.job.createdAt),
        sampleIndex: null,
        actualSeed: null,
        status: original.job.status,
        queueSeq: null,
        queueExpiresAt: null,
        deadlineAt: null,
        admissionEpoch: null,
        artifactId: original.job.imageId,
        cacheKey: null,
        completedAt: null,
        errorCode: null,
        errorStatus: null,
        cancellationReason: null,
        lateResultAt: null,
        freeSucceededAt: null,
        freeWarningThreshold: null,
        retiredAt: null,
        origin: origin(original.job.id),
        ...(options.job ?? {}),
      }),
      event('OpeningArtifactRecorded', {
        artifactId: original.image.id,
        memberId: original.stableMemberId,
        inputId: 'precision-input',
        originalAttemptId: null,
        operation: null,
        mimeType: original.image.mimeType,
        byteLength: bytes.length,
        sha256: hash,
        publishedAt: null,
        retiredAt: null,
        origin: origin(original.image.id),
        ...(options.artifact ?? {}),
      }),
    ];
    const { policyVersion: _p, pricingVersion: _v, ...metadata } = meta('precision-opening');
    return new RelationalCommitReplayer(f.db).appendCommit(
      {
        ...metadata,
        fingerprint: domainDigest(events),
        receipt: { importedOriginalRows: true },
        events,
        expectedVersions: Object.fromEntries(
          events.map((e) => [
            e.streamId,
            f.db
              .prepare('SELECT CAST(stream_version AS TEXT) v FROM stream_heads WHERE stream_id=?')
              .get(e.streamId)?.v ?? '0',
          ]),
        ),
      },
      f.inputs,
    );
  })();
}
function empty(t) {
  const db = new Database(':memory:');
  initializeRelationalStorage(db, 'generation-history');
  t.after(() => db.close());
  return db;
}
function repeatOpening(f, result) {
  const stored = readRelationalCommit(f.db, result.events[0].commitSeq);
  return f.db.transaction(() =>
    new RelationalCommitReplayer(f.db).appendCommit(stored.command, f.inputs),
  )();
}

test('原parentless行只形成当前FK容器；创建零不冒完成、受理、发布时点或文件MIME操作', async (t) => {
  const f = await fixture(t);
  for (const key of ['requestId', 'source', 'operation', 'sampleIndex', 'completedAt']) {
    assert.equal(Object.hasOwn(original.job, key), false);
  }
  assert.equal(Object.hasOwn(original.image, 'publishedAt'), false);
  const result = appendOpening(f),
    r = f.db.prepare("SELECT * FROM generation_requests WHERE id='structural-container'").get(),
    j = f.db.prepare('SELECT * FROM generation_jobs WHERE id=?').get(original.job.id),
    a = f.db.prepare('SELECT * FROM artifacts WHERE id=?').get(original.image.id);
  assert.equal(r.source, null);
  assert.equal(r.operation, null);
  assert.equal(r.accepted_at, null);
  assert.equal(r.sample_count, null);
  assert.equal(j.sample_index, null);
  assert.equal(j.admission_epoch, null);
  assert.equal(j.completed_at, null);
  assert.equal(a.operation, null);
  assert.equal(a.published_at, null);
  assert.equal(
    f.db.prepare('SELECT created_at FROM generation_job_origins WHERE job_id=?').get(j.id)
      .created_at,
    0,
  );
  assert.equal(f.inputs.readOriginalRequestId(r.id), null);
  assert.equal(f.inputs.readJobSeed(j.id), null);
  const before = databaseState(f.db);
  assert.throws(
    () => f.inputs.readMemberInput(r.id, 'm1'),
    (error) => error.statusCode === 410,
  );
  assert.throws(
    () => f.generations.readGenerationExecutionState(j.id),
    (error) => error.statusCode === 410,
  );
  assert.equal(databaseState(f.db), before);
  assert.deepEqual(repeatOpening(f, result).receipt, result.receipt);
  assert.equal(databaseState(f.db), before);

  const target = empty(t),
    replayer = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db);
  const fetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = () => {
    calls++;
    throw new Error('replay cannot fetch');
  };
  try {
    for (const { seq } of f.db
      .prepare('SELECT CAST(commit_seq AS TEXT) seq FROM commits ORDER BY commit_seq')
      .all()) {
      target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), inputs))();
    }
  } finally {
    globalThis.fetch = fetch;
  }
  for (const table of [
    'generation_requests',
    'generation_jobs',
    'artifacts',
    'commits',
    'domain_events',
    'stream_heads',
  ]) {
    assert.deepEqual(
      target.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
      f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
    );
  }
  assert.equal(
    target.prepare('SELECT last_commit_seq FROM projection_state').get().last_commit_seq,
    f.db.prepare('SELECT last_commit_seq FROM projection_state').get().last_commit_seq,
  );
  assert.equal(target.prepare('SELECT COUNT(*) n FROM api_keys').get().n, 0);
  assert.equal(target.prepare('SELECT COUNT(*) n FROM upstream_credentials').get().n, 0);
  assert.equal(calls, 0);
});

test('已知原parent只返回其证明；0和已知历史操作仍原值，薄查询不读input/context正文', async (t) => {
  const f = await fixture(t);
  appendOpening(f, {
    request: {
      originalRequestId: 'actual-old-parent',
      source: 'old-ui',
      operation: 'generate',
      acceptedAt: 0,
    },
    job: { sampleIndex: 0, admissionEpoch: 0, completedAt: 0 },
    artifact: { operation: 'generate', publishedAt: 0 },
  });
  const prepare = f.db.prepare.bind(f.db),
    queries = [];
  f.db.prepare = (sql) => {
    queries.push(sql);
    return prepare(sql);
  };
  try {
    assert.equal(f.inputs.readOriginalRequestId('structural-container'), 'actual-old-parent');
  } finally {
    f.db.prepare = prepare;
  }
  assert.equal(
    queries.some((sql) =>
      /FROM (generation_inputs|normalization_contexts|input_assets)\b/i.test(sql),
    ),
    false,
  );
  assert.equal(
    queries.some((sql) =>
      /SELECT event_type,payload FROM domain_events WHERE stream_id=\?/i.test(sql),
    ),
    false,
  );
  assert.equal(
    f.db
      .prepare("SELECT accepted_at FROM generation_requests WHERE id='structural-container'")
      .get().accepted_at,
    0,
  );
  assert.equal(
    f.db.prepare('SELECT published_at FROM artifacts WHERE id=?').get(original.image.id)
      .published_at,
    0,
  );
});

test('活动Opening不能借NULL、结构容器或文件MIME取得执行权；外层捕获仍原子回滚', async (t) => {
  for (const status of ['pending', 'queued']) {
    await t.test(status, async (t) => {
      const f = await fixture(t),
        before = databaseState(f.db);
      f.db.transaction(() => {
        assert.throws(() =>
          appendOpening(f, {
            job: { status, artifactId: null, queueSeq: status === 'queued' ? '10' : null },
          }),
        );
      })();
      assert.equal(databaseState(f.db), before);
      appendOpening(f);
      assert.equal(f.inputs.readOriginalRequestId('structural-container'), null);
    });
  }
});

test('Opening第二owner写入失败不留下原事实、正文、游标或回执，健康重试和重复零写', async (t) => {
  const f = await fixture(t);
  const before = databaseState(f.db),
    prepare = f.db.prepare.bind(f.db);
  let attempted = 0;
  f.db.prepare = (sql) => {
    const statement = prepare(sql);
    if (sql.includes('INSERT INTO artifacts(')) {
      statement.run = () => {
        attempted++;
        throw new Error('precision SQL tail');
      };
    }
    return statement;
  };
  try {
    f.db.transaction(() => {
      assert.throws(() => appendOpening(f), /precision SQL tail/);
    })();
  } finally {
    f.db.prepare = prepare;
  }
  assert.equal(attempted, 1, '故障必须实际到达第二owner的artifact SQL，不能由格式或前置guard遮蔽');
  assert.equal(databaseState(f.db), before);
  const result = appendOpening(f);
  const healthy = databaseState(f.db);
  assert.equal(repeatOpening(f, result).replayed, true);
  assert.equal(databaseState(f.db), healthy);
});

test('即使Input/context/执行元数据全部已知，parentless容器仍不能成为活动原parent', async (t) => {
  const f = await fixture(t);
  f.accept('definition');
  const r = f.db.prepare("SELECT * FROM generation_requests WHERE id='definition'").get(),
    before = databaseState(f.db);
  f.db.transaction(() => {
    assert.throws(
      () =>
        appendOpening(f, {
          complete: true,
          request: {
            contextId: r.context_id,
            source: r.source,
            operation: r.operation,
            model: r.model,
            sampleCount: r.sample_count,
            seedMode: r.seed_mode,
            acceptedAt: 0,
          },
          job: {
            status: 'pending',
            sampleIndex: 0,
            admissionEpoch: 0,
            actualSeed: 42,
            artifactId: null,
          },
          artifact: { operation: 'generate' },
        }),
      /历史结构容器或未知元数据不能授权执行或发布/,
    );
  })();
  assert.equal(databaseState(f.db), before);
});

test('实际c653旧DDL及正确旧hash在同event7也零写拒绝，不能用改版本号冒旧源', (t) => {
  const old = JSON.parse(
    readFileSync(new URL('../fixtures/relational-schema-c653.json', import.meta.url), 'utf8'),
  );
  assert.equal(old.sourceCommit, 'c653bf2a2654a192f7f44c40dd136fc75c2db100');
  assert.equal(old.sourceBlob, 'f9736713426a1c46b86a18839eae686ef2328431');
  assert.equal(old.eventSchemaVersion, RELATIONAL_EVENT_SCHEMA_VERSION);
  assert.equal(old.sha256, '2314a5a3e20179083ca93a8a886ff8aaa83205ce395069b447a54a76c5be7c8a');
  assert.equal(createHash('sha256').update(old.sql).digest('hex'), old.sha256);
  assert.notEqual(old.sql, RELATIONAL_SCHEMA_SQL);
  const db = new Database(':memory:');
  t.after(() => db.close());
  db.exec(old.sql);
  const insert = db.prepare('INSERT INTO app_meta(key,value) VALUES(?,?)');
  insert.run('database_format', RELATIONAL_DATABASE_FORMAT);
  insert.run('schema_sha256', old.sha256);
  insert.run(
    'event_authority',
    JSON.stringify({
      version: RELATIONAL_WRITER_VERSION,
      minimumWriterVersion: RELATIONAL_WRITER_VERSION,
    }),
  );
  db.prepare(
    "INSERT INTO projection_state(id,history_id,reducer_version,last_commit_seq,status) VALUES(1,'old',1,0,'building')",
  ).run();
  const before = db.serialize();
  for (const check of [
    () => assertRelationalStorageFormat(db),
    () => assertRelationalStorageReady(db),
    () => new RelationalInputStore(db),
    () => new RelationalGenerationStore(db),
  ]) {
    assert.throws(check, /Unsupported relational database format/);
    assert.deepEqual(db.serialize(), before);
  }
  assert.throws(
    () => initializeRelationalStorage(db, 'old'),
    /Relational initialization requires an empty target; migrate offline/,
  );
  assert.deepEqual(db.serialize(), before);
});
