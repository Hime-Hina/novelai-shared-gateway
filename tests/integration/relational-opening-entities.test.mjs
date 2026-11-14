import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { fixture, meta, NOW, databaseState } from '../helpers/relational-generation.mjs';
import { initializeFixtureConfiguration } from '../helpers/relational-configuration.mjs';
import { domainDigest } from '../../server/events/domain-events.ts';
import { relationalFactStream } from '../../server/events/relational-fact-contracts.ts';
import {
  appendRelationalCommit,
  readRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { describeStoredResult, publishStoredResult } from '../../server/storage/result-files.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { relationalSnapshotSha256 } from '../../server/storage/relational-private-restore.ts';
import { normalizeNovelAiRequest } from '../../server/protocols/novelai-client.ts';
import { requestNormalizationDefaults } from '../../server/generation/request-fingerprint.ts';
import { nativeNormalizationContext } from '../../server/protocols/native-image.ts';
import { prepareRelationalInput } from '../../server/generation/relational-input.ts';

const image = await readFile(new URL('../fixtures/synthetic.webp', import.meta.url));
const origin = (kind) => ({ sourceId: 'synthetic-opening', originalId: `original-${kind}` });
function event(eventType, payload, memberId = 'm1') {
  const kind = relationalFactStream(eventType),
    fields = {
      request: 'requestId',
      input: 'inputId',
      artifact: 'artifactId',
      storage: 'storageTargetId',
      import: 'sourceId',
      context: 'contextId',
    };
  return {
    eventType,
    payload,
    streamId: `${kind}:${payload[fields[kind]]}`,
    memberId: ['storage', 'import', 'context'].includes(kind)
      ? null
      : (payload.memberId ?? memberId),
    accountId: null,
    occurredAt: NOW + 10,
    policyVersion: 'original-policy',
    pricingVersion: 'original-price',
  };
}
function command(db, events) {
  const { policyVersion: _p, pricingVersion: _v, ...metadata } = meta('opening-entities');
  return {
    ...metadata,
    fingerprint: domainDigest(events),
    receipt: { originalEntities: true },
    events,
    expectedVersions: Object.fromEntries(
      events.map((e) => [
        e.streamId,
        db
          .prepare('SELECT CAST(stream_version AS TEXT) AS v FROM stream_heads WHERE stream_id=?')
          .get(e.streamId)?.v ?? '0',
      ]),
    ),
  };
}
function open(f, options = {}) {
  return f.db.transaction(() => {
    const inputs = new RelationalInputStore(f.db),
      original = f.db.prepare("SELECT * FROM generation_requests WHERE id='definition'").get(),
      saved = f.db.prepare('SELECT * FROM generation_inputs WHERE id=?').get(original.input_id),
      unavailable = options.unavailable === true,
      registered = event('OpeningInputRecorded', {
        ...inputs.stageOpeningInput({
          inputId: 'old-input',
          memberId: 'm1',
          completeness: unavailable ? 'unavailable' : 'complete',
          body: unavailable ? null : (options.prepared?.body ?? JSON.parse(saved.body)),
          logicalBytes: unavailable ? 0 : (options.prepared?.logicalBytes ?? saved.logical_bytes),
          assets: [],
        }),
        origin: origin('input'),
      }),
      request = event('OpeningRequestRecorded', {
        requestId: 'old-request',
        memberId: 'm1',
        inputId: 'old-input',
        contextId: unavailable ? null : (options.prepared?.contextId ?? original.context_id),
        source: 'historical-source',
        operation: 'generate',
        model: unavailable ? null : (options.prepared?.body.model ?? original.model),
        sampleCount: 1,
        seedMode: unavailable ? 'unknown' : 'fixed',
        confirmation: null,
        acceptedAt: 0,
        retiredAt: null,
        origin: origin('request'),
        ...(options.request ?? {}),
      }),
      descriptor = describeStoredResult('old-artifact', 'generate', {
        mimeType: 'image/webp',
        buffer: image,
      }),
      job = event('OpeningJobRecorded', {
        jobId: 'old-job',
        requestId: 'old-request',
        createdAt: null,
        sampleIndex: 0,
        actualSeed: unavailable ? null : 42,
        status: 'done',
        queueSeq: null,
        queueExpiresAt: null,
        deadlineAt: null,
        admissionEpoch: 0,
        artifactId: descriptor.imageId,
        cacheKey: null,
        completedAt: 0,
        errorCode: null,
        errorStatus: null,
        cancellationReason: null,
        lateResultAt: null,
        freeSucceededAt: null,
        freeWarningThreshold: null,
        retiredAt: null,
        origin: origin('job'),
        ...(options.job ?? {}),
      }),
      artifact = event('OpeningArtifactRecorded', {
        artifactId: descriptor.imageId,
        memberId: 'm1',
        inputId: 'old-input',
        originalAttemptId: null,
        operation: 'generate',
        mimeType: descriptor.mimeType,
        byteLength: descriptor.byteLength,
        sha256: descriptor.sha256,
        publishedAt: 0,
        retiredAt: null,
        origin: origin('artifact'),
        ...(options.artifact ?? {}),
      }),
      entities =
        options.withJob === false
          ? [artifact]
          : options.artifactFirst
            ? [artifact, job]
            : [job, artifact],
      events = [registered, request, ...(options.withArtifact === false ? [job] : entities)];
    if (options.prepared) {
      events.unshift(
        event(
          'ContextRegistered',
          inputs.stageOpeningContext(options.prepared.contextId, options.prepared.context),
        ),
      );
    }
    if (options.local) {
      const local = new RelationalReplicaStore(f.db).planLocalArtifactPublication(
        descriptor,
        NOW + 10,
      );
      if (local.target) events.splice(2, 0, event(local.target.eventType, local.target.payload));
      events.push(event(local.replica.eventType, local.replica.payload));
    }
    events.push(...(options.extraEvents ?? []));
    const result = options.raw
      ? appendRelationalCommit(f.db, command(f.db, events))
      : new RelationalCommitReplayer(f.db).appendCommit(command(f.db, events), inputs);
    return { ...result, descriptor };
  })();
}
async function source(t) {
  const f = await fixture(t);
  f.accept('definition');
  return f;
}
function empty(t) {
  const db = new Database(':memory:');
  initializeRelationalStorage(db, 'generation-history');
  t.after(() => db.close());
  return db;
}
function rebuild(target, source, stop = null) {
  const replay = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(source);
  for (const { seq } of source
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    if (seq === stop) break;
    target.transaction(() => replay.replayCommit(readRelationalCommit(source, seq), inputs))();
  }
}

test('Opening Job/Artifact 两种原顺序都闭合循环引用，未知原尝试/创建时点与已知零保真', async (t) => {
  for (const artifactFirst of [false, true]) {
    await t.test(String(artifactFirst), async (t) => {
      const f = await source(t),
        result = open(f, { artifactFirst, unavailable: true }),
        target = empty(t);
      assert.equal(
        f.db.prepare("SELECT created_at FROM generation_job_origins WHERE job_id='old-job'").get()
          .created_at,
        null,
      );
      const row = f.db.prepare("SELECT * FROM generation_jobs WHERE id='old-job'").get();
      assert.equal(row.actual_seed, null);
      assert.equal(row.completed_at, 0);
      assert.equal(
        f.db.prepare("SELECT original_attempt_id FROM artifacts WHERE id='old-artifact'").get()
          .original_attempt_id,
        null,
      );
      const fetch = globalThis.fetch;
      let calls = 0;
      globalThis.fetch = () => {
        calls++;
        throw new Error('No replay external IO');
      };
      try {
        rebuild(target, f.db);
        assert.deepEqual(
          target.prepare("SELECT * FROM generation_jobs WHERE id='old-job'").get(),
          row,
        );
        assert.deepEqual(
          target.prepare('SELECT * FROM artifacts').all(),
          f.db.prepare('SELECT * FROM artifacts').all(),
        );
        const before = databaseState(target),
          changes = target.prepare('SELECT total_changes() AS n').get().n;
        target.transaction(() =>
          new RelationalCommitReplayer(target).replayCommit(
            readRelationalCommit(f.db, result.commitSeq),
            new RelationalInputStore(f.db),
          ),
        )();
        assert.equal(databaseState(target), before);
        assert.equal(target.prepare('SELECT total_changes() AS n').get().n, changes);
        assert.equal(calls, 0);
        for (const table of ['generation_attempts', 'charges', 'api_keys', 'upstream_credentials'])
          assert.equal(target.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0);
      } finally {
        globalThis.fetch = fetch;
      }
    });
  }
});

test('Opening 活动任务必须有原完整输入/上下文/实际种子，pending保留原期限且不造预扣', async (t) => {
  const f = await source(t);
  const job = {
    status: 'pending',
    artifactId: null,
    completedAt: null,
    createdAt: 0,
    deadlineAt: NOW + 60000,
    queueExpiresAt: NOW + 30000,
  };
  for (const options of [
    { unavailable: true },
    { job: { ...job, actualSeed: null } },
    { job: { ...job, status: 'running' } },
  ]) {
    const before = databaseState(f.db);
    assert.throws(() =>
      open(f, { ...options, job: { ...job, ...(options.job ?? {}) }, withArtifact: false }),
    );
    assert.equal(databaseState(f.db), before);
  }
  open(f, { job, withArtifact: false });
  const row = f.db.prepare("SELECT * FROM generation_jobs WHERE id='old-job'").get();
  assert.equal(row.deadline_at, NOW + 60000);
  assert.equal(row.queue_expires_at, NOW + 30000);
  assert.equal(
    f.db.prepare("SELECT created_at FROM generation_job_origins WHERE job_id='old-job'").get()
      .created_at,
    0,
  );
  assert.equal(new RelationalInputStore(f.db).readJobInput('old-job').seed, 42);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM charges').get().n, 0);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_attempts').get().n, 0);
  const target = empty(t);
  rebuild(target, f.db);
  assert.equal(new RelationalInputStore(target).readJobInput('old-job').seed, 42);
});

test('Opening 已退休父请求不能新增活动child，原退休历史仍保留', async (t) => {
  const f = await source(t),
    before = databaseState(f.db);
  for (const status of ['pending', 'queued']) {
    for (const retiredParent of [true, false]) {
      assert.throws(
        () =>
          open(f, {
            request: { retiredAt: retiredParent ? 0 : null },
            job: {
              status,
              queueSeq: status === 'queued' ? '100' : null,
              artifactId: null,
              completedAt: null,
              retiredAt: retiredParent ? null : 0,
            },
            withArtifact: false,
          }),
        /生命周期/,
      );
      assert.equal(databaseState(f.db), before);
    }
  }
  open(f, { request: { retiredAt: 0 }, job: { retiredAt: 0 } });
  const target = empty(t);
  rebuild(target, f.db);
  assert.equal(
    target.prepare("SELECT retired_at FROM generation_requests WHERE id='old-request'").get()
      .retired_at,
    0,
  );
  assert.equal(
    target.prepare("SELECT retired_at FROM generation_jobs WHERE id='old-job'").get().retired_at,
    0,
  );
});

test('Opening REAL种子仍按原已知context分工校验，inactive未知NULL不补值', async (t) => {
  const native = await source(t),
    before = databaseState(native.db);
  for (const actualSeed of [-1, 1.5, 4294967296]) {
    assert.throws(
      () => open(native, { job: { actualSeed } }),
      (e) => e.code === 'input_unavailable',
    );
    assert.equal(databaseState(native.db), before);
  }
  open(native, { job: { actualSeed: null } });
  assert.equal(new RelationalInputStore(native.db).readJobSeed('old-job'), null);
  for (const inactive of [false, true]) {
    const f = await source(t),
      context = {
        kind: 'direct',
        native: nativeNormalizationContext(),
        defaults: requestNormalizationDefaults({}),
      },
      prepared = prepareRelationalInput(
        normalizeNovelAiRequest(
          {
            tag: 'original direct literal',
            model: 'nai-diffusion-3',
            width: 128,
            height: 128,
            steps: 23,
            scale: 20,
            cfg: 0,
            sampler: 'original-direct-sampler',
            seed: -1.5,
          },
          context.defaults,
        ),
        context,
      );
    open(f, {
      prepared,
      unavailable: inactive,
      request: { contextId: prepared.contextId },
      ...(inactive ? {} : { withArtifact: false }),
      job: {
        actualSeed: -1.5,
        ...(inactive ? {} : { status: 'pending', artifactId: null, completedAt: null }),
      },
    });
    assert.equal(new RelationalInputStore(f.db).readJobSeed('old-job'), -1.5);
    if (!inactive) assert.equal(new RelationalInputStore(f.db).readJobInput('old-job').seed, -1.5);
    const target = empty(t);
    rebuild(target, f.db);
    assert.equal(new RelationalInputStore(target).readJobSeed('old-job'), -1.5);
    assert.equal(
      target.prepare("SELECT actual_seed FROM generation_jobs WHERE id='old-job'").get()
        .actual_seed,
      -1.5,
    );
  }
});

test('Opening 缺失/跨成员/跨操作产物必须在保存点内拒绝，不拖到外层deferred FK提交', async (t) => {
  const f = await source(t),
    prefix = f.db.serialize(),
    bad = open(f, { raw: true, unavailable: true, withArtifact: false }),
    target = empty(t);
  rebuild(target, f.db, bad.commitSeq);
  target.exec('CREATE TEMP TABLE outer_sentinel(value INTEGER)');
  const before = databaseState(target),
    replay = new RelationalCommitReplayer(target);
  target.transaction(() => {
    assert.throws(
      () =>
        replay.replayCommit(
          readRelationalCommit(f.db, bad.commitSeq),
          new RelationalInputStore(f.db),
        ),
      /产物/,
    );
    target.prepare('INSERT INTO outer_sentinel VALUES(1)').run();
  })();
  assert.equal(databaseState(target), before);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM outer_sentinel').get().n, 1);
  const healthy = new Database(prefix);
  t.after(() => healthy.close());
  const good = open({ db: healthy }, { unavailable: true });
  target.transaction(() =>
    replay.replayCommit(
      readRelationalCommit(healthy, good.commitSeq),
      new RelationalInputStore(healthy),
    ),
  )();
  assert.deepEqual(target.pragma('foreign_key_check'), []);
  for (const artifact of [
    { memberId: 'm2' },
    { operation: 'upscale' },
    { inputId: 'missing-original-input' },
    { originalAttemptId: 'unproven-attempt' },
  ]) {
    const fresh = new Database(prefix);
    t.after(() => fresh.close());
    const before = databaseState(fresh);
    assert.throws(() => open({ db: fresh }, { unavailable: true, artifact }));
    assert.equal(databaseState(fresh), before);
  }
});

test('Opening cached历史可指同成员现有原产物，不把来访job冒充原尝试或重新收费', async (t) => {
  const f = await source(t);
  f.dispatch('definition-0');
  await f.publish('definition-0');
  const wallet = f.db.prepare("SELECT * FROM wallets WHERE member_id='m1'").get(),
    charge = f.db.prepare('SELECT * FROM charges').all();
  open(f, { withArtifact: false, job: { artifactId: 'artifact-definition-0' } });
  assert.equal(
    f.db.prepare("SELECT input_id FROM generation_requests WHERE id='old-request'").get().input_id,
    'old-input',
  );
  assert.equal(
    f.db.prepare("SELECT input_id FROM artifacts WHERE id='artifact-definition-0'").get().input_id,
    'input-definition',
  );
  assert.deepEqual(f.db.prepare("SELECT * FROM wallets WHERE member_id='m1'").get(), wallet);
  assert.deepEqual(f.db.prepare('SELECT * FROM charges').all(), charge);
  const target = empty(t);
  rebuild(target, f.db);
  assert.deepEqual(
    target.prepare('SELECT * FROM generation_jobs').all(),
    f.db.prepare('SELECT * FROM generation_jobs').all(),
  );
  assert.deepEqual(target.prepare('SELECT * FROM charges').all(), charge);
});

test('Opening Job已写后仍拒绝同笔晚登记的Artifact原来源，外catch不留下循环半行', async (t) => {
  const f = await source(t),
    result = open(f, {
      raw: true,
      unavailable: true,
      artifact: { origin: { sourceId: 'late-source', originalId: 'original-artifact' } },
      extraEvents: [
        {
          ...event('ImportSourceRegistered', {
            sourceId: 'late-source',
            sourceSha256: 'a'.repeat(64),
            sourceCommit: 'known-original-cut',
            cutoffSeq: '0',
            cutoffAt: 0,
            manifestSha256: 'b'.repeat(64),
            route: 'A',
          }),
          occurredAt: 0,
        },
      ],
    }),
    target = empty(t);
  rebuild(target, f.db, result.commitSeq);
  let observed = 0;
  target.function('opening_job_written', () => {
    observed++;
    return 1;
  });
  target.exec(
    "CREATE TEMP TABLE outer_sentinel(value INTEGER); CREATE TEMP TRIGGER observe_opening_job AFTER INSERT ON main.generation_jobs WHEN NEW.id='old-job' BEGIN SELECT opening_job_written(); END",
  );
  const before = databaseState(target);
  target.transaction(() => {
    assert.throws(
      () =>
        new RelationalCommitReplayer(target).replayCommit(
          readRelationalCommit(f.db, result.commitSeq),
          new RelationalInputStore(f.db),
        ),
      /来源|顺序/,
    );
    target.prepare('INSERT INTO outer_sentinel VALUES(1)').run();
  })();
  assert.equal(observed, 1);
  assert.equal(databaseState(target), before);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM outer_sentinel').get().n, 1);
  assert.deepEqual(target.pragma('foreign_key_check'), []);
});

test('循环引用DDL反向INSERT/UPDATE均拒跨成员；同成员未来产物可正常闭合', async (t) => {
  const f = await source(t);
  f.fail('definition-0', { aborted: false });
  f.accept('other-definition', { memberId: 'm2' });
  const eventId = f.db
      .prepare("SELECT source_event_id AS id FROM generation_jobs WHERE id='definition-0'")
      .get().id,
    insert = (id, member, input) =>
      f.db
        .prepare(
          "INSERT INTO artifacts(id,member_id,input_id,original_attempt_id,operation,mime_type,byte_length,sha256,published_at,retired_at,source_event_id) VALUES(?,?,?,NULL,'generate','image/webp',?,?,0,NULL,?)",
        )
        .run(
          id,
          member,
          input,
          image.length,
          describeStoredResult(id, 'generate', { mimeType: 'image/webp', buffer: image }).sha256,
          eventId,
        );
  // 仅核DDL结构边界：这些直接SQL不冒称业务事实/ready来源。
  insert('unlinked-other', 'm2', 'input-other-definition');
  f.db.exec('CREATE TEMP TABLE outer_sentinel(value INTEGER)');
  for (const kind of ['insert', 'update']) {
    const before = databaseState(f.db);
    f.db.transaction(() => {
      assert.throws(
        () =>
          f.db.transaction(() => {
            f.db
              .prepare(
                "UPDATE generation_jobs SET artifact_id='raw-future' WHERE id='definition-0'",
              )
              .run();
            if (kind === 'insert') insert('raw-future', 'm2', 'input-other-definition');
            else
              f.db.prepare("UPDATE artifacts SET id='raw-future' WHERE id='unlinked-other'").run();
          })(),
        /artifact member does not own linked job/,
      );
      f.db.prepare('INSERT INTO outer_sentinel VALUES(1)').run();
    })();
    assert.equal(databaseState(f.db), before);
  }
  f.db.transaction(() => {
    f.db
      .prepare("UPDATE generation_jobs SET artifact_id='raw-future' WHERE id='definition-0'")
      .run();
    insert('raw-future', 'm1', 'input-definition');
  })();
  assert.deepEqual(f.db.pragma('foreign_key_check'), []);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM outer_sentinel').get().n, 2);
});

test('Opening Artifact第二次SQL写后故障，外catch仍全表/原事实/回执/游标回滚并可重试', async (t) => {
  const f = await source(t),
    result = open(f),
    target = empty(t);
  rebuild(target, f.db, result.commitSeq);
  let observed = 0;
  target.function('both_opening_entities_written', (jobs, artifacts) => {
    assert.equal(jobs, 1);
    assert.equal(artifacts, 1);
    observed++;
    return 1;
  });
  target.exec(`CREATE TEMP TABLE outer_sentinel(value INTEGER);
    CREATE TEMP TRIGGER fail_opening_artifact AFTER INSERT ON main.artifacts BEGIN
      SELECT both_opening_entities_written((SELECT COUNT(*) FROM generation_jobs WHERE id='old-job'),(SELECT COUNT(*) FROM artifacts WHERE id='old-artifact'));
      SELECT RAISE(ABORT,'opening artifact SQL failure'); END`);
  const before = databaseState(target),
    stored = readRelationalCommit(f.db, result.commitSeq),
    replay = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db);
  target.transaction(() => {
    assert.throws(() => replay.replayCommit(stored, inputs), /opening artifact SQL failure/);
    target.prepare('INSERT INTO outer_sentinel VALUES(1)').run();
  })();
  assert.equal(observed, 1);
  assert.equal(databaseState(target), before);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM outer_sentinel').get().n, 1);
  target.exec('DROP TRIGGER fail_opening_artifact');
  target.transaction(() => replay.replayCommit(stored, inputs))();
  assert.deepEqual(target.pragma('foreign_key_check'), []);
});

test('Opening Artifact实际Local副本走原owner与唯一ready，缺原字节不能靠元数据安装', async (t) => {
  const f = await source(t),
    descriptor = describeStoredResult('old-artifact', 'generate', {
      mimeType: 'image/webp',
      buffer: image,
    });
  await publishStoredResult(f.dir, descriptor, image);
  open(f, { local: true, unavailable: true });
  initializeFixtureConfiguration(f.db);
  const sourcePath = path.join(f.dir, 'entities-selected.sqlite');
  await f.db.backup(sourcePath);
  const sealed = new Database(sourcePath);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  const selected = {
    sourcePath,
    sourceSha256: relationalSnapshotSha256(sourcePath),
    historyId: 'generation-history',
    finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get()
      .seq,
    replicas: { dataDir: f.dir },
  };
  const target = empty(t);
  rebuild(target, f.db);
  const result = await verifyAndInstallRelationalStorage(target, selected);
  assert.equal(result.artifactsVerified, 1);
  assert.equal(target.prepare('SELECT status FROM projection_state').get().status, 'ready');
  assert.equal(relationalSnapshotSha256(sourcePath), selected.sourceSha256);
  for (const kind of ['missing', 'corrupt']) {
    const dataDir = path.join(f.dir, kind),
      target = empty(t);
    await mkdir(path.join(dataDir, 'images'), { recursive: true });
    if (kind === 'corrupt') {
      const bad = Buffer.from(image);
      bad[0] ^= 1;
      await writeFile(path.join(dataDir, descriptor.file), bad);
    }
    rebuild(target, f.db);
    const before = databaseState(target);
    await assert.rejects(
      verifyAndInstallRelationalStorage(target, { ...selected, replicas: { dataDir } }),
    );
    assert.equal(target.prepare('SELECT status FROM projection_state').get().status, 'building');
    assert.equal(databaseState(target), before);
    assert.equal(relationalSnapshotSha256(sourcePath), selected.sourceSha256);
  }
});
