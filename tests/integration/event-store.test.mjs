import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { migrateStorage } from '../../server/storage/sqlite-migrations.ts';
import {
  beginDomainCommand,
  recordDomainEvent,
  finishDomainCommand,
  setDomainCommandReceipt,
  setDomainCommandActor,
} from '../../server/events/domain-events.ts';
import {
  appendDomainCommit,
  readDomainCommits,
  readStreamProjection,
  replayDomainEvents,
} from '../../server/events/event-store.ts';
import {
  JsonStore,
  generationContextRef,
  applyEventProjection,
} from '../../server/storage/sqlite-store.ts';

const at = '2026-11-01T00:00:00.000Z';
const requestPayload = '{"seed":17}';
const requestRow = {
  id: 'r1',
  member_id: 'm1',
  source: 'member',
  operation: 'generate',
  created_at: at,
  payload_bytes: Buffer.byteLength(requestPayload),
  payload: requestPayload,
};
function database(t) {
  const sqlite = new Database(':memory:');
  sqlite.pragma('foreign_keys = ON');
  migrateStorage(sqlite);
  sqlite
    .prepare(
      'INSERT INTO generation_requests(id,member_id,source,operation,created_at,payload_bytes,payload) VALUES(@id,@member_id,@source,@operation,@created_at,@payload_bytes,@payload)',
    )
    .run(requestRow);
  t.after(() => sqlite.close());
  return sqlite;
}
function command(id, events, options = {}) {
  const state = {};
  beginDomainCommand(state, {
    commandId: id,
    fingerprint: `intent:${id}`,
    recordedAt: at,
    ...options,
  });
  for (const event of events) {
    recordDomainEvent(state, { streamId: 'request:r1', occurredAt: at, ...event });
  }
  setDomainCommandReceipt(state, { requestId: 'r1', jobIds: ['j1'] });
  return finishDomainCommand(state);
}
const accepted = () => ({
  eventType: 'RequestAccepted',
  payload: {
    requestId: 'r1',
    memberId: 'm1',
    source: 'member',
    createdAt: at,
    inputRef: 'r1',
    inputSha256: createHash('sha256').update(requestPayload).digest('hex'),
    jobs: [{ id: 'j1', status: 'queued', queueSeq: 1, summary: { seed: 17 } }],
  },
});

test('commit, projection and receipt are atomic and command retry does not run the projector twice', (t) => {
  const sqlite = database(t),
    batch = command('accept', [accepted()]);
  assert.throws(() => appendDomainCommit(sqlite, batch), /write transaction/);
  let projected = 0;
  const append = () =>
    sqlite.transaction(() =>
      appendDomainCommit(sqlite, batch, {
        project(db, commit) {
          projected++;
          db.prepare('INSERT INTO app_meta(key,value) VALUES(?,?)').run(
            'receipt-visible',
            commit.commitSeq,
          );
        },
      }),
    )();
  assert.deepEqual(append(), {
    commitSeq: '1',
    receipt: { requestId: 'r1', jobIds: ['j1'] },
    duplicate: false,
  });
  assert.equal(append().duplicate, true);
  assert.equal(projected, 1);
  assert.equal(readStreamProjection(sqlite, 'request:r1').projection.jobs.j1.memberId, 'm1');
  assert.equal(readDomainCommits(sqlite)[0].events[0].streamVersion, '1');
  assert.throws(
    () =>
      sqlite.transaction(() => appendDomainCommit(sqlite, { ...batch, fingerprint: 'changed' }))(),
    /fingerprint conflict/,
  );
});
test('authenticated actor and command transaction correlation survive SQL and replay', (t) => {
  const source = database(t),
    target = database(t),
    state = {};
  beginDomainCommand(state, { commandId: 'actor-command', recordedAt: at });
  setDomainCommandActor(state, { actorId: 'm1', actorKind: 'member', correlationId: 'r1' });
  recordDomainEvent(state, { streamId: 'request:r1', ...accepted() });
  source.transaction(() => appendDomainCommit(source, finishDomainCommand(state)))();
  const event = readDomainCommits(source)[0].events[0];
  assert.equal(event.actorKind, 'member');
  assert.equal(event.actorId, 'm1');
  assert.equal(event.transactionId, event.commandId);
  assert.equal(event.correlationId, 'r1');
  replayDomainEvents(source, target);
  assert.deepEqual(readDomainCommits(target), readDomainCommits(source));
});

test('SQL failure and expected-version conflict leave no partial batch, cursor or receipt', (t) => {
  const sqlite = database(t);
  const batch = command('accept', [accepted()]);
  sqlite.transaction(() => {
    assert.throws(
      () =>
        appendDomainCommit(sqlite, batch, {
          project(db) {
            db.exec('INSERT INTO absent_table VALUES(1)');
          },
        }),
      /absent_table/,
    );
  })();
  for (const table of ['event_commits', 'domain_events', 'event_streams', 'projection_versions']) {
    assert.equal(sqlite.prepare(`SELECT count(*) AS n FROM ${table}`).get().n, 0);
  }
  assert.equal(sqlite.prepare('SELECT value FROM event_clock').get(), undefined);
  sqlite.transaction(() => appendDomainCommit(sqlite, batch))();
  const conflict = command(
    'dispatch',
    [
      {
        eventType: 'DispatchAuthorized',
        payload: { jobId: 'j1', accountId: 'a1', attemptId: 'attempt:j1:1', authorizedAt: at },
      },
    ],
    { expectedVersions: { 'request:r1': '0' } },
  );
  assert.throws(
    () => sqlite.transaction(() => appendDomainCommit(sqlite, conflict))(),
    /Stream version conflict/,
  );
  assert.equal(readDomainCommits(sqlite).length, 1);
  assert.equal(sqlite.prepare('SELECT value FROM event_clock').get().value, 1);
});

test('complete commit replay resumes without duplicate effects and preserves failed jobs with late artifacts', (t) => {
  const source = database(t),
    target = database(t);
  const batches = [
    command('accept', [accepted()]),
    command('failure', [
      {
        eventType: 'DispatchAuthorized',
        payload: { jobId: 'j1', accountId: 'a1', attemptId: 'attempt:j1:1', authorizedAt: at },
      },
      {
        eventType: 'JobFailed',
        payload: {
          jobId: 'j1',
          failedAt: at,
          errorStatus: 502,
          message: 'synthetic failure',
          generationRefund: { at, reason: 'upstream failure' },
        },
      },
    ]),
    command('late', [
      {
        eventType: 'ArtifactPublished',
        payload: {
          jobId: 'j1',
          artifact: { id: 'image1', file: 'output/image1.png', sha256: '1'.repeat(64) },
        },
      },
      {
        eventType: 'LateResultRecovered',
        payload: { jobId: 'j1', artifactId: 'image1', at, refundRetained: true },
      },
      {
        eventType: 'FreeImageSucceeded',
        payload: { jobId: 'j1', memberId: 'm1', succeededAt: 1793491200000, warningThreshold: 10 },
      },
    ]),
  ];
  for (const batch of batches) {
    source.transaction(() => appendDomainCommit(source, batch))();
  }
  assert.deepEqual(replayDomainEvents(source, target, { maxCommits: 2 }), {
    lastCommitSeq: '2',
    commits: 2,
    events: 3,
  });
  assert.deepEqual(replayDomainEvents(source, target), {
    lastCommitSeq: '3',
    commits: 1,
    events: 3,
  });
  assert.deepEqual(replayDomainEvents(source, target), {
    lastCommitSeq: '3',
    commits: 0,
    events: 0,
  });
  assert.deepEqual(
    readStreamProjection(target, 'request:r1'),
    readStreamProjection(source, 'request:r1'),
  );
  const job = readStreamProjection(target, 'request:r1').projection.jobs.j1;
  assert.equal(job.status, 'failed');
  assert.equal(job.lateResultAvailable, true);
  assert.equal(job.generationRefund.reason, 'upstream failure');
  assert.equal(readDomainCommits(target, '1', 1)[0].events.length, 2);
});

test('offline replay rolls back a failed 100-commit batch and resumes from its prior durable checkpoint', async (t) => {
  const source = database(t),
    directory = await mkdtemp(path.join(os.tmpdir(), 'nai-event-batch-replay-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  source.transaction(() =>
    appendDomainCommit(source, command('accept', [accepted()]), { project: applyEventProjection }),
  )();
  for (let index = 2; index <= 205; index++) {
    source.transaction(() =>
      appendDomainCommit(
        source,
        command(`queue-${index}`, [
          { eventType: 'JobQueued', payload: { jobId: 'j1', queueSeq: index, queuedAt: at } },
        ]),
        { project: applyEventProjection },
      ),
    )();
  }
  for (const { failAt, completed, resumeFullCommits } of [
    { failAt: 1, completed: 0, resumeFullCommits: 4 },
    { failAt: 101, completed: 100, resumeFullCommits: 3 },
    { failAt: 150, completed: 100, resumeFullCommits: 3 },
  ]) {
    await t.test(`SQL failure at logical commit ${failAt}`, async (t) => {
      let fullCommits = 0;
      const open = () =>
        new Database(path.join(directory, `target-${failAt}.sqlite`), {
          verbose: (sql) => {
            if (sql === 'COMMIT') {
              fullCommits++;
            }
          },
        });
      let target = open();
      t.after(() => target.close());
      target.pragma('journal_mode = WAL');
      target.pragma('synchronous = FULL');
      migrateStorage(target);
      target
        .prepare(
          'INSERT INTO generation_requests(id,member_id,source,operation,created_at,payload_bytes,payload) VALUES(@id,@member_id,@source,@operation,@created_at,@payload_bytes,@payload)',
        )
        .run(requestRow);
      target.exec(`CREATE TEMP TRIGGER fail_replay_batch BEFORE INSERT ON event_commits WHEN NEW.commit_seq=${failAt}
        BEGIN SELECT RAISE(ABORT,'synthetic replay batch failure'); END;`);
      fullCommits = 0;
      assert.throws(
        () => replayDomainEvents(source, target, { project: applyEventProjection }),
        /synthetic replay batch failure/,
      );
      assert.equal(target.prepare('SELECT count(*) AS n FROM event_commits').get().n, completed);
      assert.equal(target.prepare('SELECT count(*) AS n FROM domain_events').get().n, completed);
      assert.equal(target.prepare('SELECT value FROM event_clock').get()?.value || 0, completed);
      assert.equal(
        target.prepare("SELECT last_commit_seq FROM projection_versions WHERE name='core'").get()
          ?.last_commit_seq || 0,
        completed,
      );
      assert.equal(
        readStreamProjection(target, 'request:r1')?.projection.jobs.j1.queueSeq || 0,
        completed,
      );
      const job = target.prepare('SELECT data FROM jobs WHERE id=?').get('j1');
      assert.equal(job ? JSON.parse(job.data).queueSeq : 0, completed);
      assert.deepEqual(
        readDomainCommits(target),
        completed ? readDomainCommits(source, '0', completed) : [],
      );
      assert.equal(fullCommits, completed ? 2 : 1); // 预检查 + 完整成功组；失败组只有 ROLLBACK。
      target.close();
      target = open();
      target.pragma('synchronous = FULL');
      fullCommits = 0;
      assert.deepEqual(replayDomainEvents(source, target, { project: applyEventProjection }), {
        lastCommitSeq: '205',
        commits: 205 - completed,
        events: 205 - completed,
      });
      assert.equal(fullCommits, resumeFullCommits);
      assert.deepEqual(readDomainCommits(target, '0', 1000), readDomainCommits(source, '0', 1000));
      assert.deepEqual(
        readStreamProjection(target, 'request:r1'),
        readStreamProjection(source, 'request:r1'),
      );
      assert.deepEqual(
        target.prepare('SELECT data,order_value FROM jobs').all(),
        source.prepare('SELECT data,order_value FROM jobs').all(),
      );
    });
  }
  const bounded = database(t);
  assert.deepEqual(
    replayDomainEvents(source, bounded, {
      maxCommits: 125,
      throughCommitSeq: '150',
      project: applyEventProjection,
    }),
    { lastCommitSeq: '125', commits: 125, events: 125 },
  );
  assert.deepEqual(
    replayDomainEvents(source, bounded, { throughCommitSeq: '150', project: applyEventProjection }),
    { lastCommitSeq: '150', commits: 25, events: 25 },
  );
  assert.deepEqual(
    replayDomainEvents(source, bounded, { throughCommitSeq: '150', project: applyEventProjection }),
    { lastCommitSeq: '150', commits: 0, events: 0 },
  );
  assert.deepEqual(readDomainCommits(bounded, '0', 1000), readDomainCommits(source, '0', 150));
});

test('unknown event versions and secret payloads fail before recording facts', (t) => {
  const sqlite = database(t);
  for (const payload of [
    { userToken: 'secret' },
    { nested: { prompt: 'private' } },
    { base64: 'private' },
  ]) {
    assert.throws(
      () => command('unsafe', [{ eventType: 'RequestAccepted', payload }]),
      /forbidden/,
    );
  }
  assert.throws(
    () => command('future', [{ eventType: 'RequestAccepted', schemaVersion: 2, payload: {} }]),
    /Unsupported domain event/,
  );
  const batch = command('accept', [accepted()]);
  sqlite.transaction(() => appendDomainCommit(sqlite, batch))();
  sqlite.prepare('UPDATE domain_events SET schema_version=99').run();
  assert.throws(
    () => readDomainCommits(sqlite),
    new RegExp(`Unsupported domain event ${batch.events[0].eventId}`),
  );
});

test('64 bit cursors cross the JavaScript safe integer boundary without rounding', (t) => {
  const sqlite = database(t);
  sqlite.prepare('INSERT INTO event_clock(id,value) VALUES(1,?)').run(9_007_199_254_740_991n);
  const result = sqlite.transaction(() =>
    appendDomainCommit(sqlite, command('large', [accepted()])),
  )();
  assert.equal(result.commitSeq, '9007199254740992');
  assert.equal(readDomainCommits(sqlite, '9007199254740991')[0].commitSeq, '9007199254740992');
});
test('replay orders numeric commit sequences correctly beyond nine commits', (t) => {
  const source = database(t),
    target = database(t);
  source.transaction(() => appendDomainCommit(source, command('accept', [accepted()])))();
  for (let index = 2; index <= 12; index++) {
    source.transaction(() =>
      appendDomainCommit(
        source,
        command(`queue-${index}`, [
          { eventType: 'JobQueued', payload: { jobId: 'j1', queueSeq: index, queuedAt: at } },
        ]),
      ),
    )();
  }
  assert.deepEqual(
    readDomainCommits(source).map((commit) => commit.commitSeq),
    Array.from({ length: 12 }, (_, index) => String(index + 1)),
  );
  assert.deepEqual(replayDomainEvents(source, target), {
    lastCommitSeq: '12',
    commits: 12,
    events: 12,
  });
  assert.equal(readStreamProjection(target, 'request:r1').projection.jobs.j1.queueSeq, 12);
});

test('JsonStore commits explicit events and restores its JavaScript state after a projection failure', async (t) => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'nai-domain-store-'));
  const store = new JsonStore(directory);
  t.after(async () => {
    await store.close();
    await rm(directory, { recursive: true, force: true });
  });
  await store.init();
  await store.update(
    (db) => {
      db.settings.testFact = 'committed';
      recordDomainEvent(db, { streamId: 'request:r1', ...accepted() });
    },
    { generationRequest: requestRow },
  );
  const committedCount = readDomainCommits(store.sqlite).length;
  assert.equal(
    readDomainCommits(store.sqlite)
      .flatMap((commit) => commit.events)
      .filter((event) => event.eventType === 'RequestAccepted').length,
    1,
  );
  await assert.rejects(
    store.update((db) => {
      db.settings.testFact = 'must roll back';
      recordDomainEvent(db, {
        streamId: 'request:missing',
        eventType: 'JobCompleted',
        payload: { jobId: 'missing', completedAt: at },
      });
    }),
    /Missing request opening/,
  );
  assert.equal(store.db.settings.testFact, 'committed');
  assert.equal(readDomainCommits(store.sqlite).length, committedCount);
  const row = {
    member_id: 'm1',
    operation: 'generate',
    key_hash: 'abc',
    fingerprint: 'f',
    normalization_context: '{"prompt":"protected input"}',
    job_id: 'j1',
    request_id: null,
    created_at: at,
  };
  await store.update(() => true, { collections: [], generationIdempotency: row });
  assert.equal(
    store.sqlite
      .prepare('SELECT payload FROM generation_contexts WHERE context_ref=?')
      .get(generationContextRef(row)).payload,
    row.normalization_context,
  );
  assert.equal(JSON.stringify(readDomainCommits(store.sqlite)).includes('protected input'), false);
});
