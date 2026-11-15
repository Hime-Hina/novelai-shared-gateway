import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { mapLegacyArtifacts } from '../../scripts/migrate-legacy-artifacts.mjs';
import { domainDigest } from '../../server/events/domain-events.ts';
import { relationalFactStream } from '../../server/events/relational-fact-contracts.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { databaseState } from '../helpers/relational-generation.mjs';

const NOW = 1800000000000,
  EPOCH = '1970-01-01T00:00:00.000Z',
  sourceSha256 = 'a'.repeat(64),
  sourceId = `legacy:${sourceSha256}`;
const refs = () => ({
  sourceSha256,
  sourceId,
  origin: (originalId) => ({ sourceId, originalId }),
  eventId: (eventType, originalId) =>
    'migration:' + domainDigest({ sourceSha256, eventType, originalId }),
  memberIds: new Set(['m1', 'm2']),
  accountIds: new Set(),
  attemptIds: new Set(),
  inputsByArtifact: new Map([
    ['original-a', { inputId: 'input-a', memberId: 'm1', operation: null }],
    ['original-b', { inputId: 'input-b', memberId: 'm1', operation: null }],
  ]),
});
function descriptor(id = 'original-a', changes = {}) {
  const sha256 = domainDigest(id);
  return {
    id,
    jobId: null,
    memberId: null,
    operation: null,
    mimeType: 'image/webp',
    byteLength: 100,
    sha256,
    file: null,
    createdAt: '2026-11-01T00:00:00.000Z',
    r2: {
      bucket: 'original-images',
      key: `images/${id}/${sha256}`,
      byteLength: 100,
      sha256,
      verifiedAt: EPOCH,
      localRemovedAt: null,
      removedAt: null,
    },
    pikpak: { url: 'https://dav.invalid/original/', uploadedAt: EPOCH, encoding: 'brotli' },
    externalBytesChecked: false,
    ...changes,
  };
}
const storageTargets = [
  { kind: 'local', namespace: 'images', originalId: 'original-local-root-proof' },
  {
    kind: 'r2',
    namespace: `r2://${'1'.repeat(32)}/original-images`,
    originalId: 'original-r2-namespace-proof',
  },
  {
    kind: 'pikpak',
    namespace: 'https://dav.invalid/original/',
    originalId: 'original-pik-namespace-proof',
  },
];
function metadata(artifacts, changes = {}) {
  return { artifacts, storageTargets, ...changes };
}
function command(db, id, facts) {
  const events = facts.map((entry) => {
    const kind = relationalFactStream(entry.eventType),
      identity = entry.payload.artifactId ?? entry.payload.storageTargetId ?? entry.payload.inputId;
    return {
      ...entry,
      streamId: `${kind}:${identity ?? entry.payload.sourceId ?? entry.payload.memberId}`,
      occurredAt: NOW,
      policyVersion: 'original-policy',
      pricingVersion: 'original-pricing',
    };
  });
  return {
    commandId: id,
    fingerprint: domainDigest(events),
    actorKind: 'system',
    actorId: null,
    recordedAt: NOW,
    correlationId: null,
    causationId: null,
    expectedVersions: Object.fromEntries(
      events.map((e) => [
        e.streamId,
        db
          .prepare('SELECT CAST(stream_version AS TEXT) AS v FROM stream_heads WHERE stream_id=?')
          .get(e.streamId)?.v ?? '0',
      ]),
    ),
    receipt: { imported: id },
    events,
  };
}
function append(db, id, facts, inputs) {
  return db.transaction(() =>
    new RelationalCommitReplayer(db).appendCommit(command(db, id, facts), inputs),
  )();
}
function database(t) {
  const db = new Database(':memory:');
  initializeRelationalStorage(db, 'migration-artifact-synthetic');
  t.after(() => db.close());
  return db;
}
function fixture(t) {
  const db = database(t),
    r = refs(),
    inputs = new RelationalInputStore(db);
  append(db, 'source-and-members', [
    {
      eventType: 'ImportSourceRegistered',
      payload: {
        sourceId,
        sourceSha256,
        sourceCommit: '3caba3d',
        cutoffSeq: '1',
        cutoffAt: NOW,
        manifestSha256: 'b'.repeat(64),
        route: 'B',
      },
      memberId: null,
      accountId: null,
    },
    ...['m1', 'm2'].map((memberId) => ({
      eventType: 'OpeningMemberRecorded',
      payload: {
        memberId,
        displayName: memberId,
        status: 'active',
        acceptingJobs: false,
        admissionEpoch: 0,
        createdAt: 0,
        origin: r.origin(memberId),
      },
      memberId,
      accountId: null,
    })),
  ]);
  db.transaction(() => {
    const facts = [...r.inputsByArtifact.values()].map(({ inputId, memberId }) => ({
      eventType: 'OpeningInputRecorded',
      payload: {
        ...inputs.stageOpeningInput({
          inputId,
          memberId,
          completeness: 'unavailable',
          body: null,
          logicalBytes: 0,
          assets: [],
        }),
        origin: r.origin(inputId),
      },
      memberId,
      accountId: null,
    }));
    append(db, 'original-inputs', facts, inputs);
  })();
  return { db, inputs, refs: r };
}

test('artifact mapper preserves standalone identities, NULL times and known zero without IO; complete replay and repeat write zero', (t) => {
  const f = fixture(t),
    noNetwork = t.mock.method(globalThis, 'fetch', () =>
      assert.fail('mapping/replay must not fetch'),
    ),
    mapped = mapLegacyArtifacts(null, f.refs, metadata([descriptor()]));
  assert.deepEqual(mapped.expected, { artifacts: 1, storageTargets: 2, replicas: 2 });
  assert.deepEqual(mapped.findings, []);
  const result = append(f.db, 'original-artifacts', mapped.facts),
    artifact = f.db.prepare('SELECT * FROM artifacts').get(),
    replicas = f.db.prepare('SELECT * FROM artifact_replicas ORDER BY storage_target_id').all();
  assert.equal(artifact.id, 'original-a');
  assert.equal(artifact.member_id, 'm1');
  assert.equal(artifact.input_id, 'input-a');
  assert.equal(artifact.original_attempt_id, null);
  assert.equal(artifact.operation, null);
  assert.equal(artifact.published_at, null);
  assert.equal(artifact.retired_at, null);
  assert.deepEqual(
    replicas.map((r) => [r.encoding, r.uploaded_at, r.verified_at]),
    [
      ['br', 0, null],
      ['identity', null, 0],
    ],
  );
  for (const table of [
    'generation_requests',
    'generation_jobs',
    'generation_attempts',
    'charges',
  ]) {
    assert.equal(f.db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0);
  }
  const target = database(t);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() =>
      new RelationalCommitReplayer(target).replayCommit(readRelationalCommit(f.db, seq), f.inputs),
    )();
  }
  assert.equal(databaseState(target), databaseState(f.db));
  const before = databaseState(target),
    changes = target.prepare('SELECT total_changes() AS n').get().n;
  assert.equal(
    target.transaction(() =>
      new RelationalCommitReplayer(target).replayCommit(
        readRelationalCommit(f.db, result.commitSeq),
      ),
    )().replayed,
    true,
  );
  assert.equal(databaseState(target), before);
  assert.equal(target.prepare('SELECT total_changes() AS n').get().n, changes);
  assert.equal(target.prepare('SELECT status FROM projection_state').get().status, 'building');
  for (const table of ['api_keys', 'member_sessions', 'upstream_credentials']) {
    assert.equal(target.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0);
  }
  assert.equal(noNetwork.mock.callCount(), 0);
});

test('artifact mapper rejects conflicting stable owner, operation and original-attempt links', (t) => {
  const f = fixture(t),
    before = databaseState(f.db);
  assert.throws(
    () =>
      mapLegacyArtifacts(null, f.refs, metadata([descriptor('original-a', { memberId: 'm2' })])),
    /proven source relationships/,
  );
  f.refs.inputsByArtifact.get('original-a').operation = 'generate';
  assert.throws(
    () =>
      mapLegacyArtifacts(
        null,
        f.refs,
        metadata([descriptor('original-a', { operation: 'upscale' })]),
      ),
    /proven source relationships/,
  );
  f.refs.artifactAttemptById = new Map([
    [
      'original-a',
      {
        attemptId: 'unproven',
        accountId: 'other',
        artifactId: 'original-a',
        memberId: 'm1',
        inputId: 'input-a',
      },
    ],
  ]);
  assert.throws(
    () => mapLegacyArtifacts(null, f.refs, metadata([descriptor()])),
    /proven source relationships/,
  );
  assert.equal(databaseState(f.db), before);
});

test('artifact mapper does not invent namespace, local verification or a cloud promise from current same-name candidates', (t) => {
  const f = fixture(t),
    local = descriptor('original-a', { file: 'images/original-a.webp', r2: null, pikpak: null }),
    mapped = mapLegacyArtifacts(
      null,
      f.refs,
      metadata([local], {
        currentR2Candidates: ['original-a'],
        currentPikCandidates: ['original-a'],
      }),
    );
  assert.deepEqual(mapped.expected, { artifacts: 1, storageTargets: 1, replicas: 1 });
  const replica = mapped.facts.find((e) => e.eventType === 'OpeningReplicaRecorded').payload;
  assert.equal(replica.objectKey, 'original-a.webp');
  assert.equal(replica.uploadedAt, null);
  assert.equal(replica.verifiedAt, null);
  assert.ok(mapped.findings.some((e) => e.code === 'LOCAL_BYTES_NOT_VERIFIED'));
  append(f.db, 'local-without-byte-proof', mapped.facts);
  const missing = mapLegacyArtifacts(
    null,
    f.refs,
    metadata([descriptor('original-b')], { storageTargets: [] }),
  );
  assert.deepEqual(missing.expected, { artifacts: 1, storageTargets: 0, replicas: 0 });
  assert.deepEqual(
    missing.findings.map((e) => e.code),
    ['R2_NAMESPACE_UNPROVEN', 'PIKPAK_NAMESPACE_UNPROVEN'],
  );
  const unsupported = mapLegacyArtifacts(
    null,
    f.refs,
    metadata([descriptor('original-b', { mimeType: 'unsupported-source-value' })]),
  );
  assert.equal(unsupported.facts.length, 0);
  assert.deepEqual(unsupported.findings, [
    { originalId: 'original-b', code: 'ARTIFACT_ONLINE_DESCRIPTOR_UNSUPPORTED' },
  ]);
});

test('artifact mapper source namespace mismatch rejects instead of rebinding a bucket or Pik directory', () => {
  const row = descriptor();
  row.r2.bucket = 'different-original-bucket';
  assert.throws(
    () => mapLegacyArtifacts(null, refs(), metadata([row])),
    /proven source relationships/,
  );
  row.r2.bucket = 'original-images';
  row.pikpak.url = 'https://dav.invalid/different-original/';
  assert.throws(
    () => mapLegacyArtifacts(null, refs(), metadata([row])),
    /proven source relationships/,
  );
});

test('artifact mapper complete owner rejects conflicting object content and outer catch rolls back the whole draft', (t) => {
  const f = fixture(t),
    a = descriptor(),
    b = descriptor('original-b');
  b.r2.key = a.r2.key;
  const mapped = mapLegacyArtifacts(null, f.refs, metadata([a, b])),
    before = databaseState(f.db);
  f.db.transaction(() => {
    assert.throws(
      () =>
        new RelationalCommitReplayer(f.db).appendCommit(
          command(f.db, 'conflicting-locator', mapped.facts),
        ),
      /同一物理对象的内容或编码冲突/,
    );
    assert.equal(databaseState(f.db), before);
  })();
  assert.equal(databaseState(f.db), before);
});

test('artifact mapper valid draft hits SQL tail, retains source binding, and outer catch cannot keep partial artifacts', (t) => {
  const f = fixture(t),
    mapped = mapLegacyArtifacts(null, f.refs, metadata([descriptor()])),
    before = databaseState(f.db);
  f.db.exec(
    "CREATE TEMP TRIGGER fail_replica BEFORE INSERT ON main.artifact_replicas BEGIN SELECT RAISE(ABORT,'synthetic replica tail'); END",
  );
  f.db.transaction(() => {
    assert.throws(
      () =>
        new RelationalCommitReplayer(f.db).appendCommit(command(f.db, 'sql-tail', mapped.facts)),
      /synthetic replica tail/,
    );
    assert.equal(databaseState(f.db), before);
  })();
  f.db.exec('DROP TRIGGER temp.fail_replica');
  const wrong = structuredClone(mapped.facts);
  wrong.find((e) => e.eventType === 'OpeningArtifactRecorded').payload.origin.sourceId =
    'wrong-cut';
  assert.throws(() => append(f.db, 'wrong-source', wrong), /登记来源/);
  assert.equal(databaseState(f.db), before);
  append(f.db, 'valid-after-rejection', mapped.facts);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM artifacts').get().n, 1);
});

test('artifact mapper preserves two stable artifacts referencing one matching object instead of deduplicating their identities', (t) => {
  const f = fixture(t),
    a = descriptor(),
    b = descriptor('original-b', {
      sha256: a.sha256,
      r2: { ...a.r2 },
    }),
    mapped = mapLegacyArtifacts(null, f.refs, metadata([a, b]));
  append(f.db, 'matching-shared-object', mapped.facts);
  assert.deepEqual(mapped.expected, { artifacts: 2, storageTargets: 2, replicas: 4 });
  assert.equal(f.db.prepare('SELECT COUNT(DISTINCT id) AS n FROM artifacts').get().n, 2);
  assert.equal(
    f.db.prepare('SELECT COUNT(DISTINCT object_key) AS n FROM artifact_replicas').get().n,
    2,
  );
});

test('artifact mapper only accepts complete publication evidence for that original tuple; createdAt and cache visitors prove neither owner nor time', () => {
  const r = refs(),
    row = descriptor('original-a', { jobId: 'cached-visitor' });
  r.inputsByArtifact.clear();
  r.inputsByJob = new Map([['cached-visitor', { inputId: 'visitor-input', memberId: 'm2' }]]);
  const missing = mapLegacyArtifacts(null, r, metadata([row]));
  assert.equal(missing.facts.length, 0);
  assert.deepEqual(missing.findings, [
    { originalId: row.id, code: 'ARTIFACT_INPUT_OWNER_UNPROVEN' },
  ]);
  r.inputsByArtifact = refs().inputsByArtifact;
  const proof = {
    memberId: 'm1',
    inputId: 'input-a',
    sha256: row.sha256,
    byteLength: row.byteLength,
    publishedAt: 0,
    retiredAt: null,
  };
  r.artifactHistoryById = new Map([[row.id, proof]]);
  const mapped = mapLegacyArtifacts(null, r, metadata([row]));
  assert.equal(
    mapped.facts.find((e) => e.eventType === 'OpeningArtifactRecorded').payload.publishedAt,
    0,
  );
  proof.sha256 = 'f'.repeat(64);
  assert.throws(() => mapLegacyArtifacts(null, r, metadata([row])), /proven source relationships/);
});

test('artifact mapper retains original noncanonical locator and tombstone zero without inventing an upload or new Pik commitment', (t) => {
  const f = fixture(t),
    row = descriptor();
  row.file = 'images/original-a.webp';
  row.r2.localRemovedAt = EPOCH;
  row.r2.key = `images/original-other-location/${row.sha256}`;
  row.r2.removedAt = EPOCH;
  row.pikpak = null;
  const mapped = mapLegacyArtifacts(
    null,
    f.refs,
    metadata([row], {
      currentPikCandidates: [{ artifactId: row.id, storedBytes: 100 }],
    }),
  );
  assert.deepEqual(mapped.expected, { artifacts: 1, storageTargets: 2, replicas: 2 });
  assert.equal(
    mapped.findings.some((e) => e.code === 'LOCAL_BYTES_NOT_VERIFIED'),
    false,
  );
  append(f.db, 'original-tombstone', mapped.facts);
  const replica = f.db
    .prepare(
      "SELECT r.* FROM artifact_replicas r JOIN storage_targets s ON s.id=r.storage_target_id WHERE s.kind='r2'",
    )
    .get();
  assert.equal(replica.object_key, row.r2.key);
  assert.equal(replica.uploaded_at, null);
  assert.equal(replica.verified_at, 0);
  assert.equal(replica.deleted_at, 0);
  const local = f.db
    .prepare(
      "SELECT r.* FROM artifact_replicas r JOIN storage_targets s ON s.id=r.storage_target_id WHERE s.kind='local'",
    )
    .get();
  assert.equal(local.deleted_at, 0);
  assert.equal(local.verified_at, null);
  assert.equal(
    f.db.prepare("SELECT COUNT(*) AS n FROM storage_targets WHERE kind='pikpak'").get().n,
    0,
  );
  row.r2.removedAt = 'unknown-original-removal-time';
  assert.throws(
    () => mapLegacyArtifacts(null, f.refs, metadata([row])),
    /proven source relationships/,
  );
});
