import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { existsSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import Database from 'better-sqlite3';
import {
  JsonStore,
  activateEventAuthority,
  verifyEventProjections,
  applyEventProjection,
} from '../../server/storage/sqlite-store.ts';
import {
  migrateStorage,
  SUPPORTED_EVENT_AUTHORITY_VERSION,
} from '../../server/storage/sqlite-migrations.ts';
import {
  canonicalJson,
  domainDigest,
  DOMAIN_REDUCER_VERSION,
} from '../../server/events/domain-events.ts';
import {
  importOpeningFacts,
  readStreamProjection,
  readDomainCommits,
  appendDomainCommit,
  copyProtectedFacts,
  replayDomainEvents,
} from '../../server/events/event-store.ts';

const at = '2026-11-01T00:00:00.000Z';
async function fixture(t, { running = false } = {}) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'nai-event-version-')),
    store = new JsonStore(dir);
  await store.init();
  t.after(async () => {
    await store.close();
    await rm(dir, { recursive: true, force: true });
  });
  const db = structuredClone(store.db);
  db.users = [
    {
      id: 'member',
      token: 'synthetic-secret',
      enabled: true,
      acceptingJobs: true,
      jobAdmissionVersion: 0,
    },
  ];
  db.images = [
    {
      id: 'unlinked',
      memberId: 'member',
      token: 'synthetic-secret',
      file: 'unlinked.png',
      createdAt: at,
    },
  ];
  if (running) {
    db.jobs = [
      {
        id: 'running',
        memberId: 'member',
        userToken: 'synthetic-secret',
        status: 'running',
        queueSeq: 1,
        createdAt: at,
        resultPending: { imageId: 'unpublished', file: 'pending.webp', preparedAt: at },
      },
    ];
  }
  await store.write(db);
  return store;
}
function markV1(sqlite, { authority = true } = {}) {
  sqlite.exec(
    'UPDATE event_streams SET reducer_version=1; UPDATE projection_versions SET reducer_version=1;',
  );
  if (authority) {
    const { value } = sqlite
      .prepare('SELECT CAST(value AS TEXT) AS value FROM event_clock WHERE id=1')
      .get();
    sqlite
      .prepare(
        "INSERT INTO app_meta(key,value) VALUES('event_authority',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
      )
      .run(JSON.stringify({ version: 1, minimumWriterVersion: 1, commitSeq: value }));
  }
}
function marker(sqlite) {
  return JSON.parse(
    sqlite.prepare("SELECT value FROM app_meta WHERE key='event_authority'").get().value,
  );
}
function immutableFacts(sqlite) {
  return Object.fromEntries(
    [
      'event_commits',
      'domain_events',
      'event_clock',
      'generation_requests',
      'artifact_inputs',
      'generation_diagnostics',
      'generation_idempotency',
      'fair_quota_settlements',
      'free_image_usage',
    ].map((table) => [table, sqlite.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all()]),
  );
}

test('v1 authority is rebuilt from its events before atomically requiring the current writer/reducer', async (t) => {
  const store = await fixture(t);
  await store.enableEventAuthority();
  await store.clearCachedImages({ ids: ['unlinked'] });
  const sqlite = store.sqlite;
  markV1(sqlite);
  // 模拟 v1 cache 未应用退休语义；单纯给旧 cache 改版本号会令图片复活。
  const old = JSON.parse(
    sqlite.prepare("SELECT projection FROM event_streams WHERE stream_id='artifact:unlinked'").get()
      .projection,
  );
  delete old.retired;
  sqlite
    .prepare(
      "UPDATE event_streams SET projection=?,projection_sha256=? WHERE stream_id='artifact:unlinked'",
    )
    .run(canonicalJson(old), domainDigest(old));
  const before = immutableFacts(sqlite),
    cursor = marker(sqlite).commitSeq;
  assert.throws(
    () => readStreamProjection(sqlite, 'artifact:unlinked'),
    /Unsupported reducer version/,
  );
  assert.deepEqual(activateEventAuthority(sqlite), { version: 3, commitSeq: cursor });
  assert.equal(SUPPORTED_EVENT_AUTHORITY_VERSION, 3);
  assert.equal(DOMAIN_REDUCER_VERSION, 3);
  assert.deepEqual(marker(sqlite), { version: 3, minimumWriterVersion: 3, commitSeq: cursor });
  assert.ok(readStreamProjection(sqlite, 'artifact:unlinked').projection.retired);
  assert.equal(sqlite.prepare("SELECT 1 FROM images WHERE id='unlinked'").get(), undefined);
  assert.deepEqual(sqlite.prepare('SELECT DISTINCT reducer_version FROM event_streams').all(), [
    { reducer_version: 3 },
  ]);
  assert.deepEqual(immutableFacts(sqlite), before);
  sqlite.transaction(() => verifyEventProjections(sqlite))();
});

test('startup upgrades already authoritative v1 running/resultPending work without dispatching or finishing it', async (t) => {
  const store = await fixture(t, { running: true });
  importOpeningFacts(store.sqlite);
  markV1(store.sqlite);
  const before = immutableFacts(store.sqlite),
    pending = store.selectItemById('jobs', 'running').resultPending,
    dir = store.dataDir;
  await store.close();
  const reopened = new JsonStore(dir);
  try {
    await reopened.init();
    assert.equal(marker(reopened.sqlite).version, 3);
    assert.equal(reopened.selectItemById('jobs', 'running').status, 'running');
    assert.deepEqual(reopened.selectItemById('jobs', 'running').resultPending, pending);
    assert.deepEqual(immutableFacts(reopened.sqlite), before);
    // 首次权威切换的原有运行中保护仍存在；版本升级已在此前成功。
    await assert.rejects(reopened.enableEventAuthority(), /Recover running attempts/);
  } finally {
    await reopened.close();
  }
});

test('unknown authority/reducer versions are rejected before verification, installation or new commits', async (t) => {
  const store = await fixture(t);
  await store.enableEventAuthority();
  const sqlite = store.sqlite,
    original = marker(sqlite);
  for (const value of [
    { version: 4, minimumWriterVersion: 4 },
    { version: 3, minimumWriterVersion: 4 },
    { version: 0, minimumWriterVersion: 0 },
    { version: 1 },
  ]) {
    sqlite
      .prepare("UPDATE app_meta SET value=? WHERE key='event_authority'")
      .run(JSON.stringify(value));
    const before = sqlite.serialize();
    assert.throws(() => migrateStorage(sqlite), /compatible writer|unsupported version/);
    assert.throws(() => activateEventAuthority(sqlite), /compatible writer|unsupported version/);
    assert.throws(
      () => sqlite.transaction(() => verifyEventProjections(sqlite))(),
      /compatible writer|unsupported version/,
    );
    assert.deepEqual(sqlite.serialize(), before);
  }
  sqlite
    .prepare("UPDATE app_meta SET value=? WHERE key='event_authority'")
    .run(JSON.stringify(original));
  sqlite.exec(
    'UPDATE event_streams SET reducer_version=4; UPDATE projection_versions SET reducer_version=4;',
  );
  const before = sqlite.serialize(),
    first = readDomainCommits(sqlite)[0];
  assert.throws(() => activateEventAuthority(sqlite), /Unsupported reducer version/);
  assert.throws(
    () => sqlite.transaction(() => verifyEventProjections(sqlite))(),
    /Unsupported reducer version/,
  );
  assert.throws(
    () =>
      sqlite.transaction(() =>
        appendDomainCommit(sqlite, { ...first, events: first.events, expectedVersions: {} }),
      )(),
    /Unsupported reducer version/,
  );
  assert.deepEqual(sqlite.serialize(), before);
});

test('failed v1 installation rolls back rebuilt caches, query rows, cursor and authority marker', async (t) => {
  const store = await fixture(t);
  await store.enableEventAuthority();
  const sqlite = store.sqlite;
  markV1(sqlite);
  // 缺失的查询行确实需要重装；不能依靠未变化行的冗余 UPSERT 触发故障。
  sqlite.prepare("DELETE FROM images WHERE id='unlinked'").run();
  const before = sqlite.serialize();
  sqlite.exec(
    "CREATE TEMP TRIGGER reject_version_install BEFORE INSERT ON images BEGIN SELECT RAISE(ABORT,'synthetic version install failure'); END",
  );
  assert.throws(() => activateEventAuthority(sqlite), /synthetic version install failure/);
  assert.deepEqual(sqlite.serialize(), before);
  assert.equal(marker(sqlite).version, 1);
  sqlite.exec('DROP TRIGGER reject_version_install');
  activateEventAuthority(sqlite);
  assert.equal(marker(sqlite).version, 3);
  assert.ok(sqlite.prepare("SELECT 1 FROM images WHERE id='unlinked'").get());
});

test('current replay upgrades a v1 checkpoint and keeps retired unlinked artifacts absent', async (t) => {
  const store = await fixture(t);
  await store.enableEventAuthority();
  await store.clearCachedImages({ ids: ['unlinked'] });
  const target = new Database(':memory:');
  t.after(() => target.close());
  migrateStorage(target);
  copyProtectedFacts(store.sqlite, target);
  const total = readDomainCommits(store.sqlite).length;
  replayDomainEvents(store.sqlite, target, {
    maxCommits: total - 1,
    project: applyEventProjection,
  });
  assert.ok(target.prepare("SELECT 1 FROM images WHERE id='unlinked'").get());
  markV1(target, { authority: false });
  const checkpoint = target
    .prepare("SELECT last_commit_seq FROM projection_versions WHERE name='core'")
    .get().last_commit_seq;
  // 即使暂不追加批次，已有 checkpoint 也必须先由当前 reducer 实际重建。
  assert.equal(
    replayDomainEvents(store.sqlite, target, { maxCommits: 0, project: applyEventProjection })
      .commits,
    0,
  );
  assert.equal(
    target.prepare("SELECT last_commit_seq FROM projection_versions WHERE name='core'").get()
      .last_commit_seq,
    checkpoint,
  );
  assert.equal(readStreamProjection(target, 'artifact:unlinked').reducerVersion, 3);
  replayDomainEvents(store.sqlite, target, { project: applyEventProjection });
  assert.equal(target.prepare("SELECT 1 FROM images WHERE id='unlinked'").get(), undefined);
  assert.deepEqual(readDomainCommits(target), readDomainCommits(store.sqlite));
  target.transaction(() => verifyEventProjections(target))();
});

test('v2 query-only response observations become sourced facts atomically; missing historical values stay absent', async (t) => {
  const store = await fixture(t, { running: true }),
    sqlite = store.sqlite;
  importOpeningFacts(sqlite);
  markV1(sqlite);
  sqlite.exec(
    'UPDATE event_streams SET reducer_version=2; UPDATE projection_versions SET reducer_version=2;',
  );
  sqlite
    .prepare("UPDATE app_meta SET value=? WHERE key='event_authority'")
    .run(JSON.stringify({ ...marker(sqlite), version: 2, minimumWriterVersion: 2 }));
  const job = JSON.parse(sqlite.prepare("SELECT data FROM jobs WHERE id='running'").get().data);
  Object.assign(job, {
    upstreamStartedAt: '2026-11-01T00:00:02.000Z',
    upstreamCompletedAt: '2026-11-01T00:00:14.000Z',
    upstreamHttpStatus: 200,
  });
  sqlite.prepare("UPDATE jobs SET data=? WHERE id='running'").run(JSON.stringify(job));
  sqlite.prepare("DELETE FROM images WHERE id='unlinked'").run();
  const original = immutableFacts(sqlite),
    before = sqlite.serialize(),
    dir = store.dataDir;
  sqlite.exec(
    "CREATE TEMP TRIGGER reject_response_upgrade BEFORE INSERT ON images BEGIN SELECT RAISE(ABORT,'synthetic response upgrade failure'); END",
  );
  assert.throws(() => activateEventAuthority(sqlite), /synthetic response upgrade failure/);
  assert.deepEqual(sqlite.serialize(), before);
  assert.equal(marker(sqlite).version, 2);
  sqlite.exec('DROP TRIGGER reject_response_upgrade');
  await store.close();
  const reopened = new JsonStore(dir);
  try {
    await reopened.init();
    const upgraded = reopened.sqlite;
    assert.equal(marker(upgraded).version, 3);
    assert.equal(marker(upgraded).minimumWriterVersion, 3);
    const events = readDomainCommits(upgraded).flatMap((commit) => commit.events),
      supplement = events.find((event) => event.eventType === 'ResponseEvidenceRecorded');
    assert.equal(supplement.payload.source, 'legacy-query-projection');
    assert.equal(supplement.payload.sourceRowSha256, domainDigest(job));
    assert.equal(supplement.payload.response.upstreamHeadersAt, null);
    assert.deepEqual(
      immutableFacts(upgraded).domain_events.slice(0, original.domain_events.length),
      original.domain_events,
    );
    const target = new Database(':memory:');
    try {
      migrateStorage(target);
      copyProtectedFacts(upgraded, target);
      replayDomainEvents(upgraded, target, { project: applyEventProjection });
      const rebuilt = JSON.parse(
        target.prepare("SELECT data FROM jobs WHERE id='running'").get().data,
      );
      for (const field of [
        'upstreamStartedAt',
        'upstreamHeadersAt',
        'upstreamCompletedAt',
        'upstreamHttpStatus',
        'resultPending',
        'status',
      ]) {
        assert.deepEqual(rebuilt[field], job[field], field);
      }
      assert.equal(rebuilt.upstreamHeadersAt, undefined);
      assert.equal(rebuilt.status, 'running');
      target.transaction(() => verifyEventProjections(target))();
    } finally {
      target.close();
    }
    const commits = readDomainCommits(upgraded).length;
    await reopened.close();
    await reopened.init();
    assert.equal(readDomainCommits(reopened.sqlite).length, commits);
  } finally {
    await reopened.close();
  }
});

test('replay rejects an old source before writing either database, then preserves observations after isolated startup upgrade', async (t) => {
  const store = await fixture(t, { running: true }),
    sqlite = store.sqlite;
  importOpeningFacts(sqlite);
  markV1(sqlite);
  sqlite.exec(
    'UPDATE event_streams SET reducer_version=2; UPDATE projection_versions SET reducer_version=2;',
  );
  sqlite
    .prepare("UPDATE app_meta SET value=? WHERE key='event_authority'")
    .run(JSON.stringify({ ...marker(sqlite), version: 2, minimumWriterVersion: 2 }));
  const job = store.selectItemById('jobs', 'running');
  Object.assign(job, {
    upstreamStartedAt: '2026-11-01T00:00:02.000Z',
    upstreamCompletedAt: '2026-11-01T00:00:14.000Z',
    upstreamHttpStatus: 200,
  });
  sqlite.prepare("UPDATE jobs SET data=? WHERE id='running'").run(JSON.stringify(job));
  const sourceBefore = sqlite.serialize(),
    target = new Database(':memory:');
  t.after(() => target.close());
  migrateStorage(target);
  const targetBefore = target.serialize();
  assert.throws(
    () => replayDomainEvents(sqlite, target, { project: applyEventProjection }),
    /ISOLATED COPY/,
  );
  assert.deepEqual(sqlite.serialize(), sourceBefore);
  assert.deepEqual(target.serialize(), targetBefore);
  const oldMarker = marker(sqlite);
  sqlite
    .prepare("UPDATE app_meta SET value=? WHERE key='event_authority'")
    .run(JSON.stringify({ ...oldMarker, version: 3, minimumWriterVersion: 3 }));
  assert.throws(
    () => replayDomainEvents(sqlite, target, { project: applyEventProjection }),
    /ISOLATED COPY/,
    'old reducer is rejected even if the writer marker is current',
  );
  sqlite
    .prepare("UPDATE app_meta SET value=? WHERE key='event_authority'")
    .run(JSON.stringify(oldMarker));
  sqlite.exec(
    'UPDATE event_streams SET reducer_version=3; UPDATE projection_versions SET reducer_version=3;',
  );
  assert.throws(
    () => replayDomainEvents(sqlite, target, { project: applyEventProjection }),
    /ISOLATED COPY/,
    'old writer marker is rejected even if reducer caches were relabelled',
  );
  sqlite.exec(
    'UPDATE event_streams SET reducer_version=2; UPDATE projection_versions SET reducer_version=2;',
  );
  const targetPath = path.join(store.dataDir, 'rejected-replay.sqlite');
  const cli = spawnSync(
    process.execPath,
    ['scripts/replay-events.mjs', `--source=${store.dbPath}`, `--target=${targetPath}`],
    { encoding: 'utf8' },
  );
  assert.notEqual(cli.status, 0);
  assert.match(cli.stderr, /ISOLATED COPY/);
  assert.equal(existsSync(targetPath), false);
  assert.deepEqual(sqlite.serialize(), sourceBefore);
  const dir = store.dataDir;
  await store.close();
  const upgraded = new JsonStore(dir);
  try {
    await upgraded.init();
    copyProtectedFacts(upgraded.sqlite, target);
    replayDomainEvents(upgraded.sqlite, target, { project: applyEventProjection });
    const replayed = JSON.parse(
      target.prepare("SELECT data FROM jobs WHERE id='running'").get().data,
    );
    for (const field of [
      'upstreamStartedAt',
      'upstreamHeadersAt',
      'upstreamCompletedAt',
      'upstreamHttpStatus',
    ]) {
      assert.deepEqual(replayed[field], job[field], field);
    }
    assert.equal(
      readDomainCommits(upgraded.sqlite)
        .flatMap((commit) => commit.events)
        .filter((event) => event.eventType === 'ResponseEvidenceRecorded').length,
      1,
    );
    target.transaction(() => verifyEventProjections(target))();
  } finally {
    await upgraded.close();
  }
});
