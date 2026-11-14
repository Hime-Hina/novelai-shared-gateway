import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { fixture, meta, NOW, databaseState } from '../helpers/relational-generation.mjs';
import { initializeFixtureConfiguration } from '../helpers/relational-configuration.mjs';
import { domainDigest } from '../../server/events/domain-events.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { relationalFactStream } from '../../server/events/relational-fact-contracts.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { accountQuotaResult } from '../../server/quota/account-refresh.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { relationalSnapshotSha256 } from '../../server/storage/relational-private-restore.ts';

const origin = { sourceId: 'synthetic-opening', originalId: 'old-pending-receipt' };
const targetId = `local:${domainDigest('images')}`;
function event(eventType, payload, occurredAt = NOW) {
  const kind = relationalFactStream(eventType),
    identities = {
      import: payload.sourceId,
      member: payload.memberId,
      account: payload.accountId,
      storage: payload.storageTargetId,
    };
  return {
    eventType,
    payload,
    streamId: `${kind}:${identities[kind]}`,
    occurredAt,
    memberId: payload.memberId ?? null,
    accountId: payload.accountId ?? null,
    policyVersion: 'known-opening-policy',
    pricingVersion: 'known-opening-price',
  };
}
function command(db, id, events) {
  const { policyVersion: _policy, pricingVersion: _price, ...metadata } = meta(id);
  return {
    ...metadata,
    expectedVersions: Object.fromEntries(
      events.map((e) => [
        e.streamId,
        db
          .prepare(
            'SELECT CAST(stream_version AS TEXT) AS version FROM stream_heads WHERE stream_id=?',
          )
          .get(e.streamId)?.version ?? '0',
      ]),
    ),
    receipt: { opened: id },
    events,
  };
}
const funding = (changes = {}) =>
  event('OpeningFundingRecorded', {
    receipt: 'pending-external-receipt',
    accountId: 'a',
    resource: 'anlasFixed',
    destination: 'operating',
    remaining: '10',
    registeredAt: NOW - 1,
    snapshotAt: null,
    origin,
    ...changes,
  });
const storage = (changes = {}) =>
  event('OpeningStorageTargetRecorded', {
    storageTargetId: targetId,
    kind: 'local',
    namespace: 'images',
    credentialRef: null,
    origin: { ...origin, originalId: 'old-images-namespace' },
    ...changes,
  });
const registered = (sourceId = 'source', occurredAt = NOW) =>
  event(
    'ImportSourceRegistered',
    {
      sourceId,
      sourceSha256: 'a'.repeat(64),
      sourceCommit: 'known-synthetic-commit',
      cutoffSeq: '0',
      cutoffAt: 0,
      manifestSha256: 'b'.repeat(64),
      route: 'A',
    },
    occurredAt,
  );
const member = (sourceId = 'source') =>
  event(
    'OpeningMemberRecorded',
    {
      memberId: 'historical-member',
      displayName: 'historical',
      status: 'disabled',
      acceptingJobs: false,
      admissionEpoch: 0,
      createdAt: 0,
      origin: { sourceId, originalId: 'original-user-id' },
    },
    0,
  );
const account = (sourceId = 'source') =>
  event(
    'OpeningAccountRecorded',
    {
      accountId: 'historical-account',
      displayName: 'historical',
      enabled: false,
      credentialEpoch: 1,
      cooldownUntil: null,
      cooldownReason: null,
      ordinaryStrikes: 0,
      capacityStrikes: 0,
      capacityLimitedUntil: null,
      capacityLastErrorAt: null,
      origin: { sourceId, originalId: 'original-account-id' },
    },
    0,
  );
function empty(t) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  return db;
}

test('共用来源规则拒绝缺失、同笔晚登记、跨来源及重复登记；原Member/Gen owner均不能绕过', async (t) => {
  for (const makeOpening of [member, account]) {
    for (const kind of ['missing', 'late', 'different', 'duplicate']) {
      await t.test(`${makeOpening.name}/${kind}`, () => {
        const db = empty(t),
          replay = new RelationalCommitReplayer(db);
        let events;
        if (kind === 'missing') {
          events = [makeOpening()];
        }
        if (kind === 'late') {
          events = [makeOpening(), registered()];
        }
        if (kind === 'different') {
          events = [registered('other-source'), makeOpening()];
        }
        if (kind === 'duplicate') {
          events = [registered(), registered(), makeOpening()];
        }
        const before = databaseState(db);
        db.exec('CREATE TEMP TABLE outer_commit_survives(value INTEGER)');
        db.transaction(() => {
          assert.throws(() => replay.appendCommit(command(db, kind, events)), /来源/);
          db.prepare('INSERT INTO outer_commit_survives VALUES(1)').run();
        })();
        assert.equal(databaseState(db), before);
        assert.equal(db.prepare('SELECT COUNT(*) AS n FROM outer_commit_survives').get().n, 1);
      });
    }
  }
});

test('来源按完整提交/索引先行，时间回退和已知零保留；后续重复登记拒绝且原来源仍可用', (t) => {
  const db = empty(t),
    replay = new RelationalCommitReplayer(db);
  db.transaction(() =>
    replay.appendCommit(command(db, 'source', [registered('source', NOW + 100)])),
  )();
  const before = databaseState(db);
  db.transaction(() =>
    assert.throws(
      () => replay.appendCommit(command(db, 'duplicate-source', [registered()])),
      /来源/,
    ),
  )();
  assert.equal(databaseState(db), before);
  const opened = db.transaction(() =>
    replay.appendCommit(command(db, 'historical', [member(), account()])),
  )();
  assert.equal(db.prepare('SELECT created_at,auth_epoch FROM members').get().created_at, 0);
  assert.equal(db.prepare('SELECT auth_epoch FROM members').get().auth_epoch, null);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
  assert.equal(
    readRelationalCommit(db, opened.commitSeq).events[0].payload.origin.originalId,
    'original-user-id',
  );
});

test('Funding与Target同笔恢复原值，无钱包贷记；第二owner已写故障被外catch后全表回滚且可重试', async (t) => {
  const f = await fixture(t, { seconds: 0 }),
    replay = new RelationalCommitReplayer(f.db),
    saved = command(f.db, 'funding-and-target', [funding(), storage()]);
  let sawBoth = false;
  f.db.function('opening_second_owner', (fundings, targets) => {
    sawBoth = fundings === 1 && targets === 1;
    return 1;
  });
  f.db.exec(
    "CREATE TEMP TRIGGER fail_opening_target AFTER INSERT ON storage_targets BEGIN SELECT opening_second_owner((SELECT COUNT(*) FROM pending_funding),(SELECT COUNT(*) FROM storage_targets)); SELECT RAISE(ABORT,'opening second owner failure'); END",
  );
  const before = databaseState(f.db);
  f.db.exec('CREATE TEMP TABLE outer_commit_survives(value INTEGER)');
  f.db.transaction(() => {
    assert.throws(() => replay.appendCommit(saved), /opening second owner failure/);
    f.db.prepare('INSERT INTO outer_commit_survives VALUES(1)').run();
  })();
  assert.equal(sawBoth, true);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM outer_commit_survives').get().n, 1);
  f.db.exec('DROP TRIGGER fail_opening_target');
  const wallets = f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(),
    quota = f.db.prepare('SELECT * FROM account_quota_state').get(),
    result = f.db.transaction(() => replay.appendCommit(saved))();
  assert.deepEqual(f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(), wallets);
  assert.deepEqual(f.db.prepare('SELECT * FROM account_quota_state').get(), quota);
  const row = f.db.prepare('SELECT * FROM pending_funding').get();
  assert.equal(row.remaining, '10');
  assert.equal(row.registered_at, NOW - 1);
  assert.equal(row.snapshot_at, null);
  assert.equal(
    row.source_event_id,
    result.events.find((e) => e.eventType === 'OpeningFundingRecorded').eventId,
  );
  assert.equal(new RelationalReplicaStore(f.db).readStorageTarget(targetId).namespace, 'images');
  const state = databaseState(f.db);
  assert.equal(f.db.transaction(() => replay.appendCommit(saved))().replayed, true);
  assert.equal(databaseState(f.db), state);
});

test('开账pending收据由原source fact解释，随机eventId不被拆成receipt；新观察只消费剩余额一次', async (t) => {
  const f = await fixture(t, { seconds: 0 }),
    replay = new RelationalCommitReplayer(f.db),
    opened = f.db.transaction(() =>
      replay.appendCommit(command(f.db, 'opening-pending', [funding()])),
    )();
  assert.notEqual(opened.events[0].eventId, 'operating-receipt:pending-external-receipt');
  const quotas = new RelationalQuotaStore(f.db),
    ticket = quotas.beginQuotaObservation('a', NOW + 10),
    observed = accountQuotaResult(
      'a',
      {
        tier: 3,
        subscriptionActive: true,
        subscriptionExpiresAt: null,
        points: 0,
        fixed: '8010',
        purchased: '0',
        v5UsagePercent: '80',
        v5UsageTimeUntilNextPercent: 0,
      },
      new Date(NOW + 10).toISOString(),
    ),
    observedCommand = meta('observe-opening-funding', NOW + 11),
    result = f.db.transaction(() =>
      quotas.recordQuotaObservation(observedCommand, ticket, observed),
    )();
  const received = result.events.find((e) => e.eventType === 'FundingReceived');
  assert.equal(received.payload.receipt, 'pending-external-receipt');
  assert.equal(received.payload.receiptEventId, opened.events[0].eventId);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM pending_funding').get().n, 0);
  assert.equal(
    f.db.prepare('SELECT anlas_fixed_available AS amount FROM operating_state').get().amount,
    '10',
  );
  const before = databaseState(f.db);
  f.db.transaction(() => quotas.recordQuotaObservation(observedCommand, ticket, observed))();
  assert.equal(databaseState(f.db), before);
  let network = 0;
  t.mock.method(globalThis, 'fetch', () => {
    network++;
    throw new Error('Opening replay must not fetch.');
  });
  const target = empty(t),
    recover = new RelationalCommitReplayer(target);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => recover.replayCommit(readRelationalCommit(f.db, seq)))();
  }
  for (const table of [
    'pending_funding',
    'operating_state',
    'account_quota_state',
    'wallets',
    'commits',
    'domain_events',
  ]) {
    assert.deepEqual(
      target.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
      f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
      table,
    );
  }
  assert.equal(network, 0);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
});

test('OpeningFunding保留有依据的零和缺省NULL；非法目标不被当前配置补全', async (t) => {
  const f = await fixture(t, { seconds: 0 }),
    replay = new RelationalCommitReplayer(f.db);
  f.db.transaction(() =>
    replay.appendCommit(
      command(f.db, 'known-zero', [funding({ remaining: '0', registeredAt: 0, snapshotAt: 0 })]),
    ),
  )();
  const row = f.db.prepare('SELECT remaining,registered_at,snapshot_at FROM pending_funding').get();
  assert.deepEqual(row, { remaining: '0', registered_at: 0, snapshot_at: 0 });
  const before = databaseState(f.db);
  const wrong = storage({
    storageTargetId: `local:${domainDigest('wrong-directory')}`,
    namespace: 'wrong-directory',
  });
  f.db.transaction(() =>
    assert.throws(
      () => replay.appendCommit(command(f.db, 'wrong-target', [wrong])),
      /规范|来源|目标/,
    ),
  )();
  assert.equal(databaseState(f.db), before);
});

test('新Opening不能进入合法ready，原完整提交重复仍零写', async (t) => {
  const f = await fixture(t, { seconds: 0 }),
    replay = new RelationalCommitReplayer(f.db),
    opened = command(f.db, 'before-ready', [funding(), storage()]);
  f.db.transaction(() => replay.appendCommit(opened))();
  const sourcePath = path.join(f.dir, 'selected-opening.sqlite');
  initializeFixtureConfiguration(f.db);
  await f.db.backup(sourcePath);
  const source = new Database(sourcePath);
  source.pragma('journal_mode=DELETE');
  source.close();
  await verifyAndInstallRelationalStorage(f.db, {
    sourcePath,
    sourceSha256: relationalSnapshotSha256(sourcePath),
    historyId: 'generation-history',
    finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get()
      .seq,
    replicas: { dataDir: f.dir },
  });
  const before = databaseState(f.db);
  f.db.transaction(() =>
    assert.throws(
      () =>
        replay.appendCommit(
          command(f.db, 'after-ready', [funding({ receipt: 'new-after-ready' })]),
        ),
      /offline building/,
    ),
  )();
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.transaction(() => replay.appendCommit(opened))().replayed, true);
  assert.equal(databaseState(f.db), before);
});
