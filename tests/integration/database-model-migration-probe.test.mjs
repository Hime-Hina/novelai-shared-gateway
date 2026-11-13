import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createHash } from 'node:crypto';
import { migrateStorage } from '../../server/storage/sqlite-migrations.ts';
import { appendDomainCommit, readDomainCommits } from '../../server/events/event-store.ts';
import { domainDigest } from '../../server/events/domain-events.ts';
import { generationChargeResources } from '../../server/generation/accounting.ts';
import { parseAmount, formatAmount } from '../../server/quota/resource-amounts.ts';
import { parseRelationalFact } from '../../server/events/relational-events.ts';
import {
  appendRelationalCommit,
  readRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { databaseState } from '../helpers/relational-generation.mjs';

const at = '2026-11-01T00:00:00.000Z',
  now = Date.parse(at),
  sourceId = 'synthetic-A-probe';
// 只试验来源完整的旧终态开账；没有逐笔历史时不补造预扣/退款命令。
function convert(event) {
  assert.equal(event.eventType, 'OpeningStateRecorded');
  const { jobId, charge: c } = event.payload.state;
  assert.equal(event.streamKind, 'settlement');
  assert.equal(event.streamId, `settlement:${jobId}`);
  assert.ok(['settled', 'refunded'].includes(c.status));
  assert.ok(['v5', 'anlas'].includes(c.resource));
  assert.equal(c.billingMode, undefined);
  for (const value of [jobId, c.userId, c.accountId]) {
    assert.ok(typeof value === 'string' && value.length > 0);
  }
  for (const field of ['amount', 'fixed', 'purchased']) {
    assert.equal(formatAmount(parseAmount(c[field])), c[field]);
  }
  return {
    ...parseRelationalFact({
      eventType: 'OpeningChargeRecorded',
      payload: {
        originalJobId: jobId,
        originalMemberId: c.userId,
        originalAccountId: c.accountId,
        jobId: null,
        walletAccountId: null,
        walletMemberId: null,
        attemptId: null,
        status: c.status,
        resolution: 'archived',
        originRef: `${sourceId}:${event.commitSeq}:${event.eventIndex}`,
        resources: generationChargeResources(c),
        billingMode: c.resource,
        v5Equivalent: '0',
        sharedV5Reserved: '0',
        configVersion: c.configVersion,
        reservedAt: c.reservedAt,
        terminalAt: c.settledAt,
        reservationEventId: event.eventId,
        terminalEventId: event.eventId,
        origin: { sourceId, originalId: jobId },
      },
    }),
    eventId: event.eventId,
    streamId: `account:${c.accountId}`,
    occurredAt: Date.parse(event.occurredAt),
    accountId: c.accountId,
    memberId: c.userId,
    policyVersion: event.policyVersion,
    pricingVersion: event.pricingVersion,
  };
}

test('A 类别试验：旧目标已缺失的终态开账保持提交、原回执和精确资源，并可原子重复回放', (t) => {
  const source = new Database(':memory:'),
    converted = new Database(':memory:'),
    target = new Database(':memory:');
  t.after(() => {
    source.close();
    converted.close();
    target.close();
  });
  migrateStorage(source);
  const charges = [
    { resource: 'v5', amount: '0.000000000001', fixed: '0', purchased: '0', status: 'refunded' },
    {
      resource: 'anlas',
      amount: '9007199254740993.000000000003',
      fixed: '9007199254740993.000000000001',
      purchased: '0.000000000002',
      status: 'settled',
    },
  ];
  source.transaction(() =>
    appendDomainCommit(source, {
      commandId: 'old-opening-chunk',
      fingerprint: domainDigest('old-opening-chunk'),
      recordedAt: at,
      actorId: null,
      actorKind: 'system',
      correlationId: null,
      causationId: null,
      expectedVersions: { 'settlement:missing-0': '0', 'settlement:missing-1': '0' },
      receipt: { importId: 'original-import', chunkId: 'terminals-0' },
      events: charges.map((charge, index) => ({
        eventType: 'OpeningStateRecorded',
        eventId: `old-opening-${index}`,
        streamId: `settlement:missing-${index}`,
        occurredAt: new Date(now + index).toISOString(),
        payload: {
          source: 'legacy-opening',
          state: {
            jobId: `missing-${index}`,
            charge: {
              ...charge,
              userId: 'original-member',
              accountId: 'original-account',
              configVersion: 7,
              reservedAt: now - 1000,
              settledAt: now,
            },
          },
        },
      })),
    }),
  )();
  const [old] = readDomainCommits(source),
    facts = old.events.map(convert);
  for (const field of ['userId', 'accountId', 'amount', 'fixed', 'purchased']) {
    const broken = structuredClone(old.events[0]);
    delete broken.payload.state.charge[field];
    assert.throws(() => convert(broken));
  }
  assert.throws(() => convert({ ...old.events[0], eventType: 'LegacyFactRecorded' }));
  for (const db of [converted, target]) {
    initializeRelationalStorage(db, 'synthetic-A-history');
  }
  const metadata = {
    actorKind: 'system',
    actorId: null,
    recordedAt: now,
    correlationId: null,
    causationId: null,
  };
  converted.transaction(() =>
    appendRelationalCommit(converted, {
      ...metadata,
      commandId: 'register-source',
      fingerprint: domainDigest('register-source'),
      receipt: null,
      expectedVersions: { [`import:${sourceId}`]: '0' },
      events: [
        {
          streamId: `import:${sourceId}`,
          occurredAt: now,
          accountId: null,
          memberId: null,
          policyVersion: 'probe',
          pricingVersion: 'probe',
          eventType: 'ImportSourceRegistered',
          payload: {
            sourceId,
            sourceSha256: createHash('sha256').update(source.serialize()).digest('hex'),
            sourceCommit: 'c6d6245',
            cutoffSeq: old.commitSeq,
            cutoffAt: now,
            manifestSha256: domainDigest('synthetic-no-object-manifest'),
            route: 'A',
          },
        },
      ],
    }),
  )();
  converted.transaction(() =>
    appendRelationalCommit(converted, {
      ...metadata,
      commandId: old.commandId,
      fingerprint: old.fingerprint,
      receipt: old.receipt,
      recordedAt: Date.parse(old.recordedAt),
      expectedVersions: { 'account:original-account': '0' },
      events: facts,
    }),
  )();
  const owner = new RelationalGenerationStore(target, {}),
    stored = readRelationalCommit(converted, '2');
  target.transaction(() => owner.replayGenerationCommit(readRelationalCommit(converted, '1')))();
  const before = databaseState(target);
  target.exec(
    "CREATE TEMP TRIGGER fail_second BEFORE INSERT ON charges WHEN NEW.original_job_id='missing-1' AND EXISTS(SELECT 1 FROM charges WHERE original_job_id='missing-0') BEGIN SELECT RAISE(ABORT,'probe_second_charge'); END",
  );
  target.transaction(() =>
    assert.throws(() => owner.replayGenerationCommit(stored), /probe_second_charge/),
  )();
  assert.equal(databaseState(target), before);
  target.exec('DROP TRIGGER fail_second');
  target.transaction(() => owner.replayGenerationCommit(stored))();
  const after = target.serialize();
  target.transaction(() => assert.equal(owner.replayGenerationCommit(stored).replayed, true))();
  assert.ok(target.serialize().equals(after));
  assert.deepEqual(readRelationalCommit(target, '2'), stored);
  assert.equal(stored.command.recordedAt, Date.parse(old.recordedAt));
  assert.equal(stored.eventCount, old.events.length);
  assert.deepEqual(stored.command.receipt, old.receipt);
  assert.deepEqual(
    stored.events.map((e) => e.eventId),
    old.events.map((e) => e.eventId),
  );
  assert.deepEqual(
    stored.events.map((e) => e.occurredAt),
    old.events.map((e) => Date.parse(e.occurredAt)),
  );
  const rows = target.prepare('SELECT * FROM charges ORDER BY original_job_id').all();
  assert.equal(rows.length, 2);
  for (const [index, row] of rows.entries()) {
    assert.equal(row.original_member_id, 'original-member');
    assert.equal(row.original_account_id, 'original-account');
    assert.equal(row.reserved_at, now - 1000);
    assert.equal(row.terminal_at, now);
    assert.deepEqual(
      { v5: row.v5, anlasFixed: row.anlas_fixed, anlasPurchased: row.anlas_purchased },
      generationChargeResources(charges[index]),
    );
    assert.equal(row.status, charges[index].status);
    assert.equal(row.job_id, null);
  }
  assert.equal(source.prepare('SELECT COUNT(*) AS n FROM jobs').get().n, 0);
  for (const table of ['generation_jobs', 'members', 'wallets']) {
    assert.equal(target.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0);
  }
  assert.equal(target.pragma('foreign_keys', { simple: true }), 1);
  assert.deepEqual(target.pragma('foreign_key_check'), []);
  assert.throws(() => assertRelationalStorageReady(target), /incomplete/);
});
