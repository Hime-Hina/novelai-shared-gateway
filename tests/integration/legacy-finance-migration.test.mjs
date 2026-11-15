import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { mkdtemp, readFile, writeFile, realpath, symlink, rm } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { JsonStore } from '../../server/storage/sqlite-store.ts';
import {
  configureFairQuota,
  syncFairQuota,
  reserveFairQuota,
  settleFairQuota,
  configureAnlasFallback,
  manageFairMember,
  manageOperatingFunding,
} from '../../server/quota/fair-share.ts';
import { recordAnlasGap } from '../../server/quota/operating-accounting.ts';
import { parseAmount } from '../../server/quota/resource-amounts.ts';
import { appendDomainCommit } from '../../server/events/event-store.ts';
import {
  domainDigest,
  beginDomainCommand,
  finishDomainCommand,
} from '../../server/events/domain-events.ts';
import {
  mapLegacyFinance,
  assertLegacyFinanceProjection,
} from '../../scripts/migrate-legacy-finance.mjs';
import {
  initializeRelationalStorage,
  assertRelationalStorageFormat,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { relationalFactStream } from '../../server/events/relational-fact-contracts.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { databaseState, installReady } from '../helpers/relational-generation.mjs';
import { relationalPrivateStateEmpty } from '../../server/storage/relational-private-restore.ts';

const NOW = 1800000000000,
  sourceId = 'synthetic-v4-finance',
  members = ['m1', 'm2', 'm3', 'm4'],
  origin = (originalId) => ({ sourceId, originalId }),
  eventId = (eventType, originalId) => domainDigest([sourceId, eventType, originalId]);

async function sourceFixture(
  t,
  { blockedLiabilities = false, huge = false, latestUnknownTime = false } = {},
) {
  const directory = await mkdtemp(path.join(tmpdir(), 'nai-finance-map-')),
    store = new JsonStore(directory);
  let source;
  await store.init();
  t.after(async () => {
    if (source?.open) {
      source.close();
    }
    if (store.sqlite?.open) {
      await store.close();
    }
    await rm(directory, { recursive: true, force: true });
  });
  const db = structuredClone(store.db);
  db.users = members.map((id) => ({ id, token: `synthetic-key-${id}`, enabled: true }));
  db.accounts = ['a', 'b'].map((id) => ({
    id,
    token: `synthetic-pst-${id}`,
    enabled: id === 'a',
    quotaTier: 3,
    subscriptionActive: true,
    quotaFixed: huge ? '9007199254740993.000000000003' : 10000,
    quotaPurchased: 400,
    v5UsagePercent: 40,
    quotaCheckedAt: NOW,
  }));
  configureFairQuota(db, members);
  syncFairQuota(db, NOW);
  configureAnlasFallback(db, true, 'synthetic-owner', NOW);
  assert.equal(db.settings.fairQuota.balances.m1.v5Available, '10');
  manageFairMember(
    db,
    {
      action: 'transfer-quota',
      expectedConfigVersion: db.settings.fairQuota.configVersion,
      fromUserId: 'm1',
      toUserId: 'm2',
      resource: 'v5',
      amount: 10,
    },
    NOW,
  );
  const request = { model: 'nai-diffusion-5-full', width: 1024, height: 1024, steps: 23 };
  const failed = {
    id: 'failed-review',
    memberId: 'm1',
    userToken: db.users[0].token,
    status: 'running',
    request,
    createdAt: new Date(NOW).toISOString(),
  };
  reserveFairQuota(db, db.users[0], failed, db.accounts[0], NOW + 1);
  settleFairQuota(db, failed, false, NOW + 2);
  failed.status = 'failed';
  failed.updatedAt = new Date(NOW + 2).toISOString();
  const retired = { id: 'retired-paid', memberId: 'm1', status: 'running', request };
  reserveFairQuota(db, db.users[0], retired, db.accounts[0], NOW + 3);
  settleFairQuota(db, retired, true, NOW + 4);
  if (blockedLiabilities) {
    manageOperatingFunding(
      db,
      {
        action: 'operating-register-funding',
        resource: 'anlasFixed',
        amount: 3,
        receipt: 'synthetic_receipt',
        destination: 'operating',
      },
      {
        actorId: 'm1',
        findEvent: (id) => db.ledger.find((event) => event.id === id),
        coveredAmount: () => '0',
      },
      NOW + 4,
    );
    const before = db.settings.fairQuota.snapshot,
      after = { ...before, at: NOW + 5, anlasFixed: '9998', anlas: '10398' };
    recordAnlasGap(db, 'anlasFixed', parseAmount('2'), before, after, NOW + 5);
  }
  db.jobs = [failed];
  // A later official observation can coexist with the earlier financial baseline.
  // These are explicit synthetic cut inputs, not claimed production old-history reconstruction.
  db.accounts[0].quotaFixed = 9900;
  db.accounts[0].quotaCheckedAt = latestUnknownTime ? undefined : NOW + 5;
  await store.write(db);
  await store.enableEventAuthority();

  const other = {
    users: db.users,
    accounts: [{ ...db.accounts[1], enabled: true }],
    settings: {},
    jobs: [],
    ledger: [],
  };
  configureFairQuota(other, members);
  const metadata = {
    commandId: 'synthetic-retained-unbound-finance',
    fingerprint: domainDigest('synthetic-retained-unbound-finance'),
    recordedAt: new Date(NOW + 6).toISOString(),
    actorKind: 'system',
    actorId: null,
    correlationId: null,
    causationId: null,
  };
  beginDomainCommand(other, metadata);
  syncFairQuota(other, NOW);
  const financialEvents = finishDomainCommand(other).events.filter(
    (event) => event.streamId === 'account:b',
  );
  store.sqlite.transaction(() =>
    appendDomainCommit(store.sqlite, {
      ...metadata,
      receipt: null,
      expectedVersions: {
        'account:b': store.sqlite
          .prepare(
            "SELECT CAST(stream_version AS TEXT) AS v FROM event_streams WHERE stream_id='account:b'",
          )
          .get().v,
      },
      events: financialEvents,
    }),
  )();
  await store.close();
  const filename = path.join(directory, 'library.sqlite'),
    sealed = new Database(filename);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  source = new Database(filename, { readonly: true, fileMustExist: true });
  source.pragma('query_only=ON');
  const references = {
    sourceId,
    origin,
    eventId,
    memberIds: new Set(members),
    accountIds: new Set(['a', 'b']),
    jobIds: new Set([failed.id]),
    attemptIds: new Set([failed.generationAttempt.id]),
    requestsByJob: new Map([[failed.id, 'retained-failed-request']]),
  };
  return { source, filename, references, failed, original: db.settings.fairQuota };
}

function event(fact) {
  const kind = relationalFactStream(fact.eventType),
    p = fact.payload;
  let streamId;
  if (kind === 'account') {
    streamId = `account:${fact.accountId ?? p.accountId}`;
  } else if (kind === 'request') {
    streamId = 'request:retained-failed-request';
  } else if (kind === 'member') {
    streamId = `member:${p.memberId}`;
  } else if (kind === 'input') {
    streamId = `input:${p.inputId}`;
  } else {
    streamId = `${kind}:${kind === 'import' ? p.sourceId : 'default'}`;
  }
  return {
    memberId: null,
    accountId: null,
    ...fact,
    streamId,
    occurredAt: NOW + 20,
    policyVersion: 'original-cut',
    pricingVersion: 'original-cut',
  };
}

function command(finance) {
  const publicFacts = [
    {
      eventType: 'ImportSourceRegistered',
      payload: {
        sourceId,
        sourceSha256: 'a'.repeat(64),
        sourceCommit: '6466c9b7dca10c11c82372aa92578379e9b1cda6',
        cutoffSeq: '1',
        cutoffAt: NOW + 10,
        manifestSha256: 'b'.repeat(64),
        route: 'B',
      },
    },
    {
      eventType: 'ImportSourceRegistered',
      payload: {
        sourceId: 'another-source',
        sourceSha256: 'c'.repeat(64),
        sourceCommit: '6466c9b7dca10c11c82372aa92578379e9b1cda6',
        cutoffSeq: '1',
        cutoffAt: NOW + 10,
        manifestSha256: 'd'.repeat(64),
        route: 'B',
      },
    },
    ...members.map((memberId) => ({
      eventType: 'OpeningMemberRecorded',
      memberId,
      payload: {
        memberId,
        displayName: memberId,
        status: 'active',
        acceptingJobs: true,
        admissionEpoch: 0,
        createdAt: NOW,
        origin: origin(memberId),
      },
    })),
    ...['a', 'b'].map((accountId) => ({
      eventType: 'OpeningAccountRecorded',
      accountId,
      payload: {
        accountId,
        displayName: accountId,
        weight: 1,
        enabled: false,
        credentialEpoch: 1,
        cooldownUntil: null,
        cooldownReason: null,
        ordinaryStrikes: 0,
        capacityStrikes: 0,
        capacityLimitedUntil: null,
        capacityLastErrorAt: null,
        origin: origin(accountId),
      },
    })),
    {
      eventType: 'OpeningInputRecorded',
      memberId: 'm1',
      payload: {
        inputId: 'retained-failed-input',
        memberId: 'm1',
        inputSchemaVersion: 1,
        completeness: 'unavailable',
        inputSha256: null,
        logicalBytes: 0,
        origin: origin('failed-review'),
      },
    },
    {
      eventType: 'OpeningRequestRecorded',
      memberId: 'm1',
      payload: {
        requestId: 'retained-failed-request',
        originalRequestId: null,
        memberId: 'm1',
        inputId: 'retained-failed-input',
        contextId: null,
        source: null,
        operation: null,
        model: null,
        sampleCount: null,
        seedMode: 'unknown',
        confirmation: null,
        acceptedAt: null,
        retiredAt: null,
        origin: origin('failed-review'),
      },
    },
    {
      eventType: 'OpeningJobRecorded',
      memberId: 'm1',
      payload: {
        jobId: 'failed-review',
        requestId: 'retained-failed-request',
        createdAt: NOW,
        sampleIndex: null,
        actualSeed: null,
        status: 'failed',
        queueSeq: null,
        queueExpiresAt: null,
        deadlineAt: null,
        admissionEpoch: null,
        artifactId: null,
        cacheKey: null,
        completedAt: NOW + 2,
        errorCode: 'synthetic-original-failure',
        errorStatus: 504,
        cancellationReason: null,
        lateResultAt: null,
        freeSucceededAt: null,
        freeWarningThreshold: null,
        retiredAt: null,
        origin: origin('failed-review'),
      },
    },
  ];
  const events = [...publicFacts, ...structuredClone(finance.facts)].map(event);
  return {
    commandId: 'opening-finance',
    fingerprint: domainDigest(events),
    actorKind: 'system',
    actorId: null,
    recordedAt: NOW + 20,
    correlationId: null,
    causationId: null,
    receipt: { imported: true },
    events,
    expectedVersions: Object.fromEntries(events.map((e) => [e.streamId, '0'])),
  };
}

function initializeTarget(db, finance) {
  initializeRelationalStorage(db, 'finance-history');
  const c = command(finance),
    genesisTypes = new Set([
      'ImportSourceRegistered',
      'OpeningMemberRecorded',
      'OpeningAccountRecorded',
    ]),
    genesis = c.events.filter((event) => genesisTypes.has(event.eventType));
  db.transaction(() =>
    new RelationalCommitReplayer(db).appendCommit({
      ...c,
      commandId: 'opening-identities',
      fingerprint: domainDigest(genesis),
      events: genesis,
      expectedVersions: Object.fromEntries(genesis.map((event) => [event.streamId, '0'])),
    }),
  )();
  c.events = c.events.filter((event) => !genesisTypes.has(event.eventType));
  c.fingerprint = domainDigest(c.events);
  c.expectedVersions = Object.fromEntries(
    c.events.map((event) => [
      event.streamId,
      db
        .prepare('SELECT CAST(stream_version AS TEXT) AS v FROM stream_heads WHERE stream_id=?')
        .get(event.streamId)?.v ?? '0',
    ]),
  );
  return c;
}

test('B finance preserves nonzero original pools, fallback debt, unbound account and distinct observations in one replayable commit', async (t) => {
  const f = await sourceFixture(t),
    before = await readFile(f.filename),
    mapped = f.source.transaction(() => mapLegacyFinance(f.source, f.references))();
  assert.deepEqual(mapped.findings, []);
  assert.equal(mapped.expected.wallets.length, 8);
  const a = mapped.expected.quotas.find((value) => value.accountId === 'a');
  assert.notEqual(a.observationEventId, a.baselineEventId);
  assert.equal(a.observedAt, NOW + 5);
  assert.equal(a.pendingFallbackFixed, f.original.spent.fallbackFixed);
  assert.equal(a.sharedV5Debt, f.original.sharedV5Debt);
  assert.notEqual(a.sharedV5Debt, '0');
  assert.equal(a.observedCredentialEpoch, null);
  assert.equal(mapped.expected.quotas.find((value) => value.accountId === 'b').observedAt, NOW);
  const inputStoreModule = await import('../../server/storage/relational-input-store.ts');
  const db = new Database(':memory:');
  t.after(() => db.close());
  const c = initializeTarget(db, mapped),
    inputs = new inputStoreModule.RelationalInputStore(db),
    replayer = new RelationalCommitReplayer(db);
  db.transaction(() => {
    inputs.stageOpeningInput({
      inputId: 'retained-failed-input',
      memberId: 'm1',
      completeness: 'unavailable',
      body: null,
      logicalBytes: 0,
      assets: [],
    });
    replayer.appendCommit(c, inputs);
  })();
  const q = new RelationalQuotaStore(db).readAccountQuota('a');
  db.transaction(() => assertLegacyFinanceProjection(db, mapped.expected))();
  assert.equal(q.baselineEventId, a.baselineEventId);
  assert.equal(q.observationEventId, a.observationEventId);
  assert.equal(
    db.prepare('SELECT review_status AS status FROM generation_attempts').get().status,
    'pending',
  );
  assert.equal(
    db.prepare("SELECT status FROM charges WHERE original_job_id='failed-review'").get().status,
    'refunded',
  );
  assert.equal(
    db.prepare("SELECT status FROM charges WHERE original_job_id='retired-paid'").get().status,
    'settled',
  );
  assert.equal(relationalPrivateStateEmpty(db), true);
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'finance-history');
  const replay = new RelationalCommitReplayer(target),
    first = readRelationalCommit(db, '1'),
    stored = readRelationalCommit(db, '2');
  target.transaction(() => replay.replayCommit(first))();
  target.transaction(() => replay.replayCommit(stored, inputs))();
  assert.equal(databaseState(target), databaseState(db));
  target.transaction(() => assertLegacyFinanceProjection(target, mapped.expected))();
  const changes = target.prepare('SELECT total_changes() AS n').get().n;
  target.transaction(() => replay.replayCommit(stored, inputs))();
  assert.equal(target.prepare('SELECT total_changes() AS n').get().n, changes);
  assert.deepEqual(await readFile(f.filename), before);
});

test('opening latest observation rejects cross-account/source, changed original values and order; outer catch rolls back everything', async (t) => {
  const f = await sourceFixture(t),
    mapped = f.source.transaction(() => mapLegacyFinance(f.source, f.references))();
  for (const [mutate, expectedError] of [
    [
      (c) => {
        const observation = c.events.find((e) => e.eventType === 'OpeningQuotaObservationRecorded');
        observation.accountId = 'b';
        observation.streamId = 'account:b';
        observation.payload.accountId = 'b';
      },
      /开账观察必须是同笔先行的同账户同登记来源/,
    ],
    [
      (c) => {
        c.events.find(
          (e) => e.eventType === 'OpeningQuotaObservationRecorded',
        ).payload.origin.sourceId = 'another-source';
      },
      /开账观察必须是同笔先行的同账户同登记来源/,
    ],
    [
      (c) => {
        c.events.find((e) => e.eventType === 'OpeningQuotaObservationRecorded').payload.anlasFixed =
          '1';
      },
      /开账最新观察与同源原字段或时点不符/,
    ],
    [
      (c) => {
        c.events.find(
          (e) => e.eventType === 'OpeningQuotaRecorded',
        ).payload.observedCredentialEpoch = 1;
      },
      /开账最新观察与同源原字段或时点不符/,
    ],
    [
      (c) => {
        const i = c.events.findIndex((e) => e.eventType === 'OpeningQuotaObservationRecorded'),
          j = c.events.findIndex((e) => e.eventType === 'OpeningQuotaRecorded');
        [c.events[i], c.events[j]] = [c.events[j], c.events[i]];
      },
      /开账观察必须是同笔先行的同账户同登记来源/,
    ],
  ]) {
    const db = new Database(':memory:');
    try {
      const { RelationalInputStore } =
          await import('../../server/storage/relational-input-store.ts'),
        c = initializeTarget(db, mapped),
        inputs = new RelationalInputStore(db),
        original = structuredClone(c),
        before = databaseState(db);
      mutate(c);
      db.transaction(() => {
        assert.throws(
          () =>
            db.transaction(() => {
              inputs.stageOpeningInput({
                inputId: 'retained-failed-input',
                memberId: 'm1',
                completeness: 'unavailable',
                body: null,
                logicalBytes: 0,
                assets: [],
              });
              new RelationalCommitReplayer(db).appendCommit(c, inputs);
            })(),
          expectedError,
        );
      })();
      assert.equal(databaseState(db), before);
      db.transaction(() => {
        inputs.stageOpeningInput({
          inputId: 'retained-failed-input',
          memberId: 'm1',
          completeness: 'unavailable',
          body: null,
          logicalBytes: 0,
          assets: [],
        });
        new RelationalCommitReplayer(db).appendCommit(original, inputs);
      })();
      assert.equal(db.prepare('SELECT COUNT(*) AS n FROM commits').get().n, 2);
    } finally {
      db.close();
    }
  }
});

test('actual last charge SQL failure rolls back the entire finance commit and protected staging, then healthy retry succeeds', async (t) => {
  const f = await sourceFixture(t),
    mapped = f.source.transaction(() => mapLegacyFinance(f.source, f.references))(),
    db = new Database(':memory:');
  t.after(() => db.close());
  const c = initializeTarget(db, mapped),
    before = databaseState(db),
    { RelationalInputStore } = await import('../../server/storage/relational-input-store.ts'),
    inputs = new RelationalInputStore(db),
    prepare = db.prepare;
  let reached = false;
  db.prepare = function (sql) {
    const statement = prepare.call(this, sql);
    if (sql.startsWith('INSERT INTO charges(')) {
      return {
        run(...args) {
          const result = statement.run(...args);
          if (args[0] === 'retired-paid') {
            assert.equal(prepare.call(db, 'SELECT COUNT(*) AS n FROM charges').get().n, 2);
            assert.equal(
              prepare.call(db, 'SELECT COUNT(*) AS n FROM account_quota_state').get().n,
              2,
            );
            assert.equal(prepare.call(db, 'SELECT COUNT(*) AS n FROM wallets').get().n, 8);
            reached = true;
            throw new Error('synthetic_finance_tail');
          }
          return result;
        },
      };
    }
    return statement;
  };
  db.transaction(() => {
    assert.throws(
      () =>
        db.transaction(() => {
          inputs.stageOpeningInput({
            inputId: 'retained-failed-input',
            memberId: 'm1',
            completeness: 'unavailable',
            body: null,
            logicalBytes: 0,
            assets: [],
          });
          new RelationalCommitReplayer(db).appendCommit(c, inputs);
        })(),
      /synthetic_finance_tail/,
    );
  })();
  db.prepare = prepare;
  assert.equal(reached, true);
  assert.equal(databaseState(db), before);
  db.transaction(() => {
    inputs.stageOpeningInput({
      inputId: 'retained-failed-input',
      memberId: 'm1',
      completeness: 'unavailable',
      body: null,
      logicalBytes: 0,
      assets: [],
    });
    new RelationalCommitReplayer(db).appendCommit(c, inputs);
  })();
  assert.equal(relationalPrivateStateEmpty(db), true);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM charges').get().n, 2);
});

test('above-2^53 original decimal pools and unknown latest time remain exact instead of becoming floats or timestamp0', async (t) => {
  const f = await sourceFixture(t, { huge: true, latestUnknownTime: true }),
    mapped = f.source.transaction(() => mapLegacyFinance(f.source, f.references))();
  assert.deepEqual(mapped.findings, []);
  const q = mapped.expected.quotas.find((value) => value.accountId === 'a'),
    original = f.original.balances.m1.anlasFixedAvailable,
    wallet = mapped.expected.wallets.find(
      (value) => value.accountId === 'a' && value.memberId === 'm1',
    );
  assert.equal(wallet.available.anlasFixed, original);
  assert.equal(
    mapped.expected.wallets
      .filter((value) => value.accountId === 'a')
      .reduce((sum, value) => sum + parseAmount(value.available.anlasFixed), 0n),
    Object.values(f.original.balances).reduce(
      (sum, value) => sum + parseAmount(value.anlasFixedAvailable),
      0n,
    ),
  );
  assert.equal(q.observedAt, null);
  assert.equal(
    mapped.expected.observations.find((value) => value.eventId === q.observationEventId).observedAt,
    null,
  );
  assert.equal(
    mapped.expected.observations.find((value) => value.eventId === q.baselineEventId).observedAt,
    NOW,
  );
  assert.equal(q.observedCredentialEpoch, null);
  assert.equal(q.observedSequence, null);
});

test('old-to-new reconciliation is query-only and rejects wrong pool, debt, deadline and an extra known-zero free success', async (t) => {
  const f = await sourceFixture(t),
    mapped = f.source.transaction(() => mapLegacyFinance(f.source, f.references))(),
    db = new Database(':memory:');
  t.after(() => db.close());
  const c = initializeTarget(db, mapped),
    { RelationalInputStore } = await import('../../server/storage/relational-input-store.ts'),
    inputs = new RelationalInputStore(db);
  db.transaction(() => {
    inputs.stageOpeningInput({
      inputId: 'retained-failed-input',
      memberId: 'm1',
      completeness: 'unavailable',
      body: null,
      logicalBytes: 0,
      assets: [],
    });
    new RelationalCommitReplayer(db).appendCommit(c, inputs);
  })();
  const before = databaseState(db),
    changes = db.prepare('SELECT total_changes() AS n').get().n;
  db.transaction(() => {
    db.pragma('query_only=ON');
    try {
      const checked = assertLegacyFinanceProjection(db, mapped.expected);
      assert.deepEqual(checked, {
        wallets: 8,
        quotas: 2,
        observations: 3,
        operating: 2,
        funding: 0,
        charges: 2,
        attempts: 1,
        freeUsage: 0,
      });
    } finally {
      db.pragma('query_only=OFF');
    }
  })();
  assert.equal(db.prepare('SELECT total_changes() AS n').get().n, changes);
  assert.equal(databaseState(db), before);
  for (const [sql, category] of [
    [
      "UPDATE wallets SET anlas_fixed_available='1' WHERE account_id='a' AND member_id='m1'",
      'wallets',
    ],
    ["UPDATE account_quota_state SET shared_v5_debt='0' WHERE account_id='a'", 'quotas'],
    ['UPDATE generation_attempts SET review_deadline_at=review_deadline_at+1', 'attempts'],
    ["UPDATE generation_jobs SET free_succeeded_at=0 WHERE id='failed-review'", 'freeUsage'],
  ]) {
    assert.throws(
      () =>
        db.transaction(() => {
          db.prepare(sql).run();
          assertLegacyFinanceProjection(db, mapped.expected);
        })(),
      new RegExp(`reconciliation mismatch: ${category}`),
    );
    assert.equal(databaseState(db), before);
  }
});

test('real old gap and pending funding remain explicit blocking responsibilities instead of disappearing or becoming new spendable money', async (t) => {
  const f = await sourceFixture(t, { blockedLiabilities: true }),
    before = await readFile(f.filename),
    mapped = f.source.transaction(() => mapLegacyFinance(f.source, f.references))();
  assert.deepEqual(mapped.findings.map((finding) => finding.code).sort(), [
    'funding_receipt_history_requires_opening_decision',
    'operating_gap_source_requires_opening_role',
  ]);
  assert.ok(mapped.findings.every((finding) => finding.blocking && finding.count > 0));
  assert.equal(mapped.expected.funding[0].remaining, '3');
  assert.equal(mapped.expected.funding[0].registeredAt, NOW + 4);
  assert.equal(
    mapped.facts.some(
      (fact) => fact.eventType === 'OpeningOperatingRecorded' && fact.accountId === 'a',
    ),
    false,
  );
  assert.equal(JSON.stringify(mapped.findings).includes('synthetic_receipt'), false);
  assert.equal(JSON.stringify(mapped).includes('synthetic-pst-'), false);
  assert.equal(JSON.stringify(mapped).includes('synthetic-key-'), false);
  assert.deepEqual(await readFile(f.filename), before);
});

test('exact6466 event7 reader refuses a legally installed event8 cut before ready access; new reader refuses exact old DDL', async (t) => {
  const old = JSON.parse(
      await readFile(new URL('../fixtures/relational-schema-v7-finance.json', import.meta.url)),
    ),
    dir = await mkdtemp(path.join(tmpdir(), 'nai-finance-format-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  assert.equal(old.sourceCommit, '6466c9b7dca10c11c82372aa92578379e9b1cda6');
  assert.equal(
    createHash('sha256').update(old.sql).digest('hex'),
    '057d74f270c29492ab799d485b54966ab5e4c944b42f9a6bd70346a331261f86',
  );
  assert.equal(
    createHash('sha256').update(old.storageSource).digest('hex'),
    '6c3a4951043af9c75c0198dc7ba8ab8a9058b6d1b63b52c98cc0d579e5579fb3',
  );
  // The old reader functions remain the exact original bytes. Only their dependency
  // module supplies the independently evaluated, frozen original SQL string.
  await writeFile(path.join(dir, 'package.json'), '{"type":"module"}');
  await writeFile(path.join(dir, 'relational-storage.ts'), old.storageSource);
  await writeFile(
    path.join(dir, 'relational-schema.ts'),
    `export const RELATIONAL_SCHEMA_SQL=${JSON.stringify(old.sql)};\n`,
  );
  await symlink(
    await realpath(new URL('../../node_modules', import.meta.url)),
    path.join(dir, 'node_modules'),
    'junction',
  );
  const oldReader = await import(pathToFileURL(path.join(dir, 'relational-storage.ts')).href),
    oldDb = new Database(':memory:'),
    db = new Database(':memory:');
  t.after(() => {
    oldDb.close();
    db.close();
  });
  oldReader.initializeRelationalStorage(oldDb, 'old-finance-history');
  oldReader.assertRelationalStorageFormat(oldDb);
  assert.throws(
    () => assertRelationalStorageFormat(oldDb),
    /Unsupported relational database format/,
  );
  const f = await sourceFixture(t),
    mapped = f.source.transaction(() => mapLegacyFinance(f.source, f.references))(),
    c = initializeTarget(db, mapped),
    { RelationalInputStore } = await import('../../server/storage/relational-input-store.ts'),
    inputs = new RelationalInputStore(db);
  db.transaction(() => {
    inputs.stageOpeningInput({
      inputId: 'retained-failed-input',
      memberId: 'm1',
      completeness: 'unavailable',
      body: null,
      logicalBytes: 0,
      assets: [],
    });
    new RelationalCommitReplayer(db).appendCommit(c, inputs);
  })();
  await installReady({ db, dir });
  assertRelationalStorageReady(db);
  const before = databaseState(db);
  assert.throws(
    () => oldReader.assertRelationalStorageReady(db),
    /Unsupported relational database format/,
  );
  assert.equal(databaseState(db), before);
});
