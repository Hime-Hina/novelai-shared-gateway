import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { mkdtemp, mkdir, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import {
  normalizeMemberGeneration,
  materializeMemberGeneration,
} from '../../server/generation/member-contract.ts';
import { memberGenerationCapabilities } from '../../server/generation/capabilities.ts';
import {
  appendRelationalCommit,
  readRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import { relationalFactStream } from '../../server/events/relational-fact-contracts.ts';
import { domainDigest } from '../../server/events/domain-events.ts';
import { describeStoredResult, publishStoredResult } from '../../server/storage/result-files.ts';
import { parseAmount, formatAmount } from '../../server/quota/resource-amounts.ts';
import { quoteFairRequest } from '../../server/quota/fair-share.ts';

const NOW = 1800000000000,
  keys = { m1: 'synthetic-key-m1', m2: 'synthetic-key-m2' };
const image = await readFile(new URL('../fixtures/synthetic.webp', import.meta.url));
const zero = () => ({ v5: '0', anlasFixed: '0', anlasPurchased: '0' });
function databaseState(db) {
  return domainDigest(
    db
      .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
      .all()
      .map(({ name }) => [
        name,
        db
          .prepare(`SELECT * FROM "${name}" ORDER BY rowid`)
          .safeIntegers()
          .all()
          .map((row) =>
            Object.fromEntries(
              Object.entries(row).map(([key, value]) => [
                key,
                typeof value === 'bigint' ? String(value) : value,
              ]),
            ),
          ),
      ]),
  );
}
function meta(id, now = NOW, memberId = null) {
  return {
    commandId: id,
    fingerprint: domainDigest(id),
    actorKind: memberId ? 'member' : 'system',
    actorId: memberId,
    recordedAt: now,
    correlationId: null,
    causationId: null,
    policyVersion: 'shared-quota-v1',
    pricingVersion: 'price1',
  };
}
function openingFacts({
  v5 = '40',
  secondV5 = v5,
  fixed = '4000',
  purchased = '0',
  officialFixed = '8000',
  officialV5 = '80',
  officialPurchased = '0',
  fallback = false,
  freeLimit = null,
  seconds = 600,
  baseline = true,
} = {}) {
  const origin = { sourceId: 'synthetic-opening', originalId: 'original' };
  const facts = [
    {
      eventType: 'ImportSourceRegistered',
      payload: {
        sourceId: origin.sourceId,
        sourceSha256: 'a'.repeat(64),
        sourceCommit: '4161ea5',
        cutoffSeq: '2',
        cutoffAt: NOW,
        manifestSha256: 'b'.repeat(64),
        route: 'B',
      },
    },
    {
      eventType: 'OpeningAccountRecorded',
      payload: {
        accountId: 'a',
        displayName: 'shared',
        enabled: true,
        credentialEpoch: 1,
        cooldownUntil: null,
        cooldownReason: null,
        ordinaryStrikes: 0,
        capacityStrikes: 0,
        capacityLimitedUntil: null,
        capacityLastErrorAt: null,
        origin,
      },
    },
    {
      eventType: 'OpeningGroupRecorded',
      payload: { groupId: 'g', name: 'shared', share: 1, origin },
    },
    ...['m1', 'm2'].map((memberId) => ({
      eventType: 'OpeningGroupMemberRecorded',
      payload: { memberId, groupId: 'g', weight: 1, origin },
    })),
    {
      eventType: 'OpeningPolicyRecorded',
      payload: {
        enabled: true,
        boundAccountId: 'a',
        configVersion: 1,
        autoAnlasFallback: fallback,
        freePolicyVersion: 1,
        freeEnabled: freeLimit !== null,
        freeWarningThreshold: freeLimit,
        freeHardLimit: freeLimit,
        lossPolicyVersion: 1,
        lossV5_24h: 1,
        lossFixed24h: '100',
        lossFixed30d: '500',
        lossMemberAttempts24h: 3,
        origin,
      },
    },
    ...['m1', 'm2'].map((memberId) => ({
      eventType: 'OpeningWalletRecorded',
      payload: {
        accountId: 'a',
        memberId,
        available: {
          v5: memberId === 'm2' ? secondV5 : v5,
          anlasFixed: fixed,
          anlasPurchased: purchased,
        },
        origin,
      },
    })),
    {
      eventType: 'OpeningQuotaRecorded',
      eventId: 'opening-quota',
      payload: {
        accountId: 'a',
        status: 'active',
        observationEventId: 'opening-quota',
        observedAt: NOW,
        observedCredentialEpoch: 1,
        observedSequence: '9007199254740993',
        quotaTier: 3,
        subscriptionActive: true,
        subscriptionExpiresAt: null,
        quotaErrorCode: null,
        observedV5: officialV5,
        observedAnlasFixed: officialFixed,
        observedAnlasPurchased: officialPurchased,
        secondsPerPercent: seconds,
        baselineEventId: baseline ? 'opening-quota' : null,
        refillCursorAt: baseline ? NOW : null,
        settlementCursorAt: NOW,
        unallocatedV5: '0',
        fallbackFixed: '0',
        fallbackPurchased: '0',
        sharedV5Debt: '0',
        pendingV5Debit: '0',
        pendingFixedDebit: '0',
        pendingPurchasedDebit: '0',
        pendingFallbackV5: '0',
        pendingFallbackFixed: '0',
        pendingFallbackPurchased: '0',
        origin,
      },
    },
    {
      eventType: 'OpeningOperatingRecorded',
      payload: {
        accountId: 'a',
        available: zero(),
        unfundedFixed: '0',
        unfundedPurchased: '0',
        unattributedFixed: '0',
        unattributedPurchased: '0',
        v5CapacityGap: '0',
        fixedGapEventId: null,
        purchasedGapEventId: null,
        v5GapEventId: null,
        origin,
      },
    },
  ];
  return facts.map((f) => {
    const kind = relationalFactStream(f.eventType),
      p = f.payload;
    let streamId;
    if (kind === 'account') {
      streamId = 'account:a';
    } else if (kind === 'import') {
      streamId = `import:${p.sourceId}`;
    } else {
      streamId = 'sharing:default';
    }
    return {
      ...f,
      streamId,
      occurredAt: NOW,
      memberId: p.memberId ?? null,
      accountId: kind === 'account' ? 'a' : null,
      policyVersion: 'shared-quota-v1',
      pricingVersion: 'price1',
    };
  });
}
async function fixture(t, options = {}) {
  const dir = await mkdtemp(path.join(tmpdir(), 'nai-relational-generation-'));
  const db = new Database(path.join(dir, 'library.sqlite')),
    seed = new Database(':memory:');
  t.after(async () => {
    if (db.open) {
      db.close();
    }
    if (seed.open) {
      seed.close();
    }
    await rm(dir, { recursive: true, force: true });
  });
  await mkdir(path.join(dir, 'images'));
  initializeRelationalStorage(db, 'generation-history');
  initializeRelationalStorage(seed, 'generation-history');
  const members = new RelationalMemberStore(db),
    seedMembers = new RelationalMemberStore(seed),
    inputs = new RelationalInputStore(db),
    generations = new RelationalGenerationStore(db, options.env ?? {});
  for (const id of ['m1', 'm2']) {
    db.transaction(() =>
      members.registerMember(meta(`register-${id}`), {
        memberId: id,
        displayName: id,
        apiKey: keys[id],
      }),
    )();
  }
  for (const seq of ['1', '2']) {
    seed.transaction(() => seedMembers.replayMemberCommit(readRelationalCommit(db, seq)))();
  }
  const events = openingFacts(options);
  const { policyVersion: _policy, pricingVersion: _pricing, ...command } = meta('opening');
  seed.transaction(() =>
    appendRelationalCommit(seed, {
      ...command,
      expectedVersions: Object.fromEntries(events.map((e) => [e.streamId, '0'])),
      receipt: { opened: true },
      events,
    }),
  )();
  db.transaction(() => generations.replayGenerationCommit(readRelationalCommit(seed, '3')))();
  // 私有当前凭据不在开账事实中，测试显式模拟受保护快照恢复。
  db.prepare('INSERT INTO upstream_credentials(account_id,pst) VALUES(?,?)').run(
    'a',
    'synthetic-upstream-pst',
  );
  const controls = { dispatchEnabled: () => true, physicalAccountLoad: () => 0 };
  const accept = (
    id,
    {
      memberId = 'm1',
      samples = 1,
      model = 'nai-diffusion-4-5-full',
      steps = 30,
      allowPurchased = false,
      member = false,
      confirmation,
      editConfirmation = (c) => c,
      deadlineAt = null,
      queueExpiresAt = null,
      now = NOW,
    } = {},
  ) => {
    const normalized = normalizeMemberGeneration({
      operation: 'generate',
      model,
      input: 'private generation prompt',
      parameters: { width: 128, height: 128, steps, seed: 42, n_samples: samples },
      allowPurchasedAnlas: allowPurchased,
    });
    const confirmed =
      confirmation ??
      (member
        ? {
            requestFingerprint: normalized.requestFingerprint,
            versions: memberGenerationCapabilities(
              {
                settings: {
                  fairQuota: {
                    enabled: true,
                    autoAnlasFallback: options.fallback === true,
                    groups: [
                      { id: 'g', members: ['m1', 'm2'], share: 1, memberWeights: { m1: 1, m2: 1 } },
                    ],
                    freeImagePolicy: {
                      version: 1,
                      enabled: options.freeLimit != null,
                      warningThreshold: options.freeLimit ?? null,
                      hardLimit: options.freeLimit ?? null,
                    },
                  },
                },
              },
              undefined,
              { env: {} },
            ).versions,
            maxPerJob: { v5: '100', anlasFixed: '4000', anlasPurchased: '4000' },
            maxTotal: { v5: '100', anlasFixed: '4000', anlasPurchased: '4000' },
          }
        : null);
    const input = {
      inputId: `input-${id}`,
      requestId: id,
      memberId,
      source: member ? 'member-generation' : 'native',
      seedMode: 'fixed',
      confirmation: confirmed && editConfirmation(confirmed),
      request: materializeMemberGeneration(normalized),
      context: member
        ? { kind: 'member', member: normalized.normalizationContext }
        : { kind: 'native', native: normalized.normalizationContext.native },
      jobs: Array.from({ length: samples }, (_, i) => ({
        jobId: `${id}-${i}`,
        status: 'pending',
        queueSeq: null,
        queueExpiresAt,
        deadlineAt,
        admissionEpoch: 0,
        cacheKey: null,
      })),
    };
    return withAuthenticatedMemberApiKey(
      members,
      keys[memberId],
      () => generations.acceptGeneration(meta(`accept-${id}`, now, memberId), input),
      {},
    );
  };
  const dispatch = (id, now = NOW + 1, c = controls) =>
    db
      .transaction(() =>
        generations.dispatchGeneration(
          meta(`dispatch-${id}`, now),
          { jobId: id, accountId: 'a', attemptId: `attempt-${id}` },
          c,
        ),
      )
      .immediate();
  const response = (now = NOW + 3) => ({
    dispatchState: 'request_started',
    upstreamStartedAt: now - 2,
    upstreamHeadersAt: now - 1,
    upstreamCompletedAt: now,
    upstreamHttpStatus: 200,
  });
  const publish = async (id, now = NOW + 3) => {
    const descriptor = describeStoredResult(`artifact-${id}`, 'generate', {
      mimeType: 'image/webp',
      buffer: image,
    });
    db.transaction(() =>
      generations.prepareGenerationResult(
        meta(`prepare-${id}`, now),
        id,
        descriptor,
        response(now),
      ),
    )();
    assert.equal(db.inTransaction, false);
    await publishStoredResult(dir, descriptor, image);
    return db.transaction(() =>
      generations.completeGeneration(meta(`complete-${id}`, now + 1), id, descriptor),
    )();
  };
  const fail = (id, failure = {}, now = NOW + 2) =>
    db.transaction(() =>
      generations.failGeneration(meta(`fail-${id}`, now), id, {
        errorCode: 'upstream_unavailable',
        errorStatus: 503,
        ...failure,
      }),
    )();
  return {
    db,
    seed,
    dir,
    members,
    inputs,
    generations,
    accept,
    dispatch,
    response,
    publish,
    fail,
    controls,
  };
}

export { NOW, keys, image, zero, meta, openingFacts, databaseState, fixture };
