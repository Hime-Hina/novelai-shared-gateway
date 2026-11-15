import { readStreamProjection } from '../server/events/event-store.ts';
import { accountQuotaEventTypes } from '../server/events/quota-projections.ts';
import { parseRelationalFact } from '../server/events/relational-events.ts';
import { canonicalJson } from '../server/events/domain-events.ts';
import {
  generationChargeResources,
  generationResponseEvidence,
} from '../server/generation/accounting.ts';
import { readOperatingAccount } from '../server/quota/operating-accounting.ts';
import { parseAmount, formatAmount } from '../server/quota/resource-amounts.ts';
import { fairQuotaSnapshot } from '../server/quota/fair-share.ts';
import { quotaNumber, quotaTimestamp } from '../server/quota/eligibility.ts';
import {
  financialObservedV5,
  initialAccountQuota,
  RelationalQuotaStore,
} from '../server/storage/relational-quota-store.ts';
import { readRelationalOpeningAttempt } from '../server/storage/relational-generation-sources.ts';

const resources = ['v5', 'anlasFixed', 'anlasPurchased'];
const object = (value) => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError('Invalid legacy finance source.');
  }
  return value;
};
const amount = (value, signed = false) => {
  if (typeof value !== 'string' || formatAmount(parseAmount(value)) !== value) {
    throw new TypeError('Finance migration requires canonical current-v4 amounts.');
  }
  if (!signed && parseAmount(value) < 0n) {
    throw new RangeError('Negative legacy liability amount.');
  }
  return value;
};
const amounts = (value, signed = false) =>
  Object.fromEntries(
    resources.map((resource) => [resource, amount(object(value)[resource], signed)]),
  );
// V4 normalized the current cut, but retained Number leaves in immutable attempt/terminal JSON.
// Decode each original decision with the same nearest-decimal12 owner; never round live balances.
const historicalAmount = (value) => {
  if (typeof value === 'number' && value < 0) {
    throw new RangeError('Negative legacy liability amount.');
  }
  return amount(typeof value === 'number' ? formatAmount(parseAmount(value, 'nearest')) : value);
};
const historicalAmounts = (value) =>
  Object.fromEntries(
    resources.map((resource) => [resource, historicalAmount(object(value)[resource])]),
  );
const time = (value) => {
  if (value == null) {
    return null;
  }
  const result = typeof value === 'string' ? Date.parse(value) : value;
  if (
    !Number.isSafeInteger(result) ||
    result < 0 ||
    (typeof value === 'string' && new Date(result).toISOString() !== value)
  ) {
    throw new TypeError('Invalid known legacy finance time.');
  }
  return result;
};
const requiredTime = (value) => {
  const result = time(value);
  if (result === null) {
    throw new TypeError('Missing required original finance time.');
  }
  return result;
};
const json = (value) => (value === null ? null : JSON.parse(value));

// The caller owns one sealed readonly snapshot and the single complete import commit.
// This mapper neither initializes/migrates the source nor appends or settles anything.
export function mapLegacyFinance(source, references) {
  if (
    !source.readonly ||
    !source.inTransaction ||
    source.pragma('query_only', { simple: true }) !== 1
  ) {
    throw new Error(
      'Finance mapping requires the owning readonly query-only snapshot transaction.',
    );
  }
  const { memberIds, accountIds, jobIds, attemptIds, requestsByJob } = references;
  if (
    ![memberIds, accountIds, jobIds, attemptIds].every((value) => value instanceof Set) ||
    !(requestsByJob instanceof Map) ||
    typeof references.origin !== 'function' ||
    typeof references.eventId !== 'function'
  ) {
    throw new TypeError('Finance mapping requires the shared original-identity references.');
  }
  const facts = [],
    findings = new Map(),
    charges = new Map(),
    jobs = new Map(),
    mappedAttempts = new Set(),
    walletKeys = new Set(),
    fundingReceipts = new Set(),
    expected = {
      wallets: [],
      observations: [],
      quotas: [],
      operating: [],
      gaps: [],
      receipts: [],
      funding: [],
      charges: [],
      attempts: [],
      freeUsage: [],
    };
  const block = (code) => findings.set(code, (findings.get(code) ?? 0) + 1);
  const collect = (code, run) => {
    try {
      run();
    } catch {
      // No raw rows, credentials, body, amount or source IDs enter public findings.
      block(code);
    }
  };
  const eventId = (eventType, originalId) => references.eventId(eventType, originalId);
  const emit = (eventType, originalId, payload, memberId, accountId, originId = originalId) => {
    const fact = parseRelationalFact({
      eventType,
      payload: { ...payload, origin: references.origin(originId) },
    });
    if (fact.payload.origin.sourceId !== references.sourceId) {
      throw new Error('The financial origin is not the selected import source.');
    }
    const result = { ...fact, eventId: eventId(eventType, originalId), memberId, accountId };
    facts.push(result);
    return result;
  };
  const saveCharge = (jobId, charge, accountId) => {
    object(charge);
    if (
      charge.accountId !== accountId ||
      (charges.has(jobId) && canonicalJson(charges.get(jobId)) !== canonicalJson(charge))
    ) {
      throw new Error('Conflicting authoritative charge.');
    }
    charges.set(jobId, charge);
  };

  const officialOpening = (accountId, projection) => {
    const observedAt = time(projection.quotaCheckedAt),
      official = fairQuotaSnapshot(projection),
      latest = {
        accountId,
        credentialEpoch: null,
        sequence: null,
        observedAt,
        v5UsagePercent: quotaNumber(projection.v5UsagePercent),
        v5UsageIsNegative:
          typeof projection.v5UsageIsNegative === 'boolean' ? projection.v5UsageIsNegative : null,
        v5: financialObservedV5(projection, official),
        anlasFixed: official.anlasFixed,
        anlasPurchased: official.anlasPurchased,
        secondsPerPercent: official.seconds,
      },
      hasLatest =
        observedAt !== null ||
        ['v5UsagePercent', 'quotaFixed', 'quotaPurchased'].some(
          (field) => projection[field] != null,
        );
    let observationEventId = null;
    if (hasLatest) {
      observationEventId = emit(
        'OpeningQuotaObservationRecorded',
        canonicalJson([accountId, 'latest-observation']),
        latest,
        null,
        accountId,
        accountId,
      ).eventId;
      expected.observations.push({ eventId: observationEventId, ...latest });
    }
    return {
      latest,
      fields: {
        observationEventId,
        observedAt,
        observedCredentialEpoch: null,
        observedSequence: null,
        quotaTier: quotaNumber(projection.quotaTier),
        subscriptionActive: projection.subscriptionActive ?? null,
        subscriptionExpiresAt: quotaTimestamp(projection.subscriptionExpiresAt),
        quotaErrorCode: projection.quotaError || null,
        v5UsagePercent: latest.v5UsagePercent,
        v5UsageIsNegative: latest.v5UsageIsNegative,
        observedV5: latest.v5,
        observedAnlasFixed: latest.anlasFixed,
        observedAnlasPurchased: latest.anlasPurchased,
        secondsPerPercent: latest.secondsPerPercent,
      },
    };
  };
  // A missing cache field is not proof of a never-opened financial account.
  // Only IDs are retained transiently; original monetary/history facts are not replayed here.
  const financialAccounts = new Set(),
    quotaTypes = new Set(accountQuotaEventTypes);
  for (const row of source
    .prepare(
      `SELECT DISTINCT stream_id,account_id,event_type,
      json_extract(payload,'$.kind') AS kind,json_type(payload,'$.state.quota') AS openingQuota
      FROM domain_events WHERE stream_kind='account'`,
    )
    .iterate()) {
    if (
      (quotaTypes.has(row.event_type) &&
        !(row.event_type === 'QuotaObserved' && row.kind === 'official') &&
        !(row.event_type === 'AvailabilityChanged' && row.kind !== 'quota')) ||
      (row.event_type === 'OpeningStateRecorded' && row.openingQuota !== null)
    ) {
      financialAccounts.add(row.stream_id.slice('account:'.length));
      if (row.account_id !== null) financialAccounts.add(row.account_id);
    }
  }
  const linkedAccounts = [
    `SELECT DISTINCT account_id AS accountId,json_extract(data,'$.accountId') AS originalAccountId,
       json_extract(data,'$.fairCharge.accountId') AS chargeAccountId,
       json_extract(data,'$.generationAttempt.accountId') AS attemptAccountId FROM jobs`,
    `SELECT DISTINCT json_extract(charge,'$.accountId') AS accountId FROM fair_quota_settlements`,
    `SELECT DISTINCT account_id AS accountId,json_extract(data,'$.accountId') AS originalAccountId FROM ledger`,
    `SELECT DISTINCT json_extract(payload,'$.state.charge.accountId') AS accountId FROM domain_events
       WHERE event_type='OpeningStateRecorded' AND stream_kind='settlement'
     UNION SELECT DISTINCT json_extract(payload,'$.record.accountId') AS accountId FROM domain_events
       WHERE event_type='LegacyFactRecorded' AND stream_kind='legacy-ledger'`,
  ];
  for (const sql of linkedAccounts) {
    for (const row of source.prepare(sql).iterate()) {
      for (const id of Object.values(row)) {
        if (typeof id === 'string' && id) financialAccounts.add(id);
      }
    }
  }
  const boundAccountId = readStreamProjection(source, 'sharing:default')?.projection
    ?.boundAccountId;
  if (typeof boundAccountId === 'string') financialAccounts.add(boundAccountId);
  for (const row of source
    .prepare(
      `SELECT id,account_id,source,data FROM ledger WHERE source IN (
    'operating_funding_registered','operating_funding_received','operating_loss_covered',
    'resource_gap_reconciled','operating_debits_confirmed') ORDER BY id`,
    )
    .iterate()) {
    collect('invalid_funding_receipt_source', () => {
      const original = object(json(row.data)),
        accountId = row.account_id;
      if (
        !accountIds.has(accountId) ||
        original.accountId !== accountId ||
        original.type !== row.source ||
        original.id !== row.id ||
        !/^operating-receipt:[A-Za-z0-9_-]{8,100}$/.test(row.id)
      ) {
        throw new Error('The funding receipt lacks its original business identity.');
      }
      const request = original.request,
        requestKeys = [
          'action',
          'actorId',
          'resource',
          'amount',
          'destination',
          'fromUnattributed',
          'lossEventId',
        ],
        knownRequest =
          request &&
          typeof request === 'object' &&
          !Array.isArray(request) &&
          Object.keys(request).length === requestKeys.length &&
          requestKeys.every((key) => Object.hasOwn(request, key))
            ? request
            : null;
      const decisions = source
          .prepare(
            `SELECT c.receipt,e.payload FROM domain_events e
        JOIN event_commits c ON c.commit_seq=e.commit_seq
        WHERE e.account_id=? AND EXISTS (SELECT 1 FROM json_each(e.payload,'$.evidence')
        WHERE json_extract(value,'$.id')=?)`,
          )
          .all(accountId, row.id),
        results = [];
      for (const decision of decisions) {
        const evidence = object(json(decision.payload)).evidence,
          result = json(decision.receipt);
        if (!evidence.some((value) => canonicalJson(value) === canonicalJson(original))) {
          throw new Error('The funding evidence differs from its original audit source.');
        }
        if (
          result &&
          typeof result === 'object' &&
          !Array.isArray(result) &&
          Object.keys(result).length === 2 &&
          result.eventId === row.id &&
          typeof result.duplicate === 'boolean'
        ) {
          results.push(result);
        }
      }
      if (results.some((result) => canonicalJson(result) !== canonicalJson(results[0]))) {
        throw new Error('Conflicting original funding responses.');
      }
      const payload = {
          accountId,
          receipt: row.id.slice('operating-receipt:'.length),
          resource: original.resource,
          amount: amount(original.amount),
          originalType: original.type,
          occurredAt: time(original.at),
          request: knownRequest,
          result: results[0] ?? null,
        },
        fact = emit('OpeningFundingReceiptRecorded', row.id, payload, null, accountId);
      if (fact.eventId !== row.id) throw new Error('Funding business identity must remain exact.');
      expected.receipts.push({ eventId: fact.eventId, ...payload });
      fundingReceipts.add(row.id);
    });
  }
  for (const row of source
    .prepare("SELECT stream_id FROM event_streams WHERE stream_kind='account' ORDER BY stream_id")
    .iterate()) {
    collect('invalid_account_finance', () => {
      const projection = object(readStreamProjection(source, row.stream_id)?.projection),
        accountId = projection.id ?? row.stream_id.slice('account:'.length);
      if (row.stream_id !== `account:${accountId}` || !accountIds.has(accountId)) {
        throw new Error('Financial account lacks its original public account.');
      }
      if (projection.quota == null) {
        if (financialAccounts.has(accountId)) {
          block('account_financial_state_requires_initial_opening');
          return;
        }
        collect('invalid_initial_quota_source', () => {
          const { fields } = officialOpening(accountId, projection),
            payload = { ...initialAccountQuota(accountId), ...fields };
          emit('OpeningQuotaRecorded', accountId, payload, null, accountId);
          expected.quotas.push(payload);
        });
        return;
      }
      const state = object(projection.quota);
      for (const [memberId, balance] of Object.entries(object(state.balances))) {
        collect('invalid_wallet_source', () => {
          if (!memberIds.has(memberId)) {
            throw new Error('Wallet lacks its stable member.');
          }
          const available = Object.fromEntries(
            resources.map((resource) => [resource, amount(balance[`${resource}Available`], true)]),
          );
          if (
            Object.hasOwn(balance, 'anlasAvailable') &&
            amount(balance.anlasAvailable, true) !==
              formatAmount(
                parseAmount(available.anlasFixed) + parseAmount(available.anlasPurchased),
              )
          ) {
            throw new Error('Wallet aggregate disagrees with its original pools.');
          }
          const originalId = canonicalJson([accountId, memberId]);
          emit(
            'OpeningWalletRecorded',
            originalId,
            { accountId, memberId, available },
            memberId,
            accountId,
          );
          expected.wallets.push({ accountId, memberId, available });
          walletKeys.add(originalId);
        });
      }
      for (const [jobId, charge] of Object.entries(object(state.reservations))) {
        collect('invalid_reserved_charge', () => {
          if (charge.status !== 'reserved') {
            throw new Error('Current-v4 active reservations contain a terminal.');
          }
          saveCharge(jobId, charge, accountId);
        });
      }
      collect('invalid_quota_source', () => {
        const snapshot = state.snapshot === null ? null : object(state.snapshot),
          { latest, fields } = officialOpening(accountId, projection);
        let baselineEventId = null;
        if (snapshot !== null) {
          const baseline = {
            accountId,
            credentialEpoch: null,
            sequence: null,
            observedAt: time(snapshot.at),
            v5UsagePercent: null,
            v5UsageIsNegative: null,
            v5: snapshot.v5 === null ? null : amount(snapshot.v5),
            anlasFixed: snapshot.anlasFixed === null ? null : amount(snapshot.anlasFixed),
            anlasPurchased:
              snapshot.anlasPurchased === null ? null : amount(snapshot.anlasPurchased),
            secondsPerPercent: snapshot.seconds,
          };
          if (baseline.observedAt === null) {
            throw new Error('Financial baseline lacks its original observed time.');
          }
          const {
              v5UsagePercent: _percent,
              v5UsageIsNegative: _negative,
              ...baselineValues
            } = baseline,
            {
              v5UsagePercent: _latestPercent,
              v5UsageIsNegative: _latestNegative,
              ...latestValues
            } = latest;
          if (
            fields.observationEventId !== null &&
            canonicalJson(baselineValues) === canonicalJson(latestValues)
          ) {
            baselineEventId = fields.observationEventId;
          } else {
            baselineEventId = emit(
              'OpeningQuotaObservationRecorded',
              canonicalJson([accountId, 'financial-baseline']),
              baseline,
              null,
              accountId,
              accountId,
            ).eventId;
            expected.observations.push({ eventId: baselineEventId, ...baseline });
          }
        }
        const payload = {
          accountId,
          status: state.status,
          ...fields,
          baselineEventId,
          refillCursorAt: time(state.lastRefillAt),
          settlementCursorAt: time(state.lastSettlementAt),
          unallocatedV5: amount(state.unallocatedV5),
          fallbackFixed: amount(state.fallbackBuffer.fixed),
          fallbackPurchased: amount(state.fallbackBuffer.purchased),
          sharedV5Debt: amount(state.sharedV5Debt),
          pendingV5Debit: amount(state.spent.v5),
          pendingFixedDebit: amount(state.spent.anlasFixed),
          pendingPurchasedDebit: amount(state.spent.anlasPurchased),
          pendingFallbackV5: amount(state.spent.fallbackV5),
          pendingFallbackFixed: amount(state.spent.fallbackFixed),
          pendingFallbackPurchased: amount(state.spent.fallbackPurchased),
        };
        emit('OpeningQuotaRecorded', accountId, payload, null, accountId);
        expected.quotas.push(payload);
      });
      collect('invalid_operating_source', () => {
        const ops = readOperatingAccount(state),
          gapIds = {};
        for (const resource of resources) {
          const originalGapId = resource === 'v5' ? ops.v5GapEventId : ops.gapEventIds[resource],
            remaining = amount(resource === 'v5' ? ops.v5CapacityGap : ops.unfunded[resource]);
          gapIds[resource] = null;
          if (originalGapId === null && remaining === '0') continue;
          const row =
              originalGapId === null
                ? null
                : source
                    .prepare('SELECT id,account_id,source,data FROM ledger WHERE id=?')
                    .get(originalGapId),
            original = row ? object(json(row.data)) : null;
          if (
            original &&
            (row.id !== original.id ||
              original.id !== originalGapId ||
              row.account_id !== accountId ||
              original.accountId !== accountId ||
              original.type !== row.source ||
              original.resource !== resource ||
              ![
                'resource_gap_observed',
                'resource_gap_reconciled',
                'v5_capacity_observed',
              ].includes(original.type))
          ) {
            throw new Error('The current operating gap has a different original source.');
          }
          const payload = {
              accountId,
              resource,
              remaining,
              originalGapId,
              originalType: original?.type ?? null,
              occurredAt: time(original?.at),
            },
            fact = emit(
              'OpeningOperatingGapRecorded',
              canonicalJson([accountId, resource]),
              payload,
              null,
              accountId,
              accountId,
            );
          gapIds[resource] = fact.eventId;
          expected.gaps.push({ eventId: fact.eventId, ...payload });
        }
        const payload = {
          accountId,
          available: amounts(ops.available),
          unfundedFixed: amount(ops.unfunded.anlasFixed),
          unfundedPurchased: amount(ops.unfunded.anlasPurchased),
          unattributedFixed: amount(ops.unattributedReturns.anlasFixed),
          unattributedPurchased: amount(ops.unattributedReturns.anlasPurchased),
          v5CapacityGap: amount(ops.v5CapacityGap),
          fixedGapEventId: gapIds.anlasFixed,
          purchasedGapEventId: gapIds.anlasPurchased,
          v5GapEventId: gapIds.v5,
        };
        emit('OpeningOperatingRecorded', accountId, payload, null, accountId);
        expected.operating.push(payload);
      });
      const ops = readOperatingAccount(state);
      if (!Array.isArray(ops.pendingExternalFunding)) {
        throw new Error('Pending funding is not an original ordered array.');
      }
      for (const funding of ops.pendingExternalFunding) {
        collect('invalid_pending_funding', () => {
          if (!fundingReceipts.has(funding.eventId)) {
            if (source.prepare('SELECT 1 FROM ledger WHERE id=?').get(funding.eventId)) {
              throw new Error('Pending funding has a contradictory original audit role.');
            }
            const original = {
                accountId,
                receipt: funding.receipt,
                resource: funding.resource,
                amount: null,
                originalType: 'operating_funding_registered',
                occurredAt: requiredTime(funding.registeredAt),
                request: null,
                result: null,
              },
              fact = emit(
                'OpeningFundingReceiptRecorded',
                funding.eventId,
                original,
                null,
                accountId,
              );
            if (fact.eventId !== funding.eventId)
              throw new Error('Funding business identity must remain exact.');
            expected.receipts.push({ eventId: fact.eventId, ...original });
            fundingReceipts.add(funding.eventId);
          }

          const payload = {
            receipt: funding.receipt,
            accountId,
            resource: funding.resource,
            destination: funding.destination,
            remaining: amount(funding.remaining),
            registeredAt: requiredTime(funding.registeredAt),
            snapshotAt: time(funding.snapshotAt),
          };
          emit('OpeningFundingRecorded', funding.eventId, payload, null, accountId);
          expected.funding.push(payload);
        });
      }
    });
  }

  for (const row of source
    .prepare('SELECT job_id,charge FROM fair_quota_settlements ORDER BY job_id')
    .iterate()) {
    collect('invalid_terminal_charge', () => {
      const charge = json(row.charge);
      if (!['settled', 'refunded'].includes(charge?.status)) {
        throw new Error('Cold charge is not a terminal.');
      }
      saveCharge(row.job_id, charge, charge.accountId);
    });
  }
  const refundResponses = new Map();
  for (const row of source
    .prepare(
      `SELECT json_extract(data,'$.jobId') AS job_id,
    json_extract(data,'$.response') AS response FROM ledger
    WHERE source='generation_refunded' ORDER BY order_value,id`,
    )
    .iterate()) {
    if (row.response !== null) {
      refundResponses.set(row.job_id, json(row.response));
    }
  }
  for (const row of source
    .prepare(
      `SELECT id,status,
    json_extract(data,'$.generationAttempt') AS attempt,
    json_extract(data,'$.generationReview') AS review,
    json_extract(data,'$.generationRefund') AS refund,
    json_extract(data,'$.resultPending') AS prepared,
    json_extract(data,'$.upstreamStartedAt') AS started,
    json_extract(data,'$.upstreamHeadersAt') AS headers,
    json_extract(data,'$.upstreamCompletedAt') AS completed,
    json_extract(data,'$.upstreamHttpStatus') AS http_status
    FROM jobs ORDER BY id`,
    )
    .iterate()) {
    jobs.set(row.id, row);
    if (row.attempt === null) {
      if (row.review !== null || row.refund !== null || row.prepared !== null) {
        block('missing_original_attempt');
      }
      continue;
    }
    collect('invalid_attempt_source', () => {
      const attempt = object(json(row.attempt)),
        review = json(row.review),
        prepared = json(row.prepared),
        charge = charges.get(row.id);
      if (
        !jobIds.has(row.id) ||
        !requestsByJob.has(row.id) ||
        !attemptIds.has(attempt.id) ||
        !accountIds.has(attempt.accountId) ||
        !memberIds.has(attempt.memberId) ||
        attempt.jobId !== row.id ||
        !charge ||
        charge.accountId !== attempt.accountId ||
        charge.userId !== attempt.memberId ||
        canonicalJson(historicalAmounts(attempt.localReserved)) !==
          canonicalJson(
            (charge.status === 'reserved' ? amounts : historicalAmounts)(
              generationChargeResources(charge),
            ),
          )
      ) {
        throw new Error('Attempt/charge/job/account/member original identities disagree.');
      }
      const upstreamCandidates = attempt.upstreamCandidates.map((candidate) => ({
        resources: Object.fromEntries(
          Object.entries(object(candidate.resources)).map(([resource, value]) => [
            resource,
            historicalAmount(value),
          ]),
        ),
        amountSource: candidate.amountSource,
      }));
      const originalResponse = refundResponses.get(row.id),
        originalJob = {
          status: row.status,
          upstreamStartedAt: row.started,
          upstreamHeadersAt: row.headers,
          upstreamCompletedAt: row.completed,
          upstreamHttpStatus: row.http_status,
        };
      for (const field of [
        'upstreamStartedAt',
        'upstreamHeadersAt',
        'upstreamCompletedAt',
        'upstreamHttpStatus',
      ]) {
        if (originalJob[field] == null && originalResponse?.[field] != null) {
          originalJob[field] = originalResponse[field];
        } else if (
          originalJob[field] != null &&
          originalResponse?.[field] != null &&
          originalJob[field] !== originalResponse[field]
        ) {
          throw new Error('Original response facts conflict.');
        }
      }
      if (originalResponse?.dispatchState === 'not_sent' && originalJob.upstreamStartedAt != null) {
        throw new Error('Original not-sent evidence conflicts with an actual start.');
      }
      const response = generationResponseEvidence(originalJob, {
          requestStarted: originalResponse?.dispatchState === 'not_sent' ? false : undefined,
        }),
        payload = {
          attemptId: attempt.id,
          jobId: row.id,
          accountId: attempt.accountId,
          credentialEpoch: attempt.credentialEpoch ?? null,
          connectionSourceEventId: null,
          authorizedAt: time(attempt.dispatchIntentAt),
          response: {
            dispatchState:
              response.dispatchState === 'unknown' &&
              originalResponse?.dispatchState === 'request_started'
                ? 'request_started'
                : response.dispatchState,
            upstreamStartedAt: time(response.upstreamStartedAt),
            upstreamHeadersAt: time(response.upstreamHeadersAt),
            upstreamCompletedAt: time(response.upstreamCompletedAt),
            upstreamHttpStatus: response.upstreamHttpStatus,
          },
          candidateMode: attempt.candidateMode,
          upstreamCandidates,
          actualConsumption:
            attempt.actualConsumption == null ? null : historicalAmounts(attempt.actualConsumption),
          lossCoverageRemaining: Object.fromEntries(resources.map((resource) => [resource, null])),
          reviewStatus: review?.status ?? null,
          firstUnknownAt: time(review?.firstUnknownAt),
          reviewDeadlineAt: time(review?.deadlineAt),
          reviewClosedAt: time(review?.closedAt),
          originalLossEventId: review?.lossEventId ?? null,
          originalClosureEventId: review?.closureEventId ?? null,
          preparedArtifactId: prepared?.imageId ?? null,
          preparedFile: prepared?.file ?? null,
          preparedMimeType: prepared?.mimeType ?? null,
          preparedByteLength: prepared?.byteLength ?? null,
          preparedSha256: prepared?.sha256 ?? null,
        };
      if (prepared !== null && prepared.attemptId !== attempt.id) {
        throw new Error('Prepared bytes belong to another original attempt.');
      }
      emit('OpeningAttemptRecorded', attempt.id, payload, attempt.memberId, attempt.accountId);
      expected.attempts.push(payload);
      mappedAttempts.add(attempt.id);
    });
  }
  for (const [jobId, charge] of charges) {
    collect('invalid_charge_source', () => {
      const knownJob = jobIds.has(jobId),
        live = knownJob && walletKeys.has(canonicalJson([charge.accountId, charge.userId])),
        attempt = jobs.get(jobId)?.attempt ? json(jobs.get(jobId).attempt) : null;
      if (
        charge.status === 'reserved' &&
        (!live ||
          !attempt ||
          !mappedAttempts.has(attempt.id) ||
          jobs.get(jobId).status !== 'running')
      ) {
        throw new Error('An unresolved reservation cannot be archived or lose its real attempt.');
      }
      const chargeAmount = charge.status === 'reserved' ? amount : historicalAmount,
        originalAmount = chargeAmount(charge.amount),
        originalFixed = chargeAmount(charge.fixed),
        originalPurchased = chargeAmount(charge.purchased);
      if (
        !['free', 'v5', 'anlas'].includes(charge.resource) ||
        (charge.resource === 'free' &&
          [originalAmount, originalFixed, originalPurchased].some((value) => value !== '0')) ||
        (charge.resource === 'v5' && (originalFixed !== '0' || originalPurchased !== '0')) ||
        (charge.resource === 'anlas' &&
          parseAmount(originalAmount) !==
            parseAmount(originalFixed) + parseAmount(originalPurchased)) ||
        (attempt !== null && charge.attemptId != null && charge.attemptId !== attempt.id)
      ) {
        throw new Error('Original charge pools disagree.');
      }
      const id = eventId('OpeningChargeRecorded', jobId),
        payload = {
          originalJobId: jobId,
          originalMemberId: charge.userId,
          originalAccountId: charge.accountId,
          jobId: knownJob ? jobId : null,
          walletAccountId: live ? charge.accountId : null,
          walletMemberId: live ? charge.userId : null,
          attemptId: live && mappedAttempts.has(attempt?.id) ? attempt.id : null,
          status: charge.status,
          resolution: live ? 'live' : 'archived',
          originRef: charge.attemptId ?? jobId,
          resources: (charge.status === 'reserved' ? amounts : historicalAmounts)(
            generationChargeResources(charge),
          ),
          billingMode: charge.billingMode ?? charge.resource,
          estimated: typeof charge.estimated === 'boolean' ? charge.estimated : null,
          v5Equivalent:
            charge.billingMode === 'anlas_fallback' ? chargeAmount(charge.v5Equivalent) : '0',
          sharedV5Reserved:
            charge.billingMode === 'anlas_fallback' ? chargeAmount(charge.sharedV5Reserved) : '0',
          configVersion: charge.configVersion,
          reservedAt: requiredTime(charge.reservedAt),
          terminalAt: charge.status === 'reserved' ? null : requiredTime(charge.settledAt),
          reservationEventId: id,
          terminalEventId: charge.status === 'reserved' ? null : id,
        };
      emit('OpeningChargeRecorded', jobId, payload, charge.userId, charge.accountId);
      expected.charges.push(payload);
    });
  }
  for (const row of source
    .prepare(
      'SELECT job_id,member_id,succeeded_at,warning_threshold FROM free_image_usage ORDER BY job_id',
    )
    .iterate()) {
    if (!jobIds.has(row.job_id) || !memberIds.has(row.member_id)) {
      block('free_usage_missing_original_job');
      continue;
    }
    collect('invalid_free_usage', () => {
      expected.freeUsage.push({
        jobId: row.job_id,
        memberId: row.member_id,
        succeededAt: requiredTime(row.succeeded_at),
        warningThreshold: row.warning_threshold,
      });
    });
  }
  return {
    facts,
    findings: [...findings].map(([code, count]) => ({ code, count, blocking: true })),
    expected,
  };
}

// Compare original cut expectations, not another allocation/settlement calculation.
// The installer independently verifies complete new facts -> projections/content.
export function assertLegacyFinanceProjection(target, expected) {
  if (!target.inTransaction) {
    throw new Error('Finance reconciliation requires the owning target snapshot transaction.');
  }
  const checked = {},
    equal = (name, actual) => {
      if (!Array.isArray(expected[name])) {
        throw new TypeError('Missing original finance reconciliation expectations.');
      }
      const rows = (values) => values.map((row) => canonicalJson(row)).sort();
      if (canonicalJson(rows(actual)) !== canonicalJson(rows(expected[name]))) {
        throw Object.assign(new Error(`Legacy finance reconciliation mismatch: ${name}.`), {
          code: 'LEGACY_FINANCE_RECONCILIATION_FAILED',
        });
      }
      checked[name] = actual.length;
    };
  equal(
    'wallets',
    target
      .prepare(
        `SELECT account_id AS accountId,member_id AS memberId,
    v5_available AS v5,anlas_fixed_available AS anlasFixed,anlas_purchased_available AS anlasPurchased
    FROM wallets`,
      )
      .all()
      .map(({ accountId, memberId, ...available }) => ({ accountId, memberId, available })),
  );
  const quotas = new RelationalQuotaStore(target);
  equal(
    'quotas',
    target
      .prepare('SELECT account_id AS id FROM account_quota_state')
      .all()
      .map(({ id }) => ({
        ...quotas.readAccountQuota(id),
        ...quotas.readOfficialQuotaV5(id),
      })),
  );
  equal(
    'observations',
    target
      .prepare(
        `SELECT event_id AS eventId,payload FROM domain_events
    WHERE event_type='OpeningQuotaObservationRecorded'`,
      )
      .all()
      .map(({ eventId, payload }) => {
        const { origin: _origin, ...original } = JSON.parse(payload);
        return { eventId, ...original };
      }),
  );
  for (const [name, type] of [
    ['gaps', 'OpeningOperatingGapRecorded'],
    ['receipts', 'OpeningFundingReceiptRecorded'],
  ]) {
    equal(
      name,
      target
        .prepare('SELECT event_id AS eventId,payload FROM domain_events WHERE event_type=?')
        .all(type)
        .map(({ eventId, payload }) => {
          const { origin: _origin, ...original } = JSON.parse(payload);
          return { eventId, ...original };
        }),
    );
  }
  equal(
    'operating',
    target
      .prepare(
        `SELECT account_id AS accountId,
    v5_available AS v5,anlas_fixed_available AS anlasFixed,anlas_purchased_available AS anlasPurchased,
    unfunded_fixed AS unfundedFixed,unfunded_purchased AS unfundedPurchased,
    unattributed_fixed AS unattributedFixed,unattributed_purchased AS unattributedPurchased,
    v5_capacity_gap AS v5CapacityGap,fixed_gap_event_id AS fixedGapEventId,
    purchased_gap_event_id AS purchasedGapEventId,v5_gap_event_id AS v5GapEventId
    FROM operating_state`,
      )
      .all()
      .map(({ v5, anlasFixed, anlasPurchased, ...row }) => ({
        ...row,
        available: { v5, anlasFixed, anlasPurchased },
      })),
  );
  equal(
    'funding',
    target
      .prepare(
        `SELECT receipt,account_id AS accountId,resource,destination,
    remaining,registered_at AS registeredAt,snapshot_at AS snapshotAt FROM pending_funding`,
      )
      .all(),
  );
  equal(
    'charges',
    target
      .prepare(
        `SELECT original_job_id AS originalJobId,
    original_member_id AS originalMemberId,original_account_id AS originalAccountId,
    job_id AS jobId,wallet_account_id AS walletAccountId,wallet_member_id AS walletMemberId,
    attempt_id AS attemptId,status,resolution,origin_ref AS originRef,
    v5,anlas_fixed AS anlasFixed,anlas_purchased AS anlasPurchased,
    billing_mode AS billingMode,estimated,v5_equivalent AS v5Equivalent,shared_v5_reserved AS sharedV5Reserved,
    config_version AS configVersion,reserved_at AS reservedAt,terminal_at AS terminalAt,
    reservation_event_id AS reservationEventId,terminal_event_id AS terminalEventId
    FROM charges`,
      )
      .all()
      .map(({ v5, anlasFixed, anlasPurchased, estimated, ...row }) => ({
        ...row,
        resources: { v5, anlasFixed, anlasPurchased },
        estimated: estimated === null ? null : estimated === 1,
      })),
  );
  equal(
    'attempts',
    target
      .prepare(
        `SELECT id AS attemptId,job_id AS jobId,account_id AS accountId,
    credential_epoch AS credentialEpoch,authorized_at AS authorizedAt,
    dispatch_state AS dispatchState,upstream_started_at AS upstreamStartedAt,
    upstream_headers_at AS upstreamHeadersAt,upstream_completed_at AS upstreamCompletedAt,
    upstream_http_status AS upstreamHttpStatus,candidate_mode AS candidateMode,
    upstream_candidates AS upstreamCandidates,actual_consumption AS actualConsumption,
    review_status AS reviewStatus,first_unknown_at AS firstUnknownAt,review_deadline_at AS reviewDeadlineAt,
    review_closed_at AS reviewClosedAt,loss_event_id AS lossEventId,closure_event_id AS closureEventId,
    prepared_artifact_id AS preparedArtifactId,prepared_file AS preparedFile,
    prepared_mime_type AS preparedMimeType,prepared_byte_length AS preparedByteLength,
    prepared_sha256 AS preparedSha256 FROM generation_attempts`,
      )
      .all()
      .map((row) => {
        const original = readRelationalOpeningAttempt(target, row.attemptId);
        if (
          !original ||
          row.lossEventId !== (row.reviewStatus === null ? null : original.eventId) ||
          row.closureEventId !==
            (row.reviewStatus !== null && row.reviewStatus !== 'pending' ? original.eventId : null)
        ) {
          throw new Error('Finance reconciliation requires the original opening attempt anchors.');
        }
        const {
          dispatchState,
          upstreamStartedAt,
          upstreamHeadersAt,
          upstreamCompletedAt,
          upstreamHttpStatus,
          upstreamCandidates,
          actualConsumption,
          lossEventId: _loss,
          closureEventId: _closure,
          ...values
        } = row;
        return {
          ...values,
          connectionSourceEventId: original.payload.connectionSourceEventId,
          response: {
            dispatchState,
            upstreamStartedAt,
            upstreamHeadersAt,
            upstreamCompletedAt,
            upstreamHttpStatus,
          },
          upstreamCandidates: JSON.parse(upstreamCandidates),
          actualConsumption: json(actualConsumption),
          lossCoverageRemaining: original.payload.lossCoverageRemaining,
          originalLossEventId: original.payload.originalLossEventId,
          originalClosureEventId: original.payload.originalClosureEventId,
        };
      }),
  );
  equal(
    'freeUsage',
    target
      .prepare(
        `SELECT j.id AS jobId,r.member_id AS memberId,
    j.free_succeeded_at AS succeededAt,j.free_warning_threshold AS warningThreshold
    FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id
    WHERE j.free_succeeded_at IS NOT NULL`,
      )
      .all(),
  );
  return checked;
}
