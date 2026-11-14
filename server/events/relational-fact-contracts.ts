import { canonicalJson, assertPublicDomainFact, type Json } from './domain-events.ts';
import { parseAmount, formatAmount, AMOUNT_SCALE } from '../quota/resource-amounts.ts';
import { parseMemberGenerationConfirmation } from '../generation/member-contract.ts';

type Parser<T> = (value: unknown) => T;
type Parsed<P> = P extends Parser<infer T> ? T : never;
const invalid = (): never => {
  throw new TypeError('Invalid relational fact fields or values.');
};
const string: Parser<string> = (value) =>
  typeof value === 'string' && !value.includes('\0') ? value : invalid();
const id: Parser<string> = (value) => {
  const result = string(value);
  return result.length > 0 && result.length <= 1024 ? result : invalid();
};
const bool: Parser<boolean> = (value) => (typeof value === 'boolean' ? value : invalid());
const number: Parser<number> = (value) =>
  typeof value === 'number' && Number.isFinite(value) ? value : invalid();
const integer: Parser<number> = (value) => {
  const result = number(value);
  return Number.isSafeInteger(result) && result >= 0 ? result : invalid();
};
const positive: Parser<number> = (value) => {
  const result = integer(value);
  return result > 0 ? result : invalid();
};
const amount: Parser<string> = (value) => {
  const result = string(value);
  return formatAmount(parseAmount(result)) === result ? result : invalid();
};
const nonnegative: Parser<string> = (value) => {
  const result = amount(value);
  return parseAmount(result) >= 0n ? result : invalid();
};
const nonnegativeNumber: Parser<number> = (value) => {
  const result = number(value);
  return result >= 0 ? result : invalid();
};
const policyInteger: Parser<string> = (value) => {
  const result = nonnegative(value),
    units = parseAmount(result);
  return units % AMOUNT_SCALE === 0n && units <= BigInt(Number.MAX_SAFE_INTEGER) * AMOUNT_SCALE
    ? result
    : invalid();
};
const hash: Parser<string> = (value) => {
  const result = string(value);
  return /^[a-f0-9]{64}$/.test(result) ? result : invalid();
};
const sequence: Parser<string> = (value) => {
  const result = string(value);
  return /^(0|[1-9][0-9]*)$/.test(result) && BigInt(result) <= 9_223_372_036_854_775_807n
    ? result
    : invalid();
};
function choices<const T extends readonly (string | number | boolean | null)[]>(
  ...values: T
): Parser<T[number]> {
  return (value) => (values.includes(value as T[number]) ? (value as T[number]) : invalid());
}
function nullable<T>(parse: Parser<T>): Parser<T | null> {
  return (value) => (value === null ? null : parse(value));
}
function list<T>(parse: Parser<T>): Parser<T[]> {
  return (value) => (Array.isArray(value) ? value.map(parse) : invalid());
}
// 一个字段表同时约束运行时和写入类型；未知字段直接拒绝，不让 DTO 正文混进事实。
function fields<T extends Record<string, Parser<unknown>>>(
  shape: T,
): Parser<{ [K in keyof T]: Parsed<T[K]> }> {
  return (value) => {
    if (
      !value ||
      typeof value !== 'object' ||
      Array.isArray(value) ||
      Object.getPrototypeOf(value) !== Object.prototype
    ) {
      invalid();
    }
    const row = value as Record<string, unknown>;
    if (
      Object.keys(row).length !== Object.keys(shape).length ||
      Object.keys(row).some((key) => !Object.hasOwn(shape, key))
    ) {
      invalid();
    }
    return Object.fromEntries(
      Object.entries(shape).map(([key, parse]) => [key, parse(row[key])]),
    ) as {
      [K in keyof T]: Parsed<T[K]>;
    };
  };
}
const receipt: Parser<Json> = (value) => {
  assertPublicDomainFact(value, 'receipt');
  return JSON.parse(canonicalJson(value)) as Json;
};
const resources = fields({ v5: nonnegative, anlasFixed: nonnegative, anlasPurchased: nonnegative });
const signedResources = fields({ v5: amount, anlasFixed: amount, anlasPurchased: amount });
const candidateResources: Parser<Partial<Parsed<typeof resources>>> = (value) => {
  if (!value || typeof value !== 'object' || Array.isArray(value) || !Object.keys(value).length) {
    invalid();
  }
  for (const [key, entry] of Object.entries(value as object)) {
    if (!['v5', 'anlasFixed', 'anlasPurchased'].includes(key)) {
      invalid();
    }
    nonnegative(entry);
  }
  return { ...(value as Partial<Parsed<typeof resources>>) };
};
const candidate = fields({ resources: candidateResources, amountSource: id });
const operation = choices('generate', 'upscale', 'encode-vibe');
const billing = choices('free', 'v5', 'anlas', 'anlas_fallback');
const resolution = choices('live', 'archived', 'unavailable');
const origin = fields({ sourceId: id, originalId: id });
const response = fields({
  dispatchState: choices('request_started', 'not_sent', 'unknown'),
  upstreamStartedAt: nullable(integer),
  upstreamHeadersAt: nullable(integer),
  upstreamCompletedAt: nullable(integer),
  upstreamHttpStatus: nullable(positive),
});
const confirmation: Parser<ReturnType<typeof parseMemberGenerationConfirmation> | null> = (
  value,
) => {
  if (value === null) {
    return null;
  }
  const parsed = parseMemberGenerationConfirmation(value);
  return canonicalJson(parsed) === canonicalJson(value) ? parsed : invalid();
};
const chargeFields = {
  originalJobId: id,
  originalMemberId: id,
  originalAccountId: id,
  jobId: nullable(id),
  walletAccountId: nullable(id),
  walletMemberId: nullable(id),
  attemptId: nullable(id),
  status: choices('reserved', 'settled', 'refunded'),
  resolution,
  originRef: id,
  resources,
  billingMode: billing,
  estimated: nullable(bool),
  v5Equivalent: nonnegative,
  sharedV5Reserved: nonnegative,
  configVersion: integer,
  reservedAt: integer,
  terminalAt: nullable(integer),
  reservationEventId: id,
  terminalEventId: nullable(id),
};
const quotaFields = {
  accountId: id,
  status: id,
  observationEventId: nullable(id),
  observedAt: nullable(integer),
  observedCredentialEpoch: nullable(positive),
  observedSequence: nullable(sequence),
  quotaTier: nullable(integer),
  subscriptionActive: nullable(bool),
  subscriptionExpiresAt: nullable(integer),
  quotaErrorCode: nullable(id),
  observedV5: nullable(amount),
  observedAnlasFixed: nullable(amount),
  observedAnlasPurchased: nullable(amount),
  secondsPerPercent: nullable(number),
  baselineEventId: nullable(id),
  refillCursorAt: nullable(integer),
  settlementCursorAt: nullable(integer),
  unallocatedV5: nonnegative,
  fallbackFixed: nonnegative,
  fallbackPurchased: nonnegative,
  sharedV5Debt: nonnegative,
  pendingV5Debit: nonnegative,
  pendingFixedDebit: nonnegative,
  pendingPurchasedDebit: nonnegative,
  pendingFallbackV5: nonnegative,
  pendingFallbackFixed: nonnegative,
  pendingFallbackPurchased: nonnegative,
};
const operatingFields = {
  accountId: id,
  available: resources,
  unfundedFixed: nonnegative,
  unfundedPurchased: nonnegative,
  unattributedFixed: nonnegative,
  unattributedPurchased: nonnegative,
  v5CapacityGap: nonnegative,
  fixedGapEventId: nullable(id),
  purchasedGapEventId: nullable(id),
  v5GapEventId: nullable(id),
};
const fundingFields = {
  receipt: id,
  accountId: id,
  resource: choices('anlasFixed', 'anlasPurchased'),
  destination: id,
  remaining: nonnegative,
  registeredAt: integer,
  snapshotAt: nullable(integer),
};
const policyFields = {
  enabled: bool,
  boundAccountId: nullable(id),
  configVersion: integer,
  autoAnlasFallback: bool,
  freePolicyVersion: positive,
  freeEnabled: bool,
  freeWarningThreshold: nullable(positive),
  freeHardLimit: nullable(positive),
  lossPolicyVersion: positive,
  lossV5_24h: nonnegativeNumber,
  lossFixed24h: policyInteger,
  lossFixed30d: policyInteger,
  lossMemberAttempts24h: integer,
};
const replicaFields = {
  artifactId: id,
  storageTargetId: id,
  objectKey: id,
  encoding: choices('identity', 'gzip', 'br'),
  uploadedAt: nullable(integer),
  verifiedAt: nullable(integer),
  deletedAt: nullable(integer),
  lastErrorCode: nullable(id),
};

export const relationalPayloadParsers = {
  InputRegistered: fields({
    inputId: id,
    memberId: id,
    inputSchemaVersion: choices(1),
    inputSha256: hash,
    logicalBytes: integer,
  }),
  ContextRegistered: fields({
    contextId: id,
    contextSchemaVersion: choices(1),
    contextSha256: hash,
  }),
  DiagnosticRecorded: fields({ diagnosticId: id, jobId: id, sha256: hash, createdAt: integer }),
  RequestAccepted: fields({
    requestId: id,
    memberId: id,
    inputId: id,
    contextId: id,
    source: id,
    operation,
    model: id,
    sampleCount: positive,
    seedMode: choices('fixed', 'random', 'not_applicable'),
    confirmation,
  }),
  JobAccepted: fields({
    jobId: id,
    requestId: id,
    sampleIndex: integer,
    actualSeed: nullable(integer),
    status: choices('queued', 'pending', 'done'),
    queueSeq: nullable(sequence),
    queueExpiresAt: nullable(integer),
    deadlineAt: nullable(integer),
    admissionEpoch: integer,
    cacheKey: nullable(hash),
  }),
  DispatchAuthorized: fields({
    attemptId: id,
    jobId: id,
    accountId: id,
    credentialEpoch: positive,
    candidateMode: choices('known', 'alternatives', 'unknown'),
    upstreamCandidates: list(candidate),
  }),
  QuotaReserved: fields({
    jobId: id,
    memberId: id,
    accountId: id,
    resources,
    billingMode: billing,
    estimated: bool,
    v5Equivalent: nonnegative,
    sharedV5Reserved: nonnegative,
    configVersion: integer,
  }),
  GenerationSettled: fields({ jobId: id, reservationEventId: id }),
  GenerationRefunded: fields({ jobId: id, reservationEventId: id, lossEventId: nullable(id) }),
  ResponseEvidenceRecorded: fields({ attemptId: id, response }),
  ArtifactPrepared: fields({
    artifactId: id,
    attemptId: id,
    file: id,
    mimeType: id,
    byteLength: positive,
    sha256: hash,
    response,
  }),
  ArtifactPublished: fields({
    artifactId: id,
    memberId: id,
    inputId: id,
    originalAttemptId: id,
    operation,
    mimeType: id,
    byteLength: positive,
    sha256: hash,
  }),
  JobCompleted: fields({ jobId: id, artifactId: id, response }),
  JobFailed: fields({
    jobId: id,
    errorCode: id,
    errorStatus: positive,
    cancellationReason: nullable(id),
    diagnosticId: nullable(id),
    response,
  }),
  LateResultRecovered: fields({
    jobId: id,
    artifactId: id,
    refundRetained: choices(true),
    response,
  }),
  CachedResultLinked: fields({ jobId: id, artifactId: id }),
  FreeImageSucceeded: fields({ jobId: id, memberId: id, warningThreshold: nullable(positive) }),
  JobQueued: fields({ jobId: id, queueSeq: sequence, queueExpiresAt: nullable(integer) }),
  WaitingJobsCancelled: fields({
    jobIds: list(id),
    reason: id,
    errorStatus: positive,
    cancellationReason: id,
  }),
  ReviewOpened: fields({
    attemptId: id,
    firstUnknownAt: integer,
    deadlineAt: integer,
    lossEventId: id,
    response,
  }),
  ReviewClosed: fields({
    attemptId: id,
    status: choices('estimated_closed', 'result_recovered'),
    lossEventId: id,
    reason: id,
    actualConsumption: nullable(resources),
  }),
  JobHistoryRetired: fields({ jobIds: list(id), reason: id }),
  ArtifactHistoryRetired: fields({ artifactId: id, reason: id }),
  MemberRegistered: fields({
    memberId: id,
    displayName: string,
    status: choices('active', 'disabled'),
    acceptingJobs: bool,
    admissionEpoch: integer,
    createdAt: integer,
  }),
  MemberStatusChanged: fields({
    memberId: id,
    status: choices('active', 'disabled', 'removed'),
  }),
  AdmissionChanged: fields({ memberId: id, acceptingJobs: bool, admissionEpoch: integer }),
  MemberEligibilityChanged: fields({
    memberId: id,
    discordId: id,
    guildId: id,
    checkedAt: integer,
    eligible: nullable(bool),
  }),
  SharingConfigured: fields({
    enabled: bool,
    boundAccountId: nullable(id),
    configVersion: integer,
  }),
  GroupConfigured: fields({ groupId: id, name: string, share: number, configVersion: integer }),
  GroupMemberAssigned: fields({
    memberId: id,
    groupId: id,
    weight: number,
    configVersion: integer,
  }),
  GroupMemberRemoved: fields({ memberId: id, groupId: id, configVersion: integer }),
  FallbackPolicyChanged: fields({ enabled: bool, configVersion: integer }),
  FreePolicyChanged: fields({
    version: positive,
    enabled: bool,
    warningThreshold: nullable(positive),
    hardLimit: nullable(positive),
  }),
  LossPolicyChanged: fields({
    version: positive,
    v5_24h: nonnegativeNumber,
    fixed24h: policyInteger,
    fixed30d: policyInteger,
    memberAttempts24h: integer,
  }),
  AccountRegistered: fields({
    accountId: id,
    displayName: string,
    enabled: bool,
    credentialEpoch: positive,
  }),
  AccountBound: fields({ accountId: nullable(id), configVersion: integer }),
  CredentialRotated: fields({ accountId: id, beforeEpoch: positive, credentialEpoch: positive }),
  AvailabilityChanged: fields({
    accountId: id,
    enabled: bool,
    reason: id,
    observationEventId: nullable(id),
  }),
  CooldownChanged: fields({
    accountId: id,
    attemptId: nullable(id),
    completionKind: nullable(choices('ended', 'success', 'ordinary', 'capacity')),
    cooldownUntil: nullable(integer),
    cooldownReason: nullable(id),
    ordinaryStrikes: integer,
    capacityStrikes: integer,
    capacityLimitedUntil: nullable(integer),
    capacityLastErrorAt: nullable(integer),
  }),
  QuotaObserved: fields({
    accountId: id,
    credentialEpoch: positive,
    sequence,
    observedAt: integer,
    quotaTier: nullable(integer),
    subscriptionActive: nullable(bool),
    subscriptionExpiresAt: nullable(integer),
    v5: nullable(amount),
    anlasFixed: nullable(amount),
    anlasPurchased: nullable(amount),
    secondsPerPercent: nullable(number),
    errorCode: nullable(id),
  }),
  QuotaStatusChanged: fields({ accountId: id, status: id }),
  QuotaBaselineAdopted: fields({
    accountId: id,
    observationEventId: id,
    matchedDebits: resources,
    matchedFallbackFixed: nonnegative,
    matchedFallbackPurchased: nonnegative,
    refillCursorAt: integer,
  }),
  RefillApplied: fields({
    accountId: id,
    from: integer,
    until: integer,
    observationEventId: id,
    budget: nonnegative,
    debtRepaid: nonnegative,
    capacityRecovered: nonnegative,
    allocations: list(fields({ memberId: id, amount: nonnegative })),
    configVersion: integer,
  }),
  AllocationApplied: fields({
    accountId: id,
    reason: choices('opening', 'funding', 'redistribution'),
    sourceEventId: id,
    sourceResources: resources,
    allocations: list(fields({ memberId: id, resources })),
    configVersion: integer,
  }),
  FundsTransferred: fields({
    accountId: id,
    fromMemberId: nullable(id),
    toMemberId: nullable(id),
    resources,
    reason: choices('transfer', 'member_removal', 'operator_share', 'operator_return'),
  }),
  FundingRegistered: fields(fundingFields),
  FundingReceived: fields({
    accountId: id,
    receipt: id,
    resource: choices('v5', 'anlasFixed', 'anlasPurchased'),
    amount: nonnegative,
    destination: id,
    observationEventId: nullable(id),
    receiptEventId: nullable(id),
    source: choices('operator_share', 'declared_external_receipt', 'registered_external_funding'),
    memberId: nullable(id),
  }),
  ResourceGapObserved: fields({
    accountId: id,
    resource: choices('anlasFixed', 'anlasPurchased'),
    amount: nonnegative,
    beforeObservationEventId: id,
    afterObservationEventId: id,
    evidenceLevel: id,
    actualConsumption: choices(null),
  }),
  ReturnClassified: fields({
    accountId: id,
    resource: choices('anlasFixed', 'anlasPurchased'),
    amount,
    gapEventId: nullable(id),
    observationEventId: id,
    kind: choices('observed', 'gap_correction'),
  }),
  DebitsConfirmed: fields({
    accountId: id,
    resource: choices('anlasFixed', 'anlasPurchased'),
    amount: nonnegative,
    allocatedToMembers: nonnegative,
    evidenceLevel: choices('operator_attested_debit_reconciliation'),
  }),
  OperatingLossCovered: fields({
    accountId: id,
    resource: choices('v5', 'anlasFixed', 'anlasPurchased'),
    amount: nonnegative,
    lossEventId: id,
  }),
  V5CapacityObserved: fields({
    accountId: id,
    observationEventId: id,
    capacityGap: nonnegative,
    difference: amount,
    tolerance: nonnegative,
    status: choices('source_below_local', 'source_above_local', 'within_precision'),
  }),
  StorageTargetRegistered: fields({
    storageTargetId: id,
    kind: choices('local', 'r2', 'pikpak'),
    namespace: id,
    credentialRef: nullable(id),
  }),
  ArtifactReplicaRecorded: fields(replicaFields),
  RequestAliasRegistered: fields({
    memberId: id,
    operation,
    keyHash: hash,
    intentFingerprint: hash,
    requestId: id,
    jobId: id,
    contextId: id,
    originalCommandId: id,
  }),
  ImportSourceRegistered: fields({
    sourceId: id,
    sourceSha256: hash,
    sourceCommit: id,
    cutoffSeq: sequence,
    cutoffAt: integer,
    manifestSha256: hash,
    route: choices('A', 'B'),
  }),
  OpeningMemberRecorded: fields({
    memberId: id,
    displayName: string,
    status: choices('active', 'disabled', 'removed'),
    acceptingJobs: bool,
    admissionEpoch: integer,
    createdAt: integer,
    origin,
  }),
  OpeningGroupRecorded: fields({ groupId: id, name: string, share: number, origin }),
  OpeningGroupMemberRecorded: fields({ memberId: id, groupId: id, weight: number, origin }),
  OpeningPolicyRecorded: fields({ ...policyFields, origin }),
  OpeningAccountRecorded: fields({
    accountId: id,
    displayName: string,
    enabled: bool,
    credentialEpoch: positive,
    cooldownUntil: nullable(integer),
    cooldownReason: nullable(id),
    ordinaryStrikes: integer,
    capacityStrikes: integer,
    capacityLimitedUntil: nullable(integer),
    capacityLastErrorAt: nullable(integer),
    origin,
  }),
  OpeningWalletRecorded: fields({
    accountId: id,
    memberId: id,
    available: signedResources,
    origin,
  }),
  OpeningChargeRecorded: fields({ ...chargeFields, origin }),
  OpeningQuotaRecorded: fields({ ...quotaFields, origin }),
  OpeningOperatingRecorded: fields({ ...operatingFields, origin }),
  OpeningFundingRecorded: fields({ ...fundingFields, origin }),
  OpeningInputRecorded: fields({
    inputId: id,
    memberId: id,
    inputSchemaVersion: choices(1),
    completeness: choices('complete', 'partial', 'unavailable'),
    inputSha256: nullable(hash),
    logicalBytes: integer,
    origin,
  }),
  OpeningRequestRecorded: fields({
    requestId: id,
    memberId: id,
    inputId: id,
    contextId: nullable(id),
    source: id,
    operation,
    model: nullable(id),
    sampleCount: positive,
    seedMode: choices('fixed', 'random', 'not_applicable', 'unknown'),
    confirmation,
    acceptedAt: integer,
    retiredAt: nullable(integer),
    origin,
  }),
  OpeningJobRecorded: fields({
    jobId: id,
    requestId: id,
    createdAt: nullable(integer),
    sampleIndex: integer,
    actualSeed: nullable(integer),
    status: choices('queued', 'pending', 'running', 'done', 'failed'),
    queueSeq: nullable(sequence),
    queueExpiresAt: nullable(integer),
    deadlineAt: nullable(integer),
    admissionEpoch: integer,
    artifactId: nullable(id),
    cacheKey: nullable(hash),
    completedAt: nullable(integer),
    errorCode: nullable(id),
    errorStatus: nullable(positive),
    cancellationReason: nullable(id),
    lateResultAt: nullable(integer),
    freeSucceededAt: nullable(integer),
    freeWarningThreshold: nullable(positive),
    retiredAt: nullable(integer),
    origin,
  }),
  OpeningAttemptRecorded: fields({
    attemptId: id,
    jobId: id,
    accountId: id,
    credentialEpoch: nullable(positive),
    authorizedAt: nullable(integer),
    response,
    candidateMode: choices('known', 'alternatives', 'unknown'),
    upstreamCandidates: list(candidate),
    actualConsumption: nullable(resources),
    reviewStatus: nullable(choices('pending', 'estimated_closed', 'result_recovered')),
    firstUnknownAt: nullable(integer),
    reviewDeadlineAt: nullable(integer),
    reviewClosedAt: nullable(integer),
    lossEventId: nullable(id),
    closureEventId: nullable(id),
    preparedArtifactId: nullable(id),
    preparedFile: nullable(id),
    preparedMimeType: nullable(id),
    preparedByteLength: nullable(positive),
    preparedSha256: nullable(hash),
    origin,
  }),
  OpeningArtifactRecorded: fields({
    artifactId: id,
    memberId: id,
    inputId: id,
    originalAttemptId: nullable(id),
    operation,
    mimeType: id,
    byteLength: positive,
    sha256: hash,
    publishedAt: integer,
    retiredAt: nullable(integer),
    origin,
  }),
  OpeningStorageTargetRecorded: fields({
    storageTargetId: id,
    kind: choices('local', 'r2', 'pikpak'),
    namespace: id,
    credentialRef: nullable(id),
    origin,
  }),
  OpeningReplicaRecorded: fields({ ...replicaFields, origin }),
  OpeningIdempotencyRecorded: fields({
    originalMemberId: id,
    operation,
    keyHash: hash,
    intentFingerprint: hash,
    originalRequestId: id,
    originalJobId: id,
    originalCommandId: id,
    requestId: nullable(id),
    commitSeq: nullable(sequence),
    contextId: nullable(id),
    resolution,
    originRef: id,
    archivedReceipt: nullable(receipt),
    createdAt: integer,
    origin,
  }),
} satisfies Record<string, Parser<unknown>>;

export type RelationalPayloads = {
  [K in keyof typeof relationalPayloadParsers]: Parsed<(typeof relationalPayloadParsers)[K]>;
};
export type RelationalFact = {
  [K in keyof RelationalPayloads]: { eventType: K; payload: RelationalPayloads[K] };
}[keyof RelationalPayloads];

export const relationalFactStreams = {
  member: [
    'MemberRegistered',
    'MemberStatusChanged',
    'AdmissionChanged',
    'MemberEligibilityChanged',
    'OpeningMemberRecorded',
  ],
  sharing: [
    'SharingConfigured',
    'GroupConfigured',
    'GroupMemberAssigned',
    'GroupMemberRemoved',
    'FallbackPolicyChanged',
    'FreePolicyChanged',
    'LossPolicyChanged',
    'AccountBound',
    'OpeningGroupRecorded',
    'OpeningGroupMemberRecorded',
    'OpeningPolicyRecorded',
  ],
  account: [
    'AccountRegistered',
    'CredentialRotated',
    'AvailabilityChanged',
    'CooldownChanged',
    'QuotaObserved',
    'QuotaStatusChanged',
    'QuotaBaselineAdopted',
    'RefillApplied',
    'AllocationApplied',
    'FundsTransferred',
    'FundingRegistered',
    'FundingReceived',
    'ResourceGapObserved',
    'ReturnClassified',
    'DebitsConfirmed',
    'OperatingLossCovered',
    'V5CapacityObserved',
    'QuotaReserved',
    'GenerationSettled',
    'GenerationRefunded',
    'OpeningAccountRecorded',
    'OpeningWalletRecorded',
    'OpeningChargeRecorded',
    'OpeningQuotaRecorded',
    'OpeningOperatingRecorded',
    'OpeningFundingRecorded',
  ],
  request: [
    'RequestAccepted',
    'JobAccepted',
    'DispatchAuthorized',
    'ResponseEvidenceRecorded',
    'ArtifactPrepared',
    'JobCompleted',
    'JobFailed',
    'LateResultRecovered',
    'CachedResultLinked',
    'FreeImageSucceeded',
    'JobQueued',
    'WaitingJobsCancelled',
    'ReviewOpened',
    'ReviewClosed',
    'JobHistoryRetired',
    'RequestAliasRegistered',
    'DiagnosticRecorded',
    'OpeningRequestRecorded',
    'OpeningJobRecorded',
    'OpeningAttemptRecorded',
  ],
  artifact: [
    'ArtifactPublished',
    'ArtifactReplicaRecorded',
    'ArtifactHistoryRetired',
    'OpeningArtifactRecorded',
    'OpeningReplicaRecorded',
  ],
  storage: ['StorageTargetRegistered', 'OpeningStorageTargetRecorded'],
  input: ['InputRegistered', 'OpeningInputRecorded'],
  context: ['ContextRegistered'],
  import: ['ImportSourceRegistered'],
  idempotency: ['OpeningIdempotencyRecorded'],
} as const satisfies Record<string, readonly (keyof RelationalPayloads)[]>;

export function relationalFactStream(eventType: keyof RelationalPayloads): string {
  const owner = Object.entries(relationalFactStreams).find(([, types]) =>
    (types as readonly string[]).includes(eventType),
  );
  if (!owner) {
    throw new TypeError('Relational fact has no stream owner.');
  }
  return owner[0];
}

export function parseRelationalFact(value: unknown): RelationalFact {
  const header = fields({ eventType: id, payload: (entry: unknown) => entry })(value);
  if (!Object.hasOwn(relationalPayloadParsers, header.eventType)) {
    throw new TypeError('Unsupported relational event type.');
  }
  assertPublicDomainFact(header.payload);
  if (Buffer.byteLength(canonicalJson(header.payload)) > 4 * 1024 * 1024) {
    throw new RangeError('Relational fact exceeds the payload limit.');
  }
  const payload = relationalPayloadParsers[header.eventType as keyof RelationalPayloads](
    header.payload,
  );
  const row = payload as Record<string, unknown>;
  if (row.response) {
    const response = row.response as RelationalPayloads['ResponseEvidenceRecorded']['response'];
    if (
      response.upstreamHttpStatus !== null &&
      (response.upstreamHttpStatus < 100 || response.upstreamHttpStatus > 599)
    ) {
      invalid();
    }
    if (
      response.dispatchState === 'not_sent' &&
      [
        response.upstreamStartedAt,
        response.upstreamHeadersAt,
        response.upstreamCompletedAt,
        response.upstreamHttpStatus,
      ].some((entry) => entry !== null)
    ) {
      invalid();
    }
  }
  if (typeof row.actualSeed === 'number' && row.actualSeed > 4294967295) {
    invalid();
  }
  if (
    (typeof row.share === 'number' && row.share < 0) ||
    (typeof row.weight === 'number' && row.weight < 0) ||
    (typeof row.secondsPerPercent === 'number' && row.secondsPerPercent < 0)
  ) {
    invalid();
  }
  if (
    header.eventType === 'CredentialRotated' &&
    row.credentialEpoch !== (row.beforeEpoch as number) + 1
  ) {
    invalid();
  }
  if (row.verifiedAt !== undefined && row.verifiedAt !== null && row.uploadedAt === null) {
    invalid();
  }
  if (header.eventType === 'OpeningChargeRecorded') {
    if (
      (row.status === 'reserved' &&
        (row.resolution !== 'live' || row.terminalAt !== null || row.terminalEventId !== null)) ||
      (row.status !== 'reserved' && (row.terminalAt === null || row.terminalEventId === null)) ||
      (row.resolution === 'live' &&
        (row.jobId === null || row.walletAccountId === null || row.walletMemberId === null)) ||
      (row.jobId !== null && row.jobId !== row.originalJobId) ||
      (row.walletAccountId === null) !== (row.walletMemberId === null) ||
      (row.walletAccountId !== null &&
        (row.walletAccountId !== row.originalAccountId ||
          row.walletMemberId !== row.originalMemberId)) ||
      (row.attemptId !== null && (row.jobId === null || row.walletAccountId === null))
    ) {
      invalid();
    }
  }
  if (header.eventType === 'OpeningIdempotencyRecorded') {
    if (
      (row.commitSeq === null) === (row.archivedReceipt === null) ||
      (row.resolution === 'live' &&
        (row.requestId === null || row.commitSeq === null || row.contextId === null)) ||
      (row.requestId !== null && row.requestId !== row.originalRequestId)
    ) {
      invalid();
    }
  }
  if (
    header.eventType === 'OpeningInputRecorded' &&
    (row.completeness === 'unavailable'
      ? row.inputSha256 !== null || row.logicalBytes !== 0
      : row.inputSha256 === null)
  ) {
    invalid();
  }
  if (header.eventType === 'QuotaReserved' || header.eventType === 'OpeningChargeRecorded') {
    const amounts = row.resources as RelationalPayloads['QuotaReserved']['resources'];
    if (
      (row.billingMode === 'free' && Object.values(amounts).some((value) => value !== '0')) ||
      (row.billingMode === 'v5' &&
        (amounts.anlasFixed !== '0' || amounts.anlasPurchased !== '0')) ||
      ((row.billingMode === 'anlas' || row.billingMode === 'anlas_fallback') &&
        amounts.v5 !== '0') ||
      (row.billingMode !== 'anlas_fallback' &&
        (row.v5Equivalent !== '0' || row.sharedV5Reserved !== '0'))
    ) {
      invalid();
    }
  }
  if (
    (header.eventType === 'JobAccepted' ||
      header.eventType === 'JobQueued' ||
      header.eventType === 'OpeningJobRecorded') &&
    (row.status === 'queued' || header.eventType === 'JobQueued') &&
    (row.queueSeq === null || row.queueSeq === '0')
  ) {
    invalid();
  }
  if (
    header.eventType === 'FundingReceived' &&
    ((row.source !== 'operator_share' && row.resource === 'v5') ||
      (row.source === 'operator_share' && row.memberId === null) ||
      (row.source === 'registered_external_funding' &&
        (row.receiptEventId === null || row.observationEventId === null)))
  ) {
    invalid();
  }
  if (
    header.eventType === 'ReturnClassified' &&
    row.kind === 'gap_correction' &&
    parseAmount(row.amount as string) <= 0n
  ) {
    invalid();
  }
  if (header.eventType === 'OpeningAttemptRecorded') {
    const prepared = [
      'preparedArtifactId',
      'preparedFile',
      'preparedMimeType',
      'preparedByteLength',
      'preparedSha256',
    ].map((key) => row[key]);
    if (
      (prepared.some((entry) => entry !== null) && prepared.some((entry) => entry === null)) ||
      (row.reviewStatus !== null &&
        (row.firstUnknownAt === null || row.reviewDeadlineAt === null || row.lossEventId === null))
    ) {
      invalid();
    }
  }
  if (
    header.eventType === 'OpeningJobRecorded' &&
    row.status === 'done' &&
    (row.artifactId === null || row.completedAt === null)
  ) {
    invalid();
  }
  return { eventType: header.eventType, payload } as RelationalFact;
}
