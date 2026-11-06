import type { AccountingJob, GenerationAttempt } from '../generation/accounting.ts';
import type { FairCharge, FairGroup, FairQuotaState, FairRequest, QuotaSnapshot, Resource, ResourceAmounts } from '../quota/fair-share.ts';
import type { QuotaAllocation, RefillFact, ResourceAllocation, MemberEligibility } from './quota-projections.ts';
import type { OperatingAccount } from '../quota/operating-accounting.ts';
import type { FreeImagePolicy } from '../quota/free-image-policy.ts';
import type { LossAlertPolicy } from '../quota/failure-accounting.ts';
import type { MemberGenerationConfirmation } from '../generation/member-contract.ts';
import type { describeStoredResult } from '../storage/result-files.ts';
import type { recordSharedCompletion } from '../quota/shared-capacity.ts';
import type { R2Image } from '../storage/r2-storage.ts';

export type EventTime = number | string;
type Evidence = { evidence?: Record<string, unknown>[] };
type Review = NonNullable<AccountingJob['generationReview']>;
type Refund = NonNullable<AccountingJob['generationRefund']>;
type ResponseEvidence = { dispatchState: 'request_started' | 'not_sent' | 'unknown'; upstreamStartedAt: string | null;
  upstreamHeadersAt: string | null; upstreamCompletedAt: string | null; upstreamHttpStatus: number | null };
type Summary = Partial<Pick<FairRequest, 'model' | 'width' | 'height' | 'steps' | 'allowPurchasedAnlas' | 'operation' | 'action'>> & { requestedSteps?: number; seed?: number };
export interface AcceptedJobFact {
  id: string; index: number; source?: string; status: 'queued' | 'pending' | 'done'; summary: Summary;
  queueSeq?: number; queueExpiresAt?: string; deadlineAt?: string; jobAdmissionVersion?: number;
  batchId?: string; cacheKey?: string; cost?: number; accountCost?: number;
}
type IdempotencyFact = { memberId: string; operation: string; keyHash: string; fingerprint: string; jobId: string;
  requestId: string; createdAt: string; normalizationContextRef: string; normalizationContextHash: string };
type PublishedArtifact = { id: string; file: string; mimeType: string; byteLength: number; sha256: string; operation: string;
  r2: R2Image | null; createdAt: string; attemptId: string | null; storageNamespace: 'gateway-local'; inputRef: string;
  model?: string; width?: number; height?: number; requestedSteps?: number; routedSteps?: number;
  cacheKey: string; mock: boolean; cost: number; accountCost: number };
type ConfigurationFact = { members: string[]; groups: FairGroup[]; configVersion: number; at?: number; actorId?: string | null; redistributeRemaining?: boolean };
type MemberAllocationFact = ConfigurationFact & { sourceId: string; targetId: string; at: number;
  autoJoined: { userId: string; groupId: string; memberWeight: number; share: number } | null;
  shareChanges: { userId: string; beforeShare: number; afterShare: number }[] };
type ChargeFact = Evidence & { jobId: string; charge: FairCharge; at: number; pricingVersion?: string };
type OperatingMoneyFact = Evidence & { id: string; resource: Resource; amount: string; request?: unknown; type?: string };
export type FundingReceivedFact = OperatingMoneyFact & (
  { source: 'operator_share'; memberId: string }
  | { source: 'declared_external_receipt'; destination: string }
  | { source: 'registered_external_funding'; destination: string; receiptEventId: string; snapshotAt: number; confirmationEventId?: string });
export type ResourceGapFact = OperatingMoneyFact & { observedAmount: string; beforeSnapshot: QuotaSnapshot; afterSnapshot: QuotaSnapshot;
  evidenceLevel: string; attribution: string; actualTaskConsumption: null };
export type ObservedReturnFact = OperatingMoneyFact & { gapEventId: string | null; snapshotAt: number; evidenceLevel: string };
export type CorrectedReturnFact = OperatingMoneyFact & { correctedAmount: string; previousGapEventId: string; source: 'declared_snapshot_correction'; evidenceLevel: string };
export type DebitsConfirmedFact = OperatingMoneyFact & { evidenceLevel: 'operator_attested_debit_reconciliation'; allocatedToMembers: string };
export type OperatingLossFact = OperatingMoneyFact & { source: 'operating_available'; lossEventId: string };

// Writes are checked here; persisted JSON remains an explicitly untyped read boundary until its reducer validates it.
export interface DomainPayloads {
  OpeningStateRecorded: { state: Record<string, unknown>; source?: string; [metadata: string]: unknown };
  LegacyFactRecorded: { record: Record<string, unknown>; order: unknown; source: 'legacy-ledger' };
  RequestAccepted: { requestId: string; memberId: string; source: string; operation: string; createdAt: string;
    inputRef: string; inputSha256: string; payloadBytes: number; jobs: AcceptedJobFact[];
    clientContract?: 'member-generation-v1'; confirmation?: MemberGenerationConfirmation; idempotency?: IdempotencyFact };
  RequestAliasRegistered: { jobId: string; requestId: string | null; memberId: string;
    idempotency: { operation: string; keyHash: string; fingerprint: string; contextRef: string; createdAt: string } };
  DispatchAuthorized: Evidence & { jobId: string; accountId: string; attemptId: string; authorizedAt: number; attempt: GenerationAttempt; chargeEventId?: string };
  ArtifactPrepared: { jobId: string; artifact: ReturnType<typeof describeStoredResult> & { id: string; attemptId: string }; response?: ResponseEvidence };
  ResponseEvidenceRecorded: { jobId: string; response: ResponseEvidence; source: 'legacy-query-projection'; sourceRowSha256: string };
  ArtifactPublished: { jobId: string; memberId: string; accountId: string | null; artifact: PublishedArtifact };
  ArtifactStorageChanged: { artifactId: string; phase: 'local_published'; file: string; mimeType?: string; sha256: string; byteLength: number }
    | { artifactId: string; phase: 'reserved' | 'verified' | 'local_removed' | 'failed'; r2: R2Image | null;
      lastAttemptAt: string | null; error: string | null; sha256?: string; byteLength?: number };
  ArtifactHistoryRetired: { artifactId: string; reason: 'owner_cleared_cache' | 'cache_limit' | 'automatic_retention'; at: number; jobIds: string[] };
  JobQueued: { jobId: string; queueSeq: number; queuedAt: string; queueExpiresAt: string | undefined; queueTotal: number };
  JobFailed: { jobId: string; failedAt: EventTime; errorStatus: number; errorCode: string; message: string;
    cancellationReason: string | null; cancelledAt: string | null; diagnosticRef?: string;
    charge: FairCharge | null; refund: Refund | null; review: Review | null; response?: ResponseEvidence };
  JobCompleted: { jobId: string; artifactId: string; accountId: string | null; completedAt: EventTime; charge: FairCharge | null; response?: ResponseEvidence };
  WaitingJobsCancelled: { jobIds: string[]; at: EventTime; reason: string; errorStatus: number; cancellationReason: string };
  JobHistoryRetired: { jobIds: string[]; reason: string; at: number };
  LateResultRecovered: Evidence & { jobId: string; artifactId: string; at: number; refundRetained: true; review: Review | null; response?: ResponseEvidence };
  FreeImageSucceeded: { jobId: string; memberId: string; succeededAt: number; warningThreshold: number | null };
  ReviewOpened: Evidence & { jobId: string; review: Review; refund: Refund; response: ResponseEvidence; actualConsumed: null };
  ReviewClosed: Evidence & { jobId: string; review: Review; reason: string; actualConsumed: null; upstreamCandidates: GenerationAttempt['upstreamCandidates'] | null };
  CachedResultLinked: { jobId: string; artifactId: string; accountId: string | null; completedAt: EventTime; localConsumption: ResourceAmounts };
  SharingConfigured: ConfigurationFact;
  AllocationChanged: ConfigurationFact;
  MemberTransferred: MemberAllocationFact;
  MemberRemoved: MemberAllocationFact;
  AccountBound: { accountId: string };
  QuotaAccountOpened: { memberIds: string[] };
  AccountRegistered: { accountId: string; name?: string; routeId?: number; enabled?: boolean; weight?: unknown; v5UsageIsNegative?: unknown; createdAt: string };
  AvailabilityChanged: { kind: 'quota'; status: string } | Evidence & { accountId: string; enabled?: boolean; name?: string; weight?: unknown;
    quotaCheckedAt?: unknown; quotaError?: unknown; updatedAt: string; reason: 'credential_replaced' | 'administrator' };
  QuotaObserved: { kind: 'official'; accountId: string; observation: Record<string, unknown>; at: EventTime }
    | { kind: 'reconciled'; snapshot: QuotaSnapshot; initial: boolean; at: number;
      matchedDebits: { anlasFixed: string; anlasPurchased: string }; matchedFallback: { fixed: string; purchased: string } };
  RefillApplied: RefillFact;
  AllocationApplied: ResourceAllocation & { configVersion: number; policyVersion: string; roundingVersion: string };
  QuotaReserved: ChargeFact;
  GenerationSettled: ChargeFact;
  GenerationRefunded: ChargeFact;
  V5CapacityObserved: Evidence & { capacityGap: string; gapEventId: string | null; reconciliation: NonNullable<FairQuotaState['v5Reconciliation']>; recordHistory: boolean };
  FundsTransferred: { kind: 'redistribution'; configVersion: number; members: string[];
    redistributions: { groupId: string; resource: Resource; allocations: QuotaAllocation[] }[] }
    | { kind: 'transfer' | 'member-removal'; configVersion: number; members: string[]; sourceId: string; targetId: string; transferred: ResourceAmounts };
  FundingRegistered: Evidence & { funding: OperatingAccount['pendingExternalFunding'][number] };
  FundingReceived: FundingReceivedFact;
  ResourceGapObserved: ResourceGapFact;
  ReturnClassified: (ObservedReturnFact & { kind: 'observed' }) | (CorrectedReturnFact & { kind: 'gap-correction' });
  DebitsConfirmed: DebitsConfirmedFact;
  OperatingLossCovered: OperatingLossFact;
  PolicyChanged: Evidence & ({ policy: 'autoAnlasFallback'; value: boolean; at: EventTime; actorId: string; actor?: string }
    | { policy: 'freeImagePolicy'; value: FreeImagePolicy; beforeVersion: number; version: number; actorId: string }
    | { policy: 'lossAlertPolicy'; value: LossAlertPolicy; beforeVersion: number; version: number; actorId: string }
    | { policy: 'enabled'; value: boolean });
  AdmissionChanged: { memberId: string; acceptingJobs: boolean; jobAdmissionVersion: number; at: EventTime };
  MemberEligibilityChanged: Omit<MemberEligibility, 'at'> & { at?: EventTime; actorId?: string };
  CooldownChanged: { cooldownUntil: EventTime | null; cooldownReason: string | null; ordinaryStrikeCount: number; capacityStrikeCount: number;
    capacityConcurrencyLimit: number | null; capacityLastErrorAt: string | null; jobId: string | null;
    sharedCooldown: NonNullable<NonNullable<NonNullable<Parameters<typeof recordSharedCompletion>[2]>['job']>['sharedCooldown']> | null };
}
