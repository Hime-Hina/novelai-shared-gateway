import { randomUUID } from 'node:crypto';
import { RelationalCommitReplayer } from './relational-replay.ts';
import { RelationalQuotaStore } from './relational-quota-store.ts';
import { RelationalReplicaStore } from './relational-replica-store.ts';
import type Database from 'better-sqlite3';
import { canonicalJson, domainDigest } from '../events/domain-events.ts';
import {
  readRelationalCommit,
  readRelationalReceipt,
  assertRelationalOpeningSource,
  type StoredRelationalCommit,
} from '../events/relational-event-store.ts';
import {
  relationalFactStream,
  type RelationalPayloads,
} from '../events/relational-fact-contracts.ts';
import type {
  RelationalCommand,
  RelationalEvent,
  RelationalEventInput,
  RelationalFact,
} from '../events/relational-events.ts';
import {
  planFairQuotaReservation,
  previewFairQuotaReservations,
  quoteFairRequest,
  decideFairQuotaCharge,
  advanceFairQuotaReservation,
  fairQuotaSnapshot,
  type FairQuotaReservationState,
  type FairCharge,
  type FairGroup,
  type ResourceAmounts,
} from '../quota/fair-share.ts';
import { parseAmount, formatAmount } from '../quota/resource-amounts.ts';
import { quotaAvailability } from '../quota/eligibility.ts';
import {
  sharedAccountConcurrency,
  sharedAccountReadyDelay,
  recordSharedCompletion,
} from '../quota/shared-capacity.ts';
import {
  assertFreeImageAdmission,
  freeImageSuccessWarningThreshold,
  type FreeImageUsage,
} from '../quota/free-image-policy.ts';
import {
  assertQueueAdmission,
  assertMemberAcceptsJobs,
  queueDeadline,
} from '../generation/queue-policy.ts';
import { nextPendingBatchJob } from '../generation/batch-requests.ts';
import { memberGenerationCapabilities } from '../generation/capabilities.ts';
import {
  parseMemberGenerationConfirmation,
  buildMemberGenerationQuote,
  type NormalizedMemberGeneration,
  assertMemberGenerationVersions,
  assertMemberGenerationCost,
  addMemberGenerationResources,
  assertMemberGenerationIntent,
} from '../generation/member-contract.ts';
import { generationRequestFingerprint } from '../generation/request-fingerprint.ts';
import {
  GENERATION_REVIEW_MS,
  generationChargeResources,
  generationUpstreamCandidates,
  generationResponseEvidence,
  type RefundEvidence,
} from '../generation/accounting.ts';
import type { GenerationDispatchOptions } from '../generation/execution.ts';
import {
  RelationalInputStore,
  type AcceptedInput,
  type ReusedGenerationInput,
} from './relational-input-store.ts';
import { relationalArtifactReferenceAvailable } from './relational-replica-io.ts';
import type { RelationalReplicaReadConfiguration } from './relational-replica-store.ts';
import { assertRelationalStorageFormat } from './relational-storage.ts';
import { assertStoredResultDescriptor, type describeStoredResult } from './result-files.ts';
import { errorDetailMessage } from '../http/public-errors.ts';
import { assertCurrentOwnerCommand, type MemberUser } from '../members/authentication.ts';
import {
  assertHistoryRetirementControls,
  assertRetirableRequest,
  assertHistoryRetirementCommit,
  historyRetirementIds,
  historyRetirementFingerprint,
  readHistoryJob,
  type HistoryRetirementControls,
} from './history-retirement.ts';

type Metadata = Omit<RelationalCommand, 'events' | 'expectedVersions' | 'receipt'> & {
  policyVersion: string;
  pricingVersion: string;
};
type Response = RelationalPayloads['ResponseEvidenceRecorded']['response'];
type Publication = ReturnType<typeof describeStoredResult>;
type DispatchCapture = Readonly<{
  jobId: string;
  attemptId: string;
  accountId: string;
  credentialEpoch: number;
  pst: string;
}>;
type Completion = Pick<
  NonNullable<Parameters<typeof recordSharedCompletion>[2]>,
  'kind' | 'retryAfterUntil'
>;
export type GenerationPhysicalCompletion = Completion & {
  attemptId: string;
  credentialEpoch: number;
  kind: NonNullable<Completion['kind']>;
};

export const relationalGenerationFacts = new Set<RelationalEvent['eventType']>([
  'ImportSourceRegistered',
  'OpeningAccountRecorded',
  'OpeningGroupRecorded',
  'OpeningGroupMemberRecorded',
  'OpeningPolicyRecorded',
  'OpeningWalletRecorded',
  'OpeningQuotaRecorded',
  'OpeningOperatingRecorded',
  'OpeningChargeRecorded',
  'OpeningJobRecorded',
  'OpeningArtifactRecorded',
  'CooldownChanged',
  'QuotaReserved',
  'DispatchAuthorized',
  'ResponseEvidenceRecorded',
  'ArtifactPrepared',
  'ArtifactPublished',
  'CachedResultLinked',
  'GenerationSettled',
  'GenerationRefunded',
  'JobCompleted',
  'JobFailed',
  'DiagnosticRecorded',
  'WaitingJobsCancelled',
  'JobQueued',
  'FreeImageSucceeded',
  'ReviewOpened',
  'ReviewClosed',
  'LateResultRecovered',
  'JobHistoryRetired',
]);
const zero = (): ResourceAmounts => ({ v5: '0', anlasFixed: '0', anlasPurchased: '0' });
const iso = (value: number | null) => (value === null ? undefined : new Date(value).toISOString());
function fail(message: string, statusCode = 409): never {
  throw Object.assign(new Error(message), { statusCode });
}
const resources = ['v5', 'anlasFixed', 'anlasPurchased'] as const;

interface Job {
  id: string;
  requestId: string;
  memberId: string;
  inputId: string;
  operation: Publication['operation'];
  source: string;
  status: string;
  sampleIndex: number;
  queueSeq: string | null;
  queueExpiresAt: number | null;
  deadlineAt: number | null;
  admissionEpoch: number;
  confirmation: string | null;
  memberStatus: string;
  authEpoch: string | null;
  acceptingJobs: number;
  currentAdmissionEpoch: number;
  artifactId: string | null;
  freeSucceededAt: number | null;
  retiredAt: number | null;
}
interface Charge {
  originalJobId: string;
  originalMemberId: string;
  originalAccountId: string;
  jobId: string | null;
  attemptId: string | null;
  status: 'reserved' | 'settled' | 'refunded';
  resolution: string;
  v5: string;
  anlasFixed: string;
  anlasPurchased: string;
  billingMode: 'free' | 'v5' | 'anlas' | 'anlas_fallback';
  estimated: boolean | null;
  v5Equivalent: string;
  sharedV5Reserved: string;
  configVersion: number;
  reservedAt: number;
  terminalAt: number | null;
  reservationEventId: string;
  terminalEventId: string | null;
}
interface Attempt {
  id: string;
  jobId: string;
  accountId: string;
  credentialEpoch: number | null;
  dispatchState: Response['dispatchState'];
  upstreamStartedAt: number | null;
  upstreamHeadersAt: number | null;
  upstreamCompletedAt: number | null;
  upstreamHttpStatus: number | null;
  upstreamCandidates: string;
  reviewStatus: string | null;
  reviewDeadlineAt: number | null;
  lossEventId: string | null;
  preparedArtifactId: string | null;
  preparedFile: string | null;
  preparedMimeType: string | null;
  preparedByteLength: number | null;
  preparedSha256: string | null;
}
type Policy = {
  enabled: number;
  boundAccountId: string | null;
  configVersion: number;
  autoAnlasFallback: number;
  freeVersion: number;
  freeEnabled: number;
  warningThreshold: number | null;
  hardLimit: number | null;
};

// 只提交事实与关系投影；网络连接、物理槽和文件发布由原执行/存储 owner 在事务外持有。
export class RelationalGenerationStore {
  private sqlite: Database.Database;
  private inputs: RelationalInputStore;
  private quotas: RelationalQuotaStore;
  private replicas: RelationalReplicaStore;
  private env: NodeJS.ProcessEnv;
  // 只绑定已返回的私有 IO 对象与原派发事实；弱引用不维持另一份 applied 状态。
  private dispatchCaptures = new WeakMap<DispatchCapture, string>();
  constructor(sqlite: Database.Database, env: NodeJS.ProcessEnv = process.env) {
    assertRelationalStorageFormat(sqlite);
    this.sqlite = sqlite;
    this.quotas = new RelationalQuotaStore(sqlite, env);
    this.replicas = new RelationalReplicaStore(sqlite, undefined, env);
    this.env = env;
    this.inputs = new RelationalInputStore(sqlite, env);
  }
  private owned<T>(run: () => T): T {
    if (!this.sqlite.inTransaction) {
      throw new Error('Generation writes require the owning command transaction.');
    }
    return this.sqlite.transaction(run)();
  }
  private job(id: string): Job {
    const row = this.sqlite
      .prepare<[string], Job>(
        `SELECT j.id,j.request_id AS requestId,r.member_id AS memberId,
      r.input_id AS inputId,r.operation,r.source,j.status,j.sample_index AS sampleIndex,CAST(j.queue_seq AS TEXT) AS queueSeq,
      j.queue_expires_at AS queueExpiresAt,j.deadline_at AS deadlineAt,j.admission_epoch AS admissionEpoch,r.confirmation,
      m.status AS memberStatus,m.auth_epoch AS authEpoch,m.accepting_jobs AS acceptingJobs,m.admission_epoch AS currentAdmissionEpoch,
      j.artifact_id AS artifactId,j.free_succeeded_at AS freeSucceededAt,j.retired_at AS retiredAt
      FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id JOIN members m ON m.id=r.member_id WHERE j.id=?`,
      )
      .get(id);
    if (!row) {
      fail('任务不存在', 404);
    }
    return row;
  }
  readWallet(accountId: string, memberId: string): ResourceAmounts | null {
    return (
      this.sqlite
        .prepare<[string, string], ResourceAmounts>(
          `SELECT v5_available AS v5,anlas_fixed_available AS anlasFixed,
      anlas_purchased_available AS anlasPurchased FROM wallets WHERE account_id=? AND member_id=?`,
        )
        .get(accountId, memberId) || null
    );
  }
  readCharge(jobId: string): Charge | null {
    const row = this.sqlite
      .prepare<[string], Omit<Charge, 'estimated'> & { estimated: number | null }>(
        `SELECT original_job_id AS originalJobId,original_member_id AS originalMemberId,
      original_account_id AS originalAccountId,job_id AS jobId,attempt_id AS attemptId,status,resolution,v5,anlas_fixed AS anlasFixed,
      anlas_purchased AS anlasPurchased,billing_mode AS billingMode,estimated,v5_equivalent AS v5Equivalent,shared_v5_reserved AS sharedV5Reserved,
      config_version AS configVersion,reserved_at AS reservedAt,terminal_at AS terminalAt,reservation_event_id AS reservationEventId,
      terminal_event_id AS terminalEventId FROM charges WHERE original_job_id=?`,
      )
      .get(jobId);
    if (!row) {
      return null;
    }
    return { ...row, estimated: row.estimated === null ? null : row.estimated === 1 };
  }
  private attempt(jobId: string): Attempt | null {
    return (
      this.sqlite
        .prepare<[string], Attempt>(
          `SELECT id,job_id AS jobId,account_id AS accountId,credential_epoch AS credentialEpoch,
      dispatch_state AS dispatchState,upstream_started_at AS upstreamStartedAt,upstream_headers_at AS upstreamHeadersAt,
      upstream_completed_at AS upstreamCompletedAt,upstream_http_status AS upstreamHttpStatus,upstream_candidates AS upstreamCandidates,
      review_status AS reviewStatus,review_deadline_at AS reviewDeadlineAt,loss_event_id AS lossEventId,
      prepared_artifact_id AS preparedArtifactId,prepared_file AS preparedFile,prepared_mime_type AS preparedMimeType,
      prepared_byte_length AS preparedByteLength,prepared_sha256 AS preparedSha256 FROM generation_attempts WHERE job_id=?`,
        )
        .get(jobId) || null
    );
  }
  private policy(): Policy {
    const row = this.sqlite
      .prepare<[], Policy>(
        `SELECT enabled,bound_account_id AS boundAccountId,config_version AS configVersion,
      auto_anlas_fallback AS autoAnlasFallback,free_policy_version AS freeVersion,free_enabled AS freeEnabled,
      free_warning_threshold AS warningThreshold,free_hard_limit AS hardLimit FROM sharing_policy WHERE id=1`,
      )
      .get();
    if (!row) {
      fail('共享账本尚未配置', 503);
    }
    return row;
  }
  private policyView(policy: Policy) {
    const groups = this.sqlite
      .prepare<[], { id: string; name: string; share: number }>(
        'SELECT id,name,share FROM sharing_groups ORDER BY id',
      )
      .all();
    const memberships = this.sqlite
      .prepare<[], { memberId: string; groupId: string; weight: number }>(
        'SELECT member_id AS memberId,group_id AS groupId,weight FROM group_members ORDER BY member_id',
      )
      .all();
    return {
      settings: {
        fairQuota: {
          enabled: policy.enabled === 1,
          autoAnlasFallback: policy.autoAnlasFallback === 1,
          freeImagePolicy: {
            version: policy.freeVersion,
            enabled: policy.freeEnabled === 1,
            warningThreshold: policy.warningThreshold,
            hardLimit: policy.hardLimit,
          },
          groups: groups.map((group): FairGroup => ({
            ...group,
            members: memberships.filter((m) => m.groupId === group.id).map((m) => m.memberId),
            memberWeights: Object.fromEntries(
              memberships.filter((m) => m.groupId === group.id).map((m) => [m.memberId, m.weight]),
            ),
          })),
        },
      },
    };
  }
  readMemberGenerationCapabilities(memberId: string) {
    return memberGenerationCapabilities(
      this.policyView(this.policy()),
      { id: memberId },
      { env: this.env },
    );
  }
  private memberGenerationAccess(memberId: string) {
    const member = this.sqlite
      .prepare<
        [string],
        { status: string; authEpoch: string | null; accepting: number; version: number }
      >(
        'SELECT status,auth_epoch AS authEpoch,accepting_jobs AS accepting,admission_epoch AS version FROM members WHERE id=?',
      )
      .get(memberId);
    if (member?.status !== 'active' || !member.authEpoch) {
      fail('成员授权已失效', 401);
    }
    return member;
  }
  readMemberFreeImageUsage(memberId: string, now = Date.now()): FreeImageUsage {
    return this.sqlite.transaction(() => {
      this.memberGenerationAccess(memberId);
      return this.freeUsage(memberId, this.policy(), now);
    })();
  }
  quoteMemberGeneration(
    memberId: string,
    normalized: NormalizedMemberGeneration,
    now: number,
    activePayloadBytes: number,
  ) {
    return this.sqlite.transaction(() => {
      const member = this.memberGenerationAccess(memberId);
      const policy = this.policy();
      let state: FairQuotaReservationState | undefined;
      let unavailable: unknown;
      try {
        if (!policy.boundAccountId) {
          fail('共享账本尚未绑定账号', 503);
        }
        state = this.reservationState({ memberId }, policy.boundAccountId, now).state;
        Object.assign(
          state,
          this.quotas.estimateGenerationQuota(policy.boundAccountId, memberId, now),
        );
      } catch (error) {
        state = undefined;
        unavailable = error;
      }
      const wallet = policy.boundAccountId
        ? this.readWallet(policy.boundAccountId, memberId)
        : null;
      const balance = wallet
        ? {
            v5Available: wallet.v5,
            anlasFixedAvailable: wallet.anlasFixed,
            anlasPurchasedAvailable: wallet.anlasPurchased,
          }
        : undefined;
      const quota = policy.boundAccountId
        ? this.quotas.readAccountQuota(policy.boundAccountId)
        : null;
      const snapshot = quota
        ? fairQuotaSnapshot({
            id: quota.accountId,
            quotaCheckedAt: iso(quota.observedAt),
            v5UsagePercent: quota.observedV5,
            v5UsageIsNegative: quota.observedV5 !== null && parseAmount(quota.observedV5) < 0n,
            quotaFixed: quota.observedAnlasFixed,
            quotaPurchased: quota.observedAnlasPurchased,
            v5UsageTimeUntilNextPercent: quota.secondsPerPercent,
          })
        : null;
      return buildMemberGenerationQuote(normalized, {
        now,
        env: this.env,
        versions: this.readMemberGenerationCapabilities(memberId).versions,
        member: {
          id: memberId,
          enabled: true,
          acceptingJobs: member.accepting === 1,
          jobAdmissionVersion: member.version,
        },
        jobs: this.activeJobs(),
        activePayloadBytes,
        freeUsage: this.freeUsage(memberId, policy, now),
        freeEnabled: policy.freeEnabled === 1,
        preview: (request) =>
          decideFairQuotaCharge(
            state?.balance ?? balance,
            policy.autoAnlasFallback === 1,
            state?.snapshot ?? snapshot,
            request,
            now,
          ),
        reserve: (request) => {
          if (!state) {
            throw unavailable;
          }
          advanceFairQuotaReservation(state, planFairQuotaReservation(state, request, now));
        },
      });
    })();
  }
  private authorize(metadata: Metadata, job: Job, systemOnly = false) {
    if (metadata.actorKind === 'member' && (systemOnly || metadata.actorId !== job.memberId)) {
      fail('任务命令归属不符', 403);
    }
  }
  private authorizeResult(metadata: Metadata) {
    if (metadata.actorKind !== 'system' || metadata.actorId !== null) {
      fail('结果准备与发布只接受执行器 IO', 403);
    }
  }
  private event(
    fact: RelationalFact,
    job: Job | null,
    accountId: string | null,
    now: number,
    eventId = randomUUID(),
  ): RelationalEventInput {
    const kind = relationalFactStream(fact.eventType);
    let streamId: string;
    if (kind === 'account') {
      streamId = `account:${accountId}`;
    } else if (kind === 'artifact' && 'artifactId' in fact.payload) {
      streamId = `artifact:${fact.payload.artifactId}`;
    } else if (kind === 'request' && job) {
      streamId = `request:${job.requestId}`;
    } else if (kind === 'storage' && 'storageTargetId' in fact.payload) {
      streamId = `storage:${fact.payload.storageTargetId}`;
    } else {
      throw new Error('Generation command has an unsupported stream owner.');
    }
    return {
      ...fact,
      eventId,
      streamId,
      accountId,
      memberId: job?.memberId || null,
      occurredAt: now,
      policyVersion: '',
      pricingVersion: '',
    };
  }
  private commit(
    metadata: Metadata,
    events: RelationalEventInput[],
    receipt: RelationalCommand['receipt'],
  ) {
    const expectedVersions: Record<string, string> = {};
    for (const e of events) {
      expectedVersions[e.streamId] =
        this.sqlite
          .prepare<[string], { v: string }>(
            'SELECT CAST(stream_version AS TEXT) AS v FROM stream_heads WHERE stream_id=?',
          )
          .get(e.streamId)?.v || '0';
    }
    const { policyVersion, pricingVersion, ...command } = metadata;
    return new RelationalCommitReplayer(this.sqlite, this).appendCommit(
      {
        ...command,
        receipt,
        expectedVersions,
        events: events.map((e) => ({ ...e, policyVersion, pricingVersion })),
      },
      this.inputs,
    );
  }
  private nextQueueSeq(): string {
    return String(
      BigInt(
        this.sqlite
          .prepare<[], { v: string }>(
            'SELECT CAST(COALESCE(MAX(queue_seq),0) AS TEXT) AS v FROM generation_jobs',
          )
          .get()!.v,
      ) + 1n,
    );
  }
  private activeJobs() {
    return this.sqlite
      .prepare<[], { status: string; memberId: string }>(
        `SELECT j.status,r.member_id AS memberId FROM generation_jobs j
      JOIN generation_requests r ON r.id=j.request_id WHERE j.status IN ('queued','pending','running')`,
      )
      .all();
  }
  readAcceptedGenerationReceipt(
    metadata: Pick<Metadata, 'commandId' | 'fingerprint' | 'actorKind' | 'actorId'>,
    mapping: NonNullable<ReturnType<RelationalInputStore['readGenerationIdempotency']>>,
  ) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Generation receipt requires the current authenticated transaction.');
    }
    const member = this.sqlite
      .prepare<[string | null], { status: string; epoch: string | null }>(
        'SELECT status,auth_epoch AS epoch FROM members WHERE id=?',
      )
      .get(metadata.actorId);
    if (metadata.actorKind !== 'member' || member?.status !== 'active' || !member.epoch) {
      fail('成员授权已失效', 401);
    }
    const prior = readRelationalReceipt(this.sqlite, metadata);
    const cacheHit =
      prior &&
      readRelationalCommit(this.sqlite, prior.commitSeq).events.some(
        (event) => event.eventType === 'CachedResultLinked',
      );
    const request = this.sqlite
      .prepare<
        [string | null],
        {
          memberId: string;
          contextId: string | null;
          sampleCount: number;
          retiredAt: number | null;
        }
      >(
        'SELECT member_id AS memberId,context_id AS contextId,sample_count AS sampleCount,retired_at AS retiredAt FROM generation_requests WHERE id=?',
      )
      .get(mapping.requestId);
    const jobs = this.sqlite
      .prepare<[string | null], { id: string; retiredAt: number | null }>(
        'SELECT id,retired_at AS retiredAt FROM generation_jobs WHERE request_id=? ORDER BY sample_index',
      )
      .all(mapping.requestId);
    if (
      !prior ||
      mapping.resolution !== 'live' ||
      mapping.requestId !== mapping.originalRequestId ||
      prior.commitSeq !== mapping.commitSeq ||
      mapping.originalCommandId !== metadata.commandId ||
      mapping.intentFingerprint !== metadata.fingerprint ||
      request?.memberId !== metadata.actorId ||
      request.retiredAt !== null ||
      jobs.some((job) => job.retiredAt !== null) ||
      jobs.length !== request.sampleCount ||
      jobs[0]?.id !== mapping.originalJobId ||
      canonicalJson(prior.receipt) !==
        canonicalJson({
          requestId: mapping.requestId,
          jobIds: jobs.map((j) => j.id),
          ...(cacheHit ? { cacheHit: true } : {}),
        })
    ) {
      throw Object.assign(new Error('原请求的任务或回执不可领取；再次生成须使用新操作键。'), {
        statusCode: 410,
        code: 'result_unavailable',
      });
    }
    this.inputs.assertAcceptedGenerationIntent(
      mapping.requestId!,
      metadata.actorId!,
      prior.commitSeq,
      mapping.keyHash,
      mapping.sourceEventId,
    );
    return prior;
  }
  async findReusableGeneration(
    input: Pick<ReusedGenerationInput, 'memberId' | 'request' | 'context' | 'seedMode'> & {
      noCache: boolean;
      allowActive: boolean;
    },
    configuration: RelationalReplicaReadConfiguration,
  ) {
    if (this.sqlite.inTransaction) {
      throw new Error('Reuse lookup cannot hold a transaction across file IO.');
    }
    if (input.noCache || input.context.kind === 'member') {
      return null;
    }
    const { cacheKey } = this.inputs.generationInputIntent(
      input.memberId,
      input.request,
      input.context,
      input.seedMode,
      false,
    );
    if (!cacheKey) {
      return null;
    }
    const cached = this.sqlite
      .prepare<[string, string], { jobId: string; artifactId: string }>(
        `SELECT j.id AS jobId,a.id AS artifactId FROM generation_jobs j
       JOIN generation_requests r ON r.id=j.request_id JOIN generation_attempts t ON t.job_id=j.id
       JOIN artifacts a ON a.original_attempt_id=t.id
       WHERE j.cache_key=? AND r.member_id=? AND a.retired_at IS NULL ORDER BY a.published_at DESC,a.id DESC LIMIT 1`,
      )
      .all(cacheKey, input.memberId);
    for (const candidate of cached) {
      this.inputs.readGenerationReuseProof(candidate.jobId);
      try {
        if (
          await relationalArtifactReferenceAvailable(
            this.sqlite,
            candidate.artifactId,
            configuration,
          )
        ) {
          return {
            kind: 'artifact' as const,
            sourceJobId: candidate.jobId,
            artifactId: candidate.artifactId,
          };
        }
      } catch {}
    }
    if (!input.allowActive) {
      return null;
    }
    const active = this.sqlite
      .prepare<[string, string], { id: string }>(
        `SELECT j.id FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id
       WHERE j.cache_key=? AND r.member_id=? AND r.retired_at IS NULL AND j.retired_at IS NULL
       AND j.status IN ('queued','running') ORDER BY j.queue_seq DESC,j.id DESC LIMIT 1`,
      )
      .get(cacheKey, input.memberId);
    if (!active) {
      return null;
    }
    this.inputs.readGenerationReuseProof(active.id);
    return { kind: 'active' as const, sourceJobId: active.id };
  }

  reuseGeneration(metadata: Metadata, input: ReusedGenerationInput) {
    return this.owned(() => {
      const member = this.sqlite
        .prepare<
          [string],
          { status: string; epoch: string | null; accepting: number; version: number }
        >(
          'SELECT status,auth_epoch AS epoch,accepting_jobs AS accepting,admission_epoch AS version FROM members WHERE id=?',
        )
        .get(input.memberId);
      if (
        metadata.actorKind !== 'member' ||
        metadata.actorId !== input.memberId ||
        member?.status !== 'active' ||
        !member.epoch
      ) {
        fail('成员授权已失效', 401);
      }
      if (input.noCache !== false) {
        fail('缓存复用要求显式允许缓存', 400);
      }
      if (input.idempotency) {
        const mapping = this.inputs.readGenerationIdempotency(
          input.memberId,
          'generate',
          input.idempotency.keyHash,
        );
        if (mapping) {
          if (mapping.intentFingerprint !== metadata.fingerprint) {
            fail('操作键对应的意图已变化', 409);
          }
          return this.readAcceptedGenerationReceipt(
            { ...metadata, commandId: mapping.originalCommandId },
            mapping,
          );
        }
      }
      const prior = readRelationalReceipt(this.sqlite, metadata);
      if (prior) {
        if (!input.idempotency) {
          return prior;
        }
        const mapping = this.inputs.readGenerationIdempotency(
          input.memberId,
          'generate',
          input.idempotency.keyHash,
        );
        if (!mapping) {
          fail('原操作键已不可领取', 410);
        }
        return this.readAcceptedGenerationReceipt(metadata, mapping);
      }
      assertMemberAcceptsJobs({
        acceptingJobs: member.accepting === 1,
        jobAdmissionVersion: member.version,
      });
      const source = this.job(input.sourceJobId),
        policy = this.policy();
      if (source.memberId !== input.memberId || !policy.enabled || !policy.boundAccountId) {
        fail('共享账本尚未配置', 503);
      }
      if (input.cached) {
        const artifact = this.replicas.readArtifact(input.cached.artifactId);
        const origin = this.sqlite
          .prepare<[string], { jobId: string }>(
            'SELECT t.job_id AS jobId FROM artifacts a JOIN generation_attempts t ON t.id=a.original_attempt_id WHERE a.id=?',
          )
          .get(artifact.id);
        if (
          artifact.retiredAt !== null ||
          artifact.memberId !== input.memberId ||
          origin?.jobId !== source.id
        ) {
          fail('缓存产物不属于原来源');
        }
        const reference = this.replicas.readArtifactCacheReference(artifact.id);
        if (!reference.persistent && !reference.local) {
          fail('原产物没有可引用来源');
        }
      } else if (
        source.retiredAt !== null ||
        !['queued', 'running'].includes(source.status) ||
        source.admissionEpoch !== member.version
      ) {
        fail('原活动任务已变化');
      }
      this.refillForGeneration(metadata, policy.boundAccountId);
      const readiness = this.assertReuseReadiness(
        source,
        metadata.recordedAt,
        input.cached ? undefined : source.admissionEpoch,
      );
      if (!readiness.account.pst) {
        fail('上游私有凭据尚未恢复', 503);
      }
      const staged = this.inputs.stageGenerationReuse(metadata, input, member.version);
      if (!staged.events.length) {
        return { replayed: false, events: [], receipt: staged.receipt };
      }
      const events: RelationalEventInput[] = staged.events;
      if (input.cached) {
        events.push({
          eventType: 'CachedResultLinked',
          payload: { jobId: input.cached.jobId, artifactId: input.cached.artifactId },
          streamId: `request:${input.cached.requestId}`,
          memberId: input.memberId,
          accountId: null,
          occurredAt: metadata.recordedAt,
          policyVersion: metadata.policyVersion,
          pricingVersion: metadata.pricingVersion,
        });
      }
      return this.commit(metadata, events, staged.receipt);
    });
  }
  private assertReuseReadiness(job: Job, at: number, expectedAdmissionEpoch?: number) {
    if (job.memberStatus !== 'active') {
      fail('复用成员当前不可用', 403);
    }
    assertMemberAcceptsJobs(
      { acceptingJobs: job.acceptingJobs === 1, jobAdmissionVersion: job.currentAdmissionEpoch },
      expectedAdmissionEpoch,
    );
    const policy = this.policy();
    if (!policy.enabled || !policy.boundAccountId) {
      fail('共享账本尚未配置', 503);
    }
    return this.reservationFacts(job, policy.boundAccountId, at);
  }
  readQueuedGenerationIds(limit = 100): string[] {
    this.assertRuntimePage('', limit);
    return this.sqlite
      .prepare<[number], { id: string }>(
        "SELECT id FROM generation_jobs WHERE status='queued' ORDER BY queue_seq LIMIT ?",
      )
      .all(limit)
      .map((row) => row.id);
  }
  private assertRuntimePage(afterId: string, limit: number) {
    if (typeof afterId !== 'string' || !Number.isInteger(limit) || limit < 1 || limit > 100) {
      throw new TypeError('Invalid generation runtime page.');
    }
  }
  readInterruptedGenerationIds(afterId = '', limit = 100): string[] {
    this.assertRuntimePage(afterId, limit);
    return this.sqlite
      .prepare<[string, number], { id: string }>(
        `SELECT id FROM generation_jobs INDEXED BY idx_jobs_active_request
        WHERE status IN ('queued','pending','running') AND status='running' AND id>? ORDER BY id LIMIT ?`,
      )
      .all(afterId, limit)
      .map((row) => row.id);
  }
  readExpiredGenerationIds(now: number, limit = 100): string[] {
    this.assertRuntimePage('', limit);
    return this.sqlite
      .prepare<[number, number, number], { id: string }>(
        `SELECT id FROM generation_jobs INDEXED BY idx_jobs_active_request WHERE status IN ('queued','pending','running') AND
      (deadline_at<=? OR (status IN ('queued','pending') AND queue_expires_at<=?)) ORDER BY id LIMIT ?`,
      )
      .all(now, now, limit)
      .map((row) => row.id);
  }
  readDueGenerationReviewIds(now: number, limit = 100): string[] {
    this.assertRuntimePage('', limit);
    return this.sqlite
      .prepare<[number, number], { id: string }>(
        `SELECT job_id AS id FROM generation_attempts INDEXED BY idx_attempts_review WHERE review_status='pending'
      AND review_deadline_at<=? ORDER BY review_deadline_at,job_id LIMIT ?`,
      )
      .all(now, limit)
      .map((row) => row.id);
  }
  readNextGenerationWakeAt(): number | null {
    return this.sqlite
      .prepare<[], { at: number | null }>(
        `SELECT MIN(at) AS at FROM (
      SELECT deadline_at AS at FROM generation_jobs INDEXED BY idx_jobs_active_request WHERE status IN ('queued','pending','running')
      UNION ALL SELECT queue_expires_at FROM generation_jobs INDEXED BY idx_jobs_active_request
      WHERE status IN ('queued','pending','running') AND status!='running'
      UNION ALL SELECT review_deadline_at FROM generation_attempts INDEXED BY idx_attempts_review WHERE review_status='pending'
    )`,
      )
      .get()!.at;
  }
  readBoundGenerationAccount() {
    const row = this.sqlite
      .prepare<[], { id: string; enabled: number; cooldownUntil: number | null }>(
        `SELECT a.id,a.enabled,a.cooldown_until AS cooldownUntil FROM upstream_accounts a
      JOIN sharing_policy p ON p.bound_account_id=a.id WHERE p.id=1`,
      )
      .get();
    return row ? { ...row, enabled: row.enabled === 1 } : null;
  }
  readGenerationExecutionState(jobId: string) {
    const job = this.job(jobId),
      attempt = this.attempt(jobId);
    const origin = this.sqlite
      .prepare<
        [string, string],
        {
          eventId: string;
          policyVersion: string;
          pricingVersion: string;
        }
      >(
        `SELECT event_id AS eventId,policy_version AS policyVersion,pricing_version AS pricingVersion
      FROM domain_events WHERE stream_id=? AND (event_type='RequestAccepted' OR
      (event_type='DispatchAuthorized' AND json_extract(payload,'$.jobId')=?))
      ORDER BY stream_version DESC LIMIT 1`,
      )
      .get(`request:${job.requestId}`, jobId);
    const input = this.sqlite
      .prepare<[string], { bytes: number }>(
        'SELECT logical_bytes AS bytes FROM generation_inputs WHERE id=?',
      )
      .get(job.inputId);
    if (!origin || !input) {
      fail('任务执行缺少原输入或提交元数据', 410);
    }
    return {
      id: job.id,
      requestId: job.requestId,
      memberId: job.memberId,
      operation: job.operation,
      status: job.status,
      deadlineAt: job.deadlineAt,
      queueExpiresAt: job.queueExpiresAt,
      payloadBytes: input.bytes,
      attemptId: attempt?.id ?? null,
      credentialEpoch: attempt?.credentialEpoch ?? null,
      ...origin,
    };
  }
  acceptGeneration(metadata: Metadata, input: AcceptedInput) {
    return this.owned(() => {
      const m = this.sqlite
        .prepare<
          [string],
          { status: string; epoch: string | null; accepting: number; version: number }
        >(
          'SELECT status,auth_epoch AS epoch,accepting_jobs AS accepting,admission_epoch AS version FROM members WHERE id=?',
        )
        .get(input.memberId);
      if (
        !m ||
        m.status !== 'active' ||
        !m.epoch ||
        (metadata.actorKind === 'member' && metadata.actorId !== input.memberId)
      ) {
        fail('成员授权已失效', 401);
      }
      const prior = readRelationalReceipt(this.sqlite, metadata);
      if (prior) {
        return prior;
      }
      assertQueueAdmission(
        { jobs: this.activeJobs() },
        { id: input.memberId, acceptingJobs: m.accepting === 1, jobAdmissionVersion: m.version },
        undefined,
        this.env,
        input.jobs.length,
      );
      const first = input.jobs[0];
      const requestedDeadline = Math.min(
        first?.queueExpiresAt ?? Infinity,
        first?.deadlineAt ?? Infinity,
      );
      const deadline = Date.parse(
        queueDeadline(
          metadata.recordedAt,
          Number.isFinite(requestedDeadline) ? iso(requestedDeadline) : null,
          this.env,
        ),
      );
      const seq = this.nextQueueSeq();
      if (input.context.kind === 'member') {
        const confirmation = parseMemberGenerationConfirmation(input.confirmation);
        assertMemberGenerationVersions(
          confirmation,
          this.readMemberGenerationCapabilities(input.memberId).versions,
        );
        const request = { ...input.request, normalizationContext: input.context.member };
        if (typeof request.model !== 'string' || typeof request.prompt !== 'string') {
          fail('成员请求缺少规范输入');
        }
        assertMemberGenerationIntent(
          confirmation,
          generationRequestFingerprint(
            {
              ...request,
              model: request.model,
              prompt: request.prompt,
              operationParameters:
                request.operationParameters && typeof request.operationParameters === 'object'
                  ? request.operationParameters
                  : undefined,
            },
            { randomSeed: input.context.member.randomSeed, noCache: true },
          ),
        );
      }
      {
        const policy = this.policy();
        if (!policy.boundAccountId) {
          fail('共享账本尚未绑定账号', 503);
        }
        if (input.context.kind !== 'member') {
          this.reservationState(
            { memberId: input.memberId },
            policy.boundAccountId,
            metadata.recordedAt,
          );
        }
        this.refillForGeneration(metadata, policy.boundAccountId);
      }
      const result = this.inputs.persistAcceptedInput(metadata, {
        ...input,
        jobs: input.jobs.map((j, index) => ({
          ...j,
          status: index ? 'pending' : 'queued',
          queueSeq: index ? null : seq,
          queueExpiresAt: deadline,
          admissionEpoch: m.version,
        })),
      });
      if (input.context.kind !== 'member') {
        const policy = this.policy();
        if (quoteFairRequest(this.inputs.readJobInput(input.jobs[0].jobId)).resource === 'free') {
          assertFreeImageAdmission(this.freeUsage(input.memberId, policy, metadata.recordedAt));
        }
      }
      if (input.context.kind === 'member') {
        const policy = this.policy();
        if (!policy.boundAccountId) {
          fail('共享账本尚未绑定账号', 503);
        }
        const job = this.job(input.jobs[0].jobId);
        const { state } = this.reservationState(job, policy.boundAccountId, metadata.recordedAt);
        const plans = previewFairQuotaReservations(
          state,
          input.jobs.map((j) => this.inputs.readJobInput(j.jobId)),
          metadata.recordedAt,
        );
        const amounts = plans.map((plan) => generationChargeResources(plan));
        const total = addMemberGenerationResources(...amounts);
        const confirmation = parseMemberGenerationConfirmation(input.confirmation);
        const usage = this.freeUsage(job.memberId, policy, metadata.recordedAt);
        for (let index = 0; index < plans.length; index++) {
          assertMemberGenerationCost(confirmation, amounts[index], total);
          if (plans[index].resource === 'free') {
            assertFreeImageAdmission(usage);
            usage.reserved++;
          }
        }
      }
      return result;
    });
  }
  private refillForGeneration(metadata: Metadata, accountId: string): void {
    // 恢复仍由原额度命令拥有；与受理/派发共用最外层事务，失败连恢复事实一起回滚。
    this.quotas.refillQuota(
      {
        ...metadata,
        commandId: `generation-refill:${domainDigest([metadata.commandId, accountId])}`,
        fingerprint: domainDigest([metadata.fingerprint, accountId, metadata.recordedAt]),
        actorKind: 'system',
        actorId: null,
      },
      accountId,
    );
  }
  private freeUsage(memberId: string, policy: Policy, now: number): FreeImageUsage {
    const successes = this.sqlite
      .prepare<[string, number, number], { at: number; threshold: number | null }>(
        `SELECT free_succeeded_at AS at,free_warning_threshold AS threshold
      FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id WHERE r.member_id=? AND free_succeeded_at>? AND free_succeeded_at<=? ORDER BY free_succeeded_at,j.id`,
      )
      .all(memberId, now - 86400000, now);
    const reserved = this.sqlite
      .prepare<[string], { n: number }>(
        "SELECT COUNT(*) AS n FROM charges WHERE original_member_id=? AND status='reserved' AND billing_mode='free'",
      )
      .get(memberId)!.n;
    const needed =
      policy.freeEnabled && policy.hardLimit !== null
        ? Math.max(0, successes.length + reserved - policy.hardLimit + 1)
        : 0;
    const next = needed && needed <= successes.length ? successes[needed - 1].at + 86400000 : null;
    return {
      enabled: policy.freeEnabled === 1,
      successful: successes.length,
      reserved,
      warningThreshold: policy.warningThreshold,
      hardLimit: policy.hardLimit,
      warning:
        policy.freeEnabled === 1 &&
        policy.warningThreshold !== null &&
        successes.length >= policy.warningThreshold,
      warnedAt:
        policy.warningThreshold === null
          ? null
          : (successes.filter((s) => s.threshold === policy.warningThreshold).at(-1)?.at ?? null),
      nextAvailableAt: next || null,
      waitingForSettlement: needed > 0 && !next,
    };
  }
  private reservationState(job: Pick<Job, 'memberId'>, accountId: string, now: number) {
    const result = this.reservationFacts(job, accountId, now);
    if (!result.account.pst) {
      fail('上游私有凭据尚未恢复', 503);
    }
    return result;
  }
  // 回放只复核已保存的公开金融事实；在线caller仍经 reservationState 的私有门禁。
  private reservationFacts(job: Pick<Job, 'memberId'>, accountId: string, now: number) {
    const policy = this.policy();
    if (!policy.enabled || policy.boundAccountId !== accountId) {
      fail('共享账本只能使用当前绑定账号', 503);
    }
    if (!this.sqlite.prepare('SELECT 1 FROM group_members WHERE member_id=?').get(job.memberId)) {
      fail('该 Key 未加入共享账本', 403);
    }
    const account = this.sqlite
      .prepare<
        [string],
        {
          id: string;
          enabled: number;
          credentialEpoch: number;
          cooldownUntil: number | null;
          pst: string | null;
        }
      >(
        `SELECT a.id,a.enabled,a.credential_epoch AS credentialEpoch,a.cooldown_until AS cooldownUntil,c.pst
      FROM upstream_accounts a LEFT JOIN upstream_credentials c ON c.account_id=a.id WHERE a.id=?`,
      )
      .get(accountId);
    if (!account) {
      fail('共享账号不存在', 503);
    }
    const q = this.quotas.readAccountQuota(accountId);
    if (
      q.observedCredentialEpoch !== account.credentialEpoch ||
      !q.baselineEventId ||
      q.status !== 'active'
    ) {
      fail('账户额度基线不可用', 503);
    }
    const view = {
      id: accountId,
      enabled: account.enabled === 1,
      quotaTier: q.quotaTier,
      subscriptionActive: q.subscriptionActive === true,
      subscriptionExpiresAt: iso(q.subscriptionExpiresAt),
      quotaError: q.quotaErrorCode,
      quotaCheckedAt: iso(q.observedAt),
      v5UsagePercent: q.observedV5,
      v5UsageIsNegative: q.observedV5 !== null && parseAmount(q.observedV5) < 0n,
      quotaFixed: q.observedAnlasFixed,
      quotaPurchased: q.observedAnlasPurchased,
      v5UsageTimeUntilNextPercent: q.secondsPerPercent,
    };
    const baseline = this.sqlite
      .prepare<[string], { payload: string }>('SELECT payload FROM domain_events WHERE event_id=?')
      .get(q.baselineEventId);
    if (!baseline) {
      fail('账户观察基线不存在', 503);
    }
    const baselineAt = (JSON.parse(baseline.payload) as { observedAt?: number }).observedAt;
    const held = this.sqlite
      .prepare<
        [string],
        { v5: string; anlasFixed: string; anlasPurchased: string; sharedV5Reserved: string }
      >(
        `SELECT v5,anlas_fixed AS anlasFixed,anlas_purchased AS anlasPurchased,shared_v5_reserved AS sharedV5Reserved FROM charges WHERE original_account_id=? AND status='reserved'`,
      )
      .all(accountId);
    const availability = quotaAvailability(view, now, {
      at: iso(baselineAt ?? null),
      held: held.length > 0,
      lastSettlementAt: q.settlementCursorAt ?? 0,
    });
    if (availability !== 'active') {
      fail(`共享 Opus 额度暂不可用：${availability}`, 503);
    }
    const wallet = this.readWallet(accountId, job.memberId);
    if (!wallet) {
      fail('成员钱包尚未恢复', 503);
    }
    const reserved = zero();
    let shared = 0n;
    for (const charge of held) {
      for (const r of resources) {
        reserved[r] = formatAmount(parseAmount(reserved[r]) + parseAmount(charge[r]));
      }
      shared += parseAmount(charge.v5) + parseAmount(charge.sharedV5Reserved);
    }
    const total = this.sqlite
      .prepare<[string], { v5: string }>(
        `SELECT w.v5_available AS v5 FROM wallets w WHERE w.account_id=?`,
      )
      .all(accountId)
      .reduce((sum, w) => sum + parseAmount(w.v5), 0n);
    const state: FairQuotaReservationState = {
      balance: {
        v5Available: wallet.v5,
        anlasFixedAvailable: wallet.anlasFixed,
        anlasPurchasedAvailable: wallet.anlasPurchased,
      },
      autoAnlasFallback: policy.autoAnlasFallback === 1,
      snapshot: fairQuotaSnapshot(view),
      availableV5: formatAmount(total),
      reserved,
      reservedAnlas: formatAmount(
        parseAmount(reserved.anlasFixed) + parseAmount(reserved.anlasPurchased),
      ),
      sharedReservedV5: formatAmount(shared),
      pending: {
        v5: q.pendingV5Debit,
        anlasFixed: q.pendingFixedDebit,
        anlasPurchased: q.pendingPurchasedDebit,
      },
      pendingAnlas: formatAmount(
        parseAmount(q.pendingFixedDebit) + parseAmount(q.pendingPurchasedDebit),
      ),
      pendingFallback: {
        v5: q.pendingFallbackV5,
        anlasFixed: q.pendingFallbackFixed,
        anlasPurchased: q.pendingFallbackPurchased,
      },
      sharedV5Debt: q.sharedV5Debt,
    };
    return { policy, account, state };
  }
  private dispatchPlan(
    job: Job,
    accountId: string,
    metadata: Metadata,
    controls: Pick<GenerationDispatchOptions, 'dispatchEnabled' | 'physicalAccountLoad'>,
  ) {
    const now = metadata.recordedAt;
    const { policy, account } = this.reservationState(job, accountId, now);
    const running = this.sqlite
      .prepare<[string], { n: number }>(
        `SELECT COUNT(*) AS n FROM generation_attempts a JOIN generation_jobs j ON j.id=a.job_id WHERE a.account_id=? AND j.status='running'`,
      )
      .get(account.id)!.n;
    const physicalLoad = controls.physicalAccountLoad(account.id);
    if (!Number.isSafeInteger(physicalLoad) || physicalLoad < 0) {
      fail('物理执行槽状态无效', 503);
    }
    if (
      !controls.dispatchEnabled() ||
      Math.max(running, physicalLoad) >= sharedAccountConcurrency() ||
      sharedAccountReadyDelay(account, now) > 0
    ) {
      return null;
    }
    this.refillForGeneration(metadata, accountId);
    const { state } = this.reservationState(job, accountId, now);
    const request = this.inputs.readJobInput(job.id);
    const plan = planFairQuotaReservation(state, request, now);
    const charge = {
      ...plan,
      userId: job.memberId,
      accountId,
      configVersion: policy.configVersion,
      status: 'reserved',
      reservedAt: now,
    } as FairCharge;
    if (plan.resource === 'free') {
      assertFreeImageAdmission(this.freeUsage(job.memberId, policy, now));
    }
    if (this.inputs.readMemberContext(job.requestId, job.memberId).kind === 'member') {
      const confirmation = parseMemberGenerationConfirmation(
        job.confirmation ? JSON.parse(job.confirmation) : null,
      );
      assertMemberGenerationVersions(
        confirmation,
        memberGenerationCapabilities(
          this.policyView(policy),
          { id: job.memberId },
          { env: this.env },
        ).versions,
      );
      const prior = this.sqlite
        .prepare<[string, string], ResourceAmounts>(
          `SELECT c.v5,c.anlas_fixed AS anlasFixed,c.anlas_purchased AS anlasPurchased FROM charges c JOIN generation_jobs j ON j.id=c.job_id WHERE j.request_id=? AND j.id!=? AND c.status IN ('reserved','settled')`,
        )
        .all(job.requestId, job.id);
      const local = generationChargeResources(charge);
      assertMemberGenerationCost(
        confirmation,
        local,
        addMemberGenerationResources(local, ...prior),
      );
    }
    return { policy, account, charge };
  }
  dispatchGeneration(
    metadata: Metadata,
    input: { jobId: string; accountId: string; attemptId: string },
    controls: Pick<GenerationDispatchOptions, 'dispatchEnabled' | 'physicalAccountLoad'>,
  ) {
    return this.owned(() => {
      const job = this.job(input.jobId);
      this.authorize(metadata, job, true);
      const prior = readRelationalReceipt(this.sqlite, metadata);
      if (prior) {
        return { ...prior, dispatch: null };
      }
      if (job.status !== 'queued' || this.attempt(job.id) || this.readCharge(job.id)) {
        fail('任务已派发或已有收费凭据');
      }
      if (job.memberStatus !== 'active' || !job.authEpoch) {
        fail('成员授权已失效', 401);
      }
      assertMemberAcceptsJobs(
        { acceptingJobs: job.acceptingJobs === 1, jobAdmissionVersion: job.currentAdmissionEpoch },
        job.admissionEpoch,
      );
      if (
        (job.queueExpiresAt !== null && metadata.recordedAt >= job.queueExpiresAt) ||
        (job.deadlineAt !== null && metadata.recordedAt >= job.deadlineAt)
      ) {
        fail('任务已过期', 504);
      }
      const planned = this.dispatchPlan(job, input.accountId, metadata, controls);
      if (!planned) {
        return { dispatch: null, queued: true };
      }
      const { account, charge } = planned;
      const result = this.commit(
        metadata,
        [
          this.event(
            {
              eventType: 'QuotaReserved',
              payload: {
                jobId: job.id,
                memberId: job.memberId,
                accountId: account.id,
                resources: generationChargeResources(charge),
                billingMode: charge.billingMode || charge.resource,
                estimated: charge.estimated,
                v5Equivalent: charge.v5Equivalent || '0',
                sharedV5Reserved: charge.sharedV5Reserved || '0',
                configVersion: charge.configVersion,
              },
            },
            job,
            account.id,
            metadata.recordedAt,
          ),
          this.event(
            {
              eventType: 'DispatchAuthorized',
              payload: {
                attemptId: input.attemptId,
                jobId: job.id,
                accountId: account.id,
                credentialEpoch: account.credentialEpoch,
                candidateMode: 'alternatives',
                upstreamCandidates: generationUpstreamCandidates(charge),
              },
            },
            job,
            account.id,
            metadata.recordedAt,
          ),
        ],
        { jobId: job.id, attemptId: input.attemptId, accountId: account.id },
      );
      // 只有首次提交返回可发送的私有快照；重领回执不能再次连接上游。
      const dispatch = Object.freeze({
        jobId: job.id,
        attemptId: input.attemptId,
        accountId: account.id,
        credentialEpoch: account.credentialEpoch,
        pst: account.pst!,
      });
      this.dispatchCaptures.set(
        dispatch,
        result.events.find((e) => e.eventType === 'DispatchAuthorized')!.eventId,
      );
      return { ...result, dispatch };
    });
  }
  private currentResponse(
    job: Job,
    attempt: Attempt | null,
    evidence: RefundEvidence = {},
  ): Response {
    const raw = generationResponseEvidence(
      {
        id: job.id,
        status: job.status,
        upstreamStartedAt: iso(attempt?.upstreamStartedAt ?? null),
        upstreamHeadersAt: iso(attempt?.upstreamHeadersAt ?? null),
        upstreamCompletedAt: iso(attempt?.upstreamCompletedAt ?? null),
        upstreamHttpStatus: attempt?.upstreamHttpStatus ?? undefined,
      },
      { ...evidence, ...(attempt?.dispatchState === 'not_sent' ? { requestStarted: false } : {}) },
    );
    return {
      dispatchState:
        attempt?.dispatchState === 'request_started' ? 'request_started' : raw.dispatchState,
      upstreamStartedAt: raw.upstreamStartedAt ? Date.parse(raw.upstreamStartedAt) : null,
      upstreamHeadersAt: raw.upstreamHeadersAt ? Date.parse(raw.upstreamHeadersAt) : null,
      upstreamCompletedAt: raw.upstreamCompletedAt ? Date.parse(raw.upstreamCompletedAt) : null,
      upstreamHttpStatus: raw.upstreamHttpStatus,
    };
  }
  private mergeResponse(previous: Response, incoming: Response): Response {
    const next = { ...previous };
    for (const key of [
      'upstreamStartedAt',
      'upstreamHeadersAt',
      'upstreamCompletedAt',
      'upstreamHttpStatus',
    ] as const) {
      if (incoming[key] !== null) {
        if (next[key] !== null && next[key] !== incoming[key]) {
          fail('上游响应证据发生冲突');
        }
        next[key] = incoming[key];
      }
    }
    if (incoming.dispatchState !== 'unknown') {
      if (
        previous.dispatchState !== 'unknown' &&
        previous.dispatchState !== incoming.dispatchState
      ) {
        fail('上游发送证据发生冲突');
      }
      next.dispatchState = incoming.dispatchState;
    }
    if (next.upstreamStartedAt !== null) {
      if (next.dispatchState === 'not_sent') {
        fail('未发送证据与实际时间冲突');
      }
      next.dispatchState = 'request_started';
    }
    return next;
  }
  private assertPhysicalCompletion(
    attempt: Attempt,
    completion: Completion | GenerationPhysicalCompletion,
  ) {
    const proof = completion as Partial<GenerationPhysicalCompletion>;
    if (
      !proof ||
      proof.attemptId !== attempt.id ||
      !Number.isSafeInteger(proof.credentialEpoch) ||
      (proof.credentialEpoch ?? 0) <= 0 ||
      proof.credentialEpoch !== attempt.credentialEpoch ||
      !['ended', 'success', 'ordinary', 'capacity'].includes(proof.kind ?? '')
    ) {
      fail('物理结束证明不属于原尝试和凭据代次');
    }
  }
  private cooldown(
    job: Job,
    attempt: Attempt,
    now: number,
    completion: Completion,
  ): RelationalEventInput[] {
    const row = this.sqlite
      .prepare<
        [string],
        {
          epoch: number;
          cooldownUntil: number | null;
          cooldownReason: string | null;
          ordinaryStrikeCount: number;
          capacityStrikeCount: number;
          capacityLimitedUntil: number | null;
          capacityLastErrorAt: number | null;
        }
      >(
        `SELECT credential_epoch AS epoch,cooldown_until AS cooldownUntil,cooldown_reason AS cooldownReason,ordinary_strikes AS ordinaryStrikeCount,capacity_strikes AS capacityStrikeCount,capacity_limited_until AS capacityLimitedUntil,capacity_last_error_at AS capacityLastErrorAt FROM upstream_accounts WHERE id=?`,
      )
      .get(attempt.accountId);
    if (!row || row.epoch !== attempt.credentialEpoch) {
      return [];
    }
    const account = {
      ...row,
      cooldownReason: row.cooldownReason ?? undefined,
      capacityLastErrorAt: iso(row.capacityLastErrorAt),
    };
    const previous = this.sqlite
      .prepare<[string, string], { payload: string; at: number }>(
        `SELECT payload,occurred_at AS at FROM domain_events WHERE event_type='CooldownChanged'
      AND commit_seq IN (SELECT commit_seq FROM domain_events WHERE stream_id=?)
      AND json_extract(payload,'$.attemptId')=? ORDER BY commit_seq DESC,event_index DESC LIMIT 1`,
      )
      .get(`request:${job.requestId}`, attempt.id);
    const decision = previous
      ? (JSON.parse(previous.payload) as RelationalPayloads['CooldownChanged'])
      : null;
    if (
      !recordSharedCompletion(account, now, {
        ...completion,
        job: {
          id: job.id,
          generationAttempt: { id: attempt.id },
          sharedCooldown: decision?.completionKind
            ? {
                attemptId: attempt.id,
                kind: decision.completionKind,
                at: previous!.at,
                until: decision.cooldownUntil,
              }
            : null,
        },
      })
    ) {
      return [];
    }
    const timestamp = (v: number | string | null | undefined) => {
      if (typeof v === 'number') {
        return v;
      }
      if (v) {
        return Date.parse(v);
      }
      return null;
    };
    return [
      this.event(
        {
          eventType: 'CooldownChanged',
          payload: {
            accountId: attempt.accountId,
            attemptId: attempt.id,
            completionKind: completion.kind ?? 'ended',
            cooldownUntil: timestamp(account.cooldownUntil),
            cooldownReason: account.cooldownReason ?? null,
            ordinaryStrikes: account.ordinaryStrikeCount,
            capacityStrikes: account.capacityStrikeCount,
            capacityLimitedUntil: timestamp(account.capacityLimitedUntil),
            capacityLastErrorAt: timestamp(account.capacityLastErrorAt),
          },
        },
        job,
        attempt.accountId,
        now,
      ),
    ];
  }
  recordGenerationResponse(
    metadata: Metadata,
    jobId: string,
    response: Response,
    completion?: Completion | GenerationPhysicalCompletion,
  ) {
    return this.owned(() => {
      const job = this.job(jobId);
      this.authorize(metadata, job, true);
      const prior = readRelationalReceipt(this.sqlite, metadata);
      if (prior) {
        return prior;
      }
      const attempt = this.attempt(jobId);
      if (!attempt) {
        fail('任务没有派发尝试');
      }
      const merged = this.mergeResponse(this.currentResponse(job, attempt), response);
      if (
        completion !== undefined &&
        (merged.upstreamCompletedAt === null ||
          'attemptId' in completion ||
          'credentialEpoch' in completion)
      ) {
        this.assertPhysicalCompletion(attempt, completion);
      }
      return this.commit(
        metadata,
        [
          this.event(
            {
              eventType: 'ResponseEvidenceRecorded',
              payload: { attemptId: attempt.id, response: merged },
            },
            job,
            attempt.accountId,
            metadata.recordedAt,
          ),
          // 显式 completion 来自已结束的物理 promise；不把断流结束冒充完整响应时间。
          ...((attempt.upstreamCompletedAt === null && merged.upstreamCompletedAt !== null) ||
          (completion !== undefined && merged.upstreamCompletedAt === null)
            ? this.cooldown(job, attempt, metadata.recordedAt, completion ?? {})
            : []),
        ],
        { jobId, attemptId: attempt.id },
      );
    });
  }
  readPendingGenerationResultIds(afterId = '', limit = 100): string[] {
    if (typeof afterId !== 'string' || !Number.isInteger(limit) || limit < 1 || limit > 100) {
      throw new TypeError('Invalid pending result page.');
    }
    return this.sqlite
      .prepare<[string, number], { id: string }>(
        `SELECT a.job_id AS id FROM generation_attempts a INDEXED BY idx_attempts_prepared
        JOIN generation_jobs j ON j.id=a.job_id
        WHERE a.prepared_artifact_id IS NOT NULL AND j.artifact_id IS NULL AND a.job_id>?
        ORDER BY a.job_id LIMIT ?`,
      )
      .all(afterId, limit)
      .map((row) => row.id);
  }
  readGenerationResultPreparation(jobId: string) {
    const job = this.job(jobId),
      attempt = this.attempt(jobId);
    if (!attempt || job.artifactId) {
      fail('任务没有待恢复的准备结果');
    }
    const sources = this.sqlite
      .prepare<[string, string], { eventId: string; commitSeq: string }>(
        `SELECT event_id AS eventId,CAST(commit_seq AS TEXT) AS commitSeq FROM domain_events
        WHERE stream_id=? AND event_type='ArtifactPrepared' AND json_extract(payload,'$.attemptId')=?`,
      )
      .all(`request:${job.requestId}`, attempt.id);
    if (sources.length !== 1) {
      fail('准备结果缺少唯一的原事实');
    }
    const event = readRelationalCommit(this.sqlite, sources[0].commitSeq).events.find(
      (e) => e.eventId === sources[0].eventId,
    );
    if (
      event?.eventType !== 'ArtifactPrepared' ||
      event.accountId !== attempt.accountId ||
      event.memberId !== job.memberId
    ) {
      fail('准备结果事实归属不符');
    }
    const descriptor = {
      imageId: event.payload.artifactId,
      file: event.payload.file,
      operation: job.operation,
      mimeType: event.payload.mimeType,
      byteLength: event.payload.byteLength,
      sha256: event.payload.sha256,
    };
    assertStoredResultDescriptor(descriptor);
    if (canonicalJson(descriptor) !== canonicalJson(this.prepared(job, attempt))) {
      fail('准备结果与原事实不一致');
    }
    // 冷读复用输入 owner 的原请求/归属/摘要核验，不把当前关系行当作原授权。
    this.inputs.readJobInput(jobId);
    return { attemptId: attempt.id, event, descriptor };
  }
  private prepared(job: Job, attempt: Attempt): Publication {
    const value = {
      imageId: attempt.preparedArtifactId,
      file: attempt.preparedFile,
      operation: job.operation,
      mimeType: attempt.preparedMimeType,
      byteLength: attempt.preparedByteLength,
      sha256: attempt.preparedSha256,
    };
    assertStoredResultDescriptor(value);
    return value;
  }
  prepareGenerationResult(
    metadata: Metadata,
    jobId: string,
    descriptor: Publication,
    response: Response,
  ) {
    return this.owned(() => {
      this.authorizeResult(metadata);
      const job = this.job(jobId);
      const prior = readRelationalReceipt(this.sqlite, metadata);
      if (prior) {
        return prior;
      }
      const attempt = this.attempt(jobId),
        charge = this.readCharge(jobId);
      if (
        !attempt ||
        !charge ||
        job.retiredAt !== null ||
        !['running', 'failed'].includes(job.status) ||
        charge.status === 'settled'
      ) {
        fail('任务不能准备新结果');
      }
      assertStoredResultDescriptor(descriptor);
      if (descriptor.operation !== job.operation || attempt.preparedArtifactId) {
        fail('结果操作不符或已有准备凭据');
      }
      const merged = this.mergeResponse(this.currentResponse(job, attempt), response);
      if (merged.dispatchState === 'not_sent') {
        fail('未发送的尝试不能准备上游结果');
      }
      return this.commit(
        metadata,
        [
          this.event(
            {
              eventType: 'ArtifactPrepared',
              payload: {
                artifactId: descriptor.imageId,
                attemptId: attempt.id,
                file: descriptor.file,
                mimeType: descriptor.mimeType,
                byteLength: descriptor.byteLength,
                sha256: descriptor.sha256,
                response: merged,
              },
            },
            job,
            attempt.accountId,
            metadata.recordedAt,
          ),
        ],
        { jobId, artifactId: descriptor.imageId },
      );
    });
  }
  private waiting(job: Job) {
    return this.sqlite
      .prepare<[string, string], { id: string }>(
        "SELECT id FROM generation_jobs WHERE request_id=? AND id!=? AND status IN ('queued','pending') ORDER BY sample_index",
      )
      .all(job.requestId, job.id)
      .map((r) => r.id);
  }
  private cancellation(
    job: Job,
    ids: string[],
    reason: string,
    status: number,
    now: number,
  ): RelationalEventInput[] {
    return ids.length
      ? [
          this.event(
            {
              eventType: 'WaitingJobsCancelled',
              payload: {
                jobIds: ids,
                reason,
                errorStatus: status,
                cancellationReason: 'batch_stopped',
              },
            },
            job,
            null,
            now,
          ),
        ]
      : [];
  }
  completeGeneration(metadata: Metadata, jobId: string, publication: Publication) {
    return this.owned(() => {
      this.authorizeResult(metadata);
      const job = this.job(jobId);
      const prior = readRelationalReceipt(this.sqlite, metadata);
      if (prior) {
        return prior;
      }
      const attempt = this.attempt(jobId),
        charge = this.readCharge(jobId);
      if (
        !attempt ||
        !charge ||
        job.retiredAt !== null ||
        charge.attemptId !== attempt.id ||
        !['running', 'failed'].includes(job.status) ||
        job.artifactId ||
        charge.status === 'settled'
      ) {
        fail('任务或收费已经终结');
      }
      assertStoredResultDescriptor(publication);
      if (canonicalJson(publication) !== canonicalJson(this.prepared(job, attempt))) {
        fail('发布结果与准备凭据不符');
      }
      const response = this.currentResponse(job, attempt);
      const local = this.replicas.planLocalArtifactPublication(publication, metadata.recordedAt);
      const events: RelationalEventInput[] = [
        ...(local.target ? [this.event(local.target, null, null, metadata.recordedAt)] : []),
        this.event(
          {
            eventType: 'ArtifactPublished',
            payload: {
              artifactId: publication.imageId,
              memberId: job.memberId,
              inputId: job.inputId,
              originalAttemptId: attempt.id,
              operation: job.operation,
              mimeType: publication.mimeType,
              byteLength: publication.byteLength,
              sha256: publication.sha256,
            },
          },
          job,
          attempt.accountId,
          metadata.recordedAt,
        ),
        this.event(local.replica, job, null, metadata.recordedAt),
      ];
      if (job.status === 'running' && charge.status === 'reserved') {
        events.push(
          this.event(
            {
              eventType: 'GenerationSettled',
              payload: { jobId, reservationEventId: charge.reservationEventId },
            },
            job,
            attempt.accountId,
            metadata.recordedAt,
          ),
          this.event(
            {
              eventType: 'JobCompleted',
              payload: { jobId, artifactId: publication.imageId, response },
            },
            job,
            attempt.accountId,
            metadata.recordedAt,
          ),
        );
        const children = this.sqlite
          .prepare<
            [string],
            {
              id: string;
              requestId: string;
              sampleIndex: number;
              status: string;
              queueExpiresAt: number | null;
              deadlineAt: number | null;
            }
          >(
            `SELECT id,request_id AS requestId,sample_index AS sampleIndex,status,queue_expires_at AS queueExpiresAt,deadline_at AS deadlineAt FROM generation_jobs WHERE request_id=?`,
          )
          .all(job.requestId)
          .map((j) => (j.id === jobId ? { ...j, status: 'done' } : j));
        const next = nextPendingBatchJob(children, { requestId: job.requestId, status: 'done' });
        if (next) {
          let stopped = job.memberStatus !== 'active' || !job.authEpoch;
          try {
            assertMemberAcceptsJobs(
              {
                acceptingJobs: job.acceptingJobs === 1,
                jobAdmissionVersion: job.currentAdmissionEpoch,
              },
              job.admissionEpoch,
            );
          } catch {
            stopped = true;
          }
          if (
            stopped ||
            (next.queueExpiresAt !== null && metadata.recordedAt >= next.queueExpiresAt) ||
            (next.deadlineAt !== null && metadata.recordedAt >= next.deadlineAt)
          ) {
            events.push(
              ...this.cancellation(
                job,
                this.waiting(job),
                stopped ? 'member_stopped' : 'queue_expired',
                stopped ? 409 : 504,
                metadata.recordedAt,
              ),
            );
          } else {
            events.push(
              this.event(
                {
                  eventType: 'JobQueued',
                  payload: {
                    jobId: next.id,
                    queueSeq: this.nextQueueSeq(),
                    queueExpiresAt: next.queueExpiresAt,
                  },
                },
                job,
                null,
                metadata.recordedAt,
              ),
            );
          }
        }
      } else if (job.status === 'failed' && charge.status === 'refunded') {
        events.push(
          this.event(
            {
              eventType: 'LateResultRecovered',
              payload: { jobId, artifactId: publication.imageId, refundRetained: true, response },
            },
            job,
            attempt.accountId,
            metadata.recordedAt,
          ),
        );
        if (attempt.lossEventId) {
          events.push(
            this.event(
              {
                eventType: 'ReviewClosed',
                payload: {
                  attemptId: attempt.id,
                  status: 'result_recovered',
                  lossEventId: attempt.lossEventId,
                  reason: 'result_recovered',
                  actualConsumption: null,
                },
              },
              job,
              attempt.accountId,
              metadata.recordedAt,
            ),
          );
        }
      } else {
        fail('任务和收费终态不一致');
      }
      if (charge.billingMode === 'free' && job.freeSucceededAt === null) {
        const warningThreshold = freeImageSuccessWarningThreshold(
          this.freeUsage(job.memberId, this.policy(), metadata.recordedAt),
        );
        events.push(
          this.event(
            {
              eventType: 'FreeImageSucceeded',
              payload: { jobId, memberId: job.memberId, warningThreshold },
            },
            job,
            attempt.accountId,
            metadata.recordedAt,
          ),
        );
      }
      if (response.upstreamCompletedAt !== null) {
        events.push(...this.cooldown(job, attempt, metadata.recordedAt, { kind: 'success' }));
      }
      return this.commit(metadata, events, {
        jobId,
        artifactId: publication.imageId,
        refundRetained: charge.status === 'refunded',
      });
    });
  }
  failGeneration(
    metadata: Metadata,
    jobId: string,
    failure: {
      errorCode: string;
      errorStatus: number;
      aborted: boolean;
      cancellationReason?: string;
      evidence?: RefundEvidence;
      completion?: Completion | GenerationPhysicalCompletion;
      diagnostic?: { dispatch: DispatchCapture; error: unknown };
    },
  ) {
    return this.owned(() => {
      const job = this.job(jobId);
      this.authorize(metadata, job);
      if (typeof failure.aborted !== 'boolean') {
        fail('生成失败必须明确是否取消');
      }
      const capture = failure.diagnostic?.dispatch;
      if (failure.diagnostic !== undefined) {
        this.assertDiagnosticCapture(metadata, job, capture!);
      }
      const prior = readRelationalReceipt(this.sqlite, metadata);
      if (prior) {
        return prior;
      }
      if (
        !['queued', 'pending', 'running'].includes(job.status) ||
        (metadata.actorKind === 'member' && job.status === 'running')
      ) {
        fail('任务已经派发或终结');
      }
      const attempt = this.attempt(jobId),
        charge = this.readCharge(jobId),
        events: RelationalEventInput[] = [];
      if (failure.completion !== undefined) {
        if (!attempt) {
          fail('物理结束缺少原尝试');
        }
        if (
          attempt.upstreamCompletedAt === null ||
          'attemptId' in failure.completion ||
          'credentialEpoch' in failure.completion
        ) {
          this.assertPhysicalCompletion(attempt, failure.completion);
        }
      }
      const response = this.currentResponse(job, attempt, failure.evidence),
        responseEventId = randomUUID(),
        failureEventId = randomUUID();
      if (charge) {
        if (charge.status !== 'reserved' || !attempt || charge.attemptId !== attempt.id) {
          fail('原预扣与尝试不一致');
        }
        const risk =
          response.dispatchState !== 'not_sent' && JSON.parse(attempt.upstreamCandidates).length > 0
            ? randomUUID()
            : null;
        events.push(
          this.event(
            { eventType: 'ResponseEvidenceRecorded', payload: { attemptId: attempt.id, response } },
            job,
            attempt.accountId,
            metadata.recordedAt,
            responseEventId,
          ),
        );
        events.push(
          ...this.quotas.planGenerationCredentialFailure(metadata, {
            attemptId: attempt.id,
            responseEventId,
            failureEventId,
            aborted: failure.aborted,
            response,
          }),
          this.event(
            {
              eventType: 'GenerationRefunded',
              payload: { jobId, reservationEventId: charge.reservationEventId, lossEventId: risk },
            },
            job,
            charge.originalAccountId,
            metadata.recordedAt,
          ),
        );
        if (risk) {
          events.push(
            this.event(
              {
                eventType: 'ReviewOpened',
                payload: {
                  attemptId: attempt.id,
                  firstUnknownAt: metadata.recordedAt,
                  deadlineAt: metadata.recordedAt + GENERATION_REVIEW_MS,
                  lossEventId: risk,
                  response,
                },
              },
              job,
              attempt.accountId,
              metadata.recordedAt,
              risk,
            ),
          );
        }
      } else if (attempt || job.status === 'running') {
        fail('运行任务缺少原预扣');
      }
      const diagnostic = capture
        ? this.inputs.stageGenerationDiagnostic(
            jobId,
            errorDetailMessage(failure.diagnostic!.error, [capture.pst]),
            metadata.recordedAt,
          )
        : null;
      if (diagnostic) {
        events.push(
          this.event(
            { eventType: 'DiagnosticRecorded', payload: diagnostic },
            job,
            attempt!.accountId,
            metadata.recordedAt,
          ),
        );
      }
      events.push(
        this.event(
          {
            eventType: 'JobFailed',
            payload: {
              jobId,
              errorCode: failure.errorCode,
              errorStatus: failure.errorStatus,
              cancellationReason: failure.cancellationReason ?? null,
              diagnosticId: diagnostic?.diagnosticId ?? null,
              aborted: failure.aborted,
              response,
            },
          },
          job,
          attempt?.accountId ?? null,
          metadata.recordedAt,
          failureEventId,
        ),
        ...this.cancellation(
          job,
          this.waiting(job),
          failure.errorCode,
          failure.errorStatus,
          metadata.recordedAt,
        ),
      );
      if (attempt && (response.upstreamCompletedAt !== null || failure.completion !== undefined)) {
        events.push(...this.cooldown(job, attempt, metadata.recordedAt, failure.completion ?? {}));
      }
      return this.commit(metadata, events, { jobId, status: 'failed', refunded: charge !== null });
    });
  }
  private assertDiagnosticCapture(metadata: Metadata, job: Job, capture: DispatchCapture) {
    this.authorizeResult(metadata);
    const eventId = this.dispatchCaptures.get(capture),
      source =
        eventId &&
        this.sqlite
          .prepare<[string], { seq: string }>(
            'SELECT CAST(commit_seq AS TEXT) AS seq FROM domain_events WHERE event_id=?',
          )
          .get(eventId),
      event =
        source &&
        readRelationalCommit(this.sqlite, source.seq).events.find((e) => e.eventId === eventId),
      attempt = this.attempt(job.id);
    if (
      !event ||
      event.eventType !== 'DispatchAuthorized' ||
      !attempt ||
      capture.jobId !== job.id ||
      capture.attemptId !== attempt.id ||
      capture.accountId !== attempt.accountId ||
      capture.credentialEpoch !== attempt.credentialEpoch ||
      event.streamId !== `request:${job.requestId}` ||
      event.memberId !== job.memberId ||
      event.accountId !== attempt.accountId ||
      event.payload.jobId !== job.id ||
      event.payload.attemptId !== attempt.id ||
      event.payload.accountId !== attempt.accountId ||
      event.payload.credentialEpoch !== attempt.credentialEpoch
    ) {
      fail('诊断捕获缺少真实原派发票据', 403);
    }
  }
  cancelWaitingGeneration(metadata: Metadata, requestId: string) {
    return this.owned(() => {
      const row = this.sqlite
        .prepare<[string], { id: string }>(
          'SELECT id FROM generation_jobs WHERE request_id=? ORDER BY sample_index LIMIT 1',
        )
        .get(requestId);
      if (!row) {
        fail('请求不存在', 404);
      }
      const job = this.job(row.id);
      this.authorize(metadata, job);
      const prior = readRelationalReceipt(this.sqlite, metadata);
      if (prior) {
        return prior;
      }
      const ids = this.sqlite
        .prepare<[string], { id: string }>(
          "SELECT id FROM generation_jobs WHERE request_id=? AND status IN ('queued','pending') ORDER BY sample_index",
        )
        .all(requestId)
        .map((r) => r.id);
      if (!ids.length) {
        return null;
      }
      return this.commit(
        metadata,
        this.cancellation(job, ids, 'request_cancelled', 409, metadata.recordedAt),
        { requestId, cancelledJobIds: ids },
      );
    });
  }
  retireJobHistory(
    metadata: Omit<Metadata, 'fingerprint'>,
    jobIds: string[],
    actor: MemberUser | null,
    controls: HistoryRetirementControls,
  ) {
    return this.owned(() => {
      const current =
        actor &&
        this.sqlite
          .prepare<[string], { status: string; authEpoch: string | null }>(
            'SELECT status,auth_epoch AS authEpoch FROM members WHERE id=?',
          )
          .get(actor.id);
      assertCurrentOwnerCommand(metadata, actor, current, this.env);
      const ids = historyRetirementIds(jobIds),
        command = { ...metadata, fingerprint: historyRetirementFingerprint('jobs', ids) },
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return prior;
      }
      assertHistoryRetirementControls(controls);
      const groups = new Map<string, string[]>();
      for (const id of ids) {
        const job = readHistoryJob(this.sqlite, id);
        if (job.retiredAt !== null) {
          fail('任务历史已经退休');
        }
        const group = groups.get(job.requestId) ?? [];
        group.push(id);
        groups.set(job.requestId, group);
      }
      const events: RelationalEventInput[] = [];
      for (const requestId of [...groups.keys()].sort()) {
        assertRetirableRequest(this.sqlite, requestId, controls);
        const selected = groups.get(requestId)!;
        events.push(
          this.event(
            {
              eventType: 'JobHistoryRetired',
              payload: { jobIds: selected, reason: 'owner_cleared_history' },
            },
            this.job(selected[0]),
            null,
            metadata.recordedAt,
          ),
        );
      }
      return this.commit(command, events, { retiredJobIds: ids });
    });
  }
  closeGenerationReview(metadata: Metadata, jobId: string) {
    return this.owned(() => {
      const job = this.job(jobId);
      this.authorize(metadata, job, true);
      const prior = readRelationalReceipt(this.sqlite, metadata);
      if (prior) {
        return prior;
      }
      const a = this.attempt(jobId);
      if (
        !a ||
        a.reviewStatus !== 'pending' ||
        a.reviewDeadlineAt === null ||
        metadata.recordedAt < a.reviewDeadlineAt ||
        !a.lossEventId
      ) {
        fail('核对窗口尚未结束或已经结案');
      }
      return this.commit(
        metadata,
        [
          this.event(
            {
              eventType: 'ReviewClosed',
              payload: {
                attemptId: a.id,
                status: 'estimated_closed',
                lossEventId: a.lossEventId,
                reason: 'review_deadline_elapsed',
                actualConsumption: null,
              },
            },
            job,
            a.accountId,
            metadata.recordedAt,
          ),
        ],
        { jobId, status: 'estimated_closed' },
      );
    });
  }
  private eventJob(event: RelationalEvent, jobId: string, accountId?: string): Job {
    const job = this.job(jobId);
    if (
      event.memberId !== job.memberId ||
      (event.streamKind === 'request' && event.streamId !== `request:${job.requestId}`) ||
      (accountId !== undefined && event.accountId !== accountId)
    ) {
      fail('事实与实际任务/账户归属不符');
    }
    return job;
  }
  private reservation(event: RelationalEvent, jobId: string, reservationEventId: string): Charge {
    const c = this.readCharge(jobId);
    if (
      !c ||
      c.resolution !== 'live' ||
      c.jobId !== jobId ||
      c.status !== 'reserved' ||
      c.reservationEventId !== reservationEventId
    ) {
      fail('原预扣不存在、身份不符或已经终结');
    }
    const job = this.eventJob(event, jobId, c.originalAccountId),
      a = this.attempt(jobId);
    if (
      job.memberId !== c.originalMemberId ||
      !a ||
      a.id !== c.attemptId ||
      a.accountId !== c.originalAccountId ||
      job.status !== 'running'
    ) {
      fail('实际任务、尝试与原预扣不一致');
    }
    const anchor = this.sqlite
      .prepare<
        [string],
        { event_type: string; account_id: string | null; member_id: string | null }
      >('SELECT event_type,account_id,member_id FROM domain_events WHERE event_id=?')
      .get(reservationEventId);
    if (
      !anchor ||
      !['QuotaReserved', 'OpeningChargeRecorded'].includes(anchor.event_type) ||
      anchor.account_id !== c.originalAccountId ||
      anchor.member_id !== c.originalMemberId
    ) {
      fail('原预扣事实归属不符');
    }
    return c;
  }
  private adjustWallet(
    accountId: string,
    memberId: string,
    delta: ResourceAmounts,
    direction: 1 | -1,
    eventId: string,
  ) {
    const wallet = this.readWallet(accountId, memberId);
    if (!wallet) {
      fail('原钱包不存在');
    }
    const next = zero();
    for (const r of resources) {
      next[r] = formatAmount(parseAmount(wallet[r]) + BigInt(direction) * parseAmount(delta[r]));
      if (direction === -1 && parseAmount(delta[r]) > 0n && parseAmount(next[r]) < 0n) {
        fail('原钱包可用额不足', 402);
      }
    }
    this.sqlite
      .prepare(
        'UPDATE wallets SET v5_available=?,anlas_fixed_available=?,anlas_purchased_available=?,source_event_id=? WHERE account_id=? AND member_id=?',
      )
      .run(next.v5, next.anlasFixed, next.anlasPurchased, eventId, accountId, memberId);
  }
  private writeResponse(event: RelationalEvent, attemptId: string, incoming: Response) {
    const row = this.sqlite
      .prepare<[string], { job_id: string }>('SELECT job_id FROM generation_attempts WHERE id=?')
      .get(attemptId);
    if (!row) {
      fail('响应事实缺少真实尝试');
    }
    const a = this.attempt(row.job_id)!;
    const job = this.eventJob(event, a.jobId, a.accountId);
    const response = this.mergeResponse(this.currentResponse(job, a), incoming);
    this.sqlite
      .prepare(
        `UPDATE generation_attempts SET dispatch_state=?,upstream_started_at=?,upstream_headers_at=?,upstream_completed_at=?,upstream_http_status=?,source_event_id=? WHERE id=?`,
      )
      .run(
        response.dispatchState,
        response.upstreamStartedAt,
        response.upstreamHeadersAt,
        response.upstreamCompletedAt,
        response.upstreamHttpStatus,
        event.eventId,
        attemptId,
      );
  }
  private artifact(job: Job, artifactId: string, attemptId: string) {
    const row = this.sqlite
      .prepare<
        [string],
        {
          member_id: string;
          input_id: string;
          original_attempt_id: string | null;
          retired_at: number | null;
        }
      >('SELECT member_id,input_id,original_attempt_id,retired_at FROM artifacts WHERE id=?')
      .get(artifactId);
    if (
      !row ||
      job.retiredAt !== null ||
      row.retired_at !== null ||
      row.member_id !== job.memberId ||
      row.input_id !== job.inputId ||
      row.original_attempt_id !== attemptId
    ) {
      fail('产物与原任务/输入/尝试归属不符');
    }
  }
  createGenerationProjector(events: RelationalEvent[]) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Generation projection requires the owning transaction.');
    }
    if (!events.some((event) => event.eventType === 'RequestAccepted')) {
      for (const alias of events.filter((event) => event.eventType === 'RequestAliasRegistered')) {
        const job = this.eventJob(alias, alias.payload.jobId);
        this.assertReuseReadiness(job, alias.occurredAt, job.admissionEpoch);
      }
    }
    const diagnostics = events.filter((e) => e.eventType === 'DiagnosticRecorded'),
      failures = events.filter((e) => e.eventType === 'JobFailed');
    const openingArtifacts = new Map<
      string,
      Extract<RelationalEvent, { eventType: 'OpeningArtifactRecorded' }>
    >();
    for (const event of events) {
      if (event.eventType === 'OpeningArtifactRecorded') {
        if (openingArtifacts.has(event.payload.artifactId)) {
          fail('同笔开账产物重复登记');
        }
        openingArtifacts.set(event.payload.artifactId, event);
      }
    }
    for (const diagnostic of diagnostics) {
      const matched = failures.filter(
        (e) => e.payload.diagnosticId === diagnostic.payload.diagnosticId,
      );
      if (
        matched.length !== 1 ||
        matched[0].payload.jobId !== diagnostic.payload.jobId ||
        diagnostic.eventIndex >= matched[0].eventIndex
      ) {
        fail('诊断必须属于同笔原失败事实');
      }
    }
    for (const failure of failures) {
      if (
        failure.payload.diagnosticId !== null &&
        diagnostics.filter((e) => e.payload.diagnosticId === failure.payload.diagnosticId)
          .length !== 1
      ) {
        fail('失败事实缺少同笔原诊断');
      }
    }
    return (e: RelationalEvent): void => {
      if (!this.sqlite.inTransaction) {
        throw new Error('Generation projection requires the owning transaction.');
      }
      if (e.eventType.startsWith('Opening')) {
        assertRelationalOpeningSource(this.sqlite, e);
      }
      switch (e.eventType) {
        case 'OpeningJobRecorded': {
          const p = e.payload,
            request = this.sqlite
              .prepare<[string], { memberId: string; operation: string }>(
                'SELECT member_id AS memberId,operation FROM generation_requests WHERE id=?',
              )
              .get(p.requestId);
          if (
            !request ||
            e.memberId !== request.memberId ||
            e.accountId !== null ||
            (request.operation !== 'generate' && p.actualSeed !== null) ||
            (['pending', 'queued', 'running'].includes(p.status) &&
              (p.retiredAt !== null || p.artifactId !== null))
          ) {
            fail('开账任务与原请求/生命周期角色不符');
          }
          if (p.status === 'running') {
            fail('运行态开账任务需要原尝试和预扣责任的完整 owner');
          }
          if (p.artifactId !== null) {
            const existing = this.sqlite
                .prepare<[string], { memberId: string; operation: string }>(
                  'SELECT member_id AS memberId,operation FROM artifacts WHERE id=?',
                )
                .get(p.artifactId),
              planned = openingArtifacts.get(p.artifactId),
              artifact = existing ?? planned?.payload;
            if (
              !artifact ||
              artifact.memberId !== request.memberId ||
              artifact.operation !== request.operation
            ) {
              fail('开账任务缺少同成员/操作的唯一原产物');
            }
          }
          this.sqlite
            .prepare(
              `INSERT INTO generation_jobs(id,request_id,sample_index,actual_seed,status,queue_seq,queue_expires_at,deadline_at,admission_epoch,artifact_id,cache_key,completed_at,error_code,error_status,cancellation_reason,late_result_at,free_succeeded_at,free_warning_threshold,retired_at,source_event_id)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            )
            .run(
              p.jobId,
              p.requestId,
              p.sampleIndex,
              p.actualSeed,
              p.status,
              p.queueSeq === null ? null : BigInt(p.queueSeq),
              p.queueExpiresAt,
              p.deadlineAt,
              p.admissionEpoch,
              p.artifactId,
              p.cacheKey,
              p.completedAt,
              p.errorCode,
              p.errorStatus,
              p.cancellationReason,
              p.lateResultAt,
              p.freeSucceededAt,
              p.freeWarningThreshold,
              p.retiredAt,
              e.eventId,
            );
          this.inputs.readJobSeed(p.jobId);
          if (['pending', 'queued', 'running'].includes(p.status)) {
            this.inputs.readJobInput(p.jobId);
          }
          break;
        }
        case 'OpeningArtifactRecorded': {
          const p = e.payload,
            input = this.sqlite
              .prepare<[string, string], { operation: unknown }>(
                "SELECT json_extract(body,'$.operation') AS operation FROM generation_inputs WHERE id=? AND member_id=?",
              )
              .get(p.inputId, p.memberId);
          if (!input || (input.operation !== null && input.operation !== p.operation)) {
            fail('开账产物与原输入/操作归属不符');
          }
          if (p.originalAttemptId === null) {
            if (e.accountId !== null) {
              fail('未知原尝试的开账产物不能声明账户角色');
            }
          } else {
            const original = this.sqlite
              .prepare<
                [string],
                {
                  accountId: string;
                  memberId: string;
                  inputId: string;
                  operation: string;
                  artifactId: string | null;
                }
              >(
                `SELECT a.account_id AS accountId,r.member_id AS memberId,r.input_id AS inputId,r.operation,j.artifact_id AS artifactId
              FROM generation_attempts a JOIN generation_jobs j ON j.id=a.job_id
              JOIN generation_requests r ON r.id=j.request_id WHERE a.id=?`,
              )
              .get(p.originalAttemptId);
            if (
              !original ||
              original.accountId !== e.accountId ||
              original.memberId !== p.memberId ||
              original.inputId !== p.inputId ||
              original.operation !== p.operation ||
              original.artifactId !== p.artifactId
            ) {
              fail('开账产物与已知原尝试/任务/输入角色不符');
            }
          }
          this.sqlite
            .prepare(
              `INSERT INTO artifacts(id,member_id,input_id,original_attempt_id,operation,mime_type,byte_length,sha256,published_at,retired_at,source_event_id)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
            )
            .run(
              p.artifactId,
              p.memberId,
              p.inputId,
              p.originalAttemptId,
              p.operation,
              p.mimeType,
              p.byteLength,
              p.sha256,
              p.publishedAt,
              p.retiredAt,
              e.eventId,
            );
          break;
        }
        case 'JobHistoryRetired': {
          assertHistoryRetirementCommit(this.sqlite, e, 'jobs');
          const jobs = e.payload.jobIds.map((id) => readHistoryJob(this.sqlite, id));
          for (const job of jobs) {
            if (job.retiredAt !== null) {
              fail('任务历史已经退休');
            }
          }
          assertRetirableRequest(this.sqlite, jobs[0].requestId);
          for (const job of jobs) {
            this.sqlite
              .prepare('UPDATE generation_jobs SET retired_at=?,source_event_id=? WHERE id=?')
              .run(e.occurredAt, e.eventId, job.id);
          }
          this.sqlite
            .prepare(
              `UPDATE generation_requests SET retired_at=?,source_event_id=? WHERE id=?
            AND NOT EXISTS(SELECT 1 FROM generation_jobs WHERE request_id=? AND retired_at IS NULL)`,
            )
            .run(e.occurredAt, e.eventId, jobs[0].requestId, jobs[0].requestId);
          break;
        }
        case 'ImportSourceRegistered':
          assertRelationalOpeningSource(this.sqlite, e);
          break;
        case 'CooldownChanged': {
          const p = e.payload;
          if ((p.attemptId === null) !== (p.completionKind === null)) {
            fail('完成冷却缺少原尝试或结果种类');
          }
          if (p.attemptId) {
            const a = this.sqlite
              .prepare<[string], { jobId: string; accountId: string; epoch: number | null }>(
                'SELECT job_id AS jobId,account_id AS accountId,credential_epoch AS epoch FROM generation_attempts WHERE id=?',
              )
              .get(p.attemptId);
            if (!a || a.accountId !== p.accountId) {
              fail('冷却事实与原尝试不符');
            }
            this.eventJob(e, a.jobId, a.accountId);
            const current = this.sqlite
              .prepare<[string], { epoch: number }>(
                'SELECT credential_epoch AS epoch FROM upstream_accounts WHERE id=?',
              )
              .get(a.accountId);
            if (a.epoch !== current?.epoch) {
              fail('旧凭据尝试不能修改新凭据冷却');
            }
          }
          if (
            this.sqlite
              .prepare(
                `UPDATE upstream_accounts SET cooldown_until=?,cooldown_reason=?,ordinary_strikes=?,capacity_strikes=?,capacity_limited_until=?,capacity_last_error_at=?,source_event_id=? WHERE id=?`,
              )
              .run(
                p.cooldownUntil,
                p.cooldownReason,
                p.ordinaryStrikes,
                p.capacityStrikes,
                p.capacityLimitedUntil,
                p.capacityLastErrorAt,
                e.eventId,
                p.accountId,
              ).changes !== 1
          ) {
            fail('冷却事实缺少实际账户');
          }
          break;
        }
        case 'OpeningAccountRecorded': {
          const p = e.payload;
          this.sqlite
            .prepare(
              `INSERT INTO upstream_accounts(id,display_name,enabled,credential_epoch,cooldown_until,cooldown_reason,ordinary_strikes,capacity_strikes,capacity_limited_until,capacity_last_error_at,source_event_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
            )
            .run(
              p.accountId,
              p.displayName,
              Number(p.enabled),
              p.credentialEpoch,
              p.cooldownUntil,
              p.cooldownReason,
              p.ordinaryStrikes,
              p.capacityStrikes,
              p.capacityLimitedUntil,
              p.capacityLastErrorAt,
              e.eventId,
            );
          break;
        }
        case 'OpeningGroupRecorded':
          this.sqlite
            .prepare('INSERT INTO sharing_groups(id,name,share,source_event_id) VALUES(?,?,?,?)')
            .run(e.payload.groupId, e.payload.name, e.payload.share, e.eventId);
          break;
        case 'OpeningGroupMemberRecorded':
          this.sqlite
            .prepare(
              'INSERT INTO group_members(member_id,group_id,weight,source_event_id) VALUES(?,?,?,?)',
            )
            .run(e.payload.memberId, e.payload.groupId, e.payload.weight, e.eventId);
          break;
        case 'OpeningPolicyRecorded': {
          const p = e.payload;
          this.sqlite
            .prepare(
              `INSERT INTO sharing_policy(id,enabled,bound_account_id,config_version,auto_anlas_fallback,free_policy_version,free_enabled,free_warning_threshold,free_hard_limit,loss_policy_version,loss_v5_24h,loss_fixed_24h,loss_fixed_30d,loss_member_attempts_24h,source_event_id) VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            )
            .run(
              Number(p.enabled),
              p.boundAccountId,
              p.configVersion,
              Number(p.autoAnlasFallback),
              p.freePolicyVersion,
              Number(p.freeEnabled),
              p.freeWarningThreshold,
              p.freeHardLimit,
              p.lossPolicyVersion,
              p.lossV5_24h,
              p.lossFixed24h,
              p.lossFixed30d,
              p.lossMemberAttempts24h,
              e.eventId,
            );
          break;
        }
        case 'OpeningWalletRecorded':
          this.sqlite
            .prepare(
              'INSERT INTO wallets(account_id,member_id,v5_available,anlas_fixed_available,anlas_purchased_available,source_event_id) VALUES(?,?,?,?,?,?)',
            )
            .run(
              e.payload.accountId,
              e.payload.memberId,
              e.payload.available.v5,
              e.payload.available.anlasFixed,
              e.payload.available.anlasPurchased,
              e.eventId,
            );
          break;
        case 'OpeningQuotaRecorded':
          this.quotas.openAccountQuota(e);
          break;
        case 'OpeningOperatingRecorded': {
          const p = e.payload;
          this.sqlite
            .prepare(
              `INSERT INTO operating_state(account_id,v5_available,anlas_fixed_available,anlas_purchased_available,unfunded_fixed,unfunded_purchased,unattributed_fixed,unattributed_purchased,v5_capacity_gap,fixed_gap_event_id,purchased_gap_event_id,v5_gap_event_id,source_event_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            )
            .run(
              p.accountId,
              p.available.v5,
              p.available.anlasFixed,
              p.available.anlasPurchased,
              p.unfundedFixed,
              p.unfundedPurchased,
              p.unattributedFixed,
              p.unattributedPurchased,
              p.v5CapacityGap,
              p.fixedGapEventId,
              p.purchasedGapEventId,
              p.v5GapEventId,
              e.eventId,
            );
          break;
        }
        case 'OpeningChargeRecorded': {
          const p = e.payload;
          this.sqlite
            .prepare(
              `INSERT INTO charges(original_job_id,original_member_id,original_account_id,job_id,wallet_account_id,wallet_member_id,attempt_id,status,resolution,origin_ref,v5,anlas_fixed,anlas_purchased,billing_mode,estimated,v5_equivalent,shared_v5_reserved,config_version,reserved_at,terminal_at,reservation_event_id,terminal_event_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            )
            .run(
              p.originalJobId,
              p.originalMemberId,
              p.originalAccountId,
              p.jobId,
              p.walletAccountId,
              p.walletMemberId,
              p.attemptId,
              p.status,
              p.resolution,
              p.originRef,
              p.resources.v5,
              p.resources.anlasFixed,
              p.resources.anlasPurchased,
              p.billingMode,
              p.estimated === null ? null : Number(p.estimated),
              p.v5Equivalent,
              p.sharedV5Reserved,
              p.configVersion,
              p.reservedAt,
              p.terminalAt,
              p.reservationEventId,
              p.terminalEventId,
            );
          break;
        }
        case 'QuotaReserved': {
          const p = e.payload,
            job = this.eventJob(e, p.jobId, p.accountId);
          if (
            job.status !== 'queued' ||
            job.memberId !== p.memberId ||
            this.readCharge(job.id) ||
            this.attempt(job.id)
          ) {
            fail('任务已有预扣或不能派发');
          }
          const n = Object.fromEntries(resources.map((r) => [r, parseAmount(p.resources[r])]));
          if (
            (p.billingMode === 'free' && resources.some((r) => n[r] !== 0n)) ||
            (p.billingMode === 'v5' &&
              (n.v5 <= 0n || n.anlasFixed !== 0n || n.anlasPurchased !== 0n)) ||
            (['anlas', 'anlas_fallback'].includes(p.billingMode) &&
              (n.v5 !== 0n || n.anlasFixed + n.anlasPurchased <= 0n))
          ) {
            fail('预扣资源与计费模式不符');
          }
          if (
            p.billingMode === 'anlas_fallback'
              ? parseAmount(p.v5Equivalent) <= 0n ||
                parseAmount(p.sharedV5Reserved) > parseAmount(p.v5Equivalent)
              : p.v5Equivalent !== '0' || p.sharedV5Reserved !== '0'
          ) {
            fail('fallback 预扣事实不符');
          }
          this.adjustWallet(p.accountId, p.memberId, p.resources, -1, e.eventId);
          this.sqlite
            .prepare(
              `INSERT INTO charges(original_job_id,original_member_id,original_account_id,job_id,wallet_account_id,wallet_member_id,attempt_id,status,resolution,origin_ref,v5,anlas_fixed,anlas_purchased,billing_mode,estimated,v5_equivalent,shared_v5_reserved,config_version,reserved_at,terminal_at,reservation_event_id,terminal_event_id) VALUES(?,?,?,?,?,?,NULL,'reserved','live',?,?,?,?,?,?,?,?,?,?,NULL,?,NULL)`,
            )
            .run(
              p.jobId,
              p.memberId,
              p.accountId,
              p.jobId,
              p.accountId,
              p.memberId,
              e.eventId,
              p.resources.v5,
              p.resources.anlasFixed,
              p.resources.anlasPurchased,
              p.billingMode,
              Number(p.estimated),
              p.v5Equivalent,
              p.sharedV5Reserved,
              p.configVersion,
              e.occurredAt,
              e.eventId,
            );
          break;
        }
        case 'DispatchAuthorized': {
          const p = e.payload,
            job = this.eventJob(e, p.jobId, p.accountId),
            c = this.readCharge(job.id);
          if (
            job.status !== 'queued' ||
            !c ||
            c.status !== 'reserved' ||
            c.originalAccountId !== p.accountId ||
            c.originalMemberId !== job.memberId ||
            this.sqlite
              .prepare<[string], { epoch: number }>(
                'SELECT credential_epoch AS epoch FROM upstream_accounts WHERE id=?',
              )
              .get(p.accountId)?.epoch !== p.credentialEpoch ||
            c.attemptId ||
            this.attempt(job.id)
          ) {
            fail('派发与真实原预扣不一致');
          }
          this.sqlite
            .prepare(
              `INSERT INTO generation_attempts(id,job_id,account_id,credential_epoch,authorized_at,dispatch_state,candidate_mode,upstream_candidates,source_event_id) VALUES(?,?,?,?,?,'unknown',?,?,?)`,
            )
            .run(
              p.attemptId,
              p.jobId,
              p.accountId,
              p.credentialEpoch,
              e.occurredAt,
              p.candidateMode,
              canonicalJson(p.upstreamCandidates),
              e.eventId,
            );
          this.sqlite
            .prepare('UPDATE charges SET attempt_id=? WHERE original_job_id=?')
            .run(p.attemptId, job.id);
          this.sqlite
            .prepare("UPDATE generation_jobs SET status='running',source_event_id=? WHERE id=?")
            .run(e.eventId, job.id);
          break;
        }
        case 'ResponseEvidenceRecorded':
          this.writeResponse(e, e.payload.attemptId, e.payload.response);
          break;
        case 'ArtifactPrepared': {
          const p = e.payload,
            row = this.sqlite
              .prepare<[string], { job_id: string }>(
                'SELECT job_id FROM generation_attempts WHERE id=?',
              )
              .get(p.attemptId);
          if (!row) {
            fail('准备凭据缺少真实尝试');
          }
          const job = this.eventJob(e, row.job_id),
            a = this.attempt(job.id)!;
          const descriptor = {
            imageId: p.artifactId,
            file: p.file,
            operation: job.operation,
            mimeType: p.mimeType,
            byteLength: p.byteLength,
            sha256: p.sha256,
          };
          assertStoredResultDescriptor(descriptor);
          if (
            a.preparedArtifactId ||
            job.retiredAt !== null ||
            !['running', 'failed'].includes(job.status) ||
            p.response.dispatchState === 'not_sent'
          ) {
            fail('任务不能接受准备凭据');
          }
          this.writeResponse(e, a.id, p.response);
          this.sqlite
            .prepare(
              `UPDATE generation_attempts SET prepared_artifact_id=?,prepared_file=?,prepared_mime_type=?,prepared_byte_length=?,prepared_sha256=?,source_event_id=? WHERE id=?`,
            )
            .run(p.artifactId, p.file, p.mimeType, p.byteLength, p.sha256, e.eventId, a.id);
          break;
        }
        case 'ArtifactPublished': {
          const p = e.payload,
            a = this.sqlite
              .prepare<[string], { job_id: string }>(
                'SELECT job_id FROM generation_attempts WHERE id=?',
              )
              .get(p.originalAttemptId);
          if (!a) {
            fail('发布产物缺少真实尝试');
          }
          const job = this.job(a.job_id),
            attempt = this.attempt(job.id)!;
          if (
            job.retiredAt !== null ||
            e.memberId !== job.memberId ||
            e.accountId !== attempt.accountId ||
            p.memberId !== job.memberId ||
            p.inputId !== job.inputId ||
            p.operation !== job.operation ||
            p.artifactId !== attempt.preparedArtifactId ||
            p.sha256 !== attempt.preparedSha256 ||
            p.byteLength !== attempt.preparedByteLength ||
            p.mimeType !== attempt.preparedMimeType
          ) {
            fail('发布事实与原准备凭据不一致');
          }
          this.sqlite
            .prepare(
              `INSERT INTO artifacts(id,member_id,input_id,original_attempt_id,operation,mime_type,byte_length,sha256,published_at,retired_at,source_event_id) VALUES(?,?,?,?,?,?,?,?,?,NULL,?)`,
            )
            .run(
              p.artifactId,
              p.memberId,
              p.inputId,
              p.originalAttemptId,
              p.operation,
              p.mimeType,
              p.byteLength,
              p.sha256,
              e.occurredAt,
              e.eventId,
            );
          break;
        }
        case 'CachedResultLinked': {
          const p = e.payload,
            job = this.eventJob(e, p.jobId);
          this.assertReuseReadiness(job, e.occurredAt, job.admissionEpoch);
          const allowed = new Set([
            'ContextRegistered',
            'InputRegistered',
            'RequestAccepted',
            'JobAccepted',
            'RequestAliasRegistered',
            'CachedResultLinked',
          ]);
          if (
            events.some((event) => !allowed.has(event.eventType)) ||
            events.filter((event) => event.eventType === 'CachedResultLinked').length !== 1 ||
            events.filter((event) => event.eventType === 'RequestAccepted').length !== 1 ||
            events.filter((event) => event.eventType === 'JobAccepted').length !== 1 ||
            e.accountId !== null ||
            job.status !== 'pending' ||
            job.queueSeq !== null ||
            job.retiredAt !== null ||
            job.memberStatus !== 'active' ||
            !job.acceptingJobs ||
            job.admissionEpoch !== job.currentAdmissionEpoch ||
            this.attempt(job.id) ||
            this.readCharge(job.id)
          ) {
            fail('缓存引用不能创建执行或费用');
          }
          const artifact = this.replicas.readArtifact(p.artifactId);
          const original = this.sqlite
            .prepare<[string], { attemptId: string; jobId: string }>(
              'SELECT t.id AS attemptId,t.job_id AS jobId FROM artifacts a JOIN generation_attempts t ON t.id=a.original_attempt_id WHERE a.id=?',
            )
            .get(artifact.id);
          if (
            !original ||
            artifact.retiredAt !== null ||
            artifact.memberId !== job.memberId ||
            artifact.operation !== job.operation
          ) {
            fail('缓存引用缺少真实原产物');
          }
          const origin = this.job(original.jobId);
          if (artifact.memberId !== origin.memberId || artifact.inputId !== origin.inputId) {
            fail('缓存产物与原输入归属不符');
          }
          const proof = this.inputs.readGenerationReuseProof(origin.id);
          const current = this.inputs.readGenerationReuseProof(job.id);
          if (
            current.job.cacheKey !== proof.job.cacheKey ||
            current.prepared.firstSeed !== proof.prepared.firstSeed
          ) {
            fail('缓存引用与原生成意图不符');
          }
          this.sqlite
            .prepare(
              "UPDATE generation_jobs SET status='done',artifact_id=?,completed_at=?,source_event_id=? WHERE id=?",
            )
            .run(artifact.id, e.occurredAt, e.eventId, job.id);
          break;
        }
        case 'GenerationSettled':
        case 'GenerationRefunded': {
          const p = e.payload,
            c = this.reservation(e, p.jobId, p.reservationEventId),
            q = this.quotas.readAccountQuota(c.originalAccountId);
          if (e.eventType === 'GenerationRefunded') {
            if (e.payload.lossEventId) {
              const risk = events.find((candidate) => candidate.eventId === e.payload.lossEventId);
              if (
                risk?.eventType !== 'ReviewOpened' ||
                risk.payload.attemptId !== c.attemptId ||
                risk.payload.lossEventId !== risk.eventId
              ) {
                fail('退款核对事实必须在同一原提交中');
              }
            }
            this.adjustWallet(
              c.originalAccountId,
              c.originalMemberId,
              { v5: c.v5, anlasFixed: c.anlasFixed, anlasPurchased: c.anlasPurchased },
              1,
              e.eventId,
            );
          } else if (c.billingMode === 'anlas_fallback') {
            q.pendingFallbackFixed = formatAmount(
              parseAmount(q.pendingFallbackFixed) + parseAmount(c.anlasFixed),
            );
            q.pendingFallbackPurchased = formatAmount(
              parseAmount(q.pendingFallbackPurchased) + parseAmount(c.anlasPurchased),
            );
            q.pendingFallbackV5 = formatAmount(
              parseAmount(q.pendingFallbackV5) + parseAmount(c.sharedV5Reserved),
            );
            q.fallbackFixed = formatAmount(
              parseAmount(q.fallbackFixed) + parseAmount(c.anlasFixed),
            );
            q.fallbackPurchased = formatAmount(
              parseAmount(q.fallbackPurchased) + parseAmount(c.anlasPurchased),
            );
            q.sharedV5Debt = formatAmount(
              parseAmount(q.sharedV5Debt) + parseAmount(c.sharedV5Reserved),
            );
          } else {
            q.pendingV5Debit = formatAmount(parseAmount(q.pendingV5Debit) + parseAmount(c.v5));
            q.pendingFixedDebit = formatAmount(
              parseAmount(q.pendingFixedDebit) + parseAmount(c.anlasFixed),
            );
            q.pendingPurchasedDebit = formatAmount(
              parseAmount(q.pendingPurchasedDebit) + parseAmount(c.anlasPurchased),
            );
          }
          q.settlementCursorAt = Math.max(q.settlementCursorAt ?? 0, e.occurredAt);
          this.quotas.saveAccountQuota(q, e.eventId);
          this.sqlite
            .prepare(
              'UPDATE charges SET status=?,terminal_at=?,terminal_event_id=? WHERE original_job_id=?',
            )
            .run(
              e.eventType === 'GenerationSettled' ? 'settled' : 'refunded',
              e.occurredAt,
              e.eventId,
              c.originalJobId,
            );
          break;
        }
        case 'JobCompleted':
        case 'LateResultRecovered': {
          const p = e.payload,
            job = this.eventJob(e, p.jobId),
            a = this.attempt(job.id),
            c = this.readCharge(job.id);
          if (!a || !c || c.attemptId !== a.id) {
            fail('结果终态缺少原尝试与收费');
          }
          this.artifact(job, p.artifactId, a.id);
          if (e.eventType === 'JobCompleted') {
            if (job.status !== 'running' || c.status !== 'settled') {
              fail('成功终态与原收费不符');
            }
            this.sqlite
              .prepare(
                "UPDATE generation_jobs SET status='done',artifact_id=?,completed_at=?,source_event_id=? WHERE id=?",
              )
              .run(p.artifactId, e.occurredAt, e.eventId, job.id);
          } else {
            if (job.status !== 'failed' || c.status !== 'refunded' || job.artifactId) {
              fail('迟到结果必须保留失败与退款');
            }
            this.sqlite
              .prepare(
                'UPDATE generation_jobs SET artifact_id=?,late_result_at=?,source_event_id=? WHERE id=?',
              )
              .run(p.artifactId, e.occurredAt, e.eventId, job.id);
          }
          this.writeResponse(e, a.id, p.response);
          break;
        }
        case 'DiagnosticRecorded': {
          const job = this.eventJob(e, e.payload.jobId),
            attempt = this.attempt(job.id),
            charge = this.readCharge(job.id);
          if (
            !attempt ||
            job.status !== 'running' ||
            charge?.status !== 'refunded' ||
            e.accountId !== attempt.accountId
          ) {
            fail('诊断必须绑定原在途尝试及原资源退款');
          }
          this.inputs.readGenerationDiagnostic(e.payload.diagnosticId, job.id);
          break;
        }
        case 'JobFailed': {
          const p = e.payload,
            job = this.eventJob(e, p.jobId),
            c = this.readCharge(job.id),
            a = this.attempt(job.id);
          if (
            !['queued', 'pending', 'running'].includes(job.status) ||
            (job.status === 'running' && (!a || c?.status !== 'refunded')) ||
            (job.status !== 'running' && (a || c))
          ) {
            fail('失败终态与原派发/退款不一致');
          }
          if (a) {
            this.writeResponse(e, a.id, p.response);
          } else if (
            p.response.dispatchState !== 'not_sent' ||
            p.response.upstreamStartedAt !== null
          ) {
            fail('未派发任务不能补造尝试证据');
          }
          this.sqlite
            .prepare(
              "UPDATE generation_jobs SET status='failed',completed_at=?,error_code=?,error_status=?,cancellation_reason=?,source_event_id=? WHERE id=?",
            )
            .run(e.occurredAt, p.errorCode, p.errorStatus, p.cancellationReason, e.eventId, job.id);
          break;
        }
        case 'WaitingJobsCancelled': {
          for (const id of e.payload.jobIds) {
            const job = this.eventJob(e, id);
            if (
              !['queued', 'pending'].includes(job.status) ||
              this.readCharge(id) ||
              this.attempt(id)
            ) {
              fail('取消只能终止未派发意图');
            }
            this.sqlite
              .prepare(
                "UPDATE generation_jobs SET status='failed',completed_at=?,error_code=?,error_status=?,cancellation_reason=?,source_event_id=? WHERE id=?",
              )
              .run(
                e.occurredAt,
                e.payload.reason,
                e.payload.errorStatus,
                e.payload.cancellationReason,
                e.eventId,
                id,
              );
          }
          break;
        }
        case 'JobQueued': {
          const p = e.payload,
            job = this.eventJob(e, p.jobId);
          if (
            job.status !== 'pending' ||
            job.queueExpiresAt !== p.queueExpiresAt ||
            (p.queueExpiresAt !== null && e.occurredAt >= p.queueExpiresAt) ||
            this.sqlite
              .prepare(
                "SELECT 1 FROM generation_jobs WHERE request_id=? AND status IN ('failed','queued','running')",
              )
              .get(job.requestId)
          ) {
            fail('批次激活条件或原期限不符');
          }
          this.sqlite
            .prepare(
              "UPDATE generation_jobs SET status='queued',queue_seq=?,source_event_id=? WHERE id=?",
            )
            .run(BigInt(p.queueSeq), e.eventId, p.jobId);
          break;
        }
        case 'FreeImageSucceeded': {
          const p = e.payload,
            job = this.eventJob(e, p.jobId),
            c = this.readCharge(job.id);
          if (
            c?.billingMode !== 'free' ||
            job.freeSucceededAt !== null ||
            !job.artifactId ||
            !['done', 'failed'].includes(job.status)
          ) {
            fail('免费成功必须有唯一完整结果');
          }
          this.sqlite
            .prepare(
              'UPDATE generation_jobs SET free_succeeded_at=?,free_warning_threshold=?,source_event_id=? WHERE id=?',
            )
            .run(e.occurredAt, p.warningThreshold, e.eventId, job.id);
          break;
        }
        case 'ReviewOpened':
        case 'ReviewClosed': {
          const p = e.payload,
            row = this.sqlite
              .prepare<[string], { job_id: string }>(
                'SELECT job_id FROM generation_attempts WHERE id=?',
              )
              .get(p.attemptId);
          if (!row) {
            fail('核对事实缺少实际尝试');
          }
          const a = this.attempt(row.job_id)!,
            job = this.eventJob(e, a.jobId, a.accountId),
            c = this.readCharge(job.id);
          if (c?.status !== 'refunded' || c.attemptId !== a.id) {
            fail('核对事实与原退款不一致');
          }
          if (e.eventType === 'ReviewOpened') {
            const opened = e.payload;
            if (
              a.reviewStatus ||
              opened.lossEventId !== e.eventId ||
              opened.deadlineAt < opened.firstUnknownAt ||
              opened.response.dispatchState === 'not_sent' ||
              !JSON.parse(a.upstreamCandidates).length
            ) {
              fail('未知消费核对起点不符');
            }
            this.sqlite
              .prepare(
                `UPDATE generation_attempts SET review_status='pending',first_unknown_at=?,review_deadline_at=?,loss_event_id=?,source_event_id=? WHERE id=?`,
              )
              .run(opened.firstUnknownAt, opened.deadlineAt, opened.lossEventId, e.eventId, a.id);
          } else {
            const closed = e.payload;
            if (
              a.lossEventId !== closed.lossEventId ||
              !['pending', 'estimated_closed'].includes(a.reviewStatus || '') ||
              (closed.status === 'estimated_closed' &&
                (a.reviewStatus !== 'pending' ||
                  e.occurredAt < (a.reviewDeadlineAt ?? Infinity))) ||
              (closed.status === 'result_recovered' && !job.artifactId)
            ) {
              fail('核对结案与保存的窗口/结果不符');
            }
            this.sqlite
              .prepare(
                'UPDATE generation_attempts SET review_status=?,review_closed_at=?,closure_event_id=?,actual_consumption=?,source_event_id=? WHERE id=?',
              )
              .run(
                closed.status,
                e.occurredAt,
                e.eventId,
                closed.actualConsumption === null ? null : canonicalJson(closed.actualConsumption),
                e.eventId,
                a.id,
              );
          }
          break;
        }
        default:
          throw new Error(`Generation replay cannot apply another owner's fact: ${e.eventType}`);
      }
    };
  }
  replayGenerationCommit(stored: StoredRelationalCommit, sourceInputs?: RelationalInputStore) {
    return this.owned(() =>
      new RelationalCommitReplayer(this.sqlite, this).replayGenerationCommit(stored, sourceInputs),
    );
  }
}
