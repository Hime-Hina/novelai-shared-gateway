import { createId } from '../storage/record-values.ts';
import type {
  JsonStore,
  PersistenceOptions,
  StoreDatabase,
  StoreItem,
  StoredGenerationRequest,
} from '../storage/sqlite-store.ts';
import { isNovelAiCapacityError } from '../http/public-errors.ts';
import {
  reserveFairQuota,
  settleFairQuota,
  resolveFairQuotaCharge,
  syncFairQuota,
  quoteFairRequest,
  type FairCharge,
} from '../quota/fair-share.ts';
import { memberGenerationCapabilities } from './capabilities.ts';
import {
  addMemberGenerationResources,
  assertMemberGenerationCost,
  assertMemberGenerationVersions,
  memberGenerationChargeResources,
  parseMemberGenerationConfirmation,
} from './member-contract.ts';
import { recordDomainEvent, recordGenerationDiagnostic } from '../events/domain-events.ts';
import {
  recordSharedCompletion,
  sharedAccountConcurrency,
  sharedAccountReadyDelay,
} from '../quota/shared-capacity.ts';
import {
  assertFreeImageAdmission,
  freeImageSuccessWarningThreshold,
  type FreeImageUsage,
} from '../quota/free-image-policy.ts';
import { describeStoredResult, publishStoredResult } from '../storage/result-files.ts';
import {
  assertMemberAcceptsJobs,
  queuedDeadlineTimestamp,
  setMemberAdmission,
} from './queue-policy.ts';
import {
  activateNextBatchJob,
  cancelWaitingBatchJobs,
  singleGenerationRequest,
} from './batch-requests.ts';
import {
  closeGenerationReview,
  recordRecoveredGeneration,
  generationResponseEvidence,
} from './accounting.ts';
import { validateImageBytes } from '../protocols/native-image.ts';
import type { generateNovelAiImage } from '../protocols/novelai-client.ts';
import path from 'node:path';

export interface GenerationClock {
  now?: () => number;
  env?: NodeJS.ProcessEnv;
}
export interface GenerationTiming {
  upstreamStartedAt?: string;
  upstreamHeadersAt?: string;
  upstreamCompletedAt?: string;
  upstreamHttpStatus?: number;
  upstreamRetryAfterUntil?: string;
}
export interface GenerationReservation {
  job?: StoreItem;
  account: StoreItem | null;
  token?: string;
  userId?: string;
  ledgerId?: string;
  cost?: number;
  accountCost?: number;
  cacheKey?: string;
  upstreamTiming?: GenerationTiming;
  failurePhase?: string;
}
export type GenerationQueueReservation = (
  | (GenerationReservation & {
      job: StoreItem;
      skip?: false;
      queued?: false;
      changed?: boolean;
      jobId?: string;
      failure?: string;
      statusCode?: number;
    })
  | {
      skip: true;
      queued?: false;
      job?: undefined;
      jobId?: string;
      failure?: string;
      statusCode?: number;
      changed?: boolean;
    }
  | {
      queued: true;
      skip?: false;
      job?: undefined;
      jobId: string;
      failure?: string;
      statusCode?: number;
      changed?: boolean;
    }
) & { cooldownUntil?: string };
interface GenerationError extends Error {
  statusCode?: number;
  code?: string;
  cooldownUntil?: string;
  upstreamStatus?: number;
  upstreamRetryAfterUntil?: string;
}
export type GenerationContent = (
  | Awaited<ReturnType<typeof generateNovelAiImage>>
  | NonNullable<Awaited<ReturnType<typeof publishStoredResult>>>
) & { mock?: boolean };
function fail(statusCode: number, message: string): never {
  throw Object.assign(new Error(message), { statusCode });
}
function optionalString(value: unknown, name: string): string | undefined {
  if (value === undefined || value === null) {
    return;
  }
  if (typeof value !== 'string') {
    throw new TypeError(`${name} must be a string`);
  }
  return value;
}
function requiredString(value: unknown, name: string): string {
  const result = optionalString(value, name);
  if (!result) {
    throw new TypeError(`${name} is required`);
  }
  return result;
}
function optionalNumber(value: unknown, name: string): number | undefined {
  if (value === undefined || value === null) {
    return;
  }
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new TypeError(`${name} must be finite`);
  }
  return value;
}
function requiredNumber(value: unknown, name: string): number {
  const result = optionalNumber(value, name);
  if (result === undefined) {
    throw new TypeError(`${name} is required`);
  }
  return result;
}
function normalizedAccountCost(value: unknown): number {
  const amount = Number(value);
  return Number.isFinite(amount) && amount > 0 ? Math.ceil(amount) : 0;
}

const publishingArtifacts = new Map<
  string,
  Promise<Awaited<ReturnType<typeof publishStoredResult>>>
>();

export async function publishGenerationArtifact(
  dataDir: string,
  pending: NonNullable<StoreItem['resultPending']>,
  bytes: Buffer,
  publish = publishStoredResult,
) {
  const key = `${path.resolve(dataDir)}:${pending.attemptId}:${pending.imageId}`;
  const active = publishingArtifacts.get(key);
  if (active) {
    return active;
  }
  const publication = publish(dataDir, pending, bytes);
  publishingArtifacts.set(key, publication);
  try {
    return await publication;
  } finally {
    if (publishingArtifacts.get(key) === publication) {
      publishingArtifacts.delete(key);
    }
  }
}

export const generationStreamId = (job: Pick<StoreItem, 'id' | 'requestId'>): string =>
  `request:${job.requestId || `legacy-${job.id}`}`;

export function assertConfirmedDispatch(
  db: StoreDatabase,
  member: StoreItem,
  request: StoredGenerationRequest,
  priorCharges: (FairCharge | null | undefined)[],
  finalCharge: Parameters<typeof memberGenerationChargeResources>[0],
  env = process.env,
): void {
  if (request.clientContract !== 'member-generation-v1') {
    return;
  }
  const confirmation = parseMemberGenerationConfirmation(request.confirmation);
  assertMemberGenerationVersions(
    confirmation,
    memberGenerationCapabilities(db, member, { env }).versions,
  );
  const resources = memberGenerationChargeResources(finalCharge);
  const prior = priorCharges.filter(
    (charge): charge is FairCharge => !!charge && ['reserved', 'settled'].includes(charge.status),
  );
  assertMemberGenerationCost(
    confirmation,
    resources,
    addMemberGenerationResources(resources, ...prior.map(memberGenerationChargeResources)),
  );
}

export function recordGenerationFailure(db: StoreDatabase, job: StoreItem): void {
  job.errorCode ||= job.cancelledAt ? 'request_cancelled' : 'generation_failed';
  job.errorStatus ||= 500;
  const diagnosticRef = job.errorDetail
    ? recordGenerationDiagnostic(db, job.id, job.errorDetail)
    : null;
  if (diagnosticRef) {
    job.diagnosticRef = diagnosticRef;
  }
  recordDomainEvent(db, {
    eventType: 'JobFailed',
    streamId: generationStreamId(job),
    memberId: job.fairCharge?.userId || (job.memberId as string | undefined),
    accountId: job.accountId || null,
    occurredAt: job.completedAt || job.updatedAt || new Date().toISOString(),
    payload: {
      jobId: job.id,
      failedAt: requiredString(job.completedAt || job.updatedAt, 'failedAt'),
      errorStatus: job.errorStatus || 500,
      errorCode: requiredString(job.errorCode, 'errorCode'),
      message: job.error || '生成失败',
      cancellationReason: optionalString(job.cancellationReason, 'cancellationReason') || null,
      cancelledAt: job.cancelledAt || null,
      ...(diagnosticRef ? { diagnosticRef } : {}),
      charge: job.fairCharge || null,
      refund: job.generationRefund || null,
      review: job.generationReview || null,
      response: generationResponseEvidence(job),
    },
  });
}

export function recordPreparedArtifact(db: StoreDatabase, job: StoreItem, now = Date.now()): void {
  const pending = job.resultPending;
  if (!pending) {
    return;
  }
  recordDomainEvent(db, {
    eventType: 'ArtifactPrepared',
    streamId: generationStreamId(job),
    occurredAt: now,
    payload: {
      jobId: job.id,
      artifact: { id: pending.imageId, ...pending },
      response: generationResponseEvidence(job),
    },
  });
}

export function recordCompletedArtifact(
  db: StoreDatabase,
  job: StoreItem,
  image: StoreItem,
  now: number,
  freeUsage?: FreeImageUsage,
): void {
  recordDomainEvent(db, {
    eventType: 'ArtifactPublished',
    streamId: generationStreamId(job),
    occurredAt: now,
    payload: {
      jobId: job.id,
      memberId: requiredString(job.fairCharge?.userId || job.memberId, 'artifact memberId'),
      accountId: job.accountId || null,
      artifact: {
        id: image.id,
        file: requiredString(image.file, 'artifact file'),
        mimeType: requiredString(image.mimeType, 'artifact mimeType'),
        byteLength: requiredNumber(image.byteLength, 'artifact byteLength'),
        sha256: requiredString(image.sha256, 'artifact sha256'),
        operation: optionalString(image.operation, 'artifact operation') || 'generate',
        r2: image.r2 || null,
        createdAt: requiredString(image.createdAt, 'artifact createdAt'),
        attemptId: job.generationAttempt?.id || null,
        storageNamespace: 'gateway-local',
        inputRef: job.requestId || `legacy-${job.id}`,
        model: image.model,
        width: image.width,
        height: image.height,
        requestedSteps: optionalNumber(image.requestedSteps, 'requestedSteps'),
        routedSteps: optionalNumber(image.routedSteps, 'routedSteps'),
        cacheKey: image.cacheKey || '',
        mock: image.mock === true,
        cost: image.cost || 0,
        accountCost: image.accountCost || 0,
      },
    },
  });
  if (job.status === 'done') {
    recordDomainEvent(db, {
      eventType: 'JobCompleted',
      streamId: generationStreamId(job),
      occurredAt: now,
      payload: {
        jobId: job.id,
        artifactId: image.id,
        accountId: job.accountId || null,
        completedAt: job.completedAt || new Date(now).toISOString(),
        charge: job.fairCharge || null,
        response: generationResponseEvidence(job),
      },
    });
  }
  recordFreeImageSuccess(db, job, now, freeUsage);
}

export function recordFreeImageSuccess(
  db: StoreDatabase,
  job: StoreItem,
  now: number,
  usage?: FreeImageUsage,
): void {
  if (job.fairCharge?.resource === 'free' && (job.status === 'done' || job.lateResultAvailable)) {
    const warningThreshold = freeImageSuccessWarningThreshold(usage);
    recordDomainEvent(db, {
      eventType: 'FreeImageSucceeded',
      streamId: generationStreamId(job),
      memberId: job.fairCharge.userId,
      occurredAt: now,
      payload: {
        jobId: job.id,
        memberId: job.fairCharge.userId,
        succeededAt: now,
        warningThreshold,
      },
    });
  }
}

export function completeSharedAccount(
  db: StoreDatabase,
  account: StoreItem | null | undefined,
  now: number,
  options: NonNullable<Parameters<typeof recordSharedCompletion>[2]>,
): boolean {
  if (!recordSharedCompletion(account, now, options) || !account) {
    return false;
  }
  recordDomainEvent(db, {
    eventType: 'CooldownChanged',
    streamId: `account:${account.id}`,
    accountId: account.id,
    occurredAt: now,
    payload: {
      cooldownUntil: account.cooldownUntil || null,
      cooldownReason: optionalString(account.cooldownReason, 'cooldownReason') || null,
      ordinaryStrikeCount: Number(account.ordinaryStrikeCount || 0),
      capacityStrikeCount: Number(account.capacityStrikeCount || 0),
      capacityConcurrencyLimit:
        optionalNumber(account.capacityConcurrencyLimit, 'capacityConcurrencyLimit') || null,
      capacityLastErrorAt:
        optionalString(account.capacityLastErrorAt, 'capacityLastErrorAt') || null,
      jobId: options.job?.id || null,
      sharedCooldown: options.job?.sharedCooldown || null,
    },
  });
  return true;
}

function changedJobRows(db: StoreDatabase, jobIds: string[], extraAccountIds: string[] = []) {
  const selected = new Set(jobIds);
  const requestIds = new Set(
    db.jobs.filter((job) => selected.has(job.id) && job.requestId).map((job) => job.requestId),
  );
  for (const job of db.jobs) {
    if (job.requestId && requestIds.has(job.requestId)) {
      selected.add(job.id);
    }
  }
  const jobs = db.jobs.filter((job) => selected.has(job.id));
  const ledger = db.ledger.filter((entry) => selected.has(entry.jobId || ''));
  return {
    jobs: [...selected],
    accounts: [
      ...new Set(
        [...extraAccountIds, ...jobs.map((job) => job.accountId)].filter(
          (id): id is string => !!id,
        ),
      ),
    ],
    ledger: ledger.map((entry) => entry.id),
  };
}

export function refundGenerationJob(
  store: JsonStore,
  db: StoreDatabase,
  job: StoreItem,
  options: GenerationClock & { evidence?: Parameters<typeof settleFairQuota>[4] } = {},
) {
  const now = (options.now || Date.now)();
  return settleFairQuota(
    db,
    job,
    false,
    now,
    options.evidence || {},
    store.readFairQuotaSettlement(job.id),
  );
}

export function generationJobDeadline(job: StoreItem): number {
  const deadline = Date.parse(job.deadlineAt || '');
  return Number.isFinite(deadline) && deadline > 0 ? deadline : 0;
}
function timeoutMs(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const configured = Number(env[name] || 0);
  return Math.max(
    60_000,
    Math.floor(Number.isFinite(configured) && configured > 0 ? configured : fallback),
  );
}
export function isStaleGenerationJob(job: StoreItem, now = Date.now(), env = process.env): boolean {
  if (!['queued', 'pending', 'running'].includes(job.status!)) {
    return false;
  }
  const deadline = generationJobDeadline(job),
    queuedDeadline = queuedDeadlineTimestamp(job, env);
  if ((deadline && now >= deadline) || (queuedDeadline && now >= queuedDeadline)) {
    return true;
  }
  const updatedAt = Date.parse(job.updatedAt || job.createdAt || '');
  if (!Number.isFinite(updatedAt) || updatedAt <= 0) {
    return false;
  }
  const configuredInflight = Number(env.ACCOUNT_INFLIGHT_TIMEOUT_MS || 600_000);
  const inflight =
    Number.isFinite(configuredInflight) && configuredInflight > 0
      ? Math.max(1000, Math.floor(configuredInflight))
      : 600_000;
  if (job.status === 'running') {
    return now - updatedAt > timeoutMs(env, 'STALE_RUNNING_JOB_MS', inflight + 60_000);
  }
  return (
    job.status === 'queued' &&
    !deadline &&
    now - updatedAt >
      timeoutMs(env, 'STALE_QUEUED_JOB_MS', timeoutMs(env, 'STALE_ACTIVE_JOB_MS', 30 * 60_000))
  );
}
export function staleGenerationJobDetail(
  job: StoreItem,
  now = Date.now(),
  env = process.env,
): string {
  const deadline = generationJobDeadline(job),
    queuedDeadline = queuedDeadlineTimestamp(job, env);
  if (deadline && now >= deadline) {
    return 'job deadline expired';
  }
  if (queuedDeadline && now >= queuedDeadline) {
    return 'queued job exceeded wait deadline';
  }
  return `${job.status || 'active'} job exceeded stale timeout`;
}

export interface GenerationDispatchOptions extends GenerationClock {
  dispatchEnabled(): boolean;
  physicalAccountLoad(id: string): number;
}

// 先在同一命令中核验资格、物理槽和冷却并预留，提交派发事实后调用者才可连接上游。
// 任务超时退款不代表物理连接已结束，不能仅按逻辑 running 数释放账号容量。
export async function reserveQueuedGeneration(
  store: JsonStore,
  jobId: string,
  options: GenerationDispatchOptions,
): Promise<GenerationQueueReservation> {
  const clock = options.now || Date.now,
    env = options.env || process.env;
  return store.update<GenerationQueueReservation>(
    (db) => {
      if (!options.dispatchEnabled()) {
        return { skip: true, changed: false };
      }
      const job = db.jobs.find((item) => item.id === jobId);
      if (!job) {
        throw new Error('job not found.');
      }
      if (job.status !== 'queued') {
        return { skip: true, changed: false };
      }
      const dispatchAt = clock(),
        at = new Date(dispatchAt).toISOString(),
        accountCost = normalizedAccountCost(job.accountCost);
      job.accountCost = accountCost;
      const refund = () => refundGenerationJob(store, db, job, { now: () => dispatchAt });
      if (isStaleGenerationJob(job, dispatchAt, env)) {
        refund();
        cancelWaitingBatchJobs(db, job, '连接超时', 504, dispatchAt);
        Object.assign(job, {
          status: 'failed',
          error: '连接超时',
          errorStatus: 504,
          errorDetail: staleGenerationJobDetail(job, dispatchAt, env),
          updatedAt: at,
          completedAt: at,
        });
        recordGenerationFailure(db, job);
        return { skip: true, jobId: job.id, failure: job.error, statusCode: 504 };
      }
      let account: StoreItem;
      const state = syncFairQuota(db, dispatchAt);
      try {
        const user = db.users.find(
          (member) =>
            (job.memberId ? member.id === job.memberId : member.token === job.userToken) &&
            member.enabled !== false,
        );
        if (!user) {
          fail(401, 'invalid member.');
        }
        assertMemberAcceptsJobs(user, job.jobAdmissionVersion);
        if (!state?.enabled || state.status !== 'active') {
          fail(503, `共享 Opus 暂不可用：${state?.status || '未配置成员'}`);
        }
        const boundAccount = db.accounts.find((item) => item.id === state.boundAccountId);
        if (!boundAccount || boundAccount.enabled === false) {
          fail(503, '绑定的 Opus 账号未启用');
        }
        account = boundAccount;
        const running = db.jobs.filter(
          (item) => item.status === 'running' && item.accountId === account.id,
        ).length;
        if (
          Math.max(
            running,
            Number(account.inFlight || 0),
            options.physicalAccountLoad(account.id),
          ) >= sharedAccountConcurrency() ||
          sharedAccountReadyDelay(account, dispatchAt) > 0
        ) {
          return { queued: true, jobId: job.id };
        }
        const request = job.requestId
          ? singleGenerationRequest(store.readGenerationRequest(job.requestId)!, job.sampleIndex)
          : job.request;
        if (!request) {
          fail(409, '持久生成请求不存在');
        }
        let priorCharges: (FairCharge | null)[] = [];
        if (request.clientContract === 'member-generation-v1') {
          const children = store.readGenerationRequestJobIds(job.requestId!, user.id);
          if (!children) {
            fail(409, '持久生成请求归属不一致');
          }
          priorCharges = children
            .filter((id) => id !== job.id)
            .map(
              (id) => db.settings.fairQuota?.reservations[id] || store.readFairQuotaSettlement(id),
            );
        }
        if (state.freeImagePolicy?.enabled && quoteFairRequest(request).resource === 'free') {
          assertFreeImageAdmission(store.readFreeImageUsage(db, user.id, dispatchAt));
        }
        reserveFairQuota(
          db,
          user,
          job,
          account,
          dispatchAt,
          request,
          store.readFairQuotaSettlement(job.id),
          (charge) =>
            assertConfirmedDispatch(
              db,
              user,
              request as StoredGenerationRequest,
              priorCharges,
              charge,
              env,
            ),
        );
      } catch (error) {
        const failure = error as GenerationError;
        refund();
        cancelWaitingBatchJobs(db, job, failure.message, failure.statusCode || 503, dispatchAt);
        Object.assign(job, {
          status: 'failed',
          error: failure.message,
          errorStatus: failure.statusCode || 503,
          errorCode: failure.code || 'generation_failed',
          quotaRetryAfterUntil: failure.cooldownUntil,
          updatedAt: at,
          completedAt: at,
        });
        recordGenerationFailure(db, job);
        return {
          skip: true,
          jobId: job.id,
          failure: job.error,
          statusCode: job.errorStatus,
          cooldownUntil: job.quotaRetryAfterUntil,
        };
      }
      const running = db.jobs.filter(
        (item) => item.status === 'running' && item.accountId === account.id,
      ).length;
      account.inFlight = Math.max(running, Number(account.inFlight || 0)) + 1;
      account.lastUsedAt = at;
      account.lastSharedDispatchAt = at;
      Object.assign(job, {
        status: 'running',
        startedAt: at,
        accountId: account.id,
        completedAt: '',
        updatedAt: at,
      });
      return {
        job,
        account: { ...account },
        token: job.userToken,
        cost: job.cost,
        accountCost,
        cacheKey: job.cacheKey || '',
      };
    },
    {
      dirtyRows: (result, db) => ({
        ...changedJobRows(db, [result.job?.id || result.jobId || jobId]),
        ledger: db.ledger.map((entry) => entry.id),
      }),
      shouldPersist: (result) => result.changed !== false,
    },
  );
}

export async function cancelGenerationRequestCommand(
  store: JsonStore,
  requestId: string,
  memberId: string,
  options: GenerationClock = {},
) {
  return store.update(
    (db) => {
      if (!db.users.some((user) => user.id === memberId && user.enabled !== false)) {
        fail(401, 'invalid member.');
      }
      const request = store.readGenerationRequestState(requestId, memberId);
      if (!request) {
        fail(404, 'request not found.');
      }
      const cancelledJobIds: string[] = [],
        message = '请求已取消，未派发任务已终止',
        now = (options.now || Date.now)(),
        at = new Date(now).toISOString();
      for (const child of request.jobs) {
        if (!['queued', 'pending'].includes(child.status!)) {
          continue;
        }
        const job = store.loadJobForUpdate(child.id)!;
        refundGenerationJob(store, db, job, { now: () => now });
        Object.assign(job, {
          status: 'failed',
          error: message,
          errorDetail: message,
          errorStatus: 409,
          cancellationReason: 'request_cancelled',
          cancelledAt: at,
          completedAt: at,
          updatedAt: at,
        });
        recordGenerationFailure(db, job);
        cancelledJobIds.push(job.id);
      }
      return { cancelledJobIds };
    },
    {
      immediate: true,
      includeSettings: true,
      domainCommand: { actorId: memberId, actorKind: 'member', correlationId: requestId },
      dirtyRows: (result, db) => ({
        users: [memberId],
        jobs: result.cancelledJobIds,
        ledger: db.ledger
          .filter((entry) => result.cancelledJobIds.includes(entry.jobId!))
          .map((entry) => entry.id),
      }),
    },
  );
}

export async function changeMemberAdmissionCommand(
  store: JsonStore,
  userId: string,
  acceptingJobs: boolean,
  options: GenerationClock & { actorId?: string } & Pick<
      PersistenceOptions,
      'domainCommand' | 'authorize'
    > = {},
) {
  return store.update(
    (db) => {
      const now = (options.now || Date.now)();
      return setMemberAdmission(
        db,
        userId,
        acceptingJobs,
        (draft, job) => refundGenerationJob(store, draft, job, { now: () => now }),
        now,
      );
    },
    {
      immediate: true,
      includeSettings: true,
      authorize: options.authorize,
      domainCommand: options.domainCommand || {
        actorId: options.actorId || 'local-admin',
        actorKind: 'admin',
      },
      dirtyRows: (result, db) => ({
        users: [result.id],
        jobs: result.cancelledJobIds,
        ledger: db.ledger
          .filter((entry) => result.cancelledJobIds.includes(entry.jobId!))
          .map((entry) => entry.id),
      }),
    },
  );
}

export function completionJob(
  store: JsonStore,
  db: StoreDatabase,
  reservation: GenerationReservation,
  jobId: string,
  job = store.loadJobForUpdate(jobId),
): StoreItem & { fairCharge: FairCharge } {
  if (job) {
    job.fairCharge = resolveFairQuotaCharge(db, job, store.readFairQuotaSettlement(jobId));
  }
  if (!job || !['reserved', 'refunded', 'settled'].includes(job.fairCharge?.status!)) {
    fail(409, '任务结算凭据不存在');
  }
  if (job.userToken !== reservation.token || job.accountId !== reservation.account?.id) {
    fail(409, '生成结果与任务归属不符');
  }
  if (job.generationAttempt?.id !== reservation.job?.generationAttempt?.id) {
    fail(409, '生成结果与执行尝试不符');
  }
  if (
    job.generationAttempt &&
    (job.generationAttempt.jobId !== job.id ||
      job.generationAttempt.memberId !== job.fairCharge!.userId ||
      job.generationAttempt.accountId !== job.accountId)
  ) {
    fail(409, '任务与执行尝试归属不符');
  }
  return job as StoreItem & { fairCharge: FairCharge };
}

export interface GenerationResultOptions extends GenerationClock {
  store: JsonStore;
  dataDir: string;
  createId?: typeof createId;
  publish?: typeof publishStoredResult;
}

export async function prepareGenerationResult(
  store: JsonStore,
  reservation: GenerationReservation,
  request: StoredGenerationRequest,
  image: GenerationContent,
  jobId: string,
  options: Pick<GenerationResultOptions, 'now' | 'createId'> = {},
) {
  return store.update(
    (db) => {
      const job = completionJob(store, db, reservation, jobId);
      if (job.imageId) {
        const existing =
          db.images.find((item) => item.id === job.imageId) ||
          store.selectItemById('images', job.imageId);
        if (existing) {
          if (existing.token !== job.userToken) {
            fail(409, '已存结果与任务归属不符');
          }
          return null;
        }
      }
      if (job.fairCharge.status === 'settled') {
        fail(409, '任务已结算但结果缺失');
      }
      if (!job.generationAttempt?.id) {
        fail(409, '任务执行尝试不存在');
      }
      const pending = {
        ...describeStoredResult(
          job.resultPending?.imageId || (options.createId || createId)('img'),
          request.operation || 'generate',
          image,
        ),
        attemptId: job.generationAttempt.id,
      };
      if (
        job.resultPending &&
        (Object.keys(pending) as (keyof typeof pending)[]).some(
          (key) => job.resultPending![key] !== pending[key],
        )
      ) {
        fail(409, '待保存结果与原结果不符');
      }
      if (store.selectItemById('images', pending.imageId)) {
        fail(409, '待保存结果 ID 已被使用');
      }
      Object.assign(job, reservation.upstreamTiming || {});
      job.resultPending = pending;
      recordPreparedArtifact(db, job, (options.now || Date.now)());
      return pending;
    },
    { dirtyRows: { jobs: [jobId] } },
  );
}

/**
 * 先提交待恢复描述，再持久发布文件，最后在另一短事务中关联产物并结算。
 * 崩溃恢复按描述的长度、摘要和尝试核验已发布文件，不重新请求上游。
 * 已退款任务只补存迟到结果，保留退款且不推进下一张子任务。
 */
export async function completeGenerationCommand(
  options: GenerationResultOptions,
  reservation: GenerationReservation,
  request: StoredGenerationRequest,
  image: GenerationContent,
  meta: { jobId: string },
): Promise<StoreItem & { balance: number }> {
  const { store, dataDir } = options,
    clock = options.now || Date.now;
  const pending = await prepareGenerationResult(
    store,
    reservation,
    request,
    image,
    meta.jobId,
    options,
  );
  if (pending) {
    await publishGenerationArtifact(
      dataDir,
      pending,
      image.buffer,
      options.publish || publishStoredResult,
    );
  }
  return store.update(
    (db) => {
      const now = clock(),
        at = new Date(now).toISOString(),
        job = completionJob(store, db, reservation, meta.jobId);
      if (job.imageId) {
        const existing =
          db.images.find((item) => item.id === job.imageId) ||
          store.selectItemById('images', job.imageId);
        if (existing) {
          if (existing.token !== job.userToken) {
            fail(409, '已存结果与任务归属不符');
          }
          if (job.resultPending) {
            if (
              job.resultPending.imageId !== existing.id ||
              job.resultPending.sha256 !== existing.sha256 ||
              job.resultPending.byteLength !== existing.byteLength
            ) {
              fail(409, '已存结果与待恢复证据不符');
            }
            delete job.resultPending;
          }
          if (
            job.fairCharge.status === 'refunded' &&
            recordRecoveredGeneration(db, job, existing.id, now)
          ) {
            recordFreeImageSuccess(
              db,
              job,
              now,
              store.readFreeImageUsage(db, job.fairCharge.userId, now),
            );
            if (!existing.mock) {
              completeSharedAccount(
                db,
                db.accounts.find((account) => account.id === job.accountId),
                now,
                { job, kind: 'success', retryAfterUntil: job.upstreamRetryAfterUntil },
              );
            }
          }
          return {
            ...existing,
            balance: db.users.find((user) => user.token === reservation.token)?.balance || 0,
          };
        }
      }
      const refunded = job.fairCharge.status === 'refunded';
      if (!refunded && job.fairCharge.status !== 'reserved') {
        fail(409, '任务已结算但结果缺失');
      }
      if (!pending) {
        fail(409, '已发布结果的凭据不存在');
      }
      if (
        job.resultPending?.attemptId !== pending.attemptId ||
        job.resultPending?.sha256 !== pending.sha256
      ) {
        fail(409, '待保存结果已改变');
      }
      const account = db.accounts.find((item) => item.id === job.accountId);
      if (account) {
        if (!refunded) {
          account.inFlight = Math.max(0, Number(account.inFlight || 0) - 1);
        }
        if (!image.mock) {
          completeSharedAccount(db, account, now, {
            job,
            kind: 'success',
            retryAfterUntil: reservation.upstreamTiming?.upstreamRetryAfterUntil,
          });
        }
        account.total = Number(account.total || 0) + 1;
        account.updatedAt = at;
      }
      const saved: StoreItem = {
        id: pending.imageId,
        memberId: job.fairCharge.userId,
        token: reservation.token,
        accountId: job.accountId,
        cacheKey: image.mock ? '' : reservation.cacheKey || '',
        mock: Boolean(image.mock),
        prompt: request.tag,
        fullPrompt: request.prompt,
        model: request.model,
        width: request.width,
        height: request.height,
        requestedSteps: request.requestedSteps ?? request.steps,
        routedSteps: request.steps,
        cost: reservation.cost,
        accountCost: normalizedAccountCost(reservation.accountCost),
        mimeType: image.mimeType,
        file: pending.file,
        operation: pending.operation,
        byteLength: pending.byteLength,
        sha256: pending.sha256,
        createdAt: at,
      };
      db.images.unshift(saved);
      Object.assign(job, reservation.upstreamTiming || {});
      if (!refunded) {
        settleFairQuota(db, job, true, now, {}, store.readFairQuotaSettlement(job.id));
      }
      if (refunded) {
        recordRecoveredGeneration(db, job, saved.id, now);
      } else {
        job.status = 'done';
      }
      job.imageId = saved.id;
      delete job.resultPending;
      if (!refunded) {
        job.error = '';
        job.errorDetail = '';
        job.completedAt = at;
      }
      job.updatedAt = at;
      recordCompletedArtifact(
        db,
        job,
        saved,
        now,
        store.readFreeImageUsage(db, job.fairCharge.userId, now),
      );
      if (!refunded) {
        activateNextBatchJob(db, job, store.nextQueueSequence(), now);
      }
      return {
        ...saved,
        balance: db.users.find((user) => user.token === reservation.token)?.balance || 0,
      };
    },
    {
      dirtyRows: (saved, db) => ({
        ...changedJobRows(
          db,
          [meta.jobId],
          reservation.account?.id ? [reservation.account.id] : [],
        ),
        images: [saved.id],
      }),
    },
  );
}

export interface GenerationRecoveryOutcome {
  jobId: string;
  status: 'completed' | 'file_missing' | 'unresolved';
  imageId?: string;
  code?: string;
}
export async function recoverPendingGenerationResultsCommand(
  options: GenerationResultOptions & { onRecovery?(result: GenerationRecoveryOutcome): void },
) {
  // 恢复依据已提交描述检查字节；缺失或冲突保留证据，不能猜测结果或重新派发。
  const { store, dataDir } = options;
  const result = { completed: 0, fileMissing: 0, unresolved: 0 };
  let afterId = '';
  for (;;) {
    const ids = await store.readPendingResultJobIds(afterId);
    if (!ids.length) {
      break;
    }
    for (const jobId of ids) {
      afterId = jobId;
      let outcome: GenerationRecoveryOutcome;
      try {
        const job = store.selectItemById('jobs', jobId),
          pending = job?.resultPending;
        if (!job || !pending || pending.attemptId !== job.generationAttempt?.id) {
          throw new Error('结果与执行尝试不符');
        }
        const request = job.requestId
          ? singleGenerationRequest(store.readGenerationRequest(job.requestId)!, job.sampleIndex)
          : (job.request as StoredGenerationRequest);
        if (!request || pending.operation !== (request.operation || 'generate')) {
          throw new Error('结果与请求操作不符');
        }
        const existing = store.selectItemById('images', pending.imageId);
        if (existing && (job.imageId !== pending.imageId || existing.token !== job.userToken)) {
          throw new Error('待保存结果 ID 已被使用');
        }
        const reservation: GenerationReservation = {
          job,
          token: job.userToken,
          account: job.accountId ? { id: job.accountId } : null,
          cost: job.cost || 0,
          accountCost: job.accountCost || 0,
          cacheKey: job.cacheKey || '',
          upstreamTiming: {
            upstreamStartedAt: job.upstreamStartedAt,
            upstreamHeadersAt: job.upstreamHeadersAt,
            upstreamCompletedAt: job.upstreamCompletedAt,
            upstreamHttpStatus: job.upstreamHttpStatus,
            upstreamRetryAfterUntil: job.upstreamRetryAfterUntil,
          },
        };
        await store.update((db) => completionJob(store, db, reservation, jobId, job), {
          persist: false,
        });
        const image = await (options.publish || publishStoredResult)(dataDir, pending);
        if (!image) {
          result.fileMissing++;
          outcome = { jobId, status: 'file_missing' };
        } else {
          await completeGenerationCommand(options, reservation, request, image, { jobId });
          result.completed++;
          outcome = { jobId, imageId: pending.imageId, status: 'completed' };
        }
      } catch (error) {
        result.unresolved++;
        outcome = {
          jobId,
          status: 'unresolved',
          code: (error as GenerationError).code || 'RESULT_RECOVERY_FAILED',
        };
      }
      options.onRecovery?.(outcome);
    }
  }
  return result;
}

export function sharedFailureKind(
  reservation: Pick<GenerationReservation, 'upstreamTiming' | 'failurePhase'>,
  error: unknown,
): 'ended' | 'capacity' | 'ordinary' {
  const failure = error as GenerationError;
  if (
    reservation.failurePhase === 'commit' ||
    /^(?:SQLITE|ENOSPC|EROFS|EACCES)/.test(failure?.code || '')
  ) {
    return 'ended';
  }
  const status = failure?.upstreamStatus ?? reservation.upstreamTiming?.upstreamHttpStatus;
  if (
    status !== undefined &&
    Number(status) >= 400 &&
    Number(status) < 500 &&
    Number(status) !== 429
  ) {
    return 'ended';
  }
  if (
    isNovelAiCapacityError(
      status === undefined ? error : { message: failure?.message, upstreamStatus: status },
    )
  ) {
    return 'capacity';
  }
  return reservation.failurePhase === 'upstream' && reservation.upstreamTiming?.upstreamStartedAt
    ? 'ordinary'
    : 'ended';
}

export interface GenerationFailureOptions extends GenerationClock {
  publicErrorMessage(message: unknown): string;
  errorDetailMessage(error: unknown): string;
}

async function finishGenerationFailure(
  store: JsonStore,
  reservation: GenerationReservation,
  error: unknown,
  options: GenerationFailureOptions,
  aborted: boolean,
) {
  const failure = error as GenerationError,
    detail = options.errorDetailMessage(error);
  const kind = sharedFailureKind(reservation, error),
    capacity = kind === 'capacity';
  let statusCode = 429;
  if (!capacity) {
    statusCode = failure?.statusCode || (aborted ? 504 : 500);
  }
  let message: string;
  if (!aborted && capacity) {
    message = 'NAI 暂时限制此共享账户的并发；本次失败额度已退回，后续任务将按共享冷却时间派发。';
  } else if (!aborted && failure?.upstreamStatus === 401) {
    message = '共享 Opus Key 已失效，请联系管理员更新凭据';
  } else if (!aborted && failure?.upstreamStatus === 403) {
    message = '上游拒绝访问，请联系管理员重新验证 Opus';
  } else {
    message = options.publicErrorMessage(
      failure?.message || detail || (aborted ? 'direct generate timeout' : ''),
    );
  }
  return store.update(
    (db) => {
      const job = reservation.job?.id
        ? db.jobs.find((item) => item.id === reservation.job!.id)
        : null;
      const result = {
        changed: false,
        jobId: reservation.job?.id,
        message,
        statusCode,
        kind,
        cooldownUntil: undefined as StoreItem['cooldownUntil'],
      };
      if (!job || ['done', 'failed'].includes(job.status!)) {
        return result;
      }
      const now = (options.now || Date.now)(),
        at = new Date(now).toISOString();
      const account = reservation.account
        ? db.accounts.find((item) => item.id === reservation.account!.id)
        : null;
      if (account) {
        account.inFlight = Math.max(0, Number(account.inFlight || 0) - 1);
        completeSharedAccount(db, account, now, {
          job,
          kind,
          retryAfterUntil:
            failure?.upstreamRetryAfterUntil || reservation.upstreamTiming?.upstreamRetryAfterUntil,
        });
        result.cooldownUntil = account.cooldownUntil;
        if (
          !aborted &&
          [401, 403].includes(failure?.upstreamStatus!) &&
          account.token === reservation.account?.token
        ) {
          account.upstreamStatus = failure.upstreamStatus === 401 ? 'invalid_key' : 'access_denied';
          account.quotaError =
            failure.upstreamStatus === 401
              ? '共享 Opus Key 已失效，请管理员更新凭据'
              : '上游拒绝访问，请管理员重新验证 Opus';
          recordDomainEvent(db, {
            eventType: 'QuotaObserved',
            streamId: `account:${account.id}`,
            accountId: account.id,
            occurredAt: now,
            payload: {
              kind: 'official',
              accountId: account.id,
              at: now,
              observation: {
                upstreamStatus: account.upstreamStatus,
                quotaError: account.quotaError,
                updatedAt: at,
              },
            },
          });
        }
        if (!aborted) {
          account.failures = Number(account.failures || 0) + 1;
        }
        account.updatedAt = at;
      }
      Object.assign(job, reservation.upstreamTiming || {});
      refundGenerationJob(store, db, job, {
        now: () => now,
        evidence: { requestStarted: Boolean(reservation.upstreamTiming?.upstreamStartedAt) },
      });
      cancelWaitingBatchJobs(db, job, message, aborted ? 409 : statusCode, now);
      Object.assign(job, {
        status: 'failed',
        error: message,
        errorStatus: statusCode,
        errorDetail: detail || (aborted ? 'job aborted' : ''),
        updatedAt: at,
        completedAt: at,
      });
      recordGenerationFailure(db, job);
      return { ...result, changed: true };
    },
    {
      dirtyRows: (_result, db) =>
        changedJobRows(
          db,
          reservation.job?.id ? [reservation.job.id] : [],
          reservation.account?.id ? [reservation.account.id] : [],
        ),
      shouldPersist: (result) => result.changed,
    },
  );
}

export function cancelReservedGeneration(
  store: JsonStore,
  reservation: GenerationReservation,
  error: unknown,
  options: GenerationFailureOptions,
) {
  return finishGenerationFailure(store, reservation, error, options, true);
}
export function failGenerationCommand(
  store: JsonStore,
  reservation: GenerationReservation,
  error: unknown,
  options: GenerationFailureOptions,
) {
  return finishGenerationFailure(store, reservation, error, options, false);
}

export interface GenerationCleanupOptions extends GenerationClock {
  publicErrorMessage(message: unknown): string;
  executionEvidence?(
    jobId: string,
  ): Pick<GenerationReservation, 'failurePhase' | 'upstreamTiming'> | undefined;
}

export async function cleanupStaleGenerationJobs(
  store: JsonStore,
  reason = 'stale active job cleanup',
  options: GenerationCleanupOptions,
) {
  return store.update(
    (db) => {
      const now = (options.now || Date.now)(),
        at = new Date(now).toISOString(),
        env = options.env || process.env;
      const jobIds: string[] = [],
        message = options.publicErrorMessage('direct generate timeout');
      for (const job of db.jobs) {
        if (!isStaleGenerationJob(job, now, env)) {
          continue;
        }
        const detail = staleGenerationJobDetail(job, now, env);
        const account =
          job.status === 'running' && job.accountId
            ? db.accounts.find((item) => item.id === job.accountId)
            : null;
        let statusCode = 504;
        if (account) {
          const execution = options.executionEvidence?.(job.id) || {
            failurePhase: job.upstreamStartedAt ? 'upstream' : 'preflight',
            upstreamTiming: job,
          };
          Object.assign(job, execution.upstreamTiming || {});
          account.inFlight = Math.max(0, Number(account.inFlight || 0) - 1);
          const kind = sharedFailureKind(execution, new Error('direct generate timeout'));
          completeSharedAccount(db, account, now, {
            job,
            kind,
            retryAfterUntil: job.upstreamRetryAfterUntil,
          });
          if (kind === 'capacity') {
            statusCode = 429;
          }
          account.updatedAt = at;
        }
        refundGenerationJob(store, db, job, { now: () => now });
        Object.assign(job, {
          status: 'failed',
          error: message,
          errorStatus: statusCode,
          errorDetail: `${reason}: ${detail}`,
          updatedAt: at,
          completedAt: at,
        });
        recordGenerationFailure(db, job);
        jobIds.push(job.id, ...cancelWaitingBatchJobs(db, job, message, statusCode, now));
      }
      return {
        changed: jobIds.length,
        jobIds,
        failures: db.jobs
          .filter((job) => jobIds.includes(job.id))
          .map((job) => ({
            jobId: job.id,
            statusCode: job.errorStatus,
            cooldownUntil: job.sharedCooldown?.until,
          })),
      };
    },
    {
      dirtyRows: (result, db) => changedJobRows(db, result.jobIds),
      shouldPersist: (result) => result.changed > 0,
    },
  );
}

export async function cleanupInterruptedGenerationJobs(
  store: JsonStore,
  options: GenerationCleanupOptions,
) {
  return store.update(
    (db) => {
      const now = (options.now || Date.now)(),
        at = new Date(now).toISOString(),
        env = options.env || process.env;
      const jobIds: string[] = [],
        message = options.publicErrorMessage('direct generate timeout');
      let running = 0,
        direct = 0,
        openai = 0,
        expired = 0;
      for (const job of db.jobs) {
        if (!['queued', 'pending', 'running'].includes(job.status!)) {
          continue;
        }
        const isRunning = job.status === 'running',
          deadline = generationJobDeadline(job),
          queueExpires = queuedDeadlineTimestamp(job, env);
        const isExpired = Boolean(
          (deadline && now >= deadline) || (queueExpires && now >= queueExpires),
        );
        if (!isRunning && !isExpired) {
          continue;
        }
        if (isRunning) {
          running++;
        }
        if (job.source === 'direct') {
          direct++;
        }
        if (job.source === 'openai') {
          openai++;
        }
        if (isExpired) {
          expired++;
        }
        const account =
          isRunning && job.accountId ? db.accounts.find((item) => item.id === job.accountId) : null;
        if (account) {
          account.inFlight = Math.max(0, Number(account.inFlight || 0) - 1);
          completeSharedAccount(db, account, now, {
            job,
            kind: sharedFailureKind(
              {
                failurePhase: job.upstreamStartedAt ? 'upstream' : 'preflight',
                upstreamTiming: job,
              },
              new Error('interrupted'),
            ),
            retryAfterUntil: job.upstreamRetryAfterUntil,
          });
          account.updatedAt = at;
        }
        refundGenerationJob(store, db, job, { now: () => now });
        Object.assign(job, {
          status: 'failed',
          error: message,
          errorDetail: isRunning
            ? 'startup: running job was interrupted by server restart'
            : 'startup: job deadline expired before restart completed',
          updatedAt: at,
          completedAt: at,
        });
        recordGenerationFailure(db, job);
        jobIds.push(job.id, ...cancelWaitingBatchJobs(db, job, message, 504, now));
      }
      return {
        changed: jobIds.length,
        running,
        direct,
        openai,
        expired,
        queuedRemaining: db.jobs.filter((job) => job.status === 'queued').length,
        runningRemaining: db.jobs.filter((job) => job.status === 'running').length,
        jobIds,
      };
    },
    {
      dirtyRows: (result, db) => changedJobRows(db, result.jobIds),
      shouldPersist: (result) => result.changed > 0,
    },
  );
}

export async function reconcileGenerationReviewsCommand(
  store: JsonStore,
  options: GenerationClock & { readStoredImage(image: StoreItem): Promise<Buffer> },
) {
  const now = (options.now || Date.now)();
  let changed = 0;
  for (const pending of await store.readDueGenerationReviews(now)) {
    let localImage = pending.imageId ? await store.findImage(pending.imageId) : null;
    if (localImage?.token !== pending.userToken) {
      localImage = null;
    }
    if (localImage) {
      try {
        const buffer = await options.readStoredImage(localImage);
        if (localImage.mimeType !== 'application/octet-stream') {
          validateImageBytes(buffer, { maxBytes: 64 * 1024 * 1024, maxDimension: 16384 });
        } else if (!buffer.length) {
          localImage = null;
        }
      } catch {
        localImage = null;
      }
    }
    const updated = await store.update(
      (db) => {
        const job = store.loadJobForUpdate(pending.id);
        if (
          !job ||
          job.generationReview?.status !== 'pending' ||
          job.fairCharge?.status !== 'refunded'
        ) {
          return false;
        }
        if (localImage && job.imageId === localImage.id) {
          const recovered = recordRecoveredGeneration(db, job, localImage.id, now);
          if (recovered) {
            recordFreeImageSuccess(
              db,
              job,
              now,
              store.readFreeImageUsage(db, job.fairCharge.userId, now),
            );
          }
          if (recovered && !localImage.mock) {
            completeSharedAccount(
              db,
              db.accounts.find((account) => account.id === job.accountId),
              now,
              { job, kind: 'success', retryAfterUntil: job.upstreamRetryAfterUntil },
            );
          }
          return recovered;
        }
        return closeGenerationReview(db, job, now);
      },
      {
        includeSettings: true,
        immediate: true,
        dirtyRows: (_result, db) => changedJobRows(db, [pending.id]),
        shouldPersist: Boolean,
      },
    );
    if (updated) {
      changed++;
    }
  }
  return { changed };
}
