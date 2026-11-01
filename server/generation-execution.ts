import { createId, type JsonStore, type StoreDatabase, type StoreItem, type StoredGenerationRequest } from './store.ts';
import { reserveFairQuota, settleFairQuota, resolveFairQuotaCharge, syncFairQuota, quoteFairRequest, type FairCharge } from './fair-quota.ts';
import { memberGenerationCapabilities } from './generation-capabilities.ts';
import { addMemberGenerationResources, assertMemberGenerationCost, assertMemberGenerationVersions,
  memberGenerationChargeResources, parseMemberGenerationConfirmation } from './member-generation.ts';
import { recordDomainEvent, recordGenerationDiagnostic } from './domain-events.ts';
import { recordSharedCompletion, sharedAccountConcurrency, sharedAccountReadyDelay } from './shared-capacity.ts';
import { assertFreeImageAdmission, type FreeImageUsage } from './free-image-policy.ts';
import { describeStoredResult, publishStoredResult } from './result-files.ts';
import { assertMemberAcceptsJobs, queuedDeadlineTimestamp, setMemberAdmission } from './queue-policy.ts';
import { activateNextBatchJob, cancelWaitingBatchJobs, singleGenerationRequest } from './generation-request.ts';
import { closeGenerationReview, recordRecoveredGeneration } from './generation-accounting.ts';
import { validateImageBytes } from './native-image.ts';
import type { generateNovelAiImage } from './providers.ts';
import path from 'node:path';

export interface GenerationClock { now?: () => number; env?: NodeJS.ProcessEnv; fairShareMode?: boolean }
export interface GenerationTiming {
  upstreamStartedAt?: string; upstreamHeadersAt?: string; upstreamCompletedAt?: string;
  upstreamHttpStatus?: number; upstreamRetryAfterUntil?: string;
}
export interface GenerationReservation {
  job?: StoreItem; account: StoreItem | null; token?: string; userId?: string; ledgerId?: string;
  cost?: number; accountCost?: number; cacheKey?: string; upstreamTiming?: GenerationTiming; failurePhase?: string;
}
export type GenerationQueueReservation = ((GenerationReservation & { job: StoreItem; skip?: false; queued?: false; changed?: boolean; jobId?: string; failure?: string; statusCode?: number })
  | { skip: true; queued?: false; job?: undefined; jobId?: string; failure?: string; statusCode?: number; changed?: boolean }
  | { queued: true; skip?: false; job?: undefined; jobId: string; failure?: string; statusCode?: number; changed?: boolean }) & { cooldownUntil?: string };
interface GenerationError extends Error { statusCode?: number; code?: string; cooldownUntil?: string; upstreamStatus?: number; upstreamRetryAfterUntil?: string }
export type GenerationContent = (Awaited<ReturnType<typeof generateNovelAiImage>> | NonNullable<Awaited<ReturnType<typeof publishStoredResult>>>) & { mock?: boolean };
function fail(statusCode: number, message: string): never { throw Object.assign(new Error(message), { statusCode }); }
function optionalString(value: unknown, name: string): string | undefined {
  if (value === undefined || value === null) return;
  if (typeof value !== 'string') throw new TypeError(`${name} must be a string`);
  return value;
}
function requiredString(value: unknown, name: string): string {
  const result = optionalString(value, name); if (!result) throw new TypeError(`${name} is required`); return result;
}
function optionalNumber(value: unknown, name: string): number | undefined {
  if (value === undefined || value === null) return;
  if (typeof value !== 'number' || !Number.isFinite(value)) throw new TypeError(`${name} must be finite`);
  return value;
}
function requiredNumber(value: unknown, name: string): number {
  const result = optionalNumber(value, name); if (result === undefined) throw new TypeError(`${name} is required`); return result;
}
function normalizedAccountCost(value: unknown): number {
  const amount = Number(value); return Number.isFinite(amount) && amount > 0 ? Math.ceil(amount) : 0;
}

const publishingArtifacts = new Map<string, Promise<Awaited<ReturnType<typeof publishStoredResult>>>>();

export async function publishGenerationArtifact(dataDir: string, pending: NonNullable<StoreItem['resultPending']>, bytes: Buffer, publish = publishStoredResult) {
  const key = `${path.resolve(dataDir)}:${pending.attemptId}:${pending.imageId}`;
  const active = publishingArtifacts.get(key);
  if (active) return active;
  const publication = publish(dataDir, pending, bytes);
  publishingArtifacts.set(key, publication);
  try { return await publication; }
  finally { if (publishingArtifacts.get(key) === publication) publishingArtifacts.delete(key); }
}

export const generationStreamId = (job: Pick<StoreItem, 'id' | 'requestId'>): string => `request:${job.requestId || `legacy-${job.id}`}`;

export function assertConfirmedDispatch(db: StoreDatabase, member: StoreItem, request: StoredGenerationRequest,
  priorCharges: (FairCharge | null | undefined)[], finalCharge: Parameters<typeof memberGenerationChargeResources>[0], env = process.env): void {
  if (request.clientContract !== 'member-generation-v1') return;
  const confirmation = parseMemberGenerationConfirmation(request.confirmation);
  assertMemberGenerationVersions(confirmation, memberGenerationCapabilities(db, member, { env }).versions);
  const resources = memberGenerationChargeResources(finalCharge);
  const prior = priorCharges.filter((charge): charge is FairCharge => !!charge && ['reserved', 'settled'].includes(charge.status));
  assertMemberGenerationCost(confirmation, resources, addMemberGenerationResources(resources, ...prior.map(memberGenerationChargeResources)));
}

export function recordGenerationFailure(db: StoreDatabase, job: StoreItem): void {
  job.errorCode ||= job.cancelledAt ? 'request_cancelled' : 'generation_failed';
  job.errorStatus ||= 500;
  const diagnosticRef = job.errorDetail ? recordGenerationDiagnostic(db, job.id, job.errorDetail) : null;
  if (diagnosticRef) job.diagnosticRef = diagnosticRef;
  recordDomainEvent(db, { eventType: 'JobFailed', streamId: generationStreamId(job), memberId: job.fairCharge?.userId || job.memberId as string | undefined,
    accountId: job.accountId || null, occurredAt: job.completedAt || job.updatedAt || new Date().toISOString(),
    payload: { jobId: job.id, failedAt: requiredString(job.completedAt || job.updatedAt, 'failedAt'), errorStatus: job.errorStatus || 500,
      errorCode: requiredString(job.errorCode, 'errorCode'), message: job.error || '生成失败',
      cancellationReason: optionalString(job.cancellationReason, 'cancellationReason') || null, cancelledAt: job.cancelledAt || null,
      ...(diagnosticRef ? { diagnosticRef } : {}),
      charge: job.fairCharge || null, refund: job.generationRefund || null, review: job.generationReview || null } });
}

export function recordPreparedArtifact(db: StoreDatabase, job: StoreItem, now = Date.now()): void {
  const pending = job.resultPending;
  if (!pending) return;
  recordDomainEvent(db, { eventType: 'ArtifactPrepared', streamId: generationStreamId(job), occurredAt: now,
    payload: { jobId: job.id, artifact: { id: pending.imageId, ...pending } } });
}

export function recordCompletedArtifact(db: StoreDatabase, job: StoreItem, image: StoreItem, now: number, freeUsage?: FreeImageUsage): void {
  recordDomainEvent(db, { eventType: 'ArtifactPublished', streamId: generationStreamId(job), occurredAt: now,
    payload: { jobId: job.id, memberId: requiredString(job.fairCharge?.userId || job.memberId, 'artifact memberId'), accountId: job.accountId || null,
      artifact: { id: image.id, file: requiredString(image.file, 'artifact file'), mimeType: requiredString(image.mimeType, 'artifact mimeType'), byteLength: requiredNumber(image.byteLength, 'artifact byteLength'),
        sha256: requiredString(image.sha256, 'artifact sha256'), operation: optionalString(image.operation, 'artifact operation') || 'generate', r2: image.r2 || null, createdAt: requiredString(image.createdAt, 'artifact createdAt'),
        attemptId: job.generationAttempt?.id || null, storageNamespace: 'gateway-local', inputRef: job.requestId || `legacy-${job.id}`,
        model: image.model, width: image.width, height: image.height, requestedSteps: optionalNumber(image.requestedSteps, 'requestedSteps'), routedSteps: optionalNumber(image.routedSteps, 'routedSteps'),
        cacheKey: image.cacheKey || '', mock: image.mock === true, cost: image.cost || 0, accountCost: image.accountCost || 0 } } });
  if (job.status === 'done') recordDomainEvent(db, { eventType: 'JobCompleted', streamId: generationStreamId(job), occurredAt: now,
    payload: { jobId: job.id, artifactId: image.id, accountId: job.accountId || null, completedAt: job.completedAt || new Date(now).toISOString(),
      charge: job.fairCharge || null } });
  recordFreeImageSuccess(db, job, now, freeUsage);
}

export function recordFreeImageSuccess(db: StoreDatabase, job: StoreItem, now: number, usage?: FreeImageUsage): void {
  if (job.fairCharge?.resource === 'free' && (job.status === 'done' || job.lateResultAvailable)) {
    const warningThreshold = usage?.enabled && usage.warningThreshold !== null && usage.successful + 1 === usage.warningThreshold ? usage.warningThreshold : null;
    recordDomainEvent(db, { eventType: 'FreeImageSucceeded', streamId: generationStreamId(job), memberId: job.fairCharge.userId, occurredAt: now,
      payload: { jobId: job.id, memberId: job.fairCharge.userId, succeededAt: now, warningThreshold } });
  }
}

export function completeSharedAccount(db: StoreDatabase, account: StoreItem | null | undefined, now: number,
  options: NonNullable<Parameters<typeof recordSharedCompletion>[2]>): boolean {
  if (!recordSharedCompletion(account, now, options) || !account) return false;
  recordDomainEvent(db, { eventType: 'CooldownChanged', streamId: `account:${account.id}`, accountId: account.id, occurredAt: now,
    payload: { cooldownUntil: account.cooldownUntil || null, cooldownReason: optionalString(account.cooldownReason, 'cooldownReason') || null,
      ordinaryStrikeCount: Number(account.ordinaryStrikeCount || 0), capacityStrikeCount: Number(account.capacityStrikeCount || 0),
      capacityConcurrencyLimit: optionalNumber(account.capacityConcurrencyLimit, 'capacityConcurrencyLimit') || null, capacityLastErrorAt: optionalString(account.capacityLastErrorAt, 'capacityLastErrorAt') || null,
      jobId: options.job?.id || null, sharedCooldown: options.job?.sharedCooldown || null } });
  return true;
}

function changedJobRows(db: StoreDatabase, jobIds: string[], extraAccountIds: string[] = []) {
  const selected = new Set(jobIds);
  const requestIds = new Set(db.jobs.filter(job => selected.has(job.id) && job.requestId).map(job => job.requestId));
  for (const job of db.jobs) if (job.requestId && requestIds.has(job.requestId)) selected.add(job.id);
  const jobs = db.jobs.filter(job => selected.has(job.id));
  const ledger = db.ledger.filter(entry => selected.has(entry.jobId || ''));
  return { jobs: [...selected], accounts: [...new Set([...extraAccountIds, ...jobs.map(job => job.accountId)].filter((id): id is string => !!id))],
    users: db.users.filter(user => ledger.some(entry => entry.token === user.token)).map(user => user.id), ledger: ledger.map(entry => entry.id) };
}

export function refundGenerationJob(store: JsonStore, db: StoreDatabase, job: StoreItem, note: string,
  options: GenerationClock & { evidence?: Parameters<typeof settleFairQuota>[4] } = {}) {
  const now = (options.now || Date.now)();
  if (options.fairShareMode !== false) return settleFairQuota(db, job, false, now, options.evidence || {}, store.readFairQuotaSettlement(job.id));
  if (job.refundedAt || Number(job.cost || 0) <= 0) return;
  const user = db.users.find(member => member.token === job.userToken);
  if (!user) return;
  const cost = Number(job.cost), at = new Date(now).toISOString();
  user.balance! += cost; user.updatedAt = at; job.refundedAt = at;
  db.ledger.unshift({ id: createId('log'), type: 'refund', token: job.userToken, jobId: job.id, amount: cost, at, note });
}

export function generationJobDeadline(job: StoreItem): number {
  const deadline = Date.parse(job.deadlineAt || '');
  return Number.isFinite(deadline) && deadline > 0 ? deadline : 0;
}
function timeoutMs(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const configured = Number(env[name] || 0);
  return Math.max(60_000, Math.floor(Number.isFinite(configured) && configured > 0 ? configured : fallback));
}
export function isStaleGenerationJob(job: StoreItem, now = Date.now(), env = process.env): boolean {
  if (!['queued', 'pending', 'running'].includes(job.status!)) return false;
  const deadline = generationJobDeadline(job), queuedDeadline = queuedDeadlineTimestamp(job, env);
  if ((deadline && now >= deadline) || (queuedDeadline && now >= queuedDeadline)) return true;
  const updatedAt = Date.parse(job.updatedAt || job.createdAt || '');
  if (!Number.isFinite(updatedAt) || updatedAt <= 0) return false;
  const configuredInflight = Number(env.ACCOUNT_INFLIGHT_TIMEOUT_MS || 600_000);
  const inflight = Number.isFinite(configuredInflight) && configuredInflight > 0 ? Math.max(1000, Math.floor(configuredInflight)) : 600_000;
  if (job.status === 'running') return now - updatedAt > timeoutMs(env, 'STALE_RUNNING_JOB_MS', inflight + 60_000);
  return job.status === 'queued' && !deadline && now - updatedAt > timeoutMs(env, 'STALE_QUEUED_JOB_MS', timeoutMs(env, 'STALE_ACTIVE_JOB_MS', 30 * 60_000));
}
export function staleGenerationJobDetail(job: StoreItem, now = Date.now(), env = process.env): string {
  const deadline = generationJobDeadline(job), queuedDeadline = queuedDeadlineTimestamp(job, env);
  if (deadline && now >= deadline) return 'job deadline expired';
  if (queuedDeadline && now >= queuedDeadline) return 'queued job exceeded wait deadline';
  return `${job.status || 'active'} job exceeded stale timeout`;
}

export interface GenerationDispatchOptions extends GenerationClock {
  dispatchEnabled(): boolean;
  physicalAccountLoad(id: string): number;
  legacy?: { selectAccount(db: StoreDatabase, request: StoreItem['request']): StoreItem | null | undefined;
    hasEnabledAccounts(accounts: StoreItem[]): boolean; hasAccountWithEnoughQuota(accounts: StoreItem[], request: StoreItem['request']): boolean };
}

export async function reserveQueuedGeneration(store: JsonStore, jobId: string, options: GenerationDispatchOptions): Promise<GenerationQueueReservation> {
  const clock = options.now || Date.now, env = options.env || process.env, shared = options.fairShareMode !== false;
  return store.update<GenerationQueueReservation>(db => {
    if (!options.dispatchEnabled()) return { skip: true, changed: false };
    const job = db.jobs.find(item => item.id === jobId);
    if (!job) throw new Error('job not found.');
    if (job.status !== 'queued') return { skip: true, changed: false };
    const dispatchAt = clock(), at = new Date(dispatchAt).toISOString(), accountCost = normalizedAccountCost(job.accountCost);
    job.accountCost = accountCost;
    const refund = (message: string) => refundGenerationJob(store, db, job, message, { ...options, now: () => dispatchAt });
    if (isStaleGenerationJob(job, dispatchAt, env)) {
      refund('连接超时'); cancelWaitingBatchJobs(db, job, '连接超时', 504, dispatchAt);
      Object.assign(job, { status: 'failed', error: '连接超时', errorStatus: 504, errorDetail: staleGenerationJobDetail(job, dispatchAt, env), updatedAt: at, completedAt: at });
      recordGenerationFailure(db, job);
      return { skip: true, jobId: job.id, failure: job.error, statusCode: 504 };
    }
    let account: StoreItem | null | undefined;
    const state = shared ? syncFairQuota(db, dispatchAt) : null;
    try {
      const user = db.users.find(member => (job.memberId ? member.id === job.memberId : member.token === job.userToken) && member.enabled !== false);
      if (!user) fail(401, 'invalid member.');
      assertMemberAcceptsJobs(user, job.jobAdmissionVersion);
      if (shared) {
        if (!state?.enabled || state.status !== 'active') fail(503, `共享 Opus 暂不可用：${state?.status || '未配置成员'}`);
        account = db.accounts.find(item => item.id === state.boundAccountId);
        if (!account || account.enabled === false) fail(503, '绑定的 Opus 账号未启用');
        const running = db.jobs.filter(item => item.status === 'running' && item.accountId === account!.id).length;
        if (Math.max(running, Number(account.inFlight || 0), options.physicalAccountLoad(account.id)) >= sharedAccountConcurrency()
          || sharedAccountReadyDelay(account, dispatchAt) > 0) return { queued: true, jobId: job.id };
        const request = job.requestId ? singleGenerationRequest(store.readGenerationRequest(job.requestId)!, job.sampleIndex) : job.request;
        if (!request) fail(409, '持久生成请求不存在');
        let priorCharges: (FairCharge | null)[] = [];
        if (request.clientContract === 'member-generation-v1') {
          const children = store.readGenerationRequestJobIds(job.requestId!, user.id);
          if (!children) fail(409, '持久生成请求归属不一致');
          priorCharges = children.filter(id => id !== job.id).map(id => db.settings.fairQuota?.reservations[id] || store.readFairQuotaSettlement(id));
        }
        if (state.freeImagePolicy?.enabled && quoteFairRequest(request).resource === 'free') assertFreeImageAdmission(store.readFreeImageUsage(db, user.id, dispatchAt));
        reserveFairQuota(db, user, job, account, dispatchAt, request, store.readFairQuotaSettlement(job.id),
          charge => assertConfirmedDispatch(db, user, request as StoredGenerationRequest, priorCharges, charge, env));
      } else {
        if (!options.legacy) throw new Error('Legacy dispatch adapter is required.');
        account = options.legacy.selectAccount(db, job.request);
      }
    } catch (error) {
      const failure = error as GenerationError;
      refund(failure.message); cancelWaitingBatchJobs(db, job, failure.message, failure.statusCode || 503, dispatchAt);
      Object.assign(job, { status: 'failed', error: failure.message, errorStatus: failure.statusCode || 503, errorCode: failure.code || 'generation_failed',
        quotaRetryAfterUntil: failure.cooldownUntil, updatedAt: at, completedAt: at });
      recordGenerationFailure(db, job);
      return { skip: true, jobId: job.id, failure: job.error, statusCode: job.errorStatus, cooldownUntil: job.quotaRetryAfterUntil };
    }
    if (!account && options.legacy?.hasEnabledAccounts(db.accounts)) {
      if (!options.legacy.hasAccountWithEnoughQuota(db.accounts, job.request)) {
        refund('无符合本次参数的可用额度；试用限总像素1048576、28步以内');
        cancelWaitingBatchJobs(db, job, '无符合本次参数的可用额度', 503, dispatchAt);
        Object.assign(job, { status: 'failed', error: '无符合本次参数的可用额度；试用限总像素1048576、28步以内',
          errorDetail: 'No NovelAI account has available free allowance or paid balance for this request', updatedAt: at, completedAt: at });
        recordGenerationFailure(db, job); return { skip: true, jobId: job.id };
      }
      job.updatedAt = at; return { queued: true, jobId: job.id };
    }
    if (account) {
      const running = shared ? db.jobs.filter(item => item.status === 'running' && item.accountId === account!.id).length : 0;
      account.inFlight = Math.max(running, Number(account.inFlight || 0)) + 1; account.lastUsedAt = at;
      if (shared) account.lastSharedDispatchAt = at;
    }
    Object.assign(job, { status: 'running', startedAt: at, accountId: account?.id || '', completedAt: '', updatedAt: at });
    return { job, account: account ? { ...account } : null, token: job.userToken, cost: job.cost, accountCost, cacheKey: job.cacheKey || '' };
  }, { dirtyRows: (result, db) => ({ ...changedJobRows(db, [result.job?.id || result.jobId || jobId]), ledger: db.ledger.map(entry => entry.id) }),
    shouldPersist: result => result.changed !== false });
}

export async function cancelGenerationRequestCommand(store: JsonStore, requestId: string, memberId: string, options: GenerationClock = {}) {
  return store.update(db => {
    if (!db.users.some(user => user.id === memberId && user.enabled !== false)) fail(401, 'invalid member.');
    const request = store.readGenerationRequestState(requestId, memberId);
    if (!request) fail(404, 'request not found.');
    const cancelledJobIds: string[] = [], message = '请求已取消，未派发任务已终止', now = (options.now || Date.now)(), at = new Date(now).toISOString();
    for (const child of request.jobs) {
      if (!['queued', 'pending'].includes(child.status!)) continue;
      const job = store.loadJobForUpdate(child.id)!;
      refundGenerationJob(store, db, job, message, { ...options, now: () => now });
      Object.assign(job, { status: 'failed', error: message, errorDetail: message, errorStatus: 409,
        cancellationReason: 'request_cancelled', cancelledAt: at, completedAt: at, updatedAt: at });
      recordGenerationFailure(db, job); cancelledJobIds.push(job.id);
    }
    return { cancelledJobIds };
  }, { immediate: true, includeSettings: true, domainCommand: { actorId: memberId, actorKind: 'member', correlationId: requestId },
    dirtyRows: (result, db) => ({ users: [memberId], jobs: result.cancelledJobIds,
      ledger: db.ledger.filter(entry => result.cancelledJobIds.includes(entry.jobId!)).map(entry => entry.id) }) });
}

export async function changeMemberAdmissionCommand(store: JsonStore, userId: string, acceptingJobs: boolean, options: GenerationClock & { actorId?: string } = {}) {
  return store.update(db => {
    const now = (options.now || Date.now)();
    return setMemberAdmission(db, userId, acceptingJobs,
      (draft, job, message) => refundGenerationJob(store, draft, job, message, { ...options, now: () => now }), now);
  }, { immediate: true, includeSettings: true, domainCommand: { actorId: options.actorId || 'local-admin', actorKind: 'admin' },
    dirtyRows: (result, db) => ({ users: [result.id], jobs: result.cancelledJobIds,
      ledger: db.ledger.filter(entry => result.cancelledJobIds.includes(entry.jobId!)).map(entry => entry.id) }) });
}

export function completionJob(store: JsonStore, db: StoreDatabase, reservation: GenerationReservation, jobId: string,
  job = store.loadJobForUpdate(jobId)): StoreItem & { fairCharge: FairCharge } {
  if (job) job.fairCharge = resolveFairQuotaCharge(db, job, store.readFairQuotaSettlement(jobId));
  if (!job || !['reserved', 'refunded', 'settled'].includes(job.fairCharge?.status!)) fail(409, '任务结算凭据不存在');
  if (job.userToken !== reservation.token || job.accountId !== reservation.account?.id) fail(409, '生成结果与任务归属不符');
  if (job.generationAttempt?.id !== reservation.job?.generationAttempt?.id) fail(409, '生成结果与执行尝试不符');
  if (job.generationAttempt && (job.generationAttempt.jobId !== job.id || job.generationAttempt.memberId !== job.fairCharge!.userId
    || job.generationAttempt.accountId !== job.accountId)) fail(409, '任务与执行尝试归属不符');
  return job as StoreItem & { fairCharge: FairCharge };
}

export interface GenerationResultOptions extends GenerationClock {
  store: JsonStore; dataDir: string; createId?: typeof createId; publish?: typeof publishStoredResult;
  legacy?: { writeStoredImage(id: string, image: GenerationContent): Promise<string>;
    removeStoredImages(images: { id: string; file: string }[]): Promise<unknown> };
}

export async function prepareGenerationResult(store: JsonStore, reservation: GenerationReservation, request: StoredGenerationRequest,
  image: GenerationContent, jobId: string, options: Pick<GenerationResultOptions, 'now' | 'createId'> = {}) {
  return store.update(db => {
    const job = completionJob(store, db, reservation, jobId);
    if (job.imageId) {
      const existing = db.images.find(item => item.id === job.imageId) || store.selectItemById('images', job.imageId);
      if (existing) {
        if (existing.token !== job.userToken) fail(409, '已存结果与任务归属不符');
        return null;
      }
    }
    if (job.fairCharge.status === 'settled') fail(409, '任务已结算但结果缺失');
    if (!job.generationAttempt?.id) fail(409, '任务执行尝试不存在');
    const pending = { ...describeStoredResult(job.resultPending?.imageId || (options.createId || createId)('img'), request.operation || 'generate', image),
      attemptId: job.generationAttempt.id };
    if (job.resultPending && (Object.keys(pending) as (keyof typeof pending)[]).some(key => job.resultPending![key] !== pending[key])) fail(409, '待保存结果与原结果不符');
    if (store.selectItemById('images', pending.imageId)) fail(409, '待保存结果 ID 已被使用');
    Object.assign(job, reservation.upstreamTiming || {}); job.resultPending = pending;
    recordPreparedArtifact(db, job, (options.now || Date.now)());
    return pending;
  }, { dirtyRows: { jobs: [jobId] } });
}

export async function completeGenerationCommand(options: GenerationResultOptions, reservation: GenerationReservation,
  request: StoredGenerationRequest, image: GenerationContent, meta: { jobId?: string } = {}): Promise<StoreItem & { balance: number }> {
  const { store, dataDir } = options, clock = options.now || Date.now, shared = options.fairShareMode !== false;
  const pending = shared && meta.jobId ? await prepareGenerationResult(store, reservation, request, image, meta.jobId, options) : null;
  const imageId = pending?.imageId || (options.createId || createId)('img');
  let imageFile: string | null = null;
  try {
    if (pending) {
      await publishGenerationArtifact(dataDir, pending, image.buffer, options.publish || publishStoredResult);
      imageFile = pending.file;
    } else if (!shared || !meta.jobId) {
      if (!options.legacy) throw new Error('Legacy result storage adapter is required.');
      imageFile = await options.legacy.writeStoredImage(imageId, image);
    }
    const savedImage = await store.update(db => {
      const now = clock(), at = new Date(now).toISOString();
      let currentJob: ReturnType<typeof completionJob> | null = null, refunded = false;
      if (shared && meta.jobId) {
        currentJob = completionJob(store, db, reservation, meta.jobId);
        if (currentJob.imageId) {
          const existing = db.images.find(item => item.id === currentJob!.imageId) || store.selectItemById('images', currentJob.imageId);
          if (existing) {
            if (existing.token !== currentJob.userToken) fail(409, '已存结果与任务归属不符');
            if (currentJob.resultPending) {
              if (currentJob.resultPending.imageId !== existing.id || currentJob.resultPending.sha256 !== existing.sha256
                || currentJob.resultPending.byteLength !== existing.byteLength) fail(409, '已存结果与待恢复证据不符');
              delete currentJob.resultPending;
            }
            if (currentJob.fairCharge.status === 'refunded' && recordRecoveredGeneration(db, currentJob, existing.id, now)) {
              recordFreeImageSuccess(db, currentJob, now, store.readFreeImageUsage(db, currentJob.fairCharge.userId, now));
              if (!existing.mock) completeSharedAccount(db, db.accounts.find(account => account.id === currentJob!.accountId), now,
                { job: currentJob, kind: 'success', retryAfterUntil: currentJob.upstreamRetryAfterUntil });
            }
            return { ...existing, balance: db.users.find(user => user.token === reservation.token)?.balance || 0 };
          }
        }
        refunded = currentJob.fairCharge.status === 'refunded';
        if (!refunded && currentJob.fairCharge.status !== 'reserved') fail(409, '任务已结算但结果缺失');
      }
      const user = db.users.find(member => member.token === reservation.token);
      if (!currentJob && (!user || user.enabled === false)) fail(401, 'invalid user token.');
      if (pending) {
        if (currentJob!.resultPending?.attemptId !== pending.attemptId || currentJob!.resultPending?.sha256 !== pending.sha256) fail(409, '待保存结果已改变');
      } else if (!imageFile) fail(409, '已发布结果的凭据不存在');
      const account = reservation.account ? db.accounts.find(item => item.id === reservation.account!.id) : null;
      if (account) {
        if (!refunded) account.inFlight = Math.max(0, Number(account.inFlight || 0) - 1);
        if (shared && currentJob && !image.mock) completeSharedAccount(db, account, now,
          { job: currentJob, kind: 'success', retryAfterUntil: reservation.upstreamTiming?.upstreamRetryAfterUntil });
        account.total = Number(account.total || 0) + 1; account.updatedAt = at;
      }
      const saved: StoreItem = { id: imageId, memberId: currentJob?.fairCharge.userId || user?.id, token: reservation.token,
        accountId: reservation.account?.id || '', cacheKey: image.mock ? '' : reservation.cacheKey || '', mock: Boolean(image.mock),
        prompt: request.tag, fullPrompt: request.prompt, model: request.model, width: request.width, height: request.height,
        requestedSteps: request.requestedSteps ?? request.steps, routedSteps: request.steps, cost: reservation.cost,
        accountCost: normalizedAccountCost(reservation.accountCost), mimeType: image.mimeType, file: imageFile!,
        ...(pending ? { operation: pending.operation, byteLength: pending.byteLength, sha256: pending.sha256 } : {}), createdAt: at };
      db.images.unshift(saved);
      if (meta.jobId) {
        const job = db.jobs.find(item => item.id === meta.jobId);
        if (job) {
          Object.assign(job, reservation.upstreamTiming || {});
          if (shared && !refunded) settleFairQuota(db, job, true, now, {}, store.readFairQuotaSettlement(job.id));
          if (refunded) recordRecoveredGeneration(db, job, saved.id, now); else job.status = 'done';
          job.imageId = saved.id; delete job.resultPending;
          job.accountId = reservation.account?.id || job.accountId || '';
          if (!refunded) { job.error = ''; job.errorDetail = ''; job.completedAt = at; }
          job.updatedAt = at;
          if (shared) recordCompletedArtifact(db, job, saved, now, job.fairCharge ? store.readFreeImageUsage(db, job.fairCharge.userId, now) : undefined);
          if (!refunded) activateNextBatchJob(db, job, store.nextQueueSequence(), now);
        }
      }
      return { ...saved, balance: user?.balance || 0 };
    }, { dirtyRows: (saved, db) => ({ ...changedJobRows(db, meta.jobId ? [meta.jobId] : [], reservation.account?.id ? [reservation.account.id] : []), images: [saved.id] }) });
    if (imageFile && savedImage.id !== imageId && !pending) await options.legacy!.removeStoredImages([{ id: imageId, file: imageFile }]);
    return savedImage;
  } catch (error) {
    if (imageFile && !pending) await options.legacy!.removeStoredImages([{ id: imageId, file: imageFile }]);
    throw error;
  }
}

export interface GenerationRecoveryOutcome { jobId: string; status: 'completed' | 'file_missing' | 'unresolved'; imageId?: string; code?: string }
export async function recoverPendingGenerationResultsCommand(options: GenerationResultOptions & { onRecovery?(result: GenerationRecoveryOutcome): void }) {
  const { store, dataDir } = options;
  const result = { completed: 0, fileMissing: 0, unresolved: 0 };
  let afterId = '';
  for (;;) {
    const ids = await store.readPendingResultJobIds(afterId);
    if (!ids.length) break;
    for (const jobId of ids) {
      afterId = jobId;
      let outcome: GenerationRecoveryOutcome;
      try {
        const job = store.selectItemById('jobs', jobId), pending = job?.resultPending;
        if (!job || !pending || pending.attemptId !== job.generationAttempt?.id) throw new Error('结果与执行尝试不符');
        const request = job.requestId ? singleGenerationRequest(store.readGenerationRequest(job.requestId)!, job.sampleIndex) : job.request as StoredGenerationRequest;
        if (!request || pending.operation !== (request.operation || 'generate')) throw new Error('结果与请求操作不符');
        const existing = store.selectItemById('images', pending.imageId);
        if (existing && (job.imageId !== pending.imageId || existing.token !== job.userToken)) throw new Error('待保存结果 ID 已被使用');
        const reservation: GenerationReservation = { job, token: job.userToken, account: job.accountId ? { id: job.accountId } : null,
          cost: job.cost || 0, accountCost: job.accountCost || 0, cacheKey: job.cacheKey || '', upstreamTiming: {
            upstreamStartedAt: job.upstreamStartedAt, upstreamHeadersAt: job.upstreamHeadersAt, upstreamCompletedAt: job.upstreamCompletedAt,
            upstreamHttpStatus: job.upstreamHttpStatus, upstreamRetryAfterUntil: job.upstreamRetryAfterUntil } };
        await store.update(db => completionJob(store, db, reservation, jobId, job), { persist: false });
        const image = await (options.publish || publishStoredResult)(dataDir, pending);
        if (!image) { result.fileMissing++; outcome = { jobId, status: 'file_missing' }; }
        else {
          await completeGenerationCommand(options, reservation, request, image, { jobId });
          result.completed++; outcome = { jobId, imageId: pending.imageId, status: 'completed' };
        }
      } catch (error) {
        result.unresolved++; outcome = { jobId, status: 'unresolved', code: (error as GenerationError).code || 'RESULT_RECOVERY_FAILED' };
      }
      options.onRecovery?.(outcome);
    }
  }
  return result;
}

export function isNovelAiCapacityError(error: unknown): boolean {
  const failure = error as GenerationError;
  if (Number(failure?.upstreamStatus) === 429) return true;
  if ([401, 403].includes(Number(failure?.upstreamStatus))) return false;
  return /NovelAI returned 429|statusCode["']?\s*:\s*429|Concurrent generation is locked|并发生成被锁定|concurrent generation/i.test(String(failure?.message || error || ''));
}

export function sharedFailureKind(reservation: Pick<GenerationReservation, 'upstreamTiming' | 'failurePhase'>, error: unknown): 'ended' | 'capacity' | 'ordinary' {
  const failure = error as GenerationError;
  if (reservation.failurePhase === 'commit' || /^(?:SQLITE|ENOSPC|EROFS|EACCES)/.test(failure?.code || '')) return 'ended';
  const status = failure?.upstreamStatus ?? reservation.upstreamTiming?.upstreamHttpStatus;
  if (status !== undefined && Number(status) >= 400 && Number(status) < 500 && Number(status) !== 429) return 'ended';
  if (isNovelAiCapacityError(status === undefined ? error : { message: failure?.message, upstreamStatus: status })) return 'capacity';
  return reservation.failurePhase === 'upstream' && reservation.upstreamTiming?.upstreamStartedAt ? 'ordinary' : 'ended';
}

export interface GenerationFailureOptions extends GenerationClock {
  publicErrorMessage(message: unknown): string;
  errorDetailMessage(error: unknown): string;
}

async function finishGenerationFailure(store: JsonStore, reservation: GenerationReservation, error: unknown, options: GenerationFailureOptions, aborted: boolean) {
  const shared = options.fairShareMode !== false, failure = error as GenerationError, detail = options.errorDetailMessage(error);
  const kind = sharedFailureKind(reservation, error), capacity = shared && kind === 'capacity';
  const statusCode = (aborted ? kind === 'capacity' : capacity) ? 429 : failure?.statusCode || (aborted ? 504 : 500);
  const message = !aborted && capacity ? 'NAI 暂时限制此共享账户的并发；本次失败额度已退回，后续任务将按共享冷却时间派发。'
    : !aborted && shared && failure?.upstreamStatus === 401 ? '共享 Opus Key 已失效，请联系管理员更新凭据'
    : !aborted && shared && failure?.upstreamStatus === 403 ? '上游拒绝访问，请联系管理员重新验证 Opus'
    : options.publicErrorMessage(failure?.message || detail || (aborted ? 'direct generate timeout' : ''));
  return store.update(db => {
    const job = reservation.job?.id ? db.jobs.find(item => item.id === reservation.job!.id) : null;
    const result = { changed: false, jobId: reservation.job?.id, message, statusCode, kind, cooldownUntil: undefined as StoreItem['cooldownUntil'] };
    if ((aborted || (shared && reservation.job?.id)) && (!job || ['done', 'failed'].includes(job.status!))) return result;
    const now = (options.now || Date.now)(), at = new Date(now).toISOString();
    if (!aborted && !shared) {
      const user = db.users.find(member => member.token === reservation.token);
      if (user) { user.balance! += Number(reservation.cost || 0); user.updatedAt = at; }
    }
    const account = reservation.account ? db.accounts.find(item => item.id === reservation.account!.id) : null;
    if (account) {
      account.inFlight = Math.max(0, Number(account.inFlight || 0) - 1);
      if (shared) completeSharedAccount(db, account, now, { job: job || undefined, kind,
        retryAfterUntil: failure?.upstreamRetryAfterUntil || reservation.upstreamTiming?.upstreamRetryAfterUntil });
      result.cooldownUntil = account.cooldownUntil;
      if (!aborted && shared && [401, 403].includes(failure?.upstreamStatus!) && account.token === reservation.account?.token) {
        account.upstreamStatus = failure.upstreamStatus === 401 ? 'invalid_key' : 'access_denied';
        account.quotaError = failure.upstreamStatus === 401 ? '共享 Opus Key 已失效，请管理员更新凭据' : '上游拒绝访问，请管理员重新验证 Opus';
        recordDomainEvent(db, { eventType: 'QuotaObserved', streamId: `account:${account.id}`, accountId: account.id, occurredAt: now,
          payload: { kind: 'official', accountId: account.id, at: now, observation: { upstreamStatus: account.upstreamStatus, quotaError: account.quotaError, updatedAt: at } } });
      }
      if (!aborted) account.failures = Number(account.failures || 0) + 1;
      account.updatedAt = at;
    }
    if (job) {
      Object.assign(job, reservation.upstreamTiming || {});
      if (aborted || shared) refundGenerationJob(store, db, job, message, { ...options, now: () => now,
        evidence: { requestStarted: Boolean(reservation.upstreamTiming?.upstreamStartedAt) } });
      cancelWaitingBatchJobs(db, job, message, aborted ? 409 : statusCode, now);
      Object.assign(job, { status: 'failed', error: message, errorStatus: statusCode, errorDetail: detail || (aborted ? 'job aborted' : ''), updatedAt: at, completedAt: at });
      recordGenerationFailure(db, job);
    }
    if (!aborted && !shared) db.ledger.unshift({ id: createId('log'), type: 'refund', token: reservation.token,
      jobId: reservation.job?.id || '', amount: Number(reservation.cost || 0), at, note: message });
    return { ...result, changed: true };
  }, { dirtyRows: (_result, db) => changedJobRows(db, reservation.job?.id ? [reservation.job.id] : [], reservation.account?.id ? [reservation.account.id] : []),
    shouldPersist: result => result.changed });
}

export function cancelReservedGeneration(store: JsonStore, reservation: GenerationReservation, error: unknown, options: GenerationFailureOptions) {
  return finishGenerationFailure(store, reservation, error, options, true);
}
export function failGenerationCommand(store: JsonStore, reservation: GenerationReservation, error: unknown, options: GenerationFailureOptions) {
  return finishGenerationFailure(store, reservation, error, options, false);
}

export interface GenerationCleanupOptions extends GenerationClock {
  publicErrorMessage(message: unknown): string;
  executionEvidence?(jobId: string): Pick<GenerationReservation, 'failurePhase' | 'upstreamTiming'> | undefined;
}

export async function cleanupStaleGenerationJobs(store: JsonStore, reason = 'stale active job cleanup', options: GenerationCleanupOptions) {
  const shared = options.fairShareMode !== false;
  return store.update(db => {
    const now = (options.now || Date.now)(), at = new Date(now).toISOString(), env = options.env || process.env;
    const jobIds: string[] = [], message = options.publicErrorMessage('direct generate timeout');
    for (const job of db.jobs) {
      if (!isStaleGenerationJob(job, now, env)) continue;
      const detail = staleGenerationJobDetail(job, now, env);
      const account = job.status === 'running' && job.accountId ? db.accounts.find(item => item.id === job.accountId) : null;
      let statusCode = 504;
      if (account) {
        const execution = options.executionEvidence?.(job.id) || { failurePhase: job.upstreamStartedAt ? 'upstream' : 'preflight', upstreamTiming: job };
        Object.assign(job, execution.upstreamTiming || {});
        account.inFlight = Math.max(0, Number(account.inFlight || 0) - 1);
        const kind = sharedFailureKind(execution, new Error('direct generate timeout'));
        if (shared) completeSharedAccount(db, account, now, { job, kind, retryAfterUntil: job.upstreamRetryAfterUntil });
        if (shared && kind === 'capacity') statusCode = 429;
        account.updatedAt = at;
      }
      refundGenerationJob(store, db, job, message, { ...options, now: () => now });
      Object.assign(job, { status: 'failed', error: message, errorStatus: statusCode, errorDetail: `${reason}: ${detail}`, updatedAt: at, completedAt: at });
      recordGenerationFailure(db, job);
      jobIds.push(job.id, ...cancelWaitingBatchJobs(db, job, message, statusCode, now));
    }
    return { changed: jobIds.length, jobIds,
      failures: db.jobs.filter(job => jobIds.includes(job.id)).map(job => ({ jobId: job.id, statusCode: job.errorStatus, cooldownUntil: job.sharedCooldown?.until })) };
  }, { dirtyRows: (result, db) => changedJobRows(db, result.jobIds), shouldPersist: result => result.changed > 0 });
}

export async function cleanupInterruptedGenerationJobs(store: JsonStore, options: GenerationCleanupOptions) {
  const shared = options.fairShareMode !== false;
  return store.update(db => {
    const now = (options.now || Date.now)(), at = new Date(now).toISOString(), env = options.env || process.env;
    const jobIds: string[] = [], message = options.publicErrorMessage('direct generate timeout');
    let running = 0, direct = 0, openai = 0, expired = 0;
    for (const job of db.jobs) {
      if (!['queued', 'pending', 'running'].includes(job.status!)) continue;
      const isRunning = job.status === 'running', deadline = generationJobDeadline(job), queueExpires = queuedDeadlineTimestamp(job, env);
      const isExpired = Boolean((deadline && now >= deadline) || (queueExpires && now >= queueExpires));
      if (!isRunning && !isExpired) continue;
      if (isRunning) running++; if (job.source === 'direct') direct++; if (job.source === 'openai') openai++; if (isExpired) expired++;
      const account = isRunning && job.accountId ? db.accounts.find(item => item.id === job.accountId) : null;
      if (account) {
        account.inFlight = Math.max(0, Number(account.inFlight || 0) - 1);
        if (shared) completeSharedAccount(db, account, now, { job,
          kind: sharedFailureKind({ failurePhase: job.upstreamStartedAt ? 'upstream' : 'preflight', upstreamTiming: job }, new Error('interrupted')),
          retryAfterUntil: job.upstreamRetryAfterUntil });
        account.updatedAt = at;
      }
      refundGenerationJob(store, db, job, message, { ...options, now: () => now });
      Object.assign(job, { status: 'failed', error: message, errorDetail: isRunning
        ? 'startup: running job was interrupted by server restart' : 'startup: job deadline expired before restart completed', updatedAt: at, completedAt: at });
      recordGenerationFailure(db, job);
      jobIds.push(job.id, ...cancelWaitingBatchJobs(db, job, message, 504, now));
    }
    return { changed: jobIds.length, running, direct, openai, expired,
      queuedRemaining: db.jobs.filter(job => job.status === 'queued').length,
      runningRemaining: db.jobs.filter(job => job.status === 'running').length, jobIds };
  }, { dirtyRows: (result, db) => changedJobRows(db, result.jobIds), shouldPersist: result => result.changed > 0 });
}

export async function reconcileGenerationReviewsCommand(store: JsonStore,
  options: GenerationClock & { readStoredImage(image: StoreItem): Promise<Buffer> }) {
  const now = (options.now || Date.now)();
  let changed = 0;
  for (const pending of await store.readDueGenerationReviews(now)) {
    let localImage = pending.imageId ? await store.findImage(pending.imageId) : null;
    if (localImage?.token !== pending.userToken) localImage = null;
    if (localImage) {
      try {
        const buffer = await options.readStoredImage(localImage);
        if (localImage.mimeType !== 'application/octet-stream') validateImageBytes(buffer, { maxBytes: 64 * 1024 * 1024, maxDimension: 16384 });
        else if (!buffer.length) localImage = null;
      } catch { localImage = null; }
    }
    const updated = await store.update(db => {
      const job = store.loadJobForUpdate(pending.id);
      if (!job || job.generationReview?.status !== 'pending' || job.fairCharge?.status !== 'refunded') return false;
      if (localImage && job.imageId === localImage.id) {
        const recovered = recordRecoveredGeneration(db, job, localImage.id, now);
        if (recovered) recordFreeImageSuccess(db, job, now, store.readFreeImageUsage(db, job.fairCharge.userId, now));
        if (recovered && options.fairShareMode !== false && !localImage.mock) completeSharedAccount(db, db.accounts.find(account => account.id === job.accountId), now,
          { job, kind: 'success', retryAfterUntil: job.upstreamRetryAfterUntil });
        return recovered;
      }
      return closeGenerationReview(db, job, now);
    }, { includeSettings: true, immediate: true, dirtyRows: (_result, db) => changedJobRows(db, [pending.id]), shouldPersist: Boolean });
    if (updated) changed++;
  }
  return { changed };
}
