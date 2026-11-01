import type { StoreDatabase, StoreItem, StoredGenerationRequest } from './store.ts';
import type { FairCharge } from './fair-quota.ts';
import { previewFairQuotaCharge } from './fair-quota.ts';
import { memberGenerationCapabilities } from './generation-capabilities.ts';
import { addMemberGenerationResources, assertMemberGenerationCost, assertMemberGenerationVersions,
  memberGenerationChargeResources, parseMemberGenerationConfirmation } from './member-generation.ts';
import { recordDomainEvent } from './domain-events.ts';
import { recordSharedCompletion } from './shared-capacity.ts';
import type { FreeImageUsage } from './free-image-policy.ts';

export const generationStreamId = (job: Pick<StoreItem, 'id' | 'requestId'>): string => `request:${job.requestId || `legacy-${job.id}`}`;

export function assertConfirmedDispatch(db: StoreDatabase, member: StoreItem, request: StoredGenerationRequest,
  priorCharges: (FairCharge | null | undefined)[], now: number, env = process.env): void {
  if (request.clientContract !== 'member-generation-v1') return;
  const confirmation = parseMemberGenerationConfirmation(request.confirmation);
  assertMemberGenerationVersions(confirmation, memberGenerationCapabilities(db, member, { env }).versions);
  const resources = memberGenerationChargeResources(previewFairQuotaCharge(db, member, request, now));
  const prior = priorCharges.filter((charge): charge is FairCharge => !!charge && ['reserved', 'settled'].includes(charge.status));
  assertMemberGenerationCost(confirmation, resources, addMemberGenerationResources(resources, ...prior.map(memberGenerationChargeResources)));
}

export function recordGenerationFailure(db: StoreDatabase, job: StoreItem): void {
  recordDomainEvent(db, { eventType: 'JobFailed', streamId: generationStreamId(job), memberId: job.fairCharge?.userId || job.memberId as string | undefined,
    accountId: job.accountId || null, occurredAt: job.completedAt || job.updatedAt || new Date().toISOString(),
    payload: { jobId: job.id, failedAt: job.completedAt || job.updatedAt, errorStatus: job.errorStatus || 500,
      errorCode: job.errorCode || (job.cancelledAt ? 'request_cancelled' : 'generation_failed'), message: job.error || '生成失败',
      cancellationReason: job.cancellationReason || null, cancelledAt: job.cancelledAt || null,
      charge: job.fairCharge || null, refund: job.generationRefund || null, review: job.generationReview || null } });
}

export function recordPreparedArtifact(db: StoreDatabase, job: StoreItem): void {
  const pending = job.resultPending;
  if (!pending) return;
  recordDomainEvent(db, { eventType: 'ArtifactPrepared', streamId: generationStreamId(job), occurredAt: new Date().toISOString(),
    payload: { jobId: job.id, artifact: { id: pending.imageId, ...pending } } });
}

export function recordCompletedArtifact(db: StoreDatabase, job: StoreItem, image: StoreItem, now: number, freeUsage?: FreeImageUsage): void {
  recordDomainEvent(db, { eventType: 'ArtifactPublished', streamId: generationStreamId(job), occurredAt: now,
    payload: { jobId: job.id, memberId: job.fairCharge?.userId || job.memberId, accountId: job.accountId || null,
      artifact: { id: image.id, file: image.file, mimeType: image.mimeType, byteLength: image.byteLength,
        sha256: image.sha256, operation: image.operation || 'generate', r2: image.r2 || null, createdAt: image.createdAt,
        attemptId: job.generationAttempt?.id || null, storageNamespace: 'gateway-local', inputRef: job.requestId || `legacy-${job.id}`,
        model: image.model, width: image.width, height: image.height, requestedSteps: image.requestedSteps, routedSteps: image.routedSteps,
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
    payload: { cooldownUntil: account.cooldownUntil || null, cooldownReason: account.cooldownReason || null,
      ordinaryStrikeCount: Number(account.ordinaryStrikeCount || 0), capacityStrikeCount: Number(account.capacityStrikeCount || 0),
      capacityConcurrencyLimit: account.capacityConcurrencyLimit || null, capacityLastErrorAt: account.capacityLastErrorAt || null,
      jobId: options.job?.id || null, sharedCooldown: options.job?.sharedCooldown || null } });
  return true;
}
