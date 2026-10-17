import type { FairCharge, FairRequest, QuotaSnapshot, ResourceAmounts } from './fair-quota.ts';

interface LedgerDatabase { ledger?: unknown[] }
interface UpstreamCandidate { resources: Partial<ResourceAmounts>; amountSource: string }
export interface GenerationAttempt {
  id: string; jobId: string; batchId: string | null; memberId: string; accountId: string; sampleIndex: number | null;
  operation: string; model: string | null; source: string | null; configVersion: number; reservedAt: number;
  localReserved: ResourceAmounts; upstreamCandidates: UpstreamCandidate[]; candidateMode: string; sharedV5Reserved: number;
  evidenceOrigin: string; dispatchIntentAt: number | null; snapshot: QuotaSnapshot | null;
}
interface GenerationRefund { eventId: string; refundedAt: number; lossEventId: string | null }
interface GenerationReview {
  status: 'pending' | 'estimated_closed' | 'result_recovered'; firstUnknownAt: number; deadlineAt: number;
  lossEventId: string; closedAt: number | null; closureEventId: string | null; resultImageId?: string;
}
export interface AccountingJob {
  id: string; status?: string; batchId?: string; sampleIndex?: number; source?: string; userToken?: string;
  request?: FairRequest; requestPayload?: FairRequest; fairCharge?: FairCharge; generationAttempt?: GenerationAttempt;
  generationRefund?: GenerationRefund; generationReview?: GenerationReview;
  upstreamStartedAt?: string; upstreamHeadersAt?: string; upstreamCompletedAt?: string; upstreamHttpStatus?: number;
  lateResultAvailable?: boolean; lateResultAt?: string; updatedAt?: string;
}
export interface RefundEvidence { requestStarted?: boolean }

// 共享模式不自动重发原任务：一个任务目前只有一次上游尝试。
// 本模块只追加证据；钱包变更仍由 fair-quota 在同一个存储事务中执行。
export const GENERATION_REVIEW_MS = 5 * 60 * 1000;
function localResources(charge: FairCharge): ResourceAmounts {
  return {
    v5: charge.resource === 'v5' ? charge.amount : 0,
    anlasFixed: charge.resource === 'anlas' ? charge.fixed : 0,
    anlasPurchased: charge.resource === 'anlas' ? charge.purchased : 0
  };
}

function upstreamCandidates(charge: FairCharge): UpstreamCandidate[] {
  const anlas = { resources: { anlasFixed: charge.fixed, anlasPurchased: charge.purchased }, amountSource: 'quote' };
  if (charge.billingMode === 'anlas_fallback') {
    // 两种计费路径是候选，不可相加，也不能据本地 Anlas 预扣断言上游用了哪一池。
    return [{ resources: { v5: charge.v5Equivalent }, amountSource: 'quote' }, anlas];
  }
  if (charge.resource === 'v5') return [{ resources: { v5: charge.amount }, amountSource: 'quote' }];
  return charge.resource === 'anlas' ? [anlas] : [];
}

function appendEvent<T extends Record<string, unknown>>(db: LedgerDatabase, attempt: GenerationAttempt, type: string, now: number, details: T) {
  const event = {
    id: `${attempt.id}:${type}`, type, at: new Date(now).toISOString(),
    attemptId: attempt.id, jobId: attempt.jobId, batchId: attempt.batchId,
    memberId: attempt.memberId, accountId: attempt.accountId,
    ...details
  };
  // 独立 JSON 副本防止后续任务/预扣状态变化覆写旧证据；不记录认证令牌和生成内容。
  (db.ledger ||= []).unshift(structuredClone(event));
  return event;
}

export function recordGenerationAttempt(db: LedgerDatabase, job: AccountingJob, charge: FairCharge, snapshot: QuotaSnapshot | null, now: number): GenerationAttempt {
  if (job.generationAttempt) return job.generationAttempt;
  const request = job.request || job.requestPayload || {};
  const attempt: GenerationAttempt = {
    id: `attempt:${job.id}:1`, jobId: job.id, batchId: job.batchId || null,
    memberId: charge.userId, accountId: charge.accountId, sampleIndex: job.sampleIndex ?? null,
    operation: request.operation || request.action || 'generate', model: request.model || null,
    source: job.source || null, configVersion: charge.configVersion,
    reservedAt: charge.reservedAt, localReserved: localResources(charge),
    upstreamCandidates: upstreamCandidates(charge), candidateMode: 'alternatives',
    sharedV5Reserved: charge.sharedV5Reserved || 0,
    // 旧版残留预扣不能补造派发前快照；派发意图也不等于请求已到达 NAI。
    evidenceOrigin: snapshot ? 'dispatch_intent' : 'legacy_reservation',
    dispatchIntentAt: snapshot ? now : null, snapshot: snapshot ? structuredClone(snapshot) : null
  };
  job.generationAttempt = attempt;
  charge.attemptId = attempt.id;
  appendEvent(db, attempt, 'generation_reserved', now, { attempt });
  return attempt;
}

export function recordGenerationRefund(db: LedgerDatabase, job: AccountingJob, charge: FairCharge, now: number, evidence: RefundEvidence = {}) {
  const attempt = job.generationAttempt || recordGenerationAttempt(db, job, charge, null, now);
  // 活进程能明确证明尚未进入 fetch；重启后仅有 running/派发意图时则必须保留不确定性。
  const dispatchState = job.upstreamStartedAt ? 'request_started'
    : evidence.requestStarted === false || ['queued', 'pending'].includes(job.status as string) ? 'not_sent' : 'unknown';
  const response = {
    dispatchState, upstreamStartedAt: job.upstreamStartedAt || null,
    upstreamHeadersAt: job.upstreamHeadersAt || null, upstreamCompletedAt: job.upstreamCompletedAt || null,
    upstreamHttpStatus: job.upstreamHttpStatus ?? null
  };
  const refund = appendEvent(db, attempt, 'generation_refunded', now, {
    resources: localResources(charge), refundedAt: now, evidenceLevel: 'local_refund', response
  });
  job.generationRefund = { eventId: refund.id, refundedAt: now, lossEventId: null };
  if (dispatchState === 'not_sent' || !attempt.upstreamCandidates.length) return;
  const loss = appendEvent(db, attempt, 'generation_loss_suspected', now, {
    refundEventId: refund.id, firstUnknownAt: now, evidenceLevel: 'unconfirmed',
    // 候选报价不是实际扣费；未知金额明确为空，后续核对只追加调整记录。
    actualConsumed: null, upstreamCandidates: attempt.upstreamCandidates, candidateMode: 'alternatives', response
  });
  job.generationRefund.lossEventId = loss.id;
  job.generationReview = { status: 'pending', firstUnknownAt: now, deadlineAt: now + GENERATION_REVIEW_MS,
    lossEventId: loss.id, closedAt: null, closureEventId: null };
}

export function closeGenerationReview(db: LedgerDatabase, job: AccountingJob, now = Date.now(), reason = 'deadline'): boolean {
  const review = job.generationReview;
  if (!review || review.status !== 'pending' || job.fairCharge?.status !== 'refunded') return false;
  if (reason === 'deadline' && (job.status !== 'failed' || now < review.deadlineAt)) return false;
  const attempt = job.generationAttempt || recordGenerationAttempt(db, job, job.fairCharge, null, now);
  const candidates = attempt.upstreamCandidates;
  const usable = Array.isArray(candidates) && candidates.length > 0 && candidates.every(candidate =>
    candidate.resources && Object.values(candidate.resources).length > 0 && Object.entries(candidate.resources).every(([resource, amount]) =>
      ['v5', 'anlasFixed', 'anlasPurchased'].includes(resource) && typeof amount === 'number' && Number.isFinite(amount) && amount >= 0)
    && Object.values(candidate.resources).some(amount => amount > 0));
  const event = appendEvent(db, attempt, 'generation_loss_estimated', now, {
    lossEventId: review.lossEventId, firstUnknownAt: review.firstUnknownAt, deadlineAt: review.deadlineAt,
    closedAt: now, reason, evidenceLevel: 'unconfirmed_estimate', actualConsumed: null,
    estimateAmountStatus: usable ? 'quoted_candidates' : 'unknown',
    upstreamCandidates: usable ? candidates : null, candidateMode: 'alternatives',
    // 按尝试记录的运营风险可能已包含在聚合缺口里；不相加作实耗，也不再次核销运营资金。
    liability: 'operator_pending_attribution', accountingScope: 'operator_attempt_estimate_nonadditive',
    aggregateSettlement: 'unmatched'
  });
  Object.assign(review, { status: 'estimated_closed', closedAt: now, closureEventId: event.id });
  return true;
}

export function recordRecoveredGeneration(db: LedgerDatabase, job: AccountingJob, imageId: string, now = Date.now()): boolean {
  if (job.lateResultAvailable) return false;
  closeGenerationReview(db, job, now, 'result_recovered');
  const attempt = job.generationAttempt || recordGenerationAttempt(db, job, job.fairCharge!, null, now);
  appendEvent(db, attempt, 'generation_result_recovered', now, {
    imageId, lossEventId: job.generationRefund?.lossEventId || null,
    closureEventId: job.generationReview?.closureEventId || null,
    refundRetained: true, actualConsumed: null, evidenceLevel: 'complete_local_result',
    // 完整图片证明执行完成，但不能从图片倒推出 NAI 单次实际扣费。
    chargeEvidence: 'unconfirmed'
  });
  job.lateResultAvailable = true; job.lateResultAt = new Date(now).toISOString();
  job.updatedAt = job.lateResultAt;
  if (job.generationReview) Object.assign(job.generationReview, { status: 'result_recovered', resultImageId: imageId });
  return true;
}
