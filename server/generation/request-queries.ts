import { createHash } from 'node:crypto';
import type { JsonStore, StoreItem } from '../storage/sqlite-store.ts';
import { addMemberGenerationResources, memberGenerationChargeResources } from './member-contract.ts';
import { singleGenerationRequest } from './batch-requests.ts';

type State = NonNullable<ReturnType<JsonStore['readGenerationRequestState']>>;
type PresentedJob = Pick<StoreItem, 'id' | 'source' | 'status' | 'request' | 'accountId' | 'cost' | 'accountCost' | 'imageId' | 'lateResultAvailable' | 'error' | 'queueTotal' | 'generationProgress' | 'createdAt' | 'updatedAt' | 'completedAt'> & { requestId?: string | null };
type JobContext = { queue?: ReturnType<JsonStore['jobQueueProgress']>; accounts?: StoreItem[]; account?: StoreItem | null };

// Legacy points DTOs remain zero; actual member fees come from the shared resource quote.
export function generationCost() { return 0; }

export function jobAccountCost(job: Pick<StoreItem, 'accountCost'>): number {
  const cost = Number(job.accountCost);
  return Number.isFinite(cost) && cost > 0 ? Math.ceil(cost) : 0;
}

export function jobDurationMs(job: Pick<StoreItem, 'createdAt' | 'status' | 'completedAt' | 'updatedAt'>, now = Date.now()) {
  const started = Date.parse(job.createdAt || '');
  if (!started) return 0;
  const ended = ['done', 'failed'].includes(job.status!) ? Date.parse(job.completedAt || job.updatedAt || '') : now;
  return !ended || ended < started ? 0 : ended - started;
}

export function generationRequestForErrorLog(store: Pick<JsonStore, 'readGenerationRequest'>, job: Pick<StoreItem, 'requestId' | 'sampleIndex' | 'request'>) {
  return job.requestId ? singleGenerationRequest(store.readGenerationRequest(job.requestId) || {}, job.sampleIndex) : job.request || {};
}

export function createJobPresentation(deps: {
  store: Pick<JsonStore, 'readGenerationRequest' | 'jobQueueProgress' | 'findJobContext'>;
  imageUrl(id: string): string;
  errorMessage(message: string): string;
  progress?(job: PresentedJob): unknown;
}) {
  function publicJob(job: PresentedJob, db: JobContext | null = null, { includePrompt = true } = {}) {
    const queue = db?.queue || deps.store.jobQueueProgress(job);
    const request = (includePrompt && job.requestId ? deps.store.readGenerationRequest(job.requestId) : job.request) || {};
    const account = db?.account || (db && job.accountId ? db.accounts?.find(item => item.id === job.accountId) : null);
    return {
      id: job.id, source: job.source || 'web', requestId: job.requestId || null, status: job.status,
      prompt: request.tag || '', model: request.model || '', requestedSteps: request.requestedSteps ?? request.steps ?? 0, routedSteps: request.steps ?? 0,
      accountId: job.accountId || '', accountRouteId: account?.routeId || 0, cost: job.cost, accountCost: jobAccountCost(job),
      generationProgress: deps.progress?.(job) || job.generationProgress || {}, imageId: job.imageId || '',
      imageUrl: job.imageId ? deps.imageUrl(job.imageId) : '', lateResultAvailable: job.lateResultAvailable === true,
      error: deps.errorMessage(job.error || ''), queuePosition: queue.progress, queuedCount: queue.total, durationMs: jobDurationMs(job),
      createdAt: job.createdAt, updatedAt: job.updatedAt,
      completedAt: job.completedAt || (['done', 'failed'].includes(job.status!) ? job.updatedAt : '')
    };
  }
  async function publicJobSnapshot(jobId: string) {
    const context = await deps.store.findJobContext(jobId);
    return context ? publicJob(context.job, context, { includePrompt: false }) : null;
  }
  return { publicJob, publicJobSnapshot };
}

export function generationRequestLinks(requestId: string) {
  const root = `/api/member/requests/${encodeURIComponent(requestId)}`;
  return { status: root, events: `${root}/events`, cancel: `${root}/cancel`, input: `${root}/input` };
}

export function publicGenerationRequest(request: State, options: {
  imageUrl?(imageId: string): string;
  errorMessage?(message: string): string;
} = {}) {
  const jobs = request.jobs.map(job => ({
    id: job.id, index: job.sampleIndex, status: job.status, model: job.model || '',
    createdAt: job.createdAt || '', updatedAt: job.updatedAt || '', completedAt: job.completedAt || '',
    error: options.errorMessage?.(job.error || '') || (job.error ? '生成失败，请查看任务状态。' : ''),
    errorCode: 'errorCode' in job ? job.errorCode || null : null, cancelled: Boolean(job.cancelledAt),
    lateResultAvailable: Boolean(job.lateResultAvailable),
    charge: job.fairCharge ? { status: job.fairCharge.status, estimated: job.fairCharge.estimated,
      resources: memberGenerationChargeResources(job.fairCharge) } : null,
    imageUrl: job.imageId ? options.imageUrl?.(job.imageId) || `/api/member/jobs/${encodeURIComponent(job.id)}/content` : '',
    contentUrl: `/api/member/jobs/${encodeURIComponent(job.id)}/content`
  }));
  const counts: Record<string, number> = { total: jobs.length, queued: 0, pending: 0, running: 0, done: 0, failed: 0 };
  for (const job of jobs) if (Object.hasOwn(counts, job.status!)) counts[job.status!]++;
  const status = counts.running ? 'running' : counts.queued ? 'queued' : counts.pending ? 'pending'
    : !counts.total ? 'empty' : counts.done === counts.total ? 'done' : counts.done ? 'partial'
      : jobs.every(job => job.cancelled) ? 'cancelled' : 'failed';
  const snapshot = { id: request.id, source: request.source, operation: request.operation, createdAt: request.createdAt,
    status, cancellable: counts.queued + counts.pending > 0, counts, jobs,
    localCharges: addMemberGenerationResources(...jobs.filter(job => job.charge && ['reserved', 'settled'].includes(job.charge.status)).map(job => job.charge!.resources)) };
  // A content revision also works for imported requests without an event cursor.
  const revision = createHash('sha256').update(JSON.stringify(snapshot)).digest('hex');
  return { ...snapshot, revision, links: generationRequestLinks(request.id) };
}

export async function readOwnedGenerationRequest(store: JsonStore, requestId: string, memberId: string) {
  const { users } = await store.readCollections(['users']);
  if (!users.some(user => user.id === memberId && user.enabled !== false)) {
    throw Object.assign(new Error('登录已失效，请重新登录'), { statusCode: 401, code: 'session_expired' });
  }
  const request = store.readGenerationRequestState(requestId, memberId);
  if (!request) throw Object.assign(new Error('请求不存在'), { statusCode: 404, code: 'request_not_found' });
  return request;
}
