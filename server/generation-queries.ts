import { createHash } from 'node:crypto';
import type { JsonStore } from './store.ts';
import { addMemberGenerationResources, memberGenerationChargeResources } from './member-generation.ts';

type State = NonNullable<ReturnType<JsonStore['readGenerationRequestState']>>;

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
