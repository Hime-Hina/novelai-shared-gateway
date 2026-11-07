import { queuedDeadlineTimestamp, type QueueJob } from './queue-policy.ts';
import { recordDomainEvent } from '../events/domain-events.ts';

type SummaryField =
  | 'model'
  | 'width'
  | 'height'
  | 'steps'
  | 'requestedSteps'
  | 'seed'
  | 'allowPurchasedAnlas'
  | 'operation'
  | 'action';
interface BatchableRequest {
  seed?: number;
  nativeParameters?: Record<string, unknown> | null;
}
interface BatchJob extends Omit<QueueJob, 'userToken'> {
  requestId?: string;
  sampleIndex?: number;
  queueSeq?: number;
  queueTotal?: number;
}
interface BatchState {
  jobs: BatchJob[];
}

export { generationPayloadLimits, createGenerationPayloadBudget } from './payload-budget.ts';

// 队列展示和操作类型需要的小字段；完整执行参数只在父请求表保存。
export function webJobRequestSummary<T extends object>(
  request: T,
): Partial<Pick<T, Extract<keyof T, SummaryField>>> {
  const fields: SummaryField[] = [
    'model',
    'width',
    'height',
    'steps',
    'requestedSteps',
    'seed',
    'allowPurchasedAnlas',
    'operation',
    'action',
  ];
  // 只投影白名单内的已有字段，不校正调用方已验证的值。
  return Object.fromEntries(
    fields
      .filter((key) => request[key as keyof T] !== undefined)
      .map((key) => [key, request[key as keyof T]]),
  ) as Partial<Pick<T, Extract<keyof T, SummaryField>>>;
}

export function singleGenerationRequest<T extends BatchableRequest>(request: T, sampleIndex = 0) {
  if (!request.nativeParameters) {
    return request;
  }
  const seed = (request.seed! + sampleIndex) % 4294967296;
  // 字符串/参考数组只读共享；不为每个 child 深拷贝整份 base64。
  return {
    ...request,
    sampleCount: 1,
    n_samples: 1,
    seed,
    nativeParameters: { ...request.nativeParameters, n_samples: 1, seed },
  };
}

export function cancelWaitingBatchJobs(
  db: BatchState,
  job: BatchJob,
  message: string,
  statusCode = 409,
  now = Date.now(),
): string[] {
  if (!job.requestId) {
    return [];
  }
  const changed: string[] = [];
  for (const child of db.jobs) {
    if (
      child.id === job.id ||
      child.requestId !== job.requestId ||
      !['pending', 'queued'].includes(child.status!)
    ) {
      continue;
    }
    // 未派发 child 没有生成预扣；不为取消意图补造 attempt 或消费证据。
    child.status = 'failed';
    child.error = child.errorDetail = message;
    child.errorStatus = statusCode;
    child.cancelledAt = child.completedAt = child.updatedAt = new Date(now).toISOString();
    child.cancellationReason = 'batch_stopped';
    changed.push(child.id);
  }
  if (changed.length) {
    recordDomainEvent(db, {
      eventType: 'WaitingJobsCancelled',
      streamId: `request:${job.requestId}`,
      occurredAt: now,
      payload: {
        jobIds: changed,
        at: now,
        reason: message,
        errorStatus: statusCode,
        cancellationReason: 'batch_stopped',
      },
    });
  }
  return changed;
}

export function activateNextBatchJob(
  db: BatchState,
  job: BatchJob,
  queueSeq: number,
  now = Date.now(),
): void {
  if (!job.requestId || job.status !== 'done') {
    return;
  }
  const children = db.jobs.filter((child) => child.requestId === job.requestId);
  if (children.some((child) => ['failed', 'queued', 'running'].includes(child.status!))) {
    return;
  }
  const next = children
    .filter((child) => child.status === 'pending')
    .sort((a, b) => a.sampleIndex! - b.sampleIndex!)[0];
  if (!next) {
    return;
  }
  const deadline = queuedDeadlineTimestamp(next);
  if (deadline && now >= deadline) {
    cancelWaitingBatchJobs(db, job, '任务已过期，剩余图片已取消', 504, now);
    return;
  }
  next.status = 'queued';
  next.queueSeq = queueSeq;
  next.queueTotal = db.jobs.filter((child) => ['queued', 'running'].includes(child.status!)).length;
  next.updatedAt = new Date(now).toISOString();
  recordDomainEvent(db, {
    eventType: 'JobQueued',
    streamId: `request:${job.requestId}`,
    occurredAt: now,
    payload: {
      jobId: next.id,
      queueSeq,
      queuedAt: next.updatedAt,
      queueExpiresAt: next.queueExpiresAt,
      queueTotal: next.queueTotal,
    },
  });
  // queueExpiresAt 沿用首次提交期限；激活只领取新的队尾位置。
}
