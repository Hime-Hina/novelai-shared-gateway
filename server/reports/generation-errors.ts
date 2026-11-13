import {
  errorDetailMessage,
  isNovelAiCapacityError,
  publicErrorMessage,
} from '../http/public-errors.ts';
import { jobAccountCost } from '../generation/request-queries.ts';
import { quotaNumber as number } from '../quota/eligibility.ts';
import { activityDate } from './activity-heatmap.ts';

export interface ErrorLogRequest {
  tag?: string | null;
  prompt?: string | null;
  artist?: string | null;
  negative?: string | null;
  model?: string | null;
  size?: string | null;
  width?: number | null;
  height?: number | null;
  requestedSteps?: number | null;
  steps?: number | null;
  scale?: number | null;
  cfg?: number | null;
  sampler?: string | null;
  noiseSchedule?: string | null;
  seed?: number | null;
}
interface ErrorLogJob {
  id: string;
  source?: string | null;
  accountId?: string | null;
  status?: string | null;
  error?: string | null;
  errorDetail?: string | null;
  retryCount?: number | string | null;
  cost?: number | string | null;
  accountCost?: number | string | null;
  queueTotal?: number | string | null;
  createdAt?: string | null;
  updatedAt?: string | null;
  completedAt?: string | null;
}
// 管理鉴权仍由调用入口承担；筛选和限量先于原请求的冷读取。
export function selectErrorLogJobs<T extends ErrorLogJob>(
  jobs: readonly T[],
  cutoff: number,
  limit: number,
) {
  return jobs
    .filter((job) => job.status === 'failed' && job.accountId)
    .filter((job) => {
      const text = `${job.error || ''}\n${job.errorDetail || ''}`;
      return (
        !isNovelAiCapacityError({ message: text }) &&
        !/all NovelAI accounts are busy|server busy|direct generate timeout|AbortError|operation was aborted/i.test(
          text,
        ) &&
        !/job deadline expired|stale timeout|queued job exceeded|running job exceeded/i.test(
          text,
        ) &&
        !/invalid user token|invalid STA1N|密钥额度不足|用户额度不足/i.test(text)
      );
    })
    .filter((job) => {
      const at = Date.parse(job.updatedAt || job.createdAt || '');
      return Number.isFinite(at) && at >= cutoff;
    })
    .sort(
      (a, b) =>
        Date.parse(b.updatedAt || b.createdAt || '') - Date.parse(a.updatedAt || a.createdAt || ''),
    )
    .slice(0, limit);
}

export function publicErrorLog(
  job: ErrorLogJob,
  request: ErrorLogRequest | null,
  context: {
    maskedUserToken: string | null;
    accountRouteId: number | null;
    durationMs: number | null;
  },
) {
  const accountCost = number(job.accountCost);
  return {
    id: job.id,
    source: job.source ?? null,
    userToken: context.maskedUserToken,
    accountId: job.accountId ?? null,
    accountRouteId: number(context.accountRouteId),
    status: job.status ?? null,
    error: job.error == null ? null : publicErrorMessage(job.error),
    errorDetail: job.errorDetail == null ? null : errorDetailMessage(job.errorDetail),
    retryCount: number(job.retryCount),
    cost: number(job.cost),
    accountCost: accountCost === null ? null : jobAccountCost({ accountCost }),
    queueTotal: number(job.queueTotal),
    durationMs: number(context.durationMs),
    request: errorLogRequest(request),
    createdAt: job.createdAt ?? null,
    updatedAt: job.updatedAt ?? null,
    completedAt: job.completedAt ?? null,
    beijingDate: activityDate(job.updatedAt || job.createdAt),
  };
}

export function errorLogRequest(request: ErrorLogRequest | null) {
  return {
    tag: request?.tag ?? null,
    prompt: request?.prompt ?? null,
    artist: request?.artist ?? null,
    negative: request?.negative ?? null,
    model: request?.model ?? null,
    size: request?.size ?? null,
    width: number(request?.width),
    height: number(request?.height),
    requestedSteps: number(request?.requestedSteps),
    routedSteps: number(request?.steps),
    scale: number(request?.scale),
    cfg: number(request?.cfg),
    sampler: request?.sampler ?? null,
    noiseSchedule: request?.noiseSchedule ?? null,
    seed: number(request?.seed),
  };
}
