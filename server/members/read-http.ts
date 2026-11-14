import type { IncomingMessage, ServerResponse } from 'node:http';
import type {
  RelationalRequestStore,
  MemberJobPageOptions,
} from '../storage/relational-request-store.ts';
import { replyMemberJson } from './http-request.ts';
import type { fairQuotaSummary } from '../quota/fair-share.ts';
import type { FreeImageUsage } from '../quota/free-image-policy.ts';
import type { memberCharge } from '../quota/failure-accounting.ts';

// 部署配置优先，历史库配置只作原回退；成员与管理读取入口共用此顺序。
export function resolveMemberPublicBaseUrl(
  stored: string | null | undefined,
  env: NodeJS.ProcessEnv = process.env,
): string {
  return env.PUBLIC_BASE_URL || stored || (env.PUBLIC_HOST ? `https://${env.PUBLIC_HOST}` : '');
}

type HistoryPage = ReturnType<RelationalRequestStore['readMemberJobPage']>;
type ProfileJob = Omit<HistoryPage['jobs'][number], 'requestId' | 'createdAt' | 'status'> & {
  requestId?: string | null;
  createdAt?: string | null;
  status?: string;
};
export interface MemberMeState {
  username: string | undefined;
  owner: boolean;
  sessionExpiresAt: number;
  acceptingJobs: boolean;
  quota:
    (NonNullable<ReturnType<typeof fairQuotaSummary>> & { freeImages?: FreeImageUsage }) | null;
  jobs: ProfileJob[];
  lateResults: {
    id: string;
    model: string | null;
    createdAt?: string | null;
    lateResultAt: string;
    imageId: string | null;
    charge: ReturnType<typeof memberCharge>;
  }[];
}

export function publicMemberProfile(state: MemberMeState, imageUrl: (id: string) => string) {
  return {
    username: state.username,
    owner: state.owner,
    sessionExpiresAt: state.sessionExpiresAt,
    acceptingJobs: state.acceptingJobs,
    quota: state.quota,
    jobs: state.jobs.map((job) => ({
      id: job.id,
      requestId: job.requestId,
      createdAt: job.createdAt,
      lateResultAvailable: job.lateResultAvailable,
      imageId: job.imageId || '',
      model: job.model,
      status: job.status,
      charge: job.charge,
      imageUrl: job.imageId ? imageUrl(job.imageId) : '',
    })),
    lateResults: state.lateResults.map(
      ({ id, model, createdAt, lateResultAt, imageId, charge }) => ({
        id,
        model,
        createdAt,
        lateResultAt,
        imageUrl: imageId ? imageUrl(imageId) : '',
        charge,
      }),
    ),
  };
}
interface Dependencies {
  readHistory(options: MemberJobPageOptions): Promise<
    Omit<HistoryPage, 'jobs'> & {
      jobs: (Omit<HistoryPage['jobs'][number], 'requestId'> & { requestId: string | null })[];
    }
  >;
  readKey(): Promise<{ apiKey: string | undefined; publicBaseUrl: string }>;
  publicImageUrl(imageId: string): string;
  now?(): number;
}

// 解析与公开映射归同一 HTTP owner；当前授权、薄关系读取和 URL 签名仍由实际能力持有者负责。
export async function handleMemberReadRoutes(
  req: IncomingMessage,
  res: ServerResponse,
  url: URL,
  deps: Dependencies,
): Promise<boolean> {
  if (req.method !== 'GET') {
    return false;
  }
  if (url.pathname === '/api/member/key') {
    const key = await deps.readKey(),
      base = key.publicBaseUrl.replace(/\/+$/, '');
    replyMemberJson(res, 200, {
      apiKey: key.apiKey,
      baseUrl: base ? (base.endsWith('/v1') ? base : `${base}/v1`) : '',
    });
    return true;
  }
  if (url.pathname !== '/api/member/history') {
    return false;
  }
  const query = url.searchParams,
    page = Number(query.get('page') || 1),
    pageSize = Number(query.get('pageSize') || 25),
    q = (query.get('q') || '').trim(),
    status = query.get('status') || '',
    sort = query.get('sort') || 'createdAt',
    order = query.get('order') || 'desc',
    until = query.get('until') || new Date((deps.now ?? Date.now)()).toISOString();
  if (
    !Number.isSafeInteger(page) ||
    page < 1 ||
    page > 1000000 ||
    !Number.isSafeInteger(pageSize) ||
    pageSize < 1 ||
    pageSize > 100 ||
    q.length > 200 ||
    !['', 'done', 'failed', 'running', 'queued', 'pending', 'late'].includes(status) ||
    !['createdAt', 'model', 'status'].includes(sort) ||
    !['asc', 'desc'].includes(order) ||
    !Number.isFinite(Date.parse(until)) ||
    new Date(until).toISOString() !== until
  ) {
    throw Object.assign(new Error('历史查询参数无效'), { statusCode: 400 });
  }
  const result = await deps.readHistory({ page, pageSize, q, status, sort, order, until });
  replyMemberJson(res, 200, {
    ...result,
    jobs: result.jobs.map(({ imageId, ...job }) => ({
      ...job,
      imageUrl: imageId ? deps.publicImageUrl(imageId) : '',
    })),
  });
  return true;
}
