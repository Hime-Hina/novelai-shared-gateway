import type { IncomingMessage, ServerResponse } from 'node:http';
import type {
  RelationalRequestStore,
  MemberJobPageOptions,
} from '../storage/relational-request-store.ts';
import { replyMemberJson } from './http-request.ts';

type HistoryPage = ReturnType<RelationalRequestStore['readMemberJobPage']>;
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
