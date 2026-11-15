import type { IncomingMessage, ServerResponse } from 'node:http';
import { buildErrorImage } from './novelai-client.ts';
import { publicErrorMessage } from '../http/public-errors.ts';
import type { GenerationPayloadClaim } from '../generation/admission.ts';

export interface DirectHttpRequest extends IncomingMessage {
  generationPayloadClaim?: GenerationPayloadClaim;
  generationIdempotencyKey?: string | null;
}

// 只分派既有客户端协议；认证、受理和读取分别交回实际 owner。
export function createDirectHttp<Request extends IncomingMessage>(deps: {
  readJson(req: Request): Promise<Record<string, unknown>>;
  acceptJob(req: Request, url: URL, body: Record<string, unknown>): Promise<unknown>;
  generate(req: Request, res: ServerResponse, url: URL): Promise<void>;
  subscribe(req: Request, res: ServerResponse, url: URL): Promise<void>;
  readJob(req: Request, url: URL, jobId: string): Promise<unknown>;
  readJobContent(req: Request, res: ServerResponse, url: URL, jobId: string): Promise<void>;
  readImageContent(req: Request, res: ServerResponse, url: URL, imageId: string): Promise<void>;
  sendJson(res: ServerResponse, status: number, value: unknown): void;
}) {
  return async (req: Request, res: ServerResponse, url: URL): Promise<boolean> => {
    if (req.method === 'POST' && ['/api/jobs', '/api/web/jobs'].includes(url.pathname)) {
      const body = await deps.readJson(req);
      if (body.edit) {
        throw Object.assign(new Error('图片编辑功能已移除，请刷新页面后重试。'), {
          statusCode: 400,
        });
      }
      deps.sendJson(res, 202, await deps.acceptJob(req, url, body));
      return true;
    }
    if (req.method !== 'GET') {
      return false;
    }
    if (url.pathname === '/generate') {
      await deps.generate(req, res, url);
      return true;
    }
    if (url.pathname === '/api/jobs/events') {
      await deps.subscribe(req, res, url);
      return true;
    }
    if (/^\/api\/jobs\/[^/]+\/content$/.test(url.pathname)) {
      await deps.readJobContent(
        req,
        res,
        url,
        decodeURIComponent(url.pathname.split('/').at(-2) || ''),
      );
      return true;
    }
    if (url.pathname.startsWith('/api/jobs/')) {
      deps.sendJson(
        res,
        200,
        await deps.readJob(req, url, decodeURIComponent(url.pathname.split('/').pop() || '')),
      );
      return true;
    }
    if (url.pathname.startsWith('/api/images/')) {
      await deps.readImageContent(
        req,
        res,
        url,
        decodeURIComponent(url.pathname.split('/').at(-2) || ''),
      );
      return true;
    }
    return false;
  };
}

export function directRequestErrorImage(error: unknown, publicMode: boolean) {
  const source = error as Error & { statusCode?: number };
  if (publicMode && [401, 403].includes(source.statusCode!)) {
    return null;
  }
  return {
    ...buildErrorImage(publicErrorMessage(source.message || 'Generation failed')),
    statusCode: 200,
    headers: { 'x-error': '1' },
  };
}
