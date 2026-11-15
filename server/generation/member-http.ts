import type { IncomingMessage, ServerResponse } from 'node:http';
import type { EventEmitter } from 'node:events';
import { readNativeBody } from '../protocols/native-body.ts';
import { readGenerationIdempotencyKey, type GenerationPayloadClaim } from './admission.ts';
import { generationRequestLinks, type publicGenerationRequest } from './request-queries.ts';
import type { JobPreviews } from './job-previews.ts';
import type { createGenerationPayloadBudget } from './batch-requests.ts';
import { publicErrorMessage } from '../http/public-errors.ts';
import { readMemberJson } from '../members/http-request.ts';

type MemberRequest = IncomingMessage & { generationPayloadClaim?: GenerationPayloadClaim };
type Awaitable<T> = T | Promise<T>;
type State = ReturnType<typeof publicGenerationRequest>;
export interface MemberGenerationHttpDependencies {
  streamOwner: object;
  events: Pick<EventEmitter, 'on' | 'once' | 'off'>;
  budget: ReturnType<typeof createGenerationPayloadBudget>;
  previews: JobPreviews;
  capabilities(memberId: string): Awaitable<unknown>;
  quote(body: unknown, memberId: string, claim: GenerationPayloadClaim): Awaitable<unknown>;
  submit(
    req: IncomingMessage,
    body: unknown,
    memberId: string,
    claim: GenerationPayloadClaim,
  ): Promise<{
    requestId: string;
    jobIds: string[];
    replayed: boolean;
  }>;
  readState(requestId: string, memberId: string): Awaitable<State>;
  cancel(requestId: string, memberId: string): Promise<unknown>;
  readInput(requestId: string, memberId: string): Awaitable<unknown>;
  readAsset(
    requestId: string,
    memberId: string,
    slot: string,
  ): Awaitable<{ bytes: Buffer; mimeType: string }>;
  readContent(jobId: string, memberId: string): Promise<{ bytes: Buffer; mimeType: string }>;
  scheduleQueueDrain(): void;
}
const streamCounts = new Map<string, number>();
const storeStreams = new WeakMap<object, Set<() => void>>();
const closingStores = new WeakSet<object>();
export function closeMemberGenerationStreams(store: object): void {
  closingStores.add(store);
  for (const close of storeStreams.get(store) || []) {
    close();
  }
}
function fail(statusCode: number, code: string, message: string): never {
  throw Object.assign(new Error(message), { statusCode, code });
}
function reply(res: ServerResponse, status: number, value: unknown) {
  const payload = JSON.stringify(value);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(payload),
    'cache-control': 'private, no-store',
    'x-content-type-options': 'nosniff',
  });
  res.end(payload);
}
async function jsonBody(req: MemberRequest) {
  const chunks: Buffer[] = [];
  let size = 0;
  try {
    for await (const raw of req) {
      const chunk = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
      size += chunk.length;
      if (size > 32 * 1024 * 1024) {
        fail(413, 'payload_too_large', '生成请求超过 32 MiB。');
      }
      req.generationPayloadClaim?.resize(size);
      chunks.push(chunk);
    }
  } catch (error) {
    if ((error as { statusCode?: number }).statusCode) {
      throw error;
    }
    fail(400, 'invalid_body', '生成请求正文未完整接收。');
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8')) as unknown;
  } catch {
    fail(400, 'invalid_body', 'JSON 格式错误。');
  }
}

const errors: Record<number, string> = {
  400: 'invalid_body',
  401: 'session_expired',
  403: 'membership_required',
  402: 'insufficient_balance',
  404: 'request_not_found',
  405: 'method_not_allowed',
  409: 'quote_changed',
  410: 'result_unavailable',
  413: 'payload_too_large',
  415: 'unsupported_media_type',
  422: 'invalid_parameters',
  429: 'queue_full',
  503: 'upstream_unavailable',
};
export function memberGenerationError(error: unknown) {
  const source = error as Error & {
    statusCode?: number;
    status?: number;
    code?: string;
    requestId?: string;
    details?: Record<string, unknown>;
  };
  const statusCode = source.statusCode || source.status || 500;
  const safeStatus = Object.hasOwn(errors, statusCode) ? statusCode : 500;
  const code =
    source.code && /^[a-z][a-z0-9_]{1,63}$/.test(source.code)
      ? source.code
      : errors[safeStatus] || 'internal_error';
  const details = Object.fromEntries(
    Object.entries(source.details || {}).filter(
      ([key, value]) =>
        ['field', 'resource', 'limit', 'requote'].includes(key) &&
        ['string', 'number', 'boolean'].includes(typeof value),
    ),
  );
  if (source.requestId) {
    details.requestId = source.requestId;
  }
  return {
    statusCode: safeStatus,
    code,
    message: safeStatus === 500 ? '服务暂时无法完成请求。' : publicErrorMessage(source.message),
    ...(Object.keys(details).length ? { details } : {}),
  };
}

const requestRoute =
  /^\/api\/member\/requests\/([^/]+)(?:\/(cancel|events|input|inputs\/([^/]+)))?$/;
const contentRoute = /^\/api\/member\/jobs\/([^/]+)\/content$/;
export function isMemberGenerationPath(path: string): boolean {
  return (
    path.startsWith('/api/member/generation/') || requestRoute.test(path) || contentRoute.test(path)
  );
}

export function createMemberGenerationHttp(deps: MemberGenerationHttpDependencies) {
  const readState = (requestId: string, memberId: string) =>
    Promise.resolve(deps.readState(requestId, memberId));
  return async function handle(
    req: IncomingMessage,
    res: ServerResponse,
    url: URL,
    user: { id: string },
    revalidate: () => Promise<boolean>,
  ): Promise<boolean> {
    const base = '/api/member/generation',
      path = url.pathname;
    const requestPath = path.match(requestRoute);
    const contentPath = path.match(contentRoute);
    if (!path.startsWith(base + '/') && !requestPath && !contentPath) {
      return false;
    }
    try {
      if (path === base + '/capabilities' && req.method === 'GET') {
        const result = await deps.capabilities(user.id);
        reply(res, 200, result);
        return true;
      }
      if ([base + '/quote', base + '/requests'].includes(path) && req.method === 'POST') {
        const submit = path.endsWith('/requests');
        if (submit) {
          readGenerationIdempotencyKey(req, true);
        }
        const claim = deps.budget.claim();
        (req as MemberRequest).generationPayloadClaim = claim;
        try {
          const body = await readNativeBody(req as MemberRequest, {
            jsonReader: (source) => jsonBody(source as MemberRequest),
            maxBytes: 32 * 1024 * 1024,
          });
          if (!submit) {
            const result = await deps.quote(body, user.id, claim);
            reply(res, 200, result);
            return true;
          }
          const result = await deps.submit(req, body, user.id, claim);
          deps.scheduleQueueDrain();
          const requestId = result.requestId!,
            state = await readState(requestId, user.id),
            links = generationRequestLinks(requestId);
          res.setHeader('Location', links.status);
          res.setHeader('X-Nai-Request-Id', requestId);
          reply(res, 202, {
            contractVersion: 1,
            requestId,
            jobIds: result.jobIds,
            status: state.status,
            replayed: Boolean(result.replayed),
            links,
          });
          return true;
        } finally {
          claim.release();
          delete (req as MemberRequest).generationPayloadClaim;
        }
      }
      if (requestPath && requestPath[2] === 'cancel' && req.method === 'POST') {
        await readMemberJson(req);
        reply(res, 200, await deps.cancel(decodeURIComponent(requestPath[1]), user.id));
        return true;
      }
      if (requestPath && req.method === 'GET' && requestPath[2] !== 'cancel') {
        const requestId = decodeURIComponent(requestPath[1]),
          mode = requestPath[2];
        if (!mode) {
          reply(res, 200, await readState(requestId, user.id));
          return true;
        }
        if (mode === 'events') {
          await readState(requestId, user.id);
          await streamRequest(deps, res, requestId, user.id, revalidate, () =>
            readState(requestId, user.id),
          );
          return true;
        }
        if (mode === 'input') {
          reply(res, 200, await deps.readInput(requestId, user.id));
          return true;
        }
        const asset = await deps.readAsset(requestId, user.id, decodeURIComponent(requestPath[3]));
        res.writeHead(200, {
          'content-type': asset.mimeType,
          'content-length': asset.bytes.length,
          'cache-control': 'private, no-store',
          'x-content-type-options': 'nosniff',
        });
        res.end(asset.bytes);
        return true;
      }
      if (contentPath && req.method === 'GET') {
        const { bytes, mimeType } = await deps.readContent(
          decodeURIComponent(contentPath[1]),
          user.id,
        );
        res.writeHead(200, {
          'content-type': mimeType,
          'content-length': bytes.length,
          'cache-control': 'private, no-store',
          'x-content-type-options': 'nosniff',
        });
        res.end(bytes);
        return true;
      }
      fail(405, 'method_not_allowed', '此生成接口不支持该方法。');
    } catch (error) {
      if (res.headersSent) {
        res.end();
        return true;
      }
      const payload = memberGenerationError(error);
      reply(res, payload.statusCode, payload);
      return true;
    }
  };
}

async function streamRequest(
  deps: MemberGenerationHttpDependencies,
  res: ServerResponse,
  requestId: string,
  memberId: string,
  revalidate: () => Promise<boolean>,
  snapshot: () => Promise<ReturnType<typeof publicGenerationRequest>>,
) {
  // Authentication/ownership reads above may finish after the client already left.
  if (res.destroyed || res.writableEnded) {
    return;
  }
  if (closingStores.has(deps.streamOwner)) {
    fail(503, 'executor_closing', '服务正在关闭，请稍后重试。');
  }
  if (
    (streamCounts.get(memberId) || 0) >= 3 ||
    [...streamCounts.values()].reduce((sum, count) => sum + count, 0) >= 100
  ) {
    fail(429, 'stream_limit', '预览连接过多，请关闭多余页面。');
  }
  streamCounts.set(memberId, (streamCounts.get(memberId) || 0) + 1);
  let closed = false,
    running = false,
    requested = false,
    first = true,
    previous = '';
  const watched = new Map<string, () => void>();
  const streams = storeStreams.get(deps.streamOwner) || new Set<() => void>();
  storeStreams.set(deps.streamOwner, streams);
  const close = () => {
    if (!closed) {
      res.end();
      stop();
    }
  };
  // 连接只拥有订阅和预览；关闭时释放这些资源，已受理任务仍由执行器继续处理。
  const stop = () => {
    if (closed) {
      return;
    }
    closed = true;
    clearInterval(heartbeat);
    deps.events.off('commit', wake);
    deps.events.off('close', close);
    streams.delete(close);
    for (const unwatch of watched.values()) {
      unwatch();
    }
    watched.clear();
    res.off('close', stop);
    res.off('error', stop);
    res.off('drain', wake);
    const count = (streamCounts.get(memberId) || 1) - 1;
    if (count) {
      streamCounts.set(memberId, count);
    } else {
      streamCounts.delete(memberId);
    }
  };
  const send = (event: string, data: unknown) => {
    if (closed || res.destroyed || res.writableEnded) {
      return;
    }
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
    if (res.writableLength > 1024 * 1024) {
      res.end();
      stop();
    }
  };
  const wake = () => {
    requested = true;
    if (!running && !closed) {
      void tick();
    }
  };
  const tick = async () => {
    running = true;
    try {
      while (requested && !closed) {
        requested = false;
        const state = await snapshot();
        if (closed) {
          return;
        }
        if (first || state.revision !== previous) {
          send(first ? 'snapshot' : 'state', state);
          first = false;
          previous = state.revision;
        }
        for (const job of state.jobs) {
          if (!watched.has(job.id) && job.status === 'running') {
            watched.set(
              job.id,
              deps.previews.watch(job.id, (frame) => {
                if (!closed && !res.writableNeedDrain && frame.buffer.length <= 512 * 1024) {
                  send('preview', {
                    jobId: frame.jobId,
                    progress: frame.progress,
                    preview: `data:${frame.mimeType};base64,${frame.buffer.toString('base64')}`,
                  });
                }
              }),
            );
          }
        }
        if (!['queued', 'pending', 'running'].includes(state.status)) {
          send('complete', state);
          res.end();
          stop();
        }
      }
    } catch (error) {
      if (!closed) {
        send(
          [401, 403].includes((error as { statusCode?: number }).statusCode || 0)
            ? 'auth-expired'
            : 'error',
          memberGenerationError(error),
        );
        res.end();
        stop();
      }
    } finally {
      running = false;
    }
  };
  const heartbeat = setInterval(() => {
    void revalidate()
      .then((valid) => {
        if (closed) {
          return;
        }
        if (!valid) {
          send('auth-expired', {});
          res.end();
          stop();
        } else {
          if (!res.writableNeedDrain) {
            res.write(': keepalive\n\n');
          }
          wake();
        }
      })
      .catch((error) => {
        if (!closed) {
          send(
            [401, 403].includes((error as { statusCode?: number }).statusCode || 0)
              ? 'auth-expired'
              : 'error',
            memberGenerationError(error),
          );
          res.end();
          stop();
        }
      });
  }, 15_000);
  heartbeat.unref();
  streams.add(close);
  deps.events.once('close', close);
  res.once('close', stop);
  res.once('error', stop);
  res.on('drain', wake);
  // Subscribe before the first asynchronous read: a commit during it requests a second read.
  deps.events.on('commit', wake);
  res.writeHead(200, {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-cache, no-transform',
    'x-accel-buffering': 'no',
  });
  res.flushHeaders?.();
  wake();
}
