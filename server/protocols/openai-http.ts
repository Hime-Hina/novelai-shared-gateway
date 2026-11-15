import type { IncomingMessage, ServerResponse } from 'node:http';
import type { GenerationDeliveryResult } from '../generation/delivery.ts';
import { isTimeoutResultMessage, publicErrorMessage } from '../http/public-errors.ts';
import { openAiModelsResponse } from './openai-models.ts';
import type { GenerationPayloadClaim } from '../generation/admission.ts';

export interface OpenAiHttpRequest extends IncomingMessage {
  generationPayloadClaim?: GenerationPayloadClaim;
  generationIdempotencyKey?: string | null;
}

export interface OpenAiAcceptedJob {
  id: string;
  requestId?: string | null;
  model: string;
  stream: boolean;
}
type Response = ServerResponse & { generationDeliveryTracked?: boolean };
type SavedImage = NonNullable<GenerationDeliveryResult['saved']>;

export function createOpenAiHttp<Request extends IncomingMessage>(deps: {
  accept(req: Request): Promise<OpenAiAcceptedJob>;
  waitForJobResult(
    jobId: string,
    deadline: number,
    response?: Response,
  ): Promise<GenerationDeliveryResult | null>;
  stream(req: Request, res: Response, job: OpenAiAcceptedJob, deadline: number): Promise<void>;
  imageMarkdown(req: Request, image: SavedImage): string | Promise<string>;
  sendJson(res: Response, status: number, value: unknown): void;
  timeoutMs: number;
  now?(): number;
}) {
  const now = deps.now ?? Date.now;
  return async (req: Request, res: Response, url: URL): Promise<boolean> => {
    if (req.method === 'GET' && url.pathname === '/v1/models') {
      deps.sendJson(res, 200, openAiModelsResponse());
      return true;
    }
    if (req.method !== 'POST' || url.pathname !== '/v1/chat/completions') {
      return false;
    }
    const job = await deps.accept(req);
    if (job.requestId) {
      res.setHeader('X-Nai-Request-Id', job.requestId);
    }
    if (res.destroyed) {
      return true;
    }
    const deadline = now() + deps.timeoutMs;
    if (job.stream) {
      await deps.stream(req, res, job, deadline);
      return true;
    }
    const result = await deps.waitForJobResult(job.id, deadline, res);
    if (res.destroyed) {
      return true;
    }
    if (!result) {
      throw Object.assign(new Error('direct generate timeout'), { statusCode: 504 });
    }
    if (result.error) {
      throw Object.assign(new Error(result.error), {
        statusCode: result.statusCode || (isTimeoutResultMessage(result.error) ? 504 : 500),
        cooldownUntil: result.cooldownUntil,
      });
    }
    const content = await deps.imageMarkdown(req, result.saved!);
    if (!res.destroyed) {
      deps.sendJson(
        res,
        200,
        openAiChatCompletionResponse({ model: job.model, content, id: `chatcmpl-${job.id}` }),
      );
    }
    return true;
  };
}

function openAiChatCompletionResponse({
  model,
  content,
  id,
}: {
  model: string;
  content: string;
  id: string;
}) {
  return {
    id,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{ index: 0, message: { role: 'assistant', content }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
  };
}

export function openAiGenerationError(error: unknown) {
  const source = error as Error & { statusCode?: number };
  const statusCode = source?.statusCode || 500,
    code = Number(statusCode);
  let type = 'invalid_request_error';
  if (code === 401 || code === 403) {
    type = 'authentication_error';
  } else if (code === 429) {
    type = 'rate_limit_error';
  } else if (code >= 500) {
    type = 'server_error';
  }
  return {
    statusCode,
    body: {
      error: {
        message: publicErrorMessage(source?.message || 'Internal server error'),
        type,
        param: null,
        code: type,
      },
    },
  };
}
