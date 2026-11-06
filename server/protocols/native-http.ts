import type { IncomingMessage, ServerResponse, OutgoingHttpHeaders } from 'node:http';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { randomBytes } from 'node:crypto';
import Negotiator from 'negotiator';
import type { JsonStore, StoredGenerationRequest, StoreDatabase } from '../storage/sqlite-store.ts';
import { parseNativeImageRequest, parseNativeAuxiliaryRequest, nativeNormalizationContext, NATIVE_MAX_BODY_BYTES, type NativeNormalizationContext } from './native-image.ts';
import { readNativeBody } from './native-body.ts';
import { quoteFairRequest } from '../quota/fair-share.ts';
import { singleGenerationRequest } from '../generation/batch-requests.ts';
import { generationIntent, type createGenerationAdmission, type GenerationMetadata, type GenerationPayloadClaim } from '../generation/admission.ts';
import type { GenerationDeliveryResult } from '../generation/delivery.ts';
import type { JobPreviews } from '../generation/job-previews.ts';
import { nativeImageMsgpack, nativeImageSse, nativeImagesJson, nativeImageZip, nativeImagesZip, imageExtension } from './native-stream.ts';
import { publicErrorMessage, isTimeoutResultMessage } from '../http/public-errors.ts';

export interface NativeHttpRequest extends IncomingMessage {
  generationPayloadClaim?: GenerationPayloadClaim;
  generationIdempotencyKey?: string | null;
}
type NativeBatch = GenerationMetadata & { streamType?: 'sse' | 'msgpack' };
type StoredResult = NonNullable<GenerationDeliveryResult['saved']>;
type NativeError = Error & { statusCode?: number; cooldownUntil?: unknown };
interface NativeHttpDependencies {
  store: Pick<JsonStore, 'readUserByToken'>;
  admitGeneration: ReturnType<typeof createGenerationAdmission>;
  readJson(req: NativeHttpRequest, limit: number): Promise<unknown>;
  waitForJobResult(jobId: string, deadline: number, res: ServerResponse): Promise<GenerationDeliveryResult | null>;
  readStoredImage(image: StoredResult): Promise<Buffer>;
  sendImage(res: ServerResponse, status: number, mimeType: string, buffer: Buffer, headers?: OutgoingHttpHeaders): void;
  previews: Pick<JobPreviews, 'watch'>;
  schedule(): void;
  timeoutMs: number;
  now?: () => number;
  logDiagnostic?(id: string, event: string, details: object): void;
}
function error(statusCode: number, message: string): NativeError { return Object.assign(new Error(message), { statusCode }); }
export function nativeRequestDiagnostic(body: { parameters?: Record<string, unknown>; model?: unknown; action?: unknown; input?: unknown } | null) {
  const describe = (value: unknown) => {
    if (value === null) return { type: 'null' };
    if (Array.isArray(value)) return { type: 'array', length: value.length, nonEmpty: value.filter(item => item !== null && item !== '' && item !== undefined).length };
    if (typeof value === 'string') return { type: 'string', length: value.length };
    if (typeof value === 'number') return { type: 'number', value: Number.isFinite(value) ? value : 'non-finite' };
    if (typeof value === 'boolean') return { type: 'boolean', value };
    return { type: typeof value };
  };
  const identifier = (value: unknown) => typeof value === 'string' && /^[a-z][a-z0-9_-]{0,63}$/.test(value)
    && !/pst-|sta1n|token|secret|key/i.test(value) ? value : describe(value);
  const parameters = body?.parameters, numeric: Record<string, ReturnType<typeof describe> & { numericValue?: number }> = {};
  for (const field of ['width', 'height', 'steps', 'n_samples', 'seed', 'scale', 'cfg_rescale']) {
    numeric[field] = describe(parameters?.[field]);
    if (typeof parameters?.[field] === 'string' && /^-?\d+(?:\.\d+)?$/.test(parameters[field]) && parameters[field].length <= 24) numeric[field].numericValue = Number(parameters[field]);
  }
  const structure: Record<string, ReturnType<typeof describe>> = {};
  for (const field of ['image', 'mask', 'reference_image', 'reference_image_multiple', 'director_reference_images', 'controlnet_condition', 'controlnet_model', 'v4_prompt', 'v4_negative_prompt', 'characterPrompts', 'stream']) {
    if (parameters && Object.hasOwn(parameters, field)) structure[field] = describe(parameters[field]);
  }
  return { model: identifier(body?.model), action: identifier(body?.action), prompt: describe(body?.input), numeric, structure };
}

export function createNativeHttp(deps: NativeHttpDependencies) {
  const now = deps.now || Date.now;
  const logDiagnostic = deps.logDiagnostic || ((id: string, event: string, details: object) =>
    console.warn('[native-diagnostic] ' + JSON.stringify({ time: new Date(now()).toISOString(), id, event, ...details })));

  async function accept(req: NativeHttpRequest, diagnosticId: string, operation?: 'encode-vibe' | 'upscale'): Promise<NativeBatch> {
    const token = String(req.headers.authorization || '').match(/^Bearer\s+(.+)$/i)?.[1]?.trim() || '';
    if (!token.startsWith('STA1N-')) throw error(401, 'Existing gateway key required.');
    const user = await deps.store.readUserByToken(token);
    if (!user || user.enabled === false) throw error(401, 'invalid token.');
    let body = await readNativeBody(req, { jsonReader: request => deps.readJson(request as NativeHttpRequest, NATIVE_MAX_BODY_BYTES) }) as
      { parameters?: { seed?: unknown; stream?: NativeBatch['streamType'] }; allowPurchasedAnlas?: unknown } | null;
    logDiagnostic(diagnosticId, 'received', nativeRequestDiagnostic(body));
    const normalize = (context?: Partial<StoreDatabase['settings']>) => {
      // Old mappings saved only gateway defaults; they cannot recover earlier Native definitions.
      const native = context?.native as NativeNormalizationContext | undefined;
      const request: StoredGenerationRequest = operation ? parseNativeAuxiliaryRequest(body, operation, native) : parseNativeImageRequest(body, native);
      request.allowPurchasedAnlas = body?.allowPurchasedAnlas === true;
      quoteFairRequest(singleGenerationRequest(request));
      return request;
    };
    const intent = generationIntent(req.generationIdempotencyKey, normalize, { operation: operation || 'generate',
      randomSeed: !operation && (body?.parameters?.seed === undefined || body.parameters.seed === -1), noCache: true, claim: req.generationPayloadClaim,
      context: () => ({ native: nativeNormalizationContext() }) });
    const batch = await deps.admitGeneration({ identity: { token }, request: intent ? null : normalize(), diagnosticId,
      admissionVersion: user.jobAdmissionVersion || 0, claim: req.generationPayloadClaim, intent, batch: true, source: 'novelai-native', noCache: true });
    const streamType = body?.parameters?.stream ?? 'sse';
    body = null; deps.schedule();
    return { ...batch, streamType };
  }

  async function wait(jobId: string, deadline: number, res: ServerResponse) {
    const result = await deps.waitForJobResult(jobId, deadline, res);
    if (res.destroyed) return null;
    if (!result) throw error(504, 'direct generate timeout');
    if (result.error) throw Object.assign(error(result.statusCode || (isTimeoutResultMessage(result.error) ? 504 : 500), result.error), { cooldownUntil: result.cooldownUntil });
    return result;
  }
  function responseType(req: NativeHttpRequest, res: ServerResponse) {
    res.setHeader('Vary', 'Accept');
    const type = new Negotiator(req).mediaType(['application/zip', 'application/json; charset=utf-8']);
    if (!type) throw error(406, '响应仅支持 application/zip 或 application/json');
    return type;
  }
  async function sendJson(res: ServerResponse, entries: { saved?: StoredResult; seed?: number }[], summary?: Parameters<typeof nativeImagesJson>[1], headers: OutgoingHttpHeaders = {}) {
    async function* images() {
      for (const [index, entry] of entries.entries()) yield { buffer: await deps.readStoredImage(entry.saved!), index, seed: entry.seed };
    }
    const chunks = nativeImagesJson(images(), summary), first = await chunks.next();
    try {
      res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', ...headers });
      await pipeline(Readable.from((async function* () { yield first.value; yield* chunks; })(), { objectMode: false }), res);
    } finally { await chunks.return(); }
  }
  function* streamChunks(res: ServerResponse, event: Parameters<typeof nativeImageMsgpack>[0], type: NativeBatch['streamType']) {
    for (const chunk of type === 'sse' ? nativeImageSse(event) : [nativeImageMsgpack(event)]) {
      if (!res.headersSent) res.writeHead(200, { 'content-type': type === 'sse' ? 'text/event-stream' : 'application/msgpack',
        'cache-control': 'no-store, no-transform', 'x-accel-buffering': 'no' });
      yield chunk;
    }
  }
  async function streamImages(res: ServerResponse, batch: NativeBatch, deadline: number) {
    res.once('error', () => res.destroy());
    for (let index = 0; index < batch.count; index++) {
      if (res.destroyed) return;
      const unwatch = deps.previews.watch(batch.jobIds[index]!, frame => {
        if (res.destroyed || res.writableEnded || res.writableNeedDrain || !Number.isSafeInteger(frame.stepIndex) || frame.stepIndex! < 0) return;
        for (const chunk of streamChunks(res, { event_type: 'intermediate', samp_ix: index, step_ix: frame.stepIndex, image: frame.buffer }, batch.streamType)) res.write(chunk);
      });
      try {
        const result = await wait(batch.jobIds[index]!, deadline, res); unwatch();
        if (!result) return;
        const image = await deps.readStoredImage(result.saved!);
        if (res.destroyed) return;
        for (const chunk of streamChunks(res, { event_type: 'final', samp_ix: index, image }, batch.streamType)) {
          if (res.destroyed) return;
          await new Promise<void>((resolve, reject) => {
            const onClose = () => resolve(); res.once('close', onClose);
            res.write(chunk, failure => { res.off('close', onClose); if (failure) reject(failure); else resolve(); });
          });
        }
      } catch (failure) {
        if (res.destroyed) return;
        if (!res.headersSent) throw failure;
        for (const chunk of streamChunks(res, { event_type: 'error', samp_ix: index, code: String((failure as NativeError).statusCode || 500),
          message: publicErrorMessage((failure as NativeError).message || '生成失败', { generation: true }) }, batch.streamType)) res.write(chunk);
        break;
      } finally { unwatch(); }
    }
    if (!res.destroyed) res.end();
  }

  async function handleImage(req: NativeHttpRequest, res: ServerResponse, { stream = false } = {}) {
    const diagnosticId = randomBytes(8).toString('hex'); res.setHeader('X-Nai-Diagnostic-Id', diagnosticId);
    const json = !stream && responseType(req, res) !== 'application/zip', batch = await accept(req, diagnosticId);
    if (batch.requestId || !batch.replayed) res.setHeader('X-Nai-Request-Id', batch.requestId || batch.id);
    const deadline = now() + deps.timeoutMs;
    if (stream) { await streamImages(res, batch, deadline); return; }
    const entries: { saved?: StoredResult; buffer?: Buffer; filename: string; seed?: number }[] = [];
    const results: NonNullable<NonNullable<Parameters<typeof nativeImagesJson>[1]>['results']> = [];
    for (let index = 0; index < batch.count; index++) {
      if (res.destroyed) return;
      try {
        const result = await wait(batch.jobIds[index]!, deadline, res);
        if (!result) return;
        const filename = `image_${index}.${imageExtension(result.saved!.mimeType || '')}`;
        entries.push({ ...(json ? { saved: result.saved } : { buffer: await deps.readStoredImage(result.saved!) }), filename, seed: result.seed });
        results.push({ index, status: 'done', filename });
      } catch (failure) {
        if (!entries.length) throw failure;
        results.push({ index, status: 'failed', message: publicErrorMessage((failure as NativeError).message || '生成失败', { generation: true }) }); break;
      }
    }
    if (res.destroyed) return;
    const count = batch.count, completed = entries.length;
    const summary = count > 1 ? { requested: count, completed, remaining: count - completed, billing: 'independent_single_images' as const, results } : null;
    const headers = { 'X-Nai-Image-Count': String(completed), 'X-Nai-Partial': String(completed < count) };
    if (json) { await sendJson(res, entries, summary, headers); return; }
    if (summary) entries.push({ filename: 'results.json', buffer: Buffer.from(JSON.stringify(summary), 'utf8') });
    deps.sendImage(res, 200, 'application/zip', nativeImagesZip(entries.map(entry => ({ filename: entry.filename, buffer: entry.buffer! }))),
      { 'cache-control': 'no-store', 'content-disposition': 'attachment; filename="images.zip"', ...headers });
  }

  async function handleAuxiliary(req: NativeHttpRequest, res: ServerResponse, operation: 'encode-vibe' | 'upscale') {
    const diagnosticId = randomBytes(8).toString('hex'); res.setHeader('X-Nai-Diagnostic-Id', diagnosticId);
    const json = operation === 'upscale' && responseType(req, res) !== 'application/zip', batch = await accept(req, diagnosticId, operation);
    if (batch.requestId || !batch.replayed) res.setHeader('X-Nai-Request-Id', batch.requestId || batch.id);
    const result = await wait(batch.jobIds[0]!, now() + deps.timeoutMs, res);
    if (!result) return;
    if (json) { await sendJson(res, [{ saved: result.saved }]); return; }
    const buffer = await deps.readStoredImage(result.saved!);
    if (operation === 'encode-vibe') deps.sendImage(res, 201, 'application/binary', buffer, { 'cache-control': 'no-store' });
    else deps.sendImage(res, 200, 'application/zip', nativeImageZip(buffer, `image_0.${imageExtension(result.saved!.mimeType || '')}`), { 'cache-control': 'no-store' });
  }

  async function handle(req: NativeHttpRequest, res: ServerResponse, url: URL): Promise<boolean> {
    if (req.method !== 'POST') return false;
    if (['/ai/generate-image', '/v1/ai/generate-image', '/ai/generate-image-stream', '/v1/ai/generate-image-stream'].includes(url.pathname)) {
      await handleImage(req, res, { stream: url.pathname.endsWith('-stream') }); return true;
    }
    if (['/ai/encode-vibe', '/v1/ai/encode-vibe', '/ai/upscale', '/v1/ai/upscale'].includes(url.pathname)) {
      await handleAuxiliary(req, res, url.pathname.endsWith('/encode-vibe') ? 'encode-vibe' : 'upscale'); return true;
    }
    return false;
  }
  return { handle, handleImage, handleAuxiliary };
}
