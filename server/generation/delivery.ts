import type { ServerResponse } from 'node:http';
import type { JsonStore, StoreItem } from '../storage/sqlite-store.ts';
import { cancelGenerationRequestCommand, changeMemberAdmissionCommand, failGenerationCommand, cancelReservedGeneration,
  cleanupStaleGenerationJobs, cleanupInterruptedGenerationJobs, type GenerationFailureOptions, type GenerationReservation } from './execution.ts';
import type { createJobPresentation } from './request-queries.ts';
import { buildErrorImage } from '../protocols/novelai-client.ts';
import { cooldownRetryAfter } from '../quota/shared-capacity.ts';

export interface GenerationDeliveryResult {
  image?: { buffer: Buffer; mimeType: string };
  saved?: Pick<StoreItem, 'file' | 'mimeType' | 'r2' | 'base64'> & { id?: string; imageId?: string; operation?: string; sha256?: string; byteLength?: number };
  seed?: number; balance?: number; error?: string; statusCode?: number; cooldownUntil?: string | number | null;
}
type Response = ServerResponse & { generationDeliveryTracked?: boolean };
type Waiter = { resolve(value: GenerationDeliveryResult | null): void; timer: ReturnType<typeof setTimeout>; cleanup(): void };
type PublicJob = NonNullable<Awaited<ReturnType<ReturnType<typeof createJobPresentation>['publicJobSnapshot']>>>;

export function createGenerationFailureHandlers(store: JsonStore, options: GenerationFailureOptions & {
  notify(jobId: string, result: GenerationDeliveryResult): void;
  clearProgress(jobId: string): void;
  schedule(): void;
  executionEvidence?(jobId: string): GenerationReservation | undefined;
  log?(value: object): void;
}) {
  async function fail(reservation: GenerationReservation, error: unknown, cancelled: boolean) {
    const requestedAt = Date.now();
    const result = await (cancelled ? cancelReservedGeneration : failGenerationCommand)(store, reservation, error, options);
    if (!result.changed) return;
    options.log?.({ event: cancelled ? 'cancel_commit' : 'failure_commit', jobId: result.jobId,
      requestedAt, committedAt: Date.now(), cooldownUntil: result.cooldownUntil, kind: result.kind });
    options.schedule();
    if (result.jobId) {
      options.clearProgress(result.jobId);
      options.notify(result.jobId, { error: result.message, statusCode: result.statusCode, cooldownUntil: result.cooldownUntil });
    }
  }
  return {
    failGeneration: (reservation: GenerationReservation, error: unknown) => fail(reservation, error, false),
    cancelReservedJob: (reservation: GenerationReservation, error: unknown) => fail(reservation, error, true),
    async cleanupStaleActiveJobs(reason = 'stale active job cleanup') {
      const result = await cleanupStaleGenerationJobs(store, reason, options);
      for (const { jobId, ...failure } of result.failures) options.notify(jobId, { error: 'direct generate timeout', ...failure });
      return result;
    },
    async cleanupInterruptedStartupJobs() {
      const result = await cleanupInterruptedGenerationJobs(store, options);
      for (const jobId of result.jobIds) options.notify(jobId, { error: 'direct generate timeout' });
      return result;
    }
  };
}

export function createRequestControls(store: JsonStore, deps: {
  clearProgress(jobId: string): void;
  notify(jobId: string, result: GenerationDeliveryResult): void;
  schedule(): void;
  readRequest(requestId: string, memberId: string): Promise<object>;
  now?: () => number;
}) {
  function notifyCancelled(ids: string[], message: string) {
    for (const id of ids) { deps.clearProgress(id); deps.notify(id, { error: message, statusCode: 409 }); }
    deps.schedule();
  }
  return {
    async cancelGenerationRequest(requestId: string, memberId: string) {
      const result = await cancelGenerationRequestCommand(store, requestId, memberId, deps);
      notifyCancelled(result.cancelledJobIds, '请求已取消，未派发任务已终止');
      return { ...await deps.readRequest(requestId, memberId), cancelledCount: result.cancelledJobIds.length };
    },
    async changeMemberAdmission(userId: string, acceptingJobs: boolean, actorId = 'local-admin') {
      const result = await changeMemberAdmissionCommand(store, userId, acceptingJobs, { ...deps, actorId });
      notifyCancelled(result.cancelledJobIds, '管理员已停止此成员接单，未派发任务已取消');
      return { ...result, cancelledCount: result.cancelledJobIds.length };
    }
  };
}

export function createJobDelivery(deps: {
  store: Pick<JsonStore, 'update' | 'selectItemById' | 'findImage'>;
  errorMessage(message: string): string;
  snapshot(jobId: string): Promise<PublicJob | null>;
  sendStreamHeaders(res: Response): void;
  imageMarkdown(job: PublicJob): string;
  pollMs?: number;
  log?(value: object): void;
}) {
  const waiters = new Map<string, Set<Waiter>>();
  function remove(jobId: string, waiter: Waiter) {
    clearTimeout(waiter.timer); waiter.cleanup();
    const subscriptions = waiters.get(jobId);
    subscriptions?.delete(waiter); if (!subscriptions?.size) waiters.delete(jobId);
  }
  function notifyJobWaiters(jobId: string, payload: GenerationDeliveryResult) {
    const subscriptions = waiters.get(jobId); if (!subscriptions) return;
    waiters.delete(jobId);
    if (payload.error) payload = { ...payload, error: deps.errorMessage(payload.error) };
    for (const waiter of subscriptions) { clearTimeout(waiter.timer); waiter.cleanup(); waiter.resolve(payload); }
  }
  function waitForJobResult(jobId: string, deadline: number, response?: Response) {
    if (response && !response.generationDeliveryTracked) {
      response.generationDeliveryTracked = true;
      const waitingAt = Date.now();
      const delivered = () => {
        response.off('finish', delivered); response.off('close', delivered);
        deps.log?.({ event: 'http_delivery', jobId, waitingAt, at: Date.now(), finished: response.writableFinished === true });
      };
      response.once('finish', delivered); response.once('close', delivered);
    }
    return new Promise<GenerationDeliveryResult | null>(resolve => {
      const timer = setTimeout(() => { remove(jobId, waiter); resolve(null); }, Math.max(1, deadline - Date.now()));
      const onClose = () => { remove(jobId, waiter); resolve(null); };
      const waiter: Waiter = { resolve, timer, cleanup: () => response?.off('close', onClose) };
      if (!waiters.has(jobId)) waiters.set(jobId, new Set());
      waiters.get(jobId)!.add(waiter);
      if (response?.destroyed) { onClose(); return; }
      response?.once('close', onClose);
      // Subscribe first; serial read only sees committed state and covers a missed terminal notification.
      deps.store.update(db => {
        const job = db.jobs.find(item => item.id === jobId) || deps.store.selectItemById('jobs', jobId);
        if (!job) return null;
        return { status: job.status, imageId: job.imageId, seed: job.request?.seed, error: job.error, errorStatus: job.errorStatus,
          cooldownUntil: job.quotaRetryAfterUntil || job.sharedCooldown?.until, balance: db.users.find(user => user.token === job.userToken)?.balance };
      }, { persist: false }).then(async job => {
        if (job?.status === 'failed') notifyJobWaiters(jobId, { error: job.error, statusCode: job.errorStatus || 500, cooldownUntil: job.cooldownUntil });
        else if (job?.status === 'done') {
          const saved = await deps.store.findImage(job.imageId!);
          if (saved) notifyJobWaiters(jobId, { saved, seed: job.seed, balance: job.balance });
        }
      }).catch(() => notifyJobWaiters(jobId, { error: '任务状态暂时无法读取，请稍后查询', statusCode: 503 }));
    });
  }
  async function streamOpenAiImageJob(res: Response, job: { id: string }, model: string, deadline: number, imageMarkdown = deps.imageMarkdown) {
    deps.sendStreamHeaders(res);
    const id = `chatcmpl-${job.id}`;
    writeOpenAiChunk(res, { id, model, content: '<think>\n任务已提交，正在进入队列\n' });
    let lastLine = '', reachedRunning = false;
    while (!res.destroyed && Date.now() < deadline) {
      const snapshot = await deps.snapshot(job.id); if (res.destroyed) return;
      if (!snapshot) {
        writeOpenAiChunk(res, { id, model, content: '任务不存在\n</think>\n任务不存在\n' }); finishOpenAiStream(res, id, model); return;
      }
      const line = snapshot.status === 'queued'
        ? snapshot.queuePosition && snapshot.queuedCount ? `排队中：第 ${snapshot.queuePosition} / ${snapshot.queuedCount} 个` : '排队中，正在等待可用账号'
        : snapshot.status === 'running' ? '已路由账号，正在生成' : '';
      const queuedAfterRunning = reachedRunning && snapshot.status === 'queued';
      if (snapshot.status === 'running') reachedRunning = true;
      if (line && !queuedAfterRunning && line !== lastLine) { writeOpenAiChunk(res, { id, model, content: `${line}\n` }); lastLine = line; }
      if (snapshot.status === 'done') {
        writeOpenAiChunk(res, { id, model, content: `生成完成\n</think>\n${imageMarkdown(snapshot)}\n` }); finishOpenAiStream(res, id, model); return;
      }
      if (snapshot.status === 'failed') {
        const message = snapshot.error || '生成失败'; writeOpenAiChunk(res, { id, model, content: `${message}\n</think>\n${message}\n` }); finishOpenAiStream(res, id, model); return;
      }
      await waitForJobResult(job.id, Math.min(deadline, Date.now() + (deps.pollMs ?? 650)), res);
    }
    if (res.destroyed) return;
    writeOpenAiChunk(res, { id, model, content: '连接超时\n</think>\n连接超时\n' }); finishOpenAiStream(res, id, model);
  }
  return { waiters, waitForJobResult, notifyJobWaiters, streamOpenAiImageJob };
}

export function writeOpenAiChunk(res: Response, { id, model, content }: { id: string; model: string; content: string }) {
  res.write(`data: ${JSON.stringify({ id, object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model,
    choices: [{ index: 0, delta: { content }, finish_reason: null }] })}\n\n`);
}
export function finishOpenAiStream(res: Response, id: string, model: string) {
  res.write(`data: ${JSON.stringify({ id, object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model,
    choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`);
  res.write('data: [DONE]\n\n'); res.end();
}

export async function deliverDirectGeneration(res: Response, accepted: { id: string; requestId?: string | null; cached?: GenerationDeliveryResult['saved']; balance?: number; cacheHit?: boolean; replayed?: boolean }, deps: {
  waitForJobResult(jobId: string, deadline: number, response: Response): Promise<GenerationDeliveryResult | null>;
  timeoutMs: number;
  sendStoredImage(res: Response, status: number, image: GenerationDeliveryResult['saved'], headers: Record<string, string>): Promise<void>;
  sendBusyImage(res: Response, until?: string): void;
  sendTimeoutImage(res: Response, until?: string): void;
  isCapacityError(error: unknown): boolean;
}) {
  if (accepted.requestId) res.setHeader('X-Nai-Request-Id', accepted.requestId);
  if (res.destroyed) return;
  if (accepted.cached) { await deps.sendStoredImage(res, 200, accepted.cached, { 'x-cache': 'hit', 'x-balance': String(accepted.balance) }); return; }
  try {
    const result = await deps.waitForJobResult(accepted.id, Date.now() + deps.timeoutMs, res);
    if (res.destroyed) return;
    if (!result) { deps.sendTimeoutImage(res); return; }
    if (result.error) throw Object.assign(new Error(result.error), { statusCode: result.statusCode || 500, cooldownUntil: result.cooldownUntil });
    await deps.sendStoredImage(res, 200, result.saved, { 'x-cache': accepted.cacheHit ? 'hit' : 'miss', 'x-balance': String((accepted.replayed ? accepted.balance : result.balance) ?? '') });
  } catch (error) {
    if (res.destroyed) return;
    const failure = error as Error & { statusCode?: number; cooldownUntil?: string };
    if (failure.statusCode === 429 || deps.isCapacityError(error)) { deps.sendBusyImage(res, failure.cooldownUntil); return; }
    if (failure.message === 'direct generate timeout') { deps.sendTimeoutImage(res, failure.cooldownUntil); return; }
    throw error;
  }
}

export function generationErrorImage(kind: 'busy' | 'timeout', cooldownUntil?: string, now = Date.now()) {
  const image = buildErrorImage(kind === 'busy' ? '服务器繁忙，请稍后再试' : '连接超时'), retryAfter = cooldownRetryAfter(cooldownUntil, now);
  return { ...image, statusCode: 200, headers: { 'cache-control': 'no-store', 'x-error': '1', [`x-${kind}`]: '1', ...(retryAfter ? { 'retry-after': String(retryAfter) } : {}) } };
}
