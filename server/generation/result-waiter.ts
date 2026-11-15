import type { ServerResponse } from 'node:http';
import type { GenerationDeliveryResult } from './delivery.ts';

type Response = ServerResponse & { generationDeliveryTracked?: boolean };
type Waiter = {
  resolve(value: GenerationDeliveryResult | null): void;
  timer: ReturnType<typeof setTimeout>;
  cleanup(): void;
};

// 所有 HTTP 领取共用订阅先于冷读、截止和断线清理；readResult 仍由真实 owner 提供。
export function createJobResultWaiter(deps: {
  readResult(jobId: string): Promise<GenerationDeliveryResult | null>;
  errorMessage(message: string): string;
  log?(value: object): void;
  now?(): number;
}) {
  const now = deps.now ?? Date.now;
  const waiters = new Map<string, Set<Waiter>>();
  function remove(jobId: string, waiter: Waiter) {
    clearTimeout(waiter.timer);
    waiter.cleanup();
    const subscriptions = waiters.get(jobId);
    subscriptions?.delete(waiter);
    if (!subscriptions?.size) {
      waiters.delete(jobId);
    }
  }
  function notifyJobWaiters(jobId: string, payload: GenerationDeliveryResult) {
    const subscriptions = waiters.get(jobId);
    if (!subscriptions) {
      return;
    }
    waiters.delete(jobId);
    if (payload.error) {
      payload = { ...payload, error: deps.errorMessage(payload.error) };
    }
    for (const waiter of subscriptions) {
      clearTimeout(waiter.timer);
      waiter.cleanup();
      waiter.resolve(payload);
    }
  }
  // HTTP 截止或断连只解除领取等待；任务的业务期限、取消和结算由命令层管理。
  function waitForJobResult(jobId: string, deadline: number, response?: Response) {
    if (response && !response.generationDeliveryTracked) {
      response.generationDeliveryTracked = true;
      const waitingAt = now();
      const delivered = () => {
        response.off('finish', delivered);
        response.off('close', delivered);
        deps.log?.({
          event: 'http_delivery',
          jobId,
          waitingAt,
          at: now(),
          finished: response.writableFinished === true,
        });
      };
      response.once('finish', delivered);
      response.once('close', delivered);
    }
    return new Promise<GenerationDeliveryResult | null>((resolve) => {
      const timer = setTimeout(
        () => {
          remove(jobId, waiter);
          resolve(null);
        },
        Math.max(1, deadline - now()),
      );
      const onClose = () => {
        remove(jobId, waiter);
        resolve(null);
      };
      const waiter: Waiter = { resolve, timer, cleanup: () => response?.off('close', onClose) };
      if (!waiters.has(jobId)) {
        waiters.set(jobId, new Set());
      }
      waiters.get(jobId)!.add(waiter);
      if (response?.destroyed) {
        onClose();
        return;
      }
      response?.once('close', onClose);
      // Subscribe first; serial read only sees committed state and covers a missed terminal notification.
      void readResult(jobId);
    });
  }
  async function readResult(jobId: string): Promise<void> {
    try {
      const result = await deps.readResult(jobId);
      if (result) {
        notifyJobWaiters(jobId, result);
      }
    } catch {
      notifyJobWaiters(jobId, { error: '任务状态暂时无法读取，请稍后查询', statusCode: 503 });
    }
  }
  return {
    waiters,
    waitForJobResult,
    notifyJobWaiters,
    refresh() {
      for (const jobId of waiters.keys()) {
        void readResult(jobId);
      }
    },
  };
}
