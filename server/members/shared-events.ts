import type { ServerResponse } from 'node:http';
import type { SharedStatus } from '../quota/shared-status.ts';
import {
  stateEventsFor,
  mergeStateEvents,
  stateInvalidation,
} from '../events/state-notifications.ts';

type PublishedEvent = Parameters<Parameters<ReturnType<typeof stateEventsFor>['subscribe']>[0]>[0];
const statusStreams = new Map<string, number>();

// 复用原状态流寿命和限流；格式适配只提供每轮重新授权后的当前公开快照。
export async function streamMemberSharedEvents(
  res: ServerResponse,
  deps: {
    memberId: string;
    store: Parameters<typeof stateEventsFor>[0];
    readCurrent(): Promise<{ memberId: string; owner: boolean; state: SharedStatus } | null>;
  },
): Promise<void> {
  const { memberId, store } = deps;
  if (res.destroyed || res.writableEnded) {
    return;
  }
  if (
    (statusStreams.get(memberId) || 0) >= 3 ||
    [...statusStreams.values()].reduce((n, v) => n + v, 0) >= 100
  ) {
    throw Object.assign(new Error('状态连接过多，请关闭多余页面'), { statusCode: 429 });
  }
  statusStreams.set(memberId, (statusStreams.get(memberId) || 0) + 1);
  res.writeHead(200, {
    'content-type': 'text/event-stream',
    'cache-control': 'no-store',
    'x-accel-buffering': 'no',
    connection: 'keep-alive',
  });
  res.flushHeaders?.();
  const hub = stateEventsFor(store);
  let closed = false,
    timer: ReturnType<typeof setTimeout> | undefined,
    previous = '',
    heartbeat = 0,
    running = false,
    requested = false,
    resync = true,
    pending: PublishedEvent | undefined;
  let unsubscribe = () => {};
  const stop = () => {
    if (closed) {
      return;
    }
    closed = true;
    clearTimeout(timer);
    unsubscribe();
    res.off('close', stop);
    res.off('error', stop);
    const count = (statusStreams.get(memberId) || 1) - 1;
    if (count) {
      statusStreams.set(memberId, count);
    } else {
      statusStreams.delete(memberId);
    }
  };
  res.once('close', stop);
  res.once('error', stop);
  const wake = (event?: PublishedEvent): void => {
    if (closed) {
      return;
    }
    if (event) {
      pending = mergeStateEvents(pending, event);
    }
    requested = true;
    clearTimeout(timer);
    if (!running) {
      void tick();
    }
  };
  const tick = async () => {
    running = true;
    try {
      while (requested && !closed) {
        requested = false;
        const event = pending || hub.current(),
          full = resync;
        pending = undefined;
        resync = false;
        const current = await deps.readCurrent();
        if (closed) {
          return;
        }
        if (!current) {
          res.write('event: auth-expired\ndata: {}\n\n');
          res.end();
          stop();
          return;
        }
        const text = JSON.stringify(current.state);
        if (text !== previous) {
          res.write(`event: shared-status\ndata: ${text}\n\n`);
          previous = text;
          heartbeat = Date.now();
        }
        const invalidation = stateInvalidation(event, current.memberId, current.owner, full);
        if (invalidation) {
          res.write(`event: state-invalidated\ndata: ${JSON.stringify(invalidation)}\n\n`);
          heartbeat = Date.now();
        }
        if (Date.now() - heartbeat > 15000) {
          res.write(': keepalive\n\n');
          heartbeat = Date.now();
        }
        if (res.writableLength > 65536) {
          res.end();
          stop();
          return;
        }
      }
    } catch (error) {
      if (!closed) {
        if ([401, 403].includes((error as { statusCode?: number }).statusCode!)) {
          res.write('event: auth-expired\ndata: {}\n\n');
        }
        res.end();
        stop();
      }
    } finally {
      running = false;
      if (!closed) {
        timer = setTimeout(() => wake(), 2000);
        timer.unref?.();
      }
    }
  };
  unsubscribe = hub.subscribe(wake, () => {
    res.end();
    stop();
  });
  wake();
}
