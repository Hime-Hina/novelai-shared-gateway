import type { ServerResponse } from 'node:http';

interface PreviewProgress {
  previewBuffer?: Buffer;
  previewMimeType?: string;
  stepIndex?: number;
  percent?: number;
  step?: number;
  total?: number;
}

interface PreviewFrame {
  token: string;
  jobId: string;
  buffer: Buffer;
  mimeType?: string;
  stepIndex?: number;
  progress: Pick<PreviewProgress, 'percent' | 'step' | 'total'>;
  sentAt: number;
}

type PreviewWatcher = (frame: PreviewFrame) => unknown;
type PreviewResponse = Pick<ServerResponse, 'destroyed' | 'writableEnded' | 'writableNeedDrain' | 'writeHead' | 'write' | 'end' | 'once'>;

// Only the latest preview of each running job lives here. Never persisted.
export class JobPreviews {
  frames = new Map<string, PreviewFrame>();
  listeners = new Map<string, Set<PreviewResponse>>();
  watchers = new Map<string, Set<PreviewWatcher>>();
  closed = false;

  watch(jobId: string, callback: PreviewWatcher): () => void {
    const watchers = this.watchers.get(jobId) || new Set<PreviewWatcher>();
    const notify = (frame: PreviewFrame): void => {
      // 预览观察者不能把传输异常变成生成失败或影响任务结算。
      try { Promise.resolve(callback(frame)).catch(() => {}); } catch {}
    };
    watchers.add(notify);
    this.watchers.set(jobId, watchers);
    const frame = this.frames.get(jobId);
    if (frame) notify(frame);
    return () => {
      watchers.delete(notify);
      if (!watchers.size && this.watchers.get(jobId) === watchers) this.watchers.delete(jobId);
    };
  }

  subscribe(token: string, res: PreviewResponse): void {
    if (this.closed) { res.end(); return; }
    res.writeHead(200, {
      'content-type': 'text/event-stream; charset=utf-8',
      'cache-control': 'no-cache, no-transform',
      'x-accel-buffering': 'no'
    });
    res.write(': connected\n\n');
    const listeners = this.listeners.get(token) || new Set<PreviewResponse>();
    listeners.add(res);
    this.listeners.set(token, listeners);
    const heartbeat = setInterval(() => {
      if (!res.destroyed && !res.writableNeedDrain) res.write(': keepalive\n\n');
    }, 15000);
    heartbeat.unref();
    res.once('close', () => {
      clearInterval(heartbeat);
      listeners.delete(res);
      if (!listeners.size) this.listeners.delete(token);
    });
    for (const frame of this.frames.values()) {
      if (frame.token === token) this.send(res, frame);
    }
  }

  closeSubscriptions(): void {
    if (this.closed) return;
    this.closed = true;
    for (const listeners of this.listeners.values()) {
      for (const res of listeners) if (!res.destroyed && !res.writableEnded) res.end();
    }
  }

  update(job: { id: string; userToken: string; status: string } | null | undefined, progress: PreviewProgress): void {
    if (!job || job.status !== 'running') return;
    const buffer = progress.previewBuffer;
    if (!buffer?.length || buffer.length > 512 * 1024) return;
    const previous = this.frames.get(job.id);
    const now = Date.now();
    const frame: PreviewFrame = {
      token: job.userToken, jobId: job.id, buffer,
      mimeType: progress.previewMimeType,
      stepIndex: progress.stepIndex,
      progress: { percent: progress.percent, step: progress.step, total: progress.total },
      sentAt: previous?.sentAt || 0
    };
    this.frames.set(job.id, frame);
    // Bound repaint/network work while retaining the newest frame for reconnects.
    if (now - frame.sentAt < 150) return;
    frame.sentAt = now;
    for (const notify of this.watchers.get(job.id) || []) notify(frame);
    const listeners = this.listeners.get(frame.token);
    if (!listeners?.size) return;
    const message = this.message(frame);
    for (const res of listeners) this.send(res, frame, message);
  }

  message(frame: PreviewFrame): string {
    return `data: ${JSON.stringify({
      jobId: frame.jobId,
      progress: frame.progress,
      preview: `data:${frame.mimeType};base64,${frame.buffer.toString('base64')}`
    })}\n\n`;
  }

  send(res: PreviewResponse, frame: PreviewFrame, message?: string): void {
    // Slow/disconnected clients must not block generation or queue old frames.
    if (!res.destroyed && !res.writableEnded && !res.writableNeedDrain) {
      res.write(message || this.message(frame));
    }
  }

  clear(jobId: string): void {
    this.frames.delete(jobId);
  }
}
