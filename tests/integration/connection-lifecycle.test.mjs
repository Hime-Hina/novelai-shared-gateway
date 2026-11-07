import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { createJobPresentation } from '../../server/generation/request-queries.ts';
import { createJobDelivery, deliverDirectGeneration } from '../../server/generation/delivery.ts';

test('OpenAI SSE 进度只读小 child，普通任务查询仍返回父请求完整提示词', async () => {
  const prompt = 'x'.repeat(128000),
    job = {
      id: 'job',
      requestId: 'parent',
      status: 'queued',
      request: { model: 'nai-diffusion-4-5-full' },
    };
  let payloadReads = 0;
  const ctx = createJobPresentation({
    store: {
      findJobContext: async () => ({ job, queue: { progress: 1, total: 2 } }),
      readGenerationRequest: () => {
        payloadReads++;
        return { prompt, tag: prompt, model: job.request.model };
      },
    },
    progress: () => ({}),
    errorMessage: (value) => value,
    imageUrl: () => '',
  });
  for (let index = 0; index < 3; index++) {
    const progress = await ctx.publicJobSnapshot('job');
    assert.equal(progress.queuePosition, 1);
    assert.equal(progress.prompt, '');
  }
  assert.equal(payloadReads, 0);
  assert.equal(ctx.publicJob(job, { queue: { progress: 1, total: 2 } }).prompt, prompt);
  assert.equal(payloadReads, 1);
});

test('SSE close 立即清除轮询等待者与定时器，不继续写入、不取消已受理任务', async (t) => {
  const job = { id: 'job', status: 'queued' },
    timers = new Set(),
    writes = [];
  const response = Object.assign(new EventEmitter(), {
    destroyed: false,
    write: (value) => writes.push(value),
    end() {},
  });
  const nativeSetTimeout = globalThis.setTimeout,
    nativeClearTimeout = globalThis.clearTimeout;
  t.mock.method(globalThis, 'setTimeout', (fn, ms) => {
    const timer = nativeSetTimeout(() => {
      timers.delete(timer);
      fn();
    }, ms);
    timers.add(timer);
    return timer;
  });
  t.mock.method(globalThis, 'clearTimeout', (timer) => {
    timers.delete(timer);
    nativeClearTimeout(timer);
  });
  const ctx = createJobDelivery({
    pollMs: 10000,
    errorMessage: (value) => value,
    store: {
      update: async (fn, options) => {
        assert.equal(options.persist, false);
        return fn({ jobs: [job], users: [] });
      },
    },
    snapshot: async () => ({ ...job, queuePosition: 1, queuedCount: 1 }),
    sendStreamHeaders() {},
    imageMarkdown: () => 'unused',
  });
  const jobWaiters = ctx.waiters;
  const running = ctx.streamOpenAiImageJob(response, { id: 'job' }, 'model', Date.now() + 30000);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(jobWaiters.get('job')?.size, 1);
  assert.equal(timers.size, 1);
  const before = writes.length;
  response.destroyed = true;
  response.emit('close');
  await running;
  assert.equal(jobWaiters.size, 0);
  assert.equal(timers.size, 0);
  assert.equal(response.listenerCount('close'), 0);
  assert.equal(writes.length, before);
  assert.equal(job.status, 'queued');
});

test('direct 已完成结果发送失败不重写任务、不退款或重新入队', async () => {
  const job = { id: 'job', status: 'done', imageId: 'image', fairCharge: { status: 'settled' } };
  const failure = new Error('synthetic socket write failed');
  let sends = 0;
  await assert.rejects(
    deliverDirectGeneration({ destroyed: false }, job, {
      timeoutMs: 1000,
      waitForJobResult: async () => ({ saved: { id: job.imageId } }),
      sendStoredImage: async () => {
        sends++;
        throw failure;
      },
      isCapacityError: () => false,
      sendBusyImage: () => assert.fail('must propagate socket failure'),
      sendTimeoutImage: () => assert.fail('must propagate socket failure'),
    }),
    (error) => error === failure,
  );
  assert.equal(sends, 1);
  assert.equal(job.status, 'done');
  assert.equal(job.fairCharge.status, 'settled');
  assert.equal(job.imageId, 'image');
});
