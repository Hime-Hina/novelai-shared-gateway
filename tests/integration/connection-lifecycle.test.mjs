import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { EventEmitter } from 'node:events';
import vm from 'node:vm';

const source = await readFile(new URL('../../server/gateway-runtime.js', import.meta.url), 'utf8');
function load(context, names) {
  const ctx = vm.createContext(context);
  for (const name of names) {
    const start = source.indexOf(`function ${name}(`), end = source.indexOf('\n}', start) + 2;
    vm.runInContext((source.slice(start - 6, start) === 'async ' ? 'async ' : '') + source.slice(start, end), ctx);
  }
  return ctx;
}

test('OpenAI SSE 进度只读小 child，普通任务查询仍返回父请求完整提示词', async () => {
  const prompt = 'x'.repeat(128000), job = { id: 'job', requestId: 'parent', status: 'queued', request: { model: 'nai-diffusion-4-5-full' } };
  let payloadReads = 0;
  const ctx = load({ store: { findJobContext: async () => ({ job, queue: { progress: 1, total: 2 } }),
    readGenerationRequest: () => { payloadReads++; return { prompt, tag: prompt, model: job.request.model }; } },
    jobAccountCost: () => 0, publicGenerationProgress: () => ({}), publicErrorMessage: value => value,
    publicImageUrl: () => '', jobDurationMs: () => 0
  }, ['publicJob', 'publicJobSnapshot']);
  for (let index = 0; index < 3; index++) {
    const progress = await ctx.publicJobSnapshot('job');
    assert.equal(progress.queuePosition, 1);
    assert.equal(progress.prompt, '');
  }
  assert.equal(payloadReads, 0);
  assert.equal(ctx.publicJob(job, { queue: { progress: 1, total: 2 } }).prompt, prompt);
  assert.equal(payloadReads, 1);
});

test('SSE close 立即清除轮询等待者与定时器，不继续写入、不取消已受理任务', async () => {
  const job = { id: 'job', status: 'queued' }, jobWaiters = new Map(), timers = new Set(), writes = [];
  const response = Object.assign(new EventEmitter(), { destroyed: false, write: value => writes.push(value), end() {} });
  const ctx = load({ jobWaiters, openAiQueuePollMs: 10000,
    setTimeout: (fn, ms) => { const timer = setTimeout(() => { timers.delete(timer); fn(); }, ms); timers.add(timer); return timer; },
    clearTimeout: timer => { timers.delete(timer); clearTimeout(timer); },
    store: { update: async (fn, options) => { assert.equal(options.persist, false); return fn({ jobs: [job], users: [] }); } },
    publicJobSnapshot: async () => ({ ...job, queuePosition: 1, queuedCount: 1 }),
    sendOpenAiStreamHeaders() {}, openAiImageMarkdown: () => 'unused', timeoutJob: () => assert.fail('HTTP close must not cancel job')
  }, ['streamOpenAiImageJob', 'openAiProgressLine', 'writeOpenAiChunk', 'finishOpenAiStream', 'waitForJobResult', 'removeJobWaiter', 'notifyJobWaiters']);
  const running = ctx.streamOpenAiImageJob({}, response, { id: 'job' }, 'model', Date.now() + 30000);
  await new Promise(resolve => setImmediate(resolve));
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
  let accepted = 0;
  const ctx = load({ isOfficialKey: () => false, directGenerateTimeoutMs: 1000,
    acceptDirectRequest: async () => { accepted++; return { id: job.id }; },
    waitForJobResult: async () => ({ saved: { id: job.imageId } }), sendStoredImage: async () => { throw failure; },
    isNovelAiCapacityError: () => false, markDirectJobFailed: () => { job.status = 'failed'; },
    removeJob: () => assert.fail('must retain admitted job'), timeoutJob: () => assert.fail('must retain admitted job')
  }, ['handleDirectGenerate']);
  await assert.rejects(ctx.handleDirectGenerate(new URL('http://local/generate?token=STA1N-synthetic'), { destroyed: false }), error => error === failure);
  assert.equal(accepted, 1);
  assert.equal(job.status, 'done');
  assert.equal(job.fairCharge.status, 'settled');
  assert.equal(job.imageId, 'image');
});
