import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { JobPreviews } from '../../server/job-previews.ts';

const job = { id: 'job-a', userToken: 'member-a', status: 'running' };
const progress = (stepIndex, buffer = Buffer.from([stepIndex])) => ({ previewBuffer: buffer,
  previewMimeType: 'image/jpeg', stepIndex, step: stepIndex + 1, total: 28, percent: stepIndex * 3 });

test('逐job订阅独立于门户，节流仍缓存最新帧，回放保留上游stepIndex', t => {
  t.mock.timers.enable({ apis: ['Date'], now: 1800000000000 });
  const previews = new JobPreviews(), received = [], replayed = [], other = [];
  const remove = previews.watch(job.id, frame => received.push(frame));
  const removeOther = previews.watch('job-b', frame => other.push(frame));
  previews.update(job, progress(0));
  assert.equal(received.length, 1); assert.equal(received[0].stepIndex, 0);
  assert.deepEqual(received[0].progress, { step: 1, total: 28, percent: 0 });
  assert.equal(previews.listeners.size, 0); assert.equal(other.length, 0);
  t.mock.timers.tick(149); previews.update(job, progress(1));
  assert.equal(received.length, 1);
  const removeReplay = previews.watch(job.id, frame => replayed.push(frame));
  assert.equal(replayed[0].stepIndex, 1); assert.deepEqual(replayed[0].buffer, Buffer.from([1]));
  t.mock.timers.tick(1); previews.update(job, progress(2));
  assert.deepEqual(received.map(frame => frame.stepIndex), [0, 2]);
  assert.deepEqual(replayed.map(frame => frame.stepIndex), [1, 2]);
  remove(); remove(); removeReplay(); removeOther();
  assert.equal(previews.watchers.size, 0);
  const replacement = [], removeReplacement = previews.watch(job.id, frame => replacement.push(frame));
  remove();
  t.mock.timers.tick(150); previews.update(job, progress(3));
  assert.equal(received.length, 2);
  assert.deepEqual(replacement.map(frame => frame.stepIndex), [2, 3]);
  removeReplacement();
});

test('clear只清帧且订阅继续，空图/超限/非running不产生预览', t => {
  t.mock.timers.enable({ apis: ['Date'], now: 1800000000000 });
  const previews = new JobPreviews(), received = [];
  const remove = previews.watch(job.id, frame => received.push(frame));
  previews.update(job, progress(0, Buffer.alloc(512 * 1024)));
  previews.clear(job.id); assert.equal(previews.frames.has(job.id), false);
  previews.update(job, progress(1));
  assert.deepEqual(received.map(frame => frame.stepIndex), [0, 1]);
  t.mock.timers.tick(150);
  previews.update(job, progress(2, Buffer.alloc(512 * 1024 + 1)));
  previews.update(job, progress(3, Buffer.alloc(0)));
  previews.update({ ...job, status: 'done' }, progress(4));
  assert.equal(received.length, 2); assert.equal(previews.frames.get(job.id).stepIndex, 1);
  remove();
});

test('同步/异步及回放回调错误隔离，后续观察者仍收到预览', async () => {
  const previews = new JobPreviews(), received = [];
  const removeSync = previews.watch(job.id, () => { throw new Error('synthetic synchronous observer'); });
  const removeAsync = previews.watch(job.id, async () => { throw new Error('synthetic asynchronous observer'); });
  const removeGood = previews.watch(job.id, frame => received.push(frame));
  assert.doesNotThrow(() => previews.update(job, progress(0)));
  let removeReplay;
  assert.doesNotThrow(() => { removeReplay = previews.watch(job.id, () => { throw new Error('synthetic replay observer'); }); });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(received.length, 1);
  removeSync(); removeAsync(); removeGood(); removeReplay();
});

test('门户关闭只结束门户SSE，Native观察者在关闭宽限内继续接收', t => {
  t.mock.timers.enable({ apis: ['Date'], now: 1800000000000 });
  const previews = new JobPreviews(), chunks = [], native = [];
  const response = new EventEmitter();
  Object.assign(response, { writableEnded: false, destroyed: false, writableNeedDrain: false,
    writeHead() {}, write(chunk) { chunks.push(chunk); }, end() { this.writableEnded = true; this.emit('close'); } });
  t.after(() => { previews.closeSubscriptions(); remove(); });
  previews.subscribe(job.userToken, response);
  const remove = previews.watch(job.id, frame => native.push(frame));
  previews.update(job, progress(0));
  assert.ok(chunks.some(chunk => chunk.includes('data:image/jpeg;base64,AA==')));
  previews.closeSubscriptions(); previews.closeSubscriptions();
  assert.equal(response.writableEnded, true); assert.equal(previews.listeners.size, 0);
  const before = chunks.length;
  t.mock.timers.tick(150); previews.update(job, progress(1));
  assert.equal(chunks.length, before); assert.deepEqual(native.map(frame => frame.stepIndex), [0, 1]);
  let lateEnded = false;
  previews.subscribe(job.userToken, { end() { lateEnded = true; } });
  assert.equal(lateEnded, true);
  remove(); assert.equal(previews.watchers.size, 0);
});
