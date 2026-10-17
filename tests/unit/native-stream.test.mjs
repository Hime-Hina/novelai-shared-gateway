import test from 'node:test';
import assert from 'node:assert/strict';
import { nativeImagesMsgpack, nativeImagesJson } from '../../server/native-stream.js';
import { readNativeEvents } from '../helpers/native-stream-client.mjs';

test('NAI 原生 MessagePack 由独立客户端解码，保留二进制和零号样本', () => {
  for (const length of [8, 255, 256, 65535, 65536]) {
    const image = Buffer.alloc(length, 0x89);
    const frames = nativeImagesMsgpack([image]);
    assert.equal(frames.readUInt32BE(0), frames.length - 4);
    const [event] = readNativeEvents(frames);
    assert.equal(event.event_type, 'final');
    assert.equal(event.samp_ix, 0);
    assert.ok(event.image instanceof Uint8Array);
    assert.deepEqual(Buffer.from(event.image), image);
  }
});

test('四张图按顺序分帧，部分成功保留已完成图片并发送中文错误帧', () => {
  const images = [0, 1, 2, 3].map(value => Buffer.from([value, 0, 255]));
  const events = readNativeEvents(nativeImagesMsgpack(images));
  assert.deepEqual(events.map(event => event.samp_ix), [0, 1, 2, 3]);
  for (const [index, event] of events.entries()) assert.deepEqual(Buffer.from(event.image), images[index]);
  const partial = readNativeEvents(nativeImagesMsgpack(images.slice(0, 1), { index: 1, message: '下一张失败，已退回本张额度' }));
  assert.equal(partial.length, 2);
  assert.equal(partial[0].event_type, 'final');
  assert.deepEqual(partial[1], { event_type: 'error', samp_ix: 1, message: '下一张失败，已退回本张额度' });
});

test('缺少图片、越界数量与不连续的失败编号不能伪装成功帧', () => {
  for (const images of [[], [Buffer.alloc(0)], ['not binary'], Array(5).fill(Buffer.from('x'))]) assert.throws(() => nativeImagesMsgpack(images));
  assert.throws(() => nativeImagesMsgpack([Buffer.from('x')], { index: 2, message: 'bad index' }));
});

async function* images(...values) { yield* values; }
async function collect(source) {
  const chunks = [];
  for await (const chunk of source) chunks.push(chunk);
  return chunks;
}

test('原生JSON保留逐图二进制、index、零和最大seed，缺省seed不虚构数值', async () => {
  const inputs = [
    { buffer: Buffer.from([0, 255, 1]), index: 0, seed: 0 },
    { buffer: Buffer.from('第二张图片'), index: 1, seed: 4294967295 },
    { buffer: Buffer.from([137, 80, 78, 71]), index: 2 }
  ];
  const summary = { requested: 4, completed: 3, remaining: 1, billing: 'independent_single_images',
    results: [{ index: 0, status: 'done', filename: 'image_0.png' }, { index: 3, status: 'failed', message: '后续失败' }] };
  const decoded = JSON.parse((await collect(nativeImagesJson(images(...inputs), summary))).join(''));
  assert.deepEqual(Object.keys(decoded), ['images', 'requested', 'completed', 'remaining', 'billing', 'results']);
  const { images: output, ...metadata } = decoded;
  assert.deepEqual(metadata, summary); assert.equal(output.length, 3);
  output.forEach((entry, index) => {
    assert.deepEqual(Buffer.from(entry.image, 'base64'), inputs[index].buffer);
    assert.equal(entry.index, inputs[index].index);
    assert.equal(entry.seed, inputs[index].seed);
  });
  assert.equal(Object.hasOwn(output[2], 'seed'), false);
  const single = JSON.parse((await collect(nativeImagesJson(images(inputs[0])))).join(''));
  assert.deepEqual(Object.keys(single), ['images']);
});

test('JSON图片按有限块输出，跨三字节及48KiB边界仍可精确还原', async () => {
  for (const length of [1, 2, 3, 49151, 49152, 49153, 98303, 98304, 98305]) {
    const buffer = Buffer.from(Array.from({ length }, (_, index) => index % 256));
    const chunks = await collect(nativeImagesJson(images({ buffer, index: 0 })));
    assert.ok(chunks.every(chunk => typeof chunk === 'string' && Buffer.byteLength(chunk) <= 65536));
    assert.deepEqual(Buffer.from(JSON.parse(chunks.join('')).images[0].image, 'base64'), buffer);
    if (length > 49152) assert.ok(chunks.length > 6, '大图片必须分块输出');
  }
});

test('首项读取完成前不输出，按消费进度读取后续图片，提前结束会关闭source', async () => {
  let release, firstRead = false, secondRead = false, closed = false;
  const gate = new Promise(resolve => { release = resolve; });
  async function* source() {
    try {
      firstRead = true; await gate;
      yield { buffer: Buffer.from('first'), index: 0 };
      secondRead = true; yield { buffer: Buffer.from('second'), index: 1 };
    } finally { closed = true; }
  }
  const output = nativeImagesJson(source());
  let emitted = false; const pending = output.next().then(value => { emitted = true; return value; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(firstRead, true); assert.equal(emitted, false); assert.equal(secondRead, false);
  release(); assert.equal((await pending).value, '{"images":[');
  assert.equal(secondRead, false); await output.return();
  assert.equal(closed, true); assert.equal(secondRead, false);
});

test('初次与后续source错误原样抛出，首项失败或空source没有成功正文', async () => {
  const initial = new Error('first file failed');
  async function* failedFirst() { throw initial; }
  const first = nativeImagesJson(failedFirst());
  await assert.rejects(first.next(), error => error === initial);
  await assert.rejects(nativeImagesJson(images()).next(), /缺少图片/);
  await assert.rejects(nativeImagesJson(images({ buffer: Buffer.alloc(0), index: 0 })).next(), /缺少二进制图片/);
  const later = new Error('second file failed'), chunks = [];
  async function* failedLater() { yield { buffer: Buffer.from('complete first'), index: 0 }; throw later; }
  await assert.rejects(async () => {
    for await (const chunk of nativeImagesJson(failedLater())) chunks.push(chunk);
  }, error => error === later);
  assert.ok(chunks.join('').includes(Buffer.from('complete first').toString('base64')));
  assert.throws(() => JSON.parse(chunks.join('')));
});
