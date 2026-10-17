import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { nativeImageMsgpack, nativeImageSse, nativeImagesMsgpack, nativeImagesJson } from '../../server/native-stream.js';
import { readNativeEvents, readNativeSseEvents } from '../helpers/native-stream-client.mjs';

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

test('实时中间帧保留原始步骤与批次编号，缺少有效步骤不能伪造预览', () => {
  const event = { event_type: 'intermediate', samp_ix: 2, step_ix: 37, image: Buffer.from([0, 255, 2]) };
  const [decoded] = readNativeEvents(nativeImageMsgpack(event));
  assert.equal(decoded.step_ix, 37); assert.equal(decoded.samp_ix, 2);
  assert.deepEqual(Buffer.from(decoded.image), event.image);
  for (const step_ix of [undefined, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
    assert.throws(() => nativeImageMsgpack({ ...event, step_ix }));
  }
  assert.throws(() => nativeImageMsgpack({ ...event, event_type: 'heartbeat' }));
  assert.throws(() => nativeImageMsgpack({ ...event, samp_ix: 4 }));
  // 图片本身恰好64MiB时，加上事件字段已越界。
  assert.throws(() => nativeImageMsgpack({ event_type: 'final', samp_ix: 0, image: Buffer.alloc(64 * 1024 * 1024) }), /单帧限制/);
});

test('SSE 独立客户端还原 PNG/WebP，保留预览步骤与多帧顺序，final 后无额外终止标记', () => {
  const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
  const webp = readFileSync(new URL('../fixtures/synthetic.webp', import.meta.url));
  const inputs = [
    { event_type: 'intermediate', samp_ix: 0, step_ix: 37, image: png },
    { event_type: 'final', samp_ix: 0, image: png },
    { event_type: 'final', samp_ix: 1, image: webp }
  ];
  const wire = inputs.flatMap(input => [...nativeImageSse(input)]).join('');
  const events = readNativeSseEvents(Buffer.from(wire));
  assert.deepEqual(events, [
    { event_type: 'intermediate', samp_ix: 0, step_ix: 37, image: png.toString('base64') },
    { event_type: 'final', samp_ix: 0, image: png.toString('base64') },
    { event_type: 'final', samp_ix: 1, image: webp.toString('base64') }
  ]);
  events.forEach((event, index) => assert.deepEqual(Buffer.from(event.image, 'base64'), inputs[index].image));
  assert.equal(wire.includes('data:image/'), false);
  assert.equal(wire.includes('[DONE]'), false);
  assert.ok(wire.endsWith('"}\n\n'));
});

test('SSE 错误 code 为字符串，中文与换行不能注入新事件，沿用错误消息长度限制', () => {
  const message = '下一张失败\r\nevent: final\ndata: {"event_type":"final"}\n\n仍是同一错误';
  const wire = [...nativeImageSse({ event_type: 'error', samp_ix: 1, code: 429, message })].join('');
  assert.deepEqual(readNativeSseEvents(Buffer.from(wire)), [
    { event_type: 'error', samp_ix: 1, message, code: '429' }
  ]);
  for (const code of [undefined, '500\ndata: injected']) {
    const [event] = readNativeSseEvents(Buffer.from([...nativeImageSse({ event_type: 'error', samp_ix: 0, code })].join('')));
    assert.equal(event.code, code ?? '500');
    assert.equal(event.message, '生成失败');
  }
  const [event] = readNativeSseEvents(Buffer.from([...nativeImageSse({ event_type: 'error', samp_ix: 0, message: '错'.repeat(2001) })].join('')));
  assert.equal(event.message, '错'.repeat(2000));
});

test('SSE 在输出任何前缀前拒绝无效类型、图片、编号、步骤及过大图片', () => {
  const valid = { event_type: 'intermediate', samp_ix: 2, step_ix: 0, image: Buffer.from([1, 2, 3]) };
  const invalid = [
    { event_type: 'heartbeat' },
    ...[undefined, Buffer.alloc(0), 'not binary'].map(image => ({ image })),
    ...[undefined, -1, 4, 0.5].map(samp_ix => ({ samp_ix })),
    ...[undefined, -1, 0.5, Number.MAX_SAFE_INTEGER + 1].map(step_ix => ({ step_ix })),
    { image: Buffer.alloc(64 * 1024 * 1024 + 1) }
  ];
  for (const override of invalid) {
    const output = nativeImageSse({ ...valid, ...override });
    assert.throws(() => output.next());
    assert.equal(output.next().done, true);
  }
  const error = new Error('code conversion failed');
  assert.throws(() => nativeImageSse({ event_type: 'error', samp_ix: 0, code: { toString() { throw error; } } }).next(), thrown => thrown === error);
});

test('SSE base64 分块跨三字节与 48 KiB 边界精确还原，单块不超过 64 KiB', () => {
  for (const length of [1, 2, 3, 49151, 49152, 49153, 98303, 98304, 98305]) {
    const image = Buffer.from(Array.from({ length }, (_, index) => index % 256));
    const chunks = [...nativeImageSse({ event_type: 'final', samp_ix: 0, image })];
    assert.ok(chunks.every(chunk => typeof chunk === 'string' && Buffer.byteLength(chunk) <= 65536));
    assert.equal(chunks.length, 2 + Math.ceil(length / 49152));
    assert.ok(chunks.slice(1, -2).every(chunk => !chunk.includes('=')), '只有最后一个 base64 块可以包含 padding');
    const [event] = readNativeSseEvents(Buffer.from(chunks.join('')));
    assert.deepEqual(Buffer.from(event.image, 'base64'), image);
  }
});

test('SSE 提前结束消费后不编码后续图片块或输出完整帧', () => {
  const image = Buffer.alloc(98305, 0x89);
  const subarray = image.subarray.bind(image);
  let reads = 0;
  image.subarray = (...args) => { reads++; return subarray(...args); };
  const output = nativeImageSse({ event_type: 'final', samp_ix: 0, image });
  const prefix = output.next().value;
  assert.equal(reads, 0);
  const first = output.next().value;
  assert.equal(reads, 1); assert.equal(first.length, 65536);
  assert.equal(output.return().done, true);
  assert.equal(output.next().done, true); assert.equal(reads, 1);
  assert.throws(() => readNativeSseEvents(Buffer.from(prefix + first)), /未完整结束/);
});

test('独立 SSE 客户端拒绝截断、事件名不匹配与额外终止标记', () => {
  assert.throws(() => readNativeSseEvents(Buffer.from('event: final\ndata: {}\n')), /未完整结束/);
  assert.throws(() => readNativeSseEvents(Buffer.from('event: final\ndata: {"event_type":"error"}\n\n')), /类型不一致/);
  assert.throws(() => readNativeSseEvents(Buffer.from('data: [DONE]\n\n')));
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
