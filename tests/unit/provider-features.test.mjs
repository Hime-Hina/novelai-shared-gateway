import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { deflateRawSync, deflateSync, crc32 } from 'node:zlib';
import { EventEmitter } from 'node:events';
import { encode } from '@msgpack/msgpack';
import { generationPrice, sizeMap } from '../../public/generation-pricing.js';
import { generateNovelAiImage, normalizeNovelAiRequest, __providerInternals } from '../../server/providers.js';
import { parseNativeImageRequest, parseNativeAuxiliaryRequest, validateImageBytes } from '../../server/native-image.js';
import { nativeImagesMsgpack } from '../../server/native-stream.js';

const account = { token: 'offline-provider-feature-token' };
const env = { NOVELAI_API_URL: 'https://configured-upstream.invalid/' };
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
const webp = readFileSync(new URL('../fixtures/synthetic.webp', import.meta.url));
const originalFetch = globalThis.fetch;

async function captured(run, response = () => new Response(png, { headers: { 'content-type': 'image/png' } })) {
  const calls = [];
  globalThis.fetch = async (url, options) => {
    calls.push({ url, options, body: JSON.parse(options.body) });
    return response();
  };
  try { return { value: await run(), calls }; }
  finally { globalThis.fetch = originalFetch; }
}

function request(action = 'generate') {
  return { action, model: 'nai-diffusion-4-5-full', prompt: 'a blue test circle',
    width: 1024, height: 1024, steps: 23, seed: 42,
    nativeParameters: { width: 2048, height: 2048, steps: 50, seed: 9, n_samples: 4,
      scale: 5.5, sampler: 'k_euler', strength: 0.43, noise: 0.12,
      image: png.toString('base64'), mask: png.toString('base64'),
      extra_noise_seed: 77, add_original_image: true,
      reference_image_multiple_cached: [{ data: 'encoded-vibe', cache_secret_key: 'offline-cache-key' }],
      reference_strength_multiple: [0.6],
      director_reference_images: [png.toString('base64')],
      director_reference_strengths: [0.5],
      director_reference_information_extracted: [1],
      director_reference_descriptions: [{ caption: { base_caption: 'character&style', char_captions: [] } }],
      v4_prompt: { caption: { base_caption: 'a blue test circle', char_captions: [] }, use_coords: false },
      stream: 'msgpack' }
  };
}

test('图生图和局部重绘按原动作转发，保留图像、遮罩和已核价的参考强度', async () => {
  for (const action of ['generate', 'img2img', 'infill']) {
    const input = request(action);
    if (action === 'infill') input.model += '-inpainting';
    const before = structuredClone(input);
    const { calls, value } = await captured(() => generateNovelAiImage(input, account, env, { forceStream: false }));
    assert.deepEqual(input, before);
    assert.equal(value.mimeType, 'image/png');
    assert.deepEqual(value.buffer, png);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].url, 'https://configured-upstream.invalid/ai/generate-image');
    assert.equal(calls[0].body.action, action);
    assert.equal(calls[0].body.model, input.model);
    const expected = { ...before.nativeParameters, width: 1024, height: 1024, steps: 23, seed: 42, n_samples: 1 };
    delete expected.stream;
    assert.deepEqual(calls[0].body.parameters, expected);
  }
});

test('V3 动作维持非流式，V4 流式也保留 img2img 动作与收费参数', async () => {
  const v3 = request('img2img'); v3.model = 'nai-diffusion-3';
  const v4 = request('img2img'); v4.model = 'nai-diffusion-4-full';
  const { calls } = await captured(async () => {
    await generateNovelAiImage(v3, account, env);
    await generateNovelAiImage(v4, account, env);
  }, () => Response.json({ image: png.toString('base64') }));
  assert.deepEqual(calls.map(c => c.url), [
    'https://configured-upstream.invalid/ai/generate-image',
    'https://configured-upstream.invalid/ai/generate-image-stream'
  ]);
  for (const call of calls) {
    assert.equal(call.body.action, 'img2img');
    assert.equal(call.body.parameters.strength, 0.43);
    assert.equal(call.body.parameters.n_samples, 1);
    assert.equal(call.body.parameters.image, png.toString('base64'));
  }
  assert.equal(calls[1].body.parameters.stream, 'msgpack');
});

test('Vibe 编码使用固定上游、共享身份和取消信号，二进制完整返回', async () => {
  const signal = new AbortController().signal;
  const parameters = { image: png.toString('base64'), mask: png.toString('base64'), information_extracted: 0.8, model: 'nai-diffusion-4-5-full' };
  const encoding = Buffer.from([0, 255, 3, 4, 5, 128]);
  const { calls, value } = await captured(() => generateNovelAiImage({ operation: 'encode-vibe', action: 'encode-vibe', operationParameters: parameters }, account, env, { signal, forceStream: true }),
    () => new Response(encoding, { headers: { 'content-type': 'application/octet-stream' } }));
  assert.deepEqual(value, { buffer: encoding, mimeType: 'application/octet-stream' });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, 'https://configured-upstream.invalid/ai/encode-vibe');
  assert.equal(calls[0].options.headers.authorization, `Bearer ${account.token}`);
  assert.equal(calls[0].options.signal, signal);
  assert.deepEqual(calls[0].body, parameters);
});

test('放大维持官网新版参数并返回图片，不使用客户端URL或自动重试旧付费接口', async () => {
  const parameters = { image: png.toString('base64'), model: 'nai-diffusion-5-curated', declared_blur_sigma: 0 };
  const input = { operation: 'upscale', operationParameters: parameters, url: 'https://untrusted.invalid/steal' };
  const { calls, value } = await captured(() => generateNovelAiImage(input, account, env));
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, 'https://configured-upstream.invalid/ai/upscale');
  assert.deepEqual(calls[0].body, parameters);
  assert.equal(value.mimeType, 'image/png');
  assert.deepEqual(value.buffer, png);
  await captured(async () => {
    await assert.rejects(generateNovelAiImage(input, account, env), error => error.upstreamStatus === 402);
  }, () => new Response('insufficient credits', { status: 402 }));
});

test('编码和放大错误保留上游状态，空响应及伪装成功的编码JSON不结算成功', async () => {
  for (const operation of ['encode-vibe', 'upscale']) {
    const input = { operation, operationParameters: { image: png.toString('base64'), model: 'nai-diffusion-4-5-full' } };
    for (const status of [400, 401, 402, 422, 429, 500]) {
      const { calls } = await captured(async () => {
        await assert.rejects(generateNovelAiImage(input, account, env), error => error.upstreamStatus === status);
      }, () => new Response('offline failure', { status }));
      assert.equal(calls.length, 1);
    }
    await captured(async () => {
      await assert.rejects(generateNovelAiImage(input, account, env), /empty operation result/);
    }, () => new Response());
  }
  await captured(async () => {
    await assert.rejects(generateNovelAiImage({ operation: 'encode-vibe', operationParameters: {} }, account, env), /binary encoding data/);
  }, () => Response.json({ error: 'invalid image' }));
});

test('未知操作或动作在联网前拒绝', async () => {
  const { calls } = await captured(async () => {
    await assert.rejects(generateNovelAiImage({ operation: '../user/data' }, account, env), /Unsupported NovelAI operation/);
    await assert.rejects(generateNovelAiImage({ action: 'unknown' }, account, env), /Unsupported NovelAI image action/);
    await assert.rejects(generateNovelAiImage({ operation: 'encode-vibe' }, account, env), /parameters are required/);
    await assert.rejects(generateNovelAiImage({ operation: 'encode-vibe', operationParameters: {} }, {}, env), /No enabled NovelAI account/);
  });
  assert.equal(calls.length, 0);
});

test('Retry-After 按收头时间解析整秒和 HTTP-date，无效或过期不输出，不截断长等待', async t => {
  const now = Date.UTC(2026, 9, 17), future = now + 300000;
  t.mock.method(Date, 'now', () => now);
  const valid = [['30', now + 30000], ['300', future], ['3600', now + 3600000],
    ['Sat, 17 Oct 2026 00:05:00 GMT', future], ['Saturday, 17-Oct-26 00:05:00 GMT', future], ['Sat Oct 17 00:05:00 2026', future]];
  const invalid = ['', '-1', '0', '+30', '1.5', '1e3', '30, 60', 'tomorrow', '99999999999999999999',
    '2026-10-17T00:05:00Z', 'Fri, 17 Oct 2026 00:05:00 GMT', 'Sat, 31 Feb 2099 00:05:00 GMT',
    'Sat, 17 Oct 2026 00:00:00 GMT', 'Fri, 16 Oct 2026 23:59:59 GMT'];
  for (const [header, deadline] of [...valid, ...invalid.map(value => [value, undefined])]) {
    const timing = {};
    await captured(() => generateNovelAiImage(request(), account, env, { forceStream: false, onUpstreamTiming: value => Object.assign(timing, value) }),
      () => new Response(png, { headers: { 'content-type': 'image/png', 'retry-after': header } }));
    assert.equal(timing.upstreamHttpStatus, 200);
    assert.equal(timing.upstreamHeadersAt, new Date(now).toISOString());
    assert.equal(timing.upstreamRetryAfterUntil, deadline === undefined ? undefined : new Date(deadline).toISOString(), header);
    if (deadline === undefined) assert.equal(Object.hasOwn(timing, 'upstreamRetryAfterUntil'), false);
  }
});

test('普通、流式、放大和编码保留收头证据，429 后断流和 200 坏产物都不丢状态/截止', async t => {
  const start = Date.UTC(2026, 9, 17), until = new Date(start + 300000).toISOString();
  let now = start;
  t.mock.method(Date, 'now', () => now);
  const encoding = Buffer.from([0, 255, 3, 4]);
  const variants = [
    { request: request(), forceStream: false, bytes: png, type: 'image/png' },
    { request: request(), forceStream: true, bytes: nativeImagesMsgpack([png]), type: 'application/octet-stream' },
    { request: { operation: 'upscale', operationParameters: {} }, bytes: png, type: 'image/png' },
    { request: { operation: 'encode-vibe', operationParameters: {} }, bytes: encoding, type: 'application/octet-stream' }
  ];
  for (const variant of variants) {
    const timing = {}, options = { forceStream: variant.forceStream, onUpstreamTiming(value) {
      Object.assign(timing, value);
      // 模拟响应体读取耗时，不能在错误落地时用新时钟重新计算秒数。
      if (value.upstreamHeadersAt) now = start + 90000;
    } };
    const call = () => generateNovelAiImage(variant.request, account, env, options);
    now = start;
    const success = await captured(call, () => new Response(variant.bytes, { headers: { 'content-type': variant.type, 'retry-after': '300' } }));
    assert.equal(success.calls.length, 1); assert.equal(timing.upstreamHttpStatus, 200); assert.equal(timing.upstreamRetryAfterUntil, until);
    now = start;
    await captured(() => assert.rejects(call(), error => error.upstreamStatus === 429 && error.upstreamRetryAfterUntil === until),
      () => new Response('synthetic limit', { status: 429, headers: { 'retry-after': '300' } }));
    now = start;
    const broken = Object.assign(new Error('synthetic body reset', { cause: new Error('socket closed') }), { code: 'ECONNRESET' });
    await captured(() => assert.rejects(call(), error => error === broken && error.code === 'ECONNRESET' && error.cause.message === 'socket closed'
      && error.upstreamStatus === 429 && error.upstreamRetryAfterUntil === until),
    () => new Response(new ReadableStream({ start(controller) { controller.error(broken); } }), { status: 429, headers: { 'retry-after': '300' } }));
    assert.equal(timing.upstreamHttpStatus, 429); assert.equal(timing.upstreamRetryAfterUntil, until);
    now = start;
    await captured(() => assert.rejects(call(), error => error.upstreamStatus === 200 && error.upstreamRetryAfterUntil === until),
      () => new Response('invalid artifact', { headers: { 'content-type': variant.type, 'retry-after': '300' } }));
    const noHeaders = Object.freeze(new Error('synthetic network error before headers'));
    await captured(() => assert.rejects(call(), error => error === noHeaders && error.upstreamStatus === undefined && error.upstreamRetryAfterUntil === undefined),
      () => { throw noHeaders; });
  }
});

function msgpack(value) {
  if (Buffer.isBuffer(value)) return Buffer.concat([Buffer.from([0xc4, value.length]), value]);
  if (typeof value === 'string') {
    const text = Buffer.from(value);
    return Buffer.concat([Buffer.from([0xa0 | text.length]), text]);
  }
  const fields = Object.entries(value);
  return Buffer.concat([Buffer.from([0x80 | fields.length]), ...fields.flatMap(([key, item]) => [msgpack(key), msgpack(item)])]);
}

function streamFrame(payload) {
  const length = Buffer.alloc(4); length.writeUInt32BE(payload.length);
  return Buffer.concat([length, payload]);
}

function streamBody(events, encodeEvent = msgpack) {
  return Buffer.concat(events.map(event => streamFrame(encodeEvent(event))));
}

test('流式预览后报错或缺失最终结果必须失败，不能把预览当作已完成扣费', async () => {
  const intermediate = { event_type: 'intermediate', image: png };
  for (const events of [[intermediate], [intermediate, { event_type: 'error', message: 'offline stream failure' }]]) {
    await captured(async () => {
      await assert.rejects(generateNovelAiImage(request('img2img'), account, env), /without a final image|stream error/);
    }, () => new Response(streamBody(events), { headers: { 'content-type': 'application/octet-stream' } }));
  }
  const { value } = await captured(() => generateNovelAiImage(request('img2img'), account, env),
    () => new Response(streamBody([intermediate, { event_type: 'final', image: png }]), { headers: { 'content-type': 'application/octet-stream' } }));
  assert.deepEqual(value.buffer, png);
});

test('MessagePack 拒绝恶意容器/文本/二进制长度、原型键及所有 extension', async () => {
  const declared = (marker, size) => {
    const bytes = Buffer.alloc(5);
    bytes[0] = marker; bytes.writeUInt32BE(size, 1);
    return bytes;
  };
  const proto = encode(JSON.parse('{"__proto__":{"event_type":"final"}}'));
  const invalid = [
    [declared(0xdd, 0xffffffff), /maxArrayLength/],
    [declared(0xdd, 1025), /maxArrayLength/],
    [declared(0xdf, 1025), /maxMapLength/],
    [declared(0xdb, 64 * 1024 + 1), /maxStrLength/],
    [declared(0xc6, 64 * 1024 * 1024 + 1), /maxBinLength/],
    [Buffer.from([0xd4, 1, 0]), /maxExtLength/],
    [Buffer.from([0xc7, 0, 1]), /Unsupported NovelAI msgpack extension/],
    [proto, /__proto__/],
    [Buffer.from([0x81, 0x90, 0xc0]), /key must be string or number/]
  ];
  for (const [payload, expected] of invalid) {
    const bytes = streamFrame(payload);
    for (const readable of [true, false]) {
      await captured(() => assert.rejects(generateNovelAiImage(request(), account, env), expected), () => readable
        ? new Response(bytes, { headers: { 'content-type': 'application/octet-stream' } })
        : { ok: true, headers: new Headers(), arrayBuffer: async () => bytes });
    }
  }
  assert.equal(Object.prototype.event_type, undefined);
});

test('原始安全 int64 step_ix 保留为 stepIndex，新预览不被相同显示百分比丢弃', async () => {
  const notifications = [];
  const events = [
    { event_type: 'intermediate', image: png, step_ix: 0n, total_steps: 1n },
    { event_type: 'intermediate', image: Buffer.concat([png, Buffer.from([1])]), step_ix: 1n, total_steps: 1n },
    { event_type: 'intermediate', image: png, step_ix: 9007199254740993n, progress: 9007199254740993n },
    { event_type: 'intermediate', image: png, step_ix: -1n },
    { event_type: 'intermediate', image: png, step_ix: 0.5 },
    { event_type: 'intermediate', image: 'not binary', step_ix: 2n },
    { event_type: 'final', image: png, step_ix: 23n }
  ];
  const { value } = await captured(() => generateNovelAiImage(request(), account, env, { onProgress: progress => notifications.push(progress) }),
    () => new Response(streamBody(events, value => encode(value, { useBigInt64: true })), { headers: { 'content-type': 'application/octet-stream' } }));
  assert.deepEqual(value.buffer, png);
  assert.equal(notifications.length, 6);
  assert.deepEqual(notifications.slice(0, 2).map(item => [item.percent, item.step, item.stepIndex]), [[98, 1, 0], [98, 1, 1]]);
  assert.deepEqual(notifications[1].previewBuffer, events[1].image);
  for (const item of notifications.slice(2, 5)) {
    assert.equal(Object.hasOwn(item, 'stepIndex'), false);
    assert.deepEqual(item.previewBuffer, png);
  }
  assert.equal(notifications.at(-1).percent, 100);
  assert.equal(notifications.at(-1).stepIndex, 23);
  assert.equal(Object.hasOwn(notifications.at(-1), 'previewBuffer'), false);
});

test('只有元数据的进度更新不把旧预览绑定到新 stepIndex', async () => {
  const notifications = [];
  const events = [
    { event_type: 'intermediate', image: png, step_ix: 0 },
    { event_type: 'intermediate', step_ix: 1 },
    { event_type: 'final', image: png }
  ];
  await captured(() => generateNovelAiImage(request(), account, env, { onProgress: progress => notifications.push(progress) }),
    () => new Response(streamBody(events, encode), { headers: { 'content-type': 'application/octet-stream' } }));
  assert.equal(notifications.length, 3);
  assert.equal(notifications[0].stepIndex, 0);
  assert.deepEqual(notifications[0].previewBuffer, png);
  assert.equal(notifications[1].stepIndex, 1);
  assert.equal(Object.hasOwn(notifications[1], 'previewBuffer'), false);
  assert.equal(Object.hasOwn(notifications[2], 'stepIndex'), false);
});

test('单帧上限只计算 payload，分片中额外的四字节前缀不会误拒', async () => {
  const limit = 64 * 1024 * 1024;
  // 两次 padding 都使用 bin32，准确求出固定元数据开销；只保留一份大 payload。
  const overhead = encode({ event_type: 'final', image: png, padding: Buffer.alloc(65536) }).length - 65536;
  const payload = encode({ event_type: 'final', image: png, padding: Buffer.alloc(limit - overhead) }, { initialBufferSize: limit });
  assert.equal(payload.length, limit);
  const prefix = Buffer.alloc(4); prefix.writeUInt32BE(payload.length);
  const { value } = await captured(() => generateNovelAiImage(request(), account, env), () => new Response(new ReadableStream({
    start(controller) {
      controller.enqueue(prefix);
      controller.enqueue(payload.subarray(0, -3));
      controller.enqueue(payload.subarray(-3));
      controller.close();
    }
  }), { headers: { 'content-type': 'application/octet-stream' } }));
  assert.deepEqual(value.buffer, png);
  const oversized = Buffer.alloc(4); oversized.writeUInt32BE(limit + 1);
  await captured(() => assert.rejects(generateNovelAiImage(request(), account, env), /Invalid NovelAI msgpack stream frame/),
    () => new Response(oversized, { headers: { 'content-type': 'application/octet-stream' } }));
});

function imageZip(bytes, { method = 0, uncompressedSize = bytes.length, filename = 'image.png' } = {}) {
  const name = Buffer.from(filename);
  const compressed = method === 8 ? deflateRawSync(bytes) : bytes;
  const local = Buffer.alloc(30);
  local.writeUInt32LE(0x04034b50); local.writeUInt16LE(method, 8);
  local.writeUInt32LE(crc32(bytes), 14); local.writeUInt32LE(compressed.length, 18);
  local.writeUInt32LE(uncompressedSize, 22); local.writeUInt16LE(name.length, 26);
  const central = Buffer.alloc(46);
  central.writeUInt32LE(0x02014b50); central.writeUInt16LE(method, 10);
  central.writeUInt32LE(crc32(bytes), 16); central.writeUInt32LE(compressed.length, 20);
  central.writeUInt32LE(uncompressedSize, 24); central.writeUInt16LE(name.length, 28);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50); end.writeUInt16LE(1, 8); end.writeUInt16LE(1, 10);
  end.writeUInt32LE(central.length + name.length, 12);
  end.writeUInt32LE(local.length + name.length + compressed.length, 16);
  return Buffer.concat([local, name, compressed, central, name, end]);
}

test('有效 WebP 经原二进制、JSON、data URL、ZIP 和 MsgPack 保留字节与原生输出格式', async () => {
  assert.deepEqual(validateImageBytes(webp), { width: 2, height: 2, mimeType: 'image/webp', byteLength: 34 });
  const input = parseNativeImageRequest({ input: 'synthetic four color pixels', model: 'nai-diffusion-5-full',
    parameters: { width: 1024, height: 1024, steps: 23, seed: 42, image_format: 'webp' } });
  const before = structuredClone(input);
  const responses = [
    () => new Response(webp, { headers: { 'content-type': 'image/webp' } }),
    () => new Response(webp, { headers: { 'content-type': 'application/octet-stream' } }),
    () => Response.json({ images: [{ image: webp.toString('base64') }] }),
    () => Response.json({ image: 'data:image/webp;base64,' + webp.toString('base64') }),
    () => new Response(imageZip(webp, { filename: 'image.webp' }), { headers: { 'content-type': 'application/zip' } })
  ];
  for (const response of responses) {
    const { value, calls } = await captured(() => generateNovelAiImage(input, account, env, { forceStream: false }), response);
    assert.deepEqual(value, { buffer: webp, mimeType: 'image/webp' });
    assert.equal(calls.length, 1); assert.equal(calls[0].body.parameters.image_format, 'webp');
    assert.match(calls[0].options.headers.accept, /(?:^|,)image\/webp(?:,|$)/);
  }
  const { value, calls } = await captured(() => generateNovelAiImage(input, account, env, { forceStream: true }),
    () => new Response(nativeImagesMsgpack([webp]), { headers: { 'content-type': 'application/octet-stream' } }));
  assert.deepEqual(value.buffer, webp); assert.equal(value.mimeType, 'image/webp');
  assert.equal(calls[0].body.parameters.image_format, 'webp'); assert.equal(calls[0].body.parameters.stream, 'msgpack');
  assert.deepEqual(input, before);
});

test('WebP 的 MIME 不匹配和截断仍在各传输边界拒绝', async () => {
  const responses = [
    () => new Response(webp, { headers: { 'content-type': 'image/png' } }),
    () => new Response(png, { headers: { 'content-type': 'image/webp' } }),
    () => Response.json({ image: 'data:image/png;base64,' + webp.toString('base64') }),
    () => new Response(webp.subarray(0, -1), { headers: { 'content-type': 'image/webp' } }),
    () => Response.json({ images: [{ image: webp.subarray(0, -1).toString('base64') }] }),
    () => new Response(imageZip(webp.subarray(0, -1), { filename: 'image.webp' }), { headers: { 'content-type': 'application/zip' } })
  ];
  for (const response of responses) {
    await captured(() => assert.rejects(generateNovelAiImage(request(), account, env, { forceStream: false })), response);
  }
  await captured(() => assert.rejects(generateNovelAiImage(request(), account, env, { forceStream: true })),
    () => new Response(nativeImagesMsgpack([webp.subarray(0, -1)]), { headers: { 'content-type': 'application/octet-stream' } }));
});

function grayscalePng(width, height) {
  const chunk = (name, data) => {
    const type = Buffer.from(name), length = Buffer.alloc(4), checksum = Buffer.alloc(4);
    length.writeUInt32BE(data.length); checksum.writeUInt32BE(crc32(Buffer.concat([type, data])));
    return Buffer.concat([length, type, data, checksum]);
  };
  const header = Buffer.alloc(13);
  header.writeUInt32BE(width); header.writeUInt32BE(height, 4); header[8] = 8;
  return Buffer.concat([Buffer.from('89504e470d0a1a0a', 'hex'), chunk('IHDR', header),
    chunk('IDAT', deflateSync(Buffer.alloc((width + 1) * height))), chunk('IEND', Buffer.alloc(0))]);
}

test('已接受的8192边长输入可取得两倍放大结果，上传上限和结果总像素限制不变', async () => {
  const inputImage = grayscalePng(8192, 128), outputImage = grayscalePng(16384, 256);
  const input = parseNativeAuxiliaryRequest({ image: inputImage.toString('base64'), scale: 2 }, 'upscale');
  assert.equal(input.width, 8192); assert.equal(input.height, 128);
  assert.throws(() => parseNativeAuxiliaryRequest({ image: outputImage.toString('base64') }, 'upscale'), error => error.statusCode === 422);
  const responses = [
    () => new Response(outputImage, { headers: { 'content-type': 'image/png' } }),
    () => new Response(outputImage, { headers: { 'content-type': 'application/octet-stream' } }),
    () => Response.json({ images: [{ image: outputImage.toString('base64') }] }),
    () => Response.json({ image: 'data:image/png;base64,' + outputImage.toString('base64') }),
    () => new Response(imageZip(outputImage), { headers: { 'content-type': 'application/zip' } })
  ];
  for (const response of responses) {
    const { value } = await captured(() => generateNovelAiImage(input, account, env), response);
    assert.deepEqual(value, { buffer: outputImage, mimeType: 'image/png' });
  }
  const { value } = await captured(() => generateNovelAiImage(request(), account, env),
    () => new Response(nativeImagesMsgpack([outputImage]), { headers: { 'content-type': 'application/octet-stream' } }));
  assert.deepEqual(value.buffer, outputImage);
  for (const invalid of [grayscalePng(16385, 1), grayscalePng(16384, 1025)]) {
    await captured(() => assert.rejects(generateNovelAiImage(input, account, env), /invalid or incomplete/),
      () => new Response(invalid, { headers: { 'content-type': 'image/png' } }));
  }
});

test('普通生成和放大统一拒绝 HTML、空内容、截断图片和伪造 JSON 图片', async () => {
  const damaged = Buffer.from(png); damaged[damaged.length - 1] ^= 1;
  const responses = [
    () => new Response('<html>upstream error</html>', { headers: { 'content-type': 'text/html' } }),
    () => new Response('<html>upstream error</html>', { headers: { 'content-type': 'image/png' } }),
    () => new Response(null, { headers: { 'content-type': 'image/png' } }),
    () => new Response(png.subarray(0, -4), { headers: { 'content-type': 'image/png' } }),
    () => new Response(damaged, { headers: { 'content-type': 'image/png' } }),
    () => Response.json({ image: 'not-an-image' }),
    () => Response.json({ image: 'data:text/html;base64,' + Buffer.from('<html/>').toString('base64') }),
    () => Response.json({ image: png.subarray(0, -4).toString('base64') }),
    () => new Response(png, { headers: { 'content-type': 'image/jpeg' } })
  ];
  for (const input of [request(), { operation: 'upscale', operationParameters: {} }]) {
    for (const response of responses) {
      await captured(() => assert.rejects(generateNovelAiImage(input, account, env, { forceStream: false }), error => error.statusCode !== 422), response);
    }
    const { value } = await captured(() => generateNovelAiImage(input, account, env, { forceStream: false }),
      () => Response.json({ image: 'data:image/png;base64,' + png.toString('base64') }));
    assert.deepEqual(value.buffer, png);
  }
});

test('ZIP 提取验证目录、CRC、解压大小和图片内容，压缩炸弹不返回成功', async () => {
  const call = () => generateNovelAiImage(request(), account, env, { forceStream: false });
  for (const method of [0, 8]) {
    const bytes = imageZip(png, { method });
    const { value } = await captured(call, () => new Response(bytes));
    assert.deepEqual(value.buffer, png);
  }
  const wrongCrc = imageZip(png);
  wrongCrc[30 + 9 + png.length + 16] ^= 1;
  const badResponses = [
    imageZip(Buffer.from('not an image')),
    imageZip(png.subarray(0, -4)),
    imageZip(png, { method: 99 }),
    imageZip(png, { uncompressedSize: 64 * 1024 * 1024 + 1 }),
    imageZip(png, { uncompressedSize: png.length + 1 }),
    imageZip(png).subarray(0, -1),
    wrongCrc,
    imageZip(Buffer.alloc(64 * 1024 * 1024 + 1), { method: 8, uncompressedSize: png.length })
  ];
  for (const bytes of badResponses) await captured(() => assert.rejects(call()), () => new Response(bytes));
});

test('流式最终帧必须包含完整图片，尾部截断或帧内尾随垃圾不能成功', async () => {
  const complete = streamBody([{ event_type: 'final', image: png }]);
  const extraWithinFrame = Buffer.concat([complete, Buffer.from([0])]);
  extraWithinFrame.writeUInt32BE(complete.readUInt32BE(0) + 1);
  const oversizedFrame = Buffer.alloc(4); oversizedFrame.writeUInt32BE(64 * 1024 * 1024 + 1);
  for (const bytes of [
    streamBody([{ event_type: 'final', image: Buffer.from('<html>bad</html>') }]),
    streamBody([{ event_type: 'final', image: png.subarray(0, -4) }]),
    streamBody([{ event_type: 'final', image: 'not binary' }]),
    Buffer.concat([complete, Buffer.from([0])]), extraWithinFrame, oversizedFrame
  ]) {
    // 同时覆盖真实流读取与旧 SOCKS 无 body 返回的兼容路径。
    for (const readable of [true, false]) {
      await captured(() => assert.rejects(generateNovelAiImage(request(), account, env)), () => readable
        ? new Response(bytes, { headers: { 'content-type': 'application/octet-stream' } })
        : { ok: true, headers: new Headers(), arrayBuffer: async () => bytes });
    }
  }
});

test('响应大小在读取过程中限制，拒绝超长声明并取消未读完的流', async () => {
  for (const declared of [true, false]) {
    let cancelled = false, reads = 0;
    const body = new ReadableStream({
      pull(controller) { reads++; controller.enqueue(Buffer.alloc(1024 * 1024)); },
      cancel() { cancelled = true; }
    });
    await captured(() => assert.rejects(generateNovelAiImage(request(), account, env, { forceStream: false }), /size limit/),
      () => new Response(body, { headers: declared ? { 'content-length': String(64 * 1024 * 1024 + 1) } : {} }));
    assert.equal(cancelled, true);
    assert.ok(reads <= (declared ? 1 : 66));
  }
  const encoding = Buffer.from([0, 255, 3, 4]);
  await captured(() => assert.rejects(generateNovelAiImage({ operation: 'encode-vibe', operationParameters: {} }, account, env), /Truncated/),
    () => new Response(encoding, { headers: { 'content-type': 'application/octet-stream', 'content-length': '10' } }));
});

test('Vibe 按二进制产物验证，拒绝伪装为 octet-stream 的错误文本而不猜测魔数', async () => {
  const call = () => generateNovelAiImage({ operation: 'encode-vibe', operationParameters: {} }, account, env);
  for (const bytes of ['upstream error', '<html>错误</html>', '{"error":"错误"}']) {
    await captured(() => assert.rejects(call(), /binary encoding data/),
      () => new Response(bytes, { headers: { 'content-type': 'application/octet-stream' } }));
  }
  for (const firstByte of [0, 60, 91, 123]) {
    const binary = Buffer.from([firstByte, 255, 3, 4, 5, 128]);
    for (const contentType of ['application/octet-stream', 'application/binary']) {
      const { value } = await captured(call, () => new Response(binary, { headers: { 'content-type': contentType } }));
      assert.deepEqual(value, { buffer: binary, mimeType: 'application/octet-stream' });
    }
  }
});

test('SOCKS HTTP 分块传输只有收到完整结束块才成功，拒绝超长块声明', async () => {
  const binary = Buffer.from([0, 255, 3, 4]);
  const dataChunk = Buffer.concat([Buffer.from('4\r\n'), binary, Buffer.from('\r\n')]);
  const header = Buffer.from('HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nTransfer-Encoding: chunked\r\n\r\n');
  const call = () => generateNovelAiImage({ operation: 'encode-vibe', operationParameters: {} }, account, env);
  function response(chunks) {
    const socket = new EventEmitter();
    socket.destroy = () => {};
    socket.write = () => queueMicrotask(() => {
      for (const chunk of chunks) socket.emit('data', chunk);
      socket.emit('end');
    });
    return __providerInternals.writeAndReadHttpResponse(socket, Buffer.alloc(0));
  }
  for (const ending of ['0\r\n\r\n', '0\r\nX-Diagnostic: complete\r\n\r\n']) {
    const { value } = await captured(call, () => response([header, dataChunk, Buffer.from(ending)]));
    assert.deepEqual(value.buffer, binary);
  }
  for (const chunks of [
    [header, dataChunk],
    [header, dataChunk, Buffer.from('0\r\n')],
    [header, Buffer.from('4000001\r\n')],
    [header, Buffer.from('4z\r\n')],
    [header, Buffer.concat([Buffer.from('4\r\n'), binary, Buffer.from('xx0\r\n\r\n')])]
  ]) await captured(() => assert.rejects(call(), /chunked/), () => response(chunks));
  const retryDate = new Date(Date.now() + 300000).toUTCString(), until = new Date(retryDate).toISOString();
  const throttled = Buffer.from(`HTTP/1.1 429 Too Many Requests\r\nRetry-After: ${retryDate}\r\nTransfer-Encoding: chunked\r\n\r\n`);
  await captured(() => assert.rejects(call(), error => /chunked/.test(error.message) && error.upstreamStatus === 429 && error.upstreamRetryAfterUntil === until),
    () => response([throttled, dataChunk]));
});


test('Medium 通用客户端与原生请求都按实际固定参数发往同一模型，失败不自动重发',async()=>{
 const model='nai-diffusion-5-full-medium';
 const generic=normalizeNovelAiRequest({model,prompt:'a calm garden',width:1024,height:1024,steps:28,sampler:'k_dpmpp_2m_sde',cfg:0.5,negative:'discard this custom negative',scale:7},{defaults:{}});
 assert.equal(generic.steps,14);assert.equal(generic.requestedSteps,14);assert.equal(generic.sampler,'k_euler_ancestral');assert.equal(generic.cfg,0);
 const native={...generic,nativeParameters:{steps:23,sampler:'k_euler',cfg_rescale:0.7,negative_prompt:'discard this too',v4_prompt:{caption:{base_caption:generic.prompt,char_captions:[]},use_order:true}}};
 for(const input of [generic,native]){
  const before=structuredClone(input),{calls}=await captured(()=>generateNovelAiImage(input,account,env,{forceStream:false}));
  assert.equal(calls.length,1);assert.equal(calls[0].body.model,model);assert.equal(calls[0].body.parameters.steps,14);assert.equal(calls[0].body.parameters.sampler,'k_euler_ancestral');assert.equal(calls[0].body.parameters.cfg_rescale,0);assert.equal(calls[0].body.parameters.tag_hint_uc_preset,2);assert.match(calls[0].body.parameters.negative_prompt,/^nsfw, lowres,/);assert.doesNotMatch(JSON.stringify(calls[0].body),/discard this/);assert.deepEqual(input,before);
 }
 const failure=await captured(async()=>{await assert.rejects(generateNovelAiImage(generic,account,env,{forceStream:false}),error=>error.upstreamStatus===429);},()=>new Response('Concurrent generation is locked',{status:429}));assert.equal(failure.calls.length,1);
});


test('模型目录提供 Medium 的唯一采样器组合，旧 High 与 5C 条目保留',()=>{
 const source=readFileSync(new URL('../../server/gateway-runtime.js',import.meta.url),'utf8');
 const blocks=[/const openAiSamplers = \[[\s\S]*?\n\];/,/const openAiImageModels = \[[\s\S]*?\n\];/,/const openAiSizeTiers = \{[\s\S]*?\n\};/].map(pattern=>{const match=source.match(pattern);assert.ok(match);return match[0];});
 const start=source.indexOf('function openAiModelsResponse('),end=source.indexOf('\n}',start)+2;assert.ok(start>0);
 const ctx=vm.createContext({Date,Object,sizeMap,openAiFixedSteps:28,generationCost:generationPrice});
 const result=vm.runInContext(blocks.join('\n')+'\n'+source.slice(start,end)+'\nopenAiModelsResponse()',ctx);
 const medium=result.data.filter(item=>item.id.includes('nai-diffusion-5-full-medium'));
 assert.equal(medium.length,3);assert.ok(medium.every(item=>item.id.endsWith(':k_euler_ancestral')));
 for(const id of ['nai-diffusion-5-full:k_euler','nai-diffusion-5-curated:k_euler','nai-diffusion-4-5-full:k_euler'])assert.ok(result.data.some(item=>item.id===id));
 const paid=medium.find(item=>item.id.startsWith('[2K]'));assert.equal(paid.cost,generationPrice({model:'nai-diffusion-5-full-medium',width:1088,height:1600,steps:14}));
});
