import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { parseNativeImageRequest, parseNativeAuxiliaryRequest } from '../../server/native-image.js';
import { quoteNativeRequest } from '../../server/native-pricing.ts';
import { sharedSurface } from '../../server/shared-surface.ts';

const png = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=';
const fixture = name => JSON.parse(readFileSync(new URL(`../fixtures/${name}.json`, import.meta.url), 'utf8'));
const request = () => ({ input: 'a blue bird', model: 'nai-diffusion-4-5-full', action: 'generate',
  parameters: { width: 1024, height: 1024, steps: 28, seed: 7, n_samples: 1 } });

test('C02：官方图片操作和四个订阅别名开放，其余官方功能不被误认为透传接口', () => {
  for (const operation of ['generate-image', 'generate-image-stream', 'encode-vibe', 'upscale']) {
    for (const prefix of ['', '/v1']) {
      const path = `${prefix}/ai/${operation}`;
      assert.deepEqual(sharedSurface(path, 'POST'), { allowed: true, pathname: path });
      assert.equal(sharedSurface(path, 'GET').allowed, false);
    }
  }
  for (const path of ['/user/subscription', '/v1/user/subscription', '/ai/user/subscription', '/v1/ai/user/subscription']) {
    assert.equal(sharedSurface(path, 'GET').allowed, true);
    assert.equal(sharedSurface(path, 'POST').allowed, false);
  }
  for (const [path, method] of [
    ['/ai/augment-image', 'POST'], ['/ai/generate-image/suggest-tags', 'GET'],
    ['/ai/generate', 'POST'], ['/ai/generate-stream', 'POST'], ['/oa/v1/models', 'GET'],
    ['/oa/v1/chat/completions', 'POST'], ['/user/data', 'GET'], ['/user/login', 'POST'],
    ['/user/create-persistent-token', 'POST']
  ]) assert.equal(sharedSurface(path, method).allowed, false, path);
});

test('C02：三个客户端样本经解析与核价得到固定独立期望，不用被测公式生成 expected', () => {
  const cases = [
    { file: 'novelai-image-desktop-request', model: 'nai-diffusion-4-5-full', seed: 810, scale: 5, noise: 'native', resource: 'free', amount: 0, estimated: false },
    { file: 'littlewhitebox-v5-request', model: 'nai-diffusion-5-full', seed: 12617, scale: 7, noise: 'karras', resource: 'v5', amount: 0.057803468208092484, estimated: true },
    { file: 'aaalice-v5-curated-request', model: 'nai-diffusion-5-curated', seed: 429026, scale: 4, noise: 'karras', resource: 'v5', amount: 0.057803468208092484, estimated: true }
  ];
  for (const expected of cases) {
    const body = fixture(expected.file), before = structuredClone(body);
    const parsed = parseNativeImageRequest(body);
    assert.deepEqual({ model: parsed.model, width: parsed.width, height: parsed.height, steps: parsed.steps,
      seed: parsed.seed, scale: parsed.scale, sampler: parsed.sampler, noise: parsed.noiseSchedule, sampleCount: parsed.sampleCount },
    { model: expected.model, width: 1024, height: 1024, steps: 23, seed: expected.seed,
      scale: expected.scale, sampler: 'k_euler_ancestral', noise: expected.noise, sampleCount: 1 });
    for (const removed of ['uc_preset', 'quality_toggle', 'characterPrompts', 'stream', 'inpaintImg2ImgStrength']) {
      assert.equal(Object.hasOwn(parsed.nativeParameters, removed), false, removed);
    }
    const quote = quoteNativeRequest(parsed);
    assert.equal(quote.resource, expected.resource);
    assert.ok(Math.abs(quote.amount - expected.amount) < 1e-12);
    assert.equal(quote.estimated, expected.estimated);
    assert.deepEqual(body, before);
  }
});

test('C02：迁移期间保留明确拒绝和 stream 清理，不因 Swagger 新字段而直接透传', () => {
  const invalid = [
    { input: '' }, { parameters: null }, { url: 'https://untrusted.invalid/image.png' },
    { parameters: { ...request().parameters, prompt: 'a different prompt' } },
    { parameters: { ...request().parameters, image_format: 'avif' } },
    { parameters: { ...request().parameters, controlnet_model: 'hed' } },
    { parameters: { ...request().parameters, upscale: { declared_blur_sigma: 0 } } },
    { parameters: { ...request().parameters, lora_unet_weights: { unpriced: 1 } } },
    { parameters: { ...request().parameters, n_samples: 5 } },
    ...[null, false, 0, 'zip', 'SSE'].map(stream => ({ parameters: { ...request().parameters, stream } }))
  ];
  for (const [index, patch] of invalid.entries()) {
    assert.throws(() => parseNativeImageRequest({ ...request(), ...patch }), error => error.statusCode === (index < 2 ? 400 : 422));
  }
  for (const stream of ['sse', 'msgpack']) {
    const parsed = parseNativeImageRequest({ ...request(), allowPurchasedAnlas: true, parameters: { ...request().parameters, stream } });
    assert.equal(Object.hasOwn(parsed.nativeParameters, 'stream'), false);
    assert.equal(Object.hasOwn(parsed.nativeParameters, 'allowPurchasedAnlas'), false);
  }
});

test('C02：辅助操作归一和报价采用固定值，官方尚未适配字段继续返回422', () => {
  const vibeBody = { image: png, model: 'nai-diffusion-4-5-full', allowPurchasedAnlas: true };
  const vibe = parseNativeAuxiliaryRequest(vibeBody, 'encode-vibe');
  assert.deepEqual(vibe.operationParameters, { image: png, model: 'nai-diffusion-4-5-full', information_extracted: 1 });
  assert.deepEqual(quoteNativeRequest(vibe), { resource: 'anlas', amount: 2, estimated: false, breakdown: { encoding: 2 } });
  for (const patch of [{ mask: '' }, { crop_to_mask: false }, { focus_seed: null }, { info_extract_seed: '0' }]) {
    assert.throws(() => parseNativeAuxiliaryRequest({ ...vibeBody, ...patch }, 'encode-vibe'), error => error.statusCode === 422);
  }
  const upscale = parseNativeAuxiliaryRequest({ image: png, scale: 2, width: 1, height: 1 }, 'upscale');
  assert.deepEqual(upscale.operationParameters, { image: png, model: 'nai-diffusion-5-curated', declared_blur_sigma: 0 });
  assert.deepEqual(quoteNativeRequest(upscale), { resource: 'anlas', amount: 1, estimated: false, breakdown: { upscale: 1 } });
  for (const patch of [{ declared_blur_sigma: 0.31 }, { scale: 4 }, { width: 2 }, { model: 'nai-diffusion-4-5-full' }]) {
    assert.throws(() => parseNativeAuxiliaryRequest({ image: png, ...patch }, 'upscale'), error => error.statusCode === 422);
  }
});

test('C02：28/29步免费边界与V5本地Anlas续用保留独立固定价格', () => {
  const free = parseNativeImageRequest(request());
  assert.equal(quoteNativeRequest(free).resource, 'free');
  assert.equal(quoteNativeRequest(free).amount, 0);
  const paid = parseNativeImageRequest({ ...request(), parameters: { ...request().parameters, steps: 29 } });
  assert.equal(quoteNativeRequest(paid).resource, 'anlas');
  assert.equal(quoteNativeRequest(paid).amount, 21);
  const v5 = parseNativeImageRequest(fixture('littlewhitebox-v5-request'));
  assert.equal(quoteNativeRequest(v5, { forceAnlas: true }).amount, 26);
  // 批次由调度器拆成单张；不能在这里把两张免费图合成一次官方付费批次。
  assert.throws(() => quoteNativeRequest({ ...free, n_samples: 2 }), error => error.statusCode === 422);
});
