import test from 'node:test';
import assert from 'node:assert/strict';
import { generationRequestFingerprint as fingerprint, requestNormalizationDefaults } from '../../server/request-fingerprint.js';
import { normalizeNovelAiRequest, buildNovelAiParameters } from '../../server/providers.js';
import { parseNativeImageRequest, parseNativeAuxiliaryRequest } from '../../server/native-image.js';

const model = 'nai-diffusion-4-5-full';
const native = parameters => parseNativeImageRequest({ input: 'test prompt', model, parameters: { width: 128, height: 128, steps: 23, seed: 123, ...parameters } });
const png = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=';

test('只比较实际执行参数，固定种子/提示词/模型/操作冲突，元数据不影响指纹', () => {
  const request = native(), expected = fingerprint(request), before = structuredClone(request);
  assert.match(expected, /^[a-f0-9]{64}$/);
  assert.equal(fingerprint(native({ prompt: 'test prompt' })), expected, '冗余提示词不改变执行参数或幂等指纹');
  for (const patch of [{ seed: 124 }, { prompt: 'another prompt' }, { model: 'nai-diffusion-4-full' }, { action: 'img2img' }, { width: 256 }]) {
    assert.notEqual(fingerprint({ ...request, ...patch }), expected);
  }
  assert.equal(fingerprint({ ...request, token: 'private', diagnosticId: 'other', createdAt: 'later', source: 'openai', stream: true, requestedSteps: 99, cost: 999,
    nativeParameters: { ...request.nativeParameters, stream: 'msgpack', seed: 999, width: 2048, height: 2048, steps: 50 } }), expected);
  assert.deepEqual(request, before);
});

test('随机主种子的重试意图稳定，固定种子不同；不改变 extra_noise_seed 的三种语义', () => {
  const a = native(), b = { ...a, seed: 456 };
  assert.equal(fingerprint(a, { randomSeed: true }), fingerprint(b, { randomSeed: true }));
  assert.notEqual(fingerprint(a), fingerprint(b));
  assert.notEqual(fingerprint(a), fingerprint(a, { randomSeed: true }));
  const absent = fingerprint(native(), { randomSeed: true });
  const randomNoise = native({ extra_noise_seed: -1 });
  const fixedNoise = native({ extra_noise_seed: 0 });
  assert.equal(new Set([absent, fingerprint(randomNoise, { randomSeed: true }), fingerprint(fixedNoise, { randomSeed: true })]).size, 3);
  assert.notEqual(fingerprint(native({ extra_noise_seed: 1 })), fingerprint(fixedNoise));
  assert.equal(randomNoise.nativeParameters.extra_noise_seed, -1);
  assert.equal(fingerprint(randomNoise, { randomSeed: true }), fingerprint({ ...randomNoise, seed: 789 }, { randomSeed: true }));
});

test('对象键顺序无关，角色数组与参考数组顺序保留', () => {
  const characters = [{ char_caption: 'first', centers: [{ x: 0.2, y: 0.5 }] }, { char_caption: 'second', centers: [{ x: 0.8, y: 0.5 }] }];
  const a = native({ v4_prompt: { caption: { base_caption: 'test', char_captions: characters }, use_coords: true } });
  const b = native({ v4_prompt: { use_coords: true, caption: { char_captions: characters.map(c => ({ centers: c.centers.map(p => ({ y: p.y, x: p.x })), char_caption: c.char_caption })), base_caption: 'test' } } });
  assert.equal(fingerprint(a), fingerprint(b));
  assert.notEqual(fingerprint(a), fingerprint(native({ v4_prompt: { caption: { base_caption: 'test', char_captions: [...characters].reverse() }, use_coords: true } })));
  const one = Buffer.alloc(128, 1).toString('base64'), two = Buffer.alloc(128, 2).toString('base64');
  assert.notEqual(fingerprint(native({ reference_image_multiple: [one, two], reference_strength_multiple: [0.5, 0.5] })), fingerprint(native({ reference_image_multiple: [two, one], reference_strength_multiple: [0.5, 0.5] })));
});

test('图片、遮罩、参考图和编码按解码字节摘要，编码方式不影响结果，字节变化有冲突', () => {
  const variants = [png, png.replace(/=+$/, ''), 'data:image/png;base64,' + png];
  const base = native();
  const locations = [
    value => ({ image: value }), value => ({ mask: value }), value => ({ reference_image: value }),
    value => ({ reference_image_multiple: [value] }), value => ({ director_reference_images: [value] }),
    value => ({ reference_image_multiple_cached: [{ data: value, cache_secret_key: 'cache' }] }),
    value => ({ director_reference_images_cached: [{ data: value, cache_secret_key: 'cache' }] })
  ];
  for (const fields of locations) {
    const request = value => ({ ...base, nativeParameters: { ...base.nativeParameters, ...fields(value) } });
    assert.equal(new Set(variants.map(value => fingerprint(request(value)))).size, 1);
    assert.notEqual(fingerprint(request(png)), fingerprint(request(Buffer.from('different bytes').toString('base64'))));
    const original = request(png), before = structuredClone(original); fingerprint(original); assert.deepEqual(original, before);
  }
  const encoded = Buffer.alloc(128, 42).toString('base64');
  const vibe = value => native({ reference_image_multiple: [value], reference_strength_multiple: [0.5] });
  assert.equal(fingerprint(vibe(encoded)), fingerprint(vibe(encoded.replace(/=+$/, ''))));
});

test('批次数、购买授权和实际缓存选择都参与指纹', () => {
  const request = native();
  assert.notEqual(fingerprint(request), fingerprint(native({ n_samples: 2 })));
  assert.notEqual(fingerprint(request), fingerprint({ ...request, allowPurchasedAnlas: true }));
  assert.equal(fingerprint(request), fingerprint({ ...request, allowPurchasedAnlas: false }));
  assert.notEqual(fingerprint(request), fingerprint(request, { noCache: true }));
});

test('Medium 使用实际固定参数，忽略被覆写的 steps、sampler、cfg 和负向词', () => {
  const settings = { defaults: { scale: 7, cfg: 0.5, sampler: 'k_euler', noiseSchedule: 'karras' } };
  const a = normalizeNovelAiRequest({ model: 'nai-diffusion-5-full-medium', prompt: 'test', seed: 42, steps: 50, negative: 'one' }, settings);
  const b = { ...a, steps: 3, sampler: 'ddim', cfg: 0.8, negative: 'two', requestedSteps: 3 };
  const parameters = buildNovelAiParameters(b);
  assert.equal(parameters.steps, 14); assert.equal(parameters.sampler, 'k_euler_ancestral'); assert.equal(parameters.cfg_rescale, 0);
  assert.equal(fingerprint(a), fingerprint(b));
  assert.notEqual(fingerprint(a), fingerprint({ ...a, scale: 6 }));
  const nativeA = { ...native(), model: 'nai-diffusion-5-full-medium' };
  const nativeB = { ...nativeA, steps: 3, nativeParameters: { ...nativeA.nativeParameters, sampler: 'ddim', cfg_rescale: 0.8, negative_prompt: 'ignored', uc: 'ignored' } };
  assert.equal(fingerprint(nativeA), fingerprint(nativeB));
});

test('辅助操作按实际 operationParameters 比较，图片编码等价且不改入参', () => {
  const a = parseNativeAuxiliaryRequest({ model, image: png, information_extracted: 0.8 }, 'encode-vibe');
  const b = { ...a, operationParameters: { ...a.operationParameters, image: 'data:image/png;base64,' + png } };
  const before = structuredClone(b);
  assert.equal(fingerprint(a), fingerprint(b));
  assert.deepEqual(b, before);
  assert.notEqual(fingerprint(a), fingerprint({ ...a, operationParameters: { ...a.operationParameters, information_extracted: 0.7 } }));
  const upscale = parseNativeAuxiliaryRequest({ image: png }, 'upscale');
  assert.notEqual(fingerprint(a), fingerprint(upscale));
  assert.equal(fingerprint(upscale), fingerprint(upscale, { randomSeed: true }));
});

test('重试默认上下文只保留规范化实际读取的有限字段', () => {
  const settings = { defaultModel: model, defaultArtist: 'artist', defaultNegative: 'negative', apiKey: 'secret', fairQuota: { private: true },
    defaults: { size: '自定', width: 640, height: 768, steps: 23, scale: 5, cfg: 0.2, sampler: 'k_euler', noiseSchedule: 'karras', secret: 'hidden' } };
  const snapshot = requestNormalizationDefaults(settings);
  assert.deepEqual(snapshot, { defaultModel: model, defaultArtist: 'artist', defaultNegative: 'negative', defaults: { size: '自定', width: 640, height: 768, steps: 23, scale: 5, cfg: 0.2, sampler: 'k_euler', noiseSchedule: 'karras' } });
  assert.equal(fingerprint(normalizeNovelAiRequest({ prompt: 'test', seed: 1 }, settings)), fingerprint(normalizeNovelAiRequest({ prompt: 'test', seed: 1 }, snapshot)));
  assert.deepEqual(requestNormalizationDefaults({}), { defaults: {} });
});
