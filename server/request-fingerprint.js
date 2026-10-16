import { createHash } from 'node:crypto';
import { buildNovelAiParameters } from './providers.js';

// 入口已验证 base64；这里按解码字节摘要，不再复制或拼接整段图片文本。
function imageDigest(value) {
  if (typeof value !== 'string' || !value) return value;
  const encoded = value.startsWith('data:') ? value.slice(value.indexOf(',') + 1) : value;
  return { sha256: createHash('sha256').update(encoded, 'base64').digest('hex') };
}

function sortedKeys(value) {
  if (Array.isArray(value)) return value.map(sortedKeys);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map(key => [key, sortedKeys(value[key])]));
  return value;
}

// request 必须已经通过入口校验/规范化；randomSeed 和 noCache 由入口保留原始意图并判定。
export function generationRequestFingerprint(request, { randomSeed = false, noCache = false } = {}) {
  const operation = request.operation || 'generate';
  const parameters = operation === 'generate' ? buildNovelAiParameters(request) : { ...request.operationParameters };
  if (operation === 'generate' && randomSeed) parameters.seed = 'random';
  for (const field of ['image', 'mask', 'reference_image']) if (Object.hasOwn(parameters, field)) parameters[field] = imageDigest(parameters[field]);
  for (const field of ['reference_image_multiple', 'director_reference_images']) {
    if (Array.isArray(parameters[field])) parameters[field] = parameters[field].map(imageDigest);
  }
  for (const field of ['reference_image_multiple_cached', 'director_reference_images_cached']) {
    if (Array.isArray(parameters[field])) parameters[field] = parameters[field].map(item => ({ ...item, data: imageDigest(item.data) }));
  }
  const execution = { operation, action: request.action || operation, model: request.model, prompt: request.prompt,
    parameters, sampleCount: request.sampleCount || 1, allowPurchasedAnlas: request.allowPurchasedAnlas === true, noCache: noCache === true };
  return createHash('sha256').update(JSON.stringify(sortedKeys(execution))).digest('hex');
}

export function requestNormalizationDefaults(settings = {}) {
  const pick = (source, fields) => Object.fromEntries(fields.filter(key => source?.[key] !== undefined).map(key => [key, source[key]]));
  return { ...pick(settings, ['defaultModel', 'defaultArtist', 'defaultNegative']),
    defaults: pick(settings.defaults, ['size', 'width', 'height', 'steps', 'scale', 'cfg', 'sampler', 'noiseSchedule']) };
}
