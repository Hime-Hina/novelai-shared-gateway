import { createHash } from 'node:crypto';
import { buildNovelAiParameters } from './providers.js';

interface FingerprintRequest {
  operation?: string;
  action?: string;
  model: string;
  prompt: string;
  sampleCount?: number;
  allowPurchasedAnlas?: unknown;
  operationParameters?: Record<string, unknown>;
}

type NormalizationSettings = {
  defaultModel?: unknown;
  defaultArtist?: unknown;
  defaultNegative?: unknown;
  defaults?: Partial<Record<'size' | 'width' | 'height' | 'steps' | 'scale' | 'cfg' | 'sampler' | 'noiseSchedule', unknown>> | null;
};

// 入口已验证 base64；这里按解码字节摘要，不再复制或拼接整段图片文本。
function imageDigest(value: unknown): unknown {
  if (typeof value !== 'string' || !value) return value;
  const encoded = value.startsWith('data:') ? value.slice(value.indexOf(',') + 1) : value;
  return { sha256: createHash('sha256').update(encoded, 'base64').digest('hex') };
}

function sortedKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortedKeys);
  if (value && typeof value === 'object') {
    const record = value as Record<string, unknown>;
    return Object.fromEntries(Object.keys(record).sort().map(key => [key, sortedKeys(record[key])]));
  }
  return value;
}

// request 必须已经通过入口校验/规范化；randomSeed 和 noCache 由入口保留原始意图并判定。
export function generationRequestFingerprint(request: FingerprintRequest, { randomSeed = false, noCache = false }: { randomSeed?: boolean; noCache?: boolean } = {}): string {
  const operation = request.operation || 'generate';
  const parameters: Record<string, unknown> = operation === 'generate' ? buildNovelAiParameters(request) : { ...request.operationParameters };
  if (operation === 'generate' && randomSeed) parameters.seed = 'random';
  for (const field of ['image', 'mask', 'reference_image']) if (Object.hasOwn(parameters, field)) parameters[field] = imageDigest(parameters[field]);
  for (const field of ['reference_image_multiple', 'director_reference_images']) {
    if (Array.isArray(parameters[field])) parameters[field] = parameters[field].map(imageDigest);
  }
  for (const field of ['reference_image_multiple_cached', 'director_reference_images_cached']) {
    if (Array.isArray(parameters[field])) parameters[field] = parameters[field].map((item: Record<string, unknown>) => ({ ...item, data: imageDigest(item.data) }));
  }
  const execution = { operation, action: request.action || operation, model: request.model, prompt: request.prompt,
    parameters, sampleCount: request.sampleCount || 1, allowPurchasedAnlas: request.allowPurchasedAnlas === true, noCache: noCache === true };
  return createHash('sha256').update(JSON.stringify(sortedKeys(execution))).digest('hex');
}

export function requestNormalizationDefaults(settings: NormalizationSettings = {}) {
  const pick = (source: Record<string, unknown> | null | undefined, fields: string[]): Record<string, unknown> => Object.fromEntries(fields.filter(key => source?.[key] !== undefined).map(key => [key, source![key]]));
  return { ...pick(settings, ['defaultModel', 'defaultArtist', 'defaultNegative']),
    defaults: pick(settings.defaults, ['size', 'width', 'height', 'steps', 'scale', 'cfg', 'sampler', 'noiseSchedule']) };
}
