import { canonicalJson, domainDigest } from '../events/domain-events.ts';
import { requestNormalizationDefaults } from './request-fingerprint.ts';

export const defaultArtist2_5D = `0.9::misaka_12003-gou ::, dino_(dinoartforame), wanke, liduke, year 2025, realistic, 4k, -2::green ::, textless version, The image is highly intricate finished drawn. Only the character's face is in anime style, but their body is in realistic style. 1.35::A highly finished photo-style artwork that has lively color, graphic texture, realistic skin surface, and lifelike flesh with little obliques::. 1.63::photorealistic::, 1.63::photo(medium)::, \\n20::best quality, absurdres, very aesthetic, detailed, masterpiece::,, very aesthetic, masterpiece, no text,`;

// 首次安装沿用当前实际默认值；历史迁移必须显式导入选定来源。
export const initialNormalizationDefaults = {
  defaultModel: 'nai-diffusion-4-5-full',
  defaultArtist: defaultArtist2_5D,
  defaultNegative:
    '{{{{bad anatomy}}}},{bad feet},bad hands,{{{bad proportions}}},{blurry},cloned face,cropped,{{{deformed}}},{{{disfigured}}},error,{{{extra arms}}},{extra digit},{{{extra legs}}},extra limbs,{{extra limbs}},{fewer digits},{{{fused fingers}}},gross proportions,jpeg artifacts,{{{{long neck}}}},low quality,{malformed limbs},{{missing arms}},{missing fingers},{{missing legs}},mutated hands,{{{mutation}}},normal quality,poorly drawn face,poorly drawn hands,signature,text,{{too many fingers}},{{{ugly}}},username,watermark,worst quality',
  defaults: {
    size: '绔栧浘',
    width: 832,
    height: 1216,
    steps: 28,
    scale: 6,
    cfg: 0,
    sampler: 'k_dpmpp_2m_sde',
    noiseSchedule: 'karras',
  },
};

export interface NormalizationDefaultsProfile {
  kind: 'normalization-defaults';
  values: ReturnType<typeof requestNormalizationDefaults>;
}

export function configurationUnavailable(): never {
  throw Object.assign(new Error('当前网关配置或其受保护来源不可用。'), {
    statusCode: 503,
    code: 'configuration_unavailable',
  });
}

function object(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object' && Object.getPrototypeOf(value) === Object.prototype;
}

// 这是当前默认值的专用载体，不能作为缺少 Native 定义的生成首次上下文。
export function parseNormalizationDefaultsProfile(value: unknown): NormalizationDefaultsProfile {
  if (
    !object(value) ||
    value.kind !== 'normalization-defaults' ||
    Object.keys(value).length !== 2 ||
    !object(value.values) ||
    !object(value.values.defaults)
  ) {
    configurationUnavailable();
  }
  const selected = requestNormalizationDefaults(value.values);
  const primitive = (entry: unknown) =>
    entry === null ||
    typeof entry === 'string' ||
    typeof entry === 'boolean' ||
    (typeof entry === 'number' && Number.isFinite(entry));
  if (
    Object.keys(selected).length !== 4 ||
    Object.keys(selected.defaults).length !== 8 ||
    Object.entries(value.values).some(([key, entry]) => key !== 'defaults' && !primitive(entry)) ||
    Object.values(value.values.defaults).some((entry) => !primitive(entry)) ||
    canonicalJson(selected) !== canonicalJson(value.values)
  ) {
    configurationUnavailable();
  }
  return { kind: 'normalization-defaults', values: structuredClone(selected) };
}

export function prepareNormalizationDefaultsProfile(values: unknown) {
  const profile = parseNormalizationDefaultsProfile({ kind: 'normalization-defaults', values }),
    body = canonicalJson(profile),
    contextSha256 = domainDigest(profile);
  return { profile, body, contextId: `defaults_${contextSha256}`, contextSha256 };
}
