import { V5_MEDIUM_STEPS } from './v5-medium-rules.js';

export interface V4Prompt {
  caption: {
    base_caption: string;
    char_captions: { char_caption: string; centers: { x: number; y: number }[] }[];
  };
  use_coords?: boolean;
  use_order?: boolean;
  legacy_uc?: boolean;
}

interface MediumParameters {
  steps?: number;
  sampler?: string;
  cfg_rescale?: number;
  params_version?: number;
  ucPresetId?: string;
  tag_hint_uc_preset?: number;
  ucPreset?: unknown;
  legacy_uc?: boolean;
  v4_prompt?: V4Prompt | false | '' | null;
  negative_prompt?: string;
  uc?: string;
  v4_negative_prompt?: unknown;
}

// 与官网 V5 的 heavy 预设及默认负向构造一致；不拼接调用者自定义负面词。
export interface V5MediumProfile {
  steps: number;
  sampler: string;
  cfgRescale: number;
  defaultScale: number;
  paramsVersion: number;
  ucPresetId: string;
  tagHintUcPreset: number;
  legacyUc: boolean;
  negativePreset: string;
}
export const V5_MEDIUM_PROFILE: V5MediumProfile = {
  steps: V5_MEDIUM_STEPS,
  sampler: 'k_euler_ancestral',
  cfgRescale: 0,
  defaultScale: 7,
  paramsVersion: 4,
  ucPresetId: 'heavy',
  tagHintUcPreset: 2,
  legacyUc: false,
  negativePreset:
    'lowres, artistic error, film grain, scan artifacts, worst quality, bad quality, jpeg artifacts, very displeasing, chromatic aberration, dithering, halftone, screentone, multiple views, logo, too many watermarks, negative space, blank page',
};
export const mediumNegativePrompt = (prompt: unknown, profile = V5_MEDIUM_PROFILE): string =>
  (String(prompt || '')
    .toLowerCase()
    .includes('nsfw')
    ? ''
    : 'nsfw, ') + profile.negativePreset;

// 只对已经复制、验证的参数应用固定配置，原始请求和正向角色提示词保持完整。
export function applyV5MediumParameters<T extends MediumParameters>(
  parameters: T,
  prompt: string,
  profile = V5_MEDIUM_PROFILE,
): T {
  parameters.steps = profile.steps;
  parameters.sampler = profile.sampler;
  parameters.cfg_rescale = profile.cfgRescale;
  parameters.params_version = profile.paramsVersion;
  parameters.ucPresetId = profile.ucPresetId;
  parameters.tag_hint_uc_preset = profile.tagHintUcPreset;
  delete parameters.ucPreset;
  parameters.legacy_uc = profile.legacyUc;
  parameters.v4_prompt ||= {
    caption: { base_caption: prompt, char_captions: [] },
    use_coords: false,
    use_order: true,
  };
  const positive = parameters.v4_prompt.caption;
  const negative = mediumNegativePrompt(positive.base_caption, profile);
  parameters.negative_prompt = negative;
  parameters.uc = negative;
  parameters.v4_negative_prompt = {
    caption: {
      base_caption: negative,
      char_captions: positive.char_captions.map((character) => ({
        char_caption: '',
        centers: structuredClone(character.centers),
      })),
    },
    legacy_uc: false,
  };
  return parameters;
}
