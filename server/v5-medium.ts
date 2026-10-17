import { V5_MEDIUM_STEPS } from '../public/v5-medium.js';

interface V4Prompt {
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
const HEAVY_PRESET = 'lowres, artistic error, film grain, scan artifacts, worst quality, bad quality, jpeg artifacts, very displeasing, chromatic aberration, dithering, halftone, screentone, multiple views, logo, too many watermarks, negative space, blank page';
export const mediumNegativePrompt = (prompt: unknown): string => (String(prompt || '').toLowerCase().includes('nsfw') ? '' : 'nsfw, ') + HEAVY_PRESET;

// 只对已经复制、验证的参数应用固定配置，原始请求和正向角色提示词保持完整。
export function applyV5MediumParameters<T extends MediumParameters>(parameters: T, prompt: string): T {
  parameters.steps = V5_MEDIUM_STEPS;
  parameters.sampler = 'k_euler_ancestral';
  parameters.cfg_rescale = 0;
  parameters.params_version = 4;
  parameters.ucPresetId = 'heavy';
  parameters.tag_hint_uc_preset = 2;
  delete parameters.ucPreset;
  parameters.legacy_uc = false;
  parameters.v4_prompt ||= { caption: { base_caption: prompt, char_captions: [] }, use_coords: false, use_order: true };
  const positive = parameters.v4_prompt.caption;
  const negative = mediumNegativePrompt(positive.base_caption);
  parameters.negative_prompt = negative;
  parameters.uc = negative;
  parameters.v4_negative_prompt = {
    caption: {
      base_caption: negative,
      char_captions: positive.char_captions.map(character => ({ char_caption: '', centers: structuredClone(character.centers) }))
    },
    legacy_uc: false
  };
  return parameters;
}
