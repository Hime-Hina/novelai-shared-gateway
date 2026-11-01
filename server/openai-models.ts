import { sizeMap } from '../public/generation-pricing.js';
export const openAiFixedSteps = 28;
const openAiSamplers = [
  'k_euler_ancestral',
  'k_euler',
  'k_dpmpp_2s_ancestral',
  'k_dpmpp_2m_sde',
  'k_dpmpp_2m',
  'k_dpmpp_sde'
];
const openAiImageModels = [
  { id: 'nai-diffusion-4-5-full', cost: 1 },
  { id: 'nai-diffusion-5-full', cost: 8 },
  { id: 'nai-diffusion-5-full-medium', cost: 8 },
  { id: 'nai-diffusion-5-curated', cost: 8 }
];
export const openAiSizeTiers: Record<string, { label: string; sizes: Record<string, { width: number; height: number }> }> = {
  '2K': {
    label: '[2K]',
    sizes: {
      '竖图': sizeMap['2K竖图'],
      '横图': sizeMap['2K横图'],
      '方图': sizeMap['2K方图']
    }
  },
  '4K': {
    label: '[4K]',
    sizes: {
      '竖图': sizeMap['4K竖图'],
      '横图': sizeMap['4K横图'],
      '方图': sizeMap['4K方图']
    }
  }
};

export function openAiModelsResponse() {
  const created = Math.floor(Date.now() / 1000);
  return {
    object: 'list',
    data: [
      ...openAiImageModels.flatMap((model) => openAiSamplers.filter(sampler => model.id !== 'nai-diffusion-5-full-medium' || sampler === 'k_euler_ancestral').map((sampler) => ({
        id: `${model.id}:${sampler}`,
        object: 'model',
        created,
        owned_by: 'nai2api',
        cost: model.cost,
        resolution_tier: 'standard'
      }))),
      ...Object.entries(openAiSizeTiers).flatMap(([tierName, tier]) => openAiImageModels.flatMap((model) => openAiSamplers.filter(sampler => model.id !== 'nai-diffusion-5-full-medium' || sampler === 'k_euler_ancestral').map((sampler) => ({
        id: `${tier.label}${model.id}:${sampler}`,
        object: 'model',
        created,
        owned_by: 'nai2api',
        cost: 0,
        cost_by_size: Object.fromEntries(Object.keys(tier.sizes).map(size => [size, 0])),
        resolution_tier: tierName
      }))))
    ]
  };
}
