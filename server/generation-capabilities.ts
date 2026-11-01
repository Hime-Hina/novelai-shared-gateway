import { createHash } from 'node:crypto';
import { BASE_MODELS, INPAINT_MODELS, BOOLEAN_PARAMETERS, NATIVE_SAMPLERS, NATIVE_NOISE_SCHEDULES,
  NATIVE_VIBE_MODELS, NATIVE_BLUR_VALUES, NATIVE_VIBE_FIELDS, NATIVE_PRECISE_FIELDS, NATIVE_DEFAULTS, NATIVE_LIMITS,
  NATIVE_MAX_BODY_BYTES, MAX_IMAGE_BYTES } from './native-image.ts';
import { NATIVE_PRICING_VERSION } from './native-pricing.ts';
import { NATIVE_MULTIPART_MAX_PARTS } from './native-body.ts';
import { isV5MediumModel } from '../public/v5-medium.js';
import { applyV5MediumParameters, V5_MEDIUM_PROFILE } from './v5-medium.ts';
import { generationPayloadLimits } from './generation-request.ts';
import { queuePolicy } from './queue-policy.ts';
import { freeImagePolicy } from './free-image-policy.ts';
import type { FairQuotaDatabase, FairUser } from './fair-quota.ts';

export type MemberGenerationOperation = 'generate' | 'upscale' | 'encode-vibe';
export interface FieldCapability {
  type: 'integer' | 'number' | 'boolean' | 'string' | 'enum' | 'image' | 'array' | 'object';
  required: boolean; min?: number; max?: number; step?: number;
  values?: (string | number | boolean)[]; default?: unknown; fixed?: unknown;
}
export interface ModelCapability {
  operation: MemberGenerationOperation; id: string; label: string;
  actions: ('generate' | 'img2img' | 'infill')[]; fields: Record<string, FieldCapability>;
  features: { multiCharacter: boolean; coordinatePlacement: boolean; vibe: boolean; preciseReference: boolean };
  requirements: { action?: 'generate' | 'img2img' | 'infill'; when?: string; requires: string[]; excludes: string[] }[];
  transforms: { code: string; fields: string[]; description: string }[];
}
export interface MemberGenerationVersions { capabilities: string; normalization: string; defaults: string; pricing: string; billingPolicy: string }
export const MEMBER_GENERATION_CONTRACT_VERSION = 1;
export const MEMBER_NORMALIZATION_FORMAT = 'native-member-v1';

// A canonical digest is also used for confirmation, so JSON object key order is never intent.
export function generationDefinitionDigest(value: unknown): string {
  const ordered = (item: unknown): unknown => Array.isArray(item) ? item.map(ordered) : item && typeof item === 'object'
    ? Object.fromEntries(Object.keys(item).sort().map(key => [key, ordered((item as Record<string, unknown>)[key])])) : item;
  return createHash('sha256').update(JSON.stringify(ordered(value))).digest('hex');
}

const integer = (min: number, max: number, required = false, extra: Partial<FieldCapability> = {}): FieldCapability => ({ type: 'integer', required, min, max, step: 1, ...extra });
const numeric = (min: number, max: number, extra: Partial<FieldCapability> = {}): FieldCapability => ({ type: 'number', required: false, min, max, ...extra });
const field = (type: FieldCapability['type'], extra: Partial<FieldCapability> = {}): FieldCapability => ({ type, required: false, ...extra });
const none = { multiCharacter: false, coordinatePlacement: false, vibe: false, preciseReference: false };

function generationModel(id: string): ModelCapability {
  const base = INPAINT_MODELS.get(id) || id, inpaint = INPAINT_MODELS.has(id), v3 = base.endsWith('-3'), v5 = base.startsWith('nai-diffusion-5-'), medium = isV5MediumModel(base);
  const actions: ModelCapability['actions'] = inpaint ? ['infill'] : base === 'nai-diffusion-5-curated' ? ['generate', 'img2img'] : ['generate', 'img2img', 'infill'];
  const fields: Record<string, FieldCapability> = {
    input: field('string', { required: true, min: 1, max: NATIVE_LIMITS.promptCharacters }),
    model: field('enum', { required: true, values: [id] }), action: field('enum', { required: inpaint, values: [...actions], ...(!inpaint ? { default: 'generate' } : {}) }),
    allowPurchasedAnlas: field('boolean', { default: false }),
    'parameters.width': integer(NATIVE_LIMITS.dimensionMin, NATIVE_LIMITS.dimensionMax, true),
    'parameters.height': integer(NATIVE_LIMITS.dimensionMin, NATIVE_LIMITS.dimensionMax, true),
    'parameters.steps': integer(NATIVE_LIMITS.stepsMin, NATIVE_LIMITS.stepsMax, !medium, medium ? { default: V5_MEDIUM_PROFILE.steps, fixed: V5_MEDIUM_PROFILE.steps } : {}),
    'parameters.n_samples': integer(1, NATIVE_LIMITS.samplesMax, false, { default: NATIVE_DEFAULTS.sampleCount }),
    'parameters.scale': numeric(0, 10, { default: medium ? V5_MEDIUM_PROFILE.defaultScale : NATIVE_DEFAULTS.scale }),
    'parameters.cfg_rescale': numeric(0, 1, { default: NATIVE_DEFAULTS.cfgRescale, ...(medium ? { fixed: V5_MEDIUM_PROFILE.cfgRescale } : {}) }),
    'parameters.seed': integer(-1, Number.MAX_SAFE_INTEGER, false, { default: -1 }),
    'parameters.sampler': field('enum', { values: [...NATIVE_SAMPLERS], default: NATIVE_DEFAULTS.sampler, ...(medium ? { fixed: V5_MEDIUM_PROFILE.sampler } : {}) }),
    'parameters.noise_schedule': field('enum', { values: [...NATIVE_NOISE_SCHEDULES], default: NATIVE_DEFAULTS.noiseSchedule }),
    'parameters.negative_prompt': field('string', { max: NATIVE_LIMITS.promptCharacters }),
    'parameters.uc': field('string', { max: NATIVE_LIMITS.promptCharacters }),
    'parameters.image_format': field('enum', { values: ['png', 'webp'] }),
    'parameters.image': field('image'), 'parameters.mask': field('image'),
    'parameters.strength': numeric(0, 1), 'parameters.noise': numeric(0, 1, { default: NATIVE_DEFAULTS.noise }),
    'parameters.extra_noise_seed': integer(-1, 4294967295),
    'parameters.img2img': field('object'), 'parameters.inpaintImg2ImgStrength': numeric(0, 1),
    'parameters.cfg_sched_eligibility': field('string', { min: 1, max: 64 }),
    'parameters.ucPresetId': field('string', { min: 1, max: 64 }), 'parameters.qualityPresetId': field('string', { min: 1, max: 64 }),
    'parameters.stream': field('enum', { values: ['msgpack', 'sse'] }),
  };
  for (const name of BOOLEAN_PARAMETERS) fields[`parameters.${name}`] = field('boolean', name === 'autoSmea' || (!v3 && ['sm', 'sm_dyn'].includes(name)) ? { fixed: false } : {});
  for (const name of ['params_version', 'ucPreset', 'tag_hint_qt', 'tag_hint_uc_preset', 'version']) fields[`parameters.${name}`] = integer(0, 20);
  for (const name of ['skip_cfg_above_sigma', 'skip_cfg_below_sigma']) fields[`parameters.${name}`] = numeric(0, 1000);
  for (const name of ['uncond_scale', 'dynamic_thresholding_mimic_scale']) fields[`parameters.${name}`] = numeric(0, 100);
  fields['parameters.dynamic_thresholding_percentile'] = numeric(0, 1);
  fields['parameters.controlnet_strength'] = numeric(0, 2);
  if (!v3) {
    fields['parameters.v4_prompt'] = field('object'); fields['parameters.v4_negative_prompt'] = field('object');
    fields['parameters.characterPrompts'] = field('array', { max: NATIVE_LIMITS.charactersMax });
  }
  if (!v5) {
    fields['parameters.reference_image'] = field('image');
    fields['parameters.reference_strength'] = numeric(0, 1, { default: 0.6 });
    fields['parameters.reference_information_extracted'] = numeric(0, 1, { default: 1 });
    for (const name of ['reference_image_multiple', 'reference_image_multiple_cached', 'reference_strength_multiple', 'reference_information_extracted_multiple']) fields[`parameters.${name}`] = field('array', { max: NATIVE_LIMITS.referencesMax });
  }
  if (base.startsWith('nai-diffusion-4-5-')) for (const name of ['director_reference_images', 'director_reference_images_cached', 'director_reference_descriptions', 'director_reference_information_extracted', 'director_reference_strength_values', 'director_reference_secondary_strength_values']) fields[`parameters.${name}`] = field('array', { max: NATIVE_LIMITS.referencesMax });
  const transforms: ModelCapability['transforms'] = [
    { code: 'seed_uint32', fields: ['parameters.seed'], description: '固定种子按 uint32 归一；随机种子在首次受理确定。' },
    { code: 'native_parameter_aliases', fields: ['parameters.uc', 'parameters.ucPreset', 'parameters.qualityToggle'], description: '沿用 Native 参数别名与负面词归一规则。' },
    { code: 'metadata_removed', fields: ['parameters.characterPrompts', 'parameters.stream'], description: '界面角色元数据和传输选择不作为上游执行参数。' },
  ];
  if (!inpaint && actions.includes('infill')) transforms.push({ code: 'infill_model', fields: ['model'], description: '局部重绘使用此基础模型对应的已验证重绘模型。' });
  if (medium) transforms.push({ code: 'medium_fixed_parameters', fields: ['parameters.steps', 'parameters.sampler', 'parameters.cfg_rescale', 'parameters.negative_prompt', 'parameters.v4_negative_prompt'], description: 'Medium 固定 14 步、Euler Ancestral、CFG rescale 0，并按官方 heavy 规则生成负面词。' });
  const vibes = NATIVE_VIBE_FIELDS.map(field => `parameters.${field}`), precise = NATIVE_PRECISE_FIELDS.map(field => `parameters.${field}`);
  const requirements: ModelCapability['requirements'] = [
    { action: 'generate', requires: [], excludes: ['parameters.image', 'parameters.mask', 'parameters.img2img'] },
    { action: 'img2img', requires: ['parameters.image'], excludes: ['parameters.mask', 'parameters.img2img', 'parameters.sm', 'parameters.sm_dyn'] },
    { action: 'infill', requires: ['parameters.image', 'parameters.mask'], excludes: [...vibes, 'parameters.sm', 'parameters.sm_dyn'] },
  ].filter(item => actions.includes(item.action as 'generate')) as ModelCapability['requirements'];
  if (!v5 && !inpaint) for (const when of vibes) requirements.push({ when, requires: [],
    excludes: [...vibes.filter(path => path !== when), ...(base.startsWith('nai-diffusion-4-5-') ? precise : [])] });
  if (base.startsWith('nai-diffusion-4-5-')) for (const when of precise) requirements.push({ when, requires: [], excludes: [...precise.filter(path => path !== when), ...vibes] });
  return { operation: 'generate', id, label: id, actions, fields,
    features: { multiCharacter: !v3, coordinatePlacement: !v3, vibe: !v5 && !inpaint, preciseReference: base.startsWith('nai-diffusion-4-5-') }, requirements, transforms };
}

export function generationModels(): ModelCapability[] {
  const models = [...BASE_MODELS, ...INPAINT_MODELS.keys()].map(generationModel);
  models.push({ operation: 'upscale', id: 'nai-diffusion-5-curated', label: 'nai-diffusion-5-curated', actions: [], features: { ...none },
    fields: { image: field('image', { required: true }), model: field('enum', { values: ['nai-diffusion-5-curated'], default: 'nai-diffusion-5-curated', fixed: 'nai-diffusion-5-curated' }), scale: field('enum', { values: [2], default: 2, fixed: 2 }),
      width: integer(1, NATIVE_LIMITS.imageDimension), height: integer(1, NATIVE_LIMITS.imageDimension), declared_blur_sigma: field('enum', { values: [...NATIVE_BLUR_VALUES], default: NATIVE_DEFAULTS.blurSigma }), allowPurchasedAnlas: field('boolean', { default: false }) },
    requirements: [{ requires: ['image'], excludes: [] }], transforms: [{ code: 'image_dimensions', fields: ['width', 'height'], description: '尺寸来自实际图片；显式声明必须一致。' }] });
  for (const id of NATIVE_VIBE_MODELS) models.push({ operation: 'encode-vibe', id, label: id, actions: [], features: { ...none },
    fields: { image: field('image', { required: true }), model: field('enum', { required: true, values: [id] }), information_extracted: numeric(0, 1, { default: NATIVE_DEFAULTS.informationExtracted }), focus_seed: integer(Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER), info_extract_seed: integer(Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER), allowPurchasedAnlas: field('boolean', { default: false }) },
    requirements: [{ requires: ['image'], excludes: ['mask'] }], transforms: [] });
  return models;
}

export function memberGenerationCapabilities(db: FairQuotaDatabase, _member?: Pick<FairUser, 'id'>, { env = process.env }: { env?: NodeJS.ProcessEnv } = {}) {
  const models = generationModels(), payload = generationPayloadLimits(env), queue = queuePolicy(env), free = freeImagePolicy(db), quota = db.settings?.fairQuota;
  const operations = [
    { id: 'generate', inputContentTypes: ['application/json', 'multipart/form-data'], outputMimeTypes: ['image/png', 'image/webp'] },
    { id: 'upscale', inputContentTypes: ['application/json', 'multipart/form-data'], outputMimeTypes: ['image/png', 'image/webp'] },
    { id: 'encode-vibe', inputContentTypes: ['application/json', 'multipart/form-data'], outputMimeTypes: ['application/octet-stream'] },
  ];
  const limits = { wireBytes: NATIVE_MAX_BODY_BYTES, normalizedBytes: payload.request, activePayloadBytes: payload.active, attachmentBytes: Math.min(MAX_IMAGE_BYTES, NATIVE_LIMITS.imageBytes), multipartParts: NATIVE_MULTIPART_MAX_PARTS,
    sampleCount: { min: 1, max: NATIVE_LIMITS.samplesMax }, generationPixels: NATIVE_LIMITS.generationPixels, imagePixels: NATIVE_LIMITS.imagePixels, imageDimension: NATIVE_LIMITS.imageDimension,
    vibeEncodingBytes: NATIVE_LIMITS.vibeEncodingBytes,
    references: NATIVE_LIMITS.referencesMax, characters: NATIVE_LIMITS.charactersMax, centersPerCharacter: NATIVE_LIMITS.centersMax, queue };
  const billing = { units: { v5: 'percentage_points', anlasFixed: 'subscription_anlas', anlasPurchased: 'purchased_anlas' }, estimatedV5: true,
    allowPurchasedAnlasDefault: false, autoAnlasFallback: quota?.autoAnlasFallback === true, freeImagePolicy: { enabled: free.enabled, warningThreshold: free.warningThreshold, hardLimit: free.hardLimit } };
  const rules = models.map(({ label: _label, transforms, ...model }) => ({ ...model, transforms: transforms.map(({ description: _description, ...transform }) => transform) }))
    .sort((left, right) => `${left.operation}:${left.id}`.localeCompare(`${right.operation}:${right.id}`, 'en'));
  const defaults = { native: NATIVE_DEFAULTS, medium: applyV5MediumParameters({}, ''), allowPurchasedAnlas: false };
  const versions: MemberGenerationVersions = {
    capabilities: generationDefinitionDigest({ operations, rules, limits }),
    normalization: generationDefinitionDigest({ format: MEMBER_NORMALIZATION_FORMAT, rules, limits: NATIVE_LIMITS }),
    defaults: generationDefinitionDigest(defaults), pricing: NATIVE_PRICING_VERSION,
    billingPolicy: generationDefinitionDigest({ enabled: quota?.enabled === true, autoAnlasFallback: billing.autoAnlasFallback,
      groups: quota?.groups?.map(group => ({ id: group.id, members: [...group.members].sort(), share: group.share, memberWeights: group.memberWeights })).sort((left, right) => left.id.localeCompare(right.id, 'en')),
      free: { enabled: free.enabled, hardLimit: free.hardLimit }, queue, payload }),
  };
  return { contractVersion: MEMBER_GENERATION_CONTRACT_VERSION, versions, operations, models, limits, billing };
}
