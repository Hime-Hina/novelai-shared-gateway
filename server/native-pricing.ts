import { isV5MediumModel, V5_MEDIUM_STEPS, V5_MEDIUM_ANLAS_STEP_FACTOR, V5_MEDIUM_USAGE_FACTOR } from '../public/v5-medium.js';
import { createHash } from 'node:crypto';
import { MAX_IMAGE_PIXELS, NATIVE_LIMITS } from './native-image.ts';

interface PricingParameters {
  width?: unknown;
  height?: unknown;
  steps?: unknown;
  n_samples?: unknown;
  sm?: unknown;
  sm_dyn?: unknown;
  image?: unknown;
  mask?: unknown;
  strength?: unknown;
  img2img?: { strength?: unknown } | null;
  inpaintImg2ImgStrength?: unknown;
  reference_image?: unknown;
  reference_image_multiple?: unknown;
  reference_image_multiple_cached?: unknown;
  director_reference_images?: unknown;
  director_reference_images_cached?: unknown;
}

interface PricingRequest {
  model?: string;
  operation?: string;
  action?: string;
  width?: unknown;
  height?: unknown;
  steps?: unknown;
  n_samples?: unknown;
  n?: unknown;
  sm?: unknown;
  sm_dyn?: unknown;
  nativeParameters?: PricingParameters | null;
  parameters?: PricingParameters | null;
  operationParameters?: { image?: unknown } | null;
}

interface NativeQuote {
  resource: 'free' | 'v5' | 'anlas';
  amount: number;
  estimated: boolean;
  breakdown: { generation: number; vibe: number; precise: number } | { encoding: number } | { upscale: number };
}

// 2026-10-10 核对官网 50464 / 63509；Medium 的步数项在第一次取整前单独修正。
// 官方生图公式先对基础费用取整，再乘 SMEA、V5 和图生图强度，最后取整。
const v5Models = /^nai-diffusion-5-(?:full(?:-inpainting)?|curated)$/;
const legacyModels = /^nai-diffusion-(?:(?:furry-)?3(?:-inpainting)?|4-(?:full|curated-preview|curated-inpainting|full-inpainting)|4-5-(?:full|curated)(?:-inpainting)?)$/;
const vibeModels = /^nai-diffusion-4(?:-5)?-(?:full|curated|curated-preview)$/;
export const NATIVE_PRICING_DEFINITION = {
  algorithm: 'independent-image-ceil-base-then-multiply-v1',
  coefficientA: 2.951823174884865e-6, coefficientB: 5.753298233447344e-7, megapixel: 1048576,
  generationMaxPixels: NATIVE_LIMITS.generationPixels, encodingMaxPixels: MAX_IMAGE_PIXELS, maxSteps: NATIVE_LIMITS.stepsMax,
  minimumBase: 2, maximumBase: 140, opusFreeSteps: 28, v5AnlasMultiplier: 1.5,
  smeaMultiplier: 1.2, smeaDynamicMultiplier: 1.4, encodingCost: 2, preciseReferenceCost: 5,
  includedVibes: 4, additionalVibeCost: 2, referenceLimit: NATIVE_LIMITS.referencesMax,
  v5Capacity: 100, v5BaselineImages: 1730, v5BaselineSteps: 23,
  medium: { steps: V5_MEDIUM_STEPS, anlasStepFactor: V5_MEDIUM_ANLAS_STEP_FACTOR, usageFactor: V5_MEDIUM_USAGE_FACTOR },
  upscaleTiers: [{ pixels: 1048576, amount: 1 }, { pixels: 1747627, amount: 2 }, { pixels: 2446678, amount: 3 }, { pixels: NATIVE_LIMITS.generationPixels, amount: 4 }],
  models: { v5: v5Models.source, legacy: legacyModels.source, vibeEncoding: vibeModels.source },
};
// Constants are shared by the calculator and digest; only changes to rule structure need an algorithm revision.
export const NATIVE_PRICING_VERSION = createHash('sha256').update(JSON.stringify(NATIVE_PRICING_DEFINITION)).digest('hex');
const rules = NATIVE_PRICING_DEFINITION, A = rules.coefficientA, B = rules.coefficientB, MP = rules.megapixel;
function fail(message: string): never { throw Object.assign(new Error(message), {status:422, statusCode:422}); }
const numeric = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value);
const present = (value: unknown): boolean => Array.isArray(value) ? value.length > 0 : Boolean(value);

export function quoteNativeRequest(request: PricingRequest = {}, options: { forceAnlas?: unknown } = {}): NativeQuote {
  const p = request.nativeParameters || request.parameters || {};
  const width = Number(request.width ?? p.width), height = Number(request.height ?? p.height);
  const pixels = width * height;
  const maxPixels = request.operation === 'encode-vibe' ? rules.encodingMaxPixels : rules.generationMaxPixels;
  if (!Number.isInteger(width) || !Number.isInteger(height) || width < 1 || height < 1 || pixels > maxPixels) fail('无法计价：图片尺寸无效或超过官方上限');
  const count = Number(request.n_samples ?? p.n_samples ?? request.n ?? 1);
  if (count !== 1) fail('多图必须逐张排队与结算');
  if (request.operation === 'encode-vibe') {
    if (!vibeModels.test(request.model ?? '') || !request.operationParameters?.image) fail('Vibe 编码参数不完整');
    return {resource:'anlas', amount:rules.encodingCost, estimated:false, breakdown:{encoding:rules.encodingCost}};
  }
  if (request.operation === 'upscale') {
    if (request.model !== 'nai-diffusion-5-curated' || !request.operationParameters?.image) fail('放大参数不完整');
    const amount = rules.upscaleTiers.find(tier => pixels <= tier.pixels)!.amount;
    return {resource:'anlas', amount, estimated:false, breakdown:{upscale:amount}};
  }
  if (request.operation !== undefined) fail('该操作尚未建立公平计价规则');
  // 旧账本中保存过简写，保留重启后的历史任务兼容；原生入口仍只接受官方模型名。
  const model = request.model === 'nai-diffusion-5' ? 'nai-diffusion-5-full' : String(request.model || '');
  const medium = isV5MediumModel(model);
  const v5 = medium || v5Models.test(model);
  const legacy = legacyModels.test(model);
  if (!v5 && !legacy) fail('该模型尚未建立公平计价规则');
  const steps = Number(request.steps ?? p.steps), action = request.action || 'generate';
  if (!Number.isInteger(steps) || steps < 1 || steps > rules.maxSteps) fail('无法计价：步数无效');
  if (medium && steps !== rules.medium.steps) fail('Medium 必须先转换为固定 14 步再计价');
  if (!['generate','img2img','infill'].includes(action)) fail('该动作尚未建立公平计价规则');
  if (model === 'nai-diffusion-5-curated' && action === 'infill') fail('V5 Curated 的局部重绘请使用 V4.5 Curated 的重绘模型');
  const refs = (plain: 'reference_image_multiple' | 'director_reference_images', cached: 'reference_image_multiple_cached' | 'director_reference_images_cached'): number => {
    const a=p[plain] || [], b=p[cached] || [];
    if (!Array.isArray(a) || !Array.isArray(b) || a.length && b.length) fail('参考数据格式不明确，无法计价');
    const values: unknown[] = a.length ? a : b.map((item: { data?: unknown } | null | undefined) => item?.data);
    if (values.length > rules.referenceLimit || values.some(value => typeof value !== 'string' || value.length < 100)) fail('参考数据无效，请先上传或编码');
    return values.length;
  };
  const vibes = refs('reference_image_multiple','reference_image_multiple_cached');
  const precise = refs('director_reference_images','director_reference_images_cached');
  if (present(p.reference_image)) fail('单张 Vibe 必须先归一为参考数组');
  if (v5 && (vibes || precise)) fail('V5 官方尚不支持 Vibe 或 Precise Reference');
  if (precise && (!model.startsWith('nai-diffusion-4-5-') || vibes)) fail('Precise Reference 仅支持 V4.5 且不能与 Vibe 同用');
  if (vibes && action === 'infill') fail('局部重绘不能使用 Vibe');
  let strength = 1;
  if (action !== 'generate') {
    if (!present(p.image) || action === 'infill' && !present(p.mask)) fail('图生图或局部重绘缺少底图或遮罩');
    const value = action === 'infill' ? p.img2img?.strength ?? p.inpaintImg2ImgStrength ?? 1 : p.strength;
    if (!numeric(value) || value < 0 || value > 1) fail('无法计价：图生图强度无效');
    strength = value;
  } else if (present(p.image) || present(p.mask)) fail('纯文生图不能携带底图或遮罩');
  const sm = Boolean(request.sm ?? p.sm), dyn = Boolean(request.sm_dyn ?? p.sm_dyn);
  if (action !== 'generate' && (sm || dyn)) fail('图生图不能使用 SMEA');
  const base = Math.max(rules.minimumBase, Math.ceil(Math.ceil(A*pixels+B*pixels*steps*(medium ? rules.medium.anlasStepFactor : 1))*(sm ? dyn ? rules.smeaDynamicMultiplier : rules.smeaMultiplier : 1)*(v5 ? rules.v5AnlasMultiplier : 1)*strength));
  if (base > rules.maximumBase) fail('本次基础生成费用超过官方单张上限，请降低尺寸或步数');
  const vibeCost = model.startsWith('nai-diffusion-4-') ? rules.additionalVibeCost*Math.max(0,vibes-rules.includedVibes) : 0;
  const extras = vibeCost + rules.preciseReferenceCost*precise;
  // Opus 生成校准：标准 Opus Precise 保留基础免费，只收每个参考 5 Anlas。
  const free = pixels <= MP && steps <= rules.opusFreeSteps;
  const breakdown = {generation:free ? 0 : base, vibe:vibeCost, precise:rules.preciseReferenceCost*precise};
  if (v5 && free && options.forceAnlas !== true) return {resource:'v5', amount:(rules.v5Capacity/rules.v5BaselineImages)*(pixels/MP)*(medium ? rules.medium.usageFactor : (A+B*steps)/(A+B*rules.v5BaselineSteps))*strength, estimated:true, breakdown};
  if (legacy && free && extras === 0) return {resource:'free', amount:0, estimated:false, breakdown};
  const amount = (legacy && free ? 0 : base) + extras;
  return {resource:'anlas', amount, estimated:true, breakdown:{...breakdown,generation:amount-extras}};
}
