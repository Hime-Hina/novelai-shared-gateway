import { createHash } from 'node:crypto';
import { canonicalJson, domainDigest } from '../events/domain-events.ts';
import {
  buildNovelAiParameters,
  type ProviderRequest,
  type NormalizedImageRequest,
} from '../protocols/novelai-client.ts';
import {
  nativeNormalizationContext,
  parseNativeImageRequest,
  parseNativeAuxiliaryRequest,
  validateImageBytes,
  validateVibeEncodingBytes,
  type NativeNormalizationContext,
} from '../protocols/native-image.ts';
import {
  recoveredInput,
  inputAssetSlots,
  decodeInputAsset,
  type RecoverableRequest,
  type MemberNormalizationContext,
} from './member-contract.ts';
import { requestNormalizationDefaults, requestCacheKey } from './request-fingerprint.ts';
import { generationPayloadLimits } from './payload-budget.ts';
import { MEMBER_NORMALIZATION_FORMAT } from './capabilities.ts';
import { singleGenerationRequest } from './batch-requests.ts';

export const RELATIONAL_INPUT_VERSION = 1;
export type RelationalNormalizationContext =
  | { kind: 'member'; member: MemberNormalizationContext }
  | { kind: 'native'; native: NativeNormalizationContext }
  | {
      kind: 'direct';
      native: NativeNormalizationContext;
      defaults: ReturnType<typeof requestNormalizationDefaults>;
    };
export interface RelationalInputAsset {
  assetId: string;
  mimeType: string;
  byteLength: number;
  sha256: string;
}
export interface PreparedRelationalInput {
  body: Record<string, unknown>;
  assets: (RelationalInputAsset & { bytes: Buffer })[];
  context: RelationalNormalizationContext;
  contextId: string;
  contextSha256: string;
  sha256: string;
  logicalBytes: number;
  wireBytes: number;
  sampleCount: number;
  firstSeed: number | null;
}

// Direct 的单图 literal seed 沿原 owner 保留；Native/member 才有 uint32 批次回绕。
export function relationalChildSeed(
  seed: number,
  sampleIndex: number,
  context: RelationalNormalizationContext,
): number {
  if (
    !Number.isFinite(seed) ||
    !Number.isSafeInteger(sampleIndex) ||
    sampleIndex < 0 ||
    (context.kind === 'direct'
      ? sampleIndex !== 0
      : !Number.isSafeInteger(seed) || seed < 0 || seed > 4294967295)
  ) {
    invalid();
  }
  return singleGenerationRequest(
    context.kind === 'direct' ? { seed } : { seed, nativeParameters: {} },
    sampleIndex,
  ).seed;
}

// 复用旧 direct 缓存等价；出站丢掉的原选择只取保存值，不按当前默认反推。
export function relationalInputCacheKey(
  memberId: string,
  input: Pick<PreparedRelationalInput, 'body' | 'assets' | 'context' | 'firstSeed' | 'sampleCount'>,
  seedMode: 'fixed' | 'random' | 'not_applicable',
): string | null {
  if (
    input.body.operation !== 'generate' ||
    input.sampleCount !== 1 ||
    seedMode === 'not_applicable' ||
    input.context.kind !== 'direct' ||
    input.body.cacheSelection === undefined
  ) {
    return null;
  }
  validateRelationalInputBody(input.body);
  const parameters = input.body.parameters as Record<string, unknown>,
    request = directInputRequest(
      input.body,
      input.firstSeed,
      input.body.cacheSelection as DirectCacheSelection,
    );
  const rebuilt = buildNovelAiParameters({
    ...request,
    normalizationContext: { native: input.context.native },
  } as ProviderRequest);
  delete rebuilt.seed;
  delete rebuilt.n_samples;
  if (input.body.action !== 'generate' || canonicalJson(rebuilt) !== canonicalJson(parameters)) {
    return null;
  }
  return requestCacheKey(memberId, request, seedMode === 'fixed' ? input.firstSeed : '');
}

interface DirectCacheSelection {
  size: string;
  cost?: number | null;
  purchaseField: boolean;
  sampler?: string;
}

function directInputRequest(
  body: Record<string, unknown>,
  seed: number | null,
  selection?: DirectCacheSelection,
) {
  const parameters = body.parameters as Record<string, unknown>,
    prompt = body.input as string,
    tag = (body.displayPrompt ?? prompt) as string,
    artist = directArtist(prompt, tag);
  // 顺序沿原 normalizeNovelAiRequest 返回值；requestedSteps 原 owner 明确不参与缓存。
  return {
    tag,
    prompt,
    artist,
    model: body.model,
    negative: parameters.negative_prompt,
    width: parameters.width,
    height: parameters.height,
    size: selection?.size,
    steps: parameters.steps,
    scale: parameters.scale,
    cfg: parameters.cfg_rescale,
    sampler: selection?.sampler ?? parameters.sampler,
    noiseSchedule: parameters.noise_schedule,
    seed,
    cost: selection?.cost,
    ...(selection?.purchaseField ? { allowPurchasedAnlas: body.allowPurchasedAnlas } : {}),
  };
}

function directArtist(prompt: string, tag: string): string {
  if (prompt === tag) {
    return '';
  }
  if (tag === '') {
    return prompt;
  }
  if (!prompt.endsWith('\n' + tag)) {
    invalid();
  }
  const artist = prompt.slice(0, -(tag.length + 1));
  if (!artist) {
    invalid();
  }
  return artist;
}

function invalid(): never {
  throw Object.assign(new Error('规范输入、附件或首次上下文不完整。'), {
    statusCode: 410,
    code: 'input_unavailable',
  });
}
function object(value: unknown): value is Record<string, unknown> {
  return (
    !!value &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    Object.getPrototypeOf(value) === Object.prototype
  );
}
function keys(value: Record<string, unknown>, allowed: string[]): void {
  if (Object.keys(value).some((key) => !allowed.includes(key))) {
    invalid();
  }
}
// 只保存已命名的规范化定义；不接收整个 settings、账号或认证对象。
function sameShape(value: unknown, template: unknown): void {
  if (Array.isArray(template)) {
    if (!Array.isArray(value)) {
      invalid();
    }
    for (const entry of value) {
      sameShape(entry, template[0]);
    }
  } else if (object(template)) {
    if (!object(value) || Object.keys(value).length !== Object.keys(template).length) {
      invalid();
    }
    keys(value, Object.keys(template));
    for (const [key, entry] of Object.entries(template)) {
      sameShape(value[key], entry);
    }
  } else if (
    typeof value !== typeof template ||
    (typeof value === 'number' && !Number.isFinite(value))
  ) {
    invalid();
  }
}
function nativeDefinition(value: unknown): void {
  const template = nativeNormalizationContext();
  if (!object(value) || Object.keys(value).length !== Object.keys(template).length) {
    invalid();
  }
  keys(value, Object.keys(template));
  for (const key of ['defaults', 'mediumProfile', 'limits'] as const) {
    sameShape(value[key], template[key]);
  }
  // 枚举只检查保存值的类型，不能按当前是否仍支持该模型/采样器拒绝历史上下文。
  for (const key of [
    'baseModels',
    'samplers',
    'noiseSchedules',
    'parameterNames',
    'booleanParameters',
    'vibeModels',
  ]) {
    if (!Array.isArray(value[key]) || value[key].some((entry) => typeof entry !== 'string')) {
      invalid();
    }
  }
  if (
    !Array.isArray(value.inpaintModels) ||
    value.inpaintModels.some(
      (entry) =>
        !Array.isArray(entry) ||
        entry.length !== 2 ||
        entry.some((item) => typeof item !== 'string'),
    ) ||
    !Array.isArray(value.blurValues) ||
    value.blurValues.some((entry) => typeof entry !== 'number' || !Number.isFinite(entry))
  ) {
    invalid();
  }
}
export function parseRelationalNormalizationContext(
  value: unknown,
): RelationalNormalizationContext {
  if (!object(value)) {
    invalid();
  }
  if (value.kind === 'member') {
    keys(value, ['kind', 'member']);
    if (!object(value.member)) {
      invalid();
    }
    const member = value.member;
    keys(member, ['format', 'versions', 'randomSeed', 'native']);
    if (
      member.format !== MEMBER_NORMALIZATION_FORMAT ||
      !object(member.versions) ||
      typeof member.versions.normalization !== 'string' ||
      !member.versions.normalization ||
      typeof member.versions.defaults !== 'string' ||
      !member.versions.defaults ||
      typeof member.randomSeed !== 'boolean'
    ) {
      invalid();
    }
    keys(member.versions, ['normalization', 'defaults']);
    nativeDefinition(member.native);
  } else if (value.kind === 'native' || value.kind === 'direct') {
    keys(value, value.kind === 'native' ? ['kind', 'native'] : ['kind', 'native', 'defaults']);
    nativeDefinition(value.native);
    if (value.kind === 'direct') {
      if (!object(value.defaults)) {
        invalid();
      }
      keys(value.defaults, ['defaultModel', 'defaultArtist', 'defaultNegative', 'defaults']);
      for (const key of ['defaultModel', 'defaultArtist', 'defaultNegative']) {
        if (
          value.defaults[key] !== undefined &&
          value.defaults[key] !== null &&
          !['string', 'number', 'boolean'].includes(typeof value.defaults[key])
        ) {
          invalid();
        }
      }
      if (!object(value.defaults.defaults)) {
        invalid();
      }
      keys(value.defaults.defaults, [
        'size',
        'width',
        'height',
        'steps',
        'scale',
        'cfg',
        'sampler',
        'noiseSchedule',
      ]);
      for (const [key, entry] of Object.entries(value.defaults.defaults)) {
        if (
          (entry !== null && !['string', 'number', 'boolean'].includes(typeof entry)) ||
          (typeof entry === 'number' && !Number.isFinite(entry))
        ) {
          invalid();
        }
      }
    }
  } else {
    invalid();
  }
  canonicalJson(value);
  return structuredClone(value) as RelationalNormalizationContext;
}
export function inputNativeContext(
  context: RelationalNormalizationContext,
): NativeNormalizationContext {
  return context.kind === 'member' ? context.member.native : context.native;
}
export function relationalInputDigest(
  body: Record<string, unknown>,
  assets: RelationalInputAsset[],
  logicalBytes: number,
): string {
  return domainDigest({
    schemaVersion: RELATIONAL_INPUT_VERSION,
    body,
    logicalBytes,
    assets: [...assets].sort((a, b) =>
      a.assetId < b.assetId ? -1 : Number(a.assetId > b.assetId),
    ),
  });
}

function canonicalGenerationWire(request: RecoverableRequest, native: NativeNormalizationContext) {
  const parameters = buildNovelAiParameters({
      ...request,
      normalizationContext: { native },
    } as ProviderRequest),
    wire = recoveredInput({ ...request, nativeParameters: parameters });
  if (!wire) {
    invalid();
  }
  return wire;
}

// seed/count 不在规范正文中；这里只用临时确定值校验与种子无关的原定义，不恢复历史意图。
export function assertRelationalCompleteInput(
  body: Record<string, unknown>,
  assets: (RelationalInputAsset & { bytes: Buffer })[],
  context: RelationalNormalizationContext,
  sampleCount: number,
) {
  const wire = restoreRelationalWire(body, assets, 0, sampleCount, context),
    native = inputNativeContext(context);
  let expected: Record<string, unknown> | null;
  if (wire.operation === 'generate') {
    if (context.kind === 'direct') {
      // Direct 的原 DTO 没有 nativeParameters；不能借透传分支为外加字段背书。
      expected = recoveredInput({
        ...directInputRequest(body, 0),
        normalizationContext: { native },
        allowPurchasedAnlas: wire.allowPurchasedAnlas === true,
        sampleCount,
      } as RecoverableRequest);
    } else {
      const { operation: _operation, ...nativeBody } = wire;
      expected = canonicalGenerationWire(
        {
          ...parseNativeImageRequest(nativeBody, native),
          allowPurchasedAnlas: wire.allowPurchasedAnlas === true,
        },
        native,
      );
    }
  } else {
    const { operation, ...nativeBody } = wire,
      request = parseNativeAuxiliaryRequest(nativeBody, operation, native);
    if (request.sampleCount !== sampleCount) {
      invalid();
    }
    expected = recoveredInput({
      ...request,
      allowPurchasedAnlas: wire.allowPurchasedAnlas === true,
    });
  }
  if (canonicalJson(expected) !== canonicalJson(wire)) {
    invalid();
  }
}

export function prepareRelationalInput(
  request: RecoverableRequest,
  capturedContext: RelationalNormalizationContext,
  env: NodeJS.ProcessEnv = process.env,
): PreparedRelationalInput {
  const context = parseRelationalNormalizationContext(capturedContext),
    native = inputNativeContext(context);
  const wire = recoveredInput(request);
  if (!wire) {
    invalid();
  }
  const generation = wire.operation === 'generate';
  let sampleCount = 1,
    firstSeed: number | null = null;
  if (generation) {
    sampleCount = request.sampleCount || 1;
    firstSeed = request.seed ?? null;
    if (
      !Number.isSafeInteger(sampleCount) ||
      sampleCount < 1 ||
      sampleCount > native.limits.samplesMax ||
      (context.kind === 'direct' && sampleCount !== 1) ||
      firstSeed === null
    ) {
      invalid();
    }
    relationalChildSeed(firstSeed, 0, context);
    // 复用实际出站构造器，随后正文只留一种参数表示；seed/count 归 child/parent。
    const cleaned = canonicalGenerationWire(request, native);
    wire.parameters = cleaned.parameters;
    (wire.parameters as Record<string, unknown>).n_samples = sampleCount;
  }
  // 入口 owner 已按各自契约规范化；持久层只校验保存表示，不能借 Native 收窄 direct。
  const wireBytes = Buffer.byteLength(canonicalJson(wire));
  // 保留原规范化 DTO 的活动预算下界；BLOB 去重和别名删除不能扩大受理上限。
  const logicalBytes = Math.max(wireBytes, Buffer.byteLength(JSON.stringify(request)));
  if (logicalBytes > generationPayloadLimits(env).request) {
    throw Object.assign(new Error('规范化生成请求超过载荷大小限制'), {
      statusCode: 413,
      code: 'payload_too_large',
    });
  }
  const body = structuredClone(wire),
    assets: PreparedRelationalInput['assets'] = [];
  for (const slot of inputAssetSlots(body)) {
    if (typeof slot.value !== 'string') {
      invalid();
    }
    const decoded = decodeInputAsset(slot, { native });
    const sha256 = createHash('sha256').update(decoded.bytes).digest('hex');
    let asset = assets.find(
      (entry) =>
        entry.byteLength === decoded.bytes.length &&
        entry.sha256 === sha256 &&
        entry.bytes.equals(decoded.bytes),
    );
    if (!asset) {
      asset = {
        assetId: `a${assets.length + 1}`,
        mimeType: decoded.mimeType,
        byteLength: decoded.bytes.length,
        sha256,
        bytes: decoded.bytes,
      };
      assets.push(asset);
    }
    (slot.owner as Record<string | number, unknown>)[slot.key] = { assetId: asset.assetId };
  }
  if (generation) {
    const parameters = body.parameters as Record<string, unknown>;
    delete parameters.seed;
    delete parameters.n_samples;
    if (typeof request.tag === 'string' && request.tag !== request.prompt) {
      body.displayPrompt = request.tag;
    }
    if (request.requestedSteps !== undefined && request.requestedSteps !== request.steps) {
      body.requestedSteps = request.requestedSteps;
    }
    const direct = request as RecoverableRequest &
      Partial<Pick<NormalizedImageRequest, 'artist' | 'size' | 'cost'>>;
    if (context.kind === 'direct' && typeof direct.size === 'string') {
      if (
        typeof direct.tag !== 'string' ||
        typeof direct.artist !== 'string' ||
        directArtist(direct.prompt!, direct.tag) !== direct.artist ||
        (direct.cost !== undefined && typeof direct.cost !== 'number') ||
        (direct.allowPurchasedAnlas !== undefined &&
          typeof direct.allowPurchasedAnlas !== 'boolean')
      ) {
        invalid();
      }
      body.cacheSelection = {
        size: direct.size,
        ...(direct.cost !== undefined
          ? { cost: Number.isFinite(direct.cost) ? direct.cost : null }
          : {}),
        purchaseField: direct.allowPurchasedAnlas !== undefined,
        ...(direct.sampler !== parameters.sampler ? { sampler: direct.sampler } : {}),
      };
    }
  }
  const contextSha256 = domainDigest(context);
  const metadata = assets.map(({ bytes: _bytes, ...asset }) => asset);
  validateRelationalInputBody(body);
  relationalAssetSlots(body, metadata);
  return {
    body,
    assets,
    context,
    contextId: `ctx_${contextSha256}`,
    contextSha256,
    sha256: relationalInputDigest(body, metadata, logicalBytes),
    logicalBytes,
    wireBytes,
    sampleCount,
    firstSeed,
  };
}

export function relationalAssetSlots(
  body: Record<string, unknown>,
  assets: RelationalInputAsset[],
) {
  const slots = inputAssetSlots(body),
    used = new Set<string>();
  for (const slot of slots) {
    if (
      !object(slot.value) ||
      Object.keys(slot.value).length !== 1 ||
      typeof slot.value.assetId !== 'string'
    ) {
      invalid();
    }
    const asset = assets.find(
      (entry) => entry.assetId === (slot.value as { assetId: string }).assetId,
    );
    if (
      !asset ||
      asset.byteLength < 1 ||
      !Number.isSafeInteger(asset.byteLength) ||
      !/^[a-f0-9]{64}$/.test(asset.sha256)
    ) {
      invalid();
    }
    used.add(asset.assetId);
  }
  if (
    used.size !== assets.length ||
    new Set(assets.map((asset) => asset.assetId)).size !== assets.length
  ) {
    invalid();
  }
  return slots;
}

export function validateRelationalInputBody(body: Record<string, unknown>, complete = true): void {
  if (!object(body)) {
    invalid();
  }
  if (!complete) {
    keys(body, [
      'operation',
      'input',
      'model',
      'action',
      'parameters',
      'allowPurchasedAnlas',
      'displayPrompt',
      'requestedSteps',
      'image',
      'declared_blur_sigma',
      'information_extracted',
      'focus_seed',
      'info_extract_seed',
    ]);
    if (body.parameters !== null && body.parameters !== undefined) {
      if (!object(body.parameters)) {
        invalid();
      }
      keys(
        body.parameters,
        nativeNormalizationContext().parameterNames.filter(
          (key) =>
            !['seed', 'n_samples'].includes(key) &&
            !key.includes('cache_secret_key') &&
            !key.endsWith('_cached'),
        ),
      );
    }
    canonicalJson(body);
    return;
  }
  if (
    !['generate', 'upscale', 'encode-vibe'].includes(String(body.operation)) ||
    typeof body.model !== 'string' ||
    !body.model ||
    typeof body.allowPurchasedAnlas !== 'boolean'
  ) {
    invalid();
  }
  if (body.operation === 'generate') {
    keys(body, [
      'operation',
      'input',
      'model',
      'action',
      'parameters',
      'allowPurchasedAnlas',
      'displayPrompt',
      'requestedSteps',
      'cacheSelection',
    ]);
    if (
      typeof body.input !== 'string' ||
      !['generate', 'img2img', 'infill'].includes(String(body.action)) ||
      !object(body.parameters) ||
      Object.hasOwn(body.parameters, 'seed') ||
      Object.hasOwn(body.parameters, 'n_samples') ||
      (body.displayPrompt !== undefined && typeof body.displayPrompt !== 'string') ||
      (body.requestedSteps !== undefined &&
        (typeof body.requestedSteps !== 'number' || !Number.isFinite(body.requestedSteps)))
    ) {
      invalid();
    }
    if (body.cacheSelection !== undefined) {
      const selection = body.cacheSelection;
      if (!object(selection)) {
        invalid();
      }
      keys(selection, ['size', 'cost', 'purchaseField', 'sampler']);
      if (
        typeof selection.size !== 'string' ||
        typeof selection.purchaseField !== 'boolean' ||
        (!selection.purchaseField && body.allowPurchasedAnlas !== false) ||
        (selection.cost !== undefined &&
          selection.cost !== null &&
          (typeof selection.cost !== 'number' || !Number.isFinite(selection.cost))) ||
        (selection.sampler !== undefined && typeof selection.sampler !== 'string')
      ) {
        invalid();
      }
      directArtist(body.input as string, (body.displayPrompt ?? body.input) as string);
    }
  } else {
    keys(
      body,
      body.operation === 'upscale'
        ? ['operation', 'image', 'model', 'declared_blur_sigma', 'allowPurchasedAnlas']
        : [
            'operation',
            'image',
            'model',
            'information_extracted',
            'focus_seed',
            'info_extract_seed',
            'allowPurchasedAnlas',
          ],
    );
    if (!Object.hasOwn(body, 'image')) {
      invalid();
    }
  }
}

export function validateRelationalAssetBytes(
  asset: RelationalInputAsset & { bytes: Buffer },
  image: boolean,
  context: RelationalNormalizationContext,
): void {
  if (
    asset.bytes.length !== asset.byteLength ||
    createHash('sha256').update(asset.bytes).digest('hex') !== asset.sha256
  ) {
    invalid();
  }
  const limits = inputNativeContext(context).limits;
  try {
    if (image) {
      if (
        validateImageBytes(asset.bytes, {
          maxBytes: limits.imageBytes,
          maxDimension: limits.imageDimension,
          maxPixels: limits.imagePixels,
        }).mimeType !== asset.mimeType
      ) {
        invalid();
      }
    } else {
      if (asset.mimeType !== 'application/octet-stream') {
        invalid();
      }
      validateVibeEncodingBytes(asset.bytes, limits.vibeEncodingBytes);
    }
  } catch {
    invalid();
  }
}

export function restoreRelationalWire(
  body: Record<string, unknown>,
  assets: (RelationalInputAsset & { bytes: Buffer })[],
  seed: number | null,
  sampleCount: number,
  context: RelationalNormalizationContext,
): Record<string, unknown> {
  const wire = structuredClone(body);
  for (const slot of relationalAssetSlots(wire, assets)) {
    const asset = assets.find(
      (entry) => entry.assetId === (slot.value as { assetId: string }).assetId,
    )!;
    if (
      asset.bytes.length !== asset.byteLength ||
      createHash('sha256').update(asset.bytes).digest('hex') !== asset.sha256
    ) {
      invalid();
    }
    (slot.owner as Record<string | number, unknown>)[slot.key] = asset.bytes.toString('base64');
  }
  delete wire.displayPrompt;
  delete wire.requestedSteps;
  delete wire.cacheSelection;
  if (wire.operation === 'generate') {
    if (seed === null || (context.kind === 'direct' && sampleCount !== 1)) {
      invalid();
    }
    const parameters = wire.parameters as Record<string, unknown>;
    parameters.seed = relationalChildSeed(seed, 0, context);
    parameters.n_samples = sampleCount;
  }
  return wire;
}

// complete v1 省略与规范正文相等的显示值；partial 没有这个编码保证。
export function relationalInputDisplay(body: Record<string, unknown> | null, complete: boolean) {
  const parameters = body?.parameters as Record<string, unknown> | null | undefined,
    tag = body?.displayPrompt ?? (complete ? body?.input : null),
    requestedSteps = body?.requestedSteps ?? (complete ? parameters?.steps : null);
  return {
    tag: typeof tag === 'string' ? tag : null,
    requestedSteps:
      typeof requestedSteps === 'number' && Number.isFinite(requestedSteps) ? requestedSteps : null,
  };
}

// 仅在执行边界展开 Base64 和兼容现有上游客户端 DTO，不把此临时对象写回关系状态。
export function relationalProviderRequest(
  wire: Record<string, unknown>,
  context: RelationalNormalizationContext,
): ProviderRequest & {
  allowPurchasedAnlas: boolean;
  operationParameters?: ReturnType<typeof parseNativeAuxiliaryRequest>['operationParameters'];
} {
  if (wire.operation !== 'generate') {
    const { operation, ...body } = wire;
    return {
      ...parseNativeAuxiliaryRequest(body, operation, inputNativeContext(context)),
      allowPurchasedAnlas: wire.allowPurchasedAnlas === true,
    };
  }
  const parameters = wire.parameters as NonNullable<ProviderRequest['nativeParameters']>;
  return {
    action: wire.action as string,
    model: wire.model as string,
    prompt: wire.input as string,
    width: parameters.width,
    height: parameters.height,
    steps: parameters.steps,
    seed: parameters.seed,
    nativeParameters: parameters,
    normalizationContext: { native: inputNativeContext(context) },
    allowPurchasedAnlas: wire.allowPurchasedAnlas === true,
  };
}
