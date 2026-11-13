import { randomBytes } from 'node:crypto';
import {
  parseNativeImageRequest,
  parseNativeAuxiliaryRequest,
  nativeNormalizationContext,
  validateImageBytes,
  type NativeGenerationRequest,
  type NativeAuxiliaryRequest,
  type NativeNormalizationContext,
} from '../protocols/native-image.ts';
import { generationRequestFingerprint } from './request-fingerprint.ts';
import { buildNovelAiParameters, type ProviderRequest } from '../protocols/novelai-client.ts';
import { singleGenerationRequest, generationPayloadLimits } from './batch-requests.ts';
import { assertQueueAdmission } from './queue-policy.ts';
import {
  assertFreeImageAdmission,
  freeImagePolicy,
  type FreeImageUsage,
} from '../quota/free-image-policy.ts';
import {
  estimateFairQuota,
  reserveFairQuota,
  previewFairQuotaCharge,
  type FairQuotaDatabase,
  type FairUser,
} from '../quota/fair-share.ts';
import { parseAmount, formatAmount } from '../quota/resource-amounts.ts';
import { publicErrorMessage } from '../http/public-errors.ts';
import {
  memberGenerationCapabilities,
  generationDefinitionDigest,
  MEMBER_NORMALIZATION_FORMAT,
  type MemberGenerationOperation,
  type MemberGenerationVersions,
} from './capabilities.ts';

export type MemberGenerationResources = { v5: string; anlasFixed: string; anlasPurchased: string };
export interface MemberGenerationConfirmation {
  requestFingerprint: string;
  versions: MemberGenerationVersions;
  maxPerJob: MemberGenerationResources;
  maxTotal: MemberGenerationResources;
}
export interface MemberNormalizationContext {
  format: typeof MEMBER_NORMALIZATION_FORMAT;
  versions: Pick<MemberGenerationVersions, 'normalization' | 'defaults'>;
  randomSeed: boolean;
  native: NativeNormalizationContext;
}
export type MemberGenerationRequest = (NativeGenerationRequest | NativeAuxiliaryRequest) & {
  allowPurchasedAnlas: boolean;
  normalizationContext?: MemberNormalizationContext;
};
export interface NormalizedMemberGeneration {
  operation: MemberGenerationOperation;
  request: MemberGenerationRequest;
  randomSeed: boolean;
  requestFingerprint: string;
  normalizationContext: MemberNormalizationContext;
  confirmation?: MemberGenerationConfirmation;
  warnings: { code: string; message: string; field?: string }[];
}
export type MemberGenerationMember = FairUser & {
  acceptingJobs?: boolean;
  jobAdmissionVersion?: number;
};
export interface MemberQuoteOptions {
  now?: number;
  env?: NodeJS.ProcessEnv;
  freeUsage?: FreeImageUsage;
  activePayloadBytes?: number;
  executorAvailable?: boolean;
}
export interface MemberGenerationReason {
  code: string;
  statusCode: number;
  message: string;
  retryAt?: string;
}
const resourceNames = ['v5', 'anlasFixed', 'anlasPurchased'] as const;
const versionNames = [
  'capabilities',
  'normalization',
  'defaults',
  'pricing',
  'billingPolicy',
] as const;
const object = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === 'object' && !Array.isArray(value);
const rows = <T>(value: T[] | Record<string, T> | null | undefined): T[] =>
  Array.isArray(value) ? value : Object.values(value || {});
const zeroResources = (): MemberGenerationResources => ({
  v5: '0',
  anlasFixed: '0',
  anlasPurchased: '0',
});
function fail(
  statusCode: number,
  code: string,
  message: string,
  details?: Record<string, unknown>,
): never {
  throw Object.assign(new Error(message), { statusCode, code, ...(details ? { details } : {}) });
}
function exactKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  return (
    Object.keys(value).length === keys.length && keys.every((key) => Object.hasOwn(value, key))
  );
}

export function parseMemberGenerationConfirmation(value: unknown): MemberGenerationConfirmation {
  if (value === undefined || value === null) {
    fail(400, 'confirmation_required', '请先报价并明确确认费用上限。');
  }
  if (
    !object(value) ||
    !exactKeys(value, ['requestFingerprint', 'versions', 'maxPerJob', 'maxTotal']) ||
    typeof value.requestFingerprint !== 'string' ||
    !/^[a-f0-9]{64}$/.test(value.requestFingerprint) ||
    !object(value.versions) ||
    !exactKeys(value.versions, versionNames) ||
    !versionNames.every(
      (key) =>
        typeof (value.versions as Record<string, unknown>)[key] === 'string' &&
        ((value.versions as Record<string, unknown>)[key] as string).length > 0,
    )
  ) {
    fail(400, 'invalid_body', '费用确认的指纹或版本格式无效。');
  }
  const amounts = (input: unknown): MemberGenerationResources => {
    if (!object(input) || !exactKeys(input, resourceNames)) {
      fail(400, 'invalid_body', '费用确认必须包含三种资源的上限。');
    }
    const result = zeroResources();
    for (const resource of resourceNames) {
      const amount = input[resource];
      if (
        typeof amount !== 'string' ||
        amount.length > 64 ||
        !/^(0|[1-9]\d*)(\.\d{1,12})?$/.test(amount)
      ) {
        fail(400, 'invalid_body', '费用上限须为不超过 12 位小数的非负十进制字符串。', {
          field: resource,
        });
      }
      result[resource] = formatAmount(parseAmount(amount));
    }
    return result;
  };
  return {
    requestFingerprint: value.requestFingerprint,
    versions: { ...value.versions } as unknown as MemberGenerationVersions,
    maxPerJob: amounts(value.maxPerJob),
    maxTotal: amounts(value.maxTotal),
  };
}

// The stored context selects this format's parser; new admission always captures current definitions.
export function normalizeMemberGeneration(
  input: unknown,
  { normalizationContext }: { normalizationContext?: MemberNormalizationContext } = {},
): NormalizedMemberGeneration {
  if (!object(input)) {
    fail(400, 'invalid_body', '请求正文必须是对象。');
  }
  const operation = input.operation;
  if (operation !== 'generate' && operation !== 'upscale' && operation !== 'encode-vibe') {
    fail(422, 'invalid_parameters', 'operation 必须为 generate、upscale 或 encode-vibe。');
  }
  if (normalizationContext && normalizationContext.format !== MEMBER_NORMALIZATION_FORMAT) {
    fail(409, 'idempotency_conflict', '无法按原请求的规范化版本重放。');
  }
  const body = structuredClone(input),
    confirmation = Object.hasOwn(body, 'confirmation')
      ? parseMemberGenerationConfirmation(body.confirmation)
      : undefined;
  delete body.operation;
  delete body.confirmation;
  const randomSeed =
    operation === 'generate' &&
    object(body.parameters) &&
    (body.parameters.seed === undefined || body.parameters.seed === -1);
  // Quoting is deterministic and does not select the accepted seed; the original intent remains random.
  if (randomSeed) {
    (body.parameters as Record<string, unknown>).seed = 0;
  }
  const current = normalizationContext ? undefined : memberGenerationCapabilities({});
  const context: MemberNormalizationContext = normalizationContext || {
    format: MEMBER_NORMALIZATION_FORMAT,
    versions: {
      normalization: current!.versions.normalization,
      defaults: current!.versions.defaults,
    },
    randomSeed,
    native: nativeNormalizationContext(),
  };
  let request: MemberGenerationRequest;
  try {
    request = {
      ...(operation === 'generate'
        ? parseNativeImageRequest(body, context.native)
        : parseNativeAuxiliaryRequest(body, operation, context.native)),
      allowPurchasedAnlas: body.allowPurchasedAnlas === true,
    };
  } catch (error) {
    const source = error as Error & { statusCode?: number };
    fail(
      source.statusCode || 422,
      source.statusCode === 400 ? 'invalid_body' : 'invalid_parameters',
      source.message,
    );
  }
  const warnings: NormalizedMemberGeneration['warnings'] = [];
  if (operation === 'generate' && object(input.parameters)) {
    const p = input.parameters,
      actual = (request as NativeGenerationRequest).nativeParameters;
    if (request.model !== input.model) {
      warnings.push({
        code: 'infill_model',
        message: '局部重绘已使用对应的重绘模型。',
        field: 'model',
      });
    }
    for (const key of ['steps', 'sampler', 'cfg_rescale', 'negative_prompt']) {
      if (p[key] !== undefined && p[key] !== actual[key]) {
        warnings.push({
          code: 'parameter_normalized',
          message: '此参数已按模型的固定规则规范化。',
          field: `parameters.${key}`,
        });
      }
    }
  }
  const fingerprintRequest = { ...request, normalizationContext: context };
  return {
    operation,
    request,
    randomSeed,
    requestFingerprint: generationRequestFingerprint(fingerprintRequest, {
      randomSeed,
      noCache: true,
    }),
    normalizationContext: context,
    ...(confirmation ? { confirmation } : {}),
    warnings,
  };
}

export function materializeMemberGeneration(
  normalized: NormalizedMemberGeneration,
  seed?: number,
): MemberGenerationRequest {
  const request = {
    ...structuredClone(normalized.request),
    normalizationContext: structuredClone(normalized.normalizationContext),
  };
  if (!normalized.randomSeed || !('nativeParameters' in request)) {
    return request;
  }
  const actual = seed ?? randomBytes(4).readUInt32LE(0);
  if (!Number.isInteger(actual) || actual < 0 || actual > 4294967295) {
    throw new Error('Materialized generation seed must be uint32.');
  }
  return {
    ...request,
    seed: actual,
    nativeParameters: { ...request.nativeParameters, seed: actual },
  };
}

export function memberGenerationIntentFingerprint(
  normalized: Pick<NormalizedMemberGeneration, 'requestFingerprint' | 'confirmation'>,
  confirmation = normalized.confirmation,
): string {
  return generationDefinitionDigest({
    clientContract: 'member-generation-v1',
    requestFingerprint: normalized.requestFingerprint,
    confirmation: parseMemberGenerationConfirmation(confirmation),
  });
}

export function assertMemberGenerationVersions(
  confirmation: MemberGenerationConfirmation,
  versions: MemberGenerationVersions,
): void {
  if (versionNames.some((key) => confirmation.versions[key] !== versions[key])) {
    fail(409, 'quote_changed', '生成规则已变化，请重新报价。', { requote: true });
  }
}

export function assertMemberGenerationCost(
  confirmation: MemberGenerationConfirmation,
  resources: MemberGenerationResources,
  totalWithCurrent: MemberGenerationResources = resources,
): void {
  for (const resource of resourceNames) {
    if (
      parseAmount(resources[resource]) > parseAmount(confirmation.maxPerJob[resource]) ||
      parseAmount(totalWithCurrent[resource]) > parseAmount(confirmation.maxTotal[resource])
    ) {
      fail(409, 'cost_limit_exceeded', '当前费用超过原确认的单张或整批资源上限。', {
        resource,
        requote: true,
      });
    }
  }
}

export function addMemberGenerationResources(
  ...amounts: MemberGenerationResources[]
): MemberGenerationResources {
  return Object.fromEntries(
    resourceNames.map((resource) => [
      resource,
      formatAmount(amounts.reduce((sum, amount) => sum + parseAmount(amount[resource]), 0n)),
    ]),
  ) as MemberGenerationResources;
}

export function memberGenerationChargeResources(charge: {
  resource: string;
  amount: string | number;
  fixed?: string | number;
  purchased?: string | number;
}): MemberGenerationResources {
  const result = zeroResources();
  if (charge.resource === 'v5') {
    result.v5 = formatAmount(parseAmount(charge.amount, 'ceil'));
  }
  if (charge.resource === 'anlas') {
    result.anlasFixed = formatAmount(parseAmount(charge.fixed ?? charge.amount, 'ceil'));
    result.anlasPurchased = formatAmount(parseAmount(charge.purchased ?? 0, 'ceil'));
  }
  return result;
}

function admissionReason(error: unknown, fallbackCode: string): MemberGenerationReason {
  const source = error as Error & { statusCode?: number; code?: string; cooldownUntil?: string };
  const statusCode = source.statusCode || 503;
  let code = source.code;
  if (!code) {
    code = fallbackCode;
    if (statusCode === 503) {
      code = 'upstream_unavailable';
    } else if (statusCode === 403 && fallbackCode !== 'member_stopped') {
      code = 'membership_required';
    }
  }
  return {
    statusCode,
    code,
    message: publicErrorMessage(source.message),
    ...(source.cooldownUntil ? { retryAt: source.cooldownUntil } : {}),
  };
}

export function quoteMemberGeneration(
  db: FairQuotaDatabase,
  member: MemberGenerationMember,
  normalized: NormalizedMemberGeneration,
  options: MemberQuoteOptions = {},
) {
  const now = options.now ?? Date.now(),
    env = options.env ?? process.env;
  const versions = memberGenerationCapabilities(db, member, { env }).versions;
  const reasons: MemberGenerationReason[] = [],
    append = (reason: MemberGenerationReason) => {
      if (!reasons.some((item) => item.code === reason.code && item.message === reason.message)) {
        reasons.push(reason);
      }
    };
  const count = normalized.request.sampleCount,
    payloadBytes = Buffer.byteLength(JSON.stringify(normalized.request)),
    payloadLimits = generationPayloadLimits(env);
  if (payloadBytes > payloadLimits.request) {
    fail(413, 'payload_too_large', '规范化生成请求超过载荷大小限制。');
  }
  if ((options.activePayloadBytes || 0) + payloadBytes > payloadLimits.active) {
    append({
      statusCode: 429,
      code: 'payload_budget_exhausted',
      message: '待执行请求载荷已满，请等待已有任务完成。',
    });
  }
  if (options.executorAvailable === false) {
    append({ statusCode: 503, code: 'executor_unavailable', message: '执行器暂不可用。' });
  }
  if (member.enabled === false) {
    append({ statusCode: 403, code: 'membership_required', message: '成员当前不可生成。' });
  }
  try {
    assertQueueAdmission({ jobs: rows(db.jobs) }, member, undefined, env, count);
  } catch (error) {
    append(
      admissionReason(error, member.acceptingJobs === false ? 'member_stopped' : 'queue_full'),
    );
  }
  const draft = estimateFairQuota(db, now),
    account = rows(draft.accounts).find(
      (item) => item.id === draft.settings?.fairQuota?.boundAccountId,
    );
  const perJob: {
    sampleIndex: number;
    resource: 'free' | 'v5' | 'anlas';
    resources: MemberGenerationResources;
    estimated: boolean;
    breakdown?: object;
    billingMode?: string;
  }[] = [];
  const usage = options.freeUsage ? structuredClone(options.freeUsage) : undefined;
  for (let sampleIndex = 0; sampleIndex < count; sampleIndex++) {
    const request = singleGenerationRequest(normalized.request, sampleIndex);
    const candidate = previewFairQuotaCharge(draft, member, request, now);
    const quote = {
      sampleIndex,
      resource: candidate.resource,
      resources: memberGenerationChargeResources(candidate),
      estimated: candidate.estimated,
      breakdown: candidate.breakdown,
      ...(candidate.billingMode ? { billingMode: candidate.billingMode } : {}),
    };
    perJob.push(quote);
    if (candidate.resource === 'free') {
      if (usage) {
        try {
          assertFreeImageAdmission(usage);
        } catch (error) {
          append(admissionReason(error, 'free_image_limit'));
        }
        usage.reserved++;
      } else if (freeImagePolicy(draft).enabled) {
        append({
          statusCode: 503,
          code: 'upstream_unavailable',
          message: '免费图片使用记录暂不可用。',
        });
      }
    }
    if (!account || !draft.settings?.fairQuota?.enabled) {
      append({
        statusCode: 503,
        code: 'upstream_unavailable',
        message: '共享 Opus 额度暂不可用。',
      });
      continue;
    }
    try {
      reserveFairQuota(
        draft,
        member,
        { id: `quote:${sampleIndex}`, request },
        account,
        now,
        request,
      );
    } catch (error) {
      append(
        admissionReason(
          error,
          candidate.resource === 'anlas' &&
            parseAmount(candidate.purchased) > 0n &&
            !request.allowPurchasedAnlas
            ? 'purchased_anlas_not_authorized'
            : 'insufficient_balance',
        ),
      );
    }
  }
  let seedMode = 'not_applicable';
  if (normalized.operation === 'generate') {
    seedMode = normalized.randomSeed ? 'random' : 'fixed';
  }
  const effective = {
    operation: normalized.operation,
    model: normalized.request.model,
    action: normalized.request.action,
    width: normalized.request.width,
    height: normalized.request.height,
    steps: normalized.request.steps,
    sampleCount: count,
    seedMode,
    ...(!normalized.randomSeed && normalized.operation === 'generate'
      ? { seed: normalized.request.seed }
      : {}),
    allowPurchasedAnlas: normalized.request.allowPurchasedAnlas,
  };
  return {
    contractVersion: 1,
    asOf: new Date(now).toISOString(),
    versions,
    requestFingerprint: normalized.requestFingerprint,
    effective,
    charges: {
      perJob,
      total: addMemberGenerationResources(...perJob.map((quote) => quote.resources)),
    },
    admission: { allowed: reasons.length === 0, reasons },
    warnings: normalized.warnings,
  };
}

export function assertMemberGenerationConfirmation(
  confirmation: MemberGenerationConfirmation,
  quote: ReturnType<typeof quoteMemberGeneration>,
): void {
  assertMemberGenerationVersions(confirmation, quote.versions);
  assertMemberGenerationIntent(confirmation, quote.requestFingerprint);
  for (const charge of quote.charges.perJob) {
    assertMemberGenerationCost(confirmation, charge.resources, quote.charges.total);
  }
}

export function assertMemberGenerationIntent(
  confirmation: MemberGenerationConfirmation,
  requestFingerprint: string,
): void {
  if (confirmation.requestFingerprint !== requestFingerprint) {
    fail(409, 'quote_changed', '生成意图与费用确认不一致，请重新报价。', { requote: true });
  }
}

export type RecoverableRequest = Omit<Partial<ProviderRequest>, 'nativeParameters'> & {
  tag?: string;
  requestedSteps?: number;
  sampleCount?: number;
  nativeParameters?: Record<string, unknown> | null;
  allowPurchasedAnlas?: unknown;
  confirmation?: MemberGenerationConfirmation;
};
export function recoveredInput(
  request: RecoverableRequest | null | undefined,
): Record<string, unknown> | null {
  if (!request) {
    return null;
  }
  if (request.operation === 'upscale' || request.operation === 'encode-vibe') {
    if (
      !object(request.operationParameters) ||
      typeof request.operationParameters.image !== 'string'
    ) {
      return null;
    }
    const fields =
      request.operation === 'upscale'
        ? ['image', 'model', 'declared_blur_sigma']
        : ['image', 'model', 'information_extracted', 'focus_seed', 'info_extract_seed'];
    return {
      operation: request.operation,
      ...Object.fromEntries(
        fields
          .filter((key) => Object.hasOwn(request.operationParameters as object, key))
          .map((key) => [key, (request.operationParameters as Record<string, unknown>)[key]]),
      ),
      allowPurchasedAnlas: request.allowPurchasedAnlas === true,
    };
  }
  if (typeof request.model !== 'string' || typeof request.prompt !== 'string') {
    return null;
  }
  let parameters = request.nativeParameters;
  if (!object(parameters)) {
    if (
      (request.action && request.action !== 'generate') ||
      !['width', 'height', 'steps', 'seed', 'scale', 'cfg'].every(
        (key) => typeof request[key as keyof RecoverableRequest] === 'number',
      ) ||
      !['sampler', 'noiseSchedule', 'negative'].every(
        (key) => typeof request[key as keyof RecoverableRequest] === 'string',
      )
    ) {
      return null;
    }
    parameters = buildNovelAiParameters(request as ProviderRequest);
  }
  const clean = structuredClone(parameters);
  for (const key of [
    'image_cache_secret_key',
    'mask_cache_secret_key',
    'reference_image_cache_secret_key',
  ]) {
    delete clean[key];
  }
  for (const key of ['reference_image_multiple_cached', 'director_reference_images_cached']) {
    if (Array.isArray(clean[key])) {
      clean[key] = clean[key].map((value) => (object(value) ? { data: value.data } : value));
    }
  }
  return {
    operation: 'generate',
    input: request.prompt,
    model: request.model,
    action: request.action || 'generate',
    parameters: {
      ...clean,
      ...(request.seed !== undefined ? { seed: request.seed } : {}),
      n_samples: request.sampleCount || parameters.n_samples || 1,
    },
    allowPurchasedAnlas: request.allowPurchasedAnlas === true,
  };
}
export function inputAssetSlots(input: Record<string, unknown>): {
  slot: string;
  owner: Record<string, unknown> | unknown[];
  key: string | number;
  value: unknown;
  image: boolean;
}[] {
  const slots: {
    slot: string;
    owner: Record<string, unknown> | unknown[];
    key: string | number;
    value: unknown;
    image: boolean;
  }[] = [];
  const add = (
    owner: Record<string, unknown> | unknown[],
    key: string | number,
    slot: string,
    image: boolean,
  ) => {
    const value = (owner as Record<string | number, unknown>)[key];
    if (value !== undefined && value !== null && value !== '') {
      slots.push({ slot, owner, key, value, image });
    }
  };
  for (const [prefix, owner] of [
    ['', input],
    ['parameters.', input.parameters],
  ] as const) {
    if (!object(owner)) {
      continue;
    }
    for (const key of ['image', 'mask', 'reference_image']) {
      add(
        owner,
        key,
        prefix + key,
        key !== 'reference_image' || String(input.model).endsWith('-3'),
      );
    }
    for (const key of [
      'reference_image_multiple',
      'director_reference_images',
      'reference_image_multiple_cached',
      'director_reference_images_cached',
    ]) {
      const values = owner[key];
      if (!Array.isArray(values)) {
        continue;
      }
      values.forEach((value, index) => {
        const image = key.startsWith('director_') || String(input.model).endsWith('-3');
        if (key.endsWith('_cached')) {
          if (object(value)) {
            add(value, 'data', `${prefix}${key}.${index}.data`, image);
          }
        } else {
          add(values, index, `${prefix}${key}.${index}`, image);
        }
      });
    }
  }
  return slots;
}
export function decodeInputAsset(
  asset: { value: unknown; image: boolean },
  context?: Pick<MemberNormalizationContext, 'native'>,
) {
  if (typeof asset.value !== 'string') {
    fail(410, 'input_unavailable', '原输入附件不可用。');
  }
  const base64 = asset.value.startsWith('data:')
    ? asset.value.slice(asset.value.indexOf(',') + 1)
    : asset.value;
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(base64)) {
    fail(410, 'input_unavailable', '原输入附件不可用。');
  }
  const bytes = Buffer.from(base64, 'base64');
  if (!bytes.length || bytes.toString('base64').replace(/=+$/, '') !== base64.replace(/=+$/, '')) {
    fail(410, 'input_unavailable', '原输入附件不可用。');
  }
  const limits = context?.native.limits;
  try {
    let mimeType = 'application/octet-stream';
    if (asset.image) {
      const imageLimits = limits
        ? {
            maxBytes: limits.imageBytes,
            maxDimension: limits.imageDimension,
            maxPixels: limits.imagePixels,
          }
        : {};
      mimeType = validateImageBytes(bytes, imageLimits).mimeType;
    }
    return { bytes, mimeType };
  } catch {
    fail(410, 'input_unavailable', '原输入附件不可用。');
  }
}

export function memberInputAssetUrl(requestId: string, slot: string): string {
  return `/api/member/requests/${encodeURIComponent(requestId)}/inputs/${encodeURIComponent(slot)}`;
}

export function memberGenerationInput(
  requestId: string,
  request: RecoverableRequest | null | undefined,
) {
  const input = recoveredInput(request),
    assets: { slot: string; mimeType: string; byteLength: number; url: string }[] = [];
  if (input) {
    for (const asset of inputAssetSlots(input)) {
      if (typeof asset.value !== 'string') {
        continue;
      }
      const decoded = decodeInputAsset(asset, request?.normalizationContext);
      assets.push({
        slot: asset.slot,
        mimeType: decoded.mimeType,
        byteLength: decoded.bytes.length,
        url: memberInputAssetUrl(requestId, asset.slot),
      });
      (asset.owner as Record<string | number, unknown>)[asset.key] = { slot: asset.slot };
    }
  }
  return {
    contractVersion: 1,
    requestId,
    input,
    assets,
    confirmation: request?.confirmation ? structuredClone(request.confirmation) : null,
    recoverable: input !== null,
  };
}

export function memberGenerationInputAsset(
  request: RecoverableRequest | null | undefined,
  slot: string,
) {
  const input = recoveredInput(request);
  if (!input) {
    fail(410, 'input_unavailable', '历史请求未保存完整输入。');
  }
  const asset = inputAssetSlots(input).find(
    (item) => item.slot === slot && typeof item.value === 'string',
  );
  if (!asset) {
    fail(404, 'input_not_found', '原输入附件不存在。');
  }
  return decodeInputAsset(asset, request?.normalizationContext);
}
