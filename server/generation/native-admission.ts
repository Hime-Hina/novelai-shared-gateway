import {
  parseNativeImageRequest,
  parseNativeAuxiliaryRequest,
  type NativeNormalizationContext,
} from '../protocols/native-image.ts';
import { quoteFairRequest } from '../quota/fair-share.ts';
import { singleGenerationRequest } from './batch-requests.ts';
import type { GenerationPayloadClaim } from './admission.ts';

export type NativeOperation = 'generate' | 'upscale' | 'encode-vibe';
export interface NativeAdmissionOptions {
  token: string;
  body: unknown;
  operation: NativeOperation;
  key: string | null | undefined;
  diagnosticId: string;
  admissionVersion: number;
  claim?: GenerationPayloadClaim;
}

export function nativeRandomSeedIntent(body: unknown, operation: NativeOperation): boolean {
  const seed = (body as { parameters?: { seed?: unknown } } | null)?.parameters?.seed;
  return operation === 'generate' && (seed === undefined || seed === -1);
}

// Native 的原解析、购买授权与单图计价校验只维护这一处；首次上下文由受理 owner 选择。
export function normalizeNativeGeneration(
  body: unknown,
  operation: NativeOperation,
  context?: NativeNormalizationContext,
) {
  const request =
    operation === 'generate'
      ? parseNativeImageRequest(body, context)
      : parseNativeAuxiliaryRequest(body, operation, context);
  const normalized = {
    ...request,
    allowPurchasedAnlas:
      (body as { allowPurchasedAnlas?: unknown } | null)?.allowPurchasedAnlas === true,
  };
  quoteFairRequest(singleGenerationRequest(normalized));
  return normalized;
}
