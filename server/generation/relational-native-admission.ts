import { nativeNormalizationContext } from '../protocols/native-image.ts';
import {
  createRelationalApiGenerationAdmission,
  type RelationalApiAdmissionOptions,
} from './relational-api-admission.ts';
import {
  normalizeNativeGeneration,
  nativeRandomSeedIntent,
  type NativeAdmissionOptions,
} from './native-admission.ts';

export function createRelationalNativeAdmission(
  deps: Parameters<typeof createRelationalApiGenerationAdmission>[0],
) {
  const admit = createRelationalApiGenerationAdmission(deps);
  return (options: NativeAdmissionOptions) => {
    let seedMode: RelationalApiAdmissionOptions['seedMode'] = 'not_applicable';
    if (options.operation === 'generate') {
      seedMode = nativeRandomSeedIntent(options.body, options.operation) ? 'random' : 'fixed';
    }
    return admit({
      token: options.token,
      key: options.key,
      operation: options.operation,
      source: 'novelai-native',
      admissionVersion: options.admissionVersion,
      seedMode,
      noCache: true,
      correlationId: options.diagnosticId,
      claim: options.claim,
      context: () => ({ kind: 'native', native: nativeNormalizationContext() }),
      normalize: (context) => {
        if (context.kind !== 'native') {
          throw Object.assign(new Error('操作键已用于另一种生成意图。'), {
            statusCode: 409,
            code: 'idempotency_conflict',
          });
        }
        return normalizeNativeGeneration(options.body, options.operation, context.native);
      },
    });
  };
}
