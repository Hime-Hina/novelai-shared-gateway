import { createNativeHttp, type NativeHttpDependencies } from '../protocols/native-http.ts';
import {
  nativeNormalizationContext,
  type NativeNormalizationContext,
} from '../protocols/native-image.ts';
import type { JsonStore, StoreDatabase } from '../storage/sqlite-store.ts';
import { generationIntent, type createGenerationAdmission } from './admission.ts';
import { normalizeNativeGeneration, nativeRandomSeedIntent } from './native-admission.ts';

export function createLegacyNativeHttp(
  deps: Omit<NativeHttpDependencies, 'readMember' | 'admitNative'> & {
    store: Pick<JsonStore, 'readUserByToken'>;
    admitGeneration: ReturnType<typeof createGenerationAdmission>;
  },
) {
  return createNativeHttp({
    ...deps,
    waitForJobResult: (...args) => deps.waitForJobResult(...args),
    readMember: async (token) => {
      const user = await deps.store.readUserByToken(token);
      return user && user.enabled !== false
        ? { admissionVersion: user.jobAdmissionVersion || 0 }
        : null;
    },
    admitNative: (options) => {
      const normalize = (context?: Partial<StoreDatabase['settings']>) => {
        // 旧映射只保存 defaults 时仍沿原适配，不能补造此前没有保存的 Native 定义。
        const request = normalizeNativeGeneration(
          options.body,
          options.operation,
          context?.native as NativeNormalizationContext | undefined,
        );
        return request;
      };
      const intent = generationIntent(options.key, normalize, {
        operation: options.operation,
        randomSeed: nativeRandomSeedIntent(options.body, options.operation),
        noCache: true,
        claim: options.claim,
        context: () => ({ native: nativeNormalizationContext() }),
      });
      return deps.admitGeneration({
        identity: { token: options.token },
        request: intent ? null : normalize(),
        diagnosticId: options.diagnosticId,
        admissionVersion: options.admissionVersion,
        claim: options.claim,
        intent,
        batch: true,
        source: 'novelai-native',
        noCache: true,
      });
    },
  });
}
