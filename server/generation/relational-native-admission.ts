import { randomUUID } from 'node:crypto';
import { domainDigest } from '../events/domain-events.ts';
import { withAuthenticatedMemberApiKey } from '../members/authentication.ts';
import type { RelationalMemberStore } from '../storage/relational-member-store.ts';
import type { RelationalGenerationStore } from '../storage/relational-generation-store.ts';
import type { RelationalInputStore } from '../storage/relational-input-store.ts';
import { nativeNormalizationContext } from '../protocols/native-image.ts';
import { generationIdempotencyKeyHash } from './admission.ts';
import { generationRequestFingerprint } from './request-fingerprint.ts';
import { assertMemberAcceptsJobs } from './queue-policy.ts';
import { prepareRelationalInput } from './relational-input.ts';
import {
  normalizeNativeGeneration,
  nativeRandomSeedIntent,
  type NativeAdmissionOptions,
} from './native-admission.ts';
import type { createGenerationPayloadBudget } from './payload-budget.ts';

function fail(statusCode: number, code: string, message: string): never {
  throw Object.assign(new Error(message), { statusCode, code });
}

// Native 没有成员费用确认；原键/上下文与当前 Key 授权仍在同一真实受理事务内核对。
export function createRelationalNativeAdmission(deps: {
  members: RelationalMemberStore;
  inputs: Pick<RelationalInputStore, 'readGenerationIdempotency' | 'readMemberContext'>;
  generations: Pick<
    RelationalGenerationStore,
    'acceptGeneration' | 'readAcceptedGenerationReceipt' | 'readMemberGenerationCapabilities'
  >;
  payloadBudget: ReturnType<typeof createGenerationPayloadBudget>;
  env?: NodeJS.ProcessEnv;
  now?(): number;
}) {
  return async (options: NativeAdmissionOptions) => {
    const claim = options.claim ?? deps.payloadBudget.claim();
    try {
      const result = await withAuthenticatedMemberApiKey(
        deps.members,
        options.token,
        (member) => {
          const keyHash = options.key ? generationIdempotencyKeyHash(options.key) : null;
          const mapping = keyHash
            ? deps.inputs.readGenerationIdempotency(member.id, options.operation, keyHash)
            : null;
          if (
            mapping &&
            (mapping.resolution !== 'live' || !mapping.requestId || !mapping.contextId)
          ) {
            throw Object.assign(
              new Error('原请求的任务或结果已不可领取；如需再次生成，请使用新的操作键'),
              { statusCode: 410, code: 'result_unavailable', requestId: mapping.originalRequestId },
            );
          }
          const context = mapping
            ? deps.inputs.readMemberContext(mapping.requestId!, member.id)
            : { kind: 'native' as const, native: nativeNormalizationContext() };
          if (context.kind !== 'native') {
            fail(409, 'idempotency_conflict', '操作键已用于另一种生成意图。');
          }
          const request = normalizeNativeGeneration(
            options.body,
            options.operation,
            context.native,
          );
          const randomSeed = nativeRandomSeedIntent(options.body, options.operation);
          const fingerprint = generationRequestFingerprint(request, { randomSeed, noCache: true });
          const identity = {
            commandId:
              mapping?.originalCommandId ??
              (keyHash
                ? `generation:${domainDigest([member.id, options.operation, keyHash])}`
                : randomUUID()),
            fingerprint,
            actorKind: 'member' as const,
            actorId: member.id,
          };
          if (mapping) {
            if (mapping.intentFingerprint !== fingerprint) {
              fail(409, 'idempotency_conflict', '相同 Idempotency-Key 对应的请求参数不同');
            }
            try {
              return deps.generations.readAcceptedGenerationReceipt(identity, mapping);
            } catch (error) {
              if ((error as { statusCode?: number }).statusCode === 410) {
                Object.assign(error as object, { requestId: mapping.originalRequestId });
              }
              throw error;
            }
          }
          const current = deps.members.readMember(member.id);
          if (!current || current.status !== 'active') {
            fail(401, 'invalid_token', 'invalid token.');
          }
          assertMemberAcceptsJobs(
            {
              acceptingJobs: current.acceptingJobs === 1,
              jobAdmissionVersion: current.admissionEpoch,
            },
            options.admissionVersion,
          );
          claim.resize(prepareRelationalInput(request, context, deps.env).logicalBytes);
          const versions = deps.generations.readMemberGenerationCapabilities(member.id).versions;
          let seedMode: 'fixed' | 'random' | 'not_applicable' = 'not_applicable';
          if (options.operation === 'generate') {
            seedMode = randomSeed ? 'random' : 'fixed';
          }
          const accepted = deps.generations.acceptGeneration(
            {
              ...identity,
              recordedAt: deps.now?.() ?? Date.now(),
              correlationId: null,
              causationId: null,
              policyVersion: versions.billingPolicy,
              pricingVersion: versions.pricing,
            },
            {
              inputId: randomUUID(),
              requestId: randomUUID(),
              memberId: member.id,
              source: 'novelai-native',
              seedMode,
              confirmation: null,
              request,
              context,
              ...(keyHash ? { idempotency: { keyHash, noCache: true } } : {}),
              jobs: Array.from({ length: request.sampleCount }, () => ({
                jobId: randomUUID(),
                status: 'pending' as const,
                queueSeq: null,
                queueExpiresAt: null,
                deadlineAt: null,
                admissionEpoch: 0,
                cacheKey: null,
              })),
            },
          );
          claim.release();
          return accepted;
        },
        deps.env,
      );
      if (!result) {
        fail(401, 'invalid_token', 'invalid token.');
      }
      const receipt = result.receipt as { requestId: string; jobIds: string[] };
      return {
        id: receipt.jobIds[0],
        requestId: receipt.requestId,
        jobIds: receipt.jobIds,
        count: receipt.jobIds.length,
        replayed: result.replayed,
      };
    } finally {
      claim.release();
    }
  };
}
