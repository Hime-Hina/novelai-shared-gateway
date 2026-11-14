import { randomUUID } from 'node:crypto';
import type { IncomingMessage } from 'node:http';
import { domainDigest } from '../events/domain-events.ts';
import type { RelationalInputStore } from '../storage/relational-input-store.ts';
import type { RelationalGenerationStore } from '../storage/relational-generation-store.ts';
import type { RelationalMemberStore } from '../storage/relational-member-store.ts';
import {
  withAuthenticatedMemberApiKey,
  withAuthenticatedMemberSession,
} from '../members/authentication.ts';
import {
  readGenerationIdempotencyKey,
  generationIdempotencyKeyHash,
  type GenerationPayloadClaim,
} from './admission.ts';
import {
  normalizeMemberGeneration,
  materializeMemberGeneration,
  memberGenerationIntentFingerprint,
  parseMemberGenerationConfirmation,
} from './member-contract.ts';
import { prepareRelationalInput } from './relational-input.ts';
import type { createGenerationPayloadBudget } from './payload-budget.ts';

interface Dependencies {
  members: RelationalMemberStore;
  inputs: Pick<RelationalInputStore, 'readGenerationIdempotency' | 'readMemberContext'>;
  generations: Pick<
    RelationalGenerationStore,
    'acceptGeneration' | 'readAcceptedGenerationReceipt'
  >;
  payloadBudget: ReturnType<typeof createGenerationPayloadBudget>;
  env?: NodeJS.ProcessEnv;
  now?(): number;
}
export type RelationalGenerationCredential = { apiKey: unknown } | { sessionToken: unknown };
function fail(statusCode: number, code: string, message: string): never {
  throw Object.assign(new Error(message), { statusCode, code });
}

// HTTP 在这里交出原正文/键及当前凭据；首次上下文和随机 seed 只由原受理事务确定。
export function createRelationalMemberGenerationAdmission(deps: Dependencies) {
  return async function admitRelationalMemberGeneration(options: {
    credential: RelationalGenerationCredential;
    request: Pick<IncomingMessage, 'headers' | 'rawHeaders'>;
    body: unknown;
    claim?: GenerationPayloadClaim;
  }) {
    const env = deps.env ?? process.env;
    const claim = options.claim ?? deps.payloadBudget.claim();
    try {
      const authorized = (member: { id: string }) => {
        const key = readGenerationIdempotencyKey(options.request, true)!;
        const input = options.body as { operation?: unknown; confirmation?: unknown } | null;
        if (!input || !['generate', 'upscale', 'encode-vibe'].includes(String(input.operation))) {
          fail(422, 'invalid_parameters', '生成操作无效。');
        }
        const confirmation = parseMemberGenerationConfirmation(input.confirmation);
        const keyHash = generationIdempotencyKeyHash(key);
        const mapping = deps.inputs.readGenerationIdempotency(
          member.id,
          String(input.operation),
          keyHash,
        );
        if (
          mapping &&
          (mapping.resolution !== 'live' || !mapping.requestId || !mapping.contextId)
        ) {
          fail(410, 'result_unavailable', '原请求已归档或不可领取；再次生成须使用新操作键。');
        }
        const original = mapping
          ? deps.inputs.readMemberContext(mapping.requestId!, member.id)
          : null;
        if (original && original.kind !== 'member') {
          fail(409, 'idempotency_conflict', '操作键已用于另一种生成意图。');
        }
        const normalized = normalizeMemberGeneration(options.body, {
          ...(original?.kind === 'member' ? { normalizationContext: original.member } : {}),
        });
        const fingerprint = memberGenerationIntentFingerprint(normalized, confirmation);
        const metadata = {
          commandId:
            mapping?.originalCommandId ??
            `generation:${domainDigest([member.id, normalized.operation, keyHash])}`,
          fingerprint,
          actorKind: 'member' as const,
          actorId: member.id,
          recordedAt: deps.now?.() ?? Date.now(),
          correlationId: null,
          causationId: null,
          policyVersion: confirmation.versions.billingPolicy,
          pricingVersion: confirmation.versions.pricing,
        };
        if (mapping) {
          if (mapping.intentFingerprint !== fingerprint) {
            fail(409, 'idempotency_conflict', '操作键对应的生成意图或费用确认已变化。');
          }
          return deps.generations.readAcceptedGenerationReceipt(metadata, mapping);
        }
        const request = {
          ...materializeMemberGeneration(normalized),
          clientContract: 'member-generation-v1',
          confirmation,
        };
        const context = { kind: 'member' as const, member: normalized.normalizationContext };
        claim.resize(prepareRelationalInput(request, context, env).logicalBytes);
        let seedMode: 'fixed' | 'random' | 'not_applicable' = 'not_applicable';
        if (normalized.operation === 'generate') {
          seedMode = normalized.randomSeed ? 'random' : 'fixed';
        }
        const result = deps.generations.acceptGeneration(metadata, {
          inputId: randomUUID(),
          requestId: randomUUID(),
          memberId: member.id,
          source: 'member-generation',
          seedMode,
          confirmation,
          request,
          context,
          idempotency: { keyHash, noCache: true },
          jobs: Array.from({ length: request.sampleCount }, () => ({
            jobId: randomUUID(),
            status: 'pending' as const,
            queueSeq: null,
            queueExpiresAt: null,
            deadlineAt: null,
            admissionEpoch: 0,
            cacheKey: null,
          })),
        });
        if (result.replayed) {
          fail(410, 'result_unavailable', '原操作键映射不完整；不能重新受理或领取无关联回执。');
        }
        // 同步受理事务成功后由 SQL 活动输入接管；finally 仍负责失败和重放的临时正文。
        claim.release();
        return result;
      };
      const result =
        'apiKey' in options.credential
          ? await withAuthenticatedMemberApiKey(
              deps.members,
              options.credential.apiKey,
              authorized,
              env,
            )
          : await withAuthenticatedMemberSession(
              deps.members,
              options.credential.sessionToken,
              env,
              authorized,
              deps.now?.(),
            );
      if (!result) {
        fail(401, 'session_expired', '成员认证已失效。');
      }
      return result;
    } finally {
      claim.release();
    }
  };
}
