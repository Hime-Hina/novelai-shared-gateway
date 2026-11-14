import { randomUUID } from 'node:crypto';
import type Database from 'better-sqlite3';
import { domainDigest } from '../events/domain-events.ts';
import { withAuthenticatedMemberApiKey } from '../members/authentication.ts';
import type { RelationalMemberStore } from '../storage/relational-member-store.ts';
import type { RelationalGenerationStore } from '../storage/relational-generation-store.ts';
import type { RelationalInputStore } from '../storage/relational-input-store.ts';
import { generationIdempotencyKeyHash } from './admission.ts';
import { generationRequestFingerprint } from './request-fingerprint.ts';
import { assertMemberAcceptsJobs } from './queue-policy.ts';
import { prepareRelationalInput, type RelationalNormalizationContext } from './relational-input.ts';
import type { RecoverableRequest } from './member-contract.ts';
import type { GenerationPayloadClaim } from './admission.ts';
import type { createGenerationPayloadBudget } from './payload-budget.ts';
import type { RelationalReplicaReadConfiguration } from '../storage/relational-replica-store.ts';
import { assertRelationalStorageReady } from '../storage/relational-storage.ts';

function fail(statusCode: number, code: string, message: string): never {
  throw Object.assign(new Error(message), { statusCode, code });
}

export interface RelationalApiAdmissionOptions {
  token: string;
  key: string | null | undefined;
  admissionVersion: number;
  operation: 'generate' | 'upscale' | 'encode-vibe';
  source: 'novelai-native' | 'openai';
  seedMode: 'fixed' | 'random' | 'not_applicable';
  noCache: boolean;
  correlationId?: string;
  claim?: GenerationPayloadClaim;
  context(): RelationalNormalizationContext;
  normalize(context: RelationalNormalizationContext): RecoverableRequest & {
    model: string;
    prompt: string;
    operationParameters?: object;
  };
}

// API 原键/首次上下文与当前 Key 授权共用一个真实受理事务，不借成员 Cookie/confirmation。
export function createRelationalApiGenerationAdmission(deps: {
  sqlite: Database.Database;
  members: RelationalMemberStore;
  inputs: Pick<
    RelationalInputStore,
    'readGenerationIdempotency' | 'readGenerationIdempotencyContext' | 'generationInputIntent'
  >;
  generations: Pick<
    RelationalGenerationStore,
    | 'acceptGeneration'
    | 'readAcceptedGenerationReceipt'
    | 'readMemberGenerationCapabilities'
    | 'findReusableGeneration'
    | 'reuseGeneration'
  >;
  replicas: RelationalReplicaReadConfiguration;
  payloadBudget: ReturnType<typeof createGenerationPayloadBudget>;
  env?: NodeJS.ProcessEnv;
  now?(): number;
}) {
  return async (options: RelationalApiAdmissionOptions) => {
    const claim = options.claim ?? deps.payloadBudget.claim();
    type Search = {
      memberId: string;
      commandId: string;
      request: ReturnType<RelationalApiAdmissionOptions['normalize']>;
      context: RelationalNormalizationContext;
      seedMode: 'fixed' | 'random';
    };
    type Candidate = Awaited<ReturnType<RelationalGenerationStore['findReusableGeneration']>>;
    try {
      const execute = async (search?: Search, candidate?: Candidate) => {
        const authorized = await withAuthenticatedMemberApiKey(
          deps.members,
          options.token,
          (member) => {
            // 正文和缓存资格 IO 后都回到同一当前凭据事务；工厂曾 ready 不代表此刻仍可写。
            assertRelationalStorageReady(deps.sqlite);
            if (search && search.memberId !== member.id) {
              fail(401, 'invalid_token', 'invalid token.');
            }
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
                {
                  statusCode: 410,
                  code: 'result_unavailable',
                  requestId: mapping.originalRequestId,
                },
              );
            }
            const context = mapping
              ? deps.inputs.readGenerationIdempotencyContext(member.id, options.operation, keyHash!)
              : (search?.context ?? options.context());
            const request = mapping
              ? options.normalize(context)
              : (search?.request ?? options.normalize(context));
            const fingerprint = generationRequestFingerprint(request, {
              randomSeed: options.seedMode === 'random',
              noCache: options.noCache,
            });
            const identity = {
              commandId:
                mapping?.originalCommandId ??
                (keyHash
                  ? `generation:${domainDigest([member.id, options.operation, keyHash])}`
                  : (search?.commandId ?? randomUUID())),
              fingerprint,
              actorKind: 'member' as const,
              actorId: member.id,
            };
            if (mapping) {
              if (mapping.intentFingerprint !== fingerprint) {
                fail(409, 'idempotency_conflict', '相同 Idempotency-Key 对应的请求参数不同');
              }
              try {
                return {
                  kind: 'accepted' as const,
                  result: deps.generations.readAcceptedGenerationReceipt(identity, mapping),
                };
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
            if (!options.noCache && options.seedMode !== 'not_applicable' && !search) {
              return {
                kind: 'search' as const,
                search: {
                  memberId: member.id,
                  commandId: identity.commandId,
                  request,
                  context,
                  seedMode: options.seedMode,
                },
              };
            }
            const versions = deps.generations.readMemberGenerationCapabilities(member.id).versions;
            const metadata = {
              ...identity,
              recordedAt: deps.now?.() ?? Date.now(),
              correlationId: options.correlationId ?? null,
              causationId: null,
              policyVersion: versions.billingPolicy,
              pricingVersion: versions.pricing,
            };
            let accepted:
              | ReturnType<RelationalGenerationStore['acceptGeneration']>
              | ReturnType<RelationalGenerationStore['reuseGeneration']>;
            if (candidate && search) {
              accepted = deps.generations.reuseGeneration(metadata, {
                memberId: member.id,
                request,
                context,
                seedMode: search.seedMode,
                noCache: false,
                ...(keyHash ? { idempotency: { keyHash, noCache: false } } : {}),
                sourceJobId: candidate.sourceJobId,
                ...(candidate.kind === 'artifact'
                  ? {
                      cached: {
                        requestId: randomUUID(),
                        jobId: randomUUID(),
                        inputId: randomUUID(),
                        source: options.source,
                        artifactId: candidate.artifactId,
                      },
                    }
                  : {}),
              });
            } else {
              const cacheKey =
                options.source === 'openai'
                  ? deps.inputs.generationInputIntent(
                      member.id,
                      request,
                      context,
                      options.seedMode,
                      options.noCache,
                    ).cacheKey
                  : null;
              accepted = deps.generations.acceptGeneration(metadata, {
                inputId: randomUUID(),
                requestId: randomUUID(),
                memberId: member.id,
                source: options.source,
                seedMode: options.seedMode,
                confirmation: null,
                request,
                context,
                ...(keyHash ? { idempotency: { keyHash, noCache: options.noCache } } : {}),
                jobs: Array.from({ length: request.sampleCount ?? 1 }, () => ({
                  jobId: randomUUID(),
                  status: 'pending' as const,
                  queueSeq: null,
                  queueExpiresAt: null,
                  deadlineAt: null,
                  admissionEpoch: 0,
                  cacheKey,
                })),
              });
            }
            claim.release();
            return { kind: 'accepted' as const, result: accepted };
          },
          deps.env,
        );
        if (!authorized) {
          fail(401, 'invalid_token', 'invalid token.');
        }
        return authorized;
      };
      let completed = await execute();
      if (completed.kind === 'search') {
        const search = completed.search;
        // 缓存资格 IO 在 SQL 外；随后重新进入当前 Key 事务，先重查原键再提交唯一 owner。
        const candidate = await deps.generations.findReusableGeneration(
          {
            memberId: search.memberId,
            request: search.request,
            context: search.context,
            seedMode: search.seedMode,
            noCache: false,
            allowActive: options.source === 'openai',
          },
          deps.replicas,
        );
        completed = await execute(search, candidate);
      }
      if (completed.kind !== 'accepted') {
        throw new Error('API admission search did not resolve.');
      }
      const result = completed.result;
      const receipt = result.receipt as { requestId: string; jobIds: string[]; cacheHit?: boolean };
      return {
        id: receipt.jobIds[0],
        requestId: receipt.requestId,
        jobIds: receipt.jobIds,
        count: receipt.jobIds.length,
        replayed: result.replayed,
        ...(receipt.cacheHit ? { cacheHit: true } : {}),
      };
    } finally {
      claim.release();
    }
  };
}
