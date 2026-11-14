import type { IncomingMessage } from 'node:http';
import {
  createId,
  hashObject,
  generationContextRef,
  type JsonStore,
  type StoreDatabase,
  type StoreItem,
  type StoredGenerationRequest,
} from '../storage/sqlite-store.ts';
import {
  estimateFairQuota,
  syncFairQuota,
  quoteFairRequest,
  type FairQuotaState,
} from '../quota/fair-share.ts';
import { assertFreeImageAdmission } from '../quota/free-image-policy.ts';
import { assertMemberAcceptsJobs, assertQueueAdmission, queueDeadline } from './queue-policy.ts';
import {
  createGenerationPayloadBudget,
  singleGenerationRequest,
  webJobRequestSummary,
} from './batch-requests.ts';
import {
  generationRequestFingerprint,
  requestNormalizationDefaults,
  requestCacheKey,
} from './request-fingerprint.ts';
import {
  domainDigest,
  recordDomainEvent,
  setDomainCommandReceipt,
  setDomainCommandActor,
} from '../events/domain-events.ts';
import type { AcceptedJobFact } from '../events/domain-payloads.ts';

export type GenerationPayloadClaim = ReturnType<
  ReturnType<typeof createGenerationPayloadBudget>['claim']
>;
type Request = StoredGenerationRequest;
type Context = Partial<StoreDatabase['settings']>;
type Mapping = NonNullable<ReturnType<JsonStore['readGenerationIdempotency']>>;

export interface GenerationIntent {
  keyHash: string;
  operation: string;
  randomSeed: boolean;
  noCache: boolean;
  normalize(context: Context): Request;
  claim?: GenerationPayloadClaim;
  context?(settings: StoreDatabase['settings']): Context;
  fingerprint?(request: Request): string;
  validate?(db: StoreDatabase, member: StoreItem, request: Request, now: number): void;
}
export interface GenerationMetadata {
  id: string;
  requestId?: string | null;
  jobIds: string[];
  count: number;
  replayed?: boolean;
  cacheHit?: boolean;
  balance?: number;
}
interface Replay extends GenerationMetadata {
  replayImages?: { id: string; token?: string | null }[];
}
export interface AdmissionOptions {
  identity: { token: string } | { memberId: string };
  request?: Request | null;
  normalize?(settings: Context): Request;
  intent?: GenerationIntent | null;
  claim?: GenerationPayloadClaim;
  source: string;
  batch?: boolean;
  admissionVersion?: number;
  diagnosticId?: string;
  deadlineAt?: string;
  cacheKey?: string;
  noCache?: boolean;
  explicitSeed?: unknown;
  cached?: { imageId: string; accountId?: string };
}
interface AdmissionDependencies {
  store: JsonStore;
  payloadBudget: ReturnType<typeof createGenerationPayloadBudget>;
  artifactAvailable(image: StoreItem): Promise<boolean>;
  cleanupStaleActiveJobs(reason: string): Promise<unknown>;
  now?(): number;
  env?: NodeJS.ProcessEnv;
}
function fail(statusCode: number, message: string, code?: string): never {
  throw Object.assign(new Error(message), { statusCode, ...(code ? { code } : {}) });
}

export function readGenerationIdempotencyKey(
  req: Pick<IncomingMessage, 'headers' | 'rawHeaders'>,
  required = false,
): string | null {
  const key = req.headers['idempotency-key'];
  if (key === undefined && !required) {
    return null;
  }
  const count = (req.rawHeaders || []).filter(
    (value, index) => index % 2 === 0 && value.toLowerCase() === 'idempotency-key',
  ).length;
  if (count > 1 || typeof key !== 'string' || !/^[\x21-\x7e]{1,128}$/.test(key)) {
    fail(
      400,
      'Idempotency-Key 必须是单个非空可见 ASCII 字符串，最多 128 字节',
      'invalid_idempotency_key',
    );
  }
  return key;
}

export function generationIntent(
  key: string | null | undefined,
  normalize: GenerationIntent['normalize'],
  options: Partial<Omit<GenerationIntent, 'keyHash' | 'normalize'>> = {},
): GenerationIntent | null {
  return key
    ? {
        keyHash: generationIdempotencyKeyHash(key),
        operation: 'generate',
        randomSeed: false,
        noCache: false,
        ...options,
        normalize,
      }
    : null;
}

export function generationIdempotencyKeyHash(key: string): string {
  return hashObject(key);
}

export function isNoCache(value: unknown): boolean {
  return value === true || value === 1 || value === '1' || value === 'true';
}

function unavailable(requestId?: string | null): never {
  throw Object.assign(new Error('原请求的任务或结果已不可领取；如需再次生成，请使用新的操作键'), {
    statusCode: 410,
    code: 'result_unavailable',
    requestId: requestId || null,
  });
}

function memberFor(db: StoreDatabase, identity: AdmissionOptions['identity']): StoreItem {
  const member = db.users.find((user) =>
    'memberId' in identity ? user.id === identity.memberId : user.token === identity.token,
  );
  if (!member || member.enabled === false) {
    fail(401, 'invalid member.');
  }
  return member;
}

function sharingReadiness(state: FairQuotaState | null | undefined, memberId: string) {
  if (!state?.enabled) {
    return { status: 503, message: '共享账本尚未配置' };
  }
  if (!state.members.includes(memberId)) {
    return { status: 403, message: '该 Key 未加入共享账本' };
  }
  if (state.status !== 'active') {
    return { status: 503, message: `共享 Opus 暂不可用：${state.status}` };
  }
  return null;
}

/**
 * 所有协议共用受理边界；先按首次上下文领取原请求，再决定新额度、确认与容量。
 * 产物检查可异步等待，但由 JsonStore 在其后开启短写事务，原子保存事实、输入和回执。
 * 身份仍按当前状态核验；失败不留下半个请求或独立的额度恢复提交。
 */
export function createGenerationAdmission(deps: AdmissionDependencies) {
  const { store } = deps,
    now = deps.now || Date.now,
    env = deps.env || process.env;
  const activeCount = (db: StoreDatabase) =>
    db.jobs.filter((job) => ['queued', 'running'].includes(job.status!)).length;
  const resolve = (
    db: StoreDatabase,
    user: StoreItem,
    intent: GenerationIntent | null | undefined,
    existingOnly = false,
  ) => {
    if (!intent) {
      return null;
    }
    const existing = store.readGenerationIdempotency(user.id, intent.operation, intent.keyHash);
    if (!existing && existingOnly) {
      return null;
    }
    const context = existing
      ? JSON.parse(existing.normalization_context)
      : intent.context?.(db.settings) || requestNormalizationDefaults(db.settings);
    const serializedContext = JSON.stringify(context),
      contextBytes = Buffer.byteLength(serializedContext);
    intent.claim?.resize(contextBytes);
    const request = intent.normalize(context),
      payload = JSON.stringify(request);
    intent.claim?.resize(Buffer.byteLength(payload) + contextBytes);
    const fingerprint =
      intent.fingerprint?.(request) || generationRequestFingerprint(request, intent);
    if (!existing) {
      return {
        request,
        payload,
        contextBytes,
        mapping: {
          member_id: user.id,
          operation: intent.operation,
          key_hash: intent.keyHash,
          fingerprint,
          normalization_context: serializedContext,
          job_id: '',
          request_id: null,
          created_at: new Date(now()).toISOString(),
        } as Mapping,
      };
    }
    if (existing.fingerprint !== fingerprint) {
      fail(409, '相同 Idempotency-Key 对应的请求参数不同', 'idempotency_conflict');
    }
    const job =
      db.jobs.find((item) => item.id === existing.job_id) ||
      store.selectItemById('jobs', existing.job_id);
    if (!job) {
      unavailable(existing.request_id);
    }
    const children = existing.request_id
      ? store.readGenerationRequestState(existing.request_id, user.id)?.jobs
      : [job];
    if (
      !children ||
      (existing.request_id &&
        (job.requestId !== existing.request_id ||
          children.length !== (request.sampleCount || 1) ||
          children.some((child, index) => child.sampleIndex !== index)))
    ) {
      unavailable(existing.request_id);
    }
    const replayImages = children
      .filter(
        (child) =>
          child.status === 'done' ||
          child.lateResultAvailable ||
          child.imageId ||
          ('referencedImageId' in child && child.referencedImageId),
      )
      .map((child) => {
        if (!child.imageId) {
          unavailable(existing.request_id);
        }
        return { id: child.imageId, token: existing.request_id ? null : job.userToken };
      });
    return {
      replay: {
        id: job.id,
        requestId: existing.request_id,
        jobIds: children.map((child) => child.id),
        count: children.length,
        replayed: true,
        cacheHit: job.status === 'done' && !job.startedAt,
        balance: user.balance,
        replayImages,
      } as Replay,
    };
  };
  const validateReplay = async (result: Replay): Promise<GenerationMetadata> => {
    for (const expected of result.replayImages || []) {
      const image = await store.findImage(expected.id);
      if (!image || (expected.token && image.token !== expected.token)) {
        unavailable(result.requestId);
      }
      try {
        if (!(await deps.artifactAvailable(image))) {
          unavailable(result.requestId);
        }
      } catch {
        unavailable(result.requestId);
      }
    }
    const { replayImages: _images, ...metadata } = result;
    return metadata;
  };
  return async function admit(options: AdmissionOptions): Promise<GenerationMetadata> {
    const claim = options.claim || deps.payloadBudget.claim(),
      intent = options.intent;
    let parent:
      | {
          id: string;
          member_id: string;
          source: string;
          operation: string;
          created_at: string;
          payload_bytes: number;
          payload: string;
        }
      | undefined;
    let mapping: Mapping | undefined;
    try {
      // Replays survive later membership suspension, exhausted capacity and policy changes.
      const readiness = await store.update(
        (db) => {
          const user = memberFor(db, options.identity),
            resolved = resolve(db, user, intent, true);
          if (resolved?.replay) {
            return { replay: resolved.replay };
          }
          return sharingReadiness(estimateFairQuota(db, now()).settings.fairQuota, user.id);
        },
        { persist: false },
      );
      if (readiness) {
        if ('replay' in readiness) {
          return await validateReplay(readiness.replay);
        }
        fail(readiness.status, readiness.message);
      }
      await deps.cleanupStaleActiveJobs('generation admission');
      const result = await store.update(
        async (db) => {
          const user = memberFor(db, options.identity),
            resolved = resolve(db, user, intent);
          if (resolved?.replay) {
            return resolved.replay;
          }
          const timestamp = now(),
            createdAt = new Date(timestamp).toISOString();
          const request = resolved?.request || options.normalize?.(db.settings) || options.request;
          if (!request) {
            fail(400, '生成请求缺失');
          }
          // Recheck after queued cleanup; refill and acceptance share one durable outcome.
          const currentReadiness = sharingReadiness(syncFairQuota(db, timestamp), user.id);
          if (currentReadiness) {
            fail(currentReadiness.status, currentReadiness.message);
          }
          mapping = resolved?.mapping;
          intent?.validate?.(db, user, request, timestamp);
          const count = options.batch ? request.sampleCount || 1 : 1;
          const noCache = intent?.noCache ?? options.noCache ?? true;
          let cacheKey = '';
          if (!options.batch) {
            cacheKey = options.cacheKey || '';
            if (!cacheKey) {
              let explicitSeed = options.explicitSeed;
              if (intent) {
                explicitSeed = intent.randomSeed ? undefined : request.seed;
              }
              cacheKey = requestCacheKey(user.token!, request, explicitSeed);
            }
          }
          let cached = options.cached;
          if (!cached && !noCache && !options.batch) {
            const image = await store.findImageByCacheKey(cacheKey);
            if (image && image.token === user.token) {
              try {
                if (await deps.artifactAvailable(image)) {
                  cached = { imageId: image.id, accountId: image.accountId };
                }
              } catch {}
            }
          }
          assertMemberAcceptsJobs(user, options.admissionVersion);
          if (!cached && !noCache && !options.batch) {
            const active = db.jobs.find(
              (job) =>
                (job.memberId ? job.memberId === user.id : job.userToken === user.token) &&
                job.cacheKey === cacheKey &&
                ['queued', 'running'].includes(job.status!),
            );
            if (active) {
              if (mapping) {
                Object.assign(mapping, { job_id: active.id, request_id: active.requestId || null });
              }
              const aliasEvent =
                mapping &&
                recordDomainEvent(db, {
                  eventType: 'RequestAliasRegistered',
                  streamId: `request:${active.requestId || `legacy-${active.id}`}`,
                  memberId: user.id,
                  occurredAt: timestamp,
                  payload: {
                    jobId: active.id,
                    requestId: active.requestId || null,
                    memberId: user.id,
                    idempotency: {
                      operation: mapping.operation,
                      keyHash: mapping.key_hash,
                      fingerprint: mapping.fingerprint,
                      contextRef: generationContextRef(mapping),
                      createdAt: mapping.created_at,
                    },
                  },
                });
              const receipt = {
                id: active.id,
                requestId: active.requestId || null,
                jobIds: [active.id],
                count: 1,
              };
              if (aliasEvent) {
                setDomainCommandActor(db, {
                  actorId: user.id,
                  actorKind: 'member',
                  correlationId: active.requestId || active.id,
                });
                setDomainCommandReceipt(db, receipt);
              }
              return receipt;
            }
          }
          if (!cached) {
            assertQueueAdmission(db, user, options.admissionVersion, env, count);
            if (
              db.settings.fairQuota?.freeImagePolicy?.enabled &&
              quoteFairRequest(singleGenerationRequest(request)).resource === 'free'
            ) {
              assertFreeImageAdmission(store.readFreeImageUsage(db, user.id, timestamp));
            }
          }
          const payload = resolved?.payload || JSON.stringify(request),
            payloadBytes = Buffer.byteLength(payload);
          claim.resize(payloadBytes + (resolved?.contextBytes || 0));
          parent = {
            id: createId('req'),
            member_id: user.id,
            source: options.source,
            operation: request.operation || 'generate',
            created_at: createdAt,
            payload_bytes: payloadBytes,
            payload,
          };
          const jobIds: string[] = [],
            acceptedJobs: AcceptedJobFact[] = [];
          for (let index = 0; index < count; index++) {
            let status: AcceptedJobFact['status'] = 'pending';
            if (cached) {
              status = 'done';
            } else if (index === 0) {
              status = 'queued';
            }
            const job = {
              id: createId('job'),
              memberId: user.id,
              userToken: user.token,
              requestId: parent.id,
              ...(options.batch ? { batchId: parent.id, sampleIndex: index } : { sampleIndex: 0 }),
              ...(options.diagnosticId ? { diagnosticId: options.diagnosticId } : {}),
              source: options.source,
              status,
              ...(!cached && index === 0 ? { queueSeq: store.nextQueueSequence() } : {}),
              request: webJobRequestSummary(singleGenerationRequest(request, index)),
              payloadBytes: 0,
              cacheKey,
              queueTotal: cached ? 1 : activeCount(db) + 1,
              cost: 0,
              accountCost: 0,
              deadlineAt: options.deadlineAt || '',
              queueExpiresAt: cached ? '' : queueDeadline(timestamp, options.deadlineAt, env),
              jobAdmissionVersion: user.jobAdmissionVersion || 0,
              createdAt,
              updatedAt: createdAt,
              completedAt: cached ? createdAt : '',
              imageId: cached?.imageId || '',
              accountId: cached?.accountId || '',
              error: '',
              errorDetail: '',
            };
            db.jobs.unshift(job);
            jobIds.push(job.id);
            acceptedJobs.push({
              id: job.id,
              index,
              source: job.source,
              status: job.status,
              queueSeq: job.queueSeq,
              queueExpiresAt: job.queueExpiresAt,
              deadlineAt: job.deadlineAt,
              jobAdmissionVersion: job.jobAdmissionVersion,
              batchId: 'batchId' in job ? job.batchId : undefined,
              cacheKey,
              cost: 0,
              accountCost: 0,
              summary: job.request,
            });
          }
          if (mapping) {
            Object.assign(mapping, { job_id: jobIds[0], request_id: parent.id });
          }
          const acceptedEvent = recordDomainEvent(db, {
            eventType: 'RequestAccepted',
            streamId: `request:${parent.id}`,
            memberId: user.id,
            occurredAt: createdAt,
            payload: {
              requestId: parent.id,
              memberId: user.id,
              source: options.source,
              operation: parent.operation,
              createdAt,
              inputRef: parent.id,
              inputSha256: hashObject(request),
              payloadBytes,
              jobs: acceptedJobs,
              ...(request.clientContract
                ? { clientContract: request.clientContract, confirmation: request.confirmation }
                : {}),
              ...(mapping
                ? {
                    idempotency: {
                      memberId: user.id,
                      operation: mapping.operation,
                      keyHash: mapping.key_hash,
                      fingerprint: mapping.fingerprint,
                      jobId: mapping.job_id,
                      requestId: parent.id,
                      createdAt: mapping.created_at,
                      normalizationContextRef: generationContextRef(mapping),
                      normalizationContextHash: domainDigest(
                        JSON.parse(mapping.normalization_context),
                      ),
                    },
                  }
                : {}),
            },
          });
          if (cached) {
            recordDomainEvent(db, {
              eventType: 'CachedResultLinked',
              streamId: `request:${parent.id}`,
              memberId: user.id,
              occurredAt: createdAt,
              payload: {
                jobId: jobIds[0],
                artifactId: cached.imageId,
                accountId: cached.accountId || null,
                completedAt: createdAt,
                localConsumption: { v5: '0', anlasFixed: '0', anlasPurchased: '0' },
              },
            });
          }
          const receipt = {
            id: options.batch ? parent.id : jobIds[0],
            requestId: parent.id,
            jobIds,
            count,
            ...(cached ? { cacheHit: true } : {}),
          };
          if (acceptedEvent) {
            setDomainCommandActor(db, {
              actorId: user.id,
              actorKind: 'member',
              correlationId: parent.id,
            });
            setDomainCommandReceipt(db, receipt);
          }
          return receipt;
        },
        {
          immediate: true,
          includeSettings: true,
          generationRequest: () => parent,
          generationIdempotency: () => mapping,
          // 提交成功才将临时正文占用移交给持久活动输入统计，finally 释放剩余 claim。
          generationPayloadClaim: claim,
          dirtyRows: (result, db) => ({
            jobs: result.jobIds,
            ledger: db.ledger.map((entry) => entry.id),
          }),
        },
      );
      return await validateReplay(result);
    } finally {
      claim.release();
    }
  };
}
