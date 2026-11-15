import {
  generationIntent,
  readGenerationIdempotencyKey,
  type createGenerationAdmission,
} from './admission.ts';
import { memberGenerationCapabilities } from './capabilities.ts';
import {
  normalizeMemberGeneration,
  materializeMemberGeneration,
  parseMemberGenerationConfirmation,
  quoteMemberGeneration,
  memberGenerationIntentFingerprint,
  assertMemberGenerationConfirmation,
  memberGenerationInput,
  memberGenerationInputAsset,
  type MemberNormalizationContext,
  type NormalizedMemberGeneration,
} from './member-contract.ts';
import { publicGenerationRequest, readOwnedGenerationRequest } from './request-queries.ts';
import type {
  JsonStore,
  StoreItem,
  StoredGenerationRequest,
  StoreDatabase,
} from '../storage/sqlite-store.ts';
import type { ImageStorage } from '../storage/r2-storage.ts';
import type { JobPreviews } from './job-previews.ts';
import type { createGenerationPayloadBudget } from './batch-requests.ts';
import { describeStoredResult } from '../storage/result-files.ts';
import { createMemberGenerationHttp } from './member-http.ts';

interface Dependencies {
  store: JsonStore;
  admit: ReturnType<typeof createGenerationAdmission>;
  budget: ReturnType<typeof createGenerationPayloadBudget>;
  imageStorage: ImageStorage;
  previews: JobPreviews;
  scheduleQueueDrain(): void;
  cancelGenerationRequest(requestId: string, memberId: string): Promise<unknown>;
  env?: NodeJS.ProcessEnv;
}
function fail(statusCode: number, code: string, message: string): never {
  throw Object.assign(new Error(message), { statusCode, code });
}

export function createLegacyMemberGenerationHttp(deps: Dependencies) {
  const { store, env = process.env } = deps;
  const readState = async (requestId: string, memberId: string) =>
    publicGenerationRequest(await readOwnedGenerationRequest(store, requestId, memberId));
  const readPayload = async (requestId: string, memberId: string) => {
    await readOwnedGenerationRequest(store, requestId, memberId);
    return store.readGenerationRequest(requestId);
  };
  const quote = (
    db: StoreDatabase,
    user: StoreItem,
    normalized: NormalizedMemberGeneration,
    now: number,
  ) =>
    quoteMemberGeneration(db, user, normalized, {
      now,
      env,
      freeUsage: store.readFreeImageUsage(db, user.id, now),
      activePayloadBytes: store.activeGenerationPayloadBytes(),
    });
  const handle = createMemberGenerationHttp({
    streamOwner: store,
    events: store.events,
    budget: deps.budget,
    previews: deps.previews,
    scheduleQueueDrain: deps.scheduleQueueDrain,
    capabilities: (memberId) =>
      store.update(
        (db) =>
          memberGenerationCapabilities(
            db,
            db.users.find((member) => member.id === memberId),
            { env },
          ),
        { persist: false },
      ),
    quote: (body, memberId, claim) => {
      const normalized = normalizeMemberGeneration(body);
      claim.resize(Buffer.byteLength(JSON.stringify(normalized.request)));
      return store.update(
        (db) =>
          quote(
            db,
            db.users.find((member) => member.id === memberId)!,
            normalized,
            Date.now(),
          ),
        { persist: false },
      );
    },
    submit: async (req, body, memberId, claim) => {
      const user = { id: memberId };
      const key = readGenerationIdempotencyKey(req, true);
      const input = body as Record<string, unknown> | null;
      if (!input || !['generate', 'upscale', 'encode-vibe'].includes(String(input.operation))) {
        fail(422, 'invalid_parameters', '生成操作无效。');
      }
      const confirmation = parseMemberGenerationConfirmation(input.confirmation);
      let normalized: NormalizedMemberGeneration;
      // 受理层在同一串行命令内选择首次上下文并校验新提交；重放沿用原授权，不改用当前报价。
      const intent = generationIntent(
        key,
        (context) => {
          normalized = normalizeMemberGeneration(input, {
            normalizationContext: context as unknown as MemberNormalizationContext,
          });
          return {
            ...materializeMemberGeneration(normalized),
            clientContract: 'member-generation-v1',
            confirmation,
          } as StoredGenerationRequest;
        },
        {
          operation: String(input.operation),
          randomSeed: false,
          noCache: true,
          claim,
          context: () =>
            normalizeMemberGeneration(input).normalizationContext as unknown as Partial<
              StoreDatabase['settings']
            >,
          fingerprint: () => memberGenerationIntentFingerprint(normalized!, confirmation),
          validate: (db, member, _request, now) => {
            const currentQuote = quote(db, member, normalized!, now);
            assertMemberGenerationConfirmation(confirmation, currentQuote);
            const rejection = currentQuote.admission.reasons[0];
            if (rejection) {
              fail(rejection.statusCode, rejection.code, rejection.message);
            }
          },
        },
      );
      const result = await deps.admit({
        identity: { memberId: user.id },
        intent,
        claim,
        batch: true,
        source: 'member-generation',
        noCache: true,
      });
      return {
        requestId: result.requestId!,
        jobIds: result.jobIds,
        replayed: Boolean(result.replayed),
      };
    },
    readState,
    cancel: deps.cancelGenerationRequest,
    readInput: async (requestId, memberId) =>
      memberGenerationInput(requestId, await readPayload(requestId, memberId)),
    readAsset: async (requestId, memberId, slot) =>
      memberGenerationInputAsset(await readPayload(requestId, memberId), slot),
    readContent: async (jobId, memberId) => {
      const context = await store.findJobContext(jobId),
        job = context?.job;
      const owned =
        job &&
        (job.requestId
          ? store
              .readGenerationRequestState(job.requestId, memberId)
              ?.jobs.some((child) => child.id === job.id)
          : (job.memberId || job.generationAttempt?.memberId || job.fairCharge?.userId) ===
            memberId);
      if (!job || !owned) {
        fail(404, 'job_not_found', '任务不存在。');
      }
      if (!job.imageId) {
        if (['pending', 'queued', 'running'].includes(job.status!)) {
          fail(409, 'result_not_ready', '结果尚未完成。');
        }
        if (job.status === 'done' || job.lateResultAvailable) {
          fail(410, 'result_unavailable', '原结果已不可领取。');
        }
        fail(409, 'generation_failed', '任务未产生可领取的结果。');
      }
      const image = await store.findImage(job.imageId);
      if (!image || image.token !== job.userToken || image.mimeType === 'image/svg+xml') {
        fail(410, 'result_unavailable', '原结果已不可领取。');
      }
      let bytes: Buffer;
      try {
        bytes = await deps.imageStorage.read(image);
        const verified = describeStoredResult(
          image.id,
          image.operation || job.request?.operation || 'generate',
          { mimeType: image.mimeType, buffer: bytes },
        );
        if (
          (image.sha256 && image.sha256 !== verified.sha256) ||
          (image.byteLength !== undefined && image.byteLength !== verified.byteLength)
        ) {
          throw new Error('stored content mismatch');
        }
      } catch (error) {
        if ((error as { statusCode?: number }).statusCode === 503) {
          fail(503, 'content_temporarily_unavailable', '结果存储暂不可用，请稍后重试。');
        }
        fail(410, 'result_unavailable', '原结果已不可领取。');
      }
      return { bytes, mimeType: image.mimeType || 'application/octet-stream' };
    },
  });
  // 已有状态/取消仍经成员路由的原 DTO；新生成端点继续共用 transport。
  return (...args: Parameters<typeof handle>) => {
    if (/^\/api\/member\/requests\/[^/]+(?:\/cancel)?$/.test(args[2].pathname)) {
      return Promise.resolve(false);
    }
    return handle(...args);
  };
}
