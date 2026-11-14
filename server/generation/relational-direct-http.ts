import type Database from 'better-sqlite3';
import { relationalStateNotificationsFor } from '../events/relational-state-notifications.ts';
import type { EventEmitter } from 'node:events';
import type { ServerResponse } from 'node:http';
import {
  authenticateMemberApiKey,
  withAuthenticatedMemberApiKey,
} from '../members/authentication.ts';
import { requireDiscordMembership, type DiscordDependencies } from '../members/discord-auth.ts';
import { gatewayKeyFrom } from '../members/http-request.ts';
import { RelationalMemberStore } from '../storage/relational-member-store.ts';
import { RelationalInputStore } from '../storage/relational-input-store.ts';
import { RelationalGenerationStore } from '../storage/relational-generation-store.ts';
import { RelationalRequestStore } from '../storage/relational-request-store.ts';
import { assertRelationalStorageReady } from '../storage/relational-storage.ts';
import { readRelationalArtifactContent } from '../storage/relational-replica-io.ts';
import type { RelationalReplicaReadConfiguration } from '../storage/relational-replica-store.ts';
import {
  createDirectHttp,
  directRequestErrorImage,
  type DirectHttpRequest,
} from '../protocols/direct-http.ts';
import {
  normalizeNovelAiRequest,
  DIRECT_URL_MAX_STEPS,
  buildErrorImage,
} from '../protocols/novelai-client.ts';
import { nativeNormalizationContext } from '../protocols/native-image.ts';
import { publicErrorMessage, isNovelAiCapacityError } from '../http/public-errors.ts';
import { cooldownRetryAfter } from '../quota/shared-capacity.ts';
import {
  isNoCache,
  readGenerationIdempotencyKey,
  type GenerationPayloadClaim,
} from './admission.ts';
import { requestNormalizationDefaults } from './request-fingerprint.ts';
import { createRelationalApiGenerationAdmission } from './relational-api-admission.ts';
import { createRelationalApiDelivery } from './relational-api-delivery.ts';
import { deliverDirectGeneration, generationErrorImage } from './delivery.ts';
import { generationCost, jobDurationMs } from './request-queries.ts';
import type { JobPreviews } from './job-previews.ts';
import type { createGenerationPayloadBudget } from './payload-budget.ts';
import type { RelationalGenerationLifecycle } from './relational-queue-runtime.ts';

function fail(statusCode: number, message: string): never {
  throw Object.assign(new Error(message), { statusCode });
}

export function createRelationalDirectHttp(deps: {
  sqlite: Database.Database;
  events: EventEmitter;
  previews: JobPreviews;
  replicas: RelationalReplicaReadConfiguration;
  payloadBudget: ReturnType<typeof createGenerationPayloadBudget>;
  runtime: RelationalGenerationLifecycle;
  readNormalizationDefaults(): NonNullable<Parameters<typeof requestNormalizationDefaults>[0]>;
  readJson(req: DirectHttpRequest, limit: number): Promise<unknown>;
  imageUrl(artifactId: string): string;
  validImageSignature(url: URL): boolean;
  isAdminRead(req: DirectHttpRequest, url: URL): boolean;
  sendJson(res: ServerResponse, status: number, value: unknown): void;
  sendImage(
    res: ServerResponse,
    status: number,
    mimeType: string,
    bytes: Buffer,
    headers?: Record<string, string>,
  ): void;
  schedule(): void;
  timeoutMs: number;
  env?: NodeJS.ProcessEnv;
  discordFetch?: DiscordDependencies['discordFetch'];
  now?(): number;
}) {
  const { sqlite, env = process.env } = deps,
    now = deps.now ?? Date.now;
  assertRelationalStorageReady(sqlite);
  const members = new RelationalMemberStore(
      sqlite,
      env,
      relationalStateNotificationsFor(sqlite, deps.events),
    ),
    inputs = new RelationalInputStore(sqlite, env),
    generations = new RelationalGenerationStore(sqlite, env),
    requests = new RelationalRequestStore(sqlite),
    admit = createRelationalApiGenerationAdmission({
      sqlite,
      members,
      inputs,
      generations,
      payloadBudget: deps.payloadBudget,
      runtime: deps.runtime,
      replicas: deps.replicas,
      env,
      now,
    });
  return async (req: DirectHttpRequest, res: ServerResponse, url: URL): Promise<boolean> => {
    const direct = req.method === 'GET' && url.pathname === '/generate',
      post = req.method === 'POST' && url.pathname === '/api/jobs',
      imageRoute = req.method === 'GET' && url.pathname.startsWith('/api/images/'),
      read = req.method === 'GET' && (url.pathname.startsWith('/api/jobs/') || imageRoute);
    if (!direct && !post && !read) {
      return false;
    }
    let claim: GenerationPayloadClaim | undefined;
    let token = direct
      ? String(url.searchParams.get('token') || '').trim()
      : gatewayKeyFrom(req, url);
    const current = async <T>(read: (memberId: string) => T) => {
      const value = await withAuthenticatedMemberApiKey(
        members,
        token,
        (member) => {
          assertRelationalStorageReady(sqlite);
          return { value: read(member.id) };
        },
        env,
      );
      if (!value) {
        fail(401, 'invalid token.');
      }
      return value.value;
    };
    const authenticate = async () => {
      if (!token.startsWith('STA1N-')) {
        fail(401, 'Existing gateway key required.');
      }
      const member = await authenticateMemberApiKey(members, token);
      if (!member) {
        fail(401, 'invalid token.');
      }
      await requireDiscordMembership(member, {
        store: members,
        env,
        discordFetch: deps.discordFetch,
      });
      return current((memberId) => members.readMember(memberId)!.admissionEpoch);
    };
    // 签名和已认证的admin只用于读取；writer始终由实际成员Key拥有。
    const access = async <T>(select: (memberId: string | null) => T): Promise<T> => {
      if ((imageRoute && deps.validImageSignature(url)) || (read && deps.isAdminRead(req, url))) {
        assertRelationalStorageReady(sqlite);
        return select(null);
      }
      return current(select);
    };
    const ownedJob = (jobId: string, memberId: string | null) => {
      const job = requests.readApiJob(jobId);
      if (memberId !== null && job.memberId !== memberId) {
        fail(403, 'forbidden.');
      }
      if (job.retiredAt !== null || job.requestRetiredAt !== null) {
        fail(410, '原结果已不可领取。');
      }
      return job;
    };
    const artifact = (artifactId: string, memberId: string | null) => {
      const value = requests.readApiArtifact(artifactId);
      if (memberId !== null && value.memberId !== memberId) {
        fail(403, 'forbidden.');
      }
      if (value.retiredAt !== null) {
        fail(410, '原结果已不可领取。');
      }
      return value;
    };
    const readArtifact = async (artifactId: string) => {
      const before = await access((memberId) => artifact(artifactId, memberId));
      const content = await readRelationalArtifactContent(sqlite, artifactId, deps.replicas);
      const after = await access((memberId) => artifact(artifactId, memberId));
      if (after.memberId !== before.memberId) {
        fail(410, '原结果已不可领取。');
      }
      return content;
    };
    const snapshot = (jobId: string, memberId: string | null) => {
      const job = ownedJob(jobId, memberId),
        input = inputs.readErrorLogRequest(jobId);
      return {
        id: job.id,
        source: job.source,
        requestId: job.requestId,
        status: job.status,
        prompt: input.tag,
        model: job.model,
        requestedSteps: input.requestedSteps,
        routedSteps: input.steps,
        accountId: job.accountId,
        accountRouteId: null,
        cost: job.currentAccepted ? generationCost() : null,
        accountCost: null,
        generationProgress: deps.previews.frames.get(jobId)?.progress ?? {},
        imageId: job.imageId ?? '',
        imageUrl: job.imageId ? deps.imageUrl(job.imageId) : '',
        lateResultAvailable: job.lateResultAvailable,
        error: publicErrorMessage(job.errorCode || '', { generation: true }),
        queuePosition: job.queuePosition,
        queuedCount: job.queuedCount,
        durationMs: jobDurationMs(job, now()),
        createdAt: job.createdAt,
        updatedAt: job.updatedAt,
        completedAt: job.completedAt ?? (['done', 'failed'].includes(job.status) ? null : ''),
      };
    };
    const createDelivery = () =>
      createRelationalApiDelivery({
        sqlite,
        members,
        requests,
        token,
        replicas: deps.replicas,
        events: deps.events,
        runtime: deps.runtime,
        env,
        now,
      });
    const accept = async (
      body: Record<string, unknown>,
      source: 'direct' | 'web',
      admissionVersion: number,
    ) => {
      const result = await admit({
        token,
        key: req.generationIdempotencyKey,
        admissionVersion,
        operation: 'generate',
        source,
        seedMode: body.seed === undefined || body.seed === '' ? 'random' : 'fixed',
        noCache: isNoCache(body.nocache),
        claim,
        context: () => ({
          kind: 'direct',
          native: nativeNormalizationContext(),
          defaults: requestNormalizationDefaults(deps.readNormalizationDefaults()),
        }),
        normalize(context) {
          if (context.kind !== 'direct') {
            fail(409, '操作键已用于另一种生成意图。');
          }
          const normalized = normalizeNovelAiRequest(body, context.defaults, {
            maxSteps: DIRECT_URL_MAX_STEPS,
          });
          return {
            ...normalized,
            ...(source === 'web' ? { allowPurchasedAnlas: body.allowPurchasedAnlas === true } : {}),
            normalizationContext: { native: context.native },
          };
        },
      });
      if (result.replayed) {
        const delivery = createDelivery();
        try {
          await delivery.validateReplay(result.jobIds);
        } finally {
          delivery.close();
        }
      }
      deps.schedule();
      return result;
    };
    try {
      assertRelationalStorageReady(sqlite);
      claim =
        direct || post ? (req.generationPayloadClaim ?? deps.payloadBudget.claim()) : undefined;
      req.generationPayloadClaim = claim;
      if (claim) {
        req.generationIdempotencyKey = readGenerationIdempotencyKey(req);
        if (direct) {
          claim.resize(Buffer.byteLength(url.search));
        }
      }
      // body.token优先，实际正文必须在确定POST身份之前限量读完；不借header身份替代。
      let version: number | undefined;
      if (
        !post &&
        !(read && (deps.isAdminRead(req, url) || (imageRoute && deps.validImageSignature(url))))
      ) {
        version = await authenticate();
      }
      const http = createDirectHttp<DirectHttpRequest>({
        async readJson(request) {
          const body = await deps.readJson(request, 2 * 1024 * 1024);
          if (!body || typeof body !== 'object') {
            fail(400, 'invalid JSON body.');
          }
          const input = body as Record<string, unknown>;
          token = String(input.token || gatewayKeyFrom(req, url) || '');
          version = await authenticate();
          return input;
        },
        acceptJob: async (_request, _url, body) => {
          const accepted = await accept(body, 'web', version!);
          return current((memberId) => snapshot(accepted.id, memberId));
        },
        generate: async () => {
          const accepted = await accept(
              Object.fromEntries(url.searchParams.entries()),
              'direct',
              version!,
            ),
            delivery = createDelivery();
          try {
            const sendError = (kind: 'busy' | 'timeout', until?: string) => {
              const image = generationErrorImage(kind, until, now());
              deps.sendImage(res, image.statusCode, image.mimeType, image.buffer, image.headers);
            };
            await deliverDirectGeneration(res, accepted, {
              waitForJobResult: delivery.waitForJobResult,
              timeoutMs: deps.timeoutMs,
              now,
              sendStoredImage: async (_response, status, image, headers) => {
                const content = await delivery.readSavedImage(image!);
                deps.sendImage(res, status, content.descriptor.mimeType, content.buffer, headers);
              },
              sendBusyImage: (_response, until) => sendError('busy', until),
              sendTimeoutImage: (_response, until) => sendError('timeout', until),
              isCapacityError: isNovelAiCapacityError,
            });
          } finally {
            delivery.close();
          }
        },
        subscribe: async () => {
          const memberId = await current((id) => id);
          const check = deps.previews.subscribe(memberId, res, async (write) => {
            await current((id) => {
              if (id !== memberId) {
                fail(401, 'invalid token.');
              }
              write();
            });
          });
          if (res.destroyed || res.writableEnded) {
            return;
          }
          deps.events.on('commit', check);
          const close = () => res.end();
          deps.events.on('close', close);
          res.once('close', () => {
            deps.events.off('commit', check);
            deps.events.off('close', close);
          });
        },
        readJob: async (_request, _url, jobId) => access((memberId) => snapshot(jobId, memberId)),
        readJobContent: async (_request, _response, _url, jobId) => {
          const job = await access((memberId) => ownedJob(jobId, memberId));
          if (job.status === 'done' && job.imageId) {
            const content = await readArtifact(job.imageId);
            await access((memberId) => ownedJob(jobId, memberId));
            deps.sendImage(res, 200, content.descriptor.mimeType, content.buffer);
            return;
          }
          if (job.status !== 'failed') {
            fail(409, 'job is not finished.');
          }
          const image = buildErrorImage(
            publicErrorMessage(job.errorCode || 'Generation failed', { generation: true }),
          );
          deps.sendImage(res, 200, image.mimeType, image.buffer, { 'x-error': '1' });
        },
        readImageContent: async (_request, _response, _url, artifactId) => {
          const content = await readArtifact(artifactId);
          deps.sendImage(res, 200, content.descriptor.mimeType, content.buffer);
        },
        sendJson: deps.sendJson,
      });
      return await http(req, res, url);
    } catch (error) {
      if (!direct || res.headersSent || res.destroyed) {
        throw error;
      }
      const image = directRequestErrorImage(error, env.PUBLIC_MODE === 'true');
      if (!image) {
        throw error;
      }
      const source = error as { requestId?: string; cooldownUntil?: number | string },
        retry = cooldownRetryAfter(source.cooldownUntil, now());
      if (source.requestId) {
        res.setHeader('X-Nai-Request-Id', source.requestId);
      }
      if (retry) {
        res.setHeader('Retry-After', String(retry));
      }
      deps.sendImage(res, image.statusCode, image.mimeType, image.buffer, image.headers);
      return true;
    } finally {
      claim?.release();
      delete req.generationPayloadClaim;
    }
  };
}
