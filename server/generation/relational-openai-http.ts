import { relationalStateNotificationsFor } from '../events/relational-state-notifications.ts';
import type Database from 'better-sqlite3';
import type { EventEmitter } from 'node:events';
import type { ServerResponse } from 'node:http';
import {
  authenticateMemberApiKey,
  withAuthenticatedMemberApiKey,
} from '../members/authentication.ts';
import { gatewayKeyFrom } from '../members/http-request.ts';
import { requireDiscordMembership, type DiscordDependencies } from '../members/discord-auth.ts';
import { RelationalMemberStore } from '../storage/relational-member-store.ts';
import { RelationalInputStore } from '../storage/relational-input-store.ts';
import { RelationalGenerationStore } from '../storage/relational-generation-store.ts';
import { RelationalRequestStore } from '../storage/relational-request-store.ts';
import { assertRelationalStorageReady } from '../storage/relational-storage.ts';
import type { RelationalReplicaReadConfiguration } from '../storage/relational-replica-store.ts';
import { createOpenAiHttp, type OpenAiHttpRequest } from '../protocols/openai-http.ts';
import {
  parseOpenAiImageRequest,
  type OpenAiNormalizationDefaults,
} from '../protocols/openai-chat.ts';
import { normalizeNovelAiRequest, DIRECT_URL_MAX_STEPS } from '../protocols/novelai-client.ts';
import { nativeNormalizationContext } from '../protocols/native-image.ts';
import { publicErrorMessage } from '../http/public-errors.ts';
import { isNoCache, readGenerationIdempotencyKey } from './admission.ts';
import { requestNormalizationDefaults } from './request-fingerprint.ts';
import { createRelationalApiGenerationAdmission } from './relational-api-admission.ts';
import { createRelationalApiDelivery } from './relational-api-delivery.ts';
import { createOpenAiJobStream } from './delivery.ts';
import type { createGenerationPayloadBudget } from './payload-budget.ts';

function fail(statusCode: number, message: string): never {
  throw Object.assign(new Error(message), { statusCode });
}

export function createRelationalOpenAiHttp(deps: {
  sqlite: Database.Database;
  events: EventEmitter;
  replicas: RelationalReplicaReadConfiguration;
  payloadBudget: ReturnType<typeof createGenerationPayloadBudget>;
  readNormalizationDefaults(): OpenAiNormalizationDefaults;
  readJson(req: OpenAiHttpRequest, limit: number): Promise<unknown>;
  imageUrl(req: OpenAiHttpRequest, artifactId: string): string;
  sendJson(res: ServerResponse, status: number, value: unknown): void;
  sendStreamHeaders(res: ServerResponse): void;
  schedule(): void;
  timeoutMs: number;
  pollMs?: number;
  env?: NodeJS.ProcessEnv;
  discordFetch?: DiscordDependencies['discordFetch'];
  now?(): number;
}) {
  const { sqlite, env = process.env } = deps;
  assertRelationalStorageReady(sqlite);
  const members = new RelationalMemberStore(
      sqlite,
      env,
      relationalStateNotificationsFor(sqlite, deps.events),
    ),
    inputs = new RelationalInputStore(sqlite, env),
    generations = new RelationalGenerationStore(sqlite, env),
    requests = new RelationalRequestStore(sqlite);
  const admit = createRelationalApiGenerationAdmission({
    sqlite,
    members,
    inputs,
    generations,
    payloadBudget: deps.payloadBudget,
    replicas: deps.replicas,
    env,
    now: deps.now,
  });
  return async (req: OpenAiHttpRequest, res: ServerResponse, url: URL): Promise<boolean> => {
    const generation = req.method === 'POST' && url.pathname === '/v1/chat/completions';
    if (!generation && !(req.method === 'GET' && url.pathname === '/v1/models')) {
      return false;
    }
    assertRelationalStorageReady(sqlite);
    const token = generation
      ? String(req.headers.authorization || '')
          .match(/^Bearer\s+(.+)$/i)?.[1]
          ?.trim() || ''
      : gatewayKeyFrom(req, url);
    if (!token) {
      fail(401, 'missing API key.');
    }
    if (!token.startsWith('STA1N-')) {
      fail(401, 'Existing gateway key required.');
    }
    const claim = generation
      ? (req.generationPayloadClaim ?? deps.payloadBudget.claim())
      : undefined;
    req.generationPayloadClaim = claim;
    try {
      if (generation) {
        req.generationIdempotencyKey = readGenerationIdempotencyKey(req);
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
      const current = await withAuthenticatedMemberApiKey(
        members,
        token,
        (authorized) => {
          // Discord IO 后目录读取与生成都重核原 Key 和当前 ready，不能只检查成员仍 active。
          assertRelationalStorageReady(sqlite);
          return members.readMember(authorized.id);
        },
        env,
      );
      if (!current) {
        fail(401, 'invalid token.');
      }
      const admissionVersion = current.admissionEpoch;
      const delivery = createRelationalApiDelivery({
        sqlite,
        members,
        requests,
        token,
        env,
        replicas: deps.replicas,
        events: deps.events,
        now: deps.now,
      });
      try {
        const http = createOpenAiHttp<OpenAiHttpRequest>({
          accept: async (request) => {
            const body = (await deps.readJson(request, 2 * 1024 * 1024)) as Record<string, unknown>;
            let model!: string;
            const accepted = await admit({
              token,
              key: request.generationIdempotencyKey,
              admissionVersion,
              operation: 'generate',
              source: 'openai',
              seedMode: 'random',
              noCache: isNoCache(
                (body.nai as Record<string, unknown> | undefined)?.nocache ?? body.nocache ?? '1',
              ),
              claim,
              context: () => ({
                kind: 'direct',
                native: nativeNormalizationContext(),
                defaults: requestNormalizationDefaults(deps.readNormalizationDefaults()),
              }),
              normalize: (context) => {
                if (context.kind !== 'direct') {
                  fail(409, '操作键已用于另一种生成意图。');
                }
                const parsed = parseOpenAiImageRequest(body, context.defaults);
                model = parsed.model;
                return {
                  ...normalizeNovelAiRequest(parsed.request, context.defaults, {
                    maxSteps: DIRECT_URL_MAX_STEPS,
                  }),
                  allowPurchasedAnlas: parsed.request.allowPurchasedAnlas === true,
                  normalizationContext: { native: context.native },
                };
              },
            });
            if (accepted.replayed) {
              await delivery.validateReplay(accepted.jobIds);
            }
            deps.schedule();
            return { ...accepted, model, stream: body.stream === true };
          },
          waitForJobResult: delivery.waitForJobResult,
          imageMarkdown: async (request, image) => {
            await delivery.readSavedImage(image);
            return `![image](${deps.imageUrl(request, image.id!)})`;
          },
          stream: async (request, response, job, deadline) => {
            const stream = createOpenAiJobStream({
              now: deps.now,
              pollMs: deps.pollMs,
              waitForJobResult: delivery.waitForJobResult,
              sendStreamHeaders: deps.sendStreamHeaders,
              imageMarkdown: (snapshot) => `![image](${deps.imageUrl(request, snapshot.imageId!)})`,
              snapshot: async (jobId) => {
                try {
                  const value = await delivery.readJob(jobId);
                  if (value.status === 'done') {
                    await delivery.readContent(jobId);
                  }
                  return {
                    ...value,
                    imageId: value.artifactId,
                    queuePosition: value.queuePosition ?? undefined,
                    error: value.error ? publicErrorMessage(value.error, { generation: true }) : '',
                  };
                } catch (error) {
                  return {
                    status: 'failed',
                    error: publicErrorMessage((error as Error).message, { generation: true }),
                    imageId: null,
                  };
                }
              },
            });
            await stream(response, job, job.model, deadline);
          },
          sendJson: deps.sendJson,
          timeoutMs: deps.timeoutMs,
          now: deps.now,
        });
        return await http(req, res, url);
      } finally {
        delivery.close();
      }
    } finally {
      claim?.release();
      delete req.generationPayloadClaim;
    }
  };
}
