import type Database from 'better-sqlite3';
import type { EventEmitter } from 'node:events';
import type { ServerResponse } from 'node:http';
import {
  authenticateMemberApiKey,
  withAuthenticatedMemberApiKey,
} from '../members/authentication.ts';
import { requireDiscordMembership, type DiscordDependencies } from '../members/discord-auth.ts';
import { RelationalMemberStore } from '../storage/relational-member-store.ts';
import { RelationalInputStore } from '../storage/relational-input-store.ts';
import { RelationalGenerationStore } from '../storage/relational-generation-store.ts';
import { RelationalRequestStore } from '../storage/relational-request-store.ts';
import { assertRelationalStorageReady } from '../storage/relational-storage.ts';
import { readRelationalArtifactContent } from '../storage/relational-replica-io.ts';
import type { RelationalReplicaReadConfiguration } from '../storage/relational-replica-store.ts';
import {
  createNativeHttp,
  type NativeHttpRequest,
  type NativeHttpDependencies,
} from '../protocols/native-http.ts';
import { publicErrorMessage } from '../http/public-errors.ts';
import { readGenerationIdempotencyKey } from './admission.ts';
import { createRelationalNativeAdmission } from './relational-native-admission.ts';
import { createJobResultWaiter } from './result-waiter.ts';
import type { GenerationDeliveryResult } from './delivery.ts';
import type { JobPreviews } from './job-previews.ts';
import type { createGenerationPayloadBudget } from './payload-budget.ts';

function fail(statusCode: number, message: string): never {
  throw Object.assign(new Error(message), { statusCode });
}

export function createRelationalNativeHttp(deps: {
  sqlite: Database.Database;
  events: EventEmitter;
  previews: JobPreviews;
  replicas: RelationalReplicaReadConfiguration;
  payloadBudget: ReturnType<typeof createGenerationPayloadBudget>;
  readJson(req: NativeHttpRequest, limit: number): Promise<unknown>;
  sendImage: NativeHttpDependencies['sendImage'];
  schedule(): void;
  timeoutMs: number;
  env?: NodeJS.ProcessEnv;
  discordFetch?: DiscordDependencies['discordFetch'];
  now?(): number;
  logDiagnostic?(id: string, event: string, details: object): void;
}) {
  const { sqlite, env = process.env } = deps;
  assertRelationalStorageReady(sqlite);
  const members = new RelationalMemberStore(sqlite, env),
    inputs = new RelationalInputStore(sqlite, env),
    generations = new RelationalGenerationStore(sqlite, env),
    requests = new RelationalRequestStore(sqlite);
  const admit = createRelationalNativeAdmission({
    members,
    inputs,
    generations,
    payloadBudget: deps.payloadBudget,
    env,
    now: deps.now,
  });
  return async (req: NativeHttpRequest, res: ServerResponse, url: URL): Promise<boolean> => {
    if (
      req.method !== 'POST' ||
      !/^\/(?:v1\/)?ai\/(?:generate-image(?:-stream)?|encode-vibe|upscale)$/.test(url.pathname)
    ) {
      return false;
    }
    assertRelationalStorageReady(sqlite);
    const claim = req.generationPayloadClaim ?? deps.payloadBudget.claim();
    req.generationPayloadClaim = claim;
    try {
      req.generationIdempotencyKey = readGenerationIdempotencyKey(req);
      const token =
        String(req.headers.authorization || '')
          .match(/^Bearer\s+(.+)$/i)?.[1]
          ?.trim() || '';
      const authorize = async <T>(read: (memberId: string) => T): Promise<T> => {
        const result = await withAuthenticatedMemberApiKey(
          members,
          token,
          (member) => ({ value: read(member.id) }),
          env,
        );
        if (!result) {
          fail(401, 'invalid token.');
        }
        return result.value;
      };
      const readContent = async (jobId: string) => {
        const artifact = await authorize((memberId) => requests.readMemberResult(jobId, memberId));
        const content = await readRelationalArtifactContent(
          sqlite,
          artifact.artifactId,
          deps.replicas,
        );
        const current = await authorize((memberId) => requests.readMemberResult(jobId, memberId));
        if (current.artifactId !== artifact.artifactId) {
          fail(410, '原结果已不可领取。');
        }
        return content;
      };
      const delivery = createJobResultWaiter({
        now: deps.now,
        errorMessage: (message) => publicErrorMessage(message, { generation: true }),
        readResult: async (jobId): Promise<GenerationDeliveryResult | null> => {
          try {
            const job = await authorize((memberId) =>
              requests.readMemberJobDelivery(jobId, memberId),
            );
            if (job.status === 'failed') {
              return {
                error: job.error || '生成失败',
                statusCode: job.errorStatus || 500,
                cooldownUntil: job.cooldownUntil,
              };
            }
            if (job.status !== 'done') {
              return null;
            }
            const artifact = await authorize((memberId) =>
              requests.readMemberResult(jobId, memberId),
            );
            return {
              saved: { id: artifact.artifactId, jobId, mimeType: artifact.mimeType },
              ...(job.seed === null ? {} : { seed: job.seed }),
            };
          } catch (error) {
            const source = error as Error & { statusCode?: number };
            return { error: source.message, statusCode: source.statusCode || 503 };
          }
        },
      });
      const refresh = () => delivery.refresh();
      deps.events.on('commit', refresh);
      try {
        const native = createNativeHttp({
          readMember: async (key) => {
            const member = await authenticateMemberApiKey(members, key);
            if (!member) {
              return null;
            }
            await requireDiscordMembership(member, {
              store: members,
              env,
              discordFetch: deps.discordFetch,
            });
            const current = members.readMember(member.id);
            return current?.status === 'active'
              ? { admissionVersion: current.admissionEpoch }
              : null;
          },
          admitNative: async (options) => {
            const batch = await admit(options);
            if (batch.replayed) {
              // 旧 Native 在重领回执前核对所有已承诺结果；缺失不能重发或变成新意图。
              for (const jobId of batch.jobIds) {
                const job = await authorize((memberId) =>
                  requests.readMemberJobDelivery(jobId, memberId),
                );
                if (job.status === 'done' || job.artifactId) {
                  try {
                    await readContent(jobId);
                  } catch (error) {
                    if ((error as { statusCode?: number }).statusCode === 401) {
                      throw error;
                    }
                    fail(410, '原请求的任务或结果已不可领取；如需再次生成，请使用新的操作键');
                  }
                }
              }
            } else {
              try {
                deps.events.emit('commit');
              } catch {
                /* 失效通知不能改变已提交受理结果。 */
              }
            }
            return batch;
          },
          readJson: deps.readJson,
          waitForJobResult: delivery.waitForJobResult,
          readStoredImage: async (image) => {
            const content = await readContent(image.jobId!);
            if (
              content.descriptor.imageId !== image.id ||
              content.descriptor.mimeType !== image.mimeType
            ) {
              fail(410, '原结果已不可领取。');
            }
            return content.buffer;
          },
          sendImage: deps.sendImage,
          previews: deps.previews,
          schedule: deps.schedule,
          timeoutMs: deps.timeoutMs,
          now: deps.now,
          logDiagnostic: deps.logDiagnostic,
        });
        return await native.handle(req, res, url);
      } finally {
        deps.events.off('commit', refresh);
      }
    } finally {
      claim.release();
      delete req.generationPayloadClaim;
    }
  };
}
