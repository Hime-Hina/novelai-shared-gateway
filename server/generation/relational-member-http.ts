import { relationalStateNotificationsFor } from '../events/relational-state-notifications.ts';
import type Database from 'better-sqlite3';
import { randomUUID } from 'node:crypto';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { EventEmitter } from 'node:events';
import { domainDigest } from '../events/domain-events.ts';
import {
  authenticateMemberSession,
  withAuthenticatedMemberSession,
} from '../members/authentication.ts';
import { requireDiscordMembership, type DiscordDependencies } from '../members/discord-auth.ts';
import { assertMemberSameOrigin, memberCookie } from '../members/http-request.ts';
import { RelationalMemberStore } from '../storage/relational-member-store.ts';
import { RelationalInputStore } from '../storage/relational-input-store.ts';
import { RelationalGenerationStore } from '../storage/relational-generation-store.ts';
import { RelationalRequestStore } from '../storage/relational-request-store.ts';
import { assertRelationalStorageReady } from '../storage/relational-storage.ts';
import { readRelationalArtifactContent } from '../storage/relational-replica-io.ts';
import type { RelationalReplicaReadConfiguration } from '../storage/relational-replica-store.ts';
import { normalizeMemberGeneration } from './member-contract.ts';
import {
  createMemberGenerationHttp,
  isMemberGenerationPath,
  memberGenerationError,
} from './member-http.ts';
import { createRelationalMemberGenerationAdmission } from './relational-member-admission.ts';
import type { createGenerationPayloadBudget } from './payload-budget.ts';
import { publicGenerationRequest } from './request-queries.ts';
import type { JobPreviews } from './job-previews.ts';

interface Dependencies {
  sqlite: Database.Database;
  events: EventEmitter;
  previews: JobPreviews;
  replicas: RelationalReplicaReadConfiguration;
  payloadBudget: ReturnType<typeof createGenerationPayloadBudget>;
  scheduleQueueDrain(): void;
  env?: NodeJS.ProcessEnv;
  discordFetch?: DiscordDependencies['discordFetch'];
  now?(): number;
}
function fail(statusCode: number, code: string, message: string): never {
  throw Object.assign(new Error(message), { statusCode, code });
}

// 仅装配成员 Cookie transport；Key 与 Native/OpenAI 仍由各自入口拥有。
export function createRelationalMemberGenerationHttp(deps: Dependencies) {
  const { sqlite, env = process.env } = deps;
  assertRelationalStorageReady(sqlite);
  const members = new RelationalMemberStore(
      sqlite,
      env,
      relationalStateNotificationsFor(sqlite, deps.events),
    ),
    inputs = new RelationalInputStore(sqlite, env),
    generations = new RelationalGenerationStore(sqlite, env),
    requests = new RelationalRequestStore(sqlite),
    admit = createRelationalMemberGenerationAdmission({
      members,
      inputs,
      generations,
      payloadBudget: deps.payloadBudget,
      env,
      now: deps.now,
    });
  const now = deps.now ?? Date.now;
  return async (req: IncomingMessage, res: ServerResponse, url: URL): Promise<boolean> => {
    if (!isMemberGenerationPath(url.pathname)) {
      return false;
    }
    try {
      assertRelationalStorageReady(sqlite);
      assertMemberSameOrigin(req);
      const sessionToken = memberCookie(req);
      const session = await authenticateMemberSession(members, sessionToken, env, now());
      if (!session) {
        fail(401, 'session_expired', '请先登录');
      }
      await requireDiscordMembership(session.user, {
        store: members,
        env,
        discordFetch: deps.discordFetch,
      });
      // 正文/IO 可能等待；每次事实读取或取消均在当前凭据事务内重新检查。
      const authorize = async <T>(read: (memberId: string) => T): Promise<T> => {
        const result = await withAuthenticatedMemberSession(
          members,
          sessionToken,
          env,
          (member) => {
            if (member.id !== session.user.id) {
              fail(401, 'session_expired', '成员认证已失效。');
            }
            return { value: read(member.id) };
          },
          now(),
        );
        if (!result) {
          fail(401, 'session_expired', '成员认证已失效。');
        }
        return result.value;
      };
      const handle = createMemberGenerationHttp({
        streamOwner: sqlite,
        events: deps.events,
        budget: deps.payloadBudget,
        previews: deps.previews,
        scheduleQueueDrain: deps.scheduleQueueDrain,
        capabilities: () =>
          authorize((memberId) => generations.readMemberGenerationCapabilities(memberId)),
        quote: (body, _memberId, claim) => {
          const normalized = normalizeMemberGeneration(body);
          claim.resize(Buffer.byteLength(JSON.stringify(normalized.request)));
          return authorize((memberId) =>
            generations.quoteMemberGeneration(
              memberId,
              normalized,
              now(),
              deps.payloadBudget.activeBytes(),
            ),
          );
        },
        submit: async (request, body, _memberId, claim) => {
          const result = await admit({ credential: { sessionToken }, request, body, claim });
          const receipt = result.receipt as { requestId: string; jobIds: string[] };
          return { ...receipt, replayed: result.replayed };
        },
        readState: (requestId) =>
          authorize((memberId) =>
            publicGenerationRequest(requests.readMemberRequest(requestId, memberId)),
          ),
        cancel: async (requestId) => {
          const result = await authorize((memberId) => {
            requests.readMemberRequest(requestId, memberId);
            const result = generations.cancelWaitingGeneration(
              {
                commandId: randomUUID(),
                fingerprint: domainDigest(['cancel', requestId]),
                actorKind: 'member',
                actorId: memberId,
                recordedAt: now(),
                correlationId: null,
                causationId: null,
                policyVersion: 'member-cancel-v1',
                pricingVersion: 'none',
              },
              requestId,
            );
            return {
              ...publicGenerationRequest(requests.readMemberRequest(requestId, memberId)),
              cancelledCount: result
                ? (result.receipt as { cancelledJobIds: string[] }).cancelledJobIds.length
                : 0,
            };
          });
          deps.scheduleQueueDrain();
          return result;
        },
        readInput: (requestId) =>
          authorize((memberId) => inputs.readMemberInput(requestId, memberId)),
        readAsset: (requestId, _memberId, slot) =>
          authorize((memberId) => inputs.readMemberAsset(requestId, memberId, slot)),
        readContent: async (jobId) => {
          const artifact = await authorize((memberId) =>
            requests.readMemberResult(jobId, memberId),
          );
          let content: Awaited<ReturnType<typeof readRelationalArtifactContent>>;
          try {
            content = await readRelationalArtifactContent(
              sqlite,
              artifact.artifactId,
              deps.replicas,
            );
          } catch {
            fail(503, 'content_temporarily_unavailable', '结果存储暂不可用，请稍后重试。');
          }
          await authorize((memberId) => requests.readMemberResult(jobId, memberId));
          return { bytes: content.buffer, mimeType: content.descriptor.mimeType };
        },
      });
      return await handle(req, res, url, session.user, async () => {
        const current = await authenticateMemberSession(members, sessionToken, env, now());
        if (!current || current.user.id !== session.user.id) {
          return false;
        }
        await requireDiscordMembership(current.user, {
          store: members,
          env,
          discordFetch: deps.discordFetch,
        });
        return true;
      });
    } catch (error) {
      const payload = memberGenerationError(error);
      res.writeHead(payload.statusCode, {
        'content-type': 'application/json; charset=utf-8',
        'cache-control': 'private, no-store',
        'x-content-type-options': 'nosniff',
      });
      res.end(JSON.stringify(payload));
      return true;
    }
  };
}
