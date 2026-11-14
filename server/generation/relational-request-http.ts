import { randomUUID } from 'node:crypto';
import type { EventEmitter } from 'node:events';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type Database from 'better-sqlite3';
import { domainDigest } from '../events/domain-events.ts';
import { relationalStateNotificationsFor } from '../events/relational-state-notifications.ts';
import {
  authenticateMemberApiKey,
  withAuthenticatedMemberApiKey,
  type MemberApiKeyStore,
} from '../members/authentication.ts';
import { requireDiscordMembership, type DiscordDependencies } from '../members/discord-auth.ts';
import { publicErrorMessage } from '../http/public-errors.ts';
import { RelationalMemberStore } from '../storage/relational-member-store.ts';
import { RelationalRequestStore } from '../storage/relational-request-store.ts';
import { RelationalGenerationStore } from '../storage/relational-generation-store.ts';
import { assertRelationalStorageReady } from '../storage/relational-storage.ts';
import { publicGenerationRequest } from './request-queries.ts';

function fail(): never {
  throw Object.assign(new Error('invalid token.'), { statusCode: 401 });
}

// Cookie/Bearer 的当前授权事务共用一个取消 owner；这里只产生服务端 metadata，不接客户端回执。
export function cancelRelationalGenerationRequest(
  requests: Pick<RelationalRequestStore, 'readMemberRequest'>,
  generations: Pick<RelationalGenerationStore, 'cancelWaitingGeneration'>,
  requestId: string,
  memberId: string,
  now: number,
) {
  requests.readMemberRequest(requestId, memberId);
  const result = generations.cancelWaitingGeneration(
    {
      commandId: randomUUID(),
      fingerprint: domainDigest(['cancel', requestId]),
      actorKind: 'member',
      actorId: memberId,
      recordedAt: now,
      correlationId: null,
      causationId: null,
      policyVersion: 'member-cancel-v1',
      pricingVersion: 'none',
    },
    requestId,
  );
  return {
    request: requests.readMemberRequest(requestId, memberId),
    cancelledCount: result
      ? (result.receipt as { cancelledJobIds: string[] }).cancelledJobIds.length
      : 0,
  };
}

export function createRelationalRequestHttp(deps: {
  sqlite: Database.Database;
  events: EventEmitter;
  readJson(req: IncomingMessage, maxBytes: number): Promise<unknown>;
  sendJson(res: ServerResponse, status: number, value: unknown): void;
  imageUrl(artifactId: string): string;
  schedule(): void;
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
    requests = new RelationalRequestStore(sqlite),
    generations = new RelationalGenerationStore(sqlite, env);
  // 仅窄化现认证 owner 的事务能力；首次资格 IO 前也必须在事务内复核 ready。
  const authentication: MemberApiKeyStore = {
    memberApiKeyTransaction: (keyHash, authorized) =>
      members.memberApiKeyTransaction(keyHash, (member) => {
        assertRelationalStorageReady(sqlite);
        return authorized(member);
      }),
  };
  return async (req: IncomingMessage, res: ServerResponse, url: URL): Promise<boolean> => {
    const route = url.pathname.match(/^\/api\/requests\/([^/]+)(\/cancel)?$/),
      cancel = Boolean(route?.[2]);
    if (!route || (cancel ? req.method !== 'POST' : req.method !== 'GET')) {
      return false;
    }
    assertRelationalStorageReady(sqlite);
    const token =
        String(req.headers.authorization || '')
          .match(/^Bearer\s+(.+)$/i)?.[1]
          ?.trim() || '',
      original = await authenticateMemberApiKey(authentication, token);
    if (!original) {
      fail();
    }
    await requireDiscordMembership(original, {
      store: members,
      env,
      discordFetch: deps.discordFetch,
    });
    const current = async <T>(read: (memberId: string) => T): Promise<T> => {
      const result = await withAuthenticatedMemberApiKey(
        authentication,
        token,
        (member) => {
          if (
            member.id !== original.id ||
            member.memberAuth?.version !== original.memberAuth?.version
          ) {
            fail();
          }
          return { value: read(member.id) };
        },
        env,
      );
      if (!result) {
        fail();
      }
      return result.value;
    };
    const requestId = decodeURIComponent(route[1]);
    if (cancel) {
      await current(() => null);
      await deps.readJson(req, 32768);
    }
    const result = await current((memberId) =>
      cancel
        ? cancelRelationalGenerationRequest(requests, generations, requestId, memberId, now())
        : { request: requests.readMemberRequest(requestId, memberId) },
    );
    // URL 能力只在真实授权/业务事务结束后执行；它不参与取消或金融决定。
    deps.sendJson(res, 200, {
      ...publicGenerationRequest(result.request, {
        imageUrl: deps.imageUrl,
        errorMessage: (message) => publicErrorMessage(message, { generation: true }),
      }),
      ...('cancelledCount' in result ? { cancelledCount: result.cancelledCount } : {}),
    });
    if (cancel) {
      deps.schedule();
    }
    return true;
  };
}
