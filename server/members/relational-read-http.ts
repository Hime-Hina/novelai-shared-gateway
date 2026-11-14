import type Database from 'better-sqlite3';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { RelationalMemberStore } from '../storage/relational-member-store.ts';
import { RelationalRequestStore } from '../storage/relational-request-store.ts';
import { RelationalGenerationStore } from '../storage/relational-generation-store.ts';
import { RelationalQuotaStore } from '../storage/relational-quota-store.ts';
import { assertRelationalStorageReady } from '../storage/relational-storage.ts';
import {
  authenticateMemberSession,
  withAuthenticatedMemberSession,
  isOwner,
  type MemberUser,
} from './authentication.ts';
import { renewMemberCookie } from './authentication-http.ts';
import { requireDiscordMembership, type DiscordDependencies } from './discord-auth.ts';
import { assertMemberSameOrigin, memberCookie, replyMemberJson } from './http-request.ts';
import { handleMemberReadRoutes, publicMemberProfile } from './read-http.ts';

function fail(statusCode: number, message: string): never {
  throw Object.assign(new Error(message), { statusCode });
}

export function createRelationalMemberReadHttp(deps: {
  sqlite: Database.Database;
  publicBaseUrl: string;
  publicImageUrl(imageId: string): string;
  env?: NodeJS.ProcessEnv;
  discordFetch?: DiscordDependencies['discordFetch'];
  now?(): number;
}) {
  const { sqlite, env = process.env } = deps;
  assertRelationalStorageReady(sqlite);
  const members = new RelationalMemberStore(sqlite, env),
    requests = new RelationalRequestStore(sqlite),
    generations = new RelationalGenerationStore(sqlite, env),
    quotas = new RelationalQuotaStore(sqlite, env),
    now = deps.now ?? Date.now;
  return async (req: IncomingMessage, res: ServerResponse, url: URL): Promise<boolean> => {
    if (!['/api/member/history', '/api/member/key', '/api/member/me'].includes(url.pathname)) {
      return false;
    }
    assertRelationalStorageReady(sqlite);
    assertMemberSameOrigin(req);
    const token = memberCookie(req),
      session = await authenticateMemberSession(members, token, env, now());
    if (!session) {
      fail(401, '请先登录');
    }
    await requireDiscordMembership(session.user, {
      store: members,
      env,
      discordFetch: deps.discordFetch,
    });
    if (req.method !== 'GET') {
      fail(403, '成员账户没有管理权限，请联系管理员');
    }
    assertRelationalStorageReady(sqlite);
    if (url.pathname === '/api/member/me' && req.headers['x-member-activity'] === '1') {
      await renewMemberCookie(req, res, members, env);
    }
    // Discord IO 不占 SQL 事务，返回后才以当前会话、稳定成员和 ready 状态读取。
    const authorize = async <T>(read: (member: MemberUser, expiresAt: number) => T): Promise<T> => {
      const result = await withAuthenticatedMemberSession(
        members,
        token,
        env,
        (member, expiresAt) => {
          assertRelationalStorageReady(sqlite);
          if (member.id !== session.user.id) {
            fail(401, '登录已失效，请重新登录');
          }
          return { value: read(member, expiresAt) };
        },
        now(),
      );
      if (!result) {
        fail(401, '登录已失效，请重新登录');
      }
      return result.value;
    };
    if (url.pathname === '/api/member/me') {
      const profile = await authorize((member, expiresAt) => {
        const current = members.readMember(member.id),
          at = now(),
          quota = quotas.readMemberQuotaSummary(member.id, at);
        if (!current) {
          fail(401, '登录已失效，请重新登录');
        }
        return publicMemberProfile(
          {
            username: member.memberAuth?.username,
            owner: isOwner(member, env),
            sessionExpiresAt: expiresAt,
            acceptingJobs: current.acceptingJobs === 1,
            quota: quota
              ? { ...quota, freeImages: generations.readMemberFreeImageUsage(member.id, at) }
              : null,
            jobs: requests.readMemberJobPage(member.id, {
              page: 1,
              pageSize: 30,
              q: '',
              status: '',
              sort: 'createdAt',
              order: 'desc',
              until: new Date(at).toISOString(),
            }).jobs,
            lateResults: requests.readMemberLateResults(member.id),
          },
          deps.publicImageUrl,
        );
      });
      replyMemberJson(res, 200, profile);
      return true;
    }
    return handleMemberReadRoutes(req, res, url, {
      now,
      publicImageUrl: deps.publicImageUrl,
      readHistory: (options) =>
        authorize((member) => requests.readMemberJobPage(member.id, options)),
      readKey: () =>
        authorize((member) => {
          const apiKey = members.readMemberApiKey(member.id);
          if (apiKey === null) {
            fail(503, '成员 Key 尚未恢复，请联系管理员');
          }
          return { apiKey, publicBaseUrl: deps.publicBaseUrl };
        }),
    });
  };
}
