import type Database from 'better-sqlite3';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { RelationalMemberStore } from '../storage/relational-member-store.ts';
import { RelationalRequestStore } from '../storage/relational-request-store.ts';
import { assertRelationalStorageReady } from '../storage/relational-storage.ts';
import { authenticateMemberSession, withAuthenticatedMemberSession } from './authentication.ts';
import { requireDiscordMembership, type DiscordDependencies } from './discord-auth.ts';
import { assertMemberSameOrigin, memberCookie } from './http-request.ts';
import { handleMemberReadRoutes } from './read-http.ts';

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
    now = deps.now ?? Date.now;
  return async (req: IncomingMessage, res: ServerResponse, url: URL): Promise<boolean> => {
    if (!['/api/member/history', '/api/member/key'].includes(url.pathname)) {
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
    // Discord IO 不占 SQL 事务，返回后才以当前会话、稳定成员和 ready 状态读取。
    const authorize = async <T>(read: (memberId: string) => T): Promise<T> => {
      const result = await withAuthenticatedMemberSession(
        members,
        token,
        env,
        (member) => {
          assertRelationalStorageReady(sqlite);
          if (member.id !== session.user.id) {
            fail(401, '登录已失效，请重新登录');
          }
          return { value: read(member.id) };
        },
        now(),
      );
      if (!result) {
        fail(401, '登录已失效，请重新登录');
      }
      return result.value;
    };
    return handleMemberReadRoutes(req, res, url, {
      now,
      publicImageUrl: deps.publicImageUrl,
      readHistory: (options) =>
        authorize((memberId) => requests.readMemberJobPage(memberId, options)),
      readKey: () =>
        authorize((memberId) => {
          const apiKey = members.readMemberApiKey(memberId);
          if (apiKey === null) {
            fail(503, '成员 Key 尚未恢复，请联系管理员');
          }
          return { apiKey, publicBaseUrl: deps.publicBaseUrl };
        }),
    });
  };
}
