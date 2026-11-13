import type Database from 'better-sqlite3';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { RelationalMemberStore } from '../storage/relational-member-store.ts';
import { assertRelationalStorageReady } from '../storage/relational-storage.ts';
import { handleMemberAuthenticationRoutes } from './authentication-http.ts';
import type { DiscordDependencies } from './discord-auth.ts';

// 只绑定现认证 owner；初始化/恢复/监听继续由唯一 gateway 的生命周期拥有。
export function createRelationalMemberAuthenticationHttp(deps: {
  sqlite: Database.Database;
  env?: NodeJS.ProcessEnv;
  discordFetch?: DiscordDependencies['discordFetch'];
}) {
  assertRelationalStorageReady(deps.sqlite);
  const store = new RelationalMemberStore(deps.sqlite, deps.env);
  return (req: IncomingMessage, res: ServerResponse, url: URL): Promise<boolean> => {
    if (!url.pathname.startsWith('/api/member/')) {
      return Promise.resolve(false);
    }
    assertRelationalStorageReady(deps.sqlite);
    return handleMemberAuthenticationRoutes(req, res, url, { ...deps, store });
  };
}
