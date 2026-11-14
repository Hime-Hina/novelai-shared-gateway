import type Database from 'better-sqlite3';
import type { EventEmitter } from 'node:events';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { relationalStateNotificationsFor } from '../events/relational-state-notifications.ts';
import { gatewayKeyFrom } from '../members/http-request.ts';
import { authorizeRelationalMemberApiKey } from '../members/relational-api-key-access.ts';
import type { DiscordDependencies } from '../members/discord-auth.ts';
import { quotaHttpRoute } from '../protocols/quota-http.ts';
import { RelationalMemberStore } from '../storage/relational-member-store.ts';
import { RelationalQuotaStore } from '../storage/relational-quota-store.ts';
import { RelationalGenerationStore } from '../storage/relational-generation-store.ts';
import { assertRelationalStorageReady } from '../storage/relational-storage.ts';

export function createRelationalQuotaHttp(deps: {
  sqlite: Database.Database;
  events: EventEmitter;
  sendJson(res: ServerResponse, status: number, value: unknown): void;
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
    quotas = new RelationalQuotaStore(sqlite, env),
    generations = new RelationalGenerationStore(sqlite, env);
  return async (req: IncomingMessage, res: ServerResponse, url: URL): Promise<boolean> => {
    const route = quotaHttpRoute(req.method, url.pathname);
    if (!route) {
      return false;
    }
    const current = await authorizeRelationalMemberApiKey({
      sqlite,
      members,
      token: gatewayKeyFrom(req, url),
      env,
      discordFetch: deps.discordFetch,
    });
    await current((memberId) => {
      const at = now();
      if (route === 'subscription') {
        deps.sendJson(res, 200, quotas.readLauncherSubscription(memberId, at));
      } else {
        const quota = quotas.readMemberQuotaSummary(memberId, at);
        deps.sendJson(res, 200, {
          fairQuota: quota
            ? { ...quota, freeImages: generations.readMemberFreeImageUsage(memberId, at) }
            : null,
        });
      }
    });
    return true;
  };
}
