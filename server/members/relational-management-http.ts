import type Database from 'better-sqlite3';
import type { EventEmitter } from 'node:events';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { randomUUID } from 'node:crypto';
import { relationalStateNotificationsFor } from '../events/relational-state-notifications.ts';
import { domainDigest } from '../events/domain-events.ts';
import { RelationalMemberStore } from '../storage/relational-member-store.ts';
import { RelationalQuotaStore, type QuotaPolicyChange } from '../storage/relational-quota-store.ts';
import { RelationalRequestStore } from '../storage/relational-request-store.ts';
import { RelationalGenerationStore } from '../storage/relational-generation-store.ts';
import { RelationalReportStore } from '../storage/relational-report-store.ts';
import { RelationalConfigurationStore } from '../storage/relational-configuration-store.ts';
import { assertRelationalStorageReady } from '../storage/relational-storage.ts';
import { privateAccessAllowed } from '../http/public-hosts.ts';
import { managementCommand } from '../http/management-command.ts';
import {
  authenticateMemberSession,
  withAuthenticatedMemberSession,
  isOwner,
  type MemberUser,
} from './authentication.ts';
import {
  requireDiscordMembership,
  discordLoginEnabled,
  type DiscordDependencies,
} from './discord-auth.ts';
import {
  assertMemberSameOrigin,
  memberCookie,
  readMemberJson,
  replyMemberJson,
} from './http-request.ts';
import { renewMemberCookie } from './authentication-http.ts';
import { resolveMemberPublicBaseUrl } from './read-http.ts';
import { publicSharedAccountStatus } from '../quota/shared-status.ts';
import { readActivityHeatmap } from '../reports/activity-heatmap.ts';
import { readServiceResources } from '../reports/service-resources.ts';

function fail(statusCode: number, message: string): never {
  throw Object.assign(new Error(message), { statusCode });
}

// 既有两个管理入口的读取和政策变更；业务规则继续由各自 owner 拥有。
export function createRelationalManagementHttp(deps: {
  sqlite: Database.Database;
  events: EventEmitter;
  dataDir: string;
  assertAdmin(req: IncomingMessage, url: URL): void;
  readServiceResources?: typeof readServiceResources;
  env?: NodeJS.ProcessEnv;
  discordFetch?: DiscordDependencies['discordFetch'];
  now?(): number;
}) {
  const { sqlite, env = process.env } = deps,
    now = deps.now ?? Date.now;
  assertRelationalStorageReady(sqlite);
  const notifications = relationalStateNotificationsFor(sqlite, deps.events),
    members = new RelationalMemberStore(sqlite, env, notifications),
    quotas = new RelationalQuotaStore(sqlite, env),
    requests = new RelationalRequestStore(sqlite),
    generations = new RelationalGenerationStore(sqlite, env),
    reports = new RelationalReportStore(sqlite),
    configuration = new RelationalConfigurationStore(sqlite);
  return async (req: IncomingMessage, res: ServerResponse, url: URL): Promise<boolean> => {
    const ownerRoute = url.pathname === '/api/member/owner/member-management',
      adminRoute = url.pathname === '/api/admin/member-management';
    if ((!ownerRoute && !adminRoute) || !['GET', 'POST'].includes(req.method!)) {
      return false;
    }
    const assertAccess = () => {
      assertRelationalStorageReady(sqlite);
      if (
        !privateAccessAllowed(req, env) ||
        (adminRoute && !/^(127\.0\.0\.1|localhost)(:\d+)?$/.test(String(req.headers.host || '')))
      ) {
        fail(404, 'not found.');
      }
      assertMemberSameOrigin(req);
      if (adminRoute) {
        deps.assertAdmin(req, url);
      }
    };
    assertAccess();
    let authorize: <T>(read: (member: MemberUser | null) => T) => Promise<T>;
    if (ownerRoute) {
      const token = memberCookie(req),
        session = await authenticateMemberSession(members, token, env, now());
      if (!session) {
        fail(401, '请先使用 Discord 登录');
      }
      if (!isOwner(session.user, env)) {
        fail(403, '仅 owner 可访问管理功能');
      }
      const memberId = session.user.id,
        epoch = session.user.memberAuth?.version;
      await requireDiscordMembership(session.user, {
        store: members,
        env,
        discordFetch: deps.discordFetch,
      });
      authorize = async (read) => {
        const result = await withAuthenticatedMemberSession(
          members,
          token,
          env,
          (member) => {
            assertAccess();
            if (
              member.id !== memberId ||
              member.memberAuth?.version !== epoch ||
              !isOwner(member, env)
            ) {
              fail(403, '管理授权已失效，请重新确认身份');
            }
            return { value: read(member) };
          },
          now(),
        );
        if (!result) {
          fail(401, '管理会话已失效，请重新登录');
        }
        return result.value;
      };
      await authorize(() => null);
      if (req.method === 'GET' && req.headers['x-member-activity'] === '1' && !url.search) {
        await renewMemberCookie(req, res, members, env);
      }
    } else {
      authorize = async (read) => {
        const write = () => {
          assertAccess();
          return read(null);
        };
        return req.method === 'POST'
          ? notifications.commit(() => sqlite.transaction(write).immediate())
          : sqlite.transaction(write)();
      };
    }
    if (req.method === 'POST') {
      const body = await readMemberJson(req);
      if (
        !['fallback-policy', 'free-image-policy', 'loss-alert-policy'].includes(
          body.action as string,
        )
      ) {
        fail(400, '尚未接入此管理操作');
      }
      const receipt = await authorize((member) => {
        const actorId = member?.id ?? 'local-admin',
          actorKind = member ? 'member' : 'admin',
          operation = `member-management:${body.action}`,
          command = managementCommand(req, actorId, operation, body, actorKind);
        return quotas.configureQuotaPolicy(
          {
            commandId: command.commandId ?? randomUUID(),
            fingerprint: command.fingerprint ?? domainDigest({ operation, body }),
            actorId,
            actorKind,
            recordedAt: now(),
            correlationId: null,
            causationId: null,
            policyVersion: 'quota-policy-v1',
            pricingVersion: 'none',
          },
          body as QuotaPolicyChange,
          member,
        ).receipt;
      });
      replyMemberJson(res, 200, receipt);
      return true;
    }
    if (url.searchParams.get('view') === 'heatmap') {
      const data = await readActivityHeatmap(
        reports,
        url.searchParams,
        { memberId: null, allowMemberFilter: true },
        now(),
      );
      replyMemberJson(res, 200, await authorize(() => data));
    } else if (url.searchParams.get('view') === 'losses') {
      replyMemberJson(
        res,
        200,
        await authorize(() => reports.readFailureAccounting(url.searchParams.get('before'), now())),
      );
    } else {
      const resources = await (deps.readServiceResources ?? readServiceResources)(deps.dataDir);
      const data = await authorize(() => {
        const at = now(),
          config = configuration.readCurrent(),
          policy = quotas.readQuotaConfiguration(),
          shared = publicSharedAccountStatus(
            {
              ...quotas.readSharedStatusSource(),
              counts: requests.readManagementSharedJobCounts(at),
            },
            at,
          );
        return {
          groups: policy.groups,
          status: quotas.readManagementQuotaStatus(at),
          shared,
          autoAnlasFallback: policy.autoAnlasFallback,
          configVersion: policy.configVersion,
          freeImagePolicy: policy.freeImagePolicy,
          publicBaseUrl: resolveMemberPublicBaseUrl(config.publicBaseUrl, env),
          discordLogin: discordLoginEnabled(env),
          ownerUserId: env.OWNER_USER_ID || null,
          users: members.readManagementMemberProfiles().map((profile) => {
            const quota = quotas.readMemberQuotaSummary(profile.id, at);
            return {
              id: profile.id,
              username: profile.username,
              displayName: profile.displayName,
              note: null,
              enabled: profile.status === 'active',
              acceptingJobs: profile.acceptingJobs,
              discordId: profile.discordId,
              inviteExpiresAt: profile.inviteExpiresAt,
              quota: quota
                ? {
                    ...quota,
                    freeImages: generations.readManagementMemberFreeImageUsage(profile.id, at),
                  }
                : null,
              recentActivity: requests.readManagementRecentActivity(profile.id),
            };
          }),
          serviceResources: resources,
        };
      });
      replyMemberJson(res, 200, data);
    }
    return true;
  };
}
