import type Database from 'better-sqlite3';
import type { EventEmitter } from 'node:events';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { randomUUID } from 'node:crypto';
import { relationalStateNotificationsFor } from '../events/relational-state-notifications.ts';
import { domainDigest } from '../events/domain-events.ts';
import { RelationalMemberStore } from '../storage/relational-member-store.ts';
import { RelationalAccountStore } from '../storage/relational-account-store.ts';
import type { createAccountQuotaQuery } from '../quota/account-refresh.ts';
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
import { physicalAccountLoad } from '../generation/queue-dispatch.ts';
import type { OperatingFundingInput } from '../quota/operating-accounting.ts';
import { quotaNumber } from '../quota/eligibility.ts';
import {
  safeAccount,
  ownerAccountPatch,
  ownerAccountCommand,
  selectSharedAccount,
  ownerStatisticsRange,
  summarizeOwnerUsage,
} from './owner-operations.ts';

function fail(statusCode: number, message: string): never {
  throw Object.assign(new Error(message), { statusCode });
}
const operatingActions = [
  'operating-contribute',
  'operating-register-funding',
  'operating-cover-loss',
  'operating-reconcile-return',
  'operating-confirm-debits',
];

// 管理读取和命令共用当前授权，业务规则继续由各自 owner 拥有。
export function createRelationalManagementHttp(deps: {
  sqlite: Database.Database;
  events: EventEmitter;
  dataDir: string;
  controls: Parameters<typeof physicalAccountLoad>[0];
  assertAdmin(req: IncomingMessage, url: URL): void;
  readServiceResources?: typeof readServiceResources;
  queryAccountQuota?: ReturnType<typeof createAccountQuotaQuery>;
  // 与定时观察共用同一实例及 query，保持实际发起顺序和真实网络占用。
  quotas?: RelationalQuotaStore;
  env?: NodeJS.ProcessEnv;
  discordFetch?: DiscordDependencies['discordFetch'];
  now?(): number;
}) {
  const { sqlite, env = process.env } = deps,
    now = deps.now ?? Date.now;
  assertRelationalStorageReady(sqlite);
  const notifications = relationalStateNotificationsFor(sqlite, deps.events),
    members = new RelationalMemberStore(sqlite, env, notifications),
    quotas = deps.quotas ?? new RelationalQuotaStore(sqlite, env),
    accounts = new RelationalAccountStore(sqlite, quotas, deps.controls, env),
    requests = new RelationalRequestStore(sqlite),
    generations = new RelationalGenerationStore(sqlite, env),
    reports = new RelationalReportStore(sqlite),
    configuration = new RelationalConfigurationStore(sqlite);
  return async (req: IncomingMessage, res: ServerResponse, url: URL): Promise<boolean> => {
    const accountRoute = url.pathname.match(/^\/api\/(member\/owner|admin)\/accounts\/([^/]+)$/),
      accountPatchRoute = req.method === 'PATCH' && Boolean(accountRoute),
      memberManagementRoute = url.pathname === '/api/member/owner/member-management',
      summaryRoute = url.pathname === '/api/member/owner/summary',
      statisticsRoute = url.pathname === '/api/member/owner/statistics',
      ownerRoute =
        memberManagementRoute ||
        summaryRoute ||
        statisticsRoute ||
        (accountPatchRoute && accountRoute![1] === 'member/owner'),
      adminRoute =
        url.pathname === '/api/admin/member-management' ||
        (accountPatchRoute && accountRoute![1] === 'admin');
    if (
      (!ownerRoute && !adminRoute) ||
      (!['GET', 'POST'].includes(req.method!) && !accountPatchRoute) ||
      (req.method === 'POST' && !memberManagementRoute && !adminRoute)
    ) {
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
    let commandActorId = 'local-admin';
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
      commandActorId = memberId;
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
      if (
        req.method === 'GET' &&
        memberManagementRoute &&
        req.headers['x-member-activity'] === '1' &&
        !url.search
      ) {
        await renewMemberCookie(req, res, members, env);
      }
    } else {
      authorize = async (read) => {
        const write = () => {
          assertAccess();
          return read(null);
        };
        return req.method !== 'GET'
          ? notifications.commit(() => sqlite.transaction(write).immediate())
          : sqlite.transaction(write)();
      };
    }
    if (accountPatchRoute) {
      const body = await readMemberJson(req),
        input = ownerAccountPatch(decodeURIComponent(accountRoute![2]!), body),
        actorId = commandActorId,
        actorKind = ownerRoute ? 'member' : 'admin',
        operation = `PATCH:${url.pathname}`,
        command = managementCommand(req, actorId, operation, body, actorKind);
      const result = await ownerAccountCommand(
        accounts,
        authorize,
        {
          commandId: command.commandId ?? randomUUID(),
          fingerprint: command.fingerprint ?? domainDigest({ operation, body }),
          actorId,
          actorKind,
          recordedAt: now(),
          correlationId: null,
          causationId: null,
          policyVersion: 'account-management-v1',
          pricingVersion: 'none',
        },
        input,
        deps.queryAccountQuota,
        now,
      );
      replyMemberJson(res, 200, result.receipt);
      return true;
    }
    if (req.method === 'POST') {
      const body = await readMemberJson(req),
        groupInput = {
          groups: body.groups,
          expectedConfigVersion: body.expectedConfigVersion as number,
          redistributeRemaining: body.redistributeRemaining as boolean | undefined,
        };
      if (body.action === 'member-key') {
        const result = await authorize(() => {
          if (typeof body.userId !== 'string') {
            fail(404, '成员不存在');
          }
          const target = members.readMember(body.userId);
          if (!target || target.status === 'removed') {
            fail(404, '成员不存在');
          }
          const apiKey = members.readMemberApiKey(target.id);
          if (apiKey === null) {
            fail(503, '成员 Key 尚未恢复，请联系管理员');
          }
          return { apiKey };
        });
        // 原操作只领取当前私有值；不生成/轮换 Key，也不保存含秘密的业务回执。
        replyMemberJson(res, 200, result);
        return true;
      }
      if (body.action === 'preview-groups') {
        const preview = await authorize((member) =>
          quotas.previewGroups(
            {
              actorId: member?.id ?? 'local-admin',
              actorKind: member ? 'member' : 'admin',
              recordedAt: now(),
            },
            groupInput,
            member,
          ),
        );
        replyMemberJson(res, 200, preview);
        return true;
      }
      if (
        ![
          'fallback-policy',
          'free-image-policy',
          'loss-alert-policy',
          'member-status',
          'member-admission',
          'groups',
          ...operatingActions,
        ].includes(body.action as string)
      ) {
        fail(400, '尚未接入此管理操作');
      }
      const receipt = await authorize((member) => {
        const actorId = member?.id ?? 'local-admin',
          actorKind: 'member' | 'admin' = member ? 'member' : 'admin',
          operation = `member-management:${body.action}`,
          command = managementCommand(req, actorId, operation, body, actorKind),
          metadata: Parameters<RelationalMemberStore['setMemberStatus']>[0] = {
            commandId: command.commandId ?? randomUUID(),
            fingerprint: command.fingerprint ?? domainDigest({ operation, body }),
            actorId,
            actorKind,
            recordedAt: now(),
            correlationId: null,
            causationId: null,
            policyVersion: ['member-status', 'member-admission'].includes(body.action as string)
              ? 'member-management-v1'
              : 'quota-policy-v1',
            pricingVersion: 'none',
          };
        if (operatingActions.includes(body.action as string)) {
          if (!env.OWNER_USER_ID || (body.userId && body.userId !== env.OWNER_USER_ID)) {
            fail(403, '只能从运营者本人已归属份额划入');
          }
          return quotas.manageQuotaFunding(
            { ...metadata, commandId: command.commandId },
            quotas.readBoundAccountId(),
            body as OperatingFundingInput,
            member,
          ).response;
        }
        if (body.action === 'member-status') {
          if (typeof body.enabled !== 'boolean') {
            fail(400, 'enabled 必须为布尔值');
          }
          if (typeof body.userId !== 'string' || !body.userId) {
            fail(404, '成员不存在');
          }
          const saved = members.setMemberStatus(
            metadata,
            body.userId,
            body.enabled ? 'active' : 'disabled',
            member,
          ).receipt as { memberId: string };
          return { id: saved.memberId, enabled: body.enabled };
        }
        if (body.action === 'member-admission') {
          if (typeof body.acceptingJobs !== 'boolean') {
            fail(400, 'acceptingJobs 必须为布尔值');
          }
          if (typeof body.userId !== 'string' || !body.userId) {
            fail(404, '成员不存在');
          }
          const saved = members.changeMemberAdmission(
            metadata,
            { memberId: body.userId, acceptingJobs: body.acceptingJobs },
            member,
          ).receipt as { id: string; acceptingJobs: boolean; cancelledJobIds: string[] };
          return { ...saved, cancelledCount: saved.cancelledJobIds.length };
        }
        return (
          body.action === 'groups'
            ? quotas.configureGroups(metadata, groupInput, member)
            : quotas.configureQuotaPolicy(metadata, body as QuotaPolicyChange, member)
        ).receipt;
      });
      replyMemberJson(res, 200, receipt);
      return true;
    }
    if (summaryRoute) {
      const data = await authorize(() => {
        const source = quotas.readSharedStatusSource(),
          account = selectSharedAccount(source.accounts, source.boundAccountId);
        return {
          accounts: account
            ? [
                safeAccount({
                  ...account,
                  enabled: account.enabled === true,
                  inFlight: physicalAccountLoad(deps.controls, account.id),
                  quotaFixed: quotaNumber(account.quotaFixed),
                  quotaPurchased: quotaNumber(account.quotaPurchased),
                  quotaCheckedAt:
                    typeof account.quotaCheckedAt === 'string' ? account.quotaCheckedAt : undefined,
                }),
              ]
            : [],
        };
      });
      replyMemberJson(res, 200, data);
    } else if (statisticsRoute) {
      const range = ownerStatisticsRange(url.searchParams, now()),
        rows = await reports.readOwnerJobRows(range.since, range.until);
      replyMemberJson(
        res,
        200,
        await authorize(() => ({ ...range, ...summarizeOwnerUsage(rows) })),
      );
    } else if (url.searchParams.get('view') === 'heatmap') {
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
