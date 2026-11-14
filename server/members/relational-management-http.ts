import type Database from 'better-sqlite3';
import type { EventEmitter } from 'node:events';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { randomUUID, randomBytes } from 'node:crypto';
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
  prepareMemberCredentials,
  assertMemberRegistrationCapacity,
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
import { publicRelationalJob } from '../generation/request-queries.ts';
import type { JobPreviews } from '../generation/job-previews.ts';
import { publicErrorMessage } from '../http/public-errors.ts';
import { finalizeStats } from '../reports/admin-statistics.ts';
import {
  safeAccount,
  ownerAccountPatch,
  adminAccountPatch,
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
  previews: Pick<JobPreviews, 'frames'>;
  imageUrl(artifactId: string): string;
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
      accountQuotaRoute =
        req.method === 'POST' && Boolean(accountRoute) && accountRoute![2] === 'quota',
      accountCollectionRoute =
        url.pathname === '/api/admin/accounts' && ['POST', 'PATCH'].includes(req.method!),
      memberManagementRoute = url.pathname === '/api/member/owner/member-management',
      summaryRoute = url.pathname === '/api/member/owner/summary',
      adminSummaryRoute = req.method === 'GET' && url.pathname === '/api/admin/summary',
      statisticsRoute = url.pathname === '/api/member/owner/statistics',
      fairQuotaRoute = req.method === 'POST' && url.pathname === '/api/admin/fair-quota',
      ownerRoute =
        memberManagementRoute ||
        summaryRoute ||
        statisticsRoute ||
        ((accountPatchRoute || accountQuotaRoute) && accountRoute![1] === 'member/owner'),
      adminRoute =
        url.pathname === '/api/admin/member-management' ||
        adminSummaryRoute ||
        fairQuotaRoute ||
        accountCollectionRoute ||
        ((accountPatchRoute || accountQuotaRoute) && accountRoute![1] === 'admin');
    if (
      (!ownerRoute && !adminRoute) ||
      (!['GET', 'POST'].includes(req.method!) && !accountPatchRoute && !accountCollectionRoute) ||
      (req.method === 'POST' && !memberManagementRoute && !adminRoute && !accountQuotaRoute)
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
    if (fairQuotaRoute) {
      const body = await readMemberJson(req, 2 * 1024 * 1024),
        result = await authorize(() =>
          quotas.configureMemberIds(
            {
              commandId: randomUUID(),
              fingerprint: domainDigest({ operation: 'fair-quota', requestId: randomUUID() }),
              actorId: 'local-admin',
              actorKind: 'admin',
              recordedAt: now(),
              correlationId: null,
              causationId: null,
              policyVersion: 'fair-quota-confirmation-v1',
              pricingVersion: 'none',
            },
            body.memberIds,
          ),
        );
      replyMemberJson(
        res,
        200,
        (result.receipt as { fairQuotaConfirmation: unknown }).fairQuotaConfirmation,
      );
      return true;
    }
    if (accountCollectionRoute) {
      const body = await readMemberJson(req),
        operation = `${req.method}:${url.pathname}`,
        metadata = {
          commandId: randomUUID(),
          fingerprint: domainDigest({ operation, requestId: randomUUID() }),
          actorId: 'local-admin',
          actorKind: 'admin' as const,
          recordedAt: now(),
          correlationId: null,
          causationId: null,
          policyVersion: 'account-management-v1',
          pricingVersion: 'none',
        };
      if (req.method === 'POST') {
        // 新生命周期要求真实 Opus 才能启用；不为第二账户伪造资格或改选共享池。
        if (body.enabled !== false) {
          fail(409, '新账户启用尚需当前共享绑定与真实 Opus 核验，请先显式 enabled:false');
        }
        const id = randomUUID();
        await authorize((actor) => {
          const directory = accounts.readAccountManagementDirectory(),
            parsed = adminAccountPatch(id, {
              ...body,
              name: body.name || `NovelAI ${directory.length + 1}`,
              weight: body.weight || 1,
            });
          return accounts.prepareAccountCommand(
            metadata,
            {
              kind: 'register',
              accountId: id,
              displayName: parsed.changes.displayName!,
              token: String(body.token || '').trim(),
              weight: parsed.changes.weight,
              proxyUrl: parsed.changes.proxyUrl,
              configVersion: quotas.readQuotaConfiguration().configVersion,
              bind: false,
            },
            actor,
          );
        });
        replyMemberJson(
          res,
          201,
          await authorize((actor) => accounts.readAdminAccountProfile(id, false, actor)),
        );
        return true;
      }
      // 原集合解析接受 ids/accounts 的数组或单值，忽略未命中的 ID，但不得全无命中。
      const value = body.ids || body.accounts;
      let values: unknown[] = [];
      if (Array.isArray(value)) {
        values = value;
      } else if (value !== undefined && value !== null && value !== '') {
        values = [value];
      }
      const ids = values.map((id) => String(id).trim()).filter(Boolean),
        changes = adminAccountPatch('', { weight: body.weight, enabled: body.enabled }).changes;
      if (!ids.length) {
        fail(400, 'account ids are required.');
      }
      if (changes.enabled === true) {
        const selected = await authorize(() =>
          accounts.readAccountManagementDirectory().filter((account) => ids.includes(account.id)),
        );
        if (!selected.length) {
          fail(404, 'no matching accounts found.');
        }
        if (
          selected.length !== 1 ||
          selected[0]!.id !==
            (await authorize(() => accounts.readSharedAccountManagementProfile()))?.id
        ) {
          fail(409, '批量启用未绑定账户尚未接入真实 Opus 核验');
        }
        await ownerAccountCommand(
          accounts,
          authorize,
          metadata,
          { kind: 'configure', accountId: selected[0]!.id, changes },
          deps.queryAccountQuota,
          now,
        );
        replyMemberJson(res, 200, {
          accounts: await authorize((actor) =>
            selected.map((account) => accounts.readAdminAccountProfile(account.id, false, actor)),
          ),
        });
        return true;
      }
      const saved = await authorize((actor) =>
          accounts.configureAccounts(
            metadata,
            { ids, weight: changes.weight, enabled: changes.enabled === false ? false : undefined },
            actor,
          ),
        ),
        selected = (saved.receipt as { accountIds: string[] }).accountIds;
      replyMemberJson(res, 200, {
        accounts: await authorize((actor) =>
          selected.map((id) => accounts.readAdminAccountProfile(id, false, actor)),
        ),
      });
      return true;
    }
    if (accountQuotaRoute) {
      const body = await readMemberJson(req),
        profile = await authorize(() => accounts.readSharedAccountManagementProfile());
      if (!profile) {
        fail(409, '尚未确定共享上游账户');
      }
      if (
        Object.keys(body).some((key) => key !== 'ids') ||
        (ownerRoute &&
          (!Array.isArray(body.ids) || body.ids.length !== 1 || body.ids[0] !== profile.id)) ||
        (adminRoute &&
          body.ids !== undefined &&
          (!Array.isArray(body.ids) ||
            (body.ids.length !== 0 && (body.ids.length !== 1 || body.ids[0] !== profile.id))))
      ) {
        fail(400, '仅可刷新当前共享账户');
      }
      const actorId = commandActorId,
        actorKind = ownerRoute ? 'member' : 'admin',
        operation = `POST:${url.pathname}`,
        command: ReturnType<typeof managementCommand> = adminRoute
          ? { actorId, actorKind }
          : managementCommand(req, actorId, operation, body, actorKind),
        result = await ownerAccountCommand(
          accounts,
          authorize,
          {
            commandId: command.commandId ?? randomUUID(),
            fingerprint:
              command.fingerprint ?? domainDigest({ operation, requestId: randomUUID() }),
            actorId,
            actorKind,
            recordedAt: now(),
            correlationId: null,
            causationId: null,
            policyVersion: 'account-observation-v1',
            pricingVersion: 'none',
          },
          { kind: 'observe', accountId: profile.id },
          deps.queryAccountQuota,
          now,
        );
      if (ownerRoute && command.commandId && 'ignored' in result) {
        // 原 keyed 零事实命令同样为409，不为过期查询编造可重领事实。
        fail(409, '操作已被更新或未产生可提交事实，请刷新后确认结果');
      }
      const response = adminRoute
        ? {
            ...(result.receipt as object),
            accounts: [
              await authorize((actor) => accounts.readAdminAccountProfile(profile.id, true, actor)),
            ],
          }
        : result.receipt;
      replyMemberJson(res, 200, response);
      return true;
    }
    if (accountPatchRoute) {
      const body = await readMemberJson(req),
        input = adminRoute
          ? adminAccountPatch(decodeURIComponent(accountRoute![2]!), body)
          : ownerAccountPatch(decodeURIComponent(accountRoute![2]!), body),
        actorId = commandActorId,
        actorKind = ownerRoute ? 'member' : 'admin',
        operation = `PATCH:${url.pathname}`,
        command: ReturnType<typeof managementCommand> = adminRoute
          ? { actorId, actorKind }
          : managementCommand(req, actorId, operation, body, actorKind);
      const result = await ownerAccountCommand(
        accounts,
        authorize,
        {
          commandId: command.commandId ?? randomUUID(),
          fingerprint:
            command.fingerprint ??
            (adminRoute
              ? domainDigest({ operation, requestId: randomUUID() })
              : domainDigest({ operation, body })),
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
      const response = adminRoute
        ? await authorize((actor) =>
            accounts.readAdminAccountProfile(input.accountId, false, actor),
          )
        : result.receipt;
      replyMemberJson(res, 200, response);
      return true;
    }
    if (req.method === 'POST') {
      const body = await readMemberJson(req),
        groupInput = {
          groups: body.groups,
          expectedConfigVersion: body.expectedConfigVersion as number,
          redistributeRemaining: body.redistributeRemaining as boolean | undefined,
        };
      if (body.action === 'discord-invite') {
        const result = await authorize((member) => {
          if (!discordLoginEnabled(env)) {
            fail(409, 'Discord 登录未启用');
          }
          return members.createMemberDiscordInvite(body.userId, member, now());
        });
        replyMemberJson(res, 200, result);
        return true;
      }
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
      if (body.action === 'preview-delete-member') {
        const preview = await authorize((member) =>
          quotas.manageMemberQuota(
            {
              commandId: randomUUID(),
              fingerprint: domainDigest({
                operation: 'member-management:preview-delete-member',
                body,
              }),
              actorId: member?.id ?? 'local-admin',
              actorKind: member ? 'member' : 'admin',
              recordedAt: now(),
              correlationId: null,
              causationId: null,
              policyVersion: 'member-management-v1',
              pricingVersion: 'none',
            },
            body as Parameters<RelationalQuotaStore['manageMemberQuota']>[1],
            member,
            deps.controls,
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
          'transfer-quota',
          'delete-member',
          'groups',
          ...operatingActions,
          'credentials',
        ].includes(body.action as string)
      ) {
        fail(400, '尚未接入此管理操作');
      }
      if (body.action === 'credentials') {
        const updating = Boolean(body.userId),
          hasGroup = body.groupId !== undefined && body.groupId !== null && body.groupId !== '';
        const validate = () => {
          if (discordLoginEnabled(env) && updating) {
            fail(409, 'Discord 成员不使用登录密码');
          }
          if (hasGroup && (typeof body.groupId !== 'string' || updating)) {
            fail(400, '只有创建新成员时可以指定 groupId');
          }
          if (updating && typeof body.userId !== 'string') {
            fail(404, '账户不存在或未启用');
          }
        };
        await authorize(() => {
          validate();
          if (!updating) {
            assertMemberRegistrationCapacity(members.readManagementMemberProfiles().length);
          }
        });
        // credentials 沿原私有非 keyed 契约；KDF 在 SQL 外，不把秘密或其意图摘要写入公共回执。
        const fields = discordLoginEnabled(env)
            ? { ...body, password: randomBytes(32).toString('base64url') }
            : body,
          prepared = updating
            ? {
                kind: 'update' as const,
                value: await members.prepareMemberCredentials(body.userId as string, fields),
              }
            : { kind: 'create' as const, value: await prepareMemberCredentials(fields) };
        const result = await authorize((member) => {
          validate();
          if (prepared.kind === 'update') {
            return members.setMemberCredentials(prepared.value, member);
          }
          const credentials = prepared.value,
            memberId = randomUUID(),
            actorId = member?.id ?? 'local-admin',
            actorKind = member ? 'member' : 'admin',
            metadata: Parameters<RelationalMemberStore['registerMember']>[0] = {
              commandId: randomUUID(),
              fingerprint: domainDigest({ operation: 'register-management-member', memberId }),
              actorId,
              actorKind,
              recordedAt: now(),
              correlationId: null,
              causationId: null,
              policyVersion: 'member-management-v1',
              pricingVersion: 'none',
            };
          members.registerMember(
            metadata,
            {
              memberId,
              displayName: credentials.username,
              note: String(body.note || '管理员创建的成员').slice(0, 120),
              apiKey: `STA1N-${randomBytes(18).toString('base64url')}`,
              credentials,
            },
            member,
          );
          if (hasGroup) {
            const policy = quotas.readQuotaConfiguration(),
              groups = structuredClone(policy.groups),
              group = groups.find((item) => item.id === body.groupId);
            if (!group) {
              fail(400, '指定的共享组不存在');
            }
            group.members.push(memberId);
            quotas.configureGroups(
              {
                ...metadata,
                commandId: randomUUID(),
                fingerprint: domainDigest({
                  operation: 'assign-created-member',
                  memberId,
                  groupId: group.id,
                }),
              },
              { groups, expectedConfigVersion: policy.configVersion },
              member,
            );
          }
          return {
            id: memberId,
            username: credentials.username,
            ...(hasGroup ? { groupId: body.groupId } : {}),
          };
        });
        replyMemberJson(res, 200, result);
        return true;
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
            policyVersion: [
              'member-status',
              'member-admission',
              'transfer-quota',
              'delete-member',
            ].includes(body.action as string)
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
        if (['transfer-quota', 'delete-member'].includes(body.action as string)) {
          const result = quotas.manageMemberQuota(
            metadata,
            body as Parameters<RelationalQuotaStore['manageMemberQuota']>[1],
            member,
            deps.controls,
          );
          return 'receipt' in result ? result.receipt : result;
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
    if (adminSummaryRoute) {
      await authorize(() => null);
      const at = now(),
        stats = await reports.readAdminSummaryStats(at);
      const data = await authorize((actor) => {
        const imageCount = reports.readLogicalArtifactCount(),
          jobs = requests.readManagementFailedJobs().map((job) =>
            publicRelationalJob(job, job.request, {
              imageUrl: deps.imageUrl,
              errorMessage: (message) => publicErrorMessage(message, { generation: true }),
              generationProgress: deps.previews.frames.get(job.id)?.progress ?? {},
              now: at,
            }),
          );
        // 统计 await 后才复核当前管理员并读取当前私有连接；不缓存旧 PST/proxy。
        return {
          settings: {
            ...configuration.readAdminSettings(),
            fairQuota: quotas.readAdminQuotaConfiguration(),
          },
          userCount: members.readManagementMemberCount(),
          accounts: accounts.readAccountManagementDirectory().map(({ id }) => {
            const { cooldownReason: _reason, ...profile } = accounts.readAdminAccountProfile(
              id,
              url.searchParams.get('revealTokens') === '1',
              actor,
            );
            return {
              ...profile,
              stats1h: stats.accountStats1h[id] ?? finalizeStats({ done: 0, failed: 0 }),
            };
          }),
          imageCount,
          imageTotal: imageCount,
          cacheImageCount: imageCount,
          requestStats1m: stats.requestStats1m,
          generationStats1m: stats.generationStats1m,
          jobStats1h: stats.jobStats1h,
          generationSpeed1h: stats.generationSpeed1h,
          usageHourlyDays: stats.usageHourlyDays,
          errorLogs: reports.readErrorLogs(Math.max(0, at - 7 * 86400000), 100),
          jobs,
        };
      });
      replyMemberJson(res, 200, data);
    } else if (summaryRoute) {
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
              note: profile.note,
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
