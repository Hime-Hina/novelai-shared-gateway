import {
  handleMemberAuthenticationRoutes,
  renewMemberCookie as renewCookie,
} from './authentication-http.ts';
import {
  memberCookie as cookie,
  assertMemberSameOrigin as sameOrigin,
  readMemberJson as json,
  replyMemberJson as reply,
} from './http-request.ts';
import { createHash, randomBytes } from 'node:crypto';
import { managementCommand } from '../http/management-command.ts';
import { formatAmount, parseAmount } from '../quota/resource-amounts.ts';
import {
  domainDigest,
  inheritDomainCommand,
  recordDomainEvent,
  type DomainCommandMetadata,
} from '../events/domain-events.ts';
import type { PersistenceOptions } from '../storage/sqlite-store.ts';
import type { IncomingMessage, ServerResponse } from 'node:http';
import {
  setMemberCredentials,
  authenticateMemberSession,
  isOwner,
  type MemberSession,
  type MemberPasswordStore,
  assertMemberRegistrationCapacity,
  assertMemberCanDisable,
} from './authentication.ts';
import {
  configureFairGroups,
  configureAnlasFallback,
  fairQuotaSummary,
  estimateFairQuota,
  syncFairQuota,
  manageFairMember,
  manageOperatingFunding,
  type FairQuotaDatabase,
  type FairQuotaState,
  type FairCharge,
} from '../quota/fair-share.ts';
import { configureFreeImages, freeImagePolicy } from '../quota/free-image-policy.ts';
import { sharedAccountStatus } from '../quota/shared-status.ts';
import {
  discordLoginEnabled,
  createDiscordInvite,
  requireDiscordMembership,
  type DiscordUser,
  type DiscordDependencies,
  type DiscordAuthenticationStore,
} from './discord-auth.ts';
import { memberCharge, updateLossAlertPolicy } from '../quota/failure-accounting.ts';
import type { stateEventsFor } from '../events/state-notifications.ts';
import { streamMemberSharedEvents } from './shared-events.ts';
import { readActivityHeatmap } from '../reports/activity-heatmap.ts';
import {
  handleMemberReadRoutes,
  publicMemberProfile,
  resolveMemberPublicBaseUrl,
} from './read-http.ts';
import { readServiceResources } from '../reports/service-resources.ts';
import { readR2Backups, readR2Operations } from '../reports/r2-analytics.ts';
import { readPikPakQuota } from '../storage/pikpak-storage.ts';
import type { AccountingJob } from '../generation/accounting.ts';
import type { OperatingFundingContext } from '../quota/operating-accounting.ts';
import type { ownerOperation } from './owner-operations.ts';

interface MemberUser extends DiscordUser {
  token?: string;
  acceptingJobs?: boolean;
  balance?: number;
  createdAt?: string;
}
interface MemberJob extends AccountingJob {
  status?: string;
  requestId?: string;
  imageId?: string;
  createdAt?: string;
}
type LedgerRow = Partial<NonNullable<ReturnType<OperatingFundingContext['findEvent']>>> & {
  id: string;
  lossEventId?: string;
};
type MemberQuotaState = FairQuotaState & {
  policyHistory?: { at: number; actor: string; autoAnlasFallback: boolean }[];
};
interface MemberDatabase extends FairQuotaDatabase {
  users: MemberUser[];
  jobs: MemberJob[];
  ledger: LedgerRow[];
  settings: { publicBaseUrl?: string; fairQuota?: MemberQuotaState | null };
}
type AuthStore = MemberPasswordStore;
type ActivityStore = Parameters<typeof readActivityHeatmap>[0];
interface MemberStore extends AuthStore, ActivityStore, DiscordAuthenticationStore {
  readMemberSessionState(tokenHash: string): MemberSession | null;
  dataDir?: string;
  readR2Usage?: import('../storage/sqlite-store.ts').JsonStore['readR2Usage'];
  readPikPakUsage?: import('../storage/sqlite-store.ts').JsonStore['readPikPakUsage'];
  events: Parameters<typeof stateEventsFor>[0]['events'];
  update<T>(
    mutator: (db: MemberDatabase) => T | Promise<T>,
    options: ({ persist: false } | { collections: string[]; immediate: true }) &
      Pick<PersistenceOptions<T>, 'domainCommand' | 'authorize'>,
  ): Promise<T>;
  readFailureAccounting(before: string | null): Promise<unknown>;
  readFreeImageUsage?: import('../storage/sqlite-store.ts').JsonStore['readFreeImageUsage'];
  selectItemById(collection: 'ledger', id: string | undefined): LedgerRow | null;
  readOperatingLossSettlements(id: string): LedgerRow[];
  readMemberImageIds(user: MemberUser, jobIds: string[]): Promise<Map<string, string>>;
  readMemberJobPage: import('../storage/sqlite-store.ts').JsonStore['readMemberJobPage'];
  readMemberLateResults(user: MemberUser): Promise<
    {
      id: string;
      model?: string | null;
      createdAt?: string | null;
      lateResultAt: string;
      imageId: string;
      charge?: FairCharge | null;
    }[]
  >;
}
interface MemberDependencies extends Omit<DiscordDependencies, 'store'> {
  store: MemberStore;
  assertAdmin(req: IncomingMessage, url: URL): void;
  createId(prefix: string): string;
  createPublicToken(prefix: string): string;
  publicImageUrl(imageId: string): string;
  ownerOperation(
    url: URL,
    method: string | undefined,
    body: Record<string, unknown> | undefined,
    command?: Pick<PersistenceOptions, 'domainCommand' | 'authorize'>,
  ): ReturnType<typeof ownerOperation>;
  changeMemberAdmission(
    userId: unknown,
    acceptingJobs: boolean,
    actorId?: string,
    command?: Pick<PersistenceOptions, 'domainCommand' | 'authorize'>,
  ): Promise<unknown>;
  getGenerationRequest(requestId: string, memberId: string): Promise<unknown>;
  cancelGenerationRequest(requestId: string, memberId: string): Promise<unknown>;
  handleMemberGeneration?(
    req: IncomingMessage,
    res: ServerResponse,
    url: URL,
    user: { id: string },
    revalidate: () => Promise<boolean>,
  ): Promise<boolean>;
}
const error: (statusCode: number, message: string) => never = (statusCode, message) => {
  throw Object.assign(new Error(message), { statusCode });
};

const keyedManagementActions = new Set([
  'free-image-policy',
  'loss-alert-policy',
  'operating-contribute',
  'operating-register-funding',
  'operating-cover-loss',
  'operating-reconcile-return',
  'operating-confirm-debits',
  'transfer-quota',
  'delete-member',
  'fallback-policy',
  'member-status',
  'member-admission',
  'groups',
]);
function recentActivity(db: MemberDatabase, user: MemberUser) {
  const jobs = db.jobs.filter((job) => job.userToken === user.token).slice(0, 30);
  return {
    total: jobs.length,
    done: jobs.filter((job) => job.status === 'done').length,
    failed: jobs.filter((job) => job.status === 'failed').length,
    queued: jobs.filter((job) => ['queued', 'pending'].includes(job.status!)).length,
    running: jobs.filter((job) => job.status === 'running').length,
  };
}

// 部署配置优先于历史库内地址，成员与管理员复制到的入口保持一致。
export function memberPublicBaseUrl(
  db: Pick<MemberDatabase, 'settings'>,
  env: NodeJS.ProcessEnv = process.env,
): string {
  return resolveMemberPublicBaseUrl(db.settings.publicBaseUrl, env);
}

export async function handleMemberRoutes(
  req: IncomingMessage,
  res: ServerResponse,
  url: URL,
  deps: MemberDependencies,
): Promise<boolean> {
  const member = url.pathname.startsWith('/api/member/');
  const ownerRoute = url.pathname.startsWith('/api/member/owner/');
  const admin =
    url.pathname === '/api/admin/member-management' ||
    url.pathname === '/api/member/owner/member-management';
  if (!member && !admin) {
    return false;
  }
  if (await handleMemberAuthenticationRoutes(req, res, url, deps)) {
    return true;
  }
  const env = deps.env || process.env,
    discordEnabled = discordLoginEnabled(env);
  sameOrigin(req);
  const pageVisit = req.method === 'GET' && req.headers['x-member-activity'] === '1';
  const { store } = deps;
  const quotaView = (db: MemberDatabase, user: MemberUser) => {
    const quota = fairQuotaSummary(db, user);
    return quota && { ...quota, freeImages: store.readFreeImageUsage?.(db, user.id) };
  };
  let actorId = 'local-admin';
  let command: DomainCommandMetadata | undefined;
  let authorize: PersistenceOptions['authorize'] = () => deps.assertAdmin(req, url);
  const updateManagement: MemberStore['update'] = (mutator, options) =>
    store.update(mutator, {
      ...options,
      domainCommand: command || { actorId, actorKind: 'admin' },
      authorize,
    });
  if (ownerRoute) {
    // 认证返回同一 store.users 记录；认证模块自身只声明它读取的字段。
    const viewer = (await authenticateMemberSession(store, cookie(req), env))?.user as
      MemberUser | undefined;
    if (!viewer) {
      error(401, '请先使用 Discord 登录');
    }
    if (!isOwner(viewer, env)) {
      error(403, '仅 owner 可访问管理功能');
    }
    await requireDiscordMembership(viewer, deps);
    if (pageVisit && admin && !url.search) {
      await renewCookie(req, res, store, env);
    }
    actorId = viewer.id;
    const authVersion = viewer.memberAuth?.version;
    const sessionHash = createHash('sha256').update(cookie(req)).digest('hex');
    // 入队前的鉴权可能已过时；领取旧回执和执行新命令都必须检查当前会话与权限。
    authorize = (db) => {
      const session = store.readMemberSessionState(sessionHash);
      if (
        !session ||
        session.memberId !== actorId ||
        session.authEpoch !== authVersion ||
        session.expiresAt <= Date.now()
      ) {
        error(401, '管理会话已失效，请重新登录');
      }
      const current = db.users.find((user) => user.id === actorId) as MemberUser | undefined;
      if (
        !current ||
        current.enabled !== true ||
        current.memberAuth?.version !== authVersion ||
        current.discord?.member !== true ||
        !isOwner(current, env)
      ) {
        error(403, '管理授权已失效，请重新确认身份');
      }
    };
    if (!admin) {
      const body = req.method === 'GET' ? undefined : await json(req);
      const keyed =
        body &&
        ((req.method === 'PATCH' && /^\/api\/member\/owner\/accounts\/[^/]+$/.test(url.pathname)) ||
          (req.method === 'POST' && url.pathname === '/api/member/owner/accounts/quota'));
      const options = keyed
        ? {
            domainCommand: managementCommand(req, actorId, `${req.method}:${url.pathname}`, body!),
            authorize,
          }
        : undefined;
      const result = await deps.ownerOperation(url, req.method, body, options);
      reply(res, 200, result);
      return true;
    }
  }
  if (admin) {
    // 原私有管理 API 保持本机与密钥校验；owner 别名已经过独立会话鉴权。
    if (!ownerRoute) {
      if (!/^(127\.0\.0\.1|localhost)(:\d+)?$/.test(String(req.headers.host || ''))) {
        error(404, 'not found.');
      }
      deps.assertAdmin(req, url);
    }
    if (req.method === 'GET') {
      if (url.searchParams.get('view') === 'heatmap') {
        reply(
          res,
          200,
          await readActivityHeatmap(store, url.searchParams, {
            memberId: null,
            allowMemberFilter: true,
          }),
        );
        return true;
      }
      if (url.searchParams.get('view') === 'losses') {
        reply(res, 200, await store.readFailureAccounting(url.searchParams.get('before')));
        return true;
      }
      const payload = await updateManagement(
        (db) => {
          const draft = estimateFairQuota(db, Date.now());
          return {
            groups: draft.settings.fairQuota?.groups || [],
            status: draft.settings.fairQuota?.status,
            shared: sharedAccountStatus(draft),
            autoAnlasFallback: draft.settings.fairQuota?.autoAnlasFallback === true,
            configVersion: draft.settings.fairQuota?.configVersion ?? 1,
            freeImagePolicy: freeImagePolicy(draft),
            publicBaseUrl: memberPublicBaseUrl(db),
            discordLogin: discordEnabled,
            ownerUserId: env.OWNER_USER_ID || null,
            users: db.users.map((u) => ({
              id: u.id,
              username: u.memberAuth?.username || '',
              note: u.note || '',
              enabled: u.enabled !== false,
              acceptingJobs: u.acceptingJobs !== false,
              discordId: u.discord?.id || null,
              inviteExpiresAt: u.discordInvite?.expiresAt || null,
              quota: quotaView(draft, u),
              recentActivity: recentActivity(db, u),
            })),
          };
        },
        { persist: false },
      );
      reply(res, 200, {
        ...payload,
        serviceResources: {
          ...(await readServiceResources(store.dataDir)),
          ...(env.R2_BUCKET && store.readR2Usage
            ? {
                r2: {
                  ...store.readR2Usage(),
                  operations: readR2Operations(env),
                  backups: readR2Backups(env),
                },
              }
            : {}),
          ...(env.PIKPAK_WEBDAV_URL && store.readPikPakUsage
            ? { pikpak: { ...store.readPikPakUsage(), quota: readPikPakQuota(env) } }
            : {}),
        },
      });
      return true;
    }
    if (req.method !== 'POST') {
      error(405, 'method not allowed');
    }
    const body = await json(req);
    if (keyedManagementActions.has(body.action as string)) {
      command = managementCommand(req, actorId, `member-management:${body.action}`, body);
    }
    if (body.action === 'free-image-policy') {
      const policy = await updateManagement((db) => configureFreeImages(db, body, actorId), {
        collections: ['settings', 'ledger'],
        immediate: true,
      });
      reply(res, 200, { policy });
      return true;
    }
    if (body.action === 'loss-alert-policy') {
      const policy = await updateManagement((db) => updateLossAlertPolicy(db, body, actorId), {
        collections: ['settings', 'ledger'],
        immediate: true,
      });
      reply(res, 200, { policy });
      return true;
    }
    if (
      ownerRoute &&
      body.userId === env.OWNER_USER_ID &&
      (['delete-member', 'preview-delete-member'].includes(body.action as string) ||
        (body.action === 'member-status' && body.enabled === false))
    ) {
      error(409, '不能停用或删除当前 owner');
    }
    if (
      [
        'operating-contribute',
        'operating-register-funding',
        'operating-cover-loss',
        'operating-reconcile-return',
        'operating-confirm-debits',
      ].includes(body.action as string)
    ) {
      // 身份来自已鉴权的 owner 配置，不接受客户端指定其他成员作为注资来源。
      if (!env.OWNER_USER_ID || (body.userId && body.userId !== env.OWNER_USER_ID)) {
        error(403, '只能从运营者本人已归属份额划入');
      }
      const result = await updateManagement(
        (db) => {
          syncFairQuota(db);
          return manageOperatingFunding(db, body, {
            actorId: env.OWNER_USER_ID!,
            findEvent: (id) =>
              (db.ledger.find((event) => event.id === id) ||
                store.selectItemById('ledger', id)) as ReturnType<
                OperatingFundingContext['findEvent']
              >,
            coveredAmount: (id) =>
              formatAmount(
                [
                  ...new Map(
                    [
                      ...store.readOperatingLossSettlements(id),
                      ...db.ledger.filter(
                        (event) =>
                          event.type === 'operating_loss_covered' && event.lossEventId === id,
                      ),
                    ].map((event) => [event.id, event]),
                  ).values(),
                ].reduce((total, event) => total + parseAmount(event.amount!, 'nearest'), 0n),
              ),
          });
        },
        { collections: ['settings', 'ledger'], immediate: true },
      );
      reply(res, 200, result);
      return true;
    }
    if (body.action === 'discord-invite') {
      if (!discordEnabled) {
        error(409, 'Discord 登录未启用');
      }
      const result = await updateManagement((db) => createDiscordInvite(db, body.userId), {
        collections: ['users'],
        immediate: true,
      });
      reply(res, 200, result);
      return true;
    }
    if (
      ['transfer-quota', 'preview-delete-member', 'delete-member'].includes(body.action as string)
    ) {
      const preview = body.action === 'preview-delete-member';
      const result = await updateManagement(
        (db) => manageFairMember(db, body),
        preview
          ? { persist: false }
          : { collections: ['users', 'settings', 'ledger'], immediate: true },
      );
      reply(res, 200, result);
      return true;
    }
    if (body.action === 'fallback-policy') {
      const result = await updateManagement(
        (db) => configureAnlasFallback(db, body.enabled, actorId),
        { collections: ['settings'], immediate: true },
      );
      reply(res, 200, result);
      return true;
    }
    if (body.action === 'member-key') {
      const result = await updateManagement(
        (db) => {
          const user = db.users.find((u) => u.id === body.userId);
          if (!user) {
            error(404, '成员不存在');
          }
          return { apiKey: user.token };
        },
        { persist: false },
      );
      reply(res, 200, result);
      return true;
    }
    if (body.action === 'member-status') {
      if (typeof body.enabled !== 'boolean') {
        error(400, 'enabled 必须为布尔值');
      }
      const result = await updateManagement(
        (db) => {
          const user = db.users.find((u) => u.id === body.userId);
          if (!user) {
            error(404, '成员不存在');
          }
          if (!body.enabled) {
            assertMemberCanDisable(
              db.jobs.some(
                (job) =>
                  job.userToken === user.token &&
                  ['queued', 'pending', 'running'].includes(job.status!),
              ) ||
                Object.values(db.settings.fairQuota?.reservations || {}).some(
                  (charge) => charge.userId === user.id && charge.status === 'reserved',
                ),
            );
          }
          user.enabled = body.enabled as boolean;
          user.updatedAt = new Date().toISOString();
          // 停用时改变会话版本，之后重新启用也不能复活先前登录的 Cookie。
          if (!body.enabled && user.memberAuth) {
            user.memberAuth = { ...user.memberAuth, version: randomBytes(16).toString('hex') };
          }
          recordDomainEvent(db, {
            eventType: 'MemberEligibilityChanged',
            streamId: 'sharing:default',
            memberId: user.id,
            payload: { memberId: user.id, enabled: user.enabled, at: user.updatedAt, actorId },
          });
          return { id: user.id, enabled: user.enabled };
        },
        { collections: ['users'], immediate: true },
      );
      reply(res, 200, result);
      return true;
    }
    if (body.action === 'member-admission') {
      if (typeof body.acceptingJobs !== 'boolean') {
        error(400, 'acceptingJobs 必须为布尔值');
      }
      const result = await deps.changeMemberAdmission(body.userId, body.acceptingJobs, actorId, {
        domainCommand: command,
        authorize,
      });
      reply(res, 200, result);
      return true;
    }
    if (body.action === 'preview-groups') {
      if (
        body.redistributeRemaining !== undefined &&
        typeof body.redistributeRemaining !== 'boolean'
      ) {
        error(400, 'redistributeRemaining 必须为布尔值');
      }
      const result = await updateManagement(
        (db) => {
          const configVersion = db.settings.fairQuota?.configVersion ?? 1;
          if (
            !Number.isInteger(body.expectedConfigVersion) ||
            body.expectedConfigVersion !== configVersion
          ) {
            error(409, '分组版本缺失或已变化，请保留草稿并刷新后重新编辑');
          }
          // 预览只操作独立副本，恢复、重分与审计都不能污染真实账本。
          const draft = {
            ...db,
            ledger: [...(db.ledger || [])],
            settings: structuredClone(db.settings),
            users: structuredClone(db.users),
          };
          configureFairGroups(draft, body.groups, {
            actorId: 'administrator',
            redistributeRemaining: body.redistributeRemaining === true,
          });
          syncFairQuota(draft);
          return {
            groups: draft.settings.fairQuota!.groups,
            users: draft.users.map((user) => ({
              id: user.id,
              quota: fairQuotaSummary(draft, user),
            })),
            redistributeRemaining: body.redistributeRemaining === true,
            configVersion,
          };
        },
        { persist: false },
      );
      reply(res, 200, result);
      return true;
    }
    if (body.action === 'credentials') {
      if (discordEnabled && body.userId) {
        error(409, 'Discord 成员不使用登录密码');
      }
      const hasGroup = body.groupId !== undefined && body.groupId !== null && body.groupId !== '';
      if (hasGroup && (typeof body.groupId !== 'string' || body.userId)) {
        error(400, '只有创建新成员时可以指定 groupId');
      }
      const result = await updateManagement(
        async (db) => {
          const draft = {
            ...db,
            ledger: [...(db.ledger || [])],
            users: structuredClone(db.users),
            settings: structuredClone(db.settings),
          };
          inheritDomainCommand(db, draft);
          let userId = body.userId;
          if (!userId) {
            assertMemberRegistrationCapacity(draft.users.length);
            const user = {
              id: deps.createId('usr'),
              token: deps.createPublicToken('STA1N'),
              balance: 0,
              enabled: true,
              acceptingJobs: true,
              jobAdmissionVersion: 0,
              note: String(body.note || '管理员创建的成员').slice(0, 120),
              createdAt: new Date().toISOString(),
            };
            userId = user.id;
            draft.users.push(user);
            recordDomainEvent(draft, {
              eventType: 'MemberEligibilityChanged',
              streamId: 'sharing:default',
              memberId: user.id,
              payload: {
                memberId: user.id,
                enabled: true,
                acceptingJobs: true,
                jobAdmissionVersion: 0,
                createdAt: user.createdAt,
                actorId,
              },
            });
          }
          const result = await setMemberCredentials(
            draft,
            userId as string,
            discordEnabled ? { ...body, password: randomBytes(32).toString('base64url') } : body,
          );
          if (hasGroup) {
            syncFairQuota(draft);
            const groups = structuredClone(draft.settings.fairQuota?.groups || []);
            const group = groups.find((g) => g.id === body.groupId);
            if (!group) {
              error(400, '指定的共享组不存在');
            }
            group.members.push(userId as string);
            configureFairGroups(draft, groups, { actorId: 'administrator' });
          }
          // 所有校验和密码派生成功后才整体提交，失败不会遗留用户或半份账本。
          db.users = draft.users;
          if (hasGroup) {
            db.settings = draft.settings;
            db.ledger = draft.ledger;
          }
          return { ...result, ...(hasGroup ? { groupId: body.groupId } : {}) };
        },
        { collections: ['users', ...(hasGroup ? ['settings', 'ledger'] : [])], immediate: true },
      );
      reply(res, 200, result);
      return true;
    }
    if (body.action === 'groups') {
      if (
        body.redistributeRemaining !== undefined &&
        typeof body.redistributeRemaining !== 'boolean'
      ) {
        error(400, 'redistributeRemaining 必须为布尔值');
      }
      const result = await updateManagement(
        (db) => {
          const currentVersion = db.settings.fairQuota?.configVersion ?? 1;
          if (
            !Number.isInteger(body.expectedConfigVersion) ||
            body.expectedConfigVersion !== currentVersion
          ) {
            error(409, '分组版本缺失或已变化，请保留草稿并刷新后重新编辑');
          }
          configureFairGroups(db, body.groups, {
            actorId: 'administrator',
            redistributeRemaining: body.redistributeRemaining === true,
          });
          syncFairQuota(db);
          return {
            groups: db.settings.fairQuota!.groups,
            status: db.settings.fairQuota!.status,
            configVersion: db.settings.fairQuota!.configVersion,
          };
        },
        { collections: ['settings', 'ledger'], immediate: true },
      );
      reply(res, 200, result);
      return true;
    }
    error(400, '不支持的管理操作');
  }

  let session = await authenticateMemberSession(store, cookie(req), env);
  const user = session?.user as MemberUser | undefined;
  if (!user) {
    error(401, '请先登录');
  }
  await requireDiscordMembership(user, deps);
  if (pageVisit && url.pathname === '/api/member/me') {
    session = await renewCookie(req, res, store, env);
  }
  if (
    await deps.handleMemberGeneration?.(req, res, url, user, async () => {
      const current = (await authenticateMemberSession(store, cookie(req), env))?.user;
      if (!current || current.id !== user.id) {
        return false;
      }
      await requireDiscordMembership(current, deps);
      return true;
    })
  ) {
    return true;
  }
  const requestRoute = url.pathname.match(/^\/api\/member\/requests\/([^/]+)(\/cancel)?$/);
  if (
    requestRoute &&
    ((req.method === 'GET' && !requestRoute[2]) || (req.method === 'POST' && requestRoute[2]))
  ) {
    const requestId = decodeURIComponent(requestRoute[1]);
    if (requestRoute[2]) {
      await json(req);
    }
    const result = requestRoute[2]
      ? await deps.cancelGenerationRequest(requestId, user.id)
      : await deps.getGenerationRequest(requestId, user.id);
    reply(res, 200, result);
    return true;
  }
  if (req.method !== 'GET') {
    error(403, '成员账户没有管理权限，请联系管理员');
  }
  if (url.pathname === '/api/member/heatmap') {
    reply(
      res,
      200,
      await readActivityHeatmap(store, url.searchParams, {
        memberId: user.id,
        allowMemberFilter: false,
      }),
    );
    return true;
  }
  if (url.pathname === '/api/member/shared-status') {
    const state = await store.update((db) => sharedAccountStatus(db), { persist: false });
    reply(res, 200, state);
    return true;
  }
  if (url.pathname === '/api/member/shared-events') {
    await streamMemberSharedEvents(res, {
      memberId: user.id,
      store,
      readCurrent: async () => {
        const viewer = (await authenticateMemberSession(store, cookie(req), env))?.user;
        if (!viewer) {
          return null;
        }
        await requireDiscordMembership(viewer, deps);
        const state = await store.update((db) => sharedAccountStatus(db), { persist: false });
        return { memberId: viewer.id, owner: isOwner(viewer, env), state };
      },
    });
    return true;
  }
  if (
    await handleMemberReadRoutes(req, res, url, {
      publicImageUrl: deps.publicImageUrl,
      readKey: async () => ({
        apiKey: user.token,
        publicBaseUrl: await store.update((db) => memberPublicBaseUrl(db), { persist: false }),
      }),
      readHistory: async (options) => {
        const result = await store.readMemberJobPage(user, options);
        return {
          ...result,
          jobs: result.jobs.map(({ charge, ...job }) => ({
            ...job,
            charge: memberCharge(charge),
          })),
        };
      },
    })
  ) {
    return true;
  }
  if (url.pathname === '/api/member/me') {
    const payload = await store.update(
      (db) => {
        const draft = estimateFairQuota(db, Date.now());
        return {
          username: user.memberAuth!.username,
          owner: isOwner(user, env),
          sessionExpiresAt: session!.expiresAt,
          acceptingJobs: user.acceptingJobs !== false,
          quota: quotaView(draft, user),
          jobs: db.jobs
            .filter((j) => j.userToken === user.token)
            .slice(0, 30)
            .map((j) => ({
              id: j.id,
              requestId: j.requestId || null,
              createdAt: j.createdAt,
              lateResultAvailable: j.lateResultAvailable === true,
              imageId: j.imageId || '',
              model: j.request?.model || '',
              status: j.status,
              charge: memberCharge(j.fairCharge),
            })),
        };
      },
      { persist: false },
    );
    const [ownedImages, lateResults] = await Promise.all([
      store.readMemberImageIds(
        user,
        payload.jobs.map((job) => job.id),
      ),
      store.readMemberLateResults(user),
    ]);
    for (const job of payload.jobs) {
      job.imageId = ownedImages.get(job.id) || '';
    }
    const ownedLate = lateResults.map((job) => ({
      id: job.id,
      model: job.model || '',
      createdAt: job.createdAt,
      lateResultAt: job.lateResultAt,
      imageId: job.imageId,
      charge: memberCharge(job.charge),
    }));
    reply(
      res,
      200,
      publicMemberProfile({ ...payload, lateResults: ownedLate }, deps.publicImageUrl),
    );
    return true;
  }
  error(404, 'not found.');
}
