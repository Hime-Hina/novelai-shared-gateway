import type { RelationalCommitResult } from '../events/relational-event-store.ts';
import type {
  RelationalAccountStore,
  AccountCommandMetadata,
  OwnerAccountMutation,
  AccountEnableTicket,
} from '../storage/relational-account-store.ts';
import type { MemberUser } from './authentication.ts';
import {
  accountQuotaResult,
  accountQuotaErrorResult,
  type createAccountQuotaQuery,
} from '../quota/account-refresh.ts';
import type { FairAccount } from '../quota/fair-share.ts';
import { isUsableOpus } from '../quota/eligibility.ts';
import type { PersistenceOptions } from '../storage/sqlite-store.ts';
import type { Json } from '../events/domain-events.ts';
import { parseAmount, amountNumber } from '../quota/resource-amounts.ts';
import { activityDate } from '../reports/activity-heatmap.ts';
import { normalizeSocksProxyUrl } from '../protocols/novelai-client.ts';

export interface OwnerJobRow {
  id: string;
  memberId: string;
  username: string;
  model?: string | null;
  status: string | null;
  createdAt: string | null;
  startedAt?: string | null;
  upstreamStartedAt?: string | null;
  upstreamCompletedAt?: string | null;
  chargeStatus?: string | null;
  knownNoCharge?: boolean;
  resource?: string | null;
  amount?: number | string | null;
  fixed?: number | string | null;
  purchased?: number | string | null;
  errorStatus?: number | null;
}
type UsageAccumulator = ReturnType<typeof empty>;
type GroupAccumulator = UsageAccumulator & { id: string; label: string };
type JobStatus = 'done' | 'failed' | 'queued' | 'pending' | 'running';
interface OwnerAccount extends Pick<
  FairAccount,
  | 'quotaTier'
  | 'quotaFixed'
  | 'quotaPurchased'
  | 'v5UsagePercent'
  | 'quotaCheckedAt'
  | 'quotaError'
  | 'subscriptionActive'
  | 'subscriptionExpiresAt'
> {
  id: string;
  token?: string;
  enabled?: boolean;
  inFlight?: number | string;
}
interface OwnerDatabase<T extends OwnerAccount = OwnerAccount> {
  accounts: T[];
  settings: { fairQuota?: { boundAccountId?: string | null } | null };
}
type EnableObservation = { id: string; token: string } | { error: unknown } | null;
export type OwnerAccountChange =
  | { kind: 'token'; token: string }
  | { kind: 'pause' }
  | { kind: 'enable'; observation: EnableObservation };
interface OwnerQuotaRefresh {
  checked: number;
  ok: number;
  failed: number;
  ignored?: number;
  accounts?: Pick<OwnerAccount, 'id' | 'token'>[];
}
interface OwnerDependencies {
  store: {
    readOwnerJobRows(since: string, until: string): Promise<OwnerJobRow[]>;
    update<T>(
      query: (db: OwnerDatabase) => T,
      options: { persist: false } & Pick<PersistenceOptions<T>, 'domainCommand' | 'authorize'>,
    ): Promise<T>;
  };
  refreshAccountQuotas(
    options: { ids: string[] },
    command?: Pick<
      PersistenceOptions<OwnerQuotaRefresh>,
      'domainCommand' | 'authorize' | 'commandReceipt'
    >,
  ): Promise<OwnerQuotaRefresh>;
  updateAccount(
    id: string,
    change: OwnerAccountChange,
    command?: Pick<
      PersistenceOptions<OwnerAccount>,
      'domainCommand' | 'authorize' | 'commandReceipt'
    >,
  ): Promise<OwnerAccount>;
}

const fail: (statusCode: number, message: string) => never = (statusCode, message) => {
  throw Object.assign(new Error(message), { statusCode });
};
const dayMs = 86400000,
  offsetMs = 8 * 3600000;
const day = (value: string | null): string => activityDate(value) || 'unknown';
const seconds = (
  start: string | null | undefined,
  end: string | null | undefined,
): number | null => {
  // 保持历史缺失日期经 Date.parse 得到 NaN、最终返回 null 的行为。
  const value = (Date.parse(end as string) - Date.parse(start as string)) / 1000;
  return Number.isFinite(value) && value >= 0 ? value : null;
};
const empty = () => ({
  total: 0,
  done: 0,
  failed: 0,
  queued: 0,
  pending: 0,
  running: 0,
  refunded: 0,
  free: 0,
  v5: 0n as bigint | null,
  fixed: 0n as bigint | null,
  purchased: 0n as bigint | null,
  waitSeconds: 0,
  waitCount: 0,
  generationSeconds: 0,
  generationCount: 0,
});
// 旧记录的 number 沿用账务 owner 的 nearest 读取规则；新关系文本仍严格保留 12 位精度。
const usageAmount = (value: number | string) =>
  parseAmount(value, typeof value === 'number' ? 'nearest' : 'exact');
function addResource(
  total: UsageAccumulator,
  resource: 'v5' | 'fixed' | 'purchased',
  value: OwnerJobRow['amount'],
) {
  const parsed = value == null ? null : usageAmount(value);
  total[resource] = total[resource] === null || parsed === null ? null : total[resource]! + parsed;
}
function resourceNumber(value: bigint | null): number | null {
  if (value === null) {
    return null;
  }
  const result = amountNumber(value);
  return Number.isFinite(result) ? result : null;
}
function add(total: UsageAccumulator, job: OwnerJobRow): void {
  total.total++;
  if (['done', 'failed', 'queued', 'pending', 'running'].includes(job.status!)) {
    total[job.status as JobStatus]++;
  }
  if (job.chargeStatus === 'refunded') {
    total.refunded++;
  }
  if (job.status === 'done' && job.chargeStatus === 'settled') {
    if (job.resource === 'free') {
      total.free++;
    }
    if (job.resource === 'v5') {
      addResource(total, 'v5', job.amount);
    }
    if (job.resource === 'anlas') {
      addResource(total, 'fixed', job.fixed);
      addResource(total, 'purchased', job.purchased);
    }
  }
  if (job.status === 'done' && !job.chargeStatus && job.knownNoCharge !== true) {
    total.v5 = null;
    total.fixed = null;
    total.purchased = null;
  }
  const wait = seconds(job.createdAt, job.startedAt),
    generation = seconds(job.upstreamStartedAt, job.upstreamCompletedAt);
  if (wait !== null) {
    total.waitSeconds += wait;
    total.waitCount++;
  }
  if (job.status === 'done' && generation !== null) {
    total.generationSeconds += generation;
    total.generationCount++;
  }
}
function finish<T extends UsageAccumulator>(total: T) {
  const {
    waitSeconds,
    waitCount,
    generationSeconds,
    generationCount,
    v5,
    fixed,
    purchased,
    ...counts
  } = total;
  return {
    ...counts,
    v5: resourceNumber(v5),
    fixed: resourceNumber(fixed),
    purchased: resourceNumber(purchased),
    averageWaitSeconds: waitCount ? waitSeconds / waitCount : null,
    averageGenerationSeconds: generationCount ? generationSeconds / generationCount : null,
  };
}

export function summarizeOwnerUsage(rows: readonly OwnerJobRow[]) {
  const total = empty(),
    members = new Map<string, GroupAccumulator>(),
    models = new Map<string, GroupAccumulator>(),
    dates = new Map<string, GroupAccumulator>();
  for (const job of rows) {
    add(total, job);
    for (const [map, key, label] of [
      [members, job.memberId, job.username],
      [models, job.model || 'unknown', job.model || '未知模型'],
      [dates, day(job.createdAt), activityDate(job.createdAt) || '未知日期'],
    ] as const) {
      if (!map.has(key)) {
        map.set(key, { id: key, label, ...empty() });
      }
      add(map.get(key)!, job);
    }
  }
  return {
    coverage: 'retained-records' as const,
    totals: finish(total),
    members: [...members.values()].map(finish),
    models: [...models.values()].map(finish),
    days: [...dates.values()].map(finish),
    recent: rows.map((j) => ({
      id: j.id,
      memberId: j.memberId,
      username: j.username,
      model: j.model,
      status: j.status,
      createdAt: j.createdAt,
      waitSeconds: seconds(j.createdAt, j.startedAt),
      generationSeconds: seconds(j.upstreamStartedAt, j.upstreamCompletedAt),
      resource: j.resource,
      amount: j.amount == null ? j.amount : resourceNumber(usageAmount(j.amount)),
      fixed: j.fixed == null ? j.fixed : resourceNumber(usageAmount(j.fixed)),
      purchased: j.purchased == null ? j.purchased : resourceNumber(usageAmount(j.purchased)),
      chargeStatus: j.chargeStatus,
      knownNoCharge: j.knownNoCharge,
      errorStatus: j.errorStatus,
    })),
  };
}

export function selectSharedAccount<T extends { id: string }>(
  accounts: readonly T[],
  boundAccountId: string | null | undefined,
) {
  return (
    accounts.find((account) => account.id === boundAccountId) ||
    (!boundAccountId && accounts.length === 1 ? accounts[0] : null)
  );
}
function sharedAccount<T extends OwnerAccount>(db: OwnerDatabase<T>) {
  return selectSharedAccount(db.accounts, db.settings.fairQuota?.boundAccountId);
}
export function ownerAccountConfiguration<T extends OwnerAccount>(
  db: OwnerDatabase<T>,
  id: string,
  change: OwnerAccountChange,
  now: number,
) {
  const account = sharedAccount(db);
  if (!account) {
    fail(409, '尚未确定共享上游账户');
  }
  if (account.id !== id) {
    fail(404, '未开放此管理操作');
  }
  // 仅在共同提交入口未命中原回执时，才按当前绑定和资格决定是否启用。
  if (change.kind === 'enable') {
    const { observation } = change;
    if (observation && 'error' in observation) {
      throw observation.error;
    }
    if (
      observation?.id !== account.id ||
      observation.token !== account.token ||
      !isUsableOpus({ ...account, enabled: true }, now)
    ) {
      fail(409, '未核验到有效 Opus，上游保持暂停');
    }
    return { account, body: { enabled: true } };
  }
  return {
    account,
    body: change.kind === 'token' ? { token: change.token, enabled: false } : { enabled: false },
  };
}
export function safeAccount(a: OwnerAccount | null | undefined) {
  return (
    a && {
      id: a.id,
      enabled: a.enabled === true,
      inFlight: Number(a.inFlight || 0),
      quotaTier: a.quotaTier,
      quotaFixed: a.quotaFixed,
      quotaPurchased: a.quotaPurchased,
      v5UsagePercent: a.v5UsagePercent,
      quotaCheckedAt: a.quotaCheckedAt,
      quotaError: a.quotaError ? '官方额度同步失败，请重新验证凭据' : '',
    }
  );
}

export function ownerStatisticsRange(params: URLSearchParams, now: number) {
  const days = Number(params.get('days') || 7);
  if (![0, 1, 7, 30, 365].includes(days)) {
    fail(400, '统计范围仅支持全部、1、7、30 或 365 天');
  }
  const since =
    days === 0
      ? ''
      : new Date(
          Math.floor((now + offsetMs) / dayMs) * dayMs - offsetMs - (days - 1) * dayMs,
        ).toISOString();
  return { days, since, until: new Date(now).toISOString(), timezone: 'Asia/Shanghai' };
}

export function assertOwnerAccountCredentialChangeAllowed(
  physicalLoad: number,
  reserved: boolean,
): void {
  if (physicalLoad > 0 || reserved) {
    fail(409, '有在途生成任务，请等待完成后更新密钥');
  }
}

export function assertOwnerAccountPst(token: unknown): asserts token is string {
  if (typeof token !== 'string' || token.length > 4096 || !/^pst-[A-Za-z0-9_-]+$/.test(token)) {
    fail(400, '请输入完整 PST');
  }
}

// 两个账户 PATCH 入口共享原单字段规则；名称、代理与多账户权重不映射到当前模型。
export function ownerAccountPatch(accountId: string, input: Record<string, unknown>) {
  if (Object.keys(input).length !== 1 || !['token', 'enabled'].includes(Object.keys(input)[0]!)) {
    fail(400, '只允许更新启用状态或 PST');
  }
  if ('token' in input) {
    assertOwnerAccountPst(input.token);
    return { kind: 'token' as const, accountId, token: input.token };
  }
  if (typeof input.enabled !== 'boolean') {
    fail(400, 'enabled 必须为布尔值');
  }
  return { kind: input.enabled ? ('enable' as const) : ('pause' as const), accountId };
}

export function normalizeAccountProxyUrl(value: unknown = ''): string {
  try {
    return normalizeSocksProxyUrl(value);
  } catch (error) {
    fail(400, error instanceof Error ? error.message : 'invalid SOCKS5 proxy URL.');
  }
}

export function adminAccountPatch(
  accountId: string,
  input: Record<string, unknown>,
): Extract<OwnerAccountMutation, { kind: 'configure' }> {
  const changes: Extract<OwnerAccountMutation, { kind: 'configure' }>['changes'] = {};
  if (input.name !== undefined) {
    changes.displayName = String(input.name).slice(0, 80);
  }
  if (input.weight !== undefined) {
    const value = Number(input.weight);
    changes.weight = Number.isFinite(value) ? Math.max(1, Math.min(100, value)) : 1;
  }
  if (input.proxyUrl !== undefined || input.socksProxy !== undefined || input.proxy !== undefined) {
    changes.proxyUrl = normalizeAccountProxyUrl(
      input.proxyUrl || input.socksProxy || input.proxy || '',
    );
  }
  if (input.token !== undefined) {
    changes.token = String(input.token).trim();
    assertOwnerAccountPst(changes.token);
  }
  if (input.enabled !== undefined) {
    if (typeof input.enabled !== 'boolean') {
      fail(400, 'enabled 必须为布尔值');
    }
    changes.enabled = input.enabled;
  }
  return { kind: 'configure', accountId, changes };
}

function accountCommandResult<T extends { receipt: Json }>(result: T): T {
  const receipt = result.receipt as { error?: { statusCode: number; message: string } };
  if (receipt.error) {
    fail(receipt.error.statusCode, receipt.error.message);
  }
  return result;
}

// 每次准备和落库都重新进入当前鉴权事务，网络期间不占写锁。
export async function ownerAccountCommand(
  store: Pick<
    RelationalAccountStore,
    'prepareAccountCommand' | 'startAccountEnable' | 'completeAccountEnable'
  >,
  authorize: <T>(write: (member: MemberUser | null) => T) => Promise<T | null>,
  metadata: AccountCommandMetadata,
  input: OwnerAccountMutation,
  query: ReturnType<typeof createAccountQuotaQuery> | undefined,
  now: () => number = Date.now,
) {
  const prepared = await authorize((member) =>
    store.prepareAccountCommand(metadata, input, member),
  );
  if (!prepared) {
    fail(401, '会话已失效，请重新登录');
  }
  if ('result' in prepared) {
    return accountCommandResult(prepared.result);
  }
  if (input.kind !== 'enable' && input.kind !== 'observe' && input.kind !== 'configure') {
    throw new Error('Only account observation may query upstream.');
  }
  if (!query) {
    fail(503, '账户额度查询尚未配置');
  }
  let ticket: AccountEnableTicket | undefined,
    prior: RelationalCommitResult | undefined,
    observation;
  try {
    const quota = await query(
      prepared.prepared.pst,
      prepared.prepared.configuration?.proxyUrl ?? prepared.prepared.proxyUrl,
      prepared.prepared.accountId,
      async () => {
        const started = await authorize((member) =>
          store.startAccountEnable(
            { ...metadata, recordedAt: now() },
            input,
            prepared.prepared,
            member,
          ),
        );
        if (!started) {
          fail(401, '会话已失效，请重新登录');
        }
        if ('result' in started) {
          prior = started.result;
          throw new Error('Account command already completed.');
        }
        ticket = started.ticket;
      },
    );
    if (!ticket) {
      throw new Error('Account quota query did not start its observation.');
    }
    observation = accountQuotaResult(
      ticket.accountId,
      quota,
      new Date(ticket.startedAt).toISOString(),
    );
  } catch (error) {
    if (prior) {
      return accountCommandResult(prior);
    }
    if (!ticket) {
      throw error;
    }
    observation = accountQuotaErrorResult(
      ticket.accountId,
      error,
      new Date(ticket.startedAt).toISOString(),
    );
  }
  const result = await authorize((member) =>
    store.completeAccountEnable(
      { ...metadata, recordedAt: now() },
      input,
      ticket!,
      observation,
      member,
    ),
  );
  if (!result) {
    fail(401, '会话已失效，请重新登录');
  }
  return accountCommandResult(result);
}

export async function ownerOperation(
  url: URL,
  method: string | undefined,
  body: Record<string, unknown> | undefined,
  deps: OwnerDependencies,
  command?: Pick<PersistenceOptions, 'domainCommand' | 'authorize'>,
) {
  const path = url.pathname.slice('/api/member/owner'.length),
    { store } = deps;
  if (path === '/statistics' && method === 'GET') {
    const range = ownerStatisticsRange(url.searchParams, Date.now());
    // 既有响应的 days 是逐日统计数组，会覆盖查询范围的天数；保持键顺序和输出形态。
    return {
      ...range,
      ...summarizeOwnerUsage(await store.readOwnerJobRows(range.since, range.until)),
    };
  }
  // 原回执仍受当前授权约束；命中后不再触发外部额度查询或重新决定账户配置。
  if (command?.domainCommand?.commandId) {
    const prior = await store.update(() => null, { persist: false, ...command });
    if (prior !== null) {
      return prior;
    }
  }
  const account = await store.update((db) => sharedAccount(db), { persist: false });
  if (path === '/summary' && method === 'GET') {
    return { accounts: account ? [safeAccount(account)] : [] };
  }
  // 成员路由只为写操作提供已解析的 JSON 对象，GET 不读取正文。
  const input = body!;
  const targetId = method === 'PATCH' ? /^\/accounts\/([^/]+)$/.exec(path)?.[1] : undefined;
  if (targetId) {
    const patch = ownerAccountPatch(targetId, input);
    let change: OwnerAccountChange;
    if (patch.kind === 'token') {
      change = { kind: 'token', token: patch.token };
    } else {
      change = { kind: 'pause' };
      if (patch.kind === 'enable') {
        let observation: EnableObservation = null;
        // 只查询已选中的共享账户。使用实际提交观察时返回的来源，网络等待不在写事务内。
        if (account?.id === targetId) {
          try {
            const result = await deps.refreshAccountQuotas({ ids: [targetId] });
            const applied = result.accounts?.find((item) => item.id === targetId);
            if (result.ok === 1 && !result.failed && !result.ignored && applied?.token) {
              observation = { id: applied.id, token: applied.token };
            }
          } catch (error) {
            observation = { error };
          }
        }
        change = { kind: 'enable', observation };
      }
    }
    return safeAccount(
      await deps.updateAccount(targetId, change, {
        ...command,
        commandReceipt: (account) => safeAccount(account) as Json,
      }),
    );
  }
  if (!account) {
    fail(409, '尚未确定共享上游账户');
  }
  if (path === '/accounts/quota' && method === 'POST') {
    if (
      Object.keys(input).some((k) => k !== 'ids') ||
      !Array.isArray(input.ids) ||
      input.ids.length !== 1 ||
      input.ids[0] !== account.id
    ) {
      fail(400, '仅可刷新当前共享账户');
    }
    const result = await deps.refreshAccountQuotas(
      { ids: [account.id] },
      {
        ...command,
        commandReceipt: (result) => ({
          checked: result.checked,
          ok: result.ok,
          failed: result.failed,
          ...(result.ignored === undefined ? {} : { ignored: result.ignored }),
        }),
      },
    );
    return {
      checked: result.checked,
      ok: result.ok,
      failed: result.failed,
      ...(result.ignored === undefined ? {} : { ignored: result.ignored }),
    };
  }
  fail(404, '未开放此管理操作');
}

export function maskToken(token = '') {
  if (token.length <= 12) {
    return token ? '******' : '';
  }
  return `${token.slice(0, 6)}...${token.slice(-4)}`;
}

export function accountTierText(tier: unknown, fallback = '会员未查询') {
  if (tier === null || tier === undefined || tier === '') {
    return fallback;
  }
  const value = Number(tier);
  const tierNames: Record<number, string> = {
    0: '无订阅',
    1: 'Tablet 会员',
    2: 'Scroll 会员',
    3: 'Opus 会员',
  };
  if (Number.isFinite(value)) {
    return tierNames[value] || `Tier ${value}`;
  }
  const key = String(tier).trim().toLowerCase();
  return (
    {
      none: '无订阅',
      paper: '无订阅',
      tablet: 'Tablet 会员',
      scroll: 'Scroll 会员',
      opus: 'Opus 会员',
    }[key] || String(tier)
  );
}

export function maskProxyUrl(value: unknown = '') {
  const text = String(value || '').trim();
  if (!text) {
    return '';
  }
  try {
    const url = new URL(text);
    if (url.password) {
      url.password = '******';
    }
    if (url.username) {
      url.username = `${url.username.slice(0, 2)}***`;
    }
    return url.toString();
  } catch {
    return text.replace(/:\/\/([^:@]+):([^@]+)@/, '://$1:******@');
  }
}
