import { sharedAccountConcurrency } from './shared-capacity.ts';
import {
  isQuotaSnapshotFresh,
  isUsableOpus,
  quotaAvailability,
  quotaNumber as number,
  quotaTimestamp as timestamp,
} from './eligibility.ts';

export interface SharedStatus {
  status: string;
  subscriptionActive: boolean | null;
  expiresAt: string | null;
  officialCheckedAt: string | null;
  stale: boolean;
  v5Percent: number | null;
  v5IsNegative: boolean | null;
  refillSecondsPerPercent: number | null;
  anlasFixed: number | null;
  anlasPurchased: number | null;
  running: number;
  queued: number;
  pending: number;
  concurrencyLimit: number;
  cooldownUntil: string | null;
  cooldownReason: string | null;
  quotaSyncError: boolean;
  recentFailures: number | null;
}

export type SharedJobCounts = Pick<
  SharedStatus,
  'running' | 'queued' | 'pending' | 'recentFailures'
>;

type Rows<T> = T[] | Record<string, T> | null | undefined;
interface StatusUser {
  id: string;
  token?: string | null;
}
interface StatusJob {
  userToken?: string | null;
  status?: string;
  completedAt?: unknown;
  failedAt?: unknown;
  updatedAt?: unknown;
}
export interface StatusAccount {
  id: string;
  enabled?: unknown;
  subscriptionActive?: unknown;
  subscriptionExpiresAt?: unknown;
  quotaTier?: unknown;
  quotaCheckedAt?: unknown;
  quotaError?: unknown;
  v5UsagePercent?: unknown;
  v5UsageIsNegative?: unknown;
  v5UsageTimeUntilNextPercent?: unknown;
  quotaFixed?: unknown;
  quotaPurchased?: unknown;
  cooldownUntil?: unknown;
  cooldownReason?: unknown;
}
export interface SharedStatusSource {
  accounts: readonly StatusAccount[];
  boundAccountId: string | null;
  baseline: NonNullable<Parameters<typeof quotaAvailability>[2]>;
  counts: SharedJobCounts;
}
interface StatusDatabase {
  settings?: {
    fairQuota?: {
      members?: string[] | null;
      boundAccountId?: string | null;
      snapshot?: { at: unknown } | null;
      reservations?: Record<string, { status?: string }>;
      lastSettlementAt?: number;
    } | null;
  } | null;
  users?: Rows<StatusUser>;
  accounts?: Rows<StatusAccount>;
  jobs?: Rows<StatusJob>;
}

// 共享状态仅返回白名单汇总；不改变账本、不调用上游，也不返回个人账户资料。
const rows = <T>(value: Rows<T>): T[] =>
  Array.isArray(value) ? value : Object.values(value || {});
const iso = (value: number | null): string | null =>
  value === null ? null : new Date(value).toISOString();
const bool = (value: unknown): boolean | null => (typeof value === 'boolean' ? value : null);
const cooldownReasons = new Set([
  'success_gap',
  'ordinary_failure',
  'capacity_limit',
  'upstream_retry_after',
  'completion_gap',
]);

export function sharedAccountStatus(
  db: StatusDatabase | null | undefined,
  now = Date.now(),
): SharedStatus {
  const state = db?.settings?.fairQuota;
  const members = new Set(state?.members || []);
  const tokens = new Set(
    rows(db?.users)
      .filter((u) => members.has(u.id) && typeof u.token === 'string' && u.token)
      .map((u) => u.token),
  );
  const jobs = rows(db?.jobs).filter((j) => tokens.has(j.userToken));
  return publicSharedAccountStatus(
    {
      accounts: rows(db?.accounts),
      boundAccountId: state?.boundAccountId ?? null,
      baseline: {
        at: state?.snapshot?.at,
        held: Object.values(state?.reservations || {}).some(
          (charge) => charge.status === 'reserved',
        ),
        lastSettlementAt: state?.lastSettlementAt,
      },
      counts: {
        running: jobs.filter((j) => j.status === 'running').length,
        queued: jobs.filter((j) => j.status === 'queued').length,
        pending: jobs.filter((j) => j.status === 'pending').length,
        recentFailures: jobs.filter((j) => {
          const at = timestamp(j.completedAt ?? j.failedAt ?? j.updatedAt);
          return j.status === 'failed' && at !== null && at <= now && at >= now - 3600000;
        }).length,
      },
    },
    now,
  );
}

// 原资格、优先级和白名单只在这一展示 owner 组装；关系读取仅提供真实安全来源与计数。
export function publicSharedAccountStatus(
  { accounts, boundAccountId, baseline, counts }: SharedStatusSource,
  now = Date.now(),
): SharedStatus {
  const base: SharedStatus = {
    status: 'pending_opus',
    subscriptionActive: null,
    expiresAt: null,
    officialCheckedAt: null,
    stale: true,
    v5Percent: null,
    v5IsNegative: null,
    refillSecondsPerPercent: null,
    anlasFixed: null,
    anlasPurchased: null,
    running: counts.running,
    queued: counts.queued,
    pending: counts.pending,
    concurrencyLimit: sharedAccountConcurrency(),
    cooldownUntil: null,
    cooldownReason: null,
    quotaSyncError: false,
    recentFailures: counts.recentFailures,
  };
  // 未绑定时不挑选其他上游账户，防止把试用池资料带入共享状态。
  if (!boundAccountId) {
    return {
      ...base,
      status:
        accounts.filter((account) => isUsableOpus(account, now)).length > 1
          ? 'ambiguous_opus'
          : 'pending_opus',
    };
  }
  const account = accounts.find((a) => a.id === boundAccountId);
  if (!account) {
    return { ...base, status: 'inactive_opus' };
  }
  const checked = timestamp(account.quotaCheckedAt),
    expires = timestamp(account.subscriptionExpiresAt),
    cooldown = timestamp(account.cooldownUntil);
  const availability = quotaAvailability(account, now, baseline);
  const stale = !isQuotaSnapshotFresh(account.quotaCheckedAt, now) || availability === 'stale';
  const quotaSyncError = Boolean(account.quotaError);
  const v5 = number(account.v5UsagePercent),
    fixed = number(account.quotaFixed),
    purchased = number(account.quotaPurchased);
  const cooling = cooldown !== null && cooldown > now;
  const status = availability === 'active' && cooling ? 'cooldown' : availability;
  const concurrencyLimit = sharedAccountConcurrency();
  return {
    ...base,
    status,
    concurrencyLimit,
    subscriptionActive: bool(account.subscriptionActive),
    expiresAt: iso(expires),
    officialCheckedAt: iso(checked),
    stale,
    v5Percent: v5,
    v5IsNegative: bool(account.v5UsageIsNegative),
    refillSecondsPerPercent: number(account.v5UsageTimeUntilNextPercent),
    anlasFixed: fixed,
    anlasPurchased: purchased,
    cooldownUntil: cooling ? iso(cooldown) : null,
    cooldownReason:
      cooling &&
      typeof account.cooldownReason === 'string' &&
      cooldownReasons.has(account.cooldownReason)
        ? account.cooldownReason
        : null,
    quotaSyncError,
  };
}
