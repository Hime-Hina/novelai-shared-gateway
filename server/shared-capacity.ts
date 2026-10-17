// 每份 Opus 独立控制并发；这里只保存调度状态，不改变成员额度或请求参数。
type Timestamp = string | number | null | undefined;
type CompletionKind = 'ended' | 'success' | 'ordinary' | 'capacity';

interface SharedAccount {
  cooldownUntil?: Timestamp;
  cooldownReason?: string;
  ordinaryStrikeCount?: unknown;
  capacityStrikeCount?: unknown;
  capacityLimitedUntil?: Timestamp;
  capacityConcurrencyLimit?: number;
  capacityLastErrorAt?: string;
}

interface SharedJob {
  id?: string;
  generationAttempt?: { id?: string } | null;
  sharedCooldown?: {
    attemptId?: string;
    kind: CompletionKind;
    at: number;
    until: Timestamp;
  } | null;
}

interface CompletionOptions {
  job?: SharedJob | null;
  kind?: CompletionKind;
  retryAfterUntil?: Timestamp;
}

export const SHARED_ACCOUNT_MAX_CONCURRENCY = 1;
const SHARED_ACCOUNT_GAP_MS = 10_000;
const count = (value: unknown): number => Number.isFinite(Number(value)) ? Math.max(0, Math.floor(Number(value))) : 0;
const timestamp = (value: Timestamp): number => typeof value === 'number' ? value : Date.parse(value || '') || 0;

export function sharedAccountConcurrency(): number {
  // 免费生成会锁住整个上游账户；错开十秒也不能保证上一张已经完成。
  return SHARED_ACCOUNT_MAX_CONCURRENCY;
}

export function sharedAccountReadyDelay(account: SharedAccount | null | undefined, now = Date.now()): number {
  return Math.max(0, timestamp(account?.cooldownUntil) - now);
}

export function cooldownRetryAfter(until: Timestamp, now = Date.now()): number | null {
  const remaining = timestamp(until) - now;
  return Number.isFinite(remaining) && remaining > 0 ? Math.ceil(remaining / 1000) : null;
}

export function recordSharedCompletion(account: SharedAccount | null | undefined, now = Date.now(), { job, kind = 'ended', retryAfterUntil }: CompletionOptions = {}): boolean {
  if (!account) return false;
  const attemptId = job?.generationAttempt?.id || job?.id;
  const previous = job?.sharedCooldown;
  // 首次完整迟到产物可以纠正失败结果；重复回调不能清除另一任务后来积累的失败。
  if (previous?.attemptId === attemptId && previous && (kind !== 'success' || previous.kind === 'success')) return false;
  let delay = SHARED_ACCOUNT_GAP_MS, reason = 'completion_gap';
  if (kind === 'success') {
    account.ordinaryStrikeCount = 0; account.capacityStrikeCount = 0; reason = 'success_gap';
  } else if (kind === 'ordinary') {
    const strikes = Math.min(4, count(account.ordinaryStrikeCount) + 1);
    account.ordinaryStrikeCount = strikes;
    delay = [10000, 20000, 40000, 60000][strikes - 1]; reason = 'ordinary_failure';
  } else if (kind === 'capacity') {
    const legacyStrike = account.capacityStrikeCount == null && timestamp(account.capacityLimitedUntil) > now ? 1 : 0;
    const strikes = Math.min(4, Math.max(legacyStrike, count(account.capacityStrikeCount)) + 1);
    account.capacityStrikeCount = strikes;
    account.capacityConcurrencyLimit = SHARED_ACCOUNT_MAX_CONCURRENCY;
    account.capacityLastErrorAt = new Date(now).toISOString();
    delay = Math.min(120000, 30000 * 2 ** (strikes - 1)); reason = 'capacity_limit';
  }
  let until = now + delay;
  const upstream = timestamp(retryAfterUntil);
  if (Number.isFinite(upstream) && upstream <= 8640000000000000 && upstream > until) { until = upstream; reason = 'upstream_retry_after'; }
  // 已有更长等待保留原原因，所有等待取最大，不叠加或从重试时间重置。
  if (timestamp(account.cooldownUntil) <= until) {
    account.cooldownUntil = new Date(until).toISOString(); account.cooldownReason = reason;
  }
  if (job) job.sharedCooldown = { attemptId, kind, at: now, until: account.cooldownUntil };
  return true;
}

export function recordSharedCapacityError(account: SharedAccount | null | undefined, now = Date.now(), options: CompletionOptions = {}): boolean {
  return recordSharedCompletion(account, now, { ...options, kind: 'capacity' });
}
