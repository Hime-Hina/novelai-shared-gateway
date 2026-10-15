// 每份 Opus 独立控制并发；这里只保存调度状态，不改变成员额度或请求参数。
export const SHARED_ACCOUNT_MAX_CONCURRENCY = 1;
const count = value => Number.isFinite(Number(value)) ? Math.max(0, Math.floor(Number(value))) : 0;
const timestamp = value => typeof value === 'number' ? value : Date.parse(value || '') || 0;

export function sharedAccountConcurrency() {
  // 免费生成会锁住整个上游账户；错开十秒也不能保证上一张已经完成。
  return SHARED_ACCOUNT_MAX_CONCURRENCY;
}

export function sharedAccountReadyDelay(account, now = Date.now()) {
  return Math.max(0, timestamp(account?.cooldownUntil) - now);
}

export function recordSharedCapacityError(account, now = Date.now()) {
  if (!account) return;
  const legacyStrike = timestamp(account.capacityLimitedUntil) > now ? 1 : 0;
  account.capacityStrikeCount = Math.min(4, Math.max(legacyStrike, count(account.capacityStrikeCount)) + 1);
  account.capacityConcurrencyLimit = SHARED_ACCOUNT_MAX_CONCURRENCY;
  account.capacityLastErrorAt = new Date(now).toISOString();
  const cooldownMs = Math.min(120_000, 30_000 * 2 ** (account.capacityStrikeCount - 1));
  account.cooldownUntil = new Date(Math.max(timestamp(account.cooldownUntil), now + cooldownMs)).toISOString();
}
