export const QUOTA_SNAPSHOT_MAX_AGE_MS = 10 * 60 * 1000;
export const QUOTA_CLOCK_SKEW_MS = 60 * 1000;

interface OpusSubscription {
  enabled?: unknown; quotaTier?: unknown; subscriptionActive?: unknown;
  subscriptionExpiresAt?: unknown; quotaError?: unknown;
}

export function quotaTimestamp(value: unknown): number | null {
  if (value === null || value === undefined || value === '' || typeof value === 'boolean') return null;
  const numeric = Number(value);
  const result = Number.isFinite(numeric) ? (numeric < 1e11 ? numeric * 1000 : numeric) : Date.parse(String(value));
  return Number.isFinite(result) && result >= 0 && result <= 8640000000000000 ? result : null;
}

export function hasActiveOpusSubscription(account: OpusSubscription | null | undefined, now: number): boolean {
  if (account?.enabled !== true || account.subscriptionActive !== true || Number(account.quotaTier) !== 3) return false;
  if (!account.subscriptionExpiresAt) return true;
  const expires = quotaTimestamp(account.subscriptionExpiresAt);
  return expires !== null && expires > now;
}

export function isUsableOpus(account: OpusSubscription | null | undefined, now: number): boolean {
  return hasActiveOpusSubscription(account, now) && !account?.quotaError;
}

export function isQuotaSnapshotFresh(checkedAt: unknown, now: number): boolean {
  const checked = quotaTimestamp(checkedAt);
  return checked !== null && checked <= now + QUOTA_CLOCK_SKEW_MS && now - checked <= QUOTA_SNAPSHOT_MAX_AGE_MS;
}
