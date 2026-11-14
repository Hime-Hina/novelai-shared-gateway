import {
  estimateFairQuota,
  fairQuotaSummary,
  type FairQuotaDatabase,
  type FairAccount,
} from './fair-share.ts';
import { hasActiveOpusSubscription, quotaTimestamp } from './eligibility.ts';

interface SubscriptionAccount extends Pick<
  FairAccount,
  'id' | 'subscriptionActive' | 'subscriptionExpiresAt' | 'quotaTier'
> {
  enabled?: boolean;
}

interface SubscriptionDatabase {
  accounts?: SubscriptionAccount[] | Record<string, SubscriptionAccount>;
  ledger?: unknown[];
  settings?: FairQuotaDatabase['settings'];
}

type SubscriptionQuota = Pick<
  NonNullable<ReturnType<typeof fairQuotaSummary>>,
  | 'member'
  | 'boundAccountId'
  | 'status'
  | 'v5Available'
  | 'v5RefillLimited'
  | 'v5RefillPerDay'
  | 'anlasFixedAvailable'
  | 'anlasPurchasedAvailable'
>;

const rows = (value: SubscriptionDatabase['accounts']): SubscriptionAccount[] =>
  Array.isArray(value) ? value : Object.values(value || {});
const nonnegative = (value: unknown): number =>
  typeof value === 'number' && Number.isFinite(value) ? Math.max(0, value) : 0;
const wholeBalance = (value: unknown): number => Math.floor(nonnegative(value) + 1e-9);

export function assertLauncherSubscriptionMember(
  user: { id: string; enabled?: boolean } | null | undefined,
): asserts user is { id: string; enabled?: boolean } {
  if (!user || user.enabled === false) {
    throw Object.assign(new Error('成员未启用'), { statusCode: 403 });
  }
}

// 兼容启动器的官方字段名，内容仅表示调用者本地配额，不是共享上游的总余额。
// 两种存储共用展示规则；读取不能改变真实余额、审计或配置，也不返回上游标识。
export function launcherSubscriptionFromQuota(
  quota: SubscriptionQuota | null,
  subscriptionAccount: SubscriptionAccount | undefined,
  now = Date.now(),
) {
  if (!quota?.member) {
    throw Object.assign(new Error('该用户未加入共享分组账本'), { statusCode: 403 });
  }
  const account =
    subscriptionAccount?.id === quota.boundAccountId ? subscriptionAccount : undefined;
  const expires = Math.floor((quotaTimestamp(account?.subscriptionExpiresAt) || 0) / 1000);
  // 官方兼容 DTO 必须携带可用的 expiresAt；缺失到期观测时不伪造订阅期限。
  const active =
    quota.status === 'active' &&
    hasActiveOpusSubscription(account, now) &&
    expires > Math.floor(now / 1000);
  const percent = nonnegative(quota.v5Available);
  const recovery =
    active && quota.v5RefillLimited !== true && quota.v5RefillPerDay > 0
      ? Math.ceil(86400 / quota.v5RefillPerDay)
      : 0;
  return {
    tier: active ? 3 : 0,
    active,
    expiresAt: active ? expires : 0,
    trainingStepsLeft: {
      fixedTrainingStepsLeft: wholeBalance(quota.anlasFixedAvailable),
      purchasedTrainingSteps: wholeBalance(quota.anlasPurchasedAvailable),
    },
    perks: {
      imageGeneration: active,
      unlimitedImageGeneration: active,
      unlimitedImageGenerationLimits: active ? [{ resolution: 1048576, maxPrompts: 1 }] : [],
    },
    usage: {
      percent,
      isNegative: percent <= 0,
      timeUntilNextPercent: Number.isSafeInteger(recovery) ? recovery : 0,
    },
  };
}

export function launcherSubscription(
  db: SubscriptionDatabase,
  user: { id: string; enabled?: boolean } | null | undefined,
  now = Date.now(),
) {
  assertLauncherSubscriptionMember(user);
  const draft = estimateFairQuota({ ...db, settings: db.settings || {} }, now),
    quota = fairQuotaSummary(draft, user, now);
  return launcherSubscriptionFromQuota(
    quota,
    quota?.member ? rows(db.accounts).find((a) => a.id === quota.boundAccountId) : undefined,
    now,
  );
}
