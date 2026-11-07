import type {
  JsonStore,
  StoreDatabase,
  StoreItem,
  PersistenceOptions,
} from '../storage/sqlite-store.ts';
import { fetchNovelAiAccountQuota } from '../protocols/novelai-client.ts';
import { withQuotaRequestDeadline } from './refresh-timeout.ts';
import { syncFairQuota } from './fair-share.ts';
import { domainDigest, recordDomainEvent } from '../events/domain-events.ts';
import {
  isNovelAiAccountBannedError,
  isNovelAiOutOfTrialImageGenerationError,
} from '../http/public-errors.ts';
export {
  isNovelAiAccountBannedError,
  isNovelAiOutOfTrialImageGenerationError,
} from '../http/public-errors.ts';

type AccountQuota = Awaited<ReturnType<typeof fetchNovelAiAccountQuota>>;
type QuotaError = Error & { upstreamStatus?: number; code?: string; cause?: { code?: string } };
function fail(statusCode: number, message: string): never {
  throw Object.assign(new Error(message), { statusCode });
}

function quotaFailure(error: unknown) {
  const value = error as QuotaError;
  const httpStatus = [400, 401, 402, 403, 404, 408, 409, 422, 429, 500, 502, 503, 504].includes(
    value?.upstreamStatus!,
  )
    ? value.upstreamStatus!
    : null;
  const candidate = value?.code || value?.cause?.code;
  const errorCode = [
    'QUOTA_REQUEST_TIMEOUT',
    'ETIMEDOUT',
    'ECONNRESET',
    'ECONNREFUSED',
    'ENOTFOUND',
    'EAI_AGAIN',
    'EPIPE',
    'UND_ERR_CONNECT_TIMEOUT',
    'UND_ERR_HEADERS_TIMEOUT',
    'UND_ERR_BODY_TIMEOUT',
    'UND_ERR_SOCKET',
  ].includes(candidate!)
    ? candidate!
    : null;
  let category = 'query_error';
  if (isNovelAiAccountBannedError(value?.message)) {
    category = 'banned';
  } else if (
    isNovelAiOutOfTrialImageGenerationError(value?.message) ||
    value?.message === '试用次数已用完'
  ) {
    category = 'out_of_trial';
  } else if (httpStatus === 401) {
    category = 'invalid_key';
  } else if (httpStatus === 403) {
    category = 'access_denied';
  } else if (
    errorCode === 'QUOTA_REQUEST_TIMEOUT' ||
    errorCode?.includes('TIMEOUT') ||
    value?.name === 'AbortError' ||
    value?.message === '订阅状态查询超时，将在下一轮自动重试'
  ) {
    category = 'timeout';
  } else if (errorCode) {
    category = 'network_error';
  } else if (httpStatus) {
    category = 'http_error';
  }
  let message = '官方额度同步失败，请稍后重试';
  if (category === 'banned') {
    message = '账号已封禁';
  } else if (category === 'out_of_trial') {
    message = '试用次数已用完';
  } else if (category === 'invalid_key') {
    message = '共享 Opus Key 已失效，请联系管理员更新凭据';
  } else if (category === 'access_denied') {
    message = '上游拒绝访问，请联系管理员重新验证 Opus';
  } else if (category === 'timeout') {
    message = '订阅状态查询超时，将在下一轮自动重试';
  }
  return { httpStatus, errorCode, category, message };
}

export function isUnsubscribedAccountTier(tier: unknown): boolean {
  if (tier === null || tier === undefined || tier === '') {
    return false;
  }
  const value = Number(tier);
  return Number.isFinite(value)
    ? value === 0
    : ['none', 'paper', 'free', 'unsubscribed'].includes(String(tier).trim().toLowerCase());
}

export function disableNovelAiAccount(account: StoreItem): void {
  account.enabled = false;
}

export function accountQuotaResult(id: string, quota: AccountQuota, now: string) {
  const noSubscription = isUnsubscribedAccountTier(quota.tier);
  const validTrial =
    quota.trialStatusKnown === true &&
    quota.trialEligible === true &&
    Number(quota.trialRemainingImages) > 0;
  return {
    id,
    ok: true as const,
    quotaPoints: quota.points,
    quotaFixed: quota.fixed,
    quotaPurchased: quota.purchased,
    quotaTier: quota.tier,
    subscriptionActive: quota.subscriptionActive,
    subscriptionExpiresAt: quota.subscriptionExpiresAt,
    v5UsagePercent: quota.v5UsagePercent,
    v5UsageIsNegative: quota.v5UsageIsNegative,
    v5UsageTimeUntilNextPercent: quota.v5UsageTimeUntilNextPercent,
    quotaCheckedAt: now,
    quotaError: '',
    upstreamStatus:
      Number(quota.tier) === 3 && quota.subscriptionActive === true ? 'active' : 'expired',
    trialRemainingImages: quota.trialRemainingImages ?? null,
    trialUsedImages: quota.trialUsedImages ?? null,
    trialEligible: quota.trialEligible === true,
    trialStatusKnown: quota.trialStatusKnown === true,
    disableAccount: noSubscription && !validTrial,
    disableReason: noSubscription && !validTrial ? '无可用试用额度' : '',
  };
}

export function accountQuotaErrorResult(id: string, error: unknown, now: string) {
  const failure = quotaFailure(error);
  const banned = failure.category === 'banned',
    outOfTrial = failure.category === 'out_of_trial';
  let upstreamStatus = 'sync_error';
  if (failure.httpStatus === 401) {
    upstreamStatus = 'invalid_key';
  } else if (failure.httpStatus === 403) {
    upstreamStatus = 'access_denied';
  }
  let disableReason = '';
  if (banned) {
    disableReason = '账号已封禁';
  } else if (outOfTrial) {
    disableReason = '试用次数已用完';
  }
  return {
    id,
    ok: false as const,
    quotaPoints: null,
    quotaFixed: null,
    quotaPurchased: null,
    quotaTier: null,
    v5UsagePercent: null,
    v5UsageIsNegative: false,
    v5UsageTimeUntilNextPercent: null,
    quotaCheckedAt: now,
    quotaError: failure.message,
    upstreamStatus,
    disableAccount: banned || outOfTrial,
    disableReason,
  };
}

type QuotaResult =
  ReturnType<typeof accountQuotaResult> | ReturnType<typeof accountQuotaErrorResult>;

export function recordOfficialObservation(
  db: StoreDatabase,
  account: StoreItem,
  now: string,
): void {
  const fields = [
    'enabled',
    'subscriptionActive',
    'subscriptionExpiresAt',
    'quotaTier',
    'quotaPoints',
    'quotaFixed',
    'quotaPurchased',
    'v5UsagePercent',
    'v5UsageIsNegative',
    'v5UsageTimeUntilNextPercent',
    'quotaCheckedAt',
    'quotaAttemptedAt',
    'quotaError',
    'upstreamStatus',
    'updatedAt',
  ];
  recordDomainEvent(db, {
    eventType: 'QuotaObserved',
    streamId: `account:${account.id}`,
    accountId: account.id,
    occurredAt: now,
    payload: {
      kind: 'official',
      accountId: account.id,
      observation: Object.fromEntries(
        fields.filter((key) => account[key] !== undefined).map((key) => [key, account[key]]),
      ),
      at: now,
    },
  });
}

export function applyAccountQuotaResult(
  db: StoreDatabase,
  account: StoreItem,
  result: QuotaResult,
  now: string,
): void {
  if (!result.ok) {
    account.quotaError = result.quotaError;
    account.upstreamStatus = result.upstreamStatus;
    account.quotaAttemptedAt = now;
    account.updatedAt = now;
    if (result.disableAccount) {
      disableNovelAiAccount(account);
    }
    recordOfficialObservation(db, account, now);
    return;
  }
  Object.assign(account, {
    trialRemainingImages: result.trialRemainingImages,
    trialUsedImages: result.trialUsedImages,
    trialEligible: result.trialEligible,
    trialStatusKnown: result.trialStatusKnown,
    subscriptionActive: result.subscriptionActive === true,
    subscriptionExpiresAt: result.subscriptionExpiresAt ?? null,
    quotaPoints: result.quotaPoints,
    quotaFixed: result.quotaFixed,
    quotaPurchased: result.quotaPurchased,
    quotaTier: result.quotaTier,
    v5UsagePercent: result.v5UsagePercent,
    v5UsageIsNegative: result.v5UsageIsNegative,
    v5UsageTimeUntilNextPercent: result.v5UsageTimeUntilNextPercent,
    quotaCheckedAt: result.quotaCheckedAt,
    quotaAttemptedAt: now,
    quotaError: result.quotaError,
    upstreamStatus: result.upstreamStatus,
    updatedAt: now,
  });
  if (result.disableAccount) {
    disableNovelAiAccount(account);
  }
  recordOfficialObservation(db, account, now);
}

interface AccountQuotaRefreshOptions<T> {
  store: JsonStore;
  fetchQuota?: typeof fetchNovelAiAccountQuota;
  env?: NodeJS.ProcessEnv;
  now?: () => number;
  requestTimeoutMs?: number;
  refreshIntervalMs?: number;
  publicAccount(account: StoreItem): T;
  publicErrorMessage(message: unknown): string;
  logger?: Pick<Console, 'log' | 'error'>;
}
interface RefreshResult<T> {
  checked: number;
  ok: number;
  failed: number;
  ignored: number;
  accounts: T[];
}

export function createAccountQuotaRefresh<T>(options: AccountQuotaRefreshOptions<T>) {
  const { store } = options,
    clock = options.now || Date.now;
  const logger = options.logger || console,
    fetchQuota = options.fetchQuota || fetchNovelAiAccountQuota;
  let pending: Promise<RefreshResult<T>> | null = null;
  let timer: ReturnType<typeof setTimeout> | null = null,
    started = false,
    stopped = false;
  const latestTickets = new Map<string, number>();

  const fetch = async (
    token: string | undefined,
    proxyUrl: unknown = '',
    accountId: string | null = null,
  ) => {
    const startedAt = performance.now();
    try {
      const quota = await withQuotaRequestDeadline(
        (signal) => fetchQuota(token!, options.env || process.env, { signal, proxyUrl }),
        options.requestTimeoutMs ?? 15_000,
      );
      logger.log(
        '[quota-query]',
        JSON.stringify({
          event: 'succeeded',
          at: new Date(clock()).toISOString(),
          accountId,
          durationMs: Math.round(performance.now() - startedAt),
          httpStatus: null,
          errorCode: null,
          category: 'success',
        }),
      );
      return quota;
    } catch (error) {
      const failure = quotaFailure(error);
      logger.log(
        '[quota-query]',
        JSON.stringify({
          event: 'failed',
          at: new Date(clock()).toISOString(),
          accountId,
          durationMs: Math.round(performance.now() - startedAt),
          httpStatus: failure.httpStatus,
          errorCode: failure.errorCode,
          category: failure.category,
        }),
      );
      throw Object.assign(new Error(failure.message), {
        upstreamStatus: failure.httpStatus,
        code: failure.errorCode,
      });
    }
  };

  // 同一账户和凭据按实际发起顺序领票。网络等待在事务外，写入前核对最新票号与当前凭据。
  async function refresh(
    ids: string[] = [],
    command?: Pick<
      PersistenceOptions<RefreshResult<T>>,
      'domainCommand' | 'authorize' | 'commandReceipt'
    >,
  ): Promise<RefreshResult<T>> {
    const selected = new Set(ids);
    const snapshot = await store.readCollections(['accounts']);
    const targets = (
      selected.size
        ? snapshot.accounts.filter((account) => selected.has(account.id))
        : snapshot.accounts
    ).map((account) => ({
      id: account.id,
      token: account.token,
      proxyUrl: account.proxyUrl || '',
    }));
    if (!targets.length) {
      fail(
        selected.size ? 404 : 400,
        selected.size ? 'no matching accounts found.' : 'no accounts found.',
      );
    }
    const attempts = new Map<
      string,
      (typeof targets)[number] & {
        identity: string;
        ticket: number;
        startedAt: string;
        result: QuotaResult;
      }
    >();
    let next = 0;
    await Promise.all(
      Array.from({ length: Math.min(5, targets.length) }, async () => {
        while (next < targets.length) {
          const target = targets[next++]!;
          const identity = domainDigest([target.id, target.token ?? null]);
          const attempt = {
            ...target,
            identity,
            ticket: (latestTickets.get(identity) || 0) + 1,
            startedAt: new Date(clock()).toISOString(),
          };
          let result: QuotaResult;
          latestTickets.set(attempt.identity, attempt.ticket);
          try {
            const quota = await fetch(attempt.token, attempt.proxyUrl, attempt.id);
            result = accountQuotaResult(attempt.id, quota, attempt.startedAt);
          } catch (error) {
            result = accountQuotaErrorResult(attempt.id, error, attempt.startedAt);
          }
          attempts.set(attempt.id, { ...attempt, result });
        }
      }),
    );
    return await store.update(
      (db) => {
        const applied: QuotaResult[] = [];
        for (const account of db.accounts) {
          const attempt = attempts.get(account.id);
          if (
            !attempt ||
            account.token !== attempt.token ||
            latestTickets.get(attempt.identity) !== attempt.ticket
          ) {
            continue;
          }
          applyAccountQuotaResult(db, account, attempt.result, attempt.startedAt);
          applied.push(attempt.result);
        }
        if (applied.length) {
          syncFairQuota(db, clock());
        }
        return {
          checked: targets.length,
          ok: applied.filter((result) => result.ok).length,
          failed: applied.filter((result) => !result.ok).length,
          ignored: targets.length - applied.length,
          accounts: db.accounts
            .filter((account) => attempts.has(account.id))
            .map(options.publicAccount),
        };
      },
      { collections: ['accounts', 'settings', 'ledger'], includeSettings: true, ...command },
    );
  }

  function refreshEnabled(): Promise<RefreshResult<T>> {
    if (pending) {
      return pending;
    }
    pending = (async () => {
      try {
        const db = await store.readCollections(['accounts']),
          ids = db.accounts
            .filter((account) => account.enabled !== false)
            .map((account) => account.id);
        return ids.length
          ? await refresh(ids)
          : { checked: 0, ok: 0, failed: 0, ignored: 0, accounts: [] };
      } finally {
        pending = null;
      }
    })();
    return pending;
  }

  function start(): void {
    if (started || stopped) {
      return;
    }
    started = true;
    const run = async () => {
      if (stopped) {
        return;
      }
      try {
        const result = await refreshEnabled();
        if (result.checked) {
          logger.log(
            `[runtime] account quota refresh: checked=${result.checked} ok=${result.ok} failed=${result.failed}`,
          );
        }
      } catch {
        logger.error('[runtime] account quota refresh failed');
      } finally {
        if (!stopped) {
          timer = setTimeout(run, options.refreshIntervalMs ?? 60_000);
          timer.unref?.();
        }
      }
    };
    void run();
  }

  function stop(): void {
    stopped = true;
    if (timer) {
      clearTimeout(timer);
    }
    timer = null;
  }
  return {
    refresh,
    refreshEnabled,
    fetch,
    start,
    stop,
    get pending() {
      return pending;
    },
  };
}
