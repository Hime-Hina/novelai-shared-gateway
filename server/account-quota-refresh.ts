import { createId, type JsonStore, type StoreDatabase, type StoreItem } from './store.ts';
import { fetchNovelAiAccountQuota } from './providers.ts';
import { withQuotaRequestDeadline } from './quota-timeout.ts';
import { syncFairQuota } from './fair-quota.ts';
import { recordDomainEvent } from './domain-events.ts';
import { isNovelAiAccountBannedError, isNovelAiOutOfTrialImageGenerationError } from './public-errors.ts';
export { isNovelAiAccountBannedError, isNovelAiOutOfTrialImageGenerationError } from './public-errors.ts';

type AccountQuota = Awaited<ReturnType<typeof fetchNovelAiAccountQuota>>;
type QuotaError = Error & { upstreamStatus?: number };
interface AccountMode { fairShareMode?: boolean }
function fail(statusCode: number, message: string): never { throw Object.assign(new Error(message), { statusCode }); }

export function isUnsubscribedAccountTier(tier: unknown): boolean {
  if (tier === null || tier === undefined || tier === '') return false;
  const value = Number(tier);
  return Number.isFinite(value) ? value === 0 : ['none', 'paper', 'free', 'unsubscribed'].includes(String(tier).trim().toLowerCase());
}

export function disableNovelAiAccount(account: StoreItem, options: AccountMode = {}): void {
  account.enabled = false;
  if (options.fairShareMode === false) { account.inFlight = 0; account.cooldownUntil = ''; }
}

export function accountQuotaResult(id: string, quota: AccountQuota, now: string) {
  const noSubscription = isUnsubscribedAccountTier(quota.tier);
  const validTrial = quota.trialStatusKnown === true && quota.trialEligible === true && Number(quota.trialRemainingImages) > 0;
  return { id, ok: true as const, quotaPoints: quota.points, quotaFixed: quota.fixed, quotaPurchased: quota.purchased,
    quotaTier: quota.tier, subscriptionActive: quota.subscriptionActive, subscriptionExpiresAt: quota.subscriptionExpiresAt,
    v5UsagePercent: quota.v5UsagePercent, v5UsageIsNegative: quota.v5UsageIsNegative, v5UsageTimeUntilNextPercent: quota.v5UsageTimeUntilNextPercent,
    quotaCheckedAt: now, quotaError: '', upstreamStatus: Number(quota.tier) === 3 && quota.subscriptionActive === true ? 'active' : 'expired',
    trialRemainingImages: quota.trialRemainingImages ?? null, trialUsedImages: quota.trialUsedImages ?? null,
    trialEligible: quota.trialEligible === true, trialStatusKnown: quota.trialStatusKnown === true,
    disableAccount: noSubscription && !validTrial, disableReason: noSubscription && !validTrial ? '无可用试用额度' : '' };
}

export function accountQuotaErrorResult(id: string, error: unknown, now: string, publicErrorMessage: (message: unknown) => string) {
  const failure = error as QuotaError, message = failure?.message || 'quota query failed.';
  const banned = isNovelAiAccountBannedError(message), outOfTrial = isNovelAiOutOfTrialImageGenerationError(message);
  return { id, ok: false as const, quotaPoints: null, quotaFixed: null, quotaPurchased: null, quotaTier: null,
    v5UsagePercent: null, v5UsageIsNegative: false, v5UsageTimeUntilNextPercent: null, quotaCheckedAt: now,
    quotaError: publicErrorMessage(message), upstreamStatus: failure?.upstreamStatus === 401 ? 'invalid_key' : failure?.upstreamStatus === 403 ? 'access_denied' : 'sync_error',
    disableAccount: banned || outOfTrial, disableReason: banned ? '账号已封禁' : outOfTrial ? '试用次数已用完' : '' };
}

type QuotaResult = ReturnType<typeof accountQuotaResult> | ReturnType<typeof accountQuotaErrorResult>;

export function recordOfficialObservation(db: StoreDatabase, account: StoreItem, now: string): void {
  const fields = ['enabled', 'subscriptionActive', 'subscriptionExpiresAt', 'quotaTier', 'quotaPoints', 'quotaFixed', 'quotaPurchased',
    'v5UsagePercent', 'v5UsageIsNegative', 'v5UsageTimeUntilNextPercent', 'quotaCheckedAt', 'quotaAttemptedAt', 'quotaError', 'upstreamStatus', 'updatedAt'];
  recordDomainEvent(db, { eventType: 'QuotaObserved', streamId: `account:${account.id}`, accountId: account.id, occurredAt: now,
    payload: { kind: 'official', accountId: account.id, observation: Object.fromEntries(fields.filter(key => account[key] !== undefined).map(key => [key, account[key]])), at: now } });
}

export function applyAccountQuotaResult(db: StoreDatabase, account: StoreItem, result: QuotaResult, now: string, options: AccountMode = {}): void {
  if (!result.ok) {
    account.quotaError = result.quotaError; account.upstreamStatus = result.upstreamStatus;
    account.quotaAttemptedAt = now; account.updatedAt = now;
    if (result.disableAccount) disableNovelAiAccount(account, options);
    recordOfficialObservation(db, account, now);
    return;
  }
  Object.assign(account, { trialRemainingImages: result.trialRemainingImages, trialUsedImages: result.trialUsedImages,
    trialEligible: result.trialEligible, trialStatusKnown: result.trialStatusKnown, subscriptionActive: result.subscriptionActive === true,
    subscriptionExpiresAt: result.subscriptionExpiresAt ?? null, quotaPoints: result.quotaPoints, quotaFixed: result.quotaFixed,
    quotaPurchased: result.quotaPurchased, quotaTier: result.quotaTier, v5UsagePercent: result.v5UsagePercent,
    v5UsageIsNegative: result.v5UsageIsNegative, v5UsageTimeUntilNextPercent: result.v5UsageTimeUntilNextPercent,
    quotaCheckedAt: result.quotaCheckedAt, quotaError: result.quotaError, upstreamStatus: result.upstreamStatus, updatedAt: now });
  if (options.fairShareMode === false) account.cooldownUntil = '';
  if (result.disableAccount) disableNovelAiAccount(account, options);
  recordOfficialObservation(db, account, now);
}

export async function resetAccountStats(store: JsonStore, ids: string[], options: AccountMode & { now?: () => number } = {}) {
  const selected = new Set(ids);
  if (!selected.size) fail(400, 'account ids are required.');
  return store.update(db => {
    const accounts = db.accounts.filter(account => selected.has(account.id));
    if (!accounts.length) fail(404, 'no matching accounts found.');
    const at = new Date((options.now || Date.now)()).toISOString();
    for (const account of accounts) {
      if (options.fairShareMode === false) { account.inFlight = 0; account.cooldownUntil = ''; account.lastUsedAt = ''; }
      account.total = 0; account.failures = 0; account.updatedAt = at;
    }
    db.ledger.unshift({ id: createId('log'), type: 'reset-account-stats', amount: accounts.length, at,
      note: `Reset monitoring stats for ${accounts.length} NovelAI account(s)` });
    return { reset: accounts.length };
  }, { collections: ['accounts', 'ledger'] });
}

interface AccountQuotaRefreshOptions<T> extends AccountMode {
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
interface RefreshResult<T> { checked: number; ok: number; failed: number; accounts: T[] }

export function createAccountQuotaRefresh<T>(options: AccountQuotaRefreshOptions<T>) {
  const { store } = options, clock = options.now || Date.now, shared = options.fairShareMode !== false;
  const logger = options.logger || console, fetchQuota = options.fetchQuota || fetchNovelAiAccountQuota;
  let pending: Promise<RefreshResult<T>> | null = null;
  let timer: ReturnType<typeof setTimeout> | null = null, started = false, stopped = false;

  const fetch = (token: string | undefined, proxyUrl: unknown = '') => withQuotaRequestDeadline(
    signal => fetchQuota(token!, options.env || process.env, { signal, proxyUrl }), options.requestTimeoutMs ?? 15_000);

  async function refresh(ids: string[] = []): Promise<RefreshResult<T>> {
    const selected = new Set(ids);
    const snapshot = await store.readCollections(['accounts']);
    const targets = (selected.size ? snapshot.accounts.filter(account => selected.has(account.id)) : snapshot.accounts)
      .map(account => ({ id: account.id, token: account.token, proxyUrl: account.proxyUrl || '' }));
    if (!targets.length) fail(selected.size ? 404 : 400, selected.size ? 'no matching accounts found.' : 'no accounts found.');
    const at = new Date(clock()).toISOString(), results = new Array<QuotaResult>(targets.length);
    let next = 0;
    await Promise.all(Array.from({ length: Math.min(5, targets.length) }, async () => {
      while (next < targets.length) {
        const index = next++, target = targets[index]!;
        try { results[index] = accountQuotaResult(target.id, await fetch(target.token, target.proxyUrl), at); }
        catch (error) { results[index] = accountQuotaErrorResult(target.id, error, at, options.publicErrorMessage); }
      }
    }));
    const resultMap = new Map(results.map(result => [result.id, result]));
    const tokens = new Map(targets.map(target => [target.id, target.token]));
    const accounts = await store.update(db => {
      for (const account of db.accounts) {
        const result = resultMap.get(account.id);
        if (!result || account.token !== tokens.get(account.id) || Date.parse(String(account.quotaCheckedAt || '')) > Date.parse(result.quotaCheckedAt)) continue;
        applyAccountQuotaResult(db, account, result, at, options);
      }
      if (shared) syncFairQuota(db, clock());
      return db.accounts.filter(account => resultMap.has(account.id));
    }, { collections: ['accounts', 'settings', 'ledger'], includeSettings: true });
    return { checked: results.length, ok: results.filter(result => result.ok).length, failed: results.filter(result => !result.ok).length,
      accounts: accounts.map(options.publicAccount) };
  }

  function refreshEnabled(): Promise<RefreshResult<T>> {
    if (pending) return pending;
    pending = (async () => {
      try {
        const db = await store.readCollections(['accounts']), ids = db.accounts.filter(account => account.enabled !== false).map(account => account.id);
        return ids.length ? await refresh(ids) : { checked: 0, ok: 0, failed: 0, accounts: [] };
      } finally { pending = null; }
    })();
    return pending;
  }

  function start(): void {
    if (started || stopped) return;
    started = true;
    const run = async () => {
      if (stopped) return;
      try {
        const result = await refreshEnabled();
        if (result.checked) logger.log(`[runtime] account quota refresh: checked=${result.checked} ok=${result.ok} failed=${result.failed}`);
      } catch (error) { logger.error('[runtime] account quota refresh failed:', error); }
      finally {
        if (!stopped) { timer = setTimeout(run, options.refreshIntervalMs ?? (shared ? 60_000 : 300_000)); timer.unref?.(); }
      }
    };
    void run();
  }

  function stop(): void {
    stopped = true;
    if (timer) clearTimeout(timer);
    timer = null;
  }
  return { refresh, refreshEnabled, fetch, start, stop, get pending() { return pending; } };
}
