const classA = new Set('ListBuckets PutBucket ListObjects PutObject CopyObject CompleteMultipartUpload CreateMultipartUpload LifecycleStorageTierTransition ListMultipartUploads UploadPart UploadPartCopy ListParts PutBucketEncryption PutBucketCors PutBucketLifecycleConfiguration PutBucketStorageClass'.split(' '));
const classB = new Set('HeadBucket HeadObject GetObject UsageSummary ReportUsageSummary GetBucketEncryption GetBucketLocation GetBucketCors GetBucketLifecycleConfiguration'.split(' '));
// Sippy 配置查询也会出现在分析中；Sippy 本身免费，仅其对象操作计费。
const free = new Set(['DeleteObject', 'DeleteBucket', 'AbortMultipartUpload', 'GetBucketSippyConfiguration']);
const refreshMs = 10 * 60_000;

type Snapshot = { classA: number; classB: number; checkedAt: string };
type Cache = { key: string; periodStart: string; nextRefreshAt: number; pending: boolean; failed: boolean; snapshot?: Snapshot };
let cache: Cache | undefined;

function monthBoundary(year: number, month: number, day: number) {
  return Date.UTC(year, month, Math.min(day, new Date(Date.UTC(year, month + 1, 0)).getUTCDate()));
}

function billingPeriod(now: Date, day: number) {
  const year = now.getUTCFullYear(), month = now.getUTCMonth();
  const offset = now.getTime() < monthBoundary(year, month, day) ? -1 : 0;
  return { periodStart: new Date(monthBoundary(year, month + offset, day)).toISOString(),
    periodEnd: new Date(monthBoundary(year, month + offset + 1, day)).toISOString() };
}

async function queryOperations(accountId: string, token: string, start: string, end: string): Promise<Snapshot> {
  const response = await fetch('https://api.cloudflare.com/client/v4/graphql', {
    method: 'POST', redirect: 'error', signal: AbortSignal.timeout(10_000),
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: `query R2Operations($account: string!, $start: Time!, $end: Time!) {
      viewer { accounts(filter: {accountTag: $account}) {
        r2OperationsAdaptiveGroups(limit: 1000, filter: {
          datetime_geq: $start, datetime_lt: $end, storageClass: "Standard",
          actionStatus_in: ["success", "userError"], responseStatusCode_neq: 401
        }) { dimensions { actionType } sum { requests } }
      } }
    }`, variables: { account: accountId, start, end } })
  });
  if (!response.ok) { await response.body?.cancel(); throw new Error('R2 analytics unavailable'); }
  const body = await response.json() as { errors?: unknown[]; data?: { viewer?: { accounts?: { r2OperationsAdaptiveGroups?: { dimensions?: { actionType?: string }; sum?: { requests?: number } }[] }[] } } };
  const accounts = body.data?.viewer?.accounts;
  const rows = accounts?.[0]?.r2OperationsAdaptiveGroups;
  if (body.errors?.length || !Array.isArray(accounts) || accounts.length !== 1 || !Array.isArray(rows) || rows.length >= 1000) throw new Error('Incomplete R2 analytics');
  let a = 0, b = 0;
  for (const row of rows) {
    const action = row?.dimensions?.actionType, count = row?.sum?.requests;
    if (typeof action !== 'string' || !Number.isSafeInteger(count) || count! < 0) throw new Error('Invalid R2 analytics');
    if (classA.has(action)) a += count!;
    else if (classB.has(action)) b += count!;
    else if (!free.has(action)) throw new Error('Unknown R2 operation');
  }
  if (!Number.isSafeInteger(a) || !Number.isSafeInteger(b)) throw new Error('Invalid R2 totals');
  return { classA: a, classB: b, checkedAt: end };
}

function quota(used: number | undefined, limit: number) {
  return { used: used ?? null, limit, remaining: used === undefined ? null : Math.max(0, limit - used) };
}

export function readR2Operations(env: NodeJS.ProcessEnv = process.env) {
  const account = env.R2_ACCOUNT_ID || '', token = env.R2_ANALYTICS_TOKEN || '', day = Number(env.R2_BILLING_DAY);
  const configured = /^[a-f0-9]{32}$/i.test(account) && Boolean(token) && Number.isInteger(day) && day >= 1 && day <= 31;
  const now = new Date(), period = configured ? billingPeriod(now, day) : { periodStart: null, periodEnd: null };
  if (!configured) return { ...period, status: 'unconfigured' as const, checkedAt: null, classA: quota(undefined, 1_000_000), classB: quota(undefined, 10_000_000) };
  const key = `${account}\0${token}\0${day}`;
  if (!cache || cache.key !== key || cache.periodStart !== period.periodStart) {
    cache = { key, periodStart: period.periodStart!, nextRefreshAt: 0, pending: false, failed: false };
  }
  const entry = cache;
  // 管理页只读取缓存；官方接口变慢或不可用时不阻塞网关，也不把缺失统计显示为零。
  if (!entry.pending && now.getTime() >= entry.nextRefreshAt) {
    entry.pending = true;
    void queryOperations(account, token, entry.periodStart, now.toISOString())
      .then(snapshot => { entry.snapshot = snapshot; entry.failed = false; })
      .catch(() => { entry.failed = true; })
      .finally(() => { entry.pending = false; entry.nextRefreshAt = Date.now() + refreshMs; });
  }
  return { ...period, status: entry.snapshot ? (entry.failed ? 'stale' as const : 'ready' as const)
    : (entry.failed ? 'unavailable' as const : 'loading' as const), checkedAt: entry.snapshot?.checkedAt ?? null,
    classA: quota(entry.snapshot?.classA, 1_000_000), classB: quota(entry.snapshot?.classB, 10_000_000) };
}
