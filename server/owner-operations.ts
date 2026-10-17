interface OwnerJobRow {
  id: string; memberId: string; username: string; model?: string | null; status: string; createdAt: string;
  startedAt?: string | null; upstreamStartedAt?: string | null; upstreamCompletedAt?: string | null;
  chargeStatus?: string | null; resource?: string | null;
  amount?: number | string | null; fixed?: number | string | null; purchased?: number | string | null;
  errorStatus?: number | null;
}
type UsageAccumulator = ReturnType<typeof empty>;
type GroupAccumulator = UsageAccumulator & { id: string; label: string };
type JobStatus = 'done' | 'failed' | 'queued' | 'pending' | 'running';
interface OwnerAccount {
  id: string; enabled?: boolean; inFlight?: number | string;
  quotaTier?: number | string | null; quotaFixed?: number | null; quotaPurchased?: number | null;
  v5UsagePercent?: number | null; quotaCheckedAt?: string | null; quotaError?: unknown;
  subscriptionActive?: boolean; subscriptionExpiresAt?: string | number | boolean | null;
}
interface OwnerDatabase { accounts: OwnerAccount[]; settings: { fairQuota?: { boundAccountId?: string | null } } }
interface OwnerDependencies {
  store: {
    readOwnerJobRows(since: string, until: string): Promise<OwnerJobRow[]>;
    update<T>(query: (db: OwnerDatabase) => T, options: { persist: false }): Promise<T>;
  };
  refreshAccountQuotas(options: { ids: string[] }): Promise<{ checked: number; ok: number; failed: number }>;
  updateAccount(id: string, change: { token?: string; enabled: boolean }): Promise<OwnerAccount>;
}

const fail: (statusCode: number, message: string) => never = (statusCode, message) => { throw Object.assign(new Error(message), { statusCode }); };
const dayMs = 86400000, offsetMs = 8 * 3600000;
const day = (value: string): string => new Date(Date.parse(value) + offsetMs).toISOString().slice(0, 10);
const seconds = (start: string | null | undefined, end: string | null | undefined): number | null => {
  // 保持历史缺失日期经 Date.parse 得到 NaN、最终返回 null 的行为。
  const value = (Date.parse(end as string) - Date.parse(start as string)) / 1000;
  return Number.isFinite(value) && value >= 0 ? value : null;
};
const empty = () => ({ total:0, done:0, failed:0, queued:0, pending:0, running:0, refunded:0, free:0, v5:0, fixed:0, purchased:0, waitSeconds:0, waitCount:0, generationSeconds:0, generationCount:0 });
function add(total: UsageAccumulator, job: OwnerJobRow): void {
  total.total++;
  if (['done','failed','queued','pending','running'].includes(job.status)) total[job.status as JobStatus]++;
  if (job.chargeStatus === 'refunded') total.refunded++;
  if (job.status === 'done' && job.chargeStatus === 'settled') {
    if (job.resource === 'free') total.free++;
    if (job.resource === 'v5') total.v5 += Number(job.amount) || 0;
    if (job.resource === 'anlas') { total.fixed += Number(job.fixed) || 0; total.purchased += Number(job.purchased) || 0; }
  }
  const wait = seconds(job.createdAt, job.startedAt), generation = seconds(job.upstreamStartedAt, job.upstreamCompletedAt);
  if (wait !== null) { total.waitSeconds += wait; total.waitCount++; }
  if (job.status === 'done' && generation !== null) { total.generationSeconds += generation; total.generationCount++; }
}
function finish<T extends UsageAccumulator>(total: T) {
  const { waitSeconds, waitCount, generationSeconds, generationCount, ...counts } = total;
  return { ...counts, averageWaitSeconds: waitCount ? waitSeconds / waitCount : null, averageGenerationSeconds: generationCount ? generationSeconds / generationCount : null };
}

export function summarizeOwnerUsage(rows: readonly OwnerJobRow[]) {
  const total = empty(), members = new Map<string, GroupAccumulator>(), models = new Map<string, GroupAccumulator>(), dates = new Map<string, GroupAccumulator>();
  for (const job of rows) {
    add(total, job);
    for (const [map, key, label] of [[members,job.memberId,job.username],[models,job.model || 'unknown',job.model || '未知模型'],[dates,day(job.createdAt),day(job.createdAt)]] as const) {
      if (!map.has(key)) map.set(key, { id:key, label, ...empty() });
      add(map.get(key)!, job);
    }
  }
  return { totals:finish(total), members:[...members.values()].map(finish), models:[...models.values()].map(finish), days:[...dates.values()].map(finish),
    recent:rows.slice(0,50).map(j => ({ id:j.id, memberId:j.memberId, username:j.username, model:j.model, status:j.status, createdAt:j.createdAt,
      waitSeconds:seconds(j.createdAt,j.startedAt), generationSeconds:seconds(j.upstreamStartedAt,j.upstreamCompletedAt),
      resource:j.resource, amount:j.amount, fixed:j.fixed, purchased:j.purchased, chargeStatus:j.chargeStatus, errorStatus:j.errorStatus })) };
}

function sharedAccount(db: OwnerDatabase) {
  return db.accounts.find(a => a.id === db.settings.fairQuota?.boundAccountId) || (!db.settings.fairQuota?.boundAccountId && db.accounts.length === 1 ? db.accounts[0] : null);
}
function safeAccount(a: OwnerAccount | null | undefined) {
  return a && { id:a.id, enabled:a.enabled === true, inFlight:Number(a.inFlight || 0), quotaTier:a.quotaTier,
    quotaFixed:a.quotaFixed, quotaPurchased:a.quotaPurchased, v5UsagePercent:a.v5UsagePercent,
    quotaCheckedAt:a.quotaCheckedAt, quotaError:a.quotaError ? '官方额度同步失败，请重新验证凭据' : '' };
}

export async function ownerOperation(url: URL, method: string | undefined, body: Record<string, unknown> | undefined, deps: OwnerDependencies) {
  const path = url.pathname.slice('/api/member/owner'.length), {store} = deps;
  if (path === '/statistics' && method === 'GET') {
    const days = Number(url.searchParams.get('days') || 7);
    if (![1,7,30].includes(days)) fail(400, '统计范围仅支持 1、7 或 30 天');
    const now = Date.now(), since = new Date(Math.floor((now + offsetMs) / dayMs) * dayMs - offsetMs - (days - 1) * dayMs).toISOString();
    // 既有响应的 days 是逐日统计数组，会覆盖查询范围的天数；保持键顺序和输出形态。
    const range = { days, since, until:new Date(now).toISOString(), timezone:'Asia/Taipei' };
    return { ...range, ...summarizeOwnerUsage(await store.readOwnerJobRows(since,new Date(now).toISOString())) };
  }
  const account = await store.update(db => sharedAccount(db), {persist:false});
  if (path === '/summary' && method === 'GET') return { accounts:account ? [safeAccount(account)] : [] };
  if (!account) fail(409, '尚未确定共享上游账户');
  // 成员路由只为写操作提供已解析的 JSON 对象，GET 不读取正文。
  const input = body!;
  if (path === '/accounts/quota' && method === 'POST') {
    if (Object.keys(input).some(k => k !== 'ids') || !Array.isArray(input.ids) || input.ids.length !== 1 || input.ids[0] !== account.id) fail(400, '仅可刷新当前共享账户');
    const result = await deps.refreshAccountQuotas({ids:[account.id]});
    return { checked:result.checked, ok:result.ok, failed:result.failed };
  }
  if (path === '/accounts/' + account.id && method === 'PATCH') {
    if (Object.keys(input).length !== 1 || !['token','enabled'].includes(Object.keys(input)[0])) fail(400, '只允许更新启用状态或 PST');
    if ('token' in input) {
      if (typeof input.token !== 'string' || input.token.length > 4096 || !/^pst-[A-Za-z0-9_-]+$/.test(input.token)) fail(400, '请输入完整 PST');
      return safeAccount(await deps.updateAccount(account.id,{token:input.token,enabled:false}));
    }
    if (typeof input.enabled !== 'boolean') fail(400, 'enabled 必须为布尔值');
    if (input.enabled) {
      const result = await deps.refreshAccountQuotas({ids:[account.id]});
      const checked = await store.update(db => sharedAccount(db), {persist:false});
      const expiry = Number(checked?.subscriptionExpiresAt), expiresAt = Number.isFinite(expiry) && expiry > 0 ? (expiry < 1e11 ? expiry * 1000 : expiry) : Date.parse(checked?.subscriptionExpiresAt as string);
      if (result.failed || checked?.quotaError || checked?.quotaTier !== 3 || checked?.subscriptionActive !== true || (checked.subscriptionExpiresAt && !(expiresAt > Date.now()))) fail(409, '未核验到有效 Opus，上游保持暂停');
    }
    return safeAccount(await deps.updateAccount(account.id,{enabled:input.enabled}));
  }
  fail(404, '未开放此管理操作');
}
