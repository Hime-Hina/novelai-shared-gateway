const fail = (statusCode, message) => { throw Object.assign(new Error(message), { statusCode }); };
const dayMs = 86400000, offsetMs = 8 * 3600000;
const day = value => new Date(Date.parse(value) + offsetMs).toISOString().slice(0, 10);
const seconds = (start, end) => {
  const value = (Date.parse(end) - Date.parse(start)) / 1000;
  return Number.isFinite(value) && value >= 0 ? value : null;
};
const empty = () => ({ total:0, done:0, failed:0, queued:0, pending:0, running:0, refunded:0, free:0, v5:0, fixed:0, purchased:0, waitSeconds:0, waitCount:0, generationSeconds:0, generationCount:0 });
function add(total, job) {
  total.total++;
  if (['done','failed','queued','pending','running'].includes(job.status)) total[job.status]++;
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
function finish(total) {
  const { waitSeconds, waitCount, generationSeconds, generationCount, ...counts } = total;
  return { ...counts, averageWaitSeconds: waitCount ? waitSeconds / waitCount : null, averageGenerationSeconds: generationCount ? generationSeconds / generationCount : null };
}

export function summarizeOwnerUsage(rows) {
  const total = empty(), members = new Map(), models = new Map(), dates = new Map();
  for (const job of rows) {
    add(total, job);
    for (const [map, key, label] of [[members,job.memberId,job.username],[models,job.model || 'unknown',job.model || '未知模型'],[dates,day(job.createdAt),day(job.createdAt)]]) {
      if (!map.has(key)) map.set(key, { id:key, label, ...empty() });
      add(map.get(key), job);
    }
  }
  return { totals:finish(total), members:[...members.values()].map(finish), models:[...models.values()].map(finish), days:[...dates.values()].map(finish),
    recent:rows.slice(0,50).map(j => ({ id:j.id, memberId:j.memberId, username:j.username, model:j.model, status:j.status, createdAt:j.createdAt,
      waitSeconds:seconds(j.createdAt,j.startedAt), generationSeconds:seconds(j.upstreamStartedAt,j.upstreamCompletedAt),
      resource:j.resource, amount:j.amount, fixed:j.fixed, purchased:j.purchased, chargeStatus:j.chargeStatus, errorStatus:j.errorStatus })) };
}

function sharedAccount(db) {
  return db.accounts.find(a => a.id === db.settings.fairQuota?.boundAccountId) || (!db.settings.fairQuota?.boundAccountId && db.accounts.length === 1 ? db.accounts[0] : null);
}
function safeAccount(a) {
  return a && { id:a.id, enabled:a.enabled === true, inFlight:Number(a.inFlight || 0), quotaTier:a.quotaTier,
    quotaFixed:a.quotaFixed, quotaPurchased:a.quotaPurchased, v5UsagePercent:a.v5UsagePercent,
    quotaCheckedAt:a.quotaCheckedAt, quotaError:a.quotaError ? '官方额度同步失败，请重新验证凭据' : '' };
}

export async function ownerOperation(url, method, body, deps) {
  const path = url.pathname.slice('/api/member/owner'.length), {store} = deps;
  if (path === '/statistics' && method === 'GET') {
    const days = Number(url.searchParams.get('days') || 7);
    if (![1,7,30].includes(days)) fail(400, '统计范围仅支持 1、7 或 30 天');
    const now = Date.now(), since = new Date(Math.floor((now + offsetMs) / dayMs) * dayMs - offsetMs - (days - 1) * dayMs).toISOString();
    return { days, since, until:new Date(now).toISOString(), timezone:'Asia/Taipei', ...summarizeOwnerUsage(await store.readOwnerJobRows(since,new Date(now).toISOString())) };
  }
  const account = await store.update(db => sharedAccount(db), {persist:false});
  if (path === '/summary' && method === 'GET') return { accounts:account ? [safeAccount(account)] : [] };
  if (!account) fail(409, '尚未确定共享上游账户');
  if (path === '/accounts/quota' && method === 'POST') {
    if (Object.keys(body).some(k => k !== 'ids') || !Array.isArray(body.ids) || body.ids.length !== 1 || body.ids[0] !== account.id) fail(400, '仅可刷新当前共享账户');
    const result = await deps.refreshAccountQuotas({ids:[account.id]});
    return { checked:result.checked, ok:result.ok, failed:result.failed };
  }
  if (path === '/accounts/' + account.id && method === 'PATCH') {
    if (Object.keys(body).length !== 1 || !['token','enabled'].includes(Object.keys(body)[0])) fail(400, '只允许更新启用状态或 PST');
    if ('token' in body) {
      if (typeof body.token !== 'string' || body.token.length > 4096 || !/^pst-[A-Za-z0-9_-]+$/.test(body.token)) fail(400, '请输入完整 PST');
      return safeAccount(await deps.updateAccount(account.id,{token:body.token,enabled:false}));
    }
    if (typeof body.enabled !== 'boolean') fail(400, 'enabled 必须为布尔值');
    if (body.enabled) {
      const result = await deps.refreshAccountQuotas({ids:[account.id]});
      const checked = await store.update(db => sharedAccount(db), {persist:false});
      const expiry = Number(checked?.subscriptionExpiresAt), expiresAt = Number.isFinite(expiry) && expiry > 0 ? (expiry < 1e11 ? expiry * 1000 : expiry) : Date.parse(checked?.subscriptionExpiresAt);
      if (result.failed || checked?.quotaError || checked?.quotaTier !== 3 || checked?.subscriptionActive !== true || (checked.subscriptionExpiresAt && !(expiresAt > Date.now()))) fail(409, '未核验到有效 Opus，上游保持暂停');
    }
    return safeAccount(await deps.updateAccount(account.id,{enabled:body.enabled}));
  }
  fail(404, '未开放此管理操作');
}
