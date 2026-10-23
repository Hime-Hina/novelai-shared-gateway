import { parentPort, workerData } from 'node:worker_threads';
import Database from 'better-sqlite3';
import { activityDate, type ActivityDay, type ActivityOperation } from './activity-heatmap.ts';
import type { images, users } from './storage-schema.ts';

export interface ActivityQuery { since: string; until: string; memberId?: string | null; operation?: ActivityOperation; dates?: string[] | null }
export interface AdminStatsQuery { now?: number; days?: number }
export interface PageQuery { limit?: number; offset?: number; q?: string }
export interface ImagePageQuery extends PageQuery { tier?: string; model?: string }
export type ReadTask = { id: number } & (
  | { type: 'adminStats'; payload?: AdminStatsQuery }
  | { type: 'activityDays'; payload: ActivityQuery }
  | { type: 'imagePage'; payload?: ImagePageQuery }
  | { type: 'userPage'; payload?: PageQuery }
);
interface AccountStatsRow { status: string | null; accountId: string | null; statsExcluded: number; count: number }
interface SpeedRow { modelVersion: 'v45' | 'v5' | ''; count: number; durationSeconds: number | null }
interface UsageRow { date: string | null; hour: number | null; status: string | null; count: number; credits: number | null }
interface JobStats { done: number; failed: number; total?: number; successRate?: number }
interface SpeedStats { seconds: number | null; count: number }
type DataRow = Pick<typeof images.$inferSelect | typeof users.$inferSelect, 'data'>;

const sqlite = new Database((workerData as { dbPath: string }).dbPath, { readonly: true, fileMustExist: true });
sqlite.pragma('query_only = ON');
sqlite.pragma('busy_timeout = 3000');
sqlite.function('activity_time', { deterministic: true }, value => activityDate(value) ? Date.parse(value as string) : null);

parentPort!.on('message', ({ id, type, payload = {} }: ReadTask) => {
  try {
    const result = type === 'adminStats'
      ? readAdminStats(payload as AdminStatsQuery)
      : type === 'activityDays'
        ? readActivityDays(payload as ActivityQuery)
      : type === 'imagePage'
        ? readImagePage(payload as ImagePageQuery)
        : type === 'userPage'
          ? readUserPage(payload as PageQuery)
          : (() => { throw new Error(`Unknown read task: ${type}`); })();
    parentPort!.postMessage({ id, result });
  } catch (error) {
    parentPort!.postMessage({ id, error: (error as { message?: unknown } | null)?.message || String(error) });
  }
});

function readActivityDays({ since, until, memberId = null, operation = 'generate', dates = null }: ActivityQuery): ActivityDay[] {
  if (dates?.length === 0) return [];
  // ISO 偏移可跨 UTC 日期；先用现有索引取宽边界，再按严格解析的时间精确过滤。
  const margin = 2 * 86400000;
  const start = Date.parse(since), end = Date.parse(until), selectedDates = dates?.slice().sort();
  const lower = selectedDates ? Math.max(start, Date.parse(selectedDates[0] + 'T00:00:00Z') - 8 * 3600000) : start;
  const upper = selectedDates ? Math.min(end, Date.parse(selectedDates.at(-1) + 'T00:00:00Z') + 16 * 3600000) : end;
  const params = { since: start, until: end, memberId, operation,
    lower: new Date(lower - margin).toISOString().slice(0, 10),
    upper: new Date(upper + margin).toISOString().slice(0, 10),
    dates: dates === null ? null : JSON.stringify(dates) };
  return sqlite.prepare<typeof params, ActivityDay>(`
    WITH activity AS (
      SELECT activity_time(j.created_at) AS submitted,
        COALESCE(NULLIF(p.member_id, ''), NULLIF(json_extract(j.data, '$.generationAttempt.memberId'), ''),
          NULLIF(json_extract(j.data, '$.fairCharge.userId'), ''),
          (SELECT u.id FROM users u WHERE u.token = j.user_token LIMIT 1)) AS member,
        COALESCE(NULLIF(p.operation, ''), NULLIF(json_extract(j.data, '$.request.operation'), ''),
          NULLIF(json_extract(j.data, '$.generationAttempt.operation'), ''),
          NULLIF(json_extract(j.data, '$.request.action'), ''), 'generate') AS operation,
        CASE WHEN j.status = 'failed' AND NULLIF(json_extract(j.data, '$.cancelledAt'), '') IS NOT NULL
          THEN 'cancelled' ELSE COALESCE(j.status, 'other') END AS state
      FROM jobs j INDEXED BY idx_jobs_created_stats
      LEFT JOIN generation_requests p ON p.id = j.request_id
      WHERE j.created_at >= @lower AND j.created_at < @upper
    ), selected AS (
      SELECT strftime('%Y-%m-%d', submitted / 1000.0, 'unixepoch', '+8 hours') AS date, state
      FROM activity WHERE submitted >= @since AND submitted < @until
        AND (@memberId IS NULL OR member = @memberId)
        AND CASE WHEN operation IN ('generate', 'img2img', 'infill') THEN 'generate' ELSE operation END = @operation
    )
    SELECT date, COUNT(*) AS total,
      SUM(state = 'done') AS done, SUM(state = 'failed') AS failed,
      SUM(state = 'cancelled') AS cancelled, SUM(state = 'queued') AS queued,
      SUM(state = 'pending') AS pending, SUM(state = 'running') AS running,
      SUM(state NOT IN ('done', 'failed', 'cancelled', 'queued', 'pending', 'running')) AS other
    FROM selected WHERE @dates IS NULL OR date IN (SELECT value FROM json_each(@dates))
    GROUP BY date ORDER BY date
  `).all(params);
}

function readAdminStats({ now = Date.now(), days = 7 }: AdminStatsQuery = {}) {
  const oneMinuteAgo = new Date(now - 60_000).toISOString();
  const oneHourAgo = new Date(now - 60 * 60_000).toISOString();
  const chartCutoff = new Date(now - days * 24 * 60 * 60_000).toISOString();
  const requestRow = sqlite.prepare<[string], { total: number }>(`
    SELECT COUNT(*) AS total
    FROM jobs INDEXED BY idx_jobs_created_stats
    WHERE created_at >= ?
  `).get(oneMinuteAgo);
  // Generation prices start at 1; zero/fractional-cost completions are cached deliveries.
  const generationRow = sqlite.prepare<{ since: string; now: string }, { total: number }>(`
    SELECT COUNT(*) AS total
    FROM jobs INDEXED BY idx_jobs_updated_stats
    WHERE updated_at >= @since AND updated_at <= @now
      AND status = 'done' AND cost >= 1
  `).get({ since: oneMinuteAgo, now: new Date(now).toISOString() });
  const accountRows = sqlite.prepare<{ since: string }, AccountStatsRow>(`
    SELECT status, account_id AS accountId, stats_excluded AS statsExcluded, COUNT(*) AS count
    FROM jobs INDEXED BY idx_jobs_created_stats
    WHERE created_at >= @since
      AND status IN ('done', 'failed')
    GROUP BY status, account_id, stats_excluded
  `).all({ since: oneHourAgo });
  const speedRows = sqlite.prepare<{ since: string }, SpeedRow>(`
    SELECT
      CASE
        WHEN model LIKE 'nai-diffusion-5%' THEN 'v5'
        WHEN model LIKE 'nai-diffusion-4-5%' THEN 'v45'
        ELSE ''
      END AS modelVersion,
      COUNT(*) AS count,
      SUM(MAX(0, (julianday(updated_at) - julianday(created_at)) * 86400.0)) AS durationSeconds
    FROM jobs INDEXED BY idx_jobs_updated_stats
    WHERE updated_at >= @since
      AND status = 'done'
      AND created_at IS NOT NULL
      AND model LIKE 'nai-diffusion-%'
    GROUP BY modelVersion
  `).all({ since: oneHourAgo });
  const usageRows = sqlite.prepare<{ cutoff: string }, UsageRow>(`
    SELECT
      strftime('%Y-%m-%d', datetime(updated_at, '+8 hours')) AS date,
      CAST(strftime('%H', datetime(updated_at, '+8 hours')) AS INTEGER) AS hour,
      status,
      SUM(CASE WHEN status = 'failed' OR cost >= 1 THEN 1 ELSE 0 END) AS count,
      SUM(CASE WHEN status = 'done' THEN cost ELSE 0 END) AS credits
    FROM jobs INDEXED BY idx_jobs_updated_stats
    WHERE updated_at >= @cutoff
      AND status IN ('done', 'failed')
    GROUP BY date, hour, status
  `).all({ cutoff: chartCutoff });

  const jobStats = { done: 0, failed: 0 };
  const accountStats: Record<string, JobStats> = {};
  const speed: Record<'v45' | 'v5', SpeedStats> = {
    v45: { seconds: null, count: 0 },
    v5: { seconds: null, count: 0 }
  };
  const speedTotals = { v45: 0, v5: 0 };

  for (const row of accountRows) {
    const count = Number(row.count || 0);
    if (!row.statsExcluded && (row.status === 'done' || row.status === 'failed')) {
      jobStats[row.status] += count;
      if (row.accountId) {
        accountStats[row.accountId] ||= { done: 0, failed: 0 };
        accountStats[row.accountId][row.status] += count;
      }
    }
  }
  for (const row of speedRows) {
    if (!speed[row.modelVersion as keyof typeof speed]) continue;
    speed[row.modelVersion as keyof typeof speed].count = Number(row.count || 0);
    speedTotals[row.modelVersion as keyof typeof speedTotals] = Number(row.durationSeconds || 0);
  }

  for (const version of Object.keys(speed) as (keyof typeof speed)[]) {
    const count = speed[version].count;
    speed[version].seconds = count ? Math.max(0, speedTotals[version] / count) : null;
  }
  for (const accountId of Object.keys(accountStats)) accountStats[accountId] = finalizeStats(accountStats[accountId]);

  return {
    requestStats1m: { total: Number(requestRow?.total || 0) },
    generationStats1m: { total: Number(generationRow?.total || 0) },
    jobStats1h: finalizeStats(jobStats),
    generationSpeed1h: speed,
    accountStats1h: accountStats,
    usageHourlyDays: buildUsageDays(usageRows, now, days),
    statsRowsRead: accountRows.length + speedRows.length + usageRows.length + 2
  };
}

function readImagePage({ limit = 60, offset = 0, q = '', tier = '', model = '' }: ImagePageQuery = {}) {
  const filters: string[] = [];
  const params: { limit: number; offset: number; tier?: string; model?: string; q?: string } = { limit, offset };
  if (tier) {
    filters.push('resolution_tier = @tier');
    params.tier = tier;
  }
  if (model) {
    filters.push('model = @model');
    params.model = model;
  }
  if (q) {
    filters.push('search_text LIKE @q');
    params.q = `%${q}%`;
  }
  const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
  const total = Number(sqlite.prepare<[], { count: number }>('SELECT COUNT(*) AS count FROM images').get()?.count || 0);
  const matched = filters.length
    ? Number(sqlite.prepare<typeof params, { count: number }>(`SELECT COUNT(*) AS count FROM images ${where}`).get(params)?.count || 0)
    : total;
  const rows = sqlite.prepare<typeof params, DataRow>(`
    SELECT data FROM images
    ${where}
    ORDER BY order_value DESC
    LIMIT @limit OFFSET @offset
  `).all(params);
  return { images: parseRows(rows), total, matched, offset, limit };
}

function readUserPage({ limit = 300, offset = 0, q = '' }: PageQuery = {}) {
  const params: { limit: number; offset: number; q?: string } = { limit, offset };
  const where = q ? 'WHERE search_text LIKE @q' : '';
  if (q) params.q = `%${q}%`;
  const total = Number(sqlite.prepare<[], { count: number }>('SELECT COUNT(*) AS count FROM users').get()?.count || 0);
  const matched = q
    ? Number(sqlite.prepare<typeof params, { count: number }>(`SELECT COUNT(*) AS count FROM users ${where}`).get(params)?.count || 0)
    : total;
  const rows = sqlite.prepare<typeof params, DataRow>(`
    SELECT data FROM users
    ${where}
    ORDER BY order_value DESC
    LIMIT @limit OFFSET @offset
  `).all(params);
  return { users: parseRows(rows), total, matched, offset, limit };
}

function parseRows(rows: DataRow[]): unknown[] {
  const values: unknown[] = [];
  for (const row of rows) {
    try {
      values.push(JSON.parse(row.data));
    } catch {
      // Skip corrupt rows without failing the whole admin page.
    }
  }
  return values;
}

function buildUsageDays(rows: UsageRow[], now: number, days: number) {
  const offsetMs = 8 * 60 * 60_000;
  const current = new Date(now + offsetMs);
  const midnight = Date.UTC(current.getUTCFullYear(), current.getUTCMonth(), current.getUTCDate());
  const result = Array.from({ length: days }, (_, index) => {
    const date = new Date(midnight + (index - days + 1) * 24 * 60 * 60_000).toISOString().slice(0, 10);
    return {
      date,
      label: date.slice(5),
      done: 0,
      failed: 0,
      total: 0,
      credits: 0,
      successRate: 0,
      hours: Array.from({ length: 24 }, (_, hour) => ({
        hour,
        label: `${String(hour).padStart(2, '0')}:00`,
        done: 0,
        failed: 0,
        total: 0,
        credits: 0,
        successRate: 0
      }))
    };
  });
  const byDate = new Map(result.map((item) => [item.date, item]));
  for (const row of rows) {
    const day = byDate.get(row.date!);
    const hour = Number(row.hour || 0);
    if (!day || !day.hours[hour]) continue;
    const count = Number(row.count || 0);
    const credits = Math.max(0, Number(row.credits || 0));
    if (row.status === 'done') {
      day.done += count;
      day.credits += credits;
      day.hours[hour].done += count;
      day.hours[hour].credits += credits;
    } else if (row.status === 'failed') {
      day.failed += count;
      day.hours[hour].failed += count;
    }
  }
  for (const day of result) {
    Object.assign(day, finalizeStats(day));
    for (const hour of day.hours) Object.assign(hour, finalizeStats(hour));
  }
  return result;
}

function finalizeStats(value: JobStats) {
  const done = Number(value.done || 0);
  const failed = Number(value.failed || 0);
  const total = done + failed;
  return { done, failed, total, successRate: total ? done / total : 0 };
}

export type ReadResults = { adminStats: ReturnType<typeof readAdminStats>; activityDays: ActivityDay[]; imagePage: ReturnType<typeof readImagePage>; userPage: ReturnType<typeof readUserPage> };
