import type Database from 'better-sqlite3';
import type { OwnerJobRow } from '../members/owner-operations.ts';
import type { ActivityDay, readActivityHeatmap } from '../reports/activity-heatmap.ts';
import { activityDate } from '../reports/activity-heatmap.ts';
import { parseAmount, formatAmount } from '../quota/resource-amounts.ts';
import { assertRelationalStorageFormat } from './relational-storage.ts';

type ActivityQuery = Parameters<Parameters<typeof readActivityHeatmap>[0]['readActivityDays']>[0];
const iso = (at: number | null) => (at === null ? null : new Date(at).toISOString());
function timestamp(value: string): number {
  if (!activityDate(value)) {
    throw Object.assign(new Error('统计时间范围无效。'), { statusCode: 400 });
  }
  return Date.parse(value);
}
interface UsageRow {
  id: string;
  memberId: string;
  username: string;
  model: string | null;
  status: string;
  createdAt: number | null;
  startedAt: number | null;
  upstreamStartedAt: number | null;
  upstreamCompletedAt: number | null;
  errorStatus: number | null;
  chargeStatus: string | null;
  resource: string | null;
  amount: string | null;
  fixed: string | null;
  purchased: string | null;
  knownNoCharge: number;
}

// 授权继续归现有成员/owner/admin 入口；这里不解释旧模型或持有业务写能力。
export class RelationalReportStore {
  private sqlite: Database.Database;
  constructor(sqlite: Database.Database) {
    assertRelationalStorageFormat(sqlite);
    this.sqlite = sqlite;
  }

  async readOwnerJobRows(since: string, until: string): Promise<OwnerJobRow[]> {
    const start = since ? timestamp(since) : null,
      end = timestamp(until);
    if (start !== null && start > end) {
      throw Object.assign(new Error('统计时间范围无效。'), { statusCode: 400 });
    }
    return this.sqlite.transaction(() =>
      this.sqlite
        .prepare<unknown[], UsageRow>(
          `SELECT j.id,r.member_id AS memberId,m.display_name AS username,r.model,j.status,
        origin.created_at AS createdAt,a.authorized_at AS startedAt,
        a.upstream_started_at AS upstreamStartedAt,a.upstream_completed_at AS upstreamCompletedAt,
        j.error_status AS errorStatus,c.status AS chargeStatus,
        CASE c.billing_mode WHEN 'anlas_fallback' THEN 'anlas' ELSE c.billing_mode END AS resource,
        c.v5 AS amount,c.anlas_fixed AS fixed,c.anlas_purchased AS purchased,
        EXISTS(SELECT 1 FROM domain_events cached WHERE cached.stream_id='request:'||r.id
          AND cached.event_type='CachedResultLinked' AND json_extract(cached.payload,'$.jobId')=j.id
          AND json_extract(cached.payload,'$.artifactId')=j.artifact_id) AS knownNoCharge
        FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id
        JOIN members m ON m.id=r.member_id
        LEFT JOIN generation_job_origins origin ON origin.stream_id='request:'||r.id
          AND origin.job_id=j.id AND origin.request_id=r.id
        LEFT JOIN generation_attempts a ON a.job_id=j.id LEFT JOIN charges c ON c.job_id=j.id
        WHERE j.retired_at IS NULL AND r.retired_at IS NULL
        ${start === null ? 'AND (origin.created_at IS NULL OR origin.created_at<=?)' : 'AND origin.created_at>=? AND origin.created_at<=?'}
        ORDER BY origin.created_at DESC,j.id DESC`,
        )
        .all(...(start === null ? [end] : [start, end]))
        .map((row) => {
          let amount = row.amount;
          if (row.resource === 'anlas') {
            amount = null;
            if (row.fixed !== null && row.purchased !== null) {
              amount = formatAmount(parseAmount(row.fixed) + parseAmount(row.purchased));
            }
          }
          return {
            ...row,
            createdAt: iso(row.createdAt),
            startedAt: iso(row.startedAt),
            upstreamStartedAt: iso(row.upstreamStartedAt),
            upstreamCompletedAt: iso(row.upstreamCompletedAt),
            knownNoCharge: row.knownNoCharge === 1,
            amount,
          };
        }),
    )();
  }

  async readActivityDays({
    since,
    until,
    memberId,
    operation,
    dates,
  }: ActivityQuery): Promise<ActivityDay[]> {
    if (dates?.length === 0) {
      return [];
    }
    const start = timestamp(since),
      end = timestamp(until);
    if (start > end) {
      throw Object.assign(new Error('统计时间范围无效。'), { statusCode: 400 });
    }
    const params = {
      start,
      end,
      memberId,
      operation,
      dates: dates === null ? null : JSON.stringify(dates),
    };
    return this.sqlite.transaction(() =>
      this.sqlite
        .prepare<typeof params, ActivityDay>(
          `WITH selected AS (
          SELECT strftime('%Y-%m-%d',origin.created_at / 1000,'unixepoch','+8 hours') AS date,
            CASE WHEN j.status='failed' AND j.cancellation_reason IS NOT NULL THEN 'cancelled'
              ELSE j.status END AS state
          FROM generation_job_origins origin JOIN generation_jobs j ON j.id=origin.job_id AND j.request_id=origin.request_id
          JOIN generation_requests r ON r.id=j.request_id
          WHERE origin.created_at>=@start AND origin.created_at<@end AND r.operation=@operation
            AND j.retired_at IS NULL AND r.retired_at IS NULL
            ${memberId === null ? '' : 'AND r.member_id=@memberId'}
        ) SELECT date,COUNT(*) AS total,SUM(state='done') AS done,SUM(state='failed') AS failed,
          SUM(state='cancelled') AS cancelled,SUM(state='queued') AS queued,SUM(state='pending') AS pending,
          SUM(state='running') AS running,SUM(state NOT IN ('done','failed','cancelled','queued','pending','running')) AS other
        FROM selected WHERE @dates IS NULL OR date IN (SELECT value FROM json_each(@dates))
        GROUP BY date ORDER BY date`,
        )
        .all(params),
    )();
  }
}
