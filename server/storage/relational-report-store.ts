import type Database from 'better-sqlite3';
import type { OwnerJobRow } from '../members/owner-operations.ts';
import type { ActivityDay, readActivityHeatmap } from '../reports/activity-heatmap.ts';
import { activityDate } from '../reports/activity-heatmap.ts';
import { parseAmount, formatAmount } from '../quota/resource-amounts.ts';
import { assertRelationalStorageFormat } from './relational-storage.ts';
import { RelationalInputStore } from './relational-input-store.ts';
import { selectErrorLogJobs, publicErrorLog } from '../reports/generation-errors.ts';
import { jobDurationMs } from '../generation/request-queries.ts';
import { relationalFailureTimeSql, relationalFailurePredicate } from './relational-schema.ts';
import { RelationalQuotaStore } from './relational-quota-store.ts';
import { parseRelationalFact } from '../events/relational-events.ts';
import type { Resource } from '../quota/fair-share.ts';
import {
  summarizeRelationalFailureRisk,
  publicFailureRisk,
  failureAccountingAlerts,
  publicFailureOperating,
  publicRelationalFailureEvent,
  relationalLossPolicy,
  RELATIONAL_FAILURE_EVENT_PREDICATE,
} from '../quota/failure-accounting.ts';

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
interface ErrorRow {
  id: string;
  source: string;
  accountId: string;
  status: string;
  error: string | null;
  createdAt: number | null;
  failedAt: number;
  eventId: string;
  diagnosticId: string | null;
}
interface FailureAttemptRow {
  memberId: string;
  memberName: string | null;
  firstUnknownAt: number | null;
  upstreamCandidates: string;
  candidateMode: string;
}
interface FailureHistoryRow {
  eventId: string;
  type: string;
  payload: string;
  commitSeq: string;
  eventIndex: number;
  at: number;
  memberId: string | null;
  accountId: string | null;
  actorId: string | null;
  memberName: string | null;
  jobId: string | null;
  attemptId: string | null;
  operation: string | null;
  model: string | null;
  candidateMode: string | null;
  upstreamCandidates: string | null;
  v5: string | null;
  fixed: string | null;
  purchased: string | null;
  lossEventId: string | null;
  closureEventId: string | null;
  firstUnknownAt: number | null;
  deadlineAt: number | null;
}

function lossCursor(value: string | null, historyId: string): [bigint, number] | null {
  if (value === null) {
    return null;
  }
  try {
    if (!value || value.length > 4096 || !/^[A-Za-z0-9_-]+$/.test(value)) {
      throw new Error();
    }
    const point: unknown = JSON.parse(Buffer.from(value, 'base64url').toString());
    if (
      !Array.isArray(point) ||
      point.length !== 3 ||
      point[0] !== historyId ||
      typeof point[1] !== 'string' ||
      !/^[1-9][0-9]*$/.test(point[1]) ||
      BigInt(point[1]) > 9223372036854775807n ||
      !Number.isSafeInteger(point[2]) ||
      point[2] < 0
    ) {
      throw new Error();
    }
    return [BigInt(point[1]), point[2]];
  } catch {
    throw Object.assign(new Error('历史分页游标无效'), { statusCode: 400 });
  }
}

// 授权继续归现有成员/owner/admin 入口；这里不解释旧模型或持有业务写能力。
export class RelationalReportStore {
  private sqlite: Database.Database;
  constructor(sqlite: Database.Database) {
    assertRelationalStorageFormat(sqlite);
    this.sqlite = sqlite;
  }

  readFailureAccounting(before: string | null, now = Date.now()) {
    if (!Number.isSafeInteger(now) || now < 0) {
      throw Object.assign(new Error('损耗读取时间无效'), { statusCode: 400 });
    }
    return this.sqlite.transaction(() => {
      const historyId = this.sqlite
          .prepare<[], { historyId: string }>(
            'SELECT history_id AS historyId FROM projection_state WHERE id=1',
          )
          .get()!.historyId,
        point = lossCursor(before, historyId),
        state = new RelationalQuotaStore(this.sqlite).readFailureAccountingState(),
        since30 = Math.max(0, now - 30 * 86400000),
        sqlAttempts = this.sqlite.prepare<[number, number], FailureAttemptRow>(`WITH eligible AS (
          SELECT id FROM generation_attempts WHERE loss_event_id IS NOT NULL AND first_unknown_at>=? AND first_unknown_at<=?
          UNION ALL SELECT id FROM generation_attempts WHERE loss_event_id IS NOT NULL AND first_unknown_at IS NULL
          ) SELECT r.member_id AS memberId,
          m.display_name AS memberName,a.first_unknown_at AS firstUnknownAt,a.upstream_candidates AS upstreamCandidates,a.candidate_mode AS candidateMode
          FROM eligible selected JOIN generation_attempts a ON a.id=selected.id
          JOIN generation_jobs j ON j.id=a.job_id
          JOIN generation_requests r ON r.id=j.request_id LEFT JOIN members m ON m.id=r.member_id`),
        attemptRows = sqlAttempts.iterate(since30, now),
        aggregate = this.sqlite
          .prepare<[number, number], { at: number; resource: Resource; amount: string }>(
            `SELECT occurred_at AS at,
          CASE event_type WHEN 'V5CapacityObserved' THEN 'v5' ELSE json_extract(payload,'$.resource') END AS resource,
          CASE event_type WHEN 'V5CapacityObserved' THEN json_extract(payload,'$.delta') ELSE json_extract(payload,'$.amount') END AS amount
          FROM domain_events WHERE event_type IN ('ResourceGapObserved','V5CapacityObserved') AND occurred_at>=? AND occurred_at<=?`,
          )
          .iterate(since30, now);
      function* attempts() {
        for (const row of attemptRows) {
          yield { ...row, upstreamCandidates: JSON.parse(row.upstreamCandidates) };
        }
      }
      const summary = summarizeRelationalFailureRisk(attempts(), aggregate, now),
        rows = this.sqlite
          .prepare<unknown[], FailureHistoryRow>(
            `WITH page AS MATERIALIZED (
          SELECT event_id,event_type,payload,commit_seq,event_index,occurred_at,member_id,account_id,stream_id
          FROM domain_events WHERE (${RELATIONAL_FAILURE_EVENT_PREDICATE}) ${point ? 'AND (commit_seq,event_index)<(?,?)' : ''}
          ORDER BY commit_seq DESC,event_index DESC LIMIT 51
        ) SELECT e.event_id AS eventId,e.event_type AS type,e.payload,CAST(e.commit_seq AS TEXT) AS commitSeq,
          e.event_index AS eventIndex,e.occurred_at AS at,e.member_id AS memberId,e.account_id AS accountId,
          command.actor_id AS actorId,m.display_name AS memberName,COALESCE(json_extract(e.payload,'$.jobId'),a.job_id) AS jobId,
          a.id AS attemptId,r.operation,r.model,a.candidate_mode AS candidateMode,a.upstream_candidates AS upstreamCandidates,
          c.v5,c.anlas_fixed AS fixed,c.anlas_purchased AS purchased,a.loss_event_id AS lossEventId,
          CASE WHEN e.event_type='LateResultRecovered' THEN (
            SELECT closed.event_id FROM domain_events closed WHERE closed.stream_id=e.stream_id
              AND closed.member_id IS e.member_id AND closed.account_id IS e.account_id
              AND json_extract(closed.payload,'$.attemptId')=a.id
              AND ((closed.event_type='OpeningAttemptRecorded' AND closed.event_id=a.loss_event_id
                AND json_extract(closed.payload,'$.reviewStatus')='estimated_closed'
                AND (closed.commit_seq,closed.event_index)<(e.commit_seq,e.event_index))
                OR (closed.event_type='ReviewClosed' AND json_extract(closed.payload,'$.lossEventId')=a.loss_event_id
                AND ((json_extract(closed.payload,'$.status')='estimated_closed'
                AND (closed.commit_seq,closed.event_index)<(e.commit_seq,e.event_index))
                OR (json_extract(closed.payload,'$.status')='result_recovered'
                  AND closed.commit_seq=e.commit_seq AND closed.event_index>e.event_index))))
            ORDER BY (closed.event_type='OpeningAttemptRecorded' OR json_extract(closed.payload,'$.status')='estimated_closed') DESC,closed.stream_version LIMIT 1
          ) END AS closureEventId,
          a.first_unknown_at AS firstUnknownAt,a.review_deadline_at AS deadlineAt
          FROM page e JOIN commits command ON command.commit_seq=e.commit_seq
          LEFT JOIN generation_attempts a ON a.id=COALESCE(json_extract(e.payload,'$.attemptId'),
            (SELECT id FROM generation_attempts WHERE job_id=json_extract(e.payload,'$.jobId')))
          LEFT JOIN generation_jobs j ON j.id=COALESCE(json_extract(e.payload,'$.jobId'),a.job_id)
          LEFT JOIN generation_requests r ON r.id=j.request_id
          LEFT JOIN members m ON m.id=e.member_id
          LEFT JOIN charges c ON c.original_job_id=COALESCE(json_extract(e.payload,'$.jobId'),a.job_id)
          ORDER BY e.commit_seq DESC,e.event_index DESC`,
          )
          .all(...(point ?? [])),
        page = rows.slice(0, 50),
        last = page.at(-1);
      return {
        asOf: iso(now),
        coverage: 'retained-facts',
        policy: state.policy,
        operating: publicFailureOperating(state.operating, state.pendingAnlasDebits),
        risk: {
          ...publicFailureRisk(summary.risk),
          unknownTimeAttempts: summary.unknownTimeAttempts,
        },
        alerts: failureAccountingAlerts(state.policy, summary.risk, summary.members),
        events: page.map((row) => {
          const fact = parseRelationalFact({
            eventType: row.type,
            payload: JSON.parse(row.payload),
          });
          let beforePolicy: null | ReturnType<typeof relationalLossPolicy> = null;
          if (fact.eventType === 'LossPolicyChanged') {
            const previous = this.sqlite
              .prepare<[bigint, number], { type: string; payload: string }>(
                `SELECT event_type AS type,payload FROM domain_events
              WHERE stream_id='sharing:default' AND event_type IN ('OpeningPolicyRecorded','LossPolicyChanged')
              AND (commit_seq,event_index)<(?,?) ORDER BY stream_version DESC LIMIT 1`,
              )
              .get(BigInt(row.commitSeq), row.eventIndex);
            if (previous) {
              const original = parseRelationalFact({
                eventType: previous.type,
                payload: JSON.parse(previous.payload),
              });
              if (
                original.eventType === 'OpeningPolicyRecorded' ||
                original.eventType === 'LossPolicyChanged'
              ) {
                beforePolicy = relationalLossPolicy(original);
              }
            }
          }
          return publicRelationalFailureEvent(
            {
              ...fact,
              eventId: row.eventId,
              occurredAt: row.at,
              memberId: row.memberId,
              accountId: row.accountId,
            },
            {
              actorId: row.actorId,
              memberName: row.memberName,
              jobId: row.jobId,
              attempt: row.attemptId
                ? {
                    ...(row.operation === null ? {} : { operation: row.operation }),
                    model: row.model,
                    ...(row.candidateMode === null ? {} : { candidateMode: row.candidateMode }),
                    ...(row.upstreamCandidates === null
                      ? {}
                      : { upstreamCandidates: JSON.parse(row.upstreamCandidates) }),
                  }
                : null,
              resources:
                row.v5 === null || row.fixed === null || row.purchased === null
                  ? null
                  : { v5: row.v5, anlasFixed: row.fixed, anlasPurchased: row.purchased },
              beforePolicy,
              lossEventId: row.lossEventId,
              closureEventId: row.closureEventId,
              firstUnknownAt: row.firstUnknownAt,
              deadlineAt: row.deadlineAt,
            },
          );
        }),
        nextCursor:
          rows.length > 50
            ? Buffer.from(JSON.stringify([historyId, last!.commitSeq, last!.eventIndex])).toString(
                'base64url',
              )
            : null,
      };
    })();
  }

  readErrorLogs(since: number, limit = 100) {
    if (
      !Number.isSafeInteger(since) ||
      since < 0 ||
      !Number.isSafeInteger(limit) ||
      limit < 1 ||
      limit > 100
    ) {
      throw Object.assign(new Error('错误记录查询范围无效。'), { statusCode: 400 });
    }
    return this.sqlite.transaction(() => {
      const inputs = new RelationalInputStore(this.sqlite),
        result: ReturnType<typeof publicErrorLog>[] = [];
      let before: { at: number; id: string } | null = null;
      while (result.length < limit) {
        const rows: ErrorRow[] = this.sqlite
          .prepare<unknown[], ErrorRow>(
            `SELECT j.id,r.source,a.account_id AS accountId,j.status,j.error_code AS error,
            origin.created_at AS createdAt,${relationalFailureTimeSql} AS failedAt,e.event_id AS eventId,
            CASE WHEN e.event_type='JobFailed' THEN json_extract(e.payload,'$.diagnosticId') END AS diagnosticId
            FROM domain_events e INDEXED BY idx_job_failure_time
            JOIN generation_jobs j ON j.id=json_extract(e.payload,'$.jobId')
            JOIN generation_requests r ON r.id=j.request_id
            JOIN generation_attempts a ON a.job_id=j.id
            LEFT JOIN generation_job_origins origin ON origin.stream_id=e.stream_id
              AND origin.job_id=j.id AND origin.request_id=r.id
            WHERE ${relationalFailurePredicate}
              AND e.stream_id='request:'||r.id AND e.member_id=r.member_id
              AND j.status='failed' AND j.retired_at IS NULL AND r.retired_at IS NULL
              AND ${relationalFailureTimeSql}>=?
              ${before ? `AND (${relationalFailureTimeSql},e.event_id)<(?,?)` : ''}
            ORDER BY ${relationalFailureTimeSql} DESC,e.event_id DESC LIMIT 100`,
          )
          .all(...(before ? [since, before.at, before.id] : [since]));
        if (!rows.length) {
          break;
        }
        for (const row of rows) {
          let errorDetail: string | null = null;
          if (row.diagnosticId !== null) {
            try {
              errorDetail = inputs.readGenerationDiagnostic(row.diagnosticId, row.id);
            } catch (error) {
              if ((error as { code?: string }).code !== 'diagnostic_unavailable') {
                throw error;
              }
            }
          }
          const job = {
            ...row,
            errorDetail,
            createdAt: iso(row.createdAt),
            updatedAt: iso(row.failedAt),
            completedAt: iso(row.failedAt),
          };
          if (!selectErrorLogJobs([job], since, 1).length) {
            continue;
          }
          let request: ReturnType<RelationalInputStore['readErrorLogRequest']> | null = null;
          try {
            request = inputs.readErrorLogRequest(row.id);
          } catch (error) {
            if ((error as { code?: string }).code !== 'input_unavailable') {
              throw error;
            }
          }
          result.push(
            publicErrorLog(job, request, {
              maskedUserToken: null,
              accountRouteId: null,
              durationMs: jobDurationMs(job),
            }),
          );
          if (result.length === limit) {
            break;
          }
        }
        const last = rows.at(-1)!;
        before = { at: last.failedAt, id: last.eventId };
      }
      return result;
    })();
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
          `SELECT j.id,r.member_id AS memberId,COALESCE(p.username,'已移除成员') AS username,r.model,j.status,
        origin.created_at AS createdAt,a.authorized_at AS startedAt,
        a.upstream_started_at AS upstreamStartedAt,a.upstream_completed_at AS upstreamCompletedAt,
        j.error_status AS errorStatus,c.status AS chargeStatus,
        CASE c.billing_mode WHEN 'anlas_fallback' THEN 'anlas' ELSE c.billing_mode END AS resource,
        c.v5 AS amount,c.anlas_fixed AS fixed,c.anlas_purchased AS purchased,
        EXISTS(SELECT 1 FROM domain_events cached WHERE cached.stream_id='request:'||r.id
          AND cached.event_type='CachedResultLinked' AND json_extract(cached.payload,'$.jobId')=j.id
          AND json_extract(cached.payload,'$.artifactId')=j.artifact_id) AS knownNoCharge
        FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id
        LEFT JOIN password_credentials p ON p.member_id=r.member_id
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
