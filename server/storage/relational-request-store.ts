import type Database from 'better-sqlite3';
import type { GenerationRequestState } from '../generation/request-queries.ts';
import { parseAmount, formatAmount } from '../quota/resource-amounts.ts';
import { memberCharge } from '../quota/failure-accounting.ts';
import { assertRelationalStorageFormat } from './relational-storage.ts';

const iso = (at: number | null) => (at === null ? null : new Date(at).toISOString());
function fail(statusCode: number, code: string, message: string): never {
  throw Object.assign(new Error(message), { statusCode, code });
}
interface RequestRow {
  id: string;
  source: string;
  operation: string;
  model: string | null;
  acceptedAt: number;
}
interface JobRow {
  id: string;
  requestId: string;
  model: string | null;
  sampleIndex: number;
  createdAt: number | null;
  status: string;
  updatedAt: number;
  completedAt: number | null;
  cancellationReason: string | null;
  lateResultAt: number | null;
  errorCode: string | null;
  imageId: string | null;
  chargeStatus: 'reserved' | 'settled' | 'refunded' | null;
  billingMode: 'free' | 'v5' | 'anlas' | 'anlas_fallback' | null;
  estimated: number | null;
  v5: string | null;
  fixed: string | null;
  purchased: string | null;
  v5Equivalent: string | null;
  lateResultAvailable: number;
}

export interface MemberJobPageOptions {
  page: number;
  pageSize: number;
  q: string;
  status: string;
  sort: string;
  order: string;
  until: string;
}

const jobFrom = `FROM generation_requests r JOIN generation_jobs j ON j.request_id=r.id
  JOIN domain_events e ON e.event_id=j.source_event_id
  LEFT JOIN generation_job_origins origin ON origin.stream_id='request:'||j.request_id
    AND origin.job_id=j.id AND origin.request_id=j.request_id
  LEFT JOIN charges c ON c.original_job_id=j.id AND c.original_member_id=r.member_id
  LEFT JOIN artifacts a ON a.id=j.artifact_id AND a.member_id=r.member_id
    AND a.retired_at IS NULL AND a.mime_type!='image/svg+xml'`;
// 迟到必须保留原退款与原找回事实；后来的 source_event_id 只表示当前投影来源。
const lateResultPredicate = `j.status='failed' AND j.late_result_at IS NOT NULL
  AND c.status='refunded' AND a.id IS NOT NULL AND EXISTS (
    SELECT 1 FROM domain_events late WHERE late.stream_id='request:'||j.request_id
      AND late.member_id=r.member_id AND json_extract(late.payload,'$.jobId')=j.id
      AND json_extract(late.payload,'$.artifactId')=a.id AND (
        (late.event_type='LateResultRecovered' AND late.occurred_at=j.late_result_at
          AND json_extract(late.payload,'$.refundRetained')=1) OR
        (late.event_type='OpeningJobRecorded' AND json_extract(late.payload,'$.status')='failed'
          AND json_extract(late.payload,'$.lateResultAt')=j.late_result_at)))`;
const jobColumns = `j.id,j.request_id AS requestId,r.model,j.sample_index AS sampleIndex,
  origin.created_at AS createdAt,j.status,e.occurred_at AS updatedAt,
  j.completed_at AS completedAt,j.cancellation_reason AS cancellationReason,
  j.late_result_at AS lateResultAt,j.error_code AS errorCode,a.id AS imageId,
  c.status AS chargeStatus,c.billing_mode AS billingMode,c.estimated,c.v5,
  c.anlas_fixed AS fixed,c.anlas_purchased AS purchased,c.v5_equivalent AS v5Equivalent,
  (${lateResultPredicate}) AS lateResultAvailable`;

function jobCharge(job: JobRow) {
  return job.chargeStatus
    ? {
        status: job.chargeStatus,
        resource: job.billingMode === 'anlas_fallback' ? 'anlas' : job.billingMode!,
        amount:
          job.billingMode === 'v5'
            ? job.v5!
            : formatAmount(parseAmount(job.fixed!) + parseAmount(job.purchased!)),
        fixed: job.fixed!,
        purchased: job.purchased!,
        estimated: job.estimated === null ? null : job.estimated === 1,
        billingMode: job.billingMode,
        v5Equivalent: job.v5Equivalent!,
      }
    : null;
}

function memberJob(job: JobRow) {
  return {
    id: job.id,
    requestId: job.requestId,
    createdAt: iso(job.createdAt),
    model: job.model,
    status: job.status,
    lateResultAvailable: job.lateResultAvailable === 1,
    imageId: job.imageId,
    charge: memberCharge(jobCharge(job)),
  };
}

// 只读取薄关系状态；完整输入和附件继续由 RelationalInputStore 按需校验。
export class RelationalRequestStore {
  private sqlite: Database.Database;
  constructor(sqlite: Database.Database) {
    assertRelationalStorageFormat(sqlite);
    this.sqlite = sqlite;
  }

  private assertMember(memberId: string): void {
    const member = this.sqlite
      .prepare<[string], { status: string; authEpoch: string | null }>(
        'SELECT status,auth_epoch AS authEpoch FROM members WHERE id=?',
      )
      .get(memberId);
    if (member?.status !== 'active' || !member.authEpoch) {
      fail(401, 'session_expired', '成员当前不可访问。');
    }
  }

  private requestState(request: RequestRow, memberId: string): GenerationRequestState {
    const rows = this.sqlite
      .prepare<[string, string], JobRow>(
        `SELECT ${jobColumns} ${jobFrom}
        WHERE r.member_id=? AND j.request_id=? AND j.retired_at IS NULL ORDER BY j.sample_index,j.id`,
      )
      .all(memberId, request.id);
    const createdAt = iso(request.acceptedAt)!;
    return {
      id: request.id,
      source: request.source,
      operation: request.operation,
      createdAt,
      jobs: rows.map((job) => ({
        id: job.id,
        sampleIndex: job.sampleIndex,
        status: job.status,
        model: request.model,
        createdAt: iso(job.createdAt),
        updatedAt: iso(job.updatedAt),
        completedAt: iso(job.completedAt),
        error: job.errorCode,
        errorCode: job.errorCode,
        cancelledAt: job.cancellationReason ? iso(job.completedAt) : null,
        lateResultAvailable: job.lateResultAvailable === 1,
        imageId: job.imageId,
        fairCharge: jobCharge(job),
      })),
    };
  }

  readMemberJobPage(memberId: string, options: MemberJobPageOptions) {
    const sort = { createdAt: 'origin.created_at', model: 'r.model', status: 'j.status' };
    if (
      !Number.isSafeInteger(options.page) ||
      options.page < 1 ||
      options.page > 1000000 ||
      !Number.isSafeInteger(options.pageSize) ||
      options.pageSize < 1 ||
      options.pageSize > 100 ||
      typeof options.q !== 'string' ||
      options.q.length > 200 ||
      !['', 'done', 'failed', 'running', 'queued', 'pending', 'late'].includes(options.status) ||
      typeof options.sort !== 'string' ||
      !Object.hasOwn(sort, options.sort) ||
      !['asc', 'desc'].includes(options.order) ||
      typeof options.until !== 'string' ||
      !Number.isFinite(Date.parse(options.until)) ||
      new Date(options.until).toISOString() !== options.until
    ) {
      fail(400, 'invalid_parameters', '历史查询参数无效。');
    }
    const params = {
      memberId,
      until: Date.parse(options.until),
      status: options.status,
      q: `%${options.q.replace(/[\\%_]/g, '\\$&')}%`,
      limit: options.pageSize,
      offset: (options.page - 1) * options.pageSize,
    };
    // 未知原创建时间不伪造为开账日，仍属于保留历史；SQLite 将 NULL 排在升序首/降序尾。
    const from = `${jobFrom} WHERE r.member_id=@memberId
      AND r.retired_at IS NULL AND j.retired_at IS NULL
      AND (origin.created_at IS NULL OR origin.created_at<=@until)
      AND (@status='' OR j.status=@status OR (@status='late' AND ${lateResultPredicate}))
      AND (j.id LIKE @q ESCAPE '\\' OR r.model LIKE @q ESCAPE '\\')`;
    return this.sqlite.transaction(() => {
      this.assertMember(memberId);
      const total = this.sqlite
        .prepare<typeof params, { total: number }>(`SELECT COUNT(*) AS total ${from}`)
        .get(params)!.total;
      const rows = this.sqlite
        .prepare<typeof params, JobRow>(
          `SELECT ${jobColumns} ${from}
        ORDER BY ${sort[options.sort as keyof typeof sort]} ${options.order === 'asc' ? 'ASC' : 'DESC'},
          j.id DESC LIMIT @limit OFFSET @offset`,
        )
        .all(params);
      return {
        total,
        page: options.page,
        pageSize: options.pageSize,
        until: options.until,
        jobs: rows.map(memberJob),
      };
    })();
  }

  readMemberLateResults(memberId: string) {
    return this.sqlite.transaction(() => {
      this.assertMember(memberId);
      return this.sqlite
        .prepare<[string], JobRow>(
          `SELECT ${jobColumns} ${jobFrom} WHERE r.member_id=?
        AND r.retired_at IS NULL AND j.retired_at IS NULL AND ${lateResultPredicate}
        ORDER BY j.late_result_at DESC,j.id DESC LIMIT 10`,
        )
        .all(memberId)
        .map((job) => ({ ...memberJob(job), lateResultAt: iso(job.lateResultAt)! }));
    })();
  }

  readMemberRequest(requestId: string, memberId: string): GenerationRequestState {
    return this.sqlite.transaction(() => {
      this.assertMember(memberId);
      const request = this.sqlite
        .prepare<[string, string], RequestRow>(
          `SELECT id,source,operation,model,accepted_at AS acceptedAt
          FROM generation_requests WHERE id=? AND member_id=? AND retired_at IS NULL`,
        )
        .get(requestId, memberId);
      if (!request) {
        fail(404, 'request_not_found', '请求不存在。');
      }
      return this.requestState(request, memberId);
    })();
  }

  readMemberRequests(
    memberId: string,
    limit = 20,
    before: { acceptedAt: number; id: string } | null = null,
  ) {
    if (
      !Number.isSafeInteger(limit) ||
      limit < 1 ||
      limit > 100 ||
      (before &&
        (!Number.isSafeInteger(before.acceptedAt) ||
          before.acceptedAt < 0 ||
          typeof before.id !== 'string' ||
          !before.id))
    ) {
      fail(400, 'invalid_parameters', '历史分页参数无效。');
    }
    return this.sqlite.transaction(() => {
      this.assertMember(memberId);
      const rows = this.sqlite
        .prepare<unknown[], RequestRow>(
          `SELECT id,source,operation,model,accepted_at AS acceptedAt
          FROM generation_requests WHERE member_id=? AND retired_at IS NULL
          ${before ? 'AND (accepted_at,id)<(?,?)' : ''}
          ORDER BY accepted_at DESC,id DESC LIMIT ?`,
        )
        .all(...(before ? [memberId, before.acceptedAt, before.id, limit] : [memberId, limit]));
      return {
        requests: rows.map((row) => this.requestState(row, memberId)),
        before:
          rows.length === limit
            ? { acceptedAt: rows.at(-1)!.acceptedAt, id: rows.at(-1)!.id }
            : null,
      };
    })();
  }

  readMemberResult(jobId: string, memberId: string) {
    return this.sqlite.transaction(() => {
      this.assertMember(memberId);
      const job = this.sqlite
        .prepare<
          [string, string],
          {
            status: string;
            artifactId: string | null;
            lateResultAt: number | null;
            retiredAt: number | null;
            requestRetiredAt: number | null;
          }
        >(
          `SELECT j.status,j.artifact_id AS artifactId,j.late_result_at AS lateResultAt,
          j.retired_at AS retiredAt,r.retired_at AS requestRetiredAt
          FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id
          WHERE j.id=? AND r.member_id=?`,
        )
        .get(jobId, memberId);
      if (!job) {
        fail(404, 'job_not_found', '任务不存在。');
      }
      if (job.retiredAt !== null || job.requestRetiredAt !== null) {
        fail(410, 'result_unavailable', '原结果已不可领取。');
      }
      if (!job.artifactId) {
        if (['pending', 'queued', 'running'].includes(job.status)) {
          fail(409, 'result_not_ready', '结果尚未完成。');
        }
        if (job.status !== 'done' && job.lateResultAt === null) {
          fail(409, 'generation_failed', '任务未产生可领取的结果。');
        }
      }
      const artifact = this.sqlite
        .prepare<
          [string | null, string],
          {
            artifactId: string;
            inputId: string;
            originalAttemptId: string | null;
            operation: string;
            mimeType: string;
            byteLength: number;
            sha256: string;
          }
        >(
          `SELECT id AS artifactId,input_id AS inputId,original_attempt_id AS originalAttemptId,
          operation,mime_type AS mimeType,byte_length AS byteLength,sha256
          FROM artifacts WHERE id=? AND member_id=? AND retired_at IS NULL`,
        )
        .get(job.artifactId, memberId);
      if (!artifact || artifact.mimeType === 'image/svg+xml') {
        fail(410, 'result_unavailable', '原结果已不可领取。');
      }
      return artifact;
    })();
  }

  readMemberJobDelivery(jobId: string, memberId: string) {
    return this.sqlite.transaction(() => {
      this.assertMember(memberId);
      const job = this.sqlite
        .prepare<
          [string, string],
          {
            status: string;
            requestId: string;
            seed: number | null;
            artifactId: string | null;
            error: string | null;
            errorStatus: number | null;
            cooldownUntil: number | null;
            retiredAt: number | null;
            requestRetiredAt: number | null;
          }
        >(
          `SELECT j.status,j.request_id AS requestId,j.actual_seed AS seed,j.artifact_id AS artifactId,
        j.error_code AS error,j.error_status AS errorStatus,j.retired_at AS retiredAt,r.retired_at AS requestRetiredAt,
        (SELECT json_extract(e.payload,'$.cooldownUntil') FROM domain_events e
          WHERE e.event_type='CooldownChanged' AND e.stream_id='account:'||a.account_id
          AND json_extract(e.payload,'$.attemptId')=a.id ORDER BY e.stream_version DESC LIMIT 1) AS cooldownUntil
        FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id
        LEFT JOIN generation_attempts a ON a.job_id=j.id WHERE j.id=? AND r.member_id=?`,
        )
        .get(jobId, memberId);
      if (!job) {
        fail(404, 'job_not_found', '任务不存在。');
      }
      if (job.retiredAt !== null || job.requestRetiredAt !== null) {
        fail(410, 'result_unavailable', '原结果已不可领取。');
      }
      return job;
    })();
  }
}
