import type Database from 'better-sqlite3';
import type { GenerationRequestState } from '../generation/request-queries.ts';
import { parseAmount, formatAmount } from '../quota/resource-amounts.ts';
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
  sampleIndex: number;
  createdAt: number | null;
  status: string;
  updatedAt: number;
  completedAt: number | null;
  cancellationReason: string | null;
  lateResultAt: number | null;
  errorCode: string | null;
  imageId: string | null;
  chargeStatus: string | null;
  billingMode: string | null;
  estimated: number | null;
  v5: string | null;
  fixed: string | null;
  purchased: string | null;
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
        `SELECT j.id,j.sample_index AS sampleIndex,origin.created_at AS createdAt,j.status,e.occurred_at AS updatedAt,
        j.completed_at AS completedAt,j.cancellation_reason AS cancellationReason,
        j.late_result_at AS lateResultAt,j.error_code AS errorCode,a.id AS imageId,
        c.status AS chargeStatus,c.billing_mode AS billingMode,c.estimated,c.v5,
        c.anlas_fixed AS fixed,c.anlas_purchased AS purchased
        FROM generation_jobs j JOIN domain_events e ON e.event_id=j.source_event_id
        LEFT JOIN generation_job_origins origin ON origin.stream_id='request:'||j.request_id
          AND origin.job_id=j.id AND origin.request_id=j.request_id
        LEFT JOIN charges c ON c.job_id=j.id
        LEFT JOIN artifacts a ON a.id=j.artifact_id AND a.member_id=? AND a.retired_at IS NULL
        WHERE j.request_id=? ORDER BY j.sample_index,j.id`,
      )
      .all(memberId, request.id);
    const createdAt = iso(request.acceptedAt)!;
    return {
      id: request.id,
      source: request.source,
      operation: request.operation,
      createdAt,
      jobs: rows.map((job) => {
        let fairCharge: GenerationRequestState['jobs'][number]['fairCharge'] = null;
        if (job.chargeStatus) {
          fairCharge = {
            status: job.chargeStatus,
            resource: job.billingMode === 'anlas_fallback' ? 'anlas' : job.billingMode!,
            amount:
              job.billingMode === 'v5'
                ? job.v5!
                : formatAmount(parseAmount(job.fixed!) + parseAmount(job.purchased!)),
            fixed: job.fixed!,
            purchased: job.purchased!,
            estimated: job.estimated === null ? null : job.estimated === 1,
          };
        }
        return {
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
          lateResultAvailable: job.lateResultAt !== null,
          imageId: job.imageId,
          fairCharge,
        };
      }),
    };
  }

  readMemberRequest(requestId: string, memberId: string): GenerationRequestState {
    return this.sqlite.transaction(() => {
      this.assertMember(memberId);
      const request = this.sqlite
        .prepare<[string, string], RequestRow>(
          `SELECT id,source,operation,model,accepted_at AS acceptedAt
          FROM generation_requests WHERE id=? AND member_id=?`,
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
          { status: string; artifactId: string | null; lateResultAt: number | null }
        >(
          `SELECT j.status,j.artifact_id AS artifactId,j.late_result_at AS lateResultAt
          FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id
          WHERE j.id=? AND r.member_id=?`,
        )
        .get(jobId, memberId);
      if (!job) {
        fail(404, 'job_not_found', '任务不存在。');
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
}
