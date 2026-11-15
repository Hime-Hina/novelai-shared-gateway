import type Database from 'better-sqlite3';
import { canonicalJson, domainDigest } from '../events/domain-events.ts';
import { readRelationalCommit } from '../events/relational-event-store.ts';
import type { RelationalEvent } from '../events/relational-events.ts';

export type HistoryRetirementControls = Pick<ReadonlyMap<string, unknown>, 'has'>;
type RetirementKind = 'jobs' | 'artifacts';
export interface HistoryJob {
  id: string;
  requestId: string;
  memberId: string;
  status: string;
  artifactId: string | null;
  retiredAt: number | null;
  resultPending: number;
  reviewStatus: string | null;
}
function fail(message: string, statusCode = 409): never {
  throw Object.assign(new Error(message), { statusCode });
}
export function assertHistoryRetirementControls(controls: HistoryRetirementControls) {
  if (!controls || typeof controls.has !== 'function') {
    fail('退休需要当前物理执行占用', 503);
  }
}

// 原旧存储条件只维护一次；新关系把尚未发布的准备和核对映射到同一窄视图。
export function canRetireJobHistory(job: {
  status?: string | null;
  resultPending?: unknown;
  generationReview?: { status?: string | null } | null;
}): boolean {
  return (
    !!job.status &&
    !['queued', 'pending', 'running'].includes(job.status) &&
    !job.resultPending &&
    job.generationReview?.status !== 'pending'
  );
}

const jobs = `SELECT j.id,j.request_id AS requestId,r.member_id AS memberId,j.status,
  j.artifact_id AS artifactId,j.retired_at AS retiredAt,
  (a.prepared_artifact_id IS NOT NULL AND j.artifact_id IS NULL) AS resultPending,
  a.review_status AS reviewStatus FROM generation_jobs j
  JOIN generation_requests r ON r.id=j.request_id
  LEFT JOIN generation_attempts a ON a.job_id=j.id`;

export function readHistoryJob(sqlite: Database.Database, jobId: string): HistoryJob {
  const job = sqlite.prepare<[string], HistoryJob>(`${jobs} WHERE j.id=?`).get(jobId);
  if (!job) {
    fail('退休任务不存在', 404);
  }
  return job;
}

export function assertRetirableRequest(
  sqlite: Database.Database,
  requestId: string,
  controls?: HistoryRetirementControls,
) {
  const rows = sqlite
    .prepare<[string], HistoryJob>(`${jobs} WHERE j.request_id=? ORDER BY j.sample_index,j.id`)
    .all(requestId);
  if (!rows.length) {
    fail('退休请求不存在', 404);
  }
  for (const row of rows) {
    if (
      !canRetireJobHistory({
        status: row.status,
        resultPending: row.resultPending,
        generationReview: { status: row.reviewStatus },
      }) ||
      controls?.has(row.id)
    ) {
      fail('不能退休活动任务、未发布准备结果、未决核对或仍占用物理执行的历史');
    }
  }
}

export function assertRetirableArtifact(
  sqlite: Database.Database,
  artifactId: string,
  controls?: HistoryRetirementControls,
) {
  const requests = sqlite
    .prepare<[string, string, string], { requestId: string }>(
      `SELECT request_id AS requestId FROM generation_jobs WHERE artifact_id=?
       UNION ALL SELECT j.request_id FROM generation_attempts a
         JOIN generation_jobs j ON j.id=a.job_id
         WHERE a.prepared_artifact_id=? AND j.artifact_id IS NULL
       UNION ALL SELECT j.request_id FROM artifacts f
         JOIN generation_attempts a ON a.id=f.original_attempt_id
         JOIN generation_jobs j ON j.id=a.job_id WHERE f.id=?`,
    )
    .iterate(artifactId, artifactId, artifactId);
  const seen = new Set<string>();
  for (const request of requests) {
    if (seen.has(request.requestId)) {
      continue;
    }
    seen.add(request.requestId);
    assertRetirableRequest(sqlite, request.requestId, controls);
  }
}

export function historyRetirementIds(value: unknown): string[] {
  if (
    !Array.isArray(value) ||
    value.length < 1 ||
    value.length > 200 ||
    value.some((id) => typeof id !== 'string' || !id) ||
    new Set(value).size !== value.length
  ) {
    fail('人工退休需要 1–200 个不重复的原 ID', 400);
  }
  return [...value].sort();
}

export function historyRetirementFingerprint(kind: RetirementKind, ids: string[]) {
  return domainDigest({ action: `retire-${kind}`, ids });
}

// 选择是该命令的全部语义，直接核对完整指纹；原事实和回执一起删改也不能缩小集合。
export function assertHistoryRetirementCommit(
  sqlite: Database.Database,
  event: RelationalEvent,
  kind: RetirementKind,
) {
  const stored = readRelationalCommit(sqlite, event.commitSeq),
    key = kind === 'jobs' ? 'retiredJobIds' : 'retiredArtifactIds',
    receipt = stored.command.receipt as Record<string, unknown> | null;
  if (!receipt || Array.isArray(receipt) || Object.keys(receipt).join() !== key) {
    fail('退休必须保留完整原回执');
  }
  const ids = historyRetirementIds(receipt[key]);
  if (
    canonicalJson(receipt[key]) !== canonicalJson(ids) ||
    stored.command.fingerprint !== historyRetirementFingerprint(kind, ids)
  ) {
    fail('退休选择与原指纹不符');
  }
  const expected: {
    streamId: string;
    memberId: string;
    payload: unknown;
  }[] = [];
  if (kind === 'jobs') {
    const groups = new Map<string, { memberId: string; ids: string[] }>();
    for (const id of ids) {
      const job = readHistoryJob(sqlite, id),
        group = groups.get(job.requestId) ?? { memberId: job.memberId, ids: [] };
      group.ids.push(id);
      groups.set(job.requestId, group);
    }
    for (const requestId of [...groups.keys()].sort()) {
      const group = groups.get(requestId)!;
      expected.push({
        streamId: `request:${requestId}`,
        memberId: group.memberId,
        payload: { jobIds: group.ids, reason: 'owner_cleared_history' },
      });
    }
  } else {
    for (const artifactId of ids) {
      const row = sqlite
        .prepare<[string], { memberId: string }>(
          'SELECT member_id AS memberId FROM artifacts WHERE id=?',
        )
        .get(artifactId);
      if (!row) {
        fail('退休产物不存在', 404);
      }
      expected.push({
        streamId: `artifact:${artifactId}`,
        memberId: row.memberId,
        payload: { artifactId, reason: 'owner_cleared_cache' },
      });
    }
  }
  if (
    stored.events.length !== expected.length ||
    !stored.events.some((entry) => canonicalJson(entry) === canonicalJson(event)) ||
    stored.events.some((entry, index) => {
      const target = expected[index];
      return (
        entry.eventType !== (kind === 'jobs' ? 'JobHistoryRetired' : 'ArtifactHistoryRetired') ||
        entry.streamId !== target.streamId ||
        entry.memberId !== target.memberId ||
        entry.accountId !== null ||
        entry.occurredAt !== stored.command.recordedAt ||
        canonicalJson(entry.payload) !== canonicalJson(target.payload)
      );
    })
  ) {
    fail('退休事实的完整集合、归属或原序不符');
  }
}
