import type Database from 'better-sqlite3';
import { parseRelationalEvent } from '../events/relational-events.ts';
import {
  assertRelationalOpeningSource,
  readRelationalCommit,
} from '../events/relational-event-store.ts';

// 薄查询只展开唯一原请求元数据，不读取整笔 child 或受保护 input/context。
export function readRelationalRequestOrigin(sqlite: Database.Database, requestId: string) {
  const rows = sqlite
    .prepare<[string, string], Record<string, unknown>>(
      `SELECT o.original_request_id AS originalRequestId,e.event_id AS eventId,
      CAST(e.commit_seq AS TEXT) AS commitSeq,e.event_index AS eventIndex,
      e.stream_id AS streamId,e.stream_kind AS streamKind,CAST(e.stream_version AS TEXT) AS streamVersion,
      e.event_type AS eventType,e.schema_version AS schemaVersion,e.occurred_at AS occurredAt,
      e.correlation_id AS correlationId,e.causation_id AS causationId,e.member_id AS memberId,
      e.account_id AS accountId,e.policy_version AS policyVersion,e.pricing_version AS pricingVersion,
      e.amount_version AS amountVersion,e.payload
      FROM generation_request_origins o JOIN domain_events e ON e.event_id=o.event_id
      WHERE o.stream_id=? AND o.request_id=? LIMIT 2`,
    )
    .all(`request:${requestId}`, requestId);
  if (rows.length !== 1) {
    throw new Error('请求缺少唯一原接受来源。');
  }
  const { originalRequestId, ...row } = rows[0]!;
  const event = parseRelationalEvent({ ...row, payload: JSON.parse(row.payload as string) });
  if (
    (event.eventType !== 'RequestAccepted' && event.eventType !== 'OpeningRequestRecorded') ||
    event.streamId !== `request:${requestId}` ||
    event.payload.requestId !== requestId ||
    event.accountId !== null ||
    originalRequestId !==
      (event.eventType === 'RequestAccepted' ? requestId : event.payload.originalRequestId)
  ) {
    throw new Error('请求原parent来源与结构容器不符。');
  }
  if (event.eventType === 'OpeningRequestRecorded') {
    assertRelationalOpeningSource(sqlite, event);
  }
  return event;
}

// Opening 是当前 cut 的来源；原归档 loss/closure 身份不能借同号新事件充当授权或核对事实。
export function readRelationalOpeningAttempt(sqlite: Database.Database, attemptId: string) {
  const role = sqlite
    .prepare<
      [string],
      {
        jobId: string;
        accountId: string;
        memberId: string;
        requestId: string;
        epoch: number | null;
      }
    >(
      `SELECT a.job_id AS jobId,a.account_id AS accountId,a.credential_epoch AS epoch,
    r.member_id AS memberId,j.request_id AS requestId FROM generation_attempts a
    JOIN generation_jobs j ON j.id=a.job_id JOIN generation_requests r ON r.id=j.request_id WHERE a.id=?`,
    )
    .get(attemptId);
  if (!role) {
    return null;
  }
  const rows = sqlite
    .prepare<[string, string], { eventId: string; seq: string }>(
      `SELECT event_id AS eventId,CAST(commit_seq AS TEXT) AS seq FROM domain_events
    WHERE stream_id=? AND event_type='OpeningAttemptRecorded' AND json_extract(payload,'$.attemptId')=? LIMIT 2`,
    )
    .all(`request:${role.requestId}`, attemptId);
  if (!rows.length) {
    return null;
  }
  const event = readRelationalCommit(sqlite, rows[0].seq).events.find(
    (e) => e.eventId === rows[0].eventId,
  );
  if (
    rows.length !== 1 ||
    event?.eventType !== 'OpeningAttemptRecorded' ||
    event.payload.attemptId !== attemptId ||
    event.payload.jobId !== role.jobId ||
    event.payload.accountId !== role.accountId ||
    event.payload.credentialEpoch !== role.epoch ||
    event.accountId !== role.accountId ||
    event.memberId !== role.memberId ||
    event.streamId !== `request:${role.requestId}`
  ) {
    throw new Error('开账尝试来源与原任务/成员/账户角色不符。');
  }
  assertRelationalOpeningSource(sqlite, event);
  return event;
}
