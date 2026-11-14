import type Database from 'better-sqlite3';
import {
  assertRelationalOpeningSource,
  readRelationalCommit,
} from '../events/relational-event-store.ts';

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
