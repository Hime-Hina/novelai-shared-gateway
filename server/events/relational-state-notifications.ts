import type Database from 'better-sqlite3';
import { EventEmitter } from 'node:events';
import { readRelationalCommit } from './relational-event-store.ts';
import { relationalFactStream } from './relational-fact-contracts.ts';
import type { RelationalEvent } from './relational-events.ts';
import { emitStoreEvent, type StateEvent } from './state-notifications.ts';
import { activityDate } from '../reports/activity-heatmap.ts';

type PrivateMemberState = {
  member: string;
  authorization: string;
  sessions: readonly {
    tokenHash: string;
    authEpoch: string;
    ownerBinding: string | null;
    expiresAt: number;
  }[];
};
type SessionCleanup = { memberId: string; authEpoch: string; createdAt: number };
export interface RelationalStateNotifications {
  readonly events: EventEmitter;
  commit<T>(write: () => T): T;
  trackMember(memberId: string, read: () => PrivateMemberState): void;
  markSessionCleanup(row: SessionCleanup): void;
}
const notifications = new WeakMap<Database.Database, RelationalStateNotifications>();

export function assertRelationalStateNotifications(
  sqlite: Database.Database,
  candidate: RelationalStateNotifications,
): void {
  if (notifications.get(sqlite) !== candidate) {
    throw new TypeError('State notifications must belong to the same runtime database.');
  }
}
const quiet = new Set<RelationalEvent['eventType']>([
  'RequestAliasRegistered',
  'InputRegistered',
  'ContextRegistered',
  'ImportSourceRegistered',
]);
const activity = new Set<RelationalEvent['eventType']>([
  'JobAccepted',
  'JobQueued',
  'DispatchAuthorized',
  'JobCompleted',
  'JobFailed',
  'CachedResultLinked',
  'WaitingJobsCancelled',
  'JobHistoryRetired',
]);

export function relationalStateNotificationsFor(
  sqlite: Database.Database,
  events: EventEmitter,
): RelationalStateNotifications {
  if (!(events instanceof EventEmitter)) {
    throw new TypeError('State notifications require the shared EventEmitter.');
  }
  const previous = notifications.get(sqlite);
  if (previous) {
    if (previous.events !== events) {
      throw new Error('One database requires one runtime state emitter.');
    }
    return previous;
  }
  let tracked: Map<string, { before: PrivateMemberState; read: () => PrivateMemberState }> | null =
    null;
  let sessionCleanup: SessionCleanup | null = null;
  const cut = () =>
    sqlite
      .prepare<[], { seq: string }>(
        'SELECT CAST(COALESCE(MAX(commit_seq),0) AS TEXT) AS seq FROM commits',
      )
      .get()!.seq;
  const describe = (before: string, after: string): StateEvent => {
    const collections = new Set<string>(),
      memberIds = new Set<string>();
    const dates = new Map<string, { memberId: string | null; date: string }>();
    let allMembers = false;
    const job = sqlite.prepare<[string], { memberId: string; createdAt: number | null }>(
      `SELECT r.member_id AS memberId,o.created_at AS createdAt FROM generation_jobs j
       JOIN generation_requests r ON r.id=j.request_id
       LEFT JOIN generation_job_origins o ON o.stream_id='request:'||r.id AND o.job_id=j.id AND o.request_id=j.request_id WHERE j.id=?`,
    );
    for (const row of sqlite
      .prepare<[string, string], { seq: string }>(
        'SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE commit_seq>CAST(? AS INTEGER) AND commit_seq<=CAST(? AS INTEGER) ORDER BY commit_seq',
      )
      .all(before, after)) {
      for (const event of readRelationalCommit(sqlite, row.seq).events) {
        if (quiet.has(event.eventType)) {
          continue;
        }
        const owner = relationalFactStream(event.eventType);
        if (event.memberId) {
          memberIds.add(event.memberId);
        }
        const references = event.payload as unknown as {
          memberId?: string;
          fromMemberId?: string;
          toMemberId?: string;
          allocations?: { memberId: string }[];
        };
        for (const id of [references.memberId, references.fromMemberId, references.toMemberId]) {
          if (id) {
            memberIds.add(id);
          }
        }
        for (const allocation of references.allocations ?? []) {
          memberIds.add(allocation.memberId);
        }
        if (owner === 'configuration' || owner === 'sharing') {
          collections.add('settings');
          allMembers = true;
        } else if (owner === 'member') {
          collections.add('users');
        } else if (owner === 'account') {
          collections.add('accounts');
          collections.add('ledger');
        } else if (owner === 'request') {
          collections.add('jobs');
          const payload = event.payload as unknown as { jobId?: string; jobIds?: string[] };
          const ids = payload.jobId ? [payload.jobId] : (payload.jobIds ?? []);
          for (const id of ids) {
            const original = job.get(id);
            if (original) {
              memberIds.add(original.memberId);
            } else {
              allMembers = true;
            }
            let date: string | null = null;
            if (original && original.createdAt !== null && activity.has(event.eventType)) {
              const at = new Date(original.createdAt);
              if (Number.isFinite(at.getTime())) {
                date = activityDate(at.toISOString());
              }
            }
            if (date) {
              const change = { memberId: original?.memberId ?? null, date };
              dates.set(JSON.stringify([change.memberId, date]), change);
            }
          }
        } else if (owner === 'artifact' || owner === 'storage') {
          collections.add('images');
        }
      }
    }
    for (const [memberId, change] of tracked!) {
      const after = change.read();
      if (change.before.member !== after.member) {
        collections.add('users');
        memberIds.add(memberId);
      }
      const sessions = new Map(after.sessions.map((session) => [session.tokenHash, session]));
      // 只比较原会话撤销；签发新增及原owner实际清理的过期/旧epoch行不构成撤销。
      const revoked = change.before.sessions.some((session) => {
        if (
          sessionCleanup &&
          (session.expiresAt <= sessionCleanup.createdAt ||
            (memberId === sessionCleanup.memberId &&
              session.authEpoch !== sessionCleanup.authEpoch))
        ) {
          return false;
        }
        const current = sessions.get(session.tokenHash);
        return (
          !current ||
          current.authEpoch !== session.authEpoch ||
          current.ownerBinding !== session.ownerBinding
        );
      });
      if (change.before.authorization !== after.authorization || revoked) {
        collections.add('sessions');
        memberIds.add(memberId);
      }
    }
    return {
      collections: [...collections],
      memberIds: [...memberIds],
      allMembers,
      activityChanges: [...dates.values()],
    };
  };
  const notifier: RelationalStateNotifications = Object.freeze({
    events,
    commit<T>(write: () => T): T {
      if (sqlite.inTransaction || tracked) {
        throw new Error('State notification requires the actual outer commit boundary.');
      }
      const before = cut();
      tracked = new Map();
      sessionCleanup = null;
      try {
        const result = write();
        if (
          sqlite.inTransaction ||
          (result && typeof (result as { then?: unknown }).then === 'function')
        ) {
          throw new Error('State notification requires a finished synchronous commit.');
        }
        try {
          const event = describe(before, cut());
          // 订阅者可立即重新鉴权；此时必须已离开本次收集上下文。
          tracked = null;
          sessionCleanup = null;
          if (event.collections!.length) {
            emitStoreEvent(
              events,
              'commit',
              Object.freeze({
                ...event,
                collections: Object.freeze(event.collections),
                memberIds: Object.freeze(event.memberIds),
                activityChanges: Object.freeze(
                  event.activityChanges!.map((change) => Object.freeze(change)),
                ),
              }),
            );
          }
        } catch {
          // 只隔离 COMMIT 后的通知；不能把已提交业务伪装成失败而重发。
          console.error('[runtime] Committed state notification failed.');
        }
        return result;
      } finally {
        tracked = null;
        sessionCleanup = null;
      }
    },
    trackMember(memberId: string, read: () => PrivateMemberState) {
      if (!tracked) {
        throw new Error('Private changes require the notified outer command boundary.');
      }
      if (!tracked.has(memberId)) {
        tracked.set(memberId, { before: read(), read });
      }
    },
    markSessionCleanup(row: SessionCleanup) {
      if (!tracked) {
        throw new Error('Session cleanup requires the notified outer command boundary.');
      }
      sessionCleanup = { ...row };
    },
  });
  notifications.set(sqlite, notifier);
  return notifier;
}
