import { randomUUID } from 'node:crypto';
import type Database from 'better-sqlite3';
import { canonicalJson, assertPublicDomainFact, type Json } from './domain-events.ts';
import {
  parseRelationalEvent,
  RELATIONAL_EVENT_SCHEMA_VERSION,
  type RelationalCommand,
  type RelationalEvent,
} from './relational-events.ts';
import {
  assertRelationalStorageFormat,
  RELATIONAL_REDUCER_VERSION,
} from '../storage/relational-storage.ts';

export interface RelationalCommitResult {
  commitSeq: string;
  receipt: Json;
  replayed: boolean;
  events: RelationalEvent[];
}

export function readRelationalReceipt(
  sqlite: Database.Database,
  command: Pick<RelationalCommand, 'commandId' | 'fingerprint' | 'actorKind' | 'actorId'>,
): RelationalCommitResult | null {
  if (
    typeof command.commandId !== 'string' ||
    !command.commandId ||
    !/^[a-f0-9]{64}$/.test(command.fingerprint)
  ) {
    throw new TypeError('Invalid relational command identity.');
  }
  const prior = sqlite
    .prepare<
      [string],
      {
        commit_seq: string;
        fingerprint: string;
        actor_kind: string;
        actor_id: string | null;
        receipt: string;
      }
    >(
      'SELECT CAST(commit_seq AS TEXT) AS commit_seq, fingerprint,actor_kind,actor_id,receipt FROM commits WHERE command_id=?',
    )
    .get(command.commandId);
  if (!prior) {
    return null;
  }
  if (
    prior.fingerprint !== command.fingerprint ||
    prior.actor_kind !== command.actorKind ||
    prior.actor_id !== command.actorId
  ) {
    throw Object.assign(new Error('Relational command idempotency conflict.'), {
      statusCode: 409,
      code: 'idempotency_conflict',
    });
  }
  const receipt = JSON.parse(prior.receipt) as Json;
  assertPublicDomainFact(receipt, 'receipt');
  return { commitSeq: prior.commit_seq, receipt, replayed: true, events: [] };
}

function assertCommand(command: RelationalCommand): void {
  if (
    !command ||
    typeof command.commandId !== 'string' ||
    !command.commandId ||
    !/^[a-f0-9]{64}$/.test(command.fingerprint) ||
    !['system', 'member', 'admin'].includes(command.actorKind) ||
    (command.actorId !== null && (typeof command.actorId !== 'string' || !command.actorId)) ||
    !Number.isSafeInteger(command.recordedAt) ||
    command.recordedAt < 0 ||
    !Array.isArray(command.events) ||
    !command.events.length
  ) {
    throw new TypeError('Invalid relational command.');
  }
  assertPublicDomainFact(command.receipt, 'receipt');
  if (Buffer.byteLength(canonicalJson(command.receipt)) > 4 * 1024 * 1024) {
    throw new RangeError('Relational receipt exceeds the payload limit.');
  }
  if (
    command.events.some(
      (event) =>
        event.eventType.startsWith('Opening') || event.eventType === 'ImportSourceRegistered',
    ) &&
    command.actorKind !== 'system'
  ) {
    throw new Error('Opening facts require the offline system importer.');
  }
}

function currentStreams(sqlite: Database.Database, command: RelationalCommand) {
  if (
    !command.expectedVersions ||
    typeof command.expectedVersions !== 'object' ||
    Array.isArray(command.expectedVersions)
  ) {
    throw new TypeError('Relational command requires explicit stream versions.');
  }
  const current = new Map<string, { version: string; kind: string }>();
  for (const input of command.events) {
    if (typeof input.streamId !== 'string') {
      throw new TypeError('Invalid relational stream.');
    }
    if (current.has(input.streamId)) {
      continue;
    }
    const head = sqlite
      .prepare<[string], { version: string; kind: string }>(
        'SELECT CAST(stream_version AS TEXT) AS version, stream_kind AS kind FROM stream_heads WHERE stream_id=?',
      )
      .get(input.streamId);
    const expected = command.expectedVersions[input.streamId];
    if (
      typeof expected !== 'string' ||
      !/^(0|[1-9][0-9]*)$/.test(expected) ||
      expected !== (head?.version || '0')
    ) {
      throw new Error('Relational stream version conflict.');
    }
    current.set(input.streamId, head || { version: '0', kind: input.streamId.split(':')[0] });
  }
  if (Object.keys(command.expectedVersions).some((streamId) => !current.has(streamId))) {
    throw new Error('Unexpected relational stream precondition.');
  }
  return current;
}

// 调用者先在同一事务重验授权，再追加事实和写投影/受保护内容；任一步失败全部回滚。
export function appendRelationalCommit(
  sqlite: Database.Database,
  command: RelationalCommand,
): RelationalCommitResult {
  if (!sqlite.inTransaction) {
    throw new Error('Relational facts require the owning command transaction.');
  }
  assertRelationalStorageFormat(sqlite);
  assertCommand(command);
  const prior = readRelationalReceipt(sqlite, command);
  if (prior) {
    return prior;
  }
  const current = currentStreams(sqlite, command);
  const last = sqlite
    .prepare<[], { value: string }>(
      'SELECT CAST(COALESCE(MAX(commit_seq),0) AS TEXT) AS value FROM commits',
    )
    .get()!.value;
  const commitSeq = String(BigInt(last) + 1n);
  const events = command.events.map((input, eventIndex) => {
    const head = current.get(input.streamId)!;
    const streamVersion = String(BigInt(head.version) + 1n);
    head.version = streamVersion;
    return parseRelationalEvent({
      ...input,
      eventId: input.eventId ?? randomUUID(),
      commitSeq,
      eventIndex,
      streamKind: head.kind,
      streamVersion,
      schemaVersion: RELATIONAL_EVENT_SCHEMA_VERSION,
      correlationId: command.correlationId,
      causationId: command.causationId,
      amountVersion: 'decimal-12-v1',
    });
  });
  return persistCommit(sqlite, command, commitSeq, events);
}

function persistCommit(
  sqlite: Database.Database,
  command: RelationalCommand,
  commitSeq: string,
  events: RelationalEvent[],
): RelationalCommitResult {
  return sqlite.transaction(() => {
    sqlite
      .prepare(
        'INSERT INTO commits(commit_seq,command_id,fingerprint,actor_kind,actor_id,recorded_at,event_count,receipt) VALUES(?,?,?,?,?,?,?,?)',
      )
      .run(
        BigInt(commitSeq),
        command.commandId,
        command.fingerprint,
        command.actorKind,
        command.actorId,
        command.recordedAt,
        events.length,
        canonicalJson(command.receipt),
      );
    const insert =
      sqlite.prepare(`INSERT INTO domain_events(event_id,commit_seq,event_index,stream_id,stream_kind,stream_version,
    event_type,schema_version,occurred_at,correlation_id,causation_id,account_id,member_id,policy_version,pricing_version,amount_version,payload)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
    const head =
      sqlite.prepare(`INSERT INTO stream_heads(stream_id,stream_kind,stream_version,last_commit_seq,last_event_id) VALUES(?,?,?,?,?)
    ON CONFLICT(stream_id) DO UPDATE SET stream_version=excluded.stream_version,last_commit_seq=excluded.last_commit_seq,last_event_id=excluded.last_event_id`);
    for (const event of events) {
      insert.run(
        event.eventId,
        BigInt(commitSeq),
        event.eventIndex,
        event.streamId,
        event.streamKind,
        BigInt(event.streamVersion),
        event.eventType,
        event.schemaVersion,
        event.occurredAt,
        event.correlationId,
        event.causationId,
        event.accountId,
        event.memberId,
        event.policyVersion,
        event.pricingVersion,
        event.amountVersion,
        canonicalJson(event.payload),
      );
      head.run(
        event.streamId,
        event.streamKind,
        BigInt(event.streamVersion),
        BigInt(commitSeq),
        event.eventId,
      );
    }
    // 游标和事实同事务推进，building 仍保持 building；ready 安装属于完整重建核对。
    const advanced = sqlite
      .prepare(
        `UPDATE projection_state SET last_commit_seq=? WHERE id=1 AND reducer_version=?
        AND status IN ('building','ready') AND last_commit_seq=?`,
      )
      .run(BigInt(commitSeq), RELATIONAL_REDUCER_VERSION, BigInt(commitSeq) - 1n);
    if (advanced.changes !== 1) {
      throw new Error('Relational projection is incomplete or incompatible.');
    }
    return { commitSeq, receipt: structuredClone(command.receipt), replayed: false, events };
  })();
}

export function readRelationalEvents(
  sqlite: Database.Database,
  commitSeq: string,
): RelationalEvent[] {
  if (!/^[1-9][0-9]*$/.test(commitSeq) || BigInt(commitSeq) > 9_223_372_036_854_775_807n) {
    throw new TypeError('Invalid relational commit cursor.');
  }
  return sqlite
    .prepare<[bigint], Record<string, unknown>>(
      `SELECT event_id,CAST(commit_seq AS TEXT) AS commit_seq,event_index,
    stream_id,stream_kind,CAST(stream_version AS TEXT) AS stream_version,event_type,schema_version,occurred_at,
    correlation_id,causation_id,account_id,member_id,policy_version,pricing_version,amount_version,payload
    FROM domain_events WHERE commit_seq=? ORDER BY event_index`,
    )
    .all(BigInt(commitSeq))
    .map((row) =>
      parseRelationalEvent({
        eventId: row.event_id,
        commitSeq: row.commit_seq,
        eventIndex: row.event_index,
        streamId: row.stream_id,
        streamKind: row.stream_kind,
        streamVersion: row.stream_version,
        eventType: row.event_type,
        schemaVersion: row.schema_version,
        occurredAt: row.occurred_at,
        correlationId: row.correlation_id,
        causationId: row.causation_id,
        accountId: row.account_id,
        memberId: row.member_id,
        policyVersion: row.policy_version,
        pricingVersion: row.pricing_version,
        amountVersion: row.amount_version,
        payload: JSON.parse(row.payload as string),
      }),
    );
}

export interface StoredRelationalCommit {
  historyId: string;
  eventCount: number;
  command: RelationalCommand;
  events: RelationalEvent[];
}
function eventInput(event: RelationalEvent): RelationalCommand['events'][number] {
  const {
    eventId: _id,
    commitSeq: _commit,
    eventIndex: _index,
    streamKind: _kind,
    streamVersion: _version,
    schemaVersion: _schema,
    correlationId: _correlation,
    causationId: _causation,
    amountVersion: _amount,
    ...input
  } = event;
  return input;
}

export function readRelationalCommit(
  sqlite: Database.Database,
  commitSeq: string,
): StoredRelationalCommit {
  assertRelationalStorageFormat(sqlite);
  const events = readRelationalEvents(sqlite, commitSeq);
  const row = sqlite
    .prepare<
      [bigint],
      {
        command_id: string;
        fingerprint: string;
        actor_kind: RelationalCommand['actorKind'];
        actor_id: string | null;
        recorded_at: number;
        event_count: number;
        receipt: string;
      }
    >(
      'SELECT command_id,fingerprint,actor_kind,actor_id,recorded_at,event_count,receipt FROM commits WHERE commit_seq=?',
    )
    .get(BigInt(commitSeq));
  if (
    !row ||
    events.length !== row.event_count ||
    events.some((event, index) => event.eventIndex !== index)
  ) {
    throw new Error('Incomplete relational source commit.');
  }
  const expectedVersions: Record<string, string> = {};
  for (const event of events) {
    expectedVersions[event.streamId] ??= String(BigInt(event.streamVersion) - 1n);
  }
  const history = sqlite
    .prepare<[], { history_id: string }>('SELECT history_id FROM projection_state WHERE id=1')
    .get();
  if (!history) {
    throw new Error('Missing relational history identity.');
  }
  const command: RelationalCommand = {
    commandId: row.command_id,
    fingerprint: row.fingerprint,
    actorKind: row.actor_kind,
    actorId: row.actor_id,
    recordedAt: row.recorded_at,
    expectedVersions,
    receipt: JSON.parse(row.receipt) as Json,
    correlationId: events[0].correlationId,
    causationId: events[0].causationId,
    events: events.map(eventInput),
  };
  assertCommand(command);
  return { historyId: history.history_id, eventCount: row.event_count, command, events };
}

// 重放只接受原完整提交；保留 ID/时间/回执及决定，不重新选择种子、报价或调用网络。
export function replayRelationalCommit(
  sqlite: Database.Database,
  stored: StoredRelationalCommit,
): RelationalCommitResult {
  if (!sqlite.inTransaction) {
    throw new Error('Relational replay requires the owning import transaction.');
  }
  assertRelationalStorageFormat(sqlite);
  assertCommand(stored.command);
  if (
    !Number.isSafeInteger(stored.eventCount) ||
    stored.eventCount < 1 ||
    stored.events.length !== stored.eventCount ||
    stored.command.events.length !== stored.eventCount
  ) {
    throw new Error('Relational replay requires one complete original commit.');
  }
  const events = stored.events.map(parseRelationalEvent);
  const state = sqlite
    .prepare<[], { history_id: string; status: string }>(
      'SELECT history_id,status FROM projection_state WHERE id=1',
    )
    .get();
  if (!state || state.history_id !== stored.historyId) {
    throw new Error('Relational replay history mismatch.');
  }
  const commitSeq = events[0]?.commitSeq;
  if (
    !commitSeq ||
    canonicalJson(events.map(eventInput)) !== canonicalJson(stored.command.events) ||
    events.some(
      (event, index) =>
        event.commitSeq !== commitSeq ||
        event.eventIndex !== index ||
        event.correlationId !== stored.command.correlationId ||
        event.causationId !== stored.command.causationId,
    )
  ) {
    throw new Error('Relational replay requires one complete original commit.');
  }
  const prior = readRelationalReceipt(sqlite, stored.command);
  if (prior) {
    if (
      prior.commitSeq !== commitSeq ||
      canonicalJson(readRelationalCommit(sqlite, commitSeq)) !== canonicalJson(stored)
    ) {
      throw new Error('Relational checkpoint differs from source facts.');
    }
    return prior;
  }
  if (state.status !== 'building') {
    throw new Error('New replay commits require a building target.');
  }
  const last = sqlite
    .prepare<[], { value: string }>(
      'SELECT CAST(COALESCE(MAX(commit_seq),0) AS TEXT) AS value FROM commits',
    )
    .get()!.value;
  if (String(BigInt(last) + 1n) !== commitSeq) {
    throw new Error('Relational replay commit gap.');
  }
  const heads = currentStreams(sqlite, stored.command);
  for (const event of events) {
    const head = heads.get(event.streamId)!;
    if (
      event.streamVersion !== String(BigInt(head.version) + 1n) ||
      event.streamKind !== head.kind
    ) {
      throw new Error('Relational replay stream gap.');
    }
    head.version = event.streamVersion;
  }
  return persistCommit(sqlite, stored.command, commitSeq, events);
}
