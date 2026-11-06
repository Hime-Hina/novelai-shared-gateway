import type Database from 'better-sqlite3';
import { createHash, randomUUID } from 'node:crypto';
import {
  assertDomainEvent,
  assertPublicDomainFact,
  canonicalJson,
  domainDigest,
  DOMAIN_AMOUNT_VERSION,
  DOMAIN_REDUCER_VERSION,
  type DomainCommand,
  type DomainReplayCommand,
  type DomainCommit,
  type DomainEvent,
  type DomainEventInput,
  type Json,
} from './domain-events.ts';
import {
  reduceSharingEvent,
  reduceAccountQuotaEvent,
  accountQuotaEventTypes,
  splitQuotaState,
  composeQuotaState,
  type SharingQuotaState,
  type AccountQuotaState,
} from './quota-projections.ts';
import {
  normalizeOpeningQuota,
  type FairQuotaState,
  type OpeningAmountGroupReport,
} from '../quota/fair-share.ts';
import { parseAmount } from '../quota/resource-amounts.ts';
import {
  readCompatibleEventAuthority,
  SUPPORTED_EVENT_AUTHORITY_VERSION,
} from '../storage/sqlite-migrations.ts';

export type DomainProjection = Record<string, unknown> | null;
export interface StreamProjection {
  streamId: string;
  streamKind: string;
  streamVersion: string;
  lastCommitSeq: string;
  lastEventId: string;
  reducerVersion: number;
  projection: DomainProjection;
  projectionSha256: string;
}
export interface AppendOptions {
  project?: (sqlite: Database.Database, commit: DomainCommit, streams: StreamProjection[]) => void;
}
export interface ProjectionRow {
  collection: 'jobs' | 'images' | 'accounts' | 'users' | 'ledger';
  item: Record<string, unknown>;
  order?: number;
  remove?: true;
}
export interface CoreProjection {
  rows: ProjectionRow[];
  fairQuota?: FairQuotaState | null;
  queueSequence?: string;
}
export const EVENT_AUTHORITY_VERSION = SUPPORTED_EVENT_AUTHORITY_VERSION;
export function removeLegacyFreeImageTriggers(sqlite: Database.Database): void {
  sqlite.exec(
    'DROP TRIGGER IF EXISTS free_image_settled; DROP TRIGGER IF EXISTS free_image_recovered_insert; DROP TRIGGER IF EXISTS free_image_recovered_update; DROP TRIGGER IF EXISTS free_image_warning;',
  );
}
export function eventAuthorityEnabled(sqlite: Database.Database): boolean {
  return readCompatibleEventAuthority(sqlite) !== null;
}
interface CommitRow {
  commit_seq: string;
  command_id: string;
  fingerprint: string;
  recorded_at: string;
  event_count: number;
  receipt: string;
}
interface EventRow {
  event_id: string;
  commit_seq: string;
  event_index: number;
  stream_id: string;
  stream_kind: string;
  stream_version: string;
  event_type: DomainEvent['eventType'];
  schema_version: number;
  occurred_at: string;
  recorded_at: string;
  actor_id: string | null;
  actor_kind: DomainEvent['actorKind'];
  correlation_id: string | null;
  causation_id: string | null;
  account_id: string | null;
  member_id: string | null;
  policy_version: string;
  pricing_version: string;
  amount_version: string;
  payload: string;
}
const maxSequence = 9_223_372_036_854_775_807n;
const iso = (value: string | number): string => {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime()))
    throw new Error('Domain event time must be an explicit valid timestamp.');
  return date.toISOString();
};
const object = (value: unknown): Record<string, unknown> => {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('Domain projection expects an object fact.');
  return value as Record<string, unknown>;
};
const text = (value: unknown, field: string): string => {
  if (typeof value !== 'string' || !value) throw new Error(`Domain fact requires ${field}.`);
  return value;
};

export function readStreamProjection(
  sqlite: Database.Database,
  streamId: string,
): StreamProjection | null {
  const row = sqlite
    .prepare(
      `SELECT stream_id,stream_kind,CAST(stream_version AS TEXT) AS stream_version,
    CAST(last_commit_seq AS TEXT) AS last_commit_seq,last_event_id,reducer_version,projection,projection_sha256
    FROM event_streams WHERE stream_id=?`,
    )
    .get(streamId) as
    | {
        stream_id: string;
        stream_kind: string;
        stream_version: string;
        last_commit_seq: string;
        last_event_id: string;
        reducer_version: number;
        projection: string;
        projection_sha256: string;
      }
    | undefined;
  if (!row) return null;
  if (row.reducer_version !== DOMAIN_REDUCER_VERSION)
    throw new Error(`Unsupported reducer version for ${streamId}: ${row.reducer_version}`);
  const projection = JSON.parse(row.projection) as DomainProjection;
  if (domainDigest(projection) !== row.projection_sha256)
    throw new Error(`Corrupt domain projection ${streamId}`);
  return {
    streamId: row.stream_id,
    streamKind: row.stream_kind,
    streamVersion: row.stream_version,
    lastCommitSeq: row.last_commit_seq,
    lastEventId: row.last_event_id,
    reducerVersion: row.reducer_version,
    projection,
    projectionSha256: row.projection_sha256,
  };
}

export function readProjectedFairQuota(sqlite: Database.Database): FairQuotaState | null {
  const sharing = readStreamProjection(sqlite, 'sharing:default')?.projection as
    SharingQuotaState | null | undefined;
  if (!sharing) return null;
  const account = sharing.boundAccountId
    ? readStreamProjection(sqlite, `account:${sharing.boundAccountId}`)?.projection
    : null;
  if (sharing.boundAccountId && !account?.quota)
    throw new Error(`Missing authoritative quota account ${sharing.boundAccountId}`);
  return composeQuotaState(sharing, (account?.quota as AccountQuotaState | null) || null);
}

export function readArtifactInput(
  sqlite: Database.Database,
  id: string,
  expectedHash?: string,
): Record<string, unknown> | null {
  const row = sqlite
    .prepare('SELECT payload,payload_sha256 FROM artifact_inputs WHERE artifact_id=?')
    .get(id) as { payload: string; payload_sha256: string } | undefined;
  if (!row) return null;
  const payload = JSON.parse(row.payload) as Record<string, unknown>,
    digest = domainDigest(payload);
  if (digest !== row.payload_sha256 || (expectedHash && digest !== expectedHash))
    throw new Error(`Protected artifact input hash mismatch ${id}`);
  return payload;
}
function projectionArtifact(
  sqlite: Database.Database,
  value: Record<string, unknown>,
): Record<string, unknown> {
  const artifact = structuredClone(value);
  if (artifact.artifactInputRef) {
    const input = readArtifactInput(
      sqlite,
      String(artifact.artifactInputRef),
      artifact.artifactInputSha256 as string,
    );
    if (!input) throw new Error(`Missing protected artifact input ${artifact.artifactInputRef}`);
    Object.assign(artifact, pick(input, ['prompt', 'fullPrompt']));
  } else if (artifact.inputRef) {
    let inputRef = String(artifact.inputRef);
    if (inputRef.startsWith('legacy-')) {
      const request = readStreamProjection(sqlite, `request:${inputRef}`)?.projection;
      inputRef = String(
        (request?.request as Record<string, unknown> | undefined)?.inputRef || inputRef,
      );
    }
    const row = sqlite
      .prepare('SELECT payload FROM generation_requests WHERE id=?')
      .get(inputRef) as { payload: string } | undefined;
    if (row) {
      const input = JSON.parse(row.payload) as Record<string, unknown>,
        request = input.legacyInline ? object(input.requestPayload || input.request) : input;
      if (request.tag !== undefined) artifact.prompt = request.tag;
      if (request.prompt !== undefined) artifact.fullPrompt = request.prompt;
    }
  }
  return artifact;
}

const jobDateFields = [
  'createdAt',
  'updatedAt',
  'startedAt',
  'completedAt',
  'cancelledAt',
  'queueExpiresAt',
  'queuedAt',
  'deadlineAt',
  'lateResultAt',
];
function projectionJob(
  sqlite: Database.Database,
  raw: Record<string, unknown>,
): Record<string, unknown> {
  const job = structuredClone(raw);
  delete job.summary;
  delete job.index;
  for (const key of jobDateFields)
    if (typeof job[key] === 'number') job[key] = iso(job[key] as number);
  const terminal = sqlite
    .prepare('SELECT charge FROM fair_quota_settlements WHERE job_id=?')
    .get(job.id) as { charge: string } | undefined;
  const accountId =
    job.accountId || (job.generationAttempt as Record<string, unknown> | undefined)?.accountId;
  const account = accountId
    ? readStreamProjection(sqlite, `account:${accountId}`)?.projection
    : null;
  const reserved = (account?.quota as AccountQuotaState | undefined)?.reservations[String(job.id)];
  if (terminal) job.fairCharge = JSON.parse(terminal.charge);
  else if (reserved) job.fairCharge = structuredClone(reserved);
  if (job.legacyInputRef) {
    const input = sqlite
      .prepare('SELECT payload FROM generation_requests WHERE id=?')
      .get(job.legacyInputRef) as { payload: string } | undefined;
    if (!input) throw new Error(`Missing protected legacy input ${job.legacyInputRef}`);
    const payload = JSON.parse(input.payload) as Record<string, unknown>;
    if (payload.request) job.request = payload.request;
    if (payload.requestPayload) job.requestPayload = payload.requestPayload;
  }
  if (job.diagnosticRef) {
    const diagnostic = sqlite
      .prepare(
        'SELECT job_id,detail,detail_sha256 FROM generation_diagnostics WHERE diagnostic_id=?',
      )
      .get(job.diagnosticRef) as
      { job_id: string; detail: string; detail_sha256: string } | undefined;
    if (
      !diagnostic ||
      diagnostic.job_id !== job.id ||
      createHash('sha256').update(diagnostic.detail).digest('hex') !== diagnostic.detail_sha256
    )
      throw new Error(`Missing or corrupt protected diagnostic ${job.diagnosticRef}`);
    job.errorDetail = diagnostic.detail;
  }
  const imageId = job.imageId || job.lateResultImageId;
  if (imageId && readStreamProjection(sqlite, `artifact:${imageId}`)?.projection?.retired) {
    job.imageId = '';
    if (job.lateResultImageId) job.lateResultImageId = '';
    if (job.lateResultAvailable) job.lateResultAvailable = false;
  }
  return job;
}

// 与在线提交、离线重放共用；只把事件事实投影为现有查询表，不执行领域决定。
export function coreProjectionRows(
  sqlite: Database.Database,
  commit: DomainCommit,
  streams: StreamProjection[],
): CoreProjection {
  const rows: ProjectionRow[] = [],
    seenRequests = new Set<string>(),
    requests = [...streams];
  for (const event of commit.events) {
    const relatedIds =
      event.eventType === 'ArtifactHistoryRetired'
        ? (event.payload.jobIds as string[])
        : event.streamKind === 'account' && event.payload.jobId
          ? [String(event.payload.jobId)]
          : [];
    for (const jobId of relatedIds) {
      const stored = sqlite.prepare('SELECT request_id FROM jobs WHERE id=?').get(jobId) as
        { request_id: string | null } | undefined;
      const streamId = `request:${stored?.request_id || `legacy-${jobId}`}`;
      if (!requests.some((stream) => stream.streamId === streamId)) {
        const request = readStreamProjection(sqlite, streamId);
        if (request) requests.push(request);
      }
    }
    if (Array.isArray(event.payload.evidence))
      for (const evidence of event.payload.evidence)
        rows.push({ collection: 'ledger', item: structuredClone(object(evidence)) });
  }
  let queueSequence: string | undefined;
  for (const stream of requests) {
    const state = stream.projection;
    if (!state) continue;
    if (stream.streamKind === 'request') {
      if (seenRequests.has(stream.streamId)) continue;
      seenRequests.add(stream.streamId);
      for (const raw of Object.values(object(state.jobs))) {
        const id = String(object(raw).id);
        if (object(state.retiredJobs || {})[id]) {
          rows.push({ collection: 'jobs', item: { id }, remove: true });
          continue;
        }
        const job = projectionJob(sqlite, object(raw));
        rows.push({ collection: 'jobs', item: job });
        if (
          typeof job.queueSeq === 'number' &&
          (!queueSequence || BigInt(job.queueSeq) > BigInt(queueSequence))
        )
          queueSequence = String(job.queueSeq);
      }
      for (const raw of Object.values(object(state.artifacts || {}))) {
        const artifact = structuredClone(object(raw)),
          storage = readStreamProjection(sqlite, `artifact:${artifact.id}`)?.projection;
        if (storage?.retired) {
          rows.push({
            collection: 'images',
            item: { id: artifact.id, memberId: artifact.memberId },
            remove: true,
          });
          continue;
        }
        if (storage)
          Object.assign(
            artifact,
            pick(storage, [
              'file',
              'mimeType',
              'sha256',
              'byteLength',
              'r2',
              'r2LastAttemptAt',
              'r2Error',
              'storagePhase',
            ]),
          );
        rows.push({ collection: 'images', item: projectionArtifact(sqlite, artifact) });
      }
    } else if (stream.streamKind === 'account') {
      const { quota, lastEventId, ...account } = state;
      rows.push({ collection: 'accounts', item: account });
    } else if (stream.streamKind === 'sharing') {
      for (const [id, value] of Object.entries(object(state.memberEligibility || {}))) {
        const member = object(value);
        rows.push({
          collection: 'users',
          item: {
            id,
            ...pick(member, [
              'enabled',
              'acceptingJobs',
              'jobAdmissionVersion',
              'discord',
              'createdAt',
            ]),
          },
          ...(member.removed ? { remove: true as const } : {}),
        });
      }
    } else if (stream.streamKind === 'artifact')
      rows.push(
        state.retired
          ? { collection: 'images', item: { id: state.id, memberId: state.memberId }, remove: true }
          : { collection: 'images', item: projectionArtifact(sqlite, state) },
      );
    else if (stream.streamKind === 'legacy-ledger')
      rows.push({
        collection: 'ledger',
        item: structuredClone(object(state.record)),
        order: state.order as number,
      });
    else if (stream.streamKind === 'system' && stream.streamId === 'system:queue')
      queueSequence = String(state.queueSequence);
  }
  const cleared = commit.events.filter(
    (event) =>
      event.eventType === 'ArtifactHistoryRetired' &&
      event.payload.reason === 'owner_cleared_cache',
  );
  if (cleared.length)
    rows.push({
      collection: 'ledger',
      item: {
        id: `cache-retired:${commit.commandId}`,
        type: 'clear-cache',
        amount: cleared.length,
        at: commit.recordedAt,
        note: 'Cleared cached images',
      },
    });
  const fairQuota = streams.some((stream) => ['sharing', 'account'].includes(stream.streamKind))
    ? readProjectedFairQuota(sqlite)
    : undefined;
  return {
    rows,
    ...(fairQuota !== undefined ? { fairQuota } : {}),
    ...(queueSequence ? { queueSequence } : {}),
  };
}

export function readCommandReceipt(
  sqlite: Database.Database,
  commandId: string,
): { commitSeq: string; fingerprint: string; receipt: Json } | null {
  const row = sqlite
    .prepare(
      'SELECT CAST(commit_seq AS TEXT) AS commit_seq,fingerprint,receipt FROM event_commits WHERE command_id=?',
    )
    .get(commandId) as Pick<CommitRow, 'commit_seq' | 'fingerprint' | 'receipt'> | undefined;
  return row
    ? {
        commitSeq: row.commit_seq,
        fingerprint: row.fingerprint,
        receipt: JSON.parse(row.receipt) as Json,
      }
    : null;
}

function eventFromRow(row: EventRow, commandId: string): DomainEvent {
  const event: DomainEvent = {
    eventId: row.event_id,
    commitSeq: row.commit_seq,
    eventIndex: row.event_index,
    commandId,
    transactionId: commandId,
    streamId: row.stream_id,
    streamKind: row.stream_kind,
    streamVersion: row.stream_version,
    eventType: row.event_type,
    schemaVersion: row.schema_version,
    occurredAt: row.occurred_at,
    recordedAt: row.recorded_at,
    actorId: row.actor_id,
    actorKind: row.actor_kind,
    correlationId: row.correlation_id,
    causationId: row.causation_id,
    accountId: row.account_id,
    memberId: row.member_id,
    policyVersion: row.policy_version,
    pricingVersion: row.pricing_version,
    amountVersion: row.amount_version,
    payload: JSON.parse(row.payload),
  };
  assertDomainEvent(event);
  return event;
}

export function readDomainCommits(
  sqlite: Database.Database,
  afterCommitSeq = '0',
  limit = 100,
): DomainCommit[] {
  if (!/^\d+$/.test(afterCommitSeq) || !Number.isInteger(limit) || limit < 1 || limit > 1000)
    throw new Error('Invalid event commit cursor.');
  const rows = sqlite
    .prepare(
      `SELECT CAST(commit_seq AS TEXT) AS commit_seq,command_id,fingerprint,recorded_at,event_count,receipt
    FROM event_commits WHERE event_commits.commit_seq > CAST(? AS INTEGER) ORDER BY event_commits.commit_seq LIMIT ?`,
    )
    .all(afterCommitSeq, limit) as CommitRow[];
  const selectEvents =
    sqlite.prepare(`SELECT *,CAST(commit_seq AS TEXT) AS commit_seq,CAST(stream_version AS TEXT) AS stream_version
    FROM domain_events WHERE commit_seq=CAST(? AS INTEGER) ORDER BY event_index`);
  return rows.map((row) => {
    const events = (selectEvents.all(row.commit_seq) as EventRow[]).map((item) =>
      eventFromRow(item, row.command_id),
    );
    if (
      events.length !== row.event_count ||
      events.some((event, index) => event.eventIndex !== index)
    )
      throw new Error(`Incomplete event commit ${row.commit_seq}`);
    return {
      commitSeq: row.commit_seq,
      commandId: row.command_id,
      fingerprint: row.fingerprint,
      recordedAt: row.recorded_at,
      receipt: JSON.parse(row.receipt) as Json,
      events,
    };
  });
}

export function assertCurrentEventReducer(sqlite: Database.Database): void {
  readCompatibleEventAuthority(sqlite);
  const checkpoint = sqlite
    .prepare("SELECT reducer_version FROM projection_versions WHERE name='core'")
    .get() as { reducer_version: number } | undefined;
  if (checkpoint && checkpoint.reducer_version !== DOMAIN_REDUCER_VERSION)
    throw new Error(
      `Unsupported reducer version: ${checkpoint.reducer_version}; upgrade event projections before use.`,
    );
}

// 只升级可重建的 stream cache；调用方在同一事务内核对/安装业务投影及 writer 标记。
// 不重插事件、收据或冷凭据，不以旧 cache 内容冒充新 reducer 的结果。
export function upgradeEventReducers(sqlite: Database.Database): boolean {
  if (!sqlite.inTransaction) throw new Error('Reducer upgrade requires a database transaction.');
  const authority = readCompatibleEventAuthority(sqlite);
  const versions = (
    sqlite
      .prepare(
        'SELECT DISTINCT reducer_version FROM event_streams UNION SELECT reducer_version FROM projection_versions',
      )
      .all() as { reducer_version: number }[]
  ).map((row) => row.reducer_version);
  for (const version of versions)
    if (![1, 2, DOMAIN_REDUCER_VERSION].includes(version))
      throw new Error(`Unsupported reducer version: ${version}`);
  if (
    (!authority || authority.version === EVENT_AUTHORITY_VERSION) &&
    versions.every((version) => version === DOMAIN_REDUCER_VERSION)
  )
    return false;
  const clock = sqlite
    .prepare('SELECT CAST(value AS TEXT) AS value FROM event_clock WHERE id=1')
    .get() as { value: string } | undefined;
  const checkpoint = sqlite
    .prepare(
      "SELECT CAST(last_commit_seq AS TEXT) AS value FROM projection_versions WHERE name='core'",
    )
    .get() as { value: string } | undefined;
  sqlite.exec('DELETE FROM event_streams');
  const save =
    sqlite.prepare(`INSERT INTO event_streams(stream_id,stream_kind,stream_version,last_commit_seq,last_event_id,reducer_version,projection,projection_sha256)
    VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(stream_id) DO UPDATE SET stream_version=excluded.stream_version,last_commit_seq=excluded.last_commit_seq,
    last_event_id=excluded.last_event_id,reducer_version=excluded.reducer_version,projection=excluded.projection,projection_sha256=excluded.projection_sha256`);
  let cursor = '0',
    recordedAt = '';
  while (true) {
    const commits = readDomainCommits(sqlite, cursor, 100);
    if (!commits.length) break;
    for (const commit of commits) {
      if (cursor !== '0' && BigInt(commit.commitSeq) !== BigInt(cursor) + 1n)
        throw new Error(`Replay commit order mismatch at ${commit.commitSeq}`);
      for (const event of commit.events) {
        const prior = readStreamProjection(sqlite, event.streamId);
        if (
          BigInt(event.streamVersion) !== BigInt(prior?.streamVersion || '0') + 1n ||
          (prior && prior.streamKind !== event.streamKind)
        )
          throw new Error(`Replayed stream version mismatch at ${event.eventId}`);
        const projection = reduceDomainEvent(prior?.projection || null, event);
        save.run(
          event.streamId,
          event.streamKind,
          BigInt(event.streamVersion),
          BigInt(commit.commitSeq),
          event.eventId,
          DOMAIN_REDUCER_VERSION,
          canonicalJson(projection),
          domainDigest(projection),
        );
      }
      cursor = commit.commitSeq;
      recordedAt = commit.recordedAt;
    }
  }
  if (cursor !== (clock?.value || '0') || (checkpoint && cursor !== checkpoint.value))
    throw new Error('Reducer upgrade commit cursor differs from committed history.');
  if (recordedAt)
    sqlite
      .prepare(
        `INSERT INTO projection_versions(name,epoch,reducer_version,last_commit_seq,status,updated_at) VALUES('core','live',?,?,'ready',?)
    ON CONFLICT(name) DO UPDATE SET reducer_version=excluded.reducer_version,last_commit_seq=excluded.last_commit_seq,status='ready',updated_at=excluded.updated_at`,
      )
      .run(DOMAIN_REDUCER_VERSION, BigInt(cursor), recordedAt);
  else
    sqlite.prepare('UPDATE projection_versions SET reducer_version=?').run(DOMAIN_REDUCER_VERSION);
  return true;
}

// 所有金额已在业务决定时确定；reducer 不读取当前时钟、报价或外部状态。
export function reduceDomainEvent(
  previous: DomainProjection,
  event: DomainEvent,
): DomainProjection {
  assertDomainEvent(event);
  const state = previous === null ? null : structuredClone(previous);
  const payload = event.payload;
  if (event.eventType === 'OpeningStateRecorded') {
    if (state !== null) throw new Error(`Opening must be the first fact of ${event.streamId}`);
    return structuredClone(object(payload.state));
  }
  if (event.streamKind === 'sharing')
    return reduceSharingEvent(
      state as unknown as SharingQuotaState | null,
      event,
    ) as unknown as DomainProjection;
  if (event.eventType === 'LegacyFactRecorded') {
    if (state !== null) throw new Error(`Legacy fact already exists for ${event.streamId}`);
    return structuredClone(payload);
  }
  if (event.streamKind === 'request') return reduceRequestEvent(state, event);
  if (event.streamKind === 'account') {
    const next: Record<string, unknown> = state || { id: event.accountId || payload.accountId };
    if (
      (accountQuotaEventTypes as readonly string[]).includes(event.eventType) &&
      !(event.eventType === 'QuotaObserved' && payload.kind === 'official') &&
      !(event.eventType === 'AvailabilityChanged' && payload.kind !== 'quota')
    ) {
      next.quota = reduceAccountQuotaEvent((next.quota as AccountQuotaState | null) || null, event);
      return next;
    }
    if (
      [
        'AccountRegistered',
        'AvailabilityChanged',
        'QuotaObserved',
        'CooldownChanged',
        'AdmissionChanged',
      ].includes(event.eventType)
    ) {
      return {
        ...next,
        ...structuredClone(payload.observation ? object(payload.observation) : payload),
        lastEventId: event.eventId,
      };
    }
  }
  if (
    event.streamKind === 'member' &&
    ['MemberEligibilityChanged', 'AdmissionChanged'].includes(event.eventType)
  ) {
    return {
      ...(state || { id: event.memberId || payload.memberId }),
      ...structuredClone(payload),
      lastEventId: event.eventId,
    };
  }
  if (event.streamKind === 'artifact' && event.eventType === 'ArtifactStorageChanged') {
    return {
      ...(state || { id: payload.artifactId }),
      storagePhase: payload.phase,
      ...pick(payload, ['file', 'mimeType', 'sha256', 'byteLength', 'r2']),
      ...(payload.lastAttemptAt !== undefined ? { r2LastAttemptAt: payload.lastAttemptAt } : {}),
      ...(payload.error !== undefined ? { r2Error: payload.error } : {}),
    };
  }
  if (event.streamKind === 'artifact' && event.eventType === 'ArtifactHistoryRetired')
    return {
      ...(state || { id: payload.artifactId, memberId: event.memberId }),
      retired: { at: payload.at, reason: payload.reason },
    };
  if (event.eventType === 'FreeImageSucceeded') return { ...payload, eventId: event.eventId };
  throw new Error(
    `No reducer for event ${event.eventId}: ${event.eventType} on ${event.streamKind}`,
  );
}

function reduceRequestEvent(previous: DomainProjection, event: DomainEvent): DomainProjection {
  const p = event.payload;
  if (event.eventType === 'RequestAccepted') {
    if (previous !== null) throw new Error(`Request already exists: ${event.streamId}`);
    if (!Array.isArray(p.jobs) || !p.jobs.length)
      throw new Error(`RequestAccepted ${event.eventId} requires jobs`);
    const jobs = Object.fromEntries(
      p.jobs.map((raw) => {
        const job = object(raw),
          id = text(job.id, 'job.id');
        return [
          id,
          {
            ...structuredClone(job),
            memberId: p.memberId,
            requestId: p.requestId,
            batchId: job.batchId,
            source: job.source || p.source,
            createdAt: job.createdAt || p.createdAt,
            request: job.summary || job.request,
            sampleIndex: job.index ?? job.sampleIndex ?? 0,
          },
        ];
      }),
    );
    return { request: structuredClone(p), jobs, artifacts: {}, aliases: [], freeUsage: {} };
  }
  if (!previous) throw new Error(`Missing request opening before event ${event.eventId}`);
  const state = previous;
  const jobs = object(state.jobs),
    artifacts = object((state.artifacts ||= {}));
  if (event.eventType === 'JobHistoryRetired') {
    if (!Array.isArray(p.jobIds)) throw new Error(`Missing retired jobs in ${event.eventId}`);
    const retired = object((state.retiredJobs ||= {}));
    for (const id of p.jobIds) {
      const job = object(jobs[String(id)]);
      if (
        ['queued', 'pending', 'running'].includes(String(job.status)) ||
        job.resultPending ||
        (job.generationReview as { status?: string } | undefined)?.status === 'pending'
      )
        throw new Error(`Cannot retire active job or unresolved review ${id}`);
      retired[String(id)] = { at: p.at, reason: p.reason };
    }
    return state;
  }
  if (event.eventType === 'RequestAliasRegistered') {
    const aliases = (state.aliases ||= []) as unknown[];
    aliases.push(structuredClone(p));
    return state;
  }
  if (event.eventType === 'WaitingJobsCancelled') {
    if (!Array.isArray(p.jobIds)) throw new Error(`Missing cancelled job IDs in ${event.eventId}`);
    for (const id of p.jobIds) {
      const job = object(jobs[String(id)]);
      if (!['queued', 'pending'].includes(String(job.status)))
        throw new Error(`Cannot cancel non-waiting job ${id}`);
      Object.assign(job, {
        status: 'failed',
        cancelledAt: iso(p.at as string | number),
        completedAt: iso(p.at as string | number),
        updatedAt: iso(p.at as string | number),
        error: p.reason,
        errorDetail: p.reason,
        errorStatus: p.errorStatus,
        cancellationReason: p.cancellationReason,
      });
    }
    return state;
  }
  const jobId = text(p.jobId, 'jobId'),
    job = object(jobs[jobId]);
  if (
    p.response &&
    [
      'ArtifactPrepared',
      'JobCompleted',
      'JobFailed',
      'LateResultRecovered',
      'ReviewOpened',
      'ResponseEvidenceRecorded',
    ].includes(event.eventType)
  ) {
    const response = object(p.response);
    // Old events can lack observations; absence must never erase previously observed evidence.
    for (const field of [
      'upstreamStartedAt',
      'upstreamHeadersAt',
      'upstreamCompletedAt',
      'upstreamHttpStatus',
    ]) {
      if (response[field] !== null && response[field] !== undefined) job[field] = response[field];
    }
  }
  switch (event.eventType) {
    case 'ResponseEvidenceRecorded':
      break;
    case 'JobQueued':
      Object.assign(job, {
        status: 'queued',
        queueSeq: p.queueSeq,
        queueExpiresAt: p.queueExpiresAt,
        queuedAt: p.queuedAt,
      });
      break;
    case 'DispatchAuthorized':
      Object.assign(job, {
        status: 'running',
        accountId: p.accountId,
        attemptId: p.attemptId,
        startedAt: p.authorizedAt,
        generationAttempt: p.attempt,
        chargeEventId: p.chargeEventId,
      });
      break;
    case 'ArtifactPrepared': {
      const { id, ...descriptor } = object(p.artifact);
      job.resultPending = structuredClone(descriptor);
      break;
    }
    case 'ArtifactPublished': {
      const artifact = object(p.artifact);
      artifacts[text(artifact.id, 'artifact.id')] = {
        ...structuredClone(artifact),
        memberId: job.memberId,
        jobId,
      };
      break;
    }
    case 'CachedResultLinked':
      Object.assign(job, {
        status: 'done',
        imageId: p.imageId || p.artifactId,
        accountId: p.accountId,
        completedAt: p.completedAt,
        updatedAt: p.completedAt,
        cached: true,
      });
      break;
    case 'JobCompleted':
      if (job.status === 'failed')
        throw new Error(`Failed job ${jobId} requires LateResultRecovered`);
      Object.assign(job, {
        status: 'done',
        imageId: p.artifactId || p.imageId,
        completedAt: p.completedAt,
        updatedAt: p.completedAt,
        accountId: p.accountId || job.accountId,
        fairCharge: p.charge,
      });
      if (p.artifact) {
        const artifact = object(p.artifact);
        artifacts[text(artifact.id, 'artifact.id')] = {
          ...structuredClone(artifact),
          memberId: job.memberId,
          jobId,
        };
      }
      delete job.resultPending;
      break;
    case 'JobFailed':
      if (job.status === 'done') throw new Error(`Completed job ${jobId} cannot fail`);
      Object.assign(job, {
        status: 'failed',
        completedAt: p.failedAt,
        updatedAt: p.failedAt,
        error: p.message,
        errorStatus: p.errorStatus,
        errorCode: p.errorCode,
        fairCharge: p.charge,
        generationRefund: p.generationRefund || p.refund,
        generationReview: p.generationReview || p.review,
      });
      if (p.cancelledAt) job.cancelledAt = p.cancelledAt;
      if (p.cancellationReason) job.cancellationReason = p.cancellationReason;
      if (p.diagnosticRef) job.diagnosticRef = p.diagnosticRef;
      break;
    case 'LateResultRecovered':
      if (job.status !== 'failed' || p.refundRetained !== true)
        throw new Error(`Late result must preserve failure/refund for ${jobId}`);
      Object.assign(job, {
        lateResultAvailable: true,
        lateResultAt: iso(p.at as string | number),
        updatedAt: iso(p.at as string | number),
        lateResultImageId: p.artifactId,
        imageId: p.artifactId,
      });
      delete job.resultPending;
      if (p.review) job.generationReview = structuredClone(p.review);
      break;
    case 'ReviewOpened':
    case 'ReviewClosed':
      job.generationReview = structuredClone(p.review);
      break;
    case 'FreeImageSucceeded':
      object((state.freeUsage ||= {}))[jobId] = structuredClone(p);
      break;
    default:
      throw new Error(`No request reducer for event ${event.eventId}: ${event.eventType}`);
  }
  return state;
}

export function appendDomainCommit(
  sqlite: Database.Database,
  command: DomainCommand | DomainReplayCommand,
  options: AppendOptions = {},
): { commitSeq: string; receipt: Json; duplicate: boolean } {
  if (!sqlite.inTransaction)
    throw new Error('Domain commit must run inside the business write transaction.');
  assertCurrentEventReducer(sqlite);
  if (!command.events.length)
    throw new Error('A domain commit requires at least one explicit fact.');
  command.events.forEach(assertDomainEvent);
  assertPublicDomainFact(command.receipt, 'receipt');
  const fingerprint =
    command.fingerprint || domainDigest(command.events.map(({ eventId, ...event }) => event));
  const prior = readCommandReceipt(sqlite, command.commandId);
  if (prior) {
    if (prior.fingerprint !== fingerprint)
      throw Object.assign(new Error(`Command fingerprint conflict: ${command.commandId}`), {
        statusCode: 409,
      });
    return { commitSeq: prior.commitSeq, receipt: prior.receipt, duplicate: true };
  }
  // 嵌套事务提供 savepoint，调用者捕获失败后也不能提交半个事件批次。
  return sqlite.transaction(() => {
    for (const diagnostic of command.diagnostics || [])
      sqlite
        .prepare(
          'INSERT OR IGNORE INTO generation_diagnostics(diagnostic_id,job_id,detail,detail_sha256,created_at) VALUES(?,?,?,?,?)',
        )
        .run(
          diagnostic.diagnosticId,
          diagnostic.jobId,
          diagnostic.detail,
          diagnostic.detailSha256,
          command.recordedAt,
        );
    sqlite.prepare('INSERT OR IGNORE INTO event_clock(id,value) VALUES(1,0)').run();
    const clock = sqlite
      .prepare('SELECT CAST(value AS TEXT) AS value FROM event_clock WHERE id=1')
      .get() as { value: string };
    const next = BigInt(clock.value) + 1n;
    if (next > maxSequence) throw new Error('Event commit sequence exhausted.');
    sqlite.prepare('UPDATE event_clock SET value=? WHERE id=1').run(next);
    const commitSeq = String(next),
      recordedAt = iso(command.recordedAt);
    sqlite
      .prepare(
        'INSERT INTO event_commits(commit_seq,command_id,fingerprint,recorded_at,event_count,receipt) VALUES(?,?,?,?,?,?)',
      )
      .run(
        next,
        command.commandId,
        fingerprint,
        recordedAt,
        command.events.length,
        canonicalJson(command.receipt),
      );
    const streams = new Map<string, StreamProjection>();
    const insertEvent =
      sqlite.prepare(`INSERT INTO domain_events(event_id,commit_seq,event_index,stream_id,stream_kind,stream_version,
      event_type,schema_version,occurred_at,recorded_at,actor_id,actor_kind,correlation_id,causation_id,account_id,member_id,
      policy_version,pricing_version,amount_version,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
    const events = command.events.map((input, eventIndex): DomainEvent => {
      let stream = streams.get(input.streamId);
      if (!stream) {
        stream = readStreamProjection(sqlite, input.streamId) || {
          streamId: input.streamId,
          streamKind: input.streamId.split(':', 1)[0]!,
          streamVersion: '0',
          lastCommitSeq: '0',
          lastEventId: '',
          reducerVersion: DOMAIN_REDUCER_VERSION,
          projection: null,
          projectionSha256: domainDigest(null),
        };
        const expected = command.expectedVersions[input.streamId];
        if (expected !== undefined && expected !== stream.streamVersion)
          throw new Error(
            `Stream version conflict ${input.streamId}: expected ${expected}, found ${stream.streamVersion}`,
          );
        streams.set(input.streamId, stream);
      }
      const version = BigInt(stream.streamVersion) + 1n;
      if (version > maxSequence) throw new Error(`Stream version exhausted: ${input.streamId}`);
      if ('streamVersion' in input && input.streamVersion !== String(version))
        throw new Error(`Replayed stream version mismatch at ${input.eventId}`);
      const event: DomainEvent = {
        eventId: text(input.eventId, 'eventId'),
        commitSeq,
        eventIndex,
        commandId: command.commandId,
        transactionId: command.commandId,
        streamId: stream.streamId,
        streamKind: stream.streamKind,
        streamVersion: String(version),
        eventType: input.eventType,
        schemaVersion: input.schemaVersion ?? 1,
        occurredAt: iso(input.occurredAt ?? recordedAt),
        recordedAt,
        actorId: command.actorId,
        actorKind: command.actorKind || 'system',
        correlationId: command.correlationId,
        causationId: command.causationId,
        accountId: input.accountId ?? null,
        memberId: input.memberId ?? null,
        policyVersion: input.policyVersion || '1',
        pricingVersion: input.pricingVersion || '1',
        amountVersion: input.amountVersion || DOMAIN_AMOUNT_VERSION,
        payload: JSON.parse(canonicalJson(input.payload)),
      };
      stream.projection = reduceDomainEvent(stream.projection, event);
      stream.streamVersion = String(version);
      stream.lastCommitSeq = commitSeq;
      stream.lastEventId = event.eventId;
      stream.projectionSha256 = domainDigest(stream.projection);
      insertEvent.run(
        event.eventId,
        next,
        eventIndex,
        event.streamId,
        event.streamKind,
        version,
        event.eventType,
        event.schemaVersion,
        event.occurredAt,
        recordedAt,
        event.actorId,
        event.actorKind,
        event.correlationId,
        event.causationId,
        event.accountId,
        event.memberId,
        event.policyVersion,
        event.pricingVersion,
        event.amountVersion,
        canonicalJson(event.payload),
      );
      return event;
    });
    const saveStream =
      sqlite.prepare(`INSERT INTO event_streams(stream_id,stream_kind,stream_version,last_commit_seq,last_event_id,reducer_version,projection,projection_sha256)
      VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(stream_id) DO UPDATE SET stream_version=excluded.stream_version,last_commit_seq=excluded.last_commit_seq,
      last_event_id=excluded.last_event_id,reducer_version=excluded.reducer_version,projection=excluded.projection,projection_sha256=excluded.projection_sha256`);
    for (const stream of streams.values())
      saveStream.run(
        stream.streamId,
        stream.streamKind,
        BigInt(stream.streamVersion),
        next,
        stream.lastEventId,
        DOMAIN_REDUCER_VERSION,
        canonicalJson(stream.projection),
        stream.projectionSha256,
      );
    const commit: DomainCommit = {
      commitSeq,
      commandId: command.commandId,
      fingerprint,
      recordedAt,
      receipt: command.receipt,
      events,
    };
    projectColdDomainFacts(sqlite, commit);
    options.project?.(sqlite, commit, [...streams.values()]);
    sqlite
      .prepare(
        `INSERT INTO projection_versions(name,epoch,reducer_version,last_commit_seq,status,updated_at) VALUES('core','live',?,?, 'ready',?)
      ON CONFLICT(name) DO UPDATE SET reducer_version=excluded.reducer_version,last_commit_seq=excluded.last_commit_seq,status='ready',updated_at=excluded.updated_at`,
      )
      .run(DOMAIN_REDUCER_VERSION, next, recordedAt);
    return { commitSeq, receipt: command.receipt, duplicate: false };
  })();
}

function idempotencyFact(
  sqlite: Database.Database,
  fact: Record<string, unknown>,
  owner?: unknown,
): Record<string, unknown> {
  const memberId = text(fact.memberId || owner, 'idempotency.memberId');
  const contextRef = text(
    fact.normalizationContextRef || fact.contextRef,
    'idempotency.contextRef',
  );
  const context = sqlite
    .prepare('SELECT payload FROM generation_contexts WHERE context_ref=?')
    .get(contextRef) as { payload: string } | undefined;
  if (!context) throw new Error(`Missing protected normalization input ${contextRef}`);
  if (
    fact.normalizationContextHash &&
    domainDigest(JSON.parse(context.payload)) !== fact.normalizationContextHash
  )
    throw new Error(`Protected normalization input hash mismatch ${contextRef}`);
  return {
    member_id: memberId,
    operation: text(fact.operation, 'idempotency.operation'),
    key_hash: text(fact.keyHash, 'idempotency.keyHash'),
    fingerprint: text(fact.fingerprint, 'idempotency.fingerprint'),
    normalization_context: context.payload,
    job_id: text(fact.jobId, 'idempotency.jobId'),
    request_id: fact.requestId ?? null,
    created_at: text(fact.createdAt, 'idempotency.createdAt'),
  };
}
function saveIdempotencyFact(
  sqlite: Database.Database,
  fact: Record<string, unknown>,
  owner?: unknown,
): void {
  const row = idempotencyFact(sqlite, fact, owner),
    values = Object.values(row);
  const existing = sqlite
    .prepare(
      'SELECT * FROM generation_idempotency WHERE member_id=? AND operation=? AND key_hash=?',
    )
    .get(...values.slice(0, 3)) as Record<string, unknown> | undefined;
  if (
    existing &&
    (existing.fingerprint !== row.fingerprint ||
      existing.job_id !== row.job_id ||
      existing.request_id !== row.request_id)
  )
    throw new Error(`Conflicting idempotency fact ${row.key_hash}`);
  sqlite
    .prepare(
      `INSERT OR IGNORE INTO generation_idempotency(member_id,operation,key_hash,fingerprint,normalization_context,job_id,request_id,created_at)
    VALUES(?,?,?,?,?,?,?,?)`,
    )
    .run(...values);
}

// 切换/离线核对才扫描冷凭据；普通命令仍只同步本批键。
export function verifyColdDomainFacts(sqlite: Database.Database): void {
  sqlite.exec(
    'CREATE TEMP TABLE event_cold_check(kind TEXT NOT NULL, key TEXT NOT NULL, digest TEXT NOT NULL, seen INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(kind,key))',
  );
  try {
    const expected = sqlite.prepare('SELECT digest FROM event_cold_check WHERE kind=? AND key=?');
    const put = (kind: string, key: string, value: unknown) => {
      const digest = domainDigest(value),
        prior = expected.get(kind, key) as { digest: string } | undefined;
      if (prior && prior.digest !== digest)
        throw new Error(`Conflicting historical ${kind} fact ${key}`);
      sqlite
        .prepare('INSERT OR IGNORE INTO event_cold_check(kind,key,digest) VALUES(?,?,?)')
        .run(kind, key, digest);
    };
    const putIdempotency = (fact: Record<string, unknown>, owner?: unknown) => {
      const row = idempotencyFact(sqlite, fact, owner);
      put(
        'generation_idempotency',
        canonicalJson([row.member_id, row.operation, row.key_hash]),
        row,
      );
    };
    const putFree = (p: Record<string, unknown>) =>
      put('free_image_usage', String(p.jobId), {
        job_id: p.jobId,
        member_id: p.memberId,
        succeeded_at: p.succeededAt,
        warning_threshold: p.warningThreshold ?? null,
      });
    let cursor = '0',
      eventIndex = -1;
    while (true) {
      const events = sqlite
        .prepare(
          `SELECT event_id,event_type,stream_kind,schema_version,payload,CAST(commit_seq AS TEXT) AS commit_seq,event_index FROM domain_events
        WHERE (domain_events.commit_seq,event_index)>(CAST(? AS INTEGER),?) AND (event_type IN ('GenerationSettled','GenerationRefunded','FreeImageSucceeded','RequestAccepted','RequestAliasRegistered')
        OR (event_type='OpeningStateRecorded' AND stream_kind IN ('settlement','idempotency','free-usage')))
        ORDER BY domain_events.commit_seq,event_index LIMIT 200`,
        )
        .all(cursor, eventIndex) as {
        event_id: string;
        event_type: string;
        stream_kind: string;
        schema_version: number;
        payload: string;
        commit_seq: string;
        event_index: number;
      }[];
      if (!events.length) break;
      for (const event of events) {
        if (event.schema_version !== 1)
          throw new Error(`Unsupported cold fact schema ${event.event_id}`);
        const p = JSON.parse(event.payload) as Record<string, unknown>;
        if (['GenerationSettled', 'GenerationRefunded'].includes(event.event_type))
          put('fair_quota_settlements', String(p.jobId), { job_id: p.jobId, charge: p.charge });
        else if (event.event_type === 'FreeImageSucceeded') putFree(p);
        else if (event.event_type === 'RequestAccepted' && p.idempotency)
          putIdempotency(object(p.idempotency), p.memberId);
        else if (event.event_type === 'RequestAliasRegistered')
          putIdempotency(
            { ...object(p.idempotency), jobId: p.jobId, requestId: p.requestId },
            p.memberId,
          );
        else if (event.event_type === 'OpeningStateRecorded') {
          const state = object(p.state);
          if (event.stream_kind === 'settlement')
            put('fair_quota_settlements', String(state.jobId), {
              job_id: state.jobId,
              charge: state.charge,
            });
          else if (event.stream_kind === 'idempotency') putIdempotency(state);
          else if (event.stream_kind === 'free-usage') putFree(state);
        }
      }
      cursor = events.at(-1)!.commit_seq;
      eventIndex = events.at(-1)!.event_index;
    }
    for (const table of ['fair_quota_settlements', 'generation_idempotency', 'free_image_usage']) {
      const keys =
        table === 'generation_idempotency' ? ['member_id', 'operation', 'key_hash'] : ['job_id'];
      let last = keys.map(() => '');
      while (true) {
        const rows = sqlite
          .prepare(
            `SELECT * FROM ${table} WHERE (${keys.join(',')})>(${keys.map(() => '?').join(',')}) ORDER BY ${keys.join(',')} LIMIT 200`,
          )
          .all(...last) as Record<string, unknown>[];
        if (!rows.length) break;
        for (const row of rows) {
          const key =
            keys.length === 1 ? String(row[keys[0]!]) : canonicalJson(keys.map((key) => row[key]));
          const fact =
            table === 'fair_quota_settlements'
              ? { ...row, charge: JSON.parse(String(row.charge)) }
              : row;
          const wanted = expected.get(table, key) as { digest: string } | undefined;
          if (!wanted) throw new Error(`Unexpected cold projection ${table}:${key}`);
          if (wanted.digest !== domainDigest(fact))
            throw new Error(`Cold projection mismatch ${table}:${key}`);
          sqlite
            .prepare('UPDATE event_cold_check SET seen=1 WHERE kind=? AND key=?')
            .run(table, key);
        }
        last = keys.map((key) => String(rows.at(-1)![key]));
      }
    }
    const missing = sqlite
      .prepare('SELECT kind,key FROM event_cold_check WHERE seen=0 LIMIT 1')
      .get() as { kind: string; key: string } | undefined;
    if (missing) throw new Error(`Missing cold projection ${missing.kind}:${missing.key}`);
  } finally {
    sqlite.exec('DROP TABLE event_cold_check');
  }
}

// 冷凭据和其源事件在同一事务内生效；这些行不进入通用热缓存。
export function projectColdDomainFacts(sqlite: Database.Database, commit: DomainCommit): void {
  for (const event of commit.events) {
    const p = event.payload;
    if (event.eventType === 'JobHistoryRetired')
      for (const id of p.jobIds as string[]) sqlite.prepare('DELETE FROM jobs WHERE id=?').run(id);
    if (event.eventType === 'ArtifactHistoryRetired')
      sqlite.prepare('DELETE FROM images WHERE id=?').run(p.artifactId);
    const requestFact =
      event.eventType === 'RequestAccepted'
        ? p
        : event.eventType === 'OpeningStateRecorded' && event.streamKind === 'request'
          ? object(object(p.state).request)
          : null;
    if (requestFact?.inputRef) {
      const input = sqlite
        .prepare('SELECT member_id,payload FROM generation_requests WHERE id=?')
        .get(requestFact.inputRef) as { member_id: string; payload: string } | undefined;
      if (!input) throw new Error(`Missing protected generation input ${requestFact.inputRef}`);
      if (requestFact.memberId && input.member_id !== requestFact.memberId)
        throw new Error(`Protected generation input owner mismatch ${requestFact.inputRef}`);
      if (createHash('sha256').update(input.payload).digest('hex') !== requestFact.inputSha256)
        throw new Error(`Protected generation input hash mismatch ${requestFact.inputRef}`);
    }
    if (event.eventType === 'RequestAccepted' && p.idempotency)
      saveIdempotencyFact(sqlite, object(p.idempotency), p.memberId);
    if (event.eventType === 'RequestAliasRegistered')
      saveIdempotencyFact(
        sqlite,
        { ...object(p.idempotency), jobId: p.jobId, requestId: p.requestId },
        p.memberId,
      );
    if (['GenerationSettled', 'GenerationRefunded'].includes(event.eventType) && p.charge) {
      const charge = object(p.charge);
      if (!['settled', 'refunded'].includes(String(charge.status)))
        throw new Error(`Terminal event has nonterminal charge ${event.eventId}`);
      const id = text(p.jobId, 'settlement.jobId'),
        serialized = canonicalJson(charge);
      const prior = sqlite
        .prepare('SELECT charge FROM fair_quota_settlements WHERE job_id=?')
        .get(id) as { charge: string } | undefined;
      if (prior && domainDigest(JSON.parse(prior.charge)) !== domainDigest(charge))
        throw new Error(`Conflicting terminal charge ${id}`);
      sqlite
        .prepare('INSERT OR IGNORE INTO fair_quota_settlements(job_id,charge) VALUES(?,?)')
        .run(id, serialized);
    }
    if (event.eventType === 'FreeImageSucceeded') {
      sqlite
        .prepare(
          'INSERT OR IGNORE INTO free_image_usage(job_id,member_id,succeeded_at,warning_threshold) VALUES(?,?,?,?)',
        )
        .run(
          text(p.jobId, 'freeUsage.jobId'),
          text(p.memberId, 'freeUsage.memberId'),
          p.succeededAt,
          p.warningThreshold ?? null,
        );
    }
    if (event.eventType === 'OpeningStateRecorded') {
      const state = object(p.state);
      if (event.streamKind === 'idempotency') saveIdempotencyFact(sqlite, state);
      if (event.streamKind === 'settlement')
        sqlite
          .prepare('INSERT OR IGNORE INTO fair_quota_settlements(job_id,charge) VALUES(?,?)')
          .run(state.jobId, canonicalJson(state.charge));
      if (event.streamKind === 'free-usage')
        sqlite
          .prepare(
            'INSERT OR IGNORE INTO free_image_usage(job_id,member_id,succeeded_at,warning_threshold) VALUES(?,?,?,?)',
          )
          .run(state.jobId, state.memberId, state.succeededAt, state.warningThreshold ?? null);
    }
  }
}

export function assertReplaySourceReady(source: Database.Database): void {
  const authority = readCompatibleEventAuthority(source);
  const versions = source
    .prepare(
      'SELECT DISTINCT reducer_version FROM event_streams UNION SELECT reducer_version FROM projection_versions',
    )
    .all() as { reducer_version: number }[];
  if (
    (authority && authority.version !== EVENT_AUTHORITY_VERSION) ||
    versions.some((row) => row.reducer_version !== DOMAIN_REDUCER_VERSION)
  ) {
    throw new Error(
      'Replay source requires a current writer/reducer. Upgrade an ISOLATED COPY of the complete backup with JsonStore.init() first, preserving legacy query-only response evidence; keep the original backup unchanged.',
    );
  }
}

export function replayDomainEvents(
  source: Database.Database,
  target: Database.Database,
  options: AppendOptions & { throughCommitSeq?: string; maxCommits?: number } = {},
): { lastCommitSeq: string; commits: number; events: number } {
  assertReplaySourceReady(source);
  target.transaction(() => {
    const authority = readCompatibleEventAuthority(target),
      upgraded = upgradeEventReducers(target);
    // 新库迁移的旧兼容触发器不能按今日政策改写历史成功时间或告警阈值。
    removeLegacyFreeImageTriggers(target);
    if (upgraded && options.project) {
      const clock = target
        .prepare('SELECT CAST(value AS TEXT) AS value FROM event_clock WHERE id=1')
        .get() as { value: string } | undefined;
      const commit: DomainCommit = {
        commitSeq: clock?.value || '0',
        commandId: 'upgrade-replayed-projections',
        fingerprint: '',
        recordedAt: new Date().toISOString(),
        receipt: null,
        events: [],
      };
      let cursor = '';
      while (true) {
        const rows = target
          .prepare(
            'SELECT stream_id FROM event_streams WHERE stream_id>? ORDER BY stream_id LIMIT 100',
          )
          .all(cursor) as { stream_id: string }[];
        if (!rows.length) break;
        options.project(
          target,
          commit,
          rows.map((row) => readStreamProjection(target, row.stream_id)!),
        );
        cursor = rows.at(-1)!.stream_id;
      }
    }
    if (authority && upgraded)
      target
        .prepare("UPDATE app_meta SET value=? WHERE key='event_authority'")
        .run(
          JSON.stringify({
            ...authority,
            version: EVENT_AUTHORITY_VERSION,
            minimumWriterVersion: EVENT_AUTHORITY_VERSION,
          }),
        );
  })();
  const checkpoint = target
    .prepare(
      "SELECT CAST(last_commit_seq AS TEXT) AS value FROM projection_versions WHERE name='core'",
    )
    .get() as { value: string } | undefined;
  let cursor = checkpoint?.value || '0',
    commits = 0,
    events = 0;
  const through = options.throughCommitSeq === undefined ? null : BigInt(options.throughCommitSeq);
  while (commits < (options.maxCommits ?? Infinity)) {
    const batch = readDomainCommits(
      source,
      cursor,
      Math.min(100, (options.maxCommits ?? Infinity) - commits),
    ).filter((commit) => through === null || BigInt(commit.commitSeq) <= through);
    if (!batch.length) break;
    // 离线重建以最多 100 个完整逻辑提交共用一次 FULL 事务；生产写入仍逐命令提交。
    target.transaction(() => {
      for (const commit of batch) {
        const clock = target
          .prepare('SELECT CAST(value AS TEXT) AS value FROM event_clock WHERE id=1')
          .get() as { value: string } | undefined;
        if (!clock)
          target
            .prepare('INSERT INTO event_clock(id,value) VALUES(1,?)')
            .run(BigInt(commit.commitSeq) - 1n);
        else if (BigInt(clock.value) + 1n !== BigInt(commit.commitSeq))
          throw new Error(`Replay commit order mismatch at ${commit.commitSeq}`);
        appendDomainCommit(
          target,
          {
            commandId: commit.commandId,
            fingerprint: commit.fingerprint,
            recordedAt: commit.recordedAt,
            actorId: commit.events[0]?.actorId ?? null,
            actorKind: commit.events[0]?.actorKind || 'system',
            correlationId: commit.events[0]?.correlationId ?? null,
            causationId: commit.events[0]?.causationId ?? null,
            receipt: commit.receipt,
            expectedVersions: {},
            events: commit.events,
          },
          options,
        );
      }
    })();
    cursor = batch.at(-1)!.commitSeq;
    commits += batch.length;
    events += batch.reduce((count, commit) => count + commit.events.length, 0);
  }
  return { lastCommitSeq: cursor, commits, events };
}

const legacySecretKey =
  /^(?:userToken|token|accessToken|refreshToken|access_token|refresh_token|pst|password|passwordHash|password_hash|authorization|cookie|apiKey|api_key|secret|base64|prompt|fullPrompt|negativePrompt|negative_prompt|uc|nativeParameters|operationParameters|image|mask|reference_image_multiple|errorDetail)$/i;
function legacyPublicFact(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(legacyPublicFact);
  if (value && typeof value === 'object')
    return Object.fromEntries(
      Object.entries(value)
        .filter(([key, item]) => item !== undefined && !legacySecretKey.test(key))
        .map(([key, item]) => [key, legacyPublicFact(item)]),
    );
  return value;
}
const pick = (value: Record<string, unknown>, fields: string[]): Record<string, unknown> =>
  Object.fromEntries(
    fields.filter((field) => value[field] !== undefined).map((field) => [field, value[field]]),
  );
const accountFacts = [
  'id',
  'name',
  'enabled',
  'quotaTier',
  'subscriptionActive',
  'subscriptionExpiresAt',
  'v5UsagePercent',
  'quotaFixed',
  'quotaPurchased',
  'v5UsageIsNegative',
  'quotaCheckedAt',
  'v5UsageTimeUntilNextPercent',
  'quotaError',
  'upstreamStatus',
  'cooldownUntil',
  'upstreamRetryAfterUntil',
  'quotaRetryAfterUntil',
  'lastUsedAt',
];
const requestSummaryFields = [
  'operation',
  'model',
  'action',
  'width',
  'height',
  'steps',
  'requestedSteps',
  'seed',
  'allowPurchasedAnlas',
  'sampleCount',
  'clientContract',
];

export interface OpeningReport {
  status: 'complete';
  importId: string;
  chunks: number;
  facts: number;
  recordedAt: string;
}

export function copyProtectedFacts(source: Database.Database, target: Database.Database): void {
  // 认证与输入不是可重建投影；仍需连同事件备份。逐行复制避免将大图片请求载荷热载。
  for (const table of [
    'generation_requests',
    'generation_contexts',
    'artifact_inputs',
    'generation_diagnostics',
    'member_sessions',
    'users',
    'accounts',
    'cards',
  ]) {
    const columns = (source.prepare(`PRAGMA table_info(${table})`).all() as { name: string }[]).map(
      (column) => column.name,
    );
    const insert = target.prepare(
      `INSERT OR IGNORE INTO ${table}(${columns.join(',')}) VALUES(${columns.map((column) => `@${column}`).join(',')})`,
    );
    target.transaction(() => {
      for (const row of source.prepare(`SELECT * FROM ${table}`).iterate()) insert.run(row);
    })();
  }
  const settings = source.prepare("SELECT value FROM app_settings WHERE key='settings'").get() as
    { value: string } | undefined;
  if (settings) {
    const value = JSON.parse(settings.value) as Record<string, unknown>;
    delete value.fairQuota;
    target
      .prepare("INSERT OR IGNORE INTO app_settings(key,value) VALUES('settings',?)")
      .run(JSON.stringify(value));
  }
  for (const row of source
    .prepare("SELECT key,value FROM app_meta WHERE key IN ('event_opening','member_session_owner')")
    .iterate() as Iterable<{ key: string; value: string }>)
    target.prepare('INSERT OR IGNORE INTO app_meta(key,value) VALUES(?,?)').run(row.key, row.value);
}
// 调用方已停止业务写入；每块可恢复，完成标记之前不允许受理新命令。
export function importOpeningFacts(sqlite: Database.Database): OpeningReport {
  const importId = 'opening-v1';
  const readMeta = sqlite.prepare('SELECT value FROM app_meta WHERE key=?');
  const existing = readMeta.get('event_opening') as { value: string } | undefined;
  const marker = existing
    ? (JSON.parse(existing.value) as { status: string; recordedAt: string; historyId?: string })
    : null;
  if (marker?.status === 'complete') {
    const totals = sqlite
      .prepare(
        'SELECT count(*) AS chunks,COALESCE(sum(record_count),0) AS facts FROM event_imports WHERE import_id=?',
      )
      .get(importId) as { chunks: number; facts: number };
    return { status: 'complete', importId, ...totals, recordedAt: marker.recordedAt };
  }
  if (!marker && sqlite.prepare('SELECT 1 FROM event_commits LIMIT 1').get())
    throw new Error('Domain history exists without an opening import marker.');
  const recordedAt =
    marker?.recordedAt ||
    (sqlite.prepare("SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now') AS at").get() as { at: string })
      .at;
  const historyId = marker?.historyId || randomUUID();
  sqlite
    .prepare(
      'INSERT INTO app_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
    )
    .run(
      'event_opening',
      canonicalJson({ status: 'importing', recordedAt, version: 1, historyId }),
    );
  const memberByToken = new Map<string, string>(),
    memberEligibility: Record<string, unknown> = {};
  for (const row of sqlite.prepare('SELECT data FROM users').iterate() as Iterable<{
    data: string;
  }>) {
    const user = JSON.parse(row.data) as Record<string, unknown>;
    if (typeof user.token === 'string') memberByToken.set(user.token, String(user.id));
    memberEligibility[String(user.id)] = {
      memberId: user.id,
      ...pick(user, ['enabled', 'acceptingJobs', 'jobAdmissionVersion', 'createdAt']),
      ...(user.discord
        ? { discord: pick(object(user.discord), ['id', 'guildId', 'checkedAt', 'member']) }
        : {}),
    };
  }
  const memberOf = (value: Record<string, unknown>): unknown =>
    value.memberId ||
    value.userId ||
    (value.generationAttempt as Record<string, unknown> | undefined)?.memberId ||
    (value.fairCharge as Record<string, unknown> | undefined)?.userId ||
    memberByToken.get(String(value.userToken || value.token || '')) ||
    null;
  let chunks = 0,
    facts = 0;
  const chunk = (
    kind: string,
    index: number,
    events: DomainEventInput[],
    firstKey: string | null = null,
    lastKey: string | null = null,
  ) => {
    if (!events.length) return;
    const chunkId = `${kind}:${index}`,
      digest = domainDigest(events);
    const prior = sqlite
      .prepare('SELECT source_sha256 FROM event_imports WHERE import_id=? AND chunk_id=?')
      .get(importId, chunkId) as { source_sha256: string } | undefined;
    if (prior) {
      if (prior.source_sha256 !== digest)
        throw new Error(`Opening source changed during import at ${chunkId}`);
      chunks++;
      facts += events.length;
      return;
    }
    sqlite.transaction(() => {
      const result = appendDomainCommit(sqlite, {
        commandId: `${importId}:${chunkId}`,
        fingerprint: digest,
        recordedAt,
        actorId: null,
        actorKind: 'system',
        correlationId: importId,
        causationId: null,
        expectedVersions: {},
        receipt: { importId, chunkId },
        events: events.map((event, index) => ({
          ...event,
          eventId: `${importId}:${chunkId}:${index}`,
          occurredAt: recordedAt,
        })),
      });
      sqlite
        .prepare(
          `INSERT INTO event_imports(import_id,chunk_id,source_sha256,first_key,last_key,record_count,commit_seq,completed_at)
        VALUES(?,?,?,?,?,?,?,?)`,
        )
        .run(
          importId,
          chunkId,
          digest,
          firstKey,
          lastKey,
          events.length,
          BigInt(result.commitSeq),
          recordedAt,
        );
    })();
    chunks++;
    facts += events.length;
  };
  const opening = (
    streamId: string,
    state: Record<string, unknown>,
    extra: Record<string, unknown> = {},
  ): DomainEventInput => ({
    eventType: 'OpeningStateRecorded',
    streamId,
    payload: { state, source: 'legacy-opening', ...extra },
  });
  const settingsRow = sqlite
    .prepare("SELECT value FROM app_settings WHERE key='settings'")
    .get() as { value: string } | undefined;
  const settings = settingsRow
    ? (JSON.parse(settingsRow.value) as { fairQuota?: FairQuotaState })
    : {};
  let openingQuota: AccountQuotaState | null = null,
    openingAccountId: string | null = null;
  if (settings.fairQuota) {
    const amountReport: OpeningAmountGroupReport[] = [];
    const state = normalizeOpeningQuota(settings.fairQuota, amountReport);
    if (
      !state.boundAccountId &&
      (Object.values(state.balances).some((balance) =>
        ['v5Available', 'anlasFixedAvailable', 'anlasPurchasedAvailable'].some(
          (key) => parseAmount(balance[key as keyof typeof balance]) !== 0n,
        ),
      ) ||
        Object.keys(state.reservations).length)
    ) {
      throw new Error('Opening quota has money or reservations without a proven bound account.');
    }
    const terminals = Object.entries(state.reservations).filter(
      ([, charge]) => charge.status !== 'reserved',
    );
    state.reservations = Object.fromEntries(
      Object.entries(state.reservations).filter(([, charge]) => charge.status === 'reserved'),
    );
    const split = splitQuotaState(state);
    openingQuota = split.quota;
    openingAccountId = state.boundAccountId;
    const openingEvents = [
      opening('sharing:default', { ...split.sharing, memberEligibility } as unknown as Record<
        string,
        unknown
      >),
    ];
    if (openingAccountId) {
      const account = sqlite
        .prepare('SELECT data FROM accounts WHERE id=?')
        .get(openingAccountId) as { data: string } | undefined;
      if (!account) throw new Error(`Opening quota references missing account ${openingAccountId}`);
      openingEvents.push(
        opening(
          `account:${openingAccountId}`,
          {
            ...pick(JSON.parse(account.data) as Record<string, unknown>, accountFacts),
            quota: openingQuota,
          },
          { amountReport },
        ),
      );
    }
    chunk('configuration', 0, openingEvents);
    for (const [jobId, charge] of terminals)
      sqlite
        .prepare('INSERT OR IGNORE INTO fair_quota_settlements(job_id,charge) VALUES(?,?)')
        .run(jobId, canonicalJson(charge));
  } else chunk('configuration', 0, [opening('sharing:default', { memberEligibility })]);
  function records(
    table: string,
    key: string,
    transform: (row: Record<string, unknown>) => DomainEventInput[],
  ) {
    let cursor = '',
      index = 0;
    const pageSize = ['jobs', 'images'].includes(table) ? 1 : 100;
    const select = sqlite.prepare(`SELECT * FROM ${table} WHERE ${key}>? ORDER BY ${key} LIMIT ?`);
    const importBlock = sqlite.transaction(() => {
      let sourceCount = 0,
        firstKey = '';
      const events: DomainEventInput[] = [];
      while (sourceCount < 100) {
        const rows = select.all(cursor, Math.min(pageSize, 100 - sourceCount)) as Record<
          string,
          unknown
        >[];
        if (!rows.length) break;
        for (const row of rows) {
          if (!sourceCount) firstKey = String(row[key]);
          events.push(...transform(row));
          cursor = String(row[key]);
          sourceCount++;
        }
      }
      // 私有输入先写，但与引用它的开账事件、来源摘要共用这次 FULL 提交。
      if (sourceCount) chunk(table, index++, events, firstKey, cursor);
      return sourceCount;
    });
    while (importBlock()) {
      /* 每块最多读取 100 条；大 payload 仍逐条转换。 */
    }
  }
  records('users', 'id', (row) => [opening(`member:${row.id}`, { id: row.id })]);
  records('accounts', 'id', (row) => {
    if (row.id === openingAccountId) return [];
    const value = JSON.parse(String(row.data)) as Record<string, unknown>;
    return [opening(`account:${row.id}`, pick(value, accountFacts))];
  });
  records('jobs', 'id', (row) => {
    const job = JSON.parse(String(row.data)) as Record<string, unknown>;
    const streamId = `request:${job.requestId || `legacy-${job.id}`}`;
    const sourceJobs = job.requestId
      ? (
          sqlite
            .prepare('SELECT data FROM jobs WHERE request_id=? ORDER BY id')
            .all(job.requestId) as { data: string }[]
        ).map((value) => JSON.parse(value.data) as Record<string, unknown>)
      : [job];
    if (sourceJobs[0]?.id !== job.id) return [];
    const parent = job.requestId
      ? (sqlite.prepare('SELECT * FROM generation_requests WHERE id=?').get(job.requestId) as
          Record<string, unknown> | undefined)
      : undefined;
    let inputRef = parent?.id as string | undefined;
    let inputPayload = parent?.payload as string | undefined;
    if (!parent && (job.request || job.requestPayload)) {
      inputRef = `legacy-input:${job.id}`;
      const payload = canonicalJson({
        legacyInline: true,
        request: job.request || null,
        requestPayload: job.requestPayload || null,
      });
      const input = {
        id: inputRef,
        member_id: String(memberOf(job) || ''),
        source: String(job.source || 'legacy'),
        operation: String(
          (job.request as Record<string, unknown> | undefined)?.operation || 'generate',
        ),
        created_at: String(job.createdAt || recordedAt),
        payload_bytes: Buffer.byteLength(payload),
        payload,
      };
      const existing = sqlite
        .prepare('SELECT * FROM generation_requests WHERE id=?')
        .get(inputRef) as typeof input | undefined;
      if (
        existing &&
        Object.entries(input).some(([key, value]) => existing[key as keyof typeof input] !== value)
      )
        throw new Error(`Protected legacy generation input conflict ${inputRef}`);
      if (!existing)
        sqlite
          .prepare(
            'INSERT INTO generation_requests(id,member_id,source,operation,created_at,payload_bytes,payload) VALUES(@id,@member_id,@source,@operation,@created_at,@payload_bytes,@payload)',
          )
          .run(input);
      inputPayload = payload;
    }
    const jobs = Object.fromEntries(
      sourceJobs.map((item) => {
        const stableOwner =
          item.memberId ||
          (item.generationAttempt as { memberId?: string } | undefined)?.memberId ||
          (item.fairCharge as { userId?: string } | undefined)?.userId;
        if (parent?.member_id && stableOwner && parent.member_id !== stableOwner)
          throw new Error(`Conflicting legacy job ownership ${item.id}`);
        const fact = legacyPublicFact(item) as Record<string, unknown>;
        delete fact.requestPayload;
        fact.memberId = parent?.member_id || memberOf(item);
        if (item.request) fact.request = pick(object(item.request), requestSummaryFields);
        if (!parent && inputRef) {
          fact.legacyInputRef = inputRef;
          fact.legacyInlineInput = true;
        }
        if (typeof item.errorDetail === 'string' && item.errorDetail) {
          const digest = createHash('sha256').update(item.errorDetail).digest('hex'),
            ref = `diagnostic:${item.id}:${digest}`;
          sqlite
            .prepare(
              'INSERT OR IGNORE INTO generation_diagnostics(diagnostic_id,job_id,detail,detail_sha256,created_at) VALUES(?,?,?,?,?)',
            )
            .run(
              ref,
              item.id,
              item.errorDetail,
              digest,
              String(item.completedAt || item.updatedAt || recordedAt),
            );
          fact.diagnosticRef = ref;
        }
        return [String(item.id), fact];
      }),
    );
    return [
      opening(streamId, {
        request: {
          requestId: parent?.id || null,
          memberId: parent?.member_id || memberOf(job),
          source: job.source || 'legacy',
          operation:
            parent?.operation ||
            (job.request as Record<string, unknown> | undefined)?.operation ||
            'generate',
          createdAt: parent?.created_at || job.createdAt || recordedAt,
          inputRef: inputRef || null,
          inputSha256:
            inputPayload === undefined
              ? null
              : createHash('sha256').update(inputPayload).digest('hex'),
          legacy: true,
        },
        jobs,
        artifacts: {},
        aliases: [],
        freeUsage: {},
      }),
    ];
  });
  records('images', 'id', (row) => {
    const value = JSON.parse(String(row.data)) as Record<string, unknown>;
    const owners = new Set<string>();
    if (value.memberId || value.userId) owners.add(String(value.memberId || value.userId));
    const associated = sqlite
      .prepare(
        `SELECT DISTINCT COALESCE(r.member_id,json_extract(j.data,'$.memberId'),json_extract(j.data,'$.generationAttempt.memberId'),json_extract(j.data,'$.fairCharge.userId')) AS member_id
      FROM jobs j LEFT JOIN generation_requests r ON r.id=j.request_id WHERE j.image_id=? OR j.id=?`,
      )
      .all(value.id, value.jobId || '') as { member_id: string | null }[];
    for (const owner of associated) if (owner.member_id) owners.add(owner.member_id);
    if (owners.size > 1) throw new Error(`Conflicting legacy artifact ownership ${value.id}`);
    const memberId = [...owners][0] || memberOf(value);
    const inlinePrivate = pick(value, ['base64', 'prompt', 'fullPrompt']),
      savedPrivate = value.artifactInputRef
        ? readArtifactInput(
            sqlite,
            String(value.artifactInputRef),
            value.artifactInputSha256 as string,
          )
        : null;
    if (savedPrivate)
      for (const [key, value] of Object.entries(inlinePrivate))
        if (canonicalJson(value) !== canonicalJson(savedPrivate[key]))
          throw new Error(`Conflicting projected artifact private field ${row.id}.${key}`);
    const privateInput = savedPrivate || inlinePrivate;
    const fact = {
      ...(legacyPublicFact(value) as Record<string, unknown>),
      memberId,
      legacy: true,
    };
    if (Object.keys(privateInput).length) {
      const digest = domainDigest(privateInput),
        existing = sqlite
          .prepare('SELECT payload_sha256 FROM artifact_inputs WHERE artifact_id=?')
          .get(value.id) as { payload_sha256: string } | undefined;
      if (existing && existing.payload_sha256 !== digest)
        throw new Error(`Protected legacy artifact input conflict ${value.id}`);
      sqlite
        .prepare(
          'INSERT OR IGNORE INTO artifact_inputs(artifact_id,member_id,payload_sha256,payload) VALUES(?,?,?,?)',
        )
        .run(value.id, memberId, digest, canonicalJson(privateInput));
      Object.assign(fact, { artifactInputRef: value.id, artifactInputSha256: digest });
    }
    return [opening(`artifact:${row.id}`, fact)];
  });
  records('ledger', 'id', (row) => {
    const value = JSON.parse(String(row.data)) as Record<string, unknown>;
    return [
      {
        eventType: 'LegacyFactRecorded',
        streamId: `legacy-ledger:${row.id}`,
        payload: {
          record: {
            ...(legacyPublicFact(value) as Record<string, unknown>),
            memberId: memberOf(value),
          },
          order: row.order_value,
          source: 'legacy-ledger',
        },
      },
    ];
  });
  records('fair_quota_settlements', 'job_id', (row) => [
    opening(`settlement:${row.job_id}`, {
      jobId: row.job_id,
      charge: JSON.parse(String(row.charge)),
    }),
  ]);
  records('free_image_usage', 'job_id', (row) => [
    opening(`free-usage:${row.job_id}`, {
      jobId: row.job_id,
      memberId: row.member_id,
      succeededAt: row.succeeded_at,
      warningThreshold: row.warning_threshold,
    }),
  ]);
  let idempotencyIndex = 0;
  let mappingCursor = ['', '', ''];
  while (true) {
    const idempotencies = sqlite
      .prepare(
        'SELECT * FROM generation_idempotency WHERE (member_id,operation,key_hash)>(?,?,?) ORDER BY member_id,operation,key_hash LIMIT 100',
      )
      .all(...mappingCursor) as Record<string, unknown>[];
    if (!idempotencies.length) break;
    sqlite.transaction(() => {
      const mappings: DomainEventInput[] = [];
      for (const row of idempotencies) {
        const contextRef = `generation-idempotency:${row.member_id}:${row.operation}:${row.key_hash}`;
        sqlite
          .prepare('INSERT OR IGNORE INTO generation_contexts(context_ref,payload) VALUES(?,?)')
          .run(contextRef, row.normalization_context);
        mappings.push(
          opening(`idempotency:${domainDigest([row.member_id, row.operation, row.key_hash])}`, {
            memberId: row.member_id,
            operation: row.operation,
            keyHash: row.key_hash,
            fingerprint: row.fingerprint,
            normalizationContextRef: contextRef,
            normalizationContextHash: domainDigest(JSON.parse(String(row.normalization_context))),
            jobId: row.job_id,
            requestId: row.request_id,
            createdAt: row.created_at,
          }),
        );
      }
      chunk('idempotency', idempotencyIndex, mappings);
    })();
    idempotencyIndex++;
    const last = idempotencies.at(-1)!;
    mappingCursor = [String(last.member_id), String(last.operation), String(last.key_hash)];
  }
  const queue = readMeta.get('queue_sequence') as { value: string } | undefined;
  chunk('queue', 0, [opening('system:queue', { queueSequence: queue?.value || '0' })]);
  sqlite
    .prepare('UPDATE app_meta SET value=? WHERE key=?')
    .run(canonicalJson({ status: 'complete', recordedAt, version: 1, historyId }), 'event_opening');
  return { status: 'complete', importId, chunks, facts, recordedAt };
}
