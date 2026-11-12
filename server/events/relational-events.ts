import { parseRelationalFact, relationalFactStream } from './relational-fact-contracts.ts';
import { canonicalJson } from './domain-events.ts';
import type { Json } from './domain-events.ts';
import type { RelationalFact, RelationalPayloads } from './relational-fact-contracts.ts';
export { parseRelationalFact } from './relational-fact-contracts.ts';
export type { RelationalFact, RelationalPayloads } from './relational-fact-contracts.ts';

export const RELATIONAL_EVENT_SCHEMA_VERSION = 2;

// 生命周期类型直接来自同一字段校验表，不能另维护一份可漂移的写入接口。
export type RelationalLifecyclePayloads = Pick<
  RelationalPayloads,
  | 'InputRegistered'
  | 'RequestAccepted'
  | 'JobAccepted'
  | 'DispatchAuthorized'
  | 'QuotaReserved'
  | 'GenerationSettled'
  | 'GenerationRefunded'
  | 'ResponseEvidenceRecorded'
  | 'ArtifactPrepared'
  | 'ArtifactPublished'
  | 'JobCompleted'
  | 'JobFailed'
  | 'LateResultRecovered'
  | 'CachedResultLinked'
  | 'FreeImageSucceeded'
>;
export type RelationalResponseEvidence = RelationalPayloads['ResponseEvidenceRecorded']['response'];
export type RelationalLifecycleFact = Extract<
  RelationalFact,
  { eventType: keyof RelationalLifecyclePayloads }
>;
export type RelationalLifecycleEvent = Extract<
  RelationalEvent,
  { eventType: keyof RelationalLifecyclePayloads }
>;
export type RelationalEventInput = RelationalFact & {
  streamId: string;
  occurredAt: number;
  accountId: string | null;
  memberId: string | null;
  policyVersion: string;
  pricingVersion: string;
};
export type RelationalEvent = RelationalEventInput & {
  eventId: string;
  commitSeq: string;
  eventIndex: number;
  streamKind: string;
  streamVersion: string;
  schemaVersion: typeof RELATIONAL_EVENT_SCHEMA_VERSION;
  correlationId: string | null;
  causationId: string | null;
  amountVersion: 'decimal-12-v1';
};
export interface RelationalCommand {
  commandId: string;
  fingerprint: string;
  actorKind: 'system' | 'member' | 'admin';
  actorId: string | null;
  recordedAt: number;
  expectedVersions: Record<string, string>;
  receipt: Json;
  correlationId: string | null;
  causationId: string | null;
  events: RelationalEventInput[];
}

export function parseRelationalEvent(value: unknown): RelationalEvent {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError('Invalid relational event envelope.');
  }
  const row = value as Record<string, unknown>;
  const keys = [
    'eventType',
    'payload',
    'eventId',
    'commitSeq',
    'eventIndex',
    'streamId',
    'streamKind',
    'streamVersion',
    'schemaVersion',
    'occurredAt',
    'correlationId',
    'causationId',
    'accountId',
    'memberId',
    'policyVersion',
    'pricingVersion',
    'amountVersion',
  ];
  if (
    Object.keys(row).length !== keys.length ||
    Object.keys(row).some((key) => !keys.includes(key)) ||
    row.schemaVersion !== RELATIONAL_EVENT_SCHEMA_VERSION ||
    row.amountVersion !== 'decimal-12-v1'
  ) {
    throw new TypeError('Unsupported relational event schema or amount version.');
  }
  const fact = parseRelationalFact({ eventType: row.eventType, payload: row.payload });
  if (row.streamKind !== relationalFactStream(fact.eventType)) {
    throw new TypeError('Relational event has the wrong stream owner.');
  }
  const payload = fact.payload as Record<string, unknown>;
  if (
    row.streamKind === 'account' &&
    (typeof row.accountId !== 'string' ||
      !row.accountId ||
      row.streamId !== `account:${row.accountId}` ||
      (typeof payload.originalAccountId === 'string' &&
        payload.originalAccountId !== row.accountId))
  ) {
    throw new TypeError('Relational account stream does not match the fact identity.');
  }
  for (const key of ['memberId', 'accountId']) {
    if (typeof payload[key] === 'string' && payload[key] !== row[key]) {
      throw new TypeError('Relational event reference does not match the fact identity.');
    }
  }
  const identityField: Record<string, string> = {
    member: 'memberId',
    account: 'accountId',
    input: 'inputId',
    context: 'contextId',
    request: 'requestId',
    artifact: 'artifactId',
    storage: 'storageTargetId',
    import: 'sourceId',
  };
  const identity = payload[identityField[row.streamKind as string]];
  if (typeof identity === 'string' && row.streamId !== `${row.streamKind}:${identity}`) {
    throw new TypeError('Relational stream does not match the fact identity.');
  }
  for (const key of ['eventId', 'streamId', 'streamKind', 'policyVersion', 'pricingVersion']) {
    if (typeof row[key] !== 'string' || !row[key] || (row[key] as string).includes('\0')) {
      throw new TypeError('Invalid relational event identity.');
    }
  }
  for (const key of ['accountId', 'memberId', 'correlationId', 'causationId']) {
    if (
      row[key] !== null &&
      (typeof row[key] !== 'string' || !row[key] || (row[key] as string).includes('\0'))
    ) {
      throw new TypeError('Invalid relational event reference.');
    }
  }
  if (
    !/^[a-z][a-z-]*:.+$/u.test(row.streamId as string) ||
    (row.streamId as string).split(':')[0] !== row.streamKind ||
    !Number.isSafeInteger(row.occurredAt) ||
    (row.occurredAt as number) < 0 ||
    !Number.isSafeInteger(row.eventIndex) ||
    (row.eventIndex as number) < 0
  ) {
    throw new TypeError('Invalid relational event stream or time.');
  }
  for (const key of ['commitSeq', 'streamVersion']) {
    if (
      typeof row[key] !== 'string' ||
      !/^[1-9][0-9]*$/.test(row[key] as string) ||
      BigInt(row[key] as string) > 9_223_372_036_854_775_807n
    ) {
      throw new TypeError('Invalid relational event sequence.');
    }
  }
  canonicalJson(row);
  return { ...row, ...fact } as RelationalEvent;
}
