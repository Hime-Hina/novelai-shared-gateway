import { createHash, randomUUID } from 'node:crypto';

export const DOMAIN_SCHEMA_VERSION = 1;
export const DOMAIN_REDUCER_VERSION = 1;
export const DOMAIN_AMOUNT_VERSION = 'decimal-12-v1';
export const domainEventTypes = [
  'OpeningStateRecorded', 'LegacyFactRecorded', 'SharingConfigured', 'AllocationChanged', 'MemberTransferred', 'MemberRemoved',
  'AccountBound', 'AccountRegistered', 'AvailabilityChanged', 'QuotaObserved', 'RefillApplied', 'QuotaReserved',
  'GenerationSettled', 'GenerationRefunded', 'FundingRegistered', 'FundingReceived', 'FundsTransferred',
  'ResourceGapObserved', 'ReturnClassified', 'OperatingLossCovered', 'PolicyChanged', 'CooldownChanged',
  'RequestAccepted', 'JobQueued', 'DispatchAuthorized', 'ArtifactPrepared', 'ArtifactPublished', 'JobCompleted', 'JobFailed',
  'WaitingJobsCancelled', 'LateResultRecovered', 'FreeImageSucceeded', 'ReviewOpened', 'ReviewClosed', 'AdmissionChanged',
  'CachedResultLinked', 'RequestAliasRegistered', 'AllocationApplied', 'V5CapacityObserved', 'DebitsConfirmed', 'MemberEligibilityChanged', 'QuotaAccountOpened', 'ArtifactStorageChanged'
] as const;
export type DomainEventType = typeof domainEventTypes[number];
export type DomainActorKind = 'system' | 'member' | 'admin';
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export interface DomainEventInput {
  eventType: DomainEventType; streamId: string; payload: Record<string, unknown>; eventId?: string;
  occurredAt?: string | number; accountId?: string | null; memberId?: string | null; schemaVersion?: number;
  policyVersion?: string; pricingVersion?: string; amountVersion?: string;
}
export interface DomainCommandMetadata {
  commandId?: string; fingerprint?: string; recordedAt?: string; actorId?: string | null;
  actorKind?:DomainActorKind;
  correlationId?: string | null; causationId?: string | null; expectedVersions?: Record<string, string>;
  receipt?: Json;
}
export interface DomainCommand {
  commandId: string; fingerprint?: string; recordedAt: string; actorId: string | null; actorKind:DomainActorKind;correlationId: string | null;
  causationId: string | null; expectedVersions: Record<string, string>; receipt: Json; events: DomainEventInput[];
}
export interface DomainEvent {
  eventId: string; commitSeq: string; eventIndex: number; commandId: string; transactionId:string;streamId: string; streamKind: string; streamVersion: string;
  eventType: DomainEventType; schemaVersion: number; occurredAt: string; recordedAt: string;
  actorId: string | null; actorKind:DomainActorKind;correlationId: string | null; causationId: string | null; accountId: string | null; memberId: string | null;
  policyVersion: string; pricingVersion: string; amountVersion: string; payload: Record<string, unknown>;
}
export interface DomainCommit { commitSeq: string; commandId: string; fingerprint: string; recordedAt: string; receipt: Json; events: DomainEvent[] }

const commands = new WeakMap<object, DomainCommand>();
const closedCommands = new WeakSet<DomainCommand>();
const knownTypes = new Set<string>(domainEventTypes);
const forbiddenKeys = /^(?:userToken|token|accessToken|refreshToken|access_token|refresh_token|pst|password|passwordHash|password_hash|authorization|cookie|apiKey|api_key|secret|base64|prompt|fullPrompt|negativePrompt|negative_prompt|uc)$/i;

export function canonicalJson(value: unknown): string {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return JSON.stringify(value);
  if (typeof value === 'number' && Number.isFinite(value)) return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value && typeof value === 'object' && Object.getPrototypeOf(value) === Object.prototype) {
    return `{${Object.entries(value).filter(([,item]) => item !== undefined).sort(([a],[b]) => a < b ? -1 : a > b ? 1 : 0).map(([key,item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`).join(',')}}`;
  }
  throw new Error('Domain facts must contain only finite JSON values.');
}
export const domainDigest = (value: unknown): string => createHash('sha256').update(canonicalJson(value)).digest('hex');

export function assertPublicDomainFact(value: unknown, at = 'payload'): void {
  if (Array.isArray(value)) { value.forEach((item,index) => assertPublicDomainFact(item, `${at}[${index}]`)); return; }
  if (value && typeof value === 'object') {
    for (const [key,item] of Object.entries(value)) {
      if (item === undefined) continue;
      if (forbiddenKeys.test(key)) throw new Error(`Protected request or authentication field is forbidden in domain events: ${at}.${key}`);
      assertPublicDomainFact(item, `${at}.${key}`);
    }
  }
  canonicalJson(value);
}

export function assertDomainEvent(input: DomainEventInput | DomainEvent): void {
  const id = input.eventId || '(new event)';
  if (!knownTypes.has(input.eventType) || (input.schemaVersion ?? 1) !== DOMAIN_SCHEMA_VERSION) {
    throw new Error(`Unsupported domain event ${id}: ${input.eventType} schema ${input.schemaVersion ?? 1}`);
  }
  if (!input.streamId || !/^[a-z][a-z-]*:.+/.test(input.streamId)) throw new Error(`Invalid stream for event ${id}`);
  if (!input.payload || Array.isArray(input.payload) || typeof input.payload !== 'object') throw new Error(`Invalid payload for event ${id}`);
  assertPublicDomainFact(input.payload);
  if (Buffer.byteLength(canonicalJson(input.payload)) > 4 * 1024 * 1024) throw new Error(`Domain event ${id} exceeds the fact payload limit`);
  if (input.amountVersion && input.amountVersion !== DOMAIN_AMOUNT_VERSION) throw new Error(`Unsupported amount version in event ${id}`);
}

export function beginDomainCommand(db: object, metadata: DomainCommandMetadata = {}): DomainCommand {
  if (commands.has(db)) throw new Error('A domain command is already active for this state.');
  assertPublicDomainFact(metadata.receipt ?? null,'receipt');
  const command: DomainCommand = {
    commandId: metadata.commandId || randomUUID(), fingerprint: metadata.fingerprint,
    recordedAt: metadata.recordedAt || new Date().toISOString(), actorId: metadata.actorId || null,
    actorKind:metadata.actorKind||'system',
    correlationId: metadata.correlationId || null, causationId: metadata.causationId || null,
    expectedVersions: { ...metadata.expectedVersions }, receipt: structuredClone(metadata.receipt ?? null), events: []
  };
  commands.set(db, command);
  return command;
}

// 领域函数可在验证副本上决定结果；显式继承同一命令后才收集副本产生的事实。
export function inheritDomainCommand(source: object, draft: object): void {
  const command = commands.get(source);
  if(command && closedCommands.has(command))throw new Error('Domain command is closed.');
  if (command) commands.set(draft, command);
}
export function recordDomainEvent(db: object, input: DomainEventInput): string | null {
  const command = commands.get(db);
  if (!command) return null;
  if(closedCommands.has(command))throw new Error('Domain command is closed.');
  assertDomainEvent(input);
  const event = structuredClone({ ...input, eventId: input.eventId || randomUUID(), occurredAt: input.occurredAt ?? command.recordedAt });
  command.events.push(event);
  return event.eventId;
}
export function setDomainCommandReceipt(db: object, receipt: Json): void {
  const command = commands.get(db);
  if (!command) throw new Error('No domain command is active.');
  if(closedCommands.has(command))throw new Error('Domain command is closed.');
  assertPublicDomainFact(receipt, 'receipt');
  command.receipt = structuredClone(receipt);
}
export function setDomainCommandActor(db:object,actor:{actorId:string|null;actorKind:DomainActorKind;correlationId?:string|null;causationId?:string|null}):void {
  const command=commands.get(db);
  if(!command||closedCommands.has(command))throw new Error('No open domain command is active.');
  if(!['system','member','admin'].includes(actor.actorKind)||(actor.actorKind!=='system'&&!actor.actorId))throw new Error('A member or admin command requires a stable actor ID.');
  command.actorId=actor.actorId;command.actorKind=actor.actorKind;
  if(actor.correlationId!==undefined)command.correlationId=actor.correlationId;
  if(actor.causationId!==undefined)command.causationId=actor.causationId;
}
export function finishDomainCommand(db: object): DomainCommand | null {
  const command = commands.get(db) || null;
  commands.delete(db);
  if(command)closedCommands.add(command);
  return command;
}
