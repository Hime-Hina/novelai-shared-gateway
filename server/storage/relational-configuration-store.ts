import { randomUUID } from 'node:crypto';
import type Database from 'better-sqlite3';
import { canonicalJson, domainDigest } from '../events/domain-events.ts';
import type {
  RelationalCommand,
  RelationalEvent,
  RelationalPayloads,
} from '../events/relational-events.ts';
import {
  readRelationalReceipt,
  readRelationalCommit,
  type StoredRelationalCommit,
} from '../events/relational-event-store.ts';
import {
  prepareNormalizationDefaultsProfile,
  configurationUnavailable,
} from '../generation/normalization-defaults.ts';
import { RelationalInputStore } from './relational-input-store.ts';
import { RelationalCommitReplayer } from './relational-replay.ts';
import { assertRelationalStorageFormat } from './relational-storage.ts';

type ConfigurationFact = Extract<RelationalEvent, { eventType: 'GatewayConfigurationInitialized' }>;
type ConfigurationPayload = RelationalPayloads['GatewayConfigurationInitialized'];
type Metadata = Pick<
  RelationalCommand,
  'commandId' | 'actorKind' | 'actorId' | 'recordedAt' | 'correlationId' | 'causationId'
>;
const streamId = 'configuration:gateway';
export const relationalConfigurationFacts = new Set<RelationalEvent['eventType']>([
  'GatewayConfigurationInitialized',
]);
function fingerprint(payload: ConfigurationPayload) {
  return domainDigest({ command: 'initialize-gateway-configuration', ...payload });
}
function receipt(fact: Pick<ConfigurationFact, 'eventId' | 'payload'>) {
  return { configurationEventId: fact.eventId, ...fact.payload };
}
function assertCommit(stored: StoredRelationalCommit): ConfigurationFact {
  const fact = stored.events[0];
  if (
    stored.events.length !== 1 ||
    fact?.eventType !== 'GatewayConfigurationInitialized' ||
    fact.streamId !== streamId ||
    fact.streamVersion !== '1' ||
    fact.accountId !== null ||
    fact.memberId !== null ||
    fact.occurredAt !== stored.command.recordedAt ||
    stored.command.actorKind !== 'system' ||
    stored.command.actorId !== null ||
    stored.command.fingerprint !== fingerprint(fact.payload) ||
    canonicalJson(stored.command.receipt) !== canonicalJson(receipt(fact))
  ) {
    configurationUnavailable();
  }
  return fact;
}

// 只初始化真实当前来源；没有配置编辑 caller，不开放任意 settings/覆盖命令。
export class RelationalConfigurationStore {
  private inputs: RelationalInputStore;
  private sqlite: Database.Database;
  constructor(sqlite: Database.Database) {
    assertRelationalStorageFormat(sqlite);
    this.sqlite = sqlite;
    this.inputs = new RelationalInputStore(sqlite);
  }

  readCurrent() {
    return this.sqlite.transaction(() => {
      const row = this.sqlite
        .prepare<[], { contextId: string; publicBaseUrl: string | null; sourceEventId: string }>(
          'SELECT normalization_context_id AS contextId,public_base_url AS publicBaseUrl,source_event_id AS sourceEventId FROM gateway_configuration WHERE id=1',
        )
        .get();
      const source =
        row &&
        this.sqlite
          .prepare<[string], { seq: string }>(
            'SELECT CAST(commit_seq AS TEXT) AS seq FROM domain_events WHERE event_id=?',
          )
          .get(row.sourceEventId);
      if (!row || !source) {
        configurationUnavailable();
      }
      const fact = assertCommit(readRelationalCommit(this.sqlite, source.seq));
      if (
        fact.eventId !== row.sourceEventId ||
        fact.payload.contextId !== row.contextId ||
        fact.payload.publicBaseUrl !== row.publicBaseUrl
      ) {
        configurationUnavailable();
      }
      const profile = this.inputs.readNormalizationDefaultsProfile(
        fact.payload.contextId,
        fact.payload.contextSha256,
      );
      return {
        normalizationDefaults: profile.values,
        publicBaseUrl: row.publicBaseUrl,
        originRef: fact.payload.originRef,
      };
    })();
  }

  readAdminSettings() {
    const current = this.readCurrent();
    return {
      serviceName: null,
      costPerImage: null,
      maxCacheImages: null,
      accountConcurrency: null,
      publicBaseUrl: current.publicBaseUrl,
      mockWhenNoAccount: null,
      // 没有选定的旧 PromptAPI 来源，不能把默认空配置冒充已知未配置。
      promptApi: null,
      ...current.normalizationDefaults,
    };
  }

  initializeGatewayConfiguration(
    metadata: Metadata,
    input: { normalizationDefaults: unknown; publicBaseUrl: string | null; originRef: string },
  ) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Configuration initialization requires the owning system transaction.');
    }
    return this.sqlite.transaction(() => {
      if (metadata.actorKind !== 'system' || metadata.actorId !== null) {
        throw Object.assign(new Error('当前配置只能由明确的系统初始化来源建立。'), {
          statusCode: 403,
        });
      }
      if (
        !input ||
        typeof input.originRef !== 'string' ||
        !input.originRef.trim() ||
        (input.publicBaseUrl !== null && typeof input.publicBaseUrl !== 'string')
      ) {
        configurationUnavailable();
      }
      const prepared = prepareNormalizationDefaultsProfile(input.normalizationDefaults),
        payload: ConfigurationPayload = {
          contextId: prepared.contextId,
          contextSha256: prepared.contextSha256,
          publicBaseUrl: input.publicBaseUrl,
          originRef: input.originRef,
        },
        command = { ...metadata, fingerprint: fingerprint(payload) },
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        assertCommit(readRelationalCommit(this.sqlite, prior.commitSeq));
        this.readCurrent();
        return prior;
      }
      if (
        this.sqlite.prepare('SELECT 1 FROM gateway_configuration').get() ||
        this.sqlite.prepare("SELECT 1 FROM projection_state WHERE status='building'").get() ===
          undefined
      ) {
        throw Object.assign(new Error('当前配置已初始化或目标不是 building。'), {
          statusCode: 409,
        });
      }
      this.inputs.stageNormalizationDefaultsProfile(prepared.profile.values);
      const eventId = randomUUID();
      return new RelationalCommitReplayer(this.sqlite).appendCommit(
        {
          ...command,
          expectedVersions: { [streamId]: '0' },
          receipt: receipt({ eventId, payload }),
          events: [
            {
              eventId,
              eventType: 'GatewayConfigurationInitialized',
              payload,
              streamId,
              occurredAt: metadata.recordedAt,
              accountId: null,
              memberId: null,
              policyVersion: 'gateway-configuration-v1',
              pricingVersion: 'not-applicable',
            },
          ],
        },
        this.inputs,
      );
    })();
  }

  createConfigurationProjector(
    stored: StoredRelationalCommit,
    source: RelationalInputStore,
    replayed: boolean,
  ) {
    const fact = assertCommit(stored),
      copy = this.inputs.createNormalizationDefaultsProjector(source, stored, replayed);
    return () => {
      copy();
      if (replayed) {
        this.readCurrent();
        return;
      }
      this.sqlite
        .prepare(
          'INSERT INTO gateway_configuration(id,normalization_context_id,public_base_url,source_event_id) VALUES(1,?,?,?)',
        )
        .run(fact.payload.contextId, fact.payload.publicBaseUrl, fact.eventId);
    };
  }
}
