import { createHash } from 'node:crypto';
import type Database from 'better-sqlite3';
import { canonicalJson, domainDigest } from '../events/domain-events.ts';
import {
  appendRelationalCommit,
  readRelationalReceipt,
  readRelationalCommit,
  replayRelationalCommit,
} from '../events/relational-event-store.ts';
import {
  parseRelationalFact,
  type RelationalCommand,
  type RelationalEvent,
  type RelationalPayloads,
} from '../events/relational-events.ts';
import { assertRelationalStorageFormat } from './relational-storage.ts';
import {
  prepareRelationalInput,
  parseRelationalNormalizationContext,
  relationalAssetSlots,
  relationalInputDigest,
  restoreRelationalWire,
  relationalProviderRequest,
  validateRelationalInputBody,
  validateRelationalAssetBytes,
  type RelationalInputAsset,
  type RelationalNormalizationContext,
} from '../generation/relational-input.ts';
import {
  memberInputAssetUrl,
  type RecoverableRequest,
  type MemberGenerationConfirmation,
} from '../generation/member-contract.ts';
import { singleGenerationRequest } from '../generation/batch-requests.ts';
import { generationPayloadLimits } from '../generation/payload-budget.ts';

type CommandMetadata = Omit<RelationalCommand, 'events' | 'expectedVersions' | 'receipt'> & {
  policyVersion: string;
  pricingVersion: string;
};
export interface AcceptedInput {
  inputId: string;
  requestId: string;
  memberId: string;
  source: string;
  seedMode: 'fixed' | 'random' | 'not_applicable';
  confirmation: MemberGenerationConfirmation | null;
  request: RecoverableRequest;
  context: RelationalNormalizationContext;
  jobs: {
    jobId: string;
    status: 'queued' | 'pending';
    queueSeq: string | null;
    queueExpiresAt: number | null;
    deadlineAt: number | null;
    admissionEpoch: number;
    cacheKey: string | null;
  }[];
}
interface InputRow {
  id: string;
  member_id: string;
  schema_version: number;
  completeness: 'complete' | 'partial' | 'unavailable';
  sha256: string | null;
  logical_bytes: number;
  body: string | null;
}
interface RequestRow {
  id: string;
  member_id: string;
  input_id: string;
  context_id: string | null;
  operation: 'generate' | 'upscale' | 'encode-vibe';
  sample_count: number;
  seed_mode: string;
  confirmation: string | null;
}
function fail(statusCode: number, code: string, message: string): never {
  throw Object.assign(new Error(message), { statusCode, code });
}
function unavailable(): never {
  return fail(410, 'input_unavailable', '原输入或首次上下文不可用。');
}

// 只拥有新格式受保护内容和受理引用；调度、资格/报价及收费决定仍由 T3/T4 命令拥有。
export class RelationalInputStore {
  private sqlite: Database.Database;
  private env: NodeJS.ProcessEnv;
  constructor(sqlite: Database.Database, env: NodeJS.ProcessEnv = process.env) {
    assertRelationalStorageFormat(sqlite);
    this.sqlite = sqlite;
    this.env = env;
  }

  private ownedRequest(requestId: string, memberId: string): RequestRow {
    const member = this.sqlite
      .prepare<[string], { status: string; auth_epoch: string | null }>(
        'SELECT status,auth_epoch FROM members WHERE id=?',
      )
      .get(memberId);
    if (member?.status !== 'active' || !member.auth_epoch) {
      fail(401, 'session_expired', '成员当前不可访问。');
    }
    return this.request(requestId, memberId);
  }

  private request(requestId: string, memberId: string): RequestRow {
    const request = this.sqlite
      .prepare<[string, string], RequestRow>(
        'SELECT id,member_id,input_id,context_id,operation,sample_count,seed_mode,confirmation FROM generation_requests WHERE id=? AND member_id=?',
      )
      .get(requestId, memberId);
    if (!request) {
      fail(404, 'request_not_found', '请求不存在。');
    }
    const registered = this.registeredFact(`request:${requestId}`, [
      'RequestAccepted',
      'OpeningRequestRecorded',
    ]);
    if (
      registered.eventType !== 'RequestAccepted' &&
      registered.eventType !== 'OpeningRequestRecorded'
    ) {
      unavailable();
    }
    const expected = registered.payload;
    if (
      expected.requestId !== request.id ||
      expected.memberId !== request.member_id ||
      expected.inputId !== request.input_id ||
      expected.contextId !== request.context_id ||
      expected.operation !== request.operation ||
      expected.sampleCount !== request.sample_count ||
      expected.seedMode !== request.seed_mode ||
      canonicalJson(expected.confirmation) !== (request.confirmation ?? 'null')
    ) {
      unavailable();
    }
    return request;
  }

  private registeredFact(streamId: string, types: string[]) {
    const rows = this.sqlite
      .prepare<[string], { event_type: string; payload: string }>(
        'SELECT event_type,payload FROM domain_events WHERE stream_id=? ORDER BY stream_version',
      )
      .all(streamId);
    const row = rows.find((entry) => types.includes(entry.event_type));
    if (!row) {
      unavailable();
    }
    return parseRelationalFact({ eventType: row.event_type, payload: JSON.parse(row.payload) });
  }

  private context(contextId: string): RelationalNormalizationContext {
    const row = this.sqlite
      .prepare<[string], { schema_version: number; sha256: string; body: string }>(
        'SELECT schema_version,sha256,body FROM normalization_contexts WHERE id=?',
      )
      .get(contextId);
    if (!row || row.schema_version !== 1) {
      unavailable();
    }
    const fact = this.registeredFact(`context:${contextId}`, ['ContextRegistered']);
    if (
      fact.eventType !== 'ContextRegistered' ||
      fact.payload.contextId !== contextId ||
      fact.payload.contextSchemaVersion !== row.schema_version ||
      fact.payload.contextSha256 !== row.sha256
    ) {
      unavailable();
    }
    const context = parseRelationalNormalizationContext(JSON.parse(row.body));
    if (domainDigest(context) !== row.sha256) {
      unavailable();
    }
    return context;
  }

  private input(inputId: string, memberId: string) {
    const row = this.sqlite
      .prepare<[string, string], InputRow>(
        'SELECT id,member_id,schema_version,completeness,sha256,logical_bytes,body FROM generation_inputs WHERE id=? AND member_id=?',
      )
      .get(inputId, memberId);
    if (!row || row.schema_version !== 1) {
      unavailable();
    }
    const assets = this.sqlite
      .prepare<[string], RelationalInputAsset>(
        'SELECT asset_id AS assetId,mime_type AS mimeType,byte_length AS byteLength,sha256 FROM input_assets WHERE input_id=? ORDER BY asset_id',
      )
      .all(inputId);
    const registered = this.registeredFact(`input:${inputId}`, [
      'InputRegistered',
      'OpeningInputRecorded',
    ]);
    if (
      registered.eventType !== 'InputRegistered' &&
      registered.eventType !== 'OpeningInputRecorded'
    ) {
      unavailable();
    }
    if (
      registered.payload.inputId !== inputId ||
      registered.payload.memberId !== memberId ||
      registered.payload.inputSchemaVersion !== row.schema_version ||
      registered.payload.inputSha256 !== row.sha256 ||
      registered.payload.logicalBytes !== row.logical_bytes ||
      (registered.eventType === 'InputRegistered'
        ? row.completeness !== 'complete'
        : registered.payload.completeness !== row.completeness)
    ) {
      unavailable();
    }
    const body = row.body === null ? null : (JSON.parse(row.body) as Record<string, unknown>);
    if (body) {
      if (row.completeness === 'unavailable') {
        unavailable();
      }
      validateRelationalInputBody(body, row.completeness === 'complete');
      relationalAssetSlots(body, assets);
      if (relationalInputDigest(body, assets, row.logical_bytes) !== row.sha256) {
        unavailable();
      }
    } else if (row.completeness !== 'unavailable' || assets.length) {
      unavailable();
    }
    return { row, body, assets };
  }

  activePayloadBytes(): number {
    const row = this.sqlite
      .prepare<[], { bytes: number }>(
        `SELECT COALESCE(SUM(i.logical_bytes),0) AS bytes
      FROM (SELECT DISTINCT request_id FROM generation_jobs WHERE status IN ('queued','pending','running')) active
      JOIN generation_requests r ON r.id=active.request_id
      JOIN generation_inputs i ON i.id=r.input_id`,
      )
      .get()!;
    if (!Number.isSafeInteger(row.bytes) || row.bytes < 0) {
      throw new Error('Unsafe active input byte total.');
    }
    return row.bytes;
  }

  // 必须由受理命令持有事务；这里不提供独立的正文写入或跨成员 blob 查找 API。
  persistAcceptedInput(metadata: CommandMetadata, accepted: AcceptedInput) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Accepted input requires the owning command transaction.');
    }
    return this.sqlite.transaction(() => {
      const member = this.sqlite
        .prepare<
          [string],
          {
            status: string;
            auth_epoch: string | null;
            accepting_jobs: number;
            admission_epoch: number;
          }
        >('SELECT status,auth_epoch,accepting_jobs,admission_epoch FROM members WHERE id=?')
        .get(accepted.memberId);
      if (
        member?.status !== 'active' ||
        !member.auth_epoch ||
        (metadata.actorKind === 'member' && metadata.actorId !== accepted.memberId)
      ) {
        fail(403, 'membership_required', '成员当前不可受理。');
      }
      // 当前授权先于重领回执；暂停新接单不拦截已经受理的同键操作。
      const prior = readRelationalReceipt(this.sqlite, metadata);
      if (prior) {
        return prior;
      }
      if (
        !member.accepting_jobs ||
        accepted.jobs.some((job) => job.admissionEpoch !== member.admission_epoch)
      ) {
        fail(409, 'admission_changed', '接单状态已变化。');
      }
      const prepared = prepareRelationalInput(accepted.request, accepted.context, this.env);
      if (
        (accepted.context.kind === 'member' &&
          prepared.firstSeed !== null &&
          accepted.context.member.randomSeed !== (accepted.seedMode === 'random')) ||
        (accepted.request.confirmation !== undefined &&
          canonicalJson(accepted.request.confirmation) !== canonicalJson(accepted.confirmation))
      ) {
        unavailable();
      }
      if (
        accepted.jobs.length !== prepared.sampleCount ||
        new Set(accepted.jobs.map((job) => job.jobId)).size !== accepted.jobs.length ||
        (prepared.firstSeed === null) !== (accepted.seedMode === 'not_applicable')
      ) {
        unavailable();
      }
      if (
        this.activePayloadBytes() + prepared.logicalBytes >
        generationPayloadLimits(this.env).active
      ) {
        fail(429, 'payload_budget_exhausted', '活动生成输入超过载荷预算。');
      }
      const contextExists = this.sqlite
        .prepare<[string], { id: string }>(
          'SELECT id FROM normalization_contexts WHERE schema_version=1 AND sha256=?',
        )
        .get(prepared.contextSha256);
      if (contextExists) {
        prepared.contextId = contextExists.id;
        if (canonicalJson(this.context(prepared.contextId)) !== canonicalJson(prepared.context)) {
          unavailable();
        }
      }
      const common = {
        occurredAt: metadata.recordedAt,
        memberId: accepted.memberId,
        accountId: null,
        policyVersion: metadata.policyVersion,
        pricingVersion: metadata.pricingVersion,
      };
      const events: RelationalCommand['events'] = [];
      if (!contextExists) {
        events.push({
          ...common,
          streamId: `context:${prepared.contextId}`,
          eventType: 'ContextRegistered',
          payload: {
            contextId: prepared.contextId,
            contextSchemaVersion: 1,
            contextSha256: prepared.contextSha256,
          },
        });
      }
      events.push(
        {
          ...common,
          streamId: `input:${accepted.inputId}`,
          eventType: 'InputRegistered',
          payload: {
            inputId: accepted.inputId,
            memberId: accepted.memberId,
            inputSchemaVersion: 1,
            inputSha256: prepared.sha256,
            logicalBytes: prepared.logicalBytes,
          },
        },
        {
          ...common,
          streamId: `request:${accepted.requestId}`,
          eventType: 'RequestAccepted',
          payload: {
            requestId: accepted.requestId,
            memberId: accepted.memberId,
            inputId: accepted.inputId,
            contextId: prepared.contextId,
            source: accepted.source,
            operation: prepared.body.operation as RequestRow['operation'],
            sampleCount: prepared.sampleCount,
            seedMode: accepted.seedMode,
            confirmation: accepted.confirmation,
          },
        },
      );
      accepted.jobs.forEach((job, sampleIndex) => {
        const actualSeed =
          prepared.firstSeed === null
            ? null
            : singleGenerationRequest(
                { seed: prepared.firstSeed, nativeParameters: {} },
                sampleIndex,
              ).seed;
        events.push({
          ...common,
          streamId: `request:${accepted.requestId}`,
          eventType: 'JobAccepted',
          payload: {
            ...job,
            requestId: accepted.requestId,
            sampleIndex,
            actualSeed,
          },
        });
      });
      const expectedVersions = Object.fromEntries(events.map((event) => [event.streamId, '0']));
      const { policyVersion: _policy, pricingVersion: _pricing, ...command } = metadata;
      const result = appendRelationalCommit(this.sqlite, {
        ...command,
        events,
        expectedVersions,
        receipt: { requestId: accepted.requestId, jobIds: accepted.jobs.map((job) => job.jobId) },
      });
      if (!contextExists) {
        this.sqlite
          .prepare(
            'INSERT INTO normalization_contexts(id,schema_version,sha256,body) VALUES(?,1,?,?)',
          )
          .run(prepared.contextId, prepared.contextSha256, canonicalJson(prepared.context));
      }
      this.sqlite
        .prepare(
          "INSERT INTO generation_inputs(id,member_id,schema_version,completeness,sha256,logical_bytes,body) VALUES(?,?,1,'complete',?,?,?)",
        )
        .run(
          accepted.inputId,
          accepted.memberId,
          prepared.sha256,
          prepared.logicalBytes,
          canonicalJson(prepared.body),
        );
      const insertAsset = this.sqlite.prepare(
        'INSERT INTO input_assets(input_id,asset_id,mime_type,byte_length,sha256,bytes) VALUES(?,?,?,?,?,?)',
      );
      for (const asset of prepared.assets) {
        insertAsset.run(
          accepted.inputId,
          asset.assetId,
          asset.mimeType,
          asset.byteLength,
          asset.sha256,
          asset.bytes,
        );
      }
      this.applyAcceptedEvents(result.events);
      return result;
    })();
  }

  private applyAcceptedEvents(events: RelationalEvent[]): void {
    for (const event of events) {
      if (event.eventType === 'RequestAccepted') {
        const p = event.payload;
        this.sqlite
          .prepare(
            `INSERT INTO generation_requests(id,member_id,input_id,context_id,source,operation,sample_count,seed_mode,confirmation,accepted_at,source_event_id)
          VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
          )
          .run(
            p.requestId,
            p.memberId,
            p.inputId,
            p.contextId,
            p.source,
            p.operation,
            p.sampleCount,
            p.seedMode,
            p.confirmation === null ? null : canonicalJson(p.confirmation),
            event.occurredAt,
            event.eventId,
          );
      } else if (event.eventType === 'JobAccepted') {
        const p = event.payload;
        this.sqlite
          .prepare(
            `INSERT INTO generation_jobs(id,request_id,sample_index,actual_seed,status,queue_seq,queue_expires_at,deadline_at,admission_epoch,cache_key,source_event_id)
          VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
          )
          .run(
            p.jobId,
            p.requestId,
            p.sampleIndex,
            p.actualSeed,
            p.status,
            p.queueSeq === null ? null : BigInt(p.queueSeq),
            p.queueExpiresAt,
            p.deadlineAt,
            p.admissionEpoch,
            p.cacheKey,
            event.eventId,
          );
      }
    }
  }

  // T2 的有界重放：调用者先恢复成员基线；这里只有输入/上下文及受理引用，不接管账务 reducer。
  replayAcceptedInput(source: RelationalInputStore, commitSeq: string) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Input replay requires the owning import transaction.');
    }
    return this.sqlite.transaction(() => {
      const stored = readRelationalCommit(source.sqlite, commitSeq);
      if (
        stored.events.some(
          (event) =>
            !['InputRegistered', 'ContextRegistered', 'RequestAccepted', 'JobAccepted'].includes(
              event.eventType,
            ),
        )
      ) {
        throw new Error("Input replay cannot apply another owner's facts.");
      }
      const contexts = new Map<string, RelationalNormalizationContext>();
      const inputs = new Map<string, ReturnType<RelationalInputStore['input']>>();
      for (const event of stored.events) {
        if (event.eventType === 'ContextRegistered') {
          contexts.set(event.payload.contextId, source.context(event.payload.contextId));
        }
        if (event.eventType === 'RequestAccepted') {
          contexts.set(event.payload.contextId, source.context(event.payload.contextId));
        }
        if (event.eventType === 'InputRegistered') {
          inputs.set(
            event.payload.inputId,
            source.input(event.payload.inputId, event.payload.memberId),
          );
        }
      }
      const content = [...inputs].map(([inputId, input]) => ({
        inputId,
        ...input,
        assets: input.assets.map((asset) => ({
          ...asset,
          bytes: source.asset(inputId, asset).bytes,
        })),
      }));
      const result = replayRelationalCommit(this.sqlite, stored);
      for (const [contextId, context] of contexts) {
        const existing = this.sqlite
          .prepare('SELECT 1 FROM normalization_contexts WHERE id=?')
          .get(contextId);
        if (existing) {
          if (canonicalJson(this.context(contextId)) !== canonicalJson(context)) {
            unavailable();
          }
        } else {
          if (result.replayed) {
            unavailable();
          }
          this.sqlite
            .prepare(
              'INSERT INTO normalization_contexts(id,schema_version,sha256,body) VALUES(?,1,?,?)',
            )
            .run(contextId, domainDigest(context), canonicalJson(context));
        }
      }
      for (const input of content) {
        if (result.replayed) {
          const existing = this.input(input.inputId, input.row.member_id);
          if (
            canonicalJson(existing) !==
            canonicalJson({
              row: input.row,
              body: input.body,
              assets: input.assets.map(({ bytes: _bytes, ...asset }) => asset),
            })
          ) {
            unavailable();
          }
          for (const asset of input.assets) {
            if (!this.asset(input.inputId, asset).bytes.equals(asset.bytes)) {
              unavailable();
            }
          }
          continue;
        }
        this.sqlite
          .prepare(
            'INSERT INTO generation_inputs(id,member_id,schema_version,completeness,sha256,logical_bytes,body) VALUES(?,?,?,?,?,?,?)',
          )
          .run(
            input.row.id,
            input.row.member_id,
            input.row.schema_version,
            input.row.completeness,
            input.row.sha256,
            input.row.logical_bytes,
            input.row.body,
          );
        const insert = this.sqlite.prepare(
          'INSERT INTO input_assets(input_id,asset_id,mime_type,byte_length,sha256,bytes) VALUES(?,?,?,?,?,?)',
        );
        for (const asset of input.assets) {
          insert.run(
            input.inputId,
            asset.assetId,
            asset.mimeType,
            asset.byteLength,
            asset.sha256,
            asset.bytes,
          );
        }
      }
      if (!result.replayed) {
        this.applyAcceptedEvents(result.events);
      }
      return result;
    })();
  }

  private jobSeed(requestId: string, jobId?: string): number | null {
    const row = jobId
      ? this.sqlite
          .prepare<[string, string], { event_type: string; payload: string }>(
            `SELECT event_type,payload FROM domain_events WHERE stream_id=? AND event_type IN ('JobAccepted','OpeningJobRecorded')
       AND json_extract(payload,'$.jobId')=? ORDER BY stream_version LIMIT 1`,
          )
          .get(`request:${requestId}`, jobId)
      : this.sqlite
          .prepare<[string], { event_type: string; payload: string }>(
            `SELECT event_type,payload FROM domain_events WHERE stream_id=? AND event_type IN ('JobAccepted','OpeningJobRecorded')
       AND json_extract(payload,'$.sampleIndex')=0 ORDER BY stream_version LIMIT 1`,
          )
          .get(`request:${requestId}`);
    if (!row) {
      unavailable();
    }
    const fact = parseRelationalFact({
      eventType: row.event_type,
      payload: JSON.parse(row.payload),
    });
    if (fact.eventType !== 'JobAccepted' && fact.eventType !== 'OpeningJobRecorded') {
      unavailable();
    }
    if (fact.payload.requestId !== requestId) {
      unavailable();
    }
    const projected = this.sqlite
      .prepare<[string], { request_id: string; sample_index: number; actual_seed: number | null }>(
        'SELECT request_id,sample_index,actual_seed FROM generation_jobs WHERE id=?',
      )
      .get(fact.payload.jobId);
    if (
      projected &&
      (projected.request_id !== requestId ||
        projected.sample_index !== fact.payload.sampleIndex ||
        projected.actual_seed !== fact.payload.actualSeed)
    ) {
      unavailable();
    }
    return fact.payload.actualSeed;
  }

  readMemberContext(requestId: string, memberId: string): RelationalNormalizationContext {
    const request = this.ownedRequest(requestId, memberId);
    if (!request.context_id) {
      unavailable();
    }
    return this.context(request.context_id);
  }

  readMemberInput(requestId: string, memberId: string) {
    const request = this.ownedRequest(requestId, memberId),
      input = this.input(request.input_id, memberId);
    const body = input.body ? structuredClone(input.body) : null;
    const assets: { slot: string; mimeType: string; byteLength: number; url: string }[] = [];
    if (body) {
      for (const slot of relationalAssetSlots(body, input.assets)) {
        const descriptor = input.assets.find(
          (asset) => asset.assetId === (slot.value as { assetId: string }).assetId,
        )!;
        assets.push({
          slot: slot.slot,
          mimeType: descriptor.mimeType,
          byteLength: descriptor.byteLength,
          url: memberInputAssetUrl(requestId, slot.slot),
        });
        (slot.owner as Record<string | number, unknown>)[slot.key] = { slot: slot.slot };
      }
    }
    let recoverable = input.row.completeness === 'complete';
    if (recoverable) {
      if (!request.context_id) {
        unavailable();
      }
      this.context(request.context_id);
    }
    if (body?.operation === 'generate' && recoverable) {
      const firstSeed = this.jobSeed(requestId);
      recoverable &&= firstSeed !== null;
      if (recoverable) {
        Object.assign(body.parameters as object, {
          seed: firstSeed,
          n_samples: request.sample_count,
        });
      }
    }
    if (body) {
      delete body.displayPrompt;
      delete body.requestedSteps;
    }
    return {
      contractVersion: 1,
      requestId,
      input: body,
      assets,
      confirmation:
        request.confirmation === null
          ? null
          : (JSON.parse(request.confirmation) as MemberGenerationConfirmation),
      recoverable,
    };
  }

  readMemberAsset(requestId: string, memberId: string, slotName: string) {
    const request = this.ownedRequest(requestId, memberId),
      input = this.input(request.input_id, memberId);
    if (input.row.completeness !== 'complete' || !input.body || !request.context_id) {
      unavailable();
    }
    const context = this.context(request.context_id);
    const slot = relationalAssetSlots(input.body, input.assets).find(
      (entry) => entry.slot === slotName,
    );
    if (!slot) {
      fail(404, 'input_not_found', '原输入附件不存在。');
    }
    const assetId = (slot.value as { assetId: string }).assetId;
    const descriptor = input.assets.find((asset) => asset.assetId === assetId)!;
    const asset = this.asset(request.input_id, descriptor);
    validateRelationalAssetBytes({ ...descriptor, bytes: asset.bytes }, slot.image, context);
    return asset;
  }

  private asset(inputId: string, descriptor: RelationalInputAsset) {
    const row = this.sqlite
      .prepare<[string, string], { bytes: Buffer }>(
        'SELECT bytes FROM input_assets WHERE input_id=? AND asset_id=?',
      )
      .get(inputId, descriptor.assetId);
    if (
      !row ||
      row.bytes.length !== descriptor.byteLength ||
      createHash('sha256').update(row.bytes).digest('hex') !== descriptor.sha256
    ) {
      unavailable();
    }
    return { mimeType: descriptor.mimeType, bytes: row.bytes };
  }

  readJobInput(jobId: string) {
    const job = this.sqlite
      .prepare<
        [string],
        {
          input_id: string;
          member_id: string;
          context_id: string | null;
          actual_seed: number | null;
          request_id: string;
        }
      >(
        `SELECT r.input_id,r.member_id,r.context_id,j.actual_seed,j.request_id FROM generation_jobs j
       JOIN generation_requests r ON r.id=j.request_id WHERE j.id=?`,
      )
      .get(jobId);
    if (!job || !job.context_id) {
      unavailable();
    }
    this.request(job.request_id, job.member_id);
    const seed = this.jobSeed(job.request_id, jobId);
    const input = this.input(job.input_id, job.member_id);
    if (!input.body || input.row.completeness !== 'complete') {
      unavailable();
    }
    const context = this.context(job.context_id);
    const assets = input.assets.map((descriptor) => ({
      ...descriptor,
      bytes: this.asset(job.input_id, descriptor).bytes,
    }));
    for (const slot of relationalAssetSlots(input.body, assets)) {
      const asset = assets.find(
        (entry) => entry.assetId === (slot.value as { assetId: string }).assetId,
      )!;
      validateRelationalAssetBytes(asset, slot.image, context);
    }
    const wire = restoreRelationalWire(input.body, assets, seed, 1);
    const request = relationalProviderRequest(wire, context);
    return {
      ...request,
      sampleCount: 1,
      ...(wire.operation === 'generate'
        ? {
            tag: (input.body.displayPrompt ?? wire.input) as string,
            requestedSteps: (input.body.requestedSteps ?? request.steps) as number,
          }
        : {}),
    };
  }
}
