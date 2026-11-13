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
  relationalInputDisplay,
  validateRelationalInputBody,
  validateRelationalAssetBytes,
  type RelationalInputAsset,
  type RelationalNormalizationContext,
} from '../generation/relational-input.ts';
import {
  memberInputAssetUrl,
  assertMemberGenerationIntent,
  memberGenerationIntentFingerprint,
  type RecoverableRequest,
  type MemberGenerationConfirmation,
} from '../generation/member-contract.ts';
import { generationRequestFingerprint } from '../generation/request-fingerprint.ts';
import { singleGenerationRequest } from '../generation/batch-requests.ts';
import { generationPayloadLimits } from '../generation/payload-budget.ts';
import { validateImageBytes } from '../protocols/native-image.ts';
import type { ErrorLogRequest } from '../reports/generation-errors.ts';

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
  idempotency?: { keyHash: string; noCache: boolean };
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
  model: string | null;
  sample_count: number;
  seed_mode: string;
  confirmation: string | null;
  retired_at: number | null;
}
function fail(statusCode: number, code: string, message: string): never {
  throw Object.assign(new Error(message), { statusCode, code });
}
function unavailable(): never {
  return fail(410, 'input_unavailable', '原输入或首次上下文不可用。');
}
function diagnosticUnavailable(): never {
  return fail(410, 'diagnostic_unavailable', '原诊断正文或来源不可用。');
}

function assertRequestBody(
  body: Record<string, unknown> | null,
  expected: { operation: string; model: string | null },
): void {
  if (body && (body.model !== expected.model || body.operation !== expected.operation)) {
    unavailable();
  }
}

export const relationalInputFacts = new Set<RelationalEvent['eventType']>([
  'InputRegistered',
  'ContextRegistered',
  'RequestAccepted',
  'JobAccepted',
  'RequestAliasRegistered',
]);

// 只拥有新格式受保护内容和受理引用；调度、资格/报价及收费决定仍由 T3/T4 命令拥有。
export class RelationalInputStore {
  private sqlite: Database.Database;
  private env: NodeJS.ProcessEnv;
  constructor(sqlite: Database.Database, env: NodeJS.ProcessEnv = process.env) {
    assertRelationalStorageFormat(sqlite);
    this.sqlite = sqlite;
    this.env = env;
  }

  // 只供 generation 原事务暂存；后续事实/投影失败必须随调用者的保存点一起回滚。
  stageGenerationDiagnostic(jobId: string, detail: string, createdAt: number) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Diagnostic capture requires the owning generation transaction.');
    }
    if (
      typeof detail !== 'string' ||
      detail.length > 4000 ||
      !Number.isSafeInteger(createdAt) ||
      createdAt < 0
    ) {
      diagnosticUnavailable();
    }
    if (!detail) {
      return null;
    }
    // SQLite TEXT 与摘要必须使用同一有效 UTF-8 表示，保留公共错误 owner 的原截断语义。
    detail = Buffer.from(detail, 'utf8').toString('utf8');
    const sha256 = createHash('sha256').update(detail).digest('hex'),
      diagnosticId = `diagnostic:${domainDigest([jobId, sha256])}`,
      byteLength = Buffer.byteLength(detail);
    this.sqlite
      .prepare(
        'INSERT INTO generation_diagnostics(id,job_id,detail,sha256,created_at) VALUES(?,?,?,?,?)',
      )
      .run(diagnosticId, jobId, detail, sha256, createdAt);
    return { diagnosticId, jobId, sha256, byteLength, createdAt };
  }

  // 管理详情/离线恢复共用；先按小元数据限制正文，再核原事实及实际 UTF-8 字节。
  readGenerationDiagnostic(diagnosticId: string, jobId: string): string {
    return this.sqlite.transaction(() => {
      const row = this.sqlite
        .prepare<
          [string, string],
          {
            requestId: string;
            memberId: string;
            sha256: string;
            createdAt: number;
            byteLength: number;
          }
        >(
          `SELECT j.request_id AS requestId,r.member_id AS memberId,d.sha256,d.created_at AS createdAt,
        length(CAST(d.detail AS BLOB)) AS byteLength FROM generation_diagnostics d
        JOIN generation_jobs j ON j.id=d.job_id JOIN generation_requests r ON r.id=j.request_id
        WHERE d.id=? AND d.job_id=?`,
        )
        .get(diagnosticId, jobId);
      if (!row || row.byteLength < 1 || row.byteLength > 16000) {
        diagnosticUnavailable();
      }
      const matches = this.sqlite
        .prepare<[string, string], { eventId: string; seq: string }>(
          `SELECT event_id AS eventId,CAST(commit_seq AS TEXT) AS seq FROM domain_events
        WHERE stream_id=? AND event_type='DiagnosticRecorded'
          AND json_extract(payload,'$.diagnosticId')=? LIMIT 2`,
        )
        .all(`request:${row.requestId}`, diagnosticId);
      if (matches.length !== 1) {
        diagnosticUnavailable();
      }
      const event = readRelationalCommit(this.sqlite, matches[0].seq).events.find(
        (e) => e.eventId === matches[0].eventId,
      );
      if (event?.eventType !== 'DiagnosticRecorded') {
        diagnosticUnavailable();
      }
      const p = event.payload,
        body = this.sqlite
          .prepare<[string, string], { detail: string }>(
            'SELECT detail FROM generation_diagnostics WHERE id=? AND job_id=?',
          )
          .get(diagnosticId, jobId);
      if (
        event.memberId !== row.memberId ||
        p.jobId !== jobId ||
        p.sha256 !== row.sha256 ||
        p.createdAt !== row.createdAt ||
        p.createdAt !== event.occurredAt ||
        p.byteLength !== row.byteLength ||
        !body ||
        body.detail.length > 4000 ||
        createHash('sha256').update(body.detail).digest('hex') !== p.sha256
      ) {
        diagnosticUnavailable();
      }
      return body.detail;
    })();
  }

  createDiagnosticProjector(
    source: RelationalInputStore,
    stored: ReturnType<typeof readRelationalCommit>,
    replayed: boolean,
  ) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Diagnostic projection requires the owning import transaction.');
    }
    if (
      stored.command.actorKind !== 'system' ||
      stored.command.actorId !== null ||
      !stored.events.length ||
      canonicalJson(readRelationalCommit(source.sqlite, stored.events[0].commitSeq)) !==
        canonicalJson(stored)
    ) {
      diagnosticUnavailable();
    }
    const diagnostics = new Map(
      stored.events
        .filter((e) => e.eventType === 'DiagnosticRecorded')
        .map((event) => [
          event.eventId,
          {
            event,
            detail: source.readGenerationDiagnostic(
              event.payload.diagnosticId,
              event.payload.jobId,
            ),
          },
        ]),
    );
    return (event: RelationalEvent) => {
      if (!this.sqlite.inTransaction) {
        throw new Error('Diagnostic projection requires the owning import transaction.');
      }
      const original = diagnostics.get(event.eventId);
      if (
        !original ||
        event.eventType !== 'DiagnosticRecorded' ||
        canonicalJson(event) !== canonicalJson(original.event)
      ) {
        diagnosticUnavailable();
      }
      const p = event.payload;
      if (replayed || source.sqlite === this.sqlite) {
        if (this.readGenerationDiagnostic(p.diagnosticId, p.jobId) !== original.detail) {
          diagnosticUnavailable();
        }
      } else {
        this.sqlite
          .prepare(
            'INSERT INTO generation_diagnostics(id,job_id,detail,sha256,created_at) VALUES(?,?,?,?,?)',
          )
          .run(p.diagnosticId, p.jobId, original.detail, p.sha256, p.createdAt);
      }
    };
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
    const request = this.request(requestId, memberId);
    if (request.retired_at !== null) {
      unavailable();
    }
    return request;
  }

  private request(requestId: string, memberId: string): RequestRow {
    const request = this.sqlite
      .prepare<[string, string], RequestRow>(
        'SELECT id,member_id,input_id,context_id,operation,model,sample_count,seed_mode,confirmation,retired_at FROM generation_requests WHERE id=? AND member_id=?',
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
      expected.model !== request.model ||
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

  private requestInput(
    request: Pick<RequestRow, 'input_id' | 'member_id' | 'operation' | 'model'>,
  ) {
    const input = this.input(request.input_id, request.member_id);
    assertRequestBody(input.body, request);
    return input;
  }

  private assertAcceptedEvents(
    events: RelationalCommand['events'],
    body: Record<string, unknown> | null,
    context: RelationalNormalizationContext,
    assets: (RelationalInputAsset & { bytes: Buffer })[],
    command: Pick<
      RelationalCommand,
      'commandId' | 'fingerprint' | 'actorKind' | 'actorId' | 'receipt'
    >,
  ): void {
    const requests = events.filter((event) => event.eventType === 'RequestAccepted');
    if (requests.length !== 1 || !body) {
      unavailable();
    }
    const accepted = requests[0],
      p = accepted.payload;
    assertRequestBody(body, p);
    const registered = events.filter((event) => event.eventType === 'InputRegistered');
    if (
      registered.length !== 1 ||
      registered[0].payload.inputId !== p.inputId ||
      registered[0].payload.memberId !== p.memberId ||
      registered[0].accountId !== null ||
      events.some(
        (event) =>
          event.eventType === 'ContextRegistered' && event.payload.contextId !== p.contextId,
      )
    ) {
      unavailable();
    }
    const children = events
      .filter((event) => event.eventType === 'JobAccepted')
      .sort((a, b) => a.payload.sampleIndex - b.payload.sampleIndex);
    if (
      accepted.memberId !== p.memberId ||
      accepted.accountId !== null ||
      children.length !== p.sampleCount ||
      children.some(
        (child, index) =>
          child.payload.requestId !== p.requestId ||
          child.memberId !== p.memberId ||
          child.accountId !== null ||
          child.payload.sampleIndex !== index,
      ) ||
      (p.operation === 'generate') === (p.seedMode === 'not_applicable') ||
      (p.operation === 'generate' &&
        context.kind === 'member' &&
        context.member.randomSeed !== (p.seedMode === 'random'))
    ) {
      unavailable();
    }
    const seed = children[0].payload.actualSeed;
    if (p.operation === 'generate' && seed === null) {
      unavailable();
    }
    for (const child of children) {
      const expected =
        p.operation === 'generate'
          ? singleGenerationRequest(
              { seed: seed!, nativeParameters: {} },
              child.payload.sampleIndex,
            ).seed
          : null;
      if (child.payload.actualSeed !== expected) {
        unavailable();
      }
    }
    const aliases = events.filter((event) => event.eventType === 'RequestAliasRegistered');
    if (aliases.length > 1) {
      unavailable();
    }
    for (const alias of aliases) {
      const a = alias.payload;
      const request = relationalProviderRequest(
        restoreRelationalWire(body, assets, seed, p.sampleCount),
        context,
      );
      const requestFingerprint = generationRequestFingerprint(
        { ...request, operation: p.operation, sampleCount: p.sampleCount },
        { randomSeed: p.seedMode === 'random', noCache: a.noCache },
      );
      let intentFingerprint = requestFingerprint;
      if (context.kind === 'member') {
        if (!a.noCache || p.confirmation === null) {
          unavailable();
        }
        try {
          assertMemberGenerationIntent(p.confirmation, requestFingerprint);
        } catch {
          unavailable();
        }
        intentFingerprint = memberGenerationIntentFingerprint(
          { requestFingerprint },
          p.confirmation,
        );
      }
      if (
        a.memberId !== p.memberId ||
        alias.memberId !== p.memberId ||
        alias.accountId !== null ||
        a.operation !== p.operation ||
        a.requestId !== p.requestId ||
        a.jobId !== children[0].payload.jobId ||
        a.contextId !== p.contextId ||
        a.originalCommandId !== command.commandId ||
        a.intentFingerprint !== command.fingerprint ||
        a.intentFingerprint !== intentFingerprint ||
        command.actorKind !== 'member' ||
        command.actorId !== p.memberId ||
        canonicalJson(command.receipt) !==
          canonicalJson({
            requestId: p.requestId,
            jobIds: children.map((child) => child.payload.jobId),
          })
      ) {
        unavailable();
      }
    }
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
            model: prepared.body.model as string,
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
      if (accepted.idempotency !== undefined) {
        events.push({
          ...common,
          streamId: `request:${accepted.requestId}`,
          eventType: 'RequestAliasRegistered',
          payload: {
            memberId: accepted.memberId,
            operation: prepared.body.operation as RequestRow['operation'],
            keyHash: accepted.idempotency.keyHash,
            noCache: accepted.idempotency.noCache,
            intentFingerprint: metadata.fingerprint,
            requestId: accepted.requestId,
            jobId: accepted.jobs[0].jobId,
            contextId: prepared.contextId,
            originalCommandId: metadata.commandId,
          },
        });
      }
      const receipt = {
        requestId: accepted.requestId,
        jobIds: accepted.jobs.map((job) => job.jobId),
      };
      this.assertAcceptedEvents(events, prepared.body, prepared.context, prepared.assets, {
        ...metadata,
        receipt,
      });
      const expectedVersions = Object.fromEntries(events.map((event) => [event.streamId, '0']));
      const { policyVersion: _policy, pricingVersion: _pricing, ...command } = metadata;
      const result = appendRelationalCommit(this.sqlite, {
        ...command,
        events,
        expectedVersions,
        receipt,
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
            `INSERT INTO generation_requests(id,member_id,input_id,context_id,source,operation,model,sample_count,seed_mode,confirmation,accepted_at,source_event_id)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
          )
          .run(
            p.requestId,
            p.memberId,
            p.inputId,
            p.contextId,
            p.source,
            p.operation,
            p.model,
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
      } else if (event.eventType === 'RequestAliasRegistered') {
        const p = event.payload;
        this.sqlite
          .prepare(
            `INSERT INTO idempotency_keys(
          original_member_id,operation,key_hash,intent_fingerprint,original_request_id,
          original_job_id,original_command_id,request_id,commit_seq,context_id,resolution,
          origin_ref,archived_receipt,created_at,source_event_id)
          VALUES(?,?,?,?,?,?,?,?,?,?,'live',?,NULL,?,?)`,
          )
          .run(
            p.memberId,
            p.operation,
            p.keyHash,
            p.intentFingerprint,
            p.requestId,
            p.jobId,
            p.originalCommandId,
            p.requestId,
            BigInt(event.commitSeq),
            p.contextId,
            event.eventId,
            event.occurredAt,
            event.eventId,
          );
      }
    }
  }

  createInputProjector(
    source: RelationalInputStore,
    stored: ReturnType<typeof readRelationalCommit>,
    replayed: boolean,
  ) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Input projection requires the owning import transaction.');
    }
    const commitSeq = stored.events[0]?.commitSeq;
    if (
      !commitSeq ||
      canonicalJson(readRelationalCommit(source.sqlite, commitSeq)) !== canonicalJson(stored)
    ) {
      unavailable();
    }
    const contexts = new Map<string, RelationalNormalizationContext>();
    const inputs = new Map<string, ReturnType<RelationalInputStore['input']>>();
    for (const event of stored.events) {
      if (event.eventType === 'ContextRegistered') {
        contexts.set(event.payload.contextId, source.context(event.payload.contextId));
      }
      if (event.eventType === 'RequestAccepted') {
        const context =
            contexts.get(event.payload.contextId) ?? source.context(event.payload.contextId),
          input =
            inputs.get(event.payload.inputId) ??
            source.input(event.payload.inputId, event.payload.memberId);
        if (input.row.member_id !== event.payload.memberId) {
          unavailable();
        }
        contexts.set(event.payload.contextId, context);
        this.assertAcceptedEvents(
          stored.events,
          input.body,
          context,
          input.assets.map((asset) => ({
            ...asset,
            bytes: source.asset(event.payload.inputId, asset).bytes,
          })),
          stored.command,
        );
      }
      if (event.eventType === 'InputRegistered') {
        inputs.set(
          event.payload.inputId,
          source.input(event.payload.inputId, event.payload.memberId),
        );
      }
    }
    if (!stored.events.some((event) => event.eventType === 'RequestAccepted')) {
      unavailable();
    }
    const content = [...inputs].map(([inputId, input]) => ({
      inputId,
      ...input,
      assets: input.assets.map((asset) => ({
        ...asset,
        bytes: source.asset(inputId, asset).bytes,
      })),
    }));

    const copyContext = (contextId: string) => {
      const context = contexts.get(contextId);
      if (!context) {
        unavailable();
      }
      const existing = this.sqlite
        .prepare('SELECT 1 FROM normalization_contexts WHERE id=?')
        .get(contextId);
      if (existing) {
        if (canonicalJson(this.context(contextId)) !== canonicalJson(context)) {
          unavailable();
        }
      } else {
        if (replayed) {
          unavailable();
        }
        this.sqlite
          .prepare(
            'INSERT INTO normalization_contexts(id,schema_version,sha256,body) VALUES(?,1,?,?)',
          )
          .run(contextId, domainDigest(context), canonicalJson(context));
      }
    };
    const byInput = new Map(content.map((input) => [input.inputId, input]));
    const copyInput = (inputId: string) => {
      const input = byInput.get(inputId);
      if (!input) {
        unavailable();
      }
      if (replayed) {
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
        return;
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
    };
    return (event: RelationalEvent): void => {
      if (!this.sqlite.inTransaction) {
        throw new Error('Input projection requires the owning import transaction.');
      }
      if (event.eventType === 'ContextRegistered') {
        copyContext(event.payload.contextId);
      } else if (event.eventType === 'InputRegistered') {
        copyInput(event.payload.inputId);
      } else if (event.eventType === 'RequestAccepted') {
        copyContext(event.payload.contextId);
        if (!replayed) {
          this.applyAcceptedEvents([event]);
        }
      } else if (event.eventType === 'JobAccepted') {
        if (!replayed) {
          this.applyAcceptedEvents([event]);
        }
      } else if (event.eventType === 'RequestAliasRegistered') {
        if (replayed) {
          const p = event.payload;
          const mapping = this.readGenerationIdempotency(p.memberId, p.operation, p.keyHash);
          if (
            !mapping ||
            mapping.intentFingerprint !== p.intentFingerprint ||
            mapping.originalCommandId !== p.originalCommandId ||
            mapping.requestId !== p.requestId ||
            mapping.contextId !== p.contextId ||
            mapping.originalJobId !== p.jobId ||
            mapping.originalRequestId !== p.requestId ||
            mapping.sourceEventId !== event.eventId ||
            mapping.commitSeq !== event.commitSeq ||
            mapping.resolution !== 'live'
          ) {
            unavailable();
          }
        } else {
          this.applyAcceptedEvents([event]);
        }
      } else {
        throw new Error("Input projection cannot apply another owner's facts.");
      }
    };
  }

  // T2 的窄回放继续使用原完整提交和相同输入规则，不拆分混合提交。
  replayAcceptedInput(source: RelationalInputStore, commitSeq: string) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Input replay requires the owning import transaction.');
    }
    return this.sqlite.transaction(() => {
      const stored = readRelationalCommit(source.sqlite, commitSeq);
      if (stored.events.some((event) => !relationalInputFacts.has(event.eventType))) {
        throw new Error("Input replay cannot apply another owner's facts.");
      }
      const result = replayRelationalCommit(this.sqlite, stored);
      const apply = this.createInputProjector(source, stored, result.replayed);
      for (const event of stored.events) {
        apply(event);
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
    this.requestInput(request);
    if (!request.context_id) {
      unavailable();
    }
    return this.context(request.context_id);
  }

  readGenerationIdempotency(memberId: string, operation: string, keyHash: string) {
    return (
      this.sqlite
        .prepare<
          [string, string, string],
          {
            intentFingerprint: string;
            keyHash: string;
            sourceEventId: string;
            originalCommandId: string;
            originalRequestId: string;
            originalJobId: string;
            requestId: string | null;
            contextId: string | null;
            commitSeq: string | null;
            resolution: 'live' | 'archived' | 'unavailable';
          }
        >(
          `SELECT intent_fingerprint AS intentFingerprint,key_hash AS keyHash,source_event_id AS sourceEventId,
      original_command_id AS originalCommandId,
      original_request_id AS originalRequestId,original_job_id AS originalJobId,request_id AS requestId,
      context_id AS contextId,CAST(commit_seq AS TEXT) AS commitSeq,resolution FROM idempotency_keys
      WHERE original_member_id=? AND operation=? AND key_hash=?`,
        )
        .get(memberId, operation, keyHash) || null
    );
  }

  assertAcceptedGenerationIntent(
    requestId: string,
    memberId: string,
    commitSeq: string,
    keyHash: string,
    sourceEventId: string,
  ): void {
    const request = this.ownedRequest(requestId, memberId);
    if (!request.context_id) {
      unavailable();
    }
    const input = this.requestInput(request),
      stored = readRelationalCommit(this.sqlite, commitSeq);
    const accepted = stored.events.find((event) => event.eventType === 'RequestAccepted');
    const alias = stored.events.find((event) => event.eventType === 'RequestAliasRegistered');
    if (
      accepted?.payload.requestId !== requestId ||
      accepted.payload.memberId !== memberId ||
      alias?.payload.keyHash !== keyHash ||
      alias.eventId !== sourceEventId
    ) {
      unavailable();
    }
    this.assertAcceptedEvents(
      stored.events,
      input.body,
      this.context(request.context_id),
      input.assets.map((asset) => ({ ...asset, bytes: this.asset(request.input_id, asset).bytes })),
      stored.command,
    );
  }

  // 管理入口先鉴权、限量；这里共用原输入关系/摘要校验，不恢复附件或按当前默认生成参数。
  readErrorLogRequest(jobId: string): ErrorLogRequest {
    const job = this.sqlite
      .prepare<[string], { requestId: string; memberId: string }>(
        `SELECT j.request_id AS requestId,r.member_id AS memberId FROM generation_jobs j
        JOIN generation_requests r ON r.id=j.request_id WHERE j.id=?`,
      )
      .get(jobId);
    if (!job) {
      unavailable();
    }
    const request = this.request(job.requestId, job.memberId),
      { body, row } = this.requestInput(request),
      display = relationalInputDisplay(body, row.completeness === 'complete'),
      parameters = (body?.parameters ?? {}) as Record<string, unknown>,
      text = (value: unknown) => (typeof value === 'string' ? value : null),
      numeric = (value: unknown) =>
        typeof value === 'number' && Number.isFinite(value) ? value : null;
    return {
      tag: display.tag,
      prompt: text(body?.input),
      model: request.model,
      width: numeric(parameters.width),
      height: numeric(parameters.height),
      requestedSteps: display.requestedSteps,
      steps: numeric(parameters.steps),
      negative: text(parameters.negative_prompt),
      scale: numeric(parameters.scale),
      cfg: numeric(parameters.cfg_rescale),
      sampler: text(parameters.sampler),
      noiseSchedule: text(parameters.noise_schedule),
      seed: this.jobSeed(job.requestId, jobId),
    };
  }

  readMemberInput(requestId: string, memberId: string) {
    const request = this.ownedRequest(requestId, memberId),
      input = this.requestInput(request);
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
      input = this.requestInput(request);
    if (!input.body) {
      unavailable();
    }
    let context: RelationalNormalizationContext | null = null;
    if (input.row.completeness === 'complete') {
      if (!request.context_id) {
        unavailable();
      }
      context = this.context(request.context_id);
    }
    const slot = relationalAssetSlots(input.body, input.assets).find(
      (entry) => entry.slot === slotName,
    );
    if (!slot) {
      fail(404, 'input_not_found', '原输入附件不存在。');
    }
    const assetId = (slot.value as { assetId: string }).assetId;
    const descriptor = input.assets.find((asset) => asset.assetId === assetId)!;
    const asset = this.asset(request.input_id, descriptor);
    if (context) {
      validateRelationalAssetBytes({ ...descriptor, bytes: asset.bytes }, slot.image, context);
    } else {
      // 历史已知字节可复制；缺首次定义不能补成 Native 派发资格。
      try {
        if (slot.image || asset.mimeType.startsWith('image/')) {
          if (validateImageBytes(asset.bytes).mimeType !== asset.mimeType) {
            unavailable();
          }
        } else if (asset.mimeType !== 'application/octet-stream') {
          unavailable();
        }
      } catch {
        unavailable();
      }
    }
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
    const accepted = this.request(job.request_id, job.member_id);
    const seed = this.jobSeed(job.request_id, jobId);
    const input = this.requestInput(accepted);
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
            ...relationalInputDisplay(input.body, true),
          }
        : {}),
    };
  }
}
