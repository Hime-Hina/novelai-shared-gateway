import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';
import { inventoryLegacyDatabase } from './database-model-inventory.mjs';
import { domainDigest, canonicalJson } from '../server/events/domain-events.ts';
import { relationalFactStream } from '../server/events/relational-fact-contracts.ts';
import { RelationalInputStore } from '../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../server/storage/relational-storage.ts';
import { readRetainedArchiveBinding } from '../server/storage/retained-sources.ts';
import { readProjectedFairQuota } from '../server/events/event-store.ts';
import { freeImagePolicy } from '../server/quota/free-image-policy.ts';
import { lossAlertPolicy } from '../server/quota/failure-accounting.ts';
import { parseAmount, formatAmount } from '../server/quota/resource-amounts.ts';
import { inputAssetSlots, decodeInputAsset } from '../server/generation/member-contract.ts';
import { generationRequestFingerprint } from '../server/generation/request-fingerprint.ts';
import {
  parseRelationalNormalizationContext,
  prepareRelationalInput,
} from '../server/generation/relational-input.ts';

const hash = (bytes) => createHash('sha256').update(bytes).digest('hex');
const reject = () => {
  throw new Error('Legacy migration requires the selected unchanged offline source and proof.');
};
const time = (value) => {
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) {
    return value;
  }
  if (typeof value !== 'string' || !value) {
    return null;
  }
  const parsed = Date.parse(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
};

// 本次新基线的事实 ID 可预分配；原实体 ID 与原审计来源不因此改变。
export function migrationReferences(sourceSha256) {
  if (!/^[a-f0-9]{64}$/.test(sourceSha256)) {
    reject();
  }
  const sourceId = `legacy:${sourceSha256}`;
  return {
    sourceSha256,
    sourceId,
    origin: (originalId) => ({ sourceId, originalId }),
    eventId: (eventType, originalId) =>
      `migration:${domainDigest({ sourceSha256, eventType, originalId })}`,
    memberIds: new Set(),
    accountIds: new Set(),
    jobIds: new Set(),
    attemptIds: new Set(),
    requestsByJob: new Map(),
    credentialEpochByAccount: new Map(),
    inputsByJob: new Map(),
    inputsByArtifact: new Map(),
    requestProofs: new Map(),
  };
}

// 原键逐份证明，不按盘点数量给一批键标 live；缺定义不能调用当前 defaults 补齐。
export function proveLegacyGenerationIntent(request, originalContext, mapping) {
  if (!originalContext?.native) {
    return { reason: 'FIRST_DEFINITION_UNPROVEN' };
  }
  let context;
  try {
    const keys = Object.keys(originalContext);
    if (keys.length === 1 && keys[0] === 'native') {
      context = parseRelationalNormalizationContext({
        kind: 'native',
        native: originalContext.native,
      });
    } else {
      return { reason: 'FIRST_DEFINITION_KIND_UNPROVEN' };
    }
    if (
      !request ||
      (request.operation !== undefined && request.operation !== mapping.operation) ||
      (mapping.operation === 'generate' &&
        (!Number.isSafeInteger(request.sampleCount) || request.sampleCount < 1))
    ) {
      return { reason: 'FIRST_INTENT_FIELDS_UNPROVEN' };
    }
    const original = {
        ...request,
        operation: mapping.operation,
        normalizationContext: { native: context.native },
      },
      matches = new Map();
    for (const noCache of [false, true]) {
      for (const randomSeed of [false, true]) {
        if (
          generationRequestFingerprint(original, { noCache, randomSeed }) !== mapping.fingerprint
        ) {
          continue;
        }
        let seedMode = 'not_applicable';
        if (mapping.operation === 'generate') {
          seedMode = randomSeed ? 'random' : 'fixed';
        }
        matches.set(`${seedMode}:${noCache}`, { seedMode, noCache });
      }
    }
    if (matches.size !== 1) {
      return { reason: 'FIRST_INTENT_FINGERPRINT_UNPROVEN' };
    }
    const prepared = prepareRelationalInput(original, context),
      [intent] = matches.values();
    return { prepared, context, ...intent };
  } catch {
    return { reason: 'FIRST_DEFINITION_UNSUPPORTED' };
  }
}

function append(target, references, at, key, drafts) {
  const events = drafts.map((draft) => {
      const kind = relationalFactStream(draft.eventType);
      let owner;
      switch (kind) {
        case 'member':
          owner = draft.memberId;
          break;
        case 'account':
          owner = draft.accountId;
          break;
        case 'sharing':
          owner = 'default';
          break;
        case 'import':
          owner = references.sourceId;
          break;
        case 'context':
          owner = draft.payload.contextId;
          break;
        default:
          owner = draft.payload.requestId ?? draft.payload.inputId;
      }
      return {
        ...draft,
        streamId: draft.streamId ?? `${kind}:${owner}`,
        occurredAt: at,
        policyVersion: 'legacy-B-opening-v1',
        pricingVersion: 'saved-original-decisions',
        accountId: draft.accountId ?? null,
        memberId: draft.memberId ?? null,
      };
    }),
    commandId = `migration:${references.sourceId}:${key}`;
  return new RelationalCommitReplayer(target).appendCommit(
    {
      commandId,
      fingerprint: domainDigest(events),
      actorKind: 'system',
      actorId: null,
      recordedAt: at,
      correlationId: null,
      causationId: null,
      expectedVersions: Object.fromEntries(
        events.map((event) => [
          event.streamId,
          target
            .prepare(
              'SELECT CAST(stream_version AS TEXT) AS version FROM stream_heads WHERE stream_id=?',
            )
            .get(event.streamId)?.version ?? '0',
        ]),
      ),
      receipt: { imported: key, sourceId: references.sourceId },
      events,
    },
    new RelationalInputStore(target),
  );
}

function memberOwner(job, input) {
  const candidates = new Set(
    [
      job.memberId,
      job.userId,
      job.generationAttempt?.memberId,
      job.fairCharge?.userId,
      input?.member_id,
    ].filter((value) => typeof value === 'string' && value),
  );
  // 当前 token 不是历史身份依据；即使恰好只匹配一个当前成员也不能补归属。
  return candidates.size === 1 ? [...candidates][0] : null;
}

export function mapLegacyAccounts(source, references) {
  const facts = [],
    findings = [];
  for (const row of source
    .prepare('SELECT id,data FROM accounts ORDER BY order_value DESC')
    .iterate()) {
    const account = JSON.parse(row.data);
    if (account.id !== row.id) {
      reject();
    }
    if (account.capacityConcurrencyLimit != null && account.capacityLimitedUntil == null) {
      findings.push({ code: 'ACCOUNT_CAPACITY_SOURCE_REQUIRES_MAPPING', originalId: row.id });
    }
    references.accountIds.add(row.id);
    // 首代仅标识目标基线选定的当前 PST，不是旧 dispatch/observation 的历史代次。
    references.credentialEpochByAccount.set(row.id, 1);
    facts.push({
      eventId: references.eventId('OpeningAccountRecorded', row.id),
      eventType: 'OpeningAccountRecorded',
      accountId: row.id,
      payload: {
        accountId: row.id,
        displayName: typeof account.name === 'string' ? account.name : '',
        weight: typeof account.weight === 'number' ? account.weight : null,
        enabled: account.enabled === true,
        credentialEpoch: 1,
        cooldownUntil: time(account.cooldownUntil),
        cooldownReason: typeof account.cooldownReason === 'string' ? account.cooldownReason : null,
        ordinaryStrikes: Number(account.ordinaryStrikeCount || 0),
        capacityStrikes: Number(account.capacityStrikeCount || 0),
        capacityLimitedUntil: time(account.capacityLimitedUntil),
        capacityLastErrorAt: time(account.capacityLastErrorAt),
        origin: references.origin(row.id),
      },
    });
  }
  return { facts, findings };
}

export function mapLegacySharing(source, references) {
  const state = readProjectedFairQuota(source);
  if (!state) {
    return { facts: [], findings: [] };
  }
  const facts = [],
    findings = [],
    memberships = new Map(),
    free = freeImagePolicy({ settings: { fairQuota: state } }),
    loss = lossAlertPolicy(state.lossAlertPolicy);
  for (const group of state.groups) {
    facts.push({
      eventId: references.eventId('OpeningGroupRecorded', group.id),
      eventType: 'OpeningGroupRecorded',
      payload: {
        groupId: group.id,
        name: group.name ?? '',
        share: group.share,
        origin: references.origin(group.id),
      },
    });
    for (const memberId of group.members) {
      if (
        memberships.has(memberId) ||
        !references.memberIds.has(memberId) ||
        !state.members.includes(memberId)
      ) {
        reject();
      }
      memberships.set(memberId, group.id);
      facts.push({
        eventId: references.eventId('OpeningGroupMemberRecorded', memberId),
        eventType: 'OpeningGroupMemberRecorded',
        memberId,
        payload: {
          memberId,
          groupId: group.id,
          weight: group.memberWeights[memberId],
          memberOrdinal: state.members.indexOf(memberId),
          origin: references.origin(memberId),
        },
      });
    }
  }
  if (
    memberships.size !== state.members.length ||
    new Set(state.members).size !== state.members.length
  ) {
    reject();
  }
  facts.push({
    eventId: references.eventId('OpeningPolicyRecorded', 'sharing'),
    eventType: 'OpeningPolicyRecorded',
    payload: {
      enabled: state.enabled,
      boundAccountId: state.boundAccountId,
      configVersion: state.configVersion,
      autoAnlasFallback: state.autoAnlasFallback === true,
      freePolicyVersion: free.version,
      freeEnabled: free.enabled,
      freeWarningThreshold: free.warningThreshold,
      freeHardLimit: free.hardLimit,
      lossPolicyVersion: loss.version,
      lossV5_24h: loss.v5Points24h,
      lossFixed24h: formatAmount(parseAmount(loss.fixedAnlas24h)),
      lossFixed30d: formatAmount(parseAmount(loss.fixedAnlas30d)),
      lossMemberAttempts24h: loss.memberAttempts24h,
      origin: references.origin('sharing:default'),
    },
  });
  return { facts, findings };
}

export function mapLegacyMembers(source, references) {
  const facts = [],
    findings = [];
  for (const row of source
    .prepare('SELECT id,data FROM users ORDER BY order_value DESC')
    .iterate()) {
    const member = JSON.parse(row.data),
      createdAt = time(member.createdAt);
    if (
      member.id !== row.id ||
      createdAt === null ||
      typeof member.enabled !== 'boolean' ||
      (member.acceptingJobs !== undefined && typeof member.acceptingJobs !== 'boolean') ||
      (member.jobAdmissionVersion !== undefined &&
        (!Number.isSafeInteger(member.jobAdmissionVersion) || member.jobAdmissionVersion < 0))
    ) {
      findings.push({ code: 'MEMBER_OPENING_SOURCE_INCOMPLETE', originalId: row.id });
      continue;
    }
    references.memberIds.add(row.id);
    facts.push({
      eventId: references.eventId('OpeningMemberRecorded', row.id),
      eventType: 'OpeningMemberRecorded',
      memberId: row.id,
      payload: {
        memberId: row.id,
        displayName: typeof member.name === 'string' ? member.name : '',
        status: member.enabled ? 'active' : 'disabled',
        // 旧 owner 的缺省接单/首代规则，不生成认证代次。
        acceptingJobs: member.acceptingJobs !== false,
        admissionEpoch: member.jobAdmissionVersion ?? 0,
        createdAt,
        origin: references.origin(row.id),
      },
    });
  }
  return { facts, findings };
}

function partialInput(request) {
  if (!request || typeof request !== 'object' || Array.isArray(request)) {
    return null;
  }
  const body = {},
    parameters = {};
  for (const [oldName, newName] of [
    ['prompt', 'input'],
    ['model', 'model'],
    ['action', 'action'],
    ['operation', 'operation'],
    ['requestedSteps', 'requestedSteps'],
    ['allowPurchasedAnlas', 'allowPurchasedAnlas'],
  ]) {
    if (Object.hasOwn(request, oldName)) {
      body[newName] = request[oldName];
    }
  }
  if (request.nativeParameters !== undefined) {
    Object.assign(parameters, structuredClone(request.nativeParameters));
    delete parameters.seed;
    delete parameters.n_samples;
  } else {
    for (const [oldName, newName] of [
      ['width', 'width'],
      ['height', 'height'],
      ['steps', 'steps'],
      ['scale', 'scale'],
      ['cfg', 'cfg_rescale'],
      ['negative', 'negative_prompt'],
      ['sampler', 'sampler'],
      ['noiseSchedule', 'noise_schedule'],
    ]) {
      if (Object.hasOwn(request, oldName)) {
        parameters[newName] = request[oldName];
      }
    }
  }
  if (Object.keys(parameters).length) {
    body.parameters = parameters;
  }
  return Object.keys(body).length ? body : null;
}

function sourceRequestGroups(source, references) {
  const groups = new Map();
  for (const row of source
    .prepare('SELECT id,request_id FROM jobs ORDER BY order_value DESC')
    .iterate()) {
    const requestId = row.request_id || `legacy-job-anchor:${row.id}`;
    let group = groups.get(requestId);
    if (!group) {
      group = { requestId, originalRequestId: row.request_id, jobIds: [] };
      groups.set(requestId, group);
    } else if (group.originalRequestId !== row.request_id) {
      reject();
    }
    group.jobIds.push(row.id);
    references.requestsByJob.set(row.id, requestId);
  }
  return groups;
}

function openingInput(original, proof) {
  if (proof?.prepared) {
    const { body, assets, logicalBytes } = proof.prepared;
    return { body, assets, logicalBytes, completeness: 'complete' };
  }
  const body = partialInput(original),
    assets = [];
  if (body === null) {
    return { body: null, assets, logicalBytes: 0, completeness: 'unavailable' };
  }
  const logicalBytes = Buffer.byteLength(canonicalJson(body));
  for (const slot of inputAssetSlots(body)) {
    const decoded = decodeInputAsset(slot),
      sha256 = hash(decoded.bytes);
    let asset = assets.find(
      (entry) => entry.sha256 === sha256 && entry.bytes.equals(decoded.bytes),
    );
    if (!asset) {
      asset = {
        assetId: `a${assets.length + 1}`,
        sha256,
        mimeType: decoded.mimeType,
        byteLength: decoded.bytes.length,
        bytes: decoded.bytes,
      };
      assets.push(asset);
    }
    slot.owner[slot.key] = { assetId: asset.assetId };
  }
  return { body, assets, logicalBytes, completeness: 'partial' };
}

function sourceContextProof(original, mappings) {
  const proofs = mappings.map((mapping) => ({
      mapping,
      proof: proveLegacyGenerationIntent(
        original,
        JSON.parse(mapping.normalization_context),
        mapping,
      ),
    })),
    proven = proofs.filter(({ proof }) => proof.prepared),
    definitions = new Set(proven.map(({ proof }) => domainDigest(proof.context)));
  if (definitions.size === 1 && proven.length) {
    return { proof: proven[0].proof, proofs };
  }
  return { proof: null, proofs };
}

// 每个真实 parent 的正文与 child 共用原来源；没有 parent 的 FK 容器不授予公开原请求身份。
function appendLegacyRequests(source, target, references, at, findings) {
  const inputs = new RelationalInputStore(target),
    groups = sourceRequestGroups(source, references);
  for (const group of groups.values()) {
    const jobs = group.jobIds.map((id) => {
        const row = source
          .prepare('SELECT id,data,request_id,order_value FROM jobs WHERE id=?')
          .get(id);
        return { row, value: JSON.parse(row.data) };
      }),
      first = jobs[0],
      originalRow = group.originalRequestId
        ? source
            .prepare('SELECT * FROM generation_requests WHERE id=?')
            .get(group.originalRequestId)
        : null,
      legacyRef = first.value.legacyInputRef || `legacy-input:${first.row.id}`,
      protectedRow =
        originalRow ??
        source.prepare('SELECT * FROM generation_requests WHERE id=?').get(legacyRef),
      saved = protectedRow ? JSON.parse(protectedRow.payload) : null,
      original = saved?.legacyInline === true ? saved.request : (saved ?? first.value.request),
      members = new Set(jobs.map(({ value }) => memberOwner(value, protectedRow))),
      memberId = members.size === 1 ? [...members][0] : null;
    if (
      !memberId ||
      !references.memberIds.has(memberId) ||
      jobs.some(({ row, value }) => row.id !== value.id)
    ) {
      findings.push({ code: 'REQUEST_MEMBER_SOURCE_UNPROVEN', originalId: group.requestId });
      continue;
    }
    const mappings = group.originalRequestId
        ? source
            .prepare(
              'SELECT * FROM generation_idempotency WHERE request_id=? ORDER BY created_at,key_hash',
            )
            .all(group.originalRequestId)
        : source
            .prepare(
              'SELECT * FROM generation_idempotency WHERE job_id=? ORDER BY created_at,key_hash',
            )
            .all(first.row.id),
      { proof, proofs } = sourceContextProof(original, mappings),
      content = openingInput(original, proof),
      operation =
        originalRow?.operation ??
        proof?.prepared.body.operation ??
        (['generate', 'upscale', 'encode-vibe'].includes(original?.operation)
          ? original.operation
          : null),
      model = typeof original?.model === 'string' ? original.model : null,
      count =
        Number.isSafeInteger(original?.sampleCount) && original.sampleCount > 0
          ? original.sampleCount
          : null,
      hasActive = jobs.some(({ value }) => ['queued', 'pending', 'running'].includes(value.status));
    if (
      jobs.some(
        ({ value }) =>
          !['queued', 'pending', 'running', 'failed', 'done'].includes(value.status) ||
          value.imageId ||
          value.generationAttempt ||
          value.resultPending,
      ) ||
      (hasActive && (!proof?.prepared || count === null))
    ) {
      findings.push({ code: 'JOB_REQUIRES_COMPLETE_OPENING_MAPPING', originalId: group.jobIds[0] });
      continue;
    }
    const inputId = group.originalRequestId || `legacy-job-input:${first.row.id}`;
    target.transaction(() => {
      const input = inputs.stageOpeningInput({ inputId, memberId, ...content }),
        events = [];
      let contextId = null;
      if (proof?.prepared) {
        contextId = proof.prepared.contextId;
        if (!target.prepare('SELECT 1 FROM normalization_contexts WHERE id=?').get(contextId)) {
          const context = inputs.stageOpeningContext(contextId, proof.context);
          events.push({
            eventId: references.eventId('ContextRegistered', contextId),
            eventType: 'ContextRegistered',
            payload: context,
          });
        }
      }
      events.push(
        {
          eventId: references.eventId('OpeningInputRecorded', inputId),
          eventType: 'OpeningInputRecorded',
          memberId,
          payload: { ...input, origin: references.origin(protectedRow?.id || first.row.id) },
        },
        {
          eventId: references.eventId('OpeningRequestRecorded', group.requestId),
          eventType: 'OpeningRequestRecorded',
          memberId,
          payload: {
            requestId: group.requestId,
            originalRequestId: group.originalRequestId,
            memberId,
            inputId,
            contextId,
            source:
              originalRow?.source ??
              (typeof first.value.source === 'string' ? first.value.source : null),
            operation,
            model,
            sampleCount: count,
            seedMode: proof?.seedMode ?? 'unknown',
            confirmation: original?.confirmation ?? null,
            acceptedAt: originalRow ? time(originalRow.created_at) : null,
            retiredAt: null,
            origin: references.origin(group.originalRequestId || first.row.id),
          },
        },
      );
      for (const { row, value: job } of jobs) {
        const seed =
          typeof job.request?.seed === 'number'
            ? job.request.seed
            : group.originalRequestId
              ? null
              : original?.seed;
        events.push({
          eventId: references.eventId('OpeningJobRecorded', row.id),
          eventType: 'OpeningJobRecorded',
          memberId,
          payload: {
            jobId: row.id,
            requestId: group.requestId,
            createdAt: time(job.createdAt),
            sampleIndex:
              Number.isSafeInteger(job.sampleIndex) && job.sampleIndex >= 0
                ? job.sampleIndex
                : null,
            actualSeed: typeof seed === 'number' ? seed : null,
            status: job.status,
            queueSeq:
              Number.isSafeInteger(job.queueSeq) && job.queueSeq > 0 ? String(job.queueSeq) : null,
            queueExpiresAt: time(job.queueExpiresAt),
            deadlineAt: time(job.deadlineAt),
            admissionEpoch:
              Number.isSafeInteger(job.jobAdmissionVersion) && job.jobAdmissionVersion >= 0
                ? job.jobAdmissionVersion
                : null,
            artifactId: null,
            cacheKey: typeof job.cacheKey === 'string' && job.cacheKey ? job.cacheKey : null,
            completedAt: time(job.completedAt),
            errorCode: typeof job.errorCode === 'string' && job.errorCode ? job.errorCode : null,
            errorStatus:
              Number.isSafeInteger(job.errorStatus) && job.errorStatus > 0 ? job.errorStatus : null,
            cancellationReason:
              typeof job.cancellationReason === 'string' && job.cancellationReason
                ? job.cancellationReason
                : null,
            lateResultAt: null,
            freeSucceededAt: null,
            freeWarningThreshold: null,
            retiredAt: null,
            origin: references.origin(row.id),
          },
        });
      }
      append(target, references, at, `request:${group.requestId}`, events);
      for (const { row } of jobs) {
        references.jobIds.add(row.id);
        references.inputsByJob.set(row.id, { inputId, memberId, operation });
      }
      references.requestProofs.set(group.requestId, {
        inputId,
        memberId,
        contextId,
        proofs,
        originalRequestId: group.originalRequestId,
      });
    })();
  }
}

export async function migrateLegacyDatabase(options) {
  if (
    !options ||
    !/^[a-f0-9]{64}$/.test(options.sourceSha256 ?? '') ||
    !/^[a-f0-9]{64}$/.test(options.auditSha256 ?? '') ||
    !/^[1-9][0-9]*$/.test(options.cut ?? '')
  ) {
    reject();
  }
  const inventory = inventoryLegacyDatabase(options.source, options.sourceCommit);
  if (
    inventory.source.databaseSha256 !== options.sourceSha256 ||
    inventory.cut.lastCommitSeq !== options.cut ||
    !inventory.integrity ||
    inventory.foreignKeyViolations ||
    !inventory.cut.openingComplete ||
    Object.entries(inventory.events).some(
      ([key, value]) => key !== 'byType' && typeof value === 'number' && value !== 0,
    ) ||
    Object.values(inventory.inputs).some((value) => value !== 0)
  ) {
    reject();
  }
  const binding = await readRetainedArchiveBinding(
    options.source,
    options.audit,
    options.artifacts,
    options.auditSha256,
  );
  if (binding.format !== 'legacy-export' || binding.finalCommitSeq !== options.cut) {
    reject();
  }
  const audit = JSON.parse(await readFile(options.audit, 'utf8'));
  if (
    audit.sourceCommit !== options.sourceCommit ||
    audit.openingSha256 !== inventory.cut.openingSha256 ||
    audit.protectedInputs !== inventory.tables.generation_requests.rows ||
    audit.protectedInputBytes !== inventory.tables.generation_requests.logicalContentBytes ||
    audit.contexts !== inventory.tables.generation_contexts.rows ||
    audit.artifactInputs !== inventory.tables.artifact_inputs.rows ||
    audit.diagnostics !== inventory.tables.generation_diagnostics.rows ||
    audit.artifacts !== inventory.tables.images.rows ||
    audit.commits !== inventory.tables.event_commits.rows
  ) {
    reject();
  }
  const inputBytes = await readFile(path.join(path.dirname(options.artifacts), 'inputs.jsonl')),
    tuple = audit.exports.inputs,
    inputLines = new TextDecoder('utf-8', { fatal: true }).decode(inputBytes).split('\n');
  if (inputLines.at(-1) !== '') {
    reject();
  }
  inputLines.pop();
  if (
    tuple.basename !== 'inputs.jsonl' ||
    tuple.byteLength !== inputBytes.length ||
    tuple.rowCount !== inputLines.length ||
    tuple.sha256 !== hash(inputBytes)
  ) {
    reject();
  }
  for (const line of inputLines) {
    JSON.parse(line);
  }
  const output = path.resolve(options.output);
  await mkdir(output, { mode: 0o700 });
  const source = new Database(path.resolve(options.source), {
      readonly: true,
      fileMustExist: true,
    }),
    references = migrationReferences(options.sourceSha256),
    at = Date.now(),
    findings = [];
  let target;
  try {
    source.pragma('query_only=ON');
    target = new Database(path.join(output, 'library.sqlite'));
    initializeRelationalStorage(target, `B:${options.sourceSha256}:${options.cut}`);
    const cutoff = source
        .prepare('SELECT recorded_at FROM event_commits WHERE commit_seq=?')
        .get(BigInt(options.cut)),
      cutoffAt = time(cutoff?.recorded_at);
    if (cutoffAt === null) {
      reject();
    }
    target.transaction(() => {
      append(target, references, at, 'source', [
        {
          eventId: references.eventId('ImportSourceRegistered', references.sourceId),
          eventType: 'ImportSourceRegistered',
          payload: {
            sourceId: references.sourceId,
            sourceSha256: options.sourceSha256,
            sourceCommit: options.sourceCommit,
            cutoffSeq: options.cut,
            cutoffAt,
            manifestSha256: options.auditSha256,
            route: 'B',
          },
        },
      ]);
      const members = mapLegacyMembers(source, references);
      findings.push(...members.findings);
      if (members.facts.length) {
        append(target, references, at, 'members', members.facts);
      }
      const accounts = mapLegacyAccounts(source, references);
      findings.push(...accounts.findings);
      if (accounts.facts.length) {
        append(target, references, at, 'accounts', accounts.facts);
      }
      const sharing = mapLegacySharing(source, references);
      findings.push(...sharing.findings);
      if (sharing.facts.length) {
        append(target, references, at, 'sharing', sharing.facts);
      }
    })();
    appendLegacyRequests(source, target, references, at, findings);
    findings.push({ code: 'FINANCIAL_PRIVATE_AND_REMAINING_PUBLIC_MAPPING_PENDING' });
    const report = {
      status: 'building',
      sourceSha256: options.sourceSha256,
      sourceCommit: options.sourceCommit,
      sourceCut: options.cut,
      route: 'B',
      findings,
      imported: { members: references.memberIds.size, jobs: references.jobIds.size },
      readyInstalled: false,
    };
    target.pragma('wal_checkpoint(TRUNCATE)');
    target.pragma('journal_mode=DELETE');
    if (
      inventoryLegacyDatabase(options.source, options.sourceCommit).source.databaseSha256 !==
      options.sourceSha256
    ) {
      reject();
    }
    await writeFile(path.join(output, 'report.private.json'), JSON.stringify(report, null, 2), {
      flag: 'wx',
      mode: 0o600,
    });
    return report;
  } finally {
    target?.close();
    source.close();
  }
}

if (process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url) {
  try {
    const names = new Map([
        ['source', 'source'],
        ['source-sha256', 'sourceSha256'],
        ['source-commit', 'sourceCommit'],
        ['cut', 'cut'],
        ['audit', 'audit'],
        ['audit-sha256', 'auditSha256'],
        ['artifacts', 'artifacts'],
        ['output', 'output'],
      ]),
      options = {};
    for (const argument of process.argv.slice(2)) {
      const match = /^--([^=]+)=(.+)$/.exec(argument),
        key = match && names.get(match[1]);
      if (!key || Object.hasOwn(options, key)) {
        reject();
      }
      options[key] = match[2];
    }
    if (Object.keys(options).length !== names.size) {
      reject();
    }
    const result = await migrateLegacyDatabase(options);
    console.log(
      JSON.stringify({
        status: result.status,
        imported: result.imported,
        findings: result.findings.length,
      }),
    );
    process.exitCode = result.readyInstalled ? 0 : 2;
  } catch {
    console.error(
      'Legacy migration failed; source is unchanged and no ready target was installed.',
    );
    process.exitCode = 1;
  }
}
