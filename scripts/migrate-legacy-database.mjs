import { createHash } from 'node:crypto';
import { existsSync } from 'node:fs';
import { lstat, mkdir, readFile, writeFile, open } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';
import { inventoryLegacyDatabase } from './database-model-inventory.mjs';
import { mapLegacyFinance, assertLegacyFinanceProjection } from './migrate-legacy-finance.mjs';
import { mapLegacyArtifacts } from './migrate-legacy-artifacts.mjs';
import { migrateLegacyPrivate } from './migrate-legacy-private.mjs';
import { RelationalConfigurationStore } from '../server/storage/relational-configuration-store.ts';
import { domainDigest, canonicalJson } from '../server/events/domain-events.ts';
import { relationalFactStream } from '../server/events/relational-fact-contracts.ts';
import { readableResultFile, readStoredResultContent } from '../server/storage/result-files.ts';
import { RelationalInputStore } from '../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../server/storage/relational-storage.ts';
import {
  readRetainedArchiveBinding,
  retainedFileSha256,
} from '../server/storage/retained-sources.ts';
import {
  readProjectedFairQuota,
  readStreamProjection,
  readArtifactInput,
} from '../server/events/event-store.ts';
import { freeImagePolicy } from '../server/quota/free-image-policy.ts';
import { lossAlertPolicy } from '../server/quota/failure-accounting.ts';
import { parseAmount, formatAmount } from '../server/quota/resource-amounts.ts';
import { inputAssetSlots, decodeInputAsset } from '../server/generation/member-contract.ts';
import {
  generationRequestFingerprint,
  requestNormalizationDefaults,
} from '../server/generation/request-fingerprint.ts';
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

export async function readLegacyCutConfiguration(filename, expectedSha256, selected, effectiveAt) {
  if (typeof filename !== 'string' || !/^[a-f0-9]{64}$/.test(expectedSha256 ?? '')) {
    reject();
  }
  const stat = await lstat(filename);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 65536) {
    reject();
  }
  const bytes = await readFile(filename);
  if (hash(bytes) !== expectedSha256) {
    reject();
  }
  const config = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)),
    keys = [
      'format',
      'sourceSha256',
      'cut',
      'sourceCommit',
      'captureStartedAt',
      'configuredOwnerUserId',
      'configuredGuildId',
      'discordLoginEnabled',
      'sourceNamespaces',
      'sourceCapture',
      'namespaceReceipt',
      'identityReceipt',
      'localOriginal',
    ];
  if (
    Object.keys(config).length !== keys.length ||
    keys.some((key) => !Object.hasOwn(config, key)) ||
    config.format !== 'novelai-legacy-cut-config-v1' ||
    config.sourceSha256 !== selected.sourceSha256 ||
    config.cut !== selected.cut ||
    config.sourceCommit !== selected.sourceCommit ||
    typeof config.configuredOwnerUserId !== 'string' ||
    typeof config.configuredGuildId !== 'string' ||
    typeof config.discordLoginEnabled !== 'boolean' ||
    (config.configuredGuildId && !config.discordLoginEnabled) ||
    !Number.isSafeInteger(effectiveAt) ||
    effectiveAt <= (time(config.captureStartedAt) ?? Infinity)
  ) {
    reject();
  }
  const directory = path.dirname(path.resolve(filename));
  for (const name of ['sourceCapture', 'namespaceReceipt', 'identityReceipt']) {
    const proof = config[name];
    if (
      !proof ||
      Object.keys(proof).length !== 2 ||
      typeof proof.file !== 'string' ||
      !/^[a-f0-9]{64}$/.test(proof.sha256 ?? '')
    ) {
      reject();
    }
    const file = path.resolve(directory, proof.file),
      info = await lstat(file);
    if (
      !info.isFile() ||
      info.isSymbolicLink() ||
      info.size > 16777216 ||
      (await retainedFileSha256(file)) !== proof.sha256
    ) {
      reject();
    }
  }
  const namespaces = config.sourceNamespaces;
  if (
    !namespaces ||
    Object.keys(namespaces).length !== 4 ||
    !['dataDir', 'r2AccountId', 'r2Bucket', 'pikpakUrl'].every(
      (name) =>
        Object.hasOwn(namespaces, name) &&
        (typeof namespaces[name] === 'string' || namespaces[name] === null),
    )
  ) {
    reject();
  }
  const local = config.localOriginal;
  if (local !== null) {
    const pointer = local?.sourceArtifactRowPointer;
    if (
      !local ||
      Object.keys(local).length !== 4 ||
      typeof local.file !== 'string' ||
      !local.file ||
      !Number.isSafeInteger(local.byteLength) ||
      local.byteLength < 1 ||
      !/^[a-f0-9]{64}$/.test(local.sha256 ?? '') ||
      !pointer ||
      Object.keys(pointer).length !== 4 ||
      typeof pointer.file !== 'string' ||
      !pointer.file ||
      !/^[a-f0-9]{64}$/.test(pointer.fileSha256 ?? '') ||
      !Number.isSafeInteger(pointer.line) ||
      pointer.line < 1 ||
      !/^[a-f0-9]{64}$/.test(pointer.lineSha256 ?? '')
    ) {
      reject();
    }
  }
  // 这些是原部署的 namespace；远端 dataDir 永不成为本地 target DATA_DIR。
  return {
    ...config,
    selectedSha256: expectedSha256,
    filename: path.resolve(filename),
    effectiveAt,
  };
}

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
    eventId: (eventType, originalId) => {
      if (eventType === 'OpeningFundingReceiptRecorded') {
        return originalId;
      }
      return `migration:${domainDigest({ sourceSha256, eventType, originalId })}`;
    },
    memberIds: new Set(),
    accountIds: new Set(),
    jobIds: new Set(),
    attemptIds: new Set(),
    requestsByJob: new Map(),
    credentialEpochByAccount: new Map(),
    inputsByJob: new Map(),
    inputsByArtifact: new Map(),
    requestProofs: new Map(),
    artifactAttemptById: new Map(),
    artifactHistoryById: new Map(),
    boundLocalMaterializationProofs: new Map(),
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

function append(target, references, at, replayer, inputs, key, drafts) {
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
        case 'artifact':
          owner = draft.payload.artifactId;
          break;
        case 'storage':
          owner = draft.payload.storageTargetId;
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
  return replayer.appendCommit(
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
    inputs,
  );
}

function appendIndependentOpenings(appendOpening, key, drafts) {
  for (let offset = 0; offset < drafts.length; offset += 100) {
    appendOpening(`${key}:${offset / 100}`, drafts.slice(offset, offset + 100));
  }
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
        enabled: account.enabled !== false,
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
        // 公共资格沿原 Raw owner；密码/Cookie 的 strict-true 在私有 adapter 各自核对。
        status: member.enabled !== false ? 'active' : 'disabled',
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

function originalDiagnostic(source, job, originalRequestId) {
  const state = readStreamProjection(
      source,
      `request:${originalRequestId || `legacy-${job.id}`}`,
    )?.projection,
    ids = new Set(
      [job.diagnosticRef, state?.jobs?.[job.id]?.diagnosticRef].filter(
        (value) => value !== undefined && value !== null,
      ),
    );
  if (ids.size > 1) {
    reject();
  }
  if (!ids.size) {
    return null;
  }
  const [id] = ids,
    row = source
      .prepare('SELECT * FROM generation_diagnostics WHERE diagnostic_id=? AND job_id=?')
      .get(id, job.id);
  if (
    !row ||
    typeof row.detail !== 'string' ||
    hash(Buffer.from(row.detail)) !== row.detail_sha256
  ) {
    reject();
  }
  let createdAt = null;
  if (row.created_at !== null) {
    if (typeof row.created_at !== 'string' || !row.created_at) {
      reject();
    }
    if (/^(?:0|[1-9][0-9]*)$/.test(row.created_at)) {
      const exact = BigInt(row.created_at);
      if (exact > BigInt(Number.MAX_SAFE_INTEGER)) {
        reject();
      }
      createdAt = Number(exact);
    } else {
      createdAt = Date.parse(row.created_at);
      if (
        !Number.isSafeInteger(createdAt) ||
        createdAt < 0 ||
        new Date(createdAt).toISOString() !== row.created_at
      ) {
        reject();
      }
    }
  }
  return {
    diagnosticId: id,
    jobId: job.id,
    sha256: row.detail_sha256,
    detail: row.detail,
    createdAt,
  };
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

function sourceContextProof(source, requestId, original, mappings) {
  const proofs = mappings.map((mapping) => ({
      mapping,
      proof: proveLegacyGenerationIntent(
        original,
        JSON.parse(mapping.normalization_context),
        mapping,
      ),
    })),
    accepted = source
      .prepare(
        `SELECT payload FROM domain_events WHERE stream_id=? AND event_type='RequestAccepted'
      ORDER BY commit_seq,event_index LIMIT 2`,
      )
      .all(`request:${requestId}`),
    first = accepted.length === 1 ? JSON.parse(accepted[0].payload).idempotency : null,
    primary = first
      ? proofs.find(
          ({ mapping, proof }) =>
            proof.prepared &&
            first.memberId === mapping.member_id &&
            first.operation === mapping.operation &&
            first.keyHash === mapping.key_hash &&
            first.fingerprint === mapping.fingerprint &&
            first.jobId === mapping.job_id &&
            first.requestId === mapping.request_id &&
            first.normalizationContextHash ===
              domainDigest(JSON.parse(mapping.normalization_context)),
        )
      : null;
  // 之后 alias 的定义只证明该 alias 自身，不能借给原未保存定义的受理。
  return { proof: primary?.proof ?? null, proofs };
}

// 每个真实 parent 的正文与 child 共用原来源；没有 parent 的 FK 容器不授予公开原请求身份。
function planLegacyRequests(source, references, findings) {
  const plans = [],
    groups = sourceRequestGroups(source, references);
  for (const group of groups.values()) {
    const jobs = group.jobIds.map((id) => {
        const row = source
          .prepare('SELECT id,data,request_id,order_value FROM jobs WHERE id=?')
          .get(id);
        const value = JSON.parse(row.data);
        return {
          row,
          value,
          diagnostic: originalDiagnostic(source, value, group.originalRequestId),
        };
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
      { proof, proofs } = sourceContextProof(source, group.originalRequestId, original, mappings),
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
        ({ value }) => !['queued', 'pending', 'running', 'failed', 'done'].includes(value.status),
      ) ||
      (hasActive && (!proof?.prepared || count === null))
    ) {
      findings.push({ code: 'JOB_REQUIRES_COMPLETE_OPENING_MAPPING', originalId: group.jobIds[0] });
      continue;
    }
    const inputId = group.originalRequestId || `legacy-job-input:${first.row.id}`;
    for (const { row, value: job } of jobs) {
      references.jobIds.add(row.id);
      if (typeof job.generationAttempt?.id === 'string') {
        references.attemptIds.add(job.generationAttempt.id);
      }
      references.inputsByJob.set(row.id, { inputId, memberId, operation });
    }
    plans.push({
      group,
      jobs,
      originalRow,
      protectedRow,
      original,
      memberId,
      proof,
      proofs,
      content,
      operation,
      model,
      count,
      inputId,
    });
  }
  return plans;
}

// 先规划原身份，金融 mapper 再返回原尝试/预扣；三者共用一笔完整提交。
function appendLegacyRequest(
  target,
  references,
  inputs,
  appendOpening,
  plan,
  financial,
  artifacts,
) {
  const {
      group,
      jobs,
      originalRow,
      protectedRow,
      original,
      memberId,
      proof,
      proofs,
      content,
      operation,
      model,
      count,
      inputId,
    } = plan,
    first = jobs[0],
    freeUsage = new Map(financial.expected.freeUsage.map((row) => [row.jobId, row]));
  for (const { row } of jobs) {
    if (freeUsage.has(row.id) && freeUsage.get(row.id).memberId !== memberId) {
      reject();
    }
  }
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
    for (const { row, value: job, diagnostic } of jobs) {
      const seed =
        typeof job.request?.seed === 'number'
          ? job.request.seed
          : group.originalRequestId
            ? null
            : original?.seed;
      let actualSeed = typeof seed === 'number' ? seed : null;
      if (operation !== null && operation !== 'generate') {
        actualSeed = null;
      }
      events.push({
        eventId: references.eventId('OpeningJobRecorded', row.id),
        eventType: 'OpeningJobRecorded',
        memberId,
        payload: {
          jobId: row.id,
          requestId: group.requestId,
          createdAt: time(job.createdAt),
          sourceOrder: row.order_value,
          displayCost:
            Object.hasOwn(job, 'cost') && typeof job.cost === 'number' && Number.isFinite(job.cost)
              ? job.cost
              : null,
          sampleIndex:
            Number.isSafeInteger(job.sampleIndex) && job.sampleIndex >= 0 ? job.sampleIndex : null,
          actualSeed,
          status: job.status,
          queueSeq:
            Number.isSafeInteger(job.queueSeq) && job.queueSeq > 0 ? String(job.queueSeq) : null,
          queueExpiresAt: time(job.queueExpiresAt),
          deadlineAt: time(job.deadlineAt),
          admissionEpoch:
            Number.isSafeInteger(job.jobAdmissionVersion) && job.jobAdmissionVersion >= 0
              ? job.jobAdmissionVersion
              : null,
          artifactId:
            typeof (job.imageId || job.lateResultImageId) === 'string' &&
            (job.imageId || job.lateResultImageId)
              ? job.imageId || job.lateResultImageId
              : null,
          cacheKey: typeof job.cacheKey === 'string' && job.cacheKey ? job.cacheKey : null,
          completedAt: time(job.completedAt),
          diagnosticId: diagnostic?.diagnosticId ?? null,
          errorCode: typeof job.errorCode === 'string' && job.errorCode ? job.errorCode : null,
          errorStatus:
            Number.isSafeInteger(job.errorStatus) && job.errorStatus > 0 ? job.errorStatus : null,
          cancellationReason:
            typeof job.cancellationReason === 'string' && job.cancellationReason
              ? job.cancellationReason
              : null,
          lateResultAt: time(job.lateResultAt),
          freeSucceededAt: freeUsage.get(row.id)?.succeededAt ?? null,
          freeWarningThreshold: freeUsage.get(row.id)?.warningThreshold ?? null,
          retiredAt: null,
          origin: references.origin(row.id),
        },
      });
    }

    for (const { diagnostic } of jobs) {
      if (diagnostic) {
        const payload = inputs.stageOpeningDiagnostic({
          ...diagnostic,
          origin: references.origin(diagnostic.diagnosticId),
        });
        events.push({
          eventId: references.eventId('OpeningDiagnosticRecorded', diagnostic.diagnosticId),
          eventType: 'OpeningDiagnosticRecorded',
          streamId: `request:${group.requestId}`,
          memberId,
          payload,
        });
      }
    }
    const ids = new Set(jobs.map(({ row }) => row.id));
    for (const fact of financial.facts) {
      if (fact.eventType === 'OpeningAttemptRecorded' && ids.has(fact.payload.jobId)) {
        events.push({ ...fact, streamId: `request:${group.requestId}` });
      }
    }
    for (const fact of artifacts.facts) {
      if (
        fact.eventType === 'OpeningArtifactRecorded' &&
        fact.payload.originalAttemptId !== null &&
        jobs.some(({ value }) => value.generationAttempt?.id === fact.payload.originalAttemptId)
      ) {
        events.push(fact);
      }
    }
    for (const fact of financial.facts) {
      if (fact.eventType === 'OpeningChargeRecorded' && ids.has(fact.payload.jobId)) {
        events.push(fact);
      }
    }
    appendOpening(`request:${group.requestId}`, events);
    references.requestProofs.set(group.requestId, {
      inputId,
      memberId,
      contextId,
      proofs,
      originalRequestId: group.originalRequestId,
    });
  })();
}

// 原 artifact stream 的归属是旧 owner 已保存的 cut 事实；不从 cache visitor/当前 token 反推 creator。
function planLegacyArtifacts(
  source,
  references,
  descriptors,
  cutConfiguration,
  localMaterializationsByArtifact = new Map(),
) {
  const inputs = [],
    findings = [],
    storageTargets = [{ kind: 'local', namespace: 'images', originalId: 'storage:local' }],
    publications = new Map();
  // 一次原事实扫描；不为每个产物重复 json_extract 全事件，也不持久化派生索引。
  for (const row of source
    .prepare(
      "SELECT occurred_at,payload FROM domain_events WHERE event_type='ArtifactPublished' ORDER BY commit_seq,event_index",
    )
    .iterate()) {
    const payload = JSON.parse(row.payload),
      id = payload.artifact?.id;
    if (typeof id !== 'string' || publications.has(id)) {
      reject();
    }
    publications.set(id, { ...row, value: payload });
  }
  if (
    cutConfiguration?.sourceNamespaces.r2AccountId &&
    cutConfiguration.sourceNamespaces.r2Bucket
  ) {
    storageTargets.push({
      kind: 'r2',
      namespace: `r2://${cutConfiguration.sourceNamespaces.r2AccountId}/${cutConfiguration.sourceNamespaces.r2Bucket}`,
      originalId: 'storage:r2',
    });
  }
  if (cutConfiguration?.sourceNamespaces.pikpakUrl) {
    storageTargets.push({
      kind: 'pikpak',
      namespace: cutConfiguration.sourceNamespaces.pikpakUrl,
      originalId: 'storage:pikpak',
    });
  }
  for (const descriptor of descriptors) {
    const original = readStreamProjection(source, `artifact:${descriptor.id}`)?.projection,
      savedPublication = publications.get(descriptor.id),
      publication = savedPublication?.value ?? null,
      memberId = publication ? publication.memberId : original?.memberId;
    // publication 拥有 creator/input，后续 artifact stream 只拥有 storage/retirement。
    // 不能要求仅有 storage 事件的流再保存一份 member，也不能借当前 Key 补原身份。
    if (
      (original && original.id !== descriptor.id) ||
      (publication &&
        (typeof memberId !== 'string' ||
          !memberId ||
          (original?.memberId != null && original.memberId !== memberId) ||
          publication.artifact.sha256 !== descriptor.sha256 ||
          publication.artifact.byteLength !== descriptor.byteLength))
    ) {
      reject();
    }
    if (!references.memberIds.has(memberId)) {
      continue;
    }
    if (descriptor.memberId !== null && descriptor.memberId !== memberId) {
      reject();
    }
    let input = null;
    if (
      publication &&
      publication.memberId === memberId &&
      publication.artifact.sha256 === descriptor.sha256 &&
      publication.artifact.byteLength === descriptor.byteLength
    ) {
      input = references.inputsByJob.get(publication.jobId);
      const attemptId = publication.artifact.attemptId;
      if (attemptId && !references.attemptIds.has(attemptId)) {
        findings.push({ code: 'ARTIFACT_ORIGINAL_ATTEMPT_UNMAPPED', originalId: descriptor.id });
      }
      if (!input) {
        findings.push({ code: 'ARTIFACT_ORIGINAL_INPUT_UNMAPPED', originalId: descriptor.id });
      }
      if (input && attemptId !== null && references.attemptIds.has(attemptId)) {
        references.artifactAttemptById.set(descriptor.id, {
          attemptId,
          artifactId: descriptor.id,
          inputId: input.inputId,
          memberId: input.memberId,
          accountId: publication.accountId,
        });
      }
      if (input) {
        references.artifactHistoryById.set(descriptor.id, {
          inputId: input.inputId,
          memberId: input.memberId,
          sha256: descriptor.sha256,
          byteLength: descriptor.byteLength,
          publishedAt: time(savedPublication.occurred_at),
          retiredAt: time(original?.retired?.at),
        });
      }
    }
    if (!input) {
      const saved = original?.artifactInputRef
          ? readArtifactInput(source, original?.artifactInputRef, original.artifactInputSha256)
          : null,
        request = {};
      // base64 在 artifact_inputs 是原结果，不能成为上游 image 附件或新输入字节。
      if (saved && Object.hasOwn(saved, 'fullPrompt')) {
        request.prompt = saved.fullPrompt;
      } else if (saved && Object.hasOwn(saved, 'prompt')) {
        request.prompt = saved.prompt;
      }
      if (descriptor.operation !== null) {
        request.operation = descriptor.operation;
      }
      const inputId = `legacy-artifact-input:${descriptor.id}`;
      input = { inputId, memberId, operation: descriptor.operation };
      inputs.push({ ...input, content: openingInput(request, null), originalId: descriptor.id });
    }
    if (!references.artifactHistoryById.has(descriptor.id)) {
      references.artifactHistoryById.set(descriptor.id, {
        inputId: input.inputId,
        memberId: input.memberId,
        sha256: descriptor.sha256,
        byteLength: descriptor.byteLength,
        publishedAt: null,
        retiredAt: time(original?.retired?.at),
      });
    }
    references.inputsByArtifact.set(descriptor.id, input);
  }
  const mapped = mapLegacyArtifacts(source, references, {
    artifacts: descriptors,
    storageTargets,
    localMaterializationsByArtifact,
  });
  return { ...mapped, inputs, findings: [...findings, ...mapped.findings] };
}

function appendLegacyArtifactInputs(inputs, references, appendOpening, artifacts) {
  for (let offset = 0; offset < artifacts.inputs.length; offset += 100) {
    const events = artifacts.inputs.slice(offset, offset + 100).map((input) => {
      const payload = inputs.stageOpeningInput({
        inputId: input.inputId,
        memberId: input.memberId,
        ...input.content,
      });
      return {
        eventId: references.eventId('OpeningInputRecorded', input.inputId),
        eventType: 'OpeningInputRecorded',
        memberId: input.memberId,
        payload: { ...payload, origin: references.origin(input.originalId) },
      };
    });
    appendOpening(`artifact-inputs:${offset / 100}`, events);
  }
}

function appendLegacyKeys(source, target, references, inputs, appendOpening, findings) {
  const decisions = [];
  const registration =
    source.prepare(`SELECT e.event_id,e.event_type,e.actor_id,e.actor_kind,e.payload,
      c.command_id,c.receipt FROM domain_events e JOIN event_commits c USING(commit_seq)
    WHERE e.event_type IN ('RequestAccepted','RequestAliasRegistered')
      AND e.actor_kind='member' AND e.actor_id=?
      AND json_extract(e.payload,'$.idempotency.operation')=?
      AND json_extract(e.payload,'$.idempotency.keyHash')=? ORDER BY e.commit_seq,e.event_index LIMIT 2`);
  for (const row of source
    .prepare('SELECT * FROM generation_idempotency ORDER BY member_id,operation,key_hash')
    .iterate()) {
    const originalId = canonicalJson([row.member_id, row.operation, row.key_hash]),
      registered = registration.all(row.member_id, row.operation, row.key_hash),
      record = registered.length === 1 ? registered[0] : null,
      recorded = record ? JSON.parse(record.payload) : null,
      saved =
        record?.event_type === 'RequestAliasRegistered'
          ? {
              ...recorded.idempotency,
              memberId: recorded.memberId,
              jobId: recorded.jobId,
              requestId: recorded.requestId,
            }
          : recorded?.idempotency,
      actual =
        record &&
        record.actor_kind === 'member' &&
        record.actor_id === row.member_id &&
        saved.memberId === row.member_id &&
        saved.fingerprint === row.fingerprint &&
        saved.jobId === row.job_id &&
        (saved.requestId ?? null) === row.request_id
          ? record
          : null,
      group = row.request_id === null ? null : references.requestProofs.get(row.request_id),
      state = readStreamProjection(
        source,
        `request:${row.request_id || `legacy-${row.job_id}`}`,
      )?.projection,
      retired = Boolean(state?.retiredJobs?.[row.job_id]),
      originalInputRef = row.request_id || state?.request?.inputRef,
      originalInput = originalInputRef
        ? source
            .prepare('SELECT member_id,payload FROM generation_requests WHERE id=?')
            .get(originalInputRef)
        : null,
      savedInput = originalInput ? JSON.parse(originalInput.payload) : null,
      original = savedInput?.legacyInline === true ? savedInput.request : savedInput,
      entry =
        group?.proofs.find(
          ({ mapping }) =>
            mapping.member_id === row.member_id &&
            mapping.operation === row.operation &&
            mapping.key_hash === row.key_hash,
        ) ??
        (originalInput?.member_id === row.member_id
          ? {
              mapping: row,
              proof: proveLegacyGenerationIntent(
                original,
                JSON.parse(row.normalization_context),
                row,
              ),
            }
          : null),
      receipt = actual ? JSON.parse(actual.receipt) : null,
      createdAt = time(row.created_at);
    if (
      createdAt === null ||
      !/^[a-f0-9]{64}$/.test(row.key_hash) ||
      !/^[a-f0-9]{64}$/.test(row.fingerprint)
    ) {
      findings.push({ code: 'KEY_SOURCE_FIELDS_UNSUPPORTED', originalId });
      continue;
    }
    let live = Boolean(
      actual &&
      group?.contextId &&
      entry?.proof.prepared &&
      receipt?.requestId === row.request_id &&
      Array.isArray(receipt.jobIds) &&
      receipt.jobIds.length === entry.proof.prepared.sampleCount &&
      receipt.jobIds[0] === row.job_id &&
      receipt.jobIds.every((id) => references.jobIds.has(id)),
    );
    const jobs = group
      ? target
          .prepare('SELECT id FROM generation_jobs WHERE request_id=? ORDER BY sample_index')
          .all(row.request_id)
      : [];
    live &&= canonicalJson(jobs.map(({ id }) => id)) === canonicalJson(receipt.jobIds);
    const payload = {
      originalMemberId: row.member_id,
      operation: row.operation,
      keyHash: row.key_hash,
      intentFingerprint: row.fingerprint,
      originalRequestId: row.request_id,
      originalJobId: row.job_id,
      originalCommandId: actual?.command_id ?? null,
      noCache: entry?.proof.prepared ? entry.proof.noCache : null,
      seedMode: entry?.proof.prepared ? entry.proof.seedMode : null,
      requestId: group ? row.request_id : null,
      commitSeq: null,
      contextId: entry?.proof.prepared?.contextId ?? null,
      resolution: live ? 'live' : retired ? 'unavailable' : actual ? 'archived' : 'unavailable',
      originRef: actual?.event_id ?? originalId,
      archivedReceipt: live
        ? {
            requestId: row.request_id,
            jobIds: receipt.jobIds,
            ...(receipt.cacheHit === true ? { cacheHit: true } : {}),
          }
        : null,
      createdAt,
      origin: references.origin(originalId),
    };
    decisions.push({
      originalId,
      firstDefinitionProved: Boolean(entry?.proof.prepared),
      originalCommandProved: Boolean(actual),
      sourceRetired: retired,
      resolution: payload.resolution,
    });
    if (entry?.proof.prepared && !live && !retired) {
      findings.push({ code: 'COMPLETE_KEY_TARGET_CLOSURE_UNPROVEN', originalId });
    }
    if (entry?.proof.reason && JSON.parse(row.normalization_context).native) {
      findings.push({ code: entry.proof.reason, originalId });
    }
    target.transaction(() => {
      const events = [];
      if (
        payload.contextId !== null &&
        !target.prepare('SELECT 1 FROM normalization_contexts WHERE id=?').get(payload.contextId)
      ) {
        const context = inputs.stageOpeningContext(payload.contextId, entry.proof.context);
        events.push({
          eventId: references.eventId('ContextRegistered', payload.contextId),
          eventType: 'ContextRegistered',
          payload: context,
        });
      }
      events.push({
        eventId: references.eventId('OpeningIdempotencyRecorded', originalId),
        eventType: 'OpeningIdempotencyRecorded',
        streamId: `idempotency:${row.member_id}:${row.operation}:${row.key_hash}`,
        memberId: row.member_id,
        payload,
      });
      appendOpening(`key:${originalId}`, events);
    })();
  }
  return decisions;
}

export async function materializeSelectedLocalOriginal(
  configuration,
  descriptors,
  references,
  output,
) {
  const proofs = new Map();
  if (!configuration?.localOriginal) {
    return proofs;
  }
  const directory = path.dirname(configuration.filename),
    selected = configuration.localOriginal,
    pointer = selected.sourceArtifactRowPointer,
    rowFile = path.resolve(directory, pointer.file),
    rowStat = await lstat(rowFile);
  if (!rowStat.isFile() || rowStat.isSymbolicLink() || rowStat.size > 16777216) {
    reject();
  }
  const rowBytes = await readFile(rowFile);
  if (hash(rowBytes) !== pointer.fileSha256) {
    reject();
  }
  let start = 0,
    end = -1;
  for (let index = 1; index <= pointer.line; index++) {
    end = rowBytes.indexOf(10, start);
    if (end < 0) {
      reject();
    }
    if (index !== pointer.line) {
      start = end + 1;
    }
  }
  // 选定 pointer 绑定原行完整 bytes（含其终止 LF）；不 trim、重编码或改用另一摘要。
  const line = rowBytes.subarray(start, end + 1);
  if (hash(line) !== pointer.lineSha256) {
    reject();
  }
  const original = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(line)),
    descriptor = descriptors.find((row) => row.id === original.id),
    input = references.inputsByArtifact.get(original.id);
  if (
    !descriptor ||
    !input ||
    !references.memberIds.has(input.memberId) ||
    original.sha256 !== descriptor.sha256 ||
    original.byteLength !== descriptor.byteLength ||
    selected.sha256 !== descriptor.sha256 ||
    selected.byteLength !== descriptor.byteLength
  ) {
    reject();
  }
  const file = readableResultFile({
      imageId: descriptor.id,
      operation: descriptor.operation,
      mimeType: descriptor.mimeType,
    }),
    selectedFile = path.resolve(directory, selected.file),
    info = await lstat(selectedFile);
  if (!info.isFile() || info.isSymbolicLink() || info.size !== selected.byteLength) {
    reject();
  }
  const handle = await open(selectedFile, 'r');
  let bytes;
  try {
    const before = await handle.stat({ bigint: true });
    bytes = await handle.readFile();
    const after = await handle.stat({ bigint: true });
    if (
      !before.isFile() ||
      before.dev !== after.dev ||
      before.ino !== after.ino ||
      before.size !== after.size ||
      before.mtimeNs !== after.mtimeNs ||
      before.ctimeNs !== after.ctimeNs ||
      bytes.length !== selected.byteLength ||
      hash(bytes) !== selected.sha256
    ) {
      reject();
    }
  } finally {
    await handle.close();
  }
  await mkdir(path.join(output, 'images'), { mode: 0o700 });
  await writeFile(path.join(output, file), bytes, { flag: 'wx', mode: 0o600 });
  await readStoredResultContent(output, {
    imageId: descriptor.id,
    operation: descriptor.operation,
    mimeType: descriptor.mimeType,
    byteLength: descriptor.byteLength,
    sha256: descriptor.sha256,
    file,
  });
  const tuple = {
      artifactId: descriptor.id,
      memberId: input.memberId,
      inputId: input.inputId,
      sha256: descriptor.sha256,
      byteLength: descriptor.byteLength,
      file,
      verifiedAt: Date.now(),
    },
    proof = {
      ...tuple,
      proofRef: `local-materialization:${domainDigest({
        sourceSha256: references.sourceSha256,
        cutConfigurationSha256: configuration.selectedSha256,
        ...tuple,
      })}`,
    };
  proofs.set(descriptor.id, proof);
  references.boundLocalMaterializationProofs.set(proof.proofRef, domainDigest(proof));
  return proofs;
}

function appendLegacyConfiguration(source, target, references, at, findings) {
  const row = source.prepare("SELECT value FROM app_settings WHERE key='settings'").get();
  if (!row) {
    findings.push({ code: 'CURRENT_CONFIGURATION_SOURCE_MISSING' });
    return;
  }
  const settings = JSON.parse(row.value);
  new RelationalConfigurationStore(target).initializeGatewayConfiguration(
    {
      commandId: `migration:${references.sourceId}:configuration`,
      actorKind: 'system',
      actorId: null,
      recordedAt: at,
      correlationId: null,
      causationId: null,
    },
    {
      normalizationDefaults: requestNormalizationDefaults(settings),
      publicBaseUrl: typeof settings.publicBaseUrl === 'string' ? settings.publicBaseUrl : null,
      originRef: `${references.sourceId}:app_settings:settings`,
    },
  );
}

function selectedPrivateRows(source, configuration, effectiveAt) {
  const rows = (table) =>
      source
        .prepare(`SELECT id,token,data FROM ${table} ORDER BY id`)
        .all()
        .map((row) => ({ ...row, data: JSON.parse(row.data) })),
    safeTime = (value) =>
      typeof value === 'bigint' && value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER)
        ? Number(value)
        : null,
    ack = source.prepare("SELECT value FROM app_meta WHERE key='member_session_owner'").get();
  return {
    users: rows('users'),
    accounts: rows('accounts'),
    sessions: source
      .prepare(
        'SELECT token_hash,user_id,auth_version,created_at,expires_at,owner_binding FROM member_sessions ORDER BY token_hash',
      )
      .safeIntegers(true)
      .all()
      .map((row) => ({
        ...row,
        created_at: safeTime(row.created_at),
        expires_at: safeTime(row.expires_at),
      })),
    sessionOwner: ack ? JSON.parse(ack.value) : null,
    configuredOwner: {
      ownerUserId: configuration.configuredOwnerUserId,
      guildId: configuration.configuredGuildId,
    },
    discordLoginEnabled: configuration.discordLoginEnabled,
    effectiveAt,
  };
}

export async function migrateLegacyDatabase(options) {
  let phase = 'arguments';
  try {
    const at = Date.now();
    if (
      !options ||
      !/^[a-f0-9]{64}$/.test(options.sourceSha256 ?? '') ||
      !/^[a-f0-9]{64}$/.test(options.auditSha256 ?? '') ||
      !/^[1-9][0-9]*$/.test(options.cut ?? '')
    ) {
      reject();
    }
    phase = 'source-inventory';
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
    phase = 'archive-proof';
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
    const descriptors = (await readFile(options.artifacts, 'utf8'))
      .split('\n')
      .filter(Boolean)
      .map((line) => JSON.parse(line));
    phase = 'cut-configuration';
    const cutConfiguration = options.cutConfig
      ? await readLegacyCutConfiguration(options.cutConfig, options.cutConfigSha256, options, at)
      : null;
    const output = path.resolve(options.output);
    await mkdir(output, { mode: 0o700 });
    const source = new Database(path.resolve(options.source), {
        readonly: true,
        fileMustExist: true,
      }),
      references = migrationReferences(options.sourceSha256),
      findings = [];
    let target,
      financial,
      privateResult = null,
      keyDecisions = [];
    try {
      source.pragma('query_only=ON');
      phase = 'public-source-plan';
      const prepared = source.transaction(() => {
        const members = mapLegacyMembers(source, references),
          accounts = mapLegacyAccounts(source, references),
          sharing = mapLegacySharing(source, references);
        findings.push(...members.findings, ...accounts.findings, ...sharing.findings);
        const requests = planLegacyRequests(source, references, findings);
        // Build proven stable input references before the selected physical copy, with no target SQL or upload.
        planLegacyArtifacts(source, references, descriptors, cutConfiguration);
        return { members, accounts, sharing, requests };
      })();
      phase = 'local-materialization';
      const localMaterializations = await materializeSelectedLocalOriginal(
          cutConfiguration,
          descriptors,
          references,
          output,
        ),
        inputManifest = {
          format: 'legacy-B-inputs-v1',
          converterSha256: hash(await readFile(new URL(import.meta.url))),
          sourceSha256: options.sourceSha256,
          cut: options.cut,
          sourceCommit: options.sourceCommit,
          auditSha256: options.auditSha256,
          cutConfigurationSha256: cutConfiguration?.selectedSha256 ?? null,
          effectiveAt: at,
          localMaterializations: [...localMaterializations.values()],
        },
        manifestBytes = Buffer.from(canonicalJson(inputManifest)),
        manifestSha256 = hash(manifestBytes);
      await writeFile(path.join(output, 'migration-inputs.private.json'), manifestBytes, {
        flag: 'wx',
        mode: 0o600,
      });
      phase = 'public-opening';
      target = new Database(path.join(output, 'library.sqlite'));
      initializeRelationalStorage(target, `B:${options.sourceSha256}:${options.cut}`);
      const replayer = new RelationalCommitReplayer(target),
        inputs = new RelationalInputStore(target),
        appendOpening = append.bind(null, target, references, at, replayer, inputs),
        cutoff = source
          .prepare('SELECT recorded_at FROM event_commits WHERE commit_seq=?')
          .get(BigInt(options.cut)),
        cutoffAt = time(cutoff?.recorded_at);
      if (cutoffAt === null) {
        reject();
      }
      source.transaction(() =>
        target.transaction(() => {
          appendOpening('source', [
            {
              eventId: references.eventId('ImportSourceRegistered', references.sourceId),
              eventType: 'ImportSourceRegistered',
              payload: {
                sourceId: references.sourceId,
                sourceSha256: options.sourceSha256,
                sourceCommit: options.sourceCommit,
                cutoffSeq: options.cut,
                cutoffAt,
                manifestSha256,
                route: 'B',
              },
            },
          ]);
          appendLegacyConfiguration(source, target, references, at, findings);
          const { members, accounts, sharing, requests } = prepared;
          if (members.facts.length) {
            appendOpening('members', members.facts);
          }

          if (accounts.facts.length) {
            appendOpening('accounts', accounts.facts);
          }

          if (sharing.facts.length) {
            appendOpening('sharing', sharing.facts);
          }
          const artifacts = planLegacyArtifacts(
            source,
            references,
            descriptors,
            cutConfiguration,
            localMaterializations,
          );
          findings.push(...artifacts.findings);
          financial = mapLegacyFinance(source, references);
          findings.push(...financial.findings);
          const baseline = financial.facts.filter(
            (fact) =>
              fact.eventType !== 'OpeningAttemptRecorded' &&
              !(fact.eventType === 'OpeningChargeRecorded' && fact.payload.jobId !== null),
          );
          if (baseline.length) {
            appendOpening('financial', baseline);
          }
          appendLegacyArtifactInputs(inputs, references, appendOpening, artifacts);
          const artifactBaseline = artifacts.facts.filter(
            (fact) =>
              fact.eventType === 'OpeningStorageTargetRecorded' ||
              (fact.eventType === 'OpeningArtifactRecorded' &&
                fact.payload.originalAttemptId === null),
          );
          appendIndependentOpenings(appendOpening, 'artifacts', artifactBaseline);
          for (const request of requests) {
            appendLegacyRequest(
              target,
              references,
              inputs,
              appendOpening,
              request,
              financial,
              artifacts,
            );
          }
          const replicas = artifacts.facts.filter(
            (fact) => fact.eventType === 'OpeningReplicaRecorded',
          );
          appendIndependentOpenings(appendOpening, 'replicas', replicas);
          assertLegacyFinanceProjection(target, financial.expected);
          keyDecisions = appendLegacyKeys(
            source,
            target,
            references,
            inputs,
            appendOpening,
            findings,
          );
          if (cutConfiguration) {
            phase = 'private-opening';
            privateResult = migrateLegacyPrivate(
              target,
              selectedPrivateRows(source, cutConfiguration, at),
              references,
            );
          }
        })(),
      )();
      if (!cutConfiguration) {
        findings.push({ code: 'SELECTED_PRIVATE_CONFIGURATION_REQUIRED' });
      }
      findings.push({ code: 'REMAINING_PUBLIC_RECONCILIATION_AND_READY_PENDING' });
      const report = {
        status: 'building',
        sourceSha256: options.sourceSha256,
        sourceCommit: options.sourceCommit,
        sourceCut: options.cut,
        route: 'B',
        findings,
        imported: { members: references.memberIds.size, jobs: references.jobIds.size },
        readyInstalled: false,
        financial: financial.expected,
        private: privateResult,
        keyDecisions,
        effectiveAt: at,
        cutConfigurationSha256: cutConfiguration?.selectedSha256 ?? null,
        inputManifestSha256: manifestSha256,
      };
      target.pragma('wal_checkpoint(TRUNCATE)');
      target.pragma('journal_mode=DELETE');
      phase = 'source-final-hash';
      const sidecars = ['-wal', '-shm', '-journal'];
      if (
        sidecars.some((suffix) => existsSync(options.source + suffix)) ||
        (await retainedFileSha256(options.source)) !== options.sourceSha256 ||
        sidecars.some((suffix) => existsSync(options.source + suffix))
      ) {
        reject();
      }
      phase = 'report';
      await writeFile(path.join(output, 'report.private.json'), JSON.stringify(report, null, 2), {
        flag: 'wx',
        mode: 0o600,
      });
      return report;
    } finally {
      target?.close();
      source.close();
    }
  } catch (error) {
    if (error && typeof error === 'object') {
      Object.defineProperty(error, 'migrationPhase', { value: phase, configurable: true });
    }
    throw error;
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
        ['cut-config', 'cutConfig'],
        ['cut-config-sha256', 'cutConfigSha256'],
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
  } catch (error) {
    const phase = typeof error?.migrationPhase === 'string' ? error.migrationPhase : 'arguments';
    console.error(`Legacy migration failed at ${phase}; no ready target was installed.`);
    process.exitCode = 1;
  }
}
