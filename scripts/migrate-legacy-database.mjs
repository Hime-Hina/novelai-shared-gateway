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
import { inputAssetSlots } from '../server/generation/member-contract.ts';

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
  };
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

// 先交付可证的终态困难切片；活动、附件、产物和金融责任不能被当作空历史吞掉。
function appendTerminalSlice(source, target, references, at, findings) {
  const inputs = new RelationalInputStore(target);
  for (const row of source
    .prepare('SELECT id,data,request_id FROM jobs ORDER BY order_value DESC')
    .iterate()) {
    const job = JSON.parse(row.data),
      inputRef = row.request_id || job.legacyInputRef || job.requestId,
      originalInput = inputRef
        ? source
            .prepare('SELECT member_id,payload FROM generation_requests WHERE id=?')
            .get(inputRef)
        : null,
      memberId = memberOwner(job, originalInput);
    if (
      job.id !== row.id ||
      !memberId ||
      !references.memberIds.has(memberId) ||
      !['failed', 'done'].includes(job.status) ||
      job.imageId ||
      job.generationAttempt ||
      job.resultPending ||
      row.request_id
    ) {
      findings.push({ code: 'JOB_REQUIRES_COMPLETE_OPENING_MAPPING', originalId: row.id });
      continue;
    }
    const original = originalInput ? JSON.parse(originalInput.payload).request : job.request,
      body = partialInput(original),
      inputId = `legacy-job-input:${row.id}`,
      requestId = `legacy-job-anchor:${row.id}`;
    if (body !== null && inputAssetSlots(body).length) {
      findings.push({ code: 'INPUT_ATTACHMENTS_REQUIRE_MAPPING', originalId: row.id });
      continue;
    }
    target.transaction(() => {
      const input = inputs.stageOpeningInput({
        inputId,
        memberId,
        completeness: body === null ? 'unavailable' : 'partial',
        body,
        logicalBytes: body === null ? 0 : Buffer.byteLength(canonicalJson(body)),
        assets: [],
      });
      append(target, references, at, `job:${row.id}`, [
        {
          eventId: references.eventId('OpeningInputRecorded', inputId),
          eventType: 'OpeningInputRecorded',
          memberId,
          payload: { ...input, origin: references.origin(inputRef || row.id) },
        },
        {
          eventId: references.eventId('OpeningRequestRecorded', requestId),
          eventType: 'OpeningRequestRecorded',
          memberId,
          payload: {
            requestId,
            originalRequestId: null,
            memberId,
            inputId,
            contextId: null,
            source: typeof job.source === 'string' ? job.source : null,
            operation: ['generate', 'upscale', 'encode-vibe'].includes(original?.operation)
              ? original.operation
              : null,
            model: typeof original?.model === 'string' ? original.model : null,
            sampleCount: null,
            seedMode: 'unknown',
            confirmation: null,
            acceptedAt: null,
            retiredAt: null,
            origin: references.origin(row.id),
          },
        },
        {
          eventId: references.eventId('OpeningJobRecorded', row.id),
          eventType: 'OpeningJobRecorded',
          memberId,
          payload: {
            jobId: row.id,
            requestId,
            createdAt: time(job.createdAt),
            sampleIndex: null,
            actualSeed: typeof original?.seed === 'number' ? original.seed : null,
            status: job.status,
            queueSeq: null,
            queueExpiresAt: null,
            deadlineAt: null,
            admissionEpoch: null,
            artifactId: null,
            cacheKey: typeof job.cacheKey === 'string' ? job.cacheKey : null,
            completedAt: time(job.completedAt),
            errorCode: typeof job.errorCode === 'string' ? job.errorCode : null,
            errorStatus:
              Number.isSafeInteger(job.errorStatus) && job.errorStatus > 0 ? job.errorStatus : null,
            cancellationReason:
              typeof job.cancellationReason === 'string' ? job.cancellationReason : null,
            lateResultAt: null,
            freeSucceededAt: null,
            freeWarningThreshold: null,
            retiredAt: null,
            origin: references.origin(row.id),
          },
        },
      ]);
    })();
    references.jobIds.add(row.id);
    references.requestsByJob.set(row.id, requestId);
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
  if (audit.sourceCommit !== options.sourceCommit) {
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
    appendTerminalSlice(source, target, references, at, findings);
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
