import { createHash } from 'node:crypto';
import { mapLegacyArtifacts } from './migrate-legacy-artifacts.mjs';
import { legacyProxy } from './migrate-legacy-private.mjs';
import { canonicalJson, domainDigest } from '../server/events/domain-events.ts';
import { readStreamProjection, readProjectedFairQuota } from '../server/events/event-store.ts';
import { relationalInputDigest } from '../server/generation/relational-input.ts';
import { requestNormalizationDefaults } from '../server/generation/request-fingerprint.ts';
import { RelationalConfigurationStore } from '../server/storage/relational-configuration-store.ts';
import { relationalPrivateTables } from '../server/storage/relational-private-restore.ts';
import { isOwner } from '../server/members/authentication.ts';

const fail = (category) => {
  throw new Error(`Legacy source reconciliation mismatch: ${category}.`);
};
const requireValue = (condition, category) => {
  if (!condition) {
    fail(category);
  }
};
const same = (actual, expected, category) =>
  requireValue(canonicalJson(actual) === canonicalJson(expected), category);
const sha = (value) => createHash('sha256').update(value).digest('hex');
const time = (value) => {
  const result = typeof value === 'string' && value ? Date.parse(value) : value;
  return Number.isSafeInteger(result) && result >= 0 ? result : null;
};
const nullable = (value) => value ?? null;
const sets = (actual, expected, category) =>
  same([...actual].sort(), [...expected].sort(), category);
const sqlValue = (value) => {
  if (typeof value === 'boolean') {
    return Number(value);
  }
  if (value !== null && typeof value === 'object') {
    return canonicalJson(value);
  }
  return value;
};

function assertOriginalPrivateState(source, target, configuration, effectiveAt) {
  requireValue(
    configuration && Number.isSafeInteger(effectiveAt) && effectiveAt >= 0,
    'private_source',
  );
  const expected = Object.fromEntries(relationalPrivateTables.map(([table]) => [table, []])),
    users = new Map(),
    validText = (value) =>
      typeof value === 'string' && Buffer.from(value).toString('utf8') === value,
    validHash = (value) => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value),
    validTime = (value) =>
      typeof value === 'bigint' && value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER),
    environment = {
      OWNER_USER_ID: configuration.configuredOwnerUserId,
      DISCORD_GUILD_ID: configuration.configuredGuildId,
    },
    previous = source.prepare("SELECT value FROM app_meta WHERE key='member_session_owner'").get(),
    owner = previous ? JSON.parse(previous.value) : null;
  // 从封存旧源重新读取；mapper 的计数、草稿和目标私有快照都不能证明原来源内容。
  for (const row of source.prepare('SELECT id,token,data FROM users ORDER BY id').iterate()) {
    const user = JSON.parse(row.data),
      originalEpoch =
        validText(user.memberAuth?.version) && user.memberAuth.version
          ? user.memberAuth.version
          : null,
      membershipPermits = !configuration.discordLoginEnabled || user.discord?.member !== false,
      apiEligible = user.enabled !== false && membershipPermits,
      key = user.token,
      keyValid =
        apiEligible &&
        validText(key) &&
        key.startsWith('STA1N-') &&
        key.trim() === key &&
        key.length <= 4096 &&
        !key.includes('\0'),
      member = target.prepare('SELECT auth_epoch,note_json FROM members WHERE id=?').get(row.id);
    requireValue(member, 'private_member_identity');
    if (apiEligible && !originalEpoch && keyValid) {
      // 无旧版本的 API-only Key 允许新首代；它不能成为保留旧密码/Cookie 的依据。
      requireValue(
        typeof member.auth_epoch === 'string' && /^[a-f0-9]{32}$/.test(member.auth_epoch),
        'private_member_epoch',
      );
    } else {
      same(member.auth_epoch, apiEligible ? originalEpoch : null, 'private_member_epoch');
    }
    same(
      member.note_json,
      typeof user.note === 'string' ? JSON.stringify(user.note) : null,
      'private_member_note',
    );
    users.set(row.id, {
      user,
      originalEpoch,
      cookieEligible: user.enabled === true && membershipPermits,
    });
    const auth = user.memberAuth;
    if (auth?.username !== undefined) {
      const passwordEligible = user.enabled === true && originalEpoch !== null;
      expected.password_credentials.push({
        member_id: row.id,
        username: auth.username,
        salt: passwordEligible ? (auth.salt ?? null) : null,
        password_hash: passwordEligible ? (auth.hash ?? null) : null,
      });
    }
    if (keyValid) {
      same(row.token, key, 'private_key_source');
      expected.api_keys.push({ member_id: row.id, key_hash: sha(key), recoverable_key: key });
    }
    const discord = user.discord;
    if (discord?.id) {
      expected.discord_bindings.push({
        member_id: row.id,
        discord_id: discord.id,
        guild_id: discord.guildId,
        access_token: apiEligible ? (discord.accessToken ?? null) : null,
        refresh_token: apiEligible ? (discord.refreshToken ?? null) : null,
        expires_at: discord.expiresAt ?? null,
        checked_at: discord.checkedAt ?? null,
        eligible: discord.member == null ? null : Number(discord.member),
      });
    }
    const invite = user.discordInvite;
    if (
      apiEligible &&
      !discord?.id &&
      validHash(invite?.hash) &&
      Number.isSafeInteger(invite.expiresAt) &&
      invite.expiresAt > effectiveAt
    ) {
      expected.discord_invites.push({
        member_id: row.id,
        code_hash: invite.hash,
        expires_at: invite.expiresAt,
      });
    }
  }
  if (owner) {
    expected.member_auth_state.push({
      id: 1,
      owner_member_id: owner.ownerUserId,
      guild_id: owner.guildId,
    });
  }
  const ownerChanged =
    owner &&
    (owner.ownerUserId !== environment.OWNER_USER_ID ||
      owner.guildId !== environment.DISCORD_GUILD_ID);
  for (const session of source
    .prepare('SELECT * FROM member_sessions ORDER BY token_hash')
    .safeIntegers()
    .iterate()) {
    const identity = users.get(session.user_id),
      binding = isOwner(identity?.user, environment)
        ? JSON.stringify([session.user_id, identity.user.discord.id, environment.DISCORD_GUILD_ID])
        : null,
      affected =
        ownerChanged &&
        (session.owner_binding !== null ||
          session.user_id === owner.ownerUserId ||
          session.user_id === environment.OWNER_USER_ID);
    if (
      owner &&
      validHash(session.token_hash) &&
      identity?.cookieEligible &&
      identity.originalEpoch &&
      session.auth_version === identity.originalEpoch &&
      validTime(session.created_at) &&
      validTime(session.expires_at) &&
      session.expires_at > session.created_at &&
      session.expires_at > BigInt(effectiveAt) &&
      !affected &&
      session.owner_binding === binding
    ) {
      expected.member_sessions.push({
        token_hash: session.token_hash,
        member_id: session.user_id,
        auth_epoch: session.auth_version,
        owner_binding: session.owner_binding,
        created_at: Number(session.created_at),
        expires_at: Number(session.expires_at),
      });
    }
  }
  for (const row of source.prepare('SELECT id,token,data FROM accounts ORDER BY id').iterate()) {
    const account = JSON.parse(row.data);
    same(
      target.prepare('SELECT credential_epoch FROM upstream_accounts WHERE id=?').get(row.id)
        ?.credential_epoch,
      1,
      'private_account_epoch',
    );
    if (account.token) {
      same(row.token, account.token, 'private_upstream_source');
      let proxyUrl;
      try {
        proxyUrl = legacyProxy(account);
      } catch {
        fail('private_upstream_proxy');
      }
      expected.upstream_credentials.push({
        account_id: row.id,
        pst: account.token,
        proxy_url: proxyUrl,
      });
    }
  }
  for (const [table, primary] of relationalPrivateTables) {
    const rows = expected[table];
    same(
      target.prepare(`SELECT * FROM ${table} ORDER BY ${primary}`).all(),
      rows,
      'private_' + table,
    );
  }
}

// Pure comparisons in the caller's existing source snapshot / target savepoint. No IO, new mapper,
// price/normalization rules or persisted coverage state. The original owners supplied these plans.
export function assertLegacyDatabaseProjection(source, target, proof) {
  requireValue(
    source.readonly &&
      source.inTransaction &&
      source.pragma('query_only', { simple: true }) === 1 &&
      target.inTransaction,
    'transaction',
  );
  const {
    references: refs,
    prepared,
    artifacts,
    descriptors,
    keyDecisions,
    archive,
    privateResult,
  } = proof;
  requireValue(
    refs && prepared && artifacts && Array.isArray(descriptors) && Array.isArray(keyDecisions),
    'proof',
  );
  requireValue(
    typeof proof.readOriginalBaselines === 'function' &&
      typeof proof.readOriginalDiagnostic === 'function' &&
      Array.isArray(proof.freeUsage) &&
      artifacts.metadata,
    'source_owners',
  );
  // Appending can mutate a draft. Re-read the small baseline owners and rebuild artifact decisions
  // from their separate original metadata, never from the same object the writer consumed.
  const originalBaselines = proof.readOriginalBaselines(),
    originalArtifacts = mapLegacyArtifacts(source, refs, artifacts.metadata),
    freeUsage = new Map(proof.freeUsage.map((row) => [row.jobId, row]));
  const facts = new Map(),
    expectedContexts = new Set();
  for (const row of target
    .prepare('SELECT event_id,event_type,payload FROM domain_events')
    .iterate()) {
    const payload = JSON.parse(row.payload);
    if (
      payload.origin?.sourceId === refs.sourceId ||
      (row.event_type === 'ImportSourceRegistered' && payload.sourceId === refs.sourceId)
    ) {
      requireValue(!facts.has(row.event_id), 'event_identity');
      facts.set(row.event_id, { eventType: row.event_type, payload });
    }
  }
  const opening = (type, originalId) => {
    const row = facts.get(refs.eventId(type, originalId));
    requireValue(row?.eventType === type, type);
    same(row.payload.origin, refs.origin(originalId), type + '_origin');
    return row.payload;
  };
  const imported = facts.get(refs.eventId('ImportSourceRegistered', refs.sourceId));
  requireValue(
    imported?.eventType === 'ImportSourceRegistered' &&
      imported.payload.sourceSha256 === refs.sourceSha256 &&
      imported.payload.route === 'B',
    'source',
  );
  requireValue(
    archive?.binding?.format === 'legacy-export' &&
      archive.binding.sourceSha256 === refs.sourceSha256 &&
      archive.binding.finalCommitSeq === imported.payload.cutoffSeq &&
      archive.audit?.sourceSha256 === refs.sourceSha256 &&
      archive.audit.sourceCommit === imported.payload.sourceCommit &&
      archive.audit.lastCommitSeq === imported.payload.cutoffSeq &&
      archive.binding.manifestSha256 === archive.auditSha256 &&
      Array.isArray(archive.inputs),
    'archive_binding',
  );

  const decision = (fact, category) => {
    const stored = facts.get(fact.eventId);
    requireValue(stored?.eventType === fact.eventType, category);
    same(stored.payload, fact.payload, category);
  };
  const context = (id, original) => {
    expectedContexts.add(id);
    const row = target
      .prepare('SELECT schema_version,sha256,body FROM normalization_contexts WHERE id=?')
      .get(id);
    requireValue(
      row &&
        row.schema_version === 1 &&
        row.sha256 === domainDigest(original) &&
        row.body === canonicalJson(original),
      'original_context',
    );
  };
  const projection = (table, keys, fields, payload, eventId) => {
    const row = target
      .prepare(
        `SELECT * FROM ${table} WHERE ${Object.keys(keys)
          .map((k) => k + '=?')
          .join(' AND ')}`,
      )
      .get(...Object.values(keys));
    requireValue(row, table);
    for (const [column, field] of Object.entries(fields)) {
      same(
        row[column],
        sqlValue(typeof field === 'function' ? field(payload) : payload[field]),
        table + '_' + column,
      );
    }
    if (eventId) {
      same(row.source_event_id, eventId, table + '_source');
    }
  };
  const fieldMap = (fields) => Object.fromEntries(fields.map(([column, field]) => [column, field]));
  const checkOwnerFacts = (mapping, table, type, primary, columns) => {
    const sourceTable = { members: 'users', upstream_accounts: 'accounts' }[table];
    if (sourceTable) {
      sets(
        mapping.facts.map((f) => f.payload[primary]),
        source
          .prepare(`SELECT id FROM ${sourceTable}`)
          .all()
          .map((r) => r.id),
        table + '_coverage',
      );
      sets(
        target
          .prepare(`SELECT id FROM ${table}`)
          .all()
          .map((r) => r.id),
        mapping.facts.map((f) => f.payload[primary]),
        table + '_projection_coverage',
      );
    }
    for (const fact of mapping.facts) {
      if (fact.eventType !== type) {
        continue;
      }
      decision(fact, type + '_decision');
      projection(table, { id: fact.payload[primary] }, columns, fact.payload, fact.eventId);
    }
  };
  checkOwnerFacts(originalBaselines.members, 'members', 'OpeningMemberRecorded', 'memberId', {
    display_name: 'displayName',
    status: 'status',
    accepting_jobs: 'acceptingJobs',
    admission_epoch: 'admissionEpoch',
    created_at: 'createdAt',
  });
  checkOwnerFacts(
    originalBaselines.accounts,
    'upstream_accounts',
    'OpeningAccountRecorded',
    'accountId',
    {
      display_name_json: (p) => JSON.stringify(p.displayName),
      weight: 'weight',
      enabled: 'enabled',
      credential_epoch: 'credentialEpoch',
      cooldown_until: 'cooldownUntil',
      cooldown_reason: 'cooldownReason',
      ordinary_strikes: 'ordinaryStrikes',
      capacity_strikes: 'capacityStrikes',
      capacity_limited_until: 'capacityLimitedUntil',
      capacity_last_error_at: 'capacityLastErrorAt',
    },
  );
  const sharing = originalBaselines.sharing.facts;
  const originalSharing = readProjectedFairQuota(source);
  sets(
    sharing.filter((f) => f.eventType === 'OpeningGroupRecorded').map((f) => f.payload.groupId),
    originalSharing?.groups.map((g) => g.id) ?? [],
    'source_group_coverage',
  );
  sets(
    sharing
      .filter((f) => f.eventType === 'OpeningGroupMemberRecorded')
      .map((f) => f.payload.memberId),
    originalSharing?.members ?? [],
    'source_membership_coverage',
  );
  requireValue(
    sharing.filter((f) => f.eventType === 'OpeningPolicyRecorded').length ===
      (originalSharing ? 1 : 0),
    'source_policy_coverage',
  );
  for (const fact of sharing.filter((f) => f.eventType === 'OpeningGroupMemberRecorded')) {
    same(
      fact.payload.memberOrdinal,
      originalSharing.members.indexOf(fact.payload.memberId),
      'source_member_order',
    );
  }

  sets(
    target
      .prepare('SELECT id FROM sharing_groups')
      .all()
      .map((r) => r.id),
    sharing.filter((f) => f.eventType === 'OpeningGroupRecorded').map((f) => f.payload.groupId),
    'group_coverage',
  );
  sets(
    target
      .prepare('SELECT member_id FROM group_members')
      .all()
      .map((r) => r.member_id),
    sharing
      .filter((f) => f.eventType === 'OpeningGroupMemberRecorded')
      .map((f) => f.payload.memberId),
    'membership_coverage',
  );
  for (const fact of sharing) {
    const p = fact.payload;
    decision(fact, 'sharing_decision');
    if (fact.eventType === 'OpeningGroupRecorded') {
      projection(
        'sharing_groups',
        { id: p.groupId },
        { name: 'name', share: 'share' },
        p,
        fact.eventId,
      );
    } else if (fact.eventType === 'OpeningGroupMemberRecorded') {
      projection(
        'group_members',
        { member_id: p.memberId },
        { group_id: 'groupId', weight: 'weight' },
        p,
        fact.eventId,
      );
    } else {
      projection(
        'sharing_policy',
        { id: 1 },
        fieldMap([
          ['enabled', 'enabled'],
          ['bound_account_id', 'boundAccountId'],
          ['config_version', 'configVersion'],
          ['auto_anlas_fallback', 'autoAnlasFallback'],
          ['free_policy_version', 'freePolicyVersion'],
          ['free_enabled', 'freeEnabled'],
          ['free_warning_threshold', 'freeWarningThreshold'],
          ['free_hard_limit', 'freeHardLimit'],
          ['loss_policy_version', 'lossPolicyVersion'],
          ['loss_v5_24h', 'lossV5_24h'],
          ['loss_fixed_24h', 'lossFixed24h'],
          ['loss_fixed_30d', 'lossFixed30d'],
          ['loss_member_attempts_24h', 'lossMemberAttempts24h'],
        ]),
        p,
        fact.eventId,
      );
    }
  }
  same(
    target.prepare('SELECT COUNT(*) AS n FROM sharing_policy').get().n,
    sharing.some((f) => f.eventType === 'OpeningPolicyRecorded') ? 1 : 0,
    'policy_coverage',
  );
  const settings = source.prepare("SELECT value FROM app_settings WHERE key='settings'").get();
  requireValue(settings, 'configuration_source');
  expectedContexts.add(
    target
      .prepare('SELECT normalization_context_id AS id FROM gateway_configuration WHERE id=1')
      .get()?.id,
  );
  const originalSettings = JSON.parse(settings.value),
    config = new RelationalConfigurationStore(target).readCurrent();
  same(
    config.normalizationDefaults,
    requestNormalizationDefaults(originalSettings),
    'configuration_defaults',
  );
  same(
    config.publicBaseUrl,
    typeof originalSettings.publicBaseUrl === 'string' ? originalSettings.publicBaseUrl : null,
    'configuration_base',
  );
  same(config.originRef, `${refs.sourceId}:app_settings:settings`, 'configuration_origin');

  const sourceInput = source.prepare('SELECT * FROM generation_requests WHERE id=?');
  const sameSourceRow = (planned, actual, category) => {
    if (!actual) {
      requireValue(planned == null, category);
      return;
    }
    requireValue(
      planned &&
        Object.keys(actual).length === Object.keys(planned).length &&
        Object.entries(actual).every(([key, value]) => planned[key] === value),
      category,
    );
  };
  const inputPlans = new Map(),
    requestIds = new Set(),
    jobIds = new Set();
  for (const plan of prepared.requests) {
    const {
      group,
      memberId,
      inputId,
      content,
      proof: definition,
      operation,
      model,
      count,
      originalRow,
      protectedRow,
    } = plan;
    const firstSource = source.prepare('SELECT data FROM jobs WHERE id=?').get(plan.jobs[0].row.id);
    requireValue(firstSource, 'request_original_job');
    const firstJob = JSON.parse(firstSource.data),
      actualOriginalRow = group.originalRequestId ? sourceInput.get(group.originalRequestId) : null,
      actualProtectedRow =
        actualOriginalRow ??
        sourceInput.get(firstJob.legacyInputRef || `legacy-input:${plan.jobs[0].row.id}`);
    sameSourceRow(originalRow, actualOriginalRow, 'request_original_row');
    sameSourceRow(protectedRow, actualProtectedRow, 'request_protected_source');
    let sourceOriginal = firstJob.request;
    if (actualProtectedRow) {
      // Only the required source scalars/confirmation are extracted; large original bodies remain
      // in the already parsed input owner plan and the exact bound archive.
      const path =
        source
          .prepare(
            "SELECT json_type(payload,'$.legacyInline') AS legacy FROM generation_requests WHERE id=?",
          )
          .get(actualProtectedRow.id).legacy === 'true'
          ? '$.request'
          : '$';
      const values = source
        .prepare(
          `SELECT payload -> '${path}.model' AS model,payload -> '${path}.sampleCount' AS sampleCount,payload -> '${path}.seed' AS seed,payload -> '${path}.operation' AS operation,payload -> '${path}.confirmation' AS confirmation FROM generation_requests WHERE id=?`,
        )
        .get(actualProtectedRow.id);
      sourceOriginal = Object.fromEntries(
        Object.entries(values).map(([key, value]) => [
          key,
          value === null ? null : JSON.parse(value),
        ]),
      );
    }
    requireValue(!requestIds.has(group.requestId), 'request_identity');
    requestIds.add(group.requestId);
    // Request fact IDs use the internal FK anchor, while origin retains the real parent/job role.
    const requestFact = facts.get(refs.eventId('OpeningRequestRecorded', group.requestId));
    requireValue(requestFact?.eventType === 'OpeningRequestRecorded', 'request');
    same(
      requestFact.payload.origin,
      refs.origin(group.originalRequestId || plan.jobs[0].row.id),
      'request_origin',
    );
    const r = requestFact.payload;
    if (definition?.prepared) {
      same(r.contextId, definition.prepared.contextId, 'request_context');
      context(r.contextId, definition.context);
    } else {
      same(r.contextId, null, 'request_unknown_context');
    }
    same(
      [
        r.originalRequestId,
        r.memberId,
        r.inputId,
        r.operation,
        r.model,
        r.sampleCount,
        r.seedMode,
        r.acceptedAt,
      ],
      [
        group.originalRequestId,
        memberId,
        inputId,
        operation,
        model,
        count,
        definition?.seedMode ?? 'unknown',
        originalRow ? time(originalRow.created_at) : null,
      ],
      'request_source',
    );
    same(
      r.source,
      originalRow?.source ??
        (typeof plan.jobs[0].value.source === 'string' ? plan.jobs[0].value.source : null),
      'request_kind',
    );
    same(r.confirmation, sourceOriginal?.confirmation ?? null, 'request_confirmation');
    same(
      r.model,
      typeof sourceOriginal?.model === 'string' ? sourceOriginal.model : null,
      'request_source_model',
    );
    same(
      r.sampleCount,
      Number.isSafeInteger(sourceOriginal?.sampleCount) && sourceOriginal.sampleCount > 0
        ? sourceOriginal.sampleCount
        : null,
      'request_source_count',
    );
    projection(
      'generation_requests',
      { id: group.requestId },
      {
        member_id: 'memberId',
        input_id: 'inputId',
        context_id: 'contextId',
        source: 'source',
        operation: 'operation',
        model: 'model',
        sample_count: 'sampleCount',
        seed_mode: 'seedMode',
        confirmation: 'confirmation',
        accepted_at: 'acceptedAt',
        retired_at: 'retiredAt',
      },
      r,
      refs.eventId('OpeningRequestRecorded', group.requestId),
    );
    inputPlans.set(inputId, {
      inputId,
      memberId,
      ...content,
      origin: refs.origin(protectedRow?.id || plan.jobs[0].row.id),
    });
    for (const planned of plan.jobs) {
      const row = source
        .prepare('SELECT id,request_id,order_value,data FROM jobs WHERE id=?')
        .get(planned.row.id);
      requireValue(row && !jobIds.has(row.id), 'job_source');
      jobIds.add(row.id);
      same(row, planned.row, 'job_plan');
      const job = JSON.parse(row.data);
      same(job, planned.value, 'job_original');
      same(nullable(row.request_id), group.originalRequestId, 'original_parent');
      const owners = [
        job.memberId,
        job.userId,
        job.generationAttempt?.memberId,
        job.fairCharge?.userId,
        protectedRow?.member_id,
      ].filter((value) => typeof value === 'string' && value);
      requireValue(owners.length && owners.every((id) => id === memberId), 'job_owner');
      const q = opening('OpeningJobRecorded', row.id);
      const diagnostic = proof.readOriginalDiagnostic(source, job, group.originalRequestId);
      same(q.diagnosticId, diagnostic?.diagnosticId ?? null, 'job_diagnostic');
      if (diagnostic) {
        const saved = target
          .prepare('SELECT job_id,sha256,created_at,detail FROM generation_diagnostics WHERE id=?')
          .get(diagnostic.diagnosticId);
        same(
          saved,
          {
            job_id: diagnostic.jobId,
            sha256: diagnostic.sha256,
            created_at: diagnostic.createdAt,
            detail: diagnostic.detail,
          },
          'diagnostic_source',
        );
        const recorded = opening('OpeningDiagnosticRecorded', diagnostic.diagnosticId);
        same(
          [recorded.jobId, recorded.sha256, recorded.createdAt, recorded.byteLength],
          [
            diagnostic.jobId,
            diagnostic.sha256,
            diagnostic.createdAt,
            Buffer.byteLength(diagnostic.detail),
          ],
          'diagnostic_original_fields',
        );
      }
      let seed = sourceOriginal?.seed;
      if (group.originalRequestId) {
        seed = null;
      }
      if (typeof job.request?.seed === 'number') {
        seed = job.request.seed;
      }
      let expectedSeed = typeof seed === 'number' ? seed : null;
      if (operation !== null && operation !== 'generate') {
        expectedSeed = null;
      }
      same(q.actualSeed, expectedSeed, 'job_original_seed');
      same(
        q.artifactId,
        typeof (job.imageId || job.lateResultImageId) === 'string' &&
          (job.imageId || job.lateResultImageId)
          ? job.imageId || job.lateResultImageId
          : null,
        'job_original_artifact',
      );

      same(
        [
          q.requestId,
          q.status,
          q.createdAt,
          q.sourceOrder,
          q.displayCost,
          q.sampleIndex,
          q.completedAt,
          q.admissionEpoch,
          q.queueSeq,
          q.queueExpiresAt,
          q.deadlineAt,
          q.cacheKey,
          q.errorCode,
          q.errorStatus,
          q.cancellationReason,
          q.lateResultAt,
          q.retiredAt,
        ],
        [
          group.requestId,
          job.status,
          time(job.createdAt),
          row.order_value,
          typeof job.cost === 'number' && Number.isFinite(job.cost) ? job.cost : null,
          Number.isSafeInteger(job.sampleIndex) && job.sampleIndex >= 0 ? job.sampleIndex : null,
          time(job.completedAt),
          Number.isSafeInteger(job.jobAdmissionVersion) && job.jobAdmissionVersion >= 0
            ? job.jobAdmissionVersion
            : null,
          Number.isSafeInteger(job.queueSeq) && job.queueSeq > 0 ? String(job.queueSeq) : null,
          time(job.queueExpiresAt),
          time(job.deadlineAt),
          typeof job.cacheKey === 'string' && job.cacheKey ? job.cacheKey : null,
          typeof job.errorCode === 'string' && job.errorCode ? job.errorCode : null,
          Number.isSafeInteger(job.errorStatus) && job.errorStatus > 0 ? job.errorStatus : null,
          typeof job.cancellationReason === 'string' && job.cancellationReason
            ? job.cancellationReason
            : null,
          time(job.lateResultAt),
          null,
        ],
        'job_source_fields',
      );
      const success = freeUsage.get(row.id);
      if (success) {
        same(success.memberId, memberId, 'free_success_owner');
      }
      same(
        [q.freeSucceededAt, q.freeWarningThreshold],
        [success?.succeededAt ?? null, success?.warningThreshold ?? null],
        'job_free_success',
      );
      projection(
        'generation_jobs',
        { id: row.id },
        {
          request_id: 'requestId',
          sample_index: 'sampleIndex',
          actual_seed: 'actualSeed',
          status: 'status',
          queue_seq: (p) => (p.queueSeq === null ? null : Number(p.queueSeq)),
          queue_expires_at: 'queueExpiresAt',
          deadline_at: 'deadlineAt',
          admission_epoch: 'admissionEpoch',
          artifact_id: 'artifactId',
          cache_key: 'cacheKey',
          completed_at: 'completedAt',
          error_code: 'errorCode',
          error_status: 'errorStatus',
          cancellation_reason: 'cancellationReason',
          late_result_at: 'lateResultAt',
          free_succeeded_at: 'freeSucceededAt',
          free_warning_threshold: 'freeWarningThreshold',
          retired_at: 'retiredAt',
        },
        q,
        refs.eventId('OpeningJobRecorded', row.id),
      );
      if (['pending', 'queued', 'running'].includes(job.status)) {
        requireValue(
          content.completeness === 'complete' &&
            definition?.prepared &&
            r.contextId === definition.prepared.contextId,
          'active_input',
        );
      }
    }
  }
  sets(
    jobIds,
    source
      .prepare('SELECT id FROM jobs')
      .all()
      .map((r) => r.id),
    'job_coverage',
  );
  sets(
    target
      .prepare('SELECT id FROM generation_jobs')
      .all()
      .map((r) => r.id),
    jobIds,
    'job_projection_coverage',
  );
  sets(
    target
      .prepare('SELECT id FROM generation_requests')
      .all()
      .map((r) => r.id),
    requestIds,
    'request_coverage',
  );
  for (const input of artifacts.inputs) {
    inputPlans.set(input.inputId, {
      ...input,
      ...input.content,
      origin: refs.origin(input.originalId),
    });
  }
  sets(
    target
      .prepare('SELECT id FROM generation_inputs')
      .all()
      .map((r) => r.id),
    inputPlans.keys(),
    'input_coverage',
  );
  for (const input of inputPlans.values()) {
    const row = target.prepare('SELECT * FROM generation_inputs WHERE id=?').get(input.inputId),
      assets = input.assets.map(({ bytes, ...metadata }) => metadata);
    requireValue(row, 'input');
    same(
      [row.member_id, row.completeness, row.logical_bytes, row.body, row.sha256],
      [
        input.memberId,
        input.completeness,
        input.logicalBytes,
        input.body === null ? null : canonicalJson(input.body),
        input.body === null ? null : relationalInputDigest(input.body, assets, input.logicalBytes),
      ],
      'input_content',
    );
    const f = facts.get(refs.eventId('OpeningInputRecorded', input.inputId));
    requireValue(f?.eventType === 'OpeningInputRecorded', 'input_origin');
    same(f.payload.origin, input.origin, 'input_origin');
    const saved = target
        .prepare(
          'SELECT asset_id AS assetId,mime_type AS mimeType,byte_length AS byteLength,sha256,bytes FROM input_assets WHERE input_id=? ORDER BY asset_id',
        )
        .all(input.inputId),
      expected = [...input.assets].sort((a, b) =>
        a.assetId < b.assetId ? -1 : Number(a.assetId > b.assetId),
      );
    requireValue(saved.length === expected.length, 'asset_coverage');
    for (let i = 0; i < saved.length; i++) {
      same({ ...saved[i], bytes: null }, { ...expected[i], bytes: null }, 'asset_metadata');
      requireValue(saved[i].bytes.equals(expected[i].bytes), 'asset_bytes');
    }
  }

  const artifactFacts = originalArtifacts.facts.filter(
    (f) => f.eventType === 'OpeningArtifactRecorded',
  );
  sets(
    descriptors.map((d) => d.id),
    source
      .prepare('SELECT id FROM images')
      .all()
      .map((r) => r.id),
    'artifact_source_coverage',
  );
  sets(
    artifactFacts.map((f) => f.payload.artifactId),
    descriptors.map((d) => d.id),
    'artifact_coverage',
  );
  sets(
    target
      .prepare('SELECT id FROM artifacts')
      .all()
      .map((r) => r.id),
    descriptors.map((d) => d.id),
    'artifact_projection_coverage',
  );
  const targetsById = new Map(
    originalArtifacts.facts
      .filter((f) => f.eventType === 'OpeningStorageTargetRecorded')
      .map((f) => [f.payload.storageTargetId, f.payload]),
  );
  for (const descriptor of descriptors) {
    const expectedKinds = new Set([
      ...(descriptor.file !== null ? ['local'] : []),
      ...(descriptor.r2 !== null ? ['r2'] : []),
      ...(descriptor.pikpak !== null ? ['pikpak'] : []),
      ...(proof.localMaterializations?.has(descriptor.id) ? ['local'] : []),
    ]);
    sets(
      originalArtifacts.facts
        .filter(
          (f) => f.eventType === 'OpeningReplicaRecorded' && f.payload.artifactId === descriptor.id,
        )
        .map((f) => targetsById.get(f.payload.storageTargetId)?.kind),
      expectedKinds,
      'source_replica_promises',
    );
    const artifact = artifactFacts.find((f) => f.payload.artifactId === descriptor.id).payload;
    same(
      [artifact.mimeType, artifact.byteLength, artifact.sha256],
      [descriptor.mimeType, descriptor.byteLength, descriptor.sha256],
      'source_artifact_descriptor',
    );
    if (descriptor.memberId !== null) {
      same(artifact.memberId, descriptor.memberId, 'source_artifact_owner');
    }
    if (descriptor.operation !== null) {
      same(artifact.operation, descriptor.operation, 'source_artifact_operation');
    }
    const original = readStreamProjection(source, `artifact:${descriptor.id}`)?.projection;
    same(artifact.retiredAt, time(original?.retired?.at), 'source_artifact_retirement');
  }
  for (const fact of originalArtifacts.facts) {
    const p = fact.payload;
    decision(fact, 'artifact_decision');
    if (fact.eventType === 'OpeningArtifactRecorded') {
      projection(
        'artifacts',
        { id: p.artifactId },
        {
          member_id: 'memberId',
          input_id: 'inputId',
          original_attempt_id: 'originalAttemptId',
          operation: 'operation',
          mime_type: 'mimeType',
          byte_length: 'byteLength',
          sha256: 'sha256',
          published_at: 'publishedAt',
          retired_at: 'retiredAt',
        },
        p,
        fact.eventId,
      );
    } else if (fact.eventType === 'OpeningStorageTargetRecorded') {
      projection(
        'storage_targets',
        { id: p.storageTargetId },
        { kind: 'kind', namespace: 'namespace', credential_ref: 'credentialRef' },
        p,
        fact.eventId,
      );
    } else {
      projection(
        'artifact_replicas',
        { artifact_id: p.artifactId, storage_target_id: p.storageTargetId },
        {
          object_key: 'objectKey',
          encoding: 'encoding',
          uploaded_at: 'uploadedAt',
          verified_at: 'verifiedAt',
          deleted_at: 'deletedAt',
          last_error_code: 'lastErrorCode',
        },
        p,
        fact.eventId,
      );
    }
  }
  same(
    target.prepare('SELECT COUNT(*) AS n FROM artifact_replicas').get().n,
    originalArtifacts.facts.filter((f) => f.eventType === 'OpeningReplicaRecorded').length,
    'replica_coverage',
  );
  same(
    target.prepare('SELECT COUNT(*) AS n FROM storage_targets').get().n,
    originalArtifacts.facts.filter((f) => f.eventType === 'OpeningStorageTargetRecorded').length,
    'target_coverage',
  );

  const originalRegistration =
    source.prepare(`SELECT e.event_id,e.event_type,e.actor_id,e.actor_kind,e.payload,c.command_id,c.receipt
    FROM domain_events e JOIN event_commits c USING(commit_seq)
    WHERE e.event_type IN ('RequestAccepted','RequestAliasRegistered') AND e.actor_kind='member' AND e.actor_id=?
    AND json_extract(e.payload,'$.idempotency.operation')=? AND json_extract(e.payload,'$.idempotency.keyHash')=?
    ORDER BY e.commit_seq,e.event_index LIMIT 2`);
  const decisions = new Map(keyDecisions.map((d) => [d.originalId, d]));
  requireValue(decisions.size === keyDecisions.length, 'key_decision_identity');
  const keyIds = [];
  const contextMappings = new Map();
  for (const row of source.prepare('SELECT * FROM generation_idempotency').iterate()) {
    const id = canonicalJson([row.member_id, row.operation, row.key_hash]),
      decision = decisions.get(id),
      p = opening('OpeningIdempotencyRecorded', id);
    const registrations = originalRegistration.all(row.member_id, row.operation, row.key_hash),
      originalRegistrationRow = registrations.length === 1 ? registrations[0] : null;
    const registered = originalRegistrationRow ? JSON.parse(originalRegistrationRow.payload) : null,
      alias = originalRegistrationRow?.event_type === 'RequestAliasRegistered',
      originalKey = registered?.idempotency,
      commandProved = Boolean(
        originalKey &&
        originalKey.fingerprint === row.fingerprint &&
        (alias ? registered.memberId : originalKey.memberId) === row.member_id &&
        (alias ? registered.jobId : originalKey.jobId) === row.job_id &&
        nullable(alias ? registered.requestId : originalKey.requestId) === row.request_id,
      );
    same(decision?.originalCommandProved, commandProved, 'key_command_proof');
    const originalState = readStreamProjection(
      source,
      `request:${row.request_id || `legacy-${row.job_id}`}`,
    )?.projection;
    same(
      decision?.sourceRetired,
      Boolean(originalState?.retiredJobs?.[row.job_id]),
      'key_retirement_source',
    );
    if (commandProved) {
      same(
        [p.originalCommandId, p.originRef],
        [originalRegistrationRow.command_id, originalRegistrationRow.event_id],
        'key_command_origin',
      );
      if (p.resolution === 'live') {
        const receipt = JSON.parse(originalRegistrationRow.receipt);
        same(
          p.archivedReceipt,
          {
            requestId: receipt.requestId,
            jobIds: receipt.jobIds,
            ...(receipt.cacheHit === true ? { cacheHit: true } : {}),
          },
          'key_original_receipt',
        );
      }
    }
    let definition;
    for (const plan of prepared.requests) {
      definition ??= plan.proofs.find(
        (entry) =>
          entry.mapping.member_id === row.member_id &&
          entry.mapping.operation === row.operation &&
          entry.mapping.key_hash === row.key_hash,
      )?.proof;
    }
    if (!definition) {
      requireValue(typeof proof.proveGenerationIntent === 'function', 'key_definition_owner');
      const state = readStreamProjection(
          source,
          `request:${row.request_id || `legacy-${row.job_id}`}`,
        )?.projection,
        inputRef = row.request_id || state?.request?.inputRef,
        originalInput = inputRef
          ? source
              .prepare('SELECT member_id,payload FROM generation_requests WHERE id=?')
              .get(inputRef)
          : null;
      if (originalInput?.member_id === row.member_id) {
        const saved = JSON.parse(originalInput.payload);
        definition = proof.proveGenerationIntent(
          saved?.legacyInline === true ? saved.request : saved,
          JSON.parse(row.normalization_context),
          row,
        );
      }
    }
    same(Boolean(definition?.prepared), decision?.firstDefinitionProved, 'key_first_definition');
    if (definition?.prepared) {
      same(
        [p.contextId, p.noCache, p.seedMode],
        [definition.prepared.contextId, definition.noCache, definition.seedMode],
        'key_definition_values',
      );
      context(p.contextId, definition.context);
      const originalContextHash = domainDigest(JSON.parse(row.normalization_context));
      if (!contextMappings.has(originalContextHash)) {
        contextMappings.set(originalContextHash, new Set());
      }
      contextMappings.get(originalContextHash).add(p.contextId);
    }
    keyIds.push(id);
    requireValue(decision, 'key_decision');
    same(
      [
        p.originalMemberId,
        p.operation,
        p.keyHash,
        p.intentFingerprint,
        p.originalRequestId,
        p.originalJobId,
        p.createdAt,
        p.resolution,
      ],
      [
        row.member_id,
        row.operation,
        row.key_hash,
        row.fingerprint,
        row.request_id,
        row.job_id,
        time(row.created_at),
        decision.resolution,
      ],
      'key_source',
    );
    requireValue(
      ['live', 'archived', 'unavailable'].includes(decision.resolution),
      'key_resolution',
    );
    if (!decision.firstDefinitionProved) {
      requireValue(
        p.contextId === null &&
          p.noCache === null &&
          p.seedMode === null &&
          p.resolution !== 'live',
        'key_unknown_definition',
      );
    }
    if (!decision.originalCommandProved) {
      requireValue(
        p.originalCommandId === null && p.resolution === 'unavailable',
        'key_unknown_command',
      );
    }
    if (decision.sourceRetired) {
      requireValue(p.resolution === 'unavailable', 'key_retired');
    }
    projection(
      'idempotency_keys',
      { original_member_id: row.member_id, operation: row.operation, key_hash: row.key_hash },
      {
        intent_fingerprint: 'intentFingerprint',
        original_request_id: 'originalRequestId',
        original_job_id: 'originalJobId',
        original_command_id: 'originalCommandId',
        request_id: 'requestId',
        commit_seq: 'commitSeq',
        context_id: 'contextId',
        resolution: 'resolution',
        origin_ref: 'originRef',
        archived_receipt: 'archivedReceipt',
        created_at: 'createdAt',
      },
      p,
      refs.eventId('OpeningIdempotencyRecorded', id),
    );
  }
  sets(decisions.keys(), keyIds, 'key_decision_coverage');
  same(
    target.prepare('SELECT COUNT(*) AS n FROM idempotency_keys').get().n,
    keyIds.length,
    'key_coverage',
  );

  sets(
    target
      .prepare('SELECT id FROM normalization_contexts')
      .all()
      .map((row) => row.id),
    expectedContexts,
    'context_coverage',
  );

  // Every original protected row retains an exact audited archive pointer, including thin or orphaned
  // history. This is not permission to omit an active input, nor a claim that partial online == full.
  const rows = new Map(),
    retained = [];
  for (const row of archive.inputs) {
    const id = row.kind + ':' + (row.ref ?? row.id);
    requireValue(!rows.has(id), 'archive_duplicate');
    rows.set(id, row);
  }
  const retain = (kind, id, hash, memberId, onlineIds) => {
    const row = rows.get(kind + ':' + id);
    requireValue(row && row.sha256 === hash, 'archive_input');
    if (memberId !== undefined) {
      same(
        Object.hasOwn(row, 'member_id') ? row.member_id : row.memberId,
        memberId,
        'archive_owner',
      );
    }
    rows.delete(kind + ':' + id);
    retained.push({ kind, originalId: id, sha256: hash, onlineIds });
  };
  for (const row of source
    .prepare('SELECT id,member_id,payload FROM generation_requests')
    .iterate()) {
    retain(
      'request',
      row.id,
      sha(row.payload),
      row.member_id,
      [...inputPlans.values()].filter((p) => p.origin.originalId === row.id).map((p) => p.inputId),
    );
  }
  for (const row of source
    .prepare('SELECT context_ref,payload FROM generation_contexts')
    .iterate()) {
    retain('context', row.context_ref, domainDigest(JSON.parse(row.payload)), undefined, [
      ...(contextMappings.get(domainDigest(JSON.parse(row.payload))) ?? []),
    ]);
  }
  for (const row of source
    .prepare('SELECT artifact_id,member_id,payload_sha256 FROM artifact_inputs')
    .iterate()) {
    retain(
      'artifact-input',
      row.artifact_id,
      row.payload_sha256,
      row.member_id,
      refs.inputsByArtifact.has(row.artifact_id)
        ? [refs.inputsByArtifact.get(row.artifact_id).inputId]
        : [],
    );
  }
  const originalDiagnostics = new Set();
  for (const row of source
    .prepare('SELECT diagnostic_id,job_id,detail_sha256 FROM generation_diagnostics')
    .iterate()) {
    originalDiagnostics.add(row.diagnostic_id);
    retain(
      'diagnostic',
      row.diagnostic_id,
      row.detail_sha256,
      undefined,
      target.prepare('SELECT 1 FROM generation_diagnostics WHERE id=?').get(row.diagnostic_id)
        ? [row.diagnostic_id]
        : [],
    );
    const online = target
      .prepare('SELECT job_id,sha256,detail FROM generation_diagnostics WHERE id=?')
      .get(row.diagnostic_id);
    if (online) {
      requireValue(
        online.job_id === row.job_id &&
          online.sha256 === row.detail_sha256 &&
          sha(online.detail) === row.detail_sha256,
        'diagnostic_content',
      );
    }
  }
  requireValue(
    target
      .prepare('SELECT id FROM generation_diagnostics')
      .all()
      .every((row) => originalDiagnostics.has(row.id)),
    'diagnostic_source_coverage',
  );
  requireValue(
    rows.size === 0 && retained.length === archive.audit.exports.inputs.rowCount,
    'archive_input_coverage',
  );
  if (proof.cutConfiguration) {
    assertOriginalPrivateState(source, target, proof.cutConfiguration, proof.effectiveAt);
    for (const [count, table] of Object.entries({
      credentials: 'password_credentials',
      apiKeys: 'api_keys',
      discordBindings: 'discord_bindings',
      invites: 'discord_invites',
      sessions: 'member_sessions',
      upstreamCredentials: 'upstream_credentials',
    })) {
      same(
        target.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n,
        privateResult?.[count],
        'private_' + count,
      );
    }
  }
  const imageIds = new Set(descriptors.map((d) => d.id));
  for (const row of source
    .prepare("SELECT stream_id FROM event_streams WHERE stream_kind='artifact'")
    .iterate()) {
    const original = readStreamProjection(source, row.stream_id)?.projection;
    if (imageIds.has(original?.id)) {
      continue;
    }
    requireValue(
      original &&
        row.stream_id === `artifact:${original.id}` &&
        original.retired &&
        time(original.retired.at) !== null,
      'unmapped_artifact_promise',
    );
    retained.push({
      kind: 'artifact-history',
      originalId: original.id,
      retiredAt: time(original.retired.at),
      onlineIds: [],
    });
  }
  return {
    status: 'complete',
    sourceSha256: refs.sourceSha256,
    auditSha256: archive.auditSha256,
    retained,
  };
}
