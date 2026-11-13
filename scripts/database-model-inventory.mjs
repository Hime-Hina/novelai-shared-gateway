import { createHash } from 'node:crypto';
import { openSync, readSync, closeSync, existsSync, statSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';
import {
  migrateStorage,
  readCompatibleEventAuthority,
} from '../server/storage/sqlite-migrations.ts';
import { assertReplaySourceReady, readProjectedFairQuota } from '../server/events/event-store.ts';
import { domainDigest, domainEventTypes } from '../server/events/domain-events.ts';
import { parseAmount, formatAmount } from '../server/quota/resource-amounts.ts';

const tables = {
  users: 'data',
  accounts: 'data',
  cards: 'data',
  jobs: 'data',
  images: 'data',
  ledger: 'data',
  generation_requests: 'payload',
  generation_contexts: 'payload',
  artifact_inputs: 'payload',
  generation_diagnostics: 'detail',
  generation_idempotency: 'normalization_context',
  fair_quota_settlements: 'charge',
  free_image_usage: null,
  member_sessions: null,
  event_commits: 'receipt',
  domain_events: 'payload',
  event_streams: 'projection',
};
const resources = ['v5', 'anlasFixed', 'anlasPurchased'];
const script = fileURLToPath(import.meta.url);
const sha256 = (value) => createHash('sha256').update(value).digest('hex');
const failure = () => new Error('Inventory requires an unchanged standalone current-v4 snapshot.');

function fileDigest(filename) {
  const handle = openSync(filename, 'r');
  try {
    const hash = createHash('sha256'),
      buffer = Buffer.alloc(64 * 1024);
    let bytes;
    while ((bytes = readSync(handle, buffer)) !== 0) {
      hash.update(buffer.subarray(0, bytes));
    }
    return hash.digest('hex');
  } finally {
    closeSync(handle);
  }
}

function standaloneSnapshot(filename) {
  if (['-wal', '-shm', '-journal'].some((suffix) => existsSync(filename + suffix))) {
    throw failure();
  }
  const handle = openSync(filename, 'r');
  try {
    const header = Buffer.alloc(20);
    if (
      readSync(handle, header) !== 20 ||
      header.subarray(0, 16).toString() !== 'SQLite format 3\0' ||
      header[18] !== 1 ||
      header[19] !== 1
    ) {
      throw failure();
    }
  } finally {
    closeSync(handle);
  }
}

function requireSupportedSchema(db) {
  if (db.prepare("SELECT value FROM app_meta WHERE key='database_format'").get()) {
    throw failure();
  }
  const reference = new Database(':memory:');
  try {
    migrateStorage(reference);
    for (const table of [
      ...Object.keys(tables),
      'projection_versions',
      'event_imports',
      'app_meta',
      'app_settings',
    ]) {
      if (
        JSON.stringify(db.pragma(`table_info(${table})`)) !==
        JSON.stringify(reference.pragma(`table_info(${table})`))
      ) {
        throw failure();
      }
    }
    const journal = 'SELECT hash,created_at FROM __drizzle_migrations ORDER BY created_at';
    if (
      JSON.stringify(db.prepare(journal).all()) !== JSON.stringify(reference.prepare(journal).all())
    ) {
      throw failure();
    }
  } finally {
    reference.close();
  }
  const authority = readCompatibleEventAuthority(db);
  if (authority?.version !== 4) {
    throw failure();
  }
  assertReplaySourceReady(db);
}

function inspect(db) {
  requireSupportedSchema(db);
  const count = (sql, ...args) => db.prepare(`SELECT COUNT(*) AS n ${sql}`).get(...args).n;
  const opening = db.prepare("SELECT value FROM app_meta WHERE key='event_opening'").get();
  const report = {
    format: 'legacy-v4-inventory-v1',
    scope: 'aggregate database preflight; no conversion or external-object verification',
    integrity: db.pragma('integrity_check', { simple: true }) === 'ok',
    foreignKeyViolations: [...db.prepare('PRAGMA foreign_key_check').iterate()].length,
    cut: {
      lastCommitSeq: db
        .prepare('SELECT CAST(COALESCE(MAX(commit_seq),0) AS TEXT) AS value FROM event_commits')
        .get().value,
      openingComplete: opening ? JSON.parse(opening.value).status === 'complete' : false,
      openingSha256: opening ? sha256(opening.value) : null,
    },
    tables: {},
    events: {
      commitsWithWrongEventCount: count(
        'FROM event_commits c WHERE c.event_count != (SELECT COUNT(*) FROM domain_events e WHERE e.commit_seq=c.commit_seq)',
      ),
      unknownTypes: 0,
      unsupportedSchemaVersions: count('FROM domain_events WHERE schema_version != 1'),
      invalidProjectionHashes: 0,
      byType: {},
    },
    inputs: {
      invalidJson: 0,
      wrongPayloadBytes: 0,
      invalidArtifactHashes: 0,
      invalidDiagnosticHashes: 0,
      missingReferencedInputs: 0,
      conflictingRequestHashes: 0,
      conflictingRequestOwners: 0,
    },
    ownership: {
      jobsWithoutProvenMember: 0,
      jobsWithConflictingMembers: 0,
      jobsWithCurrentTokenCandidateOnly: 0,
      artifactsWithoutProvenMember: 0,
      artifactsWithConflictingMembers: 0,
      artifactsWithCurrentTokenCandidateOnly: 0,
    },
    liabilities: {
      activeJobs: count("FROM jobs WHERE status IN ('queued','pending','running')"),
      pendingReviews: count(
        "FROM jobs WHERE json_extract(data,'$.generationReview.status')='pending'",
      ),
      preparedResults: count("FROM jobs WHERE json_type(data,'$.resultPending')='object'"),
      idempotencyWithoutJob: count(
        'FROM generation_idempotency i WHERE NOT EXISTS(SELECT 1 FROM jobs j WHERE j.id=i.job_id)',
      ),
      idempotencyWithoutRequest: count(
        'FROM generation_idempotency i WHERE i.request_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM generation_requests r WHERE r.id=i.request_id)',
      ),
      settlementsWithoutJob: count(
        'FROM fair_quota_settlements s WHERE NOT EXISTS(SELECT 1 FROM jobs j WHERE j.id=s.job_id)',
      ),
      refundsWithoutJob: count(
        "FROM fair_quota_settlements s WHERE json_extract(charge,'$.status')='refunded' AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.id=s.job_id)",
      ),
      freeUsageWithoutJob: count(
        'FROM free_image_usage u WHERE NOT EXISTS(SELECT 1 FROM jobs j WHERE j.id=u.job_id)',
      ),
    },
    d4: {
      cardRecordsRequiringDisposition: count('FROM cards'),
      officialArtifacts: count(
        "FROM images WHERE token LIKE 'official:%' OR json_extract(data,'$.authMode')='official'",
      ),
      officialVolatileJobs:
        'not recoverable from this database; inspect the stopped runtime separately',
    },
    quota: null,
    pages: {
      pageSize: db.pragma('page_size', { simple: true }),
      pageCount: db.pragma('page_count', { simple: true }),
      freePages: db.pragma('freelist_count', { simple: true }),
      dbstat: null,
    },
    unverified: [
      'A conversion feasibility',
      'B opening and private snapshot closure',
      'external object bytes and archive protection',
      'complete replay and state reconciliation',
      'production scale and cutover',
    ],
  };
  for (const [table, content] of Object.entries(tables)) {
    report.tables[table] = {
      rows: count(`FROM ${table}`),
      logicalContentBytes: content
        ? db
            .prepare(`SELECT COALESCE(SUM(length(CAST(${content} AS BLOB))),0) AS n FROM ${table}`)
            .get().n
        : null,
    };
  }
  try {
    report.pages.dbstat = db
      .prepare(
        'SELECT SUM(pgsize) AS allocatedBytes,SUM(payload) AS payloadBytes,SUM(unused) AS unusedInAllocatedPages FROM dbstat',
      )
      .get();
  } catch {
    // dbstat 是可选编译能力；不可用不能写成零空间。
  }
  const known = new Set(domainEventTypes);
  for (const row of db
    .prepare('SELECT event_type,COUNT(*) AS n FROM domain_events GROUP BY event_type')
    .iterate()) {
    if (known.has(row.event_type)) {
      report.events.byType[row.event_type] = row.n;
    } else {
      report.events.unknownTypes += row.n;
    }
  }
  const input = db.prepare('SELECT member_id,payload FROM generation_requests WHERE id=?');
  for (const row of db.prepare('SELECT payload,payload_bytes FROM generation_requests').iterate()) {
    try {
      JSON.parse(row.payload);
    } catch {
      report.inputs.invalidJson++;
    }
    if (Buffer.byteLength(row.payload) !== row.payload_bytes) {
      report.inputs.wrongPayloadBytes++;
    }
  }
  for (const row of db.prepare('SELECT payload,payload_sha256 FROM artifact_inputs').iterate()) {
    try {
      if (domainDigest(JSON.parse(row.payload)) !== row.payload_sha256) {
        report.inputs.invalidArtifactHashes++;
      }
    } catch {
      report.inputs.invalidJson++;
    }
  }
  for (const row of db
    .prepare('SELECT detail,detail_sha256 FROM generation_diagnostics')
    .iterate()) {
    if (sha256(row.detail) !== row.detail_sha256) {
      report.inputs.invalidDiagnosticHashes++;
    }
  }
  for (const row of db
    .prepare('SELECT stream_kind,projection,projection_sha256 FROM event_streams')
    .iterate()) {
    const projection = JSON.parse(row.projection);
    if (domainDigest(projection) !== row.projection_sha256) {
      report.events.invalidProjectionHashes++;
    }
    if (
      row.stream_kind === 'sharing' &&
      Object.hasOwn(projection, 'members') &&
      !Array.isArray(projection.members)
    ) {
      throw failure();
    }
    if (row.stream_kind !== 'request' || !projection.request?.inputRef) {
      continue;
    }
    const saved = input.get(projection.request.inputRef);
    if (!saved) {
      report.inputs.missingReferencedInputs++;
      continue;
    }
    if (sha256(saved.payload) !== projection.request.inputSha256) {
      report.inputs.conflictingRequestHashes++;
    }
    if (saved.member_id !== projection.request.memberId) {
      report.inputs.conflictingRequestOwners++;
    }
  }
  const byToken = db.prepare('SELECT id FROM users WHERE token=?');
  function membersOf(value, parentId) {
    const owners = new Set(
      [
        value.memberId,
        value.userId,
        value.generationAttempt?.memberId,
        value.fairCharge?.userId,
      ].filter(Boolean),
    );
    const parentRef = parentId || value.legacyInputRef || value.requestId;
    const parent = parentRef ? input.get(parentRef) : null;
    if (parent?.member_id) {
      owners.add(parent.member_id);
    }
    return owners;
  }
  for (const row of db.prepare('SELECT data,request_id FROM jobs').iterate()) {
    const value = JSON.parse(row.data),
      owners = membersOf(value, row.request_id);
    if (!owners.size) {
      report.ownership.jobsWithoutProvenMember++;
      if (byToken.all(value.userToken || value.token || '').length === 1) {
        report.ownership.jobsWithCurrentTokenCandidateOnly++;
      }
    }
    if (owners.size > 1) {
      report.ownership.jobsWithConflictingMembers++;
    }
  }
  const related = db.prepare('SELECT data,request_id FROM jobs WHERE image_id=? OR id=?');
  const artifactOwner = db.prepare('SELECT member_id FROM artifact_inputs WHERE artifact_id=?');
  for (const row of db.prepare('SELECT id,data FROM images').iterate()) {
    const value = JSON.parse(row.data),
      owners = membersOf(value);
    const saved = artifactOwner.get(value.artifactInputRef || row.id);
    if (saved?.member_id) {
      owners.add(saved.member_id);
    }
    for (const job of related.iterate(row.id, value.jobId || '')) {
      for (const id of membersOf(JSON.parse(job.data), job.request_id)) {
        owners.add(id);
      }
    }
    if (!owners.size) {
      report.ownership.artifactsWithoutProvenMember++;
      if (byToken.all(value.userToken || value.token || '').length === 1) {
        report.ownership.artifactsWithCurrentTokenCandidateOnly++;
      }
    }
    if (owners.size > 1) {
      report.ownership.artifactsWithConflictingMembers++;
    }
  }
  const state = readProjectedFairQuota(db);
  if (state) {
    if (
      !Array.isArray(state.members) ||
      (state.operating != null && !Array.isArray(state.operating.pendingExternalFunding))
    ) {
      throw failure();
    }
    const sum = (values) =>
      formatAmount(values.reduce((total, value) => total + parseAmount(value), 0n));
    const reservedAmount = (charge, resource) => {
      if (resource === 'v5') {
        return charge.resource === 'v5' ? charge.amount : '0';
      }
      return charge[resource === 'anlasFixed' ? 'fixed' : 'purchased'];
    };
    report.quota = {
      members: state.members.length,
      reservedCharges: Object.keys(state.reservations).length,
      walletAvailable: Object.fromEntries(
        resources.map((r) => [
          r,
          sum(Object.values(state.balances).map((b) => b[`${r}Available`])),
        ]),
      ),
      reserved: Object.fromEntries(
        resources.map((r) => [
          r,
          sum(Object.values(state.reservations).map((c) => reservedAmount(c, r))),
        ]),
      ),
      sharedV5Debt: formatAmount(parseAmount(state.sharedV5Debt)),
      unallocatedV5: formatAmount(parseAmount(state.unallocatedV5)),
      operatingAvailable: state.operating
        ? Object.fromEntries(
            resources.map((r) => [r, formatAmount(parseAmount(state.operating.available[r]))]),
          )
        : null,
      operatingUnfunded: state.operating
        ? Object.fromEntries(
            resources
              .slice(1)
              .map((r) => [r, formatAmount(parseAmount(state.operating.unfunded[r]))]),
          )
        : null,
      operatingUnattributedReturns: state.operating
        ? Object.fromEntries(
            resources
              .slice(1)
              .map((r) => [r, formatAmount(parseAmount(state.operating.unattributedReturns[r]))]),
          )
        : null,
      pendingExternalFundingCount: state.operating?.pendingExternalFunding.length ?? null,
    };
  }
  return report;
}

export function inventoryLegacyDatabase(source, sourceCommit) {
  if (!/^[a-f0-9]{40}$/.test(sourceCommit || '')) {
    throw new Error('source-commit must be a full lowercase Git SHA.');
  }
  const filename = path.resolve(source);
  standaloneSnapshot(filename);
  const before = fileDigest(filename),
    started = performance.now();
  const db = new Database(filename, { readonly: true, fileMustExist: true });
  try {
    db.pragma('query_only=ON');
    const report = db.transaction(() => inspect(db))();
    standaloneSnapshot(filename);
    if (fileDigest(filename) !== before) {
      throw failure();
    }
    return {
      ...report,
      source: {
        declaredCommit: sourceCommit,
        databaseSha256: before,
        databaseBytes: statSync(filename).size,
      },
      toolSha256: sha256(readFileSync(script)),
      measurement: {
        elapsedMs: performance.now() - started,
        peakRssBytes: process.resourceUsage().maxRSS * 1024,
      },
    };
  } finally {
    db.close();
  }
}

if (process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url) {
  try {
    const args = process.argv.slice(2),
      values = {};
    for (const arg of args) {
      const match = /^--(source|source-commit)=(.+)$/.exec(arg);
      if (!match || values[match[1]]) {
        throw failure();
      }
      values[match[1]] = match[2];
    }
    if (!values.source || !values['source-commit']) {
      throw failure();
    }
    console.log(
      JSON.stringify(inventoryLegacyDatabase(values.source, values['source-commit']), null, 2),
    );
  } catch {
    // 解析错误可能带入私有输入原文，CLI 只返回固定消息。
    console.error(
      'Inventory failed. Use --source=FILE --source-commit=FULL_SHA with an unchanged standalone current-v4 snapshot (no WAL/SHM/journal); no source upgrade is performed.',
    );
    process.exitCode = 1;
  }
}
