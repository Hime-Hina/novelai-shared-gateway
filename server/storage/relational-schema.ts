import { RELATIONAL_EVENT_SCHEMA_VERSION } from '../events/relational-events.ts';

// 新格式的唯一 DDL；不进入旧库 migration 链，也不创建 JSON 记录或流投影副本。
// 金额只检查规范十进制文本；计算仍由 resource-amounts 的 12 位 BigInt 核心负责。
function amount(name: string, nullable = false): string {
  return `${name} TEXT ${nullable ? '' : 'NOT NULL'} CHECK (
    instr(${name}, char(0)) = 0 AND ${name} NOT GLOB '*[^0-9.-]*'
    AND (instr(${name}, '-') = 0 OR
      (substr(${name}, 1, 1) = '-' AND instr(substr(${name}, 2), '-') = 0))
    AND ltrim(${name}, '-') GLOB '[0-9]*'
    AND ltrim(${name}, '-') NOT GLOB '0[0-9]*'
    AND ${name} != '-0'
    AND (instr(${name}, '.') = 0 OR
      (length(substr(${name}, instr(${name}, '.') + 1)) BETWEEN 1 AND 12
       AND substr(${name}, -1) BETWEEN '1' AND '9'
       AND instr(substr(${name}, instr(${name}, '.') + 1), '.') = 0))
  )`;
}

const fact = 'source_event_id TEXT NOT NULL REFERENCES domain_events(event_id)';

export const RELATIONAL_SCHEMA_SQL = `
CREATE TABLE app_meta (
  key TEXT PRIMARY KEY CHECK (key IN ('database_format', 'schema_sha256', 'event_authority')),
  value TEXT NOT NULL
) STRICT;

CREATE TABLE commits (
  commit_seq INTEGER PRIMARY KEY CHECK (commit_seq > 0),
  command_id TEXT NOT NULL UNIQUE,
  fingerprint TEXT NOT NULL,
  actor_kind TEXT NOT NULL CHECK (actor_kind IN ('system', 'member', 'admin')),
  actor_id TEXT,
  recorded_at INTEGER NOT NULL,
  event_count INTEGER NOT NULL CHECK (event_count > 0),
  receipt TEXT NOT NULL CHECK (json_valid(receipt))
) STRICT;

CREATE TABLE domain_events (
  event_id TEXT PRIMARY KEY,
  commit_seq INTEGER NOT NULL REFERENCES commits(commit_seq) DEFERRABLE INITIALLY DEFERRED,
  event_index INTEGER NOT NULL CHECK (event_index >= 0),
  stream_id TEXT NOT NULL,
  stream_kind TEXT NOT NULL,
  stream_version INTEGER NOT NULL CHECK (stream_version > 0),
  event_type TEXT NOT NULL,
  schema_version INTEGER NOT NULL CHECK (schema_version = ${RELATIONAL_EVENT_SCHEMA_VERSION}),
  occurred_at INTEGER NOT NULL,
  correlation_id TEXT,
  causation_id TEXT,
  account_id TEXT,
  member_id TEXT,
  policy_version TEXT NOT NULL,
  pricing_version TEXT NOT NULL,
  amount_version TEXT NOT NULL CHECK (amount_version = 'decimal-12-v1'),
  payload TEXT NOT NULL CHECK (json_valid(payload) AND json_type(payload) = 'object'),
  UNIQUE (commit_seq, event_index),
  UNIQUE (stream_id, stream_version)
) STRICT;

CREATE TABLE stream_heads (
  stream_id TEXT PRIMARY KEY,
  stream_kind TEXT NOT NULL,
  stream_version INTEGER NOT NULL CHECK (stream_version > 0),
  last_commit_seq INTEGER NOT NULL REFERENCES commits(commit_seq),
  last_event_id TEXT NOT NULL REFERENCES domain_events(event_id)
) STRICT;

CREATE TABLE projection_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  history_id TEXT NOT NULL,
  reducer_version INTEGER NOT NULL CHECK (reducer_version > 0),
  last_commit_seq INTEGER NOT NULL CHECK (last_commit_seq >= 0),
  status TEXT NOT NULL CHECK (status IN ('building', 'ready', 'failed'))
) STRICT;

CREATE TABLE members (
  id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('active', 'disabled', 'removed')),
  accepting_jobs INTEGER NOT NULL CHECK (accepting_jobs IN (0, 1)),
  -- 认证 epoch 只由私有快照/认证命令维护；未恢复或撤销时 NULL，业务重放不得重建它。
  auth_epoch TEXT,
  admission_epoch INTEGER NOT NULL CHECK (admission_epoch >= 0),
  created_at INTEGER NOT NULL,
  ${fact}
) STRICT;

CREATE TABLE password_credentials (
  member_id TEXT PRIMARY KEY REFERENCES members(id),
  username TEXT NOT NULL UNIQUE,
  salt TEXT NOT NULL,
  password_hash TEXT NOT NULL
) STRICT;

CREATE TABLE api_keys (
  member_id TEXT PRIMARY KEY REFERENCES members(id),
  key_hash TEXT NOT NULL UNIQUE,
  recoverable_key TEXT NOT NULL
) STRICT;

CREATE TABLE discord_bindings (
  member_id TEXT PRIMARY KEY REFERENCES members(id),
  discord_id TEXT NOT NULL UNIQUE,
  guild_id TEXT NOT NULL,
  access_token TEXT,
  refresh_token TEXT,
  expires_at INTEGER,
  checked_at INTEGER,
  eligible INTEGER CHECK (eligible IN (0, 1))
) STRICT;

CREATE TABLE member_sessions (
  token_hash TEXT PRIMARY KEY,
  member_id TEXT NOT NULL REFERENCES members(id),
  auth_epoch TEXT NOT NULL,
  owner_binding TEXT,
  created_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL CHECK (expires_at > created_at)
) STRICT;
CREATE INDEX idx_sessions_member ON member_sessions(member_id);
CREATE INDEX idx_sessions_expiry ON member_sessions(expires_at);

CREATE TABLE sharing_groups (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  share REAL NOT NULL CHECK (share >= 0),
  ${fact}
) STRICT;

CREATE TABLE group_members (
  member_id TEXT PRIMARY KEY REFERENCES members(id),
  group_id TEXT NOT NULL REFERENCES sharing_groups(id),
  weight REAL NOT NULL CHECK (weight >= 0),
  ${fact}
) STRICT;
CREATE INDEX idx_group_members_group ON group_members(group_id);

CREATE TABLE upstream_accounts (
  id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
  credential_epoch INTEGER NOT NULL CHECK (credential_epoch > 0),
  cooldown_until INTEGER,
  cooldown_reason TEXT,
  ordinary_strikes INTEGER NOT NULL CHECK (ordinary_strikes >= 0),
  capacity_strikes INTEGER NOT NULL CHECK (capacity_strikes >= 0),
  capacity_limited_until INTEGER,
  capacity_last_error_at INTEGER,
  ${fact}
) STRICT;

CREATE TABLE upstream_credentials (
  account_id TEXT PRIMARY KEY REFERENCES upstream_accounts(id),
  pst TEXT NOT NULL
) STRICT;

CREATE TABLE sharing_policy (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
  bound_account_id TEXT REFERENCES upstream_accounts(id),
  config_version INTEGER NOT NULL CHECK (config_version >= 0),
  auto_anlas_fallback INTEGER NOT NULL CHECK (auto_anlas_fallback IN (0, 1)),
  free_policy_version INTEGER NOT NULL CHECK (free_policy_version > 0),
  free_enabled INTEGER NOT NULL CHECK (free_enabled IN (0, 1)),
  free_warning_threshold INTEGER CHECK (free_warning_threshold > 0),
  free_hard_limit INTEGER CHECK (free_hard_limit > 0),
  loss_policy_version INTEGER NOT NULL CHECK (loss_policy_version > 0),
  ${amount('loss_v5_24h')},
  ${amount('loss_fixed_24h')},
  ${amount('loss_fixed_30d')},
  loss_member_attempts_24h INTEGER NOT NULL CHECK (loss_member_attempts_24h >= 0),
  ${fact}
) STRICT;

CREATE TABLE normalization_contexts (
  id TEXT PRIMARY KEY,
  schema_version INTEGER NOT NULL CHECK (schema_version = 1),
  sha256 TEXT NOT NULL CHECK (length(sha256) = 64 AND sha256 NOT GLOB '*[^a-f0-9]*'),
  body TEXT NOT NULL CHECK (json_valid(body) AND json_type(body) = 'object'),
  UNIQUE (schema_version, sha256)
) STRICT;

CREATE TABLE generation_inputs (
  id TEXT PRIMARY KEY,
  member_id TEXT NOT NULL REFERENCES members(id),
  schema_version INTEGER NOT NULL CHECK (schema_version = 1),
  completeness TEXT NOT NULL CHECK (completeness IN ('complete', 'partial', 'unavailable')),
  sha256 TEXT CHECK (length(sha256) = 64 AND sha256 NOT GLOB '*[^a-f0-9]*'),
  logical_bytes INTEGER NOT NULL CHECK (logical_bytes >= 0),
  body TEXT CHECK (json_valid(body) AND json_type(body) = 'object'),
  CHECK ((completeness = 'unavailable' AND body IS NULL AND sha256 IS NULL AND logical_bytes = 0)
    OR (completeness != 'unavailable' AND body IS NOT NULL AND sha256 IS NOT NULL)),
  UNIQUE (id, member_id)
) STRICT;

CREATE TABLE input_assets (
  input_id TEXT NOT NULL REFERENCES generation_inputs(id),
  asset_id TEXT NOT NULL,
  mime_type TEXT NOT NULL,
  byte_length INTEGER NOT NULL CHECK (byte_length > 0),
  sha256 TEXT NOT NULL CHECK (length(sha256) = 64 AND sha256 NOT GLOB '*[^a-f0-9]*'),
  bytes BLOB NOT NULL CHECK (length(bytes) = byte_length),
  PRIMARY KEY (input_id, asset_id)
) STRICT;

CREATE TABLE generation_requests (
  id TEXT PRIMARY KEY,
  member_id TEXT NOT NULL REFERENCES members(id),
  input_id TEXT NOT NULL,
  context_id TEXT REFERENCES normalization_contexts(id),
  source TEXT NOT NULL,
  operation TEXT NOT NULL CHECK (operation IN ('generate', 'upscale', 'encode-vibe')),
  sample_count INTEGER NOT NULL CHECK (sample_count > 0),
  seed_mode TEXT NOT NULL CHECK (seed_mode IN ('fixed', 'random', 'not_applicable', 'unknown')),
  confirmation TEXT CHECK (json_valid(confirmation) AND json_type(confirmation) = 'object'),
  accepted_at INTEGER NOT NULL,
  retired_at INTEGER,
  ${fact},
  FOREIGN KEY (input_id, member_id) REFERENCES generation_inputs(id, member_id),
  UNIQUE (id, member_id),
  UNIQUE (id, input_id)
) STRICT;
CREATE INDEX idx_requests_member_time ON generation_requests(member_id, accepted_at DESC, id DESC);

CREATE TABLE generation_jobs (
  id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL REFERENCES generation_requests(id),
  sample_index INTEGER NOT NULL CHECK (sample_index >= 0),
  actual_seed INTEGER CHECK (actual_seed BETWEEN 0 AND 4294967295),
  status TEXT NOT NULL CHECK (status IN ('queued', 'pending', 'running', 'done', 'failed')),
  queue_seq INTEGER UNIQUE CHECK (queue_seq > 0),
  queue_expires_at INTEGER,
  deadline_at INTEGER,
  admission_epoch INTEGER NOT NULL CHECK (admission_epoch >= 0),
  artifact_id TEXT REFERENCES artifacts(id) DEFERRABLE INITIALLY DEFERRED,
  cache_key TEXT,
  completed_at INTEGER,
  error_code TEXT,
  error_status INTEGER,
  cancellation_reason TEXT,
  late_result_at INTEGER,
  free_succeeded_at INTEGER,
  free_warning_threshold INTEGER CHECK (free_warning_threshold > 0),
  retired_at INTEGER,
  ${fact},
  UNIQUE (request_id, sample_index),
  CHECK (status != 'queued' OR queue_seq IS NOT NULL),
  CHECK (status != 'done' OR (artifact_id IS NOT NULL AND completed_at IS NOT NULL))
) STRICT;
CREATE INDEX idx_jobs_queue ON generation_jobs(queue_seq) WHERE status = 'queued';
CREATE INDEX idx_jobs_request_status ON generation_jobs(request_id, status);
CREATE INDEX idx_jobs_free_success ON generation_jobs(free_succeeded_at)
  WHERE free_succeeded_at IS NOT NULL;

CREATE TABLE generation_attempts (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL UNIQUE REFERENCES generation_jobs(id),
  account_id TEXT NOT NULL REFERENCES upstream_accounts(id),
  credential_epoch INTEGER CHECK (credential_epoch > 0),
  authorized_at INTEGER,
  dispatch_state TEXT NOT NULL CHECK (dispatch_state IN ('unknown', 'not_sent', 'request_started')),
  upstream_started_at INTEGER,
  upstream_headers_at INTEGER,
  upstream_completed_at INTEGER,
  upstream_http_status INTEGER CHECK (upstream_http_status BETWEEN 100 AND 599),
  candidate_mode TEXT NOT NULL CHECK (candidate_mode IN ('known', 'alternatives', 'unknown')),
  upstream_candidates TEXT NOT NULL CHECK (json_valid(upstream_candidates)
    AND json_type(upstream_candidates) = 'array'),
  actual_consumption TEXT CHECK (json_valid(actual_consumption)
    AND json_type(actual_consumption) = 'object'),
  review_status TEXT CHECK (review_status IN ('pending', 'estimated_closed', 'result_recovered')),
  first_unknown_at INTEGER,
  review_deadline_at INTEGER,
  review_closed_at INTEGER,
  loss_event_id TEXT REFERENCES domain_events(event_id),
  closure_event_id TEXT REFERENCES domain_events(event_id),
  prepared_artifact_id TEXT,
  prepared_file TEXT,
  prepared_mime_type TEXT,
  prepared_byte_length INTEGER CHECK (prepared_byte_length > 0),
  prepared_sha256 TEXT CHECK (length(prepared_sha256) = 64
    AND prepared_sha256 NOT GLOB '*[^a-f0-9]*'),
  ${fact},
  CHECK ((prepared_artifact_id IS NULL AND prepared_file IS NULL AND prepared_mime_type IS NULL
      AND prepared_byte_length IS NULL AND prepared_sha256 IS NULL)
    OR (prepared_artifact_id IS NOT NULL AND prepared_file IS NOT NULL AND prepared_mime_type IS NOT NULL
      AND prepared_byte_length IS NOT NULL AND prepared_sha256 IS NOT NULL)),
  CHECK (review_status IS NULL OR
    (first_unknown_at IS NOT NULL AND review_deadline_at IS NOT NULL AND loss_event_id IS NOT NULL)),
  UNIQUE (id, job_id, account_id)
) STRICT;
CREATE INDEX idx_attempts_review ON generation_attempts(review_deadline_at)
  WHERE review_status = 'pending';
CREATE INDEX idx_attempts_prepared ON generation_attempts(job_id)
  WHERE prepared_artifact_id IS NOT NULL;

CREATE TABLE wallets (
  account_id TEXT NOT NULL REFERENCES upstream_accounts(id),
  member_id TEXT NOT NULL REFERENCES members(id),
  ${amount('v5_available')},
  ${amount('anlas_fixed_available')},
  ${amount('anlas_purchased_available')},
  ${fact},
  PRIMARY KEY (account_id, member_id)
) STRICT;

CREATE TABLE charges (
  original_job_id TEXT PRIMARY KEY,
  original_member_id TEXT NOT NULL,
  original_account_id TEXT NOT NULL,
  job_id TEXT UNIQUE,
  wallet_account_id TEXT,
  wallet_member_id TEXT,
  attempt_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('reserved', 'settled', 'refunded')),
  resolution TEXT NOT NULL CHECK (resolution IN ('live', 'archived', 'unavailable')),
  origin_ref TEXT NOT NULL,
  ${amount('v5')},
  ${amount('anlas_fixed')},
  ${amount('anlas_purchased')},
  billing_mode TEXT NOT NULL CHECK (billing_mode IN ('free', 'v5', 'anlas', 'anlas_fallback')),
  ${amount('v5_equivalent')},
  ${amount('shared_v5_reserved')},
  config_version INTEGER NOT NULL CHECK (config_version >= 0),
  reserved_at INTEGER NOT NULL,
  terminal_at INTEGER,
  reservation_event_id TEXT NOT NULL REFERENCES domain_events(event_id),
  terminal_event_id TEXT REFERENCES domain_events(event_id),
  CHECK (job_id IS NULL OR job_id = original_job_id),
  CHECK ((wallet_account_id IS NULL AND wallet_member_id IS NULL)
    OR (wallet_account_id IS NOT NULL AND wallet_member_id IS NOT NULL
      AND wallet_account_id = original_account_id AND wallet_member_id = original_member_id)),
  CHECK ((resolution = 'live' AND job_id IS NOT NULL AND wallet_account_id IS NOT NULL)
    OR (resolution != 'live' AND status != 'reserved')),
  CHECK ((status = 'reserved' AND terminal_at IS NULL AND terminal_event_id IS NULL)
    OR (status != 'reserved' AND terminal_at IS NOT NULL AND terminal_event_id IS NOT NULL)),
  FOREIGN KEY (job_id) REFERENCES generation_jobs(id),
  FOREIGN KEY (wallet_account_id, wallet_member_id) REFERENCES wallets(account_id, member_id),
  FOREIGN KEY (attempt_id, job_id, wallet_account_id)
    REFERENCES generation_attempts(id, job_id, account_id),
  CHECK (attempt_id IS NULL OR (job_id IS NOT NULL AND wallet_account_id IS NOT NULL))
) STRICT;
CREATE INDEX idx_charges_reserved ON charges(original_account_id, original_member_id)
  WHERE status = 'reserved';

CREATE TABLE account_quota_state (
  account_id TEXT PRIMARY KEY REFERENCES upstream_accounts(id),
  status TEXT NOT NULL,
  observation_event_id TEXT REFERENCES domain_events(event_id),
  observed_at INTEGER,
  observed_credential_epoch INTEGER CHECK (observed_credential_epoch > 0),
  observed_sequence INTEGER CHECK (observed_sequence > 0),
  quota_tier INTEGER,
  subscription_active INTEGER CHECK (subscription_active IN (0, 1)),
  subscription_expires_at INTEGER,
  quota_error_code TEXT,
  ${amount('observed_v5', true)},
  ${amount('observed_anlas_fixed', true)},
  ${amount('observed_anlas_purchased', true)},
  seconds_per_percent REAL CHECK (seconds_per_percent >= 0),
  baseline_event_id TEXT REFERENCES domain_events(event_id),
  refill_cursor_at INTEGER,
  settlement_cursor_at INTEGER,
  ${amount('unallocated_v5')},
  ${amount('fallback_fixed')},
  ${amount('fallback_purchased')},
  ${amount('shared_v5_debt')},
  ${amount('pending_v5_debit')},
  ${amount('pending_fixed_debit')},
  ${amount('pending_purchased_debit')},
  ${amount('pending_fallback_v5')},
  ${amount('pending_fallback_fixed')},
  ${amount('pending_fallback_purchased')},
  ${fact}
) STRICT;

CREATE TABLE operating_state (
  account_id TEXT PRIMARY KEY REFERENCES upstream_accounts(id),
  ${amount('v5_available')},
  ${amount('anlas_fixed_available')},
  ${amount('anlas_purchased_available')},
  ${amount('unfunded_fixed')},
  ${amount('unfunded_purchased')},
  ${amount('unattributed_fixed')},
  ${amount('unattributed_purchased')},
  ${amount('v5_capacity_gap')},
  fixed_gap_event_id TEXT REFERENCES domain_events(event_id),
  purchased_gap_event_id TEXT REFERENCES domain_events(event_id),
  v5_gap_event_id TEXT REFERENCES domain_events(event_id),
  ${fact}
) STRICT;

CREATE TABLE pending_funding (
  receipt TEXT PRIMARY KEY,
  account_id TEXT NOT NULL REFERENCES upstream_accounts(id),
  resource TEXT NOT NULL CHECK (resource IN ('anlasFixed', 'anlasPurchased')),
  destination TEXT NOT NULL,
  ${amount('remaining')},
  registered_at INTEGER NOT NULL,
  snapshot_at INTEGER,
  ${fact}
) STRICT;

CREATE TABLE artifacts (
  id TEXT PRIMARY KEY,
  member_id TEXT NOT NULL REFERENCES members(id),
  input_id TEXT NOT NULL,
  original_attempt_id TEXT UNIQUE REFERENCES generation_attempts(id),
  operation TEXT NOT NULL CHECK (operation IN ('generate', 'upscale', 'encode-vibe')),
  mime_type TEXT NOT NULL,
  byte_length INTEGER NOT NULL CHECK (byte_length > 0),
  sha256 TEXT NOT NULL CHECK (length(sha256) = 64 AND sha256 NOT GLOB '*[^a-f0-9]*'),
  published_at INTEGER NOT NULL,
  retired_at INTEGER,
  ${fact},
  FOREIGN KEY (input_id, member_id) REFERENCES generation_inputs(id, member_id)
) STRICT;
CREATE INDEX idx_artifacts_member_time ON artifacts(member_id, published_at DESC, id DESC);

CREATE TABLE storage_targets (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL CHECK (kind IN ('local', 'r2', 'pikpak')),
  namespace TEXT NOT NULL,
  credential_ref TEXT,
  ${fact},
  UNIQUE (kind, namespace)
) STRICT;

CREATE TABLE artifact_replicas (
  artifact_id TEXT NOT NULL REFERENCES artifacts(id),
  storage_target_id TEXT NOT NULL REFERENCES storage_targets(id),
  object_key TEXT NOT NULL,
  encoding TEXT NOT NULL CHECK (encoding IN ('identity', 'gzip')),
  uploaded_at INTEGER,
  verified_at INTEGER,
  deleted_at INTEGER,
  last_error_code TEXT,
  ${fact},
  PRIMARY KEY (artifact_id, storage_target_id),
  UNIQUE (storage_target_id, object_key),
  CHECK (verified_at IS NULL OR uploaded_at IS NOT NULL)
) STRICT;

CREATE TABLE idempotency_keys (
  original_member_id TEXT NOT NULL,
  operation TEXT NOT NULL,
  key_hash TEXT NOT NULL,
  intent_fingerprint TEXT NOT NULL,
  original_request_id TEXT NOT NULL,
  original_job_id TEXT NOT NULL,
  original_command_id TEXT NOT NULL,
  request_id TEXT,
  commit_seq INTEGER REFERENCES commits(commit_seq),
  context_id TEXT REFERENCES normalization_contexts(id),
  resolution TEXT NOT NULL CHECK (resolution IN ('live', 'archived', 'unavailable')),
  origin_ref TEXT NOT NULL,
  archived_receipt TEXT CHECK (json_valid(archived_receipt)),
  created_at INTEGER NOT NULL,
  ${fact},
  PRIMARY KEY (original_member_id, operation, key_hash),
  CHECK (request_id IS NULL OR request_id = original_request_id),
  CHECK (resolution != 'live' OR
    (request_id IS NOT NULL AND commit_seq IS NOT NULL AND context_id IS NOT NULL)),
  CHECK ((commit_seq IS NOT NULL AND archived_receipt IS NULL)
    OR (commit_seq IS NULL AND archived_receipt IS NOT NULL)),
  FOREIGN KEY (request_id, original_member_id) REFERENCES generation_requests(id, member_id)
) STRICT;

CREATE TABLE generation_diagnostics (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES generation_jobs(id),
  detail TEXT NOT NULL,
  sha256 TEXT NOT NULL CHECK (length(sha256) = 64 AND sha256 NOT GLOB '*[^a-f0-9]*'),
  created_at INTEGER NOT NULL
) STRICT;

${['INSERT', 'UPDATE']
  .map(
    (operation) => `
CREATE TRIGGER charge_owner_${operation.toLowerCase()} BEFORE ${operation} ON charges
WHEN NEW.job_id IS NOT NULL
BEGIN
  SELECT RAISE(ABORT, 'charge member does not own job') WHERE NOT EXISTS (
    SELECT 1 FROM generation_jobs j JOIN generation_requests r ON r.id = j.request_id
    WHERE j.id = NEW.job_id AND r.member_id = NEW.original_member_id
  );
END;
CREATE TRIGGER job_artifact_owner_${operation.toLowerCase()} BEFORE ${operation} ON generation_jobs
WHEN NEW.artifact_id IS NOT NULL
BEGIN
  SELECT RAISE(ABORT, 'job member does not own artifact') WHERE NOT EXISTS (
    SELECT 1 FROM generation_requests r JOIN artifacts a ON a.member_id = r.member_id
    WHERE r.id = NEW.request_id AND a.id = NEW.artifact_id
  );
END;
CREATE TRIGGER job_input_${operation.toLowerCase()} BEFORE ${operation} ON generation_jobs
BEGIN
  SELECT RAISE(ABORT, 'sample index exceeds accepted request') WHERE NOT EXISTS (
    SELECT 1 FROM generation_requests r
    WHERE r.id = NEW.request_id AND NEW.sample_index < r.sample_count
  );
  SELECT RAISE(ABORT, 'active job requires complete input and first context')
  WHERE NEW.status IN ('queued', 'pending', 'running') AND NOT EXISTS (
    SELECT 1 FROM generation_requests r JOIN generation_inputs i ON i.id = r.input_id
    WHERE r.id = NEW.request_id AND i.completeness = 'complete' AND r.context_id IS NOT NULL
      AND r.seed_mode != 'unknown'
  );
END;
CREATE TRIGGER artifact_attempt_owner_${operation.toLowerCase()} BEFORE ${operation} ON artifacts
WHEN NEW.original_attempt_id IS NOT NULL
BEGIN
  SELECT RAISE(ABORT, 'artifact member does not own original attempt') WHERE NOT EXISTS (
    SELECT 1 FROM generation_attempts a
    JOIN generation_jobs j ON j.id = a.job_id
    JOIN generation_requests r ON r.id = j.request_id
    WHERE a.id = NEW.original_attempt_id AND r.member_id = NEW.member_id
  );
END;
CREATE TRIGGER idempotency_target_${operation.toLowerCase()} BEFORE ${operation} ON idempotency_keys
BEGIN
  SELECT RAISE(ABORT, 'idempotency command identity mismatch')
  WHERE NEW.commit_seq IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM commits c
    WHERE c.commit_seq = NEW.commit_seq AND c.command_id = NEW.original_command_id
  );
  SELECT RAISE(ABORT, 'live idempotency job does not belong to request')
  WHERE NEW.resolution = 'live' AND NOT EXISTS (
    SELECT 1 FROM generation_jobs j
    WHERE j.id = NEW.original_job_id AND j.request_id = NEW.request_id
  );
END;`,
  )
  .join('\n')}

${[
  ['normalization_contexts', 'schema_version, sha256, body'],
  ['generation_inputs', 'member_id, schema_version, completeness, sha256, logical_bytes, body'],
  ['input_assets', 'input_id, asset_id, mime_type, byte_length, sha256, bytes'],
  [
    'generation_requests',
    'member_id, input_id, context_id, source, operation, sample_count, seed_mode, confirmation, accepted_at',
  ],
  ['generation_jobs', 'request_id, sample_index, actual_seed'],
  ['generation_attempts', 'job_id, account_id, credential_epoch'],
  ['artifacts', 'member_id, input_id, original_attempt_id'],
  [
    'charges',
    'original_job_id, original_member_id, original_account_id, v5, anlas_fixed, anlas_purchased, billing_mode, v5_equivalent, shared_v5_reserved, config_version, reserved_at, reservation_event_id',
  ],
  [
    'idempotency_keys',
    'original_member_id, operation, key_hash, intent_fingerprint, original_request_id, original_job_id, original_command_id',
  ],
]
  .map(
    ([table, fields]) => `
CREATE TRIGGER ${table}_identity BEFORE UPDATE OF ${fields} ON ${table}
WHEN ${fields
      .split(', ')
      .map((field) => `NEW.${field} IS NOT OLD.${field}`)
      .join(' OR ')}
BEGIN
  SELECT RAISE(ABORT, 'immutable ${table} identity');
END;`,
  )
  .join('\n')}

CREATE TRIGGER charge_terminal BEFORE UPDATE OF status, terminal_at, terminal_event_id ON charges
WHEN OLD.status != 'reserved' AND
  (NEW.status IS NOT OLD.status OR NEW.terminal_at IS NOT OLD.terminal_at
    OR NEW.terminal_event_id IS NOT OLD.terminal_event_id)
BEGIN
  SELECT RAISE(ABORT, 'charge already terminal');
END;

${['commits', 'domain_events']
  .map(
    (table) => `
CREATE TRIGGER ${table}_append_only_update BEFORE UPDATE ON ${table}
BEGIN
  SELECT RAISE(ABORT, 'append-only ${table}');
END;
CREATE TRIGGER ${table}_append_only_delete BEFORE DELETE ON ${table}
BEGIN
  SELECT RAISE(ABORT, 'append-only ${table}');
END;`,
  )
  .join('\n')}
`;
