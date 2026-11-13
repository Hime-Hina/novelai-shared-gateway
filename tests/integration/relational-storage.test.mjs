import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
  RELATIONAL_DATABASE_FORMAT,
  RELATIONAL_WRITER_VERSION,
} from '../../server/storage/relational-storage.ts';
import { migrateStorage } from '../../server/storage/sqlite-migrations.ts';
import { parseAmount } from '../../server/quota/resource-amounts.ts';

const sha256 = 'a'.repeat(64);
const fact = { source_event_id: 'e1' };

function insert(sqlite, table, row) {
  const fields = Object.keys(row);
  sqlite
    .prepare(
      `INSERT INTO ${table} (${fields.join(', ')}) VALUES (${fields.map(() => '?').join(', ')})`,
    )
    .run(...Object.values(row));
}

function database(t) {
  const sqlite = new Database(':memory:');
  t.after(() => sqlite.close());
  initializeRelationalStorage(sqlite, 'synthetic-history');
  return sqlite;
}

function event(overrides = {}) {
  return {
    event_id: 'e1',
    commit_seq: 1,
    event_index: 0,
    stream_id: 'test:baseline',
    stream_kind: 'test',
    stream_version: 1,
    event_type: 'TestBaselineRecorded',
    schema_version: 2,
    occurred_at: 1000,
    account_id: null,
    member_id: null,
    policy_version: 'p1',
    pricing_version: 'p1',
    amount_version: 'decimal-12-v1',
    payload: '{}',
    ...overrides,
  };
}

function fixture(t) {
  const sqlite = database(t);
  insert(sqlite, 'commits', {
    commit_seq: 1,
    command_id: 'c1',
    fingerprint: sha256,
    actor_kind: 'system',
    recorded_at: 1000,
    event_count: 1,
    receipt: '{"requestId":"r1"}',
  });
  insert(sqlite, 'domain_events', event());
  insert(sqlite, 'normalization_contexts', {
    id: 'ctx',
    schema_version: 1,
    sha256,
    body: '{}',
  });
  for (const n of [1, 2]) {
    insert(sqlite, 'members', {
      id: `m${n}`,
      display_name: `member ${n}`,
      status: 'active',
      accepting_jobs: 1,
      auth_epoch: `auth-${n}`,
      admission_epoch: 0,
      created_at: 1000,
      ...fact,
    });
    insert(sqlite, 'upstream_accounts', {
      id: `a${n}`,
      display_name: `account ${n}`,
      enabled: 1,
      credential_epoch: 1,
      ordinary_strikes: 0,
      capacity_strikes: 0,
      ...fact,
    });
    insert(sqlite, 'generation_inputs', {
      id: `i${n}`,
      member_id: `m${n}`,
      schema_version: 1,
      completeness: 'complete',
      sha256,
      logical_bytes: 2,
      body: '{}',
    });
    insert(sqlite, 'generation_requests', {
      id: `r${n}`,
      member_id: `m${n}`,
      input_id: `i${n}`,
      context_id: 'ctx',
      source: 'native',
      operation: 'generate',
      sample_count: 2,
      seed_mode: 'fixed',
      accepted_at: 1000,
      ...fact,
    });
    insert(sqlite, 'generation_jobs', {
      id: `j${n}`,
      request_id: `r${n}`,
      sample_index: 0,
      actual_seed: 42,
      status: 'pending',
      admission_epoch: 0,
      ...fact,
    });
    insert(sqlite, 'wallets', {
      account_id: 'a1',
      member_id: `m${n}`,
      v5_available: '1',
      anlas_fixed_available: '2',
      anlas_purchased_available: '3',
      ...fact,
    });
  }
  return sqlite;
}

function attempt(overrides = {}) {
  return {
    id: 'attempt1',
    job_id: 'j1',
    account_id: 'a1',
    credential_epoch: 1,
    authorized_at: 1000,
    dispatch_state: 'unknown',
    candidate_mode: 'unknown',
    upstream_candidates: '[]',
    ...fact,
    ...overrides,
  };
}

function charge(overrides = {}) {
  return {
    original_job_id: 'j1',
    original_member_id: 'm1',
    original_account_id: 'a1',
    job_id: 'j1',
    wallet_account_id: 'a1',
    wallet_member_id: 'm1',
    attempt_id: null,
    status: 'reserved',
    resolution: 'live',
    origin_ref: 'native:e1',
    v5: '0.058',
    anlas_fixed: '0',
    anlas_purchased: '0',
    billing_mode: 'v5',
    v5_equivalent: '0',
    shared_v5_reserved: '0',
    config_version: 1,
    reserved_at: 1000,
    terminal_at: null,
    reservation_event_id: 'e1',
    terminal_event_id: null,
    ...overrides,
  };
}

function artifact(overrides = {}) {
  return {
    id: 'image1',
    member_id: 'm1',
    input_id: 'i1',
    original_attempt_id: null,
    operation: 'generate',
    mime_type: 'image/png',
    byte_length: 3,
    sha256,
    published_at: 2000,
    ...fact,
    ...overrides,
  };
}

test('新格式独立初始化为 building，没有旧通用记录、正文影子或流投影', (t) => {
  const sqlite = database(t);
  assert.equal(sqlite.pragma('foreign_keys', { simple: true }), 1);
  assert.equal(
    sqlite.prepare("SELECT value FROM app_meta WHERE key='database_format'").get().value,
    RELATIONAL_DATABASE_FORMAT,
  );
  const marker = JSON.parse(
    sqlite.prepare("SELECT value FROM app_meta WHERE key='event_authority'").get().value,
  );
  assert.equal(marker.version, RELATIONAL_WRITER_VERSION);
  assert.equal(sqlite.prepare('SELECT status FROM projection_state').get().status, 'building');
  assert.throws(() => assertRelationalStorageReady(sqlite), /incomplete/);
  const tables = sqlite
    .prepare("SELECT name FROM sqlite_schema WHERE type='table'")
    .all()
    .map(({ name }) => name);
  for (const name of [
    'cards',
    'users',
    'accounts',
    'jobs',
    'images',
    'ledger',
    'app_records',
    'app_settings',
    'event_streams',
  ]) {
    assert.ok(!tables.includes(name), name);
  }
  for (const table of tables) {
    const columns = sqlite
      .prepare(`PRAGMA table_info(${table})`)
      .all()
      .map(({ name }) => name);
    for (const name of [
      'data',
      'projection',
      'user_token',
      'token',
      'search_text',
      'full_prompt',
      'request_payload',
    ]) {
      assert.ok(!columns.includes(name), `${table}.${name}`);
    }
  }
  assert.deepEqual(sqlite.pragma('foreign_key_check'), []);
  assert.equal(sqlite.pragma('integrity_check', { simple: true }), 'ok');
});

test('新旧 writer 相互拒绝，拒绝初始化非空目标，不改旧迁移和库内容', (t) => {
  const relational = database(t);
  const before = relational.serialize();
  assert.throws(() => migrateStorage(relational), /compatible writer|unsupported version/);
  assert.deepEqual(relational.serialize(), before);
  assert.throws(() => initializeRelationalStorage(relational, 'another-history'), /empty target/);
  assert.deepEqual(relational.serialize(), before);

  const legacy = new Database(':memory:');
  t.after(() => legacy.close());
  migrateStorage(legacy);
  const old = legacy.serialize();
  assert.throws(() => assertRelationalStorageReady(legacy), /Unsupported/);
  assert.throws(() => initializeRelationalStorage(legacy, 'history'), /empty target/);
  assert.deepEqual(legacy.serialize(), old);

  const view = new Database(':memory:');
  t.after(() => view.close());
  view.exec('CREATE VIEW sqliteXreserved AS SELECT 1');
  assert.throws(() => initializeRelationalStorage(view, 'history'), /empty target/);
});

test('DDL 中断整笔回滚，空历史身份与外层事务不能创建半库', (t) => {
  const sqlite = new Database(':memory:');
  t.after(() => sqlite.close());
  assert.throws(() => initializeRelationalStorage(sqlite, '  '), /history identity/);
  assert.throws(
    () => sqlite.transaction(() => initializeRelationalStorage(sqlite, 'h'))(),
    /own transaction/,
  );
  const exec = sqlite.exec;
  sqlite.exec = function (sql) {
    exec.call(this, sql);
    throw new Error('synthetic interruption after DDL');
  };
  assert.throws(() => initializeRelationalStorage(sqlite, 'h'), /synthetic interruption/);
  sqlite.exec = exec;
  assert.deepEqual(sqlite.prepare('SELECT name FROM sqlite_schema').all(), []);
  initializeRelationalStorage(sqlite, 'h');
  assert.throws(() => assertRelationalStorageReady(sqlite), /incomplete/);
});

test('ready 门禁拒绝缺失游标、未来版本、DDL 漂移和关闭外键，保留 64 位游标', (t) => {
  const sqlite = fixture(t);
  sqlite.prepare("UPDATE projection_state SET status='ready'").run();
  assert.throws(() => assertRelationalStorageReady(sqlite), /incomplete/);
  sqlite.prepare('UPDATE projection_state SET last_commit_seq=1').run();
  assert.doesNotThrow(() => assertRelationalStorageReady(sqlite));
  sqlite.prepare('UPDATE projection_state SET reducer_version=2').run();
  assert.throws(() => assertRelationalStorageReady(sqlite), /incompatible/);
  sqlite.prepare('UPDATE projection_state SET reducer_version=1').run();
  const authority = sqlite
    .prepare("SELECT value FROM app_meta WHERE key='event_authority'")
    .get().value;
  sqlite
    .prepare("UPDATE app_meta SET value=? WHERE key='event_authority'")
    .run(JSON.stringify({ version: 6, minimumWriterVersion: 6 }));
  assert.throws(() => assertRelationalStorageReady(sqlite), /writer/);
  sqlite.prepare("UPDATE app_meta SET value=? WHERE key='event_authority'").run(authority);
  sqlite.pragma('foreign_keys = OFF');
  assert.throws(() => assertRelationalStorageReady(sqlite), /foreign_keys/);
  sqlite.pragma('foreign_keys = ON');
  sqlite.pragma('foreign_keys = OFF');
  insert(sqlite, 'wallets', {
    account_id: 'absent',
    member_id: 'absent',
    v5_available: '0',
    anlas_fixed_available: '0',
    anlas_purchased_available: '0',
    ...fact,
  });
  sqlite.pragma('foreign_keys = ON');
  assert.throws(() => assertRelationalStorageReady(sqlite), /foreign key references/);
  sqlite.prepare("DELETE FROM wallets WHERE account_id='absent'").run();

  const cursor = 9_007_199_254_740_993n;
  // 这里只构造游标大于 JS 安全整数的 schema 夹具，不重写旧提交。
  sqlite.transaction(() => {
    insert(sqlite, 'commits', {
      commit_seq: cursor,
      command_id: 'large-cursor',
      fingerprint: sha256,
      actor_kind: 'system',
      recorded_at: 2000,
      event_count: 1,
      receipt: '{}',
    });
    insert(
      sqlite,
      'domain_events',
      event({ event_id: 'e2', commit_seq: cursor, stream_version: 2 }),
    );
    sqlite.prepare('UPDATE projection_state SET last_commit_seq=?').run(cursor);
  })();
  assert.doesNotThrow(() => assertRelationalStorageReady(sqlite));
  sqlite
    .prepare("UPDATE app_meta SET value='novelai-relational-v2' WHERE key='database_format'")
    .run();
  assert.throws(() => assertRelationalStorageReady(sqlite), /format/);
  sqlite
    .prepare("UPDATE app_meta SET value=? WHERE key='database_format'")
    .run(RELATIONAL_DATABASE_FORMAT);
  sqlite.exec('ALTER TABLE members ADD COLUMN unwanted_copy TEXT');
  assert.throws(() => assertRelationalStorageReady(sqlite), /installed schema/);
});

test('金额保存规范十进制 TEXT，保留超出 SQLite int64 的范围和一格精度', (t) => {
  const sqlite = fixture(t);
  const update = sqlite.prepare(
    "UPDATE wallets SET v5_available=? WHERE account_id='a1' AND member_id='m1'",
  );
  for (const value of [
    '0',
    '-1',
    '0.000000000001',
    '-0.000000000001',
    `${'9'.repeat(400)}.123456789012`,
  ]) {
    update.run(value);
    const saved = sqlite
      .prepare(
        "SELECT v5_available, typeof(v5_available) AS kind FROM wallets WHERE member_id='m1'",
      )
      .get();
    assert.equal(saved.v5_available, value);
    assert.equal(saved.kind, 'text');
    assert.equal(parseAmount(saved.v5_available), parseAmount(value));
  }
  for (const value of [
    '',
    'NaN',
    'Infinity',
    '1e3',
    '+1',
    '01',
    '-0',
    '1.',
    '.1',
    '1.0',
    '1.00',
    '0.0000000000001',
    '1..2',
    '--1',
    '1-2',
    '-.1',
    ' 1',
    '1\0',
    '0.1\0hidden',
  ]) {
    assert.throws(() => update.run(value), /CHECK/, value);
  }
  assert.throws(() => update.run(null), /NOT NULL/);
});

test('输入附件只在 input 内引用，SQLite 验证二进制长度和稳定成员归属', (t) => {
  const sqlite = fixture(t);
  const asset = {
    input_id: 'i1',
    asset_id: 'source',
    mime_type: 'image/png',
    byte_length: 3,
    sha256,
    bytes: Buffer.from([1, 2, 3]),
  };
  insert(sqlite, 'input_assets', asset);
  assert.deepEqual(sqlite.prepare('SELECT bytes FROM input_assets').get().bytes, asset.bytes);
  assert.throws(
    () => insert(sqlite, 'input_assets', { ...asset, asset_id: 'bad', byte_length: 4 }),
    /CHECK/,
  );
  assert.throws(
    () => insert(sqlite, 'input_assets', { ...asset, input_id: 'absent' }),
    /FOREIGN KEY/,
  );
  assert.throws(
    () =>
      insert(sqlite, 'generation_requests', {
        id: 'cross-member',
        member_id: 'm2',
        input_id: 'i1',
        source: 'native',
        operation: 'generate',
        sample_count: 1,
        seed_mode: 'fixed',
        accepted_at: 1000,
        ...fact,
      }),
    /FOREIGN KEY/,
  );
  assert.throws(
    () =>
      sqlite
        .prepare("UPDATE generation_requests SET member_id='m2', input_id='i2' WHERE id='r1'")
        .run(),
    /immutable/,
  );
  assert.throws(
    () => sqlite.prepare("UPDATE generation_inputs SET completeness='partial' WHERE id='i1'").run(),
    /immutable/,
  );
  insert(sqlite, 'generation_inputs', {
    id: 'partial',
    member_id: 'm1',
    schema_version: 1,
    completeness: 'partial',
    sha256,
    logical_bytes: 2,
    body: '{}',
  });
  insert(sqlite, 'generation_requests', {
    id: 'partial-request',
    member_id: 'm1',
    input_id: 'partial',
    context_id: 'ctx',
    source: 'native',
    operation: 'generate',
    sample_count: 1,
    seed_mode: 'unknown',
    accepted_at: 1000,
    ...fact,
  });
  assert.throws(
    () =>
      insert(sqlite, 'generation_jobs', {
        id: 'partial-job',
        request_id: 'partial-request',
        sample_index: 0,
        status: 'pending',
        admission_epoch: 0,
        ...fact,
      }),
    /complete input/,
  );
  assert.throws(
    () =>
      insert(sqlite, 'generation_jobs', {
        id: 'extra',
        request_id: 'r2',
        sample_index: 2,
        status: 'pending',
        admission_epoch: 0,
        ...fact,
      }),
    /sample index/,
  );
});

test('每个 job 最多一次尝试，收费的成员、账号和尝试一致，失败事务不改变钱包', (t) => {
  const sqlite = fixture(t);
  insert(sqlite, 'generation_attempts', attempt());
  assert.throws(() => insert(sqlite, 'generation_attempts', attempt({ id: 'second' })), /UNIQUE/);
  assert.throws(
    () => insert(sqlite, 'charges', charge({ original_member_id: 'm2', wallet_member_id: 'm2' })),
    /does not own job/,
  );
  insert(
    sqlite,
    'generation_attempts',
    attempt({ id: 'other-account', job_id: 'j2', account_id: 'a2', credential_epoch: null }),
  );
  assert.throws(
    () => insert(sqlite, 'charges', charge({ attempt_id: 'other-account' })),
    /FOREIGN KEY/,
  );
  const before = sqlite.prepare('SELECT * FROM wallets ORDER BY member_id').all();
  assert.throws(
    () =>
      sqlite.transaction(() => {
        sqlite.prepare("UPDATE wallets SET v5_available='0' WHERE member_id='m1'").run();
        insert(sqlite, 'charges', charge({ attempt_id: 'other-account' }));
      })(),
    /FOREIGN KEY/,
  );
  assert.deepEqual(sqlite.prepare('SELECT * FROM wallets ORDER BY member_id').all(), before);
  insert(sqlite, 'charges', charge({ attempt_id: 'attempt1' }));
  assert.throws(() => sqlite.prepare("UPDATE charges SET v5='1'").run(), /immutable/);
  sqlite
    .prepare("UPDATE charges SET status='refunded', terminal_at=2000, terminal_event_id='e1'")
    .run();
  assert.throws(
    () => sqlite.prepare("UPDATE charges SET status='settled'").run(),
    /already terminal/,
  );
  assert.throws(
    () => sqlite.prepare("UPDATE generation_attempts SET account_id='a2'").run(),
    /immutable/,
  );
});

test('产物属于原成员，缓存复用保留原 input/attempt，上传确认不冒充已核验', (t) => {
  const sqlite = fixture(t);
  insert(sqlite, 'generation_attempts', attempt());
  insert(sqlite, 'artifacts', artifact({ original_attempt_id: 'attempt1' }));
  assert.throws(
    () => insert(sqlite, 'artifacts', artifact({ id: 'wrong-input', member_id: 'm2' })),
    /does not own|FOREIGN KEY/,
  );
  assert.throws(
    () =>
      insert(
        sqlite,
        'artifacts',
        artifact({
          id: 'wrong-attempt',
          member_id: 'm2',
          input_id: 'i2',
          original_attempt_id: 'attempt1',
        }),
      ),
    /does not own/,
  );
  assert.throws(
    () => sqlite.prepare("UPDATE generation_jobs SET artifact_id='image1' WHERE id='j2'").run(),
    /does not own artifact/,
  );
  sqlite
    .prepare(
      "UPDATE generation_jobs SET artifact_id='image1', completed_at=2000, status='done' WHERE id='j1'",
    )
    .run();
  insert(sqlite, 'storage_targets', { id: 'r2', kind: 'r2', namespace: 'bucket/images', ...fact });
  const replica = {
    artifact_id: 'image1',
    storage_target_id: 'r2',
    object_key: 'image1.png',
    encoding: 'identity',
    uploaded_at: 2000,
    ...fact,
  };
  insert(sqlite, 'artifact_replicas', replica);
  assert.equal(sqlite.prepare('SELECT verified_at FROM artifact_replicas').get().verified_at, null);
  assert.throws(
    () => sqlite.prepare('UPDATE artifact_replicas SET uploaded_at=NULL, verified_at=2000').run(),
    /CHECK/,
  );
  assert.deepEqual(sqlite.pragma('foreign_key_check'), []);
});

test('副本 DDL 保留 identity/gzip/br，未知编码被拒绝', (t) => {
  const sqlite = fixture(t);
  insert(sqlite, 'artifacts', artifact());
  insert(sqlite, 'artifacts', artifact({ id: 'image2', member_id: 'm2', input_id: 'i2' }));
  for (const encoding of ['identity', 'gzip', 'br']) {
    insert(sqlite, 'storage_targets', {
      id: encoding,
      kind: 'pikpak',
      namespace: `synthetic/${encoding}`,
      ...fact,
    });
    insert(sqlite, 'artifact_replicas', {
      artifact_id: 'image1',
      storage_target_id: encoding,
      object_key: 'same-object',
      encoding,
      uploaded_at: 1,
      ...fact,
    });
  }
  assert.throws(
    () =>
      insert(sqlite, 'artifact_replicas', {
        artifact_id: 'image2',
        storage_target_id: 'gzip',
        object_key: 'bad-encoding',
        encoding: 'deflate',
        ...fact,
      }),
    /CHECK/,
  );
  assert.deepEqual(sqlite.pragma('foreign_key_check'), []);
});

test('独立产物可引用同对象且按 target/key 索引查询，不以对象定位合并所有者', (t) => {
  const sqlite = fixture(t);
  insert(sqlite, 'artifacts', artifact());
  insert(sqlite, 'artifacts', artifact({ id: 'image2', member_id: 'm2', input_id: 'i2' }));
  insert(sqlite, 'storage_targets', { id: 'r2', kind: 'r2', namespace: 'synthetic/r2', ...fact });
  for (const artifactId of ['image1', 'image2']) {
    insert(sqlite, 'artifact_replicas', {
      artifact_id: artifactId,
      storage_target_id: 'r2',
      object_key: 'same-object',
      encoding: 'identity',
      uploaded_at: 1,
      ...fact,
    });
  }
  assert.equal(
    sqlite
      .prepare(
        "SELECT COUNT(*) AS n FROM artifact_replicas WHERE storage_target_id='r2' AND object_key='same-object'",
      )
      .get().n,
    2,
  );
  assert.match(
    JSON.stringify(
      sqlite
        .prepare(
          "EXPLAIN QUERY PLAN SELECT artifact_id FROM artifact_replicas WHERE storage_target_id='r2' AND object_key='same-object'",
        )
        .all(),
    ),
    /idx_replica_object/,
  );
  assert.deepEqual(sqlite.pragma('foreign_key_check'), []);
});

test('原目标缺失的旧键和退款薄凭据保留原身份，外键开启仍能开账和防重', (t) => {
  const sqlite = fixture(t);
  const key = {
    original_member_id: 'removed-member',
    operation: 'generate',
    key_hash: sha256,
    intent_fingerprint: sha256,
    original_request_id: 'missing-request',
    original_job_id: 'missing-job',
    original_command_id: 'old-command',
    resolution: 'archived',
    origin_ref: 'archive:manifest#key',
    archived_receipt: '{"requestId":"missing-request","jobId":"missing-job"}',
    created_at: 500,
    ...fact,
  };
  insert(sqlite, 'idempotency_keys', key);
  assert.throws(() => insert(sqlite, 'idempotency_keys', key), /UNIQUE/);
  assert.throws(
    () => sqlite.prepare("UPDATE idempotency_keys SET intent_fingerprint='different'").run(),
    /immutable/,
  );
  assert.throws(
    () => sqlite.prepare("UPDATE idempotency_keys SET resolution='live'").run(),
    /live idempotency|CHECK/,
  );
  const refund = charge({
    original_job_id: 'missing-job',
    original_member_id: 'removed-member',
    original_account_id: 'missing-account',
    job_id: null,
    wallet_account_id: null,
    wallet_member_id: null,
    status: 'refunded',
    resolution: 'archived',
    origin_ref: 'archive:manifest#refund',
    terminal_at: 700,
    terminal_event_id: 'e1',
  });
  insert(sqlite, 'charges', refund);
  assert.throws(() => insert(sqlite, 'charges', refund), /UNIQUE/);
  assert.throws(
    () => sqlite.prepare("UPDATE charges SET status='settled'").run(),
    /already terminal/,
  );
  assert.deepEqual(sqlite.pragma('foreign_key_check'), []);
});

test('新活动键需要完整关系和原命令，事件顺序与子任务索引唯一', (t) => {
  const sqlite = fixture(t);
  const key = {
    original_member_id: 'm1',
    operation: 'generate',
    key_hash: sha256,
    intent_fingerprint: sha256,
    original_request_id: 'r1',
    original_job_id: 'j1',
    original_command_id: 'c1',
    request_id: 'r1',
    commit_seq: 1,
    context_id: 'ctx',
    resolution: 'live',
    origin_ref: 'native:e1',
    created_at: 1000,
    ...fact,
  };
  insert(sqlite, 'idempotency_keys', key);
  assert.throws(
    () =>
      insert(sqlite, 'idempotency_keys', {
        ...key,
        key_hash: 'b'.repeat(64),
        original_command_id: 'wrong',
      }),
    /command identity/,
  );
  assert.throws(
    () =>
      insert(sqlite, 'idempotency_keys', {
        ...key,
        key_hash: 'b'.repeat(64),
        original_job_id: 'j2',
      }),
    /does not belong/,
  );
  assert.throws(
    () =>
      insert(sqlite, 'generation_jobs', {
        id: 'duplicate',
        request_id: 'r1',
        sample_index: 0,
        status: 'pending',
        admission_epoch: 0,
        ...fact,
      }),
    /UNIQUE/,
  );
  assert.throws(
    () =>
      insert(sqlite, 'domain_events', event({ event_id: 'duplicate-index', stream_version: 2 })),
    /UNIQUE/,
  );
  assert.throws(
    () => insert(sqlite, 'domain_events', event({ event_id: 'duplicate-stream', event_index: 1 })),
    /UNIQUE/,
  );
  assert.throws(
    () =>
      insert(
        sqlite,
        'domain_events',
        event({ event_id: 'old-schema', event_index: 1, stream_version: 2, schema_version: 1 }),
      ),
    /CHECK/,
  );
  assert.throws(
    () =>
      insert(
        sqlite,
        'domain_events',
        event({
          event_id: 'bad-money',
          event_index: 1,
          stream_version: 2,
          amount_version: 'float',
        }),
      ),
    /CHECK/,
  );
  assert.throws(() => sqlite.prepare("UPDATE domain_events SET payload='{}'").run(), /append-only/);
  assert.throws(() => sqlite.prepare('DELETE FROM domain_events').run(), /append-only/);
  assert.throws(() => sqlite.prepare("UPDATE commits SET receipt='{}'").run(), /append-only/);
  assert.throws(() => sqlite.prepare('DELETE FROM commits').run(), /append-only/);
});
