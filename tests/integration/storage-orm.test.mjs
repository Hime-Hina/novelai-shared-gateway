import test, { after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile, cp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { Worker } from 'node:worker_threads';
import Database from 'better-sqlite3';
import { JsonStore } from '../../server/store.js';
import { migrateStorage, storageMigrationsDirectory } from '../../server/storage-migrations.js';
import { configureFairQuota, reserveFairQuota, syncFairQuota } from '../../server/fair-quota.js';

const legacySchema = await readFile(new URL('../fixtures/sqlite-before-drizzle.sql', import.meta.url), 'utf8');
const migrationCount = JSON.parse(await readFile(new URL('../../server/migrations/meta/_journal.json', import.meta.url), 'utf8')).entries.length;
const recordTables = ['cards', 'users', 'accounts', 'jobs', 'images', 'ledger'];
const now = Date.now();
const job = { id: 'queued', status: 'queued', userToken: 'synthetic-a', createdAt: new Date(now).toISOString(),
  request: { model: 'nai-diffusion-5-full', width: 1024, height: 1024, steps: 23, prompt: 'synthetic' } };
const temporaryDirectories = [];
// Windows 只有在所有测试的连接/worker 清理完成后才能删除 SQLite 文件。
after(() => Promise.all(temporaryDirectories.map(result => rm(result, { recursive: true, force: true }))));

async function directory() {
  const result = await mkdtemp(path.join(tmpdir(), 'gateway-storage-orm-'));
  temporaryDirectories.push(result);
  return result;
}

async function openStore(t, dataDir = null) {
  const store = new JsonStore(dataDir || await directory());
  t.after(() => store.close());
  await store.init();
  return store;
}

function metadata(sqlite) {
  return sqlite.prepare("SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE '__drizzle_%' ORDER BY name").all()
    .map(({ name }) => ({ name, columns: sqlite.prepare(`PRAGMA table_info(${name})`).all(),
      indexes: sqlite.prepare(`PRAGMA index_list(${name})`).all().filter(index => index.origin === 'c')
        .sort((a, b) => a.name.localeCompare(b.name)).map(index => ({ name: index.name, partial: index.partial,
          fields: sqlite.prepare('SELECT name, "desc", coll, "key" FROM pragma_index_xinfo(?)').all(index.name) })) }));
}

async function seedLedger(store) {
  await store.update(db => {
    db.users = ['a', 'b', 'c', 'd'].map(id => ({ id, token: `synthetic-${id}`, enabled: true, balance: 10 }));
    db.accounts = [{ id: 'opus', enabled: true, quotaTier: 3, subscriptionActive: true,
      quotaFixed: 10000, quotaPurchased: 0, v5UsagePercent: 80, quotaCheckedAt: now }];
    db.jobs = [structuredClone(job)];
    configureFairQuota(db, ['a', 'b', 'c', 'd']);
    syncFairQuota(db, now);
  }, { immediate: true });
}

test('新库的列、默认值、主键和索引方向与旧版完整 schema 一致', async t => {
  const legacy = new Database(':memory:');
  t.after(() => legacy.close());
  legacy.exec(legacySchema);
  const store = await openStore(t);
  assert.deepEqual(metadata(store.sqlite).filter(table => !['generation_requests', 'generation_idempotency', 'fair_quota_settlements', 'member_sessions'].includes(table.name)).map(table => ({ ...table, columns: table.columns.filter(column => !['request_id', 'queue_seq', 'payload_bytes'].includes(column.name)), indexes: table.indexes.filter(index => !['idx_jobs_queued_sequence', 'idx_jobs_request_status', 'idx_jobs_generation_review_due', 'idx_jobs_result_pending', 'idx_jobs_member_late_results', 'idx_ledger_source_at', 'idx_ledger_failure_history'].includes(index.name)) })), metadata(legacy));
  assert.equal(metadata(store.sqlite).length, 13);
  const partial = store.sqlite.prepare("SELECT name, sql FROM sqlite_schema WHERE type='index' AND sql LIKE '%WHERE%' ORDER BY name").all();
  assert.equal(partial.length, 8);
  assert.match(partial.find(index => index.name === 'idx_jobs_active_created').sql, /IN \('queued', 'pending', 'running'\)/);
  const plan = store.sqlite.prepare(`EXPLAIN QUERY PLAN SELECT id, order_value, data FROM jobs
    WHERE status IN ('queued', 'pending', 'running') ORDER BY created_at`).all();
  assert.ok(plan.some(row => row.detail.includes('idx_jobs_active_created')));
  const before = metadata(store.sqlite);
  migrateStorage(store.sqlite);
  assert.deepEqual(metadata(store.sqlite), before);
  assert.equal(store.sqlite.prepare('SELECT count(*) AS n FROM __drizzle_migrations').get().n, migrationCount);
});

for (const retainedMetrics of [0, 1, 3]) {
  test(`旧 typed tables 保留任务并补已知统计列：原有 ${retainedMetrics} 列`, async t => {
    const dataDir = await directory(), sqlite = new Database(path.join(dataDir, 'library.sqlite'));
    sqlite.exec(legacySchema);
    sqlite.prepare('INSERT INTO jobs (id, order_value, data, status, created_at) VALUES (?, ?, ?, ?, ?)')
      .run(job.id, 1, JSON.stringify(job), job.status, job.createdAt);
    if (retainedMetrics !== 3) {
      sqlite.exec('DROP INDEX idx_jobs_created_stats; DROP INDEX idx_jobs_updated_stats; DROP INDEX idx_jobs_error_log_updated;');
      for (const column of ['cost', 'stats_excluded', 'error_loggable'].slice(retainedMetrics)) {
        sqlite.exec(`ALTER TABLE jobs DROP COLUMN ${column}`);
      }
    }
    sqlite.exec('CREATE INDEX idx_jobs_updated_usage ON jobs (updated_at);');
    sqlite.close();
    const store = await openStore(t, dataDir);
    assert.deepEqual(store.selectItemById('jobs', job.id), { ...job, queueSeq: 1, payloadBytes: Buffer.byteLength(JSON.stringify(job.request)) });
    assert.deepEqual(store.sqlite.prepare('SELECT cost, stats_excluded, error_loggable FROM jobs').get(),
      { cost: 0, stats_excluded: 0, error_loggable: 0 });
    assert.equal(store.sqlite.prepare("SELECT 1 FROM sqlite_schema WHERE name='idx_jobs_updated_usage'").get(), undefined);
    await store.close();
    const reopened = await openStore(t, dataDir);
    assert.equal(reopened.countRecords('jobs'), 1);
    assert.deepEqual(reopened.selectItemById('jobs', job.id), { ...job, queueSeq: 1, payloadBytes: Buffer.byteLength(JSON.stringify(job.request)) });
  });
}

test('旧 app_records 在迁移后完整导入 typed tables，重复启动不重复导入', async t => {
  const dataDir = await directory(), sqlite = new Database(path.join(dataDir, 'library.sqlite'));
  sqlite.exec(legacySchema);
  for (const table of recordTables) sqlite.exec(`DROP TABLE ${table}`);
  sqlite.prepare('INSERT INTO app_records(collection,id,order_value,data) VALUES(?,?,?,?)')
    .run('jobs', job.id, 1, JSON.stringify(job));
  sqlite.prepare('INSERT INTO app_settings(key,value) VALUES(?,?)').run('settings', JSON.stringify({ serviceName: 'legacy-test' }));
  sqlite.close();
  const store = await openStore(t, dataDir);
  assert.equal(store.db.settings.serviceName, 'legacy-test');
  assert.deepEqual(store.selectItemById('jobs', job.id), { ...job, queueSeq: 1, payloadBytes: Buffer.byteLength(JSON.stringify(job.request)) });
  assert.equal(store.sqlite.prepare('SELECT count(*) AS n FROM app_records').get().n, 0);
  await store.close();
  assert.equal((await openStore(t, dataDir)).countRecords('jobs'), 1);
});

test('旧 JSON 文件经过同一新库迁移路径导入，保护模式保留源文件', async t => {
  const old = process.env.NO_AUTO_DELETE;
  process.env.NO_AUTO_DELETE = 'true';
  t.after(() => { if (old === undefined) delete process.env.NO_AUTO_DELETE; else process.env.NO_AUTO_DELETE = old; });
  const dataDir = await directory(), source = JSON.stringify({ settings: { serviceName: 'legacy-json' }, jobs: [job] });
  await writeFile(path.join(dataDir, 'library.json'), source);
  const store = await openStore(t, dataDir);
  assert.deepEqual(store.selectItemById('jobs', job.id), { ...job, queueSeq: 1, payloadBytes: Buffer.byteLength(JSON.stringify(job.request)) });
  assert.equal(await readFile(path.join(dataDir, 'library.json'), 'utf8'), source);
  assert.equal(store.sqlite.prepare('SELECT count(*) AS n FROM __drizzle_migrations').get().n, migrationCount);
});

test('不支持的旧结构或同名错误部分索引不登记基线、不留下半次补列', async t => {
  for (const mutation of [
    'ALTER TABLE users ADD unsupported_column TEXT;',
    "DROP INDEX idx_jobs_active_created; CREATE INDEX idx_jobs_active_created ON jobs(created_at) WHERE status='done';"
  ]) {
    const dataDir = await directory(), sqlite = new Database(path.join(dataDir, 'library.sqlite'));
    t.after(() => sqlite.close());
    sqlite.exec(legacySchema);
    sqlite.exec('DROP INDEX idx_jobs_error_log_updated; ALTER TABLE jobs DROP COLUMN error_loggable;');
    sqlite.exec(mutation);
    assert.throws(() => migrateStorage(sqlite), /baseline mismatch/);
    assert.equal(sqlite.prepare('PRAGMA table_info(jobs)').all().some(column => column.name === 'error_loggable'), false);
    assert.equal(sqlite.prepare("SELECT 1 FROM sqlite_schema WHERE name='__drizzle_migrations'").get(), undefined);
  }
});

test('增量迁移失败回滚、重复执行不重跑，并拒绝改写已执行迁移', async t => {
  const store = await openStore(t);
  const migrations = path.join(await directory(), 'migrations');
  await cp(storageMigrationsDirectory, migrations, { recursive: true });
  const journalPath = path.join(migrations, 'meta', '_journal.json');
  const journal = JSON.parse(await readFile(journalPath, 'utf8'));
  journal.entries.push({ idx: journal.entries.length, version: '6', when: journal.entries.at(-1).when + 1, tag: 'test_upgrade', breakpoints: true });
  await writeFile(journalPath, JSON.stringify(journal));
  const migrationFile = path.join(migrations, 'test_upgrade.sql');
  const addition = 'ALTER TABLE jobs ADD test_upgrade TEXT;';
  await writeFile(migrationFile, addition + '\n--> statement-breakpoint\nUPDATE missing_table SET value=1;');
  assert.throws(() => migrateStorage(store.sqlite, migrations), /missing_table/);
  assert.equal(store.sqlite.prepare('PRAGMA table_info(jobs)').all().some(column => column.name === 'test_upgrade'), false);
  assert.equal(store.sqlite.prepare('SELECT count(*) AS n FROM __drizzle_migrations').get().n, migrationCount);
  await writeFile(migrationFile, addition);
  migrateStorage(store.sqlite, migrations);
  migrateStorage(store.sqlite, migrations);
  assert.equal(store.sqlite.prepare('PRAGMA table_info(jobs)').all().filter(column => column.name === 'test_upgrade').length, 1);
  assert.equal(store.sqlite.prepare('SELECT count(*) AS n FROM __drizzle_migrations').get().n, migrationCount + 1);
  await writeFile(migrationFile, addition + '\n-- changed after application');
  assert.throws(() => migrateStorage(store.sqlite, migrations), /migration history differs/);
});

test('ORM 持久预扣保持 JSON/影子列一致、重复预扣不双扣，只读 worker 继续查询', async t => {
  const store = await openStore(t);
  await seedLedger(store);
  const reserve = db => {
    reserveFairQuota(db, db.users[0], db.jobs[0], db.accounts[0], now);
    db.jobs[0].status = 'running'; db.jobs[0].accountId = 'opus'; db.accounts[0].inFlight = 1;
  };
  const options = { includeSettings: true, dirtyRows: { jobs: ['queued'], accounts: ['opus'], ledger: ['attempt:queued:1:generation_reserved'] }, immediate: true };
  await store.update(reserve, options);
  const balance = store.db.settings.fairQuota.balances.a.v5Available;
  await store.update(reserve, options);
  assert.equal(store.db.settings.fairQuota.balances.a.v5Available, balance);
  const persisted = store.sqlite.prepare('SELECT data, status, account_id FROM jobs WHERE id=?').get(job.id);
  assert.equal(JSON.parse(persisted.data).status, persisted.status);
  assert.equal(JSON.parse(persisted.data).accountId, persisted.account_id);
  assert.equal(store.selectItemById('ledger', 'attempt:queued:1:generation_reserved').attempt.id, JSON.parse(persisted.data).generationAttempt.id);
  const worker = new Worker(new URL('../../server/sqlite-read-worker.js', import.meta.url), { workerData: { dbPath: store.dbPath } });
  t.after(() => worker.terminate());
  const stats = await new Promise((resolve, reject) => {
    worker.once('error', reject);
    worker.once('message', message => message.error ? reject(new Error(message.error)) : resolve(message.result));
    worker.postMessage({ id: 1, type: 'adminStats', payload: { now } });
  });
  assert.equal(stats.requestStats1m.total, 1);
  const reader = new Database(store.dbPath, { readonly: true, fileMustExist: true });
  t.after(() => reader.close());
  reader.pragma('query_only = ON');
  assert.throws(() => reader.prepare("UPDATE jobs SET status='failed'").run(), /readonly/);
});

test('实际 persister 在事务中退出不留半笔预扣，提交后退出则完整恢复', async t => {
  const store = await openStore(t);
  await seedLedger(store);
  const before = structuredClone(store.db.settings);
  await store.close();
  const script = `
    import { JsonStore } from ${JSON.stringify(new URL('../../server/store.js', import.meta.url).href)};
    import { reserveFairQuota } from ${JSON.stringify(new URL('../../server/fair-quota.js', import.meta.url).href)};
    const store = new JsonStore(process.argv[1]); await store.init();
    if (process.argv[2] === 'during') store.statements.upsertRecord.jobs.run = () => process.exit(73);
    await store.update(db => {
      reserveFairQuota(db, db.users[0], db.jobs[0], db.accounts[0], ${now});
      db.jobs[0].status = 'running'; db.accounts[0].inFlight = 1;
    }, { includeSettings: true, dirtyRows: { accounts: ['opus'], jobs: ['queued'], ledger: ['attempt:queued:1:generation_reserved'] }, immediate: true });
    process.exit(74);
  `;
  const crash = spawnSync(process.execPath, ['--input-type=module', '-e', script, store.dataDir, 'during'], { encoding: 'utf8', timeout: 15000 });
  assert.equal(crash.status, 73, crash.stderr);
  const sqlite = new Database(store.dbPath);
  t.after(() => sqlite.close());
  const settings = () => JSON.parse(sqlite.prepare("SELECT value FROM app_settings WHERE key='settings'").get().value);
  assert.deepEqual(settings(), before);
  assert.equal(sqlite.prepare('SELECT status FROM jobs').get().status, 'queued');
  const committed = spawnSync(process.execPath, ['--input-type=module', '-e', script, store.dataDir, 'after'], { encoding: 'utf8', timeout: 15000 });
  assert.equal(committed.status, 74, committed.stderr);
  assert.equal(settings().fairQuota.reservations.queued.status, 'reserved');
  assert.equal(sqlite.prepare('SELECT status FROM jobs').get().status, 'running');
  assert.equal(JSON.parse(sqlite.prepare('SELECT data FROM accounts').get().data).inFlight, 1);
  assert.equal(JSON.parse(sqlite.prepare('SELECT data FROM ledger').get().data).attempt.id, 'attempt:queued:1');
  assert.equal(sqlite.pragma('integrity_check', { simple: true }), 'ok');
});
