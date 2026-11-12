import { createHash } from 'node:crypto';
import Database from 'better-sqlite3';
import { RELATIONAL_SCHEMA_SQL } from './relational-schema.ts';

export const RELATIONAL_DATABASE_FORMAT = 'novelai-relational-v1';
export const RELATIONAL_WRITER_VERSION = 5;
export const RELATIONAL_REDUCER_VERSION = 1;
const schemaSha256 = createHash('sha256').update(RELATIONAL_SCHEMA_SQL).digest('hex');

function schema(sqlite: Database.Database): string {
  return JSON.stringify(
    sqlite
      .prepare(
        "SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY type, name",
      )
      .all(),
  );
}

let expectedSchema: string | undefined;
function referenceSchema(): string {
  if (expectedSchema === undefined) {
    const reference = new Database(':memory:');
    try {
      reference.exec(RELATIONAL_SCHEMA_SQL);
      expectedSchema = schema(reference);
    } finally {
      reference.close();
    }
  }
  return expectedSchema;
}

// 只初始化空目标；迁移/重放负责导入和完整核对，初始化绝不把半成品标为 ready。
export function initializeRelationalStorage(sqlite: Database.Database, historyId: string): void {
  if (typeof historyId !== 'string' || !historyId.trim()) {
    throw new Error('Relational storage requires an explicit history identity.');
  }
  if (sqlite.inTransaction) {
    throw new Error('Relational initialization requires its own transaction.');
  }
  sqlite.pragma('foreign_keys = ON');
  sqlite
    .transaction(() => {
      if (
        sqlite.prepare("SELECT 1 FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' LIMIT 1").get()
      ) {
        throw new Error('Relational initialization requires an empty target; migrate offline.');
      }
      sqlite.exec(RELATIONAL_SCHEMA_SQL);
      const insert = sqlite.prepare('INSERT INTO app_meta(key, value) VALUES (?, ?)');
      insert.run('database_format', RELATIONAL_DATABASE_FORMAT);
      insert.run('schema_sha256', schemaSha256);
      // 复用旧 writer 已有的迁移前拒绝门禁；不改旧 SQL/journal 或降低 hash guard。
      insert.run(
        'event_authority',
        JSON.stringify({
          version: RELATIONAL_WRITER_VERSION,
          minimumWriterVersion: RELATIONAL_WRITER_VERSION,
        }),
      );
      sqlite
        .prepare(
          "INSERT INTO projection_state(id, history_id, reducer_version, last_commit_seq, status) VALUES (1, ?, ?, 0, 'building')",
        )
        .run(historyId, RELATIONAL_REDUCER_VERSION);
    })
    .immediate();
}

// 只读就绪门禁；旧格式、未知格式、DDL 漂移和未完成的目标均不能进入新运行时。
export function assertRelationalStorageFormat(sqlite: Database.Database): void {
  const hasMeta = sqlite
    .prepare("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'app_meta'")
    .get();
  if (!hasMeta) {
    throw new Error('Unsupported database format; use the offline relational migration.');
  }
  const values = Object.fromEntries(
    sqlite
      .prepare<[], { key: string; value: string }>('SELECT key, value FROM app_meta')
      .all()
      .map(({ key, value }) => [key, value]),
  );
  if (
    values.database_format !== RELATIONAL_DATABASE_FORMAT ||
    values.schema_sha256 !== schemaSha256
  ) {
    throw new Error('Unsupported relational database format or schema version.');
  }
  const marker: unknown = JSON.parse(values.event_authority || 'null');
  if (
    !marker ||
    typeof marker !== 'object' ||
    !('version' in marker) ||
    marker.version !== RELATIONAL_WRITER_VERSION ||
    !('minimumWriterVersion' in marker) ||
    marker.minimumWriterVersion !== RELATIONAL_WRITER_VERSION ||
    schema(sqlite) !== referenceSchema()
  ) {
    throw new Error('Unsupported relational writer or installed schema.');
  }
  if (sqlite.pragma('foreign_keys', { simple: true }) !== 1) {
    throw new Error('Relational storage requires foreign_keys = ON.');
  }
}

export function assertRelationalStorageReady(sqlite: Database.Database): void {
  assertRelationalStorageFormat(sqlite);
  const state = sqlite
    .prepare<[], { status: string; reducer_version: number; last_commit_seq: string }>(
      'SELECT status, reducer_version, CAST(last_commit_seq AS TEXT) AS last_commit_seq FROM projection_state WHERE id = 1',
    )
    .get();
  const latest = sqlite
    .prepare<[], { value: string }>(
      'SELECT CAST(COALESCE(MAX(commit_seq), 0) AS TEXT) AS value FROM commits',
    )
    .get()!;
  if (
    state?.status !== 'ready' ||
    state.reducer_version !== RELATIONAL_REDUCER_VERSION ||
    state.last_commit_seq !== latest.value
  ) {
    throw new Error(
      'Relational projection is incomplete or incompatible; finish offline validation.',
    );
  }
  if (sqlite.prepare('PRAGMA foreign_key_check').get()) {
    throw new Error('Relational storage has broken foreign key references.');
  }
}
