import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import { migrate } from 'drizzle-orm/better-sqlite3/migrator';
import { readMigrationFiles, type MigrationMeta } from 'drizzle-orm/migrator';

interface TableColumn { cid: number; name: string; type: string; notnull: number; dflt_value: string | null; pk: number }
interface IndexRow { name: string; unique: number; origin: 'c' | 'u' | 'pk'; partial: number }
interface IndexColumn { name: string | null; desc: number; coll: string | null; key: number }

export const storageMigrationsDirectory = fileURLToPath(new URL('./migrations/', import.meta.url));
const migrationTable = '__drizzle_migrations';
const legacyJobColumns = new Set(['cost', 'stats_excluded', 'error_loggable']);

export function migrateStorage(sqlite: Database.Database, migrationsFolder = storageMigrationsDirectory) {
  const migrations = readMigrationFiles({ migrationsFolder });
  const hasJournal = sqlite.prepare("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?").get(migrationTable);
  if (!hasJournal || !sqlite.prepare(`SELECT 1 FROM ${migrationTable} LIMIT 1`).get()) {
    adoptLegacySchema(sqlite, migrations[0]);
  }
  // 稳定版迁移器只按时间跳过旧文件；这里拒绝改写历史及用旧程序打开更高版本的库。
  const expected = new Map<number | null, string>(migrations.map(item => [item.folderMillis, item.hash]));
  for (const applied of sqlite.prepare<[], { hash: string; created_at: number | null }>(`SELECT hash, created_at FROM ${migrationTable}`).all()) {
    if (expected.get(applied.created_at) !== applied.hash) {
      throw new Error('SQLite migration history differs from this release; restore the matching migration files before starting.');
    }
  }
  migrate(drizzle(sqlite), { migrationsFolder });
}

function adoptLegacySchema(sqlite: Database.Database, baseline: MigrationMeta) {
  // 只在首次接入时建立内存参考库；实际结构来自已审阅的基线 SQL，不再维护第二套 DDL。
  const reference = new Database(':memory:');
  try {
    for (const statement of baseline.sql) reference.exec(statement);
    const tables = reference.prepare<[], { name: string }>("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name").all();
    sqlite.transaction(() => {
      for (const { name } of tables) {
        const existing = sqlite.prepare<[string], { type: string }>('SELECT type FROM sqlite_schema WHERE name = ?').get(name);
        if (!existing) continue;
        if (existing.type !== 'table') throw new Error(`SQLite baseline mismatch: ${name} is not a table.`);
        const actualColumns = columns(sqlite, name), expectedColumns = columns(reference, name);
        const missing = expectedColumns.filter(column => !actualColumns.some(actual => actual.name === column.name));
        if (missing.some(column => name !== 'jobs' || !legacyJobColumns.has(column.name))) {
          throw new Error(`SQLite baseline mismatch: ${name} has unsupported missing columns.`);
        }
        assertSame(actualColumns, expectedColumns.filter(column => !missing.includes(column)), `${name} columns`);
        const actualIndexes = indexes(sqlite, name);
        for (const expectedIndex of indexes(reference, name)) {
          const actualIndex = actualIndexes.find(index => index.name === expectedIndex.name);
          if (actualIndex) assertSame(actualIndex, expectedIndex, expectedIndex.name);
        }
        // 支持旧版逐步增加过的三个统计列；字段定义取自基线，不从模型重新推测历史。
        for (const column of missing) {
          sqlite.exec(`ALTER TABLE jobs ADD COLUMN ${column.name} ${column.type} NOT NULL DEFAULT ${column.dflt_value}`);
        }
      }
      for (const statement of baseline.sql) sqlite.exec(statement);
      sqlite.exec(`CREATE TABLE IF NOT EXISTS ${migrationTable} (id SERIAL PRIMARY KEY, hash text NOT NULL, created_at numeric)`);
      sqlite.prepare(`INSERT INTO ${migrationTable} (hash, created_at) VALUES (?, ?)`).run(baseline.hash, baseline.folderMillis);
    }).immediate();
  } finally {
    reference.close();
  }
}

function columns(sqlite: Database.Database, table: string) {
  return sqlite.prepare<[], TableColumn>(`PRAGMA table_info(${table})`).all()
    .map(({ cid, ...column }) => column).sort((a, b) => a.name.localeCompare(b.name));
}

function indexes(sqlite: Database.Database, table: string) {
  return sqlite.prepare<[], IndexRow>(`PRAGMA index_list(${table})`).all().filter(index => index.origin === 'c').map(index => {
    const definition = sqlite.prepare<[string], { sql: string }>('SELECT sql FROM sqlite_schema WHERE name = ?').get(index.name)!.sql;
    return { name: index.name, unique: index.unique, partial: index.partial,
      columns: sqlite.prepare<[string], IndexColumn>('SELECT name, "desc", coll, "key" FROM pragma_index_xinfo(?)').all(index.name)
        .map(({ name, desc, coll, key }) => ({ name, desc, coll, key })),
      predicate: predicateTokens(definition.split(/\bWHERE\b/i)[1] || '', table) };
  });
}

function predicateTokens(expression: string, table: string) {
  // 比较标识符和空白时忽略 SQL 的书写差异；字符串常量保持原样。
  const tokens = (expression.match(/'(?:''|[^'])*'|"(?:""|[^"])*"|`[^`]*`|\w+|[^\s]/g) || [])
    .map(token => token.startsWith("'") ? token : token.replace(/["`]/g, '').toLowerCase());
  return tokens.filter((token, index) => !(token === table && tokens[index + 1] === '.')
    && !(token === '.' && tokens[index - 1] === table)).join(' ');
}

function assertSame(actual: unknown, expected: unknown, label: string) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`SQLite baseline mismatch: ${label}; inspect a backup before migrating.`);
  }
}
