import { cp, mkdtemp, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { migrateStorage } from '../../server/storage/sqlite-migrations.ts';
import { removeLegacyFreeImageTriggers } from '../../server/events/event-store.ts';

// A separate pre-event fixture keeps bootstrap's real history intact.
export async function createLegacyDatabaseFixture(sourceDataDir) {
  const relative = path.relative(path.resolve(tmpdir()), path.resolve(sourceDataDir));
  if (!relative || relative.startsWith('..') || path.isAbsolute(relative))
    throw new Error('Legacy fixtures must come from a synthetic temporary directory.');
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-legacy-fixture-'));
  const source = new Database(path.join(sourceDataDir, 'library.sqlite'), {
    readonly: true,
    fileMustExist: true,
  });
  const target = new Database(path.join(dataDir, 'library.sqlite'));
  try {
    migrateStorage(target);
    target.transaction(() => {
      removeLegacyFreeImageTriggers(target);
      for (const table of [
        'app_settings',
        'users',
        'accounts',
        'cards',
        'generation_requests',
        'generation_contexts',
        'generation_idempotency',
        'fair_quota_settlements',
        'free_image_usage',
        'member_sessions',
        'jobs',
        'images',
        'ledger',
        'app_records',
      ]) {
        const columns = source
          .prepare(`PRAGMA table_info(${table})`)
          .all()
          .map((row) => row.name);
        const insert = target.prepare(
          `INSERT INTO ${table}(${columns.join(',')}) VALUES(${columns.map(() => '?').join(',')})`,
        );
        for (const row of source.prepare(`SELECT * FROM ${table}`).iterate())
          insert.run(...columns.map((column) => row[column]));
      }
      for (const row of source
        .prepare(
          "SELECT key,value FROM app_meta WHERE key IN ('queue_sequence','member_session_owner','schema_version','last_migrated_at','generic_records_migrated_at')",
        )
        .iterate()) {
        target.prepare('INSERT INTO app_meta(key,value) VALUES(?,?)').run(row.key, row.value);
      }
    })();
  } finally {
    target.close();
    source.close();
  }
  const images = path.join(sourceDataDir, 'images');
  if (
    await stat(images).then(
      (value) => value.isDirectory(),
      (error) => {
        if (error.code === 'ENOENT') return false;
        throw error;
      },
    )
  ) {
    await cp(images, path.join(dataDir, 'images'), { recursive: true, errorOnExist: true });
  }
  return dataDir;
}
