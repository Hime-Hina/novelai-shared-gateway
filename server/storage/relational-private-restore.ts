import { createHash } from 'node:crypto';
import { closeSync, existsSync, lstatSync, openSync, readSync, statSync } from 'node:fs';
import path from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import Database from 'better-sqlite3';
import { readRelationalCommit } from '../events/relational-event-store.ts';
import { assertRelationalStorageFormat, RELATIONAL_REDUCER_VERSION } from './relational-storage.ts';

const privateTables = [
  ['password_credentials', 'member_id'],
  ['api_keys', 'member_id'],
  ['discord_bindings', 'member_id'],
  ['discord_invites', 'member_id'],
  ['member_auth_state', 'id'],
  ['member_sessions', 'token_hash'],
  ['upstream_credentials', 'account_id'],
] as const;

export function relationalPrivateStateEmpty(sqlite: Database.Database): boolean {
  return (
    !sqlite.prepare('SELECT 1 FROM members WHERE auth_epoch IS NOT NULL LIMIT 1').get() &&
    privateTables.every(([table]) => !sqlite.prepare(`SELECT 1 FROM ${table} LIMIT 1`).get())
  );
}

function invalid(): never {
  throw new Error('Private snapshot does not match the offline relational recovery target.');
}

function snapshotDigest(filename: string): string {
  // readonly 打开 WAL header 仍可能创建 sidecar；这里只接受可按原字节核验的封存单文件。
  if (
    !lstatSync(filename).isFile() ||
    ['-wal', '-shm', '-journal'].some((suffix) => existsSync(filename + suffix))
  ) {
    invalid();
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
      invalid();
    }
    const hash = createHash('sha256').update(header),
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

function sameRows(source: Database.Database, target: Database.Database, query: string): boolean {
  const left = source.prepare(query).safeIntegers().iterate(),
    right = target.prepare(query).safeIntegers().iterate();
  try {
    for (;;) {
      const a = left.next(),
        b = right.next();
      if (a.done || b.done) {
        return a.done === b.done;
      }
      if (!isDeepStrictEqual(a.value, b.value)) {
        return false;
      }
    }
  } finally {
    left.return?.();
    right.return?.();
  }
}

function recoveryState(sqlite: Database.Database) {
  const state = sqlite
    .prepare<
      [],
      { history_id: string; reducer_version: number; last_commit_seq: string; status: string }
    >(
      'SELECT history_id,reducer_version,CAST(last_commit_seq AS TEXT) AS last_commit_seq,status FROM projection_state WHERE id=1',
    )
    .get();
  const latest = sqlite
    .prepare<[], { value: string }>(
      'SELECT CAST(COALESCE(MAX(commit_seq),0) AS TEXT) AS value FROM commits',
    )
    .get()!.value;
  if (
    !state ||
    state.reducer_version !== RELATIONAL_REDUCER_VERSION ||
    state.last_commit_seq !== latest ||
    !['building', 'ready'].includes(state.status)
  ) {
    invalid();
  }
  return state;
}

function assertBusinessCut(source: Database.Database, target: Database.Database): void {
  const original = recoveryState(source),
    rebuilt = recoveryState(target);
  if (
    rebuilt.status !== 'building' ||
    original.history_id !== rebuilt.history_id ||
    original.last_commit_seq !== rebuilt.last_commit_seq
  ) {
    invalid();
  }
  const page = source.prepare<[bigint], { seq: string }>(
    'SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE commit_seq>? ORDER BY commit_seq LIMIT 128',
  );
  let after = 0n;
  for (;;) {
    const rows = page.all(after);
    if (!rows.length) {
      break;
    }
    for (const { seq } of rows) {
      if (
        BigInt(seq) !== after + 1n ||
        !isDeepStrictEqual(readRelationalCommit(source, seq), readRelationalCommit(target, seq))
      ) {
        invalid();
      }
      after = BigInt(seq);
    }
  }
  if (String(after) !== original.last_commit_seq) {
    invalid();
  }
  for (const query of [
    'SELECT * FROM stream_heads ORDER BY stream_id',
    'SELECT id,display_name,status,accepting_jobs,admission_epoch,created_at,source_event_id FROM members ORDER BY id',
    'SELECT * FROM upstream_accounts ORDER BY id',
  ]) {
    if (!sameRows(source, target, query)) {
      invalid();
    }
  }
}

// 只恢复选定备份的当前秘密；相同业务切点不代表相同私有快照，非空目标不能被另一份覆盖。
export function restoreRelationalPrivateSnapshot(
  target: Database.Database,
  sourcePath: string,
  expectedSha256: string,
): { restored: boolean; members: number; privateRows: number } {
  if (!target.inTransaction) {
    throw new Error('Private restoration requires the owning offline transaction.');
  }
  assertRelationalStorageFormat(target);
  if (!/^[a-f0-9]{64}$/.test(expectedSha256)) {
    invalid();
  }
  const filename = path.resolve(sourcePath);
  if (target.name !== ':memory:' && existsSync(target.name)) {
    const sourceFile = statSync(filename),
      targetFile = statSync(target.name);
    if (sourceFile.dev === targetFile.dev && sourceFile.ino === targetFile.ino) {
      invalid();
    }
  }
  if (snapshotDigest(filename) !== expectedSha256) {
    invalid();
  }
  const source = new Database(filename, { readonly: true, fileMustExist: true });
  try {
    source.pragma('foreign_keys=ON');
    source.pragma('query_only=ON');
    assertRelationalStorageFormat(source);
    return target.transaction(() =>
      source.transaction(() => {
        assertBusinessCut(source, target);
        if (source.prepare('PRAGMA foreign_key_check').get()) {
          invalid();
        }
        const epochs = 'SELECT id,auth_epoch FROM members ORDER BY id',
          privateQueries = privateTables.map(
            ([table, key]) => `SELECT * FROM ${table} ORDER BY ${key}`,
          );
        const equal =
          sameRows(source, target, epochs) &&
          privateQueries.every((query) => sameRows(source, target, query));
        const empty = relationalPrivateStateEmpty(target);
        if (!equal && !empty) {
          invalid();
        }
        let privateRows = 0;
        if (!equal) {
          const update = target.prepare('UPDATE members SET auth_epoch=? WHERE id=?');
          for (const row of source
            .prepare<[], { id: string; auth_epoch: string | null }>(epochs)
            .iterate()) {
            update.run(row.auth_epoch, row.id);
          }
          for (const [table] of privateTables) {
            const fields = source.prepare(`PRAGMA table_info(${table})`).all() as {
                name: string;
              }[],
              columns = fields.map(({ name }) => name);
            const insert = target.prepare(
              `INSERT INTO ${table}(${columns.join(',')}) VALUES(${columns.map(() => '?').join(',')})`,
            );
            for (const row of source.prepare(`SELECT * FROM ${table}`).safeIntegers().iterate()) {
              insert.run(...columns.map((column) => (row as Record<string, unknown>)[column]));
              privateRows++;
            }
          }
        }
        if (
          target.prepare('PRAGMA foreign_key_check').get() ||
          snapshotDigest(filename) !== expectedSha256
        ) {
          invalid();
        }
        const members = source
          .prepare<[], { count: number }>('SELECT COUNT(*) AS count FROM members')
          .get()!.count;
        return { restored: !equal, members, privateRows };
      })(),
    )();
  } finally {
    source.close();
  }
}
