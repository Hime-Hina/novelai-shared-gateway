import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { readRelationalCommit } from '../events/relational-event-store.ts';
import { RelationalInputStore } from './relational-input-store.ts';
import { RelationalCommitReplayer } from './relational-replay.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageFormat,
  assertRelationalStorageReady,
} from './relational-storage.ts';
import {
  openRelationalSnapshot,
  readRelationalRecoveryState,
  relationalPrivateStateEmpty,
  relationalPrivateStateSha256,
  relationalPrivateTables,
  relationalSnapshotSha256,
  restoreRelationalPrivateSnapshot,
  sameRelationalPrivateState,
  sameRelationalRows,
} from './relational-private-restore.ts';
import { RelationalReplicaStore } from './relational-replica-store.ts';
import { readRelationalArtifactContent } from './relational-replica-io.ts';
import { RelationalGenerationStore } from './relational-generation-store.ts';
import { RelationalConfigurationStore } from './relational-configuration-store.ts';
import { readPreparedStoredResultContent } from './result-files.ts';

interface RecoverySource {
  sourcePath: string;
  sourceSha256: string;
  historyId: string;
  finalCommitSeq: string;
  replicas: Parameters<typeof readRelationalArtifactContent>[2];
}

function invalid(message: string): never {
  throw new Error(`Relational ready validation failed: ${message}`);
}

function assertCut(sqlite: Database.Database, expected: RecoverySource, building = false) {
  assertRelationalStorageFormat(sqlite);
  new RelationalConfigurationStore(sqlite).readCurrent();
  const state = readRelationalRecoveryState(sqlite);
  if (
    state.history_id !== expected.historyId ||
    state.last_commit_seq !== expected.finalCommitSeq ||
    (building && state.status !== 'building')
  ) {
    invalid('the selected history and complete final cut must match.');
  }
  if (
    sqlite.prepare<[], { integrity_check: string }>('PRAGMA integrity_check').get()
      ?.integrity_check !== 'ok' ||
    sqlite.prepare('PRAGMA foreign_key_check').get()
  ) {
    invalid('database integrity or foreign keys.');
  }
}

// 枚举唯一 DDL 的全部表，私有列只有原私有 owner 能例外；新表不会静默漏核对。
function assertSameTables(
  source: Database.Database,
  target: Database.Database,
  includePrivate: boolean,
) {
  const privateNames = new Set<string>(relationalPrivateTables.map(([name]) => name));
  for (const { name } of source
    .prepare<[], { name: string }>(
      "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name",
    )
    .iterate()) {
    if (!includePrivate && privateNames.has(name)) {
      continue;
    }
    const columns = source.prepare(`PRAGMA table_info("${name}")`).all() as {
      name: string;
      pk: number;
    }[];
    const selected = columns.filter(
      (column) =>
        !(name === 'projection_state' && column.name === 'status') &&
        !(
          !includePrivate &&
          name === 'members' &&
          ['auth_epoch', 'note_json'].includes(column.name)
        ),
    );
    const keys = columns.filter((column) => column.pk).sort((a, b) => a.pk - b.pk);
    if (!keys.length) {
      invalid('a recovery table has no stable identity.');
    }
    const query = `SELECT ${selected.map((column) => `"${column.name}"`).join(',')} FROM "${name}" ORDER BY ${keys.map((column) => `"${column.name}"`).join(',')}`;
    if (!sameRelationalRows(source, target, query)) {
      invalid(`table ${name} differs from complete replay.`);
    }
  }
}

// 离线恢复安装：对象 IO 不持有 SQL 事务；最终保存点重验全部事实、投影与当前私有状态。
export async function verifyAndInstallRelationalStorage(
  target: Database.Database,
  expected: RecoverySource,
) {
  if (target.inTransaction) {
    invalid('offline installation cannot hold a transaction across object IO.');
  }
  if (
    typeof expected.historyId !== 'string' ||
    !expected.historyId.trim() ||
    !/^[1-9][0-9]*$/.test(expected.finalCommitSeq) ||
    BigInt(expected.finalCommitSeq) > 9_223_372_036_854_775_807n
  ) {
    invalid('an explicit nonempty completed history and final cut are required.');
  }
  assertCut(target, expected, true);
  const source = openRelationalSnapshot(target, expected.sourcePath, expected.sourceSha256);
  let directory: string | undefined, shadow: Database.Database | undefined;
  try {
    assertCut(source, expected);
    directory = mkdtempSync(path.join(tmpdir(), 'novelai-relational-ready-'));
    shadow = new Database(path.join(directory, 'shadow.sqlite'));
    initializeRelationalStorage(shadow, expected.historyId);
    const rebuilt = shadow,
      replayer = new RelationalCommitReplayer(rebuilt),
      inputs = new RelationalInputStore(source),
      page = source.prepare<[bigint], { seq: string }>(
        'SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE commit_seq>? ORDER BY commit_seq LIMIT 128',
      );
    let after = 0n;
    source.transaction(() => {
      for (;;) {
        const rows = page.all(after);
        if (!rows.length) {
          break;
        }
        for (const { seq } of rows) {
          rebuilt.transaction(() =>
            replayer.replayCommit(readRelationalCommit(source, seq), inputs),
          )();
          after = BigInt(seq);
        }
      }
      rebuilt.transaction(() =>
        restoreRelationalPrivateSnapshot(rebuilt, source.name, expected.sourceSha256),
      )();
      assertCut(rebuilt, expected, true);
      assertSameTables(source, rebuilt, true);
    })();

    const privateBefore = target.transaction(() => {
      assertCut(target, expected, true);
      assertSameTables(rebuilt, target, false);
      if (!relationalPrivateStateEmpty(target) && !sameRelationalPrivateState(rebuilt, target)) {
        invalid('current private authentication differs from the selected snapshot.');
      }
      return relationalPrivateStateSha256(target);
    })();

    const replicas = new RelationalReplicaStore(source);
    let artifactCursor: string | null = null;
    let artifactsVerified = 0;
    for (;;) {
      const artifacts = replicas.readRecoverableArtifacts(artifactCursor, 100);
      if (!artifacts.length) {
        break;
      }
      for (const artifact of artifacts) {
        await readRelationalArtifactContent(source, artifact.id, expected.replicas);
        artifactCursor = artifact.id;
        artifactsVerified++;
      }
    }

    const generations = new RelationalGenerationStore(source);
    let pendingCursor = '',
      preparedResultsVerified = 0;
    for (;;) {
      const jobs = generations.readPendingGenerationResultIds(pendingCursor);
      if (!jobs.length) {
        break;
      }
      for (const jobId of jobs) {
        const { descriptor } = generations.readGenerationResultPreparation(jobId);
        if (!(await readPreparedStoredResultContent(expected.replicas.dataDir, descriptor))) {
          invalid('a pending prepared result has no complete final or partial content.');
        }
        pendingCursor = jobId;
        preparedResultsVerified++;
      }
    }

    if (target.inTransaction) {
      invalid('the target gained a transaction during object IO.');
    }
    target
      .transaction(() => {
        assertCut(target, expected, true);
        assertCut(source, expected);
        if (relationalPrivateStateSha256(target) !== privateBefore) {
          invalid('current private authentication changed during object IO.');
        }
        assertSameTables(source, rebuilt, true);
        assertSameTables(rebuilt, target, false);
        restoreRelationalPrivateSnapshot(target, source.name, expected.sourceSha256);
        assertSameTables(rebuilt, target, true);
        target
          .prepare("UPDATE projection_state SET status='ready' WHERE id=1 AND status='building'")
          .run();
        assertRelationalStorageReady(target);
        assertSameTables(rebuilt, target, true);
        if (relationalSnapshotSha256(source.name) !== expected.sourceSha256) {
          invalid('the selected source snapshot changed.');
        }
        // 临时私有副本清理失败也须回滚安装，不能先提交 ready 再失败退出。
        rebuilt.close();
        shadow = undefined;
        rmSync(directory!, { recursive: true, force: true });
        directory = undefined;
      })
      .immediate();
    return {
      historyId: expected.historyId,
      finalCommitSeq: expected.finalCommitSeq,
      artifactsVerified,
      preparedResultsVerified,
    };
  } finally {
    shadow?.close();
    source.close();
    if (directory) {
      rmSync(directory, { recursive: true, force: true });
    }
  }
}
