import { chmod, link, lstat, mkdtemp, open, realpath, rm, unlink } from 'node:fs/promises';
import path from 'node:path';
import { parseArgs } from 'node:util';
import Database from 'better-sqlite3';
import { readRelationalCommit } from '../server/events/relational-event-store.ts';
import { RelationalInputStore } from '../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../server/storage/relational-storage.ts';
import {
  openRelationalSnapshot,
  readRelationalRecoveryState,
  relationalSnapshotSha256,
} from '../server/storage/relational-private-restore.ts';
import { verifyAndInstallRelationalStorage } from '../server/storage/relational-ready.ts';
import { R2Storage, r2Config } from '../server/storage/r2-storage.ts';
import { PikPakStorage, pikpakConfig } from '../server/storage/pikpak-storage.ts';

const names = ['source', 'sha256', 'history', 'cut', 'output', 'data-dir'];
const sidecars = ['', '-wal', '-shm', '-journal'];

async function assertAbsent(filename) {
  try {
    await lstat(filename);
  } catch (error) {
    if (error.code === 'ENOENT') {
      return;
    }
    throw error;
  }
  throw new Error('Destination exists.');
}

async function assertOutputAbsent(output) {
  for (const suffix of sidecars) {
    await assertAbsent(output + suffix);
  }
}

// Only the directory created directly under the selected output parent can be removed.
async function removeStage(parent, stage) {
  if (
    path.dirname(path.resolve(stage)) !== parent ||
    !path.basename(stage).startsWith('.restore-relational-')
  ) {
    throw new Error('Invalid staging directory.');
  }
  await rm(stage, { recursive: true, force: true });
}

let phase = 'arguments',
  parent,
  stage,
  target,
  source,
  output,
  published = false,
  identity;
try {
  const { values, tokens } = parseArgs({
    strict: true,
    allowPositionals: false,
    tokens: true,
    options: Object.fromEntries(names.map((name) => [name, { type: 'string' }])),
  });
  if (
    names.some(
      (name) =>
        !values[name]?.trim() ||
        tokens.filter((token) => token.kind === 'option' && token.name === name).length !== 1,
    ) ||
    !/^[a-f0-9]{64}$/.test(values.sha256) ||
    !/^[1-9][0-9]*$/.test(values.cut) ||
    BigInt(values.cut) > 9_223_372_036_854_775_807n
  ) {
    throw new Error('Six explicit recovery parameters are required.');
  }

  phase = 'paths';
  const requestedOutput = path.resolve(values.output);
  parent = await realpath(path.dirname(requestedOutput));
  output = path.join(parent, path.basename(requestedOutput));
  await assertOutputAbsent(output);
  const selected = {
    sourcePath: path.resolve(values.source),
    sourceSha256: values.sha256,
    historyId: values.history,
    finalCommitSeq: values.cut,
  };
  const sourceIdentity = await realpath(selected.sourcePath),
    sourceSpelling = path.join(
      await realpath(path.dirname(selected.sourcePath)),
      path.basename(selected.sourcePath),
    ),
    comparable = (filename) =>
      process.platform === 'win32' ? path.toNamespacedPath(filename).toLowerCase() : filename;
  if (
    [sourceIdentity, sourceSpelling].some((filename) =>
      sidecars.some((suffix) => comparable(output) === comparable(filename + suffix)),
    )
  ) {
    throw new Error('Destination overlaps the selected snapshot.');
  }
  const r2 = r2Config(),
    pikpak = pikpakConfig();
  const replicas = {
    dataDir: path.resolve(values['data-dir']),
    ...(r2 ? { r2: new R2Storage(r2) } : {}),
    ...(pikpak ? { pikpak: new PikPakStorage(pikpak) } : {}),
  };

  stage = await mkdtemp(path.join(parent, '.restore-relational-'));
  await chmod(stage, 0o700);
  const filename = path.join(stage, 'recovered.sqlite'),
    empty = await open(filename, 'wx', 0o600);
  await empty.close();
  target = new Database(filename);
  initializeRelationalStorage(target, selected.historyId);

  phase = 'source';
  source = openRelationalSnapshot(target, selected.sourcePath, selected.sourceSha256);
  const state = readRelationalRecoveryState(source);
  if (
    state.history_id !== selected.historyId ||
    state.last_commit_seq !== selected.finalCommitSeq
  ) {
    throw new Error('Selected recovery cut does not match.');
  }

  phase = 'replay';
  const replayer = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(source),
    page = source.prepare(
      'SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE commit_seq>? ORDER BY commit_seq LIMIT 128',
    );
  let after = 0n,
    commitsRestored = 0n;
  source.transaction(() => {
    for (;;) {
      const rows = page.all(after);
      if (!rows.length) {
        break;
      }
      for (const { seq } of rows) {
        target.transaction(() =>
          replayer.replayCommit(readRelationalCommit(source, seq), inputs),
        )();
        after = BigInt(seq);
        commitsRestored++;
      }
    }
  })();
  source.close();
  source = undefined;

  phase = 'verify';
  const verified = await verifyAndInstallRelationalStorage(target, { ...selected, replicas });

  phase = 'seal';
  target.pragma('wal_checkpoint(TRUNCATE)');
  target.pragma('journal_mode=DELETE');
  target.close();
  target = undefined;
  await chmod(filename, 0o600);
  // The owner checks sealed SQLite headers and absence of every SQLite sidecar.
  relationalSnapshotSha256(filename);
  if (relationalSnapshotSha256(selected.sourcePath) !== selected.sourceSha256) {
    throw new Error('Selected snapshot changed.');
  }
  const sealed = await open(filename, 'r+');
  try {
    await sealed.sync();
  } finally {
    await sealed.close();
  }
  identity = await lstat(filename, { bigint: true });

  phase = 'publish';
  await assertOutputAbsent(output);
  // Same-parent hard link is an atomic, exclusive publication; rename could overwrite.
  await link(filename, output);
  published = true;
  for (const suffix of sidecars.slice(1)) {
    await assertAbsent(output + suffix);
  }
  await removeStage(parent, stage);
  stage = undefined;
  console.log(
    JSON.stringify({
      restored: true,
      commitsRestored: String(commitsRestored),
      artifactsVerified: verified.artifactsVerified,
      preparedResultsVerified: verified.preparedResultsVerified,
    }),
  );
} catch {
  // Never expose owner exceptions: they can contain private diagnostics or IO credentials.
  let cleanupFailed = false;
  for (const handle of [source, target]) {
    try {
      if (handle?.open) {
        handle.close();
      }
    } catch {
      cleanupFailed = true;
    }
  }
  if (published) {
    try {
      const current = await lstat(output, { bigint: true }).catch((error) => {
        if (error.code !== 'ENOENT') {
          throw error;
        }
        return null;
      });
      if (current?.dev === identity.dev && current?.ino === identity.ino) {
        await unlink(output);
      }
    } catch {
      cleanupFailed = true;
    }
  }
  if (stage) {
    try {
      await removeStage(parent, stage);
    } catch {
      cleanupFailed = true;
    }
  }
  console.error(
    JSON.stringify({
      restored: false,
      code: 'RELATIONAL_RESTORE_FAILED',
      phase: cleanupFailed ? 'cleanup' : phase,
    }),
  );
  process.exitCode = 1;
}
