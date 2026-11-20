import { mkdtemp, mkdir, writeFile, rm, realpath, lstat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import Database from 'better-sqlite3';
import { describeRetainedSnapshot } from '../../scripts/retained-sources.mjs';
import {
  initializeRetainedSources,
  beginRetainedCapture,
  bindRetainedCapture,
  publishRetainedLocal,
  checkpointRetainedSources,
  retainedFileSha256,
  adoptRetainedInventory,
  readRetainedSourceProtection,
} from '../../server/storage/retained-sources.ts';

// Real owner path: snapshot -> describe -> immutable manifest/binding -> checkpoint -> adoption.
// Consumers mock only their concrete R2 client's listBackups transport when a remote scope exists.
export async function createRetainedReleaseFixture(t, sqlite, dataDir, { r2 = null } = {}) {
  const root = await mkdtemp(path.join(tmpdir(), 'nai-retained-release-')),
    registry = path.join(root, 'registry'),
    backups = path.join(root, 'backups'),
    initialized = await initializeRetainedSources(registry);
  await mkdir(backups);
  t.after(async () => {
    const actual = await realpath(root),
      parent = await realpath(tmpdir()),
      info = await lstat(root);
    if (
      !info.isDirectory() ||
      info.isSymbolicLink() ||
      path.dirname(actual) !== parent ||
      !path.basename(actual).startsWith('nai-retained-release-')
    ) {
      throw new Error('Refuse cleanup outside the owned retained-release temporary root.');
    }
    await rm(root, { recursive: true, force: true });
  });
  async function addSnapshot(database = sqlite, originalDataDir = dataDir) {
    const snapshot = path.join(backups, 'daily-' + randomUUID()),
      capture = await beginRetainedCapture(registry, 'daily', snapshot);
    await mkdir(snapshot);
    await mkdir(path.join(snapshot, 'data'));
    await mkdir(path.join(snapshot, 'config'));
    const filename = path.join(snapshot, 'data/library.sqlite');
    await database.backup(filename);
    const sealed = new Database(filename);
    sealed.pragma('wal_checkpoint(TRUNCATE)');
    sealed.pragma('journal_mode=DELETE');
    sealed.close();
    const scope = path.join(snapshot, 'config/retained-inventory-scope.json');
    const catalog = await readRetainedSourceProtection(registry),
      archiveRoots = [
        ...new Set(
          catalog.sources.flatMap((row) =>
            row.holds
              .filter((hold) => hold.kind === 'archive' && hold.state !== 'retired')
              .map((hold) => path.dirname(hold.location)),
          ),
        ),
      ];
    await writeFile(
      scope,
      JSON.stringify({
        format: 'novelai-retained-inventory-scope-v1',
        registryId: initialized.registryId,
        localRoots: [backups],
        archiveRoots,
        r2Namespaces: r2 ? [`r2://${r2.config.accountId}/${r2.config.bucket}`] : [],
      }),
      { flag: 'wx', mode: 0o600 },
    );
    const dependencies = path.join(snapshot, 'retained-dependencies.jsonl'),
      binding = await describeRetainedSnapshot(filename, dependencies, originalDataDir),
      manifest = path.join(snapshot, 'manifest.json');
    await writeFile(
      manifest,
      JSON.stringify({
        files: {
          'data/library.sqlite': binding.sourceSha256,
          'retained-dependencies.jsonl': binding.dependenciesSha256,
          'config/retained-inventory-scope.json': await retainedFileSha256(scope),
        },
        retainedSource: { ...capture, binding },
      }),
      { flag: 'wx', mode: 0o600 },
    );
    await bindRetainedCapture(
      registry,
      capture.sourceId,
      manifest,
      { ...binding, manifestSha256: await retainedFileSha256(manifest) },
      dependencies,
    );
    await publishRetainedLocal(registry, capture.sourceId, snapshot);
    const checkpoint = path.join(root, 'checkpoint-' + randomUUID()),
      proof = await checkpointRetainedSources(registry, checkpoint),
      selected = {
        checkpoint,
        checkpointSha256: proof.sha256,
        scope,
        scopeSha256: await retainedFileSha256(scope),
      };
    await adoptRetainedInventory(registry, selected, { r2 });
    return { snapshot, sourceId: capture.sourceId, holdId: capture.holdId, selected };
  }
  const first = await addSnapshot();
  return { registry, addSnapshot, first, root };
}
