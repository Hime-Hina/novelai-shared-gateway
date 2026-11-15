import { readFileSync, realpathSync } from 'node:fs';
import { open, readFile } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  initializeRetainedSources,
  readRetainedSourceProtection,
  readRetainedArchiveBinding,
  beginRetainedCapture,
  bindRetainedCapture,
  publishRetainedLocal,
  publishRetainedArchive,
  checkpointRetainedSources,
  beginRetainedRetirement,
  confirmRetainedRetirement,
  restoreRetainedSources,
  retainedFileSha256,
  inspectRetainedLease,
  recoverRetainedLease,
} from '../server/storage/retained-sources.ts';
// 只派生原owner声明；不实读/删除对象，不把未知旧格式解释成空依赖。
export async function describeRetainedSnapshot(sourceFile, output, dataDir) {
  const [
    { default: Database },
    { openRelationalSnapshot, readRelationalRecoveryState },
    { RelationalReplicaStore },
    { RelationalGenerationStore },
  ] = await Promise.all([
    import('better-sqlite3'),
    import('../server/storage/relational-private-restore.ts'),
    import('../server/storage/relational-replica-store.ts'),
    import('../server/storage/relational-generation-store.ts'),
  ]);
  const sourceSha256 = await retainedFileSha256(sourceFile),
    target = new Database(':memory:');
  let source;
  try {
    source = openRelationalSnapshot(target, sourceFile, sourceSha256);
  } catch {
  } finally {
    target.close();
  }
  const file = await open(output, 'wx', 0o600);
  const emit = async (value) => file.write(JSON.stringify(value) + '\n');
  let description = {
    format: 'opaque',
    sourceSha256,
    historyId: null,
    finalCommitSeq: null,
    dependencies: 'unknown',
  };
  try {
    if (!source) {
      await emit({ kind: 'unknown-dependencies', sourceSha256 });
    } else {
      const state = readRelationalRecoveryState(source),
        replicas = new RelationalReplicaStore(source),
        generations = new RelationalGenerationStore(source);
      let after = '';
      for (;;) {
        const rows = replicas.readRetainedArtifactDependencies(after);
        if (!rows.length) {
          break;
        }
        for (const row of rows) {
          await emit({ kind: 'artifact', ...row });
          after = row.id;
        }
      }
      after = '';
      for (;;) {
        const jobs = generations.readPendingGenerationResultIds(after);
        if (!jobs.length) {
          break;
        }
        for (const jobId of jobs) {
          const { descriptor, event, attemptId } =
            generations.readGenerationResultDependency(jobId);
          await emit({
            kind: 'prepared-result',
            jobId,
            attemptId,
            sourceEventId: event.eventId,
            dataDir: dataDir ? path.resolve(dataDir) : null,
            descriptor,
          });
          after = jobId;
        }
      }
      description = {
        format: 'relational',
        sourceSha256,
        historyId: state.history_id,
        finalCommitSeq: state.last_commit_seq,
        dependencies: dataDir ? 'known' : 'unknown',
      };
    }
    await file.sync();
  } finally {
    await file.close();
    source?.close();
  }
  if ((await retainedFileSha256(sourceFile)) !== sourceSha256) {
    throw Error('Selected snapshot changed.');
  }
  return { ...description, dependenciesSha256: await retainedFileSha256(output) };
}
async function main() {
  const [mode, registry, expectedAuditSha256, ...extra] = process.argv.slice(2);
  if (
    !mode ||
    !registry ||
    extra.length ||
    (expectedAuditSha256 !== undefined && mode !== 'archive')
  ) {
    throw Error('Mode and registry are required.');
  }
  const raw = readFileSync(0, 'utf8');
  if (Buffer.byteLength(raw) > 65536) {
    throw Error('Metadata input too large.');
  }
  const request = raw ? JSON.parse(raw) : {};
  let result;
  if (mode === 'init') {
    result = await initializeRetainedSources(registry);
  } else if (mode === 'read') {
    result = await readRetainedSourceProtection(registry, request.checkpointSha256);
  } else if (mode === 'begin') {
    result = await beginRetainedCapture(registry, request.category, request.location);
  } else if (mode === 'describe') {
    result = await describeRetainedSnapshot(request.source, request.output, request.dataDir);
  } else if (mode === 'bind') {
    const manifest = JSON.parse(await readFile(request.manifest, 'utf8'));
    const binding = {
      ...manifest.retainedSource.binding,
      manifestSha256: await retainedFileSha256(request.manifest),
    };
    if (
      manifest.retainedSource.sourceId !== request.sourceId ||
      manifest.files['data/library.sqlite'] !== binding.sourceSha256 ||
      manifest.files['retained-dependencies.jsonl'] !== binding.dependenciesSha256
    ) {
      throw Error('Capture binding mismatch.');
    }
    result = await bindRetainedCapture(
      registry,
      request.sourceId,
      request.manifest,
      binding,
      request.dependencies,
      request.location,
    );
    if (request.checkpoint) {
      result = await checkpointRetainedSources(registry, request.checkpoint);
    }
  } else if (mode === 'publish') {
    result = await publishRetainedLocal(registry, request.sourceId, request.location);
  } else if (mode === 'checkpoint') {
    result = await checkpointRetainedSources(registry, request.output);
  } else if (mode === 'restore') {
    result = await restoreRetainedSources(registry, request.checkpoint, request.sha256);
  } else if (mode === 'lease') {
    result = await inspectRetainedLease(registry);
  } else if (mode === 'recover-lease') {
    result = await recoverRetainedLease(registry, request);
  } else if (mode === 'retire-local') {
    const current = await readRetainedSourceProtection(registry);
    let row = current.sources.find((s) =>
      s.holds.some(
        (h) => h.kind === 'local' && h.state !== 'retired' && h.location === request.location,
      ),
    );
    if (!row) {
      const manifestFile = path.join(request.location, 'manifest.json');
      const manifest = JSON.parse(await readFile(manifestFile, 'utf8'));
      const sourceSha256 = await retainedFileSha256(
        path.join(request.location, 'data/library.sqlite'),
      );
      if (manifest.files['data/library.sqlite'] !== sourceSha256) {
        throw Error('Existing snapshot mismatch.');
      }
      const created = await beginRetainedCapture(registry, 'daily', request.location);
      await bindRetainedCapture(registry, created.sourceId, manifestFile, {
        format: 'opaque',
        sourceSha256,
        manifestSha256: await retainedFileSha256(manifestFile),
        dependenciesSha256: null,
        dependencies: 'unknown',
        historyId: null,
        finalCommitSeq: null,
      });
      await publishRetainedLocal(registry, created.sourceId, request.location);
      row = (await readRetainedSourceProtection(registry)).sources.find(
        (s) => s.id === created.sourceId,
      );
    }
    const hold = row.holds.find((h) => h.kind === 'local' && h.location === request.location);
    result = {
      sourceId: row.id,
      holdId: hold.id,
      hold: await beginRetainedRetirement(registry, row.id, hold.id),
    };
  } else if (mode === 'retire-candidate') {
    const catalog = await readRetainedSourceProtection(registry);
    const source = catalog.sources.find((row) => row.id === request.sourceId);
    if (
      !source ||
      request.registryId !== catalog.registryId ||
      request.manifestSha256 !== source.binding?.manifestSha256
    ) {
      throw Error('Explicit retained source selection differs.');
    }
    result = await beginRetainedRetirement(
      registry,
      request.sourceId,
      request.holdId,
      'explicit-retirement',
    );
  } else if (mode === 'confirm-local' || mode === 'confirm-absent') {
    const catalog = await readRetainedSourceProtection(registry);
    const hold = catalog.sources
      .find((row) => row.id === request.sourceId)
      ?.holds.find((row) => row.id === request.holdId);
    if (
      !hold ||
      hold.kind === 'remote' ||
      typeof request.location !== 'string' ||
      (mode === 'confirm-local' && hold.kind !== 'local')
    ) {
      throw Error('Remote retirement needs its actual IO owner.');
    }
    result = await confirmRetainedRetirement(
      registry,
      request.sourceId,
      request.holdId,
      request.location,
    );
  } else if (mode === 'archive') {
    const created = await beginRetainedCapture(registry, 'archive', path.resolve(request.source));
    const binding = await readRetainedArchiveBinding(
      request.source,
      request.manifest,
      request.dependencies,
      expectedAuditSha256,
    );
    await bindRetainedCapture(
      registry,
      created.sourceId,
      request.manifest,
      binding,
      request.dependencies,
    );
    result = await publishRetainedArchive(
      registry,
      created.sourceId,
      path.resolve(request.source),
      request.manifest,
    );
  } else {
    throw Error('Unknown retained source operation.');
  }
  console.log(JSON.stringify(result ?? { ok: true }));
}
if (process.argv[1] && pathToFileURL(realpathSync(process.argv[1])).href === import.meta.url) {
  main().catch(() => {
    console.error('Retained source operation failed; existing protection is retained.');
    process.exitCode = 1;
  });
}
