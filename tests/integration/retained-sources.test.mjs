import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rm, lstat, rename, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { randomUUID } from 'node:crypto';
import { createHash } from 'node:crypto';
import filesystem from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import {
  fixture,
  meta,
  keys,
  NOW,
  image,
  databaseState,
} from '../helpers/relational-generation.mjs';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import { R2Storage } from '../../server/storage/r2-storage.ts';
import { describeStoredResult } from '../../server/storage/result-files.ts';
import { describeRetainedSnapshot } from '../../scripts/retained-sources.mjs';
import { resolveLegacyRetainedDependencies } from '../../scripts/resolve-legacy-retained-dependencies.mjs';
import { createRetainedReleaseFixture } from '../helpers/retained-release.mjs';
import {
  initializeRetainedSources,
  readRetainedSourceProtection,
  readRetainedArchiveBinding,
  beginRetainedCapture,
  bindRetainedCapture,
  publishRetainedLocal,
  publishRetainedArchive,
  beginRetainedRemote,
  confirmRetainedRemote,
  beginRetainedRetirement,
  confirmRetainedRetirement,
  retainedFileSha256,
  checkpointRetainedSources,
  restoreRetainedSources,
  inspectRetainedLease,
  recoverRetainedLease,
  withRetainedArtifactRelease,
} from '../../server/storage/retained-sources.ts';
async function temporary(t) {
  const root = await mkdtemp(path.join(tmpdir(), 'nai-retained-sources-'));
  t.after(async () => {
    const actual = await realpath(root),
      parent = await realpath(tmpdir()),
      info = await lstat(root);
    assert.equal(info.isSymbolicLink(), false);
    assert.equal(path.dirname(actual), parent);
    assert.ok(path.basename(actual).startsWith('nai-retained-sources-'));
    await rm(root, { recursive: true, force: true });
  });
  const registry = path.join(root, 'registry');
  await initializeRetainedSources(registry);
  return { root, registry };
}
test('retained source uses original owners for retired artifact and unpublished prepared dependencies', async (t) => {
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { env });
  await f.accept('retired');
  f.dispatch('retired-0');
  await f.publish('retired-0');
  const replicas = new RelationalReplicaStore(f.db, undefined, env);
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    (actor) =>
      replicas.retireArtifactHistory(
        meta('retire-source', NOW + 90000, actor.id),
        ['artifact-retired-0'],
        actor,
        new Map(),
      ),
    env,
  );
  await f.accept('prepared', { now: NOW + 100000 });
  f.dispatch('prepared-0', NOW + 100001);
  const descriptor = describeStoredResult('artifact-prepared-0', 'generate', {
    mimeType: 'image/webp',
    buffer: image,
  });
  f.db.transaction(() =>
    f.generations.prepareGenerationResult(
      meta('prepare-only', NOW + 100003),
      'prepared-0',
      descriptor,
      f.response(NOW + 100003),
    ),
  )();
  const changes = f.db.prepare('SELECT total_changes() AS n').get().n;
  assert.deepEqual(
    f.generations.readGenerationResultDependency('prepared-0'),
    f.generations.readGenerationResultPreparation('prepared-0'),
  );
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
  const beforeIdentity = databaseState(f.db);
  assert.throws(
    () => f.db.prepare("UPDATE generation_requests SET operation=NULL WHERE id='prepared'").run(),
    /immutable generation_requests identity/,
  );
  assert.equal(databaseState(f.db), beforeIdentity);
  assert.deepEqual(
    f.generations.readGenerationResultDependency('prepared-0'),
    f.generations.readGenerationResultPreparation('prepared-0'),
  );
  const before = await readFile(path.join(f.dir, 'images/artifact-retired-0.webp'));
  const filename = f.db.name;
  f.db.pragma('wal_checkpoint(TRUNCATE)');
  f.db.pragma('journal_mode=DELETE');
  f.db.close();
  const hash = await retainedFileSha256(filename),
    output = path.join(f.dir, 'dependencies.jsonl');
  const result = await describeRetainedSnapshot(filename, output, f.dir);
  assert.equal(result.dependencies, 'known');
  assert.equal(result.sourceSha256, hash);
  const lines = (await readFile(output, 'utf8')).trim().split('\n').map(JSON.parse);
  assert.ok(
    lines.some(
      (x) => x.kind === 'artifact' && x.id === 'artifact-retired-0' && x.replicas.length === 1,
    ),
  );
  const pending = lines.find((x) => x.kind === 'prepared-result');
  assert.equal(pending.jobId, 'prepared-0');
  assert.deepEqual(pending.descriptor, descriptor);
  assert.equal(pending.dataDir, f.dir);
  assert.equal(await retainedFileSha256(filename), hash);
  assert.deepEqual(await readFile(path.join(f.dir, 'images/artifact-retired-0.webp')), before);
});

const archiveDigest = (value) => createHash('sha256').update(value).digest('hex');
async function selectedArchive(t, empty = false) {
  const state = await temporary(t),
    selected = path.join(state.root, 'selected-export');
  await mkdir(selected);
  const source = path.join(state.root, 'sealed-original.sqlite');
  await writeFile(source, 'selected synthetic archive source; no SQLite interpretation');
  const artifacts = path.join(selected, 'artifacts.jsonl');
  const artifactBytes = empty
    ? Buffer.alloc(0)
    : Buffer.from(
        JSON.stringify({
          id: 'original-artifact',
          jobId: null,
          memberId: null,
          operation: null,
          mimeType: null,
          byteLength: null,
          sha256: null,
          file: null,
          createdAt: null,
          r2: null,
          pikpak: null,
          externalBytesChecked: false,
        }) + '\n',
      );
  await writeFile(artifacts, artifactBytes);
  // inputs 的选定元组随 audit 保存；本 consumer 不读取或要求这份实际正文文件。
  const inputBytes = Buffer.from('{"kind":"request","id":"original-input"}\n');
  const tuple = (basename, value, rowCount) => ({
    basename,
    byteLength: value.length,
    rowCount,
    sha256: archiveDigest(value),
  });
  const audit = {
    format: 'legacy-archive-audit-v1',
    sourceCommit: '3caba3dfa0b5d9f582acabfc02e36e7db4af038f',
    toolSha256: 'be05b47b12f7ed8dc7b31acaca0e62187ed165e224cd07ba40142b455869472c',
    sourceSha256: await retainedFileSha256(source),
    openingSha256: 'a'.repeat(64),
    lastCommitSeq: '1',
    commits: 1,
    events: 1,
    protectedInputs: 1,
    protectedInputBytes: 123,
    contexts: 0,
    artifactInputs: 0,
    diagnostics: 0,
    artifacts: empty ? 0 : 1,
    networkCalls: 0,
    dispatches: 0,
    externalObjectBytesVerified: false,
    exports: {
      inputs: tuple('inputs.jsonl', inputBytes, 1),
      artifacts: tuple('artifacts.jsonl', artifactBytes, empty ? 0 : 1),
    },
  };
  const manifest = path.join(selected, 'audit.json');
  await writeFile(manifest, JSON.stringify(audit, null, 2) + '\n');
  return {
    ...state,
    source,
    manifest,
    artifacts,
    audit,
    artifactBytes,
    pin: await retainedFileSha256(manifest),
  };
}
async function retainSelectedArchive(f, pin) {
  const capture = await beginRetainedCapture(f.registry, 'archive', f.source);
  const binding = await readRetainedArchiveBinding(f.source, f.manifest, f.artifacts, pin);
  await bindRetainedCapture(f.registry, capture.sourceId, f.manifest, binding, f.artifacts);
  await publishRetainedArchive(f.registry, capture.sourceId, f.source, f.manifest);
  return { capture, binding };
}
async function resolvedArchive(t, preparedResults = 0) {
  const f = await selectedArchive(t),
    archive = path.join(f.root, 'archive-body');
  await mkdir(archive);
  const moved = path.join(archive, 'source.sqlite');
  assert.equal(path.dirname(f.source), f.root);
  assert.equal(path.dirname(moved), archive);
  await rename(f.source, moved);
  f.source = moved;
  const original = JSON.parse(f.artifactBytes.toString('utf8'));
  original.r2 = {
    bucket: 'legacy-bucket',
    key: `images/original/${'c'.repeat(64)}`,
    byteLength: null,
    sha256: null,
    verifiedAt: 0,
    localRemovedAt: null,
    removedAt: null,
  };
  f.artifactBytes = Buffer.from(JSON.stringify(original) + '\n');
  await writeFile(f.artifacts, f.artifactBytes);
  f.audit.exports.artifacts.byteLength = f.artifactBytes.length;
  f.audit.exports.artifacts.sha256 = archiveDigest(f.artifactBytes);
  await writeFile(f.manifest, JSON.stringify(f.audit));
  f.pin = await retainedFileSha256(f.manifest);
  const proofs = {};
  for (const name of ['sourceCapture', 'namespaceReceipt']) {
    const file = path.join(f.root, name + '.json');
    await writeFile(
      file,
      JSON.stringify({ sourceSha256: f.audit.sourceSha256, cut: f.audit.lastCommitSeq }),
    );
    proofs[name] = { file, sha256: await retainedFileSha256(file) };
  }
  const authority = path.join(f.root, 'original-namespace.json'),
    inventory = path.join(f.root, 'original-inventory.json'),
    resolved = path.join(f.root, 'resolved.jsonl');
  await writeFile(
    authority,
    JSON.stringify({
      format: 'novelai-legacy-cut-config-v1',
      sourceSha256: f.audit.sourceSha256,
      sourceCommit: f.audit.sourceCommit,
      cut: f.audit.lastCommitSeq,
      sourceNamespaces: {
        dataDir: '/original/data',
        r2AccountId: 'a'.repeat(32),
        r2Bucket: 'legacy-bucket',
        pikpakUrl: null,
      },
      ...proofs,
    }),
  );
  await writeFile(
    inventory,
    JSON.stringify({
      format: 'legacy-v4-inventory-v1',
      integrity: true,
      foreignKeyViolations: 0,
      source: { databaseSha256: f.audit.sourceSha256, declaredCommit: f.audit.sourceCommit },
      cut: { lastCommitSeq: f.audit.lastCommitSeq, openingComplete: true },
      liabilities: { preparedResults },
    }),
  );
  await resolveLegacyRetainedDependencies({
    source: f.source,
    audit: f.manifest,
    auditSha256: f.pin,
    artifacts: f.artifacts,
    namespaceAuthority: authority,
    namespaceAuthoritySha256: await retainedFileSha256(authority),
    inventory,
    inventorySha256: await retainedFileSha256(inventory),
    output: resolved,
  });
  return { ...f, resolved, resolvedSha256: await retainedFileSha256(resolved), original };
}
async function archiveCli(f, pin, resolvedPin, requestExtra = {}) {
  const preload =
    'data:text/javascript,' +
    encodeURIComponent(`
    import net from 'node:net';import {syncBuiltinESMExports} from 'node:module';
    net.Socket.prototype.connect=function(){throw Error('No network in archive test');};
    globalThis.fetch=()=>{throw Error('No fetch in archive test');};syncBuiltinESMExports();
  `);
  const args = [
    '--import',
    preload,
    path.resolve('scripts/retained-sources.mjs'),
    'archive',
    f.registry,
  ];
  if (pin !== undefined) {
    args.push(pin);
  }
  if (resolvedPin !== undefined) {
    args.push(resolvedPin);
  }
  const child = spawn(process.execPath, args, {
    stdio: ['pipe', 'pipe', 'pipe'],
    windowsHide: true,
  });
  let stdout = '',
    stderr = '';
  child.stdout.on('data', (chunk) => {
    stdout += chunk;
  });
  child.stderr.on('data', (chunk) => {
    stderr += chunk;
  });
  const exited = once(child, 'exit');
  child.stdin.end(
    JSON.stringify({
      source: f.source,
      manifest: f.manifest,
      dependencies: f.artifacts,
      ...requestExtra,
    }),
  );
  const [code] = await exited;
  return { code, stdout, stderr };
}
test('archive selected export: independent CLI pin saves original tuples through checkpoint and restore', async (t) => {
  const f = await selectedArchive(t);
  assert.equal(
    await lstat(path.join(path.dirname(f.manifest), 'inputs.jsonl')).catch(() => null),
    null,
  );
  const result = await archiveCli(f, f.pin);
  assert.equal(result.code, 0, result.stderr);
  const { sourceId } = JSON.parse(result.stdout);
  const current = await readRetainedSourceProtection(f.registry),
    row = current.sources[0];
  assert.equal(row.id, sourceId);
  assert.equal(row.holds[0].state, 'retained');
  assert.equal(row.binding.manifestSha256, f.pin);
  assert.equal(row.binding.dependenciesSha256, f.audit.exports.artifacts.sha256);
  assert.equal(row.binding.dependencies, 'unknown');
  assert.equal(current.coverage, 'unknown');
  assert.equal(current.releaseAllowed, false);
  const saved = path.join(f.registry, 'sources', sourceId);
  assert.deepEqual(JSON.parse(await readFile(path.join(saved, 'manifest.json'))), f.audit);
  assert.deepEqual(await readFile(path.join(saved, 'dependencies.jsonl')), f.artifactBytes);
  await rm(f.manifest);
  await rm(f.artifacts);
  const checkpoint = path.join(f.root, 'checkpoint'),
    sealed = await checkpointRetainedSources(f.registry, checkpoint);
  const restored = path.join(f.root, 'restored');
  await restoreRetainedSources(restored, checkpoint, sealed.sha256);
  assert.deepEqual(await readRetainedSourceProtection(restored), current);
  assert.equal(
    await retainedFileSha256(path.join(restored, 'sources', sourceId, 'manifest.json')),
    f.pin,
  );
});

test('legacy generic proof refines only its original Source, preserves raw export and survives checkpoint/restore; same proof repeats zero metadata writes', async (t) => {
  const f = await resolvedArchive(t, 2),
    old = await retainSelectedArchive(f, f.pin),
    unknown = await readRetainedSourceProtection(f.registry),
    binding = await readRetainedArchiveBinding(f.source, f.manifest, f.artifacts, f.pin, {
      file: f.resolved,
      sha256: f.resolvedSha256,
    });
  assert.equal(binding.historyId, null);
  assert.equal(binding.dependencies, 'known');
  assert.equal(binding.dependenciesSha256, old.binding.dependenciesSha256);
  assert.equal(binding.resolvedDependenciesSha256, f.resolvedSha256);
  const accepted = await archiveCli(f, f.pin, f.resolvedSha256, {
    sourceId: old.capture.sourceId,
    resolvedDependencies: f.resolved,
  });
  assert.equal(accepted.code, 0, accepted.stderr);
  assert.equal(JSON.parse(accepted.stdout).sourceId, old.capture.sourceId);
  const known = await readRetainedSourceProtection(f.registry);
  assert.deepEqual(known.sources[0].holds, unknown.sources[0].holds);
  assert.equal(known.sources[0].id, old.capture.sourceId);
  assert.deepEqual(
    await readFile(path.join(f.registry, 'sources', old.capture.sourceId, 'dependencies.jsonl')),
    f.artifactBytes,
  );
  const registryBytes = await readFile(path.join(f.registry, 'registry.json'));
  await bindRetainedCapture(
    f.registry,
    old.capture.sourceId,
    f.manifest,
    binding,
    f.artifacts,
    undefined,
    f.resolved,
  );
  assert.deepEqual(await readFile(path.join(f.registry, 'registry.json')), registryBytes);
  const checkpoint = path.join(f.root, 'resolved-checkpoint'),
    sealed = await checkpointRetainedSources(f.registry, checkpoint),
    restored = path.join(f.root, 'resolved-restored');
  await restoreRetainedSources(restored, checkpoint, sealed.sha256);
  assert.equal(
    (await readRetainedSourceProtection(restored)).sources[0].binding.resolvedDependenciesSha256,
    f.resolvedSha256,
  );
  assert.deepEqual(
    await readFile(
      path.join(restored, 'sources', old.capture.sourceId, 'resolved-dependencies.jsonl'),
    ),
    await readFile(f.resolved),
  );
});

test('legacy resolved proof needs independent positional pin and exact original rows; immutable-copy tail leaves unknown binding and healthy retry works', async (t) => {
  const f = await resolvedArchive(t),
    old = await retainSelectedArchive(f, f.pin),
    before = await readFile(path.join(f.registry, 'registry.json')),
    selected = { file: f.resolved, sha256: f.resolvedSha256 };
  const denied = await archiveCli(f, f.pin, undefined, {
    resolvedDependencies: f.resolved,
    resolvedDependenciesSha256: f.resolvedSha256,
    sourceId: old.capture.sourceId,
  });
  assert.equal(denied.code, 1);
  assert.deepEqual(await readFile(path.join(f.registry, 'registry.json')), before);
  const badLines = (await readFile(f.resolved, 'utf8')).trimEnd().split('\n').map(JSON.parse);
  badLines[1].originalRow.sha256 = '0'.repeat(64);
  const badFile = path.join(f.root, 'wrong-origin.jsonl'),
    bad = Buffer.from(badLines.map(JSON.stringify).join('\n') + '\n');
  await writeFile(badFile, bad);
  await assert.rejects(
    readRetainedArchiveBinding(f.source, f.manifest, f.artifacts, f.pin, {
      file: badFile,
      sha256: archiveDigest(bad),
    }),
  );
  const binding = await readRetainedArchiveBinding(
      f.source,
      f.manifest,
      f.artifacts,
      f.pin,
      selected,
    ),
    copy = filesystem.copyFile;
  let reached = false;
  filesystem.copyFile = async function (from, to, ...args) {
    const result = await Reflect.apply(copy, this, [from, to, ...args]);
    if (path.basename(to) === 'resolved-dependencies.jsonl') {
      reached = true;
      throw Error('synthetic resolved-copy-tail');
    }
    return result;
  };
  syncBuiltinESMExports();
  try {
    await assert.rejects(
      bindRetainedCapture(
        f.registry,
        old.capture.sourceId,
        f.manifest,
        binding,
        f.artifacts,
        undefined,
        f.resolved,
      ),
      /synthetic resolved-copy-tail/,
    );
  } finally {
    filesystem.copyFile = copy;
    syncBuiltinESMExports();
  }
  assert.equal(reached, true);
  assert.deepEqual(await readFile(path.join(f.registry, 'registry.json')), before);
  await bindRetainedCapture(
    f.registry,
    old.capture.sourceId,
    f.manifest,
    binding,
    f.artifacts,
    undefined,
    f.resolved,
  );
  assert.equal(
    (await readRetainedSourceProtection(f.registry)).sources[0].binding.dependencies,
    'known',
  );
});

test('resolved legacy unknown descriptor pins its exact key and prepared count pins only the original Local domain', async (t) => {
  const f = await resolvedArchive(t, 2),
    live = await fixture(t),
    r2 = new R2Storage({
      accountId: 'b'.repeat(32),
      bucket: 'current-bucket',
      accessKeyId: 'synthetic',
      secretAccessKey: 'synthetic',
    });
  t.mock.method(r2, 'listBackups', async () => []);
  const held = await createRetainedReleaseFixture(t, live.db, live.dir, { r2 }),
    binding = await readRetainedArchiveBinding(f.source, f.manifest, f.artifacts, f.pin, {
      file: f.resolved,
      sha256: f.resolvedSha256,
    }),
    capture = await beginRetainedCapture(held.registry, 'archive', f.source);
  await bindRetainedCapture(
    held.registry,
    capture.sourceId,
    f.manifest,
    binding,
    f.artifacts,
    undefined,
    f.resolved,
  );
  await publishRetainedArchive(held.registry, capture.sourceId, f.source, f.manifest);
  await held.addSnapshot(live.db, live.dir);
  const claim = {
    artifactId: 'new-artifact',
    sha256: 'c'.repeat(64),
    byteLength: 1,
    source: {
      kind: 'local',
      namespace: 'images',
      objectKey: 'new.webp',
      encoding: 'identity',
      dataDir: live.dir,
    },
    destination: {
      kind: 'r2',
      namespace: `r2://${'b'.repeat(32)}/current-bucket`,
      objectKey: `images/new/${'c'.repeat(64)}`,
      encoding: 'identity',
    },
  };
  assert.equal(
    await withRetainedArtifactRelease(held.registry, claim, async () => 'allowed', { r2 }),
    'allowed',
  );
  assert.equal(
    await withRetainedArtifactRelease(
      held.registry,
      { ...claim, source: { ...claim.source, dataDir: '/original/data' } },
      async () => assert.fail('Original prepared Local domain must remain pinned'),
      { r2 },
    ),
    null,
  );
  assert.equal(
    await withRetainedArtifactRelease(
      held.registry,
      {
        ...claim,
        source: {
          kind: 'r2',
          namespace: `r2://${'a'.repeat(32)}/legacy-bucket`,
          objectKey: f.original.r2.key,
          encoding: 'identity',
        },
      },
      async () => assert.fail('Unknown original descriptor must pin its physical key'),
      { r2 },
    ),
    null,
  );
});
test('archive selected export: a self-consistent empty rewrite cannot replace the externally selected pin', async (t) => {
  const f = await selectedArchive(t),
    originalSource = await retainedFileSha256(f.source);
  f.audit.artifacts = 0;
  f.audit.exports.artifacts = {
    basename: 'artifacts.jsonl',
    byteLength: 0,
    rowCount: 0,
    sha256: archiveDigest(Buffer.alloc(0)),
  };
  await writeFile(f.artifacts, Buffer.alloc(0));
  await writeFile(f.manifest, JSON.stringify(f.audit));
  assert.notEqual(await retainedFileSha256(f.manifest), f.pin);
  const result = await archiveCli(f, f.pin);
  assert.equal(result.code, 1);
  assert.equal(result.stdout, '');
  assert.equal(result.stderr.includes('original-artifact'), false);
  const current = await readRetainedSourceProtection(f.registry);
  assert.equal(current.sources.length, 1);
  assert.equal(current.sources[0].holds[0].state, 'preparing');
  assert.equal(current.sources[0].binding, null);
  assert.equal(await retainedFileSha256(f.source), originalSource);
});
test('archive selected export: exact source, audit structure and physical artifact tuple are required', async (t) => {
  const cases = [
    [
      'same-length bytes',
      async (f) => {
        const changed = Buffer.from(f.artifactBytes);
        changed[2] ^= 1;
        await writeFile(f.artifacts, changed);
      },
    ],
    [
      'wrong cut',
      async (f) => {
        f.audit.lastCommitSeq = '2';
        f.audit.commits = 2;
        await writeFile(f.manifest, JSON.stringify(f.audit));
      },
    ],
    [
      'wrong tool',
      async (f) => {
        f.audit.toolSha256 = 'b'.repeat(64);
        await writeFile(f.manifest, JSON.stringify(f.audit));
      },
    ],
    [
      'missing artifact file',
      async (f) => {
        await rm(f.artifacts);
      },
    ],
    [
      'missing tuple',
      async (f) => {
        delete f.audit.exports.inputs;
        await writeFile(f.manifest, JSON.stringify(f.audit));
        f.pin = await retainedFileSha256(f.manifest);
      },
    ],
    [
      'unsafe count',
      async (f) => {
        f.audit.events = Number.MAX_SAFE_INTEGER + 1;
        await writeFile(f.manifest, JSON.stringify(f.audit));
        f.pin = await retainedFileSha256(f.manifest);
      },
    ],
    [
      'actual line count',
      async (f) => {
        f.audit.artifacts = 2;
        f.audit.exports.artifacts.rowCount = 2;
        await writeFile(f.manifest, JSON.stringify(f.audit));
        f.pin = await retainedFileSha256(f.manifest);
      },
    ],
    [
      'wrong basename',
      async (f) => {
        const other = path.join(f.root, 'other.jsonl');
        await writeFile(other, f.artifactBytes);
        f.artifacts = other;
      },
    ],
    [
      'wrong source',
      async (f) => {
        await writeFile(f.source, 'another complete source');
      },
    ],
  ];
  for (const [name, change] of cases) {
    await t.test(name, async (t) => {
      const f = await selectedArchive(t);
      await change(f);
      await assert.rejects(readRetainedArchiveBinding(f.source, f.manifest, f.artifacts, f.pin));
      assert.equal((await readRetainedSourceProtection(f.registry)).sources.length, 0);
    });
  }
});
test('archive selected export: a trusted zero-row artifact list is a real empty file', async (t) => {
  const f = await selectedArchive(t, true);
  const { binding } = await retainSelectedArchive(f, f.pin);
  assert.equal(binding.dependenciesSha256, archiveDigest(Buffer.alloc(0)));
  assert.equal(
    (await readRetainedSourceProtection(f.registry)).sources[0].holds[0].state,
    'retained',
  );
  await rm(f.artifacts);
  await assert.rejects(readRetainedArchiveBinding(f.source, f.manifest, f.artifacts, f.pin));
});
test('archive selected export: actual copy-tail failure preserves preparing and can retry the same proof', async (t) => {
  const f = await selectedArchive(t),
    capture = await beginRetainedCapture(f.registry, 'archive', f.source);
  const binding = await readRetainedArchiveBinding(f.source, f.manifest, f.artifacts, f.pin);
  const before = await readFile(path.join(f.registry, 'registry.json'));
  const actual = filesystem.copyFile;
  let copied = 0;
  filesystem.copyFile = async function (from, to, ...options) {
    const result = await actual(from, to, ...options);
    if (path.basename(to) === 'dependencies.jsonl') {
      copied++;
      throw Error('archive copy tail injected');
    }
    return result;
  };
  syncBuiltinESMExports();
  try {
    await assert.rejects(
      bindRetainedCapture(f.registry, capture.sourceId, f.manifest, binding, f.artifacts),
      /archive copy tail injected/,
    );
  } finally {
    filesystem.copyFile = actual;
    syncBuiltinESMExports();
  }
  assert.equal(copied, 1);
  assert.equal(
    await retainedFileSha256(
      path.join(f.registry, 'sources', capture.sourceId, 'dependencies.jsonl'),
    ),
    binding.dependenciesSha256,
  );
  assert.deepEqual(await readFile(path.join(f.registry, 'registry.json')), before);
  await assert.rejects(publishRetainedArchive(f.registry, capture.sourceId, f.source, f.manifest));
  assert.equal(
    (await readRetainedSourceProtection(f.registry)).sources[0].holds[0].state,
    'preparing',
  );
  await bindRetainedCapture(f.registry, capture.sourceId, f.manifest, binding, f.artifacts);
  await publishRetainedArchive(f.registry, capture.sourceId, f.source, f.manifest);
  assert.equal(
    (await readRetainedSourceProtection(f.registry)).sources[0].holds[0].state,
    'retained',
  );
});
test('archive selected export: stored artifact hashes cannot detach from the preserved audit tuple', async (t) => {
  const f = await selectedArchive(t),
    { capture } = await retainSelectedArchive(f, f.pin);
  const stored = path.join(f.registry, 'sources', capture.sourceId, 'dependencies.jsonl');
  const changed = Buffer.from(f.artifactBytes);
  changed[2] ^= 1;
  await writeFile(stored, changed);
  const registryFile = path.join(f.registry, 'registry.json'),
    registry = JSON.parse(await readFile(registryFile));
  registry.sources[0].binding.dependenciesSha256 = archiveDigest(changed);
  await writeFile(registryFile, JSON.stringify(registry));
  await assert.rejects(readRetainedSourceProtection(f.registry));
  await assert.rejects(
    checkpointRetainedSources(f.registry, path.join(f.root, 'refused-checkpoint')),
  );
});
test('archive selected export: declaring exports requires artifacts even without an external pin', async (t) => {
  const f = await selectedArchive(t);
  for (const pin of [undefined, f.pin]) {
    await assert.rejects(readRetainedArchiveBinding(f.source, f.manifest, undefined, pin));
  }
  const binding = await readRetainedArchiveBinding(f.source, f.manifest, f.artifacts, f.pin);
  const capture = await beginRetainedCapture(f.registry, 'archive', f.source);
  await assert.rejects(
    bindRetainedCapture(f.registry, capture.sourceId, f.manifest, {
      ...binding,
      dependenciesSha256: null,
    }),
  );
  const current = await readRetainedSourceProtection(f.registry);
  assert.equal(current.sources[0].binding, null);
  assert.equal(current.sources[0].holds[0].state, 'preparing');
});
test('archive selected export: null cannot detach a declared artifact file from read or recovery', async (t) => {
  const f = await selectedArchive(t),
    { capture, binding } = await retainSelectedArchive(f, f.pin);
  const checkpoint = path.join(f.root, 'healthy-checkpoint');
  await checkpointRetainedSources(f.registry, checkpoint);
  async function detach(root) {
    const registryFile = path.join(root, 'registry.json');
    const registry = JSON.parse(await readFile(registryFile));
    registry.sources[0].binding.dependenciesSha256 = null;
    await writeFile(registryFile, JSON.stringify(registry));
    const saved = path.join(root, 'sources', capture.sourceId);
    await filesystem.rename(
      path.join(saved, 'dependencies.jsonl'),
      path.join(saved, 'dependencies.held'),
    );
    assert.equal(await retainedFileSha256(path.join(saved, 'manifest.json')), f.pin);
    assert.equal(
      await retainedFileSha256(path.join(saved, 'dependencies.held')),
      binding.dependenciesSha256,
    );
    await assert.rejects(lstat(path.join(saved, 'dependencies.jsonl')), { code: 'ENOENT' });
  }
  await detach(f.registry);
  assert.equal(await retainedFileSha256(f.source), f.audit.sourceSha256);
  assert.equal(await retainedFileSha256(f.manifest), f.pin);
  await assert.rejects(readRetainedSourceProtection(f.registry));
  await assert.rejects(
    checkpointRetainedSources(f.registry, path.join(f.root, 'refused-checkpoint')),
  );

  await detach(checkpoint);
  const changedSha = await retainedFileSha256(path.join(checkpoint, 'registry.json'));
  const markerFile = path.join(checkpoint, 'checkpoint.json');
  const marker = JSON.parse(await readFile(markerFile));
  await writeFile(markerFile, JSON.stringify({ ...marker, sha256: changedSha }));
  assert.equal(JSON.parse(await readFile(markerFile)).sha256, changedSha);
  await assert.rejects(readRetainedSourceProtection(checkpoint, changedSha));
  const restored = path.join(f.root, 'refused-restore');
  await assert.rejects(restoreRetainedSources(restored, checkpoint, changedSha));
  await assert.rejects(lstat(restored), { code: 'ENOENT' });
});
test('archive selected export: old reports without an external pin remain unknown', async (t) => {
  const f = await selectedArchive(t);
  delete f.audit.exports;
  delete f.audit.toolSha256;
  await writeFile(f.manifest, JSON.stringify(f.audit));
  const { binding } = await retainSelectedArchive(f);
  assert.equal(binding.dependencies, 'unknown');
  assert.equal((await readRetainedSourceProtection(f.registry)).releaseAllowed, false);
  await assert.rejects(
    readRetainedArchiveBinding(
      f.source,
      f.manifest,
      f.artifacts,
      await retainedFileSha256(f.manifest),
    ),
  );
});
test('local retirement keeps independent remote pin and checkpoint metadata survives local backup removal', async (t) => {
  const { root, registry } = await temporary(t),
    snapshot = path.join(root, 'daily-one');
  await mkdir(path.join(snapshot, 'data'), { recursive: true });
  const db = path.join(snapshot, 'data/library.sqlite'),
    manifest = path.join(snapshot, 'manifest.json');
  await writeFile(db, 'opaque source; not interpreted as a new schema');
  await writeFile(manifest, '{"files":{}}');
  const capture = await beginRetainedCapture(registry, 'daily', snapshot);
  const binding = {
    format: 'opaque',
    sourceSha256: await retainedFileSha256(db),
    manifestSha256: await retainedFileSha256(manifest),
    dependenciesSha256: null,
    dependencies: 'unknown',
    historyId: null,
    finalCommitSeq: null,
  };
  await bindRetainedCapture(registry, capture.sourceId, manifest, binding);
  await publishRetainedLocal(registry, capture.sourceId, snapshot);
  const object = {
    namespace: 'r2://' + 'a'.repeat(32) + '/synthetic',
    bucket: 'synthetic',
    key: 'backups/daily-20261115T000000Z/' + 'a'.repeat(64),
    sha256: 'a'.repeat(64),
    byteLength: 123,
  };
  const remote = await beginRetainedRemote(registry, object, capture.sourceId);
  await confirmRetainedRemote(registry, remote.sourceId, remote.holdId, object);
  await assert.rejects(
    beginRetainedRemote(registry, { ...object, byteLength: object.byteLength + 1 }),
  );
  await beginRetainedRetirement(registry, capture.sourceId, capture.holdId);
  await assert.rejects(
    confirmRetainedRetirement(registry, capture.sourceId, capture.holdId, snapshot),
  );
  await rm(snapshot, { recursive: true });
  await confirmRetainedRetirement(registry, capture.sourceId, capture.holdId, snapshot);
  const current = await readRetainedSourceProtection(registry);
  assert.equal(current.sources[0].holds.find((x) => x.kind === 'local').state, 'retired');
  assert.equal(current.sources[0].holds.find((x) => x.kind === 'remote').state, 'retained');
  assert.equal(current.releaseAllowed, false);
  assert.equal(current.coverage, 'unknown');
  const checkpoint = path.join(root, 'checkpoint'),
    sealed = await checkpointRetainedSources(registry, checkpoint);
  const restored = path.join(root, 'restored');
  await restoreRetainedSources(restored, checkpoint, sealed.sha256);
  assert.deepEqual(await readRetainedSourceProtection(restored), current);
  await writeFile(path.join(checkpoint, 'lease.json'), 'live lease must not be imported');
  await assert.rejects(
    restoreRetainedSources(path.join(root, 'live-checkpoint'), checkpoint, sealed.sha256),
  );
  await rm(path.join(checkpoint, 'lease.json'));
  await writeFile(path.join(restored, 'sources', capture.sourceId, 'manifest.json'), 'changed');
  await assert.rejects(readRetainedSourceProtection(restored));
});
test('live and unverifiable metadata leases fail closed; matched Linux recovery preserves preparing', async (t) => {
  const { root, registry } = await temporary(t),
    module = pathToFileURL(path.resolve('server/storage/retained-sources.ts')).href;
  const program = `
    import fs from 'node:fs/promises';import net from 'node:net';import {syncBuiltinESMExports} from 'node:module';
    net.Socket.prototype.connect=function(){throw Error('No network in retained source test');};globalThis.fetch=()=>{throw Error('No fetch');};
    const [registry,module]=process.argv.slice(1),owner=await import(module);
    await owner.beginRetainedCapture(registry,'daily',registry+'/pending-capture');
    const actual=fs.rename;fs.rename=async function(from,to){if(to.endsWith('registry.json')){process.send({held:true});await new Promise(()=>{});}return actual(from,to);};
    syncBuiltinESMExports();process.on('message',()=>{});
    await owner.beginRetainedCapture(registry,'daily',registry+'/not-yet-committed');
  `;
  const child = spawn(process.execPath, ['--input-type=module', '-e', program, registry, module], {
    stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    windowsHide: true,
  });
  let stderr = '';
  child.stderr.on('data', (b) => (stderr += b));
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill('SIGKILL');
      await once(child, 'exit');
    }
  });
  await Promise.race([
    once(child, 'message'),
    once(child, 'exit').then(() => {
      throw Error(stderr);
    }),
  ]);
  const proof = await inspectRetainedLease(registry);
  assert.equal(proof.pid, child.pid);
  await assert.rejects(recoverRetainedLease(registry, proof));
  await assert.rejects(readRetainedSourceProtection(registry));
  child.kill('SIGKILL');
  await once(child, 'exit');
  let current;
  if (process.platform === 'linux' && proof.domain) {
    await recoverRetainedLease(registry, proof);
    current = await readRetainedSourceProtection(registry);
    assert.equal(current.releaseAllowed, false);
  } else {
    await assert.rejects(recoverRetainedLease(registry, proof));
    await assert.rejects(readRetainedSourceProtection(registry));
    current = JSON.parse(await readFile(path.join(registry, 'registry.json'), 'utf8'));
    assert.equal(current.coverage, 'unknown');
  }
  assert.equal(current.sources.length, 1);
  assert.equal(current.sources[0].holds[0].state, 'preparing');
});
test('a foreign owner domain cannot use an exited PID and matching nonce to take a metadata lease', async (t) => {
  const { registry } = await temporary(t);
  await beginRetainedCapture(registry, 'daily', path.join(registry, 'pending'));
  const child = spawn(process.execPath, ['--eval', 'process.exit(0)'], {
    stdio: 'ignore',
    windowsHide: true,
  });
  await once(child, 'exit');
  const catalog = JSON.parse(await readFile(path.join(registry, 'registry.json'), 'utf8'));
  await writeFile(
    path.join(registry, 'lease.json'),
    JSON.stringify({
      nonce: randomUUID(),
      pid: child.pid,
      registryId: catalog.registryId,
      revision: catalog.revision,
      domain: {
        platform: 'linux',
        host: 'foreign-' + randomUUID(),
        bootId: randomUUID(),
        pidNamespace: 'pid:[1]',
      },
    }),
  );
  const proof = await inspectRetainedLease(registry);
  await assert.rejects(recoverRetainedLease(registry, proof));
  assert.deepEqual(await inspectRetainedLease(registry), proof);
});
test('missing catalog or unsupported coverage never becomes an empty complete inventory', async (t) => {
  const { root, registry } = await temporary(t);
  await assert.rejects(readRetainedSourceProtection(path.join(root, 'missing')));
  const file = path.join(registry, 'registry.json'),
    catalog = JSON.parse(await readFile(file));
  catalog.coverage = 'complete';
  await writeFile(file, JSON.stringify(catalog));
  await assert.rejects(readRetainedSourceProtection(registry));
});
test('checkpoint reads and restores reject the same incomplete proof before creating output', async (t) => {
  const { root, registry } = await temporary(t);
  const manifest = path.join(root, 'selected-manifest.json');
  const dependencies = path.join(root, 'selected-dependencies.jsonl');
  await writeFile(manifest, '{"selected":"original source"}');
  await writeFile(dependencies, '{"kind":"unknown-dependencies"}\n');
  const capture = await beginRetainedCapture(registry, 'daily', path.join(root, 'snapshot'));
  await bindRetainedCapture(
    registry,
    capture.sourceId,
    manifest,
    {
      format: 'opaque',
      sourceSha256: null,
      manifestSha256: await retainedFileSha256(manifest),
      dependenciesSha256: await retainedFileSha256(dependencies),
      dependencies: 'unknown',
      historyId: null,
      finalCommitSeq: null,
    },
    dependencies,
  );
  const checkpoint = path.join(root, 'checkpoint');
  const sealed = await checkpointRetainedSources(registry, checkpoint);
  const markerFile = path.join(checkpoint, 'checkpoint.json');
  const registryFile = path.join(checkpoint, 'registry.json');
  const proofFile = path.join(checkpoint, 'sources', capture.sourceId, 'dependencies.jsonl');
  const marker = JSON.parse(await readFile(markerFile, 'utf8'));
  const cases = [
    [markerFile, null],
    [markerFile, JSON.stringify({ ...marker, format: 'unknown-checkpoint' })],
    [markerFile, JSON.stringify({ ...marker, registryId: randomUUID() })],
    [markerFile, JSON.stringify({ ...marker, sha256: '0'.repeat(64) })],
    [markerFile, JSON.stringify({ ...marker, unsupported: true })],
    [registryFile, JSON.stringify({ ...JSON.parse(await readFile(registryFile)), revision: 99 })],
    [proofFile, null],
  ];
  assert.deepEqual(
    await readRetainedSourceProtection(checkpoint, sealed.sha256),
    await readRetainedSourceProtection(registry),
  );
  for (const [index, [filename, changed]] of cases.entries()) {
    const original = await readFile(filename);
    if (changed === null) {
      await rm(filename);
    } else {
      await writeFile(filename, changed);
    }
    const output = path.join(root, 'refused-' + index);
    try {
      await assert.rejects(readRetainedSourceProtection(checkpoint, sealed.sha256));
      await assert.rejects(restoreRetainedSources(output, checkpoint, sealed.sha256));
      await assert.rejects(lstat(output), { code: 'ENOENT' });
    } finally {
      await writeFile(filename, original);
    }
  }
  await assert.rejects(restoreRetainedSources(path.join(root, 'no-hash'), checkpoint));
  const restored = path.join(root, 'healthy-restored');
  await restoreRetainedSources(restored, checkpoint, sealed.sha256);
  assert.deepEqual(
    await readRetainedSourceProtection(restored),
    await readRetainedSourceProtection(registry),
  );
});
