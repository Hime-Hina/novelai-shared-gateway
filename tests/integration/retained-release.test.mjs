import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdir, writeFile, readFile } from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import { fixture, image, meta } from '../helpers/relational-generation.mjs';
import { createRetainedReleaseFixture } from '../helpers/retained-release.mjs';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import { R2Storage } from '../../server/storage/r2-storage.ts';
import { PikPakStorage } from '../../server/storage/pikpak-storage.ts';
import {
  withRetainedArtifactRelease,
  assertRetainedArtifactReleasePermit,
  beginRetainedCapture,
  adoptRetainedInventory,
  initializeRetainedSources,
  checkpointRetainedSources,
  readRetainedSourceProtection,
  restoreRetainedSources,
} from '../../server/storage/retained-sources.ts';

async function populated(t) {
  const f = await fixture(t);
  await f.accept('retained');
  f.dispatch('retained-0');
  await f.publish('retained-0');
  const r2 = new R2Storage({
      accountId: 'a'.repeat(32),
      bucket: 'retained-fixture',
      accessKeyId: 'synthetic',
      secretAccessKey: 'synthetic',
    }),
    pikpak = new PikPakStorage({
      url: 'https://dav.test/retained/',
      username: 'synthetic',
      password: 'synthetic',
    }),
    configuration = { dataDir: f.dir, r2, pikpak, idle: () => true },
    store = new RelationalReplicaStore(f.db, configuration);
  t.mock.method(r2, 'listBackups', async () => []);
  const record = (kind, phases) => {
    f.db.transaction(() => store.registerStorageTarget(meta(`register-${kind}`), kind))();
    const target = store.readConfiguredStorageTarget(kind);
    for (const phase of phases) {
      const ticket = store.beginArtifactReplica('artifact-retained-0', target.storageTargetId);
      f.db.transaction(() =>
        store.recordArtifactReplica(meta(`${kind}-${phase}`), ticket, phase),
      )();
    }
  };
  record('r2', ['reserved', 'uploaded', 'verified']);
  const claim = (sourceKind, destinationKind) => {
    const source = store.readConfiguredStorageTarget(sourceKind),
      destination = store.readConfiguredStorageTarget(destinationKind),
      from = store.readArtifactReplica('artifact-retained-0', source.storageTargetId),
      to = store.readArtifactReplica('artifact-retained-0', destination.storageTargetId);
    return {
      artifactId: 'artifact-retained-0',
      sha256: createHash('sha256').update(image).digest('hex'),
      byteLength: image.length,
      source: {
        kind: sourceKind,
        namespace: source.namespace,
        objectKey: from.objectKey,
        encoding: from.encoding,
        ...(sourceKind === 'local' ? { dataDir: f.dir } : {}),
      },
      destination: {
        kind: destinationKind,
        namespace: destination.namespace,
        objectKey: to.objectKey,
        encoding: to.encoding,
      },
    };
  };
  return { ...f, configuration, store, r2, record, claim };
}

test('protected inventory grants only a live same-claim lease; no fresh registry/self scope/opaque capability authority', async (t) => {
  const f = await populated(t),
    protectedSource = await createRetainedReleaseFixture(t, f.db, f.dir, { r2: f.r2 }),
    claim = f.claim('local', 'r2');
  let retained;
  const result = await withRetainedArtifactRelease(
    protectedSource.registry,
    claim,
    async (permit) => {
      retained = permit;
      const evidence = assertRetainedArtifactReleasePermit(permit, claim);
      assert.match(evidence.proofSha256, /^[a-f0-9]{64}$/);
      assert.throws(() => assertRetainedArtifactReleasePermit({}, claim));
      assert.throws(() =>
        assertRetainedArtifactReleasePermit(permit, { ...claim, artifactId: 'other-entity' }),
      );
      await assert.rejects(
        beginRetainedCapture(
          protectedSource.registry,
          'daily',
          path.join(protectedSource.root, 'concurrent'),
        ),
      );
      return 'released';
    },
    { r2: f.r2 },
  );
  assert.equal(result, 'released');
  assert.throws(() => assertRetainedArtifactReleasePermit(retained, claim));
  const fresh = path.join(protectedSource.root, 'fresh-registry');
  await initializeRetainedSources(fresh);
  assert.equal(
    await withRetainedArtifactRelease(
      fresh,
      claim,
      async () => assert.fail('Fresh unknown registry must not grant authority'),
      { r2: f.r2 },
    ),
    null,
  );
  const freshCheckpoint = path.join(protectedSource.root, 'fresh-checkpoint'),
    freshProof = await checkpointRetainedSources(fresh, freshCheckpoint);
  await assert.rejects(
    adoptRetainedInventory(
      fresh,
      {
        ...protectedSource.first.selected,
        checkpoint: freshCheckpoint,
        checkpointSha256: freshProof.sha256,
      },
      { r2: f.r2 },
    ),
  );
  const arbitraryScope = path.join(protectedSource.root, 'caller-scope.json');
  const sameScope = Buffer.from(
    JSON.stringify(
      JSON.parse(await readFile(protectedSource.first.selected.scope, 'utf8')),
      null,
      2,
    ),
  );
  await writeFile(arbitraryScope, sameScope);
  await assert.rejects(
    adoptRetainedInventory(
      protectedSource.registry,
      {
        ...protectedSource.first.selected,
        scope: arbitraryScope,
        scopeSha256: createHash('sha256').update(sameScope).digest('hex'),
      },
      { r2: f.r2 },
    ),
  );
});

test('qualification avoids rehashing the large snapshot, detects changed identity and cannot survive checkpoint/restore as deployment authority', async (t) => {
  const f = await populated(t),
    held = await createRetainedReleaseFixture(t, f.db, f.dir, { r2: f.r2 }),
    filename = path.join(held.first.snapshot, 'data/library.sqlite'),
    original = fs.openSync;
  fs.openSync = function (file, ...args) {
    if (typeof file === 'string' && path.resolve(file) === path.resolve(filename)) {
      assert.fail(
        'Release must use the already-qualified file identity, not re-open the full snapshot.',
      );
    }
    return Reflect.apply(original, this, [file, ...args]);
  };
  syncBuiltinESMExports();
  try {
    assert.equal(
      await withRetainedArtifactRelease(held.registry, f.claim('local', 'r2'), async () => 'ok', {
        r2: f.r2,
      }),
      'ok',
    );
  } finally {
    fs.openSync = original;
    syncBuiltinESMExports();
  }
  const checkpoint = path.join(held.root, 'exported'),
    proof = await checkpointRetainedSources(held.registry, checkpoint),
    restored = path.join(held.root, 'restored');
  assert.equal((await readRetainedSourceProtection(checkpoint, proof.sha256)).coverage, 'unknown');
  await restoreRetainedSources(restored, checkpoint, proof.sha256);
  assert.equal((await readRetainedSourceProtection(restored)).coverage, 'unknown');
  assert.equal(
    await withRetainedArtifactRelease(
      restored,
      f.claim('local', 'r2'),
      async () => assert.fail('Restored lease domain cannot reuse qualification'),
      { r2: f.r2 },
    ),
    null,
  );
  const altered = await readFile(filename);
  altered[altered.length - 1] ^= 1;
  await writeFile(filename, altered);
  await assert.rejects(
    withRetainedArtifactRelease(
      held.registry,
      f.claim('local', 'r2'),
      async () => assert.fail('Changed snapshot identity must retain the source'),
      { r2: f.r2 },
    ),
  );
});

test('old protected Source without that destination pins even after current promotion; unknown new snapshot and real unregistered child block', async (t) => {
  const f = await populated(t),
    protectedSource = await createRetainedReleaseFixture(t, f.db, f.dir, { r2: f.r2 });
  f.record('pikpak', ['reserved', 'uploaded']);
  let calls = 0;
  assert.equal(
    await withRetainedArtifactRelease(
      protectedSource.registry,
      f.claim('r2', 'pikpak'),
      async () => ++calls,
      { r2: f.r2 },
    ),
    null,
  );
  await protectedSource.addSnapshot(f.db, f.dir);
  assert.equal(
    await withRetainedArtifactRelease(
      protectedSource.registry,
      f.claim('r2', 'pikpak'),
      async () => ++calls,
      { r2: f.r2 },
    ),
    null,
  );
  assert.equal(calls, 0);
  const stray = path.join(protectedSource.root, 'backups', 'unregistered-partial');
  await mkdir(stray);
  assert.equal(
    await withRetainedArtifactRelease(
      protectedSource.registry,
      f.claim('local', 'r2'),
      async () => ++calls,
      { r2: f.r2 },
    ),
    null,
  );
  assert.equal(calls, 0);
});

test('R2 inventory comes from the same actual client owner, and an unregistered remote-only backup cannot vanish from coverage', async (t) => {
  const f = await populated(t),
    calls = [];
  t.mock.method(f.r2, 'listBackups', async () => {
    calls.push('list');
    return [];
  });
  const protectedSource = await createRetainedReleaseFixture(t, f.db, f.dir, { r2: f.r2 });
  assert.equal(
    await withRetainedArtifactRelease(
      protectedSource.registry,
      f.claim('local', 'r2'),
      async () => 'ok',
      { r2: f.r2 },
    ),
    'ok',
  );
  assert.equal(calls.length, 2);
  t.mock.method(f.r2, 'listBackups', async () => [
    { key: `backups/daily-20261116T000000Z/${'b'.repeat(64)}`, bytes: 1 },
  ]);
  let called = false;
  assert.equal(
    await withRetainedArtifactRelease(
      protectedSource.registry,
      f.claim('local', 'r2'),
      async () => {
        called = true;
      },
      { r2: f.r2 },
    ),
    null,
  );
  assert.equal(called, false);
});
