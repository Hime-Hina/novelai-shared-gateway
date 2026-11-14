import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rm, lstat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { randomUUID } from 'node:crypto';
import { fixture, meta, keys, NOW, image } from '../helpers/relational-generation.mjs';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import { describeStoredResult } from '../../server/storage/result-files.ts';
import { describeRetainedSnapshot } from '../../scripts/retained-sources.mjs';
import {
  initializeRetainedSources,
  readRetainedSourceProtection,
  beginRetainedCapture,
  bindRetainedCapture,
  publishRetainedLocal,
  beginRetainedRemote,
  confirmRetainedRemote,
  beginRetainedRetirement,
  confirmRetainedRetirement,
  retainedFileSha256,
  checkpointRetainedSources,
  restoreRetainedSources,
  inspectRetainedLease,
  recoverRetainedLease,
} from '../../server/storage/retained-sources.ts';
async function temporary(t) {
  const root = await mkdtemp(path.join(tmpdir(), 'nai-retained-sources-'));
  t.after(() => rm(root, { recursive: true, force: true }));
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
