import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { fixture, meta, NOW, databaseState } from '../helpers/relational-generation.mjs';
import { createRetainedReleaseFixture } from '../helpers/retained-release.mjs';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import { R2Storage } from '../../server/storage/r2-storage.ts';
import { PikPakStorage } from '../../server/storage/pikpak-storage.ts';
import { withRetainedArtifactRelease } from '../../server/storage/retained-sources.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { relationalPrivateStateEmpty } from '../../server/storage/relational-private-restore.ts';

const DAY = 86400000,
  LATE = NOW + 8 * DAY,
  ARTIFACT = 'artifact-release-0';
async function setup(t, { cold = true } = {}) {
  const f = await fixture(t);
  await f.accept('release');
  f.dispatch('release-0');
  await f.publish('release-0');
  const busy = { value: false },
    r2 = new R2Storage({
      accountId: 'a'.repeat(32),
      bucket: 'release-images',
      accessKeyId: 'synthetic-access',
      secretAccessKey: 'synthetic-secret',
    }),
    pikpak = new PikPakStorage({
      url: 'https://dav.pikpak.test/release/',
      username: 'synthetic-user',
      password: 'synthetic-password',
    }),
    config = { dataDir: f.dir, r2, pikpak, idle: () => !busy.value },
    store = new RelationalReplicaStore(f.db, config),
    target = (kind) => store.readConfiguredStorageTarget(kind).storageTargetId,
    phase = (kind, state, at, errorCode = null) =>
      f.db.transaction(() =>
        store.recordArtifactReplica(
          meta(`${kind}:${state}:${at}`, at),
          store.beginArtifactReplica(ARTIFACT, target(kind)),
          state,
          errorCode,
        ),
      )();
  t.mock.method(r2, 'listBackups', async () => {
    assert.equal(f.db.inTransaction, false);
    return []; // 合成provider目录；不是实际云盘点结论。
  });
  for (const kind of ['r2', 'pikpak']) {
    f.db.transaction(() => store.registerStorageTarget(meta(`target:${kind}`), kind))();
  }
  phase('r2', 'reserved', NOW + 5);
  phase('r2', 'uploaded', NOW + 6);
  phase('r2', 'verified', NOW + 7);
  if (cold) {
    phase('pikpak', 'reserved', NOW + 8);
    phase('pikpak', 'uploaded', NOW + 9);
  }
  return { ...f, store, config, busy, target, phase };
}
function replay(t, f, through = null) {
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(
    target,
    f.db.prepare('SELECT history_id FROM projection_state').get().history_id,
  );
  const inputs = new RelationalInputStore(f.db),
    replayer = new RelationalCommitReplayer(target);
  for (const row of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    if (through !== null && BigInt(row.seq) > BigInt(through)) {
      break;
    }
    target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, row.seq), inputs))();
  }
  return target;
}
const release = (f, id, value, permit, at = LATE) =>
  f.store.recordArtifactRelease(meta(id, at), value.ticket, permit);

// Store tests produce acknowledged synthetic phases through the real command owner; no provider IO.
test('副本释放保留原时点，必须真实lease/原票据，原回执零写且完整公共回放不恢复认证', async (t) => {
  const f = await setup(t),
    before = databaseState(f.db),
    original = f.store.readArtifactReplica(ARTIFACT, f.target('local')),
    value = f.store.beginArtifactRelease(ARTIFACT, 'local', LATE);
  assert.deepEqual(value.claim.source, {
    kind: 'local',
    namespace: 'images',
    objectKey: 'artifact-release-0.webp',
    encoding: 'identity',
    dataDir: f.dir,
  });
  assert.throws(() => f.db.transaction(() => release(f, 'fake-permit', value, {}))(), /Retained/);
  assert.throws(() => f.store.assertArtifactReleaseTicket({ ...value.ticket }, LATE), /票据/);
  assert.equal(databaseState(f.db), before);
  const retained = await createRetainedReleaseFixture(t, f.db, f.dir, { r2: f.config.r2 });
  let used, commit;
  await withRetainedArtifactRelease(
    retained.registry,
    value.claim,
    async (permit) => {
      used = permit;
      assert.throws(
        () =>
          f.db.transaction(() =>
            f.store.recordArtifactRelease(meta('bad-actor', LATE, 'm1'), value.ticket, permit),
          )(),
        /后台/,
      );
      assert.equal(databaseState(f.db), before);
      commit = f.db.transaction(() => release(f, 'release-local', value, permit))();
      const row = f.store.readArtifactReplica(ARTIFACT, f.target('local'));
      assert.equal(row.uploadedAt, original.uploadedAt);
      assert.equal(row.verifiedAt, original.verifiedAt);
      assert.equal(row.deletedAt, LATE);
      assert.equal(f.store.beginArtifactRead(ARTIFACT, 'local'), null);
      const stable = databaseState(f.db),
        changes = f.db.prepare('SELECT total_changes() AS n').get().n;
      const duplicate = f.db.transaction(() => release(f, 'release-local', value, permit))();
      assert.equal(duplicate.replayed, true);
      assert.equal(duplicate.commitSeq, commit.commitSeq);
      assert.equal(databaseState(f.db), stable);
      assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
      f.busy.value = true;
      assert.throws(
        () => f.db.transaction(() => release(f, 'release-local', value, permit))(),
        /不可执行/,
      );
      f.busy.value = false;
    },
    { r2: f.config.r2 },
  );
  assert.throws(
    () => f.db.transaction(() => release(f, 'release-local', value, used))(),
    /Retained/,
  );
  assert.equal(JSON.stringify(commit.receipt).includes(f.dir), false, '不持久化Local机器路径');
  const restored = replay(t, f);
  for (const table of [
    'commits',
    'domain_events',
    'stream_heads',
    'artifacts',
    'storage_targets',
    'artifact_replicas',
    'generation_jobs',
    'generation_attempts',
    'charges',
    'wallets',
  ]) {
    assert.deepEqual(
      restored.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
      f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
      table,
    );
  }
  assert.equal(relationalPrivateStateEmpty(restored), true);
  const state = databaseState(restored),
    total = restored.prepare('SELECT total_changes() n').get().n;
  restored.transaction(() =>
    new RelationalCommitReplayer(restored).replayCommit(
      readRelationalCommit(f.db, commit.commitSeq),
      new RelationalInputStore(f.db),
    ),
  )();
  assert.equal(databaseState(restored), state);
  assert.equal(restored.prepare('SELECT total_changes() n').get().n, total);
});

test('释放必须保原24h/7d和同artifact双来源；专属delete失败1h后重试而不进入复制', async (t) => {
  const f = await setup(t),
    published = f.store.readArtifact(ARTIFACT).publishedAt;
  assert.equal(f.store.readDueArtifactRelease(published + DAY), null);
  assert.equal(f.store.readDueArtifactRelease(published + DAY + 1).kind, 'local');
  assert.throws(() => f.store.beginArtifactRelease(ARTIFACT, 'local', published + DAY), /保留期/);
  const verified = f.store.readArtifactReplica(ARTIFACT, f.target('r2')).verifiedAt;
  assert.throws(() => f.store.beginArtifactRelease(ARTIFACT, 'r2', verified + 7 * DAY), /保留期/);
  const local = f.store.readDueArtifactRelease(LATE);
  assert.equal(local.kind, 'local');
  const hot = f.store.readDueArtifactRelease(LATE, local.artifactId, local.kind);
  assert.equal(hot.kind, 'r2', '同一artifact Local pin不能遮挡R2候选');
  assert.equal(f.store.readDueArtifactRelease(LATE, hot.artifactId, hot.kind), null);
  const ticket = f.store.beginArtifactRelease(ARTIFACT, 'r2', LATE);
  f.phase('r2', 'failed', LATE, 'REPLICA_RELEASE_FAILED');
  assert.throws(() => f.store.assertArtifactReleaseTicket(ticket.ticket, LATE), /另一提交|重试/);
  assert.equal(f.store.readDueArtifactReplica(LATE + 2 * 3600000), null);
  assert.equal(f.store.readDueArtifactRelease(LATE + 3600000, ARTIFACT, 'local'), null);
  assert.equal(f.store.readDueArtifactRelease(LATE + 3600001, ARTIFACT, 'local').kind, 'r2');
  const state = databaseState(f.db);
  assert.throws(() => f.phase('r2', 'verified', LATE + 3600001), /删除未决/);
  assert.throws(() => f.phase('r2', 'failed', LATE + 3600001, 'REPLICA_IO_FAILED'), /删除未决/);
  assert.equal(databaseState(f.db), state);
  const other = new RelationalReplicaStore(f.db, {
    ...f.config,
    r2: new R2Storage({
      ...f.config.r2.config,
      bucket: 'wrong-release-images',
    }),
  });
  assert.throws(() => other.assertArtifactReleaseTicket(ticket.ticket, LATE), /票据/);
  const oldLocal = f.store.beginArtifactRelease(ARTIFACT, 'r2', LATE + 3600001);
  f.config.r2 = new R2Storage({ ...f.config.r2.config, secretAccessKey: 'changed-secret' });
  assert.throws(() => f.store.assertArtifactReleaseTicket(oldLocal.ticket, LATE + 3600001), /配置/);
});

test('删除投影尾错在外层catch后全回滚并可原票据健康重试；完整commit不得丢换删除证据', async (t) => {
  const f = await setup(t),
    retained = await createRetainedReleaseFixture(t, f.db, f.dir, { r2: f.config.r2 }),
    value = f.store.beginArtifactRelease(ARTIFACT, 'r2', LATE),
    before = databaseState(f.db),
    count = f.db.prepare('SELECT COUNT(*) n FROM commits').get().n;
  let reached = false,
    saved;
  await withRetainedArtifactRelease(
    retained.registry,
    value.claim,
    async (permit) => {
      const prepare = f.db.prepare,
        fault = t.mock.method(f.db, 'prepare', function (sql) {
          const statement = prepare.call(this, sql);
          if (!sql.startsWith('INSERT INTO artifact_replicas')) {
            return statement;
          }
          return new Proxy(statement, {
            get(target, key) {
              if (key === 'run') {
                return () => {
                  assert.equal(
                    prepare.call(f.db, 'SELECT COUNT(*) n FROM commits').get().n,
                    count + 1,
                  );
                  assert.equal(
                    prepare
                      .call(
                        f.db,
                        'SELECT payload FROM domain_events ORDER BY commit_seq DESC,event_index DESC LIMIT 1',
                      )
                      .get()
                      .payload.includes('"deletedAt":' + LATE),
                    true,
                  );
                  reached = true;
                  throw new Error('synthetic release projection tail SQL failure');
                };
              }
              const result = Reflect.get(target, key, target);
              return typeof result === 'function' ? result.bind(target) : result;
            },
          });
        });
      try {
        f.db.transaction(() => {
          assert.throws(() => release(f, 'release-hot', value, permit), /tail SQL failure/);
          assert.equal(databaseState(f.db), before);
        })();
      } finally {
        fault.mock.restore();
      }
      assert.equal(reached, true);
      saved = f.db.transaction(() => release(f, 'release-hot', value, permit))();
    },
    { r2: f.config.r2 },
  );
  const stored = readRelationalCommit(f.db, saved.commitSeq),
    beforeSeq = String(BigInt(saved.commitSeq) - 1n);
  for (const mode of [
    'missing-proof',
    'wrong-source',
    'wrong-destination',
    'wrong-claim',
    'not-deleted',
  ]) {
    const target = replay(t, f, beforeSeq),
      state = databaseState(target),
      command = structuredClone(stored.command);
    if (mode === 'missing-proof') {
      delete command.receipt.release;
    } else if (mode === 'wrong-source') {
      command.receipt.release.sourceEventId = command.receipt.release.destinationEventId;
    } else if (mode === 'wrong-destination') {
      command.receipt.release.destinationEventId = command.receipt.release.sourceEventId;
    } else if (mode === 'wrong-claim') {
      command.receipt.release.claimSha256 = 'f'.repeat(64);
    } else {
      command.events[0].payload.deletedAt = null;
      command.events[0].payload.lastErrorCode = 'REPLICA_RELEASE_FAILED';
    }
    target.transaction(() => {
      assert.throws(() => new RelationalCommitReplayer(target).appendCommit(command), /删除/);
      assert.equal(databaseState(target), state, mode);
    })();
  }
});

test('Opening verifiedAt/uploadedAt已知0可释放，未知NULL不能冒充持久目的', async (t) => {
  const f = await setup(t),
    original = f.store.readArtifact(ARTIFACT),
    artifactId = 'zero-artifact',
    origin = { sourceId: 'synthetic-opening', originalId: artifactId },
    { policyVersion, pricingVersion, ...command } = meta('zero-source');
  const facts = [
    {
      eventType: 'OpeningArtifactRecorded',
      payload: {
        artifactId,
        memberId: 'm1',
        inputId: original.inputId,
        originalAttemptId: null,
        operation: 'generate',
        mimeType: original.mimeType,
        byteLength: original.byteLength,
        sha256: original.sha256,
        publishedAt: 0,
        retiredAt: null,
        origin,
      },
    },
    ...['local', 'r2', 'pikpak'].map((kind) => ({
      eventType: 'OpeningReplicaRecorded',
      payload: {
        artifactId,
        storageTargetId: f.target(kind),
        encoding: kind === 'pikpak' ? 'br' : 'identity',
        objectKey: {
          local: `${artifactId}.webp`,
          r2: `images/${artifactId}/${original.sha256}`,
          pikpak: original.sha256 + '.br',
        }[kind],
        uploadedAt: kind === 'pikpak' ? 0 : null,
        verifiedAt: kind === 'pikpak' ? null : 0,
        deletedAt: null,
        lastErrorCode: null,
        origin,
      },
    })),
  ];
  f.db.transaction(() =>
    new RelationalCommitReplayer(f.db).appendCommit({
      ...command,
      expectedVersions: { ['artifact:' + artifactId]: '0' },
      receipt: { originalZero: true },
      events: facts.map((fact) => ({
        ...fact,
        streamId: 'artifact:' + artifactId,
        memberId: 'm1',
        accountId: null,
        occurredAt: NOW,
        policyVersion,
        pricingVersion,
      })),
    }),
  )();
  const at = 7 * DAY + 1;
  assert.equal(
    f.store.beginArtifactRelease(artifactId, 'local', at).destination.replica.verifiedAt,
    0,
  );
  assert.equal(
    f.store.beginArtifactRelease(artifactId, 'r2', at).destination.replica.uploadedAt,
    0,
  );
  assert.equal(f.store.readDueArtifactRelease(at).kind, 'local');
  assert.equal(f.store.readDueArtifactRelease(at, artifactId, 'local').kind, 'r2');
  for (const uploadedAt of [null, 0]) {
    const id = uploadedAt === null ? 'unknown-local' : 'unverified-local',
      origin = { sourceId: 'synthetic-opening', originalId: id },
      incompleteFacts = structuredClone(facts.slice(0, 3));
    for (const fact of incompleteFacts) {
      fact.payload.artifactId = id;
      fact.payload.origin = origin;
      if (fact.eventType === 'OpeningReplicaRecorded') {
        fact.payload.objectKey = fact.payload.objectKey.replace(artifactId, id);
        if (fact.payload.storageTargetId === f.target('local')) {
          fact.payload.uploadedAt = uploadedAt;
          fact.payload.verifiedAt = null;
        }
      }
    }
    f.db.transaction(() =>
      new RelationalCommitReplayer(f.db).appendCommit({
        ...command,
        commandId: id,
        fingerprint: meta(id).fingerprint,
        expectedVersions: { ['artifact:' + id]: '0' },
        receipt: { unknownLocal: true },
        events: incompleteFacts.map((fact) => ({
          ...fact,
          streamId: 'artifact:' + id,
          memberId: 'm1',
          accountId: null,
          occurredAt: NOW,
          policyVersion,
          pricingVersion,
        })),
      }),
    )();
    const state = databaseState(f.db);
    assert.throws(() => f.store.beginArtifactRelease(id, 'local', at), /持久目的|原定位/);
    assert.equal(databaseState(f.db), state);
  }
  const auxiliaryId = 'auxiliary-artifact',
    auxiliaryFacts = structuredClone(facts),
    auxiliaryOrigin = { sourceId: 'synthetic-opening', originalId: auxiliaryId };
  for (const fact of auxiliaryFacts) {
    fact.payload.artifactId = auxiliaryId;
    fact.payload.origin = auxiliaryOrigin;
    if (fact.eventType === 'OpeningArtifactRecorded') {
      fact.payload.operation = 'encode-vibe';
      fact.payload.mimeType = 'application/octet-stream';
      fact.payload.inputId = 'auxiliary-input';
    } else {
      fact.payload.objectKey = fact.payload.objectKey.replace(artifactId, auxiliaryId);
      if (fact.payload.storageTargetId === f.target('local')) {
        fact.payload.objectKey = auxiliaryId + '.bin';
      }
    }
  }
  f.db.transaction(() => {
    const input = f.inputs.stageOpeningInput({
      inputId: 'auxiliary-input',
      memberId: 'm1',
      completeness: 'unavailable',
      body: null,
      assets: [],
      logicalBytes: 0,
    });
    new RelationalCommitReplayer(f.db).appendCommit(
      {
        ...command,
        commandId: auxiliaryId,
        fingerprint: meta(auxiliaryId).fingerprint,
        expectedVersions: { ['artifact:' + auxiliaryId]: '0', 'input:auxiliary-input': '0' },
        receipt: { auxiliarySource: true },
        events: [
          {
            eventType: 'OpeningInputRecorded',
            payload: { ...input, origin: auxiliaryOrigin },
            streamId: 'input:auxiliary-input',
            memberId: 'm1',
            accountId: null,
            occurredAt: NOW,
            policyVersion,
            pricingVersion,
          },
          ...auxiliaryFacts.map((fact) => ({
            ...fact,
            streamId: 'artifact:' + auxiliaryId,
            memberId: 'm1',
            accountId: null,
            occurredAt: NOW,
            policyVersion,
            pricingVersion,
          })),
        ],
      },
      f.inputs,
    );
  })();
  assert.throws(() => f.store.beginArtifactRelease(auxiliaryId, 'local', at), /保留期/);
  assert.throws(() => f.store.beginArtifactRelease(auxiliaryId, 'r2', at), /保留期/);
  assert.equal(f.store.readDueArtifactRelease(at).artifactId, artifactId);
  const pendingId = 'opening-release-without-original-attempt',
    pendingFacts = structuredClone(facts);
  for (const fact of pendingFacts) {
    fact.payload.artifactId = pendingId;
    fact.payload.origin = { sourceId: 'synthetic-opening', originalId: pendingId };
    if (fact.eventType === 'OpeningReplicaRecorded') {
      fact.payload.objectKey = fact.payload.objectKey.replace(artifactId, pendingId);
      if (fact.payload.storageTargetId === f.target('r2')) {
        fact.payload.lastErrorCode = 'REPLICA_RELEASE_FAILED';
      }
    }
  }
  f.db.transaction(() =>
    new RelationalCommitReplayer(f.db).appendCommit({
      ...command,
      commandId: pendingId,
      fingerprint: meta(pendingId).fingerprint,
      expectedVersions: { ['artifact:' + pendingId]: '0' },
      receipt: { originalFailureUnknown: true },
      events: pendingFacts.map((fact) => ({
        ...fact,
        streamId: 'artifact:' + pendingId,
        memberId: 'm1',
        accountId: null,
        occurredAt: NOW,
        policyVersion,
        pricingVersion,
      })),
    }),
  )();
  assert.throws(() => f.store.beginArtifactRelease(pendingId, 'r2', LATE), /重试期限/);
  const incomplete = await setup(t, { cold: false });
  assert.throws(() => incomplete.store.beginArtifactRelease(ARTIFACT, 'r2', LATE), /持久目的/);
});
