import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { readFile, writeFile, unlink, stat } from 'node:fs/promises';
import {
  fixture,
  meta,
  NOW,
  image,
  databaseState,
  keys,
} from '../helpers/relational-generation.mjs';
import { describeStoredResult, publishStoredResult } from '../../server/storage/result-files.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { recoverRelationalGenerationResults } from '../../server/storage/relational-result-recovery.ts';
import {
  appendRelationalCommit,
  readRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import { relationalFactStream } from '../../server/events/relational-fact-contracts.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';

function descriptor(jobId) {
  return describeStoredResult(`artifact-${jobId}`, 'generate', {
    mimeType: 'image/webp',
    buffer: image,
  });
}
function prepare(f, jobId) {
  const value = descriptor(jobId);
  f.db.transaction(() =>
    f.generations.prepareGenerationResult(
      meta(`prepare-${jobId}`, NOW + 3),
      jobId,
      value,
      f.response(),
    ),
  )();
  return value;
}
function assertLocal(db, jobId, commitSeq) {
  const stored = readRelationalCommit(db, commitSeq),
    published = stored.events.find((e) => e.eventType === 'ArtifactPublished'),
    local = stored.events.find((e) => e.eventType === 'ArtifactReplicaRecorded');
  assert.ok(published);
  assert.ok(local, '原完成提交必须包含 Local 副本事实');
  assert.equal(local.eventIndex, published.eventIndex + 1);
  const target = db
    .prepare('SELECT kind,namespace,credential_ref FROM storage_targets WHERE id=?')
    .get(local.payload.storageTargetId);
  assert.deepEqual(target, { kind: 'local', namespace: 'images', credential_ref: null });
  const registration = stored.events.find((e) => e.eventType === 'StorageTargetRegistered');
  if (registration) {
    assert.ok(registration.eventIndex < published.eventIndex);
  }
  assert.deepEqual(
    db
      .prepare(
        'SELECT artifact_id,object_key,encoding,uploaded_at,verified_at,deleted_at,source_event_id FROM artifact_replicas WHERE artifact_id=?',
      )
      .get(`artifact-${jobId}`),
    {
      artifact_id: `artifact-${jobId}`,
      object_key: `artifact-${jobId}.webp`,
      encoding: 'identity',
      uploaded_at: stored.command.recordedAt,
      verified_at: stored.command.recordedAt,
      deleted_at: null,
      source_event_id: local.eventId,
    },
  );
  assert.equal(
    db.prepare('SELECT source_event_id FROM artifacts WHERE id=?').get(`artifact-${jobId}`)
      .source_event_id,
    published.eventId,
  );
  assert.equal(stored.command.actorKind, 'system');
  assert.equal(stored.command.actorId, null);
  assert.deepEqual(db.pragma('foreign_key_check'), []);
  return stored;
}

test('普通完成同笔登记 images 目标、发布和紧接 Local，第二张复用唯一目标且原回执不变', async (t) => {
  const f = await fixture(t);
  await f.accept('local', { samples: 2 });
  f.dispatch('local-0');
  const first = await f.publish('local-0');
  const stored = assertLocal(f.db, 'local-0', first.commitSeq);
  assert.equal(stored.events.filter((e) => e.eventType === 'StorageTargetRegistered').length, 1);
  assert.deepEqual(first.receipt, {
    jobId: 'local-0',
    artifactId: 'artifact-local-0',
    refundRetained: false,
  });
  f.dispatch('local-1', NOW + 10005);
  const second = await f.publish('local-1', NOW + 10007);
  const secondCommit = assertLocal(f.db, 'local-1', second.commitSeq);
  assert.equal(
    secondCommit.events.some((e) => e.eventType === 'StorageTargetRegistered'),
    false,
  );
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM storage_targets').get().n, 1);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM artifact_replicas').get().n, 2);
  const before = databaseState(f.db);
  f.db.transaction(() =>
    assert.equal(
      f.generations.completeGeneration(
        meta('complete-local-1', NOW + 10008),
        'local-1',
        descriptor('local-1'),
      ).replayed,
      true,
    ),
  )();
  assert.equal(databaseState(f.db), before);
});

test('只有 system/null 能准备或完成，旧回执也不能绕过；成员管理取消保持原授权', async (t) => {
  const f = await fixture(t);
  await f.accept('roles');
  f.dispatch('roles-0');
  const value = descriptor('roles-0'),
    actors = [
      { actorKind: 'admin', actorId: 'operator' },
      { actorKind: 'member', actorId: 'm1' },
      { actorKind: 'system', actorId: 'm1' },
    ];
  const rejected = (run) => {
    const before = databaseState(f.db);
    f.db.transaction(() => assert.throws(run, (error) => error.statusCode === 403))();
    assert.equal(databaseState(f.db), before);
  };
  for (const actor of actors) {
    rejected(() =>
      f.generations.prepareGenerationResult(
        { ...meta('prepare-roles-0', NOW + 3), ...actor },
        'roles-0',
        value,
        f.response(),
      ),
    );
  }
  prepare(f, 'roles-0');
  for (const actor of actors) {
    rejected(() =>
      f.generations.prepareGenerationResult(
        { ...meta('prepare-roles-0', NOW + 3), ...actor },
        'roles-0',
        value,
        f.response(),
      ),
    );
  }
  await publishStoredResult(f.dir, value, image);
  for (const actor of actors) {
    rejected(() =>
      f.generations.completeGeneration(
        { ...meta('complete-roles-0', NOW + 4), ...actor },
        'roles-0',
        value,
      ),
    );
  }
  const result = f.db.transaction(() =>
    f.generations.completeGeneration(meta('complete-roles-0', NOW + 4), 'roles-0', value),
  )();
  assertLocal(f.db, 'roles-0', result.commitSeq);
  for (const actor of actors) {
    rejected(() =>
      f.generations.completeGeneration(
        { ...meta('complete-roles-0', NOW + 4), ...actor },
        'roles-0',
        value,
      ),
    );
  }
  await f.accept('cancel');
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    () => f.generations.cancelWaitingGeneration(meta('member-cancel', NOW + 5, 'm1'), 'cancel'),
    {},
  );
  assert.equal(
    f.db.prepare("SELECT status FROM generation_jobs WHERE id='cancel-0'").get().status,
    'failed',
  );
});

test('Local 或终态写入故障后整笔回滚，保留文件供原恢复入口一次完成', async (t) => {
  for (const phase of ['replica', 'terminal']) {
    await t.test(phase, async (t) => {
      const f = await fixture(t);
      await f.accept('atomic-local', { samples: 2 });
      f.dispatch('atomic-local-0');
      const value = prepare(f, 'atomic-local-0');
      await publishStoredResult(f.dir, value, image);
      let reached = 0;
      f.db.function('observed_atomic_local', (artifacts, local, status) => {
        assert.equal(artifacts, 1);
        assert.equal(local, phase === 'replica' ? 0 : 1);
        assert.equal(status, phase === 'replica' ? 'reserved' : 'settled');
        reached++;
        return 1;
      });
      const table =
        phase === 'replica'
          ? 'BEFORE INSERT ON artifact_replicas'
          : "BEFORE UPDATE OF status ON generation_jobs WHEN NEW.status='done'";
      f.db.exec(
        `CREATE TEMP TRIGGER fail_atomic_local ${table} BEGIN SELECT observed_atomic_local((SELECT COUNT(*) FROM artifacts),(SELECT COUNT(*) FROM artifact_replicas),(SELECT status FROM charges)); SELECT RAISE(ABORT,'atomic Local fault'); END`,
      );
      const before = databaseState(f.db);
      f.db.transaction(() =>
        assert.throws(
          () =>
            f.generations.completeGeneration(
              meta('atomic-local-fault', NOW + 4),
              'atomic-local-0',
              value,
            ),
          /atomic Local fault/,
        ),
      )();
      assert.equal(reached, 1);
      assert.equal(databaseState(f.db), before);
      assert.deepEqual(await readFile(path.join(f.dir, value.file)), image);
      assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM artifacts').get().n, 0);
      assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM artifact_replicas').get().n, 0);
      f.db.exec('DROP TRIGGER fail_atomic_local');
      assert.deepEqual(
        await recoverRelationalGenerationResults(f.db, f.dir, { now: () => NOW + 5 }),
        { completed: 1, fileMissing: 0, unresolved: 0 },
      );
      const seq = f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS n FROM commits').get().n;
      assertLocal(f.db, 'atomic-local-0', seq);
    });
  }
});

test('final/完整 part 关闭重开后同笔恢复 Local，重试不追加回执或发起外部复制', async (t) => {
  for (const location of ['final', 'part']) {
    await t.test(location, async (t) => {
      let reopened;
      t.after(() => {
        if (reopened?.open) {
          reopened.close();
        }
      });
      const f = await fixture(t);
      await f.accept('reopened-local');
      f.dispatch('reopened-local-0');
      const value = prepare(f, 'reopened-local-0');
      if (location === 'final') {
        await publishStoredResult(f.dir, value, image);
      } else {
        await writeFile(path.join(f.dir, value.file) + '.part', image);
      }
      f.db.close();
      reopened = new Database(path.join(f.dir, 'library.sqlite'));
      reopened.pragma('foreign_keys=ON');
      const fetch = t.mock.method(globalThis, 'fetch', () => {
        throw new Error('unexpected external IO');
      });
      assert.deepEqual(
        await recoverRelationalGenerationResults(reopened, f.dir, { now: () => NOW + 5 }),
        { completed: 1, fileMissing: 0, unresolved: 0 },
      );
      const seq = reopened
        .prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS n FROM commits')
        .get().n;
      assertLocal(reopened, 'reopened-local-0', seq);
      const before = databaseState(reopened);
      assert.deepEqual(
        await recoverRelationalGenerationResults(reopened, f.dir, { now: () => NOW + 6 }),
        { completed: 0, fileMissing: 0, unresolved: 0 },
      );
      assert.equal(databaseState(reopened), before);
      assert.equal(fetch.mock.callCount(), 0);
    });
  }
});

test('退款迟到发布也包含 Local，原退款、失败和批次停止保持不变', async (t) => {
  const f = await fixture(t);
  await f.accept('late-local', { samples: 2 });
  const wallet = f.generations.readWallet('a', 'm1');
  f.dispatch('late-local-0');
  f.fail('late-local-0');
  const result = await f.publish('late-local-0', NOW + 4);
  assertLocal(f.db, 'late-local-0', result.commitSeq);
  assert.equal(result.receipt.refundRetained, true);
  assert.equal(f.generations.readCharge('late-local-0').status, 'refunded');
  assert.deepEqual(f.generations.readWallet('a', 'm1'), wallet);
  assert.deepEqual(f.db.prepare('SELECT status FROM generation_jobs ORDER BY sample_index').all(), [
    { status: 'failed' },
    { status: 'failed' },
  ]);
});

test('原完成混合提交整体回放保持顺序/回执，坏 Local 关系被外层捕获后仍全表回滚', async (t) => {
  const f = await fixture(t);
  await f.accept('mixed-local');
  f.dispatch('mixed-local-0');
  const value = prepare(f, 'mixed-local-0');
  await publishStoredResult(f.dir, value, image);
  const checkpoint = f.db.serialize();
  const result = f.db.transaction(() =>
    f.generations.completeGeneration(meta('complete-mixed-local', NOW + 4), 'mixed-local-0', value),
  )();
  const stored = assertLocal(f.db, 'mixed-local-0', result.commitSeq);
  const target = new Database(checkpoint);
  t.after(() => target.close());
  target.pragma('foreign_keys=ON');
  const generations = new RelationalGenerationStore(target);
  for (const change of ['key', 'encoding', 'member', 'truncated']) {
    const bad = structuredClone(stored),
      before = databaseState(target),
      index = bad.events.findIndex((e) => e.eventType === 'ArtifactReplicaRecorded');
    if (change === 'truncated') {
      bad.events.splice(index, 1);
      bad.command.events.splice(index, 1);
    } else {
      for (const event of [bad.events[index], bad.command.events[index]]) {
        if (change === 'key') {
          event.payload.objectKey = 'another-artifact.webp';
        } else if (change === 'encoding') {
          event.payload.encoding = 'br';
        } else {
          event.memberId = 'm2';
        }
      }
    }
    target.transaction(() =>
      assert.throws(() => generations.replayGenerationCommit(bad), /副本|归属|完整|complete/),
    )();
    assert.equal(databaseState(target), before);
  }
  await unlink(path.join(f.dir, value.file));
  const fetch = t.mock.method(globalThis, 'fetch', () => {
    throw new Error('unexpected replay IO');
  });
  target.transaction(() => generations.replayGenerationCommit(stored))();
  assert.equal(databaseState(target), databaseState(f.db));
  assert.deepEqual(readRelationalCommit(target, result.commitSeq), stored);
  const bytes = target.serialize();
  target.transaction(() =>
    assert.equal(generations.replayGenerationCommit(stored).replayed, true),
  )();
  assert.ok(target.serialize().equals(bytes));
  assert.equal(fetch.mock.callCount(), 0);
  await assert.rejects(stat(path.join(f.dir, value.file)), { code: 'ENOENT' });
});

test('旧契约可产生的原 admin 准备/完成历史原样回放，不改 actor/回执或补造 Local', async (t) => {
  const f = await fixture(t);
  await f.accept('historical');
  f.dispatch('historical-0');
  const value = descriptor('historical-0'),
    response = { ...f.response(), upstreamCompletedAt: null },
    charge = f.generations.readCharge('historical-0');
  const appendHistorical = (id, facts, receipt, at) =>
    f.db.transaction(() => {
      const { policyVersion, pricingVersion, ...command } = meta(id, at);
      const events = facts.map((fact) => {
        const kind = relationalFactStream(fact.eventType);
        let streamId = 'request:historical';
        if (kind === 'artifact') {
          streamId = `artifact:${value.imageId}`;
        } else if (kind === 'account') {
          streamId = 'account:a';
        }
        return {
          ...fact,
          streamId,
          memberId: 'm1',
          accountId: 'a',
          occurredAt: at,
          policyVersion,
          pricingVersion,
        };
      });
      const expectedVersions = Object.fromEntries(
        events.map((event) => [
          event.streamId,
          f.db
            .prepare(
              'SELECT CAST(stream_version AS TEXT) AS value FROM stream_heads WHERE stream_id=?',
            )
            .get(event.streamId)?.value || '0',
        ]),
      );
      // 合成旧未发布 owner 允许的原提交；不会调用新 IO 命令或在回放时重写 actor。
      const result = appendRelationalCommit(f.db, {
        ...command,
        actorKind: 'admin',
        actorId: 'historical-operator',
        expectedVersions,
        receipt,
        events,
      });
      new RelationalCommitReplayer(f.db, f.generations).applyCommit(result);
      return result;
    })();
  const prepared = appendHistorical(
    'historical-prepare',
    [
      {
        eventType: 'ArtifactPrepared',
        payload: {
          artifactId: value.imageId,
          attemptId: charge.attemptId,
          file: value.file,
          mimeType: value.mimeType,
          byteLength: value.byteLength,
          sha256: value.sha256,
          response,
        },
      },
    ],
    { jobId: 'historical-0', artifactId: value.imageId },
    NOW + 3,
  );
  await publishStoredResult(f.dir, value, image);
  const completed = appendHistorical(
    'historical-complete',
    [
      {
        eventType: 'ArtifactPublished',
        payload: {
          artifactId: value.imageId,
          memberId: 'm1',
          inputId: 'input-historical',
          originalAttemptId: charge.attemptId,
          operation: 'generate',
          mimeType: value.mimeType,
          byteLength: value.byteLength,
          sha256: value.sha256,
        },
      },
      {
        eventType: 'GenerationSettled',
        payload: { jobId: 'historical-0', reservationEventId: charge.reservationEventId },
      },
      {
        eventType: 'JobCompleted',
        payload: { jobId: 'historical-0', artifactId: value.imageId, response },
      },
    ],
    { jobId: 'historical-0', artifactId: value.imageId, refundRetained: false },
    NOW + 4,
  );
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(
    target,
    f.db.prepare('SELECT history_id FROM projection_state').get().history_id,
  );
  const replayer = new RelationalCommitReplayer(target),
    fetch = t.mock.method(globalThis, 'fetch', () => {
      throw new Error('unexpected historical replay IO');
    });
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), f.inputs))();
  }
  for (const result of [prepared, completed]) {
    const original = readRelationalCommit(f.db, result.commitSeq),
      replayed = readRelationalCommit(target, result.commitSeq);
    assert.deepEqual(replayed, original);
    assert.equal(replayed.command.actorKind, 'admin');
    assert.equal(replayed.command.actorId, 'historical-operator');
    assert.deepEqual(replayed.command.receipt, result.receipt);
  }
  assert.equal(new RelationalGenerationStore(target).readCharge('historical-0').status, 'settled');
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM artifacts').get().n, 1);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM artifact_replicas').get().n, 0);
  assert.equal(target.prepare('SELECT status FROM projection_state').get().status, 'building');
  assert.equal(fetch.mock.callCount(), 0);
});
