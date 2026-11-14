import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { readFile } from 'node:fs/promises';
import {
  fixture,
  installReady,
  meta,
  keys,
  NOW,
  image,
  databaseState,
} from '../helpers/relational-generation.mjs';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import { RelationalRequestStore } from '../../server/storage/relational-request-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import {
  appendRelationalCommit,
  readRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import {
  readRelationalArtifactContent,
  copyRelationalArtifactReplica,
} from '../../server/storage/relational-replica-io.ts';
import { R2Storage } from '../../server/storage/r2-storage.ts';
import { describeStoredResult } from '../../server/storage/result-files.ts';
import { createRelationalMemberGenerationAdmission } from '../../server/generation/relational-member-admission.ts';
import { createGenerationPayloadBudget } from '../../server/generation/payload-budget.ts';
import { normalizeMemberGeneration } from '../../server/generation/member-contract.ts';
import {
  withAuthenticatedMemberApiKey,
  withAuthenticatedMemberSession,
  createMemberSession,
  logoutMemberSession,
} from '../../server/members/authentication.ts';

async function setup(t, options = {}) {
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { ...options, env }),
    controls = new Map(),
    replicas = new RelationalReplicaStore(f.db, undefined, env),
    queries = new RelationalRequestStore(f.db);
  const asKey = (id, run, memberId = 'm1', key = keys[memberId]) =>
    withAuthenticatedMemberApiKey(
      f.members,
      key,
      (actor) => run(meta(id, NOW + 90000, actor.id), actor),
      env,
    );
  return {
    ...f,
    env,
    controls,
    replicas,
    queries,
    asKey,
    retireJobs: (id, ids) =>
      asKey(id, (metadata, actor) =>
        f.generations.retireJobHistory(metadata, ids, actor, controls),
      ),
    retireArtifacts: (id, ids) =>
      asKey(id, (metadata, actor) =>
        replicas.retireArtifactHistory(metadata, ids, actor, controls),
      ),
  };
}
async function complete(f, id, now = NOW, options = {}) {
  await f.accept(id, { ...options, now });
  f.dispatch(`${id}-0`, now + 1);
  return f.publish(`${id}-0`, now + 3);
}
const rows = (db, table) => db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all();
function evidence(db) {
  return Object.fromEntries(
    [
      'generation_inputs',
      'input_assets',
      'generation_attempts',
      'charges',
      'wallets',
      'idempotency_keys',
      'artifact_replicas',
      'account_quota_state',
      'upstream_credentials',
    ].map((table) => [table, rows(db, table)]),
  );
}

test('owner Key 退休部分/全部 child 只改薄标记，原输入/收费/副本/旧键与文件保留', async (t) => {
  const f = await setup(t);
  await f.accept('batch', { samples: 2 });
  f.dispatch('batch-0');
  await f.publish('batch-0');
  f.dispatch('batch-1', NOW + 10005);
  await f.publish('batch-1', NOW + 10007);
  const before = evidence(f.db),
    first = await f.retireJobs('retire-first', ['batch-0']);
  assert.deepEqual(first.receipt, { retiredJobIds: ['batch-0'] });
  assert.equal(f.db.prepare('SELECT retired_at FROM generation_requests').get().retired_at, null);
  assert.deepEqual(
    f.queries.readMemberRequest('batch', 'm1').jobs.map((j) => j.id),
    ['batch-1'],
  );
  assert.throws(() => f.queries.readMemberResult('batch-0', 'm1'), { statusCode: 410 });
  assert.equal(f.inputs.readMemberInput('batch', 'm1').input.operation, 'generate');
  await f.retireJobs('retire-last', ['batch-1']);
  assert.equal(
    f.db.prepare('SELECT retired_at FROM generation_requests').get().retired_at,
    NOW + 90000,
  );
  assert.deepEqual(f.queries.readMemberRequests('m1').requests, []);
  assert.throws(() => f.queries.readMemberRequest('batch', 'm1'), { code: 'request_not_found' });
  assert.throws(() => f.inputs.readMemberInput('batch', 'm1'), { code: 'input_unavailable' });
  assert.ok(f.inputs.readJobInput('batch-0'));
  assert.deepEqual(evidence(f.db), before);
  for (const id of ['batch-0', 'batch-1']) {
    assert.deepEqual(await readFile(path.join(f.dir, `images/artifact-${id}.webp`)), image);
  }
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_jobs').get().n, 2);
  assert.deepEqual(f.db.pragma('foreign_key_check'), []);
  const checkpoint = f.db.serialize();
  assert.equal((await f.retireJobs('retire-first', ['batch-0'])).replayed, true);
  assert.ok(f.db.serialize().equals(checkpoint));
  await assert.rejects(f.retireJobs('retire-first', ['batch-1']), /different|意图|conflict/i);
  assert.ok(f.db.serialize().equals(checkpoint));
});

test('artifact 人工退休保留原 job/late/退款与所有字节，读取不再暴露已退休结果', async (t) => {
  const f = await setup(t);
  await f.accept('late', { samples: 2 });
  f.dispatch('late-0');
  f.fail('late-0', { aborted: false });
  await f.publish('late-0');
  const before = evidence(f.db),
    jobs = rows(f.db, 'generation_jobs'),
    fetch = t.mock.method(globalThis, 'fetch', () => {
      throw new Error('retirement cannot access external IO');
    });
  const result = await f.retireArtifacts('retire-image', ['artifact-late-0']);
  assert.deepEqual(result.receipt, { retiredArtifactIds: ['artifact-late-0'] });
  assert.deepEqual(rows(f.db, 'generation_jobs'), jobs);
  assert.deepEqual(evidence(f.db), before);
  assert.equal(f.generations.readCharge('late-0').status, 'refunded');
  const dto = f.queries.readMemberRequest('late', 'm1').jobs[0];
  assert.equal(dto.imageId, null);
  assert.equal(dto.lateResultAvailable, false);
  assert.throws(() => f.queries.readMemberResult('late-0', 'm1'), { code: 'result_unavailable' });
  const artifact = f.replicas.readArtifact('artifact-late-0');
  assert.equal(artifact.source.eventType, 'ArtifactPublished');
  assert.equal(artifact.retiredAt, NOW + 90000);
  assert.equal(
    readRelationalCommit(f.db, result.commitSeq).events[0].eventId,
    artifact.sourceEventId,
  );
  await assert.rejects(readRelationalArtifactContent(f.db, artifact.id, { dataDir: f.dir }), {
    code: 'ARTIFACT_CONTENT_UNAVAILABLE',
  });
  assert.deepEqual(await readFile(path.join(f.dir, artifact.descriptor.file)), image);
  assert.deepEqual(f.replicas.readRecoverableArtifacts(null, 100), []);
  assert.equal(fetch.mock.callCount(), 0);
  const once = f.db.serialize();
  assert.equal((await f.retireArtifacts('retire-image', ['artifact-late-0'])).replayed, true);
  assert.ok(f.db.serialize().equals(once));
});

test('真实受理桥原键保留；图退休只返回原回执，任一原 child 历史退休后不再执行旧键', async (t) => {
  const f = await setup(t);
  await installReady(f);
  const input = {
      operation: 'generate',
      model: 'nai-diffusion-4-5-full',
      input: 'retirement idempotency input',
      parameters: { width: 128, height: 128, steps: 30, seed: 42, n_samples: 2 },
    },
    body = {
      ...input,
      confirmation: {
        requestFingerprint: normalizeMemberGeneration(input).requestFingerprint,
        versions: f.generations.readMemberGenerationCapabilities('m1').versions,
        maxPerJob: { v5: '100', anlasFixed: '4000', anlasPurchased: '4000' },
        maxTotal: { v5: '100', anlasFixed: '4000', anlasPurchased: '4000' },
      },
    },
    admit = createRelationalMemberGenerationAdmission({
      ...f,
      sqlite: f.db,
      payloadBudget: createGenerationPayloadBudget(() => f.inputs.activePayloadBytes()),
      env: f.env,
      now: () => NOW,
    }),
    request = {
      headers: { 'idempotency-key': 'original-retired-key' },
      rawHeaders: ['Idempotency-Key', 'original-retired-key'],
    },
    submit = () => admit({ credential: { apiKey: keys.m1 }, request, body }),
    accepted = await submit(),
    [first, second] = accepted.receipt.jobIds;
  f.dispatch(first);
  await f.publish(first);
  f.dispatch(second, NOW + 10005);
  await f.publish(second, NOW + 10007);
  const original = rows(f.db, 'idempotency_keys');
  assert.equal(original.length, 1);
  await f.retireArtifacts('retire-original-image', [`artifact-${first}`]);
  const before = databaseState(f.db);
  assert.deepEqual((await submit()).receipt, accepted.receipt);
  assert.equal(databaseState(f.db), before);
  await f.retireJobs('retire-original-child', [first]);
  let retired = databaseState(f.db);
  await assert.rejects(submit(), { statusCode: 410 });
  assert.equal(databaseState(f.db), retired);
  await f.retireJobs('retire-original-rest', [second]);
  retired = databaseState(f.db);
  await assert.rejects(submit(), { statusCode: 410 });
  assert.equal(databaseState(f.db), retired);
  assert.deepEqual(rows(f.db, 'idempotency_keys'), original);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_jobs').get().n, 2);
});

test('活动 child、已关闭核对但未发布准备、pending review、物理 promise 与同请求 sibling 均保护', async (t) => {
  for (const kind of ['queued', 'prepared', 'review', 'physical', 'sibling']) {
    await t.test(kind, async (t) => {
      const f = await setup(t);
      await f.accept('protected', { samples: kind === 'sibling' ? 2 : 1 });
      if (kind !== 'queued') {
        f.dispatch('protected-0');
      }
      if (kind === 'prepared') {
        const descriptor = describeStoredResult('artifact-protected-0', 'generate', {
          mimeType: 'image/webp',
          buffer: image,
        });
        f.db.transaction(() =>
          f.generations.prepareGenerationResult(
            meta('prepare-protected', NOW + 2),
            'protected-0',
            descriptor,
            f.response(),
          ),
        )();
        f.fail('protected-0', { aborted: false });
        f.db.transaction(() =>
          f.generations.closeGenerationReview(meta('close-prepared', NOW + 300003), 'protected-0'),
        )();
      } else if (kind === 'review') {
        f.fail('protected-0', { aborted: false });
      } else if (kind === 'physical' || kind === 'sibling') {
        await f.publish('protected-0');
      }
      if (kind === 'physical') {
        assert.ok(
          f.db.prepare('SELECT upstream_completed_at FROM generation_attempts').get()
            .upstream_completed_at,
        );
        f.controls.set('protected-0', { accountId: 'a' });
      }
      const before = databaseState(f.db);
      await assert.rejects(f.retireJobs(`blocked-${kind}`, ['protected-0']), /不能退休/);
      assert.equal(databaseState(f.db), before);
      if (kind === 'physical' || kind === 'sibling') {
        await assert.rejects(
          f.retireArtifacts(`blocked-artifact-${kind}`, ['artifact-protected-0']),
          /不能退休/,
        );
        assert.equal(databaseState(f.db), before);
        if (kind === 'physical') {
          f.controls.delete('protected-0');
        } else {
          f.db.transaction(() =>
            f.generations.cancelWaitingGeneration(meta('cancel-sibling'), 'protected'),
          )();
        }
        await f.retireArtifacts(`released-${kind}`, ['artifact-protected-0']);
      }
    });
  }
});

test('人工选择有界且规范排序；无关请求活跃不阻止安全的人工退休', async (t) => {
  const f = await setup(t);
  await complete(f, 'alpha');
  await complete(f, 'beta', NOW + 20000);
  await f.accept('unrelated', { memberId: 'm2', now: NOW + 40000 });
  f.dispatch('unrelated-0', NOW + 40001);
  f.controls.set('unrelated-0', { accountId: 'a' });
  const before = databaseState(f.db);
  for (const [index, ids] of [
    [],
    ['alpha-0', 'alpha-0'],
    Array.from({ length: 201 }, (_, i) => `job-${i}`),
    [new String('alpha-0')],
    [''],
  ].entries()) {
    await assert.rejects(f.retireJobs(`bounds-${index}`, ids), { statusCode: 400 });
    assert.equal(databaseState(f.db), before);
  }
  const first = await f.retireJobs('canonical-selection', ['beta-0', 'alpha-0']);
  assert.deepEqual(first.receipt, { retiredJobIds: ['alpha-0', 'beta-0'] });
  const once = f.db.serialize();
  assert.equal((await f.retireJobs('canonical-selection', ['alpha-0', 'beta-0'])).replayed, true);
  assert.ok(f.db.serialize().equals(once));
  assert.equal(
    f.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get('unrelated-0').status,
    'running',
  );
});

test('artifact 引用保护走真实窄索引，另一个请求的未发布准备也能阻止退休', async (t) => {
  const f = await setup(t);
  await complete(f, 'original');
  await f.accept('other', { memberId: 'm2', now: NOW + 20000 });
  f.dispatch('other-0', NOW + 20001);
  const descriptor = f.replicas.readArtifact('artifact-original-0').descriptor;
  f.db.transaction(() =>
    f.generations.prepareGenerationResult(
      meta('other-prepared', NOW + 20002),
      'other-0',
      descriptor,
      f.response(NOW + 20002),
    ),
  )();
  let guardedSql;
  const prepare = f.db.prepare.bind(f.db);
  t.mock.method(f.db, 'prepare', (sql, ...args) => {
    if (sql.startsWith('SELECT request_id AS requestId FROM generation_jobs')) {
      guardedSql = sql;
    }
    return prepare(sql, ...args);
  });
  const before = databaseState(f.db);
  await assert.rejects(
    f.retireArtifacts('guard-other-prepare', ['artifact-original-0']),
    /不能退休/,
  );
  assert.equal(databaseState(f.db), before);
  assert.ok(guardedSql);
  const plan = f.db
    .prepare(`EXPLAIN QUERY PLAN ${guardedSql}`)
    .all(descriptor.imageId, descriptor.imageId, descriptor.imageId)
    .map((row) => row.detail)
    .join('\n');
  assert.match(plan, /idx_jobs_artifact/);
  assert.match(plan, /idx_attempts_prepared_artifact/);
  assert.doesNotMatch(plan, /\bSCAN\b|TEMP B-TREE/);
});

test('退休后新结果准备与伪造准备回放拒绝，后台复制也不能先留下无关目标事实', async (t) => {
  const f = await setup(t);
  await f.accept('failed');
  f.dispatch('failed-0');
  f.fail('failed-0', { aborted: false });
  f.db.transaction(() =>
    f.generations.closeGenerationReview(meta('close-before-retire', NOW + 300003), 'failed-0'),
  )();
  await f.retireJobs('failed-history-retire', ['failed-0']);
  const descriptor = describeStoredResult('new-after-retire', 'generate', {
      buffer: image,
      mimeType: 'image/webp',
    }),
    before = databaseState(f.db);
  f.db.transaction(() =>
    assert.throws(
      () =>
        f.generations.prepareGenerationResult(
          meta('prepare-after-retire'),
          'failed-0',
          descriptor,
          f.response(),
        ),
      /准备/,
    ),
  )();
  assert.equal(databaseState(f.db), before);
  const forged = new Database(f.db.serialize());
  try {
    const event = {
        eventType: 'ArtifactPrepared',
        payload: {
          artifactId: descriptor.imageId,
          attemptId: 'attempt-failed-0',
          file: descriptor.file,
          mimeType: descriptor.mimeType,
          byteLength: descriptor.byteLength,
          sha256: descriptor.sha256,
          response: f.response(),
        },
        streamId: 'request:failed',
        memberId: 'm1',
        accountId: 'a',
        occurredAt: NOW + 90001,
        policyVersion: 'shared-quota-v1',
        pricingVersion: 'price1',
      },
      { policyVersion, pricingVersion, ...command } = meta('forged-prepared', NOW + 90001);
    const appended = forged.transaction(() =>
      appendRelationalCommit(forged, {
        ...command,
        events: [event],
        receipt: { jobId: 'failed-0', artifactId: descriptor.imageId },
        expectedVersions: {
          [event.streamId]: forged
            .prepare(
              'SELECT CAST(stream_version AS TEXT) AS version FROM stream_heads WHERE stream_id=?',
            )
            .get(event.streamId).version,
        },
      }),
    )();
    f.db.transaction(() =>
      assert.throws(
        () =>
          f.generations.replayGenerationCommit(readRelationalCommit(forged, appended.commitSeq)),
        /准备/,
      ),
    )();
    assert.equal(databaseState(f.db), before);
  } finally {
    forged.close();
  }
  await complete(f, 'image', NOW + 400000);
  await f.retireArtifacts('image-history-retire', ['artifact-image-0']);
  const retired = databaseState(f.db),
    network = t.mock.method(globalThis, 'fetch', () => {
      throw new Error('retired copies cannot access network');
    });
  await assert.rejects(
    copyRelationalArtifactReplica(f.db, 'artifact-image-0', 'r2', {
      dataDir: f.dir,
      idle: () => true,
      r2: new R2Storage({
        accountId: 'a'.repeat(32),
        bucket: 'retired-images',
        accessKeyId: 'private-access',
        secretAccessKey: 'private-secret',
      }),
    }),
    /Retired/,
  );
  assert.equal(databaseState(f.db), retired);
  assert.equal(network.mock.callCount(), 0);
});

test('当前 owner Key/Cookie 和 epoch 先于重领；旧 Key、退出、普通成员、伪 actor 与新 owner 配置拒绝', async (t) => {
  const f = await setup(t);
  await complete(f, 'key');
  await complete(f, 'cookie', NOW + 20000);
  let snapshot;
  await f.asKey('snapshot', (_metadata, actor) => {
    snapshot = actor;
  });
  const session = await createMemberSession(f.members, snapshot, f.env, NOW);
  const keyResult = await f.retireJobs('key-retire', ['key-0']);
  const cookieResult = await withAuthenticatedMemberSession(
    f.members,
    session.token,
    f.env,
    (actor) =>
      f.replicas.retireArtifactHistory(
        meta('cookie-retire', NOW + 90000, actor.id),
        ['artifact-cookie-0'],
        actor,
        f.controls,
      ),
    NOW + 1,
  );
  assert.ok(keyResult.commitSeq);
  assert.ok(cookieResult.commitSeq);
  let before = databaseState(f.db);
  await assert.rejects(
    f.asKey(
      'not-owner',
      (metadata, actor) =>
        f.generations.retireJobHistory(metadata, ['cookie-0'], actor, f.controls),
      'm2',
    ),
    { statusCode: 403 },
  );
  await assert.rejects(
    f.asKey(
      'forged-actor',
      (_metadata, actor) =>
        f.replicas.retireArtifactHistory(
          meta('forged-actor', NOW, 'm1'),
          ['artifact-cookie-0'],
          actor,
          f.controls,
        ),
      'm2',
    ),
    { statusCode: 403 },
  );
  f.db.transaction(() =>
    assert.throws(
      () =>
        f.generations.retireJobHistory(meta('system-not-admin'), ['cookie-0'], null, f.controls),
      { statusCode: 403 },
    ),
  )();
  assert.equal(databaseState(f.db), before);
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'new-owner-key'))();
  before = databaseState(f.db);
  assert.equal(await f.retireJobs('key-retire', ['key-0']), null);
  assert.equal(databaseState(f.db), before);
  await logoutMemberSession(f.members, session.token);
  before = databaseState(f.db);
  assert.equal(
    await withAuthenticatedMemberSession(
      f.members,
      session.token,
      f.env,
      (actor) =>
        f.replicas.retireArtifactHistory(
          meta('cookie-retire', NOW + 90000, actor.id),
          ['artifact-cookie-0'],
          actor,
          f.controls,
        ),
      NOW + 2,
    ),
    null,
  );
  assert.equal(databaseState(f.db), before);
  const prepared = await f.members.prepareMemberCredentials('m1', {
    username: 'owner.history',
    password: 'synthetic-password-history',
  });
  f.db.transaction(() => f.members.setMemberCredentials(prepared, null))();
  before = databaseState(f.db);
  f.db.transaction(() =>
    assert.throws(
      () =>
        f.generations.retireJobHistory(
          meta('key-retire', NOW, 'm1'),
          ['key-0'],
          snapshot,
          f.controls,
        ),
      { statusCode: 403 },
    ),
  )();
  assert.equal(databaseState(f.db), before);
  f.env.OWNER_USER_ID = 'm2';
  await assert.rejects(
    f.asKey(
      'key-retire',
      (metadata, actor) => f.generations.retireJobHistory(metadata, ['key-0'], actor, f.controls),
      'm1',
      'new-owner-key',
    ),
    { statusCode: 403 },
  );
  assert.equal(databaseState(f.db), before);
  f.env.OWNER_USER_ID = 'm1';
  assert.equal(
    (
      await f.asKey(
        'key-retire',
        (metadata, actor) => f.generations.retireJobHistory(metadata, ['key-0'], actor, f.controls),
        'm1',
        'new-owner-key',
      )
    ).replayed,
    true,
  );
  const admin = {
    ...meta('local-admin-retire', NOW + 90001),
    actorKind: 'admin',
    actorId: 'local-admin',
  };
  f.db.transaction(() => f.generations.retireJobHistory(admin, ['cookie-0'], null, f.controls))();
});

test('后段 SQL 故障已见前段退休与原事实后，外层 catch 仍全表回滚，原命令重试/重领安全', async (t) => {
  for (const kind of ['jobs', 'artifacts']) {
    await t.test(kind, async (t) => {
      const f = await setup(t);
      await complete(f, 'alpha');
      await complete(f, 'beta', NOW + 20000, { memberId: 'm2' });
      const before = databaseState(f.db);
      let transient = 0;
      f.db.function('saw_retirement', (count) => {
        transient = count;
      });
      const table = kind === 'jobs' ? 'generation_jobs' : 'artifacts',
        second = kind === 'jobs' ? 'beta-0' : 'artifact-beta-0';
      f.db.exec(`CREATE TEMP TRIGGER retirement_fault BEFORE UPDATE OF retired_at ON ${table}
        WHEN NEW.id='${second}' BEGIN SELECT saw_retirement((SELECT COUNT(*) FROM ${table} WHERE retired_at IS NOT NULL));
        SELECT RAISE(ABORT,'retirement projection fault'); END`);
      const metadata = {
          ...meta(`retry-${kind}`, NOW + 90000),
          actorKind: 'admin',
          actorId: 'local-admin',
        },
        run = () =>
          kind === 'jobs'
            ? f.generations.retireJobHistory(metadata, ['beta-0', 'alpha-0'], null, f.controls)
            : f.replicas.retireArtifactHistory(
                metadata,
                ['artifact-beta-0', 'artifact-alpha-0'],
                null,
                f.controls,
              );
      f.db.transaction(() => assert.throws(run, /retirement projection fault/))();
      assert.equal(transient, 1);
      assert.equal(databaseState(f.db), before);
      f.db.exec('DROP TRIGGER retirement_fault');
      const result = f.db.transaction(run)();
      assert.equal(result.events.length, 2);
      const once = f.db.serialize();
      assert.equal(f.db.transaction(run)().replayed, true);
      assert.ok(f.db.serialize().equals(once));
      assert.deepEqual(f.db.pragma('foreign_key_check'), []);
    });
  }
});

test('完整退休提交按原选择/分组/原序回放；同改事实和回执的漏集合仍被原指纹识别', async (t) => {
  for (const kind of ['jobs', 'artifacts']) {
    await t.test(kind, async (t) => {
      const f = await setup(t);
      await complete(f, 'alpha');
      await complete(f, 'beta', NOW + 20000, { memberId: 'm2' });
      const checkpoint = f.db.serialize(),
        result =
          kind === 'jobs'
            ? await f.retireJobs('original-retirement', ['alpha-0', 'beta-0'])
            : await f.retireArtifacts('original-retirement', [
                'artifact-alpha-0',
                'artifact-beta-0',
              ]),
        stored = readRelationalCommit(f.db, result.commitSeq),
        target = new Database(checkpoint),
        generations = new RelationalGenerationStore(target);
      t.after(() => target.close());
      target.pragma('foreign_keys=ON');
      for (const change of ['member', 'scope', 'order', 'reason', 'receipt-order']) {
        const bad = structuredClone(stored),
          before = databaseState(target);
        if (change === 'scope') {
          const key = kind === 'jobs' ? 'retiredJobIds' : 'retiredArtifactIds';
          bad.command.receipt[key].pop();
          const removed = bad.events.pop();
          bad.command.events.pop();
          delete bad.command.expectedVersions[removed.streamId];
          bad.eventCount--;
        } else if (change === 'receipt-order') {
          const key = kind === 'jobs' ? 'retiredJobIds' : 'retiredArtifactIds';
          bad.command.receipt[key].reverse();
        } else if (change === 'order') {
          bad.events.reverse();
          bad.command.events.reverse();
          bad.events.forEach((event, index) => {
            event.eventIndex = index;
          });
        } else {
          for (const event of [bad.events[0], bad.command.events[0]]) {
            if (change === 'member') {
              event.memberId = 'm2';
            } else {
              event.payload.reason = 'another-reason';
            }
          }
        }
        target.transaction(() =>
          assert.throws(
            () => generations.replayGenerationCommit(bad),
            /退休|指纹|集合|原序|归属|complete/i,
          ),
        )();
        assert.equal(databaseState(target), before);
      }
      target.transaction(() => generations.replayGenerationCommit(stored))();
      assert.equal(databaseState(target), databaseState(f.db));
      const once = target.serialize();
      target.transaction(() =>
        assert.equal(generations.replayGenerationCommit(stored).replayed, true),
      )();
      assert.ok(target.serialize().equals(once));
      assert.deepEqual(readRelationalCommit(target, result.commitSeq), stored);
    });
  }
});

test('空业务库恢复两类退休不复活认证、结果、收费或免费窗口，所有原事实和输入仍可核对', async (t) => {
  const f = await setup(t, { freeLimit: 1 });
  await f.accept('still-free-limit', { steps: 23 });
  await complete(f, 'free', NOW, { steps: 23 });
  const saved = evidence(f.db);
  await f.retireArtifacts('free-image-retire', ['artifact-free-0']);
  await f.retireJobs('free-history-retire', ['free-0']);
  assert.deepEqual(evidence(f.db), saved);
  const checkpoint = f.db.serialize();
  await assert.rejects(f.accept('new-free-limit', { steps: 23, now: NOW + 100000 }), {
    statusCode: 429,
    code: 'free_image_limit',
  });
  assert.ok(f.db.serialize().equals(checkpoint));
  assert.throws(() => f.dispatch('still-free-limit-0', NOW + 100001), /免费|限额/);
  assert.ok(f.db.serialize().equals(checkpoint));
  const target = new Database(':memory:');
  try {
    initializeRelationalStorage(target, 'generation-history');
    const replayer = new RelationalCommitReplayer(target);
    const network = t.mock.method(globalThis, 'fetch', () => {
      throw new Error('replay cannot access external IO');
    });
    for (const { seq } of f.db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all()) {
      target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), f.inputs))();
    }
    for (const table of [
      'generation_requests',
      'generation_jobs',
      'generation_attempts',
      'artifacts',
      'artifact_replicas',
      'charges',
      'wallets',
      'idempotency_keys',
      'generation_inputs',
      'input_assets',
      'commits',
      'domain_events',
      'stream_heads',
    ]) {
      assert.deepEqual(rows(target, table), rows(f.db, table), table);
    }
    assert.equal(
      target.prepare('SELECT auth_epoch FROM members WHERE id=?').get('m1').auth_epoch,
      null,
    );
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
    assert.equal(target.prepare('SELECT status FROM projection_state').get().status, 'building');
    assert.equal(network.mock.callCount(), 0);
  } finally {
    target.close();
  }
});
