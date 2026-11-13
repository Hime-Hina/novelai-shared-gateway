import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { fixture, meta, NOW, keys, databaseState } from '../helpers/relational-generation.mjs';
import { RelationalRequestStore } from '../../server/storage/relational-request-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { publicGenerationRequest } from '../../server/generation/request-queries.ts';
import {
  normalizeMemberGeneration,
  materializeMemberGeneration,
} from '../../server/generation/member-contract.ts';
import {
  withAuthenticatedMemberApiKey,
  withAuthenticatedMemberSession,
  createMemberSession,
} from '../../server/members/authentication.ts';
import {
  readRelationalCommit,
  replayRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';

const hasCode = (code) => (error) => error.code === code;

test('关系状态与分页复用现有 DTO，查询不读取正文/BLOB、不改变任何持久状态', async (t) => {
  const f = await fixture(t);
  await f.accept('request', { samples: 2 });
  f.dispatch('request-0');
  await f.publish('request-0');
  const store = new RelationalRequestStore(f.db),
    before = databaseState(f.db),
    sql = [],
    prepare = f.db.prepare;
  f.db.prepare = function (text) {
    sql.push(text);
    return prepare.call(this, text);
  };
  let state;
  try {
    state = store.readMemberRequest('request', 'm1');
    const page = store.readMemberRequests('m1', 1);
    assert.deepEqual(page.requests, [state]);
    assert.deepEqual(store.readMemberRequests('m1', 1, page.before).requests, []);
  } finally {
    f.db.prepare = prepare;
  }
  assert.equal(
    sql.some((text) => /generation_inputs|input_assets|\bbody\b/i.test(text)),
    false,
  );
  const dto = publicGenerationRequest(state);
  assert.equal(dto.status, 'queued');
  assert.equal(dto.jobs[0].model, 'nai-diffusion-4-5-full');
  assert.equal(dto.jobs[0].charge.estimated, true);
  assert.deepEqual(dto.localCharges, { v5: '0', anlasFixed: '2', anlasPurchased: '0' });
  assert.equal(dto.jobs[0].contentUrl, '/api/member/jobs/request-0/content');
  assert.equal(JSON.stringify(dto).includes('private generation prompt'), false);
  assert.equal(f.inputs.readMemberInput('request', 'm1').input.input, 'private generation prompt');
  assert.equal(databaseState(f.db), before);
  for (const column of ['model', 'estimated']) {
    const table = column === 'model' ? 'generation_requests' : 'charges';
    assert.throws(() => f.db.prepare(`UPDATE ${table} SET ${column}=NULL`).run(), /immutable/);
  }
});

test('当前 Key/Cookie 认证与稳定归属保持，换 Key 和停止接单不改变旧请求读取', async (t) => {
  const f = await fixture(t),
    store = new RelationalRequestStore(f.db);
  await f.accept('owned');
  f.dispatch('owned-0');
  await f.publish('owned-0');
  assert.throws(() => store.readMemberRequest('owned', 'm2'), hasCode('request_not_found'));
  assert.throws(() => store.readMemberResult('owned-0', 'm2'), hasCode('job_not_found'));
  assert.throws(() => f.inputs.readMemberInput('owned', 'm2'), hasCode('request_not_found'));
  const user = await withAuthenticatedMemberApiKey(f.members, keys.m1, (member) => member, {}),
    session = await createMemberSession(f.members, user, {}, NOW);
  const read = (key) =>
    withAuthenticatedMemberApiKey(
      f.members,
      key,
      (member) => publicGenerationRequest(store.readMemberRequest('owned', member.id)),
      {},
    );
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'new-member-key'))();
  assert.equal(await read(keys.m1), null);
  assert.equal((await read('new-member-key')).id, 'owned');
  // 接单状态的命令仍属 #47；这里验证读取不会把该状态当成历史授权撤销。
  f.db.prepare("UPDATE members SET accepting_jobs=0 WHERE id='m1'").run();
  assert.equal((await read('new-member-key')).id, 'owned');
  assert.equal(
    (
      await withAuthenticatedMemberSession(
        f.members,
        session.token,
        {},
        (member) => store.readMemberRequest('owned', member.id),
        NOW + 1,
      )
    ).id,
    'owned',
  );
  f.db.transaction(() => f.members.setMemberStatus(meta('disable'), 'm1', 'disabled'))();
  assert.equal(await read('new-member-key'), null);
  assert.throws(() => store.readMemberRequest('owned', 'm1'), hasCode('session_expired'));
  assert.throws(() => store.readMemberResult('owned-0', 'm1'), hasCode('session_expired'));
  assert.equal(
    await withAuthenticatedMemberSession(
      f.members,
      session.token,
      {},
      (member) => store.readMemberRequest('owned', member.id),
      NOW + 2,
    ),
    null,
  );
});

test('结果读取保留准备/失败/迟到/退休语义及产物原始 input/attempt，不制造新收费', async (t) => {
  const f = await fixture(t),
    store = new RelationalRequestStore(f.db);
  await f.accept('late', { samples: 2 });
  assert.throws(() => store.readMemberResult('late-0', 'm1'), hasCode('result_not_ready'));
  f.dispatch('late-0');
  f.fail('late-0');
  assert.throws(() => store.readMemberResult('late-0', 'm1'), hasCode('generation_failed'));
  await f.publish('late-0', NOW + 100);
  const before = databaseState(f.db),
    result = store.readMemberResult('late-0', 'm1'),
    dto = publicGenerationRequest(store.readMemberRequest('late', 'm1'));
  assert.equal(result.inputId, 'input-late');
  assert.equal(result.originalAttemptId, 'attempt-late-0');
  assert.equal(dto.jobs[0].lateResultAvailable, true);
  assert.equal(dto.jobs[0].status, 'failed');
  assert.equal(dto.jobs[0].charge.status, 'refunded');
  assert.equal(dto.jobs[1].cancelled, true);
  assert.deepEqual(dto.localCharges, { v5: '0', anlasFixed: '0', anlasPurchased: '0' });
  assert.equal(databaseState(f.db), before);
  await f.accept('cached');
  // #31 只验证合法同成员引用的读取边界；CachedResultLinked 命令/投影尚属后续片。
  f.db
    .prepare("UPDATE generation_jobs SET artifact_id=? WHERE id='cached-0'")
    .run(result.artifactId);
  assert.equal(store.readMemberResult('cached-0', 'm1').inputId, 'input-late');
  f.db.prepare('UPDATE artifacts SET retired_at=? WHERE id=?').run(NOW + 101, result.artifactId);
  assert.throws(() => store.readMemberResult('late-0', 'm1'), hasCode('result_unavailable'));
  assert.equal(publicGenerationRequest(store.readMemberRequest('late', 'm1')).jobs[0].imageUrl, '');
});

test('原 estimated 来自已提交规划，未知保留 NULL；输入模型摘要不能悄悄偏离原输入', async (t) => {
  const f = await fixture(t, { freeLimit: 2 });
  await f.accept('free', { steps: 23 });
  f.dispatch('free-0');
  const event = f.db
    .prepare("SELECT payload FROM domain_events WHERE event_type='QuotaReserved'")
    .get();
  assert.equal(JSON.parse(event.payload).estimated, false);
  assert.equal(f.generations.readCharge('free-0').estimated, false);
  const state = new RelationalRequestStore(f.db).readMemberRequest('free', 'm1');
  state.jobs[0].fairCharge.estimated = null;
  assert.equal(publicGenerationRequest(state).jobs[0].charge.estimated, null);
  // 模拟损坏的受保护来源；源事实摘要不变时，冷读取必须拒绝，不以摘要列兜底。
  f.db.exec('DROP TRIGGER generation_inputs_identity');
  f.db.prepare("UPDATE generation_inputs SET body=json_set(body,'$.model','other-model')").run();
  assert.throws(() => f.inputs.readMemberInput('free', 'm1'), hasCode('input_unavailable'));
});

test('受理输入、完整 child 集合和原 seed 语义一致，错误提交外层捕获后不留半笔', async (t) => {
  const changes = [
    [
      'model',
      (list) => {
        list.find((e) => e.eventType === 'RequestAccepted').payload.model = 'wrong-model';
      },
    ],
    [
      'operation',
      (list) => {
        list.find((e) => e.eventType === 'RequestAccepted').payload.operation = 'upscale';
      },
    ],
    [
      'sampleCount',
      (list) => {
        list.find((e) => e.eventType === 'RequestAccepted').payload.sampleCount = 3;
      },
    ],
    [
      'child-member',
      (list) => {
        list.filter((e) => e.eventType === 'JobAccepted')[1].memberId = 'm2';
      },
    ],
    [
      'child-seed',
      (list) => {
        list.filter((e) => e.eventType === 'JobAccepted')[1].payload.actualSeed = 47;
      },
    ],
    [
      'seedMode',
      (list) => {
        list.find((e) => e.eventType === 'RequestAccepted').payload.seedMode = 'random';
      },
    ],
  ];
  for (const [name, change] of changes) {
    await t.test(name, async (t) => {
      const f = await fixture(t),
        baseline = f.db.serialize();
      await f.accept('mismatch', { samples: 2, member: true });
      const seq = f.db
          .prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits')
          .get().seq,
        stored = structuredClone(readRelationalCommit(f.db, seq));
      for (const list of [stored.command.events, stored.events]) {
        change(list);
      }
      const source = new Database(baseline),
        target = new Database(baseline);
      t.after(() => {
        source.close();
        target.close();
      });
      source.pragma('foreign_keys=ON');
      target.pragma('foreign_keys=ON');
      source.transaction(() => replayRelationalCommit(source, stored))();
      for (const table of ['normalization_contexts', 'generation_inputs', 'input_assets']) {
        for (const row of f.db.prepare(`SELECT * FROM ${table}`).all()) {
          const columns = Object.keys(row);
          source
            .prepare(
              `INSERT OR IGNORE INTO ${table}(${columns.join(',')}) VALUES(${columns.map(() => '?').join(',')})`,
            )
            .run(...Object.values(row));
        }
      }
      const inputs = new RelationalInputStore(target),
        sourceInputs = new RelationalInputStore(source),
        before = databaseState(target);
      target.transaction(() => {
        assert.throws(
          () => inputs.replayAcceptedInput(sourceInputs, seq),
          hasCode('input_unavailable'),
        );
      })();
      assert.equal(databaseState(target), before);
      target.transaction(() => inputs.replayAcceptedInput(f.inputs, seq))();
      assert.equal(
        target.prepare("SELECT operation,model FROM generation_requests WHERE id='mismatch'").get()
          .operation,
        'generate',
      );
      const restored = target.serialize();
      target.transaction(() => inputs.replayAcceptedInput(f.inputs, seq))();
      assert.ok(target.serialize().equals(restored));
    });
  }
});

test('同毫秒历史按稳定 ID 分页，使用既有覆盖索引而非正文扫描或临时排序', async (t) => {
  const f = await fixture(t),
    store = new RelationalRequestStore(f.db);
  for (const id of ['a', 'b', 'c']) {
    await f.accept(id, { now: NOW });
  }
  const first = store.readMemberRequests('m1', 2),
    second = store.readMemberRequests('m1', 2, first.before);
  assert.deepEqual(
    first.requests.map((request) => request.id),
    ['c', 'b'],
  );
  assert.deepEqual(
    second.requests.map((request) => request.id),
    ['a'],
  );
  assert.equal(second.before, null);
  assert.deepEqual(store.readMemberRequests('m2').requests, []);
  const plan = f.db
    .prepare(
      `EXPLAIN QUERY PLAN SELECT id,source,operation,model,accepted_at
    FROM generation_requests WHERE member_id=? AND retired_at IS NULL AND (accepted_at,id)<(?,?)
    ORDER BY accepted_at DESC,id DESC LIMIT ?`,
    )
    .all('m1', NOW, 'c', 2)
    .map((row) => row.detail)
    .join('\n');
  assert.match(plan, /idx_requests_member_time/);
  assert.doesNotMatch(plan, /SCAN|TEMP B-TREE/);
  for (const limit of [0, 101, 1.1]) {
    assert.throws(() => store.readMemberRequests('m1', limit), hasCode('invalid_parameters'));
  }
  for (const before of [
    { acceptedAt: NOW, id: {} },
    { acceptedAt: -1, id: 'x' },
  ]) {
    assert.throws(() => store.readMemberRequests('m1', 2, before), hasCode('invalid_parameters'));
  }
});

test('合法随机输入使用原实际 seed 和首次上下文，完整提交回放不重抽种子', async (t) => {
  const f = await fixture(t),
    baseline = f.db.serialize();
  const normalized = normalizeMemberGeneration({
    operation: 'generate',
    model: 'nai-diffusion-4-5-full',
    input: 'random original',
    parameters: { width: 128, height: 128, steps: 30, n_samples: 2 },
  });
  assert.equal(normalized.randomSeed, true);
  // T2 的输入边界控制；confirmation 的实际业务授权仍由 T4 受理 owner 检查。
  const saved = await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    () =>
      f.inputs.persistAcceptedInput(meta('random', NOW, 'm1'), {
        inputId: 'random-input',
        requestId: 'random',
        memberId: 'm1',
        source: 'member-generation',
        seedMode: 'random',
        confirmation: null,
        request: materializeMemberGeneration(normalized),
        context: { kind: 'member', member: normalized.normalizationContext },
        jobs: [0, 1].map((index) => ({
          jobId: `random-${index}`,
          status: 'pending',
          queueSeq: null,
          queueExpiresAt: null,
          deadlineAt: null,
          admissionEpoch: 0,
          cacheKey: null,
        })),
      }),
    {},
  );
  const original = f.inputs.readMemberInput('random', 'm1'),
    target = new Database(baseline);
  target.pragma('foreign_keys=ON');
  try {
    const inputs = new RelationalInputStore(target);
    target.transaction(() => inputs.replayAcceptedInput(f.inputs, saved.commitSeq))();
    assert.deepEqual(inputs.readMemberInput('random', 'm1'), original);
    assert.equal(
      inputs.readJobInput('random-1').seed,
      (original.input.parameters.seed + 1) % 4294967296,
    );
  } finally {
    target.close();
  }
});

test('完整提交重建与关闭重开后状态 DTO 一致，原输入只冷读，业务回放不恢复认证', async (t) => {
  const f = await fixture(t);
  await f.accept('restored');
  f.dispatch('restored-0');
  await f.publish('restored-0');
  const expected = publicGenerationRequest(
      new RelationalRequestStore(f.db).readMemberRequest('restored', 'm1'),
    ),
    target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const inputs = new RelationalInputStore(target),
    replayer = new RelationalCommitReplayer(target);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), f.inputs))();
  }
  const store = new RelationalRequestStore(target);
  assert.throws(() => store.readMemberRequest('restored', 'm1'), hasCode('session_expired'));
  // 仅为本片读验证提供当前私有 epoch；这不是 T7 的快照恢复/ready 安装实现。
  target.prepare("UPDATE members SET auth_epoch='current-private-epoch' WHERE id='m1'").run();
  assert.deepEqual(publicGenerationRequest(store.readMemberRequest('restored', 'm1')), expected);
  assert.equal(inputs.readMemberInput('restored', 'm1').input.parameters.seed, 42);
  f.db.close();
  const reopened = new Database(path.join(f.dir, 'library.sqlite'));
  reopened.pragma('foreign_keys=ON');
  const before = databaseState(reopened);
  assert.deepEqual(
    publicGenerationRequest(
      new RelationalRequestStore(reopened).readMemberRequest('restored', 'm1'),
    ),
    expected,
  );
  assert.equal(
    new RelationalInputStore(reopened).readMemberInput('restored', 'm1').input.input,
    'private generation prompt',
  );
  assert.equal(databaseState(reopened), before);
  for (const table of ['generation_jobs', 'artifacts', 'stream_heads']) {
    assert.equal(
      JSON.stringify(reopened.prepare(`SELECT * FROM ${table}`).all()).includes(
        'private generation prompt',
      ),
      false,
    );
  }
  reopened.close();
});
