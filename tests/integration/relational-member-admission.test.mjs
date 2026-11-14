import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import {
  fixture as buildingFixture,
  installReady,
  meta,
  keys,
  NOW,
  databaseState,
  image,
} from '../helpers/relational-generation.mjs';
import { createRelationalMemberGenerationAdmission } from '../../server/generation/relational-member-admission.ts';
import {
  normalizeMemberGeneration,
  materializeMemberGeneration,
  memberGenerationIntentFingerprint,
} from '../../server/generation/member-contract.ts';
import { prepareRelationalInput } from '../../server/generation/relational-input.ts';
import { generationIdempotencyKeyHash } from '../../server/generation/admission.ts';
import {
  generationRequestFingerprint,
  requestNormalizationDefaults,
} from '../../server/generation/request-fingerprint.ts';
import { normalizeNovelAiRequest } from '../../server/protocols/novelai-client.ts';
import { createGenerationPayloadBudget } from '../../server/generation/payload-budget.ts';
import {
  NATIVE_DEFAULTS,
  nativeNormalizationContext,
} from '../../server/protocols/native-image.ts';
import {
  createMemberSession,
  authenticateMemberApiKey,
  withAuthenticatedMemberApiKey,
} from '../../server/members/authentication.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { canonicalJson } from '../../server/events/domain-events.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';

const header = (key = 'synthetic-client-key') => ({
  headers: { 'idempotency-key': key },
  rawHeaders: ['Idempotency-Key', key],
});
async function fixture(t, options) {
  const f = await buildingFixture(t, options);
  await installReady(f);
  return f;
}
function body(f, parameters = {}) {
  const input = {
    operation: 'generate',
    model: 'nai-diffusion-4-5-full',
    input: 'private accepted prompt',
    parameters: { width: 128, height: 128, steps: 23, seed: -1, n_samples: 2, ...parameters },
  };
  return confirmed(f, input);
}
function confirmed(f, input) {
  return {
    ...input,
    confirmation: {
      requestFingerprint: normalizeMemberGeneration(input).requestFingerprint,
      versions: f.generations.readMemberGenerationCapabilities('m1').versions,
      maxPerJob: { v5: '100', anlasFixed: '4000', anlasPurchased: '4000' },
      maxTotal: { v5: '100', anlasFixed: '4000', anlasPurchased: '4000' },
    },
  };
}
function bridge(f, overrides = {}) {
  return createRelationalMemberGenerationAdmission({
    ...f,
    sqlite: f.db,
    runtime: f.commandOnlyRuntime,
    payloadBudget: createGenerationPayloadBudget(() => f.inputs.activePayloadBytes()),
    env: {},
    now: () => NOW,
    ...overrides,
  });
}
function submit(admit, input, key, credential = { apiKey: keys.m1 }) {
  return admit({ credential, request: header(key), body: input });
}

test('在线成员受理桥拒绝building，只有唯一installer合法ready后才受理', async (t) => {
  const f = await buildingFixture(t),
    admit = bridge(f),
    input = body(f),
    before = databaseState(f.db);
  await assert.rejects(submit(admit, input), /incomplete/);
  assert.equal(databaseState(f.db), before);
  await installReady(f);
  const accepted = await submit(admit, input);
  assert.equal(accepted.replayed, false);
  const saved = databaseState(f.db);
  assert.deepEqual((await submit(admit, input)).receipt, accepted.receipt);
  assert.equal(databaseState(f.db), saved);
});

test('真实Key受理保存薄键、首次随机seed和批次；同键零写入并隔离成员', async (t) => {
  const f = await fixture(t),
    admit = bridge(f),
    input = body(f);
  const first = await submit(admit, input);
  assert.equal(first.replayed, false);
  const { requestId, jobIds } = first.receipt;
  assert.equal(jobIds.length, 2);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM idempotency_keys').get().n, 1);
  assert.equal(
    f.db.prepare('SELECT seed_mode AS mode FROM generation_requests WHERE id=?').get(requestId)
      .mode,
    'random',
  );
  const actual = f.inputs.readJobInput(jobIds[0]).seed;
  assert.equal(f.inputs.readJobInput(jobIds[1]).seed, (actual + 1) % 4294967296);
  const before = databaseState(f.db);
  const equivalent = structuredClone(input);
  equivalent.confirmation.maxPerJob.v5 = '100.000';
  assert.deepEqual((await submit(admit, equivalent)).receipt, first.receipt);
  assert.equal(databaseState(f.db), before);
  const changed = structuredClone(input);
  changed.parameters.seed = actual;
  await assert.rejects(submit(admit, changed), (e) => e.code === 'idempotency_conflict');
  assert.equal(databaseState(f.db), before);
  const fixed = confirmed(f, { ...input, parameters: { ...input.parameters, seed: actual } });
  const fixedAccepted = await submit(admit, fixed, 'fixed-same-actual-seed');
  assert.equal(f.inputs.readJobInput(fixedAccepted.receipt.jobIds[0]).seed, actual);
  assert.notEqual(fixedAccepted.receipt.requestId, requestId);
  const other = await submit(admit, input, undefined, { apiKey: keys.m2 });
  assert.notEqual(other.receipt.requestId, requestId);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM idempotency_keys').get().n, 3);
  const event = readRelationalCommit(f.db, first.commitSeq).events.at(-1);
  assert.equal(event.eventType, 'RequestAliasRegistered');
  assert.equal(
    event.payload.originalCommandId,
    f.db.prepare('SELECT command_id AS id FROM commits WHERE commit_seq=?').get(first.commitSeq).id,
  );
  assert.ok(
    !JSON.stringify(readRelationalCommit(f.db, first.commitSeq)).includes('synthetic-client-key'),
  );
  assertRelationalStorageReady(f.db);
});

test('同键沿首次默认值重领取消回执，新键仍使用当前定义并重新确认', async (t) => {
  const f = await fixture(t),
    admit = bridge(f),
    input = body(f);
  const first = await submit(admit, input);
  f.db.transaction(() =>
    f.generations.cancelWaitingGeneration(
      meta('cancel-first', NOW + 1, 'm1'),
      first.receipt.requestId,
    ),
  )();
  const previous = NATIVE_DEFAULTS.scale;
  try {
    NATIVE_DEFAULTS.scale = previous + 1;
    const before = databaseState(f.db);
    assert.deepEqual((await submit(admit, input)).receipt, first.receipt);
    assert.equal(databaseState(f.db), before);
    await assert.rejects(submit(admit, input, 'fresh-key'), (e) => e.code === 'quote_changed');
    assert.equal(databaseState(f.db), before);
    const fresh = await submit(admit, body(f), 'fresh-key');
    assert.notEqual(fresh.receipt.requestId, first.receipt.requestId);
    assert.equal(
      f.inputs.readJobInput(fresh.receipt.jobIds[0]).nativeParameters.scale,
      previous + 1,
    );
  } finally {
    NATIVE_DEFAULTS.scale = previous;
  }
});

test('真实Cookie授权与Key轮换先于原回执，撤销后不能凭已知键重领', async (t) => {
  const f = await fixture(t),
    admit = bridge(f),
    input = body(f);
  const member = await authenticateMemberApiKey(f.members, keys.m1);
  const session = await createMemberSession(f.members, member, {}, NOW);
  const cookie = { sessionToken: session.token };
  const first = await submit(admit, input, undefined, cookie);
  assert.deepEqual((await submit(admit, input)).receipt, first.receipt);
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    () => f.members.replaceMemberApiKey('m1', 'rotated-key'),
    {},
  );
  await assert.rejects(submit(admit, input), (e) => e.statusCode === 401);
  assert.deepEqual(
    (await submit(admit, input, undefined, { apiKey: 'rotated-key' })).receipt,
    first.receipt,
  );
  f.db.transaction(() =>
    f.generations.cancelWaitingGeneration(
      meta('cancel-cookie', NOW + 1, 'm1'),
      first.receipt.requestId,
    ),
  )();
  f.db.transaction(() =>
    f.members.setMemberStatus(meta('disable-member', NOW + 2), 'm1', 'disabled'),
  )();
  const before = f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n;
  await assert.rejects(submit(admit, input, undefined, cookie), (e) => e.statusCode === 401);
  await assert.rejects(
    submit(admit, input, undefined, { apiKey: 'rotated-key' }),
    (e) => e.statusCode === 401,
  );
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n, before);
});

test('原确认改变、伪造费用与SQL中途失败均不留下映射/输入/任务', async (t) => {
  const f = await fixture(t),
    admit = bridge(f),
    input = body(f);
  const first = await submit(admit, input);
  const changed = structuredClone(input);
  changed.confirmation.maxTotal.v5 = '99';
  let before = databaseState(f.db);
  await assert.rejects(submit(admit, changed), (e) => e.code === 'idempotency_conflict');
  assert.equal(databaseState(f.db), before);
  const underfunded = body(f, { steps: 30 });
  underfunded.confirmation.maxPerJob = { v5: '0', anlasFixed: '0', anlasPurchased: '0' };
  underfunded.confirmation.maxTotal = underfunded.confirmation.maxPerJob;
  await assert.rejects(
    submit(admit, underfunded, 'underfunded'),
    (e) => e.code === 'cost_limit_exceeded',
  );
  assert.equal(databaseState(f.db), before);
  f.db.exec(
    "CREATE TEMP TRIGGER fail_alias BEFORE INSERT ON idempotency_keys BEGIN SELECT RAISE(ABORT,'synthetic alias failure'); END",
  );
  await assert.rejects(submit(admit, input, 'sql-failure'), /synthetic alias failure/);
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER fail_alias');
  const accepted = await submit(admit, input, 'sql-failure');
  assert.notEqual(accepted.receipt.requestId, first.receipt.requestId);
});

test('捕获异常的外层仍保留受理保存点原子性', async (t) => {
  const f = await fixture(t),
    input = body(f);
  const admit = bridge(f, {
    generations: {
      readAcceptedGenerationReceipt: f.generations.readAcceptedGenerationReceipt.bind(
        f.generations,
      ),
      acceptGeneration(metadata, accepted) {
        try {
          f.generations.acceptGeneration(metadata, accepted);
        } catch (e) {
          assert.equal(e.code, 'cost_limit_exceeded');
        }
        return { receipt: { caught: true }, replayed: false, events: [], commitSeq: '0' };
      },
    },
  });
  input.confirmation.maxPerJob = { v5: '0', anlasFixed: '0', anlasPurchased: '0' };
  input.confirmation.maxTotal = input.confirmation.maxPerJob;
  input.parameters.steps = 30;
  input.confirmation.requestFingerprint = normalizeMemberGeneration(input).requestFingerprint;
  const before = databaseState(f.db);
  await submit(admit, input);
  assert.equal(databaseState(f.db), before);
});

test('完整受理提交联合受保护输入回放，薄键首child不能替换', async (t) => {
  const f = await fixture(t),
    admit = bridge(f),
    first = await submit(admit, body(f));
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replay = new RelationalCommitReplayer(target);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replay.replayCommit(readRelationalCommit(f.db, seq), f.inputs))();
  }
  assert.deepEqual(
    target.prepare('SELECT * FROM idempotency_keys').all(),
    f.db.prepare('SELECT * FROM idempotency_keys').all(),
  );
  const restored = new RelationalInputStore(target);
  assert.deepEqual(
    restored.readJobInput(first.receipt.jobIds[0]),
    f.inputs.readJobInput(first.receipt.jobIds[0]),
  );
  const before = databaseState(target),
    stored = readRelationalCommit(f.db, first.commitSeq);
  target.transaction(() => replay.replayCommit(stored, f.inputs))();
  assert.equal(databaseState(target), before);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
  assert.throws(() => assertRelationalStorageReady(target), /incomplete/);

  // 同一请求的第二 child 也满足 SQL FK，仍不能替换原回执的首 child。
  const forgedSource = new Database(f.db.serialize()),
    rejectedTarget = new Database(':memory:');
  t.after(() => {
    forgedSource.close();
    rejectedTarget.close();
  });
  const appendOnly = forgedSource
    .prepare("SELECT sql FROM sqlite_schema WHERE name='domain_events_append_only_update'")
    .get().sql;
  forgedSource.exec('DROP TRIGGER domain_events_append_only_update');
  forgedSource
    .prepare(
      "UPDATE domain_events SET payload=json_set(payload,'$.jobId',?) WHERE event_type='RequestAliasRegistered'",
    )
    .run(first.receipt.jobIds[1]);
  forgedSource.exec(appendOnly);
  initializeRelationalStorage(rejectedTarget, 'generation-history');
  const rejectedReplay = new RelationalCommitReplayer(rejectedTarget);
  for (const { seq } of forgedSource
    .prepare(
      'SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE commit_seq<? ORDER BY commit_seq',
    )
    .all(BigInt(first.commitSeq))) {
    rejectedTarget.transaction(() =>
      rejectedReplay.replayCommit(
        readRelationalCommit(forgedSource, seq),
        new RelationalInputStore(forgedSource),
      ),
    )();
  }
  const empty = databaseState(rejectedTarget);
  assert.throws(
    () =>
      rejectedTarget.transaction(() =>
        rejectedReplay.replayCommit(
          readRelationalCommit(forgedSource, first.commitSeq),
          new RelationalInputStore(forgedSource),
        ),
      )(),
    (e) => e.code === 'input_unavailable',
  );
  assert.equal(databaseState(rejectedTarget), empty);
});

test('当前接单epoch拦截新键，同键仍重领原回执', async (t) => {
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { env }),
    admit = bridge(f),
    input = body(f);
  const first = await submit(admit, input);
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    (member) =>
      new RelationalMemberStore(f.db, env).changeMemberAdmission(
        meta('stop-new-admission', NOW + 1, 'm1'),
        { memberId: 'm1', acceptingJobs: false },
        member,
      ),
    env,
  );
  assert.equal(
    f.db
      .prepare("SELECT COUNT(*) AS n FROM generation_jobs WHERE status IN ('queued','pending')")
      .get().n,
    0,
  );
  const before = databaseState(f.db);
  assert.deepEqual((await submit(admit, input)).receipt, first.receipt);
  await assert.rejects(submit(admit, input, 'new-after-stop'), (e) => e.statusCode === 403);
  assert.equal(databaseState(f.db), before);
});

test('HTTP键格式和未知业务字段继续由原owner严格拒绝', async (t) => {
  const f = await fixture(t),
    admit = bridge(f),
    input = body(f),
    before = databaseState(f.db);
  for (const request of [
    { headers: {}, rawHeaders: [] },
    header(''),
    header('contains space'),
    header('x'.repeat(129)),
    {
      headers: { 'idempotency-key': 'duplicate' },
      rawHeaders: ['Idempotency-Key', 'duplicate', 'idempotency-key', 'duplicate'],
    },
  ]) {
    await assert.rejects(
      admit({ credential: { apiKey: keys.m1 }, request, body: input }),
      (e) => e.code === 'invalid_idempotency_key',
    );
    assert.equal(databaseState(f.db), before);
  }
  for (const field of ['memberId', 'accountId', 'pst', 'clientContract']) {
    await assert.rejects(
      submit(admit, { ...input, [field]: 'forged' }),
      (e) => e.code === 'invalid_parameters',
    );
    assert.equal(databaseState(f.db), before);
  }
});

test('两种辅助操作沿原解析保存私有附件，同一键按操作独立', async (t) => {
  const f = await fixture(t),
    admit = bridge(f);
  for (const operation of ['upscale', 'encode-vibe']) {
    const input = confirmed(f, {
      operation,
      image: image.toString('base64'),
      ...(operation === 'encode-vibe'
        ? { model: 'nai-diffusion-4-5-full', information_extracted: 0.5, focus_seed: -7 }
        : {}),
    });
    const first = await submit(admit, input, 'same-aux-key');
    const before = f.db.serialize();
    assert.deepEqual((await submit(admit, input, 'same-aux-key')).receipt, first.receipt);
    assert.ok(f.db.serialize().equals(before));
    assert.equal(
      f.db
        .prepare('SELECT seed_mode AS mode FROM generation_requests WHERE id=?')
        .get(first.receipt.requestId).mode,
      'not_applicable',
    );
    assert.equal(f.inputs.readJobInput(first.receipt.jobIds[0]).operation, operation);
    assert.equal(f.inputs.readMemberInput(first.receipt.requestId, 'm1').assets.length, 1);
  }
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM idempotency_keys').get().n, 2);
});

test('临时正文占用在同步受理后移交SQL，两笔并发调用不重复计数', async (t) => {
  const f = await fixture(t),
    input = body(f),
    normalized = normalizeMemberGeneration(input);
  const maximum = prepareRelationalInput(
    {
      ...materializeMemberGeneration(normalized, 4294967295),
      clientContract: 'member-generation-v1',
      confirmation: input.confirmation,
    },
    { kind: 'member', member: normalized.normalizationContext },
    {},
  ).logicalBytes;
  const payloadBudget = createGenerationPayloadBudget(() => f.inputs.activePayloadBytes(), {
    request: maximum,
    active: maximum * 2,
  });
  const admit = bridge(f, { payloadBudget });
  const first = submit(admit, input, 'budget-one'),
    second = submit(admit, input, 'budget-two');
  const accepted = await Promise.all([first, second]);
  assert.notEqual(accepted[0].receipt.requestId, accepted[1].receipt.requestId);
  assert.ok(f.inputs.activePayloadBytes() <= maximum * 2);
});

test('原协议薄键不能被成员协议另建请求；不可用薄键不会重新生成', async (t) => {
  const f = await fixture(t),
    admit = bridge(f),
    input = body(f, { n_samples: 1 });
  const normalized = normalizeMemberGeneration(input),
    key = 'original-native-key';
  const originalRequest = materializeMemberGeneration(normalized);
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    () =>
      f.generations.acceptGeneration(
        {
          ...meta('native-original', NOW, 'm1'),
          fingerprint: generationRequestFingerprint(originalRequest, {
            randomSeed: true,
            noCache: true,
          }),
        },
        {
          inputId: 'native-input',
          requestId: 'native-request',
          memberId: 'm1',
          source: 'native',
          seedMode: 'random',
          confirmation: null,
          request: originalRequest,
          context: { kind: 'native', native: normalized.normalizationContext.native },
          idempotency: { keyHash: generationIdempotencyKeyHash(key), noCache: true },
          jobs: [
            {
              jobId: 'native-job',
              status: 'pending',
              queueSeq: null,
              queueExpiresAt: null,
              deadlineAt: null,
              admissionEpoch: 0,
              cacheKey: null,
            },
          ],
        },
      ),
    {},
  );
  let before = databaseState(f.db);
  await assert.rejects(submit(admit, input, key), (e) => e.code === 'idempotency_conflict');
  assert.equal(databaseState(f.db), before);
  const first = await submit(admit, input, 'no-longer-available');
  f.db
    .prepare("UPDATE idempotency_keys SET resolution='unavailable' WHERE original_request_id=?")
    .run(first.receipt.requestId);
  before = databaseState(f.db);
  await assert.rejects(submit(admit, input, 'no-longer-available'), (e) => e.statusCode === 410);
  assert.equal(databaseState(f.db), before);
});

test('原规范输入约束指纹及确认，互相一致的伪B摘要不能覆盖A', async (t) => {
  for (const changeConfirmation of [false, true]) {
    await t.test(changeConfirmation ? '同时伪造确认指纹' : '只伪造命令和薄键指纹', async (t) => {
      const f = await fixture(t),
        original = body(f, { seed: 42, n_samples: 1 });
      const other = confirmed(f, { ...original, input: 'different private B prompt' });
      const first = await submit(bridge(f), original),
        forged = new Database(f.db.serialize());
      const target = new Database(':memory:');
      t.after(() => {
        forged.close();
        target.close();
      });
      const fingerprint = memberGenerationIntentFingerprint(
        normalizeMemberGeneration(other),
        other.confirmation,
      );
      const names = [
        'commits_append_only_update',
        'domain_events_append_only_update',
        'idempotency_keys_identity',
        'generation_requests_identity',
      ];
      const guards = names.map((name) => ({
        name,
        sql: forged.prepare('SELECT sql FROM sqlite_schema WHERE name=?').get(name).sql,
      }));
      forged.transaction(() => {
        for (const guard of guards) {
          forged.exec(`DROP TRIGGER ${guard.name}`);
        }
        forged
          .prepare('UPDATE commits SET fingerprint=? WHERE commit_seq=?')
          .run(fingerprint, BigInt(first.commitSeq));
        forged
          .prepare(
            "UPDATE domain_events SET payload=json_set(payload,'$.intentFingerprint',?) WHERE event_type='RequestAliasRegistered'",
          )
          .run(fingerprint);
        forged.prepare('UPDATE idempotency_keys SET intent_fingerprint=?').run(fingerprint);
        if (changeConfirmation) {
          forged
            .prepare(
              "UPDATE domain_events SET payload=json_set(payload,'$.confirmation',json(?)) WHERE event_type='RequestAccepted'",
            )
            .run(canonicalJson(other.confirmation));
          // 投影也保存同一规范确认，排除“只是副本不等”的较早拒绝。
          forged
            .prepare('UPDATE generation_requests SET confirmation=?')
            .run(canonicalJson(other.confirmation));
        }
        for (const guard of guards) {
          forged.exec(guard.sql);
        }
      })();
      assert.deepEqual(forged.pragma('foreign_key_check'), []);
      const source = new RelationalInputStore(forged);
      assert.equal(
        source.readMemberInput(first.receipt.requestId, 'm1').input.input,
        original.input,
      );
      const beforeSource = databaseState(forged);
      await assert.rejects(
        submit(
          bridge({
            db: forged,
            members: new RelationalMemberStore(forged),
            inputs: source,
            generations: new RelationalGenerationStore(forged, {}),
          }),
          other,
        ),
        (e) => e.code === 'input_unavailable',
      );
      assert.equal(databaseState(forged), beforeSource);
      initializeRelationalStorage(target, 'generation-history');
      const replay = new RelationalCommitReplayer(target);
      for (const { seq } of forged
        .prepare(
          'SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE commit_seq<? ORDER BY commit_seq',
        )
        .all(BigInt(first.commitSeq))) {
        target.transaction(() => replay.replayCommit(readRelationalCommit(forged, seq), source))();
      }
      const beforeTarget = databaseState(target);
      target.transaction(() => {
        assert.throws(
          () => replay.replayCommit(readRelationalCommit(forged, first.commitSeq), source),
          (e) => e.code === 'input_unavailable',
        );
      })();
      assert.equal(databaseState(target), beforeTarget);
      target.transaction(() =>
        replay.replayCommit(readRelationalCommit(f.db, first.commitSeq), f.inputs),
      )();
      assert.deepEqual(
        target.prepare('SELECT * FROM idempotency_keys').all(),
        f.db.prepare('SELECT * FROM idempotency_keys').all(),
      );
    });
  }
});

test('Direct原clamp与noCache真/假意图均从保存事实回放，不收窄为Native输入', async (t) => {
  const f = await fixture(t),
    settings = { defaults: { steps: 23, scale: 5, cfg: 0 } };
  const request = normalizeNovelAiRequest(
    { tag: 'original direct subject', width: 128, height: 128, steps: 30, scale: 11, seed: 7 },
    settings,
    { maxSteps: 28 },
  );
  const context = {
    kind: 'direct',
    native: nativeNormalizationContext(),
    defaults: requestNormalizationDefaults(settings),
  };
  for (const noCache of [false, true]) {
    const id = `direct-${noCache}`,
      metadata = {
        ...meta(id, NOW, 'm1'),
        fingerprint: generationRequestFingerprint(request, { randomSeed: false, noCache }),
      };
    const accepted = await withAuthenticatedMemberApiKey(
      f.members,
      keys.m1,
      () =>
        f.generations.acceptGeneration(metadata, {
          memberId: 'm1',
          requestId: id,
          inputId: `input-${id}`,
          source: 'direct',
          seedMode: 'fixed',
          confirmation: null,
          request,
          context,
          idempotency: { keyHash: generationIdempotencyKeyHash(id), noCache },
          jobs: [
            {
              jobId: `job-${id}`,
              status: 'pending',
              queueSeq: null,
              queueExpiresAt: null,
              deadlineAt: null,
              admissionEpoch: 0,
              cacheKey: null,
            },
          ],
        }),
      {},
    );
    const event = readRelationalCommit(f.db, accepted.commitSeq).events.at(-1);
    assert.equal(event.payload.noCache, noCache);
    assert.equal(f.inputs.readJobInput(`job-${id}`).requestedSteps, 30);
    assert.equal(f.inputs.readJobInput(`job-${id}`).steps, 28);
    f.inputs.assertAcceptedGenerationIntent(
      id,
      'm1',
      accepted.commitSeq,
      event.payload.keyHash,
      event.eventId,
    );
  }
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const replay = new RelationalCommitReplayer(db);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    db.transaction(() => replay.replayCommit(readRelationalCommit(f.db, seq), f.inputs))();
  }
  assert.deepEqual(
    db.prepare('SELECT * FROM idempotency_keys').all(),
    f.db.prepare('SELECT * FROM idempotency_keys').all(),
  );
});

test('薄键必须反向对应原alias事实，缺失映射不能借command回执绕过', async (t) => {
  const f = await fixture(t),
    admit = bridge(f),
    input = body(f, { n_samples: 1 });
  const first = await submit(admit, input);
  const guard = f.db
    .prepare("SELECT sql FROM sqlite_schema WHERE name='idempotency_keys_identity'")
    .get().sql;
  f.db.exec('DROP TRIGGER idempotency_keys_identity');
  f.db
    .prepare('UPDATE idempotency_keys SET key_hash=?')
    .run(generationIdempotencyKeyHash('different-client-key'));
  f.db.exec(guard);
  const before = databaseState(f.db);
  await assert.rejects(
    submit(admit, input, 'different-client-key'),
    (e) => e.code === 'input_unavailable',
  );
  assert.equal(databaseState(f.db), before);
  await assert.rejects(submit(admit, input), (e) => e.code === 'result_unavailable');
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_requests').get().n, 1);
  assert.equal(f.inputs.readMemberInput(first.receipt.requestId, 'm1').input.input, input.input);
});
