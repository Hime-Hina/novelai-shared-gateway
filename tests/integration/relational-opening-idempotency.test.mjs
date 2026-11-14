import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createServer } from 'node:http';
import { EventEmitter, once } from 'node:events';
import path from 'node:path';
import {
  fixture,
  installReady,
  NOW,
  meta,
  databaseState,
  image,
} from '../helpers/relational-generation.mjs';
import { domainDigest, canonicalJson } from '../../server/events/domain-events.ts';
import {
  readRelationalCommit,
  appendRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { generationIdempotencyKeyHash } from '../../server/generation/admission.ts';
import {
  restoreRelationalWire,
  relationalProviderRequest,
} from '../../server/generation/relational-input.ts';
import { memberGenerationIntentFingerprint } from '../../server/generation/member-contract.ts';
import { createGenerationPayloadBudget } from '../../server/generation/payload-budget.ts';
import { createRelationalNativeHttp } from '../../server/generation/relational-native-http.ts';
import { createRelationalMemberGenerationHttp } from '../../server/generation/relational-member-http.ts';
import { nativeGenerationError } from '../../server/protocols/native-http.ts';
import {
  authenticateMemberApiKey,
  createMemberSession,
  withAuthenticatedMemberApiKey,
} from '../../server/members/authentication.ts';
import { JobPreviews } from '../../server/generation/job-previews.ts';
import { initializeFixtureConfiguration } from '../helpers/relational-configuration.mjs';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { relationalSnapshotSha256 } from '../../server/storage/relational-private-restore.ts';

const origin = (originalId) => ({ sourceId: 'synthetic-opening', originalId });
function append(f, events, id = 'import-key') {
  const { policyVersion, pricingVersion, ...command } = meta(id, NOW + 100);
  return f.db.transaction(() =>
    new RelationalCommitReplayer(f.db).appendCommit(
      {
        ...command,
        fingerprint: domainDigest(events),
        receipt: { imported: true },
        expectedVersions: Object.fromEntries(
          events.map((e) => [
            e.streamId,
            f.db
              .prepare(
                'SELECT CAST(stream_version AS TEXT) AS v FROM stream_heads WHERE stream_id=?',
              )
              .get(e.streamId)?.v ?? '0',
          ]),
        ),
        events: events.map((e) => ({
          occurredAt: NOW + 100,
          policyVersion,
          pricingVersion,
          accountId: null,
          ...e,
        })),
      },
      f.inputs,
    ),
  )();
}
function definition(f, id, rawKey = id, patch = {}) {
  const r = f.db.prepare('SELECT * FROM generation_requests WHERE id=?').get(id),
    context = f.inputs.readMemberContext(id, r.member_id),
    body = JSON.parse(
      f.db.prepare('SELECT body FROM generation_inputs WHERE id=?').get(r.input_id).body,
    ),
    jobs = f.db
      .prepare(
        'SELECT id,actual_seed FROM generation_jobs WHERE request_id=? ORDER BY sample_index',
      )
      .all(id),
    wire = restoreRelationalWire(body, [], jobs[0].actual_seed, r.sample_count, context),
    request = {
      ...relationalProviderRequest(wire, context),
      operation: r.operation,
      sampleCount: r.sample_count,
    },
    fingerprint = f.inputs.generationInputIntent(
      r.member_id,
      request,
      context,
      'fixed',
      true,
    ).fingerprint,
    confirmation = r.confirmation === null ? null : JSON.parse(r.confirmation),
    payload = {
      originalMemberId: r.member_id,
      operation: r.operation,
      keyHash: generationIdempotencyKeyHash(rawKey),
      intentFingerprint:
        context.kind === 'member'
          ? memberGenerationIntentFingerprint({ requestFingerprint: fingerprint }, confirmation)
          : fingerprint,
      originalRequestId: id,
      originalJobId: jobs[0].id,
      originalCommandId: `original-command-${rawKey}`,
      noCache: true,
      seedMode: 'fixed',
      requestId: id,
      commitSeq: null,
      contextId: r.context_id,
      resolution: 'live',
      originRef: `archive:original-key:${rawKey}`,
      archivedReceipt: { requestId: id, jobIds: jobs.map((j) => j.id) },
      createdAt: 0,
      origin: origin(`original-key-${rawKey}`),
      ...patch,
    };
  return {
    payload,
    context,
    request,
    body,
    wire,
    confirmation,
    event: {
      eventType: 'OpeningIdempotencyRecorded',
      streamId: `idempotency:${payload.originalMemberId}:${payload.operation}:${payload.keyHash}`,
      memberId: payload.originalMemberId,
      payload,
    },
  };
}
function rebuild(t, source) {
  const target = new Database(':memory:');
  initializeRelationalStorage(target, 'generation-history');
  t.after(() => target.close());
  const replay = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(source);
  for (const { seq } of source
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replay.replayCommit(readRelationalCommit(source, seq), inputs))();
  }
  return target;
}
function opened(f, id, count = 2) {
  const original = f.db.prepare('SELECT * FROM generation_requests WHERE id=?').get(id),
    events = [
      {
        eventType: 'OpeningRequestRecorded',
        streamId: `request:opened-${id}`,
        memberId: 'm1',
        payload: {
          requestId: `opened-${id}`,
          memberId: 'm1',
          inputId: original.input_id,
          contextId: original.context_id,
          source: 'original-history',
          operation: original.operation,
          model: original.model,
          sampleCount: original.sample_count,
          seedMode: original.seed_mode,
          confirmation: original.confirmation === null ? null : JSON.parse(original.confirmation),
          acceptedAt: 0,
          retiredAt: null,
          origin: origin('original-request'),
        },
      },
    ];
  for (let i = 0; i < count; i++)
    events.push({
      eventType: 'OpeningJobRecorded',
      streamId: `request:opened-${id}`,
      memberId: 'm1',
      payload: {
        jobId: `opened-${id}-${i}`,
        requestId: `opened-${id}`,
        createdAt: 0,
        sampleIndex: i,
        actualSeed: 42 + i,
        status: 'pending',
        queueSeq: null,
        queueExpiresAt: null,
        deadlineAt: null,
        admissionEpoch: 0,
        artifactId: null,
        cacheKey: null,
        completedAt: null,
        errorCode: null,
        errorStatus: null,
        cancellationReason: null,
        lateResultAt: null,
        freeSucceededAt: null,
        freeWarningThreshold: null,
        retiredAt: null,
        origin: origin(`original-job-${i}`),
      },
    });
  return append(f, events, 'import-entities');
}

test('Opening live保存真正原回执而非import回执，完整回放不恢复秘密/网络且重复零写', async (t) => {
  const f = await fixture(t);
  f.accept('original', { samples: 2 });
  opened(f, 'original');
  const { event, payload } = definition(f, 'opened-original');
  const saved = append(f, [event]);
  assert.notDeepEqual(saved.receipt, payload.archivedReceipt);
  const network = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = () => {
    calls++;
    throw new Error('No replay network');
  };
  try {
    const target = rebuild(t, f.db),
      inputs = new RelationalInputStore(target),
      before = databaseState(target),
      changes = target.prepare('SELECT total_changes() AS n').get().n;
    assert.equal(
      inputs.readGenerationIdempotency('m1', 'generate', payload.keyHash).commitSeq,
      null,
    );
    assert.deepEqual(
      inputs.readOpeningGenerationIdempotency('m1', 'generate', payload.keyHash).receipt,
      payload.archivedReceipt,
    );
    target.transaction(() =>
      new RelationalCommitReplayer(target).replayCommit(
        readRelationalCommit(f.db, saved.commitSeq),
        f.inputs,
      ),
    )();
    assert.equal(databaseState(target), before);
    assert.equal(target.prepare('SELECT total_changes() AS n').get().n, changes);
    for (const table of ['api_keys', 'upstream_credentials'])
      assert.equal(target.prepare(`SELECT count(*) AS n FROM ${table}`).get().n, 0);
    assert.equal(calls, 0);
  } finally {
    globalThis.fetch = network;
  }
});

test('Opening未知tombstone没有原receipt也保留旧键且拒live，不猜当前值', async (t) => {
  const f = await fixture(t);
  for (const resolution of ['archived', 'unavailable']) {
    const p = {
      originalMemberId: 'removed-original-member',
      operation: 'generate',
      keyHash: generationIdempotencyKeyHash(resolution),
      intentFingerprint: 'd'.repeat(64),
      originalRequestId: 'missing-request',
      originalJobId: 'missing-job',
      originalCommandId: 'missing-command',
      noCache: null,
      seedMode: null,
      requestId: null,
      commitSeq: null,
      contextId: null,
      resolution,
      originRef: 'archive:tombstone',
      archivedReceipt: null,
      createdAt: 0,
      origin: origin('original-tombstone'),
    };
    append(
      f,
      [
        {
          eventType: 'OpeningIdempotencyRecorded',
          streamId: `idempotency:${p.originalMemberId}:generate:${p.keyHash}`,
          memberId: p.originalMemberId,
          payload: p,
        },
      ],
      resolution,
    );
    const row = f.inputs.readGenerationIdempotency(p.originalMemberId, p.operation, p.keyHash);
    assert.equal(row.archivedReceipt, null);
    assert.throws(
      () => f.inputs.readGenerationIdempotencyContext(p.originalMemberId, p.operation, p.keyHash),
      (e) => e.code === 'input_unavailable',
    );
  }
  const target = rebuild(t, f.db);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM idempotency_keys').get().n, 2);
});

test('原Opening key完整提交在后续合法退休后仍可历史重复零写，当前POST资格仍拒绝', async (t) => {
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { env });
  f.accept('original');
  const key = definition(f, 'original'),
    saved = append(f, [key.event]);
  f.fail('original-0', { aborted: false, errorCode: 'synthetic_failure', errorStatus: 500 });
  await withAuthenticatedMemberApiKey(
    f.members,
    'synthetic-key-m1',
    (actor) =>
      f.generations.retireJobHistory(
        meta('retire-original', NOW + 90000, actor.id),
        ['original-0'],
        actor,
        new Map(),
      ),
    env,
  );
  const target = rebuild(t, f.db),
    before = databaseState(target),
    changes = target.prepare('SELECT total_changes() AS n').get().n;
  assert.throws(
    () => f.inputs.readGenerationIdempotencyContext('m1', 'generate', key.payload.keyHash),
    (e) => e.code === 'input_unavailable',
  );
  target.transaction(() =>
    new RelationalCommitReplayer(target).replayCommit(
      readRelationalCommit(f.db, saved.commitSeq),
      f.inputs,
    ),
  )();
  assert.equal(databaseState(target), before);
  assert.equal(target.prepare('SELECT total_changes() AS n').get().n, changes);
});

test('Opening每键显式原fixed/random与noCache决定独立于目标first intent，不能从同seed猜模式', async (t) => {
  const f = await fixture(t);
  f.accept('original');
  for (const [seedMode, noCache] of [
    ['fixed', false],
    ['random', true],
  ]) {
    const key = definition(f, 'original', seedMode, { seedMode, noCache });
    key.payload.intentFingerprint = f.inputs.generationInputIntent(
      'm1',
      key.request,
      key.context,
      seedMode,
      noCache,
    ).fingerprint;
    append(f, [key.event], `import-${seedMode}`);
    assert.deepEqual(
      f.inputs.readGenerationIdempotencyContext('m1', 'generate', key.payload.keyHash),
      key.context,
    );
  }
  const target = rebuild(t, f.db);
  assert.deepEqual(
    target
      .prepare('SELECT key_hash,intent_fingerprint FROM idempotency_keys ORDER BY key_hash')
      .all(),
    f.db
      .prepare('SELECT key_hash,intent_fingerprint FROM idempotency_keys ORDER BY key_hash')
      .all(),
  );
  assert.equal(
    target.prepare("SELECT seed_mode FROM generation_requests WHERE id='original'").get().seed_mode,
    'fixed',
  );
});

test('Opening重领严格绑定原source角色和projection，不能因commit缺失或同号而fallback', async (t) => {
  const f = await fixture(t);
  f.accept('original');
  const key = definition(f, 'original');
  append(f, [key.event]);
  const original = databaseState(f.db),
    mapping = f.inputs.readGenerationIdempotency('m1', 'generate', key.payload.keyHash);
  assert.equal(mapping.commitSeq, null);
  assert.equal(
    f.db.prepare('SELECT 1 FROM commits WHERE command_id=?').get(mapping.originalCommandId),
    undefined,
  );
  f.db.transaction(() => {
    const result = f.generations.readAcceptedGenerationReceipt(
      { ...meta(mapping.originalCommandId, NOW, 'm1'), fingerprint: mapping.intentFingerprint },
      mapping,
    );
    assert.equal(Object.hasOwn(result, 'commitSeq'), false);
    assert.deepEqual(result.receipt, key.payload.archivedReceipt);
    assert.throws(() =>
      f.generations.readAcceptedGenerationReceipt(
        { ...meta('wrong-command', NOW, 'm1'), fingerprint: mapping.intentFingerprint },
        mapping,
      ),
    );
    assert.throws(() =>
      f.generations.readAcceptedGenerationReceipt(
        { ...meta(mapping.originalCommandId, NOW, 'm2'), fingerprint: mapping.intentFingerprint },
        mapping,
      ),
    );
  })();
  assert.equal(databaseState(f.db), original);
  const sourceId = f.db
    .prepare("SELECT source_event_id AS id FROM generation_requests WHERE id='original'")
    .get().id;
  f.db.prepare('UPDATE idempotency_keys SET source_event_id=?').run(sourceId);
  const wrongSource = databaseState(f.db);
  assert.throws(
    () => f.inputs.readGenerationIdempotencyContext('m1', 'generate', key.payload.keyHash),
    (e) => e.code === 'input_unavailable',
  );
  assert.equal(databaseState(f.db), wrongSource);
});

test('Opening live原模式、回执、来源、成员及child闭包错误全笔拒绝', async (t) => {
  const f = await fixture(t);
  f.accept('original', { samples: 2 });
  opened(f, 'original', 1);
  assert.throws(() => append(f, [definition(f, 'opened-original').event]));
  const complete = definition(f, 'original');
  for (const patch of [
    { noCache: null },
    { seedMode: null },
    { noCache: false },
    { seedMode: 'random' },
    { commitSeq: '3' },
    { archivedReceipt: null },
    { archivedReceipt: { requestId: 'wrong', jobIds: ['original-0', 'original-1'] } },
    { archivedReceipt: { requestId: 'original', jobIds: ['original-0'] } },
    { originalJobId: 'original-1' },
    { originalMemberId: 'm2' },
    { origin: origin('x'), requestId: 'missing' },
    { origin: { sourceId: 'wrong-source', originalId: 'original' } },
  ]) {
    const before = databaseState(f.db),
      event = definition(f, 'original', 'original', patch).event;
    assert.throws(() => append(f, [event]));
    assert.equal(databaseState(f.db), before);
  }
  append(f, [complete.event]);
  assert.deepEqual(
    f.inputs.readOpeningGenerationIdempotency('m1', 'generate', complete.payload.keyHash).receipt,
    complete.payload.archivedReceipt,
  );
});

test('Opening原键第二次SQL写后故障外catch仍全表/事实/回执/游标回滚，可原提交重试', async (t) => {
  const f = await fixture(t);
  f.accept('original');
  const first = definition(f, 'original', 'first'),
    second = definition(f, 'original', 'second'),
    before = databaseState(f.db);
  f.db.exec(
    "CREATE TEMP TABLE outer_sentinel(value INTEGER); CREATE TEMP TRIGGER fail_second AFTER INSERT ON main.idempotency_keys WHEN NEW.key_hash='" +
      second.payload.keyHash +
      "' BEGIN SELECT RAISE(ABORT,'second key fault'); END",
  );
  f.db.transaction(() => {
    assert.throws(() => append(f, [first.event, second.event]), /second key fault/);
    f.db.prepare('INSERT INTO outer_sentinel VALUES(1)').run();
  })();
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM outer_sentinel').get().n, 1);
  f.db.exec('DROP TRIGGER fail_second');
  append(f, [first.event, second.event]);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM idempotency_keys').get().n, 2);
});

test('恶意完整source同时换原意图和projection仍不能全回放或经唯一installer ready', async (t) => {
  const f = await fixture(t);
  f.accept('original');
  initializeFixtureConfiguration(f.db);
  const prefix = f.db.serialize(),
    target = rebuild(t, f.db),
    bad = definition(f, 'original', 'bad', { intentFingerprint: 'b'.repeat(64) }),
    { policyVersion, pricingVersion, ...metadata } = meta('raw-untrusted-import', NOW + 100),
    saved = f.db.transaction(() =>
      appendRelationalCommit(f.db, {
        ...metadata,
        receipt: { imported: true },
        expectedVersions: { [bad.event.streamId]: '0' },
        events: [
          { ...bad.event, accountId: null, occurredAt: NOW + 100, policyVersion, pricingVersion },
        ],
      }),
    )(),
    replay = new RelationalCommitReplayer(target),
    before = databaseState(target);
  target.exec('CREATE TEMP TABLE outer_sentinel(value INTEGER)');
  target.transaction(() => {
    assert.throws(
      () => replay.replayCommit(readRelationalCommit(f.db, saved.commitSeq), f.inputs),
      (e) => e.code === 'input_unavailable',
    );
    target.prepare('INSERT INTO outer_sentinel VALUES(1)').run();
  })();
  assert.equal(databaseState(target), before);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM outer_sentinel').get().n, 1);
  // 只为不可信来源负例合成相同坏projection；不能让缺projection/错误cut遮住intent核验。
  const p = bad.payload;
  f.db
    .prepare(
      `INSERT INTO idempotency_keys(original_member_id,operation,key_hash,intent_fingerprint,
    original_request_id,original_job_id,original_command_id,request_id,commit_seq,context_id,resolution,
    origin_ref,archived_receipt,created_at,source_event_id) VALUES(?,?,?,?,?,?,?,?,NULL,?,?,?,?,?,?)`,
    )
    .run(
      p.originalMemberId,
      p.operation,
      p.keyHash,
      p.intentFingerprint,
      p.originalRequestId,
      p.originalJobId,
      p.originalCommandId,
      p.requestId,
      p.contextId,
      p.resolution,
      p.originRef,
      canonicalJson(p.archivedReceipt),
      p.createdAt,
      saved.events[0].eventId,
    );
  const sourcePath = path.join(f.dir, 'untrusted-complete-source.sqlite');
  await f.db.backup(sourcePath);
  const source = new Database(sourcePath);
  source.pragma('journal_mode=DELETE');
  source.close();
  const sourceSha256 = relationalSnapshotSha256(sourcePath),
    badState = databaseState(f.db);
  await assert.rejects(
    verifyAndInstallRelationalStorage(f.db, {
      sourcePath,
      sourceSha256,
      historyId: 'generation-history',
      finalCommitSeq: saved.commitSeq,
      replicas: { dataDir: f.dir },
    }),
    (e) => e.code === 'input_unavailable',
  );
  assert.equal(f.db.prepare('SELECT status FROM projection_state').get().status, 'building');
  assert.equal(databaseState(f.db), badState);
  assert.equal(relationalSnapshotSha256(sourcePath), sourceSha256);
  const healthy = new Database(prefix);
  t.after(() => healthy.close());
  const h = { ...f, db: healthy, inputs: new RelationalInputStore(healthy) },
    good = append(h, [definition(h, 'original', 'bad').event]);
  target.transaction(() =>
    replay.replayCommit(readRelationalCommit(healthy, good.commitSeq), h.inputs),
  )();
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM idempotency_keys').get().n, 1);
});

test('合法ready真实Native Key及member Cookie HTTP重领原Opening receipt，改变模式/当前凭据拒绝', async (t) => {
  const f = await fixture(t, { seconds: 0 });
  f.accept('native');
  f.dispatch('native-0', NOW);
  await f.publish('native-0');
  f.accept('member', { member: true, samples: 2 });
  const native = definition(f, 'native', 'native-key'),
    member = definition(f, 'member', 'member-key');
  // 每键自己的原 context 可以不同于 target request，不能借目标的首次默认定义。
  const ownContext = structuredClone(native.context);
  ownContext.native.defaults.scale += 1;
  const ownContextId = `context_${domainDigest(ownContext)}`;
  native.payload.contextId = ownContextId;
  const tombstone = {
    ...native.payload,
    keyHash: generationIdempotencyKeyHash('unknown-key'),
    originalRequestId: 'missing-original',
    originalJobId: 'missing-child',
    requestId: null,
    contextId: null,
    noCache: null,
    seedMode: null,
    resolution: 'unavailable',
    archivedReceipt: null,
  };
  f.db.transaction(() => {
    const payload = f.inputs.stageOpeningContext(ownContextId, ownContext);
    append(f, [
      {
        eventType: 'ContextRegistered',
        streamId: `context:${ownContextId}`,
        memberId: null,
        payload,
      },
      native.event,
      member.event,
      {
        eventType: 'OpeningIdempotencyRecorded',
        streamId: `idempotency:m1:generate:${tombstone.keyHash}`,
        memberId: 'm1',
        payload: tombstone,
      },
    ]);
  })();
  assert.deepEqual(
    f.inputs.readGenerationIdempotencyContext('m1', 'generate', native.payload.keyHash),
    ownContext,
  );
  const apiKey = 'STA1N-opening-restored-key';
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', apiKey))();
  const session = await createMemberSession(
    f.members,
    await authenticateMemberApiKey(f.members, apiKey),
    {},
    NOW,
  );
  await installReady(f);
  const events = new EventEmitter(),
    previews = new JobPreviews(),
    budget = createGenerationPayloadBudget(() => f.inputs.activePayloadBytes()),
    deps = {
      sqlite: f.db,
      runtime: f.commandOnlyRuntime,
      events,
      previews,
      payloadBudget: budget,
      replicas: { dataDir: f.dir },
      env: {},
      now: () => NOW + 1000,
    },
    handleNative = createRelationalNativeHttp({
      ...deps,
      schedule() {},
      timeoutMs: 1000,
      async readJson(req) {
        const chunks = [];
        for await (const chunk of req) chunks.push(chunk);
        return JSON.parse(Buffer.concat(chunks));
      },
      sendImage(res, status, mimeType, buffer, headers) {
        res.writeHead(status, { 'content-type': mimeType, ...headers });
        res.end(buffer);
      },
    }),
    handleMember = createRelationalMemberGenerationHttp({ ...deps, scheduleQueueDrain() {} }),
    server = createServer(async (req, res) => {
      try {
        const url = new URL(req.url, `http://${req.headers.host}`);
        if (await handleNative(req, res, url)) return;
        if (await handleMember(req, res, url)) return;
        res.writeHead(404);
        res.end();
      } catch (error) {
        const result = nativeGenerationError(error);
        res.writeHead(result.statusCode, { 'content-type': 'application/json' });
        res.end(JSON.stringify(result.body));
      }
    });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const url = `http://127.0.0.1:${server.address().port}`;
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  });
  const body = (d) => ({ model: d.wire.model, input: d.wire.input, parameters: d.wire.parameters }),
    submit = (endpoint, input, headers) =>
      fetch(url + endpoint, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...headers },
        body: JSON.stringify(input),
      }),
    before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() AS n').get().n;
  const imageResponse = await submit(
    '/ai/generate-image',
    { ...body(native), action: 'generate' },
    {
      authorization: `Bearer ${apiKey}`,
      'idempotency-key': 'native-key',
      accept: 'application/json',
    },
  );
  assert.equal(imageResponse.status, 200);
  assert.deepEqual(Buffer.from((await imageResponse.json()).images[0].image, 'base64'), image);
  const memberResponse = await submit(
    '/api/member/generation/requests',
    { ...body(member), operation: 'generate', confirmation: member.confirmation },
    { cookie: `nai_member_session=${session.token}`, origin: url, 'idempotency-key': 'member-key' },
  );
  assert.equal(memberResponse.status, 202);
  const reply = await memberResponse.json();
  assert.equal(reply.requestId, 'member');
  assert.deepEqual(reply.jobIds, ['member-0', 'member-1']);
  assert.equal(reply.replayed, true);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
  const changed = await submit(
    '/ai/generate-image',
    { ...body(native), action: 'generate', parameters: { ...native.wire.parameters, seed: -1 } },
    { authorization: `Bearer ${apiKey}`, 'idempotency-key': 'native-key' },
  );
  assert.equal(changed.status, 409);
  await changed.arrayBuffer();
  assert.equal(databaseState(f.db), before);
  const changedMember = await submit(
    '/api/member/generation/requests',
    {
      ...body(member),
      operation: 'generate',
      confirmation: {
        ...member.confirmation,
        maxTotal: { ...member.confirmation.maxTotal, v5: '1' },
      },
    },
    { cookie: `nai_member_session=${session.token}`, origin: url, 'idempotency-key': 'member-key' },
  );
  assert.equal(changedMember.status, 409);
  await changedMember.arrayBuffer();
  assert.equal(databaseState(f.db), before);
  const unavailable = await submit(
    '/ai/generate-image',
    { ...body(native), action: 'generate' },
    { authorization: `Bearer ${apiKey}`, 'idempotency-key': 'unknown-key' },
  );
  assert.equal(unavailable.status, 410);
  await unavailable.arrayBuffer();
  assert.equal(databaseState(f.db), before);
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'replacement-current-key'))();
  const revokedState = databaseState(f.db),
    revoked = await submit(
      '/ai/generate-image',
      { ...body(native), action: 'generate' },
      { authorization: `Bearer ${apiKey}`, 'idempotency-key': 'native-key' },
    );
  assert.equal(revoked.status, 401);
  await revoked.arrayBuffer();
  assert.equal(databaseState(f.db), revokedState);
});
