import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { createHash } from 'node:crypto';
import {
  fixture,
  NOW,
  meta,
  keys,
  image,
  databaseState,
} from '../helpers/relational-generation.mjs';
import { RelationalRequestStore } from '../../server/storage/relational-request-store.ts';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { JsonStore } from '../../server/storage/sqlite-store.ts';
import { relationalInputDigest } from '../../server/generation/relational-input.ts';
import { memberCharge } from '../../server/quota/failure-accounting.ts';
import {
  appendRelationalCommit,
  readRelationalCommit,
  replayRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import {
  withAuthenticatedMemberApiKey,
  withAuthenticatedMemberSession,
  createMemberSession,
} from '../../server/members/authentication.ts';

const iso = (at) => new Date(at).toISOString();
const options = (change = {}) => ({
  page: 1,
  pageSize: 25,
  q: '',
  status: '',
  sort: 'createdAt',
  order: 'desc',
  until: iso(NOW + 30 * 86400000),
  ...change,
});
const ids = (page) => page.jobs.map((job) => job.id);
const hasCode = (code) => (error) => error.code === code;

// 查询用的有来源 partial 开账夹具。Opening 实体写 owner 尚未交付，不能把此夹具称为迁移/ready 验证。
function opening(f, name, createdAt, { model = null, charge = false, lateResultAt = null } = {}) {
  const origin = { sourceId: 'synthetic-opening', originalId: name },
    body = { operation: 'generate', model, input: null, parameters: {} },
    logicalBytes = Buffer.byteLength(JSON.stringify(body)),
    sha256 = relationalInputDigest(body, [], logicalBytes),
    common = {
      occurredAt: NOW + 900000,
      memberId: 'm1',
      accountId: null,
      policyVersion: 'historical',
      pricingVersion: 'historical',
    },
    job = {
      jobId: `${name}-0`,
      requestId: name,
      createdAt,
      sampleIndex: 0,
      actualSeed: null,
      status: 'failed',
      queueSeq: null,
      queueExpiresAt: null,
      deadlineAt: null,
      admissionEpoch: 0,
      artifactId: lateResultAt === null ? null : `artifact-${name}`,
      cacheKey: null,
      completedAt: 0,
      errorCode: 'generation_failed',
      errorStatus: 500,
      cancellationReason: null,
      lateResultAt,
      freeSucceededAt: null,
      freeWarningThreshold: null,
      retiredAt: null,
      origin,
    },
    events = [
      {
        ...common,
        eventType: 'OpeningInputRecorded',
        streamId: `input:${name}`,
        payload: {
          inputId: name,
          memberId: 'm1',
          inputSchemaVersion: 1,
          completeness: 'partial',
          inputSha256: sha256,
          logicalBytes,
          origin,
        },
      },
      {
        ...common,
        eventType: 'OpeningRequestRecorded',
        streamId: `request:${name}`,
        payload: {
          requestId: name,
          memberId: 'm1',
          inputId: name,
          contextId: null,
          source: 'historical',
          operation: 'generate',
          model,
          sampleCount: 1,
          seedMode: 'unknown',
          confirmation: null,
          acceptedAt: NOW + 800000,
          retiredAt: null,
          origin,
        },
      },
      { ...common, eventType: 'OpeningJobRecorded', streamId: `request:${name}`, payload: job },
    ];
  if (job.artifactId)
    events.push({
      ...common,
      eventType: 'OpeningArtifactRecorded',
      streamId: `artifact:${job.artifactId}`,
      payload: {
        artifactId: job.artifactId,
        memberId: 'm1',
        inputId: name,
        originalAttemptId: null,
        operation: 'generate',
        mimeType: 'image/webp',
        byteLength: image.length,
        sha256: createHash('sha256').update(image).digest('hex'),
        publishedAt: lateResultAt,
        retiredAt: null,
        origin,
      },
    });
  const { policyVersion: _policy, pricingVersion: _price, ...metadata } = meta(`opening-${name}`);
  f.db.transaction(() => {
    const result = appendRelationalCommit(f.db, {
      ...metadata,
      expectedVersions: Object.fromEntries(events.map((event) => [event.streamId, '0'])),
      receipt: null,
      events,
    });
    f.db
      .prepare(
        `INSERT INTO generation_inputs(id,member_id,schema_version,completeness,sha256,logical_bytes,body)
      VALUES(?,'m1',1,'partial',?,?,?)`,
      )
      .run(name, sha256, logicalBytes, JSON.stringify(body));
    f.db
      .prepare(
        `INSERT INTO generation_requests(id,member_id,input_id,context_id,source,operation,model,
      sample_count,seed_mode,accepted_at,source_event_id) VALUES(?,'m1',?,NULL,'historical','generate',?,1,'unknown',?,?)`,
      )
      .run(name, name, model, NOW + 800000, result.events[1].eventId);
    if (job.artifactId)
      f.db
        .prepare(
          `INSERT INTO artifacts(id,member_id,input_id,operation,mime_type,byte_length,sha256,published_at,source_event_id)
      VALUES(?,'m1',?,'generate','image/webp',?,?,?,?)`,
        )
        .run(
          job.artifactId,
          name,
          image.length,
          events[3].payload.sha256,
          lateResultAt,
          result.events[3].eventId,
        );
    f.db
      .prepare(
        `INSERT INTO generation_jobs(id,request_id,sample_index,status,completed_at,admission_epoch,artifact_id,late_result_at,source_event_id)
      VALUES(?,?,0,'failed',0,0,?,?,?)`,
      )
      .run(job.jobId, name, job.artifactId, lateResultAt, result.events[2].eventId);
    if (charge) {
      // 已归档收费凭据不再保有 job FK，仍保留稳定 original job/member 归属与原已知零金额。
      const version = f.db
        .prepare(
          "SELECT CAST(stream_version AS TEXT) AS n FROM stream_heads WHERE stream_id='account:a'",
        )
        .get().n;
      const proof = appendRelationalCommit(f.db, {
        ...metadata,
        commandId: `charge-${name}`,
        expectedVersions: { 'account:a': version },
        receipt: null,
        events: [
          {
            ...common,
            eventId: `charge-proof-${name}`,
            eventType: 'OpeningChargeRecorded',
            streamId: 'account:a',
            accountId: 'a',
            payload: {
              originalJobId: job.jobId,
              originalMemberId: 'm1',
              originalAccountId: 'a',
              jobId: null,
              walletAccountId: null,
              walletMemberId: null,
              attemptId: null,
              status: 'refunded',
              resolution: 'archived',
              originRef: `old-${name}`,
              resources: { v5: '0', anlasFixed: '0', anlasPurchased: '0' },
              billingMode: 'anlas',
              estimated: null,
              v5Equivalent: '0',
              sharedV5Reserved: '0',
              configVersion: 1,
              reservedAt: 0,
              terminalAt: 0,
              reservationEventId: `charge-proof-${name}`,
              terminalEventId: `charge-proof-${name}`,
              origin,
            },
          },
        ],
      });
      const project = f.generations.createGenerationProjector(proof.events);
      for (const event of proof.events) project(event);
    }
  })();
}

test('子任务总量、页/排序/until/字面搜索与原真实 JsonStore 成员查询一致，不展开父页', async (t) => {
  const f = await fixture(t),
    store = new RelationalRequestStore(f.db);
  const oldDir = await mkdtemp(path.join(tmpdir(), 'nai-member-history-legacy-')),
    old = new JsonStore(oldDir);
  await old.init();
  t.after(async () => {
    await old.close();
    await rm(oldDir, { recursive: true, force: true });
  });
  const oldDb = structuredClone(old.db);
  oldDb.users = [
    { id: 'm1', token: keys.m1, enabled: true },
    { id: 'm2', token: keys.m2, enabled: true },
  ];
  oldDb.jobs = [];
  oldDb.images = [];
  for (const [index, name, model, samples, memberId] of [
    [0, 'first', 'nai-diffusion-4-5-full', 3, 'm1'],
    [1, 'literal_%', 'nai-diffusion-4-5-full', 1, 'm1'],
    [2, 'last', 'nai-diffusion-4-full', 2, 'm1'],
    [3, 'future', 'nai-diffusion-4-full', 1, 'm1'],
    [0, 'other', 'nai-diffusion-4-full', 2, 'm2'],
  ]) {
    await f.accept(name, { samples, memberId, model, now: NOW + index * 100 });
    if (name === 'first') {
      for (let i = 0; i < samples; i++) {
        f.dispatch(`first-${i}`, NOW + i * 20000 + 1);
        await f.publish(`first-${i}`, NOW + i * 20000 + 3);
      }
    } else if (name === 'literal_%') {
      f.dispatch('literal_%-0', NOW + 60001);
      f.fail('literal_%-0', { aborted: false }, NOW + 60002);
    }
    for (let i = 0; i < samples; i++)
      oldDb.jobs.push({
        id: `${name}-${i}`,
        userToken: keys[memberId],
        imageId: name === 'first' ? `artifact-first-${i}` : null,
        status:
          name === 'first'
            ? 'done'
            : name === 'literal_%'
              ? 'failed'
              : i === 0
                ? 'queued'
                : 'pending',
        createdAt: iso(NOW + index * 100),
        request: { model, prompt: 'private-marker' },
      });
    if (name === 'first')
      for (let i = 0; i < samples; i++)
        oldDb.images.push({ id: `artifact-first-${i}`, token: keys.m1 });
  }
  await old.write(oldDb);
  const user = oldDb.users[0];
  for (const change of [
    { pageSize: 2 },
    { page: 2, pageSize: 2 },
    { page: 3, pageSize: 2 },
    { page: 4, pageSize: 2 },
    { sort: 'createdAt', order: 'asc' },
    { sort: 'model' },
    { sort: 'status', order: 'asc' },
    { status: 'pending' },
    { q: '4-5' },
    { q: '%' },
    { q: '_' },
    { q: '\\' },
    { q: 'private-marker' },
    { q: 'FIRST' },
    { page: 1000000, pageSize: 100 },
  ]) {
    const opts = options({ until: iso(NOW + 200), ...change }),
      actual = store.readMemberJobPage('m1', opts),
      expected = await old.readMemberJobPage(user, opts);
    assert.deepEqual(
      { ...actual, jobs: actual.jobs.map(({ charge: _charge, requestId: _parent, ...j }) => j) },
      {
        ...expected,
        jobs: expected.jobs.map(({ charge: _charge, requestId: _parent, ...j }) => j),
      },
    );
    assert.ok(actual.jobs.every((job) => job.id.startsWith(`${job.requestId}-`)));
  }
  assert.equal(store.readMemberJobPage('m1', options({ until: iso(NOW + 200) })).total, 6);
  assert.equal(store.readMemberJobPage('m2', options()).total, 2);
  for (const change of [
    { page: 0 },
    { pageSize: 101 },
    { q: 'x'.repeat(201) },
    { sort: 'created_at; DELETE' },
    { sort: '__proto__' },
    { status: 'cancelled' },
    { order: 'DESC' },
    { until: '2026-11-14' },
    { until: 'invalid' },
  ]) {
    assert.throws(
      () => store.readMemberJobPage('m1', options(change)),
      hasCode('invalid_parameters'),
    );
  }
});

test('原 child 时间与未知 NULL/已知 0 独立于父受理和开账 header，原收费未知不补零', async (t) => {
  const f = await fixture(t),
    store = new RelationalRequestStore(f.db);
  opening(f, 'zero', 0, { charge: true, lateResultAt: 0 });
  opening(f, 'unknown', null);
  await f.accept('current');
  const page = store.readMemberJobPage('m1', options({ until: iso(0), order: 'asc' }));
  assert.deepEqual(ids(page), ['unknown-0', 'zero-0']);
  assert.equal(page.jobs[0].createdAt, null);
  assert.equal(page.jobs[0].model, null);
  assert.equal(page.jobs[0].charge, null);
  assert.equal(page.jobs[1].createdAt, iso(0));
  assert.deepEqual(
    page.jobs[1].charge,
    memberCharge({
      status: 'refunded',
      resource: 'anlas',
      amount: '0',
      fixed: '0',
      purchased: '0',
      estimated: null,
      billingMode: 'anlas',
      v5Equivalent: '0',
    }),
  );
  assert.deepEqual(ids(store.readMemberJobPage('m1', options({ until: iso(0) }))), [
    'zero-0',
    'unknown-0',
  ]);
  assert.equal(store.readMemberRequest('zero', 'm1').jobs[0].createdAt, iso(0));
  assert.equal(store.readMemberRequest('unknown', 'm1').jobs[0].createdAt, null);
  assert.equal(store.readMemberRequest('zero', 'm1').jobs[0].fairCharge.estimated, null);
  assert.equal(store.readMemberLateResults('m1')[0].lateResultAt, iso(0));
  assert.equal(store.readMemberLateResults('m1')[0].lateResultAvailable, true);
});

test('JobAccepted 原 child 时点决定 until 与排序，不使用父时点或后来的 source header', async (t) => {
  const f = await fixture(t),
    baseline = f.db.serialize();
  await f.accept('original-time', { samples: 2 });
  const seq = f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get().seq,
    stored = structuredClone(readRelationalCommit(f.db, seq));
  for (const list of [stored.command.events, stored.events])
    for (const event of list) {
      if (event.eventType === 'JobAccepted')
        event.occurredAt = NOW - 100 + event.payload.sampleIndex;
    }
  const source = new Database(baseline),
    target = new Database(':memory:');
  t.after(() => {
    source.close();
    target.close();
  });
  source.pragma('foreign_keys=ON');
  target.pragma('foreign_keys=ON');
  initializeRelationalStorage(target, 'generation-history');
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
  const replayer = new RelationalCommitReplayer(target);
  for (const { seq: previous } of source
    .prepare(
      'SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE commit_seq<? ORDER BY commit_seq',
    )
    .all(seq)) {
    target.transaction(() => replayer.replayCommit(readRelationalCommit(source, previous)))();
  }
  target.transaction(() => replayer.replayCommit(stored, new RelationalInputStore(source)))();
  target.prepare("UPDATE members SET auth_epoch='current-private-epoch' WHERE id='m1'").run();
  const store = new RelationalRequestStore(target),
    page = store.readMemberJobPage('m1', options({ until: iso(NOW - 100) }));
  assert.deepEqual(ids(page), ['original-time-0']);
  assert.equal(page.jobs[0].createdAt, iso(NOW - 100));
  assert.equal(store.readMemberRequest('original-time', 'm1').createdAt, iso(NOW));
});

test('迟到冷历史最近十条保留失败/退款，实际产物、原事实和稳定归属共同授权', async (t) => {
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { env, seconds: 0 }),
    store = new RelationalRequestStore(f.db),
    replicas = new RelationalReplicaStore(f.db, undefined, env);
  for (let i = 0; i < 12; i++) {
    const id = `late-${String(i).padStart(2, '0')}`,
      at = NOW + i * 20000;
    await f.accept(id, { now: at });
    f.dispatch(`${id}-0`, at + 1);
    f.fail(`${id}-0`, { aborted: false }, at + 2);
    await f.publish(`${id}-0`, at + 3);
  }
  await f.accept('other', { memberId: 'm2', now: NOW + 240000 });
  f.dispatch('other-0', NOW + 240001);
  await f.publish('other-0', NOW + 240003);
  const late = store.readMemberLateResults('m1');
  assert.equal(late.length, 10);
  assert.deepEqual(
    late.map((j) => j.id),
    Array.from({ length: 10 }, (_, i) => `late-${String(11 - i).padStart(2, '0')}-0`),
  );
  assert.ok(
    late.every(
      (j) => j.status === 'failed' && j.charge.status === 'refunded' && j.lateResultAvailable,
    ),
  );
  assert.equal(store.readMemberJobPage('m1', options({ status: 'late' })).total, 12);
  assert.deepEqual(store.readMemberLateResults('m2'), []);
  const asOwner = (id, run) =>
    withAuthenticatedMemberApiKey(
      f.members,
      keys.m1,
      (actor) => run(meta(id, NOW + 300000, actor.id), actor),
      env,
    );
  await asOwner('retire-job', (metadata, actor) =>
    f.generations.retireJobHistory(metadata, ['late-11-0'], actor, new Map()),
  );
  await asOwner('retire-artifact', (metadata, actor) =>
    replicas.retireArtifactHistory(metadata, ['artifact-late-10-0'], actor, new Map()),
  );
  assert.equal(store.readMemberJobPage('m1', options()).total, 11);
  assert.equal(store.readMemberJobPage('m1', options({ status: 'late' })).total, 10);
  assert.equal(store.readMemberLateResults('m1')[0].id, 'late-09-0');
  assert.equal(store.readMemberJobPage('m1', options({ q: 'late-10' })).jobs[0].imageId, null);
  // 损坏投影的跨成员 artifact 及伪找回时间不能生成可领取结果；不拿当前 Key 修正所有权。
  assert.throws(
    () =>
      f.db
        .prepare("UPDATE generation_jobs SET artifact_id='artifact-other-0' WHERE id='late-09-0'")
        .run(),
    /does not own artifact/,
  );
  f.db.exec('DROP TRIGGER job_artifact_owner_update');
  f.db
    .prepare("UPDATE generation_jobs SET artifact_id='artifact-other-0' WHERE id='late-09-0'")
    .run();
  f.db
    .prepare("UPDATE generation_jobs SET late_result_at=late_result_at+1 WHERE id='late-08-0'")
    .run();
  const visible = store.readMemberLateResults('m1');
  assert.equal(
    visible.some((j) => ['late-09-0', 'late-08-0'].includes(j.id)),
    false,
  );
  assert.equal(store.readMemberJobPage('m1', options({ q: 'late-09' })).jobs[0].imageId, null);
  assert.equal(store.readMemberRequest('late-08', 'm1').jobs[0].lateResultAvailable, false);
});

test('当前认证边界保留换 Key 与 Cookie 语义，停止接单仍可读，撤销立即拒绝且零写', async (t) => {
  const f = await fixture(t),
    store = new RelationalRequestStore(f.db);
  await f.accept('owned');
  f.dispatch('owned-0');
  await f.publish('owned-0');
  const user = await withAuthenticatedMemberApiKey(f.members, keys.m1, (member) => member, {}),
    session = await createMemberSession(f.members, user, {}, NOW),
    byKey = (key) =>
      withAuthenticatedMemberApiKey(
        f.members,
        key,
        (member) => store.readMemberJobPage(member.id, options()),
        {},
      ),
    bySession = () =>
      withAuthenticatedMemberSession(
        f.members,
        session.token,
        {},
        (member) => store.readMemberJobPage(member.id, options()),
        NOW + 1,
      );
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'rotated-private-key'))();
  assert.equal(await byKey(keys.m1), null);
  assert.deepEqual(ids(await byKey('rotated-private-key')), ['owned-0']);
  assert.deepEqual(ids(await bySession()), ['owned-0']);
  f.db.prepare("UPDATE members SET accepting_jobs=0 WHERE id='m1'").run();
  const before = databaseState(f.db);
  assert.deepEqual(ids(await bySession()), ['owned-0']);
  assert.deepEqual(store.readMemberLateResults('m1'), []);
  assert.equal(databaseState(f.db), before);
  // 模拟外部等待后再进入原认证事务，不能使用等待前的成员快照。
  f.db.transaction(() => f.members.setMemberStatus(meta('disable'), 'm1', 'disabled'))();
  assert.equal(await bySession(), null);
  for (const read of [
    () => store.readMemberJobPage('m1', options()),
    () => store.readMemberLateResults('m1'),
  ]) {
    assert.throws(read, hasCode('session_expired'));
  }
});

test('页、recent30 与迟到读取只取薄事实且有实际 SQL 计划，重开与完整提交回放不回填正文', async (t) => {
  const f = await fixture(t, { seconds: 0 });
  await f.accept('batch', { samples: 3 });
  f.dispatch('batch-0');
  f.fail('batch-0', { aborted: false });
  await f.publish('batch-0');
  const store = new RelationalRequestStore(f.db),
    before = databaseState(f.db),
    sql = [],
    prepare = f.db.prepare,
    fetch = t.mock.method(globalThis, 'fetch', () => {
      throw new Error('read has no network');
    });
  f.db.prepare = function (text) {
    sql.push(text);
    return prepare.call(this, text);
  };
  let page, late;
  try {
    page = store.readMemberJobPage('m1', options({ pageSize: 30 }));
    late = store.readMemberLateResults('m1');
    store.readMemberJobPage('m1', options({ q: 'batch', sort: 'model', status: 'late' }));
  } finally {
    f.db.prepare = prepare;
  }
  assert.equal(fetch.mock.callCount(), 0);
  assert.equal(
    sql.some((text) => /generation_inputs|input_assets|\bbody\b|\bbytes\b/i.test(text)),
    false,
  );
  assert.equal(JSON.stringify([page, late]).includes('private generation prompt'), false);
  assert.equal(JSON.stringify([page, late]).includes('synthetic-key'), false);
  assert.equal(databaseState(f.db), before);
  const query = sql.find((text) => text.includes('ORDER BY origin.created_at')),
    params = {
      memberId: 'm1',
      until: Date.parse(options().until),
      status: '',
      q: '%%',
      limit: 30,
      offset: 0,
    },
    plan = f.db
      .prepare(`EXPLAIN QUERY PLAN ${query}`)
      .all(params)
      .map((r) => r.detail)
      .join('\n');
  assert.match(plan, /SEARCH r USING INDEX idx_requests_member_time/);
  assert.match(plan, /SEARCH j USING INDEX sqlite_autoindex_generation_jobs_3 \(request_id=\?\)/);
  assert.match(plan, /SEARCH domain_events USING INDEX sqlite_autoindex_domain_events_3/);
  assert.match(plan, /TEMP B-TREE FOR ORDER BY/);
  console.log(`child-history query plan:\n${plan}`);
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replay = new RelationalCommitReplayer(target);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replay.replayCommit(readRelationalCommit(f.db, seq), f.inputs))();
  }
  const restored = new RelationalRequestStore(target);
  assert.throws(() => restored.readMemberJobPage('m1', options()), hasCode('session_expired'));
  // 读验证只提供当前私有 epoch；业务回放未恢复凭据，不冒称完整私有恢复或 ready。
  target.prepare("UPDATE members SET auth_epoch='current-private-epoch' WHERE id='m1'").run();
  assert.deepEqual(restored.readMemberJobPage('m1', options({ pageSize: 30 })), page);
  assert.deepEqual(restored.readMemberLateResults('m1'), late);
  f.db.close();
  const reopened = new Database(path.join(f.dir, 'library.sqlite'));
  try {
    const beforeRead = databaseState(reopened),
      reads = new RelationalRequestStore(reopened);
    assert.deepEqual(reads.readMemberJobPage('m1', options({ pageSize: 30 })), page);
    assert.deepEqual(reads.readMemberLateResults('m1'), late);
    assert.equal(databaseState(reopened), beforeRead);
    for (const table of ['generation_jobs', 'charges', 'artifacts']) {
      assert.equal(
        JSON.stringify(reopened.prepare(`SELECT * FROM ${table}`).all()).includes(
          'private generation prompt',
        ),
        false,
      );
    }
  } finally {
    reopened.close();
  }
});
