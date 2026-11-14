import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { rename, writeFile } from 'node:fs/promises';
import {
  fixture,
  meta,
  NOW,
  keys,
  image,
  databaseState,
} from '../helpers/relational-generation.mjs';
import {
  normalizeNovelAiRequest,
  DIRECT_URL_MAX_STEPS,
  buildNovelAiParameters,
} from '../../server/protocols/novelai-client.ts';
import { nativeNormalizationContext } from '../../server/protocols/native-image.ts';
import {
  requestNormalizationDefaults,
  generationRequestFingerprint,
  requestCacheKey,
} from '../../server/generation/request-fingerprint.ts';
import { prepareRelationalInput } from '../../server/generation/relational-input.ts';
import { generationIdempotencyKeyHash } from '../../server/generation/admission.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import {
  readRelationalArtifactContent,
  copyRelationalArtifactReplica,
} from '../../server/storage/relational-replica-io.ts';
import { R2Storage } from '../../server/storage/r2-storage.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { relationalSnapshotSha256 } from '../../server/storage/relational-private-restore.ts';

async function setup(t, options = {}) {
  const f = await fixture(t, { seconds: 0, ...options });
  const context = (defaults = {}) => ({
    kind: 'direct',
    native: nativeNormalizationContext(),
    defaults: requestNormalizationDefaults(defaults),
  });
  const request = (changes = {}, captured = context()) =>
    normalizeNovelAiRequest(
      {
        tag: 'shared cache prompt',
        model: 'nai-diffusion-4-5-full',
        width: 128,
        height: 128,
        steps: 30,
        seed: 42,
        ...changes,
      },
      captured.defaults,
      { maxSteps: DIRECT_URL_MAX_STEPS },
    );
  const command = (id, req, ctx, seedMode, noCache, memberId = 'm1') => ({
    ...meta(id, NOW, memberId),
    fingerprint: generationRequestFingerprint(req, { randomSeed: seedMode === 'random', noCache }),
  });
  const authorize = (run, memberId = 'm1', key = keys[memberId]) =>
    withAuthenticatedMemberApiKey(f.members, key, () => run(), {});
  const accept = (id, req = request(), ctx = context(), seedMode = 'fixed') =>
    authorize(() => {
      const intent = f.inputs.generationInputIntent('m1', req, ctx, seedMode, true);
      return f.generations.acceptGeneration(command(`accept-${id}`, req, ctx, seedMode, true), {
        memberId: 'm1',
        inputId: `input-${id}`,
        requestId: id,
        source: 'direct',
        request: req,
        context: ctx,
        seedMode,
        confirmation: null,
        idempotency: { keyHash: generationIdempotencyKeyHash(`producer-${id}`), noCache: true },
        jobs: [
          {
            jobId: `${id}-0`,
            status: 'pending',
            queueSeq: null,
            queueExpiresAt: null,
            deadlineAt: null,
            admissionEpoch: 0,
            cacheKey: intent.cacheKey,
          },
        ],
      });
    });
  const reuseArgs = (
    id,
    sourceJobId,
    req = request(),
    ctx = context(),
    seedMode = 'fixed',
    cached,
    memberId = 'm1',
  ) => [
    command(`reuse-${id}`, req, ctx, seedMode, false, memberId),
    {
      memberId,
      request: req,
      context: ctx,
      seedMode,
      noCache: false,
      sourceJobId,
      idempotency: { keyHash: generationIdempotencyKeyHash(id), noCache: false },
      ...(cached
        ? {
            cached: {
              requestId: id,
              jobId: `${id}-0`,
              inputId: `input-${id}`,
              source: 'direct',
              artifactId: cached,
            },
          }
        : {}),
    },
  ];
  const reuse = (...args) =>
    authorize(() => f.generations.reuseGeneration(...reuseArgs(...args)), args[6] ?? 'm1');
  return {
    ...f,
    context,
    request,
    command,
    authorize,
    acceptCachedSource: accept,
    reuse,
    reuseArgs,
  };
}

function replay(t, f) {
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replayer = new RelationalCommitReplayer(target),
    source = new RelationalInputStore(f.db),
    before = f.db.serialize(),
    previousFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = () => {
    calls++;
    throw new Error('replay must not fetch');
  };
  try {
    const commits = f.db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all();
    for (const { seq } of commits) {
      target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), source))();
    }
    for (const table of [
      'generation_inputs',
      'normalization_contexts',
      'generation_requests',
      'generation_jobs',
      'generation_attempts',
      'charges',
      'artifacts',
      'artifact_replicas',
      'idempotency_keys',
      'wallets',
    ]) {
      assert.deepEqual(
        target.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
        f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
        table,
      );
    }
    const once = target.serialize();
    for (const { seq } of commits) {
      assert.equal(
        target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), source))()
          .replayed,
        true,
      );
    }
    assert.ok(target.serialize().equals(once));
    assert.ok(f.db.serialize().equals(before));
    for (const table of ['api_keys', 'member_sessions', 'upstream_credentials']) {
      assert.equal(target.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0);
    }
    assert.throws(() => assertRelationalStorageReady(target), /incomplete/);
  } finally {
    globalThis.fetch = previousFetch;
  }
  assert.equal(calls, 0);
  return target;
}

test('活动复用只保存每键自己的首次context/随机意图，不新建任务输入或费用并可完整回放', async (t) => {
  const f = await setup(t),
    sourceContext = f.context({ defaultArtist: 'first' }),
    req = f.request({ seed: undefined }, sourceContext);
  await f.acceptCachedSource('source', req, sourceContext, 'random');
  const before = [
    'generation_inputs',
    'generation_requests',
    'generation_jobs',
    'generation_attempts',
    'charges',
  ].map((table) => f.db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n);
  const aliasContext = f.context({ defaultArtist: 'different saved defaults' });
  const a = await f.reuse('alias-a', 'source-0', req, aliasContext, 'random');
  const b = await f.reuse('alias-b', 'source-0', req, f.context(), 'random');
  assert.deepEqual(a.receipt, { requestId: 'source', jobIds: ['source-0'] });
  assert.deepEqual(b.receipt, a.receipt);
  assert.deepEqual(
    [
      'generation_inputs',
      'generation_requests',
      'generation_jobs',
      'generation_attempts',
      'charges',
    ].map((table) => f.db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n),
    before,
  );
  assert.deepEqual(
    f.inputs.readGenerationIdempotencyContext(
      'm1',
      'generate',
      generationIdempotencyKeyHash('alias-a'),
    ),
    aliasContext,
  );
  const once = databaseState(f.db);
  assert.equal((await f.reuse('alias-a', 'source-0', req, aliasContext, 'random')).replayed, true);
  assert.equal(databaseState(f.db), once);
  replay(t, f);
});

test('缓存小job引用原产物/input，不新attempt/charge/freecount且整体回放恢复', async (t) => {
  const f = await setup(t);
  await f.acceptCachedSource('source');
  f.dispatch('source-0');
  await f.publish('source-0');
  const artifact = f.db.prepare('SELECT id FROM artifacts').get().id,
    origin = f.db.prepare('SELECT * FROM artifacts').get(),
    before = ['generation_inputs', 'generation_attempts', 'charges'].map((table) =>
      f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
    );
  const candidate = await f.generations.findReusableGeneration(
    {
      memberId: 'm1',
      request: f.request(),
      context: f.context(),
      seedMode: 'fixed',
      noCache: false,
      allowActive: false,
    },
    { dataDir: f.dir },
  );
  assert.equal(candidate.artifactId, artifact);
  const cached = await f.reuse(
    'cached',
    candidate.sourceJobId,
    f.request(),
    f.context(),
    'fixed',
    artifact,
  );
  const job = f.db.prepare("SELECT * FROM generation_jobs WHERE id='cached-0'").get();
  assert.equal(job.status, 'done');
  assert.equal(job.artifact_id, artifact);
  assert.equal(job.queue_seq, null);
  assert.equal(job.free_succeeded_at, null);
  assert.equal(
    f.db.prepare("SELECT input_id FROM generation_requests WHERE id='cached'").get().input_id,
    origin.input_id,
  );
  assert.deepEqual(f.db.prepare('SELECT * FROM artifacts').get(), origin);
  assert.deepEqual(
    ['generation_inputs', 'generation_attempts', 'charges'].map((table) =>
      f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
    ),
    before,
  );
  assert.ok(cached.events.some((event) => event.eventType === 'CachedResultLinked'));
  replay(t, f);
  const sourcePath = path.join(f.dir, 'selected-reuse.sqlite');
  await f.db.backup(sourcePath);
  const sealed = new Database(sourcePath);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  await verifyAndInstallRelationalStorage(f.db, {
    sourcePath,
    sourceSha256: relationalSnapshotSha256(sourcePath),
    historyId: 'generation-history',
    finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get()
      .seq,
    replicas: { dataDir: f.dir },
  });
  assertRelationalStorageReady(f.db);
});

test('缓存保留来访已知展示而不改原产物输入，免费上限满时不重记成功或收费', async (t) => {
  const f = await setup(t, { freeLimit: 1, v5: '0', fixed: '0' }),
    req = f.request({ steps: 23 });
  await f.acceptCachedSource('source', req);
  f.dispatch('source-0');
  await f.publish('source-0');
  const origin = f.db.prepare('SELECT * FROM artifacts').get(),
    before = f.db
      .prepare('SELECT COUNT(*) AS n FROM generation_jobs WHERE free_succeeded_at IS NOT NULL')
      .get().n;
  assert.equal(before, 1);
  const visiting = { ...req, requestedSteps: 999 };
  await f.reuse('display', 'source-0', visiting, f.context(), 'fixed', origin.id);
  const current = f.db
    .prepare("SELECT input_id FROM generation_requests WHERE id='display'")
    .get().input_id;
  assert.notEqual(current, origin.input_id);
  assert.equal(
    JSON.parse(f.db.prepare('SELECT body FROM generation_inputs WHERE id=?').get(current).body)
      .requestedSteps,
    999,
  );
  assert.equal(f.inputs.readJobInput('display-0').requestedSteps, 999);
  assert.deepEqual(f.db.prepare('SELECT * FROM artifacts').get(), origin);
  assert.equal(
    f.db
      .prepare('SELECT COUNT(*) AS n FROM generation_jobs WHERE free_succeeded_at IS NOT NULL')
      .get().n,
    before,
  );
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_attempts').get().n, 1);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM charges').get().n, 1);
  replay(t, f);
});

function rewrittenSource(t, f, seq, mutate) {
  const source = new Database(f.db.serialize());
  t.after(() => source.close());
  source.pragma('foreign_keys=ON');
  const stored = readRelationalCommit(source, seq);
  source.pragma('foreign_keys=OFF');
  const guards = source
    .prepare(
      "SELECT name,sql FROM sqlite_schema WHERE type='trigger' AND tbl_name IN ('domain_events','commits')",
    )
    .all();
  for (const guard of guards) {
    source.exec(`DROP TRIGGER ${guard.name}`);
  }
  mutate(stored);
  source.prepare('DELETE FROM domain_events WHERE commit_seq=?').run(BigInt(seq));
  const versions = new Map();
  for (const event of stored.events) {
    if (!versions.has(event.streamId)) {
      const prior = source
        .prepare(
          'SELECT CAST(MAX(stream_version) AS TEXT) AS version FROM domain_events WHERE stream_id=? AND commit_seq<?',
        )
        .get(event.streamId, BigInt(seq)).version;
      versions.set(event.streamId, BigInt(prior ?? '0'));
    }
  }
  const insert = source.prepare(
    `INSERT INTO domain_events(event_id,commit_seq,event_index,stream_id,stream_kind,stream_version,event_type,schema_version,occurred_at,correlation_id,causation_id,account_id,member_id,policy_version,pricing_version,amount_version,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
  );
  stored.events.forEach((event, index) => {
    versions.set(event.streamId, versions.get(event.streamId) + 1n);
    insert.run(
      event.eventId,
      BigInt(seq),
      index,
      event.streamId,
      event.streamKind,
      versions.get(event.streamId),
      event.eventType,
      event.schemaVersion,
      event.occurredAt,
      event.correlationId,
      event.causationId,
      event.accountId,
      event.memberId,
      event.policyVersion,
      event.pricingVersion,
      event.amountVersion,
      JSON.stringify(event.payload),
    );
  });
  source
    .prepare('UPDATE commits SET fingerprint=?,event_count=?,receipt=? WHERE commit_seq=?')
    .run(
      stored.command.fingerprint,
      stored.events.length,
      JSON.stringify(stored.command.receipt),
      BigInt(seq),
    );
  source.exec(`UPDATE generation_jobs SET source_event_id=(
    SELECT event_id FROM domain_events WHERE event_type='JobAccepted' AND json_extract(payload,'$.jobId')=generation_jobs.id LIMIT 1
  ) WHERE source_event_id NOT IN (SELECT event_id FROM domain_events)`);
  for (const row of source.prepare('SELECT stream_id FROM stream_heads').all()) {
    const last = source
      .prepare(
        'SELECT event_id,commit_seq,stream_version FROM domain_events WHERE stream_id=? ORDER BY stream_version DESC LIMIT 1',
      )
      .get(row.stream_id);
    source
      .prepare(
        'UPDATE stream_heads SET stream_version=?,last_commit_seq=?,last_event_id=? WHERE stream_id=?',
      )
      .run(last.stream_version, last.commit_seq, last.event_id, row.stream_id);
  }
  for (const guard of guards) {
    source.exec(guard.sql);
  }
  source.pragma('foreign_keys=ON');
  assert.deepEqual(source.pragma('foreign_key_check'), []);
  return source;
}

function rejectMutatedCommit(t, f, source, badSeq) {
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replayer = new RelationalCommitReplayer(target);
  for (const { seq } of source
    .prepare(
      'SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE commit_seq<=? ORDER BY commit_seq',
    )
    .all(BigInt(badSeq))) {
    const stored = readRelationalCommit(source, seq);
    if (seq !== badSeq) {
      target.transaction(() => replayer.replayCommit(stored, new RelationalInputStore(source)))();
    } else {
      const before = databaseState(target);
      target.transaction(() =>
        assert.throws(
          () => replayer.replayCommit(stored, new RelationalInputStore(source)),
          /缓存|原输入|任务不存在|活动|受理|context|source/i,
        ),
      )();
      assert.equal(databaseState(target), before);
      assert.equal(
        target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), f.inputs))()
          .replayed,
        false,
      );
      const once = databaseState(target);
      assert.equal(
        target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), f.inputs))()
          .replayed,
        true,
      );
      assert.equal(databaseState(target), once);
    }
  }
}

test('两个合法事件列表/完整header一致仍拒绝alias伪意图与缓存缺换重排，失败全回滚且原提交可重试', async (t) => {
  const f = await setup(t),
    random = f.request({ seed: 777 });
  const produced = await f.acceptCachedSource('source', random, f.context(), 'random');
  const aliased = await f.reuse('alias', 'source-0', random, f.context(), 'random');
  f.dispatch('source-0');
  await f.publish('source-0');
  const artifact = f.db.prepare('SELECT id FROM artifacts').get().id;
  await f.acceptCachedSource('other', f.request({ tag: 'different output', seed: 777 }));
  const secondAt = f.db.prepare('SELECT cooldown_until AS at FROM upstream_accounts').get().at + 1;
  assert.ok(f.dispatch('other-0', secondAt).dispatch);
  await f.publish('other-0', secondAt + 1);
  const otherArtifact = f.db.prepare('SELECT id FROM artifacts WHERE id!=?').get(artifact).id;
  const cached = await f.reuse(
    'cached',
    'source-0',
    { ...random, requestedSteps: 999 },
    f.context(),
    'random',
    artifact,
  );
  const variants = [
    [
      'source-cache-key-with-unchanged-protected-body',
      produced.commitSeq,
      (stored) => {
        stored.events.find((event) => event.eventType === 'JobAccepted').payload.cacheKey =
          'f'.repeat(64);
      },
    ],
    [
      'alias-fingerprint',
      aliased.commitSeq,
      (stored) => {
        const fingerprint = f.inputs.generationInputIntent(
          'm1',
          f.request({ tag: 'forged prompt' }),
          f.context(),
          'random',
          false,
        ).fingerprint;
        stored.command.fingerprint = fingerprint;
        stored.events.find(
          (event) => event.eventType === 'RequestAliasRegistered',
        ).payload.intentFingerprint = fingerprint;
      },
    ],
    [
      'alias-event-time-not-original-command',
      aliased.commitSeq,
      (stored) => {
        stored.events.find((event) => event.eventType === 'RequestAliasRegistered').occurredAt--;
      },
    ],
    [
      'alias-fixed-same-actual-seed',
      aliased.commitSeq,
      (stored) => {
        const alias = stored.events.find((event) => event.eventType === 'RequestAliasRegistered');
        alias.payload.seedMode = 'fixed';
        alias.payload.intentFingerprint = f.inputs.generationInputIntent(
          'm1',
          random,
          f.context(),
          'fixed',
          false,
        ).fingerprint;
        stored.command.fingerprint = alias.payload.intentFingerprint;
      },
    ],
    [
      'cache-missing-link',
      cached.commitSeq,
      (stored) => {
        stored.events = stored.events.filter((event) => event.eventType !== 'CachedResultLinked');
      },
    ],
    [
      'cache-downgrade-done-without-link',
      cached.commitSeq,
      (stored) => {
        stored.events = stored.events.filter((event) => event.eventType !== 'CachedResultLinked');
        stored.events.find((event) => event.eventType === 'JobAccepted').payload.status = 'pending';
      },
    ],
    [
      'cache-pending-with-link',
      cached.commitSeq,
      (stored) => {
        stored.events.find((event) => event.eventType === 'JobAccepted').payload.status = 'pending';
      },
    ],
    [
      'cache-duplicate-link',
      cached.commitSeq,
      (stored) => {
        const link = structuredClone(
          stored.events.find((event) => event.eventType === 'CachedResultLinked'),
        );
        link.eventId += '-duplicate';
        stored.events.push(link);
      },
    ],
    [
      'cache-swapped-artifact',
      cached.commitSeq,
      (stored) => {
        stored.events.find((event) => event.eventType === 'CachedResultLinked').payload.artifactId =
          otherArtifact;
      },
    ],
    [
      'cache-link-before-acceptance',
      cached.commitSeq,
      (stored) => {
        const link = stored.events.find((event) => event.eventType === 'CachedResultLinked');
        stored.events = [link, ...stored.events.filter((event) => event !== link)];
      },
    ],
  ];
  for (const [name, seq, mutate] of variants) {
    await t.test(name, (t) => rejectMutatedCommit(t, f, rewrittenSource(t, f, seq, mutate), seq));
  }
});

test('停止/恢复只拦新alias；退休旧键410但未退休原产物仍能被新小job合法引用', async (t) => {
  const env = { OWNER_USER_ID: 'm1' },
    f = await setup(t, { env });
  await f.acceptCachedSource('active');
  const alias = await f.reuse('alias', 'active-0');
  const owner = new RelationalMemberStore(f.db, env);
  const admission = (acceptingJobs) =>
    withAuthenticatedMemberApiKey(
      f.members,
      keys.m1,
      (actor) =>
        owner.changeMemberAdmission(
          meta(`admission-${acceptingJobs}`, NOW + 1, 'm1'),
          { memberId: 'm1', acceptingJobs },
          actor,
        ),
      env,
    );
  await admission(false);
  const paused = databaseState(f.db);
  assert.equal((await f.reuse('alias', 'active-0')).replayed, true);
  await assert.rejects(f.reuse('new-alias', 'active-0'), (error) =>
    [403, 409].includes(error.statusCode),
  );
  assert.equal(databaseState(f.db), paused);
  await admission(true);
  assert.deepEqual((await f.reuse('alias', 'active-0')).receipt, alias.receipt);
  assert.equal(
    f.db.prepare("SELECT status FROM generation_jobs WHERE id='active-0'").get().status,
    'failed',
  );
  await f.acceptCachedSource('published');
  f.dispatch('published-0');
  await f.publish('published-0');
  const artifact = f.db.prepare('SELECT id FROM artifacts').get().id,
    origin = f.db.prepare('SELECT * FROM artifacts').get(),
    generations = new RelationalGenerationStore(f.db, env);
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    (actor) =>
      generations.retireJobHistory(
        meta('retire-source', NOW + 10, 'm1'),
        ['published-0'],
        actor,
        new Map(),
      ),
    env,
  );
  const mapping = f.inputs.readGenerationIdempotency(
    'm1',
    'generate',
    generationIdempotencyKeyHash('producer-published'),
  );
  await assert.rejects(
    f.authorize(() =>
      f.generations.readAcceptedGenerationReceipt(
        f.command('accept-published', f.request(), f.context(), 'fixed', true),
        mapping,
      ),
    ),
    (error) => error.statusCode === 410,
  );
  await f.reuse('after-retired-source', 'published-0', f.request(), f.context(), 'fixed', artifact);
  assert.deepEqual(f.db.prepare('SELECT * FROM artifacts').get(), origin);
  assert.equal(
    f.db.prepare("SELECT status FROM generation_jobs WHERE id='after-retired-source-0'").get()
      .status,
    'done',
  );
  replay(t, f);
});

test('原任务仍running也不能在停止接单后伪造完整新alias提交，原alias回执保持', async (t) => {
  const env = { OWNER_USER_ID: 'm1' },
    f = await setup(t, { env });
  await f.acceptCachedSource('running');
  f.dispatch('running-0');
  const original = await f.reuse('before-pause', 'running-0');
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    (actor) =>
      new RelationalMemberStore(f.db, env).changeMemberAdmission(
        meta('pause-running', NOW + 1, 'm1'),
        { memberId: 'm1', acceptingJobs: false },
        actor,
      ),
    env,
  );
  assert.equal(
    f.db.prepare("SELECT status FROM generation_jobs WHERE id='running-0'").get().status,
    'running',
  );
  const alias = original.events.find((event) => event.eventType === 'RequestAliasRegistered'),
    command = {
      ...f.command('forged-new-alias', f.request(), f.context(), 'fixed', false),
      recordedAt: NOW + 2,
    },
    before = databaseState(f.db);
  await f.authorize(() => {
    assert.throws(
      () =>
        new RelationalCommitReplayer(f.db).appendCommit(
          {
            ...command,
            expectedVersions: {
              [alias.streamId]: f.db
                .prepare(
                  'SELECT CAST(stream_version AS TEXT) AS v FROM stream_heads WHERE stream_id=?',
                )
                .get(alias.streamId).v,
            },
            receipt: original.receipt,
            events: [
              {
                eventType: 'RequestAliasRegistered',
                streamId: alias.streamId,
                accountId: null,
                memberId: 'm1',
                occurredAt: NOW + 2,
                policyVersion: command.policyVersion,
                pricingVersion: command.pricingVersion,
                payload: {
                  ...alias.payload,
                  keyHash: generationIdempotencyKeyHash('forged-after-pause'),
                  originalCommandId: command.commandId,
                },
              },
            ],
          },
          f.inputs,
        ),
      /停止|接单/,
    );
    return true;
  });
  assert.equal(databaseState(f.db), before);
  assert.equal((await f.reuse('before-pause', 'running-0')).replayed, true);
  assert.equal(databaseState(f.db), before);
  replay(t, f);
});

test('caller显式活动选择和noCache保持原差异；新键冲突/跨成员拒绝，当前Key轮换后原键仍零写领取', async (t) => {
  const f = await setup(t);
  await f.acceptCachedSource('source');
  const lookup = {
    memberId: 'm1',
    request: f.request(),
    context: f.context(),
    seedMode: 'fixed',
    noCache: false,
  };
  assert.equal(
    await f.generations.findReusableGeneration(
      { ...lookup, allowActive: false },
      { dataDir: f.dir },
    ),
    null,
  );
  assert.equal(
    await f.generations.findReusableGeneration(
      { ...lookup, allowActive: true, noCache: true },
      { dataDir: f.dir },
    ),
    null,
  );
  assert.deepEqual(
    await f.generations.findReusableGeneration(
      { ...lookup, allowActive: true },
      { dataDir: f.dir },
    ),
    { kind: 'active', sourceJobId: 'source-0' },
  );
  const first = await f.reuse('alias', 'source-0'),
    before = databaseState(f.db);
  await assert.rejects(
    f.reuse('alias', 'source-0', f.request({ tag: 'different' })),
    (error) => error.statusCode === 409,
  );
  await assert.rejects(
    f.reuse('foreign', 'source-0', f.request(), f.context(), 'fixed', undefined, 'm2'),
  );
  assert.equal(databaseState(f.db), before);
  await f.authorize(() => f.members.replaceMemberApiKey('m1', 'new-member-key'));
  const changed = databaseState(f.db);
  assert.equal(await f.reuse('alias', 'source-0'), null);
  assert.equal(databaseState(f.db), changed);
  const again = await f.authorize(
    () => f.generations.reuseGeneration(...f.reuseArgs('alias', 'source-0')),
    'm1',
    'new-member-key',
  );
  assert.equal(again.replayed, true);
  assert.deepEqual(again.receipt, first.receipt);
  assert.equal(databaseState(f.db), changed);
});

test('安全非空坏Local仍沿引用资格，真缺失无持久来源新键miss而原键不新执行', async (t) => {
  const f = await setup(t);
  await f.acceptCachedSource('source');
  f.dispatch('source-0');
  await f.publish('source-0');
  const artifact = f.db.prepare('SELECT id FROM artifacts').get().id,
    file = path.join(f.dir, 'images', `${artifact}.webp`),
    lookup = {
      memberId: 'm1',
      request: f.request(),
      context: f.context(),
      seedMode: 'fixed',
      noCache: false,
      allowActive: false,
    };
  await writeFile(file, Buffer.from('bad but nonempty Local'));
  assert.equal(
    (await f.generations.findReusableGeneration(lookup, { dataDir: f.dir })).artifactId,
    artifact,
  );
  const cached = await f.reuse('cached', 'source-0', f.request(), f.context(), 'fixed', artifact);
  await assert.rejects(readRelationalArtifactContent(f.db, artifact, { dataDir: f.dir }), {
    statusCode: 503,
  });
  await rename(file, file + '.missing');
  assert.equal(await f.generations.findReusableGeneration(lookup, { dataDir: f.dir }), null);
  const once = databaseState(f.db);
  assert.equal(
    (await f.reuse('cached', 'source-0', f.request(), f.context(), 'fixed', artifact)).replayed,
    true,
  );
  assert.equal(databaseState(f.db), once);
  assert.equal(cached.receipt.cacheHit, true);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_attempts').get().n, 1);
});

test('已确认冷来源缺部署配置仍可缓存引用，受理不GET且实际领取503不重新执行', async (t) => {
  const f = await setup(t);
  await f.acceptCachedSource('source');
  f.dispatch('source-0');
  await f.publish('source-0');
  const artifact = f.db.prepare('SELECT id FROM artifacts').get().id,
    r2 = new R2Storage({
      accountId: 'a'.repeat(32),
      bucket: 'reuse-images',
      accessKeyId: 'synthetic-access',
      secretAccessKey: 'synthetic-secret',
    });
  const put = t.mock.method(r2, 'put', async () => {}),
    get = t.mock.method(r2, 'get', async () => Buffer.from(image));
  await copyRelationalArtifactReplica(
    f.db,
    artifact,
    'r2',
    { dataDir: f.dir, r2, idle: () => true },
    () => NOW + 30000,
  );
  assert.equal(put.mock.callCount(), 1);
  const readCount = get.mock.callCount();
  await rename(
    path.join(f.dir, 'images', `${artifact}.webp`),
    path.join(f.dir, 'images', `${artifact}.saved`),
  );
  const before = databaseState(f.db);
  const candidate = await f.generations.findReusableGeneration(
    {
      memberId: 'm1',
      request: f.request(),
      context: f.context(),
      seedMode: 'fixed',
      noCache: false,
      allowActive: false,
    },
    { dataDir: f.dir },
  );
  assert.equal(candidate.artifactId, artifact);
  assert.equal(databaseState(f.db), before);
  await f.reuse('cold', candidate.sourceJobId, f.request(), f.context(), 'fixed', artifact);
  const once = databaseState(f.db);
  await assert.rejects(readRelationalArtifactContent(f.db, artifact, { dataDir: f.dir }), {
    statusCode: 503,
  });
  assert.equal(databaseState(f.db), once);
  assert.equal(get.mock.callCount(), readCount);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_attempts').get().n, 1);
  replay(t, f);
});

test('别名和缓存后续SQL投影故障被当前认证外层捕获，保护内容/恢复/事实/回执全回滚', async (t) => {
  const f = await setup(t);
  await f.acceptCachedSource('source');
  f.db.exec(
    "CREATE TEMP TRIGGER fail_alias BEFORE INSERT ON idempotency_keys WHEN NEW.key_hash!='' BEGIN SELECT RAISE(ABORT,'alias fault'); END",
  );
  let before = databaseState(f.db);
  await f.authorize(() => {
    assert.throws(
      () =>
        f.generations.reuseGeneration(
          ...f.reuseArgs(
            'alias',
            'source-0',
            f.request(),
            f.context({ defaultArtist: 'uncommitted' }),
          ),
        ),
      /alias fault/,
    );
    return true;
  });
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER fail_alias');
  await f.reuse('alias', 'source-0', f.request(), f.context({ defaultArtist: 'uncommitted' }));
  f.dispatch('source-0');
  await f.publish('source-0');
  const artifact = f.db.prepare('SELECT id FROM artifacts').get().id;
  f.db.exec(
    "CREATE TEMP TRIGGER fail_cache BEFORE UPDATE OF artifact_id ON generation_jobs WHEN NEW.id='cached-0' BEGIN SELECT RAISE(ABORT,'cache fault'); END",
  );
  before = databaseState(f.db);
  const args = f.reuseArgs(
    'cached',
    'source-0',
    { ...f.request(), requestedSteps: 999 },
    f.context(),
    'fixed',
    artifact,
  );
  await f.authorize(() => {
    assert.throws(() => f.generations.reuseGeneration(...args), /cache fault/);
    return true;
  });
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER fail_cache');
  assert.equal((await f.authorize(() => f.generations.reuseGeneration(...args))).replayed, false);
  replay(t, f);
});

test('direct缓存从唯一正文及最小原选择精确复算旧key，保留空词、多行、cost JSON值和purchase字段缺省', async (t) => {
  const f = await setup(t),
    ctx = f.context();
  const cases = [
    {},
    { artist: '', tag: '' },
    { artist: 'same', tag: 'same' },
    { artist: 'first\nsecond', tag: '' },
    { artist: '', tag: 'first\nsecond' },
    { artist: 'first', tag: 'second' },
    { artist: 'first\nsecond', tag: 'third\nfourth' },
    { size: '横图' },
    { size: ' ' },
    { cost: '' },
    { cost: null },
    { cost: false },
    { cost: 0 },
    { cost: -1 },
    { cost: 'invalid' },
    { cost: Infinity },
    { sampler: 'unsupported-original-sampler' },
  ];
  for (const changes of cases) {
    for (const purchase of [undefined, false, true]) {
      const normalized = f.request(changes),
        original = {
          ...normalized,
          ...(purchase === undefined ? {} : { allowPurchasedAnlas: purchase }),
        },
        request = {
          ...original,
          normalizationContext: { native: ctx.native },
          action: 'generate',
          operation: 'generate',
        };
      for (const seedMode of ['fixed', 'random']) {
        const expected = requestCacheKey(
            'm1',
            original,
            seedMode === 'random' ? '' : original.seed,
          ),
          actual = f.inputs.generationInputIntent('m1', request, ctx, seedMode, false);
        assert.equal(actual.cacheKey, expected, JSON.stringify({ changes, purchase, seedMode }));
      }
      const prepared = prepareRelationalInput(request, ctx);
      assert.equal(Object.hasOwn(prepared.body, 'artist'), false);
      assert.equal(Object.hasOwn(prepared.body, 'tag'), false);
      assert.equal(prepared.body.cacheSelection.purchaseField, purchase !== undefined);
      assert.equal(
        Object.hasOwn(prepared.body.cacheSelection, 'cost'),
        normalized.cost !== undefined,
      );
      assert.equal(
        Object.hasOwn(prepared.body.cacheSelection, 'sampler'),
        normalized.sampler !== prepared.body.parameters.sampler,
      );
    }
  }
  const plain = f.request(),
    extraNativeParameter = {
      ...plain,
      nativeParameters: { ...buildNovelAiParameters(plain), sm: true },
    };
  assert.equal(
    f.inputs.generationInputIntent('m1', extraNativeParameter, ctx, 'fixed', false).cacheKey,
    null,
  );
});

test('同出站但旧缓存key不同的原选择不能合并活动请求或缓存；相同旧key仍复用并完整回放', async (t) => {
  const pairs = [
    ['size', {}, { size: '横图' }],
    ['artist-tag-split', { artist: 'first', tag: 'second' }, { artist: '', tag: 'first\nsecond' }],
    ['cost-presence', {}, { cost: 0 }],
    [
      'sampler-rewrite',
      { sampler: 'unsupported-original-sampler' },
      { sampler: 'k_euler_ancestral' },
    ],
    ['purchase-presence', {}, {}],
  ];
  for (const [label, first, second] of pairs) {
    await t.test(label, async (t) => {
      const f = await setup(t),
        ctx = f.context(),
        a = f.request(first),
        b = {
          ...f.request(second),
          ...(label === 'purchase-presence' ? { allowPurchasedAnlas: false } : {}),
        };
      assert.equal(generationRequestFingerprint(a), generationRequestFingerprint(b));
      assert.notEqual(requestCacheKey('m1', a, a.seed), requestCacheKey('m1', b, b.seed));
      await f.acceptCachedSource('source', a, ctx);
      const lookup = (request) =>
        f.generations.findReusableGeneration(
          {
            memberId: 'm1',
            request,
            context: ctx,
            seedMode: 'fixed',
            noCache: false,
            allowActive: true,
          },
          { dataDir: f.dir },
        );
      const before = databaseState(f.db);
      assert.equal(await lookup(b), null);
      await assert.rejects(f.reuse('wrong-active', 'source-0', b, ctx), /意图|来源|复用|输入/);
      assert.equal(databaseState(f.db), before);
      const equivalent = {
        ...a,
        requestedSteps: 999,
        normalizationContext: { native: ctx.native },
      };
      assert.deepEqual(await lookup(equivalent), { kind: 'active', sourceJobId: 'source-0' });
      const alias = await f.reuse('same-active', 'source-0', equivalent, ctx);
      assert.equal(alias.receipt.requestId, 'source');
      f.dispatch('source-0');
      await f.publish('source-0');
      const artifact = f.db.prepare('SELECT id FROM artifacts').get().id,
        afterPublish = databaseState(f.db);
      assert.equal(await lookup(b), null);
      await assert.rejects(
        f.reuse('wrong-cache', 'source-0', b, ctx, 'fixed', artifact),
        /意图|来源|复用|输入/,
      );
      assert.equal(databaseState(f.db), afterPublish);
      assert.deepEqual(await lookup(equivalent), {
        kind: 'artifact',
        sourceJobId: 'source-0',
        artifactId: artifact,
      });
      const hit = await f.reuse('same-cache', 'source-0', equivalent, ctx, 'fixed', artifact);
      assert.equal(hit.receipt.cacheHit, true);
      assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_attempts').get().n, 1);
      assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM charges').get().n, 1);
      assert.equal(
        Object.hasOwn(f.inputs.readMemberInput('same-cache', 'm1').input, 'cacheSelection'),
        false,
      );
      replay(t, f);
    });
  }
});

test('缓存owner先验证真实来访fixed seed，不能用原产物seed替换后让错误metadata背书；random仍引用原实际seed', async (t) => {
  const f = await setup(t),
    ctx = f.context();
  await f.acceptCachedSource('source');
  f.dispatch('source-0');
  await f.publish('source-0');
  const artifact = f.db
      .prepare('SELECT artifact_id AS id FROM generation_jobs WHERE id=?')
      .get('source-0').id,
    args = f.reuseArgs('forged-fixed-seed', 'source-0', f.request(), ctx, 'fixed', artifact),
    before = databaseState(f.db);
  args[1].request = f.request({ seed: 43 });
  await assert.rejects(
    f.authorize(() => f.generations.reuseGeneration(...args)),
    /意图|来源|复用|输入/,
  );
  assert.equal(databaseState(f.db), before);
  const good = await f.reuse('valid-fixed-seed', 'source-0', f.request(), ctx, 'fixed', artifact);
  assert.equal(good.receipt.cacheHit, true);

  await f.acceptCachedSource('random-source', f.request(), ctx, 'random');
  f.dispatch('random-source-0', NOW + 20000);
  await f.publish('random-source-0');
  const randomArtifact = f.db
    .prepare('SELECT artifact_id AS id FROM generation_jobs WHERE id=?')
    .get('random-source-0').id;
  const randomIncoming = f.request({ seed: 777 }),
    incomingBytes = prepareRelationalInput(randomIncoming, ctx).logicalBytes,
    sourceBytes = f.db
      .prepare(
        'SELECT i.logical_bytes AS n FROM generation_inputs i JOIN generation_requests r ON r.input_id=i.id WHERE r.id=?',
      )
      .get('random-source').n;
  assert.ok(incomingBytes > sourceBytes);
  const random = await f.reuse(
    'valid-random-seed',
    'random-source-0',
    randomIncoming,
    ctx,
    'random',
    randomArtifact,
  );
  assert.equal(random.receipt.cacheHit, true);
  assert.equal(
    f.db
      .prepare('SELECT actual_seed AS seed FROM generation_jobs WHERE id=?')
      .get('valid-random-seed-0').seed,
    42,
  );
  assert.equal(
    f.db
      .prepare(
        'SELECT i.logical_bytes AS n FROM generation_inputs i JOIN generation_requests r ON r.input_id=i.id WHERE r.id=?',
      )
      .get('valid-random-seed').n,
    incomingBytes,
  );
  replay(t, f);
});
