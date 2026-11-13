import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { rename, writeFile } from 'node:fs/promises';
import { fixture, meta, NOW, keys, image, databaseState } from '../helpers/relational-generation.mjs';
import { normalizeNovelAiRequest, DIRECT_URL_MAX_STEPS } from '../../server/protocols/novelai-client.ts';
import { nativeNormalizationContext } from '../../server/protocols/native-image.ts';
import { requestNormalizationDefaults } from '../../server/generation/request-fingerprint.ts';
import { generationIdempotencyKeyHash } from '../../server/generation/admission.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { initializeRelationalStorage, assertRelationalStorageReady } from '../../server/storage/relational-storage.ts';
import { readRelationalArtifactContent } from '../../server/storage/relational-replica-io.ts';

async function setup(t, options = {}) {
  const f = await fixture(t, {seconds: 0, ...options});
  const context = (defaults = {}) => ({kind: 'direct', native: nativeNormalizationContext(), defaults: requestNormalizationDefaults(defaults)});
  const request = (changes = {}, captured = context()) => normalizeNovelAiRequest({
    tag: 'shared cache prompt', model: 'nai-diffusion-4-5-full', width: 128, height: 128, steps: 30, seed: 42, ...changes,
  }, captured.defaults, {maxSteps: DIRECT_URL_MAX_STEPS});
  const command = (id, req, ctx, seedMode, noCache, memberId = 'm1') => ({...meta(id, NOW, memberId),
    fingerprint: f.inputs.generationInputIntent(memberId, req, ctx, seedMode, noCache).fingerprint});
  const authorize = (run, memberId = 'm1', key = keys[memberId]) => withAuthenticatedMemberApiKey(f.members, key, () => run(), {});
  const accept = (id, req = request(), ctx = context(), seedMode = 'fixed') => authorize(() => {
    const intent = f.inputs.generationInputIntent('m1', req, ctx, seedMode, true);
    return f.generations.acceptGeneration(command(`accept-${id}`, req, ctx, seedMode, true), {
      memberId: 'm1', inputId: `input-${id}`, requestId: id, source: 'direct', request: req, context: ctx, seedMode, confirmation: null,
      idempotency: {keyHash: generationIdempotencyKeyHash(`producer-${id}`), noCache: true},
      jobs: [{jobId: `${id}-0`, status: 'pending', queueSeq: null, queueExpiresAt: null, deadlineAt: null, admissionEpoch: 0, cacheKey: intent.cacheKey}],
    });
  });
  const reuseArgs = (id, sourceJobId, req = request(), ctx = context(), seedMode = 'fixed', cached, memberId = 'm1') => [command(`reuse-${id}`, req, ctx, seedMode, false, memberId), {
      memberId, request: req, context: ctx, seedMode, noCache: false, sourceJobId,
      idempotency: {keyHash: generationIdempotencyKeyHash(id), noCache: false},
      ...(cached ? {cached: {requestId: id, jobId: `${id}-0`, inputId: `input-${id}`, source: 'direct', artifactId: cached, admissionEpoch: 0}} : {}),
    }];
  const reuse = (...args) => authorize(() => f.generations.reuseGeneration(...reuseArgs(...args)), args[6] ?? 'm1');
  return {...f, context, request, command, authorize, acceptCachedSource: accept, reuse, reuseArgs};
}

function replay(t, f) {
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replayer = new RelationalCommitReplayer(target), source = new RelationalInputStore(f.db),
    before = f.db.serialize(), previousFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = () => { calls++; throw new Error('replay must not fetch'); };
  try {
    const commits = f.db.prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq').all();
    for (const {seq} of commits) target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), source))();
    for (const table of ['generation_inputs', 'normalization_contexts', 'generation_requests', 'generation_jobs', 'generation_attempts', 'charges', 'artifacts', 'artifact_replicas', 'idempotency_keys', 'wallets']) {
      assert.deepEqual(target.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(), f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(), table);
    }
    const once = target.serialize();
    for (const {seq} of commits) assert.equal(target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), source))().replayed, true);
    assert.ok(target.serialize().equals(once));
    assert.ok(f.db.serialize().equals(before));
    for (const table of ['api_keys', 'member_sessions', 'upstream_credentials']) assert.equal(target.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0);
    assert.throws(() => assertRelationalStorageReady(target), /incomplete/);
  } finally { globalThis.fetch = previousFetch; }
  assert.equal(calls, 0);
  return target;
}

test('活动复用只保存每键自己的首次context/随机意图，不新建任务输入或费用并可完整回放', async t => {
  const f = await setup(t), sourceContext = f.context({defaultArtist: 'first'}),
    req = f.request({seed: undefined}, sourceContext);
  await f.acceptCachedSource('source', req, sourceContext, 'random');
  const before = ['generation_inputs', 'generation_requests', 'generation_jobs', 'generation_attempts', 'charges'].map(table => f.db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n);
  const aliasContext = f.context({defaultArtist: 'different saved defaults'});
  const a = await f.reuse('alias-a', 'source-0', req, aliasContext, 'random');
  const b = await f.reuse('alias-b', 'source-0', req, f.context(), 'random');
  assert.deepEqual(a.receipt, {requestId: 'source', jobIds: ['source-0']});
  assert.deepEqual(b.receipt, a.receipt);
  assert.deepEqual(['generation_inputs', 'generation_requests', 'generation_jobs', 'generation_attempts', 'charges'].map(table => f.db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n), before);
  assert.deepEqual(f.inputs.readGenerationIdempotencyContext('m1', 'generate', generationIdempotencyKeyHash('alias-a')), aliasContext);
  const once = databaseState(f.db);
  assert.equal((await f.reuse('alias-a', 'source-0', req, aliasContext, 'random')).replayed, true);
  assert.equal(databaseState(f.db), once);
  replay(t, f);
});

test('缓存小job引用原产物/input，不新attempt/charge/freecount且整体回放恢复', async t => {
  const f = await setup(t);
  await f.acceptCachedSource('source');
  f.dispatch('source-0');
  await f.publish('source-0');
  const artifact = f.db.prepare('SELECT id FROM artifacts').get().id,
    origin = f.db.prepare('SELECT * FROM artifacts').get(),
    before = ['generation_inputs', 'generation_attempts', 'charges'].map(table => f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all());
  const candidate = await f.generations.findReusableGeneration({memberId: 'm1', request: f.request(), context: f.context(), seedMode: 'fixed', noCache: false, allowActive: false}, {dataDir: f.dir});
  assert.equal(candidate.artifactId, artifact);
  const cached = await f.reuse('cached', candidate.sourceJobId, f.request(), f.context(), 'fixed', artifact);
  const job = f.db.prepare("SELECT * FROM generation_jobs WHERE id='cached-0'").get();
  assert.equal(job.status, 'done');
  assert.equal(job.artifact_id, artifact);
  assert.equal(job.queue_seq, null);
  assert.equal(job.free_succeeded_at, null);
  assert.equal(f.db.prepare("SELECT input_id FROM generation_requests WHERE id='cached'").get().input_id, origin.input_id);
  assert.deepEqual(f.db.prepare('SELECT * FROM artifacts').get(), origin);
  assert.deepEqual(['generation_inputs', 'generation_attempts', 'charges'].map(table => f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all()), before);
  assert.ok(cached.events.some(event => event.eventType === 'CachedResultLinked'));
  replay(t, f);
});

test('缓存保留来访已知展示而不改原产物输入，免费上限满时不重记成功或收费', async t => {
  const f = await setup(t, {freeLimit: 1}), req = f.request({steps: 23});
  await f.acceptCachedSource('source', req);
  f.dispatch('source-0');
  await f.publish('source-0');
  const origin = f.db.prepare('SELECT * FROM artifacts').get(),
    before = f.db.prepare('SELECT COUNT(*) AS n FROM generation_jobs WHERE free_succeeded_at IS NOT NULL').get().n;
  assert.equal(before, 1);
  const visiting = {...req, requestedSteps: 999};
  await f.reuse('display', 'source-0', visiting, f.context(), 'fixed', origin.id);
  const current = f.db.prepare("SELECT input_id FROM generation_requests WHERE id='display'").get().input_id;
  assert.notEqual(current, origin.input_id);
  assert.equal(JSON.parse(f.db.prepare('SELECT body FROM generation_inputs WHERE id=?').get(current).body).requestedSteps, 999);
  assert.equal(f.inputs.readJobInput('display-0').requestedSteps, 999);
  assert.deepEqual(f.db.prepare('SELECT * FROM artifacts').get(), origin);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_jobs WHERE free_succeeded_at IS NOT NULL').get().n, before);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_attempts').get().n, 1);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM charges').get().n, 1);
  replay(t, f);
});

test('caller显式活动选择和noCache保持原差异；新键冲突/跨成员拒绝，当前Key轮换后原键仍零写领取', async t => {
  const f = await setup(t);
  await f.acceptCachedSource('source');
  const lookup = {memberId: 'm1', request: f.request(), context: f.context(), seedMode: 'fixed', noCache: false};
  assert.equal(await f.generations.findReusableGeneration({...lookup, allowActive: false}, {dataDir: f.dir}), null);
  assert.equal(await f.generations.findReusableGeneration({...lookup, allowActive: true, noCache: true}, {dataDir: f.dir}), null);
  assert.deepEqual(await f.generations.findReusableGeneration({...lookup, allowActive: true}, {dataDir: f.dir}), {kind: 'active', sourceJobId: 'source-0'});
  const first = await f.reuse('alias', 'source-0'), before = databaseState(f.db);
  await assert.rejects(f.reuse('alias', 'source-0', f.request({tag: 'different'})), error => error.statusCode === 409);
  await assert.rejects(f.reuse('foreign', 'source-0', f.request(), f.context(), 'fixed', undefined, 'm2'));
  assert.equal(databaseState(f.db), before);
  await f.authorize(() => f.members.replaceMemberApiKey('m1', 'new-member-key'));
  const changed = databaseState(f.db);
  assert.equal(await f.reuse('alias', 'source-0'), null);
  assert.equal(databaseState(f.db), changed);
  const again = await f.authorize(() => f.generations.reuseGeneration(...f.reuseArgs('alias', 'source-0')), 'm1', 'new-member-key');
  assert.equal(again.replayed, true);
  assert.deepEqual(again.receipt, first.receipt);
  assert.equal(databaseState(f.db), changed);
});

test('安全非空坏Local仍沿引用资格，真缺失无持久来源新键miss而原键不新执行', async t => {
  const f = await setup(t);
  await f.acceptCachedSource('source');
  f.dispatch('source-0');
  await f.publish('source-0');
  const artifact = f.db.prepare('SELECT id FROM artifacts').get().id,
    file = path.join(f.dir, 'images', `${artifact}.webp`),
    lookup = {memberId: 'm1', request: f.request(), context: f.context(), seedMode: 'fixed', noCache: false, allowActive: false};
  await writeFile(file, Buffer.from('bad but nonempty Local'));
  assert.equal((await f.generations.findReusableGeneration(lookup, {dataDir: f.dir})).artifactId, artifact);
  const cached = await f.reuse('cached', 'source-0', f.request(), f.context(), 'fixed', artifact);
  await assert.rejects(readRelationalArtifactContent(f.db, artifact, {dataDir: f.dir}), {statusCode: 503});
  await rename(file, file + '.missing');
  assert.equal(await f.generations.findReusableGeneration(lookup, {dataDir: f.dir}), null);
  const once = databaseState(f.db);
  assert.equal((await f.reuse('cached', 'source-0', f.request(), f.context(), 'fixed', artifact)).replayed, true);
  assert.equal(databaseState(f.db), once);
  assert.equal(cached.receipt.cacheHit, true);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_attempts').get().n, 1);
});

test('别名和缓存后续SQL投影故障被当前认证外层捕获，保护内容/恢复/事实/回执全回滚', async t => {
  const f = await setup(t);
  await f.acceptCachedSource('source');
  f.db.exec("CREATE TEMP TRIGGER fail_alias BEFORE INSERT ON idempotency_keys WHEN NEW.key_hash!='' BEGIN SELECT RAISE(ABORT,'alias fault'); END");
  let before = databaseState(f.db);
  await f.authorize(() => { assert.throws(() => f.generations.reuseGeneration(...f.reuseArgs('alias', 'source-0', f.request(), f.context({defaultArtist: 'uncommitted'}))), /alias fault/); return true; });
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER fail_alias');
  await f.reuse('alias', 'source-0', f.request(), f.context({defaultArtist: 'uncommitted'}));
  f.dispatch('source-0');
  await f.publish('source-0');
  const artifact = f.db.prepare('SELECT id FROM artifacts').get().id;
  f.db.exec("CREATE TEMP TRIGGER fail_cache BEFORE UPDATE OF artifact_id ON generation_jobs WHEN NEW.id='cached-0' BEGIN SELECT RAISE(ABORT,'cache fault'); END");
  before = databaseState(f.db);
  const args = f.reuseArgs('cached', 'source-0', {...f.request(), requestedSteps: 999}, f.context(), 'fixed', artifact);
  await f.authorize(() => { assert.throws(() => f.generations.reuseGeneration(...args), /cache fault/); return true; });
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER fail_cache');
  assert.equal((await f.authorize(() => f.generations.reuseGeneration(...args))).replayed, false);
  replay(t, f);
});
