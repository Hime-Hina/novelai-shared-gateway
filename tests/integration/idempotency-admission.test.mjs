import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { mkdtemp, readFile, stat } from 'node:fs/promises';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { JsonStore, createId, hashObject } from '../../server/store.js';
import { normalizeNovelAiRequest, MAX_STEPS, DIRECT_URL_MAX_STEPS } from '../../server/providers.js';
import * as fair from '../../server/fair-quota.js';
import * as queue from '../../server/queue-policy.ts';
import * as requests from '../../server/generation-request.js';
import { generationRequestFingerprint, requestNormalizationDefaults } from '../../server/request-fingerprint.ts';
import { parseNativeImageRequest } from '../../server/native-image.ts';

const source = await readFile(new URL('../../server/gateway-runtime.js', import.meta.url), 'utf8');
const png = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=';
function load(context, names) {
  const ctx = vm.createContext(context);
  for (const name of names) {
    const start = source.indexOf(`function ${name}(`), end = source.indexOf('\n}', start) + 2;
    vm.runInContext((source.slice(start - 6, start) === 'async ' ? 'async ' : '') + source.slice(start, end), ctx);
  }
  return ctx;
}
async function fixture(t) {
  const store = new JsonStore(await mkdtemp(path.join(tmpdir(), 'nai-idempotency-admission-')));
  await store.init();
  t.after(() => store.close());
  await store.update(db => {
    db.users = ['a', 'b', 'c', 'd'].map(id => ({ id, token: `STA1N-${id}`, enabled: true, balance: 0 }));
    db.accounts = [{ id: 'opus', enabled: true, quotaTier: 3, subscriptionActive: true,
      quotaFixed: 1000, quotaPurchased: 0, v5UsagePercent: 100, quotaCheckedAt: Date.now() }];
    db.settings.defaultArtist = 'first default artist';
    fair.configureFairQuota(db, ['a', 'b', 'c', 'd']); fair.syncFairQuota(db);
  }, { immediate: true });
  const update = store.update.bind(store);
  store.update = (fn, options) => update(fn, { ...options, immediate: true, includeSettings: true });
  const budget = requests.createGenerationPayloadBudget(() => store.activeGenerationPayloadBytes());
  const ctx = load({ store, Buffer, process, structuredClone, createId, hashObject, ...fair, ...queue, ...requests,
    generationRequestFingerprint, requestNormalizationDefaults, normalizeNovelAiRequest, MAX_STEPS, DIRECT_URL_MAX_STEPS,
    fairShareMode: true, generationPayloadBudget: budget, cleanupStaleActiveJobs: async () => {},
    activeJobCount: jobs => jobs.filter(job => ['queued', 'running'].includes(job.status)).length,
    stat, imageFilePath: file => path.join(store.dataDir, file),
    readStoredImage: image => image.file ? readFile(path.join(store.dataDir, image.file)) : Buffer.from(image.base64 || '', 'base64'),
    httpError: (statusCode, message) => Object.assign(new Error(message), { statusCode })
  }, ['readGenerationIdempotencyKey', 'generationIntent', 'unavailableGenerationResult', 'resolveGenerationIdempotency',
    'findCachedGenerationImage', 'validateGenerationReplay', 'assertFairMemberReady', 'generationRequestRecord',
    'getUserOrThrow', 'generationCost', 'isNoCache', 'cacheableRequest', 'requestCacheKey', 'uniqueIds',
    'batchJobIds', 'dirtyJobMutationRows', 'dirtyResultJobRows', 'createJob', 'createDirectJob', 'createNativeBatch']);
  const admit = (body, key = 'key-a', customClaim) => {
    const claim = customClaim || budget.claim();
    return ctx.createJob('STA1N-a', body, { generationPayloadClaim: claim, idempotencyKey: key }).finally(() => claim.release());
  };
  return { store, ctx, budget, admit };
}
const body = () => ({ prompt: 'synthetic prompt', model: 'nai-diffusion-4-5-full', steps: 23, width: 1024, height: 1024, nocache: true });

test('可选键只接受单个非空可见ASCII，128字节上限，缺键不改变旧流程', () => {
  const ctx = load({ httpError: (statusCode, message) => Object.assign(new Error(message), { statusCode }) }, ['readGenerationIdempotencyKey']);
  assert.equal(ctx.readGenerationIdempotencyKey({ headers: {} }), null);
  assert.equal(ctx.readGenerationIdempotencyKey({ headers: { 'idempotency-key': 'x'.repeat(128) } }), 'x'.repeat(128));
  for (const key of ['', 'a b', '中文', 'x'.repeat(129)]) {
    assert.throws(() => ctx.readGenerationIdempotencyKey({ headers: { 'idempotency-key': key } }), error => error.statusCode === 400);
  }
  assert.throws(() => ctx.readGenerationIdempotencyKey({ headers: { 'idempotency-key': 'one, two' },
    rawHeaders: ['Idempotency-Key', 'one', 'IDEMPOTENCY-KEY', 'two'] }), error => error.statusCode === 400);
});

test('同键保留首次随机seed与defaults；冲突409，暂停接单和共享账户不可用仍回放', async t => {
  const { store, admit } = await fixture(t);
  const first = await admit(body());
  const original = store.readGenerationRequest(first.requestId);
  await store.update(db => { db.settings.defaultArtist = 'new default artist'; db.users[0].acceptingJobs = false; db.accounts[0].enabled = false; },
    { collections: ['settings', 'users', 'accounts'] });
  const replay = await admit(body());
  assert.equal(replay.id, first.id);
  assert.equal(replay.replayed, true);
  assert.equal(store.readGenerationRequest(first.requestId).seed, original.seed);
  assert.equal(store.readGenerationRequest(first.requestId).artist, 'first default artist');
  await assert.rejects(admit({ ...body(), prompt: 'different request' }), error => error.statusCode === 409);
  assert.equal(store.countRecords('jobs'), 1);
});

test('两次preflight都未见mapping、默认值随后改变，最终串行受理按首context重算而不误报409', { timeout: 5000 }, async t => {
  const { store, ctx, admit } = await fixture(t);
  const gates = [];
  ctx.cleanupStaleActiveJobs = () => new Promise(resolve => gates.push(resolve));
  const first = admit(body()), second = admit(body());
  while (gates.length < 2) await new Promise(resolve => setImmediate(resolve));
  gates[0]();
  const accepted = await first;
  await store.update(db => { db.settings.defaultArtist = 'changed between submissions'; }, { collections: ['settings'] });
  gates[1]();
  const retried = await second;
  assert.equal(retried.id, accepted.id);
  assert.equal(store.readGenerationRequest(accepted.requestId).artist, 'first default artist');
  assert.equal(store.sqlite.prepare('SELECT COUNT(*) AS n FROM generation_idempotency').get().n, 1);
});

test('旧inline active cache只追加每个key自己的映射，首次随机意图不从target固定seed重算', async t => {
  const { store, ctx, admit } = await fixture(t);
  const input = { ...body(), nocache: false };
  const target = normalizeNovelAiRequest(input, store.db.settings, { maxSteps: DIRECT_URL_MAX_STEPS });
  target.allowPurchasedAnlas = false; target.seed = 777;
  await store.update(db => { db.jobs.push({ id: 'inline', status: 'queued', userToken: 'STA1N-a', request: target,
    cacheKey: ctx.requestCacheKey('STA1N-a', target, undefined), payloadBytes: Buffer.byteLength(JSON.stringify(target)), createdAt: new Date().toISOString() }); },
    { dirtyRows: { jobs: ['inline'] } });
  assert.equal((await admit(input, 'alias-a')).id, 'inline');
  assert.equal((await admit(input, 'alias-b')).id, 'inline');
  const mapped = store.readGenerationIdempotency('a', 'generate', hashObject('alias-a'));
  assert.equal(mapped.request_id, null);
  assert.notEqual(mapped.fingerprint, generationRequestFingerprint(target, { randomSeed: false, noCache: false }));
  assert.equal((await admit(input, 'alias-a')).id, 'inline');
  assert.equal(store.countRecords('jobs'), 1);
  assert.equal(store.sqlite.prepare('SELECT COUNT(*) AS n FROM generation_requests').get().n, 0);
});

test('首次completed cache仅绑定本人可读产物，缺失或错配回落cache miss且不绑定错误mapping', async t => {
  for (const variant of ['valid', 'foreign', 'missing']) {
    const { store, ctx, admit } = await fixture(t);
    const input = { ...body(), seed: 42, nocache: false };
    const normalized = normalizeNovelAiRequest(input, store.db.settings, { maxSteps: DIRECT_URL_MAX_STEPS });
    normalized.allowPurchasedAnlas = false;
    await store.update(db => { db.images.push({ id: 'cached', token: variant === 'foreign' ? 'STA1N-b' : 'STA1N-a',
      cacheKey: ctx.requestCacheKey('STA1N-a', normalized, 42), mimeType: 'image/png',
      ...(variant === 'missing' ? { file: 'absent.png' } : { base64: png }) }); }, { dirtyRows: { images: ['cached'] } });
    const accepted = await admit(input);
    const job = store.selectItemById('jobs', accepted.id);
    assert.equal(job.status, variant === 'valid' ? 'done' : 'queued');
    assert.equal(job.imageId, variant === 'valid' ? 'cached' : '');
    assert.equal(store.readGenerationIdempotency('a', 'generate', hashObject('key-a')).job_id, job.id);
  }
});

test('有限context与规范化payload合计占原临时预算，落盘只计执行输入；重试也不能绕过', async t => {
  const { store, ctx, admit } = await fixture(t);
  const input = { ...body(), seed: 42 };
  const normalized = normalizeNovelAiRequest(input, store.db.settings, { maxSteps: DIRECT_URL_MAX_STEPS });
  normalized.allowPurchasedAnlas = false;
  const bytes = Buffer.byteLength(JSON.stringify(normalized));
  const combined = bytes + Buffer.byteLength(JSON.stringify(requestNormalizationDefaults(store.db.settings)));
  const small = requests.createGenerationPayloadBudget(() => store.activeGenerationPayloadBytes(), { request: bytes, active: 100000 });
  await assert.rejects(admit(input, 'budget', small.claim()), error => error.statusCode === 413);
  assert.equal(store.countRecords('jobs'), 0);
  const exact = requests.createGenerationPayloadBudget(() => store.activeGenerationPayloadBytes(), { request: combined, active: 100000 });
  const accepted = await admit(input, 'budget', exact.claim());
  assert.equal(store.activeGenerationPayloadBytes(), bytes);
  await assert.rejects(admit(input, 'budget', small.claim()), error => error.statusCode === 413);
  assert.equal((await admit(input, 'budget', exact.claim())).id, accepted.id);
});

test('同key failed-late回放核对parent及旧inline产物归属，错误关联410且不重生', async t => {
  for (const inline of [false, true]) {
    const { store, ctx, admit } = await fixture(t);
    const input = { ...body(), nocache: !inline };
    if (inline) {
      const normalized = normalizeNovelAiRequest(input, store.db.settings, { maxSteps: DIRECT_URL_MAX_STEPS });
      normalized.allowPurchasedAnlas = false;
      await store.update(db => { db.jobs.push({ id: 'inline', status: 'queued', userToken: 'STA1N-a', request: normalized,
        cacheKey: ctx.requestCacheKey('STA1N-a', normalized, undefined), createdAt: new Date().toISOString() }); }, { dirtyRows: { jobs: ['inline'] } });
    }
    const accepted = await admit(input);
    await store.update(db => {
      const job = db.jobs.find(job => job.id === accepted.id);
      job.status = 'failed'; job.imageId = 'foreign'; job.lateResultAvailable = true;
      db.images.push({ id: 'foreign', token: 'STA1N-b', mimeType: 'image/png', base64: png });
    }, { dirtyRows: { jobs: [accepted.id], images: ['foreign'] } });
    await assert.rejects(admit(input), error => error.statusCode === 410 && error.requestId === (accepted.requestId || null));
    assert.equal(store.countRecords('jobs'), 1);
    assert.equal(store.selectItemById('jobs', accepted.id).status, 'failed');
  }
});

test('Native剩余child被清理时同键410保留parent ID，不把剩余图静默当完整批次', async t => {
  const { store, ctx, budget } = await fixture(t);
  const raw = { input: 'synthetic batch', model: 'nai-diffusion-4-5-full', parameters: { width: 1024, height: 1024, steps: 23, n_samples: 3 } };
  const submit = () => {
    const claim = budget.claim();
    const intent = ctx.generationIntent('native', () => parseNativeImageRequest(raw), { randomSeed: true, noCache: true, claim });
    return ctx.createNativeBatch('STA1N-a', null, 'synthetic', 0, claim, intent).finally(() => claim.release());
  };
  const accepted = await submit();
  await store.update(db => { db.jobs = db.jobs.filter(job => job.id !== accepted.jobIds[1]); }, { dirtyRows: { jobs: [accepted.jobIds[1]] } });
  await assert.rejects(submit(), error => error.statusCode === 410 && error.requestId === accepted.requestId);
  assert.equal(store.countRecords('jobs'), 2);
  assert.equal(store.readGenerationIdempotency('a', 'generate', hashObject('native')).request_id, accepted.requestId);
});
