import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { createHash, randomFillSync } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { deflateSync } from 'node:zlib';
import { JsonStore, createId } from '../../server/storage/sqlite-store.ts';
import { createMemberSession, logoutMemberSession } from '../../server/members/authentication.ts';
import { handleMemberRoutes } from '../../server/members/http-routes.ts';
import { createMemberGenerationHttp, memberGenerationError, closeMemberGenerationStreams } from '../../server/generation/member-http.ts';
import { createGenerationAdmission } from '../../server/generation/admission.ts';
import { createGenerationPayloadBudget, singleGenerationRequest } from '../../server/generation/batch-requests.ts';
import { publicGenerationRequest, readOwnedGenerationRequest } from '../../server/generation/request-queries.ts';
import { recordGenerationFailure, recordCompletedArtifact, assertConfirmedDispatch } from '../../server/generation/execution.ts';
import { configureFairQuota, syncFairQuota, reserveFairQuota, settleFairQuota } from '../../server/quota/fair-share.ts';
import { recordRecoveredGeneration } from '../../server/generation/accounting.ts';
import { ImageStorage } from '../../server/storage/r2-storage.ts';
import { JobPreviews } from '../../server/generation/job-previews.ts';
import { crc32, BASE_MODELS } from '../../server/protocols/native-image.ts';
import { recordDomainEvent } from '../../server/events/domain-events.ts';

function png(width = 128, height = 128, noise = false) {
  const chunk = (name, body) => { const h = Buffer.alloc(8), t = Buffer.alloc(4); h.writeUInt32BE(body.length); h.write(name, 4); t.writeUInt32BE(crc32(Buffer.concat([Buffer.from(name), body]))); return Buffer.concat([h, body, t]); };
  const h = Buffer.alloc(13); h.writeUInt32BE(width); h.writeUInt32BE(height, 4); h[8] = 8; h[9] = 6;
  const rows = Buffer.alloc((width * 4 + 1) * height); if (noise) { randomFillSync(rows); for (let row = 0; row < height; row++) rows[row * (width * 4 + 1)] = 0; }
  return Buffer.concat([Buffer.from('89504e470d0a1a0a', 'hex'), chunk('IHDR', h), chunk('IDAT', deflateSync(rows)), chunk('IEND', Buffer.alloc(0))]);
}
const image = png(), image64 = image.toString('base64');
const zero = () => ({ v5: '0', anlasFixed: '0', anlasPurchased: '0' });
const input = (parameters = {}, root = {}) => ({ operation: 'generate', input: 'private synthetic prompt', model: 'nai-diffusion-4-5-full', action: 'generate', parameters: { width: 128, height: 128, steps: 23, seed: -1, ...parameters }, ...root });
const confirmation = quote => ({ requestFingerprint: quote.requestFingerprint, versions: quote.versions, maxPerJob: quote.charges.perJob[0].resources, maxTotal: quote.charges.total });
const unavailable = () => { throw new Error('This test must not call an external service or an unrelated route.'); };

async function fixture(t) {
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-member-generation-http-'));
  let store = new JsonStore(dataDir);
  await store.init();
  const env = { OWNER_USER_ID: 'a', DISCORD_GUILD_ID: '12345678901234567', DISCORD_CLIENT_ID: '22345678901234567', DISCORD_CLIENT_SECRET: 'synthetic-offline-client-secret', PUBLIC_BASE_URL: 'https://synthetic.invalid' };
  const seed=structuredClone(store.db);
  await (async db => {
    db.users = ['a', 'b', 'c', 'd'].map((id, index) => ({ id, token: `synthetic-key-${id}`, enabled: true, balance: 0,
      memberAuth: { username: id, version: `synthetic-auth-${id}` }, discord: { id: `3234567890123456${index}`, guildId: env.DISCORD_GUILD_ID, checkedAt: Date.now(), member: true } }));
    db.accounts = [{ id: 'opus-synthetic', token: 'upstream-secret-not-for-member', enabled: true, quotaTier: 3, subscriptionActive: true,
      quotaFixed: 10000, quotaPurchased: 100, v5UsagePercent: 80, v5UsageIsNegative: false, quotaCheckedAt: Date.now() }];
    configureFairQuota(db, ['a', 'b', 'c', 'd']); syncFairQuota(db);
  })(seed);
  await store.write(seed);
  await store.enableEventAuthority();
  const sessions = {};
  for (const user of store.db.users) sessions[user.id] = await createMemberSession(store, user, env);
  const budget = createGenerationPayloadBudget(() => store.activeGenerationPayloadBytes()), imageStorage = new ImageStorage(dataDir, null, id => store.findImage(id)), previews = new JobPreviews();
  let drainCalls = 0;
  const handleGeneration = () => createMemberGenerationHttp({ store,
    admit: createGenerationAdmission({ store, payloadBudget: budget, artifactAvailable: image => imageStorage.available(image), cleanupStaleActiveJobs: async () => {}, env }),
    budget, imageStorage, previews, scheduleQueueDrain() { drainCalls++; }, env });
  const deps = { store, env, fetch: unavailable, discordFetch: unavailable, assertAdmin: unavailable, createId, createPublicToken: unavailable, publicImageUrl: id => `/images/${id}`, ownerOperation: unavailable,
    changeMemberAdmission: unavailable, cancelGenerationRequest: unavailable,
    getGenerationRequest: async (id, memberId) => publicGenerationRequest(await readOwnedGenerationRequest(store, id, memberId)), handleMemberGeneration: handleGeneration() };
  const server = createServer(async (req, res) => {
    try { if (!await handleMemberRoutes(req, res, new URL(req.url, `http://${req.headers.host}`), deps)) { res.writeHead(404); res.end(); } }
    catch (error) { const body = memberGenerationError(error); res.writeHead(body.statusCode, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(JSON.stringify(body)); }
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const origin = `http://127.0.0.1:${server.address().port}`;
  const activeStreams = new Set();
  t.after(async () => {
    for (const stream of activeStreams) await stream.close();
    previews.closeSubscriptions(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); await store.close();
    const root = path.resolve(tmpdir()), target = path.resolve(dataDir);
    assert.equal(path.dirname(target), root); assert.ok(path.basename(target).startsWith('nai-member-generation-http-'));
    await rm(target, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  const raw = (pathname, { member = 'a', method = 'GET', body, key, headers = {}, ...options } = {}) => fetch(origin + pathname, {
    method, headers: { ...(member ? { cookie: `nai_member_session=${sessions[member].token}` } : {}),
      ...(method === 'POST' ? { origin, ...(body instanceof FormData ? {} : { 'content-type': 'application/json' }) } : {}), ...(key ? { 'idempotency-key': key } : {}), ...headers },
    ...(body !== undefined ? { body: typeof body === 'string' || body instanceof FormData ? body : JSON.stringify(body) } : {}), ...options });
  const json = async (pathname, options) => { const response = await raw(pathname, options); return { status: response.status, headers: response.headers, body: await response.json() }; };
  const quote = async (body = input(), member = 'a') => {
    const result = await json('/api/member/generation/quote', { member, method: 'POST', body }); assert.equal(result.status, 200, JSON.stringify(result.body)); return result.body;
  };
  const submit = async (body = input(), key = 'synthetic-key', member = 'a') => {
    const priced = await quote(body, member); return json('/api/member/generation/requests', { member, method: 'POST', key, body: { ...body, confirmation: confirmation(priced) } });
  };
  const openEvents = async requestId => {
    const abort = new AbortController(), response = await raw(`/api/member/requests/${requestId}/events`, { signal: abort.signal });
    assert.equal(response.status, 200); const reader = response.body.getReader(), decoder = new TextDecoder(), events = [], waiters = []; let pending = '', ended = false;
    const notify = item => { events.push(item); for (const waiter of [...waiters]) if (waiter.type === item.type) { waiters.splice(waiters.indexOf(waiter), 1); clearTimeout(waiter.timer); waiter.resolve(item); } };
    const pump = (async () => { try { while (true) {
      const next = await reader.read(); if (next.done) break; pending += decoder.decode(next.value, { stream: true });
      let boundary; while ((boundary = pending.indexOf('\n\n')) >= 0) { const frame = pending.slice(0, boundary); pending = pending.slice(boundary + 2);
        const type = frame.split('\n').find(line => line.startsWith('event: '))?.slice(7), data = frame.split('\n').find(line => line.startsWith('data: '))?.slice(6);
        if (type && data) notify({ type, data: JSON.parse(data) });
      }
    } } catch (error) { if (error.name !== 'AbortError') throw error; } finally { ended = true; } })();
    const stream = { response, events, done: pump, get ended() { return ended; },
      wait(type, timeout = 3000) { const found = events.find(event => event.type === type); if (found) return Promise.resolve(found);
        return new Promise((resolve, reject) => { const waiter = { type, resolve, timer: setTimeout(() => { waiters.splice(waiters.indexOf(waiter), 1); reject(new Error(`Missing SSE ${type}; received ${events.map(event => event.type)}`)); }, timeout) }; waiters.push(waiter); }); },
      async close() { abort.abort(); await pump; activeStreams.delete(stream); for (const waiter of waiters.splice(0)) clearTimeout(waiter.timer); } };
    activeStreams.add(stream); return stream;
  };
  const dispatch = async requestId => store.update(db => {
    const job = db.jobs.find(job => job.requestId === requestId), user = db.users.find(user => user.token === job.userToken), request = singleGenerationRequest(store.readGenerationRequest(requestId), job.sampleIndex);
    const now = Date.now();
    job.status = 'running'; job.accountId = db.accounts[0].id; job.startedAt = job.updatedAt = new Date(now).toISOString();
    reserveFairQuota(db, user, job, db.accounts[0], now, request, null, charge => assertConfirmedDispatch(db, user, request, [], charge, env)); return job.id;
  }, { immediate: true });
  const finish = async (requestId, { failed = false, late = false, bytes = image, mimeType = 'image/png' } = {}) => {
    const imageId = `image-${requestId}`, file = `images/${imageId}.png`; await mkdir(path.join(dataDir, 'images'), { recursive: true });
    if (!failed || late) await writeFile(path.join(dataDir, file), bytes);
    return store.update(db => {
      const job = db.jobs.find(job => job.requestId === requestId), now = Date.now(); job.updatedAt = new Date(now).toISOString();
      if (failed && !late) {
        settleFairQuota(db, job, false, now, { requestStarted: false }); job.status = 'failed'; job.completedAt = job.updatedAt;
        job.error = 'synthetic upstream failure'; job.errorStatus = 502; job.errorCode = 'generation_failed'; recordGenerationFailure(db, job);
      } else {
        const artifact = { id: imageId, token: job.userToken, file, mimeType, byteLength: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex'), createdAt: job.updatedAt };
        db.images.push(artifact); job.imageId = imageId;
        if (late) recordRecoveredGeneration(db, job, imageId, now);
        else { settleFairQuota(db, job, true, now); job.status = 'done'; job.completedAt = job.updatedAt; }
        recordCompletedArtifact(db, job, artifact, now);
      }
      return job.id;
    }, { immediate: true });
  };
  const restart = async () => { await store.close(); store = new JsonStore(dataDir); await store.init(); deps.store = store; deps.handleMemberGeneration = handleGeneration(); };
  return { get store() { return store; }, deps, server, sessions, budget, imageStorage, previews, origin, raw, json, quote, submit, openEvents, dispatch, finish, restart, get drainCalls() { return drainCalls; } };
}

test('HTTP capabilities and quotes require Cookie/Origin, remain private and do not write wallets or domain facts', async t => {
  const f = await fixture(t), before = JSON.stringify(f.store.db.settings.fairQuota), ledger = JSON.stringify(f.store.db.ledger), events = f.store.sqlite.prepare('SELECT COUNT(*) n FROM domain_events').get().n;
  const sessionRows = f.store.sqlite.prepare('SELECT * FROM member_sessions ORDER BY token_hash').all();
  const capabilities = await f.json('/api/member/generation/capabilities'); assert.equal(capabilities.status, 200);
  assert.match(capabilities.headers.get('cache-control'), /private/); assert.match(capabilities.headers.get('cache-control'), /no-store/);
  for (let index = 0; index < 3; index++) { const quote = await f.quote(); assert.deepEqual(quote.charges.total, zero()); }
  assert.equal(JSON.stringify(f.store.db.settings.fairQuota), before); assert.equal(JSON.stringify(f.store.db.ledger), ledger);
  assert.equal(f.store.sqlite.prepare('SELECT COUNT(*) n FROM domain_events').get().n, events); assert.equal(f.store.countRecords('jobs'), 0);
  assert.deepEqual(f.store.sqlite.prepare('SELECT * FROM member_sessions ORDER BY token_hash').all(), sessionRows);
  const bearer = await f.json('/api/member/generation/capabilities', { member: null, headers: { authorization: 'Bearer synthetic-key-a' } }); assert.equal(bearer.status, 401);
  const cross = await f.json('/api/member/generation/quote', { method: 'POST', body: input(), headers: { origin: 'https://other.invalid' } });
  assert.equal(cross.status, 403); assert.equal(cross.body.code, 'origin_rejected');
  assert.equal((await f.json('/api/member/generation/capabilities', { headers: { 'sec-fetch-site': 'cross-site' } })).status, 403);
  const spoof = await f.json('/api/member/generation/quote', { method: 'POST', body: { ...input(), memberId: 'b' } }); assert.equal(spoof.status, 422);
  assert.equal(f.store.activeGenerationPayloadBytes(), 0);
});

test('HTTP confirmation tampering and same-key concurrency preserve one durable request and its first context', async t => {
  const f = await fixture(t), body = input(), quote = await f.quote(body), acceptedBody = { ...body, confirmation: confirmation(quote) };
  assert.equal((await f.json('/api/member/generation/requests', { method: 'POST', key: 'missing-confirmation', body })).body.code, 'confirmation_required');
  assert.equal((await f.json('/api/member/generation/requests', { method: 'POST', body: acceptedBody })).body.code, 'invalid_idempotency_key');
  const changed = structuredClone(acceptedBody); changed.confirmation.versions.pricing = 'untrusted';
  assert.equal((await f.json('/api/member/generation/requests', { method: 'POST', key: 'tampered', body: changed })).body.code, 'quote_changed');
  assert.equal(f.store.countRecords('jobs'), 0);
  const [first, second] = await Promise.all([1, 2].map(() => f.json('/api/member/generation/requests', { method: 'POST', key: 'concurrent', body: acceptedBody })));
  assert.equal(first.status, 202, JSON.stringify(first.body)); assert.equal(second.status, 202, JSON.stringify(second.body)); assert.equal(first.body.requestId, second.body.requestId);
  assert.deepEqual([first.body.replayed, second.body.replayed].sort(), [false, true]);
  const requestId = first.body.requestId, original = f.store.readGenerationRequest(requestId);
  assert.ok(Number.isInteger(original.seed)); assert.equal(f.store.countRecords('jobs'), 1);
  assert.equal(original.normalizationContext.format, 'native-member-v1');
  assert.equal(f.store.sqlite.prepare("SELECT COUNT(*) n FROM domain_events WHERE event_type = 'RequestAccepted'").get().n, 1);
  const alteredBudget = structuredClone(acceptedBody); alteredBudget.confirmation.maxTotal.v5 = '1';
  assert.equal((await f.json('/api/member/generation/requests', { method: 'POST', key: 'concurrent', body: alteredBudget })).body.code, 'idempotency_conflict');
  await f.restart();
  const policy = await f.json('/api/member/owner/member-management', { method: 'POST', body: { action: 'fallback-policy', enabled: true } }); assert.equal(policy.status, 200, JSON.stringify(policy.body));
  BASE_MODELS.delete(body.model);
  try {
    const replay = await f.json('/api/member/generation/requests', { method: 'POST', key: 'concurrent', body: acceptedBody });
    assert.equal(replay.status, 202, JSON.stringify(replay.body)); assert.equal(replay.body.requestId, requestId); assert.equal(replay.body.replayed, true);
    assert.equal(f.store.readGenerationRequest(requestId).seed, original.seed);
  } finally { BASE_MODELS.add(body.model); }
  const saved = await f.json(`/api/member/requests/${requestId}/input`); assert.equal(saved.body.input.parameters.seed, original.seed);
  assert.equal((await f.json(`/api/member/requests/${requestId}/input`, { member: 'b' })).status, 404);
});

test('HTTP paid confirmations enforce per-job and total caps without reserving resources on rejection', async t => {
  const f = await fixture(t), body = input({ n_samples: 2, width: 1024, height: 1024 }, { model: 'nai-diffusion-5-full' }), quote = await f.quote(body);
  assert.equal(quote.charges.total.v5, '0.115606936418'); const balance = structuredClone(f.store.db.settings.fairQuota.balances.a);
  const denied = { ...body, confirmation: { ...confirmation(quote), maxTotal: quote.charges.perJob[0].resources } };
  const total = await f.json('/api/member/generation/requests', { method: 'POST', key: 'under-total', body: denied });
  assert.equal(total.status, 409); assert.equal(total.body.code, 'cost_limit_exceeded');
  denied.confirmation = { ...confirmation(quote), maxPerJob: zero() };
  const perJob = await f.json('/api/member/generation/requests', { method: 'POST', key: 'under-one', body: denied });
  assert.equal(perJob.status, 409); assert.equal(perJob.body.code, 'cost_limit_exceeded');
  assert.equal(f.store.countRecords('jobs'), 0); assert.equal(f.store.activeGenerationPayloadBytes(), 0);
  assert.deepEqual(f.store.db.settings.fairQuota.balances.a, balance); assert.deepEqual(f.store.db.settings.fairQuota.reservations, {});
});

test('HTTP JSON and multipart share large-image quote semantics and release failed attachment budgets', async t => {
  const f = await fixture(t), bytes = png(768, 768, true); assert.ok(bytes.length > 2 * 1024 * 1024);
  const body = { operation: 'encode-vibe', image: bytes.toString('base64'), model: 'nai-diffusion-4-5-full', information_extracted: 0.75 }, json = await f.quote(body);
  const form = new FormData(); form.set('request', JSON.stringify({ ...body, image: 'image' })); form.set('image', new Blob([bytes], { type: 'image/png' }), 'synthetic-large.png');
  const multipart = await f.json('/api/member/generation/quote', { method: 'POST', body: form });
  assert.equal(multipart.status, 200, JSON.stringify(multipart.body)); assert.equal(multipart.body.requestFingerprint, json.requestFingerprint); assert.deepEqual(multipart.body.charges, json.charges);
  const oversized = new FormData(); oversized.set('request', JSON.stringify({ ...body, image: 'image' })); oversized.set('image', new Blob([Buffer.alloc(8 * 1024 * 1024 + 1)], { type: 'image/png' }), 'too-large.png');
  assert.equal((await f.json('/api/member/generation/quote', { method: 'POST', body: oversized })).status, 422);
  assert.equal(f.store.activeGenerationPayloadBytes(), 0); assert.equal(f.store.countRecords('jobs'), 0);
  assert.equal((await f.quote()).admission.allowed, true);
});

test('HTTP input assets and result states enforce ownership including owner and retain refunded late results', async t => {
  const f = await fixture(t), other = await f.submit(input({ image: image64, mask: image64 }, { action: 'infill' }), 'other-assets', 'b');
  assert.equal(other.status, 202, JSON.stringify(other.body)); const { requestId } = other.body, jobId = other.body.jobIds[0];
  const ownInput = await f.json(`/api/member/requests/${requestId}/input`, { member: 'b' }); assert.equal(ownInput.status, 200); assert.equal(ownInput.body.assets.length, 2);
  assert.ok(!JSON.stringify(ownInput.body).includes(image64)); assert.ok(!JSON.stringify(ownInput.body).includes('upstream-secret'));
  const asset = await f.raw(ownInput.body.assets[0].url, { member: 'b' }); assert.equal(asset.status, 200); assert.deepEqual(Buffer.from(await asset.arrayBuffer()), image);
  for (const suffix of ['/input', '/inputs/parameters.mask', '/events']) assert.equal((await f.json(`/api/member/requests/${requestId}${suffix}`)).status, 404, 'owner cannot read another member input');
  assert.equal((await f.json(`/api/member/jobs/${jobId}/content`)).status, 404);
  assert.equal((await f.json(`/api/member/jobs/${jobId}/content`, { member: 'b' })).body.code, 'result_not_ready');
  await f.dispatch(requestId); await f.finish(requestId, { failed: true });
  assert.equal((await f.json(`/api/member/jobs/${jobId}/content`, { member: 'b' })).body.code, 'generation_failed');
  const refunded = structuredClone(f.store.db.settings.fairQuota.balances.b); await f.finish(requestId, { late: true });
  const recovered = await f.raw(`/api/member/jobs/${jobId}/content`, { member: 'b' }); assert.equal(recovered.status, 200); assert.deepEqual(Buffer.from(await recovered.arrayBuffer()), image);
  const state = await f.json(`/api/member/requests/${requestId}`, { member: 'b' }); assert.equal(state.body.status, 'failed'); assert.equal(state.body.jobs[0].lateResultAvailable, true);
  assert.equal(state.body.jobs[0].charge.status, 'refunded'); assert.deepEqual(f.store.db.settings.fairQuota.balances.b, refunded);
  await f.store.update(db=>{
    const legacy={id:'legacy-inline-own-result',memberId:'b',userToken:'synthetic-key-b',status:'done',imageId:`image-${requestId}`};
    db.jobs.push(legacy);
    recordDomainEvent(db,{eventType:'OpeningStateRecorded',streamId:`request:legacy-${legacy.id}`,
      payload:{state:{request:{requestId:null,memberId:'b'},jobs:{[legacy.id]:{id:legacy.id,memberId:'b',status:'done',imageId:legacy.imageId}},artifacts:{},aliases:[],freeUsage:{}}}});
  },{dirtyRows:{jobs:['legacy-inline-own-result']}});
  const legacyResult=await f.raw('/api/member/jobs/legacy-inline-own-result/content',{member:'b'});
  assert.equal(legacyResult.status,200); assert.deepEqual(Buffer.from(await legacyResult.arrayBuffer()),image);
  assert.equal((await f.json('/api/member/jobs/legacy-inline-own-result/content',{member:'a'})).status,404);
});

test('SSE publishes snapshot, committed state, preview over 64KiB and terminal complete without persisting previews', async t => {
  const f = await fixture(t), accepted = await f.submit(), requestId = accepted.body.requestId;
  assert.equal(accepted.status, 202, JSON.stringify(accepted.body)); const stream = await f.openEvents(requestId);
  assert.match(stream.response.headers.get('cache-control'), /no-cache/); assert.match(stream.response.headers.get('cache-control'), /no-transform/);
  assert.equal((await stream.wait('snapshot')).data.status, 'queued'); const jobId = await f.dispatch(requestId);
  assert.equal((await stream.wait('state')).data.status, 'running');
  const before = f.store.sqlite.prepare('SELECT COUNT(*) n FROM domain_events').get().n, preview = Buffer.alloc(90 * 1024, 42);
  f.previews.update(f.store.db.jobs.find(job => job.id === jobId), { previewBuffer: preview, previewMimeType: 'image/png', percent: 40, step: 9, total: 23 });
  const frame = await stream.wait('preview'); assert.equal(frame.data.jobId, jobId); assert.equal(Buffer.from(frame.data.preview.split(',')[1], 'base64').length, preview.length);
  assert.equal(f.store.sqlite.prepare('SELECT COUNT(*) n FROM domain_events').get().n, before);
  await f.finish(requestId); assert.equal((await stream.wait('complete')).data.status, 'done');
  const content = await f.raw(`/api/member/jobs/${jobId}/content`); assert.equal(content.status, 200); assert.deepEqual(Buffer.from(await content.arrayBuffer()), image);
  await stream.close(); assert.equal(f.previews.watchers.size, 0);
});

test('SSE closes on real session revocation or client disconnect without cancelling accepted jobs', async t => {
  const f = await fixture(t), accepted = await f.submit(), requestId = accepted.body.requestId, callbacks = [], original = globalThis.setInterval;
  t.mock.method(globalThis, 'setInterval', (callback, milliseconds, ...args) => { if (milliseconds === 15000) callbacks.push(callback); return original(callback, milliseconds, ...args); });
  const stream = await f.openEvents(requestId); await stream.wait('snapshot'); await stream.close();
  assert.equal(f.store.readGenerationRequestState(requestId, 'a').jobs[0].status, 'queued');
  const expired = await f.openEvents(requestId); await expired.wait('snapshot');
  await logoutMemberSession(f.store, f.sessions.a.token); callbacks.at(-1)();
  await expired.wait('auth-expired'); await expired.close(); assert.equal(f.store.readGenerationRequestState(requestId, 'a').jobs[0].status, 'queued');
  assert.equal(f.previews.watchers.size, 0);
});

test('a terminal commit during the initial SSE snapshot cannot lose its complete notification', async t => {
  const f = await fixture(t), accepted = await f.submit(), requestId = accepted.body.requestId;
  const original = f.store.readCollections.bind(f.store); let calls = 0, release, reached;
  const gate = new Promise(resolve => { release = resolve; }), paused = new Promise(resolve => { reached = resolve; });
  t.mock.method(f.store, 'readCollections', async (...args) => {
    const result = await original(...args);
    if (args[0].includes('users') && ++calls === 2) { reached(); await gate; }
    return result;
  });
  const opening = f.openEvents(requestId); await paused; const stream = await opening;
  try { await f.finish(requestId, { failed: true }); } finally { release(); }
  assert.equal((await stream.wait('snapshot')).data.status, 'failed'); assert.equal((await stream.wait('complete')).data.status, 'failed');
  await stream.close();
});

test('disconnects during initial ownership reads do not consume SSE slots or timers', async t => {
  const f=await fixture(t), accepted=await f.submit(), requestId=accepted.body.requestId;
  const originalRead=f.store.readCollections.bind(f.store), originalInterval=globalThis.setInterval;
  let enter, release, gate; const intervals=[];
  t.mock.method(globalThis,'setInterval',(callback,milliseconds,...args)=>{
    if(milliseconds===15000)intervals.push(callback); return originalInterval(callback,milliseconds,...args);
  });
  t.mock.method(f.store,'readCollections',async (...args)=>{
    const result=await originalRead(...args);
    if(gate&&args[0].includes('users')){enter();await gate;}
    return result;
  });
  for(let attempt=0;attempt<3;attempt++){
    const entered=new Promise(resolve=>{enter=resolve;}); gate=new Promise(resolve=>{release=resolve;});
    const closed=new Promise(resolve=>f.server.once('request',(_req,res)=>res.once('close',resolve)));
    const abort=new AbortController(), request=f.raw(`/api/member/requests/${requestId}/events`,{signal:abort.signal});
    await entered; abort.abort(); await assert.rejects(request,error=>error.name==='AbortError'); await closed;
    release(); gate=null; await new Promise(resolve=>setImmediate(resolve));
  }
  assert.equal(intervals.length,0); assert.equal(f.previews.watchers.size,0);
  const active=await f.openEvents(requestId); await active.wait('snapshot'); assert.equal(intervals.length,1); await active.close();
});

test('temporary membership service failure ends SSE with error without claiming auth expiry', async t => {
  const f=await fixture(t), accepted=await f.submit(), requestId=accepted.body.requestId;
  const originalInterval=globalThis.setInterval, callbacks=[];
  t.mock.method(globalThis,'setInterval',(callback,milliseconds,...args)=>{
    if(milliseconds===15000)callbacks.push(callback); return originalInterval(callback,milliseconds,...args);
  });
  const stream=await f.openEvents(requestId); await stream.wait('snapshot');
  const originalSession=f.store.readMemberSession.bind(f.store);
  t.mock.method(f.store,'readMemberSession',()=>{throw Object.assign(new Error('synthetic membership service unavailable'),{statusCode:503});});
  callbacks.at(-1)(); const error=await stream.wait('error'); assert.equal(error.data.statusCode,503);
  assert.ok(!stream.events.some(event=>event.type==='auth-expired')); await stream.close();
  assert.ok(originalSession(createHash('sha256').update(f.sessions.a.token).digest('hex')),'session remains valid');
  assert.equal(f.store.readGenerationRequestState(requestId,'a').jobs[0].status,'queued');
});

test('executor shutdown closes member preview subscriptions before waiting for HTTP connections', async t => {
  const f=await fixture(t), accepted=await f.submit(), requestId=accepted.body.requestId;
  const stream=await f.openEvents(requestId); await stream.wait('snapshot');
  closeMemberGenerationStreams(f.store); await stream.done;
  assert.equal(stream.ended,true); assert.equal(f.previews.watchers.size,0);
  assert.equal(f.store.readGenerationRequestState(requestId,'a').jobs[0].status,'queued');
});

for (const waitingOn of ['ownership', 'discord-membership']) test(`shutdown while awaiting ${waitingOn} refuses a late SSE subscription`, async t => {
  const f = await fixture(t), accepted = await f.submit(), requestId = accepted.body.requestId;
  let enter, release;
  const entered = new Promise(resolve => { enter = resolve; }), gate = new Promise(resolve => { release = resolve; });
  if (waitingOn === 'ownership') {
    const originalRead = f.store.readCollections.bind(f.store);
    t.mock.method(f.store, 'readCollections', async (...args) => {
      const result = await originalRead(...args);
      if (args[0].includes('users')) { enter(); await gate; }
      return result;
    });
  } else {
    await f.store.update(db => {
      const member = db.users.find(user => user.id === 'a');
      Object.assign(member.discord, { checkedAt: Date.now() - 6 * 60000, accessToken: 'synthetic-discord-token', expiresAt: Date.now() + 3600000 });
      const { id, guildId, checkedAt, member: eligible } = member.discord;
      recordDomainEvent(db, { eventType: 'MemberEligibilityChanged', streamId: 'sharing:default', memberId: member.id,
        payload: { memberId: member.id, discord: { id, guildId, checkedAt, member: eligible } } });
    }, { collections: ['users'], immediate: true });
    t.mock.method(f.deps, 'discordFetch', async url => {
      assert.match(String(url), /\/users\/@me\/guilds\/12345678901234567\/member$/);
      enter(); await gate;
      return Response.json({ pending: false, user: { id: '32345678901234560' } });
    });
  }
  const originalInterval = globalThis.setInterval, intervals = [], initialCommitListeners = f.store.events.listenerCount('commit'), initialCloseListeners = f.store.events.listenerCount('close');
  t.mock.method(globalThis, 'setInterval', (callback, milliseconds, ...args) => {
    if (milliseconds === 15000) intervals.push(callback);
    return originalInterval(callback, milliseconds, ...args);
  });
  const opening = f.raw(`/api/member/requests/${requestId}/events`);
  await entered; closeMemberGenerationStreams(f.store);
  const serverClosed = new Promise(resolve => f.server.close(resolve));
  release();
  const response = await opening;
  assert.equal(response.status, 503); assert.equal((await response.json()).code, 'executor_closing');
  await serverClosed;
  assert.equal(intervals.length, 0); assert.equal(f.previews.watchers.size, 0);
  assert.equal(f.store.events.listenerCount('commit'), initialCommitListeners);
  assert.equal(f.store.events.listenerCount('close'), initialCloseListeners);
  assert.equal(f.store.readGenerationRequestState(requestId, 'a').jobs[0].status, 'queued');
});
