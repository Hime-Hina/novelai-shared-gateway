import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { JsonStore } from '../../server/store.ts';
import { FAILURE_EVENT_PREDICATE, updateLossAlertPolicy } from '../../server/failure-accounting.ts';
import { configureFairQuota, syncFairQuota, reserveFairQuota, settleFairQuota } from '../../server/fair-quota.ts';
import { handleMemberRoutes } from '../../server/member-routes.ts';
import { createMemberSession, setMemberCredentials } from '../../server/member-auth.ts';

const day = 86400000, iso = time => new Date(time).toISOString();
const candidate = resources => ({ resources, amountSource: 'synthetic-evidence' });
async function fixture(t, now = Date.now()) {
  const store = new JsonStore(await mkdtemp(path.join(tmpdir(), 'nai-failure-accounting-')));
  await store.init(); t.after(() => store.close());
  await store.update(db => {
    db.users = ['a', 'b', 'c', 'd'].map((id, i) => ({ id, token: `STA1N-${id}`, enabled: true, balance: 0,
      memberAuth: { username: ['Alice', 'Bob', 'Carol', 'Dan'][i], version: id },
      discord: { id: String(222222222222222220n + BigInt(i)), guildId: '1134557553011998840', checkedAt: Date.now(), member: true, accessToken: 'private-discord' } }));
    db.accounts = [{ id: 'opus', token: 'pst-synthetic-private-upstream', enabled: true, quotaTier: 3, subscriptionActive: true,
      quotaFixed: 10000, quotaPurchased: 100, v5UsagePercent: 80, v5UsageTimeUntilNextPercent: 0, quotaCheckedAt: now }];
    configureFairQuota(db, db.users.map(user => user.id)); syncFairQuota(db, now);
  }, { immediate: true });
  return store;
}
async function addEvents(store, events) {
  await store.update(db => db.ledger.unshift(...events), { dirtyRows: { ledger: events.map(event => event.id) }, immediate: true });
}
function suspected(id, at, candidates, memberId = 'a', attemptId = id) {
  return { id, type: 'generation_loss_suspected', attemptId, memberId, at: iso(at), upstreamCandidates: candidates,
    prompt: 'private-prompt', token: 'private-token', upstreamError: 'private-upstream-error' };
}

test('冷历史滚动窗口、候选去重、安全分页、重启与查询索引', async t => {
  const now = 1800000000000, store = await fixture(t, now);
  const alternatives = [candidate({ v5: 0.05 }), candidate({ anlasFixed: 26, anlasPurchased: 0 })];
  const events = [
    ...Array.from({ length: 1100 }, (_, i) => ({ id: `old-${i}`, type: 'generation_refunded', at: iso(now - 40 * day), resources: { v5: .1 }, prompt: 'private-prompt' })),
    suspected('fallback', now - 1000, alternatives), suspected('duplicate', now - 1000, alternatives, 'a', 'fallback'),
    suspected('purchased', now - day, [candidate({ anlasPurchased: 4 })], 'b'),
    suspected('old24', now - day - 1, [candidate({ anlasFixed: 5 })]),
    suspected('old30', now - 30 * day, [candidate({ v5: .1 })]),
    suspected('outside', now - 30 * day - 1, [candidate({ anlasFixed: 999 })]),
    suspected('future', now + 1, [candidate({ anlasPurchased: 999 })]),
    suspected('unknown', now - 2000, null), suspected('malformed', now - 3000, ['not-json']),
    { id: 'estimated', type: 'generation_loss_estimated', at: iso(now), attemptId: 'fallback', upstreamCandidates: alternatives },
    { id: 'recovered', type: 'generation_result_recovered', at: iso(now), attemptId: 'fallback', upstreamCandidates: alternatives },
    { id: 'fixed-gap', type: 'resource_gap_observed', at: iso(now), resource: 'anlasFixed', observedAmount: 10, amount: 500 },
    { id: 'purchase-gap', type: 'resource_gap_observed', at: iso(now - 30 * day), resource: 'anlasPurchased', observedAmount: 2, amount: 500 },
    { id: 'capacity', type: 'v5_capacity_observed', at: iso(now), resource: 'v5', delta: .2, amount: 2 },
    { id: 'capacity-negative', type: 'v5_capacity_observed', at: iso(now), resource: 'v5', delta: -.1 },
    { id: 'covered', type: 'operating_loss_covered', at: iso(now), resource: 'anlasFixed', amount: 20 }
  ];
  await addEvents(store, events);
  assert.ok(store.db.ledger.length <= 1000); assert.ok(!store.db.ledger.some(event => event.id === 'fallback'));
  let result = await store.readFailureAccounting(null, now);
  assert.deepEqual(result.risk.last24h, { attempts: 4, unknownAmounts: 2, possible: { v5: .05, anlasFixed: 26, anlasPurchased: 4 } });
  assert.equal(result.risk.last30d.attempts, 6); assert.equal(result.risk.last30d.unknownAmounts, 2);
  assert.ok(Math.abs(result.risk.last30d.possible.v5 - .15) < 1e-12);
  assert.equal(result.risk.last30d.possible.anlasFixed, 31); assert.equal(result.risk.last30d.possible.anlasPurchased, 4);
  assert.deepEqual(result.risk.aggregate, { last24h: { v5: .2, anlasFixed: 10, anlasPurchased: 0 }, last30d: { v5: .2, anlasFixed: 10, anlasPurchased: 2 } });
  assert.match(result.alerts.find(alert => alert.id === 'member-24h:a').message, /Alice/);
  assert.deepEqual(result.alerts.map(alert => alert.id).sort(), ['amount-unknown', 'member-24h:a', 'purchased-30d']);
  const firstCursor = result.nextCursor, seen = new Set();
  do {
    assert.ok(result.events.length <= 50);
    for (const event of result.events) { assert.ok(!seen.has(event.id)); seen.add(event.id); }
    for (const secret of ['private-prompt', 'private-token', 'private-upstream-error', 'private-discord']) assert.ok(!JSON.stringify(result).includes(secret));
    if (!result.nextCursor) break;
    result = await store.readFailureAccounting(result.nextCursor, now);
  } while (true);
  assert.equal(seen.size, events.length); assert.ok(seen.has('outside')); assert.ok(seen.has('estimated'));
  await assert.rejects(store.readFailureAccounting('not-a-cursor', now), error => error.statusCode === 400);
  for (const [sql, index] of [
    [`SELECT id FROM ledger INDEXED BY idx_ledger_failure_history WHERE ${FAILURE_EVENT_PREDICATE} ORDER BY order_value DESC, id DESC LIMIT 51`, 'idx_ledger_failure_history'],
    ["SELECT data FROM ledger WHERE source='generation_loss_suspected' AND at >= '2026'", 'idx_ledger_source_at'],
    ["SELECT id FROM jobs WHERE user_token='STA1N-a' AND json_extract(data, '$.lateResultAvailable')=1 AND json_extract(data, '$.lateResultAt')>'' ORDER BY json_extract(data, '$.lateResultAt') DESC, id DESC LIMIT 10", 'idx_jobs_member_late_results']
  ]) assert.ok(store.sqlite.prepare(`EXPLAIN QUERY PLAN ${sql}`).all().some(row => row.detail.includes(index)), index);
  await store.close(); const reopened = new JsonStore(store.dataDir); t.after(() => reopened.close()); await reopened.init();
  result = await reopened.readFailureAccounting(null, now);
  assert.equal(result.nextCursor, firstCursor); assert.equal(result.risk.last24h.attempts, 4);
});

test('聚合差额独立触发软告警，候选与聚合不相加，零阈值有效且不改变准入/退款', async t => {
  const now = Date.now(), store = await fixture(t, now);
  await addEvents(store, [
    { id: 'fixed', type: 'resource_gap_observed', at: iso(now), resource: 'anlasFixed', observedAmount: 101 },
    { id: 'paid', type: 'resource_gap_observed', at: iso(now), resource: 'anlasPurchased', observedAmount: 25 },
    { id: 'v5', type: 'v5_capacity_observed', at: iso(now), resource: 'v5', delta: 2 }
  ]);
  let result = await store.readFailureAccounting(null, now);
  assert.equal(result.risk.last24h.attempts, 0);
  assert.deepEqual(result.alerts.map(alert => alert.id), ['v5-24h', 'fixed-24h', 'purchased-30d']);
  assert.ok(result.alerts.every(alert => /未相加，不代表已确认单次消费/.test(alert.message)));
  await addEvents(store, [suspected('attempt', now, [candidate({ v5: 2, anlasFixed: 101, anlasPurchased: 25 })])]);
  await store.update(db => updateLossAlertPolicy(db, { expectedVersion: 1, policy: { v5Points24h: 3, fixedAnlas24h: 150, fixedAnlas30d: 500, memberAttempts24h: 3 } }, 'a'), { collections: ['settings', 'ledger'], immediate: true });
  result = await store.readFailureAccounting(null, now);
  assert.deepEqual(result.alerts.map(alert => alert.id), ['purchased-30d']); // 2+2 或 101+101 不触发。
  await store.update(db => updateLossAlertPolicy(db, { expectedVersion: 2, policy: { v5Points24h: 0, fixedAnlas24h: 0, fixedAnlas30d: 0, memberAttempts24h: 0 } }, 'a'), { collections: ['settings', 'ledger'], immediate: true });
  result = await store.readFailureAccounting(null, now);
  assert.deepEqual(result.alerts.map(alert => alert.id), ['v5-24h', 'fixed-24h', 'fixed-30d', 'purchased-30d', 'member-24h:a']);
  assert.equal(new Set(result.alerts.map(alert => alert.id)).size, result.alerts.length);
  const before = structuredClone(store.db.settings.fairQuota.balances);
  await store.update(db => {
    const job = { id: 'still-admitted', userToken: 'STA1N-a', status: 'queued', request: { model: 'nai-diffusion-5-full', width: 1024, height: 1024, steps: 23 } };
    reserveFairQuota(db, db.users[0], job, db.accounts[0], now);
    assert.equal(job.fairCharge.status, 'reserved'); settleFairQuota(db, job, false, now);
    assert.equal(job.fairCharge.status, 'refunded');
  }, { collections: ['settings', 'ledger'], immediate: true });
  assert.deepEqual(store.db.settings.fairQuota.balances, before);
});

async function routes(t, store) {
  const env = { OWNER_USER_ID: 'a', DISCORD_GUILD_ID: '1134557553011998840', DISCORD_CLIENT_ID: '123456789012345678', DISCORD_CLIENT_SECRET: 'synthetic-secret-only', PUBLIC_BASE_URL: 'https://nai.example' };
  const deps = { env, store, assertAdmin(req) { if (req.headers['x-admin-token'] !== 'synthetic-admin') throw Object.assign(new Error('admin required'), { statusCode: 401 }); } };
  const server = http.createServer(async (req, res) => {
    try { await handleMemberRoutes(req, res, new URL(req.url, 'https://nai.example'), deps); }
    catch (error) { res.writeHead(error.statusCode || 500, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: error.message })); }
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => { server.closeAllConnections(); server.close(); });
  const cookies = await Promise.all(store.db.users.map(async user => 'nai_member_session=' + (await createMemberSession(store, user, env)).token));
  return (route, { viewer = 0, body, admin = false, origin } = {}) => new Promise((resolve, reject) => {
    const host = admin ? `127.0.0.1:${server.address().port}` : 'nai.example';
    const req = http.request({ hostname: '127.0.0.1', port: server.address().port, path: route, method: body ? 'POST' : 'GET',
      headers: { host, origin: origin || (admin ? `http://${host}` : 'https://nai.example'), 'content-type': 'application/json',
        ...(viewer === null ? {} : { cookie: cookies[viewer] }), ...(admin ? { 'x-admin-token': 'synthetic-admin' } : {}) } }, res => {
      let text = ''; res.on('data', chunk => text += chunk); res.on('end', () => resolve({ status: res.statusCode, data: JSON.parse(text) }));
    }); req.on('error', reject); req.end(body ? JSON.stringify(body) : undefined);
  });
}

test('管理损耗 API 鉴权、独立策略版本、来源限制、可信审计及写失败同事务回滚', async t => {
  const store = await fixture(t), call = await routes(t, store), owner = '/api/member/owner/member-management', admin = '/api/admin/member-management';
  const policy = { v5Points24h: 0, fixedAnlas24h: 0, fixedAnlas30d: 0, memberAttempts24h: 0 };
  const body = { action: 'loss-alert-policy', expectedVersion: 1, policy, actorId: 'forged' };
  for (const viewer of [null, 1]) {
    assert.equal((await call(`${owner}?view=losses`, { viewer })).status, viewer === null ? 401 : 403);
    assert.equal((await call(owner, { viewer, body })).status, viewer === null ? 401 : 403);
  }
  assert.equal((await call(`${admin}?view=losses`)).status, 404);
  assert.equal((await call(owner, { body, origin: 'https://evil.example' })).status, 403);
  assert.equal((await call(`${owner}?view=losses`)).status, 200);
  assert.equal((await call(`${admin}?view=losses`, { viewer: null, admin: true })).status, 200);
  await store.update(db => db.settings.fairQuota.configVersion = 99, { collections: ['settings'], immediate: true });
  let response = await call(owner, { body }); assert.equal(response.status, 200); assert.deepEqual(response.data, { policy: { version: 2, ...policy } });
  assert.equal((await call(owner, { body })).status, 409);
  let history = await store.readFailureAccounting();
  assert.equal(history.events.filter(event => event.type === 'loss_alert_policy_updated').length, 1);
  assert.equal(history.events[0].actorId, 'a'); assert.equal(history.events[0].before.version, 1); assert.equal(history.events[0].after.fixedAnlas24h, 0);
  for (const invalid of [-1, 1e20, .5, null]) assert.equal((await call(owner, { body: { ...body, expectedVersion: 2, policy: { ...policy, fixedAnlas24h: invalid } } })).status, 400);
  assert.equal((await call(owner, { body: { ...body, expectedVersion: 2, policy: { ...policy, v5Points24h: null } } })).status, 400);
  store.sqlite.exec("CREATE TEMP TRIGGER reject_policy BEFORE INSERT ON ledger WHEN NEW.source='loss_alert_policy_updated' BEGIN SELECT RAISE(ABORT,'synthetic policy failure'); END;");
  assert.equal((await call(admin, { viewer: null, admin: true, body: { ...body, expectedVersion: 2 } })).status, 500);
  history = await store.readFailureAccounting(); assert.equal(history.policy.version, 2); assert.equal(history.events.length, 1);
  store.sqlite.exec('DROP TRIGGER reject_policy');
  response = await call(admin, { viewer: null, admin: true, body: { ...body, expectedVersion: 2 } }); assert.equal(response.status, 200);
  history = await store.readFailureAccounting(); assert.equal(history.policy.version, 3); assert.equal(history.events[0].actorId, 'local-admin');
  assert.equal(store.db.settings.fairQuota.configVersion, 99);
});

test('成员冷历史迟到结果只签发本人仍存在的图片，真实 HTTP 无 Cookie 领取签名链接', { timeout: 20000 }, async t => {
  const store = await fixture(t), png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
  await mkdir(path.join(store.dataDir, 'images'), { recursive: true }); await writeFile(path.join(store.dataDir, 'images', 'result.png'), png);
  await store.update(async db => {
    for (const user of db.users) delete user.discord;
    await setMemberCredentials(db, 'a', { username: 'alice', password: 'synthetic-password-123456789' });
    await setMemberCredentials(db, 'b', { username: 'bob', password: 'synthetic-password-123456789' });
    db.accounts[0].enabled = false;
  }, { collections: ['users', 'accounts'], immediate: true });
  const now = Date.now(), charge = { resource: 'anlas', status: 'refunded', amount: 7, fixed: 5, purchased: 2 };
  const jobs = Array.from({ length: 3100 }, (_, i) => ({ id: `filler-${i}`, userToken: 'STA1N-b', status: 'done', createdAt: iso(now), request: { model: 'nai-diffusion-5-full' } }));
  const images = [], add = (id, userToken, imageToken = userToken, lateAt = now) => {
    jobs.push({ id, userToken, status: 'failed', createdAt: iso(now - 40 * day), lateResultAvailable: true, lateResultAt: iso(lateAt),
      imageId: `image-${id}`, fairCharge: charge, request: { model: 'nai-diffusion-5-full', prompt: 'private-prompt' } });
    images.push({ id: `image-${id}`, token: imageToken, file: 'images/result.png', mimeType: 'image/png', createdAt: iso(now) });
  };
  for (let i = 0; i < 12; i++) add(`late-${i}`, 'STA1N-a', 'STA1N-a', now - i);
  add('other', 'STA1N-b'); add('mismatch', 'STA1N-a', 'STA1N-b'); add('deleted-image', 'STA1N-a'); images.pop();
  await store.update(db => { db.jobs.push(...jobs); db.images.push(...images); }, { dirtyRows: { jobs: jobs.map(job => job.id), images: images.map(image => image.id) }, immediate: true });
  assert.ok(!store.db.jobs.some(job => job.id === 'late-0'));
  const result = await store.readMemberLateResults(store.db.users[0]);
  assert.deepEqual(result.map(row => row.id), Array.from({ length: 10 }, (_, i) => `late-${i}`));
  assert.equal((await store.readMemberLateResults({ id: 'missing', token: 'STA1N-a' })).length, 0);
  const owned = await store.readMemberImageIds(store.db.users[0], ['late-0', 'other', 'mismatch', 'deleted-image']);
  assert.deepEqual([...owned], [['late-0', 'image-late-0']]);
  // 另加一个热任务，覆盖 me.jobs 的分池退款和签名链接。
  const hot = { ...jobs.find(job => job.id === 'late-0'), id: 'hot-result' };
  await store.update(db => db.jobs.unshift(hot), { dirtyRows: { jobs: [hot.id] }, immediate: true });
  await store.close();
  const listener = http.createServer(); listener.listen(0, '127.0.0.1'); await once(listener, 'listening');
  const port = listener.address().port; await new Promise(resolve => listener.close(resolve));
  const env = Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE'].filter(key => process.env[key]).map(key => [key, process.env[key]]));
  Object.assign(env, { ADMIN_TOKEN: 'synthetic-admin-for-local-result-test-12345', DATA_DIR: store.dataDir, HOST: '127.0.0.1', PORT: String(port), PUBLIC_HOST: 'gateway.example',
    NOVELAI_API_URL: 'http://127.0.0.1:1', NOVELAI_ACCOUNT_API_URL: 'http://127.0.0.1:1' });
  const child = spawn(process.execPath, ['server/index.js'], { cwd: fileURLToPath(new URL('../../', import.meta.url)), env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  t.after(async () => { if (child.exitCode === null) { child.kill(); await once(child, 'exit'); } }); child.stderr.resume();
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('server startup timeout')), 10000);
    child.once('exit', code => { clearTimeout(timer); reject(new Error(`server exited ${code}`)); });
    child.stdout.on('data', chunk => { if (chunk.toString().includes('listening on')) { clearTimeout(timer); resolve(); } });
  });
  const base = `http://127.0.0.1:${port}`, headers = { origin: base, 'content-type': 'application/json' };
  async function me(username) {
    const login = await fetch(`${base}/api/member/login`, { method: 'POST', headers, body: JSON.stringify({ username, password: 'synthetic-password-123456789' }) });
    assert.equal(login.status, 200, await login.text()); const cookie = login.headers.get('set-cookie'); assert.match(cookie, /Path=\/api\/member/);
    const response = await fetch(`${base}/api/member/me`, { headers: { ...headers, cookie: cookie.split(';')[0] } });
    assert.equal(response.status, 200); return response.json();
  }
  const alice = await me('alice'); assert.equal(alice.lateResults.length, 10);
  assert.ok(alice.lateResults.some(job => job.id === 'late-0'));
  assert.ok(!alice.lateResults.some(job => ['other', 'mismatch', 'deleted-image'].includes(job.id)));
  assert.equal(alice.jobs.find(job => job.id === 'hot-result').charge.fixed, 5);
  assert.equal(alice.jobs.find(job => job.id === 'hot-result').charge.purchased, 2);
  for (const item of [alice.lateResults[0], alice.jobs.find(job => job.id === 'hot-result')]) {
    assert.ok(item.imageUrl.includes('signature=')); assert.ok(!item.imageUrl.includes('STA1N'));
    const response = await fetch(new URL(item.imageUrl, base), { headers: { host: 'gateway.example' } });
    assert.equal(response.status, 200); assert.deepEqual(Buffer.from(await response.arrayBuffer()), png);
  }
  const bob = await me('bob'); assert.deepEqual(bob.lateResults.map(job => job.id), ['other']);
  assert.ok(!JSON.stringify(alice).includes('private-prompt'));
});
