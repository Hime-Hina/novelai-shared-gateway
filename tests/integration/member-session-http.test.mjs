import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';
import { bootstrap } from '../../scripts/bootstrap.mjs';
import { JsonStore } from '../../server/store.ts';
import { syncFairQuota } from '../../server/fair-quota.ts';
import { createMemberSession } from '../../server/member-auth.ts';

const root = fileURLToPath(new URL('../../', import.meta.url));
const memberTtl = 7 * 86400000, ownerTtl = 24 * 3600000;
const cookieFor = session => `nai_member_session=${session.token}`;
const tokenHash = cookie => createHash('sha256').update(cookie.slice(cookie.indexOf('=') + 1)).digest('hex');
async function listen(server) {
  server.listen(0, '127.0.0.1'); await once(server, 'listening'); return server.address().port;
}

async function fixture(t, { owner = false, grace = 30000 } = {}) {
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-member-session-http-'));
  const adminToken = 'synthetic-admin-member-session-only';
  const initialized = await bootstrap({ env: { ADMIN_TOKEN: adminToken, NOVELAI_TOKEN: 'pst-synthetic-member-session-only', DATA_DIR: dataDir }, log() {} });
  const members = JSON.parse(await readFile(initialized.credentialPath, 'utf8')).accounts;
  const guildId = '1134557553011998840';
  const seeded = new JsonStore(dataDir); await seeded.init();
  try {
    await seeded.update(db => {
      Object.assign(db.accounts[0], { quotaTier: 3, subscriptionActive: true, subscriptionExpiresAt: Date.now() + 86400000,
        quotaFixed: 10000, quotaPurchased: 0, v5UsagePercent: 100, quotaCheckedAt: new Date().toISOString() });
      syncFairQuota(db);
      if (owner) db.users.forEach((user, index) => {
        user.discord = { id: `22222222222222222${index}`, guildId, member: true, checkedAt: Date.now(),
          accessToken: 'synthetic-access', refreshToken: 'synthetic-refresh', expiresAt: Date.now() + 3600000 };
      });
    }, { immediate: true });
  } finally { await seeded.close(); }
  const audit = new Database(path.join(dataDir, 'library.sqlite'), { fileMustExist: true });
  const requests = [];
  const upstream = http.createServer((req, res) => {
    requests.push(req.url);
    if (req.url !== '/user/data') { res.writeHead(500); res.end('unexpected synthetic upstream request'); return; }
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ subscription: { tier: 3, active: true, expiresAt: Math.floor(Date.now() / 1000) + 86400,
      trainingStepsLeft: { fixedTrainingStepsLeft: 10000, purchasedTrainingSteps: 0 },
      usage: { percent: 100, isNegative: false, timeUntilNextPercent: 0 } } }));
  });
  const upstreamPort = await listen(upstream), reservation = http.createServer(), port = await listen(reservation);
  await new Promise(resolve => reservation.close(resolve));
  const base = `http://127.0.0.1:${port}`;
  // 预置 owner grant 只用于离线认证；任何意外的 Discord/NAI 外联都会失败。
  const preload = path.join(dataDir, 'deny-external-fetch.mjs');
  await writeFile(preload, `const original = globalThis.fetch;
globalThis.fetch = (input, options) => {
  const url = new URL(typeof input === 'string' || input instanceof URL ? input : input.url);
  if (!['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)) throw new Error('External fetch blocked by synthetic session test');
  return original(input, options);
};\n`);
  const env = Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE'].filter(key => process.env[key]).map(key => [key, process.env[key]]));
  Object.assign(env, { ADMIN_TOKEN: adminToken, DATA_DIR: dataDir, HOST: '127.0.0.1', PORT: String(port), PUBLIC_HOST: 'session.example',
    SHUTDOWN_GRACE_MS: String(grace),
    PUBLIC_BASE_URL: owner ? 'https://session.example' : base,
    NOVELAI_API_URL: `http://127.0.0.1:${upstreamPort}`, NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${upstreamPort}` });
  if (owner) Object.assign(env, { OWNER_USER_ID: members[0].user_id, DISCORD_GUILD_ID: guildId,
    DISCORD_CLIENT_ID: '123456789012345678', DISCORD_CLIENT_SECRET: 'synthetic-session-client-secret' });
  let child, exited, output = '';
  const record = chunk => { output = (output + chunk.toString()).slice(-16000); };
  const start = async (ownerUserId = env.OWNER_USER_ID) => {
    if (owner) env.OWNER_USER_ID = ownerUserId;
    child = spawn(process.execPath, ['--import', pathToFileURL(preload).href, 'server/index.ts'], { cwd: root, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    exited = once(child, 'exit'); child.stdout.on('data', record); child.stderr.on('data', record);
    await new Promise((resolve, reject) => {
      const cleanup = () => { clearTimeout(timer); child.stdout.off('data', ready); child.off('exit', failed); child.off('error', failed); };
      const ready = chunk => { if (chunk.toString().includes('Nai2API listening')) { cleanup(); resolve(); } };
      const failed = cause => { cleanup(); reject(new Error(`synthetic gateway failed ${cause}: ${output}`)); };
      const timer = setTimeout(() => failed('startup timeout'), 15000);
      child.stdout.on('data', ready); child.once('exit', failed); child.once('error', failed);
    });
  };
  const stop = async () => {
    if (child?.exitCode === null && child?.signalCode === null) child.kill('SIGKILL');
    if (exited) await exited;
  };
  t.after(async () => { await stop(); audit.close(); upstream.closeAllConnections(); await new Promise(resolve => upstream.close(resolve)); });
  const call = (endpoint, { body, cookie, headers = {} } = {}) => fetch(base + endpoint, {
    method: body === undefined ? 'GET' : 'POST', signal: AbortSignal.timeout(15000),
    headers: { origin: base, ...(cookie ? { cookie } : {}), ...(body === undefined ? {} : { 'content-type': 'application/json' }), ...headers },
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  const login = (index = 0) => call('/api/member/login', { body: { username: members[index].username, password: members[index].password } });
  const me = async cookie => {
    const response = await call('/api/member/me', { cookie });
    assert.equal(response.status, 200); assert.equal(response.headers.get('set-cookie'), null);
    return response.json();
  };
  const createOfflineSession = async (index, now = Date.now()) => {
    assert.ok(!child || child.exitCode !== null || child.signalCode !== null, 'session fixture writes require the gateway to be stopped');
    const store = new JsonStore(dataDir); await store.init();
    try {
      const user = await store.update(db => db.users.find(item => item.id === members[index].user_id), { persist: false });
      return await createMemberSession(store, user, env, now);
    } finally { await store.close(); }
  };
  const expire = cookie => audit.prepare('UPDATE member_sessions SET expires_at = ? WHERE token_hash = ?').run(Date.now() - 1, tokenHash(cookie));
  return { start, stop, call, login, me, createOfflineSession, expire, audit, members, adminToken, child: () => child, exit: () => exited,
    assertNoGeneration() { assert.ok(requests.every(url => url === '/user/data')); assert.doesNotMatch(output, /External fetch blocked/); } };
}

async function openEvents(gateway, cookie) {
  const response = await gateway.call('/api/member/shared-events', { cookie });
  assert.equal(response.status, 200); assert.match(response.headers.get('content-type'), /text\/event-stream/);
  const reader = response.body.getReader(), decoder = new TextDecoder();
  let text = '';
  while (!text.includes('event: shared-status')) {
    const chunk = await reader.read(); assert.equal(chunk.done, false); text += decoder.decode(chunk.value, { stream: true });
  }
  return async () => {
    for (;;) { const chunk = await reader.read(); if (chunk.done) break; text += decoder.decode(chunk.value, { stream: true }); }
    assert.match(text, /event: auth-expired\ndata: \{\}\n\n/);
  };
}

for (const owner of [false, true]) test(`可见网页访问续期${owner ? 'owner 一天' : '普通成员七天'}，Cookie 与数据库一致，其他查询和跨站请求不续期`, { timeout: 45000 }, async t => {
  const gateway = await fixture(t, { owner }), ttl = owner ? ownerTtl : memberTtl;
  let session = owner ? await gateway.createOfflineSession(0, Date.now() - 10000) : null;
  await gateway.start();
  let cookie = session ? cookieFor(session) : null;
  if (!cookie) { const login = await gateway.login(); cookie = login.headers.get('set-cookie').split(';')[0]; await login.json(); }
  const hash = tokenHash(cookie);
  const endpoint = owner ? '/api/member/owner/member-management' : '/api/member/me';
  const prior = Date.now() + 10000;
  gateway.audit.prepare('UPDATE member_sessions SET expires_at=? WHERE token_hash=?').run(prior, hash);
  const ordinary = await gateway.call(endpoint, { cookie });
  assert.equal(ordinary.status, 200); assert.equal(ordinary.headers.get('set-cookie'), null); await ordinary.json();
  assert.equal(gateway.audit.prepare('SELECT expires_at FROM member_sessions WHERE token_hash=?').get(hash).expires_at, prior);
  const crossSite = await gateway.call(endpoint, { cookie, headers: { 'x-member-activity': '1', origin: 'https://elsewhere.invalid' } });
  assert.equal(crossSite.status, 403); assert.equal(crossSite.headers.get('set-cookie'), null); await crossSite.text();
  assert.equal(gateway.audit.prepare('SELECT expires_at FROM member_sessions WHERE token_hash=?').get(hash).expires_at, prior);
  const before = Date.now(), visit = await gateway.call(endpoint, { cookie, headers: { 'x-member-activity': '1' } }), after = Date.now();
  assert.equal(visit.status, 200);
  const value = await visit.json(), renewed = gateway.audit.prepare('SELECT * FROM member_sessions WHERE token_hash=?').get(hash);
  assert.ok(renewed.expires_at >= before + ttl && renewed.expires_at <= after + ttl);
  assert.equal(visit.headers.get('set-cookie').split(';')[0], cookie);
  assert.equal(Date.parse(/Expires=([^;]+)/.exec(visit.headers.get('set-cookie'))[1]), Math.floor(renewed.expires_at / 1000) * 1000);
  assert.equal(Number(/Max-Age=(\d+)/.exec(visit.headers.get('set-cookie'))[1]), ttl / 1000);
  if (!owner) assert.equal(value.sessionExpiresAt, renewed.expires_at);
  const unrelated = await gateway.call('/api/member/shared-status', { cookie, headers: { 'x-member-activity': '1' } });
  assert.equal(unrelated.status, 200); assert.equal(unrelated.headers.get('set-cookie'), null); await unrelated.json();
  assert.equal(gateway.audit.prepare('SELECT expires_at FROM member_sessions WHERE token_hash=?').get(hash).expires_at, renewed.expires_at);
  gateway.audit.exec("CREATE TRIGGER synthetic_renewal_failure BEFORE UPDATE ON member_sessions BEGIN SELECT RAISE(ABORT, 'synthetic renewal failure'); END");
  try {
    const failed = await gateway.call(endpoint, { cookie, headers: { 'x-member-activity': '1' } });
    assert.equal(failed.status, 500); assert.equal(failed.headers.get('set-cookie'), null); await failed.text();
    assert.equal(gateway.audit.prepare('SELECT expires_at FROM member_sessions WHERE token_hash=?').get(hash).expires_at, renewed.expires_at);
  } finally { gateway.audit.exec('DROP TRIGGER synthetic_renewal_failure'); }
  await gateway.stop(); await gateway.start();
  assert.equal((await gateway.me(cookie)).sessionExpiresAt, renewed.expires_at);
  gateway.expire(cookie);
  const expired = await gateway.call(endpoint, { cookie, headers: { 'x-member-activity': '1' } });
  assert.equal(expired.status, 401); assert.equal(expired.headers.get('set-cookie'), null); await expired.text();
  assert.equal(gateway.audit.prepare('SELECT token_hash FROM member_sessions WHERE token_hash=?').get(hash), undefined);
  gateway.assertNoGeneration();
});

test('真实 HTTP 普通会话初始七天，刷新与强制重启保留期限，退出持久撤销并关闭 SSE', { timeout: 45000 }, async t => {
  const gateway = await fixture(t); await gateway.start();
  const before = Date.now(), login = await gateway.login(), after = Date.now();
  assert.equal(login.status, 200);
  const setCookie = login.headers.get('set-cookie'), cookie = setCookie.split(';')[0], session = await login.json();
  assert.match(setCookie, /; Path=\/api\/member; HttpOnly; SameSite=Strict;/);
  const maxAge = Number(/Max-Age=(\d+)/.exec(setCookie)?.[1]); assert.ok(maxAge >= 604798 && maxAge <= 604800);
  assert.ok(Number.isSafeInteger(session.expiresAt) && session.expiresAt >= before + memberTtl && session.expiresAt <= after + memberTtl);
  assert.equal(Date.parse(/Expires=([^;]+)/.exec(setCookie)?.[1]), Math.floor(session.expiresAt / 1000) * 1000);
  assert.equal((await gateway.me(cookie)).sessionExpiresAt, session.expiresAt);
  assert.equal((await gateway.me(cookie)).sessionExpiresAt, session.expiresAt);
  const row = gateway.audit.prepare('SELECT * FROM member_sessions WHERE token_hash = ?').get(tokenHash(cookie));
  assert.equal(row.expires_at, session.expiresAt); assert.ok(!JSON.stringify(row).includes(cookie.slice(cookie.indexOf('=') + 1)));
  await gateway.stop(); await gateway.start();
  assert.equal((await gateway.me(cookie)).sessionExpiresAt, session.expiresAt);
  const expiredEvent = await openEvents(gateway, cookie);
  const logout = await gateway.call('/api/member/logout', { body: {}, cookie });
  assert.equal(logout.status, 200); assert.match(logout.headers.get('set-cookie'), /Max-Age=0/); await logout.json();
  await expiredEvent();
  await gateway.stop(); await gateway.start();
  assert.equal((await gateway.call('/api/member/me', { cookie })).status, 401);
  gateway.assertNoGeneration();
});

test('会话写入失败不发 Cookie，浏览器持有的过期 Cookie 和密码重置会话被拒绝', { timeout: 45000 }, async t => {
  const gateway = await fixture(t); await gateway.start();
  const count = gateway.audit.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n;
  gateway.audit.exec("CREATE TRIGGER synthetic_session_failure BEFORE INSERT ON member_sessions BEGIN SELECT RAISE(ABORT, 'synthetic session storage failure'); END");
  try {
    const failed = await gateway.login(); assert.equal(failed.status, 500); assert.equal(failed.headers.get('set-cookie'), null); await failed.text();
    assert.equal(gateway.audit.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n, count);
  } finally { gateway.audit.exec('DROP TRIGGER synthetic_session_failure'); }
  const login = await gateway.login(1); assert.equal(login.status, 200);
  const cookie = login.headers.get('set-cookie').split(';')[0]; await login.json();
  const expiredEvent = await openEvents(gateway, cookie);
  assert.equal(gateway.expire(cookie).changes, 1);
  await expiredEvent();
  assert.equal((await gateway.call('/api/member/me', { cookie })).status, 401);
  const old = await gateway.login(2), unrelated = await gateway.login(3);
  assert.equal(old.status, 200); assert.equal(unrelated.status, 200); await old.json(); await unrelated.json();
  const oldCookie = old.headers.get('set-cookie').split(';')[0], unrelatedCookie = unrelated.headers.get('set-cookie').split(';')[0];
  const prior = await gateway.me(unrelatedCookie);
  const reset = await gateway.call('/api/admin/member-management', { headers: { 'x-admin-token': gateway.adminToken },
    body: { action: 'credentials', userId: gateway.members[2].user_id, username: gateway.members[2].username, password: 'synthetic-reset-password-for-session-test' } });
  assert.equal(reset.status, 200); await reset.json();
  assert.equal((await gateway.call('/api/member/me', { cookie: oldCookie })).status, 401);
  await gateway.stop(); await gateway.start();
  assert.equal((await gateway.call('/api/member/me', { cookie: oldCookie })).status, 401);
  assert.equal((await gateway.me(unrelatedCookie)).sessionExpiresAt, prior.sessionExpiresAt);
  gateway.assertNoGeneration();
});

test('owner 一天，A→B→A 不复活旧会话、普通七天 Cookie 不直接升级，无关成员不受影响', { timeout: 45000 }, async t => {
  const gateway = await fixture(t, { owner: true }), now = Date.now();
  const owner = await gateway.createOfflineSession(0, now), member = await gateway.createOfflineSession(1, now), unrelated = await gateway.createOfflineSession(2, now);
  assert.equal(owner.expiresAt, now + ownerTtl); assert.equal(member.expiresAt, now + memberTtl);
  await gateway.start();
  assert.equal((await gateway.me(cookieFor(owner))).owner, true);
  assert.equal((await gateway.me(cookieFor(owner))).sessionExpiresAt, owner.expiresAt);
  assert.equal((await gateway.me(cookieFor(member))).owner, false);
  assert.equal((await gateway.call('/api/member/owner/member-management', { cookie: cookieFor(owner) })).status, 200);
  assert.equal((await gateway.call('/api/member/owner/member-management', { cookie: cookieFor(member) })).status, 403);
  await gateway.stop(); await gateway.start(gateway.members[1].user_id);
  for (const session of [owner, member]) {
    // 启动时即撤销，不能依赖浏览器恰好在角色变化期间访问一次。
    assert.equal(gateway.audit.prepare('SELECT token_hash FROM member_sessions WHERE token_hash = ?').get(tokenHash(cookieFor(session))), undefined);
    assert.equal((await gateway.call('/api/member/me', { cookie: cookieFor(session) })).status, 401);
    assert.equal((await gateway.call('/api/member/owner/member-management', { cookie: cookieFor(session) })).status, 401);
  }
  assert.equal((await gateway.me(cookieFor(unrelated))).sessionExpiresAt, unrelated.expiresAt);
  await gateway.stop(); await gateway.start(gateway.members[0].user_id);
  assert.equal((await gateway.call('/api/member/me', { cookie: cookieFor(owner) })).status, 401);
  assert.equal((await gateway.call('/api/member/owner/member-management', { cookie: cookieFor(owner) })).status, 401);
  assert.equal((await gateway.me(cookieFor(unrelated))).sessionExpiresAt, unrelated.expiresAt);
  gateway.assertNoGeneration();
});

test('SIGTERM主动结束两类SSE订阅，无注销事件且不等满关闭宽限', { timeout: 30000, skip: process.platform === 'win32' }, async t => {
  const grace = 20000, gateway = await fixture(t, { owner: true, grace });
  const session = await gateway.createOfflineSession(0);
  await gateway.start();
  async function subscribe(route, headers, firstFrame) {
    const response = await gateway.call(route, { headers });
    assert.equal(response.status, 200); assert.match(response.headers.get('content-type'), /text\/event-stream/);
    const reader = response.body.getReader(), decoder = new TextDecoder();
    let text = '';
    while (!text.includes(firstFrame)) {
      const chunk = await reader.read(); assert.equal(chunk.done, false);
      text += decoder.decode(chunk.value, { stream: true });
    }
    // 立即观察EOF并收集错误，避免红测清理强杀后遗留未处理的读流拒绝。
    const ended = (async () => {
      for (;;) { const chunk = await reader.read(); if (chunk.done) return { text }; text += decoder.decode(chunk.value, { stream: true }); }
    })().catch(error => ({ error }));
    return { ended };
  }
  const sharedStream = await subscribe('/api/member/shared-events', { cookie: cookieFor(session) }, 'event: shared-status');
  const previewStream = await subscribe('/api/jobs/events', { authorization: 'Bearer ' + gateway.members[0].api_key }, ': connected');
  const counts = () => ['jobs', 'fair_quota_settlements', 'ledger'].map(table => gateway.audit.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n);
  const before = counts(), started = Date.now();
  gateway.child().kill('SIGTERM');
  let timer;
  try {
    const [shared, previews, exit] = await Promise.race([
      Promise.all([sharedStream.ended, previewStream.ended, gateway.exit()]),
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('空闲SSE使SIGTERM超过5000ms，仍在等待长订阅关闭')), 5000); })
    ]);
    assert.equal(shared.error, undefined, '成员状态流应正常EOF');
    assert.equal(previews.error, undefined, '任务预览流应正常EOF');
    assert.doesNotMatch(shared.text, /auth-expired/); assert.doesNotMatch(previews.text, /auth-expired/);
    assert.deepEqual(exit, [0, null]);
    const shutdownMs = Date.now() - started;
    assert.ok(shutdownMs < grace / 2);
    assert.deepEqual(counts(), before);
    gateway.assertNoGeneration();
    t.diagnostic(`SIGTERM普通EOF并退出 ${shutdownMs}ms；宽限 ${grace}ms；生成与结算计数不变`);
  } finally { clearTimeout(timer); }
});
