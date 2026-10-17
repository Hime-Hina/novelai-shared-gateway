// Local browser baseline only: real gateway routes/static assets, synthetic users and upstreams.
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { randomBytes } from 'node:crypto';
import { bootstrap } from '../../scripts/bootstrap.mjs';
import { JsonStore } from '../../server/store.js';
import { syncFairQuota } from '../../server/fair-quota.ts';
import { createMemberSession, reconcileMemberSessionOwner } from '../../server/member-auth.ts';
import { nativeImagesMsgpack } from '../../server/native-stream.ts';

const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-browser-baseline-'));
const adminToken = randomBytes(32).toString('hex');
const adminTokenPath = path.join(dataDir, 'browser-admin-token');
await writeFile(adminTokenPath, adminToken, { mode: 0o600 });
const upstreamToken = 'pst-synthetic-browser-baseline-only';
const guildId = '1134557553011998840';
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
await bootstrap({ env: { ADMIN_TOKEN: adminToken, NOVELAI_TOKEN: upstreamToken, DATA_DIR: dataDir }, log() {} });
const seed = new JsonStore(dataDir);
let members, roleSessions;
await seed.init();
try {
  await seed.update(db => {
    const now = Date.now(), account = db.accounts[0];
    Object.assign(account, { quotaTier: 3, subscriptionActive: true, subscriptionExpiresAt: now + 86400000,
      quotaPoints: 10400, quotaFixed: 10000, quotaPurchased: 400, v5UsagePercent: 80,
      v5UsageIsNegative: false, v5UsageTimeUntilNextPercent: 0, quotaCheckedAt: new Date(now).toISOString(), upstreamStatus: 'active' });
    for (const [index, user] of db.users.entries()) {
      user.memberAuth.username = ['baseline-owner', 'baseline-member', 'baseline-third', 'baseline-fourth'][index];
      user.discord = { id: String(222222222222222220n + BigInt(index)), guildId,
        member: true, checkedAt: 0, accessToken: `synthetic-discord-${index}`,
        refreshToken: `synthetic-refresh-${index}`, expiresAt: now + 86400000 };
      // Only terminal sample records: startup must not dispatch fake historical work.
      for (let entry = 0; entry < 30; entry++) {
        const created = now - (entry % 7) * 86400000 - (entry + index + 1) * 60000;
        const failed = entry % 5 === 0;
        db.jobs.push({ id: `job-baseline-${index}-${entry}`, userToken: user.token, source: 'novelai-native',
          status: failed ? 'failed' : 'done', accountId: account.id,
          request: { model: entry % 2 ? 'nai-diffusion-5-full' : 'nai-diffusion-4-5-full', steps: 23, width: 1024, height: 1024 },
          createdAt: new Date(created).toISOString(), startedAt: new Date(created + 12000).toISOString(),
          upstreamStartedAt: new Date(created + 12000).toISOString(), upstreamCompletedAt: new Date(created + 25000).toISOString(),
          updatedAt: new Date(created + 25000).toISOString(), completedAt: new Date(created + 25000).toISOString(),
          error: failed ? 'Synthetic upstream failure' : '', errorStatus: failed ? 500 : null,
          fairCharge: { userId: user.id, resource: entry % 2 ? 'v5' : 'free', amount: entry % 2 ? 0.05 : 0,
            status: failed ? 'refunded' : 'settled', estimated: Boolean(entry % 2) } });
      }
    }
    db.jobs.sort((a, b) => b.createdAt.localeCompare(a.createdAt));
    syncFairQuota(db, now);
    members = structuredClone(db.users);
  }, { collections: ['accounts', 'users', 'jobs', 'settings'], includeSettings: true, immediate: true });
  const sessionEnv = { OWNER_USER_ID: members[0].id, DISCORD_GUILD_ID: guildId };
  await reconcileMemberSessionOwner(seed, sessionEnv);
  roleSessions = { owner: await createMemberSession(seed, members[0], sessionEnv), member: await createMemberSession(seed, members[1], sessionEnv) };
} finally { await seed.close(); }

const realFetch = globalThis.fetch;
const mocks = { discord: 0, quota: 0, generation: 0, blocked: 0 };
globalThis.fetch = async (input, init = {}) => {
  const url = new URL(input instanceof Request ? input.url : String(input));
  const headers = new Headers(init.headers || (input instanceof Request ? input.headers : undefined));
  if (url.origin === 'https://nai-baseline.invalid') {
    assert.equal(headers.get('authorization'), `Bearer ${upstreamToken}`);
    if (url.pathname === '/user/data') {
      mocks.quota++;
      return Response.json({ subscription: { tier: 3, active: true, expiresAt: Math.floor(Date.now() / 1000) + 86400,
        trainingStepsLeft: { fixedTrainingStepsLeft: 10000, purchasedTrainingSteps: 400 },
        usage: { percent: 80, isNegative: false, timeUntilNextPercent: 0 } } });
    }
    if (['/ai/generate-image', '/ai/generate-image-stream'].includes(url.pathname)) {
      mocks.generation++;
      return new Response(url.pathname.endsWith('-stream') ? nativeImagesMsgpack([png]) : png,
        { headers: { 'content-type': url.pathname.endsWith('-stream') ? 'application/octet-stream' : 'image/png' } });
    }
  }
  if (url.href === `https://discord.com/api/v10/users/@me/guilds/${guildId}/member`) {
    mocks.discord++;
    const user = members.find(member => headers.get('authorization') === `Bearer ${member.discord.accessToken}`);
    return Response.json(user ? { user: { id: user.discord.id }, pending: false } : { message: 'Synthetic identity required' }, { status: user ? 200 : 403 });
  }
  if (url.protocol === 'http:' && url.hostname === '127.0.0.1') return realFetch(input, { ...init, redirect: 'error' });
  mocks.blocked++;
  throw new Error(`Browser baseline blocked external fetch to ${url.origin}`);
};

async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}

const reservation = http.createServer();
const gatewayPort = await listen(reservation);
await new Promise(resolve => reservation.close(resolve));
const gatewayUrl = `http://127.0.0.1:${gatewayPort}`;
const nuxtUrl = process.env.BROWSER_NUXT_URL ? new URL(process.env.BROWSER_NUXT_URL) : null;
if (nuxtUrl) assert.ok(nuxtUrl.protocol === 'http:' && nuxtUrl.hostname === '127.0.0.1' && nuxtUrl.pathname === '/', 'Nuxt fixture must be a loopback HTTP origin');
Object.assign(process.env, {
  ADMIN_TOKEN: adminToken, DATA_DIR: dataDir, HOST: '127.0.0.1', PORT: String(gatewayPort),
  NOVELAI_API_URL: 'https://nai-baseline.invalid', NOVELAI_ACCOUNT_API_URL: 'https://nai-baseline.invalid',
  PUBLIC_BASE_URL: 'https://browser-baseline.invalid', PUBLIC_HOST: 'browser-baseline.invalid', PUBLIC_HOST_ALIASES: '',
  OWNER_USER_ID: members[0].id, DISCORD_GUILD_ID: guildId, DISCORD_CLIENT_ID: '123456789012345678',
  DISCORD_CLIENT_SECRET: 'synthetic-browser-secret-only', SQLITE_BATCH_WRITES: 'false'
});
// 会话已在启动网关前写入合成数据库；运行期间没有第二个写入连接或认证旁路。
await import('../../server/index.js');
await realFetch(gatewayUrl + '/api/health');

let baseUrl;
const front = http.createServer((req, res) => {
  if (req.headers.host !== new URL(baseUrl).host) { res.writeHead(403); res.end(); return; }
  const pathname = new URL(req.url, baseUrl).pathname;
  const role = pathname.match(/^\/__baseline\/(anonymous|member|owner)$/)?.[1];
  if (role && req.method === 'GET') {
    const session = role === 'anonymous' ? null : roleSessions[role];
    const destination = role === 'owner' ? '/owner' : '/';
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store',
      'set-cookie': `nai_member_session=${session?.token || ''}; Path=/api/member; HttpOnly; SameSite=Strict; Max-Age=${session ? Math.max(0,Math.ceil((session.expiresAt-Date.now())/1000)) : 0}` });
    // A real same-origin click resets the navigation context; a redirect retains cross-site provenance.
    res.end(`<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>合成角色入口</title><h1>${role}</h1><p>仅使用本机合成数据。点击后在目标页面采样。</p><a href="${destination}">进入${role === 'owner' ? 'owner 控制台' : '成员页面'}</a></html>`); return;
  }
  if (pathname === '/__baseline/info' && req.method === 'GET') {
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ pid: process.pid, dataDir, gatewayUrl, baseUrl, members: 4, terminalJobs: 120, mocks })); return;
  }
  // Browser OAuth itself is excluded; never navigate a real Discord authorization flow.
  if (pathname.startsWith('/api/member/discord/')) {
    res.writeHead(503, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ error: 'OAuth is disabled in the local browser baseline; use a synthetic role entry.' })); return;
  }
  const frontend = nuxtUrl && (['/', '/account.html', '/owner', '/owner/', '/owner.html', '/admin', '/admin/', '/admin.html', '/admin/members.html'].includes(pathname) || pathname.startsWith('/_nuxt/'));
  const upstream = http.request((frontend ? nuxtUrl.origin : gatewayUrl) + req.url, { method: req.method, headers: req.headers }, reply => {
    if (pathname === '/api/member/login-options' && req.method === 'GET' && reply.statusCode === 200) {
      const chunks = [];
      reply.on('data', chunk => chunks.push(chunk));
      reply.on('end', () => {
        const options = JSON.parse(Buffer.concat(chunks).toString('utf8'));
        // The only business-response adaptation: avoid the production HTTPS origin redirect locally.
        const body = Buffer.from(JSON.stringify({ ...options, origin: baseUrl }));
        res.writeHead(reply.statusCode, { ...reply.headers, 'content-length': body.length }); res.end(body);
      });
    } else { res.writeHead(reply.statusCode, reply.headers); reply.pipe(res); }
  });
  upstream.on('error', () => { if (!res.headersSent) res.writeHead(502); res.end(); });
  res.on('close', () => upstream.destroy());
  req.pipe(upstream);
});
const frontPort = await listen(front);
baseUrl = `http://127.0.0.1:${frontPort}`;

// Check actual HTTP routes before handing the environment to a browser.
const loginOptions = await (await realFetch(baseUrl + '/api/member/login-options')).json();
assert.deepEqual(loginOptions, { discord: true, ready: true, origin: baseUrl });
assert.equal((await realFetch(baseUrl + '/api/member/me')).status, 401);
assert.equal((await realFetch(baseUrl + '/api/admin/ping', { headers: { 'x-admin-token': adminToken } })).status, 200);
assert.equal((await realFetch(baseUrl + '/api/admin/ping')).status, 401);
for (const role of ['anonymous', 'member', 'owner']) {
  const entry = await realFetch(`${baseUrl}/__baseline/${role}`, { redirect: 'manual' });
  assert.equal(entry.status, 200); assert.equal(entry.headers.get('location'), null);
  assert.ok((await entry.text()).includes(`href="${role === 'owner' ? '/owner' : '/'}"`));
  const headers = { cookie: entry.headers.get('set-cookie').split(';', 1)[0] };
  const profile = await realFetch(baseUrl + '/api/member/me', { headers });
  if (role === 'anonymous') { assert.equal(profile.status, 401); continue; }
  assert.equal(profile.status, 200);
  const member = await profile.json();
  assert.equal(member.owner, role === 'owner'); assert.equal(member.jobs.length, 30);
  const stats = await realFetch(baseUrl + '/api/member/owner/statistics?days=7', { headers });
  assert.equal(stats.status, role === 'owner' ? 200 : 403);
  if (role === 'owner') assert.equal((await stats.json()).totals.total, 120);
}
for (const asset of ['/', '/account.html', '/account.js', '/members.css', '/shared-status.js', '/owner', '/owner/', '/owner.html', '/admin', '/admin/', '/admin.html', '/admin/members.html', '/management.js']) {
  assert.equal((await realFetch(baseUrl + asset)).status, 200, asset);
}
await assert.rejects(fetch('https://forbidden-baseline.invalid/'), /blocked external/);
assert.equal(mocks.generation, 0);
console.log('[browser-baseline-ready] ' + JSON.stringify({ pid: process.pid, baseUrl, gatewayUrl, dataDir,
  admin: `${baseUrl}/admin`, adminTokenPath,
  anonymous: `${baseUrl}/__baseline/anonymous`, member: `${baseUrl}/__baseline/member`, owner: `${baseUrl}/__baseline/owner`,
  note: 'Use entries sequentially and click the same-origin link before measuring the destination page. OAuth is excluded; only login-options.origin is adapted.' }));
