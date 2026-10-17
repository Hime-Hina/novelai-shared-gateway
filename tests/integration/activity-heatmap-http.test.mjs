import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { JsonStore } from '../../server/store.js';
import { createMemberSession } from '../../server/member-auth.js';
import { handleMemberRoutes } from '../../server/member-routes.js';
import { sharedSurface } from '../../server/shared-surface.ts';

async function fixture(t) {
  const env = { OWNER_USER_ID: 'heat-owner', DISCORD_GUILD_ID: '1134557553011998840', DISCORD_CLIENT_ID: '123456789012345678',
    DISCORD_CLIENT_SECRET: 'synthetic-discord-secret', PUBLIC_BASE_URL: 'https://nai.example' };
  const store = new JsonStore(await mkdtemp(path.join(tmpdir(), 'nai-heatmap-http-')));
  await store.init();
  const createdAt = new Date().toISOString();
  await store.update(db => {
    db.users = ['heat-owner', 'heat-member', 'heat-other'].map((id, index) => ({ id, token: 'STA1N-synthetic-secret-' + id, enabled: true,
      memberAuth: { username: id, version: id }, discord: { id: String(222222222222222220n + BigInt(index)), guildId: env.DISCORD_GUILD_ID,
        checkedAt: Date.now(), member: true, accessToken: 'synthetic-discord-secret' } }));
    const job = (id, memberId, status, operation = 'generate', extra = {}) => ({ id, source: 'novelai-native', status, createdAt,
      userToken: 'STA1N-synthetic-secret-' + memberId, fairCharge: { userId: memberId },
      request: { operation, model: 'synthetic-model', prompt: 'synthetic-private-prompt', image: 'synthetic-private-base64' }, ...extra });
    db.jobs = [
      job('private-child-1', 'heat-member', 'done', 'generate', { batchId: 'private-batch', sampleIndex: 0 }),
      job('private-child-2', 'heat-member', 'failed', 'generate', { batchId: 'private-batch', sampleIndex: 1, cancelledAt: createdAt, cancellationReason: 'request_cancelled' }),
      job('private-upscale', 'heat-member', 'done', 'upscale'),
      job('private-vibe', 'heat-member', 'done', 'encode-vibe'),
      job('private-other', 'heat-other', 'pending'),
      job('private-deleted', 'heat-deleted', 'failed')
    ];
  }, { immediate: true, collections: ['users', 'jobs'] });
  const cookies = await Promise.all(store.db.users.map(async user => 'nai_member_session=' + (await createMemberSession(store, user, env)).token));
  const deps = { store, env, assertAdmin(req) {
    if (req.headers['x-admin-token'] !== 'synthetic-local-admin') throw Object.assign(new Error('Private administrator required'), { statusCode: 401 });
  } };
  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url, 'https://nai.example');
      if (!sharedSurface(url.pathname, req.method).allowed) { res.writeHead(404); res.end('{}'); return; }
      await handleMemberRoutes(req, res, url, deps);
    } catch (error) { res.writeHead(error.statusCode || 500, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: error.message })); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); await store.close(); });
  const call = (route, { viewer = 1, host = 'nai.example', method = 'GET', headers = {} } = {}) => new Promise((resolve, reject) => {
    const req = http.request({ hostname: '127.0.0.1', port: server.address().port, path: route, method,
      headers: { host, origin: host === 'localhost' ? 'http://localhost' : 'https://nai.example', ...(viewer === null ? {} : { cookie: cookies[viewer] }), ...headers } }, res => {
      let text = ''; res.on('data', chunk => { text += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, data: JSON.parse(text), text }));
    });
    req.on('error', reject); req.end();
  });
  return { call, store };
}
const total = result => result.data.days.reduce((sum, day) => sum + day.total, 0);

test('热力图真实成员会话只读本人：三操作分开、每图计数、取消单列且无任务或秘密字段', async t => {
  const { call } = await fixture(t);
  assert.equal((await call('/api/member/heatmap', { viewer: null })).status, 401);
  assert.equal((await call('/api/member/heatmap', { viewer: null, headers: { authorization: 'Bearer STA1N-synthetic-secret-heat-member' } })).status, 401);
  for (const query of ['memberId=heat-other', 'memberId=heat-member', 'memberId=']) {
    assert.equal((await call('/api/member/heatmap?' + query)).status, 400);
  }
  assert.equal((await call('/api/member/heatmap', { headers: { origin: 'https://evil.example' } })).status, 403);
  assert.equal((await call('/api/member/heatmap', { method: 'POST' })).status, 404);
  assert.equal((await call('/api/member/heatmap?operation=unknown')).status, 400);
  const generated = await call('/api/member/heatmap');
  assert.equal(generated.status, 200);
  assert.equal(generated.headers['cache-control'], 'no-store');
  assert.equal(generated.data.coverage, 'retained-records');
  assert.equal(total(generated), 2);
  assert.equal(generated.data.days.reduce((sum, day) => sum + day.done, 0), 1);
  assert.equal(generated.data.days.reduce((sum, day) => sum + day.cancelled, 0), 1);
  assert.equal(generated.data.days.reduce((sum, day) => sum + day.failed, 0), 0);
  for (const operation of ['upscale', 'encode-vibe']) {
    const result = await call('/api/member/heatmap?operation=' + operation);
    assert.equal(result.status, 200); assert.equal(result.data.operation, operation); assert.equal(total(result), 1);
  }
  for (const secret of ['STA1N-', 'synthetic-private', 'synthetic-discord', 'private-child', 'private-batch']) assert.ok(!generated.text.includes(secret), secret);
  assert.equal(Object.hasOwn(generated.data, 'jobs'), false);
});

test('owner和本机admin沿用原鉴权：全局与已删除成员筛选，不接受普通成员提权', async t => {
  const { call, store } = await fixture(t);
  const owner = '/api/member/owner/member-management?view=heatmap';
  assert.equal((await call(owner, { viewer: null })).status, 401);
  assert.equal((await call(owner, { headers: { 'x-admin-token': 'synthetic-local-admin' } })).status, 403);
  const all = await call(owner, { viewer: 0 });
  assert.equal(all.status, 200); assert.equal(total(all), 4);
  const other = await call(owner + '&memberId=heat-other', { viewer: 0 });
  assert.equal(other.status, 200); assert.equal(total(other), 1); assert.equal(other.data.days[0].pending, 1);
  const deleted = await call(owner + '&memberId=heat-deleted', { viewer: 0 });
  assert.equal(deleted.status, 200); assert.equal(total(deleted), 1); assert.equal(deleted.data.days[0].failed, 1);

  const admin = '/api/admin/member-management?view=heatmap';
  assert.equal((await call(admin, { headers: { 'x-admin-token': 'synthetic-local-admin' } })).status, 404);
  assert.equal((await call(admin, { host: 'localhost', viewer: null })).status, 401);
  const local = await call(admin + '&memberId=heat-deleted', { host: 'localhost', viewer: null, headers: { 'x-admin-token': 'synthetic-local-admin' } });
  assert.equal(local.status, 200); assert.equal(total(local), 1);
  for (const secret of ['STA1N-', 'synthetic-private', 'synthetic-discord', 'private-deleted']) assert.ok(!local.text.includes(secret), secret);
  await store.update(db => { db.users[0].discord.member = false; }, { immediate: true, dirtyRows: { users: ['heat-owner'] } });
  assert.equal((await call(owner, { viewer: 0 })).status, 403);
});
