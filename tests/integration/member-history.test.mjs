import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { JsonStore } from '../../server/store.ts';
import { createMemberSession } from '../../server/member-auth.ts';
import { handleMemberRoutes } from '../../server/member-routes.ts';
import { summarizeOwnerUsage } from '../../server/owner-operations.ts';
import { sharedSurface } from '../../server/shared-surface.ts';

test('完整持久历史可分页、筛选和排序，认证与图片归属不被查询参数绕过', async t => {
  const store = new JsonStore(await mkdtemp(path.join(tmpdir(), 'nai-history-'))); await store.init();
  t.after(() => store.close());
  const user = { id: 'a', token: 'STA1N-private-a', enabled: true, memberAuth: { username: 'Alice', version: '1' } };
  await store.update(db => {
    db.users.push(user, { ...user, id: 'b', token: 'STA1N-private-b' });
    for (let i = 0; i < 81; i++) db.jobs.push({ id: `job-${String(i).padStart(3, '0')}`, userToken: i === 80 ? 'STA1N-private-b' : user.token,
      status: i % 2 ? 'done' : 'failed', createdAt: new Date(Date.now() - (100 - i) * 60000).toISOString(),
      request: { model: i % 2 ? 'v45' : 'v5', prompt: 'private-prompt' }, imageId: 'foreign-image',
      fairCharge: { userId: user.id, resource: 'free', amount: 0, status: 'settled' } });
    db.images.push({ id: 'foreign-image', token: 'STA1N-private-b' });
  }, { collections: ['users', 'jobs', 'images'], immediate: true });
  const all = await store.readOwnerJobRows('', new Date().toISOString());
  assert.equal(summarizeOwnerUsage(all).recent.length, 81);
  const cookie = 'nai_member_session=' + (await createMemberSession(store, user, {})).token;
  await store.update(db => { db.jobs = []; }, { persist: false });
  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url, 'http://' + req.headers.host);
      assert.equal(sharedSurface(url.pathname, req.method).allowed, true);
      await handleMemberRoutes(req, res, url, { store, env: {}, publicImageUrl: id => '/api/images/' + id + '/content' });
    } catch (error) { res.writeHead(error.statusCode || 500); res.end(JSON.stringify({ error: error.message })); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => { server.closeAllConnections(); server.close(); });
  const call = (query = '', authenticated = true) => fetch(`http://127.0.0.1:${server.address().port}/api/member/history${query}`, { headers: authenticated ? { cookie } : {} });
  assert.equal((await call('', false)).status, 401);
  const first = await (await call()).json();
  assert.equal(first.total, 80); assert.equal(first.jobs.length, 25); assert.equal(first.jobs[0].id, 'job-079');
  const ids = [];
  for (let page = 1; page <= 4; page++) {
    const result = await (await call(`?page=${page}&until=${first.until}&memberId=b`)).json();
    assert.equal(result.total, 80); ids.push(...result.jobs.map(job => job.id));
    assert.ok(result.jobs.every(job => job.imageUrl === ''));
    for (const secret of ['STA1N-', 'private-prompt', 'userId', 'imageId']) assert.ok(!JSON.stringify(result).includes(secret));
  }
  assert.equal(new Set(ids).size, 80);
  const filtered = await (await call('?status=done&sort=createdAt&order=asc&pageSize=100&q=v45')).json();
  assert.equal(filtered.total, 40); assert.equal(filtered.jobs[0].id, 'job-001');
  assert.equal((await (await call('?q=%25')).json()).total, 0);
  for (const query of ['?page=0', '?pageSize=101', '?sort=token', '?order=bad', '?until=bad', '?status=invalid']) assert.equal((await call(query)).status, 400);
});
