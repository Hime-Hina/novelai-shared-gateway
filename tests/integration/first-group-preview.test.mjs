import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { JsonStore } from '../../server/storage/sqlite-store.ts';
import { handleMemberRoutes } from '../../server/members/http-routes.ts';

test('首次分组预览纯读且与保存后的有效Opus钱包一致', async t => {
  const store = new JsonStore(await mkdtemp(path.join(tmpdir(), 'nai-first-preview-')));
  await store.init();
  await store.update(db => {
    db.users = ['a', 'b', 'c', 'd'].map(id => ({ id, token: `STA1N-${id}`, enabled: true, balance: 0 }));
    db.accounts = [{ id: 'opus', enabled: true, subscriptionActive: true, quotaTier: 3,
      subscriptionExpiresAt: Date.now() + 86400000, quotaCheckedAt: Date.now(), v5UsagePercent: 80,
      quotaFixed: 10000, quotaPurchased: 0 }];
  }, { immediate: true });
  const before = await store.read(), beforeRows = store.sqlite.prepare('SELECT value FROM app_settings').all();
  let commits = 0; store.events.on('commit', () => commits++);
  const server = http.createServer(async (req, res) => {
    try { await handleMemberRoutes(req, res, new URL(req.url, 'http://localhost'), { store, assertAdmin() {} }); }
    catch (error) { res.writeHead(error.statusCode || 500, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: error.message })); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); await store.close(); });
  const base = `http://127.0.0.1:${server.address().port}`;
  const call = async body => {
    const response = await fetch(base + '/api/admin/member-management', { method: body ? 'POST' : 'GET',
      headers: { origin: base, 'content-type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
    const result = await response.json(); assert.equal(response.status, 200, JSON.stringify(result)); return result;
  };
  const groups = ['a', 'b', 'c', 'd'].map(id => ({ id: `group-${id}`, members: [id], share: .25 }));
  const preview = await call({ action: 'preview-groups', groups, expectedConfigVersion: 1 });
  assert.deepEqual(await store.read(), before);
  assert.deepEqual(store.sqlite.prepare('SELECT value FROM app_settings').all(), beforeRows);
  assert.equal(commits, 0);
  for (const member of preview.users) {
    assert.equal(member.quota.status, 'active'); assert.equal(member.quota.v5Available, 20);
    assert.equal(member.quota.anlasFixedAvailable, 2500);
  }
  await call({ action: 'groups', groups, expectedConfigVersion: preview.configVersion });
  const saved = await call();
  for (const member of preview.users) {
    const actual = saved.users.find(item => item.id === member.id).quota;
    for (const field of ['status', 'v5Available', 'anlasFixedAvailable', 'anlasPurchasedAvailable', 'share']) assert.equal(actual[field], member.quota[field]);
  }
});
