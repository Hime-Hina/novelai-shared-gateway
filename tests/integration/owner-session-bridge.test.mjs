import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { JsonStore } from '../../server/store.ts';
import { authenticateMemberSession, logoutMemberSession } from '../../server/member-auth.ts';
import { captureOwnerSession, importOwnerSession } from '../../scripts/bridge-owner-session.mjs';

const env = { OWNER_USER_ID: 'owner', DISCORD_GUILD_ID: 'guild', PUBLIC_BASE_URL: 'https://gateway.example.com' };
const token = 'a'.repeat(43), now = 1000000, configHash = 'synthetic-config-hash';
async function setup(t) {
  const directory = await mkdtemp(path.join(tmpdir(), 'owner-bridge-'));
  const store = new JsonStore(directory);
  t.after(async () => { await store.close(); await rm(directory, { recursive: true, force: true }); });
  await store.init();
  await store.update(db => { db.users = [{ id: 'owner', enabled: true, token: 'synthetic-api-key',
    memberAuth: { username: 'owner', version: 'v1' }, discord: { id: 'discord-owner', guildId: 'guild' } }]; }, { collections: ['users'], immediate: true });
  const capture = options => captureOwnerSession({ database: store.sqlite, token, expiresAt: now + 3600000, env, configHash, now,
    requestImpl: async (url, options) => {
      assert.equal(url.href, 'http://127.0.0.1:18081/api/member/me');
      assert.equal(options.headers.Cookie, 'nai_member_session=' + token);
      return { status: 200, json: async () => ({ owner: true, username: 'owner' }) };
    }, ...options });
  const receipt = await capture();
  const apply = options => importOwnerSession({ store, receipt, env, configHash, claimFile: path.join(directory, 'receipt.used'), now: now + 1000, ...options });
  return { store, directory, capture, receipt, apply };
}

test('verified Cookie survives import/restart with its original expiry and without a plaintext credential', async t => {
  const { store, receipt, apply, directory } = await setup(t);
  assert.equal(JSON.stringify(receipt).includes(token), false);
  assert.equal((await apply()).imported, true);
  assert.equal((await authenticateMemberSession(store, token, env, now + 2000)).expiresAt, receipt.expiresAt);
  assert.equal((await apply()).alreadyPresent, true);
  assert.equal((await readFile(path.join(directory, 'receipt.used'), 'utf8')).includes(token), false);
  await store.close(); await store.init();
  assert.equal((await authenticateMemberSession(store, token, env, now + 3000)).user.id, 'owner');
  await logoutMemberSession(store, token);
  await assert.rejects(apply(), /cannot recreate/);
  assert.equal(await authenticateMemberSession(store, token, env, now + 4000), null);
});

test('legacy rejection, non-owner identity and expired Cookie never produce a migration receipt', async t => {
  const { capture, store } = await setup(t);
  await assert.rejects(capture({ requestImpl: async () => ({ status: 401 }) }), /authenticate/);
  await assert.rejects(capture({ requestImpl: async () => ({ status: 200, json: async () => ({ owner: false, username: 'owner' }) }) }), /Owner changed/);
  await assert.rejects(capture({ expiresAt: now }), /expired/);
  await assert.rejects(capture({ baseUrl: 'https://untrusted.example' }), /loopback/);
  await assert.rejects(capture({ requestImpl: async () => {
    await store.update(db => { db.users[0].memberAuth.version = 'changed-during-check'; }, { collections: ['users'], immediate: true });
    return { status: 200, json: async () => ({ owner: true, username: 'owner' }) };
  } }), /Owner changed/);
});

test('real loopback HTTP preserves the public Host, Origin and existing Cookie', async t => {
  const { store } = await setup(t);
  let authenticated = false;
  const server = http.createServer((req, res) => {
    authenticated = req.headers.host === 'gateway.example.com' && req.headers.origin === env.PUBLIC_BASE_URL
      && req.headers.cookie === 'nai_member_session=' + token;
    res.writeHead(authenticated ? 200 : 403, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ owner: true, username: 'owner' }));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const receipt = await captureOwnerSession({ database: store.sqlite, token, expiresAt: now + 3600000,
    env, configHash, now, baseUrl: `http://127.0.0.1:${server.address().port}` });
  assert.equal(authenticated, true);
  assert.equal(receipt.ownerId, 'owner');
});

test('freshness, config, role and authorization checks reject changed state before insertion', async t => {
  const { store, apply, receipt } = await setup(t);
  await assert.rejects(apply({ now: now + 300001 }), /stale/);
  await assert.rejects(apply({ now: now - 1 }), /stale/);
  await assert.rejects(apply({ configHash: 'changed' }), /invalid/);
  await assert.rejects(apply({ receipt: { ...receipt, expiresAt: now + 86400000 } }), /invalid/);
  await assert.rejects(apply({ env: { ...env, OWNER_USER_ID: 'someone-else' } }), /identity/);
  await store.update(db => { db.users[0].memberAuth.version = 'v2'; }, { collections: ['users'], immediate: true });
  await assert.rejects(apply(), /authorization changed/);
  assert.equal(store.sqlite.prepare('SELECT count(*) AS n FROM member_sessions').get().n, 0);
});

test('failed database insert consumes the receipt and does not turn a retry into a new login', async t => {
  const { store, apply } = await setup(t);
  store.sqlite.exec("CREATE TEMP TRIGGER reject_bridge BEFORE INSERT ON member_sessions BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END");
  await assert.rejects(apply(), /synthetic failure/);
  store.sqlite.exec('DROP TRIGGER reject_bridge');
  await assert.rejects(apply(), /cannot recreate/);
  assert.equal(await authenticateMemberSession(store, token, env, now + 2000), null);
});
