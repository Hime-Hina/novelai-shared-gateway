import { initializeFixtureConfiguration } from '../helpers/relational-configuration.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import http from 'node:http';
import { once, EventEmitter } from 'node:events';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fixture, keys, NOW, databaseState } from '../helpers/relational-generation.mjs';
import {
  authenticateMemberSession,
  authenticateMemberApiKey,
} from '../../server/members/authentication.ts';
import { requireDiscordMembership } from '../../server/members/discord-auth.ts';
import { renewMemberCookie } from '../../server/members/authentication-http.ts';
import { memberCookie, assertMemberSameOrigin } from '../../server/members/http-request.ts';
import { createRelationalMemberAuthenticationHttp } from '../../server/members/relational-authentication-http.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { sharedSurface } from '../../server/http/api-surface.ts';
import { publicErrorMessage } from '../../server/http/public-errors.ts';

const password = 'synthetic-http-password-12345';
const discordEnv = {
  DISCORD_CLIENT_ID: '123456789012345678',
  DISCORD_CLIENT_SECRET: 'synthetic-private-client-secret',
  DISCORD_GUILD_ID: '1134557553011998840',
  PUBLIC_BASE_URL: 'https://auth.example',
  OWNER_USER_ID: 'm1',
};
let sequence = 0;
async function setup(t, env = {}) {
  const f = await fixture(t),
    username = `http.member.${++sequence}`;
  const prepared = await f.members.prepareMemberCredentials('m1', { username, password });
  f.db.transaction(() => f.members.setMemberCredentials(prepared, null))();
  await f.accept('existing');
  f.dispatch('existing-0', NOW);
  await f.publish('existing-0');
  const sourcePath = path.join(f.dir, 'auth-selected.sqlite');
  initializeFixtureConfiguration(f.db);
  await f.db.backup(sourcePath);
  const sealed = new Database(sourcePath);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  await verifyAndInstallRelationalStorage(f.db, {
    sourcePath,
    sourceSha256: createHash('sha256')
      .update(await readFile(sourcePath))
      .digest('hex'),
    historyId: 'generation-history',
    finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get()
      .seq,
    replicas: { dataDir: f.dir },
  });
  const upstream = { member: true, fail: false, calls: 0, beforeGrant: undefined };
  const discordFetch = async (url, options) => {
    assert.equal(f.db.inTransaction, false, 'OAuth 网络不能持有 SQL 事务');
    upstream.calls++;
    if (upstream.fail) {
      return new Response('private remote diagnostic', { status: 503 });
    }
    if (url.endsWith('/oauth2/token')) {
      assert.equal(options.body.get('client_secret'), env.DISCORD_CLIENT_SECRET);
      return Response.json({
        access_token: 'synthetic-private-access',
        refresh_token: 'synthetic-private-refresh',
        expires_in: 3600,
        token_type: 'Bearer',
        scope: 'identify guilds.members.read',
      });
    }
    if (url.endsWith('/users/@me')) {
      return Response.json({ id: '222222222222222222' });
    }
    await upstream.beforeGrant?.();
    return upstream.member
      ? Response.json({ user: { id: '222222222222222222' } })
      : new Response('', { status: 404 });
  };
  const handle = createRelationalMemberAuthenticationHttp({
    sqlite: f.db,
    events: new EventEmitter(),
    env,
    discordFetch,
  });
  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url, `http://${req.headers.host}`);
      if (!sharedSurface(url.pathname, req.method).allowed) {
        res.writeHead(404);
        res.end();
        return;
      }
      if (await handle(req, res, url)) {
        return;
      }
      // 使用原可见 me 续期顺序验证导出的 helper；不在 auth factory 新增 me 路由。
      if (req.method === 'GET' && url.pathname === '/api/member/me') {
        assertMemberSameOrigin(req);
        let session = await authenticateMemberSession(f.members, memberCookie(req), env);
        if (!session) {
          throw Object.assign(new Error('请先登录'), { statusCode: 401 });
        }
        await requireDiscordMembership(session.user, { store: f.members, env, discordFetch });
        if (req.headers['x-member-activity'] === '1') {
          session = await renewMemberCookie(req, res, f.members, env);
        }
        res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
        res.end(JSON.stringify({ sessionExpiresAt: session.expiresAt }));
        return;
      }
      res.writeHead(404);
      res.end();
    } catch (error) {
      res.writeHead(error.statusCode || 500, {
        'content-type': 'application/json',
        'cache-control': 'no-store',
      });
      res.end(JSON.stringify({ error: publicErrorMessage(error.message) }));
    }
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => {
    server.closeAllConnections();
    return new Promise((resolve) => server.close(resolve));
  });
  const host = env.PUBLIC_BASE_URL
    ? new URL(env.PUBLIC_BASE_URL).host
    : `127.0.0.1:${server.address().port}`;
  const origin = env.PUBLIC_BASE_URL ? new URL(env.PUBLIC_BASE_URL).origin : `http://${host}`;
  const call = (url, { method = 'GET', body, headers = {} } = {}) =>
    new Promise((resolve, reject) => {
      const req = http.request(
        {
          hostname: '127.0.0.1',
          port: server.address().port,
          path: url,
          method: body === undefined ? method : 'POST',
          headers: {
            host,
            origin,
            ...(body === undefined ? {} : { 'content-type': 'application/json' }),
            ...headers,
          },
        },
        (res) => {
          const chunks = [];
          res.on('data', (chunk) => chunks.push(chunk));
          res.on('end', () =>
            resolve({
              status: res.statusCode,
              headers: res.headers,
              text: Buffer.concat(chunks).toString(),
            }),
          );
        },
      );
      req.on('error', reject);
      req.end(body === undefined ? undefined : JSON.stringify(body));
    });
  const login = async () => {
    const response = await call('/api/member/login', { body: { username, password } });
    assert.equal(response.status, 200, response.text);
    const cookie = response.headers['set-cookie'][0].split(';')[0];
    return { cookie, token: cookie.split('=')[1], response };
  };
  const start = async (invite) => {
    const response = await call('/api/member/discord/start', { body: invite ? { invite } : {} });
    assert.equal(response.status, 200, response.text);
    return {
      state: new URL(JSON.parse(response.text).authorizationUrl).searchParams.get('state'),
      cookie: response.headers['set-cookie'][0].split(';')[0],
    };
  };
  const finish = (pending, headers = {}) =>
    call(
      '/api/member/discord/callback?' +
        new URLSearchParams({ state: pending.state, code: 'synthetic-code' }),
      {
        headers: {
          cookie: pending.cookie,
          origin: 'https://discord.com',
          'sec-fetch-site': 'cross-site',
          ...headers,
        },
      },
    );
  return { ...f, env, upstream, username, call, login, start, finish };
}

test('合法 ready HTTP 密码登录/退出共用原 owner，Cookie只保存摘要且响应没有私有凭据', async (t) => {
  const building = new Database(':memory:');
  initializeRelationalStorage(building, 'auth-building');
  assert.throws(() => createRelationalMemberAuthenticationHttp({ sqlite: building }), /incomplete/);
  building.close();
  const f = await setup(t);
  const before = databaseState(f.db),
    options = await f.call('/api/member/login-options');
  assert.equal(options.status, 200);
  assert.deepEqual(JSON.parse(options.text), { discord: false, ready: false, origin: null });
  assert.equal(databaseState(f.db), before);
  assert.equal(
    (
      await f.call('/api/member/login', {
        body: { username: f.username, password },
        headers: { origin: 'https://other.invalid' },
      })
    ).status,
    403,
  );
  assert.equal(
    (await f.call('/api/member/login', { body: { username: f.username, password: 'wrong' } }))
      .status,
    401,
  );
  const signed = await f.login(),
    row = f.db.prepare('SELECT * FROM member_sessions').get();
  assert.equal(row.token_hash, createHash('sha256').update(signed.token).digest('hex'));
  assert.match(
    signed.response.headers['set-cookie'][0],
    /Path=\/api\/member; HttpOnly; SameSite=Strict/,
  );
  assert.doesNotMatch(signed.response.headers['set-cookie'][0], /; Secure/);
  assert.deepEqual(Object.keys(JSON.parse(signed.response.text)).sort(), ['expiresAt', 'ok']);
  assert.equal((await authenticateMemberSession(f.members, signed.token, {})).user.id, 'm1');
  for (const secret of [password, keys.m1, signed.token, row.token_hash]) {
    assert.equal(signed.response.text.includes(secret), false);
  }
  const logout = await f.call('/api/member/logout', {
    body: {},
    headers: { cookie: signed.cookie },
  });
  assert.equal(logout.status, 200);
  assert.match(logout.headers['set-cookie'][0], /Max-Age=0/);
  assert.equal(await authenticateMemberSession(f.members, signed.token, {}), null);
  assert.equal((await authenticateMemberApiKey(f.members, keys.m1)).id, 'm1');
  assert.equal(
    (await f.call('/api/member/logout', { body: {}, headers: { cookie: signed.cookie } })).status,
    200,
  );
});

test('认证 HTTP SQL失败不发成功Cookie，活动续期复用原帮助函数且撤销会话不能复活', async (t) => {
  const f = await setup(t);
  f.db.exec(
    "CREATE TEMP TRIGGER fail_session BEFORE INSERT ON member_sessions BEGIN SELECT RAISE(ABORT,'synthetic-session-failure'); END",
  );
  const before = databaseState(f.db),
    failed = await f.call('/api/member/login', { body: { username: f.username, password } });
  assert.equal(failed.status, 500);
  assert.equal(failed.headers['set-cookie'], undefined);
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER fail_session');
  const signed = await f.login();
  const original = f.db.prepare('SELECT * FROM member_sessions').get();
  f.db.prepare('UPDATE member_sessions SET expires_at=?').run(Date.now() + 60000);
  const quiet = await f.call('/api/member/me', { headers: { cookie: signed.cookie } });
  assert.equal(quiet.status, 200);
  assert.equal(quiet.headers['set-cookie'], undefined);
  const visible = await f.call('/api/member/me', {
    headers: { cookie: signed.cookie, 'x-member-activity': '1' },
  });
  assert.equal(visible.status, 200);
  assert.ok(JSON.parse(visible.text).sessionExpiresAt > Date.now() + 6 * 86400000);
  assert.equal(
    f.db.prepare('SELECT created_at FROM member_sessions').get().created_at,
    original.created_at,
  );
  await f.call('/api/member/logout', { body: {}, headers: { cookie: signed.cookie } });
  const revoked = await f.call('/api/member/me', {
    headers: { cookie: signed.cookie, 'x-member-activity': '1' },
  });
  assert.equal(revoked.status, 401);
  assert.equal(revoked.headers['set-cookie'], undefined);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n, 0);
});

test('认证 HTTP 沿用JSON限额和按原用户名限流，密码入口不能使用Bearer Key替代', async (t) => {
  const f = await setup(t);
  assert.equal(
    (await f.call('/api/member/login', { body: { padding: 'x'.repeat(32768) } })).status,
    413,
  );
  assert.equal(
    (
      await f.call('/api/member/login', {
        body: {},
        headers: { authorization: `Bearer ${keys.m1}` },
      })
    ).status,
    401,
  );
  for (let n = 0; n < 10; n++) {
    assert.equal(
      (await f.call('/api/member/login', { body: { username: f.username, password: 'wrong' } }))
        .status,
      401,
    );
  }
  const limited = await f.call('/api/member/login', { body: { username: f.username, password } });
  assert.equal(limited.status, 429);
  assert.equal(limited.headers['set-cookie'], undefined);
});

test('认证 HTTP 保留Only Discord、一次性state/Cookie/Host及仅callback跨站例外', async (t) => {
  const f = await setup(t, discordEnv),
    options = await f.call('/api/member/login-options');
  assert.deepEqual(JSON.parse(options.text), {
    discord: true,
    ready: true,
    origin: 'https://auth.example',
  });
  assert.equal(
    (await f.call('/api/member/login', { body: { username: f.username, password } })).status,
    403,
  );
  assert.equal(
    (
      await f.call('/api/member/discord/start', {
        body: {},
        headers: { origin: 'https://discord.com' },
      })
    ).status,
    403,
  );
  const invite = f.db.transaction(() => f.members.createMemberDiscordInvite('m1'))().code;
  const pending = await f.start(invite);
  const invalid = await f.finish(pending, { cookie: 'nai_discord_state=wrong' });
  assert.equal(invalid.status, 400);
  assert.equal(f.upstream.calls, 0);
  assert.equal((await f.finish(pending, { host: 'wrong.example' })).status, 400);
  const signed = await f.finish(pending);
  assert.equal(signed.status, 303, signed.text);
  assert.equal(signed.headers.location, 'https://auth.example/');
  assert.equal(signed.headers['set-cookie'].length, 2);
  assert.match(signed.headers['set-cookie'][0], /Path=\/api\/member\/discord;.*Max-Age=0/);
  assert.match(
    signed.headers['set-cookie'][1],
    /Path=\/api\/member; HttpOnly; SameSite=Strict;.*; Secure$/,
  );
  assert.equal((await f.finish(pending)).status, 400);
  const memberCookie = signed.headers['set-cookie'][1].split(';')[0],
    token = memberCookie.split('=')[1];
  const session = await authenticateMemberSession(f.members, token, discordEnv);
  assert.equal(session.user.id, 'm1');
  assert.ok(session.expiresAt <= Date.now() + 86400000);
  for (const secret of [
    'synthetic-private-access',
    'synthetic-private-refresh',
    discordEnv.DISCORD_CLIENT_SECRET,
    password,
    keys.m1,
  ]) {
    assert.equal(JSON.stringify(signed.headers).includes(secret), false);
  }
});

test('认证 HTTP OAuth网络或当前成员状态变化后不签发会话，错误响应不泄漏远端原文', async (t) => {
  const f = await setup(t, discordEnv),
    invite = f.db.transaction(() => f.members.createMemberDiscordInvite('m1'))().code;
  f.upstream.fail = true;
  const unavailable = await f.finish(await f.start(invite));
  assert.equal(unavailable.status, 503);
  assert.equal(
    unavailable.headers['set-cookie'].some((cookie) => cookie.startsWith('nai_member_session=')),
    false,
  );
  assert.equal(unavailable.text.includes('private remote diagnostic'), false);
  f.upstream.fail = false;
  const pending = await f.start(invite);
  f.upstream.beforeGrant = () =>
    f.db.transaction(() =>
      f.members.setMemberStatus(
        {
          commandId: 'disable-during-oauth',
          fingerprint: 'd'.repeat(64),
          actorKind: 'system',
          actorId: null,
          recordedAt: Date.now(),
          correlationId: null,
          causationId: null,
          policyVersion: 'auth-test',
          pricingVersion: 'none',
        },
        'm1',
        'disabled',
      ),
    )();
  const disabled = await f.finish(pending);
  assert.equal(disabled.status, 403);
  assert.equal(
    disabled.headers['set-cookie'].some((cookie) => cookie.startsWith('nai_member_session=')),
    false,
  );
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n, 0);
});
