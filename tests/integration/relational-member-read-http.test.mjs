import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createServer } from 'node:http';
import { once } from 'node:events';
import path from 'node:path';
import { fixture, meta, keys, NOW, databaseState } from '../helpers/relational-generation.mjs';
import {
  authenticateMemberApiKey,
  createMemberSession,
  withAuthenticatedMemberApiKey,
  logoutMemberSession,
} from '../../server/members/authentication.ts';
import { createRelationalMemberReadHttp } from '../../server/members/relational-read-http.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { assertRelationalStorageReady } from '../../server/storage/relational-storage.ts';
import { relationalSnapshotSha256 } from '../../server/storage/relational-private-restore.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { publicErrorMessage } from '../../server/http/public-errors.ts';

async function setup(t, { env = {}, beforeReady, discordFetch } = {}) {
  const cleanup = [],
    f = await fixture({ after: (fn) => cleanup.push(fn) }, { seconds: 0, env }),
    sessions = {};
  await f.accept('existing');
  f.dispatch('existing-0');
  await f.publish('existing-0');
  await beforeReady?.(f);
  for (const id of ['m1', 'm2'])
    sessions[id] = await createMemberSession(
      f.members,
      await authenticateMemberApiKey(f.members, keys[id]),
      env,
      NOW,
    );
  const sourcePath = path.join(f.dir, 'read-http-selected.sqlite');
  await f.db.backup(sourcePath);
  const sealed = new Database(sourcePath);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  await verifyAndInstallRelationalStorage(f.db, {
    sourcePath,
    sourceSha256: relationalSnapshotSha256(sourcePath),
    historyId: 'generation-history',
    finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get()
      .seq,
    replicas: { dataDir: f.dir },
  });
  assertRelationalStorageReady(f.db);
  const signed = [],
    configuration = { publicBaseUrl: 'https://entry.example/root///' };
  const handle = createRelationalMemberReadHttp({
    sqlite: f.db,
    env,
    discordFetch,
    get publicBaseUrl() {
      return configuration.publicBaseUrl;
    },
    now: () => NOW + 30000,
    // 本测试只记录已授权 URL capability 调用；15 分钟签名及图片端点总装配另验。
    publicImageUrl: (id) => {
      signed.push(id);
      return `/configured-image/${encodeURIComponent(id)}`;
    },
  });
  const server = createServer(async (req, res) => {
    try {
      if (!(await handle(req, res, new URL(req.url, `http://${req.headers.host}`)))) {
        res.writeHead(404);
        res.end();
      }
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
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    for (const close of cleanup.reverse()) await close();
  });
  const origin = `http://127.0.0.1:${server.address().port}`;
  const call = async (
    url,
    { member = 'm1', cookie = sessions[member].token, headers = {}, method = 'GET' } = {},
  ) => {
    const response = await fetch(origin + url, {
      method,
      headers: {
        cookie: `nai_member_session=${cookie}`,
        origin,
        ...headers,
      },
    });
    const text = await response.text();
    return {
      status: response.status,
      headers: response.headers,
      body: text ? JSON.parse(text) : null,
      text,
    };
  };
  return { ...f, sessions, signed, configuration, handle, call };
}

test('合法 ready 的真实 history 子页与 late 查询复用原解析/DTO，私有字段不出网且普通读取不续期', async (t) => {
  const f = await setup(t, {
    beforeReady: async (f) => {
      await f.accept('batch', { samples: 3, now: NOW + 20000 });
      f.dispatch('batch-0', NOW + 20001);
      f.fail('batch-0', { aborted: false }, NOW + 20002);
      await f.publish('batch-0', NOW + 20003);
      await f.accept('foreign', { memberId: 'm2', now: NOW + 30000 });
    },
  });
  const before = databaseState(f.db),
    statements = [],
    prepare = f.db.prepare;
  f.db.prepare = function (sql) {
    statements.push(sql);
    return prepare.call(this, sql);
  };
  let first, second, late;
  try {
    first = await f.call('/api/member/history?pageSize=2&x=ignored', {
      headers: { 'x-member-activity': '1' },
    });
    second = await f.call(
      `/api/member/history?page=2&pageSize=2&until=${encodeURIComponent(first.body.until)}&memberId=m2`,
    );
    late = await f.call('/api/member/history?status=late');
    assert.equal((await f.call('/api/member/history?q=%20BATCH%20')).body.total, 3);
    assert.equal((await f.call('/api/member/history?q=%25')).body.total, 0);
  } finally {
    f.db.prepare = prepare;
  }
  assert.equal(first.status, 200);
  assert.equal(first.body.total, 4);
  assert.equal(first.body.page, 1);
  assert.equal(first.body.pageSize, 2);
  assert.equal(new Set([...first.body.jobs, ...second.body.jobs].map((j) => j.id)).size, 4);
  assert.deepEqual(
    late.body.jobs.map((j) => j.id),
    ['batch-0'],
  );
  assert.equal(late.body.jobs[0].status, 'failed');
  assert.equal(late.body.jobs[0].charge.status, 'refunded');
  assert.equal(late.body.jobs[0].lateResultAvailable, true);
  assert.equal(late.body.jobs[0].imageUrl, '/configured-image/artifact-batch-0');
  assert.equal((await f.call('/api/member/history', { member: 'm2' })).body.total, 1);
  assert.equal(first.headers.get('cache-control'), 'no-store');
  assert.equal(first.headers.get('x-content-type-options'), 'nosniff');
  assert.equal(first.headers.has('set-cookie'), false);
  assert.ok(f.signed.every((id) => ['artifact-existing-0', 'artifact-batch-0'].includes(id)));
  for (const response of [first, second, late])
    for (const secret of [
      'synthetic-key',
      'private generation prompt',
      'synthetic-upstream-pst',
      'imageId',
      'original_member_id',
      'userToken',
    ])
      assert.equal(response.text.includes(secret), false);
  assert.equal(
    statements.some((sql) => /generation_inputs|input_assets|\bbody\b|\bbytes\b/i.test(sql)),
    false,
  );
  assert.equal(databaseState(f.db), before);
  for (const query of [
    'page=0',
    'pageSize=101',
    'status=cancelled',
    'sort=token',
    'order=bad',
    'until=bad',
    'q=' + 'x'.repeat(201),
  ]) {
    assert.equal((await f.call('/api/member/history?' + query)).status, 400);
  }
});

test('真实 key 只返回当前私有 Key 与明确配置地址，轮换不改变原Cookie/历史、不补造缺失内容', async (t) => {
  const f = await setup(t),
    original = await f.call('/api/member/key', { headers: { 'x-member-activity': '1' } });
  assert.deepEqual(original.body, { apiKey: keys.m1, baseUrl: 'https://entry.example/root/v1' });
  assert.equal(original.headers.has('set-cookie'), false);
  f.configuration.publicBaseUrl = 'https://entry.example/v1///';
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'exact-current-key'))();
  const before = databaseState(f.db),
    current = await f.call('/api/member/key');
  assert.deepEqual(current.body, {
    apiKey: 'exact-current-key',
    baseUrl: 'https://entry.example/v1',
  });
  assert.equal((await f.call('/api/member/history')).body.total, 1);
  assert.equal(databaseState(f.db), before);
  f.configuration.publicBaseUrl = '';
  assert.equal((await f.call('/api/member/key')).body.baseUrl, '');
  f.db.prepare("DELETE FROM api_keys WHERE member_id='m1'").run();
  const missing = databaseState(f.db),
    unavailable = await f.call('/api/member/key');
  assert.equal(unavailable.status, 503);
  assert.equal(unavailable.body.apiKey, undefined);
  assert.equal(databaseState(f.db), missing);
  assert.equal((await f.call('/api/member/history')).status, 200);
  assert.deepEqual(f.signed, ['artifact-existing-0', 'artifact-existing-0']);
});

test('真实 HTTP 保留 Cookie/Origin/方法/当前撤销及 ready 边界，不接受 Bearer 代替会话', async (t) => {
  const f = await setup(t);
  assert.equal(
    (
      await f.call('/api/member/history', {
        cookie: '',
        headers: { authorization: `Bearer ${keys.m1}` },
      })
    ).status,
    401,
  );
  assert.equal(
    (await f.call('/api/member/key', { headers: { origin: 'https://foreign.example' } })).status,
    403,
  );
  assert.equal((await f.call('/api/member/key', { method: 'POST' })).status, 403);
  await logoutMemberSession(f.members, f.sessions.m1.token);
  const before = databaseState(f.db),
    signed = f.signed.length;
  assert.equal((await f.call('/api/member/key')).status, 401);
  assert.equal((await f.call('/api/member/history')).status, 401);
  assert.equal(f.signed.length, signed);
  assert.equal(databaseState(f.db), before);
  assert.equal((await f.call('/api/member/me', { member: 'm2' })).status, 404);
  f.db.prepare("UPDATE projection_state SET status='building' WHERE id=1").run();
  const notReady = databaseState(f.db);
  assert.equal((await f.call('/api/member/history', { member: 'm2' })).status, 500);
  assert.equal(databaseState(f.db), notReady);
  assert.throws(
    () =>
      createRelationalMemberReadHttp({ sqlite: f.db, publicBaseUrl: '', publicImageUrl: () => '' }),
    /incomplete or incompatible/i,
  );
});

test('Discord IO 返回后必须重核当前会话，临时503及等待中退出/停用均不泄露Key或签发URL', async (t) => {
  const env = {
      DISCORD_CLIENT_ID: '123456789012345678',
      DISCORD_CLIENT_SECRET: 'synthetic-discord-secret',
      DISCORD_GUILD_ID: '1134557553011998840',
      PUBLIC_BASE_URL: 'https://member.example',
    },
    state = { fail: true, beforeGrant: undefined, calls: 0 };
  const f = await setup(t, {
    env,
    beforeReady: async (f) => {
      await f.members.discordAuthenticationTransaction((transaction) => {
        const member = transaction.readMember('m1');
        member.discord = {
          id: '222222222222222222',
          guildId: env.DISCORD_GUILD_ID,
          accessToken: 'synthetic-discord-access',
          refreshToken: 'synthetic-discord-refresh',
          expiresAt: Date.now() + 3600000,
          checkedAt: 0,
          member: true,
        };
        transaction.saveAuthentication(member, 'login');
      });
      // m2 不通过这些受保护 HTTP，仍需可签发的合法绑定用于当前 fixture 私有快照。
    },
    discordFetch: async () => {
      assert.equal(f.db.inTransaction, false);
      state.calls++;
      if (state.fail) return new Response('private remote diagnostic', { status: 503 });
      await state.beforeGrant?.();
      return Response.json({ user: { id: '222222222222222222' } });
    },
  });
  const transient = await f.call('/api/member/key');
  assert.equal(transient.status, 503);
  assert.equal(transient.text.includes('private remote diagnostic'), false);
  assert.equal(f.signed.length, 0);
  state.fail = false;
  state.beforeGrant = () => logoutMemberSession(f.members, f.sessions.m1.token);
  const loggedOut = await f.call('/api/member/key');
  assert.equal(loggedOut.status, 401);
  assert.equal(loggedOut.body.apiKey, undefined);
  assert.equal(f.members.readMember('m1').status, 'active');
  await f.members.discordAuthenticationTransaction((transaction) => {
    const member = transaction.readMember('m1');
    member.discord.checkedAt = 0;
    transaction.saveAuthentication(member);
  });
  const renewed = await createMemberSession(
    f.members,
    await authenticateMemberApiKey(f.members, keys.m1),
    env,
    NOW,
  );
  state.beforeGrant = () =>
    f.db.transaction(() =>
      f.members.setMemberStatus(meta('disable-during-discord'), 'm1', 'disabled'),
    )();
  const denied = await f.call('/api/member/history', { cookie: renewed.token });
  assert.equal(denied.status, 401);
  assert.equal(f.signed.length, 0);
  assert.equal(denied.text.includes(keys.m1), false);
  assert.equal(state.calls, 3);
});

test('免费用量原owner在策略关闭后仍保留成功事实而不展示已禁用警告', async (t) => {
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { freeLimit: 1, seconds: 0, env });
  await f.accept('free', { steps: 23 });
  f.dispatch('free-0');
  await f.publish('free-0');
  // 校验现唯一内部 usage owner；当前报价只消费 usage，不对外发布此 DTO。
  const usage = () => f.generations.freeUsage('m1', f.generations.policy(), NOW + 10010);
  assert.equal(usage().warning, true);
  const quotas = new RelationalQuotaStore(f.db, env);
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    (member) =>
      quotas.configureQuotaPolicy(
        meta('disable-free-policy', NOW + 10000, member.id),
        {
          action: 'free-image-policy',
          expectedVersion: 1,
          policy: { enabled: false, warningThreshold: 1, hardLimit: 1 },
        },
        member,
      ),
    env,
  );
  const before = databaseState(f.db),
    result = usage();
  assert.equal(result.enabled, false);
  assert.equal(result.successful, 1);
  assert.equal(result.warning, false);
  assert.equal(databaseState(f.db), before);
});

test('独立：旧Key转归他人后，原Cookie和当前Key仍各自绑定稳定成员', async (t) => {
  const f = await setup(t, {
    beforeReady: (f) => f.accept('foreign', { memberId: 'm2' }),
  });
  f.db.transaction(() => {
    f.members.replaceMemberApiKey('m1', 'independent-current-m1');
    f.members.replaceMemberApiKey('m2', keys.m1);
  })();
  const before = databaseState(f.db);
  const own = await f.call('/api/member/history?memberId=m2');
  const other = await f.call('/api/member/history', { member: 'm2' });
  assert.deepEqual(
    own.body.jobs.map((j) => j.id),
    ['existing-0'],
  );
  assert.deepEqual(
    other.body.jobs.map((j) => j.id),
    ['foreign-0'],
  );
  assert.equal((await f.call('/api/member/key')).body.apiKey, 'independent-current-m1');
  assert.equal((await f.call('/api/member/key', { member: 'm2' })).body.apiKey, keys.m1);
  assert.equal(databaseState(f.db), before);
});

test('独立：Discord等待中轮换Key后只领取本人的当前Key，旧快照不得覆盖轮换', async (t) => {
  const env = {
    DISCORD_CLIENT_ID: '123456789012345678',
    DISCORD_CLIENT_SECRET: 'synthetic-independent-secret',
    DISCORD_GUILD_ID: '1134557553011998840',
    PUBLIC_BASE_URL: 'https://member.example',
  };
  let fetched = 0;
  const f = await setup(t, {
    env,
    beforeReady: async (f) => {
      await f.members.discordAuthenticationTransaction((transaction) => {
        const member = transaction.readMember('m1');
        member.discord = {
          id: '222222222222222222',
          guildId: env.DISCORD_GUILD_ID,
          accessToken: 'synthetic-independent-access',
          refreshToken: 'synthetic-independent-refresh',
          expiresAt: Date.now() + 3600000,
          checkedAt: 0,
          member: true,
        };
        transaction.saveAuthentication(member, 'login');
      });
    },
    discordFetch: async () => {
      fetched++;
      assert.equal(f.db.inTransaction, false);
      f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'rotated-during-io'))();
      return Response.json({ user: { id: '222222222222222222' } });
    },
  });
  const originalExpiry = f.db
    .prepare('SELECT expires_at FROM member_sessions WHERE member_id=?')
    .get('m1').expires_at;
  const response = await f.call('/api/member/key', { headers: { 'x-member-activity': '1' } });
  assert.equal(response.status, 200);
  assert.equal(response.body.apiKey, 'rotated-during-io');
  assert.equal(f.members.readMemberApiKey('m1'), 'rotated-during-io');
  assert.equal(await authenticateMemberApiKey(f.members, keys.m1), null);
  assert.equal(
    f.db.prepare('SELECT expires_at FROM member_sessions WHERE member_id=?').get('m1').expires_at,
    originalExpiry,
  );
  assert.equal(response.headers.has('set-cookie'), false);
  assert.equal(fetched, 1);
});
