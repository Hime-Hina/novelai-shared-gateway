import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { createHash, randomUUID } from 'node:crypto';
import Database from 'better-sqlite3';
import {
  createMemberSession,
  authenticateMemberApiKey,
} from '../../server/members/authentication.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { childFixture, launchGateway, enableAccount, query } from '../helpers/relational-child.mjs';

const memberTtl = 7 * 86400000,
  ownerTtl = 24 * 3600000;
const cookieFor = (session) => `nai_member_session=${session.token}`;
const tokenHash = (cookie) =>
  createHash('sha256')
    .update(cookie.slice(cookie.indexOf('=') + 1))
    .digest('hex');
async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}

async function fixture(t, { owner = false, grace = 30000, paused = false, quotaStatus } = {}) {
  const state = await childFixture(t, 'member-session-http');
  const { members } = state;
  const adminToken = state.env.ADMIN_TOKEN;
  const guildId = '1134557553011998840';
  const env = {
    PUBLIC_HOST: 'session.example',
    SHUTDOWN_GRACE_MS: String(grace),
    PUBLIC_BASE_URL: owner ? 'https://session.example' : 'http://127.0.0.1',
  };
  if (owner) {
    Object.assign(env, {
      OWNER_USER_ID: members[0].user_id,
      DISCORD_GUILD_ID: guildId,
      DISCORD_CLIENT_ID: '123456789012345678',
      DISCORD_CLIENT_SECRET: 'synthetic-session-client-secret',
    });
  }
  let running,
    exited,
    activated = false;
  const memberStore = async (run) => {
    assert.ok(!running || running.exit, 'offline member changes require the gateway to be stopped');
    const db = new Database(state.filename, { fileMustExist: true });
    try {
      return await run(new RelationalMemberStore(db, env));
    } finally {
      db.close();
    }
  };
  if (owner) {
    await memberStore(async (store) => {
      for (const [index, member] of members.entries()) {
        await store.discordAuthenticationTransaction((transaction) => {
          const user = transaction.readMember(member.user_id);
          user.discord = {
            id: `22222222222222222${index}`,
            guildId,
            member: true,
            checkedAt: Date.now(),
            accessToken: 'synthetic-access',
            refreshToken: 'synthetic-refresh',
            expiresAt: Date.now() + 3600000,
          };
          transaction.saveAuthentication(user, 'observation');
        });
      }
    });
  }
  const read = (sql, ...parameters) => query(state, sql, ...parameters);
  // Only the original failure/time fixtures use direct SQL; business setup goes through its owner.
  const execute = (sql, ...parameters) => {
    const db = new Database(state.filename, { fileMustExist: true });
    try {
      return parameters.length ? db.prepare(sql).run(...parameters) : db.exec(sql);
    } finally {
      db.close();
    }
  };
  const requests = [];
  const upstream = http.createServer(async (req, res) => {
    requests.push(req.url);
    if (req.url !== '/user/data') {
      res.writeHead(500);
      res.end('unexpected synthetic upstream request');
      return;
    }
    const status = quotaStatus ? await quotaStatus() : 200;
    if (status !== 200) {
      res.writeHead(status, { 'content-type': 'text/plain' });
      res.end('SYNTHETIC_PRIVATE_OWNER_QUOTA_ERROR');
      return;
    }
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(
      JSON.stringify({
        subscription: {
          tier: 3,
          active: true,
          expiresAt: Math.floor(Date.now() / 1000) + 86400,
          trainingStepsLeft: { fixedTrainingStepsLeft: 10000, purchasedTrainingSteps: 0 },
          usage: { percent: 100, isNegative: false, timeUntilNextPercent: 0 },
        },
      }),
    );
  });
  const upstreamPort = await listen(upstream);
  const upstreamOrigin = `http://127.0.0.1:${upstreamPort}`;
  let base;
  const start = async (ownerUserId = env.OWNER_USER_ID) => {
    if (owner) {
      env.OWNER_USER_ID = ownerUserId;
    }
    running = await launchGateway(state, upstreamOrigin, env);
    exited = once(running.child, 'exit');
    base = running.origin;
    if (!paused && !activated) {
      await enableAccount(state, running);
      activated = true;
    }
  };
  const stop = async () => {
    if (running) {
      await running.kill();
    }
  };
  t.after(async () => {
    await stop();
    upstream.closeAllConnections();
    await new Promise((resolve) => upstream.close(resolve));
  });
  const call = (
    endpoint,
    { body, cookie, headers = {}, method = body === undefined ? 'GET' : 'POST' } = {},
  ) =>
    fetch(base + endpoint, {
      method,
      signal: AbortSignal.timeout(15000),
      headers: {
        origin: base,
        ...(cookie ? { cookie } : {}),
        ...(body === undefined ? {} : { 'content-type': 'application/json' }),
        ...headers,
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  const login = (index = 0) =>
    call('/api/member/login', {
      body: { username: members[index].username, password: members[index].password },
    });
  const me = async (cookie) => {
    const response = await call('/api/member/me', { cookie });
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('set-cookie'), null);
    return response.json();
  };
  const createOfflineSession = (index, now = Date.now()) =>
    memberStore(async (store) => {
      const memberId = members[index].user_id;
      const member = await authenticateMemberApiKey(store, store.readMemberApiKey(memberId));
      assert.equal(member?.id, memberId);
      return createMemberSession(store, member, env, now);
    });
  const expire = (cookie) =>
    execute(
      'UPDATE member_sessions SET expires_at = ? WHERE token_hash = ?',
      Date.now() - 1,
      tokenHash(cookie),
    );
  return {
    state,
    start,
    stop,
    call,
    login,
    me,
    createOfflineSession,
    expire,
    read,
    execute,
    memberStore,
    members,
    adminToken,
    accountId: state.accountId,
    privateToken: state.env.NOVELAI_TOKEN,
    child: () => running.child,
    exit: () => exited,
    assertNoGeneration() {
      assert.ok(requests.every((url) => url === '/user/data'));
      for (const child of state.children) {
        assert.doesNotMatch(child.output, /denied upstream before IO|must not load legacy SQLite/);
      }
      assert.equal(read('SELECT COUNT(*) AS n FROM generation_jobs')[0].n, 0);
    },
  };
}

async function openEvents(gateway, cookie) {
  const response = await gateway.call('/api/member/shared-events', { cookie });
  assert.equal(response.status, 200);
  assert.match(response.headers.get('content-type'), /text\/event-stream/);
  const reader = response.body.getReader(),
    decoder = new TextDecoder();
  let text = '';
  while (!text.includes('event: shared-status')) {
    const chunk = await reader.read();
    assert.equal(chunk.done, false);
    text += decoder.decode(chunk.value, { stream: true });
  }
  return async () => {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) {
        break;
      }
      text += decoder.decode(chunk.value, { stream: true });
    }
    assert.match(text, /event: auth-expired\ndata: \{\}\n\n/);
  };
}

test(
  'owner 启用并发重试领取原回执，保留后续暂停、失败观察和当前授权',
  { timeout: 45000 },
  async (t) => {
    const replies = [];
    const gateway = await fixture(t, {
      owner: true,
      paused: true,
      quotaStatus: () => replies.shift()?.() ?? 200,
    });
    const cookie = cookieFor(await gateway.createOfflineSession(0));
    const id = gateway.accountId;
    const current = () => {
      const account = gateway.read('SELECT id,enabled FROM upstream_accounts WHERE id=?', id)[0];
      return { ...account, enabled: account.enabled === 1 };
    };
    const patch = async (key, body = { enabled: true }) => {
      const response = await gateway.call(`/api/member/owner/accounts/${id}`, {
        cookie,
        method: 'PATCH',
        headers: { 'Idempotency-Key': key },
        body,
      });
      return { status: response.status, body: await response.json() };
    };
    const holdEnable = async (key) => {
      const entered = Promise.withResolvers(),
        response = Promise.withResolvers();
      t.after(() => response.resolve(200));
      replies.push(() => {
        entered.resolve();
        return response.promise;
      });
      const result = patch(key);
      await Promise.race([
        entered.promise,
        result.then((value) =>
          assert.fail(`owner enable did not reach upstream: ${JSON.stringify(value)}`),
        ),
      ]);
      return { result, release: () => response.resolve(200) };
    };
    const enabledWrites = () =>
      gateway.read(
        "SELECT COUNT(*) AS n FROM domain_events WHERE event_type='AvailabilityChanged' AND account_id=? AND json_extract(payload,'$.enabled')=1",
        id,
      )[0].n;
    const officialObservations = () =>
      gateway.read(
        "SELECT payload FROM domain_events WHERE event_type='QuotaObserved' AND account_id=? ORDER BY commit_seq,event_index",
        id,
      );
    await gateway.start();
    assert.equal(current().enabled, false);
    const initialWrites = enabledWrites();
    for (const pauseFirst of [false, true]) {
      const key = pauseFirst ? 'enable-before-pause' : 'enable-only';
      const held = await holdEnable(key);
      const before = officialObservations();
      const first = await patch(key);
      assert.equal(first.status, 200, JSON.stringify(first.body));
      assert.equal(first.body.enabled, true);
      assert.ok(!JSON.stringify(first.body).includes(gateway.privateToken));
      const { receipt } = gateway.read(
        "SELECT receipt FROM commits WHERE command_id LIKE 'management:%' ORDER BY commit_seq DESC LIMIT 1",
      )[0];
      assert.deepEqual(JSON.parse(receipt), first.body);
      if (pauseFirst) {
        assert.equal((await patch('later-pause', { enabled: false })).status, 200);
      }
      held.release();
      assert.deepEqual(await held.result, first);
      assert.equal(current().enabled, !pauseFirst);
      assert.equal(enabledWrites(), initialWrites + (pauseFirst ? 2 : 1));
      assert.equal(officialObservations().length, before.length + 1);
      if (!pauseFirst) {
        assert.equal((await patch('reset-to-pause', { enabled: false })).status, 200);
      }
    }

    const stale = await holdEnable('stale-new-command');
    const beforeFailure = officialObservations();
    replies.push(() => 401);
    assert.equal((await patch('failed-new-command')).status, 409);
    stale.release();
    assert.equal((await stale.result).status, 409);
    const afterFailure = officialObservations();
    assert.equal(afterFailure.length, beforeFailure.length + 1);
    assert.equal(JSON.parse(afterFailure.at(-1).payload).errorCode, 'invalid_key');
    assert.ok(!afterFailure.at(-1).payload.includes('SYNTHETIC_PRIVATE_OWNER_QUOTA_ERROR'));
    assert.equal(current().enabled, false);
    assert.equal(enabledWrites(), initialWrites + 2);

    const revoked = await holdEnable('enable-before-revocation');
    assert.equal((await patch('enable-before-revocation')).status, 200);
    assert.equal((await patch('enable-before-revocation', { enabled: false })).status, 409);
    gateway.expire(cookie);
    revoked.release();
    assert.equal((await revoked.result).status, 401);
    assert.equal(enabledWrites(), initialWrites + 3);
    gateway.assertNoGeneration();
  },
);

test(
  '真实管理员批量和单账号修改原子记录可用性事实，事件写入失败不改变账户或账本',
  { timeout: 45000 },
  async (t) => {
    const gateway = await fixture(t);
    const officialObservations = () =>
      gateway.read("SELECT COUNT(*) AS n FROM domain_events WHERE event_type='QuotaObserved'")[0].n;
    const seededObservations = officialObservations();
    await gateway.start();
    const refreshDeadline = Date.now() + 15000;
    while (officialObservations() === seededObservations) {
      assert.ok(
        Date.now() < refreshDeadline,
        'startup quota observation must commit before testing an unrelated transaction',
      );
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    const headers = { 'x-admin-token': gateway.adminToken };
    const originalId = gateway.accountId;
    const added = await gateway.call('/api/admin/accounts', {
      headers,
      body: {
        name: 'Synthetic availability peer',
        token: 'pst-synthetic-availability-peer',
        enabled: false,
      },
    });
    assert.equal(added.status, 201);
    const peer = await added.json(),
      ids = [originalId, peer.id];
    const readAccounts = () =>
      gateway.read('SELECT id,enabled,weight FROM upstream_accounts ORDER BY id').map((row) => {
        return { id: row.id, enabled: row.enabled === 1, weight: row.weight };
      });
    const snapshot = () =>
      Object.fromEntries(
        gateway
          .read("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
          .map(({ name }) => [
            name,
            gateway.read(`SELECT * FROM "${name.replaceAll('"', '""')}" ORDER BY rowid`),
          ]),
      );
    const availabilityEvents = () =>
      gateway.read(
        "SELECT * FROM domain_events WHERE event_type='AvailabilityChanged' ORDER BY commit_seq,event_index",
      );
    const before = snapshot(),
      beforeAvailability = availabilityEvents().length;
    gateway.execute(
      "CREATE TRIGGER synthetic_availability_failure BEFORE INSERT ON domain_events WHEN NEW.event_type='AvailabilityChanged' BEGIN SELECT RAISE(ABORT, 'synthetic availability storage failure'); END",
    );
    try {
      for (const [endpoint, body] of [
        ['/api/admin/accounts', { ids, enabled: false, weight: 7 }],
        [`/api/admin/accounts/${originalId}`, { enabled: false, weight: 9 }],
      ]) {
        const failed = await gateway.call(endpoint, { method: 'PATCH', headers, body });
        assert.equal(failed.status, 500);
        await failed.text();
        assert.deepEqual(
          snapshot(),
          before,
          'rejected account commands must leave all durable facts, private state and financial rows unchanged',
        );
      }
    } finally {
      gateway.execute('DROP TRIGGER synthetic_availability_failure');
    }
    const bulk = await gateway.call('/api/admin/accounts', {
      method: 'PATCH',
      headers,
      body: { ids, enabled: false, weight: 7 },
    });
    assert.equal(bulk.status, 200);
    const expectedBulk = ids
      .map((id) => ({ id, enabled: false, weight: 7 }))
      .sort((a, b) => a.id.localeCompare(b.id));
    const response = await bulk.json();
    assert.deepEqual(
      response.accounts
        .map(({ id, enabled, weight }) => ({ id, enabled, weight }))
        .sort((a, b) => a.id.localeCompare(b.id)),
      expectedBulk,
    );
    assert.deepEqual(readAccounts(), expectedBulk);
    const bulkEvents = availabilityEvents().slice(beforeAvailability);
    assert.equal(
      bulkEvents.length,
      1,
      'the already paused peer must not get a fabricated state change',
    );
    const configured = gateway.read(
      "SELECT * FROM domain_events WHERE event_type='AccountConfigurationChanged' AND commit_seq=? ORDER BY event_index",
      bulkEvents[0].commit_seq,
    );
    assert.equal(configured.length, 2);
    assert.equal(
      new Set([...bulkEvents, ...configured].map((event) => event.commit_seq)).size,
      1,
      'bulk intent must have one atomic commit',
    );
    assert.deepEqual(configured.map((event) => event.account_id).sort(), [...ids].sort());
    for (const event of bulkEvents) {
      assert.equal(event.actor_kind, 'admin');
      assert.equal(event.actor_id, 'local-admin');
      const payload = JSON.parse(event.payload);
      assert.equal(payload.accountId, event.account_id);
      assert.equal(payload.enabled, false);
    }
    for (const event of configured) {
      assert.equal(event.actor_kind, 'admin');
      assert.equal(event.actor_id, 'local-admin');
      assert.equal(JSON.parse(event.payload).accountId, event.account_id);
      assert.equal(JSON.parse(event.payload).weight, 7);
    }
    assert.equal(
      gateway.read(
        "SELECT COUNT(*) AS n FROM commits WHERE json_extract(receipt,'$.accountIntent.kind')='bulk'",
      )[0].n,
      1,
    );
    const intent = JSON.parse(
      gateway.read('SELECT receipt FROM commits WHERE commit_seq=?', bulkEvents[0].commit_seq)[0]
        .receipt,
    ).accountIntent;
    assert.deepEqual([...intent.ids].sort(), [...ids].sort());
    assert.equal(intent.weight, 7);
    assert.equal(intent.enabled, false);
    await gateway.stop();
    await gateway.start();
    assert.deepEqual(readAccounts(), expectedBulk);
    const single = await gateway.call(`/api/admin/accounts/${originalId}`, {
      method: 'PATCH',
      headers,
      body: { enabled: true, weight: 11 },
    });
    assert.equal(single.status, 200);
    const changed = await single.json();
    assert.equal(changed.id, originalId);
    assert.equal(changed.enabled, true);
    assert.equal(changed.weight, 11);
    const expectedSingle = expectedBulk.map((account) =>
      account.id === originalId ? { ...account, enabled: true, weight: 11 } : account,
    );
    assert.deepEqual(readAccounts(), expectedSingle);
    const singleEvents = availabilityEvents().slice(beforeAvailability + bulkEvents.length);
    assert.equal(singleEvents.length, 1);
    assert.equal(singleEvents[0].account_id, originalId);
    assert.equal(singleEvents[0].actor_kind, 'admin');
    assert.equal(singleEvents[0].actor_id, 'local-admin');
    assert.equal(JSON.parse(singleEvents[0].payload).enabled, true);
    const singleConfigured = gateway.read(
      "SELECT payload FROM domain_events WHERE event_type='AccountConfigurationChanged' AND commit_seq=? AND account_id=?",
      singleEvents[0].commit_seq,
      originalId,
    );
    assert.equal(singleConfigured.length, 1);
    assert.equal(JSON.parse(singleConfigured[0].payload).weight, 11);
    await gateway.stop();
    await gateway.start();
    assert.deepEqual(readAccounts(), expectedSingle);
    gateway.assertNoGeneration();
  },
);

for (const owner of [false, true]) {
  test(
    `可见网页访问续期${owner ? 'owner 一天' : '普通成员七天'}，Cookie 与数据库一致，其他查询和跨站请求不续期`,
    { timeout: 45000 },
    async (t) => {
      const gateway = await fixture(t, { owner }),
        ttl = owner ? ownerTtl : memberTtl;
      let session = owner ? await gateway.createOfflineSession(0, Date.now() - 10000) : null;
      await gateway.start();
      let cookie = session ? cookieFor(session) : null;
      if (!cookie) {
        const login = await gateway.login();
        cookie = login.headers.get('set-cookie').split(';')[0];
        await login.json();
      }
      const hash = tokenHash(cookie);
      const endpoint = owner ? '/api/member/owner/member-management' : '/api/member/me';
      const prior = Date.now() + 10000;
      gateway.execute('UPDATE member_sessions SET expires_at=? WHERE token_hash=?', prior, hash);
      const ordinary = await gateway.call(endpoint, { cookie });
      assert.equal(ordinary.status, 200);
      assert.equal(ordinary.headers.get('set-cookie'), null);
      await ordinary.json();
      assert.equal(
        gateway.read('SELECT expires_at FROM member_sessions WHERE token_hash=?', hash)[0]
          .expires_at,
        prior,
      );
      const crossSite = await gateway.call(endpoint, {
        cookie,
        headers: { 'x-member-activity': '1', origin: 'https://elsewhere.invalid' },
      });
      assert.equal(crossSite.status, 403);
      assert.equal(crossSite.headers.get('set-cookie'), null);
      await crossSite.text();
      assert.equal(
        gateway.read('SELECT expires_at FROM member_sessions WHERE token_hash=?', hash)[0]
          .expires_at,
        prior,
      );
      const before = Date.now(),
        visit = await gateway.call(endpoint, { cookie, headers: { 'x-member-activity': '1' } }),
        after = Date.now();
      assert.equal(visit.status, 200);
      const value = await visit.json(),
        renewed = gateway.read('SELECT * FROM member_sessions WHERE token_hash=?', hash)[0];
      assert.ok(renewed.expires_at >= before + ttl && renewed.expires_at <= after + ttl);
      assert.equal(visit.headers.get('set-cookie').split(';')[0], cookie);
      assert.equal(
        Date.parse(/Expires=([^;]+)/.exec(visit.headers.get('set-cookie'))[1]),
        Math.floor(renewed.expires_at / 1000) * 1000,
      );
      assert.equal(Number(/Max-Age=(\d+)/.exec(visit.headers.get('set-cookie'))[1]), ttl / 1000);
      if (!owner) {
        assert.equal(value.sessionExpiresAt, renewed.expires_at);
      }
      const unrelated = await gateway.call('/api/member/shared-status', {
        cookie,
        headers: { 'x-member-activity': '1' },
      });
      assert.equal(unrelated.status, 200);
      assert.equal(unrelated.headers.get('set-cookie'), null);
      await unrelated.json();
      assert.equal(
        gateway.read('SELECT expires_at FROM member_sessions WHERE token_hash=?', hash)[0]
          .expires_at,
        renewed.expires_at,
      );
      gateway.execute(
        "CREATE TRIGGER synthetic_renewal_failure BEFORE UPDATE ON member_sessions BEGIN SELECT RAISE(ABORT, 'synthetic renewal failure'); END",
      );
      try {
        const failed = await gateway.call(endpoint, {
          cookie,
          headers: { 'x-member-activity': '1' },
        });
        assert.equal(failed.status, 500);
        assert.equal(failed.headers.get('set-cookie'), null);
        await failed.text();
        assert.equal(
          gateway.read('SELECT expires_at FROM member_sessions WHERE token_hash=?', hash)[0]
            .expires_at,
          renewed.expires_at,
        );
      } finally {
        gateway.execute('DROP TRIGGER synthetic_renewal_failure');
      }
      await gateway.stop();
      await gateway.start();
      assert.equal((await gateway.me(cookie)).sessionExpiresAt, renewed.expires_at);
      gateway.expire(cookie);
      const expired = await gateway.call(endpoint, {
        cookie,
        headers: { 'x-member-activity': '1' },
      });
      assert.equal(expired.status, 401);
      assert.equal(expired.headers.get('set-cookie'), null);
      await expired.text();
      assert.equal(
        gateway.read('SELECT token_hash FROM member_sessions WHERE token_hash=?', hash)[0],
        undefined,
      );
      gateway.assertNoGeneration();
    },
  );
}

test(
  '真实 HTTP 普通会话初始七天，刷新与强制重启保留期限，退出持久撤销并关闭 SSE',
  { timeout: 45000 },
  async (t) => {
    const gateway = await fixture(t);
    await gateway.start();
    const before = Date.now(),
      login = await gateway.login(),
      after = Date.now();
    assert.equal(login.status, 200);
    const setCookie = login.headers.get('set-cookie'),
      cookie = setCookie.split(';')[0],
      session = await login.json();
    assert.match(setCookie, /; Path=\/api\/member; HttpOnly; SameSite=Strict;/);
    const maxAge = Number(/Max-Age=(\d+)/.exec(setCookie)?.[1]);
    assert.ok(maxAge >= 604798 && maxAge <= 604800);
    assert.ok(
      Number.isSafeInteger(session.expiresAt) &&
        session.expiresAt >= before + memberTtl &&
        session.expiresAt <= after + memberTtl,
    );
    assert.equal(
      Date.parse(/Expires=([^;]+)/.exec(setCookie)?.[1]),
      Math.floor(session.expiresAt / 1000) * 1000,
    );
    assert.equal((await gateway.me(cookie)).sessionExpiresAt, session.expiresAt);
    assert.equal((await gateway.me(cookie)).sessionExpiresAt, session.expiresAt);
    const row = gateway.read(
      'SELECT * FROM member_sessions WHERE token_hash = ?',
      tokenHash(cookie),
    )[0];
    assert.equal(row.expires_at, session.expiresAt);
    assert.ok(!JSON.stringify(row).includes(cookie.slice(cookie.indexOf('=') + 1)));
    await gateway.stop();
    await gateway.start();
    assert.equal((await gateway.me(cookie)).sessionExpiresAt, session.expiresAt);
    const expiredEvent = await openEvents(gateway, cookie);
    const logout = await gateway.call('/api/member/logout', { body: {}, cookie });
    assert.equal(logout.status, 200);
    assert.match(logout.headers.get('set-cookie'), /Max-Age=0/);
    await logout.json();
    await expiredEvent();
    await gateway.stop();
    await gateway.start();
    assert.equal((await gateway.call('/api/member/me', { cookie })).status, 401);
    gateway.assertNoGeneration();
  },
);

test(
  '会话写入失败不发 Cookie，浏览器持有的过期 Cookie 和密码重置会话被拒绝',
  { timeout: 45000 },
  async (t) => {
    const gateway = await fixture(t);
    await gateway.start();
    const count = gateway.read('SELECT COUNT(*) AS n FROM member_sessions')[0].n;
    gateway.execute(
      "CREATE TRIGGER synthetic_session_failure BEFORE INSERT ON member_sessions BEGIN SELECT RAISE(ABORT, 'synthetic session storage failure'); END",
    );
    try {
      const failed = await gateway.login();
      assert.equal(failed.status, 500);
      assert.equal(failed.headers.get('set-cookie'), null);
      await failed.text();
      assert.equal(gateway.read('SELECT COUNT(*) AS n FROM member_sessions')[0].n, count);
    } finally {
      gateway.execute('DROP TRIGGER synthetic_session_failure');
    }
    const login = await gateway.login(1);
    assert.equal(login.status, 200);
    const cookie = login.headers.get('set-cookie').split(';')[0];
    await login.json();
    const expiredEvent = await openEvents(gateway, cookie);
    assert.equal(gateway.expire(cookie).changes, 1);
    await expiredEvent();
    assert.equal((await gateway.call('/api/member/me', { cookie })).status, 401);
    const old = await gateway.login(2),
      unrelated = await gateway.login(3);
    assert.equal(old.status, 200);
    assert.equal(unrelated.status, 200);
    await old.json();
    await unrelated.json();
    const oldCookie = old.headers.get('set-cookie').split(';')[0],
      unrelatedCookie = unrelated.headers.get('set-cookie').split(';')[0];
    const prior = await gateway.me(unrelatedCookie);
    const reset = await gateway.call('/api/admin/member-management', {
      headers: { 'x-admin-token': gateway.adminToken },
      body: {
        action: 'credentials',
        userId: gateway.members[2].user_id,
        username: gateway.members[2].username,
        password: 'synthetic-reset-password-for-session-test',
      },
    });
    assert.equal(reset.status, 200);
    await reset.json();
    assert.equal((await gateway.call('/api/member/me', { cookie: oldCookie })).status, 401);
    await gateway.stop();
    await gateway.start();
    assert.equal((await gateway.call('/api/member/me', { cookie: oldCookie })).status, 401);
    assert.equal((await gateway.me(unrelatedCookie)).sessionExpiresAt, prior.sessionExpiresAt);
    gateway.assertNoGeneration();
  },
);

test(
  'owner 一天，A→B→A 不复活旧会话、普通七天 Cookie 不直接升级，无关成员不受影响',
  { timeout: 45000 },
  async (t) => {
    const gateway = await fixture(t, { owner: true }),
      now = Date.now();
    const owner = await gateway.createOfflineSession(0, now),
      member = await gateway.createOfflineSession(1, now),
      unrelated = await gateway.createOfflineSession(2, now);
    assert.equal(owner.expiresAt, now + ownerTtl);
    assert.equal(member.expiresAt, now + memberTtl);
    await gateway.start();
    assert.equal((await gateway.me(cookieFor(owner))).owner, true);
    assert.equal((await gateway.me(cookieFor(owner))).sessionExpiresAt, owner.expiresAt);
    assert.equal((await gateway.me(cookieFor(member))).owner, false);
    assert.equal(
      (await gateway.call('/api/member/owner/member-management', { cookie: cookieFor(owner) }))
        .status,
      200,
    );
    assert.equal(
      (await gateway.call('/api/member/owner/member-management', { cookie: cookieFor(member) }))
        .status,
      403,
    );
    await gateway.stop();
    await gateway.start(gateway.members[1].user_id);
    for (const session of [owner, member]) {
      // 启动时即撤销，不能依赖浏览器恰好在角色变化期间访问一次。
      assert.equal(
        gateway.read(
          'SELECT token_hash FROM member_sessions WHERE token_hash = ?',
          tokenHash(cookieFor(session)),
        )[0],
        undefined,
      );
      assert.equal(
        (await gateway.call('/api/member/me', { cookie: cookieFor(session) })).status,
        401,
      );
      assert.equal(
        (await gateway.call('/api/member/owner/member-management', { cookie: cookieFor(session) }))
          .status,
        401,
      );
    }
    assert.equal((await gateway.me(cookieFor(unrelated))).sessionExpiresAt, unrelated.expiresAt);
    await gateway.stop();
    await gateway.start(gateway.members[0].user_id);
    assert.equal((await gateway.call('/api/member/me', { cookie: cookieFor(owner) })).status, 401);
    assert.equal(
      (await gateway.call('/api/member/owner/member-management', { cookie: cookieFor(owner) }))
        .status,
      401,
    );
    assert.equal((await gateway.me(cookieFor(unrelated))).sessionExpiresAt, unrelated.expiresAt);
    gateway.assertNoGeneration();
  },
);

test(
  'Cookie 绑定稳定成员，Key 换绑不切换会话身份，停用再启用不复活旧 Cookie',
  { timeout: 45000 },
  async (t) => {
    const gateway = await fixture(t);
    await gateway.start();
    const sessions = [];
    for (const index of [0, 1]) {
      const response = await gateway.login(index);
      assert.equal(response.status, 200);
      sessions.push({
        cookie: response.headers.get('set-cookie').split(';')[0],
        value: await response.json(),
      });
    }
    const memberId = gateway.members[0].user_id;
    const originalEpoch = gateway.read('SELECT auth_epoch FROM members WHERE id=?', memberId)[0]
      .auth_epoch;
    await gateway.stop();
    let originalKey;
    await gateway.memberStore(async (store) => {
      originalKey = store.readMemberApiKey(memberId);
      store.replaceMemberApiKey(memberId, 'synthetic-current-key-for-session-owner');
      store.replaceMemberApiKey(gateway.members[1].user_id, originalKey);
    });
    assert.equal(
      gateway.read('SELECT auth_epoch FROM members WHERE id=?', memberId)[0].auth_epoch,
      originalEpoch,
    );
    await gateway.start();
    for (const [index, session] of sessions.entries()) {
      const profile = await gateway.me(session.cookie);
      assert.equal(profile.username, gateway.members[index].username);
      assert.equal(profile.sessionExpiresAt, session.value.expiresAt);
      assert.equal(
        gateway.read(
          'SELECT member_id FROM member_sessions WHERE token_hash=?',
          tokenHash(session.cookie),
        )[0].member_id,
        gateway.members[index].user_id,
      );
    }
    const noCookie = await gateway.call(`/api/member/me?token=${encodeURIComponent(originalKey)}`, {
      headers: { authorization: `Bearer ${originalKey}` },
    });
    assert.equal(noCookie.status, 401);
    assert.equal(noCookie.headers.get('set-cookie'), null);
    await noCookie.text();
    await gateway.stop();
    await gateway.memberStore((store) => {
      for (const status of ['disabled', 'active']) {
        const commandId = `session-fixture:${randomUUID()}`;
        store.setMemberStatus(
          {
            commandId,
            fingerprint: createHash('sha256').update(commandId).digest('hex'),
            actorKind: 'system',
            actorId: null,
            recordedAt: Date.now(),
            correlationId: null,
            causationId: null,
            policyVersion: 'member-status-fixture',
            pricingVersion: 'none',
          },
          memberId,
          status,
        );
      }
    });
    assert.equal(
      gateway.read(
        'SELECT token_hash FROM member_sessions WHERE token_hash=?',
        tokenHash(sessions[0].cookie),
      )[0],
      undefined,
    );
    await gateway.start();
    assert.equal(
      (await gateway.call('/api/member/me', { cookie: sessions[0].cookie })).status,
      401,
    );
    assert.equal(
      (await gateway.me(sessions[1].cookie)).sessionExpiresAt,
      sessions[1].value.expiresAt,
    );
    gateway.assertNoGeneration();
  },
);

test(
  'SIGTERM主动结束两类SSE订阅，无注销事件且不等满关闭宽限',
  { timeout: 30000, skip: process.platform === 'win32' },
  async (t) => {
    const grace = 20000,
      gateway = await fixture(t, { owner: true, grace });
    const session = await gateway.createOfflineSession(0);
    await gateway.start();
    async function subscribe(route, headers, firstFrame) {
      const response = await gateway.call(route, { headers });
      assert.equal(response.status, 200);
      assert.match(response.headers.get('content-type'), /text\/event-stream/);
      const reader = response.body.getReader(),
        decoder = new TextDecoder();
      let text = '';
      while (!text.includes(firstFrame)) {
        const chunk = await reader.read();
        assert.equal(chunk.done, false);
        text += decoder.decode(chunk.value, { stream: true });
      }
      // 立即观察EOF并收集错误，避免红测清理强杀后遗留未处理的读流拒绝。
      const ended = (async () => {
        for (;;) {
          const chunk = await reader.read();
          if (chunk.done) {
            return { text };
          }
          text += decoder.decode(chunk.value, { stream: true });
        }
      })().catch((error) => ({ error }));
      return { ended };
    }
    const sharedStream = await subscribe(
      '/api/member/shared-events',
      { cookie: cookieFor(session) },
      'event: shared-status',
    );
    const previewStream = await subscribe(
      '/api/jobs/events',
      { authorization: 'Bearer ' + gateway.members[0].api_key },
      ': connected',
    );
    const counts = () => ({
      jobs: gateway.read('SELECT COUNT(*) AS n FROM generation_jobs')[0].n,
      attempts: gateway.read('SELECT COUNT(*) AS n FROM generation_attempts')[0].n,
      settlements: gateway.read(
        'SELECT original_job_id,status,v5,anlas_fixed,anlas_purchased FROM charges ORDER BY original_job_id',
      ),
      wallets: gateway.read(
        'SELECT account_id,member_id,v5_available,anlas_fixed_available,anlas_purchased_available FROM wallets ORDER BY account_id,member_id',
      ),
      // 普通观察和零预算恢复游标可完成；原生成扣退及实际金额不得因关机改变。
      financialFacts: gateway.read(
        "SELECT COUNT(*) AS n FROM domain_events WHERE event_type IN ('QuotaReserved','QuotaRefunded','QuotaSettled','JobFailed','JobCompleted','ArtifactPublished','LateResultRecorded','AllocationApplied','FundsTransferred','FundingRegistered','FundingReceived','OperatingContributed','GapCovered','DebitsConfirmed','ReturnClassified')",
      )[0].n,
    });
    const before = counts(),
      started = Date.now();
    gateway.child().kill('SIGTERM');
    let timer;
    try {
      const [shared, previews, exit] = await Promise.race([
        Promise.all([sharedStream.ended, previewStream.ended, gateway.exit()]),
        new Promise((_, reject) => {
          timer = setTimeout(
            () => reject(new Error('空闲SSE使SIGTERM超过5000ms，仍在等待长订阅关闭')),
            5000,
          );
        }),
      ]);
      assert.equal(shared.error, undefined, '成员状态流应正常EOF');
      assert.equal(previews.error, undefined, '任务预览流应正常EOF');
      assert.doesNotMatch(shared.text, /auth-expired/);
      assert.doesNotMatch(previews.text, /auth-expired/);
      assert.deepEqual(exit, [0, null]);
      const shutdownMs = Date.now() - started;
      assert.ok(shutdownMs < grace / 2);
      assert.deepEqual(counts(), before);
      gateway.assertNoGeneration();
      t.diagnostic(`SIGTERM普通EOF并退出 ${shutdownMs}ms；宽限 ${grace}ms；生成与结算计数不变`);
    } finally {
      clearTimeout(timer);
    }
  },
);
