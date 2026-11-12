import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createHash } from 'node:crypto';
import http from 'node:http';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import {
  prepareMemberCredentials,
  createMemberSession,
  authenticateMemberSession,
  renewMemberSession,
  logoutMemberSession,
  reconcileMemberSessionOwner,
  withAuthenticatedMemberSession,
  withAuthenticatedMemberApiKey,
} from '../../server/members/authentication.ts';
import { handleMemberAuthenticationRoutes } from '../../server/members/http-routes.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { domainDigest } from '../../server/events/domain-events.ts';
import {
  normalizeMemberGeneration,
  materializeMemberGeneration,
} from '../../server/generation/member-contract.ts';

const password = 'synthetic-password-12345';
const credentials = await Promise.all(
  ['first.member', 'second.member', 'third.member'].map((username) =>
    prepareMemberCredentials({ username, password }),
  ),
);
const keys = [
  'synthetic-first-member-key',
  'synthetic-second-member-key',
  'synthetic-third-member-key',
];
const digest = (text) => createHash('sha256').update(text).digest('hex');
function metadata(id, memberId = null) {
  return {
    commandId: id,
    fingerprint: domainDigest(id),
    actorKind: memberId ? 'member' : 'system',
    actorId: memberId,
    recordedAt: 1000,
    correlationId: null,
    causationId: null,
    policyVersion: 'policy1',
    pricingVersion: 'price1',
  };
}
function provision(db, members, index) {
  return db
    .transaction(() =>
      members.registerMember(metadata(`register-${index}`), {
        memberId: `m${index + 1}`,
        displayName: `member ${index + 1}`,
        apiKey: keys[index],
        credentials: credentials[index],
      }),
    )
    .immediate();
}
function fixture(t) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'members-history');
  const members = new RelationalMemberStore(db),
    inputs = new RelationalInputStore(db);
  provision(db, members, 0);
  provision(db, members, 1);
  return { db, members, inputs };
}
function accepted(id, memberId = 'm1') {
  const normalized = normalizeMemberGeneration({
    operation: 'generate',
    model: 'nai-diffusion-4-5-full',
    input: 'private prompt',
    parameters: { width: 128, height: 128, steps: 23, seed: 123, n_samples: 1 },
  });
  return {
    inputId: `input-${id}`,
    requestId: id,
    memberId,
    source: 'member-generation',
    seedMode: 'fixed',
    confirmation: null,
    request: materializeMemberGeneration(normalized),
    context: { kind: 'member', member: normalized.normalizationContext },
    jobs: [
      {
        jobId: `job-${id}`,
        status: 'pending',
        queueSeq: null,
        queueExpiresAt: null,
        deadlineAt: null,
        admissionEpoch: 0,
        cacheKey: null,
      },
    ],
  };
}
async function session(members, member = 'first.member', env = {}, at = 1000) {
  const user = await members.loginMemberPassword(member, password);
  return createMemberSession(members, user, env, at);
}

test('真实成员命令原子创建稳定身份/密码/Key，业务事件和普通读取没有认证材料', async (t) => {
  const { db, members } = fixture(t);
  const view = await members.loginMemberPassword(' FIRST.MEMBER ', password);
  assert.equal(view.id, 'm1');
  assert.equal(view.memberAuth.salt, undefined);
  assert.equal(view.memberAuth.hash, undefined);
  const login = await createMemberSession(members, view, {}, 1000);
  assert.equal(login.expiresAt, 1000 + 7 * 86400000);
  assert.equal((await authenticateMemberSession(members, login.token, {}, 1001)).user.id, 'm1');
  assert.equal(
    db.prepare('SELECT token_hash FROM member_sessions').get().token_hash,
    digest(login.token),
  );
  const publicText =
    JSON.stringify(db.prepare('SELECT payload FROM domain_events').all()) +
    JSON.stringify(db.prepare('SELECT receipt FROM commits').all()) +
    JSON.stringify(members.readMember('m1'));
  for (const secret of [
    password,
    keys[0],
    credentials[0].salt,
    credentials[0].hash,
    credentials[0].version,
    login.token,
    'first.member',
  ]) {
    assert.equal(publicText.includes(secret), false, secret);
  }
  assert.equal(members.update, undefined);
  assert.equal(members.users, undefined);
  assert.throws(() => assertRelationalStorageReady(db), /incomplete/);
  const before = db.serialize();
  db.transaction(() => {
    const retry = members.registerMember(metadata('register-0'), {
      memberId: 'ignored',
      displayName: 'ignored',
      apiKey: 'ignored',
      credentials: credentials[0],
    });
    assert.equal(retry.replayed, true);
  })();
  assert.deepEqual(db.serialize(), before);
  await assert.rejects(members.loginMemberPassword('first.member', 'wrong'), { statusCode: 401 });
  await assert.rejects(members.loginMemberPassword('missing', password), { statusCode: 401 });
});

test('创建/凭据更新失败及外层回滚不留下身份、私有 epoch、流或失效会话', async (t) => {
  const { db, members } = fixture(t);
  const token = await session(members),
    before = db.serialize();
  db.transaction(() => {
    assert.throws(
      () =>
        members.registerMember(metadata('duplicate-username'), {
          memberId: 'new-member',
          displayName: 'new',
          apiKey: 'new-key',
          credentials: credentials[0],
        }),
      { statusCode: 409 },
    );
  })();
  assert.deepEqual(db.serialize(), before);
  const prepared = await members.prepareMemberCredentials('m1', {
    username: 'renamed.member',
    password: 'another-long-password-123',
  });
  db.exec(
    "CREATE TEMP TRIGGER fail_credentials BEFORE UPDATE ON password_credentials BEGIN SELECT RAISE(ABORT,'credential failure'); END",
  );
  db.transaction(() =>
    assert.throws(() => members.setMemberCredentials(prepared), /credential failure/),
  )();
  assert.deepEqual(db.serialize(), before);
  db.exec('DROP TRIGGER fail_credentials');
  assert.throws(
    () =>
      db.transaction(() => {
        members.setMemberCredentials(prepared);
        throw Error('outer rollback');
      })(),
    /outer rollback/,
  );
  assert.deepEqual(db.serialize(), before);
  assert.equal((await authenticateMemberSession(members, token.token, {}, 1001)).user.id, 'm1');
  assert.throws(() => members.setMemberCredentials(prepared), /owning authenticated/);
  assert.throws(() => members.replaceMemberApiKey('m1', 'new-key'), /owning authenticated/);
});

test('Cookie 与 Key 授权包住真实受理事务；失败回滚、跨成员和过时凭据均不能写入', async (t) => {
  const { db, members, inputs } = fixture(t),
    token = await session(members);
  const snapshot = await members.loginMemberPassword('first.member', password);
  let escaped = false;
  const serialOnly = {
    memberSessionTransaction: async (mutate) =>
      mutate({
        readMember: () => snapshot,
        readSession: () => members.readMemberSessionState(digest(token.token)),
        readOwner: () => ({ ownerUserId: '', guildId: '' }),
      }),
    assertMemberCommandTransaction: () => members.assertMemberCommandTransaction(),
  };
  await assert.rejects(
    withAuthenticatedMemberSession(
      serialOnly,
      token.token,
      {},
      () => {
        escaped = true;
      },
      1001,
    ),
    /owning authenticated/,
  );
  assert.equal(escaped, false, '串行队列不是 SQLite 写事务');
  const first = await withAuthenticatedMemberSession(
    members,
    token.token,
    {},
    (user) => inputs.persistAcceptedInput(metadata('cookie-input', user.id), accepted('cookie')),
    1001,
  );
  assert.equal(first.replayed, false);
  const second = await withAuthenticatedMemberApiKey(members, keys[0], (user) =>
    inputs.persistAcceptedInput(metadata('key-input', user.id), accepted('key')),
  );
  assert.equal(second.replayed, false);
  assert.equal(inputs.readMemberInput('key', 'm1').input.parameters.seed, 123);
  const before = db.serialize();
  await assert.rejects(
    withAuthenticatedMemberSession(
      members,
      token.token,
      {},
      (user) => {
        inputs.persistAcceptedInput(metadata('rolled-back', user.id), accepted('rolled-back'));
        throw Error('after acceptance');
      },
      1002,
    ),
    /after acceptance/,
  );
  assert.deepEqual(db.serialize(), before);
  await assert.rejects(
    withAuthenticatedMemberApiKey(members, keys[0], () =>
      inputs.persistAcceptedInput(metadata('cross-member', 'm2'), accepted('cross-member')),
    ),
    { statusCode: 403 },
  );
  assert.deepEqual(db.serialize(), before);
  let ran = false;
  assert.equal(
    await withAuthenticatedMemberApiKey(members, 'old/wrong', () => {
      ran = true;
    }),
    null,
  );
  assert.equal(ran, false);
  assert.deepEqual(db.serialize(), before);
  db.transaction(() => members.replaceMemberApiKey('m1', 'rotated-private-member-key'))();
  assert.equal(
    await withAuthenticatedMemberApiKey(members, keys[0], () => {
      ran = true;
    }),
    null,
  );
  assert.equal(ran, false);
  assert.equal(
    await withAuthenticatedMemberApiKey(members, 'rotated-private-member-key', (user) => user.id),
    'm1',
  );
  assert.equal(inputs.readMemberInput('cookie', 'm1').requestId, 'cookie');
  assert.equal(
    (await authenticateMemberSession(members, token.token, {}, 1003)).user.id,
    'm1',
    '换 Key 不改变 Cookie、成员或历史归属',
  );
});

test('用户名/密码重置保持稳定身份，旧 Cookie 立即撤销；派生后提交重验私有 epoch', async (t) => {
  const { db, members } = fixture(t),
    token = await session(members);
  const old = await members.loginMemberPassword('first.member', password);
  const stale = await members.prepareMemberCredentials('m1', {
    username: 'stale.member',
    password,
  });
  const current = await members.prepareMemberCredentials('m1', {
    username: 'changed.member',
    password: 'changed-password-12345',
  });
  await withAuthenticatedMemberSession(
    members,
    token.token,
    {},
    () => members.setMemberCredentials(current),
    1001,
  );
  assert.equal(await authenticateMemberSession(members, token.token, {}, 1002), null);
  await assert.rejects(createMemberSession(members, old, {}, 1003), { statusCode: 401 });
  await assert.rejects(members.loginMemberPassword('first.member', password), { statusCode: 401 });
  assert.equal(
    (await members.loginMemberPassword('CHANGED.MEMBER', 'changed-password-12345')).id,
    'm1',
  );
  assert.equal(await withAuthenticatedMemberApiKey(members, keys[0], (user) => user.id), 'm1');
  const before = db.serialize();
  db.transaction(() =>
    assert.throws(() => members.setMemberCredentials(stale), { statusCode: 401 }),
  )();
  assert.deepEqual(db.serialize(), before);
  const duplicate = await members.prepareMemberCredentials('m1', {
    username: 'second.member',
    password,
  });
  db.transaction(() =>
    assert.throws(() => members.setMemberCredentials(duplicate), { statusCode: 409 }),
  )();
  assert.deepEqual(db.serialize(), before);
});

test('停用沿用未决工作守卫并原子撤销；重新启用不复活 Cookie，暂停接单不撤销登录', async (t) => {
  const { db, members, inputs } = fixture(t),
    token = await session(members);
  db.prepare("UPDATE members SET accepting_jobs=0 WHERE id='m1'").run();
  assert.ok(await authenticateMemberSession(members, token.token, {}, 1001));
  db.prepare("UPDATE members SET accepting_jobs=1 WHERE id='m1'").run();
  await withAuthenticatedMemberSession(
    members,
    token.token,
    {},
    (user) => inputs.persistAcceptedInput(metadata('pending', user.id), accepted('pending')),
    1001,
  );
  const before = db.serialize();
  db.transaction(() =>
    assert.throws(() => members.setMemberStatus(metadata('disable-pending'), 'm1', 'disabled'), {
      statusCode: 409,
    }),
  )();
  assert.deepEqual(db.serialize(), before);
  db.prepare("UPDATE generation_jobs SET status='failed'").run();
  db.transaction(() => members.setMemberStatus(metadata('disable'), 'm1', 'disabled'))();
  assert.equal(await authenticateMemberSession(members, token.token, {}, 1002), null);
  assert.equal(await withAuthenticatedMemberApiKey(members, keys[0], (user) => user.id), null);
  assert.throws(() => inputs.readMemberInput('pending', 'm1'), { statusCode: 401 });
  db.transaction(() => members.setMemberStatus(metadata('enable'), 'm1', 'active'))();
  assert.equal(await authenticateMemberSession(members, token.token, {}, 1003), null);
  assert.equal((await members.loginMemberPassword('first.member', password)).id, 'm1');
  assert.equal(await withAuthenticatedMemberApiKey(members, keys[0], (user) => user.id), 'm1');
});

test('owner 1 天、普通成员 7 天，A→B→A 的私有协调在重开后不恢复旧权限', async (t) => {
  const { db, members } = fixture(t);
  provision(db, members, 2);
  db.prepare('INSERT INTO discord_bindings(member_id,discord_id,guild_id) VALUES(?,?,?)').run(
    'm1',
    'discord-a',
    'guild',
  );
  db.prepare('INSERT INTO discord_bindings(member_id,discord_id,guild_id) VALUES(?,?,?)').run(
    'm2',
    'discord-b',
    'guild',
  );
  const env = { OWNER_USER_ID: 'm1', DISCORD_GUILD_ID: 'guild' },
    next = { ...env, OWNER_USER_ID: 'm2' };
  const a = await session(members, 'first.member', env),
    b = await session(members, 'second.member', env),
    c = await session(members, 'third.member', env);
  assert.equal(a.expiresAt, 1000 + 86400000);
  assert.equal(c.expiresAt, 1000 + 7 * 86400000);
  assert.equal(await authenticateMemberSession(members, b.token, next, 2000), null);
  assert.equal(await authenticateMemberSession(members, a.token, next, 2000), null);
  const newOwner = await session(members, 'second.member', next, 3000);
  await reconcileMemberSessionOwner(members, env);
  for (const login of [a, b, newOwner]) {
    assert.equal(await authenticateMemberSession(members, login.token, env, 4000), null);
  }
  const current = await renewMemberSession(members, c.token, env, 5000);
  assert.equal(current.expiresAt, 5000 + 7 * 86400000);
  assert.equal(
    (await renewMemberSession(members, c.token, env, 4000)).expiresAt,
    current.expiresAt,
  );
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM commits').get().n, 3, '会话操作不是业务事实');
});

test('关闭重开保持会话和 owner 配置，退出删除提交后再返回；SQL 失败不返回成功', async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), 'relational-members-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const filename = path.join(directory, 'library.sqlite');
  let db = new Database(filename);
  let members;
  let login;
  try {
    initializeRelationalStorage(db, 'members-history');
    members = new RelationalMemberStore(db);
    provision(db, members, 0);
    login = await session(members);
  } finally {
    db.close();
  }
  db = new Database(filename);
  db.pragma('foreign_keys=ON');
  members = new RelationalMemberStore(db);
  try {
    assert.equal((await authenticateMemberSession(members, login.token, {}, 2000)).user.id, 'm1');
    const before = db.serialize();
    db.exec(
      "CREATE TEMP TRIGGER fail_logout BEFORE DELETE ON member_sessions BEGIN SELECT RAISE(ABORT,'logout failure'); END",
    );
    await assert.rejects(logoutMemberSession(members, login.token), /logout failure/);
    assert.deepEqual(db.serialize(), before);
    db.exec('DROP TRIGGER fail_logout');
    await logoutMemberSession(members, login.token);
  } finally {
    db.close();
  }
  db = new Database(filename);
  db.pragma('foreign_keys=ON');
  members = new RelationalMemberStore(db);
  try {
    assert.equal(await authenticateMemberSession(members, login.token, {}, 3000), null);
    assert.deepEqual(db.prepare('SELECT * FROM member_auth_state').get(), {
      id: 1,
      owner_member_id: '',
      guild_id: '',
    });
  } finally {
    db.close();
  }
});

test('原会话上限/到期清理和事务失败均生效，不允许在未提交外层事务先签发成功', async (t) => {
  const { db, members } = fixture(t),
    user = await members.loginMemberPassword('first.member', password);
  const epoch = user.memberAuth.version;
  db.prepare(
    `WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<10000)
    INSERT INTO member_sessions(token_hash,member_id,auth_epoch,owner_binding,created_at,expires_at)
    SELECT printf('%064x',n),'m1',?,NULL,1,9999999999 FROM seq`,
  ).run(epoch);
  const before = db.serialize();
  await assert.rejects(createMemberSession(members, user, {}, 1000), { statusCode: 503 });
  assert.deepEqual(db.serialize(), before);
  db.prepare('UPDATE member_sessions SET expires_at=1000').run();
  const login = await createMemberSession(members, user, {}, 1000);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n, 1);
  let nested;
  db.transaction(() => {
    nested = createMemberSession(members, user, {}, 2000);
  })();
  await assert.rejects(nested, /own commit/);
  const after = db.serialize();
  await assert.rejects(
    withAuthenticatedMemberSession(members, login.token, {}, async () => {}, 1001),
    /promise/i,
  );
  assert.deepEqual(db.serialize(), after);
});

test('业务事实重放只恢复稳定身份，不复活密码、Key、私有 epoch、owner 或 Cookie', async (t) => {
  const { db, members } = fixture(t),
    sourceSession = await session(members);
  db.transaction(() => members.setMemberStatus(metadata('disable'), 'm1', 'disabled'))();
  db.transaction(() => members.setMemberStatus(metadata('enable'), 'm1', 'active'))();
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'members-history');
  const rebuilt = new RelationalMemberStore(target);
  for (const commit of ['1', '2', '3', '4']) {
    const original = readRelationalCommit(db, commit);
    target.transaction(() => rebuilt.replayMemberCommit(original))();
  }
  assert.deepEqual(rebuilt.readMember('m1'), members.readMember('m1'));
  assert.equal(
    target.prepare("SELECT auth_epoch FROM members WHERE id='m1'").get().auth_epoch,
    null,
  );
  for (const table of [
    'password_credentials',
    'api_keys',
    'member_sessions',
    'member_auth_state',
  ]) {
    assert.equal(target.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0, table);
  }
  await assert.rejects(rebuilt.loginMemberPassword('first.member', password), { statusCode: 401 });
  assert.equal(await authenticateMemberSession(rebuilt, sourceSession.token, {}, 2000), null);
  assert.equal(await withAuthenticatedMemberApiKey(rebuilt, keys[0], (user) => user.id), null);
  const checkpoint = target.serialize();
  target.transaction(() =>
    assert.equal(rebuilt.replayMemberCommit(readRelationalCommit(db, '4')).replayed, true),
  )();
  assert.deepEqual(target.serialize(), checkpoint);
  assert.deepEqual(target.pragma('foreign_key_check'), []);
});

test('同一现有 HTTP owner 接入关系表：登录/退出、同源、失败不发 Cookie 和持久 token 摘要', async (t) => {
  const { db, members } = fixture(t);
  const server = http.createServer(async (req, res) => {
    try {
      const handled = await handleMemberAuthenticationRoutes(
        req,
        res,
        new URL(req.url, 'http://' + req.headers.host),
        { store: members, env: {} },
      );
      if (!handled) {
        res.writeHead(404);
        res.end();
      }
    } catch (error) {
      res.writeHead(error.statusCode || 500);
      res.end(error.message);
    }
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}`;
  const send = (url, body, headers = {}) =>
    fetch(base + url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', origin: base, ...headers },
      body: JSON.stringify(body),
    });
  const blocked = await send(
    '/api/member/login',
    { username: 'first.member', password },
    { origin: 'https://other.invalid' },
  );
  assert.equal(blocked.status, 403);
  assert.equal(blocked.headers.get('set-cookie'), null);
  db.exec(
    "CREATE TEMP TRIGGER fail_session BEFORE INSERT ON member_sessions BEGIN SELECT RAISE(ABORT,'session failure'); END",
  );
  const failed = await send('/api/member/login', { username: 'first.member', password });
  assert.equal(failed.status, 500);
  assert.equal(failed.headers.get('set-cookie'), null);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n, 0);
  db.exec('DROP TRIGGER fail_session');
  const success = await send('/api/member/login', { username: 'first.member', password });
  assert.equal(success.status, 200);
  const cookie = success.headers.get('set-cookie');
  assert.match(cookie, /HttpOnly/);
  assert.match(cookie, /SameSite=Strict/);
  assert.match(cookie, /Path=\/api\/member/);
  const token = cookie.split(';')[0].split('=')[1];
  assert.equal(
    db.prepare('SELECT token_hash FROM member_sessions').get().token_hash,
    digest(token),
  );
  assert.equal((await authenticateMemberSession(members, token, {})).user.id, 'm1');
  const logout = await send('/api/member/logout', {}, { cookie: cookie.split(';')[0] });
  assert.equal(logout.status, 200);
  assert.match(logout.headers.get('set-cookie'), /Max-Age=0/);
  assert.equal(await authenticateMemberSession(members, token, {}), null);
});
