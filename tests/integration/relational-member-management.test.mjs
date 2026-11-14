import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { EventEmitter } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { meta, keys, NOW, databaseState } from '../helpers/relational-generation.mjs';
import {
  authenticateMemberApiKey,
  authenticateMemberSession,
  createMemberSession,
  logoutMemberSession,
  prepareMemberCredentials,
  withAuthenticatedMemberApiKey,
  withAuthenticatedMemberSession,
} from '../../server/members/authentication.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { relationalStateNotificationsFor } from '../../server/events/relational-state-notifications.ts';

const password = 'synthetic-management-password',
  credentials = await prepareMemberCredentials({ username: 'new.management.member', password }),
  targetKey = 'synthetic-management-target-key';
const registration = () => ({
  memberId: 'new',
  displayName: 'same display name',
  apiKey: 'synthetic-management-new-key',
  credentials,
});
function fixture(t, filename = ':memory:') {
  const f = { db: new Database(filename), env: { OWNER_USER_ID: 'm1' }, commits: [] };
  t.after(() => f.db.open && f.db.close());
  initializeRelationalStorage(f.db, 'member-management');
  const bind = () => {
    f.events = new EventEmitter();
    f.events.on('commit', (event) => f.commits.push(event));
    f.notifier = relationalStateNotificationsFor(f.db, f.events);
    f.members = new RelationalMemberStore(f.db, f.env, f.notifier);
  };
  bind();
  f.write = (write) => f.notifier.commit(() => f.db.transaction(write)());
  f.write(() => {
    for (const [id, key] of [
      ['m1', keys.m1],
      ['m2', keys.m2],
      ['target', targetKey],
    ]) {
      f.members.registerMember(meta(`register-${id}`), {
        memberId: id,
        displayName: 'same display name',
        apiKey: key,
      });
    }
  });
  f.manage = (id, operation, key = keys.m1) =>
    withAuthenticatedMemberApiKey(
      f.members,
      key,
      (member) => operation(meta(id, NOW + 10, member.id), member),
      f.env,
    );
  f.reopen = () => {
    f.db.close();
    f.db = new Database(filename);
    bind();
  };
  return f;
}
const status = (f, metadata, actor, enabled = false) =>
  f.members.setMemberStatus(metadata, 'target', enabled ? 'active' : 'disabled', actor);
const register = (f, metadata, actor) => f.members.registerMember(metadata, registration(), actor);

test('真实 owner Key/Cookie 可注册和改状态，同名成员按稳定 ID；原回执零写且不覆盖后来启用', async (t) => {
  const f = fixture(t),
    owner = await authenticateMemberApiKey(f.members, keys.m1),
    ownerSession = await createMemberSession(f.members, owner, f.env, NOW),
    target = await createMemberSession(
      f.members,
      await authenticateMemberApiKey(f.members, targetKey),
      f.env,
      NOW,
    );
  const first = await f.manage('owner-register', (metadata, actor) => register(f, metadata, actor));
  assert.deepEqual(first.receipt, { memberId: 'new' });
  assert.equal((await f.members.loginMemberPassword(credentials.username, password)).id, 'new');
  assert.equal((await authenticateMemberApiKey(f.members, registration().apiKey)).id, 'new');
  const stop = await withAuthenticatedMemberSession(
    f.members,
    ownerSession.token,
    f.env,
    (actor) => status(f, meta('owner-disable', NOW + 10, actor.id), actor),
    NOW + 1,
  );
  assert.deepEqual(stop.receipt, { memberId: 'target' });
  assert.equal(f.members.readMember('m1').status, 'active');
  assert.equal(f.members.readMember('m2').status, 'active');
  assert.equal(await authenticateMemberSession(f.members, target.token, f.env, NOW + 2), null);
  await f.manage('owner-enable', (metadata, actor) => status(f, metadata, actor, true));
  const before = databaseState(f.db);
  f.commits.length = 0;
  assert.equal((await f.manage('owner-register', (m, a) => register(f, m, a))).replayed, true);
  assert.equal((await f.manage('owner-disable', (m, a) => status(f, m, a))).replayed, true);
  assert.equal(f.members.readMember('target').status, 'active');
  assert.equal(databaseState(f.db), before);
  assert.deepEqual(f.commits, []);
  await assert.rejects(
    f.manage('owner-register', (metadata, actor) =>
      register(f, { ...metadata, fingerprint: 'a'.repeat(64) }, actor),
    ),
    { statusCode: 409 },
  );
  assert.equal(databaseState(f.db), before);
});

test('两命令在原回执前核当前 stable ID/owner/active/epoch，metadata 和旧快照不能自证授权', async (t) => {
  for (const [name, operation] of [
    ['register', register],
    ['status', status],
  ]) {
    for (const change of ['missing', 'identity', 'owner', 'epoch', 'disabled']) {
      await t.test(`${name}:${change}`, async (t) => {
        const f = fixture(t),
          member = await authenticateMemberApiKey(f.members, keys.m1),
          command = meta('original', NOW + 10, 'm1');
        await f.manage('original', (metadata, actor) => operation(f, metadata, actor));
        let actor = member;
        if (change === 'missing') actor = null;
        if (change === 'identity') actor = { ...member, id: 'm2' };
        if (change === 'owner') f.env.OWNER_USER_ID = 'm2';
        if (change === 'epoch') {
          const prepared = await f.members.prepareMemberCredentials('m1', {
            username: 'changed.owner',
            password,
          });
          f.write(() => f.members.setMemberCredentials(prepared));
        }
        if (change === 'disabled') {
          f.write(() => f.members.setMemberStatus(meta('disable-owner'), 'm1', 'disabled'));
        }
        const before = databaseState(f.db);
        f.write(() => {
          assert.throws(() => operation(f, command, actor), { statusCode: 403 });
        });
        assert.equal(databaseState(f.db), before);
      });
    }
    await t.test(`${name}:current credentials`, async (t) => {
      const f = fixture(t);
      await f.manage('original', (m, a) => operation(f, m, a));
      const owner = await authenticateMemberApiKey(f.members, keys.m1),
        session = await createMemberSession(f.members, owner, f.env, NOW);
      await assert.rejects(
        f.manage('other-member', (m, a) => operation(f, m, a), keys.m2),
        {
          statusCode: 403,
        },
      );
      f.write(() => f.members.replaceMemberApiKey('m1', 'synthetic-replacement-key'));
      const afterKey = databaseState(f.db);
      assert.equal(await f.manage('original', (m, a) => operation(f, m, a)), null);
      assert.equal(databaseState(f.db), afterKey);
      await logoutMemberSession(f.members, session.token);
      const before = databaseState(f.db);
      assert.equal(
        await withAuthenticatedMemberSession(
          f.members,
          session.token,
          f.env,
          (actor) => operation(f, meta('original', NOW + 10, 'm1'), actor),
          NOW + 1,
        ),
        null,
      );
      assert.equal(databaseState(f.db), before);
    });
  }
});

test('owner 自停用归状态 owner 拒绝，admin/system 既有入口保留且 admission 不扩成 system', async (t) => {
  const f = fixture(t),
    before = databaseState(f.db);
  await assert.rejects(
    f.manage('self-disable', (metadata, actor) =>
      f.members.setMemberStatus(metadata, 'm1', 'disabled', actor),
    ),
    { statusCode: 409 },
  );
  assert.equal(databaseState(f.db), before);
  f.write(() => {
    f.members.setMemberStatus(
      { ...meta('admin-disable'), actorKind: 'admin', actorId: 'local-admin' },
      'm1',
      'disabled',
    );
    f.members.setMemberStatus(meta('system-enable'), 'm1', 'active');
    f.members.registerMember(
      { ...meta('admin-register'), actorKind: 'admin', actorId: 'local-admin' },
      registration(),
    );
  });
  assert.equal(f.members.readMember('m1').status, 'active');
  assert.equal((await authenticateMemberApiKey(f.members, keys.m1)).id, 'm1');
  f.write(() =>
    assert.throws(
      () =>
        f.members.changeMemberAdmission(
          meta('system-admission'),
          { memberId: 'm2', acceptingJobs: false },
          null,
        ),
      { statusCode: 403 },
    ),
  );
});

test('当前 Discord 资格沿原认证 owner 核验，失去或过期资格不能重领两命令回执', async (t) => {
  const f = fixture(t);
  Object.assign(f.env, {
    DISCORD_GUILD_ID: '123456789012345678',
    DISCORD_CLIENT_ID: '223456789012345678',
    DISCORD_CLIENT_SECRET: 'synthetic-client-secret',
    PUBLIC_BASE_URL: 'https://gateway.example',
  });
  const observe = async (member, checkedAt) =>
    f.members.discordAuthenticationTransaction((tx) => {
      const user = tx.readMember('m1');
      user.discord = {
        id: '323456789012345678',
        guildId: f.env.DISCORD_GUILD_ID,
        member,
        checkedAt,
      };
      tx.saveAuthentication(user, 'observation');
    });
  await observe(true, Date.now());
  await f.manage('registered', (m, a) => register(f, m, a));
  await f.manage('disabled', (m, a) => status(f, m, a));
  for (const [member, checkedAt, statusCode] of [
    [false, Date.now(), 403],
    [true, Date.now() - 6 * 60000, 503],
  ]) {
    await observe(member, checkedAt);
    const before = databaseState(f.db);
    await assert.rejects(
      f.manage('registered', (m, a) => register(f, m, a)),
      { statusCode },
    );
    await assert.rejects(
      f.manage('disabled', (m, a) => status(f, m, a)),
      { statusCode },
    );
    assert.equal(databaseState(f.db), before);
  }
});

test('两命令 SQL 尾部失败被外层 catch，事实/私有状态/会话/回执全回滚且无幽灵通知', async (t) => {
  for (const [name, operation, trigger] of [
    ['register', register, "AFTER INSERT ON api_keys WHEN NEW.member_id='new'"],
    ['status', status, "AFTER DELETE ON member_sessions WHEN OLD.member_id='target'"],
  ]) {
    await t.test(name, async (t) => {
      const f = fixture(t);
      await createMemberSession(
        f.members,
        await authenticateMemberApiKey(f.members, targetKey),
        f.env,
        NOW,
      );
      f.db.exec(
        `CREATE TEMP TRIGGER fail_tail ${trigger} BEGIN SELECT RAISE(ABORT,'tail fault'); END`,
      );
      const before = databaseState(f.db);
      f.commits.length = 0;
      await f.manage('atomic', (metadata, actor) => {
        assert.throws(() => operation(f, metadata, actor), /tail fault/);
      });
      assert.equal(databaseState(f.db), before);
      assert.deepEqual(f.commits, []);
      f.db.exec('DROP TRIGGER fail_tail');
      assert.equal((await f.manage('atomic', (m, a) => operation(f, m, a))).replayed, false);
      assert.equal(f.commits.length, 1);
      const saved = databaseState(f.db);
      f.commits.length = 0;
      assert.equal((await f.manage('atomic', (m, a) => operation(f, m, a))).replayed, true);
      assert.equal(databaseState(f.db), saved);
      assert.deepEqual(f.commits, []);
    });
  }
});

test('重开及原回执不复活旧 epoch/Cookie，全公共提交恢复身份和状态但不恢复任何私有认证或外联', async (t) => {
  const dir = await mkdtemp(path.join(tmpdir(), 'member-owner-'));
  const f = fixture(t, path.join(dir, 'source.sqlite')),
    ownerSession = await createMemberSession(
      f.members,
      await authenticateMemberApiKey(f.members, keys.m1),
      f.env,
      NOW,
    ),
    targetSession = await createMemberSession(
      f.members,
      await authenticateMemberApiKey(f.members, targetKey),
      f.env,
      NOW,
    );
  t.after(() => rm(dir, { recursive: true, force: true }));
  await f.manage('registered', (m, a) => register(f, m, a));
  await f.manage('disabled', (m, a) => status(f, m, a));
  await f.manage('enabled', (m, a) => status(f, m, a, true));
  const epoch = (await authenticateMemberApiKey(f.members, targetKey)).memberAuth.version;
  f.reopen();
  assert.ok(await authenticateMemberSession(f.members, ownerSession.token, f.env, NOW + 1));
  assert.equal(
    await authenticateMemberSession(f.members, targetSession.token, f.env, NOW + 1),
    null,
  );
  const before = databaseState(f.db);
  assert.equal((await f.manage('disabled', (m, a) => status(f, m, a))).replayed, true);
  assert.equal((await f.manage('registered', (m, a) => register(f, m, a))).replayed, true);
  assert.equal((await authenticateMemberApiKey(f.members, targetKey)).memberAuth.version, epoch);
  assert.equal(f.members.readMember('target').status, 'active');
  assert.equal(databaseState(f.db), before);
  const commits = f.db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all()
      .map(({ seq }) => readRelationalCommit(f.db, seq)),
    target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'member-management');
  const replay = new RelationalCommitReplayer(target),
    previousFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    throw Error('unexpected network');
  };
  try {
    for (const commit of commits) target.transaction(() => replay.replayCommit(commit))();
    const saved = databaseState(target);
    for (const commit of commits) {
      target.transaction(() => assert.equal(replay.replayCommit(commit).replayed, true))();
    }
    assert.equal(databaseState(target), saved);
  } finally {
    globalThis.fetch = previousFetch;
  }
  assert.equal(calls, 0);
  const publicMembers = (db) =>
    db
      .prepare(
        'SELECT id,display_name,status,accepting_jobs,admission_epoch,created_at,source_event_id FROM members ORDER BY id',
      )
      .all();
  assert.deepEqual(publicMembers(target), publicMembers(f.db));
  for (const table of ['commits', 'domain_events', 'stream_heads']) {
    const rows = (db) => db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all();
    assert.deepEqual(rows(target), rows(f.db), table);
  }
  assert.equal(
    target.prepare('SELECT COUNT(*) AS n FROM members WHERE auth_epoch IS NOT NULL').get().n,
    0,
  );
  for (const table of [
    'password_credentials',
    'api_keys',
    'member_sessions',
    'member_auth_state',
    'discord_bindings',
    'discord_invites',
  ]) {
    assert.equal(target.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0, table);
  }
  const publicText = JSON.stringify(commits);
  for (const secret of [
    password,
    registration().apiKey,
    keys.m1,
    targetKey,
    credentials.salt,
    credentials.hash,
    credentials.version,
    ownerSession.token,
    targetSession.token,
    epoch,
  ]) {
    assert.equal(publicText.includes(secret), false, secret);
  }
  assert.equal(
    await authenticateMemberApiKey(new RelationalMemberStore(target, f.env), keys.m1),
    null,
  );
});
