import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { fixture, meta, keys, NOW, databaseState } from '../helpers/relational-generation.mjs';
import {
  authenticateMemberApiKey,
  createMemberSession,
  prepareMemberCredentials,
} from '../../server/members/authentication.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import {
  restoreRelationalPrivateSnapshot,
  relationalSnapshotSha256,
} from '../../server/storage/relational-private-restore.ts';

const password = 'synthetic-directory-password',
  namedKey = 'synthetic-directory-named-key',
  credentials = await prepareMemberCredentials({ username: 'original.login', password }),
  fields = [
    'id',
    'displayName',
    'status',
    'acceptingJobs',
    'username',
    'discordId',
    'inviteExpiresAt',
    'note',
  ];

function registerNamed(f) {
  f.db.transaction(() =>
    f.members.registerMember(meta('register-named'), {
      memberId: 'named',
      displayName: 'm1',
      apiKey: namedKey,
      credentials,
      note: 'private directory note',
    }),
  )();
}

test('管理目录按稳定ID分开同名成员，保留Key-only未知身份且query_only下不读秘密或写任何表', async (t) => {
  const f = await fixture(t);
  registerNamed(f);
  await f.members.discordAuthenticationTransaction((tx) => {
    const user = tx.readMember('named');
    user.discord = {
      id: '222222222222222222',
      guildId: '1134557553011998840',
      accessToken: 'synthetic-private-directory-access',
      refreshToken: 'synthetic-private-directory-refresh',
      expiresAt: NOW + 86400000,
      checkedAt: NOW,
      member: true,
    };
    tx.saveAuthentication(user, 'login');
  });
  const keyOnly = await authenticateMemberApiKey(f.members, keys.m1);
  assert.equal(keyOnly.id, 'm1');
  const session = await createMemberSession(f.members, keyOnly, {}, NOW),
    invite = f.db.transaction(() => f.members.createMemberDiscordInvite('m1', null, NOW))(),
    inviteHash = f.db
      .prepare("SELECT code_hash FROM discord_invites WHERE member_id='m1'")
      .get().code_hash,
    before = databaseState(f.db),
    statements = [],
    prepare = f.db.prepare;
  f.db.pragma('query_only=ON');
  f.db.prepare = function (sql) {
    statements.push(sql);
    return prepare.call(this, sql);
  };
  let rows;
  try {
    rows = f.members.readManagementMemberProfiles();
    assert.deepEqual(
      rows.map((row) => row.id),
      ['m1', 'm2', 'named'],
    );
    assert.deepEqual(
      rows.filter((row) => row.displayName === 'm1').map((row) => row.id),
      ['m1', 'named'],
    );
    for (const row of rows) {
      assert.deepEqual(Object.keys(row), fields);
      assert.deepEqual(f.members.readManagementMemberProfile(row.id), row);
    }
    assert.deepEqual(rows[0], {
      id: 'm1',
      displayName: 'm1',
      status: 'active',
      acceptingJobs: true,
      username: null,
      discordId: null,
      inviteExpiresAt: invite.expiresAt,
      note: null,
    });
    assert.equal(rows[2].username, 'original.login');
    assert.equal(rows[2].displayName, 'm1');
    assert.equal(rows[2].discordId, '222222222222222222');
    assert.equal(rows[2].inviteExpiresAt, null);
    assert.equal(rows[2].note, 'private directory note');
    assert.equal(f.members.readManagementMemberProfile('original.login'), null);
    assert.equal(f.members.readManagementMemberProfile("m1' OR 1=1 --"), null);
  } finally {
    f.db.prepare = prepare;
    f.db.pragma('query_only=OFF');
  }
  assert.equal(databaseState(f.db), before);
  assert.equal(
    statements.some((sql) =>
      /api_keys|member_sessions|auth_epoch|password_hash|\bsalt\b|access_token|refresh_token|code_hash|\bbody\b|\bbytes\b|SELECT\s+\*/i.test(
        sql,
      ),
    ),
    false,
  );
  const publicText = JSON.stringify(rows);
  for (const secret of [
    keys.m1,
    keys.m2,
    namedKey,
    password,
    credentials.salt,
    credentials.hash,
    credentials.version,
    session.token,
    invite.code,
    inviteHash,
    'synthetic-private-directory-access',
    'synthetic-private-directory-refresh',
    'synthetic-upstream-pst',
  ]) {
    assert.equal(publicText.includes(secret), false, secret);
  }
});

test('disabled仍是管理目标，实际权益移除后不再列出；未知与真实截止0不会相互补值', async (t) => {
  const f = await fixture(t),
    quotas = new RelationalQuotaStore(f.db);
  f.db.transaction(() => f.members.setMemberStatus(meta('disable-m2'), 'm2', 'disabled'))();
  assert.deepEqual(f.members.readManagementMemberProfile('m2'), {
    id: 'm2',
    displayName: 'm2',
    status: 'disabled',
    acceptingJobs: true,
    username: null,
    discordId: null,
    inviteExpiresAt: null,
    note: null,
  });
  assert.equal(f.members.readManagementMemberProfiles().length, 2);
  const invite = f.db.transaction(() => f.members.createMemberDiscordInvite('m1', null, NOW))(),
    stop = new Error('rollback expiry source fault'),
    before = databaseState(f.db);
  assert.throws(
    () =>
      f.db.transaction(() => {
        // 核已保存时间的表示，不把该私有故障输入冒称真实签发操作。
        f.db.prepare("UPDATE discord_invites SET expires_at=0 WHERE member_id='m1'").run();
        assert.equal(f.members.readManagementMemberProfile('m1').inviteExpiresAt, 0);
        throw stop;
      })(),
    (error) => error === stop,
  );
  assert.equal(databaseState(f.db), before);
  assert.equal(f.members.readManagementMemberProfile('m1').inviteExpiresAt, invite.expiresAt);
  f.db.transaction(() =>
    quotas.manageMemberQuota(
      { ...meta('remove-m2'), actorKind: 'admin' },
      { action: 'delete-member', userId: 'm2', transferToUserId: 'm1', expectedConfigVersion: 1 },
      null,
      new Map(),
    ),
  )();
  assert.equal(f.members.readMember('m2').status, 'removed');
  const removed = databaseState(f.db);
  assert.deepEqual(
    f.members.readManagementMemberProfiles().map((row) => row.id),
    ['m1'],
  );
  assert.equal(f.members.readManagementMemberProfile('m2'), null);
  assert.equal(f.members.readManagementMemberProfile('missing'), null);
  assert.equal(databaseState(f.db), removed);
});

test('重开保持目录；完整业务回放不补私有用户名/绑定，选定私有快照恢复后才还原准确metadata', async (t) => {
  const cleanup = [];
  let reopened, target;
  t.after(async () => {
    reopened?.close();
    target?.close();
    for (const run of cleanup.reverse()) {
      await run();
    }
  });
  const f = await fixture({ after: (run) => cleanup.push(run) });
  registerNamed(f);
  await f.members.discordAuthenticationTransaction((tx) => {
    const user = tx.readMember('named');
    // 缺观察的绑定仍有准确ID，目录不把它冒充当前成员资格或填false。
    user.discord = { id: '222222222222222222', guildId: '1134557553011998840' };
    tx.saveAuthentication(user);
  });
  f.db.transaction(() => f.members.createMemberDiscordInvite('m1', null, NOW))();
  const expected = f.members.readManagementMemberProfiles(),
    sourcePath = path.join(f.dir, 'directory-selected.sqlite');
  await f.db.backup(sourcePath);
  const sealed = new Database(sourcePath);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  reopened = new Database(f.db.name);
  assert.deepEqual(new RelationalMemberStore(reopened).readManagementMemberProfiles(), expected);
  target = new Database(':memory:');
  initializeRelationalStorage(target, 'generation-history');
  const replayer = new RelationalCommitReplayer(target),
    source = new RelationalInputStore(f.db),
    members = new RelationalMemberStore(target);
  t.mock.method(globalThis, 'fetch', () => assert.fail('directory/replay must not fetch'));
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), source))();
  }
  const beforePrivate = databaseState(target);
  assert.deepEqual(
    members.readManagementMemberProfiles(),
    expected.map((row) => ({
      ...row,
      username: null,
      discordId: null,
      inviteExpiresAt: null,
      note: null,
    })),
  );
  assert.equal(databaseState(target), beforePrivate);
  target.transaction(() =>
    restoreRelationalPrivateSnapshot(target, sourcePath, relationalSnapshotSha256(sourcePath)),
  )();
  const restored = databaseState(target);
  target.pragma('query_only=ON');
  assert.deepEqual(members.readManagementMemberProfiles(), expected);
  assert.equal(databaseState(target), restored);
});
