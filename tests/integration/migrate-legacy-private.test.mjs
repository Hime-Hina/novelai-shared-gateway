import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, scryptSync } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { migrateLegacyPrivate } from '../../scripts/migrate-legacy-private.mjs';
import { domainDigest } from '../../server/events/domain-events.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import {
  relationalPrivateStateEmpty,
  relationalSnapshotSha256,
} from '../../server/storage/relational-private-restore.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import {
  authenticateMemberApiKey,
  authenticateMemberSession,
} from '../../server/members/authentication.ts';
import {
  discordLoginEnabled,
  requireDiscordMembership,
} from '../../server/members/discord-auth.ts';
import { initializeFixtureConfiguration } from '../helpers/relational-configuration.mjs';
import { databaseState, meta } from '../helpers/relational-generation.mjs';

const digest = (value) => createHash('sha256').update(value).digest('hex');
const password = 'synthetic-private-password';
const salt = 'b'.repeat(48);
const passwordHash = scryptSync(password, salt, 64).toString('hex');
const epoch = 'a'.repeat(32);
const configuredOwner = { ownerUserId: 'owner', guildId: '223456789012345678' };
const discord = {
  id: '123456789012345678',
  guildId: configuredOwner.guildId,
  member: true,
  checkedAt: 900,
  expiresAt: 5000,
  accessToken: 'synthetic-access',
  refreshToken: 'synthetic-refresh',
};
const cookie = 'a'.repeat(43);
const session = (userId, values = {}) => ({
  token_hash: digest(userId),
  user_id: userId,
  auth_version: epoch,
  created_at: 10,
  expires_at: 5000,
  owner_binding: null,
  ...values,
});
const row = (data) => ({ id: data.id, token: data.token ?? null, data });

function fixture(t, change = () => {}) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'private-migration-history');
  initializeFixtureConfiguration(db);
  const normalized = {
    effectiveAt: 1000,
    discordLoginEnabled: true,
    configuredOwner: { ...configuredOwner },
    sessionOwner: { ...configuredOwner },
    users: [
      row({
        id: 'owner',
        enabled: true,
        token: 'STA1N-current-owner',
        note: 'private note\ud83d',
        memberAuth: { username: 'legacy-owner', salt, hash: passwordHash, version: epoch },
        discord: { ...discord },
      }),
      row({ id: 'api-only', enabled: true, token: 'STA1N-current-api-only' }),
      row({
        id: 'ordinary',
        enabled: true,
        token: 'STA1N-current-ordinary',
        memberAuth: { username: 'old-discord-username', version: epoch },
      }),
      row({
        id: 'disabled',
        enabled: false,
        token: 'STA1N-disabled',
        memberAuth: { username: 'disabled-name', salt, hash: passwordHash, version: epoch },
      }),
      row({
        id: 'revoked',
        enabled: true,
        token: 'STA1N-revoked',
        memberAuth: { version: epoch },
        discord: { ...discord, id: '123456789012345679', member: false },
      }),
    ],
    accounts: [
      row({
        id: 'account',
        enabled: false,
        token: 'pst-synthetic-current',
        socksProxy: '127.0.0.1:1080',
      }),
    ],
    sessions: [
      session('owner', {
        token_hash: digest(cookie),
        owner_binding: JSON.stringify(['owner', discord.id, configuredOwner.guildId]),
      }),
      session('ordinary'),
      session('owner', { token_hash: digest('old-epoch'), auth_version: 'old-epoch' }),
      session('owner', { token_hash: digest('expired'), expires_at: 1000 }),
      session('api-only', { auth_version: 'unproved-old-version' }),
      session('disabled'),
      session('revoked'),
      session('removed'),
    ],
  };
  change(normalized);
  const sourceSha256 = 'c'.repeat(64),
    sourceId = 'legacy:' + sourceSha256;
  const refs = {
    sourceId,
    sourceSha256,
    origin: (originalId) => ({ sourceId, originalId }),
    eventId: (eventType, originalId) =>
      'migration:' + domainDigest({ sourceSha256, eventType, originalId }),
    memberIds: new Set([...normalized.users.map((value) => value.id), 'removed']),
    accountIds: new Set(normalized.accounts.map((value) => value.id)),
  };
  const event = (eventType, payload, id, kind) => ({
    eventType,
    payload,
    eventId: refs.eventId(eventType, id),
    streamId: `${kind}:${id}`,
    memberId: kind === 'member' ? id : null,
    accountId: kind === 'account' ? id : null,
    occurredAt: 1000,
    policyVersion: 'legacy-snapshot',
    pricingVersion: 'legacy-snapshot',
  });
  const events = [
    event(
      'ImportSourceRegistered',
      {
        sourceId,
        sourceSha256,
        sourceCommit: '3c-synthetic',
        cutoffSeq: '1',
        cutoffAt: 1000,
        manifestSha256: 'd'.repeat(64),
        route: 'B',
      },
      sourceId,
      'import',
    ),
    ...[
      ...normalized.users.map((value) => ({
        id: value.id,
        disabled: value.data.enabled === false,
      })),
      { id: 'removed', removed: true },
    ].map((user) =>
      event(
        'OpeningMemberRecorded',
        {
          memberId: user.id,
          displayName: 'public-' + user.id,
          status: user.removed ? 'removed' : user.disabled ? 'disabled' : 'active',
          acceptingJobs: !user.removed && !user.disabled,
          admissionEpoch: 0,
          createdAt: 0,
          origin: refs.origin(user.id),
        },
        user.id,
        'member',
      ),
    ),
    ...normalized.accounts.map((value) =>
      event(
        'OpeningAccountRecorded',
        {
          accountId: value.id,
          displayName: 'public-account',
          weight: null,
          enabled: value.data.enabled !== false,
          credentialEpoch: 1,
          cooldownUntil: null,
          cooldownReason: null,
          ordinaryStrikes: 0,
          capacityStrikes: 0,
          capacityLimitedUntil: null,
          capacityLastErrorAt: null,
          origin: refs.origin(value.id),
        },
        value.id,
        'account',
      ),
    ),
  ];
  const { policyVersion: _p, pricingVersion: _v, ...metadata } = meta('private-opening', 1000);
  db.transaction(() =>
    new RelationalCommitReplayer(db).appendCommit({
      ...metadata,
      fingerprint: domainDigest(events),
      receipt: { opened: true },
      expectedVersions: Object.fromEntries(events.map((value) => [value.streamId, '0'])),
      events,
    }),
  )();
  return { db, normalized, refs };
}

test('旧私有当前材料保原version/期限/owner；API-only建立新首代但不制造旧Cookie，未知与撤销不复活', async (t) => {
  const f = fixture(t);
  const summary = f.db.transaction(() => migrateLegacyPrivate(f.db, f.normalized, f.refs))();
  assert.equal(summary.sessions, 2);
  assert.equal(summary.omittedSessions, 6);
  assert.equal(summary.newBaselineAuthEpochs, 1);
  assert.equal(
    f.db.prepare("SELECT auth_epoch FROM members WHERE id='owner'").get().auth_epoch,
    epoch,
  );
  assert.match(
    f.db.prepare("SELECT auth_epoch FROM members WHERE id='api-only'").get().auth_epoch,
    /^[a-f0-9]{32}$/,
  );
  assert.equal(
    f.db.prepare("SELECT note_json FROM members WHERE id='owner'").get().note_json,
    JSON.stringify('private note\ud83d'),
  );
  for (const id of ['disabled', 'revoked', 'removed'])
    assert.equal(
      f.db.prepare('SELECT auth_epoch FROM members WHERE id=?').get(id).auth_epoch,
      null,
    );
  assert.equal(
    f.db
      .prepare(
        "SELECT COUNT(*) AS n FROM api_keys WHERE member_id IN ('disabled','revoked','removed')",
      )
      .get().n,
    0,
  );
  assert.deepEqual(
    f.db
      .prepare("SELECT salt,password_hash FROM password_credentials WHERE member_id='ordinary'")
      .get(),
    { salt: null, password_hash: null },
  );
  assert.deepEqual(
    f.db.prepare("SELECT pst,proxy_url FROM upstream_credentials WHERE account_id='account'").get(),
    { pst: 'pst-synthetic-current', proxy_url: 'socks5://127.0.0.1:1080' },
  );
  assert.equal(
    f.db.prepare("SELECT credential_epoch FROM upstream_accounts WHERE id='account'").get()
      .credential_epoch,
    1,
  );
  const stored = f.db
    .prepare('SELECT * FROM member_sessions WHERE token_hash=?')
    .get(digest(cookie));
  assert.equal(stored.created_at, 10);
  assert.equal(stored.expires_at, 5000);
  assert.equal(stored.auth_epoch, epoch);
  const before = databaseState(f.db);
  assert.throws(
    () => f.db.transaction(() => migrateLegacyPrivate(f.db, f.normalized, f.refs))(),
    /Selected legacy private/,
  );
  assert.equal(databaseState(f.db), before, 'existing private state is never overwritten');
  const publicBytes = f.db
    .prepare('SELECT payload FROM domain_events')
    .all()
    .map((value) => value.payload)
    .join('\n');
  for (const secret of [
    passwordHash,
    'STA1N-current-owner',
    'pst-synthetic-current',
    'private note',
  ])
    assert.equal(publicBytes.includes(secret), false);
});

test('旧owner ack/config变化不能留旧owner或新owner Cookie；无原version不能借新API首代启用旧password', async (t) => {
  const f = fixture(t, (value) => {
    value.configuredOwner = { ownerUserId: 'ordinary', guildId: configuredOwner.guildId };
    value.users.find((row) => row.id === 'api-only').data.memberAuth = {
      username: 'unproved-password',
      salt,
      hash: passwordHash,
    };
  });
  const summary = f.db.transaction(() => migrateLegacyPrivate(f.db, f.normalized, f.refs))();
  assert.equal(summary.unavailablePasswords, 1);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n, 0);
  const members = new RelationalMemberStore(f.db);
  assert.equal((await authenticateMemberApiKey(members, 'STA1N-current-api-only')).id, 'api-only');
  await assert.rejects(
    members.loginMemberPassword('unproved-password', password),
    /用户名或密码错误/,
  );
});

test('原Discord关闭时忽略旧false缓存保当前Key和原Cookie；启用时含owner仍拒且不凭空bypass', async (t) => {
  t.mock.method(Date, 'now', () => 1000);
  const disabledEnvironment = { OWNER_USER_ID: configuredOwner.ownerUserId };
  assert.equal(discordLoginEnabled(disabledEnvironment), false);
  const disabled = fixture(t, (value) => {
    value.discordLoginEnabled = false;
    value.configuredOwner.guildId = '';
    value.sessionOwner.guildId = '';
    value.sessions.find((row) => row.user_id === 'revoked').token_hash = digest('r'.repeat(43));
  });
  const disabledUser = disabled.normalized.users.find((row) => row.id === 'revoked').data;
  await requireDiscordMembership(disabledUser, {
    env: disabledEnvironment,
    store: { update: () => assert.fail('disabled Discord must return before any IO') },
  });
  disabled.db.transaction(() =>
    migrateLegacyPrivate(disabled.db, disabled.normalized, disabled.refs),
  )();
  const members = new RelationalMemberStore(disabled.db);
  assert.equal((await authenticateMemberApiKey(members, disabledUser.token))?.id, 'revoked');
  const originalSession = await authenticateMemberSession(
    members,
    'r'.repeat(43),
    disabledEnvironment,
    1000,
  );
  assert.equal(originalSession.user.id, 'revoked');
  assert.equal(originalSession.expiresAt, 5000);
  assert.equal(
    await authenticateMemberSession(members, 'r'.repeat(43), disabledEnvironment, 5000),
    null,
  );
  const enabled = fixture(t, (value) => {
    value.users.find((row) => row.id === 'owner').data.discord.member = false;
  });
  const enabledEnvironment = {
    OWNER_USER_ID: configuredOwner.ownerUserId,
    DISCORD_GUILD_ID: configuredOwner.guildId,
    DISCORD_CLIENT_ID: '323456789012345678',
    DISCORD_CLIENT_SECRET: 'synthetic-discord-secret',
    PUBLIC_BASE_URL: 'https://gateway.example',
  };
  assert.equal(discordLoginEnabled(enabledEnvironment), true);
  for (const id of ['owner', 'revoked']) {
    await assert.rejects(
      requireDiscordMembership(enabled.normalized.users.find((row) => row.id === id).data, {
        env: enabledEnvironment,
        store: { update: () => assert.fail('cached false must reject before IO') },
      }),
      (error) => error.statusCode === 403 && /不在指定 Discord/.test(error.message),
    );
  }
  enabled.db.transaction(() =>
    migrateLegacyPrivate(enabled.db, enabled.normalized, enabled.refs),
  )();
  const enabledMembers = new RelationalMemberStore(enabled.db);
  for (const key of ['STA1N-current-owner', 'STA1N-revoked'])
    assert.equal(await authenticateMemberApiKey(enabledMembers, key), null);
  const partial = fixture(t, (value) => {
    value.configuredOwner.guildId = '';
    value.sessionOwner.guildId = '';
    value.discordLoginEnabled = true;
  });
  assert.equal(discordLoginEnabled({ DISCORD_CLIENT_ID: 'configured-without-guild' }), true);
  partial.db.transaction(() =>
    migrateLegacyPrivate(partial.db, partial.normalized, partial.refs),
  )();
  assert.equal(
    await authenticateMemberApiKey(new RelationalMemberStore(partial.db), 'STA1N-revoked'),
    null,
  );
  for (const enabled of [undefined, false]) {
    const invalid = fixture(t, (value) => {
      value.discordLoginEnabled = enabled;
    });
    const before = databaseState(invalid.db);
    assert.throws(
      () =>
        invalid.db.transaction(() =>
          migrateLegacyPrivate(invalid.db, invalid.normalized, invalid.refs),
        )(),
      /Selected legacy private/,
    );
    assert.equal(databaseState(invalid.db), before);
  }
});

test('原source/origin/current token错配拒绝；真实私有尾INSERT失败外catch回滚全部私有且可健康重试', (t) => {
  const f = fixture(t),
    before = databaseState(f.db);
  assert.throws(
    () =>
      f.db.transaction(() =>
        migrateLegacyPrivate(f.db, f.normalized, { ...f.refs, sourceSha256: 'f'.repeat(64) }),
      )(),
    /Selected legacy private/,
  );
  assert.throws(
    () =>
      f.db.transaction(() =>
        migrateLegacyPrivate(f.db, f.normalized, {
          ...f.refs,
          origin: (id) => ({ sourceId: f.refs.sourceId, originalId: 'different:' + id }),
        }),
      )(),
    /Selected legacy private/,
  );
  const wrong = structuredClone(f.normalized);
  wrong.users[0].token = 'STA1N-old-key';
  assert.throws(
    () => f.db.transaction(() => migrateLegacyPrivate(f.db, wrong, f.refs))(),
    /Selected legacy private/,
  );
  assert.equal(databaseState(f.db), before);
  f.db.exec(
    "CREATE TEMP TRIGGER private_tail_fault BEFORE INSERT ON upstream_credentials WHEN (SELECT COUNT(*) FROM api_keys)>0 AND (SELECT COUNT(*) FROM member_sessions)>0 BEGIN SELECT RAISE(ABORT,'private tail after auth writes'); END",
  );
  f.db.transaction(() => {
    assert.throws(
      () => migrateLegacyPrivate(f.db, f.normalized, f.refs),
      /private tail after auth writes/,
    );
  })();
  assert.equal(databaseState(f.db), before);
  assert.equal(relationalPrivateStateEmpty(f.db), true);
  f.db.exec('DROP TRIGGER private_tail_fault');
  f.db.transaction(() => migrateLegacyPrivate(f.db, f.normalized, f.refs))();
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n, 2);
});

test('缺原owner ack不保Cookie，邀请只保原未过期截止；重复用户名和非法旧代理不能部分安装或回显', (t) => {
  const f = fixture(t, (value) => {
    value.sessionOwner = null;
    value.users.find((row) => row.id === 'api-only').data.discordInvite = {
      hash: 'e'.repeat(64),
      expiresAt: 1001,
    };
    value.users.find((row) => row.id === 'ordinary').data.discordInvite = {
      hash: 'f'.repeat(64),
      expiresAt: 1000,
    };
  });
  const before = databaseState(f.db),
    duplicate = structuredClone(f.normalized);
  duplicate.users.find((row) => row.id === 'ordinary').data.memberAuth.username = 'legacy-owner';
  assert.throws(
    () => f.db.transaction(() => migrateLegacyPrivate(f.db, duplicate, f.refs))(),
    /UNIQUE constraint failed/,
  );
  assert.equal(databaseState(f.db), before);
  const invalid = structuredClone(f.normalized);
  invalid.accounts[0].data.socksProxy = '127.0.0.1:1080:private-user:private-password';
  assert.throws(
    () => f.db.transaction(() => migrateLegacyPrivate(f.db, invalid, f.refs))(),
    (error) =>
      error.message ===
        'Selected legacy private state is incomplete or conflicts with its B opening.' &&
      !('input' in error),
  );
  assert.equal(databaseState(f.db), before);
  f.db.transaction(() => migrateLegacyPrivate(f.db, f.normalized, f.refs))();
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n, 0);
  assert.deepEqual(f.db.prepare('SELECT member_id,expires_at FROM discord_invites').all(), [
    { member_id: 'api-only', expires_at: 1001 },
  ]);
});

test('另一个合法登记source不能借同ID原Opening来接收被选私有材料', (t) => {
  const f = fixture(t),
    sourceSha256 = 'f'.repeat(64),
    sourceId = 'legacy:' + sourceSha256;
  const event = {
    eventId: 'second-private-source',
    eventType: 'ImportSourceRegistered',
    streamId: `import:${sourceId}`,
    memberId: null,
    accountId: null,
    occurredAt: 1000,
    policyVersion: 'legacy-snapshot',
    pricingVersion: 'legacy-snapshot',
    payload: {
      sourceId,
      sourceSha256,
      sourceCommit: '3c-synthetic',
      cutoffSeq: '1',
      cutoffAt: 1000,
      manifestSha256: 'd'.repeat(64),
      route: 'B',
    },
  };
  const {
    policyVersion: _p,
    pricingVersion: _v,
    ...metadata
  } = meta('second-private-source', 1000);
  f.db.transaction(() =>
    new RelationalCommitReplayer(f.db).appendCommit({
      ...metadata,
      fingerprint: domainDigest(event),
      receipt: {},
      events: [event],
      expectedVersions: { [event.streamId]: '0' },
    }),
  )();
  const before = databaseState(f.db);
  assert.throws(
    () =>
      f.db.transaction(() =>
        migrateLegacyPrivate(f.db, f.normalized, { ...f.refs, sourceId, sourceSha256 }),
      )(),
    /Selected legacy private/,
  );
  assert.equal(databaseState(f.db), before);
});

test('新samecut源公共重建不生auth，选定私有与唯一installer联合恢复；旧session期限不延长', async (t) => {
  const f = fixture(t),
    dir = await mkdtemp(path.join(tmpdir(), 'legacy-private-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  f.db.transaction(() => migrateLegacyPrivate(f.db, f.normalized, f.refs))();
  const sourcePath = path.join(dir, 'selected.sqlite');
  await f.db.backup(sourcePath);
  const selected = {
    sourcePath,
    sourceSha256: relationalSnapshotSha256(sourcePath),
    historyId: 'private-migration-history',
    finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS n FROM commits').get().n,
    replicas: { dataDir: dir },
  };
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, selected.historyId);
  const replayer = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db);
  t.mock.method(globalThis, 'fetch', () =>
    assert.fail('public replay and private installation have no network'),
  );
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all())
    target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), inputs))();
  assert.equal(relationalPrivateStateEmpty(target), true);
  await verifyAndInstallRelationalStorage(target, selected);
  const members = new RelationalMemberStore(target),
    env = { OWNER_USER_ID: configuredOwner.ownerUserId, DISCORD_GUILD_ID: configuredOwner.guildId };
  assert.equal((await members.loginMemberPassword('legacy-owner', password)).id, 'owner');
  assert.equal((await authenticateMemberApiKey(members, 'STA1N-current-api-only')).id, 'api-only');
  const authenticated = await authenticateMemberSession(members, cookie, env, 1000);
  assert.equal(authenticated.user.id, 'owner');
  assert.equal(authenticated.expiresAt, 5000);
  assert.equal(await authenticateMemberSession(members, cookie, env, 5000), null);
  assert.equal(await authenticateMemberApiKey(members, 'STA1N-disabled'), null);
  const before = databaseState(target);
  assert.throws(
    () => target.transaction(() => migrateLegacyPrivate(target, f.normalized, f.refs))(),
    /Selected legacy private/,
  );
  assert.equal(databaseState(target), before);
  assert.equal(relationalSnapshotSha256(sourcePath), selected.sourceSha256);
});
