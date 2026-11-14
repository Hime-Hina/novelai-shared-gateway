import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { fixture, meta, NOW, keys, databaseState } from '../helpers/relational-generation.mjs';
import { RelationalAccountStore } from '../../server/storage/relational-account-store.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import { ownerAccountCommand, selectSharedAccount } from '../../server/members/owner-operations.ts';

const env = { OWNER_USER_ID: 'm1' };
function store(db) {
  return new RelationalAccountStore(db, new RelationalQuotaStore(db, env), new Map(), env);
}
function readonly(t, db, read) {
  const before = databaseState(db),
    statements = [],
    prepare = db.prepare.bind(db),
    mock = t.mock.method(db, 'prepare', (sql, ...args) => {
      statements.push(sql);
      return prepare(sql, ...args);
    });
  db.pragma('query_only=ON');
  let result;
  try {
    result = read();
  } finally {
    mock.mock.restore();
    db.pragma('query_only=OFF');
  }
  assert.equal(databaseState(db), before);
  assert.doesNotMatch(
    statements.join('\n'),
    /upstream_credentials|\bpst\b|credential_ref|\bgrant\b|member_sessions|member_credentials/i,
  );
  return { result, statements };
}
function replay(t, source) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const owner = new RelationalCommitReplayer(db);
  for (const { seq } of source
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    db.transaction(() =>
      owner.replayCommit(readRelationalCommit(source, seq), new RelationalInputStore(source)),
    )();
  }
  return db;
}

test('账户窄读只取安全当前字段和真实注册时点，0不变为未知，暂停/轮换不覆盖原创建', async (t) => {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const members = new RelationalMemberStore(db),
    quotas = new RelationalQuotaStore(db, env),
    accounts = store(db);
  db.transaction(() =>
    members.registerMember(meta('member'), {
      memberId: 'm1',
      displayName: 'owner',
      apiKey: keys.m1,
    }),
  )();
  const authorize = (write) => withAuthenticatedMemberApiKey(members, keys.m1, write, env);
  await authorize((member) =>
    quotas.configureGroups(
      meta('groups', 0, 'm1'),
      {
        expectedConfigVersion: 1,
        groups: [
          { id: 'g', name: 'shared', share: 1, members: ['m1'], memberWeights: { m1: 1 } },
          ...['g2', 'g3', 'g4'].map((id) => ({
            id,
            name: id,
            share: 0,
            members: [],
            memberWeights: {},
          })),
        ],
      },
      member,
    ),
  );
  await ownerAccountCommand(accounts, authorize, meta('account', 0, 'm1'), {
    kind: 'register',
    accountId: 'stable-account',
    displayName: 'original name',
    token: 'pst-original-secret',
    configVersion: 2,
  });
  const expected = {
    id: 'stable-account',
    name: 'original name',
    weight: 1,
    enabled: false,
    credentialEpoch: 1,
    createdAt: 0,
  };
  assert.deepEqual(readonly(t, db, () => accounts.readAccountManagementDirectory()).result, [
    expected,
  ]);
  assert.deepEqual(
    readonly(t, db, () => accounts.readSharedAccountManagementProfile()).result,
    expected,
  );
  await ownerAccountCommand(accounts, authorize, meta('rotate', NOW, 'm1'), {
    kind: 'token',
    accountId: 'stable-account',
    token: 'pst-rotated-private',
  });
  const rotated = { ...expected, credentialEpoch: 2 };
  assert.deepEqual(
    readonly(t, db, () => accounts.readAccountManagementProfile('stable-account')).result,
    rotated,
  );
  await ownerAccountCommand(accounts, authorize, meta('pause', NOW + 1, 'm1'), {
    kind: 'pause',
    accountId: 'stable-account',
  });
  const { result, statements } = readonly(t, db, () =>
    accounts.readAccountManagementProfile('stable-account'),
  );
  assert.deepEqual(result, rotated);
  assert.deepEqual(Object.keys(result), [
    'id',
    'name',
    'weight',
    'enabled',
    'credentialEpoch',
    'createdAt',
  ]);
  assert.doesNotMatch(
    JSON.stringify(result),
    /pst-|token|grant|proxy|credentialRef|routeId|failures|total/,
  );
  const plan = db.prepare(`EXPLAIN QUERY PLAN ${statements[0]}`).all('stable-account');
  assert.match(JSON.stringify(plan), /SEARCH a USING INDEX/);
  assert.match(JSON.stringify(plan), /SEARCH e USING INDEX/);
  assert.doesNotMatch(JSON.stringify(plan), /SCAN e|TEMP B-TREE/);
  const restored = replay(t, db),
    restoredAccounts = store(restored);
  assert.deepEqual(
    readonly(t, restored, () => restoredAccounts.readAccountManagementProfile('stable-account'))
      .result,
    rotated,
  );
  assert.equal(restored.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
});

test('Opening多账户以稳定ID读取，原创建未知不借导入/冷却时点，当前绑定不被后来候选替换', async (t) => {
  const f = await fixture(t, { env }),
    accounts = store(f.db),
    opening = readRelationalCommit(f.db, '3').events.find(
      (event) => event.eventType === 'OpeningAccountRecorded',
    );
  const payload = {
      ...opening.payload,
      accountId: 'b',
      displayName: 'x'.repeat(79) + '😀'.slice(0, 1),
      enabled: false,
      credentialEpoch: 7,
      weight: null,
      origin: { ...opening.payload.origin, originalId: 'original-b' },
    },
    {
      policyVersion: _policy,
      pricingVersion: _pricing,
      ...command
    } = meta('second-account', NOW + 100);
  f.db.transaction(() =>
    new RelationalCommitReplayer(f.db).appendCommit({
      ...command,
      expectedVersions: { 'account:b': '0' },
      receipt: {},
      events: [
        {
          eventType: 'OpeningAccountRecorded',
          eventId: 'second-account-opening',
          streamId: 'account:b',
          accountId: 'b',
          memberId: null,
          occurredAt: NOW + 100,
          policyVersion: opening.policyVersion,
          pricingVersion: opening.pricingVersion,
          payload,
        },
      ],
    }),
  )();
  const a = {
      id: 'a',
      name: 'shared',
      weight: 1,
      enabled: true,
      credentialEpoch: 1,
      createdAt: null,
    },
    b = {
      id: 'b',
      name: payload.displayName,
      weight: null,
      enabled: false,
      credentialEpoch: 7,
      createdAt: null,
    };
  assert.deepEqual(readonly(t, f.db, () => accounts.readAccountManagementDirectory()).result, [
    a,
    b,
  ]);
  assert.deepEqual(
    readonly(t, f.db, () => accounts.readSharedAccountManagementProfile()).result,
    a,
  );
  assert.deepEqual(readonly(t, f.db, () => accounts.readAccountManagementProfile('b')).result, b);
  const beforeAdminRead = databaseState(f.db);
  const archived = f.db.transaction(() => accounts.readAdminAccountProfile('b', true, null))();
  assert.equal(archived.weight, null);
  assert.equal(archived.quotaTier, null);
  assert.equal(archived.quotaFixed, null);
  assert.equal(archived.proxyUrl, null);
  assert.equal(archived.token, null);
  assert.equal(databaseState(f.db), beforeAdminRead);
  await assert.rejects(
    withAuthenticatedMemberApiKey(
      f.members,
      keys.m1,
      (actor) => accounts.readAdminAccountProfile('a', true, actor),
      env,
    ),
    { statusCode: 403 },
  );
  assert.equal(
    readonly(t, f.db, () => accounts.readAccountManagementProfile("b' OR 1=1 --")).result,
    null,
  );
  await f.accept('account-inflight');
  f.dispatch('account-inflight-0');
  const authorize = (write) => withAuthenticatedMemberApiKey(f.members, keys.m1, write, env);
  await ownerAccountCommand(accounts, authorize, meta('pause-inflight', NOW + 2, 'm1'), {
    kind: 'pause',
    accountId: 'a',
  });
  assert.deepEqual(readonly(t, f.db, () => accounts.readSharedAccountManagementProfile()).result, {
    ...a,
    enabled: false,
  });
  // 实际完成会将当前 source_event_id 改为 CooldownChanged，仍不能变成创建时间。
  await f.publish('account-inflight-0');
  const current = f.db
    .prepare(
      'SELECT event_type AS type FROM domain_events WHERE event_id=(SELECT source_event_id FROM upstream_accounts WHERE id=?)',
    )
    .get('a');
  assert.equal(current.type, 'CooldownChanged');
  assert.equal(
    readonly(t, f.db, () => accounts.readAccountManagementProfile('a')).result.createdAt,
    null,
  );
  const restored = replay(t, f.db);
  assert.deepEqual(
    readonly(t, restored, () => store(restored).readAccountManagementDirectory()).result,
    [{ ...a, enabled: false }, b],
  );
});

test('共享候选选择复用原owner：未绑仅单候选，不按名称/启用/顺序猜绑定，空目录和未知profile均NULL', (t) => {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const accounts = store(db);
  assert.deepEqual(readonly(t, db, () => accounts.readAccountManagementDirectory()).result, []);
  assert.equal(
    readonly(t, db, () => accounts.readAccountManagementProfile('missing')).result,
    null,
  );
  assert.equal(readonly(t, db, () => accounts.readSharedAccountManagementProfile()).result, null);
  const a = { id: 'a', enabled: false },
    b = { id: 'b', enabled: true };
  assert.equal(selectSharedAccount([a], null), a);
  assert.equal(selectSharedAccount([b, a], 'a'), a);
  assert.equal(selectSharedAccount([a, b], null), null);
  assert.equal(selectSharedAccount([a], 'missing'), null);
});
