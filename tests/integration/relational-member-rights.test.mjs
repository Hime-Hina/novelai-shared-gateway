import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createHash } from 'node:crypto';
import {
  fixture,
  meta,
  NOW,
  keys,
  databaseState,
  openingFacts,
  image,
} from '../helpers/relational-generation.mjs';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import {
  relationalPrivateTables,
  sameRelationalRows,
} from '../../server/storage/relational-private-restore.ts';
import { RelationalAccountStore } from '../../server/storage/relational-account-store.ts';
import { describeStoredResult } from '../../server/storage/result-files.ts';
import { parseRelationalFact } from '../../server/events/relational-fact-contracts.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageFormat,
  RELATIONAL_DATABASE_FORMAT,
  RELATIONAL_WRITER_VERSION,
} from '../../server/storage/relational-storage.ts';
import { RELATIONAL_SCHEMA_SQL } from '../../server/storage/relational-schema.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { RELATIONAL_EVENT_SCHEMA_VERSION } from '../../server/events/relational-events.ts';
import {
  withAuthenticatedMemberApiKey,
  authenticateMemberApiKey,
  createMemberSession,
  withAuthenticatedMemberSession,
  logoutMemberSession,
} from '../../server/members/authentication.ts';

async function setup(t, options = {}) {
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { seconds: 0, ...options, env }),
    quotas = new RelationalQuotaStore(f.db, env),
    controls = new Map();
  const change = (id, input, at = NOW + 10) =>
    withAuthenticatedMemberApiKey(
      f.members,
      keys.m1,
      (actor) => quotas.manageMemberQuota(meta(id, at, actor.id), input, actor, controls),
      env,
    );
  return { ...f, env, quotas, controls, change };
}
const transfer = (resource = 'anlasFixed', amount = 100, expectedConfigVersion = 1) => ({
  action: 'transfer-quota',
  fromUserId: 'm1',
  toUserId: 'm2',
  resource,
  amount,
  expectedConfigVersion,
});
const remove = (action = 'delete-member', expectedConfigVersion = 1) => ({
  action,
  userId: 'm2',
  transferToUserId: 'm1',
  expectedConfigVersion,
});
async function initial(t, editOpening = null) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const env = { OWNER_USER_ID: 'm1' },
    members = new RelationalMemberStore(db),
    quotas = new RelationalQuotaStore(db, env),
    controls = new Map();
  for (const id of ['m1', 'm2'])
    db.transaction(() =>
      members.registerMember(meta(`register-${id}`), {
        memberId: id,
        displayName: id,
        apiKey: keys[id],
      }),
    )();
  if (editOpening) {
    const events = openingFacts({ seconds: 0 }),
      { policyVersion: _p, pricingVersion: _r, ...metadata } = meta('opening');
    editOpening(events);
    const expectedVersions = Object.fromEntries(events.map((event) => [event.streamId, '0']));
    db.transaction(() =>
      new RelationalCommitReplayer(db).appendCommit({
        ...metadata,
        expectedVersions,
        events,
        receipt: { opened: true },
      }),
    )();
  }
  const change = (id, input, at = NOW + 10) =>
    withAuthenticatedMemberApiKey(
      members,
      keys.m1,
      (actor) => quotas.manageMemberQuota(meta(id, at, actor.id), input, actor, controls),
      env,
    );
  return {
    db,
    env,
    members,
    quotas,
    controls,
    change,
    generations: new RelationalGenerationStore(db, env),
  };
}
const groups = (members = ['m1', 'm2']) => [
  { id: 'g', share: 1, members, memberWeights: Object.fromEntries(members.map((id) => [id, 1])) },
  ...['g2', 'g3', 'g4'].map((id) => ({ id, share: 0, members: [] })),
];
async function configure(f, configured = groups(), expectedConfigVersion = 1, at = NOW + 1) {
  return withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    (actor) =>
      f.quotas.configureGroups(
        meta(`groups-${at}`, at, actor.id),
        { groups: configured, expectedConfigVersion },
        actor,
      ),
    f.env,
  );
}
function addOtherPool(events, amount = '7') {
  for (const type of ['OpeningAccountRecorded', 'OpeningWalletRecorded', 'OpeningQuotaRecorded']) {
    const original = events.find(
        (event) =>
          event.eventType === type &&
          (type !== 'OpeningWalletRecorded' || event.payload.memberId === 'm2'),
      ),
      copy = structuredClone(original);
    copy.eventId = `other-${type}`;
    copy.streamId = 'account:b';
    copy.accountId = 'b';
    copy.payload.accountId = 'b';
    copy.payload.origin.originalId = `other-${type}`;
    if (type === 'OpeningAccountRecorded') copy.payload.enabled = false;
    if (type === 'OpeningWalletRecorded')
      copy.payload.available = { v5: '0', anlasFixed: amount, anlasPurchased: '0' };
    if (type === 'OpeningQuotaRecorded')
      Object.assign(copy.payload, {
        status: 'inactive_opus',
        observationEventId: copy.eventId,
        baselineEventId: copy.eventId,
        observedV5: '0',
        observedAnlasFixed: amount,
        observedAnlasPurchased: '0',
      });
    events.push(copy);
  }
}
function assertPublicTables(source, target) {
  const privateNames = new Set(relationalPrivateTables.map(([name]) => name));
  for (const { name } of source
    .prepare(
      "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name",
    )
    .all()) {
    if (privateNames.has(name)) continue;
    const columns = source.prepare(`PRAGMA table_info("${name}")`).all(),
      selected = columns.filter(
        (column) =>
          !(name === 'members' && column.name === 'auth_epoch') &&
          !(name === 'projection_state' && column.name === 'status'),
      ),
      keys = columns.filter((column) => column.pk).sort((a, b) => a.pk - b.pk);
    assert.ok(keys.length, name);
    const query = `SELECT ${selected.map((column) => `"${column.name}"`).join(',')} FROM "${name}" ORDER BY ${keys.map((column) => `"${column.name}"`).join(',')}`;
    assert.equal(sameRelationalRows(source, target, query), true, name);
  }
}
function resequence(stored, db) {
  const versions = new Map();
  stored.command.expectedVersions = {};
  for (const [eventIndex, event] of stored.events.entries()) {
    if (!versions.has(event.streamId)) {
      const version =
        db
          .prepare('SELECT CAST(stream_version AS TEXT) AS n FROM stream_heads WHERE stream_id=?')
          .get(event.streamId)?.n ?? '0';
      versions.set(event.streamId, BigInt(version));
      stored.command.expectedVersions[event.streamId] = version;
    }
    versions.set(event.streamId, versions.get(event.streamId) + 1n);
    Object.assign(event, { eventIndex, streamVersion: String(versions.get(event.streamId)) });
  }
  stored.eventCount = stored.events.length;
  stored.command.events = stored.events.map(
    ({
      eventType,
      payload,
      streamId,
      occurredAt,
      accountId,
      memberId,
      policyVersion,
      pricingVersion,
    }) => ({
      eventType,
      payload,
      streamId,
      occurredAt,
      accountId,
      memberId,
      policyVersion,
      pricingVersion,
    }),
  );
}
function mutatedCommit(t, snapshot, stored, mutate) {
  const source = new Database(snapshot),
    db = new Database(':memory:');
  t.after(() => source.close());
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const owner = new RelationalCommitReplayer(db),
    inputs = new RelationalInputStore(source);
  for (const { seq } of source
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    db.transaction(() => owner.replayCommit(readRelationalCommit(source, seq), inputs))();
  }
  const bad = structuredClone(stored),
    before = databaseState(db);
  mutate(bad);
  resequence(bad, db);
  db.transaction(() =>
    assert.throws(
      () => owner.replayCommit(bad, inputs),
      (error) => error.statusCode === 409,
    ),
  )();
  assert.equal(databaseState(db), before);
  db.transaction(() => owner.replayCommit(stored, inputs))();
  const once = db.serialize();
  db.transaction(() => assert.equal(owner.replayCommit(stored, inputs).replayed, true))();
  assert.deepEqual(db.serialize(), once);
}

test('新成员事实必填字段闭合，不能将旧占位缺省为合法资金决定', () => {
  const payload = {
    accountId: 'a',
    configurationEventId: 'configuration',
    configVersion: 2,
    fromMemberId: 'm1',
    toMemberId: 'm2',
    resources: { v5: '0', anlasFixed: '1', anlasPurchased: '0' },
    reason: 'transfer',
  };
  parseRelationalFact({ eventType: 'FundsTransferred', payload });
  for (const key of ['configurationEventId', 'configVersion', 'fromMemberId', 'toMemberId']) {
    const bad = structuredClone(payload);
    delete bad[key];
    assert.throws(
      () => parseRelationalFact({ eventType: 'FundsTransferred', payload: bad }),
      /Invalid relational fact/,
    );
  }
  for (const key of ['fromMemberId', 'toMemberId']) {
    assert.throws(
      () =>
        parseRelationalFact({
          eventType: 'FundsTransferred',
          payload: { ...payload, [key]: null },
        }),
      /Invalid relational fact/,
    );
  }
  assert.throws(
    () =>
      parseRelationalFact({
        eventType: 'FundsTransferred',
        payload: { ...payload, reason: 'operator_share' },
      }),
    /Invalid relational fact/,
  );
});

test('正确v3原型DDL与旧hash仍不能通过当前格式，缺成员决定新必填字段不能解释为默认', (t) => {
  const db = new Database(':memory:');
  t.after(() => db.close());
  const old = RELATIONAL_SCHEMA_SQL.replace(
    'CHECK (schema_version = ' + RELATIONAL_EVENT_SCHEMA_VERSION + ')',
    'CHECK (schema_version = 3)',
  );
  assert.notEqual(old, RELATIONAL_SCHEMA_SQL);
  db.exec(old);
  const insert = db.prepare('INSERT INTO app_meta(key,value) VALUES(?,?)');
  insert.run('database_format', RELATIONAL_DATABASE_FORMAT);
  insert.run('schema_sha256', createHash('sha256').update(old).digest('hex'));
  insert.run(
    'event_authority',
    JSON.stringify({
      version: RELATIONAL_WRITER_VERSION,
      minimumWriterVersion: RELATIONAL_WRITER_VERSION,
    }),
  );
  db.prepare(
    "INSERT INTO projection_state(id,history_id,reducer_version,last_commit_seq,status) VALUES(1,'old-v3',1,0,'building')",
  ).run();
  const before = db.serialize();
  assert.throws(() => assertRelationalStorageFormat(db), /Unsupported relational database format/);
  assert.deepEqual(db.serialize(), before);
});

test('真实成员转账只移动原账户资源，组不变但配置+1；旧键在忙碌和新版本后重领原回执', async (t) => {
  const f = await setup(t),
    beforeGroups = f.db.prepare('SELECT * FROM group_members ORDER BY member_id').all(),
    result = await f.change('transfer-fixed', transfer()),
    stored = readRelationalCommit(f.db, result.commitSeq);
  assert.equal(result.receipt.configVersion, 2);
  assert.equal('status' in result.receipt, false);
  assert.equal(result.receipt.transferred.anlasFixed, 100);
  assert.equal(f.generations.readWallet('a', 'm1').anlasFixed, '3900');
  assert.equal(f.generations.readWallet('a', 'm2').anlasFixed, '4100');
  assert.deepEqual(
    f.db.prepare('SELECT * FROM group_members ORDER BY member_id').all(),
    beforeGroups,
  );
  assert.deepEqual(
    stored.events.map((event) => event.eventType),
    ['RefillApplied', 'MemberTransferred', 'FundsTransferred'],
  );
  assert.equal(stored.events.at(-1).payload.configurationEventId, stored.events.at(-2).eventId);
  f.controls.set('still-finishing', {});
  const once = databaseState(f.db),
    retry = await f.change('transfer-fixed', transfer(), NOW + 1000);
  assert.equal(retry.replayed, true);
  assert.deepEqual(retry.receipt, result.receipt);
  assert.equal(databaseState(f.db), once);
  await assert.rejects(
    f.change('transfer-fixed', transfer('anlasFixed', 101)),
    /idempotency conflict/,
  );
});

test('预览零写；删除保留身份和零钱包，私有认证撤销，原决定完整回放不恢复 Key', async (t) => {
  const f = await setup(t);
  await withAuthenticatedMemberApiKey(f.members, keys.m1, () => {}, f.env);
  const before = databaseState(f.db),
    preview = await f.change('preview-delete', remove('preview-delete-member'));
  assert.equal(preview.configVersion, 1);
  assert.equal(preview.deletedUserId, 'm2');
  assert.equal(preview.transferred.v5, 40);
  assert.equal(databaseState(f.db), before);
  const result = await f.change('delete-member', remove()),
    stored = readRelationalCommit(f.db, result.commitSeq);
  assert.equal(result.receipt.configVersion, 2);
  assert.deepEqual(result.receipt.transferred, { v5: 40, anlasFixed: 4000, anlasPurchased: 0 });
  assert.deepEqual(f.db.prepare("SELECT status,auth_epoch FROM members WHERE id='m2'").get(), {
    status: 'removed',
    auth_epoch: null,
  });
  assert.deepEqual(f.generations.readWallet('a', 'm2'), {
    v5: '0',
    anlasFixed: '0',
    anlasPurchased: '0',
  });
  assert.equal(f.generations.readWallet('a', 'm1').v5, '80');
  assert.equal(await authenticateMemberApiKey(f.members, keys.m2, f.env), null);
  assert.equal(stored.events.at(-1).eventType, 'MemberStatusChanged');
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const owner = new RelationalCommitReplayer(db);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    db.transaction(() => owner.replayCommit(readRelationalCommit(f.db, seq), f.inputs))();
  }
  for (const table of [
    'wallets',
    'group_members',
    'sharing_groups',
    'sharing_policy',
    'commits',
    'domain_events',
  ]) {
    assert.deepEqual(
      db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
      f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
    );
  }
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
  const once = db.serialize();
  db.transaction(() => assert.equal(owner.replayCommit(stored).replayed, true))();
  assert.deepEqual(db.serialize(), once);
});

test('三池转账保留原数值规则、V5容量与配置代次上界', async (t) => {
  for (const [resource, amount, options] of [
    ['v5', 0.000000000001, {}],
    ['anlasFixed', 1, {}],
    ['anlasPurchased', 10, { purchased: '100', officialPurchased: '200' }],
  ])
    await t.test(resource, async (t) => {
      const f = await setup(t, options),
        before = f.generations.readWallet('a', 'm1'),
        result = await f.change(`move-${resource}`, transfer(resource, amount));
      assert.equal(result.receipt.transferred[resource], amount);
      assert.notEqual(f.generations.readWallet('a', 'm1')[resource], before[resource]);
      assert.equal(result.receipt.groups[0].members.length, 2);
    });
  for (const [resource, amount, message] of [
    ['v5', 11, /容量/],
    ['v5', 0.0000000000001, /12/],
    ['anlasFixed', 0.5, /整数/],
    ['anlasFixed', 4001, /不足/],
  ]) {
    await t.test(`${resource}-${amount}`, async (t) => {
      const f = await setup(t),
        before = databaseState(f.db);
      await assert.rejects(f.change('bad-amount', transfer(resource, amount)), message);
      assert.equal(databaseState(f.db), before);
    });
  }
  await t.test('version-max', async (t) => {
    const f = await initial(t, (events) => {
      events.find((event) => event.eventType === 'OpeningPolicyRecorded').payload.configVersion =
        Number.MAX_SAFE_INTEGER - 1;
    });
    const result = await f.change(
      'last-version',
      transfer('anlasFixed', 1, Number.MAX_SAFE_INTEGER - 1),
    );
    assert.equal(result.receipt.configVersion, Number.MAX_SAFE_INTEGER);
    const before = databaseState(f.db);
    await assert.rejects(
      f.change('overflow', transfer('anlasFixed', 1, Number.MAX_SAFE_INTEGER)),
      /上限|版本/,
    );
    assert.equal(databaseState(f.db), before);
    assert.equal(
      (await f.change('last-version', transfer('anlasFixed', 1, Number.MAX_SAFE_INTEGER - 1)))
        .replayed,
      true,
    );
  });
});

test('未绑定真正零权益可删除并继承最后原位置；不造账户、资金子事实或钱包', async (t) => {
  const f = await initial(t);
  await configure(f, groups(['m2']));
  const previewBefore = databaseState(f.db),
    preview = await f.change('zero-preview', remove('preview-delete-member', 2));
  assert.equal(preview.autoJoined.userId, 'm1');
  assert.equal(preview.autoJoined.memberWeight, 1);
  assert.equal(preview.configVersion, 2);
  assert.equal(databaseState(f.db), previewBefore);
  const result = await f.change('zero-delete', remove('delete-member', 2)),
    stored = readRelationalCommit(f.db, result.commitSeq);
  assert.deepEqual(
    stored.events.map((event) => event.eventType),
    ['MemberRemoved', 'MemberStatusChanged'],
  );
  assert.deepEqual(result.receipt.transferred, { v5: 0, anlasFixed: 0, anlasPurchased: 0 });
  assert.equal(result.receipt.groups[0].members[0], 'm1');
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM wallets').get().n, 0);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM upstream_accounts').get().n, 0);
  assert.equal(f.db.prepare('SELECT bound_account_id AS id FROM sharing_policy').get().id, null);
});

test('接收已分组成员可保留原超容量权益，未分组启用接收者才可继承份额', async (t) => {
  const f = await setup(t);
  f.db.transaction(() =>
    f.members.registerMember(meta('register-m3'), {
      memberId: 'm3',
      displayName: 'third',
      apiKey: 'key-m3',
    }),
  )();
  await configure(f, [
    { id: 'g', share: 0, members: ['m1', 'm2'], memberWeights: { m1: 1, m2: 1 } },
    { id: 'g2', share: 1, members: ['m3'] },
    { id: 'g3', share: 0, members: [] },
    { id: 'g4', share: 0, members: [] },
  ]);
  const result = await f.change('over-cap-delete', remove('delete-member', 2));
  assert.equal(result.receipt.users.find((user) => user.id === 'm1').quota.v5Capacity, 0);
  assert.equal(f.generations.readWallet('a', 'm1').v5, '80');
  assert.equal(result.receipt.autoJoined, null);
  const other = await setup(t);
  other.db.transaction(() =>
    other.members.registerMember(meta('register-new'), {
      memberId: 'm3',
      displayName: 'new',
      apiKey: 'key-new',
    }),
  )();
  const inherited = await other.change('auto-join', { ...remove(), transferToUserId: 'm3' });
  assert.equal(inherited.receipt.autoJoined.userId, 'm3');
  assert.equal(inherited.receipt.autoJoined.groupId, 'g');
  assert.equal(inherited.receipt.autoJoined.memberWeight, 1);
  assert.equal(inherited.receipt.groups[0].members.includes('m2'), false);
  assert.equal(other.generations.readWallet('a', 'm3').anlasFixed, '4000');
});

test('原账户其他池权利不被搬走；当前池转账合法，删除须先处理原池；零旧钱包仍保留', async (t) => {
  const f = await initial(t, (events) => addOtherPool(events));
  await f.change('current-only', { ...transfer(), fromUserId: 'm2', toUserId: 'm1' });
  assert.equal(f.generations.readWallet('b', 'm2').anlasFixed, '7');
  assert.equal(f.generations.readWallet('a', 'm2').anlasFixed, '3900');
  const before = databaseState(f.db);
  await assert.rejects(
    f.change('must-preserve-original-pool', remove('delete-member', 2)),
    /其他原账户/,
  );
  assert.equal(databaseState(f.db), before);
  const zeroOld = await initial(t, (events) => addOtherPool(events, '0'));
  await zeroOld.change('keep-zero-old', remove());
  assert.deepEqual(zeroOld.generations.readWallet('b', 'm2'), {
    v5: '0',
    anlasFixed: '0',
    anlasPurchased: '0',
  });
  assert.equal(
    zeroOld.db.prepare("SELECT COUNT(*) AS n FROM upstream_accounts WHERE id='b'").get().n,
    1,
  );
  const ungrouped = await initial(t, (events) => {
    const index = events.findIndex(
      (event) =>
        event.eventType === 'OpeningGroupMemberRecorded' && event.payload.memberId === 'm2',
    );
    events.splice(index, 1);
  });
  const protectedRights = databaseState(ungrouped.db);
  await assert.rejects(ungrouped.change('ungrouped-old-rights', remove()), /未归组权益/);
  assert.equal(databaseState(ungrouped.db), protectedRights);
});

test('当前Key/Cookie/owner/epoch在旧回执前复核，本地管理员保留原应急边界', async (t) => {
  for (const mode of ['owner', 'key', 'cookie', 'epoch', 'non-owner'])
    await t.test(mode, async (t) => {
      const f = await setup(t);
      await f.change('protected-receipt', transfer());
      const actor = await authenticateMemberApiKey(f.members, keys.m1),
        session = await createMemberSession(f.members, actor, {}, NOW + 20);
      if (mode === 'owner') f.env.OWNER_USER_ID = 'm2';
      if (mode === 'key')
        f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'new-member-key'))();
      if (mode === 'cookie') await logoutMemberSession(f.members, session.token);
      if (mode === 'epoch')
        f.db.prepare("UPDATE members SET auth_epoch='new-epoch' WHERE id='m1'").run();
      const before = databaseState(f.db);
      if (mode === 'key') assert.equal(await f.change('protected-receipt', transfer()), null);
      else if (mode === 'cookie')
        assert.equal(
          await withAuthenticatedMemberSession(
            f.members,
            session.token,
            {},
            (member) =>
              f.quotas.manageMemberQuota(
                meta('protected-receipt', NOW + 10, 'm1'),
                transfer(),
                member,
                f.controls,
              ),
            NOW + 21,
          ),
          null,
        );
      else if (mode === 'epoch')
        assert.throws(
          () =>
            f.db.transaction(() =>
              f.quotas.manageMemberQuota(
                meta('protected-receipt', NOW + 10, 'm1'),
                transfer(),
                actor,
                f.controls,
              ),
            )(),
          /当前 owner/,
        );
      else
        await assert.rejects(
          withAuthenticatedMemberApiKey(
            f.members,
            mode === 'non-owner' ? keys.m2 : keys.m1,
            (member) =>
              f.quotas.manageMemberQuota(
                meta('protected-receipt', NOW + 10, member.id),
                transfer(),
                member,
                f.controls,
              ),
            f.env,
          ),
          /当前 owner/,
        );
      assert.equal(databaseState(f.db), before);
    });
  const f = await setup(t),
    owner = await authenticateMemberApiKey(f.members, keys.m1),
    session = await createMemberSession(f.members, owner, {}, NOW + 1);
  const cookie = await withAuthenticatedMemberSession(
    f.members,
    session.token,
    {},
    (member) =>
      f.quotas.manageMemberQuota(
        meta('cookie-transfer', NOW + 2, member.id),
        transfer('anlasFixed', 1),
        member,
        f.controls,
      ),
    NOW + 2,
  );
  assert.equal(cookie.receipt.transferred.anlasFixed, 1);
  const before = databaseState(f.db);
  await assert.rejects(
    f.change('no-owner-deletion', {
      action: 'delete-member',
      userId: 'm1',
      transferToUserId: 'm2',
      expectedConfigVersion: 2,
    }),
    /当前 owner/,
  );
  await assert.rejects(
    f.change('no-owner-preview', {
      action: 'preview-delete-member',
      userId: 'm1',
      transferToUserId: 'm2',
      expectedConfigVersion: 2,
    }),
    /当前 owner/,
  );
  assert.equal(databaseState(f.db), before);
  const local = f.db.transaction(() =>
    f.quotas.manageMemberQuota(
      { ...meta('local-emergency', NOW + 3), actorKind: 'admin', actorId: 'local-admin' },
      { action: 'delete-member', userId: 'm1', transferToUserId: 'm2', expectedConfigVersion: 2 },
      null,
      f.controls,
    ),
  )();
  assert.equal(local.receipt.deletedUserId, 'm1');
  assert.equal(await authenticateMemberApiKey(f.members, keys.m1), null);
});

test('分组停用接收者保留原权益；未分组停用接收者不能继承，未分组零权益源可删除', async (t) => {
  const f = await setup(t);
  f.db.transaction(() =>
    f.members.registerMember(meta('register-disabled'), {
      memberId: 'm3',
      displayName: 'disabled',
      apiKey: 'disabled-key',
    }),
  )();
  f.db.transaction(() => f.members.setMemberStatus(meta('disable-target'), 'm3', 'disabled'))();
  const before = databaseState(f.db);
  await assert.rejects(
    f.change('disabled-ungrouped', { ...remove(), transferToUserId: 'm3' }),
    /未分组接收成员必须已启用/,
  );
  assert.equal(databaseState(f.db), before);
  const grouped = await setup(t);
  grouped.db.transaction(() =>
    grouped.members.setMemberStatus(meta('disable-member'), 'm2', 'disabled'),
  )();
  const result = await grouped.change('disabled-grouped-receiver', transfer('anlasFixed', 1));
  assert.equal(result.receipt.transferred.anlasFixed, 1);
  assert.equal(grouped.members.readMember('m2').status, 'disabled');
  assert.equal(await authenticateMemberApiKey(grouped.members, keys.m2), null);
  const zero = await initial(t);
  await configure(zero, groups(['m1']));
  const removed = await zero.change('zero-ungrouped', remove('delete-member', 2));
  assert.deepEqual(removed.receipt.transferred, { v5: 0, anlasFixed: 0, anlasPurchased: 0 });
  assert.equal(zero.members.readMember('m2').status, 'removed');
});

test('原全局排队/预扣与新增准备、核对、物理promise各自阻止调整，释放后沿原owner执行', async (t) => {
  for (const mode of ['waiting', 'reserved', 'prepared', 'review', 'physical'])
    await t.test(mode, async (t) => {
      const f = await setup(t);
      await f.accept(`duty-${mode}`, { memberId: 'm2' });
      if (mode !== 'waiting') f.dispatch(`duty-${mode}-0`);
      if (mode === 'prepared') {
        const descriptor = describeStoredResult(`artifact-duty-${mode}-0`, 'generate', {
          mimeType: 'image/webp',
          buffer: image,
        });
        f.db.transaction(() =>
          f.generations.prepareGenerationResult(
            meta('prepare-duty', NOW + 2),
            `duty-${mode}-0`,
            descriptor,
            f.response(),
          ),
        )();
        f.fail(`duty-${mode}-0`, { aborted: false });
        f.db.transaction(() =>
          f.generations.closeGenerationReview(meta('close-duty', NOW + 300003), `duty-${mode}-0`),
        )();
        assert.equal(
          f.db.prepare('SELECT review_status AS state FROM generation_attempts').get().state,
          'estimated_closed',
        );
      } else if (mode === 'review') f.fail(`duty-${mode}-0`, { aborted: false });
      else if (mode === 'physical') {
        await f.publish(`duty-${mode}-0`);
        f.controls.set(`duty-${mode}-0`, { accountId: 'a' });
      }
      const before = databaseState(f.db);
      await assert.rejects(
        f.change(`blocked-${mode}`, remove(), NOW + 300004),
        /排队|运行|预留|活动|准备|核对|收尾/,
      );
      await assert.rejects(
        f.change(`preview-blocked-${mode}`, remove('preview-delete-member'), NOW + 300004),
        /排队|运行|预留|活动|准备|核对|收尾/,
      );
      assert.equal(databaseState(f.db), before);
      if (mode === 'physical') {
        f.controls.clear();
        const result = await f.change('physical-released', remove(), NOW + 300004);
        assert.equal(result.receipt.deletedUserId, 'm2');
        assert.equal(f.generations.readCharge(`duty-${mode}-0`).originalMemberId, 'm2');
      }
    });
});

test('删除保留真实免费成功、输入、收费与原成员来源；removed不能由后续状态事实复活', async (t) => {
  const f = await setup(t, { freeLimit: 1 });
  await f.accept('source-free', { memberId: 'm2', steps: 23 });
  f.dispatch('source-free-0');
  await f.publish('source-free-0');
  assert.equal(f.generations.readCharge('source-free-0').billingMode, 'free');
  const tables = [
      'generation_inputs',
      'input_assets',
      'generation_requests',
      'generation_jobs',
      'generation_attempts',
      'charges',
      'artifacts',
      'idempotency_keys',
    ],
    original = tables.map((table) => f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all());
  await f.change('retain-original-evidence', remove());
  for (const [index, table] of tables.entries())
    assert.deepEqual(
      f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
      original[index],
      table,
    );
  assert.equal(
    f.db
      .prepare("SELECT free_succeeded_at AS at FROM generation_jobs WHERE id='source-free-0'")
      .get().at,
    NOW + 4,
  );
  const before = databaseState(f.db);
  assert.throws(
    () =>
      f.db.transaction(() =>
        f.members.registerMember(meta('same-identity'), {
          memberId: 'm2',
          displayName: 'again',
          apiKey: 'another-key',
        }),
      )(),
    /已存在/,
  );
  const { policyVersion, pricingVersion, ...metadata } = meta('no-revival', NOW + 20),
    version = f.db
      .prepare(
        "SELECT CAST(stream_version AS TEXT) AS n FROM stream_heads WHERE stream_id='member:m2'",
      )
      .get().n;
  assert.throws(
    () =>
      f.db.transaction(() =>
        new RelationalCommitReplayer(f.db).appendCommit({
          ...metadata,
          expectedVersions: { 'member:m2': version },
          receipt: { memberId: 'm2' },
          events: [
            {
              eventType: 'MemberStatusChanged',
              payload: { memberId: 'm2', status: 'active' },
              streamId: 'member:m2',
              memberId: 'm2',
              accountId: null,
              occurredAt: NOW + 20,
              policyVersion,
              pricingVersion,
            },
          ],
        }),
      )(),
    /已移除/,
  );
  assert.equal(databaseState(f.db), before);
  assert.equal(await authenticateMemberApiKey(f.members, keys.m2), null);
});

test('真实后段SQL故障含私有epoch撤销全表回滚，合法重试才提交一次', async (t) => {
  for (const phase of ['wallet', 'private-epoch'])
    await t.test(phase, async (t) => {
      const f = await setup(t);
      let walletWrites = 0,
        epochWrites = 0;
      f.db.function('actual_wallet', () => {
        walletWrites++;
      });
      f.db.function('actual_epoch', () => {
        epochWrites++;
      });
      f.db.exec(
        `CREATE TEMP TRIGGER count_wallet AFTER UPDATE ON wallets BEGIN SELECT actual_wallet(); END;`,
      );
      if (phase === 'wallet')
        f.db.exec(
          "CREATE TEMP TRIGGER member_fault AFTER UPDATE ON wallets WHEN NEW.member_id='m1' BEGIN SELECT RAISE(ABORT,'actual target wallet fault'); END;",
        );
      else
        f.db.exec(
          "CREATE TEMP TRIGGER member_fault AFTER UPDATE OF auth_epoch ON members WHEN NEW.id='m2' AND NEW.auth_epoch IS NULL BEGIN SELECT actual_epoch(); SELECT RAISE(ABORT,'actual private epoch fault'); END;",
        );
      const before = databaseState(f.db);
      f.db.transaction(() =>
        assert.throws(
          () =>
            f.quotas.manageMemberQuota(
              { ...meta('atomic-member', NOW + 10), actorKind: 'admin', actorId: 'local-admin' },
              remove(),
              null,
              f.controls,
            ),
          /actual.*fault/,
        ),
      )();
      assert.equal(walletWrites, 2);
      if (phase === 'private-epoch') assert.equal(epochWrites, 1);
      assert.equal(databaseState(f.db), before);
      assert.ok(await authenticateMemberApiKey(f.members, keys.m2));
      f.db.exec('DROP TRIGGER member_fault');
      const result = f.db.transaction(() =>
        f.quotas.manageMemberQuota(
          { ...meta('atomic-member', NOW + 10), actorKind: 'admin', actorId: 'local-admin' },
          remove(),
          null,
          f.controls,
        ),
      )();
      assert.equal(result.receipt.deletedUserId, 'm2');
      assert.equal(await authenticateMemberApiKey(f.members, keys.m2), null);
    });
});

test('完整成员提交的父子角色、配置、预算、移除身份、回执和原同步不得拆掉或夹带另一业务', async (t) => {
  for (const action of ['transfer', 'remove'])
    await t.test(action, async (t) => {
      const f = await setup(t, { v5: '10', secondV5: '10', seconds: 600 }),
        snapshot = f.db.serialize(),
        result = await f.change('original-complete', action === 'transfer' ? transfer() : remove()),
        stored = readRelationalCommit(f.db, result.commitSeq);
      for (const mutation of [
        'no-parent',
        'no-funds',
        'no-sync',
        'only-identity',
        'no-identity',
        'old-source',
        'wrong-role',
        'wrong-account',
        'wrong-version',
        'wrong-resource',
        'budget',
        'duplicate-funds',
        'reverse-funds',
        'reverse-identity',
        'identity-account',
        'identity-policy',
        'identity-price',
        'receipt-role',
        'receipt-money',
        'receipt-groups',
        'fingerprint',
        'command-time',
        'foreign-prefix',
        'foreign-suffix',
      ]) {
        if (
          action === 'transfer' &&
          (['only-identity', 'no-identity', 'reverse-identity'].includes(mutation) ||
            mutation.startsWith('identity-'))
        )
          continue;
        await t.test(mutation, (t) =>
          mutatedCommit(t, snapshot, stored, (bad) => {
            const parent = bad.events.find(
                (event) =>
                  event.eventType === 'MemberTransferred' || event.eventType === 'MemberRemoved',
              ),
              funds = bad.events.find((event) => event.eventType === 'FundsTransferred'),
              identity = bad.events.find((event) => event.eventType === 'MemberStatusChanged');
            if (mutation === 'no-parent')
              bad.events = bad.events.filter((event) => event !== parent);
            if (mutation === 'no-funds') bad.events = bad.events.filter((event) => event !== funds);
            if (mutation === 'no-sync')
              bad.events = bad.events.filter((event) => event.eventIndex >= parent.eventIndex);
            if (mutation === 'only-identity') bad.events = [identity];
            if (mutation === 'no-identity')
              bad.events = bad.events.filter((event) => event !== identity);
            if (mutation === 'identity-account') identity.accountId = 'unrelated-account';
            if (mutation === 'identity-policy') identity.policyVersion = 'unrelated-policy';
            if (mutation === 'identity-price') identity.pricingVersion = 'unrelated-price';
            if (mutation === 'old-source') funds.payload.configurationEventId = 'opening-quota';
            if (mutation === 'wrong-role') funds.payload.fromMemberId = funds.payload.toMemberId;
            if (mutation === 'wrong-account') {
              funds.accountId = 'wrong-account';
              funds.streamId = 'account:wrong-account';
              funds.payload.accountId = 'wrong-account';
            }
            if (mutation === 'wrong-version') {
              parent.payload.configVersion++;
              funds.payload.configVersion++;
            }
            if (mutation === 'wrong-resource') {
              funds.payload.resources.anlasPurchased = funds.payload.resources.anlasFixed;
              funds.payload.resources.anlasFixed = '0';
            }
            if (mutation === 'budget') funds.payload.resources.anlasFixed = '1000';
            if (mutation === 'duplicate-funds')
              bad.events.splice(bad.events.indexOf(funds) + 1, 0, {
                ...structuredClone(funds),
                eventId: 'duplicate-funds',
              });
            if (mutation === 'reverse-funds') {
              bad.events = bad.events.filter((event) => event !== funds);
              bad.events.unshift(funds);
            }
            if (mutation === 'reverse-identity') {
              bad.events = bad.events.filter((event) => event !== identity);
              bad.events.unshift(identity);
            }
            if (mutation === 'receipt-role') {
              const receipt = bad.command.receipt;
              if (action === 'transfer') receipt.toUserId = 'm1';
              else receipt.transferToUserId = 'm2';
            }
            if (mutation === 'receipt-money') bad.command.receipt.transferred.anlasFixed = 1000;
            if (mutation === 'receipt-groups') bad.command.receipt.groups = [];
            if (mutation === 'fingerprint') bad.command.fingerprint = '0'.repeat(64);
            if (mutation === 'command-time') bad.command.recordedAt++;
            if (mutation === 'foreign-prefix' || mutation === 'foreign-suffix') {
              const extra = {
                ...structuredClone(parent),
                eventId: 'foreign-status',
                eventType: 'MemberStatusChanged',
                streamId: 'member:m2',
                streamKind: 'member',
                memberId: 'm2',
                accountId: null,
                payload: { memberId: 'm2', status: 'disabled' },
              };
              if (mutation === 'foreign-prefix')
                bad.events.splice(bad.events.indexOf(parent), 0, extra);
              else bad.events.push(extra);
            }
          }),
        );
      }
    });
});

test('账户暂停不阻止移动既有权益；原同步status也不能删除后篡改receipt补成active', async (t) => {
  const f = await setup(t),
    accounts = new RelationalAccountStore(f.db, f.quotas, f.controls, f.env);
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    (actor) =>
      accounts.prepareAccountCommand(
        meta('pause-before-member', NOW + 1, actor.id),
        { kind: 'pause', accountId: 'a' },
        actor,
      ),
    f.env,
  );
  const snapshot = f.db.serialize(),
    result = await f.change('paused-current-rights', transfer()),
    stored = readRelationalCommit(f.db, result.commitSeq);
  assert.equal(result.receipt.users[0].quota.status, 'inactive_opus');
  mutatedCommit(t, snapshot, stored, (bad) => {
    bad.events = bad.events.filter((event) => event.eventType !== 'QuotaStatusChanged');
    for (const user of bad.command.receipt.users) user.quota.status = 'active';
  });
});

test('后来配置和owner改变仍按原序恢复全部公共表，保留旧actor和来源，不复活私有认证', async (t) => {
  const f = await setup(t, { freeLimit: 1 });
  await f.accept('historical-free', { memberId: 'm2', steps: 23 });
  f.dispatch('historical-free-0');
  await f.publish('historical-free-0');
  await f.change('historical-transfer', transfer('anlasFixed', 3));
  await f.change('historical-removal', remove('delete-member', 2));
  f.db.transaction(() =>
    f.members.registerMember(meta('later-member'), {
      memberId: 'm3',
      displayName: 'later',
      apiKey: 'later-key',
    }),
  )();
  await configure(f, groups(['m1', 'm3']), 3, NOW + 20);
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const originalOwner = process.env.OWNER_USER_ID;
  process.env.OWNER_USER_ID = 'm3';
  t.after(() => {
    if (originalOwner === undefined) delete process.env.OWNER_USER_ID;
    else process.env.OWNER_USER_ID = originalOwner;
  });
  const owner = new RelationalCommitReplayer(db);
  let fetchCalls = 0;
  t.mock.method(globalThis, 'fetch', async () => {
    fetchCalls++;
    throw new Error('no upstream on replay');
  });
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all())
    db.transaction(() => owner.replayCommit(readRelationalCommit(f.db, seq), f.inputs))();
  assertPublicTables(f.db, db);
  assert.equal(fetchCalls, 0);
  assert.equal(
    db.prepare("SELECT actor_id FROM commits WHERE command_id='historical-removal'").get().actor_id,
    'm1',
  );
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
});
