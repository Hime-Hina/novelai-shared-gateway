import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createHash } from 'node:crypto';
import { writeFileSync } from 'node:fs';
import { readFile, readdir, copyFile, link } from 'node:fs/promises';
import path from 'node:path';
import { fixture, meta, keys, NOW, databaseState } from '../helpers/relational-generation.mjs';
import {
  readRelationalCommit,
  replayRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { restoreRelationalPrivateSnapshot } from '../../server/storage/relational-private-restore.ts';
import {
  authenticateMemberApiKey,
  authenticateMemberSession,
  createMemberSession,
  logoutMemberSession,
} from '../../server/members/authentication.ts';

const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');
const env = { OWNER_USER_ID: 'm1', DISCORD_GUILD_ID: 'guild' };

async function snapshot(f, name = 'snapshot') {
  const filename = path.join(f.dir, `${name}.sqlite`);
  await f.db.backup(filename);
  const db = new Database(filename);
  db.pragma('journal_mode=DELETE');
  db.close();
  return { filename, sha256: sha256(await readFile(filename)) };
}

function rebuild(t, saved, changeCommit = () => {}) {
  const db = new Database(':memory:'),
    source = new Database(saved.filename, { readonly: true });
  t.after(() => db.close());
  source.pragma('foreign_keys=ON');
  initializeRelationalStorage(db, 'generation-history');
  const members = new RelationalMemberStore(db),
    inputs = new RelationalInputStore(db),
    generations = new RelationalGenerationStore(db);
  const sourceInputs = new RelationalInputStore(source);
  try {
    for (const { seq } of source
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all()) {
      const stored = readRelationalCommit(source, seq);
      changeCommit(stored, seq);
      db.transaction(() => {
        if (stored.events.every((event) => event.streamKind === 'member')) {
          members.replayMemberCommit(stored);
        } else if (stored.events.some((event) => event.eventType === 'InputRegistered')) {
          inputs.replayAcceptedInput(sourceInputs, seq);
        } else {
          generations.replayGenerationCommit(stored);
        }
      })();
    }
  } finally {
    source.close();
  }
  return { db, members };
}

const restore = (db, saved) =>
  db.transaction(() => restoreRelationalPrivateSnapshot(db, saved.filename, saved.sha256))();

async function privateState(f) {
  const credentials = await f.members.prepareMemberCredentials('m1', {
    username: 'private.member',
    password: 'synthetic-password-12345',
  });
  f.db.transaction(() => f.members.setMemberCredentials(credentials))();
  f.db
    .prepare(
      `INSERT INTO discord_bindings(member_id,discord_id,guild_id,access_token,refresh_token,expires_at,checked_at,eligible)
      VALUES('m1','private-discord','guild','private-access','private-refresh',?,?,1)`,
    )
    .run(NOW + 3600000, NOW);
  f.db
    .prepare('INSERT INTO discord_invites(member_id,code_hash,expires_at) VALUES(?,?,?)')
    .run('m2', sha256('private-invite'), NOW + 300000);
  const member = await f.members.loginMemberPassword('private.member', 'synthetic-password-12345');
  const revoked = await createMemberSession(f.members, member, env, NOW);
  await logoutMemberSession(f.members, revoked.token);
  const current = await createMemberSession(f.members, member, env, NOW + 1);
  return { current, revoked };
}

test('同切点业务重放后从选定备份恢复当前私有状态，重复零写入且不会复活已退出会话', async (t) => {
  const network = t.mock.method(globalThis, 'fetch', () => {
    throw new Error('Private restoration must not call external services');
  });
  const f = await fixture(t);
  await f.accept('generation');
  f.dispatch('generation-0');
  await f.publish('generation-0');
  const { current, revoked } = await privateState(f),
    saved = await snapshot(f),
    target = rebuild(t, saved);
  const sourceBytes = await readFile(saved.filename),
    files = await readdir(f.dir);
  assert.equal(await authenticateMemberApiKey(target.members, keys.m1), null);
  const result = restore(target.db, saved);
  assert.equal(result.restored, true);
  assert.equal(result.members, 2);
  assert.equal(databaseState(target.db), databaseState(f.db));
  assert.equal((await authenticateMemberApiKey(target.members, keys.m1)).id, 'm1');
  assert.equal(
    (await target.members.loginMemberPassword('private.member', 'synthetic-password-12345')).id,
    'm1',
  );
  assert.equal(
    (await authenticateMemberSession(target.members, current.token, env, NOW + 2)).expiresAt,
    current.expiresAt,
  );
  assert.equal(await authenticateMemberSession(target.members, revoked.token, env, NOW + 2), null);
  const before = target.db.serialize();
  assert.deepEqual(restore(target.db, saved), { restored: false, members: 2, privateRows: 0 });
  assert.deepEqual(target.db.serialize(), before);
  assert.deepEqual(await readFile(saved.filename), sourceBytes);
  assert.deepEqual(await readdir(f.dir), files);
  assert.equal(target.db.prepare('SELECT status FROM projection_state').get().status, 'building');
  assert.throws(() => assertRelationalStorageReady(target.db), /incomplete/);
  assert.equal(target.db.pragma('foreign_keys', { simple: true }), 1);
  assert.equal(network.mock.callCount(), 0);
});

test('同业务提交号的不同私有备份不可互换，旧 Key/Cookie 不会覆盖当前撤销状态', async (t) => {
  const f = await fixture(t),
    { current } = await privateState(f),
    old = await snapshot(f, 'before-revocation');
  const cut = f.db.prepare('SELECT MAX(commit_seq) AS seq FROM commits').get().seq;
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'private-new-key'))();
  await logoutMemberSession(f.members, current.token);
  f.db.prepare("UPDATE upstream_credentials SET pst='private-new-pst' WHERE account_id='a'").run();
  const saved = await snapshot(f, 'after-revocation'),
    target = rebuild(t, saved);
  assert.equal(f.db.prepare('SELECT MAX(commit_seq) AS seq FROM commits').get().seq, cut);
  assert.notEqual(old.sha256, saved.sha256);
  restore(target.db, saved);
  const before = target.db.serialize();
  target.db.transaction(() =>
    assert.throws(
      () => restoreRelationalPrivateSnapshot(target.db, old.filename, old.sha256),
      /Private snapshot/,
    ),
  )();
  assert.deepEqual(target.db.serialize(), before);
  assert.equal(await authenticateMemberApiKey(target.members, keys.m1), null);
  assert.equal((await authenticateMemberApiKey(target.members, 'private-new-key')).id, 'm1');
  assert.equal(await authenticateMemberSession(target.members, current.token, env, NOW + 2), null);
  assert.equal(
    target.db.prepare('SELECT pst FROM upstream_credentials').get().pst,
    'private-new-pst',
  );
});

test('摘要、历史、原提交、成员身份、账户代次和未完成目标错误均在写入秘密前拒绝', async (t) => {
  const f = await fixture(t);
  await privateState(f);
  const saved = await snapshot(f);
  for (const [label, mutate, expectedHash, changeCommit] of [
    ['digest', () => {}, '0'.repeat(64)],
    ['history', (db) => db.prepare("UPDATE projection_state SET history_id='different'").run()],
    [
      'receipt',
      () => {},
      null,
      (stored, seq) => {
        if (seq === '1') {
          stored.command.receipt = {};
        }
      },
    ],
    [
      'fact',
      () => {},
      null,
      (stored, seq) => {
        if (seq === '1') {
          stored.events[0].occurredAt++;
          stored.command.events[0].occurredAt++;
        }
      },
    ],
    ['member', (db) => db.prepare("UPDATE members SET status='disabled' WHERE id='m1'").run()],
    ['epoch', (db) => db.prepare('UPDATE upstream_accounts SET credential_epoch=2').run()],
    ['cut', (db) => db.prepare('UPDATE projection_state SET last_commit_seq=0').run()],
    [
      'private',
      (db) => db.prepare("UPDATE members SET auth_epoch='different' WHERE id='m1'").run(),
    ],
    ['failed', (db) => db.prepare("UPDATE projection_state SET status='failed'").run()],
  ]) {
    await t.test(label, (t) => {
      const { db } = rebuild(t, saved, changeCommit);
      mutate(db);
      const before = db.serialize();
      db.transaction(() =>
        assert.throws(() =>
          restoreRelationalPrivateSnapshot(db, saved.filename, expectedHash || saved.sha256),
        ),
      )();
      assert.deepEqual(db.serialize(), before);
    });
  }
});

test('私有表写到一半失败且被外层捕获，epoch、凭据和会话整笔回滚后可重试', async (t) => {
  const f = await fixture(t);
  await privateState(f);
  const saved = await snapshot(f),
    { db } = rebuild(t, saved);
  db.exec(`CREATE TEMP TRIGGER fail_private BEFORE INSERT ON upstream_credentials
    WHEN EXISTS(SELECT 1 FROM member_sessions) BEGIN SELECT RAISE(ABORT,'private restore failure'); END`);
  const before = databaseState(db);
  db.transaction(() =>
    assert.throws(
      () => restoreRelationalPrivateSnapshot(db, saved.filename, saved.sha256),
      /private restore failure/,
    ),
  )();
  assert.equal(databaseState(db), before);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n, 0);
  db.exec('DROP TRIGGER fail_private');
  assert.equal(restore(db, saved).restored, true);
  assert.equal(databaseState(db), databaseState(f.db));
});

test('源库别名、WAL 和未知格式拒绝，读取失败也不改源字节或创建 sidecar', async (t) => {
  const f = await fixture(t),
    saved = await snapshot(f),
    { db } = rebuild(t, saved);
  assert.throws(
    () => restoreRelationalPrivateSnapshot(db, saved.filename, saved.sha256),
    /owning offline/,
  );
  const alias = path.join(f.dir, 'same-file.sqlite');
  await link(saved.filename, alias);
  const same = new Database(saved.filename);
  try {
    same.pragma('foreign_keys=ON');
    same.transaction(() =>
      assert.throws(
        () => restoreRelationalPrivateSnapshot(same, alias, saved.sha256),
        /Private snapshot/,
      ),
    )();
  } finally {
    same.close();
  }
  for (const journal of ['WAL', 'DELETE']) {
    const filename = path.join(f.dir, `${journal}.sqlite`);
    await copyFile(saved.filename, filename);
    const work = new Database(filename);
    if (journal === 'WAL') {
      work.pragma('journal_mode=WAL');
    } else {
      work.prepare("UPDATE app_meta SET value='unknown' WHERE key='database_format'").run();
    }
    work.close();
    const bytes = await readFile(filename),
      files = await readdir(f.dir);
    db.transaction(() =>
      assert.throws(() => restoreRelationalPrivateSnapshot(db, filename, sha256(bytes))),
    )();
    assert.deepEqual(await readFile(filename), bytes);
    assert.deepEqual(await readdir(f.dir), files);
  }
});

test('超过完整提交页边界仍逐笔核对，只有原事实而未恢复业务身份的目标拒绝', async (t) => {
  const f = await fixture(t);
  for (let index = 0; index < 130; index++) {
    const memberId = `extra-${index}`;
    f.db.transaction(() =>
      f.members.registerMember(meta(`register-${memberId}`), {
        memberId,
        displayName: 'synthetic',
        apiKey: `private-key-${memberId}`,
      }),
    )();
  }
  const saved = await snapshot(f),
    { db } = rebuild(t, saved);
  assert.equal(restore(db, saved).members, 132);
  assert.equal(databaseState(db), databaseState(f.db));
  const factsOnly = new Database(':memory:');
  t.after(() => factsOnly.close());
  initializeRelationalStorage(factsOnly, 'generation-history');
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    factsOnly.transaction(() =>
      replayRelationalCommit(factsOnly, readRelationalCommit(f.db, seq)),
    )();
  }
  const before = factsOnly.serialize();
  assert.throws(() => restore(factsOnly, saved), /Private snapshot/);
  assert.deepEqual(factsOnly.serialize(), before);
});

test('读取期间源字节被外部改变时回滚本次私有恢复，不能接受同提交号的混合快照', async (t) => {
  const f = await fixture(t),
    saved = await snapshot(f),
    { db } = rebuild(t, saved),
    bytes = await readFile(saved.filename);
  const offset = bytes.indexOf(Buffer.from('synthetic-upstream-pst'));
  assert.ok(offset >= 0);
  const changed = Buffer.from(bytes);
  changed[offset] = 'x'.charCodeAt(0);
  db.function('change_source_snapshot', () => {
    writeFileSync(saved.filename, changed);
    return 0;
  });
  db.exec(`CREATE TEMP TRIGGER change_source AFTER INSERT ON upstream_credentials
    BEGIN SELECT change_source_snapshot(); END`);
  const before = databaseState(db);
  db.transaction(() =>
    assert.throws(
      () => restoreRelationalPrivateSnapshot(db, saved.filename, saved.sha256),
      /Private snapshot/,
    ),
  )();
  assert.equal(databaseState(db), before);
  assert.notEqual(sha256(await readFile(saved.filename)), saved.sha256);
});
