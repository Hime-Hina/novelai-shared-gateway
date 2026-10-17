import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, cp, appendFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { JsonStore } from '../../server/store.js';
import { migrateStorage, storageMigrationsDirectory } from '../../server/storage-migrations.ts';

async function open(t, directory) {
  const store = new JsonStore(directory || await mkdtemp(path.join(tmpdir(), 'nai-member-session-')));
  t.after(() => store.close());
  await store.init();
  return store;
}
const hash = value => createHash('sha256').update(value).digest('hex');
const owner = ownerUserId => ({ ownerUserId, guildId: 'synthetic-guild' });
const record = (id, overrides = {}) => ({ token_hash: hash(id), user_id: id, auth_version: 'version-1',
  created_at: 1000, expires_at: 1000 + 7 * 86400000, owner_binding: null, ...overrides });
const insert = (store, value) => store.update(() => value, { collections: [], memberSession: result => result });
const coordinate = (store, value) => store.update(() => null, { collections: [], memberSessionOwner: value });
const sessionCount = store => store.sqlite.prepare('SELECT count(*) AS n FROM member_sessions').get().n;

test('会话单独落盘、按摘要精确冷读，重启保持固定时间且普通快照不含会话', async t => {
  const store = await open(t), member = record('synthetic-raw-token');
  await insert(store, member);
  assert.deepEqual(store.readMemberSession(member.token_hash), member);
  assert.equal(store.readMemberSession('synthetic-raw-token'), null);
  const snapshot = await store.read();
  assert.equal(snapshot.member_sessions, undefined);
  assert.equal(store.rowState.member_sessions, undefined);
  assert.equal(store.pendingPersistScope, null);
  const read = store.readMemberSession(member.token_hash);
  read.expires_at = 0;
  assert.equal(store.readMemberSession(member.token_hash).expires_at, member.expires_at);
  const plan = store.sqlite.prepare('EXPLAIN QUERY PLAN SELECT * FROM member_sessions WHERE token_hash=?').all(member.token_hash);
  assert.ok(plan.some(row => /SEARCH.*sqlite_autoindex_member_sessions/.test(row.detail)));
  await store.close();
  const reopened = await open(t, store.dataDir);
  assert.deepEqual(reopened.readMemberSession(member.token_hash), member);
});

test('签发只清截止时刻已过期和同用户旧版本，其他用户及同版本有效会话保留', async t => {
  const store = await open(t);
  const expired = record('expired', { expires_at: 2000 });
  const old = record('old-token', { user_id: 'member-a' });
  const kept = record('other-token');
  const current = record('current-token', { user_id: 'member-a', auth_version: 'version-2', created_at: 2000 });
  for (const value of [expired, old, kept]) await insert(store, value);
  await insert(store, current);
  assert.equal(store.readMemberSession(expired.token_hash), null);
  assert.equal(store.readMemberSession(old.token_hash), null);
  assert.deepEqual(store.readMemberSession(kept.token_hash), kept);
  await insert(store, record('same-version', { user_id: 'member-a', auth_version: 'version-2', created_at: 2001 }));
  assert.deepEqual(store.readMemberSession(current.token_hash), current);
  assert.equal(sessionCount(store), 3);
  for (const [query, values, index] of [
    ['SELECT token_hash FROM member_sessions WHERE expires_at<=?', [2000], 'idx_member_sessions_expiry'],
    ['SELECT token_hash FROM member_sessions WHERE user_id=? AND auth_version<>?', ['member-a', 'version-2'], 'idx_member_sessions_user']
  ]) assert.ok(store.sqlite.prepare('EXPLAIN QUERY PLAN ' + query).all(...values).some(row => row.detail.includes(index)));
});

test('owner A→B→A、guild变化撤销相关会话，首次记录和相同配置不撤销普通成员', async t => {
  const store = await open(t);
  assert.equal(store.readMemberSessionOwner(), null);
  await coordinate(store, owner('a'));
  const ordinary = record('unrelated');
  for (const value of [record('a'), record('b'), ordinary,
    record('old-owner-token', { user_id: 'old-owner', owner_binding: '["old-owner","discord","guild"]' })]) await insert(store, value);
  await coordinate(store, owner('a'));
  assert.equal(sessionCount(store), 4);
  const newOwner = record('new-b-token', { user_id: 'b', owner_binding: '["b","discord","synthetic-guild"]', expires_at: 1000 + 12 * 3600000 });
  await store.update(() => null, { collections: [], memberSessionOwner: owner('b'), memberSession: newOwner });
  assert.deepEqual(store.readMemberSessionOwner(), owner('b'));
  assert.equal(sessionCount(store), 2);
  assert.deepEqual(store.readMemberSession(newOwner.token_hash), newOwner);
  assert.deepEqual(store.readMemberSession(ordinary.token_hash), ordinary);
  await store.close();
  const reopened = await open(t, store.dataDir);
  await coordinate(reopened, owner('a'));
  assert.equal(reopened.readMemberSession(newOwner.token_hash), null);
  assert.equal(reopened.readMemberSession(hash('a')), null);
  assert.equal(reopened.readMemberSession(hash('b')), null);
  await insert(reopened, record('fresh-a', { user_id: 'a' }));
  await coordinate(reopened, { ownerUserId: 'a', guildId: 'new-guild' });
  assert.equal(reopened.readMemberSession(hash('fresh-a')), null);
  assert.deepEqual(reopened.readMemberSession(ordinary.token_hash), ordinary);
});

for (const dirty of [false, true]) {
  test(`会话SQL失败回滚用户/设置、owner协调、过期清理和插入（${dirty ? 'dirty' : '普通'}路径）`, async t => {
    const previousBatch = process.env.SQLITE_BATCH_WRITES;
    process.env.SQLITE_BATCH_WRITES = 'true';
    t.after(() => { if (previousBatch === undefined) delete process.env.SQLITE_BATCH_WRITES; else process.env.SQLITE_BATCH_WRITES = previousBatch; });
    const store = await open(t);
    await store.update(db => { db.users = [{ id: 'a', enabled: true, memberAuth: { version: 'version-1' } }]; }, { immediate: true });
    await coordinate(store, owner('a'));
    for (const value of [record('a'), record('expired', { expires_at: 2000 })]) await insert(store, value);
    const before = await store.read();
    const options = dirty ? { includeSettings: true, dirtyRows: { users: ['a'] } } : {};
    const write = () => store.update(db => {
      db.users[0].memberAuth.version = 'version-2';
      db.settings.syntheticSessionChange = true;
      return record('new', { user_id: 'a', auth_version: 'version-2', created_at: 2000 });
    }, { ...options, memberSession: result => result, memberSessionOwner: () => owner('b'), memberSessionDelete: () => hash('a') });
    store.sqlite.exec("CREATE TEMP TRIGGER reject_session BEFORE INSERT ON member_sessions BEGIN SELECT RAISE(ABORT, 'synthetic session failure'); END");
    await assert.rejects(write(), /synthetic session failure/);
    assert.deepEqual(await store.read(), before);
    assert.deepEqual(store.readMemberSessionOwner(), owner('a'));
    assert.equal(sessionCount(store), 2);
    assert.ok(store.readMemberSession(hash('a')));
    assert.ok(store.readMemberSession(hash('expired')));
    assert.equal(store.readMemberSession(hash('new')), null);
    assert.equal(store.pendingPersistScope, null);
    store.sqlite.exec('DROP TRIGGER reject_session');
    await write();
    assert.deepEqual(store.readMemberSessionOwner(), owner('b'));
    assert.equal(store.db.users[0].memberAuth.version, 'version-2');
    assert.equal(sessionCount(store), 1);
    assert.equal(store.pendingPersistScope, null);
  });
}

test('退出删除立即持久化，SQL删除失败保留原会话；重复退出无副作用', async t => {
  const store = await open(t), member = record('member');
  await insert(store, member);
  const logout = () => store.update(() => null, { collections: [], memberSessionDelete: member.token_hash });
  store.sqlite.exec("CREATE TEMP TRIGGER reject_logout BEFORE DELETE ON member_sessions BEGIN SELECT RAISE(ABORT, 'synthetic logout failure'); END");
  await assert.rejects(logout(), /synthetic logout failure/);
  assert.deepEqual(store.readMemberSession(member.token_hash), member);
  store.sqlite.exec('DROP TRIGGER reject_logout');
  await logout(); await logout();
  await store.close();
  assert.equal((await open(t, store.dataDir)).readMemberSession(member.token_hash), null);
});

test('owner配置不变不写元数据，配置写失败撤销操作也回滚', async t => {
  const store = await open(t);
  await coordinate(store, owner('a'));
  await insert(store, record('a'));
  store.sqlite.exec("CREATE TEMP TRIGGER reject_owner BEFORE INSERT ON app_meta WHEN NEW.key='member_session_owner' BEGIN SELECT RAISE(ABORT, 'synthetic owner failure'); END");
  await coordinate(store, owner('a'));
  await assert.rejects(coordinate(store, owner('b')), /synthetic owner failure/);
  assert.deepEqual(store.readMemberSessionOwner(), owner('a'));
  assert.ok(store.readMemberSession(hash('a')));
});

test('10000有效会话上限在同一事务内检查，过期可释放且并发不能超额', async t => {
  const store = await open(t);
  const statement = store.sqlite.prepare('INSERT INTO member_sessions (token_hash,user_id,auth_version,created_at,expires_at,owner_binding) VALUES (@token_hash,@user_id,@auth_version,@created_at,@expires_at,@owner_binding)');
  store.sqlite.transaction(() => { for (let i = 0; i < 10000; i++) statement.run(record('bulk-' + i)); })();
  await assert.rejects(insert(store, record('over-limit')), error => error.statusCode === 503 && error.message === '登录会话已满，请稍后重试');
  assert.equal(sessionCount(store), 10000);
  store.sqlite.prepare('UPDATE member_sessions SET expires_at=1000 WHERE token_hash=?').run(hash('bulk-0'));
  const results = await Promise.allSettled([insert(store, record('last-slot-a')), insert(store, record('last-slot-b'))]);
  assert.equal(results.filter(result => result.status === 'fulfilled').length, 1);
  assert.equal(results.find(result => result.status === 'rejected').reason.statusCode, 503);
  assert.equal(sessionCount(store), 10000);
  assert.equal(store.readMemberSession(hash('bulk-0')), null);
  assert.ok(store.readMemberSession(hash('last-slot-a')));
  assert.equal(store.readMemberSession(hash('last-slot-b')), null);
});

test('业务全量替换和日志清理不删除冷会话及owner配置', async t => {
  const store = await open(t), member = record('member');
  await insert(store, member); await coordinate(store, owner('a'));
  await store.update(db => { db.jobs.push({ id: 'done', status: 'done', createdAt: new Date().toISOString() }); }, { immediate: true });
  assert.equal((await store.clearRequestLogs()).removed, 1);
  await store.write(await store.read());
  assert.deepEqual(store.readMemberSession(member.token_hash), member);
  assert.deepEqual(store.readMemberSessionOwner(), owner('a'));
});

test('0010迁移可重复执行，改写已登记迁移被拒绝', async t => {
  const store = await open(t);
  const journal = store.sqlite.prepare('SELECT hash,created_at FROM __drizzle_migrations ORDER BY created_at').all();
  migrateStorage(store.sqlite);
  assert.deepEqual(store.sqlite.prepare('SELECT hash,created_at FROM __drizzle_migrations ORDER BY created_at').all(), journal);
  const columns = store.sqlite.prepare('PRAGMA table_info(member_sessions)').all();
  assert.equal(columns.find(column => column.name === 'token_hash').pk, 1);
  assert.equal(columns.find(column => column.name === 'expires_at').type, 'INTEGER');
  const copy = path.join(await mkdtemp(path.join(tmpdir(), 'nai-session-migrations-')), 'migrations');
  await cp(storageMigrationsDirectory, copy, { recursive: true });
  await appendFile(path.join(copy, '0010_persistent_member_sessions.sql'), '\n-- changed applied migration\n');
  assert.throws(() => migrateStorage(store.sqlite, copy), /migration history differs/);
  assert.deepEqual(store.sqlite.prepare('SELECT hash,created_at FROM __drizzle_migrations ORDER BY created_at').all(), journal);
});
