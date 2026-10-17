import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { once } from 'node:events';
import { JsonStore } from '../../server/store.js';
import { configureFairQuota, syncFairQuota, reserveFairQuota, settleFairQuota, manageFairMember, manageOperatingFunding } from '../../server/fair-quota.ts';
import { launcherSubscription } from '../../server/launcher-subscription.ts';
import { handleMemberRoutes } from '../../server/member-routes.js';

const now = 1800000000000;
const request = { model: 'nai-diffusion-5-full', width: 1024, height: 1024, steps: 29 };
function fixture({ fixed = 1000, purchased = 1000, v5 = 80 } = {}) {
  const db = { settings: {}, users: ['a', 'b', 'c', 'd'].map(id => ({ id, token: id, enabled: true })), jobs: [], ledger: [],
    accounts: [{ id: 'opus', enabled: true, quotaTier: 3, subscriptionActive: true, quotaFixed: fixed,
      quotaPurchased: purchased, v5UsagePercent: v5, quotaCheckedAt: now, v5UsageTimeUntilNextPercent: 0 }] };
  configureFairQuota(db, ['a', 'b', 'c', 'd']); syncFairQuota(db, now);
  return db;
}
function observe(db, changes, at) {
  Object.assign(db.accounts[0], changes, { quotaCheckedAt: at });
  return syncFairQuota(db, at);
}
function fund(db, body, at = now, store) {
  const persisted = id => store?.readOperatingLossSettlements(id) || [];
  return manageOperatingFunding(db, body, { actorId: 'a',
    findEvent: id => db.ledger.find(event => event.id === id) || store?.selectItemById('ledger', id),
    coveredAmount: id => [...new Map([...persisted(id), ...db.ledger.filter(event => event.type === 'operating_loss_covered' && event.lossEventId === id)]
      .map(event => [event.id, event])).values()].reduce((total, event) => total + event.amount, 0)
  }, at);
}
const contribute = (receipt, resource, amount) => ({ action: 'operating-contribute', receipt, resource, amount });
const register = (receipt, resource, amount) => ({ action: 'operating-register-funding', receipt, resource, amount });
const cover = (receipt, resource, amount, lossEventId) => ({ action: 'operating-cover-loss', receipt, resource, amount, lossEventId });
const claimTotal = (db, resource) => Object.values(db.settings.fairQuota.balances).reduce((sum, b) => sum + b[resource + 'Available'], 0)
  + (db.settings.fairQuota.operating?.available[resource] || 0);

for (const [resource, field] of [['anlasFixed', 'quotaFixed'], ['anlasPurchased', 'quotaPurchased']]) {
  test(`${resource} 失败后真实下降只记聚合待承担，不重复扣成员、不把报价当实扣`, () => {
    const db = fixture({ fixed: resource === 'anlasPurchased' ? 0 : 1000 });
    const before = structuredClone(db.settings.fairQuota.balances);
    const job = { id: 'failed', status: 'running', request: { ...request, allowPurchasedAnlas: true } };
    const charge = reserveFairQuota(db, db.users[0], job, db.accounts[0], now);
    job.upstreamStartedAt = new Date(now + 1).toISOString();
    settleFairQuota(db, job, false, now + 2);
    observe(db, { [field]: 1000 - charge.amount }, now + 3);
    assert.deepEqual(db.settings.fairQuota.balances, before);
    assert.equal(db.settings.fairQuota.operating.unfunded[resource], charge.amount);
    const loss = db.ledger.find(event => event.type === 'resource_gap_observed');
    assert.equal(loss.observedAmount, charge.amount); assert.equal(loss.actualTaskConsumption, null);
    const ledger = structuredClone(db.ledger);
    syncFairQuota(db, now + 3); observe(db, { [field]: 1000 - charge.amount }, now + 4);
    assert.deepEqual(db.ledger, ledger);
    const next = reserveFairQuota(db, db.users[0], { id: 'next', request: { ...request, allowPurchasedAnlas: true } }, db.accounts[0], now + 4);
    assert.equal(next.resource, 'anlas'); // 无运营资金也不新增调用门槛。
  });
}

test('明确耗尽的官方固定池不扣本地固定权益，原购买权限、购买余额和快照校验仍有效', () => {
  const db = fixture(); observe(db, { quotaFixed: 0 }, now + 1);
  const before = structuredClone(db.settings.fairQuota.balances);
  const make = (id, allowed) => ({ id, request: { ...request, allowPurchasedAnlas: allowed } });
  assert.throws(() => reserveFairQuota(db, db.users[0], make('denied', false), db.accounts[0], now + 1), /未经明确授权/);
  const allowed = make('allowed', true), charge = reserveFairQuota(db, db.users[0], allowed, db.accounts[0], now + 1);
  assert.equal(charge.fixed, 0); assert.equal(charge.purchased, charge.amount);
  assert.equal(db.settings.fairQuota.balances.a.anlasFixedAvailable, before.a.anlasFixedAvailable);
  settleFairQuota(db, allowed, false, now + 1);
  observe(db, { quotaFixed: 0 }, now + 2); assert.deepEqual(db.settings.fairQuota.balances, before);
  db.settings.fairQuota.balances.a.anlasPurchasedAvailable = 0;
  assert.throws(() => reserveFairQuota(db, db.users[0], make('poor', true), db.accounts[0], now + 2), /购买 Anlas 不足/);
  for (const value of [null, undefined]) {
    const unknown = fixture(); observe(unknown, { quotaFixed: value }, now + 1);
    assert.throws(() => reserveFairQuota(unknown, unknown.users[0], make('unknown', true), unknown.accounts[0], now + 1), /官方公共额度不足/);
  }
  assert.throws(() => reserveFairQuota(fixture(), { id: 'a' }, make('stale', true), fixture().accounts[0], now + 700000), /stale/);
});

test('本人划入守恒，自动及明确核销只使用有来源运营余额并关联当前损耗', () => {
  const db = fixture(), original = claimTotal(db, 'anlasFixed');
  fund(db, contribute('own-share-1', 'anlasFixed', 20));
  assert.equal(claimTotal(db, 'anlasFixed'), original);
  assert.equal(db.settings.fairQuota.balances.b.anlasFixedAvailable, 250);
  observe(db, { quotaFixed: 970 }, now + 1);
  const ops = db.settings.fairQuota.operating, lossId = ops.gapEventIds.anlasFixed;
  assert.equal(ops.available.anlasFixed, 0); assert.equal(ops.unfunded.anlasFixed, 10);
  assert.throws(() => fund(db, cover('no-funds-1', 'anlasFixed', 10, lossId), now + 2), /余额不足/);
  fund(db, contribute('own-share-2', 'anlasFixed', 10), now + 2);
  fund(db, cover('cover-loss-1', 'anlasFixed', 10, lossId), now + 2);
  assert.equal(ops.unfunded.anlasFixed, 0); assert.equal(claimTotal(db, 'anlasFixed'), 970);
  assert.equal(db.settings.fairQuota.balances.b.anlasFixedAvailable, 250);
  assert.equal(db.ledger.filter(event => event.type === 'operating_loss_covered').reduce((sum, event) => sum + event.amount, 0), 30);
  assert.throws(() => fund(db, cover('cover-extra-1', 'anlasFixed', 1, lossId), now + 2), /超过/);
});

test('外部注资登记本身不发资金；官方新增先匹配收据、剩余才分配，重复快照不再发', () => {
  const db = fixture(), before = structuredClone(db.settings.fairQuota.balances);
  assert.throws(() => fund(db, register('unsafe-integer-funds', 'anlasPurchased', 1e20)), /数量无效/);
  assert.equal(db.ledger.length, 0);
  fund(db, register('external-topup-1', 'anlasPurchased', 100));
  assert.equal(db.settings.fairQuota.operating.available.anlasPurchased, 0);
  assert.deepEqual(db.settings.fairQuota.balances, before);
  observe(db, { quotaPurchased: 1060 }, now + 1);
  assert.equal(db.settings.fairQuota.operating.available.anlasPurchased, 60);
  assert.deepEqual(db.settings.fairQuota.balances, before);
  observe(db, { quotaPurchased: 1140 }, now + 2);
  assert.equal(db.settings.fairQuota.operating.available.anlasPurchased, 100);
  assert.equal(db.settings.fairQuota.operating.pendingExternalFunding.length, 0);
  assert.equal(db.settings.fairQuota.balances.b.anlasPurchasedAvailable, before.b.anlasPurchasedAvailable + 10);
  const total = claimTotal(db, 'anlasPurchased'); observe(db, { quotaPurchased: 1140 }, now + 3);
  assert.equal(claimTotal(db, 'anlasPurchased'), total); assert.equal(total, 1140);
  assert.throws(() => fund(db, register('not-v5-money', 'v5', 1)), /自然|本人份额/);
});

test('10000→0→10000 无法分类时不翻倍钱包、不核销损耗；明确快照修正仅追加关联证据', () => {
  const db = fixture({ fixed: 10000 }), before = structuredClone(db.settings.fairQuota.balances);
  observe(db, { quotaFixed: 0 }, now + 1);
  observe(db, { quotaFixed: 10000 }, now + 2);
  const ops = db.settings.fairQuota.operating;
  assert.deepEqual(db.settings.fairQuota.balances, before);
  assert.equal(ops.unfunded.anlasFixed, 10000); assert.equal(ops.unattributedReturns.anlasFixed, 10000);
  assert.equal(ops.available.anlasFixed, 0);
  const lossId = ops.gapEventIds.anlasFixed;
  fund(db, { action: 'operating-reconcile-return', receipt: 'confirmed-correction', resource: 'anlasFixed', amount: 10000, lossEventId: lossId }, now + 2);
  assert.equal(ops.unfunded.anlasFixed, 0); assert.equal(ops.unattributedReturns.anlasFixed, 0);
  assert.deepEqual(db.settings.fairQuota.balances, before);
  assert.ok(db.ledger.some(event => event.id === lossId && event.actualTaskConsumption === null));
  assert.equal(db.ledger[0].evidenceLevel, 'operator_attested_aggregate_correction');
  // 修正记录不是收入；相同快照/重启不能再分给成员。
  const restarted = JSON.parse(JSON.stringify(db)); observe(restarted, { quotaFixed: 10000 }, now + 3);
  assert.deepEqual(restarted.settings.fairQuota.balances, before);
});

test('真实公共充值不拿去核销旧差额；无缺口充值仍自动分配，异常归属不是调用门槛', () => {
  const db = fixture({ fixed: 10000 }); observe(db, { quotaFixed: 9000 }, now + 1);
  const before = structuredClone(db.settings.fairQuota.balances), ops = db.settings.fairQuota.operating;
  fund(db, { ...register('public-renewal-1', 'anlasFixed', 1000), destination: 'members' }, now + 1);
  observe(db, { quotaFixed: 10000 }, now + 2);
  assert.equal(db.settings.fairQuota.balances.b.anlasFixedAvailable, before.b.anlasFixedAvailable + 250);
  assert.equal(ops.unfunded.anlasFixed, 1000); assert.equal(ops.available.anlasFixed, 0);
  assert.equal(ops.unattributedReturns.anlasFixed, 0);
  assert.equal(reserveFairQuota(db, db.users[0], { id: 'normal', request }, db.accounts[0], now + 2).resource, 'anlas');
  const normal = fixture(); observe(normal, { quotaFixed: 2000 }, now + 1);
  assert.equal(normal.settings.fairQuota.balances.b.anlasFixedAvailable, 500);
  const paid = fixture(); fund(paid, contribute('already-covered-loss', 'anlasFixed', 100));
  observe(paid, { quotaFixed: 900 }, now + 1); observe(paid, { quotaFixed: 1900 }, now + 2);
  assert.equal(paid.settings.fairQuota.balances.b.anlasFixedAvailable, 500);
  assert.equal(paid.settings.fairQuota.operating.available.anlasFixed, 0);
  assert.equal(paid.ledger.filter(event => event.type === 'operating_loss_covered').reduce((sum, event) => sum + event.amount, 0), 100);
  // 先观察到异常返回，之后提供明确公共新增收据，同样只增加成员，不把收入当运营资金。
  const late = fixture(); observe(late, { quotaFixed: 900 }, now + 1); observe(late, { quotaFixed: 1000 }, now + 2);
  fund(late, { ...register('late-public-proof', 'anlasFixed', 100), destination: 'members', fromUnattributed: true }, now + 2);
  assert.equal(late.settings.fairQuota.balances.b.anlasFixedAvailable, 275);
  assert.equal(late.settings.fairQuota.operating.unfunded.anlasFixed, 100);
  assert.equal(late.settings.fairQuota.operating.unattributedReturns.anlasFixed, 0);
  fund(late, { ...register('late-public-proof', 'anlasFixed', 100), destination: 'members', fromUnattributed: true }, now + 3);
  assert.equal(late.settings.fairQuota.balances.b.anlasFixedAvailable, 275);
});

test('V5 容量差额先消耗恢复量，同一恢复不再发个人；历史及运营份额保留', () => {
  const db = fixture(); db.settings.fairQuota.balances.a.v5Available = 15;
  observe(db, { v5UsagePercent: 74.5, v5UsageTimeUntilNextPercent: 600 }, now + 1);
  const ops = db.settings.fairQuota.operating;
  assert.equal(ops.v5CapacityGap, 0.5);
  const initial = structuredClone(db.settings.fairQuota.balances);
  syncFairQuota(db, now + 300001);
  assert.equal(ops.v5CapacityGap, 0); assert.deepEqual(db.settings.fairQuota.balances, initial);
  observe(db, { v5UsagePercent: 75 }, now + 300001);
  syncFairQuota(db, now + 600001);
  assert.equal(db.settings.fairQuota.balances.a.v5Available, initial.a.v5Available + 0.125);
  assert.equal(db.ledger.filter(event => event.type === 'v5_capacity_recovered').reduce((sum, event) => sum + event.amount, 0), 0.5);
  assert.ok(db.ledger.some(event => event.type === 'v5_capacity_observed' && event.amount === 0.5));
  const full = fixture({ v5: 100 }); full.accounts[0].v5UsageTimeUntilNextPercent = 600; full.settings.fairQuota.snapshot.seconds = 600;
  fund(full, contribute('v5-own-share', 'v5', 2)); syncFairQuota(full, now + 300000);
  assert.equal(full.settings.fairQuota.balances.a.v5Available, 23);
  assert.equal(full.settings.fairQuota.operating.available.v5, 2); assert.equal(claimTotal(full, 'v5'), 100);
  const covered = fixture(); observe(covered, { v5UsagePercent: 79.5 }, now + 1);
  fund(covered, contribute('v5-cover-source', 'v5', 0.5), now + 1);
  fund(covered, cover('v5-explicit-cover', 'v5', 0.5, covered.settings.fairQuota.operating.v5GapEventId), now + 1);
  assert.equal(covered.settings.fairQuota.operating.v5CapacityGap, 0);
  assert.equal(covered.settings.fairQuota.operating.available.v5, 0);
  assert.equal(claimTotal(covered, 'v5'), 79.5);
});

test('只读启动器与管理预览不得污染真实 ledger 或运营差额', () => {
  const db = fixture(); Object.assign(db.accounts[0], { quotaFixed: 900, quotaCheckedAt: now + 1 });
  const before = structuredClone(db);
  launcherSubscription(db, db.users[0], now + 1);
  manageFairMember(db, { action: 'preview-delete-member', userId: 'a', transferToUserId: 'b', expectedConfigVersion: 1 }, now + 1);
  assert.deepEqual(db, before);
});

test('真实 SQLite 收据/核销覆盖冷历史和重启，失败整笔回滚，settings-only 调用显式提交 ledger', async t => {
  let store = new JsonStore(await mkdtemp(path.join(tmpdir(), 'nai-operating-accounting-')));
  t.after(() => store.close()); await store.init();
  await store.update(db => Object.assign(db, fixture()), { immediate: true });
  const write = { collections: ['settings', 'ledger'], immediate: true };
  await store.update(db => fund(db, contribute('durable-own-1', 'anlasFixed', 20), now, store), write);
  await store.update(db => observe(db, { quotaFixed: 970 }, now + 1), { collections: ['accounts', 'settings', 'ledger'], immediate: true });
  const lossId = store.db.settings.fairQuota.operating.gapEventIds.anlasFixed;
  await store.update(db => fund(db, contribute('durable-own-2', 'anlasFixed', 10), now + 2, store), write);
  const original = structuredClone(store.db.settings.fairQuota);
  store.sqlite.exec("CREATE TEMP TRIGGER reject_cover BEFORE INSERT ON ledger WHEN json_extract(NEW.data, '$.id') = 'operating-receipt:durable-cover' BEGIN SELECT RAISE(ABORT, 'synthetic cover failure'); END;");
  await assert.rejects(store.update(db => fund(db, cover('durable-cover', 'anlasFixed', 10, lossId), now + 2, store), write), /synthetic cover failure/);
  assert.deepEqual(store.db.settings.fairQuota, original); store.sqlite.exec('DROP TRIGGER reject_cover');
  await store.update(db => fund(db, cover('durable-cover', 'anlasFixed', 10, lossId), now + 2, store), write);
  const dataDir = store.dataDir; await store.close(); store = new JsonStore(dataDir); await store.init();
  store.trimRuntimeCollection('ledger', 0);
  const duplicate = await store.update(db => fund(db, contribute('durable-own-1', 'anlasFixed', 20), now + 3, store), write);
  assert.equal(duplicate.duplicate, true);
  await assert.rejects(store.update(db => fund(db, contribute('durable-own-1', 'anlasFixed', 21), now + 3, store), write), /不同操作/);
  const repeatedCover = await store.update(db => fund(db, cover('durable-cover', 'anlasFixed', 10, lossId), now + 3, store), write);
  assert.equal(repeatedCover.duplicate, true);
  assert.equal(store.db.settings.fairQuota.operating.unfunded.anlasFixed, 0);
  assert.equal(store.readOperatingLossSettlements(lossId).reduce((sum, event) => sum + event.amount, 0), 30);
  assert.equal(claimTotal(store.db, 'anlasFixed'), 970);
  assert.equal(store.sqlite.pragma('integrity_check', { simple: true }), 'ok');
});

test('真实管理 HTTP 保留鉴权和本人来源，GET只估算，官方同步提交资金证据', async t => {
  const store = new JsonStore(await mkdtemp(path.join(tmpdir(), 'nai-operating-routes-')));
  await store.init();
  await store.update(db => {
    Object.assign(db, fixture());
    const live = Date.now(); db.accounts[0].quotaCheckedAt = live;
    db.settings.fairQuota.snapshot.at = db.settings.fairQuota.lastRefillAt = live;
  }, { immediate: true });
  const deps = { store, env: { OWNER_USER_ID: 'a' }, assertAdmin(req) {
    if (req.headers['x-admin-token'] !== 'synthetic-admin') throw Object.assign(new Error('admin required'), { statusCode: 403 });
  } };
  const server = http.createServer(async (req, res) => {
    try { await handleMemberRoutes(req, res, new URL(req.url, 'http://localhost'), deps); }
    catch (e) { res.writeHead(e.statusCode || 500, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: e.message })); }
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(async () => { await new Promise(resolve => server.close(resolve)); await store.close(); });
  const base = `http://127.0.0.1:${server.address().port}`;
  const call = (body, authenticated = true) => fetch(`${base}/api/admin/member-management`, { method: body ? 'POST' : 'GET',
    headers: { origin: base, 'content-type': 'application/json', ...(authenticated ? { 'x-admin-token': 'synthetic-admin' } : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}) });
  assert.equal((await call(contribute('http-owner-share', 'anlasFixed', 20), false)).status, 403);
  assert.equal((await call({ ...contribute('http-other-share', 'anlasFixed', 20), userId: 'b' })).status, 403);
  assert.equal((await call(contribute('http-owner-share', 'anlasFixed', 20))).status, 200);
  assert.equal(store.selectItemById('ledger', 'operating-receipt:http-owner-share').memberId, 'a');
  assert.equal((await call(register('http-external-funds', 'anlasPurchased', 100))).status, 200);
  assert.equal(store.db.settings.fairQuota.operating.available.anlasPurchased, 0);
  await store.update(db => Object.assign(db.accounts[0], { quotaFixed: 970, quotaPurchased: 1140, quotaCheckedAt: Date.now() + 10 }), { collections: ['accounts'], immediate: true });
  const storedSettings=store.settingsState, storedLedger=store.countRecords('ledger');
  const projected=await call();assert.equal(projected.status,200);
  assert.equal((await projected.json()).users.find(user=>user.id==='b').quota.anlasPurchasedAvailable,260);
  assert.equal(store.settingsState,storedSettings);assert.equal(store.countRecords('ledger'),storedLedger);
  assert.equal(store.db.settings.fairQuota.operating.available.anlasPurchased,0);
  // 实际余额刷新在其写事务内 syncFairQuota；读取投影不能成为账务提交入口。
  await store.update(db=>syncFairQuota(db,Date.now()+20),{collections:['settings','ledger'],immediate:true});
  const ops = store.db.settings.fairQuota.operating;
  assert.equal(ops.available.anlasPurchased, 100); assert.equal(ops.unfunded.anlasFixed, 10);
  assert.equal(store.db.settings.fairQuota.balances.b.anlasPurchasedAvailable, 260);
  assert.equal(store.selectItemById('ledger', ops.gapEventIds.anlasFixed).observedAmount, 30);
  store.trimRuntimeCollection('ledger', 0);
  const repeated = await call(contribute('http-owner-share', 'anlasFixed', 20));
  assert.equal(repeated.status, 200); assert.equal((await repeated.json()).duplicate, true);
  assert.equal((await call(contribute('http-owner-share', 'anlasFixed', 21))).status, 409);
});
