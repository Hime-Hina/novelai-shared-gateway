import { stripTypeScriptTypes } from 'node:module';
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import Database from 'better-sqlite3';
import { JsonStore } from '../../server/store.ts';
import { configureFairQuota, syncFairQuota, reserveFairQuota, settleFairQuota, resolveFairQuotaCharge } from '../../server/fair-quota.ts';

const now = Date.now();
const request = { model: 'nai-diffusion-5-full', width: 1024, height: 1024, steps: 23 };
const writeOptions = { immediate: true, includeSettings: true, collections: ['jobs', 'ledger'] };
async function open(t, directory) {
  const store = new JsonStore(directory || await mkdtemp(path.join(tmpdir(), 'nai-fair-settlements-')));
  await store.init();
  t.after(() => store.close());
  return store;
}
async function fixture(t, variant = 'v5') {
  const store = await open(t);
  await store.update(db => {
    db.users = ['a', 'b', 'c', 'd'].map(id => ({ id, token: id, enabled: true, balance: 100 }));
    db.accounts = [{ id: 'opus', enabled: true, quotaTier: 3, subscriptionActive: true,
      quotaFixed: variant === 'purchased' ? 0 : 10000, quotaPurchased: 1000,
      v5UsagePercent: 80, v5UsageTimeUntilNextPercent: 0, quotaCheckedAt: now }];
    configureFairQuota(db, ['a', 'b', 'c', 'd']); syncFairQuota(db, now);
    if (variant === 'fallback') {
      db.settings.fairQuota.autoAnlasFallback = true;
      db.settings.fairQuota.balances.a.v5Available = 0;
    }
    const input = variant === 'free' ? { ...request, model: 'nai-diffusion-4-5-full' }
      : ['fixed', 'purchased'].includes(variant) ? { ...request, width: 1536, allowPurchasedAnlas: true } : request;
    const job = { id: 'job', userToken: 'a', status: 'running', request: input, createdAt: new Date(now).toISOString() };
    db.jobs.push(job);
    reserveFairQuota(db, db.users[0], job, db.accounts[0], now);
  }, { immediate: true });
  return store;
}
async function finish(store, success, options = writeOptions) {
  return store.update(db => {
    const job = db.jobs.find(job => job.id === 'job');
    const charge = settleFairQuota(db, job, success, now + 1, { requestStarted: false }, store.readFairQuotaSettlement(job.id));
    job.status = success ? 'done' : 'failed';
    job.completedAt = job.updatedAt = new Date(now + 1).toISOString();
    return charge;
  }, options);
}

for (const variant of ['free', 'v5', 'fixed', 'purchased', 'fallback']) {
  test(`${variant} 成功/失败冷存后不重复扣退，终态不能重新reserve`, async t => {
    for (const success of [true, false]) {
      const store = await fixture(t, variant);
      const reserved = structuredClone(store.db.jobs[0]);
      const original = await finish(store, success);
      assert.equal(store.db.settings.fairQuota.reservations.job, undefined);
      assert.deepEqual(store.readFairQuotaSettlement('job'), original);
      assert.equal(store.db.jobs[0].fairCharge.status, success ? 'settled' : 'refunded');
      const before = structuredClone(store.db.settings.fairQuota), ledgerCount = store.countRecords('ledger');
      await store.update(db => {
        for (const outcome of [false, true, false]) {
          assert.deepEqual(settleFairQuota(db, reserved, outcome, now + 100, {}, store.readFairQuotaSettlement('job')), original);
        }
        assert.throws(() => reserveFairQuota(db, db.users[0], reserved, db.accounts[0], now + 100, reserved.request,
          store.readFairQuotaSettlement('job')), error => error.statusCode === 409);
      }, { immediate: true, includeSettings: true, collections: ['ledger'] });
      assert.deepEqual(store.db.settings.fairQuota, before);
      assert.equal(store.countRecords('ledger'), ledgerCount);
      assert.throws(() => resolveFairQuotaCharge(store.db, { id: 'job', userToken: 'b' }, original), /归属不符/);
      assert.throws(() => settleFairQuota(store.db, { id: 'job', fairCharge: { userId: 'b' } }, false, now, {}, original), /归属不符/);
    }
  });
}

test('冷终态优先于陈旧reserved副本，删除任务/重启后refundJob不落回旧余额退款', async t => {
  const store = await fixture(t), stale = structuredClone(store.db.jobs[0]);
  await finish(store, true);
  await store.clearRequestLogs();
  await store.close();
  const reopened = await open(t, store.dataDir);
  const before = structuredClone(reopened.db.settings.fairQuota);
  const source = stripTypeScriptTypes(await readFile(new URL('../../server/gateway-runtime.ts', import.meta.url), 'utf8'), { mode: 'strip' });
  const start = source.indexOf('function refundJob('), end = source.indexOf('\n}', start) + 2;
  const ctx = vm.createContext({ store: reopened, settleFairQuota, fairShareMode: true });
  vm.runInContext(source.slice(start, end), ctx);
  await reopened.update(db => {
    stale.cost = 999;
    assert.equal(ctx.refundJob(db, stale, '重复失败').status, 'settled');
    assert.equal(ctx.refundJob(db, { id: 'unknown', userToken: 'a', cost: 999 }, '无凭据'), null);
  }, { immediate: true, includeSettings: true, collections: ['users', 'ledger'] });
  assert.equal(reopened.db.users[0].balance, 100);
  assert.deepEqual(reopened.db.settings.fairQuota, before);
  assert.equal(reopened.countRecords('jobs'), 0);
});

test('已有reserved仍须通过当前成员分组与上游资格校验', async t => {
  const store = await fixture(t), db = store.db, job = db.jobs[0];
  db.settings.fairQuota.members = ['b', 'c', 'd'];
  assert.throws(() => reserveFairQuota(db, db.users[0], job, db.accounts[0], now), error => error.statusCode === 403);
  db.settings.fairQuota.members = ['a', 'b', 'c', 'd'];
  db.accounts[0].enabled = false;
  assert.throws(() => reserveFairQuota(db, db.users[0], job, db.accounts[0], now), error => error.statusCode === 503);
  assert.equal(db.settings.fairQuota.reservations.job.status, 'reserved');
});

for (const dirty of [false, true]) {
  test(`冷行写入失败回滚任务/设置/账本；${dirty ? 'dirty' : 'incremental'}路径`, async t => {
    const store = await fixture(t), before = await store.read();
    const options = dirty ? { immediate: true, includeSettings: true,
      dirtyRows: (_value, db) => ({ jobs: ['job'], ledger: db.ledger.map(event => event.id) }) } : { immediate: true };
    store.sqlite.exec("CREATE TEMP TRIGGER reject_settlement BEFORE INSERT ON fair_quota_settlements BEGIN SELECT RAISE(ABORT, 'synthetic settlement failure'); END");
    await assert.rejects(finish(store, false, options), /synthetic settlement failure/);
    assert.deepEqual(await store.read(), before);
    assert.equal(store.readFairQuotaSettlement('job'), null);
    store.sqlite.exec('DROP TRIGGER reject_settlement');
    await finish(store, false, options);
    assert.equal(store.readFairQuotaSettlement('job').status, 'refunded');
    assert.equal(store.db.settings.fairQuota.reservations.job, undefined);
  });
}

test('冷历史不热载，已删除job的终态保留，未知reserved原样留下', async t => {
  const old = process.env.RUNTIME_JOBS_CACHE_LIMIT;
  process.env.RUNTIME_JOBS_CACHE_LIMIT = '2';
  t.after(() => { if (old === undefined) delete process.env.RUNTIME_JOBS_CACHE_LIMIT; else process.env.RUNTIME_JOBS_CACHE_LIMIT = old; });
  const store = await fixture(t);
  await store.update(db => {
    for (let i = 0; i < 1100; i++) {
      db.settings.fairQuota.reservations[`history-${i}`] = { userId: 'a', resource: 'v5', amount: .05,
        status: i % 2 ? 'settled' : 'refunded', reservedAt: now - 10, settledAt: now - 1 };
      db.jobs.push({ id: `history-${i}`, status: 'done', createdAt: new Date(now).toISOString() });
    }
    db.settings.fairQuota.reservations['missing-job-reserved'] = { userId: 'a', resource: 'v5', amount: .2, status: 'reserved' };
  }, { immediate: true });
  assert.equal(store.hasPartialCollection('jobs'), true);
  assert.equal(store.sqlite.prepare('SELECT COUNT(*) AS n FROM fair_quota_settlements').get().n, 1100);
  assert.deepEqual(Object.keys(store.db.settings.fairQuota.reservations).sort(), ['job', 'missing-job-reserved']);
  assert.equal(store.db.fair_quota_settlements, undefined);
  assert.equal(store.rowState.fair_quota_settlements, undefined);
  const marker = structuredClone(store.db.settings.fairQuota);
  await store.close();
  const reopened = await open(t, store.dataDir);
  assert.deepEqual(reopened.db.settings.fairQuota, marker);
  await reopened.update(db => {
    const job = { id: 'history-0', userToken: 'a', fairCharge: { status: 'reserved', userId: 'a' } };
    assert.equal(settleFairQuota(db, job, false, now, {}, reopened.readFairQuotaSettlement(job.id)).status, 'refunded');
  }, { immediate: true, includeSettings: true, collections: [] });
  assert.deepEqual(reopened.db.settings.fairQuota, marker);
  await reopened.clearRequestLogs();
  assert.ok(reopened.readFairQuotaSettlement('history-0'));
});

test('重复导入同凭据幂等，冲突不覆盖冷凭据且整事务回滚', async t => {
  const store = await fixture(t); await finish(store, true);
  const original = store.readFairQuotaSettlement('job');
  await store.update(db => { db.settings.fairQuota.reservations.job = { ...original }; }, { immediate: true, collections: ['settings'] });
  const before = await store.read();
  for (const conflict of [{ userId: 'b' }, { status: 'refunded' }, { amount: original.amount + 1 }]) {
    await assert.rejects(store.update(db => {
      db.settings.fairQuota.reservations.job = { ...original, ...conflict };
      db.users[0].balance = 0;
    }, { immediate: true, collections: ['settings', 'users'] }), /结算凭据冲突/);
    assert.deepEqual(await store.read(), before);
    assert.deepEqual(store.readFairQuotaSettlement('job'), original);
  }
  const replacement = structuredClone(before);
  replacement.settings.fairQuota.reservations.job = { ...original, amount: original.amount + 1 };
  await assert.rejects(store.write(replacement), /结算凭据冲突/);
  assert.deepEqual(await store.read(), before);
  // 启动遇到旧热凭据与冷凭据冲突也保留原始证据，不提交一半迁移。
  const incoming = JSON.stringify(replacement.settings);
  store.sqlite.prepare("UPDATE app_settings SET value=? WHERE key='settings'").run(incoming);
  await store.close();
  const failed = new JsonStore(store.dataDir);
  t.after(() => failed.close());
  await assert.rejects(failed.init(), /结算凭据冲突/);
  assert.equal(failed.sqlite.prepare("SELECT value FROM app_settings WHERE key='settings'").get().value, incoming);
  assert.deepEqual(failed.readFairQuotaSettlement('job'), original);
});

for (const format of ['typed', 'app_records', 'json']) {
  test(`旧${format}导入归档无job终态且保留reserved和所有累计字段`, async t => {
    const directory = await mkdtemp(path.join(tmpdir(), `nai-fair-${format}-`));
    const quota = { reservations: { old: { userId: 'a', status: 'settled', resource: 'v5', amount: .2 },
      refunded: { userId: 'a', status: 'refunded', resource: 'anlas', amount: 5, fixed: 5, purchased: 0 },
      unknown: { userId: 'a', status: 'reserved', resource: 'v5', amount: .1 } },
      spent: { v5: .2, fallbackFixed: 3 }, lastSettlementAt: now, fallbackBuffer: { fixed: 3, purchased: 0 },
      operating: { unfunded: { anlasFixed: 7 } } };
    const settings = { fairQuota: quota };
    if (format === 'json') await writeFile(path.join(directory, 'library.json'), JSON.stringify({ settings, users: [{ id: 'a' }] }));
    else {
      const sqlite = new Database(path.join(directory, 'library.sqlite'));
      sqlite.exec(await readFile(new URL('../fixtures/sqlite-before-drizzle.sql', import.meta.url), 'utf8'));
      sqlite.prepare('INSERT INTO app_settings(key,value) VALUES(?,?)').run('settings', JSON.stringify(settings));
      if (format === 'typed') sqlite.prepare('INSERT INTO users(id,order_value,data) VALUES(?,?,?)').run('a', 1, '{"id":"a"}');
      else sqlite.prepare('INSERT INTO app_records(collection,id,order_value,data) VALUES(?,?,?,?)').run('users', 'a', 1, '{"id":"a"}');
      sqlite.close();
    }
    const store = await open(t, directory);
    assert.deepEqual(store.readFairQuotaSettlement('old'), quota.reservations.old);
    assert.deepEqual(store.readFairQuotaSettlement('refunded'), quota.reservations.refunded);
    assert.deepEqual(store.db.settings.fairQuota, { ...quota, reservations: { unknown: quota.reservations.unknown } });
    await store.close();
    const reopened = await open(t, directory);
    assert.equal(reopened.sqlite.prepare('SELECT COUNT(*) AS n FROM fair_quota_settlements').get().n, 2);
    assert.deepEqual(reopened.db.settings.fairQuota, store.db.settings.fairQuota);
  });
}
