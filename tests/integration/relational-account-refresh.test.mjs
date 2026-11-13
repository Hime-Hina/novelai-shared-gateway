import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once, EventEmitter } from 'node:events';
import path from 'node:path';
import Database from 'better-sqlite3';
import { fixture, NOW, meta, keys, databaseState } from '../helpers/relational-generation.mjs';
import { createRelationalAccountQuotaRefresh } from '../../server/quota/relational-account-refresh.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalAccountStore } from '../../server/storage/relational-account-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { relationalSnapshotSha256 } from '../../server/storage/relational-private-restore.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import { ownerAccountCommand } from '../../server/members/owner-operations.ts';
import { accountQuotaResult } from '../../server/quota/account-refresh.ts';

const rawQuota = (fixed = '8100') => ({
  tier: 3,
  subscriptionActive: true,
  subscriptionExpiresAt: null,
  points: Number(fixed),
  fixed,
  purchased: '0',
  v5UsagePercent: '80',
  v5UsageTimeUntilNextPercent: 0,
});
const defer = () => {
  let resolve, reject;
  const promise = new Promise((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
};
async function turn() {
  await new Promise((resolve) => setImmediate(resolve));
}
async function setup(t, options = {}) {
  const { initialPst, ...refreshOptions } = options;
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { env, seconds: 0 });
  if (initialPst) {
    // 与原夹具一样，选择封存快照之前显式提供测试用当前私有凭据；不伪造轮换事件。
    f.db.prepare('UPDATE upstream_credentials SET pst=?').run(initialPst);
  }
  const sourcePath = path.join(f.dir, 'refresh-source.sqlite');
  await f.db.backup(sourcePath);
  const sealed = new Database(sourcePath);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  await verifyAndInstallRelationalStorage(f.db, {
    sourcePath,
    sourceSha256: relationalSnapshotSha256(sourcePath),
    historyId: 'generation-history',
    finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS n FROM commits').get().n,
    replicas: { dataDir: f.dir },
  });
  assertRelationalStorageReady(f.db);
  const quotas = new RelationalQuotaStore(f.db, env),
    accounts = new RelationalAccountStore(f.db, quotas, new Map(), env),
    events = new EventEmitter(),
    logs = [];
  let clock = NOW + 10;
  const refresh = createRelationalAccountQuotaRefresh({
    sqlite: f.db,
    quotas,
    events,
    env,
    now: () => clock,
    logger: {
      log: (...args) => logs.push(args.join(' ')),
      error: (...args) => logs.push(args.join(' ')),
    },
    ...refreshOptions,
  });
  t.after(() => refresh.stop());
  const authorize = (write) =>
    withAuthenticatedMemberApiKey(f.members, keys.m1, (member) => write(member), env);
  return {
    ...f,
    env,
    quotas,
    accounts,
    events,
    logs,
    refresh,
    authorize,
    setClock(value) {
      clock = value;
    },
    get clock() {
      return clock;
    },
    command: (id, kind, patch = {}) =>
      ownerAccountCommand(
        accounts,
        authorize,
        meta(id, clock, 'm1'),
        { kind, accountId: 'a', ...patch },
        refresh.fetch,
      ),
  };
}
function replay(t, f, until = Number.MAX_SAFE_INTEGER) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const owner = new RelationalCommitReplayer(db);
  for (const { seq } of f.db
    .prepare(
      'SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE commit_seq<=? ORDER BY commit_seq',
    )
    .all(until)) {
    db.transaction(() => owner.replayCommit(readRelationalCommit(f.db, seq), f.inputs))();
  }
  return { db, owner };
}

test('真实 HTTP 查询在 SQL 外；原转换与原金融事实同笔提交、完整回放且不暴露 PST', async (t) => {
  let requests = 0,
    authorization;
  const server = createServer((req, res) => {
    requests++;
    authorization = req.headers.authorization;
    assert.equal(f.db.inTransaction, false);
    assert.equal(req.url, '/user/data');
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(
      JSON.stringify({
        subscription: {
          tier: 3,
          active: true,
          expiresAt: null,
          trainingStepsLeft: { fixedTrainingStepsLeft: 8100, purchasedTrainingSteps: 0 },
          usage: { percent: 80, timeUntilNextPercent: 0 },
        },
      }),
    );
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  });
  const f = await setup(t, {
    env: { NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${server.address().port}` },
  });
  let notified = 0;
  f.events.on('commit', () => {
    notified++;
    assert.equal(f.db.inTransaction, false);
  });
  const first = f.refresh.refreshEnabled(),
    second = f.refresh.refreshEnabled();
  assert.equal(first, second);
  assert.deepEqual(await first, { checked: 1, ok: 1, failed: 0, ignored: 0 });
  assert.equal(requests, 1);
  assert.equal(authorization, 'Bearer synthetic-upstream-pst');
  assert.equal(notified, 1);
  assert.equal(f.quotas.readAccountQuota('a').observedAnlasFixed, '8100');
  assert.ok(BigInt(f.quotas.readAccountQuota('a').observedSequence) > 9007199254740993n);
  assert.equal(f.generations.readWallet('a', 'm1').anlasFixed, '4050');
  const seq = f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS n FROM commits').get().n,
    stored = readRelationalCommit(f.db, seq);
  assert.ok(stored.command.commandId.startsWith('quota-refresh:'));
  assert.equal(stored.command.actorKind, 'system');
  assert.ok(stored.events.some((event) => event.eventType === 'AllocationApplied'));
  assert.equal(JSON.stringify([stored, f.logs]).includes('synthetic-upstream-pst'), false);
  const r = replay(t, f);
  for (const table of [
    'wallets',
    'account_quota_state',
    'operating_state',
    'commits',
    'domain_events',
    'stream_heads',
  ]) {
    assert.deepEqual(
      r.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
      f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
      table,
    );
  }
  assert.equal(r.db.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
  const before = r.db.serialize();
  r.db.transaction(() => assert.equal(r.owner.replayCommit(stored).replayed, true))();
  assert.deepEqual(r.db.serialize(), before);
  assert.equal(requests, 1);
});

test('共用票据 owner：同一时钟或时钟回拨时，后发启用观察胜过旧自动查询', async (t) => {
  for (const rollback of [false, true]) {
    await t.test(String(rollback), async (t) => {
      const older = defer();
      let calls = 0;
      const f = await setup(t, {
        fetchQuota: async () => (++calls === 1 ? older.promise : rawQuota('8200')),
      });
      t.mock.method(Date, 'now', () => f.clock);
      const pending = f.refresh.refreshEnabled();
      await turn();
      assert.equal(calls, 1);
      if (rollback) f.setClock(NOW + 5);
      const command = meta('newer-authorized-enable', f.clock, 'm1'),
        input = { kind: 'enable', accountId: 'a' },
        prepared = await f.authorize((member) =>
          f.accounts.prepareAccountCommand(command, input, member),
        );
      assert.ok(prepared.ticket);
      assert.throws(() => f.refresh.fetch('synthetic', '', 'a'), /has not finished/);
      const before = databaseState(f.db);
      older.resolve(rawQuota('9999'));
      assert.deepEqual(await pending, { checked: 1, ok: 0, failed: 0, ignored: 1 });
      assert.equal(databaseState(f.db), before);
      const result = accountQuotaResult(
        'a',
        await f.refresh.fetch(prepared.ticket.pst, '', 'a'),
        new Date(prepared.ticket.startedAt).toISOString(),
      );
      await f.authorize((member) =>
        f.accounts.completeAccountEnable(command, input, prepared.ticket, result, member),
      );
      assert.equal(f.quotas.readAccountQuota('a').observedAnlasFixed, '8200');
      assert.equal(f.quotas.readAccountQuota('a').observedSequence, '9007199254740995');
    });
  }
});

test('A1→B→A2 的旧查询不能写新代次；后台查询不启用暂停账户', async (t) => {
  const old = defer();
  let calls = 0;
  const originalPst = 'pst-synthetic-refresh-A';
  const f = await setup(t, {
    initialPst: originalPst,
    fetchQuota: async () => (++calls === 1 ? old.promise : rawQuota('8300')),
  });
  t.mock.method(Date, 'now', () => f.clock);
  const pending = f.refresh.refreshEnabled();
  await turn();
  await f.command('rotate-B', 'token', { token: 'pst-synthetic-refresh-B' });
  await f.command('rotate-A2', 'token', { token: originalPst });
  const rotated = databaseState(f.db);
  old.resolve(rawQuota('9999'));
  assert.equal((await pending).ignored, 1);
  assert.equal(databaseState(f.db), rotated);
  assert.equal(f.db.prepare('SELECT enabled FROM upstream_accounts').get().enabled, 0);
  assert.equal(f.db.prepare('SELECT pst FROM upstream_credentials').get().pst, originalPst);
  assert.equal((await f.refresh.refreshEnabled()).checked, 0);
  assert.equal(calls, 1);
  await f.command('verify-A2', 'enable');
  assert.equal(f.quotas.readAccountQuota('a').observedCredentialEpoch, 3);
  assert.equal(f.quotas.readAccountQuota('a').observedAnlasFixed, '8300');
});

test('暂停发生在实际查询之后仍记录原观察，但不会复活 enabled；下一轮不取数', async (t) => {
  const pendingQuery = defer();
  let calls = 0;
  const f = await setup(t, {
    fetchQuota: async () => {
      calls++;
      return pendingQuery.promise;
    },
  });
  const pending = f.refresh.refreshEnabled();
  await turn();
  await f.command('pause-mid-query', 'pause');
  pendingQuery.resolve(rawQuota());
  assert.equal((await pending).ok, 1);
  assert.equal(f.db.prepare('SELECT enabled FROM upstream_accounts').get().enabled, 0);
  assert.equal(f.quotas.readAccountQuota('a').observedAnlasFixed, '8100');
  assert.equal(f.quotas.readAccountQuota('a').status, 'inactive_opus');
  const before = databaseState(f.db);
  assert.deepEqual(await f.refresh.refreshEnabled(), { checked: 0, ok: 0, failed: 0, ignored: 0 });
  assert.equal(databaseState(f.db), before);
  assert.equal(calls, 1);
});

test('deadline 结束等待但 raw 未结束时不累积下一轮；stop/pending 等真实 settle', async (t) => {
  const late = defer();
  let calls = 0,
    signal;
  const f = await setup(t, {
    requestTimeoutMs: 15,
    fetchQuota: async (_token, _env, options) => {
      signal = options.signal;
      return ++calls === 1 ? late.promise : rawQuota('8200');
    },
  });
  assert.equal((await f.refresh.refreshEnabled()).failed, 1);
  assert.equal(signal.aborted, true);
  const physicalPending = f.refresh.pending;
  assert.ok(physicalPending);
  let ended = false;
  physicalPending.then(() => {
    ended = true;
  });
  await turn();
  assert.equal(ended, false);
  const blocked = databaseState(f.db);
  for (let round = 0; round < 3; round++) {
    assert.equal((await f.refresh.refreshEnabled()).checked, 0);
  }
  assert.throws(() => f.refresh.fetch('synthetic', '', 'a'), /has not finished/);
  assert.equal(calls, 1);
  assert.equal(databaseState(f.db), blocked);
  f.refresh.stop();
  const before = databaseState(f.db);
  late.resolve(rawQuota('9999'));
  await physicalPending;
  await turn();
  assert.equal(f.refresh.pending, null);
  assert.equal(databaseState(f.db), before);
  assert.equal((await f.refresh.refreshEnabled()).ok, 1);
  assert.equal(f.quotas.readAccountQuota('a').observedAnlasFixed, '8200');
  assert.equal(calls, 2);
});

test('真实 HTTP 响应体挂起由原 fetch 的 abort 结束；槽释放后可以实际刷新', async (t) => {
  let hang = true,
    requests = 0;
  const server = createServer((_req, res) => {
    requests++;
    res.writeHead(200, { 'content-type': 'application/json' });
    if (hang) {
      res.write('{"subscription":');
      return;
    }
    res.end(
      JSON.stringify({
        subscription: {
          tier: 3,
          active: true,
          expiresAt: null,
          trainingStepsLeft: { fixedTrainingStepsLeft: 8100, purchasedTrainingSteps: 0 },
          usage: { percent: 80, timeUntilNextPercent: 0 },
        },
      }),
    );
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  });
  const f = await setup(t, {
    requestTimeoutMs: 250,
    env: { NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${server.address().port}` },
  });
  assert.equal((await f.refresh.refreshEnabled()).failed, 1);
  const pending = f.refresh.pending;
  if (pending) await pending;
  assert.equal(f.refresh.pending, null);
  hang = false;
  assert.equal((await f.refresh.refreshEnabled()).ok, 1);
  assert.equal(requests, 2);
  assert.equal(f.quotas.readAccountQuota('a').observedAnlasFixed, '8100');
});

test('原失败/期限转换保留最后有效余额，401/403不虚构停用，封禁才保存原停用决定', async (t) => {
  for (const kind of [401, 403, 'banned', 'expiration']) {
    await t.test(String(kind), async (t) => {
      const f = await setup(t, {
        fetchQuota: async () => {
          if (kind === 'expiration')
            return { ...rawQuota('9999'), subscriptionExpiresAt: 'invalid-expiration' };
          throw Object.assign(
            new Error(
              kind === 'banned'
                ? 'Account BANNED synthetic-upstream-pst'
                : 'synthetic-upstream-pst private upstream body',
            ),
            { upstreamStatus: kind === 'banned' ? 403 : kind },
          );
        },
      });
      const before = f.quotas.readAccountQuota('a');
      assert.equal((await f.refresh.refreshEnabled()).failed, 1);
      const after = f.quotas.readAccountQuota('a');
      assert.equal(after.observedAnlasFixed, before.observedAnlasFixed);
      assert.equal(after.observedAt, before.observedAt);
      assert.equal(
        f.db.prepare('SELECT enabled FROM upstream_accounts').get().enabled,
        kind === 'banned' ? 0 : 1,
      );
      assert.equal(
        after.quotaErrorCode,
        kind === 401
          ? 'invalid_key'
          : kind === 403 || kind === 'banned'
            ? 'access_denied'
            : 'sync_error',
      );
      assert.equal(
        JSON.stringify([f.db.prepare('SELECT payload FROM domain_events').all(), f.logs]).includes(
          'synthetic-upstream-pst',
        ),
        false,
      );
      const restored = replay(t, f);
      assert.deepEqual(
        restored.db.prepare('SELECT * FROM account_quota_state').all(),
        f.db.prepare('SELECT * FROM account_quota_state').all(),
      );
    });
  }
});

test('底层吞 abort 返回成功或迟到拒绝，都不能改写原 timeout 观察或产生未处理拒绝', async (t) => {
  for (const kind of ['swallow', 'reject-late']) {
    await t.test(kind, async (t) => {
      const late = defer();
      let signal;
      const f = await setup(t, {
        requestTimeoutMs: 15,
        fetchQuota: async (_token, _env, options) => {
          signal = options.signal;
          if (kind === 'swallow')
            signal.addEventListener('abort', () => late.resolve(rawQuota('9999')), { once: true });
          return late.promise;
        },
      });
      assert.equal((await f.refresh.refreshEnabled()).failed, 1);
      assert.equal(signal.aborted, true);
      assert.equal(f.quotas.readAccountQuota('a').observedAnlasFixed, '8000');
      const before = databaseState(f.db),
        pending = f.refresh.pending;
      if (kind === 'reject-late') late.reject(new Error('synthetic private late failure'));
      if (pending) await pending;
      await turn();
      assert.equal(databaseState(f.db), before);
      assert.equal(f.refresh.pending, null);
    });
  }
});

test('自动 start 幂等，stop 等待中的真实观察可以提交，但不安排下一轮或重新启动', async (t) => {
  const query = defer();
  let calls = 0;
  const f = await setup(t, {
    requestTimeoutMs: 1000,
    refreshIntervalMs: 10,
    fetchQuota: async () => {
      calls++;
      return query.promise;
    },
  });
  f.refresh.start();
  f.refresh.start();
  await turn();
  assert.equal(calls, 1);
  f.refresh.stop();
  const pending = f.refresh.pending;
  assert.ok(pending);
  query.resolve(rawQuota());
  await pending;
  assert.equal(f.quotas.readAccountQuota('a').observedAnlasFixed, '8100');
  await new Promise((resolve) => setTimeout(resolve, 25));
  f.refresh.start();
  await turn();
  assert.equal(calls, 1);
  assert.equal(f.refresh.pending, null);
});

test('公开 query 调用后立即关闭也能取得 pending，直到实际取数结束', async (t) => {
  const raw = defer();
  let calls = 0;
  const f = await setup(t, {
    requestTimeoutMs: 1000,
    fetchQuota: async () => {
      calls++;
      return raw.promise;
    },
  });
  t.after(() => raw.resolve(rawQuota()));
  const query = f.refresh.fetch('synthetic', '', 'a');
  f.refresh.stop();
  const pending = f.refresh.pending;
  assert.ok(pending, 'shutdown must see a query before its first microtask');
  let ended = false;
  pending.then(() => {
    ended = true;
  });
  await turn();
  assert.equal(calls, 1);
  assert.equal(ended, false);
  raw.resolve(rawQuota());
  await pending;
  await query;
  assert.equal(f.refresh.pending, null);
});

test('合法第二钱包已写后 SQL 故障被外层捕获，全表回滚且健康下一票可重试', async (t) => {
  let calls = 0,
    writes = 0,
    notifications = 0;
  const f = await setup(t, {
    fetchQuota: async () => {
      calls++;
      return rawQuota('9000');
    },
  });
  f.events.on('commit', () => {
    notifications++;
    assert.equal(f.db.inTransaction, false);
  });
  f.db.function('refresh_wallet_write', () => {
    writes++;
  });
  f.db
    .exec(`CREATE TEMP TRIGGER fail_second_refresh_wallet AFTER UPDATE OF anlas_fixed_available ON wallets
    BEGIN SELECT refresh_wallet_write(); SELECT CASE WHEN NEW.member_id='m2' THEN RAISE(ABORT,'actual refresh second wallet fault') END; END;`);
  const before = databaseState(f.db);
  await assert.rejects(f.refresh.refreshEnabled(), /actual refresh second wallet fault/);
  assert.equal(writes, 2);
  assert.equal(databaseState(f.db), before);
  assert.equal(notifications, 0);
  assert.equal(f.refresh.pending, null);
  f.db.exec('DROP TRIGGER fail_second_refresh_wallet');
  assert.equal((await f.refresh.refreshEnabled()).ok, 1);
  assert.equal(f.generations.readWallet('a', 'm1').anlasFixed, '4500');
  assert.equal(notifications, 1);
  assert.equal(calls, 2);
  const restored = replay(t, f);
  assert.deepEqual(
    restored.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(),
    f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(),
  );
});

test('building/SQL内查询和缺私有凭据均不取数；通知失败不撤销提交或重发', async (t) => {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'not-ready');
  assert.throws(
    () =>
      createRelationalAccountQuotaRefresh({
        sqlite: db,
        quotas: new RelationalQuotaStore(db),
        events: new EventEmitter(),
      }),
    /incomplete/,
  );
  let calls = 0;
  const f = await setup(t, {
    fetchQuota: async () => {
      calls++;
      return rawQuota();
    },
  });
  f.db.transaction(() =>
    assert.throws(() => f.refresh.fetch('synthetic', '', 'a'), /transaction/),
  )();
  assert.equal(calls, 0);
  f.events.on('commit', () => {
    throw new Error('notification failed');
  });
  assert.equal((await f.refresh.refreshEnabled()).ok, 1);
  assert.equal(f.quotas.readAccountQuota('a').observedAnlasFixed, '8100');
  assert.equal(calls, 1);
  f.db.prepare('DELETE FROM upstream_credentials').run();
  const before = databaseState(f.db);
  await assert.rejects(f.refresh.refreshEnabled(), /私有凭据/);
  assert.equal(databaseState(f.db), before);
  assert.equal(calls, 1);
});
