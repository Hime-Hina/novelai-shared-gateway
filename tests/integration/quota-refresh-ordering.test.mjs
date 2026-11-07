import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import Database from 'better-sqlite3';
import {
  JsonStore,
  applyEventProjection,
  verifyEventProjections,
} from '../../server/storage/sqlite-store.ts';
import { migrateStorage } from '../../server/storage/sqlite-migrations.ts';
import {
  copyProtectedFacts,
  replayDomainEvents,
  readStreamProjection,
} from '../../server/events/event-store.ts';
import { configureFairQuota, syncFairQuota } from '../../server/quota/fair-share.ts';
import { createAccountQuotaRefresh } from '../../server/quota/account-refresh.ts';
import { publicErrorMessage } from '../../server/http/public-errors.ts';
import { officialKeyError } from '../../server/protocols/official-key-jobs.ts';

const now = 1793491200000,
  at = new Date(now).toISOString();
const quota = {
  tier: 3,
  subscriptionActive: true,
  subscriptionExpiresAt: now + 86400000,
  points: 10000,
  fixed: 10000,
  purchased: 0,
  v5UsagePercent: 80,
};
const defer = () => {
  let resolve, reject;
  const promise = new Promise((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
};
const launched = () => new Promise((resolve) => setImmediate(resolve));
async function fixture(t, { auxiliaryAccounts = [], eventAuthority = true } = {}) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'nai-quota-order-'));
  let store = new JsonStore(dir);
  await store.init();
  t.after(async () => {
    await store.close();
    await rm(dir, { recursive: true, force: true });
  });
  const db = structuredClone(store.db);
  db.users = ['a', 'b', 'c', 'd'].map((id) => ({ id, token: `synthetic-${id}`, enabled: true }));
  db.accounts = [
    ...auxiliaryAccounts,
    {
      id: 'opus',
      token: 'synthetic-pst',
      enabled: true,
      inFlight: 0,
      quotaTier: 3,
      subscriptionActive: true,
      subscriptionExpiresAt: quota.subscriptionExpiresAt,
      quotaFixed: 9000,
      quotaPurchased: 0,
      v5UsagePercent: 80,
      quotaCheckedAt: at,
    },
  ];
  configureFairQuota(db, ['a', 'b', 'c', 'd']);
  syncFairQuota(db, now);
  await store.write(db);
  if (eventAuthority) {
    await store.enableEventAuthority();
  }
  return {
    get store() {
      return store;
    },
    async reopen() {
      await store.close();
      store = new JsonStore(dir);
      await store.init();
    },
    refresh(fetchQuota, extra = {}) {
      return createAccountQuotaRefresh({
        store,
        fetchQuota,
        now: () => now,
        requestTimeoutMs: 1000,
        publicAccount: (value) => ({ ...value, token: undefined }),
        publicErrorMessage,
        logger: { log() {}, error() {} },
        ...extra,
      });
    },
  };
}
const observations = (store) =>
  store.sqlite
    .prepare(
      "SELECT payload FROM domain_events WHERE event_type='QuotaObserved' AND json_extract(payload,'$.kind')='official'",
    )
    .all();

test('newest actual launch wins for success/failure with equal and rolled-back clocks, across automatic/manual refresh', async (t) => {
  for (const rollback of [false, true]) {
    for (const latestFails of [false, true]) {
      const f = await fixture(t),
        first = defer(),
        second = defer();
      let calls = 0,
        time = now;
      const refresh = f.refresh(() => (++calls === 1 ? first : second).promise, {
        now: () => time,
      });
      const older = refresh.refreshEnabled();
      await launched();
      assert.equal(calls, 1);
      if (rollback) {
        time -= 1000;
      }
      const newer = refresh.refresh(['opus']);
      await launched();
      assert.equal(calls, 2);
      if (latestFails) {
        second.reject(
          Object.assign(new Error('synthetic unknown private body'), { upstreamStatus: 401 }),
        );
      } else {
        second.resolve(quota);
      }
      const current = await newer,
        accepted = structuredClone(f.store.db.accounts[0]);
      assert.equal(current.ignored, 0);
      assert.equal(observations(f.store).length, 1);
      if (latestFails) {
        first.resolve({ ...quota, fixed: 99999 });
      } else {
        first.reject(Object.assign(new Error('synthetic old body'), { upstreamStatus: 401 }));
      }
      const ignored = await older;
      assert.deepEqual([ignored.ok, ignored.failed, ignored.ignored], [0, 0, 1]);
      assert.deepEqual(f.store.db.accounts[0], accepted);
      assert.equal(observations(f.store).length, 1);
      assert.equal(accepted.quotaAttemptedAt, new Date(time).toISOString());
      assert.equal(accepted.quotaCheckedAt, latestFails ? at : new Date(time).toISOString());
      assert.equal(accepted.quotaError !== '', latestFails);
    }
  }
});

for (const currentFails of [false, true]) {
  const outcome = currentFails ? '401' : 'success';
  test(`an old credential queued in a full batch cannot supersede a current ${outcome}`, async (t) => {
    const blocked = Array.from({ length: 5 }, defer);
    const auxiliary = blocked.map((_, index) => ({
      id: `auxiliary-${index}`,
      token: `synthetic-auxiliary-${index}`,
      enabled: false,
    }));
    const f = await fixture(t, { auxiliaryAccounts: auxiliary });
    const previousToken = 'synthetic-pst';
    const currentToken = 'synthetic-replacement';
    const previous = defer(),
      current = defer();
    const responses = new Map([
      ...auxiliary.map(({ token }, index) => [token, blocked[index]]),
      [previousToken, previous],
      [currentToken, current],
    ]);
    const calls = [];
    let time = now;
    const refresh = f.refresh(
      async (token) => {
        calls.push(token);
        const result = await responses.get(token).promise;
        if (result instanceof Error) {
          throw result;
        }
        return result;
      },
      { now: () => time },
    );

    assert.equal(f.store.db.settings.fairQuota.boundAccountId, 'opus');
    const batch = refresh.refresh();
    await launched();
    const initiallyLaunched = [...calls];

    await f.store.update(
      (db) => {
        db.accounts.find(({ id }) => id === 'opus').token = currentToken;
      },
      { collections: ['accounts'] },
    );
    time += 1000;
    const currentAt = new Date(time).toISOString();
    const pending = refresh.refresh(['opus']);
    await launched();
    const beforeRelease = [...calls];
    time += 1000;
    blocked[0].resolve(quota);
    await launched();
    const afterRelease = [...calls];

    const invalidKey = Object.assign(new Error('synthetic response'), { upstreamStatus: 401 });
    current.resolve(currentFails ? invalidKey : quota);
    const latest = await pending;
    previous.resolve(currentFails ? { ...quota, fixed: 99999, points: 99999 } : invalidKey);
    for (const helper of blocked.slice(1)) {
      helper.resolve(quota);
    }
    const older = await batch;

    const auxiliaryTokens = auxiliary.map(({ token }) => token);
    assert.deepEqual(initiallyLaunched, auxiliaryTokens);
    assert.deepEqual(beforeRelease, [...auxiliaryTokens, currentToken]);
    assert.deepEqual(afterRelease, [...beforeRelease, previousToken]);
    assert.deepEqual(calls, afterRelease);
    assert.deepEqual(
      [latest.checked, latest.ok, latest.failed, latest.ignored],
      currentFails ? [1, 0, 1, 0] : [1, 1, 0, 0],
    );
    assert.deepEqual([older.checked, older.ok, older.failed, older.ignored], [6, 5, 0, 1]);
    assert.ok(older.accounts.filter(({ id }) => id !== 'opus').every(({ enabled }) => !enabled));
    assert.equal(
      f.store.db.settings.fairQuota.status,
      currentFails ? 'quota_sync_error' : 'active',
    );

    const facts = observations(f.store).map(({ payload }) => JSON.parse(payload));
    assert.equal(facts.length, 6);
    assert.deepEqual(
      facts.filter(({ accountId }) => accountId === 'opus'),
      [
        {
          kind: 'official',
          accountId: 'opus',
          at: currentAt,
          observation: {
            enabled: true,
            subscriptionActive: true,
            subscriptionExpiresAt: quota.subscriptionExpiresAt,
            quotaTier: quota.tier,
            ...(currentFails ? {} : { quotaPoints: quota.points }),
            quotaFixed: currentFails ? 9000 : quota.fixed,
            quotaPurchased: quota.purchased,
            v5UsagePercent: quota.v5UsagePercent,
            quotaCheckedAt: currentFails ? at : currentAt,
            quotaAttemptedAt: currentAt,
            quotaError: currentFails ? '共享 Opus Key 已失效，请联系管理员更新凭据' : '',
            upstreamStatus: currentFails ? 'invalid_key' : 'active',
            updatedAt: currentAt,
          },
        },
      ],
    );
  });
}

test('changing the token during a query ignores the response without an observation', async (t) => {
  const f = await fixture(t),
    result = defer(),
    refresh = f.refresh(() => result.promise);
  const pending = refresh.refresh(['opus']);
  await launched();
  await f.store.update(
    (db) => {
      db.accounts[0].token = 'synthetic-replacement';
    },
    { collections: ['accounts'] },
  );
  result.resolve(quota);
  const output = await pending;
  assert.deepEqual([output.ok, output.failed, output.ignored], [0, 0, 1]);
  assert.equal(observations(f.store).length, 0);
  assert.equal(f.store.db.accounts[0].quotaFixed, 9000);
});

test('removing a target from a legacy store during a query ignores the response', async (t) => {
  const f = await fixture(t, {
    auxiliaryAccounts: [{ id: 'auxiliary', token: 'synthetic-auxiliary', enabled: false }],
    eventAuthority: false,
  });
  const result = defer();
  const pending = f.refresh(() => result.promise).refresh(['auxiliary']);
  await launched();
  await f.store.update(
    (db) => {
      db.accounts = db.accounts.filter(({ id }) => id !== 'auxiliary');
    },
    { collections: ['accounts'] },
  );
  result.resolve(quota);
  assert.deepEqual(await pending, { checked: 1, ok: 0, failed: 0, ignored: 1, accounts: [] });
  assert.equal(observations(f.store).length, 0);
});

test('safe HTTP failure and recovery commit atomically, survive reopen and offline replay', async (t) => {
  const marker = 'SYNTHETIC_PRIVATE_RESPONSE_7c55c6d',
    f = await fixture(t),
    logs = [];
  const server = http.createServer((req, res) => {
    res.writeHead(503);
    res.end(marker);
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const refresh = f.refresh(undefined, {
    env: { NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${server.address().port}/${marker}` },
    logger: { log: (...value) => logs.push(value), error: (...value) => logs.push(value) },
  });
  const output = await refresh.refresh(['opus']);
  assert.equal(output.failed, 1);
  assert.equal(f.store.db.settings.fairQuota.status, 'quota_sync_error');
  const failed = structuredClone(f.store.db.accounts[0]);
  assert.equal(failed.quotaError, '官方额度同步失败，请稍后重试');
  assert.ok(!JSON.stringify([output, observations(f.store), failed, logs]).includes(marker));
  const entry = JSON.parse(logs[0][1]);
  assert.equal(entry.httpStatus, 503);
  assert.equal(entry.accountId, 'opus');
  assert.equal(entry.category, 'http_error');
  const observationCommit = f.store.sqlite
    .prepare(
      "SELECT commit_seq FROM domain_events WHERE event_type='QuotaObserved' ORDER BY commit_seq DESC LIMIT 1",
    )
    .get().commit_seq;
  assert.equal(
    f.store.sqlite
      .prepare(
        "SELECT commit_seq FROM domain_events WHERE event_type='AvailabilityChanged' ORDER BY commit_seq DESC LIMIT 1",
      )
      .get().commit_seq,
    observationCommit,
  );
  const count = observations(f.store).length,
    before = structuredClone(f.store.db.settings.fairQuota);
  f.store.sqlite.exec(
    "CREATE TEMP TRIGGER reject_quota BEFORE INSERT ON domain_events WHEN NEW.event_type='AvailabilityChanged' BEGIN SELECT RAISE(ABORT,'synthetic quota rollback'); END;",
  );
  await assert.rejects(f.refresh(async () => quota).refresh(['opus']), /synthetic quota rollback/);
  assert.deepEqual(f.store.db.accounts[0], failed);
  assert.deepEqual(f.store.db.settings.fairQuota, before);
  assert.equal(observations(f.store).length, count);
  f.store.sqlite.exec('DROP TRIGGER reject_quota');
  await f.reopen();
  assert.deepEqual(f.store.db.accounts[0], failed);
  const recovery = f.refresh(async () => quota, {
    logger: { log: (...value) => logs.push(value), error() {} },
  });
  await recovery.refresh(['opus']);
  assert.equal(f.store.db.accounts[0].quotaError, '');
  assert.equal(f.store.db.settings.fairQuota.status, 'active');
  assert.ok(logs.some((value) => JSON.parse(value[1]).event === 'failed'));
  assert.ok(logs.some((value) => JSON.parse(value[1]).event === 'succeeded'));
  await f.reopen();
  const target = new Database(':memory:');
  t.after(() => target.close());
  migrateStorage(target);
  copyProtectedFacts(f.store.sqlite, target);
  replayDomainEvents(f.store.sqlite, target, { project: applyEventProjection });
  target.transaction(() => verifyEventProjections(target))();
  assert.deepEqual(
    readStreamProjection(target, 'account:opus'),
    readStreamProjection(f.store.sqlite, 'account:opus'),
  );
  assert.deepEqual(
    target.prepare('SELECT data FROM accounts WHERE id=?').get('opus'),
    f.store.sqlite.prepare('SELECT data FROM accounts WHERE id=?').get('opus'),
  );
  assert.ok(
    !JSON.stringify(target.prepare('SELECT payload FROM domain_events').all()).includes(marker),
  );
});

test('bound, unbound and timeout diagnostics contain only whitelist fields and fixed errors', async (t) => {
  const marker = 'SYNTHETIC_PRIVATE_DIAGNOSTIC',
    f = await fixture(t),
    logs = [];
  const logger = { log: (...value) => logs.push(value), error: (...value) => logs.push(value) };
  for (const source of [
    'body',
    'http://url/',
    'socks5://proxy/',
    'pst-',
    'member-key-',
    'stack',
    'code',
  ]) {
    const error = Object.assign(new Error(`${source}${marker}`), {
      code: marker,
      upstreamStatus: 599,
      stack: marker,
    });
    const refresh = f.refresh(
      async () => {
        throw error;
      },
      { logger },
    );
    await refresh.refresh(['opus']);
    await assert.rejects(
      refresh.fetch(`pst-${marker}`, `socks5://${marker}`),
      (value) => !value.message.includes(marker),
    );
    assert.equal(f.store.db.accounts[0].quotaError, '官方额度同步失败，请稍后重试');
  }
  await f
    .refresh(async () => new Promise(() => {}), { logger, requestTimeoutMs: 5 })
    .refresh(['opus']);
  await f
    .refresh(
      async () => {
        throw Object.assign(new Error(marker), { cause: { code: 'ECONNRESET' } });
      },
      { logger },
    )
    .refresh(['opus']);
  assert.ok(!JSON.stringify([observations(f.store), f.store.db.accounts, logs]).includes(marker));
  const entries = logs.map((value) => {
    assert.equal(value[0], '[quota-query]');
    return JSON.parse(value[1]);
  });
  assert.ok(entries.some((value) => value.accountId === null));
  assert.ok(entries.some((value) => value.errorCode === 'QUOTA_REQUEST_TIMEOUT'));
  assert.ok(entries.some((value) => value.errorCode === 'ECONNRESET'));
  assert.ok(
    entries
      .filter((value) => value.category === 'query_error')
      .every((value) => value.errorCode === null && value.httpStatus === null),
  );
  for (const entry of entries) {
    assert.deepEqual(
      Object.keys(entry).sort(),
      ['event', 'at', 'accountId', 'durationMs', 'httpStatus', 'errorCode', 'category'].sort(),
    );
  }
  for (const status of [401, 403, 402, 429]) {
    assert.equal(
      officialKeyError(
        Object.assign(new Error('官方额度同步失败，请稍后重试'), { upstreamStatus: status }),
      ).statusCode,
      status === 403 ? 401 : status,
    );
  }
});

test('a keyed refresh stores its public receipt in the same observation commit', async (t) => {
  const f = await fixture(t),
    refresh = f.refresh(async () => quota);
  const command = {
    domainCommand: {
      commandId: 'synthetic-refresh-command',
      fingerprint: 'synthetic-refresh-fingerprint',
    },
    commandReceipt: ({ checked, ok, failed, ignored }) => ({ checked, ok, failed, ignored }),
  };
  const first = await refresh.refresh(['opus'], command),
    count = observations(f.store).length;
  const duplicate = await refresh.refresh(['opus'], command);
  assert.deepEqual(duplicate, command.commandReceipt(first));
  assert.equal(observations(f.store).length, count);
  const commit = f.store.sqlite
    .prepare('SELECT receipt FROM event_commits WHERE command_id=?')
    .get(command.domainCommand.commandId);
  assert.deepEqual(JSON.parse(commit.receipt), command.commandReceipt(first));
  assert.ok(!commit.receipt.includes('synthetic-pst'));
});

test('a superseded keyed refresh rejects without a receipt or observation', async (t) => {
  const f = await fixture(t),
    older = defer();
  let calls = 0;
  const refresh = f.refresh(() => (++calls === 1 ? older.promise : quota));
  const command = {
    domainCommand: {
      commandId: 'synthetic-superseded-command',
      fingerprint: 'synthetic-superseded-fingerprint',
    },
    commandReceipt: ({ checked, ok, failed, ignored }) => ({ checked, ok, failed, ignored }),
  };
  const pending = refresh.refresh(['opus'], command);
  await launched();
  await refresh.refresh(['opus']);
  const count = observations(f.store).length,
    account = structuredClone(f.store.db.accounts[0]);
  older.resolve({ ...quota, fixed: 99999 });
  await assert.rejects(pending, (error) => error.statusCode === 409);
  assert.equal(observations(f.store).length, count);
  assert.deepEqual(f.store.db.accounts[0], account);
  assert.equal(
    f.store.sqlite
      .prepare('SELECT receipt FROM event_commits WHERE command_id=?')
      .get(command.domainCommand.commandId),
    undefined,
  );
});
