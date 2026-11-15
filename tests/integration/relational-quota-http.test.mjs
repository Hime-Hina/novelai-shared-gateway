import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { EventEmitter, once } from 'node:events';
import Database from 'better-sqlite3';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import {
  fixture as generationFixture,
  installReady,
  meta,
  NOW,
  keys as sourceKeys,
  databaseState,
} from '../helpers/relational-generation.mjs';
import { createRelationalQuotaHttp } from '../../server/quota/relational-quota-http.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { relationalStateNotificationsFor } from '../../server/events/relational-state-notifications.ts';
import {
  createMemberSession,
  authenticateMemberApiKey,
  withAuthenticatedMemberApiKey,
} from '../../server/members/authentication.ts';
import { accountQuotaResult } from '../../server/quota/account-refresh.ts';
import { launcherSubscription } from '../../server/quota/launcher-subscription.ts';
import { configureFairGroups, syncFairQuota } from '../../server/quota/fair-share.ts';
import { sharedSurface } from '../../server/http/api-surface.ts';
import { gatewayKeyFrom } from '../../server/members/http-request.ts';

const paths = [
  '/user/subscription',
  '/v1/user/subscription',
  '/ai/user/subscription',
  '/v1/ai/user/subscription',
];
const keys = { m1: 'STA1N-relational-quota-m1', m2: 'STA1N-relational-quota-m2' };
const discordEnv = {
  DISCORD_CLIENT_ID: '111111111111111111',
  DISCORD_CLIENT_SECRET: 'synthetic-discord-secret',
  DISCORD_GUILD_ID: '444444444444444444',
  PUBLIC_BASE_URL: 'https://gateway.example',
};
function observe(f, expiresAt, patch = {}) {
  const quotas = new RelationalQuotaStore(f.db),
    ticket = quotas.beginQuotaObservation('a', NOW);
  f.db.transaction(() =>
    quotas.recordQuotaObservation(
      meta(`http-observe-${ticket.sequence}`, NOW),
      ticket,
      accountQuotaResult(
        'a',
        {
          tier: 3,
          subscriptionActive: true,
          subscriptionExpiresAt: expiresAt,
          fixed: '8000',
          purchased: '0',
          v5UsagePercent: 80,
          v5UsageIsNegative: false,
          v5UsageTimeUntilNextPercent: 600,
          ...patch,
        },
        new Date(NOW).toISOString(),
      ),
    ),
  )();
}
function legacy(expiry) {
  const db = {
    users: ['m1', 'm2'].map((id) => ({ id, enabled: true })),
    settings: {},
    accounts: [
      {
        id: 'a',
        enabled: true,
        quotaTier: 3,
        subscriptionActive: true,
        subscriptionExpiresAt: expiry,
        quotaCheckedAt: NOW,
        v5UsagePercent: 80,
        v5UsageIsNegative: false,
        v5UsageTimeUntilNextPercent: 600,
        quotaFixed: 8000,
        quotaPurchased: 0,
      },
    ],
  };
  configureFairGroups(
    db,
    [
      { id: 'g', name: 'shared', share: 1, members: ['m1', 'm2'], memberWeights: { m1: 1, m2: 1 } },
      ...['g2', 'g3', 'g4'].map((id) => ({
        id,
        name: id,
        share: 0,
        members: [],
        memberWeights: {},
      })),
    ],
    { now: NOW },
  );
  syncFairQuota(db, NOW);
  return db;
}
async function memberFixture(t) {
  const dir = await mkdtemp(path.join(tmpdir(), 'nai-quota-http-members-')),
    db = new Database(path.join(dir, 'library.sqlite'));
  t.after(async () => {
    db.close();
    await rm(dir, { recursive: true, force: true });
  });
  initializeRelationalStorage(db, 'generation-history');
  const members = new RelationalMemberStore(db);
  for (const id of ['m1', 'm2']) {
    db.transaction(() =>
      members.registerMember(meta(`register-${id}`), {
        memberId: id,
        displayName: id,
        apiKey: sourceKeys[id],
      }),
    )();
  }
  return { dir, db, members, generations: new RelationalGenerationStore(db) };
}
async function setup(t, options = {}) {
  const cleanup = [],
    f = await (options.withoutPolicy ? memberFixture : generationFixture)(
      { after: (fn) => cleanup.push(fn) },
      options,
    ),
    env = { OWNER_USER_ID: 'm1', PUBLIC_MODE: 'true', ...options.env },
    events = new EventEmitter(),
    members = new RelationalMemberStore(f.db, env, relationalStateNotificationsFor(f.db, events));
  await options.initialize?.(f, members);
  relationalStateNotificationsFor(f.db, events).commit(() =>
    f.db.transaction(() => {
      for (const [memberId, key] of Object.entries(keys)) {
        members.replaceMemberApiKey(memberId, key);
      }
    })(),
  );
  if (options.expiry !== undefined) {
    observe(f, options.expiry);
  }
  let clock = options.clock ?? NOW;
  if (env.DISCORD_GUILD_ID) {
    t.mock.method(Date, 'now', () => clock);
    for (const [memberId, discordId] of [
      ['m1', '222222222222222222'],
      ['m2', '333333333333333333'],
    ]) {
      await members.discordAuthenticationTransaction((tx) => {
        const user = tx.readMember(memberId);
        user.discord = {
          id: discordId,
          guildId: env.DISCORD_GUILD_ID,
          accessToken: `synthetic-quota-access-${memberId}`,
          refreshToken: `synthetic-quota-refresh-${memberId}`,
          expiresAt: clock + 86400000,
          checkedAt: memberId === 'm1' ? 0 : clock,
          member: true,
        };
        tx.saveAuthentication(user, 'observation');
      });
    }
  }
  await installReady(f);
  const notifier = relationalStateNotificationsFor(f.db, events),
    handle = createRelationalQuotaHttp({
      sqlite: f.db,
      events,
      env,
      discordFetch: options.discordFetch && ((...args) => options.discordFetch(f, ...args)),
      now: () => clock,
      sendJson(res, status, value) {
        assert.equal(f.db.inTransaction, true, '真正发送仍在当前 Key 的同步授权回调内');
        res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' });
        res.end(JSON.stringify(value));
      },
    });
  const server = createServer(async (req, res) => {
    try {
      const url = new URL(req.url, `http://${req.headers.host}`);
      if (!sharedSurface(url.pathname, req.method).allowed || !(await handle(req, res, url))) {
        res.writeHead(404);
        res.end();
      }
    } catch (error) {
      res.writeHead(error.statusCode ?? 500, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: error.message }));
    }
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const origin = `http://127.0.0.1:${server.address().port}`;
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    for (const run of cleanup) {
      await run();
    }
  });
  return {
    ...f,
    members,
    notifier,
    events,
    env,
    origin,
    get clock() {
      return clock;
    },
    set clock(value) {
      clock = value;
    },
    async call(endpoint = paths[0], { token = keys.m1, headers = {}, ...rest } = {}) {
      const response = await fetch(origin + endpoint, {
        headers: { ...(token ? { authorization: `Bearer ${token}` } : {}), ...headers },
        ...rest,
      });
      const text = await response.text();
      return {
        status: response.status,
        headers: response.headers,
        body: text ? JSON.parse(text) : null,
      };
    },
  };
}

test('真实 ready localhost 四订阅 alias 完整等于原 launcher DTO，quota 保留完整摘要/免费次数且 query_only 零写', async (t) => {
  const expiry = NOW + 86400000,
    f = await setup(t, { expiry }),
    old = legacy(expiry),
    before = databaseState(f.db);
  {
    for (const endpoint of paths) {
      const result = await f.call(endpoint);
      assert.equal(result.status, 200, JSON.stringify(result.body));
      assert.deepEqual(result.body, launcherSubscription(old, old.users[0], NOW));
      assert.equal(result.headers.get('set-cookie'), null);
    }
    const result = await f.call('/api/me/quota');
    assert.equal(result.status, 200);
    assert.deepEqual(result.body, {
      fairQuota: {
        ...new RelationalQuotaStore(f.db).readMemberQuotaSummary('m1', NOW),
        freeImages: f.generations.readMemberFreeImageUsage('m1', NOW),
      },
    });
    assert.equal(databaseState(f.db), before);
  }
  // 在线 Key owner 使用原 BEGIN IMMEDIATE；query_only 验证实际 DTO 读取，不改认证事务语义。
  f.db.pragma('query_only=ON');
  try {
    const quotas = new RelationalQuotaStore(f.db);
    assert.deepEqual(
      quotas.readLauncherSubscription('m1', NOW),
      launcherSubscription(old, old.users[0], NOW),
    );
    quotas.readMemberQuotaSummary('m1', NOW);
    f.generations.readMemberFreeImageUsage('m1', NOW);
    assert.equal(databaseState(f.db), before);
  } finally {
    f.db.pragma('query_only=OFF');
  }
  for (const secret of [keys.m1, keys.m2, 'synthetic-upstream-pst', 'authEpoch', 'tokenHash']) {
    assert.ok(!JSON.stringify((await f.call()).body).includes(secret));
  }
  // 到期由原 launcher owner 判定；GET 不落实恢复、钱包或 cursor。
  f.clock = expiry + 1;
  const expired = await f.call();
  assert.equal(expired.status, 200);
  assert.equal(expired.body.active, false);
  assert.equal(expired.body.expiresAt, 0);
  assert.equal(databaseState(f.db), before);
});

test('五 GET 沿原 gatewayKeyFrom 精确优先级，不续 Cookie、不接 PST/管理员或 surface 排除路径', async (t) => {
  const f = await setup(t, {
    expiry: NOW + 86400000,
    async initialize(f, members) {
      await withAuthenticatedMemberApiKey(members, sourceKeys.m1, (actor) =>
        new RelationalQuotaStore(f.db, { OWNER_USER_ID: 'm1' }).manageMemberQuota(
          meta('http-distinguish-wallets', NOW, 'm1'),
          {
            action: 'transfer-quota',
            expectedConfigVersion: 1,
            fromUserId: 'm1',
            toUserId: 'm2',
            resource: 'v5',
            amount: 10,
          },
          actor,
          new Map(),
        ),
      );
    },
  });
  const m1 = (await f.call()).body,
    m2 = (await f.call(paths[0], { token: keys.m2 })).body;
  assert.notEqual(m1.usage.percent, m2.usage.percent);
  const session = await createMemberSession(
      f.members,
      await authenticateMemberApiKey(f.members, keys.m1),
      f.env,
      NOW,
    ),
    before = databaseState(f.db);
  for (const endpoint of [...paths, '/api/me/quota']) {
    const expected1 = (await f.call(endpoint)).body,
      expected2 = (await f.call(endpoint, { token: keys.m2 })).body;
    assert.deepEqual(
      (await f.call(`${endpoint}?token=${keys.m2}`, { headers: { 'x-user-token': keys.m2 } })).body,
      expected1,
    );
    assert.deepEqual(
      (
        await f.call(`${endpoint}?token=${keys.m1}`, {
          token: null,
          headers: { 'x-user-token': keys.m2 },
        })
      ).body,
      expected1,
    );
    assert.deepEqual(
      (await f.call(endpoint, { token: null, headers: { 'x-user-token': keys.m2 } })).body,
      expected2,
    );
    assert.deepEqual(
      (
        await f.call(`${endpoint}?token=${keys.m1}`, {
          headers: { authorization: 'bearer unrecognized' },
        })
      ).body,
      expected1,
    );
    assert.equal(
      (
        await f.call(`${endpoint}?token=${keys.m1}`, {
          headers: { authorization: 'Bearer invalid' },
        })
      ).status,
      401,
    );
    for (const headers of [
      { cookie: `nai_member_session=${session.token}` },
      { 'x-admin-token': 'synthetic-admin' },
      { authorization: 'Bearer synthetic-upstream-pst' },
    ]) {
      assert.equal((await f.call(endpoint, { token: null, headers })).status, 401);
    }
    const cookieRead = await f.call(endpoint, {
      headers: {
        cookie: `nai_member_session=${session.token}`,
        origin: 'https://launcher.example',
      },
    });
    assert.equal(cookieRead.status, 200);
    assert.equal(cookieRead.headers.get('set-cookie'), null);
    assert.equal((await f.call(endpoint, { method: 'POST' })).status, 404);
  }
  // HTTP 解析器会去掉 header 尾部 OWS；精确空 Bearer 的纯 helper 规则也保持不变。
  assert.equal(
    gatewayKeyFrom(
      { headers: { authorization: 'Bearer ' } },
      new URL(`https://gateway.example/?token=${keys.m1}`),
    ),
    '',
  );
  for (const endpoint of [
    '/user/data',
    '/v1/user/data',
    '/api/web/jobs',
    '/v1/ai/user/subscription/',
  ]) {
    assert.equal((await f.call(endpoint)).status, 404);
  }
  assert.equal(databaseState(f.db), before);
});

test('真实免费成功计数、未知期限和已知零保留原 DTO 含义，禁用成员仍先拒绝', async (t) => {
  const free = await setup(t, {
    freeLimit: 2,
    clock: NOW + 20,
    async initialize(f) {
      await f.accept('http-free', { steps: 28 });
      f.dispatch('http-free-0');
      await f.publish('http-free-0');
    },
  });
  const result = await free.call('/api/me/quota');
  assert.equal(result.status, 200);
  assert.deepEqual(
    result.body.fairQuota.freeImages,
    free.generations.readMemberFreeImageUsage('m1', free.clock),
  );
  assert.equal(result.body.fairQuota.freeImages.successful, 1);
  const zero = await setup(t, {
      v5: '0',
      secondV5: '40',
      fixed: '3329.999999999999',
      officialFixed: '6659.999999999998',
      purchased: '8.9',
      officialPurchased: '17.8',
      seconds: 0,
    }),
    before = databaseState(zero.db),
    unknown = await zero.call();
  assert.equal(unknown.status, 200);
  assert.equal(unknown.body.active, false);
  assert.equal(unknown.body.expiresAt, 0);
  assert.deepEqual(unknown.body.trainingStepsLeft, {
    fixedTrainingStepsLeft: 3330,
    purchasedTrainingSteps: 8,
  });
  assert.deepEqual(unknown.body.usage, { percent: 0, isNegative: true, timeUntilNextPercent: 0 });
  assert.equal(databaseState(zero.db), before);
  zero.notifier.commit(() =>
    zero.db.transaction(() =>
      zero.members.setMemberStatus(meta('http-disable'), 'm1', 'disabled'),
    )(),
  );
  assert.equal((await zero.call()).status, 401);
});

test('无共享政策仍是合法 ready：quota NULL、订阅原 403，factory 不接受 building 或不同 emitter', async (t) => {
  const building = await memberFixture(t),
    deps = {
      sqlite: building.db,
      events: new EventEmitter(),
      sendJson() {
        assert.fail('building must not send');
      },
    },
    before = databaseState(building.db);
  assert.throws(() => createRelationalQuotaHttp(deps), /incomplete/);
  assert.equal(databaseState(building.db), before);
  const f = await setup(t, { withoutPolicy: true }),
    readyBefore = databaseState(f.db);
  assert.deepEqual((await f.call('/api/me/quota')).body, { fairQuota: null });
  for (const endpoint of paths) {
    assert.equal((await f.call(endpoint)).status, 403);
  }
  assert.throws(
    () => createRelationalQuotaHttp({ ...deps, sqlite: f.db }),
    /one runtime state emitter/,
  );
  assert.equal(databaseState(f.db), readyBefore);
});

test('Discord 真 IO 后当前 Key 转归、撤销、auth epoch 或 ready 变化均拒绝，不持网络 SQL', async (t) => {
  for (const change of ['reassign', 'revoke', 'epoch', 'ready']) {
    await t.test(change, async (t) => {
      let enter, release;
      const entered = new Promise((resolve) => {
          enter = resolve;
        }),
        gate = new Promise((resolve) => {
          release = resolve;
        }),
        f = await setup(t, {
          env: discordEnv,
          async discordFetch(f) {
            assert.equal(f.db.inTransaction, false);
            enter();
            await gate;
            return Response.json({ user: { id: '222222222222222222' } });
          },
        }),
        prepared =
          change === 'epoch'
            ? await f.members.prepareMemberCredentials('m1', {
                username: 'new.quota.member',
                password: 'synthetic-quota-new-password',
              })
            : null,
        pending = f.call(change === 'epoch' ? '/api/me/quota' : paths[0]);
      await entered;
      t.after(() => release());
      if (change === 'reassign') {
        f.notifier.commit(() =>
          f.db.transaction(() => {
            f.members.replaceMemberApiKey('m1', 'replacement-m1');
            f.members.replaceMemberApiKey('m2', keys.m1);
          })(),
        );
      } else if (change === 'revoke') {
        f.notifier.commit(() =>
          f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'replacement-m1'))(),
        );
      } else if (change === 'epoch') {
        f.notifier.commit(() =>
          f.db.transaction(() => f.members.setMemberCredentials(prepared, null))(),
        );
      } else {
        f.db.prepare("UPDATE projection_state SET status='building' WHERE id=1").run();
      }
      const financial = f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(),
        commitCount = f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n,
        before = databaseState(f.db);
      release();
      const result = await pending;
      assert.equal(result.status, change === 'ready' ? 500 : 401);
      assert.equal(result.body.usage, undefined);
      assert.deepEqual(f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(), financial);
      assert.equal(
        f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n,
        commitCount + (change === 'ready' ? 0 : 1),
      );
      assert.deepEqual(
        f.db
          .prepare('SELECT event_type,member_id FROM domain_events WHERE commit_seq>?')
          .all(commitCount),
        change === 'ready' ? [] : [{ event_type: 'MemberEligibilityChanged', member_id: 'm1' }],
      );
      if (change === 'ready') {
        assert.equal(databaseState(f.db), before, '撤 ready 后 observation 也不能写');
      }
    });
  }
});

test('Discord 拒绝复用实际 observation 与同一 notifier，仅一次撤销通知且零金融事实', async (t) => {
  const f = await setup(t, {
      env: discordEnv,
      discordFetch: async (f) => {
        assert.equal(f.db.inTransaction, false);
        return new Response('', { status: 403 });
      },
    }),
    changes = [],
    before = f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n,
    wallets = f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all();
  f.events.on('commit', (event) => changes.push(event));
  const response = await f.call('/api/me/quota');
  assert.equal(response.status, 403);
  assert.equal(response.headers.get('set-cookie'), null);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n, before + 1);
  assert.deepEqual(
    f.db.prepare('SELECT event_type,member_id FROM domain_events WHERE commit_seq>?').all(before),
    [{ event_type: 'MemberEligibilityChanged', member_id: 'm1' }],
  );
  assert.deepEqual(f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(), wallets);
  assert.equal(changes.length, 1);
  assert.ok(changes[0].collections.includes('users'));
  assert.deepEqual(changes[0].memberIds, ['m1']);
  assert.equal((await authenticateMemberApiKey(f.members, keys.m1)).discord.member, false);
  assert.equal((await f.call()).status, 403);
});

test('Discord observation 尾部 SQL 故障由原事务完整回滚，健康重试一次，随后窄读零事实', async (t) => {
  let calls = 0;
  const f = await setup(t, {
      env: discordEnv,
      discordFetch: async (f) => {
        assert.equal(f.db.inTransaction, false);
        calls++;
        return Response.json({ user: { id: '222222222222222222' } });
      },
    }),
    before = databaseState(f.db),
    commits = [];
  f.events.on('commit', (event) => commits.push(event));
  f.db.exec(
    "CREATE TEMP TRIGGER quota_http_observation_fault AFTER INSERT ON domain_events WHEN NEW.event_type='MemberEligibilityChanged' BEGIN SELECT RAISE(ABORT,'quota http observation fault'); END",
  );
  assert.equal((await f.call()).status, 500);
  assert.equal(databaseState(f.db), before);
  assert.equal(commits.length, 0);
  f.db.exec('DROP TRIGGER quota_http_observation_fault');
  assert.equal((await f.call()).status, 200);
  assert.equal(calls, 2);
  assert.equal(commits.length, 1);
  const after = databaseState(f.db);
  assert.equal((await f.call('/api/me/quota')).status, 200);
  assert.equal(databaseState(f.db), after);
  assert.equal(calls, 2);
  assert.equal(commits.length, 1);
});

test('公开完整回放零 fetch/秘密，关系配额仍由原事实展示且 HTTP 不获得私有 Key 授权', async (t) => {
  const f = await setup(t, { expiry: NOW + 86400000 }),
    db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const replayer = new RelationalCommitReplayer(db);
  let fetches = 0;
  t.mock.method(globalThis, 'fetch', () => {
    fetches++;
    throw new Error('replay must not fetch');
  });
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    db.transaction(() =>
      replayer.replayCommit(readRelationalCommit(f.db, seq), new RelationalInputStore(f.db)),
    )();
  }
  const quota = new RelationalQuotaStore(db);
  assert.deepEqual(
    quota.readMemberQuotaSummary('m1', NOW),
    new RelationalQuotaStore(f.db).readMemberQuotaSummary('m1', NOW),
  );
  assert.deepEqual(
    quota.readLauncherSubscription('m1', NOW),
    new RelationalQuotaStore(f.db).readLauncherSubscription('m1', NOW),
  );
  for (const table of [
    'wallets',
    'account_quota_state',
    'sharing_policy',
    'commits',
    'domain_events',
  ]) {
    assert.deepEqual(
      db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
      f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
      table,
    );
  }
  for (const table of ['api_keys', 'member_sessions', 'upstream_credentials']) {
    assert.equal(db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0);
  }
  assert.ok(
    db
      .prepare('SELECT auth_epoch FROM members')
      .all()
      .every((row) => row.auth_epoch === null),
  );
  assert.equal(await authenticateMemberApiKey(new RelationalMemberStore(db), keys.m1), null);
  assert.equal(fetches, 0);
});
