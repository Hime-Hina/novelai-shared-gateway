import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request as httpRequest } from 'node:http';
import { EventEmitter, once } from 'node:events';
import Database from 'better-sqlite3';
import path from 'node:path';
import {
  fixture,
  installReady,
  keys,
  NOW,
  meta,
  databaseState,
  image,
} from '../helpers/relational-generation.mjs';
import { createRelationalManagementHttp } from '../../server/members/relational-management-http.ts';
import { createRelationalMemberAuthenticationHttp } from '../../server/members/relational-authentication-http.ts';
import { assertAdminToken, matchesAdminToken } from '../../server/http/admin-access.ts';
import { publicErrorMessage } from '../../server/http/public-errors.ts';
import {
  createMemberSession,
  authenticateMemberApiKey,
  authenticateMemberSession,
  logoutMemberSession,
  withAuthenticatedMemberApiKey,
  prepareMemberCredentials,
} from '../../server/members/authentication.ts';
import { readServiceResources } from '../../server/reports/service-resources.ts';
import { readActivityHeatmap, activityDate } from '../../server/reports/activity-heatmap.ts';
import { RelationalReportStore } from '../../server/storage/relational-report-store.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import {
  relationalSnapshotSha256,
  relationalPrivateStateSha256,
  relationalPrivateStateEmpty,
} from '../../server/storage/relational-private-restore.ts';
import { domainDigest } from '../../server/events/domain-events.ts';
import { createGenerationControls } from '../../server/generation/queue-dispatch.ts';
import { accountQuotaResult, createAccountQuotaQuery } from '../../server/quota/account-refresh.ts';
import { createRelationalAccountQuotaRefresh } from '../../server/quota/relational-account-refresh.ts';
import { fetchNovelAiAccountQuota } from '../../server/protocols/novelai-client.ts';
import { relationalStateNotificationsFor } from '../../server/events/relational-state-notifications.ts';
import {
  setup as setupNative,
  input as nativeInput,
  waitFor as waitNative,
} from '../helpers/relational-api-http.mjs';

const admin = '/api/admin/member-management',
  owner = '/api/member/owner/member-management',
  summary = '/api/member/owner/summary',
  statistics = '/api/member/owner/statistics',
  adminToken = 'synthetic-private-admin-token';
async function setup(
  t,
  {
    env = {},
    beforeReady,
    resources,
    discordFetch,
    onBodyRead,
    queryAccountQuota,
    quotaStore,
  } = {},
) {
  const currentEnv = {
      OWNER_USER_ID: 'm1',
      DISCORD_GUILD_ID: '1134557553011998840',
      DISCORD_CLIENT_ID: '123456789012345678',
      DISCORD_CLIENT_SECRET: 'synthetic-management-discord-secret',
      PUBLIC_BASE_URL: 'https://management.example',
      ...env,
    },
    cleanup = [],
    f = await fixture(
      { after: (fn) => cleanup.push(fn) },
      { env: currentEnv, seconds: 0, secondV5: '60' },
    ),
    sessions = {};
  let clock = NOW + 30000;
  t.mock.method(Date, 'now', () => clock);
  f.accept('existing');
  f.dispatch('existing-0', NOW);
  await f.publish('existing-0');
  for (const id of ['m1', 'm2']) {
    if (currentEnv.DISCORD_GUILD_ID) {
      await f.members.discordAuthenticationTransaction((tx) => {
        const member = tx.readMember(id);
        member.discord = {
          id: id === 'm1' ? '222222222222222222' : '333333333333333333',
          guildId: currentEnv.DISCORD_GUILD_ID,
          accessToken: `synthetic-private-access-${id}`,
          refreshToken: `synthetic-private-refresh-${id}`,
          expiresAt: NOW + 86400000,
          checkedAt: NOW,
          member: true,
        };
        tx.saveAuthentication(member, 'login');
      });
    }
    sessions[id] = await createMemberSession(
      f.members,
      await authenticateMemberApiKey(f.members, keys[id]),
      currentEnv,
      NOW,
    );
  }
  await beforeReady?.(f, currentEnv);
  const selected = await installReady(f),
    events = new EventEmitter(),
    controls = new Map();
  let activeHandle,
    adminChecks = 0,
    resourceCalls = 0;
  const handle = createRelationalManagementHttp({
      sqlite: f.db,
      events,
      controls,
      dataDir: f.dir,
      env: currentEnv,
      discordFetch,
      queryAccountQuota,
      quotas: quotaStore?.(f.db, currentEnv),
      now: () => clock,
      assertAdmin(req, url) {
        adminChecks++;
        assertAdminToken(req, url, adminToken, currentEnv);
      },
      readServiceResources: async (dir) => {
        resourceCalls++;
        assert.equal(f.db.inTransaction, false);
        return resources ? resources(f, dir, currentEnv) : readServiceResources(dir);
      },
    }),
    server = createServer(async (req, res) => {
      if (onBodyRead) {
        const iterate = req[Symbol.asyncIterator].bind(req);
        req[Symbol.asyncIterator] = async function* () {
          for await (const chunk of iterate()) {
            onBodyRead();
            yield chunk;
          }
        };
      }
      try {
        if (await activeHandle(req, res, new URL(req.url, `http://${req.headers.host}`))) return;
        res.writeHead(404);
        res.end();
      } catch (error) {
        res.writeHead(error.statusCode || 500, {
          'content-type': 'application/json',
          'cache-control': 'no-store',
        });
        res.end(JSON.stringify({ error: publicErrorMessage(error.message) }));
      }
    });
  activeHandle = handle;
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const origin = `http://127.0.0.1:${server.address().port}`;
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    for (const close of cleanup.reverse()) await close();
  });
  const call = (
    url = owner,
    { member = 'm1', cookie = sessions[member].token, headers = {}, method = 'GET', body } = {},
  ) =>
    new Promise((resolve, reject) => {
      // raw HTTP 保留实际 Host/Origin；fetch 可能重写 Host，不能用它证明该门禁。
      const req = httpRequest(
        origin + url,
        {
          method,
          headers: {
            cookie: `nai_member_session=${cookie}`,
            origin,
            ...(body === undefined ? {} : { 'content-type': 'application/json' }),
            ...headers,
          },
        },
        (res) => {
          let text = '';
          res.setEncoding('utf8');
          res.on('data', (chunk) => (text += chunk));
          res.on('end', () =>
            resolve({
              status: res.statusCode,
              headers: new Headers(
                Object.entries(res.headers)
                  .filter(([, v]) => v !== undefined)
                  .map(([k, v]) => [k, String(v)]),
              ),
              text,
              body: text ? JSON.parse(text) : null,
            }),
          );
        },
      );
      req.on('error', reject);
      req.end(body === undefined ? undefined : JSON.stringify(body));
    });
  return {
    ...f,
    env: currentEnv,
    sessions,
    selected,
    events,
    controls,
    handle,
    setHandler(next) {
      activeHandle = next;
    },
    call,
    origin,
    advance(ms = 60000) {
      clock += ms;
    },
    get clock() {
      return clock;
    },
    get adminChecks() {
      return adminChecks;
    },
    get resourceCalls() {
      return resourceCalls;
    },
  };
}

test('真实ready管理GET复用安全目录、金融/共享与真实FS，disabled可管理且无秘密/账务写入', async (t) => {
  const f = await setup(t, {
      beforeReady: async (f) => {
        f.accept('disabled-history', { memberId: 'm2' });
        f.fail('disabled-history-0', { aborted: false });
        f.db.transaction(() => f.members.setMemberStatus(meta('disable-m2'), 'm2', 'disabled'))();
      },
    }),
    before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() AS n').get().n,
    managed = await f.call(admin, { headers: { 'x-admin-token': adminToken } });
  assert.equal(managed.status, 200, managed.text);
  const value = managed.body;
  assert.deepEqual(
    value.users.map((u) => u.id),
    ['m1', 'm2'],
  );
  assert.equal(value.users[0].username, null);
  assert.equal(value.users[0].displayName, 'm1');
  assert.equal(value.users[0].note, null);
  assert.equal(value.users[1].enabled, false);
  assert.equal(value.users[1].quota.v5Available, 60);
  assert.deepEqual(value.users[1].recentActivity, {
    total: 1,
    done: 0,
    failed: 1,
    queued: 0,
    running: 0,
  });
  assert.equal(value.publicBaseUrl, 'https://management.example');
  assert.equal(value.configVersion, 1);
  assert.ok(value.serviceResources.data.bytes > 0);
  assert.ok(value.serviceResources.disk.totalBytes > 0);
  assert.ok(value.serviceResources.memory.rssBytes > 0);
  assert.equal(Object.hasOwn(value.serviceResources, 'r2'), false);
  assert.equal(Object.hasOwn(value.serviceResources, 'pikpak'), false);
  const ownerView = await f.call();
  assert.equal(ownerView.status, 200, ownerView.text);
  for (const field of ['groups', 'status', 'shared', 'configVersion', 'freeImagePolicy', 'users'])
    assert.deepEqual(ownerView.body[field], value[field]);
  assert.equal(f.adminChecks, 2);
  for (const secret of [
    keys.m1,
    keys.m2,
    'synthetic-upstream-pst',
    'synthetic-private-access-m1',
    'synthetic-private-refresh-m1',
    f.sessions.m1.token,
    adminToken,
  ])
    assert.ok(!managed.text.includes(secret));
  for (const field of [
    'authEpoch',
    'keyHash',
    'recoverableKey',
    'passwordHash',
    'salt',
    'tokenHash',
    'credentialRef',
    'pst',
  ])
    assert.ok(!managed.text.includes('"' + field + '"'));
  assert.equal(managed.headers.get('set-cookie'), null);
  assert.equal(ownerView.headers.get('set-cookie'), null);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
});

test('管理GET逐次校验admin私有Host/token与owner Cookie，PUBLIC query不授予admin', async (t) => {
  const f = await setup(t),
    before = databaseState(f.db);
  assert.equal((await f.call(owner, { member: 'm2' })).status, 403);
  assert.equal((await f.call(owner, { cookie: '' })).status, 401);
  assert.equal((await f.call(admin)).status, 403);
  assert.equal((await f.call(admin, { headers: { 'x-admin-token': 'wrong' } })).status, 401);
  assert.equal(
    (
      await f.call(admin, {
        headers: {
          'x-admin-token': adminToken,
          host: 'public.invalid',
          origin: 'https://public.invalid',
        },
      })
    ).status,
    404,
  );
  assert.equal((await f.call(owner, { headers: { origin: 'https://cross.invalid' } })).status, 404);
  assert.equal((await f.call(admin + '?adminToken=' + adminToken)).status, 200);
  f.env.PUBLIC_MODE = 'true';
  assert.equal((await f.call(admin + '?adminToken=' + adminToken)).status, 401);
  assert.equal((await f.call(admin, { headers: { 'x-admin-token': adminToken } })).status, 200);
  f.env.PUBLIC_HOST = 'management.example';
  const publicHeaders = { host: 'management.example', origin: 'https://management.example' };
  assert.equal((await f.call(owner, { headers: publicHeaders })).status, 200);
  assert.equal(
    (await f.call(admin, { headers: { ...publicHeaders, 'x-admin-token': adminToken } })).status,
    404,
  );
  assert.equal(databaseState(f.db), before);
});

test('可见且精确无query owner管理摘要沿原owner续期，其余GET不续', async (t) => {
  const f = await setup(t);
  f.advance();
  const original = f.db
      .prepare("SELECT expires_at FROM member_sessions WHERE member_id='m1'")
      .get().expires_at,
    regular = await f.call(owner, { headers: { 'x-member-activity': '1' } });
  assert.equal(regular.status, 200, regular.text);
  assert.ok(regular.headers.get('set-cookie'));
  const renewed = f.db
    .prepare("SELECT expires_at FROM member_sessions WHERE member_id='m1'")
    .get().expires_at;
  assert.ok(renewed > original);
  f.advance();
  for (const url of [
    owner,
    owner + '?x=1',
    owner + '?view=heatmap',
    owner + '?view=losses',
    admin,
    summary,
    statistics,
  ]) {
    const headers = {
        'x-admin-token': adminToken,
        ...(url === owner ? {} : { 'x-member-activity': '1' }),
      },
      result = await f.call(url, { headers });
    assert.equal(result.status, 200, result.text);
    assert.equal(result.headers.get('set-cookie'), null);
    assert.equal(
      f.db.prepare("SELECT expires_at FROM member_sessions WHERE member_id='m1'").get().expires_at,
      renewed,
    );
  }
});

test('owner summary保持原安全形态、官方未知与真实物理槽，不续期或读取资源', async (t) => {
  const f = await setup(t),
    running = createGenerationControls({
      controls: f.controls,
      timeoutDelay: () => 60000,
      schedule() {},
    }),
    control = running.createRunningJobControl({ id: 'physical-after-terminal', accountId: 'a' }),
    other = running.createRunningJobControl({ id: 'other-account', accountId: 'other' });
  t.after(() => {
    running.finishRunningJobControl('physical-after-terminal', control);
    running.finishRunningJobControl('other-account', other);
  });
  const before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() AS n').get().n,
    result = await f.call(summary + '?revealTokens=1&view=heatmap', {
      headers: { 'x-member-activity': '1' },
    });
  assert.equal(result.status, 200, result.text);
  assert.deepEqual(result.body, {
    accounts: [
      {
        id: 'a',
        enabled: true,
        inFlight: 1,
        quotaTier: 3,
        quotaFixed: 8000,
        quotaPurchased: 0,
        v5UsagePercent: null,
        quotaCheckedAt: new Date(NOW).toISOString(),
        quotaError: '',
      },
    ],
  });
  assert.equal(result.headers.get('set-cookie'), null);
  assert.equal(f.resourceCalls, 0);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
  running.finishRunningJobControl('physical-after-terminal', control);
  assert.equal((await f.call(summary)).body.accounts[0].inFlight, 0);
  f.db.pragma('query_only=ON');
  const actual = new RelationalQuotaStore(f.db, f.env).readSharedStatusSource();
  assert.equal(actual.accounts.length, 1);
  f.db.pragma('query_only=OFF');
  // 控制窄来源的候选集合，仅检适配器不猜选；不冒充一次持久配置变更。
  let selectedSource = actual;
  t.mock.method(RelationalQuotaStore.prototype, 'readSharedStatusSource', () => selectedSource);
  for (const [boundAccountId, accounts, expected] of [
    [null, [...actual.accounts, { ...actual.accounts[0], id: 'b' }], []],
    ['missing-account', actual.accounts, []],
    [null, actual.accounts, ['a']],
  ]) {
    selectedSource = { ...actual, boundAccountId, accounts };
    const value = await f.call(summary);
    assert.equal(value.status, 200, value.text);
    assert.deepEqual(
      value.body.accounts.map((account) => account.id),
      expected,
    );
  }
  assert.equal(databaseState(f.db), before);
});

test('owner statistics沿原UTC+8范围及days数组，登录名不借展示名且读取零写', async (t) => {
  const f = await setup(t, {
      beforeReady: async (f) => {
        const prepared = await f.members.prepareMemberCredentials('m2', {
          username: 'actual.login',
          password: 'synthetic-owner-statistics-password',
        });
        f.db.transaction(() => f.members.setMemberCredentials(prepared, null))();
        f.accept('named-history', { memberId: 'm2' });
        f.fail('named-history-0', { aborted: false });
      },
    }),
    before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() AS n').get().n;
  const prepare = f.db.prepare.bind(f.db),
    queries = [];
  t.mock.method(f.db, 'prepare', (sql) => {
    queries.push(sql);
    return prepare(sql);
  });
  for (const days of [0, 1, 7, 30, 365]) {
    const result = await f.call(statistics + '?days=' + days, {
      headers: { 'x-member-activity': '1' },
    });
    assert.equal(result.status, 200, result.text);
    assert.equal(result.body.timezone, 'Asia/Shanghai');
    const start =
      Math.floor((f.clock + 8 * 3600000) / 86400000) * 86400000 -
      8 * 3600000 -
      (days - 1) * 86400000;
    assert.equal(result.body.since, days === 0 ? '' : new Date(start).toISOString());
    assert.equal(result.body.until, new Date(f.clock).toISOString());
    assert.ok(Array.isArray(result.body.days));
    assert.equal(result.body.coverage, 'retained-records');
    assert.equal(result.body.members.find((row) => row.id === 'm2').label, 'actual.login');
    assert.equal(result.body.members.find((row) => row.id === 'm1').label, '已移除成员');
    assert.equal(result.headers.get('set-cookie'), null);
  }
  for (const days of ['2', '-1', 'Infinity', 'NaN', '366']) {
    assert.equal((await f.call(statistics + '?days=' + days)).status, 400);
  }
  assert.equal((await f.call(statistics + '?days=01')).status, 200);
  assert.equal((await f.call(statistics + '?view=losses')).status, 200);
  assert.equal(f.resourceCalls, 0);
  f.db.pragma('query_only=ON');
  assert.equal(
    (await new RelationalReportStore(f.db).readOwnerJobRows('', new Date(f.clock).toISOString()))
      .length,
    2,
  );
  f.db.pragma('query_only=OFF');
  const usageQueries = queries.filter((sql) =>
    sql.includes('FROM generation_jobs j JOIN generation_requests r'),
  );
  assert.ok(usageQueries.length >= 5);
  assert.ok(usageQueries.every((sql) => /p\.username/.test(sql)));
  assert.ok(
    usageQueries.every(
      (sql) =>
        !/generation_inputs|input_assets|normalization_contexts|\.body\b|\.data\b|password_hash|salt|\bpst\b|\btoken\b/i.test(
          sql,
        ),
    ),
  );
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
});

test('owner summary真实官方0不补未知值，当前凭据错误只返回原安全提示', async (t) => {
  const f = await setup(t),
    quotas = new RelationalQuotaStore(f.db, f.env),
    ticket = quotas.beginQuotaObservation('a', f.clock),
    result = accountQuotaResult(
      'a',
      {
        tier: 3,
        subscriptionActive: true,
        fixed: 0,
        purchased: 0,
        v5UsagePercent: 0,
        v5UsageIsNegative: false,
        v5UsageTimeUntilNextPercent: 0,
      },
      new Date(f.clock).toISOString(),
    );
  f.db.transaction(() =>
    quotas.recordQuotaObservation(meta('owner-summary-zero', f.clock), ticket, result),
  )();
  const before = databaseState(f.db),
    response = await f.call(summary);
  assert.equal(response.status, 200, response.text);
  assert.equal(response.body.accounts[0].v5UsagePercent, 0);
  assert.equal(response.body.accounts[0].quotaFixed, 0);
  assert.equal(response.body.accounts[0].quotaPurchased, 0);
  assert.equal(databaseState(f.db), before);
  // 模拟选定私有内容缺失；查询不能据公共余额伪称凭据有效或泄出原错误。
  f.db.prepare("DELETE FROM upstream_credentials WHERE account_id='a'").run();
  const missing = await f.call(summary + '?revealTokens=1');
  assert.equal(missing.status, 200, missing.text);
  assert.equal(missing.body.accounts[0].quotaError, '官方额度同步失败，请重新验证凭据');
  assert.ok(!missing.text.includes('credential_unavailable'));
  assert.ok(!missing.text.includes('synthetic-upstream-pst'));
});

test('新增owner GET沿同一实际Cookie/owner/同源门禁，POST和admin别名不开放', async (t) => {
  const f = await setup(t),
    before = databaseState(f.db);
  for (const route of [summary, statistics]) {
    assert.equal((await f.call(route, { member: 'm2' })).status, 403);
    assert.equal((await f.call(route, { cookie: '' })).status, 401);
    assert.equal(
      (await f.call(route, { headers: { origin: 'https://cross.invalid' } })).status,
      404,
    );
    assert.equal((await f.call(route, { method: 'POST' })).status, 404);
    assert.equal(
      (
        await f.call(route.replace('/member/owner', '/admin'), {
          headers: { 'x-admin-token': adminToken },
        })
      ).status,
      404,
    );
  }
  assert.equal(databaseState(f.db), before);
});

test('新增owner GET在真实Discord IO后重核owner、epoch和ready，不返回旧摘要', async (t) => {
  for (const route of [summary, statistics]) {
    for (const kind of ['owner', 'epoch', 'ready']) {
      await t.test(route + ':' + kind, async (t) => {
        let f,
          calls = 0;
        f = await setup(t, {
          discordFetch: async () => {
            calls++;
            assert.equal(f.db.inTransaction, false);
            if (kind === 'owner') f.env.OWNER_USER_ID = 'm2';
            if (kind === 'epoch') {
              const prepared = await f.members.prepareMemberCredentials('m1', {
                username: 'rotated.owner',
                password: 'synthetic-rotated-owner-password',
              });
              f.db.transaction(() => f.members.setMemberCredentials(prepared, null))();
            }
            if (kind === 'ready') f.db.prepare("UPDATE projection_state SET status='failed'").run();
            return Response.json({ user: { id: '222222222222222222' } });
          },
        });
        f.advance(6 * 60000);
        const result = await f.call(route);
        assert.equal(calls, 1);
        assert.equal(result.status, kind === 'ready' ? 500 : 401, result.text);
        assert.equal(Object.hasOwn(result.body, 'accounts'), false);
        assert.equal(Object.hasOwn(result.body, 'totals'), false);
        assert.equal(f.resourceCalls, 0);
      });
    }
  }
});

test('statistics异步窄读取后仍核当前授权，IO不占SQL事务', async (t) => {
  for (const kind of ['owner', 'cookie', 'ready']) {
    await t.test(kind, async (t) => {
      const f = await setup(t),
        original = RelationalReportStore.prototype.readOwnerJobRows;
      t.mock.method(RelationalReportStore.prototype, 'readOwnerJobRows', async function (...args) {
        const rows = await original.apply(this, args);
        assert.equal(f.db.inTransaction, false);
        if (kind === 'owner') f.env.OWNER_USER_ID = 'm2';
        if (kind === 'cookie') await logoutMemberSession(f.members, f.sessions.m1.token);
        if (kind === 'ready') f.db.prepare("UPDATE projection_state SET status='failed'").run();
        return rows;
      });
      const result = await f.call(statistics);
      assert.equal(result.status, kind === 'ready' ? 500 : 401, result.text);
      assert.equal(Object.hasOwn(result.body, 'recent'), false);
    });
  }
});

test('真实资源FS等待后撤Cookie/owner/ready仍拒绝管理metadata，不保存旧权限', async (t) => {
  for (const kind of ['cookie', 'owner', 'ready']) {
    await t.test(kind, async (t) => {
      let entered, release;
      const started = new Promise((resolve) => (entered = resolve)),
        gate = new Promise((resolve) => (release = resolve)),
        f = await setup(t, {
          resources: async (f, dir) => {
            const value = await readServiceResources(dir);
            entered();
            await gate;
            return value;
          },
        }),
        pending = f.call();
      t.after(() => release());
      await Promise.race([
        started,
        pending.then((result) => {
          throw new Error('Request returned before resource wait: ' + result.text);
        }),
      ]);
      if (kind === 'cookie') await logoutMemberSession(f.members, f.sessions.m1.token);
      if (kind === 'owner') f.env.OWNER_USER_ID = 'm2';
      if (kind === 'ready') f.db.prepare("UPDATE projection_state SET status='failed'").run();
      release();
      const result = await pending;
      assert.equal(result.status, kind === 'ready' ? 500 : 401, result.text);
      assert.equal(Object.hasOwn(result.body, 'users'), false);
      assert.equal(Object.hasOwn(result.body, 'serviceResources'), false);
    });
  }
});

test('heatmap及losses复用原SQL/参数owner，无FS查询且非法游标/重复operation拒绝', async (t) => {
  const f = await setup(t),
    reports = new RelationalReportStore(f.db),
    date = activityDate(new Date(NOW).toISOString()),
    query = 'view=heatmap&memberId=m1&dates=' + date,
    before = databaseState(f.db),
    heatmap = await f.call(owner + '?' + query);
  assert.equal(heatmap.status, 200, heatmap.text);
  assert.deepEqual(
    heatmap.body,
    await readActivityHeatmap(
      reports,
      new URLSearchParams(query),
      { memberId: null, allowMemberFilter: true },
      f.clock,
    ),
  );
  assert.equal(heatmap.body.days[0].total, 1);
  assert.equal(
    (await f.call(owner + '?view=heatmap&operation=generate&operation=upscale')).status,
    400,
  );
  const losses = await f.call(admin + '?view=losses', { headers: { 'x-admin-token': adminToken } });
  assert.equal(losses.status, 200, losses.text);
  assert.deepEqual(losses.body, reports.readFailureAccounting(null, f.clock));
  assert.equal((await f.call(owner + '?view=losses&before=invalid')).status, 400);
  assert.equal(f.resourceCalls, 0);
  assert.equal(databaseState(f.db), before);
});

test('admin token纯owner保留原PUBLIC/空值/精确比较与401/403语义', () => {
  const request = (header) => ({ headers: header }),
    url = new URL('http://localhost/?adminToken=' + adminToken);
  assert.equal(matchesAdminToken(request({}), url, adminToken, {}), true);
  assert.equal(matchesAdminToken(request({}), url, adminToken, { PUBLIC_MODE: 'true' }), false);
  assert.equal(matchesAdminToken(request({ 'x-admin-token': 'wrong' }), url, adminToken, {}), true);
  assert.equal(
    matchesAdminToken(request({ 'x-admin-token': adminToken }), url, adminToken, {
      PUBLIC_MODE: 'true',
    }),
    true,
  );
  assert.equal(
    matchesAdminToken(
      request({ 'x-admin-token': ' ' + adminToken }),
      new URL('http://localhost/'),
      adminToken,
      {},
    ),
    false,
  );
  assert.throws(() => assertAdminToken(request({}), new URL('http://localhost/'), adminToken, {}), {
    statusCode: 403,
  });
  assert.throws(() => assertAdminToken(request({}), url, adminToken, { PUBLIC_MODE: 'true' }), {
    statusCode: 401,
  });
});

test('真实held管理读取保留预扣与钱包，缺基线故障拒503而非空额度', async (t) => {
  const f = await setup(t, {
      beforeReady: (f) => {
        f.accept('held');
        f.dispatch('held-0', NOW + 20000);
      },
    }),
    before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() AS n').get().n,
    response = await f.call(admin, { headers: { 'x-admin-token': adminToken } });
  assert.equal(response.status, 200, response.text);
  assert.equal(response.body.shared.running, 1);
  const expected = new RelationalQuotaStore(f.db, f.env).readMemberQuotaSummary('m1', f.clock),
    { freeImages, ...actual } = response.body.users.find((u) => u.id === 'm1').quota;
  assert.deepEqual(actual, expected);
  const charge = f.db
    .prepare(
      "SELECT v5,anlas_fixed,anlas_purchased,status FROM charges WHERE original_job_id='held-0'",
    )
    .get();
  assert.equal(charge.status, 'reserved');
  assert.equal(actual.v5Reserved, Number(charge.v5));
  assert.equal(actual.anlasFixedReserved, Number(charge.anlas_fixed));
  assert.equal(actual.anlasPurchasedReserved, Number(charge.anlas_purchased));
  assert.ok(Number(charge.v5) + Number(charge.anlas_fixed) + Number(charge.anlas_purchased) > 0);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
  // 已合法安装后的原来源故障，不冒充这种源已通过ready。
  f.db.prepare("UPDATE account_quota_state SET baseline_event_id=NULL WHERE account_id='a'").run();
  const broken = databaseState(f.db),
    failed = await f.call(admin, { headers: { 'x-admin-token': adminToken } });
  assert.equal(failed.status, 503, failed.text);
  assert.equal(Object.hasOwn(failed.body, 'users'), false);
  assert.equal(databaseState(f.db), broken);
});

test('资源IO后才读当前管理metadata，admin也重新调用原私有capability', async (t) => {
  const f = await setup(t, {
      resources: async (f, dir) => {
        const resources = await readServiceResources(dir);
        if (!f.members.readManagementMemberProfile('added-during-io'))
          f.db.transaction(() =>
            f.members.registerMember(meta('io-register'), {
              memberId: 'added-during-io',
              displayName: 'actual fresh metadata',
              apiKey: 'synthetic-added-key',
            }),
          )();
        return resources;
      },
    }),
    result = await f.call(admin, { headers: { 'x-admin-token': adminToken } });
  assert.equal(result.status, 200, result.text);
  assert.equal(
    result.body.users.find((u) => u.id === 'added-during-io').displayName,
    'actual fresh metadata',
  );
  assert.equal(f.adminChecks, 2);
});

test('admin资源等待后按当前PUBLIC规则重核query能力，不缓存先前授权', async (t) => {
  const f = await setup(t, {
      resources: async (f, dir, env) => {
        const data = await readServiceResources(dir);
        env.PUBLIC_MODE = 'true';
        return data;
      },
    }),
    before = databaseState(f.db),
    response = await f.call(admin + '?adminToken=' + adminToken);
  assert.equal(response.status, 401, response.text);
  assert.equal(f.adminChecks, 2);
  assert.equal(Object.hasOwn(response.body, 'users'), false);
  assert.equal(databaseState(f.db), before);
});

test('Discord资格IO后重新验证当前Cookie，授权撤销不因真实观察写入而复活', async (t) => {
  let f,
    calls = 0;
  f = await setup(t, {
    discordFetch: async () => {
      calls++;
      assert.equal(f.db.inTransaction, false);
      await logoutMemberSession(f.members, f.sessions.m1.token);
      return Response.json({ user: { id: '222222222222222222' } });
    },
  });
  f.advance(6 * 60000);
  const financial = () =>
      JSON.stringify(
        ['wallets', 'charges', 'generation_jobs', 'generation_requests', 'account_quota_state'].map(
          (table) => f.db.prepare('SELECT * FROM ' + table + ' ORDER BY rowid').all(),
        ),
      ),
    before = financial(),
    result = await f.call();
  assert.equal(calls, 1);
  assert.equal(result.status, 401, result.text);
  assert.equal(f.resourceCalls, 0);
  assert.equal(Object.hasOwn(result.body, 'users'), false);
  assert.equal(financial(), before);
  assert.equal(
    f.db.prepare("SELECT checked_at FROM discord_bindings WHERE member_id='m1'").get().checked_at,
    f.clock,
  );
});

test('losses真实51笔原政策审计走50条cursor分页，同source视图不混页', async (t) => {
  const f = await setup(t, {
      beforeReady: async (f, env) => {
        const quota = new RelationalQuotaStore(f.db, env);
        await withAuthenticatedMemberApiKey(
          f.members,
          keys.m1,
          (actor) => {
            for (let i = 1; i <= 51; i++)
              quota.configureQuotaPolicy(
                meta('loss-policy-' + i, NOW + 100 + i, actor.id),
                {
                  action: 'loss-alert-policy',
                  expectedVersion: i,
                  policy: {
                    v5Points24h: i,
                    fixedAnlas24h: i,
                    fixedAnlas30d: i + 1,
                    memberAttempts24h: 1,
                  },
                },
                actor,
              );
          },
          env,
        );
      },
    }),
    before = databaseState(f.db),
    first = await f.call(owner + '?view=losses');
  assert.equal(first.status, 200, first.text);
  assert.equal(first.body.events.length, 50);
  assert.ok(first.body.nextCursor);
  const second = await f.call(
    admin + '?view=losses&before=' + encodeURIComponent(first.body.nextCursor),
    { headers: { 'x-admin-token': adminToken } },
  );
  assert.equal(second.status, 200, second.text);
  assert.ok(second.body.events.length > 0);
  const ids = new Set(first.body.events.map((e) => e.id));
  assert.ok(second.body.events.every((e) => !ids.has(e.id)));
  assert.equal(first.body.policy.version, 52);
  assert.deepEqual(second.body.policy, first.body.policy);
  assert.equal(databaseState(f.db), before);
});

test('管理当前private metadata只随选定一致性快照恢复，公共回放不复活登录/Key', async (t) => {
  let target, reopened;
  t.after(() => {
    if (target?.open) target.close();
    if (reopened?.open) reopened.close();
  });
  const credentials = await prepareMemberCredentials({
      username: 'named.actual',
      password: 'synthetic-management-password',
    }),
    f = await setup(t, {
      beforeReady: async (f) => {
        f.db.transaction(() =>
          f.members.registerMember(meta('named-member'), {
            memberId: 'named',
            displayName: 'm1',
            apiKey: 'synthetic-named-key',
            credentials,
          }),
        )();
        const prepared = await f.members.prepareMemberCredentials('m2', {
          username: 'stats.actual',
          password: 'synthetic-restored-statistics-password',
        });
        f.db.transaction(() => f.members.setMemberCredentials(prepared, null))();
        f.accept('named-stats', { memberId: 'm2' });
        f.fail('named-stats-0', { aborted: false });
      },
    }),
    targetPath = path.join(f.dir, 'management-restored.sqlite');
  target = new Database(targetPath);
  initializeRelationalStorage(target, 'generation-history');
  const replay = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replay.replayCommit(readRelationalCommit(f.db, seq), inputs))();
  }
  const members = new RelationalMemberStore(target, f.env),
    beforePrivate = members.readManagementMemberProfile('named');
  assert.equal(beforePrivate.username, null);
  assert.equal(beforePrivate.displayName, 'm1');
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
  assert.equal(
    (
      await new RelationalReportStore(target).readOwnerJobRows('', new Date(f.clock).toISOString())
    ).find((row) => row.memberId === 'm2').username,
    '已移除成员',
  );
  const deps = {
    sqlite: target,
    events: new EventEmitter(),
    controls: f.controls,
    dataDir: f.dir,
    env: f.env,
    now: () => f.clock,
    assertAdmin: (req, url) => assertAdminToken(req, url, adminToken, f.env),
  };
  assert.throws(() => createRelationalManagementHttp(deps), /incomplete/);
  await verifyAndInstallRelationalStorage(target, f.selected);
  f.setHandler(createRelationalManagementHttp(deps));
  const before = databaseState(target),
    view = await f.call();
  assert.equal(view.status, 200, view.text);
  const named = view.body.users.find((u) => u.id === 'named'),
    original = view.body.users.find((u) => u.id === 'm1');
  assert.equal(named.username, 'named.actual');
  assert.equal(named.displayName, 'm1');
  assert.equal(original.username, null);
  const statisticsView = await f.call(statistics + '?days=0'),
    summaryView = await f.call(summary);
  assert.equal(statisticsView.status, 200, statisticsView.text);
  assert.equal(statisticsView.body.members.find((row) => row.id === 'm2').label, 'stats.actual');
  assert.deepEqual(summaryView.body, (await f.call(summary)).body);
  for (const secret of [
    'synthetic-management-password',
    'synthetic-named-key',
    credentials.hash,
    credentials.salt,
    f.sessions.m1.token,
    'synthetic-upstream-pst',
  ])
    assert.ok(
      ![view.text, statisticsView.text, summaryView.text].some((text) => text.includes(secret)),
    );
  assert.equal(databaseState(target), before);
  target.close();
  reopened = new Database(targetPath);
  f.setHandler(createRelationalManagementHttp({ ...deps, sqlite: reopened }));
  assert.deepEqual((await f.call(statistics + '?days=0')).body, statisticsView.body);
  assert.deepEqual((await f.call(summary)).body, summaryView.body);
  assert.equal(databaseState(reopened), before);
});

test('最近30条只用原Job时间，未知与真实0不借父受理时间，排除退休且隔离成员', async (t) => {
  const f = await setup(t, {
      beforeReady: async (f, env) => {
        await withAuthenticatedMemberApiKey(
          f.members,
          keys.m1,
          (actor) =>
            f.generations.retireJobHistory(
              meta('retire-prefix', NOW + 10, actor.id),
              ['existing-0'],
              actor,
              new Map(),
            ),
          env,
        );
        const original = f.db
            .prepare("SELECT * FROM generation_requests WHERE id='existing'")
            .get(),
          artifact = f.db.prepare('SELECT id FROM artifacts').get().id,
          events = [
            {
              eventType: 'OpeningRequestRecorded',
              streamId: 'request:management-history',
              payload: {
                requestId: 'management-history',
                memberId: 'm1',
                inputId: original.input_id,
                contextId: original.context_id,
                source: 'synthetic-history',
                operation: 'generate',
                model: original.model,
                sampleCount: 2,
                seedMode: 'unknown',
                confirmation: null,
                acceptedAt: NOW + 1000,
                retiredAt: null,
                origin: { sourceId: 'synthetic-opening', originalId: 'original-request-history' },
              },
            },
            ...['zero', 'unknown'].map((id, index) => ({
              eventType: 'OpeningJobRecorded',
              streamId: 'request:management-history',
              payload: {
                jobId: 'historical-' + id,
                requestId: 'management-history',
                createdAt: index === 0 ? 0 : null,
                sampleIndex: index,
                actualSeed: null,
                status: index === 0 ? 'failed' : 'done',
                queueSeq: null,
                queueExpiresAt: null,
                deadlineAt: null,
                admissionEpoch: 0,
                artifactId: index === 0 ? null : artifact,
                cacheKey: null,
                completedAt: 0,
                errorCode: index === 0 ? 'original_failure' : null,
                errorStatus: index === 0 ? 500 : null,
                cancellationReason: null,
                lateResultAt: null,
                freeSucceededAt: null,
                freeWarningThreshold: null,
                retiredAt: null,
                origin: { sourceId: 'synthetic-opening', originalId: 'original-' + id },
              },
            })),
          ],
          { policyVersion, pricingVersion, ...command } = meta(
            'management-original-history',
            NOW + 1000,
          );
        f.db.transaction(() =>
          new RelationalCommitReplayer(f.db).appendCommit(
            {
              ...command,
              fingerprint: domainDigest(events),
              receipt: { opened: true },
              expectedVersions: { 'request:management-history': '0' },
              events: events.map((e) => ({
                ...e,
                memberId: 'm1',
                accountId: null,
                occurredAt: NOW + 1000,
                policyVersion,
                pricingVersion,
              })),
            },
            f.inputs,
          ),
        )();
        for (let i = 0; i < 29; i++) {
          f.accept('recent-' + i, { now: NOW + 100 + i });
          f.fail('recent-' + i + '-0', { aborted: false }, NOW + 200 + i);
        }
        f.accept('other-member', { memberId: 'm2', now: NOW + 900 });
        f.fail('other-member-0', { aborted: false }, NOW + 901);
      },
    }),
    before = databaseState(f.db),
    view = await f.call();
  assert.equal(view.status, 200, view.text);
  assert.deepEqual(view.body.users.find((u) => u.id === 'm1').recentActivity, {
    total: 30,
    done: 0,
    failed: 30,
    queued: 0,
    running: 0,
  });
  assert.deepEqual(view.body.users.find((u) => u.id === 'm2').recentActivity, {
    total: 1,
    done: 0,
    failed: 1,
    queued: 0,
    running: 0,
  });
  assert.equal(
    f.db
      .prepare("SELECT created_at FROM generation_job_origins WHERE job_id='historical-zero'")
      .get().created_at,
    0,
  );
  assert.equal(
    f.db
      .prepare("SELECT created_at FROM generation_job_origins WHERE job_id='historical-unknown'")
      .get().created_at,
    null,
  );
  const all = await f.call(statistics + '?days=0'),
    today = await f.call(statistics + '?days=1');
  assert.equal(all.status, 200, all.text);
  assert.equal(all.body.totals.total, 32);
  assert.equal(all.body.totals.v5, null);
  assert.equal(all.body.totals.fixed, null);
  assert.equal(all.body.totals.purchased, null);
  const unknown = all.body.recent.find((row) => row.id === 'historical-unknown'),
    zero = all.body.recent.find((row) => row.id === 'historical-zero');
  assert.equal(unknown.createdAt, null);
  assert.equal(unknown.amount, null);
  assert.equal(unknown.chargeStatus, null);
  assert.equal(unknown.knownNoCharge, false);
  assert.equal(zero.createdAt, '1970-01-01T00:00:00.000Z');
  assert.equal(today.status, 200, today.text);
  assert.equal(today.body.totals.total, 30);
  assert.ok(today.body.recent.every((row) => !row.id.startsWith('historical-')));
  assert.ok(!all.body.recent.some((row) => row.id === 'existing-0'));
  assert.equal(databaseState(f.db), before);
});

test('实际已删除成员不再列为管理对象，仍保留原历史与转交权益', async (t) => {
  const f = await setup(t, {
      beforeReady: async (f, env) => {
        f.accept('removed-history', { memberId: 'm2' });
        f.fail('removed-history-0', { aborted: false });
        const quota = new RelationalQuotaStore(f.db, env);
        await withAuthenticatedMemberApiKey(
          f.members,
          keys.m1,
          (actor) =>
            quota.manageMemberQuota(
              meta('actual-remove', NOW + 100, actor.id),
              {
                action: 'delete-member',
                userId: 'm2',
                transferToUserId: 'm1',
                expectedConfigVersion: 1,
              },
              actor,
              new Map(),
            ),
          env,
        );
      },
    }),
    before = databaseState(f.db),
    view = await f.call();
  assert.equal(view.status, 200, view.text);
  assert.deepEqual(
    view.body.users.map((u) => u.id),
    ['m1'],
  );
  assert.equal(
    f.db
      .prepare(
        "SELECT COUNT(*) AS n FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id WHERE r.member_id='m2'",
      )
      .get().n,
    1,
  );
  assert.equal(view.body.configVersion, 2);
  assert.equal(databaseState(f.db), before);
});
const freePolicy = (expectedVersion = 1) => ({
  action: 'free-image-policy',
  expectedVersion,
  policy: { enabled: true, warningThreshold: 2, hardLimit: 5 },
});
const lossPolicy = (expectedVersion = 1) => ({
  action: 'loss-alert-policy',
  expectedVersion,
  policy: {
    v5Points24h: 0.1234567890123456,
    fixedAnlas24h: 0,
    fixedAnlas30d: 9,
    memberAttempts24h: 0,
  },
});
function policyNotifications(f) {
  const seen = [];
  f.events.on('commit', (event) => {
    assert.equal(f.db.inTransaction, false);
    seen.push({
      event,
      row: f.db.prepare('SELECT * FROM sharing_policy WHERE id=1').get(),
    });
  });
  return seen;
}

test('三政策真实POST沿当前admin或owner与原回执，重复零写且提交后恰一次通知', async (t) => {
  const f = await setup(t),
    seen = policyNotifications(f),
    financial = () =>
      domainDigest(
        [
          'wallets',
          'charges',
          'account_quota_state',
          'operating_state',
          'generation_requests',
          'generation_jobs',
          'generation_attempts',
          'idempotency_keys',
        ].map((table) => [table, f.db.prepare('SELECT * FROM ' + table + ' ORDER BY rowid').all()]),
      ),
    financialBefore = financial(),
    cases = [];
  for (const [actor, endpoint] of [
    ['admin', admin],
    ['member', owner],
  ]) {
    for (const [body, eventType, expected] of [
      [
        { action: 'fallback-policy', enabled: actor === 'admin' },
        'FallbackPolicyChanged',
        { autoAnlasFallback: actor === 'admin' },
      ],
      [
        freePolicy(actor === 'admin' ? 1 : 2),
        'FreePolicyChanged',
        { policy: { ...freePolicy().policy, version: actor === 'admin' ? 2 : 3 } },
      ],
      [
        lossPolicy(actor === 'admin' ? 1 : 2),
        'LossPolicyChanged',
        { policy: { ...lossPolicy().policy, version: actor === 'admin' ? 2 : 3 } },
      ],
    ]) {
      const headers = {
          'idempotency-key': 'same-key-scoped-by-actor-and-action',
          'x-member-activity': '1',
          ...(actor === 'admin' ? { 'x-admin-token': adminToken } : {}),
        },
        intent = { ...body, actorId: 'forged-body-actor', actorKind: 'system' },
        response = await f.call(endpoint, { method: 'POST', headers, body: intent });
      assert.equal(response.status, 200, response.text);
      assert.deepEqual(response.body, expected);
      assert.equal(response.headers.get('set-cookie'), null);
      const seq = f.db
          .prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits')
          .get().seq,
        stored = readRelationalCommit(f.db, seq);
      assert.equal(stored.command.actorKind, actor);
      assert.equal(stored.command.actorId, actor === 'admin' ? 'local-admin' : 'm1');
      assert.equal(stored.command.recordedAt, f.clock);
      assert.equal(stored.events.length, 1);
      assert.equal(stored.events[0].eventType, eventType);
      assert.deepEqual(stored.command.receipt, expected);
      assert.equal(seen.length, cases.length + 1);
      assert.deepEqual(seen.at(-1).event.collections, ['settings']);
      assert.equal(seen.at(-1).event.allMembers, true);
      cases.push({ endpoint, headers, intent, expected });
    }
  }
  const before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() AS n').get().n;
  f.advance();
  for (const { endpoint, headers, intent, expected } of cases) {
    const response = await f.call(endpoint, { method: 'POST', headers, body: intent });
    assert.equal(response.status, 200, response.text);
    assert.deepEqual(response.body, expected);
    assert.equal(response.headers.get('set-cookie'), null);
  }
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
  assert.equal(seen.length, 6);
  assert.equal(f.resourceCalls, 0);
  const reordered = await f.call(owner, {
    method: 'POST',
    headers: cases[4].headers,
    body: Object.fromEntries(Object.entries(cases[4].intent).reverse()),
  });
  assert.equal(reordered.status, 200, reordered.text);
  assert.deepEqual(reordered.body, cases[4].expected);
  assert.equal(databaseState(f.db), before);
  const conflict = await f.call(owner, {
    method: 'POST',
    headers: cases[3].headers,
    body: { ...cases[3].intent, enabled: true },
  });
  assert.equal(conflict.status, 409, conflict.text);
  const stale = await f.call(owner, {
    method: 'POST',
    headers: { 'idempotency-key': 'new-stale' },
    body: freePolicy(1),
  });
  assert.equal(stale.status, 409, stale.text);
  assert.equal(databaseState(f.db), before);
  assert.equal(seen.length, 6);
  const expiry = f.db.prepare('SELECT * FROM member_sessions ORDER BY token_hash').all();
  for (let i = 0; i < 2; i++) {
    const response = await f.call(owner, {
      method: 'POST',
      headers: { 'x-member-activity': '1' },
      body: { action: 'fallback-policy', enabled: true },
    });
    assert.equal(response.status, 200, response.text);
  }
  assert.equal(seen.length, 8);
  assert.deepEqual(f.db.prepare('SELECT * FROM member_sessions ORDER BY token_hash').all(), expiry);
  const ids = f.db
    .prepare(
      "SELECT command_id FROM commits WHERE command_id NOT LIKE 'management:%' ORDER BY commit_seq DESC LIMIT 2",
    )
    .all();
  assert.equal(new Set(ids.map((r) => r.command_id)).size, 2);
  assert.ok(ids.every((r) => /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/.test(r.command_id)));
  assert.equal(financial(), financialBefore);
});

test('政策POST保留原单ASCII幂等键、JSON、版本与本站边界，未接action不落库', async (t) => {
  const f = await setup(t),
    seen = policyNotifications(f),
    before = databaseState(f.db);
  for (const [headers, body, status] of [
    [{ 'idempotency-key': '' }, freePolicy(), 400],
    [{ 'idempotency-key': 'a'.repeat(129) }, freePolicy(), 400],
    [{ 'idempotency-key': 'has space' }, freePolicy(), 400],
    [{ 'idempotency-key': '\u00e9' }, freePolicy(), 400],
    [{ 'idempotency-key': ['duplicate', 'duplicate'] }, freePolicy(), 400],
    [{ 'content-type': 'text/plain' }, freePolicy(), 415],
    [{ origin: '' }, freePolicy(), 403],
    [{}, { action: 'unsupported-management-action' }, 400],
    [{}, { action: 'fallback-policy', enabled: 'true' }, 400],
    [{}, { ...freePolicy(), policy: { enabled: true, warningThreshold: 0, hardLimit: 1 } }, 400],
    [{}, { ...lossPolicy(), expectedVersion: 0 }, 409],
    [{}, { ...freePolicy(), extra: 'x'.repeat(32768) }, 413],
  ]) {
    const response = await f.call(owner, { method: 'POST', headers, body });
    assert.equal(response.status, status, response.text);
    assert.equal(response.headers.get('set-cookie'), null);
    assert.equal(databaseState(f.db), before);
  }
  const nonowner = await f.call(owner, { method: 'POST', member: 'm2', body: freePolicy() });
  assert.equal(nonowner.status, 403, nonowner.text);
  const missingAdmin = await f.call(admin, { method: 'POST', body: freePolicy() });
  assert.equal(missingAdmin.status, 403, missingAdmin.text);
  const wrongHost = await f.call(admin, {
    method: 'POST',
    headers: { host: 'untrusted.example', 'x-admin-token': adminToken },
    body: freePolicy(),
  });
  assert.equal(wrongHost.status, 404, wrongHost.text);
  for (const path of ['/api/member/owner/summary', '/api/member/owner/statistics']) {
    const unrelated = await f.call(path, { method: 'POST', body: freePolicy() });
    assert.equal(unrelated.status, 404, unrelated.text);
  }
  assert.equal(databaseState(f.db), before);
  assert.equal(seen.length, 0);
});
async function holdPolicyBody(
  t,
  f,
  endpoint,
  started,
  headers = {},
  body = freePolicy(),
  method = 'POST',
) {
  const text = JSON.stringify(body),
    request = httpRequest(f.origin + endpoint, {
      method,
      headers: {
        origin: f.origin,
        cookie: `nai_member_session=${f.sessions.m1.token}`,
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(text),
        'idempotency-key': 'held-policy',
        ...headers,
      },
    });
  t.after(() => request.destroy());
  const response = new Promise((resolve, reject) => {
    request.on('error', reject);
    request.on('response', (res) => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => (body += chunk));
      res.on('end', () => resolve({ status: res.statusCode, text: body, headers: res.headers }));
    });
  });
  request.write(text.slice(0, -1));
  await Promise.race([
    started,
    response.then((res) => {
      throw Error('POST returned before consuming body: ' + res.status);
    }),
    new Promise((_, reject) => {
      const timer = setTimeout(() => reject(Error('body not consumed')), 3000);
      timer.unref();
    }),
  ]);
  return {
    finish: () => {
      request.end(text.slice(-1));
      return response;
    },
  };
}

test('慢正文后政策POST重核当前Cookie、epoch ABA、owner、ready与admin边界', async (t) => {
  for (const mode of ['cookie', 'epoch-ABA', 'owner', 'ready', 'admin-query']) {
    await t.test(mode, async (t) => {
      let begin;
      const f = await setup(t, { onBodyRead: () => begin?.() }),
        seen = policyNotifications(f),
        endpoint = mode === 'admin-query' ? admin + '?adminToken=' + adminToken : owner;
      if (mode === 'cookie') {
        const first = await f.call(owner, {
          method: 'POST',
          headers: { 'idempotency-key': 'held-policy' },
          body: freePolicy(),
        });
        assert.equal(first.status, 200, first.text);
      }
      const policyNotified = seen.filter(({ event }) =>
          event.collections.includes('settings'),
        ).length,
        started = new Promise((resolve) => {
          begin = resolve;
        }),
        held = await holdPolicyBody(t, f, endpoint, started);
      if (mode === 'cookie') {
        await logoutMemberSession(f.members, f.sessions.m1.token);
      } else if (mode === 'epoch-ABA') {
        const epoch = f.db.prepare("SELECT auth_epoch FROM members WHERE id='m1'").get().auth_epoch;
        // 当前 active→disabled→active 角色回到原值，原 epoch/Cookie 仍必须撤销。
        f.db.transaction(() => f.members.setMemberStatus(meta('held-disable'), 'm1', 'disabled'))();
        f.db.transaction(() =>
          f.members.setMemberStatus(meta('held-reactivate'), 'm1', 'active'),
        )();
        assert.notEqual(
          f.db.prepare("SELECT auth_epoch FROM members WHERE id='m1'").get().auth_epoch,
          epoch,
        );
        assert.equal((await authenticateMemberApiKey(f.members, keys.m1)).id, 'm1');
      } else if (mode === 'owner') {
        f.env.OWNER_USER_ID = 'm2';
      } else if (mode === 'ready') {
        // 已合法安装后的故障，不把损坏 marker 当合法 ready 夹具。
        f.db.prepare("UPDATE projection_state SET status='failed' WHERE id=1").run();
      } else {
        f.env.PUBLIC_MODE = 'true';
      }
      // owner 配置协调本身可清理会话；只断言不得新增政策提交/通知或续 Cookie。
      const policy = f.db.prepare('SELECT * FROM sharing_policy').all(),
        cut = f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get().seq,
        result = await held.finish();
      assert.equal(result.status, mode === 'ready' ? 500 : 401, result.text);
      assert.equal(result.headers['set-cookie'], undefined);
      assert.deepEqual(f.db.prepare('SELECT * FROM sharing_policy').all(), policy);
      assert.equal(
        f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get().seq,
        cut,
      );
      assert.equal(
        seen.filter(({ event }) => event.collections.includes('settings')).length,
        policyNotified,
      );
    });
  }
});

test('政策POST末尾SQL故障全表回滚无通知，同键重试及完整公共回放不复活私有认证', async (t) => {
  const f = await setup(t),
    seen = policyNotifications(f),
    headers = { 'x-admin-token': adminToken, 'idempotency-key': 'rollback-policy' };
  f.db.exec(
    "CREATE TRIGGER fail_policy AFTER UPDATE OF free_policy_version ON sharing_policy BEGIN SELECT RAISE(ABORT,'synthetic policy tail failure'); END",
  );
  const before = databaseState(f.db),
    failed = await f.call(admin, { method: 'POST', headers, body: freePolicy() });
  assert.equal(failed.status, 500, failed.text);
  assert.equal(databaseState(f.db), before);
  assert.equal(seen.length, 0);
  f.db.exec('DROP TRIGGER fail_policy');
  const response = await f.call(admin, { method: 'POST', headers, body: freePolicy() });
  assert.equal(response.status, 200, response.text);
  assert.equal(seen.length, 1);
  assert.equal(seen[0].row.free_policy_version, 2);
  for (const body of [lossPolicy(), { action: 'fallback-policy', enabled: true }]) {
    assert.equal((await f.call(owner, { method: 'POST', body })).status, 200);
  }
  assert.equal(seen.length, 3);
  const target = new Database(':memory:');
  initializeRelationalStorage(target, 'generation-history');
  t.after(() => target.close());
  const replay = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db),
    commits = f.db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all();
  let network = 0;
  t.mock.method(globalThis, 'fetch', async () => {
    network++;
    throw Error('replay must not fetch');
  });
  for (const { seq } of commits)
    target.transaction(() => replay.replayCommit(readRelationalCommit(f.db, seq), inputs))();
  assert.deepEqual(
    target.prepare('SELECT * FROM sharing_policy').all(),
    f.db.prepare('SELECT * FROM sharing_policy').all(),
  );
  for (const table of [
    'api_keys',
    'password_credentials',
    'upstream_credentials',
    'member_sessions',
    'discord_bindings',
  ])
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM ' + table).get().n, 0);
  assert.equal(
    target.prepare('SELECT COUNT(*) AS n FROM members WHERE auth_epoch IS NOT NULL').get().n,
    0,
  );
  const rebuilt = databaseState(target),
    changes = target.prepare('SELECT total_changes() AS n').get().n;
  for (const { seq } of commits)
    target.transaction(() =>
      assert.equal(replay.replayCommit(readRelationalCommit(f.db, seq), inputs).replayed, true),
    )();
  assert.equal(databaseState(target), rebuilt);
  assert.equal(target.prepare('SELECT total_changes() AS n').get().n, changes);
  assert.equal(network, 0);
});
const memberStatus = (enabled, userId = 'm2') => ({ action: 'member-status', userId, enabled });
const memberAdmission = (acceptingJobs, userId = 'm2') => ({
  action: 'member-admission',
  userId,
  acceptingJobs,
});

test('成员管理POST用当前owner身份映射旧DTO，同键返回原状态且不复活Cookie或覆盖后来恢复', async (t) => {
  const f = await setup(t),
    seen = policyNotifications(f),
    headers = { 'idempotency-key': 'member-state', 'x-member-activity': '1' },
    originalKey = f.members.readMemberApiKey('m2'),
    disabled = await f.call(owner, {
      method: 'POST',
      headers,
      body: { ...memberStatus(false), actorKind: 'admin', actorId: 'forged' },
    });
  assert.equal(disabled.status, 200, disabled.text);
  assert.deepEqual(disabled.body, { id: 'm2', enabled: false });
  assert.equal(disabled.headers.get('set-cookie'), null);
  assert.equal(
    await authenticateMemberSession(f.members, f.sessions.m2.token, f.env, f.clock),
    null,
  );
  assert.equal(await authenticateMemberApiKey(f.members, originalKey), null);
  const stoppedDisabled = await f.call(owner, { method: 'POST', body: memberAdmission(false) });
  assert.equal(stoppedDisabled.status, 200, stoppedDisabled.text);
  assert.equal(stoppedDisabled.body.acceptingJobs, false);
  const enabled = await f.call(owner, { method: 'POST', body: memberStatus(true) });
  assert.equal(enabled.status, 200, enabled.text);
  assert.deepEqual(enabled.body, { id: 'm2', enabled: true });
  assert.equal(f.members.readMemberApiKey('m2'), originalKey);
  assert.equal((await f.call(owner, { method: 'POST', body: memberAdmission(true) })).status, 200);
  const fresh = await createMemberSession(
      f.members,
      await authenticateMemberApiKey(f.members, originalKey),
      f.env,
      f.clock,
    ),
    before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() AS n').get().n,
    again = await f.call(owner, {
      method: 'POST',
      headers,
      body: { ...memberStatus(false), actorKind: 'admin', actorId: 'forged' },
    });
  assert.equal(again.status, 200, again.text);
  assert.deepEqual(again.body, disabled.body);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
  assert.ok(await authenticateMemberSession(f.members, fresh.token, f.env, f.clock));
  assert.equal(
    (await f.call(owner, { method: 'POST', headers, body: memberStatus(true) })).status,
    409,
  );
  const stop = await f.call(owner, { method: 'POST', headers, body: memberAdmission(false) });
  assert.equal(stop.status, 200, stop.text);
  assert.deepEqual(stop.body, {
    id: 'm2',
    acceptingJobs: false,
    cancelledJobIds: [],
    cancelledCount: 0,
  });
  const epoch = f.db.prepare("SELECT auth_epoch FROM members WHERE id='m2'").get().auth_epoch,
    resume = await f.call(admin, {
      method: 'POST',
      headers: { 'x-admin-token': adminToken },
      body: memberAdmission(true),
    });
  assert.equal(resume.status, 200, resume.text);
  assert.deepEqual(resume.body, {
    id: 'm2',
    acceptingJobs: true,
    cancelledJobIds: [],
    cancelledCount: 0,
  });
  const resumed = databaseState(f.db),
    count = seen.length;
  assert.deepEqual(
    (await f.call(owner, { method: 'POST', headers, body: memberAdmission(false) })).body,
    stop.body,
  );
  assert.equal(databaseState(f.db), resumed);
  assert.equal(seen.length, count);
  assert.equal(
    f.db.prepare("SELECT accepting_jobs FROM members WHERE id='m2'").get().accepting_jobs,
    1,
  );
  assert.equal(
    f.db.prepare("SELECT auth_epoch FROM members WHERE id='m2'").get().auth_epoch,
    epoch,
  );
  assert.ok(await authenticateMemberSession(f.members, fresh.token, f.env, f.clock));
  assert.equal(f.resourceCalls, 0);
  assert.equal(seen.length, 6);
  const command = f.db
    .prepare(
      "SELECT actor_kind,actor_id FROM commits WHERE command_id LIKE 'management:%' ORDER BY commit_seq LIMIT 1",
    )
    .get();
  assert.deepEqual(command, { actor_kind: 'member', actor_id: 'm1' });
});

test('成员POST严格保留boolean/目标404与当前owner自停用，admin应急启停不复活旧owner Cookie', async (t) => {
  const f = await setup(t),
    before = databaseState(f.db);
  for (const [body, status] of [
    [memberStatus('false'), 400],
    [memberAdmission('false'), 400],
    [memberStatus(false, null), 404],
    [memberAdmission(false, null), 404],
    [memberStatus(false, ''), 404],
    [memberAdmission(false, ''), 404],
    [memberStatus(false, 3), 404],
    [memberAdmission(false, 3), 404],
    [memberStatus(false, 'missing'), 404],
    [memberAdmission(false, 'missing'), 404],
    [memberStatus(false, 'm1'), 409],
  ]) {
    const response = await f.call(owner, { method: 'POST', body });
    assert.equal(response.status, status, response.text);
    assert.equal(databaseState(f.db), before);
  }
  const forged = await f.call(owner, {
    method: 'POST',
    member: 'm2',
    body: {
      ...memberStatus(false),
      member: { id: 'm1' },
      actorKind: 'admin',
      actorId: 'local-admin',
    },
  });
  assert.equal(forged.status, 403, forged.text);
  assert.equal(databaseState(f.db), before);
  for (const enabled of [false, true]) {
    const response = await f.call(admin, {
      method: 'POST',
      headers: { 'x-admin-token': adminToken },
      body: memberStatus(enabled, 'm1'),
    });
    assert.equal(response.status, 200, response.text);
    assert.deepEqual(response.body, { id: 'm1', enabled });
    assert.equal(response.headers.get('set-cookie'), null);
  }
  assert.equal((await f.call()).status, 401);
  const fresh = await createMemberSession(
    f.members,
    await authenticateMemberApiKey(f.members, keys.m1),
    f.env,
    f.clock,
  );
  assert.equal((await f.call(owner, { cookie: fresh.token })).status, 200);
});

function assertMemberManagementPublicReplay(t, f) {
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replay = new RelationalCommitReplayer(target),
    commits = f.db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all();
  let io = 0;
  t.mock.method(globalThis, 'fetch', async () => {
    io++;
    throw Error('public member replay must not fetch');
  });
  for (const { seq } of commits)
    target.transaction(() => replay.replayCommit(readRelationalCommit(f.db, seq), f.inputs))();
  for (const table of [
    'generation_requests',
    'generation_jobs',
    'charges',
    'wallets',
    'sharing_policy',
    'sharing_groups',
    'group_members',
    'account_quota_state',
    'operating_state',
    'commits',
    'domain_events',
    'stream_heads',
  ])
    assert.deepEqual(
      target
        .prepare('SELECT * FROM ' + table + ' ORDER BY rowid')
        .safeIntegers()
        .all(),
      f.db
        .prepare('SELECT * FROM ' + table + ' ORDER BY rowid')
        .safeIntegers()
        .all(),
      table,
    );
  const publicMembers = (db) =>
    db
      .prepare(
        'SELECT id,status,accepting_jobs,admission_epoch,source_event_id FROM members ORDER BY id',
      )
      .all();
  assert.deepEqual(publicMembers(target), publicMembers(f.db));
  for (const table of [
    'api_keys',
    'password_credentials',
    'upstream_credentials',
    'member_sessions',
    'discord_bindings',
    'discord_invites',
    'member_auth_state',
  ])
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM ' + table).get().n, 0, table);
  assert.equal(
    target.prepare('SELECT COUNT(*) AS n FROM members WHERE auth_epoch IS NOT NULL').get().n,
    0,
  );
  const before = databaseState(target),
    changes = target.prepare('SELECT total_changes() AS n').get().n;
  for (const { seq } of commits)
    target.transaction(() =>
      assert.equal(replay.replayCommit(readRelationalCommit(f.db, seq), f.inputs).replayed, true),
    )();
  assert.equal(databaseState(target), before);
  assert.equal(target.prepare('SELECT total_changes() AS n').get().n, changes);
  assert.equal(io, 0);
}

test('成员POST末SQL故障全private/public回滚无通知，原键可重试并完整公共回放', async (t) => {
  for (const action of ['member-status', 'member-admission'])
    await t.test(action, async (t) => {
      const f = await setup(t, {
          beforeReady:
            action === 'member-admission'
              ? async (f) => {
                  f.accept('member-stop-rollback', { memberId: 'm2', samples: 2 });
                }
              : undefined,
        }),
        seen = policyNotifications(f),
        body = action === 'member-status' ? memberStatus(false) : memberAdmission(false),
        headers = { 'idempotency-key': 'member-rollback' };
      if (action === 'member-status')
        f.db.exec(
          "CREATE TRIGGER fail_member_tail AFTER DELETE ON member_sessions WHEN OLD.member_id='m2' BEGIN SELECT RAISE(ABORT,'synthetic private member tail'); END",
        );
      else
        f.db.exec(
          "CREATE TRIGGER fail_member_tail AFTER UPDATE OF status ON generation_jobs WHEN NEW.status='failed' BEGIN SELECT RAISE(ABORT,'synthetic cancel member tail'); END",
        );
      const before = databaseState(f.db),
        failed = await f.call(owner, { method: 'POST', headers, body });
      assert.equal(failed.status, 500, failed.text);
      assert.equal(databaseState(f.db), before);
      assert.equal(seen.length, 0);
      f.db.exec('DROP TRIGGER fail_member_tail');
      const success = await f.call(owner, { method: 'POST', headers, body });
      assert.equal(success.status, 200, success.text);
      assert.equal(seen.length, 1);
      if (action === 'member-status')
        assert.equal(
          await authenticateMemberSession(f.members, f.sessions.m2.token, f.env, f.clock),
          null,
        );
      else {
        assert.equal(success.body.cancelledCount, 2);
        assert.equal(success.body.cancelledJobIds.length, 2);
      }
      const after = databaseState(f.db),
        changes = f.db.prepare('SELECT total_changes() AS n').get().n;
      assert.deepEqual((await f.call(owner, { method: 'POST', headers, body })).body, success.body);
      assert.equal(databaseState(f.db), after);
      assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
      assert.equal(seen.length, 1);
      assertMemberManagementPublicReplay(t, f);
    });
});

test('成员旧回执慢正文后仍核当前Cookie，不用客户端member/actor授信', async (t) => {
  for (const body of [memberStatus(false), memberAdmission(false)])
    await t.test(body.action, async (t) => {
      let begin;
      const f = await setup(t, { onBodyRead: () => begin?.() }),
        headers = { 'idempotency-key': 'held-policy' },
        first = await f.call(owner, { method: 'POST', headers, body });
      assert.equal(first.status, 200, first.text);
      const started = new Promise((resolve) => {
          begin = resolve;
        }),
        held = await holdPolicyBody(t, f, owner, started, headers, body);
      await logoutMemberSession(f.members, f.sessions.m1.token);
      const before = databaseState(f.db),
        response = await held.finish();
      assert.equal(response.status, 401, response.text);
      assert.equal(response.headers['set-cookie'], undefined);
      assert.equal(databaseState(f.db), before);
    });
});
test('真实Native队列physical pending时禁用409，停止仅取消waiting且恢复不复活旧child', async (t) => {
  const env = { OWNER_USER_ID: 'm1' },
    n = await setupNative(t, { env, seconds: 0 });
  const handle = createRelationalManagementHttp({
      sqlite: n.db,
      events: n.events,
      controls: n.controls,
      dataDir: n.dir,
      env,
      now: () => n.clock,
      assertAdmin: (req, url) => assertAdminToken(req, url, adminToken, env),
    }),
    server = createServer(async (req, res) => {
      try {
        if (await handle(req, res, new URL(req.url, `http://${req.headers.host}`))) return;
        res.writeHead(404);
        res.end();
      } catch (error) {
        res.writeHead(error.statusCode || 500, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: publicErrorMessage(error.message) }));
      }
    });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  });
  const origin = `http://127.0.0.1:${server.address().port}`,
    manage = async (body, key) => {
      const response = await fetch(origin + admin, {
        method: 'POST',
        headers: {
          origin,
          'content-type': 'application/json',
          'x-admin-token': adminToken,
          ...(key ? { 'idempotency-key': key } : {}),
        },
        body: JSON.stringify(body),
      });
      return { status: response.status, body: await response.json() };
    };
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  n.upstream.release = release;
  n.upstream.handler = async (_req, res) => {
    await gate;
    res.writeHead(200, { 'content-type': 'image/webp' });
    res.end(image);
  };
  const running = n.call(nativeInput({ n_samples: 2, seed: 42 }), {
    member: 'm2',
    key: 'managed-running',
  });
  running.catch(() => {});
  await waitNative(() => n.upstream.calls.length === 1 && n.controls.size === 1);
  const waiting = n.call(nativeInput({ seed: 43 }), { member: 'm2', key: 'managed-waiting' }),
    other = n.call(nativeInput({ seed: 44 }), { member: 'm1', key: 'managed-other' });
  waiting.catch(() => {});
  other.catch(() => {});
  await waitNative(
    () =>
      n.db
        .prepare("SELECT COUNT(*) AS n FROM generation_jobs WHERE status IN ('queued','pending')")
        .get().n === 3,
  );
  const control = [...n.controls.values()][0],
    live = n.db.prepare("SELECT id FROM generation_jobs WHERE status='running'").get().id,
    charges = n.db.prepare('SELECT * FROM charges ORDER BY rowid').all(),
    wallets = n.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(),
    before = databaseState(n.db);
  for (const body of [rightsTransfer(), rightsRemoval('preview-delete-member'), rightsRemoval()]) {
    assert.equal((await manage(body)).status, 409);
    assert.equal(databaseState(n.db), before);
  }
  assert.equal((await manage(memberStatus(false))).status, 409);
  assert.equal(databaseState(n.db), before);
  const stopped = await manage(memberAdmission(false), 'native-stop');
  assert.equal(stopped.status, 200, JSON.stringify(stopped.body));
  assert.equal(stopped.body.cancelledCount, 2);
  assert.equal(stopped.body.cancelledJobIds.length, 2);
  for (const id of stopped.body.cancelledJobIds)
    assert.deepEqual(
      n.db.prepare('SELECT status,cancellation_reason FROM generation_jobs WHERE id=?').get(id),
      { status: 'failed', cancellation_reason: 'member_stopped' },
    );
  assert.equal(
    n.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get(live).status,
    'running',
  );
  assert.deepEqual(n.db.prepare('SELECT * FROM charges ORDER BY rowid').all(), charges);
  assert.deepEqual(n.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(), wallets);
  assert.equal(n.controls.size, 1);
  assert.equal(control.controller.signal.aborted, false);
  assert.equal(
    n.db
      .prepare(
        "SELECT COUNT(*) AS n FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id WHERE r.member_id='m1' AND j.status='queued'",
      )
      .get().n,
    1,
  );
  assert.equal(
    (await n.call(nativeInput({ seed: 45 }), { member: 'm2', key: 'stopped-new' })).status,
    403,
  );
  const cancelled = await Promise.race([
    waiting,
    new Promise((_, reject) =>
      setTimeout(() => reject(Error('Native cancellation was not delivered')), 5000).unref(),
    ),
  ]);
  assert.equal(cancelled.status, 409, cancelled.buffer.toString());
  assert.equal((await manage(memberAdmission(true), 'native-resume')).status, 200);
  assert.equal(
    n.db.prepare("SELECT admission_epoch FROM members WHERE id='m2'").get().admission_epoch,
    1,
  );
  release();
  const partial = await running;
  assert.equal(partial.status, 200, partial.buffer.toString());
  assert.equal(partial.headers.get('x-nai-partial'), 'true');
  assert.equal(partial.headers.get('x-nai-image-count'), '1');
  assert.equal(
    n.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get(live).status,
    'done',
  );
  assert.equal(n.upstream.calls.length, 1);
  n.advance();
  assert.equal((await other).status, 200);
  const resumed = databaseState(n.db),
    duplicate = await manage(memberAdmission(false), 'native-stop');
  assert.deepEqual(duplicate.body, stopped.body);
  assert.equal(databaseState(n.db), resumed);
  assert.equal(
    n.db.prepare("SELECT accepting_jobs FROM members WHERE id='m2'").get().accepting_jobs,
    1,
  );
  n.advance();
  const fresh = await n.call(nativeInput({ seed: 99 }), {
    member: 'm2',
    key: 'fresh-after-resume',
  });
  assert.equal(fresh.status, 200, fresh.buffer.toString());
  assert.equal(n.upstream.calls.length, 3);
  for (const id of stopped.body.cancelledJobIds)
    assert.equal(
      n.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get(id).status,
      'failed',
    );
  assert.equal((await manage(memberStatus(false), 'idle-disable')).status, 200);
  assertMemberManagementPublicReplay(t, n);
});

const accountOwner = '/api/member/owner/accounts/a',
  accountAdmin = '/api/admin/accounts/a';
async function accountPatchFixture(t, options = {}) {
  let f, quotas;
  const calls = [],
    logs = [],
    upstream = {
      status: 200,
      onQuery: null,
      payload: {
        subscription: {
          tier: 3,
          active: true,
          expiresAt: NOW + 86400000,
          trainingStepsLeft: { fixedTrainingStepsLeft: 8000, purchasedTrainingSteps: 0 },
          usage: { percent: 80, isNegative: false, timeUntilNextPercent: 600 },
        },
      },
    },
    server = createServer(async (req, res) => {
      calls.push({ path: req.url, authorization: req.headers.authorization });
      assert.equal(f.db.inTransaction, false, 'real quota HTTP must run outside SQL');
      if (req.url === '/user/data') {
        await upstream.onQuery?.();
        res.writeHead(upstream.status, { 'content-type': 'application/json' });
        res.end(JSON.stringify(upstream.payload));
      } else {
        assert.equal(req.url, '/ai/trial-status');
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ remaining_image_actions: 0, eligible_for_image_gens: false }));
      }
    });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  });
  const quotaEnv = { NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${server.address().port}` };
  const query = createAccountQuotaQuery({
    env: quotaEnv,
    now: () => f.clock,
    logger: { log: (...args) => logs.push(args), error: (...args) => logs.push(args) },
    fetchQuota: (...args) => {
      assert.equal(f.db.inTransaction, false);
      return fetchNovelAiAccountQuota(...args);
    },
  });
  f = await setup(t, {
    ...options,
    queryAccountQuota: query,
    quotaStore: () => quotas,
    async beforeReady(f, env) {
      quotas = new RelationalQuotaStore(f.db, env);
      await options.beforeReady?.(f, env);
    },
    onBodyRead() {
      assert.equal(f.db.inTransaction, false, 'body must be read outside SQL');
      options.onBodyRead?.();
    },
  });
  let notifications = 0;
  f.events.on('commit', () => notifications++);
  const patch = (body, { route = accountOwner, key, headers = {}, ...rest } = {}) =>
    f.call(route, {
      method: 'PATCH',
      body,
      headers: { ...(key ? { 'idempotency-key': key } : {}), ...headers },
      ...rest,
    });
  return {
    ...f,
    upstream,
    calls,
    logs,
    patch,
    query,
    quotaEnv,
    quotas,
    get notifications() {
      return notifications;
    },
    // Preserve the live clock accessor instead of a spread snapshot.
    get clock() {
      return f.clock;
    },
  };
}
function patchHistory(db) {
  return JSON.stringify(
    [
      'commits',
      'domain_events',
      'stream_heads',
      'upstream_accounts',
      'upstream_credentials',
      'account_quota_state',
      'wallets',
      'charges',
      'generation_jobs',
    ].map((table) => db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all()),
  );
}

test('账户PATCH真实ready Cookie轮换/暂停/Opus核验共用owner，owner原回执零IO且admin保留原非keyed行为', async (t) => {
  const f = await accountPatchFixture(t),
    before = databaseState(f.db);
  for (const body of [
    {},
    { enabled: 'true' },
    { token: 'not-pst' },
    { enabled: false, token: 'pst-rejected' },
  ]) {
    assert.equal((await f.patch(body)).status, 400);
    assert.equal(databaseState(f.db), before);
  }
  const rotated = await f.patch({ token: 'pst-http-rotated' }, { key: 'same-transport-key' });
  assert.equal(rotated.status, 200, rotated.text);
  assert.deepEqual(rotated.body, { accountId: 'a', enabled: false });
  assert.equal(f.calls.length, 0);
  assert.equal(f.notifications, 1);
  assert.equal(
    f.db.prepare("SELECT credential_epoch FROM upstream_accounts WHERE id='a'").get()
      .credential_epoch,
    2,
  );
  assert.equal(
    f.db.prepare("SELECT pst FROM upstream_credentials WHERE account_id='a'").get().pst,
    'pst-http-rotated',
  );
  const enabled = await f.patch({ enabled: true }, { key: 'enable-http' });
  assert.equal(enabled.status, 200, enabled.text);
  assert.deepEqual(enabled.body, { accountId: 'a', enabled: true });
  assert.equal(f.calls.length, 1);
  assert.equal(f.calls[0].authorization, 'Bearer pst-http-rotated');
  assert.equal(f.notifications, 2);
  assert.equal(
    f.db
      .prepare("SELECT observed_credential_epoch FROM account_quota_state WHERE account_id='a'")
      .get().observed_credential_epoch,
    2,
  );
  const paused = await f.patch({ enabled: false }, { key: 'pause-http' });
  assert.equal(paused.status, 200);
  const snapshot = databaseState(f.db),
    count = f.notifications;
  assert.deepEqual((await f.patch({ enabled: true }, { key: 'enable-http' })).body, enabled.body);
  assert.equal(databaseState(f.db), snapshot, 'old success must not re-enable after a later pause');
  assert.equal(f.notifications, count);
  assert.equal(f.calls.length, 1);
  assert.equal((await f.patch({ enabled: false }, { key: 'enable-http' })).status, 409);
  const admin = await f.patch(
    { enabled: false },
    { route: accountAdmin, key: 'same-transport-key', headers: { 'x-admin-token': adminToken } },
  );
  assert.equal(admin.status, 200, admin.text);
  assert.equal(f.notifications, count + 1, 'admin PATCH also publishes exactly once after commit');
  const adminState = databaseState(f.db);
  assert.deepEqual(
    (
      await f.patch(
        { enabled: false },
        {
          route: accountAdmin,
          key: 'same-transport-key',
          headers: { 'x-admin-token': adminToken },
        },
      )
    ).body,
    admin.body,
  );
  assert.notEqual(
    databaseState(f.db),
    adminState,
    '原 admin PATCH 不承诺幂等 Header，新的 HTTP 是新管理决定',
  );
  assert.equal(f.notifications, count + 2);
  const actors = f.db
    .prepare(
      "SELECT actor_kind,actor_id FROM commits WHERE command_id LIKE 'management:%' ORDER BY commit_seq",
    )
    .all();
  assert.deepEqual(actors, [
    { actor_kind: 'member', actor_id: 'm1' },
    { actor_kind: 'member', actor_id: 'm1' },
    { actor_kind: 'member', actor_id: 'm1' },
  ]);
  assert.deepEqual(
    f.db.prepare('SELECT actor_kind,actor_id FROM commits ORDER BY commit_seq DESC LIMIT 2').all(),
    [
      { actor_kind: 'admin', actor_id: 'local-admin' },
      { actor_kind: 'admin', actor_id: 'local-admin' },
    ],
  );
  for (const secret of ['pst-http-rotated', keys.m1, f.sessions.m1.token]) {
    assert.ok(!JSON.stringify(f.db.prepare('SELECT * FROM domain_events').all()).includes(secret));
    assert.ok(!JSON.stringify(f.db.prepare('SELECT receipt FROM commits').all()).includes(secret));
    assert.ok(!JSON.stringify(f.logs).includes(secret));
  }
  assert.equal(rotated.headers.get('set-cookie'), null);
  await logoutMemberSession(f.members, f.sessions.m1.token);
  const revoked = databaseState(f.db);
  assert.equal((await f.patch({ enabled: true }, { key: 'enable-http' })).status, 401);
  assert.equal(databaseState(f.db), revoked);
  assert.equal(f.calls.length, 1);
});

test('账户PATCH仍需当前Cookie或local admin且同源，Bearer不替代；owner不获得name/proxy/weight权限', async (t) => {
  const f = await accountPatchFixture(t),
    before = databaseState(f.db);
  const cases = [
    [{ cookie: '', headers: { authorization: `Bearer ${keys.m1}` } }, 401],
    [{ member: 'm2' }, 403],
    [{ headers: { origin: 'https://different.example' } }, 404],
    [{ route: accountAdmin }, 403],
    [
      { route: accountAdmin, headers: { 'x-admin-token': adminToken, host: 'management.example' } },
      404,
    ],
  ];
  for (const [options, status] of cases) {
    assert.equal((await f.patch({ enabled: false }, options)).status, status);
    assert.equal(databaseState(f.db), before);
  }
  for (const field of ['name', 'weight', 'proxyUrl', 'socksProxy', 'proxy']) {
    assert.equal(
      (await f.patch({ [field]: field === 'weight' ? 5 : 'unsupported' }, { route: accountOwner }))
        .status,
      400,
    );
    assert.equal(databaseState(f.db), before);
  }
  assert.equal(
    (await f.patch({ enabled: true }, { route: '/api/member/owner/accounts/not-bound' })).status,
    404,
  );
  assert.equal(f.calls.length, 0);
});

test('账户PATCH真实额度失败或非Opus保留失败观察/原回执，重领不重新查询', async (t) => {
  for (const mode of ['non-opus', 'http403', 'expired'])
    await t.test(mode, async (t) => {
      const f = await accountPatchFixture(t);
      await f.patch({ enabled: false });
      if (mode === 'non-opus') f.upstream.payload.subscription.tier = 2;
      if (mode === 'expired') f.upstream.payload.subscription.expiresAt = NOW - 1;
      if (mode === 'http403') {
        f.upstream.status = 403;
        f.upstream.payload = { error: 'pst-upstream-secret private body' };
      }
      const result = await f.patch({ enabled: true }, { key: 'failure-receipt' });
      assert.equal(result.status, 409, result.text);
      assert.equal(
        f.db.prepare("SELECT enabled FROM upstream_accounts WHERE id='a'").get().enabled,
        0,
      );
      assert.equal(f.calls.length, 1);
      assert.equal(f.notifications, 2);
      assert.ok(!result.text.includes('pst-upstream-secret'));
      const before = databaseState(f.db),
        notifications = f.notifications;
      f.upstream.status = 200;
      f.upstream.payload = { subscription: { tier: 3, active: true } };
      assert.deepEqual(
        await f.patch({ enabled: true }, { key: 'failure-receipt' }).then((r) => r.body),
        result.body,
      );
      assert.equal(databaseState(f.db), before);
      assert.equal(f.notifications, notifications);
      assert.equal(f.calls.length, 1);
    });
});

test('账户PATCH网络等待后原owner/Cookie/epoch/ready/凭据/暂停/政策/admin逐项复核，拒绝不追加旧观察', async (t) => {
  for (const mode of ['owner', 'cookie', 'epoch', 'ready', 'rotate', 'pause', 'policy', 'admin'])
    await t.test(mode, async (t) => {
      const f = await accountPatchFixture(t);
      await f.patch({ enabled: false });
      let release, entered;
      const gate = new Promise((r) => (release = r)),
        started = new Promise((r) => (entered = r));
      f.upstream.onQuery = async () => {
        entered();
        await gate;
      };
      const response = f.patch(
        { enabled: true },
        {
          key: 'waiting-' + mode,
          ...(mode === 'admin' ? { route: accountAdmin + '?adminToken=' + adminToken } : {}),
        },
      );
      await started;
      try {
        if (mode === 'owner') f.env.OWNER_USER_ID = 'm2';
        if (mode === 'cookie') await logoutMemberSession(f.members, f.sessions.m1.token);
        if (mode === 'epoch') {
          const prepared = await f.members.prepareMemberCredentials('m1', {
            username: 'changed.current',
            password: 'synthetic-patch-password',
          });
          f.db.transaction(() => f.members.setMemberCredentials(prepared, null))();
        }
        if (mode === 'ready') f.db.prepare("UPDATE projection_state SET status='building'").run();
        if (mode === 'rotate')
          assert.equal((await f.patch({ token: 'pst-new-current' })).status, 200);
        if (mode === 'pause') assert.equal((await f.patch({ enabled: false })).status, 200);
        if (mode === 'policy') {
          const q = new RelationalQuotaStore(f.db, f.env),
            groups = [
              ...q.readQuotaConfiguration().groups,
              ...['g2', 'g3', 'g4'].map((id) => ({
                id,
                name: id,
                share: 0,
                members: [],
                memberWeights: {},
              })),
            ];
          await withAuthenticatedMemberApiKey(
            f.members,
            keys.m1,
            (actor) =>
              q.configureGroups(
                meta('policy-during-query', f.clock, 'm1'),
                { expectedConfigVersion: 1, groups },
                actor,
              ),
            f.env,
          );
        }
        if (mode === 'admin') f.env.PUBLIC_MODE = 'true';
        const before = patchHistory(f.db);
        release();
        const result = await response;
        assert.equal(
          result.status,
          {
            owner: 401,
            cookie: 401,
            epoch: 401,
            ready: 500,
            rotate: 409,
            pause: 409,
            policy: 409,
            admin: 401,
          }[mode],
          result.text,
        );
        assert.equal(patchHistory(f.db), before);
        assert.equal(
          f.db.prepare("SELECT enabled FROM upstream_accounts WHERE id='a'").get().enabled,
          0,
        );
        assert.equal(f.calls.length, 1);
      } finally {
        release();
      }
    });
});

test('账户PATCH轮换全账户physical/reserved守卫与SQL尾错全回滚，健康重试/单通知和公共重放无私有外效', async (t) => {
  const f = await accountPatchFixture(t);
  // 已退款也不代表真实物理槽已释放。
  const running = createGenerationControls({
      controls: f.controls,
      timeoutDelay: () => 60000,
      schedule() {},
    }),
    control = running.createRunningJobControl({ id: 'late-physical', accountId: 'a' });
  t.after(() => running.finishRunningJobControl('late-physical', control));
  let before = databaseState(f.db);
  assert.equal((await f.patch({ token: 'pst-blocked' })).status, 409);
  assert.equal(databaseState(f.db), before);
  running.finishRunningJobControl('late-physical', control);
  f.accept('other-member-reserved', { memberId: 'm2' });
  f.dispatch('other-member-reserved-0', f.clock);
  before = databaseState(f.db);
  assert.equal((await f.patch({ token: 'pst-blocked' })).status, 409);
  assert.equal(databaseState(f.db), before);
  f.fail('other-member-reserved-0', { aborted: false });
  // 故障发生在CredentialRotated/Availability投影之后的真实私有PST写入。
  f.db.exec(
    "CREATE TEMP TRIGGER patch_private_fault BEFORE INSERT ON upstream_credentials BEGIN SELECT RAISE(ABORT,'patch private fault'); END",
  );
  before = databaseState(f.db);
  const notifications = f.notifications;
  assert.equal(
    (await f.patch({ token: 'pst-retry-current' }, { key: 'retry-rotate' })).status,
    500,
  );
  assert.equal(databaseState(f.db), before);
  assert.equal(f.notifications, notifications);
  f.db.exec('DROP TRIGGER patch_private_fault');
  assert.equal(
    (await f.patch({ token: 'pst-retry-current' }, { key: 'retry-rotate' })).status,
    200,
  );
  assert.equal(f.notifications, notifications + 1);
  const snapshot = databaseState(f.db);
  assert.equal(
    (await f.patch({ token: 'pst-retry-current' }, { key: 'retry-rotate' })).status,
    200,
  );
  assert.equal(databaseState(f.db), snapshot);
  assert.equal(f.notifications, notifications + 1);
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replay = new RelationalCommitReplayer(target),
    source = new RelationalInputStore(f.db);
  const fetch = t.mock.method(globalThis, 'fetch', () => assert.fail('public replay cannot fetch'));
  try {
    for (const { seq } of f.db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all()) {
      target.transaction(() => replay.replayCommit(readRelationalCommit(f.db, seq), source))();
    }
  } finally {
    fetch.mock.restore();
  }
  for (const table of [
    'commits',
    'domain_events',
    'stream_heads',
    'upstream_accounts',
    'account_quota_state',
    'wallets',
    'charges',
    'generation_jobs',
  ]) {
    assert.deepEqual(
      target.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
      f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
      table,
    );
  }
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
  assert.ok(
    target
      .prepare('SELECT auth_epoch FROM members')
      .all()
      .every((r) => r.auth_epoch === null),
  );
});

test('账户PATCH正文等待后当前授权拒绝且不查上游；未注入实际query能力503而暂停可用', async (t) => {
  for (const mode of ['owner', 'admin'])
    await t.test(mode, async (t) => {
      let f;
      f = await accountPatchFixture(t, {
        onBodyRead() {
          if (mode === 'owner') f.env.OWNER_USER_ID = 'm2';
          else f.env.PUBLIC_MODE = 'true';
        },
      });
      const before = patchHistory(f.db),
        result = await f.patch(
          { enabled: true },
          mode === 'admin' ? { route: accountAdmin + '?adminToken=' + adminToken } : {},
        );
      assert.equal(result.status, 401, result.text);
      assert.equal(patchHistory(f.db), before);
      assert.equal(f.calls.length, 0);
    });
  const f = await setup(t),
    before = patchHistory(f.db);
  const result = await f.call(accountOwner, { method: 'PATCH', body: { enabled: true } });
  assert.equal(result.status, 503, result.text);
  assert.equal(patchHistory(f.db), before);
  assert.equal(
    (await f.call(accountOwner, { method: 'PATCH', body: { enabled: false } })).status,
    200,
  );
});

test('账户PATCH与实际共享QuotaStore发起顺序一致，晚发票据使旧观察失效；启用SQL尾错全表回滚重试只通知一次', async (t) => {
  const f = await accountPatchFixture(t);
  await f.patch({ enabled: false });
  let later;
  f.upstream.onQuery = () => {
    later = f.quotas.beginQuotaObservation('a', f.clock);
  };
  let before = databaseState(f.db),
    notifications = f.notifications;
  assert.equal((await f.patch({ enabled: true }, { key: 'old-observation' })).status, 409);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.notifications, notifications);
  assert.ok(later.sequence);
  f.upstream.onQuery = null;
  f.db.exec(
    "CREATE TEMP TRIGGER patch_enable_fault BEFORE UPDATE ON upstream_accounts WHEN NEW.enabled=1 BEGIN SELECT RAISE(ABORT,'patch enable fault'); END",
  );
  before = databaseState(f.db);
  assert.equal((await f.patch({ enabled: true }, { key: 'enable-retry' })).status, 500);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.notifications, notifications);
  f.db.exec('DROP TRIGGER patch_enable_fault');
  const result = await f.patch({ enabled: true }, { key: 'enable-retry' });
  assert.equal(result.status, 200, result.text);
  assert.equal(f.notifications, notifications + 1);
  before = databaseState(f.db);
  const calls = f.calls.length;
  assert.deepEqual((await f.patch({ enabled: true }, { key: 'enable-retry' })).body, result.body);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.notifications, notifications + 1);
  assert.equal(f.calls.length, calls);
});

async function sharedAccountPatch(f, t, options = {}) {
  const refresh = createRelationalAccountQuotaRefresh({
    sqlite: f.db,
    quotas: f.quotas,
    events: f.events,
    env: { ...f.env, ...f.quotaEnv },
    now: () => f.clock,
    logger: { log() {}, error() {} },
    ...options,
  });
  f.setHandler(
    createRelationalManagementHttp({
      sqlite: f.db,
      quotas: f.quotas,
      queryAccountQuota: refresh.fetch,
      events: f.events,
      controls: f.controls,
      dataDir: f.dir,
      env: f.env,
      now: () => f.clock,
      assertAdmin: (req, url) => assertAdminToken(req, url, adminToken, f.env),
    }),
  );
  t.after(() => refresh.stop());
  return refresh;
}

test('账户PATCH实际shared query繁忙无GET不得领票或伪观察，唯一原成功响应与busy旧receipt保持有效', async (t) => {
  const f = await accountPatchFixture(t),
    refresh = await sharedAccountPatch(f, t);
  const saved = await f.patch({ enabled: true }, { key: 'original-success' });
  assert.equal(saved.status, 200, saved.text);
  let release, enter;
  const gate = new Promise((r) => (release = r)),
    entered = new Promise((r) => (enter = r));
  f.upstream.onQuery = async () => {
    enter();
    await gate;
  };
  const original = refresh.refreshEnabled();
  await entered;
  try {
    const before = databaseState(f.db),
      calls = f.calls.length;
    const busy = await f.patch({ enabled: true }, { key: 'busy-must-not-save' });
    assert.equal(busy.status, 409, busy.text);
    assert.equal(databaseState(f.db), before);
    assert.equal(f.calls.length, calls);
    assert.deepEqual(
      (await f.patch({ enabled: true }, { key: 'original-success' })).body,
      saved.body,
    );
    assert.equal(databaseState(f.db), before);
    assert.equal(f.calls.length, calls);
    release();
    assert.deepEqual(await original, { checked: 1, ok: 1, failed: 0, ignored: 0 });
  } finally {
    release();
  }
  f.upstream.onQuery = null;
  assert.equal(
    (await f.patch({ enabled: true }, { key: 'busy-must-not-save' })).status,
    200,
    'local rejection saved no receipt, same key can actually start',
  );
  assert.equal(f.calls.length, 3);
});

test('账户PATCH已发真实query超时可提交失败观察，raw吞abort仍占槽且stop等实际HTTPsettle', async (t) => {
  const f = await accountPatchFixture(t),
    refresh = await sharedAccountPatch(f, t, {
      requestTimeoutMs: 100,
      fetchQuota: (token, env, options) =>
        fetchNovelAiAccountQuota(token, env, { ...options, signal: undefined }),
    });
  let release, enter;
  const gate = new Promise((r) => (release = r)),
    entered = new Promise((r) => (enter = r));
  f.upstream.onQuery = async () => {
    enter();
    await gate;
  };
  const pending = f.patch({ enabled: true }, { key: 'timed-real-query' });
  await entered;
  try {
    const timeout = await pending;
    assert.equal(timeout.status, 409, timeout.text);
    assert.equal(f.calls.length, 1);
    const before = databaseState(f.db),
      observed = f.db
        .prepare(
          "SELECT CAST(observed_sequence AS TEXT) seq FROM account_quota_state WHERE account_id='a'",
        )
        .get().seq;
    assert.equal((await f.patch({ enabled: true }, { key: 'busy-after-deadline' })).status, 409);
    assert.equal(databaseState(f.db), before);
    assert.equal(f.calls.length, 1);
    assert.deepEqual(
      (await f.patch({ enabled: true }, { key: 'timed-real-query' })).body,
      timeout.body,
    );
    assert.equal(databaseState(f.db), before);
    assert.equal(f.calls.length, 1);
    let stopped = false;
    refresh.stop();
    const stop = refresh.pending.then(() => (stopped = true));
    await new Promise((r) => setTimeout(r, 5));
    assert.equal(stopped, false);
    assert.ok(refresh.pending);
    release();
    await stop;
    assert.equal(stopped, true);
    assert.equal(refresh.pending, null);
    assert.equal(databaseState(f.db), before, 'late raw success cannot rewrite timed observation');
    f.upstream.onQuery = null;
    assert.equal((await f.patch({ enabled: true }, { key: 'busy-after-deadline' })).status, 200);
    assert.ok(
      BigInt(
        f.db
          .prepare(
            "SELECT CAST(observed_sequence AS TEXT) seq FROM account_quota_state WHERE account_id='a'",
          )
          .get().seq,
      ) > BigInt(observed),
    );
  } finally {
    release();
  }
});

test('member-key 真实owner/admin只读当前Key，原header不造回执或复活轮换前私有值', async (t) => {
  const f = await setup(t, {
      discordFetch: () => {
        throw Error('unexpected member-key Discord IO');
      },
    }),
    seen = policyNotifications(f),
    body = { action: 'member-key', userId: 'm2' },
    headers = { 'idempotency-key': 'reused-read-header', 'x-member-activity': '1' };
  for (const [route, extra] of [
    [owner, {}],
    [admin, { 'x-admin-token': adminToken }],
  ]) {
    const before = databaseState(f.db),
      changes = f.db.prepare('SELECT total_changes() AS n').get().n,
      result = await f.call(route, { method: 'POST', headers: { ...headers, ...extra }, body });
    assert.equal(result.status, 200, result.text);
    assert.deepEqual(result.body, { apiKey: keys.m2 });
    assert.equal(result.headers.get('cache-control'), 'no-store');
    assert.equal(result.headers.get('set-cookie'), null);
    assert.equal(databaseState(f.db), before);
    assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
  }
  const changedKey = 'synthetic-current-private-m2-key';
  f.db.transaction(() => f.members.replaceMemberApiKey('m2', changedKey))();
  const before = databaseState(f.db);
  for (const key of ['reused-read-header', '', 'not a command key']) {
    const result = await f.call(owner, {
      method: 'POST',
      headers: { 'idempotency-key': key },
      body,
    });
    assert.equal(result.status, 200, result.text);
    assert.deepEqual(result.body, { apiKey: changedKey });
    assert.ok(!result.text.includes(keys.m2));
  }
  assert.equal(databaseState(f.db), before);
  f.db.transaction(() =>
    f.members.setMemberStatus(meta('key-disabled-target'), 'm2', 'disabled'),
  )();
  const disabled = databaseState(f.db),
    result = await f.call(owner, { method: 'POST', body });
  assert.equal(result.status, 200, result.text);
  assert.deepEqual(result.body, { apiKey: changedKey });
  assert.equal(databaseState(f.db), disabled);
  for (const userId of ['missing-member', '', null, 2, {}]) {
    const missing = await f.call(owner, { method: 'POST', body: { action: 'member-key', userId } });
    assert.equal(missing.status, 404, missing.text);
  }
  assert.equal((await f.call(owner, { member: 'm2', method: 'POST', body })).status, 401);
  assert.equal((await f.call(admin, { method: 'POST', body })).status, 403);
  assert.equal(seen.length, 0);
  assert.equal(f.resourceCalls, 0);
  const sourceFacts =
    JSON.stringify(f.db.prepare('SELECT receipt FROM commits').all()) +
    JSON.stringify(f.db.prepare('SELECT payload FROM domain_events').all());
  assert.ok(!sourceFacts.includes(changedKey));
  assert.ok(!sourceFacts.includes(keys.m2));
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replay = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db),
    commits = f.db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all();
  let network = 0;
  t.mock.method(globalThis, 'fetch', () => {
    network++;
    throw Error('key replay must not fetch');
  });
  for (const { seq } of commits) {
    target.transaction(() => replay.replayCommit(readRelationalCommit(f.db, seq), inputs))();
  }
  const restoredMembers = new RelationalMemberStore(target);
  assert.equal(restoredMembers.readMemberApiKey('m2'), null);
  for (const table of [
    'api_keys',
    'password_credentials',
    'member_sessions',
    'member_auth_state',
    'discord_bindings',
    'discord_invites',
    'upstream_credentials',
  ]) {
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM ' + table).get().n, 0);
  }
  assert.equal(
    target.prepare('SELECT COUNT(*) AS n FROM members WHERE auth_epoch IS NOT NULL').get().n,
    0,
  );
  const rebuilt = databaseState(target),
    changes = target.prepare('SELECT total_changes() AS n').get().n;
  for (const { seq } of commits) {
    target.transaction(() => replay.replayCommit(readRelationalCommit(f.db, seq), inputs))();
  }
  assert.equal(databaseState(target), rebuilt);
  assert.equal(target.prepare('SELECT total_changes() AS n').get().n, changes);
  assert.equal(network, 0);
});

test('member-key 慢正文只在末次当前授权后读取，保留epoch/owner/ready/admin边界', async (t) => {
  for (const mode of ['rotated-target', 'epoch-ABA', 'owner', 'ready', 'admin-query']) {
    await t.test(mode, async (t) => {
      let begin;
      const f = await setup(t, {
          onBodyRead: () => begin?.(),
          discordFetch: () => {
            throw Error('unexpected member-key Discord IO');
          },
        }),
        seen = policyNotifications(f),
        started = new Promise((resolve) => {
          begin = resolve;
        }),
        held = await holdPolicyBody(
          t,
          f,
          mode === 'admin-query' ? admin + '?adminToken=' + adminToken : owner,
          started,
          {},
          { action: 'member-key', userId: 'm2' },
        );
      if (mode === 'rotated-target') {
        f.db.transaction(() =>
          f.members.replaceMemberApiKey('m2', 'synthetic-new-target-key-after-body'),
        )();
      } else if (mode === 'epoch-ABA') {
        f.db.transaction(() =>
          f.members.setMemberStatus(meta('key-owner-disable'), 'm1', 'disabled'),
        )();
        f.db.transaction(() =>
          f.members.setMemberStatus(meta('key-owner-enable'), 'm1', 'active'),
        )();
      } else if (mode === 'owner') {
        f.env.OWNER_USER_ID = 'm2';
      } else if (mode === 'ready') {
        f.db.prepare("UPDATE projection_state SET status='failed' WHERE id=1").run();
      } else {
        f.env.PUBLIC_MODE = 'true';
      }
      const cut = f.db
          .prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits')
          .get().seq,
        keysBefore = f.db.prepare('SELECT * FROM api_keys ORDER BY member_id').all(),
        result = await held.finish();
      if (mode === 'rotated-target') {
        assert.equal(result.status, 200, result.text);
        assert.deepEqual(JSON.parse(result.text), {
          apiKey: 'synthetic-new-target-key-after-body',
        });
      } else {
        assert.equal(result.status, mode === 'ready' ? 500 : 401, result.text);
        assert.ok(!result.text.includes(keys.m2));
      }
      assert.equal(result.headers['set-cookie'], undefined);
      assert.equal(
        f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get().seq,
        cut,
      );
      assert.deepEqual(f.db.prepare('SELECT * FROM api_keys ORDER BY member_id').all(), keysBefore);
      assert.ok(
        seen.every(({ event }) =>
          event.collections.every((c) => ['members', 'sessions'].includes(c)),
        ),
      );
    });
  }
});

test('member-key 私有恢复不足/校验损坏/移除目标拒绝，SQL末尾失败不回包或制造私有回执', async (t) => {
  for (const mode of ['missing-private', 'corrupt-private', 'removed', 'sql-tail']) {
    await t.test(mode, async (t) => {
      const f = await setup(t, {
          discordFetch: () => {
            throw Error('unexpected member-key Discord IO');
          },
          beforeReady:
            mode === 'removed'
              ? async (f, env) => {
                  const quotas = new RelationalQuotaStore(f.db, env);
                  f.db.transaction(() =>
                    quotas.manageMemberQuota(
                      { ...meta('remove-key-target'), actorKind: 'admin', actorId: 'local-admin' },
                      {
                        action: 'delete-member',
                        userId: 'm2',
                        transferToUserId: 'm1',
                        expectedConfigVersion: 1,
                      },
                      null,
                      new Map(),
                    ),
                  )();
                }
              : undefined,
        }),
        seen = policyNotifications(f);
      // 合法 ready 之后模拟原私有材料故障，不声称这种坏源通过过 installer。
      if (mode === 'missing-private') {
        f.db.prepare("DELETE FROM api_keys WHERE member_id='m2'").run();
      }
      if (mode === 'corrupt-private') {
        f.db.prepare("UPDATE api_keys SET key_hash=? WHERE member_id='m2'").run('f'.repeat(64));
      }
      const before = databaseState(f.db);
      let tail = false,
        readMock,
        transactionMock;
      const transaction = f.db.transaction,
        read = RelationalMemberStore.prototype.readMemberApiKey;
      if (mode === 'sql-tail') {
        readMock = t.mock.method(
          RelationalMemberStore.prototype,
          'readMemberApiKey',
          function (...args) {
            const value = read.apply(this, args);
            tail = true;
            return value;
          },
        );
        transactionMock = t.mock.method(f.db, 'transaction', function (write) {
          return transaction.call(this, (...args) => {
            const value = write(...args);
            if (tail) {
              tail = false;
              this.prepare('SELECT * FROM missing_member_key_tail_table').get();
            }
            return value;
          });
        });
      }
      const body = { action: 'member-key', userId: 'm2' },
        response = await f.call(owner, {
          method: 'POST',
          body,
          headers: { 'idempotency-key': 'read-tail' },
        });
      assert.equal(
        response.status,
        mode === 'removed' ? 404 : mode === 'sql-tail' ? 500 : 503,
        response.text,
      );
      assert.ok(!response.text.includes(keys.m2));
      assert.equal(databaseState(f.db), before);
      assert.equal(seen.length, 0);
      if (mode === 'sql-tail') {
        readMock.mock.restore();
        transactionMock.mock.restore();
        const retry = await f.call(owner, {
          method: 'POST',
          body,
          headers: { 'idempotency-key': 'read-tail' },
        });
        assert.equal(retry.status, 200, retry.text);
        assert.deepEqual(retry.body, { apiKey: keys.m2 });
        assert.equal(databaseState(f.db), before);
        assert.equal(seen.length, 0);
      }
    });
  }
});

test('管理作用域HTTP：合法local-admin稳定成员与真正admin同key仍独立，组和政策原回执各自零写', async (t) => {
  const ownerId = 'local-admin',
    ownerKey = 'synthetic-local-admin-member-key',
    f = await setup(t, {
      env: { OWNER_USER_ID: ownerId },
      beforeReady: async (f) => {
        f.db.transaction(() =>
          f.members.registerMember(meta('register-local-admin-owner'), {
            memberId: ownerId,
            displayName: 'actual owner',
            apiKey: ownerKey,
          }),
        )();
        await f.members.discordAuthenticationTransaction((tx) => {
          const member = tx.readMember(ownerId);
          member.discord = {
            id: '444444444444444444',
            guildId: '1134557553011998840',
            accessToken: 'synthetic-owner-access',
            refreshToken: 'synthetic-owner-refresh',
            expiresAt: NOW + 86400000,
            checkedAt: NOW,
            member: true,
          };
          tx.saveAuthentication(member, 'login');
        });
      },
    }),
    session = await createMemberSession(
      f.members,
      await authenticateMemberApiKey(f.members, ownerKey),
      f.env,
      f.clock,
    ),
    key = 'same-key-and-id-distinct-real-roles',
    seen = policyNotifications(f),
    ownerHeaders = { 'idempotency-key': key, 'x-member-activity': '1' },
    adminHeaders = { ...ownerHeaders, 'x-admin-token': adminToken },
    ownerGroup = managementGroupBody('groups', 1),
    adminGroup = managementGroupBody('groups', 2, { m1: 1, m2: 1 });
  const first = await f.call(owner, {
    method: 'POST',
    cookie: session.token,
    headers: ownerHeaders,
    body: ownerGroup,
  });
  assert.equal(first.status, 200, first.text);
  assert.equal(first.body.configVersion, 2);
  const second = await f.call(admin, { method: 'POST', headers: adminHeaders, body: adminGroup });
  assert.equal(second.status, 200, second.text);
  assert.equal(second.body.configVersion, 3);
  const ownerPolicy = { action: 'fallback-policy', enabled: true, actorKind: 'admin' },
    adminPolicy = { action: 'fallback-policy', enabled: false, actorKind: 'member' },
    third = await f.call(owner, {
      method: 'POST',
      cookie: session.token,
      headers: ownerHeaders,
      body: ownerPolicy,
    }),
    fourth = await f.call(admin, { method: 'POST', headers: adminHeaders, body: adminPolicy });
  assert.equal(third.status, 200, third.text);
  assert.deepEqual(third.body, { autoAnlasFallback: true });
  assert.equal(fourth.status, 200, fourth.text);
  assert.deepEqual(fourth.body, { autoAnlasFallback: false });
  const commands = f.db
    .prepare(
      "SELECT command_id,actor_kind,actor_id FROM commits WHERE command_id LIKE 'management:%' ORDER BY commit_seq",
    )
    .all();
  assert.equal(new Set(commands.map((c) => c.command_id)).size, 4);
  assert.deepEqual(
    commands.map((c) => c.actor_kind),
    ['member', 'admin', 'member', 'admin'],
  );
  assert.ok(commands.every((c) => c.actor_id === ownerId));
  const before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() AS n').get().n,
    notifications = seen.length;
  for (const [endpoint, cookie, headers, body, expected] of [
    [owner, session.token, ownerHeaders, ownerGroup, first.body],
    [admin, f.sessions.m1.token, adminHeaders, adminGroup, second.body],
    [owner, session.token, ownerHeaders, ownerPolicy, third.body],
    [admin, f.sessions.m1.token, adminHeaders, adminPolicy, fourth.body],
  ]) {
    const response = await f.call(endpoint, { method: 'POST', cookie, headers, body });
    assert.equal(response.status, 200, response.text);
    assert.deepEqual(response.body, expected);
    assert.equal(response.headers.get('set-cookie'), null);
  }
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
  assert.equal(seen.length, notifications);
});

function managementGroupBody(
  action = 'groups',
  expectedConfigVersion = 1,
  weights = { m1: 3, m2: 1 },
) {
  return {
    action,
    expectedConfigVersion,
    redistributeRemaining: true,
    groups: [
      { id: 'g', name: 'shared', share: 1, members: ['m1', 'm2'], memberWeights: weights },
      ...['g2', 'g3', 'g4'].map((id) => ({
        id,
        name: id,
        share: 0,
        members: [],
        memberWeights: {},
      })),
    ],
  };
}

test('组管理HTTP预览沿真实owner/admin零写，原DTO/版本不持久化且无关操作键不成为命令', async (t) => {
  const f = await setup(t),
    seen = policyNotifications(f),
    before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() AS n').get().n,
    intent = {
      ...managementGroupBody('preview-groups'),
      actorKind: 'system',
      actorId: 'm2',
      recordedAt: -1,
      commandId: 'register-m1',
      fingerprint: 'a'.repeat(64),
    };
  for (const [endpoint, headers] of [
    [owner, { 'idempotency-key': 'not a management key', 'x-member-activity': '1' }],
    [
      admin,
      {
        'x-admin-token': adminToken,
        'idempotency-key': ['duplicate', 'duplicate'],
        'x-member-activity': '1',
      },
    ],
  ]) {
    const response = await f.call(endpoint, { method: 'POST', headers, body: intent });
    assert.equal(response.status, 200, response.text);
    assert.equal(response.body.configVersion, 1);
    assert.equal(response.body.redistributeRemaining, true);
    assert.deepEqual(response.body.groups, intent.groups);
    assert.deepEqual(
      response.body.users.map((u) => u.id),
      ['m1', 'm2'],
    );
    assert.equal(response.body.users[0].quota.v5Available, 75);
    assert.equal(response.body.users[1].quota.v5Available, 25);
    assert.equal(Object.hasOwn(response.body, 'receipt'), false);
    assert.equal(Object.hasOwn(response.body, 'commandId'), false);
    assert.equal(response.headers.get('set-cookie'), null);
    assert.equal(databaseState(f.db), before);
    assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
  }
  for (const action of ['preview-groups', 'groups'])
    for (const body of [
      { ...intent, action, groups: [] },
      { ...intent, action, redistributeRemaining: 'true' },
      { ...intent, action, expectedConfigVersion: 0 },
    ]) {
      const response = await f.call(owner, { method: 'POST', body });
      assert.equal(response.status, body.expectedConfigVersion === 0 ? 409 : 400, response.text);
      assert.equal(databaseState(f.db), before);
    }
  assert.equal((await f.call(owner, { method: 'POST', member: 'm2', body: intent })).status, 403);
  for (const endpoint of [summary, statistics]) {
    const unrelated = await f.call(endpoint, { method: 'POST', body: intent });
    assert.equal(unrelated.status, 404, unrelated.text);
  }
  assert.equal(seen.length, 0);
  assert.equal(f.resourceCalls, 0);
});

test('组管理HTTP提交沿原完整正文回执，后来的组版本前仍可重领且不复活旧决定', async (t) => {
  const f = await setup(t),
    seen = policyNotifications(f),
    body = {
      ...managementGroupBody(),
      actorKind: 'admin',
      actorId: 'local-admin',
      commandId: 'client-command',
      recordedAt: -1,
    },
    headers = { 'idempotency-key': 'original-group', 'x-member-activity': '1' };
  const preview = await f.call(owner, {
      method: 'POST',
      body: { ...body, action: 'preview-groups' },
    }),
    first = await f.call(owner, { method: 'POST', headers, body });
  assert.equal(first.status, 200, first.text);
  assert.deepEqual(first.body.groups, preview.body.groups);
  assert.equal(first.body.configVersion, 2);
  assert.equal(first.headers.get('set-cookie'), null);
  assert.equal(seen.length, 1);
  const seq = f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get().seq,
    stored = readRelationalCommit(f.db, seq);
  assert.equal(stored.command.actorKind, 'member');
  assert.equal(stored.command.actorId, 'm1');
  assert.equal(stored.command.recordedAt, f.clock);
  assert.notEqual(stored.command.commandId, 'client-command');
  // 独立政策不增加组版本；新组决定必须保留它，不能复活预览时的政策。
  const policy = await f.call(admin, {
    method: 'POST',
    headers: { 'x-admin-token': adminToken },
    body: { action: 'fallback-policy', enabled: true },
  });
  assert.equal(policy.status, 200, policy.text);
  const later = await f.call(admin, {
    method: 'POST',
    headers: { 'x-admin-token': adminToken, 'idempotency-key': 'later-group' },
    body: managementGroupBody('groups', 2, { m1: 1, m2: 1 }),
  });
  assert.equal(later.status, 200, later.text);
  assert.equal(later.body.configVersion, 3);
  assert.equal(
    f.db.prepare('SELECT auto_anlas_fallback FROM sharing_policy').get().auto_anlas_fallback,
    1,
  );
  const before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() AS n').get().n,
    notified = seen.length;
  const retry = await f.call(owner, { method: 'POST', headers, body });
  assert.equal(retry.status, 200, retry.text);
  assert.deepEqual(retry.body, first.body);
  assert.equal(retry.headers.get('set-cookie'), null);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
  assert.equal(seen.length, notified);
  const conflict = await f.call(owner, {
    method: 'POST',
    headers,
    body: { ...body, actorId: 'different-client-field' },
  });
  assert.equal(conflict.status, 409, conflict.text);
  assert.equal(databaseState(f.db), before);
  await logoutMemberSession(f.members, f.sessions.m1.token);
  const revoked = await f.call(owner, { method: 'POST', headers, body });
  assert.equal(revoked.status, 401, revoked.text);
  assert.equal(seen.filter(({ event }) => event.collections.includes('settings')).length, notified);
});

test('组管理HTTP预览不是权益预留，后来真实held或组版本变化使新提交拒绝', async (t) => {
  for (const mode of ['held', 'version'])
    await t.test(mode, async (t) => {
      const f = await setup(t),
        seen = policyNotifications(f),
        body = managementGroupBody();
      const preview = await f.call(owner, {
        method: 'POST',
        body: { ...body, action: 'preview-groups' },
      });
      assert.equal(preview.status, 200, preview.text);
      if (mode === 'held') {
        await f.accept('after-preview', { now: f.clock });
        f.dispatch('after-preview-0', f.clock + 1);
        f.advance(2);
        assert.ok(f.db.prepare("SELECT 1 FROM charges WHERE status='reserved'").get());
      } else {
        const changed = await f.call(admin, {
          method: 'POST',
          headers: { 'x-admin-token': adminToken },
          body: managementGroupBody('groups', 1, { m1: 1, m2: 1 }),
        });
        assert.equal(changed.status, 200, changed.text);
      }
      const before = databaseState(f.db),
        notified = seen.length,
        denied = await f.call(owner, {
          method: 'POST',
          headers: { 'idempotency-key': 'fresh-after-preview' },
          body,
        });
      assert.equal(denied.status, 409, denied.text);
      assert.equal(databaseState(f.db), before);
      assert.equal(seen.length, notified);
    });
});

test('组管理HTTP两action正文后重核Cookie/epoch/owner/ready，旧receipt也不绕授权', async (t) => {
  for (const action of ['preview-groups', 'groups'])
    for (const mode of ['cookie', 'epoch', 'owner', 'ready'])
      await t.test(action + ':' + mode, async (t) => {
        let begin;
        const f = await setup(t, { onBodyRead: () => begin?.() }),
          seen = policyNotifications(f),
          body = managementGroupBody(action),
          headers = { 'idempotency-key': 'held-group' };
        if (action === 'groups') {
          const original = await f.call(owner, { method: 'POST', headers, body });
          assert.equal(original.status, 200, original.text);
        }
        const started = new Promise((resolve) => {
            begin = resolve;
          }),
          held = await holdPolicyBody(t, f, owner, started, headers, body);
        if (mode === 'cookie') await logoutMemberSession(f.members, f.sessions.m1.token);
        if (mode === 'epoch') {
          f.db.transaction(() =>
            f.members.setMemberStatus(meta('disable-held-owner'), 'm1', 'disabled'),
          )();
          f.db.transaction(() =>
            f.members.setMemberStatus(meta('enable-held-owner'), 'm1', 'active'),
          )();
        }
        if (mode === 'owner') f.env.OWNER_USER_ID = 'm2';
        if (mode === 'ready')
          f.db.prepare("UPDATE projection_state SET status='failed' WHERE id=1").run();
        const policy = f.db.prepare('SELECT * FROM sharing_policy').all(),
          cut = f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n,
          notified = seen.filter(({ event }) => event.collections.includes('settings')).length,
          response = await held.finish();
        assert.equal(response.status, mode === 'ready' ? 500 : 401, response.text);
        assert.equal(response.headers['set-cookie'], undefined);
        assert.deepEqual(f.db.prepare('SELECT * FROM sharing_policy').all(), policy);
        assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n, cut);
        assert.equal(
          seen.filter(({ event }) => event.collections.includes('settings')).length,
          notified,
        );
      });
});

test('组管理HTTP第二钱包尾错整笔回滚零通知，健康重试一次且完整回放不复活私有认证', async (t) => {
  const f = await setup(t),
    seen = policyNotifications(f),
    body = managementGroupBody(),
    headers = { 'x-admin-token': adminToken, 'idempotency-key': 'atomic-group' };
  f.db.exec(
    "CREATE TEMP TRIGGER fail_second_group AFTER UPDATE OF v5_available ON wallets WHEN NEW.member_id='m2' AND NEW.v5_available!=OLD.v5_available BEGIN SELECT RAISE(ABORT,'second group wallet fault'); END",
  );
  const before = databaseState(f.db),
    failed = await f.call(admin, { method: 'POST', headers, body });
  assert.equal(failed.status, 500, failed.text);
  assert.equal(databaseState(f.db), before);
  assert.equal(seen.length, 0);
  f.db.exec('DROP TRIGGER fail_second_group');
  const success = await f.call(admin, { method: 'POST', headers, body });
  assert.equal(success.status, 200, success.text);
  assert.equal(seen.length, 1);
  assert.equal(seen[0].row.config_version, 2);
  assert.deepEqual(seen[0].event.collections, ['accounts', 'ledger', 'settings']);
  const saved = databaseState(f.db),
    duplicate = await f.call(admin, { method: 'POST', headers, body });
  assert.equal(duplicate.status, 200, duplicate.text);
  assert.deepEqual(duplicate.body, success.body);
  assert.equal(databaseState(f.db), saved);
  assert.equal(seen.length, 1);
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replay = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db),
    commits = f.db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all();
  let fetches = 0;
  t.mock.method(globalThis, 'fetch', async () => {
    fetches++;
    throw Error('public replay must not fetch');
  });
  for (const { seq } of commits)
    target.transaction(() => replay.replayCommit(readRelationalCommit(f.db, seq), inputs))();
  for (const table of [
    'wallets',
    'sharing_policy',
    'sharing_groups',
    'group_members',
    'account_quota_state',
    'commits',
    'domain_events',
    'stream_heads',
  ]) {
    const rows = (db) =>
      db
        .prepare('SELECT * FROM ' + table + ' ORDER BY rowid')
        .safeIntegers()
        .all();
    assert.deepEqual(rows(target), rows(f.db), table);
  }
  for (const table of [
    'api_keys',
    'password_credentials',
    'upstream_credentials',
    'member_sessions',
    'discord_bindings',
    'discord_invites',
    'member_auth_state',
  ])
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM ' + table).get().n, 0, table);
  assert.equal(
    target.prepare('SELECT COUNT(*) AS n FROM members WHERE auth_epoch IS NOT NULL').get().n,
    0,
  );
  const rebuilt = databaseState(target),
    changes = target.prepare('SELECT total_changes() AS n').get().n;
  for (const { seq } of commits)
    target.transaction(() =>
      assert.equal(replay.replayCommit(readRelationalCommit(f.db, seq), inputs).replayed, true),
    )();
  assert.equal(databaseState(target), rebuilt);
  assert.equal(target.prepare('SELECT total_changes() AS n').get().n, changes);
  assert.equal(fetches, 0);
});

test('账户兼容admin PATCH保留原宽字段/小数/别名/非keyed及mask DTO，PST轮换原子保留代理', async (t) => {
  const f = await accountPatchFixture(t),
    options = {
      route: accountAdmin,
      headers: {
        'x-admin-token': adminToken,
        'idempotency-key': 'old admin did not promise this header',
      },
    };
  const intent = {
    name: '',
    weight: 1.5,
    proxyUrl: '',
    socksProxy: '127.0.0.1:1080:synthetic-user:synthetic-secret',
    proxy: 'socks5://127.0.0.1:9999',
  };
  const result = await f.patch(intent, options);
  assert.equal(result.status, 200, result.text);
  assert.equal(result.body.id, 'a');
  assert.equal(result.body.name, '');
  assert.equal(result.body.weight, 1.5);
  assert.equal(result.body.hasProxy, true);
  assert.equal(result.body.enabled, true);
  assert.ok(!result.text.includes('synthetic-secret'));
  assert.match(result.body.proxyUrl, /\*+/);
  assert.equal(result.body.total, null);
  assert.equal(result.body.routeId, null);
  const source = new RelationalQuotaStore(f.db, f.env).prepareQuotaObservation('a'),
    before = databaseState(f.db);
  assert.equal((await f.patch({ name: 'display-only', weight: 2.25 }, options)).status, 200);
  assert.notEqual(databaseState(f.db), before);
  assert.equal(
    new RelationalQuotaStore(f.db, f.env).prepareQuotaObservation('a').connectionSourceEventId,
    source.connectionSourceEventId,
  );
  const raw = f.db
    .prepare("SELECT proxy_url FROM upstream_credentials WHERE account_id='a'")
    .get().proxy_url;
  assert.match(raw, /:1080$/, '原三个别名按||优先级选择，不把显式空串当作忽略后两个别名');
  const changed = await f.patch(
    { name: 'renamed', weight: 3.75, token: 'pst-compat-new', enabled: true },
    options,
  );
  assert.equal(changed.status, 200, changed.text);
  assert.equal(changed.body.enabled, false);
  assert.equal(f.calls.length, 0, '真正PST变化仍先pause，不按同body的enabled:true跳过核验');
  assert.equal(
    f.db.prepare("SELECT proxy_url FROM upstream_credentials WHERE account_id='a'").get().proxy_url,
    raw,
  );
  assert.equal(changed.body.weight, 3.75);
  assert.equal(changed.headers.get('set-cookie'), null);
  const text =
    JSON.stringify(f.db.prepare('SELECT * FROM commits').all()) +
    JSON.stringify(f.db.prepare('SELECT * FROM domain_events').all()) +
    JSON.stringify(f.logs);
  for (const secret of ['synthetic-secret', raw, 'pst-compat-new']) {
    assert.ok(!text.includes(secret));
  }
  assert.ok(
    !text.includes(domainDigest({ operation: `PATCH:${accountAdmin}`, body: intent })),
    '原admin非keyed私有输入不能变成公开低熵密码摘要',
  );
});

test('账户兼容admin字段actual SQL尾错外catch全表回滚无通知，撤admin/ready慢正文拒绝不变', async (t) => {
  const f = await accountPatchFixture(t),
    options = { route: accountAdmin, headers: { 'x-admin-token': adminToken } };
  f.db.exec(
    "CREATE TEMP TRIGGER compatibility_private_tail BEFORE INSERT ON upstream_credentials BEGIN SELECT RAISE(ABORT,'compat private tail'); END",
  );
  const before = databaseState(f.db),
    count = f.notifications;
  assert.equal(
    (
      await f.patch(
        { name: 'not committed', weight: 4.5, proxy: 'socks5://127.0.0.1:1080' },
        options,
      )
    ).status,
    500,
  );
  assert.equal(databaseState(f.db), before);
  assert.equal(f.notifications, count);
  f.db.exec('DROP TRIGGER compatibility_private_tail');
  assert.equal((await f.patch({ name: 'committed', weight: 4.5, proxy: '' }, options)).status, 200);
  assert.equal(f.notifications, count + 1);
  for (const mode of ['admin', 'ready']) {
    await t.test(mode, async (t) => {
      let begin;
      const s = await accountPatchFixture(t, {
          onBodyRead() {
            begin?.();
          },
        }),
        started = new Promise((r) => (begin = r));
      const endpoint = mode === 'admin' ? accountAdmin + '?adminToken=' + adminToken : accountAdmin;
      const held = await holdPolicyBody(
        t,
        s,
        endpoint,
        started,
        mode === 'admin' ? {} : { 'x-admin-token': adminToken },
        { name: 'blocked', weight: 2.5 },
        'PATCH',
      );
      if (mode === 'admin') {
        s.env.PUBLIC_MODE = 'true';
      } else {
        s.db.prepare("UPDATE projection_state SET status='building'").run();
      }
      const state = databaseState(s.db),
        notifications = s.notifications,
        result = await held.finish();
      assert.equal(result.status, mode === 'admin' ? 401 : 500, result.text);
      assert.equal(databaseState(s.db), state);
      assert.equal(s.notifications, notifications);
    });
  }
});

test('账户兼容manual quota POST保留owner原counts回执和admin原raw账户形态，刷新不启用暂停账户', async (t) => {
  const f = await accountPatchFixture(t),
    refresh = await sharedAccountPatch(f, t);
  assert.equal((await f.patch({ enabled: false })).status, 200);
  const ownerQuota = '/api/member/owner/accounts/quota',
    adminQuota = '/api/admin/accounts/quota';
  const first = await f.call(ownerQuota, {
    method: 'POST',
    body: { ids: ['a'] },
    headers: { 'idempotency-key': 'manual-owner' },
  });
  assert.equal(first.status, 200, first.text);
  assert.deepEqual(first.body, { checked: 1, ok: 1, failed: 0, ignored: 0 });
  assert.equal(f.db.prepare("SELECT enabled FROM upstream_accounts WHERE id='a'").get().enabled, 0);
  assert.equal(f.calls.length, 1);
  assert.equal(first.headers.get('set-cookie'), null);
  assert.equal((await f.patch({ token: 'pst-manual-later' })).status, 200);
  const before = databaseState(f.db),
    count = f.notifications;
  assert.deepEqual(
    (
      await f.call(ownerQuota, {
        method: 'POST',
        body: { ids: ['a'] },
        headers: { 'idempotency-key': 'manual-owner' },
      })
    ).body,
    first.body,
  );
  assert.equal(databaseState(f.db), before);
  assert.equal(f.notifications, count);
  assert.equal(f.calls.length, 1);
  const options = {
    method: 'POST',
    body: {},
    headers: { 'x-admin-token': adminToken, 'idempotency-key': 'not a promised admin key' },
  };
  const raw = await f.call(adminQuota, options);
  assert.equal(raw.status, 200, raw.text);
  assert.equal(raw.body.accounts[0].token, 'pst-manual-later');
  assert.equal(raw.body.accounts[0].proxyUrl, '');
  assert.equal(raw.body.ok, 1);
  assert.equal(f.db.prepare("SELECT enabled FROM upstream_accounts WHERE id='a'").get().enabled, 0);
  assert.equal((await f.call(adminQuota, options)).status, 200);
  assert.equal(f.calls.length, 3);
  assert.ok(
    !JSON.stringify(f.db.prepare('SELECT * FROM domain_events').all()).includes('pst-manual-later'),
  );
  assert.ok(
    !JSON.stringify(f.db.prepare('SELECT receipt FROM commits').all()).includes('pst-manual-later'),
  );
  assert.equal((await f.call(ownerQuota, { method: 'POST', body: { ids: [] } })).status, 400);
  assert.equal(
    (
      await f.call(adminQuota, {
        method: 'POST',
        body: { ids: ['another'] },
        headers: { 'x-admin-token': adminToken },
      })
    ).status,
    400,
  );
  await refresh.stop();
});

test('账户兼容manual query共用实际slot，busy无票/观察/回执且name-only不取消唯一真实成功', async (t) => {
  const f = await accountPatchFixture(t),
    refresh = await sharedAccountPatch(f, t);
  let enter, release;
  const entered = new Promise((r) => (enter = r)),
    gate = new Promise((r) => (release = r));
  f.upstream.onQuery = async () => {
    enter();
    await gate;
  };
  const automatic = refresh.refreshEnabled();
  await entered;
  try {
    const before = databaseState(f.db),
      sequence = f.quotas.readAccountQuota('a').observedSequence;
    const busy = await f.call('/api/member/owner/accounts/quota', {
      method: 'POST',
      body: { ids: ['a'] },
      headers: { 'idempotency-key': 'manual-busy' },
    });
    assert.equal(busy.status, 409, busy.text);
    assert.equal(databaseState(f.db), before);
    assert.equal(f.calls.length, 1);
    assert.equal(
      (
        await f.patch(
          { name: 'unrelated display', weight: 1.75 },
          { route: accountAdmin, headers: { 'x-admin-token': adminToken } },
        )
      ).status,
      200,
    );
    release();
    assert.deepEqual(await automatic, { checked: 1, ok: 1, failed: 0, ignored: 0 });
    assert.equal(BigInt(f.quotas.readAccountQuota('a').observedSequence), BigInt(sequence) + 1n);
  } finally {
    release();
    await automatic;
  }
});

test('账户兼容manual query等待中原连接变化：nonkeyed保留ignored，keyed无事实409，均不造旧观察', async (t) => {
  for (const mode of ['owner', 'keyed', 'admin']) {
    await t.test(mode, async (t) => {
      const f = await accountPatchFixture(t);
      await sharedAccountPatch(f, t);
      let enter, release;
      const entered = new Promise((r) => (enter = r)),
        gate = new Promise((r) => (release = r));
      f.upstream.onQuery = async () => {
        enter();
        await gate;
      };
      const pending = f.call(
        mode === 'admin' ? '/api/admin/accounts/quota' : '/api/member/owner/accounts/quota',
        {
          method: 'POST',
          body: { ids: ['a'] },
          headers:
            mode === 'admin'
              ? { 'x-admin-token': adminToken }
              : mode === 'keyed'
                ? { 'idempotency-key': 'stale-query' }
                : {},
        },
      );
      await Promise.race([
        entered,
        pending.then((r) => {
          throw Error('returned before real GET: ' + r.text);
        }),
      ]);
      try {
        assert.equal(
          (
            await f.patch(
              { proxy: 'socks5://127.0.0.1:9' },
              { route: accountAdmin, headers: { 'x-admin-token': adminToken } },
            )
          ).status,
          200,
        );
        const before = databaseState(f.db);
        release();
        const result = await pending;
        assert.equal(result.status, mode === 'keyed' ? 409 : 200, result.text);
        if (mode !== 'keyed') {
          assert.equal(result.body.ignored, 1);
          assert.equal(result.body.ok, 0);
        }
        assert.equal(databaseState(f.db), before);
        assert.equal(f.calls.length, 1);
      } finally {
        release();
        await pending;
      }
    });
  }
});

test('账户兼容admin enable真实GET期间name/weight改动不废票也不覆盖后来展示字段', async (t) => {
  const f = await accountPatchFixture(t);
  await sharedAccountPatch(f, t);
  let enter, release;
  const entered = new Promise((r) => (enter = r)),
    gate = new Promise((r) => (release = r));
  f.upstream.onQuery = async () => {
    enter();
    await gate;
  };
  const options = { route: accountAdmin, headers: { 'x-admin-token': adminToken } },
    pending = f.patch({ enabled: true }, options);
  await Promise.race([
    entered,
    pending.then((r) => {
      throw Error('no actual query: ' + r.text);
    }),
  ]);
  try {
    assert.equal((await f.patch({ name: 'later display', weight: 7.25 }, options)).status, 200);
    release();
    const result = await pending;
    assert.equal(result.status, 200, result.text);
    assert.equal(result.body.name, 'later display');
    assert.equal(result.body.weight, 7.25);
    assert.equal(f.calls.length, 1);
  } finally {
    release();
    await pending;
  }
});

async function sealCurrentAccountSource(f) {
  const sourcePath = path.join(
    f.dir,
    `account-current-${f.db.prepare('SELECT MAX(commit_seq) seq FROM commits').get().seq}.sqlite`,
  );
  await f.db.backup(sourcePath);
  const source = new Database(sourcePath);
  source.pragma('journal_mode=DELETE');
  source.close();
  return {
    ...f.selected,
    sourcePath,
    sourceSha256: relationalSnapshotSha256(sourcePath),
    finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) seq FROM commits').get().seq,
  };
}
test('账户兼容name原JS.slice80合法孤立代理必须精确返回，不能SQLite裸TEXT替换', async (t) => {
  const f = await accountPatchFixture(t),
    name = 'x'.repeat(79) + '😀',
    expected = name.slice(0, 80);
  assert.equal(expected.charCodeAt(79), 0xd83d);
  const result = await f.patch(
    { name },
    { route: accountAdmin, headers: { 'x-admin-token': adminToken } },
  );
  assert.equal(result.status, 200, result.text);
  assert.equal(result.body.name, expected);
  const raw = f.db
    .prepare(
      "SELECT payload FROM domain_events WHERE event_type='AccountConfigurationChanged' ORDER BY commit_seq DESC LIMIT 1",
    )
    .get().payload;
  assert.equal(JSON.parse(raw).displayName, expected);
  const selected = await sealCurrentAccountSource(f);
  assert.equal(
    JSON.parse(
      f.db.prepare("SELECT display_name_json FROM upstream_accounts WHERE id='a'").get()
        .display_name_json,
    ),
    expected,
  );
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replayer = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), inputs))();
  }
  await verifyAndInstallRelationalStorage(target, selected);
  assert.equal(
    JSON.parse(
      target.prepare("SELECT display_name_json FROM upstream_accounts WHERE id='a'").get()
        .display_name_json,
    ),
    expected,
  );
});

test('原admin新增第二paused账户和bulk合法小数权重保存单笔完整提交，未改当前绑定或调度', async (t) => {
  const f = await accountPatchFixture(t),
    headers = { 'x-admin-token': adminToken, 'idempotency-key': 'ignored-admin-header' },
    name = 'x'.repeat(79) + '😀',
    expected = name.slice(0, 80),
    first = await f.call('/api/admin/accounts', {
      method: 'POST',
      headers,
      body: {
        token: 'pst-synthetic-disabled-management-account',
        enabled: false,
        name,
        weight: 2.5,
      },
    });
  assert.equal(first.status, 201, first.text);
  assert.equal(first.body.enabled, false);
  assert.equal(first.body.name, expected);
  assert.equal(first.body.weight, 2.5);
  const id = first.body.id;
  assert.notEqual(id, 'a');
  assert.equal(f.db.prepare('SELECT COUNT(*) n FROM upstream_accounts').get().n, 2);
  assert.equal(f.db.prepare('SELECT bound_account_id id FROM sharing_policy').get().id, 'a');
  assert.equal(f.calls.length, 0);
  assert.equal(f.db.prepare('SELECT enabled FROM upstream_accounts WHERE id=?').get(id).enabled, 0);
  assert.equal(
    f.db.prepare('SELECT v5_available FROM wallets WHERE account_id=? LIMIT 1').get(id)
      .v5_available,
    '0',
  );
  assert.ok(!first.text.includes('pst-synthetic-disabled-management-account'));
  const afterCreate = f.notifications,
    bulk = await f.call('/api/admin/accounts', {
      method: 'PATCH',
      headers,
      body: { ids: ['a', id, 'absent'], weight: 2 },
    });
  assert.equal(bulk.status, 200, bulk.text);
  assert.equal(bulk.body.accounts.length, 2);
  assert.ok(bulk.body.accounts.every((a) => a.weight === 2));
  const seq = f.db.prepare('SELECT MAX(commit_seq) seq FROM commits').get().seq,
    saved = readRelationalCommit(f.db, String(seq));
  assert.equal(saved.events.length, 2);
  assert.ok(saved.events.every((e) => e.eventType === 'AccountConfigurationChanged'));
  assert.equal(f.notifications, afterCreate + 1);
  const before = databaseState(f.db),
    notices = f.notifications;
  f.db.exec(
    `CREATE TEMP TRIGGER fail_bulk AFTER UPDATE ON upstream_accounts WHEN NEW.id='${id}' BEGIN SELECT RAISE(ABORT,'synthetic bulk tail'); END;`,
  );
  const failed = await f.call('/api/admin/accounts', {
    method: 'PATCH',
    headers,
    body: { ids: ['a', id], weight: 3.25, enabled: false },
  });
  assert.equal(failed.status, 500, failed.text);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.notifications, notices);
  f.db.exec('DROP TRIGGER fail_bulk');
  const healthy = await f.call('/api/admin/accounts', {
    method: 'PATCH',
    headers,
    body: { accounts: ['a', id], weight: 3.25, enabled: false },
  });
  assert.equal(healthy.status, 200, healthy.text);
  assert.ok(healthy.body.accounts.every((a) => a.weight === 3.25 && !a.enabled));
  assert.equal(f.calls.length, 0);
  assert.equal(f.notifications, notices + 1);
  const selected = await sealCurrentAccountSource(f),
    target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replayer = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) seq FROM commits ORDER BY commit_seq')
    .all()) {
    const stored = readRelationalCommit(f.db, seq);
    target.transaction(() => replayer.replayCommit(stored, inputs))();
    const same = databaseState(target);
    assert.equal(target.transaction(() => replayer.replayCommit(stored, inputs))().replayed, true);
    assert.equal(databaseState(target), same);
  }
  assert.equal(target.prepare('SELECT COUNT(*) n FROM upstream_credentials').get().n, 0);
  await verifyAndInstallRelationalStorage(target, selected);
  assert.deepEqual(
    target.prepare('SELECT * FROM upstream_accounts ORDER BY id').all(),
    f.db.prepare('SELECT * FROM upstream_accounts ORDER BY id').all(),
  );
  assert.equal(
    target.prepare('SELECT pst FROM upstream_credentials WHERE account_id=?').get(id).pst,
    'pst-synthetic-disabled-management-account',
  );
});

const operatingBody = (action, receipt, amount = 1, extra = {}) => ({
  action,
  receipt,
  resource: 'anlasFixed',
  amount,
  ...extra,
});
function observeOperating(f, id, fixed) {
  const quotas = new RelationalQuotaStore(f.db, f.env),
    ticket = quotas.beginQuotaObservation('a', f.clock),
    current = quotas.readAccountQuota('a');
  f.db.transaction(() =>
    quotas.recordQuotaObservation(
      meta(id, f.clock + 1),
      ticket,
      accountQuotaResult(
        'a',
        {
          tier: 3,
          subscriptionActive: true,
          subscriptionExpiresAt: null,
          fixed,
          purchased: current.observedAnlasPurchased,
          v5UsagePercent: current.observedV5,
          v5UsageTimeUntilNextPercent: current.secondsPerPercent,
        },
        new Date(f.clock).toISOString(),
      ),
    ),
  )();
  f.advance(2);
}

test('运营资金HTTP五action复用真实bound与owner来源，回包/审计和原键原结果一致', async (t) => {
  const f = await setup(t),
    seen = policyNotifications(f),
    cases = [];
  observeOperating(f, 'funding-initial-observation', '8000');
  async function execute(body, asAdmin = false) {
    const endpoint = asAdmin ? admin : owner,
      headers = {
        'idempotency-key': body.receipt,
        'x-member-activity': '1',
        ...(asAdmin ? { 'x-admin-token': adminToken } : {}),
      },
      input = {
        ...body,
        accountId: 'forged-other-pool',
        fromUserId: 'm2',
        actorId: 'm2',
        actorKind: 'system',
        commandId: 'client-command',
        recordedAt: 0,
      },
      notified = seen.length,
      response = await f.call(endpoint, { method: 'POST', headers, body: input });
    assert.equal(response.status, 200, response.text);
    assert.deepEqual(response.body, {
      eventId: 'operating-receipt:' + body.receipt,
      duplicate: false,
    });
    assert.equal(response.headers.get('set-cookie'), null);
    assert.equal(seen.length, notified + 1);
    const stored = readRelationalCommit(
      f.db,
      f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get().seq,
    );
    assert.equal(stored.command.actorKind, asAdmin ? 'admin' : 'member');
    assert.equal(stored.command.actorId, asAdmin ? 'local-admin' : 'm1');
    assert.equal(stored.command.recordedAt, f.clock);
    assert.notEqual(stored.command.commandId, input.commandId);
    assert.ok(stored.events.every((event) => event.accountId === 'a'));
    assert.equal(stored.command.receipt.duplicate, false);
    cases.push({ endpoint, headers, input, expected: response.body });
    return response;
  }
  const wallets = f.db
    .prepare('SELECT member_id,anlas_fixed_available FROM wallets ORDER BY member_id')
    .all();
  await execute(operatingBody('operating-contribute', 'http-owner-contribute', 10));
  assert.equal(
    f.db.prepare("SELECT anlas_fixed_available AS n FROM wallets WHERE member_id='m1'").get().n,
    String(Number(wallets[0].anlas_fixed_available) - 10),
  );
  assert.equal(
    f.db.prepare("SELECT anlas_fixed_available AS n FROM wallets WHERE member_id='m2'").get().n,
    wallets[1].anlas_fixed_available,
  );
  const afterContribute = f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all();
  await execute(operatingBody('operating-register-funding', 'http-future-register', 3), true);
  assert.equal(f.db.prepare('SELECT remaining FROM pending_funding').get().remaining, '3');
  assert.deepEqual(f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(), afterContribute);
  observeOperating(f, 'funding-confirm-observation', '8000');
  await execute(operatingBody('operating-confirm-debits', 'http-confirm-debits', 2));
  assert.equal(
    f.db.prepare('SELECT pending_fixed_debit AS n FROM account_quota_state').get().n,
    '0',
  );
  assert.equal(f.db.prepare('SELECT remaining FROM pending_funding').get().remaining, '1');
  observeOperating(f, 'funding-down', '7900');
  observeOperating(f, 'funding-up', '8000');
  let lossEventId = f.db.prepare('SELECT fixed_gap_event_id AS id FROM operating_state').get().id;
  await execute(
    operatingBody('operating-reconcile-return', 'http-return-correction', 20, { lossEventId }),
    true,
  );
  lossEventId = f.db.prepare('SELECT fixed_gap_event_id AS id FROM operating_state').get().id;
  await execute(operatingBody('operating-cover-loss', 'http-cover-loss', 5, { lossEventId }));
  assert.equal(f.db.prepare('SELECT unfunded_fixed AS n FROM operating_state').get().n, '75');
  const before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() AS n').get().n,
    notified = seen.length;
  f.advance();
  for (const item of cases) {
    const response = await f.call(item.endpoint, {
      method: 'POST',
      headers: item.headers,
      body: item.input,
    });
    assert.equal(response.status, 200, response.text);
    assert.deepEqual(response.body, item.expected);
    assert.equal(response.headers.get('set-cookie'), null);
  }
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
  assert.equal(seen.length, notified);
});

test('运营资金HTTP区分原false与首笔true回执，完整正文换意图冲突、无事实新key409/无key零写', async (t) => {
  const f = await setup(t),
    body = operatingBody('operating-contribute', 'http-duplicate-receipt'),
    firstHeaders = { 'idempotency-key': 'http-funding-first' },
    first = await f.call(owner, { method: 'POST', headers: firstHeaders, body });
  assert.equal(first.status, 200, first.text);
  assert.equal(first.body.duplicate, false);
  let before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() AS n').get().n;
  const noKey = await f.call(owner, { method: 'POST', body });
  assert.equal(noKey.status, 200, noKey.text);
  assert.deepEqual(noKey.body, { ...first.body, duplicate: true });
  const noFacts = await f.call(owner, {
    method: 'POST',
    headers: { 'idempotency-key': 'http-funding-no-facts' },
    body,
  });
  assert.equal(noFacts.status, 409, noFacts.text);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
  f.advance(1);
  const duplicateHeaders = { 'idempotency-key': 'http-first-result-true' },
    duplicate = await f.call(owner, { method: 'POST', headers: duplicateHeaders, body });
  assert.equal(duplicate.status, 200, duplicate.text);
  assert.deepEqual(duplicate.body, noKey.body);
  const stored = readRelationalCommit(
    f.db,
    f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get().seq,
  );
  assert.equal(stored.command.receipt.duplicate, true);
  assert.ok(stored.events.some((event) => event.eventType === 'RefillApplied'));
  before = databaseState(f.db);
  changes = f.db.prepare('SELECT total_changes() AS n').get().n;
  const reclaimed = await f.call(owner, { method: 'POST', headers: duplicateHeaders, body }),
    conflict = await f.call(owner, {
      method: 'POST',
      headers: duplicateHeaders,
      body: { ...body, receipt: 'different-business-receipt', amount: 2 },
    }),
    old = await f.call(owner, { method: 'POST', headers: firstHeaders, body });
  assert.equal(reclaimed.status, 200, reclaimed.text);
  assert.deepEqual(reclaimed.body, duplicate.body);
  assert.equal(conflict.status, 409, conflict.text);
  assert.equal(old.status, 200, old.text);
  assert.deepEqual(old.body, first.body);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
});

test('运营资金HTTP原权限/正文边界不扩surface，五action慢正文仍复核当前授权', async (t) => {
  const actions = [
    'operating-contribute',
    'operating-register-funding',
    'operating-cover-loss',
    'operating-reconcile-return',
    'operating-confirm-debits',
  ];
  for (const [index, mode] of ['cookie', 'epoch', 'owner', 'ready', 'admin'].entries()) {
    await t.test(mode, async (t) => {
      let begin;
      const f = await setup(t, { onBodyRead: () => begin?.() }),
        seen = policyNotifications(f),
        body = operatingBody(actions[index], 'http-held-funding'),
        endpoint = mode === 'admin' ? admin + '?adminToken=' + adminToken : owner;
      if (mode === 'cookie') {
        const first = await f.call(owner, {
          method: 'POST',
          headers: { 'idempotency-key': 'held-policy' },
          body,
        });
        assert.equal(first.status, 200, first.text);
      }
      const started = new Promise((resolve) => {
          begin = resolve;
        }),
        held = await holdPolicyBody(t, f, endpoint, started, {}, body);
      if (mode === 'cookie') {
        await logoutMemberSession(f.members, f.sessions.m1.token);
      }
      if (mode === 'epoch') {
        f.db.transaction(() =>
          f.members.setMemberStatus(meta('funding-disable'), 'm1', 'disabled'),
        )();
        f.db.transaction(() => f.members.setMemberStatus(meta('funding-enable'), 'm1', 'active'))();
      }
      if (mode === 'owner') {
        f.env.OWNER_USER_ID = 'm2';
      }
      if (mode === 'ready') {
        f.db.prepare("UPDATE projection_state SET status='failed'").run();
      }
      if (mode === 'admin') {
        f.env.PUBLIC_MODE = 'true';
      }
      const cut = f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n,
        financial = domainDigest(
          ['wallets', 'account_quota_state', 'operating_state', 'pending_funding'].map((table) =>
            f.db.prepare('SELECT * FROM ' + table + ' ORDER BY rowid').all(),
          ),
        ),
        notified = seen.length,
        result = await held.finish();
      assert.equal(result.status, mode === 'ready' ? 500 : 401, result.text);
      assert.equal(result.headers['set-cookie'], undefined);
      assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM commits').get().n, cut);
      assert.equal(
        domainDigest(
          ['wallets', 'account_quota_state', 'operating_state', 'pending_funding'].map((table) =>
            f.db.prepare('SELECT * FROM ' + table + ' ORDER BY rowid').all(),
          ),
        ),
        financial,
      );
      assert.ok(
        seen
          .slice(notified)
          .every(({ event }) => event.collections.every((name) => name === 'sessions')),
      );
    });
  }
  const f = await setup(t),
    before = databaseState(f.db),
    body = operatingBody(actions[0], 'http-invalid-funding');
  for (const [endpoint, options, status] of [
    [owner, { member: 'm2', body }, 403],
    [admin, { body }, 403],
    [owner, { body: { ...body, userId: 'm2' } }, 403],
    [owner, { body: { ...body, action: 'operating-new-action' } }, 400],
    [owner, { body: { ...body, amount: -1 } }, 409],
    [owner, { body: { ...body, resource: 'unknown' } }, 409],
    [owner, { body: { ...body, receipt: 'short' } }, 409],
    [owner, { body: { ...body, extra: 'x'.repeat(32768) } }, 413],
    [summary, { body }, 404],
    [statistics, { body }, 404],
  ]) {
    const result = await f.call(endpoint, { method: 'POST', ...options });
    assert.equal(result.status, status, result.text);
    assert.equal(databaseState(f.db), before);
  }
});

test('运营资金HTTP第二钱包SQL尾错全回滚无幽灵通知，retry单通知且完整公共回放无认证/网络', async (t) => {
  const f = await setup(t),
    seen = policyNotifications(f);
  observeOperating(f, 'funding-fault-down', '7900');
  observeOperating(f, 'funding-fault-up', '8000');
  const body = operatingBody('operating-register-funding', 'http-funding-atomic', 10, {
      destination: 'members',
      fromUnattributed: true,
    }),
    headers = { 'x-admin-token': adminToken, 'idempotency-key': 'http-funding-atomic-key' };
  f.db.exec(
    "CREATE TEMP TRIGGER fail_second_funding AFTER UPDATE OF anlas_fixed_available ON wallets WHEN NEW.member_id='m2' AND NEW.anlas_fixed_available!=OLD.anlas_fixed_available BEGIN SELECT RAISE(ABORT,'second funding wallet fault'); END",
  );
  const before = databaseState(f.db),
    notified = seen.length,
    failed = await f.call(admin, { method: 'POST', headers, body });
  assert.equal(failed.status, 500, failed.text);
  assert.equal(databaseState(f.db), before);
  assert.equal(seen.length, notified);
  f.db.exec('DROP TRIGGER fail_second_funding');
  const success = await f.call(admin, { method: 'POST', headers, body });
  assert.equal(success.status, 200, success.text);
  assert.equal(success.body.duplicate, false);
  assert.equal(seen.length, notified + 1);
  assert.equal(seen.at(-1).event.collections.includes('ledger'), true);
  const saved = databaseState(f.db),
    duplicate = await f.call(admin, { method: 'POST', headers, body });
  assert.equal(duplicate.status, 200, duplicate.text);
  assert.deepEqual(duplicate.body, success.body);
  assert.equal(databaseState(f.db), saved);
  assert.equal(seen.length, notified + 1);
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replay = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db),
    commits = f.db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all();
  t.mock.method(globalThis, 'fetch', async () => {
    throw Error('public funding replay must not fetch');
  });
  for (const { seq } of commits) {
    target.transaction(() => replay.replayCommit(readRelationalCommit(f.db, seq), inputs))();
  }
  for (const table of [
    'commits',
    'domain_events',
    'stream_heads',
    'wallets',
    'pending_funding',
    'operating_state',
    'account_quota_state',
  ]) {
    const rows = (db) =>
      db
        .prepare('SELECT * FROM ' + table + ' ORDER BY rowid')
        .safeIntegers()
        .all();
    assert.deepEqual(rows(target), rows(f.db), table);
  }
  for (const table of [
    'api_keys',
    'password_credentials',
    'upstream_credentials',
    'member_sessions',
    'discord_bindings',
    'discord_invites',
    'member_auth_state',
  ]) {
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM ' + table).get().n, 0, table);
  }
  assert.equal(
    target.prepare('SELECT COUNT(*) AS n FROM members WHERE auth_epoch IS NOT NULL').get().n,
    0,
  );
  const replayed = databaseState(target),
    changes = target.prepare('SELECT total_changes() AS n').get().n;
  for (const { seq } of commits) {
    target.transaction(() =>
      assert.equal(replay.replayCommit(readRelationalCommit(f.db, seq), inputs).replayed, true),
    )();
  }
  assert.equal(databaseState(target), replayed);
  assert.equal(target.prepare('SELECT total_changes() AS n').get().n, changes);
});

const rightsTransfer = (version = 1, amount = 100) => ({
  action: 'transfer-quota',
  expectedConfigVersion: version,
  fromUserId: 'm2',
  toUserId: 'm1',
  resource: 'anlasFixed',
  amount,
});
const rightsRemoval = (action = 'delete-member', version = 1) => ({
  action,
  expectedConfigVersion: version,
  userId: 'm2',
  transferToUserId: 'm1',
});
const rightsNoDiscord = () => {
  throw Error('unexpected rights-test Discord IO');
};

test('权益POST真实转账及只读删除预览沿原DTO，重领旧回执不再转钱或复活已删除认证', async (t) => {
  const f = await setup(t, { discordFetch: rightsNoDiscord }),
    seen = policyNotifications(f),
    beforeWallets = f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(),
    headers = { 'idempotency-key': 'rights-transfer' },
    first = await f.call(owner, { method: 'POST', headers, body: rightsTransfer() });
  assert.equal(first.status, 200, first.text);
  assert.equal(first.body.fromUserId, 'm2');
  assert.equal(first.body.toUserId, 'm1');
  assert.deepEqual(first.body.transferred, { v5: 0, anlasFixed: 100, anlasPurchased: 0 });
  assert.equal(first.body.configVersion, 2);
  const firstCommit = readRelationalCommit(
      f.db,
      f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) seq FROM commits').get().seq,
    ),
    movement = firstCommit.events.find((e) => e.eventType === 'FundsTransferred');
  assert.equal(firstCommit.command.actorKind, 'member');
  assert.equal(firstCommit.command.actorId, 'm1');
  assert.equal(movement.payload.fromMemberId, 'm2');
  assert.equal(movement.payload.toMemberId, 'm1');
  assert.deepEqual(movement.payload.resources, { v5: '0', anlasFixed: '100', anlasPurchased: '0' });
  for (const row of beforeWallets) {
    const after = f.db.prepare('SELECT * FROM wallets WHERE member_id=?').get(row.member_id);
    assert.equal(
      Number(after.anlas_fixed_available) - Number(row.anlas_fixed_available),
      row.member_id === 'm1' ? 100 : -100,
    );
    assert.equal(after.v5_available, row.v5_available);
    assert.equal(after.anlas_purchased_available, row.anlas_purchased_available);
  }
  const once = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() n').get().n;
  assert.deepEqual(
    (await f.call(owner, { method: 'POST', headers, body: rightsTransfer() })).body,
    first.body,
  );
  assert.equal(
    (await f.call(owner, { method: 'POST', headers, body: rightsTransfer(1, 101) })).status,
    409,
  );
  assert.equal(databaseState(f.db), once);
  assert.equal(f.db.prepare('SELECT total_changes() n').get().n, changes);
  assert.equal(seen.length, 1);
  const original = RelationalQuotaStore.prototype.manageMemberQuota;
  t.mock.method(
    RelationalQuotaStore.prototype,
    'manageMemberQuota',
    function (metadata, input, ...rest) {
      assert.equal(f.db.inTransaction, true);
      assert.equal(rest[1], f.controls);
      if (input.action !== 'preview-delete-member')
        return original.call(this, metadata, input, ...rest);
      f.db.pragma('query_only=ON');
      try {
        return original.call(this, metadata, input, ...rest);
      } finally {
        f.db.pragma('query_only=OFF');
      }
    },
  );
  const previewBody = rightsRemoval('preview-delete-member', 2),
    preview = await f.call(owner, {
      method: 'POST',
      body: previewBody,
      headers: { 'idempotency-key': 'not a command key' },
    });
  assert.equal(preview.status, 200, preview.text);
  assert.equal(preview.body.configVersion, 2);
  assert.equal(preview.headers.get('set-cookie'), null);
  assert.deepEqual((await f.call(owner, { method: 'POST', body: previewBody })).body, preview.body);
  assert.equal(databaseState(f.db), once);
  assert.equal(f.db.prepare('SELECT total_changes() n').get().n, changes);
  assert.equal(seen.length, 1);
  const removed = await f.call(owner, {
    method: 'POST',
    body: rightsRemoval('delete-member', 2),
    headers: { 'idempotency-key': 'rights-delete' },
  });
  assert.equal(removed.status, 200, removed.text);
  assert.deepEqual({ ...removed.body, configVersion: 2 }, preview.body);
  assert.equal(removed.body.configVersion, 3);
  assert.equal(f.members.readMember('m2').status, 'removed');
  assert.equal(f.db.prepare("SELECT auth_epoch FROM members WHERE id='m2'").get().auth_epoch, null);
  assert.equal(
    f.db.prepare("SELECT COUNT(*) n FROM member_sessions WHERE member_id='m2'").get().n,
    0,
  );
  assert.equal(await authenticateMemberApiKey(f.members, keys.m2), null);
  assert.equal(
    await authenticateMemberSession(f.members, f.sessions.m2.token, f.env, f.clock),
    null,
  );
  const deleted = databaseState(f.db),
    afterChanges = f.db.prepare('SELECT total_changes() n').get().n;
  assert.deepEqual(
    (
      await f.call(owner, {
        method: 'POST',
        body: rightsRemoval('delete-member', 2),
        headers: { 'idempotency-key': 'rights-delete' },
      })
    ).body,
    removed.body,
  );
  assert.equal(databaseState(f.db), deleted);
  assert.equal(f.db.prepare('SELECT total_changes() n').get().n, afterChanges);
  assert.equal(seen.length, 2);
  f.db.transaction(() =>
    f.members.setMemberStatus(meta('rights-owner-disabled'), 'm1', 'disabled'),
  )();
  assert.equal(
    (await f.call(owner, { method: 'POST', headers, body: rightsTransfer() })).status,
    401,
  );
  assertMemberManagementPublicReplay(t, f);
});

test('权益POST保持原金额/角色/版本与owner自删限制，本机admin保留应急删除', async (t) => {
  const f = await setup(t, { discordFetch: rightsNoDiscord }),
    before = databaseState(f.db);
  for (const [body, status] of [
    [rightsTransfer(1, 0.5), 400],
    [rightsTransfer(1, 0), 400],
    [{ ...rightsTransfer(), toUserId: 'm2' }, 400],
    [{ ...rightsTransfer(), fromUserId: 'missing' }, 404],
    [rightsTransfer(0), 409],
    [rightsTransfer(2), 409],
    [{ ...rightsTransfer(), actorKind: 'admin' }, 400],
    [{ ...rightsRemoval('preview-delete-member'), userId: 'm1', transferToUserId: 'm2' }, 409],
    [{ ...rightsRemoval(), userId: 'm1', transferToUserId: 'm2' }, 409],
  ]) {
    const result = await f.call(owner, { method: 'POST', body });
    assert.equal(result.status, status, result.text);
    assert.equal(databaseState(f.db), before);
  }
  assert.equal(
    (await f.call(owner, { method: 'POST', member: 'm2', body: rightsTransfer() })).status,
    403,
  );
  const body = { ...rightsRemoval(), userId: 'm1', transferToUserId: 'm2' },
    result = await f.call(admin, {
      method: 'POST',
      body,
      headers: { 'x-admin-token': adminToken, 'idempotency-key': 'emergency-owner-removal' },
    });
  assert.equal(result.status, 200, result.text);
  assert.equal(result.body.deletedUserId, 'm1');
  assert.equal(await authenticateMemberApiKey(f.members, keys.m1), null);
  assert.equal((await f.call(owner)).status, 401);
  const source = readRelationalCommit(
    f.db,
    f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) seq FROM commits').get().seq,
  );
  assert.equal(source.command.actorKind, 'admin');
  assert.equal(source.command.actorId, 'local-admin');
  assertMemberManagementPublicReplay(t, f);
});

test('权益POST真正waiting/held和同一个待收尾physical Map都阻止调整，释放后沿原owner执行', async (t) => {
  for (const phase of ['waiting', 'held', 'physical']) {
    await t.test(phase, async (t) => {
      const f = await setup(t, { discordFetch: rightsNoDiscord });
      let controls, control;
      if (phase === 'physical') {
        // 原控制owner创建真实待settle promise；已完成SQL不能替它释放当前共享Map。
        assert.equal(
          f.db.prepare("SELECT status FROM generation_jobs WHERE id='existing-0'").get().status,
          'done',
        );
        controls = createGenerationControls({
          controls: f.controls,
          timeoutDelay: () => 60000,
          schedule() {},
        });
        control = controls.createRunningJobControl({ id: 'existing-0', accountId: 'a' });
        t.after(() => controls.finishRunningJobControl('existing-0', control));
      } else {
        await f.accept('rights-duty', { memberId: 'm2' });
        if (phase === 'held') f.dispatch('rights-duty-0');
      }
      const before = databaseState(f.db);
      for (const body of [
        rightsTransfer(),
        rightsRemoval('preview-delete-member'),
        rightsRemoval(),
      ]) {
        const result = await f.call(admin, {
          method: 'POST',
          body,
          headers: { 'x-admin-token': adminToken },
        });
        assert.equal(result.status, 409, result.text);
        assert.equal(databaseState(f.db), before);
      }
      if (phase === 'physical') {
        assert.equal(f.controls.size, 1);
        controls.finishRunningJobControl('existing-0', control);
        await control.done;
        const result = await f.call(owner, { method: 'POST', body: rightsTransfer() });
        assert.equal(result.status, 200, result.text);
      }
    });
  }
});

test('权益POST末尾真实SQL失败全表回滚无通知，同键健康重试和完整公共回放', async (t) => {
  for (const action of ['transfer-quota', 'delete-member']) {
    await t.test(action, async (t) => {
      const f = await setup(t, { discordFetch: rightsNoDiscord }),
        seen = policyNotifications(f),
        body = action === 'transfer-quota' ? rightsTransfer() : rightsRemoval(),
        headers = { 'idempotency-key': 'rights-rollback' };
      f.db.exec(
        action === 'transfer-quota'
          ? "CREATE TRIGGER fail_rights_tail AFTER UPDATE ON wallets WHEN NEW.member_id='m1' BEGIN SELECT RAISE(ABORT,'synthetic rights target tail'); END"
          : "CREATE TRIGGER fail_rights_tail AFTER DELETE ON member_sessions WHEN OLD.member_id='m2' BEGIN SELECT RAISE(ABORT,'synthetic rights private tail'); END",
      );
      const before = databaseState(f.db),
        failed = await f.call(owner, { method: 'POST', headers, body });
      assert.equal(failed.status, 500, failed.text);
      assert.equal(databaseState(f.db), before);
      assert.equal(seen.length, 0);
      f.db.exec('DROP TRIGGER fail_rights_tail');
      const result = await f.call(owner, { method: 'POST', headers, body });
      assert.equal(result.status, 200, result.text);
      assert.equal(seen.length, 1);
      const once = databaseState(f.db),
        changes = f.db.prepare('SELECT total_changes() n').get().n;
      assert.deepEqual((await f.call(owner, { method: 'POST', headers, body })).body, result.body);
      assert.equal(databaseState(f.db), once);
      assert.equal(f.db.prepare('SELECT total_changes() n').get().n, changes);
      assert.equal(seen.length, 1);
      assertMemberManagementPublicReplay(t, f);
    });
  }
});

test('权益POST慢正文后的当前权限仍先于原receipt，preview不因旧header变成写入', async (t) => {
  for (const mode of [
    'transfer-epoch',
    'delete-logout',
    'preview-owner',
    'preview-ready',
    'admin-query',
  ]) {
    await t.test(mode, async (t) => {
      let begin;
      const f = await setup(t, { discordFetch: rightsNoDiscord, onBodyRead: () => begin?.() }),
        body = mode.startsWith('preview')
          ? rightsRemoval('preview-delete-member')
          : mode.startsWith('delete')
            ? rightsRemoval()
            : rightsTransfer(),
        endpoint = mode === 'admin-query' ? admin + '?adminToken=' + adminToken : owner,
        headers = { 'idempotency-key': 'rights-old-receipt' };
      if (!mode.startsWith('preview')) {
        const saved = await f.call(endpoint, { method: 'POST', body, headers });
        assert.equal(saved.status, 200, saved.text);
      }
      const started = new Promise((resolve) => {
          begin = resolve;
        }),
        held = await holdPolicyBody(t, f, endpoint, started, headers, body);
      if (mode === 'transfer-epoch') {
        f.db.transaction(() =>
          f.members.setMemberStatus(meta('rights-disable'), 'm1', 'disabled'),
        )();
        f.db.transaction(() => f.members.setMemberStatus(meta('rights-enable'), 'm1', 'active'))();
      } else if (mode === 'delete-logout') {
        await logoutMemberSession(f.members, f.sessions.m1.token);
      } else if (mode === 'preview-owner') {
        f.env.OWNER_USER_ID = 'm2';
      } else if (mode === 'preview-ready') {
        f.db.prepare("UPDATE projection_state SET status='failed'").run();
      } else {
        f.env.PUBLIC_MODE = 'true';
      }
      const cut = f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) seq FROM commits').get().seq,
        wallets = f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(),
        result = await held.finish();
      assert.equal(result.status, mode === 'preview-ready' ? 500 : 401, result.text);
      assert.equal(result.headers['set-cookie'], undefined);
      assert.equal(
        f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) seq FROM commits').get().seq,
        cut,
      );
      assert.deepEqual(f.db.prepare('SELECT * FROM wallets ORDER BY member_id').all(), wallets);
    });
  }
});

const credentials = (patch = {}) => ({
  action: 'credentials',
  username: 'new.credentials.member',
  password: 'synthetic-credentials-password',
  ...patch,
});
async function credentialGroups(f, env) {
  const quotas = new RelationalQuotaStore(f.db, env),
    groups = [
      ...quotas.readQuotaConfiguration().groups,
      ...['g2', 'g3', 'g4'].map((id) => ({
        id,
        name: id,
        share: 0,
        members: [],
        memberWeights: {},
      })),
    ];
  f.db.transaction(() =>
    quotas.configureGroups(
      {
        ...meta('credential-fixture-groups', NOW + 20),
        actorKind: 'admin',
        actorId: 'local-admin',
      },
      { expectedConfigVersion: 1, groups },
      null,
    ),
  )();
}
const passwordEnvironment = {
  DISCORD_CLIENT_ID: '',
  DISCORD_CLIENT_SECRET: '',
  DISCORD_GUILD_ID: '',
};
const credentialPost = (f, body, { endpoint, headers = {}, ...options } = {}) => {
  const target = endpoint ?? (f.env.DISCORD_GUILD_ID ? owner : admin);
  return f.call(target, {
    method: 'POST',
    body,
    headers: {
      ...(target.startsWith('/api/admin/') ? { 'x-admin-token': adminToken } : {}),
      ...headers,
    },
    ...options,
  });
};

test('credentials真实POST创建仅私有密码和Key，原非keyedheader不造公共secret回执，held时不入组创建仍允许', async (t) => {
  const f = await setup(t, { env: passwordEnvironment, beforeReady: credentialGroups }),
    known = credentials(),
    header = { 'idempotency-key': 'not a keyed command', 'x-member-activity': '1' };
  const first = await credentialPost(f, known, { headers: header });
  assert.equal(first.status, 200, first.text);
  assert.deepEqual(first.body, { id: first.body.id, username: known.username });
  assert.equal(first.headers.get('set-cookie'), null);
  const key = f.members.readMemberApiKey(first.body.id);
  assert.match(key, /^STA1N-[A-Za-z0-9_-]{24}$/);
  assert.equal(
    (await f.members.loginMemberPassword(known.username, known.password)).id,
    first.body.id,
  );
  const cut = f.db.prepare('SELECT MAX(commit_seq) seq FROM commits').get().seq,
    before = databaseState(f.db);
  assert.equal(
    (await credentialPost(f, known, { headers: header })).status,
    409,
    'new HTTP create is not a saved private receipt',
  );
  assert.equal(databaseState(f.db), before);
  f.accept('credentials-held', { memberId: 'm2' });
  assert.ok(f.dispatch('credentials-held-0', f.clock).dispatch);
  assert.equal(f.generations.readCharge('credentials-held-0').status, 'reserved');
  const created = await credentialPost(f, credentials({ username: 'held.unassigned' }));
  assert.equal(created.status, 200, created.text);
  assert.equal(
    f.db.prepare('SELECT 1 FROM group_members WHERE member_id=?').get(created.body.id),
    undefined,
  );
  const publicText =
    JSON.stringify(f.db.prepare('SELECT * FROM commits').all()) +
    JSON.stringify(f.db.prepare('SELECT * FROM domain_events').all());
  for (const value of [known.password, key, 'password_hash', 'salt', 'expectedAuthEpoch']) {
    assert.ok(!publicText.includes(value));
  }
  assert.ok(
    !publicText.includes(domainDigest(known)),
    'public fingerprint must not be a dictionary oracle for a private password',
  );
  assert.ok(f.db.prepare('SELECT MAX(commit_seq) seq FROM commits').get().seq > cut);
});

test('credentials新建group保留原完整owner和held拒绝，组失败或第二owner SQL尾错全表原子回滚', async (t) => {
  const f = await setup(t, { env: passwordEnvironment, beforeReady: credentialGroups });
  let seen = 0;
  f.events.on('commit', () => seen++);
  let before = databaseState(f.db);
  for (const groupId of ['missing', 5]) {
    const result = await credentialPost(f, credentials({ groupId }));
    assert.equal(result.status, 400, result.text);
    assert.equal(databaseState(f.db), before);
    assert.equal(seen, 0);
  }
  f.accept('group-credentials-held', { memberId: 'm2' });
  assert.ok(f.dispatch('group-credentials-held-0', f.clock).dispatch);
  assert.equal(f.generations.readCharge('group-credentials-held-0').status, 'reserved');
  before = databaseState(f.db);
  assert.equal((await credentialPost(f, credentials({ groupId: 'g' }))).status, 409);
  assert.equal(databaseState(f.db), before);
  assert.equal(seen, 0);
  f.fail('group-credentials-held-0', { aborted: false }, f.clock + 1);
  f.advance(2);
  f.db.exec(
    "CREATE TEMP TRIGGER credential_group_tail AFTER UPDATE ON sharing_policy BEGIN SELECT RAISE(ABORT,'credential group tail'); END",
  );
  before = databaseState(f.db);
  assert.equal((await credentialPost(f, credentials({ groupId: 'g' }))).status, 500);
  assert.equal(databaseState(f.db), before);
  assert.equal(seen, 0);
  f.db.exec('DROP TRIGGER credential_group_tail');
  const result = await credentialPost(f, credentials({ groupId: 'g' }));
  assert.equal(result.status, 200, result.text);
  assert.deepEqual(result.body, {
    id: result.body.id,
    username: 'new.credentials.member',
    groupId: 'g',
  });
  assert.equal(
    f.db.prepare('SELECT group_id FROM group_members WHERE member_id=?').get(result.body.id)
      .group_id,
    'g',
  );
  assert.equal(seen, 1, 'registration and group commits publish once at real outer boundary');
  assert.equal(
    (
      await f.members.loginMemberPassword(
        'new.credentials.member',
        'synthetic-credentials-password',
      )
    ).id,
    result.body.id,
  );
});

test('credentials私有更新保留Key且撤Cookie，重复HTTP为新重置、原prepared CAS不能再装旧认证', async (t) => {
  const f = await setup(t, { env: passwordEnvironment }),
    key = f.members.readMemberApiKey('m2'),
    body = credentials({ userId: 'm2', username: 'updated.credentials' }),
    cut = f.db.prepare('SELECT MAX(commit_seq) seq FROM commits').get().seq;
  let count = 0;
  f.events.on('commit', () => count++);
  const result = await credentialPost(f, body, {
    headers: { 'idempotency-key': 'ignored-private-key' },
  });
  assert.equal(result.status, 200, result.text);
  assert.deepEqual(result.body, { id: 'm2', username: 'updated.credentials' });
  assert.equal(f.members.readMemberApiKey('m2'), key);
  assert.equal(
    await authenticateMemberSession(f.members, f.sessions.m2.token, f.env, f.clock),
    null,
  );
  assert.equal((await f.members.loginMemberPassword(body.username, body.password)).id, 'm2');
  assert.equal(
    f.db.prepare('SELECT MAX(commit_seq) seq FROM commits').get().seq,
    cut,
    'password operation has no invented public commit',
  );
  const epoch = f.db.prepare("SELECT auth_epoch FROM members WHERE id='m2'").get().auth_epoch;
  assert.equal(
    (await credentialPost(f, body, { headers: { 'idempotency-key': 'ignored-private-key' } }))
      .status,
    200,
  );
  assert.notEqual(
    f.db.prepare("SELECT auth_epoch FROM members WHERE id='m2'").get().auth_epoch,
    epoch,
  );
  assert.equal(count, 2);
  const store = new RelationalMemberStore(f.db, f.env),
    prepared = await store.prepareMemberCredentials('m2', {
      username: 'private.prepared',
      password: 'synthetic-new-prepared-password',
    }),
    actor = await authenticateMemberApiKey(store, keys.m1);
  f.db.transaction(() => store.setMemberCredentials(prepared, actor))();
  const before = databaseState(f.db);
  assert.throws(() => f.db.transaction(() => store.setMemberCredentials(prepared, actor))(), {
    statusCode: 401,
  });
  assert.equal(databaseState(f.db), before);
});

test('credentials Discord新建用原随机密码且更新409，私有owner拒undefined/非owner/旧actor epoch', async (t) => {
  const f = await setup(t),
    body = credentials({ password: 'short client password' }),
    result = await credentialPost(f, body);
  assert.equal(result.status, 200, result.text);
  await assert.rejects(f.members.loginMemberPassword(body.username, body.password), {
    statusCode: 401,
  });
  assert.equal((await credentialPost(f, credentials({ userId: 'm2' }))).status, 409);
  const store = new RelationalMemberStore(f.db, f.env),
    prepared = await store.prepareMemberCredentials('m2', {
      username: 'private.owner.checked',
      password: 'synthetic-owner-checked-password',
    }),
    actor = await authenticateMemberApiKey(f.members, keys.m1),
    other = await authenticateMemberApiKey(f.members, keys.m2),
    before = databaseState(f.db);
  for (const candidate of [
    undefined,
    other,
    { ...actor, memberAuth: { ...actor.memberAuth, version: '0'.repeat(32) } },
  ]) {
    assert.throws(() => f.db.transaction(() => store.setMemberCredentials(prepared, candidate))(), {
      statusCode: 403,
    });
    assert.equal(databaseState(f.db), before);
  }
  f.db.transaction(() => store.setMemberCredentials(prepared, null))();
  assert.equal(
    (
      await f.members.loginMemberPassword(
        'private.owner.checked',
        'synthetic-owner-checked-password',
      )
    ).id,
    'm2',
  );
  assert.equal((await credentialPost(f, credentials({ note: 'old note field' }))).status, 400);
});

test('credentials KDF完成后重核Cookie/owner/ready和target epoch，正文及密码工作均在SQL外', async (t) => {
  for (const mode of ['admin-query', 'ready', 'target']) {
    await t.test(mode, async (t) => {
      const f = await setup(t, { env: passwordEnvironment }),
        original = RelationalMemberStore.prototype.prepareMemberCredentials;
      let enter, release;
      const entered = new Promise((r) => (enter = r)),
        gate = new Promise((r) => (release = r));
      const mock = t.mock.method(
        RelationalMemberStore.prototype,
        'prepareMemberCredentials',
        async function (...args) {
          assert.equal(f.db.inTransaction, false);
          const value = await original.apply(this, args);
          enter();
          await gate;
          return value;
        },
      );
      const pending = credentialPost(
        f,
        credentials({ userId: 'm2' }),
        mode === 'admin-query'
          ? { endpoint: admin + '?adminToken=' + adminToken, headers: { 'x-admin-token': '' } }
          : {},
      );
      await Promise.race([
        entered,
        pending.then((r) => {
          throw Error('returned before private work: ' + r.text);
        }),
      ]);
      try {
        if (mode === 'admin-query') {
          f.env.PUBLIC_MODE = 'true';
        }
        if (mode === 'ready') {
          f.db.prepare("UPDATE projection_state SET status='building'").run();
        }
        if (mode === 'target') {
          const newer = await original.call(f.members, 'm2', {
            username: 'newer.current',
            password: 'synthetic-newer-private-password',
          });
          f.db.transaction(() => f.members.setMemberCredentials(newer, null))();
        }
        const before = databaseState(f.db);
        release();
        const result = await pending;
        assert.equal(
          result.status,
          { 'admin-query': 401, ready: 500, target: 401 }[mode],
          result.text,
        );
        assert.equal(databaseState(f.db), before);
      } finally {
        release();
        mock.mock.restore();
      }
    });
  }
});

test('credentials慢正文和新建KDF期间撤Cookie/epoch/owner/ready均无私有安装，不接受body actor自证', async (t) => {
  for (const mode of ['cookie', 'epoch', 'owner', 'ready']) {
    await t.test('new KDF ' + mode, async (t) => {
      const f = await setup(t),
        original = RelationalMemberStore.prototype.readManagementMemberProfiles;
      const changed =
        mode === 'epoch'
          ? await f.members.prepareMemberCredentials('m1', {
              username: 'new.owner.epoch',
              password: 'synthetic-owner-new-password',
            })
          : null;
      let before, resolve, reject;
      const entered = new Promise((a, b) => {
        resolve = a;
        reject = b;
      });
      const mock = t.mock.method(
        RelationalMemberStore.prototype,
        'readManagementMemberProfiles',
        function () {
          const result = original.call(this);
          queueMicrotask(async () => {
            try {
              assert.equal(
                f.db.inTransaction,
                false,
                'new password work starts after original auth commit',
              );
              if (mode === 'cookie') {
                await logoutMemberSession(f.members, f.sessions.m1.token);
              }
              if (mode === 'epoch') {
                f.db.transaction(() => f.members.setMemberCredentials(changed, null))();
              }
              if (mode === 'owner') {
                f.env.OWNER_USER_ID = 'm2';
                await authenticateMemberSession(f.members, f.sessions.m1.token, f.env, f.clock);
              }
              if (mode === 'ready') {
                f.db.prepare("UPDATE projection_state SET status='building'").run();
              }
              before = databaseState(f.db);
              resolve();
            } catch (error) {
              reject(error);
            }
          });
          return result;
        },
      );
      const pending = credentialPost(
        f,
        credentials({
          member: { id: 'm1', enabled: true },
          actorKind: 'admin',
          actorId: 'local-admin',
        }),
      );
      try {
        await Promise.race([
          entered,
          pending.then((r) => {
            throw Error('no preparation observed: ' + r.text);
          }),
        ]);
        const result = await pending;
        assert.equal(result.status, mode === 'ready' ? 500 : 401, result.text);
        assert.equal(databaseState(f.db), before);
      } finally {
        mock.mock.restore();
      }
    });
  }
  let begin;
  const f = await setup(t, { onBodyRead: () => begin?.() }),
    body = credentials();
  const started = new Promise((r) => (begin = r)),
    held = await holdPolicyBody(t, f, owner, started, {}, body);
  await logoutMemberSession(f.members, f.sessions.m1.token);
  const before = databaseState(f.db),
    response = await held.finish();
  assert.equal(response.status, 401, response.text);
  assert.equal(databaseState(f.db), before);
});

test('credentials实际private SQL尾错外catch全表回滚无通知，健康新重置只通知一次', async (t) => {
  const f = await setup(t, { env: passwordEnvironment }),
    first = credentials({ userId: 'm2', username: 'tail.original' });
  assert.equal((await credentialPost(f, first)).status, 200);
  const session = await createMemberSession(
      f.members,
      await f.members.loginMemberPassword(first.username, first.password),
      f.env,
      f.clock,
    ),
    next = credentials({
      userId: 'm2',
      username: 'tail.current',
      password: 'synthetic-current-password',
    });
  let seen = 0;
  f.events.on('commit', () => seen++);
  f.db.exec(
    "CREATE TEMP TRIGGER credential_private_tail AFTER DELETE ON member_sessions WHEN OLD.member_id='m2' BEGIN SELECT RAISE(ABORT,'credential private tail'); END",
  );
  const before = databaseState(f.db),
    cut = f.db.prepare('SELECT MAX(commit_seq) seq FROM commits').get().seq;
  assert.equal(
    (await credentialPost(f, next, { headers: { 'idempotency-key': 'still-not-keyed' } })).status,
    500,
  );
  assert.equal(databaseState(f.db), before);
  assert.equal(seen, 0);
  assert.equal((await f.members.loginMemberPassword(first.username, first.password)).id, 'm2');
  assert.ok(await authenticateMemberSession(f.members, session.token, f.env, f.clock));
  f.db.exec('DROP TRIGGER credential_private_tail');
  const success = await credentialPost(f, next, {
    headers: { 'idempotency-key': 'still-not-keyed' },
  });
  assert.equal(success.status, 200, success.text);
  assert.equal(seen, 1);
  assert.equal(await authenticateMemberSession(f.members, session.token, f.env, f.clock), null);
  assert.equal((await f.members.loginMemberPassword(next.username, next.password)).id, 'm2');
  await assert.rejects(f.members.loginMemberPassword(first.username, first.password), {
    statusCode: 401,
  });
  assert.equal(f.db.prepare('SELECT MAX(commit_seq) seq FROM commits').get().seq, cut);
});

test('credentials非keyed私有变更由选定SHA同cut联合恢复，业务回放不安装秘密或把旧private备份换成新源', async (t) => {
  const f = await setup(t, { env: passwordEnvironment, beforeReady: credentialGroups }),
    body = credentials({ groupId: 'g' }),
    created = await credentialPost(f, body);
  assert.equal(created.status, 200, created.text);
  const backup = async (name) => {
    const filename = path.join(f.dir, name + '.sqlite');
    await f.db.backup(filename);
    const sealed = new Database(filename);
    sealed.pragma('journal_mode=DELETE');
    sealed.close();
    return {
      sourcePath: filename,
      sourceSha256: relationalSnapshotSha256(filename),
      historyId: 'generation-history',
      finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) seq FROM commits').get()
        .seq,
      replicas: { dataDir: f.dir },
    };
  };
  const old = await backup('credentials-private-old'),
    hashBefore = relationalPrivateStateSha256(f.db),
    reset = { ...body, userId: created.body.id, password: 'synthetic-reset-private-password' };
  delete reset.groupId;
  assert.equal((await credentialPost(f, reset)).status, 200);
  const selected = await backup('credentials-private-current');
  assert.equal(old.finalCommitSeq, selected.finalCommitSeq);
  assert.notEqual(old.sourceSha256, selected.sourceSha256);
  assert.notEqual(relationalPrivateStateSha256(f.db), hashBefore);
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replayer = new RelationalCommitReplayer(target),
    source = new RelationalInputStore(f.db),
    network = t.mock.method(globalThis, 'fetch', () =>
      assert.fail('private/public recovery cannot fetch'),
    );
  try {
    for (const { seq } of f.db
      .prepare('SELECT CAST(commit_seq AS TEXT) seq FROM commits ORDER BY commit_seq')
      .all()) {
      target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), source))();
    }
    assert.equal(relationalPrivateStateEmpty(target), true);
    const before = databaseState(target);
    await assert.rejects(
      verifyAndInstallRelationalStorage(target, { ...selected, sourcePath: old.sourcePath }),
      /match|snapshot|Private/i,
    );
    assert.equal(databaseState(target), before);
    assert.equal(target.prepare('SELECT status FROM projection_state').get().status, 'building');
    await verifyAndInstallRelationalStorage(target, selected);
    assert.equal(relationalPrivateStateSha256(target), relationalPrivateStateSha256(f.db));
    const restored = new RelationalMemberStore(target, f.env);
    assert.equal(
      (await restored.loginMemberPassword(body.username, reset.password)).id,
      created.body.id,
    );
    await assert.rejects(restored.loginMemberPassword(body.username, body.password), {
      statusCode: 401,
    });
    assert.equal(relationalSnapshotSha256(old.sourcePath), old.sourceSha256);
    assert.equal(relationalSnapshotSha256(selected.sourcePath), selected.sourceSha256);
    assert.equal(network.mock.callCount(), 0);
  } finally {
    network.mock.restore();
  }
});

const invitationBody = (userId = 'invite-target') => ({ action: 'discord-invite', userId });
async function setupInvitation(t, options = {}) {
  const f = await setup(t, {
      ...options,
      beforeReady: async (f, env) => {
        f.db.transaction(() =>
          f.members.registerMember(meta('register-invite-target'), {
            memberId: 'invite-target',
            displayName: 'invited member',
            apiKey: 'synthetic-invite-target-key',
          }),
        )();
        await options.beforeReady?.(f, env);
      },
    }),
    notifier = relationalStateNotificationsFor(f.db, f.events);
  return {
    ...f,
    notifier,
    liveMembers: new RelationalMemberStore(f.db, f.env, notifier),
    get clock() {
      return f.clock;
    },
  };
}
function invitationInvariant(f) {
  return Object.fromEntries(
    [
      'commits',
      'domain_events',
      'stream_heads',
      'generation_requests',
      'generation_jobs',
      'generation_attempts',
      'charges',
      'wallets',
      'account_quota_state',
      'idempotency_keys',
      'members',
      'member_sessions',
      'api_keys',
      'password_credentials',
    ].map((name) => [
      name,
      f.db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).safeIntegers().all(),
    ]),
  );
}
function inviteNotices(f) {
  const notices = [];
  f.events.on('commit', (event) => {
    assert.equal(f.db.inTransaction, false);
    notices.push({ event, profile: f.liveMembers.readManagementMemberProfile('invite-target') });
  });
  return notices;
}

test('Discord邀请真实POST仅返回原私有DTO，每次新操作替换；同ms同目录0额外notice，expiry变更提交后一次', async (t) => {
  const f = await setupInvitation(t),
    notices = inviteNotices(f),
    before = invitationInvariant(f),
    headers = {
      'idempotency-key': ['ignored invalid key', 'ignored duplicate'],
      'x-member-activity': '1',
    },
    first = await f.call(owner, { method: 'POST', headers, body: invitationBody() });
  assert.equal(first.status, 200, first.text);
  assert.deepEqual(Object.keys(first.body), ['code', 'expiresAt']);
  assert.match(first.body.code, /^[A-Za-z0-9_-]{32}$/);
  assert.equal(first.body.expiresAt, f.clock + 7 * 86400000);
  assert.equal(first.headers.get('set-cookie'), null);
  assert.equal(f.db.serialize().includes(Buffer.from(first.body.code)), false);
  assert.equal(notices.length, 1);
  assert.deepEqual(notices[0].event.collections, ['users']);
  assert.deepEqual(notices[0].event.memberIds, ['invite-target']);
  assert.equal(notices[0].profile.inviteExpiresAt, first.body.expiresAt);
  assert.ok(!JSON.stringify(notices).includes(first.body.code));
  const repeated = await f.call(owner, { method: 'POST', headers, body: invitationBody() });
  assert.equal(repeated.status, 200, repeated.text);
  assert.notEqual(repeated.body.code, first.body.code);
  assert.equal(repeated.body.expiresAt, first.body.expiresAt);
  assert.equal(notices.length, 1, '新的原码不是公共目录字段，不能为同ms相同安全DTO额外发通知');
  f.advance(1);
  const third = await f.call(admin, {
    method: 'POST',
    headers: { ...headers, 'x-admin-token': adminToken },
    body: invitationBody(),
  });
  assert.equal(third.status, 200, third.text);
  assert.notEqual(third.body.code, repeated.body.code);
  assert.equal(third.body.expiresAt, first.body.expiresAt + 1);
  assert.equal(notices.length, 2);
  assert.deepEqual(notices[1].event.collections, ['users']);
  assert.equal(notices[1].profile.inviteExpiresAt, third.body.expiresAt);
  assert.deepEqual(invitationInvariant(f), before);
  for (const [userId, status] of [
    ['m2', 409],
    ['missing', 404],
    [null, 404],
    [42, 404],
    [{ id: 'invite-target' }, 404],
  ]) {
    assert.equal(
      (await f.call(owner, { method: 'POST', body: invitationBody(userId) })).status,
      status,
    );
  }
  assert.equal(
    (
      await f.call(owner, {
        method: 'POST',
        member: 'm2',
        body: { ...invitationBody(), actorKind: 'admin', actorId: 'm1' },
      })
    ).status,
    403,
  );
  assert.equal((await f.call(admin, { method: 'POST', body: invitationBody() })).status, 403);
  const actor = await authenticateMemberApiKey(f.liveMembers, keys.m1);
  f.notifier.commit(() =>
    f.db.transaction(() =>
      f.liveMembers.setMemberStatus(
        meta('disable-invitation-target', f.clock, 'm1'),
        'invite-target',
        'disabled',
        actor,
      ),
    )(),
  );
  const saved = databaseState(f.db);
  assert.equal((await f.call(owner, { method: 'POST', body: invitationBody() })).status, 404);
  assert.equal(databaseState(f.db), saved);
  for (const key of ['DISCORD_GUILD_ID', 'DISCORD_CLIENT_ID', 'DISCORD_CLIENT_SECRET']) {
    f.env[key] = '';
  }
  assert.equal(
    (
      await f.call(admin, {
        method: 'POST',
        headers: { 'x-admin-token': adminToken },
        body: invitationBody(),
      })
    ).status,
    409,
  );
  assert.equal(databaseState(f.db), saved);
});

test('Discord邀请canonical私有owner拒缺actor/非owner/旧epoch，不能由metadata自证', async (t) => {
  const f = await setupInvitation(t),
    actor = await authenticateMemberApiKey(f.liveMembers, keys.m1),
    other = await authenticateMemberApiKey(f.liveMembers, keys.m2),
    before = databaseState(f.db),
    notices = inviteNotices(f);
  for (const candidate of [undefined, other, { actorKind: 'admin', actorId: 'local-admin' }]) {
    assert.throws(
      () =>
        f.notifier.commit(() =>
          f.db.transaction(() =>
            f.liveMembers.createMemberDiscordInvite('invite-target', candidate, f.clock),
          )(),
        ),
      { statusCode: 403 },
    );
    assert.equal(databaseState(f.db), before);
  }
  const prepared = await f.liveMembers.prepareMemberCredentials('m1', {
    username: 'invite.current.owner',
    password: 'synthetic-invite-owner-new-password',
  });
  f.notifier.commit(() =>
    f.db.transaction(() => f.liveMembers.setMemberCredentials(prepared, actor))(),
  );
  const changed = databaseState(f.db),
    count = notices.length;
  assert.throws(
    () =>
      f.notifier.commit(() =>
        f.db.transaction(() =>
          f.liveMembers.createMemberDiscordInvite('invite-target', actor, f.clock),
        )(),
      ),
    { statusCode: 403 },
  );
  assert.equal(databaseState(f.db), changed);
  assert.equal(notices.length, count);
  const current = await authenticateMemberApiKey(f.liveMembers, keys.m1),
    accepted = f.notifier.commit(() =>
      f.db.transaction(() =>
        f.liveMembers.createMemberDiscordInvite('invite-target', current, f.clock),
      )(),
    );
  assert.match(accepted.code, /^[A-Za-z0-9_-]{32}$/);
});

test('Discord邀请慢正文后重核实际Cookie/epoch/owner/ready/admin，拒绝不签发邀请或续Cookie', async (t) => {
  for (const mode of ['cookie', 'epoch', 'owner', 'ready', 'admin-query']) {
    await t.test(mode, async (t) => {
      let begin;
      const f = await setupInvitation(t, { onBodyRead: () => begin?.() }),
        notices = inviteNotices(f),
        started = new Promise((resolve) => {
          begin = resolve;
        }),
        endpoint = mode === 'admin-query' ? admin + '?adminToken=' + adminToken : owner,
        held = await holdPolicyBody(t, f, endpoint, started, {}, invitationBody());
      if (mode === 'cookie') {
        await logoutMemberSession(f.liveMembers, f.sessions.m1.token);
      } else if (mode === 'epoch') {
        const actor = await authenticateMemberApiKey(f.liveMembers, keys.m1),
          prepared = await f.liveMembers.prepareMemberCredentials('m1', {
            username: 'held.invite.owner',
            password: 'synthetic-held-invite-password',
          });
        f.notifier.commit(() =>
          f.db.transaction(() => f.liveMembers.setMemberCredentials(prepared, actor))(),
        );
      } else if (mode === 'owner') {
        f.env.OWNER_USER_ID = 'm2';
      } else if (mode === 'ready') {
        f.db.prepare("UPDATE projection_state SET status='failed' WHERE id=1").run();
      } else {
        f.env.PUBLIC_MODE = 'true';
      }
      const invites = f.db.prepare('SELECT * FROM discord_invites').all(),
        cut = f.db.prepare('SELECT COUNT(*) n FROM commits').get().n,
        result = await held.finish();
      assert.equal(result.status, mode === 'ready' ? 500 : 401, result.text);
      assert.equal(result.headers['set-cookie'], undefined);
      assert.deepEqual(f.db.prepare('SELECT * FROM discord_invites').all(), invites);
      assert.equal(f.db.prepare('SELECT COUNT(*) n FROM commits').get().n, cut);
      assert.equal(
        notices.filter(
          ({ event }) =>
            event.collections.includes('users') && event.memberIds.includes('invite-target'),
        ).length,
        0,
      );
    });
  }
});

test('Discord邀请私有SQL尾错/外catch全表回滚0notice，健康重试新操作且不存receipt', async (t) => {
  const f = await setupInvitation(t),
    notices = inviteNotices(f);
  assert.equal((await f.call(owner, { method: 'POST', body: invitationBody() })).status, 200);
  f.advance(1);
  const before = databaseState(f.db),
    expiry = f.liveMembers.readManagementMemberProfile('invite-target').inviteExpiresAt,
    count = notices.length;
  f.db.exec(
    "CREATE TEMP TRIGGER invitation_tail_fault AFTER UPDATE ON discord_invites BEGIN SELECT RAISE(ABORT,'invitation tail fault'); END",
  );
  assert.equal((await f.call(owner, { method: 'POST', body: invitationBody() })).status, 500);
  assert.equal(databaseState(f.db), before);
  assert.equal(notices.length, count);
  f.notifier.commit(() =>
    f.db.transaction(() => {
      try {
        f.liveMembers.createMemberDiscordInvite('invite-target', null, f.clock);
      } catch (error) {
        assert.match(error.message, /invitation tail fault/);
      }
    })(),
  );
  assert.equal(databaseState(f.db), before);
  assert.equal(f.liveMembers.readManagementMemberProfile('invite-target').inviteExpiresAt, expiry);
  assert.equal(notices.length, count);
  f.db.exec('DROP TRIGGER invitation_tail_fault');
  const retried = await f.call(owner, { method: 'POST', body: invitationBody() });
  assert.equal(retried.status, 200, retried.text);
  assert.equal(retried.body.expiresAt, expiry + 1);
  assert.equal(notices.length, count + 1);
});

test('Discord邀请新码替换/一次性兑现复用真实OAuth owner，consume尾错回滚后可重新授权且仅提交一次通知', async (t) => {
  let f;
  const discordFetch = async (url) => {
    assert.equal(f.db.inTransaction, false);
    if (url.endsWith('/oauth2/token')) {
      return Response.json({
        access_token: 'synthetic-invitation-access',
        refresh_token: 'synthetic-invitation-refresh',
        token_type: 'Bearer',
        expires_in: 3600,
        scope: 'identify guilds.members.read',
      });
    }
    return Response.json(
      url.endsWith('/users/@me')
        ? { id: '555555555555555555' }
        : { user: { id: '555555555555555555' } },
    );
  };
  f = await setupInvitation(t, { discordFetch });
  const auth = createRelationalMemberAuthenticationHttp({
      sqlite: f.db,
      events: f.events,
      env: f.env,
      discordFetch,
    }),
    notices = inviteNotices(f);
  f.setHandler(
    async (req, res, url) => (await auth(req, res, url)) || (await f.handle(req, res, url)),
  );
  const headers = { host: 'management.example', origin: 'https://management.example' },
    start = async (code) => {
      const r = await f.call('/api/member/discord/start', {
        method: 'POST',
        body: { invite: code },
        headers,
      });
      assert.equal(r.status, 200, r.text);
      return {
        state: new URL(r.body.authorizationUrl).searchParams.get('state'),
        cookie: r.headers.get('set-cookie').split(';')[0],
      };
    },
    finish = (pending) =>
      f.call(
        '/api/member/discord/callback?' +
          new URLSearchParams({ state: pending.state, code: 'synthetic-grant-code' }),
        { headers: { ...headers, cookie: pending.cookie, origin: 'https://discord.com' } },
      );
  const first = await f.call(owner, { method: 'POST', body: invitationBody() }),
    oldFlow = await start(first.body.code),
    second = await f.call(owner, {
      method: 'POST',
      headers: { 'idempotency-key': 'same-does-not-replay' },
      body: invitationBody(),
    });
  assert.equal((await finish(oldFlow)).status, 403);
  assert.equal(
    (
      await f.call('/api/member/discord/start', {
        method: 'POST',
        body: { invite: first.body.code },
        headers,
      })
    ).status,
    403,
  );
  const flow = await start(second.body.code),
    before = databaseState(f.db),
    count = notices.length;
  f.db.exec(
    "CREATE TEMP TRIGGER invitation_consume_fault AFTER DELETE ON discord_invites WHEN OLD.member_id='invite-target' BEGIN SELECT RAISE(ABORT,'invitation consume fault'); END",
  );
  const failed = await finish(flow);
  assert.equal(failed.status, 500, failed.text);
  assert.equal((failed.headers.get('set-cookie') || '').includes('nai_member_session='), false);
  assert.equal(databaseState(f.db), before);
  assert.equal(notices.length, count);
  f.db.exec('DROP TRIGGER invitation_consume_fault');
  const done = await finish(await start(second.body.code));
  assert.equal(done.status, 303, done.text);
  assert.equal(f.liveMembers.readManagementMemberProfile('invite-target').inviteExpiresAt, null);
  assert.equal(
    f.liveMembers.readManagementMemberProfile('invite-target').discordId,
    '555555555555555555',
  );
  assert.equal(notices.length, count + 1);
  assert.ok(notices.at(-1).event.collections.includes('users'));
  assert.ok(notices.at(-1).event.collections.includes('sessions'));
  assert.equal(notices.at(-1).profile.inviteExpiresAt, null);
  assert.equal((await finish(flow)).status, 400);
  assert.equal(
    (
      await f.call('/api/member/discord/start', {
        method: 'POST',
        body: { invite: second.body.code },
        headers,
      })
    ).status,
    403,
  );
  assert.equal((await f.call(owner, { method: 'POST', body: invitationBody() })).status, 409);
  assert.ok(
    !JSON.stringify(f.db.prepare('SELECT payload FROM domain_events').all()).includes(
      second.body.code,
    ),
  );
});

test('Discord邀请过期保留私有原期限；公开回放不恢复邀请，匹配选定私有源SHA才恢复同一摘要', async (t) => {
  const f = await setupInvitation(t),
    issued = await f.call(owner, { method: 'POST', body: invitationBody() });
  assert.equal(issued.status, 200, issued.text);
  const sourcePath = path.join(f.dir, 'selected-invitation.sqlite');
  await f.db.backup(sourcePath);
  const sealed = new Database(sourcePath);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  const sourceSha256 = relationalSnapshotSha256(sourcePath),
    originalInvite = f.db.prepare('SELECT * FROM discord_invites').all(),
    target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replay = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db);
  let fetches = 0;
  t.mock.method(globalThis, 'fetch', () => {
    fetches++;
    assert.fail('invitation public replay cannot fetch');
  });
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replay.replayCommit(readRelationalCommit(f.db, seq), inputs))();
  }
  for (const name of [
    'discord_invites',
    'discord_bindings',
    'api_keys',
    'member_sessions',
    'upstream_credentials',
  ]) {
    assert.equal(target.prepare(`SELECT COUNT(*) n FROM ${name}`).get().n, 0);
  }
  assert.equal(
    target.prepare('SELECT COUNT(*) n FROM members WHERE auth_epoch IS NOT NULL').get().n,
    0,
  );
  await verifyAndInstallRelationalStorage(target, { ...f.selected, sourcePath, sourceSha256 });
  assert.deepEqual(target.prepare('SELECT * FROM discord_invites').all(), originalInvite);
  assert.equal(fetches, 0);
  const restoredAuth = createRelationalMemberAuthenticationHttp({
    sqlite: target,
    events: new EventEmitter(),
    env: f.env,
    discordFetch: () => assert.fail('valid invite start must stay offline'),
  });
  f.setHandler(restoredAuth);
  assert.equal(
    (
      await f.call('/api/member/discord/start', {
        method: 'POST',
        body: { invite: issued.body.code },
        headers: { host: 'management.example', origin: 'https://management.example' },
      })
    ).status,
    200,
  );
  const expiry = issued.body.expiresAt;
  f.advance(expiry - f.clock + 1);
  const auth = createRelationalMemberAuthenticationHttp({
    sqlite: f.db,
    events: f.events,
    env: f.env,
    discordFetch: () => assert.fail('expired invite must reject before OAuth network'),
  });
  f.setHandler(auth);
  assert.equal(
    (
      await f.call('/api/member/discord/start', {
        method: 'POST',
        body: { invite: issued.body.code },
        headers: { host: 'management.example', origin: 'https://management.example' },
      })
    ).status,
    403,
  );
  assert.equal(f.liveMembers.readManagementMemberProfile('invite-target').inviteExpiresAt, expiry);
  assert.equal(relationalSnapshotSha256(sourcePath), sourceSha256);
});

test('私有备注新建沿原120码元与默认规则，GET不借displayName且不公开原备注', async (t) => {
  const f = await setup(t, { env: passwordEnvironment }),
    value = 'private-note-' + 'x'.repeat(108) + '😀',
    expected = String(value || '管理员创建的成员').slice(0, 120),
    body = credentials({ username: 'private.note.boundary', note: value }),
    created = await credentialPost(f, body);
  assert.equal(created.status, 200, created.text);
  assert.deepEqual(created.body, { id: created.body.id, username: body.username });
  const state = await f.call(admin, { headers: { 'x-admin-token': adminToken } });
  assert.equal(state.status, 200, state.text);
  const member = state.body.users.find((user) => user.id === created.body.id);
  assert.equal(member.note, expected);
  assert.equal(member.displayName, body.username);
  assert.equal(member.username, body.username);
  assert.equal(f.db.prepare('SELECT note_json FROM members WHERE id=?').get(created.body.id).note_json, JSON.stringify(expected));
});
