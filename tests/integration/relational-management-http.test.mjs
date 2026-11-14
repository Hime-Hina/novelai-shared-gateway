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
} from '../helpers/relational-generation.mjs';
import { createRelationalManagementHttp } from '../../server/members/relational-management-http.ts';
import { assertAdminToken, matchesAdminToken } from '../../server/http/admin-access.ts';
import { publicErrorMessage } from '../../server/http/public-errors.ts';
import {
  createMemberSession,
  authenticateMemberApiKey,
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
import { domainDigest } from '../../server/events/domain-events.ts';
import { createGenerationControls } from '../../server/generation/queue-dispatch.ts';
import { accountQuotaResult, createAccountQuotaQuery } from '../../server/quota/account-refresh.ts';
import { fetchNovelAiAccountQuota } from '../../server/protocols/novelai-client.ts';
import { relationalStateNotificationsFor } from '../../server/events/relational-state-notifications.ts';

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
        f.db.transaction(() => f.members.setMemberCredentials(prepared))();
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
              f.db.transaction(() => f.members.setMemberCredentials(prepared))();
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
        f.db.transaction(() => f.members.setMemberCredentials(prepared))();
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
    [{}, { action: 'groups', groups: [] }, 400],
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
async function holdPolicyBody(t, f, endpoint, started, headers = {}) {
  const text = JSON.stringify(freePolicy()),
    request = httpRequest(f.origin + endpoint, {
      method: 'POST',
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
  const query = createAccountQuotaQuery({
    env: { NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${server.address().port}` },
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

test('账户PATCH真实ready Cookie轮换/暂停/Opus核验共用owner，原回执重领零IO且actor和操作隔离', async (t) => {
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
  const actors = f.db
    .prepare(
      "SELECT actor_kind,actor_id FROM commits WHERE command_id LIKE 'management:%' ORDER BY commit_seq",
    )
    .all();
  assert.deepEqual(actors, [
    { actor_kind: 'member', actor_id: 'm1' },
    { actor_kind: 'member', actor_id: 'm1' },
    { actor_kind: 'member', actor_id: 'm1' },
    { actor_kind: 'admin', actor_id: 'local-admin' },
  ]);
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

test('账户PATCH仍需当前Cookie或local admin且同源，Bearer不替代；admin旧未表示字段明确拒绝', async (t) => {
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
      (
        await f.patch(
          { [field]: field === 'weight' ? 5 : 'unsupported' },
          { route: accountAdmin, headers: { 'x-admin-token': adminToken } },
        )
      ).status,
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
          f.db.transaction(() => f.members.setMemberCredentials(prepared))();
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
