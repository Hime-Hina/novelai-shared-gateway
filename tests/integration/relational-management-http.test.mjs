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
import { domainDigest } from '../../server/events/domain-events.ts';
import { createGenerationControls } from '../../server/generation/queue-dispatch.ts';
import { accountQuotaResult } from '../../server/quota/account-refresh.ts';
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
async function setup(t, { env = {}, beforeReady, resources, discordFetch, onBodyRead } = {}) {
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
async function holdPolicyBody(t, f, endpoint, started, headers = {}, body = freePolicy()) {
  const text = JSON.stringify(body),
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
