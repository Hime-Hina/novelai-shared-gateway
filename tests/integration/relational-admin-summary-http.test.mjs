import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { EventEmitter, once } from 'node:events';
import Database from 'better-sqlite3';
import path from 'node:path';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import {
  fixture,
  installReady,
  NOW,
  meta,
  databaseState,
  keys,
} from '../helpers/relational-generation.mjs';
import { createRelationalManagementHttp } from '../../server/members/relational-management-http.ts';
import { assertAdminToken } from '../../server/http/admin-access.ts';
import { createPublicImageAccess } from '../../server/http/public-image.ts';
import { publicErrorMessage } from '../../server/http/public-errors.ts';
import { createGenerationControls } from '../../server/generation/queue-dispatch.ts';
import { JobPreviews } from '../../server/generation/job-previews.ts';
import { RelationalReportStore } from '../../server/storage/relational-report-store.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalAccountStore } from '../../server/storage/relational-account-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalConfigurationStore } from '../../server/storage/relational-configuration-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { relationalPrivateStateEmpty } from '../../server/storage/relational-private-restore.ts';
import {
  createMemberSession,
  authenticateMemberApiKey,
} from '../../server/members/authentication.ts';
import { maskToken, maskProxyUrl } from '../../server/members/owner-operations.ts';

const route = '/api/admin/summary',
  token = 'synthetic-summary-admin-token';
const summaryKeys = [
  'settings',
  'userCount',
  'accounts',
  'imageCount',
  'imageTotal',
  'cacheImageCount',
  'requestStats1m',
  'generationStats1m',
  'jobStats1h',
  'generationSpeed1h',
  'usageHourlyDays',
  'errorLogs',
  'jobs',
];
const accountKeys = [
  'id',
  'routeId',
  'name',
  'token',
  'proxyUrl',
  'hasProxy',
  'enabled',
  'weight',
  'inFlight',
  'total',
  'failures',
  'quotaPoints',
  'quotaFixed',
  'quotaPurchased',
  'quotaTier',
  'quotaTierText',
  'trialRemainingImages',
  'trialUsedImages',
  'trialEligible',
  'trialStatusKnown',
  'v5UsagePercent',
  'v5UsageIsNegative',
  'v5UsageTimeUntilNextPercent',
  'quotaCheckedAt',
  'quotaError',
  'cooldownUntil',
  'stats1h',
  'lastUsedAt',
];
const sameKeys = (actual, expected) =>
  assert.deepEqual(Object.keys(actual).sort(), [...expected].sort());
async function emptyFixture(t) {
  const dir = await mkdtemp(path.join(tmpdir(), 'nai-admin-summary-empty-')),
    db = new Database(path.join(dir, 'library.sqlite'));
  t.after(async () => {
    db.close();
    await rm(dir, { recursive: true, force: true });
  });
  await mkdir(path.join(dir, 'images'));
  initializeRelationalStorage(db, 'generation-history');
  const members = new RelationalMemberStore(db);
  for (const id of ['m1', 'm2', 'm3', 'm4'])
    db.transaction(() =>
      members.registerMember(meta('register-' + id), {
        memberId: id,
        displayName: id,
        apiKey: 'synthetic-key-' + id,
      }),
    )();
  return { db, dir, members };
}
async function setup(t, { beforeReady, env = {}, empty = false } = {}) {
  const cleanup = [];
  t.after(async () => {
    for (const close of cleanup.reverse()) await close();
  });
  const currentEnv = { OWNER_USER_ID: 'm1', PUBLIC_BASE_URL: 'https://deployment.example', ...env },
    f = await (empty ? emptyFixture : fixture)(
      { after: (close) => cleanup.push(close) },
      { env: currentEnv, seconds: 0, secondV5: '60' },
    ),
    events = new EventEmitter(),
    previews = new JobPreviews(),
    physical = createGenerationControls({ timeoutDelay: () => 60000, schedule() {} }),
    quotas = new RelationalQuotaStore(f.db, currentEnv),
    accounts = new RelationalAccountStore(f.db, quotas, physical.controls, currentEnv);
  let clock = NOW + 60000,
    adminToken = token,
    activeRequest,
    notifications = 0;
  t.mock.method(Date, 'now', () => clock);
  events.on('commit', () => notifications++);
  const configure = (id, changes) =>
    f.db.transaction(() =>
      accounts.prepareAccountCommand(
        { ...meta(id), actorKind: 'admin', actorId: 'local-admin' },
        { kind: 'configure', accountId: 'a', changes },
        null,
      ),
    )();
  await beforeReady?.({ ...f, quotas, accounts, configure, previews });
  const selected = await installReady(f),
    access = createPublicImageAccess(currentEnv, () => clock),
    deps = {
      sqlite: f.db,
      events,
      dataDir: f.dir,
      controls: physical.controls,
      previews,
      imageUrl: access.imageUrl,
      env: currentEnv,
      now: () => clock,
      assertAdmin(req, url) {
        activeRequest = req;
        assertAdminToken(req, url, adminToken, currentEnv);
      },
    };
  let handle = createRelationalManagementHttp(deps);
  const server = createServer(async (req, res) => {
    try {
      if (await handle(req, res, new URL(req.url, `http://${req.headers.host}`))) return;
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
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const origin = `http://127.0.0.1:${server.address().port}`;
  cleanup.push(async () => {
    for (const [id, control] of physical.controls) physical.finishRunningJobControl(id, control);
    previews.closeSubscriptions();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  });
  const call = (url = route, { headers = {}, method = 'GET', withAdmin = true } = {}) =>
    new Promise((resolve, reject) => {
      const req = request(
        origin + url,
        {
          method,
          headers: { origin, ...(withAdmin ? { 'x-admin-token': token } : {}), ...headers },
        },
        (res) => {
          let text = '';
          res.setEncoding('utf8');
          res.on('data', (c) => (text += c));
          res.on('end', () =>
            resolve({
              status: res.statusCode,
              headers: res.headers,
              text,
              body: text ? JSON.parse(text) : null,
            }),
          );
        },
      );
      req.on('error', reject);
      req.end();
    });
  return {
    ...f,
    env: currentEnv,
    events,
    previews,
    physical,
    quotas,
    accounts,
    configure,
    selected,
    call,
    access,
    deps,
    get clock() {
      return clock;
    },
    get activeRequest() {
      return activeRequest;
    },
    get notifications() {
      return notifications;
    },
    revokeAdmin() {
      adminToken = 'different-current-admin-token';
    },
    setDatabase(db) {
      handle = createRelationalManagementHttp({ ...deps, sqlite: db });
    },
    addCleanup(close) {
      cleanup.push(close);
    },
  };
}
function pauseStats(t, f) {
  let release, entered;
  const wait = new Promise((resolve) => (release = resolve)),
    started = new Promise((resolve) => (entered = resolve)),
    original = RelationalReportStore.prototype.readAdminSummaryStats;
  f.addCleanup(() => release());
  t.mock.method(RelationalReportStore.prototype, 'readAdminSummaryStats', async function (at) {
    const result = original.call(this, at);
    assert.equal(f.db.inTransaction, false);
    entered();
    await wait;
    return result;
  });
  return { started, release };
}

function openingUnknownFailure(f) {
  f.db.transaction(() => {
    const id = 'unknown-summary-history',
      origin = { sourceId: 'synthetic-opening', originalId: id },
      input = new RelationalInputStore(f.db),
      body = { operation: 'generate', model: 'nai-diffusion-5-full', input: null, parameters: {} },
      staged = input.stageOpeningInput({
        inputId: id,
        memberId: 'm1',
        completeness: 'partial',
        body,
        logicalBytes: Buffer.byteLength(JSON.stringify(body)),
        assets: [],
      }),
      common = {
        memberId: 'm1',
        accountId: null,
        occurredAt: NOW + 86400000,
        policyVersion: 'historical',
        pricingVersion: 'historical',
      },
      events = [
        {
          ...common,
          eventType: 'OpeningInputRecorded',
          streamId: 'input:' + id,
          payload: { ...staged, origin },
        },
        {
          ...common,
          eventType: 'OpeningRequestRecorded',
          streamId: 'request:' + id,
          payload: {
            requestId: id,
            memberId: 'm1',
            inputId: id,
            contextId: null,
            source: 'historical',
            operation: 'generate',
            model: body.model,
            sampleCount: 1,
            seedMode: 'unknown',
            confirmation: null,
            acceptedAt: NOW + 86400000,
            retiredAt: null,
            origin,
          },
        },
        {
          ...common,
          eventType: 'OpeningJobRecorded',
          streamId: 'request:' + id,
          payload: {
            jobId: id + '-0',
            requestId: id,
            createdAt: null,
            sampleIndex: 0,
            actualSeed: null,
            status: 'failed',
            queueSeq: null,
            queueExpiresAt: null,
            deadlineAt: null,
            admissionEpoch: 0,
            artifactId: null,
            cacheKey: null,
            completedAt: null,
            errorCode: 'historical_failure',
            errorStatus: 500,
            cancellationReason: null,
            lateResultAt: null,
            freeSucceededAt: null,
            freeWarningThreshold: null,
            retiredAt: null,
            origin,
          },
        },
      ],
      {
        policyVersion: _p,
        pricingVersion: _r,
        ...metadata
      } = meta('opening-summary-history', NOW + 86400000);
    new RelationalCommitReplayer(f.db).appendCommit(
      {
        ...metadata,
        events,
        receipt: null,
        expectedVersions: Object.fromEntries(events.map((event) => [event.streamId, '0'])),
      },
      input,
    );
  })();
}

test('完整admin summary实际13字段保留原统计与28账户来源，真实Map/未知设置且query_only零写', async (t) => {
  let amount;
  const f = await setup(t, {
    env: { PUBLIC_MODE: 'true' },
    beforeReady: async (f) => {
      f.accept('summary-batch', { samples: 2 });
      f.dispatch('summary-batch-0', NOW + 1);
      amount = f.generations.readCharge('summary-batch-0').anlasFixed;
      await f.publish('summary-batch-0', NOW + 3);
      const second = f.dispatch('summary-batch-1', NOW + 11004);
      f.fail(
        'summary-batch-1',
        {
          aborted: false,
          diagnostic: {
            dispatch: second.dispatch,
            error: new Error('synthetic provider unavailable'),
          },
        },
        NOW + 11005,
      );
      await f.publish('summary-batch-1', NOW + 11007);
      f.db.transaction(() =>
        f.members.setMemberStatus(meta('disable-summary-member'), 'm2', 'disabled'),
      )();
      f.configure('summary-proxy', {
        displayName: '',
        weight: 1.25,
        proxyUrl: 'socks5://demo:secret@127.0.0.1:19000',
      });
      f.db.transaction(() =>
        f.accounts.prepareAccountCommand(
          { ...meta('summary-extra-account'), actorKind: 'admin', actorId: 'local-admin' },
          {
            kind: 'register',
            accountId: 'toString',
            displayName: 'paused extra',
            token: 'pst-synthetic-extra',
            configVersion: 1,
            bind: false,
            weight: 2.75,
          },
          null,
        ),
      )();
    },
  });
  const control = f.physical.createRunningJobControl({
      id: 'physical-unresolved',
      requestId: 'already-terminal',
      accountId: 'a',
    }),
    before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() n').get().n;
  f.db.pragma('query_only=ON');
  let view;
  try {
    view = await f.call(route + '?fresh=1');
  } finally {
    f.db.pragma('query_only=OFF');
  }
  assert.equal(view.status, 200, view.text);
  sameKeys(view.body, summaryKeys);
  assert.equal(view.headers['cache-control'], 'no-store');
  assert.equal(view.headers['set-cookie'], undefined);
  assert.equal(view.body.userCount, 2);
  assert.equal(view.body.accounts.length, 2);
  const account = view.body.accounts[0];
  sameKeys(account, accountKeys);
  assert.equal(account.name, '');
  assert.equal(account.weight, 1.25);
  assert.equal(account.inFlight, 1);
  assert.equal(account.token, maskToken('synthetic-upstream-pst'));
  assert.equal(account.proxyUrl, maskProxyUrl('socks5://demo:secret@127.0.0.1:19000'));
  assert.equal(account.hasProxy, true);
  for (const key of [
    'routeId',
    'total',
    'failures',
    'trialRemainingImages',
    'trialUsedImages',
    'trialEligible',
    'lastUsedAt',
  ])
    assert.equal(account[key], null, key);
  assert.equal(account.trialStatusKnown, false);
  const extra = view.body.accounts[1];
  sameKeys(extra, accountKeys);
  assert.equal(extra.id, 'toString');
  assert.equal(extra.enabled, false);
  assert.equal(extra.weight, 2.75);
  assert.equal(extra.quotaTier, null);
  assert.equal(extra.quotaPoints, null);
  assert.deepEqual(extra.stats1h, { done: 0, failed: 0, total: 0, successRate: 0 });
  assert.equal(view.body.imageCount, 2);
  assert.equal(view.body.imageTotal, 2);
  assert.equal(view.body.cacheImageCount, 2);
  assert.ok(Number(amount) > 0, 'real fee is not the legacy display cost');
  assert.deepEqual(view.body.requestStats1m, { total: 2 });
  assert.deepEqual(view.body.generationStats1m, { total: 0 });
  assert.deepEqual(view.body.jobStats1h, { done: 1, failed: 1, total: 2, successRate: 0.5 });
  assert.deepEqual(account.stats1h, view.body.jobStats1h);
  assert.equal(view.body.generationSpeed1h.v45.count, 1);
  assert.ok(Math.abs(view.body.generationSpeed1h.v45.seconds - 0.003) < 0.001);
  assert.equal(view.body.usageHourlyDays.length, 7);
  assert.equal(
    view.body.usageHourlyDays.reduce((n, d) => n + d.credits, 0),
    0,
  );
  assert.equal(view.body.errorLogs.length, 1);
  assert.equal(view.body.errorLogs[0].id, 'summary-batch-1');
  assert.equal(view.body.jobs.length, 1);
  assert.equal(view.body.jobs[0].id, 'summary-batch-1');
  assert.equal(view.body.jobs[0].cost, 0);
  assert.equal(view.body.jobs[0].accountCost, null);
  assert.equal(view.body.jobs[0].accountRouteId, null);
  assert.equal(view.body.jobs[0].lateResultAvailable, true);
  assert.equal(
    f.access.validSignature(new URL(view.body.jobs[0].imageUrl, 'http://127.0.0.1')),
    true,
  );
  assert.equal(view.body.jobs[0].updatedAt, new Date(NOW + 11005).toISOString());
  const config = new RelationalConfigurationStore(f.db).readCurrent();
  for (const [key, value] of Object.entries(config.normalizationDefaults))
    assert.deepEqual(view.body.settings[key], value);
  assert.equal(view.body.settings.publicBaseUrl, config.publicBaseUrl);
  assert.notEqual(view.body.settings.publicBaseUrl, f.env.PUBLIC_BASE_URL);
  for (const key of [
    'serviceName',
    'costPerImage',
    'maxCacheImages',
    'accountConcurrency',
    'mockWhenNoAccount',
    'promptApi',
  ])
    assert.equal(view.body.settings[key], null, key);
  assert.deepEqual(view.body.settings.fairQuota, f.quotas.readAdminQuotaConfiguration());
  assert.deepEqual(view.body.settings.fairQuota.members, ['m1', 'm2']);
  assert.ok(!view.text.includes('synthetic-upstream-pst'));
  assert.ok(!view.text.includes('demo:secret'));
  assert.equal(f.physical.controls.get('physical-unresolved'), control);
  assert.equal(control.controller.signal.aborted, false);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() n').get().n, changes);
  assert.equal(f.notifications, 0);
});

test('admin summary精确GET与当前admin权限，ownerCookie/Key/PUBLIC query不能取得reveal', async (t) => {
  const f = await setup(t),
    session = await createMemberSession(
      f.members,
      await authenticateMemberApiKey(f.members, keys.m1),
      f.env,
      f.clock,
    );
  for (const options of [
    { withAdmin: false },
    { withAdmin: false, headers: { cookie: `nai_member_session=${session.token}` } },
    { withAdmin: false, headers: { authorization: `Bearer ${keys.m1}` } },
  ])
    assert.equal((await f.call(route + '?revealTokens=1', options)).status, 403);
  for (const headers of [
    { host: 'elsewhere.example' },
    { origin: 'https://elsewhere.example' },
    { 'sec-fetch-site': 'cross-site' },
  ])
    assert.equal((await f.call(route, { headers })).status, 404);
  assert.equal((await f.call(route, { method: 'POST' })).status, 404);
  assert.equal((await f.call(route + '/')).status, 404);
  assert.equal(
    (await f.call(route + '?revealTokens=true')).body.accounts[0].token,
    maskToken('synthetic-upstream-pst'),
  );
  assert.equal(
    (await f.call(route + '?revealTokens=1')).body.accounts[0].token,
    'synthetic-upstream-pst',
  );
  f.env.PUBLIC_MODE = 'true';
  assert.equal(
    (await f.call(route + '?adminToken=' + token + '&revealTokens=1', { withAdmin: false })).status,
    401,
  );
  assert.equal((await f.call(route + '?revealTokens=1')).status, 200);
});

test('admin summary无共享配置仍完整可读，后续真实四成员原序不由ID排序或group拼接替代', async (t) => {
  const f = await setup(t, { empty: true }),
    first = await f.call();
  assert.equal(first.status, 200, first.text);
  sameKeys(first.body, summaryKeys);
  assert.equal(first.body.settings.fairQuota, null);
  assert.equal(first.body.userCount, 4);
  assert.deepEqual(first.body.accounts, []);
  assert.deepEqual(first.body.requestStats1m, { total: 0 });
  assert.deepEqual(first.body.jobStats1h, { done: 0, failed: 0, total: 0, successRate: 0 });
  const ids = ['m4', 'm2', 'm3', 'm1'];
  f.db.transaction(() =>
    f.quotas.configureMemberIds(
      { ...meta('summary-ordered-members'), actorKind: 'admin', actorId: 'local-admin' },
      ids,
    ),
  )();
  const before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() n').get().n,
    second = await f.call();
  assert.equal(second.status, 200, second.text);
  assert.deepEqual(second.body.settings.fairQuota.members, ids);
  assert.equal(second.body.settings.fairQuota.boundAccountId, null);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() n').get().n, changes);
  assert.equal(f.notifications, 0);
});

test('admin summary统计await后重新验证权限/Host/private/PUBLIC/ready，不提前读取私有账户', async (t) => {
  for (const variant of ['admin', 'host', 'origin', 'public-query', 'ready'])
    await t.test(variant, async (t) => {
      const f = await setup(t),
        paused = pauseStats(t, f),
        original = RelationalAccountStore.prototype.readAdminAccountProfile;
      let privateReads = 0;
      t.mock.method(
        RelationalAccountStore.prototype,
        'readAdminAccountProfile',
        function (...args) {
          privateReads++;
          return original.apply(this, args);
        },
      );
      const response = f.call(
        route + '?revealTokens=1' + (variant === 'public-query' ? '&adminToken=' + token : ''),
        { withAdmin: variant !== 'public-query' },
      );
      await paused.started;
      assert.equal(privateReads, 0);
      let expected = 401;
      if (variant === 'admin') f.revokeAdmin();
      if (variant === 'host') {
        f.activeRequest.headers.host = 'elsewhere.example';
        expected = 404;
      }
      if (variant === 'origin') {
        f.activeRequest.headers.origin = 'https://elsewhere.example';
        expected = 404;
      }
      if (variant === 'public-query') f.env.PUBLIC_MODE = 'true';
      if (variant === 'ready') {
        // 故意破坏已由installer安装的目标，不能靠第一次ready检查继续吐出秘密。
        f.db.prepare("UPDATE projection_state SET status='building' WHERE id=1").run();
        expected = 500;
      }
      const before = databaseState(f.db);
      paused.release();
      const result = await response;
      assert.equal(result.status, expected, result.text);
      assert.equal(privateReads, 0);
      assert.ok(!result.text.includes('synthetic-upstream-pst'));
      assert.equal(databaseState(f.db), before);
      assert.equal(f.notifications, 0);
    });
});

test('admin summary await中实际轮换PST/proxy，reveal只读最终当前来源且不写业务', async (t) => {
  const f = await setup(t),
    paused = pauseStats(t, f),
    pending = f.call(route + '?revealTokens=1');
  await paused.started;
  const next = {
    token: 'pst-synthetic-new-current',
    proxyUrl: 'socks5://current:new-secret@127.0.0.1:19100',
  };
  f.configure('summary-current-connection', next);
  const before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() n').get().n;
  paused.release();
  const response = await pending;
  assert.equal(response.status, 200, response.text);
  assert.equal(response.body.accounts[0].token, next.token);
  assert.equal(response.body.accounts[0].proxyUrl, next.proxyUrl);
  assert.ok(!response.text.includes('synthetic-upstream-pst'));
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() n').get().n, changes);
  assert.equal(f.notifications, 0);
});

test('admin summary失败列表先限原插入序50，历史未知数与错误时序不被当前价或后继事件补齐', async (t) => {
  const f = await setup(t, {
    beforeReady: (f) => {
      for (let i = 0; i < 51; i++) {
        f.accept('failed-' + i, { now: NOW + i });
        f.dispatch(`failed-${i}-0`, NOW + i + 100);
        f.fail(`failed-${i}-0`, { aborted: false }, NOW + 1000 - i);
      }
    },
  });
  const readInput = RelationalInputStore.prototype.readErrorLogRequest;
  t.mock.method(RelationalInputStore.prototype, 'readErrorLogRequest', function (id) {
    if (id === 'failed-0-0')
      throw Object.assign(new Error('synthetic 51st input unavailable'), {
        code: 'input_unavailable',
      });
    return readInput.call(this, id);
  });
  const before = databaseState(f.db),
    result = await f.call();
  assert.equal(result.status, 200, result.text);
  assert.equal(result.body.jobs.length, 50);
  assert.equal(result.body.jobs[0].id, 'failed-50-0');
  assert.equal(result.body.jobs.at(-1).id, 'failed-1-0');
  assert.equal(result.body.errorLogs[0].id, 'failed-0-0');
  assert.deepEqual(result.body.jobStats1h, {
    done: 0,
    failed: null,
    total: null,
    successRate: null,
  });
  assert.equal(result.body.accounts[0].stats1h.failed, null);
  assert.equal(result.body.generationStats1m.total, 0);
  assert.equal(result.body.jobs[0].cost, 0);
  assert.equal(result.body.jobs[0].accountCost, null);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.notifications, 0);
});

test('admin summary真实Opening未知time/cost不借导入时间、当前generationCost0或绑定账号补值', async (t) => {
  const f = await setup(t, { beforeReady: openingUnknownFailure }),
    before = databaseState(f.db),
    result = await f.call();
  assert.equal(result.status, 200, result.text);
  assert.deepEqual(result.body.requestStats1m, { total: null });
  assert.deepEqual(result.body.jobStats1h, {
    done: 0,
    failed: null,
    total: null,
    successRate: null,
  });
  assert.deepEqual(result.body.accounts[0].stats1h, {
    done: 0,
    failed: 0,
    total: 0,
    successRate: 0,
  });
  assert.equal(result.body.jobs.length, 1);
  const job = result.body.jobs[0];
  assert.equal(job.source, 'historical');
  assert.equal(job.cost, null);
  assert.equal(job.accountCost, null);
  for (const key of ['createdAt', 'updatedAt', 'completedAt', 'durationMs', 'accountId'])
    assert.equal(job[key], null, key);
  assert.equal(job.prompt, null);
  assert.equal(job.model, 'nai-diffusion-5-full');
  assert.equal(databaseState(f.db), before);
  assert.equal(f.notifications, 0);
});

test('admin summary完整公共回放不恢复认证，选定同cut私有恢复/重开后实际HTTP值一致', async (t) => {
  const f = await setup(t, {
      beforeReady: async (f) => {
        f.accept('replayed-summary');
        f.dispatch('replayed-summary-0');
        await f.publish('replayed-summary-0');
      },
    }),
    expected = await f.call(),
    targetPath = path.join(f.dir, 'summary-replayed.sqlite'),
    target = new Database(targetPath);
  f.addCleanup(() => {
    if (target.open) target.close();
  });
  initializeRelationalStorage(target, 'generation-history');
  const replay = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) seq FROM commits ORDER BY commit_seq')
    .all())
    target.transaction(() => replay.replayCommit(readRelationalCommit(f.db, seq), inputs))();
  assert.equal(relationalPrivateStateEmpty(target), true);
  assert.equal(target.prepare('SELECT COUNT(*) n FROM api_keys').get().n, 0);
  assert.deepEqual(
    new RelationalReportStore(target).readAdminSummaryStats(f.clock),
    new RelationalReportStore(f.db).readAdminSummaryStats(f.clock),
  );
  await verifyAndInstallRelationalStorage(target, f.selected);
  f.setDatabase(target);
  assert.deepEqual((await f.call()).body, expected.body);
  const before = databaseState(target),
    changes = target.prepare('SELECT total_changes() n').get().n;
  assert.deepEqual((await f.call(route + '?fresh=1')).body, expected.body);
  assert.equal(databaseState(target), before);
  assert.equal(target.prepare('SELECT total_changes() n').get().n, changes);
  target.close();
  const reopened = new Database(targetPath);
  f.addCleanup(() => reopened.close());
  f.setDatabase(reopened);
  assert.deepEqual((await f.call()).body, expected.body);
});

test('admin summary统计故障不补零或提前回传reveal，保持零写与零通知', async (t) => {
  const f = await setup(t),
    before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() n').get().n;
  t.mock.method(RelationalReportStore.prototype, 'readAdminSummaryStats', () => {
    throw new Error('synthetic stats source unavailable');
  });
  const result = await f.call(route + '?revealTokens=1');
  assert.equal(result.status, 500);
  assert.ok(!result.text.includes('synthetic-upstream-pst'));
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() n').get().n, changes);
  assert.equal(f.notifications, 0);
});
