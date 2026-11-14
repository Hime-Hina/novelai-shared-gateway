import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createServer, request as httpRequest } from 'node:http';
import { once, EventEmitter } from 'node:events';
import path from 'node:path';
import { readFile } from 'node:fs/promises';
import { fixture, meta, keys, NOW, databaseState } from '../helpers/relational-generation.mjs';
import { initializeFixtureConfiguration } from '../helpers/relational-configuration.mjs';
import {
  authenticateMemberApiKey,
  createMemberSession,
  withAuthenticatedMemberApiKey,
  logoutMemberSession,
} from '../../server/members/authentication.ts';
import { createRelationalMemberReadHttp } from '../../server/members/relational-read-http.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import {
  assertRelationalStorageReady,
  initializeRelationalStorage,
} from '../../server/storage/relational-storage.ts';
import { relationalSnapshotSha256 } from '../../server/storage/relational-private-restore.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { publicErrorMessage } from '../../server/http/public-errors.ts';
import { RelationalReportStore } from '../../server/storage/relational-report-store.ts';
import { readActivityHeatmap, activityDate } from '../../server/reports/activity-heatmap.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { domainDigest } from '../../server/events/domain-events.ts';
import {
  normalizeMemberGeneration,
  materializeMemberGeneration,
} from '../../server/generation/member-contract.ts';

async function setup(
  t,
  {
    env = {},
    beforeReady,
    discordFetch,
    seconds = 0,
    v5 = '40',
    secondV5 = '40',
    fallback = false,
    openingAt = NOW,
    now = () => NOW + 30000,
  } = {},
) {
  const cleanup = [],
    f = await fixture(
      { after: (fn) => cleanup.push(fn) },
      { seconds, v5, secondV5, fallback, openingAt, env },
    ),
    sessions = {};
  await f.accept('existing', { now: openingAt });
  f.dispatch('existing-0', openingAt + 1);
  await f.publish('existing-0', openingAt + 3);
  await beforeReady?.(f);
  for (const id of ['m1', 'm2'])
    sessions[id] = await createMemberSession(
      f.members,
      await authenticateMemberApiKey(f.members, keys[id]),
      env,
      NOW,
    );
  const sourcePath = path.join(f.dir, 'read-http-selected.sqlite');
  initializeFixtureConfiguration(f.db);
  await f.db.backup(sourcePath);
  const sealed = new Database(sourcePath);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  const selected = {
    sourcePath,
    sourceSha256: relationalSnapshotSha256(sourcePath),
    historyId: 'generation-history',
    finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get()
      .seq,
    replicas: { dataDir: f.dir },
  };
  await verifyAndInstallRelationalStorage(f.db, selected);
  assertRelationalStorageReady(f.db);
  const signed = [],
    configuration = { publicBaseUrl: 'https://entry.example/root///' };
  const handle = createRelationalMemberReadHttp({
    sqlite: f.db,
    events: new EventEmitter(),
    env,
    discordFetch,
    get publicBaseUrl() {
      return configuration.publicBaseUrl;
    },
    now,
    // 本测试只记录已授权 URL capability 调用；15 分钟签名及图片端点总装配另验。
    publicImageUrl: (id) => {
      signed.push(id);
      return `/configured-image/${encodeURIComponent(id)}`;
    },
  });
  let activeHandle = handle;
  const server = createServer(async (req, res) => {
    try {
      if (!(await activeHandle(req, res, new URL(req.url, `http://${req.headers.host}`)))) {
        res.writeHead(404);
        res.end();
      }
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
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    for (const close of cleanup.reverse()) await close();
  });
  const origin = `http://127.0.0.1:${server.address().port}`;
  const call = (
    url,
    { member = 'm1', cookie = sessions[member].token, headers = {}, method = 'GET', body } = {},
  ) =>
    new Promise((resolve, reject) => {
      const payload = body === undefined ? undefined : JSON.stringify(body),
        request = httpRequest(
          origin + url,
          {
            method,
            headers: {
              cookie: `nai_member_session=${cookie}`,
              origin,
              ...(payload === undefined
                ? {}
                : {
                    'content-type': 'application/json',
                    'content-length': Buffer.byteLength(payload),
                  }),
              ...headers,
            },
          },
          (response) => {
            let text = '';
            response.setEncoding('utf8');
            response.on('data', (chunk) => {
              text += chunk;
            });
            response.on('end', () =>
              resolve({
                status: response.statusCode,
                headers: new Headers(
                  Object.entries(response.headers)
                    .filter(([, value]) => value !== undefined)
                    .map(([key, value]) => [key, String(value)]),
                ),
                body: text ? JSON.parse(text) : null,
                text,
              }),
            );
          },
        );
      request.on('error', reject);
      request.end(payload);
    });
  return {
    ...f,
    sessions,
    signed,
    configuration,
    handle,
    call,
    selected,
    setHandler(next) {
      activeHandle = next;
    },
  };
}

test('heatmap实际GET复用原DTO/日期参数、Cookie本人范围，body和Key不提权且不续期/读正文', async (t) => {
  const f = await setup(t, {
      beforeReady: async (f) => {
        await f.accept('foreign-heatmap', { memberId: 'm2', samples: 2 });
        const image = await readFile(new URL('../fixtures/synthetic.webp', import.meta.url));
        for (const operation of ['upscale', 'encode-vibe']) {
          const normalized = normalizeMemberGeneration({
            operation,
            image: image.toString('base64'),
            ...(operation === 'encode-vibe' ? { model: 'nai-diffusion-4-5-full' } : {}),
          });
          await withAuthenticatedMemberApiKey(
            f.members,
            keys.m1,
            () =>
              f.generations.acceptGeneration(meta('heatmap-' + operation, NOW, 'm1'), {
                inputId: 'input-heatmap-' + operation,
                requestId: 'heatmap-' + operation,
                memberId: 'm1',
                source: 'native',
                seedMode: 'not_applicable',
                confirmation: null,
                request: materializeMemberGeneration(normalized),
                context: { kind: 'native', native: normalized.normalizationContext.native },
                jobs: [
                  {
                    jobId: 'heatmap-' + operation + '-0',
                    status: 'queued',
                    queueSeq: null,
                    queueExpiresAt: null,
                    deadlineAt: null,
                    admissionEpoch: 0,
                    cacheKey: null,
                  },
                ],
              }),
            {},
          );
        }
      },
    }),
    before = f.db.serialize(),
    changes = f.db.prepare('SELECT total_changes() n').get().n,
    statements = [],
    prepare = f.db.prepare.bind(f.db);
  t.mock.method(f.db, 'prepare', (sql) => {
    statements.push(sql);
    return prepare(sql);
  });
  const result = await f.call('/api/member/heatmap?key=' + encodeURIComponent(keys.m2), {
    headers: { 'x-member-activity': '1', authorization: `Bearer ${keys.m2}` },
    body: { memberId: 'm2', apiKey: keys.m2 },
  });
  assert.equal(result.status, 200, result.text);
  assert.deepEqual(
    result.body,
    await readActivityHeatmap(
      new RelationalReportStore(f.db),
      new URLSearchParams(),
      { memberId: 'm1', allowMemberFilter: false },
      NOW + 30000,
    ),
  );
  assert.equal(
    result.body.days.reduce((total, day) => total + day.total, 0),
    1,
  );
  assert.equal(result.body.memberId, 'm1');
  assert.equal(result.body.operation, 'generate');
  assert.equal(result.body.coverage, 'retained-records');
  assert.equal(result.headers.get('set-cookie'), null);
  assert.equal(result.headers.get('cache-control'), 'no-store');
  assert.deepEqual(f.signed, []);
  const date = activityDate(new Date(NOW).toISOString());
  assert.deepEqual((await f.call('/api/member/heatmap?dates=' + date)).body.days, result.body.days);
  for (const query of [
    'memberId=m2',
    'memberId=m1',
    'memberId=',
    'memberId=m1&memberId=m2',
    'operation=bad',
    'operation=generate&operation=upscale',
    'dates=',
    'dates=2026-02-30',
    'dates=1900-01-01',
    `dates=${date}&dates=${date}`,
  ]) {
    assert.equal((await f.call('/api/member/heatmap?' + query)).status, 400, query);
  }
  for (const operation of ['upscale', 'encode-vibe']) {
    const response = await f.call('/api/member/heatmap?operation=' + operation);
    assert.equal(response.status, 200, response.text);
    assert.equal(response.body.operation, operation);
    assert.equal(
      response.body.days.reduce((total, day) => total + day.total, 0),
      1,
    );
  }
  assert.equal(
    (
      await f.call('/api/member/heatmap', {
        cookie: '',
        headers: { authorization: `Bearer ${keys.m1}` },
      })
    ).status,
    401,
  );
  assert.equal(
    (await f.call('/api/member/heatmap', { headers: { origin: 'https://foreign.example' } }))
      .status,
    403,
  );
  assert.equal((await f.call('/api/member/heatmap', { method: 'POST' })).status, 403);
  assert.ok(
    statements.every(
      (sql) =>
        !/generation_inputs|input_assets|normalization_contexts|\bbody\b|\bbytes\b/i.test(sql),
    ),
  );
  for (const secret of [keys.m1, keys.m2, 'private generation prompt', 'synthetic-upstream-pst']) {
    assert.ok(!result.text.includes(secret));
  }
  assert.deepEqual(f.db.serialize(), before);
  assert.equal(f.db.prepare('SELECT total_changes() n').get().n, changes);
  f.db.transaction(() => {
    f.members.replaceMemberApiKey('m1', 'rotated-heatmap-key');
    f.members.replaceMemberApiKey('m2', keys.m1);
  })();
  const own = await f.call('/api/member/heatmap'),
    foreign = await f.call('/api/member/heatmap', { member: 'm2' });
  assert.equal(own.body.memberId, 'm1');
  assert.equal(
    own.body.days.reduce((total, day) => total + day.total, 0),
    1,
  );
  assert.equal(foreign.body.memberId, 'm2');
  assert.equal(
    foreign.body.days.reduce((total, day) => total + day.total, 0),
    2,
  );
});

test('heatmap的真实SQL异步返回后重核Cookie/epoch/active/ready，不返回失效桶', async (t) => {
  for (const kind of ['cookie', 'epoch', 'active', 'ready']) {
    await t.test(kind, async (t) => {
      const f = await setup(t),
        original = RelationalReportStore.prototype.readActivityDays;
      t.mock.method(RelationalReportStore.prototype, 'readActivityDays', async function (...args) {
        const data = await original.apply(this, args);
        assert.equal(f.db.inTransaction, false);
        if (kind === 'cookie') {
          await logoutMemberSession(f.members, f.sessions.m1.token);
        }
        if (kind === 'epoch') {
          const prepared = await f.members.prepareMemberCredentials('m1', {
            username: 'heatmap.rotated',
            password: 'synthetic-heatmap-rotated-password',
          });
          f.db.transaction(() => f.members.setMemberCredentials(prepared))();
        }
        if (kind === 'active') {
          f.db.transaction(() =>
            f.members.setMemberStatus(meta('heatmap-disable'), 'm1', 'disabled'),
          )();
        }
        if (kind === 'ready') {
          f.db.prepare("UPDATE projection_state SET status='failed'").run();
        }
        return data;
      });
      const result = await f.call('/api/member/heatmap');
      assert.equal(result.status, kind === 'ready' ? 500 : 401, result.text);
      assert.equal(Object.hasOwn(result.body, 'days'), false);
    });
  }
});

test('heatmap按真实child/Opening原0和NULL、取消及退休分类，不借父时间补日期', async (t) => {
  let clock = NOW + 30000;
  const f = await setup(t, {
      now: () => clock,
      beforeReady: async (f) => {
        await f.accept('heatmap-cancel', { samples: 2 });
        f.db.transaction(() =>
          f.generations.cancelWaitingGeneration(
            meta('heatmap-cancel', NOW + 10, 'm1'),
            'heatmap-cancel',
          ),
        )();
        await f.accept('heatmap-retired');
        f.fail('heatmap-retired-0', { aborted: false });
        f.db.transaction(() =>
          f.generations.retireJobHistory(
            { ...meta('heatmap-retire', NOW + 20), actorKind: 'admin', actorId: 'local-admin' },
            ['heatmap-retired-0'],
            null,
            new Map(),
          ),
        )();
        await f.accept('heatmap-foreign', { memberId: 'm2' });
        const original = f.db
            .prepare("SELECT * FROM generation_requests WHERE id='existing'")
            .get(),
          events = [
            {
              eventType: 'OpeningRequestRecorded',
              streamId: 'request:heatmap-history',
              payload: {
                requestId: 'heatmap-history',
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
                origin: { sourceId: 'synthetic-opening', originalId: 'original-heatmap-request' },
              },
            },
            ...[0, null].map((createdAt, sampleIndex) => ({
              eventType: 'OpeningJobRecorded',
              streamId: 'request:heatmap-history',
              payload: {
                jobId: 'heatmap-history-' + sampleIndex,
                requestId: 'heatmap-history',
                createdAt,
                sampleIndex,
                actualSeed: null,
                status: 'failed',
                queueSeq: null,
                queueExpiresAt: null,
                deadlineAt: null,
                admissionEpoch: 0,
                artifactId: null,
                cacheKey: null,
                completedAt: 0,
                errorCode: 'original_failure',
                errorStatus: 500,
                cancellationReason: null,
                lateResultAt: null,
                freeSucceededAt: null,
                freeWarningThreshold: null,
                retiredAt: null,
                origin: {
                  sourceId: 'synthetic-opening',
                  originalId: 'original-heatmap-' + sampleIndex,
                },
              },
            })),
          ],
          { policyVersion, pricingVersion, ...command } = meta(
            'heatmap-original-history',
            NOW + 1000,
          );
        f.db.transaction(() =>
          new RelationalCommitReplayer(f.db).appendCommit(
            {
              ...command,
              fingerprint: domainDigest(events),
              receipt: { opened: true },
              expectedVersions: { 'request:heatmap-history': '0' },
              events: events.map((event) => ({
                ...event,
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
      },
    }),
    before = f.db.serialize(),
    current = await f.call('/api/member/heatmap');
  assert.equal(current.status, 200, current.text);
  assert.deepEqual(current.body.days, [
    {
      date: activityDate(new Date(NOW).toISOString()),
      total: 3,
      done: 1,
      failed: 0,
      cancelled: 2,
      queued: 0,
      pending: 0,
      running: 0,
      other: 0,
    },
  ]);
  clock = 0;
  const originalZero = await f.call('/api/member/heatmap?dates=1970-01-01', {
    headers: { 'x-member-activity': '1' },
  });
  assert.equal(originalZero.status, 200, originalZero.text);
  assert.equal(originalZero.body.asOf, '1970-01-01T00:00:00.000Z');
  assert.deepEqual(originalZero.body.days, [
    {
      date: '1970-01-01',
      total: 1,
      done: 0,
      failed: 1,
      cancelled: 0,
      queued: 0,
      pending: 0,
      running: 0,
      other: 0,
    },
  ]);
  assert.equal(originalZero.headers.get('set-cookie'), null);
  assert.equal(
    f.db
      .prepare("SELECT created_at FROM generation_job_origins WHERE job_id='heatmap-history-1'")
      .get().created_at,
    null,
  );
  assert.deepEqual(f.db.serialize(), before);
});

test('heatmap完整公共回放不复活认证，选定私有同cut恢复和重开后本人结果一致', async (t) => {
  let target, reopened;
  t.after(() => {
    if (target?.open) {
      target.close();
    }
    if (reopened?.open) {
      reopened.close();
    }
  });
  const f = await setup(t, {
      beforeReady: async (f) => {
        await f.accept('heatmap-replay-foreign', { memberId: 'm2', samples: 2 });
      },
    }),
    original = await f.call('/api/member/heatmap'),
    targetPath = path.join(f.dir, 'heatmap-replayed.sqlite');
  target = new Database(targetPath);
  initializeRelationalStorage(target, 'generation-history');
  const replay = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replay.replayCommit(readRelationalCommit(f.db, seq), inputs))();
  }
  for (const table of [
    'api_keys',
    'password_credentials',
    'upstream_credentials',
    'discord_bindings',
    'member_sessions',
  ]) {
    assert.equal(target.prepare('SELECT COUNT(*) n FROM ' + table).get().n, 0);
  }
  assert.equal(
    target.prepare('SELECT COUNT(*) n FROM members WHERE auth_epoch IS NOT NULL').get().n,
    0,
  );
  const deps = {
    sqlite: target,
    events: new EventEmitter(),
    env: {},
    publicBaseUrl: '',
    publicImageUrl: () => assert.fail('heatmap cannot sign images'),
    now: () => NOW + 30000,
  };
  assert.throws(() => createRelationalMemberReadHttp(deps), /incomplete/);
  await verifyAndInstallRelationalStorage(target, f.selected);
  f.setHandler(createRelationalMemberReadHttp(deps));
  const before = target.serialize(),
    restored = await f.call('/api/member/heatmap');
  assert.equal(restored.status, 200, restored.text);
  assert.deepEqual(restored.body, original.body);
  assert.deepEqual(target.serialize(), before);
  target.close();
  reopened = new Database(targetPath);
  f.setHandler(createRelationalMemberReadHttp({ ...deps, sqlite: reopened }));
  const repeated = await f.call('/api/member/heatmap');
  assert.equal(repeated.status, 200, repeated.text);
  assert.deepEqual(repeated.body, original.body);
  assert.deepEqual(reopened.serialize(), before);
});

test('heatmap真实Discord IO临时失败或退出不读取/返回桶，允许认证owner独立保存原观察', async (t) => {
  t.mock.method(Date, 'now', () => NOW + 30000);
  const env = {
    DISCORD_CLIENT_ID: '123456789012345678',
    DISCORD_CLIENT_SECRET: 'synthetic-heatmap-discord-secret',
    DISCORD_GUILD_ID: '1134557553011998840',
    PUBLIC_BASE_URL: 'https://member.example',
  };
  let f,
    temporary = true,
    calls = 0,
    reportReads = 0;
  const original = RelationalReportStore.prototype.readActivityDays;
  t.mock.method(RelationalReportStore.prototype, 'readActivityDays', function (...args) {
    reportReads++;
    return original.apply(this, args);
  });
  f = await setup(t, {
    env,
    beforeReady: async (f) => {
      await f.members.discordAuthenticationTransaction((transaction) => {
        const member = transaction.readMember('m1');
        member.discord = {
          id: '222222222222222222',
          guildId: env.DISCORD_GUILD_ID,
          accessToken: 'synthetic-heatmap-access',
          refreshToken: 'synthetic-heatmap-refresh',
          expiresAt: NOW + 3600000,
          checkedAt: 0,
          member: true,
        };
        transaction.saveAuthentication(member, 'login');
      });
    },
    discordFetch: async () => {
      calls++;
      assert.equal(f.db.inTransaction, false);
      if (temporary) {
        return new Response('private heatmap remote diagnostic', { status: 503 });
      }
      await logoutMemberSession(f.members, f.sessions.m1.token);
      return Response.json({ user: { id: '222222222222222222' } });
    },
  });
  const financial = () =>
      JSON.stringify(
        ['wallets', 'charges', 'generation_jobs', 'generation_requests', 'account_quota_state'].map(
          (table) => f.db.prepare('SELECT * FROM ' + table + ' ORDER BY rowid').all(),
        ),
      ),
    before = financial(),
    unavailable = await f.call('/api/member/heatmap');
  assert.equal(unavailable.status, 503, unavailable.text);
  assert.ok(!unavailable.text.includes('private heatmap remote diagnostic'));
  temporary = false;
  const revoked = await f.call('/api/member/heatmap', { headers: { 'x-member-activity': '1' } });
  assert.equal(revoked.status, 401, revoked.text);
  assert.equal(Object.hasOwn(revoked.body, 'days'), false);
  assert.equal(revoked.headers.get('set-cookie'), null);
  assert.equal(reportReads, 0);
  assert.equal(calls, 2);
  assert.deepEqual(f.signed, []);
  assert.equal(financial(), before);
});

test('合法 ready 的真实 history 子页与 late 查询复用原解析/DTO，私有字段不出网且普通读取不续期', async (t) => {
  const f = await setup(t, {
    beforeReady: async (f) => {
      await f.accept('batch', { samples: 3, now: NOW + 20000 });
      f.dispatch('batch-0', NOW + 20001);
      f.fail('batch-0', { aborted: false }, NOW + 20002);
      await f.publish('batch-0', NOW + 20003);
      await f.accept('foreign', { memberId: 'm2', now: NOW + 30000 });
    },
  });
  const before = databaseState(f.db),
    statements = [],
    prepare = f.db.prepare;
  f.db.prepare = function (sql) {
    statements.push(sql);
    return prepare.call(this, sql);
  };
  let first, second, late;
  try {
    first = await f.call('/api/member/history?pageSize=2&x=ignored', {
      headers: { 'x-member-activity': '1' },
    });
    second = await f.call(
      `/api/member/history?page=2&pageSize=2&until=${encodeURIComponent(first.body.until)}&memberId=m2`,
    );
    late = await f.call('/api/member/history?status=late');
    assert.equal((await f.call('/api/member/history?q=%20BATCH%20')).body.total, 3);
    assert.equal((await f.call('/api/member/history?q=%25')).body.total, 0);
  } finally {
    f.db.prepare = prepare;
  }
  assert.equal(first.status, 200);
  assert.equal(first.body.total, 4);
  assert.equal(first.body.page, 1);
  assert.equal(first.body.pageSize, 2);
  assert.equal(new Set([...first.body.jobs, ...second.body.jobs].map((j) => j.id)).size, 4);
  assert.deepEqual(
    late.body.jobs.map((j) => j.id),
    ['batch-0'],
  );
  assert.equal(late.body.jobs[0].status, 'failed');
  assert.equal(late.body.jobs[0].charge.status, 'refunded');
  assert.equal(late.body.jobs[0].lateResultAvailable, true);
  assert.equal(late.body.jobs[0].imageUrl, '/configured-image/artifact-batch-0');
  assert.equal((await f.call('/api/member/history', { member: 'm2' })).body.total, 1);
  assert.equal(first.headers.get('cache-control'), 'no-store');
  assert.equal(first.headers.get('x-content-type-options'), 'nosniff');
  assert.equal(first.headers.has('set-cookie'), false);
  assert.ok(f.signed.every((id) => ['artifact-existing-0', 'artifact-batch-0'].includes(id)));
  for (const response of [first, second, late])
    for (const secret of [
      'synthetic-key',
      'private generation prompt',
      'synthetic-upstream-pst',
      'imageId',
      'original_member_id',
      'userToken',
    ])
      assert.equal(response.text.includes(secret), false);
  assert.equal(
    statements.some((sql) => /generation_inputs|input_assets|\bbody\b|\bbytes\b/i.test(sql)),
    false,
  );
  assert.equal(databaseState(f.db), before);
  for (const query of [
    'page=0',
    'pageSize=101',
    'status=cancelled',
    'sort=token',
    'order=bad',
    'until=bad',
    'q=' + 'x'.repeat(201),
  ]) {
    assert.equal((await f.call('/api/member/history?' + query)).status, 400);
  }
});

test('真实 key 只返回当前私有 Key 与明确配置地址，轮换不改变原Cookie/历史、不补造缺失内容', async (t) => {
  const f = await setup(t),
    original = await f.call('/api/member/key', { headers: { 'x-member-activity': '1' } });
  assert.deepEqual(original.body, { apiKey: keys.m1, baseUrl: 'https://entry.example/root/v1' });
  assert.equal(original.headers.has('set-cookie'), false);
  f.configuration.publicBaseUrl = 'https://entry.example/v1///';
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'exact-current-key'))();
  const before = databaseState(f.db),
    current = await f.call('/api/member/key');
  assert.deepEqual(current.body, {
    apiKey: 'exact-current-key',
    baseUrl: 'https://entry.example/v1',
  });
  assert.equal((await f.call('/api/member/history')).body.total, 1);
  assert.equal(databaseState(f.db), before);
  f.configuration.publicBaseUrl = '';
  assert.equal((await f.call('/api/member/key')).body.baseUrl, '');
  f.db.prepare("DELETE FROM api_keys WHERE member_id='m1'").run();
  const missing = databaseState(f.db),
    unavailable = await f.call('/api/member/key');
  assert.equal(unavailable.status, 503);
  assert.equal(unavailable.body.apiKey, undefined);
  assert.equal(databaseState(f.db), missing);
  assert.equal((await f.call('/api/member/history')).status, 200);
  assert.deepEqual(f.signed, ['artifact-existing-0', 'artifact-existing-0']);
});

test('真实 HTTP 保留 Cookie/Origin/方法/当前撤销及 ready 边界，不接受 Bearer 代替会话', async (t) => {
  const f = await setup(t);
  assert.equal(
    (
      await f.call('/api/member/history', {
        cookie: '',
        headers: { authorization: `Bearer ${keys.m1}` },
      })
    ).status,
    401,
  );
  assert.equal(
    (await f.call('/api/member/key', { headers: { origin: 'https://foreign.example' } })).status,
    403,
  );
  assert.equal((await f.call('/api/member/key', { method: 'POST' })).status, 403);
  await logoutMemberSession(f.members, f.sessions.m1.token);
  const before = databaseState(f.db),
    signed = f.signed.length;
  assert.equal((await f.call('/api/member/key')).status, 401);
  assert.equal((await f.call('/api/member/history')).status, 401);
  assert.equal(f.signed.length, signed);
  assert.equal(databaseState(f.db), before);
  assert.equal((await f.call('/api/member/not-implemented', { member: 'm2' })).status, 404);
  f.db.prepare("UPDATE projection_state SET status='building' WHERE id=1").run();
  const notReady = databaseState(f.db);
  assert.equal((await f.call('/api/member/history', { member: 'm2' })).status, 500);
  assert.equal(databaseState(f.db), notReady);
  assert.throws(
    () =>
      createRelationalMemberReadHttp({ sqlite: f.db, publicBaseUrl: '', publicImageUrl: () => '' }),
    /incomplete or incompatible/i,
  );
});

test('Discord IO 返回后必须重核当前会话，临时503及等待中退出/停用均不泄露Key或签发URL', async (t) => {
  const env = {
      DISCORD_CLIENT_ID: '123456789012345678',
      DISCORD_CLIENT_SECRET: 'synthetic-discord-secret',
      DISCORD_GUILD_ID: '1134557553011998840',
      PUBLIC_BASE_URL: 'https://member.example',
    },
    state = { fail: true, beforeGrant: undefined, calls: 0 };
  const f = await setup(t, {
    env,
    beforeReady: async (f) => {
      await f.members.discordAuthenticationTransaction((transaction) => {
        const member = transaction.readMember('m1');
        member.discord = {
          id: '222222222222222222',
          guildId: env.DISCORD_GUILD_ID,
          accessToken: 'synthetic-discord-access',
          refreshToken: 'synthetic-discord-refresh',
          expiresAt: Date.now() + 3600000,
          checkedAt: 0,
          member: true,
        };
        transaction.saveAuthentication(member, 'login');
      });
      // m2 不通过这些受保护 HTTP，仍需可签发的合法绑定用于当前 fixture 私有快照。
    },
    discordFetch: async () => {
      assert.equal(f.db.inTransaction, false);
      state.calls++;
      if (state.fail) return new Response('private remote diagnostic', { status: 503 });
      await state.beforeGrant?.();
      return Response.json({ user: { id: '222222222222222222' } });
    },
  });
  const transient = await f.call('/api/member/key');
  assert.equal(transient.status, 503);
  assert.equal(transient.text.includes('private remote diagnostic'), false);
  assert.equal(f.signed.length, 0);
  state.fail = false;
  state.beforeGrant = () => logoutMemberSession(f.members, f.sessions.m1.token);
  const loggedOut = await f.call('/api/member/key');
  assert.equal(loggedOut.status, 401);
  assert.equal(loggedOut.body.apiKey, undefined);
  assert.equal(f.members.readMember('m1').status, 'active');
  await f.members.discordAuthenticationTransaction((transaction) => {
    const member = transaction.readMember('m1');
    member.discord.checkedAt = 0;
    transaction.saveAuthentication(member);
  });
  const renewed = await createMemberSession(
    f.members,
    await authenticateMemberApiKey(f.members, keys.m1),
    env,
    NOW,
  );
  state.beforeGrant = () =>
    f.db.transaction(() =>
      f.members.setMemberStatus(meta('disable-during-discord'), 'm1', 'disabled'),
    )();
  const denied = await f.call('/api/member/history', { cookie: renewed.token });
  assert.equal(denied.status, 401);
  assert.equal(f.signed.length, 0);
  assert.equal(denied.text.includes(keys.m1), false);
  assert.equal(state.calls, 3);
});

test('免费用量原owner在策略关闭后仍保留成功事实而不展示已禁用警告', async (t) => {
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { freeLimit: 1, seconds: 0, env });
  await f.accept('free', { steps: 23 });
  f.dispatch('free-0');
  await f.publish('free-0');
  // 校验现唯一内部 usage owner；当前报价只消费 usage，不对外发布此 DTO。
  const usage = () => f.generations.readMemberFreeImageUsage('m1', NOW + 10010);
  assert.equal(usage().warning, true);
  const quotas = new RelationalQuotaStore(f.db, env);
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    (member) =>
      quotas.configureQuotaPolicy(
        meta('disable-free-policy', NOW + 10000, member.id),
        {
          action: 'free-image-policy',
          expectedVersion: 1,
          policy: { enabled: false, warningThreshold: 1, hardLimit: 1 },
        },
        member,
      ),
    env,
  );
  const before = databaseState(f.db),
    result = usage();
  assert.equal(result.enabled, false);
  assert.equal(result.successful, 1);
  assert.equal(result.warning, false);
  assert.equal(databaseState(f.db), before);
  assert.throws(() => f.generations.readMemberFreeImageUsage('missing', NOW + 10010), {
    statusCode: 401,
  });
  f.db.transaction(() =>
    f.members.setMemberStatus(meta('disable-free-reader'), 'm1', 'disabled'),
  )();
  assert.throws(() => usage(), { statusCode: 401 });
});

test('me 实际 held 期间仍读当前金融摘要，recent30 按 child 跨31父，不修改业务账务', async (t) => {
  const env = { QUEUE_MAX_ACTIVE_PER_MEMBER: '40', QUEUE_MAX_ACTIVE_GLOBAL: '40' },
    f = await setup(t, {
      env,
      beforeReady: async (f) => {
        const prepared = await f.members.prepareMemberCredentials('m1', {
          username: 'profile.canonical',
          password: 'synthetic-profile-password',
        });
        f.db.transaction(() => f.members.setMemberCredentials(prepared))();
        for (let i = 0; i < 31; i++)
          await f.accept(`page-${String(i).padStart(2, '0')}`, { now: NOW + 20000 + i });
        assert.ok(f.dispatch('page-00-0', NOW + 20001).dispatch);
      },
    }),
    before = databaseState(f.db),
    result = await f.call('/api/member/me?memberId=m2');
  assert.equal(result.status, 200);
  assert.equal(result.body.username, 'profile.canonical');
  assert.equal(result.body.owner, false);
  assert.equal(result.body.acceptingJobs, true);
  assert.equal(result.body.sessionExpiresAt, f.sessions.m1.expiresAt);
  assert.equal(result.body.jobs.length, 30);
  assert.equal(result.body.jobs[0].id, 'page-30-0');
  assert.equal(result.body.jobs.at(-1).id, 'page-01-0');
  assert.ok(result.body.jobs.every((job) => job.imageId === '' && job.imageUrl === ''));
  assert.equal(
    result.body.quota.anlasFixedAvailable,
    Number(f.generations.readWallet('a', 'm1').anlasFixed),
  );
  assert.equal(
    result.body.quota.anlasFixedReserved,
    Number(f.generations.readCharge('page-00-0').anlasFixed),
  );
  assert.equal(result.body.quota.anlasFixedReserved > 0, true);
  assert.equal(result.headers.has('set-cookie'), false);
  assert.equal(databaseState(f.db), before);
});

test('me 当前恢复估算沿原summary owner，普通读取零写，仅visible me续期且续期失败不发Cookie', async (t) => {
  let clock = NOW + 300000;
  t.mock.method(Date, 'now', () => clock);
  const f = await setup(t, { seconds: 600, now: () => clock }),
    before = databaseState(f.db),
    quiet = await f.call('/api/member/me');
  assert.equal(quiet.status, 200);
  assert.equal(quiet.body.quota.v5Available > Number(f.generations.readWallet('a', 'm1').v5), true);
  assert.equal(quiet.headers.has('set-cookie'), false);
  assert.equal(databaseState(f.db), before);
  const visible = await f.call('/api/member/me', { headers: { 'x-member-activity': '1' } });
  assert.equal(visible.status, 200);
  assert.equal(visible.body.sessionExpiresAt > quiet.body.sessionExpiresAt, true);
  assert.ok(visible.headers.has('set-cookie'));
  clock += 1000;
  for (const path of ['/api/member/history', '/api/member/key']) {
    assert.equal(
      (await f.call(path, { headers: { 'x-member-activity': '1' } })).headers.has('set-cookie'),
      false,
    );
  }
  assert.equal(
    (await f.call('/api/member/me')).body.sessionExpiresAt,
    visible.body.sessionExpiresAt,
  );
  f.db
    .exec(`CREATE TEMP TRIGGER fail_profile_renew BEFORE UPDATE OF expires_at ON main.member_sessions
    BEGIN SELECT RAISE(ABORT,'synthetic profile renewal persistence failure'); END`);
  const beforeFailure = databaseState(f.db),
    signed = f.signed.length,
    failed = await f.call('/api/member/me', { headers: { 'x-member-activity': '1' } });
  assert.equal(failed.status, 500);
  assert.equal(failed.headers.has('set-cookie'), false);
  assert.equal(f.signed.length, signed);
  assert.equal(databaseState(f.db), beforeFailure);
  f.db.exec('DROP TRIGGER fail_profile_renew');
  await logoutMemberSession(f.members, f.sessions.m1.token);
  const revoked = await f.call('/api/member/me', { headers: { 'x-member-activity': '1' } });
  assert.equal(revoked.status, 401);
  assert.equal(revoked.headers.has('set-cookie'), false);
});

test('me actual late10 保留原退款/失败与 jobs.imageId，只公开本人原结果和现免费owner值', async (t) => {
  const f = await setup(t, {
      now: () => NOW + 300000,
      beforeReady: async (f) => {
        for (let i = 0; i < 11; i++) {
          const id = `profile-late-${String(i).padStart(2, '0')}`,
            at = NOW + 20000 + i * 20000;
          await f.accept(id, { now: at });
          f.dispatch(`${id}-0`, at + 1);
          f.fail(`${id}-0`, { aborted: false }, at + 2);
          await f.publish(`${id}-0`, at + 3);
        }
        await f.accept('profile-foreign', { memberId: 'm2', now: NOW + 250000 });
      },
    }),
    before = databaseState(f.db),
    result = await f.call('/api/member/me');
  assert.equal(result.status, 200);
  assert.equal(result.body.lateResults.length, 10);
  assert.equal(result.body.lateResults[0].id, 'profile-late-10-0');
  assert.equal(result.body.lateResults.at(-1).id, 'profile-late-01-0');
  assert.ok(
    result.body.lateResults.every(
      (job) => job.charge.status === 'refunded' && !Object.hasOwn(job, 'imageId'),
    ),
  );
  const job = result.body.jobs.find((job) => job.id === 'profile-late-10-0');
  assert.equal(job.status, 'failed');
  assert.equal(job.imageId, 'artifact-profile-late-10-0');
  assert.equal(job.imageUrl, '/configured-image/artifact-profile-late-10-0');
  assert.equal(result.body.quota.freeImages.enabled, false);
  assert.equal(result.body.quota.freeImages.warning, false);
  for (const secret of [
    'profile-foreign',
    'synthetic-key',
    'private generation prompt',
    'synthetic-upstream-pst',
    'memberAuth',
    'authEpoch',
  ]) {
    assert.equal(result.text.includes(secret), false);
  }
  assert.equal(databaseState(f.db), before);
});

test('me 实际fallback结算沿同一金融owner公开完整summary及免费owner，读取不回写', async (t) => {
  const f = await setup(t, {
      v5: '0',
      fallback: true,
      beforeReady: async (f) => {
        await f.accept('profile-fallback', {
          model: 'nai-diffusion-5-full',
          steps: 23,
          now: NOW + 20000,
        });
        assert.ok(f.dispatch('profile-fallback-0', NOW + 20001).dispatch);
        await f.publish('profile-fallback-0', NOW + 20003);
      },
    }),
    charge = f.generations.readCharge('profile-fallback-0'),
    quota = new RelationalQuotaStore(f.db).readMemberQuotaSummary('m1', NOW + 30000),
    before = databaseState(f.db),
    result = await f.call('/api/member/me');
  assert.equal(charge.billingMode, 'anlas_fallback');
  assert.equal(charge.status, 'settled');
  assert.equal(result.status, 200);
  assert.equal(result.body.quota.localAnlasFallbackFixedSpent, Number(charge.anlasFixed));
  assert.equal(result.body.quota.localFallbackV5Equivalent, Number(charge.v5Equivalent));
  assert.deepEqual(result.body.quota, {
    ...quota,
    freeImages: f.generations.readMemberFreeImageUsage('m1', NOW + 30000),
  });
  assert.equal(result.headers.has('set-cookie'), false);
  assert.equal(databaseState(f.db), before);
});

test('me 保留真实原创建时间0，普通profile查询不读输入正文或附件BLOB且全表不变', async (t) => {
  const f = await setup(t, { openingAt: 0 }),
    before = databaseState(f.db),
    statements = [],
    prepare = f.db.prepare;
  let result;
  f.db.prepare = function (sql) {
    statements.push(sql);
    return prepare.call(this, sql);
  };
  try {
    result = await f.call('/api/member/me');
  } finally {
    f.db.prepare = prepare;
  }
  assert.equal(result.status, 200);
  assert.equal(result.body.jobs[0].createdAt, new Date(0).toISOString());
  assert.equal(result.body.jobs[0].imageId, 'artifact-existing-0');
  assert.equal(result.body.jobs[0].imageUrl, '/configured-image/artifact-existing-0');
  assert.equal(
    statements.some((sql) => /generation_inputs|input_assets|\bbody\b|\bbytes\b/i.test(sql)),
    false,
  );
  assert.equal(result.headers.has('set-cookie'), false);
  assert.equal(databaseState(f.db), before);
});

test('独立：旧Key转归他人后，原Cookie和当前Key仍各自绑定稳定成员', async (t) => {
  const f = await setup(t, {
    beforeReady: (f) => f.accept('foreign', { memberId: 'm2' }),
  });
  f.db.transaction(() => {
    f.members.replaceMemberApiKey('m1', 'independent-current-m1');
    f.members.replaceMemberApiKey('m2', keys.m1);
  })();
  const before = databaseState(f.db);
  const own = await f.call('/api/member/history?memberId=m2');
  const other = await f.call('/api/member/history', { member: 'm2' });
  assert.deepEqual(
    own.body.jobs.map((j) => j.id),
    ['existing-0'],
  );
  assert.deepEqual(
    other.body.jobs.map((j) => j.id),
    ['foreign-0'],
  );
  assert.equal((await f.call('/api/member/key')).body.apiKey, 'independent-current-m1');
  assert.equal((await f.call('/api/member/key', { member: 'm2' })).body.apiKey, keys.m1);
  assert.equal(databaseState(f.db), before);
});

test('独立：Discord等待中轮换Key后只领取本人的当前Key，旧快照不得覆盖轮换', async (t) => {
  const env = {
    DISCORD_CLIENT_ID: '123456789012345678',
    DISCORD_CLIENT_SECRET: 'synthetic-independent-secret',
    DISCORD_GUILD_ID: '1134557553011998840',
    PUBLIC_BASE_URL: 'https://member.example',
  };
  let fetched = 0;
  const f = await setup(t, {
    env,
    beforeReady: async (f) => {
      await f.members.discordAuthenticationTransaction((transaction) => {
        const member = transaction.readMember('m1');
        member.discord = {
          id: '222222222222222222',
          guildId: env.DISCORD_GUILD_ID,
          accessToken: 'synthetic-independent-access',
          refreshToken: 'synthetic-independent-refresh',
          expiresAt: Date.now() + 3600000,
          checkedAt: 0,
          member: true,
        };
        transaction.saveAuthentication(member, 'login');
      });
    },
    discordFetch: async () => {
      fetched++;
      assert.equal(f.db.inTransaction, false);
      f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'rotated-during-io'))();
      return Response.json({ user: { id: '222222222222222222' } });
    },
  });
  const originalExpiry = f.db
    .prepare('SELECT expires_at FROM member_sessions WHERE member_id=?')
    .get('m1').expires_at;
  const response = await f.call('/api/member/key', { headers: { 'x-member-activity': '1' } });
  assert.equal(response.status, 200);
  assert.equal(response.body.apiKey, 'rotated-during-io');
  assert.equal(f.members.readMemberApiKey('m1'), 'rotated-during-io');
  assert.equal(await authenticateMemberApiKey(f.members, keys.m1), null);
  assert.equal(
    f.db.prepare('SELECT expires_at FROM member_sessions WHERE member_id=?').get('m1').expires_at,
    originalExpiry,
  );
  assert.equal(response.headers.has('set-cookie'), false);
  assert.equal(fetched, 1);
});
