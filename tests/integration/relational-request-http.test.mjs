import test from 'node:test';
import assert from 'node:assert/strict';
import { request } from 'node:http';
import Database from 'better-sqlite3';
import { setup, input, keys, waitFor } from '../helpers/relational-api-http.mjs';
import { image, meta, databaseState } from '../helpers/relational-generation.mjs';
import { createRelationalRequestHttp } from '../../server/generation/relational-request-http.ts';
import { createRelationalNativeHttp } from '../../server/generation/relational-native-http.ts';
import { createRelationalNativeAdmission } from '../../server/generation/relational-native-admission.ts';
import { createRelationalDirectHttp } from '../../server/generation/relational-direct-http.ts';
import { createPublicImageAccess } from '../../server/http/public-image.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { relationalStateNotificationsFor } from '../../server/events/relational-state-notifications.ts';
import { stateEventsFor } from '../../server/events/state-notifications.ts';
import {
  createMemberSession,
  authenticateMemberApiKey,
} from '../../server/members/authentication.ts';

async function fixture(t, { env = {}, discordFetch } = {}) {
  let f,
    currentEnv,
    signed = 0;
  const limits = [],
    hooks = { body: null };
  f = await setup(t, {
    env: { PUBLIC_MODE: 'true', ...env },
    createHttp: (deps) => {
      currentEnv = deps.env;
      const signer = createPublicImageAccess(deps.env, deps.now),
        sendJson = (res, status, value) => {
          res.writeHead(status, {
            'content-type': 'application/json',
            'cache-control': 'no-store',
          });
          res.end(JSON.stringify(value));
        },
        readJson = async (req, limit) => {
          limits.push(limit);
          const chunks = [];
          let size = 0;
          for await (const chunk of req) {
            size += chunk.length;
            if (size > limit) {
              throw Object.assign(new Error('请求体超过大小限制'), { statusCode: 413 });
            }
            chunks.push(chunk);
            if (hooks.body) {
              await hooks.body();
            }
          }
          if (!size) {
            return {};
          }
          try {
            return JSON.parse(Buffer.concat(chunks).toString('utf8'));
          } catch {
            throw Object.assign(new Error('invalid JSON body.'), { statusCode: 400 });
          }
        },
        shared = {
          ...deps,
          readJson,
          sendJson,
          discordFetch,
          imageUrl: (id) => {
            assert.equal(deps.sqlite.inTransaction, false, '签名在授权事务之外');
            signed++;
            return signer.imageUrl(id);
          },
        },
        control = createRelationalRequestHttp(shared),
        native = createRelationalNativeHttp({ ...deps, discordFetch }),
        direct = createRelationalDirectHttp({
          ...shared,
          readNormalizationDefaults: () => ({
            defaultModel: 'nai-diffusion-4-5-full',
            defaults: {},
          }),
          validImageSignature: signer.validSignature,
          isAdminRead: () => false,
        });
      return async (req, res, url) =>
        (await control(req, res, url)) ||
        (await native(req, res, url)) ||
        (await direct(req, res, url));
    },
  });
  const notifier = relationalStateNotificationsFor(f.db, f.events),
    members = new RelationalMemberStore(f.db, currentEnv, notifier),
    admit = createRelationalNativeAdmission({
      sqlite: f.db,
      members,
      inputs: f.inputs,
      generations: f.generations,
      replicas: f.replicas,
      payloadBudget: f.budget,
      env: currentEnv,
      now: () => f.clock,
    });
  if (currentEnv.DISCORD_GUILD_ID) {
    t.mock.method(Date, 'now', () => f.clock);
    for (const [id, discordId] of [
      ['m1', '222222222222222222'],
      ['m2', '333333333333333333'],
    ]) {
      await members.discordAuthenticationTransaction((tx) => {
        const user = tx.readMember(id);
        user.discord = {
          id: discordId,
          guildId: currentEnv.DISCORD_GUILD_ID,
          accessToken: `synthetic-request-access-${id}`,
          refreshToken: `synthetic-request-refresh-${id}`,
          expiresAt: f.clock + 86400000,
          checkedAt: id === 'm1' ? 0 : f.clock,
          member: true,
        };
        tx.saveAuthentication(user, 'observation');
      });
    }
  }
  const accept = async (body = input(), member = 'm1') =>
    await admit({
      token: keys[member],
      operation: 'generate',
      body,
      key: null,
      diagnosticId: 'synthetic-request-control',
      admissionVersion: members.readMember(member).admissionEpoch,
    });
  const call = async (
    id = 'existing',
    { method = 'GET', body, token = keys.m1, headers = {}, ...rest } = {},
  ) => {
    const response = await fetch(
      f.origin + `/api/requests/${encodeURIComponent(id)}${method === 'POST' ? '/cancel' : ''}`,
      {
        method,
        headers: { ...(token ? { authorization: `Bearer ${token}` } : {}), ...headers },
        ...(body === undefined
          ? {}
          : { body: typeof body === 'string' ? body : JSON.stringify(body) }),
        ...rest,
      },
    );
    return { status: response.status, body: await response.json() };
  };
  const slow = async (id, change) => {
    let release, entered;
    const gate = new Promise((resolve) => {
        release = resolve;
      }),
      ready = new Promise((resolve) => {
        entered = resolve;
      });
    hooks.body = async () => {
      entered();
      await gate;
    };
    const response = new Promise((resolve, reject) => {
      const req = request(
        f.origin + `/api/requests/${encodeURIComponent(id)}/cancel`,
        {
          method: 'POST',
          headers: { authorization: `Bearer ${keys.m1}`, 'content-type': 'application/json' },
        },
        (res) => {
          const chunks = [];
          res.on('data', (chunk) => chunks.push(chunk));
          res.on('end', () =>
            resolve({ status: res.statusCode, body: JSON.parse(Buffer.concat(chunks)) }),
          );
        },
      );
      req.on('error', reject);
      req.write('{');
      ready.then(async () => {
        try {
          await change();
          hooks.body = null;
          req.end('}');
        } catch (error) {
          reject(error);
          req.destroy();
        } finally {
          release();
        }
      });
    });
    return response;
  };
  return {
    ...f,
    env: currentEnv,
    members,
    notifier,
    accept,
    limits,
    hooks,
    read: call,
    slow,
    get clock() {
      return f.clock;
    },
    get signed() {
      return signed;
    },
  };
}

function assertReplay(t, f) {
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replayer = new RelationalCommitReplayer(target),
    source = new RelationalInputStore(f.db);
  t.mock.method(globalThis, 'fetch', () => assert.fail('public complete replay cannot fetch'));
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), source))();
  }
  for (const table of [
    'generation_requests',
    'generation_jobs',
    'generation_attempts',
    'charges',
    'wallets',
    'account_quota_state',
    'idempotency_keys',
    'artifacts',
    'artifact_replicas',
    'commits',
    'domain_events',
  ]) {
    assert.deepEqual(
      target.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
      f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
      table,
    );
  }
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
  assert.ok(
    target
      .prepare('SELECT auth_epoch FROM members')
      .all()
      .every((row) => row.auth_epoch === null),
  );
}

test('Bearer请求DTO沿原owner，合法installer+真实签名领取原图；query/body/Cookie/admin/PST都不成为Key', async (t) => {
  const building = new Database(':memory:');
  initializeRelationalStorage(building, 'building-request-http');
  assert.throws(() => createRelationalRequestHttp({ sqlite: building }), /incomplete/);
  building.close();
  const f = await fixture(t),
    response = await f.read();
  assert.equal(response.status, 200);
  assert.equal(response.body.id, 'existing');
  assert.deepEqual(response.body.counts, {
    total: 1,
    queued: 0,
    pending: 0,
    running: 0,
    done: 1,
    failed: 0,
  });
  const before = databaseState(f.db),
    signed = f.signed;
  const photo = await fetch(f.origin + response.body.jobs[0].imageUrl);
  assert.equal(photo.status, 200);
  assert.deepEqual(Buffer.from(await photo.arrayBuffer()), image);
  const badUrl = new URL(response.body.jobs[0].imageUrl, f.origin);
  badUrl.searchParams.set('signature', '0'.repeat(64));
  assert.equal((await fetch(badUrl)).status, 401);
  const cookie = await createMemberSession(
    f.members,
    await authenticateMemberApiKey(f.members, keys.m1),
    f.env,
    f.clock,
  );
  const guarded = databaseState(f.db);
  for (const headers of [
    { cookie: `nai_member_session=${cookie.token}` },
    { 'x-user-token': keys.m1 },
    { 'x-admin-token': keys.m1 },
    { authorization: 'Bearer synthetic-upstream-pst' },
  ]) {
    assert.equal((await f.read('existing', { token: '', headers })).status, 401);
  }
  assert.equal((await fetch(f.origin + `/api/requests/existing?token=${keys.m1}`)).status, 401);
  assert.equal(
    (await f.read('existing', { method: 'POST', token: '', body: { token: keys.m1 } })).status,
    401,
  );
  assert.equal((await f.read('existing', { token: keys.m2 })).status, 404);
  assert.equal((await f.read('missing')).status, 404);
  assert.equal(databaseState(f.db), guarded);
  assert.equal(f.signed, signed);
  assert.notEqual(guarded, before, '只有显式签发Cookie是私有写入');
  assert.equal(
    (await f.read('existing', { token: '', headers: { authorization: `bEaReR  ${keys.m1} ` } }))
      .status,
    200,
  );
  const captured = [],
    prepare = f.db.prepare,
    pure = databaseState(f.db);
  f.db.prepare = function (sql) {
    captured.push(sql);
    return prepare.call(this, sql);
  };
  try {
    assert.equal((await f.read()).status, 200);
  } finally {
    f.db.prepare = prepare;
  }
  assert.equal(databaseState(f.db), pure);
  assert.equal(
    captured.some((sql) => /generation_inputs|input_assets|\bbody\b|\bbytes\b/.test(sql)),
    false,
  );
  for (const path of [
    '/api/requests/existing/cancel',
    '/api/users',
    '/api/requests/existing/input',
  ]) {
    assert.equal(
      (await fetch(f.origin + path, { headers: { authorization: `Bearer ${keys.m1}` } })).status,
      404,
    );
  }
  f.notifier.commit(() =>
    f.db.transaction(() =>
      f.members.replaceMemberApiKey('m1', 'explicit-authorized-key-without-prefix'),
    )(),
  );
  assert.equal(
    (await f.read('existing', { token: 'explicit-authorized-key-without-prefix' })).status,
    200,
    '原请求路由不另猜Key前缀',
  );
});

test('取消正文32768字节边界/空正文/错误JSON保持原语义，客户端metadata不成为命令', async (t) => {
  const f = await fixture(t);
  const before = databaseState(f.db);
  assert.equal((await f.read('existing', { method: 'POST', body: '' })).body.cancelledCount, 0);
  assert.equal(
    (await f.read('existing', { method: 'POST', body: '"' + 'x'.repeat(32766) + '"' })).status,
    200,
  );
  assert.equal(
    (await f.read('existing', { method: 'POST', body: '"' + 'x'.repeat(32767) + '"' })).status,
    413,
  );
  assert.equal(
    (await f.read('existing', { method: 'POST', body: JSON.stringify('😀'.repeat(8192)) })).status,
    413,
  );
  assert.equal((await f.read('existing', { method: 'POST', body: '{' })).status, 400);
  const result = await f.read('existing', {
    method: 'POST',
    body: { commandId: 'not-a-command', actorId: 'm2', requestId: 'foreign' },
    headers: { 'idempotency-key': 'unused-client-key' },
  });
  assert.equal(result.body.cancelledCount, 0);
  assert.equal(databaseState(f.db), before);
  assert.ok(f.limits.every((limit) => limit === 32768));
  assert.equal(
    f.db.prepare("SELECT 1 FROM commits WHERE command_id='not-a-command'").get(),
    undefined,
  );
});

test('慢正文后复核原稳定成员与原authEpoch/当前Key/ready，拒绝转归和实际代次变化且无取消副作用', async (t) => {
  for (const mode of ['revoke', 'transfer', 'auth-epoch', 'ready']) {
    await t.test(mode, async (t) => {
      const f = await fixture(t);
      // 授权拒绝的全表对照隔离真实后台派发；运行中取消另用保持开启的 coordinator 验证。
      await f.runtime.stop();
      const accepted = await f.accept(input(), mode === 'transfer' ? 'm2' : 'm1'),
        prepared =
          mode === 'auth-epoch'
            ? await f.members.prepareMemberCredentials('m1', {
                username: 'new.current.username',
                password: 'synthetic-request-new-password',
              })
            : null;
      let before;
      const result = await f.slow(accepted.requestId, () => {
        assert.equal(f.db.inTransaction, false);
        if (mode === 'ready') {
          f.db.prepare("UPDATE projection_state SET status='building'").run();
        } else {
          f.notifier.commit(() =>
            f.db.transaction(() => {
              if (mode === 'auth-epoch') {
                f.members.setMemberCredentials(prepared);
              } else {
                f.members.replaceMemberApiKey('m1', 'synthetic-current-replacement');
                if (mode === 'transfer') {
                  f.members.replaceMemberApiKey('m2', keys.m1);
                }
              }
            })(),
          );
        }
        before = databaseState(f.db);
      });
      assert.equal(result.status, mode === 'ready' ? 500 : 401);
      assert.equal(databaseState(f.db), before);
      assert.equal(
        f.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get(accepted.jobIds[0])
          .status,
        'queued',
      );
    });
  }
});

test('Discord实际IO后GET重新核原Key/身份/ready，临时错误不泄露原请求', async (t) => {
  for (const mode of ['transfer', 'ready', 'temporary', 'qualification']) {
    await t.test(mode, async (t) => {
      let f,
        calls = 0;
      f = await fixture(t, {
        env: {
          DISCORD_CLIENT_ID: '123456789012345678',
          DISCORD_CLIENT_SECRET: 'synthetic-request-discord-secret',
          DISCORD_GUILD_ID: '1134557553011998840',
          PUBLIC_BASE_URL: 'https://request.example',
        },
        discordFetch: async () => {
          calls++;
          assert.equal(f.db.inTransaction, false);
          if (mode === 'transfer') {
            f.notifier.commit(() =>
              f.db.transaction(() => {
                f.members.replaceMemberApiKey('m1', 'new-request-current-key');
                f.members.replaceMemberApiKey('m2', keys.m1);
              })(),
            );
          } else if (mode === 'ready') {
            f.db.prepare("UPDATE projection_state SET status='building'").run();
          } else if (mode === 'qualification') {
            return new Response('membership denied', { status: 403 });
          } else {
            return new Response('temporary', { status: 503 });
          }
          return Response.json({ user: { id: '222222222222222222' } });
        },
      });
      const protectedTables = [
          'generation_requests',
          'generation_jobs',
          'generation_attempts',
          'charges',
          'wallets',
          'account_quota_state',
        ],
        before = protectedTables.map((table) =>
          f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
        );
      const result = await f.read();
      let expected = 503;
      if (mode === 'transfer') {
        expected = 401;
      } else if (mode === 'ready') {
        expected = 500;
      } else if (mode === 'qualification') {
        expected = 403;
      }
      assert.equal(result.status, expected);
      assert.equal(result.body.id, undefined);
      assert.equal(f.signed, 0);
      assert.equal(calls, 1);
      assert.deepEqual(
        protectedTables.map((table) =>
          f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
        ),
        before,
      );
    });
  }
});

test('真实Native两child取消只停waiting，不退running预扣或取消物理请求；完成与完整回放不复活余图', async (t) => {
  const f = await fixture(t);
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  f.upstream.release = release;
  f.upstream.handler = async (_req, res) => {
    await gate;
    res.writeHead(200, { 'content-type': 'image/webp' });
    res.end(image);
  };
  const pending = f.call(input({ n_samples: 2, seed: 7 }));
  await waitFor(() => f.upstream.calls.length === 1 && f.controls.size === 1);
  const first = f.db
      .prepare("SELECT id,request_id FROM generation_jobs WHERE status='running'")
      .get(),
    requestId = first.request_id,
    charge = f.generations.readCharge(first.id),
    wallet = f.generations.readWallet('a', 'm1');
  const result = await f.read(requestId, {
    method: 'POST',
    body: {
      commandId: 'client-must-not-own-cancel',
      actorId: 'm2',
      requestId: 'foreign',
      recordedAt: 0,
    },
  });
  assert.equal(result.status, 200);
  assert.equal(result.body.id, requestId);
  assert.equal(result.body.cancelledCount, 1);
  assert.equal(result.body.status, 'running');
  assert.deepEqual(
    result.body.jobs.map((job) => job.status),
    ['running', 'failed'],
  );
  assert.equal(result.body.jobs[1].cancelled, true);
  assert.equal(result.body.jobs[1].completedAt, new Date(f.clock).toISOString());
  assert.equal(
    f.db.prepare("SELECT 1 FROM commits WHERE command_id='client-must-not-own-cancel'").get(),
    undefined,
  );
  assert.equal(f.generations.readCharge(result.body.jobs[1].id), null, 'waiting没有预扣，不造退款');
  assert.deepEqual(f.generations.readCharge(first.id), charge);
  assert.deepEqual(f.generations.readWallet('a', 'm1'), wallet);
  assert.equal(f.controls.size, 1);
  const before = databaseState(f.db);
  assert.equal((await f.read(requestId, { method: 'POST', body: {} })).body.cancelledCount, 0);
  assert.equal(databaseState(f.db), before);
  release();
  const completed = await pending;
  assert.equal(completed.status, 200);
  assert.equal(completed.headers.get('x-nai-request-id'), requestId);
  const final = await f.read(requestId);
  assert.equal(final.body.status, 'partial');
  assert.deepEqual(
    final.body.jobs.map((job) => job.status),
    ['done', 'failed'],
  );
  assert.equal(f.upstream.calls.length, 1);
  assert.equal(f.generations.readCharge(first.id).status, 'settled');
  assertReplay(t, f);
});

test('取消末端SQL故障外catch全表回滚且无幽灵；同literal重授同成员同epoch沿原语义，重复0无新receipt', async (t) => {
  const f = await fixture(t);
  await f.runtime.stop();
  const accepted = await f.accept(),
    hub = stateEventsFor(f.notifier),
    version = hub.current().version,
    before = databaseState(f.db);
  f.db.exec(
    "CREATE TEMP TRIGGER cancel_fault AFTER UPDATE ON generation_jobs WHEN NEW.error_code='request_cancelled' BEGIN SELECT RAISE(ABORT,'cancel tail fault'); END",
  );
  assert.equal((await f.read(accepted.requestId, { method: 'POST', body: {} })).status, 500);
  assert.equal(databaseState(f.db), before);
  await new Promise((resolve) => setTimeout(resolve, 250));
  assert.equal(hub.current().version, version);
  f.db.exec('DROP TRIGGER cancel_fault');
  const originalEpoch = (await authenticateMemberApiKey(f.members, keys.m1)).memberAuth.version;
  const succeeded = await f.slow(accepted.requestId, () => {
    f.notifier.commit(() =>
      f.db.transaction(() => {
        f.members.replaceMemberApiKey('m1', 'synthetic-temporary-key');
        f.members.replaceMemberApiKey('m1', keys.m1);
      })(),
    );
  });
  assert.equal(
    (await authenticateMemberApiKey(f.members, keys.m1)).memberAuth.version,
    originalEpoch,
  );
  assert.equal(succeeded.status, 200);
  assert.equal(succeeded.body.cancelledCount, 1);
  await new Promise((resolve) => setTimeout(resolve, 250));
  const after = databaseState(f.db),
    at = hub.current().version;
  assert.equal(
    (await f.read(accepted.requestId, { method: 'POST', body: {} })).body.cancelledCount,
    0,
  );
  await new Promise((resolve) => setTimeout(resolve, 250));
  assert.equal(databaseState(f.db), after);
  assert.equal(hub.current().version, at);
  assertReplay(t, f);
});

test('实际strict活动alias沿原requestId；失败退款后的真实迟到原图仍可领，退休后请求不可取消', async (t) => {
  const f = await fixture(t);
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  f.upstream.release = release;
  f.upstream.handler = async (_req, res) => {
    await gate;
    res.writeHead(200, { 'content-type': 'image/webp' });
    res.end(image);
  };
  const body = {
      tag: 'actual cached request',
      model: 'nai-diffusion-4-5-full',
      width: 128,
      height: 128,
      steps: 23,
      seed: 7,
      nocache: 0,
    },
    original = await f.call(body, { endpoint: '/api/jobs', key: 'source-key' }),
    source = JSON.parse(original.buffer);
  assert.equal(original.status, 202, original.buffer.toString());
  await waitFor(() => f.upstream.calls.length === 1);
  const alias = await f.call(body, { endpoint: '/api/jobs', key: 'alias-key' });
  assert.equal(alias.status, 202, alias.buffer.toString());
  assert.equal(JSON.parse(alias.buffer).requestId, source.requestId);
  assert.equal((await f.read(source.requestId)).body.id, source.requestId);
  assert.equal(
    (await f.read(source.requestId, { method: 'POST', body: {} })).body.cancelledCount,
    0,
  );
  f.notifier.commit(() =>
    f.fail(
      source.id,
      { errorCode: 'upstream_unavailable', errorStatus: 502, aborted: false },
      f.clock,
    ),
  );
  const refunded = f.generations.readCharge(source.id),
    wallet = f.generations.readWallet('a', 'm1');
  release();
  await waitFor(
    () =>
      f.controls.size === 0 &&
      Boolean(
        f.db.prepare('SELECT artifact_id FROM generation_jobs WHERE id=?').get(source.id)
          .artifact_id,
      ),
  );
  const late = await f.read(source.requestId);
  assert.equal(late.body.status, 'failed');
  assert.equal(late.body.jobs[0].lateResultAvailable, true);
  assert.equal((await fetch(f.origin + late.body.jobs[0].imageUrl)).status, 200);
  assert.deepEqual(f.generations.readCharge(source.id), refunded);
  assert.deepEqual(f.generations.readWallet('a', 'm1'), wallet);
  assert.equal((await f.read(source.requestId, { token: keys.m2 })).status, 404);
  f.notifier.commit(() =>
    f.db.transaction(() =>
      f.generations.retireJobHistory(
        { ...meta('retire-request-history', f.clock + 300000), actorKind: 'admin' },
        [source.id],
        null,
        new Map(),
      ),
    )(),
  );
  const retired = databaseState(f.db);
  assert.equal((await f.read(source.requestId)).status, 404);
  assert.equal((await f.read(source.requestId, { method: 'POST', body: {} })).status, 404);
  assert.equal(databaseState(f.db), retired);
  assert.equal(f.upstream.calls.length, 1);
  assertReplay(t, f);
});
