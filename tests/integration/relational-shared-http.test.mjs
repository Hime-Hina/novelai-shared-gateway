import test from 'node:test';
import assert from 'node:assert/strict';
import { get } from 'node:http';
import { StringDecoder } from 'node:string_decoder';
import path from 'node:path';
import Database from 'better-sqlite3';
import { setup, keys, input, waitFor } from '../helpers/relational-api-http.mjs';
import { meta, databaseState, image } from '../helpers/relational-generation.mjs';
import { createRelationalMemberReadHttp } from '../../server/members/relational-read-http.ts';
import { createRelationalNativeHttp } from '../../server/generation/relational-native-http.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalRequestStore } from '../../server/storage/relational-request-store.ts';
import { relationalStateNotificationsFor } from '../../server/events/relational-state-notifications.ts';
import { stateEventsFor } from '../../server/events/state-notifications.ts';
import { accountQuotaResult } from '../../server/quota/account-refresh.ts';
import { parseAmount, formatAmount } from '../../server/quota/resource-amounts.ts';
import { publicErrorMessage } from '../../server/http/public-errors.ts';
import {
  authenticateMemberApiKey,
  authenticateMemberSession,
  createMemberSession,
  logoutMemberSession,
} from '../../server/members/authentication.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { relationalSnapshotSha256 } from '../../server/storage/relational-private-restore.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';

const discordEnv = {
  DISCORD_CLIENT_ID: '123456789012345678',
  DISCORD_CLIENT_SECRET: 'synthetic-shared-discord-secret',
  DISCORD_GUILD_ID: '1134557553011998840',
  PUBLIC_BASE_URL: 'https://shared.example',
};

async function fixture(t, { env = {}, discordFetch } = {}) {
  const cleanup = [],
    streams = [],
    sessions = {};
  let f, currentEnv;
  t.after(async () => {
    for (const stream of streams) {
      stream.req.destroy();
    }
    f?.events.emit('close');
    for (const run of cleanup.reverse()) {
      await run();
    }
  });
  f = await setup(
    { after: (run) => cleanup.push(run) },
    {
      env,
      createHttp: (deps) => {
        currentEnv = deps.env;
        const read = createRelationalMemberReadHttp({
          ...deps,
          discordFetch,
          publicBaseUrl: 'https://shared.example',
          publicImageUrl: () => assert.fail('shared reads cannot sign images'),
        });
        const native = createRelationalNativeHttp(deps);
        return async (req, res, url) =>
          (await read(req, res, url)) || (await native(req, res, url));
      },
      renderError: (error) => ({
        statusCode: error.statusCode || 500,
        body: { error: publicErrorMessage(error.message) },
      }),
    },
  );
  t.mock.method(Date, 'now', () => f.clock);
  const notifier = relationalStateNotificationsFor(f.db, f.events),
    members = new RelationalMemberStore(f.db, currentEnv, notifier),
    quotas = new RelationalQuotaStore(f.db, currentEnv);
  if (currentEnv.DISCORD_GUILD_ID) {
    for (const [id, snowflake] of [
      ['m1', '222222222222222222'],
      ['m2', '333333333333333333'],
    ]) {
      await members.discordAuthenticationTransaction((tx) => {
        const member = tx.readMember(id);
        member.discord = {
          id: snowflake,
          guildId: currentEnv.DISCORD_GUILD_ID,
          accessToken: `synthetic-access-${id}`,
          refreshToken: `synthetic-refresh-${id}`,
          expiresAt: f.clock + 86400000,
          checkedAt: f.clock,
          member: true,
        };
        tx.saveAuthentication(member, 'login');
      });
    }
  }
  for (const id of ['m1', 'm2']) {
    sessions[id] = await createMemberSession(
      members,
      await authenticateMemberApiKey(members, keys[id]),
      currentEnv,
      f.clock,
    );
  }
  const headers = (id) => ({
    cookie: `nai_member_session=${sessions[id].token}`,
    origin: f.origin,
  });
  const read = async (id = 'm1', extra = {}) => {
    const response = await fetch(f.origin + '/api/member/shared-status', {
      headers: { ...headers(id), ...extra },
    });
    return { status: response.status, headers: response.headers, body: await response.json() };
  };
  const stream = async (id = 'm1', extra = {}) => {
    const value = { frames: [], raw: '', status: 0, ended: false, headers: {} };
    const ready = new Promise((resolve, reject) => {
      const req = get(
        f.origin + '/api/member/shared-events',
        { headers: { ...headers(id), ...extra } },
        (res) => {
          value.status = res.statusCode;
          value.headers = res.headers;
          const decoder = new StringDecoder('utf8');
          let buffer = '';
          res.on('data', (bytes) => {
            const text = decoder.write(bytes);
            value.raw += text;
            buffer += text;
            let end;
            while ((end = buffer.indexOf('\n\n')) >= 0) {
              const frame = buffer.slice(0, end),
                event = frame.match(/^event: ([^\n]+)/)?.[1],
                data = frame.match(/\ndata: ([^\n]+)/)?.[1];
              if (event && data) {
                value.frames.push({ event, data: JSON.parse(data) });
              }
              buffer = buffer.slice(end + 2);
            }
          });
          res.on('end', () => {
            value.ended = true;
          });
          res.on('error', () => {
            value.ended = true;
          });
          resolve(value);
        },
      );
      value.req = req;
      req.on('error', (error) => {
        value.ended = true;
        reject(error);
      });
    });
    streams.push(value);
    return ready;
  };
  let observed = 0;
  const observe = (v5UsagePercent, v5UsageIsNegative = false) => {
    const q = quotas.readAccountQuota('a'),
      ticket = quotas.beginQuotaObservation('a', f.clock),
      result = accountQuotaResult(
        'a',
        {
          tier: 3,
          subscriptionActive: true,
          subscriptionExpiresAt: null,
          fixed: formatAmount(parseAmount(q.observedAnlasFixed) - parseAmount(q.pendingFixedDebit)),
          purchased: formatAmount(
            parseAmount(q.observedAnlasPurchased) - parseAmount(q.pendingPurchasedDebit),
          ),
          v5UsagePercent,
          v5UsageIsNegative,
          v5UsageTimeUntilNextPercent: 0,
        },
        new Date(f.clock).toISOString(),
      );
    return notifier.commit(() =>
      f.db.transaction(() =>
        quotas.recordQuotaObservation(
          meta(`shared-observe-${++observed}`, f.clock),
          ticket,
          result,
        ),
      )(),
    );
  };
  return { ...f, members, sessions, notifier, quotas, read, stream, observe, env: currentEnv };
}

test('真实shared GET复用原官方标量/纯owner，读取0writes且活动头不续期，缺PST不能借ready报可用', async (t) => {
  const f = await fixture(t);
  assert.equal((await f.read()).body.v5Percent, null, 'Opening缺原展示来源保持未知');
  f.observe(0, false);
  assert.equal((await f.read()).body.v5Percent, 0);
  assert.equal((await f.read()).body.v5IsNegative, false);
  f.observe(80, true);
  assert.equal(f.quotas.readAccountQuota('a').observedV5, '0');
  f.advance(1000);
  f.db.pragma('query_only=ON');
  try {
    assert.equal(f.quotas.readSharedStatusSource().accounts[0].v5UsagePercent, 80);
    assert.equal(
      new RelationalRequestStore(f.db).readSharedJobCounts('m1', f.clock).recentFailures,
      0,
    );
  } finally {
    f.db.pragma('query_only=OFF');
  }
  const before = databaseState(f.db),
    statements = [],
    prepare = f.db.prepare;
  f.db.prepare = function (sql) {
    statements.push(sql);
    return prepare.call(this, sql);
  };
  let result;
  try {
    result = await f.read('m1', { 'x-member-activity': '1' });
  } finally {
    f.db.prepare = prepare;
  }
  assert.equal(result.status, 200, JSON.stringify(result.body));
  assert.equal(result.body.v5Percent, 80);
  assert.equal(result.body.v5IsNegative, true);
  assert.equal(result.body.status, 'active');
  assert.equal(result.headers.has('set-cookie'), false);
  assert.equal(result.headers.get('cache-control'), 'no-store');
  assert.equal(databaseState(f.db), before);
  assert.equal(
    statements.some((sql) => /^\s*(?:UPDATE|INSERT|DELETE)\b/i.test(sql)),
    false,
  );
  assert.equal(
    statements.some((sql) => /generation_inputs|input_assets|\bbody\b|\bbytes\b/i.test(sql)),
    false,
  );
  assert.equal(
    statements.some((sql) => /SELECT\s+(?:a\.id,.*?)?c\.pst(?:\s|,)/i.test(sql)),
    false,
  );
  const text = JSON.stringify(result.body);
  for (const secret of [
    'STA1N-',
    'pst-',
    'memberIds',
    'configured',
    'credentialEpoch',
    'originalId',
  ]) {
    assert.equal(text.includes(secret), false);
  }
  f.db.prepare('DELETE FROM upstream_credentials').run();
  const sourcePath = path.join(f.dir, 'selected-without-pst.sqlite');
  await f.db.backup(sourcePath);
  const selected = new Database(sourcePath);
  selected.pragma('journal_mode=DELETE');
  selected.close();
  const rebuilt = new Database(':memory:');
  t.after(() => rebuilt.close());
  initializeRelationalStorage(rebuilt, 'generation-history');
  const replayer = new RelationalCommitReplayer(rebuilt),
    source = new RelationalInputStore(f.db);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    rebuilt.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), source))();
  }
  await verifyAndInstallRelationalStorage(rebuilt, {
    sourcePath,
    sourceSha256: relationalSnapshotSha256(sourcePath),
    historyId: 'generation-history',
    finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get()
      .seq,
    replicas: f.replicas,
  });
  assert.equal(
    new RelationalQuotaStore(rebuilt).readSharedStatusSource().accounts[0].quotaError,
    'credential_unavailable',
  );
  const absent = databaseState(f.db),
    unavailable = await f.read();
  assert.equal(unavailable.body.status, 'quota_sync_error');
  assert.equal(unavailable.body.quotaSyncError, true);
  assert.equal(unavailable.body.v5Percent, 80);
  assert.equal(databaseState(f.db), absent);
});

test('真实Native调度与原SSE hub共享提交，跨成员日期过滤、断连和重连resync不取消任务', async (t) => {
  const f = await fixture(t);
  f.observe(80, false);
  const own = await f.stream('m1', { 'x-member-activity': '1' }),
    other = await f.stream('m2');
  await waitFor(
    () =>
      own.frames.some((x) => x.event === 'state-invalidated') &&
      other.frames.some((x) => x.event === 'state-invalidated'),
  );
  assert.equal(own.headers['set-cookie'], undefined);
  const first = own.frames.find((x) => x.event === 'state-invalidated').data;
  assert.equal(first.resync, true);
  let release;
  f.upstream.handler = async (_req, res) => {
    await new Promise((resolve) => {
      release = resolve;
    });
    res.writeHead(200, { 'content-type': 'image/webp' });
    res.end(image);
  };
  const pending = f.call(input(), { member: 'm2', key: 'shared-running' });
  await waitFor(() => f.upstream.calls.length === 1 && release);
  assert.equal(f.quotas.readSharedStatusSource().baseline.held, true);
  await waitFor(() => own.frames.some((x) => x.event === 'shared-status' && x.data.running === 1));
  await waitFor(() => other.frames.filter((x) => x.event === 'state-invalidated').length > 1);
  const foreign = own.frames.filter((x) => x.event === 'state-invalidated').at(-1).data,
    mine = other.frames.filter((x) => x.event === 'state-invalidated').at(-1).data;
  assert.deepEqual(foreign.scopes, ['shared']);
  assert.equal(foreign.activityDates, undefined);
  assert.deepEqual(mine.scopes, ['shared', 'member']);
  assert.ok(mine.activityDates.length > 0);
  own.req.destroy();
  assert.equal(f.controls.size, 1);
  release();
  assert.equal((await pending).status, 200);
  assert.equal(f.quotas.readSharedStatusSource().baseline.held, false);
  assert.equal(typeof f.quotas.readSharedStatusSource().baseline.lastSettlementAt, 'number');
  const again = await f.stream('m1', { 'last-event-id': 'stale' });
  await waitFor(() => again.frames.some((x) => x.event === 'state-invalidated'));
  const resync = again.frames.find((x) => x.event === 'state-invalidated').data;
  assert.equal(resync.bootId, first.bootId);
  assert.equal(resync.resync, true);
  assert.ok(resync.version > first.version);
  assert.equal(
    JSON.stringify([...own.frames, ...other.frames, ...again.frames]).includes('memberIds'),
    false,
  );
});

test('真实受理末尾SQL回滚与原receipt均无幽灵通知，私有logout只唤醒失效而不广播业务', async (t) => {
  const f = await fixture(t);
  f.observe(80, false);
  const watching = await f.stream(),
    other = await f.stream('m2');
  await waitFor(
    () =>
      watching.frames.some((x) => x.event === 'state-invalidated') &&
      other.frames.some((x) => x.event === 'state-invalidated'),
  );
  const hub = stateEventsFor(f.notifier),
    before = databaseState(f.db),
    version = hub.current().version,
    frames = watching.frames.length;
  f.db.exec(
    "CREATE TEMP TRIGGER shared_ghost BEFORE INSERT ON idempotency_keys BEGIN SELECT RAISE(ABORT,'shared receipt rollback'); END",
  );
  assert.equal((await f.call(input(), { key: 'shared-ghost' })).status, 500);
  assert.equal(databaseState(f.db), before);
  await new Promise((resolve) => setTimeout(resolve, 300));
  assert.equal(hub.current().version, version);
  assert.equal(watching.frames.length, frames);
  f.db.exec('DROP TRIGGER shared_ghost');
  assert.equal((await f.call(input(), { key: 'shared-ghost' })).status, 200);
  await waitFor(() => watching.frames.filter((x) => x.event === 'state-invalidated').length > 1);
  const completed = databaseState(f.db),
    at = hub.current().version,
    count = watching.frames.length;
  assert.equal((await f.call(input(), { key: 'shared-ghost' })).status, 200);
  await new Promise((resolve) => setTimeout(resolve, 300));
  assert.equal(databaseState(f.db), completed);
  assert.equal(hub.current().version, at);
  assert.equal(watching.frames.length, count);
  const mine = watching.frames.filter((x) => x.event === 'state-invalidated').length,
    foreign = other.frames.length;
  await logoutMemberSession(f.members, f.sessions.m1.token);
  await waitFor(() => watching.ended);
  assert.ok(watching.frames.some((x) => x.event === 'auth-expired'));
  assert.equal(watching.frames.filter((x) => x.event === 'state-invalidated').length, mine);
  await new Promise((resolve) => setTimeout(resolve, 300));
  assert.equal(other.frames.length, foreign);
  assert.equal(other.ended, false);
});

test('真实共享流每成员三连接，关闭后释放配额且不占他人成员连接', async (t) => {
  const f = await fixture(t),
    streams = await Promise.all([f.stream(), f.stream(), f.stream()]);
  for (const stream of streams) {
    assert.equal(stream.status, 200);
  }
  const rejected = await f.stream();
  assert.equal(rejected.status, 429);
  assert.ok(rejected.raw.includes('状态连接过多'));
  assert.equal((await f.stream('m2')).status, 200);
  streams[0].req.destroy();
  await waitFor(() => streams[0].ended);
  // 客户端 close 与服务端退订各有一个 IO 回合；等待真实 socket 关闭后再重连。
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal((await f.stream()).status, 200);
});

test('同一原transport全局一百连接拒绝第101条，关闭后跨成员释放', async (t) => {
  const f = await fixture(t),
    ids = ['m1', 'm2'];
  for (let n = 0; n < 32; n++) {
    const id = `global-${n}`,
      key = `STA1N-shared-global-${n}`;
    f.notifier.commit(() =>
      f.db.transaction(() =>
        f.members.registerMember(meta(`register-${id}`, f.clock), {
          memberId: id,
          displayName: id,
          apiKey: key,
        }),
      )(),
    );
    f.sessions[id] = await createMemberSession(
      f.members,
      await authenticateMemberApiKey(f.members, key),
      f.env,
      f.clock,
    );
    ids.push(id);
  }
  const streams = [];
  for (const id of ids.slice(0, 33)) {
    for (let n = 0; n < 3; n++) {
      const stream = await f.stream(id);
      assert.equal(stream.status, 200);
      streams.push(stream);
    }
  }
  assert.equal((await f.stream(ids[33])).status, 200);
  assert.equal((await f.stream(ids[33])).status, 429);
  streams[0].req.destroy();
  await waitFor(() => streams[0].ended);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal((await f.stream(ids[33])).status, 200);
});

test('共享GET在Discord IO后复核原Cookie和ready，不能返回首次授权的旧快照', async (t) => {
  for (const boundary of ['logout', 'ready']) {
    await t.test(boundary, async (t) => {
      let f,
        calls = 0;
      f = await fixture(t, {
        env: discordEnv,
        discordFetch: async () => {
          calls++;
          assert.equal(f.db.inTransaction, false);
          if (boundary === 'logout') {
            await logoutMemberSession(f.members, f.sessions.m1.token);
          } else {
            f.db.prepare("UPDATE projection_state SET status='building' WHERE id=1").run();
          }
          return Response.json({ user: { id: '222222222222222222' } });
        },
      });
      f.db.prepare("UPDATE discord_bindings SET checked_at=0 WHERE member_id='m1'").run();
      const result = await f.read('m1', { 'x-member-activity': '1' });
      assert.equal(result.status, boundary === 'logout' ? 401 : 500);
      assert.equal(result.body.status, undefined);
      assert.equal(result.headers.has('set-cookie'), false);
      assert.equal(calls, 1);
    });
  }
});

test('已建共享流在Discord临时503时普通EOF，不伪造auth-expired或撤销原Cookie', async (t) => {
  let calls = 0;
  const f = await fixture(t, {
      env: discordEnv,
      discordFetch: async () => {
        calls++;
        assert.equal(f.db.inTransaction, false);
        return new Response('temporary outage', { status: 503 });
      },
    }),
    stream = await f.stream();
  await waitFor(() => stream.frames.some((x) => x.event === 'state-invalidated'));
  const count = stream.frames.length;
  f.advance(5 * 60000);
  const before = databaseState(f.db);
  await waitFor(() => stream.ended);
  assert.equal(calls, 1);
  assert.equal(
    stream.frames.some((x) => x.event === 'auth-expired'),
    false,
  );
  assert.equal(stream.frames.length, count);
  assert.equal(databaseState(f.db), before);
  assert.ok(await authenticateMemberSession(f.members, f.sessions.m1.token, f.env, f.clock));
});

test('共享流每轮Discord IO后重新授权，logout仅auth-expired而ready撤回普通EOF', async (t) => {
  for (const boundary of ['logout', 'ready']) {
    await t.test(boundary, async (t) => {
      let f,
        calls = 0;
      f = await fixture(t, {
        env: discordEnv,
        discordFetch: async () => {
          calls++;
          assert.equal(f.db.inTransaction, false);
          if (boundary === 'logout') {
            await logoutMemberSession(f.members, f.sessions.m1.token);
          } else {
            f.db.prepare("UPDATE projection_state SET status='building' WHERE id=1").run();
          }
          return Response.json({ user: { id: '222222222222222222' } });
        },
      });
      const stream = await f.stream();
      await waitFor(() => stream.frames.some((x) => x.event === 'state-invalidated'));
      const count = stream.frames.length;
      f.advance(5 * 60000);
      await waitFor(() => stream.ended);
      assert.equal(calls, 1);
      assert.equal(stream.frames.filter((x) => x.event === 'shared-status').length, 1);
      assert.equal(stream.frames.filter((x) => x.event === 'state-invalidated').length, 1);
      assert.equal(
        stream.frames.some((x) => x.event === 'auth-expired'),
        boundary === 'logout',
      );
      assert.equal(stream.frames.length, count + (boundary === 'logout' ? 1 : 0));
    });
  }
});

test('原hub只给当前owner全局日期，owner配置变更撤销旧双方Cookie且不自动升级权限', async (t) => {
  const f = await fixture(t, { env: { ...discordEnv, OWNER_USER_ID: 'm1' } }),
    owner = await f.stream(),
    member = await f.stream('m2');
  await waitFor(() => owner.frames.length >= 2 && member.frames.length >= 2);
  assert.ok(
    owner.frames.find((x) => x.event === 'state-invalidated').data.scopes.includes('owner'),
  );
  assert.equal((await f.call(input(), { member: 'm2', key: 'shared-owner-date' })).status, 200);
  await waitFor(() => owner.frames.filter((x) => x.event === 'state-invalidated').length > 1);
  const foreign = owner.frames.filter((x) => x.event === 'state-invalidated').at(-1).data;
  // 完成事务也同步分配当前组额度；m1 的财务变化仍可带 member，不缩掉原失效范围。
  assert.deepEqual(foreign.scopes, ['shared', 'member', 'owner']);
  assert.ok(foreign.activityDates.length > 0);
  f.env.OWNER_USER_ID = 'm2';
  await waitFor(() => owner.ended && member.ended);
  for (const stream of [owner, member]) {
    assert.ok(stream.frames.some((x) => x.event === 'auth-expired'));
  }
  assert.equal((await f.read()).status, 401);
  assert.equal((await f.read('m2')).status, 401);
  f.sessions.m2 = await createMemberSession(
    f.members,
    await authenticateMemberApiKey(f.members, keys.m2),
    f.env,
    f.clock,
  );
  const current = await f.stream('m2');
  await waitFor(() => current.frames.length >= 2);
  assert.ok(
    current.frames.find((x) => x.event === 'state-invalidated').data.scopes.includes('owner'),
  );
});
