import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import {
  fixture,
  meta,
  NOW,
  databaseState,
  image,
  keys,
  installReady,
} from '../helpers/relational-generation.mjs';
import { createServer as createTlsHttpServer } from 'node:https';
import { createServer as createTcpServer, connect } from 'node:net';
import { once, EventEmitter } from 'node:events';
import tls from 'node:tls';
import { readFile } from 'node:fs/promises';
import fsPromises from 'node:fs/promises';
import path from 'node:path';
import { syncBuiltinESMExports } from 'node:module';
import { createRelationalAccountQuotaRefresh } from '../../server/quota/relational-account-refresh.ts';
import { fetchNovelAiAccountQuota } from '../../server/protocols/novelai-client.ts';
import { createRelationalQueueRuntime } from '../../server/generation/relational-queue-runtime.ts';
import { createRelationalMemberGenerationAdmission } from '../../server/generation/relational-member-admission.ts';
import { createGenerationPayloadBudget } from '../../server/generation/payload-budget.ts';
import { normalizeMemberGeneration } from '../../server/generation/member-contract.ts';
import {
  RelationalAccountStore,
  relationalAccountFacts,
} from '../../server/storage/relational-account-store.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import {
  RelationalCommitReplayer,
  relationalReplayFacts,
} from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { relationalAccountConnectionSource } from '../../server/storage/relational-account-connection.ts';
import {
  relationalPrivateStateEmpty,
  relationalSnapshotSha256,
  restoreRelationalPrivateSnapshot,
} from '../../server/storage/relational-private-restore.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import {
  accountQuotaResult,
  accountQuotaErrorResult,
  createAccountQuotaQuery,
} from '../../server/quota/account-refresh.ts';
import {
  ownerAccountCommand,
  adminAccountBatchCommand,
} from '../../server/members/owner-operations.ts';
import { domainDigest } from '../../server/events/domain-events.ts';

const official = () => ({
  tier: 3,
  subscriptionActive: true,
  subscriptionExpiresAt: null,
  fixed: 8000,
  purchased: 0,
  v5UsagePercent: 80,
  v5UsageIsNegative: false,
  v5UsageTimeUntilNextPercent: 600,
});
function setup(f, controls = new Map(), env = {}) {
  const quotas = new RelationalQuotaStore(f.db, env),
    accounts = new RelationalAccountStore(f.db, quotas, controls, env),
    configure = (id, changes, actor = null) =>
      f.db.transaction(() =>
        accounts.prepareAccountCommand(
          { ...meta(id), actorKind: 'admin', actorId: 'local-admin' },
          { kind: 'configure', accountId: 'a', changes },
          actor,
        ),
      )();
  return { quotas, accounts, configure };
}
function publicPrefix(t, f, until) {
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const owner = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db);
  for (const { seq } of f.db
    .prepare(
      'SELECT CAST(commit_seq AS TEXT) seq FROM commits WHERE commit_seq<? ORDER BY commit_seq',
    )
    .all(BigInt(until))) {
    target.transaction(() => owner.replayCommit(readRelationalCommit(f.db, seq), inputs))();
  }
  return { target, owner, inputs };
}
function startAccountOutsideSql(f, accounts, ...args) {
  const authorized = f.db.transaction(() => accounts.startAccountEnable(...args))();
  return 'start' in authorized ? authorized.start() : authorized;
}
function resequence(stored, target) {
  const versions = new Map();
  stored.command.expectedVersions = {};
  stored.events = stored.events.map((event, eventIndex) => {
    if (!versions.has(event.streamId)) {
      const old =
        target
          .prepare('SELECT CAST(stream_version AS TEXT) v FROM stream_heads WHERE stream_id=?')
          .get(event.streamId)?.v ?? '0';
      versions.set(event.streamId, BigInt(old));
      stored.command.expectedVersions[event.streamId] = old;
    }
    versions.set(event.streamId, versions.get(event.streamId) + 1n);
    return { ...event, eventIndex, streamVersion: String(versions.get(event.streamId)) };
  });
  stored.eventCount = stored.events.length;
  stored.command.events = stored.events.map((event) =>
    Object.fromEntries(
      [
        'eventType',
        'payload',
        'streamId',
        'occurredAt',
        'accountId',
        'memberId',
        'policyVersion',
        'pricingVersion',
      ].map((key) => [key, event[key]]),
    ),
  );
  return stored;
}

test('账户兼容owner保留空名/小数权重、真实held与physical代理修改，PST仍busy，私有地址不入公共事实', async (t) => {
  const f = await fixture(t),
    controls = new Map([['physical', { accountId: 'a' }]]),
    { configure, accounts } = setup(f, controls);
  f.accept('held');
  assert.ok(f.dispatch('held-0').dispatch);
  const enabled = f.db
    .prepare("SELECT enabled,credential_epoch FROM upstream_accounts WHERE id='a'")
    .get();
  configure('rename', { displayName: '', weight: 1.5 });
  assert.equal(relationalAccountConnectionSource(f.db, 'a'), 'opening-account');
  const proxy = 'socks5://synthetic-user:synthetic-secret@127.0.0.1:19001';
  const first = configure('proxy', { proxyUrl: proxy });
  assert.equal(first.result.events[0].eventType, 'AccountConfigurationChanged');
  assert.equal(relationalAccountConnectionSource(f.db, 'a'), first.result.events[0].eventId);
  assert.deepEqual(
    f.db.prepare("SELECT enabled,credential_epoch FROM upstream_accounts WHERE id='a'").get(),
    enabled,
  );
  assert.equal(accounts.readAccountManagementProfile('a').weight, 1.5);
  assert.equal(
    f.db.prepare("SELECT proxy_url FROM upstream_credentials WHERE account_id='a'").get().proxy_url,
    proxy,
  );
  const before = databaseState(f.db);
  assert.throws(() => configure('busy-token', { token: 'pst-new-token' }), { statusCode: 409 });
  assert.equal(databaseState(f.db), before);
  const publicText =
    JSON.stringify(f.db.prepare('SELECT * FROM commits').all()) +
    JSON.stringify(f.db.prepare('SELECT * FROM domain_events').all());
  assert.ok(!publicText.includes(proxy));
  assert.ok(!publicText.includes('synthetic-secret'));
  assert.ok(
    !publicText.includes(
      domainDigest({
        command: meta('proxy').fingerprint,
        input: { kind: 'configure', accountId: 'a', changes: { proxyUrl: proxy } },
      }),
    ),
    'proxy password must not become a public dictionary oracle',
  );
  f.fail('held-0', { aborted: false }, NOW + 1);
  assert.equal(f.generations.readCharge('held-0').status, 'refunded');
});

test('名称权重不作废真实观察，proxy ABA不复活旧查询/401，原派发退款责任照常保存', async (t) => {
  const f = await fixture(t),
    { quotas, configure } = setup(f);
  const source = quotas.beginQuotaObservation('a', NOW);
  configure('display-only', { displayName: 'current', weight: 2.25 });
  const observed = f.db.transaction(() =>
    quotas.recordQuotaObservation(
      meta('observed'),
      source,
      accountQuotaResult('a', official(), new Date(NOW).toISOString()),
    ),
  )();
  assert.ok(!('ignored' in observed));
  f.accept('original');
  assert.ok(f.dispatch('original-0').dispatch);
  const stale = quotas.beginQuotaObservation('a', NOW + 1);
  configure('to-proxy', { proxyUrl: 'socks5://127.0.0.1:19001' });
  configure('to-direct', { proxyUrl: '' });
  const before = databaseState(f.db);
  assert.deepEqual(
    f.db.transaction(() =>
      quotas.recordQuotaObservation(
        meta('stale'),
        stale,
        accountQuotaResult('a', official(), new Date(NOW + 1).toISOString()),
      ),
    )(),
    { ignored: true },
  );
  assert.equal(databaseState(f.db), before);
  f.db.transaction(() =>
    f.generations.recordGenerationResponse(meta('original-response', NOW + 2), 'original-0', {
      ...f.response(NOW + 2),
      upstreamHttpStatus: 401,
    }),
  )();
  const failed = f.fail('original-0', { aborted: false }, NOW + 2);
  assert.equal(
    failed.events.find((e) => e.eventType === 'ResponseEvidenceRecorded').payload.response
      .upstreamHttpStatus,
    401,
  );
  assert.ok(!failed.events.some((e) => e.eventType === 'QuotaStatusChanged'));
  assert.equal(quotas.readAccountQuota('a').status, 'active');
  assert.equal(f.generations.readCharge('original-0').status, 'refunded');
});

test('管理员组合proxy与enable实际发起只用目标连接，最终单提交；SQL尾错全表回滚且健康重试', async (t) => {
  const f = await fixture(t),
    { accounts } = setup(f),
    proxy = 'socks5://127.0.0.1:19001';
  await installReady(f);
  const input = {
    kind: 'configure',
    accountId: 'a',
    changes: { displayName: 'new profile', weight: 3.5, proxyUrl: proxy, enabled: true },
  };
  const authorize = async (write) => f.db.transaction(() => write(null))();
  let calls = 0;
  const query = async (pst, url, id, start) => {
    assert.equal(f.db.inTransaction, false);
    assert.equal(url, proxy);
    assert.equal(id, 'a');
    calls++;
    const activate = await start();
    activate?.();
    return official();
  };
  f.db.exec(
    "CREATE TEMP TRIGGER compatibility_tail AFTER UPDATE OF enabled ON upstream_accounts BEGIN SELECT RAISE(ABORT,'compatibility tail'); END",
  );
  const before = databaseState(f.db);
  const metadata = { ...meta('combined'), actorKind: 'admin', actorId: 'local-admin' };
  await assert.rejects(
    ownerAccountCommand(accounts, authorize, metadata, input, query, () => NOW + 2),
    /compatibility tail/,
  );
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER compatibility_tail');
  const result = await ownerAccountCommand(
    accounts,
    authorize,
    metadata,
    input,
    query,
    () => NOW + 3,
  );
  assert.equal(calls, 2);
  assert.equal(result.events[0].eventType, 'AccountConfigurationChanged');
  assert.equal(
    result.events.find((e) => e.eventType === 'QuotaObserved').payload.connectionSourceEventId,
    result.events[0].eventId,
  );
  assert.equal(
    f.db.prepare("SELECT proxy_url FROM upstream_credentials WHERE account_id='a'").get().proxy_url,
    proxy,
  );
  assert.equal(new Set(result.events.map((e) => e.commitSeq)).size, 1);
});

test('新76事实真实注册、配置和原观察完整回放，不安装私有代理/PST或触发网络', async (t) => {
  const f = await fixture(t),
    { configure } = setup(f);
  configure('name-weight', { displayName: 'x'.repeat(79) + '😀'.slice(0, 1), weight: 1.125 });
  configure('proxy-change', { proxyUrl: 'socks5://127.0.0.1:19001' });
  assert.equal(relationalReplayFacts().size, 76);
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replayer = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db);
  const deny = t.mock.method(globalThis, 'fetch', () => assert.fail('replay cannot fetch'));
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) seq FROM commits ORDER BY commit_seq')
    .all()) {
    const commit = readRelationalCommit(f.db, seq);
    target.transaction(() => replayer.replayCommit(commit, inputs))();
    const before = databaseState(target);
    assert.equal(target.transaction(() => replayer.replayCommit(commit, inputs))().replayed, true);
    assert.equal(databaseState(target), before);
  }
  assert.deepEqual(
    target.prepare('SELECT * FROM upstream_accounts').all(),
    f.db.prepare('SELECT * FROM upstream_accounts').all(),
  );
  assert.equal(relationalPrivateStateEmpty(target), true);
  assert.equal(deny.mock.callCount(), 0);
});

test('新配置真实完整提交与唯一ready安装均要求已注册owner，缺owner零写拒绝而非只改76清单', async (t) => {
  const f = await fixture(t),
    { configure } = setup(f),
    result = configure('real-configuration', { displayName: 'real', weight: 2.5 }).result;
  f.accept('owner-local');
  f.dispatch('owner-local-0');
  await f.publish('owner-local-0');
  const selected = await installReady(f),
    stored = readRelationalCommit(f.db, result.commitSeq);
  const replayTarget = publicPrefix(t, f, result.commitSeq),
    readyTarget = publicPrefix(t, f, BigInt(selected.finalCommitSeq) + 1n);
  const replayBefore = databaseState(replayTarget.target),
    readyBefore = databaseState(readyTarget.target);
  assert.equal(relationalAccountFacts.delete('AccountConfigurationChanged'), true);
  try {
    replayTarget.target.transaction(() =>
      assert.throws(
        () => replayTarget.owner.replayCommit(stored, replayTarget.inputs),
        /no owner for this complete commit/,
      ),
    )();
    assert.equal(databaseState(replayTarget.target), replayBefore);
    await assert.rejects(
      verifyAndInstallRelationalStorage(readyTarget.target, selected),
      /no owner for this complete commit/,
    );
    assert.equal(databaseState(readyTarget.target), readyBefore);
  } finally {
    relationalAccountFacts.add('AccountConfigurationChanged');
  }
  replayTarget.target.transaction(() =>
    replayTarget.owner.replayCommit(stored, replayTarget.inputs),
  )();
  await verifyAndInstallRelationalStorage(readyTarget.target, selected);
  assert.equal(
    readyTarget.target.prepare("SELECT weight FROM upstream_accounts WHERE id='a'").get().weight,
    2.5,
  );
});

test('合法完整envelope仍拒跨角色连接ref、同笔未来配置与缺失当前401决定，outercatch原子回滚后可健康重试', async (t) => {
  const f = await fixture(t),
    { accounts, quotas, configure } = setup(f);
  await installReady(f);
  const ticket = quotas.beginQuotaObservation('a', NOW);
  const observation = f.db.transaction(() =>
    quotas.recordQuotaObservation(
      meta('valid-observation'),
      ticket,
      accountQuotaResult('a', official(), new Date(NOW).toISOString()),
    ),
  )();
  f.accept('source-attempt');
  const dispatch = f.dispatch('source-attempt-0');
  f.db.transaction(() =>
    f.generations.recordGenerationResponse(meta('source-response', NOW + 2), 'source-attempt-0', {
      ...f.response(NOW + 2),
      upstreamHttpStatus: 401,
    }),
  )();
  const failure = f.fail('source-attempt-0', { aborted: false }, NOW + 2);
  assert.ok(failure.events.some((event) => event.eventType === 'QuotaStatusChanged'));
  const authorize = async (write) => f.db.transaction(() => write(null))();
  const combined = await ownerAccountCommand(
    accounts,
    authorize,
    { ...meta('combined-ref', NOW + 3), actorKind: 'admin', actorId: 'local-admin' },
    {
      kind: 'configure',
      accountId: 'a',
      changes: { proxyUrl: 'socks5://127.0.0.1:19001', enabled: true },
    },
    async (_pst, _url, _id, start) => {
      const activate = await start();
      activate?.();
      return official();
    },
    () => NOW + 3,
  );
  for (const mode of [
    'dispatch-role',
    'observed-role',
    'future-configuration',
    'missing-current-401',
  ]) {
    await t.test(mode, (t) => {
      const original = readRelationalCommit(
        f.db,
        mode === 'dispatch-role'
          ? dispatch.commitSeq
          : mode === 'observed-role'
            ? observation.commitSeq
            : mode === 'missing-current-401'
              ? failure.commitSeq
              : combined.commitSeq,
      );
      const { target, owner, inputs } = publicPrefix(t, f, original.events[0].commitSeq),
        bad = structuredClone(original);
      if (mode === 'dispatch-role') {
        bad.events.find(
          (e) => e.eventType === 'DispatchAuthorized',
        ).payload.connectionSourceEventId = 'opening-quota';
      }
      if (mode === 'observed-role') {
        bad.events.find((e) => e.eventType === 'QuotaObserved').payload.connectionSourceEventId =
          'opening-quota';
      }
      if (mode === 'future-configuration') {
        bad.events.push(bad.events.shift());
      }
      if (mode === 'missing-current-401') {
        bad.events = bad.events.filter((e) => e.eventType !== 'QuotaStatusChanged');
      }
      resequence(bad, target);
      const before = databaseState(target);
      let rejected = false;
      target.transaction(() => {
        try {
          owner.replayCommit(bad, inputs);
        } catch (error) {
          assert.match(
            error.message,
            mode === 'future-configuration'
              ? /账户完整事实与原安全意图\/前态规划不符/
              : /派发与真实原预扣|观察代次或序号|凭据失效决定必须/,
          );
          rejected = true;
        }
      })();
      assert.equal(rejected, true);
      assert.equal(databaseState(target), before);
      target.transaction(() => owner.replayCommit(original, inputs))();
      assert.equal(relationalPrivateStateEmpty(target), true);
    });
  }
  configure('new-current-source', { proxyUrl: '' });
});

test('保存的私有SOCKS真实握手/TLS贯通自动观察、manual enable及队列generate/upscale/encode，直连对照不经过代理', async (t) => {
  const f = await fixture(t),
    events = new EventEmitter(),
    controls = new Map(),
    { quotas, accounts, configure } = setup(f, controls);
  const certificate = await readFile(
      new URL('../fixtures/synthetic-localhost-cert.pem', import.meta.url),
      'utf8',
    ),
    privateKey = await readFile(
      new URL('../fixtures/synthetic-localhost-key.pem', import.meta.url),
      'utf8',
    ),
    previous = tls.getCACertificates('default');
  tls.setDefaultCACertificates([certificate]);
  t.after(() => tls.setDefaultCACertificates(previous));
  const seen = [],
    sockets = new Set();
  const upstream = createTlsHttpServer({ cert: certificate, key: privateKey }, async (req, res) => {
    assert.equal(f.db.inTransaction, false);
    seen.push(req.url);
    for await (const _ of req) {
    }
    let bytes, type;
    if (req.url === '/user/data') {
      type = 'application/json';
      bytes = Buffer.from(
        JSON.stringify({
          subscription: {
            tier: 3,
            active: true,
            trainingStepsLeft: { fixedTrainingStepsLeft: 8000, purchasedTrainingSteps: 0 },
            usage: { percent: 80, isNegative: false, timeUntilNextPercent: 600 },
          },
        }),
      );
    } else if (req.url === '/ai/encode-vibe') {
      type = 'application/octet-stream';
      bytes = Buffer.alloc(128, 0x80);
    } else {
      assert.ok(['/ai/generate-image', '/ai/upscale'].includes(req.url));
      type = 'image/webp';
      bytes = image;
    }
    res.writeHead(200, { 'content-type': type, 'content-length': bytes.length });
    res.end(bytes);
  });
  upstream.on('connection', (socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
  });
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const port = upstream.address().port;
  let tunnels = 0;
  const proxyPasswords = [];
  const proxy = createTcpServer((socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    let data = Buffer.alloc(0),
      phase = 'greeting';
    const receive = (chunk) => {
      data = Buffer.concat([data, chunk]);
      if (phase === 'greeting' && data.length >= 2 + data[1]) {
        data = data.subarray(2 + data[1]);
        socket.write(Buffer.from([5, 2]));
        phase = 'auth';
      }
      if (
        phase === 'auth' &&
        data.length >= 2 &&
        data.length >= 3 + data[1] &&
        data.length >= 3 + data[1] + data[2 + data[1]]
      ) {
        const user = data.subarray(2, 2 + data[1]).toString(),
          passwordLength = data[2 + data[1]],
          end = 3 + data[1] + passwordLength;
        assert.equal(user, 'synthetic');
        const password = data.subarray(3 + data[1], end).toString();
        assert.ok(['synthetic-password', 'synthetic-new-password'].includes(password));
        proxyPasswords.push(password);
        data = data.subarray(end);
        socket.write(Buffer.from([1, 0]));
        phase = 'connect';
      }
      if (phase === 'connect' && data.length >= 5 && data.length >= 7 + data[4]) {
        assert.equal(data[3], 3);
        const host = data.subarray(5, 5 + data[4]).toString(),
          end = 7 + data[4];
        assert.equal(host, 'localhost');
        assert.equal(data.readUInt16BE(5 + data[4]), port);
        tunnels++;
        phase = 'forward';
        socket.off('data', receive);
        const target = connect({ host: '127.0.0.1', port }, () => {
          socket.write(Buffer.from([5, 0, 0, 1, 127, 0, 0, 1, 0, 0]));
          if (data.length > end) {
            target.write(data.subarray(end));
          }
          socket.pipe(target);
          target.pipe(socket);
        });
        sockets.add(target);
        target.on('close', () => sockets.delete(target));
        target.on('error', () => socket.destroy());
        socket.on('error', () => target.destroy());
      }
    };
    socket.on('data', receive);
  });
  proxy.listen(0, '127.0.0.1');
  await once(proxy, 'listening');
  t.after(async () => {
    for (const socket of sockets) {
      socket.destroy();
    }
    await Promise.all([new Promise((r) => proxy.close(r)), new Promise((r) => upstream.close(r))]);
  });
  const proxyUrl = `socks5://synthetic:synthetic-password@127.0.0.1:${proxy.address().port}`;
  configure('real-socks', { proxyUrl });
  f.accept('local-existing');
  f.dispatch('local-existing-0');
  await f.publish('local-existing-0');
  await installReady(f);
  let clock = NOW + 10010;
  const env = {
    NOVELAI_API_URL: `https://localhost:${port}`,
    NOVELAI_ACCOUNT_API_URL: `https://localhost:${port}`,
    NOVELAI_STREAM_IMAGE: 'false',
    MOCK_WHEN_NO_ACCOUNT: 'false',
  };
  const refresh = createRelationalAccountQuotaRefresh({
    sqlite: f.db,
    quotas,
    events,
    env,
    now: () => clock,
    logger: { log() {}, error() {} },
  });
  t.after(() => refresh.stop());
  assert.equal((await refresh.refreshEnabled()).ok, 1);
  const authorize = async (write) => f.db.transaction(() => write(null))();
  const nextProxyUrl = proxyUrl.replace('synthetic-password', 'synthetic-new-password');
  await ownerAccountCommand(
    accounts,
    authorize,
    { ...meta('real-enable', clock), actorKind: 'admin', actorId: 'local-admin' },
    { kind: 'configure', accountId: 'a', changes: { proxyUrl: nextProxyUrl, enabled: true } },
    refresh.fetch,
    () => clock,
  );
  assert.equal(
    f.db.prepare("SELECT proxy_url FROM upstream_credentials WHERE account_id='a'").get().proxy_url,
    nextProxyUrl,
  );
  const runtime = createRelationalQueueRuntime({
    sqlite: f.db,
    generations: f.generations,
    inputs: f.inputs,
    dataDir: f.dir,
    controls,
    events,
    env,
    now: () => clock,
    onError: (event) => assert.fail(JSON.stringify(event)),
  });
  t.after(() => runtime.stop());
  await runtime.start();
  const admit = createRelationalMemberGenerationAdmission({
    sqlite: f.db,
    members: f.members,
    inputs: f.inputs,
    generations: f.generations,
    payloadBudget: createGenerationPayloadBudget(() => f.inputs.activePayloadBytes()),
    env,
    runtime,
    now: () => clock,
  });
  for (const operation of ['generate', 'upscale', 'encode-vibe']) {
    const body =
      operation === 'generate'
        ? {
            operation,
            input: 'synthetic proxy image',
            model: 'nai-diffusion-4-5-full',
            parameters: { width: 128, height: 128, steps: 30, seed: 7 },
          }
        : {
            operation,
            image: image.toString('base64'),
            ...(operation === 'encode-vibe'
              ? { model: 'nai-diffusion-4-5-full', information_extracted: 0.5 }
              : {}),
          };
    const normalized = normalizeMemberGeneration(body),
      confirmation = {
        requestFingerprint: normalized.requestFingerprint,
        versions: f.generations.readMemberGenerationCapabilities('m1').versions,
        maxPerJob: { v5: '100', anlasFixed: '4000', anlasPurchased: '4000' },
        maxTotal: { v5: '100', anlasFixed: '4000', anlasPurchased: '4000' },
      };
    const accepted = await admit({
      credential: { apiKey: keys.m1 },
      request: {
        headers: { 'idempotency-key': operation },
        rawHeaders: ['Idempotency-Key', operation],
      },
      body: { ...body, confirmation },
    });
    runtime.tick();
    const started = Date.now();
    while (
      f.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get(accepted.receipt.jobIds[0])
        .status === 'queued' ||
      controls.size
    ) {
      assert.ok(Date.now() - started < 5000, 'SOCKS runtime did not settle');
      await new Promise((r) => setTimeout(r, 5));
    }
    assert.equal(
      f.db.prepare('SELECT status FROM generation_jobs WHERE id=?').get(accepted.receipt.jobIds[0])
        .status,
      'done',
    );
    clock += 10001;
  }
  assert.equal(tunnels, 5);
  assert.deepEqual(proxyPasswords, [
    'synthetic-password',
    ...Array(4).fill('synthetic-new-password'),
  ]);
  assert.deepEqual(seen, [
    '/user/data',
    '/user/data',
    '/ai/generate-image',
    '/ai/upscale',
    '/ai/encode-vibe',
  ]);
  await fetchNovelAiAccountQuota('synthetic-direct-control', env, { proxyUrl: '' });
  assert.equal(tunnels, 5, 'direct request must not reuse the stored SOCKS tunnel');
  assert.equal(seen.at(-1), '/user/data');
  await runtime.stop();
  await refresh.stop();
});

test('选定private SHA/同cut联合恢复原proxy，未知NULL不能默认直连，真实Local IO期间撤销不被快照复活', async (t) => {
  const f = await fixture(t),
    { configure } = setup(f),
    proxy = 'socks5://synthetic:synthetic-private@127.0.0.1:19001';
  configure('snapshot-proxy', { proxyUrl: proxy });
  f.accept('snapshot-image');
  f.dispatch('snapshot-image-0');
  await f.publish('snapshot-image-0');
  const selected = await installReady(f),
    until = BigInt(selected.finalCommitSeq) + 1n;
  const corrupted = path.join(f.dir, 'private-other.sqlite');
  await fsPromises.copyFile(selected.sourcePath, corrupted);
  const changed = new Database(corrupted);
  changed.prepare("UPDATE upstream_credentials SET proxy_url='socks5://127.0.0.1:19002'").run();
  changed.close();
  const alternateSha = relationalSnapshotSha256(corrupted);
  assert.notEqual(alternateSha, selected.sourceSha256);
  const first = publicPrefix(t, f, until),
    before = databaseState(first.target);
  await assert.rejects(
    verifyAndInstallRelationalStorage(first.target, { ...selected, sourcePath: corrupted }),
    /snapshot|Private/i,
  );
  assert.equal(databaseState(first.target), before);
  await verifyAndInstallRelationalStorage(first.target, selected);
  assert.equal(
    first.target.prepare("SELECT proxy_url FROM upstream_credentials WHERE account_id='a'").get()
      .proxy_url,
    proxy,
  );
  const nullSource = new Database(corrupted);
  nullSource.prepare('UPDATE upstream_credentials SET proxy_url=NULL').run();
  nullSource.close();
  const unknown = publicPrefix(t, f, until),
    state = databaseState(unknown.target);
  await assert.rejects(
    verifyAndInstallRelationalStorage(unknown.target, {
      ...selected,
      sourcePath: corrupted,
      sourceSha256: relationalSnapshotSha256(corrupted),
    }),
    /不能默认直连/,
  );
  assert.equal(databaseState(unknown.target), state);
  const io = publicPrefix(t, f, until);
  io.target.transaction(() =>
    restoreRelationalPrivateSnapshot(io.target, selected.sourcePath, selected.sourceSha256),
  )();
  const originalOpen = fsPromises.open;
  let revoked = false;
  const mock = t.mock.method(fsPromises, 'open', async (filename, ...args) => {
    const handle = await originalOpen(filename, ...args);
    if (
      path.resolve(String(filename)).startsWith(path.resolve(f.dir) + path.sep) &&
      String(filename).endsWith('.webp')
    ) {
      const read = handle.readFile.bind(handle);
      handle.readFile = async (...args) => {
        const bytes = await read(...args);
        if (!revoked) {
          assert.equal(io.target.inTransaction, false);
          io.target.prepare('UPDATE upstream_credentials SET proxy_url=NULL').run();
          revoked = true;
        }
        return bytes;
      };
    }
    return handle;
  });
  syncBuiltinESMExports();
  try {
    await assert.rejects(
      verifyAndInstallRelationalStorage(io.target, selected),
      /private authentication changed during object IO/,
    );
  } finally {
    mock.mock.restore();
    syncBuiltinESMExports();
  }
  assert.equal(revoked, true, 'must reach real Local bytes before revocation');
  assert.equal(io.target.prepare('SELECT status FROM projection_state').get().status, 'building');
  assert.equal(
    io.target.prepare("SELECT proxy_url FROM upstream_credentials WHERE account_id='a'").get()
      .proxy_url,
    null,
  );
  assert.throws(() => new RelationalQuotaStore(io.target).prepareQuotaObservation('a'), {
    statusCode: 503,
  });
  assert.equal(relationalSnapshotSha256(selected.sourcePath), selected.sourceSha256);
});

test('缺失当前PST时proxy-only不能200丢值，name仍可改且PST与proxy组合健康原子恢复', async (t) => {
  const f = await fixture(t),
    { configure, quotas } = setup(f);
  f.accept('withdraw-image');
  f.dispatch('withdraw-image-0');
  await f.publish('withdraw-image-0');
  await installReady(f);
  // 合成当前私有撤销；不篡改ready，也不从公开事件补回旧PST。
  f.db.prepare("DELETE FROM upstream_credentials WHERE account_id='a'").run();
  const before = databaseState(f.db),
    proxyUrl = 'socks5://127.0.0.1:19001';
  assert.throws(() => configure('missing-pst-proxy', { proxyUrl }), { statusCode: 503 });
  assert.equal(databaseState(f.db), before);
  configure('public-name-only', { displayName: 'no private credential' });
  assert.equal(f.db.prepare('SELECT COUNT(*) n FROM upstream_credentials').get().n, 0);
  configure('restore-pair', { token: 'pst-restored-source', proxyUrl });
  assert.deepEqual(f.db.prepare('SELECT pst,proxy_url FROM upstream_credentials').get(), {
    pst: 'pst-restored-source',
    proxy_url: proxyUrl,
  });
  assert.equal(f.db.prepare("SELECT enabled FROM upstream_accounts WHERE id='a'").get().enabled, 0);
  assert.equal(quotas.prepareQuotaObservation('a').proxyUrl, proxyUrl);
});

test('当前连接来源使用小型事实索引，原序切点不扫描全部账务事件或排序临时树', async (t) => {
  const f = await fixture(t);
  let sql;
  const prepare = f.db.prepare.bind(f.db),
    mock = t.mock.method(f.db, 'prepare', (statement, ...args) => {
      if (statement.includes('SELECT event_id AS id FROM domain_events WHERE account_id=')) {
        sql = statement;
      }
      return prepare(statement, ...args);
    });
  assert.equal(relationalAccountConnectionSource(f.db, 'a'), 'opening-account');
  mock.mock.restore();
  const plan = f.db
    .prepare('EXPLAIN QUERY PLAN ' + sql)
    .all('a', 'account:a', null, null, null, null);
  assert.match(JSON.stringify(plan), /idx_account_connection_source/);
  assert.doesNotMatch(JSON.stringify(plan), /SCAN|TEMP B-TREE/);
});

test('账户原安全意图用前态规划完整bulk集合/暂停/顺序，mixed no-op合法且全noop不编事实', async (t) => {
  const f = await fixture(t),
    { accounts, configure } = setup(f),
    admin = (id) => ({ ...meta(id), actorKind: 'admin', actorId: 'local-admin' }),
    register = (id, weight) =>
      f.db.transaction(() =>
        accounts.prepareAccountCommand(
          admin('register-' + id),
          {
            kind: 'register',
            accountId: id,
            displayName: id,
            token: 'pst-synthetic-' + id,
            configVersion: 1,
            bind: false,
            weight,
          },
          null,
        ),
      )();
  register('extra', 1);
  register('noop', 2.5);
  configure('a-weight', { weight: 2.5 });
  const input = { ids: ['a', 'extra', 'noop', 'absent', 'extra'], weight: 2.5, enabled: false },
    result = f.db.transaction(() =>
      accounts.configureAccounts(admin('closed-bulk'), input, null),
    )(),
    stored = readRelationalCommit(f.db, result.commitSeq);
  assert.deepEqual(stored.command.receipt.accountIds, ['a', 'extra', 'noop']);
  assert.deepEqual(stored.command.receipt.accountIntent, { kind: 'bulk', ...input });
  assert.deepEqual(
    stored.events.map((e) => [e.eventType, e.accountId]),
    [
      ['AvailabilityChanged', 'a'],
      ['AccountConfigurationChanged', 'extra'],
    ],
  );
  for (const mode of [
    'target',
    'all-availability',
    'duplicate',
    'reverse',
    'receipt-target',
    'missing-intent',
    'wrong-actor',
  ]) {
    await t.test(mode, (t) => {
      const { target, owner, inputs } = publicPrefix(t, f, result.commitSeq),
        bad = structuredClone(stored);
      if (mode === 'target') {
        bad.events = bad.events.filter((e) => e.accountId !== 'extra');
      }
      if (mode === 'all-availability') {
        bad.events = bad.events.filter((e) => e.eventType !== 'AvailabilityChanged');
      }
      if (mode === 'duplicate') {
        bad.events.push({ ...bad.events[1], eventId: 'duplicate-account-configuration' });
      }
      if (mode === 'reverse') {
        bad.events.reverse();
      }
      if (mode === 'receipt-target') {
        bad.command.receipt.accountIds = ['a', 'extra'];
      }
      if (mode === 'missing-intent') {
        delete bad.command.receipt.accountIntent;
      }
      if (mode === 'wrong-actor') {
        bad.command.actorKind = 'member';
        bad.command.actorId = 'm1';
      }
      resequence(bad, target);
      const before = databaseState(target);
      let error;
      target.transaction(() => {
        try {
          owner.replayCommit(bad, inputs);
        } catch (caught) {
          error = caught;
        }
      })();
      assert.match(error?.message ?? '', /账户.*(完整|安全|意图)/);
      assert.equal(databaseState(target), before);
      target.transaction(() => owner.replayCommit(stored, inputs))();
      assert.deepEqual(
        target.prepare('SELECT weight,enabled FROM upstream_accounts ORDER BY id').all(),
        [
          { weight: 2.5, enabled: 0 },
          { weight: 2.5, enabled: 0 },
          { weight: 2.5, enabled: 0 },
        ],
      );
      const same = databaseState(target);
      assert.equal(target.transaction(() => owner.replayCommit(stored, inputs))().replayed, true);
      assert.equal(databaseState(target), same);
    });
  }
  const before = databaseState(f.db);
  assert.equal(
    f.db.transaction(() => accounts.configureAccounts(admin('closed-bulk'), input, null))()
      .replayed,
    true,
  );
  assert.equal(databaseState(f.db), before);
  assert.equal(
    f.db.transaction(() => accounts.configureAccounts(admin('all-noop'), input, null))().ignored,
    true,
  );
  assert.equal(databaseState(f.db), before);
});

test('单账户组合原意图拒遗漏profile/轮换/暂停及重复重排，原内部意图不进入外部回包', async (t) => {
  const f = await fixture(t),
    { accounts, configure } = setup(f),
    result = configure('single-closure', {
      displayName: 'required-profile',
      weight: 2.25,
      proxyUrl: 'socks5://127.0.0.1:19001',
      token: 'pst-private-replacement',
      enabled: false,
    }).result,
    stored = readRelationalCommit(f.db, result.commitSeq);
  assert.equal(stored.command.receipt.accountIntent.credentialChanged, true);
  assert.equal(stored.command.receipt.accountIntent.connectionChanged, true);
  const publicText = JSON.stringify(stored);
  assert.ok(!publicText.includes('pst-private-replacement'));
  assert.ok(!publicText.includes('socks5://'));
  for (const mode of [
    'profile',
    'credential',
    'availability',
    'duplicate',
    'reverse',
    'missing-intent',
  ]) {
    await t.test(mode, (t) => {
      const { target, owner, inputs } = publicPrefix(t, f, result.commitSeq),
        bad = structuredClone(stored);
      const removed = {
        profile: 'AccountConfigurationChanged',
        credential: 'CredentialRotated',
        availability: 'AvailabilityChanged',
      }[mode];
      if (removed) {
        bad.events = bad.events.filter((e) => e.eventType !== removed);
      }
      if (mode === 'duplicate') {
        bad.events.push({ ...bad.events[0], eventId: 'duplicate-profile' });
      }
      if (mode === 'reverse') {
        bad.events.reverse();
      }
      if (mode === 'missing-intent') {
        delete bad.command.receipt.accountIntent;
      }
      resequence(bad, target);
      const before = databaseState(target);
      let error;
      target.transaction(() => {
        try {
          owner.replayCommit(bad, inputs);
        } catch (caught) {
          error = caught;
        }
      })();
      assert.match(error?.message ?? '', /账户.*(完整|安全|意图)/);
      assert.equal(databaseState(target), before);
      target.transaction(() => owner.replayCommit(stored, inputs))();
      assert.equal(
        target.prepare("SELECT credential_epoch FROM upstream_accounts WHERE id='a'").get()
          .credential_epoch,
        2,
      );
    });
  }
  const before = databaseState(f.db),
    safe = await ownerAccountCommand(
      accounts,
      async (run) => f.db.transaction(() => run(null))(),
      { ...meta('single-closure'), actorKind: 'admin', actorId: 'local-admin' },
      {
        kind: 'configure',
        accountId: 'a',
        changes: {
          displayName: 'required-profile',
          weight: 2.25,
          proxyUrl: 'socks5://127.0.0.1:19001',
          token: 'pst-private-replacement',
          enabled: false,
        },
      },
      undefined,
    );
  assert.deepEqual(safe.receipt, { accountId: 'a', enabled: false });
  assert.equal(databaseState(f.db), before);
});

test('非绑定真实官方观察与启用保留原值，不能同步/分配或切换现共享金融池', async (t) => {
  const f = await fixture(t),
    env = { OWNER_USER_ID: 'm1' },
    quotas = new RelationalQuotaStore(f.db, env),
    accounts = new RelationalAccountStore(f.db, quotas, new Map(), env),
    admin = (id) => ({ ...meta(id), actorKind: 'admin', actorId: 'local-admin' }),
    authorize = async (run) => f.db.transaction(() => run(null))();
  f.db.transaction(() =>
    accounts.prepareAccountCommand(
      admin('passive-register'),
      {
        kind: 'register',
        accountId: 'extra',
        displayName: 'extra',
        token: 'pst-passive-extra',
        configVersion: 1,
        bind: false,
      },
      null,
    ),
  )();
  await installReady(f);
  const pool = () =>
      JSON.stringify([
        f.db.prepare('SELECT * FROM sharing_policy').all(),
        f.db.prepare("SELECT * FROM account_quota_state WHERE account_id='a'").all(),
        f.db.prepare('SELECT * FROM wallets ORDER BY account_id,member_id').all(),
        f.db.prepare('SELECT * FROM operating_state ORDER BY account_id').all(),
        f.db.prepare('SELECT * FROM charges').all(),
      ]),
    before = pool(),
    ticket = quotas.beginQuotaObservation('extra', NOW),
    observed = f.db.transaction(() =>
      quotas.recordQuotaObservation(
        admin('passive-observed'),
        ticket,
        accountQuotaResult(
          'extra',
          { ...official(), v5UsageIsNegative: true },
          new Date(NOW).toISOString(),
        ),
      ),
    )();
  assert.deepEqual(
    observed.events.map((e) => e.eventType),
    ['QuotaObserved'],
  );
  assert.equal(pool(), before);
  assert.deepEqual(quotas.readOfficialQuotaV5('extra'), {
    v5UsagePercent: 80,
    v5UsageIsNegative: true,
  });
  assert.equal(quotas.readAccountQuota('extra').baselineEventId, null);
  assert.equal(quotas.readAccountQuota('extra').status, 'inactive_opus');
  const failedTicket = quotas.beginQuotaObservation('extra', NOW + 1),
    failed = f.db.transaction(() =>
      quotas.recordQuotaObservation(
        admin('passive-failed'),
        failedTicket,
        accountQuotaErrorResult(
          'extra',
          Object.assign(new Error('synthetic read reset'), { code: 'ECONNRESET' }),
          new Date(NOW + 1).toISOString(),
        ),
      ),
    )();
  assert.equal(failed.events[0].payload.errorCode, 'sync_error');
  assert.deepEqual(quotas.readOfficialQuotaV5('extra'), {
    v5UsagePercent: 80,
    v5UsageIsNegative: true,
  });
  assert.equal(pool(), before);
  const enabled = await ownerAccountCommand(
    accounts,
    authorize,
    admin('passive-enable'),
    { kind: 'configure', accountId: 'extra', changes: { enabled: true } },
    async (_pst, _proxy, _id, start) => {
      const activate = await start();
      activate?.();
      return official();
    },
    () => NOW + 2,
  );
  assert.equal(enabled.receipt.enabled, true);
  assert.equal(
    f.db.prepare("SELECT enabled FROM upstream_accounts WHERE id='extra'").get().enabled,
    1,
  );
  assert.equal(pool(), before);
  assert.equal(f.db.prepare('SELECT bound_account_id id FROM sharing_policy').get().id, 'a');
  assert.equal(quotas.readAccountQuota('extra').baselineEventId, null);
  const unchanged = databaseState(f.db),
    funding = {
      action: 'operating-register-funding',
      resource: 'anlasFixed',
      amount: 10,
      receipt: 'passive-funding-receipt',
    };
  f.db.transaction(() =>
    assert.throws(
      () => quotas.manageQuotaFunding(admin('no-passive-finance'), 'extra', funding, null),
      { message: '资金操作只接受当前绑定共享账户', statusCode: 409 },
    ),
  )();
  assert.equal(databaseState(f.db), unchanged);
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const owner = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => owner.replayCommit(readRelationalCommit(f.db, seq), inputs))();
  }
  assert.deepEqual(
    target.prepare('SELECT * FROM account_quota_state ORDER BY account_id').all(),
    f.db.prepare('SELECT * FROM account_quota_state ORDER BY account_id').all(),
  );
  assert.equal(relationalPrivateStateEmpty(target), true);
  assert.ok(
    f.db
      .transaction(() =>
        quotas.manageQuotaFunding(admin('healthy-bound-finance'), 'a', funding, null),
      )()
      .events.some((e) => e.eventType === 'FundingRegistered'),
  );
});

test('新增核验只能使用实际owner准备/开始对象，伪造克隆与existing身份例外拒绝，失败释放新ID票号', async (t) => {
  const f = await fixture(t);
  await installReady(f);
  const { accounts, quotas } = setup(f),
    metadata = { ...meta('new-capability'), actorKind: 'admin', actorId: 'local-admin' },
    input = {
      kind: 'register',
      accountId: 'future',
      displayName: 'future',
      token: 'pst-future-source',
      configVersion: 1,
      bind: false,
      enabled: true,
    },
    prepare = (id) =>
      f.db.transaction(() =>
        accounts.prepareAccountCommand({ ...metadata, commandId: id }, input, null),
      )();
  const prepared = prepare('new-capability').prepared,
    before = databaseState(f.db);
  for (const weight of [0, 101, NaN, Infinity]) {
    f.db.transaction(() =>
      assert.throws(() =>
        accounts.prepareAccountCommand(
          { ...metadata, commandId: 'invalid-weight' },
          { ...input, weight },
          null,
        ),
      ),
    )();
    assert.equal(databaseState(f.db), before);
  }
  assert.equal(Object.isFrozen(prepared), true);
  assert.equal(Object.isFrozen(prepared.registration), true);
  for (const fake of [{ ...prepared }, Object.create(prepared), { ...prepared, accountId: 'a' }]) {
    assert.throws(
      () => startAccountOutsideSql(f, accounts, metadata, input, fake, null),
      /新账户准备/,
    );
    assert.equal(databaseState(f.db), before);
  }
  assert.throws(
    () =>
      quotas.beginAccountRegistrationObservation(
        {
          accountId: 'a',
          credentialEpoch: 1,
          pst: 'synthetic-upstream-pst',
          proxyUrl: '',
          connectionSourceEventId: 'opening-account',
        },
        NOW,
      ),
    /新账户身份或来源已经存在/,
  );
  const started = startAccountOutsideSql(f, accounts, metadata, input, prepared, null).ticket;
  assert.equal(started.sequence, '1');
  assert.equal(Object.isFrozen(started), true);
  assert.throws(
    () => startAccountOutsideSql(f, accounts, metadata, input, prepared, null),
    /新账户准备/,
  );
  f.db.transaction(() =>
    assert.throws(
      () =>
        accounts.completeAccountEnable(
          metadata,
          input,
          { ...started },
          accountQuotaResult('future', official(), new Date(NOW).toISOString()),
          null,
        ),
      /新账户准备/,
    ),
  )();
  assert.equal(databaseState(f.db), before);
  accounts.releaseAccountRegistration(prepared);
  f.db.transaction(() =>
    assert.throws(
      () =>
        accounts.completeAccountEnable(
          metadata,
          input,
          started,
          accountQuotaResult('future', official(), new Date(NOW).toISOString()),
          null,
        ),
      /新账户准备/,
    ),
  )();
  const secondMetadata = { ...metadata, commandId: 'released-capability' },
    second = prepare(secondMetadata.commandId).prepared,
    secondTicket = startAccountOutsideSql(f, accounts, secondMetadata, input, second, null).ticket;
  assert.equal(
    secondTicket.sequence,
    '1',
    'released absent identity must not accumulate volatile order entries',
  );
  const result = f.db.transaction(() =>
    accounts.completeAccountEnable(
      secondMetadata,
      input,
      secondTicket,
      accountQuotaResult('future', official(), new Date(NOW).toISOString()),
      null,
    ),
  )();
  accounts.releaseAccountRegistration(second);
  assert.deepEqual(
    result.events.map((e) => e.eventType),
    ['AccountRegistered', 'QuotaObserved', 'AvailabilityChanged'],
  );
  assert.equal(result.events[1].payload.connectionSourceEventId, result.events[0].eventId);
  f.db.prepare("DELETE FROM upstream_credentials WHERE account_id='future'").run();
  const withdrawn = databaseState(f.db),
    prior = await ownerAccountCommand(
      accounts,
      async (run) => f.db.transaction(() => run(null))(),
      secondMetadata,
      input,
      () => assert.fail('prior registration cannot requery'),
    );
  assert.deepEqual(prior.receipt, { accountId: 'future', enabled: true });
  assert.equal(databaseState(f.db), withdrawn);
  const newerInput = { ...input, accountId: 'newer-source' },
    oldMeta = { ...metadata, commandId: 'older-new-source' },
    newMeta = { ...metadata, commandId: 'newer-new-source' },
    oldPrep = f.db.transaction(() => accounts.prepareAccountCommand(oldMeta, newerInput, null))()
      .prepared,
    newPrep = f.db.transaction(() => accounts.prepareAccountCommand(newMeta, newerInput, null))()
      .prepared,
    oldTicket = startAccountOutsideSql(f, accounts, oldMeta, newerInput, oldPrep, null).ticket,
    newTicket = startAccountOutsideSql(f, accounts, newMeta, newerInput, newPrep, null).ticket;
  assert.equal(oldTicket.sequence, '1');
  assert.equal(newTicket.sequence, '2');
  accounts.releaseAccountRegistration(oldPrep);
  const latest = f.db.transaction(() =>
    accounts.completeAccountEnable(
      newMeta,
      newerInput,
      newTicket,
      accountQuotaResult(newerInput.accountId, official(), new Date(NOW).toISOString()),
      null,
    ),
  )();
  assert.equal(
    latest.receipt.enabled,
    true,
    'cleaning older preparation must not cancel the newer actual ticket',
  );
  accounts.releaseAccountRegistration(newPrep);
});

test('新增actual-start外层授权事务尾失败即使没返回ticket，也释放中央序号而不发raw或保存事实', async (t) => {
  const f = await fixture(t);
  await installReady(f);
  const { accounts } = setup(f),
    metadata = { ...meta('new-start-tail'), actorKind: 'admin', actorId: 'local-admin' },
    input = {
      kind: 'register',
      accountId: 'start-tail',
      displayName: 'start-tail',
      token: 'pst-start-tail',
      configVersion: 1,
      bind: false,
      enabled: true,
    };
  let stages = 0,
    raw = 0;
  const before = databaseState(f.db),
    authorize = async (run) =>
      f.db.transaction(() => {
        const result = run(null);
        stages++;
        if (stages === 2) {
          throw new Error('synthetic actual-start auth tail');
        }
        return result;
      })();
  await assert.rejects(
    ownerAccountCommand(
      accounts,
      authorize,
      metadata,
      input,
      async (_pst, _proxy, _id, start) => {
        const activate = await start();
        activate?.();
        raw++;
        return official();
      },
      () => NOW,
    ),
    /synthetic actual-start auth tail/,
  );
  assert.equal(raw, 0);
  assert.equal(databaseState(f.db), before);
  const prepared = f.db.transaction(() => accounts.prepareAccountCommand(metadata, input, null))()
      .prepared,
    ticket = startAccountOutsideSql(f, accounts, metadata, input, prepared, null).ticket;
  assert.equal(ticket.sequence, '1');
  accounts.releaseAccountRegistration(prepared);
  assert.equal(databaseState(f.db), before);
});

test('新增真实核验的三事实完整闭包与原连接角色分别验证，公共回放不产生私有认证', async (t) => {
  const f = await fixture(t);
  await installReady(f);
  const { accounts } = setup(f),
    metadata = {
      ...meta('registered-with-observation'),
      actorKind: 'admin',
      actorId: 'local-admin',
    },
    input = {
      kind: 'register',
      accountId: 'verified-source',
      displayName: 'verified source',
      token: 'pst-verified-source',
      configVersion: 1,
      bind: false,
      enabled: true,
    },
    result = await ownerAccountCommand(
      accounts,
      async (run) => f.db.transaction(() => run(null))(),
      metadata,
      input,
      async (_pst, _proxy, _id, start) => {
        const activate = await start();
        activate?.();
        return official();
      },
      () => NOW,
    ),
    stored = readRelationalCommit(f.db, result.commitSeq);
  assert.equal(stored.command.receipt.accountIntent.verified, true);
  for (const mode of [
    'missing-observed',
    'missing-availability',
    'duplicate',
    'reverse',
    'wrong-source',
    'missing-proof',
  ]) {
    await t.test(mode, (t) => {
      const { target, owner, inputs } = publicPrefix(t, f, result.commitSeq),
        bad = structuredClone(stored);
      if (mode === 'missing-observed') {
        bad.events = bad.events.filter((e) => e.eventType !== 'QuotaObserved');
      }
      if (mode === 'missing-availability') {
        bad.events = bad.events.filter((e) => e.eventType !== 'AvailabilityChanged');
      }
      if (mode === 'duplicate') {
        bad.events.push({ ...bad.events[1], eventId: 'duplicate-new-observed' });
      }
      if (mode === 'reverse') {
        bad.events.reverse();
      }
      if (mode === 'wrong-source') {
        bad.events[1].payload.connectionSourceEventId = 'opening-account';
      }
      if (mode === 'missing-proof') {
        delete bad.command.receipt.accountIntent.verified;
      }
      resequence(bad, target);
      const before = databaseState(target);
      let error;
      target.transaction(() => {
        try {
          owner.replayCommit(bad, inputs);
        } catch (caught) {
          error = caught;
        }
      })();
      assert.match(
        error?.message ?? '',
        mode === 'wrong-source' ? /观察代次或序号已经过期/ : /账户.*(完整|核验|意图)/,
      );
      assert.equal(databaseState(target), before);
      target.transaction(() => owner.replayCommit(stored, inputs))();
      const same = databaseState(target);
      assert.equal(target.transaction(() => owner.replayCommit(stored, inputs))().replayed, true);
      assert.equal(databaseState(target), same);
      assert.equal(relationalPrivateStateEmpty(target), true);
      assert.equal(
        target.prepare("SELECT enabled FROM upstream_accounts WHERE id='verified-source'").get()
          .enabled,
        1,
      );
    });
  }
});

async function accountBatchFixture(t) {
  const f = await fixture(t),
    { accounts, quotas, configure } = setup(f, new Map(), { OWNER_USER_ID: 'm1' }),
    admin = (id) => ({ ...meta(id), actorKind: 'admin', actorId: 'local-admin' }),
    authorize = async (run) => f.db.transaction(() => run(null))();
  f.db.transaction(() =>
    accounts.prepareAccountCommand(
      admin('batch-register-extra'),
      {
        kind: 'register',
        accountId: 'extra',
        displayName: 'Extra',
        token: 'pst-batch-extra',
        weight: 1,
        configVersion: quotas.readQuotaConfiguration().configVersion,
        bind: false,
      },
      null,
    ),
  )();
  await installReady(f);
  let clock = NOW + 1000;
  const calls = [],
    query = (raw = async () => official()) =>
      createAccountQuotaQuery({
        now: () => clock,
        logger: { log() {} },
        fetchQuota: async (token, ...rest) => {
          assert.equal(f.db.inTransaction, false);
          calls.push(token);
          return raw(token, ...rest);
        },
      });
  return {
    ...f,
    accounts,
    quotas,
    configure,
    admin,
    authorize,
    calls,
    query,
    advance(ms) {
      clock += ms;
    },
    get clock() {
      return clock;
    },
    run(id, input, fetch = query()) {
      return adminAccountBatchCommand(
        accounts,
        authorize,
        { ...admin(id), recordedAt: clock },
        input,
        fetch,
        () => clock,
      );
    },
  };
}

test('账户批次真实两观察同一提交应用全体启用与小数weight，旧回执不被当前值重造且回放无私有', async (t) => {
  const f = await accountBatchFixture(t),
    input = { ids: ['extra', 'unknown', 'a', 'a'], mode: 'enable', weight: 2.5 },
    beforeSeq = f.db.prepare('SELECT MAX(commit_seq) n FROM commits').get().n,
    result = await f.run('batch-all-valid', input),
    stored = readRelationalCommit(f.db, result.commitSeq);
  assert.equal(f.calls.length, 2);
  assert.deepEqual(result.receipt, {
    accountIds: ['a', 'extra'],
    checked: 2,
    ok: 2,
    failed: 0,
    ignored: 0,
  });
  assert.equal(f.db.prepare('SELECT MAX(commit_seq) n FROM commits').get().n, beforeSeq + 1);
  assert.deepEqual(
    f.db.prepare('SELECT id,enabled,weight FROM upstream_accounts ORDER BY id').all(),
    [
      { id: 'a', enabled: 1, weight: 2.5 },
      { id: 'extra', enabled: 1, weight: 2.5 },
    ],
  );
  assert.equal(stored.events.filter((e) => e.eventType === 'QuotaObserved').length, 2);
  assert.equal(stored.events.filter((e) => e.eventType === 'AvailabilityChanged').length, 2);
  f.configure('batch-later-profile', { weight: 3, enabled: false });
  const before = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() n').get().n;
  const prior = await f.run('batch-all-valid', input, () =>
    assert.fail('old receipt must not fetch'),
  );
  assert.equal(prior.replayed, true);
  assert.deepEqual(prior.receipt, result.receipt);
  await assert.rejects(
    f.run('batch-all-valid', { ...input, weight: 3 }, () =>
      assert.fail('conflicting intent cannot fetch'),
    ),
    { statusCode: 409, code: 'idempotency_conflict' },
  );
  assert.equal(databaseState(f.db), before);
  assert.equal(f.db.prepare('SELECT total_changes() n').get().n, changes);
  t.mock.method(globalThis, 'fetch', () => assert.fail('batch public replay cannot fetch'));
  const { target, owner, inputs } = publicPrefix(t, f, stored.events[0].commitSeq);
  target.transaction(() => owner.replayCommit(stored, inputs))();
  assert.equal(relationalPrivateStateEmpty(target), true);
  assert.deepEqual(
    target.prepare('SELECT id,enabled,weight FROM upstream_accounts ORDER BY id').all(),
    [
      { id: 'a', enabled: 1, weight: 2.5 },
      { id: 'extra', enabled: 1, weight: 2.5 },
    ],
  );
  const unchanged = databaseState(target),
    total = target.prepare('SELECT total_changes() n').get().n;
  assert.equal(target.transaction(() => owner.replayCommit(stored, inputs))().replayed, true);
  assert.equal(databaseState(target), unchanged);
  assert.equal(target.prepare('SELECT total_changes() n').get().n, total);
  f.db.prepare("UPDATE projection_state SET status='building'").run();
  const building = databaseState(f.db);
  await assert.rejects(
    f.run('batch-all-valid', input, () => assert.fail('unready old receipt cannot fetch')),
    /Relational projection is incomplete/,
  );
  assert.equal(databaseState(f.db), building);
});

test('账户批次任一真实官方失败只保存观察和原安全暂停，不部分应用请求启用weight', async (t) => {
  const f = await accountBatchFixture(t),
    input = { ids: ['a', 'extra'], mode: 'enable', weight: 9 };
  await f.run('batch-prior-enable', { ids: ['a', 'extra'], mode: 'enable' });
  f.advance(11 * 60 * 1000);
  const before = f.db.prepare('SELECT id,enabled,weight FROM upstream_accounts ORDER BY id').all(),
    seq = f.db.prepare('SELECT MAX(commit_seq) n FROM commits').get().n;
  await assert.rejects(
    f.run(
      'batch-failed',
      input,
      f.query(async (token) => {
        if (token === 'pst-batch-extra') {
          throw Object.assign(new Error('账号已封禁'), { upstreamStatus: 403 });
        }
        return official();
      }),
    ),
    { statusCode: 409, message: '未核验到全部有效 Opus，批量启用和权重未应用' },
  );
  const stored = readRelationalCommit(f.db, String(seq + 1));
  assert.equal(stored.events.filter((e) => e.eventType === 'QuotaObserved').length, 2);
  assert.equal(
    stored.events.some((e) => e.eventType === 'AccountConfigurationChanged'),
    false,
  );
  assert.equal(
    stored.events.some((e) => e.eventType === 'AvailabilityChanged' && e.payload.enabled),
    false,
  );
  assert.deepEqual(
    f.db.prepare('SELECT id,enabled,weight FROM upstream_accounts ORDER BY id').all(),
    [before[0], { ...before[1], enabled: 0 }],
  );
  const unchanged = databaseState(f.db),
    calls = f.calls.length;
  await assert.rejects(
    f.run('batch-failed', input, () => assert.fail('saved failure must not fetch')),
    { statusCode: 409 },
  );
  assert.equal(databaseState(f.db), unchanged);
  assert.equal(f.calls.length, calls);
  const { target, owner, inputs } = publicPrefix(t, f, stored.events[0].commitSeq);
  target.transaction(() => owner.replayCommit(stored, inputs))();
  assert.equal(
    target.prepare("SELECT enabled FROM upstream_accounts WHERE id='a'").get().enabled,
    1,
  );
  assert.equal(
    target.prepare("SELECT enabled FROM upstream_accounts WHERE id='extra'").get().enabled,
    0,
  );
  assert.equal(relationalPrivateStateEmpty(target), true);
});

test('账户批次当前真实fresh no-op不重新GET，缺真实Opening来源和当前凭据错误不冒充资格', async (t) => {
  const f = await accountBatchFixture(t);
  await f.run('batch-fresh-first', { ids: ['a'], mode: 'enable' });
  const calls = f.calls.length;
  await f.run('batch-mixed-fresh', { ids: ['a', 'extra'], mode: 'enable' });
  assert.equal(f.calls.length, calls + 1);
  await f.run('batch-weight-is-not-noop', { ids: ['a', 'extra'], mode: 'enable', weight: 1.25 });
  assert.equal(f.calls.length, calls + 3);
  const before = databaseState(f.db);
  const noop = await f.run(
    'batch-all-noop',
    { ids: ['a', 'extra'], mode: 'enable', weight: 1.25 },
    () => assert.fail('fresh noop cannot GET'),
  );
  assert.equal(noop.ignored, true);
  assert.equal(databaseState(f.db), before);
  const source = f.db
    .prepare("SELECT observation_event_id id FROM account_quota_state WHERE account_id='a'")
    .get().id;
  assert.equal(f.quotas.readAccountEnableProof('a', f.clock), source);
  f.db
    .prepare("UPDATE account_quota_state SET quota_error_code='invalid_key' WHERE account_id='a'")
    .run();
  assert.equal(f.quotas.readAccountEnableProof('a', f.clock), null);
});

test('账户批次第二实际start失败或较早来源CAS变化，首个真实GET也不能留下半批观察', async (t) => {
  for (const mode of ['unstarted-busy', 'prior-source-changed']) {
    await t.test(mode, async (t) => {
      const f = await accountBatchFixture(t),
        original = f.query();
      let expected = databaseState(f.db),
        invocations = 0;
      const query = async (...args) => {
        if (++invocations === 2 && mode === 'unstarted-busy') {
          throw Object.assign(new Error('账户额度查询仍在进行'), { statusCode: 409 });
        }
        const result = await original(...args);
        if (invocations === 1 && mode === 'prior-source-changed') {
          f.configure('batch-competing-connection', { proxyUrl: 'socks5://localhost:1080' });
          expected = databaseState(f.db);
        }
        return result;
      };
      await assert.rejects(
        f.run('batch-precondition', { ids: ['a', 'extra'], mode: 'observe' }, query),
        { statusCode: 409 },
      );
      assert.equal(f.calls.length, 1);
      assert.equal(databaseState(f.db), expected);
    });
  }
});

test('账户批次第二投影SQL尾错外catch全表回滚，健康同键重试只保留一次完整批次', async (t) => {
  const f = await accountBatchFixture(t),
    input = { ids: ['a', 'extra'], mode: 'enable', weight: 4.5 };
  let firstProjected = false,
    privateDeleted = false;
  f.db.function('batch_projection_seen', (n) => {
    firstProjected = n === 1;
    return 0;
  });
  f.db.function('batch_private_seen', (n) => {
    privateDeleted = n === 0;
    return 0;
  });
  f.db.exec(
    `CREATE TEMP TRIGGER batch_second_tail AFTER UPDATE ON account_quota_state WHEN NEW.account_id='extra' BEGIN SELECT batch_projection_seen((SELECT COUNT(*) FROM account_quota_state WHERE account_id='a' AND observed_at=${f.clock})); DELETE FROM upstream_credentials WHERE account_id='a'; SELECT batch_private_seen((SELECT COUNT(*) FROM upstream_credentials WHERE account_id='a')); SELECT RAISE(ABORT,'batch second tail'); END`,
  );
  const before = databaseState(f.db);
  await assert.rejects(f.run('batch-tail', input), /batch second tail/);
  assert.equal(firstProjected, true);
  assert.equal(privateDeleted, true, 'fault tail actually changed private data before rollback');
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER batch_second_tail');
  const result = await f.run('batch-tail', input),
    same = databaseState(f.db);
  assert.equal(result.events.filter((e) => e.eventType === 'QuotaObserved').length, 2);
  assert.equal(
    (await f.run('batch-tail', input, () => assert.fail('duplicate cannot fetch'))).replayed,
    true,
  );
  assert.equal(databaseState(f.db), same);
});

test('账户批次完整envelope删目标/可用性/配置/金融、乱序或错连接均拒绝，common与窄回放相同闭包', async (t) => {
  const f = await accountBatchFixture(t),
    result = await f.run('batch-closure', { ids: ['a', 'extra'], mode: 'enable', weight: 2.5 }),
    stored = readRelationalCommit(f.db, result.commitSeq),
    owned = new Set(['QuotaObserved', 'AvailabilityChanged', 'AccountConfigurationChanged']);
  assert.ok(
    stored.events.some((e) => !owned.has(e.eventType)),
    'must reach real bound financial completeness',
  );
  for (const mode of [
    'target',
    'availability',
    'configuration',
    'financial',
    'duplicate',
    'order',
    'connection',
  ]) {
    await t.test(mode, async (t) => {
      for (const narrow of [false, true]) {
        const { target, owner, inputs } = publicPrefix(t, f, stored.events[0].commitSeq),
          bad = structuredClone(stored);
        if (mode === 'target') {
          bad.events = bad.events.filter((e) => e.accountId !== 'extra');
        } else if (mode === 'availability') {
          bad.events = bad.events.filter((e) => e.eventType !== 'AvailabilityChanged');
        } else if (mode === 'configuration') {
          bad.events = bad.events.filter((e) => e.eventType !== 'AccountConfigurationChanged');
        } else if (mode === 'financial') {
          bad.events = bad.events.filter((e) => owned.has(e.eventType));
        } else if (mode === 'duplicate') {
          bad.events.push({
            ...bad.events.find((e) => e.eventType === 'QuotaObserved'),
            eventId: 'batch-duplicate-observed',
          });
        } else if (mode === 'order') {
          bad.events.reverse();
        } else {
          const observation = bad.events.find(
            (e) => e.eventType === 'QuotaObserved' && e.accountId === 'extra',
          );
          observation.payload.connectionSourceEventId = stored.events.find(
            (e) => e.eventType === 'QuotaObserved' && e.accountId === 'a',
          ).payload.connectionSourceEventId;
        }
        resequence(bad, target);
        const before = databaseState(target),
          account = new RelationalAccountStore(target, new RelationalQuotaStore(target), new Map());
        const replay = (commit) =>
          target.transaction(() =>
            narrow ? account.replayAccountCommit(commit) : owner.replayCommit(commit, inputs),
          )();
        target.transaction(() =>
          assert.throws(
            () => replay(bad),
            mode === 'connection'
              ? { message: '观察代次或序号已经过期', statusCode: 409 }
              : undefined,
          ),
        )();
        assert.equal(databaseState(target), before);
        replay(stored);
        assert.equal(relationalPrivateStateEmpty(target), true);
      }
    });
  }
});

test('账户批次不能把改变weight伪装fresh no-op，合法完整envelope仍需本次真实核验', async (t) => {
  const f = await accountBatchFixture(t);
  await f.run('batch-proof-before-weight', { ids: ['a'], mode: 'enable' });
  const proof = f.quotas.readAccountEnableProof('a', f.clock),
    result = await f.run('batch-real-weight-change', {
      ids: ['a', 'extra'],
      mode: 'enable',
      weight: 2,
    }),
    stored = readRelationalCommit(f.db, result.commitSeq),
    bad = structuredClone(stored),
    intent = bad.command.receipt.accountIntent;
  assert.equal(typeof proof, 'string');
  assert.equal(stored.events.filter((e) => e.eventType === 'QuotaObserved').length, 2);
  intent.outcomes[0] = { freshObservationEventId: proof, availability: null };
  bad.events = bad.events.filter(
    (e) => e.accountId !== 'a' || e.eventType === 'AccountConfigurationChanged',
  );
  Object.assign(bad.command.receipt, { checked: 1, ok: 1, failed: 0 });
  for (const narrow of [false, true]) {
    const { target, owner, inputs } = publicPrefix(t, f, stored.events[0].commitSeq),
      account = new RelationalAccountStore(target, new RelationalQuotaStore(target), new Map()),
      altered = resequence(structuredClone(bad), target),
      before = databaseState(target);
    target.transaction(() =>
      assert.throws(
        () => (narrow ? account.replayAccountCommit(altered) : owner.replayCommit(altered, inputs)),
        /批量 no-op 缺少当前真实有效的原观察/,
      ),
    )();
    assert.equal(databaseState(target), before);
    target.transaction(() => owner.replayCommit(stored, inputs))();
  }
});

test('账户批次fresh no-op不能夹带别的合法金融同步，资金原序切点不能冒本批实际观察', async (t) => {
  const f = await accountBatchFixture(t);
  await f.run('batch-real-proofs', { ids: ['a', 'extra'], mode: 'enable' });
  f.advance(1000);
  const proofs = ['a', 'extra'].map((id) => f.quotas.readAccountEnableProof(id, f.clock));
  assert.ok(proofs.every((id) => typeof id === 'string'));
  const funding = f.db.transaction(() =>
      f.quotas.manageQuotaFunding(
        { ...f.admin('batch-other-sync'), recordedAt: f.clock },
        'a',
        {
          action: 'operating-register-funding',
          receipt: 'batch-unrelated-funding',
          resource: 'anlasFixed',
          amount: 10,
        },
        null,
      ),
    )(),
    stored = readRelationalCommit(f.db, funding.commitSeq),
    firstManual = stored.events.findIndex((e) => e.eventType === 'FundingRegistered'),
    bad = structuredClone(stored);
  assert.ok(firstManual > 0);
  bad.events = bad.events.slice(0, firstManual);
  assert.ok(bad.events.some((e) => e.eventType === 'RefillApplied'));
  bad.command.receipt = {
    accountIds: ['a', 'extra'],
    checked: 0,
    ok: 0,
    failed: 0,
    ignored: 0,
    accountIntent: {
      kind: 'batch',
      ids: ['a', 'extra'],
      mode: 'enable',
      applied: true,
      outcomes: proofs.map((id) => ({ freshObservationEventId: id, availability: null })),
    },
  };
  for (const narrow of [false, true]) {
    const { target, owner, inputs } = publicPrefix(t, f, stored.events[0].commitSeq),
      account = new RelationalAccountStore(target, new RelationalQuotaStore(target), new Map()),
      altered = resequence(structuredClone(bad), target),
      before = databaseState(target);
    target.transaction(() =>
      assert.throws(
        () => (narrow ? account.replayAccountCommit(altered) : owner.replayCommit(altered, inputs)),
        /批量账户原回执、目标或事实范围不符/,
      ),
    )();
    assert.equal(databaseState(target), before);
    target.transaction(() => owner.replayCommit(stored, inputs))();
  }
});

test('实际start授权事务尾错不能作废唯一已发且正等待末次授权的真实GET', async (t) => {
  const f = await accountBatchFixture(t),
    events = new EventEmitter();
  let rawCalls = 0,
    phase = 0,
    release,
    entered;
  const held = new Promise((resolve) => {
      release = resolve;
    }),
    reached = new Promise((resolve) => {
      entered = resolve;
    }),
    refresh = createRelationalAccountQuotaRefresh({
      sqlite: f.db,
      quotas: f.quotas,
      events,
      now: () => f.clock,
      logger: { log() {}, error() {} },
      fetchQuota: async () => {
        rawCalls++;
        return official();
      },
    }),
    authorizeOld = async (run) => {
      if (++phase === 3) {
        entered();
        await held;
      }
      return f.db.transaction(() => run(null))();
    },
    pending = ownerAccountCommand(
      f.accounts,
      authorizeOld,
      { ...f.admin('batch-original-real-query'), recordedAt: f.clock },
      { kind: 'observe', accountId: 'a' },
      refresh.fetch,
      () => f.clock,
    );
  t.after(async () => {
    release();
    await pending.catch(() => {});
    await refresh.stop();
  });
  await reached;
  const before = databaseState(f.db),
    authorizeFault = async (run) =>
      f.db.transaction(() => {
        const result = run(null);
        if ('start' in result) {
          f.db.prepare('SELECT * FROM missing_actual_start_tail').get();
        }
        return result;
      })();
  await assert.rejects(
    adminAccountBatchCommand(
      f.accounts,
      authorizeFault,
      { ...f.admin('batch-no-real-start'), recordedAt: f.clock },
      { ids: ['a', 'extra'], mode: 'observe' },
      refresh.fetch,
      () => f.clock,
    ),
    /no such table: missing_actual_start_tail/,
  );
  assert.equal(rawCalls, 1, 'failed callback never invoked another raw query');
  assert.equal(databaseState(f.db), before);
  release();
  const old = await pending;
  assert.deepEqual(old.receipt, { checked: 1, ok: 1, failed: 0, ignored: 0 });
});

test('实际query在授权SQL返回后真实queue停止，必须在native激活领号与raw之前拒绝', async (t) => {
  const f = await accountBatchFixture(t),
    events = new EventEmitter(),
    runtime = createRelationalQueueRuntime({
      sqlite: f.db,
      dataDir: f.dir,
      inputs: f.inputs,
      generations: f.generations,
      controls: new Map(),
      events,
      env: { OWNER_USER_ID: 'm1' },
    });
  await runtime.start();
  let raw = 0,
    stopped;
  const refresh = createRelationalAccountQuotaRefresh({
    sqlite: f.db,
    quotas: f.quotas,
    events,
    runtime,
    now: () => f.clock,
    logger: { log() {}, error() {} },
    fetchQuota: async () => {
      raw++;
      return official();
    },
  });
  t.after(async () => {
    await runtime.stop();
    await refresh.stop();
    await refresh.pending;
  });
  const before = databaseState(f.db),
    floor = BigInt(f.quotas.readAccountQuota('a').observedSequence),
    authorize = async (run) => {
      const result = f.db.transaction(() => run(null))();
      if ('start' in result) {
        stopped = runtime.stop();
      }
      return result;
    };
  let error;
  try {
    await ownerAccountCommand(
      f.accounts,
      authorize,
      { ...f.admin('runtime-stopped-start'), recordedAt: f.clock },
      { kind: 'observe', accountId: 'a' },
      refresh.fetch,
      () => f.clock,
    );
  } catch (value) {
    error = value;
  }
  await stopped;
  assert.equal(
    raw,
    0,
    'a ready database cannot authorize new quota IO after the actual queue stops',
  );
  assert.equal(error?.statusCode, 503);
  assert.equal(databaseState(f.db), before);
  assert.equal(
    f.quotas.beginQuotaObservation('a', f.clock).sequence,
    String(floor + 1n),
    'no phantom sequence before the only actual start',
  );
});

test('实际start闭包SQL外一次激活，过期owner与ready和跨owner原准备均不能领号，late prior零IO', async (t) => {
  for (const mode of ['sql-once', 'owner-epoch', 'ready', 'foreign', 'late-prior']) {
    await t.test(mode, async (t) => {
      const f = await accountBatchFixture(t),
        input = { kind: 'observe', accountId: 'a' },
        metadata = { ...f.admin('native-' + mode), recordedAt: f.clock };
      if (mode === 'foreign') {
        const registration = {
            kind: 'register',
            accountId: 'foreign-prepared',
            displayName: 'foreign',
            token: 'pst-foreign-prepared',
            configVersion: 1,
            bind: false,
            enabled: true,
          },
          prepared = f.db.transaction(() =>
            f.accounts.prepareAccountCommand(metadata, registration, null),
          )().prepared,
          own = f.db.transaction(() =>
            f.accounts.startAccountEnable(metadata, registration, prepared, null),
          )(),
          foreign = new RelationalAccountStore(f.db, f.quotas, new Map(), { OWNER_USER_ID: 'm1' }),
          other = f.db.transaction(() =>
            foreign.startAccountEnable(metadata, registration, prepared, null),
          )(),
          before = databaseState(f.db);
        assert.throws(() => other.start(), /新账户准备/);
        f.accounts.releaseAccountRegistration(prepared);
        assert.throws(() => own.start(), /新账户准备/);
        assert.equal(databaseState(f.db), before);
        return;
      }
      const ownerMetadata = mode === 'owner-epoch' ? meta('native-owner', f.clock, 'm1') : metadata,
        authorize =
          mode === 'owner-epoch'
            ? (run) =>
                withAuthenticatedMemberApiKey(f.members, keys.m1, run, { OWNER_USER_ID: 'm1' })
            : f.authorize,
        prepared = await authorize((actor) =>
          f.accounts.prepareAccountCommand(ownerMetadata, input, actor),
        ),
        cap = await authorize((actor) =>
          f.accounts.startAccountEnable(ownerMetadata, input, prepared.prepared, actor),
        );
      assert.equal(typeof cap.start, 'function');
      if (mode === 'sql-once') {
        const before = databaseState(f.db),
          floor = BigInt(f.quotas.readAccountQuota('a').observedSequence);
        f.db.transaction(() => assert.throws(() => cap.start(), /授权事务成功结束后/))();
        assert.equal(databaseState(f.db), before);
        assert.equal(cap.start().ticket.sequence, String(floor + 1n));
        assert.throws(() => cap.start(), /只能开始一次/);
      } else if (mode === 'owner-epoch') {
        f.db.transaction(() =>
          f.members.setMemberStatus(f.admin('native-disable-owner'), 'm1', 'disabled', null),
        )();
        const before = databaseState(f.db);
        assert.throws(() => cap.start(), { statusCode: 403 });
        assert.equal(databaseState(f.db), before);
      } else if (mode === 'ready') {
        f.db.prepare("UPDATE projection_state SET status='building'").run();
        const before = databaseState(f.db);
        assert.throws(() => cap.start(), /Relational projection is incomplete/);
        assert.equal(databaseState(f.db), before);
      } else {
        const winner = await ownerAccountCommand(
            f.accounts,
            f.authorize,
            metadata,
            input,
            f.query(),
            () => f.clock,
          ),
          before = databaseState(f.db),
          changes = f.db.prepare('SELECT total_changes() n').get().n,
          prior = cap.start();
        assert.equal(prior.result.replayed, true);
        assert.deepEqual(prior.result.receipt, winner.receipt);
        assert.equal(f.calls.length, 1);
        assert.equal(databaseState(f.db), before);
        assert.equal(f.db.prepare('SELECT total_changes() n').get().n, changes);
      }
    });
  }
});

test('实际query自动观察同样在最终真实runtime gate之后才领号，不把停机拒绝当官方失败', async (t) => {
  const f = await accountBatchFixture(t),
    events = new EventEmitter(),
    runtime = createRelationalQueueRuntime({
      sqlite: f.db,
      dataDir: f.dir,
      inputs: f.inputs,
      generations: f.generations,
      controls: new Map(),
      events,
      env: { OWNER_USER_ID: 'm1' },
    });
  await runtime.start();
  const original = runtime.assertAccepting;
  let raw = 0,
    stopping;
  t.mock.method(runtime, 'assertAccepting', () => {
    stopping = runtime.stop();
    return original();
  });
  const refresh = createRelationalAccountQuotaRefresh({
    sqlite: f.db,
    quotas: f.quotas,
    events,
    runtime,
    now: () => f.clock,
    logger: { log() {}, error() {} },
    fetchQuota: async () => {
      raw++;
      return official();
    },
  });
  t.after(async () => {
    await runtime.stop();
    await refresh.stop();
    await refresh.pending;
  });
  const before = databaseState(f.db),
    count = f.db.prepare('SELECT COUNT(*) n FROM commits').get().n,
    floor = BigInt(f.quotas.readAccountQuota('a').observedSequence);
  try {
    await refresh.refreshEnabled();
  } catch (error) {
    assert.equal(error.statusCode, 503);
  }
  await stopping;
  assert.notEqual(runtime.status, 'running');
  assert.equal(raw, 0);
  assert.equal(
    f.db.prepare('SELECT COUNT(*) n FROM commits').get().n,
    count,
    'runtime stop rejection cannot become a fake official observation',
  );
  assert.equal(databaseState(f.db), before);
  assert.equal(f.quotas.beginQuotaObservation('a', f.clock).sequence, String(floor + 1n));
});

test('实际query授权等待已超截止的cap不得迟到激活，raw0且不隐藏消耗中央序号', async (t) => {
  const f = await accountBatchFixture(t);
  let raw = 0,
    entered,
    release;
  const reached = new Promise((resolve) => {
      entered = resolve;
    }),
    held = new Promise((resolve) => {
      release = resolve;
    }),
    query = createAccountQuotaQuery({
      now: () => f.clock,
      requestTimeoutMs: 50,
      logger: { log() {} },
      fetchQuota: async () => {
        raw++;
        return official();
      },
    }),
    authorize = async (run) => {
      const result = f.db.transaction(() => run(null))();
      if ('start' in result) {
        entered();
        await held;
      }
      return result;
    },
    before = databaseState(f.db),
    floor = BigInt(f.quotas.readAccountQuota('a').observedSequence),
    pending = ownerAccountCommand(
      f.accounts,
      authorize,
      { ...f.admin('expired-before-activation'), recordedAt: f.clock },
      { kind: 'observe', accountId: 'a' },
      query,
      () => f.clock,
    );
  await reached;
  await assert.rejects(pending, { code: 'QUOTA_REQUEST_TIMEOUT' });
  release();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(raw, 0);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.quotas.beginQuotaObservation('a', f.clock).sequence, String(floor + 1n));
});
