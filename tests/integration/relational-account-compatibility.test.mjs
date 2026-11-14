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
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { relationalAccountConnectionSource } from '../../server/storage/relational-account-connection.ts';
import {
  relationalPrivateStateEmpty,
  relationalSnapshotSha256,
  restoreRelationalPrivateSnapshot,
} from '../../server/storage/relational-private-restore.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { accountQuotaResult } from '../../server/quota/account-refresh.ts';
import { ownerAccountCommand } from '../../server/members/owner-operations.ts';
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
function setup(f, controls = new Map()) {
  const quotas = new RelationalQuotaStore(f.db),
    accounts = new RelationalAccountStore(f.db, quotas, controls),
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
    await start();
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
      await start();
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
          assert.match(error.message, /派发与真实原预扣|观察代次或序号|凭据失效决定必须/);
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
  const admit = createRelationalMemberGenerationAdmission({
    sqlite: f.db,
    members: f.members,
    inputs: f.inputs,
    generations: f.generations,
    payloadBudget: createGenerationPayloadBudget(() => f.inputs.activePayloadBytes()),
    env,
    now: () => clock,
  });
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
