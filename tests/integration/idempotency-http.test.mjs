import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { rename } from 'node:fs/promises';
import path from 'node:path';
import Database from 'better-sqlite3';
import { nativeImageMsgpack, nativeImagesMsgpack } from '../../server/protocols/native-stream.ts';
import { resultFile } from '../../server/storage/result-files.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import { childFixture, launchGateway, enableAccount, query } from '../helpers/relational-child.mjs';

const model = 'nai-diffusion-4-5-full';
const png = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=',
  'base64',
);
async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}
async function until(predicate, timeout = 5000) {
  const deadline = Date.now() + timeout;
  while (!predicate()) {
    if (Date.now() >= deadline) {
      throw new Error('synthetic idempotency wait timed out');
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

test(
  'HTTP 幂等跨并发、断线、重启、首次 context 和 Key 轮换保留同一任务，丢失结果不重生',
  { timeout: 60000 },
  async (t) => {
    const fixture = await childFixture(t, 'idempotency-http'),
      { dataDir, members } = fixture,
      adminToken = fixture.env.ADMIN_TOKEN;
    const jobs = () =>
      query(
        fixture,
        `SELECT j.*,r.id AS requestId,r.member_id AS memberId,r.source,
      j.artifact_id AS imageId FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id ORDER BY j.queue_seq`,
      );
    const withOfflineOwners = async (mutate) => {
      assert.ok(!server || server.exit, 'offline commands must not race the actual child writer');
      const db = new Database(fixture.filename);
      db.pragma('foreign_keys=ON');
      try {
        return await mutate(
          db,
          new RelationalMemberStore(db, fixture.env),
          new RelationalGenerationStore(db, fixture.env),
        );
      } finally {
        db.close();
      }
    };
    const calls = [];
    let previewGate, releaseSetup, releaseWeb;
    const setupGate = new Promise((resolve) => (releaseSetup = resolve)),
      webGate = new Promise((resolve) => (releaseWeb = resolve));
    const upstream = http.createServer(async (req, res) => {
      if (req.url === '/user/data') {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(
          JSON.stringify({
            subscription: {
              tier: 3,
              active: true,
              expiresAt: Math.floor(Date.now() / 1000) + 86400,
              trainingStepsLeft: { fixedTrainingStepsLeft: 10000, purchasedTrainingSteps: 0 },
              usage: { percent: 100, isNegative: false, timeUntilNextPercent: 0 },
            },
          }),
        );
        return;
      }
      const chunks = [];
      for await (const chunk of req) {
        chunks.push(chunk);
      }
      calls.push({ path: req.url, body: JSON.parse(Buffer.concat(chunks)) });
      if (calls.length === 1) await setupGate;
      if (calls.length === 2) await webGate;
      if (previewGate) {
        assert.equal(
          req.url,
          '/ai/generate-image-stream',
          'member preview must use the actual upstream MessagePack path',
        );
        res.writeHead(200, { 'content-type': 'application/octet-stream' });
        res.write(
          nativeImageMsgpack({ event_type: 'intermediate', samp_ix: 0, image: png, step_ix: 2 }),
        );
        await previewGate.promise;
        res.end(nativeImagesMsgpack([png]));
        return;
      }
      res.writeHead(200, {
        'content-type': req.url.endsWith('-stream') ? 'application/octet-stream' : 'image/png',
      });
      res.end(req.url.endsWith('-stream') ? nativeImagesMsgpack([png]) : png);
    });
    const upstreamPort = await listen(upstream);
    let server, base;
    const start = async () => {
      server = await launchGateway(fixture, `http://127.0.0.1:${upstreamPort}`, {
        DIRECT_GENERATE_TIMEOUT_MS: '100',
        OPENAI_CHAT_TIMEOUT_MS: '500',
      });
      base = server.origin;
    };
    const stop = async (signal) => (signal === 'SIGKILL' ? server.kill() : server.stop());
    t.after(async () => {
      releaseSetup();
      releaseWeb();
      previewGate?.release();
      upstream.closeAllConnections();
      await new Promise((resolve) => upstream.close(resolve));
    });
    const call = (endpoint, { body, key, member = 0, token = members[member].api_key } = {}) =>
      fetch(base + endpoint, {
        method: body === undefined ? 'GET' : 'POST',
        signal: AbortSignal.timeout(5000),
        headers: {
          authorization: `Bearer ${token}`,
          ...(key === undefined ? {} : { 'Idempotency-Key': key }),
          ...(body === undefined ? {} : { 'content-type': 'application/json' }),
        },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    const cancel = async (id, member = 0) => {
      const response = await call(`/api/requests/${id}/cancel`, { body: {}, member });
      const result = await response.json();
      assert.equal(response.status, 200, JSON.stringify(result));
      return result;
    };
    await start();
    await enableAccount(fixture, server);
    for (const key of [undefined, 'null-input']) {
      assert.equal((await call('/ai/generate-image', { body: null, key })).status, 400);
    }
    assert.equal(jobs().length, 0);
    // A real fourth-member execution occupies the sole physical slot; no cooldown/projection seed.
    const setupResponse = await call('/api/jobs', {
      member: 3,
      body: {
        prompt: 'physical setup gate',
        model,
        width: 128,
        height: 128,
        steps: 23,
        seed: 123,
        nocache: true,
      },
    });
    assert.equal(setupResponse.status, 202);
    const setup = await setupResponse.json();
    await until(() => calls.length === 1);
    const webBody = {
      prompt: 'stable random web operation',
      model,
      width: 1024,
      height: 1024,
      steps: 23,
    };
    const webResponses = await Promise.all(
      Array.from({ length: 3 }, () => call('/api/jobs', { body: webBody, key: 'web-operation' })),
    );
    assert.ok(webResponses.every((response) => response.status === 202));
    const accepted = await Promise.all(webResponses.map((response) => response.json())),
      first = accepted[0];
    assert.ok(accepted.every((job) => job.id === first.id && job.requestId === first.requestId));
    assert.equal(jobs().length, 2);
    const acceptedFact = query(
      fixture,
      `SELECT c.receipt,c.actor_id,c.actor_kind,e.correlation_id FROM domain_events e
    JOIN commits c ON c.commit_seq = e.commit_seq WHERE e.event_type = 'RequestAccepted' AND json_extract(e.payload, '$.requestId') = ?`,
      first.requestId,
    )[0];
    assert.deepEqual(JSON.parse(acceptedFact.receipt), {
      requestId: first.requestId,
      jobIds: [first.id],
    });
    assert.equal(acceptedFact.actor_id, members[0].user_id);
    assert.equal(acceptedFact.actor_kind, 'member');
    assert.equal(
      acceptedFact.correlation_id,
      null,
      'Direct acceptance has no gateway diagnostic ID',
    );
    const alias = await call('/api/jobs', { body: webBody, key: 'web-cache-alias' });
    assert.equal(alias.status, 202);
    assert.equal((await alias.json()).id, first.id);
    assert.equal(jobs().length, 2);
    assert.equal(
      (
        await call('/api/jobs', {
          body: { ...webBody, prompt: 'changed intent' },
          key: 'web-operation',
        })
      ).status,
      409,
    );
    const otherMember = await call('/api/jobs', { body: webBody, key: 'web-operation', member: 1 });
    assert.equal(otherMember.status, 202);
    const other = await otherMember.json();
    assert.notEqual(other.id, first.id);
    await cancel(other.requestId, 1);
    const savedInput = () =>
        query(
          fixture,
          `SELECT r.input_id,r.context_id,i.body,c.body AS context
      FROM generation_requests r JOIN generation_inputs i ON i.id=r.input_id
      JOIN normalization_contexts c ON c.id=r.context_id WHERE r.id=?`,
          first.requestId,
        )[0],
      initialPayload = savedInput(),
      initialSeed = jobs().find((job) => job.id === first.id).actual_seed;
    releaseSetup();
    await until(() => jobs().find((job) => job.id === setup.id)?.status === 'done');
    await stop('SIGKILL');
    await start();
    const afterRestart = await call('/api/jobs', { body: webBody, key: 'web-operation' });
    assert.equal(afterRestart.status, 202);
    assert.equal((await afterRestart.json()).id, first.id);
    assert.deepEqual(
      savedInput(),
      initialPayload,
      'first input/context bytes survive actual child restart',
    );
    assert.equal(jobs().length, 3);

    // Native 两种响应形态共享同一次操作；一次受理两张后，重复提交不能再占两张容量。
    const nativeBody = {
      input: 'native random retry',
      model,
      parameters: { width: 1024, height: 1024, steps: 23, seed: -1, n_samples: 2 },
    };
    const nativeFirst = await call('/ai/generate-image', {
      body: nativeBody,
      key: 'native-operation',
      member: 1,
    });
    assert.equal(nativeFirst.status, 504);
    const nativeId = nativeFirst.headers.get('x-nai-request-id');
    const nativeAgain = await call('/v1/ai/generate-image-stream', {
      body: nativeBody,
      key: 'native-operation',
      member: 1,
    });
    assert.equal(nativeAgain.status, 504);
    assert.equal(nativeAgain.headers.get('x-nai-request-id'), nativeId);
    assert.equal(jobs().filter((job) => job.requestId === nativeId).length, 2);
    assert.equal(
      (
        await call('/ai/generate-image', {
          body: { ...nativeBody, parameters: { ...nativeBody.parameters, n_samples: 3 } },
          key: 'native-operation',
          member: 1,
        })
      ).status,
      409,
    );
    assert.equal((await cancel(nativeId, 1)).cancelledCount, 2);
    const failedReplay = await call('/ai/generate-image', {
      body: nativeBody,
      key: 'native-operation',
      member: 1,
    });
    assert.equal(failedReplay.status, 409);
    assert.equal(failedReplay.headers.get('x-nai-request-id'), nativeId);

    for (const [operation, body] of [
      ['encode-vibe', { model, image: png.toString('base64'), information_extracted: 1 }],
      ['upscale', { image: png.toString('base64') }],
    ]) {
      const a = await call(`/ai/${operation}`, {
        body,
        key: 'same-key-different-operation',
        member: 1,
      });
      const b = await call(`/ai/${operation}`, {
        body,
        key: 'same-key-different-operation',
        member: 1,
      });
      assert.equal(a.status, 504);
      assert.equal(b.status, 504);
      assert.ok(a.headers.get('x-nai-request-id'));
      assert.equal(a.headers.get('x-nai-request-id'), b.headers.get('x-nai-request-id'));
      await cancel(a.headers.get('x-nai-request-id'), 1);
    }

    const direct =
      '/generate?' +
      new URLSearchParams({
        token: members[1].api_key,
        prompt: 'direct retry',
        model,
        seed: '29',
        nocache: '1',
      });
    const directA = await call(direct, { key: 'direct-operation', member: 1 }),
      directB = await call(direct, { key: 'direct-operation', member: 1 });
    assert.equal(directA.status, 200);
    assert.equal(directB.status, 200);
    assert.equal(directA.headers.get('x-error'), '1');
    assert.equal(directB.headers.get('x-error'), '1');
    assert.ok(directA.headers.get('x-nai-request-id'));
    assert.equal(directA.headers.get('x-nai-request-id'), directB.headers.get('x-nai-request-id'));
    await cancel(directA.headers.get('x-nai-request-id'), 1);

    const chat = {
      model: model + ':k_euler_ancestral',
      messages: [
        {
          role: 'user',
          content:
            '提示词: chat retry\n画师串:\n尺寸: 方图\n提示词引导值: 5\n缩放引导值: 0\n负面提示词:',
        },
      ],
    };
    const chatA = await call('/v1/chat/completions', {
      body: chat,
      key: 'chat-operation',
      member: 1,
    });
    const chatB = await call('/v1/chat/completions', {
      body: { ...chat, stream: true },
      key: 'chat-operation',
      member: 1,
    });
    assert.equal(chatA.status, 504);
    assert.equal(chatB.status, 200);
    assert.ok(chatA.headers.get('x-nai-request-id'));
    assert.equal(chatA.headers.get('x-nai-request-id'), chatB.headers.get('x-nai-request-id'));
    assert.match(await chatB.text(), /data: \[DONE\]/);
    await cancel(chatA.headers.get('x-nai-request-id'), 1);
    assert.ok(calls.length <= 2, 'cancelled requests never escape the actual occupied slot');

    // 在受理提交后直接断开连接，再用同一 key 找回；不能依赖第一次 HTTP 回包。
    const beforeDisconnect = jobs().length;
    const disconnectedBody = {
      ...nativeBody,
      input: 'accepted before connection close',
      parameters: { ...nativeBody.parameters, n_samples: 1 },
    };
    let responded = false;
    const disconnected = http.request(
      base + '/ai/generate-image',
      {
        method: 'POST',
        headers: {
          authorization: `Bearer ${members[1].api_key}`,
          'content-type': 'application/json',
          'Idempotency-Key': 'disconnected-operation',
        },
      },
      (response) => {
        responded = true;
        response.resume();
      },
    );
    disconnected.on('error', () => {});
    disconnected.end(JSON.stringify(disconnectedBody));
    await until(() => jobs().length === beforeDisconnect + 1);
    disconnected.destroy();
    assert.equal(responded, false);
    const recovered = await call('/ai/generate-image', {
      body: disconnectedBody,
      key: 'disconnected-operation',
      member: 1,
    });
    assert.equal(recovered.status, 504);
    assert.equal(jobs().length, beforeDisconnect + 1);
    await cancel(recovered.headers.get('x-nai-request-id'), 1);

    // Honor the original real scheduler gap; the 5s completion assertion starts once physical IO begins.
    const cooldown = query(
      fixture,
      'SELECT cooldown_until FROM upstream_accounts WHERE id=?',
      fixture.accountId,
    )[0].cooldown_until;
    if (cooldown > Date.now())
      await new Promise((resolve) => setTimeout(resolve, cooldown - Date.now()));
    await until(() => calls.length === 2);
    releaseWeb();
    await until(() => jobs().find((job) => job.id === first.id)?.status === 'done');
    assert.equal(calls.length, 2);
    assert.equal(calls[1].body.parameters.seed, initialSeed);
    const done = await call('/api/jobs', { body: webBody, key: 'web-operation' });
    const result = await done.json();
    assert.equal(done.status, 202, JSON.stringify(result));
    assert.equal(result.id, first.id);
    assert.equal(result.status, 'done');
    const image = await fetch(base + result.imageUrl);
    assert.equal(image.status, 200);
    assert.deepEqual(Buffer.from(await image.arrayBuffer()), png);
    const cachedBody = { ...webBody };
    const cached = await call('/api/jobs', { body: cachedBody, key: 'completed-cache-operation' });
    const cachedJob = await cached.json();
    assert.equal(cached.status, 202, JSON.stringify(cachedJob));
    assert.equal(cachedJob.status, 'done');
    assert.equal(cachedJob.imageId, result.imageId);
    assert.notEqual(cachedJob.id, first.id);
    const cachedAgain = await call('/api/jobs', {
      body: cachedBody,
      key: 'completed-cache-operation',
    });
    assert.equal(cachedAgain.status, 202);
    assert.equal((await cachedAgain.json()).id, cachedJob.id);
    assert.equal(calls.length, 2);
    const policy = await fetch(base + '/api/admin/member-management', {
      method: 'POST',
      headers: { 'content-type': 'application/json', origin: base, 'x-admin-token': adminToken },
      body: JSON.stringify({
        action: 'free-image-policy',
        expectedVersion: query(
          fixture,
          'SELECT free_policy_version FROM sharing_policy WHERE id=1',
        )[0].free_policy_version,
        policy: { enabled: true, warningThreshold: 1, hardLimit: 1 },
      }),
    });
    assert.equal(policy.status, 200, await policy.text());
    const quota = await (await call('/api/me/quota')).json();
    assert.equal(quota.fairQuota.freeImages.successful, 1);
    assert.equal(quota.fairQuota.freeImages.warning, true);
    for (const [endpoint, body] of [
      ['/api/jobs', { ...webBody, prompt: 'blocked by free limit', nocache: true }],
      ['/ai/generate-image', disconnectedBody],
      ['/ai/generate-image-stream', disconnectedBody],
      ['/v1/chat/completions', chat],
    ]) {
      const rejected = await call(endpoint, { body, key: 'limited-' + endpoint });
      assert.equal(rejected.status, 429, endpoint);
      assert.ok(Number(rejected.headers.get('retry-after')) > 0, endpoint);
    }
    const limitedDirect = await call(
      '/generate?' +
        new URLSearchParams({
          token: members[0].api_key,
          prompt: 'limited direct',
          model,
          seed: '29',
          nocache: '1',
        }),
    );
    assert.equal(limitedDirect.headers.get('x-error'), '1');
    assert.ok(Number(limitedDirect.headers.get('retry-after')) > 0);
    const cacheAtLimit = await call('/api/jobs', { body: cachedBody, key: 'cached-at-free-limit' });
    assert.equal(cacheAtLimit.status, 202);
    assert.equal((await cacheAtLimit.json()).status, 'done');
    assert.equal(calls.length, 2);
    const settledWallets = query(fixture, 'SELECT * FROM wallets ORDER BY account_id,member_id'),
      settledCharges = query(fixture, 'SELECT * FROM charges ORDER BY original_job_id');
    assert.equal(settledCharges.length, 2);
    assert.ok(
      settledCharges.every(
        (charge) => charge.status === 'settled' && charge.original_account_id === fixture.accountId,
      ),
    );
    const replacementKey = 'STA1N-synthetic-idempotency-rotated-key';
    await stop('SIGKILL');
    await withOfflineOwners(async (_db, memberStore) => {
      assert.equal(
        await withAuthenticatedMemberApiKey(memberStore, members[0].api_key, (member) => {
          assert.equal(member.id, members[0].user_id);
          memberStore.replaceMemberApiKey(member.id, replacementKey);
          return true;
        }),
        true,
      );
    });
    await start();
    assert.equal((await call('/api/jobs', { body: webBody, key: 'web-operation' })).status, 401);
    const admission = await fetch(base + '/api/admin/member-management', {
      method: 'POST',
      headers: { origin: base, 'content-type': 'application/json', 'x-admin-token': adminToken },
      body: JSON.stringify({
        action: 'member-admission',
        userId: members[0].user_id,
        acceptingJobs: false,
      }),
    });
    assert.equal(admission.status, 200, await admission.text());
    const paused = await fetch(base + `/api/admin/accounts/${fixture.accountId}`, {
      method: 'PATCH',
      headers: { origin: base, 'content-type': 'application/json', 'x-admin-token': adminToken },
      body: JSON.stringify({ enabled: false }),
    });
    assert.equal(paused.status, 200, await paused.text());
    const rotated = await call('/api/jobs', {
      body: webBody,
      key: 'web-operation',
      token: replacementKey,
    });
    assert.equal(rotated.status, 202);
    assert.equal((await rotated.json()).id, first.id);
    assert.deepEqual(
      query(fixture, 'SELECT * FROM wallets ORDER BY account_id,member_id'),
      settledWallets,
    );
    assert.deepEqual(
      query(fixture, 'SELECT * FROM charges ORDER BY original_job_id'),
      settledCharges,
    );
    assert.ok(
      query(fixture, 'SELECT admission_epoch FROM members WHERE id=?', members[0].user_id)[0]
        .admission_epoch > jobs().find((job) => job.id === first.id).admission_epoch,
    );
    assert.equal(calls.length, 2);
    await stop('SIGKILL');
    const stored = query(
      fixture,
      'SELECT id AS imageId,operation,mime_type AS mimeType FROM artifacts WHERE id=?',
      result.imageId,
    )[0];
    const artifact = path.resolve(dataDir, resultFile(stored));
    assert.ok(artifact.startsWith(path.join(dataDir, 'images') + path.sep));
    await rename(artifact, artifact + '.missing-for-test');
    await start();
    assert.equal(
      (await call('/api/jobs', { body: webBody, key: 'web-operation', token: replacementKey }))
        .status,
      410,
    );
    assert.equal(calls.length, 2);
    await stop('SIGKILL');
    await withOfflineOwners((db, _members, generations) =>
      db
        .transaction(() =>
          generations.retireJobHistory(
            {
              commandId: 'synthetic-idempotency-retire',
              actorKind: 'admin',
              actorId: 'local-admin',
              recordedAt: Date.now(),
              correlationId: null,
              causationId: null,
              policyVersion: 'history-retirement-v1',
              pricingVersion: 'none',
            },
            [first.id],
            null,
            new Map(),
          ),
        )
        .immediate(),
    );
    await start();
    assert.equal(
      (await call('/api/jobs', { body: webBody, key: 'web-operation', token: replacementKey }))
        .status,
      410,
    );
    assert.equal(calls.length, 2);

    // The same process also serves the Cookie contract while events own every business write.
    await stop('SIGKILL');
    await start();
    await enableAccount(fixture, server);
    const login = await fetch(base + '/api/member/login', {
      method: 'POST',
      headers: { origin: base, 'content-type': 'application/json' },
      body: JSON.stringify({ username: members[2].username, password: members[2].password }),
    });
    assert.equal(login.status, 200);
    const cookie = login.headers.get('set-cookie').split(';')[0];
    const memberCall = (endpoint, body, key) =>
      fetch(base + endpoint, {
        method: body === undefined ? 'GET' : 'POST',
        signal: AbortSignal.timeout(10000),
        headers: {
          cookie,
          origin: base,
          ...(body === undefined ? {} : { 'content-type': 'application/json' }),
          ...(key ? { 'Idempotency-Key': key } : {}),
        },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    const memberInput = {
      operation: 'generate',
      input: 'cookie authority preview',
      model,
      action: 'generate',
      parameters: { width: 1024, height: 1024, steps: 23, seed: 987, n_samples: 1 },
    };
    const priceResponse = await memberCall('/api/member/generation/quote', memberInput),
      price = await priceResponse.json();
    assert.equal(priceResponse.status, 200, JSON.stringify(price));
    assert.equal(price.admission.allowed, true);
    const confirmation = {
      requestFingerprint: price.requestFingerprint,
      versions: price.versions,
      maxPerJob: price.charges.perJob[0].resources,
      maxTotal: price.charges.total,
    };
    previewGate = {};
    previewGate.promise = new Promise((resolve) => {
      previewGate.release = resolve;
    });
    const submitResponse = await memberCall(
        '/api/member/generation/requests',
        { ...memberInput, confirmation },
        'cookie-authority',
      ),
      receipt = await submitResponse.json();
    assert.equal(submitResponse.status, 202, JSON.stringify(receipt));
    assert.equal(receipt.replayed, false);
    const eventsResponse = await memberCall(receipt.links.events);
    assert.equal(eventsResponse.status, 200);
    const reader = eventsResponse.body.getReader(),
      decoder = new TextDecoder(),
      events = [];
    let buffered = '';
    for (;;) {
      const next = await reader.read();
      if (next.done) {
        break;
      }
      buffered += decoder.decode(next.value, { stream: true });
      let boundary;
      while ((boundary = buffered.indexOf('\n\n')) >= 0) {
        const frame = buffered.slice(0, boundary);
        buffered = buffered.slice(boundary + 2);
        const type = frame.match(/^event: ([^\n]+)/)?.[1],
          data = frame.match(/\ndata: ([^\n]+)/)?.[1];
        if (!type || !data) {
          continue;
        }
        const event = { type, value: JSON.parse(data) };
        events.push(event);
        if (type === 'preview') {
          assert.deepEqual(Buffer.from(event.value.preview.split(',')[1], 'base64'), png);
          previewGate.release();
        }
      }
    }
    assert.equal(events[0].type, 'snapshot');
    assert.ok(events.some((event) => event.type === 'preview'));
    assert.equal(events.at(-1).type, 'complete');
    assert.equal(events.at(-1).value.status, 'done');
    const content = await memberCall(`/api/member/jobs/${receipt.jobIds[0]}/content`);
    assert.equal(content.status, 200);
    assert.deepEqual(Buffer.from(await content.arrayBuffer()), png);
    assert.equal(calls.length, 3);
    assert.deepEqual(savedInput(), initialPayload);
    const charges = query(fixture, 'SELECT * FROM charges ORDER BY original_job_id');
    assert.equal(
      charges.length,
      3,
      'cancelled queued requests and cached aliases never reserve money',
    );
    assert.ok(
      charges.every(
        (charge) => charge.status === 'settled' && charge.original_account_id === fixture.accountId,
      ),
    );
    assert.equal(query(fixture, 'SELECT count(*) AS n FROM generation_attempts')[0].n, 3);
    await stop();

    // Public complete-commit replay has no private credentials and performs no IO.
    const source = new Database(fixture.filename, { readonly: true }),
      rebuilt = new Database(path.join(dataDir, 'rebuilt.sqlite'));
    const actualFetch = globalThis.fetch;
    let replayFetches = 0;
    globalThis.fetch = () => {
      replayFetches++;
      throw new Error('public replay denied network before IO');
    };
    try {
      const history = source
        .prepare('SELECT history_id FROM projection_state WHERE id=1')
        .get().history_id;
      initializeRelationalStorage(rebuilt, history);
      const inputs = new RelationalInputStore(source),
        replayer = new RelationalCommitReplayer(rebuilt);
      for (const { seq } of source
        .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
        .all()) {
        rebuilt.transaction(() =>
          replayer.replayCommit(readRelationalCommit(source, seq), inputs),
        )();
      }
      assert.equal(rebuilt.pragma('integrity_check', { simple: true }), 'ok');
      for (const [table, order] of [
        ['commits', 'commit_seq'],
        ['domain_events', 'commit_seq,event_index'],
        ['idempotency_keys', 'original_member_id,operation,key_hash'],
        ['wallets', 'account_id,member_id'],
        ['charges', 'original_job_id'],
        ['account_quota_state', 'account_id'],
        ['generation_jobs', 'id'],
        ['generation_attempts', 'id'],
        ['generation_requests', 'id'],
        ['generation_inputs', 'id'],
      ]) {
        const rows = (db) => db.prepare(`SELECT * FROM ${table} ORDER BY ${order}`).all();
        assert.deepEqual(rows(rebuilt), rows(source), table);
      }
      for (const table of [
        'api_keys',
        'password_credentials',
        'upstream_credentials',
        'member_sessions',
      ]) {
        assert.equal(rebuilt.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0, table);
      }
      assert.ok(
        rebuilt
          .prepare('SELECT auth_epoch FROM members')
          .all()
          .every((row) => row.auth_epoch === null),
      );
    } finally {
      globalThis.fetch = actualFetch;
      source.close();
      rebuilt.close();
    }
    assert.equal(replayFetches, 0);
    assert.equal(calls.length, 3, 'offline replay cannot generate or contact the mock upstream');
  },
);
