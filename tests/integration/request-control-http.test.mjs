import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import Database from 'better-sqlite3';
import { nativeImagesMsgpack } from '../../server/protocols/native-stream.ts';
import {
  childFixture,
  launchGateway,
  enableAccount,
  query,
  assertChildPublicReplay,
} from '../helpers/relational-child.mjs';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';

const png = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=',
  'base64',
);
async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}

test(
  '请求 API 的 Bearer/Cookie 跨秒同归属查询、CSRF 取消、真实签名领取与受理请求 ID',
  { timeout: 20000 },
  async (t) => {
    const fixture = await childFixture(t, 'request-api'),
      credentials = fixture.members;
    const newKey = 'STA1N-synthetic-replacement-key';
    const waitFor = async (predicate) => {
      const deadline = Date.now() + 5000;
      while (!predicate()) {
        assert.ok(Date.now() < deadline, 'actual request fixture did not settle');
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
    };
    let generationCalls = 0;
    const upstream = http.createServer(async (req, res) => {
      if (req.url !== '/user/data') {
        for await (const chunk of req) {
        }
        generationCalls++;
        res.writeHead(200, {
          'content-type': req.url.endsWith('-stream') ? 'application/octet-stream' : 'image/png',
        });
        res.end(req.url.endsWith('-stream') ? nativeImagesMsgpack([png]) : png);
        return;
      }
      res.setHeader('content-type', 'application/json');
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
    });
    const upstreamPort = await listen(upstream);
    // Trace only protected content selects, not harmless input byte-count metadata.
    const trace = `
      import Database from ${JSON.stringify(import.meta.resolve('better-sqlite3'))};
      const prepare = Database.prototype.prepare;
      let armed = false, reads = [];
      Database.prototype.prepare = function(sql) {
        if (armed && /\\bselect\\b/i.test(sql) && /generation_inputs|input_assets|normalization_contexts/i.test(sql)
          && /\\bbody\\b|\\bbytes\\b|select\\s+\\*/i.test(sql)) reads.push(sql);
        return prepare.call(this, sql);
      };
      process.on('message', message => {
        if (message === 'trace-prove') {
          reads = []; armed = true;
          const db = new Database(process.env.DATA_DIR + '/library.sqlite', {readonly:true});
          try { db.prepare('SELECT body FROM generation_inputs LIMIT 1').get(); } finally { db.close(); }
          armed = false; process.send({trace:'proved', count:reads.length}); reads = [];
        }
        if (message === 'trace-begin') { reads = []; armed = true; process.send({trace:'begun'}); }
        if (message === 'trace-end') { armed = false; process.send({trace:'ended', reads}); }
      });
    `;
    let server, base;
    const start = async () => {
      server = await launchGateway(fixture, `http://127.0.0.1:${upstreamPort}`, {
        OPENAI_CHAT_TIMEOUT_MS: '100',
        NODE_OPTIONS: `--import=data:text/javascript,${encodeURIComponent(trace)}`,
      });
      base = server.origin;
    };
    t.after(async () => {
      upstream.closeAllConnections();
      await new Promise((resolve) => upstream.close(resolve));
    });
    const traceMessage = async (message) => {
      const reply = once(server.child, 'message');
      server.child.send(message);
      return (await reply)[0];
    };
    const call = (path, { key, cookie, body, origin } = {}) =>
      fetch(base + path, {
        method: body === undefined ? 'GET' : 'POST',
        headers: {
          ...(key ? { authorization: `Bearer ${key}` } : {}),
          ...(cookie ? { cookie } : {}),
          ...(origin ? { origin } : {}),
          ...(body === undefined ? {} : { 'content-type': 'application/json' }),
        },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(5000),
      });
    await start();
    await enableAccount(fixture, server);
    const foreignAccepted = await call('/api/jobs', {
      key: credentials[1].api_key,
      body: {
        prompt: 'foreign actual result',
        model: 'nai-diffusion-4-5-full',
        width: 128,
        height: 128,
        steps: 23,
        seed: 91,
        nocache: true,
      },
    });
    assert.equal(foreignAccepted.status, 202);
    const foreignJob = await foreignAccepted.json();
    await waitFor(
      () =>
        query(fixture, 'SELECT status FROM generation_jobs WHERE id=?', foreignJob.id)[0].status ===
        'done',
    );
    const foreignImageId = query(
        fixture,
        'SELECT artifact_id FROM generation_jobs WHERE id=?',
        foreignJob.id,
      )[0].artifact_id,
      cooldown = query(
        fixture,
        'SELECT cooldown_until FROM upstream_accounts WHERE id=?',
        fixture.accountId,
      )[0].cooldown_until;
    if (cooldown > Date.now()) {
      await new Promise((resolve) => setTimeout(resolve, cooldown - Date.now()));
    }
    const batch = await call('/ai/generate-image', {
      key: credentials[0].api_key,
      body: {
        input: 'owned real three-child request',
        model: 'nai-diffusion-4-5-full',
        parameters: { width: 128, height: 128, steps: 23, seed: 42, n_samples: 3 },
      },
    });
    assert.ok([200, 504].includes(batch.status));
    const ownedParent = batch.headers.get('x-nai-request-id');
    assert.ok(ownedParent);
    await batch.arrayBuffer();
    const children = () =>
      query(
        fixture,
        'SELECT * FROM generation_jobs WHERE request_id=? ORDER BY sample_index',
        ownedParent,
      );
    await waitFor(() => children()[0]?.status === 'done');
    assert.deepEqual(
      children().map((child) => child.status),
      ['done', 'queued', 'pending'],
    );
    const ownedImageId = children()[0].artifact_id;
    assert.equal(
      generationCalls,
      2,
      'one actual foreign setup image plus the original first child',
    );
    await server.kill();
    const sqlite = new Database(fixture.filename);
    sqlite.pragma('foreign_keys=ON');
    try {
      assert.equal(
        await withAuthenticatedMemberApiKey(
          new RelationalMemberStore(sqlite, fixture.env),
          credentials[0].api_key,
          (member) => {
            assert.equal(member.id, credentials[0].user_id);
            new RelationalMemberStore(sqlite, fixture.env).replaceMemberApiKey(member.id, newKey);
            return true;
          },
        ),
        true,
      );
    } finally {
      sqlite.close();
    }
    await start();
    const login = await call('/api/member/login', {
      body: { username: credentials[0].username, password: credentials[0].password },
      origin: base,
    });
    assert.equal(login.status, 200);
    const cookie = login.headers.get('set-cookie').split(';')[0];
    assert.deepEqual(await traceMessage('trace-prove'), { trace: 'proved', count: 1 });
    assert.deepEqual(await traceMessage('trace-begin'), { trace: 'begun' });
    const bearerStartedAt = Date.now(),
      bearer = await call(`/api/requests/${ownedParent}`, { key: newKey });
    assert.equal(bearer.status, 200);
    const expected = await bearer.json(),
      bearerReturnedAt = Date.now(),
      issued = new URL(expected.jobs[0].imageUrl, base),
      nextSecond = (Number(issued.searchParams.get('expires')) - 900 + 1) * 1000;
    // 真正跨过签发秒，不能依赖两个独立读取恰好发生在同一秒。
    await new Promise((resolve) => setTimeout(resolve, Math.max(0, nextSecond - Date.now()) + 10));
    const memberStartedAt = Date.now(),
      member = await call(`/api/member/requests/${ownedParent}`, { cookie });
    assert.equal(member.status, 200);
    const actual = await member.json(),
      memberReturnedAt = Date.now();
    const traced = await traceMessage('trace-end');
    assert.equal(traced.trace, 'ended');
    assert.deepEqual(
      traced.reads,
      [],
      'both actual thin queries must not select protected body/BLOB',
    );
    const businessSnapshot = ({ revision, jobs, ...snapshot }) => {
      assert.match(revision, /^[a-f0-9]{64}$/);
      return {
        ...snapshot,
        jobs: jobs.map((job) => {
          const url = job.imageUrl ? new URL(job.imageUrl, base) : null;
          url?.searchParams.delete('expires');
          url?.searchParams.delete('signature');
          return { ...job, imageUrl: url?.href || '' };
        }),
      };
    };
    // revision 含本次领取链接；只排除其签发参数，路径及其余业务字段仍须精确一致。
    assert.deepEqual(businessSnapshot(actual), businessSnapshot(expected));
    assert.ok(
      Number(new URL(actual.jobs[0].imageUrl, base).searchParams.get('expires')) >
        Number(issued.searchParams.get('expires')),
      '两个真实读取必须跨过签名签发秒',
    );
    assert.notEqual(actual.revision, expected.revision);
    assert.equal(actual.jobs[1].imageUrl, '');
    assert.equal(actual.jobs[2].imageUrl, '');
    for (const [snapshot, startedAt, returnedAt] of [
      [expected, bearerStartedAt, bearerReturnedAt],
      [actual, memberStartedAt, memberReturnedAt],
    ]) {
      const signed = new URL(snapshot.jobs[0].imageUrl, base),
        expires = Number(signed.searchParams.get('expires'));
      assert.equal(signed.origin, base);
      assert.equal(signed.pathname, `/api/images/${ownedImageId}/content`);
      assert.deepEqual([...signed.searchParams.keys()].sort(), ['expires', 'signature']);
      assert.match(signed.searchParams.get('signature'), /^[a-f0-9]{64}$/);
      assert.ok(expires >= Math.floor(startedAt / 1000) + 900);
      assert.ok(expires <= Math.floor(returnedAt / 1000) + 900);
      const image = await fetch(signed);
      assert.equal(image.status, 200);
      assert.equal(image.headers.get('content-type'), 'image/png');
      assert.deepEqual(Buffer.from(await image.arrayBuffer()), png);
      const foreign = new URL(signed),
        changedExpiry = new URL(signed),
        changedSignature = new URL(signed);
      foreign.pathname = `/api/images/${foreignImageId}/content`;
      changedExpiry.searchParams.set('expires', String(expires - 1));
      const signature = signed.searchParams.get('signature');
      changedSignature.searchParams.set(
        'signature',
        `${signature[0] === '0' ? '1' : '0'}${signature.slice(1)}`,
      );
      for (const [forbidden, key] of [
        [foreign, newKey],
        [changedExpiry, credentials[1].api_key],
        [changedSignature, credentials[1].api_key],
      ]) {
        const response = await fetch(forbidden, { headers: { authorization: `Bearer ${key}` } });
        assert.equal(response.status, 403);
        await response.arrayBuffer();
      }
      assert.equal((await call(signed.pathname, { key: credentials[1].api_key })).status, 403);
    }
    assert.equal(
      (await call(`/api/requests/${ownedParent}`, { key: credentials[1].api_key })).status,
      404,
    );
    assert.equal(
      (await call(`/api/requests/${ownedParent}/cancel`, { key: credentials[1].api_key, body: {} }))
        .status,
      404,
    );
    assert.equal(
      (await call(`/api/requests/${ownedParent}`, { key: credentials[0].api_key })).status,
      401,
    );
    for (const origin of [undefined, 'https://evil.invalid']) {
      assert.equal(
        (await call(`/api/member/requests/${ownedParent}/cancel`, { cookie, body: {}, origin }))
          .status,
        403,
      );
    }
    const cancelled = await call(`/api/member/requests/${ownedParent}/cancel`, {
      cookie,
      body: {},
      origin: base,
    });
    assert.equal(cancelled.status, 200);
    const cancelledState = await cancelled.json();
    assert.equal(cancelledState.cancelledCount, 2);
    const { cancelledCount, ...cancelledSnapshot } = cancelledState;
    const rawCancelled = await call(`/api/requests/${ownedParent}`, { key: newKey });
    assert.equal(rawCancelled.status, 200);
    assert.deepEqual(
      businessSnapshot(cancelledSnapshot),
      businessSnapshot(await rawCancelled.json()),
    );
    const repeated = await call(`/api/requests/${ownedParent}/cancel`, { key: newKey, body: {} });
    assert.equal((await repeated.json()).cancelledCount, 0);
    // Existing canonical child id remains distinct from its parent route; this is not a parentless legacy fixture.
    assert.equal((await call(`/api/requests/${children()[0].id}`, { key: newKey })).status, 404);
    assert.equal((await call(`/api/jobs/${children()[0].id}`, { key: newKey })).status, 200);

    const web = await call('/api/jobs', {
      key: newKey,
      body: { prompt: 'new web request', model: 'nai-diffusion-4-5-full', seed: 42, nocache: true },
    });
    assert.equal(web.status, 202);
    const webJob = await web.json();
    assert.ok(webJob.requestId);
    const me = await (await call('/api/member/me', { cookie })).json();
    assert.equal(me.jobs.find((job) => job.id === webJob.id).requestId, webJob.requestId);
    assert.equal(
      (
        await (
          await call(`/api/requests/${webJob.requestId}/cancel`, { key: newKey, body: {} })
        ).json()
      ).cancelledCount,
      1,
    );
    for (const [endpoint, body] of [
      [
        '/ai/generate-image',
        {
          input: 'native request ID',
          model: 'nai-diffusion-4-5-full',
          parameters: { width: 1024, height: 1024, steps: 23, seed: 43 },
        },
      ],
      [
        '/ai/encode-vibe',
        {
          model: 'nai-diffusion-4-5-full',
          image: png.toString('base64'),
          information_extracted: 1,
        },
      ],
    ]) {
      const accepted = await call(endpoint, { key: newKey, body });
      assert.equal(accepted.status, 504);
      const id = accepted.headers.get('x-nai-request-id');
      assert.ok(id);
      assert.match(accepted.headers.get('access-control-expose-headers'), /X-Nai-Request-Id/);
      assert.equal((await call(`/api/requests/${id}`, { key: newKey })).status, 200);
      assert.equal(
        (await (await call(`/api/requests/${id}/cancel`, { key: newKey, body: {} })).json())
          .cancelledCount,
        1,
      );
    }
    assert.equal(generationCalls, 2);
    assert.equal(query(fixture, 'SELECT count(*) AS n FROM generation_attempts')[0].n, 2);
    const charges = query(fixture, 'SELECT * FROM charges ORDER BY rowid');
    assert.equal(
      charges.length,
      2,
      'cancelled queued/pending children and new waiting requests never reserve money',
    );
    assert.ok(
      charges.every(
        (charge) => charge.status === 'settled' && charge.original_account_id === fixture.accountId,
      ),
    );
    await server.stop();
    assertChildPublicReplay(fixture);
  },
);
