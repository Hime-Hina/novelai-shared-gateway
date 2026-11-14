import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { nativeImagesMsgpack } from '../../server/protocols/native-stream.ts';
import { childFixture, launchGateway, enableAccount, query } from '../helpers/relational-child.mjs';

const png = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=',
  'base64',
);
async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}
async function until(predicate, timeout = 100000, diagnostic) {
  const end = Date.now() + timeout;
  while (!predicate()) {
    if (Date.now() >= end) {
      throw new Error(
        'synthetic connection lifecycle wait timed out' +
          (diagnostic ? ': ' + JSON.stringify(diagnostic()) : ''),
      );
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}
const openAiBody = (label, stream = false) => ({
  model: 'nai-diffusion-4-5-full:k_euler_ancestral',
  stream,
  messages: [
    {
      role: 'user',
      content: `提示词: ${label}\n画师串:\n尺寸: 方图\n提示词引导值: 5\n缩放引导值: 0\n负面提示词:`,
    },
  ],
});

test(
  'direct/OpenAI 超时与断线只结束连接，完整父请求在强杀后继续，缓存响应不变',
  { timeout: 150000 },
  async (t) => {
    const fixture = await childFixture(t, 'connection-http'),
      credentials = fixture.members;
    let releaseFirst;
    const firstGate = new Promise((resolve) => (releaseFirst = resolve));
    const calls = [];
    let quotaCalls = 0;
    const upstream = http.createServer(async (req, res) => {
      if (req.url === '/user/data') {
        quotaCalls++;
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
        return;
      }
      const chunks = [];
      for await (const chunk of req) {
        chunks.push(chunk);
      }
      const payload = JSON.parse(Buffer.concat(chunks));
      calls.push({ at: Date.now(), payload });
      if (calls.length === 1) await firstGate;
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
        OPENAI_CHAT_TIMEOUT_MS: '100',
        QUEUE_MAX_PAYLOAD_BYTES: String(512 * 1024),
      });
      base = server.origin;
    };
    t.after(async () => {
      releaseFirst();
      upstream.closeAllConnections();
      await new Promise((resolve) => upstream.close(resolve));
    });
    const rows = () =>
      query(
        fixture,
        `SELECT j.*,r.member_id AS memberId,r.source,r.accepted_at AS createdAt,
      r.id AS requestId,j.queue_seq AS queueSeq,j.deadline_at AS deadlineAt,j.artifact_id AS imageId,
      j.error_code AS error,j.error_status AS errorStatus FROM generation_jobs j
      JOIN generation_requests r ON r.id=j.request_id ORDER BY j.queue_seq`,
      );
    const safeText = (value, limit) => {
      let text = String(value || '');
      for (const secret of [
        fixture.env.ADMIN_TOKEN,
        fixture.env.NOVELAI_TOKEN,
        ...credentials.flatMap((member) => [member.api_key, member.password]),
      ]) {
        text = text.replaceAll(secret, '[redacted]');
      }
      return text.slice(-limit);
    };
    // 只输出合成样本的状态与时间，区分失败终态、停止派发和仍在前进；不延长等待阈值。
    const diagnostic = () => ({
      calls: calls.length,
      callOffsetsMs: calls.map(({ at }) => at - calls[0].at),
      quotaCalls,
      childExit: server?.exit,
      jobs: rows().map((job) => ({
        id: job.id,
        source: job.source,
        status: job.status,
        queueSeq: job.queueSeq,
        error: safeText(job.error, 250),
        errorStatus: job.errorStatus,
        startedAt: job.startedAt,
        completedAt: job.completedAt,
        upstreamHttpStatus: job.upstreamHttpStatus,
      })),
      accounts: query(fixture, 'SELECT id,enabled,cooldown_until FROM upstream_accounts'),
      outputTail: safeText(server?.output, 5000),
    });
    const post = (body, member = 0) =>
      fetch(base + '/v1/chat/completions', {
        method: 'POST',
        signal: AbortSignal.timeout(5000),
        headers: {
          authorization: `Bearer ${credentials[member].api_key}`,
          'content-type': 'application/json',
        },
        body: typeof body === 'string' ? body : JSON.stringify(body),
      });
    const direct = (label, seed, member = 0) =>
      '/generate?' +
      new URLSearchParams({
        token: credentials[member].api_key,
        prompt: label,
        model: 'nai-diffusion-4-5-full',
        width: '1024',
        height: '1024',
        steps: '23',
        seed: String(seed),
      });
    await start();
    await enableAccount(fixture, server);
    assert.equal(quotaCalls, 1);

    assert.equal((await post('{')).status, 400);
    assert.equal((await post('x'.repeat(2 * 1024 * 1024 + 1))).status, 413);
    assert.equal((await post('x'.repeat(512 * 1024 + 1))).status, 429);
    // 中断未读完的 OpenAI 正文；其临时预算不能泄漏到随后的正常请求。
    const aborted = http.request(base + '/v1/chat/completions', {
      method: 'POST',
      headers: {
        authorization: `Bearer ${credentials[0].api_key}`,
        'content-type': 'application/json',
        'content-length': String(400000),
      },
    });
    aborted.on('error', () => {});
    aborted.write('{"messages":"' + 'x'.repeat(200000));
    await new Promise((resolve) => setTimeout(resolve, 30));
    aborted.destroy();

    const cachedPath = direct('direct timeout', 501);
    const directTimeout = await fetch(base + cachedPath);
    assert.equal(directTimeout.status, 200);
    assert.match(directTimeout.headers.get('content-type'), /image\//);
    assert.equal(directTimeout.headers.get('x-error'), '1');
    assert.equal(directTimeout.headers.get('x-timeout'), '1');
    assert.ok(directTimeout.headers.get('x-nai-request-id'));
    await directTimeout.arrayBuffer();
    const prompt = 'large JSON timeout ' + 'x'.repeat(64000);
    const jsonTimeout = await post(openAiBody(prompt));
    assert.equal(jsonTimeout.status, 504);
    assert.ok(jsonTimeout.headers.get('x-nai-request-id'));
    assert.equal((await jsonTimeout.json()).error.type, 'server_error');
    const streamTimeout = await post(openAiBody('SSE timeout', true), 1);
    assert.equal(streamTimeout.status, 200);
    assert.ok(streamTimeout.headers.get('x-nai-request-id'));
    const events = await streamTimeout.text();
    assert.match(events, /连接超时/);
    assert.match(events, /data: \[DONE\]/);
    assert.equal(rows().length, 3);
    assert.equal(calls.length, 1, 'delivery timeouts cannot release the physical upstream slot');
    assert.equal(rows()[0].status, 'running');
    assert.equal(
      rows()[0].deadlineAt,
      null,
      '100ms delivery timeout cannot persist a task deadline',
    );
    const physical = query(
      fixture,
      'SELECT authorized_at,credential_epoch,upstream_completed_at FROM generation_attempts',
    )[0];
    assert.ok(physical.authorized_at <= Date.now());
    assert.equal(
      physical.credential_epoch,
      query(
        fixture,
        'SELECT credential_epoch FROM upstream_accounts WHERE id=?',
        fixture.accountId,
      )[0].credential_epoch,
    );
    assert.equal(calls[0].payload.parameters.seed, 501);
    assert.equal(physical.upstream_completed_at, null);
    assert.equal(
      query(fixture, "SELECT count(*) AS n FROM charges WHERE status='reserved'")[0].n,
      1,
    );
    assert.ok(
      rows()
        .slice(1)
        .every((job) => job.status === 'queued' && job.deadlineAt === null),
    );
    assert.ok(rows().every((job) => job.queue_expires_at - job.createdAt > 590000));
    const parents = query(
      fixture,
      `SELECT r.*,i.body FROM generation_requests r
      JOIN generation_inputs i ON i.id=r.input_id`,
    );
    assert.equal(parents.length, 3);
    assert.ok(
      parents.some((parent) => {
        const body = JSON.parse(parent.body);
        return (body.displayPrompt ?? body.input) === prompt;
      }),
    );
    assert.ok(
      parents.every(
        (parent) =>
          !parent.body.includes(credentials[0].api_key) && !parent.body.includes('/generate?'),
      ),
    );
    assert.equal(parents.filter((parent) => parent.source === 'openai').length, 2);
    assert.ok(
      rows().every((job) => !('prompt' in job) && !('tag' in job)),
      'job projection contains no inline input',
    );

    for (const [index, endpoint, value] of [
      [0, direct('direct disconnected', 502, 1), null],
      [1, '/v1/chat/completions', openAiBody('JSON disconnected')],
      [2, '/v1/chat/completions', openAiBody('SSE disconnected', true)],
    ]) {
      const before = rows().length;
      const pending = http.request(
        base + endpoint,
        {
          method: value ? 'POST' : 'GET',
          headers: {
            authorization: `Bearer ${credentials[2].api_key}`,
            'content-type': 'application/json',
          },
        },
        (response) => response.resume(),
      );
      pending.on('error', () => {});
      pending.end(value ? JSON.stringify(value) : undefined);
      await until(() => rows().length === before + 1, 5000);
      pending.destroy();
      assert.equal(rows().length, 4 + index);
    }
    const acceptedIds = rows().map((job) => job.id);
    assert.equal(calls.length, 1);
    assert.equal(
      rows()[3].memberId,
      credentials[1].user_id,
      'Direct query token wins over another Bearer member',
    );
    assert.ok(
      rows()
        .slice(4)
        .every((job) => job.memberId === credentials[2].user_id),
    );
    releaseFirst();
    await until(() => rows()[0].status === 'done', 5000, diagnostic);
    const originalInput = query(fixture, 'SELECT * FROM generation_inputs ORDER BY id'),
      originalContext = query(fixture, 'SELECT * FROM normalization_contexts ORDER BY id');
    assert.equal(
      query(fixture, "SELECT count(*) AS n FROM charges WHERE status='settled'")[0].n,
      1,
    );
    await server.kill();
    await start();
    assert.deepEqual(query(fixture, 'SELECT * FROM generation_inputs ORDER BY id'), originalInput);
    assert.deepEqual(
      query(fixture, 'SELECT * FROM normalization_contexts ORDER BY id'),
      originalContext,
    );
    await until(
      () => {
        const jobs = rows();
        if (jobs.some((job) => job.status === 'failed')) {
          assert.fail('synthetic connection lifecycle job failed: ' + JSON.stringify(diagnostic()));
        }
        return jobs.length === 6 && jobs.every((job) => job.status === 'done');
      },
      100000,
      diagnostic,
    );
    assert.equal(calls.length, 6);
    assert.ok(acceptedIds.every((id) => rows().find((job) => job.id === id)?.imageId));
    const charges = query(fixture, 'SELECT * FROM charges');
    assert.equal(charges.length, 6);
    assert.ok(
      charges.every(
        (charge) => charge.status === 'settled' && charge.original_account_id === fixture.accountId,
      ),
    );
    assert.equal(query(fixture, 'SELECT count(*) AS n FROM generation_attempts')[0].n, 6);
    assert.ok(
      query(fixture, 'SELECT * FROM wallets').every(
        (wallet) => wallet.account_id === fixture.accountId,
      ),
    );
    const hit = await fetch(base + cachedPath);
    assert.equal(hit.status, 200);
    assert.equal(hit.headers.get('x-cache'), 'hit');
    assert.ok(hit.headers.get('x-nai-request-id'));
    assert.deepEqual(Buffer.from(await hit.arrayBuffer()), png);
    assert.equal(calls.length, 6);
    assert.equal(rows().length, 7);
    assert.equal(
      query(fixture, 'SELECT count(*) AS n FROM charges')[0].n,
      6,
      'cache delivery cannot charge again',
    );
    await server.stop();
    assert.equal(
      rows().filter((job) => job.source === 'direct' && job.status === 'done').length,
      3,
    );
  },
);
