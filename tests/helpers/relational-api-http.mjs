import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createServer } from 'node:http';
import { EventEmitter, once } from 'node:events';
import path from 'node:path';
import { fixture, NOW, image } from './relational-generation.mjs';
import { createRelationalNativeHttp } from '../../server/generation/relational-native-http.ts';
import { createRelationalQueueRuntime } from '../../server/generation/relational-queue-runtime.ts';
import { createGenerationPayloadBudget } from '../../server/generation/payload-budget.ts';
import { nativeGenerationError } from '../../server/protocols/native-http.ts';
import { JobPreviews } from '../../server/generation/job-previews.ts';
import { cooldownRetryAfter } from '../../server/quota/shared-capacity.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { relationalSnapshotSha256 } from '../../server/storage/relational-private-restore.ts';
import { R2Storage } from '../../server/storage/r2-storage.ts';
import { copyRelationalArtifactReplica } from '../../server/storage/relational-replica-io.ts';

export const keys = { m1: 'STA1N-relational-native-m1', m2: 'STA1N-relational-native-m2' };
export const input = (parameters = {}, root = {}) => ({
  input: 'synthetic private native prompt',
  model: 'nai-diffusion-4-5-full',
  action: 'generate',
  parameters: { width: 128, height: 128, steps: 30, seed: -1, ...parameters },
  ...root,
});
export async function waitFor(predicate) {
  const deadline = Date.now() + 5000;
  while (!predicate()) {
    assert.ok(Date.now() < deadline, 'real Native HTTP/queue condition did not complete');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}
export async function setup(t, options = {}) {
  const cleanup = [],
    f = await fixture({ after: (fn) => cleanup.push(fn) }, options);
  await f.accept('existing', { steps: options.fixed === '0' ? 23 : 30 });
  f.dispatch('existing-0', NOW);
  await f.publish('existing-0');
  f.db.transaction(() => {
    for (const [id, key] of Object.entries(keys)) {
      f.members.replaceMemberApiKey(id, key);
    }
  })();
  const sourcePath = path.join(f.dir, 'native-source.sqlite');
  await f.db.backup(sourcePath);
  const source = new Database(sourcePath);
  source.pragma('journal_mode=DELETE');
  source.close();
  await verifyAndInstallRelationalStorage(f.db, {
    sourcePath,
    sourceSha256: relationalSnapshotSha256(sourcePath),
    historyId: 'generation-history',
    finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get()
      .seq,
    replicas: { dataDir: f.dir },
  });
  let clock = NOW + 10010;
  const upstream = { calls: [], handler: null, release: null };
  const upstreamServer = createServer(async (req, res) => {
    assert.equal(f.db.inTransaction, false);
    const chunks = [];
    for await (const chunk of req) {
      chunks.push(chunk);
    }
    const body = JSON.parse(Buffer.concat(chunks).toString());
    upstream.calls.push({ url: req.url, body, authorization: req.headers.authorization });
    if (upstream.handler) {
      await upstream.handler(req, res, body);
      return;
    }
    if (req.url.includes('encode-vibe')) {
      res.writeHead(200, { 'content-type': 'application/octet-stream' });
      res.end(Buffer.alloc(200, 0x80));
    } else {
      res.writeHead(200, { 'content-type': 'image/webp' });
      res.end(image);
    }
  });
  upstreamServer.listen(0, '127.0.0.1');
  await once(upstreamServer, 'listening');
  const env = {
    NOVELAI_API_URL: `http://127.0.0.1:${upstreamServer.address().port}`,
    NOVELAI_STREAM_IMAGE: options.stream ? 'true' : 'false',
    MOCK_WHEN_NO_ACCOUNT: 'false',
    ...options.env,
  };
  const events = new EventEmitter(),
    previews = new JobPreviews(),
    controls = new Map();
  const budget = createGenerationPayloadBudget(() =>
    f.inputs.activePayloadBytes(controls.values()),
  );
  const errors = [],
    runtime = createRelationalQueueRuntime({
      sqlite: f.db,
      generations: f.generations,
      inputs: f.inputs,
      dataDir: f.dir,
      controls,
      events,
      previews,
      env,
      now: () => clock,
      onError: (event) => errors.push(event),
    });
  const replicas = { dataDir: f.dir };
  const handle = (options.createHttp ?? createRelationalNativeHttp)({
    sqlite: f.db,
    events,
    previews,
    payloadBudget: budget,
    replicas,
    env,
    now: () => clock,
    schedule: runtime.schedule,
    timeoutMs: options.timeoutMs ?? 60000,
    logDiagnostic() {},
    ...options.httpDependencies,
    readJson: async (req, maxBytes) => {
      const chunks = [];
      let size = 0;
      for await (const chunk of req) {
        size += chunk.length;
        if (size > maxBytes) {
          throw Object.assign(new Error('请求体超过大小限制'), { statusCode: 413 });
        }
        req.generationPayloadClaim?.resize(size);
        options.onBodyChunk?.(req);
        chunks.push(chunk);
      }
      try {
        return JSON.parse(Buffer.concat(chunks).toString());
      } catch {
        throw Object.assign(new Error('invalid JSON body.'), { statusCode: 400 });
      }
    },
    sendImage: (res, status, mimeType, buffer, headers) => {
      res.writeHead(status, {
        'content-type': mimeType,
        'content-length': buffer.length,
        ...headers,
      });
      res.end(buffer);
    },
  });
  await runtime.start();
  const server = createServer(async (req, res) => {
    try {
      if (!(await handle(req, res, new URL(req.url, `http://${req.headers.host}`)))) {
        res.writeHead(404);
        res.end();
      }
    } catch (error) {
      if (res.destroyed || res.writableEnded) {
        return;
      }
      if (res.headersSent) {
        res.destroy();
        return;
      }
      const payload = (options.renderError ?? nativeGenerationError)(error),
        retryAfter = cooldownRetryAfter(error.cooldownUntil, clock);
      res.writeHead(payload.statusCode, {
        'content-type': 'application/json; charset=utf-8',
        ...(retryAfter ? { 'retry-after': String(retryAfter) } : {}),
        ...(error.requestId ? { 'x-nai-request-id': error.requestId } : {}),
      });
      res.end(JSON.stringify(payload.body));
    }
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const origin = `http://127.0.0.1:${server.address().port}`;
  t.after(async () => {
    upstream.release?.();
    server.closeAllConnections();
    upstreamServer.closeAllConnections();
    await runtime.stop();
    await Promise.all([
      new Promise((resolve) => server.close(resolve)),
      new Promise((resolve) => upstreamServer.close(resolve)),
    ]);
    for (const run of cleanup) {
      await run();
    }
  });
  const call = async (
    body = input(),
    {
      endpoint = '/ai/generate-image',
      member = 'm1',
      key,
      accept = 'application/json',
      headers = {},
      ...rest
    } = {},
  ) => {
    const response = await fetch(origin + endpoint, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${keys[member]}`,
        ...(body instanceof FormData ? {} : { 'content-type': 'application/json' }),
        accept,
        ...(key === undefined ? {} : { 'idempotency-key': key }),
        ...headers,
      },
      body: body instanceof FormData ? body : JSON.stringify(body),
      ...rest,
    });
    return {
      status: response.status,
      headers: response.headers,
      buffer: Buffer.from(await response.arrayBuffer()),
    };
  };
  return {
    ...f,
    server,
    origin,
    runtime,
    events,
    controls,
    budget,
    previews,
    replicas,
    upstream,
    errors,
    call,
    advance(ms = 10001) {
      clock += ms;
      runtime.tick();
    },
    get clock() {
      return clock;
    },
  };
}

export async function replicateMockR2(t, f, artifactId) {
  const originalFetch = globalThis.fetch,
    calls = [],
    objects = new Map(),
    controls = { failed: false },
    r2 = new R2Storage({
      accountId: 'a'.repeat(32),
      bucket: 'native-reference-images',
      accessKeyId: 'synthetic-private-access',
      secretAccessKey: 'synthetic-private-secret',
    });
  // 仅模拟云传输；真实R2 client/签名、复制确认owner和字节摘要仍执行，不访问真实云。
  t.mock.method(globalThis, 'fetch', async (address, options) => {
    const url = new URL(address);
    if (!url.hostname.endsWith('.r2.cloudflarestorage.com')) {
      return originalFetch(address, options);
    }
    assert.equal(f.db.inTransaction, false);
    assert.ok(options.signal);
    calls.push(options.method);
    if (options.method === 'PUT') {
      objects.set(url.href, Buffer.from(options.body));
      return new Response(null, { status: 204 });
    }
    assert.equal(options.method, 'GET');
    if (controls.failed || !objects.has(url.href)) {
      return new Response(null, { status: 503 });
    }
    const bytes = objects.get(url.href);
    return new Response(bytes, { headers: { 'content-length': String(bytes.length) } });
  });
  f.replicas.r2 = r2;
  await copyRelationalArtifactReplica(
    f.db,
    artifactId,
    'r2',
    {
      ...f.replicas,
      idle: () => f.controls.size === 0,
    },
    () => f.clock,
  );
  return { r2, calls, controls };
}
