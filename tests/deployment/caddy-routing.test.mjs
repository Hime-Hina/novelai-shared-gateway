import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { gunzipSync } from 'node:zlib';

const executorToken = 'synthetic-caddy-executor-token-for-routing';
const forgedToken = 'synthetic-forged-browser-token';
const webBody = '<!doctype html><title>Synthetic web</title>' + '<p>synthetic page</p>'.repeat(500);
const apiPadding = 'synthetic-api-response '.repeat(100);

async function listen(server) {
  server.listen(0, '127.0.0.1'); await once(server, 'listening'); return server.address().port;
}

function request(port, host, url, { method = 'GET', headers = {}, body = '' } = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request({ hostname: '127.0.0.1', port, path: url, method, headers: { host, ...headers } }, res => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('error', reject);
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
    });
    req.on('error', reject);
    req.setTimeout(3000, () => req.destroy(new Error('Synthetic HTTP request timed out')));
    req.end(body);
  });
}

async function fixture(t) {
  const directory = await mkdtemp(path.join(tmpdir(), 'caddy-routing-'));
  const received = [];
  const servers = ['nuxt', 'executor'].map(upstream => http.createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    received.push({ upstream, method: req.method, url: req.url, headers: req.headers, body: Buffer.concat(chunks).toString() });
    res.setHeader('x-test-upstream', upstream);
    if (upstream === 'nuxt' && req.method === 'GET' && req.url === '/api/member/shared-events') {
      res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' });
      res.write('event: status\ndata: {"synthetic":true}\n\n');
      // Deliberately remain open: receiving a frame must not depend on upstream EOF.
      return;
    }
    if (req.url === '/' || req.url.startsWith('/_nuxt/')) {
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' }); res.end(webBody);
      return;
    }
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ upstream, padding: apiPadding }));
  }));
  for (const server of servers) t.after(async () => {
    server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
  });
  const nuxtPort = await listen(servers[0]), executorPort = await listen(servers[1]);
  const reservations = [http.createServer(), http.createServer()];
  const publicPort = await listen(reservations[0]), privatePort = await listen(reservations[1]);
  for (const server of reservations) await new Promise(resolve => server.close(resolve));

  const source = await readFile(new URL('../../Caddyfile.example', import.meta.url), 'utf8');
  assert.equal(source.match(/^gateway\.example\.com \{/gm)?.length, 1, 'public test site must come from the shipped Caddyfile');
  assert.equal(source.match(/^http:\/\/127\.0\.0\.1:18082, http:\/\/localhost:18082 \{/gm)?.length, 1);
  // Only test listeners and upstream socket addresses change. All matchers,
  // header rules, ordering, compression and flushing remain the shipped config.
  const config = '{\n    admin off\n}\n' + source
    .replace(/^gateway\.example\.com \{/m, `http://gateway.example.com:${publicPort} {\n    bind 127.0.0.1`)
    .replace(/^http:\/\/127\.0\.0\.1:18082, http:\/\/localhost:18082 \{/m, `http://127.0.0.1:${privatePort}, http://localhost:${privatePort} {`)
    .replaceAll('127.0.0.1:3000', `127.0.0.1:${nuxtPort}`)
    .replaceAll('127.0.0.1:18081', `127.0.0.1:${executorPort}`);
  const configPath = path.join(directory, 'Caddyfile'); await writeFile(configPath, config);
  let logs = '', spawnError;
  const caddy = spawn('caddy', ['run', '--config', configPath, '--adapter', 'caddyfile'], {
    env: { PATH: process.env.PATH, HOME: directory, XDG_CONFIG_HOME: directory, XDG_DATA_HOME: directory, EXECUTOR_TOKEN: executorToken },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  const exited = new Promise(resolve => caddy.once('exit', resolve));
  caddy.on('error', error => { spawnError = error; });
  for (const stream of [caddy.stdout, caddy.stderr]) stream.on('data', data => { logs = (logs + data).slice(-16000); });
  t.after(async () => {
    if (!spawnError && caddy.exitCode === null) {
      caddy.kill('SIGTERM');
      const timer = setTimeout(() => caddy.kill('SIGKILL'), 3000);
      try { await exited; } finally { clearTimeout(timer); }
    }
  });
  const publicHost = `gateway.example.com:${publicPort}`, privateHost = `localhost:${privatePort}`;
  let ready = false;
  for (let attempt = 0; attempt < 100; attempt++) {
    if (spawnError) throw spawnError; // No skipped test when Caddy is missing.
    if (caddy.exitCode !== null) throw new Error(`Caddy exited before listening: ${logs}`);
    try { ready = (await request(publicPort, publicHost, '/api/member/me')).status === 200; } catch { /* wait for listener */ }
    if (ready) break;
    await delay(50);
  }
  assert.ok(ready, `Caddy failed to become ready: ${logs}`);
  received.length = 0;
  return { received, publicPort, privatePort, publicHost, privateHost };
}

test('shipped Caddy routing over real HTTP', { timeout: 30000 }, async t => {
  const f = await fixture(t);
  async function routed(side, url, upstream, options = {}) {
    const port = side === 'public' ? f.publicPort : f.privatePort;
    const host = side === 'public' ? f.publicHost : f.privateHost;
    const response = await request(port, host, url, options);
    assert.equal(response.status, 200, `${side} ${options.method || 'GET'} ${url}`);
    assert.equal(response.headers['x-test-upstream'], upstream, `${side} ${options.method || 'GET'} ${url}`);
    const observed = f.received.at(-1);
    assert.equal(observed.url, url); assert.equal(observed.method, options.method || 'GET');
    return { response, observed };
  }

  await t.test('only declared GET queries reach Nuxt, including one-segment requests', async () => {
    for (const side of ['public', 'private']) {
      for (const url of ['/api/member/login-options', '/api/member/me', '/api/member/key', '/api/member/shared-status',
        '/api/member/heatmap?status=failed', '/api/member/owner/member-management?view=losses',
        '/api/member/owner/statistics', '/api/member/owner/summary', '/api/member/requests/batch-1']) {
        await routed(side, url, 'nuxt');
      }
      for (const url of ['/api/member/requests', '/api/member/requests/', '/api/member/requests/batch-1/',
        '/api/member/requests/batch-1/cancel', '/api/member/requests/a%2Fb', '/api/member/not-migrated']) {
        await routed(side, url, 'executor');
      }
    }
    for (const url of ['/api/admin/member-management?view=losses', '/api/admin/summary', '/api/admin/ping']) await routed('private', url, 'nuxt');
  });

  await t.test('writes, HEAD, OPTIONS and legacy generation routes stay with the executor', async () => {
    const body = JSON.stringify({ synthetic: 'payload unchanged' });
    for (const side of ['public', 'private']) {
      for (const url of ['/api/member/login', '/api/member/logout', '/api/member/me', '/api/member/owner/member-management',
        '/api/member/requests/batch-1/cancel', '/ai/generate-image', '/ai/generate-image-stream', '/ai/upscale', '/ai/encode-vibe', '/v1/chat/completions']) {
        const { observed } = await routed(side, url, 'executor', { method: 'POST', body, headers: { 'content-type': 'application/json' } });
        assert.equal(observed.body, body);
      }
      for (const method of ['HEAD', 'OPTIONS']) for (const url of ['/api/member/me', '/api/member/owner/summary', '/api/member/requests/batch-1', '/api/member/shared-events']) {
        await routed(side, url, 'executor', { method });
      }
      for (const url of ['/v1/models', '/generate?key=synthetic-key']) await routed(side, url, 'executor');
    }
    for (const method of ['POST', 'HEAD', 'OPTIONS']) await routed('private', '/api/admin/member-management', 'executor', { method });
  });

  await t.test('public private-management endpoints reject before either upstream, even with forged credentials', async () => {
    for (const url of ['/admin', '/admin/members.html', '/api/admin/member-management', '/api/admin/summary', '/api/admin/ping', '/api/health', '/memory']) {
      for (const method of ['GET', 'POST']) {
        const before = f.received.length;
        const response = await request(f.publicPort, f.publicHost, url, { method, headers: {
          'x-executor-token': executorToken, authorization: 'Bearer synthetic-admin', 'x-forwarded-for': '127.0.0.1'
        } });
        assert.equal(response.status, 404, url); assert.equal(f.received.length, before, url);
      }
    }
  });

  await t.test('preserves browser credentials and Origin, sanitizes XFF and overwrites executor token', async () => {
    for (const side of ['public', 'private']) for (const forged of [false, true]) {
      const originalHost = side === 'public' ? f.publicHost : f.privateHost;
      const headers = { origin: `http://${originalHost}`, cookie: 'member_session=synthetic-cookie', authorization: 'Bearer synthetic-member',
        'x-forwarded-for': '203.0.113.55', ...(forged ? { 'x-executor-token': forgedToken } : {}) };
      for (const [url, upstream, method] of [['/api/member/me', 'nuxt', 'GET'], ['/api/member/login', 'executor', 'POST']]) {
        const { observed } = await routed(side, url, upstream, { method, headers });
        assert.equal(observed.headers.origin, headers.origin);
        assert.equal(observed.headers.cookie, headers.cookie);
        assert.equal(observed.headers.authorization, headers.authorization);
        assert.equal(observed.headers.host, side === 'public' && upstream === 'executor' ? 'gateway.example.com' : originalHost);
        assert.equal(observed.headers['x-forwarded-host'], originalHost);
        assert.equal(observed.headers['x-forwarded-proto'], 'http');
        assert.equal(observed.headers['x-forwarded-for'], '127.0.0.1');
        if (upstream === 'executor') assert.equal(observed.headers['x-executor-token'], executorToken);
        else assert.notEqual(observed.headers['x-executor-token'], executorToken);
      }
    }
    const ipHost = `127.0.0.1:${f.privatePort}`;
    await request(f.privatePort, ipHost, '/api/member/login', { method: 'POST', headers: { origin: `http://${ipHost}` } });
    assert.equal(f.received.at(-1).headers.host, ipHost);
    assert.equal(f.received.at(-1).headers.origin, `http://${ipHost}`);
  });

  await t.test('API responses stay uncompressed while web responses can use gzip', async () => {
    for (const side of ['public', 'private']) {
      for (const [url, upstream] of [['/api/member/me', 'nuxt'], ['/v1/models', 'executor']]) {
        const { response } = await routed(side, url, upstream, { headers: { 'accept-encoding': 'gzip' } });
        assert.equal(response.headers['content-encoding'], undefined);
        assert.ok(response.body.length > 512); assert.equal(JSON.parse(response.body).upstream, upstream);
      }
      const { response } = await routed(side, '/', 'nuxt', { headers: { 'accept-encoding': 'gzip' } });
      assert.equal(response.headers['content-encoding'], 'gzip'); assert.equal(gunzipSync(response.body).toString(), webBody);
    }
  });

  await t.test('SSE first frame arrives before upstream EOF and without gzip', async () => {
    for (const [port, host] of [[f.publicPort, f.publicHost], [f.privatePort, f.privateHost]]) {
      await new Promise((resolve, reject) => {
        const req = http.request({ hostname: '127.0.0.1', port, path: '/api/member/shared-events', headers: { host, 'accept-encoding': 'gzip' } });
        const timer = setTimeout(() => { req.destroy(); reject(new Error('SSE first frame was buffered for more than 1500ms')); }, 1500);
        req.on('error', error => { clearTimeout(timer); reject(error); });
        req.on('response', res => {
          try {
            assert.equal(res.statusCode, 200); assert.equal(res.headers['x-test-upstream'], 'nuxt');
            assert.equal(res.headers['content-encoding'], undefined); assert.equal(res.headers['content-type'], 'text/event-stream');
          } catch (error) { clearTimeout(timer); req.destroy(); reject(error); return; }
          let data = '';
          res.on('data', chunk => {
            data += chunk;
            if (!data.includes('\n\n')) return;
            clearTimeout(timer); req.destroy();
            try { assert.equal(data, 'event: status\ndata: {"synthetic":true}\n\n'); resolve(); } catch (error) { reject(error); }
          });
          res.on('error', error => { clearTimeout(timer); reject(error); });
        });
        req.end();
      });
    }
  });
});
