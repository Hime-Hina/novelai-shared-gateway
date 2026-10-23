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

const forgedToken = 'synthetic-forged-browser-token';
const webBody = '<!doctype html><title>Synthetic web</title>' + '<p>synthetic page</p>'.repeat(500);
const apiPadding = 'synthetic-api-response '.repeat(100);
const callbackCookies = ['member_session=synthetic-new-session; Path=/api/member; HttpOnly; SameSite=Strict',
  'member_oauth=; Max-Age=0; Path=/api/member; HttpOnly; SameSite=Lax'];

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
  // No executor mock or executor listener: every proxied request must reach Nuxt.
  const nuxt = http.createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    received.push({ method: req.method, url: req.url, headers: req.headers, body: Buffer.concat(chunks).toString() });
    res.setHeader('x-test-upstream', 'nuxt');
    if (req.method === 'GET' && req.url.split('?')[0] === '/api/member/discord/callback') {
      res.writeHead(303, { location: '/?login=synthetic%20success', 'set-cookie': callbackCookies });
      res.end(); return;
    }
    if (req.method === 'GET' && req.url === '/api/member/shared-events') {
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
    res.end(JSON.stringify({ upstream: 'nuxt', padding: apiPadding }));
  });
  t.after(async () => { nuxt.closeAllConnections(); await new Promise(resolve => nuxt.close(resolve)); });
  const nuxtPort = await listen(nuxt);
  const reservations = [http.createServer(), http.createServer()];
  const publicPort = await listen(reservations[0]), privatePort = await listen(reservations[1]);
  for (const server of reservations) await new Promise(resolve => server.close(resolve));

  const source = await readFile(new URL('../../Caddyfile.example', import.meta.url), 'utf8');
  assert.equal(source.match(/^gateway\.example\.com \{/gm)?.length, 1, 'public test site must come from the shipped Caddyfile');
  assert.equal(source.match(/^http:\/\/127\.0\.0\.1:18082, http:\/\/localhost:18082 \{/gm)?.length, 1);
  assert.deepEqual([...new Set([...source.matchAll(/\breverse_proxy\s+(\S+)/g)].map(match => match[1]))], ['127.0.0.1:3000'],
    'the shipped config must have exactly one upstream destination: Nuxt');
  assert.doesNotMatch(source, /EXECUTOR_TOKEN|X-Executor-Token|18081/i);
  // Only test listeners and upstream socket addresses change. All matchers,
  // header rules, ordering, compression and flushing remain the shipped config.
  const config = '{\n    admin off\n}\n' + source
    .replace(/^gateway\.example\.com \{/m, `http://gateway.example.com:${publicPort} {\n    bind 127.0.0.1`)
    .replace(/^http:\/\/127\.0\.0\.1:18082, http:\/\/localhost:18082 \{/m, `http://127.0.0.1:${privatePort}, http://localhost:${privatePort} {`)
    .replaceAll('127.0.0.1:3000', `127.0.0.1:${nuxtPort}`);
  const configPath = path.join(directory, 'Caddyfile'); await writeFile(configPath, config);
  let logs = '', spawnError;
  const caddy = spawn('caddy', ['run', '--config', configPath, '--adapter', 'caddyfile'], {
    env: { PATH: process.env.PATH, HOME: directory, XDG_CONFIG_HOME: directory, XDG_DATA_HOME: directory },
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
  async function routed(side, url, options = {}) {
    const port = side === 'public' ? f.publicPort : f.privatePort;
    const host = side === 'public' ? f.publicHost : f.privateHost;
    const response = await request(port, host, url, options);
    assert.equal(response.status, 200, `${side} ${options.method || 'GET'} ${url}`);
    assert.equal(response.headers['x-test-upstream'], 'nuxt', `${side} ${options.method || 'GET'} ${url}`);
    const observed = f.received.at(-1);
    assert.equal(observed.url, url); assert.equal(observed.method, options.method || 'GET');
    return { response, observed };
  }

  await t.test('all API namespaces and methods reach Nuxt without an executor listener or internal key', async () => {
    for (const side of ['public', 'private']) {
      for (const url of ['/api', '/api/unknown/deep?value=synthetic', '/api/member/requests/batch-1/cancel', '/api/member/requests/a%2Fb',
        '/ai', '/ai/generate-image', '/v1', '/v1/chat/completions', '/user', '/user/data', '/generate?key=synthetic-key']) {
        for (const method of ['GET', 'POST', 'PATCH', 'PUT', 'DELETE', 'HEAD', 'OPTIONS']) {
          const body = ['POST', 'PATCH', 'PUT'].includes(method) ? '{"synthetic":"unchanged"}' : '';
          const { response, observed } = await routed(side, url, { method, body, headers: { 'accept-encoding': 'gzip' } });
          assert.equal(observed.body, body); assert.equal(observed.headers['x-executor-token'], undefined);
          assert.equal(response.headers['content-encoding'], undefined, `${method} ${url}`);
        }
      }
    }
    for (const url of ['/api/admin/member-management', '/api/admin/unknown/deep', '/api/health', '/memory', '/admin/members.html']) {
      for (const method of ['GET', 'POST', 'PATCH', 'HEAD', 'OPTIONS']) await routed('private', url, { method });
    }
  });

  await t.test('public private-management endpoints reject before Nuxt, even with forged credentials', async () => {
    for (const url of ['/admin', '/admin/members.html', '/api/admin', '/api/admin/member-management', '/api/admin/unknown/deep', '/api/health', '/memory']) {
      for (const method of ['GET', 'POST', 'PATCH', 'HEAD', 'OPTIONS']) {
        const before = f.received.length;
        const response = await request(f.publicPort, f.publicHost, url, { method, headers: {
          'x-executor-token': forgedToken, authorization: 'Bearer synthetic-admin', 'x-forwarded-for': '127.0.0.1'
        } });
        assert.equal(response.status, 404, url); assert.equal(f.received.length, before, url);
      }
    }
  });

  await t.test('preserves Host, Origin and browser credentials, sanitizes XFF and injects no internal token', async () => {
    for (const side of ['public', 'private']) for (const forged of [false, true]) {
      const originalHost = side === 'public' ? f.publicHost : f.privateHost;
      const headers = { origin: `http://${originalHost}`, cookie: 'member_session=synthetic-cookie', authorization: 'Bearer synthetic-member',
        'x-forwarded-for': '203.0.113.55', ...(forged ? { 'x-executor-token': forgedToken } : {}) };
      for (const [url, method] of [['/api/member/me', 'GET'], ['/api/member/login', 'POST'], ['/ai/generate-image', 'POST']]) {
        const { observed } = await routed(side, url, { method, headers });
        assert.equal(observed.headers.origin, headers.origin);
        assert.equal(observed.headers.cookie, headers.cookie);
        assert.equal(observed.headers.authorization, headers.authorization);
        assert.equal(observed.headers.host, originalHost);
        assert.equal(observed.headers['x-forwarded-host'], originalHost);
        assert.equal(observed.headers['x-forwarded-proto'], 'http');
        assert.equal(observed.headers['x-forwarded-for'], '127.0.0.1');
        assert.equal(observed.headers['x-executor-token'], forged ? forgedToken : undefined);
      }
    }
    const ipHost = `127.0.0.1:${f.privatePort}`;
    const response = await request(f.privatePort, ipHost, '/api/member/login', { method: 'POST', headers: { origin: `http://${ipHost}` } });
    assert.equal(response.headers['x-test-upstream'], 'nuxt');
    assert.equal(f.received.at(-1).headers.host, ipHost);
    assert.equal(f.received.at(-1).headers.origin, `http://${ipHost}`);
  });

  await t.test('OAuth callback queries, cross-site headers, 303 and separate Set-Cookie headers pass through unchanged', async () => {
    const url = '/api/member/discord/callback?code=synthetic%2Bcode%2Fpart&state=synthetic%20state&scope=identify+guilds';
    for (const side of ['public', 'private']) {
      const port = side === 'public' ? f.publicPort : f.privatePort, host = side === 'public' ? f.publicHost : f.privateHost;
      const headers = { origin: 'https://discord.com', referer: 'https://discord.com/oauth2/authorize',
        'sec-fetch-site': 'cross-site', 'sec-fetch-mode': 'navigate', 'sec-fetch-dest': 'document',
        cookie: 'member_oauth=synthetic-state-cookie', authorization: 'Bearer synthetic-callback-token' };
      // Nuxt/executor, not Caddy, enforce business authorization and accepted methods.
      const login = await routed(side, '/api/member/login', { method: 'POST', headers });
      for (const [key, value] of Object.entries(headers)) assert.equal(login.observed.headers[key], value);
      const response = await request(port, host, url, { headers });
      assert.equal(response.status, 303); assert.equal(response.headers['x-test-upstream'], 'nuxt');
      assert.equal(response.headers.location, '/?login=synthetic%20success');
      assert.deepEqual(response.headers['set-cookie'], callbackCookies);
      const observed = f.received.at(-1);
      assert.equal(observed.url, url); assert.equal(observed.method, 'GET'); assert.equal(observed.headers.host, host);
      for (const [key, value] of Object.entries(headers)) assert.equal(observed.headers[key], value);
    }
  });

  await t.test('all remaining web paths reach Nuxt with gzip, without broad API-prefix matching', async () => {
    for (const side of ['public', 'private']) for (const url of ['/', '/_nuxt/synthetic.js', '/owner', '/unlisted-page', '/apix', '/aix', '/v1extra', '/username', '/generate/nested']) {
      const { response } = await routed(side, url, { headers: { 'accept-encoding': 'gzip' } });
      assert.equal(response.headers['content-encoding'], 'gzip', url);
      const content = gunzipSync(response.body).toString();
      if (url === '/' || url.startsWith('/_nuxt/')) assert.equal(content, webBody);
      else assert.equal(JSON.parse(content).upstream, 'nuxt');
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
