import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { lstat, mkdir, mkdtemp, realpath, rm } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createTestContext, loadFixture, startServer, stopServer } from '@nuxt/test-utils/e2e';

const webRoot = fileURLToPath(new URL('../../', import.meta.url));
const host = 'localhost:19080';
const identity = {
  host, origin: `http://${host}`, cookie: 'nai_member_session=synthetic-development-session',
  authorization: 'Bearer synthetic-development-member-key', 'x-forwarded-for': '198.51.100.18'
};

function request(base, route, method = 'GET') {
  return new Promise((resolve, reject) => {
    const req = http.request(new URL(route, base), {
      method, agent: false, signal: AbortSignal.timeout(15000), headers: {
        ...identity, accept: 'application/json', 'x-executor-token': 'untrusted-browser-value',
        ...(method === 'POST' ? { 'content-type': 'application/json' } : {})
      }
    }, res => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('error', reject);
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks).toString('utf8') }));
    });
    req.on('error', reject);
    req.end(method === 'POST' ? '{"username":"synthetic-only"}' : undefined);
  });
}

test('真实 Nuxt dev 命中正式查询和状态路由，未迁登录不添加隐式代理', { timeout: 180000 }, async t => {
  // test-utils 的 dev CLI 不暴露 --dotenv；发现真实文件就停止，绝不加载其内容。
  await assert.rejects(lstat(path.join(webRoot, '.env')), error => error.code === 'ENOENT',
    'Use process environment and move your own web/.env aside before this smoke test; the test will not read, move, or delete it.');

  const savedEnv = { ...process.env };
  const allowed = new Set(['PATH', 'SYSTEMROOT', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE', 'APPDATA', 'LOCALAPPDATA']);
  for (const key of Object.keys(process.env)) if (!allowed.has(key.toUpperCase())) delete process.env[key];
  const internalToken = `synthetic-development-${randomUUID()}`;
  const observed = [];
  let ctx, buildDir;
  const upstream = http.createServer((req, res) => {
    observed.push({ method: req.method, path: req.url, headers: req.headers });
    if (req.url === '/api/member/shared-events') {
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store' });
      res.end('event: state-invalidated\ndata: {"bootId":"synthetic-dev","version":0,"scopes":["member"],"resync":true}\n\nevent: auth-expired\ndata: {}\n\n');
      return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify({ fixture: 'current-nuxt-development-route', path: req.url }));
  });
  t.after(async () => {
    try {
      if (ctx) {
        const serverProcess = ctx.serverProcess;
        await stopServer();
        // Windows 的树终止工具可能未实际退出；仅兜底当前 helper 创建的子进程。
        if (serverProcess && serverProcess.exitCode == null && serverProcess.process?.signalCode == null) {
          serverProcess.kill('SIGKILL');
          await Promise.resolve(serverProcess).catch(() => {});
        }
      }
      await ctx?.nuxt?.close();
      upstream.closeAllConnections();
      await new Promise(resolve => upstream.close(resolve));
      if (buildDir) {
        const root = await realpath(webRoot);
        const testRoot = await realpath(path.join(webRoot, '.nuxt', 'test'));
        assert.equal(path.relative(root, testRoot), path.join('.nuxt', 'test'), 'test root must stay inside the current web project');
        const target = await realpath(buildDir), relative = path.relative(testRoot, target);
        assert.ok(relative && !relative.startsWith('..') && !path.isAbsolute(relative), 'only the isolated Nuxt test build may be removed');
        await rm(target, { recursive: true, force: true });
      }
    } finally {
      for (const key of Object.keys(process.env)) delete process.env[key];
      Object.assign(process.env, savedEnv);
    }
  });

  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  Object.assign(process.env, {
    CI: '1', NUXT_TELEMETRY_DISABLED: '1',
    NUXT_EXECUTOR_BASE_URL: `http://127.0.0.1:${upstream.address().port}`, NUXT_EXECUTOR_TOKEN: internalToken
  });
  const testRoot = path.join(webRoot, '.nuxt', 'test');
  await mkdir(testRoot, { recursive: true });
  buildDir = await mkdtemp(path.join(testRoot, 'development-routing-'));
  ctx = createTestContext({ rootDir: webRoot, buildDir, dev: true, build: true, browser: false,
    nuxtConfig: { compatibilityDate: '2026-10-17', telemetry: false, devtools: { enabled: false } } });
  await loadFixture();
  assert.equal(ctx.nuxt.options.dev, true);
  assert.equal(path.resolve(ctx.nuxt.options.buildDir), path.resolve(buildDir));
  await startServer();

  const home = await request(ctx.url, '/');
  assert.equal(home.status, 200, home.body.slice(0, 1500));
  assert.match(home.headers['content-type'], /text\/html/);
  assert.match(home.body, /<div id="__nuxt"/);
  assert.match(home.body, /cssr-id=/);
  assert.ok(!home.body.includes(internalToken));
  for (const route of ['/api/member/login-options', '/api/member/me']) {
    const result = await request(ctx.url, route);
    assert.equal(result.status, 200, result.body);
    assert.deepEqual(JSON.parse(result.body), { fixture: 'current-nuxt-development-route', path: route });
    assert.equal(result.headers['cache-control'], 'no-store');
    assert.ok(!result.body.includes(internalToken));
  }
  const events = await request(ctx.url, '/api/member/shared-events');
  assert.equal(events.status, 200);
  assert.match(events.headers['content-type'], /text\/event-stream/);
  assert.match(events.body, /event: connection-status\ndata: \{"state":"open"\}/);
  assert.match(events.body, /"bootId":"synthetic-dev"/);

  const login = await request(ctx.url, '/api/member/login', 'POST');
  assert.ok([404, 405].includes(login.status), `unmigrated login must not silently reach the executor: ${JSON.stringify({ status: login.status, body: login.body.slice(0, 700), observed: observed.map(item => item.path) })}`);
  assert.deepEqual(observed.map(item => [item.method, item.path]), [
    ['GET', '/api/member/login-options'], ['GET', '/api/member/me'], ['GET', '/api/member/shared-events']
  ]);
  for (const { headers } of observed) {
    for (const key of ['host', 'origin', 'cookie', 'authorization']) assert.equal(headers[key], identity[key]);
    assert.equal(headers['x-executor-token'], internalToken);
    assert.match(headers['x-forwarded-for'], /^198\.51\.100\.18, /);
  }
});
