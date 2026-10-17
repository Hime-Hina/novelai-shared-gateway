import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { clientIp } from '../../server/client-ip.ts';
import { checkMemberLoginRate } from '../../server/member-auth.ts';
import { handleMemberRoutes } from '../../server/member-routes.js';

const request = (peer, forwarded) => ({ socket: { remoteAddress: peer }, headers: { 'x-forwarded-for': forwarded } });

test('默认和不可信连接忽略所有客户端自报地址', () => {
  const req = request('198.51.100.1', '203.0.113.1');
  req.headers['x-real-ip'] = '203.0.113.2';
  req.headers.forwarded = 'for=203.0.113.3';
  assert.equal(clientIp(req, {}), '198.51.100.1');
  assert.equal(clientIp(req, { TRUSTED_PROXY_IPS: '127.0.0.1' }), '198.51.100.1');
  assert.equal(clientIp(request(undefined, '203.0.113.1'), {}), 'unknown');
});

test('可信代理从右向左停止在首个不可信地址，客户端伪造前缀无效', () => {
  const env = { TRUSTED_PROXY_IPS: '127.0.0.1, 192.0.2.10' };
  assert.equal(clientIp(request('127.0.0.1', '203.0.113.1'), env), '203.0.113.1');
  assert.equal(clientIp(request('127.0.0.1', '203.0.113.99, 203.0.113.1, 192.0.2.10'), env), '203.0.113.1');
  assert.equal(clientIp(request('127.0.0.1', '203.0.113.99, 192.0.2.11'), env), '192.0.2.11');
  // 第一个不可信地址左边的任何内容都没有身份意义。
  assert.equal(clientIp(request('127.0.0.1', 'spoofed, 203.0.113.1'), env), '203.0.113.1');
});

test('IPv4 mapped IPv6 与不同 IPv6 文本写法共用身份', () => {
  const env = { TRUSTED_PROXY_IPS: '127.0.0.1, 2001:0db8:0:0:0:0:0:10' };
  assert.equal(clientIp(request('::ffff:127.0.0.1', '0:0:0:0:0:ffff:cb00:7101'), env), '203.0.113.1');
  assert.equal(clientIp(request('2001:db8::10', '2001:0db8:0:0:0:0:0:1'), env), '2001:db8::1');
  assert.equal(clientIp(request('::ffff:192.0.2.1'), {}), '192.0.2.1');
});

test('缺失或畸形转发地址回退连接来源，非法信任配置拒绝登录', () => {
  const env = { TRUSTED_PROXY_IPS: '127.0.0.1' };
  for (const forwarded of [undefined, '', 'unknown', '203.0.113.1:1234', '[2001:db8::1]', 'fe80::1%eth0', '203.0.113.1,', ['203.0.113.1']]) {
    assert.equal(clientIp(request('127.0.0.1', forwarded), env), '127.0.0.1');
  }
  for (const configured of ['*', 'localhost', '10.0.0.0/8', '127.0.0.1,']) {
    assert.throws(() => clientIp(request('127.0.0.1'), { TRUSTED_PROXY_IPS: configured }), error => error.statusCode === 503);
  }
});

async function loginServer(t, env) {
  let passwordChecks = 0;
  const deps = { env, store: { update() {
    passwordChecks++;
    throw Object.assign(new Error('synthetic invalid credentials'), { statusCode: 401 });
  } } };
  const server = http.createServer(async (req, res) => {
    try {
      await handleMemberRoutes(req, res, new URL(req.url, 'https://gateway.example'), deps);
    } catch (error) {
      res.writeHead(error.statusCode || 500);
      res.end();
    }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => { server.closeAllConnections(); server.close(); });
  const call = (path, forwarded, body = {}) => new Promise((resolve, reject) => {
    const req = http.request({
      hostname: '127.0.0.1', port: server.address().port, path, method: 'POST',
      headers: { host: 'gateway.example', origin: 'https://gateway.example', 'content-type': 'application/json', 'x-forwarded-for': forwarded }
    }, res => { res.resume(); res.on('end', () => resolve(res.statusCode)); });
    req.on('error', reject);
    req.end(JSON.stringify(body));
  });
  return { call, passwordChecks: () => passwordChecks };
}

test('密码与 Discord 路由隔离不同代理客户端，伪造链前缀不能绕过来源上限', async t => {
  const env = { TRUSTED_PROXY_IPS: '127.0.0.1' };
  const { call, passwordChecks } = await loginServer(t, env);
  for (let i = 0; i < 120; i++) checkMemberLoginRate(null, '198.51.100.20');

  assert.equal(await call('/api/member/login', '203.0.113.98, 198.51.100.20', { username: 'proxy-blocked' }), 429);
  assert.equal(passwordChecks(), 0);
  assert.equal(await call('/api/member/login', '198.51.100.21', { username: 'proxy-other' }), 401);
  assert.equal(passwordChecks(), 1);

  Object.assign(env, {
    DISCORD_CLIENT_ID: '123456789012345678', DISCORD_CLIENT_SECRET: 'synthetic-client-secret',
    DISCORD_GUILD_ID: '223456789012345678', PUBLIC_BASE_URL: 'https://gateway.example'
  });
  assert.equal(await call('/api/member/discord/start', '203.0.113.97, 198.51.100.20'), 429);
  assert.equal(await call('/api/member/discord/start', '198.51.100.21'), 200);

  // 未配置可信代理时，轮换客户端可控头不能重置真实连接的计数。
  env.TRUSTED_PROXY_IPS = '';
  for (let i = 0; i < 120; i++) checkMemberLoginRate(null, '127.0.0.1');
  assert.equal(await call('/api/member/discord/start', '198.51.100.22'), 429);
  assert.equal(await call('/api/member/discord/start', '198.51.100.23'), 429);
});
