import test from 'node:test';
import assert from 'node:assert/strict';
import { setImmediate } from 'node:timers/promises';
import { readPikPakQuota } from '../../server/pikpak-storage.ts';

const config = name => ({ PIKPAK_API_TOKEN: 'synthetic-readonly-' + name });
const response = (usage = '1366244271407', limit = '10995116277760') => new Response(JSON.stringify({ quota: { usage, limit } }));

test('未配置账户令牌时不借用 WebDAV 凭据、不联网，也不显示伪零值', t => {
  t.mock.method(globalThis, 'fetch', () => { throw new Error('Must not fetch'); });
  assert.deepEqual(readPikPakQuota({ PIKPAK_WEBDAV_PASSWORD: 'private-dav-password' }), {
    usedBytes: null, totalBytes: null, checkedAt: null, status: 'unconfigured'
  });
});

test('账户接口按分钟缓存、合并并发，不阻塞摘要；更换令牌不沿用旧账户数据', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-11-04T06:00:00Z') });
  const env = config('cache'); let finish, calls = 0;
  t.mock.method(globalThis, 'fetch', (url, options) => {
    calls++;
    assert.equal(url, 'https://api-drive.mypikpak.com/drive/v1/about');
    assert.equal(options.headers.Authorization, `Bearer ${calls === 1 ? env.PIKPAK_API_TOKEN : config('other-account').PIKPAK_API_TOKEN}`);
    assert.equal(options.redirect, 'error'); assert.ok(options.signal instanceof AbortSignal);
    return new Promise(resolve => { finish = resolve; });
  });
  for (let i = 0; i < 5; i++) assert.equal(readPikPakQuota(env).status, 'loading');
  assert.equal(calls, 1);
  finish(response()); await setImmediate();
  const actual = readPikPakQuota(env);
  assert.deepEqual(actual, { usedBytes: 1366244271407, totalBytes: 10995116277760, checkedAt: '2026-11-04T06:00:00.000Z', status: 'ready' });
  t.mock.timers.tick(59_999); readPikPakQuota(env); assert.equal(calls, 1);
  assert.equal(readPikPakQuota(config('other-account')).usedBytes, null);
  finish(response('0', '0')); await setImmediate();
  assert.equal(readPikPakQuota(config('other-account')).usedBytes, 0);
  assert.ok(!JSON.stringify(actual).includes(env.PIKPAK_API_TOKEN));
});

test('令牌失效或网络失败保留上次容量及时间，按分钟重试', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-11-04T06:00:00Z') });
  const env = config('stale'); let calls = 0;
  t.mock.method(globalThis, 'fetch', async () => ++calls === 1 ? response() : new Response('private upstream response', { status: 401 }));
  readPikPakQuota(env); await setImmediate();
  const before = readPikPakQuota(env);
  t.mock.timers.tick(60_001); readPikPakQuota(env); await setImmediate();
  assert.deepEqual(readPikPakQuota(env), { ...before, status: 'stale' });
  for (let i = 0; i < 5; i++) readPikPakQuota(env);
  assert.equal(calls, 2);
});

test('拒绝缺失、负值、非整数、溢出或错误类型，账户超额用量仍如实显示', async t => {
  let body;
  t.mock.method(globalThis, 'fetch', async () => new Response(JSON.stringify(body)));
  for (const [index, invalid] of [null, {}, { quota: {} }, ...[null, '', ' ', -1, '1.5', true, Number.MAX_SAFE_INTEGER + 1]
    .map(usage => ({ quota: { usage, limit: '100' } })), { quota: { usage: '1', limit: null } }].entries()) {
    body = invalid; const env = config('invalid-' + index);
    readPikPakQuota(env); await setImmediate();
    assert.equal(readPikPakQuota(env).status, 'unavailable');
    assert.equal(readPikPakQuota(env).usedBytes, null);
  }
  body = { quota: { usage: 101, limit: 100 } }; const env = config('over-quota');
  readPikPakQuota(env); await setImmediate();
  assert.equal(readPikPakQuota(env).usedBytes, 101);
  assert.equal(readPikPakQuota(env).totalBytes, 100);
});
