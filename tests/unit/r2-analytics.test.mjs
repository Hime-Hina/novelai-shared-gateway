import test from 'node:test';
import assert from 'node:assert/strict';
import { setImmediate } from 'node:timers/promises';
import { readR2Operations } from '../../server/r2-analytics.ts';

const config = name => ({ R2_ACCOUNT_ID: 'a'.repeat(32), R2_ANALYTICS_TOKEN: 'synthetic-' + name, R2_BILLING_DAY: '12' });
const row = (actionType, requests) => ({ dimensions: { actionType }, sum: { requests } });
const response = rows => new Response(JSON.stringify({ data: { viewer: { accounts: [{ r2OperationsAdaptiveGroups: rows }] } } }));

test('缺少统计权限或账期配置时显示未知，不伪造免费余额，也不联网', t => {
  t.mock.method(globalThis, 'fetch', () => { throw new Error('Must not fetch'); });
  for (const env of [{}, { ...config('missing'), R2_ANALYTICS_TOKEN: '' }, { ...config('day'), R2_BILLING_DAY: '' }, { ...config('day'), R2_BILLING_DAY: '32' }]) {
    const actual = readR2Operations(env);
    assert.equal(actual.status, 'unconfigured');
    assert.equal(actual.classA.used, null);
    assert.equal(actual.classB.remaining, null);
  }
});

test('按真实账期跨桶统计标准存储，缓存并合并并发请求，查询不阻塞管理摘要', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-10-24T01:00:00Z') });
  const env = config('cache'); let finish, requests = 0;
  t.mock.method(globalThis, 'fetch', (url, options) => {
    requests++;
    assert.equal(url, 'https://api.cloudflare.com/client/v4/graphql');
    assert.equal(options.redirect, 'error');
    assert.ok(options.signal instanceof AbortSignal);
    assert.equal(options.headers.Authorization, `Bearer ${env.R2_ANALYTICS_TOKEN}`);
    const { query, variables } = JSON.parse(options.body);
    assert.equal(variables.start, '2026-10-12T00:00:00.000Z');
    assert.equal(variables.end, '2026-10-24T01:00:00.000Z');
    assert.ok(!query.includes('bucketName'));
    assert.match(query, /storageClass: "Standard"/);
    assert.match(query, /actionStatus_in: \["success", "userError"\]/);
    assert.match(query, /responseStatusCode_neq: 401/);
    return new Promise(resolve => { finish = resolve; });
  });
  for (let i = 0; i < 5; i++) assert.equal(readR2Operations(env).status, 'loading');
  assert.equal(requests, 1);
  finish(response([row('PutObject', 200), row('ListObjects', 3), row('PutBucketStorageClass', 1), row('GetObject', 1200), row('HeadObject', 2), row('ReportUsageSummary', 1), row('DeleteObject', 90), row('GetBucketSippyConfiguration', 2)]));
  await setImmediate();
  const actual = readR2Operations(env);
  assert.equal(actual.periodEnd, '2026-11-12T00:00:00.000Z');
  assert.equal(actual.status, 'ready');
  assert.deepEqual(actual.classA, { used: 204, limit: 1_000_000, remaining: 999796 });
  assert.deepEqual(actual.classB, { used: 1203, limit: 10_000_000, remaining: 9998797 });
  assert.ok(!JSON.stringify(actual).includes(env.R2_ANALYTICS_TOKEN));
  assert.equal(requests, 1);
});

test('失败保留上一份同账期统计，下一账期不沿用旧余额，达到免费额度后剩余钳制为零', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-10-11T23:55:00Z') });
  const env = config('rollover'); let calls = 0;
  t.mock.method(globalThis, 'fetch', async () => {
    calls++;
    if (calls !== 1) return new Response('private upstream error', { status: 503 });
    return response([row('PutObject', 1_000_005), row('GetObject', 10_000_010)]);
  });
  assert.equal(readR2Operations(env).periodStart, '2026-09-12T00:00:00.000Z');
  await setImmediate();
  assert.equal(readR2Operations(env).classA.remaining, 0);
  assert.equal(readR2Operations(env).classB.remaining, 0);
  t.mock.timers.tick(10 * 60_000 + 1);
  const rollover = readR2Operations(env);
  assert.equal(rollover.periodStart, '2026-10-12T00:00:00.000Z');
  assert.equal(rollover.classA.used, null);
  await setImmediate();
  assert.equal(readR2Operations(env).status, 'unavailable');
});

test('后台刷新失败不清空同账期数据，并限制错误重试频率', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-10-24T01:00:00Z') });
  const env = config('stale'); let calls = 0;
  t.mock.method(globalThis, 'fetch', async () => { if (++calls > 1) throw new Error(env.R2_ANALYTICS_TOKEN); return response([row('GetObject', 42)]); });
  readR2Operations(env); await setImmediate();
  t.mock.timers.tick(600001);
  assert.equal(readR2Operations(env).classB.used, 42);
  await setImmediate();
  const actual = readR2Operations(env);
  assert.equal(actual.status, 'stale');
  assert.equal(actual.classB.used, 42);
  assert.equal(actual.checkedAt, '2026-10-24T01:00:00.000Z');
  assert.ok(!JSON.stringify(actual).includes(env.R2_ANALYTICS_TOKEN));
  for (let i = 0; i < 10; i++) readR2Operations(env);
  assert.equal(calls, 2);
});

test('拒绝不完整、未知或非法统计，空成功响应才表示零用量', async t => {
  const bodies = [{}, { errors: [{ message: 'denied' }] },
    ...[[row('UnknownPaidOperation', 1)], [row('PutObject', -1)], [row('GetObject', 0.5)], [row('GetObject', '4')], Array.from({ length: 1000 }, () => row('GetObject', 1))]
      .map(rows => ({ data: { viewer: { accounts: [{ r2OperationsAdaptiveGroups: rows }] } } }))];
  let body;
  t.mock.method(globalThis, 'fetch', async () => new Response(JSON.stringify(body)));
  for (const [index, invalid] of bodies.entries()) {
    body = invalid; const env = config('invalid-' + index);
    readR2Operations(env); await setImmediate();
    assert.equal(readR2Operations(env).status, 'unavailable');
    assert.equal(readR2Operations(env).classA.remaining, null);
  }
  body = { data: { viewer: { accounts: [{ r2OperationsAdaptiveGroups: [] }] } } };
  const env = config('empty'); readR2Operations(env); await setImmediate();
  assert.equal(readR2Operations(env).classA.remaining, 1_000_000);
});

test('月底账期按 UTC 和当月最后一天处理，包括闰年', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2028-02-29T00:00:00Z') });
  t.mock.method(globalThis, 'fetch', async () => response([]));
  const env = { ...config('leap'), R2_BILLING_DAY: '31' };
  const actual = readR2Operations(env);
  assert.equal(actual.periodStart, '2028-02-29T00:00:00.000Z');
  assert.equal(actual.periodEnd, '2028-03-31T00:00:00.000Z');
  await setImmediate();
});
