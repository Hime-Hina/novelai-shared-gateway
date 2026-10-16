import test from 'node:test';
import assert from 'node:assert/strict';
import { sharedAccountConcurrency, sharedAccountReadyDelay, recordSharedCapacityError, recordSharedCompletion } from '../../server/shared-capacity.js';

const NOW = 1800000000000;

test('共享上游固定串行，旧学习结果、成功次数和观察期均不能恢复多并发', () => {
  for (const account of [{}, { capacityConcurrencyLimit: 4 }, { capacityConcurrencyLimit: 2, capacitySuccesses: 100, capacityProbeAfter: NOW - 1 }, { capacityLimitedUntil: NOW + 300000 }]) {
    assert.equal(sharedAccountConcurrency(account, NOW), 1);
    assert.equal(sharedAccountConcurrency(structuredClone(account), NOW + 86400000), 1);
  }
});

test('完成后至少等待十秒，重启仍保留间隔且不会缩短已有冷却', () => {
  const account = { lastSharedDispatchAt: new Date(NOW).toISOString(), inFlight: 0 };
  assert.equal(sharedAccountReadyDelay(account, NOW), 0);
  recordSharedCompletion(account, NOW);
  assert.equal(sharedAccountReadyDelay(account, NOW + 1000), 9000);
  assert.equal(sharedAccountReadyDelay(structuredClone(account), NOW + 9999), 1);
  assert.equal(sharedAccountReadyDelay(account, NOW + 10000), 0);
  account.cooldownUntil = new Date(NOW + 30000).toISOString();
  recordSharedCompletion(account, NOW + 1000);
  assert.equal(sharedAccountReadyDelay(account, NOW + 1000), 29000);
  assert.equal(sharedAccountReadyDelay(account, NOW + 30000), 0);
});

test('重复拒绝保持串行，冷却从三十秒增加至两分钟且不缩短已有冷却', () => {
  const account = {};
  for (const [i, delay] of [30000, 60000, 120000, 120000].entries()) {
    const now = NOW + i * 120000;
    recordSharedCapacityError(account, now);
    assert.equal(sharedAccountConcurrency(account, now), 1);
    assert.equal(account.capacityConcurrencyLimit, 1);
    assert.equal(sharedAccountReadyDelay(account, now), delay);
  }
  account.cooldownUntil = new Date(NOW + 1000000).toISOString();
  recordSharedCapacityError(account, NOW);
  assert.equal(sharedAccountReadyDelay(account, NOW), 1000000);
  assert.equal(account.capacityStrikeCount, 4);
});

test('两类失败独立递增，真实成功清零，结束间隔和较长上游截止取最大', () => {
  const account = {};
  for (const [i, delay] of [10000, 20000, 40000, 60000, 60000].entries()) {
    const now = NOW + i * 200000;
    recordSharedCompletion(account, now, { job: { id: `ordinary-${i}` }, kind: 'ordinary' });
    assert.equal(sharedAccountReadyDelay(account, now), delay);
    assert.equal(account.cooldownReason, 'ordinary_failure');
  }
  assert.equal(account.ordinaryStrikeCount, 4);
  recordSharedCapacityError(account, NOW + 1000000, { job: { id: 'capacity' } });
  assert.equal(sharedAccountReadyDelay(account, NOW + 1000000), 30000);
  assert.equal(account.ordinaryStrikeCount, 4); assert.equal(account.capacityStrikeCount, 1);
  recordSharedCompletion(account, NOW + 1200000, { job: { id: 'local-end' } });
  assert.equal(account.ordinaryStrikeCount, 4); assert.equal(account.capacityStrikeCount, 1);
  const upstream = new Date(NOW + 1800000).toISOString();
  recordSharedCompletion(account, NOW + 1400000, { job: { id: 'success' }, kind: 'success', retryAfterUntil: upstream });
  assert.equal(account.cooldownUntil, upstream); assert.equal(account.cooldownReason, 'upstream_retry_after');
  assert.equal(account.ordinaryStrikeCount, 0); assert.equal(account.capacityStrikeCount, 0);
  recordSharedCapacityError(account, NOW + 1401000, { job: { id: 'new-capacity' } });
  assert.equal(account.capacityStrikeCount, 1); assert.equal(account.cooldownUntil, upstream);
  assert.equal(account.cooldownReason, 'upstream_retry_after');
});

test('同尝试重复失败和重启不延长；首次迟到成功可复位，重复成功不能清除后续故障', () => {
  let account = {}, job = { id: 'once', generationAttempt: { id: 'attempt:once:1' } };
  recordSharedCapacityError(account, NOW, { job });
  const first = structuredClone(account);
  ({ account, job } = JSON.parse(JSON.stringify({ account, job })));
  assert.equal(recordSharedCapacityError(account, NOW + 1000, { job }), false);
  assert.deepEqual(account, first);
  recordSharedCompletion(account, NOW + 2000, { job, kind: 'success' });
  assert.equal(account.capacityStrikeCount, 0); assert.equal(account.cooldownUntil, first.cooldownUntil);
  recordSharedCompletion(account, NOW + 3000, { job: { id: 'later' }, kind: 'ordinary' });
  const later = structuredClone(account);
  assert.equal(recordSharedCompletion(account, NOW + 4000, { job, kind: 'success' }), false);
  assert.deepEqual(account, later);
});
