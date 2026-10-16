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
