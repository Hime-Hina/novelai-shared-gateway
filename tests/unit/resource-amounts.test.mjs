import test from 'node:test';
import assert from 'node:assert/strict';
import {
  AMOUNT_SCALE,
  parseAmount,
  formatAmount,
  allocateAmount,
  divideAmount,
} from '../../server/quota/resource-amounts.ts';

test('12位定点保持大余额最小单位，拒绝静默超精度', () => {
  const amount = parseAmount('10000.000000000001');
  assert.equal(formatAmount(amount - parseAmount('10000')), '0.000000000001');
  assert.equal(formatAmount(parseAmount(1e-12)), '0.000000000001');
  assert.equal(formatAmount(parseAmount('001.230000000000')), '1.23');
  assert.throws(() => parseAmount('0.0000000000001'), RangeError);
  for (const value of [NaN, Infinity, '', ' 1', 'true', '1x'])
    assert.throws(() => parseAmount(value));
  assert.equal(parseAmount('0.0000000000001', 'ceil'), 1n);
  assert.equal(parseAmount('-0.0000000000001', 'floor'), -1n);
  assert.equal(divideAmount(-3n, 2n, 'nearest'), -2n);
});
test('最大余数按稳定成员ID分配，零权重不分配且总额精确守恒', () => {
  assert.deepEqual(
    allocateAmount(2n, [
      { id: 'c', weight: 1n },
      { id: 'a', weight: 1n },
      { id: 'b', weight: 1n },
    ]),
    { c: 0n, a: 1n, b: 1n },
  );
  const allocation = allocateAmount(10_000n * AMOUNT_SCALE, [
    { id: 'a', weight: 1n },
    { id: 'b', weight: 2n },
    { id: 'c', weight: 0n },
  ]);
  assert.equal(allocation.a + allocation.b + allocation.c, 10_000n * AMOUNT_SCALE);
  assert.equal(formatAmount(allocation.a), '3333.333333333333');
  assert.equal(formatAmount(allocation.b), '6666.666666666667');
  assert.equal(allocation.c, 0n);
});
