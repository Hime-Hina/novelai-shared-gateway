import test from 'node:test';
import assert from 'node:assert/strict';
import { activityDate, activityRange, readActivityHeatmap } from '../../server/activity-heatmap.js';

test('activity dates use Taipei submission days and reject ambiguous or invalid timestamps', () => {
  assert.equal(activityDate('2026-10-16T15:59:59.999Z'), '2026-10-16');
  assert.equal(activityDate('2026-10-16T16:00:00Z'), '2026-10-17');
  assert.equal(activityDate('2026-10-17T00:00:00+08:00'), '2026-10-17');
  assert.equal(activityDate('2024-02-29T23:30:00-08:00'), '2024-03-01');
  for (const value of [null, '', '2026-10-17', '2026-10-17T12:00:00', '2026-02-30T12:00:00Z', '2026-10-17T24:01:00Z', '2026-10-17T12:00:00+24:00', 'not-a-date']) assert.equal(activityDate(value), null, String(value));
});

test('recent calendar year includes today and correctly spans leap day and midnight', () => {
  assert.deepEqual(activityRange(Date.parse('2026-10-16T16:00:00Z')), {
    from: '2025-10-18', to: '2026-10-17', since: '2025-10-17T16:00:00.000Z', until: '2026-10-17T16:00:00.000Z'
  });
  const leap = activityRange(Date.parse('2024-02-29T12:00:00Z'));
  assert.equal(leap.from, '2023-03-01'); assert.equal(leap.to, '2024-02-29');
  assert.equal((Date.parse(leap.until) - Date.parse(leap.since)) / 86400000, 366);
  const next = activityRange(Date.parse('2025-02-28T12:00:00Z'));
  assert.equal(next.from, '2024-02-29'); assert.equal(next.to, '2025-02-28');
  assert.equal(activityRange(Date.parse('2026-10-16T15:59:59Z')).to, '2026-10-16');
});

test('activity query keeps authorized member scope and never fills missing historical days with zero', async () => {
  let query;
  const days = [{ date: '2026-10-17', total: 4, done: 1, failed: 1, cancelled: 1, pending: 1, queued: 0, running: 0, other: 0 }];
  const store = { readActivityDays: async value => { query = value; return days; } };
  const now = Date.parse('2026-10-17T02:00:00Z');
  const result = await readActivityHeatmap(store, new URLSearchParams(), { memberId: 'alice' }, now);
  assert.equal(query.memberId, 'alice'); assert.equal(query.operation, 'generate'); assert.equal(query.dates, null);
  assert.equal(result.coverage, 'retained-records'); assert.deepEqual(result.days, days); assert.equal(result.days.length, 1);
  await assert.rejects(readActivityHeatmap(store, new URLSearchParams(), {}, now), { statusCode: 400 });
  await assert.rejects(readActivityHeatmap(store, new URLSearchParams('memberId=bob'), { memberId: 'alice' }, now), { statusCode: 400 });
  await readActivityHeatmap(store, new URLSearchParams('memberId=deleted-member&operation=upscale&dates=2026-10-17,2026-10-16,2026-10-17'), { allowMemberFilter: true }, now);
  assert.equal(query.memberId, 'deleted-member'); assert.equal(query.operation, 'upscale');
  assert.deepEqual(query.dates, ['2026-10-16', '2026-10-17']);
  await readActivityHeatmap(store, new URLSearchParams('operation=encode-vibe'), { allowMemberFilter: true }, now);
  assert.equal(query.memberId, null); assert.equal(query.operation, 'encode-vibe');
});

test('activity query rejects unsupported operations, repeated fields, invalid and out-of-window days before reading', async () => {
  const store = { readActivityDays: () => assert.fail('invalid query reached storage') };
  for (const value of ['operation=unknown', 'operation=generate&operation=upscale', 'dates=', 'dates=2026-02-30', 'dates=2026-10-18', 'dates=2025-10-17', 'dates=2026-10-17&dates=2026-10-16', 'memberId=a&memberId=b']) {
    await assert.rejects(readActivityHeatmap(store, new URLSearchParams(value), { allowMemberFilter: true }, Date.parse('2026-10-17T02:00:00Z')), { statusCode: 400 }, value);
  }
});
