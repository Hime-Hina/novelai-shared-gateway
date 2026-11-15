import test from 'node:test';
import assert from 'node:assert/strict';
import { fixture, meta, NOW, databaseState } from '../helpers/relational-generation.mjs';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { accountQuotaResult } from '../../server/quota/account-refresh.ts';

test('完整quota DTO复用原恢复估算，held/未分组合法读取，事实/游标/钱包零写', async (t) => {
  const f = await fixture(t, { v5: '10', secondV5: '10', seconds: 600 }),
    quotas = new RelationalQuotaStore(f.db),
    before = databaseState(f.db),
    summary = quotas.readMemberQuotaSummary('m1', NOW + 60000);
  assert.equal(summary.v5Available, 10.05);
  assert.equal(summary.v5Capacity, 50);
  assert.equal(summary.v5RefillPolicy, 'unfilled_members_v1');
  assert.equal(summary.v5SharedBalanceStatus, 'pending');
  assert.equal(databaseState(f.db), before);
  const plan = f.db
    .prepare(
      `EXPLAIN QUERY PLAN SELECT original_member_id,anlas_fixed,anlas_purchased,v5_equivalent
    FROM charges WHERE original_account_id=? AND billing_mode='anlas_fallback' AND status='settled' AND original_member_id=?`,
    )
    .all('a', 'm1');
  assert.match(JSON.stringify(plan), /SEARCH charges USING INDEX idx_charges_fallback_summary/);
  assert.doesNotMatch(JSON.stringify(plan), /SCAN charges/);
  assert.equal(f.generations.readWallet('a', 'm1').v5, '10');
  await f.accept('held');
  f.dispatch('held-0');
  const held = databaseState(f.db),
    busy = quotas.readMemberQuotaSummary('m1', NOW + 60000);
  assert.equal(busy.anlasFixedReserved, 2);
  assert.equal(databaseState(f.db), held);
  const ungrouped = quotas.readMemberQuotaSummary('not-a-current-group-member', NOW + 60000);
  assert.equal(ungrouped.member, false);
  assert.equal(ungrouped.groupId, null);
  assert.equal(ungrouped.v5Capacity, 0);
  assert.equal(ungrouped.anlasAvailable, 0);
  assert.equal(databaseState(f.db), held);
});

test('fallback累计与风险status只读真实原charge/观察来源，不凭active补within_precision', async (t) => {
  const f = await fixture(t, { v5: '0', secondV5: '40', seconds: 0, fallback: true }),
    quotas = new RelationalQuotaStore(f.db);
  await f.accept('fallback-summary', { model: 'nai-diffusion-5-full', steps: 23 });
  f.dispatch('fallback-summary-0');
  await f.publish('fallback-summary-0');
  const charge = f.generations.readCharge('fallback-summary-0'),
    before = databaseState(f.db),
    summary = quotas.readMemberQuotaSummary('m1', NOW + 5);
  assert.equal(summary.localAnlasFallbackFixedSpent, Number(charge.anlasFixed));
  assert.equal(summary.localFallbackV5Equivalent, Number(charge.v5Equivalent));
  assert.equal(summary.v5SharedBalanceStatus, 'pending');
  assert.equal(databaseState(f.db), before);
  const ticket = quotas.beginQuotaObservation('a', NOW + 10),
    q = quotas.readAccountQuota('a'),
    observed = accountQuotaResult(
      'a',
      {
        tier: 3,
        subscriptionActive: true,
        subscriptionExpiresAt: null,
        fixed: q.observedAnlasFixed,
        purchased: q.observedAnlasPurchased,
        v5UsagePercent: '10',
        v5UsageTimeUntilNextPercent: 0,
      },
      new Date(NOW + 10).toISOString(),
    );
  f.db.transaction(() =>
    quotas.recordQuotaObservation(meta('summary-observation', NOW + 11), ticket, observed),
  )();
  const after = databaseState(f.db),
    risk = quotas.readMemberQuotaSummary('m1', NOW + 11);
  assert.equal(risk.v5SharedBalanceStatus, 'source_below_local');
  assert.equal(risk.sharedBalanceRisk.v5, true);
  assert.equal(risk.localAnlasFallbackFixedSpent, Number(charge.anlasFixed));
  assert.equal(databaseState(f.db), after);
});

test('关闭政策保持原null，真实缺失金融基线保持503而不返回假零', async (t) => {
  const f = await fixture(t, { baseline: false }),
    quotas = new RelationalQuotaStore(f.db),
    before = databaseState(f.db);
  assert.throws(
    () => quotas.readMemberQuotaSummary('m1', NOW),
    (error) => error.statusCode === 503,
  );
  assert.equal(databaseState(f.db), before);
  f.db.prepare('UPDATE sharing_policy SET enabled=0').run();
  const disabled = databaseState(f.db);
  assert.equal(quotas.readMemberQuotaSummary('m1', NOW), null);
  assert.equal(databaseState(f.db), disabled);
});
