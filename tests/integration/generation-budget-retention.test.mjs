import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { JsonStore, verifyEventProjections } from '../../server/store.ts';
import { configureFairGroups, syncFairQuota } from '../../server/fair-quota.ts';
import { createGenerationAdmission, generationIntent } from '../../server/generation-admission.ts';
import { createGenerationPayloadBudget, singleGenerationRequest } from '../../server/generation-request.ts';
import { normalizeMemberGeneration, materializeMemberGeneration, quoteMemberGeneration,
  assertMemberGenerationConfirmation, memberGenerationIntentFingerprint } from '../../server/member-generation.ts';
import { reserveQueuedGeneration, completeGenerationCommand } from '../../server/generation-execution.ts';

const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');

test('retiring a completed child preserves its cold charge in the next child authorization total', async t => {
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-budget-retention-')), store = new JsonStore(dataDir);
  await store.init(); await mkdir(path.join(dataDir, 'images'));
  t.after(async () => {
    await store.close();
    const target = path.resolve(dataDir);
    assert.equal(path.dirname(target), path.resolve(tmpdir()));
    assert.ok(path.basename(target).startsWith('nai-budget-retention-'));
    await rm(target, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  let now = 1800000000000;
  const seed = structuredClone(store.db);
  seed.users = ['a', 'b', 'c', 'd'].map(id => ({ id, token: `synthetic-${id}`, enabled: true, acceptingJobs: true, jobAdmissionVersion: 0 }));
  seed.accounts = [{ id: 'opus', enabled: true, quotaTier: 3, subscriptionActive: true,
    quotaFixed: 100000, quotaPurchased: 0, v5UsagePercent: 80, v5UsageTimeUntilNextPercent: 600, quotaCheckedAt: now }];
  configureFairGroups(seed, seed.users.map((user, index) => ({ id: `group-${user.id}`, members: [user.id],
    share: [0.0005, 0.333, 0.333, 0.3335][index] })), { now });
  syncFairQuota(seed, now); seed.settings.fairQuota.autoAnlasFallback = true;
  await store.write(seed); await store.enableEventAuthority();
  assert.equal(store.db.settings.fairQuota.balances.a.v5Available, '0.04');
  assert.equal(store.db.settings.fairQuota.balances.a.anlasFixedAvailable, '50');
  const admit = createGenerationAdmission({ store, now: () => now, env: {},
    payloadBudget: createGenerationPayloadBudget(() => store.activeGenerationPayloadBytes()),
    artifactAvailable: async () => true, cleanupStaleActiveJobs: async () => {} });
  const dispatch = id => reserveQueuedGeneration(store, id, { now: () => now, env: {}, dispatchEnabled: () => true, physicalAccountLoad: () => 0 });
  const complete = reservation => completeGenerationCommand({ store, dataDir, now: () => now }, reservation,
    singleGenerationRequest(store.readGenerationRequest(reservation.job.requestId), reservation.job.sampleIndex),
    { buffer: png, mimeType: 'image/png' }, { jobId: reservation.job.id });

  // This earlier queued generation can consume the wallet after the later batch was quoted and accepted.
  const earlier = await admit({ identity: { memberId: 'a' }, source: 'native', noCache: true,
    request: { model: 'nai-diffusion-5-full', width: 512, height: 1024, steps: 23, seed: 6, sampleCount: 1 } });
  const input = { operation: 'generate', action: 'generate', model: 'nai-diffusion-5-full', input: 'synthetic batch',
    parameters: { width: 512, height: 512, steps: 23, n_samples: 2, seed: 7 } };
  const initial = normalizeMemberGeneration(input), quote = quoteMemberGeneration(store.db, store.db.users[0], initial, { now, env: {} });
  assert.equal(quote.admission.allowed, true);
  assert.deepEqual(quote.charges.perJob.map(charge => charge.resource), ['v5', 'v5']);
  const confirmation = { requestFingerprint: quote.requestFingerprint, versions: quote.versions,
    maxPerJob: { v5: '1', anlasFixed: '8', anlasPurchased: '0' },
    maxTotal: { v5: '2', anlasFixed: '12', anlasPurchased: '0' } };
  const confirmed = { ...input, confirmation };
  let normalized;
  const intent = generationIntent('retained-batch-budget', context => {
    normalized = normalizeMemberGeneration(confirmed, { normalizationContext: context });
    return { ...materializeMemberGeneration(normalized), clientContract: 'member-generation-v1', confirmation };
  }, { context: () => initial.normalizationContext, noCache: true,
    fingerprint: () => memberGenerationIntentFingerprint(normalized, confirmation),
    validate: (db, member, _request, at) => {
      const current = quoteMemberGeneration(db, member, normalized, { now: at, env: {} });
      assertMemberGenerationConfirmation(confirmation, current);
      assert.equal(current.admission.allowed, true);
    } });
  const accepted = await admit({ identity: { memberId: 'a' }, source: 'member-generation', batch: true, noCache: true, intent });
  const [firstId, secondId] = accepted.jobIds;
  const originalInput = store.sqlite.prepare('SELECT payload FROM generation_requests WHERE id = ?').get(accepted.requestId).payload;
  const earlierReservation = await dispatch(earlier.jobIds[0]);
  assert.equal(earlierReservation.job.fairCharge.resource, 'v5');
  assert.equal(earlierReservation.job.fairCharge.amount, '0.028901734105');
  await complete(earlierReservation);

  now += 10001;
  const first = await dispatch(firstId);
  assert.equal(first.job.fairCharge.resource, 'anlas');
  assert.equal(first.job.fairCharge.billingMode, 'anlas_fallback');
  assert.equal(first.job.fairCharge.fixed, '8');
  await complete(first);
  assert.equal(store.selectItemById('jobs', secondId).status, 'queued');
  assert.deepEqual(await store.retireJobHistory([firstId], 'synthetic_history_retention', now), [firstId]);
  assert.equal(store.selectItemById('jobs', firstId), null, 'the test must actually remove the first query row');
  assert.deepEqual(store.readGenerationRequestState(accepted.requestId, 'a').jobs.map(job => job.id), [secondId]);
  assert.deepEqual(store.readGenerationRequestJobIds(accepted.requestId, 'a'), [firstId, secondId]);
  assert.equal(store.readFairQuotaSettlement(firstId).status, 'settled');
  assert.equal(store.readFairQuotaSettlement(firstId).fixed, '8');
  assert.equal(store.sqlite.prepare('SELECT payload FROM generation_requests WHERE id = ?').get(accepted.requestId).payload, originalInput);

  now += 10001;
  const beforeFixed = store.db.settings.fairQuota.balances.a.anlasFixedAvailable;
  assert.equal(beforeFixed, '42');
  const second = await dispatch(secondId);
  assert.equal(second.skip, true);
  assert.equal(second.statusCode, 409);
  const rejected = store.selectItemById('jobs', secondId);
  assert.equal(rejected.errorCode, 'cost_limit_exceeded', '8 + 8 exceeds the original total 12 despite each child costing at most 8');
  assert.equal(rejected.generationAttempt, undefined);
  assert.equal(rejected.fairCharge, null);
  assert.equal(store.readFairQuotaSettlement(secondId), null);
  assert.equal(store.db.settings.fairQuota.balances.a.anlasFixedAvailable, beforeFixed);
  assert.equal(store.db.accounts[0].inFlight, 0);
  store.sqlite.transaction(() => verifyEventProjections(store.sqlite))();
});
