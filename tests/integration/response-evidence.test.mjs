import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import {
  JsonStore,
  applyEventProjection,
  verifyEventProjections,
} from '../../server/storage/sqlite-store.ts';
import {
  copyProtectedFacts,
  replayDomainEvents,
  readDomainCommits,
} from '../../server/events/event-store.ts';
import { configureFairQuota, syncFairQuota } from '../../server/quota/fair-share.ts';
import { createGenerationAdmission } from '../../server/generation/admission.ts';
import {
  createGenerationPayloadBudget,
  singleGenerationRequest,
} from '../../server/generation/batch-requests.ts';
import {
  reserveQueuedGeneration,
  prepareGenerationResult,
  completeGenerationCommand,
  failGenerationCommand,
  recoverPendingGenerationResultsCommand,
} from '../../server/generation/execution.ts';
import { publishStoredResult } from '../../server/storage/result-files.ts';
import { summarizeOwnerUsage } from '../../server/members/owner-operations.ts';

const now = 1793491200000,
  png = Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=',
    'base64',
  );
const fields = [
  'createdAt',
  'startedAt',
  'completedAt',
  'upstreamStartedAt',
  'upstreamHeadersAt',
  'upstreamCompletedAt',
  'upstreamHttpStatus',
  'fairCharge',
  'generationRefund',
  'lateResultAvailable',
];
async function fixture(t) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'nai-response-')),
    store = new JsonStore(dir);
  await store.init();
  await mkdir(path.join(dir, 'images'));
  t.after(async () => {
    await store.close();
    assert.equal(path.dirname(path.resolve(dir)), path.resolve(os.tmpdir()));
    assert.ok(path.basename(dir).startsWith('nai-response-'));
    await rm(dir, { recursive: true, force: true });
  });
  return store;
}
function queriedJob(store, id) {
  const job = store.selectItemById('jobs', id);
  return Object.fromEntries(
    fields.map((field) => [field, job[field] === '' ? null : (job[field] ?? null)]),
  );
}

test('real admission, reservation, dispatch and result commands preserve business queries through offline replay', async (t) => {
  for (const mode of ['success', 'failure', 'prepared-recovery', 'late']) {
    await t.test(mode, async (t) => {
      const store = await fixture(t),
        seed = structuredClone(store.db);
      seed.users = ['a', 'b', 'c', 'd'].map((id) => ({
        id,
        token: `synthetic-${id}`,
        enabled: true,
        acceptingJobs: true,
        jobAdmissionVersion: 0,
      }));
      seed.accounts = [
        {
          id: 'opus',
          enabled: true,
          quotaTier: 3,
          subscriptionActive: true,
          quotaFixed: 10000,
          quotaPurchased: 0,
          v5UsagePercent: 80,
          quotaCheckedAt: now,
        },
      ];
      configureFairQuota(seed, ['a', 'b', 'c', 'd']);
      syncFairQuota(seed, now);
      await store.write(seed);
      await store.enableEventAuthority();
      const admit = createGenerationAdmission({
        store,
        now: () => now,
        env: {},
        payloadBudget: createGenerationPayloadBudget(() => store.activeGenerationPayloadBytes()),
        artifactAvailable: async () => true,
        cleanupStaleActiveJobs: async () => {},
      });
      const admitted = await admit({
        identity: { memberId: 'a' },
        source: 'api',
        noCache: true,
        request: {
          model: 'nai-diffusion-5-full',
          width: 1024,
          height: 1024,
          steps: 23,
          seed: 7,
          sampleCount: 1,
        },
      });
      const id = admitted.jobIds[0],
        reservation = await reserveQueuedGeneration(store, id, {
          now: () => now + 1000,
          env: {},
          dispatchEnabled: () => true,
          physicalAccountLoad: () => 0,
        });
      assert.ok(reservation.job?.generationAttempt);
      assert.equal(reservation.job.fairCharge.status, 'reserved');
      const request = singleGenerationRequest(store.readGenerationRequest(admitted.requestId), 0),
        image = { buffer: png, mimeType: 'image/png' };
      reservation.upstreamTiming = {
        upstreamStartedAt: new Date(now + 2000).toISOString(),
        upstreamHeadersAt: new Date(now + 13000).toISOString(),
        upstreamCompletedAt: new Date(now + 14000).toISOString(),
        upstreamHttpStatus: mode === 'failure' ? 503 : 200,
      };
      reservation.failurePhase = 'upstream';
      if (mode === 'failure' || mode === 'late') {
        await failGenerationCommand(store, reservation, new Error('synthetic upstream failure'), {
          now: () => now + 15000,
          publicErrorMessage: String,
          errorDetailMessage: String,
        });
      }
      if (mode === 'prepared-recovery') {
        const pending = await prepareGenerationResult(store, reservation, request, image, id, {
          now: () => now + 15000,
        });
        await publishStoredResult(store.dataDir, pending, png);
        const prepared = await fixture(t);
        copyProtectedFacts(store.sqlite, prepared.sqlite);
        replayDomainEvents(store.sqlite, prepared.sqlite, { project: applyEventProjection });
        assert.deepEqual(queriedJob(prepared, id), queriedJob(store, id));
        const recovered = await recoverPendingGenerationResultsCommand({
          store,
          dataDir: store.dataDir,
          now: () => now + 16000,
        });
        assert.equal(recovered.completed, 1);
      } else if (mode !== 'failure') {
        await completeGenerationCommand(
          { store, dataDir: store.dataDir, now: () => now + 16000 },
          reservation,
          request,
          image,
          { jobId: id },
        );
      }
      const before = queriedJob(store, id),
        owner = await store.readOwnerJobRows('', '9999'),
        balance = structuredClone(store.db.settings.fairQuota.balances);
      const settlements = store.sqlite.prepare('SELECT * FROM fair_quota_settlements').all();
      const target = await fixture(t);
      copyProtectedFacts(store.sqlite, target.sqlite);
      const previousFetch = globalThis.fetch;
      let calls = 0;
      globalThis.fetch = async () => {
        calls++;
        throw new Error('replay must not call upstream');
      };
      try {
        replayDomainEvents(store.sqlite, target.sqlite, { project: applyEventProjection });
        replayDomainEvents(store.sqlite, target.sqlite, { project: applyEventProjection });
      } finally {
        globalThis.fetch = previousFetch;
      }
      assert.equal(calls, 0);
      assert.deepEqual(queriedJob(target, id), before);
      assert.deepEqual(
        target.sqlite.prepare('SELECT * FROM fair_quota_settlements').all(),
        settlements,
      );
      assert.deepEqual(
        JSON.parse(
          target.sqlite.prepare("SELECT value FROM app_settings WHERE key='settings'").get().value,
        ).fairQuota.balances,
        balance,
      );
      const rebuiltOwner = await target.readOwnerJobRows('', '9999');
      assert.deepEqual(rebuiltOwner, owner);
      assert.deepEqual(summarizeOwnerUsage(rebuiltOwner), summarizeOwnerUsage(owner));
      assert.equal(
        summarizeOwnerUsage(owner).totals.averageGenerationSeconds,
        ['success', 'prepared-recovery'].includes(mode) ? 12 : null,
      );
      assert.equal(before.upstreamHttpStatus, mode === 'failure' ? 503 : 200);
      if (mode === 'late') {
        assert.equal(before.fairCharge.status, 'refunded');
        assert.equal(before.lateResultAvailable, true);
      }
      assert.ok(
        readDomainCommits(store.sqlite)
          .flatMap((commit) => commit.events)
          .some((event) => event.eventType === 'DispatchAuthorized'),
      );
      target.sqlite.transaction(() => verifyEventProjections(target.sqlite))();
    });
  }
});
