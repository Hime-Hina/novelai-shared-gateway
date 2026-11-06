import test from 'node:test';
import assert from 'node:assert/strict';
import { JsonStore } from '../../server/storage/sqlite-store.ts';

test('transient progress never exposes the mutable cached job or persists frames', () => {
  const store = new JsonStore('unused-progress-fixture');
  // JS fixture seeds private state without opening a database; production TS cannot access it.
  store.db = { jobs: [{ id: 'j', status: 'running', request: { steps: 28 } }] };
  store.persistIncremental = () => assert.fail('Progress frames must not persist');
  assert.equal(store.hasActiveJobs(), true);
  const progress = { percent: 25, step: 7 };
  const job = store.updateGenerationProgress('j', progress);
  assert.deepEqual(job.generationProgress, { percent: 25, step: 7, total: 28 });
  progress.percent = 99; job.generationProgress.percent = 100; job.request.steps = 1;
  assert.equal(store.db.jobs[0].generationProgress.percent, 25);
  assert.equal(store.db.jobs[0].request.steps, 28);
  assert.equal(store.updateGenerationProgress('missing', progress), null);
  store.db.jobs[0].status = 'done';
  assert.equal(store.hasActiveJobs(), false);
});
