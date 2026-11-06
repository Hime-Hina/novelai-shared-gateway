import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, rm } from 'node:fs/promises';
import path from 'node:path';
import { tmpdir } from 'node:os';
import {
  JsonStore,
  hashObject,
  verifyEventProjections,
} from '../../server/storage/sqlite-store.ts';
import { configureFairQuota, syncFairQuota } from '../../server/quota/fair-share.ts';
import { createGenerationAdmission } from '../../server/generation/admission.ts';
import {
  createGenerationPayloadBudget,
  singleGenerationRequest,
} from '../../server/generation/batch-requests.ts';
import {
  reserveQueuedGeneration,
  completeGenerationCommand,
} from '../../server/generation/execution.ts';
import { generationRequestFingerprint } from '../../server/generation/request-fingerprint.ts';
import { createNativeHttp } from '../../server/protocols/native-http.ts';
import {
  BASE_MODELS,
  NATIVE_DEFAULTS,
  NATIVE_VIBE_MODELS,
  NATIVE_BLUR_VALUES,
  parseNativeImageRequest,
} from '../../server/protocols/native-image.ts';

const png = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=',
  'base64',
);
const input = () => ({
  input: 'synthetic context replay',
  model: 'nai-diffusion-4-5-full',
  parameters: { width: 128, height: 128, steps: 23, seed: -1 },
});

async function fixture(t, legacy = false) {
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-native-context-'));
  let store = new JsonStore(dataDir),
    calls = 0,
    now = Date.now();
  await store.init();
  await mkdir(path.join(dataDir, 'images'));
  t.after(async () => {
    await store.close();
    const target = path.resolve(dataDir);
    assert.equal(path.dirname(target), path.resolve(tmpdir()));
    assert.ok(path.basename(target).startsWith('nai-native-context-'));
    await rm(target, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  const seed = structuredClone(store.db);
  seed.users = ['a', 'b', 'c', 'd'].map((id) => ({
    id,
    token: `STA1N-synthetic-${id}`,
    enabled: true,
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
      quotaCheckedAt: Date.now(),
    },
  ];
  configureFairQuota(
    seed,
    seed.users.map((user) => user.id),
  );
  syncFairQuota(seed);
  const legacyContext = { defaultModel: input().model, defaults: { steps: 23, scale: 9 } };
  if (legacy)
    seed.jobs.push({
      id: 'old-job',
      memberId: 'a',
      userToken: seed.users[0].token,
      status: 'failed',
      error: 'synthetic old failure',
      errorStatus: 502,
    });
  await store.write(seed);
  if (legacy)
    store.persistGenerationIdempotency({
      member_id: 'a',
      operation: 'generate',
      key_hash: hashObject('legacy'),
      fingerprint: generationRequestFingerprint(parseNativeImageRequest(input()), {
        randomSeed: true,
        noCache: true,
      }),
      normalization_context: JSON.stringify(legacyContext),
      job_id: 'old-job',
      request_id: null,
      created_at: new Date().toISOString(),
    });
  await store.enableEventAuthority();
  const budget = createGenerationPayloadBudget(() => store.activeGenerationPayloadBytes());
  const adapter = () =>
    createNativeHttp({
      store,
      timeoutMs: 1000,
      logDiagnostic() {},
      schedule() {},
      admitGeneration: createGenerationAdmission({
        store,
        now: () => now,
        payloadBudget: budget,
        artifactAvailable: async () => true,
        cleanupStaleActiveJobs: async () => {},
        env: {},
      }),
      readJson: async (req) => req.body,
      waitForJobResult: async (id) => {
        let job = store.selectItemById('jobs', id);
        if (job.status === 'failed') return { error: job.error, statusCode: job.errorStatus };
        if (job.status !== 'done') {
          now += 10001;
          const reservation = await reserveQueuedGeneration(store, id, {
            now: () => now,
            env: {},
            dispatchEnabled: () => true,
            physicalAccountLoad: () => 0,
          });
          assert.ok(reservation.job, JSON.stringify(reservation));
          calls++;
          const request = singleGenerationRequest(store.readGenerationRequest(job.requestId), 0);
          await completeGenerationCommand(
            { store, dataDir, now: () => now },
            reservation,
            request,
            {
              buffer: request.operation === 'encode-vibe' ? Buffer.alloc(200, 42) : png,
              mimeType:
                request.operation === 'encode-vibe' ? 'application/octet-stream' : 'image/png',
            },
            { jobId: id },
          );
          job = store.selectItemById('jobs', id);
        }
        return {
          saved: await store.findImage(job.imageId),
          seed: store.readGenerationRequest(job.requestId)?.seed,
        };
      },
      readStoredImage: (image) => readFile(path.join(dataDir, image.file)),
      sendImage(res, status, mime, bytes) {
        Object.assign(res, { status, mime, bytes });
      },
      previews: { watch: () => () => {} },
    });
  const submit = async (body, key, operation) => {
    const res = {
      headers: {},
      setHeader(key, value) {
        this.headers[key] = value;
      },
    };
    const req = {
      headers: { authorization: 'Bearer STA1N-synthetic-a', 'content-type': 'application/json' },
      body,
      generationIdempotencyKey: key,
    };
    if (operation) await adapter().handleAuxiliary(req, res, operation);
    else await adapter().handleImage(req, res);
    return res;
  };
  return {
    get store() {
      return store;
    },
    get calls() {
      return calls;
    },
    submit,
    legacyContext,
    async restart() {
      await store.close();
      store = new JsonStore(dataDir);
      await store.init();
    },
  };
}

test('Native full first context replays defaults, removed models and auxiliary definitions without dispatch or charge', async (t) => {
  const f = await fixture(t),
    body = input();
  const first = await f.submit(body, 'generate'),
    requestId = first.headers['X-Nai-Request-Id'];
  const vibeBody = { image: png.toString('base64'), model: body.model },
    upscaleBody = { image: png.toString('base64'), declared_blur_sigma: 0.3 };
  const vibe = await f.submit(vibeBody, 'vibe', 'encode-vibe'),
    upscale = await f.submit(upscaleBody, 'upscale', 'upscale');
  assert.equal(first.status, 200);
  assert.equal(vibe.status, 201);
  assert.equal(upscale.status, 200);
  assert.equal(f.calls, 3);
  const mapping = f.store.readGenerationIdempotency('a', 'generate', hashObject('generate'));
  assert.equal(JSON.parse(mapping.normalization_context).native.defaults.scale, 5);
  const before = {
    jobs: f.store.countRecords('jobs'),
    ledger: f.store.countRecords('ledger'),
    balance: structuredClone(f.store.db.settings.fairQuota.balances.a),
    attempts: f.store.sqlite
      .prepare("SELECT COUNT(*) n FROM domain_events WHERE event_type = 'DispatchAuthorized'")
      .get().n,
  };
  const original = f.store.readGenerationRequest(requestId),
    defaults = { ...NATIVE_DEFAULTS },
    models = [...BASE_MODELS],
    vibes = [...NATIVE_VIBE_MODELS],
    blurs = [...NATIVE_BLUR_VALUES];
  await f.restart();
  await f.store.update((db) => {
    db.settings.defaults.scale = 9;
  });
  NATIVE_DEFAULTS.scale = 6;
  NATIVE_DEFAULTS.informationExtracted = 0.2;
  BASE_MODELS.delete(body.model);
  NATIVE_VIBE_MODELS.splice(NATIVE_VIBE_MODELS.indexOf(body.model), 1);
  NATIVE_BLUR_VALUES.splice(NATIVE_BLUR_VALUES.indexOf(0.3), 1);
  try {
    const replay = await f.submit(
      { ...body, parameters: { ...body.parameters, stream: 'msgpack' } },
      'generate',
    );
    assert.equal(replay.headers['X-Nai-Request-Id'], requestId);
    assert.deepEqual(replay.bytes, first.bytes);
    assert.deepEqual((await f.submit(vibeBody, 'vibe', 'encode-vibe')).bytes, vibe.bytes);
    assert.deepEqual((await f.submit(upscaleBody, 'upscale', 'upscale')).bytes, upscale.bytes);
    await assert.rejects(
      f.submit({ ...body, parameters: { ...body.parameters, scale: 4 } }, 'generate'),
      (error) => error.statusCode === 409,
    );
    await assert.rejects(
      f.submit({ ...vibeBody, information_extracted: 0.5 }, 'vibe', 'encode-vibe'),
      (error) => error.statusCode === 409,
    );
    await assert.rejects(f.submit(body, 'new-key'), (error) => error.statusCode === 422);
    assert.equal(f.calls, 3);
    assert.deepEqual(f.store.readGenerationRequest(requestId), original);
    assert.equal(f.store.countRecords('jobs'), before.jobs);
    assert.equal(f.store.countRecords('ledger'), before.ledger);
    assert.deepEqual(f.store.db.settings.fairQuota.balances.a, before.balance);
    assert.equal(
      f.store.sqlite
        .prepare("SELECT COUNT(*) n FROM domain_events WHERE event_type = 'DispatchAuthorized'")
        .get().n,
      before.attempts,
    );
    f.store.sqlite.transaction(() => verifyEventProjections(f.store.sqlite))();
  } finally {
    Object.assign(NATIVE_DEFAULTS, defaults);
    BASE_MODELS.clear();
    models.forEach((model) => BASE_MODELS.add(model));
    NATIVE_VIBE_MODELS.splice(0, NATIVE_VIBE_MODELS.length, ...vibes);
    NATIVE_BLUR_VALUES.splice(0, NATIVE_BLUR_VALUES.length, ...blurs);
  }
});

test('old Native finite-default fixtures remain readable without inventing historical Native definitions', async (t) => {
  const f = await fixture(t, true),
    originalScale = NATIVE_DEFAULTS.scale;
  await assert.rejects(
    f.submit(input(), 'legacy'),
    (error) => error.statusCode === 502 && /old failure/.test(error.message),
  );
  NATIVE_DEFAULTS.scale = 6;
  try {
    await assert.rejects(f.submit(input(), 'legacy'), (error) => error.statusCode === 409);
  } finally {
    NATIVE_DEFAULTS.scale = originalScale;
  }
  assert.deepEqual(
    JSON.parse(
      f.store.readGenerationIdempotency('a', 'generate', hashObject('legacy'))
        .normalization_context,
    ),
    f.legacyContext,
  );
  assert.equal(f.calls, 0);
  assert.equal(f.store.countRecords('jobs'), 1);
  assert.equal(f.store.countRecords('ledger'), 0);
});
