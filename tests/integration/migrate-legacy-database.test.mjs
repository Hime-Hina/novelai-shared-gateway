import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, writeFile, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import Database from 'better-sqlite3';
import { JsonStore } from '../../server/storage/sqlite-store.ts';
import { pathToFileURL } from 'node:url';
const entry = process.env.MIGRATE_LEGACY_TEST_ENTRY;
if (entry && !path.isAbsolute(entry)) {
  throw new Error('MIGRATE_LEGACY_TEST_ENTRY must name an absolute compiled file.');
}
const TargetDatabase = entry ? createRequire(entry)('better-sqlite3') : Database;
const {
  migrateLegacyDatabase,
  proveLegacyGenerationIntent,
  readLegacyCutConfiguration,
  migrationReferences,
  mapLegacyMembers,
  mapLegacyAccounts,
  mapLegacySharing,
} = await import(
  entry
    ? pathToFileURL(entry).href
    : new URL('../../scripts/migrate-legacy-database.mjs', import.meta.url).href
);

import {
  nativeNormalizationContext,
  parseNativeImageRequest,
  parseNativeAuxiliaryRequest,
} from '../../server/protocols/native-image.ts';
import { generationRequestFingerprint } from '../../server/generation/request-fingerprint.ts';
import { configureFairQuota, syncFairQuota } from '../../server/quota/fair-share.ts';
import {
  recordSharedCapacityError,
  recordSharedCompletion,
  sharedAccountConcurrency,
  sharedAccountReadyDelay,
} from '../../server/quota/shared-capacity.ts';
import {
  createGenerationAdmission,
  generationIntent,
  generationIdempotencyKeyHash,
} from '../../server/generation/admission.ts';
import { createGenerationPayloadBudget } from '../../server/generation/payload-budget.ts';
import {
  cancelGenerationRequestCommand,
  reserveQueuedGeneration,
  completeGenerationCommand,
} from '../../server/generation/execution.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalConfigurationStore } from '../../server/storage/relational-configuration-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { domainDigest } from '../../server/events/domain-events.ts';
import { assertLegacyDatabaseProjection } from '../../scripts/reconcile-legacy-database.mjs';
import { databaseState } from '../helpers/relational-generation.mjs';
import { readStreamProjection } from '../../server/events/event-store.ts';
import { relationalPrivateStateEmpty } from '../../server/storage/relational-private-restore.ts';

const hash = (bytes) => createHash('sha256').update(bytes).digest('hex'),
  sourceCommit = '3caba3dfa0b5d9f582acabfc02e36e7db4af038f';

async function fixture(
  t,
  status = 'failed',
  parent = null,
  artifact = null,
  memberPatch = {},
  jobPatch = {},
  accounts = [],
) {
  const directory = await mkdtemp(path.join(tmpdir(), 'nai-migrate-legacy-')),
    store = new JsonStore(path.join(directory, 'old')),
    sourceArtifacts = [].concat(artifact ?? []);
  t.after(async () => {
    await store.close();
    await rm(directory, { recursive: true, force: true });
  });
  await store.init();
  await store.write({
    ...store.db,
    users: [
      {
        id: 'member',
        name: '',
        token: 'STA1N-synthetic-current-key',
        enabled: true,
        createdAt: 0,
        ...memberPatch,
      },
    ],
    images: sourceArtifacts,
    accounts,
    jobs: [
      {
        id: 'job',
        memberId: 'member',
        userToken: 'STA1N-synthetic-current-key',
        status,
        ...(sourceArtifacts.length ? { imageId: sourceArtifacts[0].id } : {}),
        source: 'novelai-native',
        createdAt: 0,
        request: { model: 'nai-diffusion-4-5-full', prompt: '', scale: 0 },
        ...jobPatch,
      },
    ],
  });
  let originalReceipt = null,
    completedImage = null;
  const acceptedAt = Date.UTC(2026, 10, 1);
  if (parent) {
    const seed = structuredClone(store.db);
    seed.users = ['member', 'second', 'third', 'fourth'].map((id) => ({
      id,
      name: '',
      token: id === 'member' ? 'STA1N-synthetic-current-key' : `synthetic-${id}`,
      enabled: true,
      createdAt: 0,
    }));
    seed.accounts = [
      {
        id: 'original-account',
        token: 'pst-synthetic-source-only',
        enabled: true,
        quotaTier: 3,
        subscriptionActive: true,
        quotaFixed: 10000,
        quotaPurchased: 0,
        v5UsagePercent: 80,
        quotaCheckedAt: acceptedAt,
      },
    ];
    seed.jobs = [];
    configureFairQuota(
      seed,
      seed.users.map((user) => user.id),
    );
    syncFairQuota(seed, acceptedAt);
    await store.write(seed);
    if (parent.running || parent.completed) {
      await store.enableEventAuthority();
    }
    const admit = createGenerationAdmission({
      store,
      now: () => acceptedAt,
      env: {},
      payloadBudget: createGenerationPayloadBudget(() => store.activeGenerationPayloadBytes()),
      artifactAvailable: async () =>
        assert.fail('noCache fixture must not read external artifacts'),
      cleanupStaleActiveJobs: async () => {},
    });
    originalReceipt = await admit({
      identity: { token: 'STA1N-synthetic-current-key' },
      request: null,
      source: 'novelai-native',
      batch: !parent.alias,
      noCache: !parent.alias,
      admissionVersion: 0,
      intent: generationIntent('synthetic-original-key', () => parent.request, {
        operation: parent.request.operation ?? 'generate',
        randomSeed: false,
        noCache: !parent.alias,
        context: () => parent.context ?? { native: parent.native },
      }),
    });
    if (parent.alias) {
      const alias = await admit({
        identity: { token: 'STA1N-synthetic-current-key' },
        request: null,
        source: 'novelai-native',
        batch: false,
        noCache: false,
        admissionVersion: 0,
        intent: generationIntent('synthetic-alias-key', () => parent.request, {
          operation: 'generate',
          randomSeed: false,
          noCache: false,
          context: () => ({ native: parent.aliasNative ?? parent.native }),
        }),
      });
      assert.equal(alias.requestId, originalReceipt.requestId);
      assert.deepEqual(alias.jobIds, originalReceipt.jobIds);
    }
    if (parent.running || parent.completed) {
      const reservation = await reserveQueuedGeneration(store, originalReceipt.jobIds[0], {
        now: () => acceptedAt + 1,
        env: {},
        dispatchEnabled: () => true,
        physicalAccountLoad: () => 0,
      });
      assert.equal(reservation.job?.status, 'running');
      if (parent.completed) {
        await mkdir(path.join(store.dataDir, 'images'));
        reservation.upstreamTiming = {
          upstreamStartedAt: new Date(acceptedAt + 1).toISOString(),
          upstreamCompletedAt: new Date(acceptedAt + 2).toISOString(),
          upstreamHttpStatus: 200,
        };
        completedImage = await completeGenerationCommand(
          {
            store,
            dataDir: store.dataDir,
            now: () => acceptedAt + 3,
            createId: () => 'modern-published',
          },
          reservation,
          parent.request,
          {
            buffer: await readFile(new URL('../fixtures/synthetic.webp', import.meta.url)),
            mimeType: 'image/webp',
          },
          { jobId: originalReceipt.jobIds[0] },
        );
      }
    } else {
      await cancelGenerationRequestCommand(store, originalReceipt.requestId, 'member', {
        now: () => acceptedAt + 1,
      });
    }
  }
  if (parent?.retired) {
    assert.deepEqual(
      await store.retireJobHistory(
        originalReceipt.jobIds,
        'synthetic-original-retired',
        acceptedAt + 2,
      ),
      originalReceipt.jobIds,
    );
  }
  if (!parent?.running && !parent?.completed) {
    await store.enableEventAuthority();
  }
  const source = path.join(directory, 'source.sqlite'),
    captureStartedAt = Date.now();
  await store.sqlite.backup(source);
  const db = new Database(source);
  db.pragma('journal_mode=DELETE');
  // Synthetic producer-shaped evidence: real source bytes/cut and exact exports,
  // externally selected below. Official packaged-producer integration is separate.
  const inputRows = db
      .prepare('SELECT id,member_id,payload FROM generation_requests ORDER BY id')
      .all(),
    contextRows = db
      .prepare('SELECT context_ref,payload FROM generation_contexts ORDER BY context_ref')
      .all(),
    diagnosticRows = db
      .prepare(
        'SELECT diagnostic_id,job_id,detail_sha256 FROM generation_diagnostics ORDER BY diagnostic_id',
      )
      .all(),
    inputBytes = Buffer.from(
      [
        ...inputRows.map(
          (row) =>
            JSON.stringify({
              kind: 'request',
              id: row.id,
              member_id: row.member_id,
              sha256: hash(row.payload),
            }) + '\n',
        ),
        ...contextRows.map(
          (row) =>
            JSON.stringify({
              kind: 'context',
              ref: row.context_ref,
              sha256: domainDigest(JSON.parse(row.payload)),
            }) + '\n',
        ),
        ...diagnosticRows.map(
          (row) =>
            JSON.stringify({
              kind: 'diagnostic',
              id: row.diagnostic_id,
              jobId: row.job_id,
              sha256: row.detail_sha256,
            }) + '\n',
        ),
      ].join(''),
    ),
    artifactRows = db.prepare('SELECT id,data FROM images ORDER BY id').all(),
    artifactBytes = Buffer.from(
      artifactRows
        .map((row) => {
          const image = JSON.parse(row.data);
          return (
            JSON.stringify({
              id: row.id,
              ...Object.fromEntries(
                [
                  'jobId',
                  'memberId',
                  'operation',
                  'mimeType',
                  'byteLength',
                  'sha256',
                  'file',
                  'createdAt',
                ].map((key) => [key, image[key] ?? null]),
              ),
              r2: null,
              pikpak: null,
              externalBytesChecked: false,
            }) + '\n'
          );
        })
        .join(''),
    ),
    cut = db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM event_commits').get().seq,
    opening = db.prepare("SELECT value FROM app_meta WHERE key='event_opening'").get().value,
    events = db.prepare('SELECT COUNT(*) AS n FROM domain_events').get().n;
  db.close();
  const sourceBytes = await readFile(source),
    sourceSha256 = hash(sourceBytes),
    exports = path.join(directory, 'exports');
  await mkdir(exports);
  const tuple = (basename, bytes, rowCount) => ({
      basename,
      byteLength: bytes.length,
      rowCount,
      sha256: hash(bytes),
    }),
    value = {
      format: 'legacy-archive-audit-v1',
      sourceCommit,
      toolSha256: hash(
        await readFile(new URL('../../scripts/audit-legacy-archive.mjs', import.meta.url)),
      ),
      sourceSha256,
      openingSha256: hash(opening),
      lastCommitSeq: cut,
      commits: Number(cut),
      events,
      protectedInputs: inputRows.length,
      protectedInputBytes: inputRows.reduce((sum, row) => sum + Buffer.byteLength(row.payload), 0),
      contexts: contextRows.length,
      artifactInputs: 0,
      diagnostics: diagnosticRows.length,
      artifacts: artifactRows.length,
      networkCalls: 0,
      dispatches: 0,
      externalObjectBytesVerified: false,
      exports: {
        inputs: tuple(
          'inputs.jsonl',
          inputBytes,
          inputRows.length + contextRows.length + diagnosticRows.length,
        ),
        artifacts: tuple('artifacts.jsonl', artifactBytes, artifactRows.length),
      },
    },
    audit = path.join(exports, 'audit.json'),
    auditBytes = Buffer.from(JSON.stringify(value));
  await writeFile(audit, auditBytes);
  await writeFile(path.join(exports, 'inputs.jsonl'), inputBytes);
  await writeFile(path.join(exports, 'artifacts.jsonl'), artifactBytes);
  return {
    directory,
    sourceBytes,
    originalReceipt,
    completedImage,
    originalDataDir: store.dataDir,
    acceptedAt,
    captureStartedAt,
    options: {
      source,
      sourceSha256,
      sourceCommit,
      cut,
      audit,
      auditSha256: hash(auditBytes),
      artifacts: path.join(exports, 'artifacts.jsonl'),
      output: path.join(directory, 'target'),
    },
  };
}

test('B困难切片保留parentless/原NULL/真实0，并由完整公共提交重建且不复活认证', async (t) => {
  const f = await fixture(t),
    result = await migrateLegacyDatabase(f.options);
  assert.equal(result.status, 'building');
  assert.equal(result.readyInstalled, false);
  assert.deepEqual(result.imported, { members: 1, jobs: 1 });
  const db = new Database(path.join(f.options.output, 'library.sqlite')),
    replay = new Database(':memory:');
  try {
    assert.equal(db.prepare('SELECT status FROM projection_state').get().status, 'building');
    const request = db.prepare('SELECT * FROM generation_requests').get(),
      job = db.prepare('SELECT * FROM generation_jobs').get(),
      input = db.prepare('SELECT * FROM generation_inputs').get();
    assert.equal(new RelationalInputStore(db).readOriginalRequestId(request.id), null);
    assert.equal(request.sample_count, null);
    assert.equal(request.accepted_at, null);
    assert.equal(job.completed_at, null);
    assert.equal(job.actual_seed, null);
    assert.equal(input.completeness, 'partial');
    assert.equal(JSON.parse(input.body).parameters.scale, 0);
    assert.equal(JSON.parse(input.body).input, '');
    assert.equal(relationalPrivateStateEmpty(db), true);
    const history = db.prepare('SELECT history_id FROM projection_state').get().history_id;
    initializeRelationalStorage(replay, history);
    const replayer = new RelationalCommitReplayer(replay),
      inputs = new RelationalInputStore(db);
    for (const { seq } of db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all()) {
      replay.transaction(() => replayer.replayCommit(readRelationalCommit(db, seq), inputs))();
    }
    assert.deepEqual(
      replay.prepare('SELECT * FROM generation_inputs').all(),
      db.prepare('SELECT * FROM generation_inputs').all(),
    );
    assert.deepEqual(
      replay.prepare('SELECT * FROM generation_jobs').all(),
      db.prepare('SELECT * FROM generation_jobs').all(),
    );
    assert.equal(relationalPrivateStateEmpty(replay), true);
  } finally {
    replay.close();
    db.close();
  }
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('外部源pin、cut或导出正文不匹配在创建目标前拒绝', async (t) => {
  const f = await fixture(t);
  await assert.rejects(migrateLegacyDatabase({ ...f.options, sourceSha256: '0'.repeat(64) }));
  await assert.rejects(
    migrateLegacyDatabase({ ...f.options, cut: String(BigInt(f.options.cut) + 1n) }),
  );
  const input = path.join(path.dirname(f.options.artifacts), 'inputs.jsonl'),
    bytes = await readFile(input);
  bytes[1] ^= 1;
  await writeFile(input, bytes);
  await assert.rejects(migrateLegacyDatabase(f.options));
  await assert.rejects(readFile(path.join(f.options.output, 'library.sqlite')), { code: 'ENOENT' });
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('无法证明活动责任不丢弃为成功迁移，明确building和原目标范围', async (t) => {
  const f = await fixture(t, 'queued'),
    result = await migrateLegacyDatabase(f.options);
  assert.equal(result.status, 'building');
  assert.equal(result.readyInstalled, false);
  assert.deepEqual(result.imported, { members: 1, jobs: 0 });
  assert.ok(
    result.findings.some(
      (row) => row.code === 'JOB_REQUIRES_COMPLETE_OPENING_MAPPING' && row.originalId === 'job',
    ),
  );
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('原capacity owner保留的并发标量不冒充旧截止，成功后真实0和原冷却完整开账', async (t) => {
  const account = {
    id: 'original-capacity-account',
    token: 'pst-synthetic-capacity-source',
    enabled: true,
  };
  recordSharedCapacityError(account, 0, { job: { id: 'capacity-failed' } });
  recordSharedCompletion(account, 100000, { job: { id: 'capacity-recovered' }, kind: 'success' });
  assert.equal(account.capacityConcurrencyLimit, 1);
  assert.equal(Object.hasOwn(account, 'capacityLimitedUntil'), false);
  assert.equal(sharedAccountConcurrency(), 1);
  const f = await fixture(t, 'failed', null, null, {}, {}, [account]),
    result = await migrateLegacyDatabase(f.options),
    target = new Database(path.join(f.options.output, 'library.sqlite'));
  try {
    assert.equal(
      result.findings.some(
        (finding) => finding.code === 'ACCOUNT_CAPACITY_SOURCE_REQUIRES_MAPPING',
      ),
      false,
    );
    const row = target
      .prepare(
        'SELECT cooldown_until,cooldown_reason,ordinary_strikes,capacity_strikes,capacity_limited_until,capacity_last_error_at FROM upstream_accounts WHERE id=?',
      )
      .get(account.id);
    assert.deepEqual(row, {
      cooldown_until: 110000,
      cooldown_reason: 'success_gap',
      ordinary_strikes: 0,
      capacity_strikes: 0,
      capacity_limited_until: null,
      capacity_last_error_at: 0,
    });
    assert.equal(
      sharedAccountReadyDelay({ cooldownUntil: row.cooldown_until }, 100000),
      sharedAccountReadyDelay(account, 100000),
    );
    assert.equal(result.readyInstalled, false);
  } finally {
    target.close();
  }
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('原键按其保存定义逐个证明首次意图，缺定义或错摘要不能借当前defaults变live', () => {
  const native = nativeNormalizationContext();
  native.defaults.scale = 9;
  const request = parseNativeImageRequest(
      {
        input: 'original protected prompt',
        model: 'nai-diffusion-4-5-full',
        parameters: { width: 1024, height: 1024, steps: 23, seed: 73, n_samples: 2 },
      },
      native,
    ),
    mapping = {
      operation: 'generate',
      fingerprint: generationRequestFingerprint(request, { noCache: true, randomSeed: true }),
    },
    before = structuredClone({ request, native, mapping }),
    proof = proveLegacyGenerationIntent(request, { native }, mapping);
  assert.equal(proof.reason, undefined);
  assert.equal(proof.seedMode, 'random');
  assert.equal(proof.noCache, true);
  assert.equal(proof.prepared.sampleCount, 2);
  assert.equal(proof.prepared.firstSeed, 73);
  assert.equal(proof.prepared.body.parameters.scale, 9);
  assert.equal(proof.context.native.defaults.scale, 9);
  assert.equal(
    proveLegacyGenerationIntent(request, {}, mapping).reason,
    'FIRST_DEFINITION_UNPROVEN',
  );
  assert.equal(
    proveLegacyGenerationIntent(request, { native }, { ...mapping, fingerprint: '0'.repeat(64) })
      .reason,
    'FIRST_INTENT_FINGERPRINT_UNPROVEN',
  );
  const missingCount = { ...request };
  delete missingCount.sampleCount;
  assert.equal(
    proveLegacyGenerationIntent(missingCount, { native }, mapping).reason,
    'FIRST_INTENT_FIELDS_UNPROVEN',
  );
  assert.deepEqual({ request, native, mapping }, before);
});

test('真实旧parent的两个child沿原定义导入唯一complete输入/context，原时点与seed不借导入时钟', async (t) => {
  const native = nativeNormalizationContext();
  native.defaults.scale = 9;
  const request = parseNativeImageRequest(
      {
        input: 'original parent',
        model: 'nai-diffusion-4-5-full',
        parameters: { width: 1024, height: 1024, steps: 23, seed: 73, n_samples: 2 },
      },
      native,
    ),
    f = await fixture(t, 'failed', { request, native }),
    result = await migrateLegacyDatabase(f.options);
  assert.deepEqual(result.imported, { members: 4, jobs: 2 });
  const target = new Database(path.join(f.options.output, 'library.sqlite'));
  try {
    const inputs = new RelationalInputStore(target),
      parent = target.prepare('SELECT * FROM generation_requests').get();
    assert.equal(inputs.readOriginalRequestId(parent.id), f.originalReceipt.requestId);
    assert.equal(parent.sample_count, 2);
    assert.equal(parent.accepted_at, f.acceptedAt);
    assert.equal(parent.operation, 'generate');
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM generation_inputs').get().n, 1);
    assert.equal(
      target
        .prepare(
          "SELECT COUNT(*) AS n FROM normalization_contexts WHERE json_extract(body,'$.kind')='native'",
        )
        .get().n,
      1,
    );
    assert.equal(
      target
        .prepare(
          "SELECT COUNT(*) AS n FROM normalization_contexts WHERE json_extract(body,'$.kind')='normalization-defaults'",
        )
        .get().n,
      1,
    );
    assert.equal(
      new RelationalConfigurationStore(target).readCurrent().normalizationDefaults.defaults.scale,
      6,
    );
    const input = target.prepare('SELECT * FROM generation_inputs').get();
    assert.equal(input.completeness, 'complete');
    assert.equal(JSON.parse(input.body).parameters.scale, 9);
    assert.deepEqual(
      target
        .prepare(
          'SELECT id,sample_index,actual_seed,completed_at FROM generation_jobs ORDER BY sample_index',
        )
        .all(),
      [
        {
          id: f.originalReceipt.jobIds[0],
          sample_index: 0,
          actual_seed: 73,
          completed_at: f.acceptedAt + 1,
        },
        {
          id: f.originalReceipt.jobIds[1],
          sample_index: 1,
          actual_seed: 74,
          completed_at: f.acceptedAt + 1,
        },
      ],
    );
    assert.equal(relationalPrivateStateEmpty(target), true);
    const opening = inputs.readOpeningGenerationIdempotency(
      'member',
      'generate',
      generationIdempotencyKeyHash('synthetic-original-key'),
    );
    assert.equal(opening.mapping.resolution, 'live');
    assert.equal(opening.event.payload.originalRequestId, f.originalReceipt.requestId);
    assert.equal(typeof opening.event.payload.originalCommandId, 'string');
    assert.deepEqual(opening.receipt, {
      requestId: f.originalReceipt.requestId,
      jobIds: f.originalReceipt.jobIds,
    });
  } finally {
    target.close();
  }
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('原已保存完整参数但首次定义缺失的旧键保留原job/receipt来源，明确archived而不变live', async (t) => {
  const native = nativeNormalizationContext(),
    request = parseNativeImageRequest(
      {
        input: 'old unproven intent',
        model: 'nai-diffusion-4-5-full',
        parameters: { width: 1024, height: 1024, steps: 23, seed: 73, n_samples: 2 },
      },
      native,
    ),
    f = await fixture(t, 'failed', { request, native, context: {} }),
    result = await migrateLegacyDatabase(f.options),
    target = new Database(path.join(f.options.output, 'library.sqlite'));
  try {
    const inputs = new RelationalInputStore(target),
      key = inputs.readOpeningGenerationIdempotency(
        'member',
        'generate',
        generationIdempotencyKeyHash('synthetic-original-key'),
      );
    assert.equal(key.mapping.resolution, 'archived');
    assert.equal(key.event.payload.originalJobId, f.originalReceipt.jobIds[0]);
    assert.equal(key.event.payload.originalRequestId, f.originalReceipt.requestId);
    assert.equal(key.mapping.contextId, null);
    assert.equal(key.event.payload.seedMode, null);
    assert.equal(key.event.payload.noCache, null);
    assert.equal(
      target.prepare('SELECT completeness FROM generation_inputs').get().completeness,
      'partial',
    );
    assert.equal(result.readyInstalled, false);
    assert.equal(relationalPrivateStateEmpty(target), true);
  } finally {
    target.close();
  }
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

async function selectedCutConfiguration(f, localOriginal = null) {
  const proofs = {};
  for (const name of ['sourceCapture', 'namespaceReceipt', 'identityReceipt']) {
    const file = path.join(f.directory, name + '.json'),
      bytes = Buffer.from(JSON.stringify({ syntheticProof: name }));
    await writeFile(file, bytes);
    proofs[name] = { file, sha256: hash(bytes) };
  }
  const captured = f.captureStartedAt,
    value = {
      format: 'novelai-legacy-cut-config-v1',
      sourceSha256: f.options.sourceSha256,
      cut: f.options.cut,
      sourceCommit,
      captureStartedAt: new Date(captured).toISOString(),
      configuredOwnerUserId: 'synthetic-discord-owner',
      configuredGuildId: 'synthetic-guild',
      discordLoginEnabled: true,
      sourceNamespaces: {
        dataDir: '/remote/original-data',
        r2AccountId: null,
        r2Bucket: null,
        pikpakUrl: null,
      },
      ...proofs,
      localOriginal,
    },
    file = path.join(f.directory, 'selected-cut-config.json'),
    bytes = Buffer.from(JSON.stringify(value));
  await writeFile(file, bytes);
  return { captured, value, file, bytes, proofs };
}

test('唯一选定cut-config核对源/cut及原证明文件；旧remote DATA_DIR不变成本地输出或current ENV', async (t) => {
  const f = await fixture(t),
    { captured, file, bytes, proofs } = await selectedCutConfiguration(f);
  const selected = await readLegacyCutConfiguration(file, hash(bytes), f.options, captured + 1);
  assert.equal(selected.sourceNamespaces.dataDir, '/remote/original-data');
  assert.equal(selected.effectiveAt, captured + 1);
  assert.equal(selected.configuredGuildId, 'synthetic-guild');
  await assert.rejects(readLegacyCutConfiguration(file, '0'.repeat(64), f.options, captured + 1));
  await assert.rejects(
    readLegacyCutConfiguration(
      file,
      hash(bytes),
      { ...f.options, cut: String(BigInt(f.options.cut) + 1n) },
      captured + 1,
    ),
  );
  await assert.rejects(readLegacyCutConfiguration(file, hash(bytes), f.options, captured));
  const migrated = await migrateLegacyDatabase({
      ...f.options,
      cutConfig: file,
      cutConfigSha256: hash(bytes),
    }),
    target = new Database(path.join(f.options.output, 'library.sqlite')),
    rebuilt = new Database(':memory:');
  try {
    assert.equal(migrated.private.apiKeys, 1);
    assert.equal(migrated.private.sessions, 0);
    assert.equal(
      target.prepare('SELECT recoverable_key FROM api_keys').get().recoverable_key,
      'STA1N-synthetic-current-key',
    );
    assert.equal(
      typeof target.prepare('SELECT auth_epoch FROM members').get().auth_epoch,
      'string',
    );
    initializeRelationalStorage(
      rebuilt,
      target.prepare('SELECT history_id FROM projection_state').get().history_id,
    );
    const replayer = new RelationalCommitReplayer(rebuilt),
      inputs = new RelationalInputStore(target);
    for (const { seq } of target
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all()) {
      rebuilt.transaction(() => replayer.replayCommit(readRelationalCommit(target, seq), inputs))();
    }
    assert.equal(relationalPrivateStateEmpty(rebuilt), true);
    assert.equal(rebuilt.prepare('SELECT auth_epoch FROM members').get().auth_epoch, null);
    assert.equal(migrated.readyInstalled, false);
  } finally {
    target.close();
    rebuilt.close();
  }

  await writeFile(proofs.identityReceipt.file, 'changed original owner proof');
  await assert.rejects(readLegacyCutConfiguration(file, hash(bytes), f.options, captured + 1));
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('真实在途与waiting同parent保留原尝试/三池预扣，同笔Job→Attempt→Charge且完整回放不造历史epoch', async (t) => {
  const native = nativeNormalizationContext(),
    request = parseNativeImageRequest(
      {
        input: 'original in flight',
        model: 'nai-diffusion-5-full',
        parameters: { width: 1024, height: 1024, steps: 23, seed: 99, n_samples: 2 },
      },
      native,
    ),
    f = await fixture(t, 'failed', { request, native, running: true }),
    result = await migrateLegacyDatabase(f.options),
    target = new Database(path.join(f.options.output, 'library.sqlite')),
    rebuilt = new Database(':memory:');
  try {
    assert.equal(result.findings.filter((row) => row.blocking).length, 0);
    const jobs = target
      .prepare('SELECT id,status FROM generation_jobs ORDER BY sample_index')
      .all();
    assert.deepEqual(jobs, [
      { id: f.originalReceipt.jobIds[0], status: 'running' },
      { id: f.originalReceipt.jobIds[1], status: 'pending' },
    ]);
    const attempt = target
        .prepare('SELECT id,job_id,credential_epoch FROM generation_attempts')
        .get(),
      charge = target
        .prepare('SELECT status,attempt_id,v5,anlas_fixed,anlas_purchased FROM charges')
        .get();
    assert.equal(attempt.job_id, jobs[0].id);
    assert.equal(attempt.credential_epoch, null);
    assert.equal(charge.status, 'reserved');
    assert.equal(charge.attempt_id, attempt.id);
    assert.ok(Number(charge.v5) > 0);
    assert.equal(charge.anlas_fixed, '0');
    assert.equal(charge.anlas_purchased, '0');
    const roles = target
      .prepare(
        `SELECT CAST(commit_seq AS TEXT) AS seq,event_index,event_type FROM domain_events
      WHERE event_type IN ('OpeningJobRecorded','OpeningAttemptRecorded','OpeningChargeRecorded')
      ORDER BY commit_seq,event_index`,
      )
      .all();
    assert.equal(new Set(roles.map((row) => row.seq)).size, 1);
    assert.deepEqual(
      roles.map((row) => row.event_type),
      [
        'OpeningJobRecorded',
        'OpeningJobRecorded',
        'OpeningAttemptRecorded',
        'OpeningChargeRecorded',
      ],
    );
    initializeRelationalStorage(
      rebuilt,
      target.prepare('SELECT history_id FROM projection_state').get().history_id,
    );
    const replayer = new RelationalCommitReplayer(rebuilt),
      inputs = new RelationalInputStore(target);
    for (const { seq } of target
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all()) {
      rebuilt.transaction(() => replayer.replayCommit(readRelationalCommit(target, seq), inputs))();
    }
    for (const table of ['generation_jobs', 'generation_attempts', 'charges', 'wallets']) {
      assert.deepEqual(
        rebuilt.prepare('SELECT * FROM ' + table).all(),
        target.prepare('SELECT * FROM ' + table).all(),
      );
    }
    assert.equal(relationalPrivateStateEmpty(rebuilt), true);
    assert.equal(result.readyInstalled, false);
  } finally {
    rebuilt.close();
    target.close();
  }
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('旧parentless done的独立产物沿原稳定归属/未知operation与publication时间导入，结果bytes不冒充上游输入', async (t) => {
  const original = Buffer.from(
      'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jf3sAAAAASUVORK5CYII=',
      'base64',
    ),
    f = await fixture(t, 'done', null, {
      id: 'original-artifact',
      memberId: 'member',
      token: 'STA1N-synthetic-current-key',
      mimeType: 'image/png',
      byteLength: original.length,
      sha256: hash(original),
      file: 'images/original-artifact.png',
    }),
    result = await migrateLegacyDatabase(f.options),
    target = new Database(path.join(f.options.output, 'library.sqlite'));
  try {
    const artifact = target.prepare('SELECT * FROM artifacts').get(),
      job = target.prepare('SELECT * FROM generation_jobs').get(),
      input = target.prepare('SELECT * FROM generation_inputs WHERE id=?').get(artifact.input_id);
    assert.equal(artifact.id, 'original-artifact');
    assert.equal(artifact.member_id, 'member');
    assert.equal(artifact.operation, null);
    assert.equal(artifact.published_at, null);
    assert.equal(artifact.original_attempt_id, null);
    assert.equal(artifact.sha256, hash(original));
    assert.equal(artifact.byte_length, original.length);
    assert.equal(job.artifact_id, artifact.id);
    assert.equal(job.status, 'done');
    assert.equal(input.completeness, 'unavailable');
    assert.equal(input.body, null);
    assert.equal(target.prepare('SELECT count(*) AS n FROM input_assets').get().n, 0);
    const replica = target
      .prepare('SELECT uploaded_at,verified_at,deleted_at FROM artifact_replicas')
      .get();
    assert.deepEqual(replica, { uploaded_at: null, verified_at: null, deleted_at: null });
    assert.ok(result.findings.some((row) => row.code === 'LOCAL_BYTES_NOT_VERIFIED'));
    assert.equal(result.readyInstalled, false);
    assert.equal(relationalPrivateStateEmpty(target), true);
  } finally {
    target.close();
  }
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('独立Opening每批至多100条且保原序完整回放，第二副本批尾SQL错回滚整个转换', async (t) => {
  const original = await readFile(new URL('../fixtures/synthetic.webp', import.meta.url)),
    artifacts = Array.from({ length: 102 }, (_, index) => {
      const id = `batch-artifact-${String(index).padStart(3, '0')}`;
      return {
        id,
        memberId: 'member',
        token: 'STA1N-synthetic-current-key',
        mimeType: 'image/webp',
        byteLength: original.length,
        sha256: hash(original),
        file: `images/${id}.webp`,
      };
    }),
    f = await fixture(t, 'done', null, artifacts),
    result = await migrateLegacyDatabase(f.options),
    target = new Database(path.join(f.options.output, 'library.sqlite')),
    replay = new Database(':memory:');
  try {
    const batches = (key) =>
      target
        .prepare(
          'SELECT command_id,event_count FROM commits WHERE command_id LIKE ? ORDER BY commit_seq',
        )
        .all(`migration:%:${key}%`);
    assert.deepEqual(
      batches('artifact-inputs').map((row) => row.event_count),
      [100, 2],
    );
    assert.deepEqual(
      batches('artifacts').map((row) => row.event_count),
      [100, 3],
    );
    assert.deepEqual(
      batches('replicas').map((row) => row.event_count),
      [100, 2],
    );
    for (const type of ['OpeningArtifactRecorded', 'OpeningReplicaRecorded']) {
      assert.deepEqual(
        target
          .prepare(
            'SELECT payload FROM domain_events WHERE event_type=? ORDER BY commit_seq,event_index',
          )
          .all(type)
          .map((row) => JSON.parse(row.payload).artifactId),
        artifacts.map((artifact) => artifact.id),
      );
    }
    assert.equal(result.readyInstalled, false);
    initializeRelationalStorage(
      replay,
      target.prepare('SELECT history_id FROM projection_state').get().history_id,
    );
    const replayer = new RelationalCommitReplayer(replay),
      inputs = new RelationalInputStore(target);
    for (const { seq } of target
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all()) {
      replay.transaction(() => replayer.replayCommit(readRelationalCommit(target, seq), inputs))();
    }
    for (const { name: table } of target
      .prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*'")
      .all()) {
      assert.deepEqual(
        replay.prepare('SELECT * FROM ' + table).all(),
        target.prepare('SELECT * FROM ' + table).all(),
        table,
      );
    }
    assert.equal(relationalPrivateStateEmpty(replay), true);
  } finally {
    replay.close();
    target.close();
  }
  const failedOutput = f.options.output + '-chunk2-fault',
    prepare = TargetDatabase.prototype.prepare;
  let stage = null;
  TargetDatabase.prototype.prepare = function (sql, ...args) {
    const statement = Reflect.apply(prepare, this, [sql, ...args]);
    if (
      this.name === path.join(failedOutput, 'library.sqlite') &&
      /^INSERT INTO artifact_replicas\(/.test(sql) &&
      Reflect.apply(prepare, this, ['SELECT count(*) AS n FROM artifact_replicas']).get().n === 101
    ) {
      stage = {
        inTransaction: this.inTransaction,
        artifacts: Reflect.apply(prepare, this, ['SELECT count(*) AS n FROM artifacts']).get().n,
        replicas: Reflect.apply(prepare, this, [
          'SELECT count(*) AS n FROM artifact_replicas',
        ]).get().n,
        batches: Reflect.apply(prepare, this, [
          "SELECT event_count FROM commits WHERE command_id LIKE 'migration:%:replicas:%' ORDER BY commit_seq",
        ]).all(),
      };
      this.exec(
        "CREATE TEMP TRIGGER synthetic_replica_chunk2_tail BEFORE INSERT ON main.artifact_replicas BEGIN SELECT RAISE(ABORT,'migration-replica-chunk2-tail'); END",
      );
    }
    return statement;
  };
  try {
    await assert.rejects(
      migrateLegacyDatabase({ ...f.options, output: failedOutput }),
      /migration-replica-chunk2-tail/,
    );
  } finally {
    TargetDatabase.prototype.prepare = prepare;
  }
  assert.deepEqual(stage, {
    inTransaction: true,
    artifacts: 102,
    replicas: 101,
    batches: [{ event_count: 100 }, { event_count: 2 }],
  });
  const failed = new Database(path.join(failedOutput, 'library.sqlite')),
    empty = new Database(':memory:');
  try {
    initializeRelationalStorage(
      empty,
      failed.prepare('SELECT history_id FROM projection_state').get().history_id,
    );
    for (const { name: table } of failed
      .prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*'")
      .all()) {
      assert.deepEqual(
        failed.prepare('SELECT * FROM ' + table).all(),
        empty.prepare('SELECT * FROM ' + table).all(),
        table,
      );
    }
    assert.equal(relationalPrivateStateEmpty(failed), true);
    assert.equal(failed.prepare('SELECT status FROM projection_state').get().status, 'building');
  } finally {
    empty.close();
    failed.close();
  }
  await assert.rejects(readFile(path.join(failedOutput, 'report.private.json')), {
    code: 'ENOENT',
  });
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('所选原图实复制到新owned target后才产生Local proof；manifest绑定原source/config/tuple且不改源', async (t) => {
  const original = await readFile(new URL('../fixtures/synthetic.webp', import.meta.url)),
    f = await fixture(t, 'done', null, {
      id: 'materialized',
      memberId: 'member',
      token: 'STA1N-synthetic-current-key',
      mimeType: 'image/webp',
      byteLength: original.length,
      sha256: hash(original),
      file: 'images/materialized.webp',
    }),
    selectedFile = path.join(f.directory, 'selected-original.bin'),
    rowBytes = await readFile(f.options.artifacts),
    rowLine = rowBytes.toString('utf8').split('\n')[0];
  await writeFile(selectedFile, original, { flag: 'wx' });
  const localOriginal = {
      file: selectedFile,
      byteLength: original.length,
      sha256: hash(original),
      sourceArtifactRowPointer: {
        file: f.options.artifacts,
        fileSha256: hash(rowBytes),
        line: 1,
        lineSha256: hash(rowLine + '\n'),
      },
    },
    { file, bytes } = await selectedCutConfiguration(f, localOriginal),
    result = await migrateLegacyDatabase({
      ...f.options,
      cutConfig: file,
      cutConfigSha256: hash(bytes),
    }),
    target = new Database(path.join(f.options.output, 'library.sqlite'));
  try {
    const manifestBytes = await readFile(
        path.join(f.options.output, 'migration-inputs.private.json'),
      ),
      manifest = JSON.parse(manifestBytes),
      proof = manifest.localMaterializations[0],
      source = JSON.parse(
        target
          .prepare("SELECT payload FROM domain_events WHERE event_type='ImportSourceRegistered'")
          .get().payload,
      );
    assert.equal(manifest.sourceSha256, f.options.sourceSha256);
    assert.equal(manifest.cutConfigurationSha256, hash(bytes));
    assert.equal(source.manifestSha256, hash(manifestBytes));
    assert.equal(result.inputManifestSha256, hash(manifestBytes));
    assert.equal(proof.artifactId, 'materialized');
    assert.equal(proof.memberId, 'member');
    assert.equal(proof.file, 'images/materialized.webp');
    assert.equal(proof.sha256, hash(original));
    assert.ok(proof.verifiedAt >= result.effectiveAt);
    assert.deepEqual(await readFile(path.join(f.options.output, proof.file)), original);
    const row = target
      .prepare('SELECT uploaded_at,verified_at,deleted_at FROM artifact_replicas')
      .get();
    assert.deepEqual(row, { uploaded_at: null, verified_at: proof.verifiedAt, deleted_at: null });
    assert.equal(target.prepare('SELECT published_at FROM artifacts').get().published_at, null);
    assert.equal(result.readyInstalled, false);
  } finally {
    target.close();
  }
  const bad = JSON.parse(bytes);
  bad.localOriginal.sourceArtifactRowPointer.lineSha256 = hash(rowLine);
  const badFile = path.join(f.directory, 'wrong-nonterminated-line-config.json'),
    badBytes = Buffer.from(JSON.stringify(bad));
  await writeFile(badFile, badBytes, { flag: 'wx' });
  await assert.rejects(
    migrateLegacyDatabase({
      ...f.options,
      output: f.options.output + '-bad-line',
      cutConfig: badFile,
      cutConfigSha256: hash(badBytes),
    }),
    (error) => error.migrationPhase === 'local-materialization',
  );
  await assert.rejects(readFile(path.join(f.options.output + '-bad-line', 'library.sqlite')), {
    code: 'ENOENT',
  });
  assert.deepEqual(await readFile(selectedFile), original);
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('私有尾部真实SQL错外catch回滚整笔公共/正文/私有，阶段已在Opening之后且健康新目标可重试', async (t) => {
  const f = await fixture(t),
    { file, bytes } = await selectedCutConfiguration(f),
    options = { ...f.options, cutConfig: file, cutConfigSha256: hash(bytes) },
    original = TargetDatabase.prototype.prepare;
  let stage = null;
  TargetDatabase.prototype.prepare = function (sql, ...args) {
    const statement = Reflect.apply(original, this, [sql, ...args]);
    if (
      this.name === path.join(f.options.output, 'library.sqlite') &&
      /^INSERT INTO api_keys\(/.test(sql)
    ) {
      stage = {
        transaction: this.inTransaction,
        jobs: Reflect.apply(original, this, ['SELECT count(*) AS n FROM generation_jobs']).get().n,
        commits: Reflect.apply(original, this, ['SELECT count(*) AS n FROM commits']).get().n,
      };
      this.exec(
        "CREATE TEMP TRIGGER synthetic_private_tail BEFORE INSERT ON main.api_keys BEGIN SELECT RAISE(ABORT,'migration-private-tail'); END",
      );
    }
    return statement;
  };
  try {
    await assert.rejects(migrateLegacyDatabase(options), /migration-private-tail/);
  } finally {
    TargetDatabase.prototype.prepare = original;
  }
  assert.equal(stage.transaction, true);
  assert.equal(stage.jobs, 1);
  assert.ok(stage.commits >= 4);
  const target = new Database(path.join(f.options.output, 'library.sqlite'));
  try {
    for (const table of [
      'commits',
      'domain_events',
      'members',
      'generation_inputs',
      'generation_jobs',
      'gateway_configuration',
      'api_keys',
    ]) {
      assert.equal(target.prepare('SELECT count(*) AS n FROM ' + table).get().n, 0, table);
    }
    assert.equal(relationalPrivateStateEmpty(target), true);
    assert.equal(target.prepare('SELECT status FROM projection_state').get().status, 'building');
  } finally {
    target.close();
  }
  await assert.rejects(readFile(path.join(f.options.output, 'report.private.json')), {
    code: 'ENOENT',
  });
  const retried = await migrateLegacyDatabase({
    ...options,
    output: f.options.output + '-healthy',
  });
  assert.equal(retried.private.apiKeys, 1);
  assert.equal(retried.readyInstalled, false);
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('旧enabled缺失Raw-only成员沿原Key资格开账，不借公共active激活密码或Cookie', async (t) => {
  const f = await fixture(t, 'failed', null, null, { enabled: undefined }),
    { file, bytes } = await selectedCutConfiguration(f),
    result = await migrateLegacyDatabase({
      ...f.options,
      cutConfig: file,
      cutConfigSha256: hash(bytes),
    }),
    target = new Database(path.join(f.options.output, 'library.sqlite'));
  try {
    assert.equal(target.prepare('SELECT status FROM members').get().status, 'active');
    assert.equal(result.private.apiKeys, 1);
    assert.equal(result.private.credentials, 0);
    assert.equal(result.private.sessions, 0);
    assert.equal(target.prepare('SELECT count(*) AS n FROM password_credentials').get().n, 0);
    assert.equal(target.prepare('SELECT count(*) AS n FROM member_sessions').get().n, 0);
  } finally {
    target.close();
  }
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('真实退休的完整旧键保原410不可领取，逐键仍证明其完整定义且不复活原child进历史', async (t) => {
  const native = nativeNormalizationContext(),
    request = parseNativeImageRequest(
      {
        input: 'original retired',
        model: 'nai-diffusion-4-5-full',
        parameters: { width: 1024, height: 1024, steps: 23, seed: 17, n_samples: 2 },
      },
      native,
    ),
    f = await fixture(t, 'failed', { request, native, retired: true }),
    result = await migrateLegacyDatabase(f.options),
    target = new Database(path.join(f.options.output, 'library.sqlite'));
  try {
    assert.equal(target.prepare('SELECT count(*) AS n FROM generation_jobs').get().n, 0);
    const key = new RelationalInputStore(target).readOpeningGenerationIdempotency(
      'member',
      'generate',
      generationIdempotencyKeyHash('synthetic-original-key'),
    );
    assert.equal(key.mapping.resolution, 'unavailable');
    assert.equal(key.event.payload.originalRequestId, f.originalReceipt.requestId);
    assert.equal(key.event.payload.originalJobId, f.originalReceipt.jobIds[0]);
    assert.equal(key.mapping.requestId, null);
    assert.equal(result.keyDecisions.length, 1);
    assert.equal(result.keyDecisions[0].firstDefinitionProved, true);
    assert.equal(result.keyDecisions[0].originalCommandProved, true);
    assert.equal(result.keyDecisions[0].sourceRetired, true);
    assert.equal(
      result.findings.filter((row) => row.code === 'COMPLETE_KEY_TARGET_CLOSURE_UNPROVEN').length,
      0,
    );
    assert.equal(relationalPrivateStateEmpty(target), true);
  } finally {
    target.close();
  }
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('真实active alias按其原外层身份/独立首次context导入，不借后来的定义补原parent', async (t) => {
  for (const originalKnown of [true, false]) {
    const native = nativeNormalizationContext(),
      aliasNative = structuredClone(native);
    aliasNative.defaults.scale = 9;
    const request = parseNativeImageRequest(
        {
          input: 'active alias source',
          model: 'nai-diffusion-4-5-full',
          parameters: { width: 1024, height: 1024, steps: 23, scale: 6, seed: 43, n_samples: 1 },
        },
        native,
      ),
      f = await fixture(t, 'failed', {
        request,
        native,
        alias: true,
        aliasNative,
        context: originalKnown ? { native } : {},
      }),
      result = await migrateLegacyDatabase(f.options),
      target = new Database(path.join(f.options.output, 'library.sqlite'));
    try {
      const owner = new RelationalInputStore(target),
        alias = owner.readOpeningGenerationIdempotency(
          'member',
          'generate',
          generationIdempotencyKeyHash('synthetic-alias-key'),
        ),
        first = owner.readOpeningGenerationIdempotency(
          'member',
          'generate',
          generationIdempotencyKeyHash('synthetic-original-key'),
        ),
        context = JSON.parse(
          target
            .prepare('SELECT body FROM normalization_contexts WHERE id=?')
            .get(alias.mapping.contextId).body,
        ),
        parent = target.prepare('SELECT context_id FROM generation_requests').get();
      assert.equal(alias.event.payload.originalRequestId, f.originalReceipt.requestId);
      assert.equal(alias.event.payload.originalJobId, f.originalReceipt.jobIds[0]);
      assert.equal(typeof alias.event.payload.originalCommandId, 'string');
      assert.equal(context.native.defaults.scale, 9);
      assert.equal(result.keyDecisions.filter((row) => row.originalCommandProved).length, 2);
      assert.equal(target.prepare('SELECT count(*) AS n FROM generation_jobs').get().n, 1);
      if (originalKnown) {
        assert.equal(alias.mapping.resolution, 'live');
        assert.equal(first.mapping.resolution, 'live');
        assert.notEqual(alias.mapping.contextId, parent.context_id);
        assert.equal(
          result.findings.filter((row) => row.code === 'COMPLETE_KEY_TARGET_CLOSURE_UNPROVEN')
            .length,
          0,
        );
      } else {
        assert.equal(alias.mapping.resolution, 'archived');
        assert.equal(first.mapping.resolution, 'archived');
        assert.equal(parent.context_id, null);
        assert.equal(
          target.prepare('SELECT completeness FROM generation_inputs').get().completeness,
          'partial',
        );
        assert.equal(
          result.findings.filter((row) => row.code === 'COMPLETE_KEY_TARGET_CLOSURE_UNPROVEN')
            .length,
          1,
        );
      }
    } finally {
      target.close();
    }
    assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
  }
});

test('原Opening诊断按真实pointer/原UTF8/hash/createdAt0冷读与完整提交回放，不重写失败或复活auth', async (t) => {
  const detail = 'original diagnostic\n尾部保留空白 ',
    f = await fixture(
      t,
      'failed',
      null,
      null,
      {},
      { errorDetail: detail, completedAt: '1970-01-01T00:00:00.000Z' },
    ),
    result = await migrateLegacyDatabase(f.options),
    target = new Database(path.join(f.options.output, 'library.sqlite')),
    rebuilt = new Database(':memory:');
  try {
    const row = target
        .prepare('SELECT id,job_id,sha256,created_at FROM generation_diagnostics')
        .get(),
      owner = new RelationalInputStore(target);
    assert.equal(row.job_id, 'job');
    assert.equal(row.created_at, 0);
    assert.equal(row.sha256, hash(Buffer.from(detail)));
    assert.equal(owner.readGenerationDiagnostic(row.id, 'job'), detail);
    assert.equal(
      target
        .prepare(
          "SELECT count(*) AS n FROM domain_events WHERE event_type IN ('JobFailed','DiagnosticRecorded','GenerationRefunded')",
        )
        .get().n,
      0,
    );
    initializeRelationalStorage(
      rebuilt,
      target.prepare('SELECT history_id FROM projection_state').get().history_id,
    );
    const replayer = new RelationalCommitReplayer(rebuilt);
    for (const { seq } of target
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all()) {
      rebuilt.transaction(() => replayer.replayCommit(readRelationalCommit(target, seq), owner))();
    }
    assert.equal(new RelationalInputStore(rebuilt).readGenerationDiagnostic(row.id, 'job'), detail);
    assert.equal(relationalPrivateStateEmpty(rebuilt), true);
    assert.equal(result.readyInstalled, false);
  } finally {
    target.close();
    rebuilt.close();
  }
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('原辅助DTO seed0不是actualSeed，generate原实际seed0仍保留', async (t) => {
  const native = nativeNormalizationContext(),
    image = await readFile(new URL('../fixtures/synthetic.webp', import.meta.url));
  for (const request of [
    parseNativeImageRequest(
      {
        input: 'real seed zero',
        model: 'nai-diffusion-4-5-full',
        parameters: { width: 1024, height: 1024, steps: 23, seed: 0, n_samples: 1 },
      },
      native,
    ),
    parseNativeAuxiliaryRequest(
      { model: native.vibeModels[0], image: image.toString('base64') },
      'encode-vibe',
      native,
    ),
  ]) {
    assert.equal(request.seed, 0);
    const f = await fixture(t, 'failed', { request, native }),
      result = await migrateLegacyDatabase(f.options),
      target = new Database(path.join(f.options.output, 'library.sqlite'));
    try {
      const job = target.prepare('SELECT actual_seed FROM generation_jobs').get(),
        parent = target.prepare('SELECT operation,seed_mode FROM generation_requests').get();
      assert.equal(job.actual_seed, (request.operation ?? 'generate') === 'generate' ? 0 : null);
      assert.equal(
        parent.seed_mode,
        (request.operation ?? 'generate') === 'generate' ? 'fixed' : 'not_applicable',
      );
      assert.equal(result.keyDecisions[0].firstDefinitionProved, true);
      assert.equal(request.seed, 0);
    } finally {
      target.close();
    }
    assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
  }
});

test('现代原ArtifactPublished拥有creator/input，storage流没有member也须保同源Local完整接缝', async (t) => {
  const native = nativeNormalizationContext(),
    request = parseNativeImageRequest(
      {
        input: 'modern publication',
        model: 'nai-diffusion-4-5-full',
        parameters: { width: 1024, height: 1024, steps: 23, seed: 7, n_samples: 1 },
      },
      native,
    ),
    f = await fixture(t, 'failed', { request, native, completed: true }),
    original = await readFile(path.join(f.originalDataDir, f.completedImage.file)),
    rowBytes = await readFile(f.options.artifacts),
    rows = rowBytes.toString('utf8').split('\n'),
    index = rows.findIndex((line) => line && JSON.parse(line).id === f.completedImage.id),
    localOriginal = {
      file: path.join(f.originalDataDir, f.completedImage.file),
      byteLength: original.length,
      sha256: hash(original),
      sourceArtifactRowPointer: {
        file: f.options.artifacts,
        fileSha256: hash(rowBytes),
        line: index + 1,
        lineSha256: hash(rows[index] + '\n'),
      },
    },
    { file, bytes } = await selectedCutConfiguration(f, localOriginal),
    result = await migrateLegacyDatabase({
      ...f.options,
      cutConfig: file,
      cutConfigSha256: hash(bytes),
    }),
    target = new Database(path.join(f.options.output, 'library.sqlite'));
  try {
    const artifact = target.prepare('SELECT * FROM artifacts').get(),
      attempt = target.prepare('SELECT * FROM generation_attempts').get(),
      parent = target.prepare('SELECT * FROM generation_requests').get();
    assert.equal(artifact.id, f.completedImage.id);
    assert.equal(artifact.member_id, 'member');
    assert.equal(artifact.input_id, parent.input_id);
    assert.equal(artifact.original_attempt_id, attempt.id);
    assert.equal(artifact.published_at, f.acceptedAt + 3);
    assert.equal(
      target.prepare('SELECT artifact_id FROM generation_jobs').get().artifact_id,
      artifact.id,
    );
    assert.equal(target.prepare('SELECT count(*) AS n FROM input_assets').get().n, 0);
    assert.equal(
      result.findings.some((row) => row.code === 'ARTIFACT_INPUT_OWNER_UNPROVEN'),
      false,
    );
    assert.equal(
      typeof target.prepare('SELECT verified_at FROM artifact_replicas').get().verified_at,
      'number',
    );
    assert.deepEqual(await readFile(path.join(f.options.output, f.completedImage.file)), original);
  } finally {
    target.close();
  }
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

async function rescuedSelection(f, ids, bytes) {
  const config = await selectedCutConfiguration(f);
  config.value.sourceNamespaces.r2AccountId = 'a'.repeat(32);
  config.value.sourceNamespaces.r2Bucket = 'selected-bucket';
  config.bytes = Buffer.from(JSON.stringify(config.value));
  await writeFile(config.file, config.bytes);
  const exported = await readFile(f.options.artifacts),
    lines = exported.toString('utf8').split('\n'),
    local = path.join(f.directory, 'rescued-sample.bin');
  await writeFile(local, bytes, { flag: 'wx' });
  const value = {
      format: 'novelai-legacy-rescued-originals-v1',
      sourceSha256: f.options.sourceSha256,
      sourceCommit,
      cut: f.options.cut,
      auditSha256: f.options.auditSha256,
      cutConfigurationSha256: hash(config.bytes),
      namespace: `r2://${'a'.repeat(32)}/selected-bucket`,
      namespaceReceipt: config.proofs.namespaceReceipt,
      originals: ids.map((id) => {
        const line = lines.findIndex((line) => line && JSON.parse(line).id === id);
        return {
          artifactId: id,
          memberId: 'member',
          inputId: `legacy-artifact-input:${id}`,
          sha256: hash(bytes),
          byteLength: bytes.length,
          sourceArtifactRowPointer: {
            file: f.options.artifacts,
            fileSha256: hash(exported),
            line: line + 1,
            lineSha256: hash(lines[line] + '\n'),
          },
          file: local,
          r2: { bucket: 'selected-bucket', key: `images/${id}/${hash(bytes)}` },
          downloadReceipt: null,
        };
      }),
    },
    file = path.join(f.directory, 'rescued-selection.private.json');
  await writeFile(file, JSON.stringify(value), { flag: 'wx' });
  return { config, value, file, local };
}

test('rescued batch preserves distinct same-SHA entities and actual planned owner/input through offline intake and full replay', async (t) => {
  const bytes = await readFile(new URL('../fixtures/synthetic.webp', import.meta.url)),
    ids = ['rescued-a', 'rescued-b'],
    f = await fixture(
      t,
      'done',
      null,
      ids.map((id) => ({
        id,
        memberId: 'member',
        token: 'STA1N-synthetic-current-key',
        mimeType: 'image/webp',
        byteLength: bytes.length,
        sha256: hash(bytes),
        file: `images/${id}.webp`,
      })),
    ),
    selected = await rescuedSelection(f, ids, bytes),
    selectedBytes = await readFile(selected.file),
    result = await migrateLegacyDatabase({
      ...f.options,
      cutConfig: selected.config.file,
      cutConfigSha256: hash(selected.config.bytes),
      rescuedOriginals: selected.file,
      rescuedOriginalsSha256: hash(selectedBytes),
    }),
    db = new Database(path.join(f.options.output, 'library.sqlite')),
    replay = new Database(':memory:');
  try {
    const manifestBytes = await readFile(
        path.join(f.options.output, 'migration-inputs.private.json'),
      ),
      manifest = JSON.parse(manifestBytes);
    assert.equal(manifest.localMaterializations.length, 2);
    assert.equal(manifest.rescuedOriginalsSha256, hash(selectedBytes));
    assert.equal(result.inputManifestSha256, hash(manifestBytes));
    assert.equal(new Set(manifest.localMaterializations.map((row) => row.inputId)).size, 2);
    for (const id of ids) {
      const replica = db.prepare('SELECT * FROM artifact_replicas WHERE artifact_id=?').get(id);
      assert.ok(replica.verified_at >= result.effectiveAt);
      assert.equal(replica.uploaded_at, null);
      assert.deepEqual(await readFile(path.join(f.options.output, `images/${id}.webp`)), bytes);
    }
    assert.equal(result.readyInstalled, false);
    initializeRelationalStorage(
      replay,
      db.prepare('SELECT history_id FROM projection_state').get().history_id,
    );
    for (const { seq } of db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all()) {
      replay.transaction(() =>
        new RelationalCommitReplayer(replay).replayCommit(
          readRelationalCommit(db, seq),
          new RelationalInputStore(db),
        ),
      )();
    }
    for (const table of ['artifacts', 'artifact_replicas', 'generation_inputs']) {
      assert.deepEqual(
        replay.prepare(`SELECT * FROM ${table}`).all(),
        db.prepare(`SELECT * FROM ${table}`).all(),
      );
    }
    assert.equal(relationalPrivateStateEmpty(replay), true);
  } finally {
    db.close();
    replay.close();
  }
  assert.deepEqual(await readFile(selected.local), bytes);
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('rescued intake rejects wrong cut/config/namespace/export/owner/input and broken bytes without constructing a target DB or proof', async (t) => {
  const bytes = await readFile(new URL('../fixtures/synthetic.webp', import.meta.url)),
    f = await fixture(t, 'done', null, {
      id: 'rescued',
      memberId: 'member',
      token: 'STA1N-synthetic-current-key',
      mimeType: 'image/webp',
      byteLength: bytes.length,
      sha256: hash(bytes),
      file: 'images/rescued.webp',
    }),
    selected = await rescuedSelection(f, ['rescued'], bytes);
  for (const kind of [
    'cut',
    'audit',
    'config',
    'namespace',
    'receipt',
    'duplicate',
    'export',
    'owner',
    'input',
    'bytes',
    'half-pair',
  ]) {
    await t.test(kind, async () => {
      const value = structuredClone(selected.value),
        file = path.join(f.directory, `rescued-${kind}.json`),
        output = path.join(f.directory, `reject-${kind}`);
      if (kind === 'cut') {
        value.cut = String(BigInt(value.cut) + 1n);
      }
      if (kind === 'audit') {
        value.auditSha256 = '0'.repeat(64);
      }
      if (kind === 'config') {
        value.cutConfigurationSha256 = '0'.repeat(64);
      }
      if (kind === 'namespace') {
        value.namespace = `r2://${'b'.repeat(32)}/other-bucket`;
      }
      if (kind === 'receipt') {
        value.namespaceReceipt.sha256 = '0'.repeat(64);
      }
      if (kind === 'duplicate') {
        value.originals.push(structuredClone(value.originals[0]));
      }
      if (kind === 'export') {
        value.originals[0].sourceArtifactRowPointer.lineSha256 = '0'.repeat(64);
      }
      if (kind === 'owner') {
        value.originals[0].memberId = 'another-member';
      }
      if (kind === 'input') {
        value.originals[0].inputId = 'another-input';
      }
      if (kind === 'bytes') {
        value.originals[0].file = path.join(f.directory, 'broken.bin');
        await writeFile(value.originals[0].file, Buffer.alloc(bytes.length), { flag: 'wx' });
      }
      const raw = Buffer.from(JSON.stringify(value));
      await writeFile(file, raw, { flag: 'wx' });
      await assert.rejects(
        migrateLegacyDatabase({
          ...f.options,
          output,
          cutConfig: selected.config.file,
          cutConfigSha256: hash(selected.config.bytes),
          rescuedOriginals: file,
          ...(kind === 'half-pair' ? {} : { rescuedOriginalsSha256: hash(raw) }),
        }),
      );
      await assert.rejects(readFile(path.join(output, 'library.sqlite')), { code: 'ENOENT' });
      await assert.rejects(readFile(path.join(output, 'migration-inputs.private.json')), {
        code: 'ENOENT',
      });
    });
  }
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
  assert.deepEqual(await readFile(selected.local), bytes);
});

test('selected R2 preparation reuses local sample, serially calls original getFile and retains only successful fresh inputs on first failure', async (t) => {
  const bytes = await readFile(new URL('../fixtures/synthetic.webp', import.meta.url)),
    ids = ['sample', 'fetched', 'failed'],
    f = await fixture(
      t,
      'done',
      null,
      ids.map((id) => ({
        id,
        memberId: 'member',
        token: 'STA1N-synthetic-current-key',
        mimeType: 'image/webp',
        byteLength: bytes.length,
        sha256: hash(bytes),
        file: `images/${id}.webp`,
      })),
    ),
    selected = await rescuedSelection(f, ids, bytes),
    { materializeLegacyOriginals } = await import(
      entry
        ? new URL('./materialize-legacy-originals.mjs', pathToFileURL(entry)).href
        : new URL('../../scripts/materialize-legacy-originals.mjs', import.meta.url).href
    ),
    { R2Storage } = await import(
      entry
        ? new URL('../server/storage/r2-storage.js', pathToFileURL(entry)).href
        : new URL('../../server/storage/r2-storage.ts', import.meta.url).href
    ),
    saved = Object.fromEntries(
      ['R2_ACCOUNT_ID', 'R2_BUCKET', 'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY'].map((key) => [
        key,
        process.env[key],
      ]),
    ),
    original = R2Storage.prototype.getFile,
    calls = [];
  Object.assign(process.env, {
    R2_ACCOUNT_ID: 'a'.repeat(32),
    R2_BUCKET: 'selected-bucket',
    R2_ACCESS_KEY_ID: 'synthetic',
    R2_SECRET_ACCESS_KEY: 'synthetic',
  });
  t.after(() => {
    R2Storage.prototype.getFile = original;
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    }
  });
  selected.value.originals[1].file = null;
  selected.value.originals[2].file = null;
  const selectionBytes = Buffer.from(JSON.stringify(selected.value));
  await writeFile(selected.file, selectionBytes);
  const output = path.join(f.directory, 'prepared-r2'),
    options = {
      selection: selected.file,
      selectionSha256: hash(selectionBytes),
      cutConfig: selected.config.file,
      cutConfigSha256: hash(selected.config.bytes),
      output,
    };
  process.env.R2_BUCKET = 'wrong-bucket';
  await assert.rejects(materializeLegacyOriginals(options));
  await assert.rejects(readFile(path.join(output, 'rescued-originals.private.json')), {
    code: 'ENOENT',
  });
  process.env.R2_BUCKET = 'selected-bucket';
  R2Storage.prototype.getFile = async function (descriptor, file) {
    calls.push(structuredClone(descriptor));
    assert.equal(this.config.bucket, 'selected-bucket');
    if (calls.length === 2) {
      throw new Error('synthetic first read failure');
    }
    await writeFile(file, bytes, { flag: 'wx', mode: 0o600 });
  };
  await assert.rejects(
    materializeLegacyOriginals(options),
    (error) => error.materializationResult.completed === 2,
  );
  assert.deepEqual(
    calls.map((row) => row.key),
    ids.slice(1).map((id) => `images/${id}/${hash(bytes)}`),
  );
  const manifestBytes = await readFile(path.join(output, 'rescued-originals.private.json')),
    manifest = JSON.parse(manifestBytes);
  assert.deepEqual(
    manifest.originals.map((row) => row.artifactId),
    ids.slice(0, 2),
  );
  assert.deepEqual(await readFile(manifest.originals[0].file), bytes);
  assert.deepEqual(await readFile(manifest.originals[1].file), bytes);
  await assert.rejects(materializeLegacyOriginals(options));
  assert.equal(calls.length, 2);
  const result = await migrateLegacyDatabase({
    ...f.options,
    cutConfig: selected.config.file,
    cutConfigSha256: hash(selected.config.bytes),
    rescuedOriginals: path.join(output, 'rescued-originals.private.json'),
    rescuedOriginalsSha256: hash(manifestBytes),
  });
  assert.equal(result.readyInstalled, false);
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

// Reuse the actual converter's protected-input staging, not a second partial/normalization mapper.
async function reconciliationFixture(t, parent = null) {
  const cleanup = [],
    f = await fixture({ after: (fn) => cleanup.push(fn) }, 'failed', parent),
    staged = new Map(),
    original = RelationalInputStore.prototype.stageOpeningInput,
    capture = t.mock.method(RelationalInputStore.prototype, 'stageOpeningInput', function (input) {
      staged.set(input.inputId, input);
      return original.call(this, input);
    });
  let result;
  try {
    result = await migrateLegacyDatabase(f.options);
  } finally {
    capture.mock.restore();
  }
  const source = new Database(f.options.source, { readonly: true, fileMustExist: true }),
    target = new Database(path.join(f.options.output, 'library.sqlite'));
  source.pragma('query_only=ON');
  t.after(async () => {
    source.close();
    target.close();
    for (const run of cleanup) {
      await run();
    }
  });
  const refs = migrationReferences(f.options.sourceSha256),
    requests = [];
  const prepared = source.transaction(() => {
    const members = mapLegacyMembers(source, refs),
      accounts = mapLegacyAccounts(source, refs),
      sharing = mapLegacySharing(source, refs),
      groups = new Map();
    for (const row of source
      .prepare('SELECT id,request_id,order_value,data FROM jobs ORDER BY order_value DESC')
      .all()) {
      const value = JSON.parse(row.data),
        requestId = row.request_id || `legacy-job-anchor:${row.id}`;
      if (!groups.has(requestId)) {
        groups.set(requestId, {
          group: { requestId, originalRequestId: row.request_id, jobIds: [] },
          jobs: [],
        });
      }
      const group = groups.get(requestId);
      group.group.jobIds.push(row.id);
      group.jobs.push({ row, value });
      refs.requestsByJob.set(row.id, requestId);
      refs.jobIds.add(row.id);
      if (value.generationAttempt?.id) {
        refs.attemptIds.add(value.generationAttempt.id);
      }
    }
    for (const plan of groups.values()) {
      const first = plan.jobs[0],
        originalRow = plan.group.originalRequestId
          ? source
              .prepare('SELECT * FROM generation_requests WHERE id=?')
              .get(plan.group.originalRequestId)
          : null,
        protectedRow =
          originalRow ??
          source
            .prepare('SELECT * FROM generation_requests WHERE id=?')
            .get(first.value.legacyInputRef || `legacy-input:${first.row.id}`),
        saved = protectedRow ? JSON.parse(protectedRow.payload) : null,
        original = saved?.legacyInline === true ? saved.request : (saved ?? first.value.request),
        memberId = first.value.memberId ?? protectedRow.member_id,
        keyRows = source
          .prepare('SELECT * FROM generation_idempotency WHERE request_id=?')
          .all(plan.group.originalRequestId),
        proofs = keyRows.map((mapping) => ({
          mapping,
          proof: proveLegacyGenerationIntent(
            original,
            JSON.parse(mapping.normalization_context),
            mapping,
          ),
        })),
        registered = source
          .prepare(
            "SELECT payload FROM domain_events WHERE stream_id=? AND event_type='RequestAccepted'",
          )
          .get(`request:${plan.group.originalRequestId}`),
        primary = registered ? JSON.parse(registered.payload).idempotency : null,
        definition = primary
          ? proofs.find((entry) => entry.mapping.key_hash === primary.keyHash)?.proof
          : null,
        inputId = plan.group.originalRequestId || `legacy-job-input:${first.row.id}`,
        content = staged.get(inputId),
        current = target
          .prepare('SELECT operation,model,sample_count FROM generation_requests WHERE id=?')
          .get(plan.group.requestId);
      for (const { row } of plan.jobs) {
        refs.inputsByJob.set(row.id, { inputId, memberId, operation: current.operation });
      }
      requests.push({
        ...plan,
        originalRow,
        protectedRow,
        original,
        memberId,
        inputId,
        content,
        proof: definition,
        proofs,
        operation: current.operation,
        model: current.model,
        count: current.sample_count,
      });
    }
    return { members, accounts, sharing, requests };
  })();
  const audit = JSON.parse(await readFile(f.options.audit)),
    archive = {
      binding: {
        format: 'legacy-export',
        sourceSha256: f.options.sourceSha256,
        finalCommitSeq: f.options.cut,
        manifestSha256: f.options.auditSha256,
      },
      audit,
      auditSha256: f.options.auditSha256,
      inputs: (await readFile(path.join(path.dirname(f.options.artifacts), 'inputs.jsonl'), 'utf8'))
        .trim()
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line)),
    };
  return {
    ...f,
    source,
    target,
    proof: {
      references: refs,
      prepared,
      artifacts: {
        facts: [],
        inputs: [],
        metadata: { artifacts: [], storageTargets: [], localMaterializationsByArtifact: new Map() },
      },
      descriptors: [],
      keyDecisions: result.keyDecisions,
      archive,
      privateResult: result.private,
      readOriginalBaselines: () => {
        const originalRefs = migrationReferences(f.options.sourceSha256);
        return {
          members: mapLegacyMembers(source, originalRefs),
          accounts: mapLegacyAccounts(source, originalRefs),
          sharing: mapLegacySharing(source, originalRefs),
        };
      },
      readOriginalDiagnostic: () => null,
      freeUsage: result.financial.freeUsage,
      proveGenerationIntent: proveLegacyGenerationIntent,
    },
  };
}
function assertReconciliationReadOnly(f, target = f.target) {
  target.pragma('query_only=ON');
  const before = databaseState(target),
    changes = target.prepare('SELECT total_changes() AS n').get().n;
  let result;
  try {
    result = f.source.transaction(() =>
      target.transaction(() => assertLegacyDatabaseProjection(f.source, target, f.proof))(),
    )();
  } finally {
    assert.equal(databaseState(target), before);
    assert.equal(target.prepare('SELECT total_changes() AS n').get().n, changes);
    assert.equal(f.source.prepare('SELECT total_changes() AS n').get().n, 0);
  }
  return result;
}
function publicShadow(t, target, omitLast = false) {
  const shadow = new Database(':memory:');
  t.after(() => shadow.close());
  initializeRelationalStorage(
    shadow,
    target.prepare('SELECT history_id FROM projection_state').get().history_id,
  );
  const rows = target
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all(),
    inputs = new RelationalInputStore(target);
  for (const row of omitLast ? rows.slice(0, -1) : rows) {
    shadow.transaction(() =>
      new RelationalCommitReplayer(shadow).replayCommit(
        readRelationalCommit(target, row.seq),
        inputs,
      ),
    )();
  }
  return shadow;
}
test('Source完整集核对接受真实B转换与合法archive薄正文，query_only全表零写', async (t) => {
  const f = await reconciliationFixture(t),
    retained = assertReconciliationReadOnly(f);
  assert.ok(retained.retained.some((row) => row.kind === 'request' && row.onlineIds.length === 1));
  const shadow = publicShadow(t, f.target);
  assert.equal(databaseState(shadow), databaseState(f.target));
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});
test('target与自身shadow完全一致仍不能漏掉旧键或整份旧job', async (t) => {
  const native = nativeNormalizationContext(),
    request = parseNativeImageRequest(
      {
        input: 'source coverage',
        model: 'nai-diffusion-4-5-full',
        parameters: { width: 128, height: 128, steps: 23, seed: 17 },
      },
      native,
    );
  const f = await reconciliationFixture(t, { native, request, running: true, alias: true });
  assertReconciliationReadOnly(f);
  const short = publicShadow(t, f.target, true),
    shadow = publicShadow(t, short);
  assert.equal(databaseState(shadow), databaseState(short));
  assert.equal(
    short.prepare('SELECT COUNT(*) AS n FROM idempotency_keys').get().n,
    f.target.prepare('SELECT COUNT(*) AS n FROM idempotency_keys').get().n - 1,
  );
  assert.throws(
    () => assertReconciliationReadOnly(f, short),
    /OpeningIdempotencyRecorded|key_coverage/,
  );
  const g = await reconciliationFixture(t),
    missing = publicShadow(t, g.target, true),
    missingShadow = publicShadow(t, missing);
  assert.equal(databaseState(missingShadow), databaseState(missing));
  assert.equal(missing.prepare('SELECT COUNT(*) AS n FROM generation_jobs').get().n, 0);
  assert.throws(
    () => assertReconciliationReadOnly(g, missing),
    /mismatch: request\.|OpeningJobRecorded/,
  );
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
  assert.deepEqual(await readFile(g.options.source), g.sourceBytes);
});

test('真实入口拒绝self-consistent假完成时间、漏旧键或副本，outer事务连输入全回滚', async (t) => {
  for (const mode of ['unknown-time', 'omitted-key', 'omitted-replica']) {
    await t.test(mode, async (t) => {
      const originalBytes = await readFile(new URL('../fixtures/synthetic.webp', import.meta.url)),
        native = nativeNormalizationContext(),
        request = parseNativeImageRequest(
          {
            input: 'source coverage',
            model: 'nai-diffusion-4-5-full',
            parameters: { width: 128, height: 128, steps: 23, seed: 17 },
          },
          native,
        ),
        artifact = {
          id: 'reconciliation-artifact',
          memberId: 'member',
          operation: 'generate',
          file: 'images/reconciliation-artifact.webp',
          mimeType: 'image/webp',
          byteLength: originalBytes.length,
          sha256: hash(originalBytes),
        },
        f = await fixture(
          t,
          'failed',
          mode === 'omitted-key' ? { native, request, running: true, alias: true } : null,
          mode === 'omitted-replica' ? artifact : null,
        ),
        append = RelationalCommitReplayer.prototype.appendCommit;
      let reached = false,
        shadowEqual = false;
      const hook = t.mock.method(
        RelationalCommitReplayer.prototype,
        'appendCommit',
        function (command, inputs) {
          const omitted =
            (mode === 'omitted-key' &&
              command.events.some(
                (e) =>
                  e.eventType === 'OpeningIdempotencyRecorded' &&
                  e.payload.keyHash === generationIdempotencyKeyHash('synthetic-alias-key'),
              )) ||
            (mode === 'omitted-replica' &&
              command.events.some((e) => e.eventType === 'OpeningReplicaRecorded'));
          if (omitted) {
            reached = true;
            const shadow = publicShadow(t, this.sqlite);
            shadowEqual = databaseState(shadow) === databaseState(this.sqlite);
            return; // Deliberately simulate the converter forgetting one independent whole commit.
          }
          if (mode === 'unknown-time') {
            for (const event of command.events) {
              if (event.eventType === 'OpeningJobRecorded') {
                assert.equal(event.payload.completedAt, null);
                event.payload.completedAt = 0;
                reached = true;
              }
            }
          }
          const result = append.call(this, command, inputs);
          if (mode === 'unknown-time' && reached) {
            const shadow = publicShadow(t, this.sqlite);
            shadowEqual = databaseState(shadow) === databaseState(this.sqlite);
          }
          return result;
        },
      );
      let expected = /artifact_decision/;
      if (mode === 'unknown-time') {
        expected = /job_source_fields/;
      }
      if (mode === 'omitted-key') {
        expected = /OpeningIdempotencyRecorded/;
      }
      try {
        await assert.rejects(migrateLegacyDatabase(f.options), expected);
      } finally {
        hook.mock.restore();
      }
      assert.equal(reached, true);
      assert.equal(shadowEqual, true, 'bad imported target is itself completely replayable');
      const failed = new Database(path.join(f.options.output, 'library.sqlite'));
      try {
        for (const table of [
          'commits',
          'domain_events',
          'generation_inputs',
          'generation_jobs',
          'idempotency_keys',
          'wallets',
          'artifacts',
          'artifact_replicas',
        ]) {
          assert.equal(failed.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0, table);
        }
      } finally {
        failed.close();
      }
      assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
    });
  }
});

test('正常B入口核对产物原locator/编码与退休承诺，不把未核Local字节当Ready', async (t) => {
  const bytes = await readFile(new URL('../fixtures/synthetic.webp', import.meta.url)),
    artifact = {
      id: 'reconciliation-artifact',
      memberId: 'member',
      operation: 'generate',
      file: 'images/reconciliation-artifact.webp',
      mimeType: 'image/webp',
      byteLength: bytes.length,
      sha256: hash(bytes),
    },
    f = await fixture(t, 'failed', null, artifact),
    report = await migrateLegacyDatabase(f.options),
    db = new Database(path.join(f.options.output, 'library.sqlite'));
  try {
    assert.equal(report.sourceReconciliation.status, 'complete');
    assert.equal(report.readyInstalled, false);
    assert.ok(report.findings.some((row) => row.code === 'LOCAL_BYTES_NOT_VERIFIED'));
    assert.deepEqual(
      db
        .prepare(
          'SELECT object_key,encoding,uploaded_at,verified_at,deleted_at FROM artifact_replicas',
        )
        .get(),
      {
        object_key: 'reconciliation-artifact.webp',
        encoding: 'identity',
        uploaded_at: null,
        verified_at: null,
        deleted_at: null,
      },
    );
    const shadow = publicShadow(t, db);
    assert.equal(databaseState(shadow), databaseState(db));
  } finally {
    db.close();
  }
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('真实入口从原Source复核取消和独立owner，不接受跟随append篡改的expected', async (t) => {
  const native = nativeNormalizationContext(),
    request = parseNativeImageRequest(
      {
        input: 'original cancellation',
        model: 'nai-diffusion-4-5-full',
        parameters: { width: 128, height: 128, steps: 23, seed: 17 },
      },
      native,
    ),
    bytes = await readFile(new URL('../fixtures/synthetic.webp', import.meta.url));
  for (const mode of [
    'cancellation',
    'replica-locator',
    'member-name',
    'account-weight',
    'group-name',
  ]) {
    await t.test(mode, async (t) => {
      const parent = ['cancellation', 'account-weight', 'group-name'].includes(mode)
          ? { native, request }
          : null,
        artifacts =
          mode === 'replica-locator'
            ? ['source-a', 'source-b'].map((id) => ({
                id,
                memberId: 'member',
                operation: 'generate',
                file: `images/${id}.webp`,
                mimeType: 'image/webp',
                byteLength: bytes.length,
                sha256: hash(bytes),
              }))
            : null,
        f = await fixture(t, 'failed', parent, artifacts),
        healthy = await migrateLegacyDatabase(f.options);
      assert.equal(healthy.sourceReconciliation.status, 'complete');
      const source = new Database(f.options.source, { readonly: true, fileMustExist: true });
      try {
        source.pragma('query_only=ON');
        if (mode === 'cancellation') {
          const original = JSON.parse(
            source.prepare('SELECT data FROM jobs WHERE id=?').get(f.originalReceipt.jobIds[0])
              .data,
          );
          assert.equal(original.cancellationReason, 'request_cancelled');
        }
        assert.equal(source.prepare('SELECT total_changes() AS n').get().n, 0);
      } finally {
        source.close();
      }
      let changed = 0,
        shadowEqual = false;
      const append = RelationalCommitReplayer.prototype.appendCommit,
        hook = t.mock.method(
          RelationalCommitReplayer.prototype,
          'appendCommit',
          function (command, inputs) {
            let altered = false;
            for (const event of command.events) {
              const p = event.payload;
              if (mode === 'cancellation' && event.eventType === 'OpeningJobRecorded') {
                assert.equal(p.cancellationReason, 'request_cancelled');
                p.cancellationReason = null;
              } else if (
                mode === 'replica-locator' &&
                event.eventType === 'OpeningReplicaRecorded'
              ) {
                assert.equal(p.objectKey, `${p.artifactId}.webp`);
                p.objectKey = `${p.artifactId === 'source-a' ? 'source-b' : 'source-a'}.webp`;
              } else if (mode === 'member-name' && event.eventType === 'OpeningMemberRecorded') {
                assert.equal(p.displayName, '');
                p.displayName = 'not the source member name';
              } else if (
                mode === 'account-weight' &&
                event.eventType === 'OpeningAccountRecorded'
              ) {
                p.weight += 1;
              } else if (mode === 'group-name' && event.eventType === 'OpeningGroupRecorded') {
                p.name += ' changed after mapping';
              } else {
                continue;
              }
              changed++;
              altered = true;
            }
            const result = append.call(this, command, inputs);
            if (altered) {
              const shadow = publicShadow(t, this.sqlite);
              shadowEqual = databaseState(shadow) === databaseState(this.sqlite);
            }
            return result;
          },
        );
      const expected = {
          cancellation: /job_source_fields/,
          'replica-locator': /artifact_decision/,
          'member-name': /OpeningMemberRecorded_decision/,
          'account-weight': /OpeningAccountRecorded_decision/,
          'group-name': /sharing_decision/,
        }[mode],
        output = path.join(f.directory, 'changed-target');
      try {
        await assert.rejects(migrateLegacyDatabase({ ...f.options, output }), expected);
      } finally {
        hook.mock.restore();
      }
      assert.ok(changed > 0);
      assert.equal(
        shadowEqual,
        true,
        'changed decisions reach the real owner and remain replayable',
      );
      const failed = new Database(path.join(output, 'library.sqlite'));
      try {
        for (const table of [
          'commits',
          'domain_events',
          'members',
          'upstream_accounts',
          'sharing_groups',
          'generation_inputs',
          'generation_jobs',
          'idempotency_keys',
          'wallets',
          'artifacts',
          'artifact_replicas',
        ]) {
          assert.equal(failed.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n, 0, table);
        }
      } finally {
        failed.close();
      }
      assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
    });
  }
});
