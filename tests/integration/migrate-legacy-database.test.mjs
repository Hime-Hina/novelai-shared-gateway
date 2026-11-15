import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, writeFile, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { JsonStore } from '../../server/storage/sqlite-store.ts';
import {
  migrateLegacyDatabase,
  proveLegacyGenerationIntent,
  readLegacyCutConfiguration,
} from '../../scripts/migrate-legacy-database.mjs';
import {
  nativeNormalizationContext,
  parseNativeImageRequest,
} from '../../server/protocols/native-image.ts';
import { generationRequestFingerprint } from '../../server/generation/request-fingerprint.ts';
import { configureFairQuota, syncFairQuota } from '../../server/quota/fair-share.ts';
import {
  createGenerationAdmission,
  generationIntent,
  generationIdempotencyKeyHash,
} from '../../server/generation/admission.ts';
import { createGenerationPayloadBudget } from '../../server/generation/payload-budget.ts';
import {
  cancelGenerationRequestCommand,
  reserveQueuedGeneration,
} from '../../server/generation/execution.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalConfigurationStore } from '../../server/storage/relational-configuration-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { domainDigest } from '../../server/events/domain-events.ts';
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
) {
  const directory = await mkdtemp(path.join(tmpdir(), 'nai-migrate-legacy-')),
    store = new JsonStore(path.join(directory, 'old'));
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
    images: artifact ? [artifact] : [],
    jobs: [
      {
        id: 'job',
        memberId: 'member',
        userToken: 'STA1N-synthetic-current-key',
        status,
        ...(artifact ? { imageId: artifact.id } : {}),
        source: 'novelai-native',
        createdAt: 0,
        request: { model: 'nai-diffusion-4-5-full', prompt: '', scale: 0 },
        ...jobPatch,
      },
    ],
  });
  let originalReceipt = null;
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
    if (parent.running) {
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
        operation: 'generate',
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
    if (parent.running) {
      const reservation = await reserveQueuedGeneration(store, originalReceipt.jobIds[0], {
        now: () => acceptedAt + 1,
        env: {},
        dispatchEnabled: () => true,
        physicalAccountLoad: () => 0,
      });
      assert.equal(reservation.job?.status, 'running');
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
  if (!parent?.running) {
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
        lineSha256: hash(rowLine),
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
  assert.deepEqual(await readFile(selectedFile), original);
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});

test('私有尾部真实SQL错外catch回滚整笔公共/正文/私有，阶段已在Opening之后且健康新目标可重试', async (t) => {
  const f = await fixture(t),
    { file, bytes } = await selectedCutConfiguration(f),
    options = { ...f.options, cutConfig: file, cutConfigSha256: hash(bytes) },
    original = Database.prototype.prepare;
  let stage = null;
  Database.prototype.prepare = function (sql, ...args) {
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
    Database.prototype.prepare = original;
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
