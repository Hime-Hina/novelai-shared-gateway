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
} from '../../scripts/migrate-legacy-database.mjs';
import {
  nativeNormalizationContext,
  parseNativeImageRequest,
} from '../../server/protocols/native-image.ts';
import { generationRequestFingerprint } from '../../server/generation/request-fingerprint.ts';
import { configureFairQuota, syncFairQuota } from '../../server/quota/fair-share.ts';
import { createGenerationAdmission, generationIntent } from '../../server/generation/admission.ts';
import { createGenerationPayloadBudget } from '../../server/generation/payload-budget.ts';
import { cancelGenerationRequestCommand } from '../../server/generation/execution.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { domainDigest } from '../../server/events/domain-events.ts';
import { relationalPrivateStateEmpty } from '../../server/storage/relational-private-restore.ts';

const hash = (bytes) => createHash('sha256').update(bytes).digest('hex'),
  sourceCommit = '3caba3dfa0b5d9f582acabfc02e36e7db4af038f';

async function fixture(t, status = 'failed', parent = null) {
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
      { id: 'member', name: '', token: 'synthetic-current-key', enabled: true, createdAt: 0 },
    ],
    jobs: [
      {
        id: 'job',
        memberId: 'member',
        userToken: 'synthetic-current-key',
        status,
        source: 'novelai-native',
        createdAt: 0,
        request: { model: 'nai-diffusion-4-5-full', prompt: '', scale: 0 },
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
      token: id === 'member' ? 'synthetic-current-key' : `synthetic-${id}`,
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
      identity: { token: 'synthetic-current-key' },
      request: null,
      source: 'novelai-native',
      batch: true,
      noCache: true,
      admissionVersion: 0,
      intent: generationIntent('synthetic-original-key', () => parent.request, {
        operation: 'generate',
        randomSeed: false,
        noCache: true,
        context: () => ({ native: parent.native }),
      }),
    });
    await cancelGenerationRequestCommand(store, originalReceipt.requestId, 'member', {
      now: () => acceptedAt + 1,
    });
  }
  await store.enableEventAuthority();
  const source = path.join(directory, 'source.sqlite');
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
    artifactBytes = Buffer.alloc(0),
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
      artifacts: 0,
      networkCalls: 0,
      dispatches: 0,
      externalObjectBytesVerified: false,
      exports: {
        inputs: tuple(
          'inputs.jsonl',
          inputBytes,
          inputRows.length + contextRows.length + diagnosticRows.length,
        ),
        artifacts: tuple('artifacts.jsonl', artifactBytes, 0),
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
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM normalization_contexts').get().n, 1);
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
  } finally {
    target.close();
  }
  assert.deepEqual(await readFile(f.options.source), f.sourceBytes);
});
