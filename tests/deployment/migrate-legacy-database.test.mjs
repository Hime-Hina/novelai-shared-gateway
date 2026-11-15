import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, writeFile, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { JsonStore } from '../../server/storage/sqlite-store.ts';
import { migrateLegacyDatabase } from '../../scripts/migrate-legacy-database.mjs';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { relationalPrivateStateEmpty } from '../../server/storage/relational-private-restore.ts';

const hash = (bytes) => createHash('sha256').update(bytes).digest('hex'),
  sourceCommit = '3caba3dfa0b5d9f582acabfc02e36e7db4af038f';

async function fixture(t, status = 'failed') {
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
    inputBytes = Buffer.from(
      inputRows
        .map(
          (row) =>
            JSON.stringify({
              kind: 'request',
              id: row.id,
              member_id: row.member_id,
              sha256: hash(row.payload),
            }) + '\n',
        )
        .join(''),
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
      contexts: 0,
      artifactInputs: 0,
      diagnostics: 0,
      artifacts: 0,
      networkCalls: 0,
      dispatches: 0,
      externalObjectBytesVerified: false,
      exports: {
        inputs: tuple('inputs.jsonl', inputBytes, inputRows.length),
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
