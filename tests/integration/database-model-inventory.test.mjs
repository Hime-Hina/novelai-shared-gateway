import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { spawnSync } from 'node:child_process';
import Database from 'better-sqlite3';
import { JsonStore } from '../../server/storage/sqlite-store.ts';
import {
  configureFairQuota,
  syncFairQuota,
  reserveFairQuota,
  settleFairQuota,
} from '../../server/quota/fair-share.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { domainDigest } from '../../server/events/domain-events.ts';
import { inventoryLegacyDatabase } from '../../scripts/database-model-inventory.mjs';

const sourceCommit = 'c6d62450a261c2ad8d055bd35d1e05bbdfc123af';
const at = 1793491200000;

async function fixture(t) {
  const directory = await mkdtemp(path.join(tmpdir(), 'nai-model-inventory-'));
  const store = new JsonStore(directory);
  await store.init();
  t.after(async () => {
    await store.close();
    await rm(directory, { recursive: true, force: true });
  });
  const db = structuredClone(store.db);
  db.users = ['private-member-a', 'private-member-b', 'private-member-c', 'private-member-d'].map(
    (id) => ({ id, token: `private-key-${id}`, enabled: true }),
  );
  db.accounts = [
    {
      id: 'private-account',
      token: 'pst-private-token',
      enabled: true,
      quotaTier: 3,
      subscriptionActive: true,
      quotaFixed: 1000,
      quotaPurchased: 0,
      v5UsagePercent: 60,
      quotaCheckedAt: at,
    },
  ];
  configureFairQuota(
    db,
    db.users.map((u) => u.id),
  );
  syncFairQuota(db, at);
  const retired = {
    id: 'private-retired-job',
    memberId: db.users[0].id,
    userToken: db.users[0].token,
    status: 'queued',
    request: { model: 'nai-diffusion-5-full', width: 1024, height: 1024, steps: 23 },
  };
  reserveFairQuota(db, db.users[0], retired, db.accounts[0], at);
  settleFairQuota(db, retired, false, at + 1000);
  db.jobs = [
    {
      id: 'private-live-job',
      memberId: db.users[0].id,
      userToken: db.users[0].token,
      status: 'queued',
      queueSeq: 1,
      createdAt: new Date(at).toISOString(),
      request: {
        model: 'nai-diffusion-5-full',
        width: 1024,
        height: 1024,
        steps: 23,
        prompt: 'private prompt content',
      },
    },
  ];
  db.images = [
    {
      id: 'private-image',
      memberId: db.users[0].id,
      token: db.users[0].token,
      prompt: 'private image prompt',
      file: 'images/private-image.png',
    },
  ];
  db.cards = [{ id: 'private-card', code: 'private-redeem-code', balance: 5 }];
  await store.write(db);
  store.sqlite
    .prepare(
      'INSERT INTO generation_idempotency(member_id,operation,key_hash,fingerprint,normalization_context,job_id,request_id,created_at) VALUES(?,?,?,?,?,?,?,?)',
    )
    .run(
      db.users[0].id,
      'generate',
      'private-keyhash',
      'private-fingerprint',
      '{"defaultNegative":"private context"}',
      retired.id,
      'private-retired-request',
      new Date(at).toISOString(),
    );
  await store.enableEventAuthority();
  const snapshot = path.join(directory, 'snapshot.sqlite');
  await store.sqlite.backup(snapshot);
  const work = new Database(snapshot);
  work.pragma('journal_mode=DELETE');
  work.close();
  return { directory, snapshot };
}

test('盘点真实旧 v4 快照，缺失目标的旧键和退款保留且报告不泄露原文', async (t) => {
  const f = await fixture(t),
    before = await readFile(f.snapshot),
    files = await readdir(f.directory);
  const report = inventoryLegacyDatabase(f.snapshot, sourceCommit);
  assert.equal(report.integrity, true);
  assert.equal(report.cut.openingComplete, true);
  assert.equal(report.tables.jobs.rows, 1);
  assert.equal(report.tables.generation_requests.rows, 1);
  assert.equal(report.liabilities.activeJobs, 1);
  assert.equal(report.liabilities.idempotencyWithoutJob, 1);
  assert.equal(report.liabilities.idempotencyWithoutRequest, 1);
  assert.equal(report.liabilities.settlementsWithoutJob, 1);
  assert.equal(report.liabilities.refundsWithoutJob, 1);
  assert.equal(report.ownership.jobsWithoutProvenMember, 0);
  assert.equal(report.ownership.artifactsWithoutProvenMember, 0);
  assert.equal(report.d4.cardRecordsRequiringDisposition, 1);
  assert.equal(report.inputs.invalidArtifactHashes, 0);
  assert.equal(report.inputs.conflictingRequestHashes, 0);
  assert.equal(report.quota.walletAvailable.v5, '60');
  assert.equal(report.source.declaredCommit, sourceCommit);
  assert.match(report.source.databaseSha256, /^[a-f0-9]{64}$/);
  assert.equal(JSON.stringify(report).includes('private-'), false);
  for (const secret of ['private prompt content', 'private image prompt', 'private context']) {
    assert.equal(JSON.stringify(report).includes(secret), false);
  }
  assert.deepEqual(await readFile(f.snapshot), before);
  assert.deepEqual(await readdir(f.directory), files);
  const cli = spawnSync(
    process.execPath,
    [
      'scripts/database-model-inventory.mjs',
      `--source=${f.snapshot}`,
      `--source-commit=${sourceCommit}`,
    ],
    { encoding: 'utf8' },
  );
  assert.equal(cli.status, 0, cli.stderr);
  assert.equal(JSON.parse(cli.stdout).source.databaseSha256, report.source.databaseSha256);
});

test('正文损坏和归属冲突分别计入缺口，不把查询投影当作唯一来源', async (t) => {
  const f = await fixture(t),
    work = new Database(f.snapshot);
  work
    .prepare(
      "UPDATE generation_requests SET payload=json_set(payload,'$.request.prompt','damaged-private-input'),member_id='conflicting-private-member'",
    )
    .run();
  work.prepare('UPDATE artifact_inputs SET payload_sha256=?').run('0'.repeat(64));
  work.prepare("UPDATE artifact_inputs SET member_id='conflicting-artifact-owner'").run();
  work.close();
  const report = inventoryLegacyDatabase(f.snapshot, sourceCommit);
  assert.equal(report.inputs.conflictingRequestHashes, 1);
  assert.equal(report.inputs.conflictingRequestOwners, 1);
  assert.equal(report.inputs.invalidArtifactHashes, 1);
  assert.equal(report.inputs.wrongPayloadBytes, 1);
  assert.equal(report.ownership.jobsWithConflictingMembers, 1);
  assert.equal(report.ownership.artifactsWithConflictingMembers, 1);
  assert.equal(JSON.stringify(report).includes('damaged-private-input'), false);
});

test('当前 Key 的唯一匹配不能替代历史稳定成员，受保护产物归属仍可证明', async (t) => {
  const f = await fixture(t),
    work = new Database(f.snapshot);
  work
    .prepare(
      "UPDATE jobs SET data=json_remove(data,'$.memberId','$.legacyInputRef','$.requestId','$.generationAttempt','$.fairCharge')",
    )
    .run();
  work.prepare("UPDATE images SET data=json_remove(data,'$.memberId','$.userId')").run();
  work.close();
  const withProtected = inventoryLegacyDatabase(f.snapshot, sourceCommit);
  assert.equal(withProtected.ownership.jobsWithoutProvenMember, 1);
  assert.equal(withProtected.ownership.jobsWithCurrentTokenCandidateOnly, 1);
  assert.equal(withProtected.ownership.artifactsWithoutProvenMember, 0);
  const changed = new Database(f.snapshot);
  changed.exec('UPDATE artifact_inputs SET member_id=NULL');
  changed.close();
  const tokenOnly = inventoryLegacyDatabase(f.snapshot, sourceCommit);
  assert.equal(tokenOnly.ownership.artifactsWithoutProvenMember, 1);
  assert.equal(tokenOnly.ownership.artifactsWithCurrentTokenCandidateOnly, 1);
});

test('金额字段含私有文本时拒绝整份报告，不直接输出受存储控制的值', async (t) => {
  const f = await fixture(t),
    work = new Database(f.snapshot);
  const row = work
    .prepare("SELECT projection FROM event_streams WHERE stream_id='account:private-account'")
    .get();
  const projection = JSON.parse(row.projection);
  projection.quota.sharedV5Debt = 'private-text-in-amount';
  work
    .prepare(
      "UPDATE event_streams SET projection=?,projection_sha256=? WHERE stream_id='account:private-account'",
    )
    .run(JSON.stringify(projection), domainDigest(projection));
  work.close();
  const cli = spawnSync(
    process.execPath,
    [
      'scripts/database-model-inventory.mjs',
      `--source=${f.snapshot}`,
      `--source-commit=${sourceCommit}`,
    ],
    { encoding: 'utf8' },
  );
  assert.equal(cli.status, 1);
  assert.equal(cli.stdout, '');
  assert.equal(cli.stderr.includes('private-text-in-amount'), false);
});

test('伪造数组 length 不能作为计数输出私有文本', async (t) => {
  for (const field of ['members', 'pendingExternalFunding']) {
    await t.test(field, async (t) => {
      const f = await fixture(t),
        work = new Database(f.snapshot);
      const stream = field === 'members' ? 'sharing:default' : 'account:private-account';
      const row = work
        .prepare('SELECT projection FROM event_streams WHERE stream_id=?')
        .get(stream);
      const projection = JSON.parse(row.projection);
      if (field === 'members') {
        projection.members = { length: 'private-pst-in-wrong-length' };
      } else {
        projection.quota.operating.pendingExternalFunding = {
          length: 'private-pst-in-wrong-length',
        };
      }
      work
        .prepare('UPDATE event_streams SET projection=?,projection_sha256=? WHERE stream_id=?')
        .run(JSON.stringify(projection), domainDigest(projection), stream);
      work.close();
      const before = await readFile(f.snapshot),
        files = await readdir(f.directory);
      const cli = spawnSync(
        process.execPath,
        [
          'scripts/database-model-inventory.mjs',
          `--source=${f.snapshot}`,
          `--source-commit=${sourceCommit}`,
        ],
        { encoding: 'utf8' },
      );
      assert.equal(cli.status, 1);
      assert.equal(cli.stdout, '');
      assert.equal(cli.stderr.includes('private-pst-in-wrong-length'), false);
      assert.deepEqual(await readFile(f.snapshot), before);
      assert.deepEqual(await readdir(f.directory), files);
    });
  }
});

test('拒绝新格式、缺列、未知 writer 与 WAL 输入；CLI 错误不带私有解析原文', async (t) => {
  const f = await fixture(t);
  for (const mutation of [
    (db) => db.exec('DROP TABLE generation_diagnostics'),
    (db) =>
      db
        .prepare("UPDATE app_meta SET value=? WHERE key='event_authority'")
        .run('{"version":99,"minimumWriterVersion":99}'),
    (db) => db.pragma('journal_mode=WAL'),
    (db) => db.prepare("UPDATE event_streams SET projection='private-secret-invalid-json'").run(),
  ]) {
    const filename = path.join(f.directory, `variant-${Math.random()}.sqlite`);
    const original = new Database(f.snapshot, { readonly: true });
    await original.backup(filename);
    original.close();
    const work = new Database(filename);
    mutation(work);
    work.close();
    const before = await readFile(filename),
      files = await readdir(f.directory);
    const cli = spawnSync(
      process.execPath,
      [
        'scripts/database-model-inventory.mjs',
        `--source=${filename}`,
        `--source-commit=${sourceCommit}`,
      ],
      { encoding: 'utf8' },
    );
    assert.equal(cli.status, 1);
    assert.equal(cli.stdout, '');
    assert.equal(cli.stderr.includes('private-secret'), false);
    assert.deepEqual(await readFile(filename), before);
    assert.deepEqual(await readdir(f.directory), files);
  }
  const filename = path.join(f.directory, 'relational.sqlite'),
    db = new Database(filename);
  initializeRelationalStorage(db, 'synthetic-history');
  db.close();
  assert.throws(() => inventoryLegacyDatabase(filename, sourceCommit), /current-v4/);
});
