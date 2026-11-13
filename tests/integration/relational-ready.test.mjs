import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createHash } from 'node:crypto';
import { syncBuiltinESMExports } from 'node:module';
import fsPromises, { readFile, writeFile, readdir, stat, unlink } from 'node:fs/promises';
import path from 'node:path';
import {
  fixture,
  meta,
  keys,
  NOW,
  image,
  databaseState,
} from '../helpers/relational-generation.mjs';
import {
  appendRelationalCommit,
  readRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import {
  relationalPrivateStateEmpty,
  relationalPrivateTables,
  restoreRelationalPrivateSnapshot,
} from '../../server/storage/relational-private-restore.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import {
  authenticateMemberApiKey,
  withAuthenticatedMemberApiKey,
} from '../../server/members/authentication.ts';
import {
  normalizeMemberGeneration,
  materializeMemberGeneration,
} from '../../server/generation/member-contract.ts';
import { describeStoredResult } from '../../server/storage/result-files.ts';
import { parseAmount, formatAmount } from '../../server/quota/resource-amounts.ts';

const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');
async function snapshot(f, name = 'selected') {
  const sourcePath = path.join(f.dir, `${name}.sqlite`);
  await f.db.backup(sourcePath);
  const sealed = new Database(sourcePath);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  return {
    sourcePath,
    sourceSha256: sha256(await readFile(sourcePath)),
    historyId: 'generation-history',
    finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get()
      .seq,
    replicas: { dataDir: f.dir },
  };
}
function rebuild(t, selected) {
  const db = new Database(':memory:'),
    source = new Database(selected.sourcePath, { readonly: true });
  t.after(() => db.close());
  source.pragma('foreign_keys=ON');
  initializeRelationalStorage(db, selected.historyId);
  const replayer = new RelationalCommitReplayer(db),
    inputs = new RelationalInputStore(source);
  try {
    for (const { seq } of source
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all()) {
      db.transaction(() => replayer.replayCommit(readRelationalCommit(source, seq), inputs))();
    }
  } finally {
    source.close();
  }
  return db;
}
async function completed(t) {
  const f = await fixture(t);
  await f.accept('complete');
  f.dispatch('complete-0');
  await f.publish('complete-0');
  return f;
}
function assertBuilding(db) {
  assert.equal(db.prepare('SELECT status FROM projection_state').get().status, 'building');
  assert.throws(() => assertRelationalStorageReady(db), /incomplete/);
}
function mutateImmutable(db, table, mutate) {
  const trigger = db
    .prepare('SELECT sql FROM sqlite_schema WHERE name=?')
    .get(`${table}_identity`).sql;
  db.exec(`DROP TRIGGER ${table}_identity`);
  mutate();
  db.exec(trigger);
}
function addOneToWallet(db) {
  const row = db.prepare("SELECT anlas_fixed_available FROM wallets WHERE member_id='m1'").get();
  db.prepare("UPDATE wallets SET anlas_fixed_available=? WHERE member_id='m1'").run(
    formatAmount(parseAmount(row.anlas_fixed_available) + parseAmount('1')),
  );
}

async function acceptAsset(f) {
  const normalized = normalizeMemberGeneration({
    operation: 'encode-vibe',
    model: 'nai-diffusion-4-5-full',
    image: image.toString('base64'),
    information_extracted: 0.5,
  });
  return withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    () =>
      f.generations.acceptGeneration(meta('accept-asset', NOW, 'm1'), {
        inputId: 'input-asset',
        requestId: 'asset',
        memberId: 'm1',
        source: 'native',
        seedMode: 'not_applicable',
        confirmation: null,
        request: materializeMemberGeneration(normalized),
        context: { kind: 'native', native: normalized.normalizationContext.native },
        jobs: [
          {
            jobId: 'asset-0',
            status: 'pending',
            queueSeq: null,
            queueExpiresAt: NOW + 60000,
            deadlineAt: null,
            admissionEpoch: 0,
            cacheKey: null,
          },
        ],
      }),
    {},
  );
}

test('完整保护输入 BLOB 在 source 或目标损坏均拒绝；合法 queued 责任和期限原样安装', async (t) => {
  for (const kind of ['valid', 'source', 'target']) {
    await t.test(kind, async (t) => {
      const f = await fixture(t);
      await acceptAsset(f);
      const damage = (db) =>
        mutateImmutable(db, 'input_assets', () => {
          const bytes = Buffer.from(db.prepare('SELECT bytes FROM input_assets').get().bytes);
          bytes[bytes.length - 1] ^= 1;
          db.prepare('UPDATE input_assets SET bytes=?').run(bytes);
        });
      if (kind === 'source') {
        damage(f.db);
      }
      const selected = await snapshot(f);
      const db = kind === 'source' ? new Database(f.db.serialize()) : rebuild(t, selected);
      if (kind === 'source') {
        t.after(() => db.close());
        db.pragma('foreign_keys=ON');
      }
      if (kind === 'target') {
        damage(db);
      }
      const before = db.serialize();
      if (kind === 'valid') {
        await verifyAndInstallRelationalStorage(db, selected);
        assertRelationalStorageReady(db);
        assert.deepEqual(db.prepare('SELECT status,queue_expires_at FROM generation_jobs').get(), {
          status: 'queued',
          queue_expires_at: NOW + 60000,
        });
      } else {
        await assert.rejects(verifyAndInstallRelationalStorage(db, selected));
        assertBuilding(db);
        assert.deepEqual(db.serialize(), before);
      }
    });
  }
});

test('同 public cut 的另一份私有快照不能覆盖，已完全一致私有状态可零私有写入安装', async (t) => {
  const f = await completed(t),
    old = await snapshot(f, 'old-private');
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'new-private-key'))();
  const selected = await snapshot(f, 'new-private');
  assert.equal(old.finalCommitSeq, selected.finalCommitSeq);
  const mismatch = rebuild(t, selected);
  mismatch.transaction(() =>
    restoreRelationalPrivateSnapshot(mismatch, old.sourcePath, old.sourceSha256),
  )();
  const before = databaseState(mismatch);
  await assert.rejects(
    verifyAndInstallRelationalStorage(mismatch, selected),
    /private authentication differs/,
  );
  assertBuilding(mismatch);
  assert.equal(databaseState(mismatch), before);
  const matching = rebuild(t, selected);
  matching.transaction(() =>
    restoreRelationalPrivateSnapshot(matching, selected.sourcePath, selected.sourceSha256),
  )();
  matching.exec(
    "CREATE TEMP TRIGGER no_private_insert BEFORE INSERT ON api_keys BEGIN SELECT RAISE(ABORT,'private rewritten'); END",
  );
  await verifyAndInstallRelationalStorage(matching, selected);
  assertRelationalStorageReady(matching);
  assert.equal(
    (await authenticateMemberApiKey(new RelationalMemberStore(matching), 'new-private-key')).id,
    'm1',
  );
});

test('唯一合法安装入口完整重建核对真实 Local 字节，在最终事务恢复私有并安装 ready，源不变且零网络', async (t) => {
  const f = await completed(t),
    selected = await snapshot(f),
    db = rebuild(t, selected),
    original = await readFile(selected.sourcePath),
    files = await readdir(f.dir);
  let network = 0;
  t.mock.method(globalThis, 'fetch', () => {
    network++;
    throw new Error('no network');
  });
  assertBuilding(db);
  assert.equal(relationalPrivateStateEmpty(db), true);
  assert.deepEqual(await verifyAndInstallRelationalStorage(db, selected), {
    historyId: selected.historyId,
    finalCommitSeq: selected.finalCommitSeq,
    artifactsVerified: 1,
    preparedResultsVerified: 0,
  });
  assertRelationalStorageReady(db);
  assert.equal((await authenticateMemberApiKey(new RelationalMemberStore(db), keys.m1)).id, 'm1');
  assert.equal(network, 0);
  assert.deepEqual(await readFile(selected.sourcePath), original);
  assert.deepEqual(await readdir(f.dir), files);
  const once = databaseState(db);
  await assert.rejects(verifyAndInstallRelationalStorage(db, selected), /final cut/);
  assert.equal(databaseState(db), once);
});

test('同 cursor 的钱包差一、缺行、多行、正文或私有错配都拒绝，不能凭格式或游标安装', async (t) => {
  for (const kind of ['wallet', 'missing', 'extra', 'body', 'private']) {
    await t.test(kind, async (t) => {
      const f = await completed(t),
        selected = await snapshot(f),
        db = rebuild(t, selected);
      if (kind === 'wallet') {
        addOneToWallet(db);
      }
      if (kind === 'missing') {
        db.prepare("DELETE FROM wallets WHERE member_id='m2'").run();
      }
      if (kind === 'extra') {
        db.prepare(
          "INSERT INTO generation_diagnostics(id,job_id,detail,sha256,created_at) VALUES('extra','complete-0','diagnostic',?,?)",
        ).run(sha256('diagnostic'), NOW);
      }
      if (kind === 'body') {
        mutateImmutable(db, 'generation_inputs', () =>
          db.prepare("UPDATE generation_inputs SET body=json_set(body,'$.input','changed')").run(),
        );
      }
      if (kind === 'private') {
        db.prepare("UPDATE members SET auth_epoch='old-revoked-epoch' WHERE id='m1'").run();
      }
      const before = databaseState(db);
      await assert.rejects(verifyAndInstallRelationalStorage(db, selected), /validation failed/);
      assertBuilding(db);
      assert.equal(databaseState(db), before);
    });
  }
});

test('选择源须有明确非零完整切点，未知事实与无 owner 诊断即使目标复制一致也不能 ready', async (t) => {
  for (const kind of ['history', 'cut', 'hash', 'empty', 'unsupported', 'diagnostic']) {
    await t.test(kind, async (t) => {
      const f = await completed(t);
      if (kind === 'unsupported') {
        const { policyVersion, pricingVersion, ...command } = meta('unknown-owner', NOW + 20);
        f.db.transaction(() =>
          appendRelationalCommit(f.db, {
            ...command,
            expectedVersions: {
              'sharing:default': f.db
                .prepare(
                  "SELECT CAST(stream_version AS TEXT) AS v FROM stream_heads WHERE stream_id='sharing:default'",
                )
                .get().v,
            },
            receipt: { configured: true },
            events: [
              {
                eventType: 'SharingConfigured',
                streamId: 'sharing:default',
                memberId: null,
                accountId: null,
                occurredAt: NOW + 20,
                policyVersion,
                pricingVersion,
                payload: { enabled: true, boundAccountId: 'a', configVersion: 1 },
              },
            ],
          }),
        )();
      }
      if (kind === 'diagnostic') {
        f.db
          .prepare(
            "INSERT INTO generation_diagnostics(id,job_id,detail,sha256,created_at) VALUES('d','complete-0','diagnostic',?,?)",
          )
          .run(sha256('diagnostic'), NOW);
      }
      const selected = await snapshot(f),
        db = new Database(f.db.serialize());
      t.after(() => db.close());
      db.pragma('foreign_keys=ON');
      if (kind === 'history') {
        selected.historyId = 'another';
      }
      if (kind === 'cut') {
        selected.finalCommitSeq = String(BigInt(selected.finalCommitSeq) - 1n);
      }
      if (kind === 'hash') {
        selected.sourceSha256 = '0'.repeat(64);
      }
      if (kind === 'empty') {
        selected.finalCommitSeq = '0';
      }
      const before = databaseState(db);
      await assert.rejects(verifyAndInstallRelationalStorage(db, selected));
      assertBuilding(db);
      assert.equal(databaseState(db), before);
    });
  }
});

test('已核验标记不能代替真实原图；缺失及同长度损坏均拒绝且不写私有', async (t) => {
  for (const kind of ['missing', 'corrupt']) {
    await t.test(kind, async (t) => {
      const f = await completed(t),
        selected = await snapshot(f),
        db = rebuild(t, selected);
      const filename = path.join(f.dir, 'images', 'artifact-complete-0.webp');
      if (kind === 'missing') {
        await unlink(filename);
      } else {
        const bytes = await readFile(filename);
        bytes[bytes.length - 1] ^= 1;
        await writeFile(filename, bytes);
      }
      const before = databaseState(db);
      await assert.rejects(verifyAndInstallRelationalStorage(db, selected));
      assertBuilding(db);
      assert.equal(databaseState(db), before);
      assert.equal(relationalPrivateStateEmpty(db), true);
    });
  }
});

test('最终 ready 更新失败时私有恢复与状态全回滚，移除故障后可安装', async (t) => {
  const f = await completed(t),
    selected = await snapshot(f),
    db = rebuild(t, selected);
  db.exec(
    "CREATE TEMP TRIGGER fail_ready AFTER UPDATE OF status ON projection_state WHEN NEW.status='ready' BEGIN SELECT RAISE(ABORT,'ready install failure'); END",
  );
  const before = databaseState(db);
  await assert.rejects(verifyAndInstallRelationalStorage(db, selected), /ready install failure/);
  assert.equal(databaseState(db), before);
  assertBuilding(db);
  assert.equal(relationalPrivateStateEmpty(db), true);
  db.exec('DROP TRIGGER fail_ready');
  await verifyAndInstallRelationalStorage(db, selected);
  assertRelationalStorageReady(db);
});

test('实际文件读取期间源私有、目标私有或同 cursor 业务变化都被最终核对拒绝', async (t) => {
  for (const kind of ['target-private', 'target-revocation', 'target-wallet', 'source-private']) {
    await t.test(kind, async (t) => {
      const f = await completed(t),
        selected = await snapshot(f),
        db = rebuild(t, selected);
      if (kind === 'target-revocation') {
        db.transaction(() =>
          restoreRelationalPrivateSnapshot(db, selected.sourcePath, selected.sourceSha256),
        )();
      }
      let changed = false,
        afterChange;
      const realOpen = fsPromises.open;
      t.mock.method(fsPromises, 'open', async (...args) => {
        assert.equal(db.inTransaction, false);
        if (!changed && String(args[0]).endsWith('artifact-complete-0.webp')) {
          changed = true;
          if (kind === 'target-private') {
            db.prepare("UPDATE members SET auth_epoch='changed-during-io' WHERE id='m1'").run();
          }
          if (kind === 'target-revocation') {
            db.transaction(() => {
              db.prepare('UPDATE members SET auth_epoch=NULL').run();
              for (const [table] of relationalPrivateTables) {
                db.prepare(`DELETE FROM ${table}`).run();
              }
            })();
          }
          if (kind === 'target-wallet') {
            addOneToWallet(db);
          }
          if (kind === 'source-private') {
            const other = new Database(selected.sourcePath);
            other
              .prepare("UPDATE members SET auth_epoch='changed-source-private' WHERE id='m1'")
              .run();
            other.close();
          }
          afterChange = databaseState(db);
        }
        return realOpen(...args);
      });
      syncBuiltinESMExports();
      try {
        await assert.rejects(verifyAndInstallRelationalStorage(db, selected));
        assert.equal(changed, true);
        assertBuilding(db);
        assert.equal(databaseState(db), afterChange);
      } finally {
        t.mock.restoreAll();
        syncBuiltinESMExports();
      }
    });
  }
});

test('待恢复准备结果只读核对 final/part；完整字节可保留原未决状态，缺坏不能 ready', async (t) => {
  for (const kind of ['final', 'part', 'missing', 'bad-part']) {
    await t.test(kind, async (t) => {
      const f = await fixture(t);
      await f.accept('pending');
      f.dispatch('pending-0');
      const descriptor = describeStoredResult('artifact-pending-0', 'generate', {
        mimeType: 'image/webp',
        buffer: image,
      });
      f.db.transaction(() =>
        f.generations.prepareGenerationResult(
          meta('prepare-pending', NOW + 3),
          'pending-0',
          descriptor,
          f.response(),
        ),
      )();
      if (kind !== 'missing') {
        const bytes = Buffer.from(image);
        if (kind === 'bad-part') {
          bytes[bytes.length - 1] ^= 1;
        }
        await writeFile(
          path.join(f.dir, descriptor.file) + (kind === 'final' ? '' : '.part'),
          bytes,
        );
      }
      const selected = await snapshot(f),
        db = rebuild(t, selected),
        before = databaseState(db);
      if (kind === 'final' || kind === 'part') {
        await verifyAndInstallRelationalStorage(db, selected);
        assertRelationalStorageReady(db);
        assert.equal(
          db.prepare("SELECT status FROM generation_jobs WHERE id='pending-0'").get().status,
          'running',
        );
        assert.equal(db.prepare('SELECT COUNT(*) AS n FROM artifacts').get().n, 0);
        assert.equal(
          (await stat(path.join(f.dir, descriptor.file) + (kind === 'final' ? '' : '.part'))).size,
          image.length,
        );
      } else {
        await assert.rejects(verifyAndInstallRelationalStorage(db, selected));
        assertBuilding(db);
        assert.equal(databaseState(db), before);
      }
    });
  }
});
