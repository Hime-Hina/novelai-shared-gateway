import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { readFile, writeFile, stat, unlink } from 'node:fs/promises';
import { fixture, meta, NOW, image, databaseState } from '../helpers/relational-generation.mjs';
import { describeStoredResult, publishStoredResult } from '../../server/storage/result-files.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { recoverRelationalGenerationResults } from '../../server/storage/relational-result-recovery.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { assertRelationalStorageReady } from '../../server/storage/relational-storage.ts';

function prepare(f, jobId, at = NOW + 3, response = f.response(at)) {
  const descriptor = describeStoredResult(`artifact-${jobId}`, 'generate', {
    mimeType: 'image/webp',
    buffer: image,
  });
  const result = f.db.transaction(() =>
    f.generations.prepareGenerationResult(
      {
        ...meta(`prepare-${jobId}`, at),
        policyVersion: 'original-policy',
        pricingVersion: 'original-price',
      },
      jobId,
      descriptor,
      response,
    ),
  )();
  return { descriptor, event: result.events[0] };
}
async function recoveryFixture(t, options = {}) {
  let reopened;
  t.after(() => {
    if (reopened?.open) {
      reopened.close();
    }
  });
  const f = await fixture(t, options);
  return {
    ...f,
    reopen() {
      f.db.close();
      reopened = new Database(path.join(f.dir, 'library.sqlite'));
      reopened.pragma('foreign_keys=ON');
      return reopened;
    },
  };
}
const recover = (db, dir, outcomes = [], now = NOW + 20) =>
  recoverRelationalGenerationResults(db, dir, {
    now: () => now,
    onRecovery: (outcome) => outcomes.push(outcome),
  });

test('关闭重开后从原 final 或完整 part 恢复一次，保留原期限与准备版本', async (t) => {
  for (const location of ['final', 'part']) {
    await t.test(location, async (t) => {
      const f = await recoveryFixture(t);
      await f.accept('recover', {
        samples: 2,
        deadlineAt: NOW + 100000,
        queueExpiresAt: NOW + 90000,
      });
      f.dispatch('recover-0');
      const { descriptor, event } = prepare(f, 'recover-0'),
        file = path.join(f.dir, descriptor.file);
      if (location === 'final') {
        await publishStoredResult(f.dir, descriptor, image);
      } else {
        await writeFile(`${file}.part`, image, { mode: 0o600 });
      }
      const db = f.reopen(),
        outcomes = [];
      assert.deepEqual(await recover(db, f.dir, outcomes), {
        completed: 1,
        fileMissing: 0,
        unresolved: 0,
      });
      assert.deepEqual(outcomes, [
        { jobId: 'recover-0', status: 'completed', artifactId: descriptor.imageId },
      ]);
      assert.deepEqual(await readFile(file), image);
      await assert.rejects(stat(`${file}.part`), { code: 'ENOENT' });
      assert.deepEqual(
        db
          .prepare(
            'SELECT status,deadline_at,queue_expires_at FROM generation_jobs ORDER BY sample_index',
          )
          .all(),
        [
          { status: 'done', deadline_at: NOW + 100000, queue_expires_at: NOW + 90000 },
          { status: 'queued', deadline_at: NOW + 100000, queue_expires_at: NOW + 90000 },
        ],
      );
      const commit = readRelationalCommit(db, String(BigInt(event.commitSeq) + 1n));
      assert.equal(commit.command.commandId, `result-recovery:${event.eventId}`);
      assert.equal(commit.command.causationId, event.eventId);
      assert.ok(
        commit.events.every(
          (e) => e.policyVersion === 'original-policy' && e.pricingVersion === 'original-price',
        ),
      );
      assert.equal(db.prepare('SELECT status FROM charges').get().status, 'settled');
      for (const table of [
        'domain_events',
        'commits',
        'generation_jobs',
        'generation_attempts',
        'artifacts',
      ]) {
        assert.equal(
          JSON.stringify(db.prepare(`SELECT * FROM ${table}`).all()).includes(
            'private generation prompt',
          ),
          false,
        );
      }
      const before = databaseState(db),
        mtime = (await stat(file)).mtimeMs;
      assert.deepEqual(await recover(db, f.dir), { completed: 0, fileMissing: 0, unresolved: 0 });
      assert.equal(databaseState(db), before);
      assert.equal((await stat(file)).mtimeMs, mtime);
      assert.throws(() => assertRelationalStorageReady(db), /incomplete/);
    });
  }
});

test('准备字节缺失保持原预扣和完整事实，重复恢复不自动退款或重生成', async (t) => {
  const f = await recoveryFixture(t);
  await f.accept('missing');
  f.dispatch('missing-0');
  prepare(f, 'missing-0');
  const db = f.reopen(),
    before = databaseState(db),
    outcomes = [];
  for (let i = 0; i < 2; i++) {
    assert.deepEqual(await recover(db, f.dir, outcomes), {
      completed: 0,
      fileMissing: 1,
      unresolved: 0,
    });
    assert.equal(databaseState(db), before);
  }
  assert.ok(outcomes.every((o) => o.status === 'file_missing'));
  assert.equal(db.prepare('SELECT status FROM charges').get().status, 'reserved');
  assert.equal(db.prepare('SELECT status FROM generation_jobs').get().status, 'running');
});

test('截断或同长损坏的 final/part 均保留原字节和准备，不覆盖或误报缺失', async (t) => {
  for (const location of ['final', 'part']) {
    for (const damage of ['truncated', 'checksum']) {
      await t.test(`${location}-${damage}`, async (t) => {
        const f = await recoveryFixture(t);
        await f.accept('damaged');
        f.dispatch('damaged-0');
        const { descriptor } = prepare(f, 'damaged-0'),
          file = path.join(f.dir, descriptor.file) + (location === 'part' ? '.part' : ''),
          bytes = damage === 'truncated' ? image.subarray(0, 10) : Buffer.from(image);
        if (damage === 'checksum') {
          bytes[bytes.length - 1] ^= 1;
        }
        await writeFile(file, bytes);
        const db = f.reopen(),
          before = databaseState(db),
          outcomes = [];
        assert.deepEqual(await recover(db, f.dir, outcomes), {
          completed: 0,
          fileMissing: 0,
          unresolved: 1,
        });
        assert.equal(outcomes[0].code, 'ERR_STORED_RESULT_INVALID');
        assert.deepEqual(await readFile(file), bytes);
        assert.equal(databaseState(db), before);
      });
    }
  }
});

test('终态 SQL 失败被恢复外层捕获后整笔回滚，已发布文件在重试时只结算一次', async (t) => {
  const f = await recoveryFixture(t);
  await f.accept('fault', { samples: 2 });
  f.dispatch('fault-0');
  const { descriptor } = prepare(f, 'fault-0'),
    file = path.join(f.dir, descriptor.file);
  await writeFile(`${file}.part`, image);
  const db = f.reopen();
  let reached = 0;
  db.function('assert_prior_publication', (artifacts, status) => {
    assert.equal(artifacts, 1);
    assert.equal(status, 'settled');
    reached++;
    return 1;
  });
  db.exec(`CREATE TEMP TRIGGER fail_recovered_terminal BEFORE UPDATE OF status ON generation_jobs
    WHEN NEW.status='done' BEGIN SELECT assert_prior_publication((SELECT COUNT(*) FROM artifacts),(SELECT status FROM charges)); SELECT RAISE(ABORT,'terminal fault'); END`);
  const before = databaseState(db),
    outcomes = [];
  assert.deepEqual(await recover(db, f.dir, outcomes), {
    completed: 0,
    fileMissing: 0,
    unresolved: 1,
  });
  assert.equal(reached, 1);
  assert.equal(outcomes[0].status, 'unresolved');
  assert.equal(databaseState(db), before);
  assert.deepEqual(await readFile(file), image);
  db.exec('DROP TRIGGER fail_recovered_terminal');
  assert.deepEqual(await recover(db, f.dir), { completed: 1, fileMissing: 0, unresolved: 0 });
  const completed = databaseState(db);
  assert.deepEqual(await recover(db, f.dir), { completed: 0, fileMissing: 0, unresolved: 0 });
  assert.equal(databaseState(db), completed);
  assert.equal(
    db.prepare("SELECT COUNT(*) AS n FROM domain_events WHERE event_type='GenerationSettled'").get()
      .n,
    1,
  );
});

test('退款后重开恢复仍保留失败/原池退款与批次停止，旧尝试不改新凭据冷却', async (t) => {
  const f = await recoveryFixture(t, { seconds: 0 });
  await f.accept('late', { samples: 2 });
  const wallet = f.generations.readWallet('a', 'm1');
  f.dispatch('late-0');
  f.fail('late-0', { aborted: false });
  const { descriptor } = prepare(f, 'late-0', NOW + 4);
  await writeFile(path.join(f.dir, descriptor.file) + '.part', image);
  f.db
    .prepare('UPDATE upstream_accounts SET credential_epoch=2,cooldown_until=?,ordinary_strikes=3')
    .run(NOW + 60000);
  f.db.prepare("UPDATE members SET auth_epoch=NULL,status='disabled' WHERE id='m1'").run();
  const db = f.reopen();
  assert.deepEqual(await recover(db, f.dir), { completed: 1, fileMissing: 0, unresolved: 0 });
  assert.deepEqual(new RelationalGenerationStore(db).readWallet('a', 'm1'), wallet);
  assert.equal(db.prepare('SELECT status FROM charges').get().status, 'refunded');
  assert.deepEqual(
    db.prepare('SELECT status,late_result_at FROM generation_jobs ORDER BY sample_index').all(),
    [
      { status: 'failed', late_result_at: NOW + 20 },
      { status: 'failed', late_result_at: null },
    ],
  );
  assert.deepEqual(
    db
      .prepare('SELECT credential_epoch,cooldown_until,ordinary_strikes FROM upstream_accounts')
      .get(),
    {
      credential_epoch: 2,
      cooldown_until: NOW + 60000,
      ordinary_strikes: 3,
    },
  );
  assert.deepEqual(db.prepare("SELECT status,auth_epoch FROM members WHERE id='m1'").get(), {
    status: 'disabled',
    auth_epoch: null,
  });
  assert.deepEqual(
    db.prepare('SELECT review_status,actual_consumption FROM generation_attempts').get(),
    { review_status: 'result_recovered', actual_consumption: null },
  );
});

test('准备关系行与原事实冲突时在文件 IO 前拒绝，不能用现存文件补造来源', async (t) => {
  const f = await recoveryFixture(t);
  await f.accept('conflict');
  f.dispatch('conflict-0');
  const { descriptor } = prepare(f, 'conflict-0'),
    file = path.join(f.dir, descriptor.file);
  await writeFile(`${file}.part`, image);
  f.db.prepare('UPDATE generation_attempts SET prepared_sha256=?').run('a'.repeat(64));
  const before = databaseState(f.db),
    outcomes = [];
  assert.deepEqual(await recover(f.db, f.dir, outcomes), {
    completed: 0,
    fileMissing: 0,
    unresolved: 1,
  });
  assert.equal(databaseState(f.db), before);
  assert.deepEqual(await readFile(`${file}.part`), image);
  await assert.rejects(stat(file), { code: 'ENOENT' });
});

test('私有输入损坏时保留完整 part 和原收费，不发布产物或恢复私有认证', async (t) => {
  const f = await recoveryFixture(t);
  await f.accept('private-input');
  f.dispatch('private-input-0');
  const { descriptor } = prepare(f, 'private-input-0'),
    file = path.join(f.dir, descriptor.file),
    trigger = f.db
      .prepare("SELECT sql FROM sqlite_schema WHERE name='generation_inputs_identity'")
      .get().sql;
  await writeFile(`${file}.part`, image);
  // 模拟磁盘/快照内容损坏后恢复原 schema；产品写入仍受原不可变触发器约束。
  f.db.transaction(() => {
    f.db.exec('DROP TRIGGER generation_inputs_identity');
    f.db
      .prepare(
        "UPDATE generation_inputs SET body=json_set(body,'$.input','tampered private input')",
      )
      .run();
    f.db.exec(trigger);
  })();
  f.db.prepare("UPDATE members SET auth_epoch=NULL,status='disabled' WHERE id='m1'").run();
  const db = f.reopen(),
    before = databaseState(db),
    outcomes = [];
  assert.deepEqual(await recover(db, f.dir, outcomes), {
    completed: 0,
    fileMissing: 0,
    unresolved: 1,
  });
  assert.equal(outcomes[0].code, 'input_unavailable');
  assert.equal(databaseState(db), before);
  assert.deepEqual(await readFile(`${file}.part`), image);
  await assert.rejects(stat(file), { code: 'ENOENT' });
  assert.equal(db.prepare('SELECT status FROM charges').get().status, 'reserved');
});

test('准备查询按有界 job ID 翻页，拒绝无界参数；恢复不能跨文件 IO 持有事务', async (t) => {
  const f = await recoveryFixture(t);
  for (const [index, id] of ['c', 'a', 'b'].entries()) {
    const now = NOW + index * 100000;
    await f.accept(id, { now });
    f.dispatch(`${id}-0`, now + 1);
    prepare(f, `${id}-0`, now + 2);
    f.fail(`${id}-0`, { aborted: false }, now + 3);
  }
  assert.deepEqual(f.generations.readPendingGenerationResultIds('', 2), ['a-0', 'b-0']);
  assert.deepEqual(f.generations.readPendingGenerationResultIds('b-0', 2), ['c-0']);
  for (const limit of [0, -1, 101, 1.5, NaN]) {
    assert.throws(() => f.generations.readPendingGenerationResultIds('', limit), /Invalid pending/);
  }
  assert.throws(() => f.generations.readPendingGenerationResultIds(null), /Invalid pending/);
  const before = databaseState(f.db);
  let pending;
  f.db.transaction(() => {
    pending = recover(f.db, f.dir);
  })();
  await assert.rejects(pending, /cannot hold a transaction/);
  assert.equal(databaseState(f.db), before);
});

test('一百条之后的准备仍被恢复扫描，缺失字节不阻塞后一页且不写账本', async (t) => {
  const f = await recoveryFixture(t);
  for (let i = 0; i < 101; i++) {
    const id = `page-${String(i).padStart(3, '0')}`,
      now = NOW + i * 4;
    await f.accept(id, { now });
    f.dispatch(`${id}-0`, now + 1);
    prepare(f, `${id}-0`, now + 2, { ...f.response(now + 2), upstreamCompletedAt: null });
    f.fail(`${id}-0`, { aborted: false }, now + 3);
  }
  const before = databaseState(f.db),
    outcomes = [];
  assert.equal(f.generations.readPendingGenerationResultIds().length, 100);
  assert.deepEqual(await recover(f.db, f.dir, outcomes), {
    completed: 0,
    fileMissing: 101,
    unresolved: 0,
  });
  assert.equal(new Set(outcomes.map((o) => o.jobId)).size, 101);
  assert.equal(outcomes.at(-1).jobId, 'page-100-0');
  assert.equal(databaseState(f.db), before);
});

test('恢复产生的原完整提交可回放，重复零写入且不读取或重建文件', async (t) => {
  const f = await recoveryFixture(t);
  await f.accept('replay');
  f.dispatch('replay-0');
  const { descriptor } = prepare(f, 'replay-0'),
    file = path.join(f.dir, descriptor.file);
  await publishStoredResult(f.dir, descriptor, image);
  const target = new Database(f.db.serialize());
  t.after(() => target.close());
  target.pragma('foreign_keys=ON');
  assert.deepEqual(await recover(f.db, f.dir), { completed: 1, fileMissing: 0, unresolved: 0 });
  const seq = f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS n FROM commits').get().n,
    stored = readRelationalCommit(f.db, seq),
    generations = new RelationalGenerationStore(target);
  await unlink(file);
  target.transaction(() => generations.replayGenerationCommit(stored))();
  assert.equal(databaseState(target), databaseState(f.db));
  const before = target.serialize();
  target.transaction(() =>
    assert.equal(generations.replayGenerationCommit(stored).replayed, true),
  )();
  assert.ok(target.serialize().equals(before));
  await assert.rejects(stat(file), { code: 'ENOENT' });
});
