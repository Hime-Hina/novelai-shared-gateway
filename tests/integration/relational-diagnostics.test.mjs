import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fixture, meta, keys, NOW, databaseState } from '../helpers/relational-generation.mjs';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalAccountStore } from '../../server/storage/relational-account-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { relationalPrivateStateEmpty } from '../../server/storage/relational-private-restore.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { errorDetailMessage } from '../../server/http/public-errors.ts';
import { ownerAccountCommand } from '../../server/members/owner-operations.ts';
import {
  withAuthenticatedMemberApiKey,
  authenticateMemberApiKey,
} from '../../server/members/authentication.ts';

const sha256 = (value) => createHash('sha256').update(value).digest('hex');
function recovery(t, f, until = null) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const replayer = new RelationalCommitReplayer(db),
    replay = (stored, inputs = f.inputs) =>
      db.transaction(() => replayer.replayCommit(stored, inputs))();
  for (const { seq } of f.db
    .prepare(
      'SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE ? IS NULL OR commit_seq<=? ORDER BY commit_seq',
    )
    .all(until, until)) {
    replay(readRelationalCommit(f.db, seq));
  }
  return { db, replayer, replay, inputs: new RelationalInputStore(db) };
}
const diagnosticFact = (f, result) =>
  readRelationalCommit(f.db, result.commitSeq).events.find(
    (e) => e.eventType === 'DiagnosticRecorded',
  );
// 损坏副本的反例显式移除只读来源表的防改触发器，目标库保留全部生产约束。
function mutateSource(db, table, mutate) {
  const triggers = db
    .prepare("SELECT name,sql FROM sqlite_schema WHERE type='trigger' AND tbl_name=?")
    .all(table);
  for (const trigger of triggers) {
    db.exec(`DROP TRIGGER "${trigger.name}"`);
  }
  mutate();
  for (const trigger of triggers) {
    db.exec(trigger.sql);
  }
}

test('真实捕获先精确脱敏再限长，同笔失败、原资源退款与保护正文完整恢复', async (t) => {
  const f = await fixture(t, { seconds: 0 });
  await f.accept('diagnostic');
  const wallet = f.generations.readWallet('a', 'm1'),
    dispatch = f.dispatch('diagnostic-0').dispatch;
  assert.equal(Object.isFrozen(dispatch), true);
  const detail = `  private-path ${dispatch.pst} ${'x'.repeat(5000)}  `,
    result = f.fail('diagnostic-0', { aborted: false, diagnostic: { dispatch, error: detail } }),
    fact = diagnosticFact(f, result),
    expected = errorDetailMessage(detail, [dispatch.pst]);
  assert.equal(fact.payload.byteLength, Buffer.byteLength(expected));
  assert.equal(fact.payload.sha256, sha256(expected));
  assert.equal(expected.length, 4000);
  assert.equal(
    f.inputs.readGenerationDiagnostic(fact.payload.diagnosticId, 'diagnostic-0'),
    expected,
  );
  assert.ok(!JSON.stringify(readRelationalCommit(f.db, result.commitSeq)).includes('private-path'));
  assert.ok(!JSON.stringify(readRelationalCommit(f.db, result.commitSeq)).includes(dispatch.pst));
  assert.deepEqual(f.generations.readWallet('a', 'm1'), wallet);
  const target = recovery(t, f);
  assert.equal(
    target.inputs.readGenerationDiagnostic(fact.payload.diagnosticId, 'diagnostic-0'),
    expected,
  );
  assert.equal(relationalPrivateStateEmpty(target.db), true);
  assert.equal(await authenticateMemberApiKey(new RelationalMemberStore(target.db), keys.m1), null);
  const before = target.db.serialize();
  target.replay(readRelationalCommit(f.db, result.commitSeq));
  assert.deepEqual(target.db.serialize(), before);
  const original = f.db.serialize();
  assert.equal(
    f.fail('diagnostic-0', {
      aborted: false,
      diagnostic: {
        dispatch,
        error: {
          get stack() {
            throw new Error('receipt must not recapture');
          },
        },
      },
    }).replayed,
    true,
  );
  assert.deepEqual(f.db.serialize(), original);
});

test('诊断只接受原 owner 不可伪造对象、当前 system/null 和持久原派发事实', async (t) => {
  const f = await fixture(t);
  await f.accept('auth');
  const dispatch = f.dispatch('auth-0').dispatch,
    before = databaseState(f.db);
  for (const invalid of [{ ...dispatch }, null, undefined]) {
    assert.throws(
      () =>
        f.fail('auth-0', { aborted: false, diagnostic: { dispatch: invalid, error: 'private' } }),
      {
        statusCode: 403,
      },
    );
    assert.equal(databaseState(f.db), before);
  }
  for (const actor of [
    { actorKind: 'member', actorId: 'm1' },
    { actorKind: 'admin', actorId: 'admin' },
    { actorKind: 'system', actorId: 'm1' },
  ]) {
    f.db.transaction(() =>
      assert.throws(
        () =>
          f.generations.failGeneration({ ...meta('bad-actor'), ...actor }, 'auth-0', {
            aborted: false,
            errorCode: 'upstream_unavailable',
            errorStatus: 503,
            diagnostic: { dispatch, error: 'private' },
          }),
        { statusCode: 403 },
      ),
    )();
    assert.equal(databaseState(f.db), before);
  }
  f.db.transaction(() =>
    assert.throws(
      () =>
        new RelationalGenerationStore(f.db).failGeneration(meta('other-owner'), 'auth-0', {
          aborted: false,
          errorCode: 'upstream_unavailable',
          errorStatus: 503,
          diagnostic: { dispatch, error: 'private' },
        }),
      { statusCode: 403 },
    ),
  )();
  assert.equal(databaseState(f.db), before);
  f.fail('auth-0', { aborted: false, diagnostic: { dispatch, error: 'private' } });
});

test('原 trim 后限长允许截断末尾空白，失败退款与诊断原字节仍完整恢复', async (t) => {
  const f = await fixture(t, { seconds: 0 });
  await f.accept('whitespace');
  const wallet = f.generations.readWallet('a', 'm1'),
    dispatch = f.dispatch('whitespace-0').dispatch,
    raw = `${'x'.repeat(3999)} tail`,
    expected = errorDetailMessage(raw),
    result = f.fail('whitespace-0', { aborted: false, diagnostic: { dispatch, error: raw } }),
    fact = diagnosticFact(f, result);
  assert.equal(expected.length, 4000);
  assert.equal(expected.at(-1), ' ');
  assert.deepEqual(f.generations.readWallet('a', 'm1'), wallet);
  assert.equal(f.generations.readCharge('whitespace-0').status, 'refunded');
  assert.equal(f.db.prepare('SELECT status FROM generation_jobs').get().status, 'failed');
  assert.equal(
    f.inputs.readGenerationDiagnostic(fact.payload.diagnosticId, 'whitespace-0'),
    expected,
  );
  const target = recovery(t, f);
  assert.equal(
    target.inputs.readGenerationDiagnostic(fact.payload.diagnosticId, 'whitespace-0'),
    expected,
  );
});

test('限长边界的 Unicode 在保护内容 owner 统一为 UTF-8 可保存文本，退款与恢复校验原字节', async (t) => {
  for (const [index, raw] of [
    `${'x'.repeat(3999)}😀tail`,
    `${'x'.repeat(3998)}😀tail`,
    `${'x'.repeat(3999)}界tail`,
    `${'x'.repeat(3999)}\ntail`,
    `original\uD800tail`,
  ].entries()) {
    await t.test(String(index), async (t) => {
      const f = await fixture(t, { seconds: 0 });
      await f.accept('unicode');
      const wallet = f.generations.readWallet('a', 'm1'),
        dispatch = f.dispatch('unicode-0').dispatch,
        expected = errorDetailMessage(raw).toWellFormed(),
        result = f.fail('unicode-0', { aborted: false, diagnostic: { dispatch, error: raw } }),
        fact = diagnosticFact(f, result);
      assert.equal(expected.isWellFormed(), true);
      assert.equal(errorDetailMessage(raw), raw.trim().slice(0, 4000));
      assert.equal(expected, raw.trim().slice(0, 4000).toWellFormed());
      assert.deepEqual(f.generations.readWallet('a', 'm1'), wallet);
      assert.equal(
        f.inputs.readGenerationDiagnostic(fact.payload.diagnosticId, 'unicode-0'),
        expected,
      );
      const target = recovery(t, f);
      assert.equal(
        target.inputs.readGenerationDiagnostic(fact.payload.diagnosticId, 'unicode-0'),
        expected,
      );
      assert.equal(fact.payload.byteLength, Buffer.byteLength(expected));
      assert.equal(fact.payload.sha256, sha256(expected));
    });
  }
});

test('外层回滚留下的派发快照即使同 attempt 重试也不是原捕获票据', async (t) => {
  const f = await fixture(t);
  await f.accept('ghost');
  let ghost;
  assert.throws(
    () =>
      f.db.transaction(() => {
        ghost = f.dispatch('ghost-0').dispatch;
        throw new Error('rollback dispatch');
      })(),
    /rollback dispatch/,
  );
  const dispatch = f.dispatch('ghost-0').dispatch,
    before = databaseState(f.db);
  assert.deepEqual(ghost, dispatch);
  assert.throws(
    () => f.fail('ghost-0', { aborted: false, diagnostic: { dispatch: ghost, error: 'private' } }),
    {
      statusCode: 403,
    },
  );
  assert.equal(databaseState(f.db), before);
  f.fail('ghost-0', { aborted: false, diagnostic: { dispatch, error: 'private' } });
});

test('诊断后续投影 SQL 失败被外层捕获时退款/事实/正文全回滚，可用原命令重试', async (t) => {
  const f = await fixture(t);
  await f.accept('atomic');
  const dispatch = f.dispatch('atomic-0').dispatch;
  f.db.exec(
    "CREATE TEMP TRIGGER fail_diagnostic_terminal BEFORE UPDATE ON generation_jobs WHEN NEW.status='failed' BEGIN SELECT RAISE(ABORT,'diagnostic terminal fault'); END",
  );
  const before = databaseState(f.db);
  f.db.transaction(() =>
    assert.throws(
      () =>
        f.fail('atomic-0', {
          aborted: false,
          diagnostic: { dispatch, error: 'protected private detail' },
        }),
      /diagnostic terminal fault/,
    ),
  )();
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER fail_diagnostic_terminal');
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_diagnostics').get().n, 0);
  const result = f.fail('atomic-0', {
    aborted: false,
    diagnostic: { dispatch, error: 'protected private detail' },
  });
  assert.ok(diagnosticFact(f, result));
});

test('原在途凭据 epoch 变化仍可诊断与原资源退款，原票据脱敏不改当前凭据或冷却', async (t) => {
  const f = await fixture(t, { seconds: 0 });
  await f.accept('stale');
  const wallet = f.generations.readWallet('a', 'm1'),
    dispatch = f.dispatch('stale-0').dispatch;
  // 与既有迟到结果反例相同，直接表示外部恢复后的当前代次；不绕过在线 owner 开放轮换。
  f.db
    .prepare('UPDATE upstream_accounts SET credential_epoch=3,cooldown_until=?,ordinary_strikes=3')
    .run(NOW + 60000);
  f.db.prepare('UPDATE upstream_credentials SET pst=?').run('pst-synthetic-current-A');
  const current = f.db
    .prepare('SELECT credential_epoch,cooldown_until,ordinary_strikes FROM upstream_accounts')
    .get();
  const result = f.fail(
      'stale-0',
      {
        aborted: false,
        diagnostic: { dispatch, error: `old response ${dispatch.pst}` },
        evidence: { requestStarted: true },
        completion: {
          kind: 'ordinary',
          attemptId: dispatch.attemptId,
          credentialEpoch: dispatch.credentialEpoch,
        },
      },
      NOW + 20,
    ),
    fact = diagnosticFact(f, result);
  assert.deepEqual(f.generations.readWallet('a', 'm1'), wallet);
  assert.deepEqual(
    f.db
      .prepare('SELECT credential_epoch,cooldown_until,ordinary_strikes FROM upstream_accounts')
      .get(),
    current,
  );
  assert.equal(
    f.inputs.readGenerationDiagnostic(fact.payload.diagnosticId, 'stale-0'),
    'old response [REDACTED]',
  );
  assert.equal(
    f.db.prepare('SELECT pst FROM upstream_credentials').get().pst,
    'pst-synthetic-current-A',
  );
});

test('真实 A1→B→A2 轮换后原失败回执仍零写且不重捕获诊断', async (t) => {
  const env = { OWNER_USER_ID: 'm1' },
    f = await fixture(t, { env, seconds: 0 });
  await f.accept('rotate');
  const wallet = f.generations.readWallet('a', 'm1'),
    dispatch = f.dispatch('rotate-0').dispatch,
    accounts = new RelationalAccountStore(
      f.db,
      new RelationalQuotaStore(f.db, env),
      new Map(),
      env,
    );
  const result = f.fail('rotate-0', {
      aborted: false,
      diagnostic: { dispatch, error: `old response ${dispatch.pst}` },
    }),
    fact = diagnosticFact(f, result);
  for (const [id, token] of [
    ['B', 'pst-synthetic-B'],
    ['A2', 'pst-synthetic-current-A'],
  ]) {
    await ownerAccountCommand(
      accounts,
      (write) => withAuthenticatedMemberApiKey(f.members, keys.m1, write, env),
      meta(`rotate-${id}`, NOW + 10, 'm1'),
      { kind: 'token', accountId: 'a', token },
      async () => {
        throw new Error('rotation cannot query upstream');
      },
    );
  }
  const current = f.db
    .prepare('SELECT credential_epoch,cooldown_until,ordinary_strikes FROM upstream_accounts')
    .get();
  assert.equal(current.credential_epoch, 3);
  const before = databaseState(f.db),
    repeated = f.fail(
      'rotate-0',
      {
        aborted: false,
        diagnostic: {
          dispatch,
          error: {
            get stack() {
              throw new Error('must not recapture');
            },
          },
        },
        evidence: { requestStarted: true },
        completion: { kind: 'ordinary' },
      },
      NOW + 20,
    );
  assert.equal(repeated.replayed, true);
  assert.equal(databaseState(f.db), before);
  assert.deepEqual(f.generations.readWallet('a', 'm1'), wallet);
  assert.deepEqual(
    f.db
      .prepare('SELECT credential_epoch,cooldown_until,ordinary_strikes FROM upstream_accounts')
      .get(),
    current,
  );
  assert.equal(
    f.inputs.readGenerationDiagnostic(fact.payload.diagnosticId, 'rotate-0'),
    'old response [REDACTED]',
  );
  assert.equal(
    f.db.prepare('SELECT pst FROM upstream_credentials').get().pst,
    'pst-synthetic-current-A',
  );
});

test('完整提交的保护源、摘要/长度和同笔失败关系不可替换，错误外层捕获后全表不变', async (t) => {
  const f = await fixture(t);
  await f.accept('restore');
  const dispatch = f.dispatch('restore-0').dispatch,
    beforeSeq = f.db.prepare('SELECT MAX(commit_seq) AS seq FROM commits').get().seq,
    result = f.fail('restore-0', {
      aborted: false,
      diagnostic: { dispatch, error: 'source private detail' },
    }),
    stored = readRelationalCommit(f.db, result.commitSeq),
    fact = diagnosticFact(f, result);
  for (const mode of ['missing-source', 'body', 'wrong-commit', 'missing-reference']) {
    await t.test(mode, async (t) => {
      const target = recovery(t, f, beforeSeq),
        before = databaseState(target.db);
      if (mode === 'missing-source') {
        target.db.transaction(() => assert.throws(() => target.replayer.replayCommit(stored)))();
      } else if (mode === 'missing-reference') {
        const broken = structuredClone(stored);
        for (const events of [broken.events, broken.command.events]) {
          events.find((e) => e.eventType === 'JobFailed').payload.diagnosticId = null;
        }
        const source = new Database(f.db.serialize());
        try {
          source.pragma('foreign_keys=ON');
          mutateSource(source, 'domain_events', () =>
            source
              .prepare(
                "UPDATE domain_events SET payload=json_set(payload,'$.diagnosticId',NULL) WHERE event_type='JobFailed'",
              )
              .run(),
          );
          target.db.transaction(() =>
            assert.throws(() =>
              target.replayer.replayCommit(broken, new RelationalInputStore(source)),
            ),
          )();
        } finally {
          source.close();
        }
      } else {
        const source = new Database(f.db.serialize());
        try {
          source.pragma('foreign_keys=ON');
          if (mode === 'body') {
            source
              .prepare('UPDATE generation_diagnostics SET detail=? WHERE id=?')
              .run('changed private detail', fact.payload.diagnosticId);
          } else {
            mutateSource(source, 'commits', () =>
              source
                .prepare('UPDATE commits SET receipt=? WHERE commit_seq=?')
                .run('{"different":true}', result.commitSeq),
            );
          }
          target.db.transaction(() =>
            assert.throws(() =>
              target.replayer.replayCommit(stored, new RelationalInputStore(source)),
            ),
          )();
        } finally {
          source.close();
        }
      }
      assert.equal(databaseState(target.db), before);
      target.replay(stored);
      assert.equal(
        target.inputs.readGenerationDiagnostic(fact.payload.diagnosticId, 'restore-0'),
        'source private detail',
      );
    });
  }
});

test('诊断与事件联合恢复后可经真实完整核对安装 ready，旧 Key 不复活', async (t) => {
  const f = await fixture(t);
  await f.accept('ready-diagnostic');
  const dispatch = f.dispatch('ready-diagnostic-0').dispatch,
    result = f.fail('ready-diagnostic-0', {
      aborted: false,
      diagnostic: { dispatch, error: 'protected recovery detail' },
    }),
    fact = diagnosticFact(f, result);
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'rotated-diagnostic-key'))();
  const sourcePath = path.join(f.dir, 'diagnostic-snapshot.sqlite');
  await f.db.backup(sourcePath);
  const sealed = new Database(sourcePath);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  const target = recovery(t, f);
  assert.equal(relationalPrivateStateEmpty(target.db), true);
  await verifyAndInstallRelationalStorage(target.db, {
    sourcePath,
    sourceSha256: sha256(await readFile(sourcePath)),
    historyId: 'generation-history',
    finalCommitSeq: result.commitSeq,
    replicas: { dataDir: f.dir },
  });
  assert.doesNotThrow(() => assertRelationalStorageReady(target.db));
  assert.equal(
    target.inputs.readGenerationDiagnostic(fact.payload.diagnosticId, 'ready-diagnostic-0'),
    'protected recovery detail',
  );
  const members = new RelationalMemberStore(target.db);
  assert.equal(await authenticateMemberApiKey(members, keys.m1), null);
  assert.equal((await authenticateMemberApiKey(members, 'rotated-diagnostic-key')).id, 'm1');
});
