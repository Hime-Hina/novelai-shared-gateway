import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { fixture, NOW, meta, keys, databaseState } from '../helpers/relational-generation.mjs';
import { RelationalReportStore } from '../../server/storage/relational-report-store.ts';
import { relationalFailureTimeSql } from '../../server/storage/relational-schema.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { publicErrorLog, selectErrorLogJobs } from '../../server/reports/generation-errors.ts';
import { relationalInputDisplay } from '../../server/generation/relational-input.ts';
import { normalizeNovelAiRequest } from '../../server/protocols/novelai-client.ts';
import { nativeNormalizationContext } from '../../server/protocols/native-image.ts';
import { requestNormalizationDefaults } from '../../server/generation/request-fingerprint.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';

const iso = (at) => new Date(at).toISOString();
async function fail(f, id, { detail = 'private stack', memberId = 'm1', at = NOW + 2 } = {}) {
  await f.accept(id, { memberId });
  const dispatch = f.dispatch(`${id}-0`).dispatch;
  return f.fail(`${id}-0`, { aborted: false, diagnostic: { dispatch, error: detail } }, at);
}
function trace(db) {
  const sql = [],
    prepare = db.prepare.bind(db);
  db.prepare = (query) => {
    sql.push(query);
    return prepare(query);
  };
  return { sql, prepare };
}

test('完整 v1 去重显示字段共用解码，partial 和旧 DTO 不冒充已保存原步数', () => {
  const body = { input: 'subject', parameters: { steps: 28 } };
  assert.deepEqual(relationalInputDisplay(body, true), { tag: 'subject', requestedSteps: 28 });
  assert.deepEqual(relationalInputDisplay(body, false), { tag: null, requestedSteps: null });
  assert.deepEqual(
    relationalInputDisplay({ ...body, displayPrompt: '', requestedSteps: 0 }, false),
    { tag: '', requestedSteps: 0 },
  );
  assert.equal(
    publicErrorLog(
      { id: 'old' },
      { steps: 28 },
      { maskedUserToken: null, accountRouteId: null, durationMs: null },
    ).request.requestedSteps,
    null,
  );
});

test('真实关系错误沿原白名单/过滤 owner，缺失旧字段 NULL，原规范输入冷读不加载 BLOB', async (t) => {
  const f = await fixture(t);
  await fail(f, 'one', { detail: 'original private detail' });
  await fail(f, 'two', { memberId: 'm2', at: NOW + 3 });
  const before = databaseState(f.db),
    captured = trace(f.db),
    reader = new RelationalReportStore(f.db),
    errors = reader.readErrorLogs(NOW + 2);
  assert.deepEqual(
    errors.map((e) => e.id),
    ['two-0', 'one-0'],
  );
  assert.equal(errors[1].errorDetail, 'original private detail');
  assert.equal(errors[1].request.prompt, 'private generation prompt');
  assert.equal(errors[1].request.requestedSteps, 30);
  assert.equal(errors[1].request.routedSteps, 30);
  assert.equal(errors[1].request.seed, 42);
  assert.equal(errors[1].durationMs, 2);
  for (const row of errors) {
    for (const field of [
      'userToken',
      'accountRouteId',
      'retryCount',
      'cost',
      'accountCost',
      'queueTotal',
    ]) {
      assert.equal(row[field], null);
    }
    assert.equal(row.request.artist, null);
    assert.equal(row.request.size, null);
    assert.ok(!JSON.stringify(row).includes(keys.m1));
    assert.ok(!JSON.stringify(row).includes('synthetic-upstream-pst'));
  }
  assert.equal(databaseState(f.db), before);
  assert.ok(
    captured.sql.every((sql) => !/SELECT\s+bytes\b|SELECT\s+\*\s+FROM\s+input_assets/i.test(sql)),
  );
  assert.deepEqual(reader.readErrorLogs(NOW + 4), []);
  const thin = captured.sql.find((sql) => sql.includes('AS failedAt')),
    plan = captured.prepare(`EXPLAIN QUERY PLAN ${thin}`).all(NOW + 2);
  assert.ok(
    plan.some((row) => /SEARCH e USING INDEX idx_job_failure_time/.test(row.detail)),
    JSON.stringify(plan),
  );
  assert.ok(
    plan.every((row) => !/SCAN j\b|TEMP B-TREE FOR ORDER BY/.test(row.detail)),
    JSON.stringify(plan),
  );
  t.diagnostic(`error-time: ${JSON.stringify(plan)}`);
});

test('失败时间表达式保留真实 0/未知与开账 header 角色，薄 DTO 的原创建时间 0 有效', async (t) => {
  const db = new Database(':memory:');
  try {
    const rows = db
      .prepare(
        `WITH events(event_type,occurred_at,payload) AS (VALUES
      ('OpeningJobRecorded',999,'{"completedAt":0}'),
      ('OpeningJobRecorded',999,'{"completedAt":null}'),
      ('JobFailed',0,'{}')) SELECT ${relationalFailureTimeSql} AS at FROM events`,
      )
      .all();
    assert.deepEqual(rows, [{ at: 0 }, { at: null }, { at: 0 }]);
  } finally {
    db.close();
  }
  const f = await fixture(t, { env: { QUEUE_WAIT_TIMEOUT_MS: String(NOW + 10000) } });
  await f.accept('zero-time', { now: 0 });
  f.dispatch('zero-time-0');
  f.fail('zero-time-0', { aborted: false });
  const row = new RelationalReportStore(f.db).readErrorLogs(NOW)[0];
  assert.equal(row.createdAt, iso(0));
  assert.equal(row.durationMs, NOW + 2);
});

test('旧 route/artist/size 不从当前配置补造；实际受理的不同原步数与路由步数保留', async (t) => {
  const f = await fixture(t),
    settings = {
      defaultArtist: 'artist prefix',
      defaultNegative: null,
      defaults: { steps: '30', scale: 5, cfg: 0 },
    },
    request = normalizeNovelAiRequest(
      { tag: 'subject', width: 128, height: 128, seed: 7 },
      settings,
      { maxSteps: 28 },
    ),
    input = {
      inputId: 'input-direct-error',
      requestId: 'direct-error',
      memberId: 'm1',
      source: 'openai',
      seedMode: 'fixed',
      confirmation: null,
      request,
      context: {
        kind: 'direct',
        native: nativeNormalizationContext(),
        defaults: requestNormalizationDefaults(settings),
      },
      jobs: [
        {
          jobId: 'direct-error-0',
          status: 'pending',
          queueSeq: null,
          queueExpiresAt: null,
          deadlineAt: null,
          admissionEpoch: 0,
          cacheKey: null,
        },
      ],
    };
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    () => f.generations.acceptGeneration(meta('accept-direct-error', NOW, 'm1'), input),
    {},
  );
  const dispatch = f.dispatch('direct-error-0').dispatch;
  f.fail('direct-error-0', {
    aborted: false,
    diagnostic: { dispatch, error: 'direct private' },
  });
  const row = new RelationalReportStore(f.db).readErrorLogs(NOW)[0];
  assert.equal(row.request.tag, 'subject');
  assert.equal(row.request.prompt, 'artist prefix\nsubject');
  assert.equal(row.request.requestedSteps, 30);
  assert.equal(row.request.routedSteps, 28);
  assert.equal(row.request.artist, null);
  assert.equal(row.request.size, null);
  assert.equal(row.request.cfg, 0);
  assert.equal(row.request.negative, '');
});

test('迟到完整结果不抹原失败时间/详情；重开和完整业务回放读取不回填正文', async (t) => {
  const f = await fixture(t);
  await fail(f, 'late');
  const initial = new RelationalReportStore(f.db).readErrorLogs(NOW);
  await f.publish('late-0', NOW + 100);
  const after = new RelationalReportStore(f.db).readErrorLogs(NOW);
  assert.deepEqual(after, initial);
  const reopened = new Database(path.join(f.dir, 'library.sqlite'));
  t.after(() => {
    if (reopened.open) {
      reopened.close();
    }
  });
  const before = databaseState(reopened);
  assert.deepEqual(new RelationalReportStore(reopened).readErrorLogs(NOW), initial);
  assert.equal(databaseState(reopened), before);
  reopened.close();
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replayer = new RelationalCommitReplayer(target);
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), f.inputs))();
  }
  const recovered = databaseState(target);
  assert.deepEqual(new RelationalReportStore(target).readErrorLogs(NOW), initial);
  assert.equal(databaseState(target), recovered);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM generation_inputs').get().n, 1);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM generation_diagnostics').get().n, 1);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
});

test('原共同过滤先于请求正文；损坏或跨成员输入/诊断不泄漏，合法当前 Key 撤销不阻管理历史', async (t) => {
  const f = await fixture(t);
  await fail(f, 'visible');
  await fail(f, 'capacity', { detail: 'Concurrent generation is locked' });
  assert.equal(
    selectErrorLogJobs(
      [
        {
          id: 'x',
          status: 'failed',
          accountId: 'a',
          errorDetail: 'Concurrent generation is locked',
          updatedAt: iso(NOW),
        },
      ],
      NOW,
      1,
    ).length,
    0,
  );
  const captured = trace(f.db),
    reader = new RelationalReportStore(f.db);
  assert.deepEqual(
    reader.readErrorLogs(NOW).map((e) => e.id),
    ['visible-0'],
  );
  const inputReads = captured.sql.filter((sql) =>
    sql.includes('SELECT id,member_id,input_id,context_id'),
  );
  assert.equal(inputReads.length, 1);
  f.db
    .prepare(
      "UPDATE generation_diagnostics SET detail='tampered private detail' WHERE job_id='visible-0'",
    )
    .run();
  const trigger = f.db
    .prepare("SELECT sql FROM sqlite_schema WHERE name='generation_inputs_identity'")
    .get().sql;
  f.db.exec('DROP TRIGGER generation_inputs_identity');
  // 显式损坏源模拟跨成员引用；正常新库的 FK 本身先拒绝该更新。
  f.db.pragma('foreign_keys=OFF');
  f.db.prepare("UPDATE generation_inputs SET member_id='m2' WHERE id='input-visible'").run();
  f.db.pragma('foreign_keys=ON');
  f.db.exec(trigger);
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'new-synthetic-key'))();
  const before = databaseState(f.db),
    result = reader.readErrorLogs(NOW);
  assert.equal(result[0].errorDetail, null);
  assert.equal(result[0].request.prompt, null);
  assert.ok(!JSON.stringify(result).includes('tampered private'));
  assert.equal(databaseState(f.db), before);
});

test('过滤超过一批的候选后仍继续原有界时间分页，拒绝非法范围且零写', async (t) => {
  const f = await fixture(t);
  await fail(f, 'old-visible', { at: NOW + 2 });
  for (let i = 0; i < 101; i++) {
    await fail(f, `filtered-${i}`, { detail: 'Concurrent generation is locked', at: NOW + 3 });
  }
  const before = databaseState(f.db),
    reader = new RelationalReportStore(f.db),
    captured = trace(f.db);
  assert.deepEqual(
    reader.readErrorLogs(NOW, 1).map((e) => e.id),
    ['old-visible-0'],
  );
  const queries = captured.sql.filter((sql) => sql.includes('AS failedAt'));
  assert.equal(queries.length, 2);
  assert.match(queries[1], /e\.event_id\)<\(\?,\?\)/);
  for (const [since, limit] of [
    [-1, 1],
    [NaN, 1],
    [NOW, 0],
    [NOW, 101],
    [NOW, 1.2],
  ]) {
    assert.throws(() => reader.readErrorLogs(since, limit), { statusCode: 400 });
  }
  assert.equal(databaseState(f.db), before);
});
