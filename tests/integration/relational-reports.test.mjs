import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import {
  fixture,
  NOW,
  keys,
  image,
  meta,
  databaseState,
} from '../helpers/relational-generation.mjs';
import { RelationalReportStore } from '../../server/storage/relational-report-store.ts';
import { RelationalRequestStore } from '../../server/storage/relational-request-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { JsonStore } from '../../server/storage/sqlite-store.ts';
import { summarizeOwnerUsage } from '../../server/members/owner-operations.ts';
import { readActivityHeatmap } from '../../server/reports/activity-heatmap.ts';
import { publicGenerationRequest } from '../../server/generation/request-queries.ts';
import {
  normalizeMemberGeneration,
  materializeMemberGeneration,
} from '../../server/generation/member-contract.ts';
import { withAuthenticatedMemberApiKey, isOwner } from '../../server/members/authentication.ts';
import {
  appendRelationalCommit,
  readRelationalCommit,
  replayRelationalCommit,
} from '../../server/events/relational-event-store.ts';

const day = 86400000,
  boundary = Math.floor((NOW + 8 * 3600000) / day) * day - 8 * 3600000;
const iso = (value) => new Date(value).toISOString();

test('仍在线旧统计的合法 number 沿既有 nearest 规则读取，新关系字符串保持 exact', async (t) => {
  const dir = await mkdtemp(path.join(tmpdir(), 'nai-report-legacy-number-')),
    store = new JsonStore(dir);
  await store.init();
  t.after(async () => {
    await store.close();
    await rm(dir, { recursive: true, force: true });
  });
  const db = structuredClone(store.db),
    amount = 0.21175719010416666;
  db.users = [{ id: 'synthetic-member', token: 'synthetic-key', enabled: true }];
  db.jobs = [
    {
      id: 'historical-job',
      userToken: 'synthetic-key',
      status: 'done',
      createdAt: iso(NOW),
      fairCharge: { status: 'settled', resource: 'v5', amount, fixed: 0, purchased: 0 },
    },
  ];
  await store.write(db);
  const rows = await store.readOwnerJobRows('', iso(NOW + day)),
    before = await store.read();
  assert.equal(rows[0].amount, amount);
  const dto = summarizeOwnerUsage(rows);
  assert.equal(dto.totals.v5, 0.211757190104);
  assert.equal(dto.recent[0].amount, dto.totals.v5);
  assert.deepEqual(await store.read(), before);
  assert.throws(() => summarizeOwnerUsage([{ ...rows[0], amount: String(amount) }]), /12 decimal/);
});

// 只构造查询所需的有来源历史；Opening 的业务投影/全库恢复仍属 #50/#51。
async function history(
  t,
  { opening = false, createdAt = NOW, headerAt = NOW + 1, secondAt = null } = {},
) {
  const f = await fixture(t),
    baseline = f.db.serialize();
  await f.accept('history', { samples: secondAt === null ? 1 : 2 });
  const seq = f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get().seq,
    stored = structuredClone(readRelationalCommit(f.db, seq)),
    origin = { sourceId: 'synthetic-opening', originalId: 'original-history' };
  stored.command.actorKind = 'system';
  stored.command.actorId = null;
  stored.command.recordedAt = Math.max(headerAt, secondAt ?? headerAt);
  for (const list of [stored.command.events, stored.events]) {
    for (const event of list) {
      if (event.eventType === 'RequestAccepted') {
        event.occurredAt = boundary - day;
        if (opening) {
          event.eventType = 'OpeningRequestRecorded';
          Object.assign(event.payload, { acceptedAt: boundary - day, retiredAt: null, origin });
        }
      } else if (event.eventType === 'JobAccepted') {
        event.occurredAt =
          secondAt !== null && event.payload.sampleIndex === 1 ? secondAt : createdAt;
        if (opening) {
          event.occurredAt = headerAt;
          event.eventType = 'OpeningJobRecorded';
          Object.assign(event.payload, {
            createdAt,
            artifactId: null,
            completedAt: null,
            errorCode: null,
            errorStatus: null,
            cancellationReason: null,
            lateResultAt: null,
            freeSucceededAt: null,
            freeWarningThreshold: null,
            retiredAt: null,
            origin,
          });
        }
      }
    }
  }
  const source = new Database(baseline),
    target = new Database(baseline);
  t.after(() => {
    source.close();
    target.close();
  });
  source.pragma('foreign_keys=ON');
  target.pragma('foreign_keys=ON');
  source.transaction(() => replayRelationalCommit(source, stored))();
  for (const table of ['normalization_contexts', 'generation_inputs', 'input_assets']) {
    for (const row of f.db.prepare(`SELECT * FROM ${table}`).all()) {
      const names = Object.keys(row);
      source
        .prepare(
          `INSERT OR IGNORE INTO ${table}(${names.join(',')}) VALUES(${names.map(() => '?').join(',')})`,
        )
        .run(...Object.values(row));
    }
  }
  if (opening) {
    target.transaction(() => replayRelationalCommit(target, stored))();
    for (const table of [
      'normalization_contexts',
      'generation_inputs',
      'input_assets',
      'generation_requests',
      'generation_jobs',
    ]) {
      for (const row of f.db.prepare(`SELECT * FROM ${table}`).all()) {
        if (table === 'generation_requests') {
          row.accepted_at = boundary - day;
        }
        const names = Object.keys(row);
        target
          .prepare(
            `INSERT OR IGNORE INTO ${table}(${names.join(',')}) VALUES(${names.map(() => '?').join(',')})`,
          )
          .run(...Object.values(row));
      }
    }
  } else {
    target.transaction(() =>
      new RelationalInputStore(target).replayAcceptedInput(new RelationalInputStore(source), seq),
    )();
  }
  return { f, db: target, reports: new RelationalReportStore(target) };
}

test('owner 用量复用实际行与原汇总 owner，三池不重复计费、退款迟到仍归失败', async (t) => {
  const f = await fixture(t);
  await f.accept('success');
  f.dispatch('success-0');
  await f.publish('success-0');
  await f.accept('late');
  assert.ok(f.dispatch('late-0', NOW + 10005).dispatch);
  f.fail('late-0', { aborted: false }, NOW + 10006);
  await f.publish('late-0', NOW + 10007);
  await f.accept('cached');
  // 缓存命令/投影属后续生命周期片；此处仅验证已有明确事实的查询，不模拟实际 HTTP 缓存命中。
  const { policyVersion, pricingVersion, ...metadata } = meta('cached-link', NOW + 10009);
  const version = f.db
    .prepare(
      'SELECT CAST(MAX(stream_version) AS TEXT) AS version FROM domain_events WHERE stream_id=?',
    )
    .get('request:cached').version;
  f.db.transaction(() => {
    const result = appendRelationalCommit(f.db, {
      ...metadata,
      expectedVersions: { 'request:cached': version },
      receipt: { linked: true },
      events: [
        {
          eventType: 'CachedResultLinked',
          streamId: 'request:cached',
          occurredAt: NOW + 10009,
          memberId: 'm1',
          accountId: null,
          policyVersion,
          pricingVersion,
          payload: { jobId: 'cached-0', artifactId: 'artifact-success-0' },
        },
      ],
    });
    f.db
      .prepare(
        "UPDATE generation_jobs SET status='done',artifact_id=?,source_event_id=?,completed_at=? WHERE id=?",
      )
      .run('artifact-success-0', result.events[0].eventId, NOW + 10009, 'cached-0');
  })();
  const reports = new RelationalReportStore(f.db),
    before = f.db.serialize();
  const rows = await reports.readOwnerJobRows('', iso(NOW + day)),
    result = summarizeOwnerUsage(rows);
  assert.equal(result.coverage, 'retained-records');
  assert.equal(result.totals.total, 3);
  assert.equal(result.totals.done, 2);
  assert.equal(result.totals.failed, 1);
  assert.equal(result.totals.refunded, 1);
  assert.equal(result.totals.fixed, 2);
  assert.equal(result.totals.purchased, 0);
  assert.equal(result.totals.v5, 0);
  assert.equal(result.recent.find((row) => row.id === 'late-0').chargeStatus, 'refunded');
  assert.equal(result.recent.find((row) => row.id === 'success-0').generationSeconds, 0.002);
  assert.equal(result.recent.find((row) => row.id === 'cached-0').knownNoCharge, true);
  assert.deepEqual(f.db.serialize(), before);
});

test('原统计 owner 定点累加后才转 DTO；金额/创建时间未知不能当零或迁移日', () => {
  const row = {
    id: 'x',
    memberId: 'm',
    username: 'same label',
    model: null,
    status: 'done',
    createdAt: iso(NOW),
    chargeStatus: 'settled',
    resource: 'anlas',
    purchased: '0',
  };
  assert.equal(
    summarizeOwnerUsage([
      { ...row, fixed: '0.1' },
      { ...row, fixed: '0.2' },
    ]).totals.fixed,
    0.3,
  );
  assert.equal(
    summarizeOwnerUsage([
      { ...row, fixed: '9007199254740992' },
      { ...row, fixed: '1' },
      { ...row, fixed: '1' },
    ]).totals.fixed,
    9007199254740994,
  );
  const unknown = summarizeOwnerUsage([{ ...row, createdAt: null, fixed: null }]);
  assert.equal(unknown.totals.fixed, null);
  assert.equal(unknown.totals.purchased, 0);
  assert.equal(unknown.days[0].id, 'unknown');
  assert.equal(unknown.days[0].label, '未知日期');
  assert.equal(unknown.totals.averageWaitSeconds, null);
  assert.equal(summarizeOwnerUsage([{ ...row, chargeStatus: null }]).totals.v5, null);
  assert.equal(
    summarizeOwnerUsage([{ ...row, chargeStatus: null, knownNoCharge: true }]).totals.fixed,
    0,
  );
  assert.equal(summarizeOwnerUsage([]).coverage, 'retained-records');
  assert.equal(
    summarizeOwnerUsage([
      { ...row, fixed: '1', memberId: 'm1' },
      { ...row, fixed: '1', memberId: 'm2' },
    ]).members.length,
    2,
  );
});

test('child 原受理时刻独立于父时间，上海边界与热力图左闭右开保持', async (t) => {
  const { reports, db } = await history(t, { createdAt: boundary - 1, secondAt: boundary });
  const query = {
    since: iso(boundary - day),
    until: iso(boundary + day),
    memberId: 'm1',
    operation: 'generate',
    dates: null,
  };
  const days = await reports.readActivityDays(query);
  assert.deepEqual(
    days.map((value) => value.total),
    [1, 1],
  );
  assert.deepEqual(
    (await reports.readActivityDays({ ...query, until: iso(boundary) })).map(
      (value) => value.total,
    ),
    [1],
  );
  assert.deepEqual(await reports.readActivityDays({ ...query, dates: [] }), []);
  assert.equal((await reports.readActivityDays({ ...query, dates: [days[1].date] }))[0].total, 1);
  const dto = publicGenerationRequest(
    new RelationalRequestStore(db).readMemberRequest('history', 'm1'),
  );
  assert.equal(dto.createdAt, iso(boundary - day));
  assert.deepEqual(
    dto.jobs.map((job) => job.createdAt),
    [iso(boundary - 1), iso(boundary)],
  );
});

test('Opening 使用可证明的原创建字段，未知 NULL 不落入 opening header 当天', async (t) => {
  for (const createdAt of [boundary - day, null]) {
    await t.test(String(createdAt), async (t) => {
      const { reports, db } = await history(t, {
        opening: true,
        createdAt,
        headerAt: boundary + 1000,
      });
      const rows = await reports.readOwnerJobRows('', iso(boundary + day));
      assert.equal(rows[0].createdAt, createdAt === null ? null : iso(createdAt));
      const dto = publicGenerationRequest(
        new RelationalRequestStore(db).readMemberRequest('history', 'm1'),
      );
      assert.equal(dto.jobs[0].createdAt, rows[0].createdAt);
      assert.deepEqual(
        await reports.readActivityDays({
          since: iso(boundary),
          until: iso(boundary + day),
          memberId: null,
          operation: 'generate',
          dates: null,
        }),
        [],
      );
      assert.equal(
        summarizeOwnerUsage(rows).days[0].id,
        createdAt === null
          ? 'unknown'
          : new Date(createdAt + 8 * 3600000).toISOString().slice(0, 10),
      );
    });
  }
});

test('真实成员/owner 认证与热力图参数隔离，换 Key 不改变历史归属', async (t) => {
  const f = await fixture(t);
  await f.accept('m1');
  await f.accept('m2', { memberId: 'm2' });
  f.db
    .prepare('INSERT INTO discord_bindings(member_id,discord_id,guild_id) VALUES(?,?,?)')
    .run('m1', 'synthetic-discord', 'guild');
  const reports = new RelationalReportStore(f.db),
    at = NOW + day;
  const read = async (key, params = new URLSearchParams()) => {
    const member = await withAuthenticatedMemberApiKey(f.members, key, (member) => member, {});
    return member && readActivityHeatmap(reports, params, { memberId: member.id }, at);
  };
  assert.equal((await read(keys.m1)).days[0].total, 1);
  await assert.rejects(read(keys.m1, new URLSearchParams('memberId=m2')), /只能查询本人/);
  const ownerMember = await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    (member) => {
      assert.equal(isOwner(member, { OWNER_USER_ID: 'm1', DISCORD_GUILD_ID: 'guild' }), true);
      return member;
    },
    {},
  );
  assert.equal(ownerMember.id, 'm1');
  const owner = await readActivityHeatmap(
    reports,
    new URLSearchParams(),
    { allowMemberFilter: true },
    at,
  );
  assert.equal(owner.days[0].total, 2);
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'rotated-key'))();
  assert.equal(await read(keys.m1), null);
  assert.equal((await read('rotated-key')).days[0].total, 1);
});

test('全部操作与取消按原状态分类，已退休对象不再显示且 SQL 不读正文/BLOB', async (t) => {
  const f = await fixture(t);
  await f.accept('cancel', { samples: 2 });
  f.db.transaction(() =>
    f.generations.cancelWaitingGeneration(meta('cancel', NOW + 1, 'm1'), 'cancel'),
  )();
  for (const operation of ['upscale', 'encode-vibe']) {
    const n = normalizeMemberGeneration({
      operation,
      image: image.toString('base64'),
      ...(operation === 'encode-vibe' ? { model: 'nai-diffusion-4-5-full' } : {}),
    });
    await withAuthenticatedMemberApiKey(
      f.members,
      keys.m1,
      () =>
        f.generations.acceptGeneration(meta(operation, NOW, 'm1'), {
          inputId: `input-${operation}`,
          requestId: operation,
          memberId: 'm1',
          source: 'native',
          seedMode: 'not_applicable',
          confirmation: null,
          request: materializeMemberGeneration(n),
          context: { kind: 'native', native: n.normalizationContext.native },
          jobs: [
            {
              jobId: `${operation}-0`,
              status: 'queued',
              queueSeq: null,
              queueExpiresAt: null,
              deadlineAt: null,
              admissionEpoch: 0,
              cacheKey: null,
            },
          ],
        }),
      {},
    );
  }
  const queries = [],
    prepare = f.db.prepare.bind(f.db);
  f.db.prepare = (sql) => {
    queries.push(sql);
    return prepare(sql);
  };
  const reports = new RelationalReportStore(f.db),
    query = {
      since: iso(NOW - day),
      until: iso(NOW + day),
      memberId: 'm1',
      dates: null,
    },
    before = f.db.serialize();
  queries.length = 0;
  const cancelled = await reports.readActivityDays({ ...query, operation: 'generate' });
  assert.equal(cancelled[0].cancelled, 2);
  assert.equal(cancelled[0].failed, 0);
  for (const operation of ['upscale', 'encode-vibe']) {
    assert.equal((await reports.readActivityDays({ ...query, operation }))[0].queued, 1);
  }
  const rows = await reports.readOwnerJobRows('', iso(NOW + day));
  assert.equal(rows.length, 4);
  assert.ok(
    queries.every(
      (sql) => !/generation_inputs|normalization_contexts|input_assets|\.body\b/i.test(sql),
    ),
  );
  assert.deepEqual(f.db.serialize(), before);
  f.db.prepare = prepare;
  // 生命周期命令由另片交付；这里仅验证查询遵守已退休关系的现行列。
  f.db.prepare('UPDATE generation_jobs SET retired_at=? WHERE id=?').run(NOW, 'upscale-0');
  assert.deepEqual(await reports.readActivityDays({ ...query, operation: 'upscale' }), []);
  f.db.prepare('UPDATE generation_requests SET retired_at=? WHERE id=?').run(NOW, 'cancel');
  assert.deepEqual(await reports.readActivityDays({ ...query, operation: 'generate' }), []);
  assert.equal((await reports.readOwnerJobRows('', iso(NOW + day))).length, 1);
});

test('关系查询使用原创建时间局部索引；重开后结果相同、读取不回填投影', async (t) => {
  const f = await fixture(t);
  await f.accept('reopen');
  const sql = `SELECT job_id FROM generation_job_origins WHERE created_at>=? AND created_at<?`,
    range = [NOW - day, NOW + day];
  const plan = f.db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all(...range);
  assert.ok(
    plan.some((row) => /SEARCH domain_events USING INDEX idx_job_created_time/.test(row.detail)),
    JSON.stringify(plan),
  );
  const reports = new RelationalReportStore(f.db),
    query = {
      since: iso(range[0]),
      until: iso(range[1]),
      memberId: null,
      operation: 'generate',
      dates: null,
    },
    before = databaseState(f.db),
    expected = await reports.readActivityDays(query),
    expectedRows = await reports.readOwnerJobRows('', iso(range[1]));
  const prepare = f.db.prepare.bind(f.db),
    captured = [];
  f.db.prepare = (sql) => {
    captured.push(sql);
    return prepare(sql);
  };
  await reports.readActivityDays(query);
  new RelationalRequestStore(f.db).readMemberRequest('reopen', 'm1');
  f.db.prepare = prepare;
  const activitySql = captured.find((sql) => sql.includes('WITH selected AS')),
    requestSql = captured.find((sql) => sql.includes('origin.created_at AS createdAt'));
  const actualPlan = prepare(`EXPLAIN QUERY PLAN ${activitySql}`).all({
    start: range[0],
    end: range[1],
    memberId: null,
    operation: 'generate',
    dates: null,
  });
  assert.ok(
    actualPlan.some((row) =>
      /SEARCH domain_events USING INDEX idx_job_created_time/.test(row.detail),
    ),
    JSON.stringify(actualPlan),
  );
  t.diagnostic(`heatmap: ${JSON.stringify(actualPlan)}`);
  const requestPlan = prepare(`EXPLAIN QUERY PLAN ${requestSql}`).all('m1', 'reopen');
  assert.ok(
    requestPlan.some((row) => /SEARCH domain_events USING INDEX.*stream_id=/.test(row.detail)),
    JSON.stringify(requestPlan),
  );
  assert.ok(!requestPlan.some((row) => /SCAN domain_events/.test(row.detail)));
  t.diagnostic(`request: ${JSON.stringify(requestPlan)}`);
  const reopened = new Database(path.join(f.dir, 'library.sqlite'));
  try {
    const reader = new RelationalReportStore(reopened);
    assert.deepEqual(await reader.readActivityDays(query), expected);
    assert.deepEqual(await reader.readOwnerJobRows('', iso(range[1])), expectedRows);
    assert.equal(databaseState(reopened), before);
  } finally {
    reopened.close();
  }
  // 隔离内存库去掉新增索引，证明同一范围查询会退化扫描；不更改候选库。
  const withoutIndex = new Database(f.db.serialize());
  try {
    withoutIndex.exec('DROP INDEX idx_job_created_time');
    assert.ok(
      withoutIndex
        .prepare(`EXPLAIN QUERY PLAN ${sql}`)
        .all(...range)
        .some((row) => /SCAN domain_events/.test(row.detail)),
    );
  } finally {
    withoutIndex.close();
  }
});
