import test from 'node:test';
import assert from 'node:assert/strict';
import { fixture, meta, NOW, databaseState } from '../helpers/relational-generation.mjs';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { accountQuotaResult } from '../../server/quota/account-refresh.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
function observe(f, quotas, patch, at = NOW + 10) {
  const ticket = quotas.beginQuotaObservation('a', at),
    result = accountQuotaResult(
      'a',
      {
        tier: 3,
        subscriptionActive: true,
        subscriptionExpiresAt: null,
        fixed: '8000',
        purchased: '0',
        v5UsagePercent: 80,
        v5UsageIsNegative: false,
        v5UsageTimeUntilNextPercent: 0,
        ...patch,
      },
      new Date(at).toISOString(),
    );
  const commit = f.db.transaction(() =>
    quotas.recordQuotaObservation(meta('observe-' + at, at + 1), ticket, result),
  )();
  return {
    commit,
    payload: readRelationalCommit(f.db, commit.commitSeq).events.find(
      (e) => e.eventType === 'QuotaObserved',
    ).payload,
  };
}
test('原80负余额与原0普通余额金融同为0，但官方原标量必须可逆区分', async (t) => {
  for (const [percent, negative] of [
    [80, true],
    [0, false],
  ])
    await t.test(percent + '/' + negative, async (t) => {
      const f = await fixture(t, { seconds: 0 }),
        quotas = new RelationalQuotaStore(f.db),
        { payload } = observe(f, quotas, { v5UsagePercent: percent, v5UsageIsNegative: negative });
      assert.equal(payload.v5, '0');
      assert.equal(payload.v5UsagePercent, percent);
      assert.equal(payload.v5UsageIsNegative, negative);
      const before = databaseState(f.db);
      assert.deepEqual(quotas.readOfficialQuotaV5('a'), {
        v5UsagePercent: percent,
        v5UsageIsNegative: negative,
      });
      assert.equal(databaseState(f.db), before);
    });
});
import Database from 'better-sqlite3';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { accountQuotaErrorResult } from '../../server/quota/account-refresh.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageFormat,
  RELATIONAL_DATABASE_FORMAT,
  RELATIONAL_WRITER_VERSION,
} from '../../server/storage/relational-storage.ts';
import { parseRelationalFact } from '../../server/events/relational-fact-contracts.ts';
function failure(f, quotas, at = NOW + 20) {
  const ticket = quotas.beginQuotaObservation('a', at),
    result = accountQuotaErrorResult(
      'a',
      new Error('synthetic query failure'),
      new Date(at).toISOString(),
    );
  const commit = f.db.transaction(() =>
    quotas.recordQuotaObservation(meta('failure-' + at, at + 1), ticket, result),
  )();
  return {
    commit,
    payload: readRelationalCommit(f.db, commit.commitSeq).events.find(
      (e) => e.eventType === 'QuotaObserved',
    ).payload,
  };
}
function restore(t, source, before = null) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, 'generation-history');
  const owner = new RelationalCommitReplayer(db),
    inputs = new RelationalInputStore(source);
  for (const { seq } of source
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    if (before !== null && BigInt(seq) >= BigInt(before)) break;
    db.transaction(() => owner.replayCommit(readRelationalCommit(source, seq), inputs))();
  }
  return { db, owner, inputs, quotas: new RelationalQuotaStore(db) };
}
function merged(stored, next, db) {
  const result = structuredClone(stored);
  result.events.push(...structuredClone(next.events));
  result.command.commandId = 'combined-official-query';
  result.command.recordedAt = next.command.recordedAt;
  result.command.expectedVersions = {};
  const versions = new Map();
  result.events.forEach((e, i) => {
    if (!versions.has(e.streamId)) {
      const n =
        db
          .prepare('SELECT CAST(stream_version AS TEXT) AS n FROM stream_heads WHERE stream_id=?')
          .get(e.streamId)?.n ?? '0';
      versions.set(e.streamId, BigInt(n));
      result.command.expectedVersions[e.streamId] = n;
    }
    versions.set(e.streamId, versions.get(e.streamId) + 1n);
    Object.assign(e, {
      eventIndex: i,
      commitSeq: result.events[0].commitSeq,
      streamVersion: String(versions.get(e.streamId)),
    });
  });
  result.eventCount = result.events.length;
  result.command.events = result.events.map(
    ({
      eventType,
      payload,
      streamId,
      occurredAt,
      accountId,
      memberId,
      policyVersion,
      pricingVersion,
    }) => ({
      eventType,
      payload,
      streamId,
      occurredAt,
      accountId,
      memberId,
      policyVersion,
      pricingVersion,
    }),
  );
  return result;
}
test('官方number保留finite原精度，missing/null/0不由金融值填补', async (t) => {
  for (const [percent, expected] of [
    [undefined, null],
    [null, null],
    [0, 0],
    [0.1234567890123456, 0.1234567890123456],
    [1e-15, 1e-15],
    [Number.MIN_VALUE, Number.MIN_VALUE],
    [Number.MAX_VALUE, Number.MAX_VALUE],
  ])
    await t.test(String(percent), async (t) => {
      const f = await fixture(t, { seconds: 0 }),
        q = new RelationalQuotaStore(f.db),
        { payload } = observe(f, q, { v5UsagePercent: percent, v5UsageIsNegative: false });
      assert.equal(payload.v5UsagePercent, expected);
      assert.equal(payload.v5UsageIsNegative, false);
      const before = databaseState(f.db);
      assert.deepEqual(q.readOfficialQuotaV5('a'), {
        v5UsagePercent: expected,
        v5UsageIsNegative: false,
      });
      assert.equal(databaseState(f.db), before);
      const r = restore(t, f.db);
      assert.deepEqual(r.quotas.readOfficialQuotaV5('a'), q.readOfficialQuotaV5('a'));
      assert.deepEqual(r.quotas.readAccountQuota('a'), q.readAccountQuota('a'));
    });
});
test('未知Opening只有金融80时仍返回原值NULL；失败也不能补false或伪称新成功', async (t) => {
  const f = await fixture(t, { seconds: 0 }),
    q = new RelationalQuotaStore(f.db);
  assert.deepEqual(q.readOfficialQuotaV5('a'), { v5UsagePercent: null, v5UsageIsNegative: null });
  assert.deepEqual(q.readOfficialQuotaV5('missing'), {
    v5UsagePercent: null,
    v5UsageIsNegative: null,
  });
  const { payload } = failure(f, q);
  assert.notEqual(payload.errorCode, null);
  assert.equal(payload.v5, '80');
  assert.equal(payload.observedAt, NOW);
  assert.equal(payload.v5UsagePercent, null);
  assert.equal(payload.v5UsageIsNegative, null);
  const r = restore(t, f.db);
  assert.deepEqual(r.quotas.readOfficialQuotaV5('a'), q.readOfficialQuotaV5('a'));
});
test('失败保留此前原值与时点，完整同笔后来0false不能取代80true来源', async (t) => {
  const f = await fixture(t, { seconds: 0 }),
    q = new RelationalQuotaStore(f.db);
  observe(f, q, { v5UsagePercent: 80, v5UsageIsNegative: true });
  const failed = failure(f, q);
  assert.equal(failed.payload.errorCode, 'sync_error');
  assert.equal(failed.payload.observedAt, NOW + 10);
  assert.equal(failed.payload.v5UsagePercent, 80);
  assert.equal(failed.payload.v5UsageIsNegative, true);
  const later = observe(f, q, { v5UsagePercent: 0, v5UsageIsNegative: false }, NOW + 30),
    cut = restore(t, f.db, failed.commit.commitSeq),
    original = merged(
      readRelationalCommit(f.db, failed.commit.commitSeq),
      readRelationalCommit(f.db, later.commit.commitSeq),
      cut.db,
    );
  cut.db.transaction(() => cut.owner.replayCommit(original, cut.inputs))();
  assert.deepEqual(cut.quotas.readOfficialQuotaV5('a'), {
    v5UsagePercent: 0,
    v5UsageIsNegative: false,
  });
  assert.deepEqual(cut.quotas.readAccountQuota('a'), q.readAccountQuota('a'));
  const once = databaseState(cut.db);
  cut.db.transaction(() =>
    assert.equal(cut.owner.replayCommit(original, cut.inputs).replayed, true),
  )();
  assert.equal(databaseState(cut.db), once);
  const badCut = restore(t, f.db, failed.commit.commitSeq),
    bad = merged(
      readRelationalCommit(f.db, failed.commit.commitSeq),
      readRelationalCommit(f.db, later.commit.commitSeq),
      badCut.db,
    ),
    observed = bad.events.find((e) => e.eventType === 'QuotaObserved');
  Object.assign(observed.payload, { v5UsagePercent: 0, v5UsageIsNegative: false });
  bad.command.events = bad.events.map(
    ({
      eventType,
      payload,
      streamId,
      occurredAt,
      accountId,
      memberId,
      policyVersion,
      pricingVersion,
    }) => ({
      eventType,
      payload,
      streamId,
      occurredAt,
      accountId,
      memberId,
      policyVersion,
      pricingVersion,
    }),
  );
  const before = databaseState(badCut.db);
  badCut.db.transaction(() =>
    assert.throws(() => badCut.owner.replayCommit(bad, badCut.inputs), /失败观察必须保留/),
  )();
  assert.equal(databaseState(badCut.db), before);
});
test('原值与金融v5不符的合法完整提交原子拒绝；事实必填且原number不是金额字符串', async (t) => {
  const f = await fixture(t, { seconds: 0 }),
    q = new RelationalQuotaStore(f.db),
    result = observe(f, q, { v5UsagePercent: 80, v5UsageIsNegative: true }),
    stored = readRelationalCommit(f.db, result.commit.commitSeq),
    target = restore(t, f.db, result.commit.commitSeq),
    bad = structuredClone(stored),
    fact = bad.events.find((e) => e.eventType === 'QuotaObserved');
  fact.payload.v5 = '80';
  bad.command.events = bad.events.map(
    ({
      eventType,
      payload,
      streamId,
      occurredAt,
      accountId,
      memberId,
      policyVersion,
      pricingVersion,
    }) => ({
      eventType,
      payload,
      streamId,
      occurredAt,
      accountId,
      memberId,
      policyVersion,
      pricingVersion,
    }),
  );
  const before = databaseState(target.db);
  target.db.transaction(() =>
    assert.throws(() => target.owner.replayCommit(bad, target.inputs), /官方原值与原金融/),
  )();
  assert.equal(databaseState(target.db), before);
  for (const key of ['v5UsagePercent', 'v5UsageIsNegative']) {
    const payload = { ...result.payload };
    delete payload[key];
    assert.throws(
      () => parseRelationalFact({ eventType: 'QuotaObserved', payload }),
      /Invalid relational fact/,
    );
  }
  for (const value of [NaN, Infinity])
    assert.throws(
      () =>
        parseRelationalFact({
          eventType: 'QuotaObserved',
          payload: { ...result.payload, v5UsagePercent: value },
        }),
      /finite JSON values/,
    );
  assert.throws(
    () =>
      parseRelationalFact({
        eventType: 'QuotaObserved',
        payload: { ...result.payload, v5UsagePercent: '80' },
      }),
    /Invalid relational fact/,
  );
  assert.throws(
    () =>
      parseRelationalFact({
        eventType: 'QuotaObserved',
        payload: { ...result.payload, v5UsageIsNegative: 0 },
      }),
    /Invalid relational fact/,
  );
});
test('真实旧c73 v4完整DDL及正确旧hash零写拒绝，不改写旧SQL或输入context版本', (t) => {
  const prior = JSON.parse(
    readFileSync(
      new URL('../fixtures/relational-schema-v4-official-v5.json', import.meta.url),
      'utf8',
    ),
  );
  assert.equal(prior.sourceCommit, 'c73a6e738d70b7c12da93164fb157fc479478018');
  assert.equal(createHash('sha256').update(prior.sql).digest('hex'), prior.sha256);
  const db = new Database(':memory:');
  t.after(() => db.close());
  db.exec(prior.sql);
  assert.equal(db.pragma('integrity_check', { simple: true }), 'ok');
  const insert = db.prepare('INSERT INTO app_meta(key,value) VALUES(?,?)');
  insert.run('database_format', RELATIONAL_DATABASE_FORMAT);
  insert.run('schema_sha256', prior.sha256);
  insert.run(
    'event_authority',
    JSON.stringify({
      version: RELATIONAL_WRITER_VERSION,
      minimumWriterVersion: RELATIONAL_WRITER_VERSION,
    }),
  );
  db.prepare("INSERT INTO projection_state VALUES(1,'old-c73',1,0,'building')").run();
  const before = db.serialize();
  assert.throws(() => assertRelationalStorageFormat(db), /Unsupported relational database format/);
  assert.deepEqual(db.serialize(), before);
});
test('官方观察后段真实SQL故障外层捕获仍恢复旧两原值/金融/事实/回执', async (t) => {
  const f = await fixture(t, { seconds: 0 }),
    q = new RelationalQuotaStore(f.db);
  observe(f, q, { v5UsagePercent: 80, v5UsageIsNegative: true });
  const before = databaseState(f.db);
  let writes = 0;
  f.db.function('official_write', () => {
    writes++;
    return 0;
  });
  f.db.exec(
    "CREATE TEMP TRIGGER fail_official AFTER UPDATE ON account_quota_state BEGIN SELECT official_write();SELECT RAISE(ABORT,'synthetic official fault');END",
  );
  const ticket = q.beginQuotaObservation('a', NOW + 30),
    result = accountQuotaResult(
      'a',
      {
        tier: 3,
        subscriptionActive: true,
        subscriptionExpiresAt: null,
        fixed: '8000',
        purchased: '0',
        v5UsagePercent: 0,
        v5UsageIsNegative: false,
        v5UsageTimeUntilNextPercent: 0,
      },
      new Date(NOW + 30).toISOString(),
    );
  f.db.transaction(() =>
    assert.throws(
      () => q.recordQuotaObservation(meta('official-sql-fault', NOW + 31), ticket, result),
      /synthetic official fault/,
    ),
  )();
  assert.equal(writes, 1);
  assert.equal(databaseState(f.db), before);
  assert.deepEqual(q.readOfficialQuotaV5('a'), { v5UsagePercent: 80, v5UsageIsNegative: true });
});

test('Opening已知两个原值与金融值各自保留，无原证据NULL不被推断；非法关联全提交回滚', async (t) => {
  const f = await fixture(t, { seconds: 0 }),
    seq = f.db
      .prepare(
        "SELECT CAST(commit_seq AS TEXT) AS seq FROM domain_events WHERE event_type='OpeningQuotaRecorded'",
      )
      .get().seq;
  const original = readRelationalCommit(f.db, seq);
  const known = structuredClone(original),
    quota = known.events.find((e) => e.eventType === 'OpeningQuotaRecorded');
  Object.assign(quota.payload, { v5UsagePercent: 80, v5UsageIsNegative: true, observedV5: '0' });
  const target = restore(t, f.db, seq),
    fixed = merged({ ...known, events: [] }, known, target.db);
  target.db.transaction(() => target.owner.replayCommit(fixed, target.inputs))();
  const before = databaseState(target.db);
  target.db.pragma('query_only=ON');
  assert.deepEqual(target.quotas.readOfficialQuotaV5('a'), {
    v5UsagePercent: 80,
    v5UsageIsNegative: true,
  });
  target.db.pragma('query_only=OFF');
  assert.equal(databaseState(target.db), before);
  assert.equal(target.quotas.readAccountQuota('a').observedV5, '0');
  const badTarget = restore(t, f.db, seq),
    bad = structuredClone(fixed);
  bad.events.find((e) => e.eventType === 'OpeningQuotaRecorded').payload.observedV5 = '80';
  bad.command.events = bad.events.map(
    ({
      eventType,
      payload,
      streamId,
      occurredAt,
      accountId,
      memberId,
      policyVersion,
      pricingVersion,
    }) => ({
      eventType,
      payload,
      streamId,
      occurredAt,
      accountId,
      memberId,
      policyVersion,
      pricingVersion,
    }),
  );
  const empty = databaseState(badTarget.db);
  badTarget.db.transaction(() =>
    assert.throws(
      () => badTarget.owner.replayCommit(bad, badTarget.inputs),
      /开账官方原值与原金融/,
    ),
  )();
  assert.equal(databaseState(badTarget.db), empty);
});
test('缺失negative与缺失percent独立保持NULL，数值负percent失败仍保留原金融值', async (t) => {
  const unknown = await fixture(t, { seconds: 0 }),
    uq = new RelationalQuotaStore(unknown.db),
    first = observe(unknown, uq, { v5UsagePercent: 80, v5UsageIsNegative: undefined });
  assert.equal(first.payload.v5, '80');
  assert.deepEqual(uq.readOfficialQuotaV5('a'), { v5UsagePercent: 80, v5UsageIsNegative: null });
  const partial = await fixture(t, { seconds: 0 }),
    pq = new RelationalQuotaStore(partial.db),
    second = observe(partial, pq, { v5UsagePercent: undefined, v5UsageIsNegative: true });
  assert.equal(second.payload.v5, null);
  assert.deepEqual(pq.readOfficialQuotaV5('a'), { v5UsagePercent: null, v5UsageIsNegative: true });
  const negative = await fixture(t, { seconds: 0 }),
    nq = new RelationalQuotaStore(negative.db),
    third = observe(negative, nq, { v5UsagePercent: -5, v5UsageIsNegative: false });
  assert.equal(third.payload.v5, '-5');
  const retained = failure(negative, nq);
  assert.equal(retained.payload.v5, third.payload.v5);
  assert.deepEqual(nq.readOfficialQuotaV5('a'), { v5UsagePercent: -5, v5UsageIsNegative: false });
  const restored = restore(t, negative.db);
  assert.deepEqual(restored.quotas.readAccountQuota('a'), nq.readAccountQuota('a'));
  assert.deepEqual(restored.quotas.readOfficialQuotaV5('a'), nq.readOfficialQuotaV5('a'));
});
test('同毫秒不同原事实按stream代次保留后来官方值，不以时点相等误领旧来源', async (t) => {
  const f = await fixture(t, { seconds: 0 }),
    q = new RelationalQuotaStore(f.db),
    first = observe(f, q, { v5UsagePercent: 80, v5UsageIsNegative: true }),
    ticket = q.beginQuotaObservation('a', NOW + 10),
    result = accountQuotaResult(
      'a',
      {
        tier: 3,
        subscriptionActive: true,
        subscriptionExpiresAt: null,
        fixed: '8000',
        purchased: '0',
        v5UsagePercent: 0,
        v5UsageIsNegative: false,
        v5UsageTimeUntilNextPercent: 0,
      },
      new Date(NOW + 10).toISOString(),
    );
  const commit = f.db.transaction(() =>
      q.recordQuotaObservation(meta('same-time-second', NOW + 11), ticket, result),
    )(),
    second = readRelationalCommit(f.db, commit.commitSeq).events.find(
      (e) => e.eventType === 'QuotaObserved',
    );
  assert.equal(second.payload.observedAt, first.payload.observedAt);
  assert.notEqual(
    second.eventId,
    readRelationalCommit(f.db, first.commit.commitSeq).events.find(
      (e) => e.eventType === 'QuotaObserved',
    ).eventId,
  );
  assert.deepEqual(q.readOfficialQuotaV5('a'), { v5UsagePercent: 0, v5UsageIsNegative: false });
  const r = restore(t, f.db);
  assert.deepEqual(r.quotas.readOfficialQuotaV5('a'), q.readOfficialQuotaV5('a'));
});
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { JsonStore } from '../../server/storage/sqlite-store.ts';
import { createAccountQuotaRefresh } from '../../server/quota/account-refresh.ts';
import { configureFairQuota, syncFairQuota } from '../../server/quota/fair-share.ts';
test('actual原JsonStore成功后网络失败保留-5false，新命令不能由金融符号改写原观察', async (t) => {
  const dir = await mkdtemp(path.join(tmpdir(), 'nai-official-original-owner-')),
    legacy = new JsonStore(dir);
  await legacy.init();
  t.after(async () => {
    await legacy.close();
    await rm(dir, { recursive: true, force: true });
  });
  const db = structuredClone(legacy.db);
  db.users = ['m1', 'm2', 'm3', 'm4'].map((id) => ({
    id,
    token: 'synthetic-' + id,
    enabled: true,
  }));
  db.accounts = [
    {
      id: 'a',
      token: 'synthetic-pst',
      enabled: true,
      quotaTier: 3,
      subscriptionActive: true,
      subscriptionExpiresAt: NOW + 86400000,
      quotaFixed: 8000,
      quotaPurchased: 0,
      v5UsagePercent: 80,
      v5UsageIsNegative: false,
      v5UsageTimeUntilNextPercent: 0,
      quotaCheckedAt: new Date(NOW).toISOString(),
    },
  ];
  configureFairQuota(
    db,
    db.users.map((u) => u.id),
  );
  syncFairQuota(db, NOW);
  await legacy.write(db);
  await legacy.enableEventAuthority();
  let time = NOW + 10,
    calls = 0;
  const refresh = createAccountQuotaRefresh({
    store: legacy,
    now: () => time,
    logger: { log() {}, error() {} },
    publicAccount: (a) => ({
      v5UsagePercent: a.v5UsagePercent,
      v5UsageIsNegative: a.v5UsageIsNegative,
      quotaCheckedAt: a.quotaCheckedAt,
    }),
    publicErrorMessage: () => '',
    fetchQuota: async () => {
      if (calls++ === 0) {
        return {
          tier: 3,
          subscriptionActive: true,
          subscriptionExpiresAt: NOW + 86400000,
          fixed: 8000,
          purchased: 0,
          v5UsagePercent: -5,
          v5UsageIsNegative: false,
          v5UsageTimeUntilNextPercent: 0,
        };
      }
      throw Object.assign(new Error('synthetic network error'), { code: 'ECONNRESET' });
    },
  });
  const success = await refresh.refresh(['a']);
  time = NOW + 20;
  const failed = await refresh.refresh(['a']);
  assert.equal(success.ok, 1);
  assert.equal(failed.failed, 1);
  assert.deepEqual(failed.accounts, success.accounts);
  assert.deepEqual(success.accounts, [
    {
      v5UsagePercent: -5,
      v5UsageIsNegative: false,
      quotaCheckedAt: new Date(NOW + 10).toISOString(),
    },
  ]);
  const events = legacy.sqlite
    .prepare(
      "SELECT payload FROM domain_events WHERE event_type='QuotaObserved' AND json_extract(payload,'$.kind')='official' ORDER BY commit_seq,event_index",
    )
    .all()
    .map((row) => JSON.parse(row.payload));
  assert.equal(events.at(-1).observation.v5UsagePercent, -5);
  assert.equal(events.at(-1).observation.v5UsageIsNegative, false);
  assert.ok(events.at(-1).observation.quotaError);
  const f = await fixture(t, { seconds: 0 }),
    quotas = new RelationalQuotaStore(f.db),
    first = observe(f, quotas, { v5UsagePercent: -5, v5UsageIsNegative: false }),
    second = failure(f, quotas);
  assert.equal(first.payload.v5, '-5');
  assert.deepEqual(
    {
      v5UsagePercent: second.payload.v5UsagePercent,
      v5UsageIsNegative: second.payload.v5UsageIsNegative,
    },
    failed.accounts.map(({ quotaCheckedAt, ...v5 }) => v5)[0],
  );
  assert.equal(second.payload.observedAt, Date.parse(failed.accounts[0].quotaCheckedAt));
  assert.equal(
    second.payload.v5,
    first.payload.v5,
    'mere network failure cannot reinterpret the unchanged original observation',
  );
  const rebuilt = restore(t, f.db);
  assert.deepEqual(rebuilt.quotas.readAccountQuota('a'), quotas.readAccountQuota('a'));
  assert.deepEqual(rebuilt.quotas.readOfficialQuotaV5('a'), quotas.readOfficialQuotaV5('a'));
  assert.equal(rebuilt.db.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
  assert.equal(rebuilt.db.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
  const beforeFault = databaseState(f.db);
  let writes = 0;
  f.db.function('negative_source_write', () => {
    writes++;
    return 0;
  });
  f.db.exec(
    "CREATE TEMP TRIGGER fail_negative_source AFTER UPDATE ON account_quota_state BEGIN SELECT negative_source_write();SELECT RAISE(ABORT,'synthetic negative source fault');END",
  );
  f.db.transaction(() =>
    assert.throws(() => failure(f, quotas, NOW + 30), /synthetic negative source fault/),
  )();
  assert.equal(writes, 1);
  assert.equal(databaseState(f.db), beforeFault);
  assert.equal(quotas.readAccountQuota('a').observedV5, first.payload.v5);
  assert.deepEqual(quotas.readOfficialQuotaV5('a'), {
    v5UsagePercent: -5,
    v5UsageIsNegative: false,
  });
  f.db.exec('DROP TRIGGER fail_negative_source');
  assert.equal(failure(f, quotas, NOW + 30).payload.v5, first.payload.v5);
});
