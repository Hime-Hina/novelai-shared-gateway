import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import path from 'node:path';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { createHash } from 'node:crypto';
import { fixture, NOW, meta, keys, databaseState } from '../helpers/relational-generation.mjs';
import { fixtureNormalizationDefaults } from '../helpers/relational-configuration.mjs';
import { RelationalConfigurationStore } from '../../server/storage/relational-configuration-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import {
  readRelationalCommit,
  appendRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import { parseRelationalNormalizationContext } from '../../server/generation/relational-input.ts';
import { prepareNormalizationDefaultsProfile } from '../../server/generation/normalization-defaults.ts';
import { normalizeNovelAiRequest } from '../../server/protocols/novelai-client.ts';
import { memberPublicBaseUrl } from '../../server/members/http-routes.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import {
  generationRequestFingerprint,
  requestNormalizationDefaults,
} from '../../server/generation/request-fingerprint.ts';
import { nativeNormalizationContext } from '../../server/protocols/native-image.ts';

const metadata = (id = 'initialize') => ({ ...meta(id), actorKind: 'system', actorId: null });
const input = (changes = {}) => ({
  normalizationDefaults: fixtureNormalizationDefaults,
  publicBaseUrl: 'https://saved.example/prefix/',
  originRef: 'explicit-bootstrap:synthetic-v1',
  ...changes,
});
const unavailable = { code: 'configuration_unavailable' };
function empty(t, history = 'configuration-history') {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, history);
  return db;
}
function initialize(db, values = input(), id = 'initialize') {
  return db.transaction(() =>
    new RelationalConfigurationStore(db).initializeGatewayConfiguration(metadata(id), values),
  )();
}
async function snapshot(db, directory, name = 'selected') {
  const sourcePath = path.join(directory, `${name}.sqlite`);
  await db.backup(sourcePath);
  const sealed = new Database(sourcePath);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  return {
    sourcePath,
    sourceSha256: createHash('sha256')
      .update(await readFile(sourcePath))
      .digest('hex'),
    historyId: db.prepare('SELECT history_id AS id FROM projection_state').get().id,
    finalCommitSeq: db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get()
      .seq,
    replicas: { dataDir: directory },
  };
}
function replay(db, source) {
  const owner = new RelationalCommitReplayer(db),
    protectedSource = new RelationalInputStore(source);
  for (const { seq } of source
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all())
    db.transaction(() => owner.replayCommit(readRelationalCommit(source, seq), protectedSource))();
}

test('明确系统初始化唯一current来源，原11字段/地址语义保留，幂等无覆盖且事件不含保护正文', (t) => {
  const db = empty(t),
    owner = new RelationalConfigurationStore(db),
    before = databaseState(db);
  assert.throws(() => owner.readCurrent(), unavailable);
  assert.throws(
    () => owner.initializeGatewayConfiguration(metadata(), input()),
    /owning system transaction/,
  );
  db.transaction(() =>
    assert.throws(
      () =>
        owner.initializeGatewayConfiguration(
          { ...metadata(), actorKind: 'member', actorId: 'm' },
          input(),
        ),
      { statusCode: 403 },
    ),
  )();
  assert.equal(databaseState(db), before);
  const values = {
    defaultModel: null,
    defaultArtist: false,
    defaultNegative: '',
    defaults: {
      size: '',
      width: 0,
      height: false,
      steps: '',
      scale: null,
      cfg: 0,
      sampler: null,
      noiseSchedule: false,
    },
  };
  const saved = initialize(db, input({ normalizationDefaults: values })),
    current = owner.readCurrent();
  assert.deepEqual(current.normalizationDefaults, values);
  assert.deepEqual(
    normalizeNovelAiRequest({ tag: 'q', seed: 42 }, current.normalizationDefaults),
    normalizeNovelAiRequest({ tag: 'q', seed: 42 }, values),
  );
  assert.equal(
    memberPublicBaseUrl({ settings: { publicBaseUrl: current.publicBaseUrl } }, {}),
    'https://saved.example/prefix/',
  );
  assert.equal(
    memberPublicBaseUrl(
      { settings: { publicBaseUrl: current.publicBaseUrl } },
      { PUBLIC_BASE_URL: 'https://deployment.example/' },
    ),
    'https://deployment.example/',
  );
  current.normalizationDefaults.defaults.steps = 99;
  assert.equal(owner.readCurrent().normalizationDefaults.defaults.steps, '');
  const once = db.serialize();
  assert.equal(initialize(db, input({ normalizationDefaults: values })).replayed, true);
  assert.deepEqual(db.serialize(), once);
  assert.throws(() => initialize(db, input(), 'other-command'), { statusCode: 409 });
  assert.throws(() => initialize(db, input()), { code: 'idempotency_conflict' });
  assert.deepEqual(db.serialize(), once);
  const stored = readRelationalCommit(db, saved.commitSeq);
  assert.deepEqual(
    stored.events.map((e) => e.eventType),
    ['GatewayConfigurationInitialized'],
  );
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM normalization_contexts').get().n, 1);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM generation_requests').get().n, 0);
  assert.ok(!JSON.stringify(stored).includes('normalization-defaults'));
});

test('profile闭合表示拒绝秘密、未知字段和非finite对象，不能充当缺Native的生成context', (t) => {
  const db = empty(t),
    before = databaseState(db);
  for (const values of [
    { defaults: {} },
    { ...fixtureNormalizationDefaults, promptApi: { apiKey: 'private-marker' } },
    { ...fixtureNormalizationDefaults, defaultArtist: {} },
    {
      ...fixtureNormalizationDefaults,
      defaults: { ...fixtureNormalizationDefaults.defaults, scale: Infinity },
    },
    {
      ...fixtureNormalizationDefaults,
      defaults: { ...fixtureNormalizationDefaults.defaults, extra: 1 },
    },
  ]) {
    db.transaction(() =>
      assert.throws(() =>
        new RelationalConfigurationStore(db).initializeGatewayConfiguration(
          metadata(),
          input({ normalizationDefaults: values }),
        ),
      ),
    )();
    assert.equal(databaseState(db), before);
  }
  const result = initialize(db),
    stored = readRelationalCommit(db, result.commitSeq),
    profile = prepareNormalizationDefaultsProfile(fixtureNormalizationDefaults).profile;
  assert.throws(() => parseRelationalNormalizationContext(profile), { code: 'input_unavailable' });
  const eventText = JSON.stringify(stored);
  for (const secret of [
    'synthetic configured artist',
    'synthetic configured negative',
    'private-marker',
  ])
    assert.ok(!eventText.includes(secret));
  const copy = empty(t),
    reader = new RelationalCommitReplayer(copy);
  copy.transaction(() => reader.replayCommit(stored, new RelationalInputStore(db)))();
  const attack = db.transaction(() =>
    appendRelationalCommit(db, {
      ...metadata('orphan-context'),
      fingerprint: 'a'.repeat(64),
      expectedVersions: { [`context:${stored.events[0].payload.contextId}`]: '0' },
      receipt: null,
      events: [
        {
          eventType: 'ContextRegistered',
          payload: {
            contextId: stored.events[0].payload.contextId,
            contextSchemaVersion: 1,
            contextSha256: stored.events[0].payload.contextSha256,
          },
          streamId: `context:${stored.events[0].payload.contextId}`,
          occurredAt: NOW,
          memberId: null,
          accountId: null,
          policyVersion: 'test',
          pricingVersion: 'test',
        },
      ],
    }),
  )();
  const unchanged = databaseState(copy);
  copy.transaction(() =>
    assert.throws(
      () =>
        reader.replayCommit(
          readRelationalCommit(db, attack.commitSeq),
          new RelationalInputStore(db),
        ),
      { code: 'input_unavailable' },
    ),
  )();
  assert.equal(databaseState(copy), unchanged);
});

test('profile与current写入同一保存点，外层捕获SQL失败无半个内容/事实/回执且可原命令重试', (t) => {
  const db = empty(t),
    before = databaseState(db),
    prepare = db.prepare.bind(db);
  db.prepare = (sql) => {
    const statement = prepare(sql);
    if (sql.startsWith('INSERT INTO gateway_configuration'))
      statement.run = () => {
        throw new Error('injected current SQL failure');
      };
    return statement;
  };
  db.transaction(() =>
    assert.throws(
      () =>
        new RelationalConfigurationStore(db).initializeGatewayConfiguration(metadata(), input()),
      /injected current SQL failure/,
    ),
  )();
  db.prepare = prepare;
  assert.equal(databaseState(db), before);
  initialize(db);
  assert.deepEqual(
    new RelationalConfigurationStore(db).readCurrent().normalizationDefaults,
    fixtureNormalizationDefaults,
  );
});

test('无请求历史也联合回放真实profile；错保护源/漏源/一致篡改拒绝，原完整提交可重试', (t) => {
  const a = empty(t),
    b = empty(t),
    target = empty(t);
  initialize(a);
  initialize(
    b,
    input({
      normalizationDefaults: {
        ...fixtureNormalizationDefaults,
        defaultArtist: 'different explicit configuration',
      },
    }),
  );
  const stored = readRelationalCommit(b, '1'),
    owner = new RelationalCommitReplayer(target),
    before = databaseState(target);
  target.transaction(() =>
    assert.throws(() => owner.replayCommit(stored), /Protected content replay requires/),
  )();
  assert.equal(databaseState(target), before);
  target.transaction(() =>
    assert.throws(() => owner.replayCommit(stored, new RelationalInputStore(a)), unavailable),
  )();
  assert.equal(databaseState(target), before);
  const duplicate = structuredClone(stored);
  duplicate.events.push({
    ...duplicate.events[0],
    eventId: 'duplicate-configuration',
    eventIndex: 1,
    streamVersion: '2',
  });
  duplicate.command.events.push(structuredClone(duplicate.command.events[0]));
  duplicate.eventCount = 2;
  target.transaction(() =>
    assert.throws(() => owner.replayCommit(duplicate, new RelationalInputStore(b))),
  )();
  assert.equal(databaseState(target), before);
  target.transaction(() => owner.replayCommit(stored, new RelationalInputStore(b)))();
  assert.deepEqual(
    new RelationalConfigurationStore(target).readCurrent(),
    new RelationalConfigurationStore(b).readCurrent(),
  );
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM generation_requests').get().n, 0);
  const once = target.serialize();
  assert.equal(
    target.transaction(() => owner.replayCommit(stored, new RelationalInputStore(b)))().replayed,
    true,
  );
  assert.deepEqual(target.serialize(), once);
});

test('重复回放仍须源和目标真实profile，缺失/损坏拒绝而不自愈', (t) => {
  const source = empty(t),
    target = empty(t),
    saved = initialize(source),
    stored = readRelationalCommit(source, saved.commitSeq);
  replay(target, source);
  const owner = new RelationalCommitReplayer(target),
    contextId = stored.events[0].payload.contextId;
  target.pragma('foreign_keys=OFF');
  target.prepare('DELETE FROM normalization_contexts WHERE id=?').run(contextId);
  target.pragma('foreign_keys=ON');
  assert.ok(target.pragma('foreign_key_check').length);
  const missing = databaseState(target);
  target.transaction(() =>
    assert.throws(() => owner.replayCommit(stored, new RelationalInputStore(source)), unavailable),
  )();
  assert.equal(databaseState(target), missing);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM normalization_contexts').get().n, 0);
  const complete = empty(t);
  replay(complete, source);
  source.pragma('foreign_keys=OFF');
  source.prepare('DELETE FROM normalization_contexts WHERE id=?').run(contextId);
  source.pragma('foreign_keys=ON');
  const before = databaseState(complete);
  complete.transaction(() =>
    assert.throws(
      () =>
        new RelationalCommitReplayer(complete).replayCommit(
          stored,
          new RelationalInputStore(source),
        ),
      unavailable,
    ),
  )();
  assert.equal(databaseState(complete), before);
});

test('保护profile保持原SHA关系，源或目标正文篡改均拒绝且重复回放不覆盖坏内容', (t) => {
  for (const badSide of ['source', 'target']) {
    const source = empty(t),
      target = empty(t),
      saved = initialize(source),
      stored = readRelationalCommit(source, saved.commitSeq);
    replay(target, source);
    const changed = badSide === 'source' ? source : target,
      contextId = stored.events[0].payload.contextId,
      original = changed
        .prepare('SELECT body FROM normalization_contexts WHERE id=?')
        .get(contextId).body,
      body = JSON.parse(original);
    body.values.defaultArtist = 'different-body-with-original-hash';
    const trigger = changed
      .prepare("SELECT sql FROM sqlite_schema WHERE name='normalization_contexts_identity'")
      .get().sql;
    changed.exec('DROP TRIGGER normalization_contexts_identity');
    changed
      .prepare('UPDATE normalization_contexts SET body=? WHERE id=?')
      .run(JSON.stringify(body), contextId);
    changed.exec(trigger);
    const before = databaseState(target);
    target.transaction(() =>
      assert.throws(
        () =>
          new RelationalCommitReplayer(target).replayCommit(
            stored,
            new RelationalInputStore(source),
          ),
        unavailable,
      ),
    )();
    assert.equal(databaseState(target), before);
    assert.equal(
      changed.prepare('SELECT body FROM normalization_contexts WHERE id=?').get(contextId).body,
      JSON.stringify(body),
    );
  }
});

test('ready拒绝两边都无current，真实owner初始化后无请求profile可经完整shadow/保护恢复合法安装', async (t) => {
  const f = await fixture(t),
    directory = await mkdtemp(path.join(tmpdir(), 'nai-configuration-ready-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const missing = await snapshot(f.db, directory, 'missing'),
    target = empty(t, 'generation-history');
  replay(target, f.db);
  const before = databaseState(target);
  await assert.rejects(verifyAndInstallRelationalStorage(target, missing), unavailable);
  assert.equal(databaseState(target), before);
  const source = empty(t),
    configured = empty(t);
  initialize(source);
  replay(configured, source);
  const selected = await snapshot(source, directory, 'configured');
  await verifyAndInstallRelationalStorage(configured, selected);
  assertRelationalStorageReady(configured);
  assert.deepEqual(
    new RelationalConfigurationStore(configured).readCurrent(),
    new RelationalConfigurationStore(source).readCurrent(),
  );
  assert.equal(configured.prepare('SELECT COUNT(*) AS n FROM generation_requests').get().n, 0);
  assert.equal(configured.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
});

test('历史首次A不能充当current；明确初始化B后新意图用B，原键/执行与联合恢复仍只用首次A', async (t) => {
  const f = await fixture(t),
    owner = new RelationalConfigurationStore(f.db),
    a = {
      ...fixtureNormalizationDefaults,
      defaultArtist: 'original-profile-A',
      defaults: {
        ...fixtureNormalizationDefaults.defaults,
        size: 'custom',
        width: 128,
        height: 128,
        steps: 30,
      },
    },
    b = { ...a, defaultArtist: 'current-profile-B' },
    raw = { tag: 'same omitted-default intent', seed: 42 },
    request = { ...normalizeNovelAiRequest(raw, a), operation: 'generate', sampleCount: 1 },
    context = {
      kind: 'direct',
      native: nativeNormalizationContext(),
      defaults: requestNormalizationDefaults(a),
    },
    acceptedMetadata = {
      ...meta('accepted-with-A', NOW, 'm1'),
      fingerprint: generationRequestFingerprint(request, { noCache: false }),
    },
    keyHash = 'a'.repeat(64);
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    () =>
      f.generations.acceptGeneration(acceptedMetadata, {
        inputId: 'historical-input',
        requestId: 'historical-request',
        memberId: 'm1',
        source: 'direct',
        seedMode: 'fixed',
        confirmation: null,
        request,
        context,
        idempotency: { keyHash, noCache: false },
        jobs: [
          {
            jobId: 'historical-job',
            status: 'pending',
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
  assert.throws(() => owner.readCurrent(), unavailable);
  initialize(f.db, input({ normalizationDefaults: b }));
  assert.equal(
    normalizeNovelAiRequest(raw, owner.readCurrent().normalizationDefaults).artist,
    'current-profile-B',
  );
  const mapping = f.inputs.readGenerationIdempotency('m1', 'generate', keyHash),
    before = databaseState(f.db);
  assert.equal(
    f.inputs.readMemberContext(mapping.requestId, 'm1').defaults.defaultArtist,
    'original-profile-A',
  );
  assert.ok(f.inputs.readJobInput('historical-job').prompt.includes('original-profile-A'));
  const receipt = await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    () => f.generations.readAcceptedGenerationReceipt(acceptedMetadata, mapping),
    {},
  );
  assert.equal(receipt.replayed, true);
  assert.equal(databaseState(f.db), before);
  const target = empty(t, 'generation-history');
  replay(target, f.db);
  const selected = await snapshot(f.db, f.dir, 'A-history-B-current');
  await verifyAndInstallRelationalStorage(target, selected);
  assert.equal(
    new RelationalConfigurationStore(target).readCurrent().normalizationDefaults.defaultArtist,
    'current-profile-B',
  );
  assert.ok(
    new RelationalInputStore(target)
      .readJobInput('historical-job')
      .prompt.includes('original-profile-A'),
  );
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM normalization_contexts').get().n, 2);
});
