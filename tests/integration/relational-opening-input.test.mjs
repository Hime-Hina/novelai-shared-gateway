import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import path from 'node:path';
import Database from 'better-sqlite3';
import { fixture, meta, NOW } from '../helpers/relational-generation.mjs';
import { initializeFixtureConfiguration } from '../helpers/relational-configuration.mjs';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import {
  appendRelationalCommit,
  readRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { relationalSnapshotSha256 } from '../../server/storage/relational-private-restore.ts';
import {
  normalizeMemberGeneration,
  materializeMemberGeneration,
} from '../../server/generation/member-contract.ts';
import { prepareRelationalInput } from '../../server/generation/relational-input.ts';

const origin = { sourceId: 'synthetic-opening', originalId: 'original-input' };
const body = {
  input: '',
  parameters: { reference_image_multiple: [{ assetId: 'original' }], scale: 0 },
};
const bytes = Buffer.from('original protected opaque attachment');
const asset = {
  assetId: 'original',
  mimeType: 'application/octet-stream',
  byteLength: bytes.length,
  sha256: createHash('sha256').update(bytes).digest('hex'),
  bytes,
};
function databaseState(db) {
  return JSON.stringify(
    db
      .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
      .all()
      .map(({ name }) => [
        name,
        db.prepare(`SELECT * FROM "${name}" ORDER BY rowid`).safeIntegers().all(),
      ]),
    (_key, value) => (typeof value === 'bigint' ? String(value) : value),
  );
}
function command(db, id, events) {
  const { policyVersion: _p, pricingVersion: _v, ...metadata } = meta(id);
  return {
    ...metadata,
    expectedVersions: Object.fromEntries(
      events.map((event) => [
        event.streamId,
        db
          .prepare('SELECT CAST(stream_version AS TEXT) AS v FROM stream_heads WHERE stream_id=?')
          .get(event.streamId)?.v ?? '0',
      ]),
    ),
    receipt: { imported: id },
    events,
  };
}
function event(eventType, payload) {
  return {
    eventType,
    streamId:
      eventType === 'ContextRegistered'
        ? `context:${payload.contextId}`
        : eventType === 'OpeningInputRecorded'
          ? `input:${payload.inputId}`
          : `request:${payload.requestId}`,
    occurredAt: NOW + 100,
    accountId: null,
    memberId: payload.memberId ?? null,
    policyVersion: 'original-policy',
    pricingVersion: 'original-price',
    payload,
  };
}
function request(inputId = 'historical', changes = {}) {
  return event('OpeningRequestRecorded', {
    requestId: `request-${inputId}`,
    memberId: 'm1',
    inputId,
    contextId: null,
    source: 'original-source',
    operation: 'generate',
    model: null,
    sampleCount: 1,
    seedMode: 'unknown',
    confirmation: null,
    acceptedAt: 0,
    retiredAt: null,
    origin: { ...origin, originalId: `original-request-${inputId}` },
    ...changes,
  });
}
function append(f, input = {}, requests = [request()]) {
  return f.db.transaction(() => {
    const store = new RelationalInputStore(f.db);
    const payload = store.stageOpeningInput({
      inputId: 'historical',
      memberId: 'm1',
      completeness: 'partial',
      body,
      logicalBytes: Buffer.byteLength(JSON.stringify(body)),
      assets: [asset],
      ...input,
    });
    return new RelationalCommitReplayer(f.db).appendCommit(
      command(f.db, `import-${payload.inputId}`, [
        event('OpeningInputRecorded', { ...payload, origin }),
        ...requests,
      ]),
      store,
    );
  })();
}
function empty(t, history = 'generation-history') {
  const db = new Database(':memory:');
  initializeRelationalStorage(db, history);
  t.after(() => db.close());
  return db;
}
function replayUntil(db, source, before = null) {
  const replay = new RelationalCommitReplayer(db),
    inputs = new RelationalInputStore(source);
  for (const { seq } of source
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    if (seq === before) {
      break;
    }
    db.transaction(() => replay.replayCommit(readRelationalCommit(source, seq), inputs))();
  }
}

test('Opening 输入/请求保留 partial 已知值与附件；完整提交重建和重复均不复活私有认证', async (t) => {
  const f = await fixture(t),
    result = append(f),
    requestRow = f.db
      .prepare("SELECT * FROM generation_requests WHERE id='request-historical'")
      .get();
  assert.equal(requestRow.accepted_at, 0);
  assert.equal(requestRow.model, null);
  assert.equal(requestRow.context_id, null);
  assert.equal(requestRow.seed_mode, 'unknown');
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_jobs').get().n, 0);
  const recovered = new RelationalInputStore(f.db).readMemberInput('request-historical', 'm1');
  assert.equal(recovered.recoverable, false);
  assert.deepEqual(recovered.input, {
    input: '',
    parameters: {
      reference_image_multiple: [{ slot: 'parameters.reference_image_multiple.0' }],
      scale: 0,
    },
  });
  assert.deepEqual(
    new RelationalInputStore(f.db).readMemberAsset(
      'request-historical',
      'm1',
      'parameters.reference_image_multiple.0',
    ).bytes,
    bytes,
  );
  const target = empty(t),
    originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = () => {
    calls++;
    throw new Error('Replay cannot perform external IO.');
  };
  try {
    replayUntil(target, f.db);
    assert.deepEqual(
      target.prepare('SELECT * FROM generation_inputs').all(),
      f.db.prepare('SELECT * FROM generation_inputs').all(),
    );
    assert.deepEqual(
      target.prepare('SELECT * FROM input_assets').all(),
      f.db.prepare('SELECT * FROM input_assets').all(),
    );
    assert.deepEqual(target.prepare('SELECT * FROM generation_requests').all(), [requestRow]);
    assert.equal(
      target.prepare("SELECT auth_epoch FROM members WHERE id='m1'").get().auth_epoch,
      null,
    );
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
    const before = databaseState(target);
    target.transaction(() =>
      new RelationalCommitReplayer(target).replayCommit(
        readRelationalCommit(f.db, result.commitSeq),
        new RelationalInputStore(f.db),
      ),
    )();
    assert.equal(databaseState(target), before);
    assert.equal(calls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Opening unavailable 明确保存空内容；complete 复用原首次上下文而不从当前默认重建', async (t) => {
  const f = await fixture(t);
  append(
    f,
    {
      inputId: 'unavailable',
      completeness: 'unavailable',
      body: null,
      logicalBytes: 0,
      assets: [],
    },
    [request('unavailable')],
  );
  const lost = new RelationalInputStore(f.db).readMemberInput('request-unavailable', 'm1');
  assert.equal(lost.input, null);
  assert.equal(lost.recoverable, false);
  f.accept('original-complete');
  const original = f.db
      .prepare("SELECT * FROM generation_requests WHERE id='original-complete'")
      .get(),
    input = f.db.prepare('SELECT * FROM generation_inputs WHERE id=?').get(original.input_id);
  append(
    f,
    {
      inputId: 'complete',
      completeness: 'complete',
      body: JSON.parse(input.body),
      logicalBytes: input.logical_bytes,
      assets: [],
    },
    [
      request('complete', {
        model: original.model,
        contextId: original.context_id,
        seedMode: 'fixed',
      }),
    ],
  );
  const target = empty(t);
  replayUntil(target, f.db);
  assert.deepEqual(
    target.prepare('SELECT * FROM normalization_contexts').all(),
    f.db.prepare('SELECT * FROM normalization_contexts').all(),
  );
  assert.equal(
    target.prepare("SELECT completeness FROM generation_inputs WHERE id='complete'").get()
      .completeness,
    'complete',
  );
  assert.equal(
    target.prepare("SELECT logical_bytes FROM generation_inputs WHERE id='unavailable'").get()
      .logical_bytes,
    0,
  );
});

test('Opening complete 可与原捕获上下文同笔登记；晚登记上下文和配置profile冒充均拒绝', async (t) => {
  const f = await fixture(t),
    normalized = normalizeMemberGeneration({
      operation: 'generate',
      model: 'nai-diffusion-4-5-full',
      input: 'explicit original synthetic input',
      parameters: { width: 128, height: 128, steps: 17, seed: 7, n_samples: 1 },
    }),
    prepared = prepareRelationalInput(materializeMemberGeneration(normalized), {
      kind: 'native',
      native: normalized.normalizationContext.native,
    }),
    before = databaseState(f.db);
  const importOriginal = (lateContext = false) =>
    f.db.transaction(() => {
      const store = new RelationalInputStore(f.db),
        context = event(
          'ContextRegistered',
          store.stageOpeningContext(prepared.contextId, prepared.context),
        ),
        input = event('OpeningInputRecorded', {
          ...store.stageOpeningInput({
            inputId: 'captured',
            memberId: 'm1',
            completeness: 'complete',
            body: prepared.body,
            logicalBytes: prepared.logicalBytes,
            assets: prepared.assets,
          }),
          origin,
        }),
        accepted = request('captured', {
          contextId: prepared.contextId,
          model: prepared.body.model,
          seedMode: 'fixed',
        });
      return new RelationalCommitReplayer(f.db).appendCommit(
        command(
          f.db,
          'captured-import',
          lateContext ? [input, accepted, context] : [context, input, accepted],
        ),
        store,
      );
    })();
  assert.throws(
    () => importOriginal(true),
    (e) => e.statusCode === 410,
  );
  assert.equal(databaseState(f.db), before);
  f.db.transaction(() => {
    assert.throws(() =>
      new RelationalInputStore(f.db).stageOpeningContext('profile-is-not-generation-context', {
        kind: 'normalization-defaults',
        values: {},
      }),
    );
  })();
  assert.equal(databaseState(f.db), before);
  const result = importOriginal(),
    target = empty(t);
  replayUntil(target, f.db);
  assert.deepEqual(
    target.prepare('SELECT * FROM normalization_contexts').all(),
    f.db.prepare('SELECT * FROM normalization_contexts').all(),
  );
  assert.equal(
    readRelationalCommit(target, result.commitSeq).events[0].eventType,
    'ContextRegistered',
  );
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM generation_jobs').get().n, 0);
});

test('Opening 不能从源未来提交借首次上下文；完整合法envelope也不能提前投影', async (t) => {
  const f = await fixture(t),
    normalized = normalizeMemberGeneration({
      operation: 'generate',
      model: 'nai-diffusion-4-5-full',
      input: 'original',
      parameters: { width: 128, height: 128, steps: 20, seed: 7 },
    }),
    prepared = prepareRelationalInput(materializeMemberGeneration(normalized), {
      kind: 'native',
      native: normalized.normalizationContext.native,
    });
  let first;
  // 仅构造不可信源反例；用原提交原语保存完整事实，故意绕过业务 owner。
  f.db.transaction(() => {
    const store = new RelationalInputStore(f.db),
      context = event(
        'ContextRegistered',
        store.stageOpeningContext(prepared.contextId, prepared.context),
      ),
      input = event('OpeningInputRecorded', {
        ...store.stageOpeningInput({
          inputId: 'future-context',
          memberId: 'm1',
          completeness: 'complete',
          body: prepared.body,
          logicalBytes: prepared.logicalBytes,
          assets: prepared.assets,
        }),
        origin,
      });
    first = appendRelationalCommit(
      f.db,
      command(f.db, 'premature-context-reference', [
        input,
        request('future-context', {
          contextId: prepared.contextId,
          model: prepared.body.model,
          seedMode: 'fixed',
        }),
      ]),
    );
    appendRelationalCommit(f.db, command(f.db, 'later-context-registration', [context]));
  })();
  const target = empty(t);
  replayUntil(target, f.db, first.commitSeq);
  const before = databaseState(target);
  target.transaction(() => {
    assert.throws(
      () =>
        new RelationalCommitReplayer(target).replayCommit(
          readRelationalCommit(f.db, first.commitSeq),
          new RelationalInputStore(f.db),
        ),
      (e) => e.statusCode === 410,
    );
  })();
  assert.equal(databaseState(target), before);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM normalization_contexts').get().n, 0);
});

test('Opening 已知正文冲突/附件坏字节/伪装unavailable均在原暂存保存点回滚', async (t) => {
  const f = await fixture(t);
  for (const [input, requests] of [
    [{ body: { operation: 'upscale', input: '' }, assets: [] }, [request()]],
    [{ assets: [{ ...asset, bytes: Buffer.from('x'.repeat(bytes.length)) }] }, [request()]],
    [{ completeness: 'unavailable', logicalBytes: 0 }, [request()]],
    [{ completeness: 'partial', body: null, assets: [] }, [request()]],
    [{}, [request('historical', { origin: { ...origin, sourceId: 'unregistered' } })]],
  ]) {
    const before = databaseState(f.db);
    assert.throws(() => append(f, input, requests));
    assert.equal(databaseState(f.db), before);
  }
});

test('Opening 请求可引用先前原输入；未知正文NULL和已退休时间0不补成可执行上下文', async (t) => {
  const f = await fixture(t);
  append(f, { body: { operation: null, model: null, input: '' }, assets: [] });
  const current = new RelationalInputStore(f.db).readMemberInput('request-historical', 'm1');
  assert.deepEqual(current.input, { operation: null, model: null, input: '' });
  assert.equal(current.recoverable, false);
  const additional = request('historical', { requestId: 'retired-reference', retiredAt: 0 });
  f.db.transaction(() =>
    new RelationalCommitReplayer(f.db).appendCommit(
      command(f.db, 'reference-old-input', [additional]),
      new RelationalInputStore(f.db),
    ),
  )();
  assert.equal(
    f.db.prepare("SELECT retired_at FROM generation_requests WHERE id='retired-reference'").get()
      .retired_at,
    0,
  );
  assert.throws(
    () => new RelationalInputStore(f.db).readMemberInput('retired-reference', 'm1'),
    (e) => e.statusCode === 410,
  );
  const target = empty(t);
  replayUntil(target, f.db);
  assert.deepEqual(
    target.prepare('SELECT * FROM generation_requests').all(),
    f.db.prepare('SELECT * FROM generation_requests').all(),
  );
});

test('Opening 原保护来源缺行、同长坏BLOB及完整原回执错源均拒绝且可用原源重试', async (t) => {
  const f = await fixture(t),
    result = append(f),
    stored = readRelationalCommit(f.db, result.commitSeq);
  for (const kind of ['missing', 'blob', 'receipt']) {
    await t.test(kind, () => {
      const source = new Database(f.db.serialize()),
        target = empty(t);
      t.after(() => source.close());
      replayUntil(target, f.db, result.commitSeq);
      if (kind === 'missing') {
        source.pragma('foreign_keys=OFF');
        source.prepare("DELETE FROM input_assets WHERE input_id='historical'").run();
        source.prepare("DELETE FROM generation_inputs WHERE id='historical'").run();
        source.pragma('foreign_keys=ON');
      } else {
        const triggerName =
            kind === 'blob' ? 'input_assets_identity' : 'commits_append_only_update',
          sql = source.prepare('SELECT sql FROM sqlite_schema WHERE name=?').get(triggerName).sql;
        source.exec(`DROP TRIGGER ${triggerName}`);
        if (kind === 'blob') {
          source
            .prepare("UPDATE input_assets SET bytes=? WHERE input_id='historical'")
            .run(Buffer.from('x'.repeat(bytes.length)));
        } else {
          source
            .prepare('UPDATE commits SET receipt=? WHERE commit_seq=?')
            .run('{"different":"original receipt"}', BigInt(result.commitSeq));
        }
        source.exec(sql);
      }
      const before = databaseState(target),
        replayer = new RelationalCommitReplayer(target);
      target.transaction(() => {
        assert.throws(() => replayer.replayCommit(stored, new RelationalInputStore(source)));
      })();
      assert.equal(databaseState(target), before);
      target.transaction(() => replayer.replayCommit(stored, new RelationalInputStore(f.db)))();
      assert.equal(
        target
          .prepare("SELECT COUNT(*) AS n FROM generation_requests WHERE id='request-historical'")
          .get().n,
        1,
      );
    });
  }
});

test('Opening 同ID第二次登记不能换原来源/正文；请求先于本笔输入登记也必须拒绝', async (t) => {
  const f = await fixture(t),
    result = append(f),
    stored = readRelationalCommit(f.db, result.commitSeq),
    duplicate = { ...stored.command.events[0], eventId: undefined };
  delete duplicate.eventId;
  const before = databaseState(f.db);
  assert.throws(() =>
    f.db.transaction(() =>
      new RelationalCommitReplayer(f.db).appendCommit(
        command(f.db, 'duplicate-input', [duplicate]),
        new RelationalInputStore(f.db),
      ),
    )(),
  );
  assert.equal(databaseState(f.db), before);
  const next = await fixture(t),
    beforeNext = databaseState(next.db);
  assert.throws(() =>
    next.db.transaction(() => {
      const inputs = new RelationalInputStore(next.db),
        payload = inputs.stageOpeningInput({
          inputId: 'historical',
          memberId: 'm1',
          completeness: 'partial',
          body,
          logicalBytes: 0,
          assets: [asset],
        });
      new RelationalCommitReplayer(next.db).appendCommit(
        command(next.db, 'request-before-input', [
          request(),
          event('OpeningInputRecorded', { ...payload, origin }),
        ]),
        inputs,
      );
    })(),
  );
  assert.equal(databaseState(next.db), beforeNext);
});

test('Opening 原保护内容进入唯一ready安装；安装后新Opening窄回放失败且原重复零写', async (t) => {
  const f = await fixture(t),
    result = append(f);
  initializeFixtureConfiguration(f.db);
  const sourcePath = path.join(f.dir, 'opening-selected.sqlite');
  await f.db.backup(sourcePath);
  const sealed = new Database(sourcePath);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  const selected = {
    sourcePath,
    sourceSha256: relationalSnapshotSha256(sourcePath),
    historyId: 'generation-history',
    finalCommitSeq: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get()
      .seq,
    replicas: { dataDir: f.dir },
  };
  const target = empty(t);
  replayUntil(target, f.db);
  await verifyAndInstallRelationalStorage(target, selected);
  assert.equal(target.prepare('SELECT status FROM projection_state').get().status, 'ready');
  const before = databaseState(target),
    inputs = new RelationalInputStore(target),
    sourceInputs = new RelationalInputStore(f.db);
  target.transaction(() => inputs.replayAcceptedInput(sourceInputs, result.commitSeq))();
  assert.equal(databaseState(target), before);
  const next = append(
    f,
    { inputId: 'later', body: null, completeness: 'unavailable', logicalBytes: 0, assets: [] },
    [request('later')],
  );
  target.transaction(() => {
    assert.throws(
      () => inputs.replayAcceptedInput(sourceInputs, next.commitSeq),
      /building target/,
    );
  })();
  assert.equal(databaseState(target), before);
  assert.equal(relationalSnapshotSha256(sourcePath), selected.sourceSha256);
});

test('Opening 复制后请求SQL失败：外层捕获仍回滚全表/完整提交，原提交可重试', async (t) => {
  const f = await fixture(t),
    result = append(f),
    target = empty(t);
  replayUntil(target, f.db, result.commitSeq);
  let observed = 0;
  target.function('opening_input_and_request_written', (inputs, requests) => {
    assert.equal(inputs, 1);
    assert.equal(requests, 1);
    observed++;
    return 1;
  });
  target.exec(`CREATE TEMP TABLE outer_sentinel(value INTEGER);
    CREATE TEMP TRIGGER fail_opening_request AFTER INSERT ON main.generation_requests
    WHEN NEW.id='request-historical' BEGIN
      SELECT opening_input_and_request_written(
        (SELECT COUNT(*) FROM generation_inputs WHERE id='historical'),
        (SELECT COUNT(*) FROM generation_requests WHERE id='request-historical'));
      SELECT RAISE(ABORT,'opening request SQL failure');
    END`);
  const before = databaseState(target),
    stored = readRelationalCommit(f.db, result.commitSeq),
    replayer = new RelationalCommitReplayer(target),
    inputs = new RelationalInputStore(f.db);
  target.transaction(() => {
    assert.throws(() => replayer.replayCommit(stored, inputs), /opening request SQL failure/);
    target.prepare('INSERT INTO outer_sentinel VALUES(1)').run();
  })();
  assert.equal(observed, 1);
  assert.equal(databaseState(target), before);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM outer_sentinel').get().n, 1);
  target.exec('DROP TRIGGER fail_opening_request');
  target.transaction(() => replayer.replayCommit(stored, inputs))();
  assert.equal(
    target
      .prepare("SELECT COUNT(*) AS n FROM generation_requests WHERE id='request-historical'")
      .get().n,
    1,
  );
});
