import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { deflateSync } from 'node:zlib';
import Database from 'better-sqlite3';
import { fixture, meta, NOW } from '../helpers/relational-generation.mjs';
import { initializeFixtureConfiguration } from '../helpers/relational-configuration.mjs';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageFormat,
} from '../../server/storage/relational-storage.ts';
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
import {
  prepareRelationalInput,
  assertRelationalCompleteInput,
  assertRelationalInputDefinition,
  relationalInputCacheKey,
} from '../../server/generation/relational-input.ts';
import {
  parseNativeImageRequest,
  parseNativeAuxiliaryRequest,
  nativeNormalizationContext,
  crc32,
} from '../../server/protocols/native-image.ts';
import { normalizeNovelAiRequest } from '../../server/protocols/novelai-client.ts';
import { requestNormalizationDefaults } from '../../server/generation/request-fingerprint.ts';

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
function png() {
  const chunk = (name, bytes) => {
      const head = Buffer.alloc(8),
        tail = Buffer.alloc(4);
      head.writeUInt32BE(bytes.length);
      head.write(name, 4);
      tail.writeUInt32BE(crc32(Buffer.concat([Buffer.from(name), bytes])));
      return Buffer.concat([head, bytes, tail]);
    },
    header = Buffer.alloc(13);
  header.writeUInt32BE(128);
  header.writeUInt32BE(128, 4);
  header[8] = 8;
  header[9] = 6;
  return Buffer.concat([
    Buffer.from('89504e470d0a1a0a', 'hex'),
    chunk('IHDR', header),
    chunk('IDAT', deflateSync(Buffer.alloc(513 * 128))),
    chunk('IEND', Buffer.alloc(0)),
  ]);
}
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

test('Opening complete 已知上下文必须核实际附件MIME和图像内容，不能只靠自洽SHA/长度', async (t) => {
  const f = await fixture(t);
  f.accept('known-image-context');
  const original = f.db
      .prepare("SELECT * FROM generation_requests WHERE id='known-image-context'")
      .get(),
    originalBody = JSON.parse(
      f.db.prepare('SELECT body FROM generation_inputs WHERE id=?').get(original.input_id).body,
    ),
    image = png(),
    context = JSON.parse(
      f.db.prepare('SELECT body FROM normalization_contexts WHERE id=?').get(original.context_id)
        .body,
    ),
    prepared = prepareRelationalInput(
      parseNativeImageRequest(
        {
          input: originalBody.input,
          model: originalBody.model,
          action: 'img2img',
          parameters: {
            ...originalBody.parameters,
            seed: 7,
            n_samples: 1,
            image: image.toString('base64'),
            strength: 0.7,
          },
        },
        context.native,
      ),
      context,
    ),
    complete = {
      ...prepared.body,
      parameters: { ...prepared.body.parameters, image: { assetId: 'original' } },
    },
    before = databaseState(f.db);
  for (const [bytes, mimeType] of [
    [asset.bytes, 'image/png'],
    [image, 'image/webp'],
  ]) {
    assert.throws(() =>
      append(
        f,
        {
          inputId: 'complete-image',
          completeness: 'complete',
          body: complete,
          logicalBytes: Buffer.byteLength(JSON.stringify(complete)) + bytes.length,
          assets: [
            {
              ...asset,
              mimeType,
              bytes,
              byteLength: bytes.length,
              sha256: createHash('sha256').update(bytes).digest('hex'),
            },
          ],
        },
        [request('complete-image', { model: original.model, contextId: original.context_id })],
      ),
    );
    assert.equal(databaseState(f.db), before);
  }
  append(
    f,
    {
      inputId: 'complete-image',
      completeness: 'complete',
      body: complete,
      logicalBytes: Buffer.byteLength(JSON.stringify(complete)) + image.length,
      assets: [
        {
          ...asset,
          mimeType: 'image/png',
          bytes: image,
          byteLength: image.length,
          sha256: createHash('sha256').update(image).digest('hex'),
        },
      ],
    },
    [request('complete-image', { model: original.model, contextId: original.context_id })],
  );
  assert.deepEqual(
    new RelationalInputStore(f.db).readMemberAsset(
      'request-complete-image',
      'm1',
      'parameters.image',
    ).bytes,
    image,
  );
  const target = empty(t);
  replayUntil(target, f.db);
  assert.deepEqual(
    target.prepare("SELECT * FROM input_assets WHERE input_id='complete-image'").all(),
    f.db.prepare("SELECT * FROM input_assets WHERE input_id='complete-image'").all(),
  );
});

test('Opening complete 按保存的Native定义拒绝未知字段/非法值；Direct保留原build范围', async (t) => {
  const f = await fixture(t);
  f.accept('original-definition');
  const original = f.db
      .prepare("SELECT * FROM generation_requests WHERE id='original-definition'")
      .get(),
    saved = JSON.parse(
      f.db.prepare('SELECT body FROM generation_inputs WHERE id=?').get(original.input_id).body,
    ),
    before = databaseState(f.db);
  for (const parameters of [
    { ...saved.parameters, not_an_original_parameter: true },
    { ...saved.parameters, width: 127 },
  ]) {
    const body = { ...saved, parameters };
    for (const sampleCount of [1, null]) {
      assert.throws(() =>
        append(
          f,
          {
            inputId: 'invalid-definition',
            completeness: 'complete',
            body,
            assets: [],
            logicalBytes: Buffer.byteLength(JSON.stringify(body)),
          },
          [
            request('invalid-definition', {
              model: original.model,
              contextId: original.context_id,
              sampleCount,
            }),
          ],
        ),
      );
      assert.equal(databaseState(f.db), before);
    }
  }
  const oldNative = JSON.parse(
    f.db.prepare('SELECT body FROM normalization_contexts WHERE id=?').get(original.context_id)
      .body,
  ).native;
  oldNative.parameterNames = oldNative.parameterNames.filter((name) => name !== 'sm');
  const appendOldDefinition = (body, sampleCount = 1) =>
    f.db.transaction(() => {
      const inputs = new RelationalInputStore(f.db),
        registered = event(
          'ContextRegistered',
          inputs.stageOpeningContext('original-old-definition', {
            kind: 'native',
            native: oldNative,
          }),
        ),
        input = event('OpeningInputRecorded', {
          ...inputs.stageOpeningInput({
            inputId: 'old-definition',
            memberId: 'm1',
            completeness: 'complete',
            body,
            logicalBytes: Buffer.byteLength(JSON.stringify(body)),
            assets: [],
          }),
          origin,
        });
      return new RelationalCommitReplayer(f.db).appendCommit(
        command(f.db, 'old-definition-import', [
          registered,
          input,
          request('old-definition', {
            model: original.model,
            contextId: 'original-old-definition',
            sampleCount,
          }),
        ]),
        inputs,
      );
    })();
  for (const sampleCount of [1, null]) {
    assert.throws(() =>
      appendOldDefinition(
        { ...saved, parameters: { ...saved.parameters, sm: false } },
        sampleCount,
      ),
    );
    assert.equal(databaseState(f.db), before);
  }
  appendOldDefinition(saved);
  const context = {
      kind: 'direct',
      native: nativeNormalizationContext(),
      defaults: requestNormalizationDefaults({}),
    },
    prepared = prepareRelationalInput(
      normalizeNovelAiRequest(
        {
          tag: 'original direct',
          model: 'nai-diffusion-3',
          width: 128,
          height: 128,
          steps: 23,
          scale: 20,
          cfg: 0,
          sampler: 'original-direct-sampler',
          noiseSchedule: 'original-direct-schedule',
          seed: 7,
        },
        context.defaults,
      ),
      context,
    ),
    originalBody = structuredClone(prepared.body);
  assert.equal(prepared.body.parameters.scale, 20);
  assert.throws(() =>
    parseNativeImageRequest(
      {
        input: prepared.body.input,
        model: prepared.body.model,
        action: prepared.body.action,
        parameters: { ...prepared.body.parameters, seed: 7, n_samples: 1 },
      },
      context.native,
    ),
  );
  const appendDirect = (body, sampleCount = 1) =>
    f.db.transaction(() => {
      const store = new RelationalInputStore(f.db),
        registered = event(
          'ContextRegistered',
          store.stageOpeningContext(prepared.contextId, context),
        ),
        input = event('OpeningInputRecorded', {
          ...store.stageOpeningInput({
            inputId: 'original-direct',
            memberId: 'm1',
            completeness: 'complete',
            body,
            logicalBytes: prepared.logicalBytes,
            assets: prepared.assets,
          }),
          origin,
        });
      new RelationalCommitReplayer(f.db).appendCommit(
        command(f.db, 'direct-original-definition', [
          registered,
          input,
          request('original-direct', {
            model: prepared.body.model,
            contextId: prepared.contextId,
            sampleCount,
          }),
        ]),
        store,
      );
    })();
  const beforeDirect = databaseState(f.db);
  for (const parameters of [
    { ...prepared.body.parameters, not_a_direct_parameter: true },
    { ...prepared.body.parameters, scale: false },
  ]) {
    for (const sampleCount of [1, null]) {
      assert.throws(() => appendDirect({ ...prepared.body, parameters }, sampleCount));
      assert.equal(databaseState(f.db), beforeDirect);
    }
  }
  for (const sampleCount of [1, null]) {
    assert.throws(() => appendDirect({ ...prepared.body, action: 'img2img' }, sampleCount));
    assert.equal(databaseState(f.db), beforeDirect);
  }
  appendDirect(prepared.body);
  const directUnknown = request('original-direct', {
    requestId: 'unknown-direct',
    model: prepared.body.model,
    contextId: prepared.contextId,
    sampleCount: null,
    origin: { ...origin, originalId: 'original-unknown-direct' },
  });
  f.db.transaction(() =>
    new RelationalCommitReplayer(f.db).appendCommit(
      command(f.db, 'unknown-direct', [directUnknown]),
      f.inputs,
    ),
  )();
  assert.equal(f.inputs.readMemberInput('unknown-direct', 'm1').recoverable, false);
  assert.equal(
    f.db.prepare("SELECT sample_count FROM generation_requests WHERE id='unknown-direct'").get()
      .sample_count,
    null,
  );
  assert.doesNotThrow(() =>
    assertRelationalInputDefinition(prepared.body, prepared.assets, context),
  );
  assert.throws(() => assertRelationalCompleteInput(prepared.body, prepared.assets, context, 2));
  const historicalBody = structuredClone(prepared.body);
  delete historicalBody.cacheSelection;
  const historicalBefore = structuredClone(historicalBody);
  assert.doesNotThrow(() =>
    assertRelationalCompleteInput(historicalBody, prepared.assets, context, 1),
  );
  assert.equal(relationalInputCacheKey('m1', { ...prepared, body: historicalBody }, 'fixed'), null);
  assert.deepEqual(historicalBody, historicalBefore);
  const blank = prepareRelationalInput(
    normalizeNovelAiRequest(
      {
        tag: '',
        artist: '',
        model: 'nai-diffusion-3',
        width: 128,
        height: 128,
        steps: 23,
        scale: 20,
        cfg: 0,
        sampler: 'original-direct-sampler',
        seed: 7,
      },
      context.defaults,
    ),
    context,
  );
  assert.equal(blank.body.input, '');
  assert.doesNotThrow(() => assertRelationalCompleteInput(blank.body, blank.assets, context, 1));
  assert.deepEqual(prepared.body, originalBody);
  assert.equal(Object.hasOwn(prepared.body.parameters, 'seed'), false);
  assert.equal(Object.hasOwn(prepared.body.parameters, 'n_samples'), false);
  const target = empty(t);
  replayUntil(target, f.db);
  assert.deepEqual(
    JSON.parse(
      target.prepare("SELECT body FROM generation_inputs WHERE id='original-direct'").get().body,
    ),
    originalBody,
  );
  assert.equal(
    target
      .prepare("SELECT seed_mode FROM generation_requests WHERE id='request-original-direct'")
      .get().seed_mode,
    'unknown',
  );
});

test('Opening未知数量仍核原Medium及辅助操作正文，局部校验不写回数量或seed', async (t) => {
  const f = await fixture(t);
  f.accept('definition');
  const context = f.inputs.readMemberContext('definition', 'm1'),
    contextId = f.db
      .prepare("SELECT context_id FROM generation_requests WHERE id='definition'")
      .get().context_id,
    image = png().toString('base64'),
    definitions = [
      parseNativeImageRequest(
        {
          input: 'known Medium body',
          model: 'nai-diffusion-5-full-medium',
          action: 'generate',
          parameters: { width: 128, height: 128, seed: 7, n_samples: 2 },
        },
        context.native,
      ),
      parseNativeAuxiliaryRequest(
        { image, model: context.native.vibeModels[0] },
        'encode-vibe',
        context.native,
      ),
      parseNativeAuxiliaryRequest({ image }, 'upscale', context.native),
    ];
  for (const definition of definitions) {
    const prepared = prepareRelationalInput(definition, context),
      operation = prepared.body.operation,
      beforeBody = structuredClone(prepared.body),
      invalid = structuredClone(prepared.body),
      id = `body-${operation}`;
    if (operation === 'generate') {
      invalid.parameters.steps = context.native.mediumProfile.steps + 1;
      for (const key of ['seed', 'n_samples']) {
        const illegal = structuredClone(prepared.body);
        illegal.parameters[key] = 7;
        assert.throws(() => assertRelationalInputDefinition(illegal, prepared.assets, context));
      }
    } else if (operation === 'encode-vibe') {
      invalid.information_extracted = 2;
    } else {
      invalid.declared_blur_sigma = 0.2;
    }
    const options = {
        inputId: id,
        completeness: 'complete',
        logicalBytes: prepared.logicalBytes,
        assets: prepared.assets,
      },
      requestOptions = {
        operation,
        model: prepared.body.model,
        contextId,
        seedMode: operation === 'generate' ? 'fixed' : 'not_applicable',
      },
      before = databaseState(f.db);
    for (const sampleCount of [1, null]) {
      assert.throws(() =>
        append(f, { ...options, body: invalid }, [request(id, { ...requestOptions, sampleCount })]),
      );
      assert.equal(databaseState(f.db), before);
    }
    assert.doesNotThrow(() =>
      assertRelationalInputDefinition(prepared.body, prepared.assets, context),
    );
    assert.deepEqual(prepared.body, beforeBody);
    append(f, { ...options, body: prepared.body }, [
      request(id, { ...requestOptions, sampleCount: null }),
    ]);
    const copied = f.inputs.readMemberInput(`request-${id}`, 'm1');
    assert.equal(copied.recoverable, false);
    assert.equal(
      f.db.prepare('SELECT sample_count FROM generation_requests WHERE id=?').get(`request-${id}`)
        .sample_count,
      null,
    );
    if (operation === 'generate') {
      assert.equal(Object.hasOwn(copied.input.parameters, 'n_samples'), false);
      assert.equal(Object.hasOwn(copied.input.parameters, 'seed'), false);
    }
  }
  const target = empty(t);
  replayUntil(target, f.db);
  assert.deepEqual(
    target
      .prepare("SELECT sample_count FROM generation_requests WHERE id LIKE 'request-body-%'")
      .all(),
    [{ sample_count: null }, { sample_count: null }, { sample_count: null }],
  );
});

test('Opening未知数量的完整坏来源不能公共回放或安装ready，已知正文规则不因NULL缺席', async (t) => {
  const f = await fixture(t);
  f.accept('definition');
  initializeFixtureConfiguration(f.db);
  const original = f.db.prepare("SELECT * FROM generation_requests WHERE id='definition'").get(),
    body = JSON.parse(
      f.db.prepare('SELECT body FROM generation_inputs WHERE id=?').get(original.input_id).body,
    );
  body.parameters.not_an_original_parameter = 123;
  // 合成不可信来源的完整facts/projection，不能让缺行/错误cut遮住正文校验。
  const stored = f.db.transaction(() => {
      const payload = f.inputs.stageOpeningInput({
          inputId: 'untrusted-body',
          memberId: 'm1',
          completeness: 'complete',
          body,
          logicalBytes: Buffer.byteLength(JSON.stringify(body)),
          assets: [],
        }),
        openingRequest = request('untrusted-body', {
          requestId: 'untrusted-request',
          model: original.model,
          contextId: original.context_id,
          seedMode: 'fixed',
          sampleCount: null,
        }),
        result = appendRelationalCommit(
          f.db,
          command(f.db, 'untrusted-body-source', [
            event('OpeningInputRecorded', { ...payload, origin }),
            openingRequest,
          ]),
        ),
        p = openingRequest.payload;
      f.db
        .prepare(
          `INSERT INTO generation_requests(id,member_id,input_id,context_id,source,operation,model,
      sample_count,seed_mode,confirmation,accepted_at,retired_at,source_event_id)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
        )
        .run(
          p.requestId,
          p.memberId,
          p.inputId,
          p.contextId,
          p.source,
          p.operation,
          p.model,
          p.sampleCount,
          p.seedMode,
          null,
          p.acceptedAt,
          p.retiredAt,
          result.events[1].eventId,
        );
      return result;
    })(),
    target = empty(t);
  replayUntil(target, f.db, stored.commitSeq);
  const before = databaseState(target),
    invalidDefinition = (error) =>
      error.statusCode === 422 && /not_an_original_parameter/.test(error.message);
  target.exec('CREATE TEMP TABLE sentinel(value INTEGER)');
  target.transaction(() => {
    assert.throws(
      () =>
        new RelationalCommitReplayer(target).replayCommit(
          readRelationalCommit(f.db, stored.commitSeq),
          f.inputs,
        ),
      invalidDefinition,
    );
    target.prepare('INSERT INTO sentinel VALUES(1)').run();
  })();
  assert.equal(databaseState(target), before);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM sentinel').get().n, 1);
  const sourcePath = path.join(f.dir, 'untrusted-definition.sqlite');
  await f.db.backup(sourcePath);
  const sealed = new Database(sourcePath);
  sealed.pragma('journal_mode=DELETE');
  sealed.close();
  const sourceSha256 = relationalSnapshotSha256(sourcePath),
    untrusted = databaseState(f.db);
  await assert.rejects(
    verifyAndInstallRelationalStorage(f.db, {
      sourcePath,
      sourceSha256,
      historyId: 'generation-history',
      finalCommitSeq: stored.commitSeq,
      replicas: { dataDir: f.dir },
    }),
    invalidDefinition,
  );
  assert.equal(f.db.prepare('SELECT status FROM projection_state').get().status, 'building');
  assert.equal(databaseState(f.db), untrusted);
  assert.equal(relationalSnapshotSha256(sourcePath), sourceSha256);
});

test('Opening 请求操作与已知种子模式遵守同一原角色，unknown保持未知', async (t) => {
  const f = await fixture(t),
    before = databaseState(f.db);
  for (const changes of [
    { operation: 'generate', seedMode: 'not_applicable' },
    { operation: 'upscale', seedMode: 'fixed' },
  ]) {
    assert.throws(
      () =>
        append(
          f,
          {
            inputId: 'roles',
            completeness: 'unavailable',
            body: null,
            assets: [],
            logicalBytes: 0,
          },
          [request('roles', changes)],
        ),
      (e) => e.statusCode === 410,
    );
    assert.equal(databaseState(f.db), before);
  }
  append(
    f,
    { inputId: 'roles', completeness: 'unavailable', body: null, assets: [], logicalBytes: 0 },
    [request('roles', { operation: 'upscale', seedMode: 'unknown' })],
  );
  assert.equal(
    f.db.prepare("SELECT seed_mode FROM generation_requests WHERE id='request-roles'").get()
      .seed_mode,
    'unknown',
  );
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

test('Opening未知数量与已知数量共享complete正文和BLOB，不降级或复制输入', async (t) => {
  const f = await fixture(t);
  f.accept('definition');
  const original = f.inputs.readMemberContext('definition', 'm1'),
    image = png(),
    prepared = prepareRelationalInput(
      parseNativeImageRequest(
        {
          model: 'nai-diffusion-4-5-full',
          input: 'known body',
          action: 'img2img',
          parameters: {
            width: 128,
            height: 128,
            steps: 30,
            seed: 7,
            n_samples: 2,
            image: image.toString('base64'),
            strength: 0.7,
          },
        },
        original.native,
      ),
      original,
    );
  const shared = 'shared-complete',
    contextId = f.db
      .prepare("SELECT context_id FROM generation_requests WHERE id='definition'")
      .get().context_id;
  append(
    f,
    {
      inputId: shared,
      completeness: 'complete',
      body: prepared.body,
      logicalBytes: prepared.logicalBytes,
      assets: prepared.assets,
    },
    [
      request(shared, {
        requestId: 'known-count',
        model: prepared.body.model,
        contextId,
        sampleCount: 2,
        seedMode: 'fixed',
      }),
      request(shared, {
        requestId: 'unknown-count',
        model: prepared.body.model,
        sampleCount: null,
        seedMode: 'fixed',
      }),
    ],
  );
  const input = f.inputs.readMemberInput('unknown-count', 'm1');
  assert.equal(input.recoverable, false);
  assert.equal(Object.hasOwn(input.input.parameters, 'n_samples'), false);
  assert.equal(input.assets.length, 1);
  assert.deepEqual(
    f.inputs.readMemberAsset('unknown-count', 'm1', 'parameters.image').bytes,
    image,
  );
  assert.equal(
    f.db.prepare('SELECT completeness FROM generation_inputs WHERE id=?').get(shared).completeness,
    'complete',
  );
  assert.equal(
    f.db
      .prepare(
        "SELECT COUNT(DISTINCT input_id) AS n FROM generation_requests WHERE id IN ('known-count','unknown-count')",
      )
      .get().n,
    1,
  );
  const target = empty(t);
  replayUntil(target, f.db);
  assert.equal(
    target.prepare("SELECT sample_count FROM generation_requests WHERE id='unknown-count'").get()
      .sample_count,
    null,
  );
  assert.deepEqual(
    target.prepare('SELECT * FROM input_assets WHERE input_id=?').all(shared),
    f.db.prepare('SELECT * FROM input_assets WHERE input_id=?').all(shared),
  );
});

test('Opening未知数量格式拒绝正确旧5ca DDL/hash，不修改原临时库或旧历史', (t) => {
  const old = JSON.parse(
      readFileSync(new URL('../fixtures/relational-schema-v7-known-count.json', import.meta.url)),
    ),
    db = new Database(':memory:');
  t.after(() => db.close());
  assert.equal(old.sourceCommit, '5ca121fb76a6f63a011a48bae630789f7f976302');
  assert.equal(createHash('sha256').update(old.sql).digest('hex'), old.schemaSha256);
  db.exec(old.sql);
  const insert = db.prepare('INSERT INTO app_meta(key,value) VALUES(?,?)');
  insert.run('database_format', 'novelai-relational-v1');
  insert.run('schema_sha256', old.schemaSha256);
  insert.run(
    'event_authority',
    JSON.stringify({ version: old.writerVersion, minimumWriterVersion: old.writerVersion }),
  );
  db.prepare(
    "INSERT INTO projection_state(id,history_id,reducer_version,last_commit_seq,status) VALUES(1,'old-five-ca',?,0,'building')",
  ).run(old.reducerVersion);
  assert.equal(
    db
      .prepare('PRAGMA table_info(generation_requests)')
      .all()
      .find((c) => c.name === 'sample_count').notnull,
    1,
  );
  const before = db.serialize();
  assert.throws(() => assertRelationalStorageFormat(db), /Unsupported relational database format/);
  assert.throws(() => new RelationalInputStore(db), /Unsupported relational database format/);
  assert.deepEqual(db.serialize(), before);
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
