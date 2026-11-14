import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { deflateSync } from 'node:zlib';
import { Readable } from 'node:stream';
import { createServer } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import {
  appendRelationalCommit,
  readRelationalCommit,
  replayRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import { domainDigest } from '../../server/events/domain-events.ts';
import {
  normalizeMemberGeneration,
  materializeMemberGeneration,
} from '../../server/generation/member-contract.ts';
import {
  buildNovelAiParameters,
  normalizeNovelAiRequest,
  generateNovelAiImage,
} from '../../server/protocols/novelai-client.ts';
import { nativeNormalizationContext } from '../../server/protocols/native-image.ts';
import { requestNormalizationDefaults } from '../../server/generation/request-fingerprint.ts';
import {
  relationalInputDigest,
  relationalAssetSlots,
  prepareRelationalInput,
  validateRelationalInputBody,
} from '../../server/generation/relational-input.ts';
import { quoteNativeRequest } from '../../server/protocols/native-pricing.ts';
import { readNativeBody } from '../../server/protocols/native-body.ts';
import { singleGenerationRequest } from '../../server/generation/batch-requests.ts';
import { NATIVE_DEFAULTS, NATIVE_SAMPLERS, crc32 } from '../../server/protocols/native-image.ts';

function png(width = 128, height = 128) {
  const chunk = (type, data) => {
    const head = Buffer.alloc(8),
      tail = Buffer.alloc(4);
    head.writeUInt32BE(data.length);
    head.write(type, 4);
    tail.writeUInt32BE(crc32(Buffer.concat([Buffer.from(type), data])));
    return Buffer.concat([head, data, tail]);
  };
  const header = Buffer.alloc(13);
  header.writeUInt32BE(width);
  header.writeUInt32BE(height, 4);
  header[8] = 8;
  header[9] = 6;
  return Buffer.concat([
    Buffer.from('89504e470d0a1a0a', 'hex'),
    chunk('IHDR', header),
    chunk('IDAT', deflateSync(Buffer.alloc((width * 4 + 1) * height))),
    chunk('IEND', Buffer.alloc(0)),
  ]);
}
const image = png();
const image64 = image.toString('base64');
function metadata(id = 'accept1', memberId = 'm1') {
  return {
    commandId: id,
    fingerprint: domainDigest(id),
    actorKind: 'member',
    actorId: memberId,
    recordedAt: 10000,
    correlationId: null,
    causationId: null,
    policyVersion: 'policy1',
    pricingVersion: 'price1',
  };
}
function normalized(parameters = {}, extra = {}) {
  return normalizeMemberGeneration({
    operation: 'generate',
    model: 'nai-diffusion-4-5-full',
    input: 'exact input',
    action: 'generate',
    parameters: {
      width: 128,
      height: 128,
      steps: 23,
      seed: 4294967295,
      n_samples: 4,
      ...parameters,
    },
    ...extra,
  });
}
function accepted(n = normalized(), id = 'r1', memberId = 'm1') {
  return {
    inputId: `input-${id}`,
    requestId: id,
    memberId,
    source: 'member-generation',
    seedMode: n.operation === 'generate' ? (n.randomSeed ? 'random' : 'fixed') : 'not_applicable',
    confirmation: null,
    request: materializeMemberGeneration(n),
    context: { kind: 'member', member: n.normalizationContext },
    jobs: Array.from({ length: n.request.sampleCount || 1 }, (_, index) => ({
      jobId: `${id}-${index}`,
      status: 'pending',
      queueSeq: null,
      queueExpiresAt: null,
      deadlineAt: null,
      admissionEpoch: 0,
      cacheKey: null,
    })),
  };
}
function seed(sqlite) {
  const events = ['m1', 'm2'].map((memberId) => ({
    eventType: 'MemberRegistered',
    streamId: `member:${memberId}`,
    occurredAt: 1000,
    accountId: null,
    memberId,
    policyVersion: 'p1',
    pricingVersion: 'p1',
    payload: {
      memberId,
      displayName: memberId,
      status: 'active',
      acceptingJobs: true,
      admissionEpoch: 0,
      createdAt: 1000,
    },
  }));
  sqlite
    .transaction(() => {
      const result = appendRelationalCommit(sqlite, {
        ...metadata('members'),
        actorKind: 'admin',
        actorId: 'owner',
        expectedVersions: { 'member:m1': '0', 'member:m2': '0' },
        receipt: null,
        events,
      });
      for (const event of result.events) {
        sqlite
          .prepare(
            'INSERT INTO members(id,display_name,status,accepting_jobs,auth_epoch,admission_epoch,created_at,source_event_id) VALUES(?,?,?,1,?,0,?,?)',
          )
          .run(
            event.payload.memberId,
            event.payload.displayName,
            event.payload.status,
            'auth1',
            event.payload.createdAt,
            event.eventId,
          );
      }
    })
    .immediate();
}
function fixture(t, filename = ':memory:', options = {}) {
  const statements = [],
    sqlite = new Database(filename, { verbose: (sql) => statements.push(sql), ...options });
  t.after(() => sqlite.close());
  initializeRelationalStorage(sqlite, 'input-history');
  seed(sqlite);
  const store = new RelationalInputStore(sqlite);
  const persist = (input, command = metadata()) =>
    sqlite.transaction(() => store.persistAcceptedInput(command, input)).immediate();
  return { sqlite, store, persist, statements };
}
function mutate(sqlite, table, action) {
  const name = `${table}_identity`,
    sql = sqlite.prepare('SELECT sql FROM sqlite_schema WHERE name=?').get(name).sql;
  sqlite.transaction(() => {
    sqlite.exec(`DROP TRIGGER ${name}`);
    action();
    sqlite.exec(sql);
  })();
}

test('四图只保存一份规范正文和 BLOB，父子引用不含正文；成员列表不加载字节', (t) => {
  const { sqlite, store, persist, statements } = fixture(t);
  const input = accepted(
    normalized({ image: image64, mask: image64, strength: 0.7 }, { action: 'infill' }),
  );
  const before = structuredClone(input);
  const result = persist(input);
  assert.equal(result.replayed, false);
  assert.deepEqual(input, before);
  assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM generation_inputs').get().n, 1);
  assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM input_assets').get().n, 1);
  assert.deepEqual(sqlite.prepare('SELECT bytes FROM input_assets').get().bytes, image);
  const body = JSON.parse(sqlite.prepare('SELECT body FROM generation_inputs').get().body);
  assert.deepEqual(body.parameters.image, { assetId: 'a1' });
  assert.deepEqual(body.parameters.mask, { assetId: 'a1' });
  assert.equal(Object.hasOwn(body.parameters, 'seed'), false);
  assert.equal(Object.hasOwn(body.parameters, 'n_samples'), false);
  assert.ok(!JSON.stringify(body).includes(image64));
  assert.equal(Object.hasOwn(body, 'tag'), false);
  assert.equal(Object.hasOwn(body, 'nativeParameters'), false);
  assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM generation_jobs').get().n, 4);
  assert.deepEqual(
    sqlite
      .prepare('SELECT actual_seed FROM generation_jobs ORDER BY sample_index')
      .all()
      .map((row) => row.actual_seed),
    [4294967295, 0, 1, 2],
  );
  assert.equal(
    store.activePayloadBytes(),
    sqlite.prepare('SELECT logical_bytes FROM generation_inputs').get().logical_bytes,
  );
  statements.length = 0;
  const dto = store.readMemberInput('r1', 'm1');
  assert.equal(dto.input.parameters.seed, 4294967295);
  assert.equal(dto.input.parameters.n_samples, 4);
  assert.deepEqual(
    dto.assets.map((asset) => asset.slot),
    ['parameters.image', 'parameters.mask'],
  );
  assert.equal(dto.recoverable, true);
  assert.ok(statements.every((sql) => !/SELECT\s+bytes\s+FROM\s+input_assets/i.test(sql)));
  assert.deepEqual(store.readMemberAsset('r1', 'm1', 'parameters.image').bytes, image);
  for (let sample = 0; sample < 4; sample++) {
    const request = store.readJobInput(`r1-${sample}`),
      params = buildNovelAiParameters(request);
    assert.equal(params.seed, (4294967295 + sample) % 4294967296);
    assert.equal(params.n_samples, 1);
    assert.equal(params.image, image64);
    assert.equal(params.mask, image64);
  }
  const events = sqlite
    .prepare('SELECT payload FROM domain_events')
    .all()
    .map((row) => row.payload)
    .join('');
  assert.ok(!events.includes(image64));
  assert.ok(!events.includes('exact input'));
  assert.deepEqual(sqlite.pragma('foreign_key_check'), []);
});

test('新提交原子保存，响应丢失重领原回执；失败和外层回滚不留下内容或流头', (t) => {
  const { sqlite, store, persist } = fixture(t);
  assert.throws(
    () => store.persistAcceptedInput(metadata(), accepted()),
    /owning command transaction/,
  );
  const original = persist(accepted());
  const before = sqlite.serialize();
  assert.equal(persist(accepted(normalized(), 'unused-id')).replayed, true);
  assert.deepEqual(persist(accepted()).receipt, original.receipt);
  assert.deepEqual(sqlite.serialize(), before);
  assert.throws(
    () =>
      persist(accepted(normalized(), 'r2'), {
        ...metadata(),
        fingerprint: domainDigest('changed'),
      }),
    /idempotency conflict/,
  );
  assert.throws(
    () => persist(accepted(normalized(), 'r2', 'm2'), { ...metadata(), actorId: 'm2' }),
    /idempotency conflict/,
  );
  const bad = accepted(normalized(), 'r2');
  bad.jobs[1].jobId = 'r1-0';
  assert.throws(() => persist(bad, metadata('second')), /UNIQUE/);
  assert.deepEqual(sqlite.serialize(), before);
  assert.throws(
    () =>
      sqlite
        .transaction(() => {
          store.persistAcceptedInput(metadata('rollback'), accepted(normalized(), 'r3'));
          throw new Error('synthetic owner rollback');
        })
        .immediate(),
    /synthetic owner rollback/,
  );
  assert.deepEqual(sqlite.serialize(), before);
});

test('上下文按实际内容复用；停接单仍可重领，当前停用和跨成员不能读取正文或附件', (t) => {
  const { sqlite, store, persist } = fixture(t);
  persist(accepted(normalized({ n_samples: 1 })));
  persist(accepted(normalized({ n_samples: 1 }), 'r2'), metadata('second'));
  assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM normalization_contexts').get().n, 1);
  assert.equal(
    sqlite
      .prepare("SELECT COUNT(*) AS n FROM domain_events WHERE event_type='ContextRegistered'")
      .get().n,
    1,
  );
  assert.throws(
    () => store.readMemberInput('r1', 'm2'),
    (error) => error.statusCode === 404,
  );
  assert.throws(
    () => store.readMemberAsset('r1', 'm2', 'parameters.image'),
    (error) => error.statusCode === 404,
  );
  assert.throws(
    () => store.readMemberContext('r1', 'm2'),
    (error) => error.statusCode === 404,
  );
  assert.throws(
    () => store.readMemberAsset('r1', 'm1', '../../.env'),
    (error) => error.statusCode === 404,
  );
  sqlite.prepare("UPDATE members SET accepting_jobs=0 WHERE id='m1'").run();
  assert.equal(persist(accepted()).replayed, true);
  assert.equal(store.readMemberInput('r1', 'm1').recoverable, true);
  assert.throws(
    () => persist(accepted(normalized(), 'r3'), metadata('third')),
    (error) => error.code === 'admission_changed',
  );
  sqlite.prepare("UPDATE members SET status='disabled' WHERE id='m1'").run();
  assert.throws(
    () => persist(accepted()),
    (error) => error.statusCode === 403,
  );
  assert.throws(
    () => store.readMemberInput('r1', 'm1'),
    (error) => error.statusCode === 401,
  );
  sqlite.prepare("UPDATE members SET status='active',auth_epoch=NULL WHERE id='m1'").run();
  assert.throws(
    () => store.readMemberInput('r1', 'm1'),
    (error) => error.statusCode === 401,
  );
  assert.throws(
    () => persist(accepted()),
    (error) => error.statusCode === 403,
  );
  assert.equal(store.readJobInput('r1-0').seed, 4294967295);
});

test('预算按原展开载荷而非 BLOB 计；普通更新和读取不在任务、事件或流中回填正文', (t) => {
  const { sqlite, persist, store } = fixture(t);
  const input = accepted(
    normalized({ image: image64, strength: 0.7, n_samples: 1 }, { action: 'img2img' }),
  );
  const prepared = prepareRelationalInput(input.request, input.context);
  assert.ok(prepared.logicalBytes >= Buffer.byteLength(JSON.stringify(input.request)));
  assert.throws(
    () =>
      prepareRelationalInput(input.request, input.context, {
        REQUEST_MAX_PAYLOAD_BYTES: String(prepared.logicalBytes - 1),
      }),
    (error) => error.statusCode === 413,
  );
  const constrained = new RelationalInputStore(sqlite, {
    QUEUE_MAX_PAYLOAD_BYTES: String(prepared.logicalBytes - 1),
  });
  assert.throws(
    () => sqlite.transaction(() => constrained.persistAcceptedInput(metadata(), input))(),
    (error) => error.statusCode === 429,
  );
  assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM generation_inputs').get().n, 0);
  persist(input);
  const original = sqlite.prepare('SELECT body FROM generation_inputs').get().body;
  store.readJobInput('r1-0');
  store.readMemberInput('r1', 'm1');
  sqlite.prepare("UPDATE generation_jobs SET status='running' WHERE id='r1-0'").run();
  assert.equal(sqlite.prepare('SELECT body FROM generation_inputs').get().body, original);
  for (const table of ['generation_requests', 'generation_jobs', 'stream_heads']) {
    assert.ok(!JSON.stringify(sqlite.prepare(`SELECT * FROM ${table}`).all()).includes(image64));
  }
});

test('活动预算只扫描活动 child 覆盖索引，按 parent 计数且不受终态历史影响', (t) => {
  const { sqlite, store, persist, statements } = fixture(t);
  persist(accepted());
  const bytes = sqlite.prepare('SELECT logical_bytes FROM generation_inputs').get().logical_bytes;
  assert.equal(store.activePayloadBytes(), bytes);
  for (const [index, status] of ['queued', 'pending', 'running', 'failed'].entries()) {
    sqlite
      .prepare('UPDATE generation_jobs SET status=?,queue_seq=? WHERE id=?')
      .run(status, status === 'queued' ? 1 : null, `r1-${index}`);
  }
  assert.equal(store.activePayloadBytes(), bytes);
  // 只放大关系查询规模，来源引用复用夹具；不把这些行当作迁移或重建验收样本。
  sqlite.transaction(() => {
    sqlite.exec(`INSERT INTO generation_requests
      (id,member_id,input_id,context_id,source,operation,sample_count,seed_mode,accepted_at,source_event_id)
      SELECT 'shared',member_id,input_id,context_id,source,operation,1,seed_mode,accepted_at,source_event_id
      FROM generation_requests WHERE id='r1';
      INSERT INTO generation_jobs(id,request_id,sample_index,status,admission_epoch,source_event_id)
      SELECT 'shared-child','shared',0,'pending',0,source_event_id FROM generation_requests WHERE id='shared';
      WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<4096)
      INSERT INTO generation_requests
      (id,member_id,input_id,context_id,source,operation,sample_count,seed_mode,accepted_at,source_event_id)
      SELECT 'history-'||n,member_id,input_id,context_id,source,operation,1,seed_mode,accepted_at,source_event_id
      FROM history CROSS JOIN generation_requests WHERE id='r1';
      INSERT INTO generation_jobs(id,request_id,sample_index,status,admission_epoch,source_event_id)
      SELECT 'child-'||id,id,0,'failed',0,source_event_id FROM generation_requests WHERE id LIKE 'history-%';`);
  })();
  assert.equal(store.activePayloadBytes(), bytes * 2);
  const sql = statements.at(-1);
  const plan = sqlite
    .prepare(`EXPLAIN QUERY PLAN ${sql}`)
    .all()
    .map((row) => row.detail)
    .join('\n');
  t.diagnostic(plan);
  assert.match(plan, /SCAN generation_jobs USING COVERING INDEX idx_jobs_active_request/);
  assert.doesNotMatch(plan, /\bSCAN (?:r|generation_requests|i|generation_inputs)\b/);
  sqlite.prepare("UPDATE generation_jobs SET status='failed' WHERE request_id='r1'").run();
  assert.equal(store.activePayloadBytes(), bytes);
  sqlite.prepare("UPDATE generation_jobs SET status='failed' WHERE request_id='shared'").run();
  assert.equal(store.activePayloadBytes(), 0);
});

test('存储摘要与不可变事实核对；附件、正文或上下文篡改不会被悄悄覆盖', (t) => {
  const { sqlite, persist, store } = fixture(t);
  persist(accepted(normalized({ image: image64, strength: 0.7 }, { action: 'img2img' })));
  const changed = Buffer.from(image);
  changed[changed.length - 1] ^= 1;
  mutate(sqlite, 'input_assets', () =>
    sqlite.prepare('UPDATE input_assets SET bytes=?').run(changed),
  );
  assert.throws(
    () => store.readMemberAsset('r1', 'm1', 'parameters.image'),
    (error) => error.code === 'input_unavailable',
  );
  assert.throws(
    () => store.readJobInput('r1-0'),
    (error) => error.code === 'input_unavailable',
  );
  mutate(sqlite, 'input_assets', () =>
    sqlite.prepare('UPDATE input_assets SET bytes=?').run(image),
  );
  const row = sqlite.prepare('SELECT body,logical_bytes FROM generation_inputs').get(),
    body = JSON.parse(row.body);
  body.input = 'tampered';
  mutate(sqlite, 'generation_inputs', () =>
    sqlite.prepare('UPDATE generation_inputs SET body=?').run(JSON.stringify(body)),
  );
  assert.throws(
    () => store.readMemberInput('r1', 'm1'),
    (error) => error.code === 'input_unavailable',
  );
  mutate(sqlite, 'generation_inputs', () =>
    sqlite.prepare('UPDATE generation_inputs SET body=?').run(row.body),
  );
  mutate(sqlite, 'normalization_contexts', () =>
    sqlite.prepare("UPDATE normalization_contexts SET body='{}'").run(),
  );
  assert.throws(
    () => store.readMemberContext('r1', 'm1'),
    (error) => error.code === 'input_unavailable',
  );
});

test('重开后保留首次种子和完整上下文，不使用当前 defaults 或模型列表补造历史', async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), 'relational-input-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const filename = path.join(directory, 'library.sqlite');
  const sqlite = new Database(filename);
  initializeRelationalStorage(sqlite, 'input-history');
  seed(sqlite);
  const store = new RelationalInputStore(sqlite),
    input = accepted(normalized({ n_samples: 1, seed: -1 }));
  input.request = materializeMemberGeneration(normalized({ n_samples: 1, seed: -1 }), 123456);
  sqlite.transaction(() => store.persistAcceptedInput(metadata(), input)).immediate();
  const first = canonicalSnapshot(store.readJobInput('r1-0'));
  sqlite.close();
  const reopened = new Database(filename);
  reopened.pragma('foreign_keys=ON');
  try {
    const next = new RelationalInputStore(reopened),
      scale = NATIVE_DEFAULTS.scale,
      sampler = [...NATIVE_SAMPLERS];
    NATIVE_DEFAULTS.scale = 19;
    NATIVE_SAMPLERS.splice(0);
    try {
      assert.deepEqual(canonicalSnapshot(next.readJobInput('r1-0')), first);
      assert.equal(next.readMemberInput('r1', 'm1').input.parameters.seed, 123456);
      assert.equal(next.readMemberContext('r1', 'm1').member.native.defaults.scale, scale);
    } finally {
      NATIVE_DEFAULTS.scale = scale;
      NATIVE_SAMPLERS.push(...sampler);
    }
  } finally {
    reopened.close();
  }
});
function canonicalSnapshot(request) {
  return {
    model: request.model,
    prompt: request.prompt,
    parameters: buildNovelAiParameters(request),
  };
}

test('Vibe、Precise 和 V5 图生图保留原槽位；放大与编码恢复后仍可按原参数核价', async (t) => {
  const { sqlite, store, persist } = fixture(t),
    encoding = Buffer.alloc(200, 0x80).toString('base64');
  const vibes = [
    normalized(
      {
        n_samples: 1,
        reference_image_multiple: [image64],
        reference_strength_multiple: [0.6],
        reference_information_extracted_multiple: [1],
      },
      { model: 'nai-diffusion-3' },
    ),
    normalized({
      n_samples: 1,
      reference_image_multiple_cached: [{ cache_secret_key: 'must-not-store', data: encoding }],
      reference_strength_multiple: [0.6],
    }),
    normalized(
      { n_samples: 1, image: image64, strength: 0.7 },
      { action: 'img2img', model: 'nai-diffusion-5-curated' },
    ),
    normalized({
      n_samples: 1,
      director_reference_images_cached: [
        { cache_secret_key: 'must-not-store', data: png(1024, 1536).toString('base64') },
      ],
      director_reference_descriptions: [
        { caption: { base_caption: 'character', char_captions: [] }, legacy_uc: false },
      ],
    }),
  ];
  for (const [index, n] of vibes.entries()) {
    const id = `vibe${index}`,
      input = accepted(n, id);
    persist(input, metadata(id));
    const recovered = store.readJobInput(`${id}-0`);
    assert.deepEqual(
      buildNovelAiParameters(recovered),
      buildNovelAiParameters(singleGenerationRequest(input.request)),
    );
    assert.deepEqual(
      quoteNativeRequest(recovered),
      quoteNativeRequest(singleGenerationRequest(input.request)),
    );
    const descriptor = store.readMemberInput(id, 'm1').assets[0];
    assert.equal(descriptor.mimeType, index === 1 ? 'application/octet-stream' : 'image/png');
    assert.ok(
      !sqlite
        .prepare('SELECT body FROM generation_inputs WHERE id=?')
        .get(input.inputId)
        .body.includes('must-not-store'),
    );
  }
  for (const operation of ['upscale', 'encode-vibe']) {
    const body = {
      operation,
      image: image64,
      ...(operation === 'encode-vibe'
        ? { model: 'nai-diffusion-4-5-full', information_extracted: 0.5, focus_seed: -7 }
        : {}),
    };
    const n = normalizeMemberGeneration(body),
      input = accepted(n, operation);
    persist(input, metadata(operation));
    const loaded = store.readJobInput(`${operation}-0`);
    assert.deepEqual(loaded.operationParameters, input.request.operationParameters);
    assert.deepEqual(quoteNativeRequest(loaded), quoteNativeRequest(input.request));
    assert.equal(store.readMemberInput(operation, 'm1').recoverable, true);
  }
  const form = new FormData();
  form.set('request', JSON.stringify({ operation: 'upscale', image: 'image' }));
  form.set('image', new Blob([image], { type: 'image/png' }), 'synthetic.png');
  const wire = new Request('http://localhost/', { method: 'POST', body: form });
  const req = Readable.from([Buffer.from(await wire.arrayBuffer())]);
  req.headers = { 'content-type': wire.headers.get('content-type') };
  const fromForm = normalizeMemberGeneration(await readNativeBody(req));
  const json = normalizeMemberGeneration({ operation: 'upscale', image: image64 });
  assert.equal(fromForm.requestFingerprint, json.requestFingerprint);
  const prepared = prepareRelationalInput(materializeMemberGeneration(fromForm), {
    kind: 'member',
    member: fromForm.normalizationContext,
  });
  assert.deepEqual(prepared.assets[0].bytes, image);
  assert.equal(
    prepared.sha256,
    prepareRelationalInput(materializeMemberGeneration(json), {
      kind: 'member',
      member: json.normalizationContext,
    }).sha256,
  );
});

test('首次定义和输入引用严格封闭；不能偷带 settings、跨 input 引用或无用途附件', (t) => {
  for (const field of ['seed', 'n_samples', 'image_cache_secret_key']) {
    assert.throws(() => validateRelationalInputBody({ parameters: { [field]: 1 } }, false));
  }
  const input = accepted(
    normalized({ image: image64, strength: 0.7, n_samples: 1 }, { action: 'img2img' }),
  );
  assert.throws(() =>
    prepareRelationalInput(input.request, { ...input.context, settings: { pst: 'private' } }),
  );
  const context = structuredClone(input.context);
  context.member.native.extra = 'private';
  assert.throws(() => prepareRelationalInput(input.request, context));
  const prepared = prepareRelationalInput(input.request, input.context),
    body = structuredClone(prepared.body);
  body.parameters.image.assetId = 'foreign-asset';
  assert.throws(() => relationalAssetSlots(body, prepared.assets));
  assert.throws(() =>
    relationalAssetSlots(prepared.body, [
      ...prepared.assets,
      { ...prepared.assets[0], assetId: 'unreferenced' },
    ]),
  );
  const same = prepared.body;
  assert.ok(Object.hasOwn(same.parameters.image, 'assetId'));
  const { sqlite, store, persist } = fixture(t);
  persist(input);
  assert.throws(
    () => store.readMemberAsset('r1', 'm1', 'a1'),
    (error) => error.statusCode === 404,
  );
  assert.throws(
    () => store.readMemberAsset('r1', 'm1', 'parameters.image.foo'),
    (error) => error.statusCode === 404,
  );
  sqlite.prepare("DELETE FROM generation_jobs WHERE request_id='r1'").run();
  assert.equal(store.readMemberInput('r1', 'm1').input.parameters.seed, 4294967295);
  assert.throws(
    () => store.readJobInput('r1-0'),
    (error) => error.statusCode === 410,
  );
});

test('输入提交可从只读源保留原事件和 BLOB 重放，整提交断点、重开续跑与重复零写入', async (t) => {
  const dir = await mkdtemp(path.join(tmpdir(), 'input-replay-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const sourcePath = path.join(dir, 'source.sqlite'),
    targetPath = path.join(dir, 'target.sqlite');
  const source = new Database(sourcePath);
  initializeRelationalStorage(source, 'input-history');
  seed(source);
  const sourceStore = new RelationalInputStore(source);
  for (const id of ['first', 'second']) {
    const input = accepted(
      normalized({ n_samples: 1, image: image64, strength: 0.7 }, { action: 'img2img' }),
      id,
    );
    source.transaction(() => sourceStore.persistAcceptedInput(metadata(id), input)).immediate();
  }
  const rows = source.prepare('SELECT * FROM generation_inputs ORDER BY id').all();
  source.close();
  const readonly = new Database(sourcePath, { readonly: true });
  readonly.pragma('foreign_keys=ON');
  readonly.pragma('query_only=ON');
  let target = new Database(targetPath);
  initializeRelationalStorage(target, 'input-history');
  try {
    const sourceBefore = readonly.serialize(),
      baseline = readRelationalCommit(readonly, '1');
    target
      .transaction(() => {
        const result = replayRelationalCommit(target, baseline);
        for (const event of result.events) {
          const p = event.payload;
          target
            .prepare(
              'INSERT INTO members(id,display_name,status,accepting_jobs,auth_epoch,admission_epoch,created_at,source_event_id) VALUES(?,?,?,1,?,0,?,?)',
            )
            .run(p.memberId, p.displayName, p.status, 'auth1', p.createdAt, event.eventId);
        }
      })
      .immediate();
    const from = new RelationalInputStore(readonly),
      into = new RelationalInputStore(target);
    assert.equal(
      target.transaction(() => into.replayAcceptedInput(from, '2')).immediate().replayed,
      false,
    );
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM generation_requests').get().n, 1);
    target.close();
    target = new Database(targetPath);
    target.pragma('foreign_keys=ON');
    const resumed = new RelationalInputStore(target);
    assert.equal(
      target.transaction(() => resumed.replayAcceptedInput(from, '2')).immediate().replayed,
      true,
    );
    assert.equal(
      target.transaction(() => resumed.replayAcceptedInput(from, '3')).immediate().replayed,
      false,
    );
    const before = target.serialize();
    assert.equal(
      target.transaction(() => resumed.replayAcceptedInput(from, '3')).immediate().replayed,
      true,
    );
    assert.deepEqual(target.serialize(), before);
    assert.deepEqual(target.prepare('SELECT * FROM generation_inputs ORDER BY id').all(), rows);
    for (const table of [
      'commits',
      'domain_events',
      'stream_heads',
      'generation_requests',
      'generation_jobs',
      'normalization_contexts',
      'input_assets',
    ]) {
      assert.deepEqual(
        target.prepare(`SELECT * FROM ${table} ORDER BY 1`).all(),
        readonly.prepare(`SELECT * FROM ${table} ORDER BY 1`).all(),
        table,
      );
    }
    assert.deepEqual(resumed.readMemberAsset('first', 'm1', 'parameters.image').bytes, image);
    assert.deepEqual(
      canonicalSnapshot(resumed.readJobInput('second-0')),
      canonicalSnapshot(from.readJobInput('second-0')),
    );
    assert.deepEqual(readonly.serialize(), sourceBefore);
    assert.equal(target.prepare('SELECT status FROM projection_state').get().status, 'building');
    assert.deepEqual(target.pragma('foreign_key_check'), []);
  } finally {
    target.close();
    readonly.close();
  }
});

test('direct/OpenAI 的实际正文、独立展示词和首次 defaults 只按各自语义保存', (t) => {
  const { sqlite, persist, store } = fixture(t);
  const settings = {
    defaultArtist: 'artist prefix',
    defaultNegative: null,
    defaults: { steps: '30', scale: 5, cfg: 0 },
  };
  const request = normalizeNovelAiRequest(
    { tag: 'subject', width: 128, height: 128, seed: 7 },
    settings,
    { maxSteps: 28 },
  );
  request.allowPurchasedAnlas = true;
  const input = {
    ...accepted(normalized({ n_samples: 1 })),
    request,
    source: 'openai',
    seedMode: 'fixed',
    context: {
      kind: 'direct',
      native: nativeNormalizationContext(),
      defaults: requestNormalizationDefaults(settings),
    },
  };
  persist(input);
  const body = JSON.parse(sqlite.prepare('SELECT body FROM generation_inputs').get().body);
  assert.equal(body.input, 'artist prefix\nsubject');
  assert.equal(body.displayPrompt, 'subject');
  assert.equal(body.requestedSteps, 30);
  assert.equal(body.parameters.steps, 28);
  assert.equal(body.parameters.negative_prompt, '');
  assert.equal(Object.hasOwn(body, 'artist'), false);
  assert.equal(Object.hasOwn(body, 'negative'), false);
  const loaded = store.readJobInput('r1-0');
  assert.equal(loaded.tag, 'subject');
  assert.equal(loaded.requestedSteps, 30);
  assert.equal(loaded.allowPurchasedAnlas, true);
  assert.deepEqual(buildNovelAiParameters(loaded), buildNovelAiParameters(request));
  assert.deepEqual(quoteNativeRequest(loaded), quoteNativeRequest(request));
  assert.equal(store.readMemberContext('r1', 'm1').defaults.defaultNegative, null);
});

for (const [label, extra] of [
  ['scale=11', { scale: 11 }],
  ['sampler=k_dpmpp_3m_sde', { sampler: 'k_dpmpp_3m_sde' }],
]) {
  test(`direct/OpenAI 的 ${label} 保存重开后出站与计价不变，Native 仍按原契约拒绝`, async (t) => {
    assert.throws(() => normalized({ n_samples: 1, ...extra }), { statusCode: 422 });
    const directory = await mkdtemp(path.join(tmpdir(), 'relational-direct-'));
    t.after(() => rm(directory, { recursive: true, force: true }));
    const filename = path.join(directory, 'library.sqlite');
    const settings = { defaults: { steps: 23, scale: 5, cfg: 0 } };
    const request = normalizeNovelAiRequest(
      { tag: 'direct input', width: 128, height: 128, seed: 7, ...extra },
      settings,
      { maxSteps: 28 },
    );
    const expected = buildNovelAiParameters(request),
      quote = quoteNativeRequest(request);
    const sqlite = new Database(filename);
    try {
      initializeRelationalStorage(sqlite, 'direct-history');
      seed(sqlite);
      const store = new RelationalInputStore(sqlite);
      for (const source of ['direct', 'openai']) {
        const input = {
          ...accepted(normalized({ n_samples: 1 }), source),
          source,
          request,
          context: {
            kind: 'direct',
            native: nativeNormalizationContext(),
            defaults: requestNormalizationDefaults(settings),
          },
        };
        sqlite.transaction(() => store.persistAcceptedInput(metadata(source), input)).immediate();
      }
    } finally {
      sqlite.close();
    }
    const reopened = new Database(filename);
    reopened.pragma('foreign_keys=ON');
    try {
      const store = new RelationalInputStore(reopened);
      for (const source of ['direct', 'openai']) {
        const loaded = store.readJobInput(`${source}-0`);
        assert.deepEqual(buildNovelAiParameters(loaded), expected);
        assert.deepEqual(quoteNativeRequest(loaded), quote);
      }
    } finally {
      reopened.close();
    }
  });
}

test('partial 输入可复制已知字段和已校验附件，缺首次上下文仍不能执行或补默认', (t) => {
  const { sqlite, store } = fixture(t),
    body = {
      operation: 'generate',
      model: 'historical',
      input: '',
      parameters: {
        negative_prompt: null,
        image: { assetId: 'image' },
        mask: { assetId: 'wrong-mime' },
        reference_image_multiple: [{ assetId: 'opaque' }],
        director_reference_images: [{ assetId: 'unsafe-mime' }],
      },
    };
  const assets = [
    { assetId: 'image', mimeType: 'image/png', bytes: image },
    { assetId: 'wrong-mime', mimeType: 'image/jpeg', bytes: image },
    {
      assetId: 'opaque',
      mimeType: 'application/octet-stream',
      bytes: Buffer.from('saved opaque bytes, without a proven Native definition'),
    },
    { assetId: 'unsafe-mime', mimeType: 'text/html', bytes: image },
  ].map((asset) => ({
    ...asset,
    byteLength: asset.bytes.length,
    sha256: createHash('sha256').update(asset.bytes).digest('hex'),
  }));
  const logicalBytes = Buffer.byteLength(JSON.stringify(body)),
    sha = relationalInputDigest(
      body,
      assets.map(({ bytes: _bytes, ...asset }) => asset),
      logicalBytes,
    );
  const origin = { sourceId: 'legacy', originalId: 'old-input' };
  const common = {
    occurredAt: 500,
    memberId: 'm1',
    accountId: null,
    policyVersion: 'historical',
    pricingVersion: 'historical',
  };
  const events = [
    {
      ...common,
      eventType: 'OpeningInputRecorded',
      streamId: 'input:partial',
      payload: {
        inputId: 'partial',
        memberId: 'm1',
        inputSchemaVersion: 1,
        completeness: 'partial',
        inputSha256: sha,
        logicalBytes,
        origin,
      },
    },
    {
      ...common,
      eventType: 'OpeningRequestRecorded',
      streamId: 'request:past',
      payload: {
        requestId: 'past',
        memberId: 'm1',
        inputId: 'partial',
        contextId: null,
        source: 'historical',
        operation: 'generate',
        model: body.model,
        sampleCount: 1,
        seedMode: 'unknown',
        confirmation: null,
        acceptedAt: 500,
        retiredAt: null,
        origin,
      },
    },
    {
      ...common,
      eventType: 'OpeningJobRecorded',
      streamId: 'request:past',
      payload: {
        jobId: 'past-job',
        requestId: 'past',
        createdAt: 500,
        sampleIndex: 0,
        actualSeed: null,
        status: 'failed',
        queueSeq: null,
        queueExpiresAt: null,
        deadlineAt: null,
        admissionEpoch: 0,
        artifactId: null,
        cacheKey: null,
        completedAt: 500,
        errorCode: 'generation_failed',
        errorStatus: 500,
        cancellationReason: null,
        lateResultAt: null,
        freeSucceededAt: null,
        freeWarningThreshold: null,
        retiredAt: null,
        origin,
      },
    },
  ];
  sqlite
    .transaction(() => {
      const result = appendRelationalCommit(sqlite, {
        ...metadata('partial'),
        actorKind: 'system',
        actorId: null,
        expectedVersions: { 'input:partial': '0', 'request:past': '0' },
        receipt: null,
        events,
      });
      sqlite
        .prepare(
          "INSERT INTO generation_inputs(id,member_id,schema_version,completeness,sha256,logical_bytes,body) VALUES('partial','m1',1,'partial',?,?,?)",
        )
        .run(sha, logicalBytes, JSON.stringify(body));
      for (const asset of assets) {
        sqlite
          .prepare(
            "INSERT INTO input_assets(input_id,asset_id,mime_type,byte_length,sha256,bytes) VALUES('partial',?,?,?,?,?)",
          )
          .run(asset.assetId, asset.mimeType, asset.byteLength, asset.sha256, asset.bytes);
      }
      sqlite
        .prepare(
          "INSERT INTO generation_requests(id,member_id,input_id,context_id,source,operation,model,sample_count,seed_mode,accepted_at,source_event_id) VALUES('past','m1','partial',NULL,'historical','generate','historical',1,'unknown',500,?)",
        )
        .run(result.events[1].eventId);
      sqlite
        .prepare(
          "INSERT INTO generation_jobs(id,request_id,sample_index,status,admission_epoch,source_event_id) VALUES('past-job','past',0,'failed',0,?)",
        )
        .run(result.events[2].eventId);
    })
    .immediate();
  const before = sqlite.serialize();
  const recovered = store.readMemberInput('past', 'm1');
  assert.equal(recovered.recoverable, false);
  assert.equal(recovered.input.input, '');
  assert.equal(recovered.input.parameters.negative_prompt, null);
  assert.equal(Object.hasOwn(recovered.input.parameters, 'seed'), false);
  assert.equal(Object.hasOwn(recovered.input.parameters, 'n_samples'), false);
  assert.equal(Object.hasOwn(recovered.input, 'action'), false);
  assert.equal(recovered.assets.length, 4);
  assert.equal(
    recovered.assets.find((asset) => asset.slot === 'parameters.image').url,
    '/api/member/requests/past/inputs/parameters.image',
  );
  assert.deepEqual(store.readMemberAsset('past', 'm1', 'parameters.image'), {
    mimeType: 'image/png',
    bytes: image,
  });
  assert.deepEqual(
    store.readMemberAsset('past', 'm1', 'parameters.reference_image_multiple.0').bytes,
    assets[2].bytes,
  );
  for (const slot of ['parameters.mask', 'parameters.director_reference_images.0']) {
    assert.throws(
      () => store.readMemberAsset('past', 'm1', slot),
      (error) => error.statusCode === 410 && error.code === 'input_unavailable',
    );
  }
  for (const [memberId, slot] of [
    ['m2', 'parameters.image'],
    ['m1', 'parameters.image/../../private'],
    ['m1', 'parameters.reference_image_multiple.1'],
  ]) {
    assert.throws(
      () => store.readMemberAsset('past', memberId, slot),
      (error) => error.statusCode === 404,
    );
  }
  assert.throws(
    () => store.readJobInput('past-job'),
    (error) => error.statusCode === 410,
  );
  assert.throws(
    () => store.readMemberContext('past', 'm1'),
    (error) => error.statusCode === 410,
  );
  assert.ok(sqlite.serialize().equals(before), '所有读取和拒绝都不得改变来源或投影');
  mutate(sqlite, 'input_assets', () => {
    const corrupt = Buffer.from(image);
    corrupt[0] ^= 1;
    sqlite
      .prepare("UPDATE input_assets SET bytes=? WHERE input_id='partial' AND asset_id='image'")
      .run(corrupt);
  });
  assert.throws(
    () => store.readMemberAsset('past', 'm1', 'parameters.image'),
    (error) => error.statusCode === 410 && error.code === 'input_unavailable',
  );
  sqlite.prepare("UPDATE members SET auth_epoch=NULL WHERE id='m1'").run();
  assert.throws(
    () => store.readMemberAsset('past', 'm1', 'parameters.reference_image_multiple.0'),
    (error) => error.statusCode === 401,
  );
});

test('数据库冷读取可直接进入现有上游客户端，实际 HTTP 仅发送原协议参数和附件字节', async (t) => {
  const { store, persist } = fixture(t),
    received = [],
    encodedResult = Buffer.alloc(128, 0x80);
  const server = createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) {
      chunks.push(chunk);
    }
    received.push({ path: req.url, body: JSON.parse(Buffer.concat(chunks).toString('utf8')) });
    res.writeHead(200, {
      'content-type': req.url.endsWith('encode-vibe') ? 'application/octet-stream' : 'image/png',
    });
    res.end(req.url.endsWith('encode-vibe') ? encodedResult : image);
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const env = {
    NOVELAI_API_URL: `http://127.0.0.1:${server.address().port}`,
    NOVELAI_STREAM_IMAGE: 'false',
  };
  const cases = [
    normalized({ n_samples: 1, image: image64, strength: 0.7 }, { action: 'img2img' }),
    normalizeMemberGeneration({ operation: 'upscale', image: image64 }),
    normalizeMemberGeneration({
      operation: 'encode-vibe',
      image: image64,
      model: 'nai-diffusion-4-5-full',
      information_extracted: 0.5,
    }),
  ];
  for (const [index, n] of cases.entries()) {
    const id = `http${index}`,
      input = accepted(n, id);
    persist(input, metadata(id));
    const result = await generateNovelAiImage(
      store.readJobInput(`${id}-0`),
      { token: 'mock-only' },
      env,
    );
    assert.deepEqual(result.buffer, index === 2 ? encodedResult : image);
    const wire = received[index].body;
    if (index === 0) {
      assert.equal(received[index].path, '/ai/generate-image');
      assert.equal(wire.input, 'exact input');
      assert.deepEqual(
        wire.parameters,
        buildNovelAiParameters(singleGenerationRequest(input.request)),
      );
    } else {
      assert.equal(received[index].path, `/ai/${n.operation}`);
      assert.deepEqual(wire, input.request.operationParameters);
    }
    for (const field of [
      'normalizationContext',
      'contextId',
      'confirmation',
      'requestId',
      'memberId',
      'allowPurchasedAnlas',
    ]) {
      assert.equal(Object.hasOwn(wire, field), false);
    }
  }
  assert.equal(received.length, 3);
});
