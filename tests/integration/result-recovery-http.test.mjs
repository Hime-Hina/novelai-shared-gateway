import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { readFile, writeFile, mkdir, rename } from 'node:fs/promises';
import path from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import Database from 'better-sqlite3';
import { nativeImagesMsgpack } from '../../server/protocols/native-stream.ts';
import {
  childFixture,
  launchGateway,
  enableAccount,
  query,
  assertChildPublicReplay,
} from '../helpers/relational-child.mjs';
import { RelationalGenerationStore } from '../../server/storage/relational-generation-store.ts';
import { databaseState } from '../helpers/relational-generation.mjs';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { relationalFactStream } from '../../server/events/relational-fact-contracts.ts';
import { domainDigest } from '../../server/events/domain-events.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import {
  restoreRelationalPrivateSnapshot,
  relationalSnapshotSha256,
  readRelationalRecoveryState,
  relationalPrivateStateEmpty,
  sameRelationalPrivateState,
} from '../../server/storage/relational-private-restore.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import {
  assertReadableStoredResultDescriptor,
  readStoredResultContent,
} from '../../server/storage/result-files.ts';

const png = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=',
  'base64',
);
const vibe = Buffer.from(Array.from({ length: 256 }, (_, i) => i));
const imageRequest = {
  input: 'synthetic result recovery',
  model: 'nai-diffusion-5-full',
  action: 'generate',
  parameters: { width: 1024, height: 1024, steps: 23, n_samples: 1, seed: 7 },
};
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(predicate, label, timeout = 15000) {
  const end = Date.now() + timeout;
  while (!(await predicate())) {
    if (Date.now() >= end) throw new Error(`synthetic recovery timeout: ${label}`);
    await delay(20);
  }
}
async function listen(server) {
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server.address().port;
}

// 只由本夹具的隔离child加载；所有状态仍由真实owner写，故障点不进入生产入口。
async function installRecoveryFaults(generationUrl) {
  const stage = process.env.RESULT_RECOVERY_TEST_STAGE,
    pointFile = process.env.RESULT_RECOVERY_TEST_POINT,
    fs = (await import('node:fs/promises')).default,
    syncFs = await import('node:fs'),
    path = (await import('node:path')).default,
    assert = (await import('node:assert/strict')).default,
    { registerHooks, syncBuiltinESMExports } = await import('node:module'),
    { EventEmitter } = await import('node:events');
  registerHooks({
    load(url, context, next) {
      if (/\/server\/storage\/(?:sqlite-store|sqlite-migrations)\.(?:ts|js)(?:[?#]|$)/.test(url))
        throw new Error('result recovery child cannot load old SQLite owners');
      return next(url, context);
    },
  });
  const { RelationalGenerationStore } = await import(generationUrl),
    imageDir = path.resolve(process.env.DATA_DIR, 'images');
  let prepared,
    paused = false;
  const prepare = RelationalGenerationStore.prototype.prepareGenerationResult;
  RelationalGenerationStore.prototype.prepareGenerationResult = function (...args) {
    const result = prepare.apply(this, args);
    if (!prepared)
      prepared = {
        sqlite: this.sqlite,
        jobId: args[1],
        descriptor: args[2],
        commitSeq: result.commitSeq,
      };
    return result;
  };
  const details = (point) => {
    assert.ok(prepared, 'fault must follow the actual prepared command');
    assert.equal(prepared.sqlite.readonly, false);
    assert.equal(prepared.sqlite.inTransaction, false, 'fault must be outside actual COMMIT');
    const row = prepared.sqlite
      .prepare(
        `SELECT j.status,j.artifact_id,a.id AS attempt_id,
      a.prepared_artifact_id,a.prepared_file,a.prepared_sha256,c.status AS charge_status
      FROM generation_jobs j JOIN generation_attempts a ON a.job_id=j.id
      JOIN charges c ON c.job_id=j.id WHERE j.id=?`,
      )
      .get(prepared.jobId);
    assert.equal(row.prepared_artifact_id, prepared.descriptor.imageId);
    assert.equal(row.prepared_file, prepared.descriptor.file);
    assert.equal(row.prepared_sha256, prepared.descriptor.sha256);
    const facts = prepared.sqlite
      .prepare(
        "SELECT event_id,CAST(commit_seq AS TEXT) AS seq FROM domain_events WHERE event_type='ArtifactPrepared' AND json_extract(payload,'$.attemptId')=?",
      )
      .all(row.attempt_id);
    assert.equal(facts.length, 1);
    assert.equal(facts[0].seq, prepared.commitSeq);
    if (point === 'final_sql') {
      assert.equal(row.status, 'done');
      assert.equal(row.artifact_id, prepared.descriptor.imageId);
      assert.equal(row.charge_status, 'settled');
    }
    return {
      point,
      jobId: prepared.jobId,
      attemptId: row.attempt_id,
      artifactId: row.prepared_artifact_id,
      preparationEventId: facts[0].event_id,
      preparationCommitSeq: facts[0].seq,
      writerInTransaction: prepared.sqlite.inTransaction,
      descriptor: prepared.descriptor,
      ...(prepared.recoveryStack ? { recoveryStack: prepared.recoveryStack } : {}),
      sqliteSynchronous: prepared.sqlite.pragma('synchronous', { simple: true }),
    };
  };
  const mark = (point) => {
    if (paused || stage !== point) return false;
    const value = details(point);
    paused = true;
    syncFs.writeFileSync(pointFile + '.tmp', JSON.stringify(value), { mode: 0o600, flag: 'wx' });
    syncFs.renameSync(pointFile + '.tmp', pointFile);
    return true;
  };
  const pause = async (point) => {
    if (mark(point)) await new Promise(() => {});
  };
  const readPreparation = RelationalGenerationStore.prototype.readGenerationResultPreparation;
  RelationalGenerationStore.prototype.readGenerationResultPreparation = function (...args) {
    const result = readPreparation.apply(this, args);
    if (stage === 'recovery_checked') {
      const recoveryStack = new Error().stack;
      if (!/[\\/]storage[\\/]relational-result-recovery\.(?:ts|js)/.test(recoveryStack))
        return result;
      prepared = {
        sqlite: this.sqlite,
        jobId: args[0],
        descriptor: result.descriptor,
        commitSeq: result.event.commitSeq,
        recoveryStack,
      };
      mark('recovery_checked');
    }
    return result;
  };
  const insideImages = (filename) =>
    typeof filename === 'string' && path.dirname(path.resolve(filename)) === imageDir;
  const open = fs.open;
  fs.open = async function (filename, ...args) {
    if (insideImages(filename) && filename.endsWith('.part')) await pause('descriptor');
    const handle = await open.call(this, filename, ...args);
    if (insideImages(filename) && filename.endsWith('.part')) {
      const write = handle.writeFile.bind(handle),
        sync = handle.sync.bind(handle);
      handle.writeFile = async (data, ...options) => {
        if (stage === 'half_write' && !paused) {
          const bytes = Buffer.from(data);
          await write(bytes.subarray(0, Math.max(1, Math.floor(bytes.length / 2))), ...options);
          await pause('half_write');
        }
        return write(data, ...options);
      };
      handle.sync = async () => {
        await sync();
        await pause('file_sync');
      };
    } else if (typeof filename === 'string' && path.resolve(filename) === imageDir) {
      const sync = handle.sync.bind(handle);
      handle.sync = async () => {
        await sync();
        await pause('dir_sync');
      };
    }
    return handle;
  };
  const rename = fs.rename;
  fs.rename = async function (from, to) {
    await rename.call(this, from, to);
    if (insideImages(from) && from.endsWith('.part')) await pause('rename');
  };
  // safe通知直接取rawListeners；同步gate位于完整COMMIT后、任何领取唤醒前。
  const listeners = EventEmitter.prototype.rawListeners;
  EventEmitter.prototype.rawListeners = function (name) {
    if (
      name === 'commit' &&
      stage === 'final_sql' &&
      prepared &&
      !prepared.sqlite.inTransaction &&
      prepared.sqlite
        .prepare('SELECT artifact_id FROM generation_jobs WHERE id=?')
        .get(prepared.jobId)?.artifact_id === prepared.descriptor.imageId &&
      mark('final_sql')
    )
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
    return listeners.call(this, name);
  };
  syncBuiltinESMExports();
}
const faultImport = `data:text/javascript,${encodeURIComponent(`await (${installRecoveryFaults.toString()})(${JSON.stringify(new URL('../../server/storage/relational-generation-store.ts', import.meta.url).href)});`)}`;

const durableState = (f, job) => ({
  job,
  pending: f.pending().includes(job.id),
  charge: f.settlement(job.id),
  artifacts: f.artifacts(),
  requestFacts: f.query(
    'SELECT event_type,payload FROM domain_events WHERE stream_id=? ORDER BY stream_version',
    'request:' + job.request_id,
  ),
});
function assertOriginalCharge(actual, original) {
  const terms = ({ status: _status, terminal_at: _at, terminal_event_id: _event, ...rest }) => rest;
  assert.deepEqual(
    terms(actual),
    terms(original),
    '结算只结束原预扣，不能改变原资源/金额/归属/依据',
  );
}
async function fixture(
  t,
  {
    stage,
    operation = 'generate',
    hold = false,
    grace = 30000,
    lateRefund = false,
    enableOnStart = true,
  } = {},
) {
  const cleanup = [],
    childFixtureState = await childFixture({ after: (fn) => cleanup.push(fn) }, 'result-recovery'),
    dataDir = childFixtureState.dataDir,
    members = childFixtureState.members;
  let generations = 0,
    release,
    server,
    base,
    pointFile;
  const held = hold ? new Promise((resolve) => (release = resolve)) : Promise.resolve();
  const upstream = http.createServer(async (req, res) => {
    if (req.url === '/user/data') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(
        JSON.stringify({
          subscription: {
            tier: 3,
            active: true,
            expiresAt: Math.floor(Date.now() / 1000) + 86400,
            trainingStepsLeft: { fixedTrainingStepsLeft: 10000, purchasedTrainingSteps: 0 },
            usage: { percent: 100, isNegative: false, timeUntilNextPercent: 0 },
          },
        }),
      );
      return;
    }
    for await (const _chunk of req) {
      /* 不记录认证或正文。 */
    }
    generations++;
    await held;
    const stream = req.url.endsWith('-stream');
    res.writeHead(200, {
      'content-type':
        operation === 'encode-vibe' || stream ? 'application/octet-stream' : 'image/png',
    });
    res.end(operation === 'encode-vibe' ? vibe : stream ? nativeImagesMsgpack([png]) : png);
  });
  const upstreamPort = await listen(upstream);
  let enabled = false;
  const env = {
    PUBLIC_MODE: 'true',
    SHUTDOWN_GRACE_MS: String(grace),
    ...(lateRefund ? { NOVELAI_GENERATE_TIMEOUT_MS: '1000' } : {}),
  };
  t.after(async () => {
    release?.();
    for (const child of childFixtureState.children) await child.kill();
    upstream.closeAllConnections();
    await new Promise((resolve) => upstream.close(resolve));
    for (const close of cleanup.reverse()) await close();
  });
  const q = (sql, ...args) => query(childFixtureState, sql, ...args);
  const jobs = () =>
    q(`SELECT j.*,r.member_id,r.operation,a.id AS attempt_id,a.credential_epoch,
    a.upstream_started_at,a.upstream_completed_at,a.prepared_artifact_id,a.prepared_file,
    a.prepared_mime_type,a.prepared_byte_length,a.prepared_sha256,a.review_status,
    c.status AS charge_status FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id
    LEFT JOIN generation_attempts a ON a.job_id=j.id LEFT JOIN charges c ON c.job_id=j.id
    ORDER BY j.rowid`);
  const readOwner = (read) => {
    const sqlite = new Database(childFixtureState.filename, { readonly: true });
    try {
      return read(sqlite);
    } finally {
      sqlite.close();
    }
  };
  async function start(faultStage = '') {
    pointFile = path.join(dataDir, 'result-recovery-point-' + randomUUID() + '.json');
    server = await launchGateway(childFixtureState, `http://127.0.0.1:${upstreamPort}`, {
      ...env,
      ...(faultStage
        ? {
            NODE_OPTIONS: '--import=' + faultImport,
            RESULT_RECOVERY_TEST_STAGE: faultStage,
            RESULT_RECOVERY_TEST_POINT: pointFile,
          }
        : {}),
    });
    base = server.origin;
    if (enableOnStart && !enabled) {
      await enableAccount(childFixtureState, server);
      enabled = true;
    }
  }
  async function rejectStart() {
    await assert.rejects(
      start('recovery_checked'),
      /actual child startup failed: \[runtime\] GENERATION_RUNTIME_FAILED/,
    );
    server = childFixtureState.children.at(-1);
    await until(() => server.exit, 'unresolved prepared child exits');
    assert.equal(server.origin, null, '未解决的原准备不得开始监听');
    assert.notEqual(server.exit.code, 0);
    const point = JSON.parse(await readFile(pointFile, 'utf8'));
    assert.equal(
      point.point,
      'recovery_checked',
      '必须实际经过 canonical prepared 核对，不以加载/格式/ready前拒代替恢复故障',
    );
    assert.match(point.recoveryStack, /[\\/]storage[\\/]relational-result-recovery\.(?:ts|js)/);
    t.diagnostic(JSON.stringify(point));
  }
  const stop = async (signal = 'SIGKILL') => (signal === 'SIGKILL' ? server.kill() : server.stop());
  const fetchLocal = (route, options = {}) =>
    fetch(new URL(route, base), { ...options, signal: AbortSignal.timeout(15000) });
  const headers = {
    authorization: `Bearer ${members[0].api_key}`,
    'content-type': 'application/json',
  };
  const generate = (parameters = {}) =>
    fetchLocal(`/ai/${operation === 'encode-vibe' ? 'encode-vibe' : 'generate-image'}`, {
      method: 'POST',
      headers,
      body: JSON.stringify(
        operation === 'encode-vibe'
          ? { image: png.toString('base64'), model: 'nai-diffusion-4-5-full' }
          : { ...imageRequest, parameters: { ...imageRequest.parameters, ...parameters } },
      ),
    }).then(
      async (response) => ({
        status: response.status,
        bytes: Buffer.from(await response.arrayBuffer()),
      }),
      () => null,
    );
  const waitPoint = async (point) => {
    let value;
    await until(async () => {
      if (server.exit) throw new Error('fault child exited before the actual point');
      try {
        value = JSON.parse(await readFile(pointFile, 'utf8'));
      } catch (error) {
        if (error.code !== 'ENOENT') throw error;
      }
      return value?.point === point;
    }, 'fault point ' + point);
    t.diagnostic(JSON.stringify(value));
    return value;
  };
  async function claim(job, expected) {
    const response = await fetchLocal('/api/jobs/' + job.id, { headers });
    assert.equal(response.status, 200);
    const view = await response.json();
    assert.ok(view.imageUrl, '持久结果必须可领取');
    const download = await fetchLocal(view.imageUrl);
    assert.equal(download.status, 200);
    assert.deepEqual(Buffer.from(await download.arrayBuffer()), expected);
    return view;
  }
  return {
    childFixture: childFixtureState,
    dataDir,
    members,
    query: q,
    readOwner,
    jobs,
    databaseState: () => readOwner(databaseState),
    artifacts: () => q('SELECT * FROM artifacts ORDER BY id'),
    settlement: (id) => q('SELECT * FROM charges WHERE original_job_id=?', id)[0],
    pending: () =>
      readOwner((sqlite) => new RelationalGenerationStore(sqlite).readPendingGenerationResultIds()),
    preparation: (id) =>
      readOwner((sqlite) =>
        new RelationalGenerationStore(sqlite).readGenerationResultPreparation(id),
      ),
    start,
    rejectStart,
    stop,
    generate,
    waitPoint,
    claim,
    fetchLocal,
    headers,
    stage,
    generations: () => generations,
    release: () => release?.(),
    exit: () => (server.exit ? Promise.resolve(server.exit) : once(server.child, 'exit')),
    child: () => server.child,
  };
}

async function installColdHistory(f, images, jobs) {
  const directory = f.childFixture.directory,
    bootstrapPath = path.join(directory, 'cold-bootstrap-source.sqlite'),
    importedPath = path.join(directory, 'cold-imported.sqlite'),
    selectedPath = path.join(directory, 'cold-selected-source.sqlite'),
    rebuiltPath = path.join(directory, 'cold-rebuilt.sqlite'),
    originalPath = path.join(directory, 'cold-original-rows.json'),
    inventoryPath = path.join(directory, 'cold-local-inventory.json'),
    sourceId = 'synthetic-parentless-results',
    memberId = f.members[0].user_id,
    handles = [],
    open = (filename, options) => {
      const db = new Database(filename, options);
      handles.push(db);
      return db;
    },
    hash = (bytes) => createHash('sha256').update(bytes).digest('hex'),
    seal = async (db, filename) => {
      await db.backup(filename);
      const sealed = open(filename);
      sealed.pragma('journal_mode=DELETE');
      sealed.close();
      return relationalSnapshotSha256(filename);
    },
    rebuild = (target, source) => {
      const replayer = new RelationalCommitReplayer(target),
        inputs = new RelationalInputStore(source);
      for (const { seq } of source
        .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
        .all())
        target.transaction(() =>
          replayer.replayCommit(readRelationalCommit(source, seq), inputs),
        )();
    };
  const actualFetch = globalThis.fetch;
  let offlineFetches = 0;
  globalThis.fetch = () => {
    offlineFetches++;
    throw new Error('offline cold history denied network before IO');
  };
  let originalSha256, inventorySha256, selectedSha256;
  try {
    const original = open(f.childFixture.filename, { readonly: true, fileMustExist: true });
    const key = new RelationalMemberStore(original).readMemberApiKey(memberId);
    assert.ok(
      key &&
        images.every((image) => image.token === key) &&
        jobs.every((job) => job.userToken === key),
      '原来源 Key 必须由选定私有来源精确映射到稳定成员，不打印秘密',
    );
    const bootstrapSha = await seal(original, bootstrapPath);
    original.close();
    const bootstrap = open(bootstrapPath, { readonly: true, fileMustExist: true }),
      cut = readRelationalRecoveryState(bootstrap),
      imported = open(importedPath);
    initializeRelationalStorage(imported, cut.history_id);
    rebuild(imported, bootstrap);
    assert.equal(relationalPrivateStateEmpty(imported), true);
    imported.transaction(() =>
      restoreRelationalPrivateSnapshot(imported, bootstrapPath, bootstrapSha),
    )();
    assert.equal(sameRelationalPrivateState(bootstrap, imported), true);

    // 原旧行及其成员映射属于受保护来源；公共事实只保存整份来源/清单摘要。
    const originalBytes = Buffer.from(JSON.stringify({ images, jobs }));
    originalSha256 = hash(originalBytes);
    await writeFile(originalPath, originalBytes, { flag: 'wx', mode: 0o600 });
    await mkdir(path.join(f.dataDir, 'images'), { recursive: true });
    const inventory = [];
    for (const image of images) {
      const bytes = image.mimeType === 'image/png' ? png : vibe,
        descriptor = {
          imageId: image.id,
          file: image.file,
          operation: null,
          mimeType: image.mimeType,
          byteLength: bytes.length,
          sha256: hash(bytes),
        };
      assertReadableStoredResultDescriptor(descriptor);
      await writeFile(path.join(f.dataDir, image.file), bytes, { flag: 'wx', mode: 0o600 });
      const actual = await readStoredResultContent(f.dataDir, descriptor);
      assert.ok(actual);
      assert.deepEqual(actual.buffer, bytes);
      // 本次真实文件核验的时点，不是原上传/发布/完成日。
      inventory.push({ descriptor, verifiedAt: Date.now() });
    }
    const inventoryBytes = Buffer.from(JSON.stringify({ sourceSha256: originalSha256, inventory }));
    inventorySha256 = hash(inventoryBytes);
    await writeFile(inventoryPath, inventoryBytes, { flag: 'wx', mode: 0o600 });
    const recordedAt = Date.now(),
      origin = (originalId) => ({ sourceId, originalId }),
      event = (eventType, payload) => {
        const kind = relationalFactStream(eventType),
          field = {
            import: 'sourceId',
            input: 'inputId',
            request: 'requestId',
            artifact: 'artifactId',
            storage: 'storageTargetId',
          }[kind];
        return {
          eventType,
          payload,
          streamId: `${kind}:${payload[field]}`,
          memberId: ['import', 'storage'].includes(kind) ? null : memberId,
          accountId: null,
          occurredAt: recordedAt,
          policyVersion: 'historical-unknown',
          pricingVersion: 'historical-unknown',
        };
      },
      inputs = new RelationalInputStore(imported),
      replayer = new RelationalCommitReplayer(imported),
      append = (id, events) =>
        replayer.appendCommit(
          {
            commandId: `cold-source:${id}`,
            fingerprint: domainDigest(events),
            actorKind: 'system',
            actorId: null,
            recordedAt,
            correlationId: null,
            causationId: null,
            expectedVersions: Object.fromEntries(
              events.map((e) => [
                e.streamId,
                imported
                  .prepare(
                    'SELECT CAST(stream_version AS TEXT) v FROM stream_heads WHERE stream_id=?',
                  )
                  .get(e.streamId)?.v ?? '0',
              ]),
            ),
            receipt: { importedOriginalRows: true },
            events,
          },
          inputs,
        ),
      target = new RelationalReplicaStore(imported, { dataDir: f.dataDir }).configuredTarget(
        'local',
      ).target;
    assert.equal(target.namespace, 'images');
    imported.transaction(() =>
      append('source', [
        event('ImportSourceRegistered', {
          sourceId,
          sourceSha256: originalSha256,
          sourceCommit: '5e629689b0ed41c2df94b52db1dcb3646ffa7314',
          cutoffSeq: '0',
          cutoffAt: recordedAt,
          manifestSha256: inventorySha256,
          route: 'B',
        }),
        event('OpeningStorageTargetRecorded', { ...target, origin: origin('images') }),
      ]),
    )();
    let finalCommit;
    for (const [index, job] of jobs.entries())
      imported.transaction(() => {
        const image = images[index],
          observed = inventory[index],
          inputId = `cold-input-${image.id}`,
          requestId = `cold-container-${image.id}`;
        assert.equal(job.imageId, image.id);
        finalCommit = append(job.id, [
          event('OpeningInputRecorded', {
            ...inputs.stageOpeningInput({
              inputId,
              memberId,
              completeness: 'unavailable',
              body: null,
              logicalBytes: 0,
              assets: [],
            }),
            origin: origin(job.id),
          }),
          event('OpeningRequestRecorded', {
            requestId,
            originalRequestId: null,
            memberId,
            inputId,
            contextId: null,
            source: null,
            operation: null,
            model: null,
            sampleCount: null,
            seedMode: 'unknown',
            confirmation: null,
            acceptedAt: null,
            retiredAt: null,
            origin: origin(job.id),
          }),
          event('OpeningJobRecorded', {
            diagnosticId: null,
            sourceOrder: null,
            displayCost: null,
            jobId: job.id,
            requestId,
            createdAt: Date.parse(job.createdAt),
            sampleIndex: null,
            actualSeed: null,
            status: job.status,
            queueSeq: null,
            queueExpiresAt: null,
            deadlineAt: null,
            admissionEpoch: null,
            artifactId: job.imageId,
            cacheKey: null,
            completedAt: null,
            errorCode: null,
            errorStatus: null,
            cancellationReason: null,
            lateResultAt: null,
            freeSucceededAt: null,
            freeWarningThreshold: null,
            retiredAt: null,
            origin: origin(job.id),
          }),
          event('OpeningArtifactRecorded', {
            artifactId: image.id,
            memberId,
            inputId,
            originalAttemptId: null,
            operation: null,
            mimeType: image.mimeType,
            byteLength: observed.descriptor.byteLength,
            sha256: observed.descriptor.sha256,
            publishedAt: null,
            retiredAt: null,
            origin: origin(image.id),
          }),
          event('OpeningReplicaRecorded', {
            artifactId: image.id,
            storageTargetId: target.storageTargetId,
            objectKey: path.posix.basename(image.file),
            encoding: 'identity',
            uploadedAt: null,
            verifiedAt: observed.verifiedAt,
            deletedAt: null,
            lastErrorCode: null,
            origin: origin(image.file),
          }),
        ]);
      })();
    for (const table of ['wallets', 'charges', 'operating_state'])
      assert.deepEqual(
        imported.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
        bootstrap.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
        '导入原终态不产生收费/退款/免费或钱包决定',
      );
    selectedSha256 = await seal(imported, selectedPath);
    const selected = open(selectedPath, { readonly: true, fileMustExist: true }),
      rebuilt = open(rebuiltPath);
    initializeRelationalStorage(rebuilt, cut.history_id);
    rebuild(rebuilt, selected);
    assert.equal(relationalPrivateStateEmpty(rebuilt), true);
    const changes = rebuilt.prepare('SELECT total_changes() n').get().n,
      before = databaseState(rebuilt);
    rebuilt.transaction(() =>
      new RelationalCommitReplayer(rebuilt).replayCommit(
        readRelationalCommit(selected, finalCommit.commitSeq),
        new RelationalInputStore(selected),
      ),
    )();
    assert.equal(databaseState(rebuilt), before);
    assert.equal(rebuilt.prepare('SELECT total_changes() n').get().n, changes);
    const installed = await verifyAndInstallRelationalStorage(rebuilt, {
      sourcePath: selectedPath,
      sourceSha256: selectedSha256,
      historyId: cut.history_id,
      finalCommitSeq: finalCommit.commitSeq,
      replicas: { dataDir: f.dataDir },
    });
    assert.equal(installed.artifactsVerified, 503);
    assertRelationalStorageReady(rebuilt);
    assert.equal(sameRelationalPrivateState(selected, rebuilt), true);
    assert.ok(new RelationalMemberStore(rebuilt).readMemberApiKey(memberId) === key);
    assert.equal(relationalSnapshotSha256(selectedPath), selectedSha256);
    rebuilt.pragma('journal_mode=DELETE');
  } finally {
    globalThis.fetch = actualFetch;
    for (const db of handles.reverse()) if (db.open) db.close();
  }
  assert.equal(offlineFetches, 0);
  assert.ok(handles.every((db) => !db.open));
  assert.ok(f.childFixture.children.every((child) => child.exit));
  const archived = path.join(directory, 'before-cold-restore.sqlite'),
    fixtureRoot = path.resolve(directory) + path.sep;
  for (const filename of [f.childFixture.filename, archived, rebuiltPath])
    assert.ok(path.resolve(filename).startsWith(fixtureRoot));
  await rename(f.childFixture.filename, archived);
  for (const suffix of ['-wal', '-shm', '-journal']) {
    try {
      await rename(f.childFixture.filename + suffix, archived + suffix);
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
  }
  await rename(rebuiltPath, f.childFixture.filename);
  return {
    originalPath,
    originalSha256,
    inventoryPath,
    inventorySha256,
    selectedPath,
    selectedSha256,
  };
}

for (const stage of ['descriptor', 'half_write', 'file_sync', 'rename', 'dir_sync', 'final_sql']) {
  test(
    `强杀 ${stage} 后两次启动仅恢复原尝试，不重发、不重复结算`,
    { timeout: 60000, skip: stage === 'dir_sync' && process.platform === 'win32' },
    async (t) => {
      const f = await fixture(t, { stage });
      await f.start(stage);
      const response = f.generate(),
        point = await f.waitPoint(stage);
      assert.equal(point.sqliteSynchronous, 2, '实际持久 writer 必须为 FULL');
      assert.equal(f.generations(), 1);
      const original = f.jobs()[0],
        originalCharge = f.settlement(original.id),
        expectedId = original.prepared_artifact_id;
      assert.equal(point.artifactId, expectedId);
      assert.equal(point.attemptId, original.attempt_id);
      assert.ok(expectedId);
      await f.stop();
      await response;
      const complete = !['descriptor', 'half_write'].includes(stage),
        prepared = f.query(
          "SELECT event_id,payload FROM domain_events WHERE event_type='ArtifactPrepared' AND stream_id=?",
          'request:' + original.request_id,
        ),
        before = durableState(f, f.jobs()[0]),
        beforeTables = complete ? null : f.databaseState();
      assert.equal(prepared.length, 1);
      if (original.status === 'running')
        assert.equal(f.preparation(original.id).descriptor.imageId, expectedId);
      let partial;
      if (!complete) {
        await assert.rejects(readFile(path.join(f.dataDir, original.prepared_file)), {
          code: 'ENOENT',
        });
        if (stage === 'descriptor')
          await assert.rejects(readFile(path.join(f.dataDir, original.prepared_file + '.part')), {
            code: 'ENOENT',
          });
        else {
          partial = await readFile(path.join(f.dataDir, original.prepared_file + '.part'));
          assert.deepEqual(partial, png.subarray(0, Math.max(1, Math.floor(png.length / 2))));
        }
      }
      let snapshot;
      for (let restart = 0; restart < 2; restart++) {
        if (complete) await f.start();
        else await f.rejectStart();
        const job = f.jobs()[0];
        assert.equal(job.status, complete ? 'done' : 'running');
        assert.equal(job.attempt_id, original.attempt_id);
        assert.equal(f.generations(), 1);
        assert.equal(f.artifacts().length, complete ? 1 : 0);
        assert.equal(f.settlement(job.id).status, complete ? 'settled' : 'reserved');
        assertOriginalCharge(f.settlement(job.id), originalCharge);
        assert.equal(f.query('SELECT COUNT(*) AS n FROM generation_attempts')[0].n, 1);
        assert.equal(f.query('SELECT COUNT(*) AS n FROM charges')[0].n, 1);
        assert.deepEqual(
          f.query(
            "SELECT event_id,payload FROM domain_events WHERE event_type='ArtifactPrepared' AND stream_id=?",
            'request:' + original.request_id,
          ),
          prepared,
          '原准备事实与描述不被恢复重造',
        );
        assert.deepEqual(
          [job.prepared_artifact_id, job.prepared_file, job.prepared_sha256],
          [original.prepared_artifact_id, original.prepared_file, original.prepared_sha256],
        );
        if (complete) {
          assert.equal(job.artifact_id, expectedId);
          assert.equal(f.pending().includes(job.id), false);
          await f.claim(job, png);
        } else {
          assert.equal(job.artifact_id, null);
          assert.equal(f.pending().includes(job.id), true);
          assert.deepEqual(
            durableState(f, job),
            before,
            '不可恢复的 prepared 保留原预扣与未决责任',
          );
          assert.equal(f.databaseState(), beforeTables, '拒绝监听不能改变任一公私表');
          await assert.rejects(readFile(path.join(f.dataDir, job.prepared_file)), {
            code: 'ENOENT',
          });
          if (stage === 'descriptor')
            await assert.rejects(readFile(path.join(f.dataDir, job.prepared_file + '.part')), {
              code: 'ENOENT',
            });
          else
            assert.deepEqual(
              await readFile(path.join(f.dataDir, job.prepared_file + '.part')),
              partial,
            );
        }
        const state = durableState(f, job);
        if (snapshot) assert.deepEqual(state, snapshot, '第二次启动无重复结算或状态漂移');
        snapshot = state;
        await f.stop();
      }
      assertChildPublicReplay(f.childFixture);
    },
  );
}

test(
  'Native多图首图强杀恢复先于启动清理，第二张只在启动完成和间隔结束后派发',
  { timeout: 45000 },
  async (t) => {
    const f = await fixture(t);
    await f.start('file_sync');
    const response = f.generate({ n_samples: 2 });
    await f.waitPoint('file_sync');
    const first = f.jobs().find((job) => job.sample_index === 0),
      second = f.jobs().find((job) => job.sample_index === 1);
    assert.equal(first.status, 'running');
    assert.equal(second.status, 'pending');
    assert.equal(f.generations(), 1);
    const imageId = first.prepared_artifact_id;
    await f.stop();
    await response;
    await f.start();
    assert.equal(f.jobs().find((job) => job.id === first.id).status, 'done');
    assert.equal(f.jobs().find((job) => job.id === first.id).artifact_id, imageId);
    assert.equal(f.jobs().find((job) => job.id === second.id).status, 'queued');
    assert.equal(f.generations(), 1);
    const completedAt = f.jobs().find((job) => job.id === first.id).completed_at;
    await until(
      () => {
        const jobs = f.jobs();
        assert.ok(!jobs.some((job) => job.status === 'failed'), '新激活 child 不得误判为旧在途');
        return jobs.every((job) => job.status === 'done');
      },
      'second recovered batch image',
      25000,
    );
    const later = f.jobs().find((job) => job.id === second.id);
    assert.ok(
      later.upstream_started_at - completedAt >= 9900,
      `恢复成功也保留十秒账户间隔：originalUpstreamCompletedAt=${first.upstream_completed_at}, recoveredCompletedAt=${completedAt}, nextStartedAt=${later.upstream_started_at}, gapMs=${later.upstream_started_at - completedAt}`,
    );
    assert.equal(f.generations(), 2);
    assert.equal(f.artifacts().length, 2);
    for (const job of f.jobs()) {
      assert.equal(f.settlement(job.id).status, 'settled');
      await f.claim(job, png);
    }
    assert.equal(
      f.query("SELECT COUNT(*) AS n FROM domain_events WHERE event_type='GenerationRefunded'")[0].n,
      0,
    );
    await f.stop();
    assertChildPublicReplay(f.childFixture);
  },
);

test(
  '完整part被篡改不能误恢复成功；已退款Vibe完整part恢复仍保留退款并可领取',
  { timeout: 60000 },
  async (t) => {
    for (const refund of [false, true]) {
      const f = await fixture(t, {
        operation: refund ? 'encode-vibe' : 'generate',
        lateRefund: refund,
      });
      await f.start('file_sync');
      const response = f.generate();
      await f.waitPoint('file_sync');
      const job = f.jobs()[0],
        originalCharge = f.settlement(job.id),
        pending = f.preparation(job.id).descriptor;
      if (refund) {
        // 真实控制 deadline 在原文件 Promise 未结束时由 expire 退款，不直接调用 fail。
        await until(
          () => f.jobs()[0].status === 'failed',
          'actual execution deadline refunds late Vibe',
        );
        assert.equal(f.settlement(job.id).status, 'refunded');
        const failures = f.query(
          "SELECT payload FROM domain_events WHERE event_type='JobFailed' AND stream_id=?",
          'request:' + job.request_id,
        );
        assert.equal(failures.length, 1);
        assert.equal(JSON.parse(failures[0].payload).aborted, true);
      }
      await f.stop();
      await response;
      let corrupted;
      if (!refund) {
        const bytes = await readFile(path.join(f.dataDir, pending.file + '.part'));
        assert.equal(bytes.length, pending.byteLength);
        bytes[bytes.length - 1] ^= 1;
        assert.notEqual(createHash('sha256').update(bytes).digest('hex'), pending.sha256);
        await writeFile(path.join(f.dataDir, pending.file + '.part'), bytes);
        corrupted = bytes;
      }
      const before = durableState(f, f.jobs()[0]),
        beforeTables = refund ? null : f.databaseState();
      let snapshot;
      for (let restart = 0; restart < 2; restart++) {
        if (refund) await f.start();
        else await f.rejectStart();
        const recovered = f.jobs()[0];
        assert.equal(recovered.status, refund ? 'failed' : 'running');
        assert.equal(recovered.attempt_id, job.attempt_id);
        assert.equal(f.settlement(job.id).status, refund ? 'refunded' : 'reserved');
        assertOriginalCharge(f.settlement(job.id), originalCharge);
        assert.equal(f.generations(), 1);
        if (refund) {
          assert.equal(recovered.artifact_id, pending.imageId);
          assert.equal((await f.claim(recovered, vibe)).lateResultAvailable, true);
        } else {
          assert.equal(recovered.artifact_id, null);
          assert.equal(f.artifacts().length, 0);
          assert.equal(f.pending().includes(job.id), true);
          assert.deepEqual(durableState(f, recovered), before);
          assert.equal(
            f.databaseState(),
            beforeTables,
            '损坏 prepared 不得借启动修改公私账务/认证',
          );
          const remaining = await readFile(path.join(f.dataDir, pending.file + '.part'));
          assert.deepEqual(remaining, corrupted, '原同长损坏字节也保持，不覆盖冲突来源');
        }
        const state = durableState(f, recovered);
        if (snapshot) assert.deepEqual(state, snapshot);
        snapshot = state;
        await f.stop();
      }
      assertChildPublicReplay(f.childFixture);
    }
  },
);

test(
  '正常SIGTERM停止新派发并等待在途落盘；超过宽限由重启按未知处理',
  { timeout: 60000, skip: process.platform === 'win32' },
  async (t) => {
    for (const timeout of [false, true]) {
      const f = await fixture(t, { hold: true, grace: timeout ? 100 : 3000 });
      await f.start();
      const response = f.generate();
      await until(() => f.generations() === 1, 'upstream started');
      if (!timeout) {
        const queued = await f.fetchLocal('/api/jobs', {
          method: 'POST',
          headers: f.headers,
          body: JSON.stringify({
            tag: 'synthetic queued during shutdown',
            model: 'nai-diffusion-4-5-full',
            width: 1024,
            height: 1024,
            steps: 23,
          }),
        });
        assert.equal(queued.status, 202);
      }
      const began = Date.now();
      f.child().kill('SIGTERM');
      if (!timeout) {
        await delay(100);
        assert.equal(f.child().exitCode, null);
        f.release();
      }
      await f.exit();
      const delivered = await response;
      assert.ok(Date.now() - began < 5000, '关闭必须遵守有限宽限');
      assert.equal(f.generations(), 1);
      if (!timeout) {
        assert.equal(delivered?.status, 200, '正常关闭必须交付完整HTTP响应');
        const bytes = delivered.bytes;
        assert.ok(bytes.length >= 52, 'ZIP至少包含本地头和完整EOCD');
        assert.equal(bytes.readUInt32LE(0), 0x04034b50);
        assert.equal(bytes.readUInt16LE(8), 0);
        const imageAt = 30 + bytes.readUInt16LE(26) + bytes.readUInt16LE(28);
        assert.deepEqual(bytes.subarray(imageAt, imageAt + bytes.readUInt32LE(18)), png);
        const end = bytes.length - 22;
        assert.equal(bytes.readUInt32LE(end), 0x06054b50);
        assert.equal(bytes.readUInt16LE(end + 20), 0);
        assert.equal(f.jobs().filter((job) => job.status === 'done').length, 1);
        assert.equal(f.jobs().filter((job) => job.status === 'queued').length, 1);
      } else {
        f.release();
        await f.start();
        assert.equal(f.jobs()[0].status, 'failed');
        assert.equal(f.settlement(f.jobs()[0].id).status, 'refunded');
        assert.equal(f.generations(), 1);
      }
      await f.stop();
      assertChildPublicReplay(f.childFixture);
    }
  },
);

test(
  '超过500份结果的冷历史PNG和Vibe在重启后仍有记录、文件和真实签名领取',
  { timeout: 60000 },
  async (t) => {
    const f = await fixture(t, { enableOnStart: false });
    // 原5e夹具的原字段、503条数量和68/256字节原内容保持；没有原parent/操作/时间则不补。
    const entries = Array.from({ length: 503 }, (_, i) => ({
        id: `synthetic-result-${i}`,
        token: f.members[0].api_key,
        mimeType: i === 502 ? 'application/octet-stream' : 'image/png',
        file: `images/synthetic-result-${i}.${i === 502 ? 'bin' : 'png'}`,
        createdAt: new Date(Date.now() - i * 1000).toISOString(),
      })),
      jobs = entries.map((image) => ({
        id: `job-${image.id}`,
        status: 'done',
        userToken: f.members[0].api_key,
        imageId: image.id,
        createdAt: image.createdAt,
      }));
    const source = await installColdHistory(f, entries, jobs);
    for (let restart = 0; restart < 2; restart++) {
      await f.start();
      const before = f.databaseState();
      assert.equal(f.artifacts().length, 503);
      assert.equal(f.jobs().length, 503);
      assert.equal(f.generations(), 0);
      assert.equal(f.query('SELECT COUNT(*) n FROM generation_attempts')[0].n, 0);
      assert.equal(f.query('SELECT COUNT(*) n FROM charges')[0].n, 0);
      assert.ok(
        f
          .jobs()
          .every(
            (job) =>
              job.actual_seed === null &&
              job.sample_index === null &&
              job.completed_at === null &&
              job.admission_epoch === null &&
              job.operation === null,
          ),
      );
      assert.ok(
        f
          .query('SELECT * FROM generation_requests')
          .every(
            (request) =>
              request.source === null &&
              request.operation === null &&
              request.model === null &&
              request.context_id === null &&
              request.sample_count === null &&
              request.accepted_at === null,
          ),
      );
      assert.ok(
        f
          .artifacts()
          .every(
            (artifact) =>
              artifact.operation === null &&
              artifact.published_at === null &&
              artifact.original_attempt_id === null,
          ),
      );
      for (const index of [501, 502]) {
        const image = entries[index],
          expected = index === 502 ? vibe : png,
          view = await f.claim({ id: `job-${image.id}` }, expected);
        assert.equal(view.requestId, null, '内部FK容器不能冒原parent');
        assert.equal(view.source, null);
        assert.equal(view.model, null);
        assert.equal(view.cost, null, '未知原费用不能变为新生成的0');
        assert.equal(view.completedAt, null);
        assert.equal(view.createdAt, image.createdAt);
        assert.deepEqual(await readFile(path.join(f.dataDir, image.file)), expected);
        const denied = await f.fetchLocal('/api/jobs/job-' + image.id, {
          headers: { authorization: `Bearer ${f.members[1].api_key}` },
        });
        assert.equal(denied.status, 403);
        await denied.arrayBuffer();
      }
      assert.equal(f.databaseState(), before, '真实冷读/签名/拒绝不写任务或资金');
      assert.equal(
        createHash('sha256')
          .update(await readFile(source.originalPath))
          .digest('hex'),
        source.originalSha256,
      );
      assert.equal(
        createHash('sha256')
          .update(await readFile(source.inventoryPath))
          .digest('hex'),
        source.inventorySha256,
      );
      assert.equal(relationalSnapshotSha256(source.selectedPath), source.selectedSha256);
      await f.stop();
    }
    assertChildPublicReplay(f.childFixture);
  },
);
