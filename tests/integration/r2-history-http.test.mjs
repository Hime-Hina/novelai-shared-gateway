import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { brotliCompressSync } from 'node:zlib';
import { getPatcher } from 'webdav';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';
import Database from 'better-sqlite3';
import { childFixture, query, assertChildPublicReplay } from '../helpers/relational-child.mjs';
import { domainDigest } from '../../server/events/domain-events.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { relationalFactStream } from '../../server/events/relational-fact-contracts.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import {
  relationalSnapshotSha256,
  restoreRelationalPrivateSnapshot,
} from '../../server/storage/relational-private-restore.ts';
import {
  assertStoredResultContent,
  readableResultFile,
} from '../../server/storage/result-files.ts';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import { R2Storage } from '../../server/storage/r2-storage.ts';
import { PikPakStorage } from '../../server/storage/pikpak-storage.ts';

const png = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=',
  'base64',
);
const sha256 = createHash('sha256').update(png).digest('hex'),
  accountId = 'a'.repeat(32),
  bucket = 'synthetic-images',
  archiveUrl = 'https://dav.pikpak.test/archive/';
const ids = ['local', 'cloud', 'pikpak', 'old_snapshot', 'missing', 'proven_snapshot'];

// Exact original seven-row construction from 5e629689 / blob0e96c827; no legacy Store is loaded.
// Keep source fields separate from any selected inventory or synthetic protocol evidence.
function originalRows(token, createdAt) {
  const db = { images: [], jobs: [] },
    corruptR2Hash = 'e'.repeat(64),
    corruptPikPakHash = 'f'.repeat(64);
  for (const id of [
    'local',
    'cloud',
    'pikpak',
    'old_snapshot',
    'cold_corrupt',
    'corrupt',
    'missing',
  ]) {
    const cold = id === 'pikpak' || id === 'cold_corrupt';
    const imageHash = { corrupt: corruptR2Hash, cold_corrupt: corruptPikPakHash }[id] ?? sha256;
    db.images.push({
      id,
      token,
      file: `images/${id}.png`,
      mimeType: 'image/png',
      createdAt,
      sha256: imageHash,
      byteLength: png.length,
      ...(['local', 'missing'].includes(id)
        ? {}
        : {
            r2: {
              bucket: 'synthetic-images',
              key: `images/${id}/${imageHash}`,
              sha256: imageHash,
              byteLength: png.length,
              verifiedAt: createdAt,
              localRemovedAt: createdAt,
              ...(cold
                ? { removedAt: createdAt, pikpak: { url: archiveUrl, uploadedAt: createdAt } }
                : {}),
            },
          }),
    });
    db.jobs.push({
      id: `job_${id}`,
      userToken: token,
      status: 'done',
      imageId: id,
      createdAt,
      completedAt: createdAt,
    });
  }
  return db;
}

// Original raw rows, a separately declared synthetic archive, and a current Local inventory
// are distinct sources. Bad original checksum declarations must never become Ready.
async function historicalFixture(t) {
  const f = await childFixture(t, 'r2-history-http'),
    at = Date.now(),
    raw = originalRows('STA1N-synthetic-r2-member', new Date(at).toISOString()),
    rawBytes = JSON.stringify(raw),
    rawSha = createHash('sha256').update(rawBytes).digest('hex'),
    origin = (sourceId, originalId) => ({ sourceId, originalId }),
    originals = raw.images.map((image) => {
      const job = raw.jobs.find((job) => job.imageId === image.id);
      return {
        id: image.id,
        jobId: job.id,
        requestId: `historical-container-${image.id}`,
        originalRequestId: null,
        source: null,
        operation: null,
        acceptedAt: null,
        createdAt: Date.parse(job.createdAt),
        completedAt: Date.parse(job.completedAt),
        publishedAt: null,
        sampleIndex: null,
        admissionEpoch: null,
        file: image.file,
        mimeType: image.mimeType,
        sha256: image.sha256,
        byteLength: image.byteLength,
        origin: origin('original-seven', image.id),
        jobOrigin: origin('original-seven', job.id),
        replicas: image.r2
          ? [
              {
                kind: 'r2',
                objectKey: image.r2.key,
                encoding: 'identity',
                uploadedAt: null,
                verifiedAt: Date.parse(image.r2.verifiedAt),
                deletedAt: image.r2.removedAt === undefined ? null : Date.parse(image.r2.removedAt),
                lastErrorCode: null,
              },
              ...(image.r2.pikpak
                ? [
                    {
                      kind: 'pikpak',
                      objectKey: `${image.sha256}.br`,
                      encoding: 'br',
                      uploadedAt: Date.parse(image.r2.pikpak.uploadedAt),
                      verifiedAt: null,
                      deletedAt: null,
                      lastErrorCode: null,
                    },
                  ]
                : []),
            ]
          : [],
      };
    }),
    proven = {
      id: 'proven_snapshot',
      jobId: 'job_proven_snapshot',
      requestId: 'historical-request-proven_snapshot',
      originalRequestId: 'proven-original-parent',
      source: 'synthetic-archive',
      operation: 'generate',
      acceptedAt: 0,
      createdAt: 0,
      completedAt: 0,
      publishedAt: 0,
      sampleIndex: 0,
      admissionEpoch: 0,
      file: 'images/proven_snapshot.png',
      mimeType: 'image/png',
      sha256,
      byteLength: png.length,
      origin: origin('synthetic-archive', 'proven_snapshot'),
      jobOrigin: origin('synthetic-archive', 'job_proven_snapshot'),
      replicas: [
        {
          kind: 'r2',
          objectKey: `images/proven_snapshot/${sha256}`,
          encoding: 'identity',
          uploadedAt: 0,
          verifiedAt: 0,
          deletedAt: null,
          lastErrorCode: null,
        },
        {
          kind: 'pikpak',
          objectKey: `${sha256}.br`,
          encoding: 'br',
          uploadedAt: 0,
          verifiedAt: null,
          deletedAt: null,
          lastErrorCode: null,
        },
      ],
    },
    valid = [...originals.filter((record) => ids.includes(record.id)), proven],
    r2 = new R2Storage({
      accountId,
      bucket,
      accessKeyId: 'synthetic-access',
      secretAccessKey: 'synthetic-secret',
    }),
    pikpak = new PikPakStorage({
      url: archiveUrl,
      username: 'synthetic-pikpak-user',
      password: 'synthetic-pikpak-secret',
    });
  assert.equal(raw.images.length, 7);
  assert.equal(raw.jobs.length, 7);
  assert.equal(valid.length, 6);
  assert.equal(originals.find((record) => record.id === 'corrupt').sha256, 'e'.repeat(64));
  assert.equal(originals.find((record) => record.id === 'cold_corrupt').sha256, 'f'.repeat(64));
  assert.equal(
    originals
      .find((record) => record.id === 'old_snapshot')
      .replicas.some((replica) => replica.kind === 'pikpak'),
    false,
  );
  for (const job of raw.jobs)
    for (const key of [
      'requestId',
      'source',
      'operation',
      'sampleIndex',
      'admissionEpoch',
      'sampleCount',
      'request',
      'fairCharge',
    ])
      assert.equal(Object.hasOwn(job, key), false, `${job.id}:${key} was not in the original row`);
  await mkdir(path.join(f.dataDir, 'images'), { recursive: true });
  for (const record of valid) {
    assert.equal(
      readableResultFile({
        imageId: record.id,
        operation: record.operation,
        mimeType: record.mimeType,
      }),
      record.file,
    );
    const descriptor = {
      imageId: record.id,
      operation: record.operation,
      file: record.file,
      mimeType: record.mimeType,
      byteLength: record.byteLength,
      sha256: record.sha256,
    };
    assertStoredResultContent(descriptor, png);
    await writeFile(path.join(f.dataDir, record.file), png, { flag: 'wx' });
    assert.deepEqual(await readFile(path.join(f.dataDir, record.file)), png);
  }
  const configured = new Database(f.filename, { readonly: true, fileMustExist: true });
  let configuredTargets;
  try {
    const owner = new RelationalReplicaStore(configured, { dataDir: f.dataDir, r2, pikpak });
    configuredTargets = ['local', 'r2', 'pikpak'].map(
      (kind) => owner.configuredTarget(kind).target,
    );
  } finally {
    configured.close();
  }
  const inventory = {
      observedAt: Date.now(),
      targets: configuredTargets,
      memberMapping: {
        originalUserId: 'usr_r2',
        originalToken: raw.jobs[0].userToken,
        memberId: f.members[0].user_id,
      },
      files: valid.map((record) => ({
        file: record.file,
        sha256: record.sha256,
        byteLength: record.byteLength,
      })),
    },
    sourceFiles = [
      ['original-seven.json', rawBytes],
      ['synthetic-archive.json', JSON.stringify(proven)],
      ['selected-local-inventory.json', JSON.stringify(inventory)],
    ],
    sources = sourceFiles.map(([filename, bytes], index) => ({
      sourceId: ['original-seven', 'synthetic-archive', 'selected-local-inventory'][index],
      sourceSha256: createHash('sha256').update(bytes).digest('hex'),
      sourceCommit:
        index === 0 ? '5e629689b0ed41c2df94b52db1dcb3646ffa7314' : 'synthetic-source-contract',
      cutoffSeq: '0',
      cutoffAt: at,
      manifestSha256: createHash('sha256').update(bytes).digest('hex'),
      route: 'B',
    }));
  for (const [filename, bytes] of sourceFiles)
    await writeFile(path.join(f.directory, filename), bytes, { flag: 'wx' });
  const fileState = async () =>
      Promise.all(
        [
          ...sourceFiles.map(([filename]) => path.join(f.directory, filename)),
          ...valid.map((record) => path.join(f.dataDir, record.file)),
        ].map(async (filename) => ({
          filename,
          sha256: createHash('sha256')
            .update(await readFile(filename))
            .digest('hex'),
        })),
      ),
    bootstrapPath = path.join(f.directory, 'bootstrap-selected.sqlite');
  await rename(f.filename, bootstrapPath);
  const original = new Database(bootstrapPath, { readonly: true, fileMustExist: true }),
    historyId = original.prepare('SELECT history_id FROM projection_state').get().history_id;
  original.pragma('query_only=ON');
  try {
    // The two known bad sources are separate installation negatives, not repaired originals.
    for (const label of ['corrupt', 'cold_corrupt', 'valid']) {
      const records = label === 'valid' ? valid : originals.filter((record) => record.id === label),
        db = new Database(
          label === 'valid' ? f.filename : path.join(f.directory, `${label}.sqlite`),
        );
      try {
        initializeRelationalStorage(db, historyId);
        const replayer = new RelationalCommitReplayer(db),
          sourceInputs = new RelationalInputStore(original),
          inputs = new RelationalInputStore(db),
          store = new RelationalReplicaStore(db, { dataDir: f.dataDir, r2, pikpak });
        for (const { seq } of original
          .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
          .all())
          db.transaction(() =>
            replayer.replayCommit(readRelationalCommit(original, seq), sourceInputs),
          )();
        db.transaction(() =>
          restoreRelationalPrivateSnapshot(
            db,
            bootstrapPath,
            relationalSnapshotSha256(bootstrapPath),
          ),
        )();
        db.transaction(() => {
          const event = (eventType, payload, memberId = null) => {
              const kind = relationalFactStream(eventType),
                identity = {
                  import: payload.sourceId,
                  storage: payload.storageTargetId,
                  input: payload.inputId,
                  request: payload.requestId,
                  artifact: payload.artifactId,
                }[kind];
              assert.ok(identity, eventType);
              return {
                eventType,
                payload,
                memberId,
                accountId: null,
                streamId: `${kind}:${identity}`,
                occurredAt: inventory.observedAt,
                policyVersion: 'historical-policy-unknown',
                pricingVersion: 'historical-price-unknown',
              };
            },
            targets = inventory.targets.map((target) => {
              assert.deepEqual(store.configuredTarget(target.kind).target, target);
              return {
                ...target,
                origin: origin('selected-local-inventory', `configured-target:${target.kind}`),
              };
            }),
            events = [
              ...sources.map((source) => event('ImportSourceRegistered', source)),
              ...targets.map((target) => event('OpeningStorageTargetRecorded', target)),
            ];
          for (const record of records) {
            const memberId = f.members[0].user_id,
              inputId = `historical-input-${record.id}`;
            events.push(
              event(
                'OpeningInputRecorded',
                {
                  ...inputs.stageOpeningInput({
                    inputId,
                    memberId,
                    completeness: 'unavailable',
                    body: null,
                    logicalBytes: 0,
                    assets: [],
                  }),
                  origin: record.jobOrigin,
                },
                memberId,
              ),
              event(
                'OpeningRequestRecorded',
                {
                  requestId: record.requestId,
                  originalRequestId: record.originalRequestId,
                  memberId,
                  inputId,
                  contextId: null,
                  source: record.source,
                  operation: record.operation,
                  model: null,
                  sampleCount: null,
                  seedMode: 'unknown',
                  confirmation: null,
                  acceptedAt: record.acceptedAt,
                  retiredAt: null,
                  origin: record.jobOrigin,
                },
                memberId,
              ),
              event(
                'OpeningJobRecorded',
                {
                  sourceOrder: null,
                  displayCost: null,
                  jobId: record.jobId,
                  requestId: record.requestId,
                  createdAt: record.createdAt,
                  sampleIndex: record.sampleIndex,
                  actualSeed: null,
                  status: 'done',
                  queueSeq: null,
                  queueExpiresAt: null,
                  deadlineAt: null,
                  admissionEpoch: record.admissionEpoch,
                  artifactId: record.id,
                  cacheKey: null,
                  completedAt: record.completedAt,
                  errorCode: null,
                  errorStatus: null,
                  cancellationReason: null,
                  lateResultAt: null,
                  freeSucceededAt: null,
                  freeWarningThreshold: null,
                  retiredAt: null,
                  origin: record.jobOrigin,
                },
                memberId,
              ),
              event(
                'OpeningArtifactRecorded',
                {
                  artifactId: record.id,
                  memberId,
                  inputId,
                  originalAttemptId: null,
                  operation: record.operation,
                  mimeType: record.mimeType,
                  byteLength: record.byteLength,
                  sha256: record.sha256,
                  publishedAt: record.publishedAt,
                  retiredAt: null,
                  origin: record.origin,
                },
                memberId,
              ),
              ...[
                ...(label === 'valid'
                  ? [
                      {
                        kind: 'local',
                        objectKey: path.posix.basename(record.file),
                        encoding: 'identity',
                        uploadedAt: null,
                        verifiedAt: inventory.observedAt,
                        deletedAt: null,
                        lastErrorCode: null,
                        origin: origin('selected-local-inventory', record.file),
                      },
                    ]
                  : []),
                ...record.replicas.map((replica) => ({ ...replica, origin: record.origin })),
              ].map(({ kind, ...replica }) =>
                event(
                  'OpeningReplicaRecorded',
                  {
                    artifactId: record.id,
                    storageTargetId: targets.find((target) => target.kind === kind).storageTargetId,
                    ...replica,
                  },
                  memberId,
                ),
              ),
            );
          }
          replayer.appendCommit(
            {
              commandId: `history-opening-${label}`,
              fingerprint: domainDigest(events),
              actorKind: 'system',
              actorId: null,
              recordedAt: inventory.observedAt,
              correlationId: null,
              causationId: null,
              expectedVersions: Object.fromEntries(events.map((event) => [event.streamId, '0'])),
              receipt: { originalSourceSha256: rawSha, selectedRole: label },
              events,
            },
            inputs,
          );
        })();
        for (const record of records) {
          assert.deepEqual(
            db
              .prepare(
                `SELECT r.source,r.operation,r.accepted_at,r.sample_count,r.model,
              j.sample_index,j.admission_epoch,origin.created_at,j.completed_at,
              a.operation artifact_operation,a.published_at,a.sha256,a.byte_length
              FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id
              JOIN generation_job_origins origin ON origin.stream_id='request:'||j.request_id
                AND origin.request_id=j.request_id AND origin.job_id=j.id
              JOIN artifacts a ON a.id=j.artifact_id WHERE j.id=?`,
              )
              .get(record.jobId),
            {
              source: record.source,
              operation: record.operation,
              accepted_at: record.acceptedAt,
              sample_count: null,
              model: null,
              sample_index: record.sampleIndex,
              admission_epoch: record.admissionEpoch,
              created_at: record.createdAt,
              completed_at: record.completedAt,
              artifact_operation: record.operation,
              published_at: record.publishedAt,
              sha256: record.sha256,
              byte_length: record.byteLength,
            },
          );
          assert.equal(inputs.readOriginalRequestId(record.requestId), record.originalRequestId);
          for (const replica of record.replicas) {
            assert.deepEqual(
              db
                .prepare(
                  `SELECT object_key,uploaded_at,verified_at,deleted_at FROM artifact_replicas
                WHERE artifact_id=? AND storage_target_id=?`,
                )
                .get(record.id, store.configuredTarget(replica.kind).target.storageTargetId),
              {
                object_key: replica.objectKey,
                uploaded_at: replica.uploadedAt,
                verified_at: replica.verifiedAt,
                deleted_at: replica.deletedAt,
              },
            );
          }
        }
        const selectedPath = path.join(f.directory, `${label}-selected.sqlite`);
        await db.backup(selectedPath);
        const selected = new Database(selectedPath);
        selected.pragma('journal_mode=DELETE');
        selected.close();
        const sourceSha = relationalSnapshotSha256(selectedPath),
          expected = {
            sourcePath: selectedPath,
            sourceSha256: sourceSha,
            historyId,
            finalCommitSeq: db
              .prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits')
              .get().seq,
            replicas: { dataDir: f.dataDir, r2, pikpak },
          };
        if (label === 'valid') {
          const installed = await verifyAndInstallRelationalStorage(db, expected);
          assert.equal(installed.artifactsVerified, 6);
          assert.equal(db.prepare('SELECT status FROM projection_state').get().status, 'ready');
          db.pragma('journal_mode=DELETE');
        } else {
          const before = db.serialize(),
            filesBefore = await fileState(),
            calls = [],
            clientErrors = [],
            record = records[0],
            client = label === 'corrupt' ? r2 : pikpak,
            actualGet = client.get.bind(client),
            get = t.mock.method(client, 'get', async (image) => {
              try {
                return await actualGet(image);
              } catch (error) {
                clientErrors.push(error.message);
                throw error;
              }
            });
          getPatcher().patch('fetch', (...args) => globalThis.fetch(...args));
          const fetch = t.mock.method(globalThis, 'fetch', async (address, options) => {
            const url = new URL(address);
            assert.equal(options.method, 'GET');
            assert.equal(
              url.origin,
              label === 'corrupt'
                ? `https://${accountId}.r2.cloudflarestorage.com`
                : 'https://dav.pikpak.test',
            );
            assert.equal(
              decodeURIComponent(url.pathname),
              label === 'corrupt'
                ? `/${bucket}/${record.replicas[0].objectKey}`
                : `/archive/${record.sha256}.br`,
            );
            calls.push({
              sha256: createHash('sha256').update(png).digest('hex'),
              byteLength: png.length,
            });
            return new Response(label === 'corrupt' ? png : brotliCompressSync(png));
          });
          try {
            await assert.rejects(verifyAndInstallRelationalStorage(db, expected), {
              code: 'ARTIFACT_CONTENT_UNAVAILABLE',
              statusCode: 503,
            });
          } finally {
            fetch.mock.restore();
            get.mock.restore();
            getPatcher().restore('fetch');
          }
          assert.deepEqual(
            calls,
            [{ sha256, byteLength: png.length }],
            'one actual transport GET reached the preserved bad checksum',
          );
          assert.notEqual(calls[0].sha256, record.sha256);
          assert.deepEqual(clientErrors, [
            label === 'corrupt' ? 'R2 object checksum mismatch' : 'PikPak object checksum mismatch',
          ]);
          assert.deepEqual(db.serialize(), before);
          assert.equal(db.prepare('SELECT status FROM projection_state').get().status, 'building');
          assert.equal(relationalSnapshotSha256(selectedPath), sourceSha);
          assert.deepEqual(await fileState(), filesBefore);
          t.diagnostic(
            JSON.stringify({
              badOriginalSource: label,
              actualGet: calls.length,
              actualBytes: calls[0],
              declaredSha256: record.sha256,
              clientError: clientErrors[0],
              target: 'building',
              unchangedSourceSha256: sourceSha,
            }),
          );
        }
      } finally {
        db.close();
      }
    }
  } finally {
    original.close();
  }
  assert.equal(
    createHash('sha256')
      .update(await readFile(path.join(f.directory, 'original-seven.json')))
      .digest('hex'),
    rawSha,
  );
  f.historical = { originals, valid };
  return f;
}

test(
  '关系 Local、R2、PikPak 与有来源 Opening 历史经真实 HTTP 领取，保持签名、归属和原图',
  { timeout: 20000 },
  async (t) => {
    const f = await historicalFixture(t),
      control = path.join(f.directory, 'storage-fault.json');
    await writeFile(control, JSON.stringify({ corrupt: false }));
    // Faults only affect this test's files/transport after actual original-byte Ready validation.
    // This is neither source-deletion functionality nor acceptance against real cloud endpoints.
    for (const id of ids.filter((id) => id !== 'local')) {
      await rename(path.join(f.dataDir, `images/${id}.png`), path.join(f.directory, `${id}.saved`));
    }
    const preload = path.join(f.directory, 'r2-preload.mjs');
    await writeFile(
      preload,
      `
      import {registerHooks} from 'node:module';
      import net from 'node:net';
      import {readFile} from 'node:fs/promises';
      import {brotliCompressSync} from 'node:zlib';
      import {getPatcher} from ${JSON.stringify(import.meta.resolve('webdav'))};
      function deny() {
        console.error('R2_HISTORY_UNEXPECTED_IO');
        throw new Error('history fixture denied network before IO');
      }
      // This child only serves inbound localhost HTTP; every storage transport is synthetic.
      // Guard actual sockets as well as fetch so a mock miss cannot reach any endpoint.
      net.Socket.prototype.connect = deny;
      registerHooks({load(url, context, next) {
        if (/\\/server\\/storage\\/(?:sqlite-store|sqlite-migrations)\\.(?:ts|js)(?:[?#]|$)/.test(url))
          throw new Error('history child must not load legacy SQLite owners');
        return next(url, context);
      }});
      getPatcher().patch('fetch', (...args) => globalThis.fetch(...args));
      const authorization = 'Basic ' + Buffer.from('synthetic-pikpak-user:synthetic-pikpak-secret').toString('base64');
      globalThis.fetch = async (address, options) => {
        const url = new URL(address), method = options?.method || 'GET';
        const fault = JSON.parse(await readFile(${JSON.stringify(control)}, 'utf8'));
        if (url.origin === 'https://dav.pikpak.test') {
          if (method !== 'GET' || new Headers(options.headers).get('authorization') !== authorization)
            return deny();
          if (url.pathname !== '/archive/${sha256}.br') return new Response(null, {status:404});
          const bytes = Buffer.from('${png.toString('base64')}', 'base64');
          if (fault.corrupt) bytes[0] ^= 1;
          return new Response(brotliCompressSync(bytes));
        }
        if (url.origin !== 'https://${accountId}.r2.cloudflarestorage.com' || method !== 'GET')
          return deny();
        const id = url.pathname.split('/').at(-2);
        if (!${JSON.stringify(ids)}.includes(id) || !url.pathname.endsWith('/${sha256}'))
          return deny();
        if (['pikpak','old_snapshot','proven_snapshot'].includes(id))
          return new Response(null, {status:404});
        const bytes = Buffer.from('${png.toString('base64')}', 'base64');
        if (fault.corrupt && id === 'cloud') bytes[0] ^= 1;
        return new Response(bytes, {headers:{'content-length':String(bytes.length)}});
      };
      process.on('message', message => {if(message === 'shutdown') process.emit('SIGTERM')});
    `,
    );
    const child = spawn(
      process.execPath,
      ['--import', pathToFileURL(preload).href, 'server/index.ts'],
      {
        cwd: fileURLToPath(new URL('../../', import.meta.url)),
        windowsHide: true,
        stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
        env: {
          ...Object.fromEntries(
            ['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE']
              .filter((key) => process.env[key])
              .map((key) => [key, process.env[key]]),
          ),
          ...f.env,
          HOST: '127.0.0.1',
          PORT: '0',
          OWNER_USER_ID: f.members[0].user_id,
          NOVELAI_API_URL: 'http://127.0.0.1:1',
          NOVELAI_ACCOUNT_API_URL: 'http://127.0.0.1:1',
          R2_ACCOUNT_ID: accountId,
          R2_BUCKET: bucket,
          R2_ACCESS_KEY_ID: 'synthetic-access',
          R2_SECRET_ACCESS_KEY: 'synthetic-secret',
          PIKPAK_WEBDAV_URL: archiveUrl,
          PIKPAK_WEBDAV_USERNAME: 'synthetic-pikpak-user',
          PIKPAK_WEBDAV_PASSWORD: 'synthetic-pikpak-secret',
        },
      },
    );
    let output = '',
      base;
    const state = { exit: null },
      finished = once(child, 'exit').then(([code, signal]) => (state.exit = { code, signal }));
    child.stdout.on('data', (chunk) => {
      output += chunk;
      const match = output.match(/Nai2API listening on http:\/\/127\.0\.0\.1:(\d+)/);
      if (match) base = `http://127.0.0.1:${match[1]}`;
    });
    child.stderr.on('data', (chunk) => (output += chunk));
    state.kill = async () => {
      if (!state.exit) child.kill('SIGKILL');
      await finished;
    };
    f.children.push(state);
    const deadline = Date.now() + 15000;
    while (!base && !state.exit && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.ok(base, `synthetic relational runtime must start: ${output}`);
    const request = (url, headers = {}, options = {}) => {
      const address = new URL(url, base);
      assert.equal(address.origin, base, 'HTTP client denies non-loopback IO');
      return fetch(address, { headers, signal: AbortSignal.timeout(3000), ...options });
    };
    const cookies = [];
    for (const member of f.members.slice(0, 2)) {
      const response = await request(
        '/api/member/login',
        { origin: base, 'content-type': 'application/json' },
        {
          method: 'POST',
          body: JSON.stringify({ username: member.username, password: member.password }),
        },
      );
      assert.equal(response.status, 200);
      cookies.push(response.headers.get('set-cookie').split(';')[0]);
      await response.json();
    }
    const business = () => ({
      commits: query(f, 'SELECT COUNT(*) AS n FROM commits')[0].n,
      charges: query(f, 'SELECT * FROM charges ORDER BY original_job_id'),
      attempts: query(f, 'SELECT * FROM generation_attempts ORDER BY id'),
      wallets: query(f, 'SELECT * FROM wallets ORDER BY account_id,member_id'),
      jobs: query(f, 'SELECT id,status,request_id FROM generation_jobs ORDER BY id'),
      requests: query(f, 'SELECT id,sample_count FROM generation_requests ORDER BY id'),
    });
    const before = business(),
      bearer = { authorization: `Bearer ${f.members[0].api_key}` },
      ownerCookie = { cookie: cookies[0] },
      otherCookie = { cookie: cookies[1] };
    for (const id of ['local', 'cloud', 'pikpak', 'proven_snapshot']) {
      const view = await request(`/api/jobs/job_${id}`, bearer);
      assert.equal(view.status, 200);
      const body = await view.json();
      assert.doesNotMatch(
        JSON.stringify(body),
        /cloudflarestorage\.com|synthetic-(pikpak-)?secret|dav\.pikpak\.test/,
      );
      assert.equal(body.cost, null, 'unknown historical fees never become a new zero quote');
      assert.equal(body.model, null);
      const original = f.historical.valid.find((record) => record.id === id);
      assert.equal(body.createdAt, new Date(original.createdAt).toISOString());
      assert.equal(body.completedAt, new Date(original.completedAt).toISOString());
      assert.equal(body.source, original.source);
      assert.equal(body.requestId, original.originalRequestId === null ? null : original.requestId);
      const memberContent = `/api/member/jobs/job_${id}/content`;
      for (const response of [
        await request(body.imageUrl),
        await request(`/api/jobs/job_${id}/content`, bearer),
        await request(memberContent, ownerCookie),
        await request(`/api/jobs/job_${id}/content`, { 'x-admin-token': f.env.ADMIN_TOKEN }),
      ]) {
        assert.equal(response.status, 200);
        assert.match(response.headers.get('content-type'), /image\/png/);
        assert.deepEqual(Buffer.from(await response.arrayBuffer()), png);
        assert.equal(response.headers.get('set-cookie'), null);
      }
      assert.equal((await request(`/api/images/${id}/content`)).status, 401);
      assert.equal((await request(memberContent)).status, 401);
      assert.equal((await request(memberContent, otherCookie)).status, 404);
      assert.equal(
        (
          await request(`/api/jobs/job_${id}/content`, {
            authorization: `Bearer ${f.members[1].api_key}`,
          })
        ).status,
        403,
      );
      const tampered = new URL(body.imageUrl, base);
      tampered.searchParams.set('signature', 'invalid');
      assert.equal((await request(tampered)).status, 401);
    }
    // A current same-SHA PikPak object is not a substitute for a declared original source.
    const unproven = await request('/api/jobs/job_old_snapshot/content', bearer);
    assert.equal(unproven.status, 503);
    await unproven.text();
    await writeFile(control, JSON.stringify({ corrupt: true }));
    for (const id of ['cloud', 'pikpak']) {
      const failure = await request(`/api/jobs/job_${id}/content`, bearer);
      assert.equal(failure.status, 503);
      assert.doesNotMatch(await failure.text(), /synthetic-(pikpak-)?secret|dav\.pikpak\.test/);
    }
    await writeFile(control, JSON.stringify({ corrupt: false }));
    for (const id of ['cloud', 'pikpak']) {
      const restored = await request(`/api/jobs/job_${id}/content`, bearer);
      assert.equal(restored.status, 200);
      assert.deepEqual(Buffer.from(await restored.arrayBuffer()), png);
      assert.equal(restored.headers.get('set-cookie'), null);
    }
    const missing = await request('/api/jobs/job_missing/content', bearer),
      missingBody = await missing.text();
    assert.ok(!missingBody.includes(f.dataDir), 'filesystem paths stay private');
    assert.deepEqual(
      business(),
      before,
      'read/failure never regenerates, charges or writes a receipt',
    );
    child.send('shutdown');
    await finished;
    assert.equal(state.exit.code, 0, output);
    assert.doesNotMatch(output, /R2_HISTORY_UNEXPECTED_IO/);
    assertChildPublicReplay(f);
    t.diagnostic(
      'Original checksum declarations e/f each reject Ready after one actual mocked transport GET; Ready=6 (five valid original rows and one separate synthetic archive); four cases/16 exact-byte deliveries plus two recovery reads; two post-install corrupt responses; original undeclared-PikPak rejection; zero business writes; graceful close/public replay verified.',
    );
    // Keep the old assertion until the API change is explicitly reviewed. Its known 503/500
    // difference is deliberately last, so it cannot mask the source/auth/content checks above.
    assert.equal(missing.status, 500, 'retain the existing local storage failure contract');
  },
);
