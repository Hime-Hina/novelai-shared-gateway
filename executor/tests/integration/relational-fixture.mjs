import assert from 'node:assert/strict';
import { mkdir, rename } from 'node:fs/promises';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);
export async function bootstrapPackaged(script, env, signal) {
  const guard = `import http from 'node:http'; import https from 'node:https'; import net from 'node:net';
import {syncBuiltinESMExports,registerHooks} from 'node:module';
const deny=()=>{process.stderr.write('Forbidden bootstrap IO\\n');throw new Error('Offline bootstrap IO');};
globalThis.fetch=deny;for(const owner of [http,https]){owner.request=deny;owner.get=deny;}
net.connect=deny;net.createConnection=deny;net.Server.prototype.listen=deny;
registerHooks({load(url,context,next){if(/\\.(?:ts|tsx|mts|cts)$/.test(url))throw new Error('Compiled bootstrap cannot load TypeScript');return next(url,context);}});
syncBuiltinESMExports();`;
  const result = await execFileAsync(
    process.execPath,
    [
      '--no-experimental-strip-types',
      '--import',
      'data:text/javascript,' + encodeURIComponent(guard),
      script,
    ],
    {
      env: {
        ...env,
        NOVELAI_API_URL: 'http://127.0.0.1:1',
        NOVELAI_ACCOUNT_API_URL: 'http://127.0.0.1:1',
      },
      windowsHide: true,
      signal,
    },
  );
  assert.doesNotMatch(result.stderr, /Forbidden bootstrap IO/);
}

// Both real process suites import only the selected compiled package; Docker mounts this directory.
export async function addRetainedHistory({
  Database,
  filename,
  members,
  load,
  idPrefix,
  prompt,
  image,
}) {
  const { canonicalJson, domainDigest } = await load('server/events/domain-events.js'),
    { readRelationalCommit } = await load('server/events/relational-event-store.js'),
    { RelationalCommitReplayer } = await load('server/storage/relational-replay.js'),
    { RelationalInputStore } = await load('server/storage/relational-input-store.js'),
    { initializeRelationalStorage, assertRelationalStorageReady } = await load(
      'server/storage/relational-storage.js',
    ),
    { restoreRelationalPrivateSnapshot, relationalSnapshotSha256 } = await load(
      'server/storage/relational-private-restore.js',
    ),
    { verifyAndInstallRelationalStorage } = await load('server/storage/relational-ready.js'),
    { RelationalReplicaStore } = await load('server/storage/relational-replica-store.js'),
    { describeStoredResult, publishStoredResult } = await load('server/storage/result-files.js');
  const sourcePath = filename + '.bootstrap-source';
  await rename(filename, sourcePath);
  const source = new Database(sourcePath, { readonly: true, fileMustExist: true }),
    target = new Database(filename);
  try {
    source.pragma('foreign_keys=ON');
    const historyId = source.prepare('SELECT history_id id FROM projection_state').get().id;
    initializeRelationalStorage(target, historyId);
    const replay = new RelationalCommitReplayer(target),
      inputs = new RelationalInputStore(source);
    for (const { seq } of source
      .prepare('SELECT CAST(commit_seq AS TEXT) seq FROM commits ORDER BY commit_seq')
      .all()) {
      target.transaction(() => replay.replayCommit(readRelationalCommit(source, seq), inputs))();
    }
    target.transaction(() =>
      restoreRelationalPrivateSnapshot(target, sourcePath, relationalSnapshotSha256(sourcePath)),
    )();
    const createdAt = Date.now(),
      records = [0, 1, 1].map((member, index) => ({
        memberId: members[member].user_id,
        jobId: `${idPrefix}-${index}`,
        requestId: `${idPrefix}-request-${index}`,
        originalRequestId: null,
        inputId: `${idPrefix}-input-${index}`,
        source: 'novelai-native',
        acceptedAt: null,
        sampleCount: null,
        sampleIndex: null,
        admissionEpoch: null,
        createdAt,
        descriptor: describeStoredResult(`${idPrefix}-artifact-${index}`, 'generate', {
          mimeType: 'image/png',
          buffer: image,
        }),
        body: { operation: 'generate', model: 'nai-diffusion-4-5-full', input: prompt },
      })),
      sourceId = idPrefix + '-source',
      staged = new RelationalInputStore(target);
    await mkdir(path.join(path.dirname(filename), 'images'), { recursive: true });
    for (const record of records) {
      await publishStoredResult(path.dirname(filename), record.descriptor, image);
      record.publishedAt = Date.now();
      record.completedAt = Date.now();
    }
    const snapshotAt = Date.now();
    target.transaction(() => {
      const event = (eventType, payload, streamId, memberId = null) => ({
          eventType,
          payload,
          streamId,
          memberId,
          accountId: null,
          occurredAt: snapshotAt,
          policyVersion: 'synthetic-retained-policy-unknown',
          pricingVersion: 'synthetic-retained-price-unknown',
        }),
        events = [
          event(
            'ImportSourceRegistered',
            {
              sourceId,
              sourceSha256: domainDigest(records),
              sourceCommit: 'synthetic-retained-records-v1',
              cutoffSeq: String(records.length),
              cutoffAt: snapshotAt,
              manifestSha256: domainDigest({ format: 'synthetic-retained-records-v1', records }),
              route: 'B',
            },
            'import:' + sourceId,
          ),
        ];
      let storageTargetId;
      for (const record of records) {
        const origin = { sourceId, originalId: record.jobId };
        const local = new RelationalReplicaStore(target).planLocalArtifactPublication(
          record.descriptor,
          record.publishedAt,
        );
        if (!storageTargetId) {
          storageTargetId = local.target.payload.storageTargetId;
          events.push(
            event(
              'OpeningStorageTargetRecorded',
              { ...local.target.payload, origin },
              'storage:' + storageTargetId,
            ),
          );
        }
        events.push(
          event(
            'OpeningInputRecorded',
            {
              ...staged.stageOpeningInput({
                inputId: record.inputId,
                memberId: record.memberId,
                completeness: 'partial',
                body: record.body,
                logicalBytes: Buffer.byteLength(canonicalJson(record.body)),
                assets: [],
              }),
              origin,
            },
            'input:' + record.inputId,
            record.memberId,
          ),
          event(
            'OpeningRequestRecorded',
            {
              requestId: record.requestId,
              originalRequestId: record.originalRequestId,
              memberId: record.memberId,
              inputId: record.inputId,
              contextId: null,
              source: record.source,
              operation: record.body.operation,
              model: record.body.model,
              sampleCount: record.sampleCount,
              seedMode: 'unknown',
              confirmation: null,
              acceptedAt: record.acceptedAt,
              retiredAt: null,
              origin,
            },
            'request:' + record.requestId,
            record.memberId,
          ),
          event(
            'OpeningJobRecorded',
            {
              jobId: record.jobId,
              requestId: record.requestId,
              diagnosticId: null,
              sourceOrder: null,
              displayCost: null,
              createdAt: record.createdAt,
              sampleIndex: record.sampleIndex,
              actualSeed: null,
              status: 'done',
              queueSeq: null,
              queueExpiresAt: null,
              deadlineAt: null,
              admissionEpoch: record.admissionEpoch,
              artifactId: record.descriptor.imageId,
              cacheKey: null,
              completedAt: record.completedAt,
              errorCode: null,
              errorStatus: null,
              cancellationReason: null,
              lateResultAt: null,
              freeSucceededAt: null,
              freeWarningThreshold: null,
              retiredAt: null,
              origin,
            },
            'request:' + record.requestId,
            record.memberId,
          ),
          event(
            'OpeningArtifactRecorded',
            {
              artifactId: record.descriptor.imageId,
              memberId: record.memberId,
              inputId: record.inputId,
              originalAttemptId: null,
              operation: record.descriptor.operation,
              mimeType: record.descriptor.mimeType,
              byteLength: record.descriptor.byteLength,
              sha256: record.descriptor.sha256,
              publishedAt: record.publishedAt,
              retiredAt: null,
              origin,
            },
            'artifact:' + record.descriptor.imageId,
            record.memberId,
          ),
          event(
            'OpeningReplicaRecorded',
            { ...local.replica.payload, origin },
            'artifact:' + record.descriptor.imageId,
            record.memberId,
          ),
        );
      }
      replay.appendCommit(
        {
          commandId: idPrefix + '-opening',
          fingerprint: domainDigest(events),
          actorKind: 'system',
          actorId: null,
          recordedAt: snapshotAt,
          correlationId: null,
          causationId: null,
          expectedVersions: Object.fromEntries(events.map((e) => [e.streamId, '0'])),
          receipt: { retainedJobs: records.length },
          events,
        },
        staged,
      );
    })();
    // 本次合成清单记录真实写图与构造完成时点，不补原受理、父请求、数量、位置或代次。
    for (const record of records) {
      assert.equal(staged.readOriginalRequestId(record.requestId), null);
      assert.deepEqual(
        target
          .prepare(
            `SELECT r.source,r.accepted_at,r.sample_count,j.sample_index,j.admission_epoch,
            j.completed_at,a.published_at FROM generation_requests r
            JOIN generation_jobs j ON j.request_id=r.id JOIN artifacts a ON a.id=j.artifact_id
            WHERE j.id=?`,
          )
          .get(record.jobId),
        {
          source: record.source,
          accepted_at: null,
          sample_count: null,
          sample_index: null,
          admission_epoch: null,
          completed_at: record.completedAt,
          published_at: record.publishedAt,
        },
      );
    }
    // The synthetic records know no attempt, charge, seed or free-use history; none is manufactured.
    assert.equal(target.prepare('SELECT COUNT(*) n FROM charges').get().n, 0);
    assert.equal(target.prepare('SELECT COUNT(*) n FROM generation_attempts').get().n, 0);
    const selected = filename + '.retained-source';
    await target.backup(selected);
    const sealed = new Database(selected);
    sealed.pragma('journal_mode=DELETE');
    sealed.close();
    await verifyAndInstallRelationalStorage(target, {
      sourcePath: selected,
      sourceSha256: relationalSnapshotSha256(selected),
      historyId,
      finalCommitSeq: target.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) seq FROM commits').get()
        .seq,
      replicas: { dataDir: path.dirname(filename) },
    });
    assertRelationalStorageReady(target);
  } finally {
    source.close();
    target.close();
  }
}
