import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, rename, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { childFixture } from './relational-child.mjs';
import { databaseState } from './relational-generation.mjs';
import { domainDigest } from '../../server/events/domain-events.ts';
import { relationalFactStream } from '../../server/events/relational-fact-contracts.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalReplicaStore } from '../../server/storage/relational-replica-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import {
  readRelationalRecoveryState,
  relationalPrivateStateEmpty,
  relationalSnapshotSha256,
  restoreRelationalPrivateSnapshot,
} from '../../server/storage/relational-private-restore.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { readableResultFile, readStoredResultContent } from '../../server/storage/result-files.ts';

// Independent, fully specified synthetic history. This is not a conversion of the
// old archive's done-without-image rows or charges with unknown account/times.
export async function lateResultsHttpFixture(t, png) {
  const f = await childFixture(t, 'member-late-results'),
    sourcePath = path.join(f.directory, 'bootstrap-source.sqlite'),
    targetPath = path.join(f.directory, 'known-history.sqlite'),
    selectedPath = path.join(f.directory, 'known-history-source.sqlite'),
    originalPath = path.join(f.directory, 'before-known-history.sqlite'),
    hash = (bytes) => createHash('sha256').update(bytes).digest('hex'),
    source = new Database(f.filename, { readonly: true, fileMustExist: true });
  try {
    await source.backup(sourcePath);
  } finally {
    source.close();
  }
  const bootstrap = new Database(sourcePath),
    db = new Database(targetPath);
  const actualFetch = globalThis.fetch;
  let offlineFetches = 0;
  globalThis.fetch = () => {
    offlineFetches++;
    throw new Error('known history construction denied network before IO');
  };
  try {
    bootstrap.pragma('journal_mode=DELETE');
    const cut = readRelationalRecoveryState(bootstrap),
      sourceSha256 = relationalSnapshotSha256(sourcePath);
    initializeRelationalStorage(db, cut.history_id);
    const replay = new RelationalCommitReplayer(db),
      savedInputs = new RelationalInputStore(bootstrap);
    for (const { seq } of bootstrap
      .prepare('SELECT CAST(commit_seq AS TEXT) seq FROM commits ORDER BY commit_seq')
      .all())
      db.transaction(() =>
        replay.replayCommit(readRelationalCommit(bootstrap, seq), savedInputs),
      )();
    assert.equal(relationalPrivateStateEmpty(db), true);
    db.transaction(() => restoreRelationalPrivateSnapshot(db, sourcePath, sourceSha256))();
    const financial = ['wallets', 'operating_state'].map((table) => [
      table,
      db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
    ]);
    const [alice, bob] = f.members.map((member) => member.user_id),
      members = new RelationalMemberStore(db);
    for (const [memberId, username] of [
      [alice, 'alice'],
      [bob, 'bob'],
    ]) {
      const prepared = await members.prepareMemberCredentials(memberId, {
        username,
        password: 'synthetic-password-123456789',
      });
      db.transaction(() => members.setMemberCredentials(prepared, null))();
    }

    const now = Date.now(),
      createdAt = now - 40 * 86400000,
      configVersion = db.prepare('SELECT config_version v FROM sharing_policy WHERE id=1').get().v,
      sourceId = 'independent-known-late-results',
      inputId = (memberId) => `known-input-${memberId}`,
      containerId = (id) => `known-container-${id}`,
      origin = (originalId) => ({ sourceId, originalId }),
      rows = Array.from({ length: 3100 }, (_, i) => ({
        id: `filler-${i}`,
        memberId: bob,
        status: 'done',
        createdAt: now,
        artifactId: 'image-filler-b',
        lateResultAt: null,
      }));
    for (let i = 0; i < 12; i++)
      rows.push({
        id: `late-${i}`,
        memberId: alice,
        status: 'failed',
        createdAt,
        artifactId: `image-late-${i}`,
        lateResultAt: now - i,
      });
    rows.push(
      {
        id: 'other',
        memberId: bob,
        status: 'failed',
        createdAt,
        artifactId: 'image-other',
        lateResultAt: now,
      },
      {
        id: 'mismatch',
        memberId: alice,
        status: 'failed',
        createdAt,
        artifactId: null,
        lateResultAt: now,
      },
      {
        id: 'deleted-image',
        memberId: alice,
        status: 'failed',
        createdAt,
        artifactId: null,
        lateResultAt: now,
      },
      {
        id: 'hot-result',
        memberId: alice,
        status: 'failed',
        createdAt,
        artifactId: 'image-late-0',
        lateResultAt: now,
      },
    );
    const artifactRows = [
      { id: 'image-filler-b', memberId: bob },
      ...Array.from({ length: 12 }, (_, i) => ({ id: `image-late-${i}`, memberId: alice })),
      { id: 'image-other', memberId: bob },
      { id: 'image-mismatch', memberId: bob },
    ];
    // These account, reservation and refund times are explicit fields of this
    // separate known source, never inferred from the old archive's createdAt.
    const known = {
      role: 'independent-known-source',
      accountId: f.accountId,
      configVersion,
      reservedAt: now - 1000,
      terminalAt: now - 900,
      rows,
      artifacts: artifactRows,
      refund: { v5: '0', anlasFixed: '5', anlasPurchased: '2' },
      body: { input: 'private-prompt', model: 'nai-diffusion-5-full' },
    };
    const originalBytes = Buffer.from(JSON.stringify(known)),
      originalSha256 = hash(originalBytes);
    await writeFile(path.join(f.directory, 'independent-known-source.json'), originalBytes, {
      flag: 'wx',
      mode: 0o600,
    });
    await mkdir(path.join(f.dataDir, 'images'), { recursive: true });
    const inventory = [];
    for (const artifact of artifactRows) {
      const descriptor = {
        imageId: artifact.id,
        operation: null,
        mimeType: 'image/png',
        file: readableResultFile({ imageId: artifact.id, operation: null, mimeType: 'image/png' }),
        byteLength: png.length,
        sha256: hash(png),
      };
      await writeFile(path.join(f.dataDir, descriptor.file), png, { flag: 'wx', mode: 0o600 });
      assert.deepEqual((await readStoredResultContent(f.dataDir, descriptor)).buffer, png);
      inventory.push({ descriptor, verifiedAt: Date.now() });
    }
    const inventoryBytes = Buffer.from(JSON.stringify({ originalSha256, inventory }));
    await writeFile(path.join(f.directory, 'known-local-inventory.json'), inventoryBytes, {
      flag: 'wx',
      mode: 0o600,
    });
    const target = new RelationalReplicaStore(db, { dataDir: f.dataDir }).configuredTarget(
        'local',
      ).target,
      inputs = new RelationalInputStore(db),
      event = (eventType, payload, memberId = null, eventId = randomUUID()) => {
        const kind = relationalFactStream(eventType),
          field = {
            import: 'sourceId',
            input: 'inputId',
            request: 'requestId',
            artifact: 'artifactId',
            storage: 'storageTargetId',
          }[kind];
        return {
          eventId,
          eventType,
          payload,
          streamId: kind === 'account' ? `account:${f.accountId}` : `${kind}:${payload[field]}`,
          memberId,
          accountId: kind === 'account' ? f.accountId : null,
          occurredAt: now,
          policyVersion: 'known-source-policy',
          pricingVersion: 'known-source-price',
        };
      },
      append = (id, events) =>
        replay.appendCommit(
          {
            commandId: `known-late:${id}`,
            actorKind: 'system',
            actorId: null,
            recordedAt: now,
            correlationId: null,
            causationId: null,
            fingerprint: domainDigest(events),
            receipt: { independentKnownHistory: true },
            events,
            expectedVersions: Object.fromEntries(
              events.map((e) => [
                e.streamId,
                db
                  .prepare(
                    'SELECT CAST(stream_version AS TEXT) v FROM stream_heads WHERE stream_id=?',
                  )
                  .get(e.streamId)?.v ?? '0',
              ]),
            ),
          },
          inputs,
        ),
      jobEvents = (row, artifactId = row.artifactId) => [
        event(
          'OpeningRequestRecorded',
          {
            requestId: containerId(row.id),
            originalRequestId: null,
            memberId: row.memberId,
            inputId: inputId(row.memberId),
            contextId: null,
            source: null,
            operation: null,
            model: known.body.model,
            sampleCount: null,
            seedMode: 'unknown',
            confirmation: null,
            acceptedAt: null,
            retiredAt: null,
            origin: origin(row.id),
          },
          row.memberId,
        ),
        event(
          'OpeningJobRecorded',
          {
            sourceOrder: null,
            displayCost: null,
            jobId: row.id,
            requestId: containerId(row.id),
            createdAt: row.createdAt,
            sampleIndex: null,
            actualSeed: null,
            status: row.status,
            queueSeq: null,
            queueExpiresAt: null,
            deadlineAt: null,
            admissionEpoch: null,
            artifactId,
            cacheKey: null,
            completedAt: null,
            errorCode: null,
            errorStatus: null,
            cancellationReason: null,
            lateResultAt: row.lateResultAt,
            freeSucceededAt: null,
            freeWarningThreshold: null,
            retiredAt: null,
            origin: origin(row.id),
          },
          row.memberId,
        ),
      ];
    db.transaction(() => {
      const events = [
        event('ImportSourceRegistered', {
          sourceId,
          sourceSha256: originalSha256,
          sourceCommit: '147aabd6f7996442d5c8d242379e080ff27afe19',
          cutoffSeq: '0',
          cutoffAt: now,
          manifestSha256: hash(inventoryBytes),
          route: 'B',
        }),
        event('OpeningStorageTargetRecorded', { ...target, origin: origin('images') }),
        ...[alice, bob].map((memberId) =>
          event(
            'OpeningInputRecorded',
            {
              ...inputs.stageOpeningInput({
                inputId: inputId(memberId),
                memberId,
                completeness: 'partial',
                body: known.body,
                logicalBytes: Buffer.byteLength(JSON.stringify(known.body)),
                assets: [],
              }),
              origin: origin(`input:${memberId}`),
            },
            memberId,
          ),
        ),
      ];
      for (const [index, artifact] of artifactRows.entries()) {
        const { descriptor, verifiedAt } = inventory[index];
        events.push(
          event(
            'OpeningArtifactRecorded',
            {
              artifactId: artifact.id,
              memberId: artifact.memberId,
              inputId: inputId(artifact.memberId),
              originalAttemptId: null,
              operation: null,
              mimeType: descriptor.mimeType,
              byteLength: descriptor.byteLength,
              sha256: descriptor.sha256,
              publishedAt: null,
              retiredAt: null,
              origin: origin(artifact.id),
            },
            artifact.memberId,
          ),
          event(
            'OpeningReplicaRecorded',
            {
              artifactId: artifact.id,
              storageTargetId: target.storageTargetId,
              objectKey: path.posix.basename(descriptor.file),
              encoding: 'identity',
              uploadedAt: null,
              verifiedAt,
              deletedAt: null,
              lastErrorCode: null,
              origin: origin(descriptor.file),
            },
            artifact.memberId,
          ),
        );
      }
      append('sources', events);
    })();
    const mismatch = rows.find((row) => row.id === 'mismatch'),
      before = databaseState(db);
    assert.throws(
      () => db.transaction(() => append('jobs', jobEvents(mismatch, 'image-mismatch')))(),
      /开账任务缺少同成员\/操作的唯一原产物/,
    );
    assert.equal(
      databaseState(db),
      before,
      'cross-owner Opening must roll back the complete commit',
    );
    db.transaction(() => {
      const events = [];
      for (const row of rows) {
        events.push(...jobEvents(row));
        if (row.status !== 'failed') continue;
        const eventId = randomUUID();
        events.push(
          event(
            'OpeningChargeRecorded',
            {
              originalJobId: row.id,
              originalMemberId: row.memberId,
              originalAccountId: known.accountId,
              jobId: row.id,
              walletAccountId: null,
              walletMemberId: null,
              attemptId: null,
              status: 'refunded',
              resolution: 'archived',
              originRef: `known-refund:${row.id}`,
              resources: known.refund,
              billingMode: 'anlas',
              estimated: false,
              v5Equivalent: '0',
              sharedV5Reserved: '0',
              configVersion: known.configVersion,
              reservedAt: known.reservedAt,
              terminalAt: known.terminalAt,
              reservationEventId: eventId,
              terminalEventId: eventId,
              origin: origin(`refund:${row.id}`),
            },
            row.memberId,
            eventId,
          ),
        );
      }
      append('jobs', events);
    })();
    for (const [table, before] of financial)
      assert.deepEqual(
        db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
        before,
        'opening archived refunds must not manufacture a current wallet change',
      );
    assert.equal(
      db.prepare("SELECT COUNT(*) n FROM generation_jobs WHERE id LIKE 'filler-%'").get().n,
      3100,
    );
    await db.backup(selectedPath);
    const selected = new Database(selectedPath);
    selected.pragma('journal_mode=DELETE');
    selected.close();
    await verifyAndInstallRelationalStorage(db, {
      sourcePath: selectedPath,
      sourceSha256: relationalSnapshotSha256(selectedPath),
      historyId: cut.history_id,
      finalCommitSeq: readRelationalRecoveryState(db).last_commit_seq,
      replicas: { dataDir: f.dataDir },
    });
    db.pragma('journal_mode=DELETE');
  } finally {
    globalThis.fetch = actualFetch;
    db.close();
    bootstrap.close();
  }
  assert.equal(offlineFetches, 0);
  assert.equal(f.children.length, 0);
  for (const filename of [f.filename, originalPath, targetPath]) {
    const relative = path.relative(path.resolve(f.directory), path.resolve(filename));
    assert.ok(
      relative &&
        relative !== '..' &&
        !relative.startsWith('..' + path.sep) &&
        !path.isAbsolute(relative),
    );
  }
  await rename(f.filename, originalPath);
  for (const suffix of ['-wal', '-shm', '-journal']) {
    try {
      await rename(f.filename + suffix, originalPath + suffix);
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
  }
  await rename(targetPath, f.filename);
  return f;
}
