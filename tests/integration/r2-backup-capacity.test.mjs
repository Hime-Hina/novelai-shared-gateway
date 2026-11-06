import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes, createHash } from 'node:crypto';
import { deflateSync } from 'node:zlib';
import { mkdir, readFile, writeFile, readdir, lstat } from 'node:fs/promises';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import Database from 'better-sqlite3';
import { backupFixture } from '../helpers/r2-backup-fixture.mjs';
import {
  JsonStore,
  applyEventProjection,
  verifyEventProjections,
} from '../../server/storage/sqlite-store.ts';
import { migrateStorage } from '../../server/storage/sqlite-migrations.ts';
import { copyProtectedFacts, replayDomainEvents } from '../../server/events/event-store.ts';
import {
  normalizeMemberGeneration,
  materializeMemberGeneration,
} from '../../server/generation/member-contract.ts';
import { createGenerationAdmission, generationIntent } from '../../server/generation/admission.ts';
import { createGenerationPayloadBudget } from '../../server/generation/batch-requests.ts';
import { configureFairQuota, syncFairQuota } from '../../server/quota/fair-share.ts';
import { crc32 } from '../../server/protocols/native-image.ts';
import { ImageStorage } from '../../server/storage/r2-storage.ts';

const run = promisify(execFile),
  sha = (data) => createHash('sha256').update(data).digest('hex');
const enabled =
  process.platform === 'linux' &&
  process.env.BACKUP_CAPACITY_CONTAINER === '1' &&
  process.env.BACKUP_CAPACITY_OUTPUT;
// Explicit opt-in: this harness provisions synthetic production paths in a disposable, network-disabled Linux container.
test(
  'production compressed encrypted business backup restores large protected inputs and event projections',
  { skip: !enabled },
  async (t) => {
    const root = process.env.BACKUP_CAPACITY_OUTPUT;
    await mkdir(root, { recursive: false });
    const disk = async (dir) => {
      let total = 0;
      for (const name of await readdir(dir)) {
        const file = path.join(dir, name),
          info = await lstat(file);
        if (!info.isSymbolicLink()) total += info.isDirectory() ? await disk(file) : info.size;
      }
      return total;
    };
    let sampledTempPeakBytes = 0,
      sample = Promise.resolve();
    const timer = setInterval(() => {
      sample = sample
        .then(async () => {
          sampledTempPeakBytes = Math.max(sampledTempPeakBytes, await disk(root));
        })
        .catch(() => {});
    }, 100);
    t.after(async () => {
      clearInterval(timer);
      await sample;
    });
    const fixture = await backupFixture(t, root),
      live = path.join(root, 'live');
    const store = new JsonStore(live);
    await store.init();
    t.after(() => store.close());
    const started = performance.now(),
      timings = {};
    const png = () => {
      const chunk = (name, bytes) => {
        const head = Buffer.alloc(8),
          tail = Buffer.alloc(4);
        head.writeUInt32BE(bytes.length);
        head.write(name, 4);
        tail.writeUInt32BE(crc32(Buffer.concat([Buffer.from(name), bytes])));
        return Buffer.concat([head, bytes, tail]);
      };
      const head = Buffer.alloc(13);
      head.writeUInt32BE(512);
      head.writeUInt32BE(512, 4);
      head[8] = 8;
      head[9] = 6;
      const rows = randomBytes((512 * 4 + 1) * 512);
      for (let i = 0; i < 512; i++) rows[i * (512 * 4 + 1)] = 0;
      return Buffer.concat([
        Buffer.from('89504e470d0a1a0a', 'hex'),
        chunk('IHDR', head),
        chunk('IDAT', deflateSync(rows)),
        chunk('IEND', Buffer.alloc(0)),
      ]);
    };
    const inputImage = png(),
      mask = png(),
      result = png(),
      reference = randomBytes(1024 * 1024);
    const normalized = normalizeMemberGeneration({
      operation: 'generate',
      action: 'infill',
      model: 'nai-diffusion-4-5-full',
      input: 'synthetic private long prompt '.repeat(100),
      parameters: {
        width: 512,
        height: 512,
        steps: 23,
        seed: 7,
        image: inputImage.toString('base64'),
        mask: mask.toString('base64'),
      },
    });
    const vibe = normalizeMemberGeneration({
      operation: 'generate',
      action: 'generate',
      model: 'nai-diffusion-4-5-full',
      input: 'synthetic private reference',
      parameters: {
        width: 512,
        height: 512,
        steps: 23,
        seed: 8,
        reference_image_multiple: [reference.toString('base64')],
        reference_strength_multiple: [0.6],
      },
    });
    const now = 1800000000000,
      seed = structuredClone(store.db);
    seed.users = ['a', 'b', 'c', 'd'].map((id) => ({
      id,
      token: `synthetic-${id}`,
      enabled: true,
      acceptingJobs: true,
    }));
    seed.accounts = [
      {
        id: 'opus',
        enabled: true,
        quotaTier: 3,
        subscriptionActive: true,
        quotaFixed: 100000,
        quotaPurchased: 0,
        v5UsagePercent: 80,
        quotaCheckedAt: now,
      },
    ];
    configureFairQuota(
      seed,
      seed.users.map((user) => user.id),
      { now },
    );
    syncFairQuota(seed, now);
    seed.jobs = [
      {
        id: 'legacy-completed',
        userToken: 'synthetic-a',
        memberId: 'a',
        status: 'done',
        imageId: 'inline-result',
        createdAt: new Date(now).toISOString(),
        completedAt: new Date(now).toISOString(),
        request: materializeMemberGeneration(normalized),
      },
    ];
    seed.images = [
      {
        id: 'inline-result',
        jobId: 'legacy-completed',
        token: 'synthetic-a',
        memberId: 'a',
        base64: result.toString('base64'),
        mimeType: 'image/png',
        byteLength: result.length,
        sha256: sha(result),
        fullPrompt: 'synthetic protected output prompt',
        createdAt: new Date(now).toISOString(),
      },
    ];
    await store.write(seed);
    await store.enableEventAuthority();
    const admission = createGenerationAdmission({
      store,
      now: () => now,
      env: {},
      artifactAvailable: async () => true,
      cleanupStaleActiveJobs: async () => {},
      payloadBudget: createGenerationPayloadBudget(() => store.activeGenerationPayloadBytes()),
    });
    const accepted = await admission({
      identity: { memberId: 'a' },
      source: 'member-generation',
      noCache: true,
      intent: generationIntent(
        'capacity-member-request',
        () => materializeMemberGeneration(normalized),
        { context: () => normalized.normalizationContext, noCache: true },
      ),
    });
    await admission({
      identity: { memberId: 'b' },
      source: 'member-generation',
      noCache: true,
      intent: generationIntent('capacity-vibe-request', () => materializeMemberGeneration(vibe), {
        context: () => vibe.normalizationContext,
        noCache: true,
      }),
    });
    store.sqlite.pragma('wal_checkpoint(TRUNCATE)');
    timings.fixtureMs = performance.now() - started;
    const tables = [
      'event_commits',
      'domain_events',
      'event_streams',
      'jobs',
      'images',
      'ledger',
      'generation_requests',
      'generation_contexts',
      'artifact_inputs',
      'generation_idempotency',
    ];
    const summaries = (db) =>
      Object.fromEntries(
        tables.map((table) => {
          const hash = createHash('sha256');
          let rows = 0;
          for (const row of db.prepare(`SELECT * FROM ${table} ORDER BY 1`).iterate()) {
            hash.update(JSON.stringify(row) + '\n');
            rows++;
          }
          return [table, { rows, sha256: hash.digest('hex') }];
        }),
      );
    const before = summaries(store.sqlite);
    for (const table of [
      'event_commits',
      'domain_events',
      'generation_requests',
      'generation_contexts',
      'artifact_inputs',
      'generation_idempotency',
    ])
      assert.ok(before[table].rows > 0, table);
    const sourceBytes = (await lstat(store.dbPath)).size;
    const preload = path.join(root, 'preload.mjs'),
      uploadMetrics = path.join(root, 'upload-metrics.json');
    const python = `
import importlib.util,json,os,resource,tarfile,time
from pathlib import Path
spec=importlib.util.spec_from_file_location('backup','/app/scripts/backup-production.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
root=Path(${JSON.stringify(root)})
config=Path('/etc/novelai-shared-gateway');config.mkdir()
for name,source in [('r2.env','r2.env'),('backup-encryption.key','backup.key')]:
    (config/name).write_bytes((root/source).read_bytes())
runtime=Path('/opt/novelai-shared-gateway/current');(runtime/'bin').mkdir(parents=True)
(runtime/'bin/node').symlink_to('/usr/local/bin/node')
package=runtime/'executor/.output/server/node_modules';package.mkdir(parents=True)
(package/'novelai-shared-gateway').symlink_to('/app',target_is_directory=True)
start=time.monotonic()
snapshot=b.backup(root/'live',root/'backups',{'config/gateway.env':root/'r2.env'})
snapshot_ms=(time.monotonic()-start)*1000
start=time.monotonic();b.upload_business_backup(snapshot);upload_ms=(time.monotonic()-start)*1000
(root/'snapshot-report.json').write_text(json.dumps({'snapshot':str(snapshot),'snapshotMs':snapshot_ms,'compressEncryptUploadVerifyMs':upload_ms,'pythonPeakRssBytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024}))
`;
    await run('python3', ['-c', python], {
      env: {
        ...process.env,
        NODE_OPTIONS: `--import=${preload}`,
        R2_TEST_METRICS: uploadMetrics,
        R2_TEST_ROOT: root,
      },
      windowsHide: true,
    });
    const snapshotReport = JSON.parse(await readFile(path.join(root, 'snapshot-report.json'))),
      snapshot = snapshotReport.snapshot;
    const archive = path.join(root, 'downloaded.tar.gz'),
      downloadStarted = performance.now();
    await fixture.call([
      'download',
      path.join(snapshot, 'r2-backup.json'),
      archive,
      fixture.envFile,
      fixture.keyFile,
    ]);
    timings.downloadDecryptMs = performance.now() - downloadStarted;
    const restored = path.join(root, 'restored');
    await mkdir(restored);
    const extractStarted = performance.now();
    await run('python3', [
      '-c',
      `import tarfile\nfrom pathlib import PurePosixPath\nbundle=tarfile.open(${JSON.stringify(archive)})\nfor member in bundle.getmembers():\n    assert not PurePosixPath(member.name).is_absolute() and '..' not in PurePosixPath(member.name).parts and (member.isfile() or member.isdir())\nbundle.extractall(${JSON.stringify(restored)})`,
    ]);
    const restoredSnapshot = path.join(restored, path.basename(snapshot));
    await run('python3', ['/app/scripts/backup-production.py', '--verify', restoredSnapshot]);
    timings.extractVerifyMs = performance.now() - extractStarted;
    const restoredDb = new Database(path.join(restoredSnapshot, 'data/library.sqlite'));
    t.after(() => restoredDb.close());
    assert.deepEqual(summaries(restoredDb), before);
    assert.deepEqual(
      JSON.parse(await readFile(path.join(restoredSnapshot, 'manifest.json'))),
      JSON.parse(await readFile(path.join(snapshot, 'manifest.json'))),
    );
    const replayStarted = performance.now(),
      replay = new Database(path.join(root, 'replayed.sqlite'));
    t.after(() => replay.close());
    migrateStorage(replay);
    copyProtectedFacts(restoredDb, replay);
    const progress = replayDomainEvents(restoredDb, replay, { project: applyEventProjection });
    replay.transaction(() => verifyEventProjections(replay))();
    assert.equal(replay.pragma('integrity_check', { simple: true }), 'ok');
    const replaySummaries = summaries(replay);
    for (const table of tables.filter((table) => !['jobs', 'images'].includes(table)))
      assert.deepEqual(replaySummaries[table], before[table], table);
    // Rebuilt query rows omit transient display fields and use fresh order_value; compare authoritative state and artifact descriptors.
    assert.deepEqual(
      replay
        .prepare('SELECT id,status,request_id,user_token,account_id,image_id FROM jobs ORDER BY id')
        .all(),
      restoredDb
        .prepare('SELECT id,status,request_id,user_token,account_id,image_id FROM jobs ORDER BY id')
        .all(),
    );
    const artifacts = (db) =>
      db
        .prepare('SELECT data FROM images ORDER BY id')
        .all()
        .map(({ data }) => {
          const image = JSON.parse(data);
          return Object.fromEntries(
            [
              'id',
              'file',
              'sha256',
              'byteLength',
              'mimeType',
              'r2',
              'artifactInputRef',
              'artifactInputSha256',
            ].map((key) => [key, image[key]]),
          );
        });
    assert.deepEqual(artifacts(replay), artifacts(restoredDb));
    const imageRow = replay.prepare('SELECT data FROM images WHERE id=?').get('inline-result');
    const restoredStore = new JsonStore(path.join(restoredSnapshot, 'data'));
    await restoredStore.init();
    t.after(() => restoredStore.close());
    const image = await restoredStore.findImage('inline-result');
    assert.ok(imageRow);
    assert.ok(
      await new ImageStorage(restoredStore.dataDir, null, async () => image).available(image),
    );
    assert.deepEqual(
      await new ImageStorage(restoredStore.dataDir, null, async () => image).read(image),
      result,
    );
    assert.equal(
      restoredStore.readGenerationRequest(accepted.requestId).nativeParameters.mask,
      mask.toString('base64'),
    );
    timings.copyReplayVerifyAndOpenMs = performance.now() - replayStarted;
    const upload = JSON.parse(await readFile(uploadMetrics)),
      receipt = JSON.parse(await readFile(path.join(snapshot, 'r2-backup.json')));
    const report = {
      fixture: {
        width: 512,
        height: 512,
        inputImageBytes: inputImage.length,
        maskBytes: mask.length,
        opaqueReferenceBytes: reference.length,
        inlineResultBytes: result.length,
        sourceSha256: process.env.BACKUP_CAPACITY_SHA,
      },
      sourceBytes,
      archiveBytes: upload.archiveBytes,
      encryptedBytes: receipt.byteLength,
      encryptionOverheadBytes: receipt.byteLength - upload.archiveBytes,
      nodeHarnessPeakRssBytes: process.resourceUsage().maxRSS * 1024,
      nodeUploadPeakRssBytes: upload.peakRssBytes,
      sampledTempPeakBytes: Math.max(
        sampledTempPeakBytes,
        upload.retainedLogicalBytes,
        await disk(root),
      ),
      tempSampleIntervalMs: 100,
      cgroupMemoryPeakBytes: Number((await readFile('/sys/fs/cgroup/memory.peak', 'utf8')).trim()),
      timings: {
        ...timings,
        snapshotMs: snapshotReport.snapshotMs,
        compressEncryptUploadVerifyMs: snapshotReport.compressEncryptUploadVerifyMs,
        totalMs: performance.now() - started,
      },
      pythonPeakRssBytes: snapshotReport.pythonPeakRssBytes,
      progress,
      summaries: before,
      replaySummaries,
      verified: [
        'manifest',
        'event facts',
        'command receipts',
        'stream projections',
        'jobs/images/ledger',
        'protected requests/contexts/artifacts',
        'idempotency',
        'inline artifact bytes',
      ],
      limits: {
        archiveBytes: 50 * 1024 * 1024,
        objectBytes: 64 * 1024 * 1024,
        remoteBytes: 500_000_000,
        keep: 7,
      },
    };
    await writeFile(path.join(root, 'report.json'), JSON.stringify(report, null, 2) + '\n');
    console.log(JSON.stringify(report));
  },
);
