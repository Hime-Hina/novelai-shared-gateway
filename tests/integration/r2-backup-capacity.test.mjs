import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes, createHash } from 'node:crypto';
import { deflateSync } from 'node:zlib';
import { copyFile, mkdir, readFile, writeFile, readdir, lstat } from 'node:fs/promises';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { parseEnv, promisify } from 'node:util';
import Database from 'better-sqlite3';
import { getPatcher } from 'webdav';
import { backupFixture } from '../helpers/r2-backup-fixture.mjs';
import {
  JsonStore,
  applyEventProjection,
  verifyEventProjections,
} from '../../server/storage/sqlite-store.ts';
import { migrateStorage } from '../../server/storage/sqlite-migrations.ts';
import {
  copyProtectedFacts,
  readProjectedFairQuota,
  replayDomainEvents,
} from '../../server/events/event-store.ts';
import {
  normalizeMemberGeneration,
  materializeMemberGeneration,
} from '../../server/generation/member-contract.ts';
import { createGenerationAdmission, generationIntent } from '../../server/generation/admission.ts';
import { createGenerationPayloadBudget } from '../../server/generation/batch-requests.ts';
import {
  failGenerationCommand,
  reserveQueuedGeneration,
} from '../../server/generation/execution.ts';
import { configureFairQuota, syncFairQuota } from '../../server/quota/fair-share.ts';
import { crc32 } from '../../server/protocols/native-image.ts';
import {
  ImageStorage,
  R2Storage,
  fileChecksums,
  r2Config,
} from '../../server/storage/r2-storage.ts';
import { PikPakStorage } from '../../server/storage/pikpak-storage.ts';

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
    await lstat('/.dockerenv');
    const root = process.env.BACKUP_CAPACITY_OUTPUT;
    await mkdir(root, { recursive: false });
    const disk = async (dir) => {
      let total = 0;
      for (const name of await readdir(dir)) {
        const file = path.join(dir, name),
          info = await lstat(file);
        if (!info.isSymbolicLink()) {
          total += info.isDirectory() ? await disk(file) : info.size;
        }
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
    const png = (side) => {
      const chunk = (name, bytes) => {
        const head = Buffer.alloc(8),
          tail = Buffer.alloc(4);
        head.writeUInt32BE(bytes.length);
        head.write(name, 4);
        tail.writeUInt32BE(crc32(Buffer.concat([Buffer.from(name), bytes])));
        return Buffer.concat([head, bytes, tail]);
      };
      const head = Buffer.alloc(13);
      head.writeUInt32BE(side);
      head.writeUInt32BE(side, 4);
      head[8] = 8;
      head[9] = 6;
      const stride = side * 4 + 1;
      const rows = randomBytes(stride * side);
      for (let i = 0; i < side; i++) {
        rows[i * stride] = 0;
      }
      return Buffer.concat([
        Buffer.from('89504e470d0a1a0a', 'hex'),
        chunk('IHDR', head),
        chunk('IDAT', deflateSync(rows, { level: 1 })),
        chunk('IEND', Buffer.alloc(0)),
      ]);
    };
    const side = 1408,
      result = png(side),
      reference = randomBytes(1024 * 1024);
    const infill = (seed) =>
      normalizeMemberGeneration({
        operation: 'generate',
        action: 'infill',
        model: 'nai-diffusion-4-5-full',
        input: 'synthetic private long prompt '.repeat(100),
        parameters: {
          width: side,
          height: side,
          steps: 23,
          seed,
          image: png(side).toString('base64'),
          mask: png(side).toString('base64'),
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
      at = new Date(now).toISOString(),
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
        createdAt: at,
        completedAt: at,
        request: materializeMemberGeneration(infill(7)),
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
        createdAt: at,
      },
    ];
    const remote = new R2Storage(r2Config(parseEnv(await readFile(fixture.envFile, 'utf8'))));
    const pikpak = new PikPakStorage({
      url: 'https://dav.pikpak.test/archive/',
      username: 'synthetic-user',
      password: 'synthetic-password',
    });
    const hotObjects = new Map(),
      coldObjects = new Map(),
      reads = [];
    getPatcher().patch('fetch', (...args) => globalThis.fetch(...args));
    t.after(() => getPatcher().restore('fetch'));
    t.mock.method(globalThis, 'fetch', async (address, options) => {
      const url = new URL(address);
      if (url.href.startsWith(pikpak.config.url)) {
        if (options.method === 'PUT') {
          coldObjects.set(url.href, Buffer.from(options.body));
          return new Response(null, { status: 204 });
        }
        assert.equal(options.method, 'GET');
        reads.push(['pikpak', url.href]);
        const bytes = coldObjects.get(url.href);
        return bytes ? new Response(bytes) : new Response(null, { status: 404 });
      }
      assert.equal(url.origin, `https://${remote.config.accountId}.r2.cloudflarestorage.com`);
      assert.equal(options.method, 'GET');
      const key = decodeURIComponent(url.pathname.split('/').slice(2).join('/'));
      reads.push(['r2', key]);
      const bytes = hotObjects.get(key);
      return bytes ? new Response(bytes) : new Response(null, { status: 404 });
    });
    const external = new Map();
    await mkdir(path.join(live, 'images'));
    for (const id of ['local', 'hot', 'cold', 'legacy-r2']) {
      const bytes = png(32);
      const image = {
        id,
        file: `images/${id}.png`,
        token: 'synthetic-a',
        memberId: 'a',
        mimeType: 'image/png',
        sha256: sha(bytes),
        byteLength: bytes.length,
        createdAt: at,
      };
      if (id === 'local') {
        await writeFile(path.join(live, image.file), bytes);
      } else {
        image.r2 = {
          bucket: remote.config.bucket,
          key: `images/${id}/${image.sha256}`,
          sha256: image.sha256,
          byteLength: image.byteLength,
          verifiedAt: at,
          localRemovedAt: at,
        };
        if (id === 'hot') {
          hotObjects.set(image.r2.key, bytes);
        } else {
          await pikpak.put(image.r2, bytes);
          if (id === 'cold') {
            image.r2.pikpak = { url: pikpak.config.url, uploadedAt: at };
            image.r2.removedAt = at;
          }
        }
      }
      seed.images.push(image);
      external.set(id, { image, bytes });
    }
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
    // Four independent PNG pairs (including the opening input) and the inline result
    // cross 64 MiB after gzip while each normalized request stays within production limits.
    const admitted = [];
    for (const [index, memberId] of ['a', 'b', 'c'].entries()) {
      const normalized = infill(8 + index),
        request = materializeMemberGeneration(normalized);
      const accepted = await admission({
        identity: { memberId },
        source: 'member-generation',
        noCache: true,
        intent: generationIntent(`capacity-member-request-${index}`, () => request, {
          context: () => normalized.normalizationContext,
          noCache: true,
        }),
      });
      admitted.push({
        ...accepted,
        maskSha256: sha(Buffer.from(request.nativeParameters.mask, 'base64')),
      });
    }
    const vibeRequest = await admission({
      identity: { memberId: 'd' },
      source: 'member-generation',
      noCache: true,
      intent: generationIntent('capacity-vibe-request', () => materializeMemberGeneration(vibe), {
        context: () => vibe.normalizationContext,
        noCache: true,
      }),
    });
    const diagnostic = 'synthetic private diagnostic';
    const reserve = (jobId, offset) =>
      reserveQueuedGeneration(store, jobId, {
        now: () => now + offset,
        env: {},
        dispatchEnabled: () => true,
        physicalAccountLoad: () => 0,
      });
    const failure = await reserve(admitted[0].jobIds[0], 1000);
    assert.equal(failure.job?.fairCharge?.status, 'reserved');
    await failGenerationCommand(store, failure, new Error(diagnostic), {
      now: () => now + 2000,
      publicErrorMessage: () => 'synthetic generation failure',
      errorDetailMessage: (error) => error.message,
    });
    const cooldownUntil = Date.parse(store.selectItemById('accounts', 'opus').cooldownUntil);
    const pending = await reserve(admitted[1].jobIds[0], cooldownUntil - now + 1);
    assert.equal(pending.job?.fairCharge?.status, 'reserved');
    store.sqlite.pragma('wal_checkpoint(TRUNCATE)');
    timings.fixtureMs = performance.now() - started;
    const tables = [
      'event_commits',
      'domain_events',
      'event_streams',
      'jobs',
      'images',
      'ledger',
      'fair_quota_settlements',
      'generation_requests',
      'generation_contexts',
      'artifact_inputs',
      'generation_diagnostics',
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
    for (const table of tables) {
      assert.ok(before[table].rows > 0, table);
    }
    const quota = readProjectedFairQuota(store.sqlite);
    assert.equal(quota.reservations[pending.job.id].status, 'reserved');
    assert.equal(
      store.sqlite
        .prepare('SELECT detail FROM generation_diagnostics WHERE job_id=?')
        .get(failure.job.id).detail,
      diagnostic,
    );
    const sourceBytes = (await lstat(store.dbPath)).size;
    const uploadMetrics = path.join(root, 'upload-metrics.json'),
      downloadMetrics = path.join(root, 'download-metrics.json');
    const python = `
import importlib.util,json,time
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
peak_kib=next(int(line.split()[1]) for line in Path('/proc/self/status').read_text().splitlines() if line.startswith('VmHWM:'))
(root/'snapshot-report.json').write_text(json.dumps({'snapshot':str(snapshot),'snapshotMs':snapshot_ms,'compressEncryptUploadVerifyMs':upload_ms,'pythonPeakRssBytes':peak_kib*1024}))
`;
    await run('python3', ['-c', python], {
      env: {
        ...process.env,
        NODE_OPTIONS: `--import=${fixture.preload}`,
        R2_TEST_METRICS: uploadMetrics,
        R2_TEST_ROOT: root,
      },
      windowsHide: true,
    });
    const snapshotReport = JSON.parse(await readFile(path.join(root, 'snapshot-report.json'))),
      snapshot = snapshotReport.snapshot;
    const archive = path.join(root, 'downloaded.tar.gz'),
      downloadStarted = performance.now();
    await fixture.call(
      [
        'download',
        path.join(snapshot, 'r2-backup.json'),
        archive,
        fixture.envFile,
        fixture.keyFile,
      ],
      { R2_TEST_METRICS: downloadMetrics, R2_TEST_ROOT: root },
    );
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
    restoredDb.transaction(() => verifyEventProjections(restoredDb))();
    assert.deepEqual(readProjectedFairQuota(restoredDb), quota);
    const manifest = JSON.parse(await readFile(path.join(restoredSnapshot, 'manifest.json')));
    assert.deepEqual(manifest, JSON.parse(await readFile(path.join(snapshot, 'manifest.json'))));
    assert.equal(manifest.imagesIncluded, false);
    const replayStarted = performance.now(),
      replay = new Database(path.join(root, 'replayed.sqlite'));
    t.after(() => replay.close());
    migrateStorage(replay);
    copyProtectedFacts(restoredDb, replay);
    const progress = replayDomainEvents(restoredDb, replay, { project: applyEventProjection });
    replay.transaction(() => verifyEventProjections(replay))();
    assert.equal(replay.pragma('integrity_check', { simple: true }), 'ok');
    assert.deepEqual(readProjectedFairQuota(replay), quota);
    const replaySummaries = summaries(replay);
    for (const table of tables.filter((table) => !['jobs', 'images', 'ledger'].includes(table))) {
      assert.deepEqual(replaySummaries[table], before[table], table);
    }
    // Rebuilt query rows use fresh order_value and may omit transient display fields.
    const ledger = (db) =>
      db
        .prepare('SELECT * FROM ledger ORDER BY id')
        .all()
        .map(({ order_value, data, ...row }) => ({ ...row, data: JSON.parse(data) }));
    assert.deepEqual(ledger(replay), ledger(restoredDb));
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
    const restoredStore = new JsonStore(path.join(restoredSnapshot, 'data'));
    await restoredStore.init();
    t.after(() => restoredStore.close());
    const storage = new ImageStorage(
      restoredStore.dataDir,
      remote,
      (id) => restoredStore.findImage(id),
      pikpak,
    );
    const inline = await restoredStore.findImage('inline-result');
    assert.ok(await storage.available(inline));
    assert.deepEqual(await storage.read(inline), result);
    const local = external.get('local').image;
    const localPath = path.join(restoredStore.dataDir, local.file);
    await assert.rejects(lstat(localPath), { code: 'ENOENT' });
    // The unarchived original must be retained separately from the business backup.
    await mkdir(path.dirname(localPath), { recursive: true });
    await copyFile(path.join(live, local.file), localPath);
    for (const [id, { bytes }] of external) {
      const image = await restoredStore.findImage(id);
      assert.ok(await storage.available(image), id);
      assert.deepEqual(await storage.read(image), bytes, id);
    }
    const hot = external.get('hot').image.r2;
    const cold = external.get('cold').image.r2;
    const legacy = external.get('legacy-r2').image.r2;
    assert.deepEqual(reads, [
      ['r2', hot.key],
      ['pikpak', `${pikpak.config.url}${cold.sha256}.br`],
      ['r2', legacy.key],
      ['pikpak', `${pikpak.config.url}${legacy.sha256}.br`],
    ]);
    for (const { requestId, maskSha256 } of admitted) {
      const request = restoredStore.readGenerationRequest(requestId);
      assert.equal(sha(Buffer.from(request.nativeParameters.mask, 'base64')), maskSha256);
    }
    assert.equal(
      restoredStore.readGenerationRequest(vibeRequest.requestId).nativeParameters
        .reference_image_multiple[0],
      reference.toString('base64'),
    );
    timings.copyReplayVerifyAndOpenMs = performance.now() - replayStarted;
    const upload = JSON.parse(await readFile(uploadMetrics)),
      download = JSON.parse(await readFile(downloadMetrics)),
      receipt = JSON.parse(await readFile(path.join(snapshot, 'r2-backup.json')));
    const restoredArchive = await fileChecksums(archive);
    assert.ok(upload.archiveBytes > 64 * 1024 ** 2);
    assert.equal(download.archiveBytes, upload.archiveBytes);
    assert.equal(restoredArchive.byteLength, upload.archiveBytes);
    assert.equal(receipt.byteLength, upload.archiveBytes + 36);
    assert.equal((await lstat(fixture.encode(receipt.key))).size, receipt.byteLength);
    const report = {
      fixture: {
        infillWidth: side,
        infillHeight: side,
        infillInputPairs: admitted.length + 1,
        opaqueReferenceBytes: reference.length,
        inlineResultBytes: result.length,
        sourceSha256: process.env.BACKUP_CAPACITY_SHA,
      },
      sourceBytes,
      snapshotDbSha256: manifest.files['data/library.sqlite'],
      archiveBytes: upload.archiveBytes,
      archiveSha256: restoredArchive.sha256,
      encryptedBytes: receipt.byteLength,
      encryptedSha256: receipt.sha256,
      encryptionOverheadBytes: receipt.byteLength - upload.archiveBytes,
      nodeHarnessPeakRssBytes: process.resourceUsage().maxRSS * 1024,
      nodeUploadPeakRssBytes: upload.peakRssBytes,
      nodeDownloadPeakRssBytes: download.peakRssBytes,
      subprocessRssSource: 'Linux VmHWM',
      sampledTempPeakBytes: Math.max(
        sampledTempPeakBytes,
        upload.retainedLogicalBytes,
        download.retainedLogicalBytes,
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
        'jobs/images/ledger/settlements',
        'wallets and active reservation',
        'protected requests/contexts/artifacts/diagnostics',
        'idempotency',
        'inline, independently retained local, hot R2 and cold PikPak original bytes',
        'legacy R2 descriptor SHA-256 fallback to PikPak',
      ],
      limits: {
        singlePutBytes: 5 * 1024 ** 3 - 5 * 1024 ** 2,
        keep: 7,
      },
    };
    await writeFile(path.join(root, 'report.json'), JSON.stringify(report, null, 2) + '\n');
    console.log(JSON.stringify(report));
  },
);
