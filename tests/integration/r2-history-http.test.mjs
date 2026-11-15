import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
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
import { describeStoredResult, publishStoredResult } from '../../server/storage/result-files.ts';

const png = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=',
  'base64',
);
const sha256 = createHash('sha256').update(png).digest('hex'),
  accountId = 'a'.repeat(32),
  bucket = 'synthetic-images',
  archiveUrl = 'https://dav.pikpak.test/archive/';
const ids = [
  'local',
  'cloud',
  'pikpak',
  'old_snapshot',
  'cold_corrupt',
  'corrupt',
  'missing',
  'unproven_snapshot',
];

// Synthetic source evidence, not a converter or an equivalent migration of the old seven rows.
// The old fixture had no original request/count/operation or PikPak pointer for old_snapshot.
async function historicalFixture(t) {
  const f = await childFixture(t, 'r2-history-http'),
    at = Date.now(),
    manifest = {
      sourceCommit: 'synthetic-archive-contract-v1',
      cutoffSeq: '1',
      cutoffAt: at,
      memberId: f.members[0].user_id,
      targets: [
        { kind: 'local', namespace: 'images', credentialRef: null },
        { kind: 'r2', namespace: `r2://${accountId}/${bucket}`, credentialRef: 'deployment:r2' },
        { kind: 'pikpak', namespace: archiveUrl, credentialRef: 'deployment:pikpak' },
      ],
      records: ids.map((id) => ({
        id,
        originalRequestId: `historical-request-${id}`,
        originalJobId: `job_${id}`,
        operation: 'generate',
        acceptedAt: 0,
        createdAt: 0,
        publishedAt: 0,
        completedAt: 0,
        sampleIndex: 0,
        requestedSampleCount: null,
        sha256,
        byteLength: png.length,
        replicas: [
          'local',
          ...(['local', 'missing'].includes(id) ? [] : ['r2']),
          ...(['pikpak', 'old_snapshot', 'cold_corrupt'].includes(id) ? ['pikpak'] : []),
        ].map((kind) => {
          let objectKey = `${id}.png`;
          if (kind === 'r2') objectKey = `images/${id}/${sha256}`;
          if (kind === 'pikpak') objectKey = `${sha256}.br`;
          return {
            kind,
            objectKey,
            encoding: kind === 'pikpak' ? 'br' : 'identity',
            uploadedAt: 0,
            verifiedAt: kind === 'pikpak' ? null : 0,
            deletedAt: null,
            lastErrorCode: null,
          };
        }),
      })),
    },
    manifestPath = path.join(f.directory, 'synthetic-archive.json'),
    manifestBytes = JSON.stringify(manifest),
    manifestSha = createHash('sha256').update(manifestBytes).digest('hex'),
    bootstrapPath = path.join(f.directory, 'bootstrap-selected.sqlite');
  await writeFile(manifestPath, manifestBytes, { flag: 'wx' });
  await rename(f.filename, bootstrapPath);
  const original = new Database(bootstrapPath, { readonly: true, fileMustExist: true }),
    db = new Database(f.filename),
    historyId = original.prepare('SELECT history_id FROM projection_state').get().history_id;
  try {
    original.pragma('query_only=ON');
    initializeRelationalStorage(db, historyId);
    const replayer = new RelationalCommitReplayer(db),
      sourceInputs = new RelationalInputStore(original),
      inputs = new RelationalInputStore(db);
    for (const { seq } of original
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all()) {
      db.transaction(() =>
        replayer.replayCommit(readRelationalCommit(original, seq), sourceInputs),
      )();
    }
    db.transaction(() =>
      restoreRelationalPrivateSnapshot(db, bootstrapPath, relationalSnapshotSha256(bootstrapPath)),
    )();
    await mkdir(path.join(f.dataDir, 'images'), { recursive: true });
    const descriptors = manifest.records.map((record) =>
      describeStoredResult(record.id, record.operation, { mimeType: 'image/png', buffer: png }),
    );
    for (const descriptor of descriptors) await publishStoredResult(f.dataDir, descriptor, png);
    db.transaction(() => {
      const origin = (originalId) => ({ sourceId: 'synthetic-archive', originalId }),
        event = (eventType, payload, memberId = null) => {
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
            occurredAt: at,
            policyVersion: 'historical-policy-unknown',
            pricingVersion: 'historical-price-unknown',
          };
        },
        targets = manifest.targets.map((target) => ({
          ...target,
          storageTargetId: `${target.kind}:${domainDigest(target.namespace)}`,
          origin: origin(`target:${target.kind}`),
        })),
        events = [
          event('ImportSourceRegistered', {
            sourceId: 'synthetic-archive',
            sourceSha256: manifestSha,
            sourceCommit: manifest.sourceCommit,
            cutoffSeq: manifest.cutoffSeq,
            cutoffAt: manifest.cutoffAt,
            manifestSha256: manifestSha,
            route: 'B', // Synthetic contract only; no real migration route is selected.
          }),
          ...targets.map((target) => event('OpeningStorageTargetRecorded', target)),
        ];
      for (const [index, record] of manifest.records.entries()) {
        const memberId = manifest.memberId,
          inputId = `historical-input-${record.id}`,
          descriptor = descriptors[index];
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
              origin: origin(inputId),
            },
            memberId,
          ),
          event(
            'OpeningRequestRecorded',
            {
              requestId: record.originalRequestId,
              memberId,
              inputId,
              contextId: null,
              source: 'synthetic-archive',
              operation: record.operation,
              model: null,
              sampleCount: record.requestedSampleCount,
              seedMode: 'unknown',
              confirmation: null,
              acceptedAt: record.acceptedAt,
              retiredAt: null,
              origin: origin(record.originalRequestId),
            },
            memberId,
          ),
          event(
            'OpeningJobRecorded',
            {
              jobId: record.originalJobId,
              requestId: record.originalRequestId,
              createdAt: record.createdAt,
              sampleIndex: record.sampleIndex,
              actualSeed: null,
              status: 'done',
              queueSeq: null,
              queueExpiresAt: null,
              deadlineAt: null,
              admissionEpoch: 0,
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
              origin: origin(record.originalJobId),
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
              mimeType: descriptor.mimeType,
              byteLength: record.byteLength,
              sha256: record.sha256,
              publishedAt: record.publishedAt,
              retiredAt: null,
              origin: origin(record.id),
            },
            memberId,
          ),
          ...record.replicas.map(({ kind, ...replica }) => {
            const target = targets.find((target) => target.kind === kind);
            return event(
              'OpeningReplicaRecorded',
              {
                artifactId: record.id,
                storageTargetId: target.storageTargetId,
                ...replica,
                origin: origin(`replica:${record.id}:${kind}`),
              },
              memberId,
            );
          }),
        );
      }
      replayer.appendCommit(
        {
          commandId: 'synthetic-archive-opening',
          fingerprint: domainDigest(events),
          actorKind: 'system',
          actorId: null,
          recordedAt: at,
          correlationId: null,
          causationId: null,
          expectedVersions: Object.fromEntries(events.map((event) => [event.streamId, '0'])),
          receipt: { archiveManifestSha256: manifestSha },
          events,
        },
        inputs,
      );
    })();
    const selectedPath = path.join(f.directory, 'historical-selected.sqlite');
    await db.backup(selectedPath);
    const selected = new Database(selectedPath);
    selected.pragma('journal_mode=DELETE');
    selected.close();
    const installed = await verifyAndInstallRelationalStorage(db, {
      sourcePath: selectedPath,
      sourceSha256: relationalSnapshotSha256(selectedPath),
      historyId,
      finalCommitSeq: db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get()
        .seq,
      replicas: { dataDir: f.dataDir },
    });
    assert.equal(installed.artifactsVerified, ids.length);
    assert.equal(db.prepare('SELECT status FROM projection_state').get().status, 'ready');
    db.pragma('journal_mode=DELETE');
  } finally {
    original.close();
    db.close();
  }
  assert.equal(
    createHash('sha256')
      .update(await readFile(manifestPath))
      .digest('hex'),
    manifestSha,
  );
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
        if (['pikpak','old_snapshot','cold_corrupt','unproven_snapshot'].includes(id))
          return new Response(null, {status:404});
        const bytes = Buffer.from('${png.toString('base64')}', 'base64');
        if (id === 'corrupt') bytes[0] ^= 1;
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
      wallets: query(f, 'SELECT * FROM wallets ORDER BY account_id,member_id'),
      jobs: query(f, 'SELECT id,status,request_id FROM generation_jobs ORDER BY id'),
      requests: query(f, 'SELECT id,sample_count FROM generation_requests ORDER BY id'),
    });
    const before = business(),
      bearer = { authorization: `Bearer ${f.members[0].api_key}` },
      ownerCookie = { cookie: cookies[0] },
      otherCookie = { cookie: cookies[1] };
    for (const id of ['local', 'cloud', 'pikpak', 'old_snapshot']) {
      const view = await request(`/api/jobs/job_${id}`, bearer);
      assert.equal(view.status, 200);
      const body = await view.json();
      assert.doesNotMatch(
        JSON.stringify(body),
        /cloudflarestorage\.com|synthetic-(pikpak-)?secret|dav\.pikpak\.test/,
      );
      assert.equal(body.cost, null, 'unknown historical fees never become a new zero quote');
      assert.equal(body.model, null);
      assert.equal(body.createdAt, new Date(0).toISOString());
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
    const unproven = await request('/api/jobs/job_unproven_snapshot/content', bearer);
    assert.equal(unproven.status, 503);
    await unproven.text();
    await writeFile(control, JSON.stringify({ corrupt: true }));
    for (const id of ['corrupt', 'cold_corrupt']) {
      const failure = await request(`/api/jobs/job_${id}/content`, bearer);
      assert.equal(failure.status, 503);
      assert.doesNotMatch(await failure.text(), /synthetic-(pikpak-)?secret|dav\.pikpak\.test/);
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
      'Ready=8; four source cases/16 exact-byte deliveries and access matrix; two corrupt sources; undeclared-PikPak rejection; zero business writes; graceful close/public replay verified.',
    );
    // Keep the old assertion until the API change is explicitly reviewed. Its known 503/500
    // difference is deliberately last, so it cannot mask the source/auth/content checks above.
    assert.equal(missing.status, 500, 'retain the existing local storage failure contract');
  },
);
