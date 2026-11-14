import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { readFileSync, readdirSync } from 'node:fs';
import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');

async function archiveTool(bundle, directory) {
  const script = path.join(bundle, 'scripts/audit-legacy-archive.mjs'),
    env = Object.fromEntries(
      ['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE']
        .filter((key) => process.env[key])
        .map((key) => [key, process.env[key]]),
    ),
    run = (args) =>
      spawnSync(process.execPath, args, {
        cwd: directory,
        env,
        encoding: 'utf8',
        timeout: 15000,
        windowsHide: true,
      });
  const manifest = JSON.parse(await readFile(path.join(bundle, 'LEGACY_TOOLS.json'), 'utf8'));
  assert.equal(manifest.sourceCommit, '3caba3dfa0b5d9f582acabfc02e36e7db4af038f');
  const verified = run([script, '--verify-package']);
  assert.equal(verified.status, 0, verified.stderr);
  return { script, env, run };
}

// These two sources belonged to online legacy-inline upgrade. D4 keeps their protected
// input proof in the independent, verified old tool, never in the ordinary gateway.
export async function verifyLegacyInlineArchive(t, bundle, directory) {
  const { script, env, run } = await archiveTool(bundle, directory);
  const sourceDirectory = path.join(directory, 'inline-source');
  const created = spawnSync(
    process.execPath,
    ['--input-type=module', '-', bundle, sourceDirectory],
    {
      cwd: directory,
      env,
      encoding: 'utf8',
      timeout: 15000,
      windowsHide: true,
      input: `
      import { createHash } from 'node:crypto';
      import { createRequire } from 'node:module';
      import path from 'node:path';
      import { pathToFileURL } from 'node:url';
      globalThis.fetch = () => { throw new Error('legacy fixture denied network before IO'); };
      const [bundle, sourceDirectory] = process.argv.slice(2);
      const { JsonStore } = await import(pathToFileURL(path.join(bundle, 'server/storage/sqlite-store.ts')).href);
      const { recordDomainEvent } = await import(pathToFileURL(path.join(bundle, 'server/events/domain-events.ts')).href);
      const { webJobRequestSummary } = await import(pathToFileURL(path.join(bundle, 'server/generation/batch-requests.ts')).href);
      const require = createRequire(path.join(bundle, 'package.json')), Database = require('better-sqlite3');
      const store = new JsonStore(sourceDirectory);
      await store.init();
      try {
        await store.write({ settings: store.db.settings, accounts: [], cards: [], ledger: [], jobs: [], images: [],
          users: [{ id: 'inline-member', token: 'synthetic-inline-private-key', enabled: true }] });
        await store.enableEventAuthority();
        for (const [index, source] of ['direct', 'openai'].entries()) {
          const request = { model: 'nai-diffusion-4-5-full', width: 1024, height: 1024, steps: 23,
            seed: 801 + index, prompt: 'legacy ' + source, tag: 'legacy ' + source, sampler: 'k_euler_ancestral' };
          const id = 'legacy-' + source, inputRef = 'legacy-input:' + id, createdAt = new Date().toISOString();
          const payload = JSON.stringify({ legacyInline: true, request });
          store.sqlite.prepare('INSERT INTO generation_requests(id,member_id,source,operation,created_at,payload_bytes,payload) VALUES(?,?,?,?,?,?,?)')
            .run(inputRef, 'inline-member', source, 'generate', createdAt, Buffer.byteLength(payload), payload);
          await store.update(db => {
            const job = { id, source, userToken: 'synthetic-inline-private-key', status: 'queued', request,
              queueSeq: store.nextQueueSequence(), queueExpiresAt: new Date(Date.now() + 600000).toISOString(), createdAt };
            db.jobs.push(job);
            const { userToken, ...facts } = job;
            recordDomainEvent(db, { eventType: 'OpeningStateRecorded', streamId: 'request:legacy-' + id,
              payload: { state: { request: { requestId: null, memberId: 'inline-member', source, inputRef,
                inputSha256: createHash('sha256').update(payload).digest('hex'), legacy: true },
                jobs: { [id]: { ...facts, memberId: 'inline-member', request: webJobRequestSummary(request),
                  legacyInputRef: inputRef, legacyInlineInput: true } }, artifacts: {}, aliases: [], freeUsage: {} } } });
          }, { immediate: true, dirtyRows: { jobs: [id] } });
        }
      } finally { await store.close(); }
      const sealed = new Database(path.join(sourceDirectory, 'library.sqlite'));
      sealed.pragma('journal_mode=DELETE'); sealed.close();
    `,
    },
  );
  assert.equal(created.status, 0, created.stderr);
  const filename = path.join(sourceDirectory, 'library.sqlite'),
    before = await readFile(filename),
    entries = await readdir(sourceDirectory);
  for (const [index, source] of ['direct', 'openai'].entries()) {
    await t.test(`原 ${source} inline 的身份、SHA 和完整输入只经独立旧工具恢复`, async () => {
      const id = 'legacy-input:legacy-' + source,
        output = path.join(directory, `inline-${source}.json`),
        result = run([
          script,
          '--source=' + filename,
          '--source-sha256=' + sha256(before),
          '--request=' + id,
          '--output=' + output,
        ]);
      assert.equal(result.status, 0, result.stderr);
      const report = JSON.parse(result.stdout),
        bytes = await readFile(output),
        original = JSON.stringify({
          legacyInline: true,
          request: {
            model: 'nai-diffusion-4-5-full',
            width: 1024,
            height: 1024,
            steps: 23,
            seed: 801 + index,
            prompt: 'legacy ' + source,
            tag: 'legacy ' + source,
            sampler: 'k_euler_ancestral',
          },
        });
      assert.equal(bytes.toString(), original);
      assert.equal(report.lookup.id, id);
      assert.equal(report.lookup.memberId, 'inline-member');
      assert.equal(report.lookup.source, source);
      assert.equal(report.lookup.operation, 'generate');
      assert.equal(report.lookup.sha256, sha256(bytes));
      assert.equal(report.lookup.byteLength, Buffer.byteLength(original));
      assert.equal(report.protectedInputs, 2);
      assert.equal(report.networkCalls, 0);
      assert.equal(report.dispatches, 0);
      assert.equal(result.stdout.includes('legacy ' + source), false);
      assert.equal(result.stdout.includes('synthetic-inline-private-key'), false);
      assert.deepEqual(await readFile(filename), before);
      assert.deepEqual(await readdir(sourceDirectory), entries);
    });
  }
  await t.test(
    '普通新 child 拒绝同一旧 inline 库，原文件不变且无加载旧 owner/网络/监听',
    async () => {
      const guard = `
      import { registerHooks, syncBuiltinESMExports } from 'node:module';
      import net from 'node:net';
      let fetches = 0, listens = 0;
      registerHooks({load(url, context, next) {
        if (/\\/server\\/storage\\/(?:sqlite-store|sqlite-migrations)\\.(?:ts|js)(?:[?#]|$)/.test(url))
          throw new Error('unexpected legacy runtime load');
        return next(url, context);
      }});
      globalThis.fetch = () => { fetches++; throw new Error('denied network before IO'); };
      net.Server.prototype.listen = () => { listens++; throw new Error('denied listener before IO'); };
      syncBuiltinESMExports();
      process.on('exit', () => console.error('INLINE-REFUSAL ' + JSON.stringify({fetches, listens})));
    `;
      const rejected = spawnSync(
        process.execPath,
        ['--import', `data:text/javascript,${encodeURIComponent(guard)}`, 'server/index.ts'],
        {
          cwd: fileURLToPath(new URL('../../', import.meta.url)),
          env: {
            ...env,
            DATA_DIR: sourceDirectory,
            ADMIN_TOKEN: 'synthetic-inline-admin-01234567890123456789',
            HOST: '127.0.0.1',
            PORT: '0',
            NOVELAI_API_URL: 'http://127.0.0.1:1',
            NOVELAI_ACCOUNT_API_URL: 'http://127.0.0.1:1',
          },
          encoding: 'utf8',
          timeout: 15000,
          windowsHide: true,
        },
      );
      assert.notEqual(rejected.status, 0);
      assert.match(
        rejected.stderr,
        /Error: Unsupported relational database format or schema version\./,
      );
      assert.match(rejected.stderr, /INLINE-REFUSAL \{"fetches":0,"listens":0\}/);
      assert.equal(rejected.stdout.includes('Nai2API listening'), false);
      assert.deepEqual(await readFile(filename), before);
      assert.deepEqual(await readdir(sourceDirectory), entries);
    },
  );
}

// 原 Native 历史由同一固定旧包解释；缺失的父、context、诊断 ID 和图片字段不补成新模型。
export async function verifyLegacyNativeArchive(t, bundle, directory) {
  const { script, env, run } = await archiveTool(bundle, directory);
  await t.test('原 Native web902、私有失败及独立 PNG 沿封存来源精确归档', async (subtest) => {
    const webPayload =
      '{"legacyInline":true,"request":{"tag":"restart queued request","prompt":"restart queued request","artist":"","negative":"","model":"nai-diffusion-4-5-full","width":1024,"height":1024,"steps":28,"seed":902,"sampler":"k_euler_ancestral","scale":6,"cfg":0,"noiseSchedule":"karras"}}';
    const png = Buffer.from(
      'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=',
      'base64',
    );
    assert.equal(Buffer.byteLength(webPayload), 279);
    assert.equal(
      sha256(webPayload),
      '6a103a29b15b35c4b9014d70e7e7f18a9ab4b921c7ba6960635436d7eaa84787',
    );
    assert.equal(png.length, 68);
    assert.equal(sha256(png), '63ef318d96b5d0d0ceba6e04a4e622b1158335cdc67c49e27839132c6f655058');
    const body = {
      input: 'a small blue paper boat',
      model: 'nai-diffusion-5-full',
      action: 'generate',
      parameters: {
        width: 1024,
        height: 1024,
        steps: 23,
        n_samples: 1,
        seed: 7,
        stream: 'msgpack',
      },
    };
    const sourceDirectory = path.join(directory, 'native-history-source'),
      legacyKey = 'synthetic-legacy-private-error',
      privateMarker = 'synthetic-private-generation-diagnostic';
    const made = spawnSync(
      process.execPath,
      ['--input-type=module', '-', bundle, sourceDirectory],
      {
        cwd: directory,
        env,
        encoding: 'utf8',
        timeout: 15000,
        windowsHide: true,
        input: `
          import { mkdir, writeFile } from 'node:fs/promises';
          import { createHash, randomUUID } from 'node:crypto';
          import { createRequire } from 'node:module';
          import path from 'node:path';
          import { pathToFileURL } from 'node:url';
          globalThis.fetch = () => { throw new Error('legacy Native fixture denied network before IO'); };
          const [bundle, sourceDirectory] = process.argv.slice(2);
          const { JsonStore, hashObject } = await import(pathToFileURL(path.join(bundle, 'server/storage/sqlite-store.ts')).href);
          const { parseNativeImageRequest } = await import(pathToFileURL(path.join(bundle, 'server/protocols/native-image.ts')).href);
          const { generationRequestFingerprint } = await import(pathToFileURL(path.join(bundle, 'server/generation/request-fingerprint.ts')).href);
          const { recordDomainEvent } = await import(pathToFileURL(path.join(bundle, 'server/events/domain-events.ts')).href);
          const { webJobRequestSummary } = await import(pathToFileURL(path.join(bundle, 'server/generation/batch-requests.ts')).href);
          const require = createRequire(path.join(bundle, 'package.json')), Database = require('better-sqlite3');
          const digest = value => createHash('sha256').update(value).digest('hex');
          const memberId = randomUUID(), accountId = randomUUID(), key = 'synthetic-native-archive-member-key';
          const nativeRequest = parseNativeImageRequest(${JSON.stringify(body)});
          const error = "ENOENT: no such file or directory, open '" + path.join(sourceDirectory, ${JSON.stringify(privateMarker)}) + "'";
          const png = Buffer.from(${JSON.stringify(png.toString('base64'))}, 'base64');
          const webPayload = ${JSON.stringify(webPayload)}, webRequest = JSON.parse(webPayload).request;
          const store = new JsonStore(sourceDirectory);
          await store.init();
          try {
            await mkdir(path.join(sourceDirectory, 'images'), { recursive: true });
            await writeFile(path.join(sourceDirectory, 'images/img-access-owner.png'), png);
            await store.write({ settings: store.db.settings, cards: [], ledger: [],
              accounts: [{ id: accountId, token: 'pst-synthetic-native-archive-only', enabled: false }],
              users: [{ id: memberId, token: key, enabled: true }],
              images: [{ id: 'img-access-owner', token: key, mimeType: 'image/png', file: 'images/img-access-owner.png', byteLength: png.length, sha256: digest(png) }],
              jobs: [{ id: 'job-access-owner', userToken: key, status: 'done', imageId: 'img-access-owner', createdAt: new Date().toISOString() },
                { id: 'job-legacy-private-error', source: 'novelai-native', userToken: key, accountId, status: 'failed', request: nativeRequest,
                  error, errorStatus: 500, errorDetail: 'Error: ' + error + '\\n    at synthetic-history',
                  createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(), completedAt: new Date().toISOString() }] });
            store.persistGenerationIdempotency({ member_id: memberId, operation: 'generate',
              key_hash: hashObject(${JSON.stringify(legacyKey)}), fingerprint: generationRequestFingerprint(nativeRequest, { noCache: true }),
              normalization_context: '{}', job_id: 'job-legacy-private-error', request_id: null, created_at: new Date().toISOString() });
            await store.enableEventAuthority();
            const createdAt = new Date().toISOString(), inputRef = 'legacy-input:job-restart-legacy';
            store.sqlite.prepare('INSERT INTO generation_requests(id,member_id,source,operation,created_at,payload_bytes,payload) VALUES(?,?,?,?,?,?,?)')
              .run(inputRef, memberId, 'web', 'generate', createdAt, Buffer.byteLength(webPayload), webPayload);
            await store.update(db => {
              const user = db.users.find(user => user.id === memberId);
              const job = { id: 'job-restart-legacy', source: 'web', status: 'queued', userToken: key,
                createdAt, updatedAt: createdAt, jobAdmissionVersion: user.jobAdmissionVersion || 0,
                cost: 0, imageId: '', queueExpiresAt: new Date(Date.now() + 60000).toISOString(),
                queueSeq: store.nextQueueSequence(), request: webRequest };
              db.jobs.unshift(job);
              const { userToken, ...fact } = job;
              recordDomainEvent(db, { eventType: 'OpeningStateRecorded', streamId: 'request:legacy-' + job.id,
                payload: { state: { request: { requestId: null, memberId, source: 'web', operation: 'generate', createdAt,
                  inputRef, inputSha256: digest(webPayload), legacy: true },
                  jobs: { [job.id]: { ...fact, memberId, request: webJobRequestSummary(job.request), legacyInputRef: inputRef, legacyInlineInput: true } },
                  artifacts: {}, aliases: [], freeUsage: {} } } });
            }, { immediate: true, dirtyRows: { jobs: ['job-restart-legacy'] } });
            store.sqlite.prepare('UPDATE jobs SET order_value=-1000 WHERE id=?').run('job-restart-legacy');
          } finally { await store.close(); }
          const sealed = new Database(path.join(sourceDirectory, 'library.sqlite'));
          sealed.pragma('journal_mode=DELETE');
          sealed.close();
        `,
      },
    );
    assert.equal(made.status, 0, made.stderr);
    const filename = path.join(sourceDirectory, 'library.sqlite'),
      before = await readFile(filename),
      entries = await readdir(sourceDirectory),
      imagePath = path.join(sourceDirectory, 'images/img-access-owner.png');
    subtest.diagnostic(`sealed original Native source SHA256 ${sha256(before)}`);
    const require = createRequire(path.join(bundle, 'package.json')),
      Database = require('better-sqlite3'),
      original = new Database(filename, { readonly: true });
    let memberId,
      failed,
      done,
      queued,
      queuedOrder,
      diagnostic,
      alias,
      context,
      opening,
      failedPayload;
    try {
      memberId = original.prepare('SELECT id FROM users').get().id;
      const readJob = (id) =>
        JSON.parse(original.prepare('SELECT data FROM jobs WHERE id=?').get(id).data);
      failed = readJob('job-legacy-private-error');
      done = readJob('job-access-owner');
      queued = readJob('job-restart-legacy');
      queuedOrder = original
        .prepare('SELECT CAST(order_value AS TEXT) AS originalOrder FROM jobs WHERE id=?')
        .get(queued.id).originalOrder;
      assert.equal(failed.accountId, original.prepare('SELECT id FROM accounts').get().id);
      diagnostic = original
        .prepare('SELECT * FROM generation_diagnostics WHERE job_id=?')
        .get(failed.id);
      alias = original
        .prepare('SELECT * FROM generation_idempotency WHERE job_id=?')
        .get(failed.id);
      context = original.prepare('SELECT * FROM generation_contexts').get();
      failedPayload = original
        .prepare('SELECT payload FROM generation_requests WHERE id=?')
        .get('legacy-input:' + failed.id).payload;
      opening = original
        .prepare(
          "SELECT c.command_id,c.receipt,CAST(c.commit_seq AS TEXT) AS commit_seq,e.event_id FROM domain_events e JOIN event_commits c USING(commit_seq) WHERE e.stream_id=? AND e.event_type='OpeningStateRecorded' ORDER BY e.commit_seq,e.event_index LIMIT 1",
        )
        .get('request:legacy-' + failed.id);
      const web = JSON.parse(
        original
          .prepare(
            "SELECT payload FROM domain_events WHERE stream_id=? AND event_type='OpeningStateRecorded' ORDER BY commit_seq,event_index LIMIT 1",
          )
          .get('request:legacy-' + queued.id).payload,
      ).state;
      assert.equal(web.jobs[queued.id].queueSeq, queued.queueSeq);
      assert.equal(web.request.inputSha256, sha256(webPayload));
      assert.equal(web.request.requestId, null);
      assert.equal(web.jobs[queued.id].legacyInlineInput, true);
      assert.equal(original.prepare('SELECT COUNT(*) AS n FROM generation_requests').get().n, 2);
      assert.equal(original.prepare('SELECT COUNT(*) AS n FROM generation_contexts').get().n, 1);
      assert.equal(original.prepare('SELECT COUNT(*) AS n FROM artifact_inputs').get().n, 0);
    } finally {
      original.close();
    }
    const error = `ENOENT: no such file or directory, open '${path.join(sourceDirectory, privateMarker)}'`,
      detail = `Error: ${error}\n    at synthetic-history`;
    assert.equal(diagnostic.detail, detail);
    assert.equal(diagnostic.detail_sha256, sha256(detail));
    assert.equal(alias.member_id, memberId);
    assert.equal(alias.operation, 'generate');
    assert.equal(alias.key_hash, sha256(JSON.stringify(legacyKey)));
    assert.equal(alias.normalization_context, '{}');
    assert.equal(alias.request_id, null);
    assert.equal(context.payload, '{}');
    const { parseNativeImageRequest } = await import(
      pathToFileURL(path.join(bundle, 'server/protocols/native-image.ts')).href
    );
    const { generationRequestFingerprint } = await import(
      pathToFileURL(path.join(bundle, 'server/generation/request-fingerprint.ts')).href
    );
    assert.deepEqual(JSON.parse(failedPayload).request, parseNativeImageRequest(body));
    assert.equal(
      alias.fingerprint,
      generationRequestFingerprint(parseNativeImageRequest(body), { noCache: true }),
    );
    for (const field of ['retryCount', 'cost', 'accountCost', 'queueTotal']) {
      assert.equal(Object.hasOwn(failed, field), false, field);
    }
    for (const field of ['source', 'request', 'requestId']) {
      assert.equal(Object.hasOwn(done, field), false, field);
    }
    assert.equal(Object.hasOwn(queued, 'requestId'), false);
    assert.ok(Number.isSafeInteger(queued.queueSeq) && queued.queueSeq > 0);
    assert.equal(queued.cost, 0);
    assert.equal(queued.imageId, '');
    const privateValues = [
        error,
        detail,
        privateMarker,
        legacyKey,
        'synthetic-native-archive-member-key',
        'pst-synthetic-native-archive-only',
        body.input,
        png.toString('base64'),
      ],
      audit = (...options) => {
        const result = run([
          script,
          '--source=' + filename,
          '--source-sha256=' + sha256(before),
          ...options,
        ]);
        assert.deepEqual(readFileSync(filename), before);
        assert.deepEqual(readdirSync(sourceDirectory), entries);
        assert.deepEqual(readFileSync(imagePath), png);
        for (const value of privateValues) {
          assert.equal(result.stdout.includes(value), false, value);
        }
        if (result.status !== 0) {
          assert.equal(result.stdout, '');
        }
        assert.equal(result.status, 0, result.stderr);
        const report = JSON.parse(result.stdout);
        assert.equal(report.sourceSha256, sha256(before));
        assert.equal(report.networkCalls, 0);
        assert.equal(report.dispatches, 0);
        return report;
      };
    const webOutput = path.join(directory, 'native-web902-input.json'),
      web = audit('--request=legacy-input:job-restart-legacy', '--output=' + webOutput);
    assert.equal((await readFile(webOutput)).toString(), webPayload);
    assert.equal(web.lookup.memberId, memberId);
    assert.equal(web.lookup.source, 'web');
    assert.equal(web.lookup.operation, 'generate');
    assert.equal(web.lookup.sha256, sha256(webPayload));
    const queuedHistory = audit('--job=' + queued.id).lookup;
    assert.equal(queuedHistory.requestId, null);
    assert.equal(queuedHistory.memberId, memberId);
    assert.equal(queuedHistory.inputRef, 'legacy-input:' + queued.id);
    assert.equal(Number(queuedOrder), -1000);
    assert.equal(queuedHistory.originalOrder, queuedOrder);
    assert.equal(queuedHistory.queueTotal, null);
    const failedOutput = path.join(directory, 'native-failed-input.json'),
      input = audit('--request=legacy-input:' + failed.id, '--output=' + failedOutput);
    assert.equal((await readFile(failedOutput)).toString(), failedPayload);
    assert.equal(input.lookup.memberId, memberId);
    assert.equal(input.lookup.source, 'novelai-native');
    assert.equal(input.lookup.operation, 'generate');
    assert.equal(input.lookup.sourceEventId, opening.event_id);
    const failure = audit('--job=' + failed.id).lookup;
    assert.equal(failure.status, 'failed');
    assert.equal(failure.memberId, memberId);
    assert.equal(failure.requestId, null);
    assert.equal(failure.diagnosticId, null);
    assert.equal(failure.model, null);
    assert.equal(failure.queueTotal, null);
    assert.equal(failure.completedAt, failed.completedAt);
    const receipt = audit('--command=' + opening.command_id).lookup;
    assert.equal(receipt.commitSeq, opening.commit_seq);
    assert.equal(receipt.receiptSha256, sha256(opening.receipt));
    assert.equal(receipt.receiptBytes, Buffer.byteLength(opening.receipt));
    const doneHistory = audit('--job=' + done.id).lookup;
    assert.equal(doneHistory.status, 'done');
    assert.equal(doneHistory.memberId, memberId);
    for (const field of [
      'requestId',
      'inputRef',
      'diagnosticId',
      'model',
      'completedAt',
      'queueTotal',
    ]) {
      assert.equal(doneHistory[field], null, field);
    }
    const declaration = audit('--artifact=img-access-owner').lookup;
    assert.equal(declaration.memberId, memberId);
    assert.equal(declaration.localContentVerified, false);
    assert.equal(declaration.externalBytesChecked, false);
    for (const field of ['jobId', 'operation', 'createdAt']) {
      assert.equal(declaration[field], null, field);
    }
    const exported = path.join(directory, 'native-metadata'),
      report = audit('--export=' + exported),
      inputs = await readFile(path.join(exported, 'inputs.jsonl'), 'utf8'),
      descriptors = await readFile(path.join(exported, 'artifacts.jsonl'), 'utf8'),
      rows = inputs
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line));
    assert.equal(report.protectedInputs, 2);
    assert.equal(report.contexts, 1);
    assert.equal(report.diagnostics, 1);
    assert.equal(report.artifactInputs, 0);
    assert.equal(report.artifacts, 1);
    const preserved = rows.find((row) => row.kind === 'diagnostic' && row.jobId === failed.id);
    assert.equal(preserved.id, diagnostic.diagnostic_id);
    assert.equal(preserved.sha256, sha256(detail));
    assert.equal(preserved.bytes, Buffer.byteLength(detail));
    assert.ok(
      rows.some((row) => row.kind === 'context' && row.sha256 === sha256('{}') && row.bytes === 2),
    );
    for (const text of [inputs, descriptors]) {
      for (const value of privateValues) {
        assert.equal(text.includes(value), false, value);
      }
    }
    const imageOutput = path.join(directory, 'native-original.png');
    subtest.diagnostic(`original PNG local read with saved operation ${declaration.operation}`);
    const located = audit(
      '--artifact=img-access-owner',
      '--data-dir=' + sourceDirectory,
      '--output=' + imageOutput,
    ).lookup;
    assert.equal(located.localContentVerified, true);
    assert.equal(located.externalBytesChecked, false);
    assert.equal(located.mimeType, 'image/png');
    assert.equal(located.sha256, sha256(png));
    assert.equal(located.byteLength, png.length);
    assert.deepEqual(await readFile(imageOutput), png);
    assert.deepEqual(await readFile(filename), before);
    assert.deepEqual(await readdir(sourceDirectory), entries);
    assert.deepEqual(await readFile(imagePath), png);
  });
}
