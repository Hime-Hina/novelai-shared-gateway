import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');

// These two sources belonged to online legacy-inline upgrade. D4 keeps their protected
// input proof in the independent, verified old tool, never in the ordinary gateway.
export async function verifyLegacyInlineArchive(t, bundle, directory) {
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
