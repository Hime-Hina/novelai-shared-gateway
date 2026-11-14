import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { createHash } from 'node:crypto';
import {
  copyFile,
  lstat,
  mkdtemp,
  mkdir,
  readFile,
  readdir,
  rename,
  rm,
  symlink,
  unlink,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { spawnSync } from 'node:child_process';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';

const root = fileURLToPath(new URL('../../', import.meta.url));
const sha256 = (value) => createHash('sha256').update(value).digest('hex');
const fixed = '3caba3dfa0b5d9f582acabfc02e36e7db4af038f';

test('固定旧工具从独立 tar 解包后只读审计、导出来源和产物声明，并拒绝损坏及新格式', async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), 'novelai-legacy-package-test-'));
  t.after(async () => {
    assert.equal(path.dirname(path.resolve(directory)), path.resolve(tmpdir()));
    assert.ok(path.basename(directory).startsWith('novelai-legacy-package-test-'));
    await rm(directory, { recursive: true, force: true });
  });
  const run = (script, options = [], cwd = root) =>
    spawnSync(process.execPath, [script, ...options], {
      cwd,
      encoding: 'utf8',
      timeout: 120000,
      env: { ...process.env, NODE_PATH: '' },
    });
  const packaged = run(path.join(root, 'scripts/package-legacy-tools.mjs'), [
    '--output=' + path.join(directory, 'output'),
  ]);
  assert.equal(packaged.status, 0, packaged.stderr);
  const output = JSON.parse(packaged.stdout);
  assert.equal(output.sourceCommit, fixed);
  assert.equal(sha256(await readFile(output.archive)), output.sha256);
  const unpacked = path.join(directory, 'unpacked');
  await mkdir(unpacked);
  const tar = spawnSync('tar', ['-xzf', output.archive, '-C', unpacked], { encoding: 'utf8' });
  assert.equal(tar.status, 0, tar.stderr);
  const bundle = path.join(unpacked, 'legacy-tools'),
    script = path.join(bundle, 'scripts/audit-legacy-archive.mjs');
  const manifest = JSON.parse(await readFile(path.join(bundle, 'LEGACY_TOOLS.json'), 'utf8'));
  assert.equal(manifest.sourceCommit, fixed);
  const require = createRequire(path.join(bundle, 'package.json'));
  assert.ok(
    require.resolve('better-sqlite3').startsWith(path.join(bundle, 'node_modules') + path.sep),
  );
  assert.deepEqual(
    Object.keys(JSON.parse(await readFile(path.join(bundle, 'package.json'), 'utf8')).scripts),
    ['audit'],
  );
  for (const name of Object.keys(manifest.files).filter((name) =>
    name.startsWith('server/migrations/'),
  )) {
    const original = spawnSync('git', ['show', `${fixed}:${name}`], { cwd: root });
    assert.equal(original.status, 0);
    assert.equal(sha256(original.stdout), sha256(await readFile(path.join(bundle, name))));
  }
  const sourceDirectory = path.join(directory, 'synthetic-source');
  const secret = 'private-legacy-package-prompt',
    key = 'private-legacy-package-key';
  const imagePath = path.join(root, 'tests/fixtures/synthetic.webp'),
    image = await readFile(imagePath);
  const fixture = path.join(directory, 'fixture.mjs');
  await writeFile(
    fixture,
    `
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
const [bundle, sourceDirectory, imagePath] = process.argv.slice(2);
const { JsonStore } = await import(pathToFileURL(path.join(bundle, 'server/storage/sqlite-store.ts')).href);
const { appendDomainCommit } = await import(pathToFileURL(path.join(bundle, 'server/events/event-store.ts')).href);
const require = createRequire(path.join(bundle, 'package.json')), Database = require('better-sqlite3');
const sha256 = value => createHash('sha256').update(value).digest('hex');
const store = new JsonStore(sourceDirectory);
await store.init();
const at = '2026-11-01T00:00:00.000Z', secret = 'private-legacy-package-prompt', key = 'private-legacy-package-key';
const image = await readFile(imagePath);
try {
  await store.write({ settings: store.db.settings, accounts: [], cards: [], ledger: [],
    users: [{ id: 'member', token: key, enabled: true }],
    jobs: [{ id: 'job', userToken: key, status: 'failed', createdAt: at, imageId: 'artifact', errorDetail: secret,
      request: { model: 'nai-diffusion-4-5-full', prompt: secret } },
      { id: 'grant-job', userToken: key, status: 'failed', createdAt: at,
        request: { model: 'nai-diffusion-4-5-full', prompt: secret, operationParameters: { discordGrant: key } } }],
    images: [{ id: 'artifact', jobId: 'job', token: key, base64: image.toString('base64'), prompt: secret,
      operation: 'generate', mimeType: 'image/webp', byteLength: image.length, sha256: sha256(image),
      file: 'images/artifact.webp', createdAt: at }],
  });
  store.sqlite.prepare('INSERT INTO generation_idempotency VALUES(?,?,?,?,?,?,?,?)').run(
    'member', 'generate', sha256('synthetic-key'), sha256('synthetic-intent'), JSON.stringify({ defaultNegative: secret }), 'job', null, at,
  );
  await store.enableEventAuthority();
  store.sqlite.transaction(() => appendDomainCommit(store.sqlite, {
    commandId: 'original-generation-receipt', recordedAt: at,
    expectedVersions: { 'request:receipt-audit': '0' },
    receipt: { id: 'job', requestId: null, jobIds: ['job'], count: 1 },
    events: [{ eventType: 'OpeningStateRecorded', eventId: 'receipt-audit-event',
      streamId: 'request:receipt-audit', occurredAt: at,
      payload: { source: 'legacy-opening', state: { request: { requestId: null, memberId: null, inputRef: null }, jobs: {}, artifacts: {}, aliases: [], freeUsage: {} } } }],
  }))();
} finally { await store.close(); }
await mkdir(path.join(sourceDirectory, 'images'), { recursive: true });
await writeFile(path.join(sourceDirectory, 'images/artifact.webp'), image);
const seal = new Database(path.join(sourceDirectory, 'library.sqlite'));
seal.pragma('journal_mode=DELETE');
seal.close();
`,
  );
  const created = run(fixture, [bundle, sourceDirectory, imagePath], directory);
  assert.equal(created.status, 0, created.stderr);
  const sourcePath = path.join(sourceDirectory, 'library.sqlite');
  const before = await readFile(sourcePath);
  const sourceEntries = await readdir(sourceDirectory);
  const guard = path.join(directory, 'no-network.mjs');
  await writeFile(guard, "globalThis.fetch = () => { throw new Error('unexpected_network'); };\n");
  const audit = (...options) =>
    run(
      script,
      ['--source=' + sourcePath, '--source-sha256=' + sha256(before), ...options],
      directory,
    );
  const verified = run(script, ['--verify-package'], directory);
  assert.equal(verified.status, 0, verified.stderr);
  const exported = path.join(directory, 'export');
  const result = spawnSync(
    process.execPath,
    [
      '--import',
      pathToFileURL(guard).href,
      script,
      '--source=' + sourcePath,
      '--export=' + exported,
    ],
    {
      cwd: directory,
      encoding: 'utf8',
      env: { ...process.env, NODE_PATH: '' },
    },
  );
  assert.equal(result.status, 0, result.stderr);
  const report = JSON.parse(result.stdout);
  assert.equal(report.sourceSha256, sha256(before));
  assert.equal(report.protectedInputs, 2);
  assert.equal(report.contexts, 1);
  assert.equal(report.artifactInputs, 1);
  assert.equal(report.diagnostics, 1);
  assert.equal(report.artifacts, 1);
  assert.equal(report.networkCalls, 0);
  assert.equal(report.dispatches, 0);
  assert.equal(report.externalObjectBytesVerified, false);
  const inputs = await readFile(path.join(exported, 'inputs.jsonl'), 'utf8');
  const descriptors = await readFile(path.join(exported, 'artifacts.jsonl'), 'utf8');
  for (const text of [result.stdout, inputs, descriptors]) {
    assert.equal(text.includes(secret), false);
    assert.equal(text.includes(key), false);
    assert.equal(text.includes(image.toString('base64')), false);
  }
  assert.equal(JSON.parse(descriptors).sha256, sha256(image));
  assert.deepEqual(await readFile(sourcePath), before);
  assert.deepEqual(await readdir(sourceDirectory), sourceEntries);
  assert.notEqual(audit('--export=' + exported).status, 0);
  assert.deepEqual(await readFile(sourcePath), before);
  const imageBefore = await readFile(path.join(sourceDirectory, 'images/artifact.webp'));
  const job = audit('--job=job');
  assert.equal(job.status, 0, job.stderr);
  const history = JSON.parse(job.stdout).lookup;
  assert.equal(history.kind, 'job');
  assert.equal(history.id, 'job');
  assert.equal(history.memberId, 'member');
  assert.equal(history.diagnosticId, null);
  assert.match(history.originalOrder, /^\d+(?:\.\d+)?$/);
  assert.equal(typeof history.streamLastCommitSeq, 'string');
  assert.equal(history.inputRef, 'legacy-input:job');
  const request = audit('--request=' + history.inputRef);
  assert.equal(request.status, 0, request.stderr);
  const inputFile = path.join(directory, 'original-input.json');
  const input = audit('--request=' + history.inputRef, '--output=' + inputFile);
  assert.equal(input.status, 0, input.stderr);
  assert.equal(JSON.parse(await readFile(inputFile, 'utf8')).request.prompt, secret);
  assert.equal(sha256(await readFile(inputFile)), JSON.parse(request.stdout).lookup.sha256);
  const receiptFile = path.join(directory, 'original-receipt.json');
  const receipt = audit('--command=original-generation-receipt', '--output=' + receiptFile);
  assert.equal(receipt.status, 0, receipt.stderr);
  assert.deepEqual(JSON.parse(await readFile(receiptFile, 'utf8')), {
    id: 'job',
    requestId: null,
    jobIds: ['job'],
    count: 1,
  });
  assert.equal(
    sha256(await readFile(receiptFile)),
    JSON.parse(receipt.stdout).lookup.receiptSha256,
  );
  const declared = audit('--artifact=artifact');
  assert.equal(declared.status, 0, declared.stderr);
  assert.equal(JSON.parse(declared.stdout).lookup.localContentVerified, false);
  const originalImage = path.join(directory, 'original-image.webp');
  const located = audit(
    '--artifact=artifact',
    '--data-dir=' + sourceDirectory,
    '--output=' + originalImage,
  );
  assert.equal(located.status, 0, located.stderr);
  assert.equal(JSON.parse(located.stdout).lookup.localContentVerified, true);
  assert.deepEqual(await readFile(originalImage), image);
  for (const lookup of [job, request, input, receipt, declared, located]) {
    assert.equal(lookup.stdout.includes(secret), false);
    assert.equal(lookup.stdout.includes(key), false);
    assert.equal(lookup.stdout.includes(image.toString('base64')), false);
  }
  for (const options of [
    ['--job=missing'],
    ['--job=job', '--source-sha256=' + '0'.repeat(64)],
    ['--job='],
    ['--request=missing'],
    ['--command=missing'],
    ['--artifact=missing'],
    ['--job=job', '--artifact=artifact'],
    ['--job=job', '--output=' + path.join(directory, 'forbidden-job.json')],
    ['--artifact=artifact', '--output=' + path.join(directory, 'unverified-image.webp')],
    ['--request=' + history.inputRef, '--output=' + inputFile],
    ['--request=' + history.inputRef, '--output=' + path.join(sourceDirectory, 'new-input.json')],
    ['--artifact=artifact', '--data-dir=' + path.join(directory, 'missing-data')],
    [
      '--request=legacy-input:grant-job',
      '--output=' + path.join(directory, 'forbidden-grant.json'),
    ],
  ]) {
    const refused = audit(...options);
    assert.notEqual(refused.status, 0, options.join(' '));
    assert.equal(refused.stdout, '');
  }
  await assert.rejects(readFile(path.join(directory, 'forbidden-grant.json')), { code: 'ENOENT' });
  const escapedRoot = path.join(directory, 'escaped-root'),
    outsideImages = path.join(directory, 'outside-images');
  await mkdir(escapedRoot);
  await mkdir(outsideImages);
  await writeFile(path.join(outsideImages, 'artifact.webp'), image);
  await symlink(
    outsideImages,
    path.join(escapedRoot, 'images'),
    process.platform === 'win32' ? 'junction' : 'dir',
  );
  const escaped = audit('--artifact=artifact', '--data-dir=' + escapedRoot);
  assert.notEqual(escaped.status, 0);
  assert.equal(escaped.stdout, '');
  const corruptRoot = path.join(directory, 'corrupt-root');
  await mkdir(path.join(corruptRoot, 'images'), { recursive: true });
  const corruptBytes = Buffer.from(image);
  corruptBytes[corruptBytes.length - 1] ^= 1;
  await writeFile(path.join(corruptRoot, 'images/artifact.webp'), corruptBytes);
  const corruptImage = audit('--artifact=artifact', '--data-dir=' + corruptRoot);
  assert.notEqual(corruptImage.status, 0);
  assert.equal(corruptImage.stdout, '');
  assert.deepEqual(await readFile(sourcePath), before);
  assert.deepEqual(await readFile(path.join(sourceDirectory, 'images/artifact.webp')), imageBefore);
  assert.deepEqual(await readdir(sourceDirectory), sourceEntries);
  const source = new Database(sourcePath, { readonly: true });
  try {
    assert.throws(() => new RelationalInputStore(source, {}), /Unsupported/);
  } finally {
    source.close();
  }
  const newPath = path.join(directory, 'new.sqlite'),
    target = new Database(newPath);
  initializeRelationalStorage(target, 'synthetic-new-history');
  const { migrateStorage } = await import(
    pathToFileURL(path.join(bundle, 'server/storage/sqlite-migrations.ts')).href
  );
  const newBefore = target.serialize();
  assert.throws(() => migrateStorage(target), /newer compatible writer|unsupported version/);
  assert.ok(target.serialize().equals(newBefore));
  target.close();
  const newFileBefore = await readFile(newPath);
  assert.notEqual(run(script, ['--source=' + newPath], directory).status, 0);
  assert.deepEqual(await readFile(newPath), newFileBefore);
  for (const [name, sql] of [
    [
      'writer',
      'UPDATE app_meta SET value=\'{"version":99,"minimumWriterVersion":99}\' WHERE key=\'event_authority\'',
    ],
    ['migration', "UPDATE __drizzle_migrations SET hash='modified'"],
    [
      'input',
      "UPDATE generation_requests SET payload=json_set(payload,'$.request.prompt','changed-private-input')",
    ],
    [
      'context',
      "UPDATE generation_contexts SET payload=json_set(payload,'$.defaultNegative','changed-private-input')",
    ],
    [
      'unknown-event',
      "UPDATE domain_events SET event_type='UnknownPrivateEvent' WHERE event_index=0",
    ],
    [
      'artifact-input',
      "UPDATE artifact_inputs SET payload=json_set(payload,'$.prompt','changed-private-input')",
    ],
    ['artifact-private-owner', "UPDATE artifact_inputs SET member_id='different-member'"],
    [
      'descriptor',
      "UPDATE images SET data=json_set(data,'$.id',json_object('token','private-descriptor-secret'))",
    ],
    ['clock-cut', 'UPDATE event_clock SET value=value+1 WHERE id=1'],
    ['projection-cut', "UPDATE projection_versions SET last_commit_seq=0 WHERE name='core'"],
    ['projection-status', "UPDATE projection_versions SET status='building' WHERE name='core'"],
    [
      'commit-gap',
      [
        'DELETE FROM domain_events WHERE commit_seq=2',
        'DELETE FROM event_commits WHERE commit_seq=2',
      ],
    ],
    ['artifact-string-id', "UPDATE images SET data=json_set(data,'$.id','different-artifact')"],
    ['user-string-id', "UPDATE users SET data=json_set(data,'$.id','different-member')"],
    ['job-string-id', "UPDATE jobs SET data=json_set(data,'$.id','different-job')"],
  ]) {
    await t.test(name + ' 损坏拒绝且不改源', async () => {
      const filename = path.join(directory, name + '.sqlite');
      await copyFile(sourcePath, filename);
      const changed = new Database(filename);
      changed.pragma('foreign_keys=ON');
      changed.transaction(() => {
        for (const statement of Array.isArray(sql) ? sql : [sql]) {
          assert.ok(changed.prepare(statement).run().changes > 0);
        }
      })();
      assert.deepEqual(changed.pragma('foreign_key_check'), []);
      changed.close();
      const original = await readFile(filename);
      assert.notEqual(run(script, ['--source=' + filename], directory).status, 0);
      assert.deepEqual(await readFile(filename), original);
    });
  }
  const sqlName = Object.keys(manifest.files).find((name) => name.endsWith('.sql'));
  const sqlPath = path.join(bundle, sqlName),
    originalSql = await readFile(sqlPath);
  await writeFile(sqlPath, Buffer.concat([originalSql, Buffer.from('\n-- changed\n')]));
  assert.notEqual(run(script, ['--verify-package'], directory).status, 0);
  await writeFile(sqlPath, originalSql);
  assert.equal(run(script, ['--verify-package'], directory).status, 0);
  await t.test('包祖先目录只允许仍在同一物理包内的链接', async () => {
    const server = path.join(bundle, 'server'),
      internal = path.join(bundle, 'server-original');
    const external = path.join(directory, 'external-server');
    assert.equal(path.dirname(external), directory);
    await rename(server, internal);
    try {
      await symlink(internal, server, 'junction');
      assert.equal(run(script, ['--verify-package'], directory).status, 0);
    } finally {
      await unlink(server);
      await rename(internal, server);
    }
    await rename(server, external);
    try {
      await symlink(external, server, 'junction');
      assert.notEqual(run(script, ['--verify-package'], directory).status, 0);
    } finally {
      await unlink(server);
      await rename(external, server);
    }
    assert.equal(run(script, ['--verify-package'], directory).status, 0);
  });
  await t.test('导出按父目录真实路径隔离，别名不能写进包内', async () => {
    const outside = path.join(directory, 'export-parent'),
      alias = path.join(directory, 'export-alias');
    await mkdir(outside);
    await symlink(outside, alias, 'junction');
    try {
      assert.equal(audit('--export=' + path.join(alias, 'valid-export')).status, 0);
      assert.ok(await lstat(path.join(outside, 'valid-export/audit.json')));
    } finally {
      await unlink(alias);
    }
    await symlink(bundle, alias, 'junction');
    try {
      assert.notEqual(audit('--export=' + path.join(alias, 'escaped-export')).status, 0);
      await assert.rejects(lstat(path.join(bundle, 'escaped-export')), { code: 'ENOENT' });
    } finally {
      await unlink(alias);
    }
    assert.deepEqual(await readFile(sourcePath), before);
    assert.deepEqual(await readdir(sourceDirectory), sourceEntries);
  });
  await t.test('包外依赖祖先被拒绝时，不先执行该依赖', async () => {
    const modules = path.join(bundle, 'node_modules'),
      external = path.join(directory, 'external-modules');
    const marker = path.join(directory, 'unverified-module-executed');
    assert.equal(path.dirname(external), directory);
    await rename(modules, external);
    const entry = path.join(external, 'better-sqlite3/lib/index.js'),
      original = await readFile(entry);
    try {
      await writeFile(
        entry,
        Buffer.concat([
          original,
          Buffer.from(
            `\nrequire('node:fs').writeFileSync(${JSON.stringify(marker)}, 'executed');\n`,
          ),
        ]),
      );
      await symlink(external, modules, 'junction');
      assert.notEqual(run(script, ['--verify-package'], directory).status, 0);
      await assert.rejects(lstat(marker), { code: 'ENOENT' });
    } finally {
      await unlink(modules);
      await writeFile(entry, original);
      await rename(external, modules);
    }
    assert.equal(run(script, ['--verify-package'], directory).status, 0);
  });
});
