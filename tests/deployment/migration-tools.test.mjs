import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, mkdtemp, readFile, rm, rmdir, unlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const root = fileURLToPath(new URL('../../', import.meta.url));
const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');

test('independent migration package excludes ignored private sources and runs compiled owners', async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), 'novelai-migration-package-test-')),
    privateDirectory = path.join(root, 'server/_private'),
    canaryName = `migration-package-canary-${randomUUID()}.ts`,
    canary = path.join(privateDirectory, canaryName),
    createdDirectory = await mkdir(privateDirectory, { recursive: true });
  let canaryCreated = false;
  t.after(async () => {
    assert.equal(path.dirname(canary), path.join(root, 'server/_private'));
    if (canaryCreated) await unlink(canary);
    if (createdDirectory === privateDirectory) await rmdir(privateDirectory);
    assert.equal(path.dirname(path.resolve(directory)), path.resolve(tmpdir()));
    assert.ok(path.basename(directory).startsWith('novelai-migration-package-test-'));
    await rm(directory, { recursive: true, force: true });
  });
  await writeFile(canary, 'export const privateCanary = true;\n', { flag: 'wx' });
  canaryCreated = true;
  const run = (args, cwd = root, env = process.env) =>
    spawnSync(process.execPath, args, {
      cwd,
      env,
      encoding: 'utf8',
      windowsHide: true,
      timeout: 120000,
    });
  const packaged = run([
    path.join(root, 'scripts/package-migration-tools.mjs'),
    '--output=' + (process.env.MIGRATION_TOOLS_TEST_OUTPUT || path.join(directory, 'output')),
  ]);
  assert.equal(packaged.status, 0, packaged.stderr);
  const result = JSON.parse(packaged.stdout);
  assert.equal(sha256(await readFile(result.archive)), result.sha256);
  const unpacked = path.join(directory, 'unpacked');
  await mkdir(unpacked);
  const tar = spawnSync('tar', ['-xzf', result.archive, '-C', unpacked], {
    encoding: 'utf8',
    windowsHide: true,
  });
  assert.equal(tar.status, 0, tar.stderr);
  const bundle = path.join(unpacked, 'migration-tools'),
    manifest = JSON.parse(await readFile(path.join(bundle, 'MIGRATION_TOOLS.json'), 'utf8')),
    names = Object.keys(manifest.files),
    packageJson = JSON.parse(await readFile(path.join(bundle, 'package.json'), 'utf8'));
  assert.equal(manifest.sourceCommit, result.sourceCommit);
  assert.ok(names.includes('scripts/reconcile-legacy-database.mjs'));
  assert.ok(!names.some((name) => name.includes(canaryName.replace(/\.ts$/, '.js'))));
  assert.ok(!names.some((name) => /^(?:server|scripts)\/.*\.(?:ts|tsx|mts|cts)$/.test(name)));
  assert.deepEqual(Object.keys(packageJson.scripts), [
    'migrate',
    'materialize-originals',
    'restore',
    'retained',
    'verify',
  ]);
  assert.ok(names.includes('scripts/materialize-legacy-originals.mjs'));
  assert.equal(packageJson.main, undefined);
  assert.equal(packageJson.devDependencies, undefined);
  const verified = run(
    ['--no-experimental-strip-types', 'scripts/verify-migration-package.mjs'],
    bundle,
  );
  assert.equal(verified.status, 0, verified.stderr);
  assert.equal(JSON.parse(verified.stdout).verified, true);
  const loaded = run(
    [
      '--no-experimental-strip-types',
      '--input-type=module',
      '-e',
      `
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { migrateLegacyDatabase } from './scripts/migrate-legacy-database.mjs';
import { materializeLegacyOriginals } from './scripts/materialize-legacy-originals.mjs';
const require = createRequire(new URL('./package.json', import.meta.url));
const db = new (require('better-sqlite3'))(':memory:');
assert.equal(db.pragma('integrity_check', {simple:true}), 'ok');
db.close();
assert.equal(typeof migrateLegacyDatabase, 'function');
assert.equal(typeof materializeLegacyOriginals, 'function');
await assert.rejects(migrateLegacyDatabase({}), /selected unchanged offline source/);
`,
    ],
    bundle,
  );
  assert.equal(loaded.status, 0, loaded.stderr);
  const conversionEnvironment = {
    ...process.env,
    MIGRATE_LEGACY_TEST_ENTRY: path.join(bundle, 'scripts/migrate-legacy-database.mjs'),
  };
  // 子进程必须独立运行测试，不能继承父 runner 的内部标记。
  delete conversionEnvironment.NODE_TEST_CONTEXT;
  const converted = run(
    ['--test', path.join(root, 'tests/integration/migrate-legacy-database.test.mjs')],
    root,
    conversionEnvironment,
  );
  assert.equal(converted.status, 0, converted.stderr + converted.stdout);
  const total = /^# tests (\d+)$/m.exec(converted.stdout),
    passed = /^# pass (\d+)$/m.exec(converted.stdout);
  assert.ok(total && Number(total[1]) > 0, converted.stdout);
  assert.doesNotMatch(converted.stdout, /^1\.\.0$/m);
  assert.match(converted.stdout, /^# Subtest: 私有尾部真实SQL错外catch回滚整笔公共\/正文\/私有/m);
  assert.equal(passed?.[1], total[1], converted.stdout);
  t.diagnostic(`Compiled conversion: ${passed[1]}/${total[1]}, no skipped tests.`);
  assert.equal(await readFile(canary, 'utf8'), 'export const privateCanary = true;\n');
});
