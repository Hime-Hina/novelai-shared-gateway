import { constants } from 'node:fs';
import {
  cp,
  copyFile,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  symlink,
  unlink,
  writeFile,
} from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { spawnSync } from 'node:child_process';
import { migrationPackageFiles, verifyMigrationPackage } from './verify-migration-package.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
const sha256 = (value) => createHash('sha256').update(value).digest('hex');
const { values } = parseArgs({ options: { output: { type: 'string' } } });
if (!values.output || process.version !== 'v22.23.4') {
  throw new Error(
    'Use Node 22.23.4: node scripts/package-migration-tools.mjs --output=/new/directory',
  );
}
const output = path.resolve(values.output);
try {
  await lstat(output);
  throw new Error('Migration tools output directory must not exist.');
} catch (error) {
  if (error.code !== 'ENOENT') throw error;
}
const pnpmScript =
  process.env.npm_execpath ||
  (process.platform === 'win32'
    ? path.join(path.dirname(process.execPath), 'node_modules/corepack/dist/pnpm.js')
    : null);
function run(command, args, cwd = root) {
  const result = spawnSync(command, args, {
    cwd,
    encoding: 'utf8',
    windowsHide: true,
    maxBuffer: 8 * 1024 * 1024,
    env: {
      ...process.env,
      CI: 'true',
      COREPACK_ENABLE_NETWORK: '0',
      PATH: path.dirname(process.execPath) + path.delimiter + process.env.PATH,
    },
  });
  if (result.error || result.status !== 0) {
    throw new Error(
      `Migration tool packaging failed: ${command}\n${result.stderr || result.stdout}`,
    );
  }
  return result.stdout.trim();
}
const runPnpm = (args) =>
  pnpmScript ? run(process.execPath, [pnpmScript, ...args]) : run('pnpm', args);
if (runPnpm(['--version']) !== '10.32.1') throw new Error('Pinned pnpm 10.32.1 is required.');
const inputs = [
  'server',
  'scripts',
  'tsconfig.backend.json',
  'package.json',
  'pnpm-lock.yaml',
  'pnpm-workspace.yaml',
  'patches',
  '.npmrc',
  '.node-version',
  'LICENSE',
  'NOTICE.md',
  'UPSTREAM-LICENSE',
];
function assertClean() {
  if (run('git', ['status', '--porcelain', '--untracked-files=all', '--', ...inputs])) {
    throw new Error('Migration package inputs must be committed and unchanged.');
  }
}
assertClean();
const sourceCommit = run('git', ['rev-parse', 'HEAD']),
  sourceTree = run('git', ['rev-parse', 'HEAD^{tree}']),
  work = await mkdtemp(path.join(tmpdir(), 'novelai-migration-tools-'));
try {
  const source = path.join(work, 'source'),
    deployed = path.join(work, 'deployed'),
    compiled = path.join(work, 'compiled'),
    bundle = path.join(work, 'migration-tools');
  await mkdir(source);
  run('git', [
    'archive',
    '--format=tar',
    `--output=${path.join(work, 'source.tar')}`,
    sourceCommit,
    ...inputs,
  ]);
  run('tar', ['-xf', path.join(work, 'source.tar'), '-C', source]);
  // 只编译固定 Git 输入；clean 状态仍可能有被 ignore 的私有源码。
  const dependencies = path.join(source, 'node_modules');
  await symlink(path.join(root, 'node_modules'), dependencies, 'junction');
  try {
    run(process.execPath, [
      path.join(root, 'node_modules/typescript/bin/tsc'),
      '-p',
      path.join(source, 'tsconfig.backend.json'),
      '--outDir',
      compiled,
    ]);
  } finally {
    await unlink(dependencies);
  }
  const options = ['--config.allowUnusedPatches=true', '--config.ignoreScripts=true'];
  runPnpm([
    '--dir',
    source,
    ...options,
    '--filter',
    'novelai-shared-gateway',
    'deploy',
    '--prod',
    '--legacy',
    '--offline',
    deployed,
  ]);
  await mkdir(bundle);
  await cp(path.join(compiled, 'server'), path.join(bundle, 'server'), { recursive: true });
  await cp(path.join(source, 'server/migrations'), path.join(bundle, 'server/migrations'), {
    recursive: true,
  });
  for (const name of [
    'patches',
    'pnpm-workspace.yaml',
    '.node-version',
    'LICENSE',
    'NOTICE.md',
    'UPSTREAM-LICENSE',
  ]) {
    await cp(path.join(source, name), path.join(bundle, name), { recursive: true });
  }
  await mkdir(path.join(bundle, 'scripts'));
  for (const name of [
    'migrate-legacy-database.mjs',
    'migrate-legacy-finance.mjs',
    'migrate-legacy-private.mjs',
    'migrate-legacy-artifacts.mjs',
    'database-model-inventory.mjs',
    'restore-relational.mjs',
    'retained-sources.mjs',
    'verify-migration-package.mjs',
  ]) {
    await copyFile(path.join(compiled, 'scripts', name), path.join(bundle, 'scripts', name));
  }
  await copyFile(path.join(source, 'package.json'), path.join(bundle, 'SOURCE_PACKAGE.json'));
  await copyFile(path.join(source, 'pnpm-lock.yaml'), path.join(bundle, 'SOURCE_LOCK.yaml'));
  await copyFile(
    path.join(deployed, 'node_modules/.pnpm/lock.yaml'),
    path.join(bundle, 'pnpm-lock.yaml'),
  );
  const original = JSON.parse(await readFile(path.join(source, 'package.json'), 'utf8'));
  const { devDependencies: _dev, scripts: _scripts, main: _main, ...manifest } = original;
  await writeFile(
    path.join(bundle, 'package.json'),
    JSON.stringify(
      {
        ...manifest,
        name: 'novelai-migration-tools',
        private: true,
        scripts: {
          migrate: 'node scripts/migrate-legacy-database.mjs',
          restore: 'node scripts/restore-relational.mjs',
          retained: 'node scripts/retained-sources.mjs',
          verify: 'node scripts/verify-migration-package.mjs',
        },
      },
      null,
      2,
    ) + '\n',
  );
  runPnpm([
    '--dir',
    bundle,
    ...options,
    'install',
    '--prod',
    '--offline',
    '--frozen-lockfile',
    '--config.node-linker=hoisted',
  ]);
  // 与独立旧工具相同，复制已可加载的同版本 native，不运行下载或重建脚本。
  const require = createRequire(path.join(root, 'package.json'));
  if (require('better-sqlite3/package.json').version !== original.dependencies['better-sqlite3']) {
    throw new Error('Installed SQLite dependency version differs.');
  }
  new (require('better-sqlite3'))(':memory:').close();
  const nativeDirectory = path.join(bundle, 'node_modules/better-sqlite3/build/Release');
  await mkdir(nativeDirectory, { recursive: true });
  await copyFile(
    require.resolve('better-sqlite3/build/Release/better_sqlite3.node'),
    path.join(nativeDirectory, 'better_sqlite3.node'),
  );
  const files = await migrationPackageFiles(bundle);
  for (const name of Object.keys(files).filter((name) => name.startsWith('server/migrations/'))) {
    if (files[name].sha256 !== sha256(await readFile(path.join(source, name)))) {
      throw new Error('Legacy SQL or journal bytes changed.');
    }
  }
  assertClean();
  if (run('git', ['rev-parse', 'HEAD']) !== sourceCommit) throw new Error('Source commit moved.');
  await writeFile(
    path.join(bundle, 'MIGRATION_TOOLS.json'),
    JSON.stringify(
      {
        format: 'novelai-migration-tools-v1',
        sourceCommit,
        sourceTree,
        node: process.version,
        platform: process.platform,
        arch: process.arch,
        abi: process.versions.modules,
        files,
      },
      null,
      2,
    ) + '\n',
  );
  await verifyMigrationPackage(bundle);
  const name = `novelai-migration-tools-${sourceCommit.slice(0, 7)}-${process.platform}-${process.arch}.tar.gz`;
  run('tar', ['-czf', path.join(work, name), '-C', work, 'migration-tools']);
  const extracted = path.join(work, 'unpacked');
  await mkdir(extracted);
  run('tar', ['-xzf', path.join(work, name), '-C', extracted]);
  run(process.execPath, [
    '--no-experimental-strip-types',
    path.join(extracted, 'migration-tools/scripts/verify-migration-package.mjs'),
  ]);
  const digest = sha256(await readFile(path.join(work, name)));
  await mkdir(output, { mode: 0o700 });
  await copyFile(path.join(work, name), path.join(output, name), constants.COPYFILE_EXCL);
  await writeFile(path.join(output, name + '.sha256'), `${digest}  ${name}\n`, { flag: 'wx' });
  console.log(
    JSON.stringify({
      sourceCommit,
      archive: path.join(output, name),
      sha256: digest,
      files: Object.keys(files).length,
    }),
  );
} finally {
  if (
    path.dirname(path.resolve(work)) !== path.resolve(tmpdir()) ||
    !path.basename(work).startsWith('novelai-migration-tools-')
  ) {
    throw new Error('Refusing to remove an unexpected migration staging path.');
  }
  await rm(work, { recursive: true, force: true });
}
