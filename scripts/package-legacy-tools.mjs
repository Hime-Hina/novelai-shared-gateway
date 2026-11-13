import { constants } from 'node:fs';
import {
  cp,
  copyFile,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  readlink,
  rm,
  writeFile,
} from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { spawnSync } from 'node:child_process';

const root = fileURLToPath(new URL('../', import.meta.url));
const sourceCommit = '3caba3dfa0b5d9f582acabfc02e36e7db4af038f';
const sha256 = (value) => createHash('sha256').update(value).digest('hex');
const { values } = parseArgs({ options: { output: { type: 'string' } } });
if (!values.output || process.version !== 'v22.23.4') {
  throw new Error(
    'Use Node 22.23.4: node scripts/package-legacy-tools.mjs --output=/new/directory',
  );
}
const output = path.resolve(values.output);
try {
  await lstat(output);
  throw new Error('Legacy tools output directory must not exist.');
} catch (error) {
  if (error.code !== 'ENOENT') {
    throw error;
  }
}
const pnpm =
  process.env.npm_execpath ||
  path.join(path.dirname(process.execPath), 'node_modules/corepack/dist/pnpm.js');
function run(command, args, cwd = root) {
  const result = spawnSync(command, args, {
    cwd,
    encoding: 'utf8',
    maxBuffer: 8 * 1024 * 1024,
    env: {
      ...process.env,
      CI: 'true',
      COREPACK_ENABLE_NETWORK: '0',
      PATH: path.dirname(process.execPath) + path.delimiter + process.env.PATH,
    },
  });
  if (result.error || result.status !== 0) {
    throw new Error(`Legacy tool packaging failed: ${command}\n${result.stderr || result.stdout}`);
  }
  return result.stdout.trim();
}
if (run(process.execPath, [pnpm, '--version']) !== '10.32.1') {
  throw new Error('Legacy tools require the pinned pnpm 10.32.1.');
}
const work = await mkdtemp(path.join(tmpdir(), 'novelai-legacy-tools-'));
try {
  const source = path.join(work, 'source'),
    deployed = path.join(work, 'deployed'),
    bundle = path.join(work, 'legacy-tools');
  await mkdir(source);
  run('git', [
    'archive',
    '--format=tar',
    `--output=${path.join(work, 'source.tar')}`,
    sourceCommit,
    'server',
    'package.json',
    'pnpm-lock.yaml',
    'pnpm-workspace.yaml',
    'patches',
    '.npmrc',
    '.node-version',
    'LICENSE',
    'NOTICE.md',
    'UPSTREAM-LICENSE',
  ]);
  run('tar', ['-xf', path.join(work, 'source.tar'), '-C', source]);
  // pnpm 裁剪原锁文件；UI patch 不属于离线工具的生产依赖，不改原始锁文件。
  const options = ['--config.allowUnusedPatches=true', '--config.ignoreScripts=true'];
  run(process.execPath, [
    pnpm,
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
  for (const name of [
    'server',
    'patches',
    'pnpm-workspace.yaml',
    '.node-version',
    'LICENSE',
    'NOTICE.md',
    'UPSTREAM-LICENSE',
  ]) {
    await cp(path.join(source, name), path.join(bundle, name), { recursive: true });
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
        name: 'novelai-legacy-tools',
        private: true,
        scripts: { audit: 'node scripts/audit-legacy-archive.mjs' },
      },
      null,
      2,
    ) + '\n',
  );
  // 独立 hoisted 布局在 Windows 解包后也不依赖原 pnpm junction 或开发工作树。
  run(process.execPath, [
    pnpm,
    '--dir',
    bundle,
    ...options,
    'install',
    '--prod',
    '--offline',
    '--frozen-lockfile',
    '--config.node-linker=hoisted',
  ]);
  // 不在打包时运行下载型 install script；复用已安装、同版本且当前 ABI 可加载的原生绑定。
  const require = createRequire(path.join(root, 'package.json'));
  if (require('better-sqlite3/package.json').version !== original.dependencies['better-sqlite3']) {
    throw new Error('Legacy tools require the matching installed SQLite native dependency.');
  }
  const nativeCheck = new (require('better-sqlite3'))(':memory:');
  nativeCheck.close();
  const nativeDirectory = path.join(bundle, 'node_modules/better-sqlite3/build/Release');
  await mkdir(nativeDirectory, { recursive: true });
  await copyFile(
    require.resolve('better-sqlite3/build/Release/better_sqlite3.node'),
    path.join(nativeDirectory, 'better_sqlite3.node'),
  );
  await mkdir(path.join(bundle, 'scripts'));
  await copyFile(
    path.join(root, 'scripts/audit-legacy-archive.mjs'),
    path.join(bundle, 'scripts/audit-legacy-archive.mjs'),
  );
  const files = {};
  async function collect(directory, prefix = '') {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const name = prefix + entry.name,
        filename = path.join(directory, entry.name),
        info = await lstat(filename);
      if (info.isSymbolicLink()) {
        const target = await readlink(filename),
          resolved = path.resolve(path.dirname(filename), target);
        if (path.isAbsolute(target) || path.relative(bundle, resolved).startsWith('..')) {
          throw new Error('Legacy tool package contains an external dependency link.');
        }
        files[name] = { link: target };
      } else if (info.isDirectory()) {
        await collect(filename, name + '/');
      } else if (info.isFile()) {
        files[name] = { sha256: sha256(await readFile(filename)) };
      }
    }
  }
  await collect(bundle);
  // server/ 与 SQL/journal 均直接来自固定 Git 对象，打包不重编或改写这些字节。
  for (const name of Object.keys(files).filter((name) => name.startsWith('server/'))) {
    if (files[name].sha256 !== sha256(await readFile(path.join(source, name)))) {
      throw new Error('Fixed legacy source bytes changed during packaging.');
    }
  }
  await writeFile(
    path.join(bundle, 'LEGACY_TOOLS.json'),
    JSON.stringify(
      {
        format: 'novelai-legacy-tools-v1',
        sourceCommit,
        sourceTree: run('git', ['rev-parse', `${sourceCommit}:server`]),
        node: process.version,
        platform: process.platform,
        arch: process.arch,
        abi: process.versions.modules,
        sourcePackageSha256: sha256(await readFile(path.join(source, 'package.json'))),
        sourceLockSha256: sha256(await readFile(path.join(source, 'pnpm-lock.yaml'))),
        files,
      },
      null,
      2,
    ) + '\n',
  );
  run(process.execPath, [
    path.join(bundle, 'scripts/audit-legacy-archive.mjs'),
    '--verify-package',
  ]);
  const name = `novelai-legacy-tools-${sourceCommit.slice(0, 7)}-${process.platform}-${process.arch}.tar.gz`;
  run('tar', ['-czf', path.join(work, name), '-C', work, 'legacy-tools']);
  const extracted = path.join(work, 'unpacked');
  await mkdir(extracted);
  run('tar', ['-xzf', path.join(work, name), '-C', extracted]);
  run(process.execPath, [
    path.join(extracted, 'legacy-tools/scripts/audit-legacy-archive.mjs'),
    '--verify-package',
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
    !path.basename(work).startsWith('novelai-legacy-tools-')
  ) {
    throw new Error('Refusing to remove an unexpected legacy tools staging path.');
  }
  await rm(work, { recursive: true, force: true });
}
