import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { packageBackend } from '../../scripts/package-backend.mjs';

async function fixture(t) {
  const root = await mkdtemp(path.join(tmpdir(), 'nai-package-closure-')),
    output = path.join(root, '.output/backend');
  t.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(
    path.join(root, 'package.json'),
    JSON.stringify({
      name: 'synthetic',
      type: 'module',
      dependencies: { 'production-package': '1' },
      devDependencies: { typescript: '6' },
      scripts: { test: 'not a release command' },
    }),
  );
  const put = async (name, value) => {
    const filename = path.join(output, name);
    await mkdir(path.dirname(filename), { recursive: true });
    await writeFile(filename, value);
  };
  await put('server/index.js', "import './runtime.js';");
  await put('server/runtime.js', "export { value } from './shared.js';");
  await put('server/shared.js', 'export const value = 1;');
  await put('scripts/bootstrap.mjs', "await import('../server/shared.js');");
  await put('scripts/restore-relational.mjs', "import '../server/shared.js';");
  await put('scripts/r2-backup.mjs', "import '../server/shared.js';");
  await put('scripts/mock-novelai.mjs', "import '../server/shared.js';");
  await put('server/storage/sqlite-store.js', 'throw new Error("unreachable old owner");');
  await put('server/migrations/old.sql', 'not a normal runtime asset');
  await put('scripts/replay-events.mjs', 'throw new Error("unreachable old tool");');
  return { root, output, put };
}

async function files(root, directory = '') {
  const names = [];
  for (const entry of await readdir(path.join(root, directory), { withFileTypes: true })) {
    const name = path.posix.join(directory, entry.name);
    names.push(...(entry.isDirectory() ? await files(root, name) : [name]));
  }
  return names.sort();
}

test('普通包只保存真实入口和运维 CLI 的完整运行闭包，包含动态模块而不保留全量发射或旧资产', async (t) => {
  const f = await fixture(t);
  await f.put(
    'server/shared.js',
    "import 'node:fs'; import 'production-package/subpath'; export const value = 1; await import('./legacy-name.js');",
  );
  await f.put(
    'server/legacy-name.js',
    "import './shared.js'; new URL('./worker.js', import.meta.url);",
  );
  await f.put('server/worker.js', 'export const worker = true;');
  await packageBackend(f.root);
  assert.deepEqual(await files(f.output), [
    'package.json',
    'scripts/bootstrap.mjs',
    'scripts/mock-novelai.mjs',
    'scripts/r2-backup.mjs',
    'scripts/restore-relational.mjs',
    'server/index.js',
    'server/legacy-name.js',
    'server/runtime.js',
    'server/shared.js',
    'server/worker.js',
  ]);
  const manifest = JSON.parse(await readFile(path.join(f.output, 'package.json'), 'utf8'));
  assert.equal(manifest.main, 'server/index.js');
  assert.deepEqual(manifest.scripts, {
    start: 'node server/index.js',
    bootstrap: 'node scripts/bootstrap.mjs',
    restore: 'node scripts/restore-relational.mjs',
  });
  assert.equal(manifest.devDependencies, undefined);
  await f.put('server/stale.js', 'from an earlier build');
  await packageBackend(f.root);
  assert.equal((await files(f.output)).includes('server/stale.js'), false);
});

test('缺失、源TS、越界、计算依赖及dev-only依赖先拒绝，不能悄悄交付残缺发布包', async (t) => {
  for (const [source, error] of [
    ["import './missing.js';", /ENOENT/],
    ["import './shared.ts';", /Invalid packaged runtime dependency/],
    ["import '../../outside.js';", /Invalid packaged runtime dependency/],
    ['await import(process.env.RUNTIME_MODULE);', /literal dependency/],
    ['new URL(process.env.RUNTIME_MODULE, import.meta.url);', /literal dependency/],
    ["import 'typescript';", /not a production dependency/],
  ]) {
    await t.test(source, async (t) => {
      const f = await fixture(t);
      await f.put('server/runtime.js', source);
      await assert.rejects(packageBackend(f.root), error);
      assert.ok((await files(f.output)).includes('server/storage/sqlite-store.js'));
    });
  }
});

test('执行器复制同一普通包，重建删除旧产物且不再混入源脚本或旧迁移', async (t) => {
  const f = await fixture(t);
  await packageBackend(f.root);
  const put = async (name, contents) => {
    const filename = path.join(f.root, name);
    await mkdir(path.dirname(filename), { recursive: true });
    await writeFile(filename, contents);
  };
  await put(
    'executor/scripts/package-runtime.mjs',
    await readFile(new URL('../../executor/scripts/package-runtime.mjs', import.meta.url)),
  );
  for (const name of [
    'LICENSE',
    'NOTICE.md',
    'UPSTREAM-LICENSE',
    'executor/start.mjs',
    'executor/node_modules/nitropack/LICENSE',
    'executor/node_modules/h3/LICENSE',
  ])
    await put(name, 'synthetic license or entry');
  await put('scripts/old-source.mjs', 'not a runtime root');
  await put('server/migrations/old.sql', 'not a normal asset');
  await put(
    'executor/.output/server/node_modules/novelai-shared-gateway/scripts/replay-events.mjs',
    'stale old package',
  );
  const result = spawnSync(process.execPath, ['executor/scripts/package-runtime.mjs'], {
    cwd: f.root,
    encoding: 'utf8',
  });
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(
    await files(path.join(f.root, 'executor/.output/server/node_modules/novelai-shared-gateway')),
    [...(await files(f.output)), 'LICENSE', 'NOTICE.md', 'UPSTREAM-LICENSE'].sort(),
  );
});
