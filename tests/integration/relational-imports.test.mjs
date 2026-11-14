import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readdir, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import Database from 'better-sqlite3';
import { assertRelationalStorageReady } from '../../server/storage/relational-storage.ts';

const root = new URL('../..', import.meta.url),
  // 子进程首次 import 即拦截，不以「没有实例化 JsonStore」替代没有加载旧模块。
  rejectLegacy = `data:text/javascript,${encodeURIComponent(String.raw`
    import {registerHooks} from 'node:module';
    registerHooks({load(url, context, nextLoad) {
      if (/\/server\/storage\/sqlite-(?:store|migrations)\.(?:ts|js)$/.test(new URL(url).pathname)) {
        process.stderr.write('blocked legacy storage load: '+url+'\n');
        throw new Error('Legacy storage must not load in relational entry points.');
      }
      return nextLoad(url, context);
    }});
  `)}`;
function child(args, env) {
  return spawnSync(process.execPath, ['--import', rejectLegacy, ...args], {
    cwd: root,
    env: { ...process.env, ...env },
    encoding: 'utf8',
    timeout: 30000,
  });
}
async function relationalRoots(directory = new URL('server/', root)) {
  const roots = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const filename = new URL(entry.name + (entry.isDirectory() ? '/' : ''), directory);
    if (entry.isDirectory()) {
      roots.push(...(await relationalRoots(filename)));
    } else if (/^relational-.*\.ts$/.test(entry.name)) {
      roots.push(filename.href);
    }
  }
  return roots;
}

test('子进程 loader 真实拒绝旧 Store/migrations，全部关系 adapters/stores/runtime 入口仍可实际 import', async () => {
  for (const filename of ['sqlite-store.ts', 'sqlite-migrations.ts']) {
    const refused = child([
      '--input-type=module',
      '-e',
      `await import(${JSON.stringify(new URL(`server/storage/${filename}`, root).href)});`,
    ]);
    assert.equal(refused.status, 1);
    assert.match(refused.stderr, /blocked legacy storage load/);
    assert.ok(refused.stderr.includes(filename));
  }
  const roots = await relationalRoots(),
    imported = child([
      '--input-type=module',
      '-e',
      `for (const filename of ${JSON.stringify(roots)}) { await import(filename); } console.log(${roots.length});`,
    ]);
  assert.ok(roots.some((filename) => filename.endsWith('/relational-account-store.ts')));
  assert.ok(roots.some((filename) => filename.endsWith('/relational-queue-runtime.ts')));
  assert.ok(roots.some((filename) => filename.endsWith('/relational-openai-http.ts')));
  assert.equal(imported.status, 0, imported.stderr);
  assert.equal(imported.stdout.trim(), String(roots.length));
  assert.ok(!imported.stderr.includes('blocked legacy storage load'));
});

test('真实 bootstrap CLI 在禁止加载旧 runtime 下完成唯一 installer ready，原离线/私有凭据交付不变', async (t) => {
  const dir = await mkdtemp(path.join(tmpdir(), 'nai-relational-import-bootstrap-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const env = {
      DATA_DIR: dir,
      ADMIN_TOKEN: 'synthetic-relational-import-admin-'.repeat(2),
      NOVELAI_TOKEN: 'pst-synthetic-relational-import-upstream',
    },
    run = child(['scripts/bootstrap.mjs'], env);
  assert.equal(run.status, 0, run.stderr);
  assert.ok(!run.stderr.includes('blocked legacy storage load'));
  const saved = JSON.parse(await readFile(path.join(dir, 'bootstrap-credentials.json'), 'utf8')),
    db = new Database(path.join(dir, 'library.sqlite'));
  try {
    assertRelationalStorageReady(db);
    assert.equal(db.prepare('SELECT COUNT(*) AS n FROM members').get().n, 4);
    assert.equal(db.prepare('SELECT COUNT(*) AS n FROM generation_jobs').get().n, 0);
    assert.equal(saved.accounts.length, 4);
    for (const secret of [
      env.NOVELAI_TOKEN,
      env.ADMIN_TOKEN,
      ...saved.accounts.flatMap((member) => [member.password, member.api_key]),
    ]) {
      assert.ok(!run.stdout.includes(secret));
      assert.ok(!run.stderr.includes(secret));
    }
  } finally {
    db.close();
  }
});

test('旧实际 caller 的临时 exports 与共享纯 owner 是同一函数，原 JSON/ID/context 语义保留', async (t) => {
  const legacy = await import('../../server/storage/sqlite-store.ts'),
    values = await import('../../server/storage/record-values.ts'),
    identity = await import('../../server/generation/request-fingerprint.ts');
  assert.equal(legacy.createId, values.createId);
  assert.equal(legacy.hashObject, values.hashObject);
  assert.equal(legacy.generationContextRef, identity.generationContextRef);
  t.mock.method(Date, 'now', () => 1800000000000);
  assert.match(values.createId(), /^item_mywpiww0_[0-9a-f]{10}$/);
  assert.match(values.createId('job'), /^job_mywpiww0_[0-9a-f]{10}$/);
  assert.equal(
    values.hashObject(null),
    '74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b',
  );
  assert.notEqual(values.hashObject({ a: 1, b: 2 }), values.hashObject({ b: 2, a: 1 }));
  assert.throws(() => values.hashObject(undefined), TypeError);
  assert.throws(() => values.hashObject(1n), TypeError);
  const circular = {};
  circular.self = circular;
  assert.throws(() => values.hashObject(circular), TypeError);
  assert.equal(
    identity.generationContextRef({
      member_id: 'member:a',
      operation: 'generate',
      key_hash: 'b:c',
    }),
    'generation-idempotency:member:a:generate:b:c',
  );
});
