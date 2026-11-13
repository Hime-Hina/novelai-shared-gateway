import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFile, readdir, writeFile, unlink, stat, symlink } from 'node:fs/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import path from 'node:path';
import Database from 'better-sqlite3';
import { fixture, meta, keys, NOW, image } from '../helpers/relational-generation.mjs';
import { assertRelationalStorageReady } from '../../server/storage/relational-storage.ts';
import {
  sameRelationalRows,
  relationalSnapshotSha256,
} from '../../server/storage/relational-private-restore.ts';
import { readRelationalArtifactContent } from '../../server/storage/relational-replica-io.ts';
import {
  authenticateMemberApiKey,
  withAuthenticatedMemberApiKey,
} from '../../server/members/authentication.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';

const entry = process.env.RESTORE_RELATIONAL_TEST_ENTRY
  ? path.resolve(process.env.RESTORE_RELATIONAL_TEST_ENTRY)
  : fileURLToPath(new URL('../../scripts/restore-relational.mjs', import.meta.url));
const digest = (bytes) => createHash('sha256').update(bytes).digest('hex');

async function selected(f, name = 'selected') {
  const source = path.join(f.dir, `${name}.sqlite`);
  await f.db.backup(source);
  const seal = new Database(source);
  seal.pragma('journal_mode=DELETE');
  seal.close();
  return {
    source,
    sha256: relationalSnapshotSha256(source),
    history: 'generation-history',
    cut: f.db.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits').get().seq,
    output: path.join(f.dir, 'restored.sqlite'),
    'data-dir': f.dir,
  };
}

function run(config, { arguments: extra = [], preload } = {}) {
  const env = { ...process.env, NODE_NO_WARNINGS: '1' };
  for (const key of Object.keys(env)) {
    if (/^(R2_|PIKPAK_|NOVELAI_|ADMIN_TOKEN$|NODE_OPTIONS$)/.test(key)) {
      delete env[key];
    }
  }
  const args = [
    ...(process.env.RESTORE_RELATIONAL_TEST_ENTRY ? ['--no-experimental-strip-types'] : []),
    ...(preload ? ['--import', pathToFileURL(preload).href] : []),
    entry,
    ...Object.entries(config).flatMap(([key, value]) => [`--${key}`, value]),
    ...extra,
  ];
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, args, { env, windowsHide: true });
    let stdout = '',
      stderr = '';
    child.stdout.setEncoding('utf8').on('data', (chunk) => (stdout += chunk));
    child.stderr.setEncoding('utf8').on('data', (chunk) => (stderr += chunk));
    child.on('error', reject);
    child.on('close', (code, signal) => resolve({ code, signal, stdout, stderr }));
  });
}

async function assertNoPublication(config, before, result, phase) {
  assert.equal(result.code, 1, JSON.stringify(result));
  assert.equal(result.signal, null);
  assert.equal(result.stdout, '');
  const failure = JSON.parse(result.stderr);
  assert.deepEqual(failure, {
    restored: false,
    code: 'RELATIONAL_RESTORE_FAILED',
    phase,
  });
  await assert.rejects(stat(config.output), { code: 'ENOENT' });
  assert.equal(digest(await readFile(config.source)), before);
  assert.deepEqual(
    (await readdir(path.dirname(config.output))).filter((name) =>
      name.startsWith('.restore-relational-'),
    ),
    [],
  );
}

function compareTables(source, restored) {
  for (const { name } of source
    .prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*'")
    .all()) {
    const columns = source.prepare(`PRAGMA table_info("${name}")`).all();
    const selected = columns.filter(
      (column) => !(name === 'projection_state' && column.name === 'status'),
    );
    const keys = columns.filter((column) => column.pk).sort((a, b) => a.pk - b.pk);
    assert.equal(
      sameRelationalRows(
        source,
        restored,
        `SELECT ${selected.map((column) => `"${column.name}"`).join(',')} FROM "${name}" ORDER BY ${keys.map((column) => `"${column.name}"`).join(',')}`,
      ),
      true,
      name,
    );
  }
}

async function completed(t) {
  const f = await fixture(t, { seconds: 0, env: { OWNER_USER_ID: 'm1' } });
  await f.accept('complete');
  f.dispatch('complete-0');
  await f.publish('complete-0');
  return f;
}

test('实际 CLI 分页重建完整业务与所选私有状态，唯一 installer 安装 ready 并核验 Local 字节', async (t) => {
  const f = await completed(t);
  await f.accept('review');
  const dispatch = f.dispatch('review-0', NOW + 20000).dispatch;
  f.fail(
    'review-0',
    {
      errorCode: 'upstream_timeout',
      errorStatus: 504,
      aborted: false,
      diagnostic: { dispatch, error: `protected detail ${dispatch.pst}` },
      completion: { dispatchState: 'request_started', upstreamStartedAt: NOW + 20000 },
    },
    NOW + 20002,
  );
  await f.accept('queued', { queueExpiresAt: NOW + 60000 });
  await withAuthenticatedMemberApiKey(
    f.members,
    keys.m1,
    (actor) =>
      f.generations.retireJobHistory(
        meta('retire-complete', NOW + 30000, actor.id),
        ['complete-0'],
        actor,
        new Map(),
      ),
    { OWNER_USER_ID: 'm1' },
  );
  // Cross a page boundary using real commands, rather than a synthetic replay callback.
  for (let index = 0; index < 130; index++) {
    f.db.transaction(() =>
      f.members.registerMember(meta(`page-${index}`), {
        memberId: `page-${index}`,
        displayName: `page-${index}`,
        apiKey: `synthetic-page-key-${index}`,
      }),
    )();
  }
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'selected-new-private-key'))();
  const config = await selected(f),
    before = config.sha256,
    result = await run(config);
  assert.equal(result.code, 0, JSON.stringify(result));
  assert.equal(result.stderr, '');
  const receipt = JSON.parse(result.stdout);
  assert.equal(receipt.restored, true);
  assert.ok(BigInt(receipt.commitsRestored) > 128n);
  assert.equal(receipt.artifactsVerified, 1);
  assert.ok(!JSON.stringify(result).includes('protected detail'));
  assert.ok(!JSON.stringify(result).includes('synthetic-upstream-pst'));
  assert.ok(!JSON.stringify(result).includes('selected-new-private-key'));
  assert.equal(relationalSnapshotSha256(config.source), before);
  assert.equal(relationalSnapshotSha256(config.output).length, 64);
  const original = new Database(config.source, { readonly: true }),
    restored = new Database(config.output, { readonly: true });
  try {
    assertRelationalStorageReady(restored);
    compareTables(original, restored);
    assert.equal(restored.prepare('SELECT status FROM projection_state').get().status, 'ready');
    assert.equal(
      restored.prepare("SELECT queue_expires_at FROM generation_jobs WHERE id='queued-0'").get()
        .queue_expires_at,
      NOW + 60000,
    );
    assert.equal(
      restored.prepare("SELECT retired_at FROM generation_jobs WHERE id='complete-0'").get()
        .retired_at,
      NOW + 30000,
    );
    assert.equal(
      restored
        .prepare("SELECT review_status FROM generation_attempts WHERE job_id='review-0'")
        .get().review_status,
      'pending',
    );
    assert.equal(restored.prepare('SELECT COUNT(*) AS n FROM generation_diagnostics').get().n, 1);
    const members = new RelationalMemberStore(restored);
    assert.equal(await authenticateMemberApiKey(members, keys.m1), null);
    assert.equal((await authenticateMemberApiKey(members, 'selected-new-private-key')).id, 'm1');
    assert.deepEqual(
      (await readRelationalArtifactContent(restored, 'artifact-complete-0', { dataDir: f.dir }))
        .buffer,
      image,
    );
  } finally {
    original.close();
    restored.close();
  }
  if (process.platform !== 'win32') {
    assert.equal((await stat(config.output)).mode & 0o777, 0o600);
  }
  assert.deepEqual(
    (await readdir(f.dir)).filter((name) => name.startsWith('.restore-relational-')),
    [],
  );
});

test('错 SHA、cut、history 与未知格式均拒绝且源字节不变', async (t) => {
  for (const kind of ['sha256', 'cut', 'history', 'format']) {
    await t.test(kind, async (t) => {
      const f = await fixture(t),
        config = await selected(f);
      if (kind === 'format') {
        const db = new Database(config.source);
        db.prepare("UPDATE app_meta SET value='unknown-format' WHERE key='database_format'").run();
        db.close();
        config.sha256 = relationalSnapshotSha256(config.source);
      } else {
        config[kind] =
          kind === 'sha256' ? '0'.repeat(64) : kind === 'cut' ? '999999' : 'other-history';
      }
      const before = digest(await readFile(config.source));
      await assertNoPublication(config, before, await run(config), 'source');
    });
  }
});

test('缺保护正文与重放后的对象验证失败不发布 ready，也不改选定来源', async (t) => {
  for (const kind of ['input', 'diagnostic', 'object']) {
    await t.test(kind, async (t) => {
      const f = await completed(t);
      if (kind === 'diagnostic') {
        await f.accept('diagnostic');
        const dispatch = f.dispatch('diagnostic-0', NOW + 20000).dispatch;
        f.fail(
          'diagnostic-0',
          { aborted: false, diagnostic: { dispatch, error: 'private body' } },
          NOW + 20002,
        );
      }
      const config = await selected(f);
      if (kind === 'object') {
        await unlink(path.join(f.dir, 'images', 'artifact-complete-0.webp'));
      } else {
        const db = new Database(config.source),
          table = kind === 'input' ? 'generation_inputs' : 'generation_diagnostics';
        db.pragma('foreign_keys=OFF');
        const triggers = db
          .prepare("SELECT name,sql FROM sqlite_schema WHERE type='trigger' AND tbl_name=?")
          .all(table);
        for (const { name } of triggers) {
          db.exec(`DROP TRIGGER "${name}"`);
        }
        db.prepare(`DELETE FROM ${table}`).run();
        for (const { sql } of triggers) {
          db.exec(sql);
        }
        db.close();
        config.sha256 = relationalSnapshotSha256(config.source);
      }
      await assertNoPublication(
        config,
        config.sha256,
        await run(config),
        kind === 'object' ? 'verify' : 'replay',
      );
    });
  }
});

test('既有 output、sidecar、源路径及重复参数不会被覆盖', async (t) => {
  for (const kind of [
    '',
    '-wal',
    '-shm',
    '-journal',
    'source',
    'source-wal',
    'source-shm',
    'source-journal',
    'duplicate',
  ]) {
    await t.test(kind || 'output', async (t) => {
      const f = await fixture(t),
        config = await selected(f),
        sentinel = 'existing private file';
      if (kind.startsWith('source')) {
        config.output = config.source + kind.slice('source'.length);
      } else if (kind !== 'duplicate') {
        await writeFile(config.output + kind, sentinel, { flag: 'wx' });
      }
      const result = await run(config, {
        arguments: kind === 'duplicate' ? ['--history', 'secret-duplicate-value'] : [],
      });
      assert.equal(result.code, 1, JSON.stringify(result));
      assert.equal(result.stdout, '');
      assert.deepEqual(JSON.parse(result.stderr), {
        restored: false,
        code: 'RELATIONAL_RESTORE_FAILED',
        phase: kind === 'duplicate' ? 'arguments' : 'paths',
      });
      assert.equal(relationalSnapshotSha256(config.source), config.sha256);
      if (!kind.startsWith('source') && kind !== 'duplicate') {
        assert.equal(await readFile(config.output + kind, 'utf8'), sentinel);
      }
      assert.deepEqual(
        (await readdir(f.dir)).filter((name) => name.startsWith('.restore-relational-')),
        [],
      );
    });
  }
});

test('真实子进程发布竞态和暂存清理故障均撤回本次输出，保留外部既有文件', async (t) => {
  for (const kind of ['race', 'cleanup', 'replacement']) {
    await t.test(kind, async (t) => {
      const f = await completed(t),
        config = await selected(f),
        preload = path.join(f.dir, 'publication-fault.mjs');
      // Fault only the filesystem publication boundary; all replay/installer/Local IO stays real.
      await writeFile(
        preload,
        `import fs from 'node:fs/promises';
const link = fs.link, rm = fs.rm;
${
  kind === 'race'
    ? `fs.link = async (source, output) => {
  await fs.writeFile(output, 'racing output', {flag: 'wx'});
  return link(source, output);
};`
    : `let failed = false;
fs.rm = async (filename, options) => {
  if (!failed && String(filename).includes('.restore-relational-')) {
    failed = true;
    ${
      kind === 'replacement'
        ? `await fs.unlink(${JSON.stringify(config.output)});
    await fs.writeFile(${JSON.stringify(config.output)}, 'external replacement', {flag: 'wx'});`
        : ''
    }
    throw new Error('secret cleanup failure');
  }
  return rm(filename, options);
};`
}
import {syncBuiltinESMExports} from 'node:module';
syncBuiltinESMExports();
`,
      );
      const result = await run(config, { preload });
      assert.equal(result.code, 1, JSON.stringify(result));
      assert.equal(result.stdout, '');
      assert.deepEqual(JSON.parse(result.stderr), {
        restored: false,
        code: 'RELATIONAL_RESTORE_FAILED',
        phase: 'publish',
      });
      if (kind === 'race' || kind === 'replacement') {
        assert.equal(
          await readFile(config.output, 'utf8'),
          kind === 'race' ? 'racing output' : 'external replacement',
        );
      } else {
        await assert.rejects(stat(config.output), { code: 'ENOENT' });
      }
      assert.equal(relationalSnapshotSha256(config.source), config.sha256);
      assert.deepEqual(
        (await readdir(f.dir)).filter((name) => name.startsWith('.restore-relational-')),
        [],
      );
    });
  }
});

test('目录别名、Windows 大小写和 namespace 不会把 output 变成源 sidecar', async (t) => {
  for (const kind of ['directory', 'case', 'namespace']) {
    await t.test(
      kind,
      { skip: kind !== 'directory' && process.platform !== 'win32' },
      async (t) => {
        const f = await fixture(t),
          config = await selected(f);
        if (kind === 'directory') {
          const alias = path.join(f.dir, 'alias');
          await symlink(f.dir, alias, process.platform === 'win32' ? 'junction' : 'dir');
          config.source = path.join(alias, 'selected.sqlite');
          config.output = path.join(alias, 'selected.sqlite-journal');
        } else if (kind === 'case') {
          config.output = config.source.toUpperCase() + '-WAL';
        } else {
          config.output = path.toNamespacedPath(config.source) + '-shm';
        }
        await assertNoPublication(config, config.sha256, await run(config), 'paths');
      },
    );
  }
});
