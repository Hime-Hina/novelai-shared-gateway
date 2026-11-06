import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { checkMigrationHistory } from '../../scripts/check-migration-history.mjs';

test('trusted Git history rejects changed/deleted SQL and rewritten journal, but allows a legal append', () => {
  const root = mkdtempSync(path.join(tmpdir(), 'migration-history-'));
  const write = (name, value) => writeFileSync(path.join(root, 'server/migrations', name), value);
  const journal = {
    version: '7',
    dialect: 'sqlite',
    entries: [{ idx: 0, version: '6', when: 100, tag: '0000_initial', breakpoints: true }],
  };
  const sql = 'CREATE TABLE initial (id TEXT);\n';
  const git = (args) =>
    execFileSync('git', args, { cwd: root, stdio: ['ignore', 'pipe', 'pipe'] })
      .toString()
      .trim();
  try {
    mkdirSync(path.join(root, 'server/migrations/meta'), { recursive: true });
    write('0000_initial.sql', sql);
    write('meta/_journal.json', JSON.stringify(journal));
    git(['init']);
    git(['add', '.']);
    git([
      '-c',
      'user.name=Fixture',
      '-c',
      'user.email=fixture@example.invalid',
      'commit',
      '-m',
      'trusted release',
    ]);
    const base = git(['rev-parse', 'HEAD']);
    assert.deepEqual(checkMigrationHistory(root, base), []);
    write('0000_initial.sql', `${sql}-- rewritten history\n`);
    assert.match(
      checkMigrationHistory(root, base).join('\n'),
      /historical migration bytes changed/,
    );
    // A PR can commit its rewrite; the externally supplied base still detects it.
    git(['add', '.']);
    git([
      '-c',
      'user.name=Fixture',
      '-c',
      'user.email=fixture@example.invalid',
      'commit',
      '-m',
      'rewrite',
    ]);
    assert.match(
      checkMigrationHistory(root, base).join('\n'),
      /historical migration bytes changed/,
    );
    rmSync(path.join(root, 'server/migrations/0000_initial.sql'));
    assert.match(checkMigrationHistory(root, base).join('\n'), /historical migration missing/);
    write('0000_initial.sql', sql);
    write(
      'meta/_journal.json',
      JSON.stringify({ ...journal, entries: [{ ...journal.entries[0], when: 101 }] }),
    );
    assert.match(
      checkMigrationHistory(root, base).join('\n'),
      /historical journal entry 0 changed/,
    );
    const next = { idx: 1, version: '6', when: 200, tag: '0001_next', breakpoints: true };
    write(
      'meta/_journal.json',
      JSON.stringify({ ...journal, entries: [...journal.entries, next] }),
    );
    write('0001_next.sql', 'CREATE TABLE next (id TEXT);\n');
    assert.deepEqual(checkMigrationHistory(root, base), []);
    write(
      'meta/_journal.json',
      JSON.stringify({ ...journal, entries: [...journal.entries, { ...next, when: 99 }] }),
    );
    assert.match(checkMigrationHistory(root, base).join('\n'), /invalid append order/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
