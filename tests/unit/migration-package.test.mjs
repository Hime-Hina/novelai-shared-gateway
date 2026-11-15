import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import {
  migrationPackageFiles,
  verifyMigrationPackage,
} from '../../scripts/verify-migration-package.mjs';

test('migration package verifies exact files and rejects changed or extra runtime code', async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), 'novelai-migration-manifest-test-'));
  t.after(async () => {
    assert.equal(path.dirname(path.resolve(directory)), path.resolve(tmpdir()));
    assert.ok(path.basename(directory).startsWith('novelai-migration-manifest-test-'));
    await rm(directory, { recursive: true, force: true });
  });
  await mkdir(path.join(directory, 'scripts'));
  const entry = path.join(directory, 'scripts/migrate-legacy-database.mjs');
  await writeFile(entry, 'export const value = 1;\n');
  const manifest = {
    format: 'novelai-migration-tools-v1',
    sourceCommit: 'a'.repeat(40),
    sourceTree: 'b'.repeat(40),
    node: process.version,
    platform: process.platform,
    arch: process.arch,
    abi: process.versions.modules,
    files: await migrationPackageFiles(directory),
  };
  await writeFile(path.join(directory, 'MIGRATION_TOOLS.json'), JSON.stringify(manifest));
  assert.equal((await verifyMigrationPackage(directory)).sourceCommit, manifest.sourceCommit);
  await writeFile(entry, 'export const value = 2;\n');
  await assert.rejects(verifyMigrationPackage(directory), /identity or bytes/);
  await writeFile(entry, 'export const value = 1;\n');
  await writeFile(path.join(directory, 'scripts/unrecorded.mjs'), 'export {};\n');
  await assert.rejects(verifyMigrationPackage(directory), /identity or bytes/);
});
