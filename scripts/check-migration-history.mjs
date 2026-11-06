import { execFileSync } from 'node:child_process';
import { readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { isDeepStrictEqual } from 'node:util';

export const releasedMigrationBase = '7c55c6dad7ec59b04550ee9e2f1a0db6047db791';
const directory = 'server/migrations';
const journalPath = `${directory}/meta/_journal.json`;

export function checkMigrationHistory(root = process.cwd(), base = releasedMigrationBase) {
  const git = (args) => execFileSync('git', args, { cwd: root, stdio: ['ignore', 'pipe', 'pipe'] });
  // Resolve a caller-supplied trusted Git object, never a PR-regenerated hash manifest.
  const commit = git(['rev-parse', '--verify', `${base}^{commit}`])
    .toString()
    .trim();
  const previous = JSON.parse(git(['show', `${commit}:${journalPath}`]).toString());
  const current = JSON.parse(readFileSync(path.join(root, journalPath), 'utf8'));
  const errors = [];
  const files = git(['ls-tree', '-r', '--name-only', commit, '--', directory])
    .toString()
    .trim()
    .split('\n');
  for (const file of files.filter(
    (file) => file.endsWith('.sql') || /meta\/\d+_snapshot\.json$/.test(file),
  )) {
    try {
      if (!git(['show', `${commit}:${file}`]).equals(readFileSync(path.join(root, file)))) {
        errors.push(`${file}: historical migration bytes changed; append a new migration.`);
      }
    } catch {
      errors.push(`${file}: historical migration missing; restore it and append a new migration.`);
    }
  }
  if (!isDeepStrictEqual({ ...previous, entries: undefined }, { ...current, entries: undefined })) {
    errors.push(`${journalPath}: historical journal metadata changed.`);
  }
  if (!Array.isArray(current.entries) || current.entries.length < previous.entries.length) {
    errors.push(`${journalPath}: historical journal entries removed.`);
  }
  for (const [index, entry] of previous.entries.entries()) {
    if (!isDeepStrictEqual(entry, current.entries?.[index])) {
      errors.push(`${journalPath}: historical journal entry ${index} changed; append only.`);
    }
  }
  const entries = Array.isArray(current.entries) ? current.entries : [];
  const sqlFiles = readdirSync(path.join(root, directory)).filter((file) => file.endsWith('.sql'));
  for (const [index, entry] of entries.entries()) {
    if (
      entry.idx !== index ||
      !new RegExp(`^${String(index).padStart(4, '0')}_[a-z0-9_]+$`).test(entry.tag) ||
      entry.version !== previous.entries.at(-1).version ||
      typeof entry.breakpoints !== 'boolean' ||
      !Number.isSafeInteger(entry.when) ||
      (index && entry.when <= entries[index - 1].when)
    ) {
      errors.push(`${journalPath}: invalid append order or entry ${index}.`);
    }
    if (!sqlFiles.includes(`${entry.tag}.sql`)) {
      errors.push(`${journalPath}: missing SQL for ${entry.tag}.`);
    }
  }
  for (const file of sqlFiles) {
    if (!entries.some((entry) => `${entry.tag}.sql` === file)) {
      errors.push(`${directory}/${file}: migration must have the next journal entry.`);
    }
  }
  return errors;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const args = process.argv.slice(2);
    if (args.length && (args.length !== 2 || args[0] !== '--base')) {
      throw new Error('Usage: node scripts/check-migration-history.mjs [--base TRUSTED_COMMIT]');
    }
    const errors = checkMigrationHistory(process.cwd(), args[1] || releasedMigrationBase);
    if (errors.length) {
      console.error(errors.join('\n'));
      process.exitCode = 1;
    } else {
      console.log('Migration history unchanged; appended migrations valid.');
    }
  } catch (error) {
    console.error(`Cannot verify trusted migration history: ${error.message}`);
    process.exitCode = 1;
  }
}
