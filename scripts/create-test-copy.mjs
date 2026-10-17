import { constants, createReadStream } from 'node:fs';
import { chmod, copyFile, link, lstat, mkdir, mkdtemp, realpath, rmdir, unlink, writeFile } from 'node:fs/promises';
import { randomBytes } from 'node:crypto';
import path from 'node:path';
import { parseArgs } from 'node:util';
import { pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';
import { migrateStorage } from '../server/storage-migrations.ts';
import { recordTables } from '../server/storage-schema.ts';
import { sqlRecord } from '../server/store.ts';
import { setMemberCredentials } from '../server/member-auth.ts';
import { settleFairQuota } from '../server/fair-quota.ts';
import { closeGenerationReview } from '../server/generation-accounting.ts';

export const TEST_COPY_UPSTREAM_TOKEN = 'pst-synthetic-d01-mock-upstream-only';
const collections = Object.keys(recordTables);
const tables = [...collections, 'app_settings', 'app_meta', 'app_records', 'generation_requests',
  'generation_idempotency', 'fair_quota_settlements', 'member_sessions', '__drizzle_migrations'];
const active = new Set(['queued', 'pending', 'running']);
const secretFields = new Set(['apiKey', 'api_key', 'password', 'accessToken', 'refreshToken', 'access_token', 'refresh_token',
  'clientSecret', 'client_secret', 'secret', 'image_cache_secret_key', 'mask_cache_secret_key', 'reference_image_cache_secret_key']);
const proxyFields = new Set(['proxyUrl', 'socksProxy', 'proxy']);
const jsonColumns = { ...Object.fromEntries(collections.map(name => [name, 'data'])),
  app_settings: 'value', app_records: 'data', generation_requests: 'payload',
  generation_idempotency: 'normalization_context', fair_quota_settlements: 'charge' };
const message = 'Isolated in an offline test copy; original production task will not be replayed.';
class CopyError extends Error {
  constructor(code, counts = {}) { super(code); this.code = code; this.counts = counts; }
}
function parse(text) {
  try { return JSON.parse(text); } catch { throw new CopyError('invalid_stored_json'); }
}
function* records(db, table) {
  const select = db.prepare(`SELECT rowid AS _rowid, * FROM ${table} WHERE rowid > ? ORDER BY rowid LIMIT 256`);
  let after = -Infinity;
  for (;;) {
    const batch = select.all(after);
    if (!batch.length) return;
    after = batch.at(-1)._rowid;
    yield* batch;
  }
}
function count(db, table) { return db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n; }

// This operates on a protected full SQLite copy, never JsonStore's partial runtime cache.
export async function createTestCopy({ source, target, imagesDir, now = Date.now() }) {
  if (!source || !target) throw new CopyError('source_and_target_required');
  const sourcePath = await realpath(source);
  if (!(await lstat(sourcePath)).isFile()) throw new CopyError('source_must_be_a_file');
  const targetPath = path.join(await realpath(path.dirname(path.resolve(target))), path.basename(path.resolve(target)));
  await mkdir(targetPath, { mode: 0o700 }); // Deliberately not recursive: existing targets are never overwritten.
  await chmod(targetPath, 0o700);
  const workDir = await mkdtemp(path.join(targetPath, '.work-'));
  await chmod(workDir, 0o700);
  const workPath = path.join(workDir, 'source.sqlite'), cleanPath = path.join(workDir, 'clean.sqlite');
  const finalPath = path.join(targetPath, 'library.sqlite');
  let sourceDb, db, published = false;
  const summary = { isolated: 0, refunded: 0, reviewsClosed: 0, imagesCopied: 0, imagesMissing: 0 };
  try {
    sourceDb = new Database(sourcePath, { readonly: true, fileMustExist: true });
    sourceDb.pragma('query_only = ON');
    await sourceDb.backup(workPath);
    sourceDb.close(); sourceDb = null;
    await chmod(workPath, 0o600);
    db = new Database(workPath);
    db.pragma('journal_mode = DELETE');
    migrateStorage(db);
    const unknownTables = db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'")
      .all().filter(row => !tables.includes(row.name));
    if (unknownTables.length) throw new CopyError('unsupported_tables', { tables: unknownTables.length });
    // A legacy generic-only database needs its existing importer before this tool can close reservations.
    if (count(db, 'app_records') && !collections.some(table => count(db, table))) throw new CopyError('legacy_import_required');
    summary.before = Object.fromEntries(tables.map(table => [table, count(db, table)]));
    const settingsRow = db.prepare("SELECT value FROM app_settings WHERE key='settings'").get();
    if (!settingsRow) throw new CopyError('settings_missing');
    const state = { settings: parse(settingsRow.value), users: [...records(db, 'users')].map(row => parse(row.data)), ledger: [] };
    const accounts = [...records(db, 'accounts')].map(row => parse(row.data));
    const replacements = new Map();
    const remember = (value, replacement = '[removed-from-test-copy]') => {
      if (typeof value === 'string' && value && value !== replacement && !replacements.has(value)) replacements.set(value, replacement);
    };
    for (const account of accounts) remember(account.token, TEST_COPY_UPSTREAM_TOKEN);
    const credentials = [], users = new Map(), usernames = new Set();
    for (const [index, original] of state.users.entries()) {
      const password = randomBytes(24).toString('base64url'), token = `STA1N-${randomBytes(24).toString('base64url')}`;
      remember(original.token, token);
      for (const value of Object.values(original.memberAuth || {})) if (value !== original.memberAuth?.username) remember(value);
      const user = { ...original, enabled: true };
      const username = /^[a-z0-9][a-z0-9_.-]{2,39}$/.test(original.memberAuth?.username || '')
        ? original.memberAuth.username : `test_member_${index + 1}_${randomBytes(4).toString('hex')}`;
      if (usernames.has(username)) throw new CopyError('duplicate_member_username');
      usernames.add(username);
      await setMemberCredentials({ users: [user] }, user.id, { username, password });
      user.enabled = original.enabled;
      users.set(user.id, { memberAuth: user.memberAuth, token, acceptingJobs: false,
        jobAdmissionVersion: Number(original.jobAdmissionVersion || 0) + 1 });
      credentials.push({ user_id: user.id, username, password, api_key: token });
    }
    const discover = (value, parent = '') => {
      if (!value || typeof value !== 'object') return;
      for (const [key, item] of Object.entries(value)) {
        if (secretFields.has(key) || proxyFields.has(key) || (parent === 'memberAuth' && key !== 'username') || (parent === 'discordInvite' && key === 'hash')) remember(item);
        if (['token', 'userToken', 'user_token'].includes(key) && typeof item === 'string')
          remember(item, item.startsWith('pst-') ? TEST_COPY_UPSTREAM_TOKEN : `STA1N-${randomBytes(24).toString('base64url')}`);
        if (key === 'code' && parent === 'cards') remember(item, randomBytes(24).toString('base64url'));
        if (key === 'baseUrl' && parent === 'promptApi' || key === 'publicBaseUrl') remember(item);
        discover(item, key);
      }
    };
    for (const [table, column] of Object.entries(jsonColumns)) for (const row of records(db, table)) discover(parse(row[column]), table === 'app_records' ? row.collection : table);
    for (const table of collections) for (const row of records(db, table)) {
      for (const field of ['token', 'user_token']) remember(row[field], table === 'accounts' ? TEST_COPY_UPSTREAM_TOKEN
        : `STA1N-${randomBytes(24).toString('base64url')}`);
      if (table === 'cards') remember(row.code, randomBytes(24).toString('base64url'));
    }
    const scrubText = value => {
      if (typeof value !== 'string') return value;
      if (replacements.has(value)) return replacements.get(value);
      for (const [secret, replacement] of replacements) if (secret.length >= 8) value = value.split(secret).join(replacement);
      return value;
    };
    const scrub = value => {
      if (Array.isArray(value)) return value.map(scrub);
      if (!value || typeof value !== 'object') return scrubText(value);
      return Object.fromEntries(Object.entries(value).filter(([key]) => !['discord', 'discordInvite', 'memberAuth'].includes(key))
        .map(([key, item]) => [key, secretFields.has(key) || proxyFields.has(key) ? '' : scrub(item)]));
    };
    const writeRecord = Object.fromEntries(collections.map(table => {
      const columns = Object.keys(sqlRecord(table, { id: '' }, 0, '{}'));
      return [table, db.prepare(`INSERT INTO ${table} (${columns.join(',')}) VALUES (${columns.map(key => '@' + key).join(',')})
        ON CONFLICT(id) DO UPDATE SET ${columns.filter(key => key !== 'id').map(key => `${key}=excluded.${key}`).join(',')}`)];
    }));
    const save = (table, value, order) => writeRecord[table].run(sqlRecord(table, value, order, JSON.stringify(value)));
    const cold = db.prepare('SELECT charge FROM fair_quota_settlements WHERE job_id=?');
    const putCharge = db.prepare('INSERT INTO fair_quota_settlements(job_id,charge) VALUES (?,?) ON CONFLICT(job_id) DO UPDATE SET charge=excluded.charge');
    const reservations = state.settings.fairQuota?.reservations || {};
    for (const [jobId, charge] of Object.entries(reservations)) {
      if (charge?.status !== 'reserved' || cold.get(jobId)) continue;
      if (!db.prepare('SELECT 1 FROM jobs WHERE id=?').get(jobId) || !state.settings.fairQuota?.balances?.[charge.userId])
        throw new CopyError('orphan_reserved_charge', { reservations: 1 });
    }
    db.transaction(() => {
      for (const row of records(db, 'jobs')) {
        const job = parse(row.data), settled = cold.get(job.id), charge = settled ? parse(settled.charge) : reservations[job.id];
        const isolate = active.has(job.status) || job.resultPending || job.generationReview?.status === 'pending' || charge?.status === 'reserved';
        if (!isolate) continue;
        if (settled && charge.status === 'reserved') throw new CopyError('nonterminal_cold_charge', { reservations: 1 });
        if (active.has(job.status) && !charge && Number(job.cost || 0) > 0 && !job.refundedAt)
          throw new CopyError('legacy_points_refund_required', { jobs: 1 });
        if (charge) {
          const wasReserved = charge.status === 'reserved';
          const result = settleFairQuota(state, job, false, now, {}, charge);
          if (wasReserved) summary.refunded++;
          if (result && ['settled', 'refunded'].includes(result.status)) putCharge.run(job.id, JSON.stringify(result));
        }
        if (job.generationReview?.status === 'pending') {
          if (!closeGenerationReview(state, job, now, 'test_copy_isolated')) throw new CopyError('unclosed_generation_review', { jobs: 1 });
          summary.reviewsClosed++;
        }
        if (active.has(job.status)) Object.assign(job, { status: 'failed', cancelledAt: new Date(now).toISOString(),
          completedAt: new Date(now).toISOString(), updatedAt: new Date(now).toISOString(), error: message,
          errorDetail: message, errorStatus: 409, cancellationReason: 'test_copy_isolated' });
        job.testCopyIsolatedAt = new Date(now).toISOString();
        delete job.resultPending;
        save('jobs', job, row.order_value);
        summary.isolated++;
      }
      for (const [jobId, charge] of Object.entries(reservations)) {
        if (cold.get(jobId)) continue;
        if (!['settled', 'refunded'].includes(charge.status)) throw new CopyError('unclosed_reserved_charge', { reservations: 1 });
        putCharge.run(jobId, JSON.stringify(charge));
      }
      if (state.settings.fairQuota) state.settings.fairQuota.reservations = {};
      let order = db.prepare('SELECT COALESCE(MAX(order_value),0) AS n FROM ledger').get().n;
      for (const event of [...state.ledger].reverse()) {
        if (db.prepare('SELECT 1 FROM ledger WHERE id=?').get(event.id)) throw new CopyError('isolation_event_conflict', { events: 1 });
        save('ledger', event, ++order);
      }
      db.prepare("UPDATE app_settings SET value=? WHERE key='settings'").run(JSON.stringify(state.settings));
      db.exec("DELETE FROM member_sessions; DELETE FROM app_meta WHERE key='member_session_owner'");
      for (const table of collections) for (const row of records(db, table)) {
        const value = scrub(parse(row.data));
        if (table === 'users') Object.assign(value, users.get(value.id));
        if (table === 'accounts') Object.assign(value, { token: TEST_COPY_UPSTREAM_TOKEN, proxyUrl: '', socksProxy: '', proxy: '', inFlight: 0 });
        if (table === 'cards') value.enabled = false;
        save(table, value, row.order_value);
      }
      for (const [table, column] of Object.entries(jsonColumns).filter(([table]) => !collections.includes(table))) {
        const update = db.prepare(`UPDATE ${table} SET ${column}=?${table === 'generation_requests' ? ',payload_bytes=?' : ''} WHERE rowid=?`);
        for (const row of records(db, table)) {
          const value = scrub(parse(row[column]));
          if (table === 'app_settings' && row.key === 'settings') {
            value.publicBaseUrl = '';
            if (value.promptApi) Object.assign(value.promptApi, { baseUrl: '', apiKey: '' });
          }
          const encoded = JSON.stringify(value);
          update.run(...(table === 'generation_requests' ? [encoded, Buffer.byteLength(encoded), row._rowid] : [encoded, row._rowid]));
        }
      }
      // Metadata and cold-table scalar columns may contain copies of known credentials too.
      for (const table of tables.filter(name => name !== '__drizzle_migrations' && !collections.includes(name))) {
        const columns = db.prepare(`PRAGMA table_info(${table})`).all().filter(column => column.type.toUpperCase() === 'TEXT').map(column => column.name);
        const update = db.prepare(`UPDATE ${table} SET ${columns.map(column => `${column}=@${column}`).join(',')} WHERE rowid=@_rowid`);
        for (const row of records(db, table)) update.run(Object.fromEntries([['_rowid', row._rowid], ...columns.map(column =>
          [column, column === jsonColumns[table] ? row[column] : scrubText(row[column])])]));
      }
    })();
    if (db.pragma('integrity_check', { simple: true }) !== 'ok' || db.pragma('foreign_key_check').length) throw new CopyError('copy_integrity_failed');
    summary.after = Object.fromEntries(tables.map(table => [table, count(db, table)]));
    const copied = new Set(), rootImages = imagesDir ? await realpath(imagesDir) : null;
    for (const row of records(db, 'images')) {
      const image = parse(row.data);
      if (!image.file) continue;
      if (!/^images\/[A-Za-z0-9_.-]+$/.test(image.file) || image.file.endsWith('.part')) throw new CopyError('unsupported_image_path');
      if (copied.has(image.file)) continue;
      copied.add(image.file);
      if (!rootImages) { summary.imagesMissing++; continue; }
      const from = path.join(rootImages, path.basename(image.file));
      let stat;
      try { stat = await lstat(from); } catch (error) { if (error.code === 'ENOENT') { summary.imagesMissing++; continue; } throw error; }
      if (!stat.isFile() || stat.isSymbolicLink()) throw new CopyError('image_must_be_regular_file');
      await mkdir(path.join(targetPath, 'images'), { mode: 0o700, recursive: true });
      await copyFile(from, path.join(targetPath, image.file), constants.COPYFILE_EXCL);
      await chmod(path.join(targetPath, image.file), 0o600);
      summary.imagesCopied++;
    }
    await writeFile(path.join(targetPath, 'test-credentials.json'), JSON.stringify({ createdAt: new Date(now).toISOString(), accounts: credentials }, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
    db.prepare('VACUUM INTO ?').run(cleanPath);
    db.close(); db = null;
    await chmod(cleanPath, 0o600);
    // Check the compacted bytes as well as logical rows: discarded credentials must not survive in free pages.
    const needles = [...replacements.keys()].filter(secret => secret.length >= 8).map(secret => Buffer.from(secret));
    const overlap = Math.max(1, ...needles.map(needle => needle.length)) - 1;
    let tail = Buffer.alloc(0);
    for await (const chunk of createReadStream(cleanPath)) {
      const bytes = Buffer.concat([tail, chunk]);
      if (needles.some(needle => bytes.includes(needle))) throw new CopyError('credential_residue');
      tail = overlap ? bytes.subarray(-overlap) : Buffer.alloc(0);
    }
    await unlink(workPath);
    await link(cleanPath, finalPath); published = true; // Fails if a file appeared; never replaces it.
    await unlink(cleanPath);
    await rmdir(workDir);
    return summary;
  } catch (error) {
    // Failed work remains private for diagnosis; it is never published as library.sqlite.
    if (published) await unlink(finalPath);
    if (error instanceof CopyError) throw error;
    throw new CopyError('test_copy_failed');
  } finally { sourceDb?.close(); db?.close(); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  try {
    const { values } = parseArgs({ options: { source: { type: 'string' }, target: { type: 'string' }, 'images-dir': { type: 'string' } } });
    console.log(JSON.stringify(await createTestCopy({ source: values.source, target: values.target, imagesDir: values['images-dir'] })));
  } catch (error) {
    console.error(JSON.stringify({ error: error instanceof CopyError ? error.code : 'test_copy_failed', counts: error.counts || {} }));
    process.exitCode = 1;
  }
}
