import path from 'node:path';
import { mkdirSync, existsSync, statSync } from 'node:fs';
import Database from 'better-sqlite3';
import { migrateStorage } from '../server/storage/sqlite-migrations.ts';
import {
  assertReplaySourceReady,
  copyProtectedFacts,
  replayDomainEvents,
} from '../server/events/event-store.ts';
import { applyEventProjection } from '../server/storage/sqlite-store.ts';
import { domainDigest } from '../server/events/domain-events.ts';

const values = Object.fromEntries(
  process.argv.slice(2).map((value) => {
    const match = /^--([a-z-]+)=(.+)$/.exec(value);
    if (!match) throw new Error(`Expected --name=value, received ${value}`);
    return [match[1], match[2]];
  }),
);
if (!values.source || !values.target)
  throw new Error(
    'Usage: node scripts/replay-events.mjs --source=/backup/library.sqlite --target=/isolated/rebuilt.sqlite [--through=COMMIT] [--max-commits=N]',
  );
for (const key of Object.keys(values))
  if (!['source', 'target', 'through', 'max-commits'].includes(key))
    throw new Error(`Unsupported replay option: ${key}`);
const sourcePath = path.resolve(values.source),
  targetPath = path.resolve(values.target);
if (
  sourcePath === targetPath ||
  (process.platform === 'win32' && sourcePath.toLowerCase() === targetPath.toLowerCase())
)
  throw new Error('Replay must use a separate target database.');
if (existsSync(targetPath)) {
  const sourceIdentity = statSync(sourcePath, { bigint: true }),
    targetIdentity = statSync(targetPath, { bigint: true });
  if (sourceIdentity.dev === targetIdentity.dev && sourceIdentity.ino === targetIdentity.ino)
    throw new Error('Replay source and target resolve to the same file.');
}
if (values.through && !/^\d+$/.test(values.through))
  throw new Error('through must be a decimal commit cursor.');
const maxCommits = values['max-commits'] ? Number(values['max-commits']) : undefined;
if (maxCommits !== undefined && (!Number.isSafeInteger(maxCommits) || maxCommits < 1))
  throw new Error('max-commits must be a positive integer.');
const source = new Database(sourcePath, { readonly: true, fileMustExist: true });
source.pragma('query_only = ON');
let target;
try {
  assertReplaySourceReady(source);
  const origin = source.prepare("SELECT value FROM app_meta WHERE key='event_opening'").get();
  if (!origin || JSON.parse(origin.value).status !== 'complete')
    throw new Error('Source has no complete opening import.');
  const first = source
    .prepare('SELECT event_id FROM domain_events ORDER BY commit_seq,event_index LIMIT 1')
    .get();
  if (!first) throw new Error('Source has no event facts.');
  const historyId =
    JSON.parse(origin.value).historyId || domainDigest({ opening: origin.value, first });
  if (existsSync(targetPath)) {
    const inspect = new Database(targetPath, { readonly: true, fileMustExist: true });
    try {
      const hasMeta = inspect
        .prepare("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='app_meta'")
        .get();
      const prior = hasMeta
        ? inspect.prepare("SELECT value FROM app_meta WHERE key='event_replay_source'").get()
        : null;
      if (prior && prior.value !== historyId)
        throw new Error('Target belongs to a different event history.');
      if (!prior)
        for (const { name } of inspect
          .prepare(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name!='__drizzle_migrations'",
          )
          .all()) {
          const identifier = '"' + name.replaceAll('"', '""') + '"';
          if (inspect.prepare(`SELECT 1 FROM ${identifier} LIMIT 1`).get())
            throw new Error(
              'Replay target must be empty or an existing checkpoint for this event history.',
            );
        }
    } finally {
      inspect.close();
    }
  }
  mkdirSync(path.dirname(targetPath), { recursive: true });
  target = new Database(targetPath);
  target.pragma('journal_mode = WAL');
  target.pragma('synchronous = FULL');
  target.pragma('foreign_keys = ON');
  migrateStorage(target);
  target
    .prepare("INSERT OR IGNORE INTO app_meta(key,value) VALUES('event_replay_source',?)")
    .run(historyId);
  const result = source.transaction(() => {
    copyProtectedFacts(source, target);
    return replayDomainEvents(source, target, {
      throughCommitSeq: values.through,
      maxCommits,
      project: applyEventProjection,
    });
  })();
  if (target.pragma('integrity_check', { simple: true }) !== 'ok')
    throw new Error('Replayed SQLite integrity check failed.');
  console.log(
    JSON.stringify({
      source: sourcePath,
      target: targetPath,
      ...result,
      networkCalls: 0,
      dispatches: 0,
      artifactNotice:
        'Artifact bytes remain in the backup artifact directory; this command restores database facts and references only.',
    }),
  );
} finally {
  target?.close();
  source.close();
}
