import { createHash } from 'node:crypto';
import {
  closeSync,
  createReadStream,
  existsSync,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  readlinkSync,
  realpathSync,
  readSync,
  writeFileSync,
  writeSync,
} from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

const root = fileURLToPath(new URL('../', import.meta.url));
const physicalRoot = realpathSync(root);
const sourceCommit = '3caba3dfa0b5d9f582acabfc02e36e7db4af038f';
const sha256 = (value) => createHash('sha256').update(value).digest('hex');
const fail = () => {
  throw new Error('Legacy archive or fixed tool package failed read-only validation.');
};
let Database;
function within(directory, filename) {
  const relative = path.relative(directory, filename);
  return !path.isAbsolute(relative) && relative !== '..' && !relative.startsWith('..' + path.sep);
}
function packagePath(name) {
  const filename = path.resolve(root, name);
  if (
    filename === path.resolve(root) ||
    !within(root, filename) ||
    !within(physicalRoot, realpathSync(filename))
  ) {
    fail();
  }
  return filename;
}
async function digest(filename) {
  const hash = createHash('sha256');
  for await (const bytes of createReadStream(filename)) {
    hash.update(bytes);
  }
  return hash.digest('hex');
}
async function verifyPackage() {
  const manifest = JSON.parse(readFileSync(packagePath('LEGACY_TOOLS.json'), 'utf8'));
  if (
    manifest.format !== 'novelai-legacy-tools-v1' ||
    manifest.sourceCommit !== sourceCommit ||
    manifest.node !== process.version ||
    manifest.platform !== process.platform ||
    manifest.arch !== process.arch ||
    manifest.abi !== process.versions.modules
  ) {
    fail();
  }
  for (const [name, expected] of Object.entries(manifest.files)) {
    const filename = packagePath(name);
    const info = lstatSync(filename);
    if (expected.link !== undefined) {
      const target = readlinkSync(filename),
        resolved = path.resolve(path.dirname(filename), target);
      if (
        !info.isSymbolicLink() ||
        target !== expected.link ||
        path.isAbsolute(target) ||
        !within(root, resolved)
      ) {
        fail();
      }
    } else if (!info.isFile() || (await digest(filename)) !== expected.sha256) {
      fail();
    }
  }
  if (
    (await digest(path.join(root, 'SOURCE_PACKAGE.json'))) !== manifest.sourcePackageSha256 ||
    (await digest(path.join(root, 'SOURCE_LOCK.yaml'))) !== manifest.sourceLockSha256
  ) {
    fail();
  }
  // 包内依赖也先验证真实路径和字节，不能先执行包外模块再拒绝。
  Database = (await import('better-sqlite3')).default;
  const check = new Database(':memory:');
  check.close();
  return manifest;
}

function standalone(filename) {
  if (
    !lstatSync(filename).isFile() ||
    ['-wal', '-shm', '-journal'].some((suffix) => existsSync(filename + suffix))
  ) {
    fail();
  }
  const handle = openSync(filename, 'r');
  try {
    const header = Buffer.alloc(20);
    if (
      readSync(handle, header) !== 20 ||
      header.subarray(0, 16).toString() !== 'SQLite format 3\0' ||
      header[18] !== 1 ||
      header[19] !== 1
    ) {
      fail();
    }
  } finally {
    closeSync(handle);
  }
}

try {
  const { values } = parseArgs({
    options: {
      source: { type: 'string' },
      export: { type: 'string' },
      'verify-package': { type: 'boolean' },
    },
  });
  const manifest = await verifyPackage();
  if (values['verify-package']) {
    if (values.source || values.export) {
      fail();
    }
    console.log(
      JSON.stringify({
        verified: true,
        sourceCommit,
        files: Object.keys(manifest.files).length,
        networkCalls: 0,
      }),
    );
  } else {
    if (!values.source) {
      throw new Error(
        'Usage: node scripts/audit-legacy-archive.mjs --source=/sealed/library.sqlite [--export=/new/private/directory]',
      );
    }
    const source = path.resolve(values.source);
    standalone(source);
    const originalHash = await digest(source);
    const [
      { migrateStorage, readCompatibleEventAuthority },
      owners,
      { canonicalJson, domainDigest, assertDomainEvent },
      { verifyEventProjections },
    ] = await Promise.all([
      import('../server/storage/sqlite-migrations.ts'),
      import('../server/events/event-store.ts'),
      import('../server/events/domain-events.ts'),
      import('../server/storage/sqlite-store.ts'),
    ]);
    const db = new Database(source, { readonly: true, fileMustExist: true });
    const handles = [];
    let report;
    let exportDirectory;
    try {
      db.pragma('temp_store=MEMORY');
      db.pragma('query_only=ON');
      report = db.transaction(() => {
        if (
          !db.prepare("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='app_meta'").get() ||
          db.prepare("SELECT value FROM app_meta WHERE key='database_format'").get() ||
          readCompatibleEventAuthority(db)?.version !== 4
        ) {
          fail();
        }
        owners.assertReplaySourceReady(db);
        const reference = new Database(':memory:');
        try {
          migrateStorage(reference);
          const tables = (database) =>
            database
              .prepare(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name",
              )
              .all();
          if (canonicalJson(tables(reference)) !== canonicalJson(tables(db))) {
            fail();
          }
          for (const { name } of tables(reference)) {
            if (
              canonicalJson(reference.pragma(`table_info(${name})`)) !==
              canonicalJson(db.pragma(`table_info(${name})`))
            ) {
              fail();
            }
          }
          const journal = 'SELECT hash,created_at FROM __drizzle_migrations ORDER BY created_at';
          if (
            canonicalJson(reference.prepare(journal).all()) !==
            canonicalJson(db.prepare(journal).all())
          ) {
            fail();
          }
        } finally {
          reference.close();
        }
        const opening = db.prepare("SELECT value FROM app_meta WHERE key='event_opening'").get();
        if (
          !opening ||
          JSON.parse(opening.value).status !== 'complete' ||
          db.pragma('integrity_check', { simple: true }) !== 'ok' ||
          db.prepare('PRAGMA foreign_key_check').get()
        ) {
          fail();
        }
        const clock = db
          .prepare('SELECT CAST(value AS TEXT) AS value FROM event_clock WHERE id=1')
          .get();
        const checkpoint = db
          .prepare(
            "SELECT CAST(last_commit_seq AS TEXT) AS value,status FROM projection_versions WHERE name='core'",
          )
          .get();
        if (
          !clock ||
          !/^[1-9][0-9]*$/.test(clock.value) ||
          !checkpoint ||
          checkpoint.status !== 'ready' ||
          checkpoint.value !== clock.value
        ) {
          fail();
        }
        for (const collection of ['users', 'accounts', 'cards', 'jobs', 'images', 'ledger']) {
          if (
            db
              .prepare(
                `SELECT 1 FROM ${collection} WHERE id IS NULL OR id='' OR COALESCE(json_type(data,'$.id'),'')!='text' OR json_extract(data,'$.id') IS NOT id LIMIT 1`,
              )
              .get()
          ) {
            fail();
          }
        }
        // 旧 owner 只在内存 TEMP 表核对冷凭据；主库句柄从未获得写权限。
        if (!db.readonly) {
          fail();
        }
        db.pragma('query_only=OFF');
        try {
          verifyEventProjections(db);
        } finally {
          db.pragma('query_only=ON');
        }
        const result = {
          format: 'legacy-archive-audit-v1',
          sourceCommit,
          sourceSha256: originalHash,
          openingSha256: sha256(opening.value),
          lastCommitSeq: '0',
          commits: 0,
          events: 0,
          protectedInputs: 0,
          protectedInputBytes: 0,
          contexts: 0,
          artifactInputs: 0,
          diagnostics: 0,
          artifacts: 0,
          networkCalls: 0,
          dispatches: 0,
          externalObjectBytesVerified: false,
        };
        let emit = () => {};
        if (values.export) {
          const requested = path.resolve(values.export);
          exportDirectory = path.join(
            realpathSync(path.dirname(requested)),
            path.basename(requested),
          );
          if (within(physicalRoot, exportDirectory)) {
            fail();
          }
          // 只向全新私有目录输出白名单清单；原文、令牌和诊断正文留在封存源内。
          mkdirSync(exportDirectory, { mode: 0o700 });
          const files = Object.fromEntries(
            ['inputs', 'artifacts'].map((kind) => {
              const handle = openSync(path.join(exportDirectory, kind + '.jsonl'), 'wx', 0o600);
              handles.push(handle);
              return [kind, handle];
            }),
          );
          emit = (kind, value) => writeSync(files[kind], JSON.stringify(value) + '\n');
        }
        const input = db.prepare('SELECT member_id,payload FROM generation_requests WHERE id=?');
        const context = db.prepare('SELECT payload FROM generation_contexts WHERE context_ref=?');
        let after = '0';
        for (;;) {
          const commits = owners.readDomainCommits(db, after, 100);
          if (!commits.length) {
            break;
          }
          for (const commit of commits) {
            if (BigInt(commit.commitSeq) !== BigInt(result.lastCommitSeq) + 1n) {
              fail();
            }
            result.commits++;
            result.events += commit.events.length;
            for (const event of commit.events) {
              assertDomainEvent(event);
              let fact = null,
                mapping = null;
              if (event.eventType === 'RequestAccepted') {
                fact = event.payload;
                mapping = fact.idempotency;
              } else if (event.eventType === 'OpeningStateRecorded') {
                if (event.streamKind === 'request') {
                  fact = event.payload.state.request;
                } else if (event.streamKind === 'idempotency') {
                  mapping = event.payload.state;
                }
              } else if (event.eventType === 'RequestAliasRegistered') {
                mapping = event.payload.idempotency;
              }
              if (fact?.inputRef) {
                const row = input.get(fact.inputRef);
                if (
                  !row ||
                  row.member_id !== fact.memberId ||
                  sha256(row.payload) !== fact.inputSha256
                ) {
                  fail();
                }
              }
              if (mapping?.normalizationContextRef) {
                const row = context.get(mapping.normalizationContextRef);
                if (
                  !row ||
                  domainDigest(JSON.parse(row.payload)) !== mapping.normalizationContextHash
                ) {
                  fail();
                }
              }
            }
            result.lastCommitSeq = commit.commitSeq;
          }
          after = commits.at(-1).commitSeq;
        }
        if (
          result.lastCommitSeq !== clock.value ||
          db
            .prepare(
              `SELECT 1 FROM event_streams s LEFT JOIN domain_events e ON e.event_id=s.last_event_id WHERE e.event_id IS NULL OR e.stream_id!=s.stream_id OR e.stream_kind!=s.stream_kind OR e.commit_seq!=s.last_commit_seq OR e.stream_version!=s.stream_version LIMIT 1`,
            )
            .get() ||
          db
            .prepare(
              'SELECT 1 FROM domain_events e LEFT JOIN event_streams s ON s.stream_id=e.stream_id WHERE s.stream_id IS NULL OR e.stream_version>s.stream_version LIMIT 1',
            )
            .get()
        ) {
          fail();
        }
        for (const row of db
          .prepare(
            'SELECT id,member_id,operation,source,created_at,payload_bytes,payload FROM generation_requests ORDER BY id',
          )
          .iterate()) {
          const bytes = Buffer.byteLength(row.payload);
          if (row.payload_bytes !== bytes) {
            fail();
          }
          JSON.parse(row.payload);
          result.protectedInputs++;
          result.protectedInputBytes += bytes;
          const { payload: _payload, ...metadata } = row;
          emit('inputs', { kind: 'request', ...metadata, sha256: sha256(row.payload) });
        }
        for (const row of db
          .prepare('SELECT context_ref,payload FROM generation_contexts ORDER BY context_ref')
          .iterate()) {
          result.contexts++;
          emit('inputs', {
            kind: 'context',
            ref: row.context_ref,
            sha256: domainDigest(JSON.parse(row.payload)),
            bytes: Buffer.byteLength(row.payload),
          });
        }
        for (const row of db
          .prepare(
            'SELECT artifact_id,member_id,payload_sha256,payload FROM artifact_inputs ORDER BY artifact_id',
          )
          .iterate()) {
          const artifact = owners.readStreamProjection(
            db,
            `artifact:${row.artifact_id}`,
          )?.projection;
          if (
            !artifact ||
            artifact.id !== row.artifact_id ||
            (artifact.memberId ?? null) !== row.member_id ||
            artifact.artifactInputRef !== row.artifact_id ||
            artifact.artifactInputSha256 !== row.payload_sha256
          ) {
            fail();
          }
          owners.readArtifactInput(db, row.artifact_id, artifact.artifactInputSha256);
          result.artifactInputs++;
          emit('inputs', {
            kind: 'artifact-input',
            id: row.artifact_id,
            memberId: row.member_id,
            sha256: row.payload_sha256,
            bytes: Buffer.byteLength(row.payload),
          });
        }
        for (const row of db
          .prepare(
            'SELECT diagnostic_id,job_id,detail,detail_sha256 FROM generation_diagnostics ORDER BY diagnostic_id',
          )
          .iterate()) {
          if (sha256(row.detail) !== row.detail_sha256) {
            fail();
          }
          result.diagnostics++;
          emit('inputs', {
            kind: 'diagnostic',
            id: row.diagnostic_id,
            jobId: row.job_id,
            sha256: row.detail_sha256,
            bytes: Buffer.byteLength(row.detail),
          });
        }
        for (const row of db.prepare('SELECT id,data FROM images ORDER BY id').iterate()) {
          const value = JSON.parse(row.data),
            r2 = value.r2;
          const pick = (object, keys) =>
            Object.fromEntries(
              keys.map((key) => {
                const value = object?.[key] ?? null;
                if (value !== null && !['string', 'number', 'boolean'].includes(typeof value)) {
                  fail();
                }
                return [key, value];
              }),
            );
          if (r2?.pikpak?.url) {
            const namespace = new URL(r2.pikpak.url);
            if (
              namespace.protocol !== 'https:' ||
              namespace.username ||
              namespace.password ||
              namespace.search ||
              namespace.hash
            ) {
              fail();
            }
          }
          result.artifacts++;
          emit('artifacts', {
            id: row.id,
            ...pick(value, [
              'jobId',
              'memberId',
              'operation',
              'mimeType',
              'byteLength',
              'sha256',
              'file',
              'createdAt',
            ]),
            r2: r2
              ? pick(r2, [
                  'bucket',
                  'key',
                  'byteLength',
                  'sha256',
                  'verifiedAt',
                  'localRemovedAt',
                  'removedAt',
                ])
              : null,
            pikpak: r2?.pikpak
              ? { ...pick(r2.pikpak, ['url', 'uploadedAt']), encoding: 'brotli' }
              : null,
            externalBytesChecked: false,
          });
        }
        return result;
      })();
    } finally {
      db.close();
      for (const handle of handles) {
        closeSync(handle);
      }
    }
    if ((await digest(source)) !== originalHash) {
      fail();
    }
    if (values.export) {
      writeFileSync(
        path.join(exportDirectory, 'audit.json'),
        JSON.stringify(report, null, 2) + '\n',
        { flag: 'wx', mode: 0o600 },
      );
    }
    console.log(JSON.stringify(report));
  }
} catch {
  console.error(
    'Legacy archive audit refused; check the fixed package, source format, and protected-content evidence.',
  );
  process.exitCode = 1;
}
