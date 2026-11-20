import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { lstat, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { spawnSync } from 'node:child_process';
import Database from 'better-sqlite3';
import { resolveLegacyRetainedDependencies } from '../../scripts/resolve-legacy-retained-dependencies.mjs';

const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');
const moduleFile = fileURLToPath(
  new URL('../../scripts/resolve-legacy-retained-dependencies.mjs', import.meta.url),
);
const zeroTime = '1970-01-01T00:00:00.000Z';
const row = (id, fields = {}) => ({
  id,
  jobId: null,
  memberId: null,
  operation: null,
  mimeType: null,
  byteLength: null,
  sha256: null,
  file: null,
  createdAt: null,
  r2: null,
  pikpak: null,
  externalBytesChecked: false,
  ...fields,
});
const remote = (fields = {}) => ({
  bucket: 'original-bucket',
  key: 'original/key',
  byteLength: null,
  sha256: null,
  verifiedAt: null,
  localRemovedAt: null,
  removedAt: null,
  ...fields,
});
async function fixture(t, rows = [row('unknown')]) {
  const directory = await mkdtemp(path.join(tmpdir(), 'novelai-legacy-dependencies-'));
  t.after(async () => {
    assert.equal(path.dirname(path.resolve(directory)), path.resolve(tmpdir()));
    assert.ok(path.basename(directory).startsWith('novelai-legacy-dependencies-'));
    await rm(directory, { recursive: true, force: true });
  });
  const source = path.join(directory, 'source.sqlite'),
    db = new Database(source);
  db.exec('CREATE TABLE original_source(id TEXT PRIMARY KEY, value TEXT);');
  db.prepare('INSERT INTO original_source VALUES (?,?)').run('original', 'synthetic source bytes');
  db.close();
  const artifacts = path.join(directory, 'artifacts.jsonl'),
    audit = path.join(directory, 'audit.json');
  const artifactBytes = Buffer.from(rows.map((value) => JSON.stringify(value) + '\n').join(''));
  await writeFile(artifacts, artifactBytes);
  const tuple = (basename, bytes, rowCount) => ({
    basename,
    byteLength: bytes.length,
    rowCount,
    sha256: sha256(bytes),
  });
  const auditValue = {
    format: 'legacy-archive-audit-v1',
    sourceCommit: 'a'.repeat(40),
    toolSha256: 'b'.repeat(64),
    sourceSha256: sha256(await readFile(source)),
    openingSha256: 'c'.repeat(64),
    lastCommitSeq: '1',
    commits: 1,
    events: 1,
    protectedInputs: 0,
    protectedInputBytes: 0,
    contexts: 0,
    artifactInputs: 0,
    diagnostics: 0,
    artifacts: rows.length,
    networkCalls: 0,
    dispatches: 0,
    externalObjectBytesVerified: false,
    exports: {
      inputs: tuple('inputs.jsonl', Buffer.alloc(0), 0),
      artifacts: tuple('artifacts.jsonl', artifactBytes, rows.length),
    },
  };
  await writeFile(audit, JSON.stringify(auditValue) + '\n');
  const inventory = path.join(directory, 'inventory.json');
  const inventoryValue = {
    format: 'legacy-v4-inventory-v1',
    integrity: true,
    foreignKeyViolations: 0,
    source: { databaseSha256: auditValue.sourceSha256, declaredCommit: auditValue.sourceCommit },
    cut: { lastCommitSeq: auditValue.lastCommitSeq, openingComplete: true },
    liabilities: { preparedResults: 0 },
  };
  await writeFile(inventory, JSON.stringify(inventoryValue) + '\n');
  const proofs = {};
  for (const name of ['sourceCapture', 'namespaceReceipt', 'identityReceipt']) {
    const bytes = Buffer.from(
      JSON.stringify({ kind: name, sourceSha256: auditValue.sourceSha256, cut: '1' }) + '\n',
    );
    const file = name + '.json';
    await writeFile(path.join(directory, file), bytes);
    proofs[name] = { file, sha256: sha256(bytes) };
  }
  const namespaceAuthority = path.join(directory, 'cut-config.json');
  const config = {
    format: 'novelai-legacy-cut-config-v1',
    sourceSha256: auditValue.sourceSha256,
    cut: '1',
    sourceCommit: auditValue.sourceCommit,
    captureStartedAt: zeroTime,
    configuredOwnerUserId: 'synthetic-owner',
    configuredGuildId: '',
    discordLoginEnabled: false,
    sourceNamespaces: {
      dataDir: '/original/data',
      r2AccountId: 'a'.repeat(32),
      r2Bucket: 'original-bucket',
      pikpakUrl: 'https://original.example/dav',
    },
    ...proofs,
    localOriginal: null,
  };
  await writeFile(namespaceAuthority, JSON.stringify(config) + '\n');
  const options = {
    source,
    audit,
    auditSha256: sha256(await readFile(audit)),
    artifacts,
    namespaceAuthority,
    namespaceAuthoritySha256: sha256(await readFile(namespaceAuthority)),
    inventory,
    inventorySha256: sha256(await readFile(inventory)),
    output: path.join(directory, 'resolved.jsonl'),
  };
  return { directory, options, rows, artifactBytes, auditValue, config, inventoryValue };
}
async function repin(f, target) {
  const [file, value, pin] =
    target === 'audit'
      ? [f.options.audit, f.auditValue, 'auditSha256']
      : target === 'inventory'
        ? [f.options.inventory, f.inventoryValue, 'inventorySha256']
        : [f.options.namespaceAuthority, f.config, 'namespaceAuthoritySha256'];
  await writeFile(file, JSON.stringify(value) + '\n');
  f.options[pin] = sha256(await readFile(file));
}
async function output(f) {
  return (await readFile(f.options.output, 'utf8')).trimEnd().split('\n').map(JSON.parse);
}
async function rewriteRows(f) {
  const bytes = Buffer.from(f.rows.map((value) => JSON.stringify(value) + '\n').join(''));
  await writeFile(f.options.artifacts, bytes);
  f.auditValue.artifacts = f.rows.length;
  f.auditValue.exports.artifacts = {
    basename: 'artifacts.jsonl',
    byteLength: bytes.length,
    rowCount: f.rows.length,
    sha256: sha256(bytes),
  };
  await repin(f, 'audit');
}

test('selected real SQLite source resolves each original physical tuple, unknown/zero/tombstone and full LF row proof without rewriting inputs', async (t) => {
  const content = 'd'.repeat(64);
  const f = await fixture(t, [
    row('original', {
      sha256: content,
      byteLength: 0,
      file: 'images/original-noncanonical.webp',
      r2: remote({ sha256: content, byteLength: 0, verifiedAt: zeroTime, localRemovedAt: 0 }),
      pikpak: { url: 'https://original.example/dav', uploadedAt: zeroTime, encoding: 'brotli' },
    }),
    row('same-sha-other-artifact', { sha256: content, byteLength: 0, file: 'images/other.webp' }),
    row('unknown-content', {
      file: 'images/unknown.webp',
      r2: remote({ key: 'unhashed-original' }),
    }),
    row('no-physical-reference'),
    row('r2-original-descriptor', {
      r2: remote({
        sha256: 'e'.repeat(64),
        byteLength: 7,
        key: 'original-not-canonical',
        removedAt: zeroTime,
      }),
    }),
  ]);
  const files = [
    f.options.source,
    f.options.audit,
    f.options.artifacts,
    f.options.namespaceAuthority,
    f.options.inventory,
    path.join(f.directory, 'sourceCapture.json'),
    path.join(f.directory, 'namespaceReceipt.json'),
  ];
  const before = await Promise.all(files.map((file) => readFile(file)));
  const result = await resolveLegacyRetainedDependencies(f.options),
    [header, ...rows] = await output(f);
  assert.deepEqual(Object.keys(header), [
    'kind',
    'format',
    'sourceSha256',
    'sourceCommit',
    'cut',
    'auditSha256',
    'artifacts',
    'namespaceAuthoritySha256',
    'sourceCaptureSha256',
    'namespaceReceiptSha256',
    'producerSha256',
    'inventorySha256',
    'preparedResults',
    'artifactRows',
  ]);
  assert.equal(header.kind, 'legacy-physical-dependencies');
  assert.equal(header.format, 'novelai-legacy-retained-dependencies-v1');
  assert.equal(header.sourceSha256, f.auditValue.sourceSha256);
  assert.equal(header.sourceCommit, f.auditValue.sourceCommit);
  assert.equal(header.cut, '1');
  assert.equal(header.auditSha256, f.options.auditSha256);
  assert.deepEqual(header.artifacts, f.auditValue.exports.artifacts);
  assert.equal(header.namespaceAuthoritySha256, f.options.namespaceAuthoritySha256);
  assert.equal(header.sourceCaptureSha256, f.config.sourceCapture.sha256);
  assert.equal(header.namespaceReceiptSha256, f.config.namespaceReceipt.sha256);
  assert.equal(header.producerSha256, sha256(await readFile(moduleFile)));
  assert.equal(header.inventorySha256, f.options.inventorySha256);
  assert.equal(header.preparedResults, 0);
  assert.equal(header.artifactRows, 5);
  assert.deepEqual(
    rows.map((value) => value.id),
    f.rows.map((value) => value.id),
  );
  const originalLines = f.artifactBytes.toString('utf8').split('\n');
  for (const [index, value] of rows.entries()) {
    assert.deepEqual(value.originalRow, {
      line: index + 1,
      sha256: sha256(Buffer.from(originalLines[index] + '\n')),
    });
    assert.equal(value.dataDir, '/original/data');
    assert.deepEqual(Object.keys(value), [
      'kind',
      'id',
      'dataDir',
      'descriptor',
      'replicas',
      'originalRow',
    ]);
  }
  assert.deepEqual(rows[0].descriptor, {
    imageId: 'original',
    file: 'images/original-noncanonical.webp',
    sha256: content,
    byteLength: 0,
  });
  assert.deepEqual(rows[0].replicas, [
    {
      target: { kind: 'local', namespace: 'images' },
      replica: {
        objectKey: 'original-noncanonical.webp',
        encoding: 'identity',
        uploadedAt: null,
        verifiedAt: null,
        deletedAt: 0,
      },
    },
    {
      target: { kind: 'r2', namespace: 'r2://' + 'a'.repeat(32) + '/original-bucket' },
      replica: {
        objectKey: 'original/key',
        encoding: 'identity',
        uploadedAt: null,
        verifiedAt: 0,
        deletedAt: null,
      },
    },
    {
      target: { kind: 'pikpak', namespace: 'https://original.example/dav' },
      replica: {
        objectKey: content + '.br',
        encoding: 'br',
        uploadedAt: 0,
        verifiedAt: null,
        deletedAt: null,
      },
    },
  ]);
  assert.equal(rows[1].replicas[0].replica.objectKey, 'other.webp');
  assert.equal(rows[2].descriptor.sha256, null);
  assert.equal(rows[2].descriptor.byteLength, null);
  assert.equal(rows[2].replicas[1].replica.objectKey, 'unhashed-original');
  assert.deepEqual(rows[3].replicas, []);
  assert.deepEqual(rows[4].descriptor, {
    imageId: 'r2-original-descriptor',
    file: null,
    sha256: 'e'.repeat(64),
    byteLength: 7,
  });
  assert.equal(rows[4].replicas[0].replica.deletedAt, 0);
  assert.equal(result.sha256, sha256(await readFile(f.options.output)));
  assert.equal(result.artifactRows, 5);
  assert.deepEqual(await Promise.all(files.map((file) => readFile(file))), before);
  assert.doesNotMatch(
    await readFile(f.options.output, 'utf8'),
    /synthetic-owner|configuredGuild|prompt|password|credential/,
  );
  if (process.platform !== 'win32') {
    assert.equal((await lstat(f.options.output)).mode & 0o777, 0o600);
  }
  await assert.rejects(resolveLegacyRetainedDependencies(f.options), /EEXIST/);
});

test('empty artifact tuple and cross-platform original roots remain literal, without invented physical references', async (t) => {
  const empty = await fixture(t, []);
  empty.config.sourceNamespaces = {
    dataDir: null,
    r2AccountId: null,
    r2Bucket: null,
    pikpakUrl: null,
  };
  await repin(empty, 'namespace');
  await resolveLegacyRetainedDependencies(empty.options);
  assert.equal((await output(empty)).length, 1);
  for (const root of ['/remote/原图', 'Z:\\original\\data']) {
    const f = await fixture(t, [row('原ID', { file: 'images/原图.webp' })]);
    f.config.sourceNamespaces.dataDir = root;
    await repin(f, 'namespace');
    await resolveLegacyRetainedDependencies(f.options);
    const [, original] = await output(f);
    assert.equal(original.dataDir, root);
    assert.equal(original.replicas[0].replica.objectKey, '原图.webp');
    assert.equal(original.originalRow.sha256, sha256(f.artifactBytes));
  }
});

const refusals = [
  [
    'wrong external audit pin',
    async (f) => {
      f.options.auditSha256 = '0'.repeat(64);
    },
  ],
  [
    'wrong external namespace pin',
    async (f) => {
      f.options.namespaceAuthoritySha256 = '0'.repeat(64);
    },
  ],
  [
    'cross-source authority',
    async (f) => {
      f.config.sourceSha256 = '0'.repeat(64);
      await repin(f, 'namespace');
    },
  ],
  [
    'cross-commit authority',
    async (f) => {
      f.config.sourceCommit = '0'.repeat(40);
      await repin(f, 'namespace');
    },
  ],
  [
    'cross-cut authority',
    async (f) => {
      f.config.cut = '2';
      await repin(f, 'namespace');
    },
  ],
  [
    'changed capture receipt',
    async (f) => {
      await writeFile(path.join(f.directory, 'sourceCapture.json'), '{}');
    },
  ],
  [
    'changed namespace receipt',
    async (f) => {
      await writeFile(path.join(f.directory, 'namespaceReceipt.json'), '{}');
    },
  ],
  [
    'namespace shape drift',
    async (f) => {
      f.config.sourceNamespaces.extra = 'unproven';
      await repin(f, 'namespace');
    },
  ],
  [
    'missing Local namespace',
    async (f) => {
      f.config.sourceNamespaces.dataDir = null;
      await repin(f, 'namespace');
    },
  ],
  [
    'missing R2 account',
    async (f) => {
      f.config.sourceNamespaces.r2AccountId = null;
      await repin(f, 'namespace');
    },
  ],
  ...['A'.repeat(32), 'original-account'].map((account) => [
    'malformed original R2 account ' + account,
    async (f) => {
      f.config.sourceNamespaces.r2AccountId = account;
      await repin(f, 'namespace');
    },
  ]),
  ...['Original-bucket', 'original.bucket'].map((bucket) => [
    'malformed original R2 bucket ' + bucket,
    async (f) => {
      f.config.sourceNamespaces.r2Bucket = bucket;
      f.rows[0].r2.bucket = bucket;
      await repin(f, 'namespace');
      await rewriteRows(f);
    },
  ]),
  [
    'wrong original R2 bucket',
    async (f) => {
      f.config.sourceNamespaces.r2Bucket = 'different';
      await repin(f, 'namespace');
    },
  ],
  [
    'unknown original R2 bucket cannot borrow the deployment bucket',
    async (f) => {
      f.rows[0].r2.bucket = null;
      await rewriteRows(f);
    },
  ],
  [
    'wrong original PikPak URL',
    async (f) => {
      f.config.sourceNamespaces.pikpakUrl = 'https://other.example/dav';
      await repin(f, 'namespace');
    },
  ],
  [
    'unsafe Local file',
    async (f) => {
      f.rows[0].file = 'images/../different.webp';
      await rewriteRows(f);
    },
  ],
  [
    'contradictory original hash',
    async (f) => {
      f.rows[0].r2.sha256 = 'e'.repeat(64);
      await rewriteRows(f);
    },
  ],
  [
    'contradictory original zero length',
    async (f) => {
      f.rows[0].r2.byteLength = 1;
      await rewriteRows(f);
    },
  ],
  [
    'uninterpretable original removal',
    async (f) => {
      f.rows[0].r2.localRemovedAt = '';
      await rewriteRows(f);
    },
  ],
  [
    'duplicate artifact identity',
    async (f) => {
      f.rows.push(f.rows[0]);
      await rewriteRows(f);
    },
  ],
  [
    'artifact tuple detached from original audit',
    async (f) => {
      await writeFile(f.options.artifacts, '{}\n');
    },
  ],
  [
    'original row count cannot self-certify a missing artifact',
    async (f) => {
      f.auditValue.artifacts++;
      f.auditValue.exports.artifacts.rowCount++;
      await repin(f, 'audit');
    },
  ],
  [
    'missing original final LF',
    async (f) => {
      const bytes = f.artifactBytes.subarray(0, -1);
      await writeFile(f.options.artifacts, bytes);
      f.auditValue.exports.artifacts.byteLength = bytes.length;
      f.auditValue.exports.artifacts.sha256 = sha256(bytes);
      await repin(f, 'audit');
    },
  ],
  [
    'missing pin pair',
    async (f) => {
      delete f.options.namespaceAuthoritySha256;
    },
  ],
  [
    'unknown option',
    async (f) => {
      f.options.currentRoot = '/not-authority';
    },
  ],
];
test('source/audit/namespace/locator contradictions refuse before creating output', async (t) => {
  for (const [name, mutate] of refusals) {
    await t.test(name, async (t) => {
      const f = await fixture(t, [
        row('original', {
          file: 'images/original.webp',
          sha256: 'd'.repeat(64),
          byteLength: 0,
          r2: remote({ sha256: 'd'.repeat(64), byteLength: 0 }),
          pikpak: { url: 'https://original.example/dav', encoding: 'brotli', uploadedAt: null },
        }),
      ]);
      const originalSource = await readFile(f.options.source);
      await mutate(f);
      await assert.rejects(resolveLegacyRetainedDependencies(f.options));
      await assert.rejects(lstat(f.options.output), { code: 'ENOENT' });
      assert.deepEqual(await readFile(f.options.source), originalSource);
    });
  }
});

test('output cannot replace source or a proof; CLI uses exactly nine kebab flags and rejects duplicates/unknown/positionals', async (t) => {
  const f = await fixture(t);
  for (const file of [
    f.options.source,
    f.options.audit,
    f.options.artifacts,
    f.options.namespaceAuthority,
    f.options.inventory,
  ]) {
    const before = await readFile(file);
    await assert.rejects(
      resolveLegacyRetainedDependencies({ ...f.options, output: file }),
      /EEXIST/,
    );
    assert.deepEqual(await readFile(file), before);
  }
  const args = Object.entries(f.options).map(
    ([key, value]) =>
      '--' + key.replace(/[A-Z]/g, (letter) => '-' + letter.toLowerCase()) + '=' + value,
  );
  assert.equal(args.length, 9);
  const guard = path.join(f.directory, 'no-network.mjs');
  await writeFile(
    guard,
    "import net from 'node:net';import {syncBuiltinESMExports} from 'node:module';const deny=()=>{throw Error('No network in offline producer test');};net.Socket.prototype.connect=deny;globalThis.fetch=deny;syncBuiltinESMExports();\n",
  );
  const run = (arguments_) =>
    spawnSync(
      process.execPath,
      ['--import=' + pathToFileURL(guard).href, moduleFile, ...arguments_],
      { encoding: 'utf8', windowsHide: true, timeout: 30000 },
    );
  const success = run(args);
  assert.equal(success.status, 0, success.stderr);
  assert.equal(JSON.parse(success.stdout).artifactRows, 1);
  for (const changed of [
    [...args, '--unexpected=value'],
    [...args, args[0]],
    ['--source', f.options.source],
    args.filter((arg) => !arg.startsWith('--audit-sha256=')),
    args.filter((arg) => !arg.startsWith('--inventory-sha256=')),
  ]) {
    const failure = run(changed);
    assert.equal(failure.status, 1, failure.stdout);
    assert.equal(failure.stdout, '');
    assert.equal(failure.stderr.trim(), 'Legacy retained dependency resolution failed.');
  }
});

test('another legitimate SQLite source cannot borrow the selected audit and namespace proof', async (t) => {
  const first = await fixture(t),
    second = await fixture(t);
  const db = new Database(second.options.source);
  db.prepare('UPDATE original_source SET value=?').run('a different original cut');
  db.close();
  const before = await Promise.all(
    [first.options.source, second.options.source].map((file) => readFile(file)),
  );
  await assert.rejects(
    resolveLegacyRetainedDependencies({ ...first.options, source: second.options.source }),
  );
  await assert.rejects(lstat(first.options.output), { code: 'ENOENT' });
  assert.deepEqual(
    await Promise.all([first.options.source, second.options.source].map((file) => readFile(file))),
    before,
  );
});

test('pinned cut receipts use the original dirname resolution, including selected child files', async (t) => {
  const f = await fixture(t);
  await mkdir(path.join(f.directory, 'proofs'));
  for (const name of ['sourceCapture', 'namespaceReceipt']) {
    const bytes = await readFile(path.join(f.directory, f.config[name].file));
    const selected = path.join(f.directory, 'proofs', name + '.json');
    await writeFile(selected, bytes);
    f.config[name].file = name === 'sourceCapture' ? selected : 'proofs/' + name + '.json';
  }
  await repin(f, 'namespace');
  await resolveLegacyRetainedDependencies(f.options);
  const [header] = await output(f);
  assert.equal(header.sourceCaptureSha256, f.config.sourceCapture.sha256);
  assert.equal(header.namespaceReceiptSha256, f.config.namespaceReceipt.sha256);
});

test('independently pinned positive prepared count protects only the original Local domain after all artifact rows', async (t) => {
  for (const root of ['/original/prepared', 'Z:\\original\\prepared']) {
    const f = await fixture(t, [row('no-published-location')]);
    f.config.sourceNamespaces.dataDir = root;
    f.inventoryValue.liabilities.preparedResults = 2;
    await repin(f, 'namespace');
    await repin(f, 'inventory');
    const files = [
      f.options.source,
      f.options.audit,
      f.options.artifacts,
      f.options.inventory,
      f.options.namespaceAuthority,
    ];
    const before = await Promise.all(files.map((file) => readFile(file)));
    await resolveLegacyRetainedDependencies(f.options);
    const [header, artifact, domain] = await output(f);
    assert.equal(header.preparedResults, 2);
    assert.equal(header.inventorySha256, f.options.inventorySha256);
    assert.equal(header.artifactRows, 1);
    assert.deepEqual(artifact.originalRow, { line: 1, sha256: sha256(f.artifactBytes) });
    assert.deepEqual(artifact.replicas, []);
    assert.deepEqual(domain, { kind: 'prepared-domain', dataDir: root, count: 2 });
    assert.deepEqual(await Promise.all(files.map((file) => readFile(file))), before);
    assert.equal((await output(f)).length, 3);
    assert.doesNotMatch(JSON.stringify(domain), /descriptor|objectKey|r2|current|new-b-root/);
  }
});

const inventoryRefusals = [
  [
    'missing inventory',
    async (f) => {
      delete f.options.inventory;
    },
  ],
  [
    'missing inventory pin',
    async (f) => {
      delete f.options.inventorySha256;
    },
  ],
  [
    'changed inventory under original pin',
    async (f) => {
      await writeFile(f.options.inventory, '{}');
    },
  ],
  [
    'another source',
    async (f) => {
      f.inventoryValue.source.databaseSha256 = '0'.repeat(64);
      await repin(f, 'inventory');
    },
  ],
  [
    'another commit',
    async (f) => {
      f.inventoryValue.source.declaredCommit = '0'.repeat(40);
      await repin(f, 'inventory');
    },
  ],
  [
    'another cut',
    async (f) => {
      f.inventoryValue.cut.lastCommitSeq = '2';
      await repin(f, 'inventory');
    },
  ],
  [
    'unsupported format',
    async (f) => {
      f.inventoryValue.format = 'unknown';
      await repin(f, 'inventory');
    },
  ],
  [
    'failed integrity',
    async (f) => {
      f.inventoryValue.integrity = false;
      await repin(f, 'inventory');
    },
  ],
  [
    'foreign-key violation',
    async (f) => {
      f.inventoryValue.foreignKeyViolations = 1;
      await repin(f, 'inventory');
    },
  ],
  [
    'incomplete opening',
    async (f) => {
      f.inventoryValue.cut.openingComplete = false;
      await repin(f, 'inventory');
    },
  ],
  ...[null, -1, 0.5, '0', undefined].map((count) => [
    'unknown or invalid prepared count ' + String(count),
    async (f) => {
      f.inventoryValue.liabilities.preparedResults = count;
      await repin(f, 'inventory');
    },
  ]),
  ...[null, '', 'relative-original'].map((root) => [
    'positive prepared count without proven absolute Local root ' + String(root),
    async (f) => {
      f.inventoryValue.liabilities.preparedResults = 1;
      f.config.sourceNamespaces.dataDir = root;
      await repin(f, 'inventory');
      await repin(f, 'namespace');
    },
  ]),
];
test('published artifacts do not authorize prepared=0 without a matching independent inventory', async (t) => {
  for (const [name, mutate] of inventoryRefusals) {
    await t.test(name, async (t) => {
      const f = await fixture(t, []),
        before = await readFile(f.options.source);
      await mutate(f);
      await assert.rejects(resolveLegacyRetainedDependencies(f.options));
      await assert.rejects(lstat(f.options.output), { code: 'ENOENT' });
      assert.deepEqual(await readFile(f.options.source), before);
    });
  }
});
