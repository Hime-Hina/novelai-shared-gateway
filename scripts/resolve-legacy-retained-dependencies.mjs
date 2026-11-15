import { createHash } from 'node:crypto';
import { lstat, open, readFile, realpath, unlink } from 'node:fs/promises';
import path from 'node:path';
import { parseArgs } from 'node:util';
import { fileURLToPath, pathToFileURL } from 'node:url';
import {
  readRetainedArchiveBinding,
  retainedFileSha256,
} from '../server/storage/retained-sources.ts';

const names = [
  'source',
  'audit',
  'auditSha256',
  'artifacts',
  'namespaceAuthority',
  'namespaceAuthoritySha256',
  'inventory',
  'inventorySha256',
  'output',
];
const hash = (bytes) => createHash('sha256').update(bytes).digest('hex');
const digest = (value) => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);
const record = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const exact = (value, keys) =>
  record(value) &&
  Object.keys(value).length === keys.length &&
  keys.every((key) => Object.hasOwn(value, key));
function reject() {
  throw new Error(
    'Legacy retained dependencies require the selected unchanged source and namespace proof.',
  );
}
function knownHash(value) {
  if (value !== null && !digest(value)) {
    reject();
  }
  return value;
}
function knownLength(value) {
  if (value !== null && (!Number.isSafeInteger(value) || value < 0)) {
    reject();
  }
  return value;
}
function timestamp(value) {
  if (value == null) {
    return null;
  }
  const at = typeof value === 'string' && value.trim() ? Date.parse(value) : value;
  // A malformed removal must not become an unknown/live location.
  if (!Number.isSafeInteger(at) || at < 0) {
    reject();
  }
  return at;
}
const unsafePart = (part) =>
  !part ||
  part === '.' ||
  part === '..' ||
  /[\\<>:"|?*\x00-\x1f]/.test(part) ||
  /[ .]$/.test(part) ||
  /^(?:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\.|$)/i.test(part);
function localFile(value) {
  if (
    typeof value !== 'string' ||
    !value.startsWith('images/') ||
    value.slice(7).split('/').some(unsafePart)
  ) {
    reject();
  }
  return value;
}
const absoluteRoot = (value) =>
  typeof value === 'string' &&
  (path.posix.isAbsolute(value) || path.win32.isAbsolute(value)) &&
  !/[\x00-\x1f]/.test(value);
function url(value) {
  if (typeof value !== 'string' || !value) {
    reject();
  }
  const parsed = new URL(value);
  if (
    parsed.protocol !== 'https:' ||
    parsed.username ||
    parsed.password ||
    parsed.search ||
    parsed.hash
  ) {
    reject();
  }
  return value;
}
async function pinnedJson(filename, expected) {
  if (!digest(expected) || (await retainedFileSha256(filename)) !== expected) {
    reject();
  }
  const info = await lstat(filename);
  if (info.size > 65536) {
    reject();
  }
  const bytes = await readFile(filename);
  if (hash(bytes) !== expected) {
    reject();
  }
  return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
}
async function namespaceProof(filename, expected, audit) {
  const config = await pinnedJson(filename, expected);
  if (
    !record(config) ||
    config.format !== 'novelai-legacy-cut-config-v1' ||
    config.sourceSha256 !== audit.sourceSha256 ||
    config.sourceCommit !== audit.sourceCommit ||
    config.cut !== audit.lastCommitSeq ||
    !exact(config.sourceNamespaces, ['dataDir', 'r2AccountId', 'r2Bucket', 'pikpakUrl']) ||
    Object.values(config.sourceNamespaces).some((v) => v !== null && typeof v !== 'string')
  ) {
    reject();
  }
  const directory = path.dirname(path.resolve(filename));
  for (const name of ['sourceCapture', 'namespaceReceipt']) {
    const receipt = config[name];
    if (
      !exact(receipt, ['file', 'sha256']) ||
      typeof receipt.file !== 'string' ||
      !receipt.file ||
      !digest(receipt.sha256)
    ) {
      reject();
    }
    const selected = path.resolve(directory, receipt.file);
    const info = await lstat(selected);
    if (
      !info.isFile() ||
      info.isSymbolicLink() ||
      info.size > 16777216 ||
      (await retainedFileSha256(selected)) !== receipt.sha256
    ) {
      reject();
    }
  }
  return config;
}
function artifact(original, namespace, line, bytes) {
  if (
    !exact(original, [
      'id',
      'jobId',
      'memberId',
      'operation',
      'mimeType',
      'byteLength',
      'sha256',
      'file',
      'createdAt',
      'r2',
      'pikpak',
      'externalBytesChecked',
    ]) ||
    typeof original.id !== 'string' ||
    !original.id ||
    original.externalBytesChecked !== false ||
    ['jobId', 'memberId', 'operation', 'mimeType', 'file', 'createdAt'].some(
      (key) => original[key] !== null && typeof original[key] !== 'string',
    )
  ) {
    reject();
  }
  let sha256 = knownHash(original.sha256),
    byteLength = knownLength(original.byteLength);
  const r2 = original.r2,
    pik = original.pikpak;
  if (r2 !== null) {
    if (
      !exact(r2, [
        'bucket',
        'key',
        'byteLength',
        'sha256',
        'verifiedAt',
        'localRemovedAt',
        'removedAt',
      ])
    ) {
      reject();
    }
    const remoteHash = knownHash(r2.sha256),
      remoteLength = knownLength(r2.byteLength);
    if (
      (sha256 !== null && remoteHash !== null && sha256 !== remoteHash) ||
      (byteLength !== null && remoteLength !== null && byteLength !== remoteLength)
    ) {
      reject();
    }
    sha256 ??= remoteHash;
    byteLength ??= remoteLength;
  }
  const replicas = [];
  const add = (kind, targetNamespace, objectKey, encoding, uploadedAt, verifiedAt, deletedAt) =>
    replicas.push({
      target: { kind, namespace: targetNamespace },
      replica: { objectKey, encoding, uploadedAt, verifiedAt, deletedAt },
    });
  if (original.file !== null) {
    localFile(original.file);
    if (!absoluteRoot(namespace.dataDir)) {
      reject();
    }
    add(
      'local',
      'images',
      original.file.slice(7),
      'identity',
      null,
      null,
      timestamp(r2?.localRemovedAt),
    );
  }
  if (r2 !== null) {
    if (
      typeof namespace.r2AccountId !== 'string' ||
      !/^[a-f0-9]{32}$/.test(namespace.r2AccountId) ||
      typeof namespace.r2Bucket !== 'string' ||
      !/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/.test(namespace.r2Bucket) ||
      r2.bucket !== namespace.r2Bucket ||
      typeof r2.key !== 'string' ||
      !r2.key ||
      /[\x00-\x1f]/.test(r2.key)
    ) {
      reject();
    }
    add(
      'r2',
      'r2://' + namespace.r2AccountId + '/' + namespace.r2Bucket,
      r2.key,
      'identity',
      null,
      timestamp(r2.verifiedAt),
      timestamp(r2.removedAt),
    );
  }
  if (pik !== null) {
    if (
      !exact(pik, ['url', 'uploadedAt', 'encoding']) ||
      r2 === null ||
      pik.encoding !== 'brotli' ||
      pik.url !== namespace.pikpakUrl ||
      sha256 === null
    ) {
      reject();
    }
    add('pikpak', url(pik.url), sha256 + '.br', 'br', timestamp(pik.uploadedAt), null, null);
  }
  return {
    kind: 'artifact',
    id: original.id,
    dataDir: namespace.dataDir,
    descriptor: { imageId: original.id, file: original.file, sha256, byteLength },
    replicas,
    originalRow: { line, sha256: hash(bytes) },
  };
}
export async function resolveLegacyRetainedDependencies(options) {
  if (
    !exact(options, names) ||
    names.some(
      (name) => typeof options[name] !== 'string' || !options[name] || options[name].includes('\0'),
    ) ||
    !digest(options.auditSha256) ||
    !digest(options.namespaceAuthoritySha256) ||
    !digest(options.inventorySha256)
  ) {
    reject();
  }
  const output = path.resolve(options.output),
    directory = path.dirname(output);
  // New ordinary file only: existing files and aliases never get replaced.
  const comparable = (value) => (process.platform === 'win32' ? value.toLowerCase() : value);
  if (
    comparable(await realpath(directory)) !== comparable(directory) ||
    !path.basename(output) ||
    /[\\/]$/.test(options.output)
  ) {
    reject();
  }
  if (
    process.platform === 'win32' &&
    output.slice(path.parse(output).root.length).split(path.sep).some(unsafePart)
  ) {
    reject();
  }
  const sourceBefore = await lstat(options.source, { bigint: true });
  const binding = await readRetainedArchiveBinding(
    options.source,
    options.audit,
    options.artifacts,
    options.auditSha256,
  );
  const audit = await pinnedJson(options.audit, options.auditSha256);
  if (binding.format !== 'legacy-export') {
    reject();
  }
  const authority = await namespaceProof(
    options.namespaceAuthority,
    options.namespaceAuthoritySha256,
    audit,
  );
  const inventory = await pinnedJson(options.inventory, options.inventorySha256);
  if (
    inventory?.format !== 'legacy-v4-inventory-v1' ||
    inventory.integrity !== true ||
    inventory.foreignKeyViolations !== 0 ||
    inventory.source?.databaseSha256 !== audit.sourceSha256 ||
    inventory.source?.declaredCommit !== audit.sourceCommit ||
    inventory.cut?.lastCommitSeq !== audit.lastCommitSeq ||
    inventory.cut?.openingComplete !== true ||
    !Number.isSafeInteger(inventory.liabilities?.preparedResults) ||
    inventory.liabilities.preparedResults < 0
  ) {
    reject();
  }
  const preparedResults = inventory.liabilities.preparedResults;
  if (preparedResults > 0 && !absoluteRoot(authority.sourceNamespaces.dataDir)) {
    reject();
  }
  const original = await readFile(options.artifacts);
  if (
    original.length !== audit.exports.artifacts.byteLength ||
    hash(original) !== audit.exports.artifacts.sha256
  ) {
    reject();
  }
  const text = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(original);
  const lines = text.split('\n');
  if (lines.pop() !== '' || lines.length !== audit.exports.artifacts.rowCount) {
    reject();
  }
  const seen = new Set();
  const rows = lines.map((line, index) => {
    const row = artifact(
      JSON.parse(line),
      authority.sourceNamespaces,
      index + 1,
      Buffer.from(line + '\n'),
    );
    if (seen.has(row.id)) {
      reject();
    }
    seen.add(row.id);
    return row;
  });
  const sourceAfter = await lstat(options.source, { bigint: true });
  if (
    ['dev', 'ino', 'size', 'mtimeNs', 'ctimeNs', 'nlink'].some(
      (key) => sourceBefore[key] !== sourceAfter[key],
    )
  ) {
    reject();
  }
  const header = {
    kind: 'legacy-physical-dependencies',
    format: 'novelai-legacy-retained-dependencies-v1',
    sourceSha256: binding.sourceSha256,
    sourceCommit: audit.sourceCommit,
    cut: audit.lastCommitSeq,
    auditSha256: options.auditSha256,
    artifacts: audit.exports.artifacts,
    namespaceAuthoritySha256: options.namespaceAuthoritySha256,
    sourceCaptureSha256: authority.sourceCapture.sha256,
    namespaceReceiptSha256: authority.namespaceReceipt.sha256,
    producerSha256: await retainedFileSha256(fileURLToPath(import.meta.url)),
    inventorySha256: options.inventorySha256,
    preparedResults,
    artifactRows: rows.length,
  };
  // The original inventory proves a Local obligation, not a per-job descriptor or a new B location.
  const prepared =
    preparedResults > 0
      ? [
          {
            kind: 'prepared-domain',
            dataDir: authority.sourceNamespaces.dataDir,
            count: preparedResults,
          },
        ]
      : [];
  const file = await open(output, 'wx', 0o600),
    owned = await file.stat();
  let complete = false;
  try {
    await file.writeFile(
      [header, ...rows, ...prepared].map((value) => JSON.stringify(value) + '\n').join(''),
      'utf8',
    );
    await file.sync();
    complete = true;
  } finally {
    await file.close();
    if (!complete) {
      const current = await lstat(output);
      if (current.dev === owned.dev && current.ino === owned.ino) {
        await unlink(output);
      }
    }
  }
  if (process.platform !== 'win32') {
    const parent = await open(directory, 'r');
    try {
      await parent.sync();
    } finally {
      await parent.close();
    }
  }
  return { output, sha256: await retainedFileSha256(output), artifactRows: rows.length };
}
if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  try {
    const flags = Object.fromEntries(
      names.map((name) => [name.replace(/[A-Z]/g, (letter) => '-' + letter.toLowerCase()), name]),
    );
    const args = process.argv.slice(2),
      seen = new Set();
    for (const argument of args) {
      const split = argument.indexOf('='),
        name = argument.slice(2, split);
      if (
        !argument.startsWith('--') ||
        split < 3 ||
        !Object.hasOwn(flags, name) ||
        seen.has(name)
      ) {
        reject();
      }
      seen.add(name);
    }
    const { values } = parseArgs({
      args,
      options: Object.fromEntries(Object.keys(flags).map((name) => [name, { type: 'string' }])),
    });
    const options = Object.fromEntries(
      Object.entries(values).map(([name, value]) => [flags[name], value]),
    );
    console.log(JSON.stringify(await resolveLegacyRetainedDependencies(options)));
  } catch {
    console.error('Legacy retained dependency resolution failed.');
    process.exitCode = 1;
  }
}
