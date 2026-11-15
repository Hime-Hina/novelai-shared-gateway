import { createHash, randomUUID } from 'node:crypto';
import {
  createReadStream,
  constants,
  lstatSync,
  readFileSync,
  realpathSync,
  existsSync,
  type BigIntStats,
} from 'node:fs';
import {
  copyFile,
  lstat,
  mkdir,
  open,
  readFile,
  realpath,
  rename,
  unlink,
  readlink,
  readdir,
} from 'node:fs/promises';
import path from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import { hostname } from 'node:os';
import { R2Storage } from './r2-storage.ts';
import Database from 'better-sqlite3';
import {
  openRelationalSnapshot,
  readRelationalRecoveryState,
} from './relational-private-restore.ts';
type State = 'preparing' | 'retained' | 'retiring' | 'retired';
export interface RetainedObject {
  namespace: string;
  bucket: string;
  key: string;
  byteLength: number;
  sha256: string;
}
interface Hold {
  id: string;
  kind: 'local' | 'remote' | 'archive';
  state: State;
  location: string | RetainedObject;
  producer?: {
    pid: number;
    domain: OwnerDomain | null;
    nonce: string;
    archiveSha256: string | null;
    bindingSha256: string;
  };
  retirementProof?: { namespace: string; listingSha256: string; retainedSha256: string };
}
export interface RetainedBinding {
  format: 'relational' | 'opaque' | 'legacy-export';
  sourceSha256: string | null;
  manifestSha256: string | null;
  dependenciesSha256: string | null;
  dependencies: 'known' | 'unknown';
  historyId: string | null;
  finalCommitSeq: string | null;
  resolvedDependenciesSha256?: string;
}
interface Source {
  id: string;
  category: 'daily' | 'cutover' | 'archive';
  origin: string;
  binding: RetainedBinding | null;
  holds: Hold[];
}
interface Registry {
  format: 'novelai-retained-sources-v1';
  registryId: string;
  revision: number;
  coverage: 'unknown' | InventoryCoverage;
  sources: Source[];
}
export interface RetainedInventorySelection {
  checkpoint: string;
  checkpointSha256: string;
  scope: string;
  scopeSha256: string;
}
interface InventoryCoverage {
  selection: RetainedInventorySelection;
  qualification: { file: string; sha256: string };
}
interface SourceQualification {
  sourceId: string;
  holdId: string;
  file: string;
  sourceSha256: string;
  dev: string;
  ino: string;
  size: string;
  mtimeNs: string;
  ctimeNs: string;
}
interface InventoryQualification {
  format: 'novelai-retained-qualification-v1';
  registryId: string;
  root: { path: string; dev: string; ino: string };
  selectionSha256: string;
  sources: SourceQualification[];
}
export interface RetainedArtifactReleaseClaim {
  artifactId: string;
  sha256: string;
  byteLength: number;
  source: {
    kind: 'local' | 'r2';
    namespace: string;
    objectKey: string;
    encoding: 'identity';
    dataDir?: string;
  };
  destination: {
    kind: 'r2' | 'pikpak';
    namespace: string;
    objectKey: string;
    encoding: 'identity' | 'br';
  };
}
declare const retainedPermitBrand: unique symbol;
export interface RetainedArtifactReleasePermit {
  readonly [retainedPermitBrand]: true;
}
interface LeaseGuard {
  assertCurrent(): void;
  active: boolean;
}
const releasePermits = new WeakMap<
  RetainedArtifactReleasePermit,
  {
    guard: LeaseGuard;
    claim: RetainedArtifactReleaseClaim;
    evidence: { registryId: string; revision: number; proofSha256: string };
  }
>();
interface OwnerDomain {
  platform: 'linux';
  host: string;
  bootId: string;
  pidNamespace: string;
}
async function currentOwnerDomain(): Promise<OwnerDomain | null> {
  if (process.platform !== 'linux') {
    return null;
  }
  try {
    const bootId = (await readFile('/proc/sys/kernel/random/boot_id', 'utf8')).trim();
    const pidNamespace = await readlink('/proc/self/ns/pid');
    if (!/^[a-f0-9-]{36}$/.test(bootId) || !/^pid:\[\d+\]$/.test(pidNamespace)) {
      return null;
    }
    return { platform: 'linux', host: hostname(), bootId, pidNamespace };
  } catch {
    return null;
  }
}
const idPattern = /^[a-f0-9-]{36}$/;
const hashPattern = /^[a-f0-9]{64}$/;
function fail(): never {
  throw new Error('Retained source protection refused; preserve the registry and pending records.');
}
function same(a: unknown, b: unknown) {
  return isDeepStrictEqual(a, b);
}
function validObject(value: RetainedObject) {
  return (
    value &&
    typeof value.bucket === 'string' &&
    !!value.bucket &&
    typeof value.namespace === 'string' &&
    /^r2:\/\/[a-f0-9]{32}\/[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/.test(value.namespace) &&
    value.namespace.endsWith('/' + value.bucket) &&
    typeof value.key === 'string' &&
    /^backups\/(?:daily|cutover)-[0-9TZ.]+\/[a-f0-9]{64}$/.test(value.key) &&
    Number.isSafeInteger(value.byteLength) &&
    value.byteLength >= 0 &&
    hashPattern.test(value.sha256) &&
    value.key.endsWith('/' + value.sha256)
  );
}
function samePhysicalObject(a: RetainedObject, b: RetainedObject) {
  return a.namespace === b.namespace && a.bucket === b.bucket && a.key === b.key;
}
async function assertStoppedOwner(owner: { pid: number; domain: OwnerDomain | null }) {
  const domain = await currentOwnerDomain();
  if (!domain || !owner.domain || !same(owner.domain, domain)) {
    fail();
  }
  try {
    process.kill(owner.pid, 0);
    fail();
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ESRCH') {
      throw error;
    }
  }
}
function validate(value: Registry): Registry {
  if (
    !value ||
    value.format !== 'novelai-retained-sources-v1' ||
    !idPattern.test(value.registryId) ||
    !Number.isSafeInteger(value.revision) ||
    value.revision < 0 ||
    (value.coverage !== 'unknown' &&
      (!exactFields(value.coverage, ['selection', 'qualification']) ||
        !validInventorySelection(value.coverage.selection) ||
        !exactFields(value.coverage.qualification, ['file', 'sha256']) ||
        typeof value.coverage.qualification.file !== 'string' ||
        !path.isAbsolute(value.coverage.qualification.file) ||
        !hashPattern.test(value.coverage.qualification.sha256))) ||
    !Array.isArray(value.sources)
  ) {
    fail();
  }
  const ids = new Set<string>();
  for (const source of value.sources) {
    if (
      !source ||
      !idPattern.test(source.id) ||
      ids.has(source.id) ||
      !['daily', 'cutover', 'archive'].includes(source.category) ||
      typeof source.origin !== 'string' ||
      !Array.isArray(source.holds) ||
      !source.holds.length
    ) {
      fail();
    }
    ids.add(source.id);
    if (source.binding) {
      const b = source.binding;
      if (
        !['relational', 'opaque', 'legacy-export'].includes(b.format) ||
        !['known', 'unknown'].includes(b.dependencies) ||
        [
          b.sourceSha256,
          b.manifestSha256,
          b.dependenciesSha256,
          b.resolvedDependenciesSha256 ?? null,
        ].some((h) => h !== null && !hashPattern.test(h)) ||
        (b.dependencies === 'known' &&
          ((b.format !== 'relational' &&
            !(b.format === 'legacy-export' && b.resolvedDependenciesSha256)) ||
            !b.sourceSha256 ||
            !b.manifestSha256 ||
            !b.dependenciesSha256)) ||
        (b.historyId !== null && typeof b.historyId !== 'string') ||
        (b.finalCommitSeq !== null && !/^[0-9]+$/.test(b.finalCommitSeq))
      ) {
        fail();
      }
    }
    const holds = new Set<string>();
    for (const hold of source.holds) {
      if (
        !hold ||
        !idPattern.test(hold.id) ||
        holds.has(hold.id) ||
        !['local', 'remote', 'archive'].includes(hold.kind) ||
        !['preparing', 'retained', 'retiring', 'retired'].includes(hold.state) ||
        (hold.kind === 'remote'
          ? !validObject(hold.location as RetainedObject)
          : typeof hold.location !== 'string')
      ) {
        fail();
      }
      holds.add(hold.id);
      if (
        hold.producer &&
        (hold.kind !== 'remote' ||
          !Number.isSafeInteger(hold.producer.pid) ||
          hold.producer.pid < 1 ||
          !idPattern.test(hold.producer.nonce) ||
          !hashPattern.test(hold.producer.bindingSha256) ||
          (hold.producer.archiveSha256 !== null && !hashPattern.test(hold.producer.archiveSha256)))
      ) {
        fail();
      }
      if (
        hold.retirementProof &&
        (hold.kind !== 'remote' ||
          hold.retirementProof.namespace !== (hold.location as RetainedObject).namespace ||
          !hashPattern.test(hold.retirementProof.listingSha256) ||
          !hashPattern.test(hold.retirementProof.retainedSha256))
      ) {
        fail();
      }
    }
  }
  return value;
}
function validInventorySelection(value: RetainedInventorySelection) {
  return (
    value &&
    Object.keys(value).length === 4 &&
    typeof value.checkpoint === 'string' &&
    path.isAbsolute(value.checkpoint) &&
    typeof value.scope === 'string' &&
    path.isAbsolute(value.scope) &&
    hashPattern.test(value.checkpointSha256) &&
    hashPattern.test(value.scopeSha256)
  );
}
function comparablePath(p: string) {
  return process.platform === 'win32' ? path.toNamespacedPath(p).toLowerCase() : p;
}
async function directory(root: string) {
  const selected = path.resolve(root);
  const stat = await lstat(selected);
  if (
    !stat.isDirectory() ||
    stat.isSymbolicLink() ||
    comparablePath(await realpath(selected)) !== comparablePath(selected)
  ) {
    fail();
  }
  return selected;
}
async function syncDirectory(root: string) {
  // Windows 标准库无同等目录 fsync；本片保持 coverage unknown，不能据此开放断电安全删除。
  if (process.platform === 'win32') {
    return;
  }
  const handle = await open(root, 'r');
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}
async function absent(filename: string) {
  try {
    await lstat(filename);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return true;
    }
    throw error;
  }
  return false;
}
async function regular(filename: string) {
  const stat = await lstat(filename, { bigint: true });
  if (!stat.isFile() || stat.isSymbolicLink() || stat.nlink !== 1n) {
    fail();
  }
  return stat;
}
function sameFileIdentity(a: BigIntStats, b: BigIntStats) {
  return (
    a.dev === b.dev &&
    a.ino === b.ino &&
    a.size === b.size &&
    a.mtimeNs === b.mtimeNs &&
    a.ctimeNs === b.ctimeNs &&
    a.nlink === b.nlink
  );
}
async function bytes(filename: string) {
  const before = await regular(filename),
    handle = await open(filename, 'r');
  try {
    const fd = await handle.stat({ bigint: true });
    if (before.dev !== fd.dev || before.ino !== fd.ino) {
      fail();
    }
    const value = await handle.readFile();
    const after = await regular(filename);
    if (
      before.dev !== after.dev ||
      before.ino !== after.ino ||
      before.size !== after.size ||
      before.mtimeNs !== after.mtimeNs
    ) {
      fail();
    }
    return value;
  } finally {
    await handle.close();
  }
}
export async function retainedFileSha256(filename: string) {
  const before = await regular(filename),
    digest = createHash('sha256');
  for await (const chunk of createReadStream(filename)) {
    digest.update(chunk);
  }
  const after = await regular(filename);
  if (
    before.dev !== after.dev ||
    before.ino !== after.ino ||
    before.size !== after.size ||
    before.mtimeNs !== after.mtimeNs
  ) {
    fail();
  }
  return digest.digest('hex');
}
interface LegacyExportFile {
  basename: string;
  byteLength: number;
  rowCount: number;
  sha256: string;
}
const legacyCounts = [
  'commits',
  'events',
  'protectedInputs',
  'protectedInputBytes',
  'contexts',
  'artifactInputs',
  'diagnostics',
  'artifacts',
  'networkCalls',
  'dispatches',
] as const;
type LegacyExportAudit = Record<(typeof legacyCounts)[number], number> & {
  format: string;
  sourceCommit: string;
  toolSha256: string;
  sourceSha256: string;
  openingSha256: string;
  lastCommitSeq: string;
  externalObjectBytesVerified: boolean;
  exports: { inputs: LegacyExportFile; artifacts: LegacyExportFile };
};
function exactFields(value: unknown, names: readonly string[]) {
  return (
    value !== null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    same(Object.keys(value).sort(), [...names].sort())
  );
}
function legacyExportAudit(value: LegacyExportAudit) {
  if (
    !exactFields(value, [
      'format',
      'sourceCommit',
      'toolSha256',
      'sourceSha256',
      'openingSha256',
      'lastCommitSeq',
      'externalObjectBytesVerified',
      'exports',
      ...legacyCounts,
    ]) ||
    value.format !== 'legacy-archive-audit-v1' ||
    typeof value.sourceCommit !== 'string' ||
    !/^[a-f0-9]{40}$/.test(value.sourceCommit) ||
    [value.toolSha256, value.sourceSha256, value.openingSha256].some(
      (hash) => typeof hash !== 'string' || !hashPattern.test(hash),
    ) ||
    typeof value.lastCommitSeq !== 'string' ||
    !/^[1-9][0-9]*$/.test(value.lastCommitSeq) ||
    legacyCounts.some((key) => !Number.isSafeInteger(value[key]) || value[key] < 0) ||
    BigInt(value.lastCommitSeq) !== BigInt(value.commits) ||
    value.networkCalls !== 0 ||
    value.dispatches !== 0 ||
    value.externalObjectBytesVerified !== false ||
    !exactFields(value.exports, ['inputs', 'artifacts'])
  ) {
    fail();
  }
  for (const kind of ['inputs', 'artifacts'] as const) {
    const file = value.exports[kind];
    if (
      !exactFields(file, ['basename', 'byteLength', 'rowCount', 'sha256']) ||
      file.basename !== kind + '.jsonl' ||
      !Number.isSafeInteger(file.byteLength) ||
      file.byteLength < 0 ||
      !Number.isSafeInteger(file.rowCount) ||
      file.rowCount < 0 ||
      typeof file.sha256 !== 'string' ||
      !hashPattern.test(file.sha256) ||
      (file.rowCount === 0) !== (file.byteLength === 0) ||
      (file.byteLength === 0 && file.sha256 !== createHash('sha256').digest('hex'))
    ) {
      fail();
    }
  }
  const inputRows =
    value.protectedInputs + value.contexts + value.artifactInputs + value.diagnostics;
  if (
    !Number.isSafeInteger(inputRows) ||
    value.exports.inputs.rowCount !== inputRows ||
    value.exports.artifacts.rowCount !== value.artifacts
  ) {
    fail();
  }
  return value;
}
async function legacyArtifactFile(filename: string, expected: LegacyExportFile) {
  const before = await regular(filename),
    handle = await open(filename, 'r');
  try {
    const fd = await handle.stat({ bigint: true });
    if (
      fd.dev !== before.dev ||
      fd.ino !== before.ino ||
      before.size !== BigInt(expected.byteLength)
    ) {
      fail();
    }
    const digest = createHash('sha256'),
      decoder = new TextDecoder('utf-8', { fatal: true });
    const buffer = Buffer.alloc(64 * 1024);
    let byteLength = 0,
      rowCount = 0,
      lastByte = -1;
    for (;;) {
      const { bytesRead } = await handle.read(buffer, 0, buffer.length, null);
      if (!bytesRead) {
        break;
      }
      const chunk = buffer.subarray(0, bytesRead);
      digest.update(chunk);
      decoder.decode(chunk, { stream: true });
      byteLength += bytesRead;
      for (const byte of chunk) {
        if (byte === 10) {
          rowCount++;
        }
      }
      lastByte = chunk[chunk.length - 1];
      if (!Number.isSafeInteger(byteLength) || !Number.isSafeInteger(rowCount)) {
        fail();
      }
    }
    decoder.decode();
    const after = await regular(filename);
    if (
      before.dev !== after.dev ||
      before.ino !== after.ino ||
      before.size !== after.size ||
      before.mtimeNs !== after.mtimeNs ||
      byteLength !== expected.byteLength ||
      rowCount !== expected.rowCount ||
      (byteLength !== 0 && lastByte !== 10) ||
      digest.digest('hex') !== expected.sha256
    ) {
      fail();
    }
  } finally {
    await handle.close();
  }
}
async function archiveManifest(filename: string) {
  if ((await regular(filename)).size > 65536n) {
    fail();
  }
  const raw = await bytes(filename);
  return {
    value: JSON.parse(raw.toString('utf8')),
    sha256: createHash('sha256').update(raw).digest('hex'),
  };
}
// 外部选定的 audit pin 只作用于本次接收；保存的原 manifest SHA 即固定这些字节，不另存认证布尔。
export async function readRetainedArchiveBinding(
  sourceFile: string,
  manifestFile: string,
  dependenciesFile?: string,
  expectedAuditSha256?: string,
  resolved?: { file: string; sha256: string },
): Promise<RetainedBinding> {
  if (
    expectedAuditSha256 !== undefined &&
    (typeof expectedAuditSha256 !== 'string' || !hashPattern.test(expectedAuditSha256))
  ) {
    fail();
  }
  const manifest = await archiveManifest(manifestFile);
  if (expectedAuditSha256 !== undefined && manifest.sha256 !== expectedAuditSha256) {
    fail();
  }
  const sourceSha256 = await retainedFileSha256(sourceFile),
    identity = manifest.value;
  if (!identity || identity.sourceSha256 !== sourceSha256) {
    fail();
  }
  let dependenciesSha256 = dependenciesFile ? await retainedFileSha256(dependenciesFile) : null;
  if (
    expectedAuditSha256 !== undefined ||
    (identity.format === 'legacy-archive-audit-v1' && identity.exports !== undefined)
  ) {
    const audit = legacyExportAudit(identity);
    if (!dependenciesFile) {
      fail();
    }
    if (path.basename(dependenciesFile) !== 'artifacts.jsonl') {
      fail();
    }
    await legacyArtifactFile(dependenciesFile, audit.exports.artifacts);
    dependenciesSha256 = audit.exports.artifacts.sha256;
  }
  const binding: RetainedBinding = {
    format: identity.format === 'legacy-archive-audit-v1' ? 'legacy-export' : 'opaque',
    sourceSha256,
    manifestSha256: manifest.sha256,
    dependenciesSha256,
    dependencies: 'unknown',
    historyId: null,
    finalCommitSeq: identity.lastCommitSeq ?? null,
  };
  if (resolved) {
    if (
      !expectedAuditSha256 ||
      !dependenciesFile ||
      !exactFields(resolved, ['file', 'sha256']) ||
      !hashPattern.test(resolved.sha256)
    ) {
      fail();
    }
    await resolvedLegacyDependencies(
      resolved.file,
      resolved.sha256,
      manifest.value,
      dependenciesFile,
      manifest.sha256,
    );
    binding.resolvedDependenciesSha256 = resolved.sha256;
    binding.dependencies = 'known';
  }
  return binding;
}
async function resolvedLegacyDependencies(
  filename: string,
  sha256: string,
  value: LegacyExportAudit,
  artifactsFile: string,
  auditSha256: string,
) {
  const audit = legacyExportAudit(value),
    raw = await bytes(filename);
  if (
    createHash('sha256').update(raw).digest('hex') !== sha256 ||
    !raw.length ||
    raw[raw.length - 1] !== 10
  ) {
    fail();
  }
  const lines = new TextDecoder('utf-8', { fatal: true }).decode(raw).split('\n');
  lines.pop();
  const header = JSON.parse(lines.shift()!);
  if (
    !exactFields(header, [
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
      'artifactRows',
      'inventorySha256',
      'preparedResults',
    ]) ||
    header.kind !== 'legacy-physical-dependencies' ||
    header.format !== 'novelai-legacy-retained-dependencies-v1' ||
    header.sourceSha256 !== audit.sourceSha256 ||
    header.sourceCommit !== audit.sourceCommit ||
    header.cut !== audit.lastCommitSeq ||
    header.auditSha256 !== auditSha256 ||
    !same(header.artifacts, audit.exports.artifacts) ||
    header.artifactRows !== audit.artifacts ||
    !Number.isSafeInteger(header.preparedResults) ||
    header.preparedResults < 0 ||
    [
      'namespaceAuthoritySha256',
      'sourceCaptureSha256',
      'namespaceReceiptSha256',
      'producerSha256',
      'inventorySha256',
    ].some((key) => !hashPattern.test(header[key]))
  ) {
    fail();
  }
  const originalBytes = await bytes(artifactsFile),
    originals: Buffer[] = [];
  let start = 0,
    end;
  while ((end = originalBytes.indexOf(10, start)) >= 0) {
    originals.push(originalBytes.subarray(start, end + 1));
    start = end + 1;
  }
  if (
    lines.length !== originals.length + (header.preparedResults > 0 ? 1 : 0) ||
    originals.length !== audit.artifacts
  ) {
    fail();
  }
  const ids = new Set<string>();
  for (let index = 0; index < originals.length; index++) {
    const row = JSON.parse(lines[index]),
      original = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(originals[index]));
    if (
      typeof original.id !== 'string' ||
      !original.id ||
      (original.file !== null &&
        (typeof original.file !== 'string' ||
          !original.file.startsWith('images/') ||
          !original.file.slice(7))) ||
      [original.r2, original.pikpak].some(
        (item) => item != null && (typeof item !== 'object' || Array.isArray(item)),
      ) ||
      (original.sha256 != null &&
        original.r2?.sha256 != null &&
        original.sha256 !== original.r2.sha256) ||
      (original.byteLength != null &&
        original.r2?.byteLength != null &&
        original.byteLength !== original.r2.byteLength)
    ) {
      fail();
    }
    if (
      !exactFields(row, ['kind', 'id', 'dataDir', 'descriptor', 'replicas', 'originalRow']) ||
      row.kind !== 'artifact' ||
      typeof row.id !== 'string' ||
      !row.id ||
      row.id !== original.id ||
      ids.has(row.id) ||
      !exactFields(row.originalRow, ['line', 'sha256']) ||
      row.originalRow.line !== index + 1 ||
      row.originalRow.sha256 !== createHash('sha256').update(originals[index]).digest('hex') ||
      !exactFields(row.descriptor, ['imageId', 'file', 'sha256', 'byteLength']) ||
      row.descriptor.imageId !== row.id ||
      !(row.descriptor.file === null || typeof row.descriptor.file === 'string') ||
      !(
        row.descriptor.sha256 === null ||
        (typeof row.descriptor.sha256 === 'string' && hashPattern.test(row.descriptor.sha256))
      ) ||
      !(
        row.descriptor.byteLength === null ||
        (Number.isSafeInteger(row.descriptor.byteLength) && row.descriptor.byteLength > 0)
      ) ||
      !(
        row.dataDir === null ||
        (typeof row.dataDir === 'string' &&
          (path.posix.isAbsolute(row.dataDir) || path.win32.isAbsolute(row.dataDir)))
      ) ||
      !Array.isArray(row.replicas)
    ) {
      fail();
    }
    const kinds = new Set<string>();
    for (const entry of row.replicas) {
      if (
        !exactFields(entry, ['target', 'replica']) ||
        !exactFields(entry.target, ['kind', 'namespace']) ||
        !exactFields(entry.replica, [
          'objectKey',
          'encoding',
          'uploadedAt',
          'verifiedAt',
          'deletedAt',
        ]) ||
        kinds.has(entry.target.kind) ||
        typeof entry.target.namespace !== 'string' ||
        !entry.target.namespace ||
        typeof entry.replica.objectKey !== 'string' ||
        !entry.replica.objectKey ||
        [entry.replica.uploadedAt, entry.replica.verifiedAt, entry.replica.deletedAt].some(
          (at) => at !== null && (!Number.isSafeInteger(at) || at < 0),
        )
      ) {
        fail();
      }
      if (entry.target.kind === 'local') {
        if (
          entry.target.namespace !== 'images' ||
          !row.dataDir ||
          row.descriptor.file !== 'images/' + entry.replica.objectKey ||
          entry.replica.encoding !== 'identity'
        ) {
          fail();
        }
      } else if (entry.target.kind === 'r2') {
        if (
          !/^r2:\/\/[a-f0-9]{32}\/[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/.test(entry.target.namespace) ||
          entry.replica.encoding !== 'identity'
        ) {
          fail();
        }
      } else if (entry.target.kind === 'pikpak') {
        const location = new URL(entry.target.namespace);
        if (
          location.protocol !== 'https:' ||
          location.username ||
          location.password ||
          location.search ||
          location.hash ||
          entry.replica.encoding !== 'br'
        ) {
          fail();
        }
      } else {
        fail();
      }
      kinds.add(entry.target.kind);
    }
    ids.add(row.id);
  }
  if (header.preparedResults > 0) {
    const domain = JSON.parse(lines[lines.length - 1]);
    if (
      !exactFields(domain, ['kind', 'dataDir', 'count']) ||
      domain.kind !== 'prepared-domain' ||
      domain.count !== header.preparedResults ||
      typeof domain.dataDir !== 'string' ||
      !(path.posix.isAbsolute(domain.dataDir) || path.win32.isAbsolute(domain.dataDir))
    ) {
      fail();
    }
  }
  return { header, rows: lines.map((line) => JSON.parse(line) as RetainedDependency) };
}
async function boundLegacyExport(root: string, binding: RetainedBinding) {
  if (binding.format !== 'legacy-export') {
    return;
  }
  const { value, sha256 } = await archiveManifest(path.join(root, 'manifest.json'));
  if (
    sha256 !== binding.manifestSha256 ||
    !value ||
    value.format !== 'legacy-archive-audit-v1' ||
    value.sourceSha256 !== binding.sourceSha256 ||
    (value.lastCommitSeq ?? null) !== binding.finalCommitSeq
  ) {
    fail();
  }
  if (value.exports === undefined) {
    return;
  }
  const audit = legacyExportAudit(value);
  if (binding.dependenciesSha256 !== audit.exports.artifacts.sha256) {
    fail();
  }
  await legacyArtifactFile(path.join(root, 'dependencies.jsonl'), audit.exports.artifacts);
  if (binding.resolvedDependenciesSha256) {
    await resolvedLegacyDependencies(
      path.join(root, 'resolved-dependencies.jsonl'),
      binding.resolvedDependenciesSha256,
      audit,
      path.join(root, 'dependencies.jsonl'),
      binding.manifestSha256!,
    );
  }
}
async function immutableFile(filename: string, value: string | Buffer) {
  const handle = await open(filename, 'wx', 0o600);
  try {
    await handle.writeFile(value);
    await handle.sync();
  } finally {
    await handle.close();
  }
  await syncDirectory(path.dirname(filename));
}
async function immutableCopy(from: string, to: string, expected: string) {
  if ((await retainedFileSha256(from)) !== expected) {
    fail();
  }
  if (!(await absent(to))) {
    if ((await retainedFileSha256(to)) !== expected) {
      fail();
    }
    return;
  }
  await copyFile(from, to, constants.COPYFILE_EXCL);
  const handle = await open(to, 'r+');
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
  if (
    (await retainedFileSha256(from)) !== expected ||
    (await retainedFileSha256(to)) !== expected
  ) {
    fail();
  }
  await syncDirectory(path.dirname(to));
}
async function readRegistry(root: string) {
  return validate(JSON.parse((await bytes(path.join(root, 'registry.json'))).toString('utf8')));
}
export async function initializeRetainedSources(root: string) {
  await mkdir(path.resolve(root), { mode: 0o700 });
  root = await directory(root);
  const registry: Registry = {
    format: 'novelai-retained-sources-v1',
    registryId: randomUUID(),
    revision: 0,
    coverage: 'unknown',
    sources: [],
  };
  await immutableFile(path.join(root, 'registry.json'), JSON.stringify(registry, null, 2) + '\n');
  await mkdir(path.join(root, 'sources'), { mode: 0o700 });
  await syncDirectory(root);
  return { registryId: registry.registryId, releaseAllowed: false };
}
async function mutate<T>(
  root: string,
  change: (registry: Registry, root: string, guard: LeaseGuard) => Promise<T>,
) {
  root = await directory(root);
  const rootIdentity = await lstat(root, { bigint: true });
  if (!(await absent(path.join(root, 'recovery.lock')))) {
    fail();
  }
  const initial = await readRegistry(root),
    filename = path.join(root, 'lease.json');
  const registryIdentity = await regular(path.join(root, 'registry.json'));
  const lease = await open(filename, 'wx', 0o600);
  const leaseIdentity = await lease.stat({ bigint: true });
  const token = {
    nonce: randomUUID(),
    pid: process.pid,
    registryId: initial.registryId,
    revision: initial.revision,
    domain: await currentOwnerDomain(),
  };
  const guard: LeaseGuard = {
    active: true,
    assertCurrent() {
      if (!guard.active || comparablePath(realpathSync(root)) !== comparablePath(root)) {
        fail();
      }
      const actualRoot = lstatSync(root, { bigint: true }),
        actualLease = lstatSync(filename, { bigint: true }),
        actualRegistry = lstatSync(path.join(root, 'registry.json'), { bigint: true });
      if (
        !actualRoot.isDirectory() ||
        actualRoot.isSymbolicLink() ||
        actualRoot.dev !== rootIdentity.dev ||
        actualRoot.ino !== rootIdentity.ino ||
        !actualLease.isFile() ||
        actualLease.isSymbolicLink() ||
        actualLease.nlink !== 1n ||
        actualLease.dev !== leaseIdentity.dev ||
        actualLease.ino !== leaseIdentity.ino ||
        !same(JSON.parse(readFileSync(filename, 'utf8')), token) ||
        !actualRegistry.isFile() ||
        actualRegistry.isSymbolicLink() ||
        actualRegistry.nlink !== 1n ||
        !sameFileIdentity(actualRegistry, registryIdentity) ||
        !same(JSON.parse(readFileSync(path.join(root, 'registry.json'), 'utf8')), initial)
      ) {
        fail();
      }
    },
  };
  let owned = false;
  try {
    await lease.writeFile(JSON.stringify(token) + '\n');
    await lease.sync();
    await syncDirectory(root);
    owned = true;
    if (!(await absent(path.join(root, 'recovery.lock')))) {
      fail();
    }
    const current = await readRegistry(root);
    if (!same(current, initial)) {
      fail();
    }
    const result = await change(current, root, guard);
    guard.assertCurrent();
    if (same(current, initial)) {
      return result;
    }
    current.revision++;
    validate(current);
    const temporary = path.join(root, 'registry-' + token.nonce + '.tmp');
    await immutableFile(temporary, JSON.stringify(current, null, 2) + '\n');
    await directory(root);
    const currentRoot = await lstat(root, { bigint: true });
    const currentLease = await regular(filename);
    if (
      currentRoot.dev !== rootIdentity.dev ||
      currentRoot.ino !== rootIdentity.ino ||
      currentLease.dev !== leaseIdentity.dev ||
      currentLease.ino !== leaseIdentity.ino
    ) {
      fail();
    }
    await rename(temporary, path.join(root, 'registry.json'));
    await syncDirectory(root);
    return result;
  } finally {
    guard.active = false;
    await lease.close();
    const currentLease = owned ? await regular(filename) : null;
    if (
      owned &&
      currentLease?.dev === leaseIdentity.dev &&
      currentLease.ino === leaseIdentity.ino &&
      same(JSON.parse((await bytes(filename)).toString()), token)
    ) {
      await unlink(filename);
      await syncDirectory(root);
    }
  }
}
async function readProtectedRetainedRegistry(root: string, checkpointSha256?: string) {
  root = await directory(root);
  if (
    !(await absent(path.join(root, 'lease.json'))) ||
    !(await absent(path.join(root, 'recovery.lock')))
  ) {
    fail();
  }
  const registry = await readRegistry(root);
  if (checkpointSha256 !== undefined) {
    if (
      typeof checkpointSha256 !== 'string' ||
      !hashPattern.test(checkpointSha256) ||
      (await retainedFileSha256(path.join(root, 'registry.json'))) !== checkpointSha256
    ) {
      fail();
    }
    const marker = JSON.parse((await bytes(path.join(root, 'checkpoint.json'))).toString());
    if (
      !same(marker, {
        format: 'novelai-retained-checkpoint-v1',
        registryId: registry.registryId,
        sha256: checkpointSha256,
      })
    ) {
      fail();
    }
  }
  for (const source of registry.sources) {
    if (!source.holds.some((hold) => hold.state !== 'retired') || !source.binding?.manifestSha256) {
      continue;
    }
    const sourceRoot = path.join(root, 'sources', source.id);
    await directory(sourceRoot);
    if (
      (await retainedFileSha256(path.join(sourceRoot, 'manifest.json'))) !==
      source.binding.manifestSha256
    ) {
      fail();
    }
    if (
      source.binding.dependenciesSha256 &&
      (await retainedFileSha256(path.join(sourceRoot, 'dependencies.jsonl'))) !==
        source.binding.dependenciesSha256
    ) {
      fail();
    }
    await boundLegacyExport(sourceRoot, source.binding);
  }
  return registry;
}
// checkpoint读取和restore共用完整证明校验；普通live registry读取不要求checkpoint标记。
export async function readRetainedSourceProtection(root: string, checkpointSha256?: string) {
  const registry = await readProtectedRetainedRegistry(root, checkpointSha256);
  return { ...registry, releaseAllowed: false as const };
}
interface InventoryScope {
  format: 'novelai-retained-inventory-scope-v1';
  registryId: string;
  localRoots: string[];
  archiveRoots: string[];
  r2Namespaces: string[];
}
interface InventoryConfiguration {
  r2?: R2Storage | null;
}
interface ArtifactDependency {
  kind: 'artifact';
  id: string;
  dataDir: string | null;
  descriptor: {
    imageId: string;
    file: string | null;
    sha256: string | null;
    byteLength: number | null;
  };
  replicas: {
    target: { kind: string; namespace: string };
    replica: {
      objectKey: string;
      encoding: string;
      deletedAt: number | null;
      uploadedAt: number | null;
      verifiedAt: number | null;
    };
  }[];
}
type RetainedDependency =
  | ArtifactDependency
  | {
      kind: 'prepared-result';
      dataDir: string | null;
      descriptor: ArtifactDependency['descriptor'];
    }
  | { kind: 'prepared-domain'; dataDir: string; count: number };
const scopeEntry = 'config/retained-inventory-scope.json';
function digest(value: unknown) {
  return createHash('sha256').update(JSON.stringify(value)).digest('hex');
}
function sourceRootNamespace(value: string) {
  const windows =
    /^[A-Za-z]:[\\/]/.test(value) || value.startsWith('\\\\') || value.startsWith('//');
  if (!windows && !path.posix.isAbsolute(value)) {
    fail();
  }
  const normalize = (root: string) =>
    windows
      ? 'win32:' + path.win32.normalize(root).toLowerCase()
      : 'posix:' + path.posix.normalize(root);
  if (windows === (process.platform === 'win32')) {
    try {
      return normalize(realpathSync(value));
    } catch {
      /* A known original namespace may not be mounted on this recovery host. */
    }
  }
  return normalize(value);
}

// Scope defines a domain, never its members. Its bytes must already be protected by an
// independently pinned Source; membership comes only from actual directory/provider enumeration.
async function verifiedInventory(
  root: string,
  registry: Registry,
  selected: RetainedInventorySelection,
  configuration: InventoryConfiguration,
  qualification: InventoryQualification | null = null,
) {
  if (
    !validInventorySelection(selected) ||
    comparablePath(selected.checkpoint) === comparablePath(root)
  ) {
    fail();
  }
  const anchor = await readProtectedRetainedRegistry(
    selected.checkpoint,
    selected.checkpointSha256,
  );
  if (
    anchor.registryId !== registry.registryId ||
    !anchor.sources.length ||
    anchor.revision > registry.revision ||
    anchor.sources.length !== registry.sources.length
  ) {
    return null;
  }
  const scopeBytes = await bytes(selected.scope);
  if (createHash('sha256').update(scopeBytes).digest('hex') !== selected.scopeSha256) {
    fail();
  }
  const scope = JSON.parse(scopeBytes.toString('utf8')) as InventoryScope;
  if (
    !exactFields(scope, ['format', 'registryId', 'localRoots', 'archiveRoots', 'r2Namespaces']) ||
    scope.format !== 'novelai-retained-inventory-scope-v1' ||
    scope.registryId !== registry.registryId ||
    ![scope.localRoots, scope.archiveRoots, scope.r2Namespaces].every(
      (rows) =>
        Array.isArray(rows) &&
        rows.every((value) => typeof value === 'string') &&
        new Set(rows).size === rows.length,
    )
  ) {
    fail();
  }
  const dependencies: RetainedDependency[] = [],
    qualifiedSources: SourceQualification[] = [],
    expectedLocal = new Set<string>(),
    expectedArchive = new Set<string>(),
    expectedRemote = new Map<string, RetainedObject>();
  const evidenceFiles = new Map<string, Awaited<ReturnType<typeof regular>>>();
  const bindEvidence = async (filename: string) => {
    evidenceFiles.set(filename, await regular(filename));
  };
  await bindEvidence(selected.scope);
  await bindEvidence(path.join(selected.checkpoint, 'registry.json'));
  let protectedScope = false;
  for (const row of registry.sources) {
    const original = anchor.sources.find((value) => value.id === row.id);
    if (
      !original ||
      row.category !== original.category ||
      row.origin !== original.origin ||
      !same(row.binding, original.binding) ||
      row.holds.length !== original.holds.length
    ) {
      return null;
    }
    for (const hold of row.holds) {
      const old = original.holds.find((value) => value.id === hold.id);
      if (
        !old ||
        old.kind !== hold.kind ||
        !same(old.location, hold.location) ||
        (old.state === 'retired' && hold.state !== 'retired')
      ) {
        return null;
      }
    }
    const active = row.holds.filter((hold) => hold.state !== 'retired');
    if (!active.length) {
      continue;
    }
    const legacy =
      row.binding?.format === 'legacy-export' && !!row.binding.resolvedDependenciesSha256;
    if (
      active.some((hold) => hold.state !== 'retained') ||
      !row.binding ||
      row.binding.dependencies !== 'known' ||
      (!legacy && (row.binding.format !== 'relational' || !row.binding.historyId)) ||
      !/^[1-9][0-9]*$/.test(row.binding.finalCommitSeq ?? '')
    ) {
      return null;
    }
    const stored = path.join(root, 'sources', row.id),
      anchored = path.join(selected.checkpoint, 'sources', row.id),
      manifestBytes = await bytes(path.join(stored, 'manifest.json')),
      originalDependencyBytes = await bytes(path.join(stored, 'dependencies.jsonl'));
    await bindEvidence(path.join(stored, 'manifest.json'));
    await bindEvidence(path.join(stored, 'dependencies.jsonl'));
    await bindEvidence(path.join(anchored, 'manifest.json'));
    await bindEvidence(path.join(anchored, 'dependencies.jsonl'));
    if (
      createHash('sha256').update(manifestBytes).digest('hex') !== row.binding.manifestSha256 ||
      createHash('sha256').update(originalDependencyBytes).digest('hex') !==
        row.binding.dependenciesSha256
    ) {
      fail();
    }
    const manifest = JSON.parse(manifestBytes.toString('utf8'));
    let dependencyBytes = originalDependencyBytes;
    if (legacy) {
      await boundLegacyExport(stored, row.binding);
      dependencyBytes = await bytes(path.join(stored, 'resolved-dependencies.jsonl'));
      await bindEvidence(path.join(stored, 'resolved-dependencies.jsonl'));
      await bindEvidence(path.join(anchored, 'resolved-dependencies.jsonl'));
    } else if (
      manifest.files?.['data/library.sqlite'] !== row.binding.sourceSha256 ||
      manifest.files?.['retained-dependencies.jsonl'] !== row.binding.dependenciesSha256 ||
      manifest.retainedSource?.sourceId !== row.id ||
      manifest.retainedSource?.registryId !== registry.registryId
    ) {
      fail();
    }
    const anchorManifest = JSON.parse(
      (await bytes(path.join(anchored, 'manifest.json'))).toString('utf8'),
    );
    if (!legacy && anchorManifest.files?.[scopeEntry] === selected.scopeSha256) {
      protectedScope = true;
    }
    if (dependencyBytes.length && dependencyBytes[dependencyBytes.length - 1] !== 10) {
      fail();
    }
    const dependencyLines = dependencyBytes.toString('utf8').split('\n').filter(Boolean);
    if (legacy) {
      dependencyLines.shift();
    }
    for (const line of dependencyLines) {
      const dependency = JSON.parse(line) as RetainedDependency;
      if (dependency.kind === 'prepared-domain') {
        if (
          typeof dependency.dataDir !== 'string' ||
          !(
            path.posix.isAbsolute(dependency.dataDir) || path.win32.isAbsolute(dependency.dataDir)
          ) ||
          !Number.isSafeInteger(dependency.count) ||
          dependency.count < 1
        ) {
          fail();
        }
        dependencies.push(dependency);
        continue;
      }
      dependency.dataDir ??= null;
      if (
        !['artifact', 'prepared-result'].includes(dependency.kind) ||
        !dependency.descriptor ||
        !(
          dependency.descriptor.sha256 === null ||
          (typeof dependency.descriptor.sha256 === 'string' &&
            hashPattern.test(dependency.descriptor.sha256))
        ) ||
        !(
          dependency.descriptor.byteLength === null ||
          (typeof dependency.descriptor.byteLength === 'number' &&
            Number.isSafeInteger(dependency.descriptor.byteLength) &&
            dependency.descriptor.byteLength > 0)
        ) ||
        !(dependency.descriptor.file === null || typeof dependency.descriptor.file === 'string') ||
        !(
          dependency.dataDir === null ||
          (typeof dependency.dataDir === 'string' &&
            (path.posix.isAbsolute(dependency.dataDir) ||
              path.win32.isAbsolute(dependency.dataDir)))
        ) ||
        (dependency.kind === 'prepared-result' &&
          (dependency.descriptor.sha256 === null ||
            dependency.descriptor.byteLength === null ||
            dependency.descriptor.file === null))
      ) {
        return null;
      }
      if (
        dependency.kind === 'artifact' &&
        (dependency.id !== dependency.descriptor.imageId || !Array.isArray(dependency.replicas))
      ) {
        fail();
      }
      if (
        dependency.kind === 'artifact' &&
        dependency.replicas.some(
          (entry) =>
            !entry ||
            !entry.target ||
            !entry.replica ||
            !['local', 'r2', 'pikpak'].includes(entry.target.kind) ||
            typeof entry.target.namespace !== 'string' ||
            typeof entry.replica.objectKey !== 'string' ||
            !['identity', 'br', 'gzip'].includes(entry.replica.encoding) ||
            [entry.replica.deletedAt, entry.replica.uploadedAt, entry.replica.verifiedAt].some(
              (at) => at !== null && (!Number.isSafeInteger(at) || at! < 0),
            ),
        )
      ) {
        fail();
      }
      dependencies.push(dependency);
    }
    for (const hold of active) {
      if (hold.kind === 'remote') {
        const object = hold.location as RetainedObject;
        if (!scope.r2Namespaces.includes(object.namespace)) {
          return null;
        }
        const key = object.namespace + '\n' + object.key;
        if (expectedRemote.has(key) && !same(expectedRemote.get(key), object)) {
          fail();
        }
        expectedRemote.set(key, object);
      } else {
        const location = path.resolve(hold.location as string),
          roots = hold.kind === 'local' ? scope.localRoots : scope.archiveRoots;
        if (
          !roots.some(
            (base) => comparablePath(path.resolve(base)) === comparablePath(path.dirname(location)),
          )
        ) {
          return null;
        }
        if (hold.kind === 'local') {
          await directory(location);
          if (
            (await retainedFileSha256(path.join(location, 'manifest.json'))) !==
            row.binding.manifestSha256
          ) {
            fail();
          }
          await bindEvidence(path.join(location, 'manifest.json'));
          const filename = path.join(location, 'data/library.sqlite'),
            before = await regular(filename),
            witness: SourceQualification = {
              sourceId: row.id,
              holdId: hold.id,
              file: filename,
              sourceSha256: row.binding.sourceSha256!,
              dev: String(before.dev),
              ino: String(before.ino),
              size: String(before.size),
              mtimeNs: String(before.mtimeNs),
              ctimeNs: String(before.ctimeNs),
            };
          if (qualification) {
            const selectedWitness = qualification.sources.find(
              (value) => value.sourceId === row.id && value.holdId === hold.id,
            );
            if (
              !same(witness, selectedWitness) ||
              ['-wal', '-shm', '-journal'].some((suffix) => existsSync(filename + suffix))
            ) {
              fail();
            }
          } else {
            const target = new Database(':memory:');
            let snapshot: Database.Database | undefined;
            try {
              snapshot = openRelationalSnapshot(target, filename, row.binding.sourceSha256!);
              const state = readRelationalRecoveryState(snapshot);
              if (
                state.history_id !== row.binding.historyId ||
                state.last_commit_seq !== row.binding.finalCommitSeq
              ) {
                fail();
              }
            } finally {
              snapshot?.close();
              target.close();
            }
          }
          const after = await regular(filename);
          if (!sameFileIdentity(before, after)) {
            fail();
          }
          qualifiedSources.push(witness);
          await bindEvidence(filename);
          expectedLocal.add(comparablePath(location));
        } else {
          const before = await regular(location),
            witness: SourceQualification = {
              sourceId: row.id,
              holdId: hold.id,
              file: location,
              sourceSha256: row.binding.sourceSha256!,
              dev: String(before.dev),
              ino: String(before.ino),
              size: String(before.size),
              mtimeNs: String(before.mtimeNs),
              ctimeNs: String(before.ctimeNs),
            };
          if (qualification) {
            if (
              !same(
                witness,
                qualification.sources.find(
                  (value) => value.sourceId === row.id && value.holdId === hold.id,
                ),
              )
            ) {
              fail();
            }
          } else if ((await retainedFileSha256(location)) !== row.binding.sourceSha256) {
            fail();
          }
          if (!sameFileIdentity(before, await regular(location))) {
            fail();
          }
          qualifiedSources.push(witness);
          await bindEvidence(location);
          expectedArchive.add(comparablePath(location));
        }
      }
    }
  }
  if (!protectedScope) {
    return null;
  }
  const observed: unknown[] = [];
  for (const [roots, expected] of [
    [scope.localRoots, expectedLocal],
    [scope.archiveRoots, expectedArchive],
  ] as const) {
    const actual = new Set<string>();
    for (const base of roots) {
      if (!path.isAbsolute(base)) {
        fail();
      }
      await directory(base);
      for (const entry of await readdir(base)) {
        const location = path.join(base, entry),
          stat = await lstat(location);
        if (stat.isSymbolicLink()) {
          return null;
        }
        actual.add(comparablePath(location));
      }
    }
    if (actual.size !== expected.size || [...actual].some((location) => !expected.has(location))) {
      return null;
    }
    observed.push([...actual].sort());
  }
  const remote = new Map<string, RetainedObject>();
  for (const namespace of scope.r2Namespaces) {
    const client = configuration.r2;
    if (
      !(client instanceof R2Storage) ||
      namespace !== `r2://${client.config.accountId}/${client.config.bucket}`
    ) {
      return null;
    }
    for (const row of await client.listBackups()) {
      const match = /^backups\/(?:daily|cutover)-[0-9TZ.]+\/([a-f0-9]{64})$/.exec(row.key);
      if (!match) {
        return null;
      }
      const object = {
        namespace,
        bucket: client.config.bucket,
        key: row.key,
        byteLength: row.bytes,
        sha256: match[1],
      };
      if (!validObject(object)) {
        fail();
      }
      const key = namespace + '\n' + row.key;
      if (remote.has(key)) {
        fail();
      }
      remote.set(key, object);
    }
  }
  if (
    remote.size !== expectedRemote.size ||
    [...remote].some(([key, object]) => !same(expectedRemote.get(key), object))
  ) {
    return null;
  }
  observed.push([...remote].sort(([a], [b]) => a.localeCompare(b)));
  const assertEvidence = () => {
    for (const [filename, identity] of evidenceFiles) {
      const current = lstatSync(filename, { bigint: true });
      if (
        !current.isFile() ||
        current.isSymbolicLink() ||
        current.nlink !== 1n ||
        current.dev !== identity.dev ||
        current.ino !== identity.ino ||
        current.size !== identity.size ||
        current.mtimeNs !== identity.mtimeNs ||
        current.ctimeNs !== identity.ctimeNs
      ) {
        fail();
      }
    }
  };
  return {
    dependencies,
    qualifiedSources,
    assertEvidence,
    r2Namespaces: scope.r2Namespaces,
    proofSha256: digest({
      registryId: registry.registryId,
      revision: registry.revision,
      selected,
      observed,
      qualifiedSources,
    }),
  };
}
export async function adoptRetainedInventory(
  root: string,
  selected: RetainedInventorySelection,
  configuration: InventoryConfiguration = {},
) {
  return mutate(root, async (registry, actualRoot) => {
    const proof = await verifiedInventory(actualRoot, registry, selected, configuration);
    if (!proof) {
      fail();
    }
    proof.assertEvidence();
    const rootIdentity = await lstat(actualRoot, { bigint: true }),
      qualification: InventoryQualification = {
        format: 'novelai-retained-qualification-v1',
        registryId: registry.registryId,
        root: { path: actualRoot, dev: String(rootIdentity.dev), ino: String(rootIdentity.ino) },
        selectionSha256: digest(selected),
        sources: proof.qualifiedSources,
      },
      filename = path.join(actualRoot, 'qualification-' + randomUUID() + '.json'),
      text = JSON.stringify(qualification) + '\n';
    await immutableFile(filename, text);
    registry.coverage = {
      selection: structuredClone(selected),
      qualification: { file: filename, sha256: createHash('sha256').update(text).digest('hex') },
    };
    return { registryId: registry.registryId, adopted: true };
  });
}
function validReleaseClaim(claim: RetainedArtifactReleaseClaim) {
  if (
    !claim ||
    typeof claim.artifactId !== 'string' ||
    !claim.artifactId ||
    !hashPattern.test(claim.sha256) ||
    !Number.isSafeInteger(claim.byteLength) ||
    claim.byteLength < 1 ||
    !claim.source ||
    !claim.destination ||
    !['local', 'r2'].includes(claim.source.kind) ||
    claim.source.encoding !== 'identity' ||
    !['r2', 'pikpak'].includes(claim.destination.kind) ||
    claim.destination.encoding !== (claim.destination.kind === 'r2' ? 'identity' : 'br') ||
    [claim.source, claim.destination].some(
      (value) =>
        typeof value.namespace !== 'string' ||
        !value.namespace ||
        typeof value.objectKey !== 'string' ||
        !value.objectKey,
    ) ||
    (claim.source.kind === 'local' &&
      (typeof claim.source.dataDir !== 'string' || !path.isAbsolute(claim.source.dataDir)))
  ) {
    fail();
  }
}
function dependenciesProtect(
  dependencies: RetainedDependency[],
  claim: RetainedArtifactReleaseClaim,
) {
  const claimRoot =
    claim.source.kind === 'local' ? sourceRootNamespace(claim.source.dataDir!) : null;
  for (const row of dependencies) {
    if (row.kind === 'prepared-domain') {
      if (claim.source.kind === 'local' && sourceRootNamespace(row.dataDir) === claimRoot) {
        return true;
      }
      continue;
    }
    if (
      claim.source.kind === 'local' &&
      row.kind === 'artifact' &&
      !row.replicas.some(
        (entry) => entry.target.kind === 'local' && entry.replica.deletedAt === null,
      )
    ) {
      continue;
    }
    if (claim.source.kind === 'local' && !row.dataDir) {
      return true;
    }
    const sourceRoot = claim.source.kind === 'local' ? sourceRootNamespace(row.dataDir!) : null;
    if (row.kind === 'prepared-result') {
      if (
        claim.source.kind === 'local' &&
        sourceRoot === claimRoot &&
        row.descriptor.file === 'images/' + claim.source.objectKey
      ) {
        return true;
      }
      continue;
    }
    const key = (value: string) => (sourceRoot?.startsWith('win32:') ? value.toLowerCase() : value);
    const sources = row.replicas.filter(
      (entry) =>
        entry.replica.deletedAt === null &&
        entry.target.kind === claim.source.kind &&
        key(entry.replica.objectKey) === key(claim.source.objectKey) &&
        (claim.source.kind === 'local'
          ? sourceRoot === claimRoot
          : entry.target.namespace === claim.source.namespace),
    );
    if (!sources.length) {
      continue;
    }
    if (
      sources.some(
        (entry) =>
          entry.target.namespace !== claim.source.namespace ||
          entry.replica.encoding !== claim.source.encoding,
      ) ||
      row.descriptor.sha256 !== claim.sha256 ||
      row.descriptor.byteLength !== claim.byteLength ||
      !row.replicas.some(
        (entry) =>
          entry.target.kind === claim.destination.kind &&
          entry.target.namespace === claim.destination.namespace &&
          entry.replica.objectKey === claim.destination.objectKey &&
          entry.replica.encoding === claim.destination.encoding &&
          entry.replica.deletedAt === null &&
          (entry.target.kind === 'pikpak'
            ? entry.replica.uploadedAt !== null
            : entry.replica.verifiedAt !== null),
      )
    ) {
      return true;
    }
  }
  return false;
}
export async function withRetainedArtifactRelease<T>(
  root: string,
  claim: RetainedArtifactReleaseClaim,
  run: (permit: RetainedArtifactReleasePermit) => Promise<T>,
  configuration: InventoryConfiguration = {},
): Promise<T | null> {
  validReleaseClaim(claim);
  return mutate(root, async (registry, actualRoot, guard) => {
    if (registry.coverage === 'unknown') {
      return null;
    }
    const qualificationBytes = await bytes(registry.coverage.qualification.file);
    if (
      createHash('sha256').update(qualificationBytes).digest('hex') !==
      registry.coverage.qualification.sha256
    ) {
      fail();
    }
    const qualification = JSON.parse(qualificationBytes.toString('utf8')) as InventoryQualification,
      actualIdentity = await lstat(actualRoot, { bigint: true });
    if (
      qualification.format !== 'novelai-retained-qualification-v1' ||
      qualification.registryId !== registry.registryId ||
      !same(qualification.root, {
        path: actualRoot,
        dev: String(actualIdentity.dev),
        ino: String(actualIdentity.ino),
      }) ||
      qualification.selectionSha256 !== digest(registry.coverage.selection) ||
      !Array.isArray(qualification.sources)
    ) {
      fail();
    }
    const proof = await verifiedInventory(
      actualRoot,
      registry,
      registry.coverage.selection,
      configuration,
      qualification,
    );
    if (!proof || dependenciesProtect(proof.dependencies, claim)) {
      return null;
    }
    guard.assertCurrent();
    const permit = Object.freeze({}) as RetainedArtifactReleasePermit;
    const qualificationIdentity = await regular(registry.coverage.qualification.file);
    const assertEvidence = () => {
      proof.assertEvidence();
      const actual = lstatSync(
        registry.coverage === 'unknown' ? '' : registry.coverage.qualification.file,
        { bigint: true },
      );
      if (
        !actual.isFile() ||
        actual.isSymbolicLink() ||
        !sameFileIdentity(actual, qualificationIdentity)
      ) {
        fail();
      }
    };
    releasePermits.set(permit, {
      guard: {
        active: true,
        assertCurrent() {
          guard.assertCurrent();
          assertEvidence();
        },
      },
      claim: structuredClone(claim),
      evidence: {
        registryId: registry.registryId,
        revision: registry.revision,
        proofSha256: proof.proofSha256,
      },
    });
    try {
      const result = await run(permit);
      assertEvidence();
      return result;
    } finally {
      releasePermits.delete(permit);
    }
  });
}
export function assertRetainedArtifactReleasePermit(
  permit: RetainedArtifactReleasePermit,
  claim: RetainedArtifactReleaseClaim,
) {
  const proof = releasePermits.get(permit);
  if (!proof || !proof.guard.active || !same(proof.claim, claim)) {
    fail();
  }
  proof.guard.assertCurrent();
  return { ...proof.evidence };
}
function source(registry: Registry, id: string) {
  const row = registry.sources.find((s) => s.id === id);
  if (!row) {
    fail();
  }
  return row;
}
export async function beginRetainedCapture(
  root: string,
  category: Source['category'],
  location: string,
) {
  if (!['daily', 'cutover', 'archive'].includes(category) || !path.isAbsolute(location)) {
    fail();
  }
  return mutate(root, async (registry) => {
    const id = randomUUID(),
      holdId = randomUUID();
    registry.sources.push({
      id,
      category,
      origin: location,
      binding: null,
      holds: [
        {
          id: holdId,
          kind: category === 'archive' ? 'archive' : 'local',
          state: 'preparing',
          location,
        },
      ],
    });
    return { sourceId: id, holdId, registryId: registry.registryId };
  });
}
export async function bindRetainedCapture(
  root: string,
  id: string,
  manifestFile: string,
  binding: RetainedBinding,
  dependenciesFile?: string,
  finalLocation?: string,
  resolvedFile?: string,
) {
  return mutate(root, async (registry, selected) => {
    const row = source(registry, id);
    if (row.binding) {
      const original = row.binding,
        base = { ...binding, dependencies: 'unknown' as const };
      delete base.resolvedDependenciesSha256;
      const refinement =
        original.format === 'legacy-export' &&
        original.dependencies === 'unknown' &&
        !original.resolvedDependenciesSha256 &&
        binding.dependencies === 'known' &&
        !!binding.resolvedDependenciesSha256 &&
        same(original, base);
      if (
        (!same(original, binding) && !refinement) ||
        (finalLocation && row.holds[0].location !== finalLocation)
      ) {
        fail();
      }
      const stored = path.join(selected, 'sources', id);
      await directory(stored);
      if (
        (await retainedFileSha256(path.join(stored, 'manifest.json'))) !== binding.manifestSha256 ||
        (binding.dependenciesSha256 &&
          (await retainedFileSha256(path.join(stored, 'dependencies.jsonl'))) !==
            binding.dependenciesSha256)
      ) {
        fail();
      }
      if (refinement) {
        if (
          !resolvedFile ||
          (await retainedFileSha256(resolvedFile)) !== binding.resolvedDependenciesSha256
        ) {
          fail();
        }
        await immutableCopy(
          resolvedFile,
          path.join(stored, 'resolved-dependencies.jsonl'),
          binding.resolvedDependenciesSha256!,
        );
      }
      await boundLegacyExport(stored, binding);
      if (refinement) {
        row.binding = binding;
      }
      return { sourceId: id };
    }
    if (row.holds.some((h) => h.state !== 'preparing')) {
      fail();
    }
    if ((await retainedFileSha256(manifestFile)) !== binding.manifestSha256) {
      fail();
    }
    if (
      binding.dependenciesSha256 &&
      (!dependenciesFile ||
        (await retainedFileSha256(dependenciesFile)) !== binding.dependenciesSha256)
    ) {
      fail();
    }
    if (
      binding.resolvedDependenciesSha256 &&
      (!resolvedFile ||
        (await retainedFileSha256(resolvedFile)) !== binding.resolvedDependenciesSha256)
    ) {
      fail();
    }
    const target = path.join(selected, 'sources', id);
    await directory(path.join(selected, 'sources'));
    await mkdir(target, { mode: 0o700, recursive: true });
    await directory(target);
    await immutableCopy(manifestFile, path.join(target, 'manifest.json'), binding.manifestSha256!);
    if (dependenciesFile && binding.dependenciesSha256) {
      await immutableCopy(
        dependenciesFile,
        path.join(target, 'dependencies.jsonl'),
        binding.dependenciesSha256,
      );
    }
    if (resolvedFile && binding.resolvedDependenciesSha256) {
      await immutableCopy(
        resolvedFile,
        path.join(target, 'resolved-dependencies.jsonl'),
        binding.resolvedDependenciesSha256,
      );
    }
    await boundLegacyExport(target, binding);
    row.binding = binding;
    if (finalLocation) {
      if (
        row.category === 'archive' ||
        !path.isAbsolute(finalLocation) ||
        path.dirname(finalLocation) !== path.dirname(row.origin)
      ) {
        fail();
      }
      row.holds[0].location = finalLocation;
    }
    return { sourceId: id };
  });
}
export async function publishRetainedLocal(root: string, id: string, location: string) {
  return mutate(root, async (registry) => {
    const row = source(registry, id),
      hold = row.holds.find((h) => h.kind !== 'remote');
    if (
      !row.binding?.manifestSha256 ||
      !hold ||
      !['preparing', 'retained'].includes(hold.state) ||
      hold.location !== location
    ) {
      fail();
    }
    if (
      (await retainedFileSha256(path.join(location, 'manifest.json'))) !==
      row.binding.manifestSha256
    ) {
      fail();
    }
    if (
      row.binding.sourceSha256 &&
      (await retainedFileSha256(path.join(location, 'data/library.sqlite'))) !==
        row.binding.sourceSha256
    ) {
      fail();
    }
    hold.state = 'retained';
    return { sourceId: id, holdId: hold.id };
  });
}
export async function publishRetainedArchive(
  root: string,
  id: string,
  sourceFile: string,
  manifestFile: string,
) {
  return mutate(root, async (registry, selected) => {
    const row = source(registry, id),
      hold = row.holds[0];
    if (
      row.category !== 'archive' ||
      !row.binding ||
      hold.kind !== 'archive' ||
      hold.state !== 'preparing' ||
      hold.location !== sourceFile ||
      (await retainedFileSha256(sourceFile)) !== row.binding.sourceSha256 ||
      (await retainedFileSha256(manifestFile)) !== row.binding.manifestSha256
    ) {
      fail();
    }
    await boundLegacyExport(path.join(selected, 'sources', id), row.binding);
    hold.state = 'retained';
    return { sourceId: id, holdId: hold.id };
  });
}
export async function beginRetainedRemote(
  root: string,
  object: RetainedObject,
  sourceId?: string,
  archiveSha256?: string,
) {
  if (!validObject(object)) {
    fail();
  }
  return mutate(root, async (registry) => {
    let row: Source;
    if (sourceId) {
      row = source(registry, sourceId);
    } else {
      row = {
        id: randomUUID(),
        category: object.key.startsWith('backups/cutover-') ? 'cutover' : 'daily',
        origin: 'opaque encrypted backup',
        binding: null,
        holds: [],
      };
      registry.sources.push(row);
    }
    if (
      registry.sources.some((s) =>
        s.holds.some(
          (h) =>
            h.kind === 'remote' &&
            h.state !== 'retired' &&
            samePhysicalObject(h.location as RetainedObject, object),
        ),
      )
    ) {
      fail();
    }
    const hold = {
      id: randomUUID(),
      kind: 'remote' as const,
      state: 'preparing' as const,
      location: object,
      producer: {
        pid: process.pid,
        domain: await currentOwnerDomain(),
        nonce: randomUUID(),
        archiveSha256: archiveSha256 ?? null,
        bindingSha256: createHash('sha256').update(JSON.stringify(row.binding)).digest('hex'),
      },
    };
    row.holds.push(hold);
    return { sourceId: row.id, holdId: hold.id };
  });
}
export async function confirmRetainedRemote(
  root: string,
  sourceId: string,
  holdId: string,
  object: RetainedObject,
) {
  return mutate(root, async (registry) => {
    const hold = source(registry, sourceId).holds.find((h) => h.id === holdId);
    if (
      !hold ||
      hold.kind !== 'remote' ||
      !['preparing', 'retained'].includes(hold.state) ||
      !same(hold.location, object)
    ) {
      fail();
    }
    hold.state = 'retained';
  });
}
export async function enrollExistingRemote(root: string, object: RetainedObject) {
  if (!validObject(object)) {
    fail();
  }
  return mutate(root, async (registry) => {
    for (const row of registry.sources) {
      for (const hold of row.holds) {
        if (
          hold.kind === 'remote' &&
          hold.state !== 'retired' &&
          samePhysicalObject(hold.location as RetainedObject, object)
        ) {
          return { sourceId: row.id, holdId: hold.id };
        }
      }
    }
    const id = randomUUID(),
      holdId = randomUUID();
    registry.sources.push({
      id,
      category: object.key.startsWith('backups/cutover-') ? 'cutover' : 'daily',
      origin: 'opaque listed encrypted backup',
      binding: null,
      holds: [{ id: holdId, kind: 'remote', state: 'retained', location: object }],
    });
    return { sourceId: id, holdId };
  });
}
// 原R2 owner完成完整listing与保留七份全GCM后才领取；retiring本身仍保护引用。
export async function beginRetainedRemoteRetirement(
  root: string,
  sourceId: string,
  holdId: string,
  object: RetainedObject,
  proof: { namespace: string; listingSha256: string; retainedSha256: string },
) {
  if (
    !validObject(object) ||
    proof.namespace !== object.namespace ||
    !hashPattern.test(proof.listingSha256) ||
    !hashPattern.test(proof.retainedSha256)
  ) {
    fail();
  }
  return mutate(root, async (registry) => {
    const row = source(registry, sourceId),
      hold = row.holds.find((h) => h.id === holdId);
    if (
      row.category !== 'daily' ||
      !hold ||
      hold.kind !== 'remote' ||
      hold.state === 'retired' ||
      !same(hold.location, object)
    ) {
      fail();
    }
    if (
      registry.sources.some((s) =>
        s.holds.some(
          (h) =>
            h.kind === 'remote' &&
            h.id !== hold.id &&
            h.state !== 'retired' &&
            samePhysicalObject(h.location as RetainedObject, object),
        ),
      )
    ) {
      fail();
    }
    if (hold.state === 'preparing') {
      const producer = hold.producer;
      if (
        !producer ||
        !hashPattern.test(producer.archiveSha256 ?? '') ||
        !idPattern.test(producer.nonce) ||
        producer.bindingSha256 !==
          createHash('sha256').update(JSON.stringify(row.binding)).digest('hex')
      ) {
        fail();
      }
      await assertStoppedOwner(producer);
    }
    hold.state = 'retiring';
    hold.retirementProof = proof;
    return hold;
  });
}
export async function beginRetainedRetirement(
  root: string,
  sourceId: string,
  holdId: string,
  reason: 'daily-rotation' | 'explicit-retirement' = 'daily-rotation',
) {
  return mutate(root, async (registry) => {
    const row = source(registry, sourceId),
      hold = row.holds.find((h) => h.id === holdId);
    if (
      (reason !== 'explicit-retirement' && row.category !== 'daily') ||
      !hold ||
      !['retained', 'retiring'].includes(hold.state)
    ) {
      fail();
    }
    hold.state = 'retiring';
    return hold;
  });
}
export async function inspectRetainedLease(root: string) {
  root = await directory(root);
  const filename = path.join(root, 'lease.json'),
    stat = await regular(filename),
    value = await bytes(filename);
  const lease = JSON.parse(value.toString()),
    registry = await readRegistry(root);
  if (
    !idPattern.test(lease.nonce) ||
    !Number.isSafeInteger(lease.pid) ||
    lease.pid < 1 ||
    lease.registryId !== registry.registryId
  ) {
    fail();
  }
  return {
    nonce: lease.nonce,
    pid: lease.pid,
    registryId: registry.registryId,
    revision: registry.revision,
    sha256: createHash('sha256').update(value).digest('hex'),
    dev: String(stat.dev),
    ino: String(stat.ino),
    domain: lease.domain ?? null,
  };
}
export async function recoverRetainedLease(
  root: string,
  proof: Awaited<ReturnType<typeof inspectRetainedLease>>,
) {
  root = await directory(root);
  const recovery = path.join(root, 'recovery.lock'),
    guard = await open(recovery, 'wx', 0o600);
  try {
    await guard.writeFile(JSON.stringify({ nonce: randomUUID(), pid: process.pid }) + '\n');
    await guard.sync();
    const actual = await inspectRetainedLease(root);
    if (!same(actual, proof)) {
      fail();
    }
    await assertStoppedOwner(actual);
    if (!same(await inspectRetainedLease(root), proof)) {
      fail();
    }
    await unlink(path.join(root, 'lease.json'));
    await syncDirectory(root);
    // 只清已停止的短metadata lease；所有capture/remote preparing责任保持原样。
    return { recovered: true, releaseAllowed: false };
  } finally {
    await guard.close();
    await unlink(recovery);
    await syncDirectory(root);
  }
}
// 只有原实际IO caller在确认对应保留物已消失后调用；不是源字节DELETE能力。
export async function confirmRetainedRetirement(
  root: string,
  sourceId: string,
  holdId: string,
  expected: Hold['location'],
) {
  return mutate(root, async (registry) => {
    const hold = source(registry, sourceId).holds.find((h) => h.id === holdId);
    if (!hold || !['retiring', 'retired'].includes(hold.state) || !same(hold.location, expected)) {
      fail();
    }
    if (hold.kind !== 'remote' && !(await absent(hold.location as string))) {
      fail();
    }
    hold.state = 'retired';
  });
}
export async function checkpointRetainedSources(root: string, filename: string) {
  return mutate(root, async (registry, selected) => {
    await directory(path.dirname(filename));
    await mkdir(filename, { mode: 0o700 });
    await mkdir(path.join(filename, 'sources'), { mode: 0o700 });
    for (const row of registry.sources) {
      if (!row.holds.some((hold) => hold.state !== 'retired') || !row.binding?.manifestSha256) {
        continue;
      }
      const from = path.join(selected, 'sources', row.id),
        to = path.join(filename, 'sources', row.id);
      await directory(from);
      await boundLegacyExport(from, row.binding);
      await mkdir(to, { mode: 0o700 });
      await immutableCopy(
        path.join(from, 'manifest.json'),
        path.join(to, 'manifest.json'),
        row.binding.manifestSha256,
      );
      if (row.binding.dependenciesSha256) {
        await immutableCopy(
          path.join(from, 'dependencies.jsonl'),
          path.join(to, 'dependencies.jsonl'),
          row.binding.dependenciesSha256,
        );
      }
      if (row.binding.resolvedDependenciesSha256) {
        await immutableCopy(
          path.join(from, 'resolved-dependencies.jsonl'),
          path.join(to, 'resolved-dependencies.jsonl'),
          row.binding.resolvedDependenciesSha256,
        );
      }
    }
    await immutableFile(
      path.join(filename, 'registry.json'),
      JSON.stringify({ ...registry, coverage: 'unknown' }, null, 2) + '\n',
    );
    const sha256 = await retainedFileSha256(path.join(filename, 'registry.json'));
    await immutableFile(
      path.join(filename, 'checkpoint.json'),
      JSON.stringify({
        format: 'novelai-retained-checkpoint-v1',
        registryId: registry.registryId,
        sha256,
      }) + '\n',
    );
    return { sha256 };
  });
}
export async function restoreRetainedSources(
  root: string,
  filename: string,
  expectedSha256: string,
) {
  filename = await directory(filename);
  if (typeof expectedSha256 !== 'string' || !hashPattern.test(expectedSha256)) {
    fail();
  }
  const restored = await readProtectedRetainedRegistry(filename, expectedSha256);
  // 部署资格含原live root/文件身份，不能随备份复制到另一个lease域继续授权。
  restored.coverage = 'unknown';
  // checkpoint 的外部 SHA 来自已验证包；恢复保留责任，不把目录缺失解释成空保护域。
  await directory(path.dirname(path.resolve(root)));
  await mkdir(path.resolve(root), { mode: 0o700 });
  root = await directory(root);
  await mkdir(path.join(root, 'sources'), { mode: 0o700 });
  for (const row of restored.sources) {
    if (!row.holds.some((hold) => hold.state !== 'retired') || !row.binding?.manifestSha256) {
      continue;
    }
    const from = path.join(filename, 'sources', row.id),
      to = path.join(root, 'sources', row.id);
    await directory(from);
    await mkdir(to, { mode: 0o700 });
    await immutableCopy(
      path.join(from, 'manifest.json'),
      path.join(to, 'manifest.json'),
      row.binding.manifestSha256,
    );
    if (row.binding.dependenciesSha256) {
      await immutableCopy(
        path.join(from, 'dependencies.jsonl'),
        path.join(to, 'dependencies.jsonl'),
        row.binding.dependenciesSha256,
      );
    }
    if (row.binding.resolvedDependenciesSha256) {
      await immutableCopy(
        path.join(from, 'resolved-dependencies.jsonl'),
        path.join(to, 'resolved-dependencies.jsonl'),
        row.binding.resolvedDependenciesSha256,
      );
    }
  }
  await immutableFile(path.join(root, 'registry.json'), JSON.stringify(restored, null, 2) + '\n');
  await syncDirectory(root);
  return { registryId: restored.registryId, releaseAllowed: false };
}
