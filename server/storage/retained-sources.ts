import { createHash, randomUUID } from 'node:crypto';
import { createReadStream, constants } from 'node:fs';
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
} from 'node:fs/promises';
import path from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import { hostname } from 'node:os';
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
  coverage: 'unknown';
  sources: Source[];
}
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
    value.coverage !== 'unknown' ||
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
        [b.sourceSha256, b.manifestSha256, b.dependenciesSha256].some(
          (h) => h !== null && !hashPattern.test(h),
        ) ||
        (b.dependencies === 'known' &&
          (b.format !== 'relational' ||
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
async function directory(root: string) {
  const selected = path.resolve(root);
  const stat = await lstat(selected);
  const comparable = (p: string) =>
    process.platform === 'win32' ? path.toNamespacedPath(p).toLowerCase() : p;
  if (
    !stat.isDirectory() ||
    stat.isSymbolicLink() ||
    comparable(await realpath(selected)) !== comparable(selected)
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
  return {
    format: identity.format === 'legacy-archive-audit-v1' ? 'legacy-export' : 'opaque',
    sourceSha256,
    manifestSha256: manifest.sha256,
    dependenciesSha256,
    dependencies: 'unknown',
    historyId: null,
    finalCommitSeq: identity.lastCommitSeq ?? null,
  };
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
async function mutate<T>(root: string, change: (registry: Registry, root: string) => Promise<T>) {
  root = await directory(root);
  const rootIdentity = await lstat(root, { bigint: true });
  if (!(await absent(path.join(root, 'recovery.lock')))) {
    fail();
  }
  const initial = await readRegistry(root),
    filename = path.join(root, 'lease.json');
  const lease = await open(filename, 'wx', 0o600);
  const leaseIdentity = await lease.stat({ bigint: true });
  const token = {
    nonce: randomUUID(),
    pid: process.pid,
    registryId: initial.registryId,
    revision: initial.revision,
    domain: await currentOwnerDomain(),
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
    const result = await change(current, root);
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
) {
  return mutate(root, async (registry, selected) => {
    const row = source(registry, id);
    if (row.binding) {
      if (
        !same(row.binding, binding) ||
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
      await boundLegacyExport(stored, binding);
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
    }
    await immutableFile(
      path.join(filename, 'registry.json'),
      JSON.stringify(registry, null, 2) + '\n',
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
  }
  await immutableFile(path.join(root, 'registry.json'), JSON.stringify(restored, null, 2) + '\n');
  await syncDirectory(root);
  return { registryId: restored.registryId, releaseAllowed: false };
}
