import { createHash } from 'node:crypto';
import { lstat, readFile, readdir, readlink, realpath } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { isDeepStrictEqual } from 'node:util';

const root = fileURLToPath(new URL('../', import.meta.url));
const manifestName = 'MIGRATION_TOOLS.json';

export async function migrationPackageFiles(directory) {
  if (!(await lstat(directory)).isDirectory()) throw new Error('Invalid migration package root.');
  const physicalRoot = await realpath(directory);
  const files = {};
  async function collect(current, prefix = '') {
    for (const name of (await readdir(current)).sort()) {
      const relative = prefix + name;
      if (relative === manifestName) continue;
      const filename = path.join(current, name),
        info = await lstat(filename);
      if (info.isSymbolicLink()) {
        const link = await readlink(filename),
          target = path.relative(physicalRoot, await realpath(filename));
        if (path.isAbsolute(link) || target === '..' || target.startsWith('..' + path.sep)) {
          throw new Error('Migration package contains an external link.');
        }
        files[relative] = { link };
      } else if (info.isDirectory()) {
        await collect(filename, relative + '/');
      } else if (info.isFile()) {
        files[relative] = {
          sha256: createHash('sha256')
            .update(await readFile(filename))
            .digest('hex'),
        };
      } else {
        throw new Error('Migration package contains a special file.');
      }
    }
  }
  await collect(directory);
  return files;
}

export async function verifyMigrationPackage(directory = root) {
  const manifestPath = path.join(directory, manifestName);
  if (!(await lstat(manifestPath)).isFile()) throw new Error('Invalid migration manifest.');
  const manifest = JSON.parse(await readFile(manifestPath, 'utf8'));
  if (
    manifest.format !== 'novelai-migration-tools-v1' ||
    !/^[a-f0-9]{40}$/.test(manifest.sourceCommit ?? '') ||
    !/^[a-f0-9]{40}$/.test(manifest.sourceTree ?? '') ||
    manifest.node !== process.version ||
    manifest.platform !== process.platform ||
    manifest.arch !== process.arch ||
    manifest.abi !== process.versions.modules ||
    !isDeepStrictEqual(await migrationPackageFiles(directory), manifest.files)
  ) {
    throw new Error('Migration package identity or bytes differ from its manifest.');
  }
  return manifest;
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  try {
    if (process.argv.length !== 2) throw new Error('Unexpected arguments.');
    const manifest = await verifyMigrationPackage();
    console.log(JSON.stringify({ verified: true, sourceCommit: manifest.sourceCommit }));
  } catch {
    console.error('MIGRATION_PACKAGE_VERIFICATION_FAILED');
    process.exitCode = 1;
  }
}
