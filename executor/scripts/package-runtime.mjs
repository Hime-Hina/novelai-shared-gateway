import { cp, mkdir, realpath, rm } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const executorDir = fileURLToPath(new URL('../', import.meta.url));
const rootDir = path.resolve(executorDir, '..');
const outputDir = path.join(executorDir, '.output', 'server');
const packageDir = path.join(outputDir, 'node_modules', 'novelai-shared-gateway');

const physicalOutput = path.join(await realpath(executorDir), '.output', 'server');
if (path.relative(physicalOutput, await realpath(outputDir)) !== '') {
  throw new Error('Linked or outside generated executor directory.');
}
await mkdir(path.dirname(packageDir), { recursive: true });
if (
  path.relative(
    path.join(physicalOutput, 'node_modules'),
    await realpath(path.dirname(packageDir)),
  ) !== ''
) {
  throw new Error('Linked or outside generated executor package directory.');
}
// Rebuilding must not leave an older normal package's scripts or legacy assets in the release.
await rm(packageDir, { recursive: true, force: true });
await mkdir(packageDir, { recursive: true });
for (const name of ['LICENSE', 'NOTICE.md', 'UPSTREAM-LICENSE']) {
  await cp(path.join(rootDir, name), path.join(packageDir, name), { recursive: true });
}
await cp(
  path.join(rootDir, '.output', 'backend', 'package.json'),
  path.join(packageDir, 'package.json'),
);
await cp(path.join(rootDir, '.output', 'backend', 'server'), path.join(packageDir, 'server'), {
  recursive: true,
});
await cp(path.join(rootDir, '.output', 'backend', 'scripts'), path.join(packageDir, 'scripts'), {
  recursive: true,
});
await cp(path.join(executorDir, 'start.mjs'), path.join(outputDir, 'start.mjs'));
const licenseDir = path.join(outputDir, 'licenses');
await mkdir(licenseDir, { recursive: true });
for (const name of ['nitropack', 'h3']) {
  await cp(
    path.join(executorDir, 'node_modules', name, 'LICENSE'),
    path.join(licenseDir, `${name}.LICENSE`),
  );
}
