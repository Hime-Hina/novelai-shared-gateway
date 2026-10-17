import { cp, mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const executorDir = fileURLToPath(new URL('../', import.meta.url));
const rootDir = path.resolve(executorDir, '..');
const outputDir = path.join(executorDir, '.output', 'server');
const packageDir = path.join(outputDir, 'node_modules', 'novelai-shared-gateway');

await mkdir(packageDir, { recursive: true });
for (const name of ['package.json', 'public', 'scripts', 'LICENSE', 'NOTICE.md', 'UPSTREAM-LICENSE']) {
  await cp(path.join(rootDir, name), path.join(packageDir, name), { recursive: true });
}
await cp(path.join(rootDir, '.output', 'backend', 'server'), path.join(packageDir, 'server'), { recursive: true });
await cp(path.join(rootDir, '.output', 'backend', 'scripts'), path.join(packageDir, 'scripts'), { recursive: true });
await cp(path.join(rootDir, 'server', 'migrations'), path.join(packageDir, 'server', 'migrations'), { recursive: true });
await cp(path.join(executorDir, 'start.mjs'), path.join(outputDir, 'start.mjs'));
const licenseDir = path.join(outputDir, 'licenses');
await mkdir(licenseDir, { recursive: true });
for (const name of ['nitropack', 'h3']) {
  await cp(path.join(executorDir, 'node_modules', name, 'LICENSE'), path.join(licenseDir, `${name}.LICENSE`));
}
