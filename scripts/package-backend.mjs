import { readFile, writeFile } from 'node:fs/promises';

const manifest = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8'));
manifest.main = 'server/index.js';
manifest.scripts.start = 'node server/index.js';
manifest.scripts.dev = 'node --watch server/index.js';
await writeFile(new URL('../.output/backend/package.json', import.meta.url), JSON.stringify(manifest, null, 2) + '\n');
