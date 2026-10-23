import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

test('导入运行时不打开数据库、不监听端口或启动后台任务', async () => {
  const directory = await mkdtemp(path.join(tmpdir(), 'nai-runtime-import-'));
  const dataDir = path.join(directory, 'not-initialized');
  const env = Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE']
    .filter(key => process.env[key]).map(key => [key, process.env[key]]));
  const runtimeUrl = new URL('../../server/gateway-runtime.ts', import.meta.url).href;
  const result = spawnSync(process.execPath, ['--input-type=module', '-e',
    `import assert from 'node:assert/strict';
const runtime = await import(${JSON.stringify(runtimeUrl)});
assert.equal(typeof runtime.startGateway, 'function');
assert.equal(typeof runtime.handleGatewayRequest, 'function');`], {
    env: { ...env, ADMIN_TOKEN: 'synthetic-runtime-import-only-12345678', DATA_DIR: dataDir },
    encoding: 'utf8', timeout: 10000, windowsHide: true
  });
  assert.equal(result.error, undefined);
  assert.equal(result.status, 0, result.stderr);
  assert.doesNotMatch(result.stdout, /listening|runtime/);
  assert.equal(existsSync(dataDir), false);
});
