// Only the isolated built-executor test loads this hook; no source business modules are imported.
import fs from 'node:fs/promises';
import path from 'node:path';
import { syncBuiltinESMExports } from 'node:module';

if (process.env.RESULT_RECOVERY_TEST_STAGE === 'file_sync') {
  const images = path.resolve(process.env.DATA_DIR, 'images'), originalOpen = fs.open;
  let paused = false;
  fs.open = async function (filename, ...args) {
    const handle = await originalOpen.call(this, filename, ...args);
    if (typeof filename === 'string' && path.dirname(path.resolve(filename)) === images && filename.endsWith('.part')) {
      const sync = handle.sync.bind(handle);
      handle.sync = async () => {
        await sync();
        if (!paused) { paused = true; process.send?.({ point: 'file_sync' }); await new Promise(() => {}); }
      };
    }
    return handle;
  };
  syncBuiltinESMExports();
}
