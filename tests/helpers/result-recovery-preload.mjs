// 仅由隔离 HTTP 测试 --import 加载；生产入口没有故障开关。
import fs from 'node:fs/promises';
import path from 'node:path';
import { syncBuiltinESMExports } from 'node:module';
import { JsonStore } from '../../server/store.js';

const stage = process.env.RESULT_RECOVERY_TEST_STAGE;
const imageDir = path.resolve(process.env.DATA_DIR, 'images');
let paused = false;
async function pause(point, detail = {}) {
  if (paused || stage !== point) return;
  paused = true;
  process.send?.({ point, ...detail });
  await new Promise(() => {});
}
const insideImages = filename => typeof filename === 'string' && path.dirname(path.resolve(filename)) === imageDir;
const originalOpen = fs.open;
fs.open = async function (filename, ...args) {
  const handle = await originalOpen.call(this, filename, ...args);
  if (insideImages(filename) && filename.endsWith('.part')) {
    const writeFile = handle.writeFile.bind(handle);
    handle.writeFile = async (data, ...options) => {
      if (stage === 'half_write' && !paused) {
        const bytes = Buffer.from(data);
        await writeFile(bytes.subarray(0, Math.max(1, Math.floor(bytes.length / 2))), ...options);
        await pause('half_write');
      }
      return writeFile(data, ...options);
    };
    const sync = handle.sync.bind(handle);
    handle.sync = async () => { await sync(); await pause('file_sync'); };
  } else if (typeof filename === 'string' && path.resolve(filename) === imageDir) {
    const sync = handle.sync.bind(handle);
    handle.sync = async () => { await sync(); await pause('dir_sync'); };
  }
  return handle;
};
const originalRename = fs.rename;
fs.rename = async function (from, to) {
  await originalRename.call(this, from, to);
  if (insideImages(from) && from.endsWith('.part')) await pause('rename');
};
syncBuiltinESMExports();

const originalUpdate = JsonStore.prototype.update;
JsonStore.prototype.update = async function (...args) {
  const pendingBefore = (this.db?.jobs || []).filter(job => job.resultPending).map(job => ({ id: job.id, imageId: job.resultPending.imageId }));
  const result = await originalUpdate.apply(this, args);
  const pending = this.db?.jobs?.find(job => job.resultPending);
  if (pending) await pause('descriptor', { jobId: pending.id, sqliteSynchronous: this.sqlite.pragma('synchronous', { simple: true }) });
  const completed = pendingBefore.find(before => this.db.jobs.some(job => job.id === before.id && !job.resultPending && job.imageId === before.imageId));
  if (completed) await pause('final_sql', { jobId: completed.id });
  return result;
};
