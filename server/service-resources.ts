import { lstat, opendir, statfs } from 'node:fs/promises';
import path from 'node:path';

type DataUsage = { bytes: number; checkedAt: string };
const dataUsageCache = new Map<string, { expiresAt: number; result: Promise<DataUsage | null> }>();

async function directoryBytes(directory: string): Promise<number> {
  let bytes = 0;
  for await (const entry of await opendir(directory)) {
    const file = path.join(directory, entry.name);
    if (entry.isDirectory()) bytes += await directoryBytes(file);
    else if (entry.isFile()) {
      try {
        const info = await lstat(file);
        if (info.isFile()) bytes += info.size;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      }
    }
  }
  return bytes;
}

function readDataUsage(directory: string): Promise<DataUsage | null> {
  const cached = dataUsageCache.get(directory);
  if (cached && cached.expiresAt > Date.now()) return cached.result;
  // 图片目录可能很大；一分钟内共用一次异步扫描，包括并发管理请求。
  const result = directoryBytes(directory)
    .then(bytes => ({ bytes, checkedAt: new Date().toISOString() }))
    .catch(() => null);
  const entry = { expiresAt: Infinity, result };
  dataUsageCache.set(directory, entry);
  void result.then(() => { entry.expiresAt = Date.now() + 60000; });
  return result;
}

export async function readServiceResources(dataDir: string | undefined) {
  const [data, disk] = dataDir ? await Promise.all([
    readDataUsage(dataDir),
    statfs(dataDir).then(info => ({
      totalBytes: info.blocks * info.bsize,
      usedBytes: (info.blocks - info.bfree) * info.bsize,
      availableBytes: info.bavail * info.bsize
    })).catch(() => null)
  ]) : [null, null];
  const memory = process.memoryUsage();
  return {
    memory: { rssBytes: memory.rss, heapUsedBytes: memory.heapUsed, heapTotalBytes: memory.heapTotal },
    uptimeSeconds: process.uptime(),
    data,
    disk
  };
}
