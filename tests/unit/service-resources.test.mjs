import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdir, mkdtemp, rm, statfs, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { readServiceResources } from '../../server/service-resources.ts';

test('资源采样递归统计数据文件，忽略链接，磁盘使用数据目录所在卷，扫描结果缓存一分钟', async t => {
  const root = await mkdtemp(path.join(tmpdir(), 'nai-resources-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const dataDir = path.join(root, 'data'), outside = path.join(root, 'outside');
  await mkdir(path.join(dataDir, 'images'), { recursive: true });
  await mkdir(outside);
  await writeFile(path.join(dataDir, 'library.sqlite'), Buffer.alloc(23));
  await writeFile(path.join(dataDir, 'library.sqlite-wal'), Buffer.alloc(5));
  await writeFile(path.join(dataDir, 'images', 'image.png'), Buffer.alloc(41));
  await writeFile(path.join(outside, 'unrelated'), Buffer.alloc(99));
  await symlink(outside, path.join(dataDir, 'linked'), process.platform === 'win32' ? 'junction' : 'dir');
  t.mock.timers.enable({ apis: ['Date'], now: Date.now() });
  const [first, concurrent] = await Promise.all([readServiceResources(dataDir), readServiceResources(dataDir)]);
  assert.equal(first.data.bytes, 69);
  assert.equal(concurrent.data, first.data);
  assert.ok(first.memory.rssBytes > 0);
  assert.ok(first.memory.heapUsedBytes > 0);
  assert.ok(first.memory.heapTotalBytes >= first.memory.heapUsedBytes);
  assert.ok(first.uptimeSeconds > 0);
  const filesystem = await statfs(dataDir);
  assert.equal(first.disk.totalBytes, filesystem.blocks * filesystem.bsize);
  assert.ok(first.disk.usedBytes >= 0);
  assert.ok(first.disk.availableBytes >= 0 && first.disk.availableBytes <= first.disk.totalBytes);
  assert.ok(!JSON.stringify(first).includes(root));

  await writeFile(path.join(dataDir, 'images', 'image.png'), Buffer.alloc(101));
  assert.equal((await readServiceResources(dataDir)).data.bytes, 69);
  t.mock.timers.tick(60001);
  const refreshed = await readServiceResources(dataDir);
  assert.equal(refreshed.data.bytes, 129);
  assert.notEqual(refreshed.data.checkedAt, first.data.checkedAt);
});

test('空目录为零，目录缺失或不可扫描返回未知，不妨碍内存与运行时间采样', async t => {
  const root = await mkdtemp(path.join(tmpdir(), 'nai-resources-empty-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  assert.equal((await readServiceResources(root)).data.bytes, 0);
  const file = path.join(root, 'file');
  await writeFile(file, 'not a directory');
  for (const directory of [undefined, path.join(root, 'missing'), file]) {
    const result = await readServiceResources(directory);
    assert.equal(result.data, null);
    if (directory !== file) assert.equal(result.disk, null);
    assert.ok(result.memory.rssBytes > 0);
    assert.ok(result.uptimeSeconds > 0);
  }
});
