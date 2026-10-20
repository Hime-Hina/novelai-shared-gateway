import { createHash } from 'node:crypto';
import { lstat, open, unlink } from 'node:fs/promises';
import path from 'node:path';
import { R2Storage, R2_IMAGE_LIMIT_BYTES, MAX_STORED_IMAGE_BYTES, storedImagePath } from './r2-storage.ts';
import type { JsonStore, StoreItem } from './store.ts';

export async function archiveColdImage(store: JsonStore, remote: R2Storage, idle = () => true, now = Date.now()) {
  const candidate = await store.update(() => idle() ? store.nextColdImage(now) : null, { persist: false });
  if (!candidate) return false;
  try {
    const file = storedImagePath(store.dataDir, candidate.file!);
    if (path.dirname(file) !== path.resolve(store.dataDir, 'images')) throw new Error('Unsafe image path');
    const directory = await lstat(path.join(store.dataDir, 'images'));
    if (!directory.isDirectory() || directory.isSymbolicLink()) throw new Error('Unsafe image directory');
    const info = await lstat(file).catch(error => {
      if (error.code === 'ENOENT' && candidate.r2?.verifiedAt) return null;
      throw error;
    });
    if (info && (!info.isFile() || info.isSymbolicLink() || info.size < 1 || info.size > MAX_STORED_IMAGE_BYTES)) throw new Error('Invalid local image file');
    const handle = info ? await open(file, 'r') : null;
    let buffer: Buffer;
    try { buffer = handle ? await handle.readFile() : await remote.get(candidate.r2!); }
    finally { await handle?.close(); }
    const sha256 = createHash('sha256').update(buffer).digest('hex');
    if (candidate.sha256 !== undefined && candidate.sha256 !== sha256
      || candidate.byteLength !== undefined && candidate.byteLength !== buffer.length) throw new Error('Local image checksum mismatch');
    if (!/^[A-Za-z0-9_-]{1,128}$/.test(candidate.id)) throw new Error('Invalid image identity');
    const reserved = await store.update(() => {
      const current = store.loadItemForUpdate('images', candidate.id);
      if (!current || current.file !== candidate.file || !idle()) return null;
      if (!current.r2) {
        if (store.readR2Usage().bytes + buffer.length > R2_IMAGE_LIMIT_BYTES) return null;
        current.r2 = { bucket: remote.config.bucket, key: `images/${current.id}/${sha256}`, sha256, byteLength: buffer.length };
      }
      if (current.r2.sha256 !== sha256 || current.r2.byteLength !== buffer.length) throw new Error('R2 reservation checksum mismatch');
      current.sha256 = sha256; current.byteLength = buffer.length;
      current.r2LastAttemptAt = new Date(now).toISOString(); delete current.r2Error;
      return current.r2;
    }, { immediate: true, dirtyRows: { images: [candidate.id] }, shouldPersist: Boolean });
    if (!reserved) return false;
    if (!reserved.verifiedAt) await remote.put(reserved, buffer, candidate.mimeType!);
    const downloaded = await remote.get(reserved);
    if (!downloaded.equals(buffer)) throw new Error('R2 round-trip mismatch');
    await store.update(() => {
      const current = store.loadItemForUpdate('images', candidate.id);
      if (!current?.r2 || current.r2.key !== reserved.key) throw new Error('R2 reservation changed');
      current.r2.verifiedAt = new Date().toISOString();
    }, { immediate: true, dirtyRows: { images: [candidate.id] } });
    // Only release an identical local copy after the remote bytes and durable receipt are verified.
    if (info) {
      const current = await lstat(file);
      if (current.ino !== info.ino || current.size !== info.size || current.mtimeMs !== info.mtimeMs) throw new Error('Local image changed during archival');
      await unlink(file);
      if (process.platform !== 'win32') {
        const directoryHandle = await open(path.dirname(file), 'r');
        try { await directoryHandle.sync(); } finally { await directoryHandle.close(); }
      }
    }
    await store.update(() => {
      const current = store.loadItemForUpdate('images', candidate.id);
      if (current?.r2?.key === reserved.key) { current.r2.localRemovedAt = new Date().toISOString(); delete current.r2Error; }
    }, { immediate: true, dirtyRows: { images: [candidate.id] } });
    return true;
  } catch (error) {
    await store.update(() => {
      const current: StoreItem | null = store.loadItemForUpdate('images', candidate.id);
      if (current) { current.r2LastAttemptAt = new Date(now).toISOString(); current.r2Error = '转存校验或写入失败，保留现有副本并稍后重试'; }
    }, { immediate: true, dirtyRows: { images: [candidate.id] } });
    throw error;
  }
}
