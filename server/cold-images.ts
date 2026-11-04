import { createHash } from 'node:crypto';
import { lstat, open, unlink } from 'node:fs/promises';
import path from 'node:path';
import { R2Storage, MAX_STORED_IMAGE_BYTES, storedImagePath } from './r2-storage.ts';
import type { JsonStore, StoreItem } from './store.ts';
import type { PikPakStorage } from './pikpak-storage.ts';

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

export async function archivePikPakImage(store: JsonStore, remote: R2Storage, pikpak: PikPakStorage, idle = () => true, now = Date.now()) {
  const candidate = await store.update(() => idle() ? store.nextPikPakImage(now) : null, { persist: false });
  if (!candidate) return false;
  try {
    const object = candidate.r2!;
    if (object.pikpak && object.pikpak.url !== pikpak.config.url) throw new Error('PikPak archive directory changed');
    if (!object.pikpak?.uploadedAt) {
      const buffer = await remote.get(object);
      await pikpak.put(object, buffer);
    }
    const uploaded = await store.update(() => {
      const current = store.loadItemForUpdate('images', candidate.id);
      if (!current?.r2 || current.r2.key !== object.key || current.r2.removedAt) throw new Error('R2 image changed during PikPak archival');
      current.r2.pikpak = { url: pikpak.config.url, uploadedAt: object.pikpak?.uploadedAt ?? new Date(now).toISOString() };
      current.pikpakLastAttemptAt = new Date(now).toISOString(); delete current.pikpakError;
      return current.r2;
    }, { immediate: true, dirtyRows: { images: [candidate.id] } });
    if (!idle()) return true;
    await remote.removeImage(uploaded);
    await store.update(() => {
      const current = store.loadItemForUpdate('images', candidate.id);
      if (!current?.r2 || current.r2.key !== uploaded.key) throw new Error('R2 image changed during removal');
      current.r2.removedAt = new Date(now).toISOString(); delete current.pikpakError;
    }, { immediate: true, dirtyRows: { images: [candidate.id] } });
    return true;
  } catch (error) {
    await store.update(() => {
      const current = store.loadItemForUpdate('images', candidate.id);
      if (current) { current.pikpakLastAttemptAt = new Date(now).toISOString(); current.pikpakError = 'PikPak 转存或删除源副本失败，保留已有副本并稍后重试'; }
    }, { immediate: true, dirtyRows: { images: [candidate.id] } });
    throw error;
  }
}
