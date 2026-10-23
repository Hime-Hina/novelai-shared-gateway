import { createHash } from 'node:crypto';
import { lstat, open, rename } from 'node:fs/promises';
import path from 'node:path';
import { validateImageBytes } from './native-image.ts';

const MAX_RESULT_BYTES = 64 * 1024 * 1024;
const extensions = { 'image/png': 'png', 'image/jpeg': 'jpg', 'image/webp': 'webp',
  'application/octet-stream': 'bin', 'application/binary': 'bin' };
type ResultOperation = 'generate' | 'upscale' | 'encode-vibe';
type ResultMime = keyof typeof extensions;
interface StoredResultDescriptor {
  imageId: string;
  file: string;
  operation: ResultOperation;
  mimeType: ResultMime;
  byteLength: number;
  sha256: string;
}
type ResultIdentity = { imageId?: unknown; operation?: unknown; mimeType?: unknown };
type ResultContent = { buffer: Buffer; mimeType: ResultMime };

function invalid(message: string, cause?: unknown): Error & { code: string } {
  return Object.assign(new Error(`Stored result ${message}`, cause ? { cause } : undefined), { code: 'ERR_STORED_RESULT_INVALID' });
}
function resultFile({ imageId, operation, mimeType }: ResultIdentity): string {
  if (typeof imageId !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(imageId) || /^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])$/i.test(imageId)) throw invalid('imageId is invalid.');
  if (!['generate', 'upscale', 'encode-vibe'].includes(operation as string)) throw invalid('operation is invalid.');
  if (!Object.hasOwn(extensions, mimeType as PropertyKey) || (operation === 'encode-vibe') !== (mimeType as string).startsWith('application/')) throw invalid('MIME type does not match the operation.');
  return `images/${imageId}.${extensions[mimeType as ResultMime]}`;
}
function contentHash(operation: unknown, mimeType: unknown, buffer: unknown): string {
  if (!Buffer.isBuffer(buffer) || !buffer.length || buffer.length > MAX_RESULT_BYTES) throw invalid('byte length is invalid.');
  if (operation !== 'encode-vibe') {
    let image;
    try { image = validateImageBytes(buffer, { maxBytes: MAX_RESULT_BYTES, maxDimension: 16384, label: 'stored result' }); }
    catch (cause) { throw invalid('image is invalid.', cause); }
    if (image.mimeType !== mimeType) throw invalid('MIME type does not match image bytes.');
  }
  return createHash('sha256').update(buffer).digest('hex');
}
function checkDescriptor(value: unknown): asserts value is StoredResultDescriptor {
  const pending = value as Partial<Record<keyof StoredResultDescriptor, unknown>> | null | undefined;
  if (!pending || pending.file !== resultFile(pending) || !Number.isSafeInteger(pending.byteLength) || (pending.byteLength as number) < 1 || (pending.byteLength as number) > MAX_RESULT_BYTES
    || typeof pending.sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(pending.sha256)) throw invalid('descriptor is invalid.');
}
function checkContent(pending: StoredResultDescriptor, buffer: unknown): asserts buffer is Buffer {
  if (!Buffer.isBuffer(buffer) || buffer.length !== pending.byteLength) throw invalid('length does not match the descriptor.');
  if (contentHash(pending.operation, pending.mimeType, buffer) !== pending.sha256) throw invalid('hash does not match the descriptor.');
}

export function describeStoredResult(imageId: unknown, operation: unknown, image: { mimeType?: unknown; buffer?: unknown } | null | undefined): StoredResultDescriptor {
  const mimeType = image?.mimeType, file = resultFile({ imageId, operation, mimeType });
  const sha256 = contentHash(operation, mimeType, image?.buffer);
  // resultFile/contentHash 已验证标识、操作、媒体类型和 Buffer；不重复运行图片校验。
  return { imageId: imageId as string, file, operation: operation as ResultOperation, mimeType: mimeType as ResultMime, byteLength: (image!.buffer as Buffer).length, sha256 };
}

async function readCandidate(file: string, pending: StoredResultDescriptor): Promise<Buffer | null> {
  let stat;
  try { stat = await lstat(file); } catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null; throw error; }
  if (!stat.isFile() || stat.isSymbolicLink()) throw invalid('path must be a regular file.');
  if (stat.size !== pending.byteLength) throw invalid('file length does not match the descriptor.');
  const handle = await open(file, 'r+');
  try {
    const buffer = await handle.readFile();
    checkContent(pending, buffer);
    await handle.sync();
    return buffer;
  } finally { await handle.close(); }
}
async function syncDirectory(directory: string): Promise<void> {
  // Windows 无可移植的目录 fsync；仅 Linux 等支持平台保证此目录项同步步骤。
  if (process.platform === 'win32') return;
  const handle = await open(directory, 'r');
  try { await handle.sync(); } finally { await handle.close(); }
}

// 调用者先创建 images 目录，并按任务串行发布；固定临时路径是 `${pending.file}.part`。
// 不覆盖或删除已有证据；完整 part 可恢复，最终文件已存在时只校验和同步。
export async function publishStoredResult(dataDir: string, pending: unknown, buffer?: unknown): Promise<ResultContent | null> {
  checkDescriptor(pending);
  if (buffer !== undefined) checkContent(pending, buffer);
  const directory = path.resolve(dataDir, 'images'), stat = await lstat(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink()) throw invalid('images directory must not be a symbolic link.');
  const file = path.resolve(dataDir, pending.file), partial = `${file}.part`;
  const existing = await readCandidate(file, pending), staged = await readCandidate(partial, pending);
  if (existing) {
    await syncDirectory(directory);
    return { buffer: buffer ?? existing, mimeType: pending.mimeType };
  }
  if (!staged && buffer === undefined) return null;
  if (!staged) {
    const handle = await open(partial, 'wx', 0o600);
    try { await handle.writeFile(buffer!); await handle.sync(); } finally { await handle.close(); }
  }
  await rename(partial, file);
  await syncDirectory(directory);
  return { buffer: (buffer ?? staged)!, mimeType: pending.mimeType };
}
