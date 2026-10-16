import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile, stat, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { deflateSync, crc32 } from 'node:zlib';
import { describeStoredResult, publishStoredResult } from '../../server/result-files.js';

const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
const image = { buffer: png, mimeType: 'image/png' };
const invalid = { code: 'ERR_STORED_RESULT_INVALID' };
async function fixture() {
  const directory = await mkdtemp(path.join(tmpdir(), 'nai-result-files-'));
  await mkdir(path.join(directory, 'images'));
  return directory;
}
function grayscalePng(width, height) {
  const chunk = (type, data) => {
    const bytes = Buffer.alloc(data.length + 12);
    bytes.writeUInt32BE(data.length); bytes.write(type, 4, 4, 'ascii'); data.copy(bytes, 8);
    bytes.writeUInt32BE(crc32(bytes.subarray(4, -4)), bytes.length - 4); return bytes;
  };
  const header = Buffer.alloc(13); header.writeUInt32BE(width); header.writeUInt32BE(height, 4); header[8] = 8;
  return Buffer.concat([Buffer.from('89504e470d0a1a0a', 'hex'), chunk('IHDR', header),
    chunk('IDAT', deflateSync(Buffer.alloc((width + 1) * height))), chunk('IEND', Buffer.alloc(0))]);
}

test('descriptor 仅包含规范路径、按操作验证的媒体类型、长度和 SHA256', () => {
  assert.deepEqual(describeStoredResult('img_result', 'generate', image), {
    imageId: 'img_result', file: 'images/img_result.png', operation: 'generate', mimeType: 'image/png', byteLength: 68,
    sha256: '63ef318d96b5d0d0ceba6e04a4e622b1158335cdc67c49e27839132c6f655058'
  });
  for (const id of ['../escape', 'a/b', 'a\\b', 'C:escape', '', '.', 'NUL', 'COM1']) {
    assert.throws(() => describeStoredResult(id, 'generate', image), invalid);
  }
  for (const operation of ['img2img', 'unknown', undefined]) assert.throws(() => describeStoredResult('img_result', operation, image), invalid);
});

test('新结果用 0600 临时文件发布，原 buffer 复用，重复发布和最终文件恢复不重写内容', async () => {
  const directory = await fixture(), pending = describeStoredResult('img_result', 'generate', image);
  const file = path.join(directory, pending.file), partial = `${file}.part`;
  const published = await publishStoredResult(directory, { ...pending, attemptId: 'attempt_result' }, png);
  assert.equal(published.buffer, png); assert.equal(published.mimeType, 'image/png');
  assert.deepEqual(await readFile(file), png); await assert.rejects(stat(partial), { code: 'ENOENT' });
  const first = await stat(file);
  if (process.platform !== 'win32') assert.equal(first.mode & 0o777, 0o600);
  assert.equal((await publishStoredResult(directory, pending, png)).buffer, png);
  assert.deepEqual(await publishStoredResult(directory, pending), image);
  const last = await stat(file); assert.equal(last.mtimeMs, first.mtimeMs); assert.equal(last.ino, first.ino);
});

test('完整 part 在恢复时验证后发布；没有 part 或 final 才返回 null', async () => {
  const directory = await fixture(), pending = describeStoredResult('img_recover', 'upscale', image);
  assert.equal(await publishStoredResult(directory, pending), null);
  const file = path.join(directory, pending.file), partial = `${file}.part`;
  await writeFile(partial, png, { mode: 0o600 });
  assert.deepEqual(await publishStoredResult(directory, pending), image);
  assert.deepEqual(await readFile(file), png); await assert.rejects(stat(partial), { code: 'ENOENT' });
});

test('损坏或截断的 part/final 保留原样，不能被新结果覆盖或当作缺失', async () => {
  const directory = await fixture(), pending = describeStoredResult('img_partial', 'generate', image);
  const file = path.join(directory, pending.file), partial = `${file}.part`;
  const truncated = png.subarray(0, 20);
  await writeFile(partial, truncated);
  for (const buffer of [undefined, png]) {
    await assert.rejects(publishStoredResult(directory, pending, buffer), invalid);
    assert.deepEqual(await readFile(partial), truncated); await assert.rejects(stat(file), { code: 'ENOENT' });
  }
  // 即使另一份副本完整，也保留并报告冲突证据。
  await writeFile(file, png);
  await assert.rejects(publishStoredResult(directory, pending), invalid);
  assert.deepEqual(await readFile(file), png); assert.deepEqual(await readFile(partial), truncated);
  const changed = Buffer.from(png); changed[40] ^= 1;
  await writeFile(file, changed); await writeFile(partial, png);
  await assert.rejects(publishStoredResult(directory, pending, png), invalid);
  assert.deepEqual(await readFile(file), changed); assert.deepEqual(await readFile(partial), png);
});

test('descriptor 和输入 buffer 不匹配时不写文件，拒绝越界路径和替换媒体类型', async () => {
  const directory = await fixture(), pending = describeStoredResult('img_checked', 'generate', image);
  for (const changed of [
    { file: '../img_checked.png' }, { file: 'images/../img_checked.png' }, { file: 'images\\img_checked.png' },
    { file: path.resolve(directory, pending.file) }, { imageId: 'another' }, { byteLength: 0 },
    { byteLength: 64 * 1024 * 1024 + 1 }, { sha256: 'not-a-hash' }, { sha256: '0'.repeat(64) },
    { mimeType: 'image/jpeg', file: 'images/img_checked.jpg' }
  ]) await assert.rejects(publishStoredResult(directory, { ...pending, ...changed }, png), invalid);
  await assert.rejects(publishStoredResult(directory, pending, png.subarray(0, 10)), invalid);
  await assert.rejects(publishStoredResult(directory, pending, null), invalid);
  await assert.rejects(stat(path.join(directory, pending.file)), { code: 'ENOENT' });
  await assert.rejects(stat(path.join(directory, pending.file + '.part')), { code: 'ENOENT' });
});

test('图片复用现有完整性检查及 16384 边长、16M 像素和 64MiB 字节上限', () => {
  const wide = grayscalePng(16384, 256);
  assert.equal(describeStoredResult('img_upscaled', 'upscale', { buffer: wide, mimeType: 'image/png' }).byteLength, wide.length);
  for (const buffer of [Buffer.from('<html>upstream error</html>'), png.subarray(0, -1), grayscalePng(16385, 1), grayscalePng(16384, 1025)]) {
    assert.throws(() => describeStoredResult('img_bad', 'generate', { buffer, mimeType: 'image/png' }), invalid);
  }
  assert.throws(() => describeStoredResult('img_bad', 'generate', { buffer: png, mimeType: 'image/jpeg' }), invalid);
  assert.throws(() => describeStoredResult('img_svg', 'generate', { buffer: Buffer.from('<svg/>'), mimeType: 'image/svg+xml' }), invalid);
  assert.throws(() => describeStoredResult('img_large', 'encode-vibe', { buffer: Buffer.alloc(64 * 1024 * 1024 + 1), mimeType: 'application/octet-stream' }), invalid);
});

test('encode-vibe 两种 binary MIME 按不透明非空数据保存和恢复，仍验证长度与哈希', async () => {
  const directory = await fixture(), bytes = Buffer.from([0, 255, 19, 42]);
  for (const [index, mimeType] of ['application/octet-stream', 'application/binary'].entries()) {
    const pending = describeStoredResult(`img_vibe_${index}`, 'encode-vibe', { buffer: bytes, mimeType });
    assert.equal(pending.file, `images/img_vibe_${index}.bin`);
    assert.equal((await publishStoredResult(directory, pending, bytes)).buffer, bytes);
    assert.deepEqual(await publishStoredResult(directory, pending), { buffer: bytes, mimeType });
    await writeFile(path.join(directory, pending.file), Buffer.from([0, 255, 19, 43]));
    await assert.rejects(publishStoredResult(directory, pending), invalid);
    assert.throws(() => describeStoredResult('img_empty', 'encode-vibe', { buffer: Buffer.alloc(0), mimeType }), invalid);
    assert.throws(() => describeStoredResult('img_image', 'generate', { buffer: bytes, mimeType }), invalid);
  }
  assert.throws(() => describeStoredResult('img_vibe', 'encode-vibe', image), invalid);
});

test('拒绝以符号链接绕过 images 目录或结果文件边界', { skip: process.platform === 'win32' }, async () => {
  const directory = await fixture(), outside = await fixture(), pending = describeStoredResult('img_link', 'generate', image);
  const target = path.join(outside, 'external.png'); await writeFile(target, png);
  await symlink(target, path.join(directory, pending.file));
  await assert.rejects(publishStoredResult(directory, pending), invalid);
  assert.deepEqual(await readFile(target), png);
  const linkedRoot = await mkdtemp(path.join(tmpdir(), 'nai-result-files-link-'));
  await symlink(outside, path.join(linkedRoot, 'images'), 'dir');
  await assert.rejects(publishStoredResult(linkedRoot, pending, png), invalid);
});
