import { encode } from '@msgpack/msgpack';

export function imageExtension(mimeType = ''): string {
  if (mimeType === 'application/octet-stream') return 'bin';
  if (mimeType.includes('jpeg') || mimeType.includes('jpg')) return 'jpg';
  if (mimeType.includes('webp')) return 'webp';
  if (mimeType.includes('svg')) return 'svg';
  return 'png';
}

export function nativeImageZip(buffer: Buffer, filename = 'image_0.png'): Buffer {
  return nativeImagesZip([{ buffer, filename }]);
}

// Store the original bytes without compression; partial batches also include their result manifest.
export function nativeImagesZip(entries: { buffer: Buffer; filename: string }[]): Buffer {
  const locals: Buffer[] = [], centrals: Buffer[] = [];
  let offset = 0;
  for (const { buffer, filename } of entries) {
    const name = Buffer.from(filename, 'utf8');
    let crc = 0xffffffff;
    for (const byte of buffer) {
      crc ^= byte;
      for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ ((crc & 1) ? 0xedb88320 : 0);
    }
    crc = (crc ^ 0xffffffff) >>> 0;
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0); local.writeUInt16LE(20, 4);
    local.writeUInt32LE(crc, 14); local.writeUInt32LE(buffer.length, 18); local.writeUInt32LE(buffer.length, 22); local.writeUInt16LE(name.length, 26);
    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50, 0); central.writeUInt16LE(20, 4); central.writeUInt16LE(20, 6);
    central.writeUInt32LE(crc, 16); central.writeUInt32LE(buffer.length, 20); central.writeUInt32LE(buffer.length, 24);
    central.writeUInt16LE(name.length, 28); central.writeUInt32LE(offset, 42);
    locals.push(local, name, buffer); centrals.push(central, name);
    offset += local.length + name.length + buffer.length;
  }
  const central = Buffer.concat(centrals), end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0); end.writeUInt16LE(entries.length, 8); end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(central.length, 12); end.writeUInt32LE(offset, 16);
  return Buffer.concat([...locals, central, end]);
}

interface NativeEventInput {
  event_type?: unknown;
  samp_ix?: unknown;
  image?: unknown;
  step_ix?: unknown;
  message?: unknown;
  code?: unknown;
}

type NativeImageEvent =
  | { event_type: 'intermediate'; samp_ix: number; image: Buffer; step_ix: number }
  | { event_type: 'final'; samp_ix: number; image: Buffer }
  | { event_type: 'error'; samp_ix: number; message: string; image?: never; code?: string };

interface NativeJsonImage {
  buffer: unknown;
  index?: number;
  seed?: number;
}

interface NativeBatchSummary {
  requested?: number;
  completed?: number;
  remaining?: number;
  billing?: 'independent_single_images';
  results?: Array<{ index: number; status: 'done'; filename: string } | { index: number; status: 'failed'; message: string }>;
}

// NAI 在 MessagePack 对象外增加四字节大端长度，不能直接串接裸对象。
const MAX_FRAME_BYTES = 64 * 1024 * 1024;

function nativeImageEvent({ event_type, samp_ix, image, step_ix, message }: NativeEventInput): NativeImageEvent {
  if (typeof samp_ix !== 'number' || !Number.isInteger(samp_ix) || samp_ix < 0 || samp_ix > 3) throw new RangeError('原生图片样本编号无效');
  if (event_type === 'error') {
    return { event_type, samp_ix, message: String(message || '生成失败').slice(0, 2000) };
  }
  if (event_type !== 'intermediate' && event_type !== 'final') throw new TypeError('原生图片事件类型无效');
  if (!Buffer.isBuffer(image) || !image.length) throw new TypeError('原生图片响应缺少二进制图片');
  if (image.length > MAX_FRAME_BYTES) throw new RangeError('原生图片响应超过单帧限制');
  if (event_type === 'intermediate') {
    if (typeof step_ix !== 'number' || !Number.isSafeInteger(step_ix) || step_ix < 0) throw new RangeError('原生图片采样步骤无效');
    return { event_type, samp_ix, image, step_ix };
  }
  return { event_type, samp_ix, image };
}

export function nativeImageMsgpack(input: NativeEventInput): Buffer {
  const event = nativeImageEvent(input), image = event.image;
  // 每帧独立编码并预留图片空间，避免扩容复制或全局 encoder 留住上一张大图。
  const payload = encode(event, image ? { initialBufferSize: image.length + 128 } : undefined);
  if (payload.length > MAX_FRAME_BYTES) throw new RangeError('原生图片响应超过单帧限制');
  const prefix = Buffer.alloc(4);
  prefix.writeUInt32BE(payload.length);
  return Buffer.concat([prefix, payload]);
}

export function* nativeImageSse(input: NativeEventInput): Generator<string, void, unknown> {
  const { image, ...event } = nativeImageEvent(input);
  if (event.event_type === 'error') event.code = String(input.code ?? '500');
  // 所有字段先校验、序列化，不能先输出事件前缀再发现无效入参。
  const metadata = JSON.stringify(event);
  const prefix = `event: ${event.event_type}\ndata: `;
  if (!image) {
    yield `${prefix}${metadata}\n\n`;
    return;
  }
  yield `${prefix}${metadata.slice(0, -1)},"image":"`;
  // 48 KiB 为 3 的倍数：每块至多 64 KiB base64，块间不会出现 padding。
  for (let offset = 0; offset < image.length; offset += 48 * 1024) {
    yield image.subarray(offset, offset + 48 * 1024).toString('base64');
  }
  yield '"}\n\n';
}

export function nativeImagesMsgpack(images: unknown, failure: { index: unknown; message?: unknown } | null = null): Buffer {
  if (!Array.isArray(images) || images.length < 1 || images.length > 4) throw new RangeError('原生图片响应数量无效');
  const frames = images.map((image: unknown, samp_ix: number) => nativeImageMsgpack({ event_type: 'final', samp_ix, image }));
  // 多图逐张结算；前面的成功图保留，后续失败通过 error 帧说明，不能冒充全量成功。
  if (failure) {
    if (failure.index !== images.length) throw new RangeError('原生图片失败样本编号不连续');
    frames.push(nativeImageMsgpack({ event_type: 'error', samp_ix: failure.index, message: failure.message }));
  }
  return Buffer.concat(frames);
}

export async function* nativeImagesJson(images: AsyncIterable<NativeJsonImage> | Iterable<NativeJsonImage>, summary?: NativeBatchSummary | null): AsyncGenerator<string, void, unknown> {
  let first = true;
  for await (const { buffer, index, seed } of images) {
    if (!Buffer.isBuffer(buffer) || !buffer.length) throw new TypeError('原生图片响应缺少二进制图片');
    // 首图读取失败时还没有响应正文，调用方仍可返回正常错误状态。
    yield first ? '{"images":[' : ',';
    first = false;
    yield '{"image":"';
    // 3 的倍数让中间块无 base64 padding，不创建整张图片的 base64 字符串。
    for (let offset = 0; offset < buffer.length; offset += 48 * 1024) {
      yield buffer.subarray(offset, offset + 48 * 1024).toString('base64');
    }
    yield `",${JSON.stringify({ index, seed }).slice(1)}`;
  }
  if (first) throw new RangeError('原生图片响应缺少图片');
  yield ']';
  if (summary) {
    const { requested, completed, remaining, billing, results } = summary;
    const metadata = JSON.stringify({ requested, completed, remaining, billing, results });
    if (metadata !== '{}') yield `,${metadata.slice(1, -1)}`;
  }
  yield '}';
}
