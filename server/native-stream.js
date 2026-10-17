import { encode } from '@msgpack/msgpack';

// NAI 在 MessagePack 对象外增加四字节大端长度，不能直接串接裸对象。
const MAX_FRAME_BYTES = 64 * 1024 * 1024;

export function nativeImageMsgpack({ event_type, samp_ix, image, step_ix, message }) {
  if (!Number.isInteger(samp_ix) || samp_ix < 0 || samp_ix > 3) throw new RangeError('原生图片样本编号无效');
  const event = { event_type, samp_ix };
  if (event_type === 'error') {
    event.message = String(message || '生成失败').slice(0, 2000);
  } else {
    if (!['intermediate', 'final'].includes(event_type)) throw new TypeError('原生图片事件类型无效');
    if (!Buffer.isBuffer(image) || !image.length) throw new TypeError('原生图片响应缺少二进制图片');
    if (image.length > MAX_FRAME_BYTES) throw new RangeError('原生图片响应超过单帧限制');
    event.image = image;
    if (event_type === 'intermediate') {
      if (!Number.isSafeInteger(step_ix) || step_ix < 0) throw new RangeError('原生图片采样步骤无效');
      event.step_ix = step_ix;
    }
  }
  // 每帧独立编码并预留图片空间，避免扩容复制或全局 encoder 留住上一张大图。
  const payload = encode(event, image ? { initialBufferSize: image.length + 128 } : undefined);
  if (payload.length > MAX_FRAME_BYTES) throw new RangeError('原生图片响应超过单帧限制');
  const prefix = Buffer.alloc(4);
  prefix.writeUInt32BE(payload.length);
  return Buffer.concat([prefix, payload]);
}

export function nativeImagesMsgpack(images, failure = null) {
  if (!Array.isArray(images) || images.length < 1 || images.length > 4) throw new RangeError('原生图片响应数量无效');
  const frames = images.map((image, samp_ix) => nativeImageMsgpack({ event_type: 'final', samp_ix, image }));
  // 多图逐张结算；前面的成功图保留，后续失败通过 error 帧说明，不能冒充全量成功。
  if (failure) {
    if (failure.index !== images.length) throw new RangeError('原生图片失败样本编号不连续');
    frames.push(nativeImageMsgpack({ event_type: 'error', samp_ix: failure.index, message: failure.message }));
  }
  return Buffer.concat(frames);
}

export async function* nativeImagesJson(images, summary) {
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
