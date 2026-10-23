import assert from 'node:assert/strict';
import { decode } from './msgpack.mjs';

// 使用客户端同版本的独立 MessagePack 解码库验协议，不复用服务端编码逻辑。
export function readNativeEvents(buffer) {
  const events = [];
  let offset = 0;
  while (offset < buffer.length) {
    assert.ok(buffer.length - offset >= 4, '缺少大端帧长度');
    const length = buffer.readUInt32BE(offset);
    assert.ok(length > 0 && length <= 64 * 1024 * 1024, '无效帧长度');
    offset += 4;
    assert.ok(offset + length <= buffer.length, '帧被截断');
    events.push(decode(buffer.subarray(offset, offset + length)));
    offset += length;
  }
  return events;
}

// 独立按 SSE event/data 行解析；图片保留协议中的 base64 字符串。
export function readNativeSseEvents(buffer) {
  const text = buffer.toString('utf8');
  assert.ok(text.endsWith('\n\n'), 'SSE 帧未完整结束');
  return text.slice(0, -2).split('\n\n').map(frame => {
    const lines = frame.split('\n');
    assert.equal(lines.length, 2, 'SSE 必须包含一行 event 和一行 JSON data');
    assert.match(lines[0], /^event: (intermediate|final|error)$/);
    assert.ok(lines[1].startsWith('data: '), 'SSE 缺少 JSON data');
    const event = JSON.parse(lines[1].slice(6));
    assert.equal(event.event_type, lines[0].slice(7), 'SSE 事件名与 JSON 类型不一致');
    return event;
  });
}
