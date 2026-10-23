import { timingSafeEqual } from 'node:crypto';

/**
 * @param {(req: import('node:http').IncomingMessage, res: import('node:http').ServerResponse) => void | Promise<void>} handler
 * @param {string | undefined} token
 */
export function protectExecutorHandler(handler, token) {
  const expected = Buffer.from(String(token || '').trim(), 'utf8');
  if (expected.byteLength < 32) {
    throw new Error('EXECUTOR_TOKEN must contain at least 32 bytes.');
  }

  /** @type {typeof handler} */
  const guarded = (req, res) => {
    const value = req.headers['x-executor-token'];
    const supplied = Buffer.from(typeof value === 'string' ? value : '', 'utf8');
    const matches = supplied.byteLength === expected.byteLength && timingSafeEqual(supplied, expected);
    let headerCount = 0;
    // 内部凭据不传入业务鉴权、请求诊断或后续转发；其他入站头保持原样。
    delete req.headers['x-executor-token'];
    for (let index = req.rawHeaders.length - 2; index >= 0; index -= 2) {
      if (req.rawHeaders[index].toLowerCase() === 'x-executor-token') {
        headerCount += 1;
        req.rawHeaders.splice(index, 2);
      }
    }
    if (!matches || headerCount !== 1) {
      res.writeHead(401, {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'no-store',
        'X-Executor-Auth-Error': '1'
      });
      res.end(JSON.stringify({ error: 'Invalid executor credentials.' }));
      return;
    }
    return handler(req, res);
  };
  return guarded;
}
