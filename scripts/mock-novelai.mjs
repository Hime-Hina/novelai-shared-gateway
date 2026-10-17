import http from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import { nativeImageMsgpack } from '../server/native-stream.js';

// 仅供隔离验收：固定公开测试凭据、固定图片和余额，不模拟官方收费或生成质量。
const token = 'pst-synthetic-d01-mock-upstream-only';
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
const maxBodyBytes = 32 * 1024 * 1024;
const options = { port: 19081, 'delay-ms': 1000 };
for (let index = 2; index < process.argv.length; index += 2) {
  const name = process.argv[index].slice(2), value = process.argv[index + 1];
  if (process.argv[index] !== `--${name}` || !Object.hasOwn(options, name) || !/^\d+$/.test(value || '')
    || Number(value) > (name === 'port' ? 65535 : 60000)) {
    throw new Error('Usage: mock-novelai.mjs [--port 0..65535] [--delay-ms 0..60000]');
  }
  options[name] = Number(value);
}

function json(res, status, body) {
  const text = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store',
    'content-length': Buffer.byteLength(text), ...(status >= 400 ? { connection: 'close' } : {}) });
  res.end(text);
}
function fail(statusCode, message) { throw Object.assign(new Error(message), { statusCode }); }
async function readJson(req) {
  if (String(req.headers['content-type'] || '').split(';')[0].trim() !== 'application/json') fail(415, 'Mock requires application/json');
  if (Number(req.headers['content-length']) > maxBodyBytes) fail(413, 'Mock request body too large');
  const chunks = []; let bytes = 0;
  // 超限后由错误响应关闭连接，不让迭代器提前销毁响应 socket 而丢失 413。
  for await (const chunk of req.iterator({ destroyOnReturn: false })) {
    bytes += chunk.length;
    if (bytes > maxBodyBytes) fail(413, 'Mock request body too large');
    chunks.push(chunk);
  }
  let body;
  try { body = JSON.parse(Buffer.concat(chunks).toString('utf8')); }
  catch { fail(400, 'Invalid mock JSON body'); }
  if (!body || Array.isArray(body) || typeof body !== 'object' || typeof body.input !== 'string'
    || !body.input.trim() || typeof body.model !== 'string' || !body.model
    || !body.parameters || Array.isArray(body.parameters) || typeof body.parameters !== 'object'
    || (body.parameters.n_samples ?? 1) !== 1) fail(400, 'Mock requires a single-image generation request');
  return body;
}

let closing = false;
const server = http.createServer(async (req, res) => {
  const controller = new AbortController();
  const abort = () => controller.abort();
  res.once('close', abort);
  try {
    if (closing) fail(503, 'Mock is stopping');
    if (req.headers.authorization !== `Bearer ${token}`) fail(401, 'Synthetic mock credential required');
    const pathname = new URL(req.url, 'http://127.0.0.1').pathname;
    if (pathname === '/user/data') {
      if (req.method !== 'GET') fail(405, 'Mock method not supported');
      json(res, 200, { subscription: { tier: 3, active: true, expiresAt: Math.floor(Date.now() / 1000) + 86400,
        trainingStepsLeft: { fixedTrainingStepsLeft: 10000, purchasedTrainingSteps: 0 },
        usage: { percent: 100, isNegative: false, timeUntilNextPercent: 0 } } });
      return;
    }
    if (!['/ai/generate-image', '/ai/generate-image-stream'].includes(pathname)) fail(404, 'Mock route not implemented');
    if (req.method !== 'POST') fail(405, 'Mock method not supported');
    await readJson(req);
    const stream = pathname.endsWith('-stream');
    if (stream) {
      res.writeHead(200, { 'content-type': 'application/octet-stream', 'cache-control': 'no-store' });
      res.write(nativeImageMsgpack({ event_type: 'intermediate', samp_ix: 0, step_ix: 0, image: png }));
    }
    await delay(options['delay-ms'], undefined, { signal: controller.signal });
    if (stream) res.end(nativeImageMsgpack({ event_type: 'final', samp_ix: 0, image: png }));
    else {
      res.writeHead(200, { 'content-type': 'image/png', 'content-length': png.length, 'cache-control': 'no-store' });
      res.end(png);
    }
  } catch (error) {
    if (!res.destroyed && !res.writableEnded) {
      if (res.headersSent) res.destroy();
      else { req.resume(); json(res, error.statusCode || 500, { message: error.statusCode ? error.message : 'Mock request failed' }); }
    }
  } finally { res.off('close', abort); }
});
server.listen(options.port, '127.0.0.1', () => {
  console.log('[mock-novelai] ' + JSON.stringify({ host: '127.0.0.1', port: server.address().port, delayMs: options['delay-ms'] }));
});
for (const signal of ['SIGTERM', 'SIGINT']) process.once(signal, () => {
  if (closing) return;
  closing = true;
  server.close();
  setTimeout(() => server.closeAllConnections(), 65000).unref();
});
