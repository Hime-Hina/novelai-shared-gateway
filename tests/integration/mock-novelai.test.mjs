import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import { fetchNovelAiAccountQuota, generateNovelAiImage } from '../../server/providers.ts';
import { validateImageBytes } from '../../server/native-image.ts';
import { readNativeEvents } from '../helpers/native-stream-client.mjs';

const script = fileURLToPath(new URL('../../scripts/mock-novelai.mjs', import.meta.url));
const token = 'pst-synthetic-d01-mock-upstream-only';
const body = { input: 'synthetic mock test', model: 'nai-diffusion-4-5-full', action: 'generate',
  parameters: { width: 1024, height: 1024, steps: 23, n_samples: 1, seed: 0 } };
const headers = { authorization: `Bearer ${token}`, 'content-type': 'application/json' };

async function start(t, delayMs = 0) {
  const env = Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP'].filter(name => process.env[name]).map(name => [name, process.env[name]]));
  const child = spawn(process.execPath, [script, '--port', '0', '--delay-ms', String(delayMs)], { env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  let output = ''; const exited = once(child, 'exit');
  child.stdout.on('data', bytes => { output += bytes; }); child.stderr.on('data', bytes => { output += bytes; });
  t.after(async () => { if (child.exitCode === null && child.signalCode === null) child.kill(); await exited; });
  const deadline = Date.now() + 5000;
  while (!output.includes('[mock-novelai] ')) {
    assert.equal(child.exitCode, null, output);
    assert.ok(Date.now() < deadline, 'mock startup timeout');
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  const ready = JSON.parse(output.split('[mock-novelai] ')[1].split('\n')[0]);
  assert.equal(ready.host, '127.0.0.1'); assert.equal(ready.delayMs, delayMs);
  return { base: `http://127.0.0.1:${ready.port}`, output: () => output };
}
function raw(base, path, extraHeaders, bytes) {
  return new Promise((resolve, reject) => {
    const req = http.request(base + path, { method: 'POST', headers: { ...headers, ...extraHeaders }, agent: false }, res => {
      let text = ''; res.setEncoding('utf8'); res.on('data', chunk => { text += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, text })); res.on('error', reject);
    });
    req.on('error', reject); req.setTimeout(5000, () => req.destroy(new Error('mock request timeout')));
    req.end(bytes);
  });
}

test('常驻mock与真实provider兼容：合成Opus、PNG及预览/最终MessagePack，余额固定', { timeout: 15000 }, async t => {
  const mock = await start(t, 200), env = { NOVELAI_API_URL: mock.base, NOVELAI_ACCOUNT_API_URL: mock.base };
  const quota = await fetchNovelAiAccountQuota(token, env);
  assert.equal(quota.tier, 3); assert.equal(quota.subscriptionActive, true);
  assert.equal(quota.fixed, 10000); assert.equal(quota.purchased, 0); assert.equal(quota.v5UsagePercent, 100);
  const request = { prompt: body.input, model: body.model, action: body.action, width: 1024, height: 1024,
    steps: 23, seed: 0, nativeParameters: body.parameters };
  const began = performance.now();
  const plain = await generateNovelAiImage(request, { token }, env, { forceStream: false });
  assert.ok(performance.now() - began >= 150); assert.equal(plain.mimeType, 'image/png');
  assert.deepEqual(validateImageBytes(plain.buffer), { width: 1, height: 1, mimeType: 'image/png', byteLength: plain.buffer.length });
  const preview = [];
  const streamed = await generateNovelAiImage(request, { token }, env, { forceStream: true, onProgress: value => preview.push(value) });
  assert.deepEqual(streamed.buffer, plain.buffer);
  assert.ok(preview.some(value => value.stepIndex === 0 && value.previewBuffer?.equals(plain.buffer)));
  const response = await fetch(mock.base + '/ai/generate-image-stream', { method: 'POST', headers, body: JSON.stringify(body) });
  const events = readNativeEvents(Buffer.from(await response.arrayBuffer()));
  assert.deepEqual(events.map(event => event.event_type), ['intermediate', 'final']);
  assert.equal(events[0].step_ix, 0); assert.deepEqual(Buffer.from(events[1].image), plain.buffer);
  const after = await fetchNovelAiAccountQuota(token, env);
  assert.equal(after.fixed, quota.fixed); assert.equal(after.v5UsagePercent, quota.v5UsagePercent);
  assert.ok(!mock.output().includes(body.input)); assert.ok(!mock.output().includes(token));
});

test('mock拒绝其他凭据、未知路径/方法与坏正文；声明和chunked正文都受32MiB限制', { timeout: 15000 }, async t => {
  const mock = await start(t);
  for (const supplied of ['', 'pst-not-the-synthetic-d01-credential']) {
    const response = await fetch(mock.base + '/user/data', { headers: { authorization: `Bearer ${supplied}` } });
    assert.equal(response.status, 401); assert.ok(!(await response.text()).includes(supplied || 'Bearer'));
  }
  const unknown = await fetch(mock.base + '/ai/upscale', { method: 'POST', headers, body: '{}' });
  assert.equal(unknown.status, 404); await unknown.text();
  const method = await fetch(mock.base + '/ai/generate-image', { headers });
  assert.equal(method.status, 405); await method.text();
  for (const input of ['not-json', '{}', JSON.stringify({ ...body, parameters: { n_samples: 4 } })]) {
    const response = await fetch(mock.base + '/ai/generate-image', { method: 'POST', headers, body: input });
    assert.equal(response.status, 400); await response.text();
  }
  assert.equal((await raw(mock.base, '/ai/generate-image', { 'content-length': String(32 * 1024 * 1024 + 1) })).status, 413);
  assert.equal((await raw(mock.base, '/ai/generate-image', { 'transfer-encoding': 'chunked' }, Buffer.alloc(32 * 1024 * 1024 + 1, 120))).status, 413);
  const alive = await fetch(mock.base + '/user/data', { headers }); assert.equal(alive.status, 200); await alive.text();
});

test('预览先到且断开仅清本次延迟，mock仍可持续服务', { timeout: 10000 }, async t => {
  const mock = await start(t, 500), began = performance.now();
  const response = await fetch(mock.base + '/ai/generate-image-stream', { method: 'POST', headers, body: JSON.stringify(body) });
  const reader = response.body.getReader(), first = await reader.read();
  assert.equal(readNativeEvents(Buffer.from(first.value))[0].event_type, 'intermediate');
  assert.ok(performance.now() - began < 500, '中间帧应在最终结果延迟之前到达');
  await reader.cancel();
  const quota = await fetch(mock.base + '/user/data', { headers }); assert.equal(quota.status, 200); await quota.text();
});

test('mock启动参数有界，不能通过参数改变回环绑定', () => {
  for (const args of [['--host', '0.0.0.0'], ['--delay-ms', '60001'], ['--port', '65536'], ['--port']]) {
    const result = spawnSync(process.execPath, [script, ...args], { encoding: 'utf8', windowsHide: true });
    assert.notEqual(result.status, 0); assert.match(result.stderr, /Usage:/);
  }
});
