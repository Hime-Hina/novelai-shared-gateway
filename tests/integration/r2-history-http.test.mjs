import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { mkdtemp, mkdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';
import { JsonStore } from '../../server/store.ts';

test('本地及 R2 历史图片经真实 HTTP 查看和下载；签名、归属、原图校验保持有效', { timeout: 20000 }, async t => {
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-r2-http-'));
  const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
  const sha256 = createHash('sha256').update(png).digest('hex');
  const token = 'STA1N-synthetic-r2-member', createdAt = new Date().toISOString();
  const store = new JsonStore(dataDir); await store.init();
  await mkdir(path.join(dataDir, 'images'), { recursive: true });
  await writeFile(path.join(dataDir, 'images/local.png'), png);
  await store.update(db => {
    db.users.push({ id: 'usr_r2', token, enabled: true, balance: 0, createdAt });
    db.users.push({ id: 'usr_other', token: 'STA1N-synthetic-other-member', enabled: true, balance: 0, createdAt });
    for (const id of ['local', 'cloud', 'corrupt', 'missing']) {
      db.images.push({ id, token, file: `images/${id}.png`, mimeType: 'image/png', createdAt, sha256, byteLength: png.length,
        ...(['local', 'missing'].includes(id) ? {} : { r2: { bucket: 'synthetic-images', key: `images/${id}/${sha256}`,
          sha256, byteLength: png.length, verifiedAt: createdAt, localRemovedAt: createdAt } }) });
      db.jobs.push({ id: `job_${id}`, userToken: token, status: 'done', imageId: id, createdAt, completedAt: createdAt });
    }
  }, { immediate: true });
  await store.close();
  const preload = path.join(dataDir, 'r2-preload.mjs');
  await writeFile(preload, `
    globalThis.fetch = async (url, options) => {
      if (url.hostname !== '${'a'.repeat(32)}.r2.cloudflarestorage.com' || options.method !== 'GET') throw new Error('Unexpected external request');
      const bytes = Buffer.from('${png.toString('base64')}', 'base64');
      if (url.pathname.includes('/corrupt/')) bytes[0] ^= 1;
      return new Response(bytes, { headers: { 'content-length': String(bytes.length) } });
    };`);
  const socket = http.createServer(); socket.listen(0, '127.0.0.1'); await once(socket, 'listening');
  const port = socket.address().port; await new Promise(resolve => socket.close(resolve));
  const base = `http://127.0.0.1:${port}`;
  const env = Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE']
    .filter(key => process.env[key]).map(key => [key, process.env[key]]));
  const child = spawn(process.execPath, ['--import', pathToFileURL(preload).href, 'server/index.ts'], {
    cwd: fileURLToPath(new URL('../../', import.meta.url)), windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...env, DATA_DIR: dataDir, ADMIN_TOKEN: 'synthetic-r2-admin-12345678901234567890', HOST: '127.0.0.1', PORT: String(port),
      PUBLIC_BASE_URL: base, R2_ACCOUNT_ID: 'a'.repeat(32), R2_BUCKET: 'synthetic-images',
      R2_ACCESS_KEY_ID: 'synthetic-access', R2_SECRET_ACCESS_KEY: 'synthetic-secret' }
  });
  const exit = once(child, 'exit'); let output = '';
  child.stdout.on('data', chunk => { output += chunk; }); child.stderr.on('data', chunk => { output += chunk; });
  t.after(async () => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); await exit; });
  const deadline = Date.now() + 15000;
  while (!output.includes('Nai2API listening')) {
    assert.equal(child.exitCode, null, 'synthetic runtime must start');
    assert.ok(Date.now() < deadline, 'runtime startup deadline');
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  const request = (url, auth = '') => fetch(new URL(url, base), { headers: auth ? { authorization: `Bearer ${auth}` } : {}, signal: AbortSignal.timeout(3000) });
  for (const id of ['local', 'cloud']) {
    const view = await request(`/api/jobs/job_${id}`, token); assert.equal(view.status, 200);
    const body = await view.json();
    assert.ok(!JSON.stringify(body).includes('cloudflarestorage.com'));
    assert.ok(!JSON.stringify(body).includes('synthetic-secret'));
    for (const response of [await request(body.imageUrl), await request(`/api/jobs/job_${id}/content`, token)]) {
      assert.equal(response.status, 200); assert.match(response.headers.get('content-type'), /image\/png/);
      assert.deepEqual(Buffer.from(await response.arrayBuffer()), png);
    }
    assert.equal((await request(`/api/images/${id}/content`)).status, 401);
    assert.equal((await request(`/api/jobs/job_${id}/content`, 'STA1N-synthetic-other-member')).status, 403);
    const tampered = new URL(body.imageUrl, base); tampered.searchParams.set('signature', 'invalid');
    assert.equal((await request(tampered)).status, 401);
  }
  const failure = await request('/api/jobs/job_corrupt/content', token);
  assert.equal(failure.status, 503);
  assert.ok(!(await failure.text()).includes('synthetic-secret'));
  const missing = await request('/api/jobs/job_missing/content', token);
  assert.equal(missing.status, 500, 'retain the existing local storage failure contract');
  assert.ok(!(await missing.text()).includes(dataDir), 'filesystem paths stay private');
});
