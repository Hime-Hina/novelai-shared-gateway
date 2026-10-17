import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, writeFile, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import Database from 'better-sqlite3';
import { bootstrap } from '../../scripts/bootstrap.mjs';
import { JsonStore } from '../../server/store.js';
import { syncFairQuota, settleFairQuota } from '../../server/fair-quota.js';

const root = fileURLToPath(new URL('../../', import.meta.url));
const preload = new URL('../helpers/result-recovery-preload.mjs', import.meta.url).href;
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
const vibe = Buffer.from(Array.from({ length: 256 }, (_, i) => i));
const imageRequest = { input: 'synthetic result recovery', model: 'nai-diffusion-5-full', action: 'generate',
  parameters: { width: 1024, height: 1024, steps: 23, n_samples: 1, seed: 7 } };
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(predicate, label, timeout = 15000) {
  const end = Date.now() + timeout;
  while (!await predicate()) {
    if (Date.now() >= end) throw new Error(`synthetic recovery timeout: ${label}`);
    await delay(20);
  }
}
async function listen(server) { server.listen(0, '127.0.0.1'); await once(server, 'listening'); return server.address().port; }
const durableState = (f, job) => ({ status: job.status, imageId: job.imageId, pending: job.resultPending,
  review: job.generationReview, cooldown: job.sharedCooldown, charge: f.settlement(job.id),
  images: f.images().map(({ id, file, sha256, byteLength }) => ({ id, file, sha256, byteLength })) });

async function fixture(t, { stage, operation = 'generate', hold = false, grace = 30000 } = {}) {
  const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-result-recovery-'));
  const adminToken = 'synthetic-admin-result-recovery-12345678';
  const initialized = await bootstrap({ env: { ADMIN_TOKEN: adminToken, NOVELAI_TOKEN: 'pst-synthetic-recovery-only', DATA_DIR: dataDir }, log() {} });
  const members = JSON.parse(await readFile(initialized.credentialPath, 'utf8')).accounts;
  const seeded = new JsonStore(dataDir); await seeded.init();
  await seeded.update(db => {
    Object.assign(db.accounts[0], { quotaTier: 3, subscriptionActive: true, subscriptionExpiresAt: Date.now() + 86400000,
      quotaFixed: 10000, quotaPurchased: 0, v5UsagePercent: 100, quotaCheckedAt: new Date().toISOString(), upstreamStatus: 'active' });
    syncFairQuota(db);
  }, { immediate: true }); await seeded.close();
  const reader = new Database(path.join(dataDir, 'library.sqlite'), { readonly: true });
  let generations = 0, release;
  const held = hold ? new Promise(resolve => { release = resolve; }) : Promise.resolve();
  const upstream = http.createServer(async (req, res) => {
    if (req.url === '/user/data') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ subscription: { tier: 3, active: true, expiresAt: Math.floor(Date.now() / 1000) + 86400,
        trainingStepsLeft: { fixedTrainingStepsLeft: 10000, purchasedTrainingSteps: 0 },
        usage: { percent: 100, isNegative: false, timeUntilNextPercent: 0 } } })); return;
    }
    for await (const _chunk of req) { /* 消费合成请求，不记录认证或正文。 */ }
    generations++; await held;
    res.writeHead(200, { 'content-type': operation === 'encode-vibe' ? 'application/octet-stream' : 'image/png' });
    res.end(operation === 'encode-vibe' ? vibe : png);
  });
  const upstreamPort = await listen(upstream), reservedPort = http.createServer(), port = await listen(reservedPort);
  await new Promise(resolve => reservedPort.close(resolve));
  const base = `http://127.0.0.1:${port}`;
  const env = Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE'].filter(key => process.env[key]).map(key => [key, process.env[key]]));
  Object.assign(env, { ADMIN_TOKEN: adminToken, DATA_DIR: dataDir, HOST: '127.0.0.1', PORT: String(port), PUBLIC_BASE_URL: base,
    NOVELAI_API_URL: `http://127.0.0.1:${upstreamPort}`, NOVELAI_ACCOUNT_API_URL: `http://127.0.0.1:${upstreamPort}`,
    SHUTDOWN_GRACE_MS: String(grace), RUNTIME_IMAGES_CACHE_LIMIT: '500', RUNTIME_JOBS_CACHE_LIMIT: '500' });
  let child, exited, output = '', points = [];
  const safeOutput = () => {
    let value = output;
    for (const secret of [adminToken, 'pst-synthetic-recovery-only', ...members.flatMap(member => [member.api_key, member.password])]) value = value.replaceAll(secret, '[redacted]');
    return value.slice(-3000);
  };
  async function start(faultStage = '') {
    points = []; output = '';
    child = spawn(process.execPath, [...(faultStage ? ['--import', preload] : []), 'server/index.js'], {
      cwd: root, env: { ...env, ...(faultStage ? { RESULT_RECOVERY_TEST_STAGE: faultStage } : {}) }, windowsHide: true,
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
    exited = once(child, 'exit');
    child.on('message', message => points.push(message));
    child.stdout.on('data', chunk => { output = (output + chunk.toString()).slice(-8000); });
    child.stderr.on('data', chunk => { output = (output + chunk.toString()).slice(-8000); });
    await until(() => {
      if (child.exitCode !== null || child.signalCode !== null) throw new Error(`synthetic child exited: ${safeOutput()}`);
      return output.includes('Nai2API listening');
    }, 'startup: ' + faultStage);
  }
  async function stop(signal = 'SIGKILL') {
    if (child?.exitCode === null && child?.signalCode === null) child.kill(signal);
    if (exited) await exited;
  }
  t.after(async () => { release?.(); await stop(); reader.close(); upstream.closeAllConnections(); await new Promise(resolve => upstream.close(resolve)); });
  const jobs = () => reader.prepare('SELECT data FROM jobs ORDER BY order_value').all().map(row => JSON.parse(row.data));
  const images = () => reader.prepare('SELECT data FROM images ORDER BY order_value').all().map(row => JSON.parse(row.data));
  const settlement = jobId => { const row = reader.prepare('SELECT charge FROM fair_quota_settlements WHERE job_id = ?').get(jobId); return row && JSON.parse(row.charge); };
  const fetchLocal = (route, options = {}) => fetch(new URL(route, base), { ...options, signal: AbortSignal.timeout(15000) });
  const headers = { authorization: `Bearer ${members[0].api_key}`, 'content-type': 'application/json' };
  const generate = (parameters = {}) => fetchLocal(`/ai/${operation === 'encode-vibe' ? 'encode-vibe' : 'generate-image'}`, { method: 'POST', headers,
    body: JSON.stringify(operation === 'encode-vibe' ? { image: png.toString('base64'), model: 'nai-diffusion-4-5-full' }
      : { ...imageRequest, parameters: { ...imageRequest.parameters, ...parameters } }) })
    .then(async response => ({ status: response.status, bytes: Buffer.from(await response.arrayBuffer()) }), () => null);
  const waitPoint = async point => {
    try { await until(() => points.some(message => message.point === point), `fault point ${point}`); }
    catch (error) { throw new Error(`${error.message}: ${safeOutput()}`); }
    return points.find(message => message.point === point);
  };
  async function claim(job, expected) {
    const response = await fetchLocal(`/api/jobs/${job.id}`, { headers }); assert.equal(response.status, 200);
    const view = await response.json(); assert.ok(view.imageUrl, '持久结果必须可领取');
    // 真实签名 URL 无需把成员 Key 塞入 URL 或附带 Authorization。
    const download = await fetchLocal(view.imageUrl); assert.equal(download.status, 200);
    assert.deepEqual(Buffer.from(await download.arrayBuffer()), expected);
    return view;
  }
  return { dataDir, reader, jobs, images, settlement, start, stop, generate, waitPoint, claim, members, fetchLocal, headers,
    generations: () => generations, release: () => release?.(), exit: () => exited, child: () => child, stage };
}

for (const stage of ['descriptor', 'half_write', 'file_sync', 'rename', 'dir_sync', 'final_sql']) {
  test(`强杀 ${stage} 后两次启动仅恢复原尝试，不重发、不重复结算`, { timeout: 60000, skip: stage === 'dir_sync' && process.platform === 'win32' }, async t => {
    const f = await fixture(t, { stage }); await f.start(stage);
    const response = f.generate(), point = await f.waitPoint(stage);
    if (stage === 'descriptor') assert.equal(point.sqliteSynchronous, 2, '持久数据库必须为 FULL');
    assert.equal(f.generations(), 1);
    const original = f.jobs()[0], expectedId = original.resultPending?.imageId || original.imageId;
    assert.ok(expectedId);
    await f.stop(); await response;
    const complete = !['descriptor', 'half_write'].includes(stage);
    let snapshot;
    for (let restart = 0; restart < 2; restart++) {
      await f.start();
      const job = f.jobs()[0]; assert.equal(job.status, complete ? 'done' : 'failed');
      assert.equal(f.generations(), 1); assert.equal(f.images().length, complete ? 1 : 0);
      assert.equal(f.settlement(job.id).status, complete ? 'settled' : 'refunded');
      if (complete) { assert.equal(job.imageId, expectedId); assert.equal(job.resultPending, undefined); await f.claim(job, png); }
      else { assert.ok(job.resultPending, '缺失或半写描述符保留为证据'); assert.ok(!job.imageId); }
      const state = durableState(f, job);
      if (snapshot) assert.deepEqual(state, snapshot, '第二次启动必须无重复结算或状态漂移');
      snapshot = state; await f.stop();
    }
  });
}

test('Native多图首图强杀恢复先于启动清理，第二张只在启动完成和间隔结束后派发', { timeout: 45000 }, async t => {
  const f = await fixture(t); await f.start('file_sync');
  const response = f.generate({ n_samples: 2 }); await f.waitPoint('file_sync');
  const first = f.jobs().find(job => job.sampleIndex === 0), second = f.jobs().find(job => job.sampleIndex === 1);
  assert.equal(first.status, 'running'); assert.equal(second.status, 'pending'); assert.equal(f.generations(), 1);
  const imageId = first.resultPending.imageId;
  await f.stop(); await response;
  await f.start();
  assert.equal(f.jobs().find(job => job.id === first.id).status, 'done');
  assert.equal(f.jobs().find(job => job.id === first.id).imageId, imageId);
  assert.equal(f.jobs().find(job => job.id === second.id).status, 'queued');
  assert.equal(f.generations(), 1);
  const completedAt = Date.parse(f.jobs().find(job => job.id === first.id).completedAt);
  await until(() => {
    const jobs = f.jobs();
    assert.ok(!jobs.some(job => job.status === 'failed'), '新激活子任务不能被启动清理误判为旧在途');
    return jobs.every(job => job.status === 'done');
  }, 'second recovered batch image', 25000);
  const later = f.jobs().find(job => job.id === second.id);
  assert.ok(Date.parse(later.startedAt) - completedAt >= 9900, '恢复成功也保留十秒账户间隔');
  assert.equal(f.generations(), 2); assert.equal(f.images().length, 2);
  for (const job of f.jobs()) { assert.equal(f.settlement(job.id).status, 'settled'); await f.claim(job, png); }
  assert.equal(f.reader.prepare("SELECT COUNT(*) AS n FROM ledger WHERE json_extract(data, '$.type') = 'generation_refunded'").get().n, 0);
});

test('完整part被篡改不能误恢复成功；已退款Vibe完整part恢复仍保留退款并可领取', { timeout: 60000 }, async t => {
  for (const refund of [false, true]) {
    const f = await fixture(t, { operation: refund ? 'encode-vibe' : 'generate' });
    await f.start('file_sync'); const response = f.generate(); await f.waitPoint('file_sync'); await f.stop(); await response;
    const job = f.jobs()[0], pending = job.resultPending;
    if (refund) {
      const store = new JsonStore(f.dataDir); await store.init();
      await store.update(db => {
        const current = store.loadJobForUpdate(job.id); settleFairQuota(db, current, false);
        current.status = 'failed'; current.error = 'synthetic prior refund';
      }, { includeSettings: true, immediate: true }); await store.close();
    } else {
      const bytes = await readFile(path.join(f.dataDir, pending.file + '.part')); bytes[bytes.length - 1] ^= 1;
      assert.notEqual(createHash('sha256').update(bytes).digest('hex'), pending.sha256);
      await writeFile(path.join(f.dataDir, pending.file + '.part'), bytes);
    }
    let snapshot;
    for (let restart = 0; restart < 2; restart++) {
      await f.start(); const recovered = f.jobs()[0];
      assert.equal(recovered.status, 'failed'); assert.equal(f.settlement(job.id).status, 'refunded'); assert.equal(f.generations(), 1);
      if (refund) { assert.equal(recovered.imageId, pending.imageId); assert.equal((await f.claim(recovered, vibe)).lateResultAvailable, true); }
      else { assert.ok(!recovered.imageId); assert.equal(f.images().length, 0); assert.ok(recovered.resultPending); }
      const state = durableState(f, recovered);
      if (snapshot) assert.deepEqual(state, snapshot); snapshot = state; await f.stop();
    }
  }
});

test('正常SIGTERM停止新派发并等待在途落盘；超过宽限由重启按未知处理', { timeout: 60000, skip: process.platform === 'win32' }, async t => {
  for (const timeout of [false, true]) {
    const f = await fixture(t, { hold: true, grace: timeout ? 100 : 3000 }); await f.start();
    const response = f.generate(); await until(() => f.generations() === 1, 'upstream started');
    if (!timeout) {
      const queued = await f.fetchLocal('/api/jobs', { method: 'POST', headers: f.headers,
        body: JSON.stringify({ tag: 'synthetic queued during shutdown', model: 'nai-diffusion-4-5-full', width: 1024, height: 1024, steps: 23 }) });
      assert.equal(queued.status, 202);
    }
    const began = Date.now(); f.child().kill('SIGTERM');
    if (!timeout) { await delay(100); assert.equal(f.child().exitCode, null); f.release(); }
    await f.exit(); const delivered = await response;
    assert.ok(Date.now() - began < 5000, '关闭必须遵守有限宽限');
    assert.equal(f.generations(), 1);
    if (!timeout) {
      assert.equal(delivered?.status, 200, '正常关闭必须交付完整HTTP响应');
      const bytes = delivered.bytes;
      assert.ok(bytes.length >= 52, 'ZIP至少包含本地头和完整EOCD');
      assert.equal(bytes.readUInt32LE(0), 0x04034b50);
      assert.equal(bytes.readUInt16LE(8), 0, 'fixture图片使用stored ZIP');
      const imageAt = 30 + bytes.readUInt16LE(26) + bytes.readUInt16LE(28);
      assert.deepEqual(bytes.subarray(imageAt, imageAt + bytes.readUInt32LE(18)), png);
      const end = bytes.length - 22;
      assert.equal(bytes.readUInt32LE(end), 0x06054b50, '响应末尾必须包含完整EOCD');
      assert.equal(bytes.readUInt16LE(end + 20), 0, 'EOCD后无注释或截断数据');
      assert.equal(bytes.readUInt16LE(end + 10), 1);
      assert.equal(bytes.readUInt32LE(end + 16) + bytes.readUInt32LE(end + 12), end);
      assert.equal(f.jobs().filter(job => job.status === 'done').length, 1);
      assert.equal(f.jobs().filter(job => job.status === 'queued').length, 1);
    }
    else {
      f.release(); await f.start();
      assert.equal(f.jobs()[0].status, 'failed'); assert.equal(f.settlement(f.jobs()[0].id).status, 'refunded'); assert.equal(f.generations(), 1);
    }
  }
});

test('超过500份结果的冷历史PNG和Vibe在重启后仍有记录、文件和真实签名领取', { timeout: 60000 }, async t => {
  const f = await fixture(t), store = new JsonStore(f.dataDir); await store.init();
  await mkdir(path.join(f.dataDir, 'images'), { recursive: true });
  const entries = Array.from({ length: 503 }, (_, i) => ({ id: `synthetic-result-${i}`, token: f.members[0].api_key,
    mimeType: i === 502 ? 'application/octet-stream' : 'image/png', file: `images/synthetic-result-${i}.${i === 502 ? 'bin' : 'png'}`,
    createdAt: new Date(Date.now() - i * 1000).toISOString() }));
  await Promise.all(entries.map(image => writeFile(path.join(f.dataDir, image.file), image.mimeType === 'image/png' ? png : vibe)));
  await store.update(db => {
    db.images = entries;
    db.jobs = entries.map(image => ({ id: `job-${image.id}`, status: 'done', userToken: f.members[0].api_key, imageId: image.id, createdAt: image.createdAt }));
  }, { immediate: true, dirtyRows: { images: entries.map(image => image.id), jobs: entries.map(image => `job-${image.id}`) } }); await store.close();
  for (let restart = 0; restart < 2; restart++) {
    await f.start(); assert.equal(f.images().length, 503); assert.equal(f.generations(), 0);
    await f.claim({ id: 'job-synthetic-result-501' }, png); await f.claim({ id: 'job-synthetic-result-502' }, vibe);
    assert.deepEqual(await readFile(path.join(f.dataDir, entries[501].file)), png);
    assert.deepEqual(await readFile(path.join(f.dataDir, entries[502].file)), vibe);
    await f.stop();
  }
});
