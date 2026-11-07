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
import { brotliCompressSync } from 'node:zlib';
import { createMemberSession } from '../../server/members/authentication.ts';
import { JsonStore } from '../../server/storage/sqlite-store.ts';

test(
  '本地、R2、PikPak 及旧快照图片经真实 HTTP 领取，校验签名、归属及原图',
  { timeout: 20000 },
  async (t) => {
    const dataDir = await mkdtemp(path.join(tmpdir(), 'nai-r2-http-'));
    const png = Buffer.from(
      'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=',
      'base64',
    );
    const sha256 = createHash('sha256').update(png).digest('hex');
    const compressed = brotliCompressSync(png);
    const corruptR2Hash = 'e'.repeat(64);
    const corruptPikPakHash = 'f'.repeat(64);
    const archiveUrl = 'https://dav.pikpak.test/archive/';
    const token = 'STA1N-synthetic-r2-member',
      otherToken = 'STA1N-synthetic-other-member',
      createdAt = new Date().toISOString();
    const store = new JsonStore(dataDir);
    await store.init();
    await mkdir(path.join(dataDir, 'images'), { recursive: true });
    await writeFile(path.join(dataDir, 'images/local.png'), png);
    const db = await store.read();
    const owner = {
      id: 'usr_r2',
      token,
      enabled: true,
      balance: 0,
      createdAt,
      memberAuth: { username: 'r2-member', version: 'r2-member-auth' },
    };
    const other = {
      ...owner,
      id: 'usr_other',
      token: otherToken,
      memberAuth: { username: 'other-member', version: 'other-member-auth' },
    };
    db.users.push(owner, other);
    for (const id of [
      'local',
      'cloud',
      'pikpak',
      'old_snapshot',
      'cold_corrupt',
      'corrupt',
      'missing',
    ]) {
      const cold = id === 'pikpak' || id === 'cold_corrupt';
      const imageHash = { corrupt: corruptR2Hash, cold_corrupt: corruptPikPakHash }[id] ?? sha256;
      db.images.push({
        id,
        token,
        file: `images/${id}.png`,
        mimeType: 'image/png',
        createdAt,
        sha256: imageHash,
        byteLength: png.length,
        ...(['local', 'missing'].includes(id)
          ? {}
          : {
              r2: {
                bucket: 'synthetic-images',
                key: `images/${id}/${imageHash}`,
                sha256: imageHash,
                byteLength: png.length,
                verifiedAt: createdAt,
                localRemovedAt: createdAt,
                ...(cold
                  ? { removedAt: createdAt, pikpak: { url: archiveUrl, uploadedAt: createdAt } }
                  : {}),
              },
            }),
      });
      db.jobs.push({
        id: `job_${id}`,
        userToken: token,
        status: 'done',
        imageId: id,
        createdAt,
        completedAt: createdAt,
      });
    }
    await store.write(db);
    const ownerSession = await createMemberSession(store, owner, {});
    const otherSession = await createMemberSession(store, other, {});
    await store.close();
    const preload = path.join(dataDir, 'r2-preload.mjs');
    await writeFile(
      preload,
      `
    import { getPatcher } from ${JSON.stringify(import.meta.resolve('webdav'))};
    getPatcher().patch('fetch', (...args) => globalThis.fetch(...args));
    const authorization = 'Basic ' + Buffer.from('synthetic-pikpak-user:synthetic-pikpak-secret').toString('base64');
    const missingR2 = new Set(['pikpak', 'old_snapshot', 'cold_corrupt']);
    globalThis.fetch = async (address, options) => {
      const url = new URL(address);
      if (url.hostname === 'dav.pikpak.test') {
        if (options.method !== 'GET' || new Headers(options.headers).get('authorization') !== authorization) {
          throw new Error('Unexpected WebDAV request');
        }
        if (!['/archive/${sha256}.br', '/archive/${corruptPikPakHash}.br'].includes(url.pathname)) {
          return new Response(null, { status: 404 });
        }
        return new Response(Buffer.from('${compressed.toString('base64')}', 'base64'));
      }
      if (url.hostname !== '${'a'.repeat(32)}.r2.cloudflarestorage.com' || options.method !== 'GET') {
        throw new Error('Unexpected external request');
      }
      const id = url.pathname.split('/').at(-2);
      if (missingR2.has(id)) {
        return new Response(null, { status: 404 });
      }
      const bytes = Buffer.from('${png.toString('base64')}', 'base64');
      if (id === 'corrupt') {
        bytes[0] ^= 1;
      }
      return new Response(bytes, { headers: { 'content-length': String(bytes.length) } });
    };`,
    );
    const socket = http.createServer();
    socket.listen(0, '127.0.0.1');
    await once(socket, 'listening');
    const port = socket.address().port;
    await new Promise((resolve) => socket.close(resolve));
    const base = `http://127.0.0.1:${port}`;
    const env = Object.fromEntries(
      ['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE']
        .filter((key) => process.env[key])
        .map((key) => [key, process.env[key]]),
    );
    const child = spawn(
      process.execPath,
      ['--import', pathToFileURL(preload).href, 'server/index.ts'],
      {
        cwd: fileURLToPath(new URL('../../', import.meta.url)),
        windowsHide: true,
        stdio: ['ignore', 'pipe', 'pipe'],
        env: {
          ...env,
          DATA_DIR: dataDir,
          ADMIN_TOKEN: 'synthetic-r2-admin-12345678901234567890',
          HOST: '127.0.0.1',
          PORT: String(port),
          PUBLIC_BASE_URL: base,
          R2_ACCOUNT_ID: 'a'.repeat(32),
          R2_BUCKET: 'synthetic-images',
          R2_ACCESS_KEY_ID: 'synthetic-access',
          R2_SECRET_ACCESS_KEY: 'synthetic-secret',
          PIKPAK_WEBDAV_URL: archiveUrl,
          PIKPAK_WEBDAV_USERNAME: 'synthetic-pikpak-user',
          PIKPAK_WEBDAV_PASSWORD: 'synthetic-pikpak-secret',
        },
      },
    );
    const exit = once(child, 'exit');
    let output = '';
    child.stdout.on('data', (chunk) => {
      output += chunk;
    });
    child.stderr.on('data', (chunk) => {
      output += chunk;
    });
    t.after(async () => {
      if (child.exitCode === null && child.signalCode === null) {
        child.kill('SIGKILL');
      }
      await exit;
    });
    const deadline = Date.now() + 15000;
    while (!output.includes('Nai2API listening')) {
      assert.equal(child.exitCode, null, 'synthetic runtime must start');
      assert.ok(Date.now() < deadline, 'runtime startup deadline');
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    const request = (url, headers = {}) =>
      fetch(new URL(url, base), {
        headers,
        signal: AbortSignal.timeout(3000),
      });
    const bearer = { authorization: `Bearer ${token}` };
    const ownerCookie = { cookie: `nai_member_session=${ownerSession.token}` };
    const otherCookie = { cookie: `nai_member_session=${otherSession.token}` };
    for (const id of ['local', 'cloud', 'pikpak', 'old_snapshot']) {
      const view = await request(`/api/jobs/job_${id}`, bearer);
      assert.equal(view.status, 200);
      const body = await view.json();
      assert.ok(!JSON.stringify(body).includes('cloudflarestorage.com'));
      assert.ok(!JSON.stringify(body).includes('synthetic-secret'));
      assert.ok(!JSON.stringify(body).includes('synthetic-pikpak-secret'));
      assert.ok(!JSON.stringify(body).includes('dav.pikpak.test'));
      const memberContent = `/api/member/jobs/job_${id}/content`;
      for (const response of [
        await request(body.imageUrl),
        await request(`/api/jobs/job_${id}/content`, bearer),
        await request(memberContent, ownerCookie),
      ]) {
        assert.equal(response.status, 200);
        assert.match(response.headers.get('content-type'), /image\/png/);
        assert.deepEqual(Buffer.from(await response.arrayBuffer()), png);
      }
      assert.equal((await request(`/api/images/${id}/content`)).status, 401);
      assert.equal((await request(memberContent)).status, 401);
      assert.equal((await request(memberContent, otherCookie)).status, 404);
      assert.equal(
        (await request(`/api/jobs/job_${id}/content`, { authorization: `Bearer ${otherToken}` }))
          .status,
        403,
      );
      const tampered = new URL(body.imageUrl, base);
      tampered.searchParams.set('signature', 'invalid');
      assert.equal((await request(tampered)).status, 401);
    }
    for (const id of ['corrupt', 'cold_corrupt']) {
      const failure = await request(`/api/jobs/job_${id}/content`, bearer);
      assert.equal(failure.status, 503);
      assert.doesNotMatch(await failure.text(), /synthetic-(pikpak-)?secret|dav\.pikpak\.test/);
    }
    const missing = await request('/api/jobs/job_missing/content', bearer);
    assert.equal(missing.status, 500, 'retain the existing local storage failure contract');
    assert.ok(!(await missing.text()).includes(dataDir), 'filesystem paths stay private');
  },
);
