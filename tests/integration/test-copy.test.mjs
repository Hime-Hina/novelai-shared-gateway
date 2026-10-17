import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { mkdtemp, mkdir, readFile, readdir, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { JsonStore, sqlRecord } from '../../server/store.js';
import { configureFairQuota, syncFairQuota, reserveFairQuota, settleFairQuota } from '../../server/fair-quota.ts';
import { setMemberCredentials, loginMember } from '../../server/member-auth.ts';
import { fetchNovelAiAccountQuota } from '../../server/providers.ts';
import { createTestCopy, TEST_COPY_UPSTREAM_TOKEN } from '../../scripts/create-test-copy.mjs';

const now = 1800000000000;
const privatePrompt = 'Private prompt and member details stay in the protected test copy.';
const secrets = {
  pst: 'pst-production-secret-synthetic-fixture', api: 'STA1N-production-api-synthetic-fixture',
  password: 'production-password-synthetic-fixture', access: 'discord-access-synthetic-secret', refresh: 'discord-refresh-synthetic-secret',
  proxy: 'socks5://private-user:private-password@proxy.invalid:1080', prompt: 'prompt-api-synthetic-secret',
  promptUrl: 'https://prompt.invalid/v1', publicUrl: 'https://production.invalid', card: 'card-code-synthetic-secret',
  shadow: 'STA1N-shadow-only-cold-synthetic-secret', deleted: 'deleted-page-synthetic-secret', session: 'a'.repeat(64)
};
const digest = buffer => createHash('sha256').update(buffer).digest('hex');
const request = { model: 'nai-diffusion-5-full', width: 1024, height: 1024, steps: 23, prompt: privatePrompt };
function save(db, table, value, order) {
  const row = sqlRecord(table, value, order, JSON.stringify(value)), columns = Object.keys(row);
  db.prepare(`INSERT INTO ${table} (${columns.join(',')}) VALUES (${columns.map(name => '@' + name).join(',')})`).run(row);
}
function all(db, table) { return db.prepare(`SELECT data FROM ${table} ORDER BY order_value DESC`).all().map(row => JSON.parse(row.data)); }
async function fixture(t, resource = 'v5') {
  const root = await mkdtemp(path.join(tmpdir(), 'nai-test-copy-'));
  const sourceDir = path.join(root, 'original'); await mkdir(sourceDir);
  const store = new JsonStore(sourceDir); await store.init();
  const state = {
    settings: store.db.settings,
    users: ['a', 'b', 'c', 'd'].map((id, index) => ({ id, token: index ? `STA1N-${id}-synthetic-fixture` : secrets.api,
      enabled: true, note: `Original member ${id}`, balance: 100,
      discord: { id: `original-discord-${id}`, guildId: 'original-guild', accessToken: secrets.access, refreshToken: secrets.refresh },
      discordInvite: { hash: 'old-invitation-hash-synthetic', expiresAt: now + 60000 } })),
    accounts: [{ id: 'opus', token: secrets.pst, name: 'Original upstream account', proxyUrl: secrets.proxy, socksProxy: secrets.proxy,
      enabled: true, inFlight: 2, quotaTier: 3, subscriptionActive: true, quotaFixed: resource === 'purchased' ? 0 : 10000, quotaPurchased: 1000,
      v5UsagePercent: 80, quotaCheckedAt: now }], jobs: [], images: [], ledger: [], cards: []
  };
  for (const user of state.users) await setMemberCredentials(state, user.id, { username: `member_${user.id}`, password: secrets.password });
  state.users[3].enabled = false;
  const oldAuth = structuredClone(state.users.map(user => user.memberAuth));
  state.settings.promptApi = { apiKey: secrets.prompt, baseUrl: secrets.promptUrl, model: 'preserved-model' };
  state.settings.publicBaseUrl = secrets.publicUrl;
  configureFairQuota(state, state.users.map(user => user.id)); syncFairQuota(state, now);
  if (resource === 'fallback') { state.settings.fairQuota.autoAnlasFallback = true; state.settings.fairQuota.balances.a.v5Available = 0; }
  const expectedBalances = structuredClone(state.settings.fairQuota.balances);
  const job = (id, index) => ({ id, userToken: state.users[index].token, accountId: 'opus', status: 'running',
    createdAt: new Date(now).toISOString(), request: { ...request }, cost: 1 });
  const running = job('running', 0); state.jobs.push(running);
  if (resource === 'free') running.request.model = 'nai-diffusion-4-5-full';
  if (['fixed', 'purchased'].includes(resource)) Object.assign(running.request, { width: 1536, allowPurchasedAnlas: true });
  reserveFairQuota(state, state.users[0], running, state.accounts[0], now);
  running.upstreamStartedAt = new Date(now).toISOString();
  running.resultPending = { imageId: 'pending-result', file: 'images/pending-result.webp', attemptId: running.generationAttempt.id,
    operation: 'generate', mimeType: 'image/webp', byteLength: 34, sha256: 'b'.repeat(64) };
  const settled = job('settled-stale-hot', 1); state.jobs.push(settled);
  reserveFairQuota(state, state.users[1], settled, state.accounts[0], now);
  const staleCharge = structuredClone(settled.fairCharge);
  settleFairQuota(state, settled, true, now + 1);
  expectedBalances.b = structuredClone(state.settings.fairQuota.balances.b);
  const review = job('review-cold', 2); state.jobs.push(review);
  reserveFairQuota(state, state.users[2], review, state.accounts[0], now);
  review.upstreamStartedAt = new Date(now).toISOString();
  settleFairQuota(state, review, false, now + 1); review.status = 'failed';
  await store.write(state); await store.close();
  const source = path.join(sourceDir, 'library.sqlite'), db = new Database(source);
  db.pragma('journal_mode = DELETE'); db.pragma('secure_delete = OFF');
  const settings = JSON.parse(db.prepare("SELECT value FROM app_settings WHERE key='settings'").get().value);
  settings.fairQuota.reservations['settled-stale-hot'] = staleCharge;
  db.prepare("UPDATE app_settings SET value=? WHERE key='settings'").run(JSON.stringify(settings));
  const settledRow = JSON.parse(db.prepare("SELECT data FROM jobs WHERE id='settled-stale-hot'").get().data);
  settledRow.fairCharge = staleCharge;
  db.prepare("UPDATE jobs SET data=? WHERE id='settled-stale-hot'").run(JSON.stringify(settledRow));
  const payload = JSON.stringify({ ...request, debug: `Bearer ${secrets.api}; ${secrets.access}; ${secrets.pst}` });
  db.prepare('INSERT INTO generation_requests VALUES (?,?,?,?,?,?,?)').run('request-cold', 'a', 'novelai-native', 'generate', new Date(now).toISOString(), Buffer.byteLength(payload), payload);
  save(db, 'jobs', { id: 'pending-child', userToken: secrets.api, status: 'pending', cost: 0, requestId: 'request-cold',
    sampleIndex: 0, createdAt: new Date(now).toISOString(), request: { model: request.model } }, 1);
  db.prepare('INSERT INTO generation_idempotency VALUES (?,?,?,?,?,?,?,?)').run('a', 'generate', 'c'.repeat(64), 'd'.repeat(64),
    JSON.stringify({ defaultArtist: privatePrompt, defaultNegative: secrets.refresh }), 'pending-child', 'request-cold', new Date(now).toISOString());
  const charge = JSON.parse(db.prepare("SELECT charge FROM fair_quota_settlements WHERE job_id='review-cold'").get().charge);
  charge.debug = secrets.pst;
  db.prepare("UPDATE fair_quota_settlements SET charge=? WHERE job_id='review-cold'").run(JSON.stringify(charge));
  db.prepare('INSERT INTO member_sessions VALUES (?,?,?,?,?,?)').run(secrets.session, 'a', oldAuth[0].version, now, now + 100000, 'original-owner-binding');
  db.prepare('INSERT INTO app_meta VALUES (?,?)').run('member_session_owner', JSON.stringify({ ownerUserId: 'a', guildId: 'original-guild' }));
  db.prepare('INSERT INTO app_meta VALUES (?,?)').run('diagnostic', `old ${secrets.access}`);
  db.prepare('INSERT INTO app_records VALUES (?,?,?,?)').run('users', 'legacy-copy', 1, JSON.stringify({ id: 'legacy-copy', token: secrets.api, note: privatePrompt,
    memberAuth: oldAuth[0], discord: { accessToken: secrets.access, refreshToken: secrets.refresh } }));
  save(db, 'cards', { id: 'card', code: secrets.card, token: secrets.api, enabled: true }, 1);
  if (resource === 'v5') db.transaction(() => {
    for (let index = 0; index < 3001; index++) save(db, 'jobs', { id: `bulk-${index}`, userToken: secrets.api,
      status: 'done', createdAt: '2025-02-01T00:00:00.000Z', request: { model: request.model } }, -index - 1);
  })();
  save(db, 'jobs', { id: 'history-cold', userToken: secrets.api, status: 'done', request: { ...request }, imageId: 'image-cold',
    errorDetail: `old ${secrets.pst}`, createdAt: '2025-01-01T00:00:00.000Z' }, -100000);
  save(db, 'images', { id: 'image-cold', token: secrets.api, accountId: 'opus', prompt: privatePrompt,
    file: 'images/image-cold.webp', mimeType: 'image/webp', width: 2, height: 2 }, -100000);
  save(db, 'images', { id: 'image-missing', token: secrets.api, file: 'images/missing.png', mimeType: 'image/png' }, -100001);
  save(db, 'ledger', { id: 'ledger-cold', token: secrets.api, type: 'usage', at: '2025-01-01T00:00:00.000Z', note: `${privatePrompt} ${secrets.pst}` }, -100000);
  db.prepare("UPDATE jobs SET token=?,search_text=search_text || ? WHERE id='history-cold'").run(secrets.shadow, secrets.shadow);
  save(db, 'ledger', { id: 'deleted', token: secrets.deleted, note: secrets.deleted.repeat(4000) }, -200000);
  db.prepare("DELETE FROM ledger WHERE id='deleted'").run();
  db.close();
  await mkdir(path.join(sourceDir, 'images'));
  const webp = await readFile(new URL('../fixtures/synthetic.webp', import.meta.url));
  await writeFile(path.join(sourceDir, 'images/image-cold.webp'), webp);
  await writeFile(path.join(sourceDir, 'images/pending-result.webp.part'), 'must not copy partial production work');
  const originalBytes = await readFile(source);
  assert.ok(originalBytes.includes(Buffer.from(secrets.deleted)), 'fixture retains a deleted secret in free pages');
  return { root, source, sourceDir, originalBytes, oldAuth, expectedBalances, webp };
}

test('离线全表副本旋转凭据、闭合预留并保留冷历史关系，源文件与原图不变', async t => {
  const f = await fixture(t), target = path.join(f.root, 'test-copy');
  const writer = new Database(f.source); t.after(() => writer.close());
  writer.pragma('journal_mode = WAL'); writer.pragma('wal_autocheckpoint = 0');
  writer.prepare('INSERT INTO app_meta VALUES (?,?)').run('wal_marker', 'committed-wal-only');
  const before = await readFile(f.source), beforeWal = await readFile(f.source + '-wal');
  const result = await createTestCopy({ source: f.source, target, imagesDir: path.join(f.sourceDir, 'images'), now: now + 2000 });
  assert.equal(digest(await readFile(f.source)), digest(before));
  assert.equal(digest(await readFile(f.source + '-wal')), digest(beforeWal));
  assert.deepEqual(await readFile(path.join(f.sourceDir, 'images/image-cold.webp')), f.webp);
  assert.equal(result.refunded, 1); assert.equal(result.reviewsClosed, 2); assert.equal(result.isolated, 4);
  assert.equal(result.imagesCopied, 1); assert.equal(result.imagesMissing, 1);
  for (const table of ['users', 'accounts', 'jobs', 'images', 'cards', 'app_records', 'generation_requests', 'generation_idempotency', 'fair_quota_settlements'])
    assert.equal(result.after[table], result.before[table] + (table === 'fair_quota_settlements' ? 1 : 0), table);
  assert.equal(result.after.member_sessions, 0);
  const db = new Database(path.join(target, 'library.sqlite'), { readonly: true }); t.after(() => db.close());
  assert.equal(db.pragma('integrity_check', { simple: true }), 'ok'); assert.deepEqual(db.pragma('foreign_key_check'), []);
  assert.equal(db.prepare("SELECT value FROM app_meta WHERE key='wal_marker'").get().value, 'committed-wal-only');
  const users = all(db, 'users'), jobs = all(db, 'jobs'), accounts = all(db, 'accounts');
  const credentials = JSON.parse(await readFile(path.join(target, 'test-credentials.json'), 'utf8')).accounts;
  for (const entry of credentials) {
    const user = users.find(user => user.id === entry.user_id);
    if (user.id === 'd') { assert.equal(user.enabled, false); await assert.rejects(loginMember({ users }, entry.username, entry.password)); }
    else assert.equal((await loginMember({ users }, entry.username, entry.password)).id, user.id);
    await assert.rejects(loginMember({ users }, entry.username, secrets.password));
    assert.equal(user.token, entry.api_key); assert.equal(user.acceptingJobs, false);
    assert.equal(user.discord, undefined); assert.equal(user.discordInvite, undefined);
    assert.equal(user.note, `Original member ${user.id}`);
  }
  assert.equal(accounts[0].token, TEST_COPY_UPSTREAM_TOKEN); assert.equal(accounts[0].proxyUrl, ''); assert.equal(accounts[0].inFlight, 0);
  const env = Object.fromEntries(['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP'].filter(key => process.env[key]).map(key => [key, process.env[key]]));
  const child = spawn(process.execPath, [fileURLToPath(new URL('../../scripts/mock-novelai.mjs', import.meta.url)), '--port', '0', '--delay-ms', '0'],
    { env, windowsHide: true, stdio: ['ignore', 'pipe', 'ignore'] });
  const exited = once(child, 'exit');
  t.after(async () => { if (child.exitCode === null && child.signalCode === null) child.kill(); await exited; });
  const lines = createInterface({ input: child.stdout });
  const [line] = await once(lines, 'line', { signal: AbortSignal.timeout(5000) }); lines.close();
  const ready = JSON.parse(line.slice('[mock-novelai] '.length)); assert.equal(ready.host, '127.0.0.1');
  const base = `http://127.0.0.1:${ready.port}`;
  const quota = await fetchNovelAiAccountQuota(accounts[0].token, { NOVELAI_API_URL: base, NOVELAI_ACCOUNT_API_URL: base });
  assert.equal(quota.tier, 3); assert.equal(quota.subscriptionActive, true); assert.equal(quota.fixed, 10000);
  assert.ok(jobs.every(job => !['queued', 'pending', 'running'].includes(job.status) && !job.resultPending && job.generationReview?.status !== 'pending'));
  const settings = JSON.parse(db.prepare("SELECT value FROM app_settings WHERE key='settings'").get().value);
  assert.deepEqual(settings.fairQuota.reservations, {}); assert.deepEqual(settings.fairQuota.balances, f.expectedBalances);
  assert.equal(settings.publicBaseUrl, ''); assert.equal(settings.promptApi.apiKey, ''); assert.equal(settings.promptApi.baseUrl, '');
  assert.equal(JSON.parse(db.prepare("SELECT charge FROM fair_quota_settlements WHERE job_id='settled-stale-hot'").get().charge).status, 'settled');
  const user = users.find(user => user.id === 'a'), history = jobs.find(job => job.id === 'history-cold');
  assert.equal(history.userToken, user.token); assert.equal(history.request.prompt, privatePrompt); assert.equal(history.imageId, 'image-cold');
  const image = all(db, 'images').find(image => image.id === history.imageId);
  assert.equal(image.token, user.token); assert.deepEqual(await readFile(path.join(target, image.file)), f.webp);
  const requestRow = db.prepare("SELECT * FROM generation_requests WHERE id='request-cold'").get();
  assert.equal(requestRow.member_id, 'a'); assert.equal(requestRow.payload_bytes, Buffer.byteLength(requestRow.payload));
  assert.equal(JSON.parse(requestRow.payload).prompt, privatePrompt);
  assert.deepEqual(db.prepare('SELECT member_id,job_id,request_id FROM generation_idempotency').get(), { member_id: 'a', job_id: 'pending-child', request_id: 'request-cold' });
  assert.equal(db.prepare("SELECT 1 FROM app_meta WHERE key='member_session_owner'").get(), undefined);
  assert.equal(all(db, 'cards')[0].enabled, false);
  const outputBytes = await readFile(path.join(target, 'library.sqlite'));
  for (const value of [...Object.values(secrets), ...f.oldAuth.flatMap(auth => [auth.hash, auth.salt, auth.version])])
    assert.equal(outputBytes.includes(Buffer.from(value)), false, 'no original credential in rows, shadow columns, cold tables or free pages');
  assert.deepEqual((await readdir(target)).sort(), ['images', 'library.sqlite', 'test-credentials.json']);
  assert.deepEqual(await readdir(path.join(target, 'images')), ['image-cold.webp']);
  if (process.platform !== 'win32') {
    assert.equal((await stat(target)).mode & 0o777, 0o700);
    assert.equal((await stat(path.join(target, 'test-credentials.json'))).mode & 0o777, 0o600);
    assert.equal((await stat(path.join(target, 'library.sqlite'))).mode & 0o777, 0o600);
  }
});

test('目标存在时拒绝覆盖；孤立预留与旧points活动任务保留私有工作副本且不交付', async t => {
  const f = await fixture(t), existing = path.join(f.root, 'existing');
  await mkdir(existing); await writeFile(path.join(existing, 'library.sqlite'), 'do not overwrite');
  await assert.rejects(createTestCopy({ source: f.source, target: existing }), { code: 'EEXIST' });
  assert.equal(await readFile(path.join(existing, 'library.sqlite'), 'utf8'), 'do not overwrite');
  for (const variant of ['orphan', 'legacy']) {
    const db = new Database(f.source);
    if (variant === 'orphan') {
      const settings = JSON.parse(db.prepare("SELECT value FROM app_settings WHERE key='settings'").get().value);
      settings.fairQuota.reservations.orphan = { ...settings.fairQuota.reservations.running };
      db.prepare("UPDATE app_settings SET value=? WHERE key='settings'").run(JSON.stringify(settings));
    } else {
      const settings = JSON.parse(db.prepare("SELECT value FROM app_settings WHERE key='settings'").get().value);
      delete settings.fairQuota.reservations.orphan;
      db.prepare("UPDATE app_settings SET value=? WHERE key='settings'").run(JSON.stringify(settings));
      save(db, 'jobs', { id: 'legacy', userToken: secrets.api, status: 'running', cost: 5 }, 999999);
    }
    db.close();
    const before = digest(await readFile(f.source)), target = path.join(f.root, variant);
    await assert.rejects(createTestCopy({ source: f.source, target }), { code: variant === 'orphan' ? 'orphan_reserved_charge' : 'legacy_points_refund_required' });
    assert.equal(digest(await readFile(f.source)), before);
    await assert.rejects(stat(path.join(target, 'library.sqlite')), { code: 'ENOENT' });
    const work = (await readdir(target)).find(name => name.startsWith('.work-'));
    assert.ok(work); assert.ok((await stat(path.join(target, work, 'source.sqlite'))).isFile());
  }
});

test('免费、订阅Anlas、购买Anlas和自动回退预留沿现有账务函数只退款一次', async t => {
  for (const resource of ['free', 'fixed', 'purchased', 'fallback']) await t.test(resource, async t => {
    const f = await fixture(t, resource), target = path.join(f.root, 'test-copy');
    const summary = await createTestCopy({ source: f.source, target, now: now + 2000 });
    assert.equal(summary.refunded, 1);
    const db = new Database(path.join(target, 'library.sqlite'), { readonly: true });
    try {
      const settings = JSON.parse(db.prepare("SELECT value FROM app_settings WHERE key='settings'").get().value);
      assert.deepEqual(settings.fairQuota.balances, f.expectedBalances); assert.deepEqual(settings.fairQuota.reservations, {});
      const charge = JSON.parse(db.prepare("SELECT charge FROM fair_quota_settlements WHERE job_id='running'").get().charge);
      assert.equal(charge.status, 'refunded');
      assert.equal(charge.resource, resource === 'free' ? 'free' : 'anlas');
      if (resource === 'purchased') { assert.equal(charge.fixed, 0); assert.ok(charge.purchased > 0); }
      if (resource === 'fallback') assert.equal(charge.billingMode, 'anlas_fallback');
      assert.equal(db.prepare("SELECT COUNT(*) AS n FROM ledger WHERE json_extract(data,'$.jobId')='running' AND source='generation_refunded'").get().n, 1);
    } finally { db.close(); }
    assert.equal(digest(await readFile(f.source)), digest(f.originalBytes));
  });
});
