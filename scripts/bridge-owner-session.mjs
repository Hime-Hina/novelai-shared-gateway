// One-off deployment bridge: preserve a verified legacy Cookie, never mint a login or expose a route.
import { createHash } from 'node:crypto';
import { readFile, writeFile, open, unlink, stat, realpath } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { execFileSync } from 'node:child_process';
import http from 'node:http';
import { parseArgs, parseEnv } from 'node:util';
import { pathToFileURL, fileURLToPath } from 'node:url';
import path from 'node:path';

const fail = message => { throw new Error(message); };
const hash = value => createHash('sha256').update(value).digest('hex');
const binding = (user, env) => JSON.stringify([user.id, user.discord.id, env.DISCORD_GUILD_ID]);
function checkedOwner(user, env) {
  if (!env.OWNER_USER_ID || !env.DISCORD_GUILD_ID || user?.id !== env.OWNER_USER_ID || user.enabled !== true
    || !user.memberAuth?.version || !user.discord?.id || user.discord.guildId !== env.DISCORD_GUILD_ID) fail('Owner identity is not valid');
  return { id: user.id, version: user.memberAuth.version, binding: binding(user, env), username: user.memberAuth.username };
}

async function legacyResponse(url, options) {
  // Node fetch replaces Host; use the same native HTTP transport as the executor bridge.
  const response = await new Promise((resolve, reject) => http.get(url, options, resolve).once('error', reject));
  if (response.statusCode !== 200) { response.resume(); return { status: response.statusCode }; }
  const chunks = []; let bytes = 0;
  for await (const chunk of response) {
    bytes += chunk.length;
    if (bytes > 2 * 1024 * 1024) { response.destroy(); fail('Legacy identity response exceeded its limit'); }
    chunks.push(chunk);
  }
  return { status: 200, json: () => JSON.parse(Buffer.concat(chunks).toString('utf8')) };
}

export async function captureOwnerSession({ database, token, expiresAt, env, configHash, baseUrl = 'http://127.0.0.1:18081', now = Date.now(), requestImpl = legacyResponse }) {
  if (!/^[A-Za-z0-9_-]{43}$/.test(token) || !Number.isSafeInteger(expiresAt) || expiresAt <= now) fail('Legacy Cookie is absent or expired');
  const origin = new URL(env.PUBLIC_BASE_URL).origin, target = new URL(baseUrl);
  if (target.protocol !== 'http:' || target.hostname !== '127.0.0.1' || target.username || target.password) fail('Legacy validation must use loopback');
  const readOwner = () => checkedOwner(JSON.parse(database.prepare('SELECT data FROM users WHERE id=?').get(env.OWNER_USER_ID)?.data || 'null'), env);
  const before = readOwner();
  const response = await requestImpl(new URL('/api/member/me', target), {
    headers: { Host: new URL(origin).host, Origin: origin, Cookie: `nai_member_session=${token}` },
    signal: AbortSignal.timeout(15000)
  });
  if (response.status !== 200) fail('Legacy service did not authenticate this Cookie');
  const profile = await response.json(), after = readOwner();
  if (profile.owner !== true || profile.username !== before.username || JSON.stringify(before) !== JSON.stringify(after)) fail('Owner changed during legacy authentication');
  return { version: 1, capturedAt: now, expiresAt: Math.min(expiresAt, now + 12 * 3600000),
    tokenHash: hash(token), ownerId: after.id, authVersion: after.version, ownerBinding: after.binding, configHash };
}

export async function importOwnerSession({ store, receipt, env, configHash, claimFile, now = Date.now() }) {
  if (receipt.version !== 1 || !/^[a-f0-9]{64}$/.test(receipt.tokenHash) || receipt.configHash !== configHash
    || !Number.isSafeInteger(receipt.capturedAt) || receipt.capturedAt > now || now - receipt.capturedAt > 5 * 60000
    || !Number.isSafeInteger(receipt.expiresAt) || receipt.expiresAt <= now || receipt.expiresAt > receipt.capturedAt + 12 * 3600000) fail('Migration receipt is stale or invalid');
  let record;
  return store.update(async db => {
    const owner = checkedOwner(db.users.find(user => user.id === receipt.ownerId), env);
    if (owner.version !== receipt.authVersion || owner.binding !== receipt.ownerBinding) fail('Owner authorization changed before migration');
    const prior = store.readMemberSessionOwner();
    if (prior && (prior.ownerUserId !== env.OWNER_USER_ID || prior.guildId !== env.DISCORD_GUILD_ID)) fail('Owner configuration changed before migration');
    // Claim before the database write. A failed attempt stays claimed and needs inspection, not replay.
    let claim;
    try { claim = await open(claimFile, 'wx', 0o600); }
    catch (error) {
      if (error.code !== 'EEXIST') throw error;
      const existing = store.readMemberSession(receipt.tokenHash);
      if (existing?.user_id === receipt.ownerId && existing.auth_version === receipt.authVersion
        && existing.owner_binding === receipt.ownerBinding && existing.expires_at >= receipt.expiresAt) return { imported: false, alreadyPresent: true };
      fail('This migration was consumed; it cannot recreate a revoked session');
    }
    try { await claim.writeFile(JSON.stringify({ tokenHash: receipt.tokenHash, at: now })); await claim.sync(); }
    finally { await claim.close(); }
    record = { token_hash: receipt.tokenHash, user_id: receipt.ownerId, auth_version: receipt.authVersion,
      created_at: receipt.capturedAt, expires_at: receipt.expiresAt, owner_binding: receipt.ownerBinding };
    return { imported: true, expiresAt: record.expires_at };
  }, { collections: [], immediate: true, memberSession: () => record,
    memberSessionOwner: { ownerUserId: env.OWNER_USER_ID, guildId: env.DISCORD_GUILD_ID } });
}

async function main() {
  const prep = '/var/lib/novelai-shared-gateway-release-prep/dff5ee4';
  const { values, positionals } = parseArgs({ allowPositionals: true, options: {
    'data-dir': { type: 'string', default: '/var/lib/novelai-shared-gateway' },
    'module-root': { type: 'string', default: '/opt/novelai-shared-gateway/releases/dff5ee4cff44ad7c3c29134af9989da96071eaa1/executor/.output/server/node_modules/novelai-shared-gateway/server' },
    config: { type: 'string', default: '/etc/novelai-shared-gateway/gateway.env' },
    'cookie-file': { type: 'string', default: `${prep}/owner-cookie.json` },
    receipt: { type: 'string', default: `${prep}/owner-session.json` }
  } });
  if (positionals.length !== 1 || !['capture', 'import'].includes(positionals[0])) fail('Use capture or import');
  process.umask(0o077);
  const config = await readFile(values.config, 'utf8'), env = parseEnv(config), configHash = hash(config);
  const moduleFile = path.join(values['module-root'], 'store.js'), require = createRequire(pathToFileURL(moduleFile));
  if (positionals[0] === 'capture') {
    if ((await stat(values['cookie-file'])).mode & 0o077) fail('Cookie input must be private (0600)');
    const Database = require('better-sqlite3'), database = new Database(path.join(values['data-dir'], 'library.sqlite'), { readonly: true, fileMustExist: true });
    try {
      const cookie = JSON.parse(await readFile(values['cookie-file'], 'utf8'));
      const receipt = await captureOwnerSession({ database, ...cookie, env, configHash });
      await writeFile(values.receipt, JSON.stringify(receipt, null, 2), { flag: 'wx', mode: 0o600 });
      console.log(JSON.stringify({ captured: true, expiresAt: new Date(receipt.expiresAt).toISOString() }));
    } finally { database.close(); await unlink(values['cookie-file']); }
  } else {
    if (await realpath(values['data-dir']) === '/var/lib/novelai-shared-gateway') {
      for (const unit of ['novelai-gateway.service', 'novelai-gateway-web.service']) {
        const pid = execFileSync('systemctl', ['show', unit, '--value', '-p', 'MainPID'], { encoding: 'utf8' }).trim();
        if (pid !== '0') fail('Stop all production writers before importing');
      }
    }
    const { JsonStore } = await import(pathToFileURL(moduleFile));
    const store = new JsonStore(values['data-dir']);
    try {
      const receipt = JSON.parse(await readFile(values.receipt, 'utf8'));
      await store.init();
      console.log(JSON.stringify(await importOwnerSession({ store, receipt, env, configHash, claimFile: values.receipt + '.used' })));
    } finally { await store.close(); }
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => {
    const safe = /^(Owner |Legacy |Migration |This migration |Stop all production |Use capture|Cookie input)/.test(error.message);
    console.error('Owner session bridge failed:', safe ? error.message : error.name);
    process.exitCode = 1;
  });
}
