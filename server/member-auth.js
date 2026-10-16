import { createHash, randomBytes, scrypt as derive, timingSafeEqual } from 'node:crypto';
import { promisify } from 'node:util';
const scrypt = promisify(derive);
const attempts = new Map();
const memberTtl = 7 * 24 * 60 * 60 * 1000, ownerTtl = 12 * 60 * 60 * 1000;
const windowMs = 15 * 60 * 1000;
const fail = (statusCode, message) => { throw Object.assign(new Error(message), { statusCode }); };
const normalized = value => String(value || '').trim().toLowerCase();

export async function setMemberCredentials(db, userId, input) {
  const user = db.users.find(u => u.id === userId && u.enabled === true);
  if (!user) fail(404, '账户不存在或未启用');
  const username = normalized(input.username);
  const password = input.password;
  if (!/^[a-z0-9][a-z0-9_.-]{2,39}$/.test(username)) fail(400, '用户名需为 3–40 位字母、数字、下划线、点或短横线');
  if (typeof password !== 'string' || password.length < 12 || password.length > 128) fail(400, '密码需为 12–128 个字符');
  if (db.users.some(u => u.id !== userId && u.memberAuth?.username === username)) fail(409, '用户名已存在');
  const salt = randomBytes(24).toString('hex');
  const hash = (await scrypt(password, salt, 64)).toString('hex');
  user.memberAuth = { username, salt, hash, version: randomBytes(16).toString('hex') };
  user.updatedAt = new Date().toISOString();
  return { id: user.id, username };
}

function prune(map, now) {
  for (const [key, value] of map) if (value.expiresAt <= now) map.delete(key);
}

export function checkMemberLoginRate(username, remote, now = Date.now()) {
  prune(attempts, now);
  const keys = [...(username === null ? [] : [`user:${normalized(username).slice(0, 128)}`]), `remote:${String(remote).slice(0, 128)}`];
  const limits = username === null ? [120] : [10, 120];
  if (attempts.size >= 10000 || keys.some((key, i) => (attempts.get(key)?.count || 0) >= limits[i])) fail(429, '登录尝试过多，请 15 分钟后重试');
  // 对每次尝试计数，防止并发请求在密码校验完成前绕过限制。
  keys.forEach(key => { const a = attempts.get(key) || { count: 0, expiresAt: now + windowMs }; a.count++; attempts.set(key, a); });
}

export async function loginMember(db, username, password) {
  const user = db.users.find(u => u.memberAuth?.username === normalized(username));
  const auth = user?.memberAuth;
  const candidate = typeof password === 'string' && password.length <= 128 ? password : '';
  // 不存在的账户也执行相同派生过程，避免通过响应时间枚举用户名。
  const hash = await scrypt(candidate, auth?.salt || '000000000000000000000000000000000000000000000000', 64);
  const expected = auth?.hash && /^[a-f0-9]{128}$/.test(auth.hash) ? Buffer.from(auth.hash, 'hex') : Buffer.alloc(64);
  if (!timingSafeEqual(hash, expected) || !auth || user.enabled !== true || !candidate) fail(401, '用户名或密码错误');
  return user;
}

const sessionHash = token => typeof token === 'string' && /^[A-Za-z0-9_-]{43}$/.test(token)
  ? createHash('sha256').update(token).digest('hex') : null;
const ownerConfig = env => ({ ownerUserId: env.OWNER_USER_ID || '', guildId: env.DISCORD_GUILD_ID || '' });
const ownerBinding = (user, env) => isOwner(user, env) ? JSON.stringify([user.id, user.discord.id, env.DISCORD_GUILD_ID]) : null;

export async function reconcileMemberSessionOwner(store, env = process.env) {
  await store.update(() => {}, { collections: [], memberSessionOwner: ownerConfig(env), immediate: true });
}

export async function createMemberSession(store, userSnapshot, env = process.env, now) {
  const token = randomBytes(32).toString('base64url');
  let row;
  return store.update(db => {
    const at = now ?? Date.now();
    // 密码/OAuth 校验后可能已被停用、重置或换绑，必须在签发事务内重新核对。
    const user = db.users.find(user => user.id === userSnapshot?.id && user.enabled === true);
    if (!user?.memberAuth?.version || user.memberAuth.version !== userSnapshot.memberAuth?.version
      || ownerBinding(user, env) !== ownerBinding(userSnapshot, env)) fail(401, '成员认证状态已变化，请重新登录');
    const binding = ownerBinding(user, env), expiresAt = at + (binding ? ownerTtl : memberTtl);
    row = { token_hash: sessionHash(token), user_id: user.id, auth_version: user.memberAuth.version,
      created_at: at, expires_at: expiresAt, owner_binding: binding };
    return { token, expiresAt };
  }, { collections: [], memberSession: () => row, memberSessionOwner: ownerConfig(env), immediate: true });
}

export async function authenticateMemberSession(store, token, env = process.env, now) {
  const tokenHash = sessionHash(token);
  if (!tokenHash) return null;
  const configured = ownerConfig(env);
  let revoke;
  return store.update(db => {
    const at = now ?? Date.now();
    const session = store.readMemberSession(tokenHash), previous = store.readMemberSessionOwner();
    if (!session) return null;
    const user = db.users.find(user => user.id === session.user_id && user.enabled === true && user.memberAuth?.version === session.auth_version);
    // 撤销发生在提交阶段；首次观察配置变化时也不能先放行旧/新 owner 的 Cookie。
    const ownerChanged = previous && (previous.ownerUserId !== configured.ownerUserId || previous.guildId !== configured.guildId);
    const affected = ownerChanged && (session.owner_binding !== null || session.user_id === previous.ownerUserId || session.user_id === configured.ownerUserId);
    if (session.expires_at <= at || !user || affected || session.owner_binding !== ownerBinding(user, env)) {
      revoke = tokenHash; return null;
    }
    return { user, expiresAt: session.expires_at };
  }, { collections: [], memberSessionDelete: () => revoke, memberSessionOwner: configured, immediate: true });
}

export async function logoutMemberSession(store, token) {
  const tokenHash = sessionHash(token);
  if (tokenHash) await store.update(() => {}, { collections: [], memberSessionDelete: tokenHash, immediate: true });
}

export function isOwner(user, env = process.env) {
  return Boolean(env.OWNER_USER_ID && env.DISCORD_GUILD_ID && user?.enabled === true && user.id === env.OWNER_USER_ID && user.discord?.id && user.discord.guildId === env.DISCORD_GUILD_ID);
}
