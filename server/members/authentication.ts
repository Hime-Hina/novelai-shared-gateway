import { createHash, randomBytes, scrypt as derive, timingSafeEqual } from 'node:crypto';
import { promisify } from 'node:util';

export interface MemberUser {
  id: string;
  enabled?: boolean;
  updatedAt?: string;
  memberAuth?: { username?: string; salt?: string; hash?: string; version?: string };
  discord?: { id?: string; guildId?: string };
}
interface MemberDatabase {
  users: MemberUser[];
}
export interface MemberSession {
  tokenHash: string;
  memberId: string;
  authEpoch: string;
  createdAt: number;
  expiresAt: number;
  ownerBinding: string | null;
}
export interface SessionOwner {
  ownerUserId: string;
  guildId: string;
}
export interface MemberSessionMutation {
  memberSession?: () => MemberSession | undefined;
  memberSessionExpiry?: () => { tokenHash: string; expiresAt: number } | undefined;
  memberSessionDelete?: string | (() => string | undefined);
  memberSessionOwner?: SessionOwner;
}
export interface MemberSessionTransaction {
  readMember(memberId: string): MemberUser | null;
  readSession(tokenHash: string): MemberSession | null;
  readOwner(): SessionOwner | null;
}
export interface MemberSessionStore {
  memberSessionTransaction<T>(
    mutate: (transaction: MemberSessionTransaction) => T,
    options: MemberSessionMutation,
  ): Promise<T>;
}
type OwnerUser = MemberUser & { enabled: true; discord: { id: string; guildId: string } };

// Node scrypt 的完成回调返回 Buffer；promisify 对重载函数只能推断 unknown。
const scrypt = promisify(derive) as (
  password: string,
  salt: string,
  keylen: number,
) => Promise<Buffer>;
const attempts = new Map<string, { count: number; expiresAt: number }>();
const memberTtl = 7 * 24 * 60 * 60 * 1000,
  ownerTtl = 24 * 60 * 60 * 1000;
const windowMs = 15 * 60 * 1000;
const fail: (statusCode: number, message: string) => never = (statusCode, message) => {
  throw Object.assign(new Error(message), { statusCode });
};
const normalized = (value: unknown): string =>
  String(value || '')
    .trim()
    .toLowerCase();

export async function setMemberCredentials(
  db: MemberDatabase,
  userId: string,
  input: { username?: unknown; password?: unknown },
) {
  const user = db.users.find((u) => u.id === userId && u.enabled === true);
  if (!user) {
    fail(404, '账户不存在或未启用');
  }
  const username = normalized(input.username);
  const password = input.password;
  if (!/^[a-z0-9][a-z0-9_.-]{2,39}$/.test(username)) {
    fail(400, '用户名需为 3–40 位字母、数字、下划线、点或短横线');
  }
  if (typeof password !== 'string' || password.length < 12 || password.length > 128) {
    fail(400, '密码需为 12–128 个字符');
  }
  if (db.users.some((u) => u.id !== userId && u.memberAuth?.username === username)) {
    fail(409, '用户名已存在');
  }
  const salt = randomBytes(24).toString('hex');
  const hash = (await scrypt(password, salt, 64)).toString('hex');
  user.memberAuth = { username, salt, hash, version: randomBytes(16).toString('hex') };
  user.updatedAt = new Date().toISOString();
  return { id: user.id, username };
}

function prune(map: typeof attempts, now: number): void {
  for (const [key, value] of map) {
    if (value.expiresAt <= now) {
      map.delete(key);
    }
  }
}

export function checkMemberLoginRate(username: unknown, remote: unknown, now = Date.now()): void {
  prune(attempts, now);
  const keys = [
    ...(username === null ? [] : [`user:${normalized(username).slice(0, 128)}`]),
    `remote:${String(remote).slice(0, 128)}`,
  ];
  const limits = username === null ? [120] : [10, 120];
  if (
    attempts.size >= 10000 ||
    keys.some((key, i) => (attempts.get(key)?.count || 0) >= limits[i])
  ) {
    fail(429, '登录尝试过多，请 15 分钟后重试');
  }
  // 对每次尝试计数，防止并发请求在密码校验完成前绕过限制。
  keys.forEach((key) => {
    const a = attempts.get(key) || { count: 0, expiresAt: now + windowMs };
    a.count++;
    attempts.set(key, a);
  });
}

export async function loginMember(
  db: MemberDatabase,
  username: unknown,
  password: unknown,
): Promise<MemberUser> {
  const user = db.users.find((u) => u.memberAuth?.username === normalized(username));
  const auth = user?.memberAuth;
  const candidate = typeof password === 'string' && password.length <= 128 ? password : '';
  // 不存在的账户也执行相同派生过程，避免通过响应时间枚举用户名。
  const hash = await scrypt(
    candidate,
    auth?.salt || '000000000000000000000000000000000000000000000000',
    64,
  );
  const expected =
    auth?.hash && /^[a-f0-9]{128}$/.test(auth.hash)
      ? Buffer.from(auth.hash, 'hex')
      : Buffer.alloc(64);
  // auth 只可能来自上面的 user；短路检查通过时 user 已存在。
  if (!timingSafeEqual(hash, expected) || !auth || user!.enabled !== true || !candidate) {
    fail(401, '用户名或密码错误');
  }
  return user!;
}

const sessionHash = (token: unknown): string | null =>
  typeof token === 'string' && /^[A-Za-z0-9_-]{43}$/.test(token)
    ? createHash('sha256').update(token).digest('hex')
    : null;
const ownerConfig = (env: NodeJS.ProcessEnv): SessionOwner => ({
  ownerUserId: env.OWNER_USER_ID || '',
  guildId: env.DISCORD_GUILD_ID || '',
});
const ownerBinding = (
  user: MemberUser | null | undefined,
  env: NodeJS.ProcessEnv,
): string | null =>
  isOwner(user, env) ? JSON.stringify([user.id, user.discord.id, env.DISCORD_GUILD_ID]) : null;

export async function reconcileMemberSessionOwner(
  store: MemberSessionStore,
  env: NodeJS.ProcessEnv = process.env,
): Promise<void> {
  await store.memberSessionTransaction(() => {}, {
    memberSessionOwner: ownerConfig(env),
  });
}

export async function createMemberSession(
  store: MemberSessionStore,
  userSnapshot: MemberUser,
  env: NodeJS.ProcessEnv = process.env,
  now?: number,
) {
  const token = randomBytes(32).toString('base64url');
  let row: MemberSession | undefined;
  return store.memberSessionTransaction(
    (transaction) => {
      const at = now ?? Date.now();
      // 密码/OAuth 校验后可能已被停用、重置或换绑，必须在签发事务内重新核对。
      const user = transaction.readMember(userSnapshot?.id);
      if (
        user?.enabled !== true ||
        !user.memberAuth?.version ||
        user.memberAuth.version !== userSnapshot.memberAuth?.version ||
        ownerBinding(user, env) !== ownerBinding(userSnapshot, env)
      ) {
        fail(401, '成员认证状态已变化，请重新登录');
      }
      const binding = ownerBinding(user, env),
        expiresAt = at + (binding ? ownerTtl : memberTtl);
      // 32 随机字节的 base64url 固定为 43 字符，必定满足 sessionHash 的现有格式检查。
      row = {
        tokenHash: sessionHash(token)!,
        memberId: user.id,
        authEpoch: user.memberAuth.version,
        createdAt: at,
        expiresAt,
        ownerBinding: binding,
      };
      return { token, expiresAt };
    },
    {
      memberSession: () => row,
      memberSessionOwner: ownerConfig(env),
    },
  );
}

export function authenticateMemberSession(
  store: MemberSessionStore,
  token: unknown,
  env: NodeJS.ProcessEnv = process.env,
  now?: number,
) {
  return checkMemberSession(store, token, env, now, false);
}

export function renewMemberSession(
  store: MemberSessionStore,
  token: unknown,
  env: NodeJS.ProcessEnv = process.env,
  now?: number,
) {
  return checkMemberSession(store, token, env, now, true);
}

async function checkMemberSession(
  store: MemberSessionStore,
  token: unknown,
  env: NodeJS.ProcessEnv,
  now: number | undefined,
  renew: boolean,
) {
  const tokenHash = sessionHash(token);
  if (!tokenHash) {
    return null;
  }
  const configured = ownerConfig(env);
  let revoke: string | undefined;
  let expiry: { tokenHash: string; expiresAt: number } | undefined;
  return store.memberSessionTransaction(
    (transaction) => {
      const at = now ?? Date.now();
      const session = transaction.readSession(tokenHash),
        previous = transaction.readOwner();
      if (!session) {
        return null;
      }
      const user = transaction.readMember(session.memberId);
      // 撤销发生在提交阶段；首次观察配置变化时也不能先放行旧/新 owner 的 Cookie。
      const ownerChanged =
        previous &&
        (previous.ownerUserId !== configured.ownerUserId ||
          previous.guildId !== configured.guildId);
      const affected =
        ownerChanged &&
        (session.ownerBinding !== null ||
          session.memberId === previous.ownerUserId ||
          session.memberId === configured.ownerUserId);
      if (
        session.expiresAt <= at ||
        user?.enabled !== true ||
        user.memberAuth?.version !== session.authEpoch ||
        affected ||
        session.ownerBinding !== ownerBinding(user, env)
      ) {
        revoke = tokenHash;
        return null;
      }
      const expiresAt = renew
        ? Math.max(session.expiresAt, at + (session.ownerBinding ? ownerTtl : memberTtl))
        : session.expiresAt;
      if (expiresAt > session.expiresAt) {
        expiry = { tokenHash, expiresAt };
      }
      return { user, expiresAt };
    },
    {
      memberSessionDelete: () => revoke,
      memberSessionExpiry: () => expiry,
      memberSessionOwner: configured,
    },
  );
}

export async function logoutMemberSession(
  store: MemberSessionStore,
  token: unknown,
): Promise<void> {
  const tokenHash = sessionHash(token);
  if (tokenHash) {
    await store.memberSessionTransaction(() => {}, {
      memberSessionDelete: tokenHash,
    });
  }
}

export function isOwner(
  user: MemberUser | null | undefined,
  env: NodeJS.ProcessEnv = process.env,
): user is OwnerUser {
  return Boolean(
    env.OWNER_USER_ID &&
    env.DISCORD_GUILD_ID &&
    user?.enabled === true &&
    user.id === env.OWNER_USER_ID &&
    user.discord?.id &&
    user.discord.guildId === env.DISCORD_GUILD_ID,
  );
}
