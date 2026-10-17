import { createHash, randomBytes } from 'node:crypto';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { loginMember } from './member-auth.ts';

interface DiscordGrant { accessToken: string; refreshToken: string; expiresAt: number; id: string; guildId: string }
export interface DiscordUser extends Awaited<ReturnType<typeof loginMember>> {
  note?: string;
  discord?: Partial<DiscordGrant> & { checkedAt?: number; member?: boolean };
  discordInvite?: { hash: string; expiresAt: number };
}
export interface DiscordDependencies {
  env?: NodeJS.ProcessEnv;
  discordFetch?: typeof fetch;
  store: {
    update<T>(mutator: (db: { users: DiscordUser[] }) => T | Promise<T>, options: { persist: false } | { collections: ['users']; immediate: true }): Promise<T>;
  };
}
interface DiscordConfig { clientId: string; secret: string; guildId: string; origin: string; redirectUri: string }
interface PendingLogin { inviteHash: string | null; expiresAt: number; clientId: string; guildId: string; redirectUri: string }
type DiscordRequest = Pick<IncomingMessage, 'headers' | 'method'>;
type DiscordResponse = Pick<ServerResponse, 'setHeader'>;

const states = new Map<string, PendingLogin>(), checks = new Map<string, Promise<void>>();
const callbackPath = '/api/member/discord/callback';
const cookieName = 'nai_discord_state';
const apiBase = 'https://discord.com/api/v10';
const scope = 'identify guilds.members.read';
const fail: (message: string, statusCode?: number) => never = (message, statusCode = 403) => { throw Object.assign(new Error(message), { statusCode }); };
const hash = (value: string): string => createHash('sha256').update(value).digest('hex');
const snowflake = (value: unknown): value is string => typeof value === 'string' && /^[0-9]{17,20}$/.test(value);
const envOf = (deps: DiscordDependencies): NodeJS.ProcessEnv => deps.env || process.env;

export function discordLoginEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return Boolean(env.DISCORD_GUILD_ID || env.DISCORD_CLIENT_ID || env.DISCORD_CLIENT_SECRET);
}

export function discordConfig(env: NodeJS.ProcessEnv = process.env): DiscordConfig | null {
  if (!discordLoginEnabled(env)) return null;
  let url: URL;
  try { url = new URL(env.PUBLIC_BASE_URL!); } catch { fail('Discord 登录需要配置 HTTPS PUBLIC_BASE_URL', 503); }
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash || !['/', '/v1', '/v1/'].includes(url.pathname)) fail('Discord 登录需要配置 HTTPS 站点根地址', 503);
  if (!snowflake(env.DISCORD_GUILD_ID) || !snowflake(env.DISCORD_CLIENT_ID) || typeof env.DISCORD_CLIENT_SECRET !== 'string' || env.DISCORD_CLIENT_SECRET.length < 16) fail('Discord 登录尚未配置完成，请联系管理员', 503);
  return { clientId: env.DISCORD_CLIENT_ID, secret: env.DISCORD_CLIENT_SECRET, guildId: env.DISCORD_GUILD_ID, origin: url.origin, redirectUri: url.origin + callbackPath };
}

export function createDiscordInvite(db: { users: DiscordUser[] }, userId: unknown, now = Date.now()) {
  const user = db.users.find(u => u.id === userId && u.enabled !== false);
  if (!user) fail('成员不存在或已停用', 404);
  if (user.discord?.id) fail('成员已绑定 Discord，无需邀请码', 409);
  const code = randomBytes(24).toString('base64url'), expiresAt = now + 7 * 86400000;
  user.discordInvite = { hash: hash(code), expiresAt };
  return { code, expiresAt };
}

export function assertDiscordBound(user: DiscordUser, env: NodeJS.ProcessEnv = process.env): void {
  if (discordLoginEnabled(env) && (!user.discord?.id || user.discord.guildId !== env.DISCORD_GUILD_ID)) fail('请先使用邀请码完成 Discord 登录');
}

function stateCookie(value: string, maxAge: number): string {
  return `${cookieName}=${value}; Path=/api/member/discord; HttpOnly; Secure; SameSite=Lax; Max-Age=${maxAge}`;
}

export async function beginDiscordLogin(req: DiscordRequest, res: DiscordResponse, body: { invite?: unknown }, deps: DiscordDependencies) {
  const config = discordConfig(envOf(deps));
  if (!config) fail('Discord 登录未启用', 404);
  if (req.headers.host !== new URL(config.origin).host) fail('请从配置的主域名登录');
  if (body.invite !== undefined && (typeof body.invite !== 'string' || !/^[A-Za-z0-9_-]{32}$/.test(body.invite.trim()))) fail('邀请码无效或已过期');
  const now = Date.now(), inviteHash = body.invite ? hash(body.invite.trim()) : null;
  if (inviteHash) await deps.store.update(db => {
    if (!db.users.some(u => u.enabled !== false && !u.discord?.id && u.discordInvite?.hash === inviteHash && u.discordInvite.expiresAt > now)) fail('邀请码无效或已过期');
  }, { persist: false });
  for (const [key, value] of states) if (value.expiresAt <= now) states.delete(key);
  if (states.size >= 1000) fail('登录请求过多，请稍后重试', 429);
  const state = randomBytes(32).toString('base64url');
  states.set(state, { inviteHash, expiresAt: now + 5 * 60000, clientId: config.clientId, guildId: config.guildId, redirectUri: config.redirectUri });
  res.setHeader('Set-Cookie', stateCookie(state, 300));
  const url = new URL('https://discord.com/oauth2/authorize');
  url.search = new URLSearchParams({ client_id: config.clientId, response_type: 'code', scope, redirect_uri: config.redirectUri, state }).toString();
  return { authorizationUrl: url.href };
}

async function discordRequest(path: string, options: RequestInit, deps: DiscordDependencies): Promise<unknown> {
  let response: Response;
  try { response = await (deps.discordFetch || fetch)(apiBase + path, { ...options, signal: AbortSignal.timeout(10000), redirect: 'error' }); }
  catch { fail('Discord 暂时无法连接，请稍后重试', 503); }
  if (response.status === 401 || response.status === 400 && path === '/oauth2/token') fail('Discord 授权已失效，请重新登录', 401);
  if (response.status === 403 || response.status === 404) fail('无法验证指定 Discord 服务器成员资格');
  if (!response.ok) fail('Discord 验证暂不可用，请稍后重试', 503);
  try { return await response.json(); } catch { fail('Discord 返回了无效响应', 503); }
}

async function exchangeToken(config: DiscordConfig, parameters: Record<string, string>, deps: DiscordDependencies) {
  const value = await discordRequest('/oauth2/token', { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ client_id: config.clientId, client_secret: config.secret, ...parameters }) }, deps) as Record<string, unknown>;
  // 保留原字段访问和校验顺序；类型收窄不改变畸形响应原有的异常路径。
  if (typeof value.access_token !== 'string' || !value.access_token || typeof value.refresh_token !== 'string' || !value.refresh_token || (value.token_type as string | null | undefined)?.toLowerCase() !== 'bearer' || !Number.isFinite(value.expires_in) || (value.expires_in as number) <= 0 || !scope.split(' ').every(s => String(value.scope || '').split(' ').includes(s))) fail('Discord 未授予所需的身份与成员权限');
  return { accessToken: value.access_token, refreshToken: value.refresh_token, expiresAt: Date.now() + (value.expires_in as number) * 1000 };
}

async function guildMember(config: DiscordConfig, grant: Partial<Pick<DiscordGrant, 'accessToken' | 'id'>>, deps: DiscordDependencies): Promise<void> {
  const member = await discordRequest(`/users/@me/guilds/${config.guildId}/member`, { headers: { authorization: `Bearer ${grant.accessToken}` } }, deps) as { pending?: unknown; user?: { id?: unknown } };
  if (member.pending === true || member.user?.id !== grant.id) fail('请先加入指定 Discord 服务器并完成成员验证');
}

export async function finishDiscordLogin(req: DiscordRequest, res: DiscordResponse, url: URL, deps: DiscordDependencies): Promise<DiscordUser> {
  const config = discordConfig(envOf(deps));
  if (!config) fail('Discord 登录未启用', 404);
  const state = url.searchParams.get('state'), pending = states.get(state!);
  const cookie = String(req.headers.cookie || '').split(';').map(s => s.trim()).find(s => s.startsWith(cookieName + '='))?.slice(cookieName.length + 1);
  if (req.method !== 'GET' || req.headers.host !== new URL(config.origin).host || url.searchParams.getAll('state').length !== 1 || !state || cookie !== state || !pending || pending.expiresAt <= Date.now() || pending.clientId !== config.clientId || pending.guildId !== config.guildId || pending.redirectUri !== config.redirectUri) fail('Discord 登录请求已失效，请返回首页重新登录', 400);
  states.delete(state);
  res.setHeader('Set-Cookie', stateCookie('', 0));
  if (url.searchParams.has('error')) fail('你已取消 Discord 授权，请返回首页重试', 400);
  const code = url.searchParams.get('code');
  if (url.searchParams.getAll('code').length !== 1 || !code || code.length > 4096) fail('Discord 授权码无效', 400);
  const token = await exchangeToken(config, { grant_type: 'authorization_code', code, redirect_uri: config.redirectUri }, deps);
  const profile = await discordRequest('/users/@me', { headers: { authorization: `Bearer ${token.accessToken}` } }, deps) as { id?: unknown; bot?: unknown };
  if (!snowflake(profile.id) || profile.bot) fail('Discord 用户身份无效');
  const grant = { ...token, id: profile.id, guildId: config.guildId };
  await guildMember(config, grant, deps);
  return deps.store.update(db => {
    const existing = db.users.find(u => u.discord?.id === profile.id);
    const invited = pending.inviteHash && db.users.find(u => u.discordInvite?.hash === pending.inviteHash && u.discordInvite.expiresAt > Date.now());
    if (pending.inviteHash && (!invited || invited.discord?.id || existing)) fail('邀请码已使用、已撤销或 Discord 已绑定其他成员');
    const user = invited || existing;
    if (!user || user.enabled === false) fail('此 Discord 尚未获邀，或成员已停用');
    user.discord = { ...grant, checkedAt: Date.now(), member: true };
    user.memberAuth = { username: user.memberAuth?.username || user.note || user.id, version: randomBytes(16).toString('hex') };
    delete user.discordInvite;
    user.updatedAt = new Date().toISOString();
    return user;
  }, { collections: ['users'], immediate: true });
}

// 页面与 Key 调用共用验证，最多缓存五分钟；不需要机器人或后台轮询。
export async function requireDiscordMembership(user: DiscordUser, deps: DiscordDependencies): Promise<void> {
  const env = envOf(deps);
  if (!discordLoginEnabled(env)) return;
  assertDiscordBound(user, env);
  // 上面的启用/绑定检查保证 config 和 discord 存在；缺失时间仍按原比较得到 false。
  const config = discordConfig(env)!, original = user.discord!;
  if (original.checkedAt! > Date.now() - 5 * 60000 && original.checkedAt! <= Date.now()) {
    if (original.member === true) return;
    fail('你已不在指定 Discord 服务器，请重新登录验证');
  }
  const key = `${config.clientId}:${config.guildId}:${user.id}`;
  if (checks.has(key)) return checks.get(key);
  const pending = (async () => {
    let grant = { ...original }, membershipError: unknown;
    if (grant.expiresAt! <= Date.now() + 30000) {
      grant = { ...grant, ...await exchangeToken(config, { grant_type: 'refresh_token', refresh_token: grant.refreshToken! }, deps) };
      // 刷新令牌可能轮换，先持久化，避免后续成员查询故障丢失新的授权。
      await deps.store.update(db => {
        const current = db.users.find(u => u.id === user.id && u.enabled !== false);
        if (!current || current.discord?.accessToken !== original.accessToken) fail('成员认证状态已变化，请重试', 401);
        current.discord = { ...grant };
      }, { collections: ['users'], immediate: true });
    }
    try { await guildMember(config, grant, deps); }
    catch (error) { if ((error as { statusCode?: number }).statusCode !== 403) throw error; membershipError = error; }
    await deps.store.update(db => {
      const current = db.users.find(u => u.id === user.id && u.enabled !== false);
      if (!current || current.discord?.accessToken !== grant.accessToken) fail('成员认证状态已变化，请重试', 401);
      current.discord = { ...grant, checkedAt: Date.now(), member: !membershipError };
      if (membershipError) current.memberAuth!.version = randomBytes(16).toString('hex');
    }, { collections: ['users'], immediate: true });
    if (membershipError) throw membershipError;
  })();
  checks.set(key, pending);
  try { await pending; } finally { checks.delete(key); }
}
