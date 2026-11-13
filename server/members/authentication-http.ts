import type { IncomingMessage, ServerResponse } from 'node:http';
import {
  checkMemberLoginRate,
  createMemberSession,
  renewMemberSession,
  logoutMemberSession,
  type MemberSessionStore,
  type MemberPasswordStore,
} from './authentication.ts';
import {
  discordLoginEnabled,
  discordConfig,
  beginDiscordLogin,
  finishDiscordLogin,
  type DiscordDependencies,
} from './discord-auth.ts';
import { clientIp } from '../http/client-ip.ts';
import {
  memberCookie as cookie,
  assertMemberSameOrigin as sameOrigin,
  readMemberJson as json,
  replyMemberJson as reply,
  setMemberCookie as setCookie,
} from './http-request.ts';

function error(statusCode: number, message: string): never {
  throw Object.assign(new Error(message), { statusCode });
}

export async function renewMemberCookie(
  req: IncomingMessage,
  res: ServerResponse,
  store: MemberSessionStore,
  env: NodeJS.ProcessEnv,
) {
  const session = await renewMemberSession(store, cookie(req), env);
  if (!session) {
    error(401, '登录已失效，请重新登录');
  }
  setCookie(req, res, cookie(req), session.expiresAt);
  return session;
}

export async function handleMemberAuthenticationRoutes(
  req: IncomingMessage,
  res: ServerResponse,
  url: URL,
  deps: DiscordDependencies & { store: MemberPasswordStore },
): Promise<boolean> {
  if (await handleMemberDiscordAuthenticationRoutes(req, res, url, deps)) {
    return true;
  }
  const env = deps.env || process.env,
    discordEnabled = discordLoginEnabled(env);
  if (url.pathname === '/api/member/login-options' && req.method === 'GET') {
    sameOrigin(req);
    let config;
    try {
      config = discordConfig(env);
    } catch {}
    reply(res, 200, {
      discord: discordEnabled,
      ready: Boolean(config),
      origin: config?.origin || null,
    });
    return true;
  }
  if (
    req.method !== 'POST' ||
    !['/api/member/login', '/api/member/logout'].includes(url.pathname)
  ) {
    return false;
  }
  sameOrigin(req);
  const store = deps.store;
  if (url.pathname === '/api/member/logout') {
    await logoutMemberSession(store, cookie(req));
    setCookie(req, res, '');
    reply(res, 200, { ok: true });
    return true;
  }
  if (discordLoginEnabled(env)) {
    error(403, '本站仅允许 Discord 登录');
  }
  const body = await json(req);
  checkMemberLoginRate(body.username, clientIp(req, env));
  const user = await store.loginMemberPassword(body.username, body.password);
  const session = await createMemberSession(store, user, env);
  setCookie(req, res, session.token, session.expiresAt);
  reply(res, 200, { ok: true, expiresAt: session.expiresAt });
  return true;
}

export async function handleMemberDiscordAuthenticationRoutes(
  req: IncomingMessage,
  res: ServerResponse,
  url: URL,
  deps: DiscordDependencies & { store: MemberSessionStore },
): Promise<boolean> {
  const env = deps.env || process.env;
  // OAuth 顶层回跳必然跨站，仅此 GET 路径改用一次性 state + 浏览器 Cookie 验证。
  if (url.pathname === '/api/member/discord/callback' && req.method === 'GET') {
    const user = await finishDiscordLogin(req, res, url, deps);
    const session = await createMemberSession(deps.store, user, env);
    // finishDiscordLogin 已确认配置并设置单个 state Cookie，保持两个 Cookie 的原顺序。
    setCookie(req, res, session.token, session.expiresAt, [res.getHeader('Set-Cookie') as string]);
    res.writeHead(303, {
      location: discordConfig(env)!.origin + '/',
      'cache-control': 'no-store',
      'referrer-policy': 'no-referrer',
    });
    res.end();
    return true;
  }
  if (url.pathname === '/api/member/discord/start' && req.method === 'POST') {
    sameOrigin(req);
    checkMemberLoginRate(null, clientIp(req, env));
    reply(res, 200, await beginDiscordLogin(req, res, await json(req), deps));
    return true;
  }
  return false;
}
