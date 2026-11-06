// 共享模式仅开放成员、管理与生图 API；门户由 Nuxt 提供。
const routes = new Map([
  ['/api/health', ['GET', 'HEAD']],
  ['/api/member/login-options', ['GET']],
  ['/api/member/discord/start', ['POST']],
  ['/api/member/discord/callback', ['GET']],
  ['/api/member/owner/member-management', ['GET', 'POST']],
  ['/api/member/owner/summary', ['GET']],
  ['/api/member/owner/statistics', ['GET']],
  ['/api/member/owner/accounts/quota', ['POST']],
  ['/api/member/generation/capabilities', ['GET']],
  ['/api/member/generation/quote', ['POST']],
  ['/api/member/generation/requests', ['POST']],
  ['/api/me/quota', ['GET']],
  ['/v1/models', ['GET']],
  ['/user/subscription', ['GET']],
  ['/v1/user/subscription', ['GET']],
  ['/ai/user/subscription', ['GET']],
  ['/v1/ai/user/subscription', ['GET']],
  ['/ai/generate-image-stream', ['POST']],
  ['/v1/ai/generate-image-stream', ['POST']],
  ['/v1/chat/completions', ['POST']],
  ['/ai/generate-image', ['POST']],
  ['/v1/ai/generate-image', ['POST']],
  ['/ai/encode-vibe', ['POST']],
  ['/v1/ai/encode-vibe', ['POST']],
  ['/ai/upscale', ['POST']],
  ['/v1/ai/upscale', ['POST']],
  ['/generate', ['GET']],
  ['/api/jobs', ['POST']],
  ['/api/jobs/events', ['GET']],
  ['/api/admin/member-management', ['GET', 'POST']],
  ['/api/admin/summary', ['GET']],
  ['/api/admin/ping', ['GET']],
  ['/api/admin/accounts', ['POST', 'PATCH']],
  ['/api/admin/accounts/quota', ['POST']],
  ['/api/admin/fair-quota', ['POST']],
]);
export function sharedSurface(pathname: string, method = 'GET'): { allowed: boolean; pathname: string } {
  let methods = routes.get(pathname);
  if (/^\/api\/member\/(login|logout)$/.test(pathname)) methods = ['POST'];
  if (/^\/api\/member\/(me|key|history|heatmap|shared-status|shared-events)$/.test(pathname)) methods = ['GET'];
  if (/^\/api\/(?:member\/)?requests\/[^/]+$/.test(pathname)) methods = ['GET'];
  if (/^\/api\/(?:member\/)?requests\/[^/]+\/cancel$/.test(pathname)) methods = ['POST'];
  if (/^\/api\/member\/requests\/[^/]+\/(?:events|input|inputs\/[^/]+)$/.test(pathname)) methods = ['GET'];
  if (/^\/api\/member\/jobs\/[^/]+\/content$/.test(pathname)) methods = ['GET'];
  if (/^\/api\/jobs\/[^/]+(?:\/content)?$/.test(pathname)) methods ||= ['GET'];
  if (/^\/api\/images\/[^/]+\/content$/.test(pathname)) methods = ['GET'];
  if (/^\/api\/admin\/accounts\/[^/]+$/.test(pathname)) methods ||= ['PATCH'];
  if (/^\/api\/member\/owner\/accounts\/[^/]+$/.test(pathname)) methods ||= ['PATCH'];
  return { allowed: Boolean(methods && (methods.includes(method) || method === 'OPTIONS')), pathname };
}
