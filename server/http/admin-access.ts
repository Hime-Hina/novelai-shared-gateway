import type { IncomingMessage } from 'node:http';

export function matchesAdminToken(
  req: Pick<IncomingMessage, 'headers'>,
  url: URL,
  adminToken: string,
  env: NodeJS.ProcessEnv = process.env,
) {
  const header = String(req.headers['x-admin-token'] || ''),
    query = env.PUBLIC_MODE === 'true' ? '' : String(url.searchParams.get('adminToken') || '');
  return Boolean(adminToken) && (header === adminToken || query === adminToken);
}

export function assertAdminToken(
  req: Pick<IncomingMessage, 'headers'>,
  url: URL,
  adminToken: string,
  env: NodeJS.ProcessEnv = process.env,
): void {
  if (matchesAdminToken(req, url, adminToken, env)) {
    return;
  }
  const suppliedToken = String(
    req.headers['x-admin-token'] || url.searchParams.get('adminToken') || '',
  ).trim();
  throw Object.assign(new Error(suppliedToken ? 'invalid token.' : 'admin token required.'), {
    statusCode: suppliedToken ? 401 : 403,
  });
}
