import type { IncomingMessage } from 'node:http';

export const memberCookieName = 'nai_member_session';
function error(statusCode: number, message: string): never {
  throw Object.assign(new Error(message), { statusCode });
}
export function memberCookie(req: IncomingMessage): string {
  return (
    String(req.headers.cookie || '')
      .split(';')
      .map((s) => s.trim())
      .find((s) => s.startsWith(memberCookieName + '='))
      ?.slice(memberCookieName.length + 1) || ''
  );
}
export function assertMemberSameOrigin(req: IncomingMessage): void {
  const host = String(req.headers.host || '');
  const origin = req.headers.origin;
  const local = /^(localhost|127\.0\.0\.1)(:\d+)?$/.test(host);
  const expected = `${local ? 'http' : 'https'}://${host}`;
  if ((origin && origin !== expected) || req.headers['sec-fetch-site'] === 'cross-site') {
    throw Object.assign(new Error('此接口仅允许本站访问'), {
      statusCode: 403,
      code: 'origin_rejected',
    });
  }
  if (!['GET', 'HEAD'].includes(req.method!) && origin !== expected) {
    throw Object.assign(new Error('缺少本站 Origin'), { statusCode: 403, code: 'origin_rejected' });
  }
}
export async function readMemberJson(req: IncomingMessage): Promise<Record<string, unknown>> {
  if (!String(req.headers['content-type'] || '').startsWith('application/json')) {
    error(415, '仅接受 JSON');
  }
  let size = 0;
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    size += chunk.length;
    if (size > 32768) {
      error(413, '请求过大');
    }
    chunks.push(chunk);
  }
  let value: unknown;
  try {
    value = JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    error(400, 'JSON 格式错误');
  }
  if (!value || Array.isArray(value) || typeof value !== 'object') {
    error(400, '请求内容无效');
  }
  return value as Record<string, unknown>;
}
