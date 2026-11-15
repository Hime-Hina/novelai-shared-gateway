import { createHmac, randomBytes, timingSafeEqual } from 'node:crypto';

// 签名是可转交的限时凭据；每个 sole 实例仅持有一个进程密钥，重启失效。
export function createPublicImageAccess(env: NodeJS.ProcessEnv = process.env, now = Date.now) {
  const secret = randomBytes(32);
  function mac(pathname: string, expires: string) {
    return createHmac('sha256', secret).update(`${pathname}\n${expires}`).digest('hex');
  }
  function imageUrl(id: string) {
    const pathname = `/api/images/${encodeURIComponent(id)}/content`;
    if (env.PUBLIC_MODE !== 'true') {
      return pathname;
    }
    const expires = String(Math.floor(now() / 1000) + 900);
    return `${pathname}?expires=${expires}&signature=${mac(pathname, expires)}`;
  }
  function validSignature(url: URL) {
    if (!/^\/api\/images\/[^/]+\/content$/.test(url.pathname)) {
      return false;
    }
    const expires = url.searchParams.get('expires') || '',
      signature = url.searchParams.get('signature') || '';
    if (!/^\d{10}$/.test(expires) || !/^[a-f0-9]{64}$/.test(signature)) {
      return false;
    }
    const current = Math.floor(now() / 1000);
    if (Number(expires) < current || Number(expires) > current + 900) {
      return false;
    }
    return timingSafeEqual(
      Buffer.from(signature, 'hex'),
      Buffer.from(mac(url.pathname, expires), 'hex'),
    );
  }
  return { imageUrl, validSignature };
}
