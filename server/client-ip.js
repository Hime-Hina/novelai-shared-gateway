import { isIP } from 'node:net';

function normalizeIp(value) {
  if (typeof value !== 'string') return null;
  const address = value.trim();
  const version = isIP(address);
  if (version === 4) return address;
  if (version !== 6 || address.includes('%')) return null;
  const canonical = new URL(`http://[${address}]/`).hostname.slice(1, -1);
  const mapped = /^::ffff:([\da-f]+):([\da-f]+)$/.exec(canonical);
  if (!mapped) return canonical;
  // IPv4 与其 mapped IPv6 写法必须共用限流桶，也要匹配同一条代理配置。
  const high = parseInt(mapped[1], 16), low = parseInt(mapped[2], 16);
  return [high >> 8, high & 255, low >> 8, low & 255].join('.');
}

export function clientIp(req, env = process.env) {
  const configured = String(env.TRUSTED_PROXY_IPS || '').trim();
  const trusted = configured ? configured.split(',').map(normalizeIp) : [];
  if (trusted.some(address => address === null)) {
    throw Object.assign(new Error('TRUSTED_PROXY_IPS 仅接受逗号分隔的 IPv4/IPv6 地址'), { statusCode: 503 });
  }
  const peer = normalizeIp(req.socket?.remoteAddress) || 'unknown';
  const forwarded = req.headers['x-forwarded-for'];
  if (!trusted.includes(peer) || typeof forwarded !== 'string') return peer;

  // 代理按顺序追加连接来源；从最近一跳向外走，在首个不可信地址处停止。
  // 不能取最左侧地址，那里可能是客户端自行添加的伪造头。
  const chain = forwarded.split(',');
  let address = peer;
  for (let i = chain.length - 1; i >= 0 && trusted.includes(address); i--) {
    address = normalizeIp(chain[i]);
    if (!address) return peer;
  }
  return address;
}
