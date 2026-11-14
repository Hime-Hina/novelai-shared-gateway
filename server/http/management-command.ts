import type { IncomingMessage } from 'node:http';
import { domainDigest, type DomainCommandMetadata } from '../events/domain-events.ts';

export function managementCommand(
  req: IncomingMessage,
  actorId: string,
  operation: string,
  body: Record<string, unknown>,
  actorKind: 'admin' | 'member' = 'admin',
): DomainCommandMetadata {
  const key = req.headers['idempotency-key'];
  if (key === undefined) {
    return { actorId, actorKind };
  }
  if (
    typeof key !== 'string' ||
    !/^[\x21-\x7e]{1,128}$/.test(key) ||
    req.rawHeaders.filter(
      (value, index) => index % 2 === 0 && value.toLowerCase() === 'idempotency-key',
    ).length !== 1
  ) {
    throw Object.assign(
      new Error('Idempotency-Key 必须是单个非空可见 ASCII 字符串，最多 128 字节'),
      { statusCode: 400 },
    );
  }
  // 键按已鉴权 actor 和操作隔离；指纹只取提交意图，不取新生成的事件或时间。
  return {
    actorId,
    actorKind,
    commandId: `management:${domainDigest([actorId, operation, key])}`,
    fingerprint: domainDigest({ operation, body }),
  };
}
