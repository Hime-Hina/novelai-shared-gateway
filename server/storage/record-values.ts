import crypto from 'node:crypto';

// 记录身份/原 JSON 摘要不依赖数据库实现；旧 Store 与实际受理、执行共用。
export function createId(prefix = 'item') {
  const random = crypto.randomBytes(5).toString('hex');
  return `${prefix}_${Date.now().toString(36)}_${random}`;
}

export function hashObject(value: unknown) {
  return crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
}
