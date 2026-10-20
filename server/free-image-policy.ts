import type Database from 'better-sqlite3';
import type { FairQuotaDatabase } from './fair-quota.ts';

export interface FreeImagePolicy {
  version: number; enabled: boolean; warningThreshold: number | null; hardLimit: number | null;
}
export interface FreeImageUsage {
  enabled: boolean; successful: number; reserved: number; warningThreshold: number | null; hardLimit: number | null;
  warning: boolean; warnedAt: number | null; nextAvailableAt: number | null; waitingForSettlement: boolean;
}
const day = 86400000;
export function freeImagePolicy(db: FairQuotaDatabase): FreeImagePolicy {
  return db.settings?.fairQuota?.freeImagePolicy || { version: 1, enabled: false, warningThreshold: null, hardLimit: null };
}

export function configureFreeImages(db: FairQuotaDatabase, body: { expectedVersion?: unknown; policy?: unknown }, actorId: string) {
  const before = freeImagePolicy(db), policy = body.policy as Partial<FreeImagePolicy> | null;
  if (body.expectedVersion !== before.version) throw Object.assign(new Error('免费图片策略已变化，请刷新后重试'), { statusCode: 409 });
  if (!db.settings?.fairQuota || !policy || Object.keys(policy).length !== 3 || typeof policy.enabled !== 'boolean'
    || !['warningThreshold', 'hardLimit'].every(key => {
      const value = policy[key as 'hardLimit'];
      return value === null || typeof value === 'number' && Number.isSafeInteger(value) && value > 0;
    }) || before.version >= Number.MAX_SAFE_INTEGER) throw Object.assign(new Error('免费图片阈值须为正整数或 null，enabled 须为布尔值'), { statusCode: 400 });
  const after: FreeImagePolicy = { version: before.version + 1, enabled: policy.enabled,
    warningThreshold: policy.warningThreshold!, hardLimit: policy.hardLimit! };
  db.settings.fairQuota.freeImagePolicy = after;
  (db.ledger ||= []).unshift({ id: `free-image-policy:${after.version}`, type: 'free_image_policy_updated',
    at: new Date().toISOString(), actorId, before, after });
  return after;
}

// 在 store.update 内读取；成功凭据在同一 SQLite 结算事务中写入，不依赖热任务缓存。
export function readFreeImageUsage(sqlite: Database.Database, db: FairQuotaDatabase, memberId: string, now = Date.now()): FreeImageUsage {
  const policy = freeImagePolicy(db);
  const reserved = Object.values(db.settings?.fairQuota?.reservations || {})
    .filter(charge => charge.userId === memberId && charge.resource === 'free' && charge.status === 'reserved').length;
  const args = { memberId, since: now - day, now };
  const { successful, warnedAt } = sqlite.prepare<typeof args & { warningThreshold: number | null }, { successful: number; warnedAt: number | null }>(
    `SELECT COUNT(*) AS successful, MAX(CASE WHEN warning_threshold = @warningThreshold THEN succeeded_at END) AS warnedAt
     FROM free_image_usage WHERE member_id = @memberId AND succeeded_at > @since AND succeeded_at <= @now`).get({ ...args, warningThreshold: policy.warningThreshold })!;
  const blocked = policy.enabled && policy.hardLimit !== null && successful + reserved >= policy.hardLimit;
  const needed = blocked ? successful + reserved - policy.hardLimit! + 1 : 0;
  const next = needed > 0 && needed <= successful ? sqlite.prepare(
    `SELECT succeeded_at FROM free_image_usage WHERE member_id = @memberId AND succeeded_at > @since AND succeeded_at <= @now
     ORDER BY succeeded_at, job_id LIMIT 1 OFFSET @offset`).get({ ...args, offset: needed - 1 }) as { succeeded_at: number } : null;
  return { ...policy, successful, reserved, warning: policy.enabled && policy.warningThreshold !== null && successful >= policy.warningThreshold,
    warnedAt, nextAvailableAt: next ? next.succeeded_at + day : null, waitingForSettlement: blocked && !next };
}

export function assertFreeImageAdmission(usage: FreeImageUsage): void {
  if (!usage.enabled || usage.hardLimit === null || usage.successful + usage.reserved < usage.hardLimit) return;
  const message = usage.waitingForSettlement ? '免费图片名额已被在途任务占用，请等待结算'
    : '滚动 24 小时免费图片数量已达上限，请等待名额恢复';
  throw Object.assign(new Error(message), { statusCode: 429, code: 'free_image_limit',
    cooldownUntil: usage.nextAvailableAt ? new Date(usage.nextAvailableAt).toISOString() : undefined });
}
