import { sql } from 'drizzle-orm';
import { sqliteTable, text, real, integer, index, primaryKey, type AnySQLiteColumn } from 'drizzle-orm/sqlite-core';
import { FAILURE_EVENT_PREDICATE } from './failure-accounting.ts';

// 每张历史记录表使用同一组影子列；data 仍是已有 JSON 文本，不能再次自动编码。
const recordFields = () => ({
  id: text(), order_value: real().notNull(), data: text().notNull(),
  token: text(), user_token: text(), account_id: text(), status: text(), source: text(),
  code: text(), cache_key: text(), image_id: text(), created_at: text(), updated_at: text(), at: text(),
  enabled: integer(), balance: real(), route_id: integer(), width: integer(), height: integer(),
  mock: integer(), mime_type: text(), model: text(), prompt: text(), full_prompt: text(),
  resolution_tier: text(), search_text: text()
});
const recordIndexes = (name: string, table: { id: AnySQLiteColumn; order_value: AnySQLiteColumn }) => [
  // 旧 SQLite TEXT PRIMARY KEY 允许 NULL；接入 ORM 时不顺带改变这个约束。
  primaryKey({ columns: [table.id] }),
  index(`idx_${name}_order`).on(sql`${table.order_value} DESC`)
];

export const appMeta = sqliteTable('app_meta', { key: text(), value: text().notNull() },
  table => [primaryKey({ columns: [table.key] })]);
export const appSettings = sqliteTable('app_settings', { key: text(), value: text().notNull() },
  table => [primaryKey({ columns: [table.key] })]);
export const appRecords = sqliteTable('app_records', {
  collection: text().notNull(), id: text().notNull(), order_value: real().notNull(), data: text().notNull()
}, table => [
  primaryKey({ columns: [table.collection, table.id] }),
  index('idx_app_records_collection_order').on(table.collection, sql`${table.order_value} DESC`)
]);

export const cards = sqliteTable('cards', recordFields(), table => recordIndexes('cards', table));
export const users = sqliteTable('users', recordFields(), table => [
  ...recordIndexes('users', table), index('idx_users_token').on(table.token)
]);
export const accounts = sqliteTable('accounts', recordFields(), table => [
  ...recordIndexes('accounts', table), index('idx_accounts_route_id').on(table.route_id)
]);
// 不加入 recordTables：历史执行参数按 ID 冷读，不能进入通用全量热缓存。
export const generationRequests = sqliteTable('generation_requests', {
  id: text().primaryKey(), member_id: text().notNull(), source: text().notNull(),
  operation: text().notNull(), created_at: text().notNull(),
  payload_bytes: integer().notNull(), payload: text().notNull()
});

// 去重证据独立于可清理的任务/产物；不使用级联外键，历史缺失仍保留原映射。
export const generationIdempotency = sqliteTable('generation_idempotency', {
  member_id: text().notNull(), operation: text().notNull(), key_hash: text().notNull(),
  fingerprint: text().notNull(), normalization_context: text().notNull(),
  job_id: text().notNull(), request_id: text(), created_at: text().notNull()
}, table => [primaryKey({ columns: [table.member_id, table.operation, table.key_hash] })]);

// 结算凭据不随任务历史清理，也不进入通用热缓存。
export const fairQuotaSettlements = sqliteTable('fair_quota_settlements', {
  job_id: text().primaryKey(), charge: text().notNull()
});

// 独立于可清理任务的成功凭据；SQLite 触发器与结算/迟到结果原子提交。
export const freeImageUsage = sqliteTable('free_image_usage', {
  job_id: text().primaryKey(), member_id: text().notNull(), succeeded_at: integer().notNull(), warning_threshold: integer()
}, table => [index('idx_free_image_member_time').on(table.member_id, table.succeeded_at, table.job_id)]);

// 会话按令牌摘要冷读，不进入业务记录热缓存或历史清理。
export const memberSessions = sqliteTable('member_sessions', {
  token_hash: text().primaryKey(), user_id: text().notNull(), auth_version: text().notNull(),
  created_at: integer().notNull(), expires_at: integer().notNull(), owner_binding: text()
}, table => [
  index('idx_member_sessions_user').on(table.user_id),
  index('idx_member_sessions_expiry').on(table.expires_at)
]);

export const jobs = sqliteTable('jobs', {
  ...recordFields(), request_id: text().references(() => generationRequests.id), queue_seq: integer(), payload_bytes: integer().default(0).notNull(), cost: real().default(0).notNull(),
  stats_excluded: integer().default(0).notNull(), error_loggable: integer().default(0).notNull()
}, table => [
  ...recordIndexes('jobs', table),
  index('idx_jobs_request_status').on(table.request_id, table.status),
  index('idx_jobs_queued_sequence').on(table.queue_seq).where(sql`${table.status} = 'queued'`),
  index('idx_jobs_user_token').on(table.user_token),
  index('idx_jobs_image_id').on(table.image_id),
  index('idx_jobs_created_stats').on(table.created_at, table.status, table.stats_excluded, table.account_id),
  index('idx_jobs_updated_stats').on(table.updated_at, table.status, table.model, table.created_at, table.cost),
  index('idx_jobs_active_created').on(table.created_at).where(sql`${table.status} IN ('queued', 'pending', 'running')`),
  index('idx_jobs_failed_order').on(sql`${table.order_value} DESC`).where(sql`${table.status} = 'failed'`),
  index('idx_jobs_error_log_updated').on(sql`${table.updated_at} DESC`).where(sql`${table.error_loggable} = 1`),
  index('idx_jobs_generation_review_due').on(sql`json_extract(${table.data}, '$.generationReview.deadlineAt')`)
    .where(sql`json_extract(${table.data}, '$.generationReview.status') = 'pending'`),
  index('idx_jobs_result_pending').on(table.id)
    .where(sql`json_type(${table.data}, '$.resultPending') = 'object'`),
  index('idx_jobs_member_late_results').on(table.user_token, sql`json_extract(${table.data}, '$.lateResultAt') DESC`, sql`${table.id} DESC`)
    .where(sql`json_extract(${table.data}, '$.lateResultAvailable') = 1`)
]);
export const images = sqliteTable('images', recordFields(), table => [
  ...recordIndexes('images', table),
  index('idx_images_cache_key').on(table.cache_key),
  index('idx_images_tier_order').on(table.resolution_tier, sql`${table.order_value} DESC`),
  index('idx_images_model_order').on(table.model, sql`${table.order_value} DESC`),
  index('idx_images_tier_model_order').on(table.resolution_tier, table.model, sql`${table.order_value} DESC`)
]);
export const ledger = sqliteTable('ledger', recordFields(), table => [
  ...recordIndexes('ledger', table), index('idx_ledger_source_at').on(table.source, table.at),
  index('idx_ledger_failure_history').on(sql`${table.order_value} DESC`, sql`${table.id} DESC`).where(sql.raw(FAILURE_EVENT_PREDICATE))
]);
export const recordTables = { cards, users, accounts, jobs, images, ledger };
