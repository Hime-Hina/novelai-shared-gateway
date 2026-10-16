import { sql } from 'drizzle-orm';
import { sqliteTable, text, real, integer, index, primaryKey } from 'drizzle-orm/sqlite-core';

// 每张历史记录表使用同一组影子列；data 仍是已有 JSON 文本，不能再次自动编码。
const recordFields = () => ({
  id: text(), order_value: real().notNull(), data: text().notNull(),
  token: text(), user_token: text(), account_id: text(), status: text(), source: text(),
  code: text(), cache_key: text(), image_id: text(), created_at: text(), updated_at: text(), at: text(),
  enabled: integer(), balance: real(), route_id: integer(), width: integer(), height: integer(),
  mock: integer(), mime_type: text(), model: text(), prompt: text(), full_prompt: text(),
  resolution_tier: text(), search_text: text()
});
const recordIndexes = (name, table) => [
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
export const jobs = sqliteTable('jobs', {
  ...recordFields(), cost: real().default(0).notNull(),
  stats_excluded: integer().default(0).notNull(), error_loggable: integer().default(0).notNull()
}, table => [
  ...recordIndexes('jobs', table),
  index('idx_jobs_user_token').on(table.user_token),
  index('idx_jobs_image_id').on(table.image_id),
  index('idx_jobs_created_stats').on(table.created_at, table.status, table.stats_excluded, table.account_id),
  index('idx_jobs_updated_stats').on(table.updated_at, table.status, table.model, table.created_at, table.cost),
  index('idx_jobs_active_created').on(table.created_at).where(sql`${table.status} IN ('queued', 'running')`),
  index('idx_jobs_failed_order').on(sql`${table.order_value} DESC`).where(sql`${table.status} = 'failed'`),
  index('idx_jobs_error_log_updated').on(sql`${table.updated_at} DESC`).where(sql`${table.error_loggable} = 1`)
]);
export const images = sqliteTable('images', recordFields(), table => [
  ...recordIndexes('images', table),
  index('idx_images_cache_key').on(table.cache_key),
  index('idx_images_tier_order').on(table.resolution_tier, sql`${table.order_value} DESC`),
  index('idx_images_model_order').on(table.model, sql`${table.order_value} DESC`),
  index('idx_images_tier_model_order').on(table.resolution_tier, table.model, sql`${table.order_value} DESC`)
]);
export const ledger = sqliteTable('ledger', recordFields(), table => recordIndexes('ledger', table));
export const recordTables = { cards, users, accounts, jobs, images, ledger };
