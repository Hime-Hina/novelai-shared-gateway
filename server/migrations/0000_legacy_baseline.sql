-- Existing installations are verified before this idempotent baseline is adopted.
CREATE TABLE IF NOT EXISTS `accounts` (
	`id` text PRIMARY KEY,
	`order_value` real NOT NULL,
	`data` text NOT NULL,
	`token` text,
	`user_token` text,
	`account_id` text,
	`status` text,
	`source` text,
	`code` text,
	`cache_key` text,
	`image_id` text,
	`created_at` text,
	`updated_at` text,
	`at` text,
	`enabled` integer,
	`balance` real,
	`route_id` integer,
	`width` integer,
	`height` integer,
	`mock` integer,
	`mime_type` text,
	`model` text,
	`prompt` text,
	`full_prompt` text,
	`resolution_tier` text,
	`search_text` text
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_accounts_order` ON `accounts` ("order_value" DESC);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_accounts_route_id` ON `accounts` (`route_id`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `app_meta` (
	`key` text PRIMARY KEY,
	`value` text NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `app_records` (
	`collection` text NOT NULL,
	`id` text NOT NULL,
	`order_value` real NOT NULL,
	`data` text NOT NULL,
	PRIMARY KEY(`collection`, `id`)
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_app_records_collection_order` ON `app_records` (`collection`,"order_value" DESC);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `app_settings` (
	`key` text PRIMARY KEY,
	`value` text NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `cards` (
	`id` text PRIMARY KEY,
	`order_value` real NOT NULL,
	`data` text NOT NULL,
	`token` text,
	`user_token` text,
	`account_id` text,
	`status` text,
	`source` text,
	`code` text,
	`cache_key` text,
	`image_id` text,
	`created_at` text,
	`updated_at` text,
	`at` text,
	`enabled` integer,
	`balance` real,
	`route_id` integer,
	`width` integer,
	`height` integer,
	`mock` integer,
	`mime_type` text,
	`model` text,
	`prompt` text,
	`full_prompt` text,
	`resolution_tier` text,
	`search_text` text
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_cards_order` ON `cards` ("order_value" DESC);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `images` (
	`id` text PRIMARY KEY,
	`order_value` real NOT NULL,
	`data` text NOT NULL,
	`token` text,
	`user_token` text,
	`account_id` text,
	`status` text,
	`source` text,
	`code` text,
	`cache_key` text,
	`image_id` text,
	`created_at` text,
	`updated_at` text,
	`at` text,
	`enabled` integer,
	`balance` real,
	`route_id` integer,
	`width` integer,
	`height` integer,
	`mock` integer,
	`mime_type` text,
	`model` text,
	`prompt` text,
	`full_prompt` text,
	`resolution_tier` text,
	`search_text` text
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_images_order` ON `images` ("order_value" DESC);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_images_cache_key` ON `images` (`cache_key`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_images_tier_order` ON `images` (`resolution_tier`,"order_value" DESC);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_images_model_order` ON `images` (`model`,"order_value" DESC);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_images_tier_model_order` ON `images` (`resolution_tier`,`model`,"order_value" DESC);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `jobs` (
	`id` text PRIMARY KEY,
	`order_value` real NOT NULL,
	`data` text NOT NULL,
	`token` text,
	`user_token` text,
	`account_id` text,
	`status` text,
	`source` text,
	`code` text,
	`cache_key` text,
	`image_id` text,
	`created_at` text,
	`updated_at` text,
	`at` text,
	`enabled` integer,
	`balance` real,
	`route_id` integer,
	`width` integer,
	`height` integer,
	`mock` integer,
	`mime_type` text,
	`model` text,
	`prompt` text,
	`full_prompt` text,
	`resolution_tier` text,
	`search_text` text,
	`cost` real DEFAULT 0 NOT NULL,
	`stats_excluded` integer DEFAULT 0 NOT NULL,
	`error_loggable` integer DEFAULT 0 NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_jobs_order` ON `jobs` ("order_value" DESC);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_jobs_user_token` ON `jobs` (`user_token`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_jobs_image_id` ON `jobs` (`image_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_jobs_created_stats` ON `jobs` (`created_at`,`status`,`stats_excluded`,`account_id`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_jobs_updated_stats` ON `jobs` (`updated_at`,`status`,`model`,`created_at`,`cost`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_jobs_active_created` ON `jobs` (`created_at`) WHERE "jobs"."status" IN ('queued', 'running');--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_jobs_failed_order` ON `jobs` ("order_value" DESC) WHERE "jobs"."status" = 'failed';--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_jobs_error_log_updated` ON `jobs` ("updated_at" DESC) WHERE "jobs"."error_loggable" = 1;--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `ledger` (
	`id` text PRIMARY KEY,
	`order_value` real NOT NULL,
	`data` text NOT NULL,
	`token` text,
	`user_token` text,
	`account_id` text,
	`status` text,
	`source` text,
	`code` text,
	`cache_key` text,
	`image_id` text,
	`created_at` text,
	`updated_at` text,
	`at` text,
	`enabled` integer,
	`balance` real,
	`route_id` integer,
	`width` integer,
	`height` integer,
	`mock` integer,
	`mime_type` text,
	`model` text,
	`prompt` text,
	`full_prompt` text,
	`resolution_tier` text,
	`search_text` text
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_ledger_order` ON `ledger` ("order_value" DESC);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `users` (
	`id` text PRIMARY KEY,
	`order_value` real NOT NULL,
	`data` text NOT NULL,
	`token` text,
	`user_token` text,
	`account_id` text,
	`status` text,
	`source` text,
	`code` text,
	`cache_key` text,
	`image_id` text,
	`created_at` text,
	`updated_at` text,
	`at` text,
	`enabled` integer,
	`balance` real,
	`route_id` integer,
	`width` integer,
	`height` integer,
	`mock` integer,
	`mime_type` text,
	`model` text,
	`prompt` text,
	`full_prompt` text,
	`resolution_tier` text,
	`search_text` text
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_users_order` ON `users` ("order_value" DESC);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_users_token` ON `users` (`token`);