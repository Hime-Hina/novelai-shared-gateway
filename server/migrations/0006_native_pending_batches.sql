DROP INDEX `idx_jobs_active_created`;--> statement-breakpoint
ALTER TABLE `jobs` ADD `payload_bytes` integer DEFAULT 0 NOT NULL;--> statement-breakpoint
CREATE INDEX `idx_jobs_active_created` ON `jobs` (`created_at`) WHERE "jobs"."status" IN ('queued', 'pending', 'running');