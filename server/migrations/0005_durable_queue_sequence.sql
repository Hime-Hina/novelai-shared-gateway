ALTER TABLE `jobs` ADD `queue_seq` integer;--> statement-breakpoint
CREATE INDEX `idx_jobs_queued_sequence` ON `jobs` (`queue_seq`) WHERE "jobs"."status" = 'queued';