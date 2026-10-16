CREATE TABLE `generation_requests` (
	`id` text PRIMARY KEY NOT NULL,
	`member_id` text NOT NULL,
	`source` text NOT NULL,
	`operation` text NOT NULL,
	`created_at` text NOT NULL,
	`payload_bytes` integer NOT NULL,
	`payload` text NOT NULL
);
--> statement-breakpoint
ALTER TABLE `jobs` ADD `request_id` text REFERENCES generation_requests(id);--> statement-breakpoint
CREATE INDEX `idx_jobs_request_status` ON `jobs` (`request_id`,`status`);