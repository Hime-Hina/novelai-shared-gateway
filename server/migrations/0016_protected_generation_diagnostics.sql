CREATE TABLE `generation_diagnostics` (
	`diagnostic_id` text PRIMARY KEY NOT NULL,
	`job_id` text NOT NULL,
	`detail` text NOT NULL,
	`detail_sha256` text NOT NULL,
	`created_at` text NOT NULL
);
