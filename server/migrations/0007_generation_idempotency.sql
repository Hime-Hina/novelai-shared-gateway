CREATE TABLE `generation_idempotency` (
	`member_id` text NOT NULL,
	`operation` text NOT NULL,
	`key_hash` text NOT NULL,
	`fingerprint` text NOT NULL,
	`normalization_context` text NOT NULL,
	`job_id` text NOT NULL,
	`request_id` text,
	`created_at` text NOT NULL,
	PRIMARY KEY(`member_id`, `operation`, `key_hash`)
);
