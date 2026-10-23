CREATE TABLE `member_sessions` (
	`token_hash` text PRIMARY KEY NOT NULL,
	`user_id` text NOT NULL,
	`auth_version` text NOT NULL,
	`created_at` integer NOT NULL,
	`expires_at` integer NOT NULL,
	`owner_binding` text
);
--> statement-breakpoint
CREATE INDEX `idx_member_sessions_user` ON `member_sessions` (`user_id`);--> statement-breakpoint
CREATE INDEX `idx_member_sessions_expiry` ON `member_sessions` (`expires_at`);