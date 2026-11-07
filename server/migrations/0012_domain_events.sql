CREATE TABLE `domain_events` (
	`event_id` text PRIMARY KEY NOT NULL,
	`commit_seq` integer NOT NULL,
	`event_index` integer NOT NULL,
	`stream_id` text NOT NULL,
	`stream_kind` text NOT NULL,
	`stream_version` integer NOT NULL,
	`event_type` text NOT NULL,
	`schema_version` integer NOT NULL,
	`occurred_at` text NOT NULL,
	`recorded_at` text NOT NULL,
	`actor_id` text,
	`correlation_id` text,
	`causation_id` text,
	`account_id` text,
	`member_id` text,
	`policy_version` text NOT NULL,
	`pricing_version` text NOT NULL,
	`amount_version` text NOT NULL,
	`payload` text NOT NULL,
	FOREIGN KEY (`commit_seq`) REFERENCES `event_commits`(`commit_seq`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE UNIQUE INDEX `idx_domain_events_commit` ON `domain_events` (`commit_seq`,`event_index`);--> statement-breakpoint
CREATE UNIQUE INDEX `idx_domain_events_stream` ON `domain_events` (`stream_id`,`stream_version`);--> statement-breakpoint
CREATE TABLE `event_clock` (
	`id` integer PRIMARY KEY NOT NULL,
	`value` integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE `event_commits` (
	`commit_seq` integer PRIMARY KEY NOT NULL,
	`command_id` text NOT NULL,
	`fingerprint` text NOT NULL,
	`recorded_at` text NOT NULL,
	`event_count` integer NOT NULL,
	`receipt` text NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `event_commits_command_id_unique` ON `event_commits` (`command_id`);--> statement-breakpoint
CREATE TABLE `event_imports` (
	`import_id` text NOT NULL,
	`chunk_id` text NOT NULL,
	`source_sha256` text NOT NULL,
	`first_key` text,
	`last_key` text,
	`record_count` integer NOT NULL,
	`commit_seq` integer NOT NULL,
	`completed_at` text NOT NULL,
	PRIMARY KEY(`import_id`, `chunk_id`)
);
--> statement-breakpoint
CREATE TABLE `event_streams` (
	`stream_id` text PRIMARY KEY NOT NULL,
	`stream_kind` text NOT NULL,
	`stream_version` integer NOT NULL,
	`last_commit_seq` integer NOT NULL,
	`last_event_id` text NOT NULL,
	`reducer_version` integer NOT NULL,
	`projection` text NOT NULL,
	`projection_sha256` text NOT NULL
);
--> statement-breakpoint
CREATE TABLE `projection_versions` (
	`name` text PRIMARY KEY NOT NULL,
	`epoch` text NOT NULL,
	`reducer_version` integer NOT NULL,
	`last_commit_seq` integer NOT NULL,
	`status` text NOT NULL,
	`updated_at` text NOT NULL
);
