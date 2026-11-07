CREATE TABLE `artifact_inputs` (
	`artifact_id` text PRIMARY KEY NOT NULL,
	`member_id` text,
	`payload_sha256` text NOT NULL,
	`payload` text NOT NULL
);
