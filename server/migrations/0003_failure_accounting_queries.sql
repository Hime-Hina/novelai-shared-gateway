-- 校准 Drizzle Kit 对 JSON 表达式逗号的错误列名转义；与 storage-schema 定义保持一致。
CREATE INDEX `idx_jobs_member_late_results` ON `jobs` (user_token, json_extract(data, '$.lateResultAt') DESC, id DESC)
WHERE json_extract(data, '$.lateResultAvailable') = 1;--> statement-breakpoint
CREATE INDEX `idx_ledger_source_at` ON `ledger` (`source`,`at`);--> statement-breakpoint
CREATE INDEX `idx_ledger_failure_history` ON `ledger` ("order_value" DESC,"id" DESC) WHERE source IN ('generation_reserved', 'generation_refunded', 'generation_loss_suspected', 'generation_loss_estimated', 'generation_result_recovered', 'resource_gap_observed', 'resource_gap_reconciled', 'resource_return_unattributed', 'v5_capacity_observed', 'v5_capacity_recovered', 'operating_funding_registered', 'operating_funding_received', 'operating_loss_covered', 'loss_alert_policy_updated');
