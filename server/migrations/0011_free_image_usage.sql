CREATE TABLE free_image_usage (
  job_id TEXT PRIMARY KEY NOT NULL,
  member_id TEXT NOT NULL,
  succeeded_at INTEGER NOT NULL,
  warning_threshold INTEGER
);
--> statement-breakpoint
CREATE INDEX idx_free_image_member_time ON free_image_usage(member_id, succeeded_at, job_id);
--> statement-breakpoint
INSERT OR IGNORE INTO free_image_usage(job_id, member_id, succeeded_at)
SELECT job_id, json_extract(charge, '$.userId'), json_extract(charge, '$.settledAt')
FROM fair_quota_settlements
WHERE json_extract(charge, '$.resource') = 'free' AND json_extract(charge, '$.status') = 'settled'
  AND json_type(charge, '$.userId') = 'text' AND json_type(charge, '$.settledAt') IN ('integer', 'real');
--> statement-breakpoint
INSERT OR IGNORE INTO free_image_usage(job_id, member_id, succeeded_at)
SELECT id, json_extract(data, '$.fairCharge.userId'),
  CAST(ROUND((julianday(json_extract(data, '$.lateResultAt')) - 2440587.5) * 86400000) AS INTEGER)
FROM jobs WHERE json_extract(data, '$.fairCharge.resource') = 'free'
  AND json_extract(data, '$.lateResultAvailable') = 1
  AND json_type(data, '$.fairCharge.userId') = 'text' AND julianday(json_extract(data, '$.lateResultAt')) IS NOT NULL;
--> statement-breakpoint
CREATE TRIGGER free_image_settled AFTER INSERT ON fair_quota_settlements
WHEN json_extract(NEW.charge, '$.resource') = 'free' AND json_extract(NEW.charge, '$.status') = 'settled'
  AND json_type(NEW.charge, '$.userId') = 'text' AND json_type(NEW.charge, '$.settledAt') IN ('integer', 'real')
BEGIN
  INSERT OR IGNORE INTO free_image_usage(job_id, member_id, succeeded_at)
  VALUES(NEW.job_id, json_extract(NEW.charge, '$.userId'), json_extract(NEW.charge, '$.settledAt'));
END;
--> statement-breakpoint
CREATE TRIGGER free_image_recovered_insert AFTER INSERT ON jobs
WHEN json_extract(NEW.data, '$.fairCharge.resource') = 'free' AND json_extract(NEW.data, '$.lateResultAvailable') = 1
  AND json_type(NEW.data, '$.fairCharge.userId') = 'text' AND julianday(json_extract(NEW.data, '$.lateResultAt')) IS NOT NULL
BEGIN
  INSERT OR IGNORE INTO free_image_usage(job_id, member_id, succeeded_at)
  VALUES(NEW.id, json_extract(NEW.data, '$.fairCharge.userId'),
    CAST(ROUND((julianday(json_extract(NEW.data, '$.lateResultAt')) - 2440587.5) * 86400000) AS INTEGER));
END;
--> statement-breakpoint
CREATE TRIGGER free_image_recovered_update AFTER UPDATE OF data ON jobs
WHEN json_extract(NEW.data, '$.fairCharge.resource') = 'free' AND json_extract(NEW.data, '$.lateResultAvailable') = 1
  AND json_type(NEW.data, '$.fairCharge.userId') = 'text' AND julianday(json_extract(NEW.data, '$.lateResultAt')) IS NOT NULL
BEGIN
  INSERT OR IGNORE INTO free_image_usage(job_id, member_id, succeeded_at)
  VALUES(NEW.id, json_extract(NEW.data, '$.fairCharge.userId'),
    CAST(ROUND((julianday(json_extract(NEW.data, '$.lateResultAt')) - 2440587.5) * 86400000) AS INTEGER));
END;
--> statement-breakpoint
CREATE TRIGGER free_image_warning AFTER INSERT ON free_image_usage
WHEN (SELECT json_extract(value, '$.fairQuota.freeImagePolicy.enabled') FROM app_settings WHERE key = 'settings') = 1
BEGIN
  UPDATE free_image_usage SET warning_threshold = (
    SELECT json_extract(value, '$.fairQuota.freeImagePolicy.warningThreshold') FROM app_settings WHERE key = 'settings'
  ) WHERE job_id = NEW.job_id AND (
    SELECT COUNT(*) FROM free_image_usage WHERE member_id = NEW.member_id
      AND succeeded_at > NEW.succeeded_at - 86400000 AND succeeded_at <= NEW.succeeded_at
  ) = (SELECT json_extract(value, '$.fairQuota.freeImagePolicy.warningThreshold') FROM app_settings WHERE key = 'settings');
END;
--> statement-breakpoint
DROP INDEX `idx_ledger_failure_history`;--> statement-breakpoint
CREATE INDEX `idx_ledger_failure_history` ON `ledger` ("order_value" DESC,"id" DESC) WHERE source IN ('generation_reserved', 'generation_refunded', 'generation_loss_suspected', 'generation_loss_estimated', 'generation_result_recovered', 'resource_gap_observed', 'resource_gap_reconciled', 'resource_return_unattributed', 'v5_capacity_observed', 'v5_capacity_recovered', 'operating_funding_registered', 'operating_funding_received', 'operating_loss_covered', 'loss_alert_policy_updated', 'operating_debits_confirmed');
