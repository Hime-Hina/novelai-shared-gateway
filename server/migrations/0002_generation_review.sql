-- Drizzle Kit 0.31 会把带逗号的 JSON 表达式误当成列名；按 schema 意图校准生成 DDL。
CREATE INDEX `idx_jobs_generation_review_due` ON `jobs` (json_extract(data, '$.generationReview.deadlineAt'))
WHERE json_extract(data, '$.generationReview.status') = 'pending';
--> statement-breakpoint
-- 沿用旧疑似损耗首次时间，绝不以升级/重启时间重新给五分钟。
WITH pending AS (
  SELECT j.id, json_extract(j.data, '$.generationRefund.lossEventId') AS loss_id,
    COALESCE((SELECT json_extract(l.data, '$.firstUnknownAt') FROM ledger l
      WHERE l.id = json_extract(j.data, '$.generationRefund.lossEventId')),
      json_extract(j.data, '$.generationRefund.refundedAt')) AS first_at
  FROM jobs j WHERE j.status = 'failed' AND json_extract(j.data, '$.generationRefund.lossEventId') IS NOT NULL
    AND json_type(j.data, '$.generationReview') IS NULL
)
UPDATE jobs SET data = json_set(jobs.data, '$.generationReview', json_object(
  'status', 'pending',
  'firstUnknownAt', CASE WHEN typeof(pending.first_at) IN ('integer', 'real') AND pending.first_at > 0 THEN pending.first_at ELSE NULL END,
  'deadlineAt', CASE WHEN typeof(pending.first_at) IN ('integer', 'real') AND pending.first_at > 0 THEN pending.first_at + 300000 ELSE 0 END,
  'lossEventId', pending.loss_id, 'closedAt', NULL, 'closureEventId', NULL
)) FROM pending WHERE jobs.id = pending.id;
