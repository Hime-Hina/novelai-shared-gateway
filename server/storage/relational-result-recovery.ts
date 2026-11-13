import type Database from 'better-sqlite3';
import { domainDigest } from '../events/domain-events.ts';
import { RelationalGenerationStore } from './relational-generation-store.ts';
import { publishStoredResult } from './result-files.ts';

export interface RelationalResultRecoveryOutcome {
  jobId: string;
  status: 'completed' | 'file_missing' | 'unresolved';
  artifactId?: string;
  code?: string;
}

// 仅供唯一执行器监听/派发前串行恢复；文件 IO 不持有 SQLite 写事务。
export async function recoverRelationalGenerationResults(
  sqlite: Database.Database,
  dataDir: string,
  options: {
    now?: () => number;
    onRecovery?(outcome: RelationalResultRecoveryOutcome): void;
  } = {},
) {
  if (sqlite.inTransaction) {
    throw new Error('Result recovery cannot hold a transaction across file IO.');
  }
  const store = new RelationalGenerationStore(sqlite),
    result = { completed: 0, fileMissing: 0, unresolved: 0 };
  let afterId = '';
  for (;;) {
    const ids = store.readPendingGenerationResultIds(afterId);
    if (!ids.length) {
      return result;
    }
    for (const jobId of ids) {
      afterId = jobId;
      let outcome: RelationalResultRecoveryOutcome;
      try {
        const { attemptId, event, descriptor } = store.readGenerationResultPreparation(jobId);
        const content = await publishStoredResult(dataDir, descriptor);
        if (!content) {
          result.fileMissing++;
          outcome = { jobId, status: 'file_missing' };
        } else {
          sqlite
            .transaction(() =>
              store.completeGeneration(
                {
                  commandId: `result-recovery:${event.eventId}`,
                  fingerprint: domainDigest({
                    jobId,
                    attemptId,
                    descriptor,
                    eventId: event.eventId,
                  }),
                  actorKind: 'system',
                  actorId: null,
                  recordedAt: (options.now || Date.now)(),
                  correlationId: event.correlationId,
                  causationId: event.eventId,
                  policyVersion: event.policyVersion,
                  pricingVersion: event.pricingVersion,
                },
                jobId,
                descriptor,
              ),
            )
            .immediate();
          result.completed++;
          outcome = { jobId, status: 'completed', artifactId: descriptor.imageId };
        }
      } catch (error) {
        result.unresolved++;
        outcome = {
          jobId,
          status: 'unresolved',
          code: (error as { code?: string }).code || 'RESULT_RECOVERY_FAILED',
        };
      }
      options.onRecovery?.(outcome);
    }
  }
}
