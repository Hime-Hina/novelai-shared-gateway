import type Database from 'better-sqlite3';
import type { EventEmitter } from 'node:events';
import type { RelationalMemberApiKeyAuthorization } from '../members/relational-api-key-access.ts';
import type { RelationalRequestStore } from '../storage/relational-request-store.ts';
import {
  readRelationalArtifactContent,
  relationalArtifactReferenceAvailable,
} from '../storage/relational-replica-io.ts';
import type { RelationalReplicaReadConfiguration } from '../storage/relational-replica-store.ts';
import { publicErrorMessage } from '../http/public-errors.ts';
import { createJobResultWaiter } from './result-waiter.ts';
import type { GenerationDeliveryResult } from './delivery.ts';
import type { RelationalGenerationLifecycle } from './relational-queue-runtime.ts';

function fail(statusCode: number, message: string): never {
  throw Object.assign(new Error(message), { statusCode });
}

// 同一次请求的领取复用首鉴权身份/epoch；外部 IO 不持 SQL，读后仍核原身份与当前 Key。
export function createRelationalApiDelivery(deps: {
  sqlite: Database.Database;
  requests: RelationalRequestStore;
  authorize: RelationalMemberApiKeyAuthorization;
  replicas: RelationalReplicaReadConfiguration;
  events: EventEmitter;
  runtime: RelationalGenerationLifecycle;
  now?(): number;
}) {
  const { sqlite, requests, authorize } = deps;
  const readContent = async (jobId: string) => {
    const artifact = await authorize((memberId) => requests.readMemberResult(jobId, memberId));
    const content = await readRelationalArtifactContent(sqlite, artifact.artifactId, deps.replicas);
    const current = await authorize((memberId) => requests.readMemberResult(jobId, memberId));
    if (current.artifactId !== artifact.artifactId) {
      fail(410, '原结果已不可领取。');
    }
    return content;
  };
  const delivery = createJobResultWaiter({
    now: deps.now,
    errorMessage: (message) => publicErrorMessage(message, { generation: true }),
    readResult: async (jobId): Promise<GenerationDeliveryResult | null> => {
      try {
        const job = await authorize((memberId) => requests.readMemberJobDelivery(jobId, memberId));
        if (job.status === 'failed') {
          return {
            error: job.error || '生成失败',
            statusCode: job.errorStatus || 500,
            cooldownUntil: job.cooldownUntil,
          };
        }
        if (job.status !== 'done') {
          // 先读真实终态；晚于 drained 才注册的 waiter 也读取同一当前 phase，不依赖事件记忆。
          if (deps.runtime.status === 'stopped') {
            return { error: '服务正在关闭，请稍后重试。', statusCode: 503 };
          }
          return null;
        }
        const artifact = await authorize((memberId) => requests.readMemberResult(jobId, memberId));
        return {
          saved: { id: artifact.artifactId, jobId, mimeType: artifact.mimeType },
          ...(job.seed === null ? {} : { seed: job.seed }),
        };
      } catch (error) {
        const source = error as Error & { statusCode?: number };
        return { error: source.message, statusCode: source.statusCode || 503 };
      }
    },
  });
  const refresh = () => delivery.refresh();
  deps.events.on('commit', refresh);
  deps.events.on('drained', refresh);
  return {
    readJob: (jobId: string) =>
      authorize((memberId) => requests.readMemberJobDelivery(jobId, memberId)),
    readContent,
    async readSavedImage(image: NonNullable<GenerationDeliveryResult['saved']>) {
      const content = await readContent(image.jobId!);
      if (
        content.descriptor.imageId !== image.id ||
        content.descriptor.mimeType !== image.mimeType
      ) {
        fail(410, '原结果已不可领取。');
      }
      return content;
    },
    waitForJobResult: delivery.waitForJobResult,
    async validateReplay(jobIds: string[]) {
      // 已承诺的产物缺失不能重发或改成新意图，当前 Key 仍须有效。
      for (const jobId of jobIds) {
        const job = await authorize((memberId) => requests.readMemberJobDelivery(jobId, memberId));
        if (job.requestId === null) {
          fail(410, '原任务没有可重领的父请求；如需生成，请使用新的操作键');
        }
        if (job.status === 'done' || job.artifactId) {
          const artifact = await authorize((memberId) =>
            requests.readMemberResult(jobId, memberId),
          );
          let available = false;
          try {
            available = await relationalArtifactReferenceAvailable(
              sqlite,
              artifact.artifactId,
              deps.replicas,
            );
          } catch {
            /* 原引用资格失败不新增意图；实际内容读取和云503仍由后续领取处理。 */
          }
          const current = await authorize((memberId) => requests.readMemberResult(jobId, memberId));
          if (!available || current.artifactId !== artifact.artifactId) {
            fail(410, '原请求的任务或结果已不可领取；如需再次生成，请使用新的操作键');
          }
        }
      }
    },
    close() {
      deps.events.off('commit', refresh);
      deps.events.off('drained', refresh);
    },
  };
}
