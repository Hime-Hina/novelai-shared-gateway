import type Database from 'better-sqlite3';
import type { EventEmitter } from 'node:events';
import { withAuthenticatedMemberApiKey } from '../members/authentication.ts';
import type { RelationalMemberStore } from '../storage/relational-member-store.ts';
import type { RelationalRequestStore } from '../storage/relational-request-store.ts';
import {
  readRelationalArtifactContent,
  relationalArtifactReferenceAvailable,
} from '../storage/relational-replica-io.ts';
import type { RelationalReplicaReadConfiguration } from '../storage/relational-replica-store.ts';
import { assertRelationalStorageReady } from '../storage/relational-storage.ts';
import { publicErrorMessage } from '../http/public-errors.ts';
import { createJobResultWaiter } from './result-waiter.ts';
import type { GenerationDeliveryResult } from './delivery.ts';

function fail(statusCode: number, message: string): never {
  throw Object.assign(new Error(message), { statusCode });
}

// Native/OpenAI 领取都以当前 Key 冷读事实；外部 IO 不持 SQL 事务，读后再次鉴权。
export function createRelationalApiDelivery(deps: {
  sqlite: Database.Database;
  members: RelationalMemberStore;
  requests: RelationalRequestStore;
  token: string;
  replicas: RelationalReplicaReadConfiguration;
  events: EventEmitter;
  env: NodeJS.ProcessEnv;
  now?(): number;
}) {
  const { sqlite, members, requests, token, env } = deps;
  const authorize = async <T>(read: (memberId: string) => T): Promise<T> => {
    const result = await withAuthenticatedMemberApiKey(
      members,
      token,
      (member) => {
        assertRelationalStorageReady(sqlite);
        return { value: read(member.id) };
      },
      env,
    );
    if (!result) {
      fail(401, 'invalid token.');
    }
    return result.value;
  };
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
  const drain = () => void delivery.drain();
  deps.events.on('commit', refresh);
  deps.events.on('drained', drain);
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
      deps.events.off('drained', drain);
    },
  };
}
