import type Database from 'better-sqlite3';
import type { EventEmitter } from 'node:events';
import { domainDigest } from '../events/domain-events.ts';
import { fetchNovelAiAccountQuota } from '../protocols/novelai-client.ts';
import { NATIVE_PRICING_VERSION } from '../protocols/native-pricing.ts';
import type { RelationalQuotaStore } from '../storage/relational-quota-store.ts';
import { assertRelationalStorageReady } from '../storage/relational-storage.ts';
import {
  accountQuotaResult,
  accountQuotaErrorResult,
  createAccountQuotaQuery,
  createAccountQuotaRefreshLoop,
  type QuotaResult,
} from './account-refresh.ts';

// 唯一绑定账户的实际自动观察；官方转换、资格和金融决定仍由原 query / quota owner 维护。
export function createRelationalAccountQuotaRefresh(options: {
  sqlite: Database.Database;
  quotas: RelationalQuotaStore;
  events: Pick<EventEmitter, 'emit'>;
  fetchQuota?: typeof fetchNovelAiAccountQuota;
  env?: NodeJS.ProcessEnv;
  now?(): number;
  requestTimeoutMs?: number;
  refreshIntervalMs?: number;
  logger?: Pick<Console, 'log' | 'error'>;
}) {
  const { sqlite, quotas } = options,
    clock = options.now || Date.now,
    rawFetch = options.fetchQuota || fetchNovelAiAccountQuota;
  assertRelationalStorageReady(sqlite);
  let physical: ReturnType<typeof fetchNovelAiAccountQuota> | null = null,
    querying: ReturnType<ReturnType<typeof createAccountQuotaQuery>> | null = null;
  const query = createAccountQuotaQuery({
    ...options,
    fetchQuota: (...args) => {
      if (sqlite.inTransaction) {
        throw new Error('Quota queries cannot hold a SQL transaction across network IO.');
      }
      if (physical) {
        throw new Error('The current account quota request has not finished.');
      }
      const request = Promise.resolve().then(() => rawFetch(...args));
      physical = request;
      // 超时 race 结束不代表底层已结束；拒绝也要被接住，直到 raw fetch 真正 settle。
      void request.then(
        () => {
          physical = null;
        },
        () => {
          physical = null;
        },
      );
      return request;
    },
  });
  const fetch: typeof query = (...args) => {
    if (sqlite.inTransaction) {
      throw new Error('Quota queries require the owning transaction to finish first.');
    }
    if (physical || querying) {
      throw new Error('The current account quota request has not finished.');
    }
    const request = query(...args);
    querying = request;
    // query 的 deadline 在首个微任务才发起 raw；关闭等待从公开调用时就必须可见。
    void request.then(
      () => {
        querying = null;
      },
      () => {
        querying = null;
      },
    );
    return request;
  };
  const loop = createAccountQuotaRefreshLoop(async () => {
    if (sqlite.inTransaction) {
      throw new Error('Quota refresh cannot span a SQL transaction.');
    }
    assertRelationalStorageReady(sqlite);
    if (physical || querying) {
      // Deadline 只结束等待；同一共享账户的真实请求未结束时不累积下一轮。
      return { checked: 0, ok: 0, failed: 0, ignored: 0 };
    }
    const account = sqlite
      .prepare<[], { id: string }>(
        `SELECT a.id FROM sharing_policy p JOIN upstream_accounts a ON a.id=p.bound_account_id
        WHERE p.id=1 AND a.enabled=1`,
      )
      .get();
    if (!account) {
      return { checked: 0, ok: 0, failed: 0, ignored: 0 };
    }
    const ticket = quotas.beginQuotaObservation(account.id, clock()),
      at = new Date(ticket.startedAt).toISOString();
    let result: QuotaResult;
    try {
      result = accountQuotaResult(account.id, await fetch(ticket.pst, '', account.id), at);
    } catch (error) {
      result = accountQuotaErrorResult(account.id, error, at);
    }
    const identity = [ticket.accountId, ticket.credentialEpoch, ticket.sequence, ticket.startedAt];
    const committed = sqlite.transaction(() =>
      quotas.recordQuotaObservation(
        {
          commandId: `quota-refresh:${domainDigest(identity)}`,
          fingerprint: domainDigest({ identity, result }),
          actorKind: 'system',
          actorId: null,
          recordedAt: clock(),
          correlationId: null,
          causationId: null,
          policyVersion: 'quota-observation-v1',
          pricingVersion: NATIVE_PRICING_VERSION,
        },
        ticket,
        result,
      ),
    )();
    if ('ignored' in committed) {
      return { checked: 1, ok: 0, failed: 0, ignored: 1 };
    }
    try {
      options.events.emit('commit');
    } catch {
      /* 通知失败不能改写已经提交的观察或使它重发。 */
    }
    return { checked: 1, ok: Number(result.ok), failed: Number(!result.ok), ignored: 0 };
  }, options);
  return {
    fetch,
    refreshEnabled: loop.refreshEnabled,
    start: loop.start,
    stop: loop.stop,
    get pending() {
      const active = [
        ...(physical ? [physical] : []),
        ...(querying ? [querying] : []),
        ...(loop.pending ? [loop.pending] : []),
      ];
      return active.length ? Promise.allSettled(active).then(() => undefined) : null;
    },
  };
}
