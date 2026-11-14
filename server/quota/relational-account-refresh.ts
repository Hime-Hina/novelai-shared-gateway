import { relationalStateNotificationsFor } from '../events/relational-state-notifications.ts';
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
  events: EventEmitter;
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
  const notifications = relationalStateNotificationsFor(sqlite, options.events);
  let physical: ReturnType<typeof fetchNovelAiAccountQuota> | null = null,
    querying: ReturnType<ReturnType<typeof createAccountQuotaQuery>> | null = null;
  const query = createAccountQuotaQuery({
    ...options,
    fetchQuota: (...args) => {
      if (sqlite.inTransaction) {
        throw new Error('Quota queries cannot hold a SQL transaction across network IO.');
      }
      if (physical) {
        throw Object.assign(new Error('The current account quota request has not finished.'), {
          statusCode: 409,
        });
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
      throw Object.assign(new Error('The current account quota request has not finished.'), {
        statusCode: 409,
      });
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
    const enabledAccount = () =>
      sqlite
        .prepare<[], { id: string }>(
          `SELECT a.id FROM sharing_policy p JOIN upstream_accounts a ON a.id=p.bound_account_id
        WHERE p.id=1 AND a.enabled=1`,
        )
        .get();
    const account = enabledAccount();
    if (!account) {
      return { checked: 0, ok: 0, failed: 0, ignored: 0 };
    }
    const source = quotas.prepareQuotaObservation(account.id);
    let ticket: ReturnType<RelationalQuotaStore['beginQuotaObservation']> | undefined;
    let result: QuotaResult;
    try {
      const quota = await fetch(source.pst, '', account.id, () => {
        assertRelationalStorageReady(sqlite);
        if (enabledAccount()?.id !== account.id) {
          throw Object.assign(new Error('自动额度查询发起前绑定或启用状态已变化'), {
            statusCode: 409,
          });
        }
        ticket = quotas.beginQuotaObservation(account.id, clock(), source);
      });
      if (!ticket) {
        throw new Error('Quota refresh did not start its observation.');
      }
      result = accountQuotaResult(account.id, quota, new Date(ticket.startedAt).toISOString());
    } catch (error) {
      if (!ticket) {
        throw error;
      }
      result = accountQuotaErrorResult(account.id, error, new Date(ticket.startedAt).toISOString());
    }
    const identity = [ticket.accountId, ticket.credentialEpoch, ticket.sequence, ticket.startedAt];
    const committed = notifications.commit(() =>
      sqlite.transaction(() =>
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
          ticket!,
          result,
        ),
      )(),
    );
    if ('ignored' in committed) {
      return { checked: 1, ok: 0, failed: 0, ignored: 1 };
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
