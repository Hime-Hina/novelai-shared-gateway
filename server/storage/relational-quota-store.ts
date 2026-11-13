import { randomUUID } from 'node:crypto';
import type Database from 'better-sqlite3';
import {
  beginDomainCommand,
  finishDomainCommand,
  type DomainEventInput,
  type Json,
} from '../events/domain-events.ts';
import {
  appendRelationalCommit,
  readRelationalReceipt,
  replayRelationalCommit,
  type StoredRelationalCommit,
} from '../events/relational-event-store.ts';
import type {
  RelationalCommand,
  RelationalEvent,
  RelationalEventInput,
  RelationalFact,
  RelationalPayloads,
} from '../events/relational-events.ts';
import {
  syncBoundFairQuota,
  manageOperatingFunding,
  fairQuotaSnapshot,
  type FairQuotaSyncState,
  type FairAccount,
  type ResourceAmounts,
} from '../quota/fair-share.ts';
import {
  createQuotaObservationOrder,
  accountQuotaPatch,
  type QuotaResult,
} from '../quota/account-refresh.ts';
import type {
  OperatingAccount,
  OperatingFundingInput,
  OperatingFundingContext,
} from '../quota/operating-accounting.ts';
import { parseAmount, formatAmount } from '../quota/resource-amounts.ts';
import { assertRelationalStorageFormat } from './relational-storage.ts';
import { quotaTimestamp as isoTimestamp } from '../quota/eligibility.ts';

type Metadata = Omit<RelationalCommand, 'events' | 'expectedVersions' | 'receipt'> & {
  policyVersion: string;
  pricingVersion: string;
};
export type AccountQuota = Omit<RelationalPayloads['OpeningQuotaRecorded'], 'origin'>;
type Quota = AccountQuota;
type Ticket = {
  accountId: string;
  credentialEpoch: number;
  sequence: string;
  startedAt: number;
  pst: string;
};
const resources = ['v5', 'anlasFixed', 'anlasPurchased'] as const;
const zero = (): ResourceAmounts => ({ v5: '0', anlasFixed: '0', anlasPurchased: '0' });
const iso = (n: number | null) => (n === null ? undefined : new Date(n).toISOString());
function fail(message: string, statusCode = 409): never {
  throw Object.assign(new Error(message), { statusCode });
}
export const relationalQuotaFacts = new Set<RelationalEvent['eventType']>([
  'QuotaObserved',
  'QuotaStatusChanged',
  'AvailabilityChanged',
  'QuotaBaselineAdopted',
  'RefillApplied',
  'AllocationApplied',
  'FundingRegistered',
  'FundingReceived',
  'ResourceGapObserved',
  'ReturnClassified',
  'DebitsConfirmed',
  'OperatingLossCovered',
  'V5CapacityObserved',
]);
const quotaColumns = {
  accountId: 'account_id',
  status: 'status',
  observationEventId: 'observation_event_id',
  observedAt: 'observed_at',
  observedCredentialEpoch: 'observed_credential_epoch',
  observedSequence: 'observed_sequence',
  quotaTier: 'quota_tier',
  subscriptionActive: 'subscription_active',
  subscriptionExpiresAt: 'subscription_expires_at',
  quotaErrorCode: 'quota_error_code',
  observedV5: 'observed_v5',
  observedAnlasFixed: 'observed_anlas_fixed',
  observedAnlasPurchased: 'observed_anlas_purchased',
  secondsPerPercent: 'seconds_per_percent',
  baselineEventId: 'baseline_event_id',
  refillCursorAt: 'refill_cursor_at',
  settlementCursorAt: 'settlement_cursor_at',
  unallocatedV5: 'unallocated_v5',
  fallbackFixed: 'fallback_fixed',
  fallbackPurchased: 'fallback_purchased',
  sharedV5Debt: 'shared_v5_debt',
  pendingV5Debit: 'pending_v5_debit',
  pendingFixedDebit: 'pending_fixed_debit',
  pendingPurchasedDebit: 'pending_purchased_debit',
  pendingFallbackV5: 'pending_fallback_v5',
  pendingFallbackFixed: 'pending_fallback_fixed',
  pendingFallbackPurchased: 'pending_fallback_purchased',
} satisfies Record<keyof Quota, string>;

// 业务规则归现有 quota owner；这里只读取当前关系状态、映射事实并提交投影。
export class RelationalQuotaStore {
  private order = createQuotaObservationOrder();
  private sqlite: Database.Database;
  private env: NodeJS.ProcessEnv;
  constructor(sqlite: Database.Database, env: NodeJS.ProcessEnv = process.env) {
    this.sqlite = sqlite;
    this.env = env;
    assertRelationalStorageFormat(sqlite);
  }
  private owned<T>(run: () => T): T {
    if (!this.sqlite.inTransaction) {
      throw new Error('Quota writes require the owning command transaction.');
    }
    return this.sqlite.transaction(run)();
  }
  readAccountQuota(accountId: string): Quota {
    const row = this.sqlite
      .prepare<[string], Quota>(
        `SELECT ${Object.entries(quotaColumns)
          .map(
            ([key, col]) =>
              `${key === 'observedSequence' ? `CAST(${col} AS TEXT)` : col} AS ${key}`,
          )
          .join(',')}
      FROM account_quota_state WHERE account_id=?`,
      )
      .get(accountId);
    if (!row) {
      fail('账户账务基线尚未恢复', 503);
    }
    return {
      ...row,
      subscriptionActive: row.subscriptionActive === null ? null : Boolean(row.subscriptionActive),
    };
  }
  saveAccountQuota(row: Quota, eventId: string) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Quota projection requires the owning transaction.');
    }
    if (
      !this.sqlite
        .prepare('SELECT 1 FROM domain_events WHERE event_id=? AND account_id=?')
        .get(eventId, row.accountId)
    ) {
      fail('额度投影缺少同账户事实');
    }
    this.sqlite
      .prepare(
        `UPDATE account_quota_state SET ${Object.entries(quotaColumns)
          .filter(([k]) => k !== 'accountId')
          .map(([key, col]) => `${col}=@${key}`)
          .join(',')},source_event_id=@eventId WHERE account_id=@accountId`,
      )
      .run({
        ...row,
        subscriptionActive: row.subscriptionActive === null ? null : Number(row.subscriptionActive),
        eventId,
      });
  }
  openAccountQuota(e: Extract<RelationalEvent, { eventType: 'OpeningQuotaRecorded' }>) {
    if (
      !this.sqlite.inTransaction ||
      !this.sqlite
        .prepare(
          "SELECT 1 FROM domain_events WHERE event_id=? AND event_type='OpeningQuotaRecorded' AND account_id=?",
        )
        .get(e.eventId, e.payload.accountId) ||
      !this.sqlite
        .prepare(
          "SELECT 1 FROM domain_events WHERE event_type='ImportSourceRegistered' AND stream_id=?",
        )
        .get(`import:${e.payload.origin.sourceId}`)
    ) {
      fail('开账额度缺少原事实或登记来源');
    }
    const { origin: _origin, ...p } = e.payload;
    this.sqlite
      .prepare(
        `INSERT INTO account_quota_state(${Object.values(quotaColumns).join(',')},source_event_id) VALUES(${Object.keys(
          quotaColumns,
        )
          .map((k) => `@${k}`)
          .join(',')},@eventId)`,
      )
      .run({
        ...p,
        subscriptionActive: p.subscriptionActive === null ? null : Number(p.subscriptionActive),
        eventId: e.eventId,
      });
  }
  private operating(accountId: string): OperatingAccount {
    const p = this.sqlite
      .prepare<
        [string],
        {
          v5: string;
          fixed: string;
          purchased: string;
          unfundedFixed: string;
          unfundedPurchased: string;
          returnedFixed: string;
          returnedPurchased: string;
          fixedGap: string | null;
          purchasedGap: string | null;
          v5Gap: string;
          v5GapEvent: string | null;
        }
      >(
        `SELECT v5_available AS v5,anlas_fixed_available AS fixed,anlas_purchased_available AS purchased,unfunded_fixed AS unfundedFixed,unfunded_purchased AS unfundedPurchased,unattributed_fixed AS returnedFixed,unattributed_purchased AS returnedPurchased,fixed_gap_event_id AS fixedGap,purchased_gap_event_id AS purchasedGap,v5_capacity_gap AS v5Gap,v5_gap_event_id AS v5GapEvent FROM operating_state WHERE account_id=?`,
      )
      .get(accountId);
    if (!p) {
      fail('运营账尚未开账', 503);
    }
    return {
      available: { v5: p.v5, anlasFixed: p.fixed, anlasPurchased: p.purchased },
      unfunded: { anlasFixed: p.unfundedFixed, anlasPurchased: p.unfundedPurchased },
      unattributedReturns: { anlasFixed: p.returnedFixed, anlasPurchased: p.returnedPurchased },
      gapEventIds: { anlasFixed: p.fixedGap, anlasPurchased: p.purchasedGap },
      v5CapacityGap: p.v5Gap,
      v5GapEventId: p.v5GapEvent,
      pendingExternalFunding: this.sqlite
        .prepare<[string], OperatingAccount['pendingExternalFunding'][number]>(
          `SELECT p.receipt,p.source_event_id AS eventId,p.resource,p.destination,p.remaining,p.registered_at AS registeredAt,p.snapshot_at AS snapshotAt FROM pending_funding p JOIN domain_events e ON e.event_id=p.source_event_id WHERE p.account_id=? ORDER BY e.commit_seq,e.event_index`,
        )
        .all(accountId),
    };
  }
  private saveOperating(accountId: string, p: OperatingAccount, eventId: string) {
    this.sqlite
      .prepare(
        `UPDATE operating_state SET v5_available=?,anlas_fixed_available=?,anlas_purchased_available=?,unfunded_fixed=?,unfunded_purchased=?,unattributed_fixed=?,unattributed_purchased=?,fixed_gap_event_id=?,purchased_gap_event_id=?,v5_capacity_gap=?,v5_gap_event_id=?,source_event_id=? WHERE account_id=?`,
      )
      .run(
        p.available.v5,
        p.available.anlasFixed,
        p.available.anlasPurchased,
        p.unfunded.anlasFixed,
        p.unfunded.anlasPurchased,
        p.unattributedReturns.anlasFixed,
        p.unattributedReturns.anlasPurchased,
        p.gapEventIds.anlasFixed,
        p.gapEventIds.anlasPurchased,
        p.v5CapacityGap,
        p.v5GapEventId,
        eventId,
        accountId,
      );
  }
  private fact(id: string) {
    const row = this.sqlite
      .prepare<
        [string],
        {
          type: RelationalEvent['eventType'];
          accountId: string | null;
          payload: string;
          commitSeq: string;
        }
      >(
        `SELECT event_type AS type,account_id AS accountId,payload,CAST(commit_seq AS TEXT) AS commitSeq FROM domain_events WHERE event_id=?`,
      )
      .get(id);
    if (!row) {
      fail('来源事实不存在');
    }
    return { ...row, payload: JSON.parse(row.payload) as Record<string, unknown> };
  }
  private observation(id: string, accountId: string) {
    const e = this.fact(id),
      p = e.payload;
    if (e.accountId !== accountId || !['QuotaObserved', 'OpeningQuotaRecorded'].includes(e.type)) {
      fail('额度来源不是同账户观察');
    }
    if (e.type === 'QuotaObserved') {
      const p = e.payload as unknown as RelationalPayloads['QuotaObserved'];
      if (p.errorCode) {
        fail('失败观察不能作为金融来源');
      }
      return {
        at: p.observedAt,
        v5: p.v5,
        anlasFixed: p.anlasFixed,
        anlasPurchased: p.anlasPurchased,
        seconds: p.secondsPerPercent,
      };
    }
    const opening = p as unknown as RelationalPayloads['OpeningQuotaRecorded'];
    if (opening.observedAt === null) {
      fail('基线缺少实际观察时间');
    }
    return {
      at: opening.observedAt,
      v5: opening.observedV5,
      anlasFixed: opening.observedAnlasFixed,
      anlasPurchased: opening.observedAnlasPurchased,
      seconds: opening.secondsPerPercent,
    };
  }
  private account(accountId: string) {
    const a = this.sqlite
      .prepare<[string], { id: string; enabled: number; epoch: number; pst: string | null }>(
        `SELECT a.id,a.enabled,a.credential_epoch AS epoch,c.pst FROM upstream_accounts a LEFT JOIN upstream_credentials c ON c.account_id=a.id WHERE a.id=?`,
      )
      .get(accountId);
    if (!a) {
      fail('上游账户不存在', 404);
    }
    return a;
  }
  private state(accountId: string) {
    const q = this.readAccountQuota(accountId),
      a = this.account(accountId);
    const policy = this.sqlite
      .prepare<[], { enabled: number; accountId: string | null; configVersion: number }>(
        'SELECT enabled,bound_account_id AS accountId,config_version AS configVersion FROM sharing_policy WHERE id=1',
      )
      .get();
    if (!policy || policy.accountId !== accountId) {
      fail('金融命令只能使用当前绑定账户', 503);
    }
    const groups = this.sqlite
      .prepare<[], { id: string; name: string; share: number }>(
        'SELECT id,name,share FROM sharing_groups ORDER BY id',
      )
      .all();
    const memberships = this.sqlite
      .prepare<[], { memberId: string; groupId: string; weight: number }>(
        'SELECT member_id AS memberId,group_id AS groupId,weight FROM group_members ORDER BY member_id',
      )
      .all();
    const balances = Object.fromEntries(
      this.sqlite
        .prepare<
          [string],
          {
            memberId: string;
            v5Available: string;
            anlasFixedAvailable: string;
            anlasPurchasedAvailable: string;
          }
        >(
          `SELECT w.member_id AS memberId,w.v5_available AS v5Available,w.anlas_fixed_available AS anlasFixedAvailable,w.anlas_purchased_available AS anlasPurchasedAvailable FROM wallets w JOIN group_members g ON g.member_id=w.member_id WHERE w.account_id=? ORDER BY w.member_id`,
        )
        .all(accountId)
        .map(({ memberId, ...b }) => [
          memberId,
          {
            ...b,
            anlasAvailable: formatAmount(
              parseAmount(b.anlasFixedAvailable) + parseAmount(b.anlasPurchasedAvailable),
            ),
          },
        ]),
    );
    const reservations = Object.fromEntries(
      this.sqlite
        .prepare<
          [string],
          {
            id: string;
            userId: string;
            status: 'reserved';
            billingMode: string;
            v5: string;
            fixed: string;
            purchased: string;
            sharedV5Reserved: string;
          }
        >(
          `SELECT original_job_id AS id,original_member_id AS userId,status,billing_mode AS billingMode,v5,anlas_fixed AS fixed,anlas_purchased AS purchased,shared_v5_reserved AS sharedV5Reserved FROM charges WHERE original_account_id=? AND status='reserved'`,
        )
        .all(accountId)
        .map((c) => {
          let resource: 'free' | 'v5' | 'anlas' = 'anlas';
          if (c.billingMode === 'free') {
            resource = 'free';
          } else if (c.billingMode === 'v5') {
            resource = 'v5';
          }
          return [
            c.id,
            {
              ...c,
              resource,
              amount:
                resource === 'v5'
                  ? c.v5
                  : formatAmount(parseAmount(c.fixed) + parseAmount(c.purchased)),
            },
          ];
        }),
    );
    const account: FairAccount = {
      id: accountId,
      enabled: a.enabled === 1,
      quotaTier: q.quotaTier,
      subscriptionActive: q.subscriptionActive,
      subscriptionExpiresAt: iso(q.subscriptionExpiresAt),
      quotaCheckedAt: iso(q.observedAt),
      quotaError: q.observedCredentialEpoch === a.epoch ? q.quotaErrorCode : 'credential_changed',
      v5UsagePercent: q.observedV5,
      v5UsageIsNegative: q.observedV5 !== null && parseAmount(q.observedV5) < 0n,
      quotaFixed: q.observedAnlasFixed,
      quotaPurchased: q.observedAnlasPurchased,
      v5UsageTimeUntilNextPercent: q.secondsPerPercent,
    };
    let snapshot = null;
    if (q.baselineEventId) {
      const old = this.observation(q.baselineEventId, accountId);
      snapshot = fairQuotaSnapshot({
        ...account,
        quotaCheckedAt: iso(old.at),
        v5UsagePercent: old.v5,
        v5UsageIsNegative: old.v5 !== null && parseAmount(old.v5) < 0n,
        quotaFixed: old.anlasFixed,
        quotaPurchased: old.anlasPurchased,
        v5UsageTimeUntilNextPercent: old.seconds,
      });
    }
    const financial: FairQuotaSyncState = {
      enabled: policy.enabled === 1,
      boundAccountId: accountId,
      members: memberships.map((m) => m.memberId),
      groups: groups.map((g) => ({
        ...g,
        members: memberships.filter((m) => m.groupId === g.id).map((m) => m.memberId),
        memberWeights: Object.fromEntries(
          memberships.filter((m) => m.groupId === g.id).map((m) => [m.memberId, m.weight]),
        ),
      })),
      configVersion: policy.configVersion,
      balances,
      reservations,
      snapshot,
      spent: {
        v5: q.pendingV5Debit,
        anlasFixed: q.pendingFixedDebit,
        anlasPurchased: q.pendingPurchasedDebit,
        anlas: formatAmount(
          parseAmount(q.pendingFixedDebit) + parseAmount(q.pendingPurchasedDebit),
        ),
        fallbackFixed: q.pendingFallbackFixed,
        fallbackPurchased: q.pendingFallbackPurchased,
        fallbackV5: q.pendingFallbackV5,
      },
      creditedV5: '0',
      lastRefillAt: q.refillCursorAt ?? 0,
      lastSettlementAt: q.settlementCursorAt ?? 0,
      status: q.status,
      unallocatedV5: q.unallocatedV5,
      fallbackBuffer: { fixed: q.fallbackFixed, purchased: q.fallbackPurchased },
      sharedV5Debt: q.sharedV5Debt,
      operating: this.operating(accountId),
    };
    const ops = financial.operating!;
    const knownAmounts = [
      ...Object.values(ops.available),
      ...Object.values(ops.unfunded),
      ...Object.values(ops.unattributedReturns),
      ops.v5CapacityGap,
      q.sharedV5Debt,
      q.pendingV5Debit,
      q.pendingFixedDebit,
      q.pendingPurchasedDebit,
      q.pendingFallbackV5,
      q.pendingFallbackFixed,
      q.pendingFallbackPurchased,
      q.fallbackFixed,
      q.fallbackPurchased,
    ];
    if (
      (!snapshot &&
        (Object.values(balances).some(
          (b) => parseAmount(b.v5Available) !== 0n || parseAmount(b.anlasAvailable) !== 0n,
        ) ||
          Object.keys(reservations).length ||
          ops.pendingExternalFunding.length ||
          knownAmounts.some((n) => parseAmount(n) !== 0n))) ||
      (snapshot && (snapshot.v5 === null || snapshot.anlas === null))
    ) {
      fail('缺少金融基线，不能重新分配已有钱包');
    }
    return { q, account, db: { settings: { fairQuota: financial }, ledger: [] as unknown[] } };
  }
  private capture<T>(view: ReturnType<RelationalQuotaStore['state']>, run: () => T) {
    beginDomainCommand(view.db);
    try {
      const result = run();
      return { result, facts: finishDomainCommand(view.db)!.events };
    } catch (error) {
      finishDomainCommand(view.db);
      throw error;
    }
  }
  private event(
    fact: RelationalFact,
    accountId: string,
    metadata: Metadata,
    eventId: string = randomUUID(),
  ): RelationalEventInput {
    return {
      ...fact,
      eventId,
      streamId: `account:${accountId}`,
      accountId,
      memberId: 'memberId' in fact.payload ? fact.payload.memberId : null,
      occurredAt: metadata.recordedAt,
      policyVersion: metadata.policyVersion,
      pricingVersion: metadata.pricingVersion,
    };
  }
  private commit(metadata: Metadata, events: RelationalEventInput[], receipt: Json) {
    if (!events.length) {
      return { unchanged: true, receipt };
    }
    const { policyVersion: _policy, pricingVersion: _price, ...command } = metadata;
    const expectedVersions = Object.fromEntries(
      events.map((e) => [
        e.streamId,
        this.sqlite
          .prepare<[string], { v: string }>(
            'SELECT CAST(stream_version AS TEXT) AS v FROM stream_heads WHERE stream_id=?',
          )
          .get(e.streamId)?.v ?? '0',
      ]),
    );
    const result = appendRelationalCommit(this.sqlite, {
      ...command,
      events,
      expectedVersions,
      receipt,
    });
    this.applyQuotaEvents(result.events);
    return result;
  }
  beginQuotaObservation(accountId: string, startedAt = Date.now()): Ticket {
    const a = this.account(accountId),
      q = this.readAccountQuota(accountId);
    if (!a.pst || !Number.isSafeInteger(startedAt) || startedAt < 0) {
      fail('观察缺少当前私有凭据或实际时间', 503);
    }
    return {
      accountId,
      credentialEpoch: a.epoch,
      sequence: this.order.issue(`${accountId}:${a.epoch}`, q.observedSequence ?? '0'),
      startedAt,
      pst: a.pst,
    };
  }
  recordQuotaObservation(metadata: Metadata, ticket: Ticket, result: QuotaResult) {
    return this.owned(() => {
      if (metadata.actorKind === 'member' && metadata.actorId !== this.env.OWNER_USER_ID) {
        fail('额度观察只能由执行 owner 提交', 403);
      }
      const prior = readRelationalReceipt(this.sqlite, metadata);
      if (prior) {
        return prior;
      }
      const a = this.account(ticket.accountId),
        q = this.readAccountQuota(ticket.accountId);
      if (
        a.epoch !== ticket.credentialEpoch ||
        a.pst !== ticket.pst ||
        !this.order.current(`${a.id}:${a.epoch}`, ticket.sequence) ||
        BigInt(ticket.sequence) <= BigInt(q.observedSequence ?? '0')
      ) {
        return { ignored: true };
      }
      if (
        result.id !== ticket.accountId ||
        result.quotaCheckedAt !== new Date(ticket.startedAt).toISOString()
      ) {
        fail('观察结果账户不符');
      }
      const view = this.state(a.id);
      Object.assign(
        view.account,
        accountQuotaPatch(result, new Date(ticket.startedAt).toISOString()),
      );
      const snapshot = fairQuotaSnapshot(view.account),
        observationId = randomUUID();
      const money = (v: unknown) =>
        (typeof v === 'string' || typeof v === 'number') && Number.isFinite(Number(v))
          ? formatAmount(parseAmount(v, typeof v === 'number' ? 'nearest' : 'exact'))
          : null;
      const events: RelationalEventInput[] = [
        this.event(
          {
            eventType: 'QuotaObserved',
            payload: {
              accountId: a.id,
              credentialEpoch: a.epoch,
              sequence: ticket.sequence,
              observedAt: snapshot.at,
              quotaTier: typeof view.account.quotaTier === 'number' ? view.account.quotaTier : null,
              subscriptionActive:
                typeof view.account.subscriptionActive === 'boolean'
                  ? view.account.subscriptionActive
                  : null,
              subscriptionExpiresAt: isoTimestamp(view.account.subscriptionExpiresAt),
              v5:
                view.account.v5UsageIsNegative && snapshot.v5 !== null
                  ? '0'
                  : money(view.account.v5UsagePercent),
              anlasFixed: snapshot.anlasFixed,
              anlasPurchased: snapshot.anlasPurchased,
              secondsPerPercent: snapshot.seconds,
              errorCode: result.ok ? null : result.upstreamStatus,
            },
          },
          a.id,
          metadata,
          observationId,
        ),
      ];
      if (result.disableAccount && a.enabled === 1) {
        events.push(
          this.event(
            {
              eventType: 'AvailabilityChanged',
              payload: { accountId: a.id, enabled: false, reason: result.disableReason },
            },
            a.id,
            metadata,
          ),
        );
      }
      const planned = this.capture(view, () =>
        syncBoundFairQuota(view.db, view.account, metadata.recordedAt),
      );
      events.push(...this.convert(planned.facts, view, metadata, observationId));
      return this.commit(metadata, events, {
        accountId: a.id,
        sequence: ticket.sequence,
        ok: result.ok,
      });
    });
  }
  refillQuota(metadata: Metadata, accountId: string) {
    return this.owned(() => {
      if (metadata.actorKind === 'member' && metadata.actorId !== this.env.OWNER_USER_ID) {
        fail('恢复只能由执行 owner 提交', 403);
      }
      const prior = readRelationalReceipt(this.sqlite, metadata);
      if (prior) {
        return prior;
      }
      const view = this.state(accountId),
        planned = this.capture(view, () =>
          syncBoundFairQuota(view.db, view.account, metadata.recordedAt),
        );
      return this.commit(metadata, this.convert(planned.facts, view, metadata), { accountId });
    });
  }
  private convert(
    facts: DomainEventInput[],
    view: ReturnType<RelationalQuotaStore['state']>,
    metadata: Metadata,
    newObservationId?: string,
  ) {
    const accountId = view.q.accountId,
      events: RelationalEventInput[] = [];
    const sourceAt = (at: number): string => {
      if (newObservationId && at === fairQuotaSnapshot(view.account).at) {
        return newObservationId;
      }
      if (view.q.baselineEventId && this.observation(view.q.baselineEventId, accountId).at === at) {
        return view.q.baselineEventId;
      }
      if (
        view.q.observationEventId &&
        this.observation(view.q.observationEventId, accountId).at === at
      ) {
        return view.q.observationEventId;
      }
      fail('金融决定缺少原观察引用');
    };
    let allocationSource = newObservationId ?? view.q.observationEventId ?? view.q.baselineEventId;
    for (const raw of facts) {
      switch (raw.eventType) {
        case 'AvailabilityChanged': {
          const p = raw.payload;
          if (!('kind' in p) || p.kind !== 'quota') {
            fail('同步 owner 返回非额度可用性事实');
          }
          events.push(
            this.event(
              { eventType: 'QuotaStatusChanged', payload: { accountId, status: p.status } },
              accountId,
              metadata,
            ),
          );
          break;
        }
        case 'QuotaObserved': {
          const p = raw.payload;
          if (p.kind !== 'reconciled') {
            fail('同步 owner 返回非基线决定');
          }
          events.push(
            this.event(
              {
                eventType: 'QuotaBaselineAdopted',
                payload: {
                  accountId,
                  observationEventId: sourceAt(p.snapshot.at),
                  matchedDebits: { v5: view.q.pendingV5Debit, ...p.matchedDebits },
                  matchedFallbackFixed: p.matchedFallback.fixed,
                  matchedFallbackPurchased: p.matchedFallback.purchased,
                  refillCursorAt: p.at,
                },
              },
              accountId,
              metadata,
            ),
          );
          break;
        }
        case 'RefillApplied': {
          const p = raw.payload;
          events.push(
            this.event(
              {
                eventType: 'RefillApplied',
                payload: {
                  accountId,
                  from: p.from,
                  until: p.until,
                  observationEventId: sourceAt(p.snapshotAt),
                  budget: p.budget,
                  debtRepaid: p.debtRepaid,
                  capacityRecovered: p.capacityRecovered,
                  allocations: p.allocations,
                  configVersion: p.configVersion,
                },
              },
              accountId,
              metadata,
            ),
          );
          break;
        }
        case 'AllocationApplied': {
          const p = raw.payload;
          if (!allocationSource) {
            fail('分配缺少来源决定');
          }
          events.push(
            this.event(
              {
                eventType: 'AllocationApplied',
                payload: {
                  accountId,
                  reason: p.reason,
                  sourceEventId: allocationSource,
                  sourceResources: { ...zero(), [p.resource]: p.sourceAmount },
                  allocations: p.allocations.map((a) => ({
                    memberId: a.memberId,
                    resources: { ...zero(), [p.resource]: a.amount },
                  })),
                  configVersion: p.configVersion,
                },
              },
              accountId,
              metadata,
            ),
          );
          break;
        }
        case 'V5CapacityObserved': {
          const p = raw.payload;
          const evidence = p.evidence?.find((e) => e.type === 'v5_capacity_observed');
          events.push(
            this.event(
              {
                eventType: 'V5CapacityObserved',
                payload: {
                  accountId,
                  observationEventId: sourceAt(p.reconciliation.sourceCheckedAt),
                  capacityGap: p.capacityGap,
                  difference: p.reconciliation.difference,
                  tolerance: p.reconciliation.tolerance,
                  status: p.reconciliation.status,
                },
              },
              accountId,
              metadata,
              typeof evidence?.id === 'string' ? evidence.id : randomUUID(),
            ),
          );
          break;
        }
        case 'ResourceGapObserved': {
          const p = raw.payload;
          events.push(
            this.event(
              {
                eventType: 'ResourceGapObserved',
                payload: {
                  accountId,
                  resource: p.resource as 'anlasFixed' | 'anlasPurchased',
                  amount: p.observedAmount,
                  beforeObservationEventId: sourceAt(p.beforeSnapshot.at),
                  afterObservationEventId: sourceAt(p.afterSnapshot.at),
                  evidenceLevel: p.evidenceLevel,
                  actualConsumption: null,
                },
              },
              accountId,
              metadata,
              p.id,
            ),
          );
          break;
        }
        case 'ReturnClassified': {
          const p = raw.payload;
          if (p.kind === 'observed') {
            events.push(
              this.event(
                {
                  eventType: 'ReturnClassified',
                  payload: {
                    accountId,
                    resource: p.resource as 'anlasFixed' | 'anlasPurchased',
                    amount: p.amount,
                    gapEventId: p.gapEventId,
                    observationEventId: sourceAt(p.snapshotAt),
                    kind: 'observed',
                  },
                },
                accountId,
                metadata,
                p.id,
              ),
            );
          } else {
            if (!view.q.baselineEventId) {
              fail('人工修正缺少基线');
            }
            events.push(
              this.event(
                {
                  eventType: 'ReturnClassified',
                  payload: {
                    accountId,
                    resource: p.resource as 'anlasFixed' | 'anlasPurchased',
                    amount: p.correctedAmount,
                    gapEventId: p.previousGapEventId,
                    observationEventId: view.q.baselineEventId,
                    kind: 'gap_correction',
                  },
                },
                accountId,
                metadata,
                p.id,
              ),
            );
          }
          break;
        }
        case 'FundingRegistered': {
          const p = raw.payload.funding;
          events.push(
            this.event(
              {
                eventType: 'FundingRegistered',
                payload: {
                  receipt: p.receipt,
                  accountId,
                  resource: p.resource,
                  destination: p.destination,
                  remaining: p.remaining,
                  registeredAt: p.registeredAt,
                  snapshotAt: p.snapshotAt,
                },
              },
              accountId,
              metadata,
              p.eventId,
            ),
          );
          break;
        }
        case 'FundingReceived': {
          const p = raw.payload;
          const registered = p.source === 'registered_external_funding';
          const receipt = registered
            ? p.receiptEventId.replace(/^operating-receipt:/, '')
            : p.id.replace(/^operating-receipt:/, '');
          const received = this.event(
            {
              eventType: 'FundingReceived',
              payload: {
                accountId,
                receipt,
                resource: p.resource,
                amount: p.amount,
                destination: p.source === 'operator_share' ? 'operating' : p.destination,
                source: p.source,
                memberId: p.source === 'operator_share' ? p.memberId : null,
                observationEventId: registered ? sourceAt(p.snapshotAt) : null,
                receiptEventId: registered ? p.receiptEventId : null,
              },
            },
            accountId,
            metadata,
            p.id,
          );
          events.push(received);
          if (!newObservationId) {
            allocationSource = p.id;
          }
          break;
        }
        case 'DebitsConfirmed': {
          const p = raw.payload;
          events.push(
            this.event(
              {
                eventType: 'DebitsConfirmed',
                payload: {
                  accountId,
                  resource: p.resource as 'anlasFixed' | 'anlasPurchased',
                  amount: p.amount,
                  allocatedToMembers: p.allocatedToMembers,
                  evidenceLevel: p.evidenceLevel,
                },
              },
              accountId,
              metadata,
              p.id,
            ),
          );
          allocationSource = p.id;
          break;
        }
        case 'OperatingLossCovered': {
          const p = raw.payload;
          events.push(
            this.event(
              {
                eventType: 'OperatingLossCovered',
                payload: {
                  accountId,
                  resource: p.resource,
                  amount: p.amount,
                  lossEventId: p.lossEventId,
                },
              },
              accountId,
              metadata,
              p.id,
            ),
          );
          break;
        }
        default:
          fail(`Quota owner returned unsupported fact ${raw.eventType}`);
      }
    }
    return events;
  }
  private coveredAmount(id: string) {
    return formatAmount(
      this.sqlite
        .prepare<[string], { amount: string }>(
          `SELECT json_extract(payload,'$.amount') AS amount FROM domain_events WHERE event_type='OperatingLossCovered' AND json_extract(payload,'$.lossEventId')=?`,
        )
        .all(id)
        .reduce((n, e) => n + parseAmount(e.amount), 0n),
    );
  }
  private fundingEvent(id: string | undefined): ReturnType<OperatingFundingContext['findEvent']> {
    if (!id) {
      return null;
    }
    const exists = this.sqlite.prepare('SELECT 1 FROM domain_events WHERE event_id=?').get(id);
    if (!exists) {
      return null;
    }
    const e = this.fact(id),
      p = e.payload;
    const receipt = this.sqlite
      .prepare<[bigint], { receipt: string }>('SELECT receipt FROM commits WHERE commit_seq=?')
      .get(BigInt(e.commitSeq));
    const saved =
      receipt &&
      (JSON.parse(receipt.receipt) as {
        funding?: ReturnType<OperatingFundingContext['findEvent']>;
      });
    if (saved?.funding?.id === id) {
      return saved.funding;
    }
    const accountId = e.accountId;
    if (!accountId || (typeof p.resource !== 'string' && e.type !== 'V5CapacityObserved')) {
      return null;
    }
    const ops = this.operating(accountId);
    let resource = p.resource as 'anlasFixed' | 'anlasPurchased' | 'v5',
      type: string,
      amount: string;
    if (e.type === 'V5CapacityObserved') {
      resource = 'v5';
      type = 'v5_capacity_observed';
      amount = String(p.capacityGap);
    } else if (e.type === 'ResourceGapObserved') {
      type = 'resource_gap_observed';
      amount = String(p.amount);
      if (resource !== 'v5' && ops.gapEventIds[resource] === id) {
        amount = formatAmount(
          parseAmount(ops.unfunded[resource]) + parseAmount(this.coveredAmount(id)),
        );
      }
    } else if (e.type === 'ReturnClassified' && p.kind === 'gap_correction') {
      type = 'resource_gap_reconciled';
      amount =
        resource === 'v5'
          ? '0'
          : formatAmount(parseAmount(ops.unfunded[resource]) + parseAmount(this.coveredAmount(id)));
    } else {
      return null;
    }
    return { id, type, accountId, resource, amount };
  }
  manageQuotaFunding(metadata: Metadata, accountId: string, body: OperatingFundingInput) {
    return this.owned(() => {
      const actorId = this.env.OWNER_USER_ID;
      if (
        !actorId ||
        metadata.actorKind === 'system' ||
        (metadata.actorKind === 'member' && metadata.actorId !== actorId)
      ) {
        fail('资金操作需要当前 owner 或管理员授权', 403);
      }
      const prior = readRelationalReceipt(this.sqlite, metadata);
      if (prior) {
        return prior;
      }
      const view = this.state(accountId);
      const planned = this.capture(view, () => {
        syncBoundFairQuota(view.db, view.account, metadata.recordedAt);
        return manageOperatingFunding(
          view.db,
          body,
          {
            actorId,
            findEvent: (id) => this.fundingEvent(id),
            coveredAmount: (id) => this.coveredAmount(id),
          },
          metadata.recordedAt,
        );
      });
      if (planned.result.duplicate) {
        const event = this.fact(planned.result.eventId);
        return {
          replayed: true,
          commitSeq: event.commitSeq,
          receipt: JSON.parse(
            this.sqlite
              .prepare<[bigint], { receipt: string }>(
                'SELECT receipt FROM commits WHERE commit_seq=?',
              )
              .get(BigInt(event.commitSeq))!.receipt,
          ),
        };
      }
      const funding = view.db.ledger.find(
        (raw) => (raw as { id?: string }).id === planned.result.eventId,
      );
      return this.commit(metadata, this.convert(planned.facts, view, metadata), {
        accountId,
        eventId: planned.result.eventId,
        funding,
      } as Json);
    });
  }
  private wallet(
    accountId: string,
    memberId: string,
    delta: ResourceAmounts,
    direction: 1 | -1,
    eventId: string,
  ) {
    const p = this.sqlite
      .prepare<[string, string], ResourceAmounts>(
        'SELECT v5_available AS v5,anlas_fixed_available AS anlasFixed,anlas_purchased_available AS anlasPurchased FROM wallets WHERE account_id=? AND member_id=?',
      )
      .get(accountId, memberId);
    if (!p || !this.sqlite.prepare('SELECT 1 FROM group_members WHERE member_id=?').get(memberId)) {
      fail('分配或划入缺少同账户当前成员钱包');
    }
    const next = zero();
    for (const r of resources) {
      next[r] = formatAmount(parseAmount(p[r]) + BigInt(direction) * parseAmount(delta[r]));
      if (direction === -1 && parseAmount(delta[r]) > 0n && parseAmount(next[r]) < 0n) {
        fail('原资源余额不足');
      }
    }
    this.sqlite
      .prepare(
        'UPDATE wallets SET v5_available=?,anlas_fixed_available=?,anlas_purchased_available=?,source_event_id=? WHERE account_id=? AND member_id=?',
      )
      .run(next.v5, next.anlasFixed, next.anlasPurchased, eventId, accountId, memberId);
  }
  private subtract(before: string, amount: string) {
    const next = parseAmount(before) - parseAmount(amount);
    if (next < 0n) {
      fail('金融事实超过原资源或未决数量');
    }
    return formatAmount(next);
  }
  private applyQuotaEvents(events: RelationalEvent[]) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Quota projection requires the owning transaction.');
    }
    for (const e of events) {
      if (
        !relationalQuotaFacts.has(e.eventType) ||
        !e.accountId ||
        e.streamId !== `account:${e.accountId}`
      ) {
        fail('Quota projection received another owner or account stream');
      }
      const accountId = e.accountId,
        q = this.readAccountQuota(accountId),
        a = this.account(accountId),
        ops = this.operating(accountId);
      switch (e.eventType) {
        case 'QuotaObserved': {
          const p = e.payload;
          if (
            p.credentialEpoch !== a.epoch ||
            BigInt(p.sequence) <= BigInt(q.observedSequence ?? '0')
          ) {
            fail('观察代次或序号已经过期');
          }
          Object.assign(q, {
            observationEventId: e.eventId,
            observedAt: p.observedAt,
            observedCredentialEpoch: p.credentialEpoch,
            observedSequence: p.sequence,
            quotaTier: p.quotaTier,
            subscriptionActive: p.subscriptionActive,
            subscriptionExpiresAt: p.subscriptionExpiresAt,
            quotaErrorCode: p.errorCode,
            observedV5: p.v5,
            observedAnlasFixed: p.anlasFixed,
            observedAnlasPurchased: p.anlasPurchased,
            secondsPerPercent: p.secondsPerPercent,
          });
          this.saveAccountQuota(q, e.eventId);
          break;
        }
        case 'AvailabilityChanged':
          this.sqlite
            .prepare('UPDATE upstream_accounts SET enabled=?,source_event_id=? WHERE id=?')
            .run(Number(e.payload.enabled), e.eventId, accountId);
          break;
        case 'QuotaStatusChanged':
          q.status = e.payload.status;
          this.saveAccountQuota(q, e.eventId);
          break;
        case 'QuotaBaselineAdopted': {
          const p = e.payload,
            observation = this.observation(p.observationEventId, accountId);
          const old = q.baselineEventId ? this.observation(q.baselineEventId, accountId) : null;
          if (
            q.observationEventId !== p.observationEventId ||
            q.status !== 'active' ||
            observation.v5 === null ||
            observation.anlasFixed === null ||
            observation.anlasPurchased === null ||
            q.quotaErrorCode ||
            q.observedCredentialEpoch !== a.epoch ||
            (old && observation.at <= old.at) ||
            observation.at < (q.settlementCursorAt ?? 0) ||
            this.sqlite
              .prepare("SELECT 1 FROM charges WHERE original_account_id=? AND status='reserved'")
              .get(accountId)
          ) {
            fail('基线不能覆盖较新、在途或失败观察');
          }
          q.pendingV5Debit = this.subtract(q.pendingV5Debit, p.matchedDebits.v5);
          if (q.pendingV5Debit !== '0') {
            fail('基线缺少原 V5 待核对扣款');
          }
          q.pendingFixedDebit = this.subtract(q.pendingFixedDebit, p.matchedDebits.anlasFixed);
          q.pendingPurchasedDebit = this.subtract(
            q.pendingPurchasedDebit,
            p.matchedDebits.anlasPurchased,
          );
          q.fallbackFixed = this.subtract(q.fallbackFixed, p.matchedFallbackFixed);
          q.fallbackPurchased = this.subtract(q.fallbackPurchased, p.matchedFallbackPurchased);
          q.pendingFallbackV5 = q.pendingFallbackFixed = q.pendingFallbackPurchased = '0';
          q.baselineEventId = p.observationEventId;
          q.refillCursorAt = p.refillCursorAt;
          this.saveAccountQuota(q, e.eventId);
          break;
        }
        case 'RefillApplied': {
          const p = e.payload,
            source = this.observation(p.observationEventId, accountId);
          this.configuration(p.configVersion);
          if (
            q.baselineEventId !== p.observationEventId ||
            p.from !== (q.refillCursorAt ?? 0) ||
            p.until < p.from ||
            p.until > source.at + 600000 ||
            new Set(p.allocations.map((x) => x.memberId)).size !== p.allocations.length
          ) {
            fail('恢复来源、原游标或分配身份不符');
          }
          const allocated = p.allocations.reduce((n, x) => n + parseAmount(x.amount), 0n);
          if (
            allocated + parseAmount(p.debtRepaid) + parseAmount(p.capacityRecovered) >
            parseAmount(p.budget)
          ) {
            fail('恢复分配超过来源预算');
          }
          q.sharedV5Debt = this.subtract(q.sharedV5Debt, p.debtRepaid);
          ops.v5CapacityGap = this.subtract(ops.v5CapacityGap, p.capacityRecovered);
          for (const allocation of p.allocations) {
            this.wallet(
              accountId,
              allocation.memberId,
              { ...zero(), v5: allocation.amount },
              1,
              e.eventId,
            );
          }
          q.refillCursorAt = p.until;
          this.saveAccountQuota(q, e.eventId);
          this.saveOperating(accountId, ops, e.eventId);
          break;
        }
        case 'AllocationApplied': {
          const p = e.payload,
            source = this.fact(p.sourceEventId);
          this.configuration(p.configVersion);
          if (
            source.accountId !== accountId ||
            ![
              'QuotaObserved',
              'OpeningQuotaRecorded',
              'FundingReceived',
              'DebitsConfirmed',
            ].includes(source.type) ||
            new Set(p.allocations.map((x) => x.memberId)).size !== p.allocations.length
          ) {
            fail('分配来源或成员身份不符');
          }
          for (const r of resources) {
            if (
              p.allocations.reduce((n, x) => n + parseAmount(x.resources[r]), 0n) >
              parseAmount(p.sourceResources[r])
            ) {
              fail('分配超过原来源预算');
            }
          }
          for (const allocation of p.allocations) {
            this.wallet(accountId, allocation.memberId, allocation.resources, 1, e.eventId);
          }
          break;
        }
        case 'V5CapacityObserved': {
          const p = e.payload;
          this.observation(p.observationEventId, accountId);
          if (ops.v5CapacityGap !== p.capacityGap) {
            ops.v5GapEventId = p.capacityGap === '0' ? null : e.eventId;
          }
          ops.v5CapacityGap = p.capacityGap;
          this.saveOperating(accountId, ops, e.eventId);
          break;
        }
        case 'ResourceGapObserved': {
          const p = e.payload;
          this.observation(p.beforeObservationEventId, accountId);
          this.observation(p.afterObservationEventId, accountId);
          if (
            q.baselineEventId !== p.beforeObservationEventId ||
            q.observationEventId !== p.afterObservationEventId
          ) {
            fail('差额必须引用原基线与新观察');
          }
          ops.unfunded[p.resource] = formatAmount(
            parseAmount(ops.unfunded[p.resource]) + parseAmount(p.amount),
          );
          ops.gapEventIds[p.resource] = e.eventId;
          this.saveOperating(accountId, ops, e.eventId);
          break;
        }
        case 'ReturnClassified': {
          const p = e.payload;
          this.observation(p.observationEventId, accountId);
          if (p.gapEventId !== ops.gapEventIds[p.resource]) {
            fail('返回归属关联的原差额已变化');
          }
          if (p.kind === 'observed') {
            const next = parseAmount(ops.unattributedReturns[p.resource]) + parseAmount(p.amount);
            if (next < 0n) {
              fail('返回回落超过原待归属额');
            }
            ops.unattributedReturns[p.resource] = formatAmount(next);
          } else {
            ops.unfunded[p.resource] = this.subtract(ops.unfunded[p.resource], p.amount);
            ops.unattributedReturns[p.resource] = this.subtract(
              ops.unattributedReturns[p.resource],
              p.amount,
            );
            ops.gapEventIds[p.resource] = e.eventId;
          }
          this.saveOperating(accountId, ops, e.eventId);
          break;
        }
        case 'FundingRegistered': {
          const p = e.payload;
          if (!['members', 'operating'].includes(p.destination)) {
            fail('资金归属无效');
          }
          this.sqlite
            .prepare(
              'INSERT INTO pending_funding(receipt,account_id,resource,destination,remaining,registered_at,snapshot_at,source_event_id) VALUES(?,?,?,?,?,?,?,?)',
            )
            .run(
              p.receipt,
              accountId,
              p.resource,
              p.destination,
              p.remaining,
              p.registeredAt,
              p.snapshotAt,
              e.eventId,
            );
          break;
        }
        case 'FundingReceived': {
          const p = e.payload;
          if (!['members', 'operating'].includes(p.destination)) {
            fail('资金归属无效');
          }
          if (p.source === 'operator_share') {
            if (p.destination !== 'operating' || !p.memberId || e.memberId !== p.memberId) {
              fail('本人划入成员身份不符');
            }
            this.wallet(
              accountId,
              p.memberId,
              { ...zero(), [p.resource]: p.amount },
              -1,
              e.eventId,
            );
          } else {
            if (p.resource === 'v5') {
              fail('外部凭据不能制造 V5 资金');
            }
            if (p.source === 'declared_external_receipt') {
              ops.unattributedReturns[p.resource] = this.subtract(
                ops.unattributedReturns[p.resource],
                p.amount,
              );
            } else {
              const pending = ops.pendingExternalFunding.find(
                (x) => x.eventId === p.receiptEventId,
              );
              const source = this.observation(p.observationEventId!, accountId);
              if (
                !pending ||
                pending.receipt !== p.receipt ||
                pending.resource !== p.resource ||
                pending.destination !== p.destination ||
                pending.registeredAt > source.at
              ) {
                fail('到账必须匹配原收据、资源、归属及有效观察');
              }
              const remaining = this.subtract(pending.remaining, p.amount);
              if (remaining === '0') {
                this.sqlite
                  .prepare('DELETE FROM pending_funding WHERE receipt=? AND account_id=?')
                  .run(pending.receipt, accountId);
              } else {
                this.sqlite
                  .prepare(
                    'UPDATE pending_funding SET remaining=? WHERE receipt=? AND account_id=?',
                  )
                  .run(remaining, pending.receipt, accountId);
              }
            }
          }
          if (p.destination === 'operating') {
            ops.available[p.resource] = formatAmount(
              parseAmount(ops.available[p.resource]) + parseAmount(p.amount),
            );
          }
          this.saveOperating(accountId, ops, e.eventId);
          break;
        }
        case 'DebitsConfirmed': {
          const p = e.payload;
          if (
            this.sqlite
              .prepare("SELECT 1 FROM charges WHERE original_account_id=? AND status='reserved'")
              .get(accountId) ||
            !q.baselineEventId ||
            this.observation(q.baselineEventId, accountId).at < (q.settlementCursorAt ?? 0) ||
            parseAmount(p.allocatedToMembers) > parseAmount(p.amount)
          ) {
            fail('扣款确认缺少已结清的观察或原资源预算');
          }
          if (p.resource === 'anlasFixed') {
            q.pendingFixedDebit = this.subtract(q.pendingFixedDebit, p.amount);
          } else {
            q.pendingPurchasedDebit = this.subtract(q.pendingPurchasedDebit, p.amount);
          }
          this.saveAccountQuota(q, e.eventId);
          break;
        }
        case 'OperatingLossCovered': {
          const p = e.payload;
          const loss = this.fact(p.lossEventId);
          if (
            loss.accountId !== accountId ||
            !['ResourceGapObserved', 'ReturnClassified', 'V5CapacityObserved'].includes(
              loss.type,
            ) ||
            (loss.type === 'ReturnClassified' && loss.payload.kind !== 'gap_correction') ||
            (p.resource === 'v5' ? ops.v5GapEventId : ops.gapEventIds[p.resource]) !==
              p.lossEventId ||
            (loss.type === 'V5CapacityObserved' ? 'v5' : loss.payload.resource) !== p.resource
          ) {
            fail('核销与原同池差额身份不符');
          }
          ops.available[p.resource] = this.subtract(ops.available[p.resource], p.amount);
          if (p.resource === 'v5') {
            ops.v5CapacityGap = this.subtract(ops.v5CapacityGap, p.amount);
          } else {
            ops.unfunded[p.resource] = this.subtract(ops.unfunded[p.resource], p.amount);
          }
          this.saveOperating(accountId, ops, e.eventId);
          break;
        }
        default:
          fail(`Unsupported quota projection ${e.eventType}`);
      }
    }
  }
  private configuration(version: number) {
    if (
      this.sqlite
        .prepare<[], { version: number }>(
          'SELECT config_version AS version FROM sharing_policy WHERE id=1',
        )
        .get()?.version !== version
    ) {
      fail('金融决定与原份额版本不符');
    }
  }
  replayQuotaCommit(stored: StoredRelationalCommit) {
    return this.owned(() => {
      if (stored.events.some((e) => !relationalQuotaFacts.has(e.eventType))) {
        fail('Quota replay requires one complete quota commit');
      }
      const result = replayRelationalCommit(this.sqlite, stored);
      if (!result.replayed) {
        this.applyQuotaEvents(result.events);
      }
      return result;
    });
  }
}
