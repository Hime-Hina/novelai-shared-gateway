import { assertCurrentOwnerCommand, type MemberUser } from '../members/authentication.ts';
import { domainDigest, canonicalJson } from '../events/domain-events.ts';
import {
  configureFreeImages,
  freeImagePolicy,
  type FreeImagePolicy,
} from '../quota/free-image-policy.ts';
import {
  updateLossAlertPolicy,
  lossAlertPolicy,
  type LossAlertPolicy,
} from '../quota/failure-accounting.ts';
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
  configureAnlasFallback,
  configureFairGroups,
  normalizeFairGroups,
  assertFairGroupMemberCount,
  assertFairGroupTransition,
  allocateFairGroupAmount,
  type FairQuotaState,
  type FairQuotaDatabase,
  type FairGroup,
  type FairQuotaSyncState,
  type FairQuotaReservationState,
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
import {
  quotaTimestamp as isoTimestamp,
  isUsableOpus,
  isQuotaSnapshotFresh,
  quotaNumber,
} from '../quota/eligibility.ts';

type Metadata = Omit<RelationalCommand, 'events' | 'expectedVersions' | 'receipt'> & {
  policyVersion: string;
  pricingVersion: string;
};
export type QuotaPolicyChange =
  | { action: 'fallback-policy'; enabled: boolean }
  | {
      action: 'free-image-policy';
      expectedVersion: number;
      policy: Omit<FreeImagePolicy, 'version'>;
    }
  | {
      action: 'loss-alert-policy';
      expectedVersion: number;
      policy: Omit<LossAlertPolicy, 'version'>;
    };
type SharingPolicy = {
  enabled: number;
  accountId: string | null;
  configVersion: number;
  fallback: number;
  freeVersion: number;
  freeEnabled: number;
  warningThreshold: number | null;
  hardLimit: number | null;
  lossVersion: number;
  v5_24h: number;
  fixed24h: string;
  fixed30d: string;
  memberAttempts24h: number;
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
  'SharingConfigured',
  'GroupBalancesRedistributed',
  'FallbackPolicyChanged',
  'FreePolicyChanged',
  'LossPolicyChanged',
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
    this.insertAccountQuota(p, e.eventId);
  }
  private insertAccountQuota(p: AccountQuota, eventId: string) {
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
        eventId,
      });
  }
  registerAccountQuota(event: Extract<RelationalEvent, { eventType: 'AccountRegistered' }>) {
    if (
      !this.sqlite.inTransaction ||
      !this.sqlite
        .prepare(
          "SELECT 1 FROM domain_events WHERE event_id=? AND event_type='AccountRegistered' AND account_id=?",
        )
        .get(event.eventId, event.payload.accountId)
    ) {
      fail('初始额度状态缺少账户注册事实');
    }
    const q: AccountQuota = {
      accountId: event.payload.accountId,
      status: 'inactive_opus',
      observationEventId: null,
      observedAt: null,
      observedCredentialEpoch: null,
      observedSequence: null,
      quotaTier: null,
      subscriptionActive: null,
      subscriptionExpiresAt: null,
      quotaErrorCode: null,
      observedV5: null,
      observedAnlasFixed: null,
      observedAnlasPurchased: null,
      secondsPerPercent: null,
      baselineEventId: null,
      refillCursorAt: null,
      settlementCursorAt: null,
      unallocatedV5: '0',
      fallbackFixed: '0',
      fallbackPurchased: '0',
      sharedV5Debt: '0',
      pendingV5Debit: '0',
      pendingFixedDebit: '0',
      pendingPurchasedDebit: '0',
      pendingFallbackV5: '0',
      pendingFallbackFixed: '0',
      pendingFallbackPurchased: '0',
    };
    this.insertAccountQuota(q, event.eventId);
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
  private groupState() {
    const groups = this.sqlite
        .prepare<[], { id: string; name: string; share: number }>(
          'SELECT id,name,share FROM sharing_groups ORDER BY id',
        )
        .all(),
      memberships = this.sqlite
        .prepare<[], { memberId: string; groupId: string; weight: number }>(
          'SELECT member_id AS memberId,group_id AS groupId,weight FROM group_members ORDER BY member_id',
        )
        .all();
    return {
      members: memberships.map((member) => member.memberId),
      groups: groups.map((group): FairGroup => ({
        ...group,
        members: memberships
          .filter((member) => member.groupId === group.id)
          .map((member) => member.memberId),
        memberWeights: Object.fromEntries(
          memberships
            .filter((member) => member.groupId === group.id)
            .map((member) => [member.memberId, member.weight]),
        ),
      })),
    };
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
    const configuration = this.groupState();
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
      members: configuration.members,
      groups: configuration.groups,
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
  private capture<T>(view: { db: object }, run: () => T) {
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
    this.assertGroupCommit(result.events, result.receipt);
    return result;
  }
  private sharingPolicy(): SharingPolicy {
    const row = this.sqlite
      .prepare<[], SharingPolicy>(
        `SELECT enabled,bound_account_id AS accountId,config_version AS configVersion,auto_anlas_fallback AS fallback,
      free_policy_version AS freeVersion,free_enabled AS freeEnabled,free_warning_threshold AS warningThreshold,free_hard_limit AS hardLimit,
      loss_policy_version AS lossVersion,loss_v5_24h AS v5_24h,loss_fixed_24h AS fixed24h,loss_fixed_30d AS fixed30d,
      loss_member_attempts_24h AS memberAttempts24h FROM sharing_policy WHERE id=1`,
      )
      .get();
    if (!row) {
      fail('共享政策尚未初始化', 503);
    }
    return row;
  }
  private authorizeOwnerCommand(metadata: Metadata, member: MemberUser | null) {
    const current =
      member &&
      this.sqlite
        .prepare<[string], { status: string; authEpoch: string | null }>(
          'SELECT status,auth_epoch AS authEpoch FROM members WHERE id=?',
        )
        .get(member.id);
    assertCurrentOwnerCommand(metadata, member, current, this.env);
  }
  configureGroups(
    metadata: Metadata,
    input: { groups: unknown; expectedConfigVersion: number; redistributeRemaining?: boolean },
    member: MemberUser | null,
  ) {
    return this.owned(() => {
      this.authorizeOwnerCommand(metadata, member);
      if (
        !metadata.actorId ||
        !input ||
        typeof input !== 'object' ||
        Array.isArray(input) ||
        Object.keys(input).some(
          (key) => !['groups', 'expectedConfigVersion', 'redistributeRemaining'].includes(key),
        ) ||
        (input.redistributeRemaining !== undefined &&
          typeof input.redistributeRemaining !== 'boolean')
      ) {
        fail('分组命令无效', 400);
      }
      const command = {
          ...metadata,
          fingerprint: domainDigest({
            command: metadata.fingerprint,
            input: { ...input, redistributeRemaining: input.redistributeRemaining === true },
          }),
        },
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return prior;
      }
      const hasPolicy = !!this.sqlite.prepare('SELECT 1 FROM sharing_policy WHERE id=1').get(),
        policy = hasPolicy ? this.sharingPolicy() : null;
      if (
        !Number.isSafeInteger(input.expectedConfigVersion) ||
        input.expectedConfigVersion !== (policy?.configVersion ?? 1)
      ) {
        fail('分组版本缺失或已变化，请保留草稿并刷新后重新编辑');
      }
      const configuration = this.groupState(),
        financial = policy?.accountId ? this.state(policy.accountId) : null;
      if (
        !policy?.accountId &&
        this.sqlite.prepare('SELECT 1 FROM upstream_accounts LIMIT 1').get()
      ) {
        fail('未绑定账户须先完成账户配置');
      }
      const state: FairQuotaState | null = policy
        ? {
            ...(financial?.db.settings.fairQuota ?? {
              enabled: policy.enabled === 1,
              boundAccountId: null,
              ...configuration,
              balances: Object.fromEntries(
                configuration.members.map((id) => [
                  id,
                  {
                    v5Available: '0',
                    anlasFixedAvailable: '0',
                    anlasPurchasedAvailable: '0',
                    anlasAvailable: '0',
                  },
                ]),
              ),
              snapshot: null,
              spent: {
                ...zero(),
                anlas: '0',
                fallbackFixed: '0',
                fallbackPurchased: '0',
                fallbackV5: '0',
              },
              creditedV5: '0',
              lastRefillAt: 0,
              lastSettlementAt: 0,
              status: 'pending_opus',
              unallocatedV5: '0',
              fallbackBuffer: { fixed: '0', purchased: '0' },
              sharedV5Debt: '0',
            }),
            version: 2,
            configVersion: policy.configVersion,
            configurationHistory: [],
            fallbackTotalsByUser: {},
            // 原分组 owner 的 idle 断言先拒绝预留；合法路径不读取窄预留之外的派发字段。
            reservations: (financial?.db.settings.fairQuota.reservations ??
              {}) as FairQuotaState['reservations'],
            autoAnlasFallback: policy.fallback === 1,
            freeImagePolicy: {
              version: policy.freeVersion,
              enabled: policy.freeEnabled === 1,
              warningThreshold: policy.warningThreshold,
              hardLimit: policy.hardLimit,
            },
            lossAlertPolicy: {
              version: policy.lossVersion,
              v5Points24h: policy.v5_24h,
              fixedAnlas24h: Number(policy.fixed24h),
              fixedAnlas30d: Number(policy.fixed30d),
              memberAttempts24h: policy.memberAttempts24h,
            },
          }
        : null;
      const db: FairQuotaDatabase = {
        settings: { fairQuota: state },
        accounts: financial ? [financial.account] : [],
        users: this.sqlite
          .prepare<[], { id: string; enabled: number }>(
            "SELECT id,(status='active') AS enabled FROM members WHERE status!='removed' ORDER BY id",
          )
          .all()
          .map((user) => ({ id: user.id, enabled: user.enabled === 1 })),
        jobs: this.sqlite
          .prepare<[], { id: string; status: string }>(
            "SELECT id,status FROM generation_jobs WHERE status IN ('queued','pending','running')",
          )
          .all(),
        ledger: [],
      };
      const planned = this.capture({ db }, () => {
          const result = configureFairGroups(db, input.groups, {
            now: metadata.recordedAt,
            actorId: metadata.actorId!,
            redistributeRemaining: input.redistributeRemaining === true,
          });
          if (financial) {
            syncBoundFairQuota(
              db as { settings: { fairQuota: FairQuotaState } },
              financial.account,
              metadata.recordedAt,
            );
          }
          return result;
        }),
        result = planned.result,
        free = freeImagePolicy(db),
        loss = lossAlertPolicy(result.lossAlertPolicy),
        anchor = planned.facts.findIndex(
          (fact) =>
            fact.eventType === 'SharingConfigured' || fact.eventType === 'AllocationChanged',
        );
      if (anchor < 0) {
        throw new Error('Group owner omitted its configuration decision.');
      }
      const events = financial
        ? this.convert(planned.facts.slice(0, anchor), financial, metadata)
        : [];
      const sharingEvent = (fact: RelationalFact): RelationalEventInput => ({
        ...fact,
        eventId: randomUUID(),
        streamId: 'sharing:default',
        accountId: null,
        memberId: 'memberId' in fact.payload ? fact.payload.memberId : null,
        occurredAt: metadata.recordedAt,
        policyVersion: metadata.policyVersion,
        pricingVersion: metadata.pricingVersion,
      });
      const configurationEvent = sharingEvent({
        eventType: 'SharingConfigured',
        payload: {
          enabled: result.enabled,
          boundAccountId: result.boundAccountId,
          configVersion: result.configVersion,
          autoAnlasFallback: result.autoAnlasFallback === true,
          freePolicyVersion: free.version,
          freeEnabled: free.enabled,
          freeWarningThreshold: free.warningThreshold,
          freeHardLimit: free.hardLimit,
          lossPolicyVersion: loss.version,
          lossV5_24h: loss.v5Points24h,
          lossFixed24h: String(loss.fixedAnlas24h),
          lossFixed30d: String(loss.fixedAnlas30d),
          lossMemberAttempts24h: loss.memberAttempts24h,
          redistributeRemaining: input.redistributeRemaining === true,
          groups: result.groups.map((group) => ({
            groupId: group.id,
            name: group.name!,
            share: group.share,
            members: group.members.map((memberId) => ({
              memberId,
              weight: group.memberWeights[memberId],
            })),
          })),
        },
      });
      events.push(configurationEvent);
      for (const fact of planned.facts.slice(anchor + 1)) {
        if (fact.eventType !== 'FundsTransferred' || fact.payload.kind !== 'redistribution') {
          if (!financial) {
            throw new Error('Unbound group owner emitted an account decision.');
          }
          events.push(...this.convert([fact], financial, metadata));
          continue;
        }
        const grouped = new Map<string, Map<string, ResourceAmounts>>();
        for (const redistribution of fact.payload.redistributions) {
          const allocations =
            grouped.get(redistribution.groupId) ?? new Map<string, ResourceAmounts>();
          for (const allocation of redistribution.allocations) {
            const amount = allocations.get(allocation.memberId) ?? zero();
            amount[redistribution.resource] = allocation.amount;
            allocations.set(allocation.memberId, amount);
          }
          grouped.set(redistribution.groupId, allocations);
        }
        for (const [groupId, allocations] of grouped) {
          if (!financial || !result.groups.some((group) => group.id === groupId)) {
            throw new Error('Redistribution omitted its original group source.');
          }
          const sourceResources = zero();
          for (const r of resources) {
            sourceResources[r] = formatAmount(
              [...allocations.values()].reduce(
                (total, allocation) => total + parseAmount(allocation[r]),
                0n,
              ),
            );
          }
          events.push(
            this.event(
              {
                eventType: 'GroupBalancesRedistributed',
                payload: {
                  accountId: financial.account.id,
                  configurationEventId: configurationEvent.eventId!,
                  groupId,
                  sourceResources,
                  allocations: [...allocations].map(([memberId, resources]) => ({
                    memberId,
                    resources,
                  })),
                  configVersion: result.configVersion,
                },
              },
              financial.account.id,
              metadata,
            ),
          );
        }
      }
      return this.commit(command, events, {
        groups: result.groups.map((group) => ({
          id: group.id,
          name: group.name!,
          share: group.share,
          members: [...group.members],
          memberWeights: { ...group.memberWeights },
        })),
        status: result.status,
        configVersion: result.configVersion,
      });
    });
  }
  assertGroupCommit(events: RelationalEvent[], receipt: Json) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Group validation requires the owning transaction.');
    }
    const configurations = events.filter((event) => event.eventType === 'SharingConfigured'),
      groupReceipt =
        receipt &&
        !Array.isArray(receipt) &&
        typeof receipt === 'object' &&
        'groups' in receipt &&
        'status' in receipt &&
        'configVersion' in receipt;
    if (!configurations.length && !groupReceipt) {
      return;
    }
    if (configurations.length !== 1 || !groupReceipt) {
      fail('分组回执必须关联唯一原配置决定');
    }
    const p = configurations[0].payload,
      groups = p.groups.map((group) => ({
        id: group.groupId,
        name: group.name,
        share: group.share,
        members: group.members.map((member) => member.memberId),
        memberWeights: Object.fromEntries(
          group.members.map((member) => [member.memberId, member.weight]),
        ),
      })),
      status = p.boundAccountId ? this.readAccountQuota(p.boundAccountId).status : 'pending_opus';
    if (
      receipt.configVersion !== p.configVersion ||
      receipt.status !== status ||
      canonicalJson(receipt.groups) !== canonicalJson(groups)
    ) {
      fail('分组回执必须保留原完整配置结果');
    }
  }
  configureQuotaPolicy(metadata: Metadata, input: QuotaPolicyChange, member: MemberUser | null) {
    return this.owned(() => {
      this.authorizeOwnerCommand(metadata, member);
      if (!metadata.actorId) {
        fail('政策命令缺少操作人身份', 400);
      }
      if (
        !input ||
        !['fallback-policy', 'free-image-policy', 'loss-alert-policy'].includes(input.action)
      ) {
        fail('政策操作无效', 400);
      }
      const command = {
          ...metadata,
          fingerprint: domainDigest({ command: metadata.fingerprint, input }),
        },
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return prior;
      }
      const currentPolicy = this.sharingPolicy();
      let fact: RelationalFact, receipt: Json;
      if (input.action === 'fallback-policy') {
        const view = {
          settings: {
            fairQuota: {
              enabled: currentPolicy.enabled === 1,
              autoAnlasFallback: currentPolicy.fallback === 1,
            },
          },
        };
        receipt = configureAnlasFallback(
          view,
          input.enabled,
          metadata.actorId,
          metadata.recordedAt,
        );
        fact = {
          eventType: 'FallbackPolicyChanged',
          payload: {
            enabled: view.settings.fairQuota.autoAnlasFallback,
            configVersion: currentPolicy.configVersion,
          },
        };
      } else if (input.action === 'free-image-policy') {
        const view = {
          settings: {
            fairQuota: {
              freeImagePolicy: {
                version: currentPolicy.freeVersion,
                enabled: currentPolicy.freeEnabled === 1,
                warningThreshold: currentPolicy.warningThreshold,
                hardLimit: currentPolicy.hardLimit,
              },
            },
          },
          ledger: [],
        };
        const policy = configureFreeImages(view, input, metadata.actorId, metadata.recordedAt);
        fact = { eventType: 'FreePolicyChanged', payload: policy };
        receipt = { policy: { ...policy } };
      } else {
        const view = {
          settings: {
            fairQuota: {
              lossAlertPolicy: {
                version: currentPolicy.lossVersion,
                v5Points24h: currentPolicy.v5_24h,
                fixedAnlas24h: Number(currentPolicy.fixed24h),
                fixedAnlas30d: Number(currentPolicy.fixed30d),
                memberAttempts24h: currentPolicy.memberAttempts24h,
              },
            },
          },
          ledger: [],
        };
        const policy = updateLossAlertPolicy(view, input, metadata.actorId, metadata.recordedAt);
        fact = {
          eventType: 'LossPolicyChanged',
          payload: {
            version: policy.version,
            v5_24h: policy.v5Points24h,
            fixed24h: formatAmount(parseAmount(policy.fixedAnlas24h)),
            fixed30d: formatAmount(parseAmount(policy.fixedAnlas30d)),
            memberAttempts24h: policy.memberAttempts24h,
          },
        };
        receipt = { policy: { ...policy } };
      }
      return this.commit(
        command,
        [
          {
            ...fact,
            streamId: 'sharing:default',
            accountId: null,
            memberId: null,
            occurredAt: metadata.recordedAt,
            policyVersion: metadata.policyVersion,
            pricingVersion: metadata.pricingVersion,
          },
        ],
        receipt,
      );
    });
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
      const planned = this.planQuotaObservation(metadata, ticket, result);
      if (!planned) {
        return { ignored: true };
      }
      return this.commit(metadata, planned, {
        accountId: ticket.accountId,
        sequence: ticket.sequence,
        ok: result.ok,
      });
    });
  }
  // 账户启用与额度决定共用一笔提交，规划不追加事实或推进投影。
  planQuotaObservation(metadata: Metadata, ticket: Ticket, result: QuotaResult, enable = false) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Quota observation planning requires the owning transaction.');
    }
    const a = this.account(ticket.accountId),
      q = this.readAccountQuota(ticket.accountId);
    if (
      a.epoch !== ticket.credentialEpoch ||
      a.pst !== ticket.pst ||
      !this.order.current(`${a.id}:${a.epoch}`, ticket.sequence) ||
      BigInt(ticket.sequence) <= BigInt(q.observedSequence ?? '0')
    ) {
      return null;
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
    const verified =
      enable &&
      result.ok &&
      isUsableOpus({ ...view.account, enabled: true }, metadata.recordedAt) &&
      isQuotaSnapshotFresh(ticket.startedAt, metadata.recordedAt);
    if (verified) {
      view.account.enabled = true;
    }
    const snapshot = fairQuotaSnapshot(view.account),
      observationId = randomUUID(),
      tier = quotaNumber(view.account.quotaTier);
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
            quotaTier: tier !== null && Number.isSafeInteger(tier) ? tier : null,
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
    if (verified || (result.disableAccount && a.enabled === 1)) {
      events.push(
        this.event(
          {
            eventType: 'AvailabilityChanged',
            payload: {
              accountId: a.id,
              enabled: verified,
              reason: verified ? 'verified_opus' : result.disableReason,
              observationEventId: verified ? observationId : null,
            },
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
    return events;
  }
  estimateGenerationQuota(
    accountId: string,
    memberId: string,
    now: number,
  ): Pick<
    FairQuotaReservationState,
    'balance' | 'availableV5' | 'pending' | 'pendingAnlas' | 'pendingFallback' | 'sharedV5Debt'
  > {
    // state 只从关系行建立独立金融值；查询复用原恢复规则，不收集或提交规划事实。
    const view = this.state(accountId),
      state = syncBoundFairQuota(view.db, view.account, now),
      balance = state.balances[memberId];
    if (!balance) {
      fail('成员钱包尚未恢复', 503);
    }
    return {
      balance,
      availableV5: formatAmount(
        state.members.reduce(
          (total, id) => total + parseAmount(state.balances[id].v5Available),
          0n,
        ),
      ),
      pending: {
        v5: state.spent.v5,
        anlasFixed: state.spent.anlasFixed,
        anlasPurchased: state.spent.anlasPurchased,
      },
      pendingAnlas: state.spent.anlas,
      pendingFallback: {
        v5: state.spent.fallbackV5,
        anlasFixed: state.spent.fallbackFixed,
        anlasPurchased: state.spent.fallbackPurchased,
      },
      sharedV5Debt: state.sharedV5Debt,
    };
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
    // 原基线与核对观察是不同角色；时间相同也不能交换事件身份。
    const baselineAt = (at: number): string => {
      const id = view.q.baselineEventId;
      if (!id || this.observation(id, accountId).at !== at) {
        fail('金融决定缺少原基线引用');
      }
      return id;
    };
    const observationAt = (at: number): string => {
      const id = newObservationId ?? view.q.observationEventId;
      if (
        !id ||
        (newObservationId
          ? fairQuotaSnapshot(view.account).at
          : this.observation(id, accountId).at) !== at
      ) {
        fail('金融决定缺少核对观察引用');
      }
      return id;
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
                  observationEventId: observationAt(p.snapshot.at),
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
                  observationEventId: baselineAt(p.snapshotAt),
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
          // 原 owner 在容量相等时不产生 evidence；这是原已知零变化，不从 difference 推算。
          const delta = evidence ? evidence.delta : '0';
          if (typeof delta !== 'string') {
            fail('容量事实缺少原 owner 的精确变化金额');
          }
          events.push(
            this.event(
              {
                eventType: 'V5CapacityObserved',
                payload: {
                  accountId,
                  observationEventId: observationAt(p.reconciliation.sourceCheckedAt),
                  capacityGap: p.capacityGap,
                  delta,
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
                  beforeObservationEventId: baselineAt(p.beforeSnapshot.at),
                  afterObservationEventId: observationAt(p.afterSnapshot.at),
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
                    observationEventId: observationAt(p.snapshotAt),
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
                observationEventId: registered ? observationAt(p.snapshotAt) : null,
                receiptEventId: registered ? p.receiptEventId : null,
              },
            },
            accountId,
            metadata,
            p.id,
          );
          events.push(received);
          // 登记到账只是观察/扣款确认净额的组成；只有明确新增凭据直接分配。
          if (p.source === 'declared_external_receipt') {
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
  createQuotaProjector(events: RelationalEvent[]) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Quota projection requires the owning transaction.');
    }
    const sources = new Map(events.map((e) => [e.eventId, e]));
    const consumed = new Set<string>();
    const observationResources = new Map<string, Set<string>>();
    return (e: RelationalEvent): void => {
      if (!this.sqlite.inTransaction) {
        throw new Error('Quota projection requires the owning transaction.');
      }
      if (e.eventType === 'SharingConfigured') {
        const p = e.payload,
          configuration = this.groupState();
        if (
          e.streamId !== 'sharing:default' ||
          e.accountId !== null ||
          e.memberId !== null ||
          this.sqlite
            .prepare(
              "SELECT 1 FROM generation_jobs WHERE status IN ('queued','pending','running') UNION ALL SELECT 1 FROM charges WHERE status='reserved' LIMIT 1",
            )
            .get()
        ) {
          fail('分组配置必须属于空闲的当前共享流');
        }
        const current = this.sqlite.prepare('SELECT 1 FROM sharing_policy WHERE id=1').get()
          ? this.sharingPolicy()
          : null;
        if (
          p.configVersion !== (current ? current.configVersion + 1 : 2) ||
          p.boundAccountId !== (current?.accountId ?? null) ||
          (!current &&
            (configuration.groups.length ||
              this.sqlite.prepare('SELECT 1 FROM upstream_accounts LIMIT 1').get()))
        ) {
          fail('分组政策版本或初始绑定不符');
        }
        if (current) {
          const expected = {
              enabled: current.enabled === 1,
              boundAccountId: current.accountId,
              autoAnlasFallback: current.fallback === 1,
              freePolicyVersion: current.freeVersion,
              freeEnabled: current.freeEnabled === 1,
              freeWarningThreshold: current.warningThreshold,
              freeHardLimit: current.hardLimit,
              lossPolicyVersion: current.lossVersion,
              lossV5_24h: current.v5_24h,
              lossFixed24h: current.fixed24h,
              lossFixed30d: current.fixed30d,
              lossMemberAttempts24h: current.memberAttempts24h,
            },
            {
              groups: _groups,
              configVersion: _version,
              redistributeRemaining: _redistribute,
              ...saved
            } = p;
          if (canonicalJson(expected) !== canonicalJson(saved)) {
            fail('分组配置不能改写原绑定或独立政策');
          }
        }
        const groups: FairGroup[] = p.groups.map((group) => ({
            id: group.groupId,
            name: group.name,
            share: group.share,
            members: group.members.map((member) => member.memberId),
            memberWeights: Object.fromEntries(
              group.members.map((member) => [member.memberId, member.weight]),
            ),
          })),
          users = this.sqlite
            .prepare<[], { id: string; enabled: number }>(
              "SELECT id,(status='active') AS enabled FROM members WHERE status!='removed' ORDER BY id",
            )
            .all()
            .map((member) => ({ id: member.id, enabled: member.enabled === 1 }));
        const normalized = normalizeFairGroups(groups, users, configuration);
        assertFairGroupMemberCount(normalized);
        const balances = Object.fromEntries(
          p.boundAccountId
            ? this.sqlite
                .prepare<
                  [string],
                  {
                    memberId: string;
                    v5Available: string;
                    anlasFixedAvailable: string;
                    anlasPurchasedAvailable: string;
                  }
                >(
                  `SELECT w.member_id AS memberId,w.v5_available AS v5Available,w.anlas_fixed_available AS anlasFixedAvailable,
          w.anlas_purchased_available AS anlasPurchasedAvailable FROM wallets w JOIN group_members g ON g.member_id=w.member_id WHERE w.account_id=?`,
                )
                .all(p.boundAccountId)
                .map(({ memberId, ...balance }) => [
                  memberId,
                  {
                    ...balance,
                    anlasAvailable: formatAmount(
                      parseAmount(balance.anlasFixedAvailable) +
                        parseAmount(balance.anlasPurchasedAvailable),
                    ),
                  },
                ])
            : [],
        );
        assertFairGroupTransition(
          { groups: configuration.groups, balances },
          groups,
          p.redistributeRemaining,
        );
        const required =
            p.redistributeRemaining && p.boundAccountId
              ? p.groups.filter((group) => group.members.some((member) => member.weight > 0))
              : [],
          actual = events.filter(
            (
              candidate,
            ): candidate is Extract<RelationalEvent, { eventType: 'GroupBalancesRedistributed' }> =>
              candidate.eventType === 'GroupBalancesRedistributed' &&
              candidate.payload.configurationEventId === e.eventId,
          ),
          following = events.slice(e.eventIndex + 1, e.eventIndex + 1 + required.length);
        if (
          actual.length !== required.length ||
          required.some(
            (group) =>
              actual.filter((candidate) => candidate.payload.groupId === group.groupId).length !==
              1,
          ) ||
          following.some(
            (candidate) =>
              candidate.eventType !== 'GroupBalancesRedistributed' ||
              candidate.payload.configurationEventId !== e.eventId,
          )
        ) {
          fail('显式重分必须完整紧接原配置决定');
        }
        this.sqlite.prepare('DELETE FROM group_members').run();
        this.sqlite.prepare('DELETE FROM sharing_groups').run();
        this.sqlite
          .prepare(
            `INSERT INTO sharing_policy(id,enabled,bound_account_id,config_version,auto_anlas_fallback,
          free_policy_version,free_enabled,free_warning_threshold,free_hard_limit,loss_policy_version,loss_v5_24h,
          loss_fixed_24h,loss_fixed_30d,loss_member_attempts_24h,source_event_id) VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(id) DO UPDATE SET enabled=excluded.enabled,bound_account_id=excluded.bound_account_id,config_version=excluded.config_version,
          auto_anlas_fallback=excluded.auto_anlas_fallback,free_policy_version=excluded.free_policy_version,free_enabled=excluded.free_enabled,
          free_warning_threshold=excluded.free_warning_threshold,free_hard_limit=excluded.free_hard_limit,loss_policy_version=excluded.loss_policy_version,
          loss_v5_24h=excluded.loss_v5_24h,loss_fixed_24h=excluded.loss_fixed_24h,loss_fixed_30d=excluded.loss_fixed_30d,
          loss_member_attempts_24h=excluded.loss_member_attempts_24h,source_event_id=excluded.source_event_id`,
          )
          .run(
            Number(p.enabled),
            p.boundAccountId,
            p.configVersion,
            Number(p.autoAnlasFallback),
            p.freePolicyVersion,
            Number(p.freeEnabled),
            p.freeWarningThreshold,
            p.freeHardLimit,
            p.lossPolicyVersion,
            p.lossV5_24h,
            p.lossFixed24h,
            p.lossFixed30d,
            p.lossMemberAttempts24h,
            e.eventId,
          );
        for (const group of p.groups) {
          this.sqlite
            .prepare('INSERT INTO sharing_groups(id,name,share,source_event_id) VALUES(?,?,?,?)')
            .run(group.groupId, group.name, group.share, e.eventId);
          for (const member of group.members) {
            this.sqlite
              .prepare(
                'INSERT INTO group_members(member_id,group_id,weight,source_event_id) VALUES(?,?,?,?)',
              )
              .run(member.memberId, group.groupId, member.weight, e.eventId);
            if (p.boundAccountId) {
              this.sqlite
                .prepare(
                  `INSERT OR IGNORE INTO wallets(account_id,member_id,v5_available,anlas_fixed_available,anlas_purchased_available,source_event_id)
              VALUES(?,?,'0','0','0',?)`,
                )
                .run(p.boundAccountId, member.memberId, e.eventId);
            }
          }
        }
        return;
      }
      if (
        e.eventType === 'FallbackPolicyChanged' ||
        e.eventType === 'FreePolicyChanged' ||
        e.eventType === 'LossPolicyChanged'
      ) {
        if (e.streamId !== 'sharing:default' || e.accountId !== null || e.memberId !== null) {
          fail('政策事实不属于当前共享流');
        }
        const policy = this.sharingPolicy();
        if (e.eventType === 'FallbackPolicyChanged') {
          if (!policy.enabled || e.payload.configVersion !== policy.configVersion) {
            fail('fallback 政策与当前共享配置不符');
          }
          this.sqlite
            .prepare('UPDATE sharing_policy SET auto_anlas_fallback=?,source_event_id=? WHERE id=1')
            .run(Number(e.payload.enabled), e.eventId);
        } else if (e.eventType === 'FreePolicyChanged') {
          const p = e.payload;
          if (p.version !== policy.freeVersion + 1) {
            fail('免费图片政策版本已经变化');
          }
          this.sqlite
            .prepare(
              'UPDATE sharing_policy SET free_policy_version=?,free_enabled=?,free_warning_threshold=?,free_hard_limit=?,source_event_id=? WHERE id=1',
            )
            .run(p.version, Number(p.enabled), p.warningThreshold, p.hardLimit, e.eventId);
        } else {
          const p = e.payload;
          if (p.version !== policy.lossVersion + 1) {
            fail('损耗告警政策版本已经变化');
          }
          this.sqlite
            .prepare(
              'UPDATE sharing_policy SET loss_policy_version=?,loss_v5_24h=?,loss_fixed_24h=?,loss_fixed_30d=?,loss_member_attempts_24h=?,source_event_id=? WHERE id=1',
            )
            .run(p.version, p.v5_24h, p.fixed24h, p.fixed30d, p.memberAttempts24h, e.eventId);
        }
        return;
      }
      if (
        !relationalQuotaFacts.has(e.eventType) ||
        !e.accountId ||
        e.streamId !== `account:${e.accountId}` ||
        ('accountId' in e.payload && e.payload.accountId !== e.accountId)
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
        case 'AvailabilityChanged': {
          const p = e.payload;
          if (p.enabled) {
            if (
              !p.observationEventId ||
              p.observationEventId !== q.observationEventId ||
              q.observedCredentialEpoch !== a.epoch ||
              q.quotaErrorCode !== null ||
              !isQuotaSnapshotFresh(q.observedAt, e.occurredAt) ||
              !isUsableOpus(
                {
                  enabled: true,
                  quotaTier: q.quotaTier,
                  subscriptionActive: q.subscriptionActive,
                  subscriptionExpiresAt: iso(q.subscriptionExpiresAt),
                },
                e.occurredAt,
              )
            ) {
              fail('启用缺少当前代次的最新有效 Opus 观察');
            }
            const observed = this.sqlite
              .prepare<[string], { event_type: string; account_id: string | null }>(
                'SELECT event_type,account_id FROM domain_events WHERE event_id=?',
              )
              .get(p.observationEventId);
            if (observed?.event_type !== 'QuotaObserved' || observed.account_id !== accountId) {
              fail('启用依据不是该账户的实际查询观察');
            }
          } else if (p.observationEventId !== null) {
            fail('暂停不能引用启用观察');
          }
          if (
            this.sqlite
              .prepare('UPDATE upstream_accounts SET enabled=?,source_event_id=? WHERE id=?')
              .run(Number(p.enabled), e.eventId, accountId).changes !== 1
          ) {
            fail('可用性事实缺少实际账户');
          }
          break;
        }
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
        case 'GroupBalancesRedistributed': {
          const p = e.payload,
            source = sources.get(p.configurationEventId),
            key = canonicalJson([p.configurationEventId, p.groupId]);
          this.configuration(p.configVersion);
          if (
            !source ||
            source.eventType !== 'SharingConfigured' ||
            source.eventIndex >= e.eventIndex ||
            source.commitSeq !== e.commitSeq ||
            source.payload.configVersion !== p.configVersion ||
            !source.payload.redistributeRemaining ||
            source.payload.boundAccountId !== accountId ||
            consumed.has(key) ||
            e.occurredAt !== source.occurredAt ||
            e.memberId !== null
          ) {
            fail('重分缺少本次原共享配置来源');
          }
          const group = source.payload.groups.find((group) => group.groupId === p.groupId);
          if (
            !group ||
            !group.members.some((member) => member.weight > 0) ||
            new Set(p.allocations.map((allocation) => allocation.memberId)).size !==
              p.allocations.length ||
            canonicalJson(p.allocations.map((allocation) => allocation.memberId).sort()) !==
              canonicalJson(group.members.map((member) => member.memberId).sort())
          ) {
            fail('重分必须覆盖原组全部成员');
          }
          const historicalGroup: FairGroup = {
            id: group.groupId,
            name: group.name,
            share: group.share,
            members: group.members.map((member) => member.memberId),
            memberWeights: Object.fromEntries(
              group.members.map((member) => [member.memberId, member.weight]),
            ),
          };
          const rows = this.sqlite
            .prepare<
              [string, string],
              { memberId: string; v5: string; anlasFixed: string; anlasPurchased: string }
            >(
              `SELECT w.member_id AS memberId,w.v5_available AS v5,w.anlas_fixed_available AS anlasFixed,w.anlas_purchased_available AS anlasPurchased
            FROM wallets w JOIN group_members g ON g.member_id=w.member_id WHERE w.account_id=? AND g.group_id=?`,
            )
            .all(accountId, p.groupId);
          for (const r of resources) {
            const budget = rows.reduce((sum, row) => sum + parseAmount(row[r]), 0n);
            if (
              budget !== parseAmount(p.sourceResources[r]) ||
              budget !==
                p.allocations.reduce(
                  (sum, allocation) => sum + parseAmount(allocation.resources[r]),
                  0n,
                )
            ) {
              fail('重分必须守恒该组原钱包的同一资源总量');
            }
            const expected = allocateFairGroupAmount(budget, historicalGroup);
            if (
              p.allocations.some(
                (allocation) =>
                  parseAmount(allocation.resources[r]) !== expected[allocation.memberId],
              )
            ) {
              fail('重分结果与本次保存的原组权重及舍入决定不符');
            }
          }
          for (const allocation of p.allocations) {
            this.sqlite
              .prepare(
                'UPDATE wallets SET v5_available=?,anlas_fixed_available=?,anlas_purchased_available=?,source_event_id=? WHERE account_id=? AND member_id=?',
              )
              .run(
                allocation.resources.v5,
                allocation.resources.anlasFixed,
                allocation.resources.anlasPurchased,
                e.eventId,
                accountId,
                allocation.memberId,
              );
          }
          consumed.add(key);
          break;
        }
        case 'AllocationApplied': {
          const p = e.payload,
            source = this.fact(p.sourceEventId);
          this.configuration(p.configVersion);
          if (
            source.accountId !== accountId ||
            !['QuotaObserved', 'FundingReceived', 'DebitsConfirmed'].includes(source.type) ||
            new Set(p.allocations.map((x) => x.memberId)).size !== p.allocations.length
          ) {
            fail('分配来源或成员身份不符');
          }
          if (source.type === 'QuotaObserved') {
            // 采纳可后于分配；真实采纳校验失败仍由整个命令保存点回滚。
            if (
              !events.some(
                (adoption) =>
                  adoption.eventType === 'QuotaBaselineAdopted' &&
                  adoption.accountId === accountId &&
                  adoption.commitSeq === e.commitSeq &&
                  adoption.payload.observationEventId === p.sourceEventId,
              )
            ) {
              fail('观察型分配缺少本次同账户同观察的基线采纳');
            }
            const used = observationResources.get(p.sourceEventId) ?? new Set<string>();
            for (const r of resources) {
              if (parseAmount(p.sourceResources[r]) > 0n) {
                if (used.has(r)) {
                  fail('同观察同资源的分配来源不能重复消费');
                }
                used.add(r);
              }
            }
            observationResources.set(p.sourceEventId, used);
          }
          if (source.type === 'FundingReceived' || source.type === 'DebitsConfirmed') {
            const original = sources.get(p.sourceEventId);
            if (
              !original ||
              original.commitSeq !== e.commitSeq ||
              original.eventIndex >= e.eventIndex ||
              consumed.has(p.sourceEventId)
            ) {
              fail('有限分配来源必须在同提交内先于分配且仅消费一次');
            }
            let resource: keyof ResourceAmounts, amount: string;
            if (original.eventType === 'FundingReceived') {
              if (
                original.payload.destination !== 'members' ||
                original.payload.source !== 'declared_external_receipt'
              ) {
                fail('分配来源不是直接用于成员的资金');
              }
              resource = original.payload.resource;
              amount = original.payload.amount;
            } else if (original.eventType === 'DebitsConfirmed') {
              resource = original.payload.resource;
              amount = original.payload.allocatedToMembers;
            } else {
              fail('有限分配来源类型不符');
            }
            for (const r of resources) {
              if (
                parseAmount(p.sourceResources[r]) !== (r === resource ? parseAmount(amount) : 0n)
              ) {
                fail('分配预算与来源原资源、原金额不符');
              }
            }
            consumed.add(p.sourceEventId);
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
          if (
            formatAmount(parseAmount(p.capacityGap) - parseAmount(ops.v5CapacityGap)) !== p.delta
          ) {
            fail('容量变化必须等于原运营账前后值');
          }
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
    };
  }
  applyQuotaEvents(events: RelationalEvent[]) {
    const apply = this.createQuotaProjector(events);
    for (const event of events) {
      apply(event);
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
        this.assertGroupCommit(result.events, result.receipt);
      }
      return result;
    });
  }
}
