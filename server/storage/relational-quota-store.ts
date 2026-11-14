import { assertCurrentOwnerCommand, type MemberUser } from '../members/authentication.ts';
import { RelationalCommitReplayer } from './relational-replay.ts';
import { relationalAccountConnectionSource } from './relational-account-connection.ts';
import { normalizeSocksProxyUrl } from '../protocols/novelai-client.ts';
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
import { readRelationalOpeningAttempt } from './relational-generation-sources.ts';
import {
  beginDomainCommand,
  finishDomainCommand,
  type DomainEventInput,
  type Json,
} from '../events/domain-events.ts';
import {
  appendRelationalCommit,
  readRelationalCommit,
  readRelationalReceipt,
  replayRelationalCommit,
  assertRelationalOpeningSource,
  type StoredRelationalCommit,
} from '../events/relational-event-store.ts';
import { parseRelationalFact } from '../events/relational-events.ts';
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
  planFairQuotaAllocation,
  assertFairQuotaIdle,
  applyFairMemberChange,
  fairQuotaSummary,
  type FairQuotaState,
  type FairQuotaDatabase,
  type FairUser,
  type FairMemberQuotaDatabase,
  type MemberChange,
  type FairQuotaSummaryState,
  type FairGroup,
  type FairQuotaSyncState,
  type FairQuotaSyncDatabase,
  type FairQuotaReservationState,
  type FairAccount,
  type ResourceAmounts,
} from '../quota/fair-share.ts';
import {
  createQuotaObservationOrder,
  accountQuotaPatch,
  generationCredentialError,
  type QuotaResult,
} from '../quota/account-refresh.ts';
import type {
  OperatingAccount,
  OperatingFundingInput,
  OperatingFundingContext,
} from '../quota/operating-accounting.ts';
import { matchExternalFunding } from '../quota/operating-accounting.ts';
import { parseAmount, formatAmount, amountNumber } from '../quota/resource-amounts.ts';
import {
  assertLauncherSubscriptionMember,
  launcherSubscriptionFromQuota,
} from '../quota/launcher-subscription.ts';
import { assertRelationalStorageFormat } from './relational-storage.ts';
import {
  quotaTimestamp as isoTimestamp,
  isUsableOpus,
  isQuotaSnapshotFresh,
  quotaNumber,
  quotaAvailability,
} from '../quota/eligibility.ts';
import type { SharedStatusSource } from '../quota/shared-status.ts';

type Metadata = Omit<RelationalCommand, 'events' | 'expectedVersions' | 'receipt'> & {
  policyVersion: string;
  pricingVersion: string;
};
type FundingMetadata = Omit<Metadata, 'commandId'> & { commandId?: string };
type GroupConfigurationInput = {
  groups: unknown;
  expectedConfigVersion: number;
  redistributeRemaining?: boolean;
};
type GenerationFailureSource = {
  attemptId: string;
  aborted: boolean;
  response: RelationalPayloads['ResponseEvidenceRecorded']['response'];
};
type QuotaAccountSnapshot = Pick<
  Quota,
  | 'quotaTier'
  | 'subscriptionActive'
  | 'subscriptionExpiresAt'
  | 'observedAt'
  | 'observedCredentialEpoch'
  | 'quotaErrorCode'
  | 'observedV5'
  | 'observedAnlasFixed'
  | 'observedAnlasPurchased'
  | 'secondsPerPercent'
>;
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
type OfficialQuotaV5 = Pick<
  RelationalPayloads['QuotaObserved'],
  'v5UsagePercent' | 'v5UsageIsNegative'
>;
export type AccountQuota = Omit<
  RelationalPayloads['OpeningQuotaRecorded'],
  'origin' | keyof OfficialQuotaV5
>;
type Quota = AccountQuota;
type Ticket = {
  accountId: string;
  credentialEpoch: number;
  sequence: string;
  startedAt: number;
  pst: string;
  proxyUrl: string;
  connectionSourceEventId: string;
};
const resources = ['v5', 'anlasFixed', 'anlasPurchased'] as const;
const unboundQuotaStatus = 'pending_opus';
const zero = (): ResourceAmounts => ({ v5: '0', anlasFixed: '0', anlasPurchased: '0' });
const iso = (n: number | null) => (n === null ? undefined : new Date(n).toISOString());
const observedAmount = (value: unknown) =>
  (typeof value === 'string' || typeof value === 'number') && Number.isFinite(Number(value))
    ? formatAmount(parseAmount(value, typeof value === 'number' ? 'nearest' : 'exact'))
    : null;
function financialObservedV5(account: FairAccount, snapshot: ReturnType<typeof fairQuotaSnapshot>) {
  return account.v5UsageIsNegative && snapshot.v5 !== null
    ? '0'
    : observedAmount(account.v5UsagePercent);
}
function fail(message: string, statusCode = 409): never {
  throw Object.assign(new Error(message), { statusCode });
}
export const relationalQuotaFacts = new Set<RelationalEvent['eventType']>([
  'SharingConfigured',
  'GroupBalancesRedistributed',
  'MemberTransferred',
  'MemberRemoved',
  'FundsTransferred',
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
  'OpeningFundingRecorded',
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
  private officialQuotaV5(accountId: string, beforeVersion?: string): OfficialQuotaV5 {
    const row = this.sqlite
      .prepare<(string | bigint)[], { type: string; payload: string }>(
        `SELECT event_type AS type,payload FROM domain_events WHERE stream_id=? AND account_id=?
        AND event_type IN ('QuotaObserved','OpeningQuotaRecorded')
        ${beforeVersion === undefined ? '' : 'AND stream_version<?'} ORDER BY stream_version DESC LIMIT 1`,
      )
      .get(
        ...(beforeVersion === undefined
          ? [`account:${accountId}`, accountId]
          : [`account:${accountId}`, accountId, BigInt(beforeVersion)]),
      );
    if (!row) {
      return { v5UsagePercent: null, v5UsageIsNegative: null };
    }
    const fact = parseRelationalFact({ eventType: row.type, payload: JSON.parse(row.payload) });
    if (
      (fact.eventType !== 'QuotaObserved' && fact.eventType !== 'OpeningQuotaRecorded') ||
      fact.payload.accountId !== accountId
    ) {
      fail('官方 V5 原事实身份不符', 503);
    }
    return {
      v5UsagePercent: fact.payload.v5UsagePercent,
      v5UsageIsNegative: fact.payload.v5UsageIsNegative,
    };
  }
  readOfficialQuotaV5(accountId: string): OfficialQuotaV5 {
    return this.officialQuotaV5(accountId);
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
        .get(e.eventId, e.payload.accountId)
    ) {
      fail('开账额度缺少原事实或登记来源');
    }
    assertRelationalOpeningSource(this.sqlite, e);
    const original = {
      id: e.payload.accountId,
      v5UsagePercent: e.payload.v5UsagePercent,
      v5UsageIsNegative: e.payload.v5UsageIsNegative,
    };
    if (
      original.v5UsagePercent !== null &&
      financialObservedV5(original, fairQuotaSnapshot(original)) !== e.payload.observedV5
    ) {
      fail('开账官方原值与原金融 V5 表示不符');
    }
    const {
      origin: _origin,
      v5UsagePercent: _percent,
      v5UsageIsNegative: _negative,
      ...p
    } = e.payload;
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
  readFailureAccountingState() {
    const row = this.sharingPolicy();
    if (!row.accountId) {
      fail('损耗读取需要已恢复的绑定账户账务基线', 503);
    }
    const q = this.readAccountQuota(row.accountId);
    return {
      policy: this.policyParameters(row).lossAlertPolicy,
      operating: this.operating(row.accountId),
      pendingAnlasDebits: {
        anlasFixed: q.pendingFixedDebit,
        anlasPurchased: q.pendingPurchasedDebit,
      },
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
      .prepare<
        [string],
        { id: string; enabled: number; epoch: number; pst: string | null; proxyUrl: string | null }
      >(
        `SELECT a.id,a.enabled,a.credential_epoch AS epoch,c.pst,c.proxy_url AS proxyUrl FROM upstream_accounts a LEFT JOIN upstream_credentials c ON c.account_id=a.id WHERE a.id=?`,
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
  private quotaAccount(
    a: { id: string; enabled: number; epoch: number },
    q: QuotaAccountSnapshot,
  ): FairAccount {
    // 金融金额不是原 negative 标量；snapshot 自行处理数值边界，不能从符号伪造原信息。
    return {
      id: a.id,
      enabled: a.enabled === 1,
      quotaTier: q.quotaTier,
      subscriptionActive: q.subscriptionActive,
      subscriptionExpiresAt: iso(q.subscriptionExpiresAt),
      quotaCheckedAt: iso(q.observedAt),
      quotaError: q.observedCredentialEpoch === a.epoch ? q.quotaErrorCode : 'credential_changed',
      v5UsagePercent: q.observedV5,
      quotaFixed: q.observedAnlasFixed,
      quotaPurchased: q.observedAnlasPurchased,
      v5UsageTimeUntilNextPercent: q.secondsPerPercent,
    };
  }
  readBoundAccountId() {
    return this.sharingPolicy().accountId;
  }
  readSharedStatusSource(): Omit<SharedStatusSource, 'counts'> {
    return this.sqlite.transaction(() => {
      const boundAccountId =
        this.sqlite
          .prepare<[], { accountId: string | null }>(
            'SELECT bound_account_id AS accountId FROM sharing_policy WHERE id=1',
          )
          .get()?.accountId ?? null;
      type Row = Omit<QuotaAccountSnapshot, 'subscriptionActive'> & {
        id: string;
        enabled: number;
        epoch: number;
        configured: number;
        quotaAccountId: string | null;
        subscriptionActive: number | null;
        cooldownUntil: number | null;
        cooldownReason: string | null;
        baselineEventId: string | null;
        settlementCursorAt: number | null;
      };
      const rows = this.sqlite
        .prepare<[string | null, string | null], Row>(
          `SELECT a.id,a.enabled,a.credential_epoch AS epoch,
          EXISTS(SELECT 1 FROM upstream_credentials c WHERE c.account_id=a.id AND c.pst!='' AND c.proxy_url IS NOT NULL) AS configured,
          a.cooldown_until AS cooldownUntil,a.cooldown_reason AS cooldownReason,
          q.account_id AS quotaAccountId,q.quota_tier AS quotaTier,
          q.subscription_active AS subscriptionActive,q.subscription_expires_at AS subscriptionExpiresAt,
          q.observed_at AS observedAt,q.observed_credential_epoch AS observedCredentialEpoch,
          q.quota_error_code AS quotaErrorCode,q.observed_v5 AS observedV5,
          q.observed_anlas_fixed AS observedAnlasFixed,q.observed_anlas_purchased AS observedAnlasPurchased,
          q.seconds_per_percent AS secondsPerPercent,q.baseline_event_id AS baselineEventId,
          q.settlement_cursor_at AS settlementCursorAt
        FROM upstream_accounts a LEFT JOIN account_quota_state q ON q.account_id=a.id
        WHERE ? IS NULL OR a.id=? ORDER BY a.id`,
        )
        .all(boundAccountId, boundAccountId);
      const bound = rows.find((row) => row.id === boundAccountId);
      if (bound && bound.quotaAccountId === null) {
        fail('共享账户额度来源尚未恢复', 503);
      }
      return {
        boundAccountId,
        accounts: rows.map((row) => {
          const account = this.quotaAccount(row, {
            ...row,
            subscriptionActive:
              row.subscriptionActive === null ? null : row.subscriptionActive === 1,
          });
          return {
            ...account,
            // 原展示标量来自原事实，金融 v5=0 不能反推原透支百分比/标志。
            ...this.readOfficialQuotaV5(row.id),
            quotaError: row.configured === 1 ? account.quotaError : 'credential_unavailable',
            cooldownUntil: iso(row.cooldownUntil),
            cooldownReason: row.cooldownReason,
          };
        }),
        baseline: {
          at: bound?.baselineEventId
            ? iso(this.observation(bound.baselineEventId, bound.id).at)
            : null,
          held:
            boundAccountId !== null &&
            !!this.sqlite
              .prepare(
                "SELECT 1 FROM charges WHERE original_account_id=? AND status='reserved' LIMIT 1",
              )
              .get(boundAccountId),
          lastSettlementAt: bound?.settlementCursorAt ?? undefined,
        },
      };
    })();
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
    // 钱包责任归原账户，不能用当前组资格过滤物理预算；新分配仍只遍历 configuration.members。
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
          `SELECT w.member_id AS memberId,w.v5_available AS v5Available,w.anlas_fixed_available AS anlasFixedAvailable,w.anlas_purchased_available AS anlasPurchasedAvailable FROM wallets w WHERE w.account_id=? ORDER BY w.member_id`,
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
    const account = this.quotaAccount(a, q);
    let snapshot = null;
    if (q.baselineEventId) {
      const old = this.observation(q.baselineEventId, accountId);
      snapshot = fairQuotaSnapshot({
        ...account,
        quotaCheckedAt: iso(old.at),
        v5UsagePercent: old.v5,
        quotaFixed: old.anlasFixed,
        quotaPurchased: old.anlasPurchased,
        v5UsageTimeUntilNextPercent: old.seconds,
      });
    }
    const financial = {
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
    } satisfies FairQuotaSyncState;
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
      fail('缺少金融基线，不能重新分配已有钱包', 503);
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
    metadata: Pick<Metadata, 'recordedAt' | 'policyVersion' | 'pricingVersion'>,
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
  private command(
    metadata: Metadata,
    events: RelationalEventInput[],
    receipt: Json,
  ): RelationalCommand {
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
    return {
      ...command,
      events,
      expectedVersions,
      receipt,
    };
  }
  private commit(metadata: Metadata, events: RelationalEventInput[], receipt: Json) {
    if (!events.length) {
      return { unchanged: true, receipt };
    }
    const result = appendRelationalCommit(this.sqlite, this.command(metadata, events, receipt));
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
  private policyParameters(policy: SharingPolicy) {
    return {
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
    };
  }
  // 配置读取不依赖金融基线或空闲检查，不补历史默认值。
  readQuotaConfiguration() {
    return this.sqlite.transaction(() => {
      const policy = this.sharingPolicy();
      return {
        groups: this.groupState().groups,
        configVersion: policy.configVersion,
        ...this.policyParameters(policy),
      };
    })();
  }
  readAdminQuotaConfiguration() {
    return this.sqlite.transaction(() => {
      if (!this.sqlite.prepare('SELECT 1 FROM sharing_policy WHERE id=1').get()) {
        return null;
      }
      const policy = this.sharingPolicy(),
        groups = this.groupState();
      return {
        enabled: policy.enabled === 1,
        boundAccountId: policy.accountId,
        members: groups.members,
        groups: groups.groups,
        configVersion: policy.configVersion,
        ...this.policyParameters(policy),
      };
    })();
  }
  private authorizeOwnerCommand(
    metadata: Pick<Metadata, 'actorKind' | 'actorId'>,
    member: MemberUser | null,
  ) {
    const current =
      member &&
      this.sqlite
        .prepare<[string], { status: string; authEpoch: string | null }>(
          'SELECT status,auth_epoch AS authEpoch FROM members WHERE id=?',
        )
        .get(member.id);
    assertCurrentOwnerCommand(metadata, member, current, this.env);
  }
  private managementView() {
    const hasPolicy = !!this.sqlite.prepare('SELECT 1 FROM sharing_policy WHERE id=1').get(),
      policy = hasPolicy ? this.sharingPolicy() : null;
    const configuration = this.groupState(),
      financial = policy?.accountId ? this.state(policy.accountId) : null;
    if (financial) {
      assertFairQuotaIdle(financial.db);
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
            status: unboundQuotaStatus,
            unallocatedV5: '0',
            fallbackBuffer: { fixed: '0', purchased: '0' },
            sharedV5Debt: '0',
          }),
          version: 2,
          configVersion: policy.configVersion,
          configurationHistory: [],
          fallbackTotalsByUser: {},
          // 同一原 owner 已拒绝实际预留；管理决定不需要伪造完整收费对象。
          reservations: {},
          ...this.policyParameters(policy),
        }
      : null;
    const db: FairQuotaDatabase & { users: FairUser[] } = {
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
    return { db, financial, policy };
  }
  private memberView() {
    const view = this.managementView(),
      db: FairMemberQuotaDatabase & { users: FairUser[] } = view.db,
      state = db.settings?.fairQuota;
    if (state) {
      this.enrichQuotaSummary(state);
    }
    return { ...view, db };
  }
  private enrichQuotaSummary(state: FairQuotaSummaryState, memberId?: string) {
    state.fallbackTotalsByUser = {};
    const selected = memberId === undefined ? '' : ' AND original_member_id=?',
      parameters =
        memberId === undefined ? [state.boundAccountId] : [state.boundAccountId, memberId];
    for (const charge of this.sqlite
      .prepare<
        (string | null)[],
        {
          memberId: string;
          fixed: string;
          purchased: string;
          v5: string;
        }
      >(
        `SELECT original_member_id AS memberId,anlas_fixed AS fixed,anlas_purchased AS purchased,
            v5_equivalent AS v5 FROM charges WHERE original_account_id=? AND billing_mode='anlas_fallback' AND status='settled'${selected}`,
      )
      .iterate(...parameters)) {
      const total = state.fallbackTotalsByUser[charge.memberId] ?? {
        fixed: '0',
        purchased: '0',
        v5Equivalent: '0',
      };
      total.fixed = formatAmount(parseAmount(total.fixed) + parseAmount(charge.fixed));
      total.purchased = formatAmount(parseAmount(total.purchased) + parseAmount(charge.purchased));
      total.v5Equivalent = formatAmount(parseAmount(total.v5Equivalent) + parseAmount(charge.v5));
      state.fallbackTotalsByUser[charge.memberId] = total;
    }
    if (state.boundAccountId) {
      const latest = this.sqlite
        .prepare<[string], { payload: string }>(
          `SELECT payload FROM domain_events
          WHERE stream_id=? AND event_type='V5CapacityObserved' ORDER BY stream_version DESC LIMIT 1`,
        )
        .get(`account:${state.boundAccountId}`);
      if (latest) {
        const saved = JSON.parse(latest.payload) as RelationalPayloads['V5CapacityObserved'];
        state.v5Reconciliation = { status: saved.status };
      }
    }
  }
  readManagementQuotaStatus(now: number) {
    return this.sqlite.transaction(() => {
      const policy = this.sharingPolicy();
      if (!policy.accountId) {
        return unboundQuotaStatus;
      }
      const view = this.state(policy.accountId);
      syncBoundFairQuota(view.db, view.account, now);
      return view.db.settings.fairQuota.status;
    })();
  }

  readMemberQuotaSummary(memberId: string, now: number) {
    return this.sqlite.transaction(() => {
      if (!this.sqlite.prepare('SELECT 1 FROM sharing_policy WHERE id=1 AND enabled=1').get())
        return null;
      const policy = this.sharingPolicy();
      if (!policy.accountId)
        return fairQuotaSummary(this.managementView().db, { id: memberId }, now);
      const view = this.state(policy.accountId),
        state: FairQuotaSummaryState = {
          ...view.db.settings.fairQuota,
          autoAnlasFallback: policy.fallback === 1,
          fallbackTotalsByUser: {},
        },
        db = { settings: { fairQuota: state }, ledger: [] as unknown[] };
      this.enrichQuotaSummary(state, memberId);
      syncBoundFairQuota(db, view.account, now);
      return fairQuotaSummary(db, { id: memberId }, now);
    })();
  }
  readLauncherSubscription(memberId: string, now: number) {
    return this.sqlite.transaction(() => {
      const row = this.sqlite
          .prepare<[string], { id: string; enabled: number }>(
            "SELECT id,(status='active') AS enabled FROM members WHERE id=?",
          )
          .get(memberId),
        member = row && { id: row.id, enabled: row.enabled === 1 };
      assertLauncherSubscriptionMember(member);
      const quota = this.readMemberQuotaSummary(memberId, now),
        account = quota?.boundAccountId
          ? this.sqlite
              .prepare<
                [string],
                {
                  id: string;
                  enabled: number;
                  quotaTier: number | null;
                  subscriptionActive: number | null;
                  subscriptionExpiresAt: number | null;
                }
              >(
                `SELECT a.id,a.enabled,q.quota_tier AS quotaTier,q.subscription_active AS subscriptionActive,
                q.subscription_expires_at AS subscriptionExpiresAt FROM upstream_accounts a
                JOIN account_quota_state q ON q.account_id=a.id WHERE a.id=?`,
              )
              .get(quota.boundAccountId)
          : undefined;
      return launcherSubscriptionFromQuota(
        quota,
        account && {
          ...account,
          enabled: account.enabled === 1,
          subscriptionActive:
            account.subscriptionActive === null ? null : account.subscriptionActive === 1,
          // SQL 保存毫秒；ISO 保留其量纲，不能把早期毫秒值再次当作秒。
          subscriptionExpiresAt:
            account.subscriptionExpiresAt === null
              ? null
              : new Date(account.subscriptionExpiresAt).toISOString(),
        },
        now,
      );
    })();
  }
  private assertMemberRightsIdle(
    sourceId: string,
    targetId: string,
    controls?: Pick<ReadonlyMap<string, unknown>, 'size' | 'has'>,
  ) {
    if (controls && controls.size !== 0) {
      fail('仍有任务正在收尾，请完成后调整成员权益');
    }
    if (
      this.sqlite.prepare("SELECT 1 FROM charges WHERE status='reserved' LIMIT 1").get() ||
      this.sqlite
        .prepare(
          `SELECT 1 FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id
        JOIN generation_attempts a ON a.job_id=j.id WHERE r.member_id IN (?,?)
        AND (a.review_status='pending' OR (a.prepared_artifact_id IS NOT NULL AND j.artifact_id IS NULL)) LIMIT 1`,
        )
        .get(sourceId, targetId)
    ) {
      fail('成员权益不能跨越活动任务、未发布准备或未决核对');
    }
  }
  private assertMemberPoolRights(sourceId: string, accountId: string | null, removing: boolean) {
    if (
      !accountId &&
      this.sqlite
        .prepare(
          `SELECT 1 FROM wallets WHERE v5_available!='0'
      OR anlas_fixed_available!='0' OR anlas_purchased_available!='0' LIMIT 1`,
        )
        .get()
    ) {
      fail('未绑定共享账本不能调整既有账户权益');
    }
    if (
      removing &&
      this.sqlite
        .prepare(
          `SELECT 1 FROM wallets w WHERE w.member_id=? AND
      (w.account_id!=? OR NOT EXISTS(SELECT 1 FROM group_members g WHERE g.member_id=w.member_id)) AND
      (w.v5_available!='0' OR w.anlas_fixed_available!='0' OR w.anlas_purchased_available!='0') LIMIT 1`,
        )
        .get(sourceId, accountId ?? '')
    ) {
      fail('成员仍有其他原账户或未归组权益，不能搬入当前池或删除');
    }
  }
  private assertMemberReceiverRights(targetId: string, accountId: string | null) {
    if (
      accountId &&
      this.sqlite
        .prepare(
          `SELECT 1 FROM wallets w WHERE w.member_id=? AND w.account_id=?
      AND NOT EXISTS(SELECT 1 FROM group_members g WHERE g.member_id=w.member_id)
      AND (w.v5_available!='0' OR w.anlas_fixed_available!='0' OR w.anlas_purchased_available!='0')`,
        )
        .get(targetId, accountId)
    ) {
      fail('接收成员尚有未归组的本账户权益，不能覆盖为零开账');
    }
  }
  manageMemberQuota(
    metadata: Metadata,
    input: MemberChange,
    actor: MemberUser | null,
    controls: Pick<ReadonlyMap<string, unknown>, 'size' | 'has'>,
  ) {
    return this.owned(() => {
      this.authorizeOwnerCommand(metadata, actor);
      if (
        !input ||
        typeof input !== 'object' ||
        Array.isArray(input) ||
        !['transfer-quota', 'delete-member', 'preview-delete-member'].includes(input.action ?? '')
      ) {
        fail('成员权益命令无效', 400);
      }
      const transfer = input.action === 'transfer-quota',
        fields = transfer
          ? ['action', 'expectedConfigVersion', 'fromUserId', 'toUserId', 'resource', 'amount']
          : ['action', 'expectedConfigVersion', 'userId', 'transferToUserId'];
      if (
        Object.entries(input).some(
          ([key, value]) =>
            !fields.includes(key) ||
            (value !== undefined &&
              typeof value !== 'string' &&
              !(typeof value === 'number' && Number.isFinite(value))),
        )
      ) {
        fail('成员权益命令字段无效', 400);
      }
      const command = {
          ...metadata,
          fingerprint: domainDigest({
            operation: 'member-quota',
            input: Object.fromEntries(
              Object.entries(input).filter(([, value]) => value !== undefined),
            ),
          } as Json),
        },
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) return prior;
      const sourceId = transfer ? input.fromUserId : input.userId,
        targetId = transfer ? input.toUserId : input.transferToUserId;
      if (!sourceId || !targetId) fail('成员角色缺失', 400);
      if (!transfer && metadata.actorKind !== 'admin' && sourceId === this.env.OWNER_USER_ID) {
        fail('不能停用或删除当前 owner');
      }
      if (
        !controls ||
        typeof controls.has !== 'function' ||
        !Number.isSafeInteger(controls.size) ||
        controls.size < 0
      ) {
        fail('成员权益调整需要当前物理执行占用', 503);
      }
      this.assertMemberRightsIdle(sourceId, targetId, controls);
      const view = this.memberView();
      this.assertMemberPoolRights(sourceId, view.policy?.accountId ?? null, !transfer);
      this.assertMemberReceiverRights(targetId, view.policy?.accountId ?? null);
      const planned = this.capture(view, () => {
        if (view.financial)
          syncBoundFairQuota(
            view.db as FairQuotaSyncDatabase,
            view.financial.account,
            metadata.recordedAt,
          );
        const draft = structuredClone(view.db);
        return applyFairMemberChange(view.db, draft, input, metadata.recordedAt);
      });
      if (input.action === 'preview-delete-member') return planned.result;
      const anchor = planned.facts.findIndex(
        (fact) => fact.eventType === 'MemberTransferred' || fact.eventType === 'MemberRemoved',
      );
      if (anchor < 0) throw new Error('Member owner omitted its original decision.');
      const events = view.financial
          ? this.convert(planned.facts.slice(0, anchor), view.financial, metadata)
          : [],
        eventId = randomUUID(),
        state = view.db.settings!.fairQuota!,
        sharing: RelationalEventInput = {
          eventType: transfer ? 'MemberTransferred' : 'MemberRemoved',
          payload: {
            sourceMemberId: sourceId,
            targetMemberId: targetId,
            boundAccountId: state.boundAccountId,
            configVersion: state.configVersion,
            ...(!transfer
              ? {
                  groups: state.groups.map((group) => ({
                    groupId: group.id,
                    name: group.name!,
                    share: group.share,
                    members: group.members.map((memberId) => ({
                      memberId,
                      weight: group.memberWeights[memberId],
                    })),
                  })),
                }
              : {}),
          },
          eventId,
          streamId: 'sharing:default',
          accountId: null,
          memberId: null,
          occurredAt: metadata.recordedAt,
          policyVersion: metadata.policyVersion,
          pricingVersion: metadata.pricingVersion,
        } as RelationalEventInput;
      events.push(sharing);
      for (const fact of planned.facts.slice(anchor + 1)) {
        if (
          fact.eventType !== 'FundsTransferred' ||
          !view.financial ||
          !('kind' in fact.payload) ||
          (fact.payload.kind !== 'transfer' && fact.payload.kind !== 'member-removal')
        ) {
          throw new Error('Member owner returned another decision after its configuration.');
        }
        events.push(
          this.event(
            {
              eventType: 'FundsTransferred',
              payload: {
                accountId: view.financial.account.id,
                configurationEventId: eventId,
                configVersion: state.configVersion,
                fromMemberId: sourceId,
                toMemberId: targetId,
                resources: fact.payload.transferred,
                reason: transfer ? 'transfer' : 'member_removal',
              },
            },
            view.financial.account.id,
            metadata,
          ),
        );
      }
      if (!transfer)
        events.push({
          eventType: 'MemberStatusChanged',
          payload: { memberId: sourceId, status: 'removed' },
          streamId: `member:${sourceId}`,
          accountId: null,
          memberId: sourceId,
          occurredAt: metadata.recordedAt,
          policyVersion: metadata.policyVersion,
          pricingVersion: metadata.pricingVersion,
        });
      return new RelationalCommitReplayer(this.sqlite).appendCommit(
        this.command(command, events, planned.result as unknown as Json),
      );
    });
  }
  private assertGroupConfigurationInput(
    actorId: Metadata['actorId'],
    input: GroupConfigurationInput,
  ) {
    if (
      !actorId ||
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
  }
  private planGroupConfiguration(
    metadata: Pick<Metadata, 'actorId' | 'recordedAt'>,
    input: GroupConfigurationInput,
  ) {
    const hasPolicy = !!this.sqlite.prepare('SELECT 1 FROM sharing_policy WHERE id=1').get(),
      policy = hasPolicy ? this.sharingPolicy() : null,
      configVersion = policy?.configVersion ?? 1;
    if (
      !Number.isSafeInteger(input.expectedConfigVersion) ||
      input.expectedConfigVersion !== configVersion
    ) {
      fail('分组版本缺失或已变化，请保留草稿并刷新后重新编辑');
    }
    if (
      !policy?.accountId &&
      this.sqlite.prepare('SELECT 1 FROM upstream_accounts LIMIT 1').get()
    ) {
      fail('未绑定账户须先完成账户配置');
    }
    const { db, financial } = this.managementView(),
      planned = this.capture({ db }, () => {
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
      anchor = planned.facts.findIndex(
        (fact) => fact.eventType === 'SharingConfigured' || fact.eventType === 'AllocationChanged',
      );
    if (anchor < 0) {
      throw new Error('Group owner omitted its configuration decision.');
    }
    return { db, financial, planned, anchor, configVersion };
  }
  previewGroups(
    metadata: Pick<Metadata, 'actorKind' | 'actorId' | 'recordedAt'>,
    input: GroupConfigurationInput,
    member: MemberUser | null,
  ) {
    return this.owned(() => {
      this.authorizeOwnerCommand(metadata, member);
      this.assertGroupConfigurationInput(metadata.actorId, input);
      const { db, planned, configVersion } = this.planGroupConfiguration(metadata, input),
        state = planned.result,
        reconciliation = state.v5Reconciliation;
      this.enrichQuotaSummary(state);
      // 新规划的核对决定优先于读取时原已提交状态；展示补充不改变提交规划的输入。
      state.v5Reconciliation = reconciliation ?? state.v5Reconciliation;
      return {
        groups: planned.result.groups,
        users: db.users.map((user) => ({
          id: user.id,
          quota: fairQuotaSummary(db, user, metadata.recordedAt),
        })),
        redistributeRemaining: input.redistributeRemaining === true,
        configVersion,
      };
    });
  }
  configureGroups(metadata: Metadata, input: GroupConfigurationInput, member: MemberUser | null) {
    return this.owned(() => {
      this.authorizeOwnerCommand(metadata, member);
      this.assertGroupConfigurationInput(metadata.actorId, input);
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
      const { db, financial, planned, anchor } = this.planGroupConfiguration(metadata, input),
        result = planned.result,
        free = freeImagePolicy(db),
        loss = lossAlertPolicy(result.lossAlertPolicy);
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
  prepareQuotaObservation(accountId: string): Omit<Ticket, 'sequence' | 'startedAt'> {
    const a = this.account(accountId),
      connectionSourceEventId = relationalAccountConnectionSource(this.sqlite, accountId);
    if (!a.pst || a.proxyUrl === null || !connectionSourceEventId) {
      fail('观察缺少当前私有凭据或实际时间', 503);
    }
    try {
      if (normalizeSocksProxyUrl(a.proxyUrl) !== a.proxyUrl) {
        fail('观察缺少获选的规范连接来源', 503);
      }
    } catch {
      fail('观察缺少获选的规范连接来源', 503);
    }
    return {
      accountId,
      credentialEpoch: a.epoch,
      pst: a.pst,
      proxyUrl: a.proxyUrl,
      connectionSourceEventId,
    };
  }
  readAccountQuotaSnapshot(accountId: string) {
    if (
      !this.sqlite.prepare('SELECT 1 FROM account_quota_state WHERE account_id=?').get(accountId)
    ) {
      return null;
    }
    return {
      ...this.quotaAccount(this.account(accountId), this.readAccountQuota(accountId)),
      ...this.readOfficialQuotaV5(accountId),
    };
  }
  beginQuotaObservation(
    accountId: string,
    startedAt = Date.now(),
    expected?: ReturnType<RelationalQuotaStore['prepareQuotaObservation']>,
  ): Ticket {
    const source = this.prepareQuotaObservation(accountId),
      q = this.readAccountQuota(accountId);
    if (!Number.isSafeInteger(startedAt) || startedAt < 0) {
      fail('观察缺少当前私有凭据或实际时间', 503);
    }
    if (
      expected &&
      (expected.accountId !== source.accountId ||
        expected.credentialEpoch !== source.credentialEpoch ||
        expected.pst !== source.pst ||
        expected.proxyUrl !== source.proxyUrl ||
        expected.connectionSourceEventId !== source.connectionSourceEventId)
    ) {
      fail('额度查询发起前原凭据已变化');
    }
    return {
      ...source,
      sequence: this.order.issue(
        `${accountId}:${source.credentialEpoch}`,
        q.observedSequence ?? '0',
      ),
      startedAt,
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
      a.proxyUrl !== ticket.proxyUrl ||
      relationalAccountConnectionSource(this.sqlite, a.id) !== ticket.connectionSourceEventId ||
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
    const originalV5: OfficialQuotaV5 = result.ok
      ? {
          v5UsagePercent: quotaNumber(result.v5UsagePercent),
          v5UsageIsNegative:
            typeof result.v5UsageIsNegative === 'boolean' ? result.v5UsageIsNegative : null,
        }
      : this.readOfficialQuotaV5(a.id);
    const snapshot = fairQuotaSnapshot(view.account),
      observationId = randomUUID(),
      tier = quotaNumber(view.account.quotaTier);
    const events: RelationalEventInput[] = [
      this.event(
        {
          eventType: 'QuotaObserved',
          payload: {
            accountId: a.id,
            credentialEpoch: a.epoch,
            connectionSourceEventId: ticket.connectionSourceEventId,
            sequence: ticket.sequence,
            observedAt: snapshot.at,
            quotaTier: tier !== null && Number.isSafeInteger(tier) ? tier : null,
            subscriptionActive:
              typeof view.account.subscriptionActive === 'boolean'
                ? view.account.subscriptionActive
                : null,
            subscriptionExpiresAt: isoTimestamp(view.account.subscriptionExpiresAt),
            ...originalV5,
            v5: financialObservedV5(view.account, snapshot),
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
    if (!state.members.includes(memberId) || !balance) {
      fail('成员钱包尚未恢复', 503);
    }
    return {
      balance,
      availableV5: formatAmount(
        Object.values(state.balances).reduce(
          (total, balance) => total + parseAmount(balance.v5Available),
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
  private generationDispatch(attemptId: string) {
    const attempt = this.sqlite
      .prepare<
        [string],
        {
          jobId: string;
          accountId: string;
          memberId: string;
          requestId: string;
          credentialEpoch: number | null;
          status: string;
          chargeStatus: string;
        }
      >(
        `SELECT a.job_id AS jobId,a.account_id AS accountId,a.credential_epoch AS credentialEpoch,
        r.member_id AS memberId,j.request_id AS requestId,j.status,c.status AS chargeStatus
      FROM generation_attempts a JOIN generation_jobs j ON j.id=a.job_id
        JOIN generation_requests r ON r.id=j.request_id
      JOIN charges c ON c.job_id=j.id AND c.attempt_id=a.id
      WHERE a.id=?`,
      )
      .get(attemptId);
    if (!attempt || attempt.status !== 'running' || attempt.chargeStatus !== 'reserved') {
      fail('凭据失效缺少原在途尝试与预扣');
    }
    if (attempt.credentialEpoch === null) {
      return null;
    }
    const rows = this.sqlite
      .prepare<[string, string], { id: string; seq: string }>(
        `SELECT event_id AS id,CAST(commit_seq AS TEXT) AS seq FROM domain_events
      WHERE stream_id=? AND event_type='DispatchAuthorized' AND json_extract(payload,'$.attemptId')=? LIMIT 2`,
      )
      .all(`request:${attempt.requestId}`, attemptId);
    const opening = readRelationalOpeningAttempt(this.sqlite, attemptId);
    if (opening) {
      if (rows.length) {
        fail('开账尝试不能冒充另一原在线派发');
      }
      // 原 epoch 不能单独证明一次授权；未知授权时间不借当前私有凭据补全。
      if (opening.payload.authorizedAt === null) {
        return null;
      }
      return {
        attempt,
        dispatch: opening,
        connectionSourceEventId: opening.payload.connectionSourceEventId,
      };
    }
    if (rows.length !== 1) {
      fail('凭据失效缺少唯一原派发事实');
    }
    const dispatch = readRelationalCommit(this.sqlite, rows[0].seq).events.find(
      (e) => e.eventId === rows[0].id,
    );
    if (
      !dispatch ||
      dispatch.eventType !== 'DispatchAuthorized' ||
      dispatch.accountId !== attempt.accountId ||
      dispatch.memberId !== attempt.memberId ||
      dispatch.payload.accountId !== attempt.accountId ||
      dispatch.payload.jobId !== attempt.jobId ||
      dispatch.payload.attemptId !== attemptId ||
      dispatch.payload.credentialEpoch !== attempt.credentialEpoch
    ) {
      fail('凭据失效与原派发归属或代次不符');
    }
    return { attempt, dispatch, connectionSourceEventId: dispatch.payload.connectionSourceEventId };
  }

  private generationCredentialDecision(
    source: GenerationFailureSource,
    at: number,
    before?: RelationalEvent,
  ) {
    const errorCode = generationCredentialError(source.aborted, source.response.upstreamHttpStatus);
    if (!errorCode) {
      return null;
    }
    const original = this.generationDispatch(source.attemptId);
    if (
      !original ||
      original.attempt.credentialEpoch !== this.account(original.attempt.accountId).epoch ||
      original.connectionSourceEventId === null ||
      original.connectionSourceEventId !==
        relationalAccountConnectionSource(this.sqlite, original.attempt.accountId, before)
    ) {
      return null;
    }
    if (source.response.dispatchState === 'not_sent') {
      fail('未发送的生成不能声称上游凭据失效');
    }
    const accountId = original.attempt.accountId,
      view = this.state(accountId),
      status = quotaAvailability({ ...view.account, quotaError: errorCode }, at);
    return { accountId, status, errorCode, dispatchEventId: original.dispatch.eventId };
  }

  planGenerationCredentialFailure(
    metadata: Metadata,
    source: GenerationFailureSource & { responseEventId: string; failureEventId: string },
  ): RelationalEventInput[] {
    if (!this.sqlite.inTransaction) {
      throw new Error('Generation credential failure planning requires the owning transaction.');
    }
    const decision = this.generationCredentialDecision(source, metadata.recordedAt);
    if (!decision) {
      return [];
    }
    return [
      this.event(
        {
          eventType: 'QuotaStatusChanged',
          payload: {
            accountId: decision.accountId,
            status: decision.status,
            generationFailure: {
              dispatchEventId: decision.dispatchEventId,
              responseEventId: source.responseEventId,
              failureEventId: source.failureEventId,
              errorCode: decision.errorCode,
            },
          },
        },
        decision.accountId,
        metadata,
      ),
    ];
  }

  // 缺失或重复决定也要拒绝；不能只在存在额度事实时单向校验来源。
  private assertGenerationFailureDecisions(events: RelationalEvent[]) {
    const quotaFacts = events.filter((event) => event.eventType === 'QuotaStatusChanged');
    for (const failure of events.filter((event) => event.eventType === 'JobFailed')) {
      const claims = quotaFacts.filter(
          (event) => event.payload.generationFailure?.failureEventId === failure.eventId,
        ),
        attempt = this.sqlite
          .prepare<[string], { id: string }>('SELECT id FROM generation_attempts WHERE job_id=?')
          .get(failure.payload.jobId);
      if (!attempt) {
        if (claims.length) {
          fail('凭据失效决定缺少真实派发尝试');
        }
        continue;
      }
      const responses = events
        .filter((event) => event.eventType === 'ResponseEvidenceRecorded')
        .filter((event) => event.payload.attemptId === attempt.id);
      if (
        responses.length !== 1 ||
        responses[0].eventIndex >= failure.eventIndex ||
        responses[0].streamId !== failure.streamId ||
        responses[0].memberId !== failure.memberId ||
        responses[0].accountId !== failure.accountId ||
        canonicalJson(responses[0].payload.response) !== canonicalJson(failure.payload.response)
      ) {
        fail('凭据失效决定需要同笔完整且唯一原响应与失败事实');
      }
      const response = responses[0],
        decision = this.generationCredentialDecision(
          {
            attemptId: attempt.id,
            aborted: failure.payload.aborted,
            response: response.payload.response,
          },
          failure.occurredAt,
          failure,
        );
      if (claims.length !== (decision ? 1 : 0)) {
        fail('凭据失效决定必须按原失败完整且恰一次保存');
      }
      if (
        decision &&
        (claims[0].payload.accountId !== decision.accountId ||
          claims[0].payload.status !== decision.status ||
          canonicalJson(claims[0].payload.generationFailure) !==
            canonicalJson({
              dispatchEventId: decision.dispatchEventId,
              responseEventId: response.eventId,
              failureEventId: failure.eventId,
              errorCode: decision.errorCode,
            }))
      ) {
        fail('凭据失效决定不符原失败、响应或派发来源');
      }
    }
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
    metadata: Pick<Metadata, 'recordedAt' | 'policyVersion' | 'pricingVersion'>,
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
              {
                eventType: 'QuotaStatusChanged',
                payload: { accountId, status: p.status, generationFailure: null },
              },
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
          let receipt: string;
          if (registered) {
            // 开账 eventId 是不透明身份；收据文本只来自确切的原登记事实。
            const planned = events.find(
              (event) =>
                event.eventType === 'FundingRegistered' && event.eventId === p.receiptEventId,
            );
            const saved = planned ? null : this.fact(p.receiptEventId);
            if (
              saved &&
              (!['FundingRegistered', 'OpeningFundingRecorded'].includes(saved.type) ||
                saved.accountId !== accountId)
            ) {
              fail('到账缺少同账户原资金登记');
            }
            const source = planned?.payload ?? saved!.payload;
            if (
              !('receipt' in source) ||
              typeof source.receipt !== 'string' ||
              source.resource !== p.resource ||
              source.destination !== p.destination
            ) {
              fail('到账与原收据、资源或归属不符');
            }
            receipt = source.receipt;
          } else {
            receipt = p.id.replace(/^operating-receipt:/, '');
          }
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
  assertFundingReceipt(result: { receipt: Json; commitSeq?: string }) {
    const receipt = result.receipt;
    if (
      !receipt ||
      Array.isArray(receipt) ||
      typeof receipt !== 'object' ||
      !('eventId' in receipt || 'funding' in receipt || 'duplicate' in receipt)
    ) {
      return null;
    }
    if (
      typeof receipt.eventId !== 'string' ||
      typeof receipt.accountId !== 'string' ||
      !result.commitSeq
    ) {
      fail('资金回执缺少原提交或账户来源');
    }
    const source = this.fact(receipt.eventId),
      manual =
        source.type === 'FundingRegistered' ||
        source.type === 'OperatingLossCovered' ||
        source.type === 'DebitsConfirmed' ||
        (source.type === 'ReturnClassified' && source.payload.kind === 'gap_correction') ||
        (source.type === 'FundingReceived' &&
          ['operator_share', 'declared_external_receipt'].includes(String(source.payload.source))),
      earlier = BigInt(source.commitSeq) < BigInt(result.commitSeq);
    if (
      !manual ||
      source.accountId !== receipt.accountId ||
      BigInt(source.commitSeq) > BigInt(result.commitSeq)
    ) {
      fail('资金回执必须引用本账户已经发生的原人工资金决定');
    }
    let duplicate: boolean;
    if (receipt.duplicate === undefined) {
      // 旧回执没有标志；同提交的原人工决定才能证明首次处理，不能猜较早引用的结果。
      if (earlier || !receipt.funding) {
        fail('旧资金回执缺少可证明的原处理结果');
      }
      duplicate = false;
    } else {
      if (typeof receipt.duplicate !== 'boolean') {
        fail('资金回执的原重复决定无效');
      }
      duplicate = receipt.duplicate;
    }
    if (
      duplicate !== earlier ||
      (!duplicate &&
        (!receipt.funding ||
          Array.isArray(receipt.funding) ||
          typeof receipt.funding !== 'object' ||
          receipt.funding.id !== receipt.eventId))
    ) {
      fail('资金回执与原人工决定的提交关系不符');
    }
    return { eventId: receipt.eventId, duplicate };
  }
  manageQuotaFunding(
    metadata: FundingMetadata,
    accountId: string | null,
    body: OperatingFundingInput,
    member: MemberUser | null,
  ) {
    return this.owned(() => {
      this.authorizeOwnerCommand(metadata, member);
      const actorId = this.env.OWNER_USER_ID;
      if (!actorId) {
        fail('资金操作需要当前 owner 或管理员授权', 403);
      }
      const prior =
        metadata.commandId === undefined
          ? null
          : readRelationalReceipt(this.sqlite, {
              ...metadata,
              commandId: metadata.commandId,
            });
      if (prior) {
        const response = this.assertFundingReceipt(prior);
        if (!response) {
          fail('原命令不是资金处理回执');
        }
        return { ...prior, response };
      }
      if (!accountId || this.sharingPolicy().accountId !== accountId) {
        fail('资金操作只接受当前绑定共享账户');
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
      const events = this.convert(planned.facts, view, metadata);
      if (!events.length) {
        if (metadata.commandId !== undefined) {
          fail('操作未产生可提交事实，请刷新后确认结果');
        }
        const event = this.fact(planned.result.eventId);
        const receipt = JSON.parse(
          this.sqlite
            .prepare<[bigint], { receipt: string }>(
              'SELECT receipt FROM commits WHERE commit_seq=?',
            )
            .get(BigInt(event.commitSeq))!.receipt,
        ) as Json;
        if (!this.assertFundingReceipt({ commitSeq: event.commitSeq, receipt })) {
          fail('原资金决定缺少有效回执');
        }
        return {
          replayed: true,
          commitSeq: event.commitSeq,
          receipt,
          response: planned.result,
        };
      }
      const funding =
        !planned.result.duplicate &&
        view.db.ledger.find((raw) => (raw as { id?: string }).id === planned.result.eventId);
      const receipt: Record<string, Json> = {
        accountId,
        eventId: planned.result.eventId,
        duplicate: planned.result.duplicate,
      };
      if (funding) {
        receipt.funding = funding as Json;
      }
      const result = this.commit(
        { ...metadata, commandId: metadata.commandId ?? randomUUID() },
        events,
        receipt,
      );
      const response = this.assertFundingReceipt(result);
      if (!response) {
        fail('资金命令未保存原处理结果');
      }
      return { ...result, response };
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
    this.assertGenerationFailureDecisions(events);
    const sources = new Map(events.map((e) => [e.eventId, e]));
    const memberChanges = events.filter(
        (event) => event.eventType === 'MemberTransferred' || event.eventType === 'MemberRemoved',
      ),
      memberTransfers = events.filter((event) => event.eventType === 'FundsTransferred'),
      memberBudgets = new Map<string, ResourceAmounts>(),
      stored = events.length
        ? this.sqlite
            .prepare<[bigint], { receipt: string; fingerprint: string; recordedAt: number }>(
              'SELECT receipt,fingerprint,recorded_at AS recordedAt FROM commits WHERE commit_seq=?',
            )
            .get(BigInt(events[0].commitSeq))
        : undefined,
      receipt = stored ? (JSON.parse(stored.receipt) as Json) : null,
      memberReceipt =
        receipt &&
        !Array.isArray(receipt) &&
        typeof receipt === 'object' &&
        [
          'fromUserId',
          'toUserId',
          'deletedUserId',
          'transferToUserId',
          'transferred',
          'autoJoined',
          'shareChanges',
        ].some((key) => key in receipt);
    if (memberChanges.length || memberTransfers.length || memberReceipt) {
      if (memberChanges.length !== 1 || !memberReceipt) fail('成员权益回执必须有唯一原共享决定');
      const change = memberChanges[0];
      if (change.eventType !== 'MemberTransferred' && change.eventType !== 'MemberRemoved')
        fail('成员权益决定类型不符');
      if (memberTransfers.length !== (change.payload.boundAccountId ? 1 : 0))
        fail('成员权益决定缺少或重复原账户资金子事实');
      const money = memberTransfers[0];
      if (
        money &&
        (money.eventType !== 'FundsTransferred' ||
          money.payload.configurationEventId !== change.eventId ||
          money.payload.configVersion !== change.payload.configVersion ||
          money.payload.accountId !== change.payload.boundAccountId ||
          money.payload.fromMemberId !== change.payload.sourceMemberId ||
          money.payload.toMemberId !== change.payload.targetMemberId ||
          money.payload.reason !==
            (change.eventType === 'MemberTransferred' ? 'transfer' : 'member_removal') ||
          money.eventIndex <= change.eventIndex ||
          money.occurredAt !== change.occurredAt ||
          money.policyVersion !== change.policyVersion ||
          money.pricingVersion !== change.pricingVersion)
      ) {
        fail('成员资金必须属于同笔原配置、原账户及原角色');
      }
      const removals = events.filter(
        (event) => event.eventType === 'MemberStatusChanged' && event.payload.status === 'removed',
      );
      if (
        change.eventType === 'MemberTransferred'
          ? removals.length !== 0
          : removals.length !== 1 ||
            removals[0].eventType !== 'MemberStatusChanged' ||
            removals[0].payload.memberId !== change.payload.sourceMemberId ||
            removals[0].eventIndex <= (money?.eventIndex ?? change.eventIndex) ||
            removals[0].occurredAt !== change.occurredAt
      ) {
        fail('成员身份撤销必须在原共享及资金决定之后');
      }
    }
    const consumed = new Set<string>();
    const observationResources = new Map<string, Set<string>>();
    const checkedSync = new Set<string>();
    const checkedConfirmations = new Set<string>();
    const syncFunding = (fact: RelationalEvent) =>
      fact.eventType === 'FundingReceived' &&
      fact.payload.source === 'registered_external_funding' &&
      events.some(
        (adoption) =>
          adoption.eventType === 'QuotaBaselineAdopted' &&
          adoption.accountId === fact.accountId &&
          adoption.payload.observationEventId === fact.payload.observationEventId &&
          fact.eventIndex < adoption.eventIndex,
      );
    const decision = (fact: RelationalEventInput) => ({
      eventType: fact.eventType,
      payload: fact.payload,
      // 收据/差额事实的 ID 是后续来源；其他生成的 envelope ID 不参与金融决定。
      sourceId:
        ['FundingReceived', 'ResourceGapObserved', 'ReturnClassified'].includes(fact.eventType) ||
        (fact.eventType === 'V5CapacityObserved' && fact.payload.delta !== '0')
          ? fact.eventId
          : null,
      occurredAt: fact.occurredAt,
      accountId: fact.accountId,
      memberId: fact.memberId,
      policyVersion: fact.policyVersion,
      pricingVersion: fact.pricingVersion,
    });
    if (memberChanges.length) {
      const change = memberChanges[0];
      if (change.eventType !== 'MemberTransferred' && change.eventType !== 'MemberRemoved')
        fail('成员权益决定类型不符');
      const policy = this.sharingPolicy(),
        view = policy.accountId ? this.state(policy.accountId) : null;
      if (policy.accountId !== change.payload.boundAccountId) fail('成员权益缺少原绑定来源');
      const prefix = view
          ? this.convert(
              this.capture(view, () => syncBoundFairQuota(view.db, view.account, change.occurredAt))
                .facts,
              view,
              {
                recordedAt: change.occurredAt,
                policyVersion: change.policyVersion,
                pricingVersion: change.pricingVersion,
              },
            )
          : [],
        removed = events.filter(
          (event) =>
            event.eventType === 'MemberStatusChanged' && event.payload.status === 'removed',
        ),
        suffix = [...memberTransfers, ...removed];
      if (
        canonicalJson(prefix.map(decision)) !==
          canonicalJson(events.slice(0, change.eventIndex).map(decision)) ||
        canonicalJson(suffix.map((event) => event.eventId)) !==
          canonicalJson(events.slice(change.eventIndex + 1).map((event) => event.eventId))
      ) {
        fail('成员完整提交必须保留原同步前缀与唯一原资金、身份后缀');
      }
    }
    // 在本提交的第一个同步金融事实前取得原序切点。规划只改独立 view，不投影或查询上游。
    // 同时核对到账/返回组成，不能以分配事实自报的预算替代原观察净额。
    const assertSyncDecisions = (e: RelationalEvent, accountId: string, q: AccountQuota) => {
      const observationId = q.observationEventId,
        configVersion = this.sharingPolicy().configVersion,
        key = canonicalJson([accountId, observationId, configVersion]);
      if (checkedSync.has(key)) {
        return;
      }
      const view = this.state(accountId);
      if (e.eventType === 'QuotaObserved') {
        // 核验启用先保存观察，再投影可用性；只在独立 view 使用引用该观察的原启用决定。
        // 实际 AvailabilityChanged 仍须经过自己的观察/代次校验，失败由保存点整笔回滚。
        const availability = events.filter(
          (fact) =>
            fact.eventType === 'AvailabilityChanged' &&
            fact.accountId === accountId &&
            fact.payload.enabled &&
            fact.payload.observationEventId === e.eventId &&
            fact.eventIndex > e.eventIndex,
        );
        if (availability.length > 1) {
          fail('观察不能同时属于多个账户可用性决定');
        }
        if (availability[0]?.eventType === 'AvailabilityChanged') {
          view.account.enabled = availability[0].payload.enabled;
        }
      }
      const belongs = (fact: RelationalEventInput) => {
        if (fact.accountId !== accountId) {
          return false;
        }
        switch (fact.eventType) {
          case 'RefillApplied':
            return (
              fact.payload.configVersion === configVersion &&
              fact.payload.observationEventId === view.q.baselineEventId
            );
          case 'AllocationApplied':
            return (
              fact.payload.configVersion === configVersion &&
              fact.payload.sourceEventId === observationId
            );
          case 'QuotaBaselineAdopted':
          case 'V5CapacityObserved':
            return fact.payload.observationEventId === observationId;
          case 'FundingReceived':
            return (
              fact.payload.source === 'registered_external_funding' &&
              fact.payload.observationEventId === observationId &&
              (!('eventIndex' in fact) || syncFunding(fact as RelationalEvent)) &&
              events.some(
                (adoption) =>
                  adoption.eventType === 'QuotaBaselineAdopted' &&
                  adoption.accountId === accountId &&
                  adoption.payload.observationEventId === observationId,
              )
            );
          case 'ReturnClassified':
            return (
              fact.payload.kind === 'observed' && fact.payload.observationEventId === observationId
            );
          case 'ResourceGapObserved':
            return fact.payload.afterObservationEventId === observationId;
          default:
            return false;
        }
      };
      const planned = this.capture(view, () =>
          syncBoundFairQuota(view.db, view.account, e.occurredAt),
        ),
        expected = this.convert(planned.facts, view, {
          recordedAt: e.occurredAt,
          policyVersion: e.policyVersion,
          pricingVersion: e.pricingVersion,
        });
      if (
        canonicalJson(expected.filter(belongs).map(decision)) !==
        canonicalJson(events.filter(belongs).map(decision))
      ) {
        fail('同步金融事实与原观察、净预算、配置及分配决定不符');
      }
      checkedSync.add(key);
    };
    // 人工扣款确认的到账组成使用同一资金 owner；不能把运营归属冒充可分配净额。
    const assertConfirmationDecisions = (e: RelationalEvent, accountId: string) => {
      const confirmations = events.filter(
        (fact) => fact.eventType === 'DebitsConfirmed' && fact.accountId === accountId,
      );
      if (confirmations.length !== 1) {
        fail('登记到账缺少本次唯一原扣款确认');
      }
      const source = confirmations[0];
      if (source.eventType !== 'DebitsConfirmed') {
        fail('扣款确认来源类型不符');
      }
      if (checkedConfirmations.has(source.eventId)) {
        return;
      }
      const view = this.state(accountId),
        snapshot = view.db.settings.fairQuota.snapshot;
      if (!snapshot || e.eventIndex > source.eventIndex) {
        fail('扣款确认缺少原金融切点');
      }
      const planned = this.capture(view, () => {
          syncBoundFairQuota(view.db, view.account, source.occurredAt);
          if (view.db.settings.fairQuota.status !== 'active') {
            fail('扣款确认缺少原有效观察');
          }
          return matchExternalFunding(
            view.db,
            source.payload.resource,
            parseAmount(source.payload.amount),
            snapshot,
            source.occurredAt,
            source.eventId,
          );
        }),
        expected = this.convert(planned.facts, view, {
          recordedAt: source.occurredAt,
          policyVersion: source.policyVersion,
          pricingVersion: source.pricingVersion,
        }),
        actual = events.filter(
          (fact) =>
            fact.accountId === accountId &&
            fact.eventType === 'FundingReceived' &&
            fact.payload.source === 'registered_external_funding' &&
            !syncFunding(fact),
        );
      if (
        source.payload.allocatedToMembers !== formatAmount(planned.result) ||
        actual.some((fact) => fact.eventIndex >= source.eventIndex) ||
        canonicalJson(expected.map(decision)) !== canonicalJson(actual.map(decision))
      ) {
        fail('扣款确认净预算或原收据、资源、资金归属不符');
      }
      checkedConfirmations.add(source.eventId);
    };
    const assertFundingAllocation = (
      source: RelationalEvent,
      accountId: string,
      resource: keyof ResourceAmounts,
      amount: string,
    ) => {
      const state = this.state(accountId).db.settings.fairQuota,
        planned = planFairQuotaAllocation(state, resource, parseAmount(amount)),
        allocations = events.filter(
          (fact) =>
            fact.eventType === 'AllocationApplied' && fact.payload.sourceEventId === source.eventId,
        ),
        expected = planned.allocations.length
          ? [
              this.event(
                {
                  eventType: 'AllocationApplied',
                  payload: {
                    accountId,
                    reason: 'funding',
                    sourceEventId: source.eventId,
                    sourceResources: { ...zero(), [resource]: amount },
                    allocations: planned.allocations.map((allocation) => ({
                      memberId: allocation.memberId,
                      resources: { ...zero(), [resource]: allocation.amount },
                    })),
                    configVersion: state.configVersion,
                  },
                },
                accountId,
                {
                  recordedAt: source.occurredAt,
                  policyVersion: source.policyVersion,
                  pricingVersion: source.pricingVersion,
                },
              ),
            ]
          : [];
      // 首次分配必须完整存在；后续重复仍由同提交 consumed 守卫逐原序拒绝。
      if (
        (expected.length === 0 && allocations.length > 0) ||
        allocations.some((fact) => fact.eventIndex <= source.eventIndex) ||
        canonicalJson(expected.map(decision)) !==
          canonicalJson(allocations.slice(0, 1).map(decision))
      ) {
        fail('有限资金来源缺少原配置、权重和容量决定的完整分配');
      }
    };
    return (e: RelationalEvent): void => {
      if (!this.sqlite.inTransaction) {
        throw new Error('Quota projection requires the owning transaction.');
      }
      if (e.eventType === 'MemberTransferred' || e.eventType === 'MemberRemoved') {
        const p = e.payload,
          policy = this.sharingPolicy(),
          money = memberTransfers[0];
        if (
          e.streamId !== 'sharing:default' ||
          e.accountId !== null ||
          e.memberId !== null ||
          p.boundAccountId !== policy.accountId ||
          e.occurredAt !== stored?.recordedAt
        ) {
          fail('成员权益决定缺少原共享绑定');
        }
        this.assertMemberRightsIdle(p.sourceMemberId, p.targetMemberId);
        this.assertMemberPoolRights(
          p.sourceMemberId,
          policy.accountId,
          e.eventType === 'MemberRemoved',
        );
        this.assertMemberReceiverRights(p.targetMemberId, policy.accountId);
        let body: MemberChange;
        if (e.eventType === 'MemberTransferred') {
          if (!money || money.eventType !== 'FundsTransferred') fail('转账必须有真实原账户资源');
          const credited = resources.filter(
            (resource) => parseAmount(money.payload.resources[resource]) > 0n,
          );
          if (credited.length !== 1) fail('转账必须保存原单一正资源决定');
          body = {
            action: 'transfer-quota',
            expectedConfigVersion: policy.configVersion,
            fromUserId: p.sourceMemberId,
            toUserId: p.targetMemberId,
            resource: credited[0],
            amount: amountNumber(parseAmount(money.payload.resources[credited[0]])),
          };
        } else {
          body = {
            action: 'delete-member',
            expectedConfigVersion: policy.configVersion,
            userId: p.sourceMemberId,
            transferToUserId: p.targetMemberId,
          };
        }
        const view = this.memberView(),
          planned = this.capture(view, () =>
            applyFairMemberChange(view.db, structuredClone(view.db), body, e.occurredAt),
          ),
          state = view.db.settings!.fairQuota!,
          funds = planned.facts.find((fact) => fact.eventType === 'FundsTransferred');
        if (
          p.configVersion !== state.configVersion ||
          (e.eventType === 'MemberRemoved' &&
            canonicalJson(e.payload.groups) !==
              canonicalJson(
                state.groups.map((group) => ({
                  groupId: group.id,
                  name: group.name,
                  share: group.share,
                  members: group.members.map((memberId) => ({
                    memberId,
                    weight: group.memberWeights[memberId],
                  })),
                })),
              )) ||
          canonicalJson(receipt) !== canonicalJson(planned.result) ||
          stored?.fingerprint !==
            domainDigest({ operation: 'member-quota', input: body } as unknown as Json)
        ) {
          fail('成员权益事实、原意图或回执与原 owner 决定不符');
        }
        if (p.boundAccountId) {
          if (
            !money ||
            money.eventType !== 'FundsTransferred' ||
            !funds ||
            funds.eventType !== 'FundsTransferred' ||
            funds.payload.kind === 'redistribution' ||
            canonicalJson(money.payload.resources) !== canonicalJson(funds.payload.transferred)
          ) {
            fail('成员资金必须保存原 owner 的同账户三池决定');
          }
          memberBudgets.set(e.eventId, money.payload.resources);
        } else if (funds) fail('未绑定成员决定不能制造资金来源');
        if (e.eventType === 'MemberRemoved') {
          this.sqlite.prepare('DELETE FROM group_members').run();
          for (const group of e.payload.groups) {
            if (
              this.sqlite
                .prepare('UPDATE sharing_groups SET name=?,share=?,source_event_id=? WHERE id=?')
                .run(group.name, group.share, e.eventId, group.groupId).changes !== 1
            )
              fail('成员移除不能删除或制造组身份');
            for (const member of group.members) {
              this.sqlite
                .prepare(
                  'INSERT INTO group_members(group_id,member_id,weight,source_event_id) VALUES(?,?,?,?)',
                )
                .run(group.groupId, member.memberId, member.weight, e.eventId);
              if (p.boundAccountId)
                this.sqlite
                  .prepare(
                    `INSERT OR IGNORE INTO wallets(account_id,member_id,v5_available,anlas_fixed_available,anlas_purchased_available,source_event_id)
                VALUES(?,?,'0','0','0',?)`,
                  )
                  .run(p.boundAccountId, member.memberId, e.eventId);
            }
          }
        }
        this.sqlite
          .prepare('UPDATE sharing_policy SET config_version=?,source_event_id=? WHERE id=1')
          .run(p.configVersion, e.eventId);
        return;
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
        if (current?.accountId) {
          assertSyncDecisions(e, current.accountId, this.readAccountQuota(current.accountId));
        }
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
          w.anlas_purchased_available AS anlasPurchasedAvailable FROM wallets w WHERE w.account_id=?`,
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
      if (
        [
          'RefillApplied',
          'QuotaBaselineAdopted',
          'V5CapacityObserved',
          'ResourceGapObserved',
          'FundingRegistered',
          'FundingReceived',
          'DebitsConfirmed',
          'OperatingLossCovered',
          'ReturnClassified',
        ].includes(e.eventType) ||
        (e.eventType === 'AllocationApplied' &&
          this.fact(e.payload.sourceEventId).type === 'QuotaObserved')
      ) {
        assertSyncDecisions(e, accountId, q);
      }
      if (
        e.eventType === 'DebitsConfirmed' ||
        (e.eventType === 'FundingReceived' &&
          e.payload.source === 'registered_external_funding' &&
          !syncFunding(e))
      ) {
        assertConfirmationDecisions(e, accountId);
      }
      if (e.eventType === 'DebitsConfirmed') {
        assertFundingAllocation(e, accountId, e.payload.resource, e.payload.allocatedToMembers);
      } else if (
        e.eventType === 'FundingReceived' &&
        e.payload.source === 'declared_external_receipt'
      ) {
        assertFundingAllocation(
          e,
          accountId,
          e.payload.resource,
          e.payload.destination === 'members' ? e.payload.amount : '0',
        );
      }
      switch (e.eventType) {
        case 'FundsTransferred': {
          const p = e.payload,
            budget = memberBudgets.get(p.configurationEventId);
          this.configuration(p.configVersion);
          if (
            !budget ||
            consumed.has(p.configurationEventId) ||
            canonicalJson(budget) !== canonicalJson(p.resources)
          ) {
            fail('成员资金缺少已应用的原配置预算或重复消费');
          }
          for (const [memberId, direction] of [
            [p.fromMemberId, -1n],
            [p.toMemberId, 1n],
          ] as const) {
            const before = this.sqlite
              .prepare<[string, string], ResourceAmounts>(
                'SELECT v5_available AS v5,anlas_fixed_available AS anlasFixed,anlas_purchased_available AS anlasPurchased FROM wallets WHERE account_id=? AND member_id=?',
              )
              .get(accountId, memberId);
            if (!before) {
              if (direction === -1n && resources.every((resource) => p.resources[resource] === '0'))
                continue;
              fail('成员资金缺少原账户钱包');
            }
            const next = zero();
            for (const resource of resources) {
              const value =
                parseAmount(before[resource]) + direction * parseAmount(p.resources[resource]);
              if (value < 0n) fail('成员资金超过原账户钱包');
              next[resource] = formatAmount(value);
            }
            this.sqlite
              .prepare(
                'UPDATE wallets SET v5_available=?,anlas_fixed_available=?,anlas_purchased_available=?,source_event_id=? WHERE account_id=? AND member_id=?',
              )
              .run(next.v5, next.anlasFixed, next.anlasPurchased, e.eventId, accountId, memberId);
          }
          consumed.add(p.configurationEventId);
          break;
        }
        case 'QuotaObserved': {
          const p = e.payload;
          if (
            p.credentialEpoch !== a.epoch ||
            p.connectionSourceEventId !==
              relationalAccountConnectionSource(this.sqlite, accountId, e) ||
            BigInt(p.sequence) <= BigInt(q.observedSequence ?? '0')
          ) {
            fail('观察代次或序号已经过期');
          }
          if (p.errorCode) {
            const previous = this.officialQuotaV5(accountId, e.streamVersion),
              original = this.state(accountId).account,
              snapshot = fairQuotaSnapshot(original);
            if (
              canonicalJson(previous) !==
                canonicalJson({
                  v5UsagePercent: p.v5UsagePercent,
                  v5UsageIsNegative: p.v5UsageIsNegative,
                }) ||
              p.observedAt !== snapshot.at ||
              p.v5 !== financialObservedV5(original, snapshot)
            ) {
              fail('失败观察必须保留此前原官方值、观察时点及原金融决定');
            }
          } else {
            const original = {
              id: accountId,
              v5UsagePercent: p.v5UsagePercent,
              v5UsageIsNegative: p.v5UsageIsNegative,
            };
            if (p.v5 !== financialObservedV5(original, fairQuotaSnapshot(original))) {
              fail('官方原值与原金融 V5 表示不符');
            }
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
          assertSyncDecisions(e, accountId, q);
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
        case 'QuotaStatusChanged': {
          const origin = e.payload.generationFailure;
          if (origin) {
            const response = sources.get(origin.responseEventId),
              failure = sources.get(origin.failureEventId);
            if (
              !response ||
              response.eventType !== 'ResponseEvidenceRecorded' ||
              !failure ||
              failure.eventType !== 'JobFailed'
            ) {
              fail('凭据失效缺少同笔原响应与失败事实');
            }
            const original = this.generationDispatch(response.payload.attemptId);
            if (
              !original ||
              original.dispatch.eventId !== origin.dispatchEventId ||
              original.attempt.accountId !== accountId ||
              original.attempt.credentialEpoch !== a.epoch ||
              original.connectionSourceEventId === null ||
              original.connectionSourceEventId !==
                relationalAccountConnectionSource(this.sqlite, accountId, e) ||
              response.commitSeq !== e.commitSeq ||
              failure.commitSeq !== e.commitSeq ||
              response.eventIndex >= e.eventIndex ||
              failure.eventIndex <= e.eventIndex ||
              response.streamId !== `request:${original.attempt.requestId}` ||
              failure.streamId !== response.streamId ||
              response.memberId !== original.attempt.memberId ||
              failure.memberId !== response.memberId ||
              response.accountId !== accountId ||
              failure.accountId !== accountId ||
              e.memberId !== null ||
              failure.payload.jobId !== original.attempt.jobId ||
              canonicalJson(failure.payload.response) !==
                canonicalJson(response.payload.response) ||
              response.payload.response.dispatchState === 'not_sent' ||
              generationCredentialError(
                failure.payload.aborted,
                response.payload.response.upstreamHttpStatus,
              ) !== origin.errorCode ||
              quotaAvailability(
                { ...this.state(accountId).account, quotaError: origin.errorCode },
                e.occurredAt,
              ) !== e.payload.status
            ) {
              fail('凭据失效与同笔原失败、响应、派发或当前代次不符');
            }
            q.quotaErrorCode = origin.errorCode;
          }
          q.status = e.payload.status;
          this.saveAccountQuota(q, e.eventId);
          break;
        }
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
        case 'FundingRegistered':
        case 'OpeningFundingRecorded': {
          const p = e.payload;
          if (e.eventType === 'OpeningFundingRecorded') {
            assertRelationalOpeningSource(this.sqlite, e);
          }
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
      this.assertFundingReceipt(result);
      if (!result.replayed) {
        this.applyQuotaEvents(result.events);
        this.assertGroupCommit(result.events, result.receipt);
      }
      return result;
    });
  }
}
