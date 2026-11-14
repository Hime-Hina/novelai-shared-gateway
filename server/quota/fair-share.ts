import { quoteNativeRequest, NATIVE_PRICING_VERSION } from '../protocols/native-pricing.ts';
import {
  recordGenerationAttempt,
  recordGenerationRefund,
  type AccountingJob,
  type RefundEvidence,
} from '../generation/accounting.ts';
import {
  operatingAccount,
  readOperatingAccount,
  recordAnlasGap,
  reconcileV5Capacity,
  recoverV5Capacity,
  matchExternalFunding,
  applyOperatingFunding,
  type OperatingAccount,
  type OperatingFundingInput,
  type OperatingFundingContext,
} from './operating-accounting.ts';
import {
  QUOTA_SNAPSHOT_MAX_AGE_MS,
  quotaTimestamp,
  isUsableOpus,
  quotaAvailability,
  quotaNumber as number,
} from './eligibility.ts';
import type { FreeImagePolicy } from './free-image-policy.ts';
import type { LossAlertPolicy } from './failure-accounting.ts';
import {
  AMOUNT_SCALE,
  parseAmount,
  formatAmount,
  amountNumber,
  decimalRatio,
  divideAmount,
  allocateAmount,
  normalizeLegacyAmountGroup,
  minAmount,
  maxAmount,
  type AmountText,
  type AmountInput,
  type ResourceAmountsText,
} from './resource-amounts.ts';
import {
  recordDomainEvent,
  inheritDomainCommand,
  type DomainEventFact,
} from '../events/domain-events.ts';
import type { QuotaAllocation, RefillFact } from '../events/quota-projections.ts';

export type Resource = 'v5' | 'anlasFixed' | 'anlasPurchased';
export type AnlasResource = Exclude<Resource, 'v5'>;
type BalanceField = `${Resource}Available` | 'anlasAvailable';
export type ResourceAmounts = ResourceAmountsText;
export type FairRequest = NonNullable<Parameters<typeof quoteNativeRequest>[0]> & {
  allowPurchasedAnlas?: unknown;
  prompt?: string;
};
type QuoteOptions = NonNullable<Parameters<typeof quoteNativeRequest>[1]>;

export interface FairBalance {
  v5Available: AmountText;
  anlasAvailable: AmountText;
  anlasFixedAvailable: AmountText;
  anlasPurchasedAvailable: AmountText;
}
export interface FairGroup {
  id: string;
  name?: string;
  members: string[];
  share: number;
  memberWeights: Record<string, number>;
}
export interface QuotaSnapshot {
  at: number;
  v5: AmountText | null;
  anlas: AmountText | null;
  anlasFixed: AmountText | null;
  anlasPurchased: AmountText | null;
  seconds: number | null;
}
// 快照的算术断言只服务于类型检查；历史 null 等值仍按原 JS 规则运算，不补值或重新归一化。
interface ChargeBase extends Omit<ReturnType<typeof quoteNativeRequest>, 'amount'> {
  amount: AmountText;
  fixed: AmountText;
  purchased: AmountText;
  configVersion: number;
  groupId?: string;
  userId: string;
  accountId: string;
  status: 'reserved' | 'settled' | 'refunded';
  reservedAt: number;
  settledAt?: number;
  attemptId?: string;
}
export type FairCharge = ChargeBase &
  (
    | {
        billingMode: 'anlas_fallback';
        v5Equivalent: AmountText;
        sharedV5Reserved: AmountText;
        upstreamBilling: 'shared_or_anlas';
      }
    | {
        billingMode?: undefined;
        v5Equivalent?: undefined;
        sharedV5Reserved?: undefined;
        upstreamBilling?: undefined;
      }
  );
interface FallbackTotals {
  fixed: AmountText;
  purchased: AmountText;
  v5Equivalent: AmountText;
}
interface FairSpent extends ResourceAmounts {
  anlas: AmountText;
  fallbackFixed: AmountText;
  fallbackPurchased: AmountText;
  fallbackV5: AmountText;
}
interface V5Reconciliation {
  at: number;
  sourceCheckedAt: number;
  officialV5: AmountText | null;
  localAvailable: AmountText;
  sharedV5Debt: AmountText;
  unallocatedV5: AmountText;
  difference: AmountText;
  tolerance: AmountText;
  status: 'source_below_local' | 'source_above_local' | 'within_precision';
  personalDelta: AmountText;
  policy: string;
}
type SyncReservation = Pick<
  FairCharge,
  'userId' | 'status' | 'resource' | 'amount' | 'fixed' | 'purchased' | 'sharedV5Reserved'
>;
export interface FairQuotaState<Reservation extends SyncReservation = FairCharge> {
  version: number;
  enabled: boolean;
  members: string[];
  boundAccountId: string | null;
  groups: FairGroup[];
  configVersion: number;
  configurationHistory: unknown[];
  balances: Record<string, FairBalance>;
  reservations: Record<string, Reservation>;
  snapshot: QuotaSnapshot | null;
  spent: FairSpent;
  creditedV5: AmountText;
  lastRefillAt: number;
  lastSettlementAt: number;
  status: string;
  unallocatedV5: AmountText;
  fallbackBuffer: { fixed: AmountText; purchased: AmountText };
  fallbackTotalsByUser: Record<string, FallbackTotals>;
  sharedV5Debt: AmountText;
  autoAnlasFallback?: unknown;
  operating?: OperatingAccount;
  v5Reconciliation?: V5Reconciliation;
  v5ReconciliationHistory?: V5Reconciliation[];
  freeImagePolicy?: FreeImagePolicy;
  lossAlertPolicy?: Partial<LossAlertPolicy> | null;
  policyHistory?: { at: number; actor: string; autoAnlasFallback: boolean }[];
}
// 当前金融状态的窄视图；不包含旧配置历史、用户、任务正文或认证数据。
export type FairQuotaSyncState = Omit<
  Pick<
    FairQuotaState,
    | 'enabled'
    | 'boundAccountId'
    | 'members'
    | 'groups'
    | 'configVersion'
    | 'balances'
    | 'reservations'
    | 'snapshot'
    | 'spent'
    | 'creditedV5'
    | 'lastRefillAt'
    | 'lastSettlementAt'
    | 'status'
    | 'unallocatedV5'
    | 'fallbackBuffer'
    | 'sharedV5Debt'
    | 'operating'
    | 'v5Reconciliation'
    | 'v5ReconciliationHistory'
  >,
  'reservations' | 'v5Reconciliation'
> & {
  v5Reconciliation?: Pick<V5Reconciliation, 'status'>;
  reservations: Record<string, SyncReservation>;
};
export interface FairQuotaSyncDatabase {
  settings: { fairQuota: FairQuotaSyncState };
  ledger?: unknown[];
}

type UpgradedField =
  | 'groups'
  | 'configVersion'
  | 'configurationHistory'
  | 'unallocatedV5'
  | 'fallbackBuffer'
  | 'fallbackTotalsByUser'
  | 'sharedV5Debt'
  | 'spent';
type LegacyFairQuotaState<Reservation extends SyncReservation> = Omit<
  FairQuotaState<Reservation>,
  UpgradedField
> &
  Partial<Omit<Pick<FairQuotaState, UpgradedField>, 'groups' | 'spent'>> & {
    groups?: (Omit<FairGroup, 'share' | 'memberWeights'> &
      Partial<Pick<FairGroup, 'share' | 'memberWeights'>>)[];
    spent: Omit<FairSpent, 'fallbackFixed' | 'fallbackPurchased' | 'fallbackV5'> &
      Partial<Pick<FairSpent, 'fallbackFixed' | 'fallbackPurchased' | 'fallbackV5'>>;
  };
export interface FairAccount {
  id: string;
  enabled?: unknown;
  quotaError?: unknown;
  quotaTier?: unknown;
  subscriptionActive?: unknown;
  subscriptionExpiresAt?: unknown;
  v5UsagePercent?: unknown;
  quotaFixed?: unknown;
  quotaPurchased?: unknown;
  v5UsageIsNegative?: unknown;
  quotaCheckedAt?: unknown;
  v5UsageTimeUntilNextPercent?: unknown;
}
export interface FairUser {
  id: string;
  token?: string;
  enabled?: boolean;
}
export interface FairQuotaDatabase<Reservation extends SyncReservation = FairCharge> {
  settings?: { fairQuota?: FairQuotaState<Reservation> | null };
  users?: FairUser[];
  accounts?: FairAccount[] | Record<string, FairAccount> | null;
  jobs?: AccountingJob[] | Record<string, AccountingJob> | null;
  ledger?: unknown[];
}
export type FairMemberQuotaState = Omit<FairQuotaState<SyncReservation>, 'v5Reconciliation'> & {
  v5Reconciliation?: Pick<V5Reconciliation, 'status'>;
};
export type FairMemberQuotaDatabase = Omit<FairQuotaDatabase, 'settings'> & {
  settings?: { fairQuota?: FairMemberQuotaState | null };
};
export type FairQuotaSummaryState = FairQuotaSyncState &
  Pick<FairQuotaState, 'autoAnlasFallback' | 'fallbackTotalsByUser'>;
type ConfiguredDatabase = FairQuotaDatabase & { settings: { fairQuota: FairQuotaState } };
interface GroupInput {
  id?: unknown;
  name?: unknown;
  members?: unknown;
  share?: unknown;
  memberWeights?: unknown;
}
interface GroupOptions {
  now?: number;
  redistributeRemaining?: unknown;
  actorId?: string;
}
export interface MemberChange {
  action?: string;
  expectedConfigVersion?: unknown;
  fromUserId?: string;
  toUserId?: string;
  userId?: string;
  transferToUserId?: string;
  resource?: string;
  amount?: unknown;
}
// 所有修改必须在存储事务内执行；账本与任务一起持久化，避免重启后重复扣款。
const STALE_MS = QUOTA_SNAPSHOT_MAX_AGE_MS;
const stamp = (v: unknown): number => quotaTimestamp(v) ?? 0;
const rows = <T>(v: T[] | Record<string, T> | null | undefined): T[] =>
  Array.isArray(v) ? v : Object.values(v || {});
const fail: (message: string, status?: number) => never = (message, status = 402) => {
  const e = new Error(message) as Error & { status: number; statusCode: number };
  e.status = e.statusCode = status;
  throw e;
};
const active = (a: FairAccount | undefined, now: number): a is FairAccount => isUsableOpus(a, now);
const units = (value: AmountInput | null | undefined): bigint => {
  if (value == null) {
    return 0n;
  }
  return parseAmount(value, typeof value === 'number' ? 'nearest' : 'exact');
};
const normalized = (value: AmountInput | null | undefined): AmountText =>
  formatAmount(units(value));
const sum = (s: FairQuotaSyncState, field: BalanceField): bigint =>
  Object.values(s.balances).reduce((n, b) => n + units(b[field]), 0n);
const held = (s: FairQuotaSyncState, id: string, resource: Resource | 'anlas'): bigint =>
  Object.values(s.reservations)
    .filter((c) => c.userId === id && c.status === 'reserved')
    .reduce((n, c) => {
      let amount: AmountInput | undefined = '0';
      if (c.resource === resource) {
        amount = c.amount;
      } else if (resource === 'anlasFixed') {
        amount = c.fixed;
      } else if (resource === 'anlasPurchased') {
        amount = c.purchased;
      }
      return n + units(amount);
    }, 0n);
const sharedHeld = (s: FairQuotaSyncState): bigint =>
  Object.values(s.reservations)
    .filter((c) => c.status === 'reserved')
    .reduce((n, c) => n + units(c.resource === 'v5' ? c.amount : c.sharedV5Reserved), 0n);
const anyHeld = (s: Pick<FairQuotaSyncState, 'reservations'>): boolean =>
  Object.values(s.reservations).some((c) => c.status === 'reserved');
const totals = (s: FairQuotaSyncState): void => {
  for (const b of Object.values(s.balances)) {
    b.anlasAvailable = formatAmount(
      units(b.anlasFixedAvailable) + units(b.anlasPurchasedAvailable),
    );
  }
};

const emptyBalance = (): FairBalance => ({
  v5Available: '0',
  anlasAvailable: '0',
  anlasFixedAvailable: '0',
  anlasPurchasedAvailable: '0',
});
function upgradeGroups<Reservation extends SyncReservation>(
  s: LegacyFairQuotaState<Reservation>,
): FairQuotaState<Reservation> {
  if (!s.groups) {
    s.groups = s.members.map((id, i) => ({
      id: `group_${i + 1}`,
      name: `第 ${i + 1} 组`,
      members: [id],
    }));
  }
  for (const group of s.groups) {
    group.share ??= 0.25;
    group.memberWeights = Object.fromEntries(
      group.members.map((id) => [id, group.memberWeights?.[id] ?? 1]),
    );
  }
  s.version = 2;
  s.configVersion ||= 1;
  s.configurationHistory ||= [];
  s.unallocatedV5 = normalized(s.unallocatedV5);
  s.creditedV5 = normalized(s.creditedV5);
  s.sharedV5Debt = normalized(s.sharedV5Debt);
  for (const field of [
    'v5',
    'anlas',
    'anlasFixed',
    'anlasPurchased',
    'fallbackFixed',
    'fallbackPurchased',
    'fallbackV5',
  ] as const) {
    s.spent[field] = normalized(s.spent[field]);
  }
  s.spent.anlas = formatAmount(units(s.spent.anlasFixed) + units(s.spent.anlasPurchased));
  s.fallbackBuffer ||= { fixed: '0', purchased: '0' };
  s.fallbackBuffer.fixed = normalized(s.fallbackBuffer.fixed);
  s.fallbackBuffer.purchased = normalized(s.fallbackBuffer.purchased);
  s.fallbackTotalsByUser ||= {};
  for (const total of Object.values(s.fallbackTotalsByUser)) {
    for (const field of ['fixed', 'purchased', 'v5Equivalent'] as const) {
      total[field] = normalized(total[field]);
    }
  }
  for (const balance of Object.values(s.balances)) {
    for (const field of [
      'v5Available',
      'anlasFixedAvailable',
      'anlasPurchasedAvailable',
      'anlasAvailable',
    ] as const) {
      balance[field] = normalized(balance[field]);
    }
  }
  for (const charge of Object.values(s.reservations)) {
    normalizeCharge(charge);
  }
  if (s.snapshot) {
    for (const resource of ['v5', 'anlasFixed', 'anlasPurchased', 'anlas'] as const) {
      if (s.snapshot[resource] !== null) {
        s.snapshot[resource] = normalized(s.snapshot[resource]);
      }
    }
  }
  if (s.snapshot && s.snapshot.anlasFixed !== null && s.snapshot.anlasPurchased !== null) {
    s.snapshot.anlas = formatAmount(
      units(s.snapshot.anlasFixed) + units(s.snapshot.anlasPurchased),
    );
  }
  totals(s as FairQuotaState<Reservation>);
  // 上面的原位升级补齐旧四人账本字段，不重建已有钱包或预留。
  return s as FairQuotaState<Reservation>;
}
const groupFor = (s: FairQuotaSyncState, id: string): FairGroup | undefined =>
  s.groups.find((g) => g.members.includes(id));
const weight = (s: FairQuotaSyncState, id: string): number => {
  const group = groupFor(s, id);
  if (!group) {
    return 0;
  }
  const total = group.members.reduce((n, member) => n + (group.memberWeights?.[member] ?? 1), 0);
  return total > 0 ? (group.share ?? 0.25) * ((group.memberWeights?.[id] ?? 1) / total) : 0;
};
function normalizeCharge<
  Charge extends Pick<FairCharge, 'amount' | 'fixed' | 'purchased'> &
    Partial<Pick<FairCharge, 'billingMode' | 'v5Equivalent' | 'sharedV5Reserved'>>,
>(charge: Charge): Charge {
  charge.amount = normalized(charge.amount);
  charge.fixed = normalized(charge.fixed);
  charge.purchased = normalized(charge.purchased);
  if (charge.billingMode === 'anlas_fallback') {
    charge.v5Equivalent = normalized(charge.v5Equivalent);
    charge.sharedV5Reserved = normalized(charge.sharedV5Reserved);
  }
  return charge;
}

export type OpeningAmountGroupReport = ReturnType<typeof normalizeLegacyAmountGroup> & {
  resource: Resource;
};
export function normalizeOpeningQuota(
  state: FairQuotaState,
  reports: OpeningAmountGroupReport[] = [],
): FairQuotaState {
  const copy = structuredClone(state),
    groups: Record<
      Resource,
      { id: string; value: AmountInput; assign: (amount: string) => void }[]
    > = { v5: [], anlasFixed: [], anlasPurchased: [] };
  for (const resource of ['v5', 'anlasFixed', 'anlasPurchased'] as const) {
    for (const [id, balance] of Object.entries(copy.balances)) {
      const field = `${resource}Available` as const;
      groups[resource].push({
        id: `member:${id}`,
        value: balance[field],
        assign: (amount) => {
          balance[field] = amount;
        },
      });
    }
    if (copy.operating) {
      groups[resource].push({
        id: 'operating',
        value: copy.operating.available[resource],
        assign: (amount) => {
          copy.operating!.available[resource] = amount;
        },
      });
    }
    for (const [jobId, charge] of Object.entries(copy.reservations)) {
      if (charge.status !== 'reserved') {
        continue;
      }
      if (resource === 'v5' && charge.resource === 'v5') {
        groups.v5.push({
          id: `reservation:${jobId}`,
          value: charge.amount,
          assign: (amount) => {
            charge.amount = amount;
          },
        });
      }
      if (resource !== 'v5' && charge.resource === 'anlas') {
        const field = resource === 'anlasFixed' ? 'fixed' : 'purchased';
        groups[resource].push({
          id: `reservation:${jobId}`,
          value: charge[field],
          assign: (amount) => {
            charge[field] = amount;
          },
        });
      }
    }
  }
  groups.v5.push({
    id: 'unallocated',
    value: copy.unallocatedV5 ?? '0',
    assign: (amount) => {
      copy.unallocatedV5 = amount;
    },
  });
  for (const resource of ['v5', 'anlasFixed', 'anlasPurchased'] as const) {
    const report = normalizeLegacyAmountGroup(groups[resource]);
    for (const entry of groups[resource]) {
      entry.assign(report.amounts[entry.id]);
    }
    reports.push({ resource, ...report });
  }
  upgradeGroups(copy);
  totals(copy);
  for (const charge of Object.values(copy.reservations)) {
    if (charge.resource === 'anlas') {
      charge.amount = formatAmount(units(charge.fixed) + units(charge.purchased));
    }
  }
  if (copy.operating) {
    operatingAccount(copy);
  }
  for (const reconciliation of [copy.v5Reconciliation, ...(copy.v5ReconciliationHistory || [])]) {
    if (!reconciliation) {
      continue;
    }
    for (const field of [
      'officialV5',
      'localAvailable',
      'sharedV5Debt',
      'unallocatedV5',
      'difference',
      'tolerance',
      'personalDelta',
    ] as const) {
      if (reconciliation[field] !== null) {
        reconciliation[field] = normalized(reconciliation[field]);
      }
    }
  }
  return copy;
}

function exactWeights(s: Pick<FairQuotaSyncState, 'groups'>): { id: string; weight: bigint }[] {
  const gcd = (left: bigint, right: bigint): bigint => {
    while (right) {
      const next = left % right;
      left = right;
      right = next;
    }
    return left;
  };
  const lcm = (left: bigint, right: bigint): bigint => (left / gcd(left, right)) * right;
  const ratios = s.groups.flatMap((group) => {
    const share = decimalRatio(group.share),
      members = group.members.map((id) => ({ id, ...decimalRatio(group.memberWeights[id] ?? 1) }));
    const denominator = members.reduce((value, member) => lcm(value, member.denominator), 1n);
    const total = members.reduce(
      (value, member) => value + member.numerator * (denominator / member.denominator),
      0n,
    );
    return members.map((member) => ({
      id: member.id,
      numerator: share.numerator * member.numerator * (denominator / member.denominator),
      denominator: share.denominator * (total || 1n),
    }));
  });
  const common = ratios.reduce((value, ratio) => lcm(value, ratio.denominator), 1n);
  return ratios.map((ratio) => ({
    id: ratio.id,
    weight: ratio.numerator * (common / ratio.denominator),
  }));
}
export function allocateFairGroupAmount(amount: bigint, group: FairGroup): Record<string, bigint> {
  return allocateAmount(amount, exactWeights({ groups: [{ ...group, share: 1 }] }));
}
const capacities = (s: FairQuotaSyncState): Record<string, bigint> =>
  allocateAmount(100n * AMOUNT_SCALE, exactWeights(s));
const recoveryUnits = (milliseconds: number, seconds: number): bigint => {
  const ratio = decimalRatio(seconds);
  return divideAmount(
    BigInt(milliseconds) * AMOUNT_SCALE * ratio.denominator,
    1000n * ratio.numerator,
    'floor',
  );
};
type QuotaFact = DomainEventFact<
  | 'AvailabilityChanged'
  | 'SharingConfigured'
  | 'AllocationChanged'
  | 'FundsTransferred'
  | 'MemberTransferred'
  | 'MemberRemoved'
  | 'AllocationApplied'
  | 'V5CapacityObserved'
  | 'AccountBound'
  | 'QuotaAccountOpened'
  | 'QuotaObserved'
  | 'RefillApplied'
  | 'QuotaReserved'
  | 'GenerationSettled'
  | 'GenerationRefunded'
>;
function quotaFact(
  db: { settings?: { fairQuota?: Pick<FairQuotaState, 'boundAccountId'> | null } },
  fact: QuotaFact,
  now?: number,
): void {
  const { eventType } = fact;
  const accountId = db.settings?.fairQuota?.boundAccountId;
  const sharing =
    [
      'SharingConfigured',
      'AllocationChanged',
      'MemberTransferred',
      'MemberRemoved',
      'AccountBound',
      'PolicyChanged',
    ].includes(eventType) ||
    (!accountId && eventType === 'AvailabilityChanged');
  if (!sharing && !accountId) {
    throw new Error(`Financial event ${eventType} requires a bound account`);
  }
  recordDomainEvent(db, {
    ...fact,
    streamId: sharing ? 'sharing:default' : `account:${accountId}`,
    occurredAt: now,
    accountId,
    policyVersion: 'personal_wallet_v1',
    amountVersion: 'decimal-12-v1',
  });
}
function quotaStatus(
  db: Parameters<typeof quotaFact>[0],
  s: FairQuotaSyncState,
  status: string,
  now: number,
): void {
  if (s.status === status) {
    return;
  }
  s.status = status;
  quotaFact(db, { eventType: 'AvailabilityChanged', payload: { kind: 'quota', status } }, now);
}
export function assertFairQuotaIdle(db: {
  settings?: { fairQuota?: Pick<FairQuotaSyncState, 'reservations'> | null };
  jobs?: FairQuotaDatabase['jobs'];
}): void {
  if (
    anyHeld(db.settings?.fairQuota || { reservations: {} }) ||
    rows(db.jobs).some((job) => ['queued', 'pending', 'running'].includes(job.status as string))
  ) {
    fail('有排队、运行或预留额度的任务，请结清后调整', 409);
  }
}

export function configureFairQuota<Reservation extends SyncReservation>(
  db: FairQuotaDatabase<Reservation>,
  memberIds: unknown,
): FairQuotaState<Reservation> {
  if (
    !Array.isArray(memberIds) ||
    memberIds.length !== 4 ||
    new Set(memberIds).size !== 4 ||
    memberIds.some((id) => typeof id !== 'string' || !id)
  ) {
    fail('公平分配需要四个不同的用户 ID', 400);
  }
  db.settings ||= {};
  if (db.settings.fairQuota) {
    if (JSON.stringify(db.settings.fairQuota.members) !== JSON.stringify(memberIds)) {
      fail('已有账本不能直接更换成员', 409);
    }
    return upgradeGroups(db.settings.fairQuota);
  }
  const s = (db.settings.fairQuota = upgradeGroups<Reservation>({
    version: 2,
    enabled: true,
    members: [...memberIds],
    boundAccountId: null,
    balances: Object.fromEntries(memberIds.map((id) => [id, emptyBalance()])),
    reservations: {},
    snapshot: null,
    spent: { v5: '0', anlas: '0', anlasFixed: '0', anlasPurchased: '0' },
    creditedV5: '0',
    lastRefillAt: 0,
    lastSettlementAt: 0,
    status: 'pending_opus',
  }));
  quotaFact(db, {
    eventType: 'SharingConfigured',
    payload: { members: s.members, groups: s.groups, configVersion: s.configVersion },
  });
  return s;
}

export function configureAnlasFallback(
  db: {
    settings: {
      fairQuota?: Pick<FairQuotaState, 'enabled' | 'autoAnlasFallback' | 'policyHistory'> | null;
    };
  },
  enabled: unknown,
  actorId: string,
  now = Date.now(),
) {
  if (typeof enabled !== 'boolean') {
    fail('enabled 必须为布尔值', 400);
  }
  const state = db.settings.fairQuota;
  if (!state?.enabled) {
    fail('共享账本尚未配置', 409);
  }
  state.autoAnlasFallback = enabled;
  state.policyHistory = [
    ...(state.policyHistory || []),
    { at: now, actor: 'administrator', autoAnlasFallback: enabled },
  ].slice(-1000);
  recordDomainEvent(db, {
    eventType: 'PolicyChanged',
    streamId: 'sharing:default',
    occurredAt: now,
    payload: {
      policy: 'autoAnlasFallback',
      value: enabled,
      actorId,
      actor: 'administrator',
      at: now,
    },
  });
  return { autoAnlasFallback: enabled };
}

export function normalizeFairGroups(
  groups: unknown,
  currentUsers: FairUser[],
  currentState?: Pick<FairQuotaState, 'groups' | 'members'> | null,
): FairGroup[] {
  if (!Array.isArray(groups) || groups.length !== 4) {
    fail('必须配置四个顶层共享组', 400);
  }
  const ids = new Set<string>(),
    members = new Set<string>(),
    users = currentUsers;
  const normalized = groups.map((g: GroupInput | null) => {
    if (
      !g ||
      typeof g.id !== 'string' ||
      !g.id.trim() ||
      ids.has(g.id) ||
      !Array.isArray(g.members)
    ) {
      fail('组 ID 必须唯一且成员必须为数组', 400);
    }
    ids.add(g.id);
    for (const id of g.members) {
      const user = users.find((u) => u.id === id);
      const oldState = currentState;
      const oldGroups =
        oldState?.groups ||
        oldState?.members?.map((member, index) => ({
          id: `group_${index + 1}`,
          members: [member],
        })) ||
        [];
      const remainsDisabled =
        user?.enabled === false &&
        oldGroups.some((group) => group.id === g.id && group.members.includes(id));
      // 停用只阻止登录与调用，不收回份额；已有停用成员留在原组时可继续管理其他成员。
      if (
        typeof id !== 'string' ||
        members.has(id) ||
        !user ||
        !(user.enabled === true || remainsDisabled)
      ) {
        fail('成员必须唯一且有效；停用成员只能保留在原组', 400);
      }
      members.add(id);
    }
    const share = g.share ?? 0.25;
    if (typeof share !== 'number' || !Number.isFinite(share) || share < 0 || share > 1) {
      fail('组份额必须介于 0 和 1', 400);
    }
    if (!g.members.length && share !== 0) {
      fail('空组份额必须为零', 400);
    }
    if (
      g.memberWeights !== undefined &&
      (!g.memberWeights || Array.isArray(g.memberWeights) || typeof g.memberWeights !== 'object')
    ) {
      fail('成员权重必须为对象', 400);
    }
    const memberWeights: Record<string, number> = Object.fromEntries(
      (g.members as string[]).map((id) => {
        const value = (g.memberWeights as Record<string, unknown> | undefined)?.[id] ?? 1;
        if (typeof value !== 'number' || !Number.isFinite(value) || value < 0 || value > 1000000) {
          fail('成员权重必须介于 0 和 1000000', 400);
        }
        return [id, value];
      }),
    );
    if (share > 0 && Object.values(memberWeights).reduce((n, value) => n + value, 0) <= 0) {
      fail('有份额的组至少需要一名正权重成员', 400);
    }
    return {
      share,
      memberWeights,
      id: g.id,
      name: typeof g.name === 'string' && g.name.trim() ? g.name.trim().slice(0, 80) : g.id,
      members: [...(g.members as string[])],
    };
  });
  if (!members.size) {
    fail('至少保留一名共享成员', 400);
  }
  if (Math.abs(normalized.reduce((n, g) => n + g.share, 0) - 1) > 1e-9) {
    fail('四个组份额总和必须为 100%', 400);
  }
  return normalized;
}

export function assertFairGroupMemberCount(groups: FairGroup[]): void {
  if (groups.reduce((total, group) => total + group.members.length, 0) > 100) {
    fail('共享成员总数不能超过 100', 400);
  }
}

function nextConfigurationVersion(version: number): number {
  if (!Number.isSafeInteger(version) || version < 1 || version >= Number.MAX_SAFE_INTEGER) {
    fail('分组配置版本已达安全边界', 409);
  }
  return version + 1;
}

export function assertFairGroupTransition(
  state: Pick<FairQuotaSyncState, 'balances'> & Partial<Pick<FairQuotaSyncState, 'groups'>>,
  groups: FairGroup[],
  redistributeRemaining: boolean,
): void {
  const members = new Set(groups.flatMap((group) => group.members)),
    positive = (balance: FairBalance) =>
      (['v5Available', 'anlasFixedAvailable', 'anlasPurchasedAvailable'] as const).some(
        (resource) => units(balance[resource]) > 0n,
      );
  if (
    Object.entries(state.balances).some(([id, balance]) => !members.has(id) && positive(balance))
  ) {
    fail('不能移除仍持有额度的成员，请先处理其余额', 409);
  }
  if (redistributeRemaining) {
    for (const group of groups) {
      for (const id of group.members) {
        const previous = state.groups?.find((old) => old.members.includes(id)),
          balance = state.balances[id];
        if (previous && previous.id !== group.id && balance && positive(balance)) {
          fail('有余额成员跨组移动与剩余额度重分必须分两次执行', 409);
        }
      }
    }
  }
}

// 仅由管理员接口调用。额度只存在于叶子钱包，组没有第二份可消费余额。
export function configureFairGroups<Reservation extends SyncReservation>(
  db: FairQuotaDatabase<Reservation>,
  groups: unknown,
  options: GroupOptions = {},
): FairQuotaState<Reservation> {
  const normalized = normalizeFairGroups(groups, rows(db.users), db.settings?.fairQuota),
    members = new Set(normalized.flatMap((group) => group.members));
  assertFairQuotaIdle(db);
  assertFairGroupMemberCount(normalized);
  if (Object.keys(options).some((k) => !['now', 'redistributeRemaining', 'actorId'].includes(k))) {
    fail('不支持的分组配置选项', 400);
  }
  if (
    options.redistributeRemaining !== undefined &&
    typeof options.redistributeRemaining !== 'boolean'
  ) {
    fail('redistributeRemaining 必须是布尔值', 400);
  }
  db.settings ||= {};
  const existing = db.settings.fairQuota ? structuredClone(db.settings.fairQuota) : null;
  if (existing && anyHeld(existing)) {
    fail('有任务正在预留额度，请结清后调整分组', 409);
  }
  if (existing) {
    assertFairGroupTransition(existing, normalized, false);
  }
  // 先按旧份额完成当前时刻的恢复，再切换后续分配比例。
  const now = options.now ?? Date.now();
  if (!Number.isFinite(now) || now < 0) {
    fail('配置时间无效', 400);
  }
  const draft = {
    ...db,
    ledger: [...(db.ledger || [])],
    settings: { ...db.settings, fairQuota: existing },
  };
  if (existing) {
    inheritDomainCommand(db, draft);
  }
  if (existing) {
    syncFairQuota(draft, now);
  }
  const s = existing || configureFairQuota(draft, ['__seed_1', '__seed_2', '__seed_3', '__seed_4']);
  upgradeGroups(s);
  if (existing) {
    assertFairGroupTransition(s, normalized, options.redistributeRemaining === true);
  }
  const before = existing ? structuredClone(s.groups) : [],
    beforeBalances = existing ? structuredClone(s.balances) : {};
  const balances = Object.fromEntries(
    [...members].map((id) => [id, s.balances[id] || emptyBalance()]),
  );
  const redistributions: { groupId: string; resource: Resource; allocations: QuotaAllocation[] }[] =
    [];
  if (options.redistributeRemaining === true) {
    // 显式重分只转移该组现有钱包总额，不增发余额，也不重分历史消费。
    for (const g of normalized) {
      for (const field of [
        'v5Available',
        'anlasFixedAvailable',
        'anlasPurchasedAvailable',
      ] as const) {
        const amount = g.members.reduce((total, id) => total + units(balances[id][field]), 0n);
        const allocation = allocateFairGroupAmount(amount, g);
        // 零份额组若全部权重为零，保留已有钱包，避免除零或静默抹去余额。
        if (g.members.some((id) => g.memberWeights[id] > 0)) {
          for (const id of g.members) {
            balances[id][field] = formatAmount(allocation[id]);
          }
          redistributions.push({
            groupId: g.id,
            resource: field.slice(0, -9) as Resource,
            allocations: g.members.map((memberId) => ({
              memberId,
              amount: formatAmount(allocation[memberId]),
            })),
          });
        }
      }
    }
  }
  s.groups = normalized;
  s.members = [...members];
  s.balances = balances;
  s.version = 2;
  s.configVersion = nextConfigurationVersion(s.configVersion);
  totals(s);
  s.configurationHistory.push({
    at: now,
    actorId: options.actorId || null,
    version: s.configVersion,
    before,
    after: structuredClone(normalized),
    beforeBalances,
    afterBalances: structuredClone(balances),
    redistributeRemaining: options.redistributeRemaining === true,
  });
  s.configurationHistory = s.configurationHistory.slice(-1000);
  totals(s);
  db.settings.fairQuota = s;
  db.ledger = draft.ledger;
  quotaFact(
    db,
    {
      eventType: existing ? 'AllocationChanged' : 'SharingConfigured',
      payload: {
        members: s.members,
        groups: s.groups,
        configVersion: s.configVersion,
        at: now,
        actorId: options.actorId || null,
        redistributeRemaining: options.redistributeRemaining === true,
      },
    },
    now,
  );
  if (s.boundAccountId) {
    quotaFact(
      db,
      {
        eventType: 'FundsTransferred',
        payload: {
          kind: 'redistribution',
          configVersion: s.configVersion,
          members: s.members,
          redistributions,
        },
      },
      now,
    );
  }
  return s;
}

// 管理调账始终在独立副本上执行，所有验证通过才提交。
export function manageFairMember(
  db: FairQuotaDatabase & { users: FairUser[] },
  body: MemberChange,
  now = Date.now(),
) {
  assertFairQuotaIdle(db);
  const draft = {
    ...db,
    ledger: [...(db.ledger || [])],
    users: structuredClone(db.users),
    settings: structuredClone(db.settings),
  };
  if (body.action !== 'preview-delete-member') {
    inheritDomainCommand(db, draft);
  }
  syncFairQuota(draft, now);
  return applyFairMemberChange(db, draft, body, now);
}

// 已同步的独立管理 view 与旧包装入口共用同一决定；摘要只需要原核对状态。
export function applyFairMemberChange(
  db: FairMemberQuotaDatabase & { users: FairUser[] },
  draft: FairMemberQuotaDatabase & { users: FairUser[] },
  body: MemberChange,
  now: number,
) {
  assertFairQuotaIdle(draft);
  const s = draft.settings?.fairQuota;
  if (!s?.enabled) {
    fail('共享账本尚未配置', 409);
  }
  if (
    !Number.isInteger(body.expectedConfigVersion) ||
    body.expectedConfigVersion !== s.configVersion
  ) {
    fail('配置已变化，请刷新后重新预览', 409);
  }
  const beforeBalances = structuredClone(s.balances),
    before = structuredClone(s.groups);
  const sourceId = body.action === 'transfer-quota' ? body.fromUserId : body.userId;
  const targetId = body.action === 'transfer-quota' ? body.toUserId : body.transferToUserId;
  const source = draft.users.find((u) => u.id === sourceId),
    target = draft.users.find((u) => u.id === targetId);
  if (!source) {
    fail('成员不存在', 404);
  }
  if (!target || target.id === source.id) {
    fail('请选择另一名成员接收余额', 400);
  }
  let autoJoined = null,
    shareChanges: { userId: string; beforeShare: number; afterShare: number }[] = [];
  const transferred: ResourceAmounts = { v5: '0', anlasFixed: '0', anlasPurchased: '0' };
  if (body.action === 'transfer-quota') {
    if (!s.members.includes(target.id) || !s.balances[target.id]) {
      fail('转账接收成员必须已加入共享分组', 400);
    }
    if (!s.members.includes(source.id) || !s.balances[source.id]) {
      fail('转出成员未加入共享分组', 400);
    }
    if (
      !Object.hasOwn(transferred, body.resource as string) ||
      typeof body.amount !== 'number' ||
      !Number.isFinite(body.amount) ||
      body.amount <= 0
    ) {
      fail('转账资源或数量无效', 400);
    }
    if (body.resource !== 'v5' && !Number.isInteger(body.amount)) {
      fail('Anlas 转账数量必须为整数', 400);
    }
    const field = (body.resource + 'Available') as BalanceField;
    let amount: bigint;
    try {
      amount = parseAmount(body.amount);
    } catch {
      fail('转账数量最多支持 12 位小数', 400);
    }
    if (units(s.balances[source.id][field]) < amount) {
      fail('转出成员余额不足', 409);
    }
    if (
      body.resource === 'v5' &&
      units(s.balances[target.id][field]) + amount > capacities(s)[target.id]
    ) {
      fail('接收成员 V5 超出容量，请先增加其份额或减少转账量', 409);
    }
    s.balances[source.id][field] = formatAmount(units(s.balances[source.id][field]) - amount);
    s.balances[target.id][field] = formatAmount(units(s.balances[target.id][field]) + amount);
    transferred[body.resource as Resource] = formatAmount(amount);
  } else {
    const originalShares = Object.fromEntries(s.members.map((id) => [id, weight(s, id)]));
    const originalGroup = groupFor(s, source.id);
    if (!s.members.includes(target.id)) {
      if (!originalGroup) {
        fail('删除未分组成员时，请选择已分组成员接收；双方均未分组无法继承份额', 400);
      }
      if (target.enabled !== true) {
        fail('未分组接收成员必须已启用，才能继承共享份额', 400);
      }
      // 直接替换原位置与权重，不先增加成员分摊，避免其他成员份额发生短暂或永久改变。
      const memberWeight = originalGroup.memberWeights[source.id] ?? 1;
      originalGroup.members = originalGroup.members.map((id) =>
        id === source.id ? target.id : id,
      );
      originalGroup.memberWeights[target.id] = memberWeight;
      delete originalGroup.memberWeights[source.id];
      s.members = s.members.map((id) => (id === source.id ? target.id : id));
      s.balances[target.id] ||= emptyBalance();
      autoJoined = {
        userId: target.id,
        groupId: originalGroup.id,
        memberWeight,
        share: weight(s, target.id),
      };
    }
    if (s.members.includes(source.id) && s.members.length <= 1) {
      fail('不能删除最后一名共享成员，请选择启用的未分组成员继承份额', 409);
    }
    for (const resource of Object.keys(transferred) as Resource[]) {
      const amount = units(s.balances[source.id]?.[(resource + 'Available') as BalanceField]);
      transferred[resource] = formatAmount(amount);
      s.balances[target.id][(resource + 'Available') as BalanceField] = formatAmount(
        units(s.balances[target.id][(resource + 'Available') as BalanceField]) + amount,
      );
    }
    // 删除接收的 V5 可以暂时超容量，恢复函数只在余额低于容量后继续入账。
    delete s.balances[source.id];
    s.members = s.members.filter((id) => id !== source.id);
    // 按删除前的全局份额精确转给指定接收者，不让组内其他成员被动分走份额。
    const finalShares = {
      ...originalShares,
      [source.id]: 0,
      [target.id]: (originalShares[target.id] || 0) + (originalShares[source.id] || 0),
    };
    s.groups = s.groups.map((g) => {
      const members = g.members.filter((id) => id !== source.id);
      return {
        ...g,
        members,
        share: members.reduce((n, id) => n + (finalShares[id] || 0), 0),
        memberWeights: Object.fromEntries(members.map((id) => [id, (finalShares[id] || 0) * 100])),
      };
    });
    shareChanges = [...new Set([...Object.keys(originalShares), source.id, target.id])].map(
      (userId) => ({
        userId,
        beforeShare: originalShares[userId] || 0,
        afterShare: finalShares[userId] || 0,
      }),
    );
    draft.users = draft.users.filter((u) => u.id !== source.id);
  }
  totals(s);
  s.configVersion = nextConfigurationVersion(s.configVersion);
  s.configurationHistory.push({
    at: now,
    actorId: 'administrator',
    action: body.action === 'transfer-quota' ? 'transfer-quota' : 'delete-member',
    version: s.configVersion,
    sourceId,
    targetId,
    transferred,
    autoJoined,
    shareChanges,
    before,
    after: structuredClone(s.groups),
    beforeBalances,
    afterBalances: structuredClone(s.balances),
  });
  s.configurationHistory = s.configurationHistory.slice(-1000);
  const result = {
    configVersion: s.configVersion,
    groups: s.groups,
    transferred: Object.fromEntries(
      Object.entries(transferred).map(([resource, amount]) => [
        resource,
        amountNumber(units(amount)),
      ]),
    ),
    autoJoined,
    shareChanges,
    users: draft.users.map((user) => ({ id: user.id, quota: fairQuotaSummary(draft, user, now) })),
    ...(body.action === 'transfer-quota'
      ? { fromUserId: sourceId, toUserId: targetId }
      : { deletedUserId: sourceId, transferToUserId: targetId }),
  };
  if (body.action === 'preview-delete-member') {
    result.configVersion = body.expectedConfigVersion;
  } else {
    db.settings = draft.settings;
    db.users = draft.users;
    db.ledger = draft.ledger;
    quotaFact(
      db,
      {
        eventType: body.action === 'transfer-quota' ? 'MemberTransferred' : 'MemberRemoved',
        payload: {
          sourceId: source.id,
          targetId: target.id,
          configVersion: s.configVersion,
          members: s.members,
          groups: s.groups,
          autoJoined,
          shareChanges,
          at: now,
        },
      },
      now,
    );
    if (s.boundAccountId) {
      quotaFact(
        db,
        {
          eventType: 'FundsTransferred',
          payload: {
            kind: body.action === 'transfer-quota' ? 'transfer' : 'member-removal',
            sourceId: source.id,
            targetId: target.id,
            transferred,
            configVersion: s.configVersion,
            members: s.members,
          },
        },
        now,
      );
    }
  }
  return result;
}

// 普通开账/注资按全部原份额分配；V5 容量截断后不改用恢复的未满额水位规则。
export function planFairQuotaAllocation(
  s: FairQuotaSyncState,
  resource: Resource,
  delta: bigint,
): { amount: bigint; allocations: QuotaAllocation[] } {
  const field = `${resource}Available` as const;
  if (delta >= 0n) {
    let actual = 0n;
    const allocation = allocateAmount(delta, exactWeights(s)),
      capacity = capacities(s);
    const allocations: QuotaAllocation[] = [];
    for (const id of s.members) {
      const b = s.balances[id];
      const add =
        resource === 'v5'
          ? minAmount(
              allocation[id] ?? 0n,
              maxAmount(0n, capacity[id] - held(s, id, 'v5') - units(b[field])),
            )
          : (allocation[id] ?? 0n);
      actual += add;
      if (add > 0n) {
        allocations.push({ memberId: id, amount: formatAmount(add) });
      }
    }
    return { amount: actual, allocations };
  }
  // 负差额没有成员归属证据，必须另记运营待承担，不能按份额削减钱包。
  return { amount: 0n, allocations: [] };
}

function distribute(
  db: Parameters<typeof quotaFact>[0],
  s: FairQuotaSyncState,
  resource: Resource,
  delta: bigint,
  now: number,
  reason: 'opening' | 'funding',
): bigint {
  const field = `${resource}Available` as const,
    { amount, allocations } = planFairQuotaAllocation(s, resource, delta);
  if (delta >= 0n) {
    const additions = new Map(allocations.map((entry) => [entry.memberId, units(entry.amount)]));
    for (const id of s.members) {
      const b = s.balances[id];
      b[field] = formatAmount(units(b[field]) + (additions.get(id) ?? 0n));
    }
    totals(s);
    if (allocations.length) {
      quotaFact(
        db,
        {
          eventType: 'AllocationApplied',
          payload: {
            resource,
            sourceAmount: formatAmount(delta),
            allocations,
            reason,
            configVersion: s.configVersion,
            policyVersion: 'weighted_allocation_v1',
            roundingVersion: 'decimal-12-v1',
          },
        },
        now,
      );
    }
  }
  return amount;
}

export function manageOperatingFunding(
  db: { settings?: { fairQuota?: FairQuotaSyncState | null }; ledger?: unknown[] },
  body: OperatingFundingInput,
  options: OperatingFundingContext,
  now = Date.now(),
) {
  const { memberFunding, ...result } = applyOperatingFunding(db, body, options, now);
  if (memberFunding) {
    distribute(
      db,
      db.settings!.fairQuota!,
      memberFunding.resource,
      units(memberFunding.amount),
      now,
      'funding',
    );
  }
  return result;
}

// 只让有份额且尚未满额的钱包参与恢复；在途预留同样占容量。
function v5RecoveryMembers(s: FairQuotaSyncState) {
  const capacity = capacities(s),
    weights = Object.fromEntries(exactWeights(s).map((entry) => [entry.id, entry.weight]));
  return s.members.flatMap((id) => {
    const share = weight(s, id),
      balance = s.balances[id];
    if (!(share > 0) || !balance) {
      return [];
    }
    const gap = maxAmount(0n, capacity[id] - held(s, id, 'v5') - units(balance.v5Available));
    if (gap === 0n) {
      return [];
    }
    // 停用只拦登录与调用；保留管理员已分配的份额。
    return [{ id, share, weight: weights[id], gap }];
  });
}

function distributeV5Recovery(
  s: FairQuotaSyncState,
  delta: bigint,
): { amount: bigint; allocations: QuotaAllocation[] } {
  let remaining = maxAmount(0n, delta),
    actual = 0n,
    participants = v5RecoveryMembers(s);
  const allocations: Record<string, bigint> = {};
  while (remaining > 0n && participants.length) {
    const allocation = allocateAmount(remaining, participants);
    let credited = 0n;
    for (const member of participants) {
      const add = minAmount(member.gap, allocation[member.id]);
      s.balances[member.id].v5Available = formatAmount(
        units(s.balances[member.id].v5Available) + add,
      );
      member.gap -= add;
      credited += add;
      allocations[member.id] = (allocations[member.id] ?? 0n) + add;
    }
    actual += credited;
    remaining = maxAmount(0n, remaining - credited);
    const unfilled = participants.filter((member) => member.gap > 0n);
    // 本轮有人满额时，把未用完的恢复继续分给其余人；没有人满额则预算已分完。
    if (unfilled.length === participants.length) {
      break;
    }
    participants = unfilled;
  }
  return {
    amount: actual,
    allocations: Object.entries(allocations)
      .filter(([, amount]) => amount > 0n)
      .map(([memberId, amount]) => ({ memberId, amount: formatAmount(amount) })),
  };
}

// 官方聚合差额只用于来源核对，不能据此向闲置成员扣款。
function recordV5Reconciliation(
  db: FairQuotaSyncDatabase,
  s: FairQuotaSyncState,
  incoming: QuotaSnapshot,
  now: number,
) {
  const ops = operatingAccount(s),
    localAvailable = sum(s, 'v5Available'),
    assigned = maxAmount(0n, units(incoming.v5) - units(s.unallocatedV5));
  const difference =
    assigned - maxAmount(0n, localAvailable + units(ops.available.v5) - units(s.sharedV5Debt));
  const tolerance = units(incoming.v5) % AMOUNT_SCALE === 0n ? AMOUNT_SCALE : 0n;
  let status: V5Reconciliation['status'] = 'within_precision';
  if (difference < -tolerance) {
    status = 'source_below_local';
  } else if (difference > tolerance) {
    status = 'source_above_local';
  }
  const entry: V5Reconciliation = {
    at: now,
    sourceCheckedAt: incoming.at,
    officialV5: incoming.v5,
    localAvailable: formatAmount(localAvailable),
    sharedV5Debt: s.sharedV5Debt,
    unallocatedV5: s.unallocatedV5,
    difference: formatAmount(difference),
    tolerance: formatAmount(tolerance),
    status,
    personalDelta: '0',
    policy: 'personal_wallet_v1',
  };
  s.v5Reconciliation = entry;
  // 整数官方快照有一个百分点的精度边界；只记录该边界之外可支持的容量差额。
  const evidence = reconcileV5Capacity(db, maxAmount(0n, -difference - tolerance), incoming, now);
  const history = (s.v5ReconciliationHistory ||= []),
    previous = history.at(-1);
  // 同一差额不按每分钟刷屏；跨显示格或恢复正常时留痕，审计不包含成员身份与请求内容。
  const absolute = (value: bigint) => (value < 0n ? -value : value);
  const changed =
    previous?.status !== status ||
    previous?.officialV5 !== incoming.v5 ||
    absolute(units(previous?.difference)) / AMOUNT_SCALE !== absolute(difference) / AMOUNT_SCALE;
  const recordHistory = Boolean(
    (status !== 'within_precision' && changed) ||
    (status === 'within_precision' && previous && previous.status !== status),
  );
  if (recordHistory) {
    history.push(entry);
    s.v5ReconciliationHistory = history.slice(-200);
  }
  quotaFact(
    db,
    {
      eventType: 'V5CapacityObserved',
      payload: {
        capacityGap: ops.v5CapacityGap,
        gapEventId: ops.v5GapEventId,
        reconciliation: entry,
        recordHistory,
        evidence: evidence ? [evidence] : [],
      },
    },
    now,
  );
}

export function fairQuotaSnapshot(a: FairAccount): QuotaSnapshot {
  const percent = number(a.v5UsagePercent),
    fixed = number(a.quotaFixed),
    purchased = number(a.quotaPurchased);
  const fixedUnits = fixed === null ? null : maxAmount(0n, units(a.quotaFixed as AmountInput)),
    purchasedUnits =
      purchased === null ? null : maxAmount(0n, units(a.quotaPurchased as AmountInput));
  const at = stamp(a.quotaCheckedAt);
  let v5: AmountText | null = null;
  if (percent !== null) {
    if (a.v5UsageIsNegative) {
      v5 = '0';
    } else {
      v5 = formatAmount(
        maxAmount(0n, minAmount(100n * AMOUNT_SCALE, units(a.v5UsagePercent as AmountInput))),
      );
    }
  }
  return {
    at,
    v5,
    anlas:
      fixedUnits === null || purchasedUnits === null
        ? null
        : formatAmount(fixedUnits + purchasedUnits),
    anlasFixed: fixedUnits === null ? null : formatAmount(fixedUnits),
    anlasPurchased: purchasedUnits === null ? null : formatAmount(purchasedUnits),
    seconds: number(a.v5UsageTimeUntilNextPercent),
  };
}

// 官方快照是资格与来源对账依据，成员钱包独立保留；查询失败不能抹掉已分配权益。
export function syncFairQuota<Reservation extends SyncReservation>(
  db: FairQuotaDatabase<Reservation>,
  now = Date.now(),
): FairQuotaState<Reservation> | null {
  const s = db.settings?.fairQuota;
  if (!s?.enabled) {
    return s || null;
  }
  upgradeGroups(s);
  const accounts = rows(db.accounts);
  if (!s.boundAccountId) {
    const eligible = accounts.filter((a) => active(a, now));
    if (eligible.length !== 1) {
      quotaStatus(db, s, eligible.length ? 'ambiguous_opus' : 'pending_opus', now);
      return s;
    }
    s.boundAccountId = eligible[0].id;
    quotaFact(db, { eventType: 'AccountBound', payload: { accountId: s.boundAccountId } }, now);
    quotaFact(db, { eventType: 'QuotaAccountOpened', payload: { memberIds: s.members } }, now);
  }
  const account = accounts.find((a) => a.id === s.boundAccountId);
  syncBoundFairQuota(db as FairQuotaSyncDatabase, account, now);
  return s;
}

// 单一已绑定账户的观察/恢复规则；旧运行时和关系命令使用同一个 owner。
export function syncBoundFairQuota(
  db: FairQuotaSyncDatabase,
  account: FairAccount | undefined,
  now = Date.now(),
): FairQuotaSyncState {
  const s = db.settings.fairQuota;
  if (!s.enabled) {
    return s;
  }
  const status = quotaAvailability(account, now, {
    at: s.snapshot?.at,
    held: anyHeld(s),
    lastSettlementAt: s.lastSettlementAt,
  });
  quotaStatus(db, s, status, now);
  if (status !== 'active' || !account) {
    return s;
  }
  const incoming = fairQuotaSnapshot(account);
  if (!s.snapshot) {
    s.snapshot = incoming;
    s.lastRefillAt = now;
    quotaFact(
      db,
      {
        eventType: 'QuotaObserved',
        payload: {
          kind: 'reconciled',
          snapshot: incoming,
          initial: true,
          at: now,
          matchedDebits: { anlasFixed: '0', anlasPurchased: '0' },
          matchedFallback: { fixed: '0', purchased: '0' },
        },
      },
      now,
    );
    distribute(
      db,
      s,
      'v5',
      maxAmount(0n, units(incoming.v5) - units(s.unallocatedV5)),
      now,
      'opening',
    );
    distribute(db, s, 'anlasFixed', units(incoming.anlasFixed), now, 'opening');
    distribute(db, s, 'anlasPurchased', units(incoming.anlasPurchased), now, 'opening');
    recordV5Reconciliation(db, s, incoming, now);
    return s;
  }
  const old = s.snapshot;
  const acceptingObservation =
    incoming.at > old.at &&
    incoming.at >= s.lastSettlementAt &&
    !anyHeld(s) &&
    incoming.v5 !== null &&
    incoming.anlas !== null;
  // 官方字段表示恢复完整 1% 的秒数。旧快照超过十分钟后不再推算恢复。
  const until = Math.min(now, old.at + STALE_MS);
  const from = s.lastRefillAt;
  let refill: RefillFact = {
    from,
    until: Math.max(from, until),
    snapshotAt: old.at,
    seconds: old.seconds,
    budget: '0',
    debtRepaid: '0',
    capacityRecovered: '0',
    allocations: [],
    configVersion: s.configVersion,
    policyVersion: 'unfilled_members_v1',
    roundingVersion: 'decimal-12-v1',
  };
  if (old.seconds! > 0 && until > s.lastRefillAt) {
    const elapsed =
      recoveryUnits(Math.max(0, until - old.at), old.seconds!) -
      recoveryUnits(Math.max(0, s.lastRefillAt - old.at), old.seconds!);
    const projected = maxAmount(
      0n,
      minAmount(
        100n * AMOUNT_SCALE,
        units(old.v5) + recoveryUnits(Math.max(0, until - old.at), old.seconds!),
      ) - units(s.unallocatedV5),
    );
    // 本地转扣可能仍消耗共享 V5。先用恢复偿还此估算负债，不能重复发放给个人。
    const repaid = minAmount(units(s.sharedV5Debt), elapsed);
    s.sharedV5Debt = formatAmount(units(s.sharedV5Debt) - repaid);
    const recovery = recoverV5Capacity(db, elapsed - repaid, s.lastRefillAt, until, now),
      recovered = recovery.amount;
    const reserves = sharedHeld(s);
    const room = maxAmount(
      0n,
      projected -
        units(s.spent.v5) -
        reserves -
        sum(s, 'v5Available') -
        units(operatingAccount(s).available.v5),
    );
    const credited = distributeV5Recovery(s, minAmount(elapsed - repaid - recovered, room));
    s.creditedV5 = formatAmount(units(s.creditedV5) + credited.amount);
    refill = {
      ...refill,
      budget: formatAmount(elapsed),
      debtRepaid: formatAmount(repaid),
      capacityRecovered: formatAmount(recovered),
      allocations: credited.allocations,
      evidence: recovery.evidence,
    };
  }
  s.lastRefillAt = Math.max(s.lastRefillAt, until);
  // 同批新观测已保存最终游标；无金融效果时不重复追加恢复事实。
  if (
    s.lastRefillAt > from &&
    (!acceptingObservation ||
      refill.allocations.length ||
      refill.debtRepaid !== '0' ||
      refill.capacityRecovered !== '0')
  ) {
    quotaFact(db, { eventType: 'RefillApplied', payload: { ...refill } }, now);
  }
  // 并发生成期间的快照无法归属消费，待全部结清且取得新快照后校准。
  if (acceptingObservation) {
    // 快照用于来源核对和安全派发，不把未知消费分摊给闲置成员，也不把差额当恢复增发。
    recordV5Reconciliation(db, s, incoming, now);
    const matchedDebits = { anlasFixed: '0', anlasPurchased: '0' },
      matchedFallback = { fixed: '0', purchased: '0' };
    for (const [resource, pool] of [
      ['anlasFixed', 'fixed'],
      ['anlasPurchased', 'purchased'],
    ] as const) {
      // 未显示的普通扣款跨快照保留；余额不变不能证明充值抵消了本次消费。
      let delta = units(incoming[resource]) - units(old[resource]);
      const consumed = minAmount(units(s.spent[resource]), maxAmount(0n, -delta));
      s.spent[resource] = formatAmount(units(s.spent[resource]) - consumed);
      delta += consumed;
      matchedDebits[resource] = formatAmount(consumed);
      if (delta < 0n) {
        // 抵消凭据跨相同快照及重启保留，也吸收随后才出现的官方扣款，避免双扣。
        const matched = minAmount(units(s.fallbackBuffer[pool]), -delta);
        s.fallbackBuffer[pool] = formatAmount(units(s.fallbackBuffer[pool]) - matched);
        delta += matched;
        matchedFallback[pool] = formatAmount(matched);
      }
      if (delta < 0n) {
        recordAnlasGap(db, resource, -delta, old, incoming, now);
      } else if (delta > 0n) {
        distribute(
          db,
          s,
          resource,
          matchExternalFunding(db, resource, delta, incoming, now),
          now,
          'funding',
        );
      }
    }
    s.snapshot = incoming;
    s.spent = {
      ...s.spent,
      v5: '0',
      anlas: formatAmount(units(s.spent.anlasFixed) + units(s.spent.anlasPurchased)),
      fallbackFixed: '0',
      fallbackPurchased: '0',
      fallbackV5: '0',
    };
    s.creditedV5 = '0';
    s.lastRefillAt = now;
    quotaFact(
      db,
      {
        eventType: 'QuotaObserved',
        payload: {
          kind: 'reconciled',
          snapshot: incoming,
          initial: false,
          at: now,
          matchedDebits,
          matchedFallback,
        },
      },
      now,
    );
  }
  return s;
}

export function quoteFairRequest(
  request: FairRequest = {},
  options: QuoteOptions = {},
): ReturnType<typeof quoteNativeRequest> {
  return quoteNativeRequest(request, options);
}

// 冷凭据是已提交的终态；job.fairCharge 只作归属校验/展示，不能重建可扣退的预留。
export function resolveFairQuotaCharge(
  db: FairQuotaDatabase,
  job: AccountingJob,
  settledCharge: FairCharge | null = null,
  userId?: string,
): FairCharge | null {
  const charge = settledCharge || db.settings?.fairQuota?.reservations?.[job.id];
  if (!charge) {
    return null;
  }
  const owner = job.userToken
    ? db.users?.find((user) => user.token === job.userToken)?.id
    : undefined;
  const owners = [userId, owner, job.fairCharge?.userId, job.generationAttempt?.memberId].filter(
    (id) => id !== undefined && id !== null,
  );
  if (owners.some((id) => id !== charge.userId)) {
    fail('任务账本归属不符', 409);
  }
  return charge;
}

function projectedV5(snapshot: QuotaSnapshot, now: number): bigint | null {
  if (snapshot.v5 === null) {
    return null;
  }
  const observed = units(snapshot.v5);
  const recovered =
    snapshot.seconds! > 0 ? recoveryUnits(Math.max(0, now - snapshot.at), snapshot.seconds!) : 0n;
  return minAmount(100n * AMOUNT_SCALE, observed + recovered);
}

export function decideFairQuotaCharge(
  balance: FairQuotaReservationState['balance'] | undefined,
  autoAnlasFallback: unknown,
  snapshot: QuotaSnapshot | null,
  request: FairRequest,
  now: number,
) {
  let quote = quoteFairRequest(request),
    amount = parseAmount(quote.amount, quote.resource === 'v5' ? 'ceil' : 'exact');
  let v5Equivalent = 0n,
    sharedV5Reserved = 0n,
    fallback = false;
  if (
    quote.resource === 'v5' &&
    autoAnlasFallback === true &&
    units(balance?.v5Available) < amount
  ) {
    fallback = true;
    v5Equivalent = amount;
    quote = quoteFairRequest(request, { forceAnlas: true });
    amount = parseAmount(quote.amount);
    sharedV5Reserved = snapshot && (projectedV5(snapshot, now) ?? 0n) > 0n ? v5Equivalent : 0n;
  }
  const fixed =
    quote.resource === 'anlas' && snapshot?.anlasFixed !== '0'
      ? minAmount(amount, units(balance?.anlasFixedAvailable))
      : 0n;
  const purchased = quote.resource === 'anlas' ? amount - fixed : 0n;
  return {
    ...quote,
    amount: formatAmount(amount),
    fixed: formatAmount(fixed),
    purchased: formatAmount(purchased),
    ...(fallback
      ? {
          billingMode: 'anlas_fallback' as const,
          v5Equivalent: formatAmount(v5Equivalent),
          sharedV5Reserved: formatAmount(sharedV5Reserved),
          upstreamBilling: 'shared_or_anlas' as const,
        }
      : {}),
  };
}

export function previewFairQuotaCharge(
  db: FairQuotaDatabase,
  user: Pick<FairUser, 'id'>,
  request: FairRequest = {},
  now = Date.now(),
) {
  const draft = estimateFairQuota(db, now),
    s = draft.settings?.fairQuota;
  const account = rows(draft.accounts).find((entry) => entry.id === s?.boundAccountId);
  return decideFairQuotaCharge(
    s?.members.includes(user.id) ? s.balances[user.id] : undefined,
    s?.autoAnlasFallback,
    account ? fairQuotaSnapshot(account) : null,
    request,
    now,
  );
}

export interface FairQuotaReservationState {
  balance: Pick<FairBalance, 'v5Available' | 'anlasFixedAvailable' | 'anlasPurchasedAvailable'>;
  autoAnlasFallback?: unknown;
  snapshot: QuotaSnapshot;
  availableV5: AmountText;
  reserved: ResourceAmounts;
  reservedAnlas: AmountText;
  sharedReservedV5: AmountText;
  pending: ResourceAmounts;
  pendingAnlas: AmountText;
  pendingFallback: ResourceAmounts;
  sharedV5Debt: AmountText;
}

// 新旧存储共用同一收费决定；输入只是当前钱包/观察/活动预留，不修改账本。
export function planFairQuotaReservation(
  state: FairQuotaReservationState,
  request: FairRequest = {},
  now = Date.now(),
) {
  const q = decideFairQuotaCharge(
      state.balance,
      state.autoAnlasFallback,
      state.snapshot,
      request,
      now,
    ),
    amount = units(q.amount),
    fixed = units(q.fixed),
    purchased = units(q.purchased);
  if (q.resource === 'anlas') {
    // 官方固定已明确耗尽时，本地保留的固定权益不能挡住原已授权的纯购买池调用。
    const officialFixed = state.snapshot.anlasFixed;
    if (purchased > 0n && request?.allowPurchasedAnlas !== true) {
      fail(
        `${officialFixed === '0' ? '官方固定 Anlas 已耗尽' : '个人固定 Anlas 不足'}；未经明确授权不会扣购买 Anlas`,
      );
    }
    if (purchased > units(state.balance.anlasPurchasedAvailable)) {
      fail('个人购买 Anlas 不足');
    }
    if (purchased > 0n) {
      // 上游优先使用固定余额，不能指定购买池；禁止混用导致其他成员被扣款。
      const official = state.snapshot,
        fixedReserved = units(state.reserved.anlasFixed);
      if (
        fixed > 0n ||
        official.anlasFixed !== '0' ||
        fixedReserved > 0n ||
        units(state.pending.anlasFixed) > 0n ||
        units(state.pendingFallback.anlasFixed) > 0n
      ) {
        fail(
          '暂不能使用购买 Anlas：请等待官方固定余额完全耗尽并完成对账；不支持混合固定与购买余额',
        );
      }
    }
  }
  if (q.resource !== 'free') {
    const available =
      q.resource === 'v5'
        ? units(state.balance.v5Available)
        : units(state.balance.anlasFixedAvailable) + units(state.balance.anlasPurchasedAvailable);
    if (available < amount) {
      fail(
        q.resource === 'v5'
          ? '个人 V5 恢复额度不足，请等待恢复；不会自动扣 Anlas'
          : '个人 Anlas 不足',
      );
    }
    if (
      q.resource === 'v5' &&
      units(state.availableV5) -
        units(state.sharedV5Debt) -
        units(state.sharedReservedV5) +
        units(state.reserved.v5) -
        amount <
        AMOUNT_SCALE
    ) {
      fail('V5 公共额度已达到安全保留边界，请等待恢复');
    }
    // 未结清并发会延后账本校准，但新快照下降必须立即限制派发。
    const latest = state.snapshot;
    const reserved =
      q.resource === 'v5' ? units(state.sharedReservedV5) : units(state.reservedAnlas);
    let projected: bigint | null;
    if (q.resource === 'v5') {
      projected = projectedV5(latest, now);
    } else {
      projected = latest.anlas === null ? null : units(latest.anlas);
    }
    const margin = q.resource === 'v5' ? AMOUNT_SCALE : 0n;
    const fallbackSpent =
      q.resource === 'v5'
        ? units(state.pendingFallback.v5)
        : units(state.pendingFallback.anlasFixed) + units(state.pendingFallback.anlasPurchased);
    if (
      projected === null ||
      projected -
        reserved -
        (q.resource === 'v5' ? units(state.pending.v5) : units(state.pendingAnlas)) -
        fallbackSpent -
        amount <
        margin
    ) {
      fail('官方公共额度不足或等待并发对账，请稍后重试');
    }
    if (q.resource === 'anlas') {
      for (const [resource, amount] of [
        ['anlasFixed', fixed],
        ['anlasPurchased', purchased],
      ] as const) {
        const globalHeld = units(state.reserved[resource]);
        const pendingFallback = units(
          resource === 'anlasFixed'
            ? state.pendingFallback.anlasFixed
            : state.pendingFallback.anlasPurchased,
        );
        if (
          units(latest[resource]) - globalHeld - units(state.pending[resource]) - pendingFallback <
          amount
        ) {
          fail('官方 Anlas 子余额不足或等待并发对账');
        }
      }
    }
  }
  return q;
}

// 报价模拟整批预扣；只推进瞬时窄状态，不写钱包、预留或尝试。
export function previewFairQuotaReservations(
  state: FairQuotaReservationState,
  requests: FairRequest[],
  now = Date.now(),
) {
  const draft = structuredClone(state);
  return requests.map((request) => {
    const charge = planFairQuotaReservation(draft, request, now);
    advanceFairQuotaReservation(draft, charge);
    return charge;
  });
}

// 报价独立副本与整批受理共用同一瞬时预留推进，真实钱包仍由命令提交。
export function advanceFairQuotaReservation(
  state: FairQuotaReservationState,
  charge: ReturnType<typeof planFairQuotaReservation>,
): void {
  if (charge.resource === 'v5') {
    state.balance.v5Available = formatAmount(
      units(state.balance.v5Available) - units(charge.amount),
    );
    state.availableV5 = formatAmount(units(state.availableV5) - units(charge.amount));
    state.reserved.v5 = formatAmount(units(state.reserved.v5) + units(charge.amount));
  } else if (charge.resource === 'anlas') {
    state.balance.anlasFixedAvailable = formatAmount(
      units(state.balance.anlasFixedAvailable) - units(charge.fixed),
    );
    state.balance.anlasPurchasedAvailable = formatAmount(
      units(state.balance.anlasPurchasedAvailable) - units(charge.purchased),
    );
    state.reserved.anlasFixed = formatAmount(
      units(state.reserved.anlasFixed) + units(charge.fixed),
    );
    state.reserved.anlasPurchased = formatAmount(
      units(state.reserved.anlasPurchased) + units(charge.purchased),
    );
    state.reservedAnlas = formatAmount(units(state.reservedAnlas) + units(charge.amount));
  }
  state.sharedReservedV5 = formatAmount(
    units(state.sharedReservedV5) +
      units(charge.sharedV5Reserved) +
      (charge.resource === 'v5' ? units(charge.amount) : 0n),
  );
}

// 调用方持有写事务：校验来源和授权后扣成员可用钱包，并一同记录预留事实。
export function reserveFairQuota(
  db: FairQuotaDatabase,
  user: FairUser,
  job: AccountingJob,
  account: FairAccount,
  now = Date.now(),
  request = job.request || job.requestPayload,
  settledCharge: FairCharge | null = null,
  authorizeCharge?: (charge: FairCharge) => void,
): FairCharge | null {
  if (!job.id) {
    fail('任务缺少持久化 ID', 500);
  }
  const previous = resolveFairQuotaCharge(db, job, settledCharge, user.id);
  if (previous && previous.status !== 'reserved') {
    fail(previous.status === 'refunded' ? '已退款任务不能重新提交' : '已结算任务不能重新提交', 409);
  }
  const s = syncFairQuota(db, now);
  if (!s?.enabled) {
    return null;
  }
  if (!s.members.includes(user.id)) {
    fail('该用户未加入共享分组账本', 403);
  }
  if (s.status !== 'active') {
    fail(`共享 Opus 额度暂不可用：${s.status}`, 503);
  }
  if (account.id !== s.boundAccountId) {
    fail('共享账本只能使用绑定的 Opus 账号', 503);
  }
  if (previous) {
    if (previous.accountId !== account.id) {
      fail('任务账本归属不符', 409);
    }
    authorizeCharge?.(previous);
    job.fairCharge = previous;
    return previous;
  }
  const q = planFairQuotaReservation(
      {
        balance: s.balances[user.id],
        autoAnlasFallback: s.autoAnlasFallback,
        snapshot: fairQuotaSnapshot(account),
        availableV5: formatAmount(sum(s, 'v5Available')),
        reserved: {
          v5: formatAmount(s.members.reduce((n, id) => n + held(s, id, 'v5'), 0n)),
          anlasFixed: formatAmount(s.members.reduce((n, id) => n + held(s, id, 'anlasFixed'), 0n)),
          anlasPurchased: formatAmount(
            s.members.reduce((n, id) => n + held(s, id, 'anlasPurchased'), 0n),
          ),
        },
        sharedReservedV5: formatAmount(sharedHeld(s)),
        reservedAnlas: formatAmount(s.members.reduce((n, id) => n + held(s, id, 'anlas'), 0n)),
        pending: {
          v5: s.spent.v5,
          anlasFixed: s.spent.anlasFixed,
          anlasPurchased: s.spent.anlasPurchased,
        },
        pendingFallback: {
          v5: s.spent.fallbackV5,
          anlasFixed: s.spent.fallbackFixed,
          anlasPurchased: s.spent.fallbackPurchased,
        },
        pendingAnlas: s.spent.anlas,
        sharedV5Debt: s.sharedV5Debt,
      },
      request || {},
      now,
    ),
    amount = units(q.amount),
    fixed = units(q.fixed),
    purchased = units(q.purchased);
  const charge = {
    ...q,
    configVersion: s.configVersion,
    groupId: groupFor(s, user.id)?.id,
    userId: user.id,
    accountId: account.id,
    status: 'reserved',
    reservedAt: now,
  } as FairCharge;
  authorizeCharge?.(charge);
  if (q.resource === 'anlas') {
    s.balances[user.id].anlasFixedAvailable = formatAmount(
      units(s.balances[user.id].anlasFixedAvailable) - fixed,
    );
    s.balances[user.id].anlasPurchasedAvailable = formatAmount(
      units(s.balances[user.id].anlasPurchasedAvailable) - purchased,
    );
    totals(s);
  } else if (q.resource === 'v5') {
    s.balances[user.id].v5Available = formatAmount(units(s.balances[user.id].v5Available) - amount);
  }
  s.reservations[job.id] = charge;
  job.fairCharge = charge;
  recordGenerationAttempt(db, job, charge, fairQuotaSnapshot(account), now);
  quotaFact(
    db,
    {
      eventType: 'QuotaReserved',
      payload: { jobId: job.id, charge, at: now, pricingVersion: NATIVE_PRICING_VERSION },
    },
    now,
  );
  return charge;
}

// 与任务终态同事务提交：成功进入待对账消耗，失败退回原钱包；已终结的预留不再次变账。
export function settleFairQuota(
  db: FairQuotaDatabase,
  job: AccountingJob,
  success: boolean,
  now = Date.now(),
  evidence: RefundEvidence = {},
  settledCharge: FairCharge | null = null,
): FairCharge | null {
  const s = db.settings?.fairQuota!,
    c = resolveFairQuotaCharge(db, job, settledCharge);
  if (!c) {
    return null;
  }
  job.fairCharge = c;
  if (c.status !== 'reserved') {
    return c;
  }
  upgradeGroups(s);
  normalizeCharge(c);
  c.status = success ? 'settled' : 'refunded';
  c.settledAt = now;
  if (c.resource !== 'free') {
    if (success) {
      if (c.billingMode === 'anlas_fallback') {
        s.spent.fallbackFixed = formatAmount(units(s.spent.fallbackFixed) + units(c.fixed));
        s.spent.fallbackPurchased = formatAmount(
          units(s.spent.fallbackPurchased) + units(c.purchased),
        );
        s.spent.fallbackV5 = formatAmount(units(s.spent.fallbackV5) + units(c.sharedV5Reserved));
        s.fallbackBuffer.fixed = formatAmount(units(s.fallbackBuffer.fixed) + units(c.fixed));
        s.fallbackBuffer.purchased = formatAmount(
          units(s.fallbackBuffer.purchased) + units(c.purchased),
        );
        s.sharedV5Debt = formatAmount(units(s.sharedV5Debt) + units(c.sharedV5Reserved));
        const total = (s.fallbackTotalsByUser[c.userId] ||= {
          fixed: '0',
          purchased: '0',
          v5Equivalent: '0',
        });
        total.fixed = formatAmount(units(total.fixed) + units(c.fixed));
        total.purchased = formatAmount(units(total.purchased) + units(c.purchased));
        total.v5Equivalent = formatAmount(units(total.v5Equivalent) + units(c.v5Equivalent));
      } else {
        s.spent[c.resource] = formatAmount(units(s.spent[c.resource]) + units(c.amount));
        if (c.resource === 'anlas') {
          s.spent.anlasFixed = formatAmount(units(s.spent.anlasFixed) + units(c.fixed));
          s.spent.anlasPurchased = formatAmount(units(s.spent.anlasPurchased) + units(c.purchased));
        }
      }
    } else if (c.resource === 'anlas') {
      s.balances[c.userId].anlasFixedAvailable = formatAmount(
        units(s.balances[c.userId].anlasFixedAvailable) + units(c.fixed),
      );
      s.balances[c.userId].anlasPurchasedAvailable = formatAmount(
        units(s.balances[c.userId].anlasPurchasedAvailable) + units(c.purchased),
      );
      totals(s);
    } else {
      s.balances[c.userId].v5Available = formatAmount(
        units(s.balances[c.userId].v5Available) + units(c.amount),
      );
    }
  }
  s.lastSettlementAt = Math.max(s.lastSettlementAt, now);
  job.fairCharge = c;
  // reserved 状态迁移是持久化幂等边界，不依赖只有最近 1000 条的 ledger 内存缓存。
  const refundEvidence = !success ? recordGenerationRefund(db, job, c, now, evidence) : null;
  quotaFact(
    db,
    {
      eventType: success ? 'GenerationSettled' : 'GenerationRefunded',
      payload: {
        jobId: job.id,
        charge: c,
        at: now,
        ...(refundEvidence ? { evidence: [refundEvidence] } : {}),
      },
    },
    now,
  );
  return c;
}

export function estimateFairQuota<T extends FairQuotaDatabase>(db: T, now = Date.now()): T {
  const draft = { ...db, settings: structuredClone(db.settings), ledger: [] };
  syncFairQuota(draft, now);
  return draft;
}

export function fairQuotaSummary(
  db: { settings?: { fairQuota?: FairQuotaSummaryState | null } },
  user: Pick<FairUser, 'id'>,
  now = Date.now(),
) {
  const s = db.settings?.fairQuota;
  if (!s?.enabled) {
    return null;
  }
  const raw = s.members.includes(user.id) ? s.balances[user.id] : undefined,
    b = raw,
    group = groupFor(s, user.id),
    share = weight(s, user.id);
  const localFallback = s.fallbackTotalsByUser[user.id] || {
    fixed: '0',
    purchased: '0',
    v5Equivalent: '0',
  };
  const capacity = amountNumber(capacities(s)[user.id] ?? 0n),
    available = amountNumber(units(b?.v5Available)),
    reserved = b ? amountNumber(held(s, user.id, 'v5')) : 0;
  const officialRate = s.snapshot?.seconds! > 0 ? 1 / s.snapshot!.seconds! : 0;
  const participants = v5RecoveryMembers(s),
    recoveringShare = participants.reduce((total, member) => total + member.share, 0);
  const participant = participants.find((member) => member.id === user.id);
  const refillRate = participant ? (officialRate * share) / recoveringShare : 0;
  const refillRoom = Math.max(0, capacity - available - reserved);
  // 按其他成员先后满额的时刻计算：目标补满前，整车需要恢复各成员实际能接收的总量。
  const refillBudget = participant
    ? participants.reduce(
        (total, member) =>
          total +
          Math.min(
            amountNumber(member.gap),
            (member.share * amountNumber(participant.gap)) / participant.share,
          ),
        0,
      )
    : 0;
  const fullSeconds = officialRate > 0 ? refillBudget / officialRate : null;
  // 当前来源刚好与钱包相等时仍能随时间恢复；只有最终来源容量不够才不能承诺补满。
  const ops = readOperatingAccount(s);
  const sourceCapacityRoom = amountNumber(
    maxAmount(
      0n,
      100n * AMOUNT_SCALE -
        units(s.unallocatedV5) -
        units(s.spent.v5) -
        sharedHeld(s) -
        sum(s, 'v5Available') -
        units(ops.available.v5),
    ),
  );
  const refillLimited =
    s.status !== 'active' ||
    units(s.sharedV5Debt) > 0n ||
    s.v5Reconciliation?.status === 'source_below_local' ||
    (refillRoom > 1e-9 && (!refillRate || refillBudget > sourceCapacityRoom + 1e-9));
  const sharedBalanceRisk = {
    v5: units(ops.v5CapacityGap) > 0n,
    anlasFixed: units(ops.unfunded.anlasFixed) > 0n,
    anlasPurchased: units(ops.unfunded.anlasPurchased) > 0n,
    pendingAnlasDebit: units(s.spent.anlas) > 0n,
    checkedAt: s.snapshot?.at ?? null,
  };
  let v5FullRefillSeconds: number | null;
  if (refillRoom <= 1e-9) {
    v5FullRefillSeconds = 0;
  } else {
    v5FullRefillSeconds = refillLimited ? null : fullSeconds;
  }
  return {
    enabled: true,
    status: s.status,
    member: Boolean(raw),
    share,
    autoAnlasFallback: s.autoAnlasFallback === true,
    sharedBalanceRisk,
    localAnlasFallbackSpent: amountNumber(
      units(localFallback.fixed) + units(localFallback.purchased),
    ),
    localAnlasFallbackFixedSpent: amountNumber(units(localFallback.fixed)),
    localAnlasFallbackPurchasedSpent: amountNumber(units(localFallback.purchased)),
    localFallbackV5Equivalent: amountNumber(units(localFallback.v5Equivalent)),
    groupId: group?.id ?? null,
    groupName: group?.name ?? null,
    groupSize: group?.members.length ?? 0,
    boundAccountId: s.boundAccountId,
    v5Available: available,
    v5Capacity: capacity,
    v5Reserved: reserved,
    v5PersonalPercent: capacity > 0 ? (100 * available) / capacity : null,
    v5RefillPerDay: refillRate * 86400,
    v5BaseRefillPerDay: officialRate * share * 86400,
    v5RecoveringShare: recoveringShare,
    v5RefillPolicy: 'unfilled_members_v1',
    v5FullRefillSeconds,
    v5RefillLimited: refillLimited,
    v5SharedBalanceStatus: s.v5Reconciliation?.status ?? 'pending',
    v5BalancePolicy: 'personal_wallet_v1',
    anlasAvailable: amountNumber(units(b?.anlasFixedAvailable) + units(b?.anlasPurchasedAvailable)),
    anlasReserved: b ? amountNumber(held(s, user.id, 'anlas')) : 0,
    anlasFixedAvailable: amountNumber(units(b?.anlasFixedAvailable)),
    anlasPurchasedAvailable: amountNumber(units(b?.anlasPurchasedAvailable)),
    anlasFixedReserved: b ? amountNumber(held(s, user.id, 'anlasFixed')) : 0,
    anlasPurchasedReserved: b ? amountNumber(held(s, user.id, 'anlasPurchased')) : 0,
    v5Estimated: true,
    quotaCheckedAt: s.snapshot?.at ?? null,
    refillSecondsPerPercent: s.snapshot?.seconds ?? null,
  };
}
