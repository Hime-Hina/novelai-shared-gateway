import type {
  FairQuotaState,
  FairGroup,
  FairCharge,
  Resource,
  ResourceAmounts,
  QuotaSnapshot,
} from '../quota/fair-share.ts';
import type { OperatingAccount } from '../quota/operating-accounting.ts';
import { parseAmount, formatAmount, type AmountText } from '../quota/resource-amounts.ts';

export interface QuotaEvent {
  eventType: string;
  payload: Record<string, unknown>;
  eventId?: string;
}
export interface QuotaAllocation {
  memberId: string;
  amount: AmountText;
}
export interface ResourceAllocation {
  resource: Resource;
  sourceAmount: AmountText;
  allocations: QuotaAllocation[];
  reason: 'opening' | 'funding';
}
export interface RefillFact {
  from: number;
  until: number;
  snapshotAt: number;
  seconds: number | null;
  budget: AmountText;
  debtRepaid: AmountText;
  capacityRecovered: AmountText;
  allocations: QuotaAllocation[];
  configVersion: number;
  policyVersion: 'unfilled_members_v1';
  roundingVersion: 'decimal-12-v1';
  evidence?: Record<string, unknown>[];
}
export const sharingEventTypes = [
  'SharingConfigured',
  'AllocationChanged',
  'MemberTransferred',
  'MemberRemoved',
  'AccountBound',
  'PolicyChanged',
  'AvailabilityChanged',
  'AdmissionChanged',
  'MemberEligibilityChanged',
] as const;
export const accountQuotaEventTypes = [
  'QuotaAccountOpened',
  'FundsTransferred',
  'AllocationApplied',
  'AvailabilityChanged',
  'QuotaObserved',
  'RefillApplied',
  'QuotaReserved',
  'GenerationSettled',
  'GenerationRefunded',
  'FundingRegistered',
  'FundingReceived',
  'DebitsConfirmed',
  'ResourceGapObserved',
  'ReturnClassified',
  'OperatingLossCovered',
  'V5CapacityObserved',
] as const;
type SharingField =
  | 'version'
  | 'enabled'
  | 'members'
  | 'boundAccountId'
  | 'groups'
  | 'configVersion'
  | 'configurationHistory'
  | 'autoAnlasFallback'
  | 'freeImagePolicy'
  | 'lossAlertPolicy'
  | 'policyHistory';
export interface MemberEligibility {
  memberId: string;
  enabled?: boolean;
  acceptingJobs?: boolean;
  jobAdmissionVersion?: number;
  removed?: boolean;
  createdAt?: string;
  at?: number;
  discord?: { id?: string; guildId?: string; checkedAt?: number; member?: boolean };
}
type ConfiguredSharingQuotaState = Pick<FairQuotaState, SharingField> & {
  pendingStatus?: string;
  memberEligibility?: Record<string, MemberEligibility>;
};
export type SharingQuotaState = Partial<ConfiguredSharingQuotaState>;
export type AccountQuotaState = Omit<FairQuotaState, SharingField> & {
  allocationHistory: Record<string, unknown>[];
};
function configuredSharing(state: SharingQuotaState | null): state is ConfiguredSharingQuotaState {
  return Boolean(
    state &&
    Array.isArray(state.members) &&
    Array.isArray(state.groups) &&
    typeof state.configVersion === 'number',
  );
}

export function splitQuotaState(state: FairQuotaState): {
  sharing: ConfiguredSharingQuotaState;
  quota: AccountQuotaState;
} {
  const {
    version,
    enabled,
    members,
    boundAccountId,
    groups,
    configVersion,
    configurationHistory,
    autoAnlasFallback,
    freeImagePolicy,
    lossAlertPolicy,
    policyHistory,
    ...financial
  } = structuredClone(state);
  const allocationHistory: Record<string, unknown>[] = [];
  const metadata = configurationHistory.map((raw) => {
    const entry = raw as Record<string, unknown>,
      { beforeBalances, afterBalances, transferred, ...configuration } = entry;
    allocationHistory.push({
      version: entry.version,
      beforeBalances,
      afterBalances,
      ...(transferred === undefined ? {} : { transferred }),
    });
    return configuration;
  });
  return {
    sharing: {
      version,
      enabled,
      members,
      boundAccountId,
      groups,
      configVersion,
      configurationHistory: metadata,
      ...(autoAnlasFallback === undefined ? {} : { autoAnlasFallback }),
      ...(freeImagePolicy === undefined ? {} : { freeImagePolicy }),
      ...(lossAlertPolicy === undefined ? {} : { lossAlertPolicy }),
      ...(policyHistory === undefined ? {} : { policyHistory }),
      ...(!boundAccountId ? { pendingStatus: state.status } : {}),
    },
    quota: { ...financial, allocationHistory },
  };
}

export function composeQuotaState(
  sharing: SharingQuotaState | null,
  quota: AccountQuotaState | null,
): FairQuotaState | null {
  if (!configuredSharing(sharing)) {
    return null;
  }
  const {
      pendingStatus,
      memberEligibility: _eligibility,
      ...configuration
    } = structuredClone(sharing),
    { allocationHistory, ...financial } = structuredClone(
      quota || {
        ...initialState(sharing.members, sharing.groups, sharing.configVersion),
        allocationHistory: [],
      },
    );
  const amounts = new Map(allocationHistory.map((entry) => [entry.version, entry]));
  const zeroBalances = (groups: unknown) =>
    Object.fromEntries(
      ((groups as FairGroup[]) || [])
        .flatMap((group) => group.members)
        .map((id) => [id, emptyBalance()]),
    );
  const history = configuration.configurationHistory.map((raw) => {
    const entry = raw as Record<string, unknown>,
      allocation = amounts.get(entry.version);
    if (allocation) {
      return {
        ...entry,
        beforeBalances: allocation.beforeBalances,
        afterBalances: allocation.afterBalances,
        ...(allocation.transferred === undefined ? {} : { transferred: allocation.transferred }),
      };
    }
    return {
      ...entry,
      beforeBalances: zeroBalances(entry.before),
      afterBalances: zeroBalances(entry.after),
      ...(entry.action ? { transferred: emptyResources() } : {}),
    };
  });
  return {
    ...financial,
    ...configuration,
    configurationHistory: history,
    ...(!quota ? { status: pendingStatus || 'pending_opus' } : {}),
  };
}

export function reduceSharingEvent(
  previous: SharingQuotaState | null,
  event: QuotaEvent,
): SharingQuotaState | null {
  const p = event.payload;
  if (event.eventType === 'AdmissionChanged' || event.eventType === 'MemberEligibilityChanged') {
    if (typeof p.memberId !== 'string' || !p.memberId) {
      throw new Error('Member eligibility fact requires a member ID');
    }
    const s = previous || {},
      memberEligibility = (s.memberEligibility ||= {}),
      member: MemberEligibility = {
        ...(memberEligibility[p.memberId] || {}),
        memberId: p.memberId,
      };
    for (const field of ['enabled', 'acceptingJobs', 'removed'] as const) {
      if (p[field] !== undefined) {
        if (typeof p[field] !== 'boolean') {
          throw new Error(`Invalid member eligibility ${field}`);
        }
        member[field] = p[field];
      }
    }
    if (p.jobAdmissionVersion !== undefined) {
      if (
        !Number.isSafeInteger(p.jobAdmissionVersion) ||
        (p.jobAdmissionVersion as number) < (member.jobAdmissionVersion || 0)
      ) {
        throw new Error('Member admission generation moved backwards');
      }
      member.jobAdmissionVersion = p.jobAdmissionVersion as number;
    }
    if (p.at !== undefined) {
      member.at = p.at as number;
    }
    if (p.createdAt !== undefined) {
      member.createdAt = p.createdAt as string;
    }
    if (p.discord) {
      const discord = p.discord as NonNullable<MemberEligibility['discord']>;
      member.discord = {
        ...member.discord,
        ...Object.fromEntries(
          ['id', 'guildId', 'checkedAt', 'member']
            .filter((field) => Object.hasOwn(discord, field))
            .map((field) => [field, discord[field as keyof typeof discord]]),
        ),
      };
    }
    memberEligibility[p.memberId] = member;
    return s;
  }
  if (event.eventType === 'SharingConfigured') {
    if (configuredSharing(previous)) {
      throw new Error('Sharing configuration already exists');
    }
    const sharing = splitQuotaState(
      initialState(p.members as string[], p.groups as FairGroup[], p.configVersion as number),
    ).sharing;
    if (previous?.memberEligibility) {
      sharing.memberEligibility = structuredClone(previous.memberEligibility);
    }
    if (p.at !== undefined) {
      sharing.configurationHistory.push({
        at: p.at,
        actorId: p.actorId || null,
        version: p.configVersion,
        before: [],
        after: structuredClone(p.groups),
        redistributeRemaining: p.redistributeRemaining === true,
      });
    }
    return sharing;
  }
  if (!configuredSharing(previous)) {
    throw new Error(`Sharing event ${event.eventType} has no configuration`);
  }
  const s = previous;
  if (event.eventType === 'AccountBound') {
    s.boundAccountId = p.accountId as string;
    delete s.pendingStatus;
    return s;
  }
  if (event.eventType === 'AvailabilityChanged') {
    s.pendingStatus = p.status as string;
    return s;
  }
  if (event.eventType === 'PolicyChanged') {
    if (p.policy === 'autoAnlasFallback') {
      if (typeof p.value !== 'boolean' || typeof p.at !== 'number') {
        throw new Error('Invalid fallback policy fact');
      }
      s.autoAnlasFallback = p.value;
      s.policyHistory = [
        ...(s.policyHistory || []),
        { at: p.at, actor: 'administrator', autoAnlasFallback: p.value },
      ].slice(-1000);
    } else if (p.policy === 'freeImagePolicy') {
      s.freeImagePolicy = structuredClone(p.value as FairQuotaState['freeImagePolicy']);
    } else if (p.policy === 'lossAlertPolicy') {
      s.lossAlertPolicy = structuredClone(p.value as FairQuotaState['lossAlertPolicy']);
    } else if (p.policy === 'enabled' && typeof p.value === 'boolean') {
      s.enabled = p.value;
    } else {
      throw new Error(`Unsupported sharing policy ${p.policy}`);
    }
    return s;
  }
  if (['AllocationChanged', 'MemberTransferred', 'MemberRemoved'].includes(event.eventType)) {
    if (p.configVersion !== s.configVersion + 1) {
      throw new Error('Sharing configuration version is out of order');
    }
    const before = structuredClone(s.groups);
    s.groups = structuredClone(p.groups as FairGroup[]);
    s.members = structuredClone(p.members as string[]);
    s.configVersion = p.configVersion as number;
    if (event.eventType === 'AllocationChanged') {
      s.configurationHistory.push({
        at: p.at,
        actorId: p.actorId || null,
        version: s.configVersion,
        before,
        after: structuredClone(s.groups),
        redistributeRemaining: p.redistributeRemaining === true,
      });
    } else {
      s.configurationHistory.push({
        at: p.at,
        actorId: 'administrator',
        action: event.eventType === 'MemberTransferred' ? 'transfer-quota' : 'delete-member',
        version: s.configVersion,
        sourceId: p.sourceId,
        targetId: p.targetId,
        autoJoined: p.autoJoined,
        shareChanges: structuredClone(p.shareChanges),
        before,
        after: structuredClone(s.groups),
      });
    }
    if (event.eventType === 'MemberRemoved') {
      const memberId = p.sourceId as string,
        eligibility = (s.memberEligibility ||= {});
      eligibility[memberId] = {
        ...eligibility[memberId],
        memberId,
        removed: true,
        at: p.at as number,
      };
    }
    s.configurationHistory = s.configurationHistory.slice(-1000);
    return s;
  }
  throw new Error(`Unsupported sharing event ${event.eventType}`);
}

export function reduceAccountQuotaEvent(
  previous: AccountQuotaState | null,
  event: QuotaEvent,
): AccountQuotaState | null {
  const p = event.payload;
  if (
    (event.eventType === 'QuotaObserved' && p.kind === 'official') ||
    (event.eventType === 'AvailabilityChanged' && p.kind !== 'quota')
  ) {
    return previous;
  }
  if (event.eventType === 'QuotaAccountOpened') {
    if (previous) {
      throw new Error('Account quota already exists');
    }
    return splitQuotaState(initialState(p.memberIds as string[], [], 0)).quota;
  }
  if (!previous) {
    throw new Error(`Account quota event ${event.eventType} has no opening`);
  }
  if (event.eventType === 'FundsTransferred') {
    const s = previous,
      beforeBalances = structuredClone(s.balances),
      members = p.members as string[];
    const beforeTotal = Object.fromEntries(
      (['v5', 'anlasFixed', 'anlasPurchased'] as const).map((resource) => [
        resource,
        Object.values(s.balances).reduce(
          (sum, b) => sum + parseAmount(b[`${resource}Available`]),
          0n,
        ),
      ]),
    );
    for (const id of members) {
      s.balances[id] ||= emptyBalance();
    }
    if (p.transferred) {
      const source = p.sourceId as string,
        target = p.targetId as string;
      for (const resource of ['v5', 'anlasFixed', 'anlasPurchased'] as const) {
        const field = `${resource}Available` as const,
          amount = (p.transferred as ResourceAmounts)[resource];
        if (s.balances[source]) {
          s.balances[source][field] = subtract(s.balances[source][field], amount);
        } else if (parseAmount(amount) > 0n) {
          throw new Error('Transfer source does not exist');
        }
        s.balances[target][field] = add(s.balances[target][field], amount);
      }
    }
    for (const redistribution of (p.redistributions as {
      resource: Resource;
      allocations: QuotaAllocation[];
    }[]) || []) {
      for (const entry of redistribution.allocations) {
        s.balances[entry.memberId][`${redistribution.resource}Available`] = entry.amount;
      }
    }
    for (const id of Object.keys(s.balances)) {
      if (!members.includes(id)) {
        if (
          ['v5Available', 'anlasFixedAvailable', 'anlasPurchasedAvailable'].some(
            (field) =>
              parseAmount(s.balances[id][field as keyof (typeof s.balances)[string]]) !== 0n,
          )
        ) {
          throw new Error('Removed wallet still has resources');
        }
        delete s.balances[id];
      }
    }
    for (const resource of ['v5', 'anlasFixed', 'anlasPurchased'] as const) {
      if (
        Object.values(s.balances).reduce(
          (sum, b) => sum + parseAmount(b[`${resource}Available`]),
          0n,
        ) !== beforeTotal[resource]
      ) {
        throw new Error('Funds transfer changes total resources');
      }
    }
    for (const balance of Object.values(s.balances)) {
      balance.anlasAvailable = add(balance.anlasFixedAvailable, balance.anlasPurchasedAvailable);
    }
    s.allocationHistory.push({
      version: p.configVersion,
      beforeBalances,
      afterBalances: structuredClone(s.balances),
      ...(p.transferred ? { transferred: structuredClone(p.transferred) } : {}),
    });
    s.allocationHistory = s.allocationHistory.slice(-1000);
    return s;
  }
  const s = composeQuotaState(
    {
      version: 2,
      enabled: true,
      members: Object.keys(previous.balances),
      boundAccountId: null,
      groups: [],
      configVersion: 0,
      configurationHistory: [],
    },
    previous,
  )!;
  const result = reduceFinancialQuotaEvent(s, event);
  if (!result) {
    throw new Error('Financial event removed quota state');
  }
  if (event.eventType === 'GenerationSettled' || event.eventType === 'GenerationRefunded') {
    delete result.reservations[p.jobId as string];
  }
  return { ...splitQuotaState(result).quota, allocationHistory: previous.allocationHistory };
}
const emptyResources = (): ResourceAmounts => ({ v5: '0', anlasFixed: '0', anlasPurchased: '0' });
const emptyBalance = () => ({
  v5Available: '0',
  anlasFixedAvailable: '0',
  anlasPurchasedAvailable: '0',
  anlasAvailable: '0',
});
const add = (left: AmountText, right: AmountText) =>
  formatAmount(parseAmount(left) + parseAmount(right));
function subtract(left: AmountText, right: AmountText): AmountText {
  const result = parseAmount(left) - parseAmount(right);
  if (result < 0n) {
    throw new Error('Quota event overdraws its recorded source');
  }
  return formatAmount(result);
}
function totals(s: FairQuotaState): void {
  for (const balance of Object.values(s.balances)) {
    balance.anlasAvailable = add(balance.anlasFixedAvailable, balance.anlasPurchasedAvailable);
  }
}
function operating(s: FairQuotaState): OperatingAccount {
  return (s.operating ||= {
    available: emptyResources(),
    unfunded: { anlasFixed: '0', anlasPurchased: '0' },
    unattributedReturns: { anlasFixed: '0', anlasPurchased: '0' },
    gapEventIds: { anlasFixed: null, anlasPurchased: null },
    v5CapacityGap: '0',
    v5GapEventId: null,
    pendingExternalFunding: [],
  });
}
function applyAllocations(
  s: FairQuotaState,
  resource: Resource,
  allocations: QuotaAllocation[],
): void {
  for (const allocation of allocations) {
    if (parseAmount(allocation.amount) < 0n) {
      throw new Error('Negative allocation in quota event');
    }
    const balance = s.balances[allocation.memberId];
    if (!balance) {
      throw new Error(`Quota event references unknown member ${allocation.memberId}`);
    }
    const field = `${resource}Available` as const;
    balance[field] = add(balance[field], allocation.amount);
  }
  totals(s);
}
function initialState(
  members: string[],
  groups: FairGroup[],
  configVersion: number,
): FairQuotaState {
  return {
    version: 2,
    enabled: true,
    members,
    boundAccountId: null,
    groups,
    configVersion,
    configurationHistory: [],
    balances: Object.fromEntries(members.map((id) => [id, emptyBalance()])),
    reservations: {},
    snapshot: null,
    spent: {
      ...emptyResources(),
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
    fallbackTotalsByUser: {},
    sharedV5Debt: '0',
  };
}

// These versioned facts contain the decisions already made; replay never calls quote, clock or upstream APIs.
function reduceFinancialQuotaEvent(s: FairQuotaState, event: QuotaEvent): FairQuotaState {
  const p = event.payload;
  if (event.eventType === 'AvailabilityChanged') {
    s.status = p.status as string;
    return s;
  }
  if (event.eventType === 'AllocationApplied') {
    const fact = p as unknown as ResourceAllocation;
    const actual = fact.allocations.reduce(
      (sum, allocation) => sum + parseAmount(allocation.amount),
      0n,
    );
    if (actual > parseAmount(fact.sourceAmount)) {
      throw new Error('Allocation exceeds recorded source amount');
    }
    applyAllocations(s, fact.resource, fact.allocations);
    return s;
  }
  if (event.eventType === 'RefillApplied') {
    const fact = p as unknown as RefillFact;
    if (fact.from !== s.lastRefillAt || fact.until <= fact.from) {
      throw new Error('Refill interval is repeated or out of order');
    }
    if (
      [fact.budget, fact.debtRepaid, fact.capacityRecovered].some(
        (amount) => parseAmount(amount) < 0n,
      )
    ) {
      throw new Error('Negative refill resource amount');
    }
    const allocated = fact.allocations.reduce(
      (sum, allocation) => sum + parseAmount(allocation.amount),
      0n,
    );
    if (
      allocated + parseAmount(fact.debtRepaid) + parseAmount(fact.capacityRecovered) >
      parseAmount(fact.budget)
    ) {
      throw new Error('Refill exceeds recorded source budget');
    }
    s.sharedV5Debt = subtract(s.sharedV5Debt, fact.debtRepaid);
    if (parseAmount(fact.capacityRecovered) > 0n) {
      const ops = operating(s);
      ops.v5CapacityGap = subtract(ops.v5CapacityGap, fact.capacityRecovered);
    }
    applyAllocations(s, 'v5', fact.allocations);
    s.creditedV5 = add(s.creditedV5, formatAmount(allocated));
    s.lastRefillAt = fact.until;
    return s;
  }
  if (event.eventType === 'QuotaObserved') {
    const snapshot = structuredClone(p.snapshot as QuotaSnapshot),
      matched = p.matchedDebits as Record<'anlasFixed' | 'anlasPurchased', AmountText>,
      fallback = p.matchedFallback as { fixed: AmountText; purchased: AmountText };
    if (p.initial !== true) {
      if (!s.snapshot || snapshot.at <= s.snapshot.at) {
        throw new Error('Quota observation is repeated or out of order');
      }
      for (const resource of ['anlasFixed', 'anlasPurchased'] as const) {
        s.spent[resource] = subtract(s.spent[resource], matched[resource]);
      }
      s.fallbackBuffer.fixed = subtract(s.fallbackBuffer.fixed, fallback.fixed);
      s.fallbackBuffer.purchased = subtract(s.fallbackBuffer.purchased, fallback.purchased);
      s.spent = {
        ...s.spent,
        v5: '0',
        anlas: add(s.spent.anlasFixed, s.spent.anlasPurchased),
        fallbackFixed: '0',
        fallbackPurchased: '0',
        fallbackV5: '0',
      };
      s.creditedV5 = '0';
    } else if (s.snapshot) {
      throw new Error('Initial quota already observed');
    }
    s.snapshot = snapshot;
    s.lastRefillAt = p.at as number;
    return s;
  }
  if (event.eventType === 'V5CapacityObserved') {
    if (parseAmount(p.capacityGap as string) < 0n) {
      throw new Error('Negative V5 capacity gap');
    }
    const ops = operating(s);
    ops.v5CapacityGap = p.capacityGap as string;
    ops.v5GapEventId = p.gapEventId as string | null;
    s.v5Reconciliation = structuredClone(p.reconciliation as FairQuotaState['v5Reconciliation']);
    s.v5ReconciliationHistory ||= [];
    if (p.recordHistory === true) {
      s.v5ReconciliationHistory.push(structuredClone(s.v5Reconciliation!));
      s.v5ReconciliationHistory = s.v5ReconciliationHistory.slice(-200);
    }
    return s;
  }
  if (event.eventType === 'QuotaReserved') {
    const jobId = p.jobId as string,
      charge = structuredClone(p.charge as FairCharge);
    if (s.reservations[jobId]) {
      throw new Error('Job already has a quota reservation');
    }
    if (charge.status !== 'reserved') {
      throw new Error('Reservation fact is not reserved');
    }
    if (
      [
        charge.amount,
        charge.fixed,
        charge.purchased,
        charge.sharedV5Reserved || '0',
        charge.v5Equivalent || '0',
      ].some((amount) => parseAmount(amount) < 0n)
    ) {
      throw new Error('Negative quota reservation');
    }
    if (charge.resource === 'anlas' && add(charge.fixed, charge.purchased) !== charge.amount) {
      throw new Error('Anlas reservation split differs from quoted amount');
    }
    const balance = s.balances[charge.userId];
    if (!balance) {
      throw new Error('Reservation member does not exist');
    }
    if (charge.resource === 'v5') {
      balance.v5Available = subtract(balance.v5Available, charge.amount);
    }
    if (charge.resource === 'anlas') {
      balance.anlasFixedAvailable = subtract(balance.anlasFixedAvailable, charge.fixed);
      balance.anlasPurchasedAvailable = subtract(balance.anlasPurchasedAvailable, charge.purchased);
    }
    s.reservations[jobId] = charge;
    totals(s);
    return s;
  }
  if (event.eventType === 'GenerationSettled' || event.eventType === 'GenerationRefunded') {
    const jobId = p.jobId as string,
      recorded = p.charge as FairCharge,
      charge = s.reservations[jobId];
    if (!charge || charge.status !== 'reserved') {
      throw new Error('Quota terminal event has no active reservation');
    }
    if (
      charge.userId !== recorded.userId ||
      charge.accountId !== recorded.accountId ||
      charge.resource !== recorded.resource ||
      charge.amount !== recorded.amount ||
      charge.fixed !== recorded.fixed ||
      charge.purchased !== recorded.purchased
    ) {
      throw new Error('Terminal quota amounts differ from original reservation');
    }
    const success = event.eventType === 'GenerationSettled';
    if (charge.resource !== 'free') {
      if (success) {
        if (charge.billingMode === 'anlas_fallback') {
          s.spent.fallbackFixed = add(s.spent.fallbackFixed, charge.fixed);
          s.spent.fallbackPurchased = add(s.spent.fallbackPurchased, charge.purchased);
          s.spent.fallbackV5 = add(s.spent.fallbackV5, charge.sharedV5Reserved);
          s.fallbackBuffer.fixed = add(s.fallbackBuffer.fixed, charge.fixed);
          s.fallbackBuffer.purchased = add(s.fallbackBuffer.purchased, charge.purchased);
          s.sharedV5Debt = add(s.sharedV5Debt, charge.sharedV5Reserved);
          const total = (s.fallbackTotalsByUser[charge.userId] ||= {
            fixed: '0',
            purchased: '0',
            v5Equivalent: '0',
          });
          total.fixed = add(total.fixed, charge.fixed);
          total.purchased = add(total.purchased, charge.purchased);
          total.v5Equivalent = add(total.v5Equivalent, charge.v5Equivalent);
        } else {
          s.spent[charge.resource] = add(s.spent[charge.resource], charge.amount);
          if (charge.resource === 'anlas') {
            s.spent.anlasFixed = add(s.spent.anlasFixed, charge.fixed);
            s.spent.anlasPurchased = add(s.spent.anlasPurchased, charge.purchased);
          }
        }
      } else {
        const balance = s.balances[charge.userId];
        if (charge.resource === 'v5') {
          balance.v5Available = add(balance.v5Available, charge.amount);
        } else {
          balance.anlasFixedAvailable = add(balance.anlasFixedAvailable, charge.fixed);
          balance.anlasPurchasedAvailable = add(balance.anlasPurchasedAvailable, charge.purchased);
        }
      }
    }
    s.reservations[jobId] = {
      ...charge,
      status: success ? 'settled' : 'refunded',
      settledAt: p.at as number,
    };
    s.lastSettlementAt = Math.max(s.lastSettlementAt, p.at as number);
    totals(s);
    return s;
  }
  if (event.eventType === 'FundingRegistered') {
    const ops = operating(s);
    ops.pendingExternalFunding.push(
      structuredClone(p.funding as OperatingAccount['pendingExternalFunding'][number]),
    );
    return s;
  }
  if (event.eventType === 'FundingReceived') {
    const ops = operating(s),
      resource = p.resource as Resource,
      amount = p.amount as string;
    if (p.source === 'operator_share') {
      const balance = s.balances[p.memberId as string],
        field = `${resource}Available` as const;
      balance[field] = subtract(balance[field], amount);
      ops.available[resource] = add(ops.available[resource], amount);
      totals(s);
    } else {
      if (resource === 'v5') {
        throw new Error('External V5 funding is unsupported');
      }
      if (p.source === 'registered_external_funding') {
        const pending = ops.pendingExternalFunding.find(
          (funding) => funding.eventId === p.receiptEventId,
        );
        if (!pending) {
          throw new Error('Funding receipt does not exist');
        }
        if (pending.resource !== resource || pending.destination !== p.destination) {
          throw new Error('Funding receipt ownership changed');
        }
        pending.remaining = subtract(pending.remaining, amount);
        ops.pendingExternalFunding = ops.pendingExternalFunding.filter(
          (funding) => parseAmount(funding.remaining) > 0n,
        );
      } else if (p.source === 'declared_external_receipt') {
        ops.unattributedReturns[resource] = subtract(ops.unattributedReturns[resource], amount);
      } else {
        throw new Error('Unsupported funding source');
      }
      if (p.destination === 'operating') {
        ops.available[resource] = add(ops.available[resource], amount);
      }
    }
    return s;
  }
  if (event.eventType === 'DebitsConfirmed') {
    const resource = p.resource as 'anlasFixed' | 'anlasPurchased',
      amount = p.amount as string;
    s.spent[resource] = subtract(s.spent[resource], amount);
    s.spent.anlas = subtract(s.spent.anlas, amount);
    return s;
  }
  if (event.eventType === 'ResourceGapObserved') {
    const ops = operating(s),
      resource = p.resource as 'anlasFixed' | 'anlasPurchased';
    if (add(ops.unfunded[resource], p.observedAmount as string) !== p.amount) {
      throw new Error('Observed resource gap does not match cumulative gap');
    }
    ops.unfunded[resource] = p.amount as string;
    ops.gapEventIds[resource] = p.id as string;
    return s;
  }
  if (event.eventType === 'ReturnClassified') {
    const ops = operating(s),
      resource = p.resource as 'anlasFixed' | 'anlasPurchased';
    if (p.kind === 'observed') {
      const next = add(ops.unattributedReturns[resource], p.amount as string);
      if (parseAmount(next) < 0n) {
        throw new Error('Unattributed return is overdrawn');
      }
      ops.unattributedReturns[resource] = next;
    } else if (p.kind === 'gap-correction') {
      ops.unfunded[resource] = subtract(ops.unfunded[resource], p.correctedAmount as string);
      if (ops.unfunded[resource] !== p.amount) {
        throw new Error('Gap correction does not match outstanding amount');
      }
      ops.unattributedReturns[resource] = subtract(
        ops.unattributedReturns[resource],
        p.correctedAmount as string,
      );
      ops.gapEventIds[resource] = p.id as string;
    } else {
      throw new Error('Unsupported return classification');
    }
    return s;
  }
  if (event.eventType === 'OperatingLossCovered') {
    const ops = operating(s),
      resource = p.resource as Resource,
      amount = p.amount as string;
    ops.available[resource] = subtract(ops.available[resource], amount);
    if (resource === 'v5') {
      ops.v5CapacityGap = subtract(ops.v5CapacityGap, amount);
    } else {
      ops.unfunded[resource] = subtract(ops.unfunded[resource], amount);
    }
    return s;
  }
  throw new Error(`Quota reducer does not support event ${event.eventType}`);
}
