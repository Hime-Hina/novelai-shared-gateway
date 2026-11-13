import type {
  AnlasResource,
  FairQuotaSyncState,
  QuotaSnapshot,
  Resource,
  ResourceAmounts,
} from './fair-share.ts';
import {
  parseAmount,
  formatAmount,
  minAmount,
  maxAmount,
  type AmountText,
  type AmountInput,
} from './resource-amounts.ts';
import { recordDomainEvent } from '../events/domain-events.ts';
import type {
  FundingReceivedFact,
  ResourceGapFact,
  ObservedReturnFact,
  CorrectedReturnFact,
  DebitsConfirmedFact,
  OperatingLossFact,
} from '../events/domain-payloads.ts';

export interface OperatingAccount {
  available: ResourceAmounts;
  unfunded: Record<AnlasResource, AmountText>;
  unattributedReturns: Record<AnlasResource, AmountText>;
  gapEventIds: Record<AnlasResource, string | null>;
  v5CapacityGap: AmountText;
  v5GapEventId: string | null;
  pendingExternalFunding: {
    receipt: string;
    eventId: string;
    resource: AnlasResource;
    destination: string;
    remaining: AmountText;
    registeredAt: number;
    snapshotAt: number;
  }[];
}
export interface OperatingFundingInput {
  action?: string;
  resource?: unknown;
  amount?: unknown;
  receipt?: unknown;
  destination?: unknown;
  fromUnattributed?: unknown;
  lossEventId?: string;
}
interface FundingEvent {
  id: string;
  type: string;
  accountId?: string | null;
  resource?: Resource;
  amount: AmountInput;
  request?: unknown;
}
export interface OperatingFundingContext {
  actorId: string;
  findEvent: (id: string | undefined) => FundingEvent | null | undefined;
  coveredAmount: (id: string) => AmountInput;
}
type OperatingDatabase = { settings: { fairQuota: FairQuotaSyncState }; ledger?: unknown[] };
interface FundingResult {
  eventId: string;
  duplicate: boolean;
  memberFunding?: { resource: AnlasResource; amount: AmountText };
}

const units = (value: AmountInput): bigint =>
  parseAmount(value, typeof value === 'number' ? 'nearest' : 'exact');
const emptyResources = (): ResourceAmounts => ({ v5: '0', anlasFixed: '0', anlasPurchased: '0' });
const error: (message: string) => never = (message) => {
  throw Object.assign(new Error(message), { statusCode: 409 });
};

export function operatingAccount(s: Pick<FairQuotaSyncState, 'operating'>): OperatingAccount {
  const ops = (s.operating ||= readOperatingAccount(s));
  for (const resource of ['v5', 'anlasFixed', 'anlasPurchased'] as const) {
    ops.available[resource] = formatAmount(units(ops.available[resource]));
  }
  for (const resource of ['anlasFixed', 'anlasPurchased'] as const) {
    ops.unfunded[resource] = formatAmount(units(ops.unfunded[resource]));
    ops.unattributedReturns[resource] = formatAmount(units(ops.unattributedReturns[resource]));
  }
  ops.v5CapacityGap = formatAmount(units(ops.v5CapacityGap));
  for (const funding of ops.pendingExternalFunding) {
    funding.remaining = formatAmount(units(funding.remaining));
  }
  return ops;
}

export function readOperatingAccount(s: Pick<FairQuotaSyncState, 'operating'>): OperatingAccount {
  return (
    s.operating || {
      available: emptyResources(),
      unfunded: { anlasFixed: '0', anlasPurchased: '0' },
      unattributedReturns: { anlasFixed: '0', anlasPurchased: '0' },
      gapEventIds: { anlasFixed: null, anlasPurchased: null },
      v5CapacityGap: '0',
      v5GapEventId: null,
      pendingExternalFunding: [],
    }
  );
}

type AuditMoney = { id: string; resource: Resource; amount: string; request?: unknown };
type OperatingAudit =
  | (FundingReceivedFact & { type: 'operating_funding_received' })
  | (AuditMoney & {
      type: 'operating_funding_registered';
      source: 'external_funding_receipt';
      destination: string;
    })
  | (ResourceGapFact & { type: 'resource_gap_observed' })
  | (ObservedReturnFact & { type: 'resource_return_unattributed' })
  | (CorrectedReturnFact & { type: 'resource_gap_reconciled' })
  | (DebitsConfirmedFact & { type: 'operating_debits_confirmed' })
  | (OperatingLossFact & { type: 'operating_loss_covered' })
  | (AuditMoney & {
      type: 'v5_capacity_observed';
      resource: 'v5';
      previous: string;
      delta: string;
      evidenceLevel: 'aggregate_capacity';
      actualTaskConsumption: null;
      snapshot: QuotaSnapshot;
    })
  | (AuditMoney & {
      type: 'v5_capacity_recovered';
      resource: 'v5';
      gapEventId: string | null;
      from: number;
      until: number;
      evidenceLevel: 'recovery_rate_estimate';
    });
type ManualFundingFact =
  | { type: 'operating_funding_received'; source: 'operator_share'; memberId: string }
  | { type: 'operating_funding_received'; source: 'declared_external_receipt'; destination: string }
  | {
      type: 'operating_funding_registered';
      source: 'external_funding_receipt';
      destination: string;
    }
  | {
      type: 'operating_debits_confirmed';
      evidenceLevel: 'operator_attested_debit_reconciliation';
      allocatedToMembers: string;
    }
  | {
      type: 'resource_gap_reconciled';
      source: 'declared_snapshot_correction';
      previousGapEventId: string;
      correctedAmount: string;
      amount: string;
      evidenceLevel: 'operator_attested_aggregate_correction';
    }
  | { type: 'operating_loss_covered'; source: 'operating_available'; lossEventId: string };

function append<T extends OperatingAudit & Record<string, unknown>>(
  db: OperatingDatabase,
  event: T,
  now: number,
): T {
  const legacyEvent = {
    at: new Date(now).toISOString(),
    accountId: db.settings.fairQuota.boundAccountId,
    ...event,
  };
  (db.ledger ||= []).unshift(structuredClone(legacyEvent));
  const envelope = {
      streamId: `account:${db.settings.fairQuota.boundAccountId}`,
      occurredAt: now,
      accountId: db.settings.fairQuota.boundAccountId,
      amountVersion: 'decimal-12-v1',
      policyVersion: 'operating_funding_v1',
    },
    evidence = [legacyEvent];
  switch (event.type) {
    case 'operating_funding_registered':
      recordDomainEvent(db, {
        ...envelope,
        eventType: 'FundingRegistered',
        payload: {
          funding: db.settings.fairQuota.operating!.pendingExternalFunding.find(
            (funding) => funding.eventId === event.id,
          )!,
          evidence,
        },
      });
      break;
    case 'operating_funding_received':
      recordDomainEvent(db, {
        ...envelope,
        eventType: 'FundingReceived',
        payload: { ...event, evidence },
      });
      break;
    case 'operating_debits_confirmed':
      recordDomainEvent(db, {
        ...envelope,
        eventType: 'DebitsConfirmed',
        payload: { ...event, evidence },
      });
      break;
    case 'resource_gap_observed':
      recordDomainEvent(db, {
        ...envelope,
        eventType: 'ResourceGapObserved',
        payload: { ...event, evidence },
      });
      break;
    case 'resource_return_unattributed':
      recordDomainEvent(db, {
        ...envelope,
        eventType: 'ReturnClassified',
        payload: { ...event, kind: 'observed', evidence },
      });
      break;
    case 'resource_gap_reconciled':
      recordDomainEvent(db, {
        ...envelope,
        eventType: 'ReturnClassified',
        payload: { ...event, kind: 'gap-correction', evidence },
      });
      break;
    case 'operating_loss_covered':
      recordDomainEvent(db, {
        ...envelope,
        eventType: 'OperatingLossCovered',
        payload: { ...event, evidence },
      });
      break;
  }
  return legacyEvent;
}

export function recordAnlasGap(
  db: OperatingDatabase,
  resource: AnlasResource,
  amount: bigint,
  before: QuotaSnapshot,
  after: QuotaSnapshot,
  now: number,
) {
  const s = db.settings.fairQuota,
    ops = operatingAccount(s);
  const id = `resource-gap:${s.boundAccountId}:${after.at}:${resource}`;
  const returned = minAmount(units(ops.unattributedReturns[resource]), amount);
  if (returned > 0n) {
    ops.unattributedReturns[resource] = formatAmount(
      units(ops.unattributedReturns[resource]) - returned,
    );
    amount -= returned;
    append(
      db,
      {
        id: `${id}:return`,
        type: 'resource_return_unattributed',
        resource,
        amount: formatAmount(-returned),
        gapEventId: ops.gapEventIds[resource],
        snapshotAt: after.at,
        evidenceLevel: 'unclassified_balance_increase',
      },
      now,
    );
  }
  if (amount <= 0n) {
    return;
  }
  // amount 是此观察点可核销的累计差额；observedAmount 才是本次新观察到的下降，不能重复相加。
  append(
    db,
    {
      id,
      type: 'resource_gap_observed',
      resource,
      amount: formatAmount(units(ops.unfunded[resource]) + amount),
      observedAmount: formatAmount(amount),
      evidenceLevel: 'aggregate_gap',
      attribution: 'unassigned',
      actualTaskConsumption: null,
      beforeSnapshot: before,
      afterSnapshot: after,
    },
    now,
  );
  // 聚合下降尚未确认，不自动消耗运营资金；沿用有收据的人工核销。
  ops.unfunded[resource] = formatAmount(units(ops.unfunded[resource]) + amount);
  ops.gapEventIds[resource] = id;
}

export function reconcileV5Capacity(
  db: OperatingDatabase,
  amount: bigint,
  snapshot: QuotaSnapshot,
  now: number,
) {
  const s = db.settings.fairQuota,
    ops = operatingAccount(s),
    previous = units(ops.v5CapacityGap);
  if (amount === previous) {
    return null;
  }
  const id = `v5-capacity:${s.boundAccountId}:${snapshot.at}`;
  const event = append(
    db,
    {
      id,
      type: 'v5_capacity_observed',
      resource: 'v5',
      amount: formatAmount(amount),
      previous: formatAmount(previous),
      delta: formatAmount(amount - previous),
      evidenceLevel: 'aggregate_capacity',
      actualTaskConsumption: null,
      snapshot,
    },
    now,
  );
  ops.v5CapacityGap = formatAmount(amount);
  ops.v5GapEventId = amount > 0n ? id : null;
  return event;
}

export function recoverV5Capacity(
  db: OperatingDatabase,
  budget: bigint,
  from: number,
  until: number,
  now: number,
): { amount: bigint; evidence: Record<string, unknown>[] } {
  const s = db.settings.fairQuota,
    ops = operatingAccount(s),
    amount = minAmount(units(ops.v5CapacityGap), budget);
  if (amount <= 0n) {
    return { amount: 0n, evidence: [] };
  }
  ops.v5CapacityGap = formatAmount(units(ops.v5CapacityGap) - amount);
  const event = append(
    db,
    {
      id: `v5-capacity-recovery:${s.boundAccountId}:${from}:${until}`,
      type: 'v5_capacity_recovered',
      resource: 'v5',
      amount: formatAmount(amount),
      gapEventId: ops.v5GapEventId,
      from,
      until,
      evidenceLevel: 'recovery_rate_estimate',
    },
    now,
  );
  return { amount, evidence: [event] };
}

export function matchExternalFunding(
  db: OperatingDatabase,
  resource: AnlasResource,
  delta: bigint,
  snapshot: QuotaSnapshot,
  now: number,
  confirmationEventId?: string,
): bigint {
  const ops = operatingAccount(db.settings.fairQuota);
  let memberFunding = 0n;
  // 只有提前登记的明确注资才进入运营钱包；未说明归属的增额沿用原分配规则。
  for (const pending of ops.pendingExternalFunding) {
    if (pending.resource !== resource || pending.registeredAt > snapshot.at || delta <= 0n) {
      continue;
    }
    const amount = minAmount(delta, units(pending.remaining));
    pending.remaining = formatAmount(units(pending.remaining) - amount);
    delta -= amount;
    if (pending.destination === 'members') {
      memberFunding += amount;
    } else {
      ops.available[resource] = formatAmount(units(ops.available[resource]) + amount);
    }
    append(
      db,
      {
        id: `operating-funding:${pending.receipt}:${snapshot.at}${confirmationEventId ? `:${confirmationEventId}` : ''}`,
        type: 'operating_funding_received',
        receiptEventId: pending.eventId,
        resource,
        amount: formatAmount(amount),
        destination: pending.destination,
        ...(confirmationEventId ? { confirmationEventId } : {}),
        source: 'registered_external_funding',
        snapshotAt: snapshot.at,
      },
      now,
    );
  }
  ops.pendingExternalFunding = ops.pendingExternalFunding.filter(
    (pending) => units(pending.remaining) > 0n,
  );
  // 只在已有未归属缺口时暂留同额正差额：仅凭快照无法区分回跳与真实新充值。
  // 它既不是运营收入，也不自动偿还损耗；普通无缺口的充值仍自动分配。
  const uncertain = confirmationEventId
    ? 0n
    : minAmount(
        delta,
        maxAmount(0n, units(ops.unfunded[resource]) - units(ops.unattributedReturns[resource])),
      );
  if (uncertain > 0n) {
    ops.unattributedReturns[resource] = formatAmount(
      units(ops.unattributedReturns[resource]) + uncertain,
    );
    delta -= uncertain;
    append(
      db,
      {
        id: `unattributed-return:${db.settings.fairQuota.boundAccountId}:${snapshot.at}:${resource}`,
        type: 'resource_return_unattributed',
        resource,
        amount: formatAmount(uncertain),
        gapEventId: ops.gapEventIds[resource],
        snapshotAt: snapshot.at,
        evidenceLevel: 'unclassified_balance_increase',
      },
      now,
    );
  }
  return delta + memberFunding;
}

// 调用者必须在现有 store.update 内提供持久 lookup/coverage；不能只查有限的热 ledger。
export function applyOperatingFunding(
  db: { settings?: { fairQuota?: FairQuotaSyncState | null }; ledger?: unknown[] },
  body: OperatingFundingInput,
  { actorId, findEvent, coveredAmount }: OperatingFundingContext,
  now = Date.now(),
): FundingResult {
  const s = db.settings?.fairQuota;
  if (!s?.enabled || !s.snapshot || !actorId || !s.balances[actorId]) {
    error('需要已分组的运营者和已初始化账本');
  }
  const resource = body.resource as Resource,
    inputAmount = body.amount,
    receipt = body.receipt;
  if (
    !['v5', 'anlasFixed', 'anlasPurchased'].includes(resource) ||
    typeof inputAmount !== 'number' ||
    !Number.isFinite(inputAmount) ||
    inputAmount <= 0 ||
    (resource !== 'v5' && !Number.isSafeInteger(inputAmount))
  ) {
    error('运营资源或数量无效');
  }
  let amount: bigint;
  try {
    amount = parseAmount(inputAmount);
  } catch {
    error('运营数量最多支持 12 位小数');
  }
  const amountText = formatAmount(amount);
  if (typeof receipt !== 'string' || !/^[A-Za-z0-9_-]{8,100}$/.test(receipt)) {
    error('请提供 8–100 位唯一收据标识，只含字母、数字、下划线或连字符');
  }
  const id = `operating-receipt:${receipt}`;
  const destination = body.destination || 'operating',
    fromUnattributed = body.fromUnattributed === true;
  if (
    (destination !== 'members' && destination !== 'operating') ||
    (body.fromUnattributed !== undefined && typeof body.fromUnattributed !== 'boolean')
  ) {
    error('资金归属选项无效');
  }
  if (
    body.action !== 'operating-register-funding' &&
    (body.destination !== undefined || body.fromUnattributed !== undefined)
  ) {
    error('只有新增资金凭据支持入账归属选项');
  }
  const request = {
    action: body.action,
    actorId,
    resource,
    amount: amountText,
    destination,
    fromUnattributed,
    lossEventId: body.lossEventId || null,
  };
  const previous = findEvent(id);
  if (previous) {
    const previousRequest =
      previous.request && typeof previous.request === 'object'
        ? {
            ...previous.request,
            amount: formatAmount(units((previous.request as { amount: AmountInput }).amount)),
          }
        : previous.request;
    if (JSON.stringify(previousRequest) !== JSON.stringify(request)) {
      error('该收据已用于不同操作');
    }
    return { eventId: id, duplicate: true };
  }
  const ops = operatingAccount(s);
  let event: ManualFundingFact, memberFunding: FundingResult['memberFunding'];
  if (body.action === 'operating-contribute') {
    const wallet = s.balances[actorId],
      field = `${resource}Available` as const;
    if (units(wallet[field]) < amount) {
      error('运营者本人可用余额不足');
    }
    wallet[field] = formatAmount(units(wallet[field]) - amount);
    ops.available[resource] = formatAmount(units(ops.available[resource]) + amount);
    wallet.anlasAvailable = formatAmount(
      units(wallet.anlasFixedAvailable) + units(wallet.anlasPurchasedAvailable),
    );
    event = { type: 'operating_funding_received', source: 'operator_share', memberId: actorId };
  } else if (body.action === 'operating-register-funding') {
    // V5 自然恢复不是新的外部注资，不能按一次余额跳升制造运营资金。
    if (resource === 'v5') {
      error('V5 只支持运营者本人份额划入');
    }
    if (fromUnattributed) {
      if (units(ops.unattributedReturns[resource]) < amount) {
        error('待归属入账不足');
      }
      ops.unattributedReturns[resource] = formatAmount(
        units(ops.unattributedReturns[resource]) - amount,
      );
      if (destination === 'members') {
        memberFunding = { resource, amount: amountText };
      } else {
        ops.available[resource] = formatAmount(units(ops.available[resource]) + amount);
      }
      event = {
        type: 'operating_funding_received',
        source: 'declared_external_receipt',
        destination,
      };
    } else {
      if (s.status !== 'active') {
        error('请先刷新有效的官方额度快照');
      }
      if (ops.pendingExternalFunding.length >= 100) {
        error('待核对注资过多，请先核对已有收据');
      }
      ops.pendingExternalFunding.push({
        receipt,
        eventId: id,
        resource,
        destination: destination as string,
        remaining: amountText,
        registeredAt: now,
        snapshotAt: s.snapshot.at,
      });
      event = {
        type: 'operating_funding_registered',
        source: 'external_funding_receipt',
        destination,
      };
    }
  } else if (body.action === 'operating-confirm-debits') {
    if (
      resource === 'v5' ||
      s.status !== 'active' ||
      Object.values(s.reservations).some((charge) => charge.status === 'reserved') ||
      s.snapshot.at < s.lastSettlementAt ||
      amount > units(s.spent[resource])
    ) {
      error('没有可确认的普通 Anlas 扣款，请先结清任务并刷新快照');
    }
    // 人工凭据确认扣款已包含在净增额中，才释放相应的待核对金额。
    s.spent[resource] = formatAmount(units(s.spent[resource]) - amount);
    s.spent.anlas = formatAmount(units(s.spent.anlas) - amount);
    memberFunding = {
      resource,
      amount: formatAmount(
        matchExternalFunding(db as OperatingDatabase, resource, amount, s.snapshot, now, id),
      ),
    };
    event = {
      type: 'operating_debits_confirmed',
      evidenceLevel: 'operator_attested_debit_reconciliation',
      allocatedToMembers: memberFunding.amount,
    };
  } else if (body.action === 'operating-reconcile-return') {
    // 仅修正仍未确认/未承担的聚合缺口；不能回冲已核销或未来计提的真实损耗。
    if (
      resource === 'v5' ||
      body.lossEventId !== ops.gapEventIds[resource] ||
      amount > units(ops.unfunded[resource]) ||
      amount > units(ops.unattributedReturns[resource])
    ) {
      error('没有可关联修正的未归属余额');
    }
    ops.unfunded[resource] = formatAmount(units(ops.unfunded[resource]) - amount);
    ops.unattributedReturns[resource] = formatAmount(
      units(ops.unattributedReturns[resource]) - amount,
    );
    ops.gapEventIds[resource] = id;
    event = {
      type: 'resource_gap_reconciled',
      source: 'declared_snapshot_correction',
      previousGapEventId: body.lossEventId!,
      correctedAmount: amountText,
      amount: ops.unfunded[resource],
      evidenceLevel: 'operator_attested_aggregate_correction',
    };
  } else if (body.action === 'operating-cover-loss') {
    const loss = findEvent(body.lossEventId);
    if (
      !loss ||
      loss.accountId !== s.boundAccountId ||
      loss.resource !== resource ||
      !['resource_gap_observed', 'resource_gap_reconciled', 'v5_capacity_observed'].includes(
        loss.type,
      )
    ) {
      error('损耗记录不存在或资源不符');
    }
    const outstanding = units(resource === 'v5' ? ops.v5CapacityGap : ops.unfunded[resource]);
    if (resource === 'v5' && ops.v5GapEventId !== loss.id) {
      error('V5 容量已更新，请使用当前差额记录');
    }
    if (resource !== 'v5' && ops.gapEventIds[resource] !== loss.id) {
      error('Anlas 聚合差额已更新，请使用当前差额记录');
    }
    if (amount > outstanding || amount > units(loss.amount) - units(coveredAmount(loss.id))) {
      error('核销超过尚未承担的差额');
    }
    if (amount > units(ops.available[resource])) {
      error('运营可用余额不足以核销；差额继续保留，不影响调用权限');
    }
    ops.available[resource] = formatAmount(units(ops.available[resource]) - amount);
    if (resource === 'v5') {
      ops.v5CapacityGap = formatAmount(units(ops.v5CapacityGap) - amount);
    } else {
      ops.unfunded[resource] = formatAmount(units(ops.unfunded[resource]) - amount);
    }
    event = { type: 'operating_loss_covered', source: 'operating_available', lossEventId: loss.id };
  } else {
    error('不支持的运营账务操作');
  }
  append(db as OperatingDatabase, { id, resource, amount: amountText, request, ...event }, now);
  return { eventId: id, duplicate: false, ...(memberFunding ? { memberFunding } : {}) };
}
