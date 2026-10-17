import type { AnlasResource, FairQuotaDatabase, FairQuotaState, QuotaSnapshot, Resource, ResourceAmounts } from './fair-quota.ts';

export interface OperatingAccount {
  available: ResourceAmounts; unfunded: Record<AnlasResource, number>; unattributedReturns: Record<AnlasResource, number>;
  gapEventIds: Record<AnlasResource, string | null>; v5CapacityGap: number; v5GapEventId: string | null;
  pendingExternalFunding: { receipt: string; eventId: string; resource: AnlasResource; destination: string; remaining: number; registeredAt: number; snapshotAt: number }[];
}
export interface OperatingFundingInput {
  action?: string; resource?: unknown; amount?: unknown; receipt?: unknown;
  destination?: unknown; fromUnattributed?: unknown; lossEventId?: string;
}
interface FundingEvent {
  id: string; type: string; accountId?: string | null; resource?: Resource; amount: number; request?: unknown;
}
export interface OperatingFundingContext {
  actorId: string; findEvent: (id: string | undefined) => FundingEvent | null | undefined; coveredAmount: (id: string) => number;
}
type OperatingDatabase = FairQuotaDatabase & { settings: { fairQuota: FairQuotaState } };
interface FundingResult { eventId: string; duplicate: boolean; memberFunding?: { resource: AnlasResource; amount: number } }

const emptyResources = (): ResourceAmounts => ({ v5: 0, anlasFixed: 0, anlasPurchased: 0 });
const error: (message: string) => never = message => { throw Object.assign(new Error(message), { statusCode: 409 }); };

export function operatingAccount(s: FairQuotaState): OperatingAccount {
  return s.operating ||= { available: emptyResources(), unfunded: { anlasFixed: 0, anlasPurchased: 0 },
    unattributedReturns: { anlasFixed: 0, anlasPurchased: 0 }, gapEventIds: { anlasFixed: null, anlasPurchased: null },
    v5CapacityGap: 0, v5GapEventId: null, pendingExternalFunding: [] };
}

function append<T extends Record<string, unknown>>(db: OperatingDatabase, event: T, now: number): T {
  (db.ledger ||= []).unshift(structuredClone({ at: new Date(now).toISOString(), accountId: db.settings.fairQuota.boundAccountId, ...event }));
  return event;
}

export function recordAnlasGap(db: OperatingDatabase, resource: AnlasResource, amount: number, before: QuotaSnapshot, after: QuotaSnapshot, now: number) {
  const s = db.settings.fairQuota, ops = operatingAccount(s);
  const id = `resource-gap:${s.boundAccountId}:${after.at}:${resource}`;
  // amount 是此观察点可核销的累计差额；observedAmount 才是本次新观察到的下降，不能重复相加。
  append(db, { id, type: 'resource_gap_observed', resource, amount: ops.unfunded[resource] + amount,
    observedAmount: amount, evidenceLevel: 'aggregate_gap',
    attribution: 'unassigned', actualTaskConsumption: null, beforeSnapshot: before, afterSnapshot: after }, now);
  const covered = Math.min(ops.available[resource], amount);
  ops.available[resource] -= covered; ops.unfunded[resource] += amount - covered;
  ops.gapEventIds[resource] = id;
  if (covered > 0) append(db, { id: `${id}:covered`, type: 'operating_loss_covered', lossEventId: id,
    resource, amount: covered, source: 'operating_available' }, now);
}

export function reconcileV5Capacity(db: OperatingDatabase, amount: number, snapshot: QuotaSnapshot, now: number) {
  const s = db.settings.fairQuota, ops = operatingAccount(s), previous = ops.v5CapacityGap;
  if (Math.abs(amount - previous) <= 1e-9) return;
  const id = `v5-capacity:${s.boundAccountId}:${snapshot.at}`;
  append(db, { id, type: 'v5_capacity_observed', resource: 'v5', amount, previous, delta: amount - previous,
    evidenceLevel: 'aggregate_capacity', actualTaskConsumption: null, snapshot }, now);
  ops.v5CapacityGap = amount; ops.v5GapEventId = amount > 0 ? id : null;
}

export function recoverV5Capacity(db: OperatingDatabase, budget: number, from: number, until: number, now: number): number {
  const s = db.settings.fairQuota, ops = operatingAccount(s), amount = Math.min(ops.v5CapacityGap, budget);
  if (amount <= 0) return 0;
  ops.v5CapacityGap -= amount;
  append(db, { id: `v5-capacity-recovery:${s.boundAccountId}:${from}:${until}`, type: 'v5_capacity_recovered',
    resource: 'v5', amount, gapEventId: ops.v5GapEventId, from, until, evidenceLevel: 'recovery_rate_estimate' }, now);
  return amount;
}

export function matchExternalFunding(db: OperatingDatabase, resource: AnlasResource, delta: number, snapshot: QuotaSnapshot, now: number): number {
  const ops = operatingAccount(db.settings.fairQuota);
  let memberFunding = 0;
  // 只有提前登记的明确注资才进入运营钱包；未说明归属的增额沿用原分配规则。
  for (const pending of ops.pendingExternalFunding) {
    if (pending.resource !== resource || pending.registeredAt > snapshot.at || delta <= 0) continue;
    const amount = Math.min(delta, pending.remaining);
    pending.remaining -= amount; delta -= amount;
    if (pending.destination === 'members') memberFunding += amount;
    else ops.available[resource] += amount;
    append(db, { id: `operating-funding:${pending.receipt}:${snapshot.at}`, type: 'operating_funding_received',
      receiptEventId: pending.eventId, resource, amount, destination: pending.destination,
      source: 'registered_external_funding', snapshotAt: snapshot.at }, now);
  }
  ops.pendingExternalFunding = ops.pendingExternalFunding.filter(pending => pending.remaining > 0);
  // 只在已有未归属缺口时暂留同额正差额：仅凭快照无法区分回跳与真实新充值。
  // 它既不是运营收入，也不自动偿还损耗；普通无缺口的充值仍自动分配。
  const uncertain = Math.min(delta, Math.max(0, ops.unfunded[resource] - ops.unattributedReturns[resource]));
  if (uncertain > 0) {
    ops.unattributedReturns[resource] += uncertain; delta -= uncertain;
    append(db, { id: `unattributed-return:${db.settings.fairQuota.boundAccountId}:${snapshot.at}:${resource}`,
      type: 'resource_return_unattributed', resource, amount: uncertain,
      gapEventId: ops.gapEventIds[resource], snapshotAt: snapshot.at, evidenceLevel: 'unclassified_balance_increase' }, now);
  }
  return delta + memberFunding;
}

// 调用者必须在现有 store.update 内提供持久 lookup/coverage；不能只查有限的热 ledger。
export function applyOperatingFunding(db: FairQuotaDatabase, body: OperatingFundingInput, { actorId, findEvent, coveredAmount }: OperatingFundingContext, now = Date.now()): FundingResult {
  const s = db.settings?.fairQuota;
  if (!s?.enabled || !s.snapshot || !actorId || !s.balances[actorId]) error('需要已分组的运营者和已初始化账本');
  const resource = body.resource as Resource, amount = body.amount, receipt = body.receipt;
  if (!['v5', 'anlasFixed', 'anlasPurchased'].includes(resource) || typeof amount !== 'number' || !Number.isFinite(amount) || amount <= 0 || (resource !== 'v5' && !Number.isSafeInteger(amount))) error('运营资源或数量无效');
  if (typeof receipt !== 'string' || !/^[A-Za-z0-9_-]{8,100}$/.test(receipt)) error('请提供 8–100 位唯一收据标识，只含字母、数字、下划线或连字符');
  const id = `operating-receipt:${receipt}`;
  const destination = body.destination || 'operating', fromUnattributed = body.fromUnattributed === true;
  if (!['members', 'operating'].includes(destination as string) || (body.fromUnattributed !== undefined && typeof body.fromUnattributed !== 'boolean')) error('资金归属选项无效');
  if (body.action !== 'operating-register-funding' && (body.destination !== undefined || body.fromUnattributed !== undefined)) error('只有新增资金凭据支持入账归属选项');
  const request = { action: body.action, actorId, resource, amount, destination, fromUnattributed, lossEventId: body.lossEventId || null };
  const previous = findEvent(id);
  if (previous) {
    if (JSON.stringify(previous.request) !== JSON.stringify(request)) error('该收据已用于不同操作');
    return { eventId: id, duplicate: true };
  }
  const ops = operatingAccount(s);
  let event: Record<string, unknown>, memberFunding: FundingResult['memberFunding'];
  if (body.action === 'operating-contribute') {
    const wallet = s.balances[actorId], field = `${resource}Available` as const;
    if (wallet[field] + 1e-9 < amount) error('运营者本人可用余额不足');
    wallet[field] = Math.max(0, wallet[field] - amount); ops.available[resource] += amount;
    wallet.anlasAvailable = wallet.anlasFixedAvailable + wallet.anlasPurchasedAvailable;
    event = { type: 'operating_funding_received', source: 'operator_share', memberId: actorId };
  } else if (body.action === 'operating-register-funding') {
    // V5 自然恢复不是新的外部注资，不能按一次余额跳升制造运营资金。
    if (resource === 'v5') error('V5 只支持运营者本人份额划入');
    if (fromUnattributed) {
      if (ops.unattributedReturns[resource] < amount) error('待归属入账不足');
      ops.unattributedReturns[resource] -= amount;
      if (destination === 'members') memberFunding = { resource, amount };
      else ops.available[resource] += amount;
      event = { type: 'operating_funding_received', source: 'declared_external_receipt', destination };
    } else {
      if (s.status !== 'active') error('请先刷新有效的官方额度快照');
      if (ops.pendingExternalFunding.length >= 100) error('待核对注资过多，请先核对已有收据');
      ops.pendingExternalFunding.push({ receipt, eventId: id, resource, destination: destination as string, remaining: amount, registeredAt: now, snapshotAt: s.snapshot.at });
      event = { type: 'operating_funding_registered', source: 'external_funding_receipt', destination };
    }
  } else if (body.action === 'operating-reconcile-return') {
    // 仅修正仍未确认/未承担的聚合缺口；不能回冲已核销或未来计提的真实损耗。
    if (resource === 'v5' || body.lossEventId !== ops.gapEventIds[resource] || amount > ops.unfunded[resource] || amount > ops.unattributedReturns[resource]) error('没有可关联修正的未归属余额');
    ops.unfunded[resource] -= amount; ops.unattributedReturns[resource] -= amount;
    ops.gapEventIds[resource] = id;
    event = { type: 'resource_gap_reconciled', source: 'declared_snapshot_correction', previousGapEventId: body.lossEventId,
      correctedAmount: amount, amount: ops.unfunded[resource], evidenceLevel: 'operator_attested_aggregate_correction' };
  } else if (body.action === 'operating-cover-loss') {
    const loss = findEvent(body.lossEventId);
    if (!loss || loss.accountId !== s.boundAccountId || loss.resource !== resource || !['resource_gap_observed', 'resource_gap_reconciled', 'v5_capacity_observed'].includes(loss.type)) error('损耗记录不存在或资源不符');
    const outstanding = resource === 'v5' ? ops.v5CapacityGap : ops.unfunded[resource];
    if (resource === 'v5' && ops.v5GapEventId !== loss.id) error('V5 容量已更新，请使用当前差额记录');
    if (resource !== 'v5' && ops.gapEventIds[resource] !== loss.id) error('Anlas 聚合差额已更新，请使用当前差额记录');
    if (amount > outstanding + 1e-9 || amount > loss.amount - coveredAmount(loss.id) + 1e-9) error('核销超过尚未承担的差额');
    if (amount > ops.available[resource] + 1e-9) error('运营可用余额不足以核销；差额继续保留，不影响调用权限');
    ops.available[resource] = Math.max(0, ops.available[resource] - amount);
    if (resource === 'v5') ops.v5CapacityGap = Math.max(0, ops.v5CapacityGap - amount);
    else ops.unfunded[resource] = Math.max(0, ops.unfunded[resource] - amount);
    event = { type: 'operating_loss_covered', source: 'operating_available', lossEventId: loss.id };
  } else error('不支持的运营账务操作');
  append(db as OperatingDatabase, { id, resource, amount, request, ...event }, now);
  return { eventId: id, duplicate: false, ...(memberFunding ? { memberFunding } : {}) };
}
