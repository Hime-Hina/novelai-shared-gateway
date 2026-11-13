import { randomUUID } from 'node:crypto';
import type Database from 'better-sqlite3';
import type { FairCharge, Resource, ResourceAmounts } from './fair-share.ts';
import {
  generationCandidateAmountsKnown,
  type GenerationAttempt,
} from '../generation/accounting.ts';
import type { OperatingAccount } from './operating-accounting.ts';
import { recordDomainEvent } from '../events/domain-events.ts';
import {
  parseAmount,
  formatAmount,
  decimalRatio,
  AMOUNT_SCALE,
  amountNumber,
} from './resource-amounts.ts';
import type { RelationalEvent, RelationalFact } from '../events/relational-events.ts';

export interface LossAlertPolicy {
  version: number;
  v5Points24h: number;
  fixedAnlas24h: number;
  fixedAnlas30d: number;
  memberAttempts24h: number;
}
type PolicyField = Exclude<keyof LossAlertPolicy, 'version'>;
type PolicyDatabase = {
  settings: {
    fairQuota?: { lossAlertPolicy?: Partial<LossAlertPolicy> | null } | null;
  };
  ledger: unknown[];
};
interface PolicyInput {
  expectedVersion?: unknown;
  policy?: unknown;
}
interface FailureLedgerEvent extends Record<string, unknown> {
  id: string;
  memberId?: string | null;
  actorId?: string | null;
  receiptEventId?: string | null;
  attempt?: Partial<GenerationAttempt> | null;
  request?: { actorId?: string | null } | null;
  resources?: Partial<ResourceAmounts> | null;
  upstreamCandidates?: GenerationAttempt['upstreamCandidates'] | null;
  before?: Partial<LossAlertPolicy> | null;
  after?: Partial<LossAlertPolicy> | null;
}
interface HistoryRow {
  id: string;
  orderValue: number;
  data: string;
  memberName: string | null;
}
type RiskWindow = 'last24h' | 'last30d';
type ResourceNumbers = Record<Resource, number>;
interface RiskSummary<T = number> {
  attempts: number;
  unknownAmounts: number;
  possible: Record<Resource, T>;
}
type FailureRisk<T = number> = Record<RiskWindow, RiskSummary<T>> & {
  aggregate: Record<RiskWindow, Record<Resource, T>>;
};
interface RiskRow extends ResourceNumbers {
  window: RiskWindow;
  attempts: number;
  unknownAmounts: number;
}
interface AggregateRow {
  source: string;
  resource: Resource;
  last24h: number;
  last30d: number;
}
interface MemberRiskRow {
  memberId: string | null;
  attempts: number;
  memberName: string | null;
}
interface RiskParameters {
  asOf: string;
  since24: string;
  since30: string;
}
interface LossAlert {
  id: string;
  level: 'warning' | 'info';
  message: string;
}
type MemberChargeInput = Partial<
  Pick<
    FairCharge,
    'resource' | 'amount' | 'status' | 'v5Equivalent' | 'upstreamBilling' | 'fixed' | 'purchased'
  >
> & {
  estimated?: boolean | null;
  billingMode?: 'free' | 'v5' | 'anlas' | 'anlas_fallback' | null;
};

export const FAILURE_EVENT_TYPES = [
  'generation_reserved',
  'generation_refunded',
  'generation_loss_suspected',
  'generation_loss_estimated',
  'generation_result_recovered',
  'resource_gap_observed',
  'resource_gap_reconciled',
  'resource_return_unattributed',
  'v5_capacity_observed',
  'v5_capacity_recovered',
  'operating_funding_registered',
  'operating_funding_received',
  'operating_loss_covered',
  'loss_alert_policy_updated',
  'operating_debits_confirmed',
];
export const FAILURE_EVENT_PREDICATE = `source IN (${FAILURE_EVENT_TYPES.map((type) => `'${type}'`).join(', ')})`;
export const RELATIONAL_FAILURE_EVENT_PREDICATE = `event_type IN ('DispatchAuthorized','GenerationRefunded','ReviewOpened',
  'LateResultRecovered','ResourceGapObserved','V5CapacityObserved','FundingRegistered',
  'FundingReceived','ReturnClassified','DebitsConfirmed','OperatingLossCovered','LossPolicyChanged')
  OR (event_type='ReviewClosed' AND json_extract(payload,'$.status')='estimated_closed')
  OR (event_type='RefillApplied' AND json_extract(payload,'$.capacityRecovered')!='0')`;
const defaults: LossAlertPolicy = {
  version: 1,
  v5Points24h: 1,
  fixedAnlas24h: 100,
  fixedAnlas30d: 500,
  memberAttempts24h: 3,
};
const fields = Object.keys(defaults).filter((key) => key !== 'version') as PolicyField[];
const fail: (statusCode: number, message: string) => never = (statusCode, message) => {
  throw Object.assign(new Error(message), { statusCode });
};
const json = <T>(value: string | null | undefined): T | null =>
  value ? (JSON.parse(value) as T) : null;
const pick = <T extends object, K extends keyof T>(
  value: T | null | undefined,
  names: readonly K[],
): Partial<Pick<T, K>> =>
  Object.fromEntries(
    names.filter((name) => value?.[name] !== undefined).map((name) => [name, value![name]]),
  ) as Partial<Pick<T, K>>;
export const lossAlertPolicy = (
  value: Partial<LossAlertPolicy> | null | undefined,
): LossAlertPolicy => ({
  ...defaults,
  ...pick(value, Object.keys(defaults) as (keyof LossAlertPolicy)[]),
});

export function updateLossAlertPolicy(
  db: PolicyDatabase,
  body: PolicyInput,
  actorId: string,
  now = Date.now(),
): LossAlertPolicy {
  const before = lossAlertPolicy(db.settings.fairQuota?.lossAlertPolicy),
    policy = body.policy as Partial<LossAlertPolicy> | null | undefined;
  if (!Number.isSafeInteger(body.expectedVersion) || body.expectedVersion !== before.version) {
    fail(409, '告警配置已变化，请刷新后重试');
  }
  if (
    !policy ||
    Array.isArray(policy) ||
    typeof policy !== 'object' ||
    Object.keys(policy).length !== fields.length ||
    fields.some(
      (key) =>
        typeof policy[key] !== 'number' ||
        !Number.isFinite(policy[key]) ||
        policy[key] < 0 ||
        (key !== 'v5Points24h' && !Number.isSafeInteger(policy[key])),
    )
  ) {
    fail(400, '告警阈值须为有限非负数，Anlas 和次数须为安全整数');
  }
  if (before.version >= Number.MAX_SAFE_INTEGER) {
    fail(409, '告警版本已达到安全上限');
  }
  if (!db.settings.fairQuota) {
    fail(409, '共享账本尚未初始化');
  }
  const after = { version: before.version + 1, ...pick(policy, fields) } as LossAlertPolicy;
  db.settings.fairQuota.lossAlertPolicy = after;
  const evidence = {
    id: `loss-policy:${randomUUID()}`,
    type: 'loss_alert_policy_updated',
    at: new Date(now).toISOString(),
    actorId,
    before,
    after,
  };
  db.ledger.unshift(structuredClone(evidence));
  recordDomainEvent(db, {
    eventType: 'PolicyChanged',
    streamId: 'sharing:default',
    occurredAt: now,
    payload: {
      policy: 'lossAlertPolicy',
      beforeVersion: before.version,
      version: after.version,
      value: after,
      actorId,
      evidence: [evidence],
    },
  });
  return after;
}

function cursor(value: string | null | undefined): [number, string] | null {
  if (!value) {
    return null;
  }
  try {
    if (value.length > 1024 || !/^[A-Za-z0-9_-]+$/.test(value)) {
      throw new Error();
    }
    const result: unknown = JSON.parse(Buffer.from(value, 'base64url').toString());
    if (
      !Array.isArray(result) ||
      result.length !== 2 ||
      !Number.isFinite(result[0]) ||
      typeof result[1] !== 'string' ||
      !result[1] ||
      result[1].length > 256
    ) {
      throw new Error();
    }
    return result as [number, string];
  } catch {
    fail(400, '历史分页游标无效');
  }
}

// Compatibility views may use Numbers; these summaries never approve or change money.
const resources = (value: Partial<ResourceAmounts> | null | undefined): Partial<ResourceNumbers> =>
  Object.fromEntries(
    Object.entries(pick(value, ['v5', 'anlasFixed', 'anlasPurchased'])).map(([key, amount]) => [
      key,
      Number(amount),
    ]),
  );
const numericFields = <T extends object, K extends keyof T>(
  value: T | null | undefined,
  fields: readonly K[],
) =>
  Object.fromEntries(
    Object.entries(pick(value, fields)).map(([key, amount]) => [
      key,
      amount == null ? amount : Number(amount),
    ]),
  );
export function publicFailureEvent(event: FailureLedgerEvent, memberName: string | null) {
  const attempt = event.attempt || {},
    request = event.request || {};
  const result: Record<string, unknown> = pick(event, [
    'id',
    'type',
    'at',
    'jobId',
    'memberId',
    'accountId',
    'resource',
    'amount',
    'observedAmount',
    'delta',
    'evidenceLevel',
    'lossEventId',
    'closureEventId',
    'confirmationEventId',
    'allocatedToMembers',
    'estimateAmountStatus',
    'refundRetained',
    'firstUnknownAt',
    'deadlineAt',
    'closedAt',
    'candidateMode',
  ]);
  result.memberName = memberName || (event.memberId ? '已移除成员' : null);
  Object.assign(result, numericFields(event, ['amount', 'observedAmount', 'delta']));
  Object.assign(result, pick(attempt, ['operation', 'model']));
  if (event.resources || attempt.localReserved) {
    result.resources = resources(event.resources || attempt.localReserved);
  }
  const candidates = event.upstreamCandidates || attempt.upstreamCandidates;
  if (attempt.candidateMode && !result.candidateMode) {
    result.candidateMode = attempt.candidateMode;
  }
  if (Array.isArray(candidates)) {
    result.upstreamCandidates = candidates.map((candidate) => ({
      resources: resources(candidate?.resources),
      ...pick(candidate, ['amountSource']),
    }));
  }
  if (event.upstreamCandidates === null) {
    result.upstreamCandidates = null;
  }
  result.actorId = event.actorId || request.actorId || null;
  const receiptId = event.id.startsWith('operating-receipt:') ? event.id : event.receiptEventId;
  if (receiptId?.startsWith('operating-receipt:')) {
    result.receipt = receiptId.slice('operating-receipt:'.length);
  }
  for (const key of ['before', 'after'] as const) {
    if (event.type === 'loss_alert_policy_updated') {
      if (event[key] === null) {
        result[key] = null;
      } else if (event[key]) {
        result[key] = lossAlertPolicy(event[key]);
      }
    }
  }
  return result;
}

interface FailureRiskAttempt {
  memberId: string | null;
  memberName: string | null;
  firstUnknownAt: number | null;
  upstreamCandidates: GenerationAttempt['upstreamCandidates'];
}
interface FailureAggregate {
  at: number;
  resource: Resource;
  amount: string;
}
// 关系查询只提供原薄事实；候选各池取最大值、窗口和汇总仍在损耗 owner 维护。
export function summarizeRelationalFailureRisk(
  attempts: Iterable<FailureRiskAttempt>,
  aggregate: Iterable<FailureAggregate>,
  now: number,
) {
  const zero = () => ({ v5: 0n, anlasFixed: 0n, anlasPurchased: 0n }),
    summary = () => ({ attempts: 0, unknownAmounts: 0, possible: zero() }),
    risk = {
      last24h: summary(),
      last30d: summary(),
      aggregate: { last24h: zero(), last30d: zero() },
    },
    members = new Map<string | null, MemberRiskRow>(),
    windows = ['last24h', 'last30d'] as const;
  let unknownTimeAttempts = 0;
  for (const attempt of attempts) {
    const at = attempt.firstUnknownAt;
    if (at === null) {
      unknownTimeAttempts++;
      continue;
    }
    if (at < now - 30 * 86400000 || at > now) {
      continue;
    }
    const possible = zero(),
      candidates = attempt.upstreamCandidates;
    const unknown = !generationCandidateAmountsKnown(candidates);
    for (const candidate of candidates) {
      for (const [resource, amount] of Object.entries(candidate.resources)) {
        if (!Object.hasOwn(possible, resource)) {
          throw new Error('Unknown candidate resource.');
        }
        const value = parseAmount(amount);
        if (value < 0n) {
          throw new Error('Negative candidate resource.');
        }
        possible[resource as Resource] =
          possible[resource as Resource] > value ? possible[resource as Resource] : value;
      }
    }
    for (const window of windows) {
      if (window === 'last24h' && at < now - 86400000) {
        continue;
      }
      risk[window].attempts++;
      risk[window].unknownAmounts += Number(unknown);
      for (const resource of Object.keys(possible) as Resource[]) {
        risk[window].possible[resource] += possible[resource];
      }
    }
    if (at >= now - 86400000) {
      const member = members.get(attempt.memberId) ?? {
        memberId: attempt.memberId,
        memberName: attempt.memberName,
        attempts: 0,
      };
      member.attempts++;
      members.set(attempt.memberId, member);
    }
  }
  for (const row of aggregate) {
    if (row.at < now - 30 * 86400000 || row.at > now) {
      continue;
    }
    const amount = parseAmount(row.amount);
    if (amount <= 0n) {
      continue;
    }
    risk.aggregate.last30d[row.resource] += amount;
    if (row.at >= now - 86400000) {
      risk.aggregate.last24h[row.resource] += amount;
    }
  }
  const money = (value: Record<Resource, bigint>) =>
    Object.fromEntries(
      Object.entries(value).map(([resource, amount]) => [resource, formatAmount(amount)]),
    ) as Record<Resource, string>;
  return {
    risk: {
      last24h: { ...risk.last24h, possible: money(risk.last24h.possible) },
      last30d: { ...risk.last30d, possible: money(risk.last30d.possible) },
      aggregate: { last24h: money(risk.aggregate.last24h), last30d: money(risk.aggregate.last30d) },
    },
    members: [...members.values()],
    unknownTimeAttempts,
  };
}

export function publicFailureRisk(risk: FailureRisk<string>) {
  const money = (value: Record<Resource, string>) =>
    Object.fromEntries(
      Object.entries(value).map(([resource, amount]) => [
        resource,
        amountNumber(parseAmount(amount)),
      ]),
    ) as ResourceNumbers;
  return {
    last24h: { ...risk.last24h, possible: money(risk.last24h.possible) },
    last30d: { ...risk.last30d, possible: money(risk.last30d.possible) },
    aggregate: { last24h: money(risk.aggregate.last24h), last30d: money(risk.aggregate.last30d) },
  };
}

interface RelationalFailureContext {
  actorId: string | null;
  memberName: string | null;
  jobId: string | null;
  attempt: Partial<GenerationAttempt> | null;
  resources: ResourceAmounts | null;
  beforePolicy: LossAlertPolicy | null;
  lossEventId: string | null;
  closureEventId: string | null;
  firstUnknownAt: number | null;
  deadlineAt: number | null;
}
// 只解释原业务事实字段；缺少的旧累计/步骤/金额状态不从当前账或报价补造。
export function publicRelationalFailureEvent(
  event: RelationalFact &
    Pick<RelationalEvent, 'eventId' | 'occurredAt' | 'memberId' | 'accountId'>,
  context: RelationalFailureContext,
) {
  const legacy: FailureLedgerEvent = {
    id: event.eventId,
    type: '',
    at: new Date(event.occurredAt).toISOString(),
    memberId: event.memberId,
    accountId: event.accountId,
    actorId: context.actorId,
    jobId: context.jobId,
    attempt: context.attempt,
  };
  switch (event.eventType) {
    case 'DispatchAuthorized':
      Object.assign(legacy, {
        type: 'generation_reserved',
        resources: context.resources,
        upstreamCandidates: event.payload.upstreamCandidates,
        candidateMode: event.payload.candidateMode,
      });
      break;
    case 'GenerationRefunded':
      Object.assign(legacy, {
        type: 'generation_refunded',
        resources: context.resources,
        lossEventId: event.payload.lossEventId,
        evidenceLevel: 'local_refund',
      });
      break;
    case 'ReviewOpened':
      Object.assign(legacy, {
        type: 'generation_loss_suspected',
        firstUnknownAt: new Date(event.payload.firstUnknownAt).toISOString(),
        deadlineAt: new Date(event.payload.deadlineAt).toISOString(),
        lossEventId: event.payload.lossEventId,
        upstreamCandidates: context.attempt?.upstreamCandidates ?? null,
        evidenceLevel: 'unconfirmed',
      });
      break;
    case 'ReviewClosed': {
      if (event.payload.status !== 'estimated_closed') {
        throw new Error(
          'A recovered review is represented by its original LateResultRecovered fact.',
        );
      }
      const candidates = context.attempt?.upstreamCandidates,
        known = generationCandidateAmountsKnown(candidates);
      Object.assign(legacy, {
        type: 'generation_loss_estimated',
        lossEventId: event.payload.lossEventId,
        closureEventId: event.eventId,
        closedAt: new Date(event.occurredAt).toISOString(),
        estimateAmountStatus: known ? 'quoted_candidates' : 'unknown',
        upstreamCandidates: known ? candidates : null,
        firstUnknownAt:
          context.firstUnknownAt === null ? null : new Date(context.firstUnknownAt).toISOString(),
        deadlineAt: context.deadlineAt === null ? null : new Date(context.deadlineAt).toISOString(),
        evidenceLevel: 'unconfirmed_estimate',
      });
      break;
    }
    case 'LateResultRecovered':
      Object.assign(legacy, {
        type: 'generation_result_recovered',
        refundRetained: true,
        evidenceLevel: 'complete_local_result',
        lossEventId: context.lossEventId,
        closureEventId: context.closureEventId,
      });
      break;
    case 'ResourceGapObserved':
      Object.assign(legacy, {
        type: 'resource_gap_observed',
        resource: event.payload.resource,
        observedAmount: event.payload.amount,
        amount: null,
        evidenceLevel: event.payload.evidenceLevel,
      });
      break;
    case 'V5CapacityObserved':
      Object.assign(legacy, {
        type: 'v5_capacity_observed',
        resource: 'v5',
        amount: event.payload.capacityGap,
        delta: event.payload.delta,
        evidenceLevel: 'aggregate_capacity',
      });
      break;
    case 'RefillApplied':
      Object.assign(legacy, {
        type: 'v5_capacity_recovered',
        resource: 'v5',
        amount: event.payload.capacityRecovered,
        evidenceLevel: 'recovery_rate_estimate',
      });
      break;
    case 'FundingRegistered':
      Object.assign(legacy, {
        type: 'operating_funding_registered',
        resource: event.payload.resource,
        amount: event.payload.remaining,
        receiptEventId: `operating-receipt:${event.payload.receipt}`,
      });
      break;
    case 'FundingReceived':
      Object.assign(legacy, {
        type: 'operating_funding_received',
        resource: event.payload.resource,
        amount: event.payload.amount,
        receiptEventId: `operating-receipt:${event.payload.receipt}`,
      });
      break;
    case 'ReturnClassified':
      Object.assign(legacy, {
        type:
          event.payload.kind === 'observed'
            ? 'resource_return_unattributed'
            : 'resource_gap_reconciled',
        resource: event.payload.resource,
        amount: event.payload.kind === 'observed' ? event.payload.amount : null,
      });
      break;
    case 'DebitsConfirmed':
      Object.assign(legacy, {
        type: 'operating_debits_confirmed',
        resource: event.payload.resource,
        amount: event.payload.amount,
        allocatedToMembers: event.payload.allocatedToMembers,
        evidenceLevel: event.payload.evidenceLevel,
      });
      break;
    case 'OperatingLossCovered':
      Object.assign(legacy, {
        type: 'operating_loss_covered',
        resource: event.payload.resource,
        amount: event.payload.amount,
        lossEventId: event.payload.lossEventId,
      });
      break;
    case 'LossPolicyChanged': {
      Object.assign(legacy, {
        type: 'loss_alert_policy_updated',
        before: context.beforePolicy,
        after: relationalLossPolicy(event),
      });
      break;
    }
    default:
      throw new Error('No failure report owner for this fact.');
  }
  return publicFailureEvent(legacy, context.memberName);
}

export function relationalLossPolicy(
  fact: Extract<RelationalFact, { eventType: 'LossPolicyChanged' | 'OpeningPolicyRecorded' }>,
): LossAlertPolicy {
  if (fact.eventType === 'LossPolicyChanged') {
    const p = fact.payload;
    return {
      version: p.version,
      v5Points24h: p.v5_24h,
      fixedAnlas24h: Number(p.fixed24h),
      fixedAnlas30d: Number(p.fixed30d),
      memberAttempts24h: p.memberAttempts24h,
    };
  }
  const p = fact.payload;
  return {
    version: p.lossPolicyVersion,
    v5Points24h: p.lossV5_24h,
    fixedAnlas24h: Number(p.lossFixed24h),
    fixedAnlas30d: Number(p.lossFixed30d),
    memberAttempts24h: p.lossMemberAttempts24h,
  };
}

function safeOperating(value: Partial<OperatingAccount> = {}) {
  return {
    available: { v5: 0, anlasFixed: 0, anlasPurchased: 0, ...resources(value.available) },
    unfunded: {
      anlasFixed: 0,
      anlasPurchased: 0,
      ...numericFields(value.unfunded, ['anlasFixed', 'anlasPurchased']),
    },
    unattributedReturns: {
      anlasFixed: 0,
      anlasPurchased: 0,
      ...numericFields(value.unattributedReturns, ['anlasFixed', 'anlasPurchased']),
    },
    v5CapacityGap: Number(value.v5CapacityGap || 0),
    gapEventIds: {
      anlasFixed: null,
      anlasPurchased: null,
      ...pick(value.gapEventIds, ['anlasFixed', 'anlasPurchased']),
      v5: value.v5GapEventId || null,
    },
    pendingExternalFunding: (value.pendingExternalFunding || []).map((item) => ({
      ...pick(item, [
        'receipt',
        'eventId',
        'resource',
        'destination',
        'registeredAt',
        'snapshotAt',
      ]),
      remaining: Number(item.remaining),
    })),
  };
}

export function publicFailureOperating(
  operating: Partial<OperatingAccount>,
  pending: Partial<ResourceAmounts> | null | undefined,
) {
  return {
    ...safeOperating(operating),
    pendingAnlasDebits: {
      anlasFixed: Number(pending?.anlasFixed || 0),
      anlasPurchased: Number(pending?.anlasPurchased || 0),
    },
  };
}

// 仅取滚动窗口内的标量并在 SQLite 汇总，不把所有 ledger JSON 载入 JS。
const candidate = "CASE WHEN c.type = 'object' THEN c.value ELSE '{}' END";
const numberSql = (
  resource: Resource,
) => `CASE WHEN json_type(${candidate}, '$.resources.${resource}') IN ('integer','real','text')
  AND CAST(json_extract(${candidate}, '$.resources.${resource}') AS REAL) >= 0 AND CAST(json_extract(${candidate}, '$.resources.${resource}') AS REAL) < 1e999
  THEN CAST(json_extract(${candidate}, '$.resources.${resource}') AS REAL) ELSE 0 END`;
const riskCte = `WITH attempts AS (
  SELECT COALESCE(json_extract(l.data, '$.attemptId'), l.id) AS attemptId, MIN(l.at) AS at,
    json_extract(l.data, '$.memberId') AS memberId,
    MAX(${numberSql('v5')}) AS v5, MAX(${numberSql('anlasFixed')}) AS anlasFixed, MAX(${numberSql('anlasPurchased')}) AS anlasPurchased,
    MAX(CASE WHEN (${numberSql('v5')}) + (${numberSql('anlasFixed')}) + (${numberSql('anlasPurchased')}) > 0 THEN 0 ELSE 1 END) AS unknownAmount
  FROM ledger l LEFT JOIN json_each(l.data, '$.upstreamCandidates') c
  WHERE l.source = 'generation_loss_suspected' AND l.at >= @since30 AND l.at <= @asOf GROUP BY attemptId
)`;
const riskColumns = `COUNT(*) AS attempts, COALESCE(SUM(unknownAmount),0) AS unknownAmounts,
  COALESCE(SUM(v5),0) AS v5, COALESCE(SUM(anlasFixed),0) AS anlasFixed, COALESCE(SUM(anlasPurchased),0) AS anlasPurchased`;

export function failureAccountingAlerts(
  policy: LossAlertPolicy,
  risk: FailureRisk<number | string>,
  members: readonly MemberRiskRow[],
) {
  const alerts: LossAlert[] = [],
    warn = (id: string, message: string) => alerts.push({ id, level: 'warning', message });
  const threshold = (value: number | string, limit: number) => {
    // 旧 number 保留原比较；关系金额先精确比较，到兼容 DTO 才能转 number。
    if (typeof value === 'number') {
      return value > 0 && value >= limit;
    }
    const units = parseAmount(value),
      boundary = decimalRatio(limit);
    return units > 0n && units * boundary.denominator >= boundary.numerator * AMOUNT_SCALE;
  };
  // 尝试风险和聚合差额可能重叠：分别比较阈值，以 OR 提示，不能相加冒充消费。
  if (
    threshold(risk.last24h.possible.v5, policy.v5Points24h) ||
    threshold(risk.aggregate.last24h.v5, policy.v5Points24h)
  ) {
    warn(
      'v5-24h',
      '最近 24 小时 V5 候选风险或聚合容量差额达到观察阈值；两者未相加，不代表已确认单次消费，也不限制调用。',
    );
  }
  if (
    threshold(risk.last24h.possible.anlasFixed, policy.fixedAnlas24h) ||
    threshold(risk.aggregate.last24h.anlasFixed, policy.fixedAnlas24h)
  ) {
    warn(
      'fixed-24h',
      '最近 24 小时固定 Anlas 候选风险或聚合差额达到观察阈值；两者未相加，不代表已确认单次消费。',
    );
  }
  if (
    threshold(risk.last30d.possible.anlasFixed, policy.fixedAnlas30d) ||
    threshold(risk.aggregate.last30d.anlasFixed, policy.fixedAnlas30d)
  ) {
    warn(
      'fixed-30d',
      '最近 30 天固定 Anlas 候选风险或聚合差额达到观察阈值；两者未相加，不代表已确认单次消费。',
    );
  }
  if (
    threshold(risk.last30d.possible.anlasPurchased, 0) ||
    threshold(risk.aggregate.last30d.anlasPurchased, 0)
  ) {
    warn(
      'purchased-30d',
      '最近 30 天有购买 Anlas 候选风险或聚合差额，请核对保留的记录；两者未相加，不代表已确认单次消费。',
    );
  }
  if (risk.last30d.unknownAmounts) {
    alerts.push({
      id: 'amount-unknown',
      level: 'info',
      message: '存在金额未定的尝试风险；未将其当作零消费。',
    });
  }
  for (const member of members) {
    if (threshold(member.attempts, policy.memberAttempts24h)) {
      warn(
        `member-24h:${member.memberId || 'unknown'}`,
        `成员 ${member.memberName || `已移除成员（${member.memberId || '未知'}）`} 最近 24 小时有 ${member.attempts} 次疑似损耗；仅提示频率，不停止退款或调用。`,
      );
    }
  }
  return alerts;
}

export function queryFailureAccounting(
  sqlite: Database.Database,
  before?: string | null,
  now = Date.now(),
) {
  const point = cursor(before),
    asOf = new Date(now).toISOString(),
    since24 = new Date(now - 86400000).toISOString(),
    since30 = new Date(now - 30 * 86400000).toISOString();
  const params = { asOf, since24, since30 };
  const state =
    sqlite
      .prepare<[], { policy?: string | null; operating?: string | null; spent?: string | null }>(
        "SELECT json_extract(value, '$.fairQuota.lossAlertPolicy') AS policy, json_extract(value, '$.fairQuota.operating') AS operating, json_extract(value, '$.fairQuota.spent') AS spent FROM app_settings WHERE key='settings'",
      )
      .get() || {};
  const policy = lossAlertPolicy(json<Partial<LossAlertPolicy>>(state.policy)),
    operating = json<Partial<OperatingAccount>>(state.operating) || {};
  const summaries = sqlite
    .prepare<RiskParameters, RiskRow>(
      `${riskCte} SELECT 'last30d' AS window, ${riskColumns} FROM attempts
    UNION ALL SELECT 'last24h', ${riskColumns} FROM attempts WHERE at >= @since24`,
    )
    .all(params);
  // 上面的 UNION 固定返回两个窗口；aggregate 沿用下方赋值，在任何读取之前补齐。
  const risk = Object.fromEntries<RiskSummary | FailureRisk['aggregate']>(
    summaries.map((row) => [
      row.window,
      {
        attempts: row.attempts,
        unknownAmounts: row.unknownAmounts,
        possible: { v5: row.v5, anlasFixed: row.anlasFixed, anlasPurchased: row.anlasPurchased },
      },
    ]),
  ) as FailureRisk;
  const aggregate = sqlite
    .prepare<RiskParameters, AggregateRow>(
      `SELECT source, json_extract(data, '$.resource') AS resource,
    SUM(CASE WHEN source = 'resource_gap_observed' THEN MAX(0,COALESCE(CAST(json_extract(data, '$.observedAmount') AS REAL),0)) ELSE MAX(0,COALESCE(CAST(json_extract(data, '$.delta') AS REAL),0)) END) AS last30d,
    SUM(CASE WHEN at >= @since24 THEN CASE WHEN source = 'resource_gap_observed' THEN MAX(0,COALESCE(CAST(json_extract(data, '$.observedAmount') AS REAL),0)) ELSE MAX(0,COALESCE(CAST(json_extract(data, '$.delta') AS REAL),0)) END ELSE 0 END) AS last24h
    FROM ledger WHERE source IN ('resource_gap_observed','v5_capacity_observed') AND at >= @since30 AND at <= @asOf GROUP BY source, resource`,
    )
    .all(params);
  risk.aggregate = {
    last24h: { v5: 0, anlasFixed: 0, anlasPurchased: 0 },
    last30d: { v5: 0, anlasFixed: 0, anlasPurchased: 0 },
  };
  for (const row of aggregate) {
    for (const window of ['last24h', 'last30d'] as const) {
      if (Object.hasOwn(risk.aggregate[window], row.resource)) {
        risk.aggregate[window][row.resource] += row[window];
      }
    }
  }
  const members = sqlite
    .prepare<RiskParameters, MemberRiskRow>(
      `${riskCte} SELECT a.memberId, COUNT(*) AS attempts, json_extract(u.data, '$.memberAuth.username') AS memberName
    FROM attempts a LEFT JOIN users u ON u.id = a.memberId WHERE a.at >= @since24 GROUP BY a.memberId`,
    )
    .all(params);
  const alerts = failureAccountingAlerts(policy, risk, members);
  const rows = sqlite
    .prepare<{ order?: number; id?: string }, HistoryRow>(
      `SELECT l.id, l.order_value AS orderValue, l.data,
    json_extract(u.data, '$.memberAuth.username') AS memberName
    FROM (SELECT id, order_value, data FROM ledger INDEXED BY idx_ledger_failure_history WHERE ${FAILURE_EVENT_PREDICATE}
      ${point ? 'AND (order_value < @order OR (order_value = @order AND id < @id))' : ''}
      ORDER BY order_value DESC, id DESC LIMIT 51) l
    LEFT JOIN users u ON u.id = json_extract(l.data, '$.memberId')
    ORDER BY l.order_value DESC, l.id DESC`,
    )
    .all(point ? { order: point[0], id: point[1] } : {});
  const page = rows.slice(0, 50),
    last = page.at(-1);
  const spent = json<Partial<ResourceAmounts>>(state.spent);
  return {
    asOf,
    policy,
    operating: publicFailureOperating(operating, spent),
    risk,
    alerts,
    events: page.map((row) =>
      publicFailureEvent(JSON.parse(row.data) as FailureLedgerEvent, row.memberName),
    ),
    nextCursor:
      rows.length > 50
        ? Buffer.from(JSON.stringify([last!.orderValue, last!.id])).toString('base64url')
        : null,
  };
}

export function memberCharge(charge: MemberChargeInput | null | undefined) {
  if (!charge) {
    return null;
  }
  return {
    ...pick(charge, ['resource', 'status', 'estimated']),
    amount: Number(charge.amount),
    billingMode: charge.billingMode || null,
    v5Equivalent: charge.v5Equivalent == null ? null : Number(charge.v5Equivalent),
    upstreamBilling: charge.upstreamBilling || null,
    fixed: charge.fixed == null ? null : Number(charge.fixed),
    purchased: charge.purchased == null ? null : Number(charge.purchased),
  };
}
