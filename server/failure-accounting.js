import { randomUUID } from 'node:crypto';

export const FAILURE_EVENT_TYPES = ['generation_reserved', 'generation_refunded', 'generation_loss_suspected',
  'generation_loss_estimated', 'generation_result_recovered', 'resource_gap_observed', 'resource_gap_reconciled',
  'resource_return_unattributed', 'v5_capacity_observed', 'v5_capacity_recovered', 'operating_funding_registered',
  'operating_funding_received', 'operating_loss_covered', 'loss_alert_policy_updated'];
export const FAILURE_EVENT_PREDICATE = `source IN (${FAILURE_EVENT_TYPES.map(type => `'${type}'`).join(', ')})`;
const defaults = { version: 1, v5Points24h: 1, fixedAnlas24h: 100, fixedAnlas30d: 500, memberAttempts24h: 3 };
const fields = Object.keys(defaults).filter(key => key !== 'version');
const fail = (statusCode, message) => { throw Object.assign(new Error(message), { statusCode }); };
const json = value => value ? JSON.parse(value) : null;
const pick = (value, names) => Object.fromEntries(names.filter(name => value?.[name] !== undefined).map(name => [name, value[name]]));
export const lossAlertPolicy = value => ({ ...defaults, ...pick(value, Object.keys(defaults)) });

export function updateLossAlertPolicy(db, body, actorId, now = Date.now()) {
  const before = lossAlertPolicy(db.settings.fairQuota?.lossAlertPolicy), policy = body.policy;
  if (!Number.isSafeInteger(body.expectedVersion) || body.expectedVersion !== before.version) fail(409, '告警配置已变化，请刷新后重试');
  if (!policy || Array.isArray(policy) || typeof policy !== 'object' || Object.keys(policy).length !== fields.length || fields.some(key =>
    typeof policy[key] !== 'number' || !Number.isFinite(policy[key]) || policy[key] < 0 || (key !== 'v5Points24h' && !Number.isSafeInteger(policy[key])))) fail(400, '告警阈值须为有限非负数，Anlas 和次数须为安全整数');
  if (before.version >= Number.MAX_SAFE_INTEGER) fail(409, '告警版本已达到安全上限');
  if (!db.settings.fairQuota) fail(409, '共享账本尚未初始化');
  const after = { version: before.version + 1, ...pick(policy, fields) };
  db.settings.fairQuota.lossAlertPolicy = after;
  db.ledger.unshift(structuredClone({ id: `loss-policy:${randomUUID()}`, type: 'loss_alert_policy_updated', at: new Date(now).toISOString(), actorId, before, after }));
  return after;
}

function cursor(value) {
  if (!value) return null;
  try {
    if (value.length > 1024 || !/^[A-Za-z0-9_-]+$/.test(value)) throw new Error();
    const result = JSON.parse(Buffer.from(value, 'base64url').toString());
    if (!Array.isArray(result) || result.length !== 2 || !Number.isFinite(result[0]) || typeof result[1] !== 'string' || !result[1] || result[1].length > 256) throw new Error();
    return result;
  } catch { fail(400, '历史分页游标无效'); }
}

const resources = value => pick(value, ['v5', 'anlasFixed', 'anlasPurchased']);
function safeEvent(row) {
  const event = JSON.parse(row.data), attempt = event.attempt || {}, request = event.request || {};
  const result = pick(event, ['id', 'type', 'at', 'jobId', 'memberId', 'accountId', 'resource', 'amount', 'observedAmount', 'delta',
    'evidenceLevel', 'lossEventId', 'closureEventId', 'estimateAmountStatus', 'refundRetained', 'firstUnknownAt', 'deadlineAt', 'closedAt', 'candidateMode']);
  result.memberName = row.memberName || (event.memberId ? '已移除成员' : null);
  Object.assign(result, pick(attempt, ['operation', 'model']));
  if (event.resources || attempt.localReserved) result.resources = resources(event.resources || attempt.localReserved);
  const candidates = event.upstreamCandidates || attempt.upstreamCandidates;
  if (attempt.candidateMode && !result.candidateMode) result.candidateMode = attempt.candidateMode;
  if (Array.isArray(candidates)) result.upstreamCandidates = candidates.map(candidate => ({ resources: resources(candidate?.resources), ...pick(candidate, ['amountSource']) }));
  if (event.upstreamCandidates === null) result.upstreamCandidates = null;
  result.actorId = event.actorId || request.actorId || null;
  const receiptId = event.id.startsWith('operating-receipt:') ? event.id : event.receiptEventId;
  if (receiptId?.startsWith('operating-receipt:')) result.receipt = receiptId.slice('operating-receipt:'.length);
  for (const key of ['before', 'after']) if (event.type === 'loss_alert_policy_updated' && event[key]) result[key] = lossAlertPolicy(event[key]);
  return result;
}

function safeOperating(value = {}) {
  return { available: { v5: 0, anlasFixed: 0, anlasPurchased: 0, ...resources(value.available) },
    unfunded: { anlasFixed: 0, anlasPurchased: 0, ...pick(value.unfunded, ['anlasFixed', 'anlasPurchased']) },
    unattributedReturns: { anlasFixed: 0, anlasPurchased: 0, ...pick(value.unattributedReturns, ['anlasFixed', 'anlasPurchased']) },
    v5CapacityGap: value.v5CapacityGap || 0,
    gapEventIds: { anlasFixed: null, anlasPurchased: null, ...pick(value.gapEventIds, ['anlasFixed', 'anlasPurchased']), v5: value.v5GapEventId || null },
    pendingExternalFunding: (value.pendingExternalFunding || []).map(item => pick(item, ['receipt', 'eventId', 'resource', 'destination', 'remaining', 'registeredAt', 'snapshotAt'])) };
}

// 仅取滚动窗口内的标量并在 SQLite 汇总，不把所有 ledger JSON 载入 JS。
const candidate = "CASE WHEN c.type = 'object' THEN c.value ELSE '{}' END";
const numberSql = resource => `CASE WHEN json_type(${candidate}, '$.resources.${resource}') IN ('integer','real')
  AND json_extract(${candidate}, '$.resources.${resource}') >= 0 AND json_extract(${candidate}, '$.resources.${resource}') < 1e999
  THEN json_extract(${candidate}, '$.resources.${resource}') ELSE 0 END`;
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

export function queryFailureAccounting(sqlite, before, now = Date.now()) {
  const point = cursor(before), asOf = new Date(now).toISOString(), since24 = new Date(now - 86400000).toISOString(), since30 = new Date(now - 30 * 86400000).toISOString();
  const params = { asOf, since24, since30 };
  const state = sqlite.prepare("SELECT json_extract(value, '$.fairQuota.lossAlertPolicy') AS policy, json_extract(value, '$.fairQuota.operating') AS operating FROM app_settings WHERE key='settings'").get() || {};
  const policy = lossAlertPolicy(json(state.policy)), operating = safeOperating(json(state.operating) || {});
  const summaries = sqlite.prepare(`${riskCte} SELECT 'last30d' AS window, ${riskColumns} FROM attempts
    UNION ALL SELECT 'last24h', ${riskColumns} FROM attempts WHERE at >= @since24`).all(params);
  const risk = Object.fromEntries(summaries.map(row => [row.window, { attempts: row.attempts, unknownAmounts: row.unknownAmounts,
    possible: { v5: row.v5, anlasFixed: row.anlasFixed, anlasPurchased: row.anlasPurchased } }]));
  const aggregate = sqlite.prepare(`SELECT source, json_extract(data, '$.resource') AS resource,
    SUM(CASE WHEN source = 'resource_gap_observed' THEN MAX(0,COALESCE(json_extract(data, '$.observedAmount'),0)) ELSE MAX(0,COALESCE(json_extract(data, '$.delta'),0)) END) AS last30d,
    SUM(CASE WHEN at >= @since24 THEN CASE WHEN source = 'resource_gap_observed' THEN MAX(0,COALESCE(json_extract(data, '$.observedAmount'),0)) ELSE MAX(0,COALESCE(json_extract(data, '$.delta'),0)) END ELSE 0 END) AS last24h
    FROM ledger WHERE source IN ('resource_gap_observed','v5_capacity_observed') AND at >= @since30 AND at <= @asOf GROUP BY source, resource`).all(params);
  risk.aggregate = { last24h: { v5: 0, anlasFixed: 0, anlasPurchased: 0 }, last30d: { v5: 0, anlasFixed: 0, anlasPurchased: 0 } };
  for (const row of aggregate) for (const window of ['last24h', 'last30d']) if (Object.hasOwn(risk.aggregate[window], row.resource)) risk.aggregate[window][row.resource] += row[window];
  const alerts = [], warn = (id, message) => alerts.push({ id, level: 'warning', message });
  const threshold = (value, limit) => value > 0 && value >= limit;
  // 尝试风险和聚合差额可能重叠：分别比较阈值，以 OR 提示，不能相加冒充消费。
  if (threshold(risk.last24h.possible.v5, policy.v5Points24h) || threshold(risk.aggregate.last24h.v5, policy.v5Points24h)) warn('v5-24h', '最近 24 小时 V5 候选风险或聚合容量差额达到观察阈值；两者未相加，不代表已确认单次消费，也不限制调用。');
  if (threshold(risk.last24h.possible.anlasFixed, policy.fixedAnlas24h) || threshold(risk.aggregate.last24h.anlasFixed, policy.fixedAnlas24h)) warn('fixed-24h', '最近 24 小时固定 Anlas 候选风险或聚合差额达到观察阈值；两者未相加，不代表已确认单次消费。');
  if (threshold(risk.last30d.possible.anlasFixed, policy.fixedAnlas30d) || threshold(risk.aggregate.last30d.anlasFixed, policy.fixedAnlas30d)) warn('fixed-30d', '最近 30 天固定 Anlas 候选风险或聚合差额达到观察阈值；两者未相加，不代表已确认单次消费。');
  if (risk.last30d.possible.anlasPurchased > 0 || risk.aggregate.last30d.anlasPurchased > 0) warn('purchased-30d', '最近 30 天有购买 Anlas 候选风险或聚合差额，请核对保留的记录；两者未相加，不代表已确认单次消费。');
  if (risk.last30d.unknownAmounts) alerts.push({ id: 'amount-unknown', level: 'info', message: '存在金额未定的尝试风险；未将其当作零消费。' });
  const members = sqlite.prepare(`${riskCte} SELECT a.memberId, COUNT(*) AS attempts, json_extract(u.data, '$.memberAuth.username') AS memberName
    FROM attempts a LEFT JOIN users u ON u.id = a.memberId WHERE a.at >= @since24 GROUP BY a.memberId`).all(params);
  for (const member of members) if (threshold(member.attempts, policy.memberAttempts24h)) warn(`member-24h:${member.memberId || 'unknown'}`,
    `成员 ${member.memberName || `已移除成员（${member.memberId || '未知'}）`} 最近 24 小时有 ${member.attempts} 次疑似损耗；仅提示频率，不停止退款或调用。`);
  const rows = sqlite.prepare(`SELECT l.id, l.order_value AS orderValue, l.data,
    json_extract(u.data, '$.memberAuth.username') AS memberName
    FROM (SELECT id, order_value, data FROM ledger INDEXED BY idx_ledger_failure_history WHERE ${FAILURE_EVENT_PREDICATE}
      ${point ? 'AND (order_value < @order OR (order_value = @order AND id < @id))' : ''}
      ORDER BY order_value DESC, id DESC LIMIT 51) l
    LEFT JOIN users u ON u.id = json_extract(l.data, '$.memberId')
    ORDER BY l.order_value DESC, l.id DESC`).all(point ? { order: point[0], id: point[1] } : {});
  const page = rows.slice(0, 50), last = page.at(-1);
  return { asOf, policy, operating, risk, alerts, events: page.map(safeEvent),
    nextCursor: rows.length > 50 ? Buffer.from(JSON.stringify([last.orderValue, last.id])).toString('base64url') : null };
}

export function memberCharge(charge) {
  return charge ? { ...pick(charge, ['resource', 'amount', 'status', 'estimated']), billingMode: charge.billingMode || null,
    v5Equivalent: charge.v5Equivalent ?? null, upstreamBilling: charge.upstreamBilling || null,
    fixed: charge.fixed ?? null, purchased: charge.purchased ?? null } : null;
}
