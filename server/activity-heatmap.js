const dayMs = 86400000, offsetMs = 8 * 3600000;
const fail = message => { throw Object.assign(new Error(message), { statusCode: 400 }); };
const calendarDate = value => /^\d{4}-\d{2}-\d{2}$/.test(value)
  && Number.isFinite(Date.parse(value + 'T00:00:00Z'))
  && new Date(value + 'T00:00:00Z').toISOString().slice(0, 10) === value;

export function activityDate(createdAt) {
  if (typeof createdAt !== 'string' || !/^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/.test(createdAt)
    || !calendarDate(createdAt.slice(0, 10))) return null;
  const timestamp = Date.parse(createdAt);
  if (!Number.isFinite(timestamp)) return null;
  const date = new Date(timestamp + offsetMs).toISOString().slice(0, 10);
  return calendarDate(date) ? date : null;
}

export function activityRange(now = Date.now()) {
  const today = new Date(now + offsetMs), year = today.getUTCFullYear(), month = today.getUTCMonth();
  const end = Date.UTC(year, month, today.getUTCDate()) + dayMs;
  // 去年同日之后至今天；闰日的去年同日夹到2月28日，保留365/366天。
  const previousDay = Math.min(today.getUTCDate(), new Date(Date.UTC(year - 1, month + 1, 0)).getUTCDate());
  const start = Date.UTC(year - 1, month, previousDay) + dayMs;
  return { from: new Date(start).toISOString().slice(0, 10), to: new Date(end - dayMs).toISOString().slice(0, 10),
    since: new Date(start - offsetMs).toISOString(), until: new Date(end - offsetMs).toISOString() };
}

export async function readActivityHeatmap(store, params, { memberId = null, allowMemberFilter = false }, now = Date.now()) {
  for (const key of ['operation', 'memberId', 'dates']) if (params.getAll(key).length > 1) fail('热力图参数不能重复');
  if (!allowMemberFilter && !memberId) fail('成员热力图需要已确认的成员身份');
  if (!allowMemberFilter && params.has('memberId')) fail('成员热力图只能查询本人');
  if (allowMemberFilter) {
    memberId = params.get('memberId') || null;
    if (memberId && (memberId.length > 128 || /[\x00-\x1f\x7f]/.test(memberId))) fail('成员标识无效');
  }
  const operation = params.get('operation') || 'generate';
  if (!['generate', 'upscale', 'encode-vibe'].includes(operation)) fail('不支持的热力图操作类型');
  const range = activityRange(now);
  let dates = null;
  if (params.has('dates')) {
    const value = params.get('dates');
    if (!value || value.length > 4026) fail('日期列表不能为空且最多366天');
    dates = [...new Set(value.split(','))].sort();
    if (dates.length > 366 || dates.some(date => !calendarDate(date) || date < range.from || date > range.to)) fail('日期须在最近一年内');
  }
  const days = await store.readActivityDays({ since: range.since, until: range.until, memberId, operation, dates });
  // 旧版本曾裁剪历史，手工清理仍可删除；没有记录不能证明当天没有调用。
  return { timezone: 'Asia/Taipei', from: range.from, to: range.to, asOf: new Date(now).toISOString(),
    memberId, operation, coverage: 'retained-records', days };
}
