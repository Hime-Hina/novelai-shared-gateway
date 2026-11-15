// 管理摘要与旧生产读取 worker 共用原日/小时桶和成功率；不解释收费资源。
export function buildUsageDays(
  rows: {
    date: string | null;
    hour: number | null;
    status: string | null;
    count: number;
    credits: number | null;
  }[],
  now: number,
  days: number,
) {
  const offsetMs = 8 * 60 * 60_000;
  const current = new Date(now + offsetMs);
  const midnight = Date.UTC(current.getUTCFullYear(), current.getUTCMonth(), current.getUTCDate());
  const result = Array.from({ length: days }, (_, index) => {
    const date = new Date(midnight + (index - days + 1) * 24 * 60 * 60_000)
      .toISOString()
      .slice(0, 10);
    return {
      date,
      label: date.slice(5),
      done: 0,
      failed: 0,
      total: 0,
      credits: 0,
      successRate: 0,
      hours: Array.from({ length: 24 }, (_, hour) => ({
        hour,
        label: `${String(hour).padStart(2, '0')}:00`,
        done: 0,
        failed: 0,
        total: 0,
        credits: 0,
        successRate: 0,
      })),
    };
  });
  const byDate = new Map(result.map((item) => [item.date, item]));
  for (const row of rows) {
    const day = byDate.get(row.date!);
    const hour = Number(row.hour || 0);
    if (!day || !day.hours[hour]) {
      continue;
    }
    const count = Number(row.count || 0);
    const credits = Math.max(0, Number(row.credits || 0));
    if (row.status === 'done') {
      day.done += count;
      day.credits += credits;
      day.hours[hour].done += count;
      day.hours[hour].credits += credits;
    } else if (row.status === 'failed') {
      day.failed += count;
      day.hours[hour].failed += count;
    }
  }
  for (const day of result) {
    Object.assign(day, finalizeStats(day));
    for (const hour of day.hours) {
      Object.assign(hour, finalizeStats(hour));
    }
  }
  return result;
}

export interface AdminJobStats {
  done: number | null;
  failed: number | null;
  total: number | null;
  successRate: number | null;
}
export function finalizeStats(value: { done: number; failed: number }): {
  done: number;
  failed: number;
  total: number;
  successRate: number;
};
export function finalizeStats(value: Pick<AdminJobStats, 'done' | 'failed'>): AdminJobStats;
export function finalizeStats(value: Pick<AdminJobStats, 'done' | 'failed'>): AdminJobStats {
  const done = value.done === null ? null : Number(value.done || 0);
  const failed = value.failed === null ? null : Number(value.failed || 0);
  if (done === null || failed === null) {
    return { done, failed, total: null, successRate: null };
  }
  const total = done + failed;
  return { done, failed, total, successRate: total ? done / total : 0 };
}

// 与旧生产 serializer 使用同一排除条件；缺少原诊断不等于已知未排除。
export function isStatsExcludedJob(
  job: {
    status?: string | null;
    error?: string | null;
    errorDetail?: string | null;
  } = {},
) {
  if (job.status !== 'failed') {
    return false;
  }
  return /insufficient balance|额度不足|余额不足/i.test(
    `${job.error || ''}\n${job.errorDetail || ''}`,
  );
}
