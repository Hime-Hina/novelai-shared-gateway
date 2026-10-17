// API 日期是日历坐标，不是 UTC 时间瞬间；NHeatmap 按浏览器本地日期排格。
export function activityDate(value: string): Date {
  const [year, month, day] = value.split('-').map(Number)
  return new Date(year!, month! - 1, day!)
}
export function activityDateKey(value: Date | number): string {
  const date = value instanceof Date ? value : new Date(value)
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`
}
export function activityDates(from: string, to: string): string[] {
  const days: string[] = [], end = activityDate(to)
  for (const date = activityDate(from); date <= end; date.setDate(date.getDate() + 1)) days.push(activityDateKey(date))
  return days
}
export function taipeiDay(now = Date.now()): string {
  const parts = new Intl.DateTimeFormat('en', { timeZone: 'Asia/Taipei', year: 'numeric', month: '2-digit', day: '2-digit' }).formatToParts(now)
  const value = (type: string) => parts.find(part => part.type === type)!.value
  return `${value('year')}-${value('month')}-${value('day')}`
}

export function aggregateActivity(dates: string[], values: Map<string, number>, mode: 'week' | 'month') {
  const periods = new Map<string, { from: string; to: string; value: number }>()
  for (const date of dates) {
    const start = activityDate(date)
    if (mode === 'week') start.setDate(start.getDate() - start.getDay())
    else start.setDate(1)
    const key = activityDateKey(start), period = periods.get(key) || { from: date, to: date, value: 0 }
    period.to = date; period.value += values.get(date) || 0; periods.set(key, period)
  }
  return [...periods.values()]
}
