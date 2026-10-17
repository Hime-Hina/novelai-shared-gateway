import type { MemberCharge } from '../types/member'

export const number = (value: number | null | undefined) => value == null ? '未知' : value.toLocaleString('zh-CN', { maximumFractionDigits: 3 })
export const date = (value: string | number | null | undefined) => value ? new Date(value).toLocaleString('zh-CN', { timeZone: 'Asia/Taipei' }) : '未知'
const statuses: Record<string, string> = { done: '成功', failed: '失败', pending: '等待前序图片', queued: '排队', running: '生成中' }
export const jobStatus = (status: string) => statuses[status] || status
export function resultUrl(value?: string) {
  if (!value) return null
  try {
    const url = new URL(value, 'https://local.invalid')
    return ['http:', 'https:'].includes(url.protocol) ? value : null
  } catch { return null }
}
export function chargeText(charge?: MemberCharge | null) {
  if (!charge) return '—'
  const settlements: Record<string, string> = { refunded: '已退款（本站额度）', settled: '已结算', reserved: '预留' }
  const resources: Record<string, string> = { v5: 'V5 百分点', anlas: 'Anlas', free: '免费' }
  const pools = charge.resource === 'anlas' ? ` · 订阅 ${number(charge.fixed)} / 购买 ${number(charge.purchased)}` : ''
  return `${settlements[charge.status] || charge.status} · ${number(charge.amount)} ${resources[charge.resource] || charge.resource}${pools}${charge.billingMode === 'anlas_fallback' ? '（V5 不足转扣）' : ''}${charge.estimated ? '（估算）' : ''}`
}
