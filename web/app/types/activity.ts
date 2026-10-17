export type ActivityOperation = 'generate' | 'upscale' | 'encode-vibe'
export interface ActivityDay {
  date: string
  total: number
  done: number
  failed: number
  cancelled: number
  queued: number
  pending: number
  running: number
  other: number
}
export interface ActivitySnapshot {
  timezone: 'Asia/Taipei'
  from: string
  to: string
  asOf: string
  memberId: string | null
  operation: ActivityOperation
  coverage: 'retained-records'
  days: ActivityDay[]
}
export interface ActivityInvalidation { version: number; dates: string[] | null }
