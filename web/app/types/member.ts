// 当前成员 API 的公开字段；上游身份、会话摘要和完整生成参数不进入页面模型。
export interface LoginOptions { discord: boolean; ready: boolean; origin: string | null }
export interface MemberQuota {
  member: boolean; status: string; groupName: string | null; groupSize: number; share: number
  autoAnlasFallback: boolean
  v5Available: number; v5Capacity: number; v5Reserved: number; v5RefillPerDay: number
  v5RefillLimited: boolean; v5FullRefillSeconds: number | null
  anlasFixedAvailable: number; anlasPurchasedAvailable: number
  anlasFixedReserved: number; anlasPurchasedReserved: number
}
export interface MemberCharge {
  status: string; resource: string; amount: number
  fixed?: number | null; purchased?: number | null; billingMode?: string; estimated?: boolean
}
export interface MemberJob {
  id: string; requestId?: string | null; createdAt: string; model: string; status: string
  charge?: MemberCharge | null; lateResultAvailable?: boolean; imageUrl?: string
}
export type LateResult = Pick<MemberJob, 'id' | 'model' | 'createdAt' | 'imageUrl' | 'charge'> & { lateResultAt: string }
export interface MemberProfile {
  username: string; owner: boolean; sessionExpiresAt: number; acceptingJobs: boolean
  quota: MemberQuota | null; jobs: MemberJob[]; lateResults: LateResult[]
}
export interface SharedStatus {
  status: string; subscriptionActive: boolean; expiresAt: string | null; officialCheckedAt: string | null
  stale: boolean; v5Percent: number | null; v5IsNegative: boolean; refillSecondsPerPercent: number | null
  anlasFixed: number | null; anlasPurchased: number | null
  running: number; queued: number; pending: number; concurrencyLimit: number
  cooldownUntil: string | null; cooldownReason: string | null; quotaSyncError: boolean; recentFailures: number
}
export interface BatchJob {
  id: string; index: number; model: string; status: string; cancelled: boolean
  imageUrl?: string; lateResultAvailable?: boolean; error?: string
}
export interface MemberBatch {
  id: string; cancellable: boolean; cancelledCount?: number
  counts: { total: number; pending: number; queued: number; running: number; done: number; failed: number }
  jobs: BatchJob[]
}
export interface StateInvalidation {
  bootId: string; version: number; scopes: ('shared' | 'member' | 'owner')[]; updatedAt: string; resync: boolean
}
export type MemberApi = <T>(path: string, body?: unknown) => Promise<T>
