export type { ActivityOperation, ActivityDay, ActivitySnapshot } from '../../../server/activity-heatmap.ts'
export interface ActivityInvalidation { version: number; dates: string[] | null }
