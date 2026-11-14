import type { MemberQuota, SharedStatus } from './member';

export type ManagementMode = 'owner' | 'admin';
export type ManagementApi = <T>(
  path: string,
  body?: unknown,
  method?: 'GET' | 'POST' | 'PATCH',
) => Promise<T>;
export type ManagementRun = (operation: () => void | Promise<void>) => Promise<void>;
export type ManagementResource = 'v5' | 'anlasFixed' | 'anlasPurchased';
export type ManagementResources = Record<ManagementResource, number>;

export interface ManagementGroup {
  id: string;
  name: string;
  members: string[];
  share: number;
  memberWeights: Record<string, number>;
}
export interface ManagementUser {
  id: string;
  username: string | null;
  displayName?: string;
  note: string | null;
  enabled: boolean;
  acceptingJobs: boolean;
  discordId: string | null;
  inviteExpiresAt: number | null;
  quota: (MemberQuota & { boundAccountId?: string | null }) | null;
  recentActivity: { total: number; done: number; failed: number; queued: number; running: number };
}
export interface ManagementState {
  groups: ManagementGroup[];
  status?: string;
  shared: SharedStatus;
  autoAnlasFallback: boolean;
  configVersion: number;
  publicBaseUrl: string;
  discordLogin: boolean;
  ownerUserId: string | null;
  users: ManagementUser[];
  freeImagePolicy?: import('./member').FreeImagePolicy;
  serviceResources?: ServiceResources;
}
type StorageStatsStatus = 'unconfigured' | 'loading' | 'ready' | 'stale' | 'unavailable';

export interface ServiceResources {
  r2?: {
    bytes: number;
    images: number;
    pending: number;
    errors: number;
    archiveAtBytes: number;
    backups?: {
      bytes: number | null;
      count: number | null;
      checkedAt: string | null;
      status: StorageStatsStatus;
    };
    operations?: {
      status: StorageStatsStatus;
      periodStart: string | null;
      periodEnd: string | null;
      checkedAt: string | null;
      classA: { used: number | null; limit: number; remaining: number | null };
      classB: { used: number | null; limit: number; remaining: number | null };
    };
  };
  pikpak?: {
    originalBytes: number;
    images: number;
    pending: number;
    errors: number;
    quota?: {
      usedBytes: number | null;
      totalBytes: number | null;
      checkedAt: string | null;
      status: StorageStatsStatus;
    };
  };
  memory: { rssBytes: number; heapUsedBytes: number; heapTotalBytes: number };
  uptimeSeconds: number;
  data: { bytes: number; checkedAt: string } | null;
  disk: { totalBytes: number; usedBytes: number; availableBytes: number } | null;
}
export interface ManagementMemberChange {
  configVersion: number;
  groups: ManagementGroup[];
  transferred: ManagementResources;
  autoJoined: { userId: string; groupId: string; memberWeight: number; share: number } | null;
  shareChanges: { userId: string; beforeShare: number; afterShare: number }[];
  users: { id: string; quota: ManagementUser['quota'] }[];
  fromUserId?: string;
  toUserId?: string;
  deletedUserId?: string;
  transferToUserId?: string;
}
export interface ManagementAccount {
  id: string;
  enabled: boolean;
  inFlight: number;
  quotaTier?: number | null;
  quotaFixed?: number | null;
  quotaPurchased?: number | null;
  v5UsagePercent?: number | null;
  quotaCheckedAt?: string;
  quotaError?: string;
  name?: string;
  token?: string;
  proxyUrl?: string;
  weight?: number;
}
export interface ManagementSummary {
  accounts: ManagementAccount[];
  settings?: { publicBaseUrl?: string };
}
export interface ManagementUsageCounts {
  total: number;
  done: number;
  failed: number;
  queued: number;
  pending: number;
  running: number;
  refunded: number;
  free: number;
  v5: number | null;
  fixed: number | null;
  purchased: number | null;
  averageWaitSeconds: number | null;
  averageGenerationSeconds: number | null;
}
export interface ManagementUsage {
  coverage?: 'retained-records';
  days: (ManagementUsageCounts & { id: string; label: string })[];
  since: string;
  until: string;
  timezone: string;
  totals: ManagementUsageCounts;
  members: (ManagementUsageCounts & { id: string; label: string })[];
  models: (ManagementUsageCounts & { id: string; label: string })[];
  recent: {
    id: string;
    memberId: string;
    username: string;
    model: string | null;
    status: string;
    createdAt: string | null;
    waitSeconds: number | null;
    generationSeconds: number | null;
    resource?: string | null;
    amount?: number | null;
    fixed?: number | null;
    purchased?: number | null;
    chargeStatus?: string | null;
    knownNoCharge?: boolean;
    errorStatus?: number | null;
  }[];
}
export interface ManagementLossPolicy {
  version: number;
  v5Points24h: number;
  fixedAnlas24h: number;
  fixedAnlas30d: number;
  memberAttempts24h: number;
}
export interface ManagementAlert {
  id: string;
  level: 'warning' | 'info';
  message: string;
}
export interface ManagementLossEvent {
  id: string;
  type: string;
  at: string;
  memberId?: string;
  memberName: string | null;
  actorId: string | null;
  jobId?: string;
  accountId?: string;
  operation?: string;
  model?: string;
  resource?: ManagementResource;
  amount?: number | null;
  observedAmount?: number | null;
  delta?: number | null;
  resources?: Partial<ManagementResources>;
  upstreamCandidates?: { resources: Partial<ManagementResources>; amountSource?: string }[] | null;
  evidenceLevel?: string;
  lossEventId?: string;
  closureEventId?: string;
  receipt?: string;
  estimateAmountStatus?: string;
  refundRetained?: boolean;
  candidateMode?: string;
  firstUnknownAt?: string;
  deadlineAt?: string;
  closedAt?: string;
  before?: ManagementLossPolicy | null;
  after?: ManagementLossPolicy;
}
export interface ManagementLosses {
  asOf: string;
  coverage?: 'retained-facts';
  policy: ManagementLossPolicy;
  alerts: ManagementAlert[];
  risk: {
    unknownTimeAttempts?: number;
    last24h: { attempts: number; unknownAmounts: number; possible: ManagementResources };
    last30d: { attempts: number; unknownAmounts: number; possible: ManagementResources };
    aggregate: { last24h: ManagementResources; last30d: ManagementResources };
  };
  operating: {
    available: ManagementResources;
    unfunded: Omit<ManagementResources, 'v5'>;
    unattributedReturns: Omit<ManagementResources, 'v5'>;
    pendingAnlasDebits?: Omit<ManagementResources, 'v5'>;
    v5CapacityGap: number;
    gapEventIds: Record<ManagementResource, string | null>;
    pendingExternalFunding: {
      receipt: string;
      eventId: string;
      resource: ManagementResource;
      destination: 'members' | 'operating';
      remaining: number;
      registeredAt: number;
      snapshotAt: number;
    }[];
  };
  events: ManagementLossEvent[];
  nextCursor: string | null;
}
