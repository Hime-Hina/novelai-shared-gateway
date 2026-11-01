import { estimateFairQuota, fairQuotaSummary, type FairQuotaDatabase, type FairAccount } from './fair-quota.ts';
import { hasActiveOpusSubscription, quotaTimestamp } from './quota-policy.ts';

interface SubscriptionAccount extends Pick<FairAccount, 'id' | 'subscriptionActive' | 'subscriptionExpiresAt' | 'quotaTier'> {
  enabled?: boolean;
}

interface SubscriptionDatabase {
  accounts?: SubscriptionAccount[] | Record<string, SubscriptionAccount>;
  ledger?: unknown[];
  settings?: FairQuotaDatabase['settings'];
}

const rows=(value: SubscriptionDatabase['accounts']): SubscriptionAccount[]=>Array.isArray(value)?value:Object.values(value || {});
const nonnegative=(value: unknown): number=>typeof value==='number'&&Number.isFinite(value)?Math.max(0,value):0;
const wholeBalance=(value: unknown): number=>Math.floor(nonnegative(value)+1e-9);

// 兼容启动器的官方字段名，内容仅表示调用者本地配额，不是共享上游的总余额。
// 在副本中估算恢复，查询不改变真实余额、审计或配置，也不返回任何上游标识。
export function launcherSubscription(db: SubscriptionDatabase,user: { id: string; enabled?: boolean } | null | undefined,now=Date.now()){
  if(!user||user.enabled===false)throw Object.assign(new Error('成员未启用'),{statusCode:403});
  const draft=estimateFairQuota({...db,settings:db.settings || {}},now);
  const quota=fairQuotaSummary(draft,user,now);
  if(!quota?.member)throw Object.assign(new Error('该用户未加入共享分组账本'),{statusCode:403});
  const account=rows(db.accounts).find(a=>a.id===quota.boundAccountId);
  const expires=Math.floor((quotaTimestamp(account?.subscriptionExpiresAt) || 0)/1000);
  // 官方兼容 DTO 必须携带可用的 expiresAt；缺失到期观测时不伪造订阅期限。
  const active=quota.status==='active'&&hasActiveOpusSubscription(account,now)&&expires>Math.floor(now/1000);
  const percent=nonnegative(quota.v5Available);
  const recovery=active&&quota.v5RefillLimited!==true&&quota.v5RefillPerDay>0?Math.ceil(86400/quota.v5RefillPerDay):0;
  return {
    tier:active?3:0,active,expiresAt:active?expires:0,
    trainingStepsLeft:{fixedTrainingStepsLeft:wholeBalance(quota.anlasFixedAvailable),purchasedTrainingSteps:wholeBalance(quota.anlasPurchasedAvailable)},
    perks:{imageGeneration:active,unlimitedImageGeneration:active,unlimitedImageGenerationLimits:active?[{resolution:1048576,maxPrompts:1}]:[]},
    usage:{percent,isNegative:percent<=0,timeUntilNextPercent:Number.isSafeInteger(recovery)?recovery:0}
  };
}
