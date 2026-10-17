<script setup lang="ts">
import { computed } from 'vue'
import { NAlert, NCard, NProgress } from 'naive-ui'
import type { MemberProfile } from '../types/member'
import { date, number } from '../utils/member-display'

const props = defineProps<{ profile: MemberProfile }>()
const quota = computed(() => props.profile.quota)
const statuses: Record<string, string> = { pending_opus: '等待管理员接入有效 Opus', inactive_opus: '订阅暂不可用', stale: '官方余额等待刷新', unknown_balance: '官方余额未知', ambiguous_opus: '等待管理员选择上游', active: '额度已同步' }
const percentage = computed(() => {
  const q = quota.value
  return q && Number.isFinite(q.v5Available) && q.v5Capacity > 0 ? Math.max(0, Math.min(100, q.v5Available / q.v5Capacity * 100)) : null
})
const recovery = computed(() => {
  const q = quota.value
  if (!q) return '恢复速度未知'
  if (q.v5RefillLimited) return '恢复时间暂无法可靠估算，请留意共享上游状态'
  if (q.v5FullRefillSeconds === 0) return '个人已满额；满额期间不累计额外恢复'
  if (q.v5RefillPerDay > 0) return `当前每天约恢复 ${number(q.v5RefillPerDay)} 个百分点，补满时间${q.v5FullRefillSeconds == null ? '未知' : `约 ${number(q.v5FullRefillSeconds / 3600)} 小时（随成员使用变化）`}`
  return '恢复速度等待官方数据'
})
</script>

<template>
  <section aria-labelledby="welcome">
    <header>
      <h2 id="welcome">{{ profile.username }} · 我的额度</h2>
      <p id="group" class="muted">{{ quota?.member ? `${quota.groupName || '未命名组'} · ${number(quota.groupSize)} 人 · 个人份额 ${number(quota.share == null ? null : quota.share * 100)}%` : '尚未分配到组，请联系管理员' }}</p>
      <p id="sessionExpiry" class="muted">本次登录有效至 {{ date(profile.sessionExpiresAt) }}（{{ profile.owner ? '管理员固定 12 小时' : '普通成员固定 7 天' }}），不会自动续期。</p>
    </header>
    <NAlert id="status" :type="profile.acceptingJobs === false ? 'warning' : 'info'" :show-icon="false" role="status">
      {{ statuses[quota?.status || ''] || '等待分配额度' }}
      <span v-if="quota?.autoAnlasFallback"> · V5 不足时自动按 Anlas 转扣（本站记账；上游可能消耗公共 V5）</span>
      <span v-if="profile.acceptingJobs === false"> · 管理员已停止接单，已有结果仍可查询和下载</span>
    </NAlert>
    <div class="quota-grid">
      <NCard title="V5 恢复额度" size="small">
        <strong id="v5" class="amount">{{ number(quota?.v5Available) }} / {{ number(quota?.v5Capacity) }}%</strong>
        <NProgress v-if="percentage !== null" id="v5bar" type="line" :percentage="percentage" :show-indicator="false" aria-label="个人 V5 余量占上限比例" />
        <p id="recovery">预留 {{ number(quota?.v5Reserved) }}% · {{ recovery }}</p>
        <p class="muted">单次消耗为估算值。恢复优先分给未满额成员，个人最多恢复到自己的上限；不用且满额时无法继续积攒。</p>
      </NCard>
      <NCard title="订阅 Anlas" size="small">
        <strong id="fixed" class="amount">{{ number(quota?.anlasFixedAvailable) }}</strong>
        <p id="fixedHeld">预留 {{ number(quota?.anlasFixedReserved) }}</p>
        <p class="muted">订阅额度与购买额度分开记录。</p>
      </NCard>
      <NCard title="购买 Anlas" size="small">
        <strong id="purchased" class="amount">{{ number(quota?.anlasPurchasedAvailable) }}</strong>
        <p id="purchasedHeld">预留 {{ number(quota?.anlasPurchasedReserved) }}</p>
        <p class="muted">需请求明确授权，且官方订阅余额耗尽并完成对账后才能使用。</p>
      </NCard>
    </div>
  </section>
</template>

<style scoped>
h2 { margin: 0; font-size: 1.4rem; overflow-wrap: anywhere; }
header { margin-bottom: 20px; }
p { line-height: 1.7; overflow-wrap: anywhere; }
.muted { color: #adb7c6; }
.quota-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 16px; margin-top: 16px; }
.amount { display: block; margin-bottom: 16px; font-size: clamp(1.5rem, 3vw, 2rem); font-variant-numeric: tabular-nums; overflow-wrap: anywhere; }
@media (max-width: 760px) { .quota-grid { grid-template-columns: 1fr; } }
</style>
