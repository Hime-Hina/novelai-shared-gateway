<script setup lang="ts">
import { computed } from 'vue'
import { NAlert, NButton, NCard, NProgress, NPopover } from 'naive-ui'
import type { MemberProfile } from '../types/member'
import { date, number } from '../utils/member-display'

const props = defineProps<{ profile: MemberProfile }>()
const quota = computed(() => props.profile.quota)
const sharedRisk = computed(() => {
  const risk = quota.value?.sharedBalanceRisk
  return risk && (risk.v5 || risk.anlasFixed || risk.anlasPurchased)
})
const statuses: Record<string, string> = { pending_opus: '等待管理员接入有效 Opus', inactive_opus: '订阅暂不可用', stale: '官方余额等待更新', unknown_balance: '官方余额未知', ambiguous_opus: '等待管理员选择上游', active: '可提交任务' }
const status = computed(() => [
  quota.value?.status !== 'active' || props.profile.acceptingJobs !== false ? statuses[quota.value?.status || ''] || '等待分配额度' : '',
  props.profile.acceptingJobs === false ? '管理员已停止接单，已有结果仍可查询和下载' : ''
].filter(Boolean).join(' · '))
const attention = computed(() => props.profile.acceptingJobs === false || quota.value?.status !== 'active')
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
    <header class="section-toolbar">
      <div><h2 id="welcome">我的额度 <span class="count-badge">{{ profile.username }}</span></h2>
      <p id="group" class="section-caption">{{ quota?.member ? `${quota.groupName || '未命名组'} · ${number(quota.groupSize)} 人 · 个人份额 ${number(quota.share == null ? null : quota.share * 100)}%` : '尚未分配到组，请联系管理员' }}</p></div>
      <NPopover trigger="click" :style="{ maxWidth: 'min(380px, calc(100vw - 32px))' }">
        <template #trigger><NButton size="small" quaternary>额度规则</NButton></template>
        <p>V5 单次消耗为估算百分点。恢复优先分给未满额成员，个人最多恢复到自己的上限；满额时不会继续积攒。</p>
        <p>订阅额度与购买额度分开记账。购买 Anlas 需请求明确授权，且官方订阅余额耗尽并完成对账后才能使用。</p>
        <p v-if="quota?.autoAnlasFallback">V5 不足时自动按 Anlas 转扣（本站记账；上游可能消耗公共 V5）。</p>
        <p v-else-if="quota?.autoAnlasFallback === false">V5 不足时停止接单，不自动转扣 Anlas。</p>
      </NPopover>
    </header>
    <div id="status" class="quota-status" :class="{ attention }" role="status">{{ status }}</div>
    <NAlert v-if="sharedRisk" id="sharedBalanceRisk" type="warning" role="status">
      检测到未解释的共享余额差额，实际可用额度可能受影响。个人余额保留，派发仍受官方余额限制。检查时间：{{ date(quota?.sharedBalanceRisk?.checkedAt) }}。
    </NAlert>
    <NAlert v-else-if="quota?.sharedBalanceRisk?.pendingAnlasDebit" type="info" role="status">普通 Anlas 扣款等待官方余额核对，可用公共额度暂按保守值计算。</NAlert>
    <div class="quota-grid">
      <NCard title="V5 恢复额度" size="small">
        <div class="quota-value"><strong id="v5" class="amount">{{ number(quota?.v5Available) }}%</strong><span id="v5Capacity" class="section-caption">上限 {{ number(quota?.v5Capacity) }}%</span></div>
        <NProgress v-if="percentage !== null" id="v5bar" type="line" :percentage="percentage" :show-indicator="false" aria-label="个人 V5 余量占上限比例" />
        <p id="v5Held" class="section-caption">预留 {{ number(quota?.v5Reserved) }}%</p>
      </NCard>
      <NCard title="订阅 Anlas" size="small">
        <strong id="fixed" class="amount">{{ number(quota?.anlasFixedAvailable) }}</strong>
        <p id="fixedHeld" class="section-caption">预留 {{ number(quota?.anlasFixedReserved) }}</p>
      </NCard>
      <NCard title="购买 Anlas" size="small">
        <strong id="purchased" class="amount">{{ number(quota?.anlasPurchasedAvailable) }}</strong>
        <p id="purchasedHeld" class="section-caption">预留 {{ number(quota?.anlasPurchasedReserved) }}</p>
      </NCard>
    </div>
    <p id="recovery" class="section-caption recovery">V5 恢复 · {{ recovery }}</p>
    <NCard v-if="quota?.freeImages" id="freeImageQuota" title="免费图片 · 滚动 24 小时" size="small">
      <p>成功 {{ quota.freeImages.successful }} 张 · 在途 {{ quota.freeImages.reserved }} 张 · {{ quota.freeImages.enabled && quota.freeImages.hardLimit !== null ? `上限 ${quota.freeImages.hardLimit} 张` : '未设置硬上限' }}</p>
      <NAlert v-if="quota.freeImages.warning" type="warning">免费图片用量已达到提醒阈值 {{ quota.freeImages.warningThreshold }} 张。</NAlert>
      <p v-if="quota.freeImages.waitingForSettlement">名额已被在途任务占用，请等待结算。</p>
      <p v-else-if="quota.freeImages.nextAvailableAt">下一名额恢复时间：{{ date(quota.freeImages.nextAvailableAt) }}。</p>
    </NCard>
  </section>
</template>

<style scoped>
h2 { overflow-wrap: anywhere; }
p { line-height: 1.7; overflow-wrap: anywhere; font-size: 13px; }
.quota-status { min-height: 30px; line-height: 30px; font-size: 12px; color: var(--text-muted); }
.attention { color: #f2c97d; }
.quota-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }
.quota-value { display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap; }
.amount { display: block; margin-bottom: 12px; font-size: 28px; line-height: 1.3; font-variant-numeric: tabular-nums; overflow-wrap: anywhere; }
.recovery { min-height: 44px; margin-bottom: 0; }
@media (max-width: 760px) { .quota-grid { grid-template-columns: 1fr; } }
</style>
