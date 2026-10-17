<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { NAlert, NCard } from 'naive-ui'
import type { SharedStatus } from '../types/member'
import { date, number } from '../utils/member-display'

const props = defineProps<{ state: SharedStatus | null; connection: string }>()
const labels: Record<string, string> = { pending_opus: '等待 Opus', inactive_opus: '订阅不可用', stale: '余额已过期', active: '可用', cooldown: '等待派发', quota_sync_error: '同步异常', disabled: '账户停用', unknown_balance: '余额未知' }
const connections: Record<string, string> = { polling: '私有管理页可见时每 30 秒读取，也可手动刷新', connecting: '正在连接共享状态…', open: '状态连接已建立', retrying: '状态连接中断，正在重连；以下为上次接收的数据', closed: '状态连接已关闭', invalid: '状态数据无效，请刷新页面' }
const reasons: Record<string, string> = { success_gap: '上一张已完成，等待生成间隔', ordinary_failure: '生成失败，短暂等待后继续', capacity_limit: '上游并发或容量限制，等待后继续', upstream_retry_after: '按上游要求等待后继续', completion_gap: '任务已结束，等待生成间隔' }
const now = ref<number | null>(null)
let timer: ReturnType<typeof setInterval> | null = null
let mounted = false
let pageActive = true
const deadline = computed(() => Date.parse(props.state?.cooldownUntil || ''))
const cooldown = computed(() => {
  if (!props.state) return '等待共享状态'
  if (!Number.isFinite(deadline.value)) return '当前无冷却'
  if (now.value === null) return '正在读取等待时间'
  const seconds = Math.max(0, Math.ceil((deadline.value - now.value) / 1000))
  if (!seconds) return '等待期已结束，等待状态更新'
  return `${reasons[props.state.cooldownReason || ''] || '共享账户等待派发'} · 剩余 ${seconds >= 60 ? `${Math.floor(seconds / 60)} 分 ` : ''}${seconds % 60} 秒 · 至 ${date(props.state.cooldownUntil)}`
})
const problems = computed(() => {
  const s = props.state
  if (!s) return []
  return [s.quotaSyncError && '官方余额同步异常', s.stale && (s.officialCheckedAt ? '余额数据已过期，请勿作为当前余量' : '尚无共享 Opus 的官方余额数据'), s.recentFailures > 0 && `最近一小时失败 ${number(s.recentFailures)} 次`].filter(Boolean)
})
function stopTimer() {
  if (timer !== null) clearInterval(timer)
  timer = null
}
function tick() {
  now.value = Date.now()
  if (!Number.isFinite(deadline.value) || deadline.value <= now.value) stopTimer()
}
function resume() {
  stopTimer()
  if (!mounted || !pageActive || document.hidden) return
  tick()
  if (deadline.value > (now.value ?? 0)) timer = setInterval(tick, 1000)
}
function pageHide() { pageActive = false; stopTimer() }
function pageShow() { pageActive = true; resume() }
watch(() => props.state?.cooldownUntil, resume)
onMounted(() => {
  mounted = true
  document.addEventListener('visibilitychange', resume)
  window.addEventListener('pagehide', pageHide)
  window.addEventListener('pageshow', pageShow)
  resume()
})
onUnmounted(() => {
  mounted = false
  stopTimer()
  document.removeEventListener('visibilitychange', resume)
  window.removeEventListener('pagehide', pageHide)
  window.removeEventListener('pageshow', pageShow)
})
</script>

<template>
  <section aria-labelledby="sharedTitle">
    <h2 id="sharedTitle">共享账户状态 <small>只读</small></h2>
    <p id="sharedConnection" role="status" class="muted">{{ connections[connection] || '等待连接' }}</p>
    <div class="shared-grid">
      <NCard title="订阅与队列" size="small">
        <strong id="sharedState" class="amount">{{ state ? labels[state.status] || '状态未知' : '未知' }}</strong>
        <p id="sharedSubscription">{{ state ? (state.subscriptionActive && state.status !== 'inactive_opus' ? `Opus 有效 · 到期 ${date(state.expiresAt)}` : '没有可用的 Opus 订阅') : '订阅状态未知' }}</p>
        <p id="sharedQueue">生成中 {{ number(state?.running) }} / {{ number(state?.concurrencyLimit) }} · 排队 {{ number(state?.queued) }} · 批次待激活 {{ number(state?.pending) }}</p>
        <p id="sharedCooldown">{{ cooldown }}</p>
      </NCard>
      <NCard title="公共 V5 余量" size="small">
        <strong id="sharedV5" class="amount">{{ state?.v5Percent == null ? '未知' : `${number(state.v5Percent)}%${state.v5IsNegative ? '（已透支）' : ''}` }}</strong>
        <p id="sharedRefill">{{ state?.refillSecondsPerPercent != null && state.refillSecondsPerPercent > 0 ? `约 ${number(86400 / state.refillSecondsPerPercent)} 个百分点 / 天` : '恢复速度未知或已暂停' }}</p>
        <p class="muted">显示官方最近一次查询值，不等同于你的个人余额。</p>
      </NCard>
      <NCard title="公共 Anlas" size="small">
        <strong id="sharedFixed" class="amount">{{ number(state?.anlasFixed) }} 订阅</strong>
        <p id="sharedPurchased">购买 Anlas：{{ number(state?.anlasPurchased) }}</p>
        <p class="muted">订阅与购买额度独立记录。</p>
      </NCard>
    </div>
    <p id="sharedUpdated" class="muted">官方余额更新：{{ date(state?.officialCheckedAt) }}。每 60 秒查询，生成结束后也刷新；{{ connection === 'polling' ? '此私有页面通过定时读取更新。' : '队列与账本提交后合并推送，通常在一秒内更新。' }}</p>
    <NAlert v-if="problems.length" id="sharedError" type="warning" role="status" :show-icon="false">{{ problems.join(' · ') }}</NAlert>
  </section>
</template>

<style scoped>
h2 { font-size: 1.25rem; margin: 0; }
small { color: #adb7c6; font-size: .8rem; font-weight: 400; }
p { line-height: 1.7; overflow-wrap: anywhere; }
.muted { color: #adb7c6; }
.shared-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 16px; }
.amount { font-size: 1.5rem; font-variant-numeric: tabular-nums; overflow-wrap: anywhere; }
@media (max-width: 760px) { .shared-grid { grid-template-columns: 1fr; } }
</style>
