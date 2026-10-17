<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, shallowRef, watch } from 'vue'
import { NButton, NCard, NHeatmap, NSelect, NRadioGroup, NRadioButton, NTooltip, NPopover } from 'naive-ui'
import type { MemberApi } from '../types/member'
import type { ActivityDay, ActivityInvalidation, ActivityOperation, ActivitySnapshot } from '../types/activity'
import { activityDate, activityDateKey, activityDates, aggregateActivity, shanghaiDay } from '../utils/activity-display'

const props = withDefaults(defineProps<{
  api: MemberApi; endpoint: string; members?: { id: string; username: string }[]
  active?: boolean; refreshVersion?: number; invalidation?: ActivityInvalidation
}>(), { active: true, refreshVersion: 0 })
const operation = ref<ActivityOperation>('generate'), memberId = ref('')
const status = ref<'total' | 'done' | 'failed' | 'cancelled' | 'active'>('total')
const aggregation = ref<'day' | 'week' | 'month' | 'cumulative'>('day')
const aggregations = [{ label: '每天', value: 'day' }, { label: '每周', value: 'week' }, { label: '每月', value: 'month' }, { label: '累计', value: 'cumulative' }]
const colors = ['#252d39', '#163d60', '#165991', '#267fc2', '#68b7f5']
const selectedDate = ref(''), snapshot = shallowRef<ActivitySnapshot | null>(null)
const chart = ref<HTMLElement | null>(null), detailsOpen = ref(false)
const loading = ref(false), error = ref('')
let mounted = false, disposed = false, epoch = 0, fullPending = true, lastDay = '', scheduled = false
let task: Promise<void> | null = null
const pendingDates = new Set<string>()
const operations = [{ label: '生图', value: 'generate' }, { label: '放大', value: 'upscale' }, { label: 'Vibe 编码', value: 'encode-vibe' }]
const statuses = [{ label: '全部', value: 'total' }, { label: '成功', value: 'done' }, { label: '失败', value: 'failed' },
  { label: '取消', value: 'cancelled' }, { label: '在途与等待', value: 'active' }]
const memberOptions = computed(() => [{ label: '全部成员', value: '' }, ...(props.members || []).map(member => ({ label: member.username, value: member.id }))])
const days = computed(() => snapshot.value ? activityDates(snapshot.value.from, snapshot.value.to) : [])
const buckets = computed(() => new Map(snapshot.value?.days.map(day => [day.date, day]) || []))
function count(day: ActivityDay): number {
  return status.value === 'active' ? day.queued + day.pending + day.running : day[status.value]
}
function breakdown(day?: ActivityDay): string {
  if (!day) return '无保留记录，覆盖未知，不能推断为零次调用。'
  return `保留记录 ${day.total} 次 · 成功 ${day.done} · 失败 ${day.failed} · 取消 ${day.cancelled} · 排队 ${day.queued} · 等待前序图片 ${day.pending} · 生成中 ${day.running} · 其他 ${day.other}。记录可能不完整。`
}
const values = computed(() => new Map(days.value.map(date => [date, buckets.value.has(date) ? count(buckets.value.get(date)!) : 0])))
const cells = computed(() => {
  let total = 0
  return days.value.map(date => { const value = values.value.get(date) || 0; total += value
    return { timestamp: activityDate(date).getTime(), value: aggregation.value === 'cumulative' ? total : value } })
})
const periods = computed(() => aggregateActivity(days.value, values.value, aggregation.value === 'month' ? 'month' : 'week'))
const total = computed(() => [...values.value.values()].reduce((sum, value) => sum + value, 0))
const peak = computed(() => Math.max(1, ...periods.value.map(period => period.value)))
const dateOptions = computed(() => [...days.value].reverse().map(date => ({ label: date, value: date })))
const selected = computed(() => buckets.value.get(selectedDate.value))
const canRead = () => mounted && !disposed && props.active && !document.hidden

function request(dates: string[] | null = null) {
  if (dates === null || !snapshot.value || dates.some(date => date > snapshot.value!.to)) {
    fullPending = true; pendingDates.clear()
  } else if (!fullPending) {
    for (const date of dates) if (date >= snapshot.value.from && date <= snapshot.value.to) pendingDates.add(date)
  }
  // 同一轮通知先合并日期；不为每条 SSE 立即发出一次请求。
  if (scheduled) return
  scheduled = true
  queueMicrotask(() => {
    scheduled = false
    if (canRead() && !task && (fullPending || pendingDates.size)) void read()
  })
}
function read(): Promise<void> {
  if (task) return task
  task = (async () => {
    while (canRead() && (fullPending || pendingDates.size)) {
      const current = epoch, full = fullPending, dates = [...pendingDates].sort()
      fullPending = false; pendingDates.clear(); loading.value = true
      const query = new URLSearchParams({ operation: operation.value })
      if (props.members !== undefined && memberId.value) query.set('memberId', memberId.value)
      if (!full) query.set('dates', dates.join(','))
      try {
        const result = await props.api<ActivitySnapshot>(props.endpoint + (props.endpoint.includes('?') ? '&' : '?') + query)
        if (disposed || current !== epoch) continue
        if (!full && snapshot.value && (result.from !== snapshot.value.from || result.to !== snapshot.value.to)) {
          fullPending = true; pendingDates.clear(); continue
        }
        const initial = !snapshot.value
        if (full || !snapshot.value) snapshot.value = result
        else {
          // 空增量也必须删除这些日期的旧桶，避免清历史后留下旧计数。
          const replaced = new Set(dates)
          snapshot.value = { ...result, days: [...snapshot.value.days.filter(day => !replaced.has(day.date)),
            ...result.days.filter(day => replaced.has(day.date))].sort((a, b) => a.date.localeCompare(b.date)) }
        }
        if (selectedDate.value < result.from || selectedDate.value > result.to) selectedDate.value = result.to
        error.value = ''
        if (initial) { await nextTick(); if (!disposed && current === epoch && chart.value) chart.value.scrollLeft = chart.value.scrollWidth }
      } catch (failure) {
        if (disposed || current !== epoch) continue
        if (full) fullPending = true
        else for (const date of dates) pendingDates.add(date)
        error.value = '调用热力图读取失败，保留上次结果：' + (failure as Error).message
        break
      }
    }
  })().finally(() => { task = null; if (!disposed) loading.value = false })
  return task
}
function reset() {
  epoch++; snapshot.value = null; selectedDate.value = ''; error.value = ''
  request()
}
function calibrateDay() {
  if (!mounted) return
  const day = shanghaiDay()
  if (day !== lastDay) { lastDay = day; request() }
}
function visibility() { if (!document.hidden) { lastDay = shanghaiDay(); request() } }
watch([operation, memberId, () => props.endpoint, () => props.api], reset)
watch(() => props.members, members => { if (!members?.some(member => member.id === memberId.value)) memberId.value = '' })
watch(() => props.invalidation?.version, () => {
  if (!props.invalidation) return
  const day = shanghaiDay(), crossedDay = mounted && day !== lastDay
  lastDay = day
  request(crossedDay ? null : props.invalidation.dates)
}, { flush: 'sync' })
watch(() => props.refreshVersion, calibrateDay)
watch(aggregation, async () => { await nextTick(); if (chart.value) chart.value.scrollLeft = chart.value.scrollWidth })
watch(() => props.active, active => { if (active) { lastDay = shanghaiDay(); request() } })
onMounted(() => {
  mounted = true; lastDay = shanghaiDay()
  document.addEventListener('visibilitychange', visibility)
  request()
})
onBeforeUnmount(() => {
  disposed = true; epoch++; fullPending = false; pendingDates.clear(); snapshot.value = null
  document.removeEventListener('visibilitychange', visibility)
})
</script>

<template>
  <NCard id="callHeatmap" size="small" :aria-busy="loading">
    <header class="section-toolbar">
      <div><h2>调用活动 <span class="count-badge">{{ snapshot ? total.toLocaleString() + ' 次' : '—' }}</span></h2><p class="section-caption">最近一年 · 上海时间 · 每张生图计一次</p></div>
      <NRadioGroup v-model:value="aggregation" name="activity-aggregation" size="small" aria-label="热力图聚合方式"><NRadioButton v-for="item in aggregations" :key="item.value" :value="item.value">{{ item.label }}</NRadioButton></NRadioGroup>
    </header>
    <div class="heatmap-filters">
      <NSelect id="heatmapOperation" v-model:value="operation" :options="operations" aria-label="调用操作" size="small" />
      <NSelect id="heatmapStatus" v-model:value="status" :options="statuses" aria-label="任务状态" size="small" />
      <NSelect v-if="members !== undefined" id="heatmapMember" v-model:value="memberId" :options="memberOptions" filterable aria-label="统计成员" size="small" />
      <NButton id="retryHeatmap" :loading="loading" size="small" quaternary @click="request()">刷新</NButton>
    </div>
    <div class="refresh-status" role="status" :title="error">{{ error || (loading ? '正在读取调用记录…' : snapshot ? `${snapshot.from} — ${snapshot.to}` + (aggregation === 'cumulative' ? ' · 自范围起始日逐日累计' : '') : '等待记录') }}</div>
    <div class="heatmap-chart">
      <div v-if="aggregation === 'day' || aggregation === 'cumulative'" ref="chart" class="heatmap-scroll" role="region" aria-label="每日调用热力图，可横向滚动" tabindex="0">
        <NHeatmap :data="cells" :loading="!snapshot" :first-day-of-week="6" :minimum-color="colors[0]" :active-colors="colors.slice(1)" :x-gap="4" :y-gap="4" :theme-overrides="{ rectSizeMedium: 'clamp(11px, calc((100cqw - 250px) / 53), 18px)' }" tooltip>
          <template #tooltip="{ timestamp, value }">
            {{ activityDateKey(timestamp) }} · {{ aggregation === 'cumulative' ? '累计' : '当天' }} {{ value }} 次保留记录
            <p class="heatmap-tooltip-details">{{ breakdown(buckets.get(activityDateKey(timestamp))) }}</p>
          </template>
        </NHeatmap>
      </div>
      <div v-else class="period-grid" :class="{ monthly: aggregation === 'month' }" :aria-label="aggregation === 'month' ? '每月调用热力图' : '每周调用热力图'">
        <NTooltip v-for="period in periods" :key="period.from">
          <template #trigger><button type="button" class="period-cell" :style="{ background: colors[period.value ? Math.min(4, Math.ceil(period.value / peak * 4)) : 0] }" :aria-label="`${period.from} 至 ${period.to}：${period.value} 次保留记录`" @click="selectedDate = period.from; detailsOpen = true"><span>{{ aggregation === 'month' ? period.from.slice(0, 7) : period.from.slice(5) }}</span><strong>{{ period.value.toLocaleString() }}</strong></button></template>
          {{ period.from }} 至 {{ period.to }} · {{ period.value }} 次保留记录
        </NTooltip>
      </div>
    </div>
    <div class="heatmap-footer">
      <span class="section-caption">仅统计保留记录（含缓存）· 灰格不代表历史完整</span>
      <NPopover v-model:show="detailsOpen" trigger="click" :style="{ width: 'min(390px, calc(100vw - 32px))' }">
        <template #trigger><NButton id="heatmapDetailToggle" size="small" quaternary>查看日期明细</NButton></template>
        <p class="section-caption">包含缓存任务，不等于实际 NAI 调用次数。灰格表示无所选状态的保留记录；历史覆盖可能不完整。</p>
        <NSelect v-if="snapshot" id="heatmapDay" v-model:value="selectedDate" :options="dateOptions" filterable size="small" aria-label="查看某一天" />
        <p id="heatmapDetails" class="section-caption" aria-live="polite">{{ selectedDate }}{{ selectedDate ? '：' : '' }}{{ breakdown(selected) }}</p>
      </NPopover>
    </div>
  </NCard>
</template>

<style scoped>
.heatmap-filters { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-top: 18px; }
.heatmap-filters .n-select { width: 145px; max-width: 100%; }
.heatmap-chart { min-height: 170px; }
.heatmap-scroll { max-width: 100%; overflow-x: auto; padding: 6px 0; container-type: inline-size; }
.heatmap-scroll :deep(.n-heatmap) { min-width: 830px; width: 100%; max-width: none; }
.heatmap-scroll :deep(.n-heatmap__calendar-table) { width: 100%; }
.heatmap-scroll :deep(.n-heatmap-rect) { border-radius: 3px; }
.heatmap-footer { display: flex; align-items: center; justify-content: space-between; gap: 16px; flex-wrap: wrap; margin-top: 8px; }
#heatmapDay { width: 150px; }
.period-grid { display: grid; grid-template-columns: repeat(14, minmax(0, 1fr)); gap: 5px; }
.period-grid.monthly { grid-template-columns: repeat(6, minmax(0, 1fr)); }
.period-cell { border: 1px solid #ffffff0a; border-radius: 5px; color: #e8f1fc; cursor: pointer; padding: 5px 2px; min-height: 38px; }
.period-cell span { display: block; font-size: 10px; opacity: .8; }
.period-cell strong { font-size: 13px; font-variant-numeric: tabular-nums; }
.monthly .period-cell { min-height: 74px; }
.monthly .period-cell strong { font-size: 20px; }
@media(max-width: 700px) { .period-grid { grid-template-columns: repeat(7, minmax(0, 1fr)); } .period-grid.monthly { grid-template-columns: repeat(3, minmax(0, 1fr)); } }
</style>
