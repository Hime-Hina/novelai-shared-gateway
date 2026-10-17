<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, shallowRef, watch } from 'vue'
import { NAlert, NButton, NCard, NHeatmap, NSelect } from 'naive-ui'
import type { MemberApi } from '../types/member'
import type { ActivityDay, ActivityInvalidation, ActivityOperation, ActivitySnapshot } from '../types/activity'
import { activityDate, activityDateKey, activityDates, taipeiDay } from '../utils/activity-display'

const props = withDefaults(defineProps<{
  api: MemberApi; endpoint: string; members?: { id: string; username: string }[]
  active?: boolean; refreshVersion?: number; invalidation?: ActivityInvalidation
}>(), { active: true, refreshVersion: 0 })
const operation = ref<ActivityOperation>('generate'), memberId = ref('')
const status = ref<'total' | 'done' | 'failed' | 'cancelled' | 'active'>('total')
const selectedDate = ref(''), snapshot = shallowRef<ActivitySnapshot | null>(null)
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
const cells = computed(() => days.value.map(date => ({ timestamp: activityDate(date).getTime(), value: buckets.value.has(date) ? count(buckets.value.get(date)!) : null })))
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
        if (full || !snapshot.value) snapshot.value = result
        else {
          // 空增量也必须删除这些日期的旧桶，避免清历史后留下旧计数。
          const replaced = new Set(dates)
          snapshot.value = { ...result, days: [...snapshot.value.days.filter(day => !replaced.has(day.date)),
            ...result.days.filter(day => replaced.has(day.date))].sort((a, b) => a.date.localeCompare(b.date)) }
        }
        if (selectedDate.value < result.from || selectedDate.value > result.to) selectedDate.value = result.to
        error.value = ''
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
  const day = taipeiDay()
  if (day !== lastDay) { lastDay = day; request() }
}
function visibility() { if (!document.hidden) { lastDay = taipeiDay(); request() } }
watch([operation, memberId, () => props.endpoint, () => props.api], reset)
watch(() => props.members, members => { if (!members?.some(member => member.id === memberId.value)) memberId.value = '' })
watch(() => props.invalidation?.version, () => {
  if (!props.invalidation) return
  const day = taipeiDay(), crossedDay = mounted && day !== lastDay
  lastDay = day
  request(crossedDay ? null : props.invalidation.dates)
}, { flush: 'sync' })
watch(() => props.refreshVersion, calibrateDay)
watch(() => props.active, active => { if (active) { lastDay = taipeiDay(); request() } })
onMounted(() => {
  mounted = true; lastDay = taipeiDay()
  document.addEventListener('visibilitychange', visibility)
  request()
})
onBeforeUnmount(() => {
  disposed = true; epoch++; fullPending = false; pendingDates.clear(); snapshot.value = null
  document.removeEventListener('visibilitychange', visibility)
})
</script>

<template>
  <NCard id="callHeatmap" title="调用热力图" size="small" :aria-busy="loading">
    <p class="heatmap-note">按台北提交日统计最近一个日历年，每张生图计一次；放大和 Vibe 编码单独查看。包含缓存任务，不等于实际 NAI 调用次数。</p>
    <div class="heatmap-filters">
      <NSelect id="heatmapOperation" v-model:value="operation" :options="operations" aria-label="调用操作" />
      <NSelect id="heatmapStatus" v-model:value="status" :options="statuses" aria-label="任务状态" />
      <NSelect v-if="members !== undefined" id="heatmapMember" v-model:value="memberId" :options="memberOptions" filterable aria-label="统计成员" />
      <NButton id="retryHeatmap" :loading="loading" @click="request()">重新读取</NButton>
    </div>
    <NAlert v-if="error" type="warning" role="status" class="heatmap-error">{{ error }}</NAlert>
    <p v-if="loading" role="status">正在读取{{ snapshot ? '，图中暂为上次结果' : '调用记录' }}…</p>
    <template v-if="snapshot">
      <p class="heatmap-note">{{ snapshot.from }} 至 {{ snapshot.to }}。颜色只代表当前保留记录；空白表示无保留记录或覆盖未知，不能推断为零次调用。</p>
      <div class="heatmap-scroll" role="region" aria-label="每日调用热力图，可横向滚动" tabindex="0">
        <NHeatmap :data="cells" :first-day-of-week="6" tooltip>
          <template #tooltip="{ timestamp, value }">
            {{ activityDateKey(timestamp) }} · {{ value == null ? '无保留记录，覆盖未知' : `所选状态 ${value} 次（仅保留记录）` }}
            <p v-if="value != null" class="heatmap-tooltip-details">{{ breakdown(buckets.get(activityDateKey(timestamp))) }}</p>
          </template>
        </NHeatmap>
      </div>
      <div class="heatmap-day">
        <span id="heatmapDayLabel">查看某一天</span>
        <NSelect id="heatmapDay" v-model:value="selectedDate" :options="dateOptions" filterable aria-labelledby="heatmapDayLabel" />
      </div>
      <p id="heatmapDetails" aria-live="polite">
        {{ selectedDate }}：{{ breakdown(selected) }}
      </p>
    </template>
  </NCard>
</template>

<style scoped>
.heatmap-filters { display: flex; flex-wrap: wrap; align-items: center; gap: 12px; }
.heatmap-filters .n-select { width: 170px; max-width: 100%; }
.heatmap-note { color: #adb7c6; }
.heatmap-error { margin-top: 12px; }
.heatmap-scroll { max-width: 100%; overflow-x: auto; padding: 8px 0; }
.heatmap-scroll :deep(.n-heatmap) { min-width: 800px; }
.heatmap-day { display: flex; align-items: center; flex-wrap: wrap; gap: 12px; margin-top: 16px; }
#heatmapDay { width: 220px; max-width: 100%; }
p { line-height: 1.7; overflow-wrap: anywhere; }
</style>
