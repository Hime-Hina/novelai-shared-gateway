<script setup lang="ts">
import { computed, h, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { NButton, NCard, NDataTable, NInput, NSelect, NTag, type DataTableColumns, type DataTableSortState } from 'naive-ui'
import MemberBatch from './MemberBatch.vue'
import type { LateResult, MemberApi, MemberJob } from '../types/member'
import { chargeText, date, jobStatus, resultUrl } from '../utils/member-display'

const props = defineProps<{ jobs: MemberJob[]; lateResults: LateResult[]; api: MemberApi; refreshVersion: number }>()
const emit = defineEmits<{ changed: [] }>()
type HistoryPage = { jobs: MemberJob[]; total: number; until: string }
const snapshot = ref<HistoryPage | null>(null), page = ref(1), pageSize = ref(10), search = ref(''), query = ref(''), status = ref('')
const sort = ref('createdAt'), order = ref<'asc' | 'desc'>('desc'), loading = ref(false), error = ref('')
let disposed = false, version = 0, until = '', refreshPending = false
const jobs = computed(() => snapshot.value?.jobs || props.jobs.slice(0, pageSize.value))
const requests = computed(() => [...new Set(jobs.value.map(job => job.requestId).filter((id): id is string => Boolean(id)))])
const statuses = [{ label: '全部状态', value: '' }, ...['done', 'failed', 'queued', 'pending', 'running'].map(value => ({ label: jobStatus(value), value })), { label: '有迟到结果', value: 'late' }]
async function load() {
  const current = ++version
  loading.value = true
  const params = new URLSearchParams({ page: String(page.value), pageSize: String(pageSize.value), q: query.value, status: status.value, sort: sort.value, order: order.value })
  if (page.value > 1 && until) params.set('until', until)
  try {
    const result = await props.api<HistoryPage>('history?' + params)
    if (disposed || current !== version) return
    snapshot.value = result; until = result.until; error.value = ''
    const last = Math.max(1, Math.ceil(result.total / pageSize.value))
    if (page.value > last) page.value = last
  } catch (failure) {
    if (!disposed && current === version && !(failure as { stale?: boolean }).stale) error.value = '历史读取失败，请刷新重试：' + (failure as Error).message
  } finally {
    if (!disposed && current === version) {
      loading.value = false
      if (refreshPending && !document.hidden) { refreshPending = false; void load() }
    }
  }
}
function filter() { page.value = 1; until = ''; query.value = search.value.trim() }
function sorted(value: DataTableSortState | DataTableSortState[] | null) {
  const state = Array.isArray(value) ? value[0] : value
  sort.value = state?.order ? String(state.columnKey) : 'createdAt'; order.value = state?.order === 'ascend' ? 'asc' : 'desc'; page.value = 1
}
const columns = computed<DataTableColumns<MemberJob>>(() => [
  { title: '提交时间', key: 'createdAt', width: 185, sorter: true, sortOrder: sort.value === 'createdAt' ? order.value === 'asc' ? 'ascend' : 'descend' : false, render: row => date(row.createdAt) },
  { title: '模型', key: 'model', width: 240, sorter: true, sortOrder: sort.value === 'model' ? order.value === 'asc' ? 'ascend' : 'descend' : false, ellipsis: { tooltip: true } },
  { title: '状态 / 结果', key: 'status', width: 225, sorter: true, sortOrder: sort.value === 'status' ? order.value === 'asc' ? 'ascend' : 'descend' : false,
    render: row => h('div', { class: 'result-cell' }, [h(NTag, { size: 'small', bordered: false, type: row.status === 'done' ? 'success' : row.status === 'failed' ? 'error' : 'info' }, () => jobStatus(row.status)),
      resultUrl(row.imageUrl) ? h('a', { href: resultUrl(row.imageUrl)!, target: '_blank', rel: 'noopener noreferrer', referrerpolicy: 'no-referrer' }, row.lateResultAvailable ? '领取迟到结果（退款保留）' : '领取结果') : null]) },
  { title: '额度记录', key: 'charge', width: 260, render: row => chargeText(row.charge) }
])
watch([page, pageSize, query, status, sort, order], () => void load())
watch([pageSize, status], () => { page.value = 1; until = '' }, { flush: 'sync' })
watch(() => props.refreshVersion, () => { if (!document.hidden) { if (loading.value) refreshPending = true; else void load() } })
onMounted(() => void load())
onBeforeUnmount(() => { disposed = true; version++ })
</script>

<template>
  <NCard title="调用记录" size="small">
    <template #header-extra><span class="count-badge">{{ snapshot?.total ?? '—' }} 条保留记录</span></template>
    <form class="section-toolbar calls-filters" @submit.prevent="filter">
      <div class="toolbar-controls"><NInput v-model:value="search" clearable placeholder="搜索模型或任务 ID" aria-label="搜索调用记录" class="table-search" /><NButton attr-type="submit">搜索</NButton></div>
      <div class="toolbar-controls"><NSelect v-model:value="status" :options="statuses" aria-label="筛选调用状态" class="status-filter" /><NButton :loading="loading" @click="load">刷新</NButton></div>
    </form>
    <div class="refresh-status" role="status" :title="error">{{ error || (loading ? '正在更新记录…' : status === 'late' ? '迟到结果领取不另扣费，原退款保留。' : '全部保留历史可翻页查看；列标题可排序。') }}</div>
    <NDataTable id="jobs" remote :columns="columns" :data="jobs" :row-key="(row: MemberJob) => row.id" :scroll-x="910" size="small"
      :pagination="{ page, pageSize, itemCount: snapshot?.total ?? jobs.length, showSizePicker: true, pageSizes: [10, 25, 50, 100], showQuickJumper: true }"
      @update:page="page = $event" @update:page-size="pageSize = $event" @update:sorter="sorted"><template #empty>暂无调用记录</template></NDataTable>
    <div v-if="requests.length" class="request-list">
      <h3>本页相关批次</h3>
      <MemberBatch v-for="requestId in requests" :key="requestId" :request-id="requestId" :api="api" :refresh-version="refreshVersion" @changed="emit('changed')" />
    </div>
    <p class="section-caption">“已退款”指本站额度已退回，不代表上游已退款。所有成员共用串行队列；生成结束后至少间隔 10 秒派发下一张，官方限流时自动退避。</p>
  </NCard>
</template>

<style scoped>
.status-filter { width: 160px; }
.request-list { display: grid; gap: 10px; margin-top: 24px; }
:deep(.result-cell) { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
</style>
