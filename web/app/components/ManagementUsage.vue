<script setup lang="ts">
import { computed, h, ref } from 'vue'
import { NButton, NCard, NDataTable, NEmpty, NInput, NSelect, NTag, type DataTableColumns } from 'naive-ui'
import type { ManagementUsage } from '../types/management'
import { date, jobStatus, number } from '../utils/member-display'

const props = defineProps<{ data: ManagementUsage | null; days: number; loading: boolean }>()
const emit = defineEmits<{ 'update:days': [value: number]; retry: [] }>()
const dimension = ref<'members' | 'models' | 'days'>('members'), search = ref('')
const ranges = [{ label: '今天', value: 1 }, { label: '近 7 天', value: 7 }, { label: '近 30 天', value: 30 }, { label: '近一年', value: 365 }, { label: '全部保留记录', value: 0 }]
const dimensions = [{ label: '按成员', value: 'members' }, { label: '按模型', value: 'models' }, { label: '按日期', value: 'days' }]
const totals = computed(() => props.data?.totals)
const rows = computed(() => props.data?.[dimension.value] || [])
const recent = computed(() => (props.data?.recent || []).filter(job => [job.username, job.model, job.id].some(value => value?.toLowerCase().includes(search.value.trim().toLowerCase()))))
const pagination = { pageSize: 10, showSizePicker: true, pageSizes: [10, 25, 50, 100], showQuickJumper: true }
const duration = (value: number | null) => value == null ? '未知' : `${number(value)} 秒`
type Summary = ManagementUsage['members'][number]
type Job = ManagementUsage['recent'][number]
function charge(job: Job) {
  const settlements: Record<string, string> = { settled: '已结算', refunded: '已退款（本站额度）', reserved: '预扣' }
  const amount = job.resource === 'free' ? '免费' : job.resource === 'v5' ? `${number(job.amount)} V5`
    : job.resource === 'anlas' ? `订阅 ${number(job.fixed)} / 购买 ${number(job.purchased)} Anlas` : '未扣款'
  return [settlements[job.chargeStatus || ''], amount, job.errorStatus ? `HTTP ${job.errorStatus}` : ''].filter(Boolean).join(' · ')
}
const summaryColumns: DataTableColumns<Summary> = [
  { title: '成员 / 模型 / 日期', key: 'label', sorter: 'default', width: 210, ellipsis: { tooltip: true } },
  ...(['total', 'done', 'failed', 'free', 'v5', 'fixed', 'purchased', 'averageWaitSeconds', 'averageGenerationSeconds'] as const).map((key, index) => ({
    title: ['总数', '成功', '失败', '免费', 'V5', '订阅 Anlas', '购买 Anlas', '平均排队', '平均生成'][index], key, width: index < 5 ? 90 : 125,
    align: 'right' as const, sorter: (a: Summary, b: Summary) => (a[key] ?? -1) - (b[key] ?? -1),
    render: (row: Summary) => key.startsWith('average') ? duration(row[key]) : number(row[key])
  }))
]
const jobColumns = computed<DataTableColumns<Job>>(() => [
  { title: '提交时间', key: 'createdAt', sorter: 'default', width: 180, render: row => date(row.createdAt) },
  { title: '成员', key: 'username', sorter: 'default', width: 130,
    filterOptions: [...new Set(props.data?.recent.map(row => row.username))].map(value => ({ label: value, value })), filter: (value, row) => row.username === value },
  { title: '模型', key: 'model', sorter: 'default', width: 230, ellipsis: { tooltip: true },
    filterOptions: [...new Set(props.data?.recent.map(row => row.model))].map(value => ({ label: value, value })), filter: (value, row) => row.model === value },
  { title: '状态', key: 'status', width: 105, sorter: 'default', filterOptions: ['done', 'failed', 'running', 'queued', 'pending'].map(value => ({ label: jobStatus(value), value })),
    filter: (value, row) => row.status === value, render: row => h(NTag, { size: 'small', bordered: false, type: row.status === 'done' ? 'success' : row.status === 'failed' ? 'error' : 'info' }, () => jobStatus(row.status)) },
  { title: '排队', key: 'waitSeconds', width: 110, align: 'right', sorter: (a, b) => (a.waitSeconds ?? -1) - (b.waitSeconds ?? -1), render: row => duration(row.waitSeconds) },
  { title: '生成', key: 'generationSeconds', width: 110, align: 'right', sorter: (a, b) => (a.generationSeconds ?? -1) - (b.generationSeconds ?? -1), render: row => duration(row.generationSeconds) },
  { title: '额度记录', key: 'chargeStatus', width: 280, render: charge }
])
</script>

<template>
  <section id="usagePanel" aria-labelledby="usageTitle" :aria-busy="loading" class="usage-panel">
    <header class="section-toolbar">
      <div><h2 id="usageTitle">用量概览</h2><p class="section-caption">按上海提交日统计 · 每张图片计一次 · V5 为估算百分点</p></div>
      <div class="toolbar-controls"><NSelect id="usageDays" :value="days" :options="ranges" aria-label="时间范围" @update:value="emit('update:days', $event)" /><NButton id="retryUsage" :loading="loading" @click="emit('retry')">刷新</NButton></div>
    </header>
    <div class="refresh-status" role="status">{{ loading ? data ? '正在更新 · 以下为上次读取的结果' : '正在读取用量…' : data ? '更新于 ' + date(data.until) : '尚未读取用量统计，请重新读取。' }}</div>
    <NEmpty v-if="!data && !loading" description="暂无可显示的统计" />
    <template v-if="data && totals">
      <div class="usage-cards">
        <NCard size="small"><span class="metric-label">成功 / 总请求</span><strong id="usageDone">{{ totals.done }} / {{ totals.total }}</strong><p id="usageFree">其中免费成功 {{ totals.free }} 次</p></NCard>
        <NCard size="small"><span class="metric-label">失败 / 本站退款</span><strong id="usageFailed">{{ totals.failed }} / {{ totals.refunded }}</strong><p id="usageActive">生成中 {{ totals.running }} · 排队 {{ totals.queued }} · 等待前序图片 {{ totals.pending }}</p></NCard>
        <NCard size="small"><span class="metric-label">V5 消耗（百分点）</span><strong id="usageV5">{{ number(totals.v5) }}</strong><p id="usageWait">平均排队 {{ duration(totals.averageWaitSeconds) }}</p></NCard>
        <NCard size="small"><span class="metric-label">Anlas 消耗</span><strong id="usageAnlas">{{ number(totals.fixed + totals.purchased) }}</strong><p id="usagePools">订阅 {{ number(totals.fixed) }} · 购买 {{ number(totals.purchased) }}</p></NCard>
      </div>
      <NCard size="small" class="data-section">
        <header class="section-toolbar"><div><h3>用量汇总</h3><p class="section-caption">点击列标题排序</p></div><NSelect id="usageGroup" v-model:value="dimension" :options="dimensions" aria-label="统计维度" /></header>
        <NDataTable id="usageRows" :key="dimension" :columns="summaryColumns" :data="rows" :row-key="(row: Summary) => row.id" :pagination="pagination" :scroll-x="1220" size="small"><template #empty>所选范围暂无记录</template></NDataTable>
        <p class="section-caption">平均排队统计已派发任务；平均生成仅统计成功任务的上游耗时。未知耗时不按零计算。</p>
      </NCard>
      <NCard size="small" class="data-section">
        <header class="section-toolbar"><div><h3>请求记录 <span class="count-badge">{{ data.recent.length }}</span></h3><p class="section-caption">所选时间范围内全部保留记录 · 表头可筛选与排序</p></div><NInput v-model:value="search" clearable placeholder="搜索成员、模型或任务 ID" aria-label="搜索请求记录" class="table-search" /></header>
        <NDataTable id="usageRecent" :columns="jobColumns" :data="recent" :row-key="(row: Job) => row.id" :pagination="pagination" :scroll-x="1145" size="small"><template #empty>所选范围暂无请求</template></NDataTable>
        <p class="section-caption">失败退款不计入消耗。“已退款”指本站额度已退回，不代表上游已退款。</p>
      </NCard>
    </template>
  </section>
</template>

<style scoped>
.usage-panel { min-width: 0; }
#usageDays { width: 170px; } #usageGroup { width: 140px; }
.usage-cards { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-bottom: 24px; }
.metric-label { color: var(--text-muted); font-size: 13px; }
.usage-cards strong { display: block; margin: 10px 0; font-size: 26px; font-variant-numeric: tabular-nums; line-height: 1.2; }
.usage-cards p { margin: 0; font-size: 12px; color: var(--text-muted); }
.data-section + .data-section { margin-top: 20px; }
.data-section .section-toolbar { margin-bottom: 16px; }
@media (max-width: 1000px) { .usage-cards { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 450px) { .usage-cards { gap: 8px; } .usage-cards strong { font-size: 22px; } }
</style>
