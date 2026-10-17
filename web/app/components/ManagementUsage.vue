<script setup lang="ts">
import { computed, ref } from 'vue'
import { NButton, NCard, NEmpty, NSelect, NTable } from 'naive-ui'
import type { ManagementUsage } from '../types/management'
import { date, jobStatus, number } from '../utils/member-display'

const props = defineProps<{ data: ManagementUsage | null; days: 1 | 7 | 30; loading: boolean }>()
const emit = defineEmits<{ 'update:days': [value: 1 | 7 | 30]; retry: [] }>()
const dimension = ref<'members' | 'models' | 'days'>('members')
const ranges = [{ label: '今天', value: 1 }, { label: '近 7 天', value: 7 }, { label: '近 30 天', value: 30 }]
const dimensions = [{ label: '按成员', value: 'members' }, { label: '按模型', value: 'models' }, { label: '按日期', value: 'days' }]
const totals = computed(() => props.data?.totals)
const rows = computed(() => props.data?.[dimension.value] || [])
const recent = computed(() => props.data?.recent.slice(0, 50) || [])
const duration = (value: number | null) => value == null ? '未知' : `${number(value)} 秒`
function charge(job: ManagementUsage['recent'][number]) {
  const settlements: Record<string, string> = { settled: '已结算', refunded: '已退款（本站额度）', reserved: '预扣' }
  const amount = job.resource === 'free' ? '免费' : job.resource === 'v5' ? `${number(job.amount)} V5`
    : job.resource === 'anlas' ? `订阅 ${number(job.fixed)} / 购买 ${number(job.purchased)} Anlas` : '未扣款'
  return [settlements[job.chargeStatus || ''], amount, job.errorStatus ? `HTTP ${job.errorStatus}` : ''].filter(Boolean).join(' · ')
}
</script>

<template>
  <section id="usagePanel" aria-labelledby="usageTitle" :aria-busy="loading">
    <header class="usage-toolbar">
      <div>
        <h2 id="usageTitle">用量与记录</h2>
        <p class="muted">按台北时间统计已受理的单图及辅助任务；失败退款不计入消耗。V5 消耗为网关估算。</p>
      </div>
      <div class="usage-controls">
        <label id="usageDaysLabel">时间范围</label>
        <NSelect id="usageDays" :value="days" :options="ranges" aria-labelledby="usageDaysLabel" @update:value="emit('update:days', $event)" />
        <NButton id="retryUsage" :loading="loading" @click="emit('retry')">重新读取</NButton>
      </div>
    </header>
    <p v-if="loading" role="status" class="muted">{{ data ? '正在更新用量，以下为上次读取的结果。' : '正在读取用量…' }}</p>
    <NEmpty v-if="!data && !loading" description="尚未读取用量统计，请重新读取。" />
    <template v-if="data && totals">
      <div class="usage-cards">
        <NCard title="成功 / 总请求" size="small">
          <strong id="usageDone">{{ totals.done }} / {{ totals.total }}</strong>
          <p id="usageFree">其中免费成功 {{ totals.free }} 次</p>
        </NCard>
        <NCard title="失败 / 退款" size="small">
          <strong id="usageFailed">{{ totals.failed }} / {{ totals.refunded }}</strong>
          <p id="usageActive">生成中 {{ totals.running }} · 排队 {{ totals.queued }} · 等待前序图片 {{ totals.pending }}</p>
        </NCard>
        <NCard title="V5 消耗（百分点）" size="small">
          <strong id="usageV5">{{ number(totals.v5) }}</strong>
          <p id="usageWait">平均排队 {{ duration(totals.averageWaitSeconds) }}</p>
        </NCard>
        <NCard title="Anlas 消耗" size="small">
          <strong id="usageAnlas">{{ number(totals.fixed + totals.purchased) }}</strong>
          <p id="usagePools">订阅 {{ number(totals.fixed) }} · 购买 {{ number(totals.purchased) }}</p>
        </NCard>
      </div>
      <div class="usage-toolbar summary-heading">
        <h3>用量汇总</h3>
        <NSelect id="usageGroup" v-model:value="dimension" :options="dimensions" aria-label="统计维度" />
      </div>
      <div class="usage-scroll" role="region" aria-label="用量汇总，可横向滚动" tabindex="0">
        <NTable :single-line="false" class="usage-summary">
          <thead><tr><th scope="col">成员 / 模型 / 日期</th><th scope="col">总数</th><th scope="col">成功</th><th scope="col">失败</th><th scope="col">免费</th><th scope="col">V5</th><th scope="col">订阅 Anlas</th><th scope="col">购买 Anlas</th><th scope="col">平均排队</th><th scope="col">平均生成</th></tr></thead>
          <tbody id="usageRows">
            <tr v-for="row in rows" :key="row.id">
              <th scope="row">{{ row.label }}</th><td>{{ row.total }}</td><td>{{ row.done }}</td><td>{{ row.failed }}</td><td>{{ row.free }}</td>
              <td>{{ number(row.v5) }}</td><td>{{ number(row.fixed) }}</td><td>{{ number(row.purchased) }}</td>
              <td>{{ duration(row.averageWaitSeconds) }}</td><td>{{ duration(row.averageGenerationSeconds) }}</td>
            </tr>
            <tr v-if="!rows.length"><td colspan="10">所选范围暂无记录</td></tr>
          </tbody>
        </NTable>
      </div>
      <p class="muted">平均排队统计已派发任务；平均生成仅统计成功任务的上游耗时。未知耗时不按零计算。</p>
      <h3>所选范围内最近 50 条请求</h3>
      <div class="usage-scroll" role="region" aria-label="最近请求记录，可横向滚动" tabindex="0">
        <NTable :single-line="false" class="usage-recent">
          <thead><tr><th scope="col">时间</th><th scope="col">成员</th><th scope="col">模型</th><th scope="col">状态</th><th scope="col">排队</th><th scope="col">生成</th><th scope="col">额度记录</th></tr></thead>
          <tbody id="usageRecent">
            <tr v-for="job in recent" :key="job.id">
              <td>{{ date(job.createdAt) }}</td><td>{{ job.username }}</td><td>{{ job.model }}</td><td>{{ jobStatus(job.status) }}</td>
              <td>{{ duration(job.waitSeconds) }}</td><td>{{ duration(job.generationSeconds) }}</td><td>{{ charge(job) }}</td>
            </tr>
            <tr v-if="!recent.length"><td colspan="7">所选范围暂无请求</td></tr>
          </tbody>
        </NTable>
      </div>
      <p class="muted">统计更新至 {{ date(data.until) }}。“已退款”指本站额度已退回，不代表上游已退款。</p>
    </template>
  </section>
</template>

<style scoped>
h2, h3 { margin: 0; }
p { line-height: 1.7; }
.muted { color: #adb7c6; }
.usage-toolbar { display: flex; align-items: center; justify-content: space-between; gap: 16px; flex-wrap: wrap; }
.usage-controls { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
#usageDays, #usageGroup { width: 150px; }
.usage-cards { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin: 20px 0; }
.usage-cards strong { font-size: 24px; font-variant-numeric: tabular-nums; }
.usage-cards p { margin-bottom: 0; }
.summary-heading { margin-bottom: 12px; }
.usage-scroll { max-width: 100%; overflow-x: auto; margin: 12px 0; }
.usage-summary { min-width: 980px; }
.usage-recent { min-width: 900px; }
td, th, p { overflow-wrap: anywhere; }
@media (max-width: 1000px) { .usage-cards { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 560px) { .usage-cards { grid-template-columns: 1fr; } }
</style>
