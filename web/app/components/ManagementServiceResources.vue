<script setup lang="ts">
import { NCard } from 'naive-ui'
import type { ServiceResources } from '../types/management'
import { date, number } from '../utils/member-display'

defineProps<{ data?: ServiceResources }>()
function bytes(value: number | undefined) {
  if (value == null || !Number.isFinite(value) || value < 0) return '暂不可用'
  const unit = value === 0 ? 0 : Math.min(Math.floor(Math.log(value) / Math.log(1024)), 4)
  return `${number(value / 1024 ** unit)} ${['B', 'KiB', 'MiB', 'GiB', 'TiB'][unit]}`
}
function uptime(seconds: number | undefined) {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return '暂不可用'
  const minutes = Math.floor(seconds / 60)
  return `${Math.floor(minutes / 1440)} 天 ${Math.floor(minutes / 60) % 24} 小时 ${minutes % 60} 分钟`
}
</script>

<template>
  <section aria-labelledby="serviceResourcesTitle">
    <h2 id="serviceResourcesTitle">服务资源</h2>
    <div class="resource-cards">
      <NCard size="small">
        <span class="metric-label">执行器内存（RSS）</span>
        <strong id="serviceMemory">{{ bytes(data?.memory.rssBytes) }}</strong>
        <p>JS 堆 {{ bytes(data?.memory.heapUsedBytes) }} / {{ bytes(data?.memory.heapTotalBytes) }}</p>
      </NCard>
      <NCard size="small">
        <span class="metric-label">服务数据文件</span>
        <strong id="serviceData">{{ bytes(data?.data?.bytes) }}</strong>
        <p>数据库、图片等文件大小之和 · 每分钟采样</p>
        <p v-if="data?.data">采样于 {{ date(data.data.checkedAt) }}</p>
        <template v-if="data?.r2">
          <p id="serviceR2">R2 {{ bytes(data.r2.bytes) }} / {{ bytes(data.r2.limitBytes) }} · 已转存 {{ data.r2.images }} 张</p>
          <p>生成超过一天后自动转存原图 · 达上限暂停新增转存</p>
          <p v-if="data.r2.pending || data.r2.errors">待验证 {{ data.r2.pending }} 张 · 转存异常 {{ data.r2.errors }} 张（稍后重试）</p>
        </template>
      </NCard>
      <NCard size="small">
        <span class="metric-label">数据目录所在磁盘（已用 / 总量）</span>
        <strong id="serviceDisk">{{ bytes(data?.disk?.usedBytes) }} / {{ bytes(data?.disk?.totalBytes) }}</strong>
        <p>可用 {{ bytes(data?.disk?.availableBytes) }} · 包含其他服务占用</p>
      </NCard>
      <NCard size="small">
        <span class="metric-label">执行器运行时间</span>
        <strong id="serviceUptime">{{ uptime(data?.uptimeSeconds) }}</strong>
        <p>进程重启后重新计时 · 内存不含独立网页服务</p>
      </NCard>
    </div>
  </section>
</template>

<style scoped>
h2 { margin: 0 0 12px; font-size: 16px; }
.resource-cards { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
.metric-label, p { color: var(--text-muted); font-size: 12px; }
strong { display: block; margin: 10px 0; font-size: 20px; line-height: 1.4; font-variant-numeric: tabular-nums; overflow-wrap: anywhere; }
p { margin: 4px 0 0; }
@media (max-width: 1100px) { .resource-cards { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 550px) { .resource-cards { grid-template-columns: minmax(0, 1fr); } }
</style>
