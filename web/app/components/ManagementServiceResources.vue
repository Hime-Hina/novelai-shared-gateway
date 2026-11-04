<script setup lang="ts">
import { NCard } from 'naive-ui'
import type { ServiceResources } from '../types/management'
import { date, number } from '../utils/member-display'

defineProps<{ data?: ServiceResources }>()
function bytes(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value) || value < 0) return '暂不可用'
  const unit = value === 0 ? 0 : Math.min(Math.floor(Math.log(value) / Math.log(1024)), 4)
  return `${number(value / 1024 ** unit)} ${['B', 'KiB', 'MiB', 'GiB', 'TiB'][unit]}`
}
function uptime(seconds: number | undefined) {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return '暂不可用'
  const minutes = Math.floor(seconds / 60)
  return `${Math.floor(minutes / 1440)} 天 ${Math.floor(minutes / 60) % 24} 小时 ${minutes % 60} 分钟`
}
function count(value: number | null | undefined) { return value == null ? '—' : number(value) }
function operationStatus(status: string | undefined) {
  return ({ ready: '官方统计', stale: '更新失败，显示上次统计', unavailable: '官方统计暂不可用', loading: '正在读取官方统计' } as Record<string, string>)[status || ''] || '未配置官方统计'
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
      <NCard v-if="data?.r2 || data?.pikpak" class="r2-card" size="small" aria-label="冷热存储">
        <div class="r2-content" :class="{ 'with-cold-storage': data?.r2 && data?.pikpak }">
          <div v-if="data?.r2">
            <span class="metric-label">R2 热存储 · 图片占用</span>
            <strong id="serviceR2">{{ bytes(data.r2.bytes) }}</strong>
            <p>热图片 {{ number(data.r2.images) }} 张</p>
            <p id="serviceR2Status">待验证 {{ data.r2.pending }} 张 · 转存异常 {{ data.r2.errors }} 张</p>
            <p>转冷水位 {{ bytes(data.r2.archiveAtBytes) }} · 不设容量硬上限</p>
            <p id="serviceR2Backups">业务备份 {{ count(data.r2.backups?.count) }} 份 · {{ bytes(data.r2.backups?.bytes) }}</p>
            <p>保留最近七份 · 新备份成功后清理旧备份</p>
            <p id="serviceR2BackupStatus">{{ data.r2.backups?.status === 'stale' ? '更新失败，显示上次统计' : data.r2.backups?.status === 'ready' ? `备份统计于 ${date(data.r2.backups.checkedAt!)}` : '备份统计暂不可用' }}</p>
          </div>
          <div v-if="data?.pikpak" class="cold-storage">
            <span class="metric-label">PikPak 冷存储 · 账户已用 / 总量</span>
            <strong id="servicePikPak">{{ bytes(data.pikpak.quota?.usedBytes) }} / {{ bytes(data.pikpak.quota?.totalBytes) }}</strong>
            <p>本站归档原图 {{ bytes(data.pikpak.originalBytes) }} · {{ number(data.pikpak.images) }} 张</p>
            <p id="servicePikPakStatus">待释放 R2 {{ data.pikpak.pending }} 张 · 转存异常 {{ data.pikpak.errors }} 张</p>
            <p>R2 存放超过七天或达到水位时自动转冷</p>
            <p>原图无损压缩 · 下载时解压校验</p>
            <p>账户用量包含其他目录；本站规模按原图计算</p>
            <p id="servicePikPakQuotaStatus">{{ operationStatus(data.pikpak.quota?.status) }}<br>{{ data.pikpak.quota?.checkedAt ? date(data.pikpak.quota.checkedAt) : '尚无更新时间' }}</p>
          </div>
          <div v-if="data?.r2" class="r2-operations">
            <p>账户标准存储 · 本账期操作次数</p>
            <p id="serviceR2Period">{{ data.r2.operations?.periodStart?.slice(0, 10) || '—' }} 至 {{ data.r2.operations?.periodEnd?.slice(0, 10) || '—' }}（UTC）</p>
            <table aria-label="R2 官方操作用量与剩余免费额度">
              <thead><tr><th scope="col">操作</th><th scope="col">已用</th><th scope="col">免费剩余</th></tr></thead>
              <tbody>
                <tr id="serviceR2ClassA"><th scope="row">A 类</th><td>{{ count(data.r2.operations?.classA.used) }}</td><td>{{ count(data.r2.operations?.classA.remaining) }}</td></tr>
                <tr id="serviceR2ClassB"><th scope="row">B 类</th><td>{{ count(data.r2.operations?.classB.used) }}</td><td>{{ count(data.r2.operations?.classB.remaining) }}</td></tr>
              </tbody>
            </table>
            <p>免费额度：A 类 100 万 / B 类 1,000 万次</p>
            <p id="serviceR2OperationsStatus">{{ operationStatus(data.r2.operations?.status) }}<br>{{ data.r2.operations?.checkedAt ? date(data.r2.operations.checkedAt) : '尚无更新时间' }}</p>
            <p>账户共享额度 · 统计有延迟<br>剩余为估算，以 Cloudflare 账单为准</p>
          </div>
        </div>
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
.r2-card { grid-column: 1 / -1; }
.r2-content { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 24px; }
.r2-content.with-cold-storage { grid-template-columns: repeat(3, minmax(0, 1fr)); }
.r2-content:has(> :only-child) { grid-template-columns: minmax(0, 1fr); }
.r2-operations, .cold-storage { padding-left: 24px; border-left: 1px solid var(--border-color, #444); }
.r2-operations table { width: 100%; margin: 8px 0; table-layout: fixed; font-size: 12px; font-variant-numeric: tabular-nums; }
.r2-operations th, .r2-operations td { padding: 3px 0; text-align: right; overflow-wrap: anywhere; }
.r2-operations th:first-child { width: 32px; text-align: left; }
#serviceR2OperationsStatus { min-height: 3.2em; }
@media (max-width: 1100px) { .resource-cards { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 550px) {
  .resource-cards, .r2-content, .r2-content.with-cold-storage { grid-template-columns: minmax(0, 1fr); }
  .r2-content { gap: 12px; }
  .r2-operations, .cold-storage { padding: 8px 0 0; border-left: 0; border-top: 1px solid var(--border-color, #444); }
}
</style>
