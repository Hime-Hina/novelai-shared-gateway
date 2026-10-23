<script setup lang="ts">
import { computed } from 'vue'
import { NButton, NPopover } from 'naive-ui'
import { date } from '../utils/member-display'

const props = defineProps<{ connection: string; loading: boolean; updatedAt: number | null; summary: string; disabled?: boolean }>()
const emit = defineEmits<{ refresh: [] }>()
const mode = computed(() => props.connection === 'open' ? '实时更新' : props.connection === 'polling' ? '每 30 秒更新' : '连接恢复中 · 每 30 秒补读')
</script>
<template>
  <div class="portal-refresh">
    <div class="sync-info">
      <NPopover trigger="click" :style="{ maxWidth: 'min(360px, calc(100vw - 32px))' }">
        <template #trigger><NButton text size="small" class="sync-mode" :type="connection === 'open' ? 'success' : 'default'">{{ mode }} · 更新说明</NButton></template>
        <p>页面可见时，额度、调用记录和当前管理页会在变更后更新，并每 30 秒补读。热力图按变更日期更新，重连或手动刷新时完整校准。</p>
        <p>切回页面时重新校准；未保存的表单草稿保留，API Key 仅在主动查看时读取。</p>
        <p>官方余额由服务每 60 秒及任务结束后查询。“刷新页面数据”读取本站最新快照，不向 NAI 发送生图请求。</p>
      </NPopover>
      <span class="sync-time" role="status">{{ summary }} {{ date(updatedAt) }} · 上海时间</span>
    </div>
    <NButton id="refresh" size="small" :loading="loading" :disabled="disabled" @click="emit('refresh')">刷新页面数据</NButton>
  </div>
</template>
<style scoped>
.portal-refresh { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 12px; padding: 12px 0; border-top: 1px solid #ffffff12; }
.sync-info { display: flex; align-items: center; flex-wrap: wrap; column-gap: 16px; row-gap: 4px; min-width: 0; }
.sync-time { color: var(--text-muted); font-size: 12px; font-variant-numeric: tabular-nums; }
#refresh { min-width: 140px; }
p { font-size: 13px; line-height: 1.7; }
</style>
