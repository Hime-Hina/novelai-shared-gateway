<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from 'vue'
import { NAlert, NButton, NCollapse, NCollapseItem } from 'naive-ui'
import type { MemberApi, MemberBatch as BatchSnapshot } from '../types/member'
import { jobStatus, number, resultUrl } from '../utils/member-display'

const props = defineProps<{ requestId: string; api: MemberApi; refreshVersion: number }>()
const emit = defineEmits<{ changed: [] }>()
const expanded = ref(false), loading = ref(false), cancelling = ref(false)
const batch = ref<BatchSnapshot | null>(null), error = ref(''), message = ref('')
let active = true, viewVersion = 0, readVersion = 0, pending = false
let reading: Promise<void> | null = null
const path = () => 'requests/' + encodeURIComponent(props.requestId)

function read() {
  if (!active || !expanded.value) return
  if (reading || cancelling.value) { pending = true; return reading }
  reading = (async () => {
    do {
      pending = false
      const view = viewVersion, request = ++readVersion
      loading.value = true
      try {
        const result = await props.api<BatchSnapshot>(path())
        if (active && view === viewVersion && request === readVersion && expanded.value && !cancelling.value) {
          batch.value = result; error.value = ''
        }
      } catch (cause) {
        if (active && view === viewVersion && request === readVersion && expanded.value) {
          error.value = '批次读取失败，可重新展开或刷新重试。' + (cause instanceof Error ? ' ' + cause.message : '')
        }
      }
    } while (pending && active && expanded.value && !cancelling.value)
  })().finally(() => { reading = null; if (active) loading.value = false })
  return reading
}
function toggle(names: Array<string | number>) {
  expanded.value = names.includes(props.requestId)
  if (expanded.value) void read()
  else { readVersion++; pending = false }
}
async function cancel() {
  if (!active || cancelling.value || !batch.value?.cancellable) return
  const view = viewVersion
  cancelling.value = true; readVersion++; pending = false; error.value = ''; message.value = ''
  try {
    const result = await props.api<BatchSnapshot>(path() + '/cancel', {})
    if (!active || view !== viewVersion) return
    batch.value = result
    message.value = `此次取消 ${number(result.cancelledCount ?? 0)} 条尚未派发任务。生成中的图片继续完成，已完成结果仍可领取。`
    emit('changed')
  } catch (cause) {
    if (active && view === viewVersion) error.value = '取消未确认，请刷新批次核实状态后再操作。' + (cause instanceof Error ? ' ' + cause.message : '')
  } finally {
    if (active && view === viewVersion) {
      cancelling.value = false
      if (pending && expanded.value) void read()
    }
  }
}
watch(() => props.refreshVersion, () => { if (expanded.value) void read() })
watch(() => props.requestId, () => {
  viewVersion++; readVersion++; pending = false
  batch.value = null; error.value = ''; message.value = ''; cancelling.value = false
  if (expanded.value) void read()
})
onBeforeUnmount(() => { active = false; viewVersion++; readVersion++; pending = false; batch.value = null })
</script>

<template>
  <NCollapse class="member-batch" :expanded-names="expanded ? [requestId] : []" @update:expanded-names="toggle">
    <NCollapseItem :name="requestId" :title="'查看批次 · ' + requestId">
      <div class="refresh-status" role="status">{{ loading ? '正在读取批次…' : '展开时随调用记录自动更新' }}</div>
      <NAlert v-if="error" type="error">{{ error }}</NAlert>
      <template v-if="batch">
        <p>共 {{ number(batch.counts.total) }} 张 · 等待前序 {{ number(batch.counts.pending) }} · 排队 {{ number(batch.counts.queued) }} · 生成中 {{ number(batch.counts.running) }} · 成功 {{ number(batch.counts.done) }} · 失败或取消 {{ number(batch.counts.failed) }}</p>
        <ul class="batch-jobs">
          <li v-for="job in batch.jobs" :key="job.id">
            第 {{ job.index + 1 }} 张 · {{ job.model }} · {{ job.status === 'failed' && job.cancelled ? '已取消（尚未派发）' : jobStatus(job.status) }}
            <p v-if="job.error && job.status === 'failed' && !job.cancelled">{{ job.error }}</p>
            <p v-if="resultUrl(job.imageUrl)"><a :href="resultUrl(job.imageUrl) || undefined" target="_blank" rel="noopener noreferrer" referrerpolicy="no-referrer">{{ job.lateResultAvailable ? '领取迟到结果（退款保留）' : '领取结果' }}</a></p>
          </li>
        </ul>
        <NAlert type="info">仅取消此批次排队或等待前序的图片。生成中的图片继续完成，已完成结果仍可领取。</NAlert>
      </template>
      <div class="batch-actions">
        <NButton v-if="error" secondary :loading="loading" :disabled="cancelling" @click="read">重新核实批次</NButton>
        <NButton v-if="batch" secondary :loading="cancelling" :disabled="!batch.cancellable" @click="cancel">取消此批次尚未派发的图片</NButton>
      </div>
      <p v-if="message" role="status">{{ message }}</p>
    </NCollapseItem>
  </NCollapse>
</template>

<style scoped>
.member-batch { min-width: 0; max-width: 100%; overflow-wrap: anywhere; }
.member-batch :deep(.n-collapse-item__header-main) { min-width: 0; white-space: normal; overflow-wrap: anywhere; }
.batch-jobs { padding-left: 22px; }
.batch-jobs li + li { margin-top: 12px; }
.batch-jobs p { margin: 4px 0; }
.batch-actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
.batch-actions :deep(.n-button) { max-width: 100%; height: auto; min-height: 34px; padding: 8px 12px; }
.batch-actions :deep(.n-button__content) { white-space: normal; }
</style>
