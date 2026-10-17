<script setup lang="ts">
import { onBeforeUnmount, ref } from 'vue'
import { NAlert, NButton, NCard, NInput } from 'naive-ui'
import type { MemberApi } from '../types/member'

const props = defineProps<{ api: MemberApi }>()
const visible = ref(false), loading = ref(false), copying = ref(false)
const apiKey = ref(''), baseUrl = ref(''), error = ref(''), message = ref('')
let active = true, version = 0

function hide() {
  version++
  visible.value = false
  apiKey.value = ''; baseUrl.value = ''; error.value = ''; message.value = ''
  loading.value = false; copying.value = false
}
async function show() {
  if (!active || loading.value) return
  const request = ++version
  loading.value = true; error.value = ''; message.value = ''
  try {
    const result = await props.api<{ apiKey: string; baseUrl: string }>('key')
    if (!active || request !== version) return
    apiKey.value = result.apiKey; baseUrl.value = result.baseUrl || ''; visible.value = true
  } catch (cause) {
    if (active && request === version) error.value = cause instanceof Error ? cause.message : 'API Key 读取失败，请重试。'
  } finally { if (active && request === version) loading.value = false }
}
async function copy() {
  if (!active || !visible.value || !apiKey.value || copying.value) return
  const request = version
  copying.value = true; error.value = ''; message.value = ''
  try {
    if (!navigator.clipboard) throw new Error('浏览器暂不支持复制，请检查安全连接和剪贴板权限。')
    await navigator.clipboard.writeText(apiKey.value)
    if (active && request === version) message.value = '已复制自己的 API Key'
  } catch (cause) {
    if (active && request === version) error.value = cause instanceof Error ? cause.message : '复制失败，请检查剪贴板权限。'
  } finally { if (active && request === version) copying.value = false }
}
onBeforeUnmount(() => { active = false; hide() })
</script>

<template>
  <NCard title="我的 API 配置" size="small">
    <NButton v-if="!visible" id="showKey" :loading="loading" @click="show">查看我的 API Key</NButton>
    <div v-else id="keyPanel" class="key-fields">
      <label for="apiBaseUrl">Base URL</label>
      <NInput :value="baseUrl" readonly :input-props="{ id: 'apiBaseUrl', autocomplete: 'off' }" />
      <label for="apiKey">我的 API Key</label>
      <NInput :value="apiKey" type="password" readonly :input-props="{ id: 'apiKey', autocomplete: 'off' }" />
      <div class="key-actions">
        <NButton id="copyKey" :loading="copying" @click="copy">复制 Key</NButton>
        <NButton id="hideKey" secondary @click="hide">隐藏</NButton>
      </div>
    </div>
    <NAlert v-if="error" type="error" class="key-message">{{ error }}</NAlert>
    <p v-if="message" role="status">{{ message }}</p>
  </NCard>
</template>

<style scoped>
.key-fields { display: grid; gap: 8px; min-width: 0; }
.key-actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px; }
.key-message { margin-top: 12px; }
</style>
