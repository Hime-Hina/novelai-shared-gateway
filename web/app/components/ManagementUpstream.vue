<script setup lang="ts">
import { computed, onBeforeUnmount, ref, shallowRef, watch } from 'vue'
import { NAlert, NButton, NCard, NCheckbox, NInput, NSpace } from 'naive-ui'
import type { ManagementAccount, ManagementApi, ManagementMode, ManagementRun, ManagementState, ManagementSummary } from '../types/management'
import { date, number } from '../utils/member-display'

const props = defineProps<{ state: ManagementState; mode: ManagementMode; api: ManagementApi; run: ManagementRun;
  busy: boolean; groupsDirty: boolean; active: boolean; refreshVersion: number }>()
const emit = defineEmits<{ changed: [message: string]; notice: [message: string, error?: boolean] }>()
type Account = Pick<ManagementAccount, 'id' | 'enabled' | 'inFlight' | 'quotaTier' | 'quotaFixed' | 'quotaPurchased' | 'v5UsagePercent' | 'quotaCheckedAt' | 'quotaError'>
const account = shallowRef<Account | null>(null), error = ref(''), loading = ref(false)
const fallback = ref(props.state.autoAnlasFallback), fallbackDirty = ref(false), pst = ref('')
let disposed = false, pending = false, task: Promise<void> | null = null
const boundId = computed(() => props.state.users.find(user => user.quota?.boundAccountId)?.quota?.boundAccountId)
watch(() => props.state.autoAnlasFallback, value => { if (!fallbackDirty.value) fallback.value = value })
function requireClean() { if (props.groupsDirty) throw new Error('分组有未保存修改，请先保存或撤销。') }
function load(): Promise<void> {
  if (!props.active || disposed) return Promise.resolve()
  pending = true
  if (task) return task
  task = (async () => {
    do {
      pending = false; loading.value = true
      try {
        const summary = await props.api<ManagementSummary>('summary')
        if (disposed) return
        const value = summary.accounts.find(candidate => candidate.id === boundId.value)
        // 私有summary可能含秘密；只保留页面所需的脱敏字段。
        account.value = value ? { id: value.id, enabled: value.enabled, inFlight: value.inFlight, quotaTier: value.quotaTier,
          quotaFixed: value.quotaFixed, quotaPurchased: value.quotaPurchased, v5UsagePercent: value.v5UsagePercent,
          quotaCheckedAt: value.quotaCheckedAt, quotaError: value.quotaError ? '官方额度查询失败' : '' } : null
        error.value = ''
      } catch (failure) { if (!disposed && !(failure as { stale?: boolean }).stale) error.value = '上游状态读取失败，保留上次结果：' + (failure as Error).message }
      finally { loading.value = false }
    } while (pending && props.active && !disposed)
  })().finally(() => { task = null })
  return task
}
function refreshQuota() {
  return props.run(async () => {
    requireClean(); if (!account.value) throw new Error('未绑定共享上游')
    const result = await props.api<{ failed: number }>('accounts/quota', { ids: [account.value.id] })
    emit('changed', result.failed ? '官方查询失败，请检查凭据；已有余额不会伪装为新数据。' : '已向官方刷新额度。')
  })
}
function toggle() {
  return props.run(async () => {
    requireClean(); if (!account.value) throw new Error('未绑定共享上游')
    const { id, enabled } = account.value
    if (!enabled && props.mode === 'admin') {
      const result = await props.api<{ failed: number }>('accounts/quota', { ids: [id] })
      if (result.failed) throw new Error('官方查询失败，上游保持暂停。')
    }
    await props.api('accounts/' + encodeURIComponent(id), { enabled: !enabled }, 'PATCH')
    emit('changed', enabled ? '上游已暂停。' : '上游已恢复。')
  })
}
function saveFallback() {
  return props.run(async () => {
    const enabled = fallback.value
    await props.api('member-management', { action: 'fallback-policy', enabled })
    if (fallback.value === enabled) fallbackDirty.value = false
    emit('changed', '续用策略已保存，仅影响之后的任务。')
  })
}
function savePst() {
  return props.run(async () => {
    requireClean(); if (!account.value) throw new Error('未绑定共享上游')
    if (account.value.inFlight) throw new Error('请等待在途生成完成。')
    const token = pst.value.trim(), id = account.value.id
    if (!/^pst-[A-Za-z0-9_-]+$/.test(token) || token.length > 4096) throw new Error('请输入完整 PST')
    await props.api('accounts/' + encodeURIComponent(id), { token }, 'PATCH')
    if (pst.value.trim() === token) pst.value = ''
    emit('changed', 'PST 已更新，上游保持暂停；确认官方额度后手动恢复。')
    try {
      const result = await props.api<{ failed: number }>('accounts/quota', { ids: [id] })
      if (result.failed) emit('notice', 'PST 已更新，但官方额度查询失败；上游保持暂停，勿重复提交。', true)
    } catch (failure) {
      if (!(failure as { stale?: boolean }).stale) emit('notice', 'PST 已更新，但后续查询失败；上游保持暂停，勿重复提交。', true)
    }
  })
}
watch([() => props.active, () => props.refreshVersion, boundId], () => { void load() }, { immediate: true })
onBeforeUnmount(() => { disposed = true; pending = false; pst.value = ''; account.value = null })
</script>

<template>
  <section class="upstream-settings">
    <NCard title="共享 Opus">
      <NAlert v-if="error" type="warning" role="status">{{ error }} <NButton text @click="load">重试读取</NButton></NAlert>
      <div class="refresh-status" role="status">{{ loading ? '正在读取上游状态…' : '随页面数据自动更新' }}</div>
      <template v-if="account">
        <p id="upstreamStatus">{{ account.enabled ? '已启用' : '已暂停' }} · {{ account.quotaTier === 3 ? 'Opus' : '订阅待核验' }} · 正在生成 {{ account.inFlight }}</p>
        <p id="upstreamQuota">V5 {{ number(account.v5UsagePercent) }}% · 订阅 {{ number(account.quotaFixed) }} · 购买 {{ number(account.quotaPurchased) }}</p>
        <p id="upstreamUpdated" class="muted">官方查询 {{ date(account.quotaCheckedAt) }}{{ account.quotaError ? ' · 查询失败' : '' }}</p>
      </template>
      <p v-else-if="!loading">尚未绑定共享 Opus</p>
      <NSpace><NButton id="refreshUpstream" :disabled="busy || !account" @click="refreshQuota">向官方查询余额</NButton>
        <NButton id="toggleUpstream" :disabled="busy || !account" @click="toggle">{{ account?.enabled ? '暂停上游' : '恢复上游' }}</NButton></NSpace>
    </NCard>
    <NCard title="V5 额度用完后">
      <NCheckbox id="autoFallback" v-model:checked="fallback" @update:checked="fallbackDirty = true">个人 V5 不足时，按 Anlas 公式转扣本人本地余额</NCheckbox>
      <p>保持原生图参数。上游仍可能消耗公共 V5；官方 Anlas 没减少也不会退本地扣款。个人余额不足则停止，失败退款。</p>
      <NButton id="saveFallback" :disabled="busy" @click="saveFallback">保存续用策略</NButton>
    </NCard>
    <NCard title="轮换同一官方账户的 PST">
      <p>只更新同一 NAI 账户的凭据。更新后上游保持暂停；查到正确额度后再明确恢复。</p>
      <form id="upstreamCredentials" @submit.prevent="savePst">
        <label for="upstreamPst">新的完整 PST</label>
        <NInput v-model:value="pst" type="password" autocomplete="off" :maxlength="4096" :input-props="{ id: 'upstreamPst' }" />
        <NButton id="saveUpstreamPst" attr-type="submit" :disabled="busy || !account || Boolean(account.inFlight) || !pst.trim()">更新 PST 并保持暂停</NButton>
      </form>
    </NCard>
  </section>
</template>
<style scoped>
.upstream-settings, form { display: grid; grid-template-columns: minmax(0, 1fr); gap: 16px; }
p { line-height: 1.7; overflow-wrap: anywhere; }
.muted { color: #adb7c6; }
</style>
