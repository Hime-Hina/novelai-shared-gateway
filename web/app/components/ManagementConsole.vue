<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { NAlert, NButton, NCard, NCheckbox, NInput, NSkeleton, NSpace, NTabPane, NTabs, NPopover, NTag } from 'naive-ui'
import type { ManagementAlert, ManagementMode } from '../types/management'
import { useManagementPortal } from '../composables/useManagementPortal'

const props = defineProps<{ mode: ManagementMode }>()
const { state, shared, panel, days, usage, usageLoading, busy, connecting, adminToken, remember, sessionVersion,
  message, messageError, messageVersion, readError, usageError, updatedAt, connection, refreshVersion, activityInvalidation,
  refreshing, api, run, notify, changed, connect, disconnect, refreshPage, loadUsage } = useManagementPortal(props.mode)
const groupsDirty = ref(false), policyDirty = ref(false), alerts = ref<ManagementAlert[]>([])
const hint = computed(() => !state.value ? (connecting.value ? '正在验证管理身份…' : props.mode === 'owner' ? '需要 owner 身份' : '私有管理入口 · 仅管理员可操作')
  : '成员、分组与共享账户管理')
function changedSession() { groupsDirty.value = false; policyDirty.value = false; alerts.value = [] }
watch(sessionVersion, changedSession)
function beforeUnload(event: BeforeUnloadEvent) {
  if (groupsDirty.value || policyDirty.value) { event.preventDefault(); event.returnValue = '' }
}
onMounted(() => window.addEventListener('beforeunload', beforeUnload))
onBeforeUnmount(() => window.removeEventListener('beforeunload', beforeUnload))
</script>

<template>
  <main class="management">
    <header class="management-header">
      <div><h1>共享账户控制台</h1><p id="connectedHint" class="muted" role="status">{{ hint }}</p></div>
      <NSpace v-if="state" id="topActions">
        <NButton v-if="mode === 'owner'" id="disconnect" tag="a" href="/">返回我的额度</NButton>
        <NButton v-else id="disconnect" @click="disconnect(); changedSession()">断开管理</NButton>
      </NSpace>
    </header>
    <PortalToast :key="messageVersion" :message="message" :type="messageError ? 'error' : 'success'" />
    <PortalToast :message="readError" type="warning" />
    <NCard v-if="connecting && !state" title="正在读取管理摘要" aria-busy="true"><NSkeleton text :repeat="5" /></NCard>
    <section v-if="state" id="dashboard" :key="sessionVersion" class="dashboard">
      <PortalRefresh :connection="connection" :loading="refreshing" :disabled="busy" :updated-at="updatedAt" summary="管理摘要更新于" @refresh="refreshPage" />
      <div class="account-strip">
        <div class="toolbar-controls"><NTag size="small" :bordered="false">{{ state.users.filter(user => user.enabled).length }} 位成员</NTag><span>生成中 {{ shared?.running ?? '—' }} / 1 · 排队 {{ shared?.queued ?? '—' }}</span><span>公共 V5 {{ shared?.v5Percent ?? '—' }}%</span></div>
        <NPopover trigger="click" style="max-width: 380px"><template #trigger><NButton size="small" quaternary :type="alerts.length ? 'warning' : 'default'">{{ alerts.length ? alerts.length + ' 项需关注' : '暂无财务告警' }}</NButton></template><div id="lossAlerts"><p v-for="alert in alerts" :key="alert.id">{{ alert.message }}</p><p v-if="!alerts.length">当前没有财务风险告警。</p><NButton size="small" @click="panel = 'settings'">查看上游与策略</NButton></div></NPopover>
      </div>
      <ManagementServiceResources :data="state.serviceResources" />
      <NTabs v-model:value="panel" type="line" size="large" aria-label="管理功能">
        <NTabPane v-if="mode === 'owner'" name="usage" display-directive="show:lazy">
          <template #tab><NButton id="usageTab" text :aria-pressed="panel === 'usage'">用量与记录</NButton></template>
          <PortalToast :message="usageError" type="warning" />
          <LazyCallHeatmap :api="api" endpoint="member-management?view=heatmap" :members="state.users" :active="panel === 'usage'" :refresh-version="refreshVersion" :invalidation="activityInvalidation" />
          <ManagementUsage :data="usage" :days="days" :loading="usageLoading" @update:days="days = $event" @retry="loadUsage" />
        </NTabPane>
        <NTabPane name="members" display-directive="show:lazy">
          <template #tab><NButton text :aria-pressed="panel === 'members'">成员与分发</NButton></template>
          <ManagementMembers :state="state" :mode="mode" :api="api" :run="run" :busy="busy" :groups-dirty="groupsDirty" @changed="changed" @notice="notify" />
        </NTabPane>
        <NTabPane name="groups" display-directive="show:lazy">
          <template #tab><NButton text :aria-pressed="panel === 'groups'">分组与额度</NButton></template>
          <ManagementGroups :state="state" :api="api" :run="run" :busy="busy" @dirty="groupsDirty = $event" @changed="changed" @notice="notify" />
        </NTabPane>
        <NTabPane name="settings" display-directive="show:lazy">
          <template #tab><NButton text :aria-pressed="panel === 'settings'">上游与策略</NButton></template>
          <SharedAccountStatus :state="shared" :connection="mode === 'admin' ? 'polling' : connection" />
          <NAlert v-if="alerts.length" type="warning"><p v-for="alert in alerts" :key="alert.id">{{ alert.message }}</p></NAlert>
          <ManagementUpstream :state="state" :mode="mode" :api="api" :run="run" :busy="busy" :groups-dirty="groupsDirty" :active="panel === 'settings'" :refresh-version="refreshVersion" @changed="changed" @notice="notify" />
        </NTabPane>
      </NTabs>
      <!-- 告警持续读取；其余表单首次访问才挂载，访问后保留草稿。 -->
      <ManagementLosses :state="state" :api="api" :run="run" :busy="busy" :active="panel === 'settings'" :refresh-version="refreshVersion" @changed="changed" @notice="notify" @dirty="policyDirty = $event" @alerts="alerts = $event" />
    </section>
    <template v-else-if="!connecting">
      <NCard v-if="mode === 'admin'" class="connect-card" title="连接管理端">
        <form id="connect" @submit.prevent="changedSession(); connect()">
          <label for="adminToken">管理员 Key</label>
          <NInput v-model:value="adminToken" type="password" autocomplete="off" :input-props="{ id: 'adminToken', required: true }" />
          <NCheckbox v-model:checked="remember">仅在当前浏览器会话中记住</NCheckbox>
          <NButton attr-type="submit" type="primary" :disabled="!adminToken.trim()">进入控制台</NButton>
        </form>
      </NCard>
      <NButton v-else id="ownerSignin" tag="a" href="/">返回成员页登录或确认权限</NButton>
    </template>
  </main>
</template>
<style scoped>
.management { max-width: 1440px; margin: auto; padding: 28px 24px 40px; }
.management-header { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 24px; }
h1 { margin: 0; font-size: 27px; }
.muted { color: #adb7c6; }
.dashboard, form { display: grid; grid-template-columns: minmax(0, 1fr); gap: 20px; }
.dashboard { margin-top: 20px; }
.account-strip { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: center; gap: 12px; min-height: 36px; font-size: 12px; color: var(--text-muted); }
:deep(.n-tab-pane) { padding-top: 24px; }
:deep(.n-tab-pane > * + *) { margin-top: 24px; }
.connect-card { max-width: 510px; margin: 32px auto; }
form { gap: 14px; }
@media (max-width: 700px) { .management { padding: 18px 14px 28px; } .management-header { align-items: start; flex-wrap: wrap; } }
</style>
