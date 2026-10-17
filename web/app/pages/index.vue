<script setup lang="ts">
import { ref, watch } from 'vue'
import { NButton, NCard, NSkeleton, NSpace, NCollapse, NCollapseItem, NDrawer, NDrawerContent } from 'naive-ui'
import { useMemberPortal } from '../composables/useMemberPortal'
import { date } from '../utils/member-display'

const { profile, loginOptions, shared, connection, initializing, working, error, updatedAt, refreshVersion, activityInvalidation,
  refreshing, initialize, refresh, refreshPage, login, startDiscord, logout, api } = useMemberPortal()
const apiOpen = ref(false)
watch(profile, value => { if (!value) apiOpen.value = false })
</script>
<template>
  <main class="portal">
    <header class="portal-header">
      <div><a href="/" class="brand">NovelAI 共享</a><p class="muted">个人额度 · 调用记录</p><p v-if="profile" id="sessionExpiry" class="section-caption">{{ profile.username }} · 登录至 {{ date(profile.sessionExpiresAt) }}（{{ profile.owner ? '管理员固定 12 小时' : '普通成员固定 7 天' }}，不自动续期）</p></div>
      <NSpace v-if="profile">
        <NButton id="apiSettings" @click="apiOpen = true">API 配置</NButton>
        <NButton v-if="profile.owner" id="ownerConsole" tag="a" href="/owner">管理控制台</NButton>
        <NButton id="logout" :loading="working" @click="logout">退出登录</NButton>
      </NSpace>
    </header>
    <PortalToast :message="profile || !loginOptions ? error : ''" type="warning" />
    <section v-if="initializing" class="initial-state" aria-label="正在读取账户" aria-busy="true">
      <NCard title="正在读取账户"><NSkeleton text :repeat="3" /></NCard>
      <div class="loading-cards"><NCard v-for="n in 3" :key="n"><NSkeleton height="112px" /></NCard></div>
    </section>
    <section v-else-if="profile && loginOptions" id="account" class="account-content">
      <PortalRefresh :connection="connection" :loading="refreshing" :updated-at="updatedAt" summary="额度更新于" @refresh="refreshPage" />
      <LazyCallHeatmap :api="api" endpoint="heatmap" :refresh-version="refreshVersion" :invalidation="activityInvalidation" />
      <MemberQuota :profile="profile" />
      <NCard size="small"><NCollapse><NCollapseItem :title="`共享账户 · 生成中 ${shared?.running ?? '—'} / 1 · 排队 ${shared?.queued ?? '—'}`" name="shared"><SharedAccountStatus :state="shared" :connection="connection" /></NCollapseItem></NCollapse></NCard>
      <LazyMemberCalls :jobs="profile.jobs" :late-results="profile.lateResults" :api="api" :refresh-version="refreshVersion" @changed="refresh" />
      <NDrawer v-model:show="apiOpen" width="min(440px, 100vw)">
        <NDrawerContent title="我的 API 配置" closable><MemberApiKey v-if="apiOpen" :api="api" /></NDrawerContent>
      </NDrawer>
    </section>
    <MemberSignIn v-else-if="loginOptions" :options="loginOptions" :busy="working" :error="error" @login="login" @discord="startDiscord" />
    <NCard v-else title="暂时无法读取登录入口"><NButton @click="initialize">重新读取</NButton></NCard>
    <footer class="muted">个人预扣与退款属于本站账本，不代表上游已退款。所有成员共用串行队列。</footer>
  </main>
</template>
<style scoped>
.portal { max-width: 1200px; margin: auto; padding: 28px 24px 40px; }
.portal-header { display: flex; justify-content: space-between; align-items: center; gap: 16px; margin-bottom: 24px; }
.brand { font-size: 25px; font-weight: 700; text-decoration: none; color: #f1f5fb; }
.portal-header p { margin: 4px 0 0; }
.muted { color: #a9b6ca; }
.account-content, .initial-state { display: grid; grid-template-columns: minmax(0, 1fr); gap: 20px; margin-top: 20px; }
.initial-state { min-height: 520px; align-content: start; }
.loading-cards { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 16px; }
footer { margin-top: 30px; font-size: 13px; line-height: 1.7; }
@media (max-width: 700px) { .portal { padding: 18px 14px 28px; } .portal-header { align-items: start; flex-wrap: wrap; } .loading-cards { grid-template-columns: 1fr; } }
</style>
