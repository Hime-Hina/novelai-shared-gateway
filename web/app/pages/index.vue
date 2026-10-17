<script setup lang="ts">
import { NAlert, NButton, NCard, NSkeleton, NSpace } from 'naive-ui'
import { useMemberPortal } from '../composables/useMemberPortal'
import { date } from '../utils/member-display'

const { profile, loginOptions, shared, connection, initializing, working, error, updatedAt, refreshVersion, activityInvalidation,
  initialize, refresh, login, startDiscord, logout, api } = useMemberPortal()
</script>
<template>
  <main class="portal">
    <header class="portal-header">
      <div><a href="/" class="brand">NovelAI 共享</a><p class="muted">个人额度 · 调用记录</p></div>
      <NSpace v-if="profile">
        <NButton v-if="profile.owner" id="ownerConsole" tag="a" href="/owner">管理控制台</NButton>
        <NButton id="logout" :loading="working" @click="logout">退出登录</NButton>
      </NSpace>
    </header>
    <NAlert v-if="error && (profile || !loginOptions)" id="message" type="warning" role="status">{{ error }}</NAlert>
    <section v-if="initializing" class="initial-state" aria-label="正在读取账户" aria-busy="true">
      <NCard title="正在读取账户"><NSkeleton text :repeat="3" /></NCard>
      <div class="loading-cards"><NCard v-for="n in 3" :key="n"><NSkeleton height="112px" /></NCard></div>
    </section>
    <section v-else-if="profile && loginOptions" id="account" class="account-content">
      <MemberQuota :profile="profile" />
      <SharedAccountStatus :state="shared" :connection="connection" />
      <div class="member-tools">
        <NSpace align="center"><NButton id="refresh" @click="refresh">刷新额度</NButton><span id="memberUpdated" class="muted">个人额度与调用更新于 {{ date(updatedAt) }}</span></NSpace>
        <MemberApiKey :api="api" />
      </div>
      <MemberCalls :jobs="profile.jobs" :late-results="profile.lateResults" :api="api" :refresh-version="refreshVersion" @changed="refresh" />
      <LazyCallHeatmap :api="api" endpoint="heatmap" :refresh-version="refreshVersion" :invalidation="activityInvalidation" />
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
.member-tools { display: grid; gap: 16px; }
footer { margin-top: 30px; font-size: 13px; line-height: 1.7; }
@media (max-width: 700px) { .portal { padding: 18px 14px 28px; } .portal-header { align-items: start; flex-wrap: wrap; } .loading-cards { grid-template-columns: 1fr; } }
</style>
