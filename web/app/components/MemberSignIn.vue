<script setup lang="ts">
import { ref } from 'vue'
import { NAlert, NButton, NCard, NInput } from 'naive-ui'
import type { LoginOptions } from '../types/member'

const props = defineProps<{ options: LoginOptions; busy: boolean; error: string }>()
const emit = defineEmits<{ login: [credentials: { username: string; password: string }]; discord: [invite: string] }>()
const username = ref('')
const password = ref('')
const invite = ref('')

function login() {
  if (props.busy || !username.value.trim() || !password.value) return
  const credentials = { username: username.value.trim(), password: password.value }
  password.value = ''
  emit('login', credentials)
}
function discord() {
  if (props.busy || !props.options.ready) return
  const value = invite.value.trim()
  invite.value = ''
  emit('discord', value)
}
</script>

<template>
  <NCard id="login" class="sign-in" :aria-busy="busy" aria-labelledby="loginTitle">
    <template #header><h2 id="loginTitle">登录你的账户</h2></template>
    <p id="loginHint" class="muted">
      {{ options.discord ? (options.ready ? '使用 Discord 身份登录；仅限受邀的指定服务器成员。' : 'Discord 登录尚未配置完成，请联系管理员。') : '账户由管理员创建。登录后查看个人额度和调用记录。' }}
    </p>
    <NAlert v-if="error" type="error" role="alert" :show-icon="false">{{ error }}</NAlert>
    <form v-if="options.discord" id="discordForm" @submit.prevent="discord">
      <label for="invite">邀请码（仅首次登录需要）</label>
      <NInput v-model:value="invite" type="password" autocomplete="off" :maxlength="32" :disabled="busy || !options.ready" :input-props="{ id: 'invite' }" />
      <NButton id="discordLogin" attr-type="submit" type="primary" block :loading="busy" :disabled="busy || !options.ready">使用 Discord 登录</NButton>
      <p class="muted">首次登录需管理员邀请，并验证你是指定 Discord 服务器成员。绑定后无需再次填写邀请码。</p>
    </form>
    <form v-else id="loginForm" @submit.prevent="login">
      <label for="username">用户名</label>
      <NInput v-model:value="username" autocomplete="username" :maxlength="40" :disabled="busy" :input-props="{ id: 'username', required: true }" />
      <label for="password">密码</label>
      <NInput v-model:value="password" type="password" autocomplete="current-password" :maxlength="128" :disabled="busy" :input-props="{ id: 'password', required: true }" />
      <NButton attr-type="submit" type="primary" block :loading="busy" :disabled="busy">登录</NButton>
    </form>
  </NCard>
</template>

<style scoped>
.sign-in { max-width: 480px; margin: 24px auto; }
h2 { margin: 0; font-size: 1.25rem; }
form { display: grid; gap: 12px; margin-top: 20px; }
label { font-weight: 600; }
.muted { color: #adb7c6; line-height: 1.7; overflow-wrap: anywhere; }
form .muted { margin: 0; }
</style>
