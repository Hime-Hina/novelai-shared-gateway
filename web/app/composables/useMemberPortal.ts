import { onBeforeUnmount, onMounted, reactive, ref, shallowRef } from 'vue'
import type { LoginOptions, MemberApi, MemberProfile, SharedStatus, StateInvalidation } from '../types/member'

type RequestFailure = Error & { status?: number; stale?: boolean }

export function useMemberPortal() {
  const loginOptions = shallowRef<LoginOptions | null>(null)
  const shared = shallowRef<SharedStatus | null>(null)
  const connection = ref('connecting'), initializing = ref(true), working = ref(false), error = ref('')
  const updatedAt = ref<number | null>(null), refreshVersion = ref(0)
  const activityInvalidation = reactive<{ version: number; dates: string[] | null }>({ version: 0, dates: null })
  let epoch = 0, disposed = false, refreshPending = false, refreshTask: Promise<void> | null = null
  let timer: ReturnType<typeof setInterval> | null = null
  let events: typeof import('../../../public/member-events.js') | null = null

  function stop() {
    events?.stopMemberEvents()
    if (timer) clearInterval(timer)
    timer = null
  }
  function clearSession(message = '') {
    epoch++; refreshPending = false; stop()
    profileQuery.clear(); shared.value = null; updatedAt.value = null
    activityInvalidation.dates = null
    error.value = message
  }
  const api: MemberApi = async <T>(path: string, body?: unknown): Promise<T> => {
    const current = epoch
    const check = () => { if (disposed || epoch !== current) throw Object.assign(new Error('旧会话响应已忽略'), { stale: true }) }
    const response = await fetch('/api/member/' + path, {
      method: body === undefined ? 'GET' : 'POST', credentials: 'same-origin',
      headers: body === undefined ? {} : { 'Content-Type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body)
    }).catch(failure => { check(); throw failure })
    const value = await response.json().catch(() => { check(); throw new Error('响应无法读取，请重试。') })
    check()
    if (!response.ok) {
      if ([401, 403].includes(response.status)) clearSession(initializing.value ? '' : '登录已失效，请重新登录。')
      throw Object.assign(new Error(value.message || value.error || '请求失败'), { status: response.status })
    }
    return value as T
  }
  // 私人数据只在浏览器查询；框架负责本页查询与取消，禁止进入 SSR payload。
  const profileQuery = useAsyncData<MemberProfile | null>('member-profile', async (): Promise<MemberProfile | null> => {
    try {
      const value = await api<MemberProfile>('me')
      updatedAt.value = Date.now(); refreshVersion.value++
      if (error.value.startsWith('额度读取失败，保留上次结果：')) error.value = ''
      if (connection.value !== 'open') { activityInvalidation.dates = null; activityInvalidation.version++ }
      return value
    } catch (failure) {
      const reason = failure as RequestFailure
      if (!reason.stale && ![401, 403].includes(reason.status || 0)) error.value = '额度读取失败，保留上次结果：' + reason.message
      return profileQuery.data.value ?? null
    }
  }, { server: false, immediate: false, default: () => null, deep: false, serialize: false })

  async function start() {
    if (!profileQuery.data.value || !loginOptions.value || disposed || initializing.value) return
    const current = epoch
    events ||= await import('../../../public/member-events.js')
    if (current !== epoch || disposed || !profileQuery.data.value) return
    events.startMemberEvents({
      onStatus: (value: SharedStatus) => { shared.value = value },
      onConnection: (value: string) => { connection.value = value },
      onExpired: () => clearSession('登录已失效，请重新登录。'),
      onInvalidate: (value: StateInvalidation) => {
        if (value.resync || value.scopes.includes('member')) {
          if (value.resync || value.activityDates?.length) {
            activityInvalidation.dates = value.resync ? null : value.activityDates!
            activityInvalidation.version++
          }
          void refresh()
        }
      }
    })
    if (!document.hidden && !timer) timer = setInterval(() => { void refresh() }, 30000)
  }
  function refresh(): Promise<void> {
    if (disposed || (!initializing.value && document.hidden)) return Promise.resolve()
    refreshPending = true
    if (refreshTask) return refreshTask
    refreshTask = (async () => {
      do {
        refreshPending = false
        await profileQuery.refresh({ dedupe: 'defer' })
      } while (refreshPending && !disposed && !document.hidden)
      await start()
    })().finally(() => { refreshTask = null })
    return refreshTask
  }
  function refreshPage() {
    if (disposed || !profileQuery.data.value || document.hidden) return Promise.resolve()
    activityInvalidation.dates = null; activityInvalidation.version++
    return refresh()
  }
  async function initialize() {
    initializing.value = true; error.value = ''
    try {
      // 登录配置是公开数据，不与匿名 me 的 401 共享会话版本。
      const [options] = await Promise.all([
        $fetch<LoginOptions>('/api/member/login-options', { retry: 0, credentials: 'same-origin' }), refresh()
      ])
      if (disposed) return
      if (options.discord && options.origin && options.origin !== location.origin) { location.replace(options.origin + '/'); return }
      loginOptions.value = options
    } catch (failure) { if (!disposed) error.value = '登录设置读取失败：' + (failure as Error).message }
    finally { initializing.value = false }
    await start()
  }
  async function action(run: () => Promise<void>) {
    if (working.value) return
    working.value = true; error.value = ''
    try { await run() }
    catch (failure) { if (!(failure as RequestFailure).stale) error.value = (failure as Error).message }
    finally { working.value = false }
  }
  function login(credentials: { username: string; password: string }) {
    return action(async () => { await api('login', credentials); await refresh() })
  }
  function startDiscord(invite: string) {
    return action(async () => {
      const result = await api<{ authorizationUrl: string }>('discord/start', invite ? { invite } : {})
      location.assign(result.authorizationUrl)
    })
  }
  function logout() { return action(async () => { await api('logout', {}); clearSession() }) }
  function visible() {
    if (document.hidden) { if (timer) clearInterval(timer); timer = null }
    else void start()
  }
  function hide() { if (timer) clearInterval(timer); timer = null }
  onMounted(() => {
    document.addEventListener('visibilitychange', visible)
    window.addEventListener('pagehide', hide); window.addEventListener('pageshow', visible)
    void initialize()
  })
  onBeforeUnmount(() => {
    disposed = true; epoch++; stop(); profileQuery.clear()
    document.removeEventListener('visibilitychange', visible)
    window.removeEventListener('pagehide', hide); window.removeEventListener('pageshow', visible)
  })
  return { profile: profileQuery.data, loginOptions, shared, connection, initializing, working, error, updatedAt, refreshVersion, activityInvalidation,
    refreshing: profileQuery.pending, initialize, refresh, refreshPage, login, startDiscord, logout, api }
}
