import { computed, onBeforeUnmount, onMounted, reactive, ref, shallowRef, watch } from 'vue'
import type { ManagementApi, ManagementMode, ManagementRun, ManagementState, ManagementUsage } from '../types/management'
import type { SharedStatus, StateInvalidation } from '../types/member'

type Failure = Error & { statusCode?: number; stale?: boolean }
export type ManagementPanel = 'members' | 'groups' | 'usage' | 'settings'

export function useManagementPortal(mode: ManagementMode) {
  const panel = ref<ManagementPanel>(mode === 'owner' ? 'usage' : 'members')
  const days = ref(7), busy = ref(false), connecting = ref(mode === 'owner')
  const adminToken = ref(''), remember = ref(true), sessionVersion = ref(0)
  const message = ref(''), messageError = ref(false), readError = ref(''), usageError = ref('')
  const messageVersion = ref(0)
  const shared = shallowRef<SharedStatus | null>(null), updatedAt = ref<number | null>(null)
  const connection = ref(mode === 'owner' ? 'connecting' : 'polling'), refreshVersion = ref(0)
  const activityInvalidation = reactive<{ version: number; dates: string[] | null }>({ version: 0, dates: null })
  let epoch = 0, disposed = false, activeToken = '', pending = false, usagePending = false
  let refreshTask: Promise<void> | null = null, usageTask: Promise<void> | null = null
  let timer: ReturnType<typeof setInterval> | null = null
  let events: typeof import('../../../public/member-events.js') | null = null

  function notify(text: string, error = false) { message.value = text; messageError.value = error; messageVersion.value++ }
  function stop() {
    if (mode === 'owner') events?.stopMemberEvents()
    if (timer) clearInterval(timer)
    timer = null
  }
  function clear(removeCredential = false, text = '') {
    epoch++; sessionVersion.value++; pending = false; usagePending = false; stop()
    refreshTask = null; usageTask = null
    stateQuery.clear(); usageQuery.clear(); shared.value = null; updatedAt.value = null
    activityInvalidation.dates = null
    activeToken = ''; adminToken.value = ''; busy.value = false; connecting.value = false
    readError.value = ''; usageError.value = ''; notify(text, Boolean(text))
    if (mode === 'admin' && removeCredential) {
      sessionStorage.removeItem('nai.adminSession'); localStorage.removeItem('nai.adminToken')
    }
  }
  const headers = (): Record<string, string> => mode === 'admin' ? { 'x-admin-token': activeToken } : {}
  async function authorized(): Promise<boolean | null> {
    try {
      const response = await fetch(mode === 'owner' ? '/api/member/me' : '/api/admin/ping', {
        credentials: 'same-origin', headers: headers()
      })
      if ([401, 403].includes(response.status)) return false
      if (!response.ok) return null
      return mode === 'admin' || (await response.json()).owner === true
    } catch { return null }
  }
  const api: ManagementApi = async <T>(path: string, body?: unknown, method?: 'GET' | 'POST' | 'PATCH'): Promise<T> => {
    const current = epoch
    const check = () => { if (disposed || current !== epoch) throw Object.assign(new Error('旧管理连接响应已忽略'), { stale: true }) }
    check()
    const response = await fetch((mode === 'owner' ? '/api/member/owner/' : '/api/admin/') + path, {
      method: method || (body === undefined ? 'GET' : 'POST'), credentials: 'same-origin',
      headers: { ...headers(), ...(mode === 'owner' && path === 'member-management' && body === undefined && !document.hidden ? { 'X-Member-Activity': '1' } : {}),
        ...(body === undefined ? {} : { 'Content-Type': 'application/json' }) },
      body: body === undefined ? undefined : JSON.stringify(body)
    }).catch(error => { check(); throw error })
    const value = await response.json().catch(() => null)
    check()
    if (!response.ok) {
      // 管理业务也会返回403；用既有鉴权查询确认，不能因此销毁仍有效的草稿。
      if ([401, 403].includes(response.status)) {
        const valid = await authorized(); check()
        if (valid === false) {
          clear(true, mode === 'owner' ? '管理授权已失效，请返回成员页重新确认身份。' : '管理 Key 已失效，请重新连接。')
          check()
        }
      }
      throw Object.assign(new Error(value?.message || value?.error?.message || value?.error || '管理请求失败，请检查连接。'), { statusCode: response.status })
    }
    if (value === null) throw new Error('响应无法读取，请检查管理入口。')
    return value as T
  }
  const stateQuery = useAsyncData<ManagementState | null>(`management-${mode}`, async (): Promise<ManagementState | null> => {
    try {
      const result = await api<ManagementState>('member-management')
      updatedAt.value = Date.now(); shared.value = result.shared; refreshVersion.value++; readError.value = ''
      if (connection.value !== 'open') { activityInvalidation.dates = null; activityInvalidation.version++ }
      return result
    } catch (error) {
      if (!(error as Failure).stale) readError.value = '管理摘要读取失败，保留上次结果：' + (error as Error).message
      return stateQuery.data.value ?? null
    }
  }, { server: false, immediate: false, default: () => null, deep: false, serialize: false })
  const usageQuery = useAsyncData<ManagementUsage | null>(`management-usage-${mode}`, async (): Promise<ManagementUsage | null> => {
    const requestedDays = days.value
    try {
      const result = await api<ManagementUsage>('statistics?days=' + requestedDays)
      if (requestedDays !== days.value) return usageQuery.data.value ?? null
      usageError.value = ''; return result
    } catch (error) {
      if (!(error as Failure).stale && requestedDays === days.value) usageError.value = '用量读取失败：' + (error as Error).message
      return usageQuery.data.value ?? null
    }
  }, { server: false, immediate: false, default: () => null, deep: false, serialize: false })

  async function start() {
    if (!stateQuery.data.value || disposed) return
    const current = epoch
    if (mode === 'owner') {
      events ||= await import('../../../public/member-events.js')
      if (current !== epoch || disposed || !stateQuery.data.value) return
      events.startMemberEvents({
        onStatus: (status: SharedStatus) => { shared.value = status },
        onConnection: (value: string) => { connection.value = value },
        onExpired: () => clear(true, '管理授权已失效，请返回成员页重新登录。'),
        onInvalidate: (event: StateInvalidation) => {
          if (event.resync || event.scopes.includes('owner')) {
            if (event.resync || event.activityDates?.length) {
              activityInvalidation.dates = event.resync ? null : event.activityDates!
              activityInvalidation.version++
            }
            requestRefresh()
          }
        }
      })
    }
    if (!document.hidden && !timer) timer = setInterval(requestRefresh, 30000)
  }
  function refresh(force = false): Promise<void> {
    if (disposed) return Promise.resolve()
    pending = true
    if (!force && (busy.value || document.hidden || !stateQuery.data.value)) return Promise.resolve()
    if (refreshTask) return refreshTask
    const current = epoch
    refreshTask = (async () => {
      do { pending = false; await stateQuery.refresh({ dedupe: 'defer' }) }
      while (pending && current === epoch && !disposed && !document.hidden && !busy.value)
      if (current === epoch) await start()
    })().finally(() => { if (current === epoch) refreshTask = null })
    return refreshTask
  }
  function loadUsage(): Promise<void> {
    if (mode !== 'owner' || disposed) return Promise.resolve()
    usagePending = true
    if (usageTask) return usageTask
    const current = epoch
    usageTask = (async () => {
      do { usagePending = false; await usageQuery.refresh({ dedupe: 'defer' }) }
      while (usagePending && current === epoch && !disposed && !document.hidden && panel.value === 'usage')
    })().finally(() => { if (current === epoch) usageTask = null })
    return usageTask
  }
  function refreshPage() {
    if (disposed || busy.value || document.hidden || !stateQuery.data.value) return Promise.resolve()
    activityInvalidation.dates = null; activityInvalidation.version++
    return Promise.all([refresh(), panel.value === 'usage' ? loadUsage() : Promise.resolve()])
  }
  const refreshing = computed(() => stateQuery.pending.value || (panel.value === 'usage' && usageQuery.pending.value))
  function requestRefresh() {
    if (!stateQuery.data.value || disposed) return
    pending = true
    if (!busy.value && !document.hidden) {
      void refresh()
      if (mode === 'owner' && panel.value === 'usage') void loadUsage()
    }
  }
  const run: ManagementRun = async operation => {
    if (busy.value) return
    const current = epoch
    busy.value = true; notify('')
    try { await operation() }
    catch (error) { if (current === epoch && !(error as Failure).stale) notify((error as Error).message, true) }
    finally {
      if (current === epoch) {
        busy.value = false
        if (pending) requestRefresh()
      }
    }
  }
  function changed(text: string) { notify(text); requestRefresh() }
  async function connect(token = adminToken.value, rememberToken = remember.value) {
    clear(); activeToken = token; adminToken.value = token; connecting.value = true
    const current = epoch
    // owner的默认用量与主要摘要独立查询，不串行等待上游或损耗记录。
    await Promise.all([refresh(true), mode === 'owner' ? loadUsage() : Promise.resolve()])
    if (current !== epoch || disposed) return
    connecting.value = false
    if (stateQuery.data.value && mode === 'admin') {
      if (rememberToken) sessionStorage.setItem('nai.adminSession', token)
      else sessionStorage.removeItem('nai.adminSession')
      localStorage.removeItem('nai.adminToken')
    }
  }
  function disconnect() { clear(true, '已断开管理连接。') }
  function visibility() {
    if (document.hidden) { if (timer) clearInterval(timer); timer = null }
    else { void start(); if (mode === 'admin') requestRefresh() }
  }
  function hide() { if (timer) clearInterval(timer); timer = null }
  watch(days, () => { usageQuery.clear(); if (mode === 'owner' && stateQuery.data.value) void loadUsage() })
  watch(panel, value => { if (value === 'usage' && stateQuery.data.value) void loadUsage() })
  onMounted(() => {
    document.addEventListener('visibilitychange', visibility)
    window.addEventListener('pagehide', hide); window.addEventListener('pageshow', visibility)
    if (mode === 'owner') void connect()
    else {
      adminToken.value = sessionStorage.getItem('nai.adminSession') || localStorage.getItem('nai.adminToken') || ''
      if (adminToken.value) void connect()
    }
  })
  onBeforeUnmount(() => {
    disposed = true; clear()
    document.removeEventListener('visibilitychange', visibility)
    window.removeEventListener('pagehide', hide); window.removeEventListener('pageshow', visibility)
  })
  return { state: stateQuery.data, shared, panel, days, usage: usageQuery.data, usageLoading: usageQuery.pending,
    busy, connecting, adminToken, remember, sessionVersion, message, messageError, messageVersion, readError, usageError,
    updatedAt, connection, refreshVersion, activityInvalidation, refreshing, api, run, notify, changed, connect, disconnect, refresh, refreshPage, loadUsage }
}
