import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import { flushPromises, type VueWrapper } from '@vue/test-utils'
import { defineComponent, h, nextTick, watch } from 'vue'
import { clearNuxtData, useNuxtApp } from '#app'
import { useManagementPortal } from '../../app/composables/useManagementPortal'
import type { ManagementMode, ManagementState, ManagementUsage, ManagementUsageCounts } from '../../app/types/management'

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>(done => { resolve = done })
  return { promise, resolve }
}

class NetworkStream {
  static CLOSED = 2
  static instances: NetworkStream[] = []
  readyState = 1
  onopen: (() => void) | null = null
  onerror: (() => void) | null = null
  handlers = new Map<string, ((event: { data: string }) => void)[]>()
  close = vi.fn(() => { this.readyState = NetworkStream.CLOSED })
  constructor(readonly url: string) { NetworkStream.instances.push(this) }
  addEventListener(name: string, listener: (event: { data: string }) => void) {
    this.handlers.set(name, [...(this.handlers.get(name) || []), listener])
  }
  emit(name: string, data: unknown = {}) {
    for (const listener of this.handlers.get(name) || []) listener({ data: JSON.stringify(data) })
  }
}

type PendingRequest = { path: string; init?: RequestInit; done: boolean; reply: (status: number, body: unknown) => void }
let requests: PendingRequest[], portal: ReturnType<typeof useManagementPortal>, wrapper: VueWrapper | undefined, hidden = false
const counts: ManagementUsageCounts = { total: 3, done: 2, failed: 1, queued: 0, pending: 0, running: 0,
  refunded: 1, free: 2, v5: 0, fixed: 0, purchased: 0, averageWaitSeconds: 2, averageGenerationSeconds: 3 }
const usage = (label = 'seven days'): ManagementUsage => ({ days: [], since: label, until: '2026-10-18', timezone: 'Asia/Shanghai',
  totals: { ...counts }, members: [], models: [], recent: [] })
const state = (name = 'Alice'): ManagementState => ({ groups: [], configVersion: 1, publicBaseUrl: 'https://gateway.example',
  discordLogin: true, ownerUserId: 'owner', autoAnlasFallback: false,
  users: [{ id: 'alice', username: name, note: 'private member note', enabled: true, acceptingJobs: true,
    discordId: null, inviteExpiresAt: null, quota: null, recentActivity: { total: 3, done: 2, failed: 1, queued: 0, running: 0 } }],
  shared: { status: 'ready', subscriptionActive: true, expiresAt: null, officialCheckedAt: null, stale: false,
    v5Percent: 20, v5IsNegative: false, refillSecondsPerPercent: 60, anlasFixed: 2000, anlasPurchased: 100,
    running: 0, queued: 0, pending: 0, concurrencyLimit: 1, cooldownUntil: null, cooldownReason: null,
    quotaSyncError: false, recentFailures: 0 } })
const settle = async () => { await flushPromises(); await nextTick() }
const summaryPath = (mode: ManagementMode) => mode === 'owner' ? '/api/member/owner/member-management' : '/api/admin/member-management'
const statisticsPath = (days = 7) => `/api/member/owner/statistics?days=${days}`
const pending = (path: string) => {
  const request = requests.find(request => request.path === path && !request.done)
  expect(request, `pending ${path}`).toBeDefined()
  return request!
}
const count = (path: string) => requests.filter(request => request.path === path).length
const invalidate = (stream: NetworkStream, version: number, scopes = ['owner'], resync = false, activityDates?: string[]) => {
  stream.emit('state-invalidated', { bootId: 'management-test', version, scopes, resync, activityDates, updatedAt: new Date().toISOString() })
}
async function mount(mode: ManagementMode) {
  wrapper = await mountSuspended(defineComponent({ setup() {
    portal = useManagementPortal(mode)
    return () => h('section', [h('span', { 'data-test': 'member' }, portal.state.value?.users[0]?.username || 'disconnected')])
  } }))
  await settle()
}
async function connected(mode: ManagementMode = 'owner') {
  if (mode === 'admin') sessionStorage.setItem('nai.adminSession', 'synthetic-admin')
  await mount(mode)
  pending(summaryPath(mode)).reply(200, state())
  if (mode === 'owner') pending(statisticsPath()).reply(200, usage())
  await vi.waitFor(() => expect(portal.connecting.value).toBe(false))
  expect(portal.state.value?.users[0]?.username).toBe('Alice')
  if (mode === 'owner') {
    await vi.waitFor(() => expect(NetworkStream.instances).toHaveLength(1))
    const stream = NetworkStream.instances[0]!
    invalidate(stream, 1, ['owner'], true)
    await vi.waitFor(() => expect(count(summaryPath(mode))).toBe(2))
    pending(summaryPath(mode)).reply(200, state()); pending(statisticsPath()).reply(200, usage())
    await settle()
    return stream
  }
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
  useNuxtApp().runWithContext(() => clearNuxtData(key => key.startsWith('management-')))
  requests = []; hidden = false; NetworkStream.instances = []; wrapper = undefined
  sessionStorage.clear(); localStorage.clear()
  vi.spyOn(document, 'hidden', 'get').mockImplementation(() => hidden)
  vi.stubGlobal('EventSource', NetworkStream)
  vi.stubGlobal('fetch', vi.fn((input: string, init?: RequestInit) => {
    const result = deferred<Response>()
    const request: PendingRequest = { path: input, init, done: false, reply(status, body) {
      if (request.done) throw new Error('Response already delivered')
      request.done = true
      result.resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }))
    } }
    requests.push(request)
    return result.promise
  }))
})
afterEach(async () => {
  wrapper?.unmount(); wrapper = undefined
  for (const request of requests) if (!request.done) request.reply(503, { error: 'fixture closed' })
  await settle()
  useNuxtApp().runWithContext(() => clearNuxtData(key => key.startsWith('management-')))
  sessionStorage.clear(); localStorage.clear()
  vi.unstubAllGlobals(); vi.restoreAllMocks(); vi.useRealTimers()
})

describe('真实 Nuxt useAsyncData 管理会话', () => {
  it('页首手动刷新同时重读摘要、当前用量和完整热力图，不触发上游操作', async () => {
    const stream = (await connected())!
    stream.onopen?.()
    const version = portal.activityInvalidation.version
    const reading = portal.refreshPage()
    expect(portal.activityInvalidation).toEqual({ version: version + 1, dates: null })
    pending(summaryPath('owner')).reply(200, state('Fresh'))
    pending(statisticsPath()).reply(200, usage('Fresh usage'))
    await reading; await settle()
    expect(portal.state.value?.users[0]?.username).toBe('Fresh')
    expect(portal.usage.value?.since).toBe('Fresh usage')
    expect(requests.every(request => !request.init?.method || request.init.method === 'GET')).toBe(true)
    portal.panel.value = 'members'; await settle()
    const previous = count(statisticsPath())
    const next = portal.refreshPage()
    pending(summaryPath('owner')).reply(200, state()); await next
    expect(count(statisticsPath())).toBe(previous)
  })

  it('owner 摘要与默认七天统计并行，私有数据不写入可序列化 payload', async () => {
    await mount('owner')
    expect(requests.map(request => request.path)).toEqual([summaryPath('owner'), statisticsPath()])
    expect(portal.state.value).toBeNull(); expect(portal.usage.value).toBeNull()
    pending(statisticsPath()).reply(200, usage()); await settle()
    expect(portal.usage.value?.since).toBe('seven days'); expect(portal.state.value).toBeNull()
    pending(summaryPath('owner')).reply(200, state()); await settle()
    expect(portal.state.value?.users[0]?.note).toBe('private member note')
    expect(useNuxtApp().payload.data['management-owner']).toBeUndefined()
    expect(useNuxtApp().payload.data['management-usage-owner']).toBeUndefined()
    expect(requests.every(request => request.init?.credentials === 'same-origin')).toBe(true)
    expect(requests.every(request => !new Headers(request.init?.headers).has('x-admin-token'))).toBe(true)
  })

  it('owner 业务写入403经 me 确认有效后保留会话与草稿承载，失效确认才清除', async () => {
    const stream = (await connected())!, version = portal.sessionVersion.value
    stream.onopen?.()
    invalidate(stream, 2, ['owner'], false, ['2026-10-17']); await settle()
    pending(summaryPath('owner')).reply(200, state()); pending(statisticsPath()).reply(200, usage()); await settle()
    const signal = portal.activityInvalidation, activityVersion = signal.version
    const write = portal.run(async () => { await portal.api('member-transfer', { amount: 1 }) })
    pending('/api/member/owner/member-transfer').reply(403, { error: '不能转移此资源' }); await settle()
    pending('/api/member/me').reply(200, { owner: true }); await write
    expect(portal.state.value?.users[0]?.username).toBe('Alice')
    expect(portal.sessionVersion.value).toBe(version)
    expect(signal.dates).toEqual(['2026-10-17'])
    expect(portal.message.value).toBe('不能转移此资源'); expect(portal.messageError.value).toBe(true)
    expect(stream.close).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(1)
    const refresh = portal.refresh(), old = pending(summaryPath('owner'))
    const expired = portal.api('member-transfer', {}).catch(error => error)
    pending('/api/member/owner/member-transfer').reply(401, { error: 'expired' }); await settle()
    pending('/api/member/me').reply(403, { error: 'expired' }); await expired; await settle()
    expect(portal.state.value).toBeNull(); expect(portal.usage.value).toBeNull(); expect(portal.shared.value).toBeNull()
    expect(signal.dates).toBeNull()
    expect(portal.sessionVersion.value).toBeGreaterThan(version)
    expect(stream.close).toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0)
    old.reply(200, state('Expired owner')); await refresh; await settle()
    expect(portal.state.value).toBeNull(); expect(portal.activityInvalidation).toBe(signal)
    expect(signal).toEqual({ version: activityVersion, dates: null })
  })

  it('owner 身份被撤销时即使 me 返回200也清理管理私有数据', async () => {
    const stream = (await connected())!
    const write = portal.api('member-transfer', {}).catch(error => error)
    pending('/api/member/owner/member-transfer').reply(403, { error: 'owner only' }); await settle()
    pending('/api/member/me').reply(200, { owner: false }); await write; await settle()
    expect(portal.state.value).toBeNull(); expect(portal.usage.value).toBeNull()
    expect(stream.close).toHaveBeenCalled()
  })

  it('摘要503和鉴权核验503保留已有私有视图，错误不能误判成会话过期', async () => {
    const stream = (await connected())!, version = portal.sessionVersion.value, updatedAt = portal.updatedAt.value
    stream.onopen?.()
    const activityVersion = portal.activityInvalidation.version
    const refresh = portal.refresh()
    pending(summaryPath('owner')).reply(503, { error: 'temporarily unavailable' }); await refresh
    expect(portal.state.value?.users[0]?.username).toBe('Alice'); expect(portal.updatedAt.value).toBe(updatedAt)
    expect(portal.readError.value).toContain('temporarily unavailable')
    expect(portal.activityInvalidation.version).toBe(activityVersion)
    const write = portal.run(async () => { await portal.api('member-transfer', {}) })
    pending('/api/member/owner/member-transfer').reply(403, { error: 'permission check failed' }); await settle()
    pending('/api/member/me').reply(503, { error: 'temporarily unavailable' }); await write
    expect(portal.state.value?.users[0]?.username).toBe('Alice'); expect(portal.usage.value?.since).toBe('seven days')
    expect(portal.sessionVersion.value).toBe(version); expect(stream.close).not.toHaveBeenCalled()
    expect(portal.message.value).toBe('permission check failed')
    await stream.onerror?.()
    expect(portal.connection.value).toBe('retrying')
    await vi.advanceTimersByTimeAsync(30000)
    pending(summaryPath('owner')).reply(200, state()); pending(statisticsPath()).reply(200, usage()); await settle()
    expect(portal.activityInvalidation).toEqual({ version: activityVersion + 1, dates: null })
  })

  it('admin 仅使用管理 token 且无成员 SSE，业务403保留，确认失效清凭据和敏感状态', async () => {
    localStorage.setItem('nai.adminToken', 'synthetic-admin')
    await mount('admin')
    expect(portal.adminToken.value).toBe('synthetic-admin')
    pending(summaryPath('admin')).reply(200, state())
    await vi.waitFor(() => expect(portal.connecting.value).toBe(false))
    expect(localStorage.getItem('nai.adminToken')).toBeNull()
    expect(sessionStorage.getItem('nai.adminSession')).toBe('synthetic-admin')
    localStorage.setItem('nai.adminToken', 'obsolete-admin')
    expect(NetworkStream.instances).toHaveLength(0)
    expect(requests.every(request => request.path.startsWith('/api/admin/'))).toBe(true)
    const version = portal.sessionVersion.value
    const write = portal.run(async () => { await portal.api('member-transfer', {}) })
    pending('/api/admin/member-transfer').reply(403, { error: 'transfer not permitted' }); await settle()
    pending('/api/admin/ping').reply(200, { ok: true }); await write
    expect(portal.state.value?.users[0]?.username).toBe('Alice'); expect(portal.sessionVersion.value).toBe(version)
    expect(sessionStorage.getItem('nai.adminSession')).toBe('synthetic-admin')
    expect(requests.every(request => new Headers(request.init?.headers).get('x-admin-token') === 'synthetic-admin')).toBe(true)
    expect(requests.every(request => !new Headers(request.init?.headers).has('authorization'))).toBe(true)
    const expired = portal.api('summary').catch(error => error)
    pending('/api/admin/summary').reply(401, { error: 'wrong token' }); await settle()
    pending('/api/admin/ping').reply(403, { error: 'wrong token' }); await expired; await settle()
    expect(portal.state.value).toBeNull(); expect(portal.shared.value).toBeNull(); expect(portal.adminToken.value).toBe('')
    expect(sessionStorage.getItem('nai.adminSession')).toBeNull(); expect(localStorage.getItem('nai.adminToken')).toBeNull()
    expect(vi.getTimerCount()).toBe(0); expect(NetworkStream.instances).toHaveLength(0)
  })

  it('admin 隐藏时停止轮询，恢复后立即读取；卸载清视图且不继续请求', async () => {
    await connected('admin')
    localStorage.setItem('nai.adminToken', 'obsolete-admin')
    const reconnect = portal.connect('synthetic-admin', false)
    pending(summaryPath('admin')).reply(200, state()); await reconnect
    expect(localStorage.getItem('nai.adminToken')).toBeNull()
    expect(sessionStorage.getItem('nai.adminSession')).toBeNull()
    const summary = summaryPath('admin'), initial = count(summary)
    hidden = true; document.dispatchEvent(new Event('visibilitychange')); await settle()
    expect(vi.getTimerCount()).toBe(0)
    await vi.advanceTimersByTimeAsync(90000); await portal.refresh(); expect(count(summary)).toBe(initial)
    hidden = false; document.dispatchEvent(new Event('visibilitychange')); await settle()
    expect(count(summary)).toBe(initial + 1)
    pending(summary).reply(200, state('Visible admin')); await settle()
    expect(portal.state.value?.users[0]?.username).toBe('Visible admin'); expect(vi.getTimerCount()).toBe(1)
    wrapper!.unmount(); wrapper = undefined
    await vi.advanceTimersByTimeAsync(90000)
    expect(portal.state.value).toBeNull(); expect(portal.adminToken.value).toBe(''); expect(vi.getTimerCount()).toBe(0)
    expect(count(summary)).toBe(initial + 1); expect(NetworkStream.instances).toHaveLength(0)
  })

  for (const status of [200, 401]) {
    it(`admin 重连之后旧连接的 ${status} 不覆盖新数据或重新校验旧身份`, async () => {
      await connected('admin')
      const refresh = portal.refresh(), old = pending(summaryPath('admin'))
      portal.disconnect()
      const reconnect = portal.connect('new-synthetic-admin')
      const current = requests.find(request => request.path === summaryPath('admin') && request !== old && !request.done)!
      expect(current).toBeDefined(); expect(new Headers(current.init?.headers).get('x-admin-token')).toBe('new-synthetic-admin')
      current.reply(200, state('Bob')); await reconnect
      const version = portal.sessionVersion.value
      old.reply(status, status === 200 ? state('Old Alice') : { error: 'old expired' }); await refresh; await settle()
      expect(portal.state.value?.users[0]?.username).toBe('Bob'); expect(portal.sessionVersion.value).toBe(version)
      expect(sessionStorage.getItem('nai.adminSession')).toBe('new-synthetic-admin')
      expect(count('/api/admin/ping')).toBe(0)
    })
  }

  it('owner 日期逐条通知，资料补读合并及busy延后不丢日期；resync全量校准', async () => {
    const stream = (await connected())!, summary = summaryPath('owner'), initial = count(summary), initialUsage = count(statisticsPath())
    stream.onopen?.()
    const signal = portal.activityInvalidation, dates: (string[] | null)[] = []
    const stop = watch(() => signal.version, () => dates.push(signal.dates && [...signal.dates]), { flush: 'sync' })
    try {
      invalidate(stream, 2, ['shared', 'member'], false, ['2026-10-15']); await settle()
      expect(count(summary)).toBe(initial); expect(dates).toEqual([])
      invalidate(stream, 3); await settle()
      expect(count(summary)).toBe(initial + 1); expect(dates).toEqual([])
      invalidate(stream, 4, ['owner'], false, ['2026-10-16'])
      invalidate(stream, 5, ['owner'], false, ['2026-10-17'])
      expect(dates).toEqual([['2026-10-16'], ['2026-10-17']])
      invalidate(stream, 6); await settle()
      expect(count(summary)).toBe(initial + 1); expect(count(statisticsPath())).toBe(initialUsage + 1)
      pending(summary).reply(200, state('First')); pending(statisticsPath()).reply(200, usage('First')); await settle()
      expect(count(summary)).toBe(initial + 2); expect(count(statisticsPath())).toBe(initialUsage + 2)
      pending(summary).reply(200, state('Latest')); pending(statisticsPath()).reply(200, usage('Latest')); await settle()
      const operation = deferred<void>(), saving = portal.run(() => operation.promise)
      invalidate(stream, 7); invalidate(stream, 8); await settle()
      expect(count(summary)).toBe(initial + 2)
      operation.resolve(); await saving; await settle()
      expect(count(summary)).toBe(initial + 3); expect(count(statisticsPath())).toBe(initialUsage + 3)
      pending(summary).reply(200, state('After save')); pending(statisticsPath()).reply(200, usage('After save')); await settle()
      expect(portal.state.value?.users[0]?.username).toBe('After save')
      expect(dates).toEqual([['2026-10-16'], ['2026-10-17']])
      invalidate(stream, 9, ['owner'], true, ['2026-10-17']); await settle()
      expect(dates).toEqual([['2026-10-16'], ['2026-10-17'], null])
      pending(summary).reply(200, state()); pending(statisticsPath()).reply(200, usage()); await settle()
      expect(dates).toHaveLength(3); expect(portal.activityInvalidation).toBe(signal)
    } finally { stop() }
  })

  it('隐藏页停轮询，恢复由 SSE 校准；卸载清私有状态并忽略迟到响应和旧事件', async () => {
    const stream = (await connected())!, summary = summaryPath('owner'), initial = count(summary)
    hidden = true; document.dispatchEvent(new Event('visibilitychange')); await settle()
    expect(stream.close).toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0)
    await vi.advanceTimersByTimeAsync(90000); await portal.refresh()
    invalidate(stream, 9, ['owner'], true); await settle(); expect(count(summary)).toBe(initial)
    hidden = false; document.dispatchEvent(new Event('visibilitychange')); await settle()
    expect(NetworkStream.instances).toHaveLength(2)
    const resumed = NetworkStream.instances[1]!
    invalidate(resumed, 10, ['owner'], true); await settle()
    expect(count(summary)).toBe(initial + 1)
    const old = pending(summary), oldUsage = pending(statisticsPath())
    wrapper!.unmount(); wrapper = undefined
    expect(resumed.close).toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0)
    old.reply(200, state('Unmounted')); oldUsage.reply(200, usage('Unmounted'))
    invalidate(resumed, 11, ['owner'], true); await settle(); await vi.advanceTimersByTimeAsync(90000)
    expect(portal.state.value).toBeNull(); expect(portal.usage.value).toBeNull(); expect(portal.shared.value).toBeNull()
    expect(count(summary)).toBe(initial + 1)
  })

  it('days 切换后旧范围响应不能显示，最终读取最后选择的范围', async () => {
    await connected()
    const original = portal.loadUsage(), oldSeven = pending(statisticsPath())
    portal.days.value = 30; await settle()
    expect(portal.usage.value).toBeNull()
    oldSeven.reply(200, usage('old seven')); await settle()
    expect(portal.usage.value?.since).not.toBe('old seven')
    const oldThirty = pending(statisticsPath(30))
    portal.days.value = 1; await settle()
    oldThirty.reply(200, usage('old thirty')); await settle()
    expect(portal.usage.value?.since).not.toBe('old thirty')
    pending(statisticsPath(1)).reply(200, usage('current day')); await original; await settle()
    expect(portal.usage.value?.since).toBe('current day')
  })
})
