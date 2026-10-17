import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mountSuspended, mockNuxtImport } from '@nuxt/test-utils/runtime'
import { flushPromises, type VueWrapper } from '@vue/test-utils'
import { defineComponent, h, nextTick, watch } from 'vue'
import { clearNuxtData, useNuxtApp } from '#app'
import { useMemberPortal } from '../../app/composables/useMemberPortal'
import type { LoginOptions, MemberProfile } from '../../app/types/member'

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

type PendingRequest = {
  path: string
  init?: RequestInit
  done: boolean
  reply: (status: number, body: unknown) => void
}
type PublicConfigRequest = (url: string, options?: { retry: number; credentials: RequestCredentials }) => Promise<LoginOptions>
const options: LoginOptions = { discord: false, ready: false, origin: null }
const profile = (username = 'Alice'): MemberProfile => ({ username, owner: false, sessionExpiresAt: Date.now() + 3600000,
  acceptingJobs: true, quota: null, jobs: [], lateResults: [] })
let requests: PendingRequest[], publicReply: ReturnType<typeof deferred<LoginOptions>>, publicFetch: ReturnType<typeof vi.fn<PublicConfigRequest>>
let portal: ReturnType<typeof useMemberPortal>, wrapper: VueWrapper | undefined, hidden = false

// Nuxt 4 把 $fetch 自动导入为模块绑定；只替换这个传输函数，保留真实 useAsyncData。
mockNuxtImport('$fetch', () => (...args: Parameters<PublicConfigRequest>) => publicFetch(...args))

const Harness = defineComponent({
  setup() {
    portal = useMemberPortal()
    return () => h('section', [h('span', { 'data-test': 'member' }, portal.profile.value?.username || 'anonymous'),
      h('span', { 'data-test': 'error' }, portal.error.value)])
  },
})
const settle = async () => { await flushPromises(); await nextTick() }
const pending = (path: string) => {
  const request = requests.find(request => request.path === path && !request.done)
  expect(request, `pending ${path}`).toBeDefined()
  return request!
}
const meCount = () => requests.filter(request => request.path === 'me').length
const invalidate = (stream: NetworkStream, version: number, scopes = ['member'], resync = false, activityDates?: string[]) => {
  stream.emit('state-invalidated', { bootId: 'test-boot', version, scopes, resync, activityDates, updatedAt: new Date().toISOString() })
}
async function mount() {
  wrapper = await mountSuspended(Harness)
  await vi.waitFor(() => { expect(publicFetch).toHaveBeenCalledTimes(1); expect(meCount()).toBe(1) })
}
async function authenticated() {
  await mount()
  publicReply.resolve(options); pending('me').reply(200, profile())
  await vi.waitFor(() => { expect(portal.initializing.value).toBe(false); expect(NetworkStream.instances).toHaveLength(1) })
  expect(wrapper!.get('[data-test="member"]').text()).toBe('Alice')
  const stream = NetworkStream.instances[0]!
  // 首次 SSE 校准使用真实客户端逻辑，随后才检查同 boot 的局部通知。
  invalidate(stream, 1, ['member'], true)
  await vi.waitFor(() => expect(meCount()).toBe(2))
  pending('me').reply(200, profile()); await settle()
  return stream
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
  useNuxtApp().runWithContext(() => clearNuxtData('member-profile'))
  requests = []; hidden = false; NetworkStream.instances = []; wrapper = undefined
  vi.spyOn(document, 'hidden', 'get').mockImplementation(() => hidden)
  publicReply = deferred<LoginOptions>()
  publicFetch = vi.fn<PublicConfigRequest>((url) => {
    expect(url).toBe('/api/member/login-options')
    return publicReply.promise
  })
  vi.stubGlobal('EventSource', NetworkStream)
  vi.stubGlobal('fetch', vi.fn((input: string, init?: RequestInit) => {
    const result = deferred<Response>()
    const request: PendingRequest = { path: input.replace('/api/member/', ''), init, done: false, reply(status, body) {
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
  publicReply.resolve(options)
  for (const request of requests) if (!request.done) request.reply(503, { error: 'fixture closed' })
  await settle()
  useNuxtApp().runWithContext(() => clearNuxtData('member-profile'))
  vi.unstubAllGlobals(); vi.restoreAllMocks(); vi.useRealTimers()
})

describe('真实 Nuxt useAsyncData 成员会话', () => {
  it('页首刷新校准完整活动并通知调用列表，不读取 API Key，成功后清除过时读取错误', async () => {
    const stream = await authenticated()
    stream.onopen?.()
    const failed = portal.refresh()
    pending('me').reply(503, { error: 'offline' }); await failed
    const version = portal.refreshVersion.value, activityVersion = portal.activityInvalidation.version
    const reading = portal.refreshPage()
    expect(portal.activityInvalidation).toEqual({ version: activityVersion + 1, dates: null })
    pending('me').reply(200, profile('Fresh')); await reading; await settle()
    expect(portal.refreshVersion.value).toBe(version + 1)
    expect(portal.error.value).toBe('')
    expect(requests.every(request => request.path === 'me')).toBe(true)
  })

  it('公开登录配置与私有 me 并行，匿名401不会丢弃后到的公开配置', async () => {
    await mount()
    expect(publicFetch).toHaveBeenCalledWith('/api/member/login-options', { retry: 0, credentials: 'same-origin' })
    expect(requests).toHaveLength(1)
    pending('me').reply(401, { error: '请先登录' }); await settle()
    expect(portal.profile.value).toBeNull(); expect(portal.loginOptions.value).toBeNull()
    expect(portal.initializing.value).toBe(true)
    publicReply.resolve(options)
    await vi.waitFor(() => expect(portal.initializing.value).toBe(false))
    expect(portal.loginOptions.value).toEqual(options)
    expect(portal.error.value).toBe(''); expect(NetworkStream.instances).toHaveLength(0)
  })

  for (const via of ['SSE过期', '退出登录']) {
    it(`${via}清除profile，旧me成功不能恢复私有数据`, async () => {
      const stream = await authenticated()
      stream.onopen?.()
      invalidate(stream, 2, ['member'], false, ['2026-10-17'])
      const refresh = portal.refresh(); await settle()
      const old = pending('me'), signal = portal.activityInvalidation, version = signal.version
      expect(signal.dates).toEqual(['2026-10-17'])
      if (via === 'SSE过期') stream.emit('auth-expired')
      else {
        const logout = portal.logout(); await settle()
        const request = pending('logout')
        expect(request.init?.method).toBe('POST'); expect(request.init?.body).toBe('{}')
        request.reply(200, { ok: true }); await logout
      }
      await nextTick()
      expect(portal.profile.value).toBeNull(); expect(portal.shared.value).toBeNull()
      expect(signal.dates).toBeNull()
      expect(stream.close).toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0)
      old.reply(200, profile('Expired Alice')); await refresh; await settle()
      expect(portal.profile.value).toBeNull()
      expect(portal.activityInvalidation).toBe(signal); expect(signal).toEqual({ version, dates: null })
      expect(wrapper!.get('[data-test="member"]').text()).toBe('anonymous')
    })
  }

  it('旧会话的迟到401不能注销之后登录的新会话', async () => {
    await authenticated()
    const oldResult = portal.api('key').catch(error => error)
    const old = pending('key')
    const logout = portal.logout(); pending('logout').reply(200, { ok: true }); await logout
    const login = portal.login({ username: 'Bob', password: 'synthetic-password' })
    pending('login').reply(200, { ok: true }); await settle()
    pending('me').reply(200, profile('Bob')); await login; await settle()
    expect(portal.profile.value?.username).toBe('Bob')
    old.reply(401, { error: 'old session expired' })
    await expect(oldResult).resolves.toMatchObject({ stale: true })
    await settle()
    expect(portal.profile.value?.username).toBe('Bob'); expect(portal.error.value).toBe('')
    expect(wrapper!.get('[data-test="member"]').text()).toBe('Bob')
  })

  it('局部日期信号不受资料补读合并影响，同tick日期逐条可见，resync才全量校准', async () => {
    const stream = await authenticated(), count = meCount()
    stream.onopen?.()
    const signal = portal.activityInvalidation, dates: (string[] | null)[] = []
    const stop = watch(() => signal.version, () => dates.push(signal.dates && [...signal.dates]), { flush: 'sync' })
    try {
      invalidate(stream, 2, ['shared', 'owner'], false, ['2026-10-15']); await settle()
      expect(meCount()).toBe(count); expect(dates).toEqual([])
      invalidate(stream, 3); await settle()
      expect(meCount()).toBe(count + 1); expect(dates).toEqual([])
      invalidate(stream, 4, ['member'], false, ['2026-10-16'])
      invalidate(stream, 5, ['member'], false, ['2026-10-17'])
      expect(dates).toEqual([['2026-10-16'], ['2026-10-17']])
      invalidate(stream, 6); await settle()
      expect(meCount()).toBe(count + 1)
      expect(requests.filter(request => request.path === 'me' && !request.done)).toHaveLength(1)
      pending('me').reply(200, profile('First')); await settle()
      expect(meCount()).toBe(count + 2)
      pending('me').reply(200, profile('Latest')); await settle()
      expect(meCount()).toBe(count + 2); expect(portal.profile.value?.username).toBe('Latest')
      expect(dates).toEqual([['2026-10-16'], ['2026-10-17']])
      invalidate(stream, 7, ['member'], true, ['2026-10-17']); await settle()
      expect(dates).toEqual([['2026-10-16'], ['2026-10-17'], null])
      pending('me').reply(200, profile()); await settle()
      expect(dates).toHaveLength(3); expect(portal.activityInvalidation).toBe(signal)
    } finally { stop() }
  })

  it('503保留最后profile并显示错误，隐藏页不轮询，恢复后由SSE校准', async () => {
    const stream = await authenticated(), updated = portal.updatedAt.value, version = portal.refreshVersion.value
    stream.onopen?.()
    const activityVersion = portal.activityInvalidation.version
    const refresh = portal.refresh(); pending('me').reply(503, { error: 'upstream temporarily unavailable' })
    await refresh; await settle()
    expect(portal.profile.value?.username).toBe('Alice')
    expect(portal.error.value).toContain('保留上次结果'); expect(portal.error.value).toContain('upstream temporarily unavailable')
    expect(portal.updatedAt.value).toBe(updated); expect(portal.refreshVersion.value).toBe(version)
    expect(portal.activityInvalidation.version).toBe(activityVersion)
    await stream.onerror?.()
    expect(portal.connection.value).toBe('retrying')
    await vi.advanceTimersByTimeAsync(30000)
    pending('me').reply(200, profile()); await settle()
    expect(portal.activityInvalidation).toEqual({ version: activityVersion + 1, dates: null })
    const count = meCount()
    hidden = true; document.dispatchEvent(new Event('visibilitychange')); await settle()
    expect(stream.close).toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0)
    await vi.advanceTimersByTimeAsync(90000); await portal.refresh(); expect(meCount()).toBe(count)
    hidden = false; document.dispatchEvent(new Event('visibilitychange')); await settle()
    expect(NetworkStream.instances).toHaveLength(2)
    invalidate(NetworkStream.instances[1]!, 7, ['member'], true); await settle()
    expect(meCount()).toBe(count + 1)
    pending('me').reply(200, profile('Resumed')); await settle()
    expect(portal.profile.value?.username).toBe('Resumed')
    expect(vi.getTimerCount()).toBe(1)
  })

  it('卸载关闭连接和计时，旧事件及迟到请求不能恢复profile或发起补读', async () => {
    const stream = await authenticated(), version = portal.refreshVersion.value
    const refresh = portal.refresh(); const old = pending('me'), count = meCount()
    wrapper!.unmount(); wrapper = undefined
    expect(stream.close).toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0)
    invalidate(stream, 9, ['member'], true)
    old.reply(200, profile('Unmounted')); await refresh; await settle()
    await vi.advanceTimersByTimeAsync(90000)
    expect(portal.profile.value).toBeNull(); expect(portal.refreshVersion.value).toBe(version)
    expect(meCount()).toBe(count)
  })
})
