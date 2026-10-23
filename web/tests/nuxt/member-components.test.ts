import { defineComponent, h } from 'vue'
import { NMessageProvider, NDataTable, NSelect } from 'naive-ui'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import { enableAutoUnmount, flushPromises } from '@vue/test-utils'
import MemberApiKey from '../../app/components/MemberApiKey.vue'
import MemberCalls from '../../app/components/MemberCalls.vue'
import MemberBatch from '../../app/components/MemberBatch.vue'
import type { MemberApi, MemberBatch as BatchSnapshot, MemberJob } from '../../app/types/member'

enableAutoUnmount(afterEach)
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers() })

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>(done => { resolve = done })
  return { promise, resolve }
}
// Mock 保留传输观测；泛型返回值由与生产 API 相同的类型边界提供。
function memberApi(transport: (...args: Parameters<MemberApi>) => Promise<unknown>): MemberApi {
  return async <T>(...args: Parameters<MemberApi>) => await transport(...args) as T
}
function mountKey(api: MemberApi) {
  return mountSuspended(defineComponent({ setup: () => () => h(NMessageProvider, null, { default: () => h(MemberApiKey, { api }) }) }))
}
const requestId = 'batch/a?b'
const requestPath = 'requests/batch%2Fa%3Fb'
function snapshot(model = 'synthetic-model'): BatchSnapshot {
  return { id: requestId, cancellable: true,
    counts: { total: 2, queued: 1, pending: 0, running: 1, done: 0, failed: 0 },
    jobs: [{ id: 'running', index: 0, model, status: 'running', cancelled: false },
      { id: 'queued', index: 1, model, status: 'queued', cancelled: false }] }
}
function cancelledSnapshot(): BatchSnapshot {
  const result = snapshot('after-cancel')
  result.cancellable = false; result.cancelledCount = 1
  result.counts.queued = 0; result.counts.failed = 1
  result.jobs[1] = { ...result.jobs[1]!, status: 'failed', cancelled: true }
  return result
}

describe('MemberApiKey', () => {
  it('点击才读取和复制，隐藏清空，并忽略隐藏后的迟到复制完成', async () => {
    const api = vi.fn<MemberApi>().mockResolvedValue({ apiKey: 'synthetic-member-key', baseUrl: '/v1' })
    const copying = deferred<void>()
    const clipboard = vi.spyOn(navigator.clipboard, 'writeText').mockReturnValue(copying.promise)
    const wrapper = await mountKey(memberApi(api))
    expect(api).not.toHaveBeenCalled(); expect(clipboard).not.toHaveBeenCalled()
    expect(wrapper.find('#keyPanel').exists()).toBe(false)
    await wrapper.get('#showKey').trigger('click'); await flushPromises()
    expect(api).toHaveBeenCalledExactlyOnceWith('key')
    expect((wrapper.get('#apiKey').element as HTMLInputElement).value).toBe('synthetic-member-key')
    expect((wrapper.get('#apiBaseUrl').element as HTMLInputElement).value).toBe('/v1')
    expect(clipboard).not.toHaveBeenCalled()
    await wrapper.get('#copyKey').trigger('click')
    expect(clipboard).toHaveBeenCalledExactlyOnceWith('synthetic-member-key')
    await wrapper.get('#hideKey').trigger('click')
    copying.resolve(undefined); await flushPromises()
    expect(wrapper.find('#keyPanel').exists()).toBe(false)
    expect(wrapper.find('#apiKey').exists()).toBe(false)
    expect(wrapper.text()).not.toContain('已复制')
    expect(wrapper.html()).not.toContain('synthetic-member-key')
  })

  it('卸载后迟到的 Key 响应不能出现在新组件中', async () => {
    const result = deferred<{ apiKey: string; baseUrl: string }>()
    const api = vi.fn<MemberApi>().mockReturnValue(result.promise)
    const old = await mountKey(memberApi(api))
    await old.get('#showKey').trigger('click')
    old.unmount()
    const nextApi = vi.fn<MemberApi>()
    const current = await mountKey(memberApi(nextApi))
    result.resolve({ apiKey: 'synthetic-late-secret', baseUrl: '/v1' }); await flushPromises()
    expect(nextApi).not.toHaveBeenCalled()
    expect(current.find('#keyPanel').exists()).toBe(false)
    expect(current.html()).not.toContain('synthetic-late-secret')
  })
})

it('调用列表保留签名相对链接及 HTTP(S)、拒绝脚本链接，缺失分池显示未知并按批次去重', async () => {
  const relative = '/images/synthetic.png?expires=1800000000&signature=synthetic-signature'
  const hostile = '<img src=x onerror=alert(1)>'
  const jobs: MemberJob[] = [
    { id: 'one', requestId, createdAt: '2026-10-17T00:00:00Z', model: hostile, status: 'failed', imageUrl: relative,
      charge: { status: 'refunded', resource: 'anlas', amount: 42, fixed: null } },
    { id: 'two', requestId, createdAt: '2026-10-17T00:01:00Z', model: 'second', status: 'done', lateResultAvailable: true, imageUrl: 'https://images.example/result.png' },
    { id: 'three', createdAt: '2026-10-17T00:02:00Z', model: 'third', status: 'done', imageUrl: 'http://images.example/result.png' },
    { id: 'four', createdAt: '2026-10-17T00:03:00Z', model: 'fourth', status: 'done', imageUrl: 'javascript:alert(1)' }
  ]
  const api = vi.fn<MemberApi>().mockResolvedValue({ jobs, total: jobs.length, until: '2026-10-17T03:00:00.000Z' })
  const wrapper = await mountSuspended(MemberCalls, { props: { api: memberApi(api), jobs, refreshVersion: 0,
    lateResults: [
      { id: 'late', model: 'late', createdAt: '2026-10-16T00:00:00Z', lateResultAt: '2026-10-17T00:00:00Z', imageUrl: relative, charge: jobs[0]!.charge },
      { id: 'invalid-late', model: 'invalid', createdAt: '2026-10-16T00:00:00Z', lateResultAt: '2026-10-17T00:00:00Z', imageUrl: 'javascript:alert(1)' }
    ] } })
  await flushPromises()
  expect(wrapper.findAll('#jobs tbody tr')).toHaveLength(4)
  expect(wrapper.get('#jobs').text()).toContain('订阅 未知 / 购买 未知')
  expect(wrapper.get('#jobs').text()).toContain('已退款（本站额度）')
  expect(wrapper.text()).toContain(hostile); expect(wrapper.find('img').exists()).toBe(false)
  expect(wrapper.get('#jobs').text()).toContain('领取迟到结果（退款保留）')
  const hrefs = wrapper.findAll('a').map(link => link.attributes('href'))
  expect(hrefs).toContain(relative); expect(hrefs).toContain('https://images.example/result.png'); expect(hrefs).toContain('http://images.example/result.png')
  expect(hrefs.some(href => href?.startsWith('javascript:'))).toBe(false)
  for (const link of wrapper.findAll('a')) {
    expect(link.attributes('target')).toBe('_blank')
    expect(link.attributes('referrerpolicy')).toBe('no-referrer')
    expect(link.attributes('rel')).toBe('noopener noreferrer')
  }
  const batches = wrapper.findAllComponents(MemberBatch)
  expect(batches).toHaveLength(1)
  expect(batches[0]!.element.closest('.calls-scroll')).toBeNull()
  expect(api).toHaveBeenCalledTimes(1)
  expect(api.mock.calls[0]![0]).toContain('history?page=1&pageSize=10')
})

it('成员历史分页参数传到服务端，筛选回首页，过时查询不能覆盖新结果', async () => {
  const late = deferred<{ jobs: MemberJob[]; total: number; until: string }>()
  const job = (id: string): MemberJob => ({ id, createdAt: '2026-10-17T00:00:00Z', model: id, status: 'done' })
  const first = { jobs: [job('first')], total: 80, until: '2026-10-17T03:00:00.000Z' }
  const api = vi.fn<MemberApi>().mockResolvedValueOnce(first).mockReturnValueOnce(late.promise)
    .mockResolvedValue({ ...first, jobs: [job('filtered')], total: 1 })
  const wrapper = await mountSuspended(MemberCalls, { props: { api: memberApi(api), jobs: [], lateResults: [], refreshVersion: 0 } })
  await flushPromises()
  const table = wrapper.findComponent(NDataTable)
  table.vm.$emit('update:page', 2); await flushPromises()
  const page = new URL(api.mock.calls[1]![0], 'https://synthetic.invalid/').searchParams
  expect(page.get('page')).toBe('2'); expect(page.get('until')).toBe(first.until)
  wrapper.findAllComponents(NSelect).find(select => select.attributes('aria-label') === '筛选调用状态')!.vm.$emit('update:value', 'late')
  await flushPromises()
  expect(api.mock.calls.at(-1)![0]).toContain('page=1')
  expect(api.mock.calls.at(-1)![0]).toContain('status=late')
  late.resolve({ ...first, jobs: [job('stale')] }); await flushPromises()
  expect(wrapper.get('#jobs').text()).toContain('filtered'); expect(wrapper.get('#jobs').text()).not.toContain('stale')
  expect(wrapper.text()).toContain('原退款保留')
})

it('搜索输入防抖回首页，清空立即恢复；卸载后不继续查询', async () => {
  const api = vi.fn<MemberApi>().mockResolvedValue({ jobs: [], total: 80, until: '2026-10-17T03:00:00.000Z' })
  const wrapper = await mountSuspended(MemberCalls, { props: { api: memberApi(api), jobs: [], lateResults: [], refreshVersion: 0 } })
  await flushPromises()
  wrapper.findComponent(NDataTable).vm.$emit('update:page', 2); await flushPromises()
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
  const input = wrapper.get('.table-search input')
  await input.setValue('v'); await vi.advanceTimersByTimeAsync(200)
  await input.setValue('v45'); await vi.advanceTimersByTimeAsync(299)
  expect(api).toHaveBeenCalledTimes(2)
  await vi.advanceTimersByTimeAsync(1); await flushPromises()
  expect(api).toHaveBeenCalledTimes(3)
  const params = new URL(api.mock.calls.at(-1)![0], 'https://synthetic.invalid/').searchParams
  expect(params.get('q')).toBe('v45'); expect(params.get('page')).toBe('1'); expect(params.has('until')).toBe(false)
  await input.setValue('unfinished'); await input.setValue(''); await flushPromises()
  expect(api).toHaveBeenCalledTimes(4)
  expect(new URL(api.mock.calls.at(-1)![0], 'https://synthetic.invalid/').searchParams.get('q')).toBe('')
  await input.setValue('unmounted'); wrapper.unmount()
  await vi.advanceTimersByTimeAsync(500)
  expect(api).toHaveBeenCalledTimes(4)
})

it('历史首屏读取期间只用一页摘要占位，不先显示三十行再收缩', async () => {
  const first = deferred<{ jobs: MemberJob[]; total: number; until: string }>()
  const jobs = Array.from({ length: 30 }, (_, i): MemberJob => ({ id: String(i), model: 'v45', status: 'done', createdAt: '2026-10-17T00:00:00Z' }))
  const wrapper = await mountSuspended(MemberCalls, { props: { api: memberApi(() => first.promise), jobs, lateResults: [], refreshVersion: 0 } })
  expect(wrapper.findAll('#jobs tbody tr')).toHaveLength(10)
  first.resolve({ jobs: jobs.slice(0, 10), total: 80, until: '2026-10-17T03:00:00.000Z' }); await flushPromises()
  expect(wrapper.findAll('#jobs tbody tr')).toHaveLength(10)
  expect(wrapper.text()).toContain('80 条保留记录')
})

describe('MemberBatch', () => {
  it('展开才查询，刷新通知单在途并合并一次补读', async () => {
    const first = deferred<BatchSnapshot>()
    let active = 0, peak = 0, calls = 0
    const api = vi.fn<MemberApi>().mockImplementation(async <T>() => {
      calls++; active++; peak = Math.max(peak, active)
      const result = calls === 1 ? await first.promise : snapshot('fresh-result')
      active--; return result as T
    })
    const wrapper = await mountSuspended(MemberBatch, { props: { requestId, api: memberApi(api), refreshVersion: 0 } })
    expect(api).not.toHaveBeenCalled()
    await wrapper.get('.n-collapse-item__header-main').trigger('click')
    expect(api).toHaveBeenCalledExactlyOnceWith(requestPath)
    for (let refreshVersion = 1; refreshVersion <= 8; refreshVersion++) await wrapper.setProps({ refreshVersion })
    expect(api).toHaveBeenCalledTimes(1)
    first.resolve(snapshot('first-result')); await flushPromises()
    expect(api).toHaveBeenCalledTimes(2); expect(peak).toBe(1)
    expect(wrapper.text()).toContain('fresh-result'); expect(wrapper.text()).not.toContain('first-result')
    expect(api.mock.calls.every(([, body]) => body === undefined)).toBe(true)
  })

  it('取消仅点击 POST 空对象一次，服务端快照不会被先前 GET 覆盖，在途图片仍显示生成中', async () => {
    const stale = deferred<BatchSnapshot>(), cancelled = deferred<BatchSnapshot>()
    let reads = 0
    const api = vi.fn<MemberApi>().mockImplementation(async <T>(path: string, body?: unknown) => {
      if (body !== undefined) { expect(path).toBe(requestPath + '/cancel'); return await cancelled.promise as T }
      return (++reads === 1 ? snapshot() : await stale.promise) as T
    })
    const wrapper = await mountSuspended(MemberBatch, { props: { requestId, api: memberApi(api), refreshVersion: 0 } })
    await wrapper.get('.n-collapse-item__header-main').trigger('click'); await flushPromises()
    await wrapper.setProps({ refreshVersion: 1 })
    expect(reads).toBe(2)
    const button = wrapper.findAll('button').find(item => item.text() === '取消此批次尚未派发的图片')!
    await button.trigger('click'); await button.trigger('click')
    expect(api.mock.calls.filter(([, body]) => body !== undefined)).toEqual([[requestPath + '/cancel', {}]])
    cancelled.resolve(cancelledSnapshot()); await flushPromises()
    expect(wrapper.text()).toContain('已取消（尚未派发）'); expect(wrapper.text()).toContain('生成中')
    expect(wrapper.emitted('changed')).toHaveLength(1)
    stale.resolve(snapshot('stale-before-cancel')); await flushPromises()
    expect(wrapper.text()).not.toContain('stale-before-cancel')
    expect(wrapper.text()).toContain('此次取消 1 条尚未派发任务')
    expect(button.attributes('disabled')).toBeDefined()
    expect(api).toHaveBeenCalledTimes(3)
  })

  it('关闭时忽略旧读取且不后台刷新，重新展开读取新快照', async () => {
    const old = deferred<BatchSnapshot>()
    const api = vi.fn<MemberApi>().mockReturnValueOnce(old.promise).mockResolvedValue(snapshot('reopened-result'))
    const wrapper = await mountSuspended(MemberBatch, { props: { requestId, api: memberApi(api), refreshVersion: 0 } })
    await wrapper.get('.n-collapse-item__header-main').trigger('click')
    await wrapper.get('.n-collapse-item__header-main').trigger('click')
    await wrapper.setProps({ refreshVersion: 1 })
    old.resolve(snapshot('closed-old-result')); await flushPromises()
    expect(api).toHaveBeenCalledTimes(1); expect(wrapper.text()).not.toContain('closed-old-result')
    await wrapper.get('.n-collapse-item__header-main').trigger('click'); await flushPromises()
    expect(api).toHaveBeenCalledTimes(2); expect(wrapper.text()).toContain('reopened-result')
  })

  for (const unmount of [false, true]) {
    it(`${unmount ? '卸载后' : '仅折叠时'}取消完成${unmount ? '不再发送变更或回写' : '仍通知钱包刷新'}，不自动重试`, async () => {
      const cancelled = deferred<BatchSnapshot>()
      const api = vi.fn<MemberApi>().mockImplementation(async <T>(_path: string, body?: unknown) => (body === undefined ? snapshot() : await cancelled.promise) as T)
      const wrapper = await mountSuspended(MemberBatch, { props: { requestId, api: memberApi(api), refreshVersion: 0 } })
      await wrapper.get('.n-collapse-item__header-main').trigger('click'); await flushPromises()
      await wrapper.findAll('button').find(item => item.text() === '取消此批次尚未派发的图片')!.trigger('click')
      if (unmount) wrapper.unmount()
      else await wrapper.get('.n-collapse-item__header-main').trigger('click')
      cancelled.resolve(cancelledSnapshot()); await flushPromises()
      expect(wrapper.emitted('changed')?.length || 0).toBe(unmount ? 0 : 1)
      expect(api.mock.calls.filter(([, body]) => body !== undefined)).toEqual([[requestPath + '/cancel', {}]])
    })
  }
})
