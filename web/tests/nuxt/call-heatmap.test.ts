import { execFileSync } from 'node:child_process'
import { resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import { enableAutoUnmount, flushPromises } from '@vue/test-utils'
import { reactive } from 'vue'
import { NHeatmap, NSelect, NRadioGroup } from 'naive-ui'
import CallHeatmap from '../../app/components/CallHeatmap.vue'
import type { MemberApi } from '../../app/types/member'
import type { ActivityDay, ActivityInvalidation, ActivitySnapshot } from '../../app/types/activity'
import { activityDateKey, activityDates, aggregateActivity } from '../../app/utils/activity-display'

enableAutoUnmount(afterEach)
beforeEach(() => vi.spyOn(document, 'hidden', 'get').mockReturnValue(false))
afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); vi.unstubAllGlobals() })
const day = (date: string, total = 2): ActivityDay => ({ date, total, done: 1, failed: 0, cancelled: 1, queued: 0, pending: 0, running: 0, other: 0 })
function snapshot(days = [day('2026-10-16')], overrides: Partial<ActivitySnapshot> = {}): ActivitySnapshot {
  return { timezone: 'Asia/Taipei', from: '2026-10-15', to: '2026-10-17', asOf: '2026-10-17T03:00:00Z', memberId: 'self',
    operation: 'generate', coverage: 'retained-records', days, ...overrides }
}
function deferred<T>() {
  let resolve!: (value: T) => void, reject!: (reason: unknown) => void
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
type Transport = (...args: Parameters<MemberApi>) => Promise<unknown>
async function fixture(transport = vi.fn<Transport>().mockResolvedValue(snapshot()), owner = false, active = true) {
  const invalidation = reactive<ActivityInvalidation>({ version: 0, dates: null })
  const api: MemberApi = async <T>(...args: Parameters<MemberApi>) => await transport(...args) as T
  const wrapper = await mountSuspended(CallHeatmap, { props: { api,
    endpoint: owner ? 'member-management?view=heatmap' : 'heatmap', active, refreshVersion: 0, invalidation,
    ...(owner ? { members: [{ id: 'member/a', username: '合成成员' }] } : {}) } })
  await flushPromises()
  const invalidate = (dates: string[] | null) => { invalidation.dates = dates; invalidation.version++ }
  return { wrapper, transport, invalidation, invalidate }
}
type Wrapper = Awaited<ReturnType<typeof fixture>>['wrapper']
function select(wrapper: Wrapper, id: string) {
  return wrapper.findAllComponents(NSelect).find(component => component.attributes('id') === id)!
}
const cells = (wrapper: Wrapper) => wrapper.findComponent(NHeatmap).props('data')!
const query = (path: string) => new URL(path, 'https://synthetic.invalid/').searchParams

describe('CallHeatmap', () => {
  it('每周、每月只汇总一次，空日期保留，累计逐日增长', async () => {
    const dates = activityDates('2024-02-28', '2024-03-04'), values = new Map([['2024-02-29', 2], ['2024-03-03', 3]])
    expect(aggregateActivity(dates, values, 'month')).toEqual([{ from: '2024-02-28', to: '2024-02-29', value: 2 }, { from: '2024-03-01', to: '2024-03-04', value: 3 }])
    expect(aggregateActivity(dates, values, 'week')).toEqual([{ from: '2024-02-28', to: '2024-03-02', value: 2 }, { from: '2024-03-03', to: '2024-03-04', value: 3 }])
    const { wrapper, transport } = await fixture()
    const toggle = wrapper.findComponent(NRadioGroup)
    toggle.vm.$emit('update:value', 'cumulative'); await flushPromises()
    expect(cells(wrapper).map(cell => cell.value)).toEqual([0, 2, 2])
    for (const mode of ['week', 'month']) {
      toggle.vm.$emit('update:value', mode); await flushPromises()
      expect(wrapper.findAll('.period-cell')).toHaveLength(1)
      expect(wrapper.get('.period-cell strong').text()).toBe('2')
    }
    expect(transport).toHaveBeenCalledTimes(1)
  })
  it('稀疏日期明确为未知，按本地日历坐标传完整范围；状态与触屏日期选择不再请求', async () => {
    const eventSource = vi.fn(); vi.stubGlobal('EventSource', eventSource)
    const { wrapper, transport } = await fixture()
    expect(transport).toHaveBeenCalledExactlyOnceWith('heatmap?operation=generate')
    expect(cells(wrapper).map(cell => [activityDateKey(cell.timestamp), cell.value])).toEqual([
      ['2026-10-15', 0], ['2026-10-16', 2], ['2026-10-17', 0]
    ])
    expect(wrapper.findComponent(NHeatmap).props('firstDayOfWeek')).toBe(6)
    expect(wrapper.findAll('.n-heatmap__calendar-table .n-heatmap-rect')).toHaveLength(3)
    expect(wrapper.findComponent(NHeatmap).props('showMonthLabels')).toBe(true)
    expect(wrapper.findComponent(NHeatmap).props('showWeekLabels')).toBe(true)
    expect(wrapper.findComponent(NHeatmap).props('showColorIndicator')).toBe(true)
    expect(wrapper.get('#heatmapDetails').text()).toContain('覆盖未知')
    select(wrapper, 'heatmapDay').vm.$emit('update:value', '2026-10-16'); await flushPromises()
    expect(wrapper.get('#heatmapDetails').text()).toContain('保留记录 2 次 · 成功 1 · 失败 0 · 取消 1')
    select(wrapper, 'heatmapStatus').vm.$emit('update:value', 'failed'); await flushPromises()
    expect(cells(wrapper).map(cell => cell.value)).toEqual([0, 0, 0])
    await wrapper.findAll('.n-heatmap__calendar-table .n-heatmap-rect')[1]!.trigger('mouseenter')
    await vi.waitFor(() => expect(document.querySelector('.heatmap-tooltip-details')?.textContent).toContain('成功 1 · 失败 0 · 取消 1'))
    expect(wrapper.text()).toContain('包含缓存任务，不等于实际 NAI 调用次数')
    expect(transport).toHaveBeenCalledTimes(1); expect(eventSource).not.toHaveBeenCalled()
  })

  it('同tick稳定对象的两次失效合并日期，空增量删除旧桶而不动其他日期', async () => {
    const transport = vi.fn<Transport>().mockResolvedValueOnce(snapshot([day('2026-10-15'), day('2026-10-16'), day('2026-10-17', 3)]))
      .mockResolvedValueOnce(snapshot([]))
    const { wrapper, invalidate } = await fixture(transport)
    invalidate(['2026-10-15']); invalidate(['2026-10-16'])
    await flushPromises()
    expect(transport).toHaveBeenCalledTimes(2)
    expect(query(transport.mock.calls[1]![0]).get('dates')).toBe('2026-10-15,2026-10-16')
    expect(cells(wrapper).map(cell => cell.value)).toEqual([0, 0, 3])
  })

  it('单在途期间合并后续日期，null重连失效优先一次全读', async () => {
    const increment = deferred<ActivitySnapshot>()
    const transport = vi.fn<Transport>().mockResolvedValueOnce(snapshot()).mockReturnValueOnce(increment.promise).mockResolvedValueOnce(snapshot([day('2026-10-17', 4)]))
    const { wrapper, invalidate } = await fixture(transport)
    invalidate(['2026-10-16']); await flushPromises()
    invalidate(['2026-10-15']); invalidate(['2026-10-17']); invalidate(null); invalidate(['2026-10-16'])
    await flushPromises(); expect(transport).toHaveBeenCalledTimes(2)
    increment.resolve(snapshot([day('2026-10-16', 3)])); await flushPromises()
    expect(transport).toHaveBeenCalledTimes(3)
    expect(query(transport.mock.calls[2]![0]).has('dates')).toBe(false)
    expect(cells(wrapper).map(cell => cell.value)).toEqual([0, 0, 4])
  })

  it('普通在途增量只补读一次合并后的日期，既有其他桶保持', async () => {
    const increment = deferred<ActivitySnapshot>()
    const transport = vi.fn<Transport>().mockResolvedValueOnce(snapshot()).mockReturnValueOnce(increment.promise)
      .mockResolvedValueOnce(snapshot([day('2026-10-15', 3), day('2026-10-17', 4)]))
    const { wrapper, invalidate } = await fixture(transport)
    invalidate(['2026-10-16']); await flushPromises()
    invalidate(['2026-10-15']); invalidate(['2026-10-17']); invalidate(['2026-10-15']); await flushPromises()
    expect(transport).toHaveBeenCalledTimes(2)
    increment.resolve(snapshot([day('2026-10-16', 5)])); await flushPromises()
    expect(transport).toHaveBeenCalledTimes(3)
    expect(query(transport.mock.calls[2]![0]).get('dates')).toBe('2026-10-15,2026-10-17')
    expect(cells(wrapper).map(cell => cell.value)).toEqual([3, 5, 4])
  })

  it('增量返回新日期窗口时再全读，不把片段当作完整新窗口', async () => {
    const full = deferred<ActivitySnapshot>(), range = { from: '2026-10-16', to: '2026-10-18' }
    const transport = vi.fn<Transport>().mockResolvedValueOnce(snapshot()).mockResolvedValueOnce(snapshot([], range)).mockReturnValueOnce(full.promise)
    const { wrapper, invalidate } = await fixture(transport)
    invalidate(['2026-10-16']); await flushPromises()
    expect(transport).toHaveBeenCalledTimes(3)
    expect(query(transport.mock.calls[2]![0]).has('dates')).toBe(false)
    expect(cells(wrapper).map(cell => activityDateKey(cell.timestamp))).toEqual(['2026-10-15', '2026-10-16', '2026-10-17'])
    full.resolve(snapshot([day('2026-10-18', 5)], range)); await flushPromises()
    expect(cells(wrapper).map(cell => cell.value)).toEqual([0, 0, 5])
  })

  it('owner成员/操作筛选清旧数据，旧请求迟到不能回填；参数不混进成员接口', async () => {
    const old = deferred<ActivitySnapshot>()
    const transport = vi.fn<Transport>().mockReturnValueOnce(old.promise).mockResolvedValueOnce(snapshot([day('2026-10-17', 7)], { operation: 'upscale', memberId: 'member/a' }))
    const { wrapper } = await fixture(transport, true)
    select(wrapper, 'heatmapMember').vm.$emit('update:value', 'member/a')
    select(wrapper, 'heatmapOperation').vm.$emit('update:value', 'upscale'); await flushPromises()
    expect(wrapper.findComponent(NHeatmap).props('loading')).toBe(true); expect(cells(wrapper)).toEqual([]); expect(transport).toHaveBeenCalledTimes(1)
    old.resolve(snapshot([day('2026-10-16', 99)])); await flushPromises()
    expect(transport).toHaveBeenCalledTimes(2)
    const params = query(transport.mock.calls[1]![0])
    expect(params.get('view')).toBe('heatmap'); expect(params.get('operation')).toBe('upscale'); expect(params.get('memberId')).toBe('member/a')
    expect(params.has('dates')).toBe(false); expect(cells(wrapper).map(cell => cell.value)).toEqual([0, 0, 7])
  })

  it('隐藏或非active延后，显示全读校准；普通refreshVersion不造成轮询', async () => {
    vi.useFakeTimers({ toFake: ['Date'] }); vi.setSystemTime(new Date('2026-10-17T03:00:00Z'))
    const { wrapper, transport, invalidate } = await fixture(undefined, false, false)
    expect(transport).not.toHaveBeenCalled()
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(true)
    await wrapper.setProps({ active: true }); invalidate(['2026-10-16']); await flushPromises()
    expect(transport).not.toHaveBeenCalled()
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(false)
    document.dispatchEvent(new Event('visibilitychange')); await flushPromises()
    expect(transport).toHaveBeenCalledTimes(1)
    await wrapper.setProps({ refreshVersion: 1 }); await wrapper.setProps({ refreshVersion: 2 }); await flushPromises()
    expect(transport).toHaveBeenCalledTimes(1)
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(true)
    invalidate(['2026-10-16']); await flushPromises(); expect(transport).toHaveBeenCalledTimes(1)
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(false)
    document.dispatchEvent(new Event('visibilitychange')); await flushPromises()
    expect(transport).toHaveBeenCalledTimes(2); expect(query(transport.mock.calls[1]![0]).has('dates')).toBe(false)
    vi.setSystemTime(new Date('2026-10-17T16:00:00Z'))
    await wrapper.setProps({ refreshVersion: 3 }); await flushPromises()
    expect(transport).toHaveBeenCalledTimes(3); expect(query(transport.mock.calls[2]![0]).has('dates')).toBe(false)
  })

  it('失败保留旧图且不自动重试，手动重试全读；卸载后不追加读取', async () => {
    const late = deferred<ActivitySnapshot>()
    const transport = vi.fn<Transport>().mockResolvedValueOnce(snapshot()).mockRejectedValueOnce(new Error('synthetic unavailable')).mockReturnValueOnce(late.promise)
    const { wrapper, invalidate } = await fixture(transport)
    invalidate(['2026-10-16']); await flushPromises()
    expect(wrapper.text()).toContain('读取失败，保留上次结果')
    expect(cells(wrapper)[1]!.value).toBe(2); expect(transport).toHaveBeenCalledTimes(2)
    await wrapper.get('#retryHeatmap').trigger('click'); await flushPromises()
    expect(transport).toHaveBeenCalledTimes(3); expect(query(transport.mock.calls[2]![0]).has('dates')).toBe(false)
    wrapper.unmount(); invalidate(null); late.resolve(snapshot([day('2026-10-17', 99)])); await flushPromises()
    expect(transport).toHaveBeenCalledTimes(3)
  })

  it('真实Node时区中日历坐标不偏一天，跨DST与闰年范围完整', () => {
    const moduleUrl = pathToFileURL(resolve('app/utils/activity-display.ts')).href
    const code = `import { activityDate, activityDateKey, activityDates, taipeiDay } from ${JSON.stringify(moduleUrl)};
      const date = activityDate('2024-02-29');
      console.log(JSON.stringify({ key:activityDateKey(date), month:date.getMonth()+1, day:date.getDate(), offset:date.getTimezoneOffset(),
        dst:activityDates('2024-03-09','2024-03-11'), leap:activityDates('2023-03-01','2024-02-29').length,
        normal:activityDates('2024-03-01','2025-02-28').length, taipei:taipeiDay(Date.parse('2026-10-16T16:00:00Z')) }));`
    const offsets = []
    for (const TZ of ['America/Los_Angeles', 'Asia/Taipei', 'Pacific/Kiritimati']) {
      const result = JSON.parse(execFileSync(process.execPath, ['--experimental-strip-types', '--input-type=module', '-e', code],
        { env: { ...process.env, TZ }, encoding: 'utf8', windowsHide: true }))
      offsets.push(result.offset)
      expect(result).toMatchObject({ key: '2024-02-29', month: 2, day: 29, dst: ['2024-03-09', '2024-03-10', '2024-03-11'],
        leap: 366, normal: 365, taipei: '2026-10-17' })
    }
    expect(offsets).toEqual([480, -480, -840])
  })
})
