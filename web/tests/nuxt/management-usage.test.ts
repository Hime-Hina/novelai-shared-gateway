import { afterEach, describe, expect, it } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import { enableAutoUnmount } from '@vue/test-utils'
import { NSelect, NDataTable } from 'naive-ui'
import { nextTick } from 'vue'
import ManagementUsage from '../../app/components/ManagementUsage.vue'
import type { ManagementUsage as Usage, ManagementUsageCounts } from '../../app/types/management'

enableAutoUnmount(afterEach)
const counts: ManagementUsageCounts = { total: 10, done: 3, failed: 2, queued: 1, pending: 3, running: 1,
  refunded: 2, free: 1, v5: 1.25, fixed: 6, purchased: 4, averageWaitSeconds: null, averageGenerationSeconds: 0 }
function usage(): Usage {
  return { since: '2026-10-10T16:00:00Z', until: '2026-10-17T03:00:00Z', timezone: 'Asia/Shanghai', totals: { ...counts },
    members: [{ id: 'member', label: '合成成员', ...counts }], models: [{ id: 'model', label: '合成模型', ...counts }],
    days: [{ id: '2026-10-17', label: '2026-10-17', ...counts }], recent: [
      { id: 'free', memberId: 'member', username: '合成成员', model: '免费模型', status: 'done', createdAt: '2026-10-17T03:00:00Z',
        waitSeconds: 0, generationSeconds: null, resource: 'free', chargeStatus: 'settled' },
      { id: 'refund', memberId: 'member', username: '合成成员', model: '付费模型', status: 'failed', createdAt: '2026-10-17T02:00:00Z',
        waitSeconds: null, generationSeconds: 2.5, resource: 'anlas', fixed: 6, purchased: 4, chargeStatus: 'refunded', errorStatus: 429 }
    ] }
}

describe('ManagementUsage', () => {
  it('后台刷新不在工具栏外插入新块，也不移除已有数据', async () => {
    const wrapper = await mountSuspended(ManagementUsage, { props: { data: usage(), days: 7, loading: false } })
    const children = wrapper.get('#usagePanel').element.childElementCount
    await wrapper.setProps({ loading: true })
    expect(wrapper.get('#usagePanel').element.childElementCount).toBe(children)
    expect(wrapper.get('#usageDone').text()).toBe('3 / 10')
  })
  it('时间范围由props控制，发出筛选/重试事件；三个统计维度切换保留完整指标', async () => {
    const wrapper = await mountSuspended(ManagementUsage, { props: { data: usage(), days: 7, loading: false } })
    const selects = ['usageDays', 'usageGroup'].map(id => wrapper.findAllComponents(NSelect).find(input => input.attributes('id') === id)!)
    expect(selects[0]!.props('value')).toBe(7)
    expect(selects[0]!.props('options')).toEqual([{ label: '今天', value: 1 }, { label: '近 7 天', value: 7 }, { label: '近 30 天', value: 30 }, { label: '近一年', value: 365 }, { label: '全部保留记录', value: 0 }])
    for (const days of [1, 30]) selects[0]!.vm.$emit('update:value', days)
    expect(wrapper.emitted('update:days')).toEqual([[1], [30]])
    expect(selects[0]!.props('value')).toBe(7)
    await wrapper.get('#retryUsage').trigger('click')
    expect(wrapper.emitted('retry')).toEqual([[]])
    expect(wrapper.get('#usageDone').text()).toBe('3 / 10')
    expect(wrapper.get('#usageFree').text()).toContain('免费成功 1 次')
    expect(wrapper.get('#usageFailed').text()).toBe('2 / 2')
    expect(wrapper.get('#usageActive').text()).toContain('生成中 1 · 排队 1 · 等待前序图片 3')
    expect(wrapper.get('#usageV5').text()).toBe('1.25')
    expect(wrapper.get('#usageAnlas').text()).toBe('10')
    expect(wrapper.get('#usagePools').text()).toBe('订阅 6 · 购买 4')
    for (const [value, label] of [['members', '合成成员'], ['models', '合成模型'], ['days', '2026-10-17']]) {
      selects[1]!.vm.$emit('update:value', value)
      await nextTick()
      expect(wrapper.get('#usageRows').text()).toContain(label)
      expect(wrapper.get('#usageRows tbody tr').findAll('td')).toHaveLength(10)
    }
  })

  it('免费/退款/分池与HTTP错误保留，未知耗时不显示零，完整记录分页且支持全范围排序筛选', async () => {
    const data = usage()
    const wrapper = await mountSuspended(ManagementUsage, { props: { data, days: 7, loading: false } })
    expect(wrapper.get('#usageWait').text()).toBe('平均排队 未知')
    const summary = wrapper.get('#usageRows tbody tr').findAll('td')
    expect(summary[8]!.text()).toBe('未知'); expect(summary[9]!.text()).toBe('0 秒')
    const records = wrapper.get('#usageRecent').findAll('tbody tr')
    expect(records[0]!.text()).toContain('已结算 · 免费')
    expect(records[0]!.findAll('td')[4]!.text()).toBe('0 秒')
    expect(records[0]!.findAll('td')[5]!.text()).toBe('未知')
    expect(records[1]!.text()).toContain('已退款（本站额度） · 订阅 6 / 购买 4 Anlas · HTTP 429')
    expect(wrapper.text()).toContain('平均生成仅统计成功任务的上游耗时')
    await wrapper.setProps({ data: { ...data, recent: Array.from({ length: 51 }, (_, index) => ({ ...data.recent[0]!, id: String(index) })) } })
    expect(wrapper.get('#usageRecent').findAll('tbody tr')).toHaveLength(10)
    const table = wrapper.findAllComponents(NDataTable).find(item => item.attributes('id') === 'usageRecent')!
    expect(table.props('data')).toHaveLength(51)
    table.vm.page(6); await nextTick()
    expect(wrapper.get('#usageRecent').findAll('tbody tr')).toHaveLength(1)
    table.vm.filter({ status: ['failed'] }); await nextTick()
    expect(wrapper.get('#usageRecent').text()).toContain('所选范围暂无请求')
    await wrapper.setProps({ data })
    expect(wrapper.get('#usageRecent').findAll('tbody tr')).toHaveLength(1)
    expect(wrapper.get('#usageRecent tbody').text()).toContain('HTTP 429')
  })

  it('无数据可重试，加载中不伪造零用量，空范围明确展示', async () => {
    const wrapper = await mountSuspended(ManagementUsage, { props: { data: null, days: 1, loading: true } })
    expect(wrapper.text()).toContain('正在读取用量')
    expect(wrapper.find('#usageDone').exists()).toBe(false)
    await wrapper.setProps({ loading: false })
    expect(wrapper.text()).toContain('尚未读取用量统计')
    const data = usage()
    await wrapper.setProps({ data: { ...data, members: [], models: [], days: [], recent: [] } })
    expect(wrapper.get('#usageRows').text()).toContain('所选范围暂无记录')
    expect(wrapper.get('#usageRecent').text()).toContain('所选范围暂无请求')
    await wrapper.setProps({ loading: true })
    expect(wrapper.text()).toContain('以下为上次读取的结果')
  })
})
