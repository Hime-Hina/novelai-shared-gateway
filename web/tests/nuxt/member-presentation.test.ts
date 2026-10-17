import { afterEach, describe, expect, it, vi } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import { nextTick } from 'vue'
import MemberSignIn from '../../app/components/MemberSignIn.vue'
import MemberQuota from '../../app/components/MemberQuota.vue'
import SharedAccountStatus from '../../app/components/SharedAccountStatus.vue'
import PortalRefresh from '../../app/components/PortalRefresh.vue'
import type { MemberProfile, SharedStatus } from '../../app/types/member'

const wrappers: { unmount: () => void }[] = []
afterEach(() => {
  wrappers.splice(0).forEach(wrapper => wrapper.unmount())
  vi.useRealTimers()
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
})

const profile: MemberProfile = {
  username: '合成成员', owner: false, sessionExpiresAt: Date.parse('2026-10-24T12:00:00Z'), acceptingJobs: false,
  quota: {
    member: true, status: 'active', groupName: '测试组', groupSize: 4, share: 0.25, autoAnlasFallback: true,
    v5Available: 12.5, v5Capacity: 25, v5Reserved: 2, v5RefillPerDay: 6, v5RefillLimited: false, v5FullRefillSeconds: null,
    anlasFixedAvailable: 120, anlasPurchasedAvailable: 48, anlasFixedReserved: 3, anlasPurchasedReserved: 5
  },
  jobs: [], lateResults: []
}
const shared: SharedStatus = {
  status: 'cooldown', subscriptionActive: true, expiresAt: null, officialCheckedAt: null,
  stale: false, v5Percent: null, v5IsNegative: false, refillSecondsPerPercent: null,
  anlasFixed: null, anlasPurchased: null, running: 0, queued: 1, pending: 2, concurrencyLimit: 1,
  cooldownUntil: null, cooldownReason: null, quotaSyncError: false, recentFailures: 0
}

describe('成员展示组件', () => {
  it('两页共用刷新入口，连接状态明确且加载时保留更新时间位置', async () => {
    const wrapper = await mountSuspended(PortalRefresh, { props: { connection: 'open', loading: false, updatedAt: Date.parse('2026-10-17T16:00:00Z'), summary: '额度更新于' } })
    wrappers.push(wrapper)
    expect(wrapper.text()).toContain('实时更新')
    expect(wrapper.text()).toContain('2026/10/18')
    expect(wrapper.text()).toContain('上海时间')
    await wrapper.get('#refresh').trigger('click')
    expect(wrapper.emitted('refresh')).toEqual([[]])
    const count = wrapper.element.childElementCount
    await wrapper.setProps({ loading: true, connection: 'retrying' })
    expect(wrapper.element.childElementCount).toBe(count)
    expect(wrapper.text()).toContain('每 30 秒补读')
    await wrapper.setProps({ loading: false, connection: 'polling' })
    expect(wrapper.text()).toContain('每 30 秒更新')
  })

  it('提交真实密码表单后发出登录事件并清空密码，不写浏览器存储', async () => {
    const storage = vi.spyOn(Storage.prototype, 'setItem')
    const wrapper = await mountSuspended(MemberSignIn, { props: { options: { discord: false, ready: true, origin: null }, busy: false, error: '' } })
    wrappers.push(wrapper)
    await wrapper.get('input#username').setValue('  member  ')
    await wrapper.get('input#password').setValue('synthetic-password')
    await wrapper.get('#loginForm').trigger('submit')
    expect(wrapper.emitted('login')).toEqual([[{ username: 'member', password: 'synthetic-password' }]])
    expect((wrapper.get('input#password').element as HTMLInputElement).value).toBe('')
    expect(storage).not.toHaveBeenCalled()
  })

  it('Discord 未配置时禁用入口，直接提交表单也不会发出授权事件', async () => {
    const wrapper = await mountSuspended(MemberSignIn, { props: { options: { discord: true, ready: false, origin: null }, busy: false, error: '' } })
    wrappers.push(wrapper)
    expect((wrapper.get('button#discordLogin').element as HTMLButtonElement).disabled).toBe(true)
    expect(wrapper.get('#loginHint').text()).toContain('尚未配置完成')
    await wrapper.get('#discordForm').trigger('submit')
    expect(wrapper.emitted('discord')).toBeUndefined()
  })

  it('三分池及预留独立显示，未知补满时间和停止接单不丢领取说明', async () => {
    const wrapper = await mountSuspended(MemberQuota, { props: { profile } })
    wrappers.push(wrapper)
    expect(wrapper.get('#welcome').text()).toContain('合成成员')
    expect(wrapper.get('#group').text()).toContain('个人份额 25%')
    expect(wrapper.get('#v5').text()).toBe('12.5%')
    expect(wrapper.get('#v5Capacity').text()).toBe('上限 25%')
    expect(wrapper.get('#fixed').text()).toBe('120')
    expect(wrapper.get('#purchased').text()).toBe('48')
    expect(wrapper.get('#fixedHeld').text()).toBe('预留 3')
    expect(wrapper.get('#purchasedHeld').text()).toBe('预留 5')
    expect(wrapper.get('#v5Held').text()).toContain('预留 2%')
    expect(wrapper.get('#recovery').text()).toContain('补满时间未知')
    expect(wrapper.get('#status').text()).toContain('管理员已停止接单，已有结果仍可查询和下载')

    await wrapper.setProps({ profile: { ...profile, quota: null } })
    expect(wrapper.get('#v5').text()).toBe('未知%')
    expect(wrapper.get('#fixed').text()).toBe('未知')
    expect(wrapper.get('#purchased').text()).toBe('未知')
    expect(wrapper.find('#v5bar').exists()).toBe(false)
  })

  it('正常额度不用成功提示框，异常沿用同一状态行，规则仍可查看', async () => {
    const wrapper = await mountSuspended(MemberQuota, { props: { profile: { ...profile, acceptingJobs: true } } })
    wrappers.push(wrapper)
    expect(wrapper.find('.n-alert').exists()).toBe(false)
    expect(wrapper.get('#status').text()).toBe('可提交任务')
    const count = wrapper.element.childElementCount
    await wrapper.setProps({ profile: { ...profile, acceptingJobs: true, quota: { ...profile.quota!, status: 'stale' } } })
    expect(wrapper.element.childElementCount).toBe(count)
    expect(wrapper.get('#status').text()).toBe('官方余额等待更新')
    expect(wrapper.text()).toContain('额度规则')
  })

  it('订阅未知、不可用与有效三种状态分别显示', async () => {
    const wrapper = await mountSuspended(SharedAccountStatus, { props: {
      state: { ...shared, subscriptionActive: null }, connection: 'open'
    } })
    wrappers.push(wrapper)
    expect(wrapper.get('#sharedSubscription').text()).toBe('订阅状态未知')

    await wrapper.setProps({ state: { ...shared, subscriptionActive: false } })
    expect(wrapper.get('#sharedSubscription').text()).toBe('没有可用的 Opus 订阅')

    await wrapper.setProps({ state: { ...shared, subscriptionActive: true } })
    expect(wrapper.get('#sharedSubscription').text()).toContain('Opus 有效 · 到期')
  })

  it('未知公共余额与冷却原因不猜测，到零仍等待服务端状态', async () => {
    vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] })
    vi.setSystemTime(new Date('2026-10-17T12:00:00Z'))
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(false)
    const wrapper = await mountSuspended(SharedAccountStatus, { props: {
      state: { ...shared, cooldownUntil: new Date(Date.now() + 2000).toISOString(), cooldownReason: 'future_reason' }, connection: 'open'
    } })
    wrappers.push(wrapper)
    expect(wrapper.get('#sharedV5').text()).toBe('未知')
    expect(wrapper.get('#sharedFixed').text()).toBe('未知 订阅')
    expect(wrapper.get('#sharedCooldown').text()).toContain('共享账户等待派发 · 剩余 2 秒')
    expect(wrapper.get('#sharedCooldown').text()).not.toContain('生成失败')
    expect(vi.getTimerCount()).toBe(1)
    await vi.advanceTimersByTimeAsync(2000)
    await nextTick()
    expect(wrapper.get('#sharedCooldown').text()).toBe('等待期已结束，等待状态更新')
    expect(wrapper.get('#sharedState').text()).toBe('等待派发')
    expect(wrapper.get('#sharedQueue').text()).toContain('排队 1 · 批次待激活 2')
    expect(vi.getTimerCount()).toBe(0)
  })

  it('隐藏、恢复和卸载只管理一个本地时钟，不请求网络或重新订阅', async () => {
    vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] })
    vi.setSystemTime(new Date('2026-10-17T12:00:00Z'))
    const visibility = vi.spyOn(document, 'hidden', 'get').mockReturnValue(false)
    const fetch = vi.fn()
    const eventSource = vi.fn()
    vi.stubGlobal('fetch', fetch)
    vi.stubGlobal('EventSource', eventSource)
    const wrapper = await mountSuspended(SharedAccountStatus, { props: {
      state: { ...shared, cooldownUntil: new Date(Date.now() + 10000).toISOString(), cooldownReason: 'ordinary_failure' }, connection: 'open'
    } })
    wrappers.push(wrapper)
    expect(vi.getTimerCount()).toBe(1)
    visibility.mockReturnValue(true)
    document.dispatchEvent(new Event('visibilitychange'))
    expect(vi.getTimerCount()).toBe(0)
    await vi.advanceTimersByTimeAsync(3000)
    visibility.mockReturnValue(false)
    document.dispatchEvent(new Event('visibilitychange'))
    document.dispatchEvent(new Event('visibilitychange'))
    await nextTick()
    expect(wrapper.get('#sharedCooldown').text()).toContain('剩余 7 秒')
    expect(vi.getTimerCount()).toBe(1)

    window.dispatchEvent(new Event('pagehide'))
    expect(vi.getTimerCount()).toBe(0)
    await vi.advanceTimersByTimeAsync(2000)
    window.dispatchEvent(new Event('pageshow'))
    window.dispatchEvent(new Event('pageshow'))
    await nextTick()
    expect(wrapper.get('#sharedCooldown').text()).toContain('剩余 5 秒')
    expect(vi.getTimerCount()).toBe(1)
    wrapper.unmount()
    wrappers.pop()
    expect(vi.getTimerCount()).toBe(0)
    window.dispatchEvent(new Event('pageshow'))
    document.dispatchEvent(new Event('visibilitychange'))
    expect(vi.getTimerCount()).toBe(0)
    expect(fetch).not.toHaveBeenCalled()
    expect(eventSource).not.toHaveBeenCalled()
  })
})
