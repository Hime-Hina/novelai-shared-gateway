import { afterEach, describe, expect, it, vi } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import { enableAutoUnmount, flushPromises } from '@vue/test-utils'
import { NCheckbox } from 'naive-ui'
import ManagementUpstream from '../../app/components/ManagementUpstream.vue'
import type { ManagementAccount, ManagementApi, ManagementMode, ManagementRun, ManagementState } from '../../app/types/management'

enableAutoUnmount(afterEach)
const accountId = 'shared/account'
function state(bound = true): ManagementState {
  return { groups: [], configVersion: 1, autoAnlasFallback: false, publicBaseUrl: '', discordLogin: true, ownerUserId: 'owner',
    shared: { status: 'active', subscriptionActive: true, expiresAt: null, officialCheckedAt: null, stale: false,
      v5Percent: 90, v5IsNegative: false, refillSecondsPerPercent: 100, anlasFixed: 1000, anlasPurchased: 100,
      running: 0, queued: 0, pending: 0, concurrencyLimit: 1, cooldownUntil: null, cooldownReason: null, quotaSyncError: false, recentFailures: 0 },
    users: [{ id: 'owner', username: '合成管理员', note: '', enabled: true, acceptingJobs: true, discordId: 'synthetic-discord', inviteExpiresAt: null,
      recentActivity: { total: 0, done: 0, failed: 0, queued: 0, running: 0 },
      quota: bound ? { boundAccountId: accountId, member: true, status: 'active', groupName: '测试组', groupSize: 1,
        share: 1, autoAnlasFallback: false, v5Available: 90, v5Capacity: 100, v5Reserved: 0, v5RefillPerDay: 1, v5RefillLimited: false, v5FullRefillSeconds: 100,
        anlasFixedAvailable: 1000, anlasPurchasedAvailable: 100, anlasFixedReserved: 0, anlasPurchasedReserved: 0 } : null }] }
}
const account: ManagementAccount = { id: accountId, enabled: false, inFlight: 0, quotaTier: 3, quotaFixed: 1000,
  quotaPurchased: 100, v5UsagePercent: 90, quotaCheckedAt: '2026-10-17T03:00:00Z' }
type Transport = (...args: Parameters<ManagementApi>) => Promise<unknown>
function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>(done => { resolve = done })
  return { promise, resolve }
}
async function fixture(transport = vi.fn<Transport>().mockResolvedValue({ accounts: [account] }), mode: ManagementMode = 'admin') {
  const errors: unknown[] = []
  const api: ManagementApi = async <T>(...args: Parameters<ManagementApi>) => await transport(...args) as T
  const run: ManagementRun = async operation => { try { await operation() } catch (error) { errors.push(error) } }
  const wrapper = await mountSuspended(ManagementUpstream, { props: { state: state(), mode, api, run, busy: false,
    groupsDirty: false, active: true, refreshVersion: 0 } })
  await flushPromises()
  return { wrapper, transport, errors }
}
type Wrapper = Awaited<ReturnType<typeof fixture>>['wrapper']
const pstValue = (wrapper: Wrapper) => (wrapper.get('input#upstreamPst').element as HTMLInputElement).value
async function editFallback(wrapper: Wrapper, checked: boolean) {
  wrapper.findComponent(NCheckbox).vm.$emit('update:checked', checked)
  await flushPromises()
}

describe('ManagementUpstream', () => {
  it('summary中的PST、代理凭据和原始错误不进入DOM或PST输入', async () => {
    const transport = vi.fn<Transport>().mockResolvedValue({ accounts: [{ ...account,
      token: 'pst-synthetic-secret', proxyUrl: 'http://secret-user:secret-password@proxy.invalid', quotaError: 'raw-error-with-secret' }] })
    const { wrapper } = await fixture(transport)
    expect(wrapper.get('#upstreamStatus').text()).toContain('已暂停 · Opus')
    expect(wrapper.get('#upstreamQuota').text()).toContain('V5 90% · 订阅 1,000 · 购买 100')
    expect(wrapper.get('#upstreamUpdated').text()).toContain('查询失败')
    for (const secret of ['pst-synthetic-secret', 'secret-user', 'secret-password', 'raw-error-with-secret']) expect(wrapper.html()).not.toContain(secret)
    expect(pstValue(wrapper)).toBe('')
  })

  it('private恢复先核验额度，失败不PATCH；owner直接PATCH交给服务端核验', async () => {
    const transport = vi.fn<Transport>().mockImplementation(async path => path === 'summary' ? { accounts: [account] } : { failed: 1 })
    const { wrapper, errors } = await fixture(transport)
    await wrapper.get('#toggleUpstream').trigger('click'); await flushPromises()
    expect(transport).toHaveBeenCalledWith('accounts/quota', { ids: [accountId] })
    expect(transport.mock.calls.some(call => call[2] === 'PATCH')).toBe(false)
    expect((errors[0] as Error).message).toContain('保持暂停')
    transport.mockClear()
    await wrapper.setProps({ mode: 'owner' })
    await wrapper.get('#toggleUpstream').trigger('click'); await flushPromises()
    expect(transport).toHaveBeenCalledExactlyOnceWith('accounts/shared%2Faccount', { enabled: true }, 'PATCH')
    expect(wrapper.emitted('changed')?.at(-1)).toEqual(['上游已恢复。'])
  })

  it.each(['failed', 'rejected'])('PST已写入但额度查询%s，清旧输入并提示勿重复提交', async failure => {
    const transport = vi.fn<Transport>().mockImplementation(async path => {
      if (path === 'summary') return { accounts: [account] }
      if (path === 'accounts/quota') {
        if (failure === 'rejected') throw new Error('synthetic network failure')
        return { failed: 1 }
      }
      return {}
    })
    const { wrapper, errors } = await fixture(transport)
    await wrapper.get('input#upstreamPst').setValue('pst-new-synthetic')
    await wrapper.get('#upstreamCredentials').trigger('submit'); await flushPromises()
    expect(pstValue(wrapper)).toBe('')
    expect(transport).toHaveBeenCalledWith('accounts/shared%2Faccount', { token: 'pst-new-synthetic' }, 'PATCH')
    expect(wrapper.emitted('changed')?.at(-1)?.[0]).toContain('PST 已更新')
    expect(wrapper.emitted('notice')?.at(-1)).toEqual([expect.stringContaining('勿重复提交'), true])
    expect(errors).toEqual([])
    await wrapper.setProps({ refreshVersion: 1 }); await flushPromises()
    expect(transport.mock.calls.filter(call => call[2] === 'PATCH')).toHaveLength(1)
  })

  it('PST保存期间的新输入不被旧请求完成清除', async () => {
    const saving = deferred<unknown>()
    const transport = vi.fn<Transport>().mockImplementation(async (path, _body, method) => {
      if (path === 'summary') return { accounts: [account] }
      return method === 'PATCH' ? saving.promise : { failed: 0 }
    })
    const { wrapper } = await fixture(transport)
    await wrapper.get('input#upstreamPst').setValue('pst-first-synthetic')
    await wrapper.get('#upstreamCredentials').trigger('submit'); await flushPromises()
    await wrapper.get('input#upstreamPst').setValue('pst-next-synthetic')
    saving.resolve({}); await flushPromises()
    expect(pstValue(wrapper)).toBe('pst-next-synthetic')
    expect(transport.mock.calls.filter(call => call[2] === 'PATCH')).toEqual([
      ['accounts/shared%2Faccount', { token: 'pst-first-synthetic' }, 'PATCH']
    ])
  })

  it('后台策略变化保留未保存草稿，无新编辑的成功提交后恢复跟随', async () => {
    const saving = deferred<unknown>()
    const transport = vi.fn<Transport>().mockImplementation(async path => path === 'summary' ? { accounts: [account] } : saving.promise)
    const { wrapper } = await fixture(transport)
    await editFallback(wrapper, true)
    await wrapper.setProps({ state: { ...state(), autoAnlasFallback: true } })
    await wrapper.setProps({ state: state() })
    expect(wrapper.findComponent(NCheckbox).props('checked')).toBe(true)
    await wrapper.get('#saveFallback').trigger('click'); await flushPromises()
    expect(transport).toHaveBeenCalledWith('member-management', { action: 'fallback-policy', enabled: true })
    saving.resolve({}); await flushPromises()
    expect(wrapper.findComponent(NCheckbox).props('checked')).toBe(true)
    await wrapper.setProps({ state: { ...state(), autoAnlasFallback: true } })
    await wrapper.setProps({ state: state() })
    expect(wrapper.findComponent(NCheckbox).props('checked')).toBe(false)
  })

  it('策略POST期间修改选择，旧请求成功和后续后台快照均不覆盖新草稿', async () => {
    const saving = deferred<unknown>()
    const transport = vi.fn<Transport>().mockImplementation(async path => path === 'summary' ? { accounts: [account] } : saving.promise)
    const { wrapper } = await fixture(transport)
    await editFallback(wrapper, true)
    await wrapper.get('#saveFallback').trigger('click'); await flushPromises()
    await editFallback(wrapper, false)
    saving.resolve({}); await flushPromises()
    await wrapper.setProps({ state: { ...state(), autoAnlasFallback: true } })
    expect(wrapper.findComponent(NCheckbox).props('checked')).toBe(false)
    await wrapper.setProps({ state: state() })
    await wrapper.setProps({ state: { ...state(), autoAnlasFallback: true } })
    expect(wrapper.findComponent(NCheckbox).props('checked')).toBe(false)
    expect(transport.mock.calls.filter(call => call[0] === 'member-management')).toEqual([
      ['member-management', { action: 'fallback-policy', enabled: true }]
    ])
  })

  it('无绑定或有在途时禁用PST，直接提交表单也不能绕过', async () => {
    const transport = vi.fn<Transport>().mockResolvedValue({ accounts: [{ ...account, inFlight: 1 }] })
    const { wrapper, errors } = await fixture(transport)
    await wrapper.get('input#upstreamPst').setValue('pst-blocked-synthetic')
    expect((wrapper.get('#saveUpstreamPst').element as HTMLButtonElement).disabled).toBe(true)
    await wrapper.get('#upstreamCredentials').trigger('submit'); await flushPromises()
    expect((errors.at(-1) as Error).message).toContain('在途生成')
    await wrapper.setProps({ state: state(false) }); await flushPromises()
    expect((wrapper.get('#saveUpstreamPst').element as HTMLButtonElement).disabled).toBe(true)
    await wrapper.get('#upstreamCredentials').trigger('submit'); await flushPromises()
    expect((errors.at(-1) as Error).message).toContain('未绑定')
    expect(transport.mock.calls.some(call => call[2] === 'PATCH')).toBe(false)
  })
})
