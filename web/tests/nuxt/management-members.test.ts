import { afterEach, describe, expect, it, vi } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import { DOMWrapper, enableAutoUnmount, flushPromises } from '@vue/test-utils'
import { NModal, NSelect, NDropdown } from 'naive-ui'
import ManagementMembers from '../../app/components/ManagementMembers.vue'
import type { ManagementApi, ManagementMemberChange, ManagementMode, ManagementRun, ManagementState } from '../../app/types/management'

enableAutoUnmount(afterEach)
afterEach(() => vi.restoreAllMocks())
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>(done => { resolve = done }); return { promise, resolve } }
const body = () => new DOMWrapper(document.body)
function state(): ManagementState {
  return {
    configVersion: 1, discordLogin: false, ownerUserId: 'a', publicBaseUrl: 'https://gateway.example', autoAnlasFallback: false,
    shared: { status: 'active', subscriptionActive: true, expiresAt: null, officialCheckedAt: null, stale: false, v5Percent: 80, v5IsNegative: false,
      refillSecondsPerPercent: 100, anlasFixed: 1000, anlasPurchased: 100, running: 0, queued: 0, pending: 0, concurrencyLimit: 1,
      cooldownUntil: null, cooldownReason: null, quotaSyncError: false, recentFailures: 0 },
    groups: ['a', 'b', 'c', 'd'].map(id => ({ id, name: '组 ' + id, members: [id], share: .25, memberWeights: { [id]: 1 } })),
    users: ['a', 'b', 'c', 'd'].map(id => ({ id, username: 'member-' + id, note: '', enabled: true, acceptingJobs: true, discordId: null, inviteExpiresAt: null,
      quota: { member: true, status: 'active', groupName: '组 ' + id, groupSize: 1, share: .25, autoAnlasFallback: false,
        v5Available: 10, v5Capacity: 25, v5Reserved: 0, v5RefillPerDay: 1, v5RefillLimited: false, v5FullRefillSeconds: 100,
        anlasFixedAvailable: 20, anlasPurchasedAvailable: 5, anlasFixedReserved: 0, anlasPurchasedReserved: 0 },
      recentActivity: { total: 0, done: 0, failed: 0, queued: 0, running: 0 } }))
  }
}
function preview(initial: ManagementState): ManagementMemberChange {
  return { configVersion: 1, groups: initial.groups, transferred: { v5: 10, anlasFixed: 20, anlasPurchased: 5 }, autoJoined: null,
    deletedUserId: 'b', transferToUserId: 'a', shareChanges: [{ userId: 'b', beforeShare: .25, afterShare: 0 }, { userId: 'a', beforeShare: .25, afterShare: .5 }], users: [] }
}
async function page(transport: (...args: Parameters<ManagementApi>) => Promise<unknown>, initial = state(), mode: ManagementMode = 'owner') {
  const errors: unknown[] = []
  let working = false
  const api: ManagementApi = async <T>(...args: Parameters<ManagementApi>) => await transport(...args) as T
  const run: ManagementRun = async operation => {
    if (working) return
    working = true
    try { await operation() } catch (error) { errors.push(error) } finally { working = false }
  }
  const wrapper = await mountSuspended(ManagementMembers, {
    props: { state: initial, mode, api, run, busy: false, groupsDirty: false },
    global: { stubs: { transition: false, 'transition-group': false } }
  })
  const action = async (userId: string, label: string) => {
    const button = wrapper.get(`[data-member-id="${userId}"]`).findAll('button').find(item => item.text() === label)
    if (button) await button.trigger('click')
    else {
      const menu = wrapper.findAllComponents(NDropdown).find(item => item.element.closest('[data-member-id]')?.getAttribute('data-member-id') === userId)!
      const option = menu.props('options')!.find(option => option?.label === label)!
      expect(option).toBeDefined(); expect(option.disabled).not.toBe(true)
      await wrapper.get(`[data-member-id="${userId}"]`).findAll('button').find(item => item.text() === '更多')!.trigger('click')
      await flushPromises()
      const menuItem = body().findAll('.n-dropdown-option').find(item => item.text() === label)
      expect(menuItem).toBeDefined(); await menuItem!.get('.n-dropdown-option-body').trigger('click')
    }
    await flushPromises()
  }
  const select = async (id: string, value: string) => {
    const input = wrapper.findAllComponents(NSelect).find(item => item.attributes('id') === id)
    expect(input).toBeDefined(); input!.vm.$emit('update:value', value); await flushPromises()
  }
  return { wrapper, errors, action, select }
}

describe('成员创建与凭据', () => {
  it('真实Modal关闭再打开保持单根默认槽，并立即清空Key和密码', async () => {
    const warnings = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const errors = vi.spyOn(console, 'error').mockImplementation(() => {})
    const { wrapper, action } = await page(async () => ({ apiKey: 'synthetic-modal-key' }))
    expect(wrapper.findComponent(NModal).exists()).toBe(true)
    expect(wrapper.find('n-modal-stub').exists()).toBe(false)
    expect(wrapper.find('transition-stub').exists()).toBe(false)
    await action('b', '分发信息')
    expect((body().get('#deliveryText').element as HTMLTextAreaElement).value).toContain('synthetic-modal-key')
    await body().get('#closeMemberOperation').trigger('click'); await flushPromises()
    expect(body().find('#deliveryText').exists()).toBe(false)
    expect(document.body.textContent).not.toContain('synthetic-modal-key')
    await wrapper.get('#newMember').trigger('click'); await flushPromises()
    await body().get('#password').setValue('synthetic-modal-password')
    await body().get('#closeMemberOperation').trigger('click'); await flushPromises()
    expect(body().find('#password').exists()).toBe(false)
    await wrapper.get('#newMember').trigger('click'); await flushPromises()
    expect((body().get('#password').element as HTMLInputElement).value).toBe('')
    await body().get('#closeMemberOperation').trigger('click'); await flushPromises()
    wrapper.unmount(); await flushPromises()
    expect([...warnings.mock.calls, ...errors.mock.calls].flat().map(String).filter(message => /getFirstSlotVNode|default slot is empty|slot\[default\]/.test(message))).toEqual([])
  })

  it('创建已成功后分发失败只提示已保存，不重复创建，也不依赖列表刷新先成功', async () => {
    const api = vi.fn<(...args: Parameters<ManagementApi>) => Promise<unknown>>().mockImplementation(async (_path, value) => {
      const request = value as { action: string }
      if (request.action === 'credentials') return { id: 'new-id', username: 'new-member' }
      if (request.action === 'member-key') throw new Error('synthetic delivery failure')
      throw new Error('unexpected request')
    })
    const { wrapper, errors } = await page(api)
    await wrapper.get('#newMember').trigger('click'); await flushPromises()
    await body().get('#username').setValue('new-member')
    await body().get('#password').setValue('synthetic-password-123')
    await body().get('#credentials').trigger('submit'); await flushPromises()
    expect(api.mock.calls.map(([, value]) => (value as { action: string }).action)).toEqual(['credentials', 'member-key'])
    expect(api.mock.calls[1]).toEqual(['member-management', { action: 'member-key', userId: 'new-id' }])
    expect(wrapper.emitted('changed')).toHaveLength(1)
    expect(wrapper.emitted('changed')![0]![0]).toContain('成员已创建')
    expect(wrapper.emitted('notice')?.at(-1)).toEqual([expect.stringContaining('账户已保存'), true])
    expect(wrapper.emitted('notice')?.at(-1)?.[0]).toContain('请勿重复创建')
    expect(body().find('#saveMember').exists()).toBe(false)
    expect(body().get('#deliveryText').element).toHaveProperty('value', '')
    expect(errors).toEqual([])
  })

  it('关闭创建弹窗后迟到成功只刷新列表，不重新打开分发或保留密码', async () => {
    const saved = deferred<{ id: string; username: string }>()
    const api = vi.fn<(...args: Parameters<ManagementApi>) => Promise<unknown>>().mockReturnValue(saved.promise)
    const { wrapper } = await page(api)
    await wrapper.get('#newMember').trigger('click'); await flushPromises()
    await body().get('#username').setValue('new-member'); await body().get('#password').setValue('synthetic-password-123')
    await body().get('#credentials').trigger('submit')
    await body().get('#closeMemberOperation').trigger('click')
    saved.resolve({ id: 'new-id', username: 'new-member' }); await flushPromises()
    expect(api).toHaveBeenCalledTimes(1)
    expect(wrapper.emitted('changed')).toHaveLength(1)
    expect(body().find('#memberDialog').exists()).toBe(false)
    expect(body().find('#deliveryDialog').exists()).toBe(false)
    expect(document.body.textContent).not.toContain('synthetic-password-123')
  })

  it('分发只按点击读取Key，关闭及卸载后的迟到响应不能恢复敏感信息', async () => {
    const closed = deferred<{ apiKey: string }>(), unmounted = deferred<{ apiKey: string }>()
    const api = vi.fn<(...args: Parameters<ManagementApi>) => Promise<unknown>>().mockReturnValueOnce(closed.promise)
      .mockResolvedValueOnce({ apiKey: 'synthetic-visible-key' }).mockReturnValueOnce(unmounted.promise)
    const { wrapper, action } = await page(api)
    expect(api).not.toHaveBeenCalled()
    await action('b', '分发信息')
    await body().get('#closeMemberOperation').trigger('click')
    closed.resolve({ apiKey: 'synthetic-closed-key' }); await flushPromises()
    expect(body().find('#deliveryDialog').exists()).toBe(false)
    await action('b', '分发信息')
    expect((body().get('#deliveryText').element as HTMLTextAreaElement).value).toContain('synthetic-visible-key')
    await body().get('#closeMemberOperation').trigger('click'); await flushPromises()
    expect(body().find('#deliveryText').exists()).toBe(false)
    await action('b', '分发信息'); wrapper.unmount()
    unmounted.resolve({ apiKey: 'synthetic-unmounted-key' }); await flushPromises()
    expect(body().find('#deliveryDialog').exists()).toBe(false)
  })
})

it('调额打开时冻结版本，后台新快照和409不覆盖输入或关闭表单', async () => {
  const initial = state()
  const api = vi.fn<(...args: Parameters<ManagementApi>) => Promise<unknown>>().mockRejectedValue(Object.assign(new Error('synthetic version conflict'), { statusCode: 409 }))
  const { wrapper, action, errors, select } = await page(api, initial)
  await action('b', '调整额度')
  await body().get('#transferAmount').setValue('5')
  await select('transferTo', 'c')
  await wrapper.setProps({ state: { ...state(), configVersion: 2 } })
  await body().get('#transferForm').trigger('submit'); await flushPromises()
  expect(api).toHaveBeenCalledExactlyOnceWith('member-management', { action: 'transfer-quota', fromUserId: 'b', toUserId: 'c', resource: 'v5', amount: 5, expectedConfigVersion: 1 })
  expect(errors).toHaveLength(1)
  expect(body().find('#transferDialog').exists()).toBe(true)
  expect((body().get('#transferAmount').element as HTMLInputElement).value).toBe('5')
  expect(wrapper.emitted('changed')).toBeUndefined()
})

it('删除必须预览和完整用户名确认，改接收人使预览失效，后台刷新不提升提交版本', async () => {
  const initial = state()
  const api = vi.fn<(...args: Parameters<ManagementApi>) => Promise<unknown>>().mockImplementation(async (_path, value) => {
    const request = value as { action: string; transferToUserId: string }
    return request.action === 'preview-delete-member' ? { ...preview(initial), transferToUserId: request.transferToUserId } : {}
  })
  const { wrapper, action, select } = await page(api, initial)
  const ownerActions = wrapper.findAllComponents(NDropdown).find(item => item.element.closest('[data-member-id]')?.getAttribute('data-member-id') === 'a')!.props('options')!
  expect(ownerActions.find(option => option?.label === '删除')!.disabled).toBe(true)
  expect(ownerActions.find(option => option?.label === '停用')!.disabled).toBe(true)
  await action('b', '删除')
  expect(api).not.toHaveBeenCalled(); expect(body().find('#confirmDelete').exists()).toBe(false)
  await body().get('#previewDelete').trigger('click'); await flushPromises()
  expect(body().get('#deleteSummary').text()).toContain('历史记录保留')
  await body().get('#deleteConfirmName').setValue('member')
  expect(body().get('#confirmDelete').attributes('disabled')).toBeDefined()
  await body().get('#deleteConfirmName').setValue('member-b')
  await select('deleteRecipient', 'c')
  expect(body().find('#deleteConfirmSection').exists()).toBe(false)
  await body().get('#previewDelete').trigger('click'); await flushPromises()
  await wrapper.setProps({ state: { ...state(), configVersion: 9 } })
  await body().get('#deleteConfirmName').setValue('member-b')
  await body().get('#confirmDelete').trigger('click'); await flushPromises()
  expect(api.mock.calls.map(([, value]) => (value as { action: string }).action)).toEqual(['preview-delete-member', 'preview-delete-member', 'delete-member'])
  expect(api.mock.calls.at(-1)).toEqual(['member-management', { action: 'delete-member', userId: 'b', transferToUserId: 'c', expectedConfigVersion: 1 }])
  expect(wrapper.emitted('changed')).toEqual([['成员已删除，全部剩余余额已转交，历史记录已保留。']])
  expect(body().find('#deleteDialog').exists()).toBe(false)
})

it('搜索、分组过滤、密码重置、邀请码及接单/停用确认保留原操作', async () => {
  const initial = state()
  const api = vi.fn<(...args: Parameters<ManagementApi>) => Promise<unknown>>().mockImplementation(async (_path, value) => {
    const request = value as { action: string; userId: string }
    if (request.action === 'credentials') return { id: request.userId, username: 'member-b' }
    if (request.action === 'member-key') return { apiKey: 'synthetic-reset-key' }
    if (request.action === 'discord-invite') return { code: 'synthetic-invite', expiresAt: 1800000000000 }
    return { cancelledCount: 2 }
  })
  const { wrapper, action, select } = await page(api, initial, 'admin')
  await wrapper.get('.table-search input').setValue('member-b')
  expect(wrapper.findAll('[data-member-id]')).toHaveLength(1)
  await wrapper.get('.table-search input').setValue(''); await select('filterGroup', 'b')
  expect(wrapper.findAll('[data-member-id]')).toHaveLength(1)
  await action('b', '重置密码')
  expect(body().get('#username').attributes('readonly')).toBeDefined()
  await body().get('#generatePassword').trigger('click')
  expect((body().get('#password').element as HTMLInputElement).value).toMatch(/^[a-f0-9]{48}$/)
  await body().get('#credentials').trigger('submit'); await flushPromises()
  expect(wrapper.emitted('changed')?.[0]).toEqual(['新密码已保存；原 API Key 不变。'])
  await body().get('#closeMemberOperation').trigger('click'); await flushPromises()
  for (const [label, expectedAction] of [['停止接单', 'member-admission'], ['停用', 'member-status']]) {
    const before = api.mock.calls.length
    await action('b', label!)
    expect(api).toHaveBeenCalledTimes(before)
    if (expectedAction === 'member-admission') expect(body().get('#confirmDescription').text()).toContain('在途生成正常完成')
    await body().get('#confirmAction').trigger('click'); await flushPromises()
    expect(api.mock.calls.at(-1)?.[1]).toMatchObject({ action: expectedAction, userId: 'b' })
  }
  await wrapper.setProps({ state: { ...state(), discordLogin: true } })
  await action('b', '邀请码')
  expect((body().get('#deliveryText').element as HTMLTextAreaElement).value).toContain('邀请码：synthetic-invite')
  await body().get('#closeMemberOperation').trigger('click'); await flushPromises()
  expect(body().find('#deliveryText').exists()).toBe(false)
})
