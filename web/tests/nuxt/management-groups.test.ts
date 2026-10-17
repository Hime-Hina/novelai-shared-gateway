import { afterEach, describe, expect, it, vi } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import { enableAutoUnmount, flushPromises } from '@vue/test-utils'
import { nextTick } from 'vue'
import { NCheckbox, NSelect } from 'naive-ui'
import ManagementGroups from '../../app/components/ManagementGroups.vue'
import type { ManagementApi, ManagementGroup, ManagementRun, ManagementState } from '../../app/types/management'

enableAutoUnmount(afterEach)
afterEach(() => vi.restoreAllMocks())

function snapshot(configVersion = 7): ManagementState {
  const quota = { member: true, status: 'active', groupName: '一组', groupSize: 2, share: .5, autoAnlasFallback: false,
    v5Available: 10, v5Capacity: 50, v5Reserved: 0, v5RefillPerDay: 5, v5RefillLimited: false, v5FullRefillSeconds: 100,
    anlasFixedAvailable: 100, anlasPurchasedAvailable: 20, anlasFixedReserved: 0, anlasPurchasedReserved: 0 }
  return { configVersion, groups: [
    { id: 'g1', name: '一组', share: 1, members: ['a', 'b'], memberWeights: { a: 1, b: 1 } },
    ...['g2', 'g3', 'g4'].map(id => ({ id, name: id, share: 0, members: [], memberWeights: {} }))
  ], autoAnlasFallback: false, publicBaseUrl: '', discordLogin: false, ownerUserId: null,
  shared: { status: 'active', subscriptionActive: true, expiresAt: null, officialCheckedAt: null, stale: false,
    v5Percent: null, v5IsNegative: false, refillSecondsPerPercent: null, anlasFixed: null, anlasPurchased: null,
    running: 0, queued: 0, pending: 0, concurrencyLimit: 1, cooldownUntil: null, cooldownReason: null, quotaSyncError: false, recentFailures: 0 },
  users: ['a', 'b', 'c'].map(id => ({ id, username: id.toUpperCase(), enabled: true, acceptingJobs: true, note: '', discordId: null, inviteExpiresAt: null,
    quota: id === 'c' ? null : { ...quota }, recentActivity: { total: 0, done: 0, failed: 0, queued: 0, running: 0 } })) }
}
type Payload = { action: string; groups: ManagementGroup[]; redistributeRemaining: boolean; expectedConfigVersion: number }
function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>(done => { resolve = done })
  return { promise, resolve }
}
const previewResult = () => { const state = snapshot(); return { groups: state.groups, users: state.users, configVersion: state.configVersion } }
async function fixture(transport = vi.fn().mockResolvedValue(previewResult())) {
  const errors: unknown[] = []
  const api: ManagementApi = async <T>(path: string, body?: unknown, method?: 'GET' | 'POST' | 'PATCH') => await transport(path, body, method) as T
  const run: ManagementRun = async operation => { try { await operation() } catch (error) { errors.push(error) } }
  const wrapper = await mountSuspended(ManagementGroups, { props: { state: snapshot(), api, run, busy: false }, global: { stubs: { Teleport: true } } })
  return { wrapper, transport, errors }
}
type Wrapper = Awaited<ReturnType<typeof fixture>>['wrapper']
async function mode(wrapper: Wrapper, value: string) {
  wrapper.findComponent(NSelect).vm.$emit('update:value', value)
  await nextTick()
}
async function shares(wrapper: Wrapper, a: number, b: number) {
  await wrapper.get('input#direct-a').setValue(String(a))
  await wrapper.get('input#direct-b').setValue(String(b))
}
const value = (wrapper: Wrapper, id: string) => (wrapper.get('input#' + id).element as HTMLInputElement).value
async function preview(wrapper: Wrapper) { await wrapper.get('#previewGroups').trigger('click'); await flushPromises() }

describe('ManagementGroups', () => {
  it('两种模式共用分组草稿，并分别保留重新分配余额的选择', async () => {
    const { wrapper } = await fixture()
    await shares(wrapper, 75, 25)
    expect(wrapper.findComponent(NCheckbox).props('checked')).toBe(true)
    wrapper.findComponent(NCheckbox).vm.$emit('update:checked', false)
    await mode(wrapper, 'advanced')
    expect(value(wrapper, 'weight-a')).toBe('75')
    expect(value(wrapper, 'weight-b')).toBe('25')
    expect(wrapper.findComponent(NCheckbox).props('checked')).toBe(false)
    wrapper.findComponent(NCheckbox).vm.$emit('update:checked', true)
    await wrapper.get('input#group-name-g1').setValue('修改后的组名')
    await wrapper.get('input#weight-a').setValue('1')
    await wrapper.get('input#weight-b').setValue('3')
    await mode(wrapper, 'direct')
    expect(value(wrapper, 'direct-a')).toBe('25')
    expect(value(wrapper, 'direct-b')).toBe('75')
    expect(wrapper.findComponent(NCheckbox).props('checked')).toBe(false)
    await mode(wrapper, 'advanced')
    expect(value(wrapper, 'group-name-g1')).toBe('修改后的组名')
    expect(wrapper.findComponent(NCheckbox).props('checked')).toBe(true)
    expect(wrapper.emitted('dirty')?.at(-1)).toEqual([true])
  })

  it('后台快照不替换脏草稿或原版本，预览409后仍可撤销到最新配置', async () => {
    const conflict = Object.assign(new Error('分组版本已变化'), { statusCode: 409 })
    const { wrapper, transport, errors } = await fixture(vi.fn().mockRejectedValue(conflict))
    await shares(wrapper, 70, 30)
    const latest = snapshot(8)
    latest.groups[0]!.memberWeights = { a: 1, b: 9 }
    await wrapper.setProps({ state: latest })
    expect(value(wrapper, 'direct-a')).toBe('70')
    expect(wrapper.get('#draftHint').text()).toContain('版本 7')
    await preview(wrapper)
    expect((transport.mock.calls[0]![1] as Payload).expectedConfigVersion).toBe(7)
    expect(errors).toEqual([conflict])
    expect(value(wrapper, 'direct-a')).toBe('70')
    expect(wrapper.emitted('dirty')?.at(-1)).toEqual([true])
    await wrapper.get('#resetDraft').trigger('click')
    expect(value(wrapper, 'direct-a')).toBe('10')
    expect(wrapper.get('#draftHint').text()).toContain('版本 8')
    expect(wrapper.emitted('dirty')?.at(-1)).toEqual([false])
  })

  it('预览和保存使用同一payload/version；保存409保留草稿和预览', async () => {
    const conflict = Object.assign(new Error('分组版本已变化'), { statusCode: 409 })
    const transport = vi.fn().mockResolvedValueOnce(previewResult()).mockRejectedValueOnce(conflict)
    const { wrapper, errors } = await fixture(transport)
    await shares(wrapper, 70, 30)
    await preview(wrapper)
    const first = structuredClone(transport.mock.calls[0]![1] as Payload)
    await wrapper.setProps({ state: snapshot(9) })
    await wrapper.get('#confirmGroups').trigger('click'); await flushPromises()
    expect(transport.mock.calls[1]![1]).toEqual({ ...first, action: 'groups' })
    expect(errors).toEqual([conflict])
    expect(value(wrapper, 'direct-a')).toBe('70')
    expect(wrapper.get('#draftHint').text()).toContain('版本 7')
    expect(wrapper.find('#confirmGroups').exists()).toBe(true)
    expect(wrapper.emitted('changed')).toBeUndefined()
  })

  it('预览期间编辑使迟到预览失效，不能确认旧分配', async () => {
    const pending = deferred<ReturnType<typeof previewResult>>()
    const { wrapper } = await fixture(vi.fn().mockReturnValue(pending.promise))
    await shares(wrapper, 70, 30)
    await wrapper.get('#previewGroups').trigger('click')
    await shares(wrapper, 60, 40)
    pending.resolve(previewResult()); await flushPromises()
    expect(wrapper.find('#confirmGroups').exists()).toBe(false)
    expect(wrapper.emitted('notice')?.at(-1)?.[0]).toContain('预览期间草稿已修改')
    expect(value(wrapper, 'direct-a')).toBe('60')
  })

  it('保存中继续编辑不丢草稿，只有本次成功响应能推进版本，刷新不重发保存', async () => {
    const saving = deferred<{ groups: ManagementGroup[]; configVersion: number }>()
    const transport = vi.fn().mockResolvedValueOnce(previewResult()).mockReturnValueOnce(saving.promise).mockResolvedValue(previewResult())
    const { wrapper } = await fixture(transport)
    await shares(wrapper, 70, 30); await preview(wrapper)
    await wrapper.get('#confirmGroups').trigger('click')
    const saved = structuredClone((transport.mock.calls[1]![1] as Payload).groups)
    await shares(wrapper, 60, 40)
    await wrapper.setProps({ state: snapshot(20) })
    saving.resolve({ groups: saved, configVersion: 8 }); await flushPromises()
    expect(value(wrapper, 'direct-a')).toBe('60')
    expect(wrapper.get('#draftHint').text()).toContain('版本 8')
    expect(wrapper.emitted('dirty')?.at(-1)).toEqual([true])
    expect(wrapper.emitted('changed')?.at(-1)?.[0]).toContain('刚才预览已保存，后续修改仍未保存')
    expect(transport).toHaveBeenCalledTimes(2)
    await preview(wrapper)
    expect((transport.mock.calls[2]![1] as Payload).expectedConfigVersion).toBe(8)
    expect((transport.mock.calls[2]![1] as Payload).groups[0]?.memberWeights).toMatchObject({ a: 60, b: 40 })
  })

  it('保存成功立即清dirty，旧快照及未完成刷新不能恢复可重发的保存', async () => {
    const transport = vi.fn().mockResolvedValueOnce(previewResult()).mockImplementation(async (_path, body: Payload) => ({ groups: body.groups, configVersion: 8 }))
    const { wrapper } = await fixture(transport)
    await shares(wrapper, 70, 30); await preview(wrapper)
    await wrapper.get('#confirmGroups').trigger('click'); await flushPromises()
    expect(wrapper.emitted('dirty')?.at(-1)).toEqual([false])
    expect(wrapper.emitted('changed')).toEqual([['分组与额度分配已保存。']])
    expect(wrapper.find('#confirmGroups').exists()).toBe(false)
    await wrapper.setProps({ state: snapshot(7) })
    await wrapper.get('#resetDraft').trigger('click')
    expect(value(wrapper, 'direct-a')).toBe('70')
    expect(wrapper.get('#draftHint').text()).toContain('版本 8')
    expect(wrapper.emitted('notice')?.at(-1)).toEqual(['分组已保存，但页面配置尚未刷新；请先刷新管理摘要再撤销修改。', true])
    expect(transport).toHaveBeenCalledTimes(2)
  })

  it('高级模式校验空组，分配成员和组份额后才发送有效预览', async () => {
    const { wrapper, transport, errors } = await fixture()
    await mode(wrapper, 'advanced')
    await wrapper.get('input#group-share-g1').setValue('60')
    await wrapper.get('input#group-share-g2').setValue('40')
    await preview(wrapper)
    expect((errors[0] as Error).message).toBe('空组份额须为 0%。')
    expect(transport).not.toHaveBeenCalled()
    wrapper.findAllComponents(NSelect).at(-1)!.vm.$emit('update:value', 'g2')
    await nextTick(); await preview(wrapper)
    const body = transport.mock.calls[0]![1] as Payload
    expect(body.groups[1]).toMatchObject({ share: .4, members: ['c'], memberWeights: { c: 1 } })
    expect(body.redistributeRemaining).toBe(false)
    expect(body.expectedConfigVersion).toBe(7)
    expect(wrapper.get('#previewDescription').text()).toContain('当前余额保持原样')
  })
})
