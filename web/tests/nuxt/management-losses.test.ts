import { afterEach, describe, expect, it, vi } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import { enableAutoUnmount, flushPromises } from '@vue/test-utils'
import { NInputNumber, NSelect } from 'naive-ui'
import ManagementLosses from '../../app/components/ManagementLosses.vue'
import type { ManagementApi, ManagementLosses as LossSnapshot, ManagementLossPolicy, ManagementRun, ManagementState } from '../../app/types/management'

enableAutoUnmount(afterEach)
const zero = () => ({ v5: 0, anlasFixed: 0, anlasPurchased: 0 })
const policy: ManagementLossPolicy = { version: 1, v5Points24h: 1, fixedAnlas24h: 100, fixedAnlas30d: 500, memberAttempts24h: 3 }
const state: ManagementState = { groups: [], configVersion: 1, autoAnlasFallback: false, publicBaseUrl: '', discordLogin: true,
  ownerUserId: 'owner', users: [], shared: { status: 'active', subscriptionActive: true, expiresAt: null, officialCheckedAt: null,
    stale: false, v5Percent: 90, v5IsNegative: false, refillSecondsPerPercent: 100, anlasFixed: 1000, anlasPurchased: 100,
    running: 0, queued: 0, pending: 0, concurrencyLimit: 1, cooldownUntil: null, cooldownReason: null, quotaSyncError: false, recentFailures: 0 } }
function snapshot(ids = ['first'], version = 1, cursor: string | null = 'older'): LossSnapshot {
  return { asOf: '2026-10-17T00:00:00Z', policy: { ...policy, version },
    alerts: [{ id: 'risk', level: 'warning', message: '真实服务端风险提示' }],
    risk: { last24h: { attempts: 1, unknownAmounts: 1, possible: zero() }, last30d: { attempts: 2, unknownAmounts: 1, possible: zero() },
      aggregate: { last24h: zero(), last30d: zero() } },
    operating: { available: zero(), unfunded: { anlasFixed: 10, anlasPurchased: 0 }, unattributedReturns: { anlasFixed: 5, anlasPurchased: 0 },
      v5CapacityGap: 0, gapEventIds: { v5: null, anlasFixed: 'current-gap', anlasPurchased: null }, pendingExternalFunding: [] },
    events: ids.map(id => ({ id, type: 'generation_loss_suspected', at: '2026-10-17T00:00:00Z', memberName: 'Alice', actorId: null,
      upstreamCandidates: [{ resources: { v5: 1 } }, { resources: { anlasFixed: 20 } }], estimateAmountStatus: 'unknown', refundRetained: true })),
    nextCursor: cursor }
}
function deferred<T>() {
  let resolve!: (value: T) => void, reject!: (reason: unknown) => void
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
type Transport = (...args: Parameters<ManagementApi>) => Promise<unknown>
async function mount(transport = vi.fn<Transport>().mockResolvedValue(snapshot()), active = true) {
  const errors: unknown[] = [], alerts = vi.fn()
  const api: ManagementApi = async <T>(...args: Parameters<ManagementApi>) => await transport(...args) as T
  const run: ManagementRun = async operation => { try { await operation() } catch (error) { errors.push(error) } }
  const wrapper = await mountSuspended(ManagementLosses, { props: { state, api, run, busy: false, refreshVersion: 0, active, onAlerts: alerts } })
  await flushPromises()
  return { wrapper, transport, errors, alerts }
}

describe('损耗、告警配置与运营账务', () => {
  it('非当前tab仍读取真实告警，后台通知合并为单在途加一次补读', async () => {
    const first = deferred<LossSnapshot>(), second = deferred<LossSnapshot>()
    const transport = vi.fn<Transport>().mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise)
    const { wrapper } = await mount(transport, false)
    expect(transport).toHaveBeenCalledExactlyOnceWith('member-management?view=losses')
    await wrapper.setProps({ refreshVersion: 1 }); await wrapper.setProps({ refreshVersion: 2 })
    expect(transport).toHaveBeenCalledTimes(1)
    first.resolve(snapshot()); await flushPromises()
    expect(transport).toHaveBeenCalledTimes(2)
    second.resolve(snapshot(['latest'])); await flushPromises()
    expect(wrapper.emitted('alerts')?.at(-1)).toEqual([snapshot().alerts])
    expect(transport).toHaveBeenCalledTimes(2)
  })

  it('阈值409保留草稿与编辑版本，背景最新版本不覆盖；显式放弃后才采用服务器值', async () => {
    const transport = vi.fn<Transport>().mockResolvedValue(snapshot())
    const { wrapper, errors } = await mount(transport)
    const field = wrapper.findAllComponents(NInputNumber).find(input => input.props('inputProps')?.id === 'loss-fixedAnlas24h')!
    field.vm.$emit('update:value', 222); await flushPromises()
    await wrapper.setProps({ refreshVersion: 1 })
    transport.mockResolvedValue(snapshot(['new'], 2)); await wrapper.setProps({ refreshVersion: 2 }); await flushPromises()
    expect(field.props('value')).toBe(222); expect(wrapper.get('#lossPolicyHint').text()).toContain('编辑版本 1')
    transport.mockRejectedValueOnce(Object.assign(new Error('告警配置已变化'), { statusCode: 409 }))
    await wrapper.get('#lossPolicyForm').trigger('submit'); await flushPromises()
    expect(transport).toHaveBeenLastCalledWith('member-management', { action: 'loss-alert-policy', expectedVersion: 1,
      policy: { v5Points24h: 1, fixedAnlas24h: 222, fixedAnlas30d: 500, memberAttempts24h: 3 } })
    expect(errors).toHaveLength(1); expect(field.props('value')).toBe(222)
    expect(wrapper.emitted('changed')).toBeUndefined()
    await wrapper.get('#resetLossPolicy').trigger('click'); await flushPromises()
    expect(field.props('value')).toBe(100); expect(wrapper.get('#lossPolicyHint').text()).toContain('编辑版本 2')
    expect(wrapper.get('#lossPolicyHint').text()).toContain('尚无修改')
  })

  it('保存阈值时继续编辑不丢失，下一次保存采用成功返回的新版本', async () => {
    const transport = vi.fn<Transport>().mockResolvedValue(snapshot()), saving = deferred<{ policy: ManagementLossPolicy }>()
    const { wrapper } = await mount(transport)
    const field = wrapper.findAllComponents(NInputNumber).find(input => input.props('inputProps')?.id === 'loss-fixedAnlas24h')!
    field.vm.$emit('update:value', 200); await flushPromises()
    transport.mockReturnValueOnce(saving.promise)
    await wrapper.get('#lossPolicyForm').trigger('submit')
    field.vm.$emit('update:value', 250); await flushPromises()
    saving.resolve({ policy: { ...policy, version: 2, fixedAnlas24h: 200 } }); await flushPromises()
    expect(field.props('value')).toBe(250); expect(wrapper.get('#lossPolicyHint').text()).toContain('编辑版本 2')
    expect(wrapper.emitted('changed')?.at(-1)?.[0]).toContain('后续编辑仍未保存')
    transport.mockResolvedValueOnce({ policy: { ...policy, version: 3, fixedAnlas24h: 250 } })
    await wrapper.get('#lossPolicyForm').trigger('submit'); await flushPromises()
    const post = transport.mock.calls.filter(call => call[1] !== undefined).at(-1)
    expect(post?.[1]).toMatchObject({ action: 'loss-alert-policy', expectedVersion: 2, policy: { fixedAnlas24h: 250 } })
  })

  it('翻页后背景新首页只更新统计告警，保留连续历史与旧游标，显式重读才重置', async () => {
    const transport = vi.fn<Transport>().mockResolvedValue(snapshot(['one'], 1, 'cursor-one'))
    const { wrapper } = await mount(transport)
    transport.mockResolvedValueOnce(snapshot(['one', 'two'], 1, 'cursor-two'))
    await wrapper.get('#moreLossEvents').trigger('click'); await flushPromises()
    expect(transport).toHaveBeenLastCalledWith('member-management?view=losses&before=cursor-one')
    expect(wrapper.get('#lossEvents').findAll('tr')).toHaveLength(2)
    transport.mockResolvedValueOnce(snapshot(['new-head'], 2, 'new-cursor'))
    await wrapper.setProps({ refreshVersion: 1 }); await flushPromises()
    expect(wrapper.get('#lossEvents').text()).not.toContain('new-head')
    expect(wrapper.text()).toContain('互斥，不相加'); expect(wrapper.text()).toContain('金额未知，不能视为零消费')
    transport.mockResolvedValueOnce(snapshot(['three'], 2, null))
    await wrapper.get('#moreLossEvents').trigger('click'); await flushPromises()
    expect(transport).toHaveBeenLastCalledWith('member-management?view=losses&before=cursor-two')
    expect(wrapper.get('#lossEvents').findAll('tr')).toHaveLength(3)
    transport.mockResolvedValueOnce(snapshot(['fresh'], 2, null))
    await wrapper.get('#reloadLossEvents').trigger('click'); await flushPromises()
    expect(wrapper.get('#lossEvents').findAll('tr')).toHaveLength(1)
    expect(wrapper.get('#lossEvents').text()).toContain('fresh')
  })

  it('运营失败保留收据和金额，重复点击只提交一次，成功重复收据不重复入账并清空', async () => {
    const transport = vi.fn<Transport>().mockResolvedValue(snapshot()), saving = deferred<unknown>()
    const { wrapper, errors } = await mount(transport)
    const amount = wrapper.findAllComponents(NInputNumber).find(input => input.props('inputProps')?.id === 'operatingAmount')!
    amount.vm.$emit('update:value', 12)
    await wrapper.get('#operatingReceipt').setValue('receipt_123')
    transport.mockReturnValueOnce(saving.promise)
    await wrapper.get('#operatingForm').trigger('submit'); await wrapper.get('#operatingForm').trigger('submit')
    expect(transport.mock.calls.filter(call => call[1] !== undefined)).toHaveLength(1)
    saving.reject(new Error('网络中断，结果未确认')); await flushPromises()
    expect(errors).toHaveLength(1); expect(amount.props('value')).toBe(12)
    expect((wrapper.get('#operatingReceipt').element as HTMLInputElement).value).toBe('receipt_123')
    transport.mockResolvedValueOnce({ eventId: 'operating-receipt:receipt_123', duplicate: true })
    await wrapper.get('#operatingForm').trigger('submit'); await flushPromises()
    const posts = transport.mock.calls.filter(call => call[1] !== undefined)
    expect(posts).toHaveLength(2); expect(posts[0]?.[1]).toEqual(posts[1]?.[1])
    expect(posts[1]?.[1]).toEqual({ action: 'operating-contribute', resource: 'anlasFixed', amount: 12, receipt: 'receipt_123' })
    expect(amount.props('value')).toBeNull(); expect((wrapper.get('#operatingReceipt').element as HTMLInputElement).value).toBe('')
    expect(wrapper.emitted('changed')?.at(-1)?.[0]).toContain('未重复入账')
  })

  it('登记资金只发送其归属选项，核销使用指定差额且后台读取不替换输入', async () => {
    const transport = vi.fn<Transport>().mockResolvedValue(snapshot())
    const { wrapper } = await mount(transport)
    const select = wrapper.findAllComponents(NSelect).find(input => input.attributes('id') === 'operatingAction')!
    select.vm.$emit('update:value', 'operating-register-funding'); await flushPromises()
    const amount = wrapper.findAllComponents(NInputNumber).find(input => input.props('inputProps')?.id === 'operatingAmount')!
    amount.vm.$emit('update:value', 10); await wrapper.get('#operatingReceipt').setValue('funding_123')
    transport.mockResolvedValueOnce({ eventId: 'funding', duplicate: false })
    await wrapper.get('#operatingForm').trigger('submit'); await flushPromises()
    expect(transport.mock.calls.find(call => call[1] !== undefined)?.[1]).toEqual({ action: 'operating-register-funding', resource: 'anlasFixed', amount: 10, receipt: 'funding_123', destination: 'operating', fromUnattributed: false })
    select.vm.$emit('update:value', 'operating-cover-loss'); await flushPromises()
    await wrapper.get('#operatingLossId').setValue('specific-gap')
    await wrapper.setProps({ refreshVersion: 1 }); await flushPromises()
    expect((wrapper.get('#operatingLossId').element as HTMLInputElement).value).toBe('specific-gap')
    amount.vm.$emit('update:value', 5); await wrapper.get('#operatingReceipt').setValue('cover_123')
    transport.mockResolvedValueOnce({ eventId: 'cover', duplicate: false })
    await wrapper.get('#operatingForm').trigger('submit'); await flushPromises()
    expect(transport.mock.calls.filter(call => call[1] !== undefined).at(-1)?.[1]).toEqual({ action: 'operating-cover-loss', resource: 'anlasFixed', amount: 5, receipt: 'cover_123', lossEventId: 'specific-gap' })
    select.vm.$emit('update:value', 'operating-reconcile-return'); await flushPromises()
    amount.vm.$emit('update:value', 2); await wrapper.get('#operatingReceipt').setValue('return_123')
    transport.mockResolvedValueOnce({ eventId: 'return', duplicate: false })
    await wrapper.get('#operatingForm').trigger('submit'); await flushPromises()
    expect(transport.mock.calls.filter(call => call[1] !== undefined).at(-1)?.[1]).toEqual({ action: 'operating-reconcile-return', resource: 'anlasFixed', amount: 2, receipt: 'return_123', lossEventId: 'current-gap' })
  })

  it('卸载后迟到损耗结果不再发出告警，读取失败不伪造空告警', async () => {
    const transport = vi.fn<Transport>().mockResolvedValue(snapshot()), late = deferred<LossSnapshot>()
    const { wrapper, alerts } = await mount(transport)
    transport.mockRejectedValueOnce(new Error('temporary'))
    await wrapper.setProps({ refreshVersion: 1 }); await flushPromises()
    expect(wrapper.emitted('alerts')).toHaveLength(1); expect(wrapper.emitted('notice')?.at(-1)?.[1]).toBe(true)
    transport.mockReturnValueOnce(late.promise)
    await wrapper.setProps({ refreshVersion: 2 }); wrapper.unmount()
    late.resolve(snapshot(['late'])); await flushPromises()
    expect(alerts).toHaveBeenCalledTimes(1)
  })
})
