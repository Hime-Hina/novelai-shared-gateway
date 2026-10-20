import { afterEach, describe, expect, it } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import { enableAutoUnmount } from '@vue/test-utils'
import ManagementServiceResources from '../../app/components/ManagementServiceResources.vue'
import type { ServiceResources } from '../../app/types/management'

enableAutoUnmount(afterEach)
const data: ServiceResources = {
  memory: { rssBytes: 128 * 1024 ** 2, heapUsedBytes: 32 * 1024 ** 2, heapTotalBytes: 64 * 1024 ** 2 },
  uptimeSeconds: 90061,
  data: { bytes: 1.5 * 1024 ** 3, checkedAt: '2026-10-21T00:00:00Z' },
  disk: { usedBytes: 30 * 1024 ** 3, totalBytes: 100 * 1024 ** 3, availableBytes: 65 * 1024 ** 3 }
}

describe('ManagementServiceResources', () => {
  it('显示可读单位、进程与磁盘统计范围，摘要更新时同步显示新结果', async () => {
    const wrapper = await mountSuspended(ManagementServiceResources, { props: { data } })
    expect(wrapper.get('#serviceMemory').text()).toBe('128 MiB')
    expect(wrapper.text()).toContain('JS 堆 32 MiB / 64 MiB')
    expect(wrapper.get('#serviceData').text()).toBe('1.5 GiB')
    expect(wrapper.get('#serviceDisk').text()).toBe('30 GiB / 100 GiB')
    expect(wrapper.text()).toContain('可用 65 GiB')
    expect(wrapper.get('#serviceUptime').text()).toBe('1 天 1 小时 1 分钟')
    expect(wrapper.text()).toContain('包含其他服务占用')
    expect(wrapper.text()).toContain('内存不含独立网页服务')
    expect(wrapper.text()).toContain('每分钟采样')
    await wrapper.setProps({ data: { ...data, memory: { ...data.memory, rssBytes: 256 * 1024 ** 2 } } })
    expect(wrapper.get('#serviceMemory').text()).toBe('256 MiB')
  })

  it('缺失数据不伪造零占用，真实零值照常显示', async () => {
    const wrapper = await mountSuspended(ManagementServiceResources)
    expect(wrapper.get('#serviceMemory').text()).toBe('暂不可用')
    expect(wrapper.get('#serviceData').text()).toBe('暂不可用')
    expect(wrapper.get('#serviceUptime').text()).toBe('暂不可用')
    await wrapper.setProps({ data: { ...data, data: null, disk: null } })
    expect(wrapper.get('#serviceMemory').text()).toBe('128 MiB')
    expect(wrapper.get('#serviceData').text()).toBe('暂不可用')
    expect(wrapper.get('#serviceDisk').text()).toBe('暂不可用 / 暂不可用')
    await wrapper.setProps({ data: { ...data, data: { ...data.data!, bytes: 0 }, uptimeSeconds: 0 } })
    expect(wrapper.get('#serviceData').text()).toBe('0 B')
    expect(wrapper.get('#serviceUptime').text()).toBe('0 天 0 小时 0 分钟')
  })

  it('显示 R2 转存容量、图片数与异常，未配置时隐藏', async () => {
    const wrapper = await mountSuspended(ManagementServiceResources, { props: { data } })
    expect(wrapper.find('#serviceR2').exists()).toBe(false)
    await wrapper.setProps({ data: { ...data, r2: { bytes: 1024 ** 3, limitBytes: 9_500_000_000, images: 100, pending: 1, errors: 1 } } })
    expect(wrapper.get('#serviceR2').text()).toContain('R2 1 GiB / 8.848 GiB')
    expect(wrapper.text()).toContain('已转存 100 张')
    expect(wrapper.text()).toContain('待验证 1 张 · 转存异常 1 张')
    expect(wrapper.text()).toContain('达上限暂停新增转存')
    const status = wrapper.get('#serviceR2Status').element
    await wrapper.setProps({ data: { ...data, r2: { bytes: 1024 ** 3, limitBytes: 9_500_000_000, images: 101, pending: 0, errors: 0 } } })
    expect(wrapper.get('#serviceR2Status').element).toBe(status)
    expect(wrapper.get('#serviceR2Status').text()).toBe('待验证 0 张 · 转存异常 0 张')
  })
})
