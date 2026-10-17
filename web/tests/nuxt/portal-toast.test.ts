import { afterEach, expect, it, vi } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import { enableAutoUnmount, flushPromises } from '@vue/test-utils'
import { defineComponent, h, ref } from 'vue'
import { NMessageProvider } from 'naive-ui'
import PortalToast from '../../app/components/PortalToast.vue'

enableAutoUnmount(afterEach)
afterEach(() => vi.useRealTimers())
it('通知使用全局消息层，定时关闭、可手动关闭，替换和卸载清理旧消息', async () => {
  vi.useFakeTimers()
  const message = ref('')
  const wrapper = await mountSuspended(defineComponent({ setup: () => () => h(NMessageProvider, { placement: 'top' }, {
    default: () => h(PortalToast, { message: message.value, type: 'success' })
  }) }))
  message.value = '保存成功'; await flushPromises()
  expect(document.querySelector('.n-message-container')?.textContent).toContain('保存成功')
  expect(document.querySelector('.n-message__close')).not.toBeNull()
  await vi.advanceTimersByTimeAsync(7000); await flushPromises()
  expect(document.querySelector('.n-message-container')?.textContent || '').not.toContain('保存成功')
  message.value = '第二条'; await flushPromises()
  ;(document.querySelector('.n-message__close') as HTMLElement).click()
  await vi.advanceTimersByTimeAsync(1000); await flushPromises()
  expect(document.querySelector('.n-message-container')?.textContent || '').not.toContain('第二条')
  message.value = '会话消息'; await flushPromises(); wrapper.unmount(); await flushPromises()
  expect(document.querySelector('.n-message-container')?.textContent || '').not.toContain('会话消息')
})
