import { sharedSurface } from '../../../server/shared-surface.ts'

export default defineEventHandler(async event => {
  const path = event.path.split('?')[0]!
  if (!/^\/(?:api|ai|v1|user)(?:\/|$)/.test(path) && path !== '/generate') return
  // Nuxt 的网页兜底会抢先处理 API 方法不匹配；沿用原白名单选择传输，仍由执行器最终判定。
  if (['HEAD', 'OPTIONS'].includes(event.method) || !sharedSurface(path, event.method).allowed) {
    await forwardExecutorStream(event)
  }
})
