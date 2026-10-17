import { closeExecutorEvents } from '../utils/executor-events'

export default defineNitroPlugin((nitro) => {
  // Nitro 2 的 close hook 在 HTTP 排空之后；信号到达先结束 SSE，避免互相等待。
  process.on('SIGTERM', closeExecutorEvents)
  process.on('SIGINT', closeExecutorEvents)
  nitro.hooks.hook('close', () => {
    closeExecutorEvents()
    process.off('SIGTERM', closeExecutorEvents)
    process.off('SIGINT', closeExecutorEvents)
  })
})
