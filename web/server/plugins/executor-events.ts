import { closeExecutorEvents } from '../utils/executor-events'
import { closeExecutorStreams } from '../utils/executor-stream'

export default defineNitroPlugin((nitro) => {
  const close = () => { closeExecutorEvents(); closeExecutorStreams() }
  // Nitro 2 的 close hook 在 HTTP 排空之后；先结束长期等待，执行器继续处理持久任务。
  process.on('SIGTERM', close)
  process.on('SIGINT', close)
  nitro.hooks.hook('close', () => {
    close()
    process.off('SIGTERM', close)
    process.off('SIGINT', close)
  })
})
