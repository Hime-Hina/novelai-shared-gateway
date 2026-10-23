import http, { type ClientRequest, type IncomingMessage } from 'node:http'
import https from 'node:https'
import { pipeline } from 'node:stream/promises'
import type { H3Event } from 'h3'
import { executorConnection, executorResponseHeaders, sendExecutorError } from './executor-client'

const activeStreams = new Set<() => void>()
let closing = false

export function closeExecutorStreams() {
  closing = true
  for (const close of activeStreams) close()
}

// 大正文、图片和长期等待逐块传输；解析、载荷预算与任务期限由唯一执行器负责。
export async function forwardExecutorStream(event: H3Event) {
  if (closing) { sendExecutorError(event, { statusCode: 503 }); return }
  const { req, res } = event.node
  const disconnected = new AbortController()
  const disconnect = () => disconnected.abort()
  let request: ClientRequest | undefined
  let response: IncomingMessage | undefined
  const close = () => {
    if (res.destroyed) { disconnect(); return }
    if (res.writableEnded) return
    if (!res.headersSent) {
      sendExecutorError(event, { statusCode: 503 })
      disconnect()
    } else if (response?.headers['content-type']?.startsWith('text/event-stream')) {
      // 先送出 SSE 的普通 EOF，再取消内部等待；二进制残片则必须表现为中断。
      res.once('finish', disconnect)
      res.end()
    } else {
      disconnect()
      res.destroy()
    }
  }
  activeStreams.add(close)
  req.once('error', disconnect)
  res.once('close', disconnect)
  try {
    const { target, headers } = executorConnection(event)
    response = await new Promise<IncomingMessage>((resolve, reject) => {
      request = (target.protocol === 'https:' ? https : http).request(target, {
        method: event.method, path: event.path, headers, signal: disconnected.signal
      }, resolve)
      request.once('error', reject)
      // 上传使用 pipe 的背压；不能用 pipeline 销毁客户端后再尝试转发提前返回的 401/413。
      req.pipe(request)
    })
    if (response.headers['x-executor-auth-error'] === '1') throw new Error('Executor authentication failed')
    res.writeHead(response.statusCode || 502, executorResponseHeaders(response))
    res.flushHeaders()
    await pipeline(response, res)
  } catch (error) {
    if (!disconnected.signal.aborted) sendExecutorError(event, error)
  } finally {
    activeStreams.delete(close)
    if (request) req.unpipe(request)
    disconnect()
    response?.destroy()
    request?.destroy()
    req.off('error', disconnect)
    res.off('close', disconnect)
    res.off('finish', disconnect)
    // 执行器可能在客户端尚未传完正文时拒绝请求；丢弃剩余输入而不缓存。
    if (!req.complete) req.resume()
  }
}
