import http, { type ClientRequest, type IncomingMessage } from 'node:http'
import https from 'node:https'
import { once } from 'node:events'
import { pipeline } from 'node:stream/promises'
import { setTimeout as delay } from 'node:timers/promises'
import type { H3Event } from 'h3'
import { executorConnection, executorResponseHeaders, sendExecutorError } from './executor-client'

const connectionFrame = (state: 'open' | 'retrying') => `event: connection-status\ndata: ${JSON.stringify({ state })}\n\n`
const expiredFrame = 'event: auth-expired\ndata: {}\n\n'
const activeBridges = new Set<() => void>()
let closing = false

export function closeExecutorEvents() {
  closing = true
  for (const close of activeBridges) close()
}

// 只桥接执行器现有 LF 状态协议；版本、权限和重连快照仍由执行器决定。
export async function forwardExecutorEvents(event: H3Event) {
  const { res } = event.node
  if (closing) { sendExecutorError(event, { statusCode: 503 }); return }
  const disconnected = new AbortController()
  const disconnect = () => disconnected.abort()
  const close = () => {
    disconnect()
    if (res.destroyed || res.writableEnded) return
    if (res.headersSent) res.end()
    else sendExecutorError(event, { statusCode: 503 })
  }
  activeBridges.add(close)
  res.once('close', disconnect)
  res.once('error', disconnect)
  let connected = false

  async function writeFrame(frame: string | Buffer) {
    disconnected.signal.throwIfAborted()
    if (!res.write(frame)) {
      try {
        await once(res, 'drain', { signal: AbortSignal.any([disconnected.signal, AbortSignal.timeout(60000)]) })
      } catch (error) {
        // 不为一个始终不读取的浏览器无限保留内部连接。
        res.destroy()
        throw error
      }
    }
  }

  try {
    const { target, headers } = executorConnection(event)
    while (!disconnected.signal.aborted) {
      let request: ClientRequest | undefined
      let response: IncomingMessage | undefined
      let handshake: ReturnType<typeof setTimeout> | undefined
      try {
        response = await new Promise<IncomingMessage>((resolve, reject) => {
          request = (target.protocol === 'https:' ? https : http).request(target, {
            method: 'GET', path: event.path, headers, signal: disconnected.signal
          }, upstream => { clearTimeout(handshake); resolve(upstream) })
          handshake = setTimeout(() => request?.destroy(new Error('Executor handshake timed out')), 45000)
          request.once('error', reject)
          request.end()
        })
        response.setTimeout(60000, () => response?.destroy(new Error('Executor event stream stalled')))
        if (response.headers['x-executor-auth-error'] === '1') throw new Error('Executor authentication failed')
        if (response.statusCode !== 200) {
          if (!connected) {
            res.writeHead(response.statusCode || 502, executorResponseHeaders(response))
            await pipeline(response, res)
            return
          }
          if ([401, 403].includes(response.statusCode || 0)) {
            await writeFrame(expiredFrame)
            res.end()
            return
          }
          throw new Error('Executor event stream unavailable')
        }
        if (!response.headers['content-type']?.startsWith('text/event-stream')) throw new Error('Invalid executor event stream')
        if (!connected) {
          res.writeHead(200, executorResponseHeaders(response))
          res.flushHeaders()
          connected = true
        }
        await writeFrame(connectionFrame('open'))

        // 固定 64 KiB 未完成帧缓冲；不解码半个 UTF-8 字符，也不把残片带到下一次连接。
        const pending = Buffer.alloc(64 * 1024)
        let length = 0
        for await (const chunk of response) {
          let offset = 0
          while (offset < chunk.length) {
            const count = Math.min(pending.length - length, chunk.length - offset)
            chunk.copy(pending, length, offset, offset + count)
            length += count
            offset += count
            let boundary = pending.subarray(0, length).indexOf('\n\n')
            while (boundary !== -1) {
              const frame = Buffer.from(pending.subarray(0, boundary + 2))
              pending.copy(pending, 0, boundary + 2, length)
              length -= boundary + 2
              await writeFrame(frame)
              if (/(?:^|\n)event: *auth-expired\n/.test(frame.toString('utf8'))) {
                res.end()
                return
              }
              boundary = pending.subarray(0, length).indexOf('\n\n')
            }
            if (length === pending.length) throw new Error('Executor event frame exceeds 64 KiB')
          }
        }
      } catch (error) {
        if (disconnected.signal.aborted) return
        if (!connected) { sendExecutorError(event, error); return }
      } finally {
        clearTimeout(handshake)
        response?.destroy()
        request?.destroy()
      }
      await writeFrame(connectionFrame('retrying'))
      await delay(5000, undefined, { signal: disconnected.signal })
    }
  } catch (error) {
    if (!disconnected.signal.aborted) sendExecutorError(event, error)
  } finally {
    activeBridges.delete(close)
    disconnect()
    res.off('close', disconnect)
    res.off('error', disconnect)
  }
}
