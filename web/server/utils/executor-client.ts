import http, { type IncomingMessage } from 'node:http'
import https from 'node:https'
import { randomUUID } from 'node:crypto'
import { pipeline } from 'node:stream/promises'
import type { H3Event } from 'h3'
import { createError, useRuntimeConfig } from '#imports'
import { createGenerationPayloadBudget } from '../../../server/generation-request.js'

const MiB = 1024 * 1024
// 复用现有纯内存预算：单次暂存正文与受理回执共 4 MiB，全进程 64 MiB，不加载存储。
const buffered = createGenerationPayloadBudget(() => 0, { request: 4 * MiB, active: 64 * MiB })
const hopHeaders = ['connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization', 'te', 'trailer', 'transfer-encoding', 'upgrade']
const unavailable = () => createError({ statusCode: 503, statusMessage: 'Executor unavailable', message: '执行服务暂不可用，请稍后重试' })

export function executorResponseHeaders(message: IncomingMessage) {
  const headers = endToEndHeaders(message)
  for (let i = headers.length - 2; i >= 0; i -= 2) {
    if (headers[i]!.toLowerCase() === 'x-executor-auth-error') headers.splice(i, 2)
  }
  return headers
}

function endToEndHeaders(message: IncomingMessage) {
  const omitted = new Set([...hopHeaders, ...(message.headers.connection || '').toLowerCase().split(',').map(name => name.trim())])
  const headers: string[] = []
  for (let i = 0; i < message.rawHeaders.length; i += 2) {
    const name = message.rawHeaders[i]!
    if (!omitted.has(name.toLowerCase())) headers.push(name, message.rawHeaders[i + 1]!)
  }
  return headers
}

export function executorConnection(event: H3Event) {
  const { req } = event.node
  const config = useRuntimeConfig(event)
  let target: URL
  try {
    target = new URL(config.executorBaseUrl)
    if (!['http:', 'https:'].includes(target.protocol) || target.username || target.password || target.pathname !== '/' || target.search || target.hash
      || Buffer.byteLength(config.executorToken.trim()) < 32) throw new Error('Invalid executor configuration')
  } catch { throw unavailable() }
  if (!req.headers.host) throw createError({ statusCode: 400, message: 'Host header required' })
  const headers = endToEndHeaders(req)
  for (let i = headers.length - 2; i >= 0; i -= 2) {
    if (['host', 'x-executor-token', 'x-forwarded-for'].includes(headers[i]!.toLowerCase())) headers.splice(i, 2)
  }
  // 即使 Connection 声称 Host 是逐跳头，也不能退回内部地址、绕过管理入口隔离。
  headers.push('Host', req.headers.host, 'X-Executor-Token', config.executorToken.trim(), 'X-Forwarded-For',
    [req.headers['x-forwarded-for'], req.socket.remoteAddress || 'unknown'].filter(Boolean).join(', '))
  return { target, headers }
}

export function sendExecutorError(event: H3Event, error: unknown) {
  const { res } = event.node
  if (res.destroyed || res.writableEnded) return
  if (res.headersSent) { res.destroy(); return }
  const known = error as { statusCode?: number; message?: string }
  const status = [400, 405, 413, 429, 503].includes(known?.statusCode || 0) ? known.statusCode! : 503
  const message = status === 503 ? '执行服务暂不可用，请稍后重试' : known.message || '请求不受支持'
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' })
  res.end(JSON.stringify({ error: message }))
}

// 正式路由复用同一传输；只有任务受理可同键重试，管理写入不能重复执行。
export async function forwardExecutor(event: H3Event) {
  const { req, res } = event.node
  try {
    const pathname = event.path.split('?')[0]!
    const mutation = ['POST', 'PATCH'].includes(event.method)
    const submit = event.method === 'POST' && ['/api/jobs', '/api/web/jobs'].includes(pathname)
    if (!mutation && !['GET', 'HEAD'].includes(event.method)) throw createError({ statusCode: 405 })
    const { target, headers } = executorConnection(event)

    const key = req.headers['idempotency-key']
    if (submit && key !== undefined && (typeof key !== 'string' || !/^[\x21-\x7e]{1,128}$/.test(key)
      || req.rawHeaders.filter((value, i) => i % 2 === 0 && value.toLowerCase() === 'idempotency-key').length !== 1)) {
      throw createError({ statusCode: 400, message: 'Idempotency-Key 必须是单个非空可见 ASCII 字符串，最多 128 字节' })
    }
    if (mutation) {
      for (let i = headers.length - 2; i >= 0; i -= 2) {
        const name = headers[i]!.toLowerCase()
        if (name === 'content-length' || submit && name === 'idempotency-key') headers.splice(i, 2)
      }
    }
    // 键在重试循环外生成；正文与成员身份也复用。不能识别无键客户端在 Nuxt 崩溃后新发的请求。
    if (submit) headers.push('Idempotency-Key', typeof key === 'string' ? key : randomUUID())

    const claim = buffered.claim()
    let held = 0
    async function readLimited(stream: IncomingMessage) {
      // /api/jobs 原有 JSON 正文上限为 2 MiB；内部 202 回执也不允许无界缓存。
      if (Number(stream.headers['content-length']) > 2 * MiB) throw createError({ statusCode: 413, message: '请求或受理响应超过大小限制' })
      const chunks: Buffer[] = []
      let bytes = 0
      for await (const chunk of stream) {
        bytes += chunk.length
        if (bytes > 2 * MiB) throw createError({ statusCode: 413, message: '请求或受理响应超过大小限制' })
        claim.resize(held + chunk.length)
        held += chunk.length
        chunks.push(chunk)
      }
      return Buffer.concat(chunks)
    }

    const disconnected = new AbortController()
    const disconnect = () => disconnected.abort()
    res.once('close', disconnect)
    try {
      const body = mutation ? await readLimited(req) : undefined
      if (body) headers.push('Content-Length', String(body.length))
      for (let attempt = 0; ; attempt++) {
        let response: IncomingMessage | undefined
        try {
          if (res.destroyed) return
          response = await new Promise<IncomingMessage>((resolve, reject) => {
            // Node HTTP 保留原 Host、重复头和二进制；不跟随内部重定向或泄漏凭据到其他地址。
            const request = (target.protocol === 'https:' ? https : http).request(target, {
              method: event.method, path: event.path, headers,
              // 私有批量刷新按账户逐批执行，沿用执行器每账户15秒截止，不能套45秒总限。
              signal: event.method === 'POST' && pathname === '/api/admin/accounts/quota'
                ? disconnected.signal
                : AbortSignal.any([disconnected.signal, AbortSignal.timeout(45000)])
            }, resolve)
            request.once('error', reject)
            request.end(body)
          })
          if (response.headers['x-executor-auth-error'] === '1') throw unavailable()
          // 完整收到受理回执才向客户端写头，避免半份 202 阻止安全的同键重试。
          const accepted = submit && response.statusCode === 202 ? await readLimited(response) : undefined
          res.writeHead(response.statusCode || 502, executorResponseHeaders(response))
          if (accepted) res.end(accepted)
          else await pipeline(response, res)
          return
        } catch (error) {
          response?.destroy()
          const code = (error as NodeJS.ErrnoException).code
          const transportFailure = ['ECONNRESET', 'ECONNREFUSED', 'EPIPE', 'ETIMEDOUT', 'ABORT_ERR', 'ERR_STREAM_PREMATURE_CLOSE'].includes(code || '')
          if (submit && attempt === 0 && transportFailure && !disconnected.signal.aborted && !res.headersSent) {
            held = body!.length
            claim.resize(held)
            continue
          }
          if (res.headersSent) { res.destroy(); return }
          throw unavailable()
        }
      }
    } finally {
      res.off('close', disconnect)
      claim.release()
    }
  } catch (error) {
    sendExecutorError(event, error)
  }
}
