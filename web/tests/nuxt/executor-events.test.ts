// @vitest-environment node
import http, { type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import type { AddressInfo } from 'node:net'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mockNuxtImport } from '@nuxt/test-utils/runtime'
import { createError as h3CreateError, createEvent } from 'h3'
import { forwardExecutorEvents } from '../../server/utils/executor-events'

const { config, retry } = vi.hoisted(() => ({
  config: { executorBaseUrl: '', executorToken: 'synthetic-executor-token-32-bytes-long' },
  retry: { milliseconds: 5, delays: [] as number[] }
}))
mockNuxtImport('useRuntimeConfig', () => () => config)
mockNuxtImport('createError', () => (input: Parameters<typeof h3CreateError>[0]) => h3CreateError(input))
vi.mock('node:timers/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:timers/promises')>()
  return { ...actual, setTimeout: (milliseconds: number, value?: unknown, options?: Parameters<typeof actual.setTimeout>[2]) => {
    retry.delays.push(milliseconds)
    return actual.setTimeout(milliseconds === 5000 ? retry.milliseconds : milliseconds, value, options)
  } }
})

const servers: Server[] = []
const handlers: Promise<unknown>[] = []
type Handler = (req: IncomingMessage, res: ServerResponse) => void | Promise<void>
const invalidation = (bootId: string, version: number) => `event: state-invalidated\ndata: ${JSON.stringify({ bootId, version, scopes: ['member'], resync: true })}\n\n`

beforeEach(() => { retry.delays.length = 0; retry.milliseconds = 5 })
afterEach(async () => {
  for (const server of servers.splice(0).reverse()) {
    server.closeAllConnections()
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()))
  }
  await Promise.all(handlers.splice(0))
  vi.restoreAllMocks()
})

async function listen(handler: Handler, highWaterMark = 64 * 1024) {
  const server = http.createServer({ highWaterMark }, (req, res) => {
    const task = Promise.resolve(handler(req, res)).catch(error => {
      if (res.destroyed || res.writableEnded) return
      if (res.headersSent) { res.destroy(); return }
      res.writeHead(500, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ error: error.message }))
    })
    handlers.push(task)
  })
  servers.push(server)
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`
}

async function fixture(upstream: Handler, forward = forwardExecutorEvents) {
  config.executorBaseUrl = await listen(upstream)
  let requests = 0, drains = 0
  const base = await listen((req, res) => {
    requests += 1
    res.on('drain', () => { drains += 1 })
    return forward(createEvent(req, res))
  }, 64)
  return { base, requests: () => requests, drains: () => drains }
}

function open(base: string) {
  return new Promise<{ response: IncomingMessage, text: () => string, ended: () => boolean }>((resolve, reject) => {
    const request = http.get(base + '/api/member/shared-events', { headers: {
      host: 'localhost:18082', origin: 'http://localhost:18082', cookie: 'nai_member_session=synthetic-member',
      authorization: 'Bearer synthetic-key', 'x-forwarded-for': '198.51.100.5', connection: 'Host',
      'x-executor-token': 'untrusted-browser-token'
    }, agent: false }, response => {
      let text = '', ended = false
      response.setEncoding('utf8')
      response.on('data', chunk => { text += chunk })
      response.on('end', () => { ended = true })
      response.on('error', () => {})
      resolve({ response, text: () => text, ended: () => ended })
    })
    request.once('error', reject)
  })
}

describe('executor status event bridge', () => {
  it('keeps one browser connection across EOF and 503, preserving authorized boot/resync and UTF-8', async () => {
    const received: IncomingMessage['headers'][] = []
    let latest: ServerResponse | undefined
    const f = await fixture(async (req, res) => {
      received.push(req.headers)
      if (received.length === 2) { res.writeHead(503); res.end('temporary failure'); return }
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store' })
      if (received.length === 1) {
        const status = Buffer.from('event: shared-status\ndata: {"label":"台北😀"}\n\n')
        const split = status.indexOf(Buffer.from('台')) + 1
        res.write(status.subarray(0, split))
        await new Promise<void>(resolve => setImmediate(resolve))
        res.write(status.subarray(split))
        res.write(invalidation('boot-before', 8))
        res.end('event: state-invalidated\ndata: {"bootId":"discard-partial')
      } else {
        latest = res
        res.write(invalidation('boot-after', 0))
      }
    })
    const browser = await open(f.base)
    await vi.waitFor(() => expect(browser.text()).toContain(invalidation('boot-after', 0)))
    expect(browser.response.statusCode).toBe(200)
    expect(browser.ended()).toBe(false)
    expect(f.requests()).toBe(1)
    expect(f.drains()).toBeGreaterThan(0)
    expect(browser.text()).toContain('"label":"台北😀"')
    expect(browser.text()).toContain(invalidation('boot-before', 8))
    expect(browser.text()).not.toContain('discard-partial')
    expect(browser.text()).not.toContain('temporary failure')
    expect(browser.text()).toContain('event: connection-status\ndata: {"state":"retrying"}\n\n')
    expect(retry.delays).toEqual([5000, 5000])
    expect(received).toHaveLength(3)
    for (const headers of received) expect(headers).toMatchObject({
      host: 'localhost:18082', origin: 'http://localhost:18082', cookie: 'nai_member_session=synthetic-member',
      authorization: 'Bearer synthetic-key', 'x-forwarded-for': '198.51.100.5, 127.0.0.1',
      'x-executor-token': config.executorToken
    })
    latest!.write('event: auth-expired\ndata: {}\n\n')
    await vi.waitFor(() => expect(browser.ended()).toBe(true))
    expect(browser.text()).toContain('event: auth-expired\ndata: {}\n\n')
    expect(received).toHaveLength(3)
  })

  it('preserves initial HTTP failures and masks only internal authentication errors', async () => {
    for (const [status, marker] of [[401, false], [403, false], [429, false], [503, false], [401, true]] as const) {
      let calls = 0
      const f = await fixture((_req, res) => {
        calls += 1
        res.writeHead(status, { 'Content-Type': 'application/json', ...(marker ? { 'X-Executor-Auth-Error': '1' } : {}) })
        res.end(JSON.stringify({ error: marker ? config.executorToken : `business-${status}` }))
      })
      const browser = await open(f.base)
      await vi.waitFor(() => expect(browser.ended()).toBe(true))
      expect(browser.response.statusCode).toBe(marker ? 503 : status)
      expect(browser.response.headers['x-executor-auth-error']).toBeUndefined()
      expect(browser.text()).not.toContain(config.executorToken)
      if (!marker) expect(JSON.parse(browser.text())).toEqual({ error: `business-${status}` })
      expect(calls).toBe(1)
    }
    expect(retry.delays).toEqual([])
  })

  it.each([401, 403])('ends an established browser stream on a subsequent member %i', async status => {
    let calls = 0
    const f = await fixture((_req, res) => {
      if (++calls === 1) {
        res.writeHead(200, { 'Content-Type': 'text/event-stream' })
        res.end(invalidation('original', 1))
      } else { res.writeHead(status); res.end('membership rejected') }
    })
    const browser = await open(f.base)
    await vi.waitFor(() => expect(browser.ended()).toBe(true))
    expect(browser.text()).toContain('event: auth-expired\ndata: {}\n\n')
    expect(browser.text()).not.toContain('membership rejected')
    expect(calls).toBe(2)
  })

  it('drops oversized incomplete frames and starts clean on the next authorized connection', async () => {
    let calls = 0
    const f = await fixture((_req, res) => {
      res.writeHead(200, { 'Content-Type': 'text/event-stream' })
      if (++calls === 1) res.write('event: shared-status\ndata: ' + 'x'.repeat(64 * 1024))
      else res.write(invalidation('after-oversize', 0))
    })
    const browser = await open(f.base)
    await vi.waitFor(() => expect(browser.text()).toContain(invalidation('after-oversize', 0)))
    expect(browser.text()).not.toContain('xxxxxxxx')
    expect(calls).toBe(2)
    browser.response.destroy()
  })

  it('cancels a pending reconnect when the browser closes', async () => {
    retry.milliseconds = 1000
    let calls = 0
    const f = await fixture((_req, res) => {
      calls += 1
      res.writeHead(200, { 'Content-Type': 'text/event-stream' })
      res.end(invalidation('before-close', 0))
    })
    const browser = await open(f.base)
    await vi.waitFor(() => expect(browser.text()).toContain('"state":"retrying"'))
    browser.response.destroy()
    await Promise.all(handlers)
    expect(calls).toBe(1)
    expect(retry.delays).toEqual([5000])
  })

  it('cancels an unfinished handshake when the browser closes', async () => {
    let calls = 0, upstreamClosed = false
    const f = await fixture((_req, res) => {
      calls += 1
      res.once('close', () => { upstreamClosed = true })
    })
    const browser = http.get(f.base + '/api/member/shared-events', { agent: false })
    browser.on('error', () => {})
    await vi.waitFor(() => expect(calls).toBe(1))
    browser.destroy()
    await vi.waitFor(() => expect(upstreamClosed).toBe(true))
    await Promise.all(handlers)
    expect(retry.delays).toEqual([])
  })

  it('normal shutdown ends active bridges with plain EOF and prevents new subscriptions', async () => {
    vi.resetModules()
    const isolated = await import('../../server/utils/executor-events')
    let calls = 0, closed = 0
    const f = await fixture((_req, res) => {
      calls += 1
      res.once('close', () => { closed += 1 })
      res.writeHead(200, { 'Content-Type': 'text/event-stream' })
      res.write(invalidation('shutdown', 1))
    }, isolated.forwardExecutorEvents)
    const browsers = await Promise.all([open(f.base), open(f.base)])
    await vi.waitFor(() => expect(browsers.every(browser => browser.text().includes(invalidation('shutdown', 1)))).toBe(true))
    isolated.closeExecutorEvents()
    isolated.closeExecutorEvents()
    await vi.waitFor(() => expect(browsers.every(browser => browser.ended())).toBe(true))
    await vi.waitFor(() => expect(closed).toBe(2))
    expect(browsers.every(browser => !browser.text().includes('auth-expired'))).toBe(true)
    const rejected = await open(f.base)
    await vi.waitFor(() => expect(rejected.ended()).toBe(true))
    expect(rejected.response.statusCode).toBe(503)
    expect(calls).toBe(2)
    expect(retry.delays).toEqual([])
  })
})
