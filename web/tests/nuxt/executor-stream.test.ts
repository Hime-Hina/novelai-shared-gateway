// @vitest-environment node
import http, { type ClientRequest, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import type { AddressInfo } from 'node:net'
import { once } from 'node:events'
import { createHash } from 'node:crypto'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { mockNuxtImport } from '@nuxt/test-utils/runtime'
import { createError as h3CreateError, createEvent } from 'h3'
import { forwardExecutorStream } from '../../server/utils/executor-stream'

const { config } = vi.hoisted(() => ({
  config: { executorBaseUrl: '', executorToken: 'synthetic-executor-token-32-bytes-long' }
}))
mockNuxtImport('useRuntimeConfig', () => () => config)
mockNuxtImport('createError', () => (input: Parameters<typeof h3CreateError>[0]) => h3CreateError(input))

type Handler = (req: IncomingMessage, res: ServerResponse) => void | Promise<void>
const servers: Server[] = []
const requests: ClientRequest[] = []
const handlers: Promise<unknown>[] = []
const identity = {
  host: 'localhost:18082', origin: 'http://localhost:18082', cookie: 'nai_member_session=synthetic-stream-member',
  authorization: 'Bearer synthetic-stream-key', 'x-forwarded-for': '198.51.100.5',
  connection: 'Host', 'x-executor-token': 'untrusted-browser-token'
}

function gate() {
  let resolve!: () => void
  const promise = new Promise<void>(done => { resolve = done })
  return { promise, resolve }
}

async function listen(handler: Handler) {
  const server = http.createServer((req, res) => {
    handlers.push(Promise.resolve(handler(req, res)).catch(error => {
      if (res.destroyed || res.writableEnded) return
      if (res.headersSent) { res.destroy(); return }
      res.writeHead(500, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ error: error.message }))
    }))
  })
  servers.push(server)
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`
}

async function fixture(upstream: Handler, forward = forwardExecutorStream) {
  config.executorBaseUrl = await listen(upstream)
  return listen((req, res) => forward(createEvent(req, res)))
}

function open(base: string, path = '/ai/generate-image', method = 'GET', headers: http.OutgoingHttpHeaders = {}) {
  let req!: ClientRequest
  const response = new Promise<IncomingMessage>((resolve, reject) => {
    req = http.request(base + path, { method, agent: false, headers: { ...identity, ...headers } }, res => {
      res.on('error', () => {})
      resolve(res)
    })
    req.on('error', reject)
  })
  requests.push(req)
  return { req, response }
}

async function body(res: IncomingMessage) {
  const chunks: Buffer[] = []
  for await (const chunk of res) chunks.push(Buffer.from(chunk))
  return Buffer.concat(chunks)
}

afterEach(async () => {
  for (const request of requests.splice(0)) request.destroy()
  for (const server of servers.splice(0).reverse()) {
    server.closeAllConnections()
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()))
  }
  await Promise.all(handlers.splice(0))
  vi.restoreAllMocks()
})

describe('executor opaque streaming bridge', () => {
  it('forwards a 32 MiB upload before its end and delivers response bytes before the final chunk', async () => {
    const firstUpload = gate(), finalResponse = gate()
    const uploadHash = createHash('sha256'), expectedHash = createHash('sha256')
    const chunk = Buffer.alloc(64 * 1024, 0xa5)
    Buffer.from('台北😀\0').copy(chunk)
    let uploaded = 0, calls = 0, receivedHeaders: IncomingMessage['headers'] = {}
    const first = Buffer.from([0, 255, 128, 10, 13]), last = Buffer.from('尾端😀')
    const base = await fixture(async (req, res) => {
      calls += 1
      receivedHeaders = req.headers
      for await (const part of req) {
        uploaded += part.length
        uploadHash.update(part)
        firstUpload.resolve()
      }
      res.writeHead(200, { 'Content-Type': 'application/zip', 'Set-Cookie': ['first=a; HttpOnly', 'second=b; HttpOnly'] })
      res.write(first)
      await finalResponse.promise
      res.end(last)
    })
    const browser = open(base, '/ai/generate-image', 'POST', { 'Content-Type': 'application/octet-stream', 'Content-Length': 32 * 1024 * 1024 })
    try {
      browser.req.write(chunk)
      expectedHash.update(chunk)
      // Only one chunk has been sent: a whole-body intermediary cannot satisfy this latch.
      await firstUpload.promise
      expect(uploaded).toBeGreaterThan(0)
      expect(uploaded).toBeLessThan(32 * 1024 * 1024)
      for (let i = 1; i < 512; i += 1) {
        expectedHash.update(chunk)
        if (!browser.req.write(chunk)) await once(browser.req, 'drain')
      }
      browser.req.end()
      const response = await browser.response
      const iterator = response[Symbol.asyncIterator]()
      const receivedFirst = await iterator.next()
      expect(Buffer.from(receivedFirst.value)).toEqual(first)
      expect(response.complete).toBe(false)
      finalResponse.resolve()
      const remaining: Buffer[] = []
      for await (const part of { [Symbol.asyncIterator]: () => iterator }) remaining.push(Buffer.from(part))
      expect(Buffer.concat(remaining)).toEqual(last)
      expect(response.headers['set-cookie']).toEqual(['first=a; HttpOnly', 'second=b; HttpOnly'])
      expect(uploaded).toBe(32 * 1024 * 1024)
      expect(uploadHash.digest('hex')).toBe(expectedHash.digest('hex'))
      expect(receivedHeaders).toMatchObject({
        host: identity.host, origin: identity.origin, cookie: identity.cookie, authorization: identity.authorization,
        'x-forwarded-for': '198.51.100.5, 127.0.0.1', 'x-executor-token': config.executorToken
      })
      expect(calls).toBe(1)
    } finally { finalResponse.resolve() }
  }, 15000)

  it('propagates a slow reader back to the executor instead of buffering its entire response', async () => {
    const chunk = Buffer.alloc(64 * 1024, 0x9b), total = 32 * 1024 * 1024
    let written = 0, completed = false, drains = 0
    const base = await fixture(async (_req, res) => {
      res.writeHead(200, { 'Content-Type': 'application/octet-stream', 'Content-Length': total })
      while (written < total) {
        written += chunk.length
        if (!res.write(chunk)) { drains += 1; await once(res, 'drain') }
      }
      completed = true
      res.end()
    })
    const browser = open(base)
    browser.req.end()
    const response = await browser.response
    await new Promise(resolve => setTimeout(resolve, 100))
    expect(completed).toBe(false)
    expect(written).toBeLessThan(total)
    let received = 0
    for await (const part of response) received += part.length
    expect(received).toBe(total)
    expect(drains).toBeGreaterThan(0)
  }, 15000)

  it.each([400, 401, 409, 413])('preserves an early %i while the browser still has an unfinished upload', async status => {
    let calls = 0
    const original = Buffer.from(JSON.stringify({ message: `synthetic-${status}`, statusCode: status,
      details: { synthetic: ['unchanged'] }, error: `synthetic-${status}` }, null, 2))
    const base = await fixture((_req, res) => {
      calls += 1
      // Deliberately reject without consuming a declared 32 MiB body.
      res.writeHead(status, { 'Content-Type': 'application/json' })
      res.end(original)
    })
    const browser = open(base, '/v1/ai/generate-image-stream', 'POST', { 'Content-Length': 32 * 1024 * 1024 })
    browser.req.write(Buffer.alloc(1024))
    const response = await browser.response
    expect(response.statusCode).toBe(status)
    expect(await body(response)).toEqual(original)
    expect(browser.req.writableEnded).toBe(false)
    expect(calls).toBe(1)
  })

  it('keeps long SSE bytes and split UTF-8 intact without the finite-request deadline', async () => {
    const original = AbortSignal.timeout.bind(AbortSignal)
    const timeout = vi.spyOn(AbortSignal, 'timeout').mockImplementation(ms => original(ms === 45000 ? 30 : ms))
    const bytes = Buffer.from('data: 台北😀\n\ndata: finished\n\n'), split = bytes.indexOf(Buffer.from('台')) + 1
    let calls = 0
    const base = await fixture(async (_req, res) => {
      calls += 1
      res.writeHead(200, { 'Content-Type': 'text/event-stream' })
      res.write(bytes.subarray(0, split))
      await new Promise(resolve => setTimeout(resolve, 100))
      res.end(bytes.subarray(split))
    })
    const browser = open(base, '/v1/chat/completions', 'POST')
    browser.req.end('{}')
    expect(await body(await browser.response)).toEqual(bytes)
    expect(timeout).not.toHaveBeenCalledWith(45000)
    expect(calls).toBe(1)
  })

  it('does not retry a transport failure or expose an internal authentication marker', async () => {
    const nativePaths = ['generate-image', 'generate-image-stream', 'encode-vibe', 'upscale']
      .flatMap(action => [`/ai/${action}`, `/v1/ai/${action}`])
    const paths: [string, boolean][] = [...nativePaths.map(path => [path + '?synthetic=1', true] as [string, boolean]),
      ['/v1/chat/completions', false], ['/ai/generate-image/extra', false], ['/api/jobs', false]]
    for (const internalAuthFailure of [false, true]) {
      let calls = 0
      const base = await fixture(async (req, res) => {
        calls += 1
        for await (const _chunk of req) { /* consume before dropping the response */ }
        if (!internalAuthFailure) { res.destroy(); return }
        res.writeHead(401, { 'X-Executor-Auth-Error': '1' })
        res.end(config.executorToken)
      })
      for (const [path, native] of paths) {
        const before = calls
        const browser = open(base, path, 'POST', { 'Idempotency-Key': 'synthetic-no-stream-retry' })
        browser.req.end('{}')
        const response = await browser.response, text = (await body(response)).toString()
        expect(response.statusCode).toBe(503)
        expect(response.headers['cache-control']).toBe('no-store')
        expect(response.headers['x-executor-auth-error']).toBeUndefined()
        expect(text).not.toContain(config.executorToken)
        expect(JSON.parse(text)).toEqual(native
          ? { message: expect.any(String), statusCode: 503, error: expect.any(String) }
          : { error: expect.any(String) })
        if (native) expect(JSON.parse(text).message).toBe(JSON.parse(text).error)
        expect(calls).toBe(before + 1)
      }
    }
  })

  it('handles executor configuration and missing Host failures through the Native JSON envelope', async () => {
    let calls = 0, omitHost = false
    const base = await fixture((_req, res) => { calls += 1; res.end('unexpected') }, event => {
      if (omitHost) delete event.node.req.headers.host
      return forwardExecutorStream(event)
    })
    const original = { ...config }
    try {
      for (const invalid of [{ executorBaseUrl: 'not a URL' }, { executorToken: 'short' }]) {
        Object.assign(config, original, invalid)
        const browser = open(base, '/ai/encode-vibe', 'POST'); browser.req.end('{}')
        const response = await browser.response
        expect(response.statusCode).toBe(503)
        expect(JSON.parse((await body(response)).toString())).toEqual({ message: expect.any(String), statusCode: 503, error: expect.any(String) })
      }
      Object.assign(config, original); omitHost = true
      const browser = open(base, '/v1/ai/upscale', 'POST'); browser.req.end('{}')
      const response = await browser.response
      expect(response.statusCode).toBe(400)
      expect(JSON.parse((await body(response)).toString())).toEqual({ message: 'Host header required', statusCode: 400, error: 'Host header required' })
      expect(calls).toBe(0)
    } finally { Object.assign(config, original) }
  })

  it('cancels the internal response when the browser disconnects without another executor call', async () => {
    const closed = gate()
    let calls = 0
    const base = await fixture((_req, res) => {
      calls += 1
      res.on('close', closed.resolve)
      res.writeHead(200, { 'Content-Type': 'application/octet-stream' })
      res.write('first bytes')
    })
    const browser = open(base)
    browser.req.end()
    const response = await browser.response
    await once(response, 'data')
    response.destroy()
    await closed.promise
    expect(calls).toBe(1)
  })

  it('closes SSE normally, fails incomplete binary streams and rejects new work during shutdown', async () => {
    vi.resetModules()
    const isolated = await import('../../server/utils/executor-stream')
    const pendingStarted = gate()
    let calls = 0, closed = 0
    const base = await fixture((req, res) => {
      calls += 1
      res.on('close', () => { closed += 1 })
      if (req.url === '/pending') { pendingStarted.resolve(); return }
      res.writeHead(200, req.url === '/sse' ? { 'Content-Type': 'text/event-stream' } : { 'Content-Type': 'application/octet-stream', 'Content-Length': '100' })
      res.write(req.url === '/sse' ? 'data: connected\n\n' : 'partial')
    }, isolated.forwardExecutorStream)
    const pending = open(base, '/pending'), sse = open(base, '/sse'), binary = open(base, '/binary')
    pending.req.end(); sse.req.end(); binary.req.end()
    await pendingStarted.promise
    const sseResponse = await sse.response, binaryResponse = await binary.response
    const sseBody = body(sseResponse)
    const binaryBody = body(binaryResponse).then(() => 'complete', () => 'interrupted')
    isolated.closeExecutorStreams()
    isolated.closeExecutorStreams()
    const pendingResponse = await pending.response
    expect(pendingResponse.statusCode).toBe(503)
    await body(pendingResponse)
    expect((await sseBody).toString()).toBe('data: connected\n\n')
    expect(sseResponse.complete).toBe(true)
    expect(await binaryBody).toBe('interrupted')
    expect(binaryResponse.complete).toBe(false)
    await vi.waitFor(() => expect(closed).toBe(3))
    const afterClose = open(base)
    afterClose.req.end()
    const rejected = await afterClose.response
    expect(rejected.statusCode).toBe(503)
    await body(rejected)
    expect(calls).toBe(3)
  })
})
