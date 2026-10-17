// @vitest-environment node
import http, { type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import type { AddressInfo } from 'node:net'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { mockNuxtImport } from '@nuxt/test-utils/runtime'
import { createError as h3CreateError, createEvent } from 'h3'
import { forwardExecutor } from '../../server/utils/executor-client'

const { config } = vi.hoisted(() => ({ config: {
  executorBaseUrl: '', executorToken: 'synthetic-executor-token-32-bytes-long'
} }))
mockNuxtImport('useRuntimeConfig', () => () => config)
mockNuxtImport('createError', () => (input: Parameters<typeof h3CreateError>[0]) => h3CreateError(input))

const servers: Server[] = []
const MiB = 1024 * 1024
type Handler = (req: IncomingMessage, res: ServerResponse) => void | Promise<void>
type Reply = { status: number, headers: IncomingMessage['headers'], body: Buffer }

afterEach(async () => {
  for (const server of servers.splice(0).reverse()) {
    server.closeAllConnections()
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()))
  }
})

async function listen(handler: Handler) {
  const server = http.createServer((req, res) => {
    Promise.resolve(handler(req, res)).catch(error => {
      if (res.destroyed || res.writableEnded) return
      if (res.headersSent) { res.destroy(); return }
      res.writeHead(error.statusCode || 500, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ error: error.message }))
    })
  })
  servers.push(server)
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`
}

async function fixture(upstream: Handler) {
  config.executorBaseUrl = await listen(upstream)
  return await listen((req, res) => forwardExecutor(createEvent(req, res)))
}

async function readBody(req: IncomingMessage) {
  const chunks: Buffer[] = []
  for await (const chunk of req) chunks.push(chunk)
  return Buffer.concat(chunks)
}

function request(base: string, path = '/api/jobs', options: {
  method?: string, headers?: string[], body?: string | Buffer
} = {}): Promise<Reply> {
  return new Promise((resolve, reject) => {
    const req = http.request(new URL(path, base), {
      method: options.method || 'POST',
      headers: ['Host', 'localhost:18082', ...(options.headers || [])],
      agent: false
    }, res => {
      readBody(res).then(body => resolve({ status: res.statusCode!, headers: res.headers, body }), reject)
    })
    req.once('error', reject)
    req.end(options.body)
  })
}

describe('executor client through loopback HTTP', () => {
  it('preserves Host, Origin, member credentials and XFF even when Connection names Host', async () => {
    let received: { url?: string, headers: IncomingMessage['headers'] } | undefined
    const base = await fixture((req, res) => {
      received = { url: req.url, headers: req.headers }
      res.writeHead(200, { 'Content-Type': 'application/octet-stream', 'Set-Cookie': ['one=1; HttpOnly', 'two=2; HttpOnly'] })
      res.end(Buffer.from([0, 255, 8]))
    })
    const result = await request(base, '/api/requests/synthetic?view=small', { method: 'GET', headers: [
      'Origin', 'http://localhost:18082', 'Cookie', 'member_session=synthetic-cookie',
      'Authorization', 'Bearer synthetic-member-key', 'Connection', 'Host, Keep-Alive',
      'Keep-Alive', 'timeout=20', 'X-Forwarded-For', '198.51.100.2, 192.0.2.10',
      'X-Executor-Token', 'untrusted-client-value'
    ] })
    expect(received!.url).toBe('/api/requests/synthetic?view=small')
    expect(received!.headers).toMatchObject({
      host: 'localhost:18082', origin: 'http://localhost:18082',
      cookie: 'member_session=synthetic-cookie', authorization: 'Bearer synthetic-member-key',
      'x-forwarded-for': '198.51.100.2, 192.0.2.10, 127.0.0.1', 'x-executor-token': config.executorToken
    })
    expect(received!.headers['keep-alive']).toBeUndefined()
    expect(result.body).toEqual(Buffer.from([0, 255, 8]))
    expect(result.headers['set-cookie']).toEqual(['one=1; HttpOnly', 'two=2; HttpOnly'])
  })

  it('retries a partial 202 once with the same generated key, body and member credentials', async () => {
    const received: { key: string | string[] | undefined, body: Buffer, cookie?: string, authorization?: string }[] = []
    const accepted = JSON.stringify({ job: { id: 'original-job', requestId: 'original-request' } })
    const base = await fixture(async (req, res) => {
      received.push({ key: req.headers['idempotency-key'], body: await readBody(req),
        cookie: req.headers.cookie, authorization: req.headers.authorization })
      if (received.length === 1) {
        res.writeHead(202, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(accepted) })
        res.write('{"job":', () => res.destroy())
      } else {
        res.writeHead(202, { 'Content-Type': 'application/json' })
        res.end(accepted)
      }
    })
    const body = '{"prompt":"synthetic request"}'
    const result = await request(base, '/api/web/jobs', { body, headers: [
      'Cookie', 'member_session=synthetic-cookie', 'Authorization', 'Bearer synthetic-member-key'
    ] })
    expect(result.status).toBe(202)
    expect(result.body.toString()).toBe(accepted)
    expect(received).toHaveLength(2)
    expect(received[0]!.key).toMatch(/^[0-9a-f-]{36}$/)
    expect(received[1]).toEqual(received[0])
    expect(received[0]!.body.toString()).toBe(body)
  })

  it('limits transport retries to one and keeps an existing operation key', async () => {
    const keys: unknown[] = []
    const base = await fixture(async (req, res) => {
      await readBody(req)
      keys.push(req.headers['idempotency-key'])
      res.destroy()
    })
    const result = await request(base, '/api/jobs', { body: '{}', headers: ['Idempotency-Key', 'existing-operation'] })
    expect(result.status).toBe(503)
    expect(result.headers['cache-control']).toBe('no-store')
    expect(JSON.parse(result.body.toString())).toEqual({ error: expect.any(String) })
    expect(keys).toEqual(['existing-operation', 'existing-operation'])
  })

  it('allows an acceptance longer than 15 seconds without retrying normal authentication work', async () => {
    let calls = 0
    const base = await fixture(async (req, res) => {
      await readBody(req)
      calls += 1
      // Existing Discord refresh + membership requests may each take up to 10 seconds.
      if (calls === 1) await new Promise(resolve => setTimeout(resolve, 16000))
      res.writeHead(202, { 'Content-Type': 'application/json' })
      res.end('{}')
    })
    const result = await request(base, '/api/jobs', { body: '{}' })
    expect(result.status).toBe(202)
    expect(calls).toBe(1)
  }, 25000)

  it('preserves business HTTP errors without retrying them', async () => {
    let calls = 0
    const base = await fixture(async (req, res) => {
      calls += 1
      await readBody(req)
      const status = Number(new URL(req.url!, 'http://synthetic.invalid').searchParams.get('status'))
      res.writeHead(status, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ error: `business-${status}` }))
    })
    const statuses = [400, 401, 403, 409, 429, 500, 503]
    for (const status of statuses) {
      const result = await request(base, `/api/jobs?status=${status}`, { body: '{}' })
      expect(result.status).toBe(status)
      expect(JSON.parse(result.body.toString())).toEqual({ error: `business-${status}` })
      expect(result.headers['x-executor-auth-error']).toBeUndefined()
    }
    expect(calls).toBe(statuses.length)
  })

  it('maps internal authentication failures to 503 without leaking the marker or secret', async () => {
    let calls = 0
    const base = await fixture(async (req, res) => {
      calls += 1
      await readBody(req)
      res.writeHead(401, { 'X-Executor-Auth-Error': '1', 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ error: config.executorToken }))
    })
    const result = await request(base, '/api/jobs', { body: '{}' })
    expect(result.status).toBe(503)
    expect(calls).toBe(1)
    expect(result.headers['cache-control']).toBe('no-store')
    expect(JSON.parse(result.body.toString())).toEqual({ error: expect.any(String) })
    expect(result.headers['x-executor-auth-error']).toBeUndefined()
    expect(result.body.toString()).not.toContain(config.executorToken)
    expect(result.body.toString()).not.toContain('X-Executor-Auth-Error')
  })

  it('rejects duplicate or invalid keys and forwards a single 128-byte key unchanged', async () => {
    const keys: unknown[] = []
    const base = await fixture(async (req, res) => {
      keys.push(req.headers['idempotency-key'])
      await readBody(req)
      res.writeHead(202, { 'Content-Type': 'application/json' })
      res.end('{}')
    })
    const invalid = ['', 'contains space', 'contains\ttab', 'é', 'x'.repeat(129)]
    for (const key of invalid) {
      const result = await request(base, '/api/jobs', { body: '{}', headers: ['Idempotency-Key', key] })
      expect(result.status).toBe(400)
      expect(result.headers['cache-control']).toBe('no-store')
      expect(JSON.parse(result.body.toString())).toEqual({ error: expect.any(String) })
    }
    expect((await request(base, '/api/jobs', { body: '{}', headers: [
      'Idempotency-Key', 'same-key', 'idempotency-key', 'same-key'
    ] })).status).toBe(400)
    expect(keys).toEqual([])
    const key = 'x'.repeat(128)
    expect((await request(base, '/api/jobs', { body: '{}', headers: ['Idempotency-Key', key] })).status).toBe(202)
    expect(keys).toEqual([key])
  })

  it('enforces the 2MiB body limit and accepts a new request after rejection', async () => {
    const sizes: number[] = []
    const base = await fixture(async (req, res) => {
      sizes.push((await readBody(req)).length)
      res.writeHead(202, { 'Content-Type': 'application/json' })
      res.end('{}')
    })
    const maximum = Buffer.alloc(2 * MiB, 'x')
    expect((await request(base, '/api/jobs', { body: maximum, headers: ['Content-Length', String(maximum.length)] })).status).toBe(202)
    const tooLarge = Buffer.alloc(2 * MiB + 1, 'x')
    expect((await request(base, '/api/jobs', { body: tooLarge, headers: ['Content-Length', String(tooLarge.length)] })).status).toBe(413)
    expect((await request(base, '/api/jobs', { body: '{}', headers: ['Content-Length', '2'] })).status).toBe(202)
    expect(sizes).toEqual([2 * MiB, 2])
  })

  it('enforces the same 2MiB limit for a chunked request', async () => {
    let calls = 0
    const base = await fixture((_req, res) => { calls += 1; res.end('{}') })
    const result = await request(base, '/api/jobs', {
      body: Buffer.alloc(2 * MiB + 1, 'x'), headers: ['Transfer-Encoding', 'chunked']
    })
    expect(result.status).toBe(413)
    expect(result.headers['cache-control']).toBe('no-store')
    expect(JSON.parse(result.body.toString())).toEqual({ error: expect.any(String) })
    expect(calls).toBe(0)
  })
})
