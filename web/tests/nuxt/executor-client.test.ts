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
  vi.restoreAllMocks()
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
    const headers = ['Host', 'localhost:18082', ...(options.headers || [])]
    // agent:false still isolates sockets, but its default Connection:close can reset an upload before an early 413 arrives.
    if (!headers.some((name, index) => index % 2 === 0 && name.toLowerCase() === 'connection')) headers.push('Connection', 'keep-alive')
    const req = http.request(new URL(path, base), {
      method: options.method || 'POST',
      headers,
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

  it('forwards ordinary POST and PATCH bytes and identity without creating an operation key', async () => {
    const received: { method?: string, path?: string, body: Buffer, headers: IncomingMessage['headers'] }[] = []
    const base = await fixture(async (req, res) => {
      received.push({ method: req.method, path: req.url, body: await readBody(req), headers: req.headers })
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end('{"ok":true}')
    })
    const body = Buffer.from('{ "username": "台北", "enabled": false }\n')
    const identity = ['Origin', 'http://localhost:18082', 'Cookie', 'member_session=synthetic-cookie',
      'Authorization', 'Bearer synthetic-member-key', 'Content-Type', 'application/json']
    expect((await request(base, '/api/member/login', { method: 'POST', body, headers: identity })).status).toBe(200)
    expect((await request(base, '/api/member/owner/accounts/synthetic', {
      method: 'PATCH', body, headers: [...identity, 'Idempotency-Key', 'ordinary-write-key']
    })).status).toBe(200)
    expect(received.map(item => [item.method, item.path, item.headers['idempotency-key']])).toEqual([
      ['POST', '/api/member/login', undefined], ['PATCH', '/api/member/owner/accounts/synthetic', 'ordinary-write-key']
    ])
    for (const item of received) {
      expect(item.body).toEqual(body)
      expect(item.headers).toMatchObject({ host: 'localhost:18082', origin: 'http://localhost:18082',
        cookie: 'member_session=synthetic-cookie', authorization: 'Bearer synthetic-member-key',
        'content-type': 'application/json', 'content-length': String(body.length) })
    }
  })

  it.each(['POST', 'PATCH'])('does not retry an ordinary %s after losing its response even with an operation key', async method => {
    const received: unknown[] = []
    const base = await fixture(async (req, res) => {
      received.push([req.method, req.headers['idempotency-key'], (await readBody(req)).toString()])
      res.destroy()
    })
    const result = await request(base, '/api/member/owner/member-management', {
      method, body: '{"action":"synthetic"}', headers: ['Idempotency-Key', 'caller-operation']
    })
    expect(result.status).toBe(503)
    expect(received).toEqual([[method, 'caller-operation', '{"action":"synthetic"}']])
  })

  it('does not replay an ordinary mutation after a partial 202 response', async () => {
    let calls = 0
    const base = await fixture(async (req, res) => {
      await readBody(req)
      calls += 1
      res.writeHead(202, { 'Content-Type': 'application/json', 'Content-Length': '100' })
      res.write('{"partial":', () => res.destroy())
    })
    const result = await request(base, '/api/member/owner/member-management', {
      body: '{}', headers: ['Idempotency-Key', 'ordinary-partial-operation']
    }).then(reply => ({ reply, error: undefined }), error => ({ reply: undefined, error: error as NodeJS.ErrnoException }))
    // 若头已发出，连接被终止；尚未发头时返回503。两者都不能自动重放普通写入。
    if (result.reply) expect(result.reply.status).toBe(503)
    else expect(['ECONNRESET', 'ERR_STREAM_PREMATURE_CLOSE']).toContain(result.error!.code)
    expect(calls).toBe(1)
  })

  it('returns OAuth cookies and a 303 Location without following the redirect', async () => {
    let followed = 0, calls = 0
    const redirect = await listen((_req, res) => { followed += 1; res.end('must not follow') })
    const cookies = ['nai_oauth_state=; Max-Age=0; Path=/api/member/discord; HttpOnly; SameSite=Lax',
      'nai_member_session=synthetic; Max-Age=604800; Path=/api/member; HttpOnly; SameSite=Strict']
    const base = await fixture((_req, res) => {
      calls += 1
      res.writeHead(303, { Location: redirect + '/after-login', 'Set-Cookie': cookies })
      res.end()
    })
    const result = await request(base, '/api/member/discord/callback?code=synthetic', { method: 'GET' })
    expect(result.status).toBe(303)
    expect(result.headers.location).toBe(redirect + '/after-login')
    expect(result.headers['set-cookie']).toEqual(cookies)
    expect(calls).toBe(1)
    expect(followed).toBe(0)
  })

  it('lets only the exact admin batch POST exceed the ordinary deadline', async () => {
    const originalTimeout = AbortSignal.timeout.bind(AbortSignal)
    const deadlines = vi.spyOn(AbortSignal, 'timeout').mockImplementation(milliseconds => originalTimeout(milliseconds === 45000 ? 100 : milliseconds))
    const received: string[] = []
    const base = await fixture(async (req, res) => {
      await readBody(req)
      received.push(`${req.method} ${req.url}`)
      await new Promise(resolve => setTimeout(resolve, 250))
      if (!res.destroyed) { res.writeHead(200); res.end('{"ok":true}') }
    })
    const batch = await request(base, '/api/admin/accounts/quota?refresh=all', { body: '{}' })
    expect(batch.status).toBe(200)
    expect(deadlines).not.toHaveBeenCalled()
    expect(received).toEqual(['POST /api/admin/accounts/quota?refresh=all'])
    for (const [method, path] of [['GET', '/api/admin/accounts/quota'], ['POST', '/api/admin/accounts/quota-extra'], ['POST', '/api/member/login']]) {
      const result = await request(base, path, { method, ...(method === 'POST' ? { body: '{}' } : {}) })
      expect(result.status).toBe(503)
    }
    expect(deadlines).toHaveBeenCalledTimes(3)
    for (const [milliseconds] of deadlines.mock.calls) expect(milliseconds).toBe(45000)
  })

  it('cancels the admin batch HTTP request when its browser disconnects without retrying', async () => {
    let calls = 0, closed = false
    const base = await fixture(async (req, res) => {
      await readBody(req)
      calls += 1
      res.once('close', () => { closed = true })
    })
    const browser = http.request(new URL('/api/admin/accounts/quota', base), {
      method: 'POST', headers: { host: 'localhost:18082', 'content-type': 'application/json' }, agent: false
    })
    browser.on('error', () => {})
    browser.end('{}')
    await vi.waitFor(() => expect(calls).toBe(1))
    browser.destroy()
    await vi.waitFor(() => expect(closed).toBe(true))
    expect(calls).toBe(1)
  })
})
