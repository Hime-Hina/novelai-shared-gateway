import assert from 'node:assert/strict';
import test from 'node:test';
import { Readable } from 'node:stream';
import { protectExecutorHandler } from '../../executor-auth.mjs';

const token = 'a'.repeat(32);

function request(value, rawTokens = value === undefined ? [] : [value]) {
  const req = Readable.from([Buffer.from('body remains unread')]);
  req.headers = {
    host: 'localhost:18082',
    origin: 'http://localhost:18082',
    cookie: 'member_session=existing-session',
    authorization: 'Bearer existing-member-key',
    ...(value === undefined ? {} : { 'x-executor-token': value })
  };
  req.rawHeaders = ['Host', req.headers.host, ...rawTokens.flatMap(value => ['X-Executor-Token', value])];
  return req;
}

function response() {
  return {
    status: null,
    headers: null,
    body: null,
    writeHead(status, headers) { this.status = status; this.headers = headers; },
    end(body) { this.body = body; }
  };
}

test('executor rejects missing or short configured credentials before handling requests', () => {
  for (const value of [undefined, '', 'a'.repeat(31)]) {
    assert.throws(() => protectExecutorHandler(() => {}, value), /at least 32 bytes/);
  }
});

test('executor rejects missing, wrong, or duplicate credentials without reaching the business handler', () => {
  let calls = 0;
  const guarded = protectExecutorHandler(() => { calls += 1; }, token);
  for (const req of [request(undefined), request('x'), request('b'.repeat(32)), request(token, [token, token])]) {
    const res = response();
    guarded(req, res);
    assert.equal(res.status, 401);
    assert.equal(res.headers['Cache-Control'], 'no-store');
    assert.equal(res.headers['X-Executor-Auth-Error'], '1');
    assert.equal(req.headers['x-executor-token'], undefined);
    assert.equal(req.rawHeaders.some(value => value === 'X-Executor-Token'), false);
    assert.equal(req.readableFlowing, null);
    assert.equal(res.body.includes(token), false);
  }
  assert.equal(calls, 0);
});

test('executor preserves the original request, response, member identity, and unread body', async () => {
  const req = request(token);
  const res = response();
  const guarded = protectExecutorHandler(async (actualRequest, actualResponse) => {
    assert.equal(actualRequest, req);
    assert.equal(actualResponse, res);
    assert.equal(req.readableFlowing, null);
    assert.deepEqual(req.headers, {
      host: 'localhost:18082',
      origin: 'http://localhost:18082',
      cookie: 'member_session=existing-session',
      authorization: 'Bearer existing-member-key'
    });
    assert.deepEqual(req.rawHeaders, ['Host', 'localhost:18082']);
    let body = '';
    for await (const chunk of req) body += chunk.toString();
    assert.equal(body, 'body remains unread');
    res.writeHead(202, { 'Content-Type': 'application/octet-stream' });
    res.end(Buffer.from([1, 2, 3]));
  }, token);
  await guarded(req, res);
  assert.equal(res.status, 202);
  assert.equal(res.headers['X-Executor-Auth-Error'], undefined);
  assert.deepEqual(res.body, Buffer.from([1, 2, 3]));
});
