import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import { brotliCompressSync } from 'node:zlib';
import { PikPakStorage } from '../../server/storage/pikpak-storage.ts';

const config = {
  url: 'https://selected.invalid/originals/',
  username: 'synthetic',
  password: 'synthetic',
};
const original = Buffer.from('synthetic original image');
const sha256 = createHash('sha256').update(original).digest('hex');
const image = { key: `images/original/${sha256}`, sha256, byteLength: original.length };

test('Pik get destroys the SDK HTTP error body and preserves the original error', async (t) => {
  const storage = new PikPakStorage(config);
  const body = new Readable({ read() {} });
  t.after(() => body.destroy());
  const error = Object.assign(new Error('synthetic HTTP failure'), {
    status: 403,
    response: { body },
  });
  const request = t.mock.method(storage.client, 'customRequest', async (filename, options) => {
    assert.equal(filename, `${sha256}.br`);
    assert.equal(options.method, 'GET');
    assert.ok(options.signal instanceof AbortSignal);
    throw error;
  });
  await assert.rejects(storage.get(image), (actual) => actual === error);
  assert.equal(request.mock.callCount(), 1);
  assert.equal(body.destroyed, true);
});

test('Pik get consumes and verifies a successful body', async (t) => {
  const storage = new PikPakStorage(config);
  const body = Readable.from([brotliCompressSync(original)]);
  t.after(() => body.destroy());
  const request = t.mock.method(storage.client, 'customRequest', async () => ({ body }));
  assert.deepEqual(await storage.get(image), original);
  assert.equal(request.mock.callCount(), 1);
  assert.equal(body.readableEnded, true);
  assert.equal(body.destroyed, true);
});

test('Pik get preserves an SDK failure without a response body', async (t) => {
  const storage = new PikPakStorage(config);
  const error = new Error('synthetic transport failure');
  t.mock.method(storage.client, 'customRequest', async () => {
    throw error;
  });
  await assert.rejects(storage.get(image), (actual) => actual === error);
});

test('Pik get preserves the SDK error when destroying its body throws', async (t) => {
  const storage = new PikPakStorage(config);
  let attempted = 0;
  const error = Object.assign(new Error('synthetic HTTP failure'), {
    response: {
      body: {
        destroy() {
          attempted++;
          throw new Error('synthetic cleanup failure');
        },
      },
    },
  });
  t.mock.method(storage.client, 'customRequest', async () => {
    throw error;
  });
  await assert.rejects(storage.get(image), (actual) => actual === error);
  assert.equal(attempted, 1);
});
