import test from 'node:test';
import assert from 'node:assert/strict';
import { request as httpRequest } from 'node:http';
import { EventEmitter, once } from 'node:events';
import Database from 'better-sqlite3';
import { image, databaseState, meta } from '../helpers/relational-generation.mjs';
import {
  setup,
  input as nativeInput,
  keys,
  waitFor,
  replicateMockR2,
} from '../helpers/relational-api-http.mjs';
import { createRelationalOpenAiHttp } from '../../server/generation/relational-openai-http.ts';
import { openAiGenerationError } from '../../server/protocols/openai-http.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { readRelationalArtifactContent } from '../../server/storage/relational-replica-io.ts';
import {
  authenticateMemberApiKey,
  withAuthenticatedMemberApiKey,
  createMemberSession,
  authenticateMemberSession,
} from '../../server/members/authentication.ts';
import fs from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import path from 'node:path';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { memberCookieName } from '../../server/members/http-request.ts';
import { R2Storage } from '../../server/storage/r2-storage.ts';
import { generationIdempotencyKeyHash } from '../../server/generation/admission.ts';
import { requestCacheKey } from '../../server/generation/request-fingerprint.ts';
import { parseOpenAiImageRequest } from '../../server/protocols/openai-chat.ts';
import {
  normalizeNovelAiRequest,
  DIRECT_URL_MAX_STEPS,
} from '../../server/protocols/novelai-client.ts';

const input = (root = {}, nai = {}) => ({
  model: 'nai-diffusion-4-5-full:k_euler_ancestral',
  messages: [
    {
      role: 'user',
      content:
        '提示词: synthetic private OpenAI prompt\n画师串:\n尺寸: 方图\n提示词引导值: 5\n缩放引导值: 0\n负面提示词:',
    },
  ],
  nai: { width: 128, height: 128, ...nai },
  ...root,
});
const json = (response) => JSON.parse(response.buffer.toString());
async function fixture(t, options = {}) {
  const defaults = { defaultModel: 'nai-diffusion-4-5-full', defaultNegative: '', defaults: {} };
  let defaultReads = 0;
  const f = await setup(t, {
    ...options,
    createHttp: createRelationalOpenAiHttp,
    renderError: openAiGenerationError,
    httpDependencies: {
      readNormalizationDefaults: () => {
        defaultReads++;
        return defaults;
      },
      imageUrl: (req, artifactId) => `http://${req.headers.host}/api/images/${artifactId}/content`,
      sendJson: (res, status, payload) => {
        const body = JSON.stringify(payload);
        res.writeHead(status, {
          'content-type': 'application/json; charset=utf-8',
          'cache-control': 'no-store',
          'content-length': Buffer.byteLength(body),
        });
        res.end(body);
      },
      sendStreamHeaders: (res) => {
        res.writeHead(200, {
          'content-type': 'text/event-stream; charset=utf-8',
          'cache-control': 'no-cache, no-transform',
          connection: 'keep-alive',
        });
      },
      ...options.httpDependencies,
    },
  });
  return {
    ...f,
    defaults,
    get defaultReads() {
      return defaultReads;
    },
    call: (body = input(), options = {}) =>
      f.call(body, { endpoint: '/v1/chat/completions', ...options }),
  };
}

function assertFullReplay(t, f) {
  const target = new Database(':memory:');
  t.after(() => target.close());
  initializeRelationalStorage(target, 'generation-history');
  const replayer = new RelationalCommitReplayer(target),
    source = new RelationalInputStore(f.db);
  t.mock.method(globalThis, 'fetch', () => assert.fail('full replay cannot fetch'));
  for (const { seq } of f.db
    .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
    .all()) {
    target.transaction(() => replayer.replayCommit(readRelationalCommit(f.db, seq), source))();
  }
  for (const table of [
    'generation_requests',
    'generation_inputs',
    'input_assets',
    'normalization_contexts',
    'generation_jobs',
    'generation_attempts',
    'charges',
    'wallets',
    'account_quota_state',
    'artifacts',
    'artifact_replicas',
    'idempotency_keys',
    'commits',
    'domain_events',
  ]) {
    assert.deepEqual(
      target.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
      f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).safeIntegers().all(),
      table,
    );
  }
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM api_keys').get().n, 0);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM upstream_credentials').get().n, 0);
  assert.ok(
    target
      .prepare('SELECT auth_epoch FROM members')
      .all()
      .every((m) => m.auth_epoch === null),
  );
}

test('合法ready OpenAI HTTP沿真实Key/队列/产物返回原JSON与SSE，原键不再次生成', async (t) => {
  const f = await fixture(t),
    [first, concurrent] = await Promise.all([
      f.call(input(), { key: 'same' }),
      f.call(input(), { key: 'same' }),
    ]);
  assert.equal(first.status, 200, first.buffer.toString());
  assert.equal(concurrent.status, 200, concurrent.buffer.toString());
  assert.equal(concurrent.headers.get('x-nai-request-id'), first.headers.get('x-nai-request-id'));
  assert.deepEqual(concurrent.buffer, first.buffer);
  const result = json(first),
    requestId = first.headers.get('x-nai-request-id');
  assert.equal(result.object, 'chat.completion');
  assert.equal(result.model, input().model);
  assert.deepEqual(result.usage, { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 });
  const artifact = f.db
    .prepare('SELECT artifact_id AS id FROM generation_jobs WHERE request_id=?')
    .get(requestId);
  assert.equal(
    result.choices[0].message.content,
    `![image](${f.origin}/api/images/${artifact.id}/content)`,
  );
  assert.deepEqual(
    (await readRelationalArtifactContent(f.db, artifact.id, { dataDir: f.dir })).buffer,
    image,
  );
  const before = databaseState(f.db),
    replay = await f.call(input({ stream: true }), { key: 'same' });
  assert.equal(replay.status, 200);
  assert.match(replay.headers.get('content-type'), /text\/event-stream/);
  assert.equal(replay.headers.get('x-nai-request-id'), requestId);
  const events = replay.buffer
    .toString()
    .split('\n\n')
    .filter((line) => line.startsWith('data: '))
    .map((line) => line.slice(6));
  assert.equal(events.at(-1), '[DONE]');
  const chunks = events.slice(0, -1).map(JSON.parse);
  assert.ok(chunks.every((chunk) => chunk.id === result.id && chunk.model === result.model));
  assert.deepEqual(chunks.at(-1).choices, [{ index: 0, delta: {}, finish_reason: 'stop' }]);
  assert.match(chunks.map((chunk) => chunk.choices[0].delta.content || '').join(''), /!\[image\]/);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.upstream.calls.length, 1);
  assert.equal(f.upstream.calls[0].body.parameters.steps, 28);
  assert.equal(f.upstream.calls[0].body.parameters.n_samples, 1);
  assert.equal(f.events.listenerCount('commit'), 0);
});

test('OpenAI目录与原中文标签/Medium/2MiB边界，Cookie和独立PST不能替代当前Key', async (t) => {
  const building = new Database(':memory:');
  initializeRelationalStorage(building, 'openai-building');
  assert.throws(() => createRelationalOpenAiHttp({ sqlite: building }), /incomplete/);
  building.close();
  const f = await fixture(t),
    before = databaseState(f.db);
  for (const authorization of ['', 'Bearer pst-independent', 'Bearer STA1N-unknown']) {
    const response = await fetch(f.origin + '/v1/models', { headers: { authorization } });
    assert.equal(response.status, 401);
    assert.equal((await response.json()).error.type, 'authentication_error');
  }
  const models = await fetch(f.origin + '/v1/models', {
    headers: { authorization: `Bearer ${keys.m1}` },
  });
  assert.equal(models.status, 200);
  assert.ok((await models.json()).data.some((model) => model.id === input().model));
  for (const [query, headers] of [
    [`?token=${keys.m1}`, {}],
    ['', { 'x-user-token': keys.m1 }],
  ]) {
    const catalog = await fetch(f.origin + '/v1/models' + query, { headers });
    assert.equal(catalog.status, 200);
    assert.equal((await catalog.json()).object, 'list');
    const post = await fetch(f.origin + '/v1/chat/completions' + query, {
      method: 'POST',
      headers: { 'content-type': 'application/json', ...headers },
      body: JSON.stringify(input()),
    });
    assert.equal(post.status, 401);
    assert.equal((await post.json()).error.type, 'authentication_error');
  }
  assert.equal(
    (
      await fetch(f.origin + `/v1/models?token=${keys.m1}`, {
        headers: { authorization: 'Bearer STA1N-unknown' },
      })
    ).status,
    401,
  );
  const member = await authenticateMemberApiKey(f.members, keys.m1),
    session = await createMemberSession(f.members, member, {}, f.clock);
  assert.equal(
    (await authenticateMemberSession(f.members, session.token, {}, f.clock)).user.id,
    'm1',
  );
  const state = databaseState(f.db);
  assert.equal(
    (
      await f.call(input(), {
        headers: { authorization: '', cookie: `${memberCookieName}=${session.token}` },
      })
    ).status,
    401,
  );
  for (const [body, status] of [
    [input({ messages: [{ role: 'user', content: '提示词: missing required labels' }] }), 400],
    [input({ messages: [{ role: 'user', content: 'x'.repeat(2 * 1024 * 1024) }] }), 413],
  ]) {
    const response = await f.call(body);
    assert.equal(response.status, status, response.buffer.toString());
    assert.equal(json(response).error.type, 'invalid_request_error');
  }
  assert.equal(databaseState(f.db), state);
  assert.notEqual(state, before);
  const medium = await f.call(
    input({ model: '[2K]nai-diffusion-5-full-medium:k_euler_ancestral' }),
    { key: 'medium' },
  );
  assert.equal(medium.status, 200, medium.buffer.toString());
  assert.equal(f.upstream.calls[0].body.model, 'nai-diffusion-5-full-medium');
  assert.equal(f.upstream.calls[0].body.parameters.steps, 14);
  assert.equal(f.upstream.calls[0].body.parameters.width, 1344);
  assert.equal(f.upstream.calls[0].body.parameters.height, 1344);
});

test('OpenAI原键仅用首次有限defaults和随机意图，无键默认每次新受理', async (t) => {
  const f = await fixture(t),
    body = input({ model: undefined });
  f.defaults.defaultNegative = 'first negative';
  assert.equal((await f.call(body, { key: 'context' })).status, 200);
  assert.equal(f.defaultReads, 1);
  const first = f.upstream.calls[0].body;
  f.defaults.defaultNegative = 'new negative';
  const before = databaseState(f.db);
  assert.equal((await f.call(body, { key: 'context' })).status, 200);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.defaultReads, 1);
  assert.equal(f.upstream.calls.length, 1);
  const changed = input({ model: undefined }, { scale: 7 });
  assert.equal((await f.call(changed, { key: 'context' })).status, 409);
  assert.equal(databaseState(f.db), before);
  f.advance();
  assert.equal((await f.call(body)).status, 200);
  f.advance();
  assert.equal((await f.call(body)).status, 200);
  assert.equal(f.upstream.calls.length, 3);
  assert.equal(first.parameters.negative_prompt, 'first negative');
  assert.equal(f.upstream.calls[1].body.parameters.negative_prompt, 'new negative');
  assert.equal(f.upstream.calls[2].body.parameters.negative_prompt, 'new negative');
  assert.equal(
    f.db.prepare("SELECT COUNT(*) AS n FROM generation_requests WHERE source='openai'").get().n,
    3,
  );
});

test('OpenAI最后alias SQL失败连恢复/输入/receipt全回滚，换Key原回执及完整提交回放不重发', async (t) => {
  const f = await fixture(t);
  f.db.exec(
    "CREATE TEMP TRIGGER reject_alias BEFORE INSERT ON idempotency_keys BEGIN SELECT RAISE(ABORT,'openai rollback'); END",
  );
  const before = databaseState(f.db);
  assert.equal((await f.call(input(), { key: 'rollback' })).status, 500);
  assert.equal(databaseState(f.db), before);
  f.db.exec('DROP TRIGGER reject_alias');
  assert.equal((await f.call(input(), { key: 'rollback' })).status, 200);
  const newKey = 'STA1N-openai-rotated';
  f.db.transaction(() => f.members.replaceMemberApiKey('m1', newKey))();
  const rotated = databaseState(f.db);
  assert.equal((await f.call(input(), { key: 'rollback' })).status, 401);
  assert.equal(
    (
      await f.call(input(), {
        key: 'rollback',
        headers: { authorization: `Bearer ${newKey}` },
      })
    ).status,
    200,
  );
  assert.equal(databaseState(f.db), rotated);
  assert.equal(f.upstream.calls.length, 1);
  assertFullReplay(t, f);
});

test(
  'OpenAI慢正文实际stop/resume与Keyreset须重验，已受理原键仍可领取',
  { timeout: 10000 },
  async (t) => {
    const env = { OWNER_USER_ID: 'm1' },
      chunks = new EventEmitter(),
      f = await fixture(t, { env, onBodyChunk: () => chunks.emit('read') }),
      ownerMembers = new RelationalMemberStore(f.db, env);
    assert.equal((await f.call(input(), { key: 'accepted' })).status, 200);
    const slow = async (key, operation, mutate) => {
      const reading = once(chunks, 'read'),
        text = JSON.stringify(input()),
        request = httpRequest(f.origin + '/v1/chat/completions', {
          method: 'POST',
          headers: {
            authorization: `Bearer ${key}`,
            'content-type': 'application/json',
            'content-length': Buffer.byteLength(text),
            'idempotency-key': operation,
          },
        });
      const response = new Promise((resolve, reject) => {
        request.on('response', async (res) => {
          for await (const _chunk of res) {
          }
          resolve(res.statusCode);
        });
        request.on('error', reject);
      });
      request.write(text.slice(0, 20));
      await reading;
      await mutate();
      const before = databaseState(f.db);
      request.end(text.slice(20));
      const status = await response;
      assert.equal(databaseState(f.db), before);
      return status;
    };
    assert.equal(
      await slow(keys.m1, 'stale-epoch', async () => {
        for (const acceptingJobs of [false, true]) {
          await withAuthenticatedMemberApiKey(
            ownerMembers,
            keys.m1,
            (actor) =>
              ownerMembers.changeMemberAdmission(
                meta(`openai-admission-${acceptingJobs}`, f.clock, 'm1'),
                { memberId: 'm1', acceptingJobs },
                actor,
              ),
            env,
          );
        }
      }),
      409,
    );
    assert.equal((await f.call(input(), { key: 'accepted' })).status, 200);
    assert.equal(
      await slow(keys.m1, 'reset-body', () =>
        f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'STA1N-openai-new'))(),
      ),
      401,
    );
    assert.equal(f.upstream.calls.length, 1);
  },
);

test('OpenAI实际final IO后Keyreset拒绝成功链接，JSON与已建SSE保留各自错误形状', async (t) => {
  for (const stream of [false, true]) {
    await t.test(String(stream), async (t) => {
      const f = await fixture(t),
        original = fs.open,
        newKey = 'STA1N-openai-after-io';
      let rotated = false;
      const mocked = t.mock.method(fs, 'open', async (...args) => {
        const handle = await original(...args);
        if (String(args[0]).startsWith(f.dir)) {
          const read = handle.readFile.bind(handle);
          handle.readFile = async (...readArgs) => {
            const bytes = await read(...readArgs);
            if (
              !rotated &&
              f.db
                .prepare(
                  "SELECT 1 FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id WHERE r.source='openai' AND j.status='done'",
                )
                .get()
            ) {
              rotated = true;
              f.db.transaction(() => f.members.replaceMemberApiKey('m1', newKey))();
            }
            return bytes;
          };
        }
        return handle;
      });
      syncBuiltinESMExports();
      t.after(() => {
        mocked.mock.restore();
        syncBuiltinESMExports();
      });
      const response = await f.call(input({ stream }), { key: 'io' });
      assert.equal(rotated, true);
      assert.equal(response.status, stream ? 200 : 401, response.buffer.toString());
      assert.doesNotMatch(response.buffer.toString(), /!\[image\]/);
      if (stream) {
        assert.match(response.buffer.toString(), /data: \[DONE\]/);
      } else {
        assert.equal(json(response).error.type, 'authentication_error');
      }
      const before = databaseState(f.db);
      assert.equal(
        (
          await f.call(input(), {
            key: 'io',
            headers: { authorization: `Bearer ${newKey}` },
          })
        ).status,
        200,
      );
      assert.equal(databaseState(f.db), before);
      assert.equal(f.upstream.calls.length, 1);
    });
  }
});

test('OpenAI新意图沿共享Gen owner免费和上游资格拒绝，失败不留下受理或恢复', async (t) => {
  const f = await fixture(t, { freeLimit: 1 });
  assert.equal((await f.call(input(), { key: 'free' })).status, 200);
  f.advance();
  let before = databaseState(f.db);
  assert.equal((await f.call(input(), { key: 'over-limit' })).status, 429);
  assert.equal(databaseState(f.db), before);
  f.db.prepare("UPDATE upstream_accounts SET enabled=0 WHERE id='a'").run();
  before = databaseState(f.db);
  assert.equal((await f.call(input(), { key: 'unavailable' })).status, 503);
  assert.equal(databaseState(f.db), before);
  f.db.prepare("UPDATE upstream_accounts SET enabled=1 WHERE id='a'").run();
  f.db.prepare("DELETE FROM group_members WHERE member_id='m1'").run();
  before = databaseState(f.db);
  assert.equal((await f.call(input(), { key: 'ungrouped' })).status, 403);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.upstream.calls.length, 1);
});

test('OpenAI断线只释放领取；已受理任务继续，原bytes丢失重领410且不重新生成', async (t) => {
  const f = await fixture(t),
    gate = new Promise((resolve) => {
      f.upstream.release = resolve;
    });
  f.upstream.handler = async (_req, res) => {
    await gate;
    res.writeHead(200, { 'content-type': 'image/webp' });
    res.end(image);
  };
  const controller = new AbortController(),
    pending = f.call(input(), { key: 'disconnect', signal: controller.signal });
  await waitFor(() => f.upstream.calls.length === 1);
  controller.abort();
  await assert.rejects(pending, { name: 'AbortError' });
  await waitFor(() => f.events.listenerCount('commit') === 0);
  assert.equal(
    f.db
      .prepare(
        "SELECT j.status FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id WHERE r.source='openai'",
      )
      .get().status,
    'running',
  );
  f.upstream.release();
  await waitFor(() => f.controls.size === 0);
  assert.equal((await f.call(input(), { key: 'disconnect' })).status, 200);
  const replica = f.db
    .prepare(
      "SELECT p.object_key FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id JOIN artifact_replicas p ON p.artifact_id=j.artifact_id WHERE r.source='openai'",
    )
    .get();
  await fs.unlink(path.join(f.dir, 'images', replica.object_key));
  const before = databaseState(f.db);
  assert.equal((await f.call(input(), { key: 'disconnect' })).status, 410);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.upstream.calls.length, 1);
});

test('OpenAI JSON/SSE等待截止保留原504/DONE语义，在途任务和物理输入继续持有', async (t) => {
  for (const stream of [false, true]) {
    await t.test(String(stream), async (t) => {
      const f = await fixture(t, { timeoutMs: 100 }),
        gate = new Promise((resolve) => {
          f.upstream.release = resolve;
        });
      f.upstream.handler = async (_req, res) => {
        await gate;
        res.writeHead(200, { 'content-type': 'image/webp' });
        res.end(image);
      };
      const pending = f.call(input({ stream }), { key: 'deadline' });
      await waitFor(() => f.upstream.calls.length === 1);
      f.advance(101);
      const response = await pending;
      assert.equal(response.status, stream ? 200 : 504, response.buffer.toString());
      if (stream) {
        assert.match(response.buffer.toString(), /连接超时/);
        assert.match(response.buffer.toString(), /data: \[DONE\]/);
      } else {
        assert.equal(json(response).error.type, 'server_error');
      }
      assert.doesNotMatch(response.buffer.toString(), /!\[image\]/);
      assert.equal(f.controls.size, 1);
      assert.ok(f.budget.activeBytes() > 0);
      f.upstream.release();
      await waitFor(() => f.controls.size === 0);
      assert.equal((await f.call(input(), { key: 'deadline' })).status, 200);
      assert.equal(f.upstream.calls.length, 1);
    });
  }
});

test('OpenAI无键显式nocache=0复用完整产物，新键仍保存独立原回执且不再执行', async (t) => {
  const f = await fixture(t, { freeLimit: 1 }),
    body = input({}, { nocache: '0' }),
    first = await f.call(body);
  assert.equal(first.status, 200, first.buffer.toString());
  const attempts = f.db.prepare('SELECT COUNT(*) AS n FROM generation_attempts').get().n;
  f.advance();
  const cached = await f.call(body);
  assert.equal(cached.status, 200, cached.buffer.toString());
  assert.equal(f.upstream.calls.length, 1);
  assert.notEqual(cached.headers.get('x-nai-request-id'), first.headers.get('x-nai-request-id'));
  assert.equal(json(cached).choices[0].message.content, json(first).choices[0].message.content);
  assert.equal(f.db.prepare('SELECT COUNT(*) AS n FROM generation_attempts').get().n, attempts);
  const aliased = await f.call(body, { key: 'cached-alias' });
  assert.equal(aliased.status, 200, aliased.buffer.toString());
  const before = databaseState(f.db),
    replay = await f.call(input({ stream: true }, { nocache: '0' }), { key: 'cached-alias' });
  assert.equal(replay.status, 200);
  assert.equal(replay.headers.get('x-nai-request-id'), aliased.headers.get('x-nai-request-id'));
  assert.equal(databaseState(f.db), before);
  assert.equal(f.upstream.calls.length, 1);
  assert.equal(
    f.db
      .prepare(
        "SELECT COUNT(*) AS n FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id WHERE r.source='openai' AND j.free_succeeded_at IS NOT NULL",
      )
      .get().n,
    1,
  );
  assertFullReplay(t, f);
});

test('OpenAI显式缓存允许无键和新键共用活动任务，alias保存来访首次context并完整回放', async (t) => {
  const f = await fixture(t),
    body = input({ nocache: '1' }, { nocache: '0', negative: 'explicit negative' }),
    gate = new Promise((resolve) => {
      f.upstream.release = resolve;
    });
  f.upstream.handler = async (_req, res) => {
    await gate;
    res.writeHead(200, { 'content-type': 'image/webp' });
    res.end(image);
  };
  const source = f.call(body, { key: 'active-source' });
  void source.catch(() => {});
  await waitFor(() => f.upstream.calls.length === 1);
  f.defaults.defaultArtist = 'unused visitor context';
  const noKey = f.call(body),
    aliased = f.call(body, { key: 'active-alias' }),
    completed = Promise.all([source, noKey, aliased]);
  void completed.catch(() => {});
  try {
    await waitFor(() => f.db.prepare('SELECT COUNT(*) AS n FROM idempotency_keys').get().n === 2);
    assert.equal(
      f.db.prepare("SELECT COUNT(*) AS n FROM generation_requests WHERE source='openai'").get().n,
      1,
    );
    assert.equal(f.defaultReads, 3);
    assert.equal(
      f.inputs.readGenerationIdempotencyContext(
        'm1',
        'generate',
        generationIdempotencyKeyHash('active-alias'),
      ).defaults.defaultArtist,
      'unused visitor context',
    );
  } finally {
    f.upstream.release();
  }
  const [first, shared, alias] = await completed;
  for (const response of [first, shared, alias]) {
    assert.equal(response.status, 200, response.buffer.toString());
    assert.equal(response.headers.get('x-nai-request-id'), first.headers.get('x-nai-request-id'));
    assert.equal(json(response).id, json(first).id);
  }
  const before = databaseState(f.db);
  f.defaults.defaultArtist = 'later current default';
  assert.equal((await f.call(body, { key: 'active-alias' })).status, 200);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.defaultReads, 3);
  assert.equal(f.upstream.calls.length, 1);
  assertFullReplay(t, f);
});

test('OpenAI缓存新回执最后SQL失败全回滚，资格stat后换Key不能用旧凭据提交', async (t) => {
  const f = await fixture(t),
    body = input({}, { nocache: '0' });
  assert.equal((await f.call(body, { key: 'cached-source' })).status, 200);
  f.advance();
  f.db.exec(
    "CREATE TEMP TRIGGER reject_cached_alias BEFORE INSERT ON idempotency_keys BEGIN SELECT RAISE(ABORT,'cached alias rollback'); END",
  );
  const before = databaseState(f.db);
  assert.equal((await f.call(body, { key: 'cached-rollback' })).status, 500);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.upstream.calls.length, 1);
  f.db.exec('DROP TRIGGER reject_cached_alias');
  assert.equal((await f.call(body, { key: 'cached-rollback' })).status, 200);
  assert.equal(f.upstream.calls.length, 1);
  const file = f.db
    .prepare(
      "SELECT p.object_key FROM artifacts a JOIN artifact_replicas p ON p.artifact_id=a.id JOIN storage_targets s ON s.id=p.storage_target_id WHERE s.kind='local' AND a.id IN (SELECT j.artifact_id FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id WHERE r.source='openai') LIMIT 1",
    )
    .get();
  const originalStat = fs.lstat,
    currentKey = 'STA1N-openai-cache-after-stat';
  let rotatedState;
  const mocked = t.mock.method(fs, 'lstat', async (...args) => {
    const stat = await originalStat(...args);
    if (!rotatedState && String(args[0]) === path.join(f.dir, 'images', file.object_key)) {
      f.db.transaction(() => f.members.replaceMemberApiKey('m1', currentKey))();
      rotatedState = databaseState(f.db);
    }
    return stat;
  });
  syncBuiltinESMExports();
  t.after(() => {
    mocked.mock.restore();
    syncBuiltinESMExports();
  });
  const revoked = await f.call(body, { key: 'cached-after-stat' });
  assert.ok(rotatedState, '实际缓存资格stat必须先执行');
  assert.equal(revoked.status, 401, revoked.buffer.toString());
  assert.equal(databaseState(f.db), rotatedState);
  assert.equal(f.upstream.calls.length, 1);
  assert.equal(
    (
      await f.call(body, {
        key: 'cached-after-stat',
        headers: { authorization: `Bearer ${currentKey}` },
      })
    ).status,
    200,
  );
  assert.equal(f.upstream.calls.length, 1);
  assertFullReplay(t, f);
});

test('OpenAI原键的云承诺与实际领取分开，缺配置/GET失败保留503或已建SSE的DONE', async (t) => {
  const f = await fixture(t),
    first = await f.call(input(), { key: 'cloud' });
  assert.equal(first.status, 200, first.buffer.toString());
  const requestId = first.headers.get('x-nai-request-id'),
    artifact = f.db
      .prepare('SELECT artifact_id AS id FROM generation_jobs WHERE request_id=?')
      .get(requestId),
    cloud = await replicateMockR2(t, f, artifact.id),
    file = path.join(f.dir, `images/${artifact.id}.webp`);
  await fs.rename(file, `${file}.saved`);
  assert.deepEqual(cloud.calls, ['PUT', 'GET']);
  const before = databaseState(f.db);
  delete f.replicas.r2;
  const missing = await f.call(input(), { key: 'cloud' });
  assert.equal(missing.status, 503, missing.buffer.toString());
  assert.equal(missing.headers.get('x-nai-request-id'), requestId);
  const stream = await f.call(input({ stream: true }), { key: 'cloud' });
  assert.equal(stream.status, 200);
  assert.doesNotMatch(stream.buffer.toString(), /!\[image\]/);
  assert.match(stream.buffer.toString(), /data: \[DONE\]/);
  assert.equal(stream.headers.get('x-nai-request-id'), requestId);
  assert.deepEqual(cloud.calls, ['PUT', 'GET']);
  f.replicas.r2 = new R2Storage({ ...cloud.r2.config, bucket: 'wrong-openai-images' });
  assert.equal((await f.call(input(), { key: 'cloud' })).status, 503);
  assert.deepEqual(cloud.calls, ['PUT', 'GET']);
  f.replicas.r2 = cloud.r2;
  cloud.controls.failed = true;
  assert.equal((await f.call(input(), { key: 'cloud' })).status, 503);
  assert.deepEqual(cloud.calls, ['PUT', 'GET', 'GET']);
  cloud.controls.failed = false;
  const recovered = await f.call(input(), { key: 'cloud' });
  assert.equal(recovered.status, 200, recovered.buffer.toString());
  assert.equal(json(recovered).choices[0].message.content, json(first).choices[0].message.content);
  assert.equal(recovered.headers.get('x-nai-request-id'), requestId);
  assert.deepEqual(cloud.calls, ['PUT', 'GET', 'GET', 'GET']);
  assert.equal(databaseState(f.db), before);
  assert.equal(f.upstream.calls.length, 1);
});

test('OpenAI显式缓存保留原normalized尺寸标签，不扩大为仅wire等价', async (t) => {
  const f = await fixture(t),
    square = input({}, { nocache: '0' }),
    landscape = structuredClone(square);
  landscape.messages[0].content = landscape.messages[0].content.replace('尺寸: 方图', '尺寸: 横图');
  const legacyKey = (body) =>
    requestCacheKey(
      'm1',
      normalizeNovelAiRequest(parseOpenAiImageRequest(body, f.defaults).request, f.defaults, {
        maxSteps: DIRECT_URL_MAX_STEPS,
      }),
    );
  assert.notEqual(legacyKey(square), legacyKey(landscape));
  assert.equal((await f.call(square)).status, 200);
  f.advance();
  assert.equal((await f.call(landscape)).status, 200);
  assert.equal(f.upstream.calls.length, 2);
  assert.equal(f.upstream.calls[0].body.parameters.width, 128);
  assert.equal(f.upstream.calls[1].body.parameters.width, 128);
  assert.equal(f.upstream.calls[0].body.parameters.height, 128);
  assert.equal(f.upstream.calls[1].body.parameters.height, 128);
});

test('OpenAI目录在Discord实际IO后重核原Key与ready，成员仍active不能替代凭据', async (t) => {
  for (const change of ['rotate', 'ready']) {
    await t.test(change, async (t) => {
      const env = {
        DISCORD_CLIENT_ID: '123456789012345678',
        DISCORD_CLIENT_SECRET: 'synthetic-discord-private-secret',
        DISCORD_GUILD_ID: '1134557553011998840',
        PUBLIC_BASE_URL: 'https://synthetic.example',
      };
      let entered, release;
      const waiting = new Promise((resolve) => {
          entered = resolve;
        }),
        barrier = new Promise((resolve) => {
          release = resolve;
        });
      t.after(() => release());
      const f = await fixture(t, {
        env,
        httpDependencies: {
          async discordFetch(url) {
            assert.equal(f.db.inTransaction, false);
            assert.match(String(url), /users\/@me\/guilds\/.+\/member$/);
            entered();
            await barrier;
            return Response.json({ user: { id: '123456789012345679' } });
          },
        },
      });
      await f.members.discordAuthenticationTransaction((tx) => {
        const member = tx.readMember('m1');
        member.discord = {
          id: '123456789012345679',
          guildId: env.DISCORD_GUILD_ID,
          accessToken: 'synthetic-private-discord-access',
          refreshToken: 'synthetic-private-discord-refresh',
          expiresAt: Date.now() + 86400000,
          checkedAt: 0,
          member: true,
        };
        tx.saveAuthentication(member);
      });
      const pending = fetch(f.origin + '/v1/models', {
        headers: { authorization: `Bearer ${keys.m1}` },
      });
      await waiting;
      if (change === 'rotate') {
        f.db.transaction(() => f.members.replaceMemberApiKey('m1', 'STA1N-openai-current'))();
      } else {
        f.db.prepare("UPDATE projection_state SET status='building'").run();
      }
      release();
      const response = await pending;
      assert.equal(response.status, change === 'rotate' ? 401 : 500, await response.text());
      assert.equal(f.upstream.calls.length, 0);
      if (change === 'rotate') {
        const before = databaseState(f.db),
          healthy = await fetch(f.origin + '/v1/models', {
            headers: { authorization: 'Bearer STA1N-openai-current' },
          });
        assert.equal(healthy.status, 200);
        await healthy.arrayBuffer();
        assert.equal(databaseState(f.db), before);
      }
    });
  }
});

test('共同API受理在实际正文IO撤下ready后拒绝，Native/OpenAI都零业务提交', async (t) => {
  for (const protocol of ['openai', 'native']) {
    await t.test(protocol, { timeout: 10000 }, async (t) => {
      let f, before;
      const options = {
        onBodyChunk() {
          if (!before) {
            f.db.prepare("UPDATE projection_state SET status='building'").run();
            before = databaseState(f.db);
          }
        },
      };
      f = await (protocol === 'openai' ? fixture(t, options) : setup(t, options));
      const controller = new AbortController(),
        committed = new Promise((resolve) =>
          f.events.once('commit', () => resolve({ kind: 'commit' })),
        ),
        pending = f.call(protocol === 'openai' ? input() : nativeInput(), {
          key: 'withdrawn-body-ready',
          signal: controller.signal,
        });
      const outcome = await Promise.race([
        pending.then((response) => ({ kind: 'response', response })),
        committed,
      ]);
      controller.abort();
      await pending.catch(() => {});
      assert.ok(before);
      assert.equal(outcome.kind, 'response', '撤下ready后不能提交新的受理');
      assert.equal(outcome.response.status, 500);
      assert.equal(databaseState(f.db), before);
      assert.equal(f.upstream.calls.length, 0);
    });
  }
});

test(
  '共同API缓存资格实际stat后撤下ready，重入当前Key事务不能追加cached回执',
  { timeout: 10000 },
  async (t) => {
    const f = await fixture(t),
      body = input({}, { nocache: '0' });
    assert.equal((await f.call(body, { key: 'ready-cache-source' })).status, 200);
    const file = f.db
      .prepare(
        "SELECT p.object_key FROM artifact_replicas p JOIN generation_jobs j ON j.artifact_id=p.artifact_id JOIN generation_requests r ON r.id=j.request_id JOIN storage_targets s ON s.id=p.storage_target_id WHERE r.source='openai' AND s.kind='local'",
      )
      .get();
    let before;
    const originalStat = fs.lstat;
    const mocked = t.mock.method(fs, 'lstat', async (...args) => {
      const stat = await originalStat(...args);
      if (!before && String(args[0]) === path.join(f.dir, 'images', file.object_key)) {
        f.db.prepare("UPDATE projection_state SET status='building'").run();
        before = databaseState(f.db);
      }
      return stat;
    });
    syncBuiltinESMExports();
    t.after(() => {
      mocked.mock.restore();
      syncBuiltinESMExports();
    });
    const controller = new AbortController(),
      committed = new Promise((resolve) =>
        f.events.once('commit', () => resolve({ kind: 'commit' })),
      ),
      pending = f.call(body, { key: 'withdrawn-cache-ready', signal: controller.signal });
    const outcome = await Promise.race([
      pending.then((response) => ({ kind: 'response', response })),
      committed,
    ]);
    controller.abort();
    await pending.catch(() => {});
    assert.ok(before, '真实缓存资格stat必须先执行');
    assert.equal(outcome.kind, 'response');
    assert.equal(outcome.response.status, 500);
    assert.equal(databaseState(f.db), before);
    assert.equal(f.upstream.calls.length, 1);
  },
);

test('共同API实际读取原文件后撤下ready，不交付Native原bytes或OpenAI成功链接', async (t) => {
  for (const protocol of ['openai', 'native']) {
    await t.test(protocol, async (t) => {
      const f = await (protocol === 'openai' ? fixture(t) : setup(t)),
        body = protocol === 'openai' ? input() : nativeInput();
      assert.equal((await f.call(body, { key: 'ready-io' })).status, 200);
      let before;
      const originalOpen = fs.open;
      const mocked = t.mock.method(fs, 'open', async (...args) => {
        const handle = await originalOpen(...args);
        if (String(args[0]).startsWith(path.join(f.dir, 'images') + path.sep)) {
          const read = handle.readFile.bind(handle);
          handle.readFile = async (...values) => {
            const bytes = await read(...values);
            if (!before) {
              f.db.prepare("UPDATE projection_state SET status='building'").run();
              before = databaseState(f.db);
            }
            return bytes;
          };
        }
        return handle;
      });
      syncBuiltinESMExports();
      t.after(() => {
        mocked.mock.restore();
        syncBuiltinESMExports();
      });
      const response = await f.call(body, { key: 'ready-io' });
      assert.ok(before);
      assert.equal(response.status, 500, response.buffer.toString());
      assert.doesNotMatch(response.buffer.toString(), /!\[image\]/);
      assert.notDeepEqual(response.buffer, image);
      assert.equal(databaseState(f.db), before);
      assert.equal(f.upstream.calls.length, 1);
    });
  }
});
