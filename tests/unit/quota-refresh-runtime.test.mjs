import test from 'node:test';
import assert from 'node:assert/strict';
import { withQuotaRequestDeadline } from '../../server/quota/refresh-timeout.ts';
import { fetchNovelAiAccountQuota } from '../../server/protocols/novelai-client.ts';
import { normalizeDb } from '../../server/storage/sqlite-store.ts';
import {
  createAccountQuotaRefresh,
  createAccountQuotaQuery,
  createQuotaObservationOrder,
  accountQuotaPatch,
  accountQuotaErrorResult,
  accountQuotaResult,
} from '../../server/quota/account-refresh.ts';
import { publicErrorMessage } from '../../server/http/public-errors.ts';

const quota = {
  tier: 3,
  subscriptionActive: true,
  subscriptionExpiresAt: Date.now() + 86400000,
  points: 10000,
  fixed: 10000,
  purchased: 0,
  v5UsagePercent: 80,
};
const account = (id) => ({
  id,
  token: `synthetic-${id}`,
  enabled: true,
  quotaTier: 3,
  subscriptionActive: true,
  subscriptionExpiresAt: quota.subscriptionExpiresAt,
  quotaFixed: 9000,
  quotaPurchased: 0,
  v5UsagePercent: 80,
  quotaCheckedAt: new Date(Date.now() - 700000).toISOString(),
});
const defer = () => {
  let resolve, reject;
  const promise = new Promise((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
};

test('非空非法订阅期限不能降为无期限，原刷新入口记录失败并保留有效快照', async () => {
  assert.throws(
    () =>
      accountQuotaResult(
        'one',
        { ...quota, subscriptionExpiresAt: 'malformed-expiration' },
        new Date().toISOString(),
      ),
    /期限格式/,
  );
  const f = runtime(async () => ({ ...quota, subscriptionExpiresAt: 'malformed-expiration' })),
    original = structuredClone(f.db.accounts[0]);
  const result = await f.refresh.refresh();
  assert.equal(result.failed, 1);
  assert.equal(result.ok, 0);
  assert.equal(f.db.accounts[0].subscriptionExpiresAt, original.subscriptionExpiresAt);
  assert.equal(f.db.accounts[0].quotaFixed, original.quotaFixed);
  assert.ok(f.db.accounts[0].quotaError);
  for (const expires of [
    undefined,
    null,
    0,
    '',
    new Date(Date.now() + 86400000).toISOString(),
    Date.now() + 86400000,
  ]) {
    assert.equal(
      accountQuotaResult(
        'one',
        { ...quota, subscriptionExpiresAt: expires },
        new Date().toISOString(),
      ).ok,
      true,
    );
  }
  assert.equal(
    accountQuotaResult('one', { ...quota, subscriptionExpiresAt: 0 }, new Date().toISOString())
      .subscriptionExpiresAt,
    null,
  );
  assert.equal(
    accountQuotaResult('one', { ...quota, subscriptionExpiresAt: '0' }, new Date().toISOString())
      .subscriptionExpiresAt,
    '0',
  );
});

test('观察票号按发起顺序比较，跨越 2^53 和重开高水位仍精确，不能循环复用', () => {
  const order = createQuotaObservationOrder();
  const first = order.issue('a:epoch1', '9007199254740992');
  const second = order.issue('a:epoch1');
  assert.equal(first, '9007199254740993');
  assert.equal(second, '9007199254740994');
  assert.equal(order.current('a:epoch1', first), false);
  assert.equal(order.current('a:epoch1', second), true);
  assert.equal(order.issue('a:epoch2'), '1');
  assert.equal(createQuotaObservationOrder().issue('a:epoch1', second), '9007199254740995');
  assert.throws(() => order.issue('full', '9223372036854775807'), /exhausted/);
});

test('失败观察只更新原 owner 的错误字段，保留上次有效余额与检查时间', () => {
  const previous = account('one'),
    at = new Date().toISOString();
  const patch = accountQuotaPatch(
    accountQuotaErrorResult('one', new Error('private upstream body'), at),
    at,
  );
  const next = { ...previous, ...patch };
  for (const key of [
    'quotaCheckedAt',
    'quotaFixed',
    'quotaPurchased',
    'v5UsagePercent',
    'subscriptionActive',
  ]) {
    assert.equal(next[key], previous[key], key);
  }
  assert.equal(next.quotaError.includes('private'), false);
  assert.equal(next.quotaAttemptedAt, at);
  assert.equal(next.enabled, true);
});

function runtime(fetchQuota, accounts = [account('one')], extra = {}) {
  const db = normalizeDb({ accounts, settings: {} });
  const refresh = createAccountQuotaRefresh({
    store: { readCollections: async () => structuredClone(db), update: async (fn) => fn(db) },
    fetchQuota,
    env: {},
    requestTimeoutMs: 15,
    refreshIntervalMs: 20,
    publicAccount: (value) => value,
    publicErrorMessage,
    logger: extra.console || { log() {}, error() {} },
    ...extra,
  });
  return {
    context: {
      publicErrorMessage,
      refreshEnabledAccountQuotas: refresh.refreshEnabled,
      startAccountQuotaAutoRefresh: refresh.start,
      stop: refresh.stop,
      get accountQuotaRefreshPromise() {
        return refresh.pending;
      },
    },
    db,
    refresh,
  };
}

async function within(promise, ms = 500) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((resolve, reject) => {
        timer = setTimeout(() => reject(new Error('刷新未按截止时间释放')), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

test('额度刷新遇到 bandwidth、banana 或词内 ban 时不会误停用账户', async () => {
  for (const message of [
    'bandwidth limit exceeded',
    'Invalid banana parameter',
    'BANANA is unavailable',
    'Account is unbanned',
  ]) {
    const f = runtime(async () => {
      throw new Error(message);
    });
    const result = await within(f.context.refreshEnabledAccountQuotas());
    assert.equal(result.failed, 1);
    assert.equal(f.db.accounts[0].enabled, true, message);
    assert.equal(f.db.accounts[0].quotaFixed, 9000);
    assert.equal(f.db.accounts[0].quotaError, '官方额度同步失败，请稍后重试');
    assert.equal(f.db.accounts[0].upstreamStatus, 'sync_error');
  }
});

test('明确 ban/banned 或封禁仍通过刷新链停用，公开文案不虚构禁用动作', async () => {
  for (const message of ['Account ban', 'Account BANNED.', '账号已封禁']) {
    const f = runtime(async () => {
      throw new Error(message);
    }, [{ ...account('one'), inFlight: 1, cooldownUntil: '2099-01-01T00:00:00.000Z' }]);
    const initial = f.db.accounts[0];
    // 文案函数本身没有执行账户修改，不能声称已经自动禁用。
    assert.equal(f.context.publicErrorMessage(message), '账号已封禁');
    assert.equal(initial.enabled, true);
    const result = await within(f.context.refreshEnabledAccountQuotas());
    assert.equal(result.failed, 1);
    assert.equal(initial.enabled, false, message);
    assert.equal(initial.quotaError, '账号已封禁');
    assert.equal(initial.quotaFixed, 9000);
    assert.equal(initial.inFlight, 1);
    assert.equal(initial.cooldownUntil, '2099-01-01T00:00:00.000Z');
  }
});

test('底层完全忽略取消时，本轮仍超时结束且可开始下一轮', async () => {
  let calls = 0,
    signal;
  const f = runtime(async (token, env, options) => {
    signal = options.signal;
    return ++calls === 1 ? new Promise(() => {}) : quota;
  });
  const first = await within(f.context.refreshEnabledAccountQuotas());
  assert.equal(first.failed, 1);
  assert.equal(signal.aborted, true);
  assert.equal(f.context.accountQuotaRefreshPromise, null);
  assert.equal(f.db.accounts[0].enabled, true);
  assert.equal(f.db.accounts[0].quotaFixed, 9000);
  assert.equal(f.db.accounts[0].upstreamStatus, 'sync_error');
  const second = await within(f.context.refreshEnabledAccountQuotas());
  assert.equal(second.ok, 1);
  assert.equal(calls, 2);
  assert.equal(f.db.accounts[0].quotaFixed, 10000);
  assert.equal(f.db.accounts[0].quotaError, '');
});

test('单个上游挂起不阻止其他账户更新，并发调用复用同一轮', async () => {
  let calls = 0;
  const f = runtime(
    async (token) => {
      calls++;
      return token === 'synthetic-one' ? new Promise(() => {}) : quota;
    },
    [account('one'), account('two')],
  );
  const results = await within(
    Promise.all([f.context.refreshEnabledAccountQuotas(), f.context.refreshEnabledAccountQuotas()]),
  );
  assert.equal(calls, 2);
  assert.equal(results[0].ok, 1);
  assert.equal(results[0].failed, 1);
  assert.equal(results[1].ok, 1);
  assert.equal(f.db.accounts[1].quotaFixed, 10000);
});

test('旧请求在超时后迟到成功，不能覆盖下一轮的余额', async () => {
  const late = defer();
  let calls = 0;
  const f = runtime(async () =>
    ++calls === 1 ? late.promise : { ...quota, fixed: 8000, points: 8000 },
  );
  await within(f.context.refreshEnabledAccountQuotas());
  await within(f.context.refreshEnabledAccountQuotas());
  late.resolve({ ...quota, fixed: 99999, points: 99999 });
  await new Promise((resolve) => setTimeout(resolve, 5));
  assert.equal(f.db.accounts[0].quotaFixed, 8000);
  assert.equal(f.db.accounts[0].quotaError, '');
});

test('自动调度在挂起请求超时后继续下一轮，不重复启动', async () => {
  let calls = 0;
  const completed = defer();
  const summaries = [];
  const f = runtime(async () => (++calls === 1 ? new Promise(() => {}) : quota), [account('one')], {
    console: {
      log: (value) => {
        if (!value.startsWith('[runtime]')) {
          return;
        }
        summaries.push(value);
        if (summaries.length === 2) {
          completed.resolve();
        }
      },
      error: (error) => completed.reject(error),
    },
  });
  try {
    f.context.startAccountQuotaAutoRefresh();
    f.context.startAccountQuotaAutoRefresh();
    await within(completed.promise);
    assert.equal(calls, 2);
    assert.match(summaries[0], /failed=1/);
    assert.match(summaries[1], /ok=1/);
  } finally {
    f.context.stop();
  }
});

test('正常完成或拒绝保持原结果，并清理超时计时器', async () => {
  let signal;
  assert.equal(
    await withQuotaRequestDeadline((value) => {
      signal = value;
      return 7;
    }, 10),
    7,
  );
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(signal.aborted, false);
  const error = Object.assign(new Error('synthetic rejection'), { upstreamStatus: 401 });
  await assert.rejects(
    withQuotaRequestDeadline(() => {
      throw error;
    }, 10),
    (value) => value === error,
  );
});

test('关闭期间允许本轮余额刷新结束，但不再安排下一轮', async () => {
  const late = defer();
  let calls = 0;
  const f = runtime(async () => {
    calls++;
    return late.promise;
  });
  f.context.startAccountQuotaAutoRefresh();
  f.context.stop();
  late.resolve(quota);
  await within(f.context.accountQuotaRefreshPromise);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(calls, 1);
  assert.equal(f.db.accounts[0].quotaFixed, 10000);
  await new Promise((resolve) => setTimeout(resolve, 25));
  assert.equal(calls, 1, 'stop must prevent a later automatic refresh');
  assert.equal(f.context.accountQuotaRefreshPromise, null);
});

test('底层吞掉取消并返回成功时，超时结果仍不能被改成成功', async () => {
  await assert.rejects(
    withQuotaRequestDeadline(
      (signal) =>
        new Promise((resolve) => {
          signal.addEventListener('abort', () => resolve(quota), { once: true });
        }),
      10,
    ),
    (error) => error.code === 'QUOTA_REQUEST_TIMEOUT',
  );
});

test('超时后的迟到拒绝已经被接住，不产生未处理拒绝', async () => {
  const late = defer();
  await assert.rejects(
    withQuotaRequestDeadline(() => late.promise, 10),
    (error) => error.code === 'QUOTA_REQUEST_TIMEOUT',
  );
  late.reject(new Error('synthetic late rejection'));
  await new Promise((resolve) => setTimeout(resolve, 5));
});

test('响应头完成但响应体永久挂起也有截止时间', async (t) => {
  t.mock.method(globalThis, 'fetch', async () => ({ ok: true, text: () => new Promise(() => {}) }));
  await assert.rejects(
    withQuotaRequestDeadline(
      (signal) =>
        fetchNovelAiAccountQuota(
          'synthetic',
          { NOVELAI_ACCOUNT_API_URL: 'https://offline.invalid' },
          { signal },
        ),
      15,
    ),
    (error) => error.code === 'QUOTA_REQUEST_TIMEOUT',
  );
});

test('有效 Opus 仅读取订阅，不为不可用的试用接口阻塞刷新', async (t) => {
  const calls = [];
  t.mock.method(globalThis, 'fetch', async (url) => {
    calls.push(url);
    if (url.endsWith('/ai/trial-status')) {
      return new Promise(() => {});
    }
    return Response.json({
      subscription: {
        tier: 3,
        active: true,
        expiresAt: quota.subscriptionExpiresAt,
        trainingStepsLeft: { fixedTrainingStepsLeft: 10000, purchasedTrainingSteps: 0 },
      },
    });
  });
  const result = await within(
    withQuotaRequestDeadline(
      (signal) =>
        fetchNovelAiAccountQuota(
          'synthetic',
          { NOVELAI_ACCOUNT_API_URL: 'https://offline.invalid' },
          { signal },
        ),
      50,
    ),
  );
  assert.equal(result.tier, 3);
  assert.equal(result.subscriptionActive, true);
  assert.deepEqual(calls, ['https://offline.invalid/user/data']);
  assert.equal(result.trialStatusKnown, false);
});

test('排队批次按账户实际发起查询分配顺序，而不是按整批开始时间', async () => {
  const blocked = Array.from({ length: 5 }, defer),
    calls = [];
  const f = runtime(
    async (token) => {
      calls.push(token);
      const index = Number(token.split('-').at(-1));
      if (index < 5) {
        return blocked[index].promise;
      }
      return {
        ...quota,
        fixed: calls.filter((value) => value === token).length === 1 ? 8000 : 10000,
      };
    },
    Array.from({ length: 6 }, (_, index) => account(String(index))),
    { requestTimeoutMs: 1000 },
  );
  const batch = f.refresh.refresh();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(calls.length, 5);
  await f.refresh.refresh(['5']);
  assert.equal(f.db.accounts[5].quotaFixed, 8000);
  blocked[0].resolve(quota);
  await new Promise((resolve) => setImmediate(resolve));
  for (const result of blocked.slice(1)) {
    result.resolve(quota);
  }
  const output = await batch;
  assert.equal(output.ok, 6);
  assert.equal(output.ignored, 0);
  assert.equal(f.db.accounts[5].quotaFixed, 10000);
});

test('query actual-start hook rejects local authorization before raw fetch or official failure logging', async () => {
  let calls = 0,
    logs = 0;
  const local = Object.assign(new Error('local authorization rejected'), { statusCode: 403 });
  const query = createAccountQuotaQuery({
    fetchQuota: async () => {
      calls++;
      return quota;
    },
    logger: {
      log() {
        logs++;
      },
      error() {},
    },
  });
  await assert.rejects(
    query('synthetic-private', '', 'a', () => {
      throw local;
    }),
    (error) => error === local,
  );
  assert.equal(calls, 0);
  assert.equal(logs, 0);
  assert.deepEqual(
    await query('synthetic-private', '', 'a', () => {
      assert.equal(calls, 0);
    }),
    quota,
  );
  assert.equal(calls, 1);
  assert.equal(logs, 1);
});
