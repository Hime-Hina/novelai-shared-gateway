import test from 'node:test';
import assert from 'node:assert/strict';
import {
  selectErrorLogJobs,
  publicErrorLog,
  errorLogRequest,
} from '../../server/reports/generation-errors.ts';
import { isNovelAiCapacityError } from '../../server/http/public-errors.ts';
import { jobDurationMs } from '../../server/generation/request-queries.ts';
import {
  publicFailureEvent,
  failureAccountingAlerts,
} from '../../server/quota/failure-accounting.ts';

const iso = (at) => new Date(at).toISOString();
const context = { maskedUserToken: null, accountRouteId: null, durationMs: null };

test('错误展示保留未知，不用更新时刻/路由步数或缺少的诊断补造原字段', () => {
  const source = { id: 'unknown', updatedAt: iso(1000), error: 'safe original message' };
  const dto = publicErrorLog(source, { steps: 30 }, context);
  for (const field of [
    'source',
    'userToken',
    'accountId',
    'accountRouteId',
    'retryCount',
    'cost',
    'accountCost',
    'queueTotal',
    'durationMs',
    'createdAt',
    'completedAt',
    'errorDetail',
  ]) {
    assert.equal(dto[field], null, field);
  }
  assert.equal(dto.request.requestedSteps, null);
  assert.equal(dto.request.routedSteps, 30);
  assert.ok(Object.values(errorLogRequest(null)).every((value) => value === null));
  assert.deepEqual(source, { id: 'unknown', updatedAt: iso(1000), error: 'safe original message' });
});

test('原先已知的零与空文本是明确值；原输入与管理员诊断只输出白名单', () => {
  const dto = publicErrorLog(
    {
      id: 'known',
      source: '',
      accountId: 'a',
      status: 'failed',
      error: 'safe error',
      errorDetail: 'private diagnostic'.repeat(500),
      retryCount: 0,
      cost: 0,
      accountCost: 0,
      queueTotal: 0,
      createdAt: iso(0),
      completedAt: iso(0),
      userToken: 'raw-secret',
      pst: 'private-pst',
    },
    { seed: 0, scale: 0, cfg: 0, prompt: '', steps: 23, requestedSteps: 30, apiKey: 'private-key' },
    { maskedUserToken: '******', accountRouteId: 0, durationMs: 0 },
  );
  for (const field of [
    'accountRouteId',
    'retryCount',
    'cost',
    'accountCost',
    'queueTotal',
    'durationMs',
  ]) {
    assert.equal(dto[field], 0, field);
  }
  assert.equal(dto.source, '');
  assert.equal(dto.request.prompt, '');
  assert.equal(dto.request.seed, 0);
  assert.equal(dto.request.requestedSteps, 30);
  assert.equal(dto.request.routedSteps, 23);
  assert.equal(dto.errorDetail.length, 4000);
  assert.equal(dto.beijingDate, '1970-01-01');
  assert.ok(!JSON.stringify(dto).includes('raw-secret'));
  assert.ok(!JSON.stringify(dto).includes('private-pst'));
  assert.ok(!JSON.stringify(dto).includes('private-key'));
  const numeric = publicErrorLog(
    {
      id: 'numeric',
      retryCount: '0',
      cost: 0.21175719010416666,
      accountCost: '1.1',
      queueTotal: '',
    },
    null,
    context,
  );
  assert.equal(numeric.retryCount, 0);
  assert.equal(numeric.cost, 0.21175719010416666);
  assert.equal(numeric.accountCost, 2);
  assert.equal(numeric.queueTotal, null);
});

test('原错误分类、截止边界及限量复用同一个选择 owner，输入顺序不被修改', () => {
  const job = {
    accountId: 'a',
    status: 'failed',
    createdAt: iso(0),
    error: 'upstream other failure',
  };
  const rows = [
    { ...job, id: 'boundary' },
    { ...job, id: 'newer', updatedAt: iso(1000) },
    { ...job, id: 'old', createdAt: iso(-1) },
    { ...job, id: 'no-time', createdAt: null },
    { ...job, id: 'not-account', accountId: null },
    { ...job, id: 'done', status: 'done' },
    ...[
      'NovelAI returned 429',
      'Concurrent generation is locked',
      'server busy',
      'direct generate timeout',
      'job deadline expired',
      'invalid STA1N',
      '密钥额度不足',
    ].map((error, index) => ({ ...job, id: String(index), error })),
  ];
  const before = structuredClone(rows);
  assert.deepEqual(
    selectErrorLogJobs(rows, 0, 10).map((row) => row.id),
    ['newer', 'boundary'],
  );
  assert.deepEqual(
    selectErrorLogJobs(rows, 0, 1).map((row) => row.id),
    ['newer'],
  );
  assert.deepEqual(rows, before);
});

test('容量状态码优先级与既有文本分类不变，时间0与未知耗时严格分开', () => {
  assert.equal(isNovelAiCapacityError({ upstreamStatus: 429, message: 'other' }), true);
  assert.equal(
    isNovelAiCapacityError({ upstreamStatus: 401, message: 'Concurrent generation is locked' }),
    false,
  );
  assert.equal(isNovelAiCapacityError({ message: 'Concurrent generation is locked' }), true);
  assert.equal(
    jobDurationMs({ createdAt: iso(0), status: 'failed', completedAt: iso(1000) }),
    1000,
  );
  assert.equal(jobDurationMs({ createdAt: iso(0), status: 'done', completedAt: iso(0) }), 0);
  assert.equal(jobDurationMs({ createdAt: iso(0), status: 'running' }, 1000), 1000);
  assert.equal(jobDurationMs({ createdAt: iso(0), status: 'failed', updatedAt: iso(1000) }), null);
  assert.equal(jobDurationMs({ createdAt: null, status: 'running' }, 1000), null);
  assert.equal(
    jobDurationMs({ createdAt: iso(1000), status: 'failed', completedAt: iso(0) }),
    null,
  );
});

const resources = () => ({ v5: 0, anlasFixed: 0, anlasPurchased: 0 });
const risk = () => ({
  last24h: { attempts: 0, unknownAmounts: 0, possible: resources() },
  last30d: { attempts: 0, unknownAmounts: 0, possible: resources() },
  aggregate: { last24h: resources(), last30d: resources() },
});
const policy = {
  version: 1,
  v5Points24h: 1,
  fixedAnlas24h: 100,
  fixedAnlas30d: 500,
  memberAttempts24h: 3,
};

test('损耗原告警 owner 不相加重叠风险；零阈值不把零风险变成告警', () => {
  const value = risk();
  value.last24h.possible.v5 = 0.6;
  value.aggregate.last24h.v5 = 0.6;
  const before = structuredClone(value);
  assert.deepEqual(failureAccountingAlerts(policy, value, []), []);
  assert.deepEqual(value, before);
  assert.deepEqual(
    failureAccountingAlerts(
      { ...policy, v5Points24h: 0, fixedAnlas24h: 0, fixedAnlas30d: 0, memberAttempts24h: 0 },
      risk(),
      [{ memberId: 'zero', memberName: 'same', attempts: 0 }],
    ),
    [],
  );
  value.aggregate.last24h.v5 = 1;
  value.last30d.unknownAmounts = 1;
  value.last30d.possible.anlasPurchased = 2;
  assert.deepEqual(
    failureAccountingAlerts(policy, value, [
      { memberId: 'm1', memberName: 'same', attempts: 3 },
    ]).map((alert) => alert.id),
    ['v5-24h', 'purchased-30d', 'amount-unknown', 'member-24h:m1'],
  );
});

test('损耗历史直接接收解析事实，白名单、未知候选和原来源不被修改', () => {
  const event = {
      id: 'loss',
      type: 'generation_loss_suspected',
      memberId: 'm1',
      actorId: null,
      amount: null,
      upstreamCandidates: null,
      privateToken: 'must-not-return',
      attempt: {
        operation: 'generate',
        model: null,
        candidateMode: 'unknown',
        upstreamCandidates: [],
        localReserved: { v5: '0', anlasFixed: '0.1', anlasPurchased: '0' },
        privateRequest: 'private',
      },
    },
    before = structuredClone(event);
  const dto = publicFailureEvent(event, null);
  assert.equal(dto.memberName, '已移除成员');
  assert.equal(dto.amount, null);
  assert.equal(dto.upstreamCandidates, null);
  assert.equal(dto.resources.anlasFixed, 0.1);
  assert.equal(dto.model, null);
  assert.ok(!JSON.stringify(dto).includes('must-not-return'));
  assert.ok(!JSON.stringify(dto).includes('privateRequest'));
  assert.deepEqual(event, before);
});

test('损耗告警在原 owner 比较精确关系金额，超 2^53/阈值超精度不先转 number，旧 number 保留', () => {
  const value = risk();
  value.last24h.possible.v5 = '9007199254740993.999999999999';
  const huge = { ...policy, v5Points24h: 9007199254740994 },
    before = structuredClone(value);
  assert.deepEqual(failureAccountingAlerts(huge, value, []), []);
  assert.deepEqual(value, before);
  value.last24h.possible.v5 = '9007199254740994';
  assert.deepEqual(
    failureAccountingAlerts(huge, value, []).map((a) => a.id),
    ['v5-24h'],
  );
  value.last24h.possible.v5 = '0.000000000001';
  assert.deepEqual(
    failureAccountingAlerts({ ...policy, v5Points24h: 1.000000000001e-12 }, value, []),
    [],
  );
  assert.deepEqual(
    failureAccountingAlerts({ ...policy, v5Points24h: 1e-13 }, value, []).map((a) => a.id),
    ['v5-24h'],
  );
  value.last24h.possible.v5 = 0.21175719010416666;
  assert.deepEqual(
    failureAccountingAlerts({ ...policy, v5Points24h: 0.21175719010416666 }, value, []).map(
      (a) => a.id,
    ),
    ['v5-24h'],
  );
  value.last24h.possible.v5 = '0.21175719010416666';
  assert.throws(() => failureAccountingAlerts(policy, value, []), /12 decimal/);
});
