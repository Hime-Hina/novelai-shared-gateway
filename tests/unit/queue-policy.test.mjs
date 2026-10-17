import test from 'node:test';
import assert from 'node:assert/strict';
import { queuePolicy, assertQueueAdmission, assertMemberAcceptsJobs, queueDeadline, queuedDeadlineTimestamp, setMemberAdmission } from '../../server/queue-policy.ts';

const now = Date.parse('2026-10-17T00:00:00Z');

test('队列初值、成员/全局边界与未激活意图计数', () => {
  assert.deepEqual(queuePolicy({}), { member: 4, global: 16, waitMs: 600000 });
  for (const value of ['0', '-1', '1.5', 'bad']) {
    assert.throws(() => queuePolicy({ QUEUE_MAX_ACTIVE_PER_MEMBER: value }), error => error.statusCode === 503);
  }
  const user = { id: 'a', token: 'a' };
  const db = { jobs: ['running', 'queued', 'pending'].map(status => ({ userToken: 'a', status })) };
  assert.doesNotThrow(() => assertQueueAdmission(db, user, undefined, {}));
  db.jobs.push({ userToken: 'a', status: 'queued' });
  assert.throws(() => assertQueueAdmission(db, user, undefined, {}), error => error.statusCode === 429);
  db.jobs[3].status = 'failed';
  db.jobs.push(...Array.from({ length: 13 }, () => ({ userToken: 'other', status: 'queued' })));
  assert.throws(() => assertQueueAdmission(db, user, undefined, {}), error => error.statusCode === 429);
  db.jobs.at(-1).status = 'done';
  assert.doesNotThrow(() => assertQueueAdmission(db, user, undefined, {}));
});

test('等待截止持久化，取原请求更早期限，已派发任务不受排队期限影响', () => {
  assert.equal(queueDeadline(now, '', {}), new Date(now + 600000).toISOString());
  const early = new Date(now + 60000).toISOString();
  assert.equal(queueDeadline(now, early, {}), early);
  const job = { status: 'queued', createdAt: new Date(now).toISOString(), queueExpiresAt: early };
  assert.equal(queuedDeadlineTimestamp(job, { QUEUE_WAIT_TIMEOUT_MS: '999999' }), now + 60000);
  assert.equal(queuedDeadlineTimestamp({ ...job, status: 'running' }, {}), 0);
  assert.equal(queuedDeadlineTimestamp({ status: 'queued', createdAt: job.createdAt }, {}), now + 600000);
});

test('停止只取消未派发任务，恢复不续旧批次、不改变登录资格', () => {
  const user = { id: 'a', token: 'a', enabled: true, memberAuth: { version: 'session-version' } };
  const db = { users: [user], jobs: ['queued', 'pending', 'running', 'done'].map(status => ({ id: status, userToken: 'a', status })) };
  db.jobs.push({ id: 'other', userToken: 'b', status: 'queued' });
  const refunded = [], refund = (_db, job) => refunded.push(job.id);
  const stopped = setMemberAdmission(db, 'a', false, refund, now);
  assert.deepEqual(stopped.cancelledJobIds, ['queued', 'pending']);
  assert.deepEqual(refunded, ['queued', 'pending']);
  assert.equal(db.jobs[2].status, 'running');
  assert.equal(db.jobs[4].status, 'queued');
  assert.equal(user.enabled, true);
  assert.equal(user.memberAuth.version, 'session-version');
  assert.throws(() => assertMemberAcceptsJobs(user, 0), error => error.statusCode === 403);
  assert.deepEqual(setMemberAdmission(db, 'a', false, refund, now).cancelledJobIds, []);
  assert.equal(user.jobAdmissionVersion, 1);
  setMemberAdmission(db, 'a', true, refund, now);
  assert.equal(db.jobs[0].status, 'failed');
  assert.throws(() => assertMemberAcceptsJobs(user, 0), error => error.statusCode === 409);
  assert.doesNotThrow(() => assertMemberAcceptsJobs(user, 1));
});
