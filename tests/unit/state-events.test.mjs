import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { stateEventsFor, stateInvalidation, mergeStateEvents } from '../../server/state-events.js';

test('每store唯一合并器：200ms尾沿，持续提交首事件最多等一秒，版本单调', t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 1800000000000 });
  const store = { events: new EventEmitter() }, hub = stateEventsFor(store), events = [];
  t.after(() => store.events.emit('close'));
  assert.equal(stateEventsFor(store), hub); hub.subscribe(event => events.push(event));
  const emit = (collections, memberIds = [], allMembers = false) => store.events.emit('commit', { collections, memberIds, allMembers });
  emit(['accounts'], ['a']); t.mock.timers.tick(150); emit(['jobs'], ['b']);
  t.mock.timers.tick(199); assert.equal(events.length, 0); t.mock.timers.tick(1);
  assert.deepEqual(events[0].collections, ['accounts', 'jobs']); assert.deepEqual(events[0].memberIds, ['a', 'b']);
  assert.equal(events[0].version, 1); assert.equal(events[0].updatedAt, new Date(1800000000350).toISOString());
  emit(['users'], ['a']);
  for (let i = 0; i < 9; i++) { t.mock.timers.tick(100); emit(['settings'], [], true); }
  t.mock.timers.tick(99); assert.equal(events.length, 1); t.mock.timers.tick(1);
  assert.equal(events.length, 2); assert.equal(events[1].version, 2); assert.equal(events[1].allMembers, true);
  assert.equal(events[1].updatedAt, new Date(1800000001350).toISOString());
  assert.equal(events[1].bootId, events[0].bootId); assert.match(events[1].bootId, /^[a-f0-9]{24}$/);
});

test('权限只投影白名单字段，版本可跳号；sessions仅促鉴权，重连总是全授权resync', () => {
  const event = { bootId: 'synthetic-boot', version: 91, updatedAt: '2026-10-17T00:00:00.000Z',
    collections: ['users'], memberIds: ['a'], allMembers: false, token: 'secret', prompt: 'private' };
  const own = stateInvalidation(event, 'a', false);
  assert.deepEqual(Object.keys(own), ['bootId', 'version', 'scopes', 'updatedAt', 'resync']);
  assert.deepEqual(own.scopes, ['member']); assert.equal(own.version, 91); assert.equal(own.resync, false);
  assert.equal(stateInvalidation(event, 'b', false), null);
  assert.deepEqual(stateInvalidation(event, 'b', true).scopes, ['owner']);
  assert.deepEqual(stateInvalidation({ ...event, collections: ['jobs'] }, 'b', false).scopes, ['shared']);
  assert.deepEqual(stateInvalidation({ ...event, allMembers: true }, 'b', false).scopes, ['member']);
  assert.equal(stateInvalidation({ ...event, collections: ['sessions'] }, 'a', true), null);
  assert.deepEqual(stateInvalidation(event, 'b', false, true).scopes, ['shared', 'member']);
  assert.deepEqual(stateInvalidation(event, 'b', true, true).scopes, ['shared', 'member', 'owner']);
  assert.ok(!JSON.stringify(own).includes('secret')); assert.ok(!JSON.stringify(own).includes('memberIds'));
  const merged = mergeStateEvents(event, { ...event, version: 95, collections: ['images'], memberIds: ['b'] });
  assert.deepEqual(merged.collections, ['users', 'images']); assert.deepEqual(merged.memberIds, ['a', 'b']);
  assert.equal(merged.version, 95); assert.equal(merged.prompt, undefined);
});

test('合并任务日期仍按成员配对过滤，未知归属只给owner，过期日期不进入全年通知', t => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-10-17T02:00:00Z') });
  const before = { bootId: 'boot', version: 1, collections: ['jobs'], memberIds: ['alice'],
    activityChanges: [{ memberId: 'alice', date: '2026-10-15' }, { memberId: 'alice', date: '2025-10-17' }] };
  const after = { bootId: 'boot', version: 2, collections: ['jobs', 'users'], memberIds: ['bob'], allMembers: true,
    activityChanges: [{ memberId: 'bob', date: '2026-10-16' }, { memberId: null, date: '2026-10-17' },
      { memberId: 'alice', date: '2026-10-15' }] };
  const merged = mergeStateEvents(before, after);
  assert.equal(merged.activityChanges.length, 4);
  assert.deepEqual(stateInvalidation(merged, 'alice', false).activityDates, ['2026-10-15']);
  assert.deepEqual(stateInvalidation(merged, 'bob', false).activityDates, ['2026-10-16']);
  assert.equal(stateInvalidation(merged, 'charlie', false).activityDates, undefined);
  const owner = stateInvalidation(merged, 'owner', true);
  assert.deepEqual(owner.activityDates, ['2026-10-15', '2026-10-16', '2026-10-17']);
  assert.equal(owner.activityChanges, undefined); assert.equal(owner.memberIds, undefined);
});

test('100个连接只占一个store监听，回调错误隔离，退订和关库清理待发事件/连接', async t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 1800000000000 });
  const store = { events: new EventEmitter() }, hub = stateEventsFor(store);
  let delivered = 0, closed = 0;
  hub.subscribe(() => { throw new Error('synthetic subscriber error'); });
  hub.subscribe(async () => { throw new Error('synthetic async subscriber error'); });
  const detach = hub.subscribe(() => assert.fail('removed listener called'));
  detach();
  for (let i = 0; i < 100; i++) hub.subscribe(() => delivered++, () => closed++);
  assert.equal(store.events.listenerCount('commit'), 1); assert.equal(store.events.listenerCount('close'), 1);
  store.events.emit('commit', { collections: ['jobs'], memberIds: [] }); t.mock.timers.tick(200);
  await Promise.resolve(); assert.equal(delivered, 100);
  store.events.emit('commit', { collections: ['jobs'], memberIds: [] }); store.events.emit('close');
  t.mock.timers.tick(1000);
  assert.equal(delivered, 100); assert.equal(closed, 100);
  assert.equal(store.events.listenerCount('commit'), 0); assert.equal(store.events.listenerCount('close'), 0);
  const reopened = stateEventsFor(store); assert.notEqual(reopened.current().bootId, hub.current().bootId);
  store.events.emit('close');
});

test('先关闭订阅但不提前关库，晚到订阅立即结束且真正关库仍清理hub', t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 1800000000000 });
  const store = { events: new EventEmitter() }, hub = stateEventsFor(store);
  t.after(() => store.events.emit('close'));
  let delivered = 0, ended = 0, lateEnded = 0;
  hub.subscribe(() => delivered++, () => ended++);
  store.events.emit('commit', { collections: ['jobs'], memberIds: ['member'] });
  hub.closeSubscriptions(); hub.closeSubscriptions();
  assert.equal(ended, 1);
  assert.equal(store.events.listenerCount('commit'), 0);
  assert.equal(store.events.listenerCount('close'), 1, '仍需等待真正的store.close做最终清理');
  assert.equal(stateEventsFor(store), hub, '关闭过程中不得创建新的可订阅hub');
  const detach = stateEventsFor(store).subscribe(() => delivered++, () => lateEnded++);
  assert.equal(lateEnded, 1); detach();
  store.events.emit('commit', { collections: ['jobs'], memberIds: ['member'] });
  t.mock.timers.tick(1000);
  assert.equal(delivered, 0); assert.equal(hub.current().version, 0);
  store.events.emit('close');
  assert.equal(store.events.listenerCount('commit'), 0); assert.equal(store.events.listenerCount('close'), 0);
  assert.equal(ended, 1); assert.equal(lateEnded, 1);
  assert.notEqual(stateEventsFor(store).current().bootId, hub.current().bootId);
});
