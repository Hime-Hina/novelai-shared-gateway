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

test('权限只投影五字段，版本可跳号；sessions仅促鉴权，重连总是全授权resync', () => {
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
