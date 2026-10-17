import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';

const source = (await readFile(new URL('../../public/member-events.js', import.meta.url), 'utf8')
  + (await readFile(new URL('../../public/shared-status.js', import.meta.url), 'utf8')).replace(/^import[^\n]+\n/, '')).replaceAll('export function ', 'function ');
const NOW = 1800000000000;
function page(fetchStatus) {
  let now = NOW, nextTimer = 0;
  const elements = new Map(), timers = new Map(), retries = new Map(), documentEvents = new Map(), windowEvents = new Map(), streams = [], emitted = [], requests = [];
  const add=(map,name,fn)=>map.set(name,[...(map.get(name)||[]),fn]);
  const get = id => { if (!elements.has(id)) elements.set(id, { textContent: '', hidden: false, after() {} }); return elements.get(id); };
  class ClockDate extends Date {
    constructor(...args) { super(...(args.length ? args : [now])); }
    static now() { return now; }
  }
  class Stream {
    static CLOSED = 2;
    constructor(url) { this.url = url; this.listeners = new Map(); streams.push(this); }
    addEventListener(name, fn) { this.listeners.set(name, fn); }
    close() { this.closed = true; }
    emit(name, value) { this.listeners.get(name)?.({ data: JSON.stringify(value) }); }
  }
  const document = { hidden: false, getElementById: get, createElement: () => ({}), addEventListener: (name, fn) => add(documentEvents,name,fn) };
  const context = vm.createContext({ document, window: { addEventListener: (name, fn) => add(windowEvents,name,fn), dispatchEvent: event => emitted.push(event.type) }, Date: ClockDate, Event,
    EventSource: Stream, setInterval(fn, delay) { assert.equal(delay, 1000); const id = ++nextTimer; timers.set(id, fn); return id; },
    setTimeout(fn,delay){assert.equal(delay,5000);const id=++nextTimer;retries.set(id,fn);return id;},clearTimeout:id=>retries.delete(id),
    clearInterval: id => timers.delete(id), async fetch(url) { requests.push(url); if(!fetchStatus)assert.fail('倒计时不得额外请求'); return { status: await fetchStatus() }; } });
  vm.runInContext(source + '\nglobalThis.ui = {startSharedStatus,stopSharedStatus};', context);
  return { get, timers, retries, streams, emitted, requests, ui: context.ui,
    advance(ms) { now += ms; for (const fn of [...timers.values()]) fn(); },
    visible(visible) { document.hidden = !visible; for(const fn of documentEvents.get('visibilitychange'))fn(); },
    event(name) { for(const fn of windowEvents.get(name))fn(); }
  };
}
const state = (reason = 'ordinary_failure', overrides = {}) => ({ status: 'cooldown', cooldownReason: reason,
  cooldownUntil: new Date(NOW + 60000).toISOString(), subscriptionActive: true, expiresAt: new Date(NOW + 86400000).toISOString(),
  running: 0, queued: 2, pending: 3, concurrencyLimit: 1, officialCheckedAt: new Date(NOW).toISOString(), ...overrides });

test('SSE截止在本地倒计时，归零仅等待更新，不推断已派发或额外请求', () => {
  const p = page(); p.ui.startSharedStatus();
  assert.equal(p.streams[0].url, '/api/member/shared-events');
  p.streams[0].emit('shared-status', state());
  assert.equal(p.get('sharedQueue').textContent, '生成中 0 / 1 · 排队 2 · 批次待激活 3');
  assert.match(p.get('sharedCooldown').textContent, /生成失败.*剩余 1 分 0 秒/);
  assert.equal(p.timers.size, 1);
  p.advance(1000); assert.match(p.get('sharedCooldown').textContent, /剩余 59 秒/);
  p.advance(59000);
  assert.equal(p.get('sharedCooldown').textContent, '等待期已结束，等待状态更新');
  assert.equal(p.get('sharedState').textContent, '等待派发');
  assert.equal(p.timers.size, 0); assert.equal(p.streams.length, 1);
  p.streams[0].emit('shared-status', state(null, { status: 'active', cooldownUntil: null }));
  assert.equal(p.get('sharedState').textContent, '可用'); assert.equal(p.get('sharedCooldown').textContent, '当前无冷却');
});

test('原因清晰区分，未知原因不猜容量限流，优先状态保持不变', () => {
  const p = page(); p.ui.startSharedStatus();
  for (const [reason, text] of [['success_gap', '上一张已完成'], ['ordinary_failure', '生成失败'], ['capacity_limit', '上游并发或容量限制'],
    ['upstream_retry_after', '按上游要求'], ['completion_gap', '任务已结束'], [null, '共享账户等待派发'], ['constructor', '共享账户等待派发']]) {
    p.streams[0].emit('shared-status', state(reason));
    assert.ok(p.get('sharedCooldown').textContent.includes(text)); assert.equal(p.timers.size, 1);
  }
  p.streams[0].emit('shared-status', state('capacity_limit', { status: 'stale' }));
  p.advance(1000); assert.equal(p.get('sharedState').textContent, '余额已过期');
  p.streams[0].emit('shared-status', state('success_gap', { status: 'inactive_opus' }));
  assert.equal(p.get('sharedState').textContent, '订阅不可用');
});

test('隐藏、停止和离开页面清理计时器，重新显示不重复连接且忽略旧连接事件', () => {
  const p = page(); p.ui.startSharedStatus(); p.ui.startSharedStatus();
  assert.equal(p.streams.length, 1);
  p.streams[0].emit('shared-status', state());
  p.visible(false); assert.equal(p.timers.size, 0); assert.equal(p.streams[0].closed, true);
  p.advance(10000); p.visible(true);
  assert.equal(p.streams.length, 2); assert.equal(p.timers.size, 1);
  assert.match(p.get('sharedCooldown').textContent, /剩余 50 秒/);
  p.streams[0].emit('shared-status', state('capacity_limit'));
  assert.match(p.get('sharedCooldown').textContent, /生成失败/);
  p.event('pagehide'); assert.equal(p.timers.size, 0); assert.equal(p.streams[1].closed, true);
  p.event('pageshow'); assert.equal(p.streams.length, 3); assert.equal(p.timers.size, 1);
  p.ui.stopSharedStatus(); assert.equal(p.timers.size, 0); assert.equal(p.streams[2].closed, true);
  p.streams[2].emit('shared-status', state()); assert.equal(p.timers.size, 0);
  p.visible(false); p.visible(true); assert.equal(p.streams.length, 3);
});

test('登录过期清理倒计时，新登录等新快照；更长上游截止覆盖原倒计时', () => {
  const p = page(); p.ui.startSharedStatus();
  p.streams[0].emit('shared-status', state('success_gap'));
  p.streams[0].emit('shared-status', state('upstream_retry_after', { cooldownUntil: new Date(NOW + 900000).toISOString() }));
  assert.match(p.get('sharedCooldown').textContent, /按上游要求.*剩余 15 分 0 秒/);
  assert.equal(p.timers.size, 1);
  p.streams[0].emit('auth-expired'); assert.equal(p.timers.size, 0);
  assert.equal(p.get('sharedConnection').textContent, '登录已失效，请重新登录');
  assert.deepEqual(p.emitted, ['member-session-expired']);
  p.ui.startSharedStatus(); assert.equal(p.timers.size, 0);
  p.streams[1].emit('shared-status', state(null, { status: 'active', cooldownUntil: null }));
  assert.equal(p.get('sharedCooldown').textContent, '当前无冷却');
});

test('首次SSE永久关闭只核验一次鉴权；网络错误不注销，旧连接结果不影响新登录', async () => {
  let status = 401;
  const p = page(async () => status); p.ui.startSharedStatus();
  await p.streams[0].onerror(); assert.equal(p.requests.length, 0);
  p.streams[0].readyState = 2; await p.streams[0].onerror();
  assert.deepEqual(p.emitted, ['member-session-expired']);
  assert.deepEqual(p.requests, ['/api/member/shared-status']);
  status = 503; p.ui.startSharedStatus(); p.streams[1].readyState = 2; await p.streams[1].onerror();
  assert.equal(p.emitted.length, 1); assert.equal(p.streams[1].closed, true); assert.equal(p.retries.size,1);
  let finish;
  const other = page(() => new Promise(resolve => { finish = resolve; })); other.ui.startSharedStatus();
  other.streams[0].readyState = 2; const old = other.streams[0].onerror();
  other.ui.stopSharedStatus(); other.ui.startSharedStatus(); finish(401); await old;
  assert.equal(other.emitted.length, 0);
});

test('状态版本允许跳号，重复通知忽略，重连与新boot强制校准，旧连接不再触发', () => {
  const p=page(), changes=[];
  p.ui.startSharedStatus(change=>changes.push(change));
  const event=(version,extra={})=>({bootId:'boot-a',version,scopes:['member'],updatedAt:new Date(NOW).toISOString(),resync:false,...extra});
  p.streams[0].emit('state-invalidated',event(3));
  p.streams[0].emit('state-invalidated',event(9));
  p.streams[0].emit('state-invalidated',event(9));
  p.streams[0].emit('state-invalidated',event(4));
  assert.equal(changes.length,2);assert.equal(changes[0].resync,true);assert.equal(changes[1].resync,false);
  p.visible(false);p.visible(true);
  p.streams[0].emit('state-invalidated',event(12));assert.equal(changes.length,2);
  p.streams[1].emit('state-invalidated',event(9,{resync:true}));
  p.streams[1].emit('state-invalidated',event(0,{bootId:'boot-b'}));
  assert.equal(changes.length,4);assert.ok(changes.slice(2).every(change=>change.resync));
  p.streams[1].emit('state-invalidated',event(1,{scopes:['credentials']}));
  assert.equal(changes.length,4);assert.match(p.get('sharedConnection').textContent,/状态数据无效/);
});

test('内部重连状态复用同一浏览器连接，新boot校准且旧连接事件无效', () => {
  const p = page(), changes = [];
  p.ui.startSharedStatus(change => changes.push(change));
  const event = (bootId, version) => ({ bootId, version, scopes: ['member'], resync: true });
  p.streams[0].emit('state-invalidated', event('before', 8));
  p.streams[0].emit('connection-status', { state: 'retrying' });
  assert.match(p.get('sharedConnection').textContent, /重连|断开/);
  assert.equal(p.streams.length, 1);
  assert.equal(p.requests.length, 0);
  p.streams[0].emit('connection-status', { state: 'open' });
  p.streams[0].emit('state-invalidated', event('after', 0));
  assert.equal(changes.length, 2);
  assert.equal(changes[1].resync, true);
  p.streams[0].emit('connection-status', { state: 'auth-expired' });
  assert.match(p.get('sharedConnection').textContent, /状态数据无效/);
  assert.equal(p.emitted.length, 0);
  p.visible(false); p.visible(true);
  p.streams[1].emit('connection-status', { state: 'open' });
  const current = p.get('sharedConnection').textContent;
  p.streams[0].emit('connection-status', { state: 'retrying' });
  assert.equal(p.get('sharedConnection').textContent, current);
});

test('永久关闭的临时故障有限重连，隐藏页面取消重试，恢复后新连接校准',async()=>{
  const p=page(async()=>503);p.ui.startSharedStatus();
  p.streams[0].readyState=2;await p.streams[0].onerror();
  assert.equal(p.retries.size,1);p.visible(false);assert.equal(p.retries.size,0);
  p.visible(true);assert.equal(p.streams.length,2);
  p.streams[1].readyState=2;await p.streams[1].onerror();
  const retry=[...p.retries.values()][0];p.retries.clear();retry();
  assert.equal(p.streams.length,3);assert.equal(p.emitted.length,0);
});

test('日期失效通知保留授权日期，非法日期或超大列表不能触发刷新', () => {
  const p = page(), changes = [];
  p.ui.startSharedStatus(change => changes.push(change));
  const event = (version, activityDates) => ({ bootId: 'activity', version, scopes: ['member'], updatedAt: new Date(NOW).toISOString(), activityDates });
  p.streams[0].emit('state-invalidated', event(1, ['2024-02-29', '2026-10-17']));
  assert.deepEqual([...changes[0].activityDates], ['2024-02-29', '2026-10-17']);
  for (const dates of [['2026-02-30'], ['private-prompt'], '2026-10-17', Array(367).fill('2026-10-17')]) {
    p.streams[0].emit('state-invalidated', event(2, dates));
  }
  assert.equal(changes.length, 1); assert.match(p.get('sharedConnection').textContent, /状态数据无效/);
  p.streams[0].emit('state-invalidated', event(2, ['2026-10-18']));
  assert.equal(changes.length, 2);
});
