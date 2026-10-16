import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { setImmediate } from 'node:timers/promises';
import vm from 'node:vm';

const source = (await readFile(new URL('../../public/account.js', import.meta.url), 'utf8')).replace(/^import[^\n]+\n/, '');
const html = await readFile(new URL('../../public/account.html', import.meta.url), 'utf8');
function element(tag = 'div') {
  return {
    tagName: tag.toUpperCase(), children: [], hidden: false, value: '', style: {}, attributes: {},
    setAttribute(name, value) { this.attributes[name] = value; },
    set innerHTML(value) { throw new Error('接口文本不能插入 innerHTML'); },
    append(...children) { this.children.push(...children); },
    replaceChildren(...children) { this.children = children; }
  };
}
const text = element => String(element.textContent ?? '') + element.children.map(text).join(' ');
const nodes = (element, tag) => [...(element.tagName === tag ? [element] : []), ...element.children.flatMap(child => nodes(child, tag))];
const links = element => nodes(element, 'A');

async function accountPage(data, extraRequest) {
  const elements = new Map(), calls = [], events = new Map();
  const get = id => { if (!elements.has(id)) elements.set(id, element()); return elements.get(id); };
  const context = vm.createContext({
    URL, location: { origin: 'https://gateway.example' },
    document: { getElementById: get, createElement: element },
    window: { addEventListener: (name, listener) => events.set(name, listener) },
    startSharedStatus() {}, stopSharedStatus() {},
    async fetch(path, options) {
      calls.push({ path, options });
      const value = path.endsWith('/login-options') ? { discord: false, ready: true } : path === '/api/member/me' ? data : await extraRequest(path, options);
      return { ok: true, json: async () => structuredClone(value) };
    }
  });
  vm.runInContext(source + '\nglobalThis.ui = {load};', context);
  await setImmediate();
  return { get, calls, context, expire: () => events.get('member-session-expired')() };
}

test('显示固定到期时间；登录失效清除Key，迟到读取不能重新展开账户或Key', async () => {
  let releaseKey;
  const data = { username: 'member', quota: {}, jobs: [], sessionExpiresAt: 1800000000000 };
  const page = await accountPage(data, () => new Promise(resolve => { releaseKey = resolve; }));
  assert.match(page.get('sessionExpiry').textContent, /本次登录有效至.*不会自动续期/);
  page.get('apiKey').value='synthetic-old-key';
  const pending = page.get('showKey').onclick();
  page.expire();
  releaseKey({ apiKey: 'synthetic-late-key', baseUrl: 'https://gateway.example/v1' }); await pending;
  assert.equal(page.get('apiKey').value, ''); assert.equal(page.get('apiBaseUrl').value, '');
  assert.equal(page.get('keyPanel').hidden, true); assert.equal(page.get('account').hidden, true);
  assert.equal(page.get('login').hidden, false); assert.match(page.get('message').textContent, /登录已失效/);
  let releaseMe;
  page.context.fetch = () => new Promise(resolve => { releaseMe = () => resolve({ ok: true, json: async () => data }); });
  const refreshing = page.context.ui.load(); page.expire(); releaseMe(); await refreshing;
  assert.equal(page.get('account').hidden, true);
  let rejectOld;
  page.context.fetch=()=>new Promise(resolve=>{rejectOld=()=>resolve({ok:false,status:401,json:async()=>({error:'old session'})});});
  const stale=page.context.ui.load();
  page.context.fetch=async()=>({ok:true,json:async()=>data}); await page.context.ui.load();
  rejectOld(); await assert.rejects(stale,/old session/);
  assert.equal(page.get('account').hidden,false,'旧会话的迟到401不能关闭新登录页面');
});

test('成员退款与冷历史迟到领取使用签名地址，文本不插 HTML，也不重扣额度', async () => {
  const hostile = '<img src=x onerror=alert(1)>';
  const signed = 'https://gateway.example/images/late.png?expires=123&sig=signed-result';
  const refund = { status: 'refunded', resource: 'anlas', amount: 15, fixed: 12, purchased: 3 };
  const data = {
    username: hostile, quota: {},
    jobs: [{ id: 'recent', createdAt: 1, model: hostile, status: 'failed', charge: refund, lateResultAvailable: true, imageUrl: signed }, { id: 'invalid', createdAt: 1, model: 'test', status: 'done', imageUrl: 'javascript:alert(1)' }, { id: 'pending', createdAt: 1, model: 'test', status: 'pending' }],
    lateResults: [{ id: 'older-than-recent-history', createdAt: 1, lateResultAt: 2, model: hostile, imageUrl: signed, charge: refund }]
  };
  const { get, calls, context } = await accountPage(data);
  assert.equal(get('welcome').textContent, hostile + ' · 我的额度');
  assert.equal(get('lateResultsPanel').hidden, false);
  assert.ok(text(get('jobs')).includes(hostile));
  assert.equal(get('jobs').children[2].children[2].textContent, '等待前序图片');
  assert.match(text(get('jobs')), /失败.*领取迟到结果（退款保留）.*已退款（本站额度）.*订阅 12 \/ 购买 3/);
  assert.match(text(get('lateResults')), /找回.*已退款（本站额度）/);
  assert.match(html, /退款保留，领取不另扣费/);
  assert.match(html, /不代表上游已退款/);
  const resultLinks = [...links(get('jobs')), ...links(get('lateResults'))];
  assert.equal(resultLinks.length, 2);
  for (const link of resultLinks) {
    assert.equal(link.href, signed);
    assert.equal(link.referrerPolicy, 'no-referrer');
    assert.equal(link.rel, 'noopener noreferrer');
  }
  assert.deepEqual(calls.map(call => call.path), ['/api/member/login-options', '/api/member/me']);
  assert.ok(calls.every(call => call.options.method === 'GET' && call.options.credentials === 'same-origin'));
  data.jobs[0].charge = { ...refund, fixed: null, purchased: undefined };
  await context.ui.load();
  assert.match(text(get('jobs')), /已退款（本站额度） · 15 Anlas · 订阅 未知 \/ 购买 未知/);
  assert.equal(get('fixed').textContent, '0');
  assert.equal(get('purchased').textContent, '0');
  data.jobs = []; data.lateResults = [];
  await context.ui.load();
  assert.equal(get('lateResultsPanel').hidden, true);
  assert.equal(links(get('jobs')).length, 0);
});

test('批次详情懒读取并去重，取消只标记未派发图片，旧读取不覆盖取消结果', async () => {
  const hostile = '<img src=x onerror=alert(1)>', signed = 'https://gateway.example/images/result.png?sig=synthetic';
  const data = { username: 'member', quota: {}, lateResults: [], jobs: [
    { id: 'done', requestId: 'req-batch', model: 'test', createdAt: 1, status: 'done' },
    { id: 'running', requestId: 'req-batch', model: 'test', createdAt: 1, status: 'running' },
    { id: 'legacy', requestId: null, model: 'test', createdAt: 1, status: 'done' }
  ] };
  let request = { id: 'req-batch', source: 'novelai-native', operation: 'generate', createdAt: 1, status: 'running', cancellable: true,
    counts: { total: 5, queued: 1, pending: 1, running: 1, done: 1, failed: 1 },
    jobs: ['done', 'running', 'pending', 'queued', 'failed'].map((status, index) => ({ id: 'job-'+index, index, status, model: hostile, createdAt: 1, updatedAt: 1, cancelled: false, imageUrl: index === 0 ? signed : '', error: index === 4 ? hostile : '' })) };
  let pendingRead = null;
  const { get, calls } = await accountPage(data, async (path, options) => {
    if (options.method === 'GET') {
      assert.equal(path, '/api/member/requests/req-batch');
      return pendingRead || request;
    }
    assert.equal(path, '/api/member/requests/req-batch/cancel');
    assert.equal(options.body, '{}');
    assert.equal(options.credentials, 'same-origin');
    assert.equal(options.headers['Content-Type'], 'application/json');
    request = { ...request, cancellable: false, cancelledCount: 2,
      counts: { total: 5, queued: 0, pending: 0, running: 1, done: 1, failed: 3 },
      jobs: request.jobs.map(job => ['pending', 'queued'].includes(job.status) ? { ...job, status: 'failed', cancelled: true } : job) };
    data.quota.anlasFixedAvailable = 77;
    return request;
  });
  assert.equal(calls.length, 2);
  assert.equal(nodes(get('jobs'), 'DETAILS').length, 1);
  let details = nodes(get('jobs'), 'DETAILS')[0];
  assert.match(text(details), /查看批次 · req-batch/);
  details.open = true; await details.ontoggle();
  assert.ok(text(details).includes(hostile));
  assert.match(text(details), /第 2 张.*生成中/);
  assert.match(text(details), /第 5 张.*失败/);
  assert.ok(!text(details).includes('已取消（尚未派发）'));
  assert.match(text(details), /仅取消此批次排队或等待前序的图片/);
  assert.equal(links(details)[0].href, signed);
  assert.equal(links(details)[0].referrerPolicy, 'no-referrer');
  const cancel = nodes(details, 'BUTTON').find(button => button.textContent.startsWith('取消'));
  let finishRead;
  const previous = structuredClone(request);
  pendingRead = new Promise(resolve => { finishRead = () => resolve(previous); });
  const oldRead = nodes(details, 'BUTTON').find(button => button.textContent === '刷新批次').onclick();
  const cancelling = cancel.onclick();
  details.open = false; await details.ontoggle();
  await cancelling;
  finishRead(); await oldRead; pendingRead = null;
  assert.equal(get('fixed').textContent, '77');
  assert.match(get('message').textContent, /此次取消 2 条尚未派发任务/);
  assert.match(text(details), /等待前序 0 · 排队 0 · 生成中 1/);
  assert.match(text(details), /第 5 张.*失败/);
  assert.equal((text(details).match(/已取消（尚未派发）/g) || []).length, 2);
  details = nodes(get('jobs'), 'DETAILS')[0];
  details.open = true; await details.ontoggle();
  assert.match(text(details), /第 2 张.*生成中/);
  assert.equal(nodes(details, 'BUTTON').find(button => button.textContent.startsWith('取消')).disabled, true);
  const reads = () => calls.filter(call => call.path.startsWith('/api/member/requests/') && call.options.method === 'GET').length;
  const beforeReopen = reads();
  details.open = false; await details.ontoggle();
  details.open = true; await details.ontoggle();
  assert.equal(reads(), beforeReopen + 1);
  await get('refresh').onclick();
  details = nodes(get('jobs'), 'DETAILS')[0];
  details.open = true; await details.ontoggle();
  assert.equal(reads(), beforeReopen + 2);
});
