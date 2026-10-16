import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { setImmediate } from 'node:timers/promises';
import vm from 'node:vm';

const source = (await readFile(new URL('../../public/account.js', import.meta.url), 'utf8')).replace(/^import[^\n]+\n/, '');
const html = await readFile(new URL('../../public/account.html', import.meta.url), 'utf8');
function element(tag = 'div') {
  return {
    tagName: tag.toUpperCase(), children: [], hidden: false, value: '',
    set innerHTML(value) { throw new Error('接口文本不能插入 innerHTML'); },
    append(...children) { this.children.push(...children); },
    replaceChildren(...children) { this.children = children; }
  };
}
const text = element => String(element.textContent ?? '') + element.children.map(text).join(' ');
const links = element => [...(element.tagName === 'A' ? [element] : []), ...element.children.flatMap(links)];

test('成员退款与冷历史迟到领取使用签名地址，文本不插 HTML，也不重扣额度', async () => {
  const elements = new Map(), calls = [];
  const get = id => { if (!elements.has(id)) elements.set(id, element()); return elements.get(id); };
  const hostile = '<img src=x onerror=alert(1)>';
  const signed = 'https://gateway.example/images/late.png?expires=123&sig=signed-result';
  const refund = { status: 'refunded', resource: 'anlas', amount: 15, fixed: 12, purchased: 3 };
  const data = {
    username: hostile, quota: {},
    jobs: [{ id: 'recent', createdAt: 1, model: hostile, status: 'failed', charge: refund, lateResultAvailable: true, imageUrl: signed }, { id: 'invalid', createdAt: 1, model: 'test', status: 'done', imageUrl: 'javascript:alert(1)' }],
    lateResults: [{ id: 'older-than-recent-history', createdAt: 1, lateResultAt: 2, model: hostile, imageUrl: signed, charge: refund }]
  };
  const context = vm.createContext({
    URL, location: { origin: 'https://gateway.example' },
    document: { getElementById: get, createElement: element },
    startSharedStatus() {}, stopSharedStatus() {},
    async fetch(path, options) {
      calls.push({ path, options });
      return { ok: true, json: async () => path.endsWith('/login-options') ? { discord: false, ready: true } : structuredClone(data) };
    }
  });
  vm.runInContext(source + '\nglobalThis.ui = {load};', context);
  await setImmediate();
  assert.equal(get('welcome').textContent, hostile + ' · 我的额度');
  assert.equal(get('lateResultsPanel').hidden, false);
  assert.ok(text(get('jobs')).includes(hostile));
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
