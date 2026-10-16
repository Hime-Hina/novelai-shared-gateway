import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { setImmediate } from 'node:timers/promises';
import vm from 'node:vm';

const source = await readFile(new URL('../../public/admin/members.js', import.meta.url), 'utf8');

function element(tag = 'div') {
  return {
    tagName: tag.toUpperCase(), value: '', checked: false, hidden: false, children: [], dataset: {}, style: {},
    set innerHTML(value) { throw new Error('不应使用 innerHTML 渲染接口文本'); },
    classList: { toggle() {} }, setAttribute() {}, addEventListener() {},
    append(...children) { this.children.push(...children); },
    replaceChildren(...children) { this.children = children; },
    showModal() { this.open = true; }, close() { this.open = false; }
  };
}

async function managementPage(pathname) {
  const elements = new Map(), calls = [];
  const get = id => { if (!elements.has(id)) elements.set(id, element()); return elements.get(id); };
  get('usageGroup').value = 'members';
  get('operatingAction').value = 'operating-contribute';
  get('operatingResource').value = 'v5';
  get('operatingDestination').value = 'operating';
  const users = ['a', 'b', 'c', 'd'].map(id => ({ id, username: id, enabled: true, quota: { member: true, share: .25 } }));
  const server = {
    state: { configVersion: 1, users, groups: users.map(user => ({ id: user.id, name: user.id, members: [user.id], share: .25, memberWeights: { [user.id]: 1 } })) },
    nextRead: null, nextLossRead: null,
    losses: {
      asOf: 1, policy: { version: 1, v5Points24h: 1, fixedAnlas24h: 100, fixedAnlas30d: 500, memberAttempts24h: 3 },
      operating: { available: { v5: 2, anlasFixed: 40, anlasPurchased: 5 }, unfunded: { anlasFixed: 7, anlasPurchased: 11 }, unattributedReturns: { anlasFixed: 3, anlasPurchased: 0 }, v5CapacityGap: 1, gapEventIds: { v5: 'v5-gap', anlasFixed: 'fixed-gap', anlasPurchased: 'purchased-gap' }, pendingExternalFunding: [] },
      risk: {
        last24h: { attempts: 1, unknownAmounts: 1, possible: { v5: 1.5, anlasFixed: 12, anlasPurchased: 3 } },
        last30d: { attempts: 2, unknownAmounts: 1, possible: { v5: 2.5, anlasFixed: 15, anlasPurchased: 4 } },
        aggregate: { last24h: { v5: 1, anlasFixed: 7, anlasPurchased: 11 }, last30d: { v5: 2, anlasFixed: 10, anlasPurchased: 20 } }
      },
      alerts: [], events: [], nextCursor: null
    }
  };
  const context = vm.createContext({
    structuredClone, location: { pathname, origin: 'https://gateway.example' },
    document: { getElementById: get, createElement: element, querySelectorAll: () => [], querySelector: () => null },
    window: { addEventListener() {} }, setInterval() {},
    sessionStorage: { getItem: () => null }, localStorage: { getItem: () => null },
    async fetch(path, options) {
      const body = options.body ? JSON.parse(options.body) : undefined;
      calls.push({ path, body, headers: options.headers });
      if (path.includes('/member-management?view=losses')) {
        const response = structuredClone(path.includes('&before=') ? server.olderLosses : server.losses);
        if (server.nextLossRead) { const read = server.nextLossRead; server.nextLossRead = null; await read; }
        return { ok: !server.lossError, status: server.lossError || 200, json: async () => server.lossError ? { error: '无权读取' } : response };
      }
      if (path.endsWith('/member-management')) {
        if (!body) {
          if (server.nextRead) { const read = server.nextRead; server.nextRead = null; await read; }
          return { ok: true, status: 200, json: async () => structuredClone(server.state) };
        }
        if (body.action === 'loss-alert-policy') {
          const conflict = body.expectedVersion !== server.losses.policy.version;
          if (!conflict) server.losses.policy = { ...body.policy, version: body.expectedVersion + 1 };
          return { ok: !conflict, status: conflict ? 409 : 200, json: async () => conflict ? { error: '阈值版本已变化，请保留草稿' } : { policy: structuredClone(server.losses.policy) } };
        }
        if (body.action.startsWith('operating-')) return { ok: true, status: 200, json: async () => ({}) };
        const conflict = body.expectedConfigVersion !== server.state.configVersion;
        return {
          ok: !conflict, status: conflict ? 409 : 200,
          json: async () => conflict ? { error: '分组版本已变化，请保留草稿并刷新后重新编辑' } : structuredClone(server.state)
        };
      }
      return { ok: true, status: 200, json: async () => path.includes('/statistics') ? { totals: {}, members: [], recent: [] } : {} };
    }
  });
  vm.runInContext(source + `
    globalThis.ui = {
      refresh, previewGroups, loadLosses,
      edit() { directShares.a=40; directShares.b=10; applyDirectShares(); markDirty(); },
      reset() { dirty=false; },
      inspect() { return structuredClone({draft,draftConfigVersion,dirty,preview}); },
      inspectPolicy() { return structuredClone({lossPolicyVersion,lossPolicyDirty}); }
    };
  `, context);
  // owner 页面在加载时自动查询；私有管理页则由连接动作触发。
  await setImmediate();
  if (pathname === '/admin') { get('adminToken').value = 'private-test-token'; await context.ui.refresh(); }
  await setImmediate();
  return { ui: context.ui, get, calls, server };
}

for (const pathname of ['/admin', '/owner']) {
  test(`${pathname} 预览和保存沿用草稿版本，冲突不重试或覆盖草稿`, async () => {
    const { ui, get, calls, server } = await managementPage(pathname);
    const apiPath = pathname === '/owner' ? '/api/member/owner/member-management' : '/api/admin/member-management';
    ui.edit();
    const draft = ui.inspect();
    server.state.configVersion = 2;
    const before = calls.length;
    await assert.rejects(ui.previewGroups(), /保留草稿/);
    assert.equal(calls.length, before + 1);
    assert.equal(calls.at(-1).path, apiPath);
    assert.equal(calls.at(-1).body.expectedConfigVersion, 1);
    assert.deepEqual(ui.inspect(), draft);

    // 用户主动放弃旧草稿、查询最新状态后，才建立新的编辑基线。
    ui.reset();
    await ui.refresh();
    ui.edit();
    await ui.previewGroups();
    assert.equal(ui.inspect().preview.expectedConfigVersion, 2);
    server.state.configVersion = 3;
    const previewed = ui.inspect(), requestsBeforeSave = calls.length;
    await get('confirmGroups').onclick();
    assert.equal(calls.length, requestsBeforeSave + 1);
    assert.equal(calls.at(-1).body.action, 'groups');
    assert.equal(calls.at(-1).body.expectedConfigVersion, 2);
    assert.deepEqual(ui.inspect(), previewed);
    assert.match(get('message').textContent, /保留草稿/);
  });
}

test('后台刷新尚未返回时开始编辑，也不覆盖草稿或抬高编辑版本', async () => {
  const { ui, server } = await managementPage('/admin');
  let finishRead;
  server.nextRead = new Promise(resolve => { finishRead = resolve; });
  const refreshing = ui.refresh();
  ui.edit();
  const draft = ui.inspect();
  server.state.configVersion = 2;
  finishRead();
  await refreshing;
  assert.deepEqual(ui.inspect(), draft);
  await assert.rejects(ui.previewGroups(), /保留草稿/);
});

const text = element => String(element.textContent ?? '') + element.children.map(text).join(' ');

for (const pathname of ['/admin', '/owner']) {
  test(`${pathname} 损耗沿用鉴权入口，阈值冲突保留草稿和版本`, async () => {
    const { ui, get, calls, server } = await managementPage(pathname);
    const prefix = pathname === '/owner' ? '/api/member/owner/' : '/api/admin/';
    const read = calls.find(call => call.path.includes('?view=losses'));
    assert.equal(read.path, prefix + 'member-management?view=losses');
    assert.equal(read.headers['x-admin-token'], pathname === '/owner' ? undefined : 'private-test-token');
    get('lossPolicyV5').value = '4.5'; get('lossPolicyV5').oninput();
    server.losses.policy.version = 2;
    server.losses.policy.v5Points24h = 8;
    await ui.loadLosses();
    assert.equal(get('lossPolicyV5').value, '4.5');
    assert.deepEqual(ui.inspectPolicy(), { lossPolicyVersion: 1, lossPolicyDirty: true });
    const before = calls.length;
    await get('lossPolicyForm').onsubmit();
    assert.equal(calls.length, before + 1);
    assert.equal(calls.at(-1).path, prefix + 'member-management');
    assert.equal(calls.at(-1).body.expectedVersion, 1);
    assert.equal(calls.at(-1).body.policy.v5Points24h, 4.5);
    assert.match(get('message').textContent, /保留草稿/);
    assert.equal(get('lossPolicyV5').value, '4.5');
    assert.equal(ui.inspectPolicy().lossPolicyVersion, 1);
    await get('resetLossPolicy').onclick();
    assert.equal(get('lossPolicyV5').value, 8);
    assert.equal(ui.inspectPolicy().lossPolicyVersion, 2);
    get('lossPolicyV5').value = '0'; get('lossPolicyV5').oninput();
    await get('lossPolicyForm').onsubmit();
    assert.equal(server.losses.policy.v5Points24h, 0);
    assert.deepEqual(ui.inspectPolicy(), { lossPolicyVersion: 3, lossPolicyDirty: false });
    if (pathname === '/owner') {
      server.lossError = 403;
      await assert.rejects(ui.loadLosses(), /无权读取/);
      assert.equal(get('dashboard').hidden, true);
      assert.equal(get('ownerSignin').hidden, false);
    }
  });
}

test('独立损耗读取不阻塞摘要，返回时保留后来开始的阈值编辑', async () => {
  const { ui, get, server } = await managementPage('/admin');
  let finishRead;
  server.nextLossRead = new Promise(resolve => { finishRead = resolve; });
  await ui.refresh();
  assert.equal(get('dashboard').hidden, false);
  get('lossPolicyFixed24').value = '50'; get('lossPolicyFixed24').oninput();
  finishRead(); await setImmediate();
  assert.equal(get('lossPolicyFixed24').value, '50');
  assert.equal(ui.inspectPolicy().lossPolicyDirty, true);
});

test('候选、聚合、运营独立展示；事件文本安全且更早分页可追加', async () => {
  const { ui, get, server, calls } = await managementPage('/owner');
  const hostile = '<img src=x onerror=alert(1)>';
  server.losses.alerts = [{ id: 'a', level: 'warning', message: hostile }];
  server.losses.events = [{ id: 'estimate', type: 'generation_loss_estimated', at: 1, memberName: hostile, evidenceLevel: 'unconfirmed_estimate', candidateMode: 'alternatives', upstreamCandidates: [{ resources: { v5: 1.5 } }, { resources: { anlasFixed: 12, anlasPurchased: 3 } }] }];
  server.losses.nextCursor = 'cursor/+=';
  await ui.loadLosses();
  assert.equal(get('lossAlerts').children[0].textContent, hostile);
  assert.match(text(get('lossEvents')), /互斥，不相加.*V5 百分点 1.5 或 订阅 Anlas 12 \/ 购买 Anlas 3/);
  assert.ok(text(get('lossEvents')).includes(hostile));
  assert.match(text(get('lossEvents')), /证据：未确认消费的估计结案/);
  assert.ok(!text(get('lossEvents')).includes('unconfirmed_estimate'));
  assert.deepEqual(get('lossRiskRows').children.map(row => row.children.map(cell => cell.textContent)), [['V5 百分点', '1.5', '2.5'], ['订阅 Anlas', '12', '15'], ['购买 Anlas', '3', '4']]);
  assert.deepEqual(get('lossBalanceRows').children[2].children.map(cell => cell.textContent), ['购买 Anlas', '11', '20', '11', '5', '0']);
  server.olderLosses = { ...server.losses, events: [{ id: 'older', type: 'generation_loss_suspected', at: 0, memberName: hostile }], nextCursor: null };
  await get('moreLossEvents').onclick();
  assert.equal(calls.at(-1).path, '/api/member/owner/member-management?view=losses&before=cursor%2F%2B%3D');
  assert.equal(get('lossEvents').children.length, 2);
  assert.equal(get('moreLossEvents').hidden, true);
  server.losses.events = [{ id: 'new', type: 'generation_result_recovered', at: 2, evidenceLevel: 'future_evidence' }];
  await ui.loadLosses();
  assert.equal(get('lossEvents').children.length, 2);
  assert.equal(get('moreLossEvents').hidden, true);
  assert.match(text(get('lossEvents')), /estimate/);
  await get('reloadLossEvents').onclick();
  assert.equal(get('lossEvents').children.length, 1);
  assert.match(text(get('lossEvents')), /证据类型待确认/);
  assert.ok(!text(get('lossEvents')).includes('future_evidence'));
});

test('运营表单只发送当前操作所需字段，不指定其他成员钱包', async () => {
  const { get, calls } = await managementPage('/admin');
  for (const action of ['operating-contribute', 'operating-register-funding', 'operating-cover-loss', 'operating-reconcile-return']) {
    get('operatingAction').value = action;
    get('operatingAction').onchange();
    get('operatingAmount').value = '2'; get('operatingReceipt').value = 'receipt_123';
    get('operatingFromUnattributed').checked = true;
    await get('operatingForm').onsubmit();
    const body = calls.filter(call => call.body?.action === action).at(-1).body;
    assert.equal(body.amount, 2); assert.equal(body.receipt, 'receipt_123');
    assert.equal(body.userId, undefined);
    assert.equal('destination' in body, action === 'operating-register-funding');
    assert.equal('fromUnattributed' in body, action === 'operating-register-funding');
    assert.equal('lossEventId' in body, ['operating-cover-loss', 'operating-reconcile-return'].includes(action));
    if (body.lossEventId) assert.equal(body.lossEventId, 'fixed-gap');
    assert.equal(get('operatingReceipt').value, '');
  }
});
