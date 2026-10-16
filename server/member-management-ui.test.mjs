import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { setImmediate } from 'node:timers/promises';
import vm from 'node:vm';

const source = await readFile(new URL('../public/admin/members.js', import.meta.url), 'utf8');

function element() {
  return {
    value: '', checked: false, hidden: false, children: [], dataset: {}, style: {},
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
  const users = ['a', 'b', 'c', 'd'].map(id => ({ id, username: id, enabled: true, quota: { member: true, share: .25 } }));
  const server = {
    state: { configVersion: 1, users, groups: users.map(user => ({ id: user.id, name: user.id, members: [user.id], share: .25, memberWeights: { [user.id]: 1 } })) },
    nextRead: null
  };
  const context = vm.createContext({
    structuredClone, location: { pathname, origin: 'https://gateway.example' },
    document: { getElementById: get, createElement: element, querySelectorAll: () => [], querySelector: () => null },
    window: { addEventListener() {} }, setInterval() {},
    sessionStorage: { getItem: () => null }, localStorage: { getItem: () => null },
    async fetch(path, options) {
      const body = options.body ? JSON.parse(options.body) : undefined;
      calls.push({ path, body });
      if (path.endsWith('/member-management')) {
        if (!body) {
          if (server.nextRead) { const read = server.nextRead; server.nextRead = null; await read; }
          return { ok: true, status: 200, json: async () => structuredClone(server.state) };
        }
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
      refresh, previewGroups,
      edit() { directShares.a=40; directShares.b=10; applyDirectShares(); markDirty(); },
      reset() { dirty=false; },
      inspect() { return structuredClone({draft,draftConfigVersion,dirty,preview}); }
    };
  `, context);
  // owner 页面在加载时自动查询；私有管理页则由连接动作触发。
  await setImmediate();
  if (pathname === '/admin') await context.ui.refresh();
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
