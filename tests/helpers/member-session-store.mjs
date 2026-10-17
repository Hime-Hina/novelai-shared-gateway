import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { JsonStore } from '../../server/store.ts';

// 旧路由测试保留领域 fixture；会话直接复用真实 SQLite，不复制会话存储和撤销规则。
export async function withMemberSessions(t, store) {
  const sessions = new JsonStore(await mkdtemp(path.join(tmpdir(), 'nai-route-sessions-')));
  await sessions.init(); t.after(() => sessions.close());
  const update = store.update.bind(store);
  store.events = sessions.events;
  store.readMemberSession = hash => sessions.readMemberSession(hash);
  store.readMemberSessionOwner = () => sessions.readMemberSessionOwner();
  store.update = (fn, options = {}) => update(db =>
    ['memberSession', 'memberSessionDelete', 'memberSessionOwner'].some(key => Object.hasOwn(options, key))
      ? sessions.update(() => fn(db), options) : fn(db), options);
  return store;
}
