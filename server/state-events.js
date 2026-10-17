import { randomBytes } from 'node:crypto';

const hubs = new WeakMap();
const businessCollections = new Set(['settings', 'accounts', 'users', 'jobs', 'images', 'ledger', 'cards']);

export function mergeStateEvents(previous, next) {
  return { bootId: next.bootId ?? previous?.bootId, version: next.version ?? previous?.version,
    updatedAt: next.updatedAt ?? previous?.updatedAt,
    collections: [...new Set([...(previous?.collections || []), ...(next.collections || [])])],
    memberIds: [...new Set([...(previous?.memberIds || []), ...(next.memberIds || [])])],
    allMembers: previous?.allMembers === true || next.allMembers === true };
}

// 内部归属信息只用于筛选，发送到浏览器的对象始终是这五个白名单字段。
export function stateInvalidation(event, memberId, owner, resync = false) {
  const business = event.collections?.some(name => businessCollections.has(name));
  const scopes = [];
  if (resync || event.collections?.some(name => ['settings', 'accounts', 'jobs'].includes(name))) scopes.push('shared');
  if (resync || business && (event.allMembers || event.memberIds?.includes(memberId))) scopes.push('member');
  if (owner && (resync || business)) scopes.push('owner');
  return scopes.length ? { bootId: event.bootId, version: event.version, scopes, updatedAt: event.updatedAt, resync } : null;
}

export function stateEventsFor(store) {
  if (hubs.has(store)) return hubs.get(store);
  const bootId = randomBytes(12).toString('hex'), listeners = new Map();
  let version = 0, updatedAt = new Date().toISOString(), pending, timer, firstAt, closed = false;
  const flush = () => {
    timer = undefined;
    if (closed || !pending) return;
    updatedAt = new Date().toISOString();
    const event = { ...pending, bootId, version: ++version, updatedAt }; pending = undefined;
    for (const listener of listeners.keys()) {
      try { Promise.resolve(listener(event)).catch(() => {}); } catch {}
    }
  };
  const commit = event => {
    if (closed) return;
    if (!pending) firstAt = Date.now();
    pending = mergeStateEvents(pending, event);
    clearTimeout(timer);
    timer = setTimeout(flush, Math.max(0, Math.min(200, firstAt + 1000 - Date.now())));
    timer.unref?.();
  };
  const close = () => {
    closed = true; clearTimeout(timer); pending = undefined;
    store.events.off('commit', commit); store.events.off('close', close);
    for (const onClose of listeners.values()) { try { onClose?.(); } catch {} }
    listeners.clear(); hubs.delete(store);
  };
  const hub = { current: () => ({ bootId, version, updatedAt }),
    subscribe(listener, onClose) {
      if (closed) { onClose?.(); return () => {}; }
      listeners.set(listener, onClose);
      return () => listeners.delete(listener);
    }
  };
  store.events.on('commit', commit); store.events.once('close', close);
  hubs.set(store, hub); return hub;
}
