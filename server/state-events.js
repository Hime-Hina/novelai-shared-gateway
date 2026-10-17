import { randomBytes } from 'node:crypto';
import { activityRange } from './activity-heatmap.js';

const hubs = new WeakMap();
const businessCollections = new Set(['settings', 'accounts', 'users', 'jobs', 'images', 'ledger', 'cards']);

export function mergeStateEvents(previous, next) {
  return { bootId: next.bootId ?? previous?.bootId, version: next.version ?? previous?.version,
    updatedAt: next.updatedAt ?? previous?.updatedAt,
    collections: [...new Set([...(previous?.collections || []), ...(next.collections || [])])],
    memberIds: [...new Set([...(previous?.memberIds || []), ...(next.memberIds || [])])],
    activityChanges: [...new Map([...(previous?.activityChanges || []), ...(next.activityChanges || [])]
      .map(change => [JSON.stringify([change.memberId, change.date]), change])).values()],
    allMembers: previous?.allMembers === true || next.allMembers === true };
}

// 内部成员与日期保持配对；浏览器只收到授权范围的日期，不收到内部成员列表。
export function stateInvalidation(event, memberId, owner, resync = false) {
  const business = event.collections?.some(name => businessCollections.has(name));
  const scopes = [];
  if (resync || event.collections?.some(name => ['settings', 'accounts', 'jobs'].includes(name))) scopes.push('shared');
  if (resync || business && (event.allMembers || event.memberIds?.includes(memberId))) scopes.push('member');
  if (owner && (resync || business)) scopes.push('owner');
  if (!scopes.length) return null;
  const { from, to } = activityRange();
  const activityDates = [...new Set((event.activityChanges || [])
    .filter(change => (owner || change.memberId === memberId) && change.date >= from && change.date <= to)
    .map(change => change.date))].sort();
  return { bootId: event.bootId, version: event.version, scopes, updatedAt: event.updatedAt, resync,
    ...(activityDates.length ? { activityDates } : {}) };
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
