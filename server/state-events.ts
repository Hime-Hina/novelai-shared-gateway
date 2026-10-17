import { randomBytes } from 'node:crypto';
import type { EventEmitter } from 'node:events';
import { activityRange } from './activity-heatmap.ts';

interface StateVersion { bootId: string; version: number; updatedAt: string }
interface StateEvent extends Partial<StateVersion> {
  collections?: readonly string[];
  memberIds?: readonly string[];
  activityChanges?: readonly { readonly memberId: string | null; readonly date: string }[];
  allMembers?: boolean;
}
type PublishedEvent = StateEvent & StateVersion;
type StateListener = (event: PublishedEvent) => unknown;
type EventStore = { events: Pick<EventEmitter, 'on' | 'once' | 'off'> };
interface StateHub {
  current(): StateVersion;
  closeSubscriptions(): void;
  subscribe(listener: StateListener, onClose?: () => void): () => void;
}
export interface StateInvalidation extends StateVersion {
  scopes: ('shared' | 'member' | 'owner')[];
  resync: boolean;
  activityDates?: string[];
}

const hubs = new WeakMap<EventStore, StateHub>();
const businessCollections = new Set(['settings', 'accounts', 'users', 'jobs', 'images', 'ledger', 'cards']);

export function mergeStateEvents(previous: StateEvent | undefined, next: PublishedEvent): PublishedEvent;
export function mergeStateEvents(previous: StateEvent | undefined, next: StateEvent): StateEvent;
export function mergeStateEvents(previous: StateEvent | undefined, next: StateEvent): StateEvent {
  return { bootId: next.bootId ?? previous?.bootId, version: next.version ?? previous?.version,
    updatedAt: next.updatedAt ?? previous?.updatedAt,
    collections: [...new Set([...(previous?.collections || []), ...(next.collections || [])])],
    memberIds: [...new Set([...(previous?.memberIds || []), ...(next.memberIds || [])])],
    activityChanges: [...new Map([...(previous?.activityChanges || []), ...(next.activityChanges || [])]
      .map(change => [JSON.stringify([change.memberId, change.date]), change])).values()],
    allMembers: previous?.allMembers === true || next.allMembers === true };
}

// 内部成员与日期保持配对；浏览器只收到授权范围的日期，不收到内部成员列表。
export function stateInvalidation(event: PublishedEvent, memberId: string, owner: boolean, resync = false): StateInvalidation | null {
  const business = event.collections?.some(name => businessCollections.has(name));
  const scopes: StateInvalidation['scopes'] = [];
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

export function stateEventsFor(store: EventStore): StateHub {
  const existing = hubs.get(store);
  if (existing) return existing;
  const bootId = randomBytes(12).toString('hex'), listeners = new Map<StateListener, (() => void) | undefined>();
  let version = 0, updatedAt = new Date().toISOString(), closed = false;
  let pending: StateEvent | undefined, timer: ReturnType<typeof setTimeout> | undefined, firstAt: number;
  const flush = () => {
    timer = undefined;
    if (closed || !pending) return;
    updatedAt = new Date().toISOString();
    const event = { ...pending, bootId, version: ++version, updatedAt }; pending = undefined;
    for (const listener of listeners.keys()) {
      try { Promise.resolve(listener(event)).catch(() => {}); } catch {}
    }
  };
  const commit = (event: StateEvent) => {
    if (closed) return;
    if (!pending) firstAt = Date.now();
    pending = mergeStateEvents(pending, event);
    clearTimeout(timer);
    timer = setTimeout(flush, Math.max(0, Math.min(200, firstAt + 1000 - Date.now())));
    timer.unref?.();
  };
  const closeSubscriptions = () => {
    if (closed) return;
    closed = true; clearTimeout(timer); pending = undefined;
    store.events.off('commit', commit);
    for (const onClose of listeners.values()) { try { onClose?.(); } catch {} }
    listeners.clear();
    // 关闭数据库前保留已关闭的 hub，阻止尚在鉴权的请求重新开启订阅。
  };
  const close = () => {
    closeSubscriptions();
    store.events.off('close', close); hubs.delete(store);
  };
  const hub: StateHub = { current: () => ({ bootId, version, updatedAt }), closeSubscriptions,
    subscribe(listener, onClose) {
      if (closed) { onClose?.(); return () => {}; }
      listeners.set(listener, onClose);
      return () => listeners.delete(listener);
    }
  };
  store.events.on('commit', commit); store.events.once('close', close);
  hubs.set(store, hub); return hub;
}
