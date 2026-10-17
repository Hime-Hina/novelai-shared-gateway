// 成员页和 owner 页共用连接生命周期；事件仅失效提示，数据仍由各自授权接口读取。
let memberStream = null, memberActive = false, memberHandlers = {}, retryTimer = null;
let bootId = '', version = -1;
function validActivityDates(value) {
  return value === undefined || Array.isArray(value) && value.length <= 366 && value.every(date =>
    typeof date === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(date) && Number.isFinite(Date.parse(date + 'T00:00:00Z'))
    && new Date(date + 'T00:00:00Z').toISOString().slice(0, 10) === date);
}
function disconnect() {
  memberStream?.close(); memberStream = null;
  clearTimeout(retryTimer); retryTimer = null;
}
function expired() {
  stopMemberEvents(); memberHandlers.onExpired?.();
}
function connect() {
  if (!memberActive || document.hidden || memberStream) return;
  const source = memberStream = new EventSource('/api/member/shared-events');
  const current = () => memberStream === source && memberActive && !document.hidden;
  let checkingSession = false;
  memberHandlers.onConnection?.('connecting');
  source.addEventListener('shared-status', event => {
    if (!current()) return;
    try { memberHandlers.onStatus?.(JSON.parse(event.data)); }
    catch { memberHandlers.onConnection?.('invalid'); }
  });
  source.addEventListener('state-invalidated', event => {
    if (!current()) return;
    try {
      const data = JSON.parse(event.data);
      if (typeof data.bootId !== 'string' || !data.bootId || !Number.isSafeInteger(data.version) || data.version < 0
        || !Array.isArray(data.scopes) || data.scopes.some(scope => !['shared', 'member', 'owner'].includes(scope))
        || !validActivityDates(data.activityDates)) throw new Error('invalid state event');
      const restarted = bootId !== data.bootId;
      if (!restarted && !data.resync && data.version <= version) return;
      // 权限过滤和合并均会跳号；只有明确校准或启动标识变化才要求全量重读。
      bootId = data.bootId; version = data.version;
      memberHandlers.onInvalidate?.({ ...data, resync: restarted || data.resync === true });
    } catch { memberHandlers.onConnection?.('invalid'); }
  });
  source.addEventListener('auth-expired', () => { if (current()) expired(); });
  source.addEventListener('connection-status', event => {
    if (!current()) return;
    try {
      const { state } = JSON.parse(event.data);
      if (!['open', 'retrying'].includes(state)) throw new Error('invalid connection state');
      memberHandlers.onConnection?.(state);
    } catch { memberHandlers.onConnection?.('invalid'); }
  });
  source.onopen = () => { if (current()) memberHandlers.onConnection?.('open'); };
  source.onerror = async () => {
    if (!current()) return;
    memberHandlers.onConnection?.('retrying');
    // 普通掉线交给 EventSource 重连；永久关闭才补查鉴权，不能把 503 当作注销。
    if (source.readyState !== EventSource.CLOSED || checkingSession) return;
    checkingSession = true;
    try {
      const response = await fetch('/api/member/shared-status');
      if (current() && [401, 403].includes(response.status)) { expired(); return; }
    } catch {}
    if (current()) {
      disconnect();
      retryTimer = setTimeout(() => { retryTimer = null; connect(); }, 5000);
    }
  };
}
export function startMemberEvents(handlers) {
  memberHandlers = handlers; memberActive = true; connect();
}
export function stopMemberEvents() {
  memberActive = false; disconnect(); bootId = ''; version = -1;
  memberHandlers.onConnection?.('closed');
}
document.addEventListener('visibilitychange', () => { if (document.hidden) disconnect(); else connect(); });
window.addEventListener('pagehide', disconnect);
window.addEventListener('pageshow', connect);
