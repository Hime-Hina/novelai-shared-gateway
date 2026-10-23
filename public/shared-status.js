import { startMemberEvents, stopMemberEvents } from '/member-events.js';
let active = false, cooldownTimer = null, snapshot = null;
const box = document.createElement('section');
box.innerHTML = `<h2>共享账户状态 <small>只读</small></h2><p id="sharedConnection" class="muted">等待连接</p><div class="cards"><article><h2>订阅与队列</h2><strong id="sharedState">—</strong><p id="sharedSubscription"></p><p id="sharedQueue"></p><p id="sharedCooldown"></p></article><article><h2>公共 V5 余量</h2><strong id="sharedV5">—</strong><p id="sharedRefill"></p><small>显示官方最近一次查询值，不等同于你的个人余额。</small></article><article><h2>公共 Anlas</h2><strong id="sharedFixed">—</strong><p id="sharedPurchased"></p><small>订阅与购买额度独立记录。</small></article></div><p id="sharedUpdated" class="muted"></p><p id="sharedError" class="notice" hidden></p>`;
document.getElementById('status').after(box);
const $ = id => document.getElementById(id);
const n = value => value == null ? '未知' : Number(value).toLocaleString('zh-CN', {maximumFractionDigits:3});
const date = value => {if (!value) return '未知'; const d = new Date(value);return Number.isNaN(d.getTime()) ? '未知' : d.toLocaleString('zh-CN');};
const labels = { pending_opus:'等待 Opus', inactive_opus:'订阅不可用', stale:'余额已过期', active:'可用', cooldown:'等待派发', quota_sync_error:'同步异常', disabled:'账户停用', unknown_balance:'余额未知' };
const cooldownReasons = { success_gap:'上一张已完成，等待生成间隔', ordinary_failure:'生成失败，短暂等待后继续',
  capacity_limit:'上游并发或容量限制，等待后继续', upstream_retry_after:'按上游要求等待后继续', completion_gap:'任务已结束，等待生成间隔' };
function clearCooldownTimer() {
  if (cooldownTimer !== null) clearInterval(cooldownTimer);
  cooldownTimer = null;
}
function renderCooldown() {
  const deadline = Date.parse(snapshot?.cooldownUntil || '');
  if (!Number.isFinite(deadline)) { $('sharedCooldown').textContent='当前无冷却'; clearCooldownTimer(); return; }
  const seconds = Math.max(0, Math.ceil((deadline-Date.now())/1000));
  if (!seconds) { $('sharedCooldown').textContent='等待期已结束，等待状态更新'; clearCooldownTimer(); return; }
  const remaining = `${seconds>=60?Math.floor(seconds/60)+' 分 ':''}${seconds%60} 秒`;
  const reason = Object.hasOwn(cooldownReasons,snapshot.cooldownReason) ? cooldownReasons[snapshot.cooldownReason] : '共享账户等待派发';
  $('sharedCooldown').textContent=`${reason} · 剩余 ${remaining} · 至 ${date(snapshot.cooldownUntil)}`;
}
function startCooldownTimer() {
  clearCooldownTimer();
  renderCooldown();
  if (active && !document.hidden && Date.parse(snapshot?.cooldownUntil || '')>Date.now()) cooldownTimer=setInterval(renderCooldown,1000);
}
function sessionExpired() {
  stopSharedStatus(); $('sharedConnection').textContent='登录已失效，请重新登录';
  window.dispatchEvent(new Event('member-session-expired'));
}
function render(s) {
  snapshot=s;
  $('sharedState').textContent=labels[s.status] || s.status;
  $('sharedSubscription').textContent=s.subscriptionActive && s.status!=='inactive_opus' ? `Opus 有效 · 到期 ${date(s.expiresAt)}` : '没有可用的 Opus 订阅';
  $('sharedQueue').textContent=`生成中 ${n(s.running)} / ${n(s.concurrencyLimit)} · 排队 ${n(s.queued)} · 批次待激活 ${n(s.pending)}`;
  startCooldownTimer();
  $('sharedV5').textContent=s.v5Percent==null ? '未知' : `${n(s.v5Percent)}%${s.v5IsNegative?'（已透支）':''}`;
  $('sharedRefill').textContent=s.refillSecondsPerPercent>0 ? `约 ${n(86400/s.refillSecondsPerPercent)} 个百分点 / 天` : '恢复速度未知或已暂停';
  $('sharedFixed').textContent=`${n(s.anlasFixed)} 订阅`;
  $('sharedPurchased').textContent=`购买 Anlas：${n(s.anlasPurchased)}`;
  $('sharedUpdated').textContent=`官方余额更新：${date(s.officialCheckedAt)}。每 60 秒查询，生成结束后也刷新；队列与账本提交后合并推送，通常在一秒内更新。`;
  const problems=[];if(s.quotaSyncError)problems.push('官方余额同步异常');if(s.stale)problems.push(s.officialCheckedAt?'余额数据已过期，请勿作为当前余量':'尚无共享 Opus 的官方余额数据');if(s.recentFailures)problems.push(`最近一小时失败 ${n(s.recentFailures)} 次`);
  $('sharedError').hidden=!problems.length;$('sharedError').textContent=problems.join(' · ');
  $('sharedConnection').textContent='状态连接正常 · 接收于 '+new Date().toLocaleTimeString('zh-CN');
}
export function startSharedStatus(onInvalidate){
  active=true;
  startMemberEvents({onStatus:render,onInvalidate,onExpired:sessionExpired,onConnection:state=>{
    const labels={connecting:'正在连接共享状态…',open:'状态连接已建立',retrying:'状态连接中断，正在重连；以下为上次接收的数据',closed:'状态连接已关闭',invalid:'状态数据无效，请刷新页面'};
    $('sharedConnection').textContent=labels[state];
    if(state==='connecting'&&snapshot)startCooldownTimer();
  }});
}
export function stopSharedStatus(){active=false;stopMemberEvents();clearCooldownTimer();snapshot=null;}
document.addEventListener('visibilitychange',()=>{if(document.hidden)clearCooldownTimer();});
window.addEventListener('pagehide',clearCooldownTimer);
