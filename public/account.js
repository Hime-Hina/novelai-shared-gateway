import { startSharedStatus, stopSharedStatus } from '/shared-status.js';
const $ = id => document.getElementById(id);
const stateNames = { pending_opus:'等待管理员接入有效 Opus', inactive_opus:'订阅暂不可用', stale:'官方余额等待刷新', unknown_balance:'官方余额未知', ambiguous_opus:'等待管理员选择上游', active:'额度已同步' };
const jobStatusNames={done:'成功',failed:'失败',pending:'等待前序图片',queued:'排队',running:'生成中'};
let accountViewVersion=0, accountReadVersion=0, accountRefreshing=false, accountRefreshPending=false, accountTimer=null;
const batchViews=new Map();
function clearMemberView(message = '') {
  accountViewVersion++; stopSharedStatus();
  accountRefreshPending=false; clearInterval(accountTimer); accountTimer=null; batchViews.clear();
  $('apiKey').value=''; $('apiBaseUrl').value=''; $('keyPanel').hidden=true;
  $('login').hidden=false; $('account').hidden=true;
  if (message) $('message').textContent=message;
}
window.addEventListener('member-session-expired',()=>clearMemberView('登录已失效，请重新登录。'));
async function api(path, body) {
  const view = accountViewVersion;
  const r = await fetch('/api/member/' + path, { method: body === undefined ? 'GET' : 'POST', credentials:'same-origin', headers:body === undefined ? {} : {'Content-Type':'application/json'}, body:body === undefined ? undefined : JSON.stringify(body) });
  const data = await r.json();
  if (!r.ok) { if (view===accountViewVersion && (r.status === 401 || r.status === 403)) clearMemberView(); throw new Error(data.message || data.error || '请求失败'); }
  return data;
}
const number = n => Number(n || 0).toLocaleString('zh-CN', {maximumFractionDigits:3});
function textNode(tag, text) { const element=document.createElement(tag); element.textContent=text; return element; }
function resultLink(url, label) {
  // 只使用后端签发的领取地址，不在前端拼接成员 Key。
  let target; try { target=new URL(url, location.origin); } catch { return null; }
  if (!['http:', 'https:'].includes(target.protocol)) return null;
  const link=textNode('a', label); link.href=target.href; link.referrerPolicy='no-referrer'; link.rel='noopener noreferrer'; link.target='_blank'; return link;
}
function chargeText(charge) {
  if (!charge) return '—';
  const settlement={refunded:'已退款（本站额度）',settled:'已结算',reserved:'预留'},resource={v5:'V5 百分点',anlas:'Anlas',free:'免费'};
  const pools=charge.resource==='anlas' ? ` · 订阅 ${charge.fixed==null?'未知':number(charge.fixed)} / 购买 ${charge.purchased==null?'未知':number(charge.purchased)}` : '';
  return `${settlement[charge.status]||charge.status} · ${number(charge.amount)} ${resource[charge.resource]||charge.resource}${pools}${charge.billingMode==='anlas_fallback'?'（V5 不足转扣）':''}${charge.estimated?'（估算）':''}`;
}
function batchDetails(requestId) {
  const details=document.createElement('details'),body=document.createElement('div'),hint=textNode('p',''),view=accountViewVersion;
  details.style.maxWidth='min(1012px, calc(100vw - 68px))';
  details.append(textNode('summary','查看批次 · '+requestId),hint,body);hint.setAttribute('role','status');
  let readVersion=0,cancelling=false;
  const current=version=>view===accountViewVersion&&version===readVersion;
  const path='requests/'+encodeURIComponent(requestId);
  function render(request) {
    const c=request.counts,list=document.createElement('ul');
    body.replaceChildren(textNode('p',`共 ${number(c.total)} 张 · 等待前序 ${number(c.pending)} · 排队 ${number(c.queued)} · 生成中 ${number(c.running)} · 成功 ${number(c.done)} · 失败或取消 ${number(c.failed)}`));
    for(const job of request.jobs){
      const status=job.status==='failed'&&job.cancelled?'已取消（尚未派发）':jobStatusNames[job.status]||job.status;
      const item=textNode('li',`第 ${job.index+1} 张 · ${job.model} · ${status}`);
      if(job.error&&job.status==='failed'&&!job.cancelled)item.append(textNode('p',job.error));
      if(job.imageUrl){const link=resultLink(job.imageUrl,job.lateResultAvailable?'领取迟到结果（退款保留）':'领取结果');if(link)item.append(textNode('br',''),link);}
      list.append(item);
    }
    const scope=textNode('p','仅取消此批次排队或等待前序的图片。生成中的图片继续完成，已完成结果仍可领取。');scope.className='notice';
    const refresh=textNode('button','刷新批次'),cancel=textNode('button','取消此批次尚未派发的图片');
    for(const button of [refresh,cancel]){button.type='button';button.className='secondary';}
    cancel.disabled=!request.cancellable;refresh.onclick=run(read);
    cancel.onclick=run(async()=>{
      if(cancelling||!request.cancellable||view!==accountViewVersion)return;
      cancelling=true;cancel.disabled=true;refresh.disabled=true;readVersion++;
      try{
        const result=await api(path+'/cancel',{});
        if(view===accountViewVersion){render(result);hint.textContent='';}
        let refreshFailed=false;try{await load();}catch{refreshFailed=true;}
        $('message').textContent=`此次取消 ${number(result.cancelledCount)} 条尚未派发任务。生成中的图片继续完成，已完成结果仍可领取。${refreshFailed?'额度和最近调用刷新失败，请手动刷新；取消操作已保存。':''}`;
      }finally{cancelling=false;cancel.disabled=!request.cancellable;refresh.disabled=false;}
    });
    body.append(list,scope,refresh,textNode('span',' '),cancel);
  }
  async function read(){
    if(cancelling||view!==accountViewVersion)return;
    const version=++readVersion;hint.textContent='正在读取批次…';
    try{const request=await api(path);if(current(version)&&details.open){render(request);hint.textContent='';}}
    catch(error){if(current(version)){hint.textContent='批次读取失败，可重新展开或刷新重试。';throw error;}}
  }
  details.refresh=read;
  details.ontoggle=run(async()=>{if(details.open)await read();else readVersion++;});
  return details;
}
async function load({automatic=false}={}) {
  const view = automatic ? accountViewVersion : ++accountViewVersion, read=++accountReadVersion;
  const data = await api('me'), q=data.quota;
  if (view !== accountViewVersion || read !== accountReadVersion) return;
  startSharedStatus(onMemberInvalidation);
  if(!automatic)batchViews.clear();
  $('login').hidden=true; $('account').hidden=false; $('ownerConsole').hidden=data.owner!==true; $('welcome').textContent=data.username+' · 我的额度';
  $('sessionExpiry').textContent=data.sessionExpiresAt ? `本次登录有效至 ${new Date(data.sessionExpiresAt).toLocaleString('zh-CN')}，不会自动续期。` : '';
  $('group').textContent=q?.member ? `${q.groupName} · ${q.groupSize} 人 · 个人份额 ${number(q.share*100)}%` : '尚未分配到组，请联系管理员';
  $('status').textContent=(stateNames[q?.status] || '等待分配额度') + (q?.autoAnlasFallback ? ' · V5 不足时自动按 Anlas 转扣（本站记账；上游可能消耗公共 V5）' : '');
  if (data.acceptingJobs === false) $('status').textContent += ' · 管理员已停止接单，已有结果仍可查询和下载';
  $('v5').textContent=`${number(q?.v5Available)} / ${number(q?.v5Capacity)}%`;
  $('v5bar').max=q?.v5Capacity || 1; $('v5bar').value=q?.v5Available || 0;
  const refill = q?.v5RefillLimited ? '恢复时间暂无法可靠估算，请留意共享上游状态' : q?.v5FullRefillSeconds === 0 ? '个人已满额；满额期间不累计额外恢复' : q?.v5RefillPerDay > 0 ? `当前每天约恢复 ${number(q.v5RefillPerDay)} 个百分点，约 ${number(q.v5FullRefillSeconds / 3600)} 小时补满（随成员使用变化）` : '恢复速度等待官方数据';
  $('recovery').textContent=`预留 ${number(q?.v5Reserved)}% · ${refill}`;
  $('fixed').textContent=number(q?.anlasFixedAvailable); $('purchased').textContent=number(q?.anlasPurchasedAvailable);
  $('fixedHeld').textContent=`预留 ${number(q?.anlasFixedReserved)}`; $('purchasedHeld').textContent=`预留 ${number(q?.anlasPurchasedReserved)}`;
  $('jobs').replaceChildren();
  const seenRequests=new Set();
  for (const j of data.jobs) {
    const row=document.createElement('tr'),result=textNode('td',jobStatusNames[j.status]||j.status);
    if (j.imageUrl) { const link=resultLink(j.imageUrl,j.lateResultAvailable?'领取迟到结果（退款保留）':'领取结果'); if(link)result.append(textNode('br',''),link); }
    row.append(textNode('td',new Date(j.createdAt).toLocaleString()),textNode('td',j.model),result,textNode('td',chargeText(j.charge))); $('jobs').append(row);
    if(j.requestId&&!seenRequests.has(j.requestId)){
      seenRequests.add(j.requestId);const batchRow=document.createElement('tr'),cell=document.createElement('td');cell.colSpan=4;cell.style.whiteSpace='normal';cell.style.overflowWrap='anywhere';
      let details=batchViews.get(j.requestId);
      if(!details){details=batchDetails(j.requestId);batchViews.set(j.requestId,details);}
      cell.append(details);batchRow.append(cell);$('jobs').append(batchRow);
      if(automatic&&details.open)details.refresh().catch(()=>{$('memberUpdated').textContent='批次详情刷新失败，保留上次结果，可手动重试。';});
    }
  }
  if (!data.jobs.length) { const row=document.createElement('tr'),cell=document.createElement('td');cell.colSpan=4;cell.textContent='暂无调用记录';row.append(cell);$('jobs').append(row); }
  $('lateResults').replaceChildren();
  for (const j of data.lateResults || []) {
    if (!j.imageUrl) continue;
    const link=resultLink(j.imageUrl,'领取结果'); if(!link)continue;
    const item=textNode('li',`${j.model} · 提交 ${new Date(j.createdAt).toLocaleString()} · 找回 ${new Date(j.lateResultAt).toLocaleString()} · ${chargeText(j.charge)} · `);
    item.append(link); $('lateResults').append(item);
  }
  $('lateResultsPanel').hidden=!$('lateResults').children.length;
  for(const id of batchViews.keys())if(!seenRequests.has(id))batchViews.delete(id);
  $('memberUpdated').textContent='个人额度与调用更新于 '+new Date().toLocaleTimeString('zh-CN');
  startAccountTimer();
}
async function refreshVisibleAccount(){
  if(accountRefreshing||document.hidden||$('account').hidden)return;
  accountRefreshing=true;
  try{
    do{accountRefreshPending=false;await load({automatic:true});}
    while(accountRefreshPending&&!document.hidden&&!$('account').hidden);
  }catch{if(!$('account').hidden)$('memberUpdated').textContent='自动刷新失败，显示上次结果；连接恢复后重试，也可手动刷新。';}
  finally{accountRefreshing=false;}
}
function onMemberInvalidation(event){
  if(!event.resync&&!event.scopes.includes('member'))return;
  accountRefreshPending=true;void refreshVisibleAccount();
}
function startAccountTimer(){
  if(accountTimer!==null||document.hidden||$('account').hidden)return;
  // 自然恢复也会改变可用额度；仅可见页面低频估算，不触发上游生图或余额查询。
  accountTimer=setInterval(()=>{accountRefreshPending=true;void refreshVisibleAccount();},30000);
}
function stopAccountTimer(){clearInterval(accountTimer);accountTimer=null;}
document.addEventListener('visibilitychange',()=>{if(document.hidden)stopAccountTimer();else startAccountTimer();});
window.addEventListener('pagehide',stopAccountTimer);
window.addEventListener('pageshow',startAccountTimer);
function run(fn) { return async event => { event?.preventDefault(); $('message').textContent='';try { await fn(); } catch(e) { $('message').textContent=e.message; } }; }
$('loginForm').onsubmit=run(async()=>{ await api('login',{username:$('username').value,password:$('password').value});$('password').value='';await load(); });
$('discordForm').onsubmit=run(async()=>{const invite=$('invite').value.trim();$('discordLogin').disabled=true;try{const result=await api('discord/start',invite?{invite}:{});$('invite').value='';location.assign(result.authorizationUrl);}finally{$('discordLogin').disabled=false;}});
$('refresh').onclick=run(load);
$('logout').onclick=run(async()=>{await api('logout',{});clearMemberView();});
$('showKey').onclick=run(async()=>{const view=accountViewVersion,data=await api('key');if(view!==accountViewVersion)return;$('apiBaseUrl').value=data.baseUrl || '';$('apiKey').value=data.apiKey;$('keyPanel').hidden=false;});
$('hideKey').onclick=()=>{$('keyPanel').hidden=true;$('apiKey').value='';};
$('copyKey').onclick=run(async()=>{await navigator.clipboard.writeText($('apiKey').value);$('message').textContent='已复制自己的 API Key';});
async function initialize(){const options=await api('login-options');if(options.discord&&options.origin&&location.origin!==options.origin){location.replace(options.origin+'/');return;}$('loginForm').hidden=options.discord;$('discordForm').hidden=!options.discord;$('discordLogin').disabled=!options.ready;$('loginHint').textContent=options.discord?(options.ready?'使用 Discord 身份登录；仅限受邀的指定服务器成员。':'Discord 登录尚未配置完成，请联系管理员。'):'账户由管理员创建。登录后查看个人额度和调用记录。';await load().catch(()=>{});}
initialize().catch(error=>{$('message').textContent=error.message;});
