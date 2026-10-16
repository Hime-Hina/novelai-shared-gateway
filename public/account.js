import { startSharedStatus, stopSharedStatus } from '/shared-status.js';
const $ = id => document.getElementById(id);
const stateNames = { pending_opus:'等待管理员接入有效 Opus', inactive_opus:'订阅暂不可用', stale:'官方余额等待刷新', unknown_balance:'官方余额未知', ambiguous_opus:'等待管理员选择上游', active:'额度已同步' };
async function api(path, body) {
  const r = await fetch('/api/member/' + path, { method: body === undefined ? 'GET' : 'POST', credentials:'same-origin', headers:body === undefined ? {} : {'Content-Type':'application/json'}, body:body === undefined ? undefined : JSON.stringify(body) });
  const data = await r.json();
  if (!r.ok) { if (r.status === 401 || r.status === 403) { stopSharedStatus(); $('login').hidden=false; $('account').hidden=true; } throw new Error(data.message || data.error || '请求失败'); }
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
async function load() {
  const data = await api('me'), q=data.quota; startSharedStatus();
  $('login').hidden=true; $('account').hidden=false; $('ownerConsole').hidden=data.owner!==true; $('welcome').textContent=data.username+' · 我的额度';
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
  const status={done:'成功',failed:'失败',queued:'排队',running:'生成中'};
  for (const j of data.jobs) {
    const row=document.createElement('tr'),result=textNode('td',status[j.status]||j.status);
    if (j.imageUrl) { const link=resultLink(j.imageUrl,j.lateResultAvailable?'领取迟到结果（退款保留）':'领取结果'); if(link)result.append(textNode('br',''),link); }
    row.append(textNode('td',new Date(j.createdAt).toLocaleString()),textNode('td',j.model),result,textNode('td',chargeText(j.charge))); $('jobs').append(row);
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
}
function run(fn) { return async event => { event?.preventDefault(); $('message').textContent='';try { await fn(); } catch(e) { $('message').textContent=e.message; } }; }
$('loginForm').onsubmit=run(async()=>{ await api('login',{username:$('username').value,password:$('password').value});$('password').value='';await load(); });
$('discordForm').onsubmit=run(async()=>{const invite=$('invite').value.trim();$('discordLogin').disabled=true;try{const result=await api('discord/start',invite?{invite}:{});$('invite').value='';location.assign(result.authorizationUrl);}finally{$('discordLogin').disabled=false;}});
$('refresh').onclick=run(load);
$('logout').onclick=run(async()=>{await api('logout',{});stopSharedStatus();$('apiKey').value='';$('keyPanel').hidden=true;$('login').hidden=false;$('account').hidden=true;});
$('showKey').onclick=run(async()=>{const data=await api('key');$('apiBaseUrl').value=data.baseUrl || '';$('apiKey').value=data.apiKey;$('keyPanel').hidden=false;});
$('hideKey').onclick=()=>{$('keyPanel').hidden=true;$('apiKey').value='';};
$('copyKey').onclick=run(async()=>{await navigator.clipboard.writeText($('apiKey').value);$('message').textContent='已复制自己的 API Key';});
async function initialize(){const options=await api('login-options');if(options.discord&&options.origin&&location.origin!==options.origin){location.replace(options.origin+'/');return;}$('loginForm').hidden=options.discord;$('discordForm').hidden=!options.discord;$('discordLogin').disabled=!options.ready;$('loginHint').textContent=options.discord?(options.ready?'使用 Discord 身份登录；仅限受邀的指定服务器成员。':'Discord 登录尚未配置完成，请联系管理员。'):'账户由管理员创建。登录后查看个人额度和调用记录。';await load().catch(()=>{});}
initialize().catch(error=>{$('message').textContent=error.message;});
