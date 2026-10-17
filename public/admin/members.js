import { startMemberEvents, stopMemberEvents } from '/member-events.js';
const $=id=>document.getElementById(id);
const ownerMode=location.pathname.startsWith('/owner');
let usage=null;
let state=null,upstream=null,publicBase='',draft=[],draftConfigVersion=null,dirty=false,preview=null,confirmTask=null,busy=false;
let allocationMode='direct',directShares={},redistributeByMode={direct:true,advanced:false};
let losses=null,lossEvents=[],lossNextCursor=null,lossPaged=false,lossRead=0,lossPolicyVersion=null,lossPolicyDirty=false,lossPolicyEdit=0;
let pageVersion=0,expired=false,eventsStarted=false,connection='connecting',lastUpdatedAt=null,readFailed=false,fallbackDirty=false;
let refreshTask=null,refreshPending=false,refreshRequired=false,usageTask=null,usagePending=false,lossTask=null,lossPending=false,lossAppendPending=false;
const number=v=>v===null||v===undefined?'—':Number(v).toLocaleString('zh-CN',{maximumFractionDigits:3});
const percent=v=>number(Number(v||0)*100)+'%';
const statusNames={active:'可用',pending_opus:'等待接入',inactive_opus:'已暂停',stale:'等待同步',quota_sync_error:'同步失败',cooldown:'冷却中',unknown_balance:'余额未知'};
const displayTime=v=>v?new Date(v).toLocaleString('zh-CN'):'尚未同步';
function notice(text,error=false){$('message').textContent=text;$('message').hidden=!text;$('message').classList.toggle('error',error);}
function connectionHint(){
 const status=expired?'登录已失效，请返回首页重新登录':!ownerMode||connection==='open'?'已连接':connection==='connecting'?'正在连接实时更新':'实时连接已断开，当前为旧数据';
 $('connectedHint').textContent=status+(lastUpdatedAt?' · 管理摘要更新 '+displayTime(lastUpdatedAt):'')+(readFailed?' · 部分数据读取失败，请重试':'');
}
function expireOwnerSession(){
 if(expired)return;expired=true;pageVersion++;refreshPending=false;refreshRequired=false;usagePending=false;lossPending=false;
 stopMemberEvents();eventsStarted=false;
 for(const id of ['deliveryText','password','upstreamPst','adminToken'])$(id).value='';
 document.querySelectorAll('dialog[open]').forEach(dialog=>dialog.close());
 state=null;upstream=null;usage=null;publicBase='';draft=[];preview=null;dirty=false;lossRead++;losses=null;lossEvents=[];lossPolicyDirty=false;confirmTask=null;
 $('dashboard').hidden=true;$('topActions').hidden=true;$('ownerSignin').hidden=false;
 connectionHint();notice('登录已失效，请返回首页重新登录。',true);
}
function startOwnerEvents(){
 if(!ownerMode||eventsStarted||expired)return;eventsStarted=true;
 startMemberEvents({
  onStatus(shared){if(state&&!expired){state.shared=shared;updateOverview();}},
  onInvalidate(event){if(event.resync||event.scopes.includes('owner'))requestRefresh();},
  onExpired:expireOwnerSession,
  onConnection(value){connection=value;connectionHint();}
 });
}
function flushRefresh(){
 if(!refreshPending||refreshTask||busy||document.hidden||!state||expired)return;
 refresh({reset:false,background:true}).catch(()=>{if(!expired){readFailed=true;connectionHint();}});
}
function requestRefresh(){if(!state||expired)return;refreshPending=true;flushRefresh();}
async function api(path='/api/admin/member-management',body,method){
 const version=pageVersion;
 if(expired)throw Object.assign(new Error('登录已失效，请重新登录。'),{code:'STALE_RESPONSE'});
 if(ownerMode)path=path.replace('/api/admin/','/api/member/owner/');
 const response=await fetch(path,{method:method||(body===undefined?'GET':'POST'),headers:{...(!ownerMode?{'x-admin-token':$('adminToken').value}:{}),'Content-Type':'application/json'},body:body===undefined?undefined:JSON.stringify(body)});
 let result;try{result=await response.json();}catch{throw new Error('响应异常，请检查管理隧道是否连接。');}
 if(version!==pageVersion)throw Object.assign(new Error('管理会话已结束。'),{code:'STALE_RESPONSE'});
 if(ownerMode&&[401,403].includes(response.status))expireOwnerSession();
 if(!response.ok)throw Object.assign(new Error(result.message||result.error?.message||result.error||'请求失败'),{statusCode:response.status});return result;
}
const manage=body=>api('/api/admin/member-management',body);
const duration=value=>value===null||value===undefined?'—':number(value)+' 秒';
function loadUsage(){
 if(usageTask){usagePending=true;return usageTask;}
 usageTask=(async()=>{do{usagePending=false;await readUsage();}while(usagePending&&!document.hidden&&state&&!expired);})().finally(()=>{usageTask=null;});
 return usageTask;
}
async function readUsage(){
 const days=$('usageDays').value,result=await api('/api/admin/statistics?days='+days);
 if(days!==$('usageDays').value)return;
 usage=result;const t=result.totals;
 $('usageDone').textContent=t.done+' / '+t.total;$('usageFree').textContent='其中免费成功 '+t.free+' 次';
 $('usageFailed').textContent=t.failed+' / '+t.refunded;$('usageActive').textContent='生成中 '+t.running+' · 排队 '+t.queued+' · 等待前序图片 '+t.pending;
 $('usageV5').textContent=number(t.v5);$('usageWait').textContent='平均排队 '+duration(t.averageWaitSeconds);
 $('usageAnlas').textContent=number(t.fixed+t.purchased);$('usagePools').textContent='订阅 '+number(t.fixed)+' · 购买 '+number(t.purchased);
 renderUsage();$('usageRecent').replaceChildren();
 const statuses={done:'成功',failed:'失败',pending:'等待前序图片',running:'生成中',queued:'排队'},settlements={settled:'已结算',refunded:'已退款',reserved:'预扣'};
 for(const j of result.recent){
  const row=node('tr'),charge=j.resource==='free'?'免费':j.resource==='v5'?number(j.amount)+' V5':j.resource==='anlas'?'订阅 '+number(j.fixed)+' / 购买 '+number(j.purchased)+' Anlas':'未扣款';
  for(const value of [displayTime(j.createdAt),j.username,j.model,statuses[j.status]||j.status,duration(j.waitSeconds),duration(j.generationSeconds),(settlements[j.chargeStatus]||'')+' '+charge+(j.errorStatus?' · HTTP '+j.errorStatus:'')])row.append(node('td',value));
  $('usageRecent').append(row);
 }
 if(!result.recent.length){const row=node('tr'),cell=node('td','所选范围暂无请求','empty');cell.colSpan=7;row.append(cell);$('usageRecent').append(row);}
}
function renderUsage(){
 $('usageRows').replaceChildren();if(!usage)return;
 for(const r of usage[$('usageGroup').value]){const row=node('tr');for(const value of [r.label,r.total,r.done,r.failed,r.free,number(r.v5),number(r.fixed),number(r.purchased),duration(r.averageWaitSeconds),duration(r.averageGenerationSeconds)])row.append(node('td',value));$('usageRows').append(row);}
 if(!$('usageRows').children.length){const row=node('tr'),cell=node('td','所选范围暂无记录','empty');cell.colSpan=10;row.append(cell);$('usageRows').append(row);}
}
$('usageDays').onchange=()=>loadUsage().catch(e=>notice(e.message,true));
$('usageGroup').onchange=renderUsage;
function run(fn){return async event=>{event?.preventDefault();if(busy)return;busy=true;const button=event?.currentTarget;const wasDisabled=button?.disabled;if(button?.tagName==='BUTTON')button.disabled=true;notice('');try{await fn(event);}catch(e){notice(e.message,true);}finally{busy=false;if(button?.tagName==='BUTTON')button.disabled=wasDisabled;flushRefresh();}};}
function node(tag,text,cls){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;}
function option(value,text){const n=node('option',text);n.value=value;return n;}
function groupOf(id,groups=state.groups){return groups.find(g=>g.members.includes(id));}
function modal(id){$(id).showModal();}
function markDirty(){dirty=true;preview=null;$('draftHint').textContent='有未保存的修改 · 请先预览';updateGroupCounts();}
function tab(id){document.querySelectorAll('[data-tab]').forEach(b=>b.setAttribute('aria-selected',String(b.dataset.tab===id)));for(const panel of ['membersPanel','groupsPanel','settingsPanel','usagePanel'])$(panel).hidden=panel!==id;if(id==='usagePanel')loadUsage().catch(e=>notice(e.message,true));}
function updateOverview(){const s=state.shared||{};$('overviewStatus').textContent=statusNames[s.status||state.status]||'待核验';$('overviewDate').textContent='官方查询 '+displayTime(s.officialCheckedAt);$('overviewV5').textContent=number(s.v5Percent)+'%';$('overviewBar').value=s.v5Percent||0;$('overviewRecovery').textContent=s.refillSecondsPerPercent>0?'每恢复 1% 约 '+number(s.refillSecondsPerPercent/60)+' 分钟':'恢复周期等待官方数据';$('overviewAnlas').textContent=number(s.anlasFixed);$('overviewPurchased').textContent='购买余额 '+number(s.anlasPurchased);$('overviewQueue').textContent=(s.running||0)+' / '+(s.queued||0);$('overviewMembers').textContent=state.users.filter(u=>u.enabled).length+' 位启用成员 · 串行，结束后间隔至少 10 秒';}
function renderMembers(){
 const query=$('search').value.trim().toLowerCase(),filter=$('filterGroup').value;
 const users=state.users.filter(user=>(user.username||user.note||user.id).toLowerCase().includes(query)
  &&(!filter||(filter==='unassigned'?!groupOf(user.id):groupOf(user.id)?.id===filter)));
 $('memberCount').textContent=users.length+' / '+state.users.length+' 位成员';
 $('members').replaceChildren();
 for(const user of users){
  const row=node('tr'),name=node('td',user.username||user.note||user.id);
  name.append(node('span',user.enabled?'已启用':'已停用','badge '+(user.enabled?'ok':'off')));
  if(user.acceptingJobs===false)name.append(node('span','已停止接单','cell-sub'));
  if(state.discordLogin)name.append(node('span',user.discordId?'Discord '+user.discordId:'尚未绑定 Discord','cell-sub'));
  const group=node('td',groupOf(user.id)?.name||'未分组');
  group.append(node('span',percent(user.quota?.share),'cell-sub'));
  row.append(name,group,node('td',number(user.quota?.v5Available)),
   node('td',number(user.quota?.anlasFixedAvailable)),node('td',number(user.quota?.anlasPurchasedAvailable)));
  const actions=node('td'),wrap=node('div',undefined,'row-actions');
  const buttons=[
   ['分发信息',()=>delivery(user)],
   state.discordLogin?['邀请码',()=>inviteMember(user)]:['重置密码',()=>editMember(user)],
   ['调整额度',()=>openTransfer(user)],
   ['删除',()=>openDelete(user)],
   [user.acceptingJobs===false?'恢复接单':'停止接单',()=>confirmMemberAdmission(user)],
   [user.enabled?'停用':'恢复',()=>confirmMemberStatus(user)]
  ];
  for(const [text,action] of buttons){
   const button=node('button',text,'secondary');
   button.type='button';
   button.onclick=run(action);
   if(ownerMode&&user.id===state.ownerUserId&&['删除','停用'].includes(text))button.disabled=true;
   if((text==='重置密码'||text==='邀请码')&&!user.enabled||text==='邀请码'&&user.discordId)button.disabled=true;
   wrap.append(button);
  }
  actions.append(wrap);
  row.append(actions);
  $('members').append(row);
 }
 if(!users.length){
  const cell=node('td','没有符合条件的成员','empty');
  cell.colSpan=6;
  const row=node('tr');
  row.append(cell);
  $('members').append(row);
 }
}
function renderDraft(){
 // 版本跟随开始编辑时的草稿，不能用后来的查询或预览响应给旧草稿换版本。
 draftConfigVersion=state.configVersion;
 draft=structuredClone(state.groups);
 for(const g of draft){g.share??=.25;g.memberWeights??={};}
 dirty=false;
 preview=null;
 redistributeByMode={direct:true,advanced:false};
 $('redistribute').checked=true;
 updateRedistributeHint();
 $('draftHint').textContent='尚无修改';
 allocationMode='direct';
 $('allocationMode').value='direct';
 $('directAllocation').hidden=false;
 $('advancedAllocation').hidden=true;
 renderAdvancedDraft();
 renderDirectDraft();
}
function renderAdvancedDraft(){
 $('groupCards').replaceChildren();$('assignments').replaceChildren();
 for(const g of draft){const card=node('div',undefined,'group-card'),label=node('label','组名'),input=node('input');input.value=g.name;input.maxLength=60;input.setAttribute('aria-label','编辑'+g.name);input.oninput=()=>{g.name=input.value;markDirty();};label.append(input);const count=node('strong');count.dataset.countGroup=g.id;const shareLabel=node('label','总份额（%）'),share=node('input');share.type='number';share.min='0';share.max='100';share.step='any';share.value=Number((g.share*100).toFixed(8));share.setAttribute('aria-label',g.name+' 总份额百分比');share.oninput=()=>{g.share=share.value===''?NaN:Number(share.value)/100;markDirty();};shareLabel.append(share);card.append(label,count,shareLabel);$('groupCards').append(card);}
 for(const u of state.users){const row=node('tr');row.append(node('td',(u.username||u.id)+(u.enabled?'':'（停用）')));const td=node('td'),select=node('select');select.setAttribute('aria-label',(u.username||u.id)+' 的目标分组');select.append(option('','不在任何组'));for(const g of draft)select.append(option(g.id,g.name));select.value=groupOf(u.id,draft)?.id||'';select.disabled=!u.enabled;const weightTd=node('td'),weight=node('input');weight.type='number';weight.min='0';weight.max='1000000';weight.step='any';weight.value=groupOf(u.id,draft)?.memberWeights[u.id]??1;weight.setAttribute('aria-label',(u.username||u.id)+' 的组内权重');weight.disabled=!select.value;weight.style.width='100px';select.onchange=()=>{const value=Number.isFinite(Number(weight.value))?Number(weight.value):1;for(const g of draft){g.members=g.members.filter(id=>id!==u.id);delete g.memberWeights[u.id];}const target=draft.find(g=>g.id===select.value);if(target){target.members.push(u.id);target.memberWeights[u.id]=value;}weight.disabled=!target;markDirty();};weight.oninput=()=>{const g=groupOf(u.id,draft);if(g)g.memberWeights[u.id]=weight.value===''?NaN:Number(weight.value);markDirty();};td.append(select);weightTd.append(weight);const after=node('td');after.dataset.shareUser=u.id;row.append(td,weightTd,node('td',percent(u.quota?.share)),after);$('assignments').append(row);}updateGroupCounts();
}
function updateGroupCounts(){
 const total=draft.reduce((n,g)=>n+g.share,0);$('shareTotal').textContent='四组总份额：'+percent(total)+'（必须为 100%）';$('shareTotal').classList.toggle('danger',!Number.isFinite(total)||Math.abs(total-1)>1e-8);
 for(const g of draft){const el=document.querySelector('[data-count-group="'+g.id+'"]');if(el)el.textContent=g.members.length+' 人';}
 for(const el of document.querySelectorAll('[data-share-user]')){const g=groupOf(el.dataset.shareUser,draft),sum=g?.members.reduce((n,id)=>n+Number(g.memberWeights[id]??1),0);const share=g&&sum>0?g.share*Number(g.memberWeights[el.dataset.shareUser]??1)/sum:0;el.textContent=percent(share)+' · V5 容量 '+number(100*share);}
}
function updateRedistributeHint(){$('balancePreserveWarning').hidden=$('redistribute').checked;}
function validateDraft(){
 if(allocationMode==='direct'){const values=Object.values(directShares);if(values.some(v=>!Number.isFinite(v)||v<0||v>100))throw new Error('每位成员的份额必须在 0% 到 100% 之间。');if(Math.abs(values.reduce((n,v)=>n+v,0)-100)>1e-6)throw new Error('成员份额合计必须为 100%，请调整后再预览。');}
 if(draft.some(g=>!Number.isFinite(g.share)||g.share<0||g.share>1)||Math.abs(draft.reduce((n,g)=>n+g.share,0)-1)>1e-8)throw new Error('四组份额须合计 100%。');
 if(draft.some(g=>!g.members.length&&g.share!==0))throw new Error('空组份额须为 0%。');
 if(!draft.some(g=>g.members.length))throw new Error('至少保留一位已分组成员。');
 for(const g of draft){let total=0;for(const id of g.members){const weight=Number(g.memberWeights[id]??1);if(!Number.isFinite(weight)||weight<0||weight>1e6)throw new Error('组内权重须在 0 到 1000000 之间。');total+=weight;}if(g.share>0&&total<=0)throw new Error('有份额的组至少一位成员权重须大于 0。');}
}
// 两种界面共用同一份草稿；切换时由草稿重新计算，避免旧表单覆盖修改。
function draftUserShare(id){const g=groupOf(id,draft),sum=g?.members.reduce((n,key)=>n+Number(g.memberWeights[key]??1),0);return g&&sum>0?g.share*Number(g.memberWeights[id]??1)/sum:0;}
function renderDirectDraft(){directShares={};$('directRows').replaceChildren();for(const u of state.users){const value=Number((draftUserShare(u.id)*100).toFixed(10));directShares[u.id]=value;const row=node('tr'),td=node('td'),input=node('input');input.type='number';input.min='0';input.max='100';input.step='any';input.value=value;input.disabled=!u.enabled&&!groupOf(u.id,draft);input.style.width='130px';input.setAttribute('aria-label',(u.username||u.id)+' 的具体总份额百分比');input.oninput=()=>{directShares[u.id]=input.value===''?NaN:Number(input.value);applyDirectShares();markDirty();updateDirectTotal();};td.append(input);if(input.disabled)td.append(node('span','先启用再分配','cell-sub'));row.append(node('td',(u.username||u.id)+(u.enabled?'':'（停用）')),node('td',percent(u.quota?.share)),td,node('td',number(u.quota?.v5Available)));$('directRows').append(row);}updateDirectTotal();}
function applyDirectShares(){for(const u of state.users)if(u.enabled&&!groupOf(u.id,draft))draft[0].members.push(u.id);for(const g of draft){g.memberWeights={};let total=0;for(const id of g.members){const value=directShares[id]??0;g.memberWeights[id]=value;total+=value;}g.share=total/100;}}
function updateDirectTotal(){const total=Object.values(directShares).reduce((n,v)=>n+v,0);$('directTotal').textContent='已分配 '+number(total)+'% / 100%'+(Number.isFinite(total)?' · '+(total<=100?'还需分配 '+number(100-total)+'%':'超出 '+number(total-100)+'%'):' · 请填写有效数字');$('directTotal').classList.toggle('danger',!Number.isFinite(total)||Math.abs(total-100)>1e-6);}
$('allocationMode').onchange=()=>{redistributeByMode[allocationMode]=$('redistribute').checked;allocationMode=$('allocationMode').value;$('redistribute').checked=redistributeByMode[allocationMode];preview=null;updateRedistributeHint();$('directAllocation').hidden=allocationMode!=='direct';$('advancedAllocation').hidden=allocationMode!=='advanced';if(allocationMode==='direct')renderDirectDraft();else renderAdvancedDraft();};
// 自动刷新发出后也可能开始编辑；响应回来时再次检查，保留草稿及其原版本。
function refresh({reset=true,background=false}={}){
 if(refreshTask){refreshPending=true;if(!background)refreshRequired=true;return refreshTask;}
 refreshTask=(async()=>{
  do{
   refreshPending=false;refreshRequired=false;
   await readRefresh({reset,background});
   reset=false;background=true;
  }while(refreshPending&&(!busy||refreshRequired)&&!document.hidden&&!expired);
 })().finally(()=>{refreshTask=null;});
 return refreshTask;
}
async function readRefresh({reset,background}){
 state=await manage();
 $('memberHelp').textContent=state.discordLogin
  ?'首次登录发放一次性邀请码，成员通过 Discord 领取自己的额度。'
  :'创建、分发、重置密码，都在成员这一行完成。';
 state.users.sort((a,b)=>(a.username||a.id).localeCompare(b.username||b.id));
 if(!fallbackDirty)$('autoFallback').checked=state.autoAnlasFallback===true;
 const filter=$('filterGroup').value;
 $('filterGroup').replaceChildren(option('','全部分组'),option('unassigned','未分组'));
 for(const g of state.groups)$('filterGroup').append(option(g.id,g.name));
 $('filterGroup').value=filter;
 $('dashboard').hidden=false;
 $('connect').hidden=true;
 $('topActions').hidden=false;
 $('ownerSignin').hidden=true;
 lastUpdatedAt=new Date().toISOString();readFailed=false;connectionHint();
 updateOverview();
 renderMembers();
 if(reset&&!dirty)renderDraft();
 // 损耗查询独立返回，不阻塞主要摘要和默认用量页。
 loadLosses().catch(showLossReadError);
 if(ownerMode&&!$('usagePanel').hidden)await loadUsage();
 try{
  if(!background||!$('settingsPanel').hidden)await loadUpstream();
 }catch(e){
  if(!expired){readFailed=true;connectionHint();notice('成员数据已刷新；上游状态读取失败：'+e.message,true);}
 }
 startOwnerEvents();
}
async function loadUpstream(){const summary=await api('/api/admin/summary');const id=state.users.find(u=>u.quota?.boundAccountId)?.quota.boundAccountId;const a=summary.accounts?.find(a=>a.id===id);publicBase=String(state.publicBaseUrl||summary.settings?.publicBaseUrl||location.origin).replace(/\/$/,'');upstream=a?{id,enabled:a.enabled===true,tier:a.quotaTier,inFlight:Number(a.inFlight||0),fixed:a.quotaFixed,purchased:a.quotaPurchased,v5:a.v5UsagePercent,checked:a.quotaCheckedAt,error:Boolean(a.quotaError)}:null;for(const id of ['refreshUpstream','toggleUpstream','saveUpstreamPst'])$(id).disabled=!upstream;if(!upstream){$('upstreamStatus').textContent='尚未绑定共享 Opus';return;}$('upstreamStatus').textContent=(upstream.enabled?'已启用':'已暂停')+' · '+(upstream.tier===3?'Opus':'订阅待核验')+' · 正在生成 '+upstream.inFlight;$('upstreamQuota').textContent='V5 '+number(upstream.v5)+'% · 订阅 '+number(upstream.fixed)+' · 购买 '+number(upstream.purchased);$('upstreamUpdated').textContent='官方查询 '+displayTime(upstream.checked)+(upstream.error?' · 查询失败':'');$('toggleUpstream').textContent=upstream.enabled?'暂停上游':'恢复上游';}
function requireClean(){if(dirty)throw new Error('分组有未保存修改，请先预览保存或撤销修改。');}
async function inviteMember(user){requireClean();const invitation=await manage({action:'discord-invite',userId:user.id});const portal=String(state.publicBaseUrl||publicBase).replace(/\/$/,'');$('deliveryText').value=['成员入口：'+portal+'/','成员：'+user.username,'邀请码：'+invitation.code,'有效期：'+displayTime(invitation.expiresAt),'首次登录填写邀请码，再通过 Discord 验证指定服务器成员身份。','邀请码仅可使用一次；重新生成会撤销旧邀请码。绑定后直接使用 Discord 登录。'].join('\n');modal('deliveryDialog');}
function randomPassword(){const bytes=crypto.getRandomValues(new Uint8Array(24));$('password').value=Array.from(bytes,b=>b.toString(16).padStart(2,'0')).join('');notice('随机密码已生成，保存后可复制分发信息。');}
function editMember(user){requireClean();$('passwordLabel').hidden=state.discordLogin===true;$('password').required=!state.discordLogin;$('editingId').value=user?.id||'';$('username').value=user?.username||'';$('username').readOnly=Boolean(user);$('password').value='';$('password').type='password';$('showPassword').textContent='显示';$('createGroupLabel').hidden=Boolean(user);$('createGroup').required=false;$('createGroup').replaceChildren(option('','暂不入组，稍后分配'));for(const g of state.groups){const item=option(g.id,g.name+(g.members.length?'':'（先在分组页分配份额）'));item.disabled=!g.members.length;$('createGroup').append(item);}$('memberDialogTitle').textContent=user?'重置 '+user.username+' 的密码':'创建成员';$('saveMember').textContent=user?'保存新密码':'创建并生成分发信息';$('credentialHint').textContent=user?'原 API Key 保持不变，旧登录会话将失效。':'可加入已有分组，或暂不入组。新成员当前余额为 0；要启用空组，请在分组页同时分配成员与组份额。';if(state.discordLogin){$('credentialHint').textContent='创建后生成一次性 Discord 邀请码；新成员余额为 0，份额由管理员分配。';$('saveMember').textContent='创建成员并生成邀请码';}modal('memberDialog');}
async function delivery(user,password){const key=await manage({action:'member-key',userId:user.id});const portal=String(state.publicBaseUrl||publicBase||location.origin).replace(/\/$/,'');$('deliveryText').value=['成员入口：'+portal+'/','用户名：'+user.username,state.discordLogin?'登录方式：Discord（首次登录需要管理员邀请码）':'密码：'+(password||'使用此前分配的密码；忘记请联系管理员重置'),'API Base：'+portal+'/v1','API Key：'+key.apiKey].join('\n');modal('deliveryDialog');}
function confirmMemberStatus(u){requireClean();$('confirmTitle').textContent=(u.enabled?'停用 ':'恢复 ')+u.username;$('confirmDescription').textContent=u.enabled?'将禁止该成员登录和新的 API 调用。余额、分组、份额保留，不会分给其他成员。':'恢复该成员登录及 API 调用，继续使用原 Key 和保留额度。';confirmTask=async()=>{await manage({action:'member-status',userId:u.id,enabled:!u.enabled});$('confirmDialog').close();await refresh();notice('成员状态已更新。');};modal('confirmDialog');}
function confirmMemberAdmission(user){
 requireClean();
 const acceptingJobs=user.acceptingJobs===false;
 $('confirmTitle').textContent=(acceptingJobs?'恢复接单：':'停止接单：')+(user.username||user.id);
 $('confirmDescription').textContent=acceptingJobs
  ?'允许提交新任务。此前取消的等待任务和旧批次剩余图片不会恢复，请重新提交。'
  :'将立即禁止新任务并取消未派发任务、退回其预留。在途生成正常完成和结算；成员仍可登录、查询及下载已有结果。';
 confirmTask=async()=>{
  const result=await manage({action:'member-admission',userId:user.id,acceptingJobs});
  $('confirmDialog').close();
  await refresh();
  notice(acceptingJobs?'已恢复接单。':'已停止接单，取消 '+result.cancelledCount+' 个未派发任务。');
 };
 modal('confirmDialog');
}
async function previewGroups(){
 if(allocationMode==='direct')applyDirectShares();
 validateDraft();
 const body={
  action:'preview-groups',
  groups:structuredClone(draft),
  redistributeRemaining:$('redistribute').checked,
  expectedConfigVersion:draftConfigVersion
 };
 preview=null;
 const result=await manage(body);
 // 预览与保存使用同一编辑基线；重新预览不能给过期草稿换成最新版本。
 preview={...body,action:'groups'};
 $('previewRows').replaceChildren();
 for(const u of state.users){
  const after=result.users.find(v=>v.id===u.id)?.quota||{},before=u.quota||{};
  const tr=node('tr');
  const values=[
   u.username||u.id,
   percent(before.share)+' → '+percent(after.share),
   number(before.v5Available)+' / '+number(before.v5Capacity)+' → '+number(after.v5Available)+' / '+number(after.v5Capacity),
   number(before.anlasFixedAvailable)+' → '+number(after.anlasFixedAvailable),
   number(before.anlasPurchasedAvailable)+' → '+number(after.anlasPurchasedAvailable)
  ];
  for(const text of values)tr.append(node('td',text));
  $('previewRows').append(tr);
 }
 $('previewDescription').textContent=body.redistributeRemaining
  ?'将保留每组总余额，按组内权重重新分配剩余余额；跨组转移请使用调整额度。确认后按下表调整。'
  :'仅以后生效：只调整分组、未来恢复与新增额度分配；当前余额保持原样，不会按新份额重新分配。';
 modal('previewDialog');
}
$('connect').onsubmit=run(async()=>{await refresh();if($('remember').checked)sessionStorage.setItem('nai.adminSession',$('adminToken').value);else sessionStorage.removeItem('nai.adminSession');});
$('disconnect').onclick=()=>{if(ownerMode){stopMemberEvents();location.href='/';return;}pageVersion++;refreshPending=false;refreshRequired=false;sessionStorage.removeItem('nai.adminSession');localStorage.removeItem('nai.adminToken');$('adminToken').value='';$('deliveryText').value='';$('password').value='';$('upstreamPst').value='';document.querySelectorAll('dialog[open]').forEach(d=>d.close());state=null;upstream=null;preview=null;draft=[];dirty=false;fallbackDirty=false;lossRead++;losses=null;lossEvents=[];lossNextCursor=null;lossPaged=false;lossPolicyDirty=false;lossPolicyVersion=null;$('dashboard').hidden=true;$('topActions').hidden=true;$('connect').hidden=false;notice('已断开管理会话。');};
$('refresh').onclick=run(async()=>{requireClean();await refresh();notice('状态已刷新。');});
$('search').oninput=renderMembers;$('filterGroup').onchange=renderMembers;
for(const b of document.querySelectorAll('[data-tab]'))b.onclick=()=>tab(b.dataset.tab);
for(const b of document.querySelectorAll('[data-close]'))b.onclick=()=>$(b.dataset.close).close();
$('deliveryDialog').addEventListener('close',()=>{$('deliveryText').value='';});$('memberDialog').addEventListener('close',()=>{$('password').value='';});
$('newMember').onclick=run(()=>editMember());$('generatePassword').onclick=randomPassword;$('showPassword').onclick=()=>{const show=$('password').type==='password';$('password').type=show?'text':'password';$('showPassword').textContent=show?'隐藏':'显示';};
$('credentials').onsubmit=run(async()=>{const id=$('editingId').value,password=$('password').value;const created=await manage({action:'credentials',userId:id,username:$('username').value,password,...(!id&&$('createGroup').value?{groupId:$('createGroup').value}:{})});$('memberDialog').close();let refreshFailed=false;try{await refresh();}catch{refreshFailed=true;}notice((id?'新密码已保存；原 API Key 不变。':($('createGroup').value?'成员已创建并加入分组。':'成员已创建，尚未入组，请到分组页设置份额。'))+(refreshFailed?' 页面刷新失败，请稍后手动刷新。':''),refreshFailed);try{if(state.discordLogin)await inviteMember({id:created.id,username:created.username});else await delivery({id:created.id,username:created.username},password);}catch{notice('账户已保存，但分发信息读取失败。请在该成员行重新生成邀请码或打开分发信息。',true);}});
$('copyDelivery').onclick=run(async()=>{await navigator.clipboard.writeText($('deliveryText').value);notice('已复制这一位成员的登录与 API 配置。');});
$('resetDraft').onclick=()=>{renderDraft();notice('未保存的分组修改已撤销。');};$('redistribute').onchange=()=>{redistributeByMode[allocationMode]=$('redistribute').checked;markDirty();updateRedistributeHint();};$('previewGroups').onclick=run(previewGroups);
$('confirmGroups').onclick=run(async()=>{if(!preview)throw new Error('请重新预览');await manage(preview);preview=null;dirty=false;$('previewDialog').close();try{await refresh();}catch{notice('分组已保存，但页面刷新失败，请稍后手动刷新。',true);return;}notice('分组与额度分配已保存。');});
$('confirmAction').onclick=run(async()=>{if(confirmTask)await confirmTask();});$('confirmDialog').addEventListener('close',()=>{confirmTask=null;});
$('autoFallback').onchange=()=>{fallbackDirty=true;};
$('saveFallback').onclick=run(async()=>{const enabled=$('autoFallback').checked;await manage({action:'fallback-policy',enabled});state.autoAnlasFallback=enabled;if($('autoFallback').checked===enabled)fallbackDirty=false;notice('续用策略已保存，仅影响之后的任务。');});
$('refreshUpstream').onclick=run(async()=>{requireClean();if(!upstream)throw new Error('未绑定上游');const result=await api('/api/admin/accounts/quota',{ids:[upstream.id]});await refresh();notice(result.failed?'官方查询失败，请检查凭据。':'已向官方刷新额度。',Boolean(result.failed));});
$('toggleUpstream').onclick=run(async()=>{requireClean();if(!upstream)throw new Error('未绑定上游');const enabled=!upstream.enabled;if(enabled&&!ownerMode){const r=await api('/api/admin/accounts/quota',{ids:[upstream.id]});if(r.failed)throw new Error('官方查询失败，保持暂停。');}await api('/api/admin/accounts/'+encodeURIComponent(upstream.id),{enabled},'PATCH');await refresh();notice(enabled?'上游已恢复。':'上游已暂停。');});
$('upstreamCredentials').onsubmit=run(async()=>{requireClean();if(!upstream)throw new Error('未绑定上游');if(upstream.inFlight)throw new Error('请等待在途生成完成。');const token=$('upstreamPst').value.trim();if(!/^pst-[A-Za-z0-9_-]+$/.test(token))throw new Error('请输入完整 PST');await api('/api/admin/accounts/'+encodeURIComponent(upstream.id),{token},'PATCH');$('upstreamPst').value='';await api('/api/admin/accounts/quota',{ids:[upstream.id]});await refresh();notice('PST 已更新，上游保持暂停；确认额度后手动恢复。');});
window.addEventListener('beforeunload',e=>{if(dirty||lossPolicyDirty){e.preventDefault();e.returnValue='';}});
if(ownerMode){$('usageTab').hidden=false;$('disconnect').textContent='返回我的额度';$('connectedHint').textContent='正在验证 owner 身份…';refresh().then(()=>{if(!expired)tab('usagePanel');}).catch(e=>{notice(e.message,true);$('ownerSignin').hidden=false;});}
else{$('connect').hidden=false;$('adminToken').value=sessionStorage.getItem('nai.adminSession')||localStorage.getItem('nai.adminToken')||'';if($('adminToken').value)refresh().catch(()=>notice('请重新连接管理端。',true));}

// 时间恢复仍需兜底读取；隐藏页暂停，操作期间仅记一次待刷新。
setInterval(()=>{if(!document.hidden)requestRefresh();},30000);
document.addEventListener('visibilitychange',()=>{if(!document.hidden){if(ownerMode)flushRefresh();else requestRefresh();}});

// 额度只在成员之间转移，删除前单独预览接收者与全部余额。
let deletePreview=null,transferConfigVersion=null;
const resourceNames={v5:'V5 余量',anlasFixed:'订阅 Anlas',anlasPurchased:'购买 Anlas'};
function sharedMembers(){return state.users.filter(u=>u.quota?.member===true);}
function fillMemberSelect(id,users){$(id).replaceChildren(...users.map(u=>option(u.id,u.username||u.id)));}
function openTransfer(u){requireClean();const users=sharedMembers();if(!users.some(v=>v.id===u.id))throw new Error('请先将该成员加入共享分组。');if(users.length<2)throw new Error('至少需要两位共享成员才能转移额度。');transferConfigVersion=state.configVersion;fillMemberSelect('transferFrom',users);fillMemberSelect('transferTo',users);$('transferFrom').value=u.id;$('transferTo').value=users.find(v=>v.id!==u.id).id;$('transferAmount').value='';$('transferResource').value='v5';updateTransferHint();modal('transferDialog');}
function updateTransferHint(){const from=state.users.find(u=>u.id===$('transferFrom').value),to=state.users.find(u=>u.id===$('transferTo').value),resource=$('transferResource').value,amount=Number($('transferAmount').value)||0,before=Number(from?.quota?.[resource+'Available']||0),after=Number(to?.quota?.[resource+'Available']||0);$('transferHint').textContent=(from?.username||'转出者')+'：'+number(before)+' → '+number(before-amount)+'；'+(to?.username||'接收者')+'：'+number(after)+' → '+number(after+amount)+(resource==='v5'?' / 容量 '+number(to?.quota?.v5Capacity):'')+'。总余额保持不变；有排队或运行任务时暂不可操作。';}
$('transferAmount').oninput=updateTransferHint;
for(const id of ['transferFrom','transferTo','transferResource'])$(id).onchange=updateTransferHint;
$('transferForm').onsubmit=run(async()=>{requireClean();const fromUserId=$('transferFrom').value,toUserId=$('transferTo').value,resource=$('transferResource').value,amount=Number($('transferAmount').value);if(fromUserId===toUserId)throw new Error('转出与接收成员不能相同。');if(!Number.isFinite(amount)||amount<=0)throw new Error('请输入大于 0 的转移额度。');await manage({action:'transfer-quota',fromUserId,toUserId,resource,amount,expectedConfigVersion:transferConfigVersion});$('transferDialog').close();try{await refresh();}catch{notice('额度已转移，但页面刷新失败，请手动刷新，勿重复提交。',true);return;}notice(resourceNames[resource]+' 已转移。');});
function openDelete(u){requireClean();const recipients=state.users.filter(v=>v.id!==u.id&&(v.quota?.member===true||(u.quota?.member===true&&v.enabled)));if(!recipients.length)throw new Error('没有可接收的其他账户。请创建并启用一个新账户；未分组的新账户也可接替当前共享成员。');$('deleteUserId').value=u.id;$('deleteName').textContent=u.username||u.id;$('deleteRecipient').replaceChildren(...recipients.map(v=>option(v.id,(v.username||v.id)+(v.quota?.member?'':'（未分组，将接替原份额）'))));resetDeletePreview();modal('deleteDialog');}
function resetDeletePreview(){deletePreview=null;$('deleteShareRows').replaceChildren();$('deleteSharePreview').hidden=true;$('deleteConfirmName').value='';$('deleteSummary').textContent='选择接收账户后预览。余额和原份额全部转交指定成员，其他成员份额不变。未分组的新账户会自动加入原分组。';$('deleteConfirmSection').hidden=true;$('confirmDelete').disabled=true;}
$('deleteRecipient').onchange=resetDeletePreview;
$('previewDelete').onclick=run(async()=>{requireClean();const body={action:'preview-delete-member',userId:$('deleteUserId').value,transferToUserId:$('deleteRecipient').value,expectedConfigVersion:state.configVersion};const result=await manage(body);deletePreview={...body,action:'delete-member',expectedConfigVersion:result.configVersion};const name=state.users.find(u=>u.id===body.userId)?.username||body.userId,to=state.users.find(u=>u.id===body.transferToUserId)?.username||body.transferToUserId,q=result.transferred,wasUnassigned=!state.users.find(u=>u.id===body.transferToUserId)?.quota?.member;$('deleteSummary').textContent='将删除成员「'+name+'」，把全部剩余余额和原份额交给「'+to+'」：V5 '+number(q.v5)+'，订阅 Anlas '+number(q.anlasFixed)+'，购买 Anlas '+number(q.anlasPurchased)+'。其他成员份额不变。登录与 API Key 将失效，历史记录保留。接收者 V5 如超过容量，将暂停恢复直到低于容量。';if(wasUnassigned)$('deleteSummary').textContent+=' 新账户将自动加入原分组并继承被删除成员的份额，其他成员的份额不变。';const changes=(result.groups||[]).filter(g=>g.share!==state.groups.find(v=>v.id===g.id)?.share);if(changes.length)$('deleteSummary').textContent+=' 分组份额调整：'+changes.map(g=>g.name+' '+percent(state.groups.find(v=>v.id===g.id)?.share)+' → '+percent(g.share)).join('；')+'。';$('deleteShareRows').replaceChildren();for(const change of result.shareChanges||[]){const user=state.users.find(u=>u.id===change.userId),row=node('tr');row.append(node('td',(user?.username||change.userId)+(change.userId===body.userId?'（删除）':change.userId===body.transferToUserId?'（接收）':'')),node('td',percent(change.beforeShare)),node('td',percent(change.afterShare)));$('deleteShareRows').append(row);}$('deleteSharePreview').hidden=!(result.shareChanges?.length);$('deleteConfirmSection').hidden=false;$('deleteConfirmName').value='';$('confirmDelete').disabled=true;});
$('deleteConfirmName').oninput=()=>{$('confirmDelete').disabled=!deletePreview||$('deleteConfirmName').value!==$('deleteName').textContent;};
$('confirmDelete').onclick=run(async()=>{requireClean();if(!deletePreview||$('deleteConfirmName').value!==$('deleteName').textContent)throw new Error('请输入被删除成员的完整用户名确认。');await manage(deletePreview);deletePreview=null;$('deleteDialog').close();try{await refresh();}catch{notice('成员已删除且余额已转交，但页面刷新失败，请手动刷新。',true);return;}notice('成员已删除，全部剩余余额已转交，历史记录已保留。');});
$('deleteDialog').addEventListener('close',resetDeletePreview);

const lossPolicyFields={v5Points24h:'lossPolicyV5',fixedAnlas24h:'lossPolicyFixed24',fixedAnlas30d:'lossPolicyFixed30',memberAttempts24h:'lossPolicyAttempts'};
const lossTypes={generation_reserved:'本地预扣',generation_refunded:'本地退款',generation_loss_suspected:'疑似上游损耗',generation_loss_estimated:'估计结案（实耗未知）',generation_result_recovered:'迟到结果找回',resource_gap_observed:'聚合差额观察',v5_capacity_observed:'V5 容量差额观察',v5_capacity_recovered:'V5 容量恢复',resource_return_unattributed:'待归属入账',operating_funding_registered:'登记外部注资',operating_funding_received:'运营资金入账',operating_loss_covered:'运营承担差额',resource_gap_reconciled:'确认快照回跳',loss_alert_policy_updated:'调整软告警阈值'};
const evidenceNames={aggregate_gap:'官方余额聚合差额',aggregate_capacity:'共享容量差额',recovery_rate_estimate:'恢复速率估计',unclassified_balance_increase:'来源待核实的余额增加',operator_attested_aggregate_correction:'运营者确认的快照修正',local_refund:'本地退款凭据',unconfirmed:'上游消费未经确认',unconfirmed_estimate:'未确认消费的估计结案',complete_local_result:'完整本地产物'};
const lossResourceName=key=>key==='v5'?'V5 百分点':resourceNames[key]||key;
const resourceText=resources=>Object.entries(resources||{}).map(([key,value])=>lossResourceName(key)+' '+number(value)).join(' / ')||'无资源金额';
const policyText=p=>'V5/24h '+number(p.v5Points24h)+'；订阅/24h '+number(p.fixedAnlas24h)+'；订阅/30d '+number(p.fixedAnlas30d)+'；单成员次数/24h '+number(p.memberAttempts24h);
function adoptLossPolicy(policy){
 lossPolicyVersion=policy.version;lossPolicyDirty=false;
 for(const [key,id] of Object.entries(lossPolicyFields)){$(id).value=policy[key];$(id).disabled=false;}
 $('saveLossPolicy').disabled=false;$('resetLossPolicy').disabled=false;
 $('lossPolicyHint').textContent='当前阈值版本 '+policy.version+' · 尚无修改';
}
function lossEvidence(event){
 const parts=[];
 if(event.upstreamCandidates?.length)parts.push('上游候选（互斥，不相加）：'+event.upstreamCandidates.map(c=>resourceText(c.resources)).join(' 或 '));
 if(event.resources)parts.push('本地账务：'+resourceText(event.resources));
 if(event.resource&&event.amount!==undefined)parts.push((['resource_gap_observed','resource_gap_reconciled','v5_capacity_observed'].includes(event.type)?'该次记录的聚合差额：':'记录数量：')+lossResourceName(event.resource)+' '+number(event.amount));
 if(event.observedAmount!==undefined)parts.push('本次新增差额 '+number(event.observedAmount));
 if(event.delta!==undefined)parts.push('本次变化 '+number(event.delta));
 if(event.estimateAmountStatus)parts.push('金额证据：'+({quoted_candidates:'提交前报价候选，实耗未确认',unknown:'未知，不能视为零消费'}[event.estimateAmountStatus]||'待确认'));
 if(event.evidenceLevel)parts.push('证据：'+(evidenceNames[event.evidenceLevel]||'证据类型待确认'));
 if(event.refundRetained)parts.push('退款保留');
 if(event.before&&event.after)parts.push('版本 '+event.before.version+' → '+event.after.version+'；原阈值 '+policyText(event.before)+'；新阈值 '+policyText(event.after));
 if(event.receipt)parts.push('收据 '+event.receipt);
 if(event.lossEventId)parts.push('关联差额 '+event.lossEventId);
 return parts.join('；')||'未提供金额，不推算实耗';
}
function renderLosses(){
 const {risk,operating,policy,alerts}=losses;
 $('lossAlerts').replaceChildren(...alerts.map(a=>node('p',a.message)));$('lossAlerts').hidden=!alerts.length;
 $('lossReadStatus').textContent='损耗数据更新 '+displayTime(losses.asOf);
 $('lossRiskCounts').textContent='近 24 小时异常尝试 '+number(risk.last24h.attempts)+' 次，其中金额未知 '+number(risk.last24h.unknownAmounts)+' 次；近 30 天 '+number(risk.last30d.attempts)+' 次，其中金额未知 '+number(risk.last30d.unknownAmounts)+' 次。';
 $('lossRiskRows').replaceChildren();$('lossBalanceRows').replaceChildren();
 for(const resource of ['v5','anlasFixed','anlasPurchased']){
  const riskRow=node('tr');for(const text of [lossResourceName(resource),number(risk.last24h.possible[resource]),number(risk.last30d.possible[resource])])riskRow.append(node('td',text));$('lossRiskRows').append(riskRow);
  const balanceRow=node('tr');for(const text of [lossResourceName(resource),number(risk.aggregate.last24h[resource]),number(risk.aggregate.last30d[resource]),number(resource==='v5'?operating.v5CapacityGap:operating.unfunded[resource]),number(operating.available[resource]),resource==='v5'?'不适用':number(operating.unattributedReturns[resource])])balanceRow.append(node('td',text));$('lossBalanceRows').append(balanceRow);
 }
 if(!lossPolicyDirty)adoptLossPolicy(policy);
 else $('lossPolicyHint').textContent='未保存草稿 · 编辑版本 '+lossPolicyVersion+'；服务器版本 '+policy.version+'。读取不会覆盖草稿；版本冲突时请保留修改内容，再主动读取最新阈值。';
 $('lossEvents').replaceChildren();
 for(const event of lossEvents){
  const row=node('tr'),kind=node('td',lossTypes[event.type]||event.type),detail=node('td',lossEvidence(event));detail.style.whiteSpace='normal';detail.style.minWidth='280px';
  kind.append(node('span',event.id,'cell-sub'));if(event.jobId)kind.append(node('span','任务 '+event.jobId,'cell-sub'));
  row.append(node('td',displayTime(event.at)),kind,node('td',event.memberName||event.memberId||event.actorId||'共享上游'),detail);$('lossEvents').append(row);
 }
 if(!lossEvents.length){const row=node('tr'),cell=node('td','暂无证据记录','empty');cell.colSpan=4;row.append(cell);$('lossEvents').append(row);}
 $('moreLossEvents').hidden=!lossNextCursor;
 $('pendingFunding').replaceChildren(...operating.pendingExternalFunding.map(p=>node('li',p.receipt+' · '+lossResourceName(p.resource)+' 待确认 '+number(p.remaining)+' · 归属'+(p.destination==='members'?'成员':'运营'))));
 if(!operating.pendingExternalFunding.length)$('pendingFunding').append(node('li','暂无待确认注资'));
 $('saveOperating').disabled=false;
}
function loadLosses(options={}){
 if(lossTask){lossPending=true;lossAppendPending||=options.append===true;return lossTask;}
 lossTask=(async()=>{do{lossPending=false;lossAppendPending=false;await readLosses(options);options={append:lossAppendPending};}while(lossPending&&!document.hidden&&state&&!expired);})().finally(()=>{lossTask=null;});
 return lossTask;
}
async function readLosses({append=false}={}){
 if(append&&!lossNextCursor)return;
 const request=++lossRead,cursor=append?lossNextCursor:null;
 let result;try{result=await api('/api/admin/member-management?view=losses'+(cursor?'&before='+encodeURIComponent(cursor):''));}catch(error){if(error.code==='STALE_RESPONSE')return;throw error;}
 if(request!==lossRead||!state)return;
 // 翻页后保持同一段连续历史；合并新第一页可能跳过两次读取之间的记录。
 const events=append?[...lossEvents,...result.events]:lossPaged?lossEvents:result.events;
 lossEvents=[...new Map(events.map(e=>[e.id,e])).values()];
 if(append||!lossPaged)lossNextCursor=result.nextCursor;
 if(append)lossPaged=true;
 losses=result;renderLosses();
}
function showLossReadError(error){if(!state)return;$('lossReadStatus').textContent='损耗读取失败，显示内容可能已过期：'+error.message;$('lossAlerts').replaceChildren(node('p','损耗告警暂不可读取，请刷新状态：'+error.message));$('lossAlerts').hidden=false;}
for(const id of Object.values(lossPolicyFields))$(id).oninput=()=>{lossPolicyDirty=true;lossPolicyEdit++;$('lossPolicyHint').textContent='未保存草稿 · 编辑版本 '+lossPolicyVersion+'；保存会记录操作者和前后阈值。';};
$('lossPolicyForm').onsubmit=run(async()=>{
 if(lossPolicyVersion===null)throw new Error('请先读取告警阈值。');
 const policy=Object.fromEntries(Object.entries(lossPolicyFields).map(([key,id])=>[key,$(id).value===''?NaN:Number($(id).value)]));
 if(Object.entries(policy).some(([key,value])=>!Number.isFinite(value)||value<0||(key!=='v5Points24h'&&!Number.isSafeInteger(value))))throw new Error('阈值须为非负数字；Anlas 和次数须为整数。');
 const edit=lossPolicyEdit;lossRead++;
 const result=await manage({action:'loss-alert-policy',expectedVersion:lossPolicyVersion,policy});
 lossPolicyVersion=result.policy.version;
 if(edit===lossPolicyEdit)adoptLossPolicy(result.policy);
 try{await loadLosses();}catch(e){showLossReadError(e);notice('阈值已保存，但审计记录读取失败，请刷新状态。',true);return;}
 notice('软告警阈值已保存并记录审计；调用权限不变。'+(lossPolicyDirty?' 后续编辑仍未保存。':''));
});
$('resetLossPolicy').onclick=run(async()=>{await loadLosses();adoptLossPolicy(losses.policy);notice('已放弃阈值草稿并读取最新版本。');});
$('moreLossEvents').onclick=run(()=>loadLosses({append:true}));
$('reloadLossEvents').onclick=run(async()=>{lossPaged=false;await loadLosses();});
const operatingHints={
 'operating-contribute':'仅从配置的运营者本人可用份额划入运营余额，不改变其他成员的钱包。',
 'operating-register-funding':'登记已实际新增的外部注资凭据，本页面不会向 NAI 付款。默认等待官方余额增加后入账；若勾选待归属入账，表示你已确认其来自这笔注资，立即按所选归属入账。',
 'operating-cover-loss':'用已有运营余额承担指定的当前聚合差额，不再次扣成员余额，也不累计任务的疑似或估计金额。',
 'operating-reconcile-return':'确认待归属入账是官方快照回跳，修正尚未承担的关联差额。这是有凭据的人工确认，不算新增注资，也不回冲已承担的损耗。'
};
function updateOperatingForm(){
 const action=$('operatingAction').value,register=action==='operating-register-funding',linked=['operating-cover-loss','operating-reconcile-return'].includes(action);
 const anlasOnly=register||action==='operating-reconcile-return';
 if(anlasOnly&&$('operatingResource').value==='v5')$('operatingResource').value='anlasFixed';
 for(const option of $('operatingResource').children)if(option.value==='v5')option.disabled=anlasOnly;
 $('operatingHint').textContent=operatingHints[action];$('operatingRegisterFields').hidden=!register;$('operatingLossFields').hidden=!linked;$('operatingLossId').required=linked;
 $('operatingAmount').step=$('operatingResource').value==='v5'?'any':'1';
 $('operatingLossId').value=linked?(losses?.operating.gapEventIds[$('operatingResource').value]||''):'';
}
for(const id of ['operatingAction','operatingResource'])$(id).onchange=updateOperatingForm;
$('useCurrentGap').onclick=()=>{$('operatingLossId').value=losses?.operating.gapEventIds[$('operatingResource').value]||'';};
$('operatingForm').onsubmit=run(async()=>{
 const action=$('operatingAction').value,resource=$('operatingResource').value,amount=Number($('operatingAmount').value),receipt=$('operatingReceipt').value.trim();
 if(!Number.isFinite(amount)||amount<=0||(resource!=='v5'&&!Number.isSafeInteger(amount)))throw new Error('数量须大于 0；Anlas 须为整数。');
 if(!/^[A-Za-z0-9_-]{8,100}$/.test(receipt))throw new Error('请填写 8–100 位唯一收据标识，只含字母、数字、下划线或连字符。');
 const body={action,resource,amount,receipt};
 if(['operating-cover-loss','operating-reconcile-return'].includes(action)){body.lossEventId=$('operatingLossId').value.trim();if(!body.lossEventId)throw new Error('请提供当前聚合差额记录 ID。');}
 if(action==='operating-register-funding'){body.destination=$('operatingDestination').value;body.fromUnattributed=$('operatingFromUnattributed').checked;}
 await manage(body);$('operatingAmount').value='';$('operatingReceipt').value='';
 try{await loadLosses();}catch(e){showLossReadError(e);notice('账务操作已保存，但记录读取失败，请刷新状态，勿重复提交。',true);return;}
 notice('账务操作已保存并记录收据。下一笔操作请使用新的收据标识。');
});
updateOperatingForm();
