import { withMemberSessions } from '../helpers/member-session-store.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {mkdtemp, rm, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {handleMemberRoutes} from '../../server/member-routes.ts';
import {createMemberSession, isOwner} from '../../server/member-auth.ts';
import {ownerOperation, summarizeOwnerUsage} from '../../server/owner-operations.ts';
import {configureFairQuota, syncFairQuota} from '../../server/fair-quota.ts';
import {sharedSurface} from '../../server/shared-surface.ts';
import {JsonStore} from '../../server/store.ts';

async function fixture(t) {
  const env={OWNER_USER_ID:'owner-a',DISCORD_GUILD_ID:'1134557553011998840',DISCORD_CLIENT_ID:'123456789012345678',DISCORD_CLIENT_SECRET:'synthetic-secret-only',PUBLIC_BASE_URL:'https://nai.example'};
  const account={id:'opus',token:'pst-synthetic-upstream-secret',enabled:true,quotaTier:3,subscriptionActive:true,subscriptionExpiresAt:Date.now()+86400000,quotaFixed:10000,quotaPurchased:100,v5UsagePercent:100,v5UsageTimeUntilNextPercent:0,quotaCheckedAt:new Date().toISOString()};
  const db={settings:{},accounts:[account],jobs:[],users:['owner-a','other-b','other-c','other-d'].map((id,index)=>({id,token:'STA1N-synthetic-'+id,enabled:true,balance:0,memberAuth:{username:'owner',version:id},discord:{id:String(222222222222222220n+BigInt(index)),guildId:env.DISCORD_GUILD_ID,checkedAt:Date.now(),member:true,accessToken:'synthetic-discord-secret'}}))};
  configureFairQuota(db,db.users.map(u=>u.id));syncFairQuota(db);
  let chain=Promise.resolve(), refreshCount=0, updateCount=0, statsCount=0;
  const store={update(fn){chain=chain.catch(()=>{}).then(()=>fn(db));return chain;},async readOwnerJobRows(){statsCount++;return [];},async readMemberLateResults(){return [];},async readMemberImageIds(){return new Map();}};
  await withMemberSessions(t,store);
  const deps={env,store,assertAdmin(){throw Object.assign(new Error('Private administrator required'),{statusCode:401});}};
  deps.ownerOperation=(url,method,body)=>ownerOperation(url,method,body,{store,
    async refreshAccountQuotas(){refreshCount++;return {checked:1,ok:1,failed:0,accounts:[account]};},
    async updateAccount(id,body){assert.equal(id,account.id);updateCount++;Object.assign(account,body);return account;}});
  const server=http.createServer(async(req,res)=>{try{const url=new URL(req.url,'https://nai.example');if(!sharedSurface(url.pathname,req.method).allowed){res.writeHead(404);res.end('{}');return;}await handleMemberRoutes(req,res,url,deps);}catch(e){res.writeHead(e.statusCode||500,{'content-type':'application/json'});res.end(JSON.stringify({error:e.message}));}});
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));t.after(()=>{server.closeAllConnections();server.close();});
  const cookies=await Promise.all(db.users.map(async u=>'nai_member_session='+(await createMemberSession(store,u,env)).token));
  const call=(route,{viewer=0,method,body,headers={}}={})=>new Promise((resolve,reject)=>{
    const req=http.request({hostname:'127.0.0.1',port:server.address().port,path:route,method:method||(body===undefined?'GET':'POST'),headers:{host:'nai.example',origin:'https://nai.example',...(viewer===null?{}:{cookie:cookies[viewer]}),'content-type':'application/json',...headers}},res=>{let text='';res.on('data',c=>text+=c);res.on('end',()=>resolve({status:res.statusCode,data:JSON.parse(text),text}));});req.on('error',reject);req.end(body===undefined?undefined:JSON.stringify(body));
  });
  return {db,env,account,store,call,counts:()=>({refreshCount,updateCount,statsCount})};
}

test('owner 权限绑定内部 ID 和有效 Discord 会话，用户名、Key、伪造管理员头均不能提权',async t=>{
  const {db,env,call,counts}=await fixture(t);
  for(const route of ['/api/member/owner/member-management','/api/member/owner/summary','/api/member/owner/statistics']) {
    assert.equal((await call(route,{viewer:null})).status,401);
    assert.equal((await call(route,{viewer:1,headers:{'x-admin-token':'forged',authorization:'Bearer '+db.users[0].token}})).status,403);
  }
  assert.deepEqual(counts(),{refreshCount:0,updateCount:0,statsCount:0});
  assert.equal((await call('/api/member/me')).data.owner,true);assert.equal((await call('/api/member/me',{viewer:1})).data.owner,false);
  const result=await call('/api/member/owner/summary?revealTokens=1');assert.equal(result.status,200);
  for(const secret of ['pst-synthetic','synthetic-discord','STA1N-'])assert.ok(!result.text.includes(secret));
  assert.equal((await call('/api/admin/member-management')).status,404);
  assert.equal((await call('/api/member/owner/accounts/opus',{method:'PATCH',body:{enabled:false},headers:{origin:'https://evil.example'}})).status,403);
  assert.equal(isOwner(db.users[0],{}),false);assert.equal(isOwner(db.users[1],env),false);
  db.users[0].discord.member=false;
  assert.equal((await call('/api/member/owner/summary')).status,403);
});

test('owner 复用成员管理和邀请码，但不能停用或删除自己',async t=>{
  const {db,call}=await fixture(t);
  const route='/api/member/owner/member-management';
  assert.equal((await call(route)).data.users.length,4);
  for(const body of [{action:'member-status',userId:'owner-a',enabled:false},{action:'preview-delete-member',userId:'owner-a'},{action:'delete-member',userId:'owner-a'}])assert.equal((await call(route,{body})).status,409);
  assert.equal((await call(route,{body:{action:'member-status',userId:'other-b',enabled:false}})).status,200);assert.equal(db.users[1].enabled,false);
  assert.equal((await call(route,{body:{action:'member-status',userId:'other-b',enabled:true}})).status,200);
  delete db.users[1].discord;
  const invite=await call(route,{body:{action:'discord-invite',userId:'other-b'}});assert.equal(invite.status,200);assert.match(invite.data.code,/^[A-Za-z0-9_-]{32}$/);assert.ok(db.users[1].discordInvite.hash);assert.equal(db.users[1].discordInvite.code,undefined);
});

test('服务资源仅随 owner 管理摘要返回，不暴露数据目录路径',async t=>{
  const {store,call}=await fixture(t);
  store.dataDir=await mkdtemp(path.join(tmpdir(),'nai-owner-resources-'));
  t.after(()=>rm(store.dataDir,{recursive:true,force:true}));
  await writeFile(path.join(store.dataDir,'library.sqlite'),Buffer.alloc(32));
  const result=await call('/api/member/owner/member-management');
  assert.equal(result.status,200);
  assert.equal(result.data.serviceResources.data.bytes,32);
  assert.ok(result.data.serviceResources.memory.rssBytes>0);
  assert.ok(result.data.serviceResources.disk.totalBytes>0);
  assert.ok(!result.text.includes(store.dataDir));
  for(const viewer of [null,1]) {
    const denied=await call('/api/member/owner/member-management',{viewer});
    assert.equal(denied.status,viewer===null?401:403);
    assert.equal(denied.data.serviceResources,undefined);
  }
  assert.equal((await call('/api/member/me')).data.serviceResources,undefined);
  assert.equal((await call('/api/member/shared-status')).data.serviceResources,undefined);
});

test('owner 上游操作只允许共享账户的暂停、核验恢复和换 Key，响应不泄漏凭据',async t=>{
  const {account,call,counts}=await fixture(t),route='/api/member/owner/accounts/opus';
  assert.equal((await call(route,{method:'PATCH',body:{proxyUrl:'http://evil.example'}})).status,400);
  assert.equal((await call('/api/member/owner/accounts/another',{method:'PATCH',body:{enabled:true}})).status,404);
  assert.equal((await call('/api/member/owner/accounts/quota',{body:{ids:['another']}})).status,400);
  const refreshed=await call('/api/member/owner/accounts/quota',{body:{ids:['opus']}});assert.deepEqual(refreshed.data,{checked:1,ok:1,failed:0});assert.ok(!refreshed.text.includes('pst-'));
  assert.equal((await call(route,{method:'PATCH',body:{token:'invalid'}})).status,400);
  const rotated=await call(route,{method:'PATCH',body:{token:'pst-synthetic-new-secret'}});assert.equal(rotated.status,200);assert.equal(account.enabled,false);assert.equal(account.token,'pst-synthetic-new-secret');assert.ok(!rotated.text.includes('pst-'));
  account.subscriptionActive=false;assert.equal((await call(route,{method:'PATCH',body:{enabled:true}})).status,409);assert.equal(account.enabled,false);
  account.subscriptionActive=true;assert.equal((await call(route,{method:'PATCH',body:{enabled:true}})).status,200);assert.equal(account.enabled,true);
  assert.ok(counts().refreshCount>=3);
  assert.equal((await call('/api/member/owner/statistics?days=365')).status,200);
  assert.equal((await call('/api/member/owner/statistics?days=0')).status,200);
  assert.equal((await call('/api/member/owner/statistics?days=999')).status,400);
});

test('用量按上海日期、成员和模型汇总，退款及预扣不算消耗，缺失耗时不算零',()=>{
  const rows=[
    {id:'v5',memberId:'a',username:'甲',model:'v5',status:'done',createdAt:'2026-10-15T16:00:00Z',startedAt:'2026-10-15T16:00:10Z',upstreamStartedAt:'2026-10-15T16:00:10Z',upstreamCompletedAt:'2026-10-15T16:00:22Z',resource:'v5',amount:.1,chargeStatus:'settled'},
    {id:'paid',memberId:'b',username:'乙',model:'v45',status:'done',createdAt:'2026-10-15T15:59:00Z',resource:'anlas',fixed:26,purchased:0,chargeStatus:'settled'},
    {id:'refund',memberId:'a',username:'甲',model:'v5',status:'failed',createdAt:'2026-10-15T15:58:00Z',resource:'v5',amount:.2,chargeStatus:'refunded'},
    {id:'running',memberId:'b',username:'乙',model:'v5',status:'running',createdAt:'2026-10-15T15:57:00Z',resource:'anlas',fixed:99,chargeStatus:'reserved'},
    {id:'free',memberId:'a',username:'甲',model:'v45',status:'done',createdAt:'2026-10-15T15:56:00Z',resource:'free',amount:0,chargeStatus:'settled'},
    {id:'pending',memberId:'b',username:'乙',model:'v5',status:'pending',createdAt:'2026-10-15T15:55:00Z'},
    {id:'queued',memberId:'b',username:'乙',model:'v5',status:'queued',createdAt:'2026-10-15T15:54:00Z'}
  ];
  const result=summarizeOwnerUsage(rows);assert.equal(result.totals.total,7);assert.equal(result.totals.done,3);assert.equal(result.totals.failed,1);assert.equal(result.totals.refunded,1);assert.equal(result.totals.free,1);assert.equal(result.totals.v5,.1);assert.equal(result.totals.fixed,26);assert.equal(result.totals.purchased,0);assert.equal(result.totals.averageWaitSeconds,10);assert.equal(result.totals.averageGenerationSeconds,12);assert.equal(result.members.length,2);assert.equal(result.models.length,2);assert.deepEqual(result.days.map(d=>d.id),['2026-10-16','2026-10-15']);
  assert.equal(result.totals.pending,1);assert.equal(result.totals.queued,1);assert.equal(result.totals.running,1);
  assert.equal(['done','failed','pending','queued','running'].reduce((sum,key)=>sum+result.totals[key],0),result.totals.total);
  for(const group of [result.members.find(row=>row.id==='b'),result.models.find(row=>row.id==='v5'),result.days.find(row=>row.id==='2026-10-15')])assert.equal(group.pending,1);
  assert.equal(result.members.find(row=>row.id==='b').averageWaitSeconds,null);
  assert.equal(result.members.find(row=>row.id==='b').averageGenerationSeconds,null);
  assert.equal(summarizeOwnerUsage([]).totals.pending,0);
});

test('SQLite 统计读取覆盖持久历史，按时间筛选且不返回任何 Key、提示词或授权数据',async t=>{
  t.mock.timers.enable({apis:['Date'],now:Date.parse('2026-10-16T00:00:00Z')});
  const store=new JsonStore(await mkdtemp(path.join(tmpdir(),'nai-owner-stats-')));await store.init();
  try{
    await store.update(db=>{db.users.push({id:'a',token:'STA1N-private',enabled:true,memberAuth:{username:'Alice'},discord:{accessToken:'private-discord'}});db.jobs.push({id:'job-a',userToken:'STA1N-private',status:'done',request:{model:'nai-diffusion-4-5-full',prompt:'private-prompt'},createdAt:'2026-10-15T16:00:00Z',fairCharge:{resource:'free',status:'settled',amount:0}});},{collections:['users','jobs'],immediate:true});
    const rows=await store.readOwnerJobRows('2026-10-15T00:00:00Z','2026-10-16T00:00:00Z');assert.equal(rows.length,1);assert.equal(rows[0].username,'Alice');assert.equal(rows[0].memberId,'a');assert.equal(rows[0].model,'nai-diffusion-4-5-full');
    for(const secret of ['STA1N-private','private-discord','private-prompt'])assert.ok(!JSON.stringify(rows).includes(secret));
    assert.equal((await store.readOwnerJobRows('2026-10-16T00:00:00Z','2026-10-17T00:00:00Z')).length,0);
  }finally{await store.close();}
});
