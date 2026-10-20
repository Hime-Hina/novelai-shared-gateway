import { withMemberSessions } from '../helpers/member-session-store.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { EventEmitter } from 'node:events';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import {handleMemberRoutes} from '../../server/member-routes.ts';
import {setMemberCredentials, createMemberSession, logoutMemberSession} from '../../server/member-auth.ts';
import {configureFairQuota, syncFairQuota} from '../../server/fair-quota.ts';
import { JsonStore } from '../../server/store.ts';
test('独立登录、只读隔离、管理员权限、CSRF、重置与事务失败',async t=>{
 const db={settings:{},accounts:[],jobs:[],users:['a','b','c','d'].map(id=>({id,enabled:true,token:'STA1N-'+id,balance:0}))};
 configureFairQuota(db,['a','b','c','d']);await setMemberCredentials(db,'a',{username:'test-member-a',password:'Valid-password-member-a'});await setMemberCredentials(db,'b',{username:'test-member-b',password:'Valid-password-member-b'});
 let queue=Promise.resolve();const store={update(fn){queue=queue.catch(()=>{}).then(()=>fn(db));return queue;},async readMemberLateResults(){return [];},async readMemberImageIds(){return new Map();}};
 await withMemberSessions(t,store);
 const deps={store,assertAdmin(req){if(req.headers['x-admin-token']!=='test-admin')throw Object.assign(new Error('admin required'),{statusCode:403});},createId:()=> 'new-user',createPublicToken:()=> 'STA1N-new'};
 const server=http.createServer(async(req,res)=>{try{const done=await handleMemberRoutes(req,res,new URL(req.url,'http://localhost'),deps);if(!done){res.writeHead(404);res.end();}}catch(e){res.writeHead(e.statusCode||500,{'content-type':'application/json'});res.end(JSON.stringify({error:e.message}));}});
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));t.after(()=>{server.closeAllConnections();server.close();});const base='http://127.0.0.1:'+server.address().port;
 async function call(path,{body,headers={},cookie}={}) {return new Promise((resolve,reject)=>{const request=http.request(base+path,{method:body===undefined?'GET':'POST',headers:{Origin:base,...(body===undefined?{}:{'content-type':'application/json'}),...(cookie?{cookie}:{}),...headers}},response=>{let text='';response.on('data',chunk=>text+=chunk);response.on('end',()=>resolve({status:response.statusCode,headers:{get:name=>String(response.headers[name]||'')},json:async()=>JSON.parse(text)}));});request.on('error',reject);request.end(body===undefined?undefined:JSON.stringify(body));});}
 let r=await call('/api/member/login',{body:{username:'test-member-a',password:'Valid-password-member-a'}});assert.equal(r.status,200);const cookie=r.headers.get('set-cookie').split(';')[0];assert.match(r.headers.get('set-cookie'),/HttpOnly; SameSite=Strict/);
 r=await call('/api/member/me?userId=b',{cookie});const mine=await r.json();assert.equal(mine.username,'test-member-a');assert.equal(mine.quota.share,.25);assert.ok(!JSON.stringify(mine).includes('STA1N-'));assert.ok(!JSON.stringify(mine).includes('memberAuth'));
 r=await call('/api/member/key?userId=b',{cookie});assert.equal((await r.json()).apiKey,'STA1N-a');
 r=await call('/api/member/shared-status',{cookie});assert.equal(r.status,200);const shared=await r.json();assert.equal(shared.status,'pending_opus');assert.equal(shared.v5Percent,null);assert.ok(!JSON.stringify(shared).includes('STA1N'));
 r=await call('/api/member/shared-status');assert.equal(r.status,401);
 const streamed=await new Promise((resolve,reject)=>{const request=http.get(base+'/api/member/shared-events',{headers:{cookie,Origin:base}},response=>{assert.match(response.headers['content-type'],/text\/event-stream/);let text='';response.on('data',chunk=>{text+=chunk;if(text.includes('\n\n')){resolve(text);request.destroy();}});});request.on('error',reject);request.setTimeout(5000,()=>{request.destroy();reject(new Error('SSE timeout'));});});
 assert.match(streamed,/event: shared-status/);assert.match(streamed,/pending_opus/);assert.ok(!streamed.includes('STA1N'));
 for(const path of ['/api/member/groups','/api/member/credentials','/api/admin/member-management']){r=await call(path,{cookie,body:{action:'groups',groups:[]},headers:{authorization:'Bearer STA1N-a'}});assert.equal(r.status,403);}
 r=await call('/api/member/me',{cookie,headers:{Origin:'https://evil.invalid'}});assert.equal(r.status,403);
 r=await call('/api/member/login',{body:{},headers:{Origin:''}});assert.equal(r.status,403);
 r=await call('/api/admin/member-management',{headers:{'x-admin-token':'test-admin'}});assert.equal(r.status,200);const admin=await r.json();assert.equal(admin.groups.length,4);assert.ok(!JSON.stringify(admin).includes('memberAuth'));
 r=await call('/api/admin/member-management',{cookie,body:{action:'fallback-policy',enabled:true}});assert.equal(r.status,403);
 r=await call('/api/admin/member-management',{body:{action:'fallback-policy',enabled:true},headers:{'x-admin-token':'test-admin'}});assert.equal(r.status,200);assert.equal(db.settings.fairQuota.autoAnlasFallback,true);
 r=await call('/api/member/me',{cookie});assert.equal((await r.json()).quota.autoAnlasFallback,true);
 const freePolicy={action:'free-image-policy',expectedVersion:1,policy:{enabled:true,warningThreshold:10,hardLimit:20}};
 r=await call('/api/admin/member-management',{cookie,body:freePolicy});assert.equal(r.status,403);
 r=await call('/api/admin/member-management',{body:freePolicy,headers:{'x-admin-token':'test-admin'}});assert.equal(r.status,200);
 assert.equal((await r.json()).policy.version,2);assert.equal(db.settings.fairQuota.configVersion,1);
 r=await call('/api/admin/member-management',{body:freePolicy,headers:{'x-admin-token':'test-admin'}});assert.equal(r.status,409);
 assert.equal(db.ledger.filter(event=>event.type==='free_image_policy_updated').length,1);
 r=await call('/api/admin/member-management',{body:{action:'credentials',username:'invalid',password:'short'},headers:{'x-admin-token':'test-admin'}});assert.equal(r.status,400);assert.equal(db.users.length,4);
 r=await call('/api/admin/member-management',{body:{action:'credentials',userId:'a',username:'test-member-a',password:'Reset-password-member-a'},headers:{'x-admin-token':'test-admin'}});assert.equal(r.status,200);
 r=await call('/api/member/me',{cookie});assert.equal(r.status,401);
 r=await call('/api/member/login',{body:{username:'test-member-b',password:'Valid-password-member-b'},headers:{Host:'nai.example',Origin:'https://nai.example'}});assert.equal(r.status,200);assert.match(r.headers.get('set-cookie'),/; Secure/);
 r=await call('/api/admin/member-management',{headers:{Host:'nai.example',Origin:'https://nai.example','x-admin-token':'test-admin'}});assert.equal(r.status,404);

 // 管理新增操作使用同一私有入口；成员 Cookie 与 Key 都不能提权。
 const manage=body=>call('/api/admin/member-management',{body,headers:{'x-admin-token':'test-admin'}});
 for(const action of ['member-key','member-status','member-admission','preview-groups']){r=await call('/api/admin/member-management',{body:{action,userId:'b',enabled:false,groups:[]},cookie,headers:{authorization:'Bearer STA1N-a'}});assert.equal(r.status,403);}
 r=await manage({action:'member-key',userId:'b'});assert.deepEqual(await r.json(),{apiKey:'STA1N-b'});
 r=await manage({action:'member-key',userId:'missing'});assert.equal(r.status,404);
 r=await manage({action:'member-status',userId:'b',enabled:'false'});assert.equal(r.status,400);assert.equal(db.users.find(u=>u.id==='b').enabled,true);
 r=await manage({action:'member-admission',userId:'b',acceptingJobs:'false'});assert.equal(r.status,400);
 r=await call('/api/admin/member-management',{body:{action:'member-key',userId:'b'},headers:{'x-admin-token':'test-admin',Origin:'https://evil.invalid'}});assert.equal(r.status,403);

 // 预览允许估算恢复与重分，但不能在真实账本留下余额、版本或审计变化。
 db.users.push({id:'e',token:'STA1N-e',enabled:true,balance:0});db.settings.fairQuota.balances.a.v5Available=8;db.settings.fairQuota.balances.a.anlasFixedAvailable=100;db.settings.fairQuota.balances.a.anlasAvailable=100;
 const proposed=structuredClone(db.settings.fairQuota.groups);proposed[0].members.push('e');const beforePreview=structuredClone(db);
 // 预览和保存都必须携带编辑开始时的版本；省略版本不能绕过并发保护。
 for(const action of ['preview-groups','groups'])for(const expectedConfigVersion of [undefined,null,'1',1.5,0]){
  r=await manage({action,groups:proposed,redistributeRemaining:true,expectedConfigVersion});assert.equal(r.status,409);assert.deepEqual(db,beforePreview);
 }
 r=await manage({action:'preview-groups',groups:proposed,redistributeRemaining:true,expectedConfigVersion:beforePreview.settings.fairQuota.configVersion});assert.equal(r.status,200);const preview=await r.json();
 assert.equal(preview.configVersion,beforePreview.settings.fairQuota.configVersion);assert.equal(preview.redistributeRemaining,true);assert.equal(preview.users.find(u=>u.id==='a').quota.v5Available,4);assert.equal(preview.users.find(u=>u.id==='e').quota.anlasFixedAvailable,50);assert.deepEqual(db,beforePreview);assert.ok(!JSON.stringify(preview).includes('STA1N-'));
 r=await manage({action:'groups',groups:proposed,redistributeRemaining:true,expectedConfigVersion:preview.configVersion-1});assert.equal(r.status,409);assert.deepEqual(db,beforePreview);
 r=await manage({action:'groups',groups:proposed,redistributeRemaining:true,expectedConfigVersion:preview.configVersion});assert.equal(r.status,200);assert.equal((await r.json()).configVersion,preview.configVersion+1);
 const afterSave=structuredClone(db);r=await manage({action:'groups',groups:proposed,expectedConfigVersion:preview.configVersion});assert.equal(r.status,409);assert.deepEqual(db,afterSave);
 // 另一个旧页面在新保存之后才请求预览，也不能拿到新版本后覆盖新配置。
 r=await manage({action:'preview-groups',groups:beforePreview.settings.fairQuota.groups,expectedConfigVersion:preview.configVersion});assert.equal(r.status,409);assert.deepEqual(db,afterSave);

 // 创建成员与加入组同事务提交，密码失败、组不存在或预留冲突不会留下半个成员。
 let beforeCreate=structuredClone(db);
 r=await manage({action:'credentials',username:'new-member',password:'synthetic-Valid-password-new-member',groupId:'missing'});assert.equal(r.status,400);assert.deepEqual(db,beforeCreate);
 r=await manage({action:'credentials',username:'new-member',password:'short',groupId:proposed[0].id});assert.equal(r.status,400);assert.deepEqual(db,beforeCreate);
 db.settings.fairQuota.reservations.testHeld={userId:'a',status:'reserved',resource:'free',amount:0};beforeCreate=structuredClone(db);
 r=await manage({action:'credentials',username:'new-member',password:'synthetic-Valid-password-new-member',groupId:proposed[0].id});assert.equal(r.status,409);assert.deepEqual(db,beforeCreate);
 db.settings.fairQuota.reservations.testHeld.status='refunded';
 r=await manage({action:'credentials',username:'new-member',password:'synthetic-Valid-password-new-member',groupId:proposed[0].id});assert.equal(r.status,200);const created=await r.json();assert.equal(created.id,'new-user');assert.equal(created.groupId,proposed[0].id);assert.ok(db.settings.fairQuota.groups[0].members.includes(created.id));assert.equal(db.settings.fairQuota.balances[created.id].v5Available,0);
 r=await manage({action:'credentials',userId:'a',username:'test-member-a',password:'synthetic-Another-password-member-a',groupId:proposed[0].id});assert.equal(r.status,400);

 // 停用保留额度与份额，立即拒绝登录；重新启用不会复活停用前的会话。
 r=await call('/api/member/login',{body:{username:'test-member-a',password:'Reset-password-member-a'}});assert.equal(r.status,200);const priorCookie=r.headers.get('set-cookie').split(';')[0];
 const quotaBeforeDisable=structuredClone(db.settings.fairQuota),tokenBefore=db.users.find(u=>u.id==='a').token;
 r=await manage({action:'member-status',userId:'a',enabled:false});assert.equal(r.status,200);assert.deepEqual(db.settings.fairQuota,quotaBeforeDisable);assert.equal(db.users.find(u=>u.id==='a').token,tokenBefore);
 r=await call('/api/member/me',{cookie:priorCookie});assert.equal(r.status,401);r=await call('/api/member/login',{body:{username:'test-member-a',password:'Reset-password-member-a'}});assert.equal(r.status,401);
 r=await manage({action:'member-status',userId:'a',enabled:true});assert.equal(r.status,200);r=await call('/api/member/me',{cookie:priorCookie});assert.equal(r.status,401);r=await call('/api/member/login',{body:{username:'test-member-a',password:'Reset-password-member-a'}});assert.equal(r.status,200);
 db.jobs.push({id:'busy',userToken:'STA1N-e',status:'queued'});r=await manage({action:'member-status',userId:'e',enabled:false});assert.equal(r.status,409);assert.equal(db.users.find(u=>u.id==='e').enabled,true);
 db.jobs.at(-1).status='failed';r=await manage({action:'member-status',userId:'e',enabled:false});assert.equal(r.status,200);
 r=await call('/api/admin/member-management',{headers:{'x-admin-token':'test-admin'}});const overview=await r.json();assert.equal(overview.configVersion,db.settings.fairQuota.configVersion);assert.equal(overview.shared.status,'pending_opus');assert.equal(overview.users.find(u=>u.id==='e').recentActivity.failed,1);assert.ok(!JSON.stringify(overview).includes('memberAuth'));assert.ok(!JSON.stringify(overview).includes('STA1N-'));

 // 删除和转账只能管理员操作，失败与预览都不能产生任何数据写入。
 for(const action of ['transfer-quota','preview-delete-member','delete-member']){r=await call('/api/admin/member-management',{body:{action,userId:'a',transferToUserId:'b'},headers:{authorization:'Bearer STA1N-a'}});assert.equal(r.status,403);}
 let deletion={action:'preview-delete-member',userId:'a',transferToUserId:'b',expectedConfigVersion:db.settings.fairQuota.configVersion};
 let beforeDelete=structuredClone(db);r=await manage(deletion);assert.equal(r.status,200);const deletePreview=await r.json();assert.equal(deletePreview.deletedUserId,'a');assert.equal(deletePreview.configVersion,deletion.expectedConfigVersion);assert.deepEqual(db,beforeDelete);assert.ok(!JSON.stringify(deletePreview).includes('STA1N-'));
 r=await manage({...deletion,action:'delete-member',expectedConfigVersion:0});assert.equal(r.status,409);assert.deepEqual(db,beforeDelete);
 r=await manage({action:'transfer-quota',fromUserId:'a',toUserId:'b',resource:'anlasFixed',amount:10,expectedConfigVersion:deletion.expectedConfigVersion});assert.equal(r.status,200);assert.equal(db.settings.fairQuota.balances.b.anlasFixedAvailable,beforeDelete.settings.fairQuota.balances.b.anlasFixedAvailable+10);
 r=await call('/api/member/login',{body:{username:'test-member-a',password:'Reset-password-member-a'}});assert.equal(r.status,200);const deletedCookie=r.headers.get('set-cookie').split(';')[0];
 deletion.expectedConfigVersion=db.settings.fairQuota.configVersion;const historicalJobs=structuredClone(db.jobs),historicalReservations=structuredClone(db.settings.fairQuota.reservations);
 r=await manage({...deletion,action:'delete-member'});assert.equal(r.status,200);assert.ok(!db.users.some(u=>u.id==='a'));assert.equal(db.settings.fairQuota.balances.a,undefined);assert.deepEqual(db.jobs,historicalJobs);assert.deepEqual(db.settings.fairQuota.reservations,historicalReservations);
 r=await call('/api/member/me',{cookie:deletedCookie});assert.equal(r.status,401);r=await call('/api/member/login',{body:{username:'test-member-a',password:'Reset-password-member-a'}});assert.equal(r.status,401);r=await manage({action:'member-key',userId:'a'});assert.equal(r.status,404);
});

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function waitFor(check) {
 const until = Date.now() + 4000;
 while(!check()) { if(Date.now() >= until) throw new Error('synthetic state event timeout'); await delay(10); }
}
async function eventFixture(t) {
 const store = new JsonStore(await mkdtemp(path.join(tmpdir(),'nai-state-events-'))); await store.init();
 const env = { OWNER_USER_ID:'event-owner', DISCORD_GUILD_ID:'1134557553011998840', DISCORD_CLIENT_ID:'123456789012345678',
  DISCORD_CLIENT_SECRET:'synthetic-state-secret', PUBLIC_BASE_URL:'https://nai.example' };
 await store.update(db=>{
  db.users=['event-owner','event-member','event-other','event-fourth'].map((id,index)=>({ id,token:'synthetic-token-'+id,enabled:true,balance:0,
   memberAuth:{username:id,version:id},discord:{id:String(222222222222222220n+BigInt(index)),guildId:env.DISCORD_GUILD_ID,checkedAt:Date.now(),member:true} }));
  db.accounts=[{id:'opus',enabled:true,quotaTier:3,subscriptionActive:true,subscriptionExpiresAt:Date.now()+86400000,
   quotaFixed:10000,quotaPurchased:0,v5UsagePercent:100,quotaCheckedAt:new Date().toISOString()}];
  configureFairQuota(db,db.users.map(user=>user.id));syncFairQuota(db);
 },{immediate:true});
 const sessions=await Promise.all(store.db.users.map(user=>createMemberSession(store,user,env)));
 const deps={store,env,assertAdmin(){},publicImageUrl:id=>'/synthetic/'+id};
 const server=http.createServer(async(req,res)=>{
  try{await handleMemberRoutes(req,res,new URL(req.url,'https://nai.example'),deps);}
  catch(error){if(res.headersSent)res.end();else{res.writeHead(error.statusCode||500,{'content-type':'application/json'});res.end(JSON.stringify({error:error.message}));}}
 });
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 const headers=index=>({host:'nai.example',origin:'https://nai.example',cookie:'nai_member_session='+sessions[index].token});
 const call=route=>new Promise((resolve,reject)=>{
  const req=http.get({hostname:'127.0.0.1',port:server.address().port,path:route,headers:headers(0)},res=>{
   res.resume();res.on('end',()=>resolve({status:res.statusCode}));
  });req.on('error',reject);
 });
 const streams=[];
 const stream=async(index,extra={})=>{
  const frames=[];let ended=false;
  const req=http.get({hostname:'127.0.0.1',port:server.address().port,path:'/api/member/shared-events',headers:{...headers(index),...extra}},res=>{
   let buffer='';res.on('data',chunk=>{
    buffer+=chunk.toString();let split;
    while((split=buffer.indexOf('\n\n'))>=0){const frame=buffer.slice(0,split);buffer=buffer.slice(split+2);
     const event=frame.match(/^event: ([^\n]+)/)?.[1],data=frame.match(/\ndata: ([^\n]+)/)?.[1];
     if(event)frames.push({event,data:JSON.parse(data)});
    }
   });res.on('end',()=>{ended=true;});
  });
  req.on('error',()=>{ended=true;});
  const result={frames,req,get ended(){return ended;},invalidations:()=>frames.filter(frame=>frame.event==='state-invalidated').map(frame=>frame.data)};
  streams.push(result);await waitFor(()=>result.invalidations().length>0);return result;
 };
 t.after(async()=>{for(const stream of streams)stream.req.destroy();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));await store.close();});
 return {store,env,deps,sessions,headers,call,stream};
}

test('真实提交及时通知本人/owner，公共状态兼容；读取无写反馈、重连全量resync、退出和关库清理', {timeout:15000}, async t=>{
 const f=await eventFixture(t),owner=await f.stream(0),member=await f.stream(1),other=await f.stream(2);
 assert.equal(member.frames[0].event,'shared-status');
 assert.deepEqual(owner.invalidations()[0].scopes,['shared','member','owner']);
 assert.deepEqual(member.invalidations()[0].scopes,['shared','member']);
 const before=structuredClone({settings:f.store.db.settings,ledger:f.store.db.ledger}),commits=[];
 f.store.events.on('commit',event=>commits.push(event));
 for(const route of ['/api/member/me','/api/member/owner/member-management'])assert.equal((await f.call(route)).status,200);
 assert.deepEqual({settings:f.store.db.settings,ledger:f.store.db.ledger},before);assert.equal(commits.length,0);
 const started=Date.now(),createdAt=new Date(started).toISOString();
 await f.store.update(db=>db.jobs.push({id:'event-job',status:'queued',userToken:db.users[1].token,createdAt,
  request:{prompt:'private prompt never sent'}}),{dirtyRows:{jobs:['event-job']},immediate:true});
 await waitFor(()=>[owner,member,other].every(stream=>stream.invalidations().length>=2));
 assert.ok(Date.now()-started<1000,'空闲鉴权和队列下业务变化一秒内送达');
 assert.deepEqual(owner.invalidations().at(-1).scopes,['shared','owner']);
 assert.deepEqual(member.invalidations().at(-1).scopes,['shared','member']);
 assert.deepEqual(other.invalidations().at(-1).scopes,['shared']);
 const date=new Date(started+8*3600000).toISOString().slice(0,10);
 assert.deepEqual(owner.invalidations().at(-1).activityDates,[date]);
 assert.deepEqual(member.invalidations().at(-1).activityDates,[date]);
 assert.equal(other.invalidations().at(-1).activityDates,undefined);
 assert.equal(member.frames.filter(frame=>frame.event==='shared-status').at(-1).data.queued,1);
 await f.store.update(db=>{db.users[1].note='private member note';},{dirtyRows:{users:['event-member']},immediate:true});
 await waitFor(()=>member.invalidations().length===3&&owner.invalidations().length===3);await delay(50);
 assert.equal(other.invalidations().length,2);assert.deepEqual(member.invalidations().at(-1).scopes,['member']);
 for(const stream of [owner,member,other])for(const payload of stream.invalidations()){
  assert.deepEqual(Object.keys(payload),['bootId','version','scopes','updatedAt','resync',
   ...(stream!==other&&payload===stream.invalidations()[1]?['activityDates']:[])]);
  assert.ok(!JSON.stringify(payload).includes('private'));assert.ok(!JSON.stringify(payload).includes('synthetic-token'));
 }
 other.req.destroy();const replay=await f.stream(2,{'last-event-id':'unrelated-boot:999999'});
 assert.equal(replay.invalidations()[0].resync,true);assert.deepEqual(replay.invalidations()[0].scopes,['shared','member']);
 assert.equal(replay.invalidations()[0].bootId,owner.invalidations()[0].bootId);
 const ownerCount=owner.invalidations().length;
 await logoutMemberSession(f.store,f.sessions[1].token);await waitFor(()=>member.ended);
 assert.ok(member.frames.some(frame=>frame.event==='auth-expired'));assert.equal(owner.invalidations().length,ownerCount);
 // 发送前重新鉴权；普通会话不能通过配置改变继承 owner 通知权限。
 f.env.OWNER_USER_ID='event-member';
 await f.store.update(db=>{db.users[2].note='trigger owner recheck';},{dirtyRows:{users:['event-other']},immediate:true});
 await waitFor(()=>owner.ended);assert.ok(owner.frames.some(frame=>frame.event==='auth-expired'));
 assert.equal(owner.invalidations().length,ownerCount);
 await f.store.close();await waitFor(()=>replay.ended);
 assert.ok(!replay.frames.some(frame=>frame.event==='auth-expired'));
 assert.equal(f.store.events.listenerCount('commit'),0);assert.equal(f.store.events.listenerCount('close'),0);
});

test('同一SSE异步tick串行，鉴权/读取途中发生的后续提交不会丢失', {timeout:10000}, async t=>{
 const f=await eventFixture(t),stream=await f.stream(1),initial=stream.invalidations().length;
 const update=f.store.update.bind(f.store);let release,entered,hold=true,active=0,maxActive=0;
 const enteredPromise=new Promise(resolve=>{entered=resolve;});const gate=new Promise(resolve=>{release=resolve;});
 f.store.update=async(fn,options={})=>{
  if(options.persist===false){active++;maxActive=Math.max(maxActive,active);try{if(hold){hold=false;entered();await gate;}return await update(fn,options);}finally{active--;}}
  return update(fn,options);
 };
 t.after(()=>release());
 await f.store.update(db=>{db.users[1].note='first';},{dirtyRows:{users:['event-member']},immediate:true});
 await enteredPromise;
 await f.store.update(db=>db.jobs.push({id:'during-read',status:'queued',userToken:db.users[2].token}),{dirtyRows:{jobs:['during-read']},immediate:true});
 await delay(250);release();
 await waitFor(()=>stream.invalidations().length>=initial+2);
 const events=stream.invalidations().slice(initial);
 assert.deepEqual(events[0].scopes,['member']);assert.deepEqual(events[1].scopes,['shared']);
 assert.ok(events[1].version>events[0].version);assert.equal(maxActive,1);
});

test('SSE背压结束和连接关闭都退订，不能耗尽每成员三个连接配额', {timeout:10000}, async t=>{
 const f=await eventFixture(t);
 class SlowResponse extends EventEmitter {
  constructor(){super();this.writableLength=65537;this.frames=[];this.ended=false;}
  writeHead(){} flushHeaders(){} write(text){this.frames.push(text);return false;}
  end(){this.ended=true;this.emit('close');}
 }
 const responses=[];
 const disconnected=new SlowResponse();disconnected.destroyed=true;
 await handleMemberRoutes({method:'GET',headers:f.headers(1)},disconnected,new URL('https://nai.example/api/member/shared-events'),f.deps);
 assert.equal(disconnected.frames.length,0);assert.equal(disconnected.listenerCount('close'),0);
 for(let i=0;i<5;i++){
  const res=new SlowResponse();responses.push(res);
  await handleMemberRoutes({method:'GET',headers:f.headers(1)},res,new URL('https://nai.example/api/member/shared-events'),f.deps);
  await waitFor(()=>res.ended);assert.equal(res.listenerCount('close'),0);assert.equal(res.listenerCount('error'),0);
 }
 const counts=responses.map(res=>res.frames.length);
 await f.store.update(db=>{db.users[1].note='after closed';},{dirtyRows:{users:['event-member']},immediate:true});
 await delay(250);assert.deepEqual(responses.map(res=>res.frames.length),counts);
 assert.equal(f.store.events.listenerCount('commit'),1);
});
