import { withMemberSessions } from '../helpers/member-session-store.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { handleMemberRoutes } from '../../server/member-routes.js';
import { setMemberCredentials, createMemberSession, authenticateMemberSession } from '../../server/member-auth.js';
import { configureFairQuota } from '../../server/fair-quota.js';
import { discordConfig, discordLoginEnabled, requireDiscordMembership, createDiscordInvite } from '../../server/discord-auth.js';
import { sharedSurface } from '../../server/shared-surface.ts';
import { privateAccessAllowed } from '../../server/public-hosts.ts';

const env = { DISCORD_CLIENT_ID:'123456789012345678', DISCORD_CLIENT_SECRET:'synthetic-client-secret', DISCORD_GUILD_ID:'1134557553011998840', PUBLIC_BASE_URL:'https://nai.example', PUBLIC_MODE:'true', PUBLIC_HOST:'nai.example' };
async function fixture(t) {
  const db = { settings:{}, accounts:[], jobs:[], users:['a','b','c','d'].map(id=>({id,enabled:true,token:'STA1N-'+id,balance:0})) };
  configureFairQuota(db,['a','b','c','d']);
  for(const user of db.users) await setMemberCredentials(db,user.id,{username:'member-'+user.id,password:'synthetic-password-for-'+user.id});
  let queue=Promise.resolve();
  const store={update(fn){queue=queue.catch(()=>{}).then(()=>fn(db));return queue;},async readMemberLateResults(){return [];},async readMemberImageIds(){return new Map();}};
  await withMemberSessions(t,store);
  const upstream={id:'222222222222222222',member:true,pending:false,fail:false,calls:[],refreshes:0,scope:'identify guilds.members.read'};
  const deps={store,env,createId:()=> 'new-member',createPublicToken:()=> 'STA1N-new-member',assertAdmin(req){if(req.headers['x-admin-token']!=='synthetic-admin')throw Object.assign(new Error('Admin required'),{statusCode:403});},async discordFetch(url,options){
    upstream.calls.push({url,options});
    if(url.endsWith('/oauth2/token')){
      const refresh=options.body.get('grant_type')==='refresh_token';if(refresh)upstream.refreshes++;
      return Response.json({access_token:'synthetic-access-'+(refresh?upstream.refreshes:'login'),refresh_token:'synthetic-refresh-'+upstream.refreshes,expires_in:3600,token_type:'Bearer',scope:upstream.scope});
    }
    if(url.endsWith('/users/@me'))return Response.json({id:upstream.id,username:'synthetic-discord-user'});
    assert.equal(url,'https://discord.com/api/v10/users/@me/guilds/'+env.DISCORD_GUILD_ID+'/member');
    if(upstream.fail)return new Response('synthetic-upstream-secret',{status:503});
    return upstream.member?Response.json({user:{id:upstream.id},pending:upstream.pending}):new Response('',{status:404});
  }};
  const server=http.createServer(async(req,res)=>{
    try{
      const url=new URL(req.url,'https://nai.example');
      if(!privateAccessAllowed(req,env)){res.writeHead(403);res.end();return;}
      if(!sharedSurface(url.pathname,req.method).allowed){res.writeHead(404);res.end();return;}
      if(!await handleMemberRoutes(req,res,url,deps)){res.writeHead(404);res.end();}
    }catch(error){res.writeHead(error.statusCode||500,{'content-type':'application/json'});res.end(JSON.stringify({error:error.message}));}
  });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  t.after(()=>{server.closeAllConnections();server.close();});
  const call=(path,{method='GET',body,headers={},cookie}={})=>new Promise((resolve,reject)=>{
    const request=http.request({hostname:'127.0.0.1',port:server.address().port,path,method:body===undefined?method:'POST',headers:{host:'nai.example',origin:'https://nai.example',...(body===undefined?{}:{'content-type':'application/json'}),...(cookie?{cookie}:{}),...headers}},response=>{let text='';response.on('data',chunk=>text+=chunk);response.on('end',()=>resolve({status:response.statusCode,headers:response.headers,text,data:text?JSON.parse(text):null}));});
    request.on('error',reject);request.end(body===undefined?undefined:JSON.stringify(body));
  });
  const admin=body=>call('/api/admin/member-management',{body,headers:{host:'127.0.0.1',origin:'http://127.0.0.1','x-admin-token':'synthetic-admin'}});
  const start=async invite=>{
    const response=await call('/api/member/discord/start',{body:invite?{invite}:{}});
    assert.equal(response.status,200,response.text);
    const url=new URL(response.data.authorizationUrl);
    assert.equal(url.origin,'https://discord.com');assert.equal(url.searchParams.get('scope'),'identify guilds.members.read');
    assert.equal(url.searchParams.get('redirect_uri'),'https://nai.example/api/member/discord/callback');
    assert.ok(!url.href.includes(invite||'never-present'));
    return {state:url.searchParams.get('state'),cookie:response.headers['set-cookie'][0].split(';')[0]};
  };
  const finish=(flow,options={})=>call('/api/member/discord/callback?code=synthetic-code&state='+flow.state,{cookie:flow.cookie,headers:{origin:'https://discord.com','sec-fetch-site':'cross-site'},...options});
  return {db,deps,upstream,call,admin,start,finish,port:server.address().port};
}

test('SSE资格失效明确通知退出，Discord临时网络故障只关闭连接且不撤销会话', {timeout:15000}, async t=>{
  for(const temporary of [false,true]){
    const {db,deps,upstream,port}=await fixture(t);
    db.users[0].discord={id:upstream.id,guildId:env.DISCORD_GUILD_ID,checkedAt:Date.now(),member:true,
      accessToken:'synthetic-access',refreshToken:'synthetic-refresh',expiresAt:Date.now()+3600000};
    const session=await createMemberSession(deps.store,db.users[0],env);
    const events=await new Promise((resolve,reject)=>{
      const req=http.get({hostname:'127.0.0.1',port,path:'/api/member/shared-events',headers:{host:'nai.example',origin:'https://nai.example',cookie:'nai_member_session='+session.token}},res=>{
        assert.equal(res.statusCode,200);let text='',changed=false;
        res.on('data',chunk=>{
          text+=chunk;
          if(!changed&&text.includes('event: shared-status')){
            changed=true;db.users[0].discord.checkedAt=Date.now()-300001;
            if(temporary)upstream.fail=true;else upstream.member=false;
          }
        });
        res.on('end',()=>resolve(text));res.on('error',reject);
      });
      req.on('error',reject);req.setTimeout(6000,()=>{req.destroy();reject(new Error('synthetic SSE close timeout'));});
    });
    assert.match(events,/event: shared-status/);
    if(temporary){assert.ok(!events.includes('auth-expired'));assert.ok(await authenticateMemberSession(deps.store,session.token,env));}
    else{assert.match(events,/event: auth-expired\ndata: \{\}/);assert.equal(await authenticateMemberSession(deps.store,session.token,env),null);}
  }
});

test('Discord 配置不完整时不能回退密码，回调只使用固定 HTTPS 地址',()=>{
  assert.equal(discordLoginEnabled({}),false);assert.equal(discordConfig({}),null);
  for(const changes of [{DISCORD_CLIENT_SECRET:''},{DISCORD_GUILD_ID:'invalid'},{PUBLIC_BASE_URL:'http://nai.example'},{PUBLIC_BASE_URL:'https://nai.example/?redirect=evil'},{PUBLIC_BASE_URL:'https://name:secret@nai.example'}])assert.throws(()=>discordConfig({...env,...changes}),e=>e.statusCode===503);
});

test('邀请 + OAuth + 服务器验证绑定原钱包，纯 Discord 再登录，密码与旧会话不能绕过',async t=>{
  const {db,deps,call,admin,start,finish}=await fixture(t);
  const oldCookie='nai_member_session='+(await createMemberSession(deps.store,db.users[0],env)).token;
  assert.equal((await call('/api/member/login',{body:{username:'member-a',password:'synthetic-password-for-a'}})).status,403);
  assert.equal((await call('/api/member/me',{cookie:oldCookie})).status,403);
  assert.equal((await call('/api/member/login-options')).data.discord,true);
  assert.equal((await call('/api/member/discord/start',{body:{},headers:{origin:'https://evil.example'}})).status,403);
  assert.equal((await call('/api/admin/member-management',{body:{action:'discord-invite',userId:'a'},headers:{'x-admin-token':'synthetic-admin'}})).status,404);
  const invitation=await admin({action:'discord-invite',userId:'a'});assert.equal(invitation.status,200);
  assert.ok(!JSON.stringify(db).includes(invitation.data.code));
  const quota=structuredClone(db.settings.fairQuota),key=db.users[0].token;
  const flow=await start(invitation.data.code),result=await finish(flow);
  assert.equal(result.status,303);assert.equal(result.headers.location,'https://nai.example/');
  assert.equal(result.headers['set-cookie'].length,2);
  const cookie=result.headers['set-cookie'].find(s=>s.startsWith('nai_member_session=')).split(';')[0];
  assert.deepEqual(db.settings.fairQuota,quota);assert.equal(db.users[0].token,key);
  assert.equal(db.users[0].discord.id,'222222222222222222');assert.equal(db.users[0].discordInvite,undefined);assert.equal(db.users[0].memberAuth.hash,undefined);
  const me=await call('/api/member/me',{cookie});assert.equal(me.status,200);assert.equal(me.data.quota.share,.25);
  assert.ok(!me.text.includes('synthetic-access'));assert.ok(!me.text.includes('refreshToken'));
  await requireDiscordMembership(db.users[0],deps);
  assert.equal((await call('/api/member/me',{cookie:oldCookie})).status,401);
  assert.equal((await finish(flow)).status,400);
  assert.equal((await call('/api/member/discord/start',{body:{invite:invitation.data.code}})).status,403);
  assert.equal((await finish(await start())).status,303);
  assert.equal((await admin({action:'discord-invite',userId:'a'})).status,409);
});

test('state 需同浏览器且只能用一次，过期或撤销的邀请码不能绑定',async t=>{
  const {db,admin,start,finish,call}=await fixture(t);
  let invitation=(await admin({action:'discord-invite',userId:'b'})).data;
  const flow=await start(invitation.code);
  assert.equal((await finish(flow,{cookie:'nai_discord_state=wrong'})).status,400);
  await admin({action:'discord-invite',userId:'b'});
  assert.equal((await finish(flow)).status,403);assert.equal(db.users[1].discord,undefined);
  invitation=(await admin({action:'discord-invite',userId:'b'})).data;
  db.users[1].discordInvite.expiresAt=Date.now()-1;
  assert.equal((await call('/api/member/discord/start',{body:{invite:invitation.code}})).status,403);
  const valid=(await admin({action:'discord-invite',userId:'b'})).data;
  const expiring=await start(valid.code);
  t.mock.method(Date,'now',()=>valid.expiresAt+1);
  assert.equal((await finish(expiring)).status,400);
});

test('非服务器成员、待审核成员、权限不足及未获邀用户均拒绝且不消耗邀请码',async t=>{
  const {db,upstream,admin,start,finish}=await fixture(t);
  const invitation=(await admin({action:'discord-invite',userId:'c'})).data;
  upstream.member=false;assert.equal((await finish(await start(invitation.code))).status,403);
  assert.ok(db.users[2].discordInvite);assert.equal(db.users[2].discord,undefined);
  upstream.member=true;upstream.pending=true;assert.equal((await finish(await start(invitation.code))).status,403);
  upstream.pending=false;upstream.scope='identify';assert.equal((await finish(await start(invitation.code))).status,403);
  upstream.scope='identify guilds.members.read';assert.equal((await finish(await start())).status,403);
  assert.equal(db.users.length,4);
});

test('同一 Discord 不能领取两个钱包，停用成员不能完成待处理邀请',async t=>{
  const {db,admin,start,finish}=await fixture(t);
  const a=(await admin({action:'discord-invite',userId:'a'})).data;
  const b=(await admin({action:'discord-invite',userId:'b'})).data;
  assert.equal((await finish(await start(a.code))).status,303);
  assert.equal((await finish(await start(b.code))).status,403);assert.equal(db.users[1].discord,undefined);
  const c=(await admin({action:'discord-invite',userId:'c'})).data,flow=await start(c.code);
  db.users[2].enabled=false;
  assert.equal((await finish(flow)).status,403);
});

test('成员复查共享缓存，刷新令牌持久化后即使查询故障也不丢失，退服撤销会话与 Key 权限',async t=>{
  const {db,deps,upstream,admin,start,finish,call}=await fixture(t);
  const invitation=(await admin({action:'discord-invite',userId:'d'})).data;
  const result=await finish(await start(invitation.code));
  const cookie=result.headers['set-cookie'].find(s=>s.startsWith('nai_member_session=')).split(';')[0];
  const user=db.users[3];user.discord.checkedAt=0;user.discord.expiresAt=0;upstream.fail=true;
  await assert.rejects(requireDiscordMembership(user,deps),e=>e.statusCode===503&&!e.message.includes('synthetic-upstream-secret'));
  assert.equal(user.discord.refreshToken,'synthetic-refresh-1');assert.equal(upstream.refreshes,1);
  upstream.fail=false;const before=upstream.calls.length;
  await Promise.all([requireDiscordMembership(user,deps),requireDiscordMembership(user,deps)]);
  assert.equal(upstream.refreshes,1);assert.equal(upstream.calls.length-before,1);
  user.discord.checkedAt=0;upstream.member=false;
  await assert.rejects(requireDiscordMembership(user,deps),e=>e.statusCode===403);
  assert.equal(user.discord.member,false);
  const calls=upstream.calls.length;await assert.rejects(requireDiscordMembership(user,deps),e=>e.statusCode===403);assert.equal(upstream.calls.length,calls);
  assert.equal((await call('/api/member/me',{cookie})).status,401);
});
