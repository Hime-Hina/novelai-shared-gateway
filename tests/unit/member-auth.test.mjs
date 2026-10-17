import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { JsonStore } from '../../server/store.ts';
import * as auth from '../../server/member-auth.ts';

async function open(t, seed, directory) {
 const store=new JsonStore(directory || await mkdtemp(path.join(tmpdir(),'nai-member-auth-')));await store.init();
 t.after(()=>store.close());
 if(seed)await store.update(db=>Object.assign(db,seed),{immediate:true});
 return store;
}
const digest=token=>createHash('sha256').update(token).digest('hex');
test('密码散列、账户唯一、固定七天会话及重置/停用持久撤销',async t=>{
 const db={users:[{id:'a',enabled:true},{id:'b',enabled:true}]};
 await auth.setMemberCredentials(db,'a',{username:'MemberOne',password:'synthetic-A-valid-test-password-123'});
 assert.equal(db.users[0].memberAuth.username,'memberone');assert.ok(!JSON.stringify(db).includes('A-valid-test-password'));
 await assert.rejects(auth.setMemberCredentials(db,'b',{username:'MEMBERONE',password:'synthetic-A-valid-test-password-123'}),e=>e.statusCode===409);
 await assert.rejects(auth.loginMember(db,'memberone','bad'),e=>e.statusCode===401);
 await assert.rejects(auth.loginMember(db,'missing','bad'),e=>e.statusCode===401);
 const user=await auth.loginMember(db,'MEMBERONE','synthetic-A-valid-test-password-123');
 const store=await open(t,db), session=await auth.createMemberSession(store,user,{},1000);
 assert.equal(session.expiresAt,1000+7*86400000);
 assert.equal((await auth.authenticateMemberSession(store,session.token,{},1001)).user.id,'a');
 assert.equal((await auth.authenticateMemberSession(store,session.token,{},session.expiresAt-1)).expiresAt,session.expiresAt);
 const row=store.readMemberSession(digest(session.token));assert.equal(row.expires_at,session.expiresAt);assert.equal(row.owner_binding,null);
 assert.ok(!JSON.stringify(row).includes(session.token));
 assert.equal(await auth.authenticateMemberSession(store,session.token,{},session.expiresAt),null);
 const next=await auth.createMemberSession(store,store.db.users[0]);
 await store.update(db=>auth.setMemberCredentials(db,'a',{username:'memberone',password:'synthetic-Another-valid-password-456'}),{collections:['users'],immediate:true});
 assert.equal(await auth.authenticateMemberSession(store,next.token),null);
 const disabled=await auth.createMemberSession(store,store.db.users[0]);
 await store.update(db=>{db.users[0].enabled=false;},{collections:['users'],immediate:true});
 assert.equal(await auth.authenticateMemberSession(store,disabled.token),null);
 await assert.rejects(auth.loginMember(store.db,'memberone','synthetic-Another-valid-password-456'),e=>e.statusCode===401);
});

test('SQLite重开保留固定到期，退出在同库持久撤销且无效token不访问存储',async t=>{
 const store=await open(t,{users:[{id:'a',enabled:true,memberAuth:{version:'version-a'}}]});
 const session=await auth.createMemberSession(store,store.db.users[0],{},1000),directory=store.dataDir;
 await store.close();const reopened=await open(t,null,directory);
 assert.equal((await auth.authenticateMemberSession(reopened,session.token,{},2000)).expiresAt,session.expiresAt);
 await auth.logoutMemberSession(reopened,session.token);
 await reopened.close();const again=await open(t,null,directory);
 assert.equal(await auth.authenticateMemberSession(again,session.token,{},3000),null);
 assert.equal(again.readMemberSession(digest(session.token)),null);
 const inaccessible={update(){throw new Error('invalid token must not read SQLite');}};
 for(const token of [undefined,null,'','x'.repeat(100000),'a'.repeat(42),'a'.repeat(42)+'!']){
  assert.equal(await auth.authenticateMemberSession(inaccessible,token),null);
  await auth.logoutMemberSession(inaccessible,token);
 }
});

test('owner固定十二小时、普通cookie不能升权；A→B→A和Discord换绑均撤销旧会话',async t=>{
 const guildId='123456789012345678',env={OWNER_USER_ID:'a',DISCORD_GUILD_ID:guildId};
 const users=['a','b','c'].map(id=>({id,enabled:true,memberAuth:{version:id},discord:{id:'discord-'+id,guildId}}));
 const store=await open(t,{users});
 const a=await auth.createMemberSession(store,users[0],env,1000),b=await auth.createMemberSession(store,users[1],env,1000),c=await auth.createMemberSession(store,users[2],env,1000);
 assert.equal(a.expiresAt,1000+12*3600000);assert.equal(b.expiresAt,1000+7*86400000);
 assert.equal(store.readMemberSession(digest(a.token)).owner_binding,JSON.stringify(['a','discord-a',guildId]));
 assert.equal((await auth.authenticateMemberSession(store,a.token,env,a.expiresAt-1)).user.id,'a');
 const next={...env,OWNER_USER_ID:'b'};
 assert.equal(await auth.authenticateMemberSession(store,b.token,next,2000),null,'首次观察升权配置的请求也必须拒绝');
 assert.equal(await auth.authenticateMemberSession(store,a.token,next,2000),null);
 assert.equal((await auth.authenticateMemberSession(store,c.token,next,2000)).user.id,'c');
 const newOwner=await auth.createMemberSession(store,users[1],next,3000);assert.equal(newOwner.expiresAt,3000+12*3600000);
 await auth.reconcileMemberSessionOwner(store,env);
 for(const session of [a,b,newOwner])assert.equal(await auth.authenticateMemberSession(store,session.token,env,4000),null);
 assert.equal((await auth.authenticateMemberSession(store,c.token,env,4000)).expiresAt,c.expiresAt);
 const fresh=await auth.createMemberSession(store,store.db.users[0],env,5000);
 await store.update(db=>{db.users[0].discord.id='new-discord-binding';},{collections:['users'],immediate:true});
 assert.equal(await auth.authenticateMemberSession(store,fresh.token,env,6000),null);
});

test('签发重新核对用户快照，写失败不产生会话；排队鉴权按进入事务时的时间判断到期',async t=>{
 const store=await open(t,{users:[{id:'a',enabled:true,memberAuth:{version:'first'}}]});
 const old=structuredClone(store.db.users[0]);
 await store.update(db=>{db.users[0].memberAuth.version='second';},{collections:['users'],immediate:true});
 await assert.rejects(auth.createMemberSession(store,old,{},1000),e=>e.statusCode===401);
 store.sqlite.exec("CREATE TEMP TRIGGER reject_session BEFORE INSERT ON member_sessions BEGIN SELECT RAISE(ABORT, 'synthetic session failure'); END");
 await assert.rejects(auth.createMemberSession(store,store.db.users[0],{},1000),/synthetic session failure/);
 assert.equal(store.sqlite.prepare('SELECT COUNT(*) AS n FROM member_sessions').get().n,0);
 store.sqlite.exec('DROP TRIGGER reject_session');
 let clock=1000;t.mock.method(Date,'now',()=>clock);
 const session=await auth.createMemberSession(store,store.db.users[0],{});
 let release;const blocked=store.update(()=>new Promise(resolve=>{release=resolve;}),{persist:false});
 await new Promise(resolve=>setImmediate(resolve));
 const pending=auth.authenticateMemberSession(store,session.token,{});
 clock=session.expiresAt;release();await blocked;
 assert.equal(await pending,null,'队列等待不能延长Cookie有效期');
});
test('登录尝试在密码验证前限流且到期可恢复',()=>{
 for(let i=0;i<10;i++)auth.checkMemberLoginRate('rate-user','test-peer',1000);
 assert.throws(()=>auth.checkMemberLoginRate('rate-user','test-peer',1001),e=>e.statusCode===429);
 assert.doesNotThrow(()=>auth.checkMemberLoginRate('rate-user','test-peer',1000+15*60000));
});

test('OAuth 尚无用户身份时仍保留来源请求上限',()=>{
 for(let i=0;i<120;i++)auth.checkMemberLoginRate(null,'synthetic-oauth-peer',1000);
 assert.throws(()=>auth.checkMemberLoginRate(null,'synthetic-oauth-peer',1001),e=>e.statusCode===429);
 assert.doesNotThrow(()=>auth.checkMemberLoginRate(null,'synthetic-oauth-peer',1000+15*60000));
});
