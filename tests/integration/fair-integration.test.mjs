import test from 'node:test';
import assert from 'node:assert/strict';
import * as fair from '../../server/fair-quota.ts';
import { createGenerationPayloadBudget } from '../../server/generation-request.ts';
import { createGenerationAdmission, requestCacheKey } from '../../server/generation-admission.ts';
import { reserveQueuedGeneration, refundGenerationJob, failGenerationCommand, cancelReservedGeneration } from '../../server/generation-execution.ts';
import { memberGenerationCapabilities } from '../../server/generation-capabilities.ts';
import { availableSharedAccountSlots, physicalAccountLoad } from '../../server/queue-dispatch.ts';
import { generationCost } from '../../server/generation-queries.ts';
function fixture(tier=3) {
  let now=Date.now();
  const db={settings:{},users:['a','b','c','d'].map(id=>({id,token:id,balance:100000})),accounts:[{id:'opus',quotaTier:tier,subscriptionActive:true,enabled:true,quotaFixed:10000,quotaPurchased:0,v5UsagePercent:100,v5UsageTimeUntilNextPercent:0,quotaCheckedAt:now}],jobs:[],ledger:[]};
  fair.configureFairQuota(db,['a','b','c','d']);
  let queue=Promise.resolve();
  const ctx={ queueDispatchEnabled:true,runningJobControls:new Map(),Date:{now:()=>now},
    store:{readFairQuotaSettlement:()=>null,update(fn){queue=queue.catch(()=>{}).then(()=>fn(db));return queue;}} };
  ctx.assertFairMemberReady = token => createGenerationAdmission({ store:ctx.store,now:()=>now,payloadBudget:createGenerationPayloadBudget(()=>0),
    artifactAvailable:async()=>true,cleanupStaleActiveJobs:async()=>{} })({identity:{token},source:'test',request:{model:'nai-diffusion-5-full',width:1024,height:1024,steps:23}});
  ctx.reserveQueuedJob = id => reserveQueuedGeneration(ctx.store, id, { now: () => now,
    dispatchEnabled: () => ctx.queueDispatchEnabled, physicalAccountLoad: id => physicalAccountLoad(ctx.runningJobControls, id) });
  ctx.refundJob = (db, job) => refundGenerationJob(ctx.store, db, job, { now: () => now });
  const failureOptions = () => ({ now: () => now, publicErrorMessage: value => value, errorDetailMessage: error => error.message });
  ctx.failGeneration = (reservation, error) => failGenerationCommand(ctx.store, reservation, error, failureOptions());
  ctx.cancelReservedJob = (reservation, error) => cancelReservedGeneration(ctx.store, reservation, error, failureOptions());
  ctx.availableAccountSlots = (accounts, settings) => availableSharedAccountSlots(accounts, settings, ctx.runningJobControls, now);
  ctx.generationCost = generationCost;
  const job=(id,token='a')=>{const j={id,userToken:token,status:'queued',cost:0,request:{model:'nai-diffusion-5-full',width:1024,height:1024,steps:23}};db.jobs.push(j);return j;};
  return {db,ctx,job,advance:milliseconds=>{now+=milliseconds;}};
}
test('已受理的免费图片派发前再次检查限额，并保留恢复时间，不占上游槽位', async () => {
  const { db, ctx, job } = fixture();
  const queued = job('queued-before-limit');
  queued.request.model = 'nai-diffusion-4-5-full';
  db.settings.fairQuota.freeImagePolicy = { enabled: true, warningThreshold: 1, hardLimit: 1 };
  const retryAt = ctx.Date.now() + 10000;
  ctx.store.readFreeImageUsage = () => ({ enabled: true, hardLimit: 1, successful: 1, reserved: 0, nextAvailableAt: retryAt });
  const result = await ctx.reserveQueuedJob(queued.id);
  assert.equal(result.statusCode, 429);
  assert.equal(result.cooldownUntil, new Date(retryAt).toISOString());
  assert.equal(queued.quotaRetryAfterUntil, result.cooldownUntil);
  assert.equal(queued.status, 'failed');
  assert.equal(queued.fairCharge, undefined);
  assert.ok(!db.accounts[0].inFlight);
});

test('成员child派发超过原确认时保留失败code并停止余图，没有预扣或上游槽位', async () => {
  const { db, ctx, job } = fixture(); fair.syncFairQuota(db,ctx.Date.now());
  const first=job('confirmed-first'), next=job('confirmed-next');
  Object.assign(first,{requestId:'confirmed-parent',sampleIndex:0});
  Object.assign(next,{requestId:'confirmed-parent',sampleIndex:1,status:'pending'});
  const request={...first.request,prompt:'synthetic',seed:1,sampleCount:2,nativeParameters:{width:1024,height:1024,steps:23,seed:1,n_samples:2},
    clientContract:'member-generation-v1',confirmation:{requestFingerprint:'a'.repeat(64),versions:memberGenerationCapabilities(db,db.users[0]).versions,
      maxPerJob:{v5:'0',anlasFixed:'0',anlasPurchased:'0'},maxTotal:{v5:'0',anlasFixed:'0',anlasPurchased:'0'}}};
  ctx.store.readGenerationRequest=()=>request;
  ctx.store.readGenerationRequestJobIds=()=>[first.id,next.id];
  const before=structuredClone(db.settings.fairQuota.balances);
  const result=await ctx.reserveQueuedJob(first.id);
  assert.equal(result.statusCode,409); assert.equal(first.errorCode,'cost_limit_exceeded');
  assert.equal(first.status,'failed'); assert.equal(next.status,'failed');
  assert.equal(first.generationAttempt,undefined); assert.equal(first.fairCharge,undefined);
  assert.ok(!db.accounts[0].inFlight); assert.deepEqual(db.settings.fairQuota.balances,before);
});

test('两位成员同时提交，超过十秒仍等待上一张完成，不重现并发锁',async()=>{
  const {db,ctx,job,advance}=fixture();job('first','a');job('second','b');
  const first=await ctx.reserveQueuedJob('first');
  advance(10000);
  assert.equal((await ctx.reserveQueuedJob('second')).queued,true);
  assert.equal(db.accounts[0].inFlight,1);
  assert.equal(db.jobs[1].fairCharge,undefined);
  advance(1600);await ctx.failGeneration(first,new Error('合成测试释放第一张'));
  assert.equal((await ctx.reserveQueuedJob('second')).queued,true);
  advance(9999);assert.equal((await ctx.reserveQueuedJob('second')).queued,true);
  advance(1);
  assert.equal((await ctx.reserveQueuedJob('second')).job.status,'running');
  assert.equal(db.accounts[0].inFlight,1);
});

test('派发事务拒绝过期队列与停止后恢复的旧任务，不预扣新额度', async () => {
  const { db, ctx, job, advance } = fixture();
  const expired = job('expired');
  expired.queueExpiresAt = new Date(ctx.Date.now() + 1000).toISOString();
  advance(1000);
  const before = structuredClone(db.settings.fairQuota.balances);
  assert.equal((await ctx.reserveQueuedJob(expired.id)).skip, true);
  assert.equal(expired.status, 'failed');
  assert.equal(expired.fairCharge, undefined);
  assert.deepEqual(db.settings.fairQuota.balances, before);
  const old = job('old-batch');
  old.jobAdmissionVersion = 0;
  db.users[0].jobAdmissionVersion = 1;
  db.users[0].acceptingJobs = true;
  assert.equal((await ctx.reserveQueuedJob(old.id)).statusCode, 409);
  assert.equal(old.status, 'failed');
  assert.equal(old.fairCharge, undefined);
});

test('停止接单在派发与收费前拒绝，不预留额度或占用执行槽', async () => {
  const { db, ctx, job } = fixture();
  db.users[0].acceptingJobs = false;
  fair.syncFairQuota(db, ctx.Date.now());
  const balances = structuredClone(db.settings.fairQuota.balances), memberBalance = db.users[0].balance;
  const queued = job('stopped');
  assert.equal((await ctx.reserveQueuedJob(queued.id)).statusCode, 403);
  assert.equal(queued.status, 'failed');
  assert.equal(queued.fairCharge, undefined);
  assert.equal(queued.generationAttempt, undefined);
  assert.equal(db.accounts[0].inFlight, undefined);
  assert.equal(db.accounts[0].lastSharedDispatchAt, undefined);
  assert.deepEqual(db.settings.fairQuota.balances, balances);
  assert.equal(db.users[0].balance, memberBalance);
});

test('同一用户多次提交也只占一个槽位，其余排队；失败退款只发生一次',async()=>{
  const {db,ctx,job,advance}=fixture();for(let i=0;i<5;i++)job(String(i));
  const result=[];
  for(const j of db.jobs){result.push(await ctx.reserveQueuedJob(j.id));advance(10000);}
  assert.equal(result.filter(r=>r.job).length,1);assert.equal(result.filter(r=>r.queued).length,4);
  assert.equal(db.accounts[0].inFlight,1);assert.equal(ctx.availableAccountSlots(db.accounts,db.settings),0);
  const charged=db.settings.fairQuota.balances.a.v5Available;
  await ctx.failGeneration(result[0],new Error('官方失败'));
  const refunded=db.settings.fairQuota.balances.a.v5Available;
  assert.ok(refunded>charged);ctx.refundJob(db,db.jobs[0]);assert.equal(db.settings.fairQuota.balances.a.v5Available,refunded);
  assert.equal((await ctx.reserveQueuedJob('1')).queued,true);advance(10000);
  assert.equal((await ctx.reserveQueuedJob('1')).job.status,'running');assert.equal(db.accounts[0].inFlight,1);
  assert.equal(db.users[0].balance,100000);assert.equal(ctx.generationCost({}),0);
});
test('只有试用时不派发，不使用虚构本地点数；错误可立即返回',async()=>{
  const {db,ctx,job}=fixture(0);await assert.rejects(ctx.assertFairMemberReady('a'),e=>e.statusCode===503);
  // 拒绝预检事务不会妨碍后续独立调度事务。
  const j=job('trial');const r=await ctx.reserveQueuedJob(j.id);
  assert.equal(r.statusCode,503);assert.equal(j.status,'failed');assert.equal(db.accounts[0].inFlight,undefined);
  assert.equal(db.settings.fairQuota.balances.a.v5Available,'0');
});
test('429退款并冷却，不自动换号重试',async()=>{
  const {db,ctx,job}=fixture();job('one');const r=await ctx.reserveQueuedJob('one');
  await ctx.failGeneration(r,Object.assign(new Error('capacity'),{statusCode:429}));
  job('two');assert.equal((await ctx.reserveQueuedJob('two')).queued,true);
  assert.ok(Date.parse(db.accounts[0].cooldownUntil)>Date.now());
});

test('管理员开启续用后个人 V5 不足按原参数转扣 Anlas，失败退回同一钱包',async()=>{
  const {db,ctx,job}=fixture();
  fair.syncFairQuota(db);db.settings.fairQuota.autoAnlasFallback=true;
  db.settings.fairQuota.balances.a.v5Available=0;
  const j=job('fallback');const original=JSON.stringify(j.request);
  const reserved=await ctx.reserveQueuedJob(j.id);
  assert.equal(reserved.job.status,'running');assert.equal(j.fairCharge.resource,'anlas');
  assert.equal(j.fairCharge.billingMode,'anlas_fallback');assert.equal(j.fairCharge.amount,'26');
  assert.equal(JSON.stringify(j.request),original);assert.equal(db.settings.fairQuota.balances.a.anlasFixedAvailable,'2474');
  assert.equal(db.settings.fairQuota.balances.b.anlasFixedAvailable,'2500');
  await ctx.failGeneration(reserved,new Error('模拟上游失败'));
  assert.equal(db.settings.fairQuota.balances.a.anlasFixedAvailable,'2500');
  ctx.refundJob(db,j);assert.equal(db.settings.fairQuota.balances.a.anlasFixedAvailable,'2500');
});

function childrenFixture() {
 const f=fixture();for(const u of f.db.users)u.enabled=true;
 f.db.users.push(...['child_one','child_two'].map(id=>({id,token:id,enabled:true,balance:0})));
 f.db.accounts[0].v5UsageTimeUntilNextPercent=0;
 fair.configureFairGroups(f.db,['a','b','c','d'].map((id,i)=>({id:`group_${i+1}`,name:`组${i+1}`,members:i===0?[id,'child_one','child_two']:[id]})),{redistributeRemaining:true});
 fair.syncFairQuota(f.db);return f;
}
test('同组子账户依次使用共享槽位，未派发者不扣额度，失败只退本人一次',async()=>{
 const {db,ctx,job,advance}=childrenFixture();const q=fair.quoteFairRequest({model:'nai-diffusion-5-full',width:1024,height:1024,steps:23});
 for(let i=0;i<4;i++)job(`child-${i}`,i%2?'child_two':'child_one');job('fifth','b');
 const before=structuredClone(db.settings.fairQuota.balances);
 const results=[];
 for(const j of db.jobs){results.push(await ctx.reserveQueuedJob(j.id));advance(10000);}
 assert.equal(results.filter(r=>r.job).length,1);assert.equal(results.filter(r=>r.queued).length,4);assert.equal(db.accounts[0].inFlight,1);
 assert.ok(Math.abs(db.settings.fairQuota.balances.child_one.v5Available-(before.child_one.v5Available-q.amount))<1e-9);
 assert.deepEqual(db.settings.fairQuota.balances.child_two,before.child_two);
 for(const id of ['a','b','c','d'])assert.deepEqual(db.settings.fairQuota.balances[id],before[id]);
 assert.equal(results[0].job.fairCharge.userId,'child_one');assert.equal(results[0].job.fairCharge.groupId,'group_1');
 await ctx.failGeneration(results[0],new Error('子账户失败'));const refunded=db.settings.fairQuota.balances.child_one.v5Available;
 await ctx.failGeneration(results[0],new Error('重复失败'));ctx.refundJob(db,db.jobs[0]);assert.equal(db.settings.fairQuota.balances.child_one.v5Available,refunded);
 assert.equal(refunded,before.child_one.v5Available);
 assert.equal((await ctx.reserveQueuedJob('child-1')).queued,true);advance(10000);
 assert.equal((await ctx.reserveQueuedJob('child-1')).job.fairCharge.userId,'child_two');
 assert.ok(Math.abs(db.settings.fairQuota.balances.child_two.v5Available-(before.child_two.v5Available-q.amount))<1e-9);
 for(const id of ['b','c','d'])assert.deepEqual(db.settings.fairQuota.balances[id],before[id]);
});

test('并发提交只立即派发一张，其余保持排队且尚未扣款',async()=>{
 const {db,ctx,job,advance}=fixture();
 for(let i=0;i<5;i++)job(`burst-${i}`);
 const results=await Promise.all(db.jobs.map(j=>ctx.reserveQueuedJob(j.id)));
 assert.equal(results.filter(result=>result.job).length,1);
 assert.equal(results.filter(result=>result.queued).length,4);
 assert.equal(Object.keys(db.settings.fairQuota.reservations).length,1);
 assert.equal(db.accounts[0].inFlight,1);
 advance(10000);
 assert.equal((await ctx.reserveQueuedJob('burst-1')).queued,true);
 assert.equal(db.accounts[0].inFlight,1);
});
test('禁用的组内子账户在派发与预检均拒绝，不影响其他成员余额',async()=>{
 const {db,ctx,job}=childrenFixture();db.users.find(u=>u.id==='child_one').enabled=false;
 const before=structuredClone(db.settings.fairQuota.balances);const j=job('disabled-child','child_one');const r=await ctx.reserveQueuedJob(j.id);
 assert.equal(r.statusCode,401);assert.equal(j.status,'failed');assert.equal(db.accounts[0].inFlight,undefined);assert.equal(j.fairCharge,undefined);assert.deepEqual(db.settings.fairQuota.balances,before);
 await assert.rejects(ctx.assertFairMemberReady('child_one'),e=>e.statusCode===401);
});

test('相同提示词和种子不能在不同成员间复用缓存',()=>{
 const request={model:'nai-diffusion-4-5-full',prompt:'synthetic test',seed:42};
 assert.equal(requestCacheKey('member-a',request,42),requestCacheKey('member-a',request,42));
 assert.notEqual(requestCacheKey('member-a',request,42),requestCacheKey('member-b',request,42));
});

test('真实共享失败/取消路径保留响应证据，重复失败与成功后的取消不退款', async () => {
  for (const cancel of [false, true]) {
    const { db, ctx, job } = fixture();
    job('sent'); const reservation = await ctx.reserveQueuedJob('sent');
    reservation.upstreamTiming = { upstreamStartedAt: new Date().toISOString(), upstreamHttpStatus: 502 };
    const finish = cancel ? ctx.cancelReservedJob : ctx.failGeneration;
    await finish(reservation, new Error('synthetic upstream interruption'));
    const refunded = structuredClone(db.settings.fairQuota.balances);
    const loss = db.ledger.filter(entry => entry.type === 'generation_loss_suspected');
    assert.equal(loss.length, 1); assert.equal(loss[0].response.upstreamHttpStatus, 502);
    assert.equal(loss[0].response.dispatchState, 'request_started');
    assert.equal(db.ledger.filter(entry => entry.type === 'generation_refunded').length, 1);
    assert.equal(db.ledger.filter(entry => entry.type === 'refund').length, 0);
    await ctx.failGeneration(reservation, new Error('duplicate')); await ctx.cancelReservedJob(reservation, new Error('duplicate'));
    assert.deepEqual(db.settings.fairQuota.balances, refunded);
    assert.equal(db.ledger.length, 3);
  }
  const { db, ctx, job } = fixture(); job('done');
  const reservation = await ctx.reserveQueuedJob('done');
  fair.settleFairQuota(db, db.jobs[0], true); db.jobs[0].status = 'done';
  const settled = structuredClone(db.settings.fairQuota.balances);
  await ctx.failGeneration(reservation, new Error('late failure'));
  await ctx.cancelReservedJob(reservation, new Error('late cancel')); ctx.refundJob(db, db.jobs[0]);
  assert.deepEqual(db.settings.fairQuota.balances, settled);
  assert.equal(db.ledger.length, 1); assert.equal(db.ledger[0].type, 'generation_reserved');
});
