import test from 'node:test';
import assert from 'node:assert/strict';
import {beginDomainCommand,finishDomainCommand} from '../../server/domain-events.ts';
import {configureFairQuota,configureFairGroups,syncFairQuota,reserveFairQuota,settleFairQuota,manageFairMember,manageOperatingFunding,previewFairQuotaCharge,normalizeOpeningQuota} from '../../server/fair-quota.ts';
import {splitQuotaState,composeQuotaState,reduceSharingEvent,reduceAccountQuotaEvent} from '../../server/quota-events.ts';
import {parseAmount,formatAmount} from '../../server/resource-amounts.ts';

const now=1800000000000,members=['a','b','c','d'];
const request={model:'nai-diffusion-5',width:1024,height:1024,steps:23};
function fresh(){return {settings:{},users:members.map(id=>({id,token:id,enabled:true})),ledger:[],jobs:[],accounts:[{id:'opus',enabled:true,quotaTier:3,subscriptionActive:true,quotaFixed:10000,quotaPurchased:1000,v5UsagePercent:80,v5UsageTimeUntilNextPercent:600,quotaCheckedAt:now}]};}
function comparable(state){
 const result=structuredClone(state);
 if(result)result.reservations=Object.fromEntries(Object.entries(result.reservations).filter(([,charge])=>charge.status==='reserved'));
 return result;
}
function audit(db,command){
 const before=db.settings.fairQuota?splitQuotaState(comparable(db.settings.fairQuota)):null;
 let sharing=before?.sharing||null,quota=before?.sharing.boundAccountId?before.quota:null;
 beginDomainCommand(db,{recordedAt:new Date(now).toISOString()});
 const value=command();
 const events=finishDomainCommand(db).events;
 for(const event of events){
   if(event.streamId==='sharing:default')sharing=reduceSharingEvent(sharing,event);
   else if(event.streamId.startsWith('request:'))continue;
   else {assert.equal(event.streamId,'account:opus');quota=reduceAccountQuotaEvent(quota,event);}
 }
 const replayed=sharing?composeQuotaState(sharing,quota):null;
 assert.deepEqual(replayed,comparable(db.settings.fairQuota||null));
 return {value,events};
}
function fixture(){const db=fresh();audit(db,()=>configureFairQuota(db,members));audit(db,()=>syncFairQuota(db,now));return db;}
const fundingContext=db=>({actorId:'a',findEvent:id=>db.ledger.find(event=>event.id===id),coveredAmount:id=>formatAmount(db.ledger.filter(event=>event.type==='operating_loss_covered'&&event.lossEventId===id).reduce((sum,event)=>sum+parseAmount(event.amount),0n))});
const fund=(db,body,at=now)=>audit(db,()=>manageOperatingFunding(db,body,fundingContext(db),at));

test('绑定与首次分配分属配置和账号，实际钱包由语义事件全量重建',()=>{
 const db=fresh();audit(db,()=>configureFairQuota(db,members));
 const {events}=audit(db,()=>syncFairQuota(db,now));
 assert.deepEqual(events.filter(event=>event.streamId==='sharing:default').map(event=>event.eventType),['AccountBound']);
 assert.equal(events.filter(event=>event.eventType==='AllocationApplied').length,3);
 assert.equal(db.settings.fairQuota.balances.a.v5Available,'20');
 assert.equal(db.settings.fairQuota.balances.a.anlasFixedAvailable,'2500');
});

test('报价在命令收集器中仍然纯读，预留退款与恢复各自只发生一次',()=>{
 const db=fixture(),job={id:'j1',request};
 const before=structuredClone(db);
 const quoted=audit(db,()=>previewFairQuotaCharge(db,{id:'a'},request,now+60000));
 assert.equal(quoted.events.length,0);assert.deepEqual(db,before);
 const reserved=audit(db,()=>reserveFairQuota(db,{id:'a'},job,db.accounts[0],now+60000));
 const refill=reserved.events.find(event=>event.eventType==='RefillApplied');
 assert.equal(refill.payload.budget,'0.1');assert.equal(refill.payload.allocations.reduce((sum,entry)=>sum+parseAmount(entry.amount),0n),100000000000n);
 assert.equal(reserved.value.amount,quoted.value.amount);
 audit(db,()=>settleFairQuota(db,job,false,now+60000));
 assert.equal(db.settings.fairQuota.balances.a.v5Available,'20.025');
 assert.equal(audit(db,()=>settleFairQuota(db,job,false,now+60001)).events.length,0);
 assert.equal(audit(db,()=>syncFairQuota(db,now+60000)).events.length,0);
});

test('重分与转移在同一命令产生配置事实和账号金额向量，删除不丢额度',()=>{
 const db=fixture();db.users.push({id:'e',token:'e',enabled:true});
 const groups=db.settings.fairQuota.groups.map((group,index)=>index?group:{...group,members:['a','e'],memberWeights:{a:1,e:2}});
 const changed=audit(db,()=>configureFairGroups(db,groups,{now,redistributeRemaining:true}));
 assert.equal(changed.events.at(-2).eventType,'AllocationChanged');assert.equal(changed.events.at(-2).streamId,'sharing:default');
 assert.equal(changed.events.at(-1).eventType,'FundsTransferred');assert.equal(changed.events.at(-1).streamId,'account:opus');
 assert.equal(db.settings.fairQuota.balances.a.anlasFixedAvailable,'833.333333333333');
 assert.equal(db.settings.fairQuota.balances.e.anlasFixedAvailable,'1666.666666666667');
 audit(db,()=>manageFairMember(db,{action:'transfer-quota',fromUserId:'e',toUserId:'a',resource:'anlasFixed',amount:1,expectedConfigVersion:2},now));
 audit(db,()=>manageFairMember(db,{action:'delete-member',userId:'e',transferToUserId:'a',expectedConfigVersion:3},now));
 assert.equal(db.settings.fairQuota.balances.a.anlasFixedAvailable,'2500');
 assert.equal(db.settings.fairQuota.balances.e,undefined);
});

test('Anlas净额抵消、聚合缺口、回跳、注资与核销均能独立重放',()=>{
 const db=fixture();db.accounts[0].v5UsageTimeUntilNextPercent=0;db.settings.fairQuota.snapshot.seconds=0;
 fund(db,{action:'operating-contribute',resource:'anlasFixed',amount:100,receipt:'contribute_1'});
 Object.assign(db.accounts[0],{quotaFixed:9900,quotaCheckedAt:now+1});audit(db,()=>syncFairQuota(db,now+1));
 assert.equal(db.settings.fairQuota.operating.unfunded.anlasFixed,'100');
 const loss=db.settings.fairQuota.operating.gapEventIds.anlasFixed;
 fund(db,{action:'operating-cover-loss',resource:'anlasFixed',amount:40,receipt:'cover_loss_1',lossEventId:loss},now+1);
 Object.assign(db.accounts[0],{quotaFixed:9960,quotaCheckedAt:now+2});audit(db,()=>syncFairQuota(db,now+2));
 assert.equal(db.settings.fairQuota.operating.unattributedReturns.anlasFixed,'60');
 fund(db,{action:'operating-reconcile-return',resource:'anlasFixed',amount:60,receipt:'return_fix_1',lossEventId:loss},now+2);
 fund(db,{action:'operating-register-funding',resource:'anlasFixed',amount:40,receipt:'external_1'},now+3);
 Object.assign(db.accounts[0],{quotaFixed:10000,quotaCheckedAt:now+4});audit(db,()=>syncFairQuota(db,now+4));
 assert.equal(db.settings.fairQuota.operating.available.anlasFixed,'100');
 assert.equal(db.settings.fairQuota.operating.pendingExternalFunding.length,0);
});

test('最小单位在大余额连续扣退后保留，opening整体规范化保存精确误差',()=>{
 const db=fixture(),s=db.settings.fairQuota;
 s.balances.a.anlasFixedAvailable='2500.000000000001';s.balances.a.anlasAvailable='2750.000000000001';
 const job={id:'fixed-precision',request:{...request,model:'nai-diffusion-5-full',steps:29}};
 audit(db,()=>reserveFairQuota(db,{id:'a'},job,db.accounts[0],now));
 audit(db,()=>settleFairQuota(db,job,false,now));
 assert.equal(s.balances.a.anlasFixedAvailable,'2500.000000000001');
 const legacy=structuredClone(s);legacy.reservations={};legacy.operating.available.anlasFixed=0;
 legacy.balances.a.anlasFixedAvailable=1/3;legacy.balances.b.anlasFixedAvailable=1/3;legacy.balances.c.anlasFixedAvailable=1/3;legacy.balances.d.anlasFixedAvailable=0;
 const reports=[],normalized=normalizeOpeningQuota(legacy,reports),report=reports.find(entry=>entry.resource==='anlasFixed');
 assert.equal(report.rawTotal,'0.9999999999999999');assert.equal(report.normalizedTotal,'1');assert.equal(report.totalDelta,'0.0000000000000001');
 assert.equal(normalized.balances.a.anlasFixedAvailable,'0.333333333334');
 assert.equal(normalized.balances.b.anlasFixedAvailable,'0.333333333333');
 assert.equal(normalized.balances.c.anlasFixedAvailable,'0.333333333333');
 assert.equal(legacy.balances.a.anlasFixedAvailable,1/3);
});

test('fallback结算保留原购买路径与共享债务，后续快照抵扣和恢复可重放',()=>{
 const db=fixture(),s=db.settings.fairQuota;
 s.autoAnlasFallback=true;s.balances.a.v5Available='0';s.snapshot.v5='60';s.snapshot.seconds=0;
 Object.assign(db.accounts[0],{v5UsagePercent:60,v5UsageTimeUntilNextPercent:0});
 const job={id:'fallback-event',request};
 const charge=audit(db,()=>reserveFairQuota(db,{id:'a'},job,db.accounts[0],now)).value;
 assert.equal(charge.amount,'26');assert.equal(charge.v5Equivalent,'0.057803468209');
 audit(db,()=>settleFairQuota(db,job,true,now+1));
 assert.equal(s.sharedV5Debt,'0.057803468209');assert.equal(s.fallbackBuffer.fixed,'26');
 Object.assign(db.accounts[0],{quotaCheckedAt:now+2});audit(db,()=>syncFairQuota(db,now+2));
 assert.equal(s.fallbackBuffer.fixed,'26');
 Object.assign(db.accounts[0],{quotaCheckedAt:now+3,quotaFixed:9974,v5UsageTimeUntilNextPercent:600});audit(db,()=>syncFairQuota(db,now+3));
 assert.equal(s.fallbackBuffer.fixed,'0');
 const {events}=audit(db,()=>syncFairQuota(db,now+60003));
 const refill=events.find(event=>event.eventType==='RefillApplied');
 assert.equal(refill.payload.debtRepaid,'0.057803468209');assert.equal(s.sharedV5Debt,'0');
});

test('V5容量缺口优先使用恢复量，净增额内有收据确认的Anlas扣款只释放一次',()=>{
 const db=fixture(),s=db.settings.fairQuota;s.snapshot.seconds=0;db.accounts[0].v5UsageTimeUntilNextPercent=0;
 Object.assign(db.accounts[0],{quotaCheckedAt:now+1,v5UsagePercent:79.5,v5UsageTimeUntilNextPercent:600});audit(db,()=>syncFairQuota(db,now+1));
 const before=structuredClone(s.balances),recovery=audit(db,()=>syncFairQuota(db,now+300001));
 assert.equal(recovery.events.find(event=>event.eventType==='RefillApplied').payload.capacityRecovered,'0.5');
 assert.deepEqual(s.balances,before);assert.equal(s.operating.v5CapacityGap,'0');
 const paid=fixture(),job={id:'confirmed-debit',request:{...request,model:'nai-diffusion-5-full',steps:29}};
 const charge=audit(paid,()=>reserveFairQuota(paid,{id:'a'},job,paid.accounts[0],now)).value;
 assert.equal(charge.amount,'32');audit(paid,()=>settleFairQuota(paid,job,true,now+1));
 Object.assign(paid.accounts[0],{quotaFixed:10968,quotaCheckedAt:now+2});audit(paid,()=>syncFairQuota(paid,now+2));
 assert.equal(paid.settings.fairQuota.spent.anlasFixed,'32');
 fund(paid,{action:'operating-confirm-debits',resource:'anlasFixed',amount:32,receipt:'net_confirm_1'},now+3);
 assert.equal(paid.settings.fairQuota.spent.anlasFixed,'0');assert.equal(paid.settings.fairQuota.balances.a.anlasFixedAvailable,'2718');
 assert.equal(fund(paid,{action:'operating-confirm-debits',resource:'anlasFixed',amount:32,receipt:'net_confirm_1'},now+4).events.length,0);
});

test('重放拒绝重复恢复和无预留终态，未知事件不能跳过',()=>{
 const db=fixture(),base=splitQuotaState(db.settings.fairQuota).quota;
 assert.throws(()=>reduceAccountQuotaEvent(structuredClone(base),{eventType:'RefillApplied',payload:{from:now-1,until:now,budget:'1',debtRepaid:'0',capacityRecovered:'0',allocations:[]}}),/interval/);
 assert.throws(()=>reduceAccountQuotaEvent(structuredClone(base),{eventType:'GenerationRefunded',payload:{jobId:'missing',charge:{},at:now}}),/reservation/);
 assert.throws(()=>reduceAccountQuotaEvent(structuredClone(base),{eventType:'FutureUnknown',payload:{}}),/support/);
});

test('管理金额超过12位精度明确拒绝，不静默接受零金额业务操作',()=>{
 const db=fixture(),before=structuredClone(db);
 assert.throws(()=>manageFairMember(db,{action:'transfer-quota',fromUserId:'a',toUserId:'b',resource:'v5',amount:1e-13,expectedConfigVersion:1},now),/12 位/);
 assert.throws(()=>manageOperatingFunding(db,{action:'operating-contribute',resource:'v5',amount:1e-13,receipt:'precision_input'},fundingContext(db),now),/12 位/);
 assert.deepEqual(db,before);
});

test('成员资格先于分组存在，停止接单代次保存在sharing且不泄漏旧额度DTO',()=>{
 let sharing=reduceSharingEvent(null,{eventType:'MemberEligibilityChanged',payload:{memberId:'a',enabled:true,discord:{id:'discord-a',guildId:'guild',checkedAt:now,member:true}}});
 assert.equal(composeQuotaState(sharing,null),null);
 sharing=reduceSharingEvent(sharing,{eventType:'AdmissionChanged',payload:{memberId:'a',acceptingJobs:false,jobAdmissionVersion:1,at:now+1}});
 assert.deepEqual(sharing.memberEligibility.a,{memberId:'a',enabled:true,discord:{id:'discord-a',guildId:'guild',checkedAt:now,member:true},acceptingJobs:false,jobAdmissionVersion:1,at:now+1});
 const db=fixture();
 sharing=reduceSharingEvent(sharing,{eventType:'SharingConfigured',payload:{members,groups:db.settings.fairQuota.groups,configVersion:1}});
 assert.equal(sharing.memberEligibility.a.jobAdmissionVersion,1);
 assert.equal(Object.hasOwn(composeQuotaState(sharing,null),'memberEligibility'),false);
 assert.throws(()=>reduceSharingEvent(structuredClone(sharing),{eventType:'AdmissionChanged',payload:{memberId:'a',acceptingJobs:true,jobAdmissionVersion:0,at:now+2}}),/backwards/);
});

test('最终预留决定在扣款前授权，1ms恢复跨fallback边界与已有预留均不能绕过确认',()=>{
 const db=fixture(),s=db.settings.fairQuota,job={id:'final-charge-authorization',request};
 s.autoAnlasFallback=true;s.balances.a.v5Available='0.0578031';
 assert.equal(previewFairQuotaCharge(db,{id:'a'},request,now).resource,'anlas');
 let authorized;
 assert.throws(()=>reserveFairQuota(db,{id:'a'},job,db.accounts[0],now+1,request,null,charge=>{
   authorized=structuredClone(charge);
   if(charge.resource==='v5')throw new Error('V5 was not confirmed');
 }),/not confirmed/);
 assert.equal(authorized.resource,'v5');assert.equal(authorized.amount,'0.057803468209');
 assert.equal(s.balances.a.v5Available,'0.057803516667');assert.equal(s.balances.a.anlasFixedAvailable,'2500');
 assert.equal(s.reservations[job.id],undefined);assert.equal(job.fairCharge,undefined);assert.equal(job.generationAttempt,undefined);
 reserveFairQuota(db,{id:'a'},job,db.accounts[0],now+1,request,null,charge=>assert.equal(charge.resource,'v5'));
 const before=structuredClone(s),ledger=structuredClone(db.ledger);
 assert.throws(()=>reserveFairQuota(db,{id:'a'},job,db.accounts[0],now+1,request,null,()=>{throw new Error('reused reservation denied');}),/denied/);
 assert.deepEqual(s,before);assert.deepEqual(db.ledger,ledger);
});

test('新观测已保存恢复游标时不追加零金额tick，独立tick仍保存未归档游标',()=>{
 const db=fixture(),s=db.settings.fairQuota;
 for(const id of members)s.balances[id].v5Available='25';s.snapshot.v5='100';db.accounts[0].v5UsagePercent=100;
 db.accounts[0].quotaCheckedAt=now+60000;
 const observed=audit(db,()=>syncFairQuota(db,now+60000));
 assert.equal(observed.events.some(event=>event.eventType==='RefillApplied'),false);
 assert.equal(observed.events.some(event=>event.eventType==='QuotaObserved'&&event.payload.kind==='reconciled'),true);
 assert.equal(s.lastRefillAt,now+60000);
 const tick=audit(db,()=>syncFairQuota(db,now+60001));
 assert.equal(tick.events.find(event=>event.eventType==='RefillApplied').payload.allocations.length,0);
 assert.equal(s.lastRefillAt,now+60001);
});
