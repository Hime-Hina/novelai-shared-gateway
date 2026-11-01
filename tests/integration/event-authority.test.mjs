import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,rm } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { JsonStore,verifyEventProjections } from '../../server/store.ts';
import { configureFairQuota,syncFairQuota,reserveFairQuota,settleFairQuota } from '../../server/fair-quota.ts';
import { recordDomainEvent } from '../../server/domain-events.ts';
import { readProjectedFairQuota,readStreamProjection,eventAuthorityEnabled } from '../../server/event-store.ts';
import { createGenerationAdmission } from '../../server/generation-admission.ts';
import { createGenerationPayloadBudget } from '../../server/generation-request.ts';

const now=1793491200000,at=new Date(now).toISOString();
async function fixture(t){
  const dir=await mkdtemp(path.join(os.tmpdir(),'nai-event-authority-'));const store=new JsonStore(dir);await store.init();
  t.after(async()=>{await store.close();await rm(dir,{recursive:true,force:true});});
  const db=structuredClone(store.db);db.users=['a','b','c','d'].map(id=>({id,token:`secret-${id}`,enabled:true,acceptingJobs:true,jobAdmissionVersion:0}));
  db.accounts=[{id:'opus',enabled:true,quotaTier:3,subscriptionActive:true,quotaFixed:10000,quotaPurchased:0,v5UsagePercent:80,quotaCheckedAt:now}];
  configureFairQuota(db,['a','b','c','d']);syncFairQuota(db,now);await store.write(db);return store;
}
test('cutover verifies the shadow, removes duplicate success triggers, and blocks unrecorded money changes',async t=>{
  const store=await fixture(t);await store.enableEventAuthority();assert.equal(eventAuthorityEnabled(store.sqlite),true);
  assert.equal(store.sqlite.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE type='trigger' AND name LIKE 'free_image_%'").get().n,0);
  const before=readProjectedFairQuota(store.sqlite).balances.a.v5Available;
  await assert.rejects(store.update(db=>{db.settings.fairQuota.balances.a.v5Available='999';},{includeSettings:true,collections:[]}),/Domain shadow mismatch/);
  assert.equal(store.db.settings.fairQuota.balances.a.v5Available,before);
  await assert.rejects(store.write(store.db),/event authority/);
  // 受保护认证与可丢的显示字段不冒充领域事实。
  await store.update(db=>{db.users[0].memberAuth={version:'changed-secret-auth-version'};},{dirtyRows:{users:['a']}});
  assert.equal(store.db.users.find(user=>user.id==='a').memberAuth.version,'changed-secret-auth-version');
});
test('authority writes admission, exact reservation and terminal state from the same event commit',async t=>{
  const store=await fixture(t);await store.enableEventAuthority();
  await store.update(db=>{
    db.jobs.push({id:'j',memberId:'a',userToken:'secret-a',status:'queued',sampleIndex:0,createdAt:at,queueSeq:1,request:{model:'nai-diffusion-5-full',width:1024,height:1024,steps:23}});
    recordDomainEvent(db,{eventType:'RequestAccepted',streamId:'request:legacy-j',occurredAt:at,payload:{requestId:null,memberId:'a',createdAt:at,source:'test',jobs:[{id:'j',status:'queued',queueSeq:1,summary:db.jobs[0].request}]}});
  },{dirtyRows:{jobs:['j']}});
  await store.update(db=>{const job=db.jobs.find(job=>job.id==='j');reserveFairQuota(db,db.users[0],job,db.accounts[0],now);job.status='running';job.accountId='opus';},{includeSettings:true,dirtyRows:{jobs:['j']}});
  assert.equal(store.db.settings.fairQuota.reservations.j.status,'reserved');
  await store.update(db=>{
    const job=db.jobs.find(job=>job.id==='j');settleFairQuota(db,job,false,now+1,{requestStarted:false});job.status='failed';job.completedAt=new Date(now+1).toISOString();job.error='synthetic';job.errorCode='generation_failed';job.errorStatus=500;
    recordDomainEvent(db,{eventType:'JobFailed',streamId:'request:legacy-j',occurredAt:now+1,payload:{jobId:'j',failedAt:job.completedAt,errorCode:job.errorCode,errorStatus:500,message:job.error,charge:job.fairCharge,refund:job.generationRefund}});
  },{includeSettings:true,dirtyRows:{jobs:['j']}});
  assert.equal(store.db.settings.fairQuota.reservations.j,undefined);
  assert.equal(JSON.parse(store.sqlite.prepare('SELECT charge FROM fair_quota_settlements WHERE job_id=?').get('j').charge).status,'refunded');
  store.sqlite.transaction(()=>verifyEventProjections(store.sqlite))();
});
test('a verified opening keeps accepted waiting jobs through cutover and still rejects unrecovered running attempts',async t=>{
  const store=await fixture(t),legacy=structuredClone(store.db);
  legacy.jobs=[{id:'waiting',userToken:'secret-a',memberId:'a',status:'queued',createdAt:at,queueSeq:7,queueExpiresAt:new Date(now+60000).toISOString(),
    request:{model:'nai-diffusion-5-full',width:1024,height:1024,steps:23}}];
  await store.write(legacy);await store.enableEventAuthority();
  assert.equal(store.selectItemById('jobs','waiting').status,'queued');
  assert.equal(store.selectItemById('jobs','waiting').queueSeq,7);
  await store.update(db=>{const job=db.jobs[0];reserveFairQuota(db,db.users[0],job,db.accounts[0],now);job.status='running';job.accountId='opus';},{includeSettings:true,dirtyRows:{jobs:['waiting']}});
  await assert.rejects(store.enableEventAuthority(),/Recover running attempts/);
});
test('real single-image admission keeps its parent request without inventing a legacy batch ID',async t=>{
  const store=await fixture(t);await store.enableEventAuthority();
  const admit=createGenerationAdmission({store,payloadBudget:createGenerationPayloadBudget(()=>store.activeGenerationPayloadBytes()),
    artifactAvailable:async()=>true,cleanupStaleActiveJobs:async()=>{},now:()=>now,env:{}});
  const result=await admit({identity:{token:'secret-a'},source:'api',batch:false,noCache:true,
    request:{model:'nai-diffusion-5-full',width:1024,height:1024,steps:23,seed:7,sampleCount:1}});
  const job=store.selectItemById('jobs',result.jobIds[0]);assert.equal(job.requestId,result.requestId);assert.equal(job.batchId,undefined);
  assert.equal(readStreamProjection(store.sqlite,`request:${result.requestId}`).projection.jobs[job.id].batchId,undefined);
});
