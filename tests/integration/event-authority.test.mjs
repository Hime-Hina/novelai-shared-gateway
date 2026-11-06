import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,rm } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import Database from 'better-sqlite3';
import { JsonStore,verifyEventProjections,applyEventProjection } from '../../server/storage/sqlite-store.ts';
import { configureFairQuota,syncFairQuota,reserveFairQuota,settleFairQuota } from '../../server/quota/fair-share.ts';
import { recordDomainEvent } from '../../server/events/domain-events.ts';
import { readProjectedFairQuota,readStreamProjection,eventAuthorityEnabled,importOpeningFacts,copyProtectedFacts,replayDomainEvents } from '../../server/events/event-store.ts';
import { parseAmount } from '../../server/quota/resource-amounts.ts';
import { createGenerationAdmission } from '../../server/generation/admission.ts';
import { createGenerationPayloadBudget } from '../../server/generation/batch-requests.ts';
import { recordGenerationFailure } from '../../server/generation/execution.ts';
import { migrateStorage } from '../../server/storage/sqlite-migrations.ts';
import { closeGenerationReview } from '../../server/generation/accounting.ts';

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
  const eventCount=store.sqlite.prepare('SELECT count(*) AS n FROM domain_events').get().n;
  await assert.rejects(store.update(db=>{
    db.settings.fairQuota.reservations.forged={status:'refunded',userId:'a',accountId:'opus',resource:'v5',amount:'0',fixed:'0',purchased:'0',configVersion:1,reservedAt:now,settledAt:now};
  },{includeSettings:true,collections:[]}),/Unrecorded terminal settlement forged/);
  assert.equal(store.sqlite.prepare('SELECT charge FROM fair_quota_settlements WHERE job_id=?').get('forged'),undefined);
  assert.equal(store.sqlite.prepare('SELECT count(*) AS n FROM domain_events').get().n,eventCount);
  const mapping={member_id:'a',operation:'generate',key_hash:'forged',fingerprint:'forged',normalization_context:'{}',job_id:'forged',request_id:null,created_at:at};
  await assert.rejects(store.update(()=>true,{collections:[],generationIdempotency:mapping}),/Unrecorded generation idempotency/);
  assert.equal(store.sqlite.prepare('SELECT job_id FROM generation_idempotency WHERE key_hash=?').get('forged'),undefined);
  assert.equal(store.sqlite.prepare('SELECT count(*) AS n FROM domain_events').get().n,eventCount);
  store.sqlite.transaction(()=>verifyEventProjections(store.sqlite))();
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
    const job=db.jobs.find(job=>job.id==='j');settleFairQuota(db,job,false,now+1,{requestStarted:false});job.status='failed';job.completedAt=new Date(now+1).toISOString();job.error='synthetic';job.errorCode='generation_failed';job.errorStatus=500;job.errorDetail='synthetic protected diagnostic';
    recordGenerationFailure(db,job);
  },{includeSettings:true,dirtyRows:{jobs:['j']}});
  assert.equal(store.db.settings.fairQuota.reservations.j,undefined);
  assert.equal(JSON.parse(store.sqlite.prepare('SELECT charge FROM fair_quota_settlements WHERE job_id=?').get('j').charge).status,'refunded');
  store.sqlite.transaction(()=>verifyEventProjections(store.sqlite))();
  const count=store.sqlite.prepare('SELECT count(*) AS n FROM generation_diagnostics').get().n;
  await assert.rejects(store.update(db=>{const job=db.jobs.find(job=>job.id==='j');job.errorDetail='rolled-back diagnostic';recordGenerationFailure(db,job);},
    {dirtyRows:{jobs:['j']},domainCommand:{expectedVersions:{'request:legacy-j':'0'}}}),/Stream version conflict/);
  assert.equal(store.sqlite.prepare('SELECT count(*) AS n FROM generation_diagnostics').get().n,count);
  assert.equal(store.db.jobs[0].errorDetail,'synthetic protected diagnostic');
  assert.equal(store.sqlite.prepare('SELECT payload FROM domain_events').all().some(row=>row.payload.includes('synthetic protected diagnostic')),false);
  const target=new Database(':memory:');try{migrateStorage(target);copyProtectedFacts(store.sqlite,target);replayDomainEvents(store.sqlite,target,{project:applyEventProjection});
    assert.equal(JSON.parse(target.prepare('SELECT data FROM jobs WHERE id=?').get('j').data).errorDetail,'synthetic protected diagnostic');
  }finally{target.close();}
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
test('unchanged related member projections avoid SQL updates and cache refresh while real changes remain checked',async t=>{
  const store=await fixture(t);await store.enableEventAuthority();
  store.sqlite.exec(`CREATE TEMP TABLE member_updates(id TEXT);
    CREATE TEMP TRIGGER count_member_update AFTER UPDATE ON users BEGIN INSERT INTO member_updates(id) VALUES(NEW.id); END;`);
  const members=new Map(store.db.users.map(user=>[user.id,user]));
  const admit=createGenerationAdmission({store,payloadBudget:createGenerationPayloadBudget(()=>store.activeGenerationPayloadBytes()),
    artifactAvailable:async()=>true,cleanupStaleActiveJobs:async()=>{},now:()=>now,env:{}});
  const result=await admit({identity:{token:'secret-a'},source:'api',batch:false,noCache:true,
    request:{model:'nai-diffusion-5-full',width:1024,height:1024,steps:23,seed:17,sampleCount:1}});
  const jobId=result.jobIds[0],artifact={id:'prepared',imageId:'prepared',attemptId:'attempt',file:'prepared.webp',mimeType:'image/webp',sha256:'synthetic',byteLength:1};
  await store.update(db=>{
    const {id,...descriptor}=artifact;db.jobs.find(job=>job.id===jobId).resultPending=descriptor;
    recordDomainEvent(db,{eventType:'ArtifactPrepared',streamId:`request:${result.requestId}`,payload:{jobId,artifact}});
  },{dirtyRows:{jobs:[jobId]}});
  assert.deepEqual(store.sqlite.prepare('SELECT id FROM member_updates').all(),[]);
  for(const user of store.db.users)assert.strictEqual(user,members.get(user.id));
  await assert.rejects(store.update(db=>{db.users.find(user=>user.id==='b').enabled=false;},{dirtyRows:{users:['b']}}),/Domain shadow mismatch/);
  assert.deepEqual(store.sqlite.prepare('SELECT id FROM member_updates').all(),[]);
  await store.update(db=>{
    const user=db.users.find(user=>user.id==='b');user.enabled=false;user.updatedAt=at;
    recordDomainEvent(db,{eventType:'MemberEligibilityChanged',streamId:'sharing:default',payload:{memberId:'b',enabled:false,at}});
  },{dirtyRows:{users:['b']}});
  assert.deepEqual(store.sqlite.prepare('SELECT id FROM member_updates').all(),[{id:'b'}]);
  assert.equal(store.selectItemById('users','b').enabled,false);
  store.sqlite.transaction(()=>verifyEventProjections(store.sqlite))();
});
test('real cached admission preserves the recorded result account without a new charge',async t=>{
  const store=await fixture(t),legacy=structuredClone(store.db);
  legacy.images=[{id:'cached-image',token:'secret-a',memberId:'a',accountId:'opus',file:'cached.png',createdAt:at}];
  await store.write(legacy);await store.enableEventAuthority();
  const before=structuredClone(store.db.settings.fairQuota.balances);
  const admit=createGenerationAdmission({store,payloadBudget:createGenerationPayloadBudget(()=>store.activeGenerationPayloadBytes()),artifactAvailable:async()=>true,cleanupStaleActiveJobs:async()=>{},now:()=>now,env:{}});
  const result=await admit({identity:{token:'secret-a'},source:'api',batch:false,cached:{imageId:'cached-image',accountId:'opus'},
    request:{model:'nai-diffusion-5-full',width:1024,height:1024,steps:23,seed:7,sampleCount:1}});
  const job=store.selectItemById('jobs',result.jobIds[0]);assert.equal(job.status,'done');assert.equal(job.accountId,'opus');assert.equal(job.imageId,'cached-image');
  assert.deepEqual(store.db.settings.fairQuota.balances,before);
});
test('retiring a completed child keeps the authoritative child set and immutable input while siblings remain queued',async t=>{
  const store=await fixture(t);await store.enableEventAuthority();
  const admit=createGenerationAdmission({store,payloadBudget:createGenerationPayloadBudget(()=>store.activeGenerationPayloadBytes()),artifactAvailable:async()=>true,cleanupStaleActiveJobs:async()=>{},now:()=>now,env:{}});
  const result=await admit({identity:{memberId:'a'},source:'native',batch:true,noCache:true,request:{model:'nai-diffusion-5-full',width:1024,height:1024,steps:23,seed:7,sampleCount:2}});
  await store.update(db=>{const job=db.jobs.find(job=>job.id===result.jobIds[0]);job.status='done';job.completedAt=at;
    recordDomainEvent(db,{eventType:'JobCompleted',streamId:`request:${result.requestId}`,payload:{jobId:job.id,completedAt:at}});
  },{dirtyRows:{jobs:[result.jobIds[0]]}});
  const input=store.readGenerationRequest(result.requestId);
  await store.retireJobHistory([result.jobIds[0]]);
  assert.equal(store.readGenerationRequestState(result.requestId,'a').jobs.length,1);
  assert.deepEqual(store.readGenerationRequestJobIds(result.requestId,'a'),result.jobIds);
  assert.deepEqual(store.readGenerationRequest(result.requestId),input);
});
test('cutover installs conservative fixed-point opening balances before the first real command',async t=>{
  const store=await fixture(t),legacy=structuredClone(store.db);
  for(const id of ['a','b','c','d'])Object.assign(legacy.settings.fairQuota.balances[id],{anlasFixedAvailable:id==='d'?0:1/3,anlasPurchasedAvailable:0,anlasAvailable:id==='d'?0:1/3});
  await store.write(legacy);await store.enableEventAuthority();
  assert.equal(store.db.settings.fairQuota.balances.a.anlasFixedAvailable,'0.333333333334');
  assert.equal(Object.values(store.db.settings.fairQuota.balances).reduce((sum,balance)=>sum+parseAmount(balance.anlasFixedAvailable),0n),1000000000000n);
  await store.update(db=>syncFairQuota(db,now),{includeSettings:true,collections:[]});
  assert.deepEqual(store.db.settings.fairQuota,readProjectedFairQuota(store.sqlite));
});
test('a failed cutover rolls back installed amounts, removed triggers and the authority marker',async t=>{
  const store=await fixture(t),legacy=structuredClone(store.db);
  for(const id of ['a','b','c'])Object.assign(legacy.settings.fairQuota.balances[id],{anlasFixedAvailable:1/3,anlasPurchasedAvailable:0,anlasAvailable:1/3});
  await store.write(legacy);importOpeningFacts(store.sqlite);
  const before=store.sqlite.prepare("SELECT value FROM app_settings WHERE key='settings'").get().value;
  const prepare=store.sqlite.prepare.bind(store.sqlite);
  store.sqlite.prepare=sql=>{if(sql.startsWith('INSERT INTO app_settings'))throw new Error('injected cutover SQL failure');return prepare(sql);};
  await assert.rejects(store.enableEventAuthority(),/injected cutover SQL failure/);store.sqlite.prepare=prepare;
  assert.equal(prepare("SELECT value FROM app_settings WHERE key='settings'").get().value,before);
  assert.equal(prepare("SELECT value FROM app_meta WHERE key='event_authority'").get(),undefined);
  assert.equal(prepare("SELECT count(*) AS n FROM sqlite_schema WHERE type='trigger' AND name LIKE 'free_image_%'").get().n,4);
});
test('pending loss reviews remain due after attempted history cleanup and retire only after explicit closure',async t=>{
  const store=await fixture(t),legacy=structuredClone(store.db),job={id:'review-job',memberId:'a',userToken:'secret-a',status:'running',createdAt:at,
    request:{model:'nai-diffusion-5-full',width:1024,height:1024,steps:23},upstreamStartedAt:at};
  legacy.jobs=[job];reserveFairQuota(legacy,legacy.users[0],job,legacy.accounts[0],now);settleFairQuota(legacy,job,false,now+1,{requestStarted:true});
  job.status='failed';job.completedAt=new Date(now+1).toISOString();await store.write(legacy);await store.enableEventAuthority();
  const deadline=store.db.jobs[0].generationReview.deadlineAt;
  assert.equal((await store.readDueGenerationReviews(deadline-1)).length,0);
  await assert.rejects(store.retireJobHistory(['review-job']),/unresolved review/);
  assert.equal((await store.clearRequestLogs()).removed,0);
  assert.equal((await store.readDueGenerationReviews(deadline+1)).length,1);
  await store.update(db=>closeGenerationReview(db,db.jobs[0],deadline+1),{dirtyRows:{jobs:['review-job']}});
  assert.equal((await store.readDueGenerationReviews(deadline+1)).length,0);
  assert.deepEqual(await store.retireJobHistory(['review-job']),['review-job']);
  assert.equal(store.readFairQuotaSettlement('review-job').status,'refunded');
});
test('explicit cache clearing and automatic cache trim record tombstones and cannot resurrect artifacts on replay',async t=>{
  const previous=process.env.NO_AUTO_DELETE;process.env.NO_AUTO_DELETE='false';t.after(()=>{if(previous===undefined)delete process.env.NO_AUTO_DELETE;else process.env.NO_AUTO_DELETE=previous;});
  const store=await fixture(t),legacy=structuredClone(store.db);
  legacy.jobs=[{id:'image-job',memberId:'a',userToken:'secret-a',status:'done',createdAt:at,imageId:'one',lateResultAvailable:true,lateResultImageId:'one'}];
  legacy.images=[{id:'one',memberId:'a',token:'secret-a',file:'images/one.png',jobId:'image-job',createdAt:at},{id:'two',memberId:'b',token:'secret-b',file:'images/two.png',createdAt:at}];
  await store.write(legacy);await store.enableEventAuthority();
  const count=store.sqlite.prepare('SELECT count(*) AS n FROM domain_events').get().n;
  store.sqlite.exec("CREATE TEMP TRIGGER reject_retirement BEFORE DELETE ON images BEGIN SELECT RAISE(ABORT,'synthetic retirement failure'); END");
  await assert.rejects(store.clearCachedImages({ids:['one']}),/synthetic retirement failure/);store.sqlite.exec('DROP TRIGGER reject_retirement');
  assert.equal(store.sqlite.prepare('SELECT count(*) AS n FROM domain_events').get().n,count);
  assert.equal(store.db.jobs[0].imageId,'one');
  assert.equal((await store.clearCachedImages({ids:['one']})).deleted,1);
  assert.equal(store.db.jobs[0].imageId,'');assert.equal(store.db.jobs[0].lateResultAvailable,false);
  const configured=store.db.settings.maxCacheImages;assert.equal((await store.trimImageCache(0,{force:true})).length,1);assert.equal(store.db.settings.maxCacheImages,configured);
  const target=new Database(':memory:');try{migrateStorage(target);copyProtectedFacts(store.sqlite,target);replayDomainEvents(store.sqlite,target,{project:applyEventProjection});
    assert.equal(target.prepare('SELECT count(*) AS n FROM images').get().n,0);assert.equal(JSON.parse(target.prepare('SELECT data FROM jobs WHERE id=?').get('image-job').data).imageId,'');
    target.transaction(()=>verifyEventProjections(target))();
  }finally{target.close();}
});
test('automatic history retention records its decision inside the command and keeps the protection flag unchanged',async t=>{
  const previous=process.env.NO_AUTO_DELETE;process.env.NO_AUTO_DELETE='true';t.after(()=>{if(previous===undefined)delete process.env.NO_AUTO_DELETE;else process.env.NO_AUTO_DELETE=previous;});
  const store=await fixture(t),legacy=structuredClone(store.db);
  legacy.jobs=[{id:'expired',memberId:'a',userToken:'secret-a',status:'done',createdAt:new Date(Date.now()-8*86400000).toISOString()}];
  await store.write(legacy);await store.enableEventAuthority();await store.update(()=>true);
  assert.ok(store.selectItemById('jobs','expired'));
  process.env.NO_AUTO_DELETE='false';await store.update(()=>true);
  assert.equal(store.selectItemById('jobs','expired'),null);
  assert.equal(store.sqlite.prepare("SELECT count(*) AS n FROM domain_events WHERE event_type='JobHistoryRetired'").get().n,1);
  const target=new Database(':memory:');try{migrateStorage(target);copyProtectedFacts(store.sqlite,target);replayDomainEvents(store.sqlite,target,{project:applyEventProjection});assert.equal(target.prepare('SELECT count(*) AS n FROM jobs').get().n,0);}finally{target.close();}
});
