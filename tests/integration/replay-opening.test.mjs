import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { mkdtemp, rm } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { spawnSync } from 'node:child_process';
import { JsonStore, applyEventProjection } from '../../server/store.ts';
import { configureFairQuota, syncFairQuota, reserveFairQuota, settleFairQuota } from '../../server/fair-quota.ts';
import { copyProtectedFacts, importOpeningFacts, readProjectedFairQuota, replayDomainEvents } from '../../server/event-store.ts';
import { migrateStorage } from '../../server/storage-migrations.ts';

const now=1793491200000,at=new Date(now).toISOString();
async function legacyStore(t){
  const directory=await mkdtemp(path.join(os.tmpdir(),'nai-opening-replay-'));
  const store=new JsonStore(directory);await store.init();
  t.after(async()=>{await store.close();await rm(directory,{recursive:true,force:true});});
  const db=structuredClone(store.db);
  db.users=['a','b','c','d'].map(id=>({id,token:`protected-key-${id}`,enabled:true}));
  db.accounts=[{id:'opus',enabled:true,userToken:'protected-PST',quotaTier:3,subscriptionActive:true,quotaFixed:1000,quotaPurchased:0,v5UsagePercent:60,quotaCheckedAt:now}];
  db.jobs=[{id:'legacy-job',userToken:'protected-key-a',status:'queued',createdAt:at,updatedAt:at,queueSeq:8,
    request:{model:'nai-diffusion-5-full',width:1024,height:1024,steps:23,prompt:'private input prompt',nativeParameters:{fixturePayload:'private-base64'}}}];
  configureFairQuota(db,['a','b','c','d']);syncFairQuota(db,now);
  reserveFairQuota(db,db.users[0],db.jobs[0],db.accounts[0],now);
  settleFairQuota(db,db.jobs[0],true,now+1000);
  Object.assign(db.jobs[0],{status:'done',imageId:'legacy-image',accountId:'opus',completedAt:new Date(now+1000).toISOString()});
  db.images=[{id:'legacy-image',jobId:'legacy-job',token:'protected-key-a',file:'images/legacy-image.png',prompt:'private input prompt',createdAt:at,model:'nai-diffusion-5-full'}];
  await store.write(db);
  store.sqlite.prepare('INSERT INTO generation_idempotency(member_id,operation,key_hash,fingerprint,normalization_context,job_id,request_id,created_at) VALUES(?,?,?,?,?,?,?,?)')
    .run('a','generate','old-key-hash','old-intent','{"defaultNegative":"private default prompt"}','legacy-job',null,at);
  store.sqlite.prepare('INSERT INTO free_image_usage(job_id,member_id,succeeded_at,warning_threshold) VALUES(?,?,?,?)').run('historical-free','a',now,10);
  return store;
}

test('opening facts reconstruct quota, historical results, protected inline input, cold idempotency and settlements',async t=>{
  const store=await legacyStore(t);
  const report=importOpeningFacts(store.sqlite);
  assert.equal(report.status,'complete');
  const count=store.sqlite.prepare('SELECT count(*) AS n FROM domain_events').get().n;
  assert.deepEqual(importOpeningFacts(store.sqlite),report);
  assert.equal(store.sqlite.prepare('SELECT count(*) AS n FROM domain_events').get().n,count);
  const eventText=store.sqlite.prepare('SELECT payload FROM domain_events').all().map(row=>row.payload).join('\n');
  for(const secret of ['protected-key','protected-PST','private input prompt','private-base64','private default prompt'])assert.equal(eventText.includes(secret),false,secret);
  const target=new Database(':memory:');t.after(()=>target.close());migrateStorage(target);
  copyProtectedFacts(store.sqlite,target);
  const partial=replayDomainEvents(store.sqlite,target,{maxCommits:2,project:applyEventProjection});
  assert.equal(partial.commits,2);
  replayDomainEvents(store.sqlite,target,{project:applyEventProjection});
  assert.deepEqual(readProjectedFairQuota(target),readProjectedFairQuota(store.sqlite));
  const restored=JSON.parse(target.prepare('SELECT data FROM jobs WHERE id=?').get('legacy-job').data);
  assert.equal(restored.status,'done');assert.equal(restored.memberId,'a');assert.equal(restored.userToken,'protected-key-a');
  assert.equal(restored.request.prompt,'private input prompt');assert.equal(restored.request.nativeParameters.fixturePayload,'private-base64');
  assert.equal(target.prepare('SELECT fingerprint FROM generation_idempotency').get().fingerprint,'old-intent');
  assert.equal(JSON.parse(target.prepare('SELECT charge FROM fair_quota_settlements').get().charge).status,'settled');
  assert.equal(target.prepare('SELECT warning_threshold FROM free_image_usage').get().warning_threshold,10);
  assert.equal(target.prepare('SELECT file FROM (SELECT json_extract(data,\'$.file\') AS file FROM images)').get().file,'images/legacy-image.png');
  assert.equal(target.prepare("SELECT value FROM app_meta WHERE key='queue_sequence'").get().value,'8');
  assert.equal(replayDomainEvents(store.sqlite,target,{project:applyEventProjection}).events,0);
});

test('opening resumes a failed block and refuses a changed source before declaring completion',async t=>{
  const store=await legacyStore(t),prepare=store.sqlite.prepare.bind(store.sqlite);
  let writes=0;
  store.sqlite.prepare=sql=>{
    const statement=prepare(sql);
    if(sql.includes('INSERT INTO event_imports')){const run=statement.run.bind(statement);statement.run=(...args)=>{if(++writes===2)throw new Error('injected import interruption');return run(...args);};}
    return statement;
  };
  assert.throws(()=>importOpeningFacts(store.sqlite),/injected import interruption/);
  store.sqlite.prepare=prepare;
  assert.equal(JSON.parse(prepare("SELECT value FROM app_meta WHERE key='event_opening'").get().value).status,'importing');
  const result=importOpeningFacts(store.sqlite);
  assert.equal(result.status,'complete');
  assert.equal(prepare('SELECT count(*) AS n FROM event_commits').get().n,result.chunks);
  const changed=await legacyStore(t);
  const original=changed.sqlite.prepare.bind(changed.sqlite);let chunks=0;
  changed.sqlite.prepare=sql=>{const statement=original(sql);if(sql.includes('INSERT INTO event_imports')){const run=statement.run.bind(statement);statement.run=(...args)=>{if(++chunks===2)throw new Error('stop');return run(...args);};}return statement;};
  assert.throws(()=>importOpeningFacts(changed.sqlite),/stop/);changed.sqlite.prepare=original;
  const settings=JSON.parse(original("SELECT value FROM app_settings WHERE key='settings'").get().value);settings.fairQuota.balances.a.v5Available='0';
  original("UPDATE app_settings SET value=? WHERE key='settings'").run(JSON.stringify(settings));
  assert.throws(()=>importOpeningFacts(changed.sqlite),/Opening source changed/);
});

test('replay CLI restores a separate database and can resume at a complete commit',async t=>{
  const store=await legacyStore(t);importOpeningFacts(store.sqlite);
  const target=path.join(store.dataDir,'replay','library.sqlite');
  const run=(...extra)=>spawnSync(process.execPath,['scripts/replay-events.mjs',`--source=${store.dbPath}`,`--target=${target}`,...extra],{encoding:'utf8',timeout:15000});
  const partial=run('--max-commits=2');assert.equal(partial.status,0,partial.stderr);assert.equal(JSON.parse(partial.stdout).commits,2);
  const complete=run();assert.equal(complete.status,0,complete.stderr);assert.equal(JSON.parse(complete.stdout).networkCalls,0);
  const repeated=run();assert.equal(repeated.status,0,repeated.stderr);assert.equal(JSON.parse(repeated.stdout).commits,0);
  const rebuilt=new Database(target,{readonly:true});
  try{assert.equal(rebuilt.prepare('SELECT count(*) AS n FROM jobs').get().n,1);}finally{rebuilt.close();}
});
