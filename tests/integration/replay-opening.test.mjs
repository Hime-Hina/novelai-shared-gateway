import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { mkdtemp, rm,mkdir,link,readFile } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { spawnSync } from 'node:child_process';
import { JsonStore, applyEventProjection,verifyEventProjections } from '../../server/store.ts';
import { configureFairQuota, syncFairQuota, reserveFairQuota, settleFairQuota } from '../../server/fair-quota.ts';
import { copyProtectedFacts, importOpeningFacts, readProjectedFairQuota, replayDomainEvents } from '../../server/event-store.ts';
import { migrateStorage } from '../../server/storage-migrations.ts';
import { ImageStorage } from '../../server/r2-storage.ts';
import { createGenerationAdmission } from '../../server/generation-admission.ts';
import { createGenerationPayloadBudget } from '../../server/generation-request.ts';
import { canonicalJson } from '../../server/domain-events.ts';

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

async function inlineOpeningStore(t,count=205){
  const directory=await mkdtemp(path.join(os.tmpdir(),'nai-inline-opening-')),store=new JsonStore(directory);await store.init();
  t.after(async()=>{await store.close();await rm(directory,{recursive:true,force:true});});
  const db=structuredClone(store.db);db.users=[{id:'a',token:'synthetic-private-key',enabled:true}];
  db.jobs=Array.from({length:count},(_,index)=>{
    const id=`inline-${String(index).padStart(3,'0')}`;
    return {id,userToken:'synthetic-private-key',status:'done',createdAt:at,completedAt:at,errorDetail:`private diagnostic ${id}`,
      request:{model:'nai-diffusion-5-full',width:1024,height:1024,steps:23,prompt:`private prompt ${id}`,
        nativeParameters:{image:Buffer.from(`private image ${id}`).toString('base64')}},requestPayload:{body:{privatePrompt:`private payload ${id}`}}};
  });
  await store.write(db);return store;
}
const openingRows=sqlite=>Object.fromEntries(['generation_requests','generation_diagnostics','domain_events','event_commits','event_clock','event_streams','projection_versions','event_imports']
  .map(table=>[table,sqlite.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all()]));

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

test('opening commits private input and diagnostics atomically with each 100-source-row event block',async t=>{
  const cases=[
    {name:'first private insert',completed:0,trigger:"BEFORE INSERT ON generation_requests WHEN NEW.id='legacy-input:inline-000'"},
    {name:'middle private insert',completed:100,trigger:"BEFORE INSERT ON generation_requests WHEN NEW.id='legacy-input:inline-150'"},
    {name:'middle event append',completed:100,trigger:"BEFORE INSERT ON domain_events WHEN NEW.stream_id='request:legacy-inline-150'"},
    {name:'block receipt after events',completed:100,trigger:"BEFORE INSERT ON event_imports WHEN NEW.chunk_id='jobs:1'"},
    {name:'final completion marker',completed:205,trigger:"BEFORE UPDATE ON app_meta WHEN NEW.key='event_opening' AND json_extract(NEW.value,'$.status')='complete'"}
  ];
  for(const {name,completed,trigger} of cases)await t.test(name,async t=>{
    const store=await inlineOpeningStore(t),sqlite=store.sqlite,source=sqlite.prepare('SELECT id,data FROM jobs ORDER BY id').all();
    sqlite.exec(`CREATE TEMP TRIGGER fail_opening ${trigger} BEGIN SELECT RAISE(ABORT,'synthetic opening failure'); END;`);
    assert.throws(()=>importOpeningFacts(sqlite),/synthetic opening failure/);
    assert.equal(JSON.parse(sqlite.prepare("SELECT value FROM app_meta WHERE key='event_opening'").get().value).status,'importing');
    assert.equal(sqlite.prepare('SELECT count(*) AS n FROM generation_requests').get().n,completed);
    assert.equal(sqlite.prepare('SELECT count(*) AS n FROM generation_diagnostics').get().n,completed);
    assert.equal(sqlite.prepare("SELECT count(*) AS n FROM domain_events WHERE stream_kind='request'").get().n,completed);
    assert.equal(sqlite.prepare("SELECT count(*) AS n FROM event_imports WHERE chunk_id LIKE 'jobs:%'").get().n,Math.ceil(completed/100));
    assert.deepEqual(sqlite.prepare('SELECT id,data FROM jobs ORDER BY id').all(),source);
    const prefix=openingRows(sqlite),cursor=sqlite.prepare('SELECT value FROM event_clock').get().value;
    const file=store.dbPath;await store.close();
    const resumed=new Database(file);resumed.pragma('synchronous = FULL');
    try{
      const report=importOpeningFacts(resumed);assert.equal(report.status,'complete');
      assert.equal(resumed.prepare('SELECT count(*) AS n FROM generation_requests').get().n,205);
      assert.equal(resumed.prepare('SELECT count(*) AS n FROM generation_diagnostics').get().n,205);
      assert.equal(resumed.prepare("SELECT count(*) AS n FROM domain_events WHERE stream_kind='request'").get().n,205);
      for(const table of ['domain_events','event_commits'])assert.deepEqual(resumed.prepare(`SELECT * FROM ${table} WHERE commit_seq<=? ORDER BY rowid`).all(cursor),prefix[table]);
      assert.deepEqual(resumed.prepare('SELECT id,data FROM jobs ORDER BY id').all(),source);
      const complete=openingRows(resumed);assert.deepEqual(importOpeningFacts(resumed),report);assert.deepEqual(openingRows(resumed),complete);
      const target=new Database(':memory:');
      try{
        migrateStorage(target);copyProtectedFacts(resumed,target);replayDomainEvents(resumed,target,{project:applyEventProjection});
        assert.equal(target.prepare('SELECT count(*) AS n FROM jobs').get().n,205);
        const job=JSON.parse(target.prepare("SELECT data FROM jobs WHERE id='inline-150'").get().data);
        assert.equal(job.request.prompt,'private prompt inline-150');
        assert.equal(job.request.nativeParameters.image,Buffer.from('private image inline-150').toString('base64'));
        assert.deepEqual(job.requestPayload,{body:{privatePrompt:'private payload inline-150'}});
        assert.equal(job.errorDetail,'private diagnostic inline-150');
      }finally{target.close();}
    }finally{resumed.close();}
  });
});

test('opening resume rejects changes confined to a committed inline prompt, image bytes or requestPayload',async t=>{
  const mutations=[
    ['prompt',job=>{job.request.prompt='changed private prompt';}],
    ['base64',job=>{job.request.nativeParameters.image=Buffer.from('changed private bytes').toString('base64');}],
    ['requestPayload',job=>{job.requestPayload.body.privatePrompt='changed private payload';}]
  ];
  for(const [name,mutate] of mutations)await t.test(name,async t=>{
    const store=await inlineOpeningStore(t,105),sqlite=store.sqlite;
    sqlite.exec("CREATE TEMP TRIGGER stop_opening BEFORE INSERT ON event_imports WHEN NEW.chunk_id='jobs:1' BEGIN SELECT RAISE(ABORT,'stop opening'); END;");
    assert.throws(()=>importOpeningFacts(sqlite),/stop opening/);sqlite.exec('DROP TRIGGER stop_opening');
    const row=sqlite.prepare("SELECT data FROM jobs WHERE id='inline-000'").get(),job=JSON.parse(row.data);mutate(job);
    sqlite.prepare("UPDATE jobs SET data=? WHERE id='inline-000'").run(JSON.stringify(job));
    const before=openingRows(sqlite);
    assert.throws(()=>importOpeningFacts(sqlite),/Opening source changed|Protected legacy .*input conflict/);
    assert.deepEqual(openingRows(sqlite),before);
    assert.equal(JSON.parse(sqlite.prepare("SELECT value FROM app_meta WHERE key='event_opening'").get().value).status,'importing');
  });
});

test('opening validates and reuses matching protected input left by an interrupted older importer',async t=>{
  const store=await inlineOpeningStore(t,1),sqlite=store.sqlite;
  const original=sqlite.prepare("SELECT data FROM jobs WHERE id='inline-000'").get().data,job=JSON.parse(original);
  const payload=canonicalJson({legacyInline:true,request:job.request,requestPayload:job.requestPayload});
  sqlite.prepare('INSERT INTO generation_requests(id,member_id,source,operation,created_at,payload_bytes,payload) VALUES(?,?,?,?,?,?,?)')
    .run('legacy-input:inline-000','a','legacy','generate',at,Buffer.byteLength(payload),payload);
  job.request.prompt='private source changed after old interruption';
  sqlite.prepare("UPDATE jobs SET data=? WHERE id='inline-000'").run(JSON.stringify(job));
  assert.throws(()=>importOpeningFacts(sqlite),/Protected legacy .*input conflict/);
  assert.equal(sqlite.prepare("SELECT count(*) AS n FROM domain_events WHERE stream_kind='request'").get().n,0);
  assert.equal(sqlite.prepare('SELECT payload FROM generation_requests').get().payload,payload);
  assert.equal(sqlite.prepare('SELECT count(*) AS n FROM generation_diagnostics').get().n,0);
  sqlite.prepare("UPDATE jobs SET data=? WHERE id='inline-000'").run(original);
  assert.equal(importOpeningFacts(sqlite).status,'complete');
  assert.equal(sqlite.prepare('SELECT count(*) AS n FROM generation_requests').get().n,1);
  assert.equal(sqlite.prepare('SELECT payload FROM generation_requests').get().payload,payload);
  assert.equal(sqlite.prepare("SELECT count(*) AS n FROM domain_events WHERE stream_kind='request'").get().n,1);
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
test('in-process and CLI replay preserve historical free timestamps and warnings under a changed current policy',async t=>{
  const store=await legacyStore(t);
  store.sqlite.prepare('INSERT INTO fair_quota_settlements(job_id,charge) VALUES(?,?)').run('historical-free',JSON.stringify({resource:'free',status:'settled',userId:'a',accountId:'opus',amount:'0',fixed:'0',purchased:'0',reservedAt:now-1,settledAt:now}));
  store.sqlite.prepare('UPDATE free_image_usage SET succeeded_at=?,warning_threshold=10 WHERE job_id=?').run(now+123,'historical-free');
  const settings=JSON.parse(store.sqlite.prepare("SELECT value FROM app_settings WHERE key='settings'").get().value);
  settings.fairQuota.freeImagePolicy={enabled:true,warningThreshold:1,hardLimit:null};
  store.sqlite.prepare("UPDATE app_settings SET value=? WHERE key='settings'").run(JSON.stringify(settings));
  importOpeningFacts(store.sqlite);
  const expected=store.sqlite.prepare('SELECT * FROM free_image_usage WHERE job_id=?').get('historical-free');
  const target=new Database(':memory:');try{
    migrateStorage(target);copyProtectedFacts(store.sqlite,target);replayDomainEvents(store.sqlite,target,{project:applyEventProjection});
    assert.deepEqual(target.prepare('SELECT * FROM free_image_usage WHERE job_id=?').get('historical-free'),expected);
  }finally{target.close();}
  const targetPath=path.join(store.dataDir,'historical-replay.sqlite');
  const process=spawnSync(globalThis.process.execPath,['scripts/replay-events.mjs',`--source=${store.dbPath}`,`--target=${targetPath}`],{encoding:'utf8',timeout:15000});
  assert.equal(process.status,0,process.stderr);
  const rebuilt=new Database(targetPath,{readonly:true});try{assert.deepEqual(rebuilt.prepare('SELECT * FROM free_image_usage WHERE job_id=?').get('historical-free'),expected);}finally{rebuilt.close();}
});
test('cutover rejects missing, extra and changed terminal, idempotency and free-usage projections',async t=>{
  const mutations={
    fair_quota_settlements:["DELETE FROM fair_quota_settlements","INSERT INTO fair_quota_settlements(job_id,charge) SELECT 'extra',charge FROM fair_quota_settlements LIMIT 1","UPDATE fair_quota_settlements SET charge=json_set(charge,'$.amount','999')"],
    generation_idempotency:["DELETE FROM generation_idempotency","INSERT INTO generation_idempotency SELECT member_id,operation,'extra',fingerprint,normalization_context,job_id,request_id,created_at FROM generation_idempotency LIMIT 1","UPDATE generation_idempotency SET fingerprint='changed'"],
    free_image_usage:["DELETE FROM free_image_usage","INSERT INTO free_image_usage SELECT 'extra',member_id,succeeded_at,warning_threshold FROM free_image_usage LIMIT 1","UPDATE free_image_usage SET succeeded_at=succeeded_at+1"]
  };
  for(const [table,statements] of Object.entries(mutations))for(const mutation of statements){
    const store=await legacyStore(t);importOpeningFacts(store.sqlite);store.sqlite.exec(mutation);
    assert.throws(()=>store.sqlite.transaction(()=>verifyEventProjections(store.sqlite))(),new RegExp(`(?:cold projection|Cold projection mismatch).*${table}`),mutation);
    await assert.rejects(store.enableEventAuthority(),/cold projection|Cold projection mismatch/);
    assert.equal(store.sqlite.prepare("SELECT 1 FROM app_meta WHERE key='event_authority'").get(),undefined);
  }
});
test('old inline output bytes and private diagnostics survive replay after Key rotation using stable parent ownership',async t=>{
  const store=await legacyStore(t),legacy=structuredClone(store.db),bytes=await readFile(new URL('../fixtures/synthetic.webp',import.meta.url));
  legacy.users[0].token='rotated-new-key';
  const payload=JSON.stringify({model:'nai-diffusion-5-full',prompt:'protected parent prompt'});
  store.sqlite.prepare('INSERT INTO generation_requests(id,member_id,source,operation,created_at,payload_bytes,payload) VALUES(?,?,?,?,?,?,?)').run('old-parent','a','native','generate',at,Buffer.byteLength(payload),payload);
  legacy.jobs=[{id:'old-child',requestId:'old-parent',status:'done',userToken:'protected-key-a',imageId:'inline-output',createdAt:at,completedAt:at,errorDetail:'private historical diagnostic /secret/path'}];
  legacy.images=[{id:'inline-output',token:'protected-key-a',base64:bytes.toString('base64'),mimeType:'image/webp',prompt:'private output prompt',fullPrompt:'private output full prompt',createdAt:at}];
  await store.write(legacy);await store.enableEventAuthority();
  const eventText=store.sqlite.prepare('SELECT payload FROM domain_events').all().map(row=>row.payload).join('\n');
  for(const privateText of [bytes.toString('base64'),'private output prompt','private output full prompt','private historical diagnostic'])assert.equal(eventText.includes(privateText),false);
  assert.equal(JSON.parse(store.sqlite.prepare('SELECT data FROM images WHERE id=?').get('inline-output').data).base64,undefined);
  assert.equal(store.db.images.find(image=>image.id==='inline-output').base64,undefined);
  const targetDir=path.join(store.dataDir,'inline-rebuilt');await mkdir(targetDir);
  const target=new Database(path.join(targetDir,'library.sqlite'));migrateStorage(target);copyProtectedFacts(store.sqlite,target);replayDomainEvents(store.sqlite,target,{project:applyEventProjection});target.close();
  const rebuilt=new JsonStore(targetDir);await rebuilt.init();
  try{
    const image=await rebuilt.findImage('inline-output');assert.equal(image.memberId,'a');assert.equal(image.token,'rotated-new-key');
    assert.equal(image.prompt,'private output prompt');assert.equal(image.fullPrompt,'private output full prompt');
    assert.deepEqual(await new ImageStorage(targetDir,null,id=>rebuilt.findImage(id)).read(image),bytes);
    assert.equal(rebuilt.readGenerationRequestState('old-parent','a').jobs[0].imageId,'inline-output');
    assert.equal(rebuilt.selectItemById('jobs','old-child').errorDetail,'private historical diagnostic /secret/path');
  }finally{await rebuilt.close();}
});
test('explicit history retirement survives restart and replay while cold idempotency still returns 410',async t=>{
  const store=await legacyStore(t);await store.enableEventAuthority();
  await store.retireJobHistory(['legacy-job'],'synthetic_history_retention',now+5000);
  assert.equal(store.selectItemById('jobs','legacy-job'),null);
  for(const table of ['generation_idempotency','fair_quota_settlements','free_image_usage'])assert.equal(store.sqlite.prepare(`SELECT count(*) AS n FROM ${table}`).get().n,1);
  await store.close();const reopened=new JsonStore(store.dataDir);await reopened.init();
  try{
    await reopened.enableEventAuthority();
    const admit=createGenerationAdmission({store:reopened,payloadBudget:createGenerationPayloadBudget(()=>reopened.activeGenerationPayloadBytes()),artifactAvailable:async()=>true,cleanupStaleActiveJobs:async()=>{},now:()=>now,env:{}});
    await assert.rejects(admit({identity:{memberId:'a'},source:'native',intent:{keyHash:'old-key-hash',operation:'generate',randomSeed:false,noCache:false,
      normalize:()=>({model:'nai-diffusion-5-full',width:1024,height:1024,steps:23}),fingerprint:()=>'old-intent'}}),error=>error.statusCode===410);
    const target=new Database(':memory:');try{migrateStorage(target);copyProtectedFacts(reopened.sqlite,target);replayDomainEvents(reopened.sqlite,target,{project:applyEventProjection});
      assert.equal(target.prepare('SELECT count(*) AS n FROM jobs').get().n,0);for(const table of ['generation_idempotency','fair_quota_settlements','free_image_usage'])assert.equal(target.prepare(`SELECT count(*) AS n FROM ${table}`).get().n,1);
    }finally{target.close();}
  }finally{await reopened.close();}
});
test('replay rejects an unrelated nonempty target and a hard-linked source before altering either',async t=>{
  const store=await legacyStore(t);importOpeningFacts(store.sqlite);
  const occupied=path.join(store.dataDir,'occupied.sqlite'),other=new Database(occupied);other.exec('CREATE TABLE unrelated_business_records(id TEXT); INSERT INTO unrelated_business_records VALUES(\'untouched\')');other.close();
  const before=await readFile(occupied);
  const run=target=>spawnSync(process.execPath,['scripts/replay-events.mjs',`--source=${store.dbPath}`,`--target=${target}`],{encoding:'utf8',timeout:15000});
  const result=run(occupied);assert.notEqual(result.status,0);assert.match(result.stderr,/target must be empty/);
  assert.deepEqual(await readFile(occupied),before);
  const check=new Database(occupied);try{assert.deepEqual(check.prepare("SELECT name FROM sqlite_schema WHERE type='table'").all(),[{name:'unrelated_business_records'}]);}finally{check.close();}
  const alias=path.join(store.dataDir,'source-alias.sqlite');await link(store.dbPath,alias);
  const same=run(alias);assert.notEqual(same.status,0);assert.match(same.stderr,/same file/);
});
