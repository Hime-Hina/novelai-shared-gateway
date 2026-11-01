import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,rm } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import Database from 'better-sqlite3';
import { JsonStore,activateEventAuthority,verifyEventProjections,applyEventProjection } from '../../server/store.ts';
import { migrateStorage,SUPPORTED_EVENT_AUTHORITY_VERSION } from '../../server/storage-migrations.ts';
import { canonicalJson,domainDigest,DOMAIN_REDUCER_VERSION } from '../../server/domain-events.ts';
import { importOpeningFacts,readStreamProjection,readDomainCommits,appendDomainCommit,copyProtectedFacts,replayDomainEvents } from '../../server/event-store.ts';

const at='2026-11-01T00:00:00.000Z';
async function fixture(t,{running=false}={}) {
  const dir=await mkdtemp(path.join(os.tmpdir(),'nai-event-version-')),store=new JsonStore(dir);
  await store.init();t.after(async()=>{await store.close();await rm(dir,{recursive:true,force:true});});
  const db=structuredClone(store.db);
  db.users=[{id:'member',token:'synthetic-secret',enabled:true,acceptingJobs:true,jobAdmissionVersion:0}];
  db.images=[{id:'unlinked',memberId:'member',token:'synthetic-secret',file:'unlinked.png',createdAt:at}];
  if(running)db.jobs=[{id:'running',memberId:'member',userToken:'synthetic-secret',status:'running',queueSeq:1,createdAt:at,
    resultPending:{imageId:'unpublished',file:'pending.webp',preparedAt:at}}];
  await store.write(db);return store;
}
function markV1(sqlite,{authority=true}={}) {
  sqlite.exec('UPDATE event_streams SET reducer_version=1; UPDATE projection_versions SET reducer_version=1;');
  if(authority){
    const {value}=sqlite.prepare('SELECT CAST(value AS TEXT) AS value FROM event_clock WHERE id=1').get();
    sqlite.prepare("INSERT INTO app_meta(key,value) VALUES('event_authority',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
      .run(JSON.stringify({version:1,minimumWriterVersion:1,commitSeq:value}));
  }
}
function marker(sqlite) { return JSON.parse(sqlite.prepare("SELECT value FROM app_meta WHERE key='event_authority'").get().value); }
function immutableFacts(sqlite) {
  return Object.fromEntries(['event_commits','domain_events','event_clock','generation_requests','artifact_inputs','generation_diagnostics',
    'generation_idempotency','fair_quota_settlements','free_image_usage'].map(table=>[table,sqlite.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all()]));
}

test('v1 authority is rebuilt from its events before atomically requiring writer/reducer v2',async t=>{
  const store=await fixture(t);await store.enableEventAuthority();await store.clearCachedImages({ids:['unlinked']});
  const sqlite=store.sqlite;markV1(sqlite);
  // 模拟 v1 cache 未应用退休语义；单纯给旧 cache 改版本号会令图片复活。
  const old=JSON.parse(sqlite.prepare("SELECT projection FROM event_streams WHERE stream_id='artifact:unlinked'").get().projection);
  delete old.retired;
  sqlite.prepare("UPDATE event_streams SET projection=?,projection_sha256=? WHERE stream_id='artifact:unlinked'").run(canonicalJson(old),domainDigest(old));
  const before=immutableFacts(sqlite),cursor=marker(sqlite).commitSeq;
  assert.throws(()=>readStreamProjection(sqlite,'artifact:unlinked'),/Unsupported reducer version/);
  assert.deepEqual(activateEventAuthority(sqlite),{version:2,commitSeq:cursor});
  assert.equal(SUPPORTED_EVENT_AUTHORITY_VERSION,2);assert.equal(DOMAIN_REDUCER_VERSION,2);
  assert.deepEqual(marker(sqlite),{version:2,minimumWriterVersion:2,commitSeq:cursor});
  assert.ok(readStreamProjection(sqlite,'artifact:unlinked').projection.retired);
  assert.equal(sqlite.prepare("SELECT 1 FROM images WHERE id='unlinked'").get(),undefined);
  assert.deepEqual(sqlite.prepare('SELECT DISTINCT reducer_version FROM event_streams').all(),[{reducer_version:2}]);
  assert.deepEqual(immutableFacts(sqlite),before);
  sqlite.transaction(()=>verifyEventProjections(sqlite))();
});

test('startup upgrades already authoritative v1 running/resultPending work without dispatching or finishing it',async t=>{
  const store=await fixture(t,{running:true});importOpeningFacts(store.sqlite);markV1(store.sqlite);
  const before=immutableFacts(store.sqlite),pending=store.selectItemById('jobs','running').resultPending,dir=store.dataDir;
  await store.close();
  const reopened=new JsonStore(dir);
  try{
    await reopened.init();assert.equal(marker(reopened.sqlite).version,2);
    assert.equal(reopened.selectItemById('jobs','running').status,'running');
    assert.deepEqual(reopened.selectItemById('jobs','running').resultPending,pending);
    assert.deepEqual(immutableFacts(reopened.sqlite),before);
    // 首次权威切换的原有运行中保护仍存在；版本升级已在此前成功。
    await assert.rejects(reopened.enableEventAuthority(),/Recover running attempts/);
  }finally{await reopened.close();}
});

test('unknown authority/reducer versions are rejected before verification, installation or new commits',async t=>{
  const store=await fixture(t);await store.enableEventAuthority();const sqlite=store.sqlite,original=marker(sqlite);
  for(const value of [{version:3,minimumWriterVersion:3},{version:2,minimumWriterVersion:3},{version:0,minimumWriterVersion:0},{version:1}]){
    sqlite.prepare("UPDATE app_meta SET value=? WHERE key='event_authority'").run(JSON.stringify(value));
    const before=sqlite.serialize();
    assert.throws(()=>migrateStorage(sqlite),/compatible writer|unsupported version/);
    assert.throws(()=>activateEventAuthority(sqlite),/compatible writer|unsupported version/);
    assert.throws(()=>sqlite.transaction(()=>verifyEventProjections(sqlite))(),/compatible writer|unsupported version/);
    assert.deepEqual(sqlite.serialize(),before);
  }
  sqlite.prepare("UPDATE app_meta SET value=? WHERE key='event_authority'").run(JSON.stringify(original));
  sqlite.exec('UPDATE event_streams SET reducer_version=3; UPDATE projection_versions SET reducer_version=3;');
  const before=sqlite.serialize(),first=readDomainCommits(sqlite)[0];
  assert.throws(()=>activateEventAuthority(sqlite),/Unsupported reducer version/);
  assert.throws(()=>sqlite.transaction(()=>verifyEventProjections(sqlite))(),/Unsupported reducer version/);
  assert.throws(()=>sqlite.transaction(()=>appendDomainCommit(sqlite,{...first,events:first.events,expectedVersions:{}}))(),/Unsupported reducer version/);
  assert.deepEqual(sqlite.serialize(),before);
});

test('failed v1 installation rolls back rebuilt caches, query rows, cursor and authority marker',async t=>{
  const store=await fixture(t);await store.enableEventAuthority();const sqlite=store.sqlite;markV1(sqlite);
  const before=sqlite.serialize();
  sqlite.exec("CREATE TEMP TRIGGER reject_version_install BEFORE INSERT ON images BEGIN SELECT RAISE(ABORT,'synthetic version install failure'); END");
  assert.throws(()=>activateEventAuthority(sqlite),/synthetic version install failure/);
  assert.deepEqual(sqlite.serialize(),before);
  assert.equal(marker(sqlite).version,1);
  sqlite.exec('DROP TRIGGER reject_version_install');
  activateEventAuthority(sqlite);assert.equal(marker(sqlite).version,2);
});

test('v2 replay upgrades a v1 checkpoint and keeps retired unlinked artifacts absent',async t=>{
  const store=await fixture(t);await store.enableEventAuthority();await store.clearCachedImages({ids:['unlinked']});
  const target=new Database(':memory:');t.after(()=>target.close());migrateStorage(target);copyProtectedFacts(store.sqlite,target);
  const total=readDomainCommits(store.sqlite).length;
  replayDomainEvents(store.sqlite,target,{maxCommits:total-1,project:applyEventProjection});
  assert.ok(target.prepare("SELECT 1 FROM images WHERE id='unlinked'").get());markV1(target,{authority:false});
  const checkpoint=target.prepare("SELECT last_commit_seq FROM projection_versions WHERE name='core'").get().last_commit_seq;
  // 即使暂不追加批次，已有 checkpoint 也必须先由 v2 实际重建。
  assert.equal(replayDomainEvents(store.sqlite,target,{maxCommits:0,project:applyEventProjection}).commits,0);
  assert.equal(target.prepare("SELECT last_commit_seq FROM projection_versions WHERE name='core'").get().last_commit_seq,checkpoint);
  assert.equal(readStreamProjection(target,'artifact:unlinked').reducerVersion,2);
  replayDomainEvents(store.sqlite,target,{project:applyEventProjection});
  assert.equal(target.prepare("SELECT 1 FROM images WHERE id='unlinked'").get(),undefined);
  assert.deepEqual(readDomainCommits(target),readDomainCommits(store.sqlite));
  target.transaction(()=>verifyEventProjections(target))();
});
