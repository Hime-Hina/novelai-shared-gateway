import fs from 'node:fs';
import {promises as fsp} from 'node:fs';
import path from 'node:path';
import {createRequire} from 'node:module';
import {createHash} from 'node:crypto';
import {performance} from 'node:perf_hooks';

const source='/bench/final-3eae814-run01/annual/annual-backup.sqlite';
const directory='/bench/startup-3eae814-run01',target=path.join(directory,'library.sqlite'),output=path.join(directory,'report.json');
if(fs.existsSync(directory))throw new Error('Startup output must be a new directory');
fs.mkdirSync(directory);
const report={kind:'single-v2-store-startup',candidateCommit:'3eae814',source,target,
  limits:{network:'none',cpus:2,memory:'4g'},
  scope:'One JsonStore.init, one already-loaded ensureLoaded, then one enableEventAuthority on an independent consistent annual backup copy; no HTTP listener or upstream calls.',
  cachePolicy:'Fresh Node process and SQLite connections; OS cache is not dropped, and copying the backup may warm it. Full logical table hashes are calculated only after the timed calls.',timings:{}};
const require=createRequire('/app/package.json'),Database=require('better-sqlite3');
const digest=value=>createHash('sha256').update(value).digest('hex');
const quote=name=>'"'+name.replaceAll('"','""')+'"';
const openReadonly=file=>{const sqlite=new Database(file,{readonly:true,fileMustExist:true});sqlite.pragma('query_only = ON');return sqlite;};
function state(sqlite){
  const values=Object.fromEntries(sqlite.prepare('SELECT key,value FROM app_meta ORDER BY key').all().map(row=>[row.key,row.value]));
  return {cursor:sqlite.prepare('SELECT CAST(value AS TEXT) AS value FROM event_clock WHERE id=1').get()?.value,
    projectionCursor:sqlite.prepare("SELECT CAST(last_commit_seq AS TEXT) AS value FROM projection_versions WHERE name='core'").get()?.value,
    authority:values.event_authority?JSON.parse(values.event_authority):null,meta:values};
}
function tableHashes(sqlite){
  const result={};
  for(const table of sqlite.prepare("SELECT name,sql FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name!='app_meta' ORDER BY name").all()){
    const columns=sqlite.prepare(`PRAGMA table_info(${quote(table.name)})`).all();
    const order=/WITHOUT\s+ROWID/i.test(table.sql||'')?columns.filter(column=>column.pk).sort((a,b)=>a.pk-b.pk).map(column=>quote(column.name)).join(','):'rowid';
    const hash=createHash('sha256');hash.update(JSON.stringify(columns.map(column=>column.name))+'\n');let rows=0;
    for(const row of sqlite.prepare(`SELECT * FROM ${quote(table.name)} ORDER BY ${order}`).raw().safeIntegers().iterate()){
      hash.update(JSON.stringify(row,(_key,value)=>typeof value==='bigint'?{int64:String(value)}:value)+'\n');rows++;
    }
    result[table.name]={rows,sha256:hash.digest('hex')};
  }
  return result;
}
try{
  let start=performance.now();await fsp.copyFile(source,target);report.timings.copyMs=performance.now()-start;
  let original=openReadonly(source);const before=state(original);original.close();
  Object.assign(process.env,{DATA_DIR:directory,FAIR_SHARE_MODE:'true',PRIVATE_MODE:'true',PUBLIC_MODE:'true',NO_AUTO_DELETE:'true',
    RUNTIME_JOBS_CACHE_LIMIT:'500',RUNTIME_LEDGER_CACHE_LIMIT:'500',MOCK_WHEN_NO_ACCOUNT:'false'});
  globalThis.fetch=()=>{throw new Error('Network is disabled for the startup check');};
  start=performance.now();const {JsonStore}=await import('file:///app/server/store.ts');report.timings.moduleImportMs=performance.now()-start;
  const store=new JsonStore(directory);
  try{
    start=performance.now();await store.init();report.timings.initMs=performance.now()-start;
    start=performance.now();await store.ensureLoaded();report.timings.warmEnsureLoadedMs=performance.now()-start;
    start=performance.now();report.enableResult=await store.enableEventAuthority();report.timings.enableEventAuthorityMs=performance.now()-start;
    report.synchronous=store.sqlite.pragma('synchronous',{simple:true});
    report.runtimeCache=Object.fromEntries(['jobs','images','ledger','users','accounts'].map(name=>[name,store.db[name].length]));
  }finally{start=performance.now();await store.close();report.timings.closeMs=performance.now()-start;}
  original=openReadonly(source);const actual=openReadonly(target);
  try{
    const after=state(actual);report.cursors={before:before.cursor,after:after.cursor,projectionBefore:before.projectionCursor,projectionAfter:after.projectionCursor};
    report.authority={before:before.authority,after:after.authority};
    report.metadataChanges=[...new Set([...Object.keys(before.meta),...Object.keys(after.meta)])].sort().filter(key=>before.meta[key]!==after.meta[key])
      .map(key=>({key,beforeSha256:before.meta[key]===undefined?null:digest(before.meta[key]),afterSha256:after.meta[key]===undefined?null:digest(after.meta[key])}));
    start=performance.now();const sourceFacts=tableHashes(original);report.timings.sourceFactHashMs=performance.now()-start;
    start=performance.now();const targetFacts=tableHashes(actual);report.timings.targetFactHashMs=performance.now()-start;
    report.tableComparisons=Object.keys(sourceFacts).map(table=>({table,...sourceFacts[table],after:targetFacts[table],equal:JSON.stringify(sourceFacts[table])===JSON.stringify(targetFacts[table])}));
    report.sameTableSet=JSON.stringify(Object.keys(sourceFacts))===JSON.stringify(Object.keys(targetFacts));
    report.factsUnchanged=report.sameTableSet&&report.tableComparisons.every(table=>table.equal);
    start=performance.now();report.integrity=actual.pragma('integrity_check',{simple:true});report.timings.integrityMs=performance.now()-start;
    report.cursorUnchanged=before.cursor===after.cursor&&before.projectionCursor===after.projectionCursor;
    report.passed=report.factsUnchanged&&report.cursorUnchanged&&report.synchronous===2&&report.integrity==='ok';
    if(!report.passed)throw new Error('Startup changed stored facts/cursors or failed durability/integrity verification');
  }finally{original.close();actual.close();}
}catch(error){report.error={message:error.message,stack:error.stack};process.exitCode=1;}
fs.writeFileSync(output,JSON.stringify(report,null,2));
console.log(JSON.stringify({output,passed:report.passed,timings:report.timings,cursors:report.cursors,authority:report.authority,metadataChanges:report.metadataChanges,error:report.error}));
