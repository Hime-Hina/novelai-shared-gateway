import { parseArgs } from 'node:util';
import { spawn, execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { EventEmitter } from 'node:events';
import { Readable } from 'node:stream';
import { performance } from 'node:perf_hooks';
import { createHash, randomUUID } from 'node:crypto';
import { promises as fs, existsSync, createWriteStream } from 'node:fs';
import { fileURLToPath, pathToFileURL } from 'node:url';
import path from 'node:path';
import os from 'node:os';

const self=fileURLToPath(import.meta.url),root=path.resolve(path.dirname(self),'..');
const {values}=parseArgs({options:{baseline:{type:'string'},candidate:{type:'string',default:root},output:{type:'string'},
  history:{type:'string',default:'100000'},rounds:{type:'string',default:'3'},samples:{type:'string',default:'100'},active:{type:'string',default:'8'},
  observations:{type:'string',default:'525600'},'annual-batches':{type:'string',default:'36500'},'owner-days':{type:'string',default:'7'},
  'skip-annual':{type:'boolean',default:false},smoke:{type:'boolean',default:false},worker:{type:'string'},help:{type:'boolean',default:false}}});
async function main(){if(values.help){
  console.log('node --expose-gc scripts/benchmark-refactor.mjs --baseline <old-source> --candidate <new-source> [--output <new-directory>] [--smoke]\nDefaults: 100000 histories, 3 rounds, 100 samples, 8 active jobs; annual: 525600 observations + 36500 batches (sizes 1/4/1/2).\nEach source must use the same installed dependencies; a missing baseline node_modules is temporarily linked to candidate node_modules.\nSynthetic databases only. No HTTP listener, upstream network, production .env, or source edits. --smoke is runner validation, never performance acceptance.');
}else if(values.worker){
  const config=JSON.parse(await fs.readFile(values.worker,'utf8'));
  try{await worker(config);}catch(error){await fs.writeFile(config.result,JSON.stringify({stage:config.stage,error:{message:error.message,stack:error.stack}},null,2));throw error;}
}else await orchestrate();}

function integer(value,name,minimum=1){const number=Number(value);if(!Number.isSafeInteger(number)||number<minimum)throw new Error(`Invalid ${name}`);return number;}
function percentile(values,p){const ordered=[...values].sort((a,b)=>a-b);return ordered.length?ordered[Math.min(ordered.length-1,Math.ceil(ordered.length*p)-1)]:null;}
const distribution=values=>({count:values.length,p50Ms:percentile(values,.5),p95Ms:percentile(values,.95),p99Ms:percentile(values,.99),maxMs:values.length?Math.max(...values):null});
const elapsed=started=>performance.now()-started;
const iso=at=>new Date(at).toISOString();
const encode=value=>JSON.stringify(value,null,2);
const hash=value=>createHash('sha256').update(value).digest('hex');
async function bytes(file){try{return (await fs.stat(file)).size;}catch(error){if(error.code==='ENOENT')return 0;throw error;}}
async function disk(file){return {database:await bytes(file),wal:await bytes(file+'-wal'),shm:await bytes(file+'-shm')};}
function cleanEnvironment(){return Object.fromEntries(Object.entries(process.env).filter(([key])=>/^(PATH|PATHEXT|SYSTEMROOT|WINDIR|TEMP|TMP|TMPDIR|HOME|USERPROFILE|LANG|LC_ALL)$/i.test(key)));}
function revision(source){try{return execFileSync('git',['-C',source,'rev-parse','HEAD'],{encoding:'utf8',stdio:['ignore','pipe','ignore']}).trim();}catch{return null;}}
async function sourceEvidence(source){
  const files=['package.json','pnpm-lock.yaml'];
  for(const directory of ['server','public'])for(const name of await fs.readdir(path.join(source,directory),{recursive:true}))if(/\.(?:ts|js|mjs|sql|json)$/.test(name))files.push(`${directory}/${name.split(path.sep).join('/')}`);
  files.sort();const digests=Object.fromEntries(await Promise.all(files.map(async file=>[file,hash(await fs.readFile(path.join(source,file)))])));
  return {path:source,commit:revision(source),codeSha256:hash(JSON.stringify(digests)),files:digests};
}

async function orchestrate(){
  if(!values.baseline)throw new Error('--baseline is required');
  const candidate=path.resolve(values.candidate),baseline=path.resolve(values.baseline),output=path.resolve(values.output||path.join(os.tmpdir(),'nai-refactor-benchmark-'+randomUUID()));
  if(existsSync(output))throw new Error('Output must be a new directory so previous evidence is not overwritten');
  await fs.mkdir(output,{recursive:true});
  const options={history:integer(values.history,'history'),rounds:integer(values.rounds,'rounds'),samples:integer(values.samples,'samples'),active:integer(values.active,'active'),
    observations:integer(values.observations,'observations'),annualBatches:integer(values['annual-batches'],'annual-batches',0),ownerDays:integer(values['owner-days'],'owner-days'),now:Date.now(),smoke:values.smoke};
  if(values.smoke)Object.assign(options,{history:120,rounds:1,samples:3,active:2,observations:12,annualBatches:4});
  if(!values.smoke&&options.rounds<3)throw new Error('Performance acceptance requires at least three rounds');
  if(options.annualBatches>options.observations)throw new Error('annual-batches cannot exceed the number of observation minutes for this single-account workload');
  const link=path.join(baseline,'node_modules'),createdLink=!existsSync(link);
  if(createdLink)await fs.symlink(path.join(candidate,'node_modules'),link,process.platform==='win32'?'junction':'dir');
  try{
  const report={kind:options.smoke?'runner-smoke':'performance-evidence',options,sources:{baseline:await sourceEvidence(baseline),candidate:await sourceEvidence(candidate)},
    cachePolicy:'Fresh child process and SQLite connection each round; first queries recorded separately; OS page cache is not forcibly dropped.',
    transactionDefinition:'Admission is the real /api/jobs handler final JsonStore.update; envelope also includes readiness. Every sample has separate dispatch, artifact-prepared, and publish/settlement/cooldown commits using the real domain functions. Synthetic PNG file fsync is outside SQL timing. Logical generation takes 2s and existing account cooldown advances the clock without sleep or network. Fixed waiting jobs use a 24h deadline during timing rounds.',
    rowCounterPolicy:'Materialized SQL/DTO rows and matched source rows are reported separately. SQLite physical page reads are not exposed by better-sqlite3.',seeds:[],rounds:[]};
  const run=async(stage,label,source,extra={})=>{
    const name=`${label}-${stage}-${randomUUID()}`,config={...options,...extra,stage,source,label,result:path.join(output,name+'.json')};
    const configFile=path.join(output,name+'.config.json');await fs.writeFile(configFile,encode(config));
    const log=createWriteStream(path.join(output,name+'.log'));
    const child=spawn(process.execPath,['--expose-gc',self,'--worker',configFile],{env:cleanEnvironment(),stdio:['ignore','pipe','pipe'],windowsHide:true});
    child.stdout.pipe(log);child.stderr.pipe(log);
    const exitCode=await new Promise((resolve,reject)=>{child.once('error',reject);child.once('exit',resolve);});
    await new Promise(resolve=>log.end(resolve));
    const result=JSON.parse(await fs.readFile(config.result,'utf8'));
    if(exitCode!==0)throw new Error(`${label}/${stage} failed; inspect ${config.result}: ${result.error?.message||exitCode}`);
    console.log(`${label} ${stage}: ${config.result}`);return result;
  };
    for(const history of [...new Set([Math.min(options.history,options.smoke?20:1000),options.history])]){
      for(const [label,source] of [['baseline',baseline],['candidate',candidate]]){
        const template=path.join(output,`${label}-${history}-template`);
        report.seeds.push(await run('seed',label,source,{directory:template,history}));
        for(let round=1;round<=options.rounds;round++){
          const directory=path.join(output,`${label}-${history}-round-${round}`);await fs.mkdir(directory);
          await fs.copyFile(path.join(template,'library.sqlite'),path.join(directory,'library.sqlite'));
          report.rounds.push(await run('measure',label,source,{directory,history,round}));
        }
      }
    }
    if(!values['skip-annual']){
      const directory=path.join(output,'annual');report.annualSeed=await run('seed','candidate',candidate,{directory,history:0});
      report.annual=await run('annual','candidate',candidate,{directory});
    }
    report.comparison=[];
    for(const history of [...new Set(report.rounds.map(round=>round.history))]){
      const variants=Object.fromEntries(['baseline','candidate'].map(label=>[label,report.rounds.filter(round=>round.label===label&&round.history===history)]));
      const metrics=Object.keys(variants.baseline[0].latency);
      for(const metric of metrics){
        const before=percentile(variants.baseline.map(round=>round.latency[metric].p95Ms),.5),after=percentile(variants.candidate.map(round=>round.latency[metric].p95Ms),.5);
        if(before===null||after===null){report.comparison.push({history,metric,baselineMedianP95Ms:before,candidateMedianP95Ms:after,requiresInvestigation:null,reason:'No samples in this run'});continue;}
        report.comparison.push({history,metric,baselineMedianP95Ms:before,candidateMedianP95Ms:after,deltaMs:after-before,ratio:before?after/before:null,requiresInvestigation:after>before*1.2&&after-before>10});
      }
      const before=percentile(variants.baseline.map(round=>round.memory.hotPeakRss),.5),after=percentile(variants.candidate.map(round=>round.memory.hotPeakRss),.5);
      report.comparison.push({history,metric:'hotRss',baselineBytes:before,candidateBytes:after,ratio:after/before,requiresInvestigation:after>before*1.2});
    }
    report.sourceChangedDuringRun=(await sourceEvidence(baseline)).codeSha256!==report.sources.baseline.codeSha256||(await sourceEvidence(candidate)).codeSha256!==report.sources.candidate.codeSha256;
    await fs.writeFile(path.join(output,'report.json'),encode(report));
    if(report.sourceChangedDuringRun&&!options.smoke)throw new Error('Source changed during measurement; performance evidence is not a fixed-revision comparison');
    console.log(`Report: ${path.join(output,'report.json')}`);
  }finally{if(createdLink)await fs.unlink(link);}
}

async function worker(config){
  process.execArgv=process.execArgv.filter(value=>value!=='--expose-gc');
  process.env.DATA_DIR=config.directory;process.env.ADMIN_TOKEN='benchmark-synthetic-admin-token-0123456789';
  Object.assign(process.env,{FAIR_SHARE_MODE:'true',PRIVATE_MODE:'true',PUBLIC_MODE:'true',NO_AUTO_DELETE:'true',MOCK_WHEN_NO_ACCOUNT:'false',
    QUEUE_MAX_ACTIVE_GLOBAL:String(config.active+10),QUEUE_MAX_ACTIVE_PER_MEMBER:String(config.active+10),RUNTIME_JOBS_CACHE_LIMIT:'500',RUNTIME_LEDGER_CACHE_LIMIT:'500'});
  const NativeDate=Date;globalThis.Date=class extends NativeDate{constructor(...args){super(...(args.length?args:[config.now]));}static now(){return config.now;}};
  globalThis.fetch=()=>{throw new Error('External network is disabled in the benchmark');};
  const load=file=>import(pathToFileURL(path.join(config.source,'server',file+'.ts')).href);
  const storeModule=await load('store'),quota=await load('fair-quota');
  const require=createRequire(path.join(config.source,'package.json')),Database=require('better-sqlite3');
  const sqliteVersion=new Database(':memory:');const sqlite=sqliteVersion.prepare('SELECT sqlite_version() AS version').get().version;sqliteVersion.close();
  let filesystem;try{filesystem=await fs.statfs(path.dirname(config.directory));}catch(error){filesystem={unavailable:error.message};}
  const result={stage:config.stage,label:config.label,history:config.history,round:config.round,machine:{platform:process.platform,architecture:process.arch,node:process.version,sqlite,cpu:os.cpus()[0]?.model,logicalCpus:os.cpus().length,totalMemory:os.totalmem(),filesystem},
    fixedActive:config.active,source:config.source};
  const modules={load,storeModule,quota,Database};
  if(config.stage==='seed')await seed(config,result,modules);
  else if(config.stage==='measure')await measure(config,result,modules);
  else if(config.stage==='annual')await annual(config,result,modules);
  else throw new Error('Unknown benchmark stage');
  await fs.writeFile(config.result,encode(result));
}

const members=Array.from({length:20},(_,index)=>({id:`member-${String(index).padStart(2,'0')}`,token:`STA1N-benchmark-member-${index}`,enabled:true,acceptingJobs:true,jobAdmissionVersion:0,balance:1e9,memberAuth:{username:`Synthetic ${index}`,version:'benchmark'}}));
const request={model:'nai-diffusion-5-full',prompt:'synthetic benchmark',width:512,height:512,steps:29,seed:42,nocache:true,sampleCount:1,action:'generate'};
async function seed(config,result,{storeModule,quota}){
  const store=new storeModule.JsonStore(config.directory);await store.init();
  const db=await store.read();db.users=structuredClone(members);db.accounts=[{id:'opus',enabled:true,token:'synthetic-not-an-upstream-credential',quotaTier:3,subscriptionActive:true,
    subscriptionExpiresAt:config.now+2*365*86400000,quotaFixed:10000000,quotaPurchased:1000000,v5UsagePercent:100,v5UsageTimeUntilNextPercent:600,quotaCheckedAt:config.now}];
  quota.configureFairGroups(db,Array.from({length:4},(_,index)=>({id:`group-${index}`,share:.25,members:members.slice(index*5,index*5+5).map(user=>user.id)})),{now:config.now});quota.syncFairQuota(db,config.now);
  const started=performance.now();
  db.jobs=Array.from({length:config.history},(_,index)=>{
    const user=members[Math.floor(index/4)%members.length],createdAt=iso(config.now-Math.floor((index+.5)/Math.max(1,config.history)*365*86400000)),failed=index%10===0;
    return {id:`history-${index}`,memberId:user.id,userToken:user.token,accountId:'opus',source:'benchmark',batchId:`historical-batch-${Math.floor(index/4)}`,sampleIndex:index%4,
      status:failed?'failed':'done',createdAt,updatedAt:createdAt,startedAt:createdAt,completedAt:createdAt,upstreamStartedAt:createdAt,upstreamCompletedAt:createdAt,
      ...(index%20===0?{cancelledAt:createdAt,cancellationReason:'request_cancelled'}:{}),
      request:{operation:index%20===1?'upscale':index%20===2?'encode-vibe':'generate',model:index%3?'nai-diffusion-5-full':'nai-diffusion-4-5-full',width:512,height:512,steps:23},cost:0,accountCost:0,
      fairCharge:{resource:'anlas',amount:8,fixed:8,purchased:0,configVersion:2,userId:user.id,accountId:'opus',status:failed?'refunded':'settled',reservedAt:Date.parse(createdAt),settledAt:Date.parse(createdAt)}};
  });
  await store.write(db);result.seedMs=elapsed(started);
  if(typeof store.enableEventAuthority==='function'){const opening=performance.now();result.authority=await store.enableEventAuthority();result.openingMs=elapsed(opening);}
  result.rows=store.sqlite.prepare('SELECT count(*) AS count FROM jobs').get().count;
  result.synchronous=store.sqlite.pragma('synchronous',{simple:true});if(result.synchronous!==2)throw new Error('Financial fixture is not synchronous FULL');
  result.checkpoint=store.sqlite.pragma('wal_checkpoint(TRUNCATE)');result.disk=await disk(store.dbPath);await store.close();
}

async function memoryRequest(handler,body,key,{method='POST',url='/api/jobs',expected=202}={}){
  const data=Buffer.from(body?JSON.stringify(body):''),req=Readable.from(data.length?[data]:[]);
  Object.assign(req,{method,url,headers:{host:'localhost',origin:'http://localhost',authorization:`Bearer ${body?.token||members[0].token}`,'content-type':'application/json','content-length':String(data.length),'idempotency-key':key},
    rawHeaders:['Idempotency-Key',key],socket:{remoteAddress:'127.0.0.1'}});
  const res=new EventEmitter();Object.assign(res,{headers:{},statusCode:200,headersSent:false,writableEnded:false,destroyed:false,chunks:[],
    setHeader(key,value){this.headers[key.toLowerCase()]=value;},getHeader(key){return this.headers[key.toLowerCase()];},writeHead(status,headers){this.statusCode=status;this.headersSent=true;Object.assign(this.headers,headers||{});},
    write(chunk){this.chunks.push(Buffer.from(chunk));return true;},end(chunk){if(chunk)this.write(chunk);this.writableEnded=true;this.emit('finish');},destroy(){this.destroyed=true;}});
  await handler(req,res);const text=Buffer.concat(res.chunks).toString(),payload=text?JSON.parse(text):null;
  if(res.statusCode!==expected)throw new Error(`Synthetic ${url} returned ${res.statusCode}: ${text}`);
  return payload;
}

async function measure(config,result,{load,storeModule,quota}){
  process.env.QUEUE_WAIT_TIMEOUT_MS='86400000';
  let captured,phase=null,hotPeakRss=process.memoryUsage().rss,loadMs=0;
  const timings={admissionTransaction:[],admissionEnvelope:[],dispatchTransaction:[],artifactPreparedTransaction:[],settlementTransaction:[],quotaRefreshTransaction:[],memberSummary:[],firstHistory:[],annualHeatmap:[],ownerStatistics:[]},rows={};
  const originalUpdate=storeModule.JsonStore.prototype.update,originalLoad=storeModule.JsonStore.prototype.ensureLoaded;
  storeModule.JsonStore.prototype.ensureLoaded=async function(...args){const cold=!this.db,t=performance.now(),value=await originalLoad.apply(this,args);if(cold)loadMs+=elapsed(t);return value;};
  result.memory={beforeLoadRss:process.memoryUsage().rss};result.failures={busy:0,conflicts:0,retries:0};
  storeModule.JsonStore.prototype.update=async function(fn,options){captured=this;const started=performance.now();try{const value=await originalUpdate.call(this,fn,options);
    if(phase==='admission'&&options?.generationRequest)timings.admissionTransaction.push(elapsed(started));return value;
  }catch(error){if(/SQLITE_BUSY/.test(error.code||error.message))result.failures.busy++;if(/conflict/i.test(error.message))result.failures.conflicts++;throw error;}};
  const {handleGatewayRequest}=await load('gateway-runtime'),heatmap=await load('activity-heatmap'),owner=await load('owner-operations');
  const execution=existsSync(path.join(config.source,'server/generation-execution.ts'))?await load('generation-execution'):null;
  const resultFiles=await load('result-files'),capacity=await load('shared-capacity'),batch=await load('generation-request');
  const domain=execution?await load('domain-events'):null,baseTime=config.now;
  await memoryRequest(handleGatewayRequest,null,'bootstrap',{method:'GET',url:'/api/me/quota',expected:200});
  for(let index=0;index<config.active;index++)await memoryRequest(handleGatewayRequest,{...request,token:members[index%20].token},`active-${index}`);
  const store=captured;result.loadMs=loadMs;global.gc?.();result.memory.afterLoadRss=process.memoryUsage().rss;hotPeakRss=result.memory.afterLoadRss;
  result.synchronous=store.sqlite.pragma('synchronous',{simple:true});if(result.synchronous!==2)throw new Error('Measured store is not synchronous FULL');
  const query=async(name,fn,materialized)=>{const started=performance.now(),value=await fn();timings[name].push(elapsed(started));rows[name]=materialized(value);hotPeakRss=Math.max(hotPeakRss,process.memoryUsage().rss);return value;};
  const queries=async()=>{
    await query('memberSummary',async()=>{const db=await store.readCollections(['settings','users','accounts','jobs']);const view=quota.estimateFairQuota?quota.estimateFairQuota(db,config.now):{...db,settings:structuredClone(db.settings),ledger:[]};return quota.fairQuotaSummary(view,members[0],config.now);},()=>1);
    await query('firstHistory',()=>store.readMemberJobPage(members[0],{page:1,pageSize:30,q:'',status:'',sort:'createdAt',order:'desc',until:iso(config.now)}),value=>value.jobs.length);
    await query('annualHeatmap',()=>heatmap.readActivityHeatmap(store,new URLSearchParams(),{memberId:members[0].id},config.now),value=>value.days.length);
    await query('ownerStatistics',()=>owner.ownerOperation(new URL(`http://localhost/api/member/owner/statistics?days=${config.ownerDays}`),'GET',undefined,{store}),value=>value.recent.length);
  };
  await queries();result.coldQueries=Object.fromEntries(Object.entries(timings).filter(([,values])=>values.length).map(([key,values])=>[key,values.splice(0)]));
  const png=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=','base64');
  await fs.mkdir(path.join(config.directory,'images'),{recursive:true});
  let spentFixed=0,spentPurchased=0,lastObservedAt=baseTime;
  for(let index=0;index<config.samples;index++){
    config.now=baseTime+index*12000;
    if(config.now-lastObservedAt>=60000){
      const refresh=performance.now();await store.update(db=>{
        const account=db.accounts.find(account=>account.id==='opus'),observation={quotaFixed:10000000-spentFixed,quotaPurchased:1000000-spentPurchased,quotaCheckedAt:config.now,v5UsagePercent:100,v5UsageTimeUntilNextPercent:600};
        Object.assign(account,observation);
        domain?.recordDomainEvent(db,{eventType:'QuotaObserved',streamId:'account:opus',accountId:'opus',occurredAt:config.now,payload:{kind:'official',accountId:'opus',observation,at:config.now}});
        quota.syncFairQuota(db,config.now);
      },{immediate:true,collections:['accounts','settings','ledger']});timings.quotaRefreshTransaction.push(elapsed(refresh));lastObservedAt=config.now;
    }
    const member=members[index%20];phase='admission';const admission=performance.now();
    const accepted=await memoryRequest(handleGatewayRequest,{...request,token:member.token,seed:index+100},`sample-${config.round}-${index}`);
    timings.admissionEnvelope.push(elapsed(admission));phase=null;
    const id=accepted.id;
    const dispatch=performance.now();await store.update(db=>{
      const job=db.jobs.find(job=>job.id===id),account=db.accounts.find(account=>account.id==='opus');if(!job)throw new Error('Accepted job is missing');
      if(capacity.sharedAccountReadyDelay(account,config.now)>0)throw new Error('Measured dispatch violates the account cooldown');
      job.status='running';job.startedAt=job.updatedAt=iso(config.now);job.accountId=account.id;
      quota.reserveFairQuota(db,db.users.find(user=>user.id===member.id),job,account,config.now,{...request,seed:index+100},store.readFairQuotaSettlement?.(id));
    },{immediate:true,includeSettings:true,dirtyRows:{jobs:[id]}});timings.dispatchTransaction.push(elapsed(dispatch));
    config.now+=2000;
    const imageId=`image-${id}`,descriptor=resultFiles.describeStoredResult(imageId,'generate',{mimeType:'image/png',buffer:png});
    const prepared=performance.now();await store.update(db=>{
      const job=db.jobs.find(job=>job.id===id);job.resultPending={...descriptor,attemptId:job.generationAttempt.id};
      execution?.recordPreparedArtifact(db,job);
    },{immediate:true,dirtyRows:{jobs:[id]}});timings.artifactPreparedTransaction.push(elapsed(prepared));
    const image={id:imageId,jobId:id,token:member.token,accountId:'opus',file:descriptor.file,mimeType:descriptor.mimeType,byteLength:descriptor.byteLength,sha256:descriptor.sha256,createdAt:iso(config.now),model:request.model,width:1,height:1,operation:'generate',mock:false};
    const file=await fs.open(path.join(config.directory,image.file),'w');try{await file.writeFile(png);await file.sync();}finally{await file.close();}
    const final=performance.now();await store.update(db=>{
      const job=db.jobs.find(job=>job.id===id),account=db.accounts.find(account=>account.id==='opus');
      if(execution)execution.completeSharedAccount(db,account,config.now,{job,kind:'success'});
      else capacity.recordSharedCompletion(account,config.now,{job,kind:'success'});
      quota.settleFairQuota(db,job,true,config.now,{},store.readFairQuotaSettlement?.(id));
      Object.assign(job,{status:'done',imageId:image.id,completedAt:iso(config.now),updatedAt:iso(config.now)});db.images.unshift(image);
      delete job.resultPending;spentFixed+=Number(job.fairCharge.fixed);spentPurchased+=Number(job.fairCharge.purchased);
      execution?.recordCompletedArtifact(db,job,image,config.now);
      batch.activateNextBatchJob(db,job,store.nextQueueSequence(),config.now);
    },{immediate:true,includeSettings:true,dirtyRows:{jobs:[id],images:[image.id],accounts:['opus']}});timings.settlementTransaction.push(elapsed(final));
    await queries();hotPeakRss=Math.max(hotPeakRss,process.memoryUsage().rss);
  }
  result.latency=Object.fromEntries(Object.entries(timings).map(([key,values])=>[key,distribution(values)]));result.samples=timings;result.materializedRows=rows;
  result.sourceRows={historyMember:store.sqlite.prepare('SELECT count(*) AS count FROM jobs WHERE user_token=?').get(members[0].token).count,historyTotal:store.sqlite.prepare('SELECT count(*) AS count FROM jobs').get().count};
  result.memory.hotPeakRss=hotPeakRss;global.gc?.();result.memory.afterGcRss=process.memoryUsage().rss;
  result.hotCollections=Object.fromEntries(['jobs','images','ledger'].map(name=>[name,store.db[name].length]));
  result.fixedActiveObserved=store.sqlite.prepare("SELECT count(*) AS count FROM jobs WHERE status IN ('queued','pending','running')").get().count;
  if(result.fixedActiveObserved!==config.active)throw new Error('The fixed active set changed during measurement');
  result.diskBeforeCheckpoint=await disk(store.dbPath);const checkpoint=performance.now();result.checkpoint=store.sqlite.pragma('wal_checkpoint(TRUNCATE)');result.checkpointMs=elapsed(checkpoint);result.disk=await disk(store.dbPath);
  const integrity=performance.now();result.integrity=store.sqlite.pragma('integrity_check');result.integrityMs=elapsed(integrity);
  await store.close();
}

async function annual(config,result,{load,storeModule,quota,Database}){
  const eventStore=await load('event-store'),domain=await load('domain-events'),migration=await load('storage-migrations'),execution=await load('generation-execution');
  const batch=await load('generation-request'),resultFiles=await load('result-files'),capacity=await load('shared-capacity'),baseTime=config.now;
  const file=path.join(config.directory,'library.sqlite'),sqlite=new Database(file);sqlite.pragma('journal_mode = WAL');sqlite.pragma('synchronous = FULL');
  const state=eventStore.readProjectedFairQuota(sqlite);
  if(!state?.balances)throw new Error('Annual fixture requires a projected opening quota');
  const db={settings:{fairQuota:state},users:structuredClone(members),jobs:[],ledger:[],accounts:[{id:'opus',enabled:true,quotaTier:3,subscriptionActive:true,subscriptionExpiresAt:config.now+2*365*86400000,quotaFixed:10000000,quotaPurchased:1000000,v5UsagePercent:100,v5UsageTimeUntilNextPercent:600,quotaCheckedAt:config.now}]};
  const input=sqlite.prepare('INSERT INTO generation_requests(id,member_id,source,operation,created_at,payload_bytes,payload) VALUES(?,?,?,?,?,?,?)');
  const commitsByPhase={},eventsByType={};
  const append=(phase,action,metadata={})=>{
    config.now=metadata.at??config.now;
    domain.beginDomainCommand(db,{recordedAt:iso(config.now)});action();const command=domain.finishDomainCommand(db);
    const committed=sqlite.transaction(()=>{if(metadata.input){const record=metadata.input;input.run(record.id,record.memberId,'benchmark','generate',record.at,Buffer.byteLength(record.payload),record.payload);}
      return eventStore.appendDomainCommit(sqlite,command,{project:storeModule.applyEventProjection});})();
    commitsByPhase[phase]=(commitsByPhase[phase]||0)+1;
    for(const event of command.events)eventsByType[event.eventType]=(eventsByType[event.eventType]||0)+1;
    return committed;
  };
  let batches=0,images=0,spent=0,queueSequence=0;const started=performance.now(),initialDisk=await disk(file),initialEvents=sqlite.prepare('SELECT count(*) AS count FROM domain_events').get().count;
  const png=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=','base64');
  for(let index=0;index<config.observations;index++){
    const at=baseTime+(index+1)*60000,account=db.accounts[0];account.quotaCheckedAt=at;account.quotaFixed=10000000-spent;
    append('quota-observation',()=>{
      domain.recordDomainEvent(db,{eventType:'QuotaObserved',streamId:'account:opus',accountId:'opus',occurredAt:at,payload:{kind:'official',accountId:'opus',observation:{quotaFixed:account.quotaFixed,quotaPurchased:1000000,v5UsagePercent:100,quotaCheckedAt:at,v5UsageTimeUntilNextPercent:600},at}});
      quota.syncFairQuota(db,at);
    },{at});
    const due=Math.floor((index+1)*config.annualBatches/config.observations);
    while(batches<due){
      const member=members[batches%20],count=[1,4,1,2][batches%4],requestId=`annual-request-${batches}`,payload=JSON.stringify({...request,sampleCount:count});
      const jobs=Array.from({length:count},(_,sampleIndex)=>({id:`annual-job-${batches}-${sampleIndex}`,requestId,memberId:member.id,userToken:member.token,
        batchId:requestId,sampleIndex,status:sampleIndex===0?'queued':'pending',...(sampleIndex===0?{queueSeq:++queueSequence}:{}),queueExpiresAt:iso(at+600000),
        source:'benchmark',request:{...request},createdAt:iso(at),updatedAt:iso(at)}));db.jobs=jobs;
      append('request-accepted',()=>{
        domain.recordDomainEvent(db,{eventType:'RequestAccepted',streamId:`request:${requestId}`,memberId:member.id,occurredAt:at,payload:{requestId,memberId:member.id,source:'benchmark',operation:'generate',createdAt:iso(at),inputRef:requestId,inputSha256:hash(payload),payloadBytes:Buffer.byteLength(payload),jobs:jobs.map(job=>({id:job.id,batchId:requestId,index:job.sampleIndex,status:job.status,queueSeq:job.queueSeq,queueExpiresAt:job.queueExpiresAt,summary:{operation:'generate',model:request.model,width:512,height:512,steps:request.steps}}))}});
      },{at,input:{id:requestId,memberId:member.id,at:iso(at),payload}});
      for(const job of jobs){
        const dispatchAt=Math.max(at,Date.parse(account.cooldownUntil||'')||at);
        append('child-dispatch',()=>{
          if(job.status!=='queued'||capacity.sharedAccountReadyDelay(account,dispatchAt)>0)throw new Error('Annual child is not ready for serial dispatch');
          Object.assign(job,{status:'running',accountId:account.id,startedAt:iso(dispatchAt),updatedAt:iso(dispatchAt)});
          quota.reserveFairQuota(db,member,job,account,dispatchAt,request);
        },{at:dispatchAt});
        const completedAt=dispatchAt+2000,imageId=`annual-image-${images++}`,descriptor=resultFiles.describeStoredResult(imageId,'generate',{mimeType:'image/png',buffer:png});
        append('artifact-prepared',()=>{
          job.resultPending={...descriptor,attemptId:job.generationAttempt.id};execution.recordPreparedArtifact(db,job);
        },{at:completedAt});
        const image={id:imageId,file:descriptor.file,mimeType:descriptor.mimeType,byteLength:descriptor.byteLength,sha256:descriptor.sha256,
          createdAt:iso(completedAt),model:request.model,width:1,height:1,operation:'generate',mock:false};
        append('child-completed',()=>{
          execution.completeSharedAccount(db,account,completedAt,{job,kind:'success'});
          quota.settleFairQuota(db,job,true,completedAt);spent+=Number(job.fairCharge.fixed);
          Object.assign(job,{status:'done',completedAt:iso(completedAt),updatedAt:iso(completedAt),imageId:image.id});delete job.resultPending;
          execution.recordCompletedArtifact(db,job,image,completedAt);batch.activateNextBatchJob(db,job,++queueSequence,completedAt);
        },{at:completedAt});
        delete db.settings.fairQuota.reservations[job.id];db.ledger=[];
      }
      batches++;
    }
    if((index+1)%10000===0)console.log(`annual ${index+1}/${config.observations}, batches ${batches}`);
  }
  result.appendMs=elapsed(started);result.observations=config.observations;result.generationBatches=batches;result.generatedImages=images;
  for(const [type,expected] of Object.entries({RequestAccepted:batches,QuotaReserved:images,DispatchAuthorized:images,ArtifactPrepared:images,ArtifactPublished:images,GenerationSettled:images,JobCompleted:images,CooldownChanged:images,JobQueued:images-batches})){
    if((eventsByType[type]||0)!==expected)throw new Error(`Annual success path emitted ${eventsByType[type]||0} ${type}, expected ${expected}`);
  }
  result.commitsByPhase=commitsByPhase;result.eventsByType=eventsByType;
  result.generationProfile={batchSizes:[1,4,1,2],model:request.model,resources:'actual quota decision',logicalGenerationMs:2000,cooldown:'existing shared-account success policy; logical clock only',
    commitBoundaries:['request accepted','each child dispatch/reservation','each artifact prepared','each publish/settlement/cooldown/next-child queue'],
    artifactBytes:'Validated synthetic PNG descriptors and publication metadata only; image byte storage, upstream latency and filesystem publication are excluded from this event-capacity fixture.'};
  const events=sqlite.prepare('SELECT count(*) AS count,sum(length(CAST(payload AS BLOB))) AS payloadBytes FROM domain_events').get();result.events={...events,added:events.count-initialEvents};
  result.initialDisk=initialDisk;result.diskBeforeCheckpoint=await disk(file);const checkpoint=performance.now();result.checkpoint=sqlite.pragma('wal_checkpoint(TRUNCATE)');result.checkpointMs=elapsed(checkpoint);result.disk=await disk(file);result.bytesPerAddedEvent=(result.disk.database-initialDisk.database)/Math.max(1,result.events.added);
  try{result.tableAndIndexBytes=sqlite.prepare('SELECT name,sum(pgsize) AS bytes FROM dbstat GROUP BY name ORDER BY bytes DESC').all();}catch(error){result.tableAndIndexBytes={unavailable:error.message};}
  const backup=path.join(config.directory,'annual-backup.sqlite'),backupStart=performance.now();await sqlite.backup(backup);result.backup={ms:elapsed(backupStart),bytes:await bytes(backup)};
  const replay=async(name,firstLimit)=>{
    const targetFile=path.join(config.directory,name+'.sqlite'),target=new Database(targetFile);target.pragma('journal_mode = WAL');target.pragma('synchronous = FULL');migration.migrateStorage(target);eventStore.copyProtectedFacts(sqlite,target);
    let checkpoint;
    if(firstLimit){const start=performance.now(),progress=eventStore.replayDomainEvents(sqlite,target,{project:storeModule.applyEventProjection,maxCommits:firstLimit});checkpoint={ms:elapsed(start),...progress};target.pragma('wal_checkpoint(TRUNCATE)');}
    const start=performance.now(),progress=eventStore.replayDomainEvents(sqlite,target,{project:storeModule.applyEventProjection});
    const digest=db=>hash(JSON.stringify(db.prepare('SELECT stream_id,projection_sha256 FROM event_streams ORDER BY stream_id').all()));
    const output={ms:elapsed(start),...progress,...(checkpoint?{checkpoint}:{}),synchronous:target.pragma('synchronous',{simple:true}),sourceProjectionSha256:digest(sqlite),replayedProjectionSha256:digest(target)};
    if(output.sourceProjectionSha256!==output.replayedProjectionSha256)throw new Error(`Replayed stream projections differ in ${name}`);
    target.pragma('wal_checkpoint(TRUNCATE)');output.disk=await disk(targetFile);target.close();return output;
  };
  result.fullReplay=await replay('full-replay');const commits=sqlite.prepare('SELECT count(*) AS count FROM event_commits').get().count;
  result.checkpointResume=await replay('checkpoint-replay',Math.max(1,Math.floor(commits/2)));
  const integrity=performance.now();result.integrity=sqlite.pragma('integrity_check');result.integrityMs=elapsed(integrity);result.synchronous=sqlite.pragma('synchronous',{simple:true});sqlite.close();
}

await main();
