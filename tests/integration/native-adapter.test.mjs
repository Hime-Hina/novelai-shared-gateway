import { quoteFairRequest } from '../../server/fair-quota.js';
import { parseNativeImageRequest as parseValidatedNativeImageRequest, parseNativeAuxiliaryRequest } from '../../server/native-image.js';
import { readNativeBody } from '../../server/native-body.js';
import { singleGenerationRequest, webJobRequestSummary, activateNextBatchJob, cancelWaitingBatchJobs } from '../../server/generation-request.js';
import { assertQueueAdmission, queueDeadline } from '../../server/queue-policy.ts';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { Readable } from 'node:stream';
import Negotiator from 'negotiator';
const source = readFileSync(new URL('../../server/gateway-runtime.js', import.meta.url), 'utf8');
const section = source.slice(source.indexOf('function parseNativeImageRequest('), source.indexOf('async function handleOpenAiChatCompletion('));
const context = vm.createContext({ parseValidatedNativeImageRequest, parseNativeAuxiliaryRequest, quoteFairRequest, singleGenerationRequest, webJobRequestSummary, assertQueueAdmission,
  queueDeadline: (now, deadline) => queueDeadline(now, deadline, {}), process: {env:{}},
  publicErrorMessage: String, Buffer, structuredClone, randomBytes, readNativeBody, Negotiator, httpError: (statusCode, message) => Object.assign(new Error(message), {statusCode}) });
vm.runInContext(section, context);
for (const name of ['generationIntent', 'resolveGenerationIdempotency', 'validateGenerationReplay']) {
  const start=source.indexOf(`function ${name}(`),end=source.indexOf('\n}',start)+2;
  vm.runInContext((source.slice(start-6,start)==='async '?'async ':'')+source.slice(start,end),context);
}
vm.runInContext(source.slice(source.indexOf('function generationRequestRecord('), source.indexOf('async function createJob(')), context);
const input = { action: 'generate', input: 'a cat', model: 'nai-diffusion-4-5-full', parameters: {width: 1024,height: 768,steps: 35, n_samples:1, seed: 4, stream: 'msgpack', v4_prompt:{caption:{base_caption:'a cat',char_captions:[{char_caption:'blue hair',centers:[{x:0.2,y:0.5}]}]}}, reference_image_multiple:[] } };
const parsed = context.parseNativeImageRequest(input);
assert.equal(parsed.steps,35); assert.equal(parsed.width,1024); assert.equal(parsed.height,768); assert.equal(parsed.prompt,'a cat');
assert.equal(parsed.nocache,true); assert.equal(parsed.nativeParameters.stream,undefined); assert.equal(input.parameters.stream,'msgpack');
assert.deepEqual(parsed.nativeParameters.v4_prompt,input.parameters.v4_prompt);
for (const patch of [{action:'img2img'}, {model:'unknown'}, {parameters:{...input.parameters,n_samples:5}}, {parameters:{...input.parameters,width:512.5}}, {parameters:{...input.parameters,reference_image_multiple:['image']}}]) {
  assert.throws(() => context.parseNativeImageRequest({...input,...patch}), error => error.statusCode===422);
}
assert.throws(() => context.parseNativeImageRequest({...input,input:''}), error => error.statusCode===400);
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=', 'base64');
const zip = context.nativeImageZip(png);
assert.equal(zip.readUInt32LE(0),0x04034b50); assert.equal(zip.readUInt16LE(8),0);
const filenameLength = zip.readUInt16LE(26);
assert.equal(zip.subarray(30,30+filenameLength).toString(),'image_0.png');
assert.deepEqual(zip.subarray(30+filenameLength,30+filenameLength+png.length),png);
assert.equal(zip.readUInt32LE(zip.length-22),0x06054b50);
// 独立按多项式逐位计算校验码，确认 ZIP 写入有效 CRC。
let crc=0xffffffff;
for(const b of png){crc^=b;for(let bit=0;bit<8;bit++){const odd=crc%2;crc=Math.floor((crc>>>0)/2);if(odd)crc^=0xedb88320;}}
assert.equal(zip.readUInt32LE(14),(crc^0xffffffff)>>>0);
let submitted, nextId=0, queueSequence=0;
const user={id:'member-test',token:'STA1N-test',enabled:true,jobAdmissionVersion:7}, db={users:[user],jobs:[]};
const claim={resize(bytes){assert.ok(Number.isSafeInteger(bytes)&&bytes>0);}};
// 隔离存储与 worker 边界；批次受理、父参数序列化和 HTTP 等待仍执行真实函数。
const store={
  readUserByToken:async token=>token===user.token?user:null,
  nextQueueSequence:()=>++queueSequence,
  update:async(fn,options)=>{
    const batch=fn(db),parent=options.generationRequest();
    assert.equal(options.immediate,true);assert.equal(options.generationPayloadClaim,claim);
    assert.deepEqual(options.dirtyRows(batch).jobs,batch.jobIds);
    submitted={request:JSON.parse(parent.payload),parent,jobs:batch.jobIds.map(id=>db.jobs.find(job=>job.id===id))};
    assert.equal(parent.payload_bytes,Buffer.byteLength(parent.payload));
    return batch;
  }
};
function completeJob(jobId,result){
  const job=db.jobs.find(item=>item.id===jobId);
  assert.ok(job);assert.equal(job.status,'queued');
  job.status=result.error?'failed':'done';
  if(result.error)cancelWaitingBatchJobs(db,job,result.error,result.statusCode||500);
  else activateNextBatchJob(db,job,store.nextQueueSequence());
  return result;
}
Object.assign(context, { bearerToken:req=>req.token||'', store, readJson:async req=>req.body,
  assertFairMemberReady:async token=>assert.equal(token,user.token),cleanupStaleActiveJobs:async()=>{},
  getUserOrThrow:(_db,token)=>{assert.equal(token,user.token);return user;},
  createId:prefix=>`${prefix}-${++nextId}`,activeJobCount:()=>0,
  openAiChatTimeoutMs:5000,waitForJobResult:async id=>completeJob(id,{saved:{mimeType:'image/png'}}),
  scheduleQueueDrain:()=>{},readStoredImage:async()=>png,imageExtension:()=> 'png',
  sendImage:(res,status,mime,buffer,headers)=>Object.assign(res,{status,mime,buffer,headers}) });
await assert.rejects(context.handleNativeImageGeneration({token:'pst-invalid',headers:{},body:input},{setHeader(){}}),e=>e.statusCode===401);
const jsonHeaders={'content-type':'application/json'};
await assert.rejects(context.handleNativeImageGeneration({token:'STA1N-test',headers:jsonHeaders,body:null,generationPayloadClaim:claim},{setHeader(){}}),error=>error.statusCode===400);
const response={setHeader:()=>{}}; await context.handleNativeImageGeneration({token:'STA1N-test',headers:jsonHeaders,body:input,generationPayloadClaim:claim},response);
assert.equal(submitted.parent.source,'novelai-native');assert.equal(submitted.parent.member_id,user.id);
assert.equal(submitted.jobs[0].requestId,submitted.parent.id);assert.equal(submitted.jobs[0].jobAdmissionVersion,7);
assert.equal(submitted.request.steps,35); assert.equal(response.status,200); assert.equal(response.mime,'application/zip'); assert.equal(response.headers['cache-control'],'no-store');
// 使用启动器实际的 blob JSON 部件，核验从解析到队列和 ZIP 响应的完整路径。
const form=new FormData();
form.append('request',new Blob([JSON.stringify(input)],{type:'application/json'}),'blob');
const encoded=new Response(form);
const multipart=Readable.from([Buffer.from(await encoded.arrayBuffer())]);
multipart.headers={'content-type':encoded.headers.get('content-type')}; multipart.token='STA1N-test';multipart.generationPayloadClaim=claim;
const multipartResponse={setHeader:()=>{}};
await context.handleNativeImageGeneration(multipart,multipartResponse);
assert.equal(submitted.request.steps,35); assert.equal(submitted.request.prompt,'a cat');
assert.deepEqual(submitted.request.nativeParameters.v4_prompt,input.parameters.v4_prompt);
assert.equal(multipartResponse.status,200); assert.deepEqual(multipartResponse.buffer,response.buffer);
context.waitForJobResult=async id=>completeJob(id,{error:'mock provider failure'});
context.isTimeoutResultMessage=()=>false;
await assert.rejects(context.handleNativeImageGeneration({token:'STA1N-test',headers:jsonHeaders,body:input,generationPayloadClaim:claim},{setHeader:()=>{}}),e=>e.statusCode===500);
assert.match(source,/url\.pathname\.startsWith\('\/ai\/'\)/);
assert.match(source,/\['\/ai\/generate-image', '\/v1\/ai\/generate-image'\]/);
assert.match(source,/if \(options\.native\) return structuredClone\(body\)/);
console.log('Native adapter offline checks passed: native payload, validation, ZIP, auth, queue and failures. No upstream calls.');

for (const seed of [4294967295,4294967296,9999999999]) {
 const value = context.parseNativeImageRequest({...input,parameters:{...input.parameters,seed}});
 assert.equal(value.seed,seed%4294967296);
}
assert.throws(()=>context.parseNativeImageRequest({...input,parameters:{...input.parameters,seed:Number.MAX_SAFE_INTEGER+1}}));
const randomRequest=context.parseNativeImageRequest({...input,parameters:{...input.parameters,seed:-1}});
assert.ok(randomRequest.seed>=0 && randomRequest.seed<=4294967295);
const diagnostic=context.nativeRequestDiagnostic({...input,input:'DO_NOT_LOG_PROMPT',token:'DO_NOT_LOG_KEY',parameters:{...input.parameters,image:'DO_NOT_LOG_IMAGE',negative_prompt:'DO_NOT_LOG_NEGATIVE'}});
const serialized=JSON.stringify(diagnostic);
for(const secret of ['DO_NOT_LOG_PROMPT','DO_NOT_LOG_KEY','DO_NOT_LOG_IMAGE','DO_NOT_LOG_NEGATIVE']) assert.ok(!serialized.includes(secret));
assert.equal(diagnostic.numeric.width.value,1024);
console.log('Diagnostic redaction and seed boundaries passed.');

// 一次受理全部 child，后续图片等待前序完成；部分失败只返回已成功的图片和清单。
function zipFiles(bytes) {
  const files = new Map(); let offset = 0;
  while (bytes.readUInt32LE(offset) === 0x04034b50) {
    const size = bytes.readUInt32LE(offset + 18), nameLength = bytes.readUInt16LE(offset + 26);
    const extraLength = bytes.readUInt16LE(offset + 28), start = offset + 30 + nameLength + extraLength;
    files.set(bytes.toString('utf8', offset + 30, offset + 30 + nameLength), bytes.subarray(start, start + size));
    offset = start + size;
  }
  return files;
}
for (const failAt of [0, 2]) {
  let completedWaits = 0;
  context.waitForJobResult = async jobId => {
    assert.equal(submitted.jobs.length,3);
    assert.equal(jobId,submitted.jobs[completedWaits].id);
    if(completedWaits===0)assert.deepEqual(Array.from(submitted.jobs,job=>job.status),['queued','pending','pending']);
    completedWaits++;
    return completeJob(jobId,completedWaits === failAt ? {error:'synthetic batch failure', statusCode:502} : {saved:{mimeType:'image/png'}});
  };
  const res = {setHeader() {}};
  await context.handleNativeImageGeneration({token:'STA1N-test',headers:jsonHeaders,body:{...input,parameters:{...input.parameters,n_samples:3}},generationPayloadClaim:claim},res);
  assert.equal(res.status,200);
  assert.equal(completedWaits,failAt || 3);
  assert.equal(submitted.request.sampleCount,3);assert.equal(submitted.request.nativeParameters.n_samples,3);
  assert.deepEqual(Array.from(submitted.jobs,job=>job.request.seed),[4,5,6]);
  assert.ok(submitted.jobs.every(job=>job.requestId===submitted.parent.id&&job.request.nativeParameters===undefined));
  assert.ok(submitted.jobs.every(job=>singleGenerationRequest(submitted.request,job.sampleIndex).nativeParameters.n_samples===1));
  assert.deepEqual(Array.from(submitted.jobs,job=>job.status),failAt?['done','failed','failed']:['done','done','done']);
  const files = zipFiles(res.buffer), summary = JSON.parse(files.get('results.json'));
  assert.equal(summary.requested,3);
  assert.equal(summary.completed,failAt ? 1 : 3);
  assert.equal(summary.remaining,failAt ? 2 : 0);
  assert.equal(files.size,summary.completed + 1);
  assert.equal(res.headers['X-Nai-Image-Count'],String(summary.completed));
  assert.equal(res.headers['X-Nai-Partial'],String(Boolean(failAt)));
}
console.log('Sequential batch generation and partial ZIP results passed.');
