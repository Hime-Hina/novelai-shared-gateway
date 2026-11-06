import test from 'node:test';
import assert from 'node:assert/strict';
import { beginDomainCommand,finishDomainCommand } from '../../server/events/domain-events.ts';
import { reduceDomainEvent } from '../../server/events/event-store.ts';
import { recordRecoveredGeneration } from '../../server/generation/accounting.ts';
import { recordGenerationFailure } from '../../server/generation/execution.ts';
import { setMemberAdmission } from '../../server/generation/queue-policy.ts';
import { cancelWaitingBatchJobs } from '../../server/generation/batch-requests.ts';
const now=1793491200000;
function replayRequest(before,command,streamId){
  let projection={jobs:Object.fromEntries(before.map(job=>[job.id,structuredClone(job)])),artifacts:{},freeUsage:{}};
  for(const [index,input] of command.events.filter(event=>event.streamId===streamId).entries())projection=reduceDomainEvent(projection,{...input,eventId:input.eventId,commitSeq:'1',eventIndex:index,commandId:command.commandId,
    streamKind:'request',streamVersion:String(index+1),schemaVersion:1,recordedAt:command.recordedAt,occurredAt:new Date(input.occurredAt).toISOString(),amountVersion:'decimal-12-v1'});
  return projection.jobs;
}
test('real recovered-generation producer replays the final review status and result reference',()=>{
  const job={id:'j',status:'failed',fairCharge:{status:'refunded'},generationAttempt:{id:'attempt:j:1',jobId:'j',memberId:'a',accountId:'opus',upstreamCandidates:[]},
    generationRefund:{eventId:'refund',lossEventId:'loss'},generationReview:{status:'pending',firstUnknownAt:now-5000,deadlineAt:now+5000,lossEventId:'loss',closedAt:null,closureEventId:null}};
  const before=structuredClone(job),db={jobs:[job],ledger:[]};beginDomainCommand(db,{recordedAt:new Date(now).toISOString()});
  assert.equal(recordRecoveredGeneration(db,job,'image1',now),true);
  const command=finishDomainCommand(db),replayed=replayRequest([before],command,'request:legacy-j').j;
  assert.deepEqual(replayed.generationReview,job.generationReview);
  assert.equal(replayed.generationReview.status,'result_recovered');assert.equal(replayed.generationReview.resultImageId,'image1');
  assert.equal(replayed.status,'failed');assert.deepEqual(replayed.generationRefund,job.generationRefund);
});
test('real member-stop and batch-cancel producers preserve cancellation classification and deadline facts',()=>{
  for(const cancel of [
    db=>setMemberAdmission(db,'a',false,()=>{},now),
    db=>cancelWaitingBatchJobs(db,db.jobs[0],'batch stopped',409,now)
  ]){
    const db={users:[{id:'a',token:'protected',acceptingJobs:true,jobAdmissionVersion:0}],jobs:[
      {id:'first',requestId:'r',memberId:'a',status:'running'},{id:'waiting',requestId:'r',memberId:'a',status:'pending'}]};
    const before=structuredClone(db.jobs);beginDomainCommand(db,{recordedAt:new Date(now).toISOString()});cancel(db);
    const replayed=replayRequest(before,finishDomainCommand(db),'request:r').waiting,live=db.jobs[1];
    for(const field of ['status','cancelledAt','completedAt','errorStatus','cancellationReason'])assert.deepEqual(replayed[field],live[field],field);
  }
});
test('real JobFailed recorder retains explicit cancellation fields',()=>{
  const before={id:'j',status:'queued'},job={...before,status:'failed',cancelledAt:new Date(now).toISOString(),completedAt:new Date(now).toISOString(),cancellationReason:'request_cancelled',error:'cancelled',errorStatus:499};
  const db={jobs:[job],settings:{},users:[],accounts:[],ledger:[],images:[],cards:[]};beginDomainCommand(db);recordGenerationFailure(db,job);
  const replayed=replayRequest([before],finishDomainCommand(db),'request:legacy-j').j;
  assert.equal(replayed.cancelledAt,job.cancelledAt);assert.equal(replayed.cancellationReason,job.cancellationReason);
});
