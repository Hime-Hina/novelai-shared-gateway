import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import ts from 'typescript';

test('事件写入判别联合拒绝丢失生命周期事实与错误金额类型',()=>{
  const filename=fileURLToPath(new URL('./domain-event-types.fixture.ts',import.meta.url));
  const source=`
import {recordDomainEvent,type DomainEventInput} from '../../server/domain-events.ts';
import type {FairCharge} from '../../server/fair-quota.ts';
import type {AccountingJob} from '../../server/generation-accounting.ts';
declare const db: object;
declare const charge: FairCharge;
declare const review: NonNullable<AccountingJob['generationReview']>;
recordDomainEvent(db,{eventType:'WaitingJobsCancelled',streamId:'request:r',payload:{jobIds:['j'],at:1,reason:'stopped',errorStatus:409,cancellationReason:'member_stopped'}});
recordDomainEvent(db,{eventType:'LateResultRecovered',streamId:'request:r',payload:{jobId:'j',artifactId:'a',at:2,refundRetained:true,review}});
recordDomainEvent(db,{eventType:'ReviewClosed',streamId:'request:r',payload:{jobId:'j',review,reason:'deadline',actualConsumed:null,upstreamCandidates:null}});
recordDomainEvent(db,{eventType:'ArtifactHistoryRetired',streamId:'artifact:a',payload:{artifactId:'a',reason:'owner_cleared_cache',at:3,jobIds:['j']}});
recordDomainEvent(db,{eventType:'FundingReceived',streamId:'account:opus',payload:{id:'f',resource:'anlasFixed',amount:'1',source:'operator_share',memberId:'m'}});
recordDomainEvent(db,{eventType:'FundingReceived',streamId:'account:opus',payload:{id:'f',resource:'anlasFixed',amount:'1',source:'registered_external_funding',receiptEventId:'receipt',destination:'members',snapshotAt:1}});
// @ts-expect-error An operator contribution must identify the wallet that supplied it.
recordDomainEvent(db,{eventType:'FundingReceived',streamId:'account:opus',payload:{id:'f',resource:'anlasFixed',amount:'1',source:'operator_share'}});
// @ts-expect-error Registered external funding must identify the receipt and destination.
recordDomainEvent(db,{eventType:'FundingReceived',streamId:'account:opus',payload:{id:'f',resource:'anlasFixed',amount:'1',source:'registered_external_funding',snapshotAt:1}});
// @ts-expect-error Cancellation reason must be recorded, not reconstructed by the reader.
recordDomainEvent(db,{eventType:'WaitingJobsCancelled',streamId:'request:r',payload:{jobIds:['j'],at:1,reason:'stopped',errorStatus:409}});
// @ts-expect-error Recovered results must preserve the current review state.
recordDomainEvent(db,{eventType:'LateResultRecovered',streamId:'request:r',payload:{jobId:'j',artifactId:'a',at:2,refundRetained:true}});
// @ts-expect-error Job queue facts cannot carry a different lifecycle event's payload.
recordDomainEvent(db,{eventType:'JobQueued',streamId:'request:r',payload:{jobId:'j',artifactId:'a',at:2,refundRetained:true,review}});
// @ts-expect-error A prepared artifact must identify its original attempt.
recordDomainEvent(db,{eventType:'ArtifactPrepared',streamId:'request:r',payload:{jobId:'j',artifact:{id:'a',imageId:'a',file:'images/a.png',operation:'generate',mimeType:'image/png',sha256:'hash',byteLength:1}}});
// @ts-expect-error Dispatch includes the immutable attempt, not only its ID.
recordDomainEvent(db,{eventType:'DispatchAuthorized',streamId:'request:r',payload:{jobId:'j',accountId:'opus',attemptId:'attempt',authorizedAt:1}});
// @ts-expect-error Failure facts require the explicit refund/review and cancellation fields.
recordDomainEvent(db,{eventType:'JobFailed',streamId:'request:r',payload:{jobId:'j',failedAt:2,message:'failed',errorStatus:500,errorCode:'generation_failed',charge}});
// @ts-expect-error A cached result retains its account source.
recordDomainEvent(db,{eventType:'CachedResultLinked',streamId:'request:r',payload:{jobId:'j',artifactId:'a',completedAt:1,localConsumption:{v5:'0',anlasFixed:'0',anlasPurchased:'0'}}});
// @ts-expect-error Admission must keep the protected input length.
recordDomainEvent(db,{eventType:'RequestAccepted',streamId:'request:r',payload:{requestId:'r',memberId:'m',source:'web',operation:'generate',createdAt:'now',inputRef:'r',inputSha256:'hash',jobs:[]}});
// @ts-expect-error Review closure explicitly records whether candidate amounts are available.
recordDomainEvent(db,{eventType:'ReviewClosed',streamId:'request:r',payload:{jobId:'j',review,reason:'deadline',actualConsumed:null}});
// @ts-expect-error Monetary facts persist decimal text.
recordDomainEvent(db,{eventType:'QuotaReserved',streamId:'account:opus',payload:{jobId:'j',charge:{...charge,amount:0.1},at:1}});
// @ts-expect-error Artifact retirement must record all affected job references.
recordDomainEvent(db,{eventType:'ArtifactHistoryRetired',streamId:'artifact:a',payload:{artifactId:'a',reason:'owner_cleared_cache',at:3}});
// @ts-expect-error A broad event-name variable and arbitrary payload are not a typed write.
const untyped:DomainEventInput={eventType:'FreeImageSucceeded',streamId:'request:r',payload:{}};
`;
  const options={target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.NodeNext,moduleResolution:ts.ModuleResolutionKind.NodeNext,
    strict:true,noEmit:true,allowJs:true,checkJs:false,allowImportingTsExtensions:true,skipLibCheck:true,types:['node']};
  const host=ts.createCompilerHost(options),read=host.readFile.bind(host),exists=host.fileExists.bind(host),get=host.getSourceFile.bind(host);
  const isFixture=name=>path.resolve(name).toLowerCase()===path.resolve(filename).toLowerCase();
  host.readFile=name=>isFixture(name)?source:read(name);
  host.fileExists=name=>isFixture(name)||exists(name);
  host.getSourceFile=(name,version,onError,newFile)=>isFixture(name)?ts.createSourceFile(name,source,version,true):get(name,version,onError,newFile);
  const program=ts.createProgram([filename],options,host),diagnostics=ts.getPreEmitDiagnostics(program);
  assert.equal(diagnostics.length,0,ts.formatDiagnosticsWithColorAndContext(diagnostics,{getCanonicalFileName:name=>name,getCurrentDirectory:()=>process.cwd(),getNewLine:()=> '\n'}));
});
