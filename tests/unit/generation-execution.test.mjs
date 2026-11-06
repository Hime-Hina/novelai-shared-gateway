import test from 'node:test';
import assert from 'node:assert/strict';
import { configureFairQuota, syncFairQuota, reserveFairQuota, previewFairQuotaCharge } from '../../server/quota/fair-share.ts';
import { normalizeMemberGeneration, materializeMemberGeneration, quoteMemberGeneration } from '../../server/generation/member-contract.ts';
import { assertConfirmedDispatch } from '../../server/generation/execution.ts';
import { singleGenerationRequest } from '../../server/generation/batch-requests.ts';

const now = 1800000000000;
const v5Charge = {resource:'v5',amount:'0.057803468209',fixed:'0',purchased:'0'};
function fixture() {
  const db = { settings: {}, ledger: [], jobs: [], users: ['a','b','c','d'].map(id => ({ id, token: id, enabled: true })),
    accounts: [{ id:'opus', enabled:true, subscriptionActive:true, quotaTier:3, quotaFixed:10000, quotaPurchased:0,
      v5UsagePercent:100, v5UsageTimeUntilNextPercent:0, quotaCheckedAt:now }] };
  configureFairQuota(db, db.users.map(user => user.id)); syncFairQuota(db, now);
  db.settings.fairQuota.autoAnlasFallback = true;
  const normalized = normalizeMemberGeneration({ operation:'generate', input:'synthetic', model:'nai-diffusion-5-full',
    parameters:{width:1024,height:1024,steps:23,seed:17,n_samples:2} });
  const quote = quoteMemberGeneration(db, db.users[0], normalized, {now});
  // Independent 12-decimal ceiling of the existing 100/1730 V5 formula.
  assert.deepEqual(quote.charges.perJob[0].resources, {v5:'0.057803468209',anlasFixed:'0',anlasPurchased:'0'});
  const confirmation = { requestFingerprint:quote.requestFingerprint, versions:quote.versions,
    maxPerJob:{v5:'0.057803468209',anlasFixed:'0',anlasPurchased:'0'}, maxTotal:{v5:'0.115606936418',anlasFixed:'0',anlasPurchased:'0'} };
  return {db,member:db.users[0],request:{...singleGenerationRequest(materializeMemberGeneration(normalized)),clientContract:'member-generation-v1',confirmation}};
}

test('each child uses exact original per-job and cumulative settled plus held bounds', () => {
  const {db,member,request} = fixture(), before = structuredClone(db);
  const settled = {resource:'v5',amount:'0.057803468209',status:'settled'};
  assert.doesNotThrow(() => assertConfirmedDispatch(db,member,request,[settled],v5Charge));
  assert.throws(() => assertConfirmedDispatch(db,member,request,[settled,{...settled,status:'reserved'}],v5Charge), error => error.code === 'cost_limit_exceeded');
  assert.doesNotThrow(() => assertConfirmedDispatch(db,member,request,[settled,{...settled,status:'refunded',lateResultAvailable:true}],v5Charge));
  assert.deepEqual(db,before);
});

test('a later billing-policy change requires a fresh quote and never silently widens a request', () => {
  const {db,member,request} = fixture(); db.settings.fairQuota.autoAnlasFallback = false;
  const before=structuredClone(db);
  assert.throws(() => assertConfirmedDispatch(db,member,request,[],v5Charge), error => error.code === 'quote_changed');
  assert.deepEqual(db,before);
});

test('a permitted fallback still cannot debit a resource with zero original authorization', () => {
  const {db,member,request} = fixture(); db.settings.fairQuota.balances.a.v5Available='0';
  const before=structuredClone(db);
  assert.throws(() => assertConfirmedDispatch(db,member,request,[],{resource:'anlas',amount:'26',fixed:'26',purchased:'0'}), error => error.code === 'cost_limit_exceeded');
  assert.deepEqual(db,before);
});

test('older protocols keep their original admission contract without synthetic confirmation', () => {
  const {db,member,request} = fixture(); delete request.clientContract; delete request.confirmation;
  assert.doesNotThrow(() => assertConfirmedDispatch(db,member,request,[],v5Charge));
});

test('one millisecond of refill cannot change billing after confirmation: authorize the final charge before writing', () => {
  for (const authorizeV5 of [false,true]) {
    const {db,member,request}=fixture(), account=db.accounts[0];
    account.v5UsagePercent=80; account.v5UsageTimeUntilNextPercent=600;
    db.settings.fairQuota.snapshot.v5='80'; db.settings.fairQuota.snapshot.seconds=600;
    db.settings.fairQuota.balances.a.v5Available='0.057803468208';
    request.confirmation.maxPerJob=request.confirmation.maxTotal={v5:authorizeV5?'0.057803468209':'0',anlasFixed:'26',anlasPurchased:'0'};
    const earlier=previewFairQuotaCharge(db,member,request,now);
    assert.equal(earlier.resource,'anlas'); assert.equal(earlier.amount,'26');
    const job={id:'last-unit-boundary',request}, check=()=>reserveFairQuota(db,member,job,account,now+1,request,null,
      charge=>assertConfirmedDispatch(db,member,request,[],charge,{}));
    if(!authorizeV5) {
      assert.throws(check,error=>error.code==='cost_limit_exceeded');
      assert.equal(job.fairCharge,undefined); assert.equal(job.generationAttempt,undefined);
      assert.equal(db.settings.fairQuota.reservations[job.id],undefined);
      assert.equal(db.settings.fairQuota.balances.a.anlasFixedAvailable,'2500');
    } else {
      const charge=check(); assert.equal(charge.resource,'v5'); assert.equal(charge.amount,'0.057803468209');
      assert.equal(job.fairCharge,charge); assert.equal(db.settings.fairQuota.balances.a.anlasFixedAvailable,'2500');
    }
  }
});
