import test from 'node:test';
import assert from 'node:assert/strict';
import { beginDomainCommand, inheritDomainCommand, recordDomainEvent, setDomainCommandReceipt, finishDomainCommand } from '../../server/domain-events.ts';

const fact={eventType:'MemberEligibilityChanged',streamId:'sharing:default',payload:{memberId:'member',enabled:true}};
test('closing a command closes all draft aliases and leaves the next command isolated',()=>{
  const source={},draft={};beginDomainCommand(source);inheritDomainCommand(source,draft);
  recordDomainEvent(draft,fact);
  const closed=finishDomainCommand(source);
  assert.equal(closed.events.length,1);
  beginDomainCommand(source);
  assert.throws(()=>recordDomainEvent(draft,fact),/closed/);
  assert.throws(()=>setDomainCommandReceipt(draft,{accepted:true}),/closed/);
  assert.throws(()=>inheritDomainCommand(draft,{}),/closed/);
  assert.equal(closed.events.length,1);
  assert.equal(finishDomainCommand(source).events.length,0);
  const discarded={};beginDomainCommand(discarded);const alias={};inheritDomainCommand(discarded,alias);finishDomainCommand(discarded);
  assert.throws(()=>recordDomainEvent(alias,fact),/closed/);
});
test('initial receipts use the same secret validation and ownership isolation as later receipts',()=>{
  const state={};assert.throws(()=>beginDomainCommand(state,{receipt:{userToken:'secret'}}),/forbidden/);
  const receipt={accepted:{requestId:'r'}};beginDomainCommand(state,{receipt});receipt.accepted.token='later secret';
  assert.deepEqual(finishDomainCommand(state).receipt,{accepted:{requestId:'r'}});
  beginDomainCommand(state);assert.throws(()=>setDomainCommandReceipt(state,{cookie:'secret'}),/forbidden/);finishDomainCommand(state);
});
