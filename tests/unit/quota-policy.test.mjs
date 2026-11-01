import test from 'node:test';
import assert from 'node:assert/strict';
import {hasActiveOpusSubscription,isUsableOpus,isQuotaSnapshotFresh,quotaTimestamp} from '../../server/quota-policy.ts';

const now=1800000000000;
test('共享订阅规则保留到期端点和同步错误区别',()=>{
 const account={enabled:true,subscriptionActive:true,quotaTier:3};
 assert.equal(hasActiveOpusSubscription(account,now),true);
 assert.equal(hasActiveOpusSubscription({...account,subscriptionExpiresAt:now},now),false);
 assert.equal(hasActiveOpusSubscription({...account,subscriptionExpiresAt:now+1},now),true);
 assert.equal(hasActiveOpusSubscription({...account,subscriptionExpiresAt:'bad'},now),false);
 assert.equal(hasActiveOpusSubscription({...account,quotaError:'failed'},now),true);
 assert.equal(isUsableOpus({...account,quotaError:'failed'},now),false);
});
test('共享快照规则接受十分钟和时钟偏差端点，拒绝无效时间',()=>{
 assert.equal(isQuotaSnapshotFresh(now-600000,now),true);
 assert.equal(isQuotaSnapshotFresh(now-600001,now),false);
 assert.equal(isQuotaSnapshotFresh(now+60000,now),true);
 assert.equal(isQuotaSnapshotFresh(now+60001,now),false);
 for(const value of [null,undefined,'',true,false,'invalid',-1,Infinity])assert.equal(isQuotaSnapshotFresh(value,now),false);
 assert.equal(quotaTimestamp(now/1000),now);
 assert.equal(quotaTimestamp(new Date(now).toISOString()),now);
});
