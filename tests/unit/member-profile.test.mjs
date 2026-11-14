import test from 'node:test';
import assert from 'node:assert/strict';
import { publicMemberProfile } from '../../server/members/read-http.ts';
import { memberCharge } from '../../server/quota/failure-accounting.ts';

test('profile 公开白名单保留原 imageId/NULL/0 与已展示金额，不带入认证或输入字段', () => {
  const zero = new Date(0).toISOString(),
    amount = 0.1 + 0.2,
    charge = memberCharge({
      status: 'refunded',
      resource: 'anlas',
      amount,
      fixed: amount,
      purchased: 0,
    }),
    state = {
      username: 'canonical-name',
      owner: false,
      sessionExpiresAt: 0,
      acceptingJobs: false,
      quota: null,
      apiKey: 'private-key',
      memberAuth: { version: 'private-epoch' },
      jobs: [
        {
          id: 'job',
          requestId: 'request',
          createdAt: zero,
          lateResultAvailable: true,
          imageId: 'artifact',
          model: null,
          status: 'failed',
          charge,
          request: { input: 'private prompt' },
          pst: 'private-pst',
        },
        {
          id: 'no-image',
          requestId: null,
          createdAt: null,
          lateResultAvailable: false,
          imageId: null,
          model: null,
          status: 'queued',
          charge: null,
        },
      ],
      lateResults: [
        {
          id: 'job',
          model: null,
          createdAt: null,
          lateResultAt: zero,
          imageId: 'artifact',
          charge,
          raw: 'private diagnostic',
        },
      ],
    },
    original = structuredClone(state),
    signed = [],
    result = publicMemberProfile(state, (id) => {
      signed.push(id);
      return `/configured/${id}`;
    });
  assert.deepEqual(result, {
    username: 'canonical-name',
    owner: false,
    sessionExpiresAt: 0,
    acceptingJobs: false,
    quota: null,
    jobs: [
      {
        id: 'job',
        requestId: 'request',
        createdAt: zero,
        lateResultAvailable: true,
        imageId: 'artifact',
        model: null,
        status: 'failed',
        charge,
        imageUrl: '/configured/artifact',
      },
      {
        id: 'no-image',
        requestId: null,
        createdAt: null,
        lateResultAvailable: false,
        imageId: '',
        model: null,
        status: 'queued',
        charge: null,
        imageUrl: '',
      },
    ],
    lateResults: [
      {
        id: 'job',
        model: null,
        createdAt: null,
        lateResultAt: zero,
        imageUrl: '/configured/artifact',
        charge,
      },
    ],
  });
  assert.deepEqual(signed, ['artifact', 'artifact']);
  assert.equal(result.jobs[0].charge.amount, amount);
  assert.equal('imageId' in result.lateResults[0], false);
  assert.equal(JSON.stringify(result).includes('private'), false);
  assert.deepEqual(state, original);
});
