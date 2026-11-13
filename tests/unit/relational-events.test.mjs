import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { domainDigest } from '../../server/events/domain-events.ts';
import {
  parseRelationalFact,
  parseRelationalEvent,
  RELATIONAL_EVENT_SCHEMA_VERSION,
} from '../../server/events/relational-events.ts';
import {
  relationalPayloadParsers,
  relationalFactStreams,
} from '../../server/events/relational-fact-contracts.ts';
import {
  appendRelationalCommit,
  readRelationalCommit,
  readRelationalEvents,
  replayRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';

const hash = 'a'.repeat(64),
  zero = { v5: '0', anlasFixed: '0', anlasPurchased: '0' };
const origin = { sourceId: 'legacy-snapshot', originalId: 'old-id' };

test('在线薄键事实显式保存noCache意图，不从seed或来源猜测', () => {
  const payload = {
    memberId: 'm',
    operation: 'generate',
    keyHash: hash,
    intentFingerprint: hash,
    requestId: 'r',
    jobId: 'j',
    contextId: 'c',
    originalCommandId: 'command',
    noCache: false,
    seedMode: 'fixed',
  };
  for (const noCache of [false, true]) {
    const fact = { eventType: 'RequestAliasRegistered', payload: { ...payload, noCache } };
    assert.deepEqual(parseRelationalFact(fact), fact);
  }
  const { noCache: _, ...missing } = payload;
  assert.throws(() =>
    parseRelationalFact({ eventType: 'RequestAliasRegistered', payload: missing }),
  );
  const {seedMode: _mode, ...withoutMode} = payload;
  assert.throws(() => parseRelationalFact({eventType: 'RequestAliasRegistered', payload: withoutMode}));
  assert.throws(() => parseRelationalFact({eventType: 'RequestAliasRegistered', payload: {...payload, seedMode: 'unknown'}}));
  assert.throws(() =>
    parseRelationalFact({
      eventType: 'RequestAliasRegistered',
      payload: { ...payload, noCache: 0 },
    }),
  );
});

test('副本事实保留实际 identity/gzip/br 编码，PikPak Brotli 不能错标或被未知编码替代', () => {
  const payload = {
    artifactId: 'a',
    storageTargetId: 's',
    objectKey: 'original',
    uploadedAt: 1,
    verifiedAt: null,
    deletedAt: null,
    lastErrorCode: null,
  };
  for (const encoding of ['identity', 'gzip', 'br']) {
    for (const eventType of ['ArtifactReplicaRecorded', 'OpeningReplicaRecorded']) {
      const fact = {
        eventType,
        payload: {
          ...payload,
          encoding,
          ...(eventType === 'OpeningReplicaRecorded' ? { origin } : {}),
        },
      };
      assert.deepEqual(parseRelationalFact(fact), fact);
    }
  }
  assert.throws(() =>
    parseRelationalFact({
      eventType: 'ArtifactReplicaRecorded',
      payload: { ...payload, encoding: 'deflate' },
    }),
  );
});

test('资金与恢复事实保存来源预算、V5 本人划入和有符号回跳，不能放宽外部资金边界', () => {
  const allocation = {
    eventType: 'AllocationApplied',
    payload: {
      accountId: 'a',
      reason: 'funding',
      sourceEventId: 'observation',
      sourceResources: { ...zero, anlasFixed: '3' },
      configVersion: 1,
      allocations: [{ memberId: 'm1', resources: { ...zero, anlasFixed: '3' } }],
    },
  };
  assert.deepEqual(parseRelationalFact(allocation), allocation);
  assert.throws(() =>
    parseRelationalFact({
      ...allocation,
      payload: { ...allocation.payload, reason: 'redistribution' },
    }),
  );
  const { sourceEventId: _, ...withoutSource } = allocation.payload;
  assert.throws(() => parseRelationalFact({ ...allocation, payload: withoutSource }));
  const transfer = {
    eventType: 'FundingReceived',
    payload: {
      accountId: 'a',
      receipt: 'receipt',
      resource: 'v5',
      amount: '0.1',
      destination: 'operating',
      observationEventId: null,
      receiptEventId: null,
      source: 'operator_share',
      memberId: 'm1',
    },
  };
  assert.deepEqual(parseRelationalFact(transfer), transfer);
  assert.throws(() =>
    parseRelationalFact({
      ...transfer,
      payload: { ...transfer.payload, source: 'declared_external_receipt' },
    }),
  );
  const returned = {
    eventType: 'ReturnClassified',
    payload: {
      accountId: 'a',
      resource: 'anlasFixed',
      amount: '-3',
      gapEventId: 'gap',
      observationEventId: 'observation',
      kind: 'observed',
    },
  };
  assert.deepEqual(parseRelationalFact(returned), returned);
  assert.throws(() =>
    parseRelationalFact({ ...returned, payload: { ...returned.payload, kind: 'gap_correction' } }),
  );
});
function database(t, historyId = 'history1') {
  const db = new Database(':memory:');
  t.after(() => db.close());
  initializeRelationalStorage(db, historyId);
  return db;
}
function member(memberId = 'm1') {
  return {
    streamId: `member:${memberId}`,
    occurredAt: 10,
    memberId,
    accountId: null,
    policyVersion: 'p1',
    pricingVersion: 'p1',
    eventType: 'MemberRegistered',
    payload: {
      memberId,
      displayName: memberId,
      status: 'active',
      acceptingJobs: true,
      admissionEpoch: 0,
      createdAt: 10,
    },
  };
}
function command(events = [member()]) {
  return {
    commandId: 'c1',
    fingerprint: hash,
    actorKind: 'admin',
    actorId: 'owner',
    recordedAt: 20,
    expectedVersions: Object.fromEntries(events.map((event) => [event.streamId, '0'])),
    receipt: { memberId: 'm1' },
    correlationId: null,
    causationId: null,
    events,
  };
}
function accountEvent(fact, accountId = 'a') {
  return {
    ...member(fact.payload.memberId || 'm1'),
    ...fact,
    accountId,
    streamId: `account:${accountId}`,
    streamKind: 'account',
    eventId: 'e',
    commitSeq: '1',
    eventIndex: 0,
    streamVersion: '1',
    schemaVersion: RELATIONAL_EVENT_SCHEMA_VERSION,
    correlationId: null,
    causationId: null,
    amountVersion: 'decimal-12-v1',
  };
}

test('所有 v3 事实都有唯一 stream owner；旧大状态、未知事实、隐私和额外字段被拒绝', () => {
  const owners = Object.values(relationalFactStreams).flat();
  assert.equal(new Set(owners).size, owners.length);
  assert.deepEqual([...owners].sort(), Object.keys(relationalPayloadParsers).sort());
  for (const eventType of ['OpeningStateRecorded', 'LegacyFactRecorded', 'NotAnEvent']) {
    assert.throws(() => parseRelationalFact({ eventType, payload: {} }), /Unsupported/);
  }
  const valid = {
    eventType: 'GenerationSettled',
    payload: { jobId: 'j', reservationEventId: 'e' },
  };
  for (const field of [
    'state',
    'request',
    'requestPayload',
    'charge',
    'attempt',
    'jobs',
    'history',
    'evidence',
  ]) {
    assert.throws(() =>
      parseRelationalFact({ ...valid, payload: { ...valid.payload, [field]: {} } }),
    );
  }
  assert.throws(
    () => parseRelationalFact({ ...valid, payload: { ...valid.payload, token: 'must-not-leak' } }),
    (error) => !error.message.includes('must-not-leak'),
  );
  assert.throws(() => parseRelationalFact({ ...valid, payload: { jobId: 'j' } }));
  assert.deepEqual(parseRelationalFact(valid), valid);
  assert.throws(() =>
    parseRelationalFact({
      eventType: 'MemberRegistered',
      payload: { ...member().payload, authEpoch: 'revoked' },
    }),
  );
});

test('金额只接受规范十进制，官方未知保持 null；资金来源与凭据代次不能被补造', () => {
  const reservation = {
    eventType: 'QuotaReserved',
    payload: {
      jobId: 'j',
      memberId: 'm',
      accountId: 'a',
      resources: { ...zero, v5: '0.058' },
      billingMode: 'v5',
      estimated: true,
      v5Equivalent: '0',
      sharedV5Reserved: '0',
      configVersion: 1,
    },
  };
  for (const bad of [0.058, '0.0580', '1e3', '-0', '0.0000000000001', 'NaN']) {
    assert.throws(() =>
      parseRelationalFact({
        ...reservation,
        payload: { ...reservation.payload, resources: { ...zero, v5: bad } },
      }),
    );
  }
  const huge = `${'9'.repeat(100)}.000000000001`;
  assert.doesNotThrow(() => parseRelationalEvent(accountEvent(reservation)));
  assert.throws(() => parseRelationalEvent({ ...accountEvent(reservation), accountId: 'wrong' }));
  assert.equal(
    parseRelationalFact({
      ...reservation,
      payload: { ...reservation.payload, resources: { ...zero, v5: huge } },
    }).payload.resources.v5,
    huge,
  );
  const observation = {
    eventType: 'QuotaObserved',
    payload: {
      accountId: 'a',
      credentialEpoch: 3,
      sequence: '9007199254740993',
      observedAt: 10,
      quotaTier: null,
      subscriptionActive: null,
      subscriptionExpiresAt: null,
      v5: null,
      anlasFixed: null,
      anlasPurchased: null,
      secondsPerPercent: null,
      errorCode: 'upstream_unavailable',
    },
  };
  assert.deepEqual(parseRelationalFact(observation), observation);
  assert.throws(() =>
    parseRelationalFact({
      eventType: 'CredentialRotated',
      payload: { accountId: 'a', beforeEpoch: 3, credentialEpoch: 1 },
    }),
  );
  assert.deepEqual(
    parseRelationalFact({
      eventType: 'CredentialRotated',
      payload: { accountId: 'a', beforeEpoch: 3, credentialEpoch: 4 },
    }).payload.credentialEpoch,
    4,
  );
  const funding = {
    eventType: 'FundingReceived',
    payload: {
      accountId: 'a',
      receipt: 'r',
      resource: 'anlasFixed',
      amount: '1',
      destination: 'members',
      observationEventId: 'o',
      receiptEventId: null,
      source: 'operator_share',
      memberId: null,
    },
  };
  assert.throws(() => parseRelationalFact(funding));
});

test('归档开账允许仅剩旧键/退款凭据，活动对象仍要求完整关系和来源', () => {
  const key = {
    eventType: 'OpeningIdempotencyRecorded',
    payload: {
      originalMemberId: 'removed',
      operation: 'generate',
      keyHash: hash,
      intentFingerprint: hash,
      originalRequestId: 'missing-request',
      originalJobId: 'missing-job',
      originalCommandId: 'missing-command',
      requestId: null,
      commitSeq: null,
      contextId: null,
      resolution: 'archived',
      originRef: 'archive:manifest',
      archivedReceipt: { requestId: 'missing-request' },
      createdAt: 1,
      origin,
    },
  };
  assert.deepEqual(parseRelationalFact(key), key);
  assert.throws(() =>
    parseRelationalFact({ ...key, payload: { ...key.payload, resolution: 'live' } }),
  );
  assert.throws(() => parseRelationalFact({ ...key, payload: { ...key.payload, origin: null } }));
  const refund = {
    eventType: 'OpeningChargeRecorded',
    payload: {
      originalJobId: 'missing-job',
      originalMemberId: 'removed',
      originalAccountId: 'old-account',
      jobId: null,
      walletAccountId: null,
      walletMemberId: null,
      attemptId: null,
      status: 'refunded',
      resolution: 'archived',
      originRef: 'archive:manifest',
      resources: { ...zero, v5: '0.058' },
      billingMode: 'v5',
      estimated: null,
      v5Equivalent: '0',
      sharedV5Reserved: '0',
      configVersion: 1,
      reservedAt: 1,
      terminalAt: 2,
      reservationEventId: 'opening-reserved',
      terminalEventId: 'opening-refund',
      origin,
    },
  };
  assert.deepEqual(parseRelationalFact(refund), refund);
  const opening = accountEvent(refund, 'old-account');
  assert.doesNotThrow(() => parseRelationalEvent(opening));
  assert.throws(() =>
    parseRelationalEvent({
      ...opening,
      accountId: 'wrong',
      streamId: 'account:wrong',
    }),
  );
  assert.throws(() =>
    parseRelationalFact({ ...refund, payload: { ...refund.payload, status: 'reserved' } }),
  );
  assert.throws(() =>
    parseRelationalFact({ ...refund, payload: { ...refund.payload, jobId: 'a-different-job' } }),
  );
  assert.throws(() =>
    parseRelationalFact({
      ...refund,
      payload: { ...refund.payload, walletAccountId: 'old-account' },
    }),
  );
});

test('所有账户流直接绑定非空 envelope 账户；结算/退款错配在写入前失败', (t) => {
  const db = database(t),
    before = db.serialize();
  for (const eventType of ['GenerationSettled', 'GenerationRefunded']) {
    const event = accountEvent({
      eventType,
      payload: {
        jobId: 'j',
        reservationEventId: 'reservation',
        ...(eventType === 'GenerationRefunded' ? { lossEventId: null } : {}),
      },
    });
    assert.doesNotThrow(() => parseRelationalEvent(event));
    for (const changed of [
      { streamId: 'account:wrong' },
      { accountId: 'wrong' },
      { accountId: null },
      { accountId: '' },
    ]) {
      assert.throws(() => parseRelationalEvent({ ...event, ...changed }));
    }
    const input = { ...event, streamId: 'account:wrong' };
    for (const key of [
      'eventId',
      'commitSeq',
      'eventIndex',
      'streamVersion',
      'streamKind',
      'schemaVersion',
      'correlationId',
      'causationId',
      'amountVersion',
    ]) {
      delete input[key];
    }
    db.transaction(() => {
      assert.throws(() => appendRelationalCommit(db, command([input])));
    })();
    assert.deepEqual(db.serialize(), before);
  }
});

test('原子提交保存固定版本、原回执和精确游标；未知内容或旧版本不能写入', (t) => {
  const db = database(t),
    cmd = command();
  assert.throws(() => appendRelationalCommit(db, cmd), /owning command/);
  const append = (value) => db.transaction(() => appendRelationalCommit(db, value)).immediate();
  const first = append(cmd),
    before = db.serialize();
  assert.equal(first.commitSeq, '1');
  assert.equal(append(cmd).replayed, true);
  assert.throws(() => append({ ...cmd, actorId: 'another' }), /idempotency conflict/);
  assert.throws(
    () => append({ ...cmd, fingerprint: domainDigest('changed') }),
    /idempotency conflict/,
  );
  assert.throws(() => append({ ...cmd, commandId: 'c2' }), /stream version conflict/);
  assert.throws(() =>
    append({
      ...command([member('m2')]),
      commandId: 'bad',
      events: [{ ...member('m2'), payload: { ...member('m2').payload, password: 'private' } }],
    }),
  );
  assert.deepEqual(db.serialize(), before);
  const event = readRelationalEvents(db, '1')[0];
  for (const changed of [
    { schemaVersion: 1 },
    { amountVersion: 'float' },
    { streamKind: 'account', streamId: 'account:m1' },
    { streamId: 'member:another' },
    { memberId: 'another' },
    { memberId: null },
  ]) {
    assert.throws(() => parseRelationalEvent({ ...event, ...changed }));
  }
  assert.deepEqual(readRelationalCommit(db, '1').events, first.events);
});

test('缺失或不兼容投影游标不能提交，即使调用者捕获失败也不留下半笔事件', (t) => {
  const db = database(t);
  for (const corrupt of [
    'UPDATE projection_state SET reducer_version=99',
    "UPDATE projection_state SET status='failed'",
    'UPDATE projection_state SET last_commit_seq=1',
    'DELETE FROM projection_state',
  ]) {
    db.transaction(() => {
      db.exec(corrupt);
      assert.throws(() => appendRelationalCommit(db, command()), /projection is incomplete/);
      assert.equal(db.prepare('SELECT COUNT(*) AS n FROM commits').get().n, 0);
      assert.equal(db.prepare('SELECT COUNT(*) AS n FROM domain_events').get().n, 0);
      assert.equal(db.prepare('SELECT COUNT(*) AS n FROM stream_heads').get().n, 0);
    })();
    db.exec('DELETE FROM projection_state');
    db.prepare("INSERT INTO projection_state VALUES(1,'history1',1,0,'building')").run();
  }
});

test('新事实重放保留原身份与回执，拒绝不同历史、提交断点和半笔写入', (t) => {
  const source = database(t),
    target = database(t),
    wrong = database(t, 'another-history');
  source
    .transaction(() => appendRelationalCommit(source, command([member('m1'), member('m2')])))
    .immediate();
  const sourceBefore = source.serialize(),
    stored = readRelationalCommit(source, '1');
  const replay = (value = stored) =>
    target.transaction(() => replayRelationalCommit(target, value)).immediate();
  wrong.transaction(() =>
    assert.throws(() => replayRelationalCommit(wrong, stored), /history mismatch/),
  )();
  const broken = structuredClone(stored);
  broken.events[1].eventId = broken.events[0].eventId;
  target.transaction(() => {
    assert.throws(() => replayRelationalCommit(target, broken), /UNIQUE/);
  })();
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM commits').get().n, 0);
  assert.equal(target.prepare('SELECT COUNT(*) AS n FROM domain_events').get().n, 0);
  assert.equal(
    target.prepare('SELECT last_commit_seq FROM projection_state').get().last_commit_seq,
    0,
  );
  const unknown = structuredClone(stored);
  unknown.events[0].schemaVersion = 99;
  assert.throws(() => replay(unknown), /Unsupported/);
  assert.equal(replay().replayed, false);
  const before = target.serialize();
  assert.equal(replay().replayed, true);
  const changedTime = structuredClone(stored);
  changedTime.command.recordedAt += 1;
  assert.throws(() => replay(changedTime), /checkpoint differs/);
  assert.deepEqual(target.serialize(), before);
  assert.deepEqual(readRelationalEvents(target, '1'), readRelationalEvents(source, '1'));
  assert.deepEqual(source.serialize(), sourceBefore);
});

test('同一命令预先分配事件身份可原子引用，空身份和重复身份不留下半笔提交', (t) => {
  const source = database(t),
    target = database(t);
  const envelope = { ...member(), streamId: 'account:a', accountId: 'a' };
  const reserved = {
    ...envelope,
    eventId: 'reservation-event',
    eventType: 'QuotaReserved',
    payload: {
      jobId: 'j',
      memberId: 'm1',
      accountId: 'a',
      resources: { ...zero },
      billingMode: 'free',
      estimated: false,
      v5Equivalent: '0',
      sharedV5Reserved: '0',
      configVersion: 1,
    },
  };
  const settled = {
    ...envelope,
    eventId: 'terminal-event',
    eventType: 'GenerationSettled',
    payload: {
      jobId: 'j',
      reservationEventId: reserved.eventId,
    },
  };
  source.transaction(() => appendRelationalCommit(source, command([reserved, settled])))();
  const original = readRelationalCommit(source, '1');
  assert.equal(original.events[0].eventId, reserved.eventId);
  assert.equal(original.events[1].payload.reservationEventId, original.events[0].eventId);
  target.transaction(() => replayRelationalCommit(target, original))();
  assert.deepEqual(readRelationalCommit(target, '1'), original);
  const checkpoint = source.serialize();
  for (const eventId of ['', reserved.eventId]) {
    const next = command([{ ...settled, eventId }]);
    next.commandId = 'conflicting-identity';
    next.expectedVersions = { 'account:a': '2' };
    source.transaction(() => assert.throws(() => appendRelationalCommit(source, next)))();
    assert.deepEqual(source.serialize(), checkpoint);
  }
});

test('原提交事件数独立保存，两个列表同时截短也不能写入事实、流头或游标', (t) => {
  const source = database(t),
    target = database(t);
  const original = command([member('m1'), member('m2')]);
  original.receipt = { memberIds: ['m1', 'm2'] };
  source.transaction(() => appendRelationalCommit(source, original))();
  const stored = readRelationalCommit(source, '1'),
    before = target.serialize();
  for (const lists of [['events', 'command'], ['events'], ['command']]) {
    const trimmed = structuredClone(stored);
    if (lists.includes('events')) {
      trimmed.events.pop();
    }
    if (lists.includes('command')) {
      trimmed.command.events.pop();
    }
    delete trimmed.command.expectedVersions['member:m2'];
    target.transaction(() => {
      assert.throws(() => replayRelationalCommit(target, trimmed), /complete original commit/);
    })();
    assert.deepEqual(target.serialize(), before);
  }
  assert.equal(stored.eventCount, 2);
  target.transaction(() => replayRelationalCommit(target, stored))();
  const checkpoint = target.serialize();
  target.transaction(() => {
    assert.equal(replayRelationalCommit(target, stored).replayed, true);
    assert.throws(
      () => replayRelationalCommit(target, { ...stored, eventCount: 1 }),
      /complete original commit/,
    );
  })();
  assert.deepEqual(target.serialize(), checkpoint);
  assert.deepEqual(readRelationalCommit(target, '1'), stored);
});
