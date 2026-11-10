import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import Database from 'better-sqlite3';
import {
  JsonStore,
  verifyEventProjections,
  applyEventProjection,
} from '../../server/storage/sqlite-store.ts';
import {
  configureFairQuota,
  syncFairQuota,
  reserveFairQuota,
  settleFairQuota,
} from '../../server/quota/fair-share.ts';
import { domainDigest, recordDomainEvent } from '../../server/events/domain-events.ts';
import {
  readCommandReceipt,
  readProjectedFairQuota,
  readStreamProjection,
  eventAuthorityEnabled,
  importOpeningFacts,
  copyProtectedFacts,
  replayDomainEvents,
} from '../../server/events/event-store.ts';
import { parseAmount } from '../../server/quota/resource-amounts.ts';
import { createGenerationAdmission } from '../../server/generation/admission.ts';
import { createGenerationPayloadBudget } from '../../server/generation/batch-requests.ts';
import { recordGenerationFailure } from '../../server/generation/execution.ts';
import { migrateStorage } from '../../server/storage/sqlite-migrations.ts';
import { closeGenerationReview } from '../../server/generation/accounting.ts';
import { createAccountQuotaRefresh } from '../../server/quota/account-refresh.ts';
import { publicErrorMessage } from '../../server/http/public-errors.ts';
import { sharedAccountStatus } from '../../server/quota/shared-status.ts';

const now = 1793491200000,
  at = new Date(now).toISOString();
function changeFallback(db, enabled, time = now) {
  db.settings.fairQuota.autoAnlasFallback = enabled;
  db.settings.fairQuota.policyHistory = [
    ...(db.settings.fairQuota.policyHistory || []),
    { at: time, actor: 'administrator', autoAnlasFallback: enabled },
  ].slice(-1000);
  recordDomainEvent(db, {
    eventType: 'PolicyChanged',
    streamId: 'sharing:default',
    occurredAt: time,
    payload: { policy: 'autoAnlasFallback', value: enabled, actorId: 'local-admin', at: time },
  });
}
async function fixture(t) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'nai-event-authority-'));
  const store = new JsonStore(dir);
  await store.init();
  t.after(async () => {
    await store.close();
    await rm(dir, { recursive: true, force: true });
  });
  const db = structuredClone(store.db);
  db.users = ['a', 'b', 'c', 'd'].map((id) => ({
    id,
    token: `secret-${id}`,
    enabled: true,
    acceptingJobs: true,
    jobAdmissionVersion: 0,
  }));
  db.accounts = [
    {
      id: 'opus',
      enabled: true,
      quotaTier: 3,
      subscriptionActive: true,
      quotaFixed: 10000,
      quotaPurchased: 0,
      v5UsagePercent: 80,
      quotaCheckedAt: now,
    },
  ];
  configureFairQuota(db, ['a', 'b', 'c', 'd']);
  syncFairQuota(db, now);
  await store.write(db);
  return store;
}

test('artifact storage changes without a fact fail and roll back the authoritative row', async (t) => {
  const store = await fixture(t);
  const image = {
    id: 'cold',
    memberId: 'a',
    file: 'images/cold.png',
    mimeType: 'image/png',
    byteLength: 64,
    sha256: 'a'.repeat(64),
    createdAt: at,
    r2: {
      bucket: 'synthetic-images',
      key: `images/cold/${'a'.repeat(64)}`,
      sha256: 'a'.repeat(64),
      byteLength: 64,
      verifiedAt: at,
      localRemovedAt: at,
    },
  };
  await store.update(
    (db) => {
      db.images.push(image);
      db.jobs.push({
        id: 'legacy-job',
        memberId: 'a',
        userToken: 'secret-a',
        status: 'done',
        imageId: image.id,
        createdAt: new Date().toISOString(),
      });
    },
    { immediate: true, collections: ['images', 'jobs'] },
  );
  await store.enableEventAuthority();
  const before = store.selectItemById('images', image.id);
  const clock = () => store.sqlite.prepare('SELECT value FROM event_clock WHERE id=1').get().value;
  const cursor = clock();
  for (const [change, field] of [
    [
      (item) => {
        item.r2.pikpak = { url: 'https://cold.invalid/archive', uploadedAt: at };
      },
      /images\.cold\.r2\.pikpak/,
    ],
    [
      (item) => {
        item.pikpakError = 'synthetic unrecorded error';
      },
      /images\.cold\.pikpakError/,
    ],
    [
      (item) => {
        item.file = 'images/replaced.png';
      },
      /images\.cold\.file/,
    ],
  ]) {
    await assert.rejects(
      store.update(
        () => {
          change(store.loadItemForUpdate('images', image.id));
        },
        { dirtyRows: { images: [image.id] }, immediate: true },
      ),
      field,
    );
    assert.deepEqual(store.selectItemById('images', image.id), before);
    assert.deepEqual(await store.findImage(image.id), before);
    assert.equal(clock(), cursor);
  }
  await assert.rejects(
    store.update(
      (db) => {
        db.images.push({ ...image, id: 'unrecorded', jobId: 'legacy-job' });
      },
      { collections: ['images'], immediate: true },
    ),
    /Unrecorded artifact mutation unrecorded/,
  );
  assert.equal(await store.findImage('unrecorded'), null);
  assert.throws(
    store.sqlite.transaction(() => {
      const drift = structuredClone(before);
      drift.r2.removedAt = at;
      store.sqlite
        .prepare('UPDATE images SET data=? WHERE id=?')
        .run(JSON.stringify(drift), image.id);
      verifyEventProjections(store.sqlite);
    }),
    /images\.cold\.r2\.removedAt/,
  );
  store.sqlite.transaction(() => verifyEventProjections(store.sqlite))();
});

test('真实刷新同批提交可用状态，失败回滚、重开与离线重放保留完整钱包和观察', async (t) => {
  const store = await fixture(t);
  await store.enableEventAuthority();
  let clock = now + 1,
    fail = true;
  const refresh = createAccountQuotaRefresh({
    store,
    now: () => clock,
    env: {},
    publicAccount: (value) => value,
    publicErrorMessage,
    logger: { log() {}, error() {} },
    fetchQuota: async () => {
      if (fail) {
        throw new Error('private upstream token pst-secret https://secret.invalid');
      }
      return {
        tier: 3,
        subscriptionActive: true,
        subscriptionExpiresAt: now + 86400000,
        points: 10000,
        fixed: 10000,
        purchased: 0,
        v5UsagePercent: 80,
      };
    },
  });
  const snapshot = () => ({
    accounts: structuredClone(store.db.accounts),
    quota: structuredClone(store.db.settings.fairQuota),
    events: store.sqlite
      .prepare('SELECT * FROM domain_events ORDER BY commit_seq,event_index')
      .all(),
    commits: store.sqlite.prepare('SELECT * FROM event_commits ORDER BY commit_seq').all(),
  });
  const assertReplay = () => {
    const target = new Database(':memory:');
    try {
      migrateStorage(target);
      copyProtectedFacts(store.sqlite, target);
      replayDomainEvents(store.sqlite, target, { project: applyEventProjection });
      target.transaction(() => verifyEventProjections(target))();
      assert.deepEqual(readProjectedFairQuota(target), readProjectedFairQuota(store.sqlite));
      assert.deepEqual(
        JSON.parse(target.prepare("SELECT data FROM accounts WHERE id='opus'").get().data),
        store.selectItemById('accounts', 'opus'),
      );
    } finally {
      target.close();
    }
  };
  const opening = snapshot(),
    wallets = structuredClone(opening.quota.balances),
    baseline = structuredClone(opening.quota.snapshot);
  store.sqlite.exec(
    "CREATE TEMP TRIGGER reject_quota_availability BEFORE INSERT ON domain_events WHEN NEW.event_type='AvailabilityChanged' BEGIN SELECT RAISE(ABORT,'synthetic availability failure'); END",
  );
  await assert.rejects(refresh.refresh(['opus']), /synthetic availability failure/);
  assert.deepEqual(snapshot(), opening);
  store.sqlite.exec('DROP TRIGGER reject_quota_availability');
  assert.equal((await refresh.refresh(['opus'])).failed, 1);
  const failed = snapshot();
  assert.equal(failed.quota.status, 'quota_sync_error');
  assert.equal(sharedAccountStatus(store.db, clock).status, 'quota_sync_error');
  assert.deepEqual(failed.quota.balances, wallets);
  assert.deepEqual(failed.quota.snapshot, baseline);
  const events = failed.events.slice(opening.events.length);
  assert.deepEqual(
    events.map((event) => event.event_type),
    ['QuotaObserved', 'AvailabilityChanged'],
  );
  assert.equal(new Set(events.map((event) => event.commit_seq)).size, 1);
  for (const secret of ['pst-secret', 'secret.invalid', 'private upstream']) {
    assert.equal(JSON.stringify(events).includes(secret), false);
  }
  assertReplay();
  await store.close();
  await store.init();
  assert.equal(store.db.settings.fairQuota.status, 'quota_sync_error');
  assert.deepEqual(store.db.settings.fairQuota.balances, wallets);
  fail = false;
  clock++;
  assert.equal((await refresh.refresh(['opus'])).ok, 1);
  const recovered = snapshot();
  assert.equal(recovered.quota.status, 'active');
  assert.deepEqual(recovered.quota.balances, wallets);
  assert.equal(store.db.accounts[0].quotaError, '');
  await refresh.refresh(['opus']);
  assert.deepEqual(store.db.settings.fairQuota.balances, wallets);
  assertReplay();
  await store.close();
  await store.init();
  assert.equal(store.db.settings.fairQuota.status, 'active');
  assert.deepEqual(store.db.settings.fairQuota.balances, wallets);
});
test('command receipts precede mutation and stale expected versions without accepting an old candidate', async (t) => {
  const store = await fixture(t);
  await store.enableEventAuthority();
  let executed = 0,
    authorized = 0;
  const version = readStreamProjection(store.sqlite, 'sharing:default').streamVersion;
  const invoke = (id, enabled, extra = {}) =>
    store.update(
      (db) => {
        executed++;
        changeFallback(db, enabled, now + executed);
        return { executed, enabled };
      },
      {
        collections: ['settings'],
        immediate: true,
        authorize() {
          authorized++;
        },
        domainCommand: { commandId: id, fingerprint: domainDigest({ enabled }), ...extra },
      },
    );
  const first = await invoke('policy-A', true, {
    expectedVersions: { 'sharing:default': version },
  });
  await invoke('policy-B', false);
  assert.deepEqual(
    await invoke('policy-A', true, { expectedVersions: { 'sharing:default': version } }),
    first,
  );
  assert.equal(executed, 2);
  assert.equal(authorized, 3);
  assert.equal(store.db.settings.fairQuota.autoAnlasFallback, false);
  assert.equal(
    JSON.parse(
      store.sqlite.prepare("SELECT value FROM app_settings WHERE key='settings'").get().value,
    ).fairQuota.autoAnlasFallback,
    false,
  );
  assert.equal(readProjectedFairQuota(store.sqlite).autoAnlasFallback, false);
  assert.deepEqual(readCommandReceipt(store.sqlite, 'policy-A').receipt, first);
  await assert.rejects(
    invoke('policy-A', false),
    (error) => error.statusCode === 409 && /fingerprint conflict/.test(error.message),
  );
  await assert.rejects(
    store.update(
      () => {
        executed++;
      },
      {
        domainCommand: { commandId: 'policy-A', fingerprint: domainDigest({ enabled: true }) },
        authorize() {
          throw Object.assign(new Error('revoked'), { statusCode: 403 });
        },
      },
    ),
    /revoked/,
  );
  assert.equal(executed, 2);
  assert.equal(store.db.settings.fairQuota.autoAnlasFallback, false);
  assert.equal(
    store.sqlite
      .prepare("SELECT count(*) AS n FROM domain_events WHERE event_type='PolicyChanged'")
      .get().n,
    2,
  );
});
test('an eventless keyed write rejects instead of claiming a success without a durable receipt', async (t) => {
  const store = await fixture(t);
  await store.enableEventAuthority();
  await assert.rejects(
    store.update(
      (db) => {
        db.users[0].memberAuth = { version: 'must-not-persist' };
        return { ignored: 1 };
      },
      {
        collections: ['users'],
        domainCommand: { commandId: 'ignored-command', fingerprint: 'ignored-intent' },
      },
    ),
    (error) => error.statusCode === 409,
  );
  assert.equal(readCommandReceipt(store.sqlite, 'ignored-command'), null);
  assert.equal(store.db.users[0].memberAuth, undefined);
  assert.equal(
    JSON.parse(store.sqlite.prepare('SELECT data FROM users WHERE id=?').get('a').data).memberAuth,
    undefined,
  );
});
test('the final duplicate defense rolls back protected writes and discards a prepared candidate', async (t) => {
  const store = await fixture(t);
  await store.enableEventAuthority();
  const other = new JsonStore(store.dataDir);
  await other.init();
  try {
    const metadata = { commandId: 'raced-policy-A', fingerprint: domainDigest({ enabled: true }) };
    const result = await store.update(
      async (db) => {
        assert.equal(store.sqlite.inTransaction, false);
        await other.update(
          (draft) => {
            changeFallback(draft, true);
            return { executed: 'original' };
          },
          { collections: ['settings'], domainCommand: metadata },
        );
        await other.update(
          (draft) => {
            changeFallback(draft, false, now + 1);
            return { enabled: false };
          },
          { collections: ['settings'] },
        );
        changeFallback(db, true);
        db.users[0].memberAuth = { version: 'discarded-candidate' };
        return { executed: 'candidate' };
      },
      {
        collections: ['settings', 'users'],
        domainCommand: metadata,
        memberSession: {
          token_hash: 'b'.repeat(64),
          user_id: 'a',
          auth_version: 'discarded',
          created_at: now,
          expires_at: now + 60000,
          owner_binding: null,
        },
      },
    );
    assert.deepEqual(result, { executed: 'original' });
    assert.equal(store.db.settings.fairQuota.autoAnlasFallback, false);
    assert.equal(store.db.users[0].memberAuth, undefined);
    assert.equal(store.sqlite.prepare('SELECT count(*) AS n FROM member_sessions').get().n, 0);
    assert.equal(readProjectedFairQuota(store.sqlite).autoAnlasFallback, false);
    assert.equal(
      store.sqlite
        .prepare("SELECT count(*) AS n FROM domain_events WHERE event_type='PolicyChanged'")
        .get().n,
      2,
    );
  } finally {
    await other.close();
  }
});
test('a failed SQL commit rolls back receipt, projections and protected writes before reloading the cache', async (t) => {
  const store = await fixture(t);
  await store.enableEventAuthority();
  const beforeSettings = store.sqlite
    .prepare("SELECT value FROM app_settings WHERE key='settings'")
    .get().value;
  const beforeUser = store.sqlite.prepare('SELECT data FROM users WHERE id=?').get('a').data;
  const beforeCount = store.sqlite.prepare('SELECT count(*) AS n FROM domain_events').get().n;
  store.sqlite.exec(
    "CREATE TEMP TRIGGER fail_command_projection BEFORE UPDATE ON projection_versions BEGIN SELECT RAISE(ABORT,'injected command projection failure'); END",
  );
  const options = {
    collections: ['settings', 'users'],
    immediate: true,
    domainCommand: { commandId: 'failed-policy', fingerprint: domainDigest({ enabled: true }) },
    memberSession: {
      token_hash: 'a'.repeat(64),
      user_id: 'a',
      auth_version: 'protected',
      created_at: now,
      expires_at: now + 60000,
      owner_binding: null,
    },
  };
  const mutate = (db) => {
    changeFallback(db, true);
    db.users[0].memberAuth = { version: 'candidate-private-auth' };
    return { enabled: true };
  };
  await assert.rejects(store.update(mutate, options), /injected command projection failure/);
  assert.equal(readCommandReceipt(store.sqlite, 'failed-policy'), null);
  assert.equal(
    store.sqlite.prepare('SELECT count(*) AS n FROM domain_events').get().n,
    beforeCount,
  );
  assert.equal(
    store.sqlite.prepare("SELECT value FROM app_settings WHERE key='settings'").get().value,
    beforeSettings,
  );
  assert.equal(store.sqlite.prepare('SELECT data FROM users WHERE id=?').get('a').data, beforeUser);
  assert.equal(store.sqlite.prepare('SELECT count(*) AS n FROM member_sessions').get().n, 0);
  assert.equal(
    store.db.settings.fairQuota.autoAnlasFallback,
    JSON.parse(beforeSettings).fairQuota.autoAnlasFallback,
  );
  assert.equal(store.db.users[0].memberAuth, undefined);
  store.sqlite.exec('DROP TRIGGER fail_command_projection');
  assert.deepEqual(await store.update(mutate, options), { enabled: true });
  assert.equal(readCommandReceipt(store.sqlite, 'failed-policy').receipt.enabled, true);
});
test('cutover verifies the shadow, removes duplicate success triggers, and blocks unrecorded money changes', async (t) => {
  const store = await fixture(t);
  await store.enableEventAuthority();
  assert.equal(eventAuthorityEnabled(store.sqlite), true);
  assert.equal(
    store.sqlite
      .prepare(
        "SELECT count(*) AS n FROM sqlite_schema WHERE type='trigger' AND name LIKE 'free_image_%'",
      )
      .get().n,
    0,
  );
  const before = readProjectedFairQuota(store.sqlite).balances.a.v5Available;
  await assert.rejects(
    store.update(
      (db) => {
        db.settings.fairQuota.balances.a.v5Available = '999';
      },
      { includeSettings: true, collections: [] },
    ),
    /Domain shadow mismatch/,
  );
  assert.equal(store.db.settings.fairQuota.balances.a.v5Available, before);
  const eventCount = store.sqlite.prepare('SELECT count(*) AS n FROM domain_events').get().n;
  await assert.rejects(
    store.update(
      (db) => {
        db.settings.fairQuota.reservations.forged = {
          status: 'refunded',
          userId: 'a',
          accountId: 'opus',
          resource: 'v5',
          amount: '0',
          fixed: '0',
          purchased: '0',
          configVersion: 1,
          reservedAt: now,
          settledAt: now,
        };
      },
      { includeSettings: true, collections: [] },
    ),
    /Unrecorded terminal settlement forged/,
  );
  assert.equal(
    store.sqlite.prepare('SELECT charge FROM fair_quota_settlements WHERE job_id=?').get('forged'),
    undefined,
  );
  assert.equal(store.sqlite.prepare('SELECT count(*) AS n FROM domain_events').get().n, eventCount);
  const mapping = {
    member_id: 'a',
    operation: 'generate',
    key_hash: 'forged',
    fingerprint: 'forged',
    normalization_context: '{}',
    job_id: 'forged',
    request_id: null,
    created_at: at,
  };
  await assert.rejects(
    store.update(() => true, { collections: [], generationIdempotency: mapping }),
    /Unrecorded generation idempotency/,
  );
  assert.equal(
    store.sqlite
      .prepare('SELECT job_id FROM generation_idempotency WHERE key_hash=?')
      .get('forged'),
    undefined,
  );
  assert.equal(store.sqlite.prepare('SELECT count(*) AS n FROM domain_events').get().n, eventCount);
  store.sqlite.transaction(() => verifyEventProjections(store.sqlite))();
  await assert.rejects(store.write(store.db), /event authority/);
  // 受保护认证与可丢的显示字段不冒充领域事实。
  await store.update(
    (db) => {
      db.users[0].memberAuth = { version: 'changed-secret-auth-version' };
    },
    { dirtyRows: { users: ['a'] } },
  );
  assert.equal(
    store.db.users.find((user) => user.id === 'a').memberAuth.version,
    'changed-secret-auth-version',
  );
});
test('authority writes admission, exact reservation and terminal state from the same event commit', async (t) => {
  const store = await fixture(t);
  await store.enableEventAuthority();
  await store.update(
    (db) => {
      db.jobs.push({
        id: 'j',
        memberId: 'a',
        userToken: 'secret-a',
        status: 'queued',
        sampleIndex: 0,
        createdAt: at,
        queueSeq: 1,
        request: { model: 'nai-diffusion-5-full', width: 1024, height: 1024, steps: 23 },
      });
      recordDomainEvent(db, {
        eventType: 'RequestAccepted',
        streamId: 'request:legacy-j',
        occurredAt: at,
        payload: {
          requestId: null,
          memberId: 'a',
          createdAt: at,
          source: 'test',
          jobs: [{ id: 'j', status: 'queued', queueSeq: 1, summary: db.jobs[0].request }],
        },
      });
    },
    { dirtyRows: { jobs: ['j'] } },
  );
  await store.update(
    (db) => {
      const job = db.jobs.find((job) => job.id === 'j');
      reserveFairQuota(db, db.users[0], job, db.accounts[0], now);
      job.status = 'running';
      job.accountId = 'opus';
    },
    { includeSettings: true, dirtyRows: { jobs: ['j'] } },
  );
  assert.equal(store.db.settings.fairQuota.reservations.j.status, 'reserved');
  await store.update(
    (db) => {
      const job = db.jobs.find((job) => job.id === 'j');
      settleFairQuota(db, job, false, now + 1, { requestStarted: false });
      job.status = 'failed';
      job.completedAt = new Date(now + 1).toISOString();
      job.error = 'synthetic';
      job.errorCode = 'generation_failed';
      job.errorStatus = 500;
      job.errorDetail = 'synthetic protected diagnostic';
      recordGenerationFailure(db, job);
    },
    { includeSettings: true, dirtyRows: { jobs: ['j'] } },
  );
  assert.equal(store.db.settings.fairQuota.reservations.j, undefined);
  assert.equal(
    JSON.parse(
      store.sqlite.prepare('SELECT charge FROM fair_quota_settlements WHERE job_id=?').get('j')
        .charge,
    ).status,
    'refunded',
  );
  store.sqlite.transaction(() => verifyEventProjections(store.sqlite))();
  const count = store.sqlite.prepare('SELECT count(*) AS n FROM generation_diagnostics').get().n;
  await assert.rejects(
    store.update(
      (db) => {
        const job = db.jobs.find((job) => job.id === 'j');
        job.errorDetail = 'rolled-back diagnostic';
        recordGenerationFailure(db, job);
      },
      {
        dirtyRows: { jobs: ['j'] },
        domainCommand: { expectedVersions: { 'request:legacy-j': '0' } },
      },
    ),
    /Stream version conflict/,
  );
  assert.equal(
    store.sqlite.prepare('SELECT count(*) AS n FROM generation_diagnostics').get().n,
    count,
  );
  assert.equal(store.db.jobs[0].errorDetail, 'synthetic protected diagnostic');
  assert.equal(
    store.sqlite
      .prepare('SELECT payload FROM domain_events')
      .all()
      .some((row) => row.payload.includes('synthetic protected diagnostic')),
    false,
  );
  const target = new Database(':memory:');
  try {
    migrateStorage(target);
    copyProtectedFacts(store.sqlite, target);
    replayDomainEvents(store.sqlite, target, { project: applyEventProjection });
    assert.equal(
      JSON.parse(target.prepare('SELECT data FROM jobs WHERE id=?').get('j').data).errorDetail,
      'synthetic protected diagnostic',
    );
  } finally {
    target.close();
  }
});
test('a verified opening keeps accepted waiting jobs through cutover and still rejects unrecovered running attempts', async (t) => {
  const store = await fixture(t),
    legacy = structuredClone(store.db);
  legacy.jobs = [
    {
      id: 'waiting',
      userToken: 'secret-a',
      memberId: 'a',
      status: 'queued',
      createdAt: at,
      queueSeq: 7,
      queueExpiresAt: new Date(now + 60000).toISOString(),
      request: { model: 'nai-diffusion-5-full', width: 1024, height: 1024, steps: 23 },
    },
  ];
  await store.write(legacy);
  await store.enableEventAuthority();
  assert.equal(store.selectItemById('jobs', 'waiting').status, 'queued');
  assert.equal(store.selectItemById('jobs', 'waiting').queueSeq, 7);
  await store.update(
    (db) => {
      const job = db.jobs[0];
      reserveFairQuota(db, db.users[0], job, db.accounts[0], now);
      job.status = 'running';
      job.accountId = 'opus';
    },
    { includeSettings: true, dirtyRows: { jobs: ['waiting'] } },
  );
  await assert.rejects(store.enableEventAuthority(), /Recover running attempts/);
});
test('real single-image admission keeps its parent request without inventing a legacy batch ID', async (t) => {
  const store = await fixture(t);
  await store.enableEventAuthority();
  const admit = createGenerationAdmission({
    store,
    payloadBudget: createGenerationPayloadBudget(() => store.activeGenerationPayloadBytes()),
    artifactAvailable: async () => true,
    cleanupStaleActiveJobs: async () => {},
    now: () => now,
    env: {},
  });
  const result = await admit({
    identity: { token: 'secret-a' },
    source: 'api',
    batch: false,
    noCache: true,
    request: {
      model: 'nai-diffusion-5-full',
      width: 1024,
      height: 1024,
      steps: 23,
      seed: 7,
      sampleCount: 1,
    },
  });
  const job = store.selectItemById('jobs', result.jobIds[0]);
  assert.equal(job.requestId, result.requestId);
  assert.equal(job.batchId, undefined);
  assert.equal(
    readStreamProjection(store.sqlite, `request:${result.requestId}`).projection.jobs[job.id]
      .batchId,
    undefined,
  );
});
test('unchanged related member projections avoid SQL updates and cache refresh while real changes remain checked', async (t) => {
  const store = await fixture(t);
  await store.enableEventAuthority();
  store.sqlite.exec(`CREATE TEMP TABLE member_updates(id TEXT);
    CREATE TEMP TRIGGER count_member_update AFTER UPDATE ON users BEGIN INSERT INTO member_updates(id) VALUES(NEW.id); END;`);
  const members = new Map(store.db.users.map((user) => [user.id, user]));
  const admit = createGenerationAdmission({
    store,
    payloadBudget: createGenerationPayloadBudget(() => store.activeGenerationPayloadBytes()),
    artifactAvailable: async () => true,
    cleanupStaleActiveJobs: async () => {},
    now: () => now,
    env: {},
  });
  const result = await admit({
    identity: { token: 'secret-a' },
    source: 'api',
    batch: false,
    noCache: true,
    request: {
      model: 'nai-diffusion-5-full',
      width: 1024,
      height: 1024,
      steps: 23,
      seed: 17,
      sampleCount: 1,
    },
  });
  const jobId = result.jobIds[0],
    artifact = {
      id: 'prepared',
      imageId: 'prepared',
      attemptId: 'attempt',
      file: 'prepared.webp',
      mimeType: 'image/webp',
      sha256: 'synthetic',
      byteLength: 1,
    };
  await store.update(
    (db) => {
      const { id, ...descriptor } = artifact;
      db.jobs.find((job) => job.id === jobId).resultPending = descriptor;
      recordDomainEvent(db, {
        eventType: 'ArtifactPrepared',
        streamId: `request:${result.requestId}`,
        payload: { jobId, artifact },
      });
    },
    { dirtyRows: { jobs: [jobId] } },
  );
  assert.deepEqual(store.sqlite.prepare('SELECT id FROM member_updates').all(), []);
  for (const user of store.db.users) {
    assert.strictEqual(user, members.get(user.id));
  }
  await assert.rejects(
    store.update(
      (db) => {
        db.users.find((user) => user.id === 'b').enabled = false;
      },
      { dirtyRows: { users: ['b'] } },
    ),
    /Domain shadow mismatch/,
  );
  assert.deepEqual(store.sqlite.prepare('SELECT id FROM member_updates').all(), []);
  await store.update(
    (db) => {
      const user = db.users.find((user) => user.id === 'b');
      user.enabled = false;
      user.updatedAt = at;
      recordDomainEvent(db, {
        eventType: 'MemberEligibilityChanged',
        streamId: 'sharing:default',
        payload: { memberId: 'b', enabled: false, at },
      });
    },
    { dirtyRows: { users: ['b'] } },
  );
  assert.deepEqual(store.sqlite.prepare('SELECT id FROM member_updates').all(), [{ id: 'b' }]);
  assert.equal(store.selectItemById('users', 'b').enabled, false);
  store.sqlite.transaction(() => verifyEventProjections(store.sqlite))();
});
test('real cached admission preserves the recorded result account without a new charge', async (t) => {
  const store = await fixture(t),
    legacy = structuredClone(store.db);
  legacy.images = [
    {
      id: 'cached-image',
      token: 'secret-a',
      memberId: 'a',
      accountId: 'opus',
      file: 'cached.png',
      createdAt: at,
    },
  ];
  await store.write(legacy);
  await store.enableEventAuthority();
  const before = structuredClone(store.db.settings.fairQuota.balances);
  const admit = createGenerationAdmission({
    store,
    payloadBudget: createGenerationPayloadBudget(() => store.activeGenerationPayloadBytes()),
    artifactAvailable: async () => true,
    cleanupStaleActiveJobs: async () => {},
    now: () => now,
    env: {},
  });
  const result = await admit({
    identity: { token: 'secret-a' },
    source: 'api',
    batch: false,
    cached: { imageId: 'cached-image', accountId: 'opus' },
    request: {
      model: 'nai-diffusion-5-full',
      width: 1024,
      height: 1024,
      steps: 23,
      seed: 7,
      sampleCount: 1,
    },
  });
  const job = store.selectItemById('jobs', result.jobIds[0]);
  assert.equal(job.status, 'done');
  assert.equal(job.accountId, 'opus');
  assert.equal(job.imageId, 'cached-image');
  assert.deepEqual(store.db.settings.fairQuota.balances, before);
});
test('retiring a completed child keeps the authoritative child set and immutable input while siblings remain queued', async (t) => {
  const store = await fixture(t);
  await store.enableEventAuthority();
  const admit = createGenerationAdmission({
    store,
    payloadBudget: createGenerationPayloadBudget(() => store.activeGenerationPayloadBytes()),
    artifactAvailable: async () => true,
    cleanupStaleActiveJobs: async () => {},
    now: () => now,
    env: {},
  });
  const result = await admit({
    identity: { memberId: 'a' },
    source: 'native',
    batch: true,
    noCache: true,
    request: {
      model: 'nai-diffusion-5-full',
      width: 1024,
      height: 1024,
      steps: 23,
      seed: 7,
      sampleCount: 2,
    },
  });
  await store.update(
    (db) => {
      const job = db.jobs.find((job) => job.id === result.jobIds[0]);
      job.status = 'done';
      job.completedAt = at;
      recordDomainEvent(db, {
        eventType: 'JobCompleted',
        streamId: `request:${result.requestId}`,
        payload: { jobId: job.id, completedAt: at },
      });
    },
    { dirtyRows: { jobs: [result.jobIds[0]] } },
  );
  const input = store.readGenerationRequest(result.requestId);
  await store.retireJobHistory([result.jobIds[0]]);
  assert.equal(store.readGenerationRequestState(result.requestId, 'a').jobs.length, 1);
  assert.deepEqual(store.readGenerationRequestJobIds(result.requestId, 'a'), result.jobIds);
  assert.deepEqual(store.readGenerationRequest(result.requestId), input);
});
test('cutover installs conservative fixed-point opening balances before the first real command', async (t) => {
  const store = await fixture(t),
    legacy = structuredClone(store.db);
  for (const id of ['a', 'b', 'c', 'd']) {
    Object.assign(legacy.settings.fairQuota.balances[id], {
      anlasFixedAvailable: id === 'd' ? 0 : 1 / 3,
      anlasPurchasedAvailable: 0,
      anlasAvailable: id === 'd' ? 0 : 1 / 3,
    });
  }
  await store.write(legacy);
  await store.enableEventAuthority();
  assert.equal(store.db.settings.fairQuota.balances.a.anlasFixedAvailable, '0.333333333334');
  assert.equal(
    Object.values(store.db.settings.fairQuota.balances).reduce(
      (sum, balance) => sum + parseAmount(balance.anlasFixedAvailable),
      0n,
    ),
    1000000000000n,
  );
  await store.update((db) => syncFairQuota(db, now), { includeSettings: true, collections: [] });
  assert.deepEqual(store.db.settings.fairQuota, readProjectedFairQuota(store.sqlite));
});
test('a failed cutover rolls back installed amounts, removed triggers and the authority marker', async (t) => {
  const store = await fixture(t),
    legacy = structuredClone(store.db);
  for (const id of ['a', 'b', 'c']) {
    Object.assign(legacy.settings.fairQuota.balances[id], {
      anlasFixedAvailable: 1 / 3,
      anlasPurchasedAvailable: 0,
      anlasAvailable: 1 / 3,
    });
  }
  await store.write(legacy);
  importOpeningFacts(store.sqlite);
  const before = store.sqlite
    .prepare("SELECT value FROM app_settings WHERE key='settings'")
    .get().value;
  const prepare = store.sqlite.prepare.bind(store.sqlite);
  store.sqlite.prepare = (sql) => {
    if (sql.startsWith('INSERT INTO app_settings')) {
      throw new Error('injected cutover SQL failure');
    }
    return prepare(sql);
  };
  await assert.rejects(store.enableEventAuthority(), /injected cutover SQL failure/);
  store.sqlite.prepare = prepare;
  assert.equal(prepare("SELECT value FROM app_settings WHERE key='settings'").get().value, before);
  assert.equal(prepare("SELECT value FROM app_meta WHERE key='event_authority'").get(), undefined);
  assert.equal(
    prepare(
      "SELECT count(*) AS n FROM sqlite_schema WHERE type='trigger' AND name LIKE 'free_image_%'",
    ).get().n,
    4,
  );
});
test('pending loss reviews remain due after attempted history cleanup and retire only after explicit closure', async (t) => {
  const store = await fixture(t),
    legacy = structuredClone(store.db),
    job = {
      id: 'review-job',
      memberId: 'a',
      userToken: 'secret-a',
      status: 'running',
      createdAt: at,
      request: { model: 'nai-diffusion-5-full', width: 1024, height: 1024, steps: 23 },
      upstreamStartedAt: at,
    };
  legacy.jobs = [job];
  reserveFairQuota(legacy, legacy.users[0], job, legacy.accounts[0], now);
  settleFairQuota(legacy, job, false, now + 1, { requestStarted: true });
  job.status = 'failed';
  job.completedAt = new Date(now + 1).toISOString();
  await store.write(legacy);
  await store.enableEventAuthority();
  const deadline = store.db.jobs[0].generationReview.deadlineAt;
  assert.equal((await store.readDueGenerationReviews(deadline - 1)).length, 0);
  await assert.rejects(store.retireJobHistory(['review-job']), /unresolved review/);
  assert.equal((await store.clearRequestLogs()).removed, 0);
  assert.equal((await store.readDueGenerationReviews(deadline + 1)).length, 1);
  await store.update((db) => closeGenerationReview(db, db.jobs[0], deadline + 1), {
    dirtyRows: { jobs: ['review-job'] },
  });
  assert.equal((await store.readDueGenerationReviews(deadline + 1)).length, 0);
  assert.deepEqual(await store.retireJobHistory(['review-job']), ['review-job']);
  assert.equal(store.readFairQuotaSettlement('review-job').status, 'refunded');
});
test('explicit cache clearing and automatic cache trim record tombstones and cannot resurrect artifacts on replay', async (t) => {
  const previous = process.env.NO_AUTO_DELETE;
  process.env.NO_AUTO_DELETE = 'false';
  t.after(() => {
    if (previous === undefined) {
      delete process.env.NO_AUTO_DELETE;
    } else {
      process.env.NO_AUTO_DELETE = previous;
    }
  });
  const store = await fixture(t),
    legacy = structuredClone(store.db);
  legacy.jobs = [
    {
      id: 'image-job',
      memberId: 'a',
      userToken: 'secret-a',
      status: 'done',
      createdAt: at,
      imageId: 'one',
      lateResultAvailable: true,
      lateResultImageId: 'one',
    },
  ];
  legacy.images = [
    {
      id: 'one',
      memberId: 'a',
      token: 'secret-a',
      file: 'images/one.png',
      jobId: 'image-job',
      createdAt: at,
    },
    { id: 'two', memberId: 'b', token: 'secret-b', file: 'images/two.png', createdAt: at },
  ];
  await store.write(legacy);
  await store.enableEventAuthority();
  const count = store.sqlite.prepare('SELECT count(*) AS n FROM domain_events').get().n;
  store.sqlite.exec(
    "CREATE TEMP TRIGGER reject_retirement BEFORE DELETE ON images BEGIN SELECT RAISE(ABORT,'synthetic retirement failure'); END",
  );
  await assert.rejects(store.clearCachedImages({ ids: ['one'] }), /synthetic retirement failure/);
  store.sqlite.exec('DROP TRIGGER reject_retirement');
  assert.equal(store.sqlite.prepare('SELECT count(*) AS n FROM domain_events').get().n, count);
  assert.equal(store.db.jobs[0].imageId, 'one');
  assert.equal((await store.clearCachedImages({ ids: ['one'] })).deleted, 1);
  assert.equal(store.db.jobs[0].imageId, '');
  assert.equal(store.db.jobs[0].lateResultAvailable, false);
  const configured = store.db.settings.maxCacheImages;
  assert.equal((await store.trimImageCache(0, { force: true })).length, 1);
  assert.equal(store.db.settings.maxCacheImages, configured);
  const target = new Database(':memory:');
  try {
    migrateStorage(target);
    copyProtectedFacts(store.sqlite, target);
    replayDomainEvents(store.sqlite, target, { project: applyEventProjection });
    assert.equal(target.prepare('SELECT count(*) AS n FROM images').get().n, 0);
    assert.equal(
      JSON.parse(target.prepare('SELECT data FROM jobs WHERE id=?').get('image-job').data).imageId,
      '',
    );
    target.transaction(() => verifyEventProjections(target))();
  } finally {
    target.close();
  }
});
test('automatic history retention records its decision inside the command and keeps the protection flag unchanged', async (t) => {
  const previous = process.env.NO_AUTO_DELETE;
  process.env.NO_AUTO_DELETE = 'true';
  t.after(() => {
    if (previous === undefined) {
      delete process.env.NO_AUTO_DELETE;
    } else {
      process.env.NO_AUTO_DELETE = previous;
    }
  });
  const store = await fixture(t),
    legacy = structuredClone(store.db);
  legacy.jobs = [
    {
      id: 'expired',
      memberId: 'a',
      userToken: 'secret-a',
      status: 'done',
      createdAt: new Date(Date.now() - 8 * 86400000).toISOString(),
    },
  ];
  await store.write(legacy);
  await store.enableEventAuthority();
  await store.update(() => true);
  assert.ok(store.selectItemById('jobs', 'expired'));
  process.env.NO_AUTO_DELETE = 'false';
  await store.update(() => true);
  assert.equal(store.selectItemById('jobs', 'expired'), null);
  assert.equal(
    store.sqlite
      .prepare("SELECT count(*) AS n FROM domain_events WHERE event_type='JobHistoryRetired'")
      .get().n,
    1,
  );
  const target = new Database(':memory:');
  try {
    migrateStorage(target);
    copyProtectedFacts(store.sqlite, target);
    replayDomainEvents(store.sqlite, target, { project: applyEventProjection });
    assert.equal(target.prepare('SELECT count(*) AS n FROM jobs').get().n, 0);
  } finally {
    target.close();
  }
});
