import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createServer, request } from 'node:http';
import { EventEmitter, once } from 'node:events';
import {
  NOW,
  meta,
  openingFacts,
  installReady,
  databaseState,
  fixture,
} from '../helpers/relational-generation.mjs';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import {
  appendRelationalCommit,
  readRelationalCommit,
} from '../../server/events/relational-event-store.ts';
import {
  restoreRelationalPrivateSnapshot,
  relationalSnapshotSha256,
} from '../../server/storage/relational-private-restore.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { createRelationalManagementHttp } from '../../server/members/relational-management-http.ts';
import { assertAdminToken } from '../../server/http/admin-access.ts';
import { authenticateMemberApiKey } from '../../server/members/authentication.ts';

const ids = ['m4', 'm2', 'm3', 'm1'],
  adminToken = 'synthetic-fair-quota-admin';
const command = (id) => ({ ...meta(id, NOW), actorKind: 'admin', actorId: 'local-admin' });

async function setup(
  t,
  {
    accounts = 0,
    onBody,
    disabled = false,
    baseline = false,
    wallets = false,
    quota = {},
    sharingOrder = false,
  } = {},
) {
  const dir = await mkdtemp(path.join(tmpdir(), 'nai-fair-quota-')),
    db = new Database(path.join(dir, 'library.sqlite')),
    env = { OWNER_USER_ID: 'm1' };
  initializeRelationalStorage(db, 'generation-history');
  const members = new RelationalMemberStore(db, env);
  await mkdir(path.join(dir, 'images'));
  t.after(async () => {
    if (db.open) {
      db.close();
    }
    await rm(dir, { recursive: true, force: true });
  });
  for (const memberId of ids) {
    db.transaction(() =>
      members.registerMember(meta(`member-${memberId}`, NOW), {
        memberId,
        displayName: memberId,
        apiKey: `synthetic-key-${memberId}`,
      }),
    )();
  }
  if (accounts || sharingOrder) {
    const originals = openingFacts({
        baseline,
        v5: '10',
        secondV5: '10',
        officialV5: '80',
        officialFixed: '8000',
        seconds: 0,
      }),
      events = [originals[0]];
    for (let i = 0; i < accounts; i++) {
      const accountId = `a${i}`;
      for (const original of originals.filter((event) =>
        [
          'OpeningAccountRecorded',
          'OpeningQuotaRecorded',
          'OpeningOperatingRecorded',
          ...(wallets ? ['OpeningWalletRecorded'] : []),
        ].includes(event.eventType),
      )) {
        const event = structuredClone(original);
        event.eventId = `${original.eventType}-${i}`;
        if (event.eventType === 'OpeningWalletRecorded') {
          event.eventId += `-${event.payload.memberId}`;
        }
        event.accountId = accountId;
        event.streamId = `account:${accountId}`;
        event.payload.accountId = accountId;
        if (event.eventType === 'OpeningAccountRecorded') {
          event.payload.enabled = !disabled;
        }
        if (event.eventType === 'OpeningQuotaRecorded') {
          event.payload.observationEventId = event.eventId;
          event.payload.baselineEventId = baseline ? event.eventId : null;
          Object.assign(event.payload, quota);
        }
        events.push(event);
      }
    }
    if (sharingOrder) {
      events.push(originals.find((event) => event.eventType === 'OpeningGroupRecorded'));
      const template = originals.find((event) => event.eventType === 'OpeningGroupMemberRecorded');
      for (const [memberOrdinal, memberId] of ids.entries()) {
        const event = structuredClone(template);
        event.memberId = memberId;
        Object.assign(event.payload, { memberId, memberOrdinal });
        events.push(event);
      }
      const policy = structuredClone(
        originals.find((event) => event.eventType === 'OpeningPolicyRecorded'),
      );
      policy.payload.boundAccountId = null;
      events.push(policy);
    }
    db.transaction(() =>
      new RelationalCommitReplayer(db).appendCommit({
        ...meta('historical-accounts', NOW),
        expectedVersions: Object.fromEntries(events.map((event) => [event.streamId, '0'])),
        receipt: { opened: true },
        events,
      }),
    )();
  }
  const selected = await installReady({ db, dir }),
    events = new EventEmitter(),
    notices = [];
  events.on('commit', (event) => notices.push(event));
  const quotas = new RelationalQuotaStore(db, env),
    handle = createRelationalManagementHttp({
      sqlite: db,
      events,
      controls: new Map(),
      dataDir: dir,
      env,
      now: () => NOW,
      assertAdmin: (req, url) => assertAdminToken(req, url, env.ADMIN_TOKEN ?? adminToken, env),
    }),
    server = createServer(async (req, res) => {
      if (onBody) {
        const iterate = req[Symbol.asyncIterator].bind(req);
        req[Symbol.asyncIterator] = async function* () {
          for await (const chunk of iterate()) {
            onBody({ db, env, members });
            yield chunk;
          }
        };
      }
      try {
        if (!(await handle(req, res, new URL(req.url, `http://${req.headers.host}`)))) {
          res.writeHead(404);
          res.end();
        }
      } catch (error) {
        res.writeHead(error.statusCode || 500, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: error.message }));
      }
    });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const origin = `http://127.0.0.1:${server.address().port}`;
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  });
  const call = (
    body = { memberIds: ids },
    headers = {},
    route = '/api/admin/fair-quota',
    method = 'POST',
  ) =>
    new Promise((resolve, reject) => {
      const req = request(
        origin + route,
        {
          method,
          headers: {
            origin,
            'content-type': 'application/json',
            'x-admin-token': adminToken,
            ...headers,
          },
        },
        (res) => {
          let text = '';
          res.setEncoding('utf8');
          res.on('data', (chunk) => (text += chunk));
          res.on('end', () =>
            resolve({
              status: res.statusCode,
              headers: res.headers,
              body: text ? JSON.parse(text) : null,
            }),
          );
        },
      );
      req.on('error', reject);
      req.end(method === 'GET' ? undefined : JSON.stringify(body));
    });
  return { db, dir, members, quotas, events, notices, selected, call, env };
}

test('fair-quota真实ready首配0/1/多账户复用原pending/唯一绑定/ambiguous，顺序与钱包完整回放', async (t) => {
  for (const accounts of [0, 1, 2]) {
    await t.test(`${accounts} eligible`, async (t) => {
      const f = await setup(t, { accounts }),
        before = f.db.prepare('SELECT count(*) AS n FROM commits').get().n;
      const response = await f.call(
        {
          memberIds: ids,
          groups: 'ignored',
          enabled: false,
          expectedConfigVersion: 'ignored',
          padding: 'x'.repeat(40000),
        },
        { 'Idempotency-Key': 'ignored invalid key !' },
      );
      assert.equal(response.status, 200, JSON.stringify(response.body));
      let status = 'pending_opus';
      if (accounts === 1) {
        status = 'active';
      } else if (accounts > 1) {
        status = 'ambiguous_opus';
      }
      assert.deepEqual(response.body, { status, members: ids, globalConcurrency: 4 });
      assert.equal(f.notices.length, 1);
      assert.equal(response.headers['set-cookie'], undefined);
      assert.equal(response.headers['cache-control'], 'no-store');
      const policy = f.db
        .prepare('SELECT bound_account_id AS bound,config_version AS version FROM sharing_policy')
        .get();
      assert.deepEqual(policy, {
        bound: accounts === 1 ? 'a0' : null,
        version: accounts === 1 ? 2 : 1,
      });
      const seq = String(f.db.prepare('SELECT MAX(commit_seq) AS seq FROM commits').get().seq),
        stored = readRelationalCommit(f.db, seq);
      assert.equal(stored.events[0].eventType, 'SharingConfigured');
      if (accounts === 1) {
        assert.equal(stored.events[1].eventType, 'AccountBound');
        assert.deepEqual(
          f.db
            .prepare(
              'SELECT v5_available AS v5,anlas_fixed_available AS fixed FROM wallets ORDER BY member_id',
            )
            .all(),
          ids.map(() => ({ v5: '20', fixed: '2000' })),
        );
        assert.equal(f.quotas.readAccountQuota('a0').baselineEventId, 'OpeningQuotaRecorded-0');
      }
      assert.equal(f.db.prepare('SELECT count(*) AS n FROM commits').get().n, before + 1);
      const stable = databaseState(f.db),
        changes = f.db.prepare('SELECT total_changes() AS n').get().n;
      const repeat = await f.call();
      assert.equal(repeat.status, 200);
      assert.equal(f.notices.length, 1);
      assert.deepEqual(repeat.body, response.body);
      assert.equal(databaseState(f.db), stable);
      assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
      assert.equal((await f.call({ memberIds: [...ids].reverse() })).status, 409);
      assert.equal(databaseState(f.db), stable);
      const targetPath = path.join(f.dir, 'public-restored.sqlite'),
        target = new Database(targetPath);
      try {
        initializeRelationalStorage(target, 'generation-history');
        for (const { seq } of f.db
          .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
          .all()) {
          target.transaction(() =>
            new RelationalCommitReplayer(target).replayCommit(
              readRelationalCommit(f.db, seq),
              new RelationalInputStore(f.db),
            ),
          )();
        }
        for (const table of [
          'sharing_policy',
          'group_members',
          'sharing_groups',
          'wallets',
          'account_quota_state',
          'operating_state',
        ]) {
          assert.deepEqual(
            target.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
            f.db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
          );
        }
        assert.equal(target.prepare('SELECT count(*) AS n FROM api_keys').get().n, 0);
        const replayChanges = target.prepare('SELECT total_changes() AS n').get().n;
        target.transaction(() => new RelationalCommitReplayer(target).replayCommit(stored))();
        assert.equal(target.prepare('SELECT total_changes() AS n').get().n, replayChanges);
        assert.equal(
          await authenticateMemberApiKey(new RelationalMemberStore(target), 'synthetic-key-m1'),
          null,
        );
        const sourcePath = path.join(f.dir, 'selected-after-confirmation.sqlite');
        await f.db.backup(sourcePath);
        const source = new Database(sourcePath);
        source.pragma('journal_mode=DELETE');
        source.close();
        const selected = {
          sourcePath,
          sourceSha256: relationalSnapshotSha256(sourcePath),
          historyId: 'generation-history',
          finalCommitSeq: seq,
          replicas: { dataDir: f.dir },
        };
        target.transaction(() =>
          restoreRelationalPrivateSnapshot(target, sourcePath, selected.sourceSha256),
        )();
        await verifyAndInstallRelationalStorage(target, selected);
        assertRelationalStorageReady(target);
        assert.equal(
          (await authenticateMemberApiKey(new RelationalMemberStore(target), 'synthetic-key-m1'))
            .id,
          'm1',
        );
      } finally {
        target.close();
      }
      const reopened = new Database(targetPath);
      try {
        assertRelationalStorageReady(reopened);
        assert.deepEqual(
          new RelationalQuotaStore(reopened).readQuotaConfiguration(),
          f.quotas.readQuotaConfiguration(),
        );
        assert.equal(
          (await authenticateMemberApiKey(new RelationalMemberStore(reopened), 'synthetic-key-m1'))
            .id,
          'm1',
        );
      } finally {
        reopened.close();
      }
    });
  }
});

test('fair-quota已有账本重确认沿原成员顺序，真实预留不被分组空闲规则误拦', async (t) => {
  const f = await fixture(t, { seconds: 0 }),
    quotas = new RelationalQuotaStore(f.db);
  for (const memberId of ['m3', 'm4']) {
    f.db.transaction(() =>
      f.members.registerMember(meta(`register-${memberId}`), {
        memberId,
        displayName: memberId,
        apiKey: `synthetic-key-${memberId}`,
      }),
    )();
  }
  f.db.transaction(() =>
    quotas.configureGroups(
      command('four-members'),
      {
        expectedConfigVersion: 1,
        groups: [
          { id: 'g', members: ids, share: 1 },
          ...['z', 'a', 'b'].map((id) => ({ id, members: [], share: 0 })),
        ],
      },
      null,
    ),
  )();
  f.accept('held-job');
  f.dispatch('held-job-0');
  assert.equal(
    f.db.prepare("SELECT count(*) AS n FROM charges WHERE status='reserved'").get().n,
    1,
  );
  const stable = databaseState(f.db);
  f.db.transaction(() => quotas.configureMemberIds(command('held-reconfirmation'), ids))();
  assert.equal(databaseState(f.db), stable);
  f.db.transaction(() =>
    assert.throws(
      () => quotas.configureMemberIds(command('sorted-is-another-intent'), [...ids].sort()),
      /不能直接更换成员/,
    ),
  )();
  assert.equal(databaseState(f.db), stable);
});

test('Sharing与MemberRemoved原顺序不等于ID排序或组flatMap，自动加入按原位置替换', async (t) => {
  const f = await setup(t);
  assert.equal((await f.call()).status, 200);
  f.db.transaction(() =>
    f.quotas.configureGroups(
      command('ordered-groups'),
      {
        expectedConfigVersion: 1,
        groups: ['z', 'a', 'y', 'b'].map((id, index) => ({
          id,
          share: 0.25,
          members: [ids[index]],
        })),
      },
      null,
    ),
  )();
  const noChange = databaseState(f.db);
  assert.equal((await f.call()).status, 200);
  assert.equal(databaseState(f.db), noChange);
  const historical = await setup(t, { sharingOrder: true });
  historical.db.transaction(() =>
    historical.members.registerMember(meta('new-receiver'), {
      memberId: 'm5',
      displayName: 'm5',
      apiKey: 'synthetic-key-m5',
    }),
  )();
  const result = historical.db.transaction(() =>
      historical.quotas.manageMemberQuota(
        command('remove-auto-join'),
        {
          action: 'delete-member',
          userId: 'm4',
          transferToUserId: 'm5',
          expectedConfigVersion: 1,
        },
        null,
        new Map(),
      ),
    )(),
    removed = result.events.find((event) => event.eventType === 'MemberRemoved'),
    expected = ['m5', 'm2', 'm3', 'm1'];
  assert.deepEqual(removed.payload.memberIds, expected);
  assert.notDeepEqual(
    removed.payload.groups.flatMap((group) => group.members.map((member) => member.memberId)),
    expected,
  );
  assert.notDeepEqual([...expected].sort(), expected);
  assert.equal((await historical.call({ memberIds: expected })).status, 200);
  assert.equal((await historical.call({ memberIds: [...expected].sort() })).status, 409);
  const target = new Database(':memory:');
  try {
    initializeRelationalStorage(target, 'generation-history');
    for (const { seq } of historical.db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all()) {
      target.transaction(() =>
        new RelationalCommitReplayer(target).replayCommit(
          readRelationalCommit(historical.db, seq),
          new RelationalInputStore(historical.db),
        ),
      )();
    }
    target.transaction(() =>
      new RelationalQuotaStore(target).configureMemberIds(command('restored-order'), expected),
    )();
    assert.equal(
      await authenticateMemberApiKey(new RelationalMemberStore(target), 'synthetic-key-m4'),
      null,
    );
  } finally {
    target.close();
  }
});

test('Opening不是额外预算：已有钱包不可重开账，原已采纳资金不因首次共享绑定再贷', async (t) => {
  for (const baseline of [false, true]) {
    await t.test(`baseline ${baseline}`, async (t) => {
      const f = await setup(t, { accounts: 1, wallets: true, baseline }),
        stable = databaseState(f.db),
        original = f.db
          .prepare(
            'SELECT member_id,v5_available,anlas_fixed_available,source_event_id FROM wallets ORDER BY member_id',
          )
          .all(),
        response = await f.call();
      if (!baseline) {
        assert.equal(response.status, 503);
        assert.match(response.body.error, /缺少金融基线/);
        assert.equal(databaseState(f.db), stable);
        assert.deepEqual(f.notices, []);
      } else {
        assert.equal(response.status, 200, JSON.stringify(response.body));
        for (const wallet of original) {
          assert.deepEqual(
            f.db
              .prepare(
                'SELECT member_id,v5_available,anlas_fixed_available,source_event_id FROM wallets WHERE member_id=?',
              )
              .get(wallet.member_id),
            wallet,
          );
        }
        assert.equal(
          f.db
            .prepare("SELECT count(*) AS n FROM domain_events WHERE event_type='AllocationApplied'")
            .get().n,
          0,
        );
        const after = databaseState(f.db);
        assert.equal((await f.call()).status, 200);
        assert.equal(databaseState(f.db), after);
      }
    });
  }
});

test('Opening原观察时点/三池未知保持未知，暂停与旧代次不被HTTP初配启用或默认填满', async (t) => {
  const variants = [
    { quota: { observedAt: null }, status: 'stale', bound: 'a0' },
    { quota: { observedV5: null }, status: 'unknown_balance', bound: 'a0' },
    { quota: { observedAnlasFixed: null }, status: 'unknown_balance', bound: 'a0' },
    { quota: { observedCredentialEpoch: null }, status: 'pending_opus', bound: null },
    { disabled: true, status: 'pending_opus', bound: null },
  ];
  for (const [index, variant] of variants.entries()) {
    await t.test(String(index), async (t) => {
      const f = await setup(t, { accounts: 1, ...variant }),
        response = await f.call();
      assert.equal(response.status, 200, JSON.stringify(response.body));
      assert.equal(response.body.status, variant.status);
      assert.equal(
        f.db.prepare('SELECT bound_account_id AS id FROM sharing_policy').get().id,
        variant.bound,
      );
      assert.equal(
        f.db
          .prepare(
            "SELECT count(*) AS n FROM domain_events WHERE event_type IN ('AllocationApplied','QuotaBaselineAdopted','QuotaObserved','AvailabilityChanged')",
          )
          .get().n,
        0,
      );
      assert.equal(
        f.db
          .prepare(
            "SELECT count(*) AS n FROM wallets WHERE v5_available!='0' OR anlas_fixed_available!='0' OR anlas_purchased_available!='0'",
          )
          .get().n,
        0,
      );
    });
  }
});

test('fair-quota完整有效envelope的缺绑定/错金额/错顺序/错回执在回放前拒绝且可原提交重试', async (t) => {
  const f = await setup(t, { accounts: 1 });
  await f.call();
  const last = String(f.db.prepare('SELECT MAX(commit_seq) AS n FROM commits').get().n),
    stored = readRelationalCommit(f.db, last);
  const mutations = [
    ['missing binding', (command) => command.events.splice(1, 1)],
    [
      'missing sync',
      (command) => {
        command.events = command.events.filter((event) =>
          ['SharingConfigured', 'AccountBound'].includes(event.eventType),
        );
      },
    ],
    [
      'binding order',
      (command) =>
        ([command.events[0], command.events[1]] = [command.events[1], command.events[0]]),
    ],
    [
      'allocation budget',
      (command) =>
        (command.events.find(
          (event) => event.eventType === 'AllocationApplied',
        ).payload.sourceResources.v5 = '81'),
    ],
    ['receipt order', (command) => command.receipt.fairQuotaConfirmation.members.reverse()],
    ['missing receipt', (command) => (command.receipt = { accountId: 'a0' })],
    [
      'Opening role',
      (command) => {
        command.events.find(
          (event) => event.eventType === 'AllocationApplied',
        ).payload.sourceEventId = 'OpeningOperatingRecorded-0';
      },
    ],
    [
      'Opening resource',
      (command) => {
        const allocation = command.events.find((event) => event.eventType === 'AllocationApplied');
        allocation.payload.sourceResources.anlasFixed = allocation.payload.sourceResources.v5;
        allocation.payload.sourceResources.v5 = '0';
        for (const child of allocation.payload.allocations) {
          child.resources.anlasFixed = child.resources.v5;
          child.resources.v5 = '0';
        }
      },
    ],
  ];
  for (const [name, mutate] of mutations) {
    await t.test(name, () => {
      const target = new Database(':memory:');
      let source;
      try {
        initializeRelationalStorage(target, 'generation-history');
        for (const { seq } of f.db
          .prepare(
            'SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE commit_seq<? ORDER BY commit_seq',
          )
          .all(Number(last))) {
          target.transaction(() =>
            new RelationalCommitReplayer(target).replayCommit(
              readRelationalCommit(f.db, seq),
              new RelationalInputStore(f.db),
            ),
          )();
        }
        source = new Database(target.serialize());
        source.pragma('foreign_keys=ON');
        const changed = structuredClone(stored.command);
        mutate(changed);
        changed.expectedVersions = Object.fromEntries(
          [...new Set(changed.events.map((event) => event.streamId))].map((streamId) => [
            streamId,
            source
              .prepare(
                'SELECT CAST(stream_version AS TEXT) AS version FROM stream_heads WHERE stream_id=?',
              )
              .get(streamId)?.version ?? '0',
          ]),
        );
        // T1 生成完整合法 envelope；拒绝必须来自业务关系，不能靠事件截断/header格式。
        source.transaction(() => appendRelationalCommit(source, changed))();
        const bad = readRelationalCommit(source, last),
          stable = databaseState(target);
        target.transaction(() =>
          assert.throws(
            () => new RelationalCommitReplayer(target).replayCommit(bad),
            /公平分配|原政策|共享/,
          ),
        )();
        assert.equal(databaseState(target), stable);
        target.transaction(() => new RelationalCommitReplayer(target).replayCommit(stored))();
        const changes = target.prepare('SELECT total_changes() AS n').get().n;
        target.transaction(() => new RelationalCommitReplayer(target).replayCommit(stored))();
        assert.equal(target.prepare('SELECT total_changes() AS n').get().n, changes);
      } finally {
        source?.close();
        target.close();
      }
    });
  }
});

test('Opening同一已采纳观察在后续完整普通金融提交不能再次开账，缺观察identity也不造预算', async (t) => {
  const missing = await setup(t, { accounts: 1, quota: { observationEventId: null } }),
    unchanged = databaseState(missing.db);
  const response = await missing.call();
  assert.equal(response.status, 409);
  assert.match(response.body.error, /缺少核对观察引用/);
  assert.equal(databaseState(missing.db), unchanged);
  const f = await setup(t, { accounts: 1 });
  await f.call();
  const original = readRelationalCommit(
      f.db,
      String(f.db.prepare('SELECT MAX(commit_seq) AS n FROM commits').get().n),
    ),
    target = new Database(':memory:');
  let source;
  try {
    initializeRelationalStorage(target, 'generation-history');
    for (const { seq } of f.db
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM commits ORDER BY commit_seq')
      .all()) {
      target.transaction(() =>
        new RelationalCommitReplayer(target).replayCommit(
          readRelationalCommit(f.db, seq),
          new RelationalInputStore(f.db),
        ),
      )();
    }
    source = new Database(target.serialize());
    source.pragma('foreign_keys=ON');
    const events = structuredClone(
      original.command.events.filter((event) =>
        ['QuotaBaselineAdopted', 'AllocationApplied'].includes(event.eventType),
      ),
    );
    for (const event of events) {
      event.policyVersion = 'shared-quota-v1';
    }
    const replayed = source.transaction(() =>
      appendRelationalCommit(source, {
        ...meta('second-opening-credit', NOW),
        expectedVersions: Object.fromEntries(
          events.map((event) => [
            event.streamId,
            source
              .prepare(
                'SELECT CAST(stream_version AS TEXT) AS version FROM stream_heads WHERE stream_id=?',
              )
              .get(event.streamId).version,
          ]),
        ),
        receipt: { accountId: 'a0' },
        events,
      }),
    )();
    const stable = databaseState(target);
    target.transaction(() =>
      assert.throws(
        () =>
          new RelationalCommitReplayer(target).replayCommit(
            readRelationalCommit(source, replayed.commitSeq),
          ),
        /同步金融事实|基线不能覆盖/,
      ),
    )();
    assert.equal(databaseState(target), stable);
  } finally {
    source?.close();
    target.close();
  }
});

test('fair-quota原序SQL尾错外层捕获仍整笔回滚且不广播，合法原命令重试零写回执', async (t) => {
  const f = await setup(t, { accounts: 1 }),
    before = databaseState(f.db),
    metadata = command('first-config');
  f.db.exec(
    "CREATE TEMP TRIGGER fair_tail BEFORE INSERT ON wallets WHEN NEW.member_id='m1' BEGIN SELECT RAISE(ABORT,'synthetic-tail'); END",
  );
  f.db.transaction(() =>
    assert.throws(() => f.quotas.configureMemberIds(metadata, ids), /synthetic-tail/),
  )();
  assert.equal(databaseState(f.db), before);
  const failedHttp = await f.call();
  assert.equal(failedHttp.status, 500);
  assert.match(failedHttp.body.error, /synthetic-tail/);
  assert.equal(databaseState(f.db), before);
  assert.deepEqual(f.notices, []);
  f.db.exec('DROP TRIGGER fair_tail');
  const accepted = f.db.transaction(() => f.quotas.configureMemberIds(metadata, ids))(),
    stable = databaseState(f.db),
    changes = f.db.prepare('SELECT total_changes() AS n').get().n;
  const repeated = f.db.transaction(() => f.quotas.configureMemberIds(metadata, ids))();
  assert.equal(repeated.replayed, true);
  assert.deepEqual(repeated.receipt, accepted.receipt);
  assert.equal(databaseState(f.db), stable);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
});

test('Opening成员原序号必须连续唯一；原未知NULL可以恢复读取但不能伪造重确认顺序', async (t) => {
  const f = await fixture(t),
    opening = readRelationalCommit(f.db, '3');
  for (const mode of ['duplicate', 'gap', 'mixed', 'unknown']) {
    await t.test(mode, () => {
      const target = new Database(':memory:');
      let source;
      try {
        initializeRelationalStorage(target, 'generation-history');
        for (const seq of ['1', '2']) {
          target.transaction(() =>
            new RelationalCommitReplayer(target).replayCommit(readRelationalCommit(f.db, seq)),
          )();
        }
        source = new Database(target.serialize());
        source.pragma('foreign_keys=ON');
        const changed = structuredClone(opening.command);
        changed.events.forEach((event, index) => {
          event.eventId = opening.events[index].eventId;
        });
        const members = changed.events.filter(
          (event) => event.eventType === 'OpeningGroupMemberRecorded',
        );
        if (mode === 'duplicate') {
          members[1].payload.memberOrdinal = 0;
        }
        if (mode === 'gap') {
          members[1].payload.memberOrdinal = 2;
        }
        if (mode === 'mixed') {
          members[1].payload.memberOrdinal = null;
        }
        if (mode === 'unknown') {
          for (const member of members) {
            member.payload.memberOrdinal = null;
          }
        }
        source.transaction(() => appendRelationalCommit(source, changed))();
        const stored = readRelationalCommit(source, '3'),
          stable = databaseState(target);
        if (mode !== 'unknown') {
          target.transaction(() =>
            assert.throws(
              () => new RelationalCommitReplayer(target).replayCommit(stored),
              /从零连续且唯一/,
            ),
          )();
          assert.equal(databaseState(target), stable);
          target.transaction(() => new RelationalCommitReplayer(target).replayCommit(opening))();
        } else {
          target.transaction(() => new RelationalCommitReplayer(target).replayCommit(stored))();
          const quotas = new RelationalQuotaStore(target);
          assert.equal(quotas.readQuotaConfiguration().groups[0].id, 'g');
          const unchanged = databaseState(target);
          target.transaction(() =>
            assert.throws(() => quotas.configureMemberIds(command('unknown-order'), ids), {
              statusCode: 503,
            }),
          )();
          assert.equal(databaseState(target), unchanged);
        }
      } finally {
        source?.close();
        target.close();
      }
    });
  }
});

test('fair-quota正文后重核ready/admin/同源且不接受owner别名，2MiB不扩大其它管理入口', async (t) => {
  for (const revoke of ['ready', 'origin', 'admin']) {
    await t.test(revoke, async (t) => {
      let fired = false;
      const f = await setup(t, {
        onBody({ db, env }) {
          if (fired) {
            return;
          }
          fired = true;
          if (revoke === 'ready') {
            db.prepare("UPDATE projection_state SET status='building'").run();
          }
          if (revoke === 'admin') {
            env.ADMIN_TOKEN = 'replacement';
          }
        },
      });
      const response = await f.call(
        undefined,
        revoke === 'origin' ? { origin: 'https://foreign.invalid' } : {},
      );
      const statuses = { ready: 500, admin: 401, origin: 404 };
      assert.equal(response.status, statuses[revoke]);
      assert.equal(f.db.prepare('SELECT count(*) AS n FROM sharing_policy').get().n, 0);
      assert.deepEqual(f.notices, []);
    });
  }
  const f = await setup(t);
  assert.equal((await f.call({}, {}, '/api/member/owner/fair-quota')).status, 404);
  assert.equal((await f.call({}, {}, '/api/admin/fair-quota', 'GET')).status, 404);
  assert.equal((await f.call({})).status, 400);
  assert.equal((await f.call({ memberIds: null })).status, 400);
  assert.equal(
    (await f.call({ memberIds: ids, padding: 'x'.repeat(2 * 1024 * 1024) })).status,
    413,
  );
  assert.equal(
    (
      await f.call(
        { action: 'ignored', padding: 'x'.repeat(40000) },
        {},
        '/api/admin/member-management',
      )
    ).status,
    413,
  );
  assert.equal((await f.call({ memberIds: ['m1', 'm2', 'm3', 'unknown'] })).status, 400);
  assert.equal((await f.call({ memberIds: ['m1', 'm1', 'm3', 'm4'] })).status, 400);
  assert.equal((await authenticateMemberApiKey(f.members, 'synthetic-key-m1')).id, 'm1');
  assertRelationalStorageReady(f.db);
});
