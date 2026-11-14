import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile, readdir, stat, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import https from 'node:https';
import net from 'node:net';
import { spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';
import { bootstrap } from '../../scripts/bootstrap.mjs';
import { authenticateMemberApiKey } from '../../server/members/authentication.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalQuotaStore } from '../../server/storage/relational-quota-store.ts';
import { RelationalConfigurationStore } from '../../server/storage/relational-configuration-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import {
  initializeRelationalStorage,
  assertRelationalStorageReady,
} from '../../server/storage/relational-storage.ts';
import {
  relationalSnapshotSha256,
  sameRelationalPrivateState,
} from '../../server/storage/relational-private-restore.ts';
import { verifyAndInstallRelationalStorage } from '../../server/storage/relational-ready.ts';
import { initialNormalizationDefaults } from '../../server/generation/normalization-defaults.ts';

const environment = (dataDir) => ({
  DATA_DIR: dataDir,
  ADMIN_TOKEN: 'test-admin-'.repeat(5),
  NOVELAI_TOKEN: 'pst-' + 'fixture-only-'.repeat(4),
});
async function fresh(t) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'shared-bootstrap-test-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  return dir;
}
function offline(t) {
  let calls = 0;
  const reject = () => {
    calls++;
    throw new Error('bootstrap must stay offline');
  };
  t.mock.method(globalThis, 'fetch', reject);
  for (const owner of [http, https]) {
    t.mock.method(owner, 'request', reject);
    t.mock.method(owner, 'get', reject);
  }
  t.mock.method(net, 'connect', reject);
  t.mock.method(net, 'createConnection', reject);
  t.mock.method(net.Server.prototype, 'listen', reject);
  return () => calls;
}
async function candidate(dir) {
  const stage = (await readdir(dir)).find(
    (name) => name.startsWith('.bootstrap-') && name !== '.bootstrap.lock',
  );
  assert.ok(stage, 'failure must retain its private stage for inspection');
  return new Database(path.join(dir, stage, 'candidate.sqlite'));
}

test('真实新空库离线建立四成员/暂停账户/零账本，由唯一installer ready且凭据只交付一次', async (t) => {
  const dir = await fresh(t),
    calls = offline(t),
    logs = [],
    result = await bootstrap({ env: environment(dir), log: (value) => logs.push(value) }),
    saved = JSON.parse(await readFile(result.credentialPath, 'utf8')),
    filename = path.join(dir, 'library.sqlite');
  assert.equal(result.memberCount, 4);
  assert.equal(calls(), 0);
  assert.deepEqual(
    saved.accounts.map((member) => member.username),
    ['member01', 'member02', 'member03', 'member04'],
  );
  assert.equal(new Set(saved.accounts.map((member) => member.password)).size, 4);
  assert.equal(new Set(saved.accounts.map((member) => member.api_key)).size, 4);
  const db = new Database(filename);
  try {
    db.pragma('foreign_keys=ON');
    assertRelationalStorageReady(db);
    assert.equal(
      db
        .prepare(
          "SELECT COUNT(*) n FROM domain_events WHERE event_type LIKE 'Opening%' OR event_type='ImportSourceRegistered'",
        )
        .get().n,
      0,
    );
    assert.equal(db.prepare('SELECT COUNT(*) n FROM members').get().n, 4);
    assert.deepEqual(db.prepare('SELECT enabled,credential_epoch FROM upstream_accounts').get(), {
      enabled: 0,
      credential_epoch: 1,
    });
    const quota = db
      .prepare(
        'SELECT status,observed_at,quota_tier,observed_v5,observed_anlas_fixed,observed_anlas_purchased,baseline_event_id FROM account_quota_state',
      )
      .get();
    assert.deepEqual(quota, {
      status: 'inactive_opus',
      observed_at: null,
      quota_tier: null,
      observed_v5: null,
      observed_anlas_fixed: null,
      observed_anlas_purchased: null,
      baseline_event_id: null,
    });
    assert.equal(
      db
        .prepare(
          "SELECT COUNT(*) n FROM wallets WHERE v5_available!='0' OR anlas_fixed_available!='0' OR anlas_purchased_available!='0'",
        )
        .get().n,
      0,
    );
    assert.equal(db.prepare('SELECT COUNT(*) n FROM wallets').get().n, 4);
    assert.deepEqual(
      db
        .prepare(
          'SELECT v5_available,anlas_fixed_available,anlas_purchased_available FROM operating_state',
        )
        .get(),
      { v5_available: '0', anlas_fixed_available: '0', anlas_purchased_available: '0' },
    );
    assert.equal(db.prepare('SELECT COUNT(*) n FROM generation_jobs').get().n, 0);
    assert.equal(db.prepare('SELECT COUNT(*) n FROM artifacts').get().n, 0);
    const config = new RelationalConfigurationStore(db).readCurrent(),
      policy = new RelationalQuotaStore(db, {}).readQuotaConfiguration();
    assert.deepEqual(config.normalizationDefaults, initialNormalizationDefaults);
    assert.equal(config.publicBaseUrl, '');
    assert.equal(config.originRef, 'bootstrap:built-in-defaults-v1');
    assert.equal(policy.configVersion, 3);
    assert.equal(policy.autoAnlasFallback, false);
    assert.deepEqual(
      policy.groups.map((group) => [group.share, group.members.length]),
      [
        [0.25, 1],
        [0.25, 1],
        [0.25, 1],
        [0.25, 1],
      ],
    );
    const members = new RelationalMemberStore(db, {});
    for (const member of saved.accounts) {
      assert.match(member.api_key, /^STA1N-/);
      assert.ok(member.password.length >= 24);
      assert.equal(
        (await members.loginMemberPassword(member.username, member.password)).id,
        member.user_id,
      );
      assert.equal((await authenticateMemberApiKey(members, member.api_key)).id, member.user_id);
      assert.equal(members.readManagementMemberProfile(member.user_id).note, member.username);
      assert.equal(
        policy.groups.find((group) => group.members.includes(member.user_id)).memberWeights[
          member.user_id
        ],
        1,
      );
    }
    const publicFacts = JSON.stringify(db.prepare('SELECT payload FROM domain_events').all());
    for (const secret of [
      ...saved.accounts.flatMap((member) => [member.password, member.api_key]),
      environment(dir).NOVELAI_TOKEN,
      initialNormalizationDefaults.defaultArtist,
      initialNormalizationDefaults.defaultNegative,
    ]) {
      assert.ok(!publicFacts.includes(secret));
      assert.ok(!logs.join('').includes(secret));
    }
  } finally {
    db.close();
  }
  assert.ok(!JSON.stringify(saved).includes(environment(dir).ADMIN_TOKEN));
  assert.ok(!JSON.stringify(saved).includes(environment(dir).NOVELAI_TOKEN));
  if (process.platform !== 'win32') {
    assert.equal((await stat(result.credentialPath)).mode & 0o777, 0o600);
    assert.equal((await stat(filename)).mode & 0o777, 0o600);
  }
  assert.deepEqual((await readdir(dir)).sort(), [
    '.bootstrap.lock',
    'bootstrap-credentials.json',
    'library.sqlite',
  ]);
  const original = await readFile(filename),
    originalCredentials = await readFile(result.credentialPath);
  await assert.rejects(bootstrap({ env: environment(dir), log() {} }), /拒绝覆盖/);
  assert.deepEqual(await readFile(filename), original);
  assert.deepEqual(await readFile(result.credentialPath), originalCredentials);
  const reopened = new Database(filename);
  try {
    reopened.pragma('foreign_keys=ON');
    assertRelationalStorageReady(reopened);
  } finally {
    reopened.close();
  }
});

test('完整公共回放认证为空，匹配同cut选定私有及保护profile才可再次合法ready/重开', async (t) => {
  const dir = await fresh(t),
    calls = offline(t),
    result = await bootstrap({ env: environment(dir), log() {} }),
    credentials = JSON.parse(await readFile(result.credentialPath, 'utf8')).accounts,
    sourcePath = path.join(dir, 'library.sqlite'),
    sourceSha256 = relationalSnapshotSha256(sourcePath),
    source = new Database(sourcePath, { readonly: true }),
    targetPath = path.join(dir, 'replayed.sqlite'),
    target = new Database(targetPath);
  try {
    source.pragma('foreign_keys=ON');
    const historyId = source.prepare('SELECT history_id id FROM projection_state').get().id,
      cut = source.prepare('SELECT CAST(MAX(commit_seq) AS TEXT) seq FROM commits').get().seq;
    initializeRelationalStorage(target, historyId);
    const replayer = new RelationalCommitReplayer(target),
      inputs = new RelationalInputStore(source);
    for (const { seq } of source
      .prepare('SELECT CAST(commit_seq AS TEXT) seq FROM commits ORDER BY commit_seq')
      .all()) {
      target.transaction(() => replayer.replayCommit(readRelationalCommit(source, seq), inputs))();
    }
    assert.equal(target.prepare('SELECT COUNT(*) n FROM api_keys').get().n, 0);
    assert.equal(target.prepare('SELECT COUNT(*) n FROM password_credentials').get().n, 0);
    assert.equal(target.prepare('SELECT COUNT(*) n FROM upstream_credentials').get().n, 0);
    assert.equal(
      target.prepare('SELECT COUNT(*) n FROM members WHERE note_json IS NOT NULL').get().n,
      0,
    );
    assert.equal(
      target.prepare('SELECT COUNT(*) n FROM members WHERE auth_epoch IS NOT NULL').get().n,
      0,
    );
    assert.throws(() => assertRelationalStorageReady(target), /incomplete/);
    const before = target.serialize();
    await assert.rejects(
      verifyAndInstallRelationalStorage(target, {
        sourcePath,
        sourceSha256,
        historyId,
        finalCommitSeq: String(BigInt(cut) - 1n),
        replicas: { dataDir: dir },
      }),
      /cut/,
    );
    assert.deepEqual(target.serialize(), before);
    await verifyAndInstallRelationalStorage(target, {
      sourcePath,
      sourceSha256,
      historyId,
      finalCommitSeq: cut,
      replicas: { dataDir: dir },
    });
    assertRelationalStorageReady(target);
    assert.equal(sameRelationalPrivateState(source, target), true);
    assert.deepEqual(
      new RelationalConfigurationStore(target).readCurrent(),
      new RelationalConfigurationStore(source).readCurrent(),
    );
    assert.equal(
      (
        await new RelationalMemberStore(target, {}).loginMemberPassword(
          credentials[0].username,
          credentials[0].password,
        )
      ).id,
      credentials[0].user_id,
    );
    assert.equal(relationalSnapshotSha256(sourcePath), sourceSha256);
  } finally {
    target.close();
    source.close();
  }
  const reopened = new Database(targetPath);
  try {
    reopened.pragma('foreign_keys=ON');
    assertRelationalStorageReady(reopened);
  } finally {
    reopened.close();
  }
  assert.equal(calls(), 0);
});

test('所有旧/新数据和sidecar标记都在依赖加载前拒覆盖，非法配置不留身份', async (t) => {
  for (const name of [
    'library.sqlite',
    'library.sqlite-wal',
    'library.sqlite-shm',
    'library.sqlite-journal',
    'library.json',
    'library.json.bak',
    'bootstrap-credentials.json',
    '.bootstrap.lock',
  ]) {
    const dir = await fresh(t),
      file = path.join(dir, name);
    await writeFile(file, 'existing marker');
    await assert.rejects(bootstrap({ env: environment(dir), log() {} }), /拒绝覆盖/);
    assert.equal(await readFile(file, 'utf8'), 'existing marker');
    assert.deepEqual(await readdir(dir), [name]);
  }
  const dir = await fresh(t);
  for (const patch of [
    { ADMIN_TOKEN: '' },
    { ADMIN_TOKEN: 'replace-me-'.repeat(5) },
    { NOVELAI_TOKEN: '' },
    { BOOTSTRAP_USERNAMES: 'same,same,other,last' },
    { BOOTSTRAP_USERNAMES: 'one,two,three' },
  ]) {
    await assert.rejects(bootstrap({ env: { ...environment(dir), ...patch }, log() {} }));
    assert.deepEqual(await readdir(dir), []);
  }
  const result = await bootstrap({
    env: { ...environment(dir), BOOTSTRAP_USERNAMES: 'Alpha,Beta,Gamma,Delta' },
    log() {},
  });
  assert.deepEqual(
    JSON.parse(await readFile(result.credentialPath, 'utf8')).accounts.map(
      (member) => member.username,
    ),
    ['alpha', 'beta', 'gamma', 'delta'],
  );
});

test('并发一次初始化恰一成功，最终身份和先存凭据一致', async (t) => {
  const dir = await fresh(t),
    calls = offline(t),
    results = await Promise.allSettled([
      bootstrap({ env: environment(dir), log() {} }),
      bootstrap({ env: environment(dir), log() {} }),
    ]);
  assert.equal(results.filter((result) => result.status === 'fulfilled').length, 1);
  assert.equal(results.filter((result) => result.status === 'rejected').length, 1);
  const saved = JSON.parse(await readFile(path.join(dir, 'bootstrap-credentials.json'), 'utf8')),
    db = new Database(path.join(dir, 'library.sqlite'));
  try {
    assert.deepEqual(
      db
        .prepare('SELECT id FROM members ORDER BY id')
        .all()
        .map((row) => row.id),
      saved.accounts.map((member) => member.user_id).sort(),
    );
    for (const member of saved.accounts) {
      assert.equal(
        db.prepare('SELECT recoverable_key key FROM api_keys WHERE member_id=?').get(member.user_id)
          .key,
        member.api_key,
      );
    }
  } finally {
    db.close();
  }
  assert.equal(calls(), 0);
});

test('第四成员后真实SQL故障即使外层捕获也不发布，初始化事实整体回滚且凭据/锁不替换', async (t) => {
  const dir = await fresh(t),
    original = RelationalMemberStore.prototype.registerMember;
  let count = 0;
  t.mock.method(RelationalMemberStore.prototype, 'registerMember', function (...args) {
    const result = original.apply(this, args);
    if (++count === 4) {
      this.sqlite.prepare('INSERT INTO api_keys SELECT * FROM api_keys LIMIT 1').run();
    }
    return result;
  });
  await assert.rejects(bootstrap({ env: environment(dir), log() {} }), /UNIQUE constraint failed/);
  assert.equal(count, 4);
  const db = await candidate(dir);
  try {
    assert.equal(db.prepare('SELECT status FROM projection_state').get().status, 'building');
    for (const table of [
      'members',
      'commits',
      'api_keys',
      'password_credentials',
      'sharing_policy',
      'gateway_configuration',
    ]) {
      assert.equal(db.prepare('SELECT COUNT(*) n FROM ' + table).get().n, 0);
    }
  } finally {
    db.close();
  }
  assert.equal((await readdir(dir)).includes('library.sqlite'), false);
  const before = await readFile(path.join(dir, 'bootstrap-credentials.json'));
  await assert.rejects(bootstrap({ env: environment(dir), log() {} }), /拒绝覆盖/);
  assert.deepEqual(await readFile(path.join(dir, 'bootstrap-credentials.json')), before);
});

test('真实ready SQL故障回滚为building，不发布半安装库或泄露私有错误', async (t) => {
  const dir = await fresh(t),
    original = Database.prototype.prepare;
  t.mock.method(Database.prototype, 'prepare', function (sql) {
    if (sql.includes("UPDATE projection_state SET status='ready'")) {
      this.exec(
        "CREATE TEMP TRIGGER bootstrap_ready_failure BEFORE UPDATE OF status ON projection_state WHEN NEW.status='ready' BEGIN SELECT RAISE(ABORT,'synthetic-private-ready-error'); END",
      );
    }
    return original.call(this, sql);
  });
  const logs = [];
  await assert.rejects(
    bootstrap({ env: environment(dir), log: (value) => logs.push(value) }),
    /synthetic-private-ready-error/,
  );
  const db = await candidate(dir);
  try {
    assert.equal(db.prepare('SELECT status FROM projection_state').get().status, 'building');
    assert.equal(db.prepare('SELECT COUNT(*) n FROM members').get().n, 4);
  } finally {
    db.close();
  }
  assert.equal((await readdir(dir)).includes('library.sqlite'), false);
  assert.deepEqual(logs, []);
});

test('同一公共cut的另一份私有快照不能覆盖本次先交付的四成员身份', async (t) => {
  const dir = await fresh(t),
    original = Database.prototype.backup;
  t.mock.method(Database.prototype, 'backup', async function (filename, ...args) {
    const result = await original.call(this, filename, ...args);
    if (path.basename(filename) === 'selected.sqlite') {
      const changed = new Database(filename);
      try {
        changed
          .prepare(
            "UPDATE password_credentials SET username='another.selected.identity' WHERE member_id=(SELECT member_id FROM password_credentials LIMIT 1)",
          )
          .run();
      } finally {
        changed.close();
      }
    }
    return result;
  });
  await assert.rejects(
    bootstrap({ env: environment(dir), log() {} }),
    /private authentication differs/,
  );
  const db = await candidate(dir);
  try {
    assert.equal(db.prepare('SELECT status FROM projection_state').get().status, 'building');
    assert.equal(
      db
        .prepare(
          "SELECT COUNT(*) n FROM password_credentials WHERE username='another.selected.identity'",
        )
        .get().n,
      0,
    );
  } finally {
    db.close();
  }
  assert.equal((await readdir(dir)).includes('library.sqlite'), false);
});

test('实际CLI成功只输出凭据路径；未知失败不打印owner异常或秘密', async (t) => {
  const dir = await fresh(t),
    preload = path.join(dir, 'offline-guard.mjs');
  await writeFile(
    preload,
    `
    import http from 'node:http'; import https from 'node:https'; import net from 'node:net';
    import {syncBuiltinESMExports,createRequire} from 'node:module';
    import {pathToFileURL} from 'node:url'; import path from 'node:path';
    const reject=()=>{throw new Error('CLI attempted external IO');};
    globalThis.fetch=reject;
    for(const owner of [http,https]){owner.request=reject;owner.get=reject;}
    net.connect=reject;net.createConnection=reject;net.Server.prototype.listen=reject;
    syncBuiltinESMExports();
    if(process.env.BOOTSTRAP_TEST_FAIL==='1'){
      const Database=createRequire(pathToFileURL(path.resolve('package.json')))('better-sqlite3');
      const prepare=Database.prototype.prepare;
      Database.prototype.prepare=function(sql){
        if(sql.includes('INSERT INTO password_credentials'))throw new Error('synthetic-private-cli-failure');
        return prepare.call(this,sql);
      };
    }
  `,
  );
  const env = { ...process.env, ...environment(dir) },
    run = spawnSync(
      process.execPath,
      ['--import', pathToFileURL(preload).href, 'scripts/bootstrap.mjs'],
      {
        cwd: new URL('../..', import.meta.url),
        env,
        encoding: 'utf8',
      },
    );
  assert.equal(run.status, 0, run.stderr);
  const saved = JSON.parse(await readFile(path.join(dir, 'bootstrap-credentials.json'), 'utf8'));
  for (const secret of [
    ...saved.accounts.flatMap((member) => [member.password, member.api_key]),
    env.ADMIN_TOKEN,
    env.NOVELAI_TOKEN,
  ]) {
    assert.ok(!run.stdout.includes(secret));
    assert.ok(!run.stderr.includes(secret));
  }
  const repeated = spawnSync(process.execPath, ['scripts/bootstrap.mjs'], {
    cwd: new URL('../..', import.meta.url),
    env,
    encoding: 'utf8',
  });
  assert.equal(repeated.status, 1);
  assert.equal(repeated.stdout, '');
  assert.match(repeated.stderr, /拒绝覆盖/);
  const failedDir = await fresh(t),
    failed = spawnSync(
      process.execPath,
      ['--import', pathToFileURL(preload).href, 'scripts/bootstrap.mjs'],
      {
        cwd: new URL('../..', import.meta.url),
        env: { ...env, DATA_DIR: failedDir, BOOTSTRAP_TEST_FAIL: '1' },
        encoding: 'utf8',
      },
    );
  assert.equal(failed.status, 1);
  assert.equal(failed.stdout, '');
  assert.ok(!failed.stderr.includes('synthetic-private-cli-failure'));
  assert.ok(!failed.stderr.includes(env.NOVELAI_TOKEN));
  assert.match(failed.stderr, /初始化失败/);
  assert.equal((await readdir(failedDir)).includes('library.sqlite'), false);
});
