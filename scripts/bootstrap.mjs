import { randomBytes, randomUUID } from 'node:crypto';
import { mkdir, lstat, open, realpath, mkdtemp, link, rm } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const projectRoot = fileURLToPath(new URL('../', import.meta.url));
class BootstrapError extends Error {}
const reject = (message) => {
  throw new BootstrapError(message);
};

function readConfig(env) {
  const adminToken = String(env.ADMIN_TOKEN || '').trim();
  const upstreamToken = String(env.NOVELAI_TOKEN || '').trim();
  if (
    adminToken.length < 32 ||
    /^(123456|change|replace|example|your[-_ ]?admin)/i.test(adminToken)
  ) {
    reject('ADMIN_TOKEN 必须设置为至少 32 位的随机管理员密钥。');
  }
  if (!/^pst-[A-Za-z0-9_-]{20,}$/.test(upstreamToken)) {
    reject('NOVELAI_TOKEN 必须是有效格式的 NovelAI PST。');
  }
  const names =
    env.BOOTSTRAP_USERNAMES === undefined
      ? ['member01', 'member02', 'member03', 'member04']
      : String(env.BOOTSTRAP_USERNAMES)
          .split(',')
          .map((s) => s.trim().toLowerCase());
  if (
    names.length !== 4 ||
    new Set(names).size !== 4 ||
    names.some((name) => !/^[a-z0-9][a-z0-9_.-]{2,39}$/.test(name))
  ) {
    reject('BOOTSTRAP_USERNAMES 需要四个唯一用户名，用英文逗号分隔。');
  }
  return {
    upstreamToken,
    names,
    dataDir: path.resolve(env.DATA_DIR || path.join(projectRoot, 'data')),
  };
}

async function ensureAbsent(filename) {
  try {
    await lstat(filename);
  } catch (error) {
    if (error.code === 'ENOENT') {
      return;
    }
    throw error;
  }
  reject('数据目录已有数据库、初始化记录或凭据，拒绝覆盖；请在首次启动服务之前初始化。');
}

export async function bootstrap({ env = process.env, log = console.log } = {}) {
  const config = readConfig(env),
    protectedNames = [
      'library.sqlite',
      'library.sqlite-wal',
      'library.sqlite-shm',
      'library.sqlite-journal',
      'library.json',
      'library.json.bak',
      'bootstrap-credentials.json',
      '.bootstrap.lock',
    ];
  for (const name of protectedNames) {
    await ensureAbsent(path.join(config.dataDir, name));
  }
  // 预检通过后才加载真实 owner；不创建旧 Store 或启动服务、网络能力。
  const { default: Database } = await import('better-sqlite3'),
    { prepareMemberCredentials } = await import('../server/members/authentication.ts'),
    { RelationalMemberStore } = await import('../server/storage/relational-member-store.ts'),
    { RelationalQuotaStore } = await import('../server/storage/relational-quota-store.ts'),
    { RelationalAccountStore } = await import('../server/storage/relational-account-store.ts'),
    { RelationalConfigurationStore } =
      await import('../server/storage/relational-configuration-store.ts'),
    { initializeRelationalStorage } = await import('../server/storage/relational-storage.ts'),
    { verifyAndInstallRelationalStorage } = await import('../server/storage/relational-ready.ts'),
    { relationalSnapshotSha256 } = await import('../server/storage/relational-private-restore.ts'),
    { initialNormalizationDefaults } =
      await import('../server/generation/normalization-defaults.ts'),
    { domainDigest } = await import('../server/events/domain-events.ts');
  const probe = new Database(':memory:');
  probe.close();
  const at = Date.now(),
    createdAt = new Date(at).toISOString(),
    credentials = config.names.map((username) => ({
      user_id: 'usr_' + randomUUID(),
      username,
      password: randomBytes(24).toString('base64url'),
      api_key: 'STA1N-' + randomBytes(18).toString('base64url'),
    })),
    prepared = [];
  for (const member of credentials) {
    prepared.push(await prepareMemberCredentials(member));
  }
  const oldMask = process.umask(0o077);
  let target, lock, stage, dataDir, credentialPath;
  try {
    await mkdir(config.dataDir, { recursive: true, mode: 0o700 });
    dataDir = await realpath(config.dataDir);
    credentialPath = path.join(dataDir, 'bootstrap-credentials.json');
    try {
      lock = await open(path.join(dataDir, '.bootstrap.lock'), 'wx', 0o600);
    } catch (error) {
      if (error.code === 'EEXIST') {
        reject('已有初始化记录，拒绝重复运行。');
      }
      throw error;
    }
    await lock.writeFile(JSON.stringify({ createdAt }) + '\n', 'utf8');
    await lock.sync();
    await lock.close();
    lock = null;
    for (const name of protectedNames.filter((name) => name !== '.bootstrap.lock')) {
      await ensureAbsent(path.join(dataDir, name));
    }
    // 先持久化同一套可领取凭据；任何失败/重复都不换一套身份覆盖它。
    const output = await open(credentialPath, 'wx', 0o600);
    try {
      await output.writeFile(
        JSON.stringify({ createdAt, accounts: credentials }, null, 2) + '\n',
        'utf8',
      );
      await output.sync();
    } finally {
      await output.close();
    }
    stage = await mkdtemp(path.join(dataDir, '.bootstrap-'));
    const filename = path.join(stage, 'candidate.sqlite'),
      empty = await open(filename, 'wx', 0o600);
    await empty.close();
    target = new Database(filename);
    const historyId = randomUUID();
    initializeRelationalStorage(target, historyId);
    const members = new RelationalMemberStore(target, env),
      quotas = new RelationalQuotaStore(target, env),
      accounts = new RelationalAccountStore(target, quotas, new Map(), env),
      configuration = new RelationalConfigurationStore(target),
      metadata = (id, actorKind = 'system') => ({
        commandId: 'bootstrap:' + id,
        fingerprint: domainDigest({ historyId, id }),
        actorKind,
        actorId: actorKind === 'admin' ? 'bootstrap' : null,
        recordedAt: at,
        correlationId: null,
        causationId: null,
        policyVersion: 'bootstrap-v1',
        pricingVersion: 'not-applicable',
      });
    target
      .transaction(() => {
        credentials.forEach((member, index) =>
          members.registerMember(metadata(member.user_id), {
            memberId: member.user_id,
            displayName: member.username,
            apiKey: member.api_key,
            credentials: prepared[index],
          }),
        );
        const groups = credentials.map((member, i) => ({
            id: 'group_' + (i + 1),
            name: `第 ${i + 1} 组`,
            share: 0.25,
            members: [member.user_id],
            memberWeights: { [member.user_id]: 1 },
          })),
          configured = quotas.configureGroups(
            metadata('groups', 'admin'),
            {
              groups,
              expectedConfigVersion: 1,
            },
            null,
          );
        accounts.prepareAccountCommand(
          metadata('account', 'admin'),
          {
            kind: 'register',
            accountId: 'acct_' + randomUUID(),
            displayName: 'Shared Opus',
            token: config.upstreamToken,
            configVersion: configured.receipt.configVersion,
          },
          null,
        );
        configuration.initializeGatewayConfiguration(metadata('configuration'), {
          normalizationDefaults: initialNormalizationDefaults,
          publicBaseUrl: '',
          originRef: 'bootstrap:built-in-defaults-v1',
        });
      })
      .immediate();
    const sourcePath = path.join(stage, 'selected.sqlite');
    const sourceFile = await open(sourcePath, 'wx', 0o600);
    await sourceFile.close();
    await target.backup(sourcePath);
    const sealedSource = new Database(sourcePath, { fileMustExist: true });
    try {
      sealedSource.pragma('journal_mode=DELETE');
    } finally {
      sealedSource.close();
    }
    const selected = {
      sourcePath,
      sourceSha256: relationalSnapshotSha256(sourcePath),
      historyId,
      finalCommitSeq: target
        .prepare('SELECT CAST(MAX(commit_seq) AS TEXT) AS seq FROM commits')
        .get().seq,
      replicas: { dataDir },
    };
    await verifyAndInstallRelationalStorage(target, selected);
    target.pragma('wal_checkpoint(TRUNCATE)');
    target.pragma('journal_mode=DELETE');
    target.close();
    target = undefined;
    relationalSnapshotSha256(filename);
    const sealed = await open(filename, 'r+');
    try {
      await sealed.sync();
    } finally {
      await sealed.close();
    }
    for (const name of protectedNames.filter(
      (name) => !['.bootstrap.lock', 'bootstrap-credentials.json'].includes(name),
    )) {
      await ensureAbsent(path.join(dataDir, name));
    }
    // 最后才公布完整 ready 文件；link 拒绝同名竞争，已发布后不通过可变路径撤回。
    await link(filename, path.join(dataDir, 'library.sqlite'));
  } finally {
    try {
      if (lock) {
        await lock.close();
      }
    } finally {
      try {
        if (target?.open) {
          target.close();
        }
      } finally {
        process.umask(oldMask);
      }
    }
  }
  try {
    if (path.dirname(stage) !== dataDir || !path.basename(stage).startsWith('.bootstrap-')) {
      throw new Error('Invalid bootstrap staging directory.');
    }
    await rm(stage, { recursive: true, force: true });
  } catch {
    log(`初始化临时目录未能清理，请保留受保护状态并核查：${stage}`);
  }
  log(`初始化完成；四个成员的登录凭据和 API Key 已保存至：${credentialPath}`);
  return { credentialPath, memberCount: 4 };
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  bootstrap().catch((error) => {
    // 不回显异常对象，防止底层数据库或依赖错误将凭据写入控制台。
    console.error(
      error instanceof BootstrapError
        ? error.message
        : '初始化失败；已有文件均保留，请检查数据目录和运行权限，勿覆盖已有数据库。',
    );
    process.exitCode = 1;
  });
}
