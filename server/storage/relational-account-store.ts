import type Database from 'better-sqlite3';
import { randomUUID } from 'node:crypto';
import { canonicalJson, domainDigest, type Json } from '../events/domain-events.ts';
import {
  appendRelationalCommit,
  readRelationalReceipt,
  replayRelationalCommit,
  type RelationalCommitResult,
  type StoredRelationalCommit,
} from '../events/relational-event-store.ts';
import type {
  RelationalCommand,
  RelationalEvent,
  RelationalEventInput,
  RelationalFact,
} from '../events/relational-events.ts';
import { parseRelationalFact } from '../events/relational-events.ts';
import { relationalFactStream } from '../events/relational-fact-contracts.ts';
import { assertCurrentOwnerCommand, type MemberUser } from '../members/authentication.ts';
import {
  assertOwnerAccountPst,
  assertOwnerAccountCredentialChangeAllowed,
  assertOwnerAccountInitialBinding,
  ownerAccountConfiguration,
  selectSharedAccount,
  maskToken,
  maskProxyUrl,
  accountTierText,
  normalizeAccountProxyUrl,
} from '../members/owner-operations.ts';
import { physicalAccountLoad } from '../generation/queue-dispatch.ts';
import { RelationalQuotaStore, relationalQuotaFacts } from './relational-quota-store.ts';
import type { QuotaResult } from '../quota/account-refresh.ts';
import {
  assertRelationalStorageFormat,
  assertRelationalStorageReady,
} from './relational-storage.ts';
import {
  relationalAccountConnectionSource,
  assertRelationalAccountRegistrationAbsent,
} from './relational-account-connection.ts';
import { quotaNumber } from '../quota/eligibility.ts';

export type AccountCommandMetadata = Omit<
  RelationalCommand,
  'events' | 'expectedVersions' | 'receipt'
> & {
  policyVersion: string;
  pricingVersion: string;
};
export type OwnerAccountMutation =
  | {
      kind: 'register';
      accountId: string;
      displayName: string;
      token: string;
      configVersion: number;
      weight?: number;
      proxyUrl?: string;
      bind?: boolean;
      enabled?: boolean;
    }
  | { kind: 'pause'; accountId: string }
  | { kind: 'token'; accountId: string; token: string }
  | { kind: 'enable'; accountId: string }
  | { kind: 'observe'; accountId: string }
  | { kind: 'configure'; accountId: string; changes: AdminAccountChanges };
export interface AdminAccountChanges {
  displayName?: string;
  weight?: number;
  proxyUrl?: string;
  token?: string;
  enabled?: boolean;
}
type AccountConfiguration = {
  displayName: string;
  weight: number | null;
  proxyUrl: string | null;
  connectionChanged: boolean;
  eventId: string;
};
type AccountIntent =
  | { kind: 'bulk'; ids: string[]; weight?: number; enabled?: false }
  | {
      kind: 'batch';
      ids: string[];
      mode: 'observe' | 'enable';
      weight?: number;
      applied: boolean;
      outcomes: {
        freshObservationEventId: string | null;
        availability: { enabled: boolean; reason: string } | null;
      }[];
    }
  | {
      kind: 'register';
      accountId: string;
      displayName: string;
      weight: number;
      bind: boolean;
      verified?: true;
    }
  | {
      kind: 'mutation';
      accountId: string;
      displayName?: string;
      weight?: number;
      profile: boolean;
      connectionChanged: boolean;
      credentialChanged: boolean;
      profileAt?: number;
      observe: boolean;
      availability: { enabled: boolean; reason: string } | null;
    };
export type AccountEnableTicket = ReturnType<RelationalQuotaStore['beginQuotaObservation']> & {
  availabilityEventId: string;
  configVersion: number;
  authEpoch: string | null;
  commandId: string;
  fingerprint: string;
  configuration?: AccountConfiguration;
  registration?: { displayName: string; weight: number };
};
export type AccountEnablePreparation = Omit<AccountEnableTicket, 'sequence' | 'startedAt'>;
export type AdminAccountBatch = { ids: string[]; mode: 'observe' | 'enable'; weight?: number };
export type AccountBatchPreparation = {
  sources: (AccountEnablePreparation & { freshObservationEventId: string | null })[];
};
type AccountObservationStarted =
  { result: RelationalCommitResult } | { ticket: AccountEnableTicket };
type AccountObservationStart =
  { result: RelationalCommitResult } | { start: () => AccountObservationStarted };

type Account = {
  id: string;
  displayName: string;
  enabled: number;
  epoch: number;
  pst: string | null;
  proxyUrl: string | null;
  weight: number | null;
};
export interface AccountManagementProfile {
  id: string;
  name: string;
  weight: number | null;
  enabled: boolean;
  credentialEpoch: number;
  createdAt: number | null;
}
function fail(message: string, statusCode = 409): never {
  throw Object.assign(new Error(message), { statusCode });
}

export const relationalAccountFacts = new Set<RelationalEvent['eventType']>([
  'AccountRegistered',
  'AccountBound',
  'CredentialRotated',
  'AccountConfigurationChanged',
]);

// 业务决定复用账户/额度 owner；这里只保存私有凭据、事实和同笔关系投影。
export class RelationalAccountStore {
  private sqlite: Database.Database;
  private quotas: RelationalQuotaStore;
  private env: NodeJS.ProcessEnv;
  private controls: Parameters<typeof physicalAccountLoad>[0];
  // 仅真实 owner 创建的临时准备/实际票据可使用尚无当前身份的例外；不是回放 applied 状态或结果缓存。
  #registrations = new WeakMap<
    AccountEnablePreparation,
    {
      prepared: AccountEnablePreparation;
      context: string;
      ticket?: AccountEnableTicket;
    }
  >();
  constructor(
    sqlite: Database.Database,
    quotas: RelationalQuotaStore,
    controls: Parameters<typeof physicalAccountLoad>[0],
    env: NodeJS.ProcessEnv = process.env,
  ) {
    assertRelationalStorageFormat(sqlite);
    this.sqlite = sqlite;
    this.controls = controls;
    this.env = env;
    this.quotas = quotas;
  }
  private managementAccounts(parameters: [] | [string]): AccountManagementProfile[] {
    // 账户当前来源会随轮换或冷却改变；原创建时间只来自注册，Opening 的导入时间不是创建时间。
    const rows = this.sqlite
      .prepare<string[], Omit<AccountManagementProfile, 'enabled'> & { enabled: number }>(
        `SELECT a.id,a.display_name_json AS name,a.weight,a.enabled,a.credential_epoch AS credentialEpoch,
        (SELECT CASE WHEN e.event_type='AccountRegistered' THEN e.occurred_at ELSE NULL END
          FROM domain_events e WHERE e.stream_id='account:'||a.id
          AND e.event_type IN ('AccountRegistered','OpeningAccountRecorded')
          ORDER BY e.stream_version LIMIT 1) AS createdAt
        FROM upstream_accounts a ${parameters.length ? 'WHERE a.id=?' : ''} ORDER BY a.id`,
      )
      .all(...parameters);
    return rows.map((row) => ({
      ...row,
      name: JSON.parse(row.name) as string,
      enabled: row.enabled === 1,
    }));
  }
  readAccountManagementDirectory() {
    return this.managementAccounts([]);
  }
  readAccountManagementProfile(accountId: string) {
    return this.managementAccounts([accountId])[0] ?? null;
  }
  readAdminAccountProfile(accountId: string, reveal: boolean, actor: MemberUser | null) {
    return this.owned(() => {
      if (actor !== null) {
        fail('只有本地管理员可以读取账户私有配置', 403);
      }
      const account = this.account(accountId),
        quota = this.quotas.readAccountQuotaSnapshot(accountId),
        publicAccount = this.sqlite
          .prepare<[string], { cooldownUntil: number | null; cooldownReason: string | null }>(
            'SELECT cooldown_until AS cooldownUntil,cooldown_reason AS cooldownReason FROM upstream_accounts WHERE id=?',
          )
          .get(accountId)!;
      const iso = (at: number | null) => (at === null ? null : new Date(at).toISOString());
      const fixed = quotaNumber(quota?.quotaFixed),
        purchased = quotaNumber(quota?.quotaPurchased);
      let token = account.pst,
        proxyUrl = account.proxyUrl;
      if (!reveal) {
        if (token !== null) {
          token = maskToken(token);
        }
        if (proxyUrl !== null) {
          proxyUrl = maskProxyUrl(proxyUrl);
        }
      }
      return {
        id: account.id,
        routeId: null,
        name: account.displayName,
        token,
        proxyUrl,
        hasProxy: account.proxyUrl === null ? null : Boolean(account.proxyUrl),
        enabled: account.enabled === 1,
        weight: account.weight,
        inFlight: physicalAccountLoad(this.controls, account.id),
        total: null,
        failures: null,
        quotaPoints: fixed === null && purchased === null ? null : (fixed ?? 0) + (purchased ?? 0),
        quotaFixed: fixed,
        quotaPurchased: purchased,
        quotaTier: quota?.quotaTier ?? null,
        quotaTierText: accountTierText(quota?.quotaTier),
        trialRemainingImages: null,
        trialUsedImages: null,
        trialEligible: null,
        trialStatusKnown: false,
        v5UsagePercent: quota?.v5UsagePercent ?? null,
        v5UsageIsNegative: quota?.v5UsageIsNegative ?? null,
        v5UsageTimeUntilNextPercent: quota?.v5UsageTimeUntilNextPercent ?? null,
        quotaCheckedAt: quota?.quotaCheckedAt ?? null,
        quotaError: quota?.quotaError ?? null,
        cooldownUntil: iso(publicAccount.cooldownUntil),
        cooldownReason: publicAccount.cooldownReason,
        stats1h: null,
        lastUsedAt: null,
      };
    });
  }
  readSharedAccountManagementProfile() {
    return this.sqlite.transaction(() => {
      const policy = this.sqlite
        .prepare<[], { boundAccountId: string | null }>(
          'SELECT bound_account_id AS boundAccountId FROM sharing_policy WHERE id=1',
        )
        .get();
      return selectSharedAccount(this.managementAccounts([]), policy?.boundAccountId);
    })();
  }
  private owned<T>(run: () => T): T {
    if (!this.sqlite.inTransaction) {
      throw new Error('Account writes require the owning authenticated command transaction.');
    }
    return this.sqlite.transaction(run)();
  }
  private authorize(metadata: AccountCommandMetadata, member: MemberUser | null) {
    const current =
      member &&
      this.sqlite
        .prepare<[string], { status: string; authEpoch: string | null }>(
          'SELECT status,auth_epoch AS authEpoch FROM members WHERE id=?',
        )
        .get(member.id);
    assertCurrentOwnerCommand(metadata, member, current, this.env);
  }
  private command(metadata: AccountCommandMetadata, input: OwnerAccountMutation) {
    // owner 原 keyed PST 意图区分不同凭据；admin 非 keyed 连接不发布低熵秘密摘要。
    if (input.kind === 'configure') {
      if (!input.changes || typeof input.changes !== 'object' || Array.isArray(input.changes)) {
        fail('账户配置参数无效', 400);
      }
      const { proxyUrl, token, ...publicChanges } = input.changes;
      // 原 admin 私有配置是非 keyed；绑定公开意图，不发布低熵代理密码的可猜测摘要。
      return {
        ...metadata,
        fingerprint: domainDigest({
          command: metadata.fingerprint,
          accountId: input.accountId,
          changes: publicChanges,
          hasProxyInput: proxyUrl !== undefined,
          hasTokenInput: token !== undefined,
        }),
      };
    }
    if (input.kind === 'register') {
      const { proxyUrl, token, ...publicInput } = input;
      const original = input.bind === false ? publicInput : { ...publicInput, token };
      return {
        ...metadata,
        fingerprint: domainDigest({
          command: metadata.fingerprint,
          input: original,
          hasProxyInput: proxyUrl !== undefined,
        }),
      };
    }
    return { ...metadata, fingerprint: domainDigest({ command: metadata.fingerprint, input }) };
  }
  private account(id: string): Account {
    const row = this.sqlite
      .prepare<[string], Account>(
        `SELECT a.id,a.display_name_json AS displayName,a.weight,a.enabled,a.credential_epoch AS epoch,
      c.pst,c.proxy_url AS proxyUrl FROM upstream_accounts a LEFT JOIN upstream_credentials c ON c.account_id=a.id WHERE a.id=?`,
      )
      .get(id);
    if (!row) {
      fail('上游账户不存在', 404);
    }
    return { ...row, displayName: JSON.parse(row.displayName) as string };
  }
  private availabilityEventId(accountId: string) {
    // 同行的冷却投影会更新 source_event_id；可用性身份只来自实际配置事实。
    const event = this.sqlite
      .prepare<[string], { id: string }>(
        `SELECT event_id AS id FROM domain_events WHERE stream_id=?
      AND event_type IN ('AccountRegistered','OpeningAccountRecorded','CredentialRotated','AvailabilityChanged')
      ORDER BY stream_version DESC LIMIT 1`,
      )
      .get(`account:${accountId}`);
    if (!event) {
      fail('账户缺少可用性来源事实');
    }
    return event.id;
  }
  private policy() {
    const row = this.sqlite
      .prepare<[], { accountId: string | null; version: number }>(
        'SELECT bound_account_id AS accountId,config_version AS version FROM sharing_policy WHERE id=1',
      )
      .get();
    if (!row) {
      fail('共享政策尚未初始化', 503);
    }
    return row;
  }
  private boundAccount(id: string) {
    if (this.policy().accountId !== id) {
      fail('未开放此账户配置操作', 404);
    }
    return this.account(id);
  }
  private event(
    metadata: Pick<AccountCommandMetadata, 'recordedAt' | 'policyVersion' | 'pricingVersion'>,
    fact: RelationalFact,
  ): RelationalEventInput {
    const accountId = 'accountId' in fact.payload ? fact.payload.accountId : null;
    return {
      ...fact,
      streamId:
        relationalFactStream(fact.eventType) === 'sharing'
          ? 'sharing:default'
          : `account:${accountId}`,
      accountId,
      memberId: null,
      occurredAt: metadata.recordedAt,
      policyVersion: metadata.policyVersion,
      pricingVersion: metadata.pricingVersion,
    };
  }
  private commit(
    metadata: AccountCommandMetadata,
    events: RelationalEventInput[],
    error?: { statusCode: number; message: string },
    receipt?: Json,
    intent?: AccountIntent,
  ) {
    const { policyVersion: _policy, pricingVersion: _pricing, ...command } = metadata;
    let enabled =
      this.sqlite
        .prepare<[string | null], { enabled: number }>(
          'SELECT enabled FROM upstream_accounts WHERE id=?',
        )
        .get(events[0].accountId)?.enabled === 1;
    for (const event of events) {
      if (event.eventType === 'AvailabilityChanged') {
        enabled = event.payload.enabled;
      }
    }
    const result = appendRelationalCommit(this.sqlite, {
      ...command,
      events,
      expectedVersions: Object.fromEntries(
        events.map((e) => [
          e.streamId,
          this.sqlite
            .prepare<[string], { v: string }>(
              'SELECT CAST(stream_version AS TEXT) AS v FROM stream_heads WHERE stream_id=?',
            )
            .get(e.streamId)?.v ?? '0',
        ]),
      ),
      receipt: {
        ...((receipt ?? {
          accountId: events[0].accountId,
          enabled,
          ...(error ? { error } : {}),
        }) as object),
        ...(intent ? { accountIntent: intent as unknown as Json } : {}),
      },
    });
    this.assertAccountCommit(result.events, result.receipt);
    this.applyAccountEvents(result.events);
    return result;
  }
  prepareAccountCommand(
    metadata: AccountCommandMetadata,
    input: OwnerAccountMutation,
    member: MemberUser | null,
  ): { result: RelationalCommitResult } | { prepared: AccountEnablePreparation } {
    return this.owned(() => {
      this.authorize(metadata, member);
      let allowed = ['kind', 'accountId'];
      if (input?.kind === 'register') {
        allowed = [
          'kind',
          'accountId',
          'displayName',
          'token',
          'configVersion',
          'weight',
          'proxyUrl',
          'bind',
          'enabled',
        ];
      } else if (input?.kind === 'token') {
        allowed = ['kind', 'accountId', 'token'];
      } else if (input?.kind === 'configure') {
        allowed = ['kind', 'accountId', 'changes'];
      }
      if (
        !input ||
        !['register', 'pause', 'token', 'enable', 'observe', 'configure'].includes(input.kind) ||
        (input.kind !== 'register' && Object.keys(input).length !== allowed.length) ||
        Object.keys(input).some((key) => !allowed.includes(key)) ||
        typeof input.accountId !== 'string' ||
        !input.accountId ||
        input.accountId.includes('\0')
      ) {
        fail('账户配置参数无效', 400);
      }
      if (input.kind === 'configure' && (member !== null || metadata.actorKind !== 'admin')) {
        fail('只有本地管理员可以修改账户档案或代理', 403);
      }
      if (input.kind === 'register' && input.enabled === true) {
        assertRelationalStorageReady(this.sqlite);
      }
      const command = this.command(metadata, input),
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return { result: prior };
      }
      if (input.kind === 'register') {
        assertOwnerAccountPst(input.token);
        if (
          input.proxyUrl !== undefined &&
          normalizeAccountProxyUrl(input.proxyUrl) !== input.proxyUrl
        ) {
          fail('代理来源必须是规范 SOCKS5 地址', 400);
        }
        if (
          typeof input.displayName !== 'string' ||
          !input.displayName ||
          input.displayName.length > 80
        ) {
          fail('账户名称需为 1–80 个字符', 400);
        }
        // 真正发起前先用事实 owner 验证公开注册决定；不能先 GET 再在追加时才发现非法权重。
        parseRelationalFact({
          eventType: 'AccountRegistered',
          payload: {
            accountId: input.accountId,
            displayName: input.displayName,
            weight: input.weight ?? 1,
            enabled: false,
            credentialEpoch: 1,
          },
        });
        const policy = this.policy(),
          binding = input.bind !== false;
        if (input.bind !== undefined && typeof input.bind !== 'boolean') {
          fail('账户注册绑定参数无效', 400);
        }
        if (input.enabled !== undefined && typeof input.enabled !== 'boolean') {
          fail('账户注册启用参数无效', 400);
        }
        if (!binding && (member !== null || metadata.actorKind !== 'admin')) {
          fail('只有本地管理员可以新增未绑定账户', 403);
        }
        if (
          policy.version !== input.configVersion ||
          (binding &&
            (policy.accountId !== null ||
              this.sqlite.prepare('SELECT 1 FROM upstream_accounts LIMIT 1').get()))
        ) {
          fail('仅允许在未绑定共享账户时首次注册');
        }
        if (input.enabled === true) {
          if (binding || member !== null || metadata.actorKind !== 'admin') {
            fail('仅本地管理员可以核验新增未绑定账户', 403);
          }
          const eventId = randomUUID();
          assertRelationalAccountRegistrationAbsent(this.sqlite, input.accountId, eventId);
          const prepared: AccountEnablePreparation = Object.freeze({
            accountId: input.accountId,
            credentialEpoch: 1,
            pst: input.token,
            proxyUrl: input.proxyUrl ?? '',
            connectionSourceEventId: eventId,
            availabilityEventId: eventId,
            configVersion: policy.version,
            authEpoch: null,
            commandId: command.commandId,
            fingerprint: command.fingerprint,
            registration: Object.freeze({
              displayName: input.displayName,
              weight: input.weight ?? 1,
            }),
          });
          const { recordedAt: _at, ...context } = command;
          this.#registrations.set(prepared, { prepared, context: canonicalJson(context) });
          return { prepared };
        }
        const intent: AccountIntent = {
            kind: 'register',
            accountId: input.accountId,
            displayName: input.displayName,
            weight: input.weight ?? 1,
            bind: binding,
          },
          result = this.commit(
            command,
            this.planRegistration(command, intent),
            undefined,
            undefined,
            intent,
          );
        this.sqlite
          .prepare('INSERT INTO upstream_credentials(account_id,pst,proxy_url) VALUES(?,?,?)')
          .run(input.accountId, input.token, input.proxyUrl ?? '');
        return { result };
      }
      const account =
        member === null && metadata.actorKind === 'admin'
          ? this.account(input.accountId)
          : this.boundAccount(input.accountId);
      if (input.kind === 'configure') {
        if (member !== null || metadata.actorKind !== 'admin') {
          fail('只有本地管理员可以修改账户档案或代理', 403);
        }
        const changes = input.changes;
        if (
          !changes ||
          Object.keys(changes).some(
            (key) => !['displayName', 'weight', 'proxyUrl', 'token', 'enabled'].includes(key),
          ) ||
          (changes.displayName !== undefined &&
            (typeof changes.displayName !== 'string' || changes.displayName.length > 80)) ||
          (changes.weight !== undefined &&
            (!Number.isFinite(changes.weight) || changes.weight < 1 || changes.weight > 100)) ||
          (changes.enabled !== undefined && typeof changes.enabled !== 'boolean') ||
          (changes.proxyUrl !== undefined &&
            normalizeAccountProxyUrl(changes.proxyUrl) !== changes.proxyUrl)
        ) {
          fail('账户配置参数无效', 400);
        }
        if (changes.token !== undefined) {
          assertOwnerAccountPst(changes.token);
        }
        if (
          changes.proxyUrl !== undefined &&
          changes.proxyUrl !== account.proxyUrl &&
          !account.pst &&
          changes.token === undefined
        ) {
          fail('当前私有 PST 尚未恢复，请同时提供 PST 与代理配置', 503);
        }
        const configuration: AccountConfiguration = {
          displayName: changes.displayName ?? account.displayName,
          weight: changes.weight ?? account.weight,
          proxyUrl: changes.proxyUrl ?? account.proxyUrl,
          connectionChanged:
            changes.proxyUrl !== undefined && changes.proxyUrl !== account.proxyUrl,
          eventId: randomUUID(),
        };
        const changingToken = changes.token !== undefined && changes.token !== account.pst;
        const configuringProfile =
          changes.displayName !== undefined ||
          changes.weight !== undefined ||
          changes.proxyUrl !== undefined;
        if (changingToken) {
          assertOwnerAccountCredentialChangeAllowed(
            physicalAccountLoad(this.controls, account.id),
            !!this.sqlite
              .prepare(
                "SELECT 1 FROM charges WHERE original_account_id=? AND status='reserved' LIMIT 1",
              )
              .get(account.id),
          );
        }
        if (changes.enabled === true && !changingToken) {
          return {
            prepared: {
              ...this.quotas.prepareQuotaObservation(account.id),
              availabilityEventId: this.availabilityEventId(account.id),
              configVersion: this.policy().version,
              authEpoch: null,
              commandId: command.commandId,
              fingerprint: command.fingerprint,
              ...(configuringProfile ? { configuration } : {}),
            },
          };
        }
        const intent: AccountIntent = {
            kind: 'mutation',
            accountId: account.id,
            profile: configuringProfile || (!changingToken && changes.enabled === undefined),
            ...(changes.displayName !== undefined ? { displayName: changes.displayName } : {}),
            ...(changes.weight !== undefined ? { weight: changes.weight } : {}),
            connectionChanged: configuration.connectionChanged,
            credentialChanged: changingToken,
            observe: false,
            availability:
              changingToken || changes.enabled === false
                ? { enabled: false, reason: changingToken ? 'credential_replaced' : 'owner_paused' }
                : null,
          },
          result = this.commit(
            command,
            this.planMutation(command, intent, undefined, configuration.eventId),
            undefined,
            undefined,
            intent,
          );
        if (configuration.connectionChanged || changingToken) {
          this.writeConnection(account.id, changes.token ?? account.pst, configuration.proxyUrl);
        }
        return { result };
      }
      if (input.kind === 'enable' || input.kind === 'observe') {
        return {
          prepared: {
            ...this.quotas.prepareQuotaObservation(account.id),
            availabilityEventId: this.availabilityEventId(account.id),
            configVersion: this.policy().version,
            authEpoch: member?.memberAuth?.version ?? null,
            commandId: command.commandId,
            fingerprint: command.fingerprint,
          },
        };
      }
      const change =
        input.kind === 'token'
          ? { kind: 'token' as const, token: input.token }
          : { kind: 'pause' as const };
      if (input.kind === 'token') {
        assertOwnerAccountPst(input.token);
      }
      ownerAccountConfiguration(
        {
          accounts: [
            { ...account, enabled: Boolean(account.enabled), token: account.pst ?? undefined },
          ],
          settings: { fairQuota: { boundAccountId: account.id } },
        },
        account.id,
        change,
        metadata.recordedAt,
      );
      if (input.kind === 'token' && input.token !== account.pst) {
        assertOwnerAccountCredentialChangeAllowed(
          physicalAccountLoad(this.controls, account.id),
          Boolean(
            this.sqlite
              .prepare(
                "SELECT 1 FROM charges WHERE original_account_id=? AND status='reserved' LIMIT 1",
              )
              .get(account.id),
          ),
        );
      }
      const intent: AccountIntent = {
          kind: 'mutation',
          accountId: account.id,
          profile: false,
          connectionChanged: false,
          credentialChanged: input.kind === 'token' && input.token !== account.pst,
          observe: false,
          availability: {
            enabled: false,
            reason: input.kind === 'token' ? 'credential_replaced' : 'owner_paused',
          },
        },
        result = this.commit(
          command,
          this.planMutation(command, intent),
          undefined,
          undefined,
          intent,
        );
      if (input.kind === 'token' && input.token !== account.pst) {
        this.sqlite
          .prepare(
            'INSERT INTO upstream_credentials(account_id,pst,proxy_url) VALUES(?,?,?) ON CONFLICT(account_id) DO UPDATE SET pst=excluded.pst,proxy_url=excluded.proxy_url',
          )
          .run(account.id, input.token, account.proxyUrl);
      }
      return { result };
    });
  }
  private batchCommand(metadata: AccountCommandMetadata, input: AdminAccountBatch) {
    if (
      !input ||
      Object.keys(input).some((key) => !['ids', 'mode', 'weight'].includes(key)) ||
      !['observe', 'enable'].includes(input.mode) ||
      !Array.isArray(input.ids) ||
      input.ids.some((id) => typeof id !== 'string' || !id || id.includes('\0')) ||
      (input.mode === 'enable' && !input.ids.length) ||
      (input.weight !== undefined &&
        (input.mode !== 'enable' ||
          !Number.isFinite(input.weight) ||
          input.weight < 1 ||
          input.weight > 100))
    ) {
      fail('批量账户核验参数无效', 400);
    }
    return { ...metadata, fingerprint: domainDigest({ command: metadata.fingerprint, input }) };
  }
  private batchTargets(input: AdminAccountBatch) {
    const ids = new Set(input.ids),
      selected = this.managementAccounts([]).filter((a) => !ids.size || ids.has(a.id));
    if (!selected.length) {
      fail(ids.size ? 'no matching accounts found.' : 'no accounts found.', ids.size ? 404 : 400);
    }
    return selected;
  }
  private authorizeBatch(metadata: AccountCommandMetadata, actor: MemberUser | null) {
    this.authorize(metadata, actor);
    if (actor !== null || metadata.actorKind !== 'admin') {
      fail('只有本地管理员可以批量核验账户', 403);
    }
    assertRelationalStorageReady(this.sqlite);
  }
  prepareAccountBatch(
    metadata: AccountCommandMetadata,
    input: AdminAccountBatch,
    actor: MemberUser | null,
  ): { result: RelationalCommitResult } | { prepared: AccountBatchPreparation } {
    return this.owned(() => {
      this.authorizeBatch(metadata, actor);
      const command = this.batchCommand(metadata, input),
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return { result: prior };
      }
      return {
        prepared: {
          sources: this.batchTargets(input).map((account) => ({
            ...this.quotas.prepareQuotaObservation(account.id),
            availabilityEventId: this.availabilityEventId(account.id),
            configVersion: this.policy().version,
            authEpoch: null,
            commandId: command.commandId,
            fingerprint: command.fingerprint,
            freshObservationEventId:
              input.mode === 'enable' &&
              (input.weight === undefined || input.weight === account.weight)
                ? this.quotas.readAccountEnableProof(account.id, metadata.recordedAt)
                : null,
          })),
        },
      };
    });
  }
  private batchSources(
    command: AccountCommandMetadata,
    input: AdminAccountBatch,
    prepared: AccountBatchPreparation,
  ) {
    const targets = this.batchTargets(input).map((a) => a.id);
    if (
      !prepared ||
      !Array.isArray(prepared.sources) ||
      canonicalJson(targets) !== canonicalJson(prepared.sources.map((s) => s.accountId))
    ) {
      fail('批量账户核验的原目标已变化');
    }
    for (const source of prepared.sources) {
      const { current } = this.enableSource(
        command,
        { kind: input.mode, accountId: source.accountId },
        source,
        null,
      );
      if (
        !current ||
        source.configVersion !== this.policy().version ||
        (source.freshObservationEventId !== null &&
          (input.mode !== 'enable' ||
            this.quotas.readAccountEnableProof(source.accountId, command.recordedAt) !==
              source.freshObservationEventId))
      ) {
        fail('批量账户核验期间原来源或资格已变化');
      }
    }
  }
  private accountObservationStart(run: () => AccountObservationStarted): AccountObservationStart {
    let used = false;
    return {
      start: () => {
        if (used || this.sqlite.inTransaction) {
          fail('实际观察必须在授权事务成功结束后开始，且只能开始一次');
        }
        used = true;
        return run();
      },
    };
  }
  startAccountBatchObservation(
    metadata: AccountCommandMetadata,
    input: AdminAccountBatch,
    prepared: AccountBatchPreparation,
    accountId: string,
    actor: MemberUser | null,
  ): AccountObservationStart {
    return this.owned(() => {
      this.authorizeBatch(metadata, actor);
      const command = this.batchCommand(metadata, input),
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return { result: prior };
      }
      this.batchSources(command, input, prepared);
      const source = prepared.sources.find((s) => s.accountId === accountId);
      if (!source || source.freshObservationEventId !== null) {
        fail('该目标没有需要实际发起的原观察');
      }
      return this.accountObservationStart(() => {
        this.authorizeBatch(metadata, actor);
        const prior = readRelationalReceipt(this.sqlite, command);
        if (prior) {
          return { result: prior };
        }
        this.batchSources(command, input, prepared);
        return {
          ticket: {
            ...source,
            ...this.quotas.beginQuotaObservation(accountId, metadata.recordedAt, source),
          },
        };
      });
    });
  }
  completeAccountBatch(
    metadata: AccountCommandMetadata,
    input: AdminAccountBatch,
    prepared: AccountBatchPreparation,
    observations: { ticket: AccountEnableTicket; result: QuotaResult }[],
    actor: MemberUser | null,
  ) {
    return this.owned(() => {
      this.authorizeBatch(metadata, actor);
      const command = this.batchCommand(metadata, input),
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return prior;
      }
      this.batchSources(command, input, prepared);
      const queried = prepared.sources.filter((s) => s.freshObservationEventId === null);
      if (
        canonicalJson(queried.map((s) => s.accountId)) !==
        canonicalJson(observations.map((o) => o.ticket.accountId))
      ) {
        fail('批量核验缺少原完整有序观察');
      }
      const plan = (enable: boolean) =>
        observations.map(({ ticket, result }, i) => {
          const source = queried[i]!;
          if (
            source.commandId !== ticket.commandId ||
            source.fingerprint !== ticket.fingerprint ||
            source.pst !== ticket.pst ||
            source.proxyUrl !== ticket.proxyUrl ||
            source.connectionSourceEventId !== ticket.connectionSourceEventId ||
            source.credentialEpoch !== ticket.credentialEpoch
          ) {
            fail('批量观察不是原准备的实际来源');
          }
          const events = this.quotas.planQuotaObservation(command, ticket, result, enable);
          if (!events) {
            fail('批量观察已经过期，整笔请求未提交');
          }
          return events;
        });
      let planned = plan(input.mode === 'enable');
      const valid = planned.every((events) =>
          events.some((e) => e.eventType === 'AvailabilityChanged' && e.payload.enabled),
        ),
        applied = input.mode === 'enable' && valid;
      if (input.mode === 'enable' && !applied) {
        // 任一官方核验失败，只保存实际观察及原安全决定，不部分应用整组请求。
        planned = plan(false);
      }
      const intent: Extract<AccountIntent, { kind: 'batch' }> = {
          kind: 'batch',
          ids: prepared.sources.map((s) => s.accountId),
          mode: input.mode,
          ...(input.weight !== undefined ? { weight: input.weight } : {}),
          applied,
          outcomes: prepared.sources.map((source) => {
            const index = queried.findIndex((s) => s.accountId === source.accountId),
              availability = planned[index]?.find((e) => e.eventType === 'AvailabilityChanged');
            return {
              freshObservationEventId: source.freshObservationEventId,
              availability:
                availability?.eventType === 'AvailabilityChanged'
                  ? { enabled: availability.payload.enabled, reason: availability.payload.reason }
                  : null,
            };
          }),
        },
        observed = planned
          .flat()
          .filter(
            (e): e is Extract<RelationalEventInput, { eventType: 'QuotaObserved' }> =>
              e.eventType === 'QuotaObserved',
          ),
        events = [
          ...this.planBatchDecisions(command, intent, observed),
          ...planned
            .flat()
            .filter((e) => !['QuotaObserved', 'AvailabilityChanged'].includes(e.eventType)),
        ],
        counts = {
          checked: observations.length,
          ok: observations.filter((o) => o.result.ok).length,
          failed: observations.filter((o) => !o.result.ok).length,
          ignored: 0,
        },
        failed = input.mode === 'enable' && !applied,
        receipt = {
          accountIds: intent.ids,
          ...counts,
          ...(failed
            ? { error: { statusCode: 409, message: '未核验到全部有效 Opus，批量启用和权重未应用' } }
            : {}),
        };
      if (!events.length) {
        return { ignored: true as const, receipt };
      }
      return this.commit(command, events, undefined, receipt, intent);
    });
  }
  configureAccounts(
    metadata: AccountCommandMetadata,
    input: { ids: string[]; weight?: number; enabled?: false },
    actor: MemberUser | null,
  ) {
    return this.owned(() => {
      this.authorize(metadata, actor);
      if (actor !== null || metadata.actorKind !== 'admin') {
        fail('只有本地管理员可以批量修改账户', 403);
      }
      if (
        !Array.isArray(input.ids) ||
        !input.ids.length ||
        input.ids.some((id) => typeof id !== 'string' || !id) ||
        (input.weight !== undefined &&
          (!Number.isFinite(input.weight) || input.weight < 1 || input.weight > 100)) ||
        (input.enabled !== undefined && input.enabled !== false)
      ) {
        fail('批量账户配置参数无效', 400);
      }
      const command = {
          ...metadata,
          fingerprint: domainDigest({ command: metadata.fingerprint, input }),
        },
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return prior;
      }
      const intent: AccountIntent = {
          kind: 'bulk',
          ids: [...input.ids],
          ...(input.weight !== undefined ? { weight: input.weight } : {}),
          ...(input.enabled === false ? { enabled: false as const } : {}),
        },
        planned = this.planBulkAccounts(command, intent),
        receipt = { accountIds: planned.accountIds, accountIntent: intent as unknown as Json };
      if (!planned.events.length) {
        // 原 admin 非 keyed 的全 no-op 无业务事实，不制造持久化回执；mixed no-op 仍保留完整目标意图。
        return { ignored: true, receipt };
      }
      return this.commit(
        command,
        planned.events,
        undefined,
        { accountIds: planned.accountIds },
        intent,
      );
    });
  }
  private planBulkAccounts(
    metadata: Pick<AccountCommandMetadata, 'recordedAt' | 'policyVersion' | 'pricingVersion'>,
    input: Extract<AccountIntent, { kind: 'bulk' }>,
  ) {
    if (
      !Array.isArray(input.ids) ||
      !input.ids.length ||
      input.ids.some((id) => typeof id !== 'string' || !id) ||
      (input.weight !== undefined &&
        (!Number.isFinite(input.weight) || input.weight < 1 || input.weight > 100)) ||
      (input.enabled !== undefined && input.enabled !== false)
    ) {
      fail('批量账户安全意图无效');
    }
    const ids = new Set(input.ids),
      selected = this.managementAccounts([]).filter((account) => ids.has(account.id)),
      events: RelationalEventInput[] = [];
    if (!selected.length) {
      fail('no matching accounts found.', 404);
    }
    for (const profile of selected) {
      const account = this.account(profile.id);
      if (input.weight !== undefined && input.weight !== account.weight) {
        events.push(
          ...this.configurationEvents(metadata, account, {
            displayName: account.displayName,
            weight: input.weight,
            proxyUrl: account.proxyUrl,
            connectionChanged: false,
            eventId: randomUUID(),
          }),
        );
      }
      if (input.enabled === false && account.enabled !== 0) {
        events.push(
          this.event(metadata, {
            eventType: 'AvailabilityChanged',
            payload: {
              accountId: account.id,
              enabled: false,
              reason: 'owner_paused',
              observationEventId: null,
            },
          }),
        );
      }
    }
    return { accountIds: selected.map((account) => account.id), events };
  }
  private planBatchDecisions(
    metadata: Pick<AccountCommandMetadata, 'recordedAt' | 'policyVersion' | 'pricingVersion'>,
    intent: Extract<AccountIntent, { kind: 'batch' }>,
    observations: Extract<RelationalEventInput, { eventType: 'QuotaObserved' }>[],
  ) {
    if (
      Object.keys(intent).some(
        (key) => !['kind', 'ids', 'mode', 'weight', 'applied', 'outcomes'].includes(key),
      ) ||
      !['observe', 'enable'].includes(intent.mode) ||
      typeof intent.applied !== 'boolean' ||
      !Array.isArray(intent.ids) ||
      !intent.ids.length ||
      !Array.isArray(intent.outcomes) ||
      intent.outcomes.length !== intent.ids.length ||
      (intent.weight !== undefined &&
        (!Number.isFinite(intent.weight) || intent.weight < 1 || intent.weight > 100)) ||
      (intent.mode === 'observe' && (intent.applied || intent.weight !== undefined)) ||
      canonicalJson(this.batchTargets(intent).map((a) => a.id)) !== canonicalJson(intent.ids)
    ) {
      fail('批量账户原安全意图或目标顺序无效');
    }
    const events: RelationalEventInput[] =
      intent.applied && intent.weight !== undefined
        ? this.planBulkAccounts(metadata, { kind: 'bulk', ids: intent.ids, weight: intent.weight })
            .events
        : [];
    let queried = 0,
      allValid = true;
    for (const [index, accountId] of intent.ids.entries()) {
      const outcome = intent.outcomes[index]!,
        account = this.account(accountId);
      if (
        !outcome ||
        Object.keys(outcome).some(
          (key) => !['freshObservationEventId', 'availability'].includes(key),
        ) ||
        (outcome.freshObservationEventId !== null &&
          (typeof outcome.freshObservationEventId !== 'string' ||
            !outcome.freshObservationEventId)) ||
        (outcome.availability !== null &&
          (!outcome.availability ||
            Object.keys(outcome.availability).some((key) => !['enabled', 'reason'].includes(key)) ||
            typeof outcome.availability.enabled !== 'boolean' ||
            typeof outcome.availability.reason !== 'string' ||
            !outcome.availability.reason))
      ) {
        fail('批量账户原观察后果无效');
      }
      if (outcome.freshObservationEventId !== null) {
        if (
          intent.mode !== 'enable' ||
          (intent.weight !== undefined && intent.weight !== account.weight) ||
          outcome.availability !== null ||
          this.quotas.readAccountEnableProof(accountId, metadata.recordedAt) !==
            outcome.freshObservationEventId
        ) {
          fail('批量 no-op 缺少当前真实有效的原观察');
        }
        continue;
      }
      const observed = observations[queried++];
      if (
        !observed?.eventId ||
        observed.payload.accountId !== accountId ||
        observed.accountId !== accountId
      ) {
        fail('批量账户缺少完整有序的原实际观察');
      }
      const valid = this.quotas.quotaObservationCanEnable(observed.payload, metadata.recordedAt);
      allValid &&= valid;
      if (
        (intent.applied &&
          (!valid ||
            outcome.availability?.enabled !== true ||
            outcome.availability.reason !== 'verified_opus')) ||
        (!intent.applied && outcome.availability?.enabled === true) ||
        (outcome.availability?.enabled === false && account.enabled !== 1)
      ) {
        fail('批量账户不能部分应用原请求或伪造安全暂停');
      }
      events.push(
        ...this.planMutation(
          metadata,
          {
            kind: 'mutation',
            accountId,
            profile: false,
            connectionChanged: false,
            credentialChanged: false,
            observe: true,
            availability: outcome.availability,
          },
          observed,
        ),
      );
    }
    if (
      observations.length !== queried ||
      (intent.mode === 'enable' && intent.applied !== allValid)
    ) {
      fail('批量启用必须保留原全体资格及全部观察决定');
    }
    return events;
  }
  private planRegistration(
    metadata: Pick<AccountCommandMetadata, 'recordedAt' | 'policyVersion' | 'pricingVersion'>,
    intent: Extract<AccountIntent, { kind: 'register' }>,
    observed?: Extract<RelationalEventInput, { eventType: 'QuotaObserved' }>,
    registrationEventId?: string,
  ) {
    const events: RelationalEventInput[] = [
      this.event(metadata, {
        eventType: 'AccountRegistered',
        payload: {
          accountId: intent.accountId,
          displayName: intent.displayName,
          weight: intent.weight,
          enabled: false,
          credentialEpoch: 1,
        },
      }),
    ];
    if (registrationEventId) {
      events[0].eventId = registrationEventId;
    }
    if (intent.bind) {
      events.push(
        this.event(metadata, {
          eventType: 'AccountBound',
          payload: { accountId: intent.accountId, configVersion: this.policy().version + 1 },
        }),
      );
    }
    if (intent.verified) {
      if (intent.bind || !observed?.eventId) {
        fail('新账户核验缺少原完整观察');
      }
      events.push(
        {
          ...this.event(metadata, { eventType: 'QuotaObserved', payload: observed.payload }),
          eventId: observed.eventId,
        },
        this.event(metadata, {
          eventType: 'AvailabilityChanged',
          payload: {
            accountId: intent.accountId,
            enabled: true,
            reason: 'verified_opus',
            observationEventId: observed.eventId,
          },
        }),
      );
    }
    return events;
  }
  private planMutation(
    metadata: Pick<AccountCommandMetadata, 'recordedAt' | 'policyVersion' | 'pricingVersion'>,
    intent: Extract<AccountIntent, { kind: 'mutation' }>,
    observed?: Extract<RelationalEventInput, { eventType: 'QuotaObserved' }>,
    configurationEventId?: string,
  ) {
    const account = this.account(intent.accountId),
      events: RelationalEventInput[] = [];
    if (intent.profile) {
      events.push(
        ...this.configurationEvents(
          metadata,
          account,
          {
            displayName: intent.displayName ?? account.displayName,
            weight: intent.weight ?? account.weight,
            proxyUrl: null,
            connectionChanged: intent.connectionChanged,
            eventId: configurationEventId ?? randomUUID(),
          },
          intent.profileAt ?? metadata.recordedAt,
        ),
      );
    }
    if (intent.credentialChanged) {
      events.push(
        this.event(metadata, {
          eventType: 'CredentialRotated',
          payload: {
            accountId: account.id,
            beforeEpoch: account.epoch,
            credentialEpoch: account.epoch + 1,
          },
        }),
      );
    }
    if (intent.observe) {
      if (!observed?.eventId) {
        fail('账户完整修改缺少原观察决定');
      }
      events.push({
        ...this.event(metadata, { eventType: 'QuotaObserved', payload: observed.payload }),
        eventId: observed.eventId,
      });
    }
    if (intent.availability) {
      events.push(
        this.event(metadata, {
          eventType: 'AvailabilityChanged',
          payload: {
            accountId: account.id,
            ...intent.availability,
            observationEventId: intent.availability.enabled ? (observed?.eventId ?? null) : null,
          },
        }),
      );
    }
    return events;
  }
  // 原安全意图和原序前态共同确定完整后果；不能由幸存事实推测被删目标/暂停/轮换/绑定。
  assertAccountCommit(events: RelationalEvent[], receipt: Json) {
    const saved = receipt as {
        accountId?: unknown;
        accountIds?: unknown;
        enabled?: unknown;
        accountIntent?: AccountIntent;
        checked?: unknown;
        ok?: unknown;
        failed?: unknown;
        ignored?: unknown;
        error?: unknown;
      },
      intent = saved?.accountIntent,
      owned = events.filter((event) =>
        [
          'AccountRegistered',
          'AccountBound',
          'AccountConfigurationChanged',
          'CredentialRotated',
          'AvailabilityChanged',
          'QuotaObserved',
        ].includes(event.eventType),
      );
    if (!intent) {
      if (
        Object.hasOwn(Object(receipt), 'accountIds') ||
        (Object.hasOwn(Object(receipt), 'accountId') &&
          Object.hasOwn(Object(receipt), 'enabled')) ||
        events.some((event) =>
          ['AccountRegistered', 'AccountConfigurationChanged', 'CredentialRotated'].includes(
            event.eventType,
          ),
        )
      ) {
        fail('账户完整提交缺少原安全意图');
      }
      return;
    }
    if (!events.length || !intent || typeof intent !== 'object') {
      fail('账户完整提交缺少原安全意图');
    }
    const row = this.sqlite
      .prepare<[string], { recordedAt: number; actorKind: string }>(
        'SELECT recorded_at AS recordedAt,actor_kind AS actorKind FROM commits WHERE commit_seq=CAST(? AS INTEGER)',
      )
      .get(events[0].commitSeq);
    if (!row || !['member', 'admin'].includes(row.actorKind)) {
      fail('账户原安全意图缺少执行角色');
    }
    const metadata = {
        recordedAt: row.recordedAt,
        policyVersion: events[0].policyVersion,
        pricingVersion: events[0].pricingVersion,
      },
      expected: RelationalEventInput[] = [];
    if (intent.kind === 'bulk') {
      if (
        row.actorKind !== 'admin' ||
        Object.keys(intent).some((key) => !['kind', 'ids', 'weight', 'enabled'].includes(key))
      ) {
        fail('批量账户原安全意图无效');
      }
      const planned = this.planBulkAccounts(metadata, intent);
      if (
        canonicalJson(saved.accountIds as Json) !== canonicalJson(planned.accountIds) ||
        events.length !== owned.length
      ) {
        fail('批量账户完整目标与原安全意图不符');
      }
      expected.push(...planned.events);
    } else if (intent.kind === 'batch') {
      const observed = owned.filter(
          (e): e is Extract<RelationalEvent, { eventType: 'QuotaObserved' }> =>
            e.eventType === 'QuotaObserved',
        ),
        // 合法金融事实仍须属于本批实际观察；fresh no-op 不能夹带另一命令的同步。
        observedAccounts = new Set(observed.map((e) => e.accountId)),
        failed = intent.mode === 'enable' && !intent.applied,
        error = saved.error as { statusCode?: unknown; message?: unknown } | undefined;
      if (
        row.actorKind !== 'admin' ||
        Object.keys(saved).some(
          (key) =>
            ![
              'accountIds',
              'accountIntent',
              'checked',
              'ok',
              'failed',
              'ignored',
              'error',
            ].includes(key),
        ) ||
        canonicalJson(saved.accountIds as Json) !== canonicalJson(intent.ids) ||
        saved.checked !== observed.length ||
        saved.ok !== observed.filter((e) => e.payload.errorCode === null).length ||
        saved.failed !== observed.filter((e) => e.payload.errorCode !== null).length ||
        saved.ignored !== 0 ||
        (failed
          ? !error ||
            error.statusCode !== 409 ||
            typeof error.message !== 'string' ||
            !error.message
          : error !== undefined) ||
        events.some(
          (e) =>
            !intent.ids?.includes(e.accountId ?? '') ||
            (!owned.includes(e) &&
              (!observedAccounts.has(e.accountId) ||
                ![
                  'QuotaStatusChanged',
                  'QuotaBaselineAdopted',
                  'RefillApplied',
                  'AllocationApplied',
                  'V5CapacityObserved',
                  'ResourceGapObserved',
                  'ReturnClassified',
                  'FundingReceived',
                ].includes(e.eventType))),
        )
      ) {
        fail('批量账户原回执、目标或事实范围不符');
      }
      expected.push(...this.planBatchDecisions(metadata, intent, observed));
    } else if (intent.kind === 'register') {
      if (
        Object.keys(intent).some(
          (key) =>
            !['kind', 'accountId', 'displayName', 'weight', 'bind', 'verified'].includes(key),
        ) ||
        typeof intent.bind !== 'boolean' ||
        (intent.bind === false && row.actorKind !== 'admin') ||
        (intent.verified !== undefined &&
          (intent.verified !== true || intent.bind || row.actorKind !== 'admin')) ||
        this.sqlite.prepare('SELECT 1 FROM upstream_accounts WHERE id=?').get(intent.accountId) ||
        (intent.bind &&
          (this.policy().accountId !== null ||
            this.sqlite.prepare('SELECT 1 FROM upstream_accounts LIMIT 1').get())) ||
        saved.accountId !== intent.accountId ||
        saved.enabled !== Boolean(intent.verified) ||
        events.length !== owned.length
      ) {
        fail('账户注册完整决定与原安全意图不符');
      }
      const observations = owned.filter((event) => event.eventType === 'QuotaObserved');
      if (observations.length !== Number(Boolean(intent.verified))) {
        fail('账户注册缺少原完整核验观察');
      }
      const observed = observations[0];
      expected.push(
        ...this.planRegistration(
          metadata,
          intent,
          observed?.eventType === 'QuotaObserved' ? observed : undefined,
        ),
      );
    } else if (intent.kind === 'mutation') {
      if (
        Object.keys(intent).some(
          (key) =>
            ![
              'kind',
              'accountId',
              'displayName',
              'weight',
              'profile',
              'connectionChanged',
              'credentialChanged',
              'profileAt',
              'observe',
              'availability',
            ].includes(key),
        ) ||
        ![intent.profile, intent.connectionChanged, intent.credentialChanged, intent.observe].every(
          (value) => typeof value === 'boolean',
        ) ||
        (intent.displayName !== undefined &&
          (typeof intent.displayName !== 'string' || intent.displayName.length > 80)) ||
        (intent.weight !== undefined &&
          (!Number.isFinite(intent.weight) || intent.weight < 1 || intent.weight > 100)) ||
        (!intent.profile && (intent.displayName !== undefined || intent.weight !== undefined)) ||
        (intent.credentialChanged && intent.observe) ||
        (intent.profileAt !== undefined &&
          (!intent.profile || !Number.isSafeInteger(intent.profileAt) || intent.profileAt < 0)) ||
        (intent.connectionChanged && !intent.profile) ||
        (intent.availability !== null &&
          (!intent.availability ||
            typeof intent.availability.enabled !== 'boolean' ||
            typeof intent.availability.reason !== 'string' ||
            Object.keys(intent.availability).some(
              (key) => !['enabled', 'reason'].includes(key),
            ))) ||
        (!intent.observe &&
          intent.availability !== null &&
          (intent.availability.enabled ||
            !['owner_paused', 'credential_replaced'].includes(intent.availability.reason))) ||
        (intent.credentialChanged &&
          (intent.availability?.enabled !== false ||
            intent.availability.reason !== 'credential_replaced')) ||
        saved.accountId !== intent.accountId
      ) {
        fail('账户修改原安全意图无效');
      }
      const account = this.account(intent.accountId);
      if (
        row.actorKind !== 'admin' &&
        (intent.profile || intent.connectionChanged || this.policy().accountId !== account.id)
      ) {
        fail('账户修改原安全意图越权');
      }
      const observations = owned.filter((event) => event.eventType === 'QuotaObserved');
      if (
        observations.length !== Number(intent.observe) ||
        (!intent.observe && events.length !== owned.length)
      ) {
        fail('账户完整修改缺少原观察决定');
      }
      const observed = observations[0];
      expected.push(
        ...this.planMutation(
          metadata,
          intent,
          observed?.eventType === 'QuotaObserved' ? observed : undefined,
        ),
      );
      if (saved.enabled !== (intent.availability?.enabled ?? account.enabled === 1)) {
        fail('账户原回执与完整可用性决定不符');
      }
    } else {
      fail('账户原安全意图种类无效');
    }
    const decision = (event: RelationalEventInput) => ({
      eventType: event.eventType,
      payload: event.payload,
      streamId: event.streamId,
      accountId: event.accountId,
      memberId: event.memberId,
      occurredAt: event.occurredAt,
      policyVersion: event.policyVersion,
      pricingVersion: event.pricingVersion,
    });
    if (
      canonicalJson(expected.map(decision) as unknown as Json) !==
      canonicalJson(owned.map(decision) as unknown as Json)
    ) {
      fail('账户完整事实与原安全意图/前态规划不符');
    }
  }
  private registrationSource(
    command: AccountCommandMetadata,
    input: Extract<OwnerAccountMutation, { kind: 'register' }>,
    source: AccountEnablePreparation,
    member: MemberUser | null,
    started: boolean,
  ) {
    const state = this.#registrations.get(source),
      original = state?.prepared;
    const { recordedAt: _at, ...context } = command;
    if (
      !state ||
      !original ||
      !original.registration ||
      member !== null ||
      command.actorKind !== 'admin' ||
      source !== (started ? state.ticket : state.prepared) ||
      (!started && state.ticket !== undefined) ||
      state.context !== canonicalJson(context) ||
      input.enabled !== true ||
      input.bind !== false ||
      input.accountId !== original.accountId ||
      input.token !== original.pst ||
      (input.proxyUrl ?? '') !== original.proxyUrl ||
      input.configVersion !== original.configVersion ||
      input.displayName !== original.registration?.displayName ||
      (input.weight ?? 1) !== original.registration.weight ||
      original.configVersion !== this.policy().version
    ) {
      fail('新账户准备、授权或原注册来源已变化');
    }
    assertRelationalAccountRegistrationAbsent(
      this.sqlite,
      original.accountId,
      original.connectionSourceEventId,
    );
    return state;
  }
  releaseAccountRegistration(source: AccountEnablePreparation) {
    const state = this.#registrations.get(source);
    if (state) {
      if (state.ticket) {
        this.quotas.releaseAccountRegistrationObservation(state.ticket);
        this.#registrations.delete(state.ticket);
      }
      this.#registrations.delete(state.prepared);
    }
  }
  private enableSource(
    command: AccountCommandMetadata,
    input: Extract<OwnerAccountMutation, { kind: 'enable' | 'observe' | 'configure' }>,
    source: AccountEnablePreparation,
    member: MemberUser | null,
  ) {
    const account =
      member === null && command.actorKind === 'admin'
        ? this.account(input.accountId)
        : this.boundAccount(input.accountId);
    if (
      source.commandId !== command.commandId ||
      source.fingerprint !== command.fingerprint ||
      source.accountId !== account.id ||
      (input.kind !== 'observe' &&
        source.availabilityEventId !== this.availabilityEventId(account.id)) ||
      (input.kind !== 'observe' && source.configVersion !== this.policy().version) ||
      source.authEpoch !== (member?.memberAuth?.version ?? null) ||
      (input.kind === 'configure' &&
        (input.changes.displayName !== undefined ||
          input.changes.weight !== undefined ||
          input.changes.proxyUrl !== undefined)) !== Boolean(source.configuration) ||
      (input.kind === 'configure' &&
        (input.changes.enabled !== true ||
          (input.changes.token !== undefined && input.changes.token !== account.pst))) ||
      (source.configuration &&
        (input.kind !== 'configure' ||
          member !== null ||
          (input.changes.displayName !== undefined &&
            source.configuration.displayName !== input.changes.displayName) ||
          (input.changes.weight !== undefined &&
            source.configuration.weight !== input.changes.weight) ||
          source.configuration.proxyUrl !== (input.changes.proxyUrl ?? account.proxyUrl) ||
          source.configuration.connectionChanged !==
            (input.changes.proxyUrl !== undefined && input.changes.proxyUrl !== account.proxyUrl)))
    ) {
      fail('核验期间账户配置或授权已变化');
    }
    const current =
      source.credentialEpoch === account.epoch &&
      source.pst === account.pst &&
      source.proxyUrl === account.proxyUrl &&
      source.connectionSourceEventId === relationalAccountConnectionSource(this.sqlite, account.id);
    if (!current && input.kind !== 'observe') {
      fail('核验期间账户配置或授权已变化');
    }
    return { account, current };
  }
  // 准备不领取观察号；只有 query owner 已取得实际槽后，才在发起前复核并开始观察。
  startAccountEnable(
    metadata: AccountCommandMetadata,
    input: Extract<OwnerAccountMutation, { kind: 'enable' | 'observe' | 'configure' | 'register' }>,
    prepared: AccountEnablePreparation,
    member: MemberUser | null,
  ): AccountObservationStart {
    return this.owned(() => {
      this.authorize(metadata, member);
      if (input.kind === 'register') {
        assertRelationalStorageReady(this.sqlite);
      }
      const command = this.command(metadata, input),
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return { result: prior };
      }
      return this.accountObservationStart(() => {
        this.authorize(metadata, member);
        assertRelationalStorageReady(this.sqlite);
        const prior = readRelationalReceipt(this.sqlite, command);
        if (prior) {
          return { result: prior };
        }
        if (input.kind === 'register') {
          const source = this.registrationSource(command, input, prepared, member, false),
            issued = this.quotas.beginAccountRegistrationObservation(
              {
                accountId: prepared.accountId,
                credentialEpoch: prepared.credentialEpoch,
                pst: prepared.pst,
                proxyUrl: prepared.proxyUrl,
                connectionSourceEventId: prepared.connectionSourceEventId,
              },
              metadata.recordedAt,
            ),
            ticket: AccountEnableTicket = Object.freeze({ ...prepared, ...issued });
          source.ticket = ticket;
          this.#registrations.set(ticket, source);
          return { ticket };
        }
        const { account, current } = this.enableSource(command, input, prepared, member);
        if (!current) {
          fail('额度查询发起前原连接已变化');
        }
        return {
          ticket: {
            ...prepared,
            ...this.quotas.beginQuotaObservation(account.id, metadata.recordedAt, prepared),
          },
        };
      });
    });
  }
  completeAccountEnable(
    metadata: AccountCommandMetadata,
    input: Extract<OwnerAccountMutation, { kind: 'enable' | 'observe' | 'configure' | 'register' }>,
    ticket: AccountEnableTicket,
    result: QuotaResult,
    member: MemberUser | null,
  ) {
    return this.owned(() => {
      this.authorize(metadata, member);
      if (input.kind === 'register') {
        assertRelationalStorageReady(this.sqlite);
      }
      const command = this.command(metadata, input),
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return prior;
      }
      if (input.kind === 'register') {
        const source = this.registrationSource(command, input, ticket, member, true),
          events = this.quotas.planAccountRegistrationObservation(command, ticket, result),
          observed = events.find((event) => event.eventType === 'QuotaObserved'),
          intent: Extract<AccountIntent, { kind: 'register' }> = {
            kind: 'register',
            accountId: ticket.accountId,
            displayName: source.prepared.registration!.displayName,
            weight: source.prepared.registration!.weight,
            bind: false,
            verified: true,
          },
          committed = this.commit(
            command,
            this.planRegistration(
              command,
              intent,
              observed?.eventType === 'QuotaObserved' ? observed : undefined,
              ticket.connectionSourceEventId,
            ),
            undefined,
            undefined,
            intent,
          );
        this.writeConnection(ticket.accountId, ticket.pst, ticket.proxyUrl);
        return committed;
      }
      const { account, current } = this.enableSource(command, input, ticket, member);
      const ignored = {
        ignored: true as const,
        receipt: { checked: 1, ok: 0, failed: 0, ignored: 1 },
      };
      if (!current) {
        return ignored;
      }
      const events = this.quotas.planQuotaObservation(
        command,
        ticket,
        result,
        input.kind !== 'observe',
      );
      if (!events) {
        if (input.kind === 'observe') {
          return ignored;
        }
        fail('核验观察已经过期，上游保持暂停');
      }
      if (input.kind === 'observe') {
        return this.commit(command, events, undefined, {
          checked: 1,
          ok: Number(result.ok),
          failed: Number(!result.ok),
          ignored: 0,
        });
      }
      if (ticket.configuration?.connectionChanged) {
        for (const event of events) {
          if (event.eventType === 'QuotaObserved') {
            event.payload.connectionSourceEventId = ticket.configuration.eventId;
          }
        }
      }
      const verified = events.some(
        (event) => event.eventType === 'AvailabilityChanged' && event.payload.enabled,
      );
      const availability = events.find((event) => event.eventType === 'AvailabilityChanged'),
        intent: AccountIntent = {
          kind: 'mutation',
          accountId: account.id,
          profile: Boolean(ticket.configuration),
          ...(input.kind === 'configure' && input.changes.displayName !== undefined
            ? { displayName: input.changes.displayName }
            : {}),
          ...(input.kind === 'configure' && input.changes.weight !== undefined
            ? { weight: input.changes.weight }
            : {}),
          connectionChanged: ticket.configuration?.connectionChanged ?? false,
          credentialChanged: false,
          ...(ticket.configuration && ticket.startedAt !== command.recordedAt
            ? { profileAt: ticket.startedAt }
            : {}),
          observe: true,
          availability:
            availability?.eventType === 'AvailabilityChanged'
              ? { enabled: availability.payload.enabled, reason: availability.payload.reason }
              : null,
        };
      const observed = events.find((event) => event.eventType === 'QuotaObserved'),
        complete = [
          ...this.planMutation(
            command,
            intent,
            observed?.eventType === 'QuotaObserved' ? observed : undefined,
            ticket.configuration?.eventId,
          ),
          ...events.filter(
            (event) => !['QuotaObserved', 'AvailabilityChanged'].includes(event.eventType),
          ),
        ];
      // 失败观察/封禁决定也必须落库；公开错误在认证事务提交后才抛出。
      if (verified) {
        const committed = this.commit(command, complete, undefined, undefined, intent);
        if (ticket.configuration?.connectionChanged) {
          this.writeConnection(account.id, account.pst, ticket.configuration.proxyUrl);
        }
        return committed;
      }
      let message = '未核验到有效 Opus，上游未启用';
      if (!result.ok) {
        message = result.quotaError;
      }
      const committed = this.commit(
        command,
        complete,
        { statusCode: 409, message },
        undefined,
        intent,
      );
      if (ticket.configuration?.connectionChanged) {
        this.writeConnection(account.id, account.pst, ticket.configuration.proxyUrl);
      }
      return committed;
    });
  }
  private configurationEvents(
    metadata: Pick<AccountCommandMetadata, 'recordedAt' | 'policyVersion' | 'pricingVersion'>,
    account: Account,
    configuration: AccountConfiguration,
    at = metadata.recordedAt,
  ): RelationalEventInput[] {
    return [
      {
        ...this.event(metadata, {
          eventType: 'AccountConfigurationChanged',
          payload: {
            accountId: account.id,
            displayName: configuration.displayName,
            weight: configuration.weight,
            connectionChanged: configuration.connectionChanged,
          },
        }),
        eventId: configuration.eventId,
        occurredAt: at,
      },
    ];
  }
  private writeConnection(accountId: string, pst: string | null, proxyUrl: string | null) {
    if (pst !== null) {
      this.sqlite
        .prepare(
          'INSERT INTO upstream_credentials(account_id,pst,proxy_url) VALUES(?,?,?) ON CONFLICT(account_id) DO UPDATE SET pst=excluded.pst,proxy_url=excluded.proxy_url',
        )
        .run(accountId, pst, proxyUrl);
    }
  }
  private registerQuota(event: Extract<RelationalEvent, { eventType: 'AccountRegistered' }>) {
    this.quotas.registerAccountQuota(event);
    this.sqlite
      .prepare(
        `INSERT INTO operating_state(account_id,v5_available,anlas_fixed_available,anlas_purchased_available,
      unfunded_fixed,unfunded_purchased,unattributed_fixed,unattributed_purchased,v5_capacity_gap,source_event_id)
      VALUES(?,'0','0','0','0','0','0','0','0',?)`,
      )
      .run(event.payload.accountId, event.eventId);
    this.ensureMemberWallets(event.payload.accountId, event.eventId);
  }
  private ensureMemberWallets(accountId: string, sourceEventId: string) {
    this.sqlite
      .prepare(
        `INSERT INTO wallets(account_id,member_id,v5_available,anlas_fixed_available,anlas_purchased_available,source_event_id)
      SELECT ?,member_id,'0','0','0',? FROM group_members WHERE true
      ON CONFLICT(account_id,member_id) DO NOTHING`,
      )
      .run(accountId, sourceEventId);
  }
  applyAccountEvent(event: RelationalEvent) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Account projection requires the owning transaction.');
    }
    switch (event.eventType) {
      case 'AccountRegistered': {
        const p = event.payload,
          command = this.sqlite
            .prepare<[string], { kind: string }>(
              'SELECT actor_kind AS kind FROM commits WHERE commit_seq=CAST(? AS INTEGER)',
            )
            .get(event.commitSeq);
        if (
          p.enabled ||
          p.credentialEpoch !== 1 ||
          p.displayName.length > 80 ||
          (this.sqlite.prepare('SELECT 1 FROM upstream_accounts LIMIT 1').get() &&
            command?.kind !== 'admin')
        ) {
          fail('账户注册必须暂停、使用初始代次且新增账户需原管理员决定');
        }
        this.sqlite
          .prepare(
            `INSERT INTO upstream_accounts(id,display_name_json,weight,enabled,credential_epoch,ordinary_strikes,capacity_strikes,source_event_id)
            VALUES(?,?,?,0,1,0,0,?)`,
          )
          .run(p.accountId, JSON.stringify(p.displayName), p.weight, event.eventId);
        this.registerQuota(event);
        break;
      }
      case 'AccountBound': {
        const p = event.payload,
          policy = this.policy();
        assertOwnerAccountInitialBinding(
          policy,
          p,
          !!this.sqlite.prepare('SELECT 1 FROM upstream_accounts WHERE id=?').get(p.accountId),
        );
        this.sqlite
          .prepare(
            'UPDATE sharing_policy SET bound_account_id=?,config_version=?,source_event_id=? WHERE id=1',
          )
          .run(p.accountId, p.configVersion, event.eventId);
        this.ensureMemberWallets(p.accountId, event.eventId);
        break;
      }
      case 'CredentialRotated': {
        const p = event.payload,
          account = this.account(p.accountId);
        if (
          this.policy().accountId !== account.id &&
          this.sqlite
            .prepare<[string], { kind: string }>(
              'SELECT actor_kind AS kind FROM commits WHERE commit_seq=CAST(? AS INTEGER)',
            )
            .get(event.commitSeq)?.kind !== 'admin'
        ) {
          fail('未绑定账户凭据轮换缺少原管理员决定');
        }
        if (p.beforeEpoch !== account.epoch) {
          fail('凭据轮换的原代次不符');
        }
        assertOwnerAccountCredentialChangeAllowed(
          0,
          Boolean(
            this.sqlite
              .prepare(
                "SELECT 1 FROM charges WHERE original_account_id=? AND status='reserved' LIMIT 1",
              )
              .get(account.id),
          ),
        );
        // 业务回放撤销旧私有 PST；只有当前命令或匹配切点的私有快照能提供新值。
        this.sqlite.prepare('DELETE FROM upstream_credentials WHERE account_id=?').run(account.id);
        this.sqlite
          .prepare(
            'UPDATE upstream_accounts SET credential_epoch=?,enabled=0,source_event_id=? WHERE id=?',
          )
          .run(p.credentialEpoch, event.eventId, account.id);
        break;
      }
      case 'AccountConfigurationChanged': {
        const p = event.payload,
          account = this.account(p.accountId),
          command = this.sqlite
            .prepare<[string], { kind: string }>(
              'SELECT actor_kind AS kind FROM commits WHERE commit_seq=CAST(? AS INTEGER)',
            )
            .get(event.commitSeq);
        if (
          command?.kind !== 'admin' ||
          event.memberId !== null ||
          p.displayName.length > 80 ||
          (p.weight === null && account.weight !== null)
        ) {
          fail('账户档案修改缺少原管理员决定');
        }
        this.sqlite
          .prepare(
            'UPDATE upstream_accounts SET display_name_json=?,weight=?,source_event_id=? WHERE id=?',
          )
          .run(JSON.stringify(p.displayName), p.weight, event.eventId, account.id);
        if (p.connectionChanged) {
          // 公共回放只能撤销旧连接；新私有值由实际命令或选定快照安装。
          this.sqlite
            .prepare('UPDATE upstream_credentials SET proxy_url=NULL WHERE account_id=?')
            .run(account.id);
        }
        break;
      }
      default:
        fail(`Unsupported account projection ${event.eventType}`);
    }
  }
  private applyAccountEvents(events: RelationalEvent[]) {
    const applyQuota = this.quotas.createQuotaProjector(events);
    for (const event of events) {
      if (relationalQuotaFacts.has(event.eventType)) {
        applyQuota(event);
      } else {
        this.applyAccountEvent(event);
      }
    }
  }
  replayAccountCommit(stored: StoredRelationalCommit) {
    return this.owned(() => {
      if (
        stored.events.some(
          (event) =>
            !relationalQuotaFacts.has(event.eventType) &&
            !relationalAccountFacts.has(event.eventType),
        )
      ) {
        fail('Account replay requires one complete account/quota commit');
      }
      const result = replayRelationalCommit(this.sqlite, stored);
      if (!result.replayed) {
        this.assertAccountCommit(result.events, result.receipt);
        this.applyAccountEvents(result.events);
      }
      return result;
    });
  }
}
