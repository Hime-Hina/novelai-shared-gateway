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
import { relationalFactStream } from '../events/relational-fact-contracts.ts';
import { assertCurrentOwnerCommand, type MemberUser } from '../members/authentication.ts';
import {
  assertOwnerAccountPst,
  assertOwnerAccountCredentialChangeAllowed,
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
import { assertRelationalStorageFormat } from './relational-storage.ts';
import { relationalAccountConnectionSource } from './relational-account-connection.ts';
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
  | { kind: 'register'; accountId: string; displayName: string; weight: number; bind: boolean }
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
};
export type AccountEnablePreparation = Omit<AccountEnableTicket, 'sequence' | 'startedAt'>;

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
        const policy = this.policy(),
          binding = input.bind !== false;
        if (input.bind !== undefined && typeof input.bind !== 'boolean') {
          fail('账户注册绑定参数无效', 400);
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
        input.kind === 'configure' && member === null
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
          this.boundAccount(account.id);
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
  private planRegistration(
    metadata: Pick<AccountCommandMetadata, 'recordedAt' | 'policyVersion' | 'pricingVersion'>,
    intent: Extract<AccountIntent, { kind: 'register' }>,
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
    if (intent.bind) {
      events.push(
        this.event(metadata, {
          eventType: 'AccountBound',
          payload: { accountId: intent.accountId, configVersion: this.policy().version + 1 },
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
    } else if (intent.kind === 'register') {
      if (
        Object.keys(intent).some(
          (key) => !['kind', 'accountId', 'displayName', 'weight', 'bind'].includes(key),
        ) ||
        typeof intent.bind !== 'boolean' ||
        (intent.bind === false && row.actorKind !== 'admin') ||
        this.sqlite.prepare('SELECT 1 FROM upstream_accounts WHERE id=?').get(intent.accountId) ||
        (intent.bind &&
          (this.policy().accountId !== null ||
            this.sqlite.prepare('SELECT 1 FROM upstream_accounts LIMIT 1').get())) ||
        saved.accountId !== intent.accountId ||
        saved.enabled !== false ||
        events.length !== owned.length
      ) {
        fail('账户注册完整决定与原安全意图不符');
      }
      expected.push(...this.planRegistration(metadata, intent));
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
  private enableSource(
    command: AccountCommandMetadata,
    input: Extract<OwnerAccountMutation, { kind: 'enable' | 'observe' | 'configure' }>,
    source: AccountEnablePreparation,
    member: MemberUser | null,
  ) {
    const account = this.boundAccount(input.accountId);
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
    input: Extract<OwnerAccountMutation, { kind: 'enable' | 'observe' | 'configure' }>,
    prepared: AccountEnablePreparation,
    member: MemberUser | null,
  ): { result: RelationalCommitResult } | { ticket: AccountEnableTicket } {
    return this.owned(() => {
      this.authorize(metadata, member);
      const command = this.command(metadata, input),
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return { result: prior };
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
  }
  completeAccountEnable(
    metadata: AccountCommandMetadata,
    input: Extract<OwnerAccountMutation, { kind: 'enable' | 'observe' | 'configure' }>,
    ticket: AccountEnableTicket,
    result: QuotaResult,
    member: MemberUser | null,
  ) {
    return this.owned(() => {
      this.authorize(metadata, member);
      const command = this.command(metadata, input),
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return prior;
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
    this.sqlite
      .prepare(
        `INSERT INTO wallets(account_id,member_id,v5_available,anlas_fixed_available,anlas_purchased_available,source_event_id)
      SELECT ?,member_id,'0','0','0',? FROM group_members`,
      )
      .run(event.payload.accountId, event.eventId);
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
        if (
          policy.accountId !== null ||
          p.accountId === null ||
          p.configVersion !== policy.version + 1
        ) {
          fail('首次绑定的原政策或账户不符');
        }
        this.account(p.accountId);
        this.sqlite
          .prepare(
            'UPDATE sharing_policy SET bound_account_id=?,config_version=?,source_event_id=? WHERE id=1',
          )
          .run(p.accountId, p.configVersion, event.eventId);
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
