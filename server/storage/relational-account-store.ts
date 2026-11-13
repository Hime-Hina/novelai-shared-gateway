import type Database from 'better-sqlite3';
import { domainDigest } from '../events/domain-events.ts';
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
import type { MemberUser } from '../members/authentication.ts';
import {
  assertOwnerAccountPst,
  assertOwnerAccountCredentialChangeAllowed,
  ownerAccountConfiguration,
} from '../members/owner-operations.ts';
import { physicalAccountLoad } from '../generation/queue-dispatch.ts';
import { RelationalQuotaStore, relationalQuotaFacts } from './relational-quota-store.ts';
import type { QuotaResult } from '../quota/account-refresh.ts';
import { assertRelationalStorageFormat } from './relational-storage.ts';

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
    }
  | { kind: 'pause'; accountId: string }
  | { kind: 'token'; accountId: string; token: string }
  | { kind: 'enable'; accountId: string };
export type AccountEnableTicket = ReturnType<RelationalQuotaStore['beginQuotaObservation']> & {
  availabilityEventId: string;
  configVersion: number;
  authEpoch: string | null;
  commandId: string;
  fingerprint: string;
};
type Account = {
  id: string;
  displayName: string;
  enabled: number;
  epoch: number;
  pst: string | null;
};
function fail(message: string, statusCode = 409): never {
  throw Object.assign(new Error(message), { statusCode });
}

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
  private owned<T>(run: () => T): T {
    if (!this.sqlite.inTransaction) {
      throw new Error('Account writes require the owning authenticated command transaction.');
    }
    return this.sqlite.transaction(run)();
  }
  private authorize(metadata: AccountCommandMetadata, member: MemberUser | null) {
    if (metadata.actorKind === 'admin' && member === null) {
      return;
    }
    const current =
      member &&
      this.sqlite
        .prepare<[string], { status: string; auth_epoch: string | null }>(
          'SELECT status,auth_epoch FROM members WHERE id=?',
        )
        .get(member.id);
    if (
      metadata.actorKind !== 'member' ||
      !member ||
      metadata.actorId !== member.id ||
      member.id !== this.env.OWNER_USER_ID ||
      !member.enabled ||
      !member.memberAuth?.version ||
      current?.status !== 'active' ||
      current.auth_epoch !== member.memberAuth.version
    ) {
      fail('账户配置需要当前 owner 或本地管理员授权', 403);
    }
  }
  private command(metadata: AccountCommandMetadata, input: OwnerAccountMutation) {
    // 轮换的秘密不进入事实；命令意图摘要仍区分不同 PST，防止同键不同操作。
    return { ...metadata, fingerprint: domainDigest({ command: metadata.fingerprint, input }) };
  }
  private account(id: string): Account {
    const row = this.sqlite
      .prepare<[string], Account>(
        `SELECT a.id,a.display_name AS displayName,a.enabled,a.credential_epoch AS epoch,
      c.pst FROM upstream_accounts a LEFT JOIN upstream_credentials c ON c.account_id=a.id WHERE a.id=?`,
      )
      .get(id);
    if (!row) {
      fail('上游账户不存在', 404);
    }
    return row;
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
  private event(metadata: AccountCommandMetadata, fact: RelationalFact): RelationalEventInput {
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
      receipt: { accountId: events[0].accountId, enabled, ...(error ? { error } : {}) },
    });
    this.applyAccountEvents(result.events);
    return result;
  }
  prepareAccountCommand(
    metadata: AccountCommandMetadata,
    input: OwnerAccountMutation,
    member: MemberUser | null,
  ): { result: RelationalCommitResult } | { ticket: AccountEnableTicket } {
    return this.owned(() => {
      this.authorize(metadata, member);
      let allowed = ['kind', 'accountId'];
      if (input?.kind === 'register') {
        allowed = ['kind', 'accountId', 'displayName', 'token', 'configVersion'];
      } else if (input?.kind === 'token') {
        allowed = ['kind', 'accountId', 'token'];
      }
      if (
        !input ||
        !['register', 'pause', 'token', 'enable'].includes(input.kind) ||
        Object.keys(input).length !== allowed.length ||
        Object.keys(input).some((key) => !allowed.includes(key)) ||
        typeof input.accountId !== 'string' ||
        !input.accountId ||
        input.accountId.includes('\0')
      ) {
        fail('账户配置参数无效', 400);
      }
      const command = this.command(metadata, input),
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return { result: prior };
      }
      if (input.kind === 'register') {
        assertOwnerAccountPst(input.token);
        if (
          typeof input.displayName !== 'string' ||
          !input.displayName ||
          input.displayName.length > 80
        ) {
          fail('账户名称需为 1–80 个字符', 400);
        }
        const policy = this.policy();
        if (
          policy.accountId !== null ||
          policy.version !== input.configVersion ||
          this.sqlite.prepare('SELECT 1 FROM upstream_accounts LIMIT 1').get()
        ) {
          fail('仅允许在未绑定共享账户时首次注册');
        }
        const result = this.commit(command, [
          this.event(command, {
            eventType: 'AccountRegistered',
            payload: {
              accountId: input.accountId,
              displayName: input.displayName,
              enabled: false,
              credentialEpoch: 1,
            },
          }),
          this.event(command, {
            eventType: 'AccountBound',
            payload: { accountId: input.accountId, configVersion: policy.version + 1 },
          }),
        ]);
        this.sqlite
          .prepare('INSERT INTO upstream_credentials(account_id,pst) VALUES(?,?)')
          .run(input.accountId, input.token);
        return { result };
      }
      const account = this.boundAccount(input.accountId);
      if (input.kind === 'enable') {
        return {
          ticket: {
            ...this.quotas.beginQuotaObservation(account.id, metadata.recordedAt),
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
      const events: RelationalEventInput[] = [];
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
        events.push(
          this.event(command, {
            eventType: 'CredentialRotated',
            payload: {
              accountId: account.id,
              beforeEpoch: account.epoch,
              credentialEpoch: account.epoch + 1,
            },
          }),
        );
      }
      events.push(
        this.event(command, {
          eventType: 'AvailabilityChanged',
          payload: {
            accountId: account.id,
            enabled: false,
            reason: input.kind === 'token' ? 'credential_replaced' : 'owner_paused',
            observationEventId: null,
          },
        }),
      );
      const result = this.commit(command, events);
      if (input.kind === 'token' && input.token !== account.pst) {
        this.sqlite
          .prepare(
            'INSERT INTO upstream_credentials(account_id,pst) VALUES(?,?) ON CONFLICT(account_id) DO UPDATE SET pst=excluded.pst',
          )
          .run(account.id, input.token);
      }
      return { result };
    });
  }
  completeAccountEnable(
    metadata: AccountCommandMetadata,
    input: Extract<OwnerAccountMutation, { kind: 'enable' }>,
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
      const account = this.boundAccount(input.accountId);
      if (
        ticket.commandId !== command.commandId ||
        ticket.fingerprint !== command.fingerprint ||
        ticket.accountId !== account.id ||
        ticket.availabilityEventId !== this.availabilityEventId(account.id) ||
        ticket.configVersion !== this.policy().version ||
        ticket.authEpoch !== (member?.memberAuth?.version ?? null)
      ) {
        fail('核验期间账户配置或授权已变化');
      }
      const events = this.quotas.planQuotaObservation(command, ticket, result, true);
      if (!events) {
        fail('核验观察已经过期，上游保持暂停');
      }
      const verified = events.some(
        (event) => event.eventType === 'AvailabilityChanged' && event.payload.enabled,
      );
      // 失败观察/封禁决定也必须落库；公开错误在认证事务提交后才抛出。
      if (verified) {
        return this.commit(command, events);
      }
      let message = '未核验到有效 Opus，上游未启用';
      if (!result.ok) {
        message = result.quotaError;
      }
      return this.commit(command, events, { statusCode: 409, message });
    });
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
  private applyAccountEvents(events: RelationalEvent[]) {
    let index = 0;
    for (; index < events.length; index++) {
      const event = events[index];
      if (relationalQuotaFacts.has(event.eventType)) {
        break;
      }
      switch (event.eventType) {
        case 'AccountRegistered': {
          const p = event.payload;
          if (
            p.enabled ||
            p.credentialEpoch !== 1 ||
            this.sqlite.prepare('SELECT 1 FROM upstream_accounts LIMIT 1').get()
          ) {
            fail('首次账户注册必须暂停且使用初始代次');
          }
          this.sqlite
            .prepare(
              `INSERT INTO upstream_accounts(id,display_name,enabled,credential_epoch,ordinary_strikes,capacity_strikes,source_event_id)
            VALUES(?,?,0,1,0,0,?)`,
            )
            .run(p.accountId, p.displayName, event.eventId);
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
            account = this.boundAccount(p.accountId);
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
          this.sqlite
            .prepare('DELETE FROM upstream_credentials WHERE account_id=?')
            .run(account.id);
          this.sqlite
            .prepare(
              'UPDATE upstream_accounts SET credential_epoch=?,enabled=0,source_event_id=? WHERE id=?',
            )
            .run(p.credentialEpoch, event.eventId, account.id);
          break;
        }
        default:
          fail(`Unsupported account projection ${event.eventType}`);
      }
    }
    if (index < events.length) {
      this.quotas.applyQuotaEvents(events.slice(index));
    }
  }
  replayAccountCommit(stored: StoredRelationalCommit) {
    return this.owned(() => {
      if (
        stored.events.some(
          (event) =>
            !relationalQuotaFacts.has(event.eventType) &&
            !['AccountRegistered', 'AccountBound', 'CredentialRotated'].includes(event.eventType),
        )
      ) {
        fail('Account replay requires one complete account/quota commit');
      }
      const result = replayRelationalCommit(this.sqlite, stored);
      if (!result.replayed) {
        this.applyAccountEvents(result.events);
      }
      return result;
    });
  }
}
