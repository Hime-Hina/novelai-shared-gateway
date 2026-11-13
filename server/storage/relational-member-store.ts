import { createHash, randomBytes } from 'node:crypto';
import type Database from 'better-sqlite3';
import {
  createDiscordInvite,
  type DiscordUser,
  type DiscordAuthenticationStore,
  type DiscordAuthenticationTransaction,
} from '../members/discord-auth.ts';
import {
  domainDigest,
  canonicalJson,
  beginDomainCommand,
  finishDomainCommand,
} from '../events/domain-events.ts';
import {
  loginMember,
  normalizeMemberUsername,
  prepareMemberCredentials,
  assertMemberRegistrationCapacity,
  assertMemberCanDisable,
  assertCurrentOwnerCommand,
  type MemberApiKeyStore,
  type MemberCredentials,
  type MemberPasswordStore,
  type MemberSession,
  type MemberSessionMutation,
  type MemberSessionTransaction,
  type MemberUser,
  type SessionOwner,
} from '../members/authentication.ts';
import {
  appendRelationalCommit,
  readRelationalReceipt,
  replayRelationalCommit,
  type StoredRelationalCommit,
} from '../events/relational-event-store.ts';
import type {
  RelationalCommand,
  RelationalEvent,
  RelationalFact,
  RelationalEventInput,
} from '../events/relational-events.ts';
import { assertRelationalStorageFormat } from './relational-storage.ts';
import { setMemberAdmission, MEMBER_STOPPED_MESSAGE } from '../generation/queue-policy.ts';
import { RelationalCommitReplayer } from './relational-replay.ts';

type MemberCommand = Omit<RelationalCommand, 'events' | 'expectedVersions' | 'receipt'> & {
  policyVersion: string;
  pricingVersion: string;
};
type MemberFact = Extract<
  RelationalFact,
  {
    eventType:
      'MemberRegistered' | 'MemberStatusChanged' | 'AdmissionChanged' | 'MemberEligibilityChanged';
  }
>;
export interface PreparedMemberCredentials {
  memberId: string;
  expectedAuthEpoch: string;
  credentials: MemberCredentials;
}
function fail(statusCode: number, message: string): never {
  throw Object.assign(new Error(message), { statusCode });
}
const digest = (text: string) => createHash('sha256').update(text).digest('hex');

export const relationalMemberFacts = new Set<RelationalEvent['eventType']>([
  'MemberRegistered',
  'OpeningMemberRecorded',
  'MemberStatusChanged',
  'AdmissionChanged',
  'MemberEligibilityChanged',
]);

// 私有认证不是业务投影；这里只有稳定 ID 的窄查询和事务，不提供 users/通用 update。
export class RelationalMemberStore
  implements MemberPasswordStore, MemberApiKeyStore, DiscordAuthenticationStore
{
  private sqlite: Database.Database;
  private env: NodeJS.ProcessEnv;
  constructor(sqlite: Database.Database, env: NodeJS.ProcessEnv = process.env) {
    assertRelationalStorageFormat(sqlite);
    this.sqlite = sqlite;
    this.env = env;
  }

  private owningTransaction<T>(write: () => T): T {
    this.assertMemberCommandTransaction();
    return this.sqlite.transaction(write)();
  }

  assertMemberCommandTransaction(): void {
    if (!this.sqlite.inTransaction) {
      throw new Error('Member writes require the owning authenticated command transaction.');
    }
  }

  private authenticationMember(memberId: string): DiscordUser | null {
    const row = this.sqlite
      .prepare<
        [string],
        {
          id: string;
          display_name: string;
          status: string;
          auth_epoch: string | null;
          username: string | null;
          discord_id: string | null;
          guild_id: string | null;
          access_token: string | null;
          refresh_token: string | null;
          expires_at: number | null;
          checked_at: number | null;
          eligible: number | null;
          code_hash: string | null;
          invite_expires_at: number | null;
        }
      >(
        `SELECT m.id,m.display_name,m.status,m.auth_epoch,p.username,d.discord_id,d.guild_id,
        d.access_token,d.refresh_token,d.expires_at,d.checked_at,d.eligible,
        i.code_hash,i.expires_at AS invite_expires_at FROM members m
      LEFT JOIN password_credentials p ON p.member_id=m.id
      LEFT JOIN discord_bindings d ON d.member_id=m.id
      LEFT JOIN discord_invites i ON i.member_id=m.id WHERE m.id=?`,
      )
      .get(memberId);
    if (!row) {
      return null;
    }
    return {
      id: row.id,
      note: row.display_name,
      enabled: row.status === 'active',
      ...(row.auth_epoch
        ? {
            memberAuth: {
              version: row.auth_epoch,
              ...(row.username === null ? {} : { username: row.username }),
            },
          }
        : {}),
      ...(row.discord_id && row.guild_id
        ? {
            discord: {
              id: row.discord_id,
              guildId: row.guild_id,
              ...(row.access_token === null ? {} : { accessToken: row.access_token }),
              ...(row.refresh_token === null ? {} : { refreshToken: row.refresh_token }),
              ...(row.expires_at === null ? {} : { expiresAt: row.expires_at }),
              ...(row.checked_at === null ? {} : { checkedAt: row.checked_at }),
              ...(row.eligible === null ? {} : { member: row.eligible === 1 }),
            },
          }
        : {}),
      ...(row.code_hash && row.invite_expires_at !== null
        ? { discordInvite: { hash: row.code_hash, expiresAt: row.invite_expires_at } }
        : {}),
    };
  }

  readMember(memberId: string) {
    return (
      this.sqlite
        .prepare<
          [string],
          {
            id: string;
            displayName: string;
            status: 'active' | 'disabled' | 'removed';
            acceptingJobs: number;
            admissionEpoch: number;
          }
        >(
          `SELECT id,display_name AS displayName,status,accepting_jobs AS acceptingJobs,
      admission_epoch AS admissionEpoch FROM members WHERE id=?`,
        )
        .get(memberId) || null
    );
  }

  async loginMemberPassword(username: unknown, password: unknown): Promise<MemberUser> {
    const credential = this.sqlite
      .prepare<[string], { member_id: string; salt: string | null; password_hash: string | null }>(
        'SELECT member_id,salt,password_hash FROM password_credentials WHERE username=?',
      )
      .get(normalizeMemberUsername(username));
    const member = credential ? this.authenticationMember(credential.member_id) : null;
    const candidate =
      member && credential
        ? {
            ...member,
            memberAuth: {
              ...member.memberAuth,
              salt: credential.salt ?? undefined,
              hash: credential.password_hash ?? undefined,
            },
          }
        : null;
    await loginMember({ users: candidate ? [candidate] : [] }, username, password);
    // 密码材料只用于现有 owner 的校验，不随会话身份视图返回。
    return member!;
  }

  async prepareMemberCredentials(
    memberId: string,
    input: { username?: unknown; password?: unknown },
  ): Promise<PreparedMemberCredentials> {
    if (this.sqlite.inTransaction) {
      throw new Error('Prepare password credentials outside the SQLite write transaction.');
    }
    const member = this.authenticationMember(memberId);
    if (member?.enabled !== true || !member.memberAuth?.version) {
      fail(404, '账户不存在或未启用');
    }
    return {
      memberId,
      expectedAuthEpoch: member.memberAuth.version,
      credentials: await prepareMemberCredentials(input),
    };
  }

  private replaceAuthenticationEpoch(memberId: string, epoch: string | null) {
    this.sqlite.prepare('UPDATE members SET auth_epoch=? WHERE id=?').run(epoch, memberId);
    this.sqlite.prepare('DELETE FROM member_sessions WHERE member_id=?').run(memberId);
  }

  private insertCredentials(memberId: string, credentials: MemberCredentials) {
    if (
      !credentials ||
      typeof credentials.username !== 'string' ||
      !credentials.username ||
      credentials.username !== normalizeMemberUsername(credentials.username) ||
      !/^[a-f0-9]{48}$/.test(credentials.salt) ||
      !/^[a-f0-9]{128}$/.test(credentials.hash) ||
      !/^[a-f0-9]{32}$/.test(credentials.version)
    ) {
      fail(400, '认证材料格式不完整');
    }
    try {
      this.sqlite
        .prepare(
          `INSERT INTO password_credentials(member_id,username,salt,password_hash) VALUES(?,?,?,?)
        ON CONFLICT(member_id) DO UPDATE SET username=excluded.username,salt=excluded.salt,password_hash=excluded.password_hash`,
        )
        .run(memberId, credentials.username, credentials.salt, credentials.hash);
    } catch (error) {
      if ((error as { code?: string }).code === 'SQLITE_CONSTRAINT_UNIQUE') {
        fail(409, '用户名已存在');
      }
      throw error;
    }
    this.replaceAuthenticationEpoch(memberId, credentials.version);
  }

  setMemberCredentials(prepared: PreparedMemberCredentials) {
    return this.owningTransaction(() => {
      const current = this.authenticationMember(prepared.memberId);
      if (current?.enabled !== true || current.memberAuth?.version !== prepared.expectedAuthEpoch) {
        fail(401, '成员认证状态已变化，请重新确认');
      }
      this.insertCredentials(prepared.memberId, prepared.credentials);
      return { id: prepared.memberId, username: prepared.credentials.username };
    });
  }

  private replaceKey(memberId: string, key: string) {
    if (typeof key !== 'string' || !key || key.length > 4096 || key.includes('\0')) {
      fail(400, '成员 Key 格式无效');
    }
    this.sqlite
      .prepare(
        `INSERT INTO api_keys(member_id,key_hash,recoverable_key) VALUES(?,?,?)
      ON CONFLICT(member_id) DO UPDATE SET key_hash=excluded.key_hash,recoverable_key=excluded.recoverable_key`,
      )
      .run(memberId, digest(key), key);
  }

  replaceMemberApiKey(memberId: string, key: string) {
    return this.owningTransaction(() => {
      if (this.readMember(memberId)?.status !== 'active') {
        fail(404, '账户不存在或未启用');
      }
      this.replaceKey(memberId, key);
    });
  }

  readMemberApiKey(memberId: string): string | null {
    const row = this.sqlite
      .prepare<[string], { recoverable_key: string; key_hash: string }>(
        'SELECT recoverable_key,key_hash FROM api_keys WHERE member_id=?',
      )
      .get(memberId);
    if (!row) {
      return null;
    }
    if (digest(row.recoverable_key) !== row.key_hash) {
      fail(503, '成员 Key 校验失败');
    }
    return row.recoverable_key;
  }

  private assertBusinessActor(metadata: MemberCommand) {
    if (!['admin', 'system'].includes(metadata.actorKind)) {
      fail(403, '成员变更需要管理命令');
    }
  }

  private commitMember(metadata: MemberCommand, memberId: string, fact: MemberFact) {
    this.assertBusinessActor(metadata);
    const streamId = `member:${memberId}`;
    const version =
      this.sqlite
        .prepare<[string], { version: string }>(
          'SELECT CAST(stream_version AS TEXT) AS version FROM stream_heads WHERE stream_id=?',
        )
        .get(streamId)?.version || '0';
    const { policyVersion, pricingVersion, ...command } = metadata;
    return appendRelationalCommit(this.sqlite, {
      ...command,
      expectedVersions: { [streamId]: version },
      receipt: { memberId },
      events: [
        {
          ...fact,
          streamId,
          memberId,
          accountId: null,
          occurredAt: metadata.recordedAt,
          policyVersion,
          pricingVersion,
        },
      ],
    });
  }

  registerMember(
    metadata: MemberCommand,
    input: {
      memberId: string;
      displayName: string;
      apiKey: string;
      credentials?: MemberCredentials;
    },
  ) {
    return this.owningTransaction(() => {
      this.assertBusinessActor(metadata);
      const prior = readRelationalReceipt(this.sqlite, metadata);
      if (prior) {
        return prior;
      }
      if (this.sqlite.prepare('SELECT 1 FROM members WHERE id=?').get(input.memberId)) {
        fail(409, '成员身份已存在');
      }
      const count = this.sqlite
        .prepare<[], { count: number }>(
          "SELECT COUNT(*) AS count FROM members WHERE status!='removed'",
        )
        .get()!.count;
      assertMemberRegistrationCapacity(count);
      const result = this.commitMember(metadata, input.memberId, {
        eventType: 'MemberRegistered',
        payload: {
          memberId: input.memberId,
          displayName: input.displayName,
          status: 'active',
          acceptingJobs: true,
          admissionEpoch: 0,
          createdAt: metadata.recordedAt,
        },
      });
      this.applyMemberEvents(result.events);
      if (input.credentials) {
        this.insertCredentials(input.memberId, input.credentials);
      } else {
        // 当前认证状态独立于具体凭据；只在真实注册初始化，业务回放仍保留 NULL。
        this.sqlite
          .prepare('UPDATE members SET auth_epoch=? WHERE id=?')
          .run(randomBytes(16).toString('hex'), input.memberId);
      }
      this.replaceKey(input.memberId, input.apiKey);
      return result;
    });
  }

  setMemberStatus(metadata: MemberCommand, memberId: string, status: 'active' | 'disabled') {
    return this.owningTransaction(() => {
      this.assertBusinessActor(metadata);
      const prior = readRelationalReceipt(this.sqlite, metadata);
      if (prior) {
        return prior;
      }
      const member = this.readMember(memberId);
      if (!['active', 'disabled'].includes(status) || !member || member.status === 'removed') {
        fail(404, '成员不存在');
      }
      if (status === 'disabled') {
        const work = this.sqlite
          .prepare<[string, string], { present: number }>(
            `SELECT
          EXISTS(SELECT 1 FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id
            WHERE r.member_id=? AND j.status IN ('queued','pending','running')) OR
          EXISTS(SELECT 1 FROM charges WHERE original_member_id=? AND status='reserved') AS present`,
          )
          .get(memberId, memberId)!;
        assertMemberCanDisable(work.present === 1);
      }
      const result = this.commitMember(metadata, memberId, {
        eventType: 'MemberStatusChanged',
        payload: { memberId, status },
      });
      this.applyMemberEvents(result.events);
      if (status === 'disabled') {
        this.replaceAuthenticationEpoch(memberId, randomBytes(16).toString('hex'));
      }
      return result;
    });
  }

  changeMemberAdmission(
    metadata: MemberCommand,
    input: { memberId: string; acceptingJobs: boolean },
    actor: MemberUser | null,
  ) {
    return this.owningTransaction(() => {
      const current =
        actor &&
        this.sqlite
          .prepare<[string], { status: string; authEpoch: string | null }>(
            'SELECT status,auth_epoch AS authEpoch FROM members WHERE id=?',
          )
          .get(actor.id);
      assertCurrentOwnerCommand(metadata, actor, current, this.env);
      if (
        !metadata.actorId ||
        !input ||
        typeof input.memberId !== 'string' ||
        !input.memberId ||
        typeof input.acceptingJobs !== 'boolean' ||
        Object.keys(input).some((key) => !['memberId', 'acceptingJobs'].includes(key))
      ) {
        fail(400, '成员接单命令无效');
      }
      const command = {
          ...metadata,
          fingerprint: domainDigest({ command: metadata.fingerprint, input }),
        },
        prior = readRelationalReceipt(this.sqlite, command);
      if (prior) {
        return prior;
      }
      const member = this.readMember(input.memberId);
      if (!member || member.status === 'removed') {
        fail(404, '成员不存在');
      }
      const jobs = this.sqlite
          .prepare<[string], { id: string; requestId: string; memberId: string; status: string }>(
            `SELECT j.id,j.request_id AS requestId,r.member_id AS memberId,j.status
        FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id
        WHERE r.member_id=? AND j.status IN ('queued','pending') ORDER BY j.rowid`,
          )
          .all(input.memberId),
        view = {
          users: [
            {
              id: member.id,
              acceptingJobs: member.acceptingJobs === 1,
              jobAdmissionVersion: member.admissionEpoch,
            },
          ],
          jobs,
        };
      beginDomainCommand(view);
      const receipt = setMemberAdmission(
        view,
        input.memberId,
        input.acceptingJobs,
        (_db, job) => {
          if (
            this.sqlite
              .prepare(
                `SELECT 1 FROM charges WHERE original_job_id=?
          UNION ALL SELECT 1 FROM generation_attempts WHERE job_id=? LIMIT 1`,
              )
              .get(job.id, job.id)
          ) {
            fail(409, '等待意图不能持有预扣或派发尝试');
          }
        },
        metadata.recordedAt,
      );
      const original = finishDomainCommand(view)!;
      const events: RelationalEventInput[] = original.events.map((event) => {
        let fact: MemberFact | Extract<RelationalFact, { eventType: 'WaitingJobsCancelled' }>;
        if (event.eventType === 'AdmissionChanged') {
          fact = {
            eventType: 'AdmissionChanged',
            payload: {
              memberId: event.payload.memberId,
              acceptingJobs: event.payload.acceptingJobs,
              admissionEpoch: event.payload.jobAdmissionVersion,
            },
          };
        } else if (event.eventType === 'WaitingJobsCancelled') {
          fact = {
            eventType: 'WaitingJobsCancelled',
            payload: {
              jobIds: event.payload.jobIds,
              reason: event.payload.reason,
              errorStatus: event.payload.errorStatus,
              cancellationReason: event.payload.cancellationReason,
            },
          };
        } else {
          throw new Error('Admission owner emitted an unsupported fact.');
        }
        return {
          ...fact,
          streamId: event.eventType === 'AdmissionChanged' ? `member:${member.id}` : event.streamId,
          memberId: member.id,
          accountId: null,
          occurredAt: metadata.recordedAt,
          policyVersion: metadata.policyVersion,
          pricingVersion: metadata.pricingVersion,
        };
      });
      const expectedVersions: Record<string, string> = {};
      for (const event of events) {
        expectedVersions[event.streamId] =
          this.sqlite
            .prepare<[string], { version: string }>(
              'SELECT CAST(stream_version AS TEXT) AS version FROM stream_heads WHERE stream_id=?',
            )
            .get(event.streamId)?.version || '0';
      }
      const { policyVersion: _policy, pricingVersion: _pricing, ...saved } = command;
      return new RelationalCommitReplayer(this.sqlite).appendCommit({
        ...saved,
        expectedVersions,
        receipt,
        events,
      });
    });
  }

  assertMemberAdmissionCommit(events: RelationalEvent[], receipt: RelationalCommand['receipt']) {
    this.assertMemberCommandTransaction();
    const decisions = new Map<
      string,
      Extract<RelationalEvent, { eventType: 'AdmissionChanged' }>
    >();
    for (const event of events) {
      if (event.eventType === 'AdmissionChanged') {
        decisions.set(event.payload.memberId, event);
      } else if (
        event.eventType === 'WaitingJobsCancelled' &&
        event.payload.cancellationReason === 'member_stopped'
      ) {
        const decision = event.memberId ? decisions.get(event.memberId) : undefined;
        if (
          !decision ||
          decision.payload.acceptingJobs ||
          !event.payload.jobIds.length ||
          event.payload.reason !== MEMBER_STOPPED_MESSAGE ||
          event.payload.errorStatus !== 409 ||
          event.accountId !== null ||
          event.occurredAt !== decision.occurredAt
        ) {
          fail(409, '停止接单取消必须保留同提交原停止决定');
        }
      }
    }
    // 原接单回执的判别字段绑定同一保存决定，不能删光事实后保存“成功停止”。
    if (
      receipt &&
      !Array.isArray(receipt) &&
      typeof receipt === 'object' &&
      ('acceptingJobs' in receipt || 'cancelledJobIds' in receipt)
    ) {
      const admission = events.filter((event) => event.eventType === 'AdmissionChanged');
      const cancelled = events.flatMap((event) =>
        event.eventType === 'WaitingJobsCancelled' &&
        event.memberId === receipt.id &&
        event.payload.cancellationReason === 'member_stopped'
          ? event.payload.jobIds
          : [],
      );
      if (
        admission.length !== 1 ||
        admission[0].payload.memberId !== receipt.id ||
        admission[0].payload.acceptingJobs !== receipt.acceptingJobs ||
        canonicalJson(cancelled) !== canonicalJson(receipt.cancelledJobIds)
      ) {
        fail(409, '接单回执必须匹配原完整接单决定');
      }
    }
  }

  private assertAdmissionChange(
    event: Extract<RelationalEvent, { eventType: 'AdmissionChanged' }>,
    completeEvents: RelationalEvent[],
  ) {
    const p = event.payload,
      member = this.readMember(p.memberId);
    if (
      !member ||
      member.status === 'removed' ||
      event.streamId !== `member:${p.memberId}` ||
      event.memberId !== p.memberId ||
      event.accountId !== null ||
      !Number.isSafeInteger(
        member.admissionEpoch + (member.acceptingJobs === 1 && !p.acceptingJobs ? 1 : 0),
      ) ||
      p.admissionEpoch !==
        member.admissionEpoch + (member.acceptingJobs === 1 && !p.acceptingJobs ? 1 : 0)
    ) {
      fail(409, '接单事实与原成员或代次不符');
    }
    if (p.acceptingJobs) {
      return;
    }
    const waiting = this.sqlite
        .prepare<[string], { id: string; requestId: string }>(
          `SELECT j.id,j.request_id AS requestId FROM generation_jobs j JOIN generation_requests r ON r.id=j.request_id
      WHERE r.member_id=? AND j.status IN ('queued','pending')`,
        )
        .all(p.memberId),
      expected = new Map(waiting.map((job) => [job.id, job.requestId]));
    for (const candidate of completeEvents.filter((item) => item.eventIndex > event.eventIndex)) {
      if (candidate.eventType === 'AdmissionChanged' && candidate.payload.memberId === p.memberId) {
        break;
      }
      if (
        candidate.eventType !== 'WaitingJobsCancelled' ||
        candidate.memberId !== p.memberId ||
        candidate.payload.cancellationReason !== 'member_stopped'
      ) {
        continue;
      }
      if (candidate.payload.errorStatus !== 409 || candidate.accountId !== null) {
        fail(409, '停止接单取消事实无效');
      }
      for (const id of candidate.payload.jobIds) {
        if (!expected.has(id) || candidate.streamId !== `request:${expected.get(id)}`) {
          fail(409, '停止接单取消集合与原等待意图不符');
        }
        expected.delete(id);
      }
    }
    if (expected.size) {
      fail(409, '停止接单缺少原等待意图的取消事实');
    }
  }

  private applyMemberEvents(events: RelationalEvent[], completeEvents = events) {
    // 资格事实保留历史；业务回放不能把它还原成当前私有授权或认证 epoch。
    for (const event of events) {
      if (event.eventType === 'MemberRegistered' || event.eventType === 'OpeningMemberRecorded') {
        const p = event.payload;
        this.sqlite
          .prepare(
            `INSERT INTO members(id,display_name,status,accepting_jobs,auth_epoch,admission_epoch,created_at,source_event_id)
          VALUES(?,?,?,?,NULL,?,?,?)`,
          )
          .run(
            p.memberId,
            p.displayName,
            p.status,
            Number(p.acceptingJobs),
            p.admissionEpoch,
            p.createdAt,
            event.eventId,
          );
      } else if (event.eventType === 'MemberStatusChanged') {
        this.sqlite
          .prepare('UPDATE members SET status=?,source_event_id=? WHERE id=?')
          .run(event.payload.status, event.eventId, event.payload.memberId);
      } else if (event.eventType === 'AdmissionChanged') {
        this.assertAdmissionChange(event, completeEvents);
        this.sqlite
          .prepare(
            'UPDATE members SET accepting_jobs=?,admission_epoch=?,source_event_id=? WHERE id=?',
          )
          .run(
            Number(event.payload.acceptingJobs),
            event.payload.admissionEpoch,
            event.eventId,
            event.payload.memberId,
          );
      } else if (event.eventType !== 'MemberEligibilityChanged') {
        throw new Error("Member replay cannot apply another owner's facts.");
      }
    }
  }

  applyReplayedMemberEvent(event: RelationalEvent, completeEvents: RelationalEvent[]) {
    this.assertMemberCommandTransaction();
    this.applyMemberEvents([event], completeEvents);
    if (
      (event.eventType === 'MemberStatusChanged' && event.payload.status !== 'active') ||
      (event.eventType === 'MemberEligibilityChanged' && event.payload.eligible === false)
    ) {
      // 重放只撤销，不制造 epoch；后续 active/资格恢复不能复活旧 Cookie 或 Key。
      this.replaceAuthenticationEpoch(event.payload.memberId, null);
    }
  }

  replayMemberCommit(stored: StoredRelationalCommit) {
    return this.owningTransaction(() => {
      if (stored.events.some((event) => !relationalMemberFacts.has(event.eventType))) {
        throw new Error('Member replay requires complete member-only commits.');
      }
      const result = replayRelationalCommit(this.sqlite, stored);
      if (!result.replayed) {
        this.assertMemberAdmissionCommit(result.events, result.receipt);
        for (const event of result.events) {
          this.applyReplayedMemberEvent(event, result.events);
        }
      }
      return result;
    });
  }

  createMemberDiscordInvite(memberId: string, now = Date.now()) {
    return this.owningTransaction(() => {
      const user = this.authenticationMember(memberId);
      const result = createDiscordInvite({ users: user ? [user] : [] }, memberId, now);
      this.saveDiscordAuthentication(user!);
      return result;
    });
  }

  private saveDiscordAuthentication(user: DiscordUser, evidence?: 'login' | 'observation') {
    this.assertMemberCommandTransaction();
    const current = this.authenticationMember(user.id);
    if (!current || current.enabled !== true) {
      fail(401, '成员认证状态已变化，请重试');
    }
    const grant = user.discord;
    if (grant?.id) {
      this.sqlite
        .prepare(
          `INSERT INTO discord_bindings
        (member_id,discord_id,guild_id,access_token,refresh_token,expires_at,checked_at,eligible)
        VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(member_id) DO UPDATE SET
        discord_id=excluded.discord_id,guild_id=excluded.guild_id,access_token=excluded.access_token,
        refresh_token=excluded.refresh_token,expires_at=excluded.expires_at,
        checked_at=excluded.checked_at,eligible=excluded.eligible`,
        )
        .run(
          user.id,
          grant.id,
          grant.guildId,
          grant.accessToken ?? null,
          grant.refreshToken ?? null,
          grant.expiresAt ?? null,
          grant.checkedAt ?? null,
          grant.member === undefined ? null : Number(grant.member),
        );
    }
    if (evidence === 'login') {
      // 只清空既有密码并保留用户名占用；展示名回退值不是新的密码身份。
      this.sqlite
        .prepare('UPDATE password_credentials SET salt=NULL,password_hash=NULL WHERE member_id=?')
        .run(user.id);
    }
    if (current.memberAuth?.version !== user.memberAuth?.version) {
      this.replaceAuthenticationEpoch(user.id, user.memberAuth?.version ?? null);
    }
    if (user.discordInvite) {
      this.sqlite
        .prepare(
          `INSERT INTO discord_invites(member_id,code_hash,expires_at) VALUES(?,?,?)
        ON CONFLICT(member_id) DO UPDATE SET code_hash=excluded.code_hash,expires_at=excluded.expires_at`,
        )
        .run(user.id, user.discordInvite.hash, user.discordInvite.expiresAt);
    } else {
      this.sqlite.prepare('DELETE FROM discord_invites WHERE member_id=?').run(user.id);
    }
    if (evidence) {
      const payload = {
        memberId: user.id,
        discordId: grant!.id!,
        guildId: grant!.guildId!,
        checkedAt: grant!.checkedAt!,
        eligible: grant!.member ?? null,
      };
      this.commitMember(
        {
          commandId: `discord-auth:${randomBytes(16).toString('hex')}`,
          fingerprint: domainDigest(payload),
          actorKind: 'system',
          actorId: null,
          recordedAt: Date.now(),
          correlationId: null,
          causationId: null,
          policyVersion: 'member-auth-v1',
          pricingVersion: 'not-priced',
        },
        user.id,
        { eventType: 'MemberEligibilityChanged', payload },
      );
    }
  }

  async discordAuthenticationTransaction<T>(
    mutate: (transaction: DiscordAuthenticationTransaction) => T,
  ): Promise<T> {
    return this.authenticationTransaction(() =>
      mutate({
        readMember: (id) => this.authenticationMember(id),
        findDiscordMember: (id) => {
          const row = this.sqlite
            .prepare<[string], { member_id: string }>(
              'SELECT member_id FROM discord_bindings WHERE discord_id=?',
            )
            .get(id);
          return row ? this.authenticationMember(row.member_id) : null;
        },
        findInviteMember: (hash, now) => {
          const row = this.sqlite
            .prepare<[string, number], { member_id: string }>(
              'SELECT member_id FROM discord_invites WHERE code_hash=? AND expires_at>?',
            )
            .get(hash, now);
          return row ? this.authenticationMember(row.member_id) : null;
        },
        saveAuthentication: (user, evidence) => this.saveDiscordAuthentication(user, evidence),
      }),
    );
  }

  readMemberSessionState(tokenHash: string): MemberSession | null {
    return (
      this.sqlite
        .prepare<[string], MemberSession>(
          `SELECT token_hash AS tokenHash,member_id AS memberId,
      auth_epoch AS authEpoch,owner_binding AS ownerBinding,created_at AS createdAt,expires_at AS expiresAt
      FROM member_sessions WHERE token_hash=?`,
        )
        .get(tokenHash) || null
    );
  }

  private readOwner(): SessionOwner | null {
    return (
      this.sqlite
        .prepare<[], SessionOwner>(
          'SELECT owner_member_id AS ownerUserId,guild_id AS guildId FROM member_auth_state WHERE id=1',
        )
        .get() || null
    );
  }

  private authenticationTransaction<T>(run: () => T): T {
    if (this.sqlite.inTransaction) {
      throw new Error('Authentication requires its own commit before returning success.');
    }
    return this.sqlite.transaction(run).immediate();
  }

  async memberApiKeyTransaction<T>(
    keyHash: string,
    authorize: (member: MemberUser | null) => T,
  ): Promise<T> {
    return this.authenticationTransaction(() => {
      const row = this.sqlite
        .prepare<[string], { member_id: string; recoverable_key: string }>(
          'SELECT member_id,recoverable_key FROM api_keys WHERE key_hash=?',
        )
        .get(keyHash);
      const member =
        row && digest(row.recoverable_key) === keyHash
          ? this.authenticationMember(row.member_id)
          : null;
      return authorize(member);
    });
  }

  async memberSessionTransaction<T>(
    mutate: (transaction: MemberSessionTransaction) => T,
    options: MemberSessionMutation,
  ): Promise<T> {
    return this.authenticationTransaction(() => {
      const result = mutate({
        readMember: (id) => this.authenticationMember(id),
        readSession: (hash) => this.readMemberSessionState(hash),
        readOwner: () => this.readOwner(),
      });
      const owner = options.memberSessionOwner;
      if (owner) {
        const previous = this.readOwner();
        if (
          !previous ||
          previous.ownerUserId !== owner.ownerUserId ||
          previous.guildId !== owner.guildId
        ) {
          if (previous) {
            this.sqlite
              .prepare(
                'DELETE FROM member_sessions WHERE owner_binding IS NOT NULL OR member_id IN (?,?)',
              )
              .run(previous.ownerUserId, owner.ownerUserId);
          }
          this.sqlite
            .prepare(
              `INSERT INTO member_auth_state(id,owner_member_id,guild_id) VALUES(1,?,?)
            ON CONFLICT(id) DO UPDATE SET owner_member_id=excluded.owner_member_id,guild_id=excluded.guild_id`,
            )
            .run(owner.ownerUserId, owner.guildId);
        }
      }
      const remove =
        typeof options.memberSessionDelete === 'function'
          ? options.memberSessionDelete()
          : options.memberSessionDelete;
      if (remove) {
        this.sqlite.prepare('DELETE FROM member_sessions WHERE token_hash=?').run(remove);
      }
      const row = options.memberSession?.();
      if (row) {
        this.sqlite.prepare('DELETE FROM member_sessions WHERE expires_at<=?').run(row.createdAt);
        this.sqlite
          .prepare('DELETE FROM member_sessions WHERE member_id=? AND auth_epoch!=?')
          .run(row.memberId, row.authEpoch);
        const count = this.sqlite
          .prepare<[number], { count: number }>(
            'SELECT COUNT(*) AS count FROM member_sessions WHERE expires_at>?',
          )
          .get(row.createdAt)!.count;
        if (count >= 10000) {
          fail(503, '登录会话已满，请稍后重试');
        }
        this.sqlite
          .prepare(
            'INSERT INTO member_sessions(token_hash,member_id,auth_epoch,owner_binding,created_at,expires_at) VALUES(?,?,?,?,?,?)',
          )
          .run(
            row.tokenHash,
            row.memberId,
            row.authEpoch,
            row.ownerBinding,
            row.createdAt,
            row.expiresAt,
          );
      }
      const expiry = options.memberSessionExpiry?.();
      if (
        expiry &&
        this.sqlite
          .prepare('UPDATE member_sessions SET expires_at=? WHERE token_hash=?')
          .run(expiry.expiresAt, expiry.tokenHash).changes !== 1
      ) {
        fail(401, '会话已失效，请重新登录');
      }
      return result;
    });
  }
}
