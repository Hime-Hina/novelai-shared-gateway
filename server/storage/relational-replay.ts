import type Database from 'better-sqlite3';
import { canonicalJson } from '../events/domain-events.ts';
import {
  appendRelationalCommit,
  replayRelationalCommit,
  readRelationalCommit,
  type RelationalCommitResult,
  type StoredRelationalCommit,
} from '../events/relational-event-store.ts';
import type { RelationalCommand } from '../events/relational-events.ts';
import { RelationalMemberStore, relationalMemberFacts } from './relational-member-store.ts';
import { RelationalInputStore, relationalInputFacts } from './relational-input-store.ts';
import {
  RelationalGenerationStore,
  relationalGenerationFacts,
} from './relational-generation-store.ts';
import { RelationalQuotaStore, relationalQuotaFacts } from './relational-quota-store.ts';
import { RelationalAccountStore, relationalAccountFacts } from './relational-account-store.ts';
import { relationalPrivateStateEmpty } from './relational-private-restore.ts';
import { RelationalReplicaStore, relationalReplicaFacts } from './relational-replica-store.ts';

export function relationalReplayFacts() {
  return new Set([
    ...relationalMemberFacts,
    ...relationalInputFacts,
    ...relationalGenerationFacts,
    ...relationalQuotaFacts,
    ...relationalAccountFacts,
    ...relationalReplicaFacts,
  ]);
}

function assertSupported(events: StoredRelationalCommit['events']) {
  const supported = relationalReplayFacts();
  if (events.some((event) => !supported.has(event.eventType))) {
    throw new Error('Business projection has no owner for this complete commit.');
  }
}

// 完整提交只保存一次；各 owner 保留原校验，按原事件顺序在同一保存点应用。
export class RelationalCommitReplayer {
  private sqlite: Database.Database;
  private members: RelationalMemberStore;
  private inputs: RelationalInputStore;
  private generations: RelationalGenerationStore;
  private quotas: RelationalQuotaStore;
  private accounts: RelationalAccountStore;
  private replicas: RelationalReplicaStore;

  constructor(sqlite: Database.Database, generations?: RelationalGenerationStore) {
    this.sqlite = sqlite;
    this.members = new RelationalMemberStore(sqlite);
    this.inputs = new RelationalInputStore(sqlite);
    this.generations = generations ?? new RelationalGenerationStore(sqlite);
    this.quotas = new RelationalQuotaStore(sqlite);
    // 离线回放没有物理执行器；账户投影不运行授权、网络查询或派发命令。
    this.accounts = new RelationalAccountStore(sqlite, this.quotas, new Map());
    this.replicas = new RelationalReplicaStore(sqlite);
  }

  #applyCommit(result: RelationalCommitResult, sourceInputs?: RelationalInputStore) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Business projection requires the owning command transaction.');
    }
    return this.sqlite.transaction(() => {
      const stored = readRelationalCommit(this.sqlite, result.commitSeq);
      if (
        typeof result.replayed !== 'boolean' ||
        canonicalJson(result.receipt) !== canonicalJson(stored.command.receipt) ||
        (result.replayed
          ? result.events.length !== 0
          : canonicalJson(result.events) !== canonicalJson(stored.events))
      ) {
        throw new Error('Business projection requires the saved complete original commit.');
      }
      assertSupported(stored.events);
      const hasInput = stored.events.some((event) => relationalInputFacts.has(event.eventType));
      const hasDiagnostic = stored.events.some((event) => event.eventType === 'DiagnosticRecorded');
      if ((hasInput || hasDiagnostic) && !sourceInputs) {
        throw new Error('Protected content replay requires the matching protected source.');
      }
      const applyInput = hasInput
        ? this.inputs.createInputProjector(sourceInputs!, stored, result.replayed)
        : null;
      const applyDiagnostic = hasDiagnostic
        ? this.inputs.createDiagnosticProjector(sourceInputs!, stored, result.replayed)
        : null;
      if (result.replayed) {
        if (applyInput) {
          for (const event of stored.events) {
            if (relationalInputFacts.has(event.eventType)) {
              applyInput(event);
            }
          }
        }
        for (const event of stored.events) {
          if (event.eventType === 'DiagnosticRecorded') {
            applyDiagnostic!(event);
          }
        }
        return result;
      }
      if (
        stored.events.some(
          (event) =>
            event.eventType === 'ImportSourceRegistered' || event.eventType.startsWith('Opening'),
        ) &&
        this.sqlite
          .prepare<[], { status: string }>('SELECT status FROM projection_state WHERE id=1')
          .get()?.status !== 'building'
      ) {
        throw new Error('Opening facts require an offline building target.');
      }
      this.members.assertMemberAdmissionCommit(stored.events, stored.command.receipt);
      const applyGeneration = this.generations.createGenerationProjector(stored.events),
        applyQuota = this.quotas.createQuotaProjector(stored.events);
      for (const event of stored.events) {
        if (relationalMemberFacts.has(event.eventType)) {
          this.members.applyReplayedMemberEvent(event, stored.events);
        } else if (relationalInputFacts.has(event.eventType)) {
          applyInput!(event);
        } else if (relationalGenerationFacts.has(event.eventType)) {
          if (event.eventType === 'DiagnosticRecorded') {
            applyDiagnostic!(event);
          }
          applyGeneration(event);
        } else if (relationalQuotaFacts.has(event.eventType)) {
          applyQuota(event);
        } else if (relationalReplicaFacts.has(event.eventType)) {
          this.replicas.applyReplicaEvents([event]);
        } else {
          this.accounts.applyAccountEvent(event);
        }
      }
      this.quotas.assertGroupCommit(stored.events, stored.command.receipt);
      return result;
    })();
  }

  appendCommit(command: RelationalCommand, sourceInputs?: RelationalInputStore) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Business append requires the owning command transaction.');
    }
    return this.sqlite.transaction(() =>
      this.#applyCommit(appendRelationalCommit(this.sqlite, command), sourceInputs),
    )();
  }

  // 保留原生成 owner 的窄回放边界；全业务回放仍要求没有当前私有认证。
  replayGenerationCommit(stored: StoredRelationalCommit, sourceInputs?: RelationalInputStore) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Generation replay requires the owning import transaction.');
    }
    if (
      stored.events.some(
        (event) =>
          !relationalGenerationFacts.has(event.eventType) &&
          !relationalReplicaFacts.has(event.eventType) &&
          !(event.eventType === 'QuotaStatusChanged' && event.payload.generationFailure !== null),
      )
    ) {
      throw new Error('Generation replay requires complete generation/accounting commits.');
    }
    return this.sqlite.transaction(() =>
      this.#applyCommit(replayRelationalCommit(this.sqlite, stored), sourceInputs),
    )();
  }

  replayCommit(stored: StoredRelationalCommit, sourceInputs?: RelationalInputStore) {
    if (!this.sqlite.inTransaction) {
      throw new Error('Business replay requires the owning offline transaction.');
    }
    return this.sqlite.transaction(() => {
      assertSupported(stored.events);
      const result = replayRelationalCommit(this.sqlite, stored);
      // 私有快照在最终业务切点恢复；不能从带当前认证的 checkpoint 继续推进历史。
      if (!result.replayed && !relationalPrivateStateEmpty(this.sqlite)) {
        throw new Error(
          'New business replay requires a checkpoint without private authentication.',
        );
      }
      return this.#applyCommit(result, sourceInputs);
    })();
  }
}
