import { relationalStateNotificationsFor } from '../events/relational-state-notifications.ts';
import { emitStoreEvent } from '../events/state-notifications.ts';
import { randomUUID } from 'node:crypto';
import type { EventEmitter } from 'node:events';
import type Database from 'better-sqlite3';
import { domainDigest } from '../events/domain-events.ts';
import type { RelationalCommitResult } from '../events/relational-event-store.ts';
import {
  RelationalGenerationStore,
  type GenerationPhysicalCompletion,
} from '../storage/relational-generation-store.ts';
import { RelationalInputStore } from '../storage/relational-input-store.ts';
import { assertRelationalStorageReady } from '../storage/relational-storage.ts';
import { recoverRelationalGenerationResults } from '../storage/relational-result-recovery.ts';
import { describeStoredResult, publishStoredResult } from '../storage/result-files.ts';
import { generateNovelAiImage } from '../protocols/novelai-client.ts';
import { isAbortError } from '../http/public-errors.ts';
import { sharedAccountReadyDelay, sharedAccountConcurrency } from '../quota/shared-capacity.ts';
import { generationResponseEvidence } from './accounting.ts';
import { sharedFailureKind, type GenerationTiming } from './execution.ts';
import { JobPreviews } from './job-previews.ts';
import {
  createGenerationControls,
  physicalAccountLoad,
  runningJobTimeoutDelay,
  type RunningControl,
} from './queue-dispatch.ts';

type State = ReturnType<RelationalGenerationStore['readGenerationExecutionState']>;
type Metadata = Parameters<RelationalGenerationStore['dispatchGeneration']>[0];
type Response = Parameters<RelationalGenerationStore['recordGenerationResponse']>[2];
type Dispatch = NonNullable<
  ReturnType<RelationalGenerationStore['dispatchGeneration']>['dispatch']
>;
type Active = {
  state: State;
  dispatch: Dispatch;
  control: RunningControl;
  timing: GenerationTiming;
  phase: 'preflight' | 'upstream' | 'commit';
};
export function createRelationalQueueRuntime(options: {
  sqlite: Database.Database;
  dataDir: string;
  generations: RelationalGenerationStore;
  inputs: RelationalInputStore;
  controls: Map<string, RunningControl>;
  events: EventEmitter;
  previews?: JobPreviews;
  env?: NodeJS.ProcessEnv;
  now?(): number;
  onError?(event: { code: string; jobId?: string }): void;
}) {
  const { sqlite, generations: store, inputs } = options;
  const notifications = relationalStateNotificationsFor(sqlite, options.events);
  if (!(options.controls instanceof Map) || typeof options.events?.emit !== 'function') {
    throw new TypeError('Shared physical controls and commit events are required.');
  }
  const env = options.env ?? process.env,
    now = options.now ?? Date.now;
  const previews = options.previews ?? new JobPreviews();
  const active = new Map<string, Active>();
  let phase: 'created' | 'starting' | 'prepared' | 'running' | 'stopping' | 'stopped' | 'faulted' =
    'created';
  let timer: ReturnType<typeof setTimeout> | null = null,
    scheduledAt = 0,
    ticking = false;
  let preparation: Promise<void> | null = null,
    startup: Promise<void> | null = null;
  let shutdown: Promise<void> | null = null;
  const controls = createGenerationControls({
    controls: options.controls,
    timeoutDelay: (job) => runningJobTimeoutDelay(job, env, now()),
    schedule: () => schedule(),
  });
  const report = (error: unknown, jobId?: string) => {
    const code = String((error as { code?: unknown })?.code ?? 'GENERATION_RUNTIME_FAILED');
    try {
      options.onError?.({
        code: /^[A-Z][A-Z0-9_]{0,63}$/.test(code) ? code : 'GENERATION_RUNTIME_FAILED',
        ...(jobId ? { jobId } : {}),
      });
    } catch {}
  };
  const metadata = (state: State, name: string, intent: object): Metadata => ({
    commandId: `executor:${name}:${state.attemptId ?? state.id}`,
    fingerprint: domainDigest({ jobId: state.id, ...intent }),
    actorKind: 'system',
    actorId: null,
    recordedAt: now(),
    correlationId: null,
    causationId: state.eventId,
    policyVersion: state.policyVersion,
    pricingVersion: state.pricingVersion,
  });
  const response = (run: Active): Response => {
    const evidence = generationResponseEvidence(
      { id: run.state.id, ...run.timing },
      { requestStarted: !!run.timing.upstreamStartedAt },
    );
    const timestamp = (value: string | null) => (value === null ? null : Date.parse(value));
    return {
      dispatchState: evidence.dispatchState,
      upstreamStartedAt: timestamp(evidence.upstreamStartedAt),
      upstreamHeadersAt: timestamp(evidence.upstreamHeadersAt),
      upstreamCompletedAt: timestamp(evidence.upstreamCompletedAt),
      upstreamHttpStatus: evidence.upstreamHttpStatus,
    };
  };
  function clearTimer() {
    if (timer) {
      clearTimeout(timer);
    }
    timer = null;
    scheduledAt = 0;
  }
  function fault(error: unknown, jobId?: string) {
    phase = 'faulted';
    clearTimer();
    report(error, jobId);
  }
  function schedule(delay = 0) {
    if (phase !== 'running') {
      return;
    }
    const at = now() + Math.max(0, delay);
    if (timer && at >= scheduledAt) {
      return;
    }
    clearTimer();
    scheduledAt = at;
    timer = setTimeout(
      () => {
        timer = null;
        scheduledAt = 0;
        tick();
      },
      Math.max(0, delay),
    );
    timer.unref();
  }
  function fail(
    state: State,
    name: string,
    code: string,
    status: number,
    aborted: boolean,
    run?: Active,
    completion?: GenerationPhysicalCompletion,
    error?: unknown,
  ) {
    notifications.commit(() =>
      sqlite
        .transaction(() => {
          const results: RelationalCommitResult[] = [];
          if (run) {
            const observed = response(run);
            results.push(
              store.recordGenerationResponse(
                metadata(state, `${name}-response`, { observed, completion: completion ?? null }),
                state.id,
                observed,
                completion,
              ),
            );
          }
          const failure = {
            errorCode: code,
            errorStatus: status,
            aborted,
            ...(run ? { evidence: { requestStarted: !!run.timing.upstreamStartedAt } } : {}),
            ...(completion ? { completion } : {}),
            ...(run ? { diagnostic: { dispatch: run.dispatch, error } } : {}),
          };
          results.push(
            store.failGeneration(
              metadata(state, name, { code, status, aborted, completion: completion ?? null }),
              state.id,
              failure,
            ),
          );
          return results;
        })
        .immediate(),
    );
    previews.clear(state.id);
  }
  async function runGeneration(run: Active) {
    let descriptor: ReturnType<typeof describeStoredResult> | null = null,
      failure: unknown;
    try {
      if (
        store.readGenerationExecutionState(run.state.id).status !== 'running' ||
        run.control.controller.signal.aborted
      ) {
        throw new Error('Generation stopped before upstream dispatch.');
      }
      const request = inputs.readJobInput(run.state.id);
      run.phase = 'upstream';
      const image = await generateNovelAiImage(
        request,
        { token: run.dispatch.pst, proxyUrl: run.dispatch.proxyUrl },
        env,
        {
          signal: run.control.controller.signal,
          onUpstreamTiming: (event) => Object.assign(run.timing, event),
          onProgress: (progress) =>
            previews.update(
              {
                id: run.state.id,
                userToken: run.state.memberId,
                status: store.readGenerationExecutionState(run.state.id).status,
              },
              progress,
            ),
        },
      );
      run.phase = 'commit';
      descriptor = describeStoredResult(randomUUID(), run.state.operation, image);
      notifications.commit(() =>
        sqlite
          .transaction(() =>
            store.prepareGenerationResult(
              metadata(run.state, 'prepare-result', { descriptor, response: response(run) }),
              run.state.id,
              descriptor!,
              response(run),
            ),
          )
          .immediate(),
      );
      await publishStoredResult(options.dataDir, descriptor, image.buffer);
    } catch (error) {
      failure = error;
    } finally {
      try {
        const current = store.readGenerationExecutionState(run.state.id);
        if (failure && run.phase === 'commit') {
          // 已取得完整产物后的本地失败保留准备/原预扣；重启先恢复，不能冒充新的上游失败。
          fault(failure, run.state.id);
        } else if (descriptor && !failure) {
          notifications.commit(() =>
            sqlite
              .transaction(() =>
                store.completeGeneration(
                  metadata(run.state, 'complete-result', { descriptor }),
                  run.state.id,
                  descriptor!,
                ),
              )
              .immediate(),
          );
        } else {
          const kind = sharedFailureKind(
            { failurePhase: run.phase, upstreamTiming: run.timing },
            failure,
          );
          const completion: GenerationPhysicalCompletion = {
            attemptId: run.dispatch.attemptId,
            credentialEpoch: run.dispatch.credentialEpoch,
            kind,
            ...(run.timing.upstreamRetryAfterUntil
              ? { retryAfterUntil: run.timing.upstreamRetryAfterUntil }
              : {}),
          };
          if (current.status === 'running') {
            const aborted = run.control.controller.signal.aborted || isAbortError(failure);
            let status = 429;
            if (kind !== 'capacity') {
              status = (failure as { statusCode?: number })?.statusCode || (aborted ? 504 : 500);
            }
            fail(
              current,
              'upstream-failed',
              kind === 'capacity' ? 'upstream_capacity' : 'upstream_failed',
              status,
              aborted,
              run,
              completion,
              failure,
            );
          } else {
            const observed = response(run);
            notifications.commit(() =>
              sqlite
                .transaction(() =>
                  store.recordGenerationResponse(
                    metadata(current, 'physical-ended', { observed, completion }),
                    current.id,
                    observed,
                    completion,
                  ),
                )
                .immediate(),
            );
          }
        }
      } catch (error) {
        fault(error, run.state.id);
      } finally {
        active.delete(run.state.id);
        previews.clear(run.state.id);
        controls.finishRunningJobControl(run.state.id, run.control);
      }
    }
  }
  function expire() {
    const ids = new Set(store.readExpiredGenerationIds(now()));
    for (const [jobId, run] of active) {
      if (run.control.controller.signal.aborted) {
        ids.add(jobId);
      }
    }
    for (const id of ids) {
      const state = store.readGenerationExecutionState(id);
      if (!['queued', 'pending', 'running'].includes(state.status)) {
        continue;
      }
      const control = active.get(id)?.control;
      if (control && !control.controller.signal.aborted) {
        control.controller.abort(new Error('Generation deadline expired.'));
      }
      fail(
        state,
        'expired',
        'generation_expired',
        504,
        control?.controller.signal.aborted ?? false,
        active.get(id),
        undefined,
        control?.controller.signal.reason,
      );
    }
    for (const id of store.readDueGenerationReviewIds(now())) {
      const state = store.readGenerationExecutionState(id);
      notifications.commit(() =>
        sqlite
          .transaction(() => store.closeGenerationReview(metadata(state, 'review-close', {}), id))
          .immediate(),
      );
    }
  }
  function tick() {
    if (phase !== 'running' || ticking) {
      return;
    }
    clearTimer();
    ticking = true;
    try {
      expire();
      for (const id of store.readQueuedGenerationIds()) {
        if (phase !== 'running') {
          break;
        }
        const state = store.readGenerationExecutionState(id),
          account = store.readBoundGenerationAccount();
        if (
          !account?.enabled ||
          sharedAccountReadyDelay(account, now()) > 0 ||
          physicalAccountLoad(controls.controls, account.id) >= sharedAccountConcurrency()
        ) {
          continue;
        }
        const accountId = account.id;
        try {
          const versions = store.readMemberGenerationCapabilities(state.memberId).versions;
          const meta = {
            ...metadata(state, 'dispatch', { accountId }),
            policyVersion: versions.billingPolicy,
            pricingVersion: versions.pricing,
          };
          const result = notifications.commit(() =>
            sqlite
              .transaction(() =>
                store.dispatchGeneration(
                  meta,
                  { jobId: id, accountId, attemptId: randomUUID() },
                  {
                    dispatchEnabled: () => phase === 'running',
                    physicalAccountLoad: (id) => physicalAccountLoad(controls.controls, id),
                  },
                ),
              )
              .immediate(),
          );
          if (result.dispatch) {
            const control = controls.createRunningJobControl({ ...state, accountId });
            const run: Active = {
              state: store.readGenerationExecutionState(id),
              dispatch: result.dispatch,
              control,
              timing: {},
              phase: 'preflight',
            };
            active.set(id, run);
            void runGeneration(run);
          }
        } catch (error) {
          const code = (error as { code?: string }).code,
            status = (error as { statusCode?: number }).statusCode;
          if (!Number.isInteger(status) || status! < 400 || status! > 599) {
            throw error;
          }
          fail(
            state,
            'dispatch-failed',
            code && /^[a-z_]{1,64}$/.test(code) ? code : 'dispatch_failed',
            status!,
            false,
          );
        }
      }
    } catch (error) {
      fault(error);
    } finally {
      ticking = false;
      const wake = phase === 'running' ? store.readNextGenerationWakeAt() : null;
      if (phase === 'running' && (wake !== null || controls.controls.size)) {
        schedule(wake === null ? 1000 : Math.max(1, Math.min(1000, wake - now())));
      }
    }
  }
  async function initialize() {
    try {
      assertRelationalStorageReady(sqlite);
      if (controls.controls.size) {
        throw new Error('A physical executor already owns this database.');
      }
      const recovered = await recoverRelationalGenerationResults(sqlite, options.dataDir, { now });
      if (recovered.fileMissing || recovered.unresolved) {
        throw new Error('Prepared results require recovery before dispatch.');
      }
      let afterId = '';
      for (;;) {
        const ids = store.readInterruptedGenerationIds(afterId);
        if (!ids.length) {
          break;
        }
        for (const id of ids) {
          afterId = id;
          fail(
            store.readGenerationExecutionState(id),
            'interrupted',
            'executor_interrupted',
            504,
            false,
          );
        }
      }
      expire();
      if (phase === 'starting') {
        assertRelationalStorageReady(sqlite);
        phase = 'prepared';
      }
    } catch (error) {
      fault(error);
      throw error;
    }
  }
  // 恢复只收尾原事实；监听前不建timer，也不取得新的派发资格。
  function prepare() {
    if (['stopping', 'stopped', 'faulted'].includes(phase)) {
      return Promise.reject(new Error('Executor cannot be restarted in place.'));
    }
    if (preparation) {
      return preparation;
    }
    phase = 'starting';
    preparation = initialize();
    return preparation;
  }
  function activate() {
    if (phase !== 'prepared' && phase !== 'running') {
      throw new Error('Executor must finish preparation before activation.');
    }
    try {
      // 恢复完成与监听成功之间可能等待；只在当前 ready 下开放原调度器。
      assertRelationalStorageReady(sqlite);
      if (phase === 'running') {
        return;
      }
      phase = 'running';
      schedule();
    } catch (error) {
      fault(error);
      throw error;
    }
  }
  return {
    controls: controls.controls,
    assertAccepting() {
      if (phase !== 'running') {
        throw Object.assign(new Error('服务正在关闭或尚未就绪，请稍后重试。'), {
          statusCode: 503,
          code: 'generation_unavailable',
        });
      }
    },
    previews,
    schedule,
    tick,
    activePayloadBytes: () => inputs.activePayloadBytes(controls.controls.values()),
    prepare,
    activate,
    start() {
      if (['stopping', 'stopped', 'faulted'].includes(phase)) {
        return Promise.reject(new Error('Executor cannot be restarted in place.'));
      }
      if (startup) {
        return startup;
      }
      // 现有调用者仍是一段式启动；sole 使用 prepare→成功监听→activate。
      startup = prepare().then(() => {
        if (phase === 'prepared') {
          activate();
        }
      });
      return startup;
    },
    stop() {
      if (shutdown) {
        return shutdown;
      }
      phase = 'stopping';
      clearTimer();
      shutdown = (async () => {
        if (preparation) {
          try {
            await preparation;
          } catch {}
        }
        await Promise.all([...controls.controls.values()].map((control) => control.done));
        phase = 'stopped';
      })();
      // 先登记关闭promise；同步close监听者重入stop也必须复用同一次关闭。
      emitStoreEvent(options.events, 'close');
      return shutdown;
    },
    get status() {
      return phase;
    },
  };
}

export type RelationalGenerationLifecycle = Pick<
  ReturnType<typeof createRelationalQueueRuntime>,
  'status' | 'assertAccepting'
>;
