import type { JsonStore, StoreItem, StoreDatabase } from './store.ts';
import { isStaleGenerationJob, type GenerationReservation, type GenerationQueueReservation } from './generation-execution.ts';
import { sharedAccountConcurrency, sharedAccountReadyDelay } from './shared-capacity.ts';
import { queuedDeadlineTimestamp } from './queue-policy.ts';

export interface RunningControl {
  controller: AbortController; jobId: string; requestId: string; payloadBytes: number; accountId: string;
  done: Promise<void>; resolveDone(): void; reason: string; timer: ReturnType<typeof setTimeout> | null;
  reservation?: GenerationReservation;
}
type PhysicalControls = ReadonlyMap<string, { accountId: string }>;
type Settings = Partial<StoreDatabase['settings']>;

export function physicalAccountLoad(controls: PhysicalControls, accountId: string) {
  let count = 0;
  for (const control of controls.values()) if (control.accountId === accountId) count++;
  return count;
}
export function availableSharedAccountSlots(accounts: StoreItem[], settings: Settings, controls: PhysicalControls, now = Date.now()) {
  const account = accounts.find(item => item.id === settings.fairQuota?.boundAccountId);
  return !account || account.enabled === false || sharedAccountReadyDelay(account, now) > 0 ? 0
    : Math.max(0, sharedAccountConcurrency() - Math.max(Number(account.inFlight || 0), physicalAccountLoad(controls, account.id)));
}
export function nextSharedAccountReadyDelay(accounts: StoreItem[], settings: Settings, controls: PhysicalControls, now = Date.now()) {
  const account = accounts.find(item => item.id === settings.fairQuota?.boundAccountId);
  return !account || account.enabled === false || Math.max(Number(account.inFlight || 0), physicalAccountLoad(controls, account.id)) >= sharedAccountConcurrency()
    ? 1000 : sharedAccountReadyDelay(account, now);
}

export function createGenerationControls(options: { timeoutDelay(job: Partial<StoreItem>): number; schedule(): void }) {
  const controls = new Map<string, RunningControl>();
  function abortRunningJob(jobId: string, reason = 'direct generate timeout') {
    const control = controls.get(jobId); if (!control) return false;
    control.reason = reason;
    if (!control.controller.signal.aborted) control.controller.abort(new Error(reason));
    return true;
  }
  function createRunningJobControl(job: Partial<StoreItem> = {}) {
    const controller = new AbortController(); let resolveDone = () => {};
    const control: RunningControl = { controller, jobId: job.id!, requestId: job.requestId || '', payloadBytes: job.payloadBytes || 0,
      accountId: job.accountId || '', done: new Promise<void>(resolve => { resolveDone = resolve; }), resolveDone, reason: '', timer: null };
    if (!job.id) return control;
    control.timer = setTimeout(() => abortRunningJob(job.id!, 'direct generate timeout'), options.timeoutDelay(job));
    controls.set(job.id, control); return control;
  }
  function finishRunningJobControl(jobId: string | undefined, control: RunningControl) {
    if (control.timer) clearTimeout(control.timer);
    if (jobId && controls.get(jobId) === control) controls.delete(jobId);
    control.resolveDone(); options.schedule();
  }
  return { controls, createRunningJobControl, finishRunningJobControl };
}

export function createQueueDispatcher(options: {
  store: Pick<JsonStore, 'update' | 'queuedJobIds'>;
  controls: PhysicalControls;
  enabled(): boolean;
  cleanup(reason: string): Promise<unknown>;
  reserve(jobId: string): Promise<GenerationQueueReservation>;
  run(reservation: GenerationQueueReservation): unknown;
  notify(jobId: string, result: { error?: string; statusCode?: number; cooldownUntil?: string }): void;
  now?: () => number;
  env?: NodeJS.ProcessEnv;
  log?(value: object): void;
  error?(value: unknown): void;
}) {
  const { store, controls } = options, now = options.now || Date.now;
  let timer: ReturnType<typeof setTimeout> | null = null, runAt = 0, draining = false, requested = false, lastWait = '';
  const slots = (accounts: StoreItem[], settings: Settings) => availableSharedAccountSlots(accounts, settings, controls, now());
  const readyDelay = (accounts: StoreItem[], settings: Settings) => nextSharedAccountReadyDelay(accounts, settings, controls, now());
  async function retryDelay() {
    return store.update(db => {
      const at = now(), expiries = db.jobs.map(job => queuedDeadlineTimestamp(job, options.env)).filter(Boolean).map(until => until - at);
      return Math.max(1000, Math.min(readyDelay(db.accounts, db.settings) || 25, ...expiries));
    }, { persist: false });
  }
  function schedule(delay = 0) {
    if (!options.enabled()) return;
    if (draining) { requested = true; return; }
    const next = now() + delay;
    if (timer) { if (next >= runAt) return; clearTimeout(timer); }
    runAt = next;
    timer = setTimeout(() => { timer = null; runAt = 0; void drain(next); }, delay);
  }
  async function drain(scheduledAt = now()) {
    if (!options.enabled()) return;
    if (draining) { requested = true; return; }
    draining = true; requested = false;
    try {
      await options.cleanup('queue expiry');
      const plan = await store.update(db => {
        if (!db.jobs.some(job => job.status === 'queued')) { lastWait = ''; return { jobIds: [] as string[], delay: 0 }; }
        const available = slots(db.accounts, db.settings);
        {
          const jobId = store.queuedJobIds()[0], account = db.accounts.find(item => item.id === db.settings.fairQuota?.boundAccountId);
          const physical = account ? physicalAccountLoad(controls, account.id) : 0;
          const reason = !account || account.enabled === false || db.settings.fairQuota?.status !== 'active' ? 'eligibility'
            : physical > 0 ? 'physical_inflight' : Number(account.inFlight || 0) > 0 ? 'account_inflight'
              : sharedAccountReadyDelay(account, now()) > 0 ? 'cooldown' : 'ready';
          const until = account && sharedAccountReadyDelay(account, now()) > 0 ? account.cooldownUntil : null;
          const signature = JSON.stringify([jobId, account?.id, reason, until]);
          if (signature !== lastWait) {
            lastWait = signature;
            options.log?.({ event: 'wait', jobId, accountId: account?.id, reason, until,
              cooldownReason: until ? account!.cooldownReason || null : null, scheduledAt, checkedAt: now() });
          }
        }
        // Crossing the cooldown boundary between reads must still preserve a wakeup.
        if (available <= 0) return { jobIds: [] as string[], delay: Math.max(1, readyDelay(db.accounts, db.settings)) };
        const active = new Set(db.jobs.filter(job => ['queued', 'running'].includes(job.status!) && !isStaleGenerationJob(job, now(), options.env)).map(job => job.id));
        return { jobIds: store.queuedJobIds().filter(id => active.has(id)).slice(0, available), delay: 0 };
      }, { collections: ['accounts'], persist: false });
      if (!plan.jobIds.length && plan.delay > 0) requested = true;
      const reservations = await Promise.all(plan.jobIds.map(id => options.reserve(id).catch(error => ({ error }))));
      for (const reservation of reservations) {
        if ('error' in reservation) { options.error?.(reservation.error); continue; }
        if (reservation.failure) options.notify(reservation.jobId!, { error: reservation.failure, statusCode: reservation.statusCode, cooldownUntil: reservation.cooldownUntil });
        if (reservation.skip || reservation.queued) { requested = true; continue; }
        options.log?.({ event: 'dispatch', jobId: reservation.job.id, scheduledAt, dispatchedAt: now() });
        options.run(reservation);
      }
    } finally {
      draining = false;
      if (requested) schedule(await retryDelay());
    }
  }
  function stop() { if (timer) clearTimeout(timer); timer = null; runAt = 0; }
  return { schedule, drain, retryDelay, stop, get timer() { return timer; }, get draining() { return draining; }, get requested() { return requested; } };
}
