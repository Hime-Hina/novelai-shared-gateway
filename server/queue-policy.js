const fail = (statusCode, message) => { throw Object.assign(new Error(message), { statusCode }); };
const unfinished = new Set(['queued', 'pending', 'running']);
const waiting = new Set(['queued', 'pending']);

export function queuePolicy(env = process.env) {
  const limits = {
    member: Number(env.QUEUE_MAX_ACTIVE_PER_MEMBER || 4),
    global: Number(env.QUEUE_MAX_ACTIVE_GLOBAL || 16),
    waitMs: Number(env.QUEUE_WAIT_TIMEOUT_MS || 10 * 60 * 1000)
  };
  if (Object.values(limits).some(value => !Number.isSafeInteger(value) || value <= 0)) {
    fail(503, '队列上限与等待期限必须配置为正整数');
  }
  return limits;
}

export function assertMemberAcceptsJobs(user, expectedVersion) {
  if (user.acceptingJobs === false) fail(403, '管理员已停止此成员接单；已有结果仍可查询');
  // 停止后再恢复不能让旧的多图请求悄悄提交下一张。
  if (expectedVersion !== undefined && expectedVersion !== (user.jobAdmissionVersion || 0)) {
    fail(409, '该请求剩余任务已被管理员取消，请重新提交');
  }
}

// 必须和预扣、入队处于同一 store.update，不能在事务外先数队列再创建任务。
export function assertQueueAdmission(db, user, expectedVersion, env = process.env, count = 1) {
  assertMemberAcceptsJobs(user, expectedVersion);
  const active = db.jobs.filter(job => unfinished.has(job.status));
  const limits = queuePolicy(env);
  if (active.filter(job => job.userToken === user.token).length + count > limits.member) {
    fail(429, `此成员最多保留 ${limits.member} 个未完成任务，请等待已有任务完成`);
  }
  if (active.length + count > limits.global) fail(429, `共享队列已满（${limits.global} 个未完成任务），请稍后重试`);
}

export function queueDeadline(now, requestDeadline, env = process.env) {
  const deadline = Date.parse(requestDeadline || '');
  return new Date(Math.min(now + queuePolicy(env).waitMs, Number.isFinite(deadline) ? deadline : Infinity)).toISOString();
}

export function queuedDeadlineTimestamp(job, env = process.env) {
  if (!waiting.has(job.status)) return 0;
  const saved = Date.parse(job.queueExpiresAt || '');
  if (Number.isFinite(saved)) return saved;
  // 发布前已经排队的任务没有新字段，以原提交时间计算，不在重启时重新计时。
  const created = Date.parse(job.createdAt || '');
  return Number.isFinite(created) ? created + queuePolicy(env).waitMs : 0;
}

// refund 复用入口原有的幂等退款，保持共享账本与旧本地点数的结算口径。
export function setMemberAdmission(db, userId, acceptingJobs, refund, now = Date.now()) {
  const user = db.users.find(item => item.id === userId);
  if (!user) fail(404, '成员不存在');
  if (!acceptingJobs && user.acceptingJobs !== false) user.jobAdmissionVersion = (user.jobAdmissionVersion || 0) + 1;
  user.acceptingJobs = acceptingJobs;
  user.updatedAt = new Date(now).toISOString();
  const cancelledJobIds = [];
  if (!acceptingJobs) {
    for (const job of db.jobs) {
      if (job.userToken !== user.token || !waiting.has(job.status)) continue;
      const message = '管理员已停止此成员接单，未派发任务已取消';
      refund(db, job, message);
      job.status = 'failed';
      job.error = job.errorDetail = message;
      job.errorStatus = 409;
      job.cancelledAt = job.completedAt = job.updatedAt = user.updatedAt;
      job.cancellationReason = 'member_stopped';
      cancelledJobIds.push(job.id);
    }
  }
  return { id: user.id, acceptingJobs, cancelledJobIds };
}
