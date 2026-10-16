// 共享模式不自动重发原任务：一个任务目前只有一次上游尝试。
// 本模块只追加证据；钱包变更仍由 fair-quota 在同一个存储事务中执行。
function localResources(charge) {
  return {
    v5: charge.resource === 'v5' ? charge.amount : 0,
    anlasFixed: charge.resource === 'anlas' ? charge.fixed : 0,
    anlasPurchased: charge.resource === 'anlas' ? charge.purchased : 0
  };
}

function upstreamCandidates(charge) {
  const anlas = { resources: { anlasFixed: charge.fixed, anlasPurchased: charge.purchased }, amountSource: 'quote' };
  if (charge.billingMode === 'anlas_fallback') {
    // 两种计费路径是候选，不可相加，也不能据本地 Anlas 预扣断言上游用了哪一池。
    return [{ resources: { v5: charge.v5Equivalent }, amountSource: 'quote' }, anlas];
  }
  if (charge.resource === 'v5') return [{ resources: { v5: charge.amount }, amountSource: 'quote' }];
  return charge.resource === 'anlas' ? [anlas] : [];
}

function appendEvent(db, attempt, type, now, details) {
  const event = {
    id: `${attempt.id}:${type}`, type, at: new Date(now).toISOString(),
    attemptId: attempt.id, jobId: attempt.jobId, batchId: attempt.batchId,
    memberId: attempt.memberId, accountId: attempt.accountId,
    ...details
  };
  // 独立 JSON 副本防止后续任务/预扣状态变化覆写旧证据；不记录认证令牌和生成内容。
  (db.ledger ||= []).unshift(structuredClone(event));
  return event;
}

export function recordGenerationAttempt(db, job, charge, snapshot, now) {
  if (job.generationAttempt) return job.generationAttempt;
  const request = job.request || job.requestPayload || {};
  const attempt = {
    id: `attempt:${job.id}:1`, jobId: job.id, batchId: job.batchId || null,
    memberId: charge.userId, accountId: charge.accountId, sampleIndex: job.sampleIndex ?? null,
    operation: request.operation || request.action || 'generate', model: request.model || null,
    source: job.source || null, configVersion: charge.configVersion,
    reservedAt: charge.reservedAt, localReserved: localResources(charge),
    upstreamCandidates: upstreamCandidates(charge), candidateMode: 'alternatives',
    sharedV5Reserved: charge.sharedV5Reserved || 0,
    // 旧版残留预扣不能补造派发前快照；派发意图也不等于请求已到达 NAI。
    evidenceOrigin: snapshot ? 'dispatch_intent' : 'legacy_reservation',
    dispatchIntentAt: snapshot ? now : null, snapshot: snapshot ? structuredClone(snapshot) : null
  };
  job.generationAttempt = attempt;
  charge.attemptId = attempt.id;
  appendEvent(db, attempt, 'generation_reserved', now, { attempt });
  return attempt;
}

export function recordGenerationRefund(db, job, charge, now, evidence = {}) {
  const attempt = job.generationAttempt || recordGenerationAttempt(db, job, charge, null, now);
  // 活进程能明确证明尚未进入 fetch；重启后仅有 running/派发意图时则必须保留不确定性。
  const dispatchState = job.upstreamStartedAt ? 'request_started'
    : evidence.requestStarted === false || ['queued', 'pending'].includes(job.status) ? 'not_sent' : 'unknown';
  const response = {
    dispatchState, upstreamStartedAt: job.upstreamStartedAt || null,
    upstreamHeadersAt: job.upstreamHeadersAt || null, upstreamCompletedAt: job.upstreamCompletedAt || null,
    upstreamHttpStatus: job.upstreamHttpStatus ?? null
  };
  const refund = appendEvent(db, attempt, 'generation_refunded', now, {
    resources: localResources(charge), refundedAt: now, evidenceLevel: 'local_refund', response
  });
  job.generationRefund = { eventId: refund.id, refundedAt: now, lossEventId: null };
  if (dispatchState === 'not_sent' || !attempt.upstreamCandidates.length) return;
  const loss = appendEvent(db, attempt, 'generation_loss_suspected', now, {
    refundEventId: refund.id, firstUnknownAt: now, evidenceLevel: 'unconfirmed',
    // 候选报价不是实际扣费；未知金额明确为空，后续核对只追加调整记录。
    actualConsumed: null, upstreamCandidates: attempt.upstreamCandidates, candidateMode: 'alternatives', response
  });
  job.generationRefund.lossEventId = loss.id;
}
