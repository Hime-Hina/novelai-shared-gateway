import { queuedDeadlineTimestamp } from './queue-policy.ts';

const MiB = 1024 * 1024;
const fail = (statusCode, message) => { throw Object.assign(new Error(message), { statusCode }); };

export function generationPayloadLimits(env = process.env) {
  const request = Number(env.REQUEST_MAX_PAYLOAD_BYTES || 32 * MiB);
  const active = Number(env.QUEUE_MAX_PAYLOAD_BYTES || 64 * MiB);
  if (![request, active].every(value => Number.isSafeInteger(value) && value > 0)) {
    throw new Error('请求载荷字节上限必须为正整数');
  }
  return { request, active };
}

// 正文读取先临时占用，提交后由 SQL 活动任务接管；失败和断线释放临时占用。
// 只保存字节数，不把已完成请求或大正文留在这个计数器里。
export function createGenerationPayloadBudget(readActiveBytes, limits = generationPayloadLimits()) {
  let readingBytes = 0;
  return {
    claim() {
      let bytes = 0;
      return {
        resize(nextBytes) {
          if (!Number.isSafeInteger(nextBytes) || nextBytes < 0 || nextBytes > limits.request) {
            fail(413, '规范化生成请求超过载荷大小限制');
          }
          if (readActiveBytes() + readingBytes - bytes + nextBytes > limits.active) {
            fail(429, '待执行请求载荷已满，请等待已有任务完成');
          }
          readingBytes += nextBytes - bytes;
          bytes = nextBytes;
        },
        release() {
          readingBytes -= bytes;
          bytes = 0;
        }
      };
    }
  };
}

// 队列展示和操作类型需要的小字段；完整执行参数只在父请求表保存。
export function webJobRequestSummary(request) {
  const fields = ['model', 'width', 'height', 'steps', 'requestedSteps', 'seed', 'allowPurchasedAnlas', 'operation', 'action'];
  return Object.fromEntries(fields.filter(key => request[key] !== undefined).map(key => [key, request[key]]));
}

export function singleGenerationRequest(request, sampleIndex = 0) {
  if (!request.nativeParameters) return request;
  const seed = (request.seed + sampleIndex) % 4294967296;
  // 字符串/参考数组只读共享；不为每个 child 深拷贝整份 base64。
  return { ...request, sampleCount: 1, n_samples: 1, seed,
    nativeParameters: { ...request.nativeParameters, n_samples: 1, seed } };
}

export function cancelWaitingBatchJobs(db, job, message, statusCode = 409, now = Date.now()) {
  if (!job.requestId) return [];
  const changed = [];
  for (const child of db.jobs) {
    if (child.id === job.id || child.requestId !== job.requestId || !['pending', 'queued'].includes(child.status)) continue;
    // 未派发 child 没有生成预扣；不为取消意图补造 attempt 或消费证据。
    child.status = 'failed';
    child.error = child.errorDetail = message;
    child.errorStatus = statusCode;
    child.cancelledAt = child.completedAt = child.updatedAt = new Date(now).toISOString();
    child.cancellationReason = 'batch_stopped';
    changed.push(child.id);
  }
  return changed;
}

export function activateNextBatchJob(db, job, queueSeq, now = Date.now()) {
  if (!job.requestId || job.status !== 'done') return;
  const children = db.jobs.filter(child => child.requestId === job.requestId);
  if (children.some(child => ['failed', 'queued', 'running'].includes(child.status))) return;
  const next = children.filter(child => child.status === 'pending').sort((a, b) => a.sampleIndex - b.sampleIndex)[0];
  if (!next) return;
  const deadline = queuedDeadlineTimestamp(next);
  if (deadline && now >= deadline) {
    cancelWaitingBatchJobs(db, job, '任务已过期，剩余图片已取消', 504, now);
    return;
  }
  next.status = 'queued';
  next.queueSeq = queueSeq;
  next.queueTotal = db.jobs.filter(child => ['queued', 'running'].includes(child.status)).length;
  next.updatedAt = new Date(now).toISOString();
  // queueExpiresAt 沿用首次提交期限；激活只领取新的队尾位置。
}
