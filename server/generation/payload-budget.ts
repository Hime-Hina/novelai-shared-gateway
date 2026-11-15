const MiB = 1024 * 1024;
const fail = (statusCode: number, message: string): never => {
  throw Object.assign(new Error(message), { statusCode });
};

export function generationPayloadLimits(env: NodeJS.ProcessEnv = process.env) {
  const request = Number(env.REQUEST_MAX_PAYLOAD_BYTES || 32 * MiB);
  const active = Number(env.QUEUE_MAX_PAYLOAD_BYTES || 64 * MiB);
  if (![request, active].every((value) => Number.isSafeInteger(value) && value > 0)) {
    throw new Error('请求载荷字节上限必须为正整数');
  }
  return { request, active };
}

// 正文读取先临时占用，提交后由 SQL 活动任务接管；失败和断线释放临时占用。
// 只保存字节数，不把已完成请求或大正文留在这个计数器里。
export function createGenerationPayloadBudget(
  readActiveBytes: () => number,
  limits = generationPayloadLimits(),
) {
  let readingBytes = 0;
  return {
    activeBytes: readActiveBytes,
    claim() {
      let bytes = 0;
      return {
        resize(nextBytes: number) {
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
        },
      };
    },
  };
}
