interface PublicError { message?: string; stack?: string; name?: string; statusCode?: number; status?: number }

export const insufficientBalanceMessage = '密钥额度不足，无法生成图片。';

const quotaUnavailableMessages: Record<string, string> = {
  pending_opus: '共享 Opus 尚未接入，请联系管理员。',
  ambiguous_opus: '共享 Opus 尚未选定，请联系管理员。',
  inactive_opus: '共享 Opus 账号或订阅不可用，请联系管理员。',
  quota_sync_error: '共享 Opus 额度同步失败，请稍后重试。',
  stale: '共享 Opus 额度已过期，请等待更新后重试。',
  unknown_balance: '共享 Opus 额度不完整，请等待更新后重试。'
};
const quotaFailureMessages = new Set([...Object.values(quotaUnavailableMessages),
  '共享 Opus 暂不可用，请稍后重试或联系管理员。',
  '共享 Opus Key 已失效，请联系管理员更新凭据', '上游拒绝访问，请联系管理员重新验证 Opus',
  '账号已封禁', '试用次数已用完', '订阅状态查询超时，将在下一轮自动重试', '官方额度同步失败，请稍后重试']);

export function isNovelAiAccountBannedError(error: unknown): boolean {
  return /\bban(?:ned)?\b|封禁/i.test(String((error as PublicError)?.message || error || ''));
}

export function isNovelAiOutOfTrialImageGenerationError(error: unknown): boolean {
  return /out of trial image generations/i.test(String((error as PublicError)?.message || error || ''));
}

export function isInsufficientBalanceError(error: unknown): boolean {
  const value = error as PublicError;
  return Number(value?.statusCode || value?.status) === 402 || /insufficient balance|额度不足|余额不足/i.test(String(value?.message || error || ''));
}

export function publicErrorMessage(message: unknown, { generation = false } = {}): string {
  const text = String(message || '');
  const generationFailure = '生成失败，请稍后重试或联系管理员。';
  const quotaUnavailable = /^共享 Opus (?:额度)?暂不可用：([\s\S]*)$/.exec(text);
  if (quotaUnavailable) return Object.hasOwn(quotaUnavailableMessages, quotaUnavailable[1]!)
    ? quotaUnavailableMessages[quotaUnavailable[1]!]! : '共享 Opus 暂不可用，请稍后重试或联系管理员。';
  if (quotaFailureMessages.has(text)) return text;
  // File and SQL failures can also arise while reading completed images.
  if (/^(?:Error:\s*)?(?:E[A-Z0-9_]+|SQLITE[A-Z0-9_]*)\s*:/.test(text)) return generationFailure;
  if (generation && /^NovelAI returned 401\b/.test(text)) return '共享 Opus Key 已失效，请联系管理员更新凭据';
  if (generation && /^NovelAI returned 403\b/.test(text)) return '上游拒绝访问，请联系管理员重新验证 Opus';
  if (isNovelAiAccountBannedError(text)) return '账号已封禁';
  if (isInsufficientBalanceError({ message: text })) return insufficientBalanceMessage;
  if (/This operation was aborted|operation was aborted|direct generate timeout|AbortError/i.test(text)) return '连接超时';
  if (/invalid token/i.test(text)) return '密钥无效或已被禁用。';
  if (/all NovelAI accounts are busy|server busy/i.test(text)) return '服务器繁忙，请稍后再试。';
  if (generation && text) {
    if (/(?:Anlas|额度|余额).*(?:不足|耗尽|保留边界)|未经明确授权|暂不能使用购买 Anlas/i.test(text)) return insufficientBalanceMessage;
    const known = [generationFailure, '连接超时', insufficientBalanceMessage, '密钥无效或已被禁用。', '服务器繁忙，请稍后再试。',
      '共享 Opus Key 已失效，请联系管理员更新凭据', '上游拒绝访问，请联系管理员重新验证 Opus',
      'NAI 暂时限制此共享账户的并发；本次失败额度已退回，后续任务将按共享冷却时间派发。',
      '共享 Opus 暂不可用，请稍后重试或联系管理员。', '绑定的 Opus 账号未启用', '共享账本尚未配置',
      '该用户未加入共享分组账本', '共享账本只能使用绑定的 Opus 账号', '已退款任务不能重新提交', '已结算任务不能重新提交',
      '任务已取消', '请求已取消，未派发任务已终止', '管理员已停止此成员接单，未派发任务已取消',
      '管理员已停止此成员接单；已有结果仍可查询', '该请求剩余任务已被管理员取消，请重新提交',
      '任务状态暂时无法读取，请稍后查询'];
    return known.includes(text) ? text : generationFailure;
  }
  return text;
}

export function errorDetailMessage(error: unknown): string {
  const value = error as PublicError;
  return String(value?.stack || value?.message || error || '').trim().slice(0, 4000);
}

export function isAbortError(error: unknown): boolean {
  const value = error as PublicError;
  return value?.name === 'AbortError' || /aborted|abort/i.test(String(value?.message || ''));
}

export function isTimeoutResultMessage(message: unknown): boolean {
  return /direct generate timeout|job timed out|aborted|abort|timeout/i.test(String(message || ''));
}
