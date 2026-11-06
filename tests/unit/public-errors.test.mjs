import test from 'node:test';
import assert from 'node:assert/strict';
import { publicErrorMessage } from '../../server/http/public-errors.ts';

test('额度不可用的所有公开读取和重复生成转换保留固定原因', () => {
  const reasons = { pending_opus: '尚未接入', ambiguous_opus: '尚未选定', inactive_opus: '账号或订阅不可用',
    quota_sync_error: '额度同步失败', stale: '额度已过期', unknown_balance: '额度不完整' };
  for (const [status, reason] of Object.entries(reasons)) for (const prefix of ['共享 Opus 暂不可用：', '共享 Opus 额度暂不可用：']) {
    const message = publicErrorMessage(prefix + status);
    assert.ok(message.includes(reason)); assert.equal(message.includes(status), false);
    assert.equal(publicErrorMessage(message), message);
    assert.equal(publicErrorMessage(message, { generation: true }), message);
    assert.equal(publicErrorMessage(prefix + status, { generation: true }), message);
  }
  for (const secret of ['constructor', 'private body https://secret.invalid?token=pst-secret\nError: private stack']) {
    const message = publicErrorMessage('共享 Opus 暂不可用：' + secret);
    assert.equal(message, '共享 Opus 暂不可用，请稍后重试或联系管理员。');
    assert.equal(publicErrorMessage(message, { generation: true }), message);
  }
});

test('安全查询原因经历史和生成重复转换仍保留，未知生成诊断仍隐藏', () => {
  for (const message of ['共享 Opus Key 已失效，请联系管理员更新凭据', '上游拒绝访问，请联系管理员重新验证 Opus',
    '账号已封禁', '试用次数已用完', '订阅状态查询超时，将在下一轮自动重试', '官方额度同步失败，请稍后重试']) {
    assert.equal(publicErrorMessage(publicErrorMessage(message), { generation: true }), message);
  }
  assert.equal(publicErrorMessage('private upstream body pst-secret', { generation: true }), '生成失败，请稍后重试或联系管理员。');
});
