import test from 'node:test';
import assert from 'node:assert/strict';
import { sharedSurface } from '../../server/shared-surface.js';

test('首页只进入共享门户，旧静态资源与未知页面不可访问', () => {
  assert.deepEqual(sharedSurface('/'), { allowed: true, pathname: '/account.html' });
  assert.equal(sharedSurface('/admin').pathname, '/admin/members.html');
  for (const path of ['/app.js', '/admin.js', '/memory', '/unknown', '/foo/../admin.js']) assert.equal(sharedSurface(path).allowed, false, path);
  assert.equal(sharedSurface('/account.js', 'POST').allowed, false);
  assert.equal(sharedSurface('/member-events.js', 'GET').allowed, true);
  assert.equal(sharedSurface('/member-events.js', 'POST').allowed, false);
});
test('酒馆调用、结果取回、成员及管理依赖保持可用', () => {
  for (const [path, method] of [['/v1/chat/completions', 'POST'], ['/ai/generate-image', 'POST'], ['/v1/ai/generate-image', 'OPTIONS'], ['/generate', 'GET'], ['/api/jobs', 'POST'], ['/api/jobs/job1', 'GET'], ['/api/jobs/job1/content', 'GET'], ['/api/images/img1/content', 'GET'], ['/api/member/shared-events', 'GET'], ['/api/member/login', 'POST'], ['/api/admin/member-management', 'POST'], ['/api/admin/accounts/quota', 'POST'], ['/api/admin/accounts/acct1', 'PATCH']]) assert.equal(sharedSurface(path, method).allowed, true, path);
});
test('兑换、旧余额、提示词转换、图库管理及整库导入导出已关闭', () => {
  for (const path of ['/api/redeem', '/api/me/merge', '/api/me', '/api/api/getUser', '/api/prompt/convert', '/api/admin/prompt-api', '/api/settings', '/api/admin/cards', '/api/admin/images', '/api/admin/import', '/api/admin/export', '/api/web/jobs']) {
    for (const method of ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS']) assert.equal(sharedSurface(path, method).allowed, false, path);
  }
});

test('启动器查询别名、编码和放大只开放对应方法', () => {
  for (const path of ['/ai/user/subscription', '/v1/ai/user/subscription']) {
    assert.equal(sharedSurface(path, 'GET').allowed, true);
    assert.equal(sharedSurface(path, 'POST').allowed, false);
  }
  for (const path of ['/ai/encode-vibe', '/v1/ai/encode-vibe', '/ai/upscale', '/v1/ai/upscale']) {
    assert.equal(sharedSurface(path, 'POST').allowed, true);
    assert.equal(sharedSurface(path, 'OPTIONS').allowed, true);
    assert.equal(sharedSurface(path, 'GET').allowed, false);
    assert.equal(sharedSurface(path, 'DELETE').allowed, false);
  }
});

test('Discord 登录只开放配置读取、同源发起和 GET 回调', () => {
  for(const [path, method] of [['/api/member/login-options','GET'],['/api/member/discord/start','POST'],['/api/member/discord/callback','GET']]) {
    assert.equal(sharedSurface(path,method).allowed,true);
    assert.equal(sharedSurface(path,method==='GET'?'POST':'GET').allowed,false);
    assert.equal(sharedSurface(path,'DELETE').allowed,false);
  }
});

test('owner 仅开放指定页面与管理接口，不能访问导出、代理设置或新增上游', () => {
  assert.deepEqual(sharedSurface('/owner'),{allowed:true,pathname:'/owner.html'});
  assert.equal(sharedSurface('/management.js').allowed,true);
  for(const [path,method] of [['/api/member/owner/statistics','GET'],['/api/member/owner/member-management','POST'],['/api/member/owner/accounts/quota','POST'],['/api/member/owner/accounts/opus','PATCH']])assert.equal(sharedSurface(path,method).allowed,true);
  for(const path of ['/api/member/owner/export','/api/member/owner/import','/api/member/owner/accounts','/owner/unknown'])assert.equal(sharedSurface(path,'POST').allowed,false);
});

test('父请求只开放本人查询与显式取消方法，Cookie 和 Bearer 路径一致', () => {
  for (const prefix of ['/api/requests', '/api/member/requests']) {
    assert.equal(sharedSurface(`${prefix}/req1`, 'GET').allowed, true);
    assert.equal(sharedSurface(`${prefix}/req1`, 'POST').allowed, false);
    assert.equal(sharedSurface(`${prefix}/req1/cancel`, 'POST').allowed, true);
    assert.equal(sharedSurface(`${prefix}/req1/cancel`, 'GET').allowed, false);
    assert.equal(sharedSurface(`${prefix}/req1/cancel`, 'DELETE').allowed, false);
  }
});
