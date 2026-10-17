import { randomBytes } from 'node:crypto';
import { setMemberCredentials, loginMember, checkMemberLoginRate, createMemberSession, authenticateMemberSession, logoutMemberSession, isOwner } from './member-auth.js';
import { configureFairGroups, fairQuotaSummary, syncFairQuota, manageFairMember, manageOperatingFunding } from './fair-quota.js';
import { sharedAccountStatus } from './shared-status.js';
import { discordLoginEnabled, discordConfig, createDiscordInvite, beginDiscordLogin, finishDiscordLogin, requireDiscordMembership } from './discord-auth.js';
import { clientIp } from './client-ip.js';
import { memberCharge, updateLossAlertPolicy } from './failure-accounting.js';
import { stateEventsFor, mergeStateEvents, stateInvalidation } from './state-events.js';
const statusStreams = new Map();
const error = (statusCode, message) => { throw Object.assign(new Error(message), { statusCode }); };
const cookieName = 'nai_member_session';
function cookie(req) { return String(req.headers.cookie || '').split(';').map(s => s.trim()).find(s => s.startsWith(cookieName + '='))?.slice(cookieName.length + 1) || ''; }
function sameOrigin(req) {
  const host = String(req.headers.host || '');
  const origin = req.headers.origin;
  const local = /^(localhost|127\.0\.0\.1)(:\d+)?$/.test(host);
  const expected = `${local ? 'http' : 'https'}://${host}`;
  if ((origin && origin !== expected) || req.headers['sec-fetch-site'] === 'cross-site') error(403, '此接口仅允许本站访问');
  if (!['GET', 'HEAD'].includes(req.method) && origin !== expected) error(403, '缺少本站 Origin');
}
function reply(res, status, value) {
  const text = JSON.stringify(value);
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'content-length': Buffer.byteLength(text), 'x-content-type-options': 'nosniff' });
  res.end(text);
}
async function json(req) {
  if (!String(req.headers['content-type'] || '').startsWith('application/json')) error(415, '仅接受 JSON');
  let size = 0; const chunks = [];
  for await (const chunk of req) { size += chunk.length; if (size > 32768) error(413, '请求过大'); chunks.push(chunk); }
  let value; try { value = JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch { error(400, 'JSON 格式错误'); }
  if (!value || Array.isArray(value) || typeof value !== 'object') error(400, '请求内容无效');
  return value;
}
function setCookie(req, res, token, expiresAt = 0, additional = []) {
  const local = /^(localhost|127\.0\.0\.1)(:\d+)?$/.test(String(req.headers.host || ''));
  const maxAge = token ? Math.max(0, Math.ceil((expiresAt - Date.now()) / 1000)) : 0;
  const value = `${cookieName}=${token}; Path=/api/member; HttpOnly; SameSite=Strict; Max-Age=${maxAge}; Expires=${new Date(expiresAt).toUTCString()}${local ? '' : '; Secure'}`;
  res.setHeader('Set-Cookie', additional.length ? [...additional, value] : value);
}

function recentActivity(db,user) {
  const jobs=db.jobs.filter(job=>job.userToken===user.token).slice(0,30);
  return {total:jobs.length,done:jobs.filter(job=>job.status==='done').length,failed:jobs.filter(job=>job.status==='failed').length,queued:jobs.filter(job=>['queued','pending'].includes(job.status)).length,running:jobs.filter(job=>job.status==='running').length};
}

// 部署配置优先于历史库内地址，成员与管理员复制到的入口保持一致。
export function memberPublicBaseUrl(db, env = process.env) {
  return env.PUBLIC_BASE_URL || db.settings.publicBaseUrl || (env.PUBLIC_HOST ? `https://${env.PUBLIC_HOST}` : '');
}

export async function handleMemberRoutes(req, res, url, deps) {
  const member = url.pathname.startsWith('/api/member/');
  const ownerRoute = url.pathname.startsWith('/api/member/owner/');
  const admin = url.pathname === '/api/admin/member-management' || url.pathname === '/api/member/owner/member-management';
  if (!member && !admin) return false;
  const env = deps.env || process.env, discordEnabled = discordLoginEnabled(env);
  // OAuth 顶层回跳必然跨站，仅此 GET 路径改用一次性 state + 浏览器 Cookie 验证。
  if (url.pathname === '/api/member/discord/callback' && req.method === 'GET') {
    const user = await finishDiscordLogin(req, res, url, deps);
    const session = await createMemberSession(deps.store, user, env);
    setCookie(req, res, session.token, session.expiresAt, [res.getHeader('Set-Cookie')]);
    res.writeHead(303, { location: discordConfig(env).origin + '/', 'cache-control': 'no-store', 'referrer-policy': 'no-referrer' });
    res.end(); return true;
  }
  sameOrigin(req);
  const { store } = deps;
  let actorId = 'local-admin';
  if (ownerRoute) {
    const viewer = (await authenticateMemberSession(store, cookie(req), env))?.user;
    if (!viewer) error(401, '请先使用 Discord 登录');
    if (!isOwner(viewer, env)) error(403, '仅 owner 可访问管理功能');
    await requireDiscordMembership(viewer, deps);
    actorId = viewer.id;
    if (!admin) {
      const result = await deps.ownerOperation(url, req.method, req.method === 'GET' ? undefined : await json(req));
      reply(res, 200, result); return true;
    }
  }
  if (url.pathname === '/api/member/login-options' && req.method === 'GET') {
    let config;
    try { config = discordConfig(env); } catch {}
    reply(res, 200, { discord: discordEnabled, ready: Boolean(config), origin: config?.origin || null }); return true;
  }
  if (url.pathname === '/api/member/discord/start' && req.method === 'POST') {
    checkMemberLoginRate(null, clientIp(req, env));
    reply(res, 200, await beginDiscordLogin(req, res, await json(req), deps)); return true;
  }
  if (admin) {
    // 原私有管理 API 保持本机与密钥校验；owner 别名已经过独立会话鉴权。
    if (!ownerRoute) {
      if (!/^(127\.0\.0\.1|localhost)(:\d+)?$/.test(String(req.headers.host || ''))) error(404, 'not found.');
      deps.assertAdmin(req, url);
    }
    if (req.method === 'GET') {
      if (url.searchParams.get('view') === 'losses') {
        reply(res, 200, await store.readFailureAccounting(url.searchParams.get('before'))); return true;
      }
      const payload = await store.update(db => {
        const draft = { ...db, ledger: [...(db.ledger || [])], settings: structuredClone(db.settings || {}) };
        syncFairQuota(draft);
        return { groups: draft.settings.fairQuota?.groups || [], status: draft.settings.fairQuota?.status,
          shared: sharedAccountStatus(draft),autoAnlasFallback: draft.settings.fairQuota?.autoAnlasFallback === true,
          configVersion: draft.settings.fairQuota?.configVersion ?? 1,
          publicBaseUrl: memberPublicBaseUrl(db), discordLogin: discordEnabled, ownerUserId: env.OWNER_USER_ID || null,
          users: db.users.map(u => ({ id: u.id, username: u.memberAuth?.username || '', note: u.note || '', enabled: u.enabled !== false,
            acceptingJobs: u.acceptingJobs !== false,
            discordId: u.discord?.id || null, inviteExpiresAt: u.discordInvite?.expiresAt || null,
            quota: fairQuotaSummary(draft, u), recentActivity: recentActivity(db, u) })) };
      }, { persist: false });
      reply(res, 200, payload); return true;
    }
    if (req.method !== 'POST') error(405, 'method not allowed');
    const body = await json(req);
    if (body.action === 'loss-alert-policy') {
      const policy = await store.update(db => updateLossAlertPolicy(db, body, actorId), { collections: ['settings', 'ledger'], immediate: true });
      reply(res, 200, { policy }); return true;
    }
    if (ownerRoute && body.userId === env.OWNER_USER_ID && (['delete-member','preview-delete-member'].includes(body.action) || body.action === 'member-status' && body.enabled === false)) error(409, '不能停用或删除当前 owner');
    if (['operating-contribute', 'operating-register-funding', 'operating-cover-loss', 'operating-reconcile-return'].includes(body.action)) {
      // 身份来自已鉴权的 owner 配置，不接受客户端指定其他成员作为注资来源。
      if (!env.OWNER_USER_ID || (body.userId && body.userId !== env.OWNER_USER_ID)) error(403, '只能从运营者本人已归属份额划入');
      const result = await store.update(db => {
        syncFairQuota(db);
        return manageOperatingFunding(db, body, {
          actorId: env.OWNER_USER_ID,
          findEvent: id => db.ledger.find(event => event.id === id) || store.selectItemById('ledger', id),
          coveredAmount: id => [...new Map([...store.readOperatingLossSettlements(id),
            ...db.ledger.filter(event => event.type === 'operating_loss_covered' && event.lossEventId === id)]
            .map(event => [event.id, event])).values()].reduce((total, event) => total + event.amount, 0)
        });
      }, { collections: ['settings', 'ledger'], immediate: true });
      reply(res, 200, result); return true;
    }
    if (body.action === 'discord-invite') {
      if (!discordEnabled) error(409, 'Discord 登录未启用');
      const result = await store.update(db => createDiscordInvite(db, body.userId), { collections: ['users'], immediate: true });
      reply(res, 200, result); return true;
    }
    if (['transfer-quota','preview-delete-member','delete-member'].includes(body.action)) {
      const preview=body.action==='preview-delete-member';
      const result=await store.update(db=>manageFairMember(db,body),preview?{persist:false}:{collections:['users','settings','ledger'],immediate:true});
      reply(res,200,result);return true;
    }
    if (body.action === 'fallback-policy') {
      if (typeof body.enabled !== 'boolean') error(400, 'enabled 必须为布尔值');
      await store.update(db => {
        const state = db.settings.fairQuota;
        if (!state?.enabled) error(409, '共享账本尚未配置');
        state.autoAnlasFallback = body.enabled;
        state.policyHistory = [...(state.policyHistory || []), { at: Date.now(), actor: 'administrator', autoAnlasFallback: body.enabled }].slice(-1000);
      }, { collections: ['settings'], immediate: true });
      reply(res, 200, { autoAnlasFallback: body.enabled }); return true;
    }
    if (body.action === 'member-key') {
      const result=await store.update(db=>{
        const user=db.users.find(u=>u.id===body.userId);if(!user)error(404,'成员不存在');
        return {apiKey:user.token};
      },{persist:false});
      reply(res,200,result);return true;
    }
    if (body.action === 'member-status') {
      if(typeof body.enabled!=='boolean')error(400,'enabled 必须为布尔值');
      const result=await store.update(db=>{
        const user=db.users.find(u=>u.id===body.userId);if(!user)error(404,'成员不存在');
        if(!body.enabled&&(db.jobs.some(job=>job.userToken===user.token&&['queued','pending','running'].includes(job.status))||Object.values(db.settings.fairQuota?.reservations||{}).some(charge=>charge.userId===user.id&&charge.status==='reserved')))error(409,'成员仍有排队或生成中的任务，请结清后停用');
        user.enabled=body.enabled;user.updatedAt=new Date().toISOString();
        // 停用时改变会话版本，之后重新启用也不能复活先前登录的 Cookie。
        if(!body.enabled&&user.memberAuth)user.memberAuth={...user.memberAuth,version:randomBytes(16).toString('hex')};
        return {id:user.id,enabled:user.enabled};
      },{collections:['users'],immediate:true});
      reply(res,200,result);return true;
    }
    if (body.action === 'member-admission') {
      if (typeof body.acceptingJobs !== 'boolean') error(400, 'acceptingJobs 必须为布尔值');
      const result = await deps.changeMemberAdmission(body.userId, body.acceptingJobs);
      reply(res, 200, result); return true;
    }
    if (body.action === 'preview-groups') {
      if(body.redistributeRemaining!==undefined&&typeof body.redistributeRemaining!=='boolean')error(400,'redistributeRemaining 必须为布尔值');
      const result=await store.update(db=>{
        const configVersion=db.settings.fairQuota?.configVersion ?? 1;
        if(!Number.isInteger(body.expectedConfigVersion)||body.expectedConfigVersion!==configVersion)error(409,'分组版本缺失或已变化，请保留草稿并刷新后重新编辑');
        // 预览只操作独立副本，恢复、重分与审计都不能污染真实账本。
        const draft={...db,ledger:[...(db.ledger || [])],settings:structuredClone(db.settings),users:structuredClone(db.users)};
        configureFairGroups(draft,body.groups,{actorId:'administrator',redistributeRemaining:body.redistributeRemaining===true});
        return {groups:draft.settings.fairQuota.groups,users:draft.users.map(user=>({id:user.id,quota:fairQuotaSummary(draft,user)})),redistributeRemaining:body.redistributeRemaining===true,configVersion};
      },{persist:false});
      reply(res,200,result);return true;
    }
    if (body.action === 'credentials') {
      if (discordEnabled && body.userId) error(409, 'Discord 成员不使用登录密码');
      const hasGroup=body.groupId!==undefined&&body.groupId!==null&&body.groupId!=='';
      if(hasGroup&&(typeof body.groupId!=='string'||body.userId))error(400,'只有创建新成员时可以指定 groupId');
      const result = await store.update(async db => {
        const draft = { ...db, ledger:[...(db.ledger || [])],users: structuredClone(db.users),settings:structuredClone(db.settings) };
        let userId = body.userId;
        if (!userId) {
          if (draft.users.length >= 200) error(409, '账户数已达上限');
          const user = { id: deps.createId('usr'), token: deps.createPublicToken('STA1N'), balance: 0, enabled: true,
            note: String(body.note || '管理员创建的成员').slice(0, 120), createdAt: new Date().toISOString() };
          userId = user.id; draft.users.push(user);
        }
        const result = await setMemberCredentials(draft, userId, discordEnabled ? { ...body, password: randomBytes(32).toString('base64url') } : body);
        if(hasGroup){
          syncFairQuota(draft);
          const groups=structuredClone(draft.settings.fairQuota?.groups || []);
          const group=groups.find(g=>g.id===body.groupId);if(!group)error(400,'指定的共享组不存在');
          group.members.push(userId);configureFairGroups(draft,groups,{actorId:'administrator'});
        }
        // 所有校验和密码派生成功后才整体提交，失败不会遗留用户或半份账本。
        db.users = draft.users;if(hasGroup){db.settings=draft.settings;db.ledger=draft.ledger;}
        return {...result,...(hasGroup?{groupId:body.groupId}:{})};
      }, { collections: ['users',...(hasGroup?['settings','ledger']:[])], immediate: true });
      reply(res, 200, result); return true;
    }
    if (body.action === 'groups') {
      if(body.redistributeRemaining!==undefined&&typeof body.redistributeRemaining!=='boolean')error(400,'redistributeRemaining 必须为布尔值');
      const result = await store.update(db => {
        const currentVersion=db.settings.fairQuota?.configVersion ?? 1;
        if(!Number.isInteger(body.expectedConfigVersion)||body.expectedConfigVersion!==currentVersion)error(409,'分组版本缺失或已变化，请保留草稿并刷新后重新编辑');
        configureFairGroups(db, body.groups, { actorId: 'administrator', redistributeRemaining: body.redistributeRemaining === true });
        syncFairQuota(db);
        return { groups: db.settings.fairQuota.groups, status: db.settings.fairQuota.status,configVersion:db.settings.fairQuota.configVersion };
      }, { collections: ['settings', 'ledger'], immediate: true });
      reply(res, 200, result); return true;
    }
    error(400, '不支持的管理操作');
  }

  if (url.pathname === '/api/member/login' && req.method === 'POST') {
    if (discordEnabled) error(403, '本站仅允许 Discord 登录');
    const body = await json(req);
    checkMemberLoginRate(body.username, clientIp(req, env));
    const user = await store.update(db => loginMember(db, body.username, body.password), { persist: false });
    const session = await createMemberSession(store, user, env);
    setCookie(req, res, session.token, session.expiresAt);
    reply(res, 200, { ok: true, expiresAt: session.expiresAt }); return true;
  }
  if (url.pathname === '/api/member/logout' && req.method === 'POST') {
    await logoutMemberSession(store, cookie(req)); setCookie(req, res, ''); reply(res, 200, { ok: true }); return true;
  }
  const session = await authenticateMemberSession(store, cookie(req), env), user = session?.user;
  if (!user) error(401, '请先登录');
  await requireDiscordMembership(user, deps);
  const requestRoute = url.pathname.match(/^\/api\/member\/requests\/([^/]+)(\/cancel)?$/);
  if (requestRoute && (req.method === 'GET' && !requestRoute[2] || req.method === 'POST' && requestRoute[2])) {
    const requestId = decodeURIComponent(requestRoute[1]);
    if (requestRoute[2]) await json(req);
    const result = requestRoute[2]
      ? await deps.cancelGenerationRequest(requestId, user.id)
      : await deps.getGenerationRequest(requestId, user.id);
    reply(res, 200, result); return true;
  }
  if (req.method !== 'GET') error(403, '成员账户没有管理权限，请联系管理员');
  if (url.pathname === '/api/member/shared-status') {
    const state = await store.update(db => sharedAccountStatus(db), { persist: false });
    reply(res, 200, state); return true;
  }
  if (url.pathname === '/api/member/shared-events') {
    if (res.destroyed || res.writableEnded) return true;
    if ((statusStreams.get(user.id) || 0) >= 3 || [...statusStreams.values()].reduce((n, v) => n + v, 0) >= 100) error(429, '状态连接过多，请关闭多余页面');
    statusStreams.set(user.id, (statusStreams.get(user.id) || 0) + 1);
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store', 'x-accel-buffering': 'no', 'connection': 'keep-alive' });
    res.flushHeaders?.();
    const hub = stateEventsFor(store);
    let closed = false, timer, previous = '', heartbeat = 0, running = false, requested = false, resync = true, pending;
    let unsubscribe = () => {};
    const stop = () => {
      if (closed) return;
      closed = true; clearTimeout(timer);
      unsubscribe(); res.off('close', stop); res.off('error', stop);
      const count = (statusStreams.get(user.id) || 1) - 1;
      if (count) statusStreams.set(user.id, count); else statusStreams.delete(user.id);
    };
    res.once('close', stop); res.once('error', stop);
    const wake = event => {
      if (closed) return;
      if (event) pending = mergeStateEvents(pending, event);
      requested = true; clearTimeout(timer);
      if (!running) void tick();
    };
    const tick = async () => {
      running = true;
      try {
        while (requested && !closed) {
          requested = false;
          const event = pending || hub.current(), full = resync;
          pending = undefined; resync = false;
          const viewer = (await authenticateMemberSession(store, cookie(req), env))?.user;
          if (viewer) await requireDiscordMembership(viewer, deps);
          const state = viewer ? await store.update(db => sharedAccountStatus(db), { persist: false }) : null;
          if (closed) return;
          if (!state) { res.write('event: auth-expired\ndata: {}\n\n'); res.end(); stop(); return; }
          const text = JSON.stringify(state);
          if (text !== previous) { res.write(`event: shared-status\ndata: ${text}\n\n`); previous = text; heartbeat = Date.now(); }
          const invalidation = stateInvalidation(event, viewer.id, isOwner(viewer, env), full);
          if (invalidation) { res.write(`event: state-invalidated\ndata: ${JSON.stringify(invalidation)}\n\n`); heartbeat = Date.now(); }
          if (Date.now() - heartbeat > 15000) { res.write(': keepalive\n\n'); heartbeat = Date.now(); }
          if (res.writableLength > 65536) { res.end(); stop(); return; }
        }
      } catch (error) {
        if (!closed) {
          if ([401, 403].includes(error.statusCode)) res.write('event: auth-expired\ndata: {}\n\n');
          res.end(); stop();
        }
      } finally {
        running = false;
        if (!closed) { timer = setTimeout(() => wake(), 2000); timer.unref?.(); }
      }
    };
    unsubscribe = hub.subscribe(wake, () => { res.end(); stop(); });
    wake();
    return true;
  }
  if (url.pathname === '/api/member/key') {
    const publicBase = await store.update(db => memberPublicBaseUrl(db), { persist: false });
    const base = publicBase.replace(/\/+$/, '');
    reply(res, 200, { apiKey: user.token, baseUrl: base ? (base.endsWith('/v1') ? base : `${base}/v1`) : '' }); return true;
  }
  if (url.pathname === '/api/member/me') {
    const payload = await store.update(db => {
      const draft = { ...db, ledger: [...(db.ledger || [])], settings: structuredClone(db.settings || {}) };
      return { username: user.memberAuth.username, owner: isOwner(user, env), sessionExpiresAt: session.expiresAt,
        acceptingJobs: user.acceptingJobs !== false, quota: fairQuotaSummary(draft, user),
        jobs: db.jobs.filter(j => j.userToken === user.token).slice(0, 30).map(j => ({ id: j.id, requestId: j.requestId || null, createdAt: j.createdAt,
          lateResultAvailable: j.lateResultAvailable === true, imageId: j.imageId || '',
          model: j.request?.model || '', status: j.status, charge: memberCharge(j.fairCharge) }))
      };
    }, { persist: false });
    const [ownedImages, lateResults] = await Promise.all([store.readMemberImageIds(user, payload.jobs.map(job => job.id)), store.readMemberLateResults(user)]);
    for (const job of payload.jobs) {
      job.imageId = ownedImages.get(job.id) || '';
      job.imageUrl = job.imageId ? deps.publicImageUrl(job.imageId) : '';
    }
    payload.lateResults = lateResults.map(job => ({ id: job.id, model: job.model || '', createdAt: job.createdAt,
      lateResultAt: job.lateResultAt, imageUrl: deps.publicImageUrl(job.imageId), charge: memberCharge(job.charge) }));
    reply(res, 200, payload); return true;
  }
  error(404, 'not found.');
}
