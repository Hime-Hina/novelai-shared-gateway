import Database from 'better-sqlite3';
import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';
import {
  closeSync,
  copyFileSync,
  lstatSync,
  mkdtempSync,
  openSync,
  readSync,
  realpathSync,
  rmSync,
  constants,
} from 'node:fs';
import { mkdir, open } from 'node:fs/promises';
import http, { type IncomingMessage, type ServerResponse } from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { isDeepStrictEqual } from 'node:util';
import { assertRelationalStorageReady } from './storage/relational-storage.ts';
import { RelationalInputStore } from './storage/relational-input-store.ts';
import { RelationalGenerationStore } from './storage/relational-generation-store.ts';
import { RelationalQuotaStore } from './storage/relational-quota-store.ts';
import { RelationalConfigurationStore } from './storage/relational-configuration-store.ts';
import { RelationalMemberStore } from './storage/relational-member-store.ts';
import { relationalStateNotificationsFor } from './events/relational-state-notifications.ts';
import { createRelationalQueueRuntime } from './generation/relational-queue-runtime.ts';
import { createRelationalAccountQuotaRefresh } from './quota/relational-account-refresh.ts';
import { createGenerationPayloadBudget } from './generation/payload-budget.ts';
import { JobPreviews } from './generation/job-previews.ts';
import type { RunningControl } from './generation/queue-dispatch.ts';
import { createRelationalNativeHttp } from './generation/relational-native-http.ts';
import { createRelationalOpenAiHttp } from './generation/relational-openai-http.ts';
import { createRelationalDirectHttp } from './generation/relational-direct-http.ts';
import { createRelationalMemberGenerationHttp } from './generation/relational-member-http.ts';
import { createRelationalRequestHttp } from './generation/relational-request-http.ts';
import { createRelationalQuotaHttp } from './quota/relational-quota-http.ts';
import { createRelationalMemberAuthenticationHttp } from './members/relational-authentication-http.ts';
import { createRelationalMemberReadHttp } from './members/relational-read-http.ts';
import { createRelationalManagementHttp } from './members/relational-management-http.ts';
import { resolveMemberPublicBaseUrl } from './members/read-http.ts';
import { gatewayKeyFrom } from './members/http-request.ts';
import { reconcileMemberSessionOwner } from './members/authentication.ts';
import { sharedSurface } from './http/api-surface.ts';
import { privateAccessAllowed } from './http/public-hosts.ts';
import { assertAdminToken, matchesAdminToken } from './http/admin-access.ts';
import { createPublicImageAccess } from './http/public-image.ts';
import { publicErrorMessage, isAbortError } from './http/public-errors.ts';
import { nativeGenerationError } from './protocols/native-http.ts';
import { openAiGenerationError } from './protocols/openai-http.ts';
import { directRequestErrorImage } from './protocols/direct-http.ts';
import { memberGenerationError } from './generation/member-http.ts';
import { cooldownRetryAfter } from './quota/shared-capacity.ts';
import { quotaHttpRoute } from './protocols/quota-http.ts';
import { r2Config, R2Storage } from './storage/r2-storage.ts';
import { pikpakConfig, PikPakStorage } from './storage/pikpak-storage.ts';
import {
  RelationalReplicaStore,
  type RelationalReplicaConfiguration,
} from './storage/relational-replica-store.ts';
import { copyRelationalArtifactReplica } from './storage/relational-replica-io.ts';
import type { GenerationPayloadClaim } from './generation/admission.ts';

type Request = IncomingMessage & { generationPayloadClaim?: GenerationPayloadClaim };
type Failure = Error & { statusCode?: number; cooldownUntil?: number | string; requestId?: string };
let boot: Promise<void> | undefined;
let closing = false;
let serve: ((req: Request, res: ServerResponse) => Promise<void>) | undefined;

function fail(statusCode: number, message: string): never {
  throw Object.assign(new Error(message), { statusCode });
}

function fileFingerprint(filename: string) {
  let info;
  try {
    info = lstatSync(filename, { bigint: true });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return null;
    }
    throw error;
  }
  if (!info.isFile()) {
    throw new Error('Gateway database and sidecars must be regular files.');
  }
  const hash = createHash('sha256'),
    buffer = Buffer.alloc(64 * 1024),
    handle = openSync(filename, 'r');
  try {
    let bytes;
    while ((bytes = readSync(handle, buffer)) !== 0) {
      hash.update(buffer.subarray(0, bytes));
    }
  } finally {
    closeSync(handle);
  }
  return {
    dev: info.dev,
    ino: info.ino,
    size: info.size,
    mtime: info.mtimeNs,
    ctime: info.ctimeNs,
    sha: hash.digest('hex'),
  };
}

// SQLite readonly 仍可创建 WAL/SHM 或要求 hot-journal 恢复。首次真实打开前只在私有副本恢复并验证。
// 启动要求单 writer/quiescent source；前后指纹拒绝观察到的变化，不承诺阻止不合作进程的全部 TOCTOU。
function openRuntimeDatabase(dataDir: string) {
  const directory = realpathSync(dataDir),
    filename = realpathSync(path.join(directory, 'library.sqlite'));
  const suffixes = ['', '-wal', '-shm', '-journal'],
    snapshot = () => suffixes.map((suffix) => fileFingerprint(filename + suffix)),
    before = snapshot();
  if (!before[0]) {
    throw new Error('Gateway requires an existing relational database.');
  }
  const stage = mkdtempSync(path.join(directory, '.gateway-preflight-')),
    identity = lstatSync(stage, { bigint: true }),
    copy = path.join(stage, 'candidate.sqlite');
  try {
    for (const suffix of ['', '-wal', '-journal']) {
      const original = before[suffixes.indexOf(suffix)];
      if (original) {
        copyFileSync(filename + suffix, copy + suffix, constants.COPYFILE_EXCL);
        if (fileFingerprint(copy + suffix)?.sha !== original.sha) {
          throw new Error('Database changed during startup validation.');
        }
      }
    }
    const probe = new Database(copy, { fileMustExist: true });
    try {
      probe.pragma('foreign_keys=ON');
      assertRelationalStorageReady(probe);
      new RelationalConfigurationStore(probe).readCurrent();
    } finally {
      probe.close();
    }
    if (!isDeepStrictEqual(snapshot(), before)) {
      throw new Error('Database changed during startup validation.');
    }
  } finally {
    const relative = path.relative(directory, stage),
      current = lstatSync(stage, { bigint: true });
    if (
      !relative ||
      relative.startsWith('..' + path.sep) ||
      path.isAbsolute(relative) ||
      current.dev !== identity.dev ||
      current.ino !== identity.ino
    ) {
      throw new Error('Startup scratch directory identity changed; cleanup refused.');
    }
    rmSync(stage, { recursive: true });
  }
  const sqlite = new Database(filename, { fileMustExist: true });
  try {
    sqlite.pragma('foreign_keys=ON');
    assertRelationalStorageReady(sqlite);
    return sqlite;
  } catch (error) {
    sqlite.close();
    throw error;
  }
}

function corsHeaders() {
  return {
    'access-control-allow-origin': '*',
    'access-control-expose-headers':
      'Retry-After, X-Nai-Diagnostic-Id, X-Nai-Request-Id, X-Nai-Image-Count, X-Nai-Partial',
    'access-control-allow-methods': 'GET,POST,PUT,PATCH,DELETE,OPTIONS',
    'access-control-allow-headers':
      'content-type,authorization,x-admin-token,x-user-token,x-requested-with,x-request-id,x-correlation-id,x-initiated-at,user-agent,accept,idempotency-key',
  };
}
function sendJson(res: ServerResponse, status: number, value: unknown) {
  const body = JSON.stringify(value);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': Buffer.byteLength(body),
    ...corsHeaders(),
  });
  res.end(body);
}
function sendImage(
  res: ServerResponse,
  status: number,
  mimeType: string,
  bytes: Buffer,
  headers: http.OutgoingHttpHeaders = {},
) {
  res.writeHead(status, {
    'content-type': mimeType,
    'cache-control': 'public, max-age=31536000, immutable',
    'content-length': bytes.length,
    ...corsHeaders(),
    ...headers,
  });
  res.end(bytes);
}
async function readJson(req: Request, limit = 2 * 1024 * 1024): Promise<unknown> {
  if (Number(req.headers['content-length']) > limit) {
    fail(413, '请求体超过大小限制');
  }
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of req) {
    bytes += chunk.length;
    if (bytes > limit) {
      fail(413, '请求体超过大小限制');
    }
    req.generationPayloadClaim?.resize(bytes);
    chunks.push(chunk);
  }
  if (!chunks.length) {
    return {};
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    fail(400, 'invalid JSON body.');
  }
}
function renderError(req: Request, res: ServerResponse, error: unknown) {
  if (res.destroyed || res.writableEnded) {
    return;
  }
  if (res.headersSent) {
    res.destroy();
    return;
  }
  const source = error as Failure,
    pathname = String(req.url || '').split('?')[0],
    retry = cooldownRetryAfter(source.cooldownUntil);
  if (retry) {
    res.setHeader('Retry-After', String(retry));
  }
  if (source.requestId) {
    res.setHeader('X-Nai-Request-Id', source.requestId);
  }
  if (
    /^\/api\/member\/(?:generation\/|requests\/[^/]+\/(?:events|inputs?)|jobs\/[^/]+\/content)/.test(
      pathname,
    )
  ) {
    const value = memberGenerationError(error);
    sendJson(res, value.statusCode, value);
    return;
  }
  if (pathname === '/generate') {
    const image = directRequestErrorImage(error, true);
    if (image) {
      sendImage(res, image.statusCode, image.mimeType, image.buffer, image.headers);
      return;
    }
  }
  if (/^\/(?:v1\/)?ai\/(?:generate-image(?:-stream)?|encode-vibe|upscale)$/.test(pathname)) {
    const value = nativeGenerationError(error);
    sendJson(res, value.statusCode, value.body);
    return;
  }
  if (pathname.startsWith('/v1/')) {
    const value = openAiGenerationError(error);
    sendJson(res, value.statusCode, value.body);
    return;
  }
  sendJson(res, source.statusCode || 500, {
    error: publicErrorMessage(source.message || 'Internal server error'),
  });
}

// 两个既有 launcher 共用此入口。导入不打开 DB、不初始化、不监听或启动 timer。
export function startGateway(handler: http.RequestListener): Promise<void> {
  if (closing) {
    return Promise.reject(new Error('Gateway cannot restart in place.'));
  }
  boot ??= start(handler);
  return boot;
}
async function start(handler: http.RequestListener) {
  const env: NodeJS.ProcessEnv = {
      ...process.env,
      FAIR_SHARE_MODE: 'true',
      PRIVATE_MODE: 'true',
      PUBLIC_MODE: 'true',
      NO_AUTO_DELETE: 'true',
      MOCK_WHEN_NO_ACCOUNT: 'false',
    },
    adminToken = String(env.ADMIN_TOKEN || '').trim();
  if (adminToken.length < 32 || /^(change|replace|example)/i.test(adminToken)) {
    throw new Error('请设置至少 32 位随机 ADMIN_TOKEN');
  }
  const dataDir = path.resolve(env.DATA_DIR || fileURLToPath(new URL('../data', import.meta.url))),
    port = Number(env.PORT || 8080),
    host = env.HOST || '0.0.0.0';
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new Error('PORT must be an integer from 0 to 65535.');
  }
  const sqlite = openRuntimeDatabase(dataDir);
  let server: http.Server | undefined, shutdown: Promise<void> | undefined;
  let replicaTimer: ReturnType<typeof setTimeout> | undefined,
    replicaCopy: Promise<void> | undefined;
  const requests = new Map<ServerResponse, Request>(),
    handlers = new Set<Promise<void>>();
  const events = new EventEmitter().setMaxListeners(0),
    previews = new JobPreviews(),
    controls = new Map<string, RunningControl>(),
    inputs = new RelationalInputStore(sqlite, env),
    generations = new RelationalGenerationStore(sqlite, env),
    quotas = new RelationalQuotaStore(sqlite, env),
    configuration = new RelationalConfigurationStore(sqlite);
  const queue = createRelationalQueueRuntime({
      sqlite,
      dataDir,
      generations,
      inputs,
      controls,
      events,
      previews,
      env,
      onError: (event) => {
        console.error('[runtime]', event.code);
        queueMicrotask(() => void exitAfterStop(1));
      },
    }),
    refresh = createRelationalAccountQuotaRefresh({ sqlite, quotas, events, env });
  function stop() {
    if (shutdown) {
      return shutdown;
    }
    closing = true;
    clearTimeout(replicaTimer);
    shutdown = (async () => {
      const disconnected = server?.listening
        ? new Promise<void>((resolve, reject) =>
            server!.close((error) => (error ? reject(error) : resolve())),
          )
        : Promise.resolve();
      const stopped = queue.stop();
      previews.closeSubscriptions();
      await refresh.stop();
      await stopped;
      // 唯一领取 owner 先冷读已完成结果，仅结束仍等待的 HTTP；不抢先覆盖正在读取的产物。
      events.emit('drained');
      for (const req of requests.values()) {
        if (!req.complete) {
          req.destroy();
        }
      }
      await Promise.allSettled([...handlers]);
      // close() 发起时仍 active 的 keep-alive，后来完成后才成为 idle；先等实际响应结束再清闲连接。
      await Promise.all(
        [...requests.keys()].map(
          (res) =>
            new Promise<void>((resolve) => {
              if (res.writableFinished || res.destroyed) {
                resolve();
                return;
              }
              const done = () => {
                res.off('finish', done);
                res.off('close', done);
                resolve();
              };
              res.once('finish', done);
              res.once('close', done);
            }),
        ),
      );
      server?.closeIdleConnections();
      await disconnected;
      // 独立额度/副本 IO 仍须真完成才关库；不能阻挡 stopped 队列唤醒已有 HTTP 领取。
      await refresh.pending;
      await replicaCopy;
      sqlite.close();
    })();
    return shutdown;
  }
  async function exitAfterStop(code: number) {
    const configured = Number(env.SHUTDOWN_GRACE_MS || 30000),
      grace =
        Number.isFinite(configured) && configured > 0
          ? Math.min(600000, Math.floor(configured))
          : 30000;
    const force = setTimeout(() => process.exit(code), grace);
    try {
      await stop();
      clearTimeout(force);
      process.exit(code);
    } catch {
      console.error('[runtime] Shutdown failed.');
      clearTimeout(force);
      process.exit(1);
    }
  }
  try {
    configuration.readCurrent();
    sqlite.pragma('journal_mode=WAL');
    sqlite.pragma('synchronous=FULL');
    const created = await mkdir(path.join(dataDir, 'images'), { recursive: true });
    if (created && process.platform !== 'win32') {
      const directory = await open(dataDir, 'r');
      try {
        await directory.sync();
      } finally {
        await directory.close();
      }
    }
    const r2 = r2Config(env),
      pikpak = pikpakConfig(env),
      replicas: RelationalReplicaConfiguration = {
        dataDir,
        ...(r2 ? { r2: new R2Storage(r2) } : {}),
        ...(pikpak ? { pikpak: new PikPakStorage(pikpak) } : {}),
        idle() {
          if (closing || queue.status !== 'running' || controls.size) {
            return false;
          }
          assertRelationalStorageReady(sqlite);
          return !generations.hasActiveGenerationResponsibilities();
        },
      },
      access = createPublicImageAccess(env),
      imageUrl = access.imageUrl,
      payloadBudget = createGenerationPayloadBudget(() =>
        inputs.activePayloadBytes(controls.values()),
      ),
      shared = {
        sqlite,
        dataDir,
        events,
        controls,
        previews,
        replicas,
        payloadBudget,
        env,
        quotas,
        queryAccountQuota: refresh.fetch,
        runtime: queue,
        readJson,
        sendJson,
        sendImage,
        imageUrl,
        publicImageUrl: imageUrl,
        schedule: queue.schedule,
        scheduleQueueDrain: queue.schedule,
        timeoutMs: Number(env.OPENAI_CHAT_TIMEOUT_MS || 10 * 60_000),
      },
      assertAdmin = (req: IncomingMessage, url: URL) => assertAdminToken(req, url, adminToken, env),
      memberHandlers = [
        createRelationalMemberAuthenticationHttp(shared),
        createRelationalMemberReadHttp({
          ...shared,
          publicBaseUrl: resolveMemberPublicBaseUrl(configuration.readCurrent().publicBaseUrl, env),
        }),
        createRelationalManagementHttp({ ...shared, assertAdmin }),
        createRelationalMemberGenerationHttp(shared),
      ],
      apiHandlers = [
        createRelationalNativeHttp(shared),
        createRelationalOpenAiHttp({
          ...shared,
          readNormalizationDefaults: () => configuration.readCurrent().normalizationDefaults,
          imageUrl: (req, id) => {
            const protocol =
                req.headers['x-forwarded-proto'] ||
                ((req.socket as typeof req.socket & { encrypted?: boolean }).encrypted
                  ? 'https'
                  : 'http'),
              authority =
                req.headers['x-forwarded-host'] || req.headers.host || `localhost:${port}`;
            return `${protocol}://${authority}${imageUrl(id)}`;
          },
          sendStreamHeaders: (res) =>
            res.writeHead(200, {
              'content-type': 'text/event-stream',
              'cache-control': 'no-cache',
              connection: 'keep-alive',
              'x-accel-buffering': 'no',
            }),
        }),
        createRelationalDirectHttp({
          ...shared,
          timeoutMs: Number(env.DIRECT_GENERATE_TIMEOUT_MS || 60_000),
          readNormalizationDefaults: () => configuration.readCurrent().normalizationDefaults,
          validImageSignature: access.validSignature,
          isAdminRead: (req, url) => matchesAdminToken(req, url, adminToken, env),
        }),
        createRelationalRequestHttp(shared),
        createRelationalQuotaHttp(shared),
      ];
    const replicaStore = new RelationalReplicaStore(sqlite, replicas);
    function scheduleReplicaCopy(delay: number) {
      if (closing || !replicas.r2) {
        return;
      }
      replicaTimer = setTimeout(() => {
        replicaTimer = undefined;
        let progress = false;
        replicaCopy = (async () => {
          if (!replicas.idle()) {
            return;
          }
          const due = replicaStore.readDueArtifactReplica(Date.now());
          if (due) {
            const current = await copyRelationalArtifactReplica(
              sqlite,
              due.artifactId,
              due.kind,
              replicas,
            );
            progress = current.sourceEventId !== due.sourceEventId;
          }
        })()
          .catch(() => console.error('[runtime] Replica copy or verification failed.'))
          .finally(() => {
            replicaCopy = undefined;
            scheduleReplicaCopy(progress ? 1000 : 60000);
          });
      }, delay);
      replicaTimer.unref();
    }
    serve = async (req, res) => {
      if (closing || queue.status !== 'running') {
        fail(503, '服务正在关闭或尚未就绪，请稍后重试。');
      }
      if (!privateAccessAllowed(req, env)) {
        res.writeHead(403, { 'content-type': 'text/plain; charset=utf-8' });
        res.end('Private access only');
        return;
      }
      for (const [key, value] of Object.entries(corsHeaders())) {
        res.setHeader(key, value);
      }
      res.setHeader('Referrer-Policy', 'no-referrer');
      res.setHeader('X-Content-Type-Options', 'nosniff');
      const url = new URL(req.url || '/', `http://${req.headers.host}`),
        method = req.method || 'GET';
      if (!sharedSurface(url.pathname, method).allowed) {
        fail(404, 'not found.');
      }
      if (
        /^\/api\/admin(?:\/|$)/.test(url.pathname) &&
        !/^(localhost|127\.0\.0\.1)(:\d+)?$/.test(String(req.headers.host || ''))
      ) {
        fail(404, 'not found.');
      }
      assertRelationalStorageReady(sqlite);
      if (method === 'OPTIONS') {
        res.writeHead(204, { 'access-control-max-age': '86400' });
        res.end();
        return;
      }
      if (url.pathname === '/api/health') {
        if (method === 'HEAD') {
          res.writeHead(200, {
            'content-type': 'application/json; charset=utf-8',
            'cache-control': 'no-store',
          });
          res.end();
        } else {
          sendJson(res, 200, { ok: true });
        }
        return;
      }
      if (url.pathname === '/api/admin/ping') {
        assertAdmin(req, url);
        sendJson(res, 200, { ok: true });
        return;
      }
      for (const handle of memberHandlers) {
        if (await handle(req, res, url)) {
          return;
        }
      }
      // Cookie/管理路径不进入 Raw Key 路由，也不回退到旧 Store 的未实现操作。
      if (url.pathname.startsWith('/api/member/') || url.pathname.startsWith('/api/admin/')) {
        fail(404, 'not found.');
      }
      if (
        (quotaHttpRoute(method, url.pathname) ||
          /^\/api\/requests\//.test(url.pathname) ||
          /^\/(?:v1\/)?ai\//.test(url.pathname)) &&
        !gatewayKeyFrom(req, url).startsWith('STA1N-')
      ) {
        fail(401, 'Existing gateway key required.');
      }
      for (const handle of apiHandlers) {
        if (await handle(req, res, url)) {
          return;
        }
      }
      fail(404, 'not found.');
    };
    await queue.prepare();
    await reconcileMemberSessionOwner(
      new RelationalMemberStore(sqlite, env, relationalStateNotificationsFor(sqlite, events)),
      env,
    );
    server = http.createServer((req, res) => {
      requests.set(res, req);
      const done = () => requests.delete(res);
      res.once('finish', done);
      res.once('close', done);
      const pending = Promise.resolve()
        .then(() => handler(req, res))
        .catch((error) => renderError(req, res, error));
      handlers.add(pending);
      void pending.finally(() => handlers.delete(pending));
    });
    await new Promise<void>((resolve, reject) => {
      server!.once('error', reject);
      server!.listen(port, host, () => {
        server!.off('error', reject);
        resolve();
      });
    });
    assertRelationalStorageReady(sqlite);
    queue.activate();
    refresh.start();
    scheduleReplicaCopy(60000);
    process.once('SIGTERM', () => {
      console.log('[runtime] SIGTERM received; draining.');
      void exitAfterStop(0);
    });
    process.once('SIGINT', () => {
      console.log('[runtime] SIGINT received; draining.');
      void exitAfterStop(0);
    });
    const safety = (error: unknown) => {
      if (
        isAbortError(error) ||
        /ECONNRESET|ERR_STREAM_DESTROYED|socket hang up|direct generate timeout/.test(
          String((error as Error)?.message || ''),
        )
      ) {
        console.error('[runtime] recovered from async abort/socket error');
        return;
      }
      console.error('[runtime] Uncaught asynchronous failure.');
      void exitAfterStop(1);
    };
    process.on('uncaughtException', safety);
    process.on('unhandledRejection', safety);
    const address = server.address();
    console.log(
      `Nai2API listening on http://${host}:${typeof address === 'object' && address ? address.port : port}`,
    );
  } catch (error) {
    await stop();
    throw error;
  }
}

export async function handleGatewayRequest(req: Request, res: ServerResponse) {
  try {
    if (!serve) {
      fail(503, '服务尚未就绪。');
    }
    await serve(req, res);
  } catch (error) {
    renderError(req, res, error);
  } finally {
    req.generationPayloadClaim?.release();
    delete req.generationPayloadClaim;
  }
}
