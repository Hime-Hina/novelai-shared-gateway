import type {
  GenerationReservation as Reservation,
  GenerationQueueReservation as QueueReservation,
  GenerationContent as GeneratedImage,
} from './generation/execution.ts';
import { createHash, createHmac, randomBytes, timingSafeEqual } from 'node:crypto';
import { mkdir, open, readdir, rm, writeFile } from 'node:fs/promises';
import type { IncomingMessage, OutgoingHttpHeaders, Server, ServerResponse } from 'node:http';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createAccountQuotaRefresh } from './quota/account-refresh.ts';
import { archiveColdImage } from './storage/cold-images.ts';
import { requireDiscordMembership } from './members/discord-auth.ts';
import { recordDomainEvent } from './events/domain-events.ts';
import {
  configureFairQuota,
  estimateFairQuota,
  fairQuotaSummary,
  syncFairQuota,
} from './quota/fair-share.ts';
import {
  createGenerationAdmission,
  generationIntent,
  isNoCache,
  readGenerationIdempotencyKey,
  requestCacheKey,
  type GenerationIntent,
} from './generation/admission.ts';
import {
  createGenerationFailureHandlers,
  createJobDelivery,
  createRequestControls,
  deliverDirectGeneration,
  generationErrorImage,
} from './generation/delivery.ts';
import {
  completeGenerationCommand,
  isNovelAiCapacityError,
  reconcileGenerationReviewsCommand,
  recoverPendingGenerationResultsCommand,
  reserveQueuedGeneration,
} from './generation/execution.ts';
import {
  createJobPresentation,
  generationCost as generationDisplayCost,
  generationRequestForErrorLog,
  jobDurationMs,
  publicGenerationRequest,
  readOwnedGenerationRequest,
} from './generation/request-queries.ts';
import {
  createGenerationPayloadBudget,
  singleGenerationRequest,
} from './generation/batch-requests.ts';
import { JobPreviews } from './generation/job-previews.ts';
import { launcherSubscription } from './quota/launcher-subscription.ts';
import { reconcileMemberSessionOwner } from './members/authentication.ts';
import {
  closeMemberGenerationStreams,
  createMemberGenerationHttp,
  memberGenerationError,
} from './generation/member-http.ts';
import { handleMemberRoutes } from './members/http-routes.ts';
import { createNativeHttp } from './protocols/native-http.ts';
import { imageExtension } from './protocols/native-stream.ts';
import {
  OfficialKeyJobs,
  isOfficialKey,
  officialKeyError,
  officialKeyOwner,
} from './protocols/official-key-jobs.ts';
import {
  openAiModelsResponse as listOpenAiModels,
  openAiFixedSteps,
  openAiSizeTiers,
} from './protocols/openai-models.ts';
import { ownerOperation } from './members/owner-operations.ts';
import { adminPromptApiConfig } from './http/prompt-api-settings.ts';
import {
  DIRECT_URL_MAX_STEPS,
  MAX_STEPS,
  buildErrorImage,
  generateNovelAiImage,
  normalizeNovelAiRequest,
} from './protocols/novelai-client.ts';
import {
  errorDetailMessage,
  isAbortError,
  isInsufficientBalanceError,
  isTimeoutResultMessage,
  publicErrorMessage,
} from './http/public-errors.ts';
import { privateAccessAllowed } from './http/public-hosts.ts';
import {
  physicalAccountLoad as countPhysicalAccountLoad,
  createGenerationControls,
  createQueueDispatcher,
} from './generation/queue-dispatch.ts';
import { ImageStorage, R2Storage, r2Config, storedImagePath } from './storage/r2-storage.ts';
import { publishStoredResult } from './storage/result-files.ts';
import { cooldownRetryAfter } from './quota/shared-capacity.ts';
import { sharedSurface } from './http/api-surface.ts';
import { stateEventsFor } from './events/state-notifications.ts';
import {
  JsonStore,
  MAX_CACHE_IMAGES_LIMIT,
  createId,
  createPublicToken,
  defaultArtist2_5D,
  hashObject,
  legacyDefaultArtist,
  maskToken,
} from './storage/sqlite-store.ts';

import type { PreviewProgress } from './generation/job-previews.ts';
import type { NormalizedImageRequest } from './protocols/novelai-client.ts';
import type {
  PersistenceOptions,
  StoreDatabase,
  StoreItem,
  StoredGenerationRequest,
} from './storage/sqlite-store.ts';

type Settings = StoreDatabase['settings'];
type Body = Record<string, unknown>;
type PayloadClaim = ReturnType<ReturnType<typeof createGenerationPayloadBudget>['claim']>;
type GenerateOptions = NonNullable<Parameters<typeof generateNovelAiImage>[3]>;
type OfficialJob = ReturnType<OfficialKeyJobs['get']>;
interface GatewayRequest extends IncomingMessage {
  publicParsedBody?: Body;
  generationPayloadClaim?: PayloadClaim;
  generationIdempotencyKey?: string | null;
}
interface GatewayResponse extends ServerResponse {
  generationDeliveryTracked?: boolean;
}
interface GatewayError extends Error {
  statusCode?: number;
  code?: string;
  cooldownUntil?: string;
  requestId?: string;
  upstreamStatus?: number;
  upstreamRetryAfterUntil?: string;
  status?: number;
  errors?: unknown[];
}

type ExecutionRequest = StoredGenerationRequest;
type RequestSummary = Partial<ExecutionRequest>;
type StoredImage = Pick<StoreItem, 'file' | 'mimeType' | 'r2'> & { id?: string; base64?: string };
type StoredResult = StoredImage & {
  id?: string;
  imageId?: string;
  operation?: string;
  sha256?: string;
  byteLength?: number;
};
interface Progress extends PreviewProgress {
  percent?: number;
  step?: number;
  total?: number;
  accountId?: string;
  accountRouteId?: number;
  eventType?: string;
  updatedAt?: string;
}

interface JobMetadata {
  id: string;
  requestId?: string | null;
  jobIds?: string[];
  count?: number;
  replayed?: boolean;
  cacheHit?: boolean;
  balance?: number;
  replayImages?: { id: string; token?: string | null }[];
  cached?: StoredImage;
}

interface JobOptions {
  native?: boolean;
  frontend?: boolean;
  source?: string;
  deadlineAt?: string;
  admissionVersion?: number;
  batchId?: string;
  sampleIndex?: number;
  generationPayloadClaim?: PayloadClaim;
  idempotencyKey?: string | null;
  idempotency?: GenerationIntent | null;
  status?: string;
  accountId?: string;
  imageId?: string;
  cost?: number;
}
interface Stats {
  total?: number;
  done?: number;
  failed?: number;
  active?: number;
  success?: number;
}
interface BoundGenerationOptions extends Pick<GenerateOptions, 'signal' | 'forceStream'> {
  onProgress?: ((progress: Progress) => void) | null;
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');
const dataDir = process.env.DATA_DIR || path.join(rootDir, 'data');
const imageDir = path.join(dataDir, 'images');
const port = Number(process.env.PORT || 8080);
const host = process.env.HOST || '0.0.0.0';
const adminToken = String(process.env.ADMIN_TOKEN || '').trim();
if (adminToken.length < 32 || /^(change|replace|example)/i.test(adminToken)) {
  throw new Error('请设置至少 32 位随机 ADMIN_TOKEN');
}
// 公开发行版始终采用共享账户与鉴权模式，空库也不开放注册或自动模拟。
process.env.FAIR_SHARE_MODE = 'true';
process.env.PRIVATE_MODE = 'true';
process.env.PUBLIC_MODE = 'true';
process.env.NO_AUTO_DELETE = 'true';
process.env.MOCK_WHEN_NO_ACCOUNT = 'false';
const store = new JsonStore(dataDir);
const coldConfig = r2Config();
const r2Storage = coldConfig ? new R2Storage(coldConfig) : null;
const imageStorage = new ImageStorage(dataDir, r2Storage, (id) => store.findImage(id));
const generationPayloadBudget = createGenerationPayloadBudget(() =>
  store.activeGenerationPayloadBytes([...runningJobControls.values()]),
);
const admitGeneration = createGenerationAdmission({
  store,
  payloadBudget: generationPayloadBudget,
  artifactAvailable: (image) => imageStorage.available(image),
  cleanupStaleActiveJobs: (reason) => cleanupStaleActiveJobs(reason),
});

let queueDispatchEnabled = false;
let runtimeClosing = false;

let generationReviewTimer: ReturnType<typeof setTimeout> | null = null;
let generationReviewPromise: Promise<unknown> | null = null;
let coldImageTimer: ReturnType<typeof setTimeout> | null = null;
let coldImagePromise: Promise<unknown> | null = null;

const {
  controls: runningJobControls,
  createRunningJobControl,
  finishRunningJobControl,
} = createGenerationControls({
  timeoutDelay: runningJobTimeoutDelay,
  schedule: scheduleQueueDrain,
});
const directGenerateTimeoutMs = Number(process.env.DIRECT_GENERATE_TIMEOUT_MS || 60_000);
const openAiChatTimeoutMs = Number(process.env.OPENAI_CHAT_TIMEOUT_MS || 10 * 60_000);
const openAiQueuePollMs = 650;

const jobStreamProgress = new Map<string, Progress>();
const jobPreviews = new JobPreviews();
const handleMemberGeneration = createMemberGenerationHttp({
  store,
  admit: admitGeneration,
  budget: generationPayloadBudget,
  imageStorage,
  previews: jobPreviews,
  scheduleQueueDrain,
});
const officialJobs = new OfficialKeyJobs({
  generate: (request, account, options) =>
    generateNovelAiImage(request, account, process.env, options),
  saveImage: saveOfficialImage,
  findImage: (cacheKey) => store.findImageByCacheKey(cacheKey),
  previews: jobPreviews,
});
const jobStreamProgressPersistState = new Map<string, { percent: number; at: number }>();
const beijingOffsetMs = 8 * 60 * 60 * 1000;
const usageChartDays = 7;
const errorLogRetentionMs = usageChartDays * 24 * 60 * 60 * 1000;
const accountQuotaRefreshIntervalMs = 60 * 1000;
const accountQuotaRequestTimeoutMs = 15 * 1000;

const { publicJob, publicJobSnapshot } = createJobPresentation({
  store,
  imageUrl: publicImageUrl,
  errorMessage: (message) => publicErrorMessage(message, { generation: true }),
  progress: publicGenerationProgress,
});
const jobDelivery = createJobDelivery({
  store,
  errorMessage: (message) => publicErrorMessage(message, { generation: true }),
  snapshot: publicJobSnapshot,
  sendStreamHeaders: sendOpenAiStreamHeaders,
  imageMarkdown: (job) => job.imageUrl,
  pollMs: openAiQueuePollMs,
  log: (value) => console.log('[queue]', JSON.stringify(value)),
});
const { waitForJobResult, notifyJobWaiters } = jobDelivery;
const { cancelGenerationRequest, changeMemberAdmission } = createRequestControls(store, {
  clearProgress: clearJobStreamProgress,
  notify: notifyJobWaiters,
  schedule: scheduleQueueDrain,
  readRequest: getGenerationRequest,
});
const failureHandlers = createGenerationFailureHandlers(store, {
  publicErrorMessage: (message) => publicErrorMessage(message, { generation: true }),
  errorDetailMessage,
  clearProgress: clearJobStreamProgress,
  notify: notifyJobWaiters,
  schedule: scheduleQueueDrain,
  executionEvidence: (jobId) => runningJobControls.get(jobId)?.reservation,
  log: (value) => console.log('[queue]', JSON.stringify(value)),
});
const { failGeneration, cancelReservedJob, cleanupStaleActiveJobs } = failureHandlers;
const queueDispatcher = createQueueDispatcher({
  store,
  controls: runningJobControls,
  enabled: () => queueDispatchEnabled,
  cleanup: cleanupStaleActiveJobs,
  reserve: reserveQueuedJob,
  run: runReservedJob,
  notify: notifyJobWaiters,
  log: (value) => console.log('[queue]', JSON.stringify(value)),
  error: (error) => console.error(error),
});
const accountQuotaRefresh = createAccountQuotaRefresh({
  store,
  requestTimeoutMs: accountQuotaRequestTimeoutMs,
  refreshIntervalMs: accountQuotaRefreshIntervalMs,
  publicAccount: (account) => publicAccount(account, { revealToken: true }),
  publicErrorMessage,
});
const { refreshEnabled: refreshEnabledAccountQuotas, fetch: fetchNovelAiAccountQuotaWithTimeout } =
  accountQuotaRefresh;
const nativeHttp = createNativeHttp({
  store,
  admitGeneration,
  readJson,
  waitForJobResult,
  readStoredImage,
  sendImage,
  previews: jobPreviews,
  schedule: scheduleQueueDrain,
  timeoutMs: openAiChatTimeoutMs,
  logDiagnostic: (id, event, details) =>
    console.warn(
      '[native-diagnostic] ' +
        JSON.stringify({ time: new Date().toISOString(), id, event, ...details }),
    ),
});

// 旧入口与 Nitro listener 共用一个宿主；恢复完成前不监听，监听成功后才派发。
export async function startGateway(handler: http.RequestListener) {
  installRuntimeSafetyHandlers();
  await store.init();
  await reconcileMemberSessionOwner(store);
  const createdImageDirectory = await mkdir(imageDir, { recursive: true });
  if (createdImageDirectory && process.platform !== 'win32') {
    const directory = await open(dataDir, 'r');
    try {
      await directory.sync();
    } finally {
      await directory.close();
    }
  }
  await recoverPendingGenerationResults();
  await cleanupInterruptedStartupJobs();
  await cleanupStaleActiveJobs('startup');
  await migrateInlineImages();
  await ensureAccountRouteIds();
  await applyRuntimeSettings();
  await cleanupImageStorage().catch((error: unknown) =>
    console.error('Failed to cleanup image storage:', error),
  );
  // Recovery precedes the verified cutover; no listener or dispatcher can race it.
  await store.enableEventAuthority();

  const server = http.createServer(handler);
  server.listen(port, host, () => {
    queueDispatchEnabled = true;
    console.log(`Nai2API listening on http://${host}:${port}`);
    startAccountQuotaAutoRefresh();
    startGenerationReviews();
    startColdImages();
    logStartupQueueState()
      .catch((error: unknown) => console.error('[runtime] failed to inspect startup queue:', error))
      .finally(() => {
        scheduleQueueDrain();
      });
  });
  installShutdownHandlers(server);
}

export async function handleGatewayRequest(req: GatewayRequest, res: GatewayResponse) {
  // PRIVATE_ACCESS_GUARD
  // 私有服务只接受本机隧道入口，阻断跨站请求与域名重绑定。
  if (process.env.PRIVATE_MODE === 'true') {
    if (!privateAccessAllowed(req)) {
      res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Private access only');
      return;
    }
  }
  try {
    if (runtimeClosing) {
      throw httpError(503, '服务正在关闭，请稍后重试。');
    }
    await route(req, res);
  } catch (error) {
    if (res.destroyed || res.writableEnded) {
      return;
    }
    if (res.headersSent) {
      res.destroy();
      return;
    }
    const retryAfter = cooldownRetryAfter((error as GatewayError).cooldownUntil);
    if (retryAfter) {
      res.setHeader('Retry-After', String(retryAfter));
    }
    if ((error as GatewayError).requestId) {
      res.setHeader('X-Nai-Request-Id', (error as GatewayError).requestId!);
    }
    if (
      /^\/api\/member\/(?:generation\/|requests\/[^/]+\/(?:events|inputs?)|jobs\/[^/]+\/content)/.test(
        req.url || '',
      )
    ) {
      const payload = memberGenerationError(error);
      sendJson(res, payload.statusCode, payload);
      return;
    }
    if (
      /^\/generate(?:\?|$)/.test(req.url || '') &&
      !(
        process.env.PUBLIC_MODE === 'true' &&
        [401, 403].includes((error as GatewayError).statusCode!)
      )
    ) {
      const image = buildErrorImage(
        publicErrorMessage((error as GatewayError).message || 'Generation failed'),
      );
      sendImage(res, 200, image.mimeType, image.buffer, { 'x-error': '1' });
      return;
    }
    if (
      /^\/(?:v1\/)?ai\/(?:generate-image(?:-stream)?|encode-vibe|upscale)$/.test(
        String(req.url || '').split('?')[0],
      )
    ) {
      const message = publicErrorMessage(
        (error as GatewayError).message || 'Internal server error',
      );
      const code = Number((error as GatewayError).statusCode);
      const statusCode = Number.isInteger(code) && code >= 400 && code <= 599 ? code : 500;
      sendJson(res, statusCode, { message, statusCode, error: message });
      return;
    }
    if (req.url?.startsWith('/v1/')) {
      sendOpenAiError(
        res,
        (error as GatewayError).statusCode || 500,
        publicErrorMessage((error as GatewayError).message || 'Internal server error'),
        openAiErrorType(error),
      );
      return;
    }
    sendJson(res, (error as GatewayError).statusCode || 500, {
      error: publicErrorMessage((error as GatewayError).message || 'Internal server error'),
    });
  } finally {
    req.generationPayloadClaim?.release();
    delete req.publicParsedBody;
  }
}

async function applyRuntimeSettings() {
  const publicBaseUrl = normalizePublicBaseUrl(process.env.PUBLIC_BASE_URL || '');
  await store.update(
    (db) => {
      if (publicBaseUrl && !db.settings.publicBaseUrl) {
        db.settings.publicBaseUrl = publicBaseUrl;
      }
      if (!db.settings.defaultArtist || db.settings.defaultArtist === legacyDefaultArtist) {
        db.settings.defaultArtist = defaultArtist2_5D;
      }
    },
    { collections: ['settings'] },
  );
}

async function migrateInlineImages() {
  if (store.hasPartialCollection('images')) {
    return;
  }
  const { images } = await store.readCollections(['images']);
  let migrated = 0;
  for (const image of images) {
    if (!image.base64 || image.file) {
      continue;
    }
    const buffer = Buffer.from(image.base64, 'base64'),
      file = imageStorageName(image.id, image.mimeType);
    try {
      // Legacy bytes already belong to this artifact. File IO stays outside the
      // command; a crash leaves the original inline bytes available for retry.
      await writeFile(path.join(dataDir, file), buffer);
      const sha256 = createHash('sha256').update(buffer).digest('hex');
      const changed = await store.update(
        (db) => {
          const current = store.loadItemForUpdate('images', image.id);
          if (!current || current.file || current.base64 !== image.base64) {
            return false;
          }
          current.file = file;
          current.sha256 = sha256;
          current.byteLength = buffer.length;
          delete current.base64;
          recordDomainEvent(db, {
            eventType: 'ArtifactStorageChanged',
            streamId: `artifact:${image.id}`,
            payload: {
              artifactId: image.id,
              phase: 'local_published',
              file,
              mimeType: image.mimeType,
              sha256,
              byteLength: buffer.length,
            },
          });
          return true;
        },
        { dirtyRows: { images: [image.id] }, shouldPersist: Boolean },
      );
      if (changed) {
        migrated++;
      }
    } catch (error) {
      console.error(`Failed to migrate cached image ${image.id}:`, error);
    }
  }
  if (migrated) {
    console.log(`[runtime] migrated ${migrated} inline image(s)`);
  }
}
// PUBLIC_SELF_USE_GUARD
// 图片签名使用进程随机密钥；重启失效，不暴露账户或网关密钥。
const publicImageSecret = randomBytes(32);
function publicImageMac(pathname: string, expires: string) {
  return createHmac('sha256', publicImageSecret).update(`${pathname}\n${expires}`).digest('hex');
}
function publicImageUrl(id: string) {
  const pathname = `/api/images/${encodeURIComponent(id)}/content`;
  if (process.env.PUBLIC_MODE !== 'true') {
    return pathname;
  }
  const expires = String(Math.floor(Date.now() / 1000) + 900);
  return `${pathname}?expires=${expires}&signature=${publicImageMac(pathname, expires)}`;
}
function validPublicImageSignature(url: URL) {
  if (!/^\/api\/images\/[^/]+\/content$/.test(url.pathname)) {
    return false;
  }
  const expires = url.searchParams.get('expires') || '';
  const signature = url.searchParams.get('signature') || '';
  if (!/^\d{10}$/.test(expires) || !/^[a-f0-9]{64}$/.test(signature)) {
    return false;
  }
  const now = Math.floor(Date.now() / 1000);
  if (Number(expires) < now || Number(expires) > now + 900) {
    return false;
  }
  return timingSafeEqual(
    Buffer.from(signature, 'hex'),
    Buffer.from(publicImageMac(url.pathname, expires), 'hex'),
  );
}
async function publicSelfUseGuard(
  req: GatewayRequest,
  res: GatewayResponse,
  url: URL,
  method: string,
) {
  if (process.env.PUBLIC_MODE !== 'true' || method === 'OPTIONS') {
    return false;
  }
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  // 禁止卡密开户、余额合并、旧兼容接口，管理员仍可经私有隧道管理。
  if (['/api/redeem', '/api/me/merge', '/api/api/getUser'].includes(url.pathname)) {
    throw httpError(403, 'Self-service registration and merging are disabled.');
  }
  // 健康检查仅返回布尔状态，不公开账户数量或管理员配置。
  if (url.pathname === '/api/health' && ['GET', 'HEAD'].includes(method)) {
    // 本机隧道和容器健康检查保持原有返回，公网代理禁止该路径。
    if (/^(127\.0\.0\.1|localhost)(:\d{1,5})?$/.test(String(req.headers.host || ''))) {
      return false;
    }
    if (method === 'HEAD') {
      sendHead(res, 200, { 'content-type': 'application/json; charset=utf-8' });
    } else {
      sendJson(res, 200, { ok: true });
    }
    return true;
  }
  if (url.pathname === '/api/settings' && method === 'GET') {
    const settings = await store.readSettings();
    sendJson(res, 200, {
      defaultModel: settings.defaultModel,
      defaultNegative: settings.defaultNegative,
      defaults: settings.defaults,
    });
    return true;
  }
  const protectedPath =
    url.pathname.startsWith('/api/') ||
    url.pathname.startsWith('/v1/') ||
    url.pathname.startsWith('/ai/') ||
    url.pathname.startsWith('/user/') ||
    ['/generate', '/memory'].includes(url.pathname);
  if (!protectedPath) {
    return false;
  }
  // 公网管理员仅接受请求头；反向代理另外隔离所有管理路由。
  if (adminToken && req.headers['x-admin-token'] === adminToken) {
    return false;
  }
  if (method === 'GET' && validPublicImageSignature(url)) {
    return false;
  }
  let bodyToken = '';
  if (
    method === 'POST' &&
    ['/api/jobs', '/api/web/jobs', '/api/prompt/convert'].includes(url.pathname)
  ) {
    req.publicParsedBody = await readJson(req);
    if (!req.publicParsedBody || typeof req.publicParsedBody !== 'object') {
      throw httpError(400, 'invalid JSON body.');
    }
    bodyToken = String(req.publicParsedBody.token || '').trim();
  }
  const token = bodyToken || tokenFrom(req, url);
  // 官方 pst 密钥不得作为本网站登录凭据，只能由管理员加入上游池。
  if (!token.startsWith('STA1N-')) {
    throw httpError(401, 'Existing gateway key required.');
  }
  const user = await store.readUserByToken(token);
  if (!user || user.enabled === false) {
    throw httpError(401, 'invalid token.');
  }
  await requireDiscordMembership(user, { store });
  return false;
}

async function route(req: GatewayRequest, res: GatewayResponse) {
  const url = new URL(req.url!, `http://${req.headers.host}`);
  const method = req.method || 'GET';
  {
    const surface = sharedSurface(url.pathname, method);
    if (!surface.allowed) {
      throw httpError(404, 'not found.');
    }
    url.pathname = surface.pathname;
    // 管理员 API 只从本机隧道进入，不能绕过代理的路径规则。
    if (
      /^\/api\/admin(?:\/|$)/.test(url.pathname) &&
      !/^(localhost|127\.0\.0\.1)(:\d+)?$/.test(String(req.headers.host || ''))
    ) {
      throw httpError(404, 'not found.');
    }
  }
  if (
    await handleMemberRoutes(req, res, url, {
      store,
      assertAdmin,
      createId,
      createPublicToken,
      publicImageUrl,
      changeMemberAdmission,
      getGenerationRequest,
      cancelGenerationRequest,
      handleMemberGeneration,
      ownerOperation: (url, method, body, command) =>
        ownerOperation(
          url,
          method,
          body,
          {
            store,
            updateAccount: (id, input, options) =>
              updateAccount(id, input, process.env.OWNER_USER_ID || 'local-admin', options),
            refreshAccountQuotas,
          },
          command,
        ),
    })
  ) {
    return;
  }
  // 与实际路由使用同一个规范化路径，必须在公网守卫读取正文之前占用预算。
  if (
    (method === 'POST' &&
      (['/api/jobs', '/api/web/jobs', '/v1/chat/completions'].includes(url.pathname) ||
        /^\/(?:v1\/)?ai\/(?:generate-image(?:-stream)?|encode-vibe|upscale)$/.test(
          url.pathname,
        ))) ||
    (method === 'GET' && url.pathname === '/generate')
  ) {
    req.generationPayloadClaim = generationPayloadBudget.claim();
    req.generationIdempotencyKey = readGenerationIdempotencyKey(req);
    if (method === 'GET') {
      req.generationPayloadClaim.resize(Buffer.byteLength(url.search));
    }
  }
  if (await publicSelfUseGuard(req, res, url, method)) {
    return;
  }

  if (method === 'OPTIONS') {
    sendCorsPreflight(res);
    return;
  }

  if (method === 'HEAD' && url.pathname === '/api/health') {
    sendHead(res, 200, { 'content-type': 'application/json; charset=utf-8' });
    return;
  }

  // 第三方 NAI 启动器只读取调用者本地钱包，不转发共享上游的原始订阅数据。
  if (
    method === 'GET' &&
    [
      '/user/subscription',
      '/v1/user/subscription',
      '/ai/user/subscription',
      '/v1/ai/user/subscription',
    ].includes(url.pathname)
  ) {
    const subscription = await store.update(
      (db) => launcherSubscription(db, getUserOrThrow(db, tokenFrom(req, url))),
      { collections: ['settings'] },
    );
    sendJson(res, 200, subscription);
    return;
  }
  if (await nativeHttp.handle(req, res, url)) {
    return;
  }

  if (method === 'GET' && url.pathname === '/v1/models') {
    sendJson(res, 200, openAiModelsResponse());
    return;
  }

  if (method === 'POST' && url.pathname === '/v1/chat/completions') {
    await handleOpenAiChatCompletion(req, res);
    return;
  }

  if (method === 'GET' && url.pathname === '/api/health') {
    const counts = await store.readCounts();
    sendJson(res, 200, {
      ok: true,
      service: 'Nai2API',
      users: counts.users,
      enabledAccounts: counts.enabledAccounts,
      cards: counts.cards,
      adminConfigured: adminToken !== '123456',
    });
    return;
  }

  if (method === 'GET' && url.pathname === '/api/me/quota') {
    const quota = await store.update(
      (db) => {
        const user = getUserOrThrow(db, tokenFrom(req, url));
        const now = Date.now(),
          draft = estimateFairQuota(db, now);
        const quota = fairQuotaSummary(draft, user, now);
        return quota && { ...quota, freeImages: store.readFreeImageUsage(draft, user.id) };
      },
      { persist: false },
    );
    sendJson(res, 200, { fairQuota: quota });
    return;
  }

  if (method === 'POST' && url.pathname === '/api/admin/fair-quota') {
    assertAdmin(req, url);
    const body = await readJson(req);
    const result = await store.update(
      (db) => {
        if (
          !Array.isArray(body.memberIds) ||
          body.memberIds.some(
            (id: unknown) => !db.users.some((u) => u.id === id && u.enabled !== false),
          )
        ) {
          throw httpError(400, '成员必须是已启用的用户');
        }
        configureFairQuota(db, body.memberIds);
        syncFairQuota(db);
        return {
          status: db.settings.fairQuota!.status,
          members: body.memberIds as unknown[],
          globalConcurrency: 4,
        };
      },
      { collections: ['settings', 'ledger'] },
    );
    sendJson(res, 200, result);
    return;
  }

  if (method === 'GET' && url.pathname === '/api/admin/summary') {
    const startedAt = Date.now();
    assertAdmin(req, url);
    const readStartedAt = Date.now();
    const db: Awaited<ReturnType<JsonStore['readAdminSummary']>> & { statsJobs?: StoreItem[] } =
      await store.readAdminSummary({ fresh: url.searchParams.get('fresh') === '1' });
    const readMs = Date.now() - readStartedAt;
    resetStaleAccountLoads(db.accounts);
    const revealTokens = url.searchParams.get('revealTokens') === '1';
    const statsJobs = db.statsJobs || db.jobs || [];
    const errorLogJobs = db.errorJobs || statsJobs;
    const computeStartedAt = Date.now();
    const accountStats1h = (db.accountStats1h ||
      Object.fromEntries(accountStatsMapSince(statsJobs, 60 * 60 * 1000))) as Record<
      string,
      ReturnType<typeof finalizeStats>
    >;
    const payload = {
      settings: adminRuntimeSettings(db.settings),
      userCount: Number(db.userCount || 0),
      accounts: db.accounts.map((account) =>
        publicAccount(account, {
          revealToken: revealTokens,
          stats1h: accountStats1h[account.id] || finalizeStats({}),
        }),
      ),
      imageCount: db.imageCount ?? 0,
      imageTotal: db.imageCount ?? 0,
      cacheImageCount: db.imageCount ?? 0,
      requestStats1m: db.requestStats1m || requestStatsSince(statsJobs, 60 * 1000),
      generationStats1m: db.generationStats1m || { total: 0 },
      jobStats1h: db.jobStats1h || jobStatsSince(statsJobs, 60 * 60 * 1000),
      generationSpeed1h: db.generationSpeed1h || {
        v45: { seconds: null, count: 0 },
        v5: { seconds: null, count: 0 },
      },
      usageHourlyDays: db.usageHourlyDays || hourlyUsageStatsByDay(statsJobs),
      errorLogs: errorLogs(errorLogJobs, db, 100),
      jobs: db.jobs.slice(0, 50).map((job) => publicJob(job, db)),
    };
    const computeMs = Date.now() - computeStartedAt;
    const sendStartedAt = Date.now();
    sendJson(res, 200, payload);
    const sendMs = Date.now() - sendStartedAt;
    runtimeRequestLog('admin summary', startedAt, {
      readMs,
      computeMs,
      sendMs,
      users: db.userCount,
      accounts: db.accounts.length,
      statsRows: db.statsRowsRead ?? statsJobs.length,
      recentJobs: db.jobs.length,
      images: db.imageCount ?? 0,
    });
    return;
  }

  if (method === 'GET' && url.pathname === '/api/admin/ping') {
    assertAdmin(req, url);
    sendJson(res, 200, { ok: true });
    return;
  }

  if (method === 'POST' && url.pathname === '/api/admin/accounts') {
    assertAdmin(req, url);
    const body = await readJson(req);
    const account = await addAccount(body);
    scheduleQueueDrain();
    sendJson(res, 201, publicAccount(account));
    return;
  }

  if (method === 'PATCH' && url.pathname === '/api/admin/accounts') {
    assertAdmin(req, url);
    const body = await readJson(req);
    const accounts = await updateAccounts(body);
    scheduleQueueDrain();
    sendJson(res, 200, { accounts: accounts.map(publicAccount) });
    return;
  }

  if (method === 'POST' && url.pathname === '/api/admin/accounts/quota') {
    assertAdmin(req, url);
    const body = await readJson(req);
    const result = await refreshAccountQuotas(body);
    sendJson(res, 200, result);
    return;
  }

  if (method === 'PATCH' && url.pathname.startsWith('/api/admin/accounts/')) {
    assertAdmin(req, url);
    const id = decodeURIComponent(url.pathname.split('/').pop() || '');
    const body = await readJson(req);
    const account = await updateAccount(id, body);
    scheduleQueueDrain();
    sendJson(res, 200, publicAccount(account));
    return;
  }

  const requestRoute = url.pathname.match(/^\/api\/requests\/([^/]+)(\/cancel)?$/);
  if (
    requestRoute &&
    ((method === 'GET' && !requestRoute[2]) || (method === 'POST' && requestRoute[2]))
  ) {
    const user = await store.readUserByToken(bearerToken(req));
    if (!user || user.enabled === false) {
      throw httpError(401, 'invalid token.');
    }
    const requestId = decodeURIComponent(requestRoute[1]);
    if (requestRoute[2]) {
      await readJson(req, 32768);
    }
    const result = requestRoute[2]
      ? await cancelGenerationRequest(requestId, user.id)
      : await getGenerationRequest(requestId, user.id);
    sendJson(res, 200, result);
    return;
  }

  if (method === 'POST' && ['/api/jobs', '/api/web/jobs'].includes(url.pathname)) {
    const body = await readJson(req);
    if (body.edit) {
      throw httpError(400, '图片编辑功能已移除，请刷新页面后重试。');
    }
    const token = String(body.token || tokenFrom(req, url) || '');
    if (isOfficialKey(token)) {
      sendJson(res, 202, await createOfficialJob(token, body, url.pathname === '/api/web/jobs'));
      return;
    }
    const job = await createJob(token, body, {
      frontend: url.pathname === '/api/web/jobs',
      generationPayloadClaim: req.generationPayloadClaim,
      idempotencyKey: req.generationIdempotencyKey,
    });
    scheduleQueueDrain();
    const snapshot = await store.findJobContext(job.id);
    sendJson(res, 202, publicJob(snapshot?.job || job, snapshot));
    return;
  }

  if (method === 'GET' && url.pathname === '/api/jobs/events') {
    const token = tokenFrom(req, url);
    if (isOfficialKey(token)) {
      await officialKeyInfo(token);
      jobPreviews.subscribe(officialKeyOwner(token), res);
      return;
    }
    const user = await store.readUserByToken(token);
    if (!user || user.enabled === false) {
      throw httpError(401, 'invalid token.');
    }
    jobPreviews.subscribe(token, res);
    return;
  }

  if (method === 'GET' && /^\/api\/jobs\/[^/]+\/content$/.test(url.pathname)) {
    const id = decodeURIComponent(url.pathname.split('/').at(-2) || '');
    const token = tokenFrom(req, url);
    if (id.startsWith('job_pst_')) {
      const job = officialJobs.get(id, token);
      if (job.status !== 'done') {
        throw httpError(409, job.error || 'job is not finished.');
      }
      await sendStoredImage(res, 200, job.image);
      return;
    }
    const snapshot = await store.findJobContext(id);
    const job = snapshot?.job;
    if (!job) {
      throw httpError(404, 'job not found.');
    }
    if (job.userToken !== token && !isAdmin(req, url)) {
      throw httpError(403, 'forbidden.');
    }
    if (job.status === 'done' && job.imageId) {
      const image = await store.findImage(job.imageId!);
      if (image) {
        await sendStoredImage(res, 200, image);
        return;
      }
    }
    if (job.status !== 'failed') {
      throw httpError(409, 'job is not finished.');
    }
    const image = buildErrorImage(
      publicErrorMessage(job.error || 'Generation failed', { generation: true }),
    );
    sendImage(res, 200, image.mimeType, image.buffer, { 'x-error': '1' });
    return;
  }

  if (method === 'GET' && url.pathname.startsWith('/api/jobs/')) {
    const id = decodeURIComponent(url.pathname.split('/').pop() || '');
    const token = tokenFrom(req, url);
    if (id.startsWith('job_pst_')) {
      sendJson(res, 200, officialJobs.snapshot(officialJobs.get(id, token)));
      return;
    }
    const snapshot = await store.findJobContext(id);
    const job = snapshot?.job;
    if (!job) {
      throw httpError(404, 'job not found.');
    }
    if (job.userToken !== token && !isAdmin(req, url)) {
      throw httpError(403, 'forbidden.');
    }
    sendJson(res, 200, publicJob(job, snapshot));
    return;
  }

  if (method === 'GET' && url.pathname.startsWith('/api/images/')) {
    const id = decodeURIComponent(url.pathname.split('/').at(-2) || '');
    const image = await store.findImage(id);
    if (!image) {
      throw httpError(404, 'image not found.');
    }
    // 签名是限时分享凭据；失效后仅图片所属成员或管理员可读取。
    if (
      !validPublicImageSignature(url) &&
      !isAdmin(req, url) &&
      image.token !== tokenFrom(req, url)
    ) {
      throw httpError(403, 'forbidden.');
    }
    await sendStoredImage(res, 200, image);
    return;
  }

  if (method === 'GET' && url.pathname === '/generate') {
    await handleDirectGenerate(url, res, req.generationPayloadClaim, req.generationIdempotencyKey);
    return;
  }

  throw httpError(404, 'not found.');
}

async function acceptDirectRequest(
  url: URL,
  claim: PayloadClaim | undefined,
  key: string | null | undefined,
): Promise<JobMetadata> {
  const token = String(url.searchParams.get('token') || '').trim();
  const rawParams = Object.fromEntries(url.searchParams.entries());
  if (key) {
    const intent = generationIntent(
      key,
      (settings) =>
        normalizeNovelAiRequest(rawParams, settings, { maxSteps: DIRECT_URL_MAX_STEPS }),
      {
        randomSeed: rawParams.seed === undefined || rawParams.seed === '',
        noCache: isNoCache(rawParams.nocache),
        claim,
      },
    );
    const job = await createDirectJob(token, null, '', {
      source: 'direct',
      generationPayloadClaim: claim,
      idempotency: intent,
    });
    scheduleQueueDrain();
    return job;
  }
  const db = await store.readCollections(['settings', 'users']);
  const request = normalizeNovelAiRequest(rawParams, db.settings, {
    maxSteps: DIRECT_URL_MAX_STEPS,
  });
  const cacheKey = requestCacheKey(token, request, rawParams.seed);
  const nocache = rawParams.nocache === '1' || rawParams.nocache === 'true';
  if (!nocache) {
    let cached = await store.findImageByCacheKey(cacheKey);
    if (cached) {
      try {
        if (!(await imageStorage.available(cached))) {
          throw new Error('empty image');
        }
      } catch (error) {
        console.error(`Cached image ${cached.id} is missing, regenerating:`, error);
        cached = null;
      }
    }
    if (cached) {
      const job = await createDirectJob(token, request, cacheKey, {
        status: 'done',
        accountId: cached.accountId || '',
        imageId: cached.id,
        cost: 0,
        source: 'direct',
        generationPayloadClaim: claim,
      });
      return {
        id: job.id,
        requestId: job.requestId,
        cached: {
          id: cached.id,
          file: cached.file,
          base64: cached.base64,
          mimeType: cached.mimeType,
          r2: cached.r2,
        },
        balance: getUserOrThrow(db, token).balance,
      };
    }
  }
  const job = await createDirectJob(token, request, cacheKey, {
    source: 'direct',
    generationPayloadClaim: claim,
  });
  scheduleQueueDrain();
  return { id: job.id, requestId: job.requestId };
}

async function handleDirectGenerate(
  url: URL,
  res: GatewayResponse,
  claim: PayloadClaim | undefined,
  key: string | null | undefined,
) {
  const token = String(url.searchParams.get('token') || '').trim();
  if (isOfficialKey(token)) {
    const snapshot = await createOfficialJob(
      token,
      Object.fromEntries(url.searchParams.entries()),
      false,
      directGenerateTimeoutMs,
    );
    const job = await officialJobs.get(snapshot.id, token).done!;
    if (job.status !== 'done') {
      throw httpError(502, job.error!);
    }
    await sendStoredImage(res, 200, job.image, {
      'cache-control': 'private, max-age=0',
      'x-cache': job.cacheHit ? 'hit' : 'miss',
      'x-auth-mode': 'official',
    });
    return;
  }
  const accepted = await acceptDirectRequest(url, claim, key);
  return deliverDirectGeneration(res, accepted, {
    waitForJobResult,
    timeoutMs: directGenerateTimeoutMs,
    sendStoredImage,
    sendBusyImage,
    sendTimeoutImage,
    isCapacityError: isNovelAiCapacityError,
  });
}

async function officialKeyInfo(token: string) {
  officialKeyOwner(token);
  try {
    const quota = await fetchNovelAiAccountQuotaWithTimeout(token);
    return {
      authMode: 'official',
      balance: null,
      anlas: quota.points,
      v5RemainingPercent: quota.v5UsageIsNegative ? 0 : quota.v5UsagePercent,
      membership: accountTierText(quota.tier, '会员未知'),
    };
  } catch (error) {
    throw officialKeyError(error);
  }
}

async function createOfficialJob(
  token: string,
  body: Body,
  frontend = false,
  timeoutMs = novelAiGenerateTimeoutMs(),
) {
  const owner = officialKeyOwner(token);
  const settings = await store.readSettings();
  const request = normalizeNovelAiRequest({ ...body, cost: undefined }, settings, {
    maxSteps: frontend ? MAX_STEPS : DIRECT_URL_MAX_STEPS,
  });
  request.steps = Math.floor(request.steps);
  request.cost = 0;
  // Existing site caching is shared; official-key caching must include its owner.
  const cacheKey = hashObject({ owner, request: requestCacheKey('', request, body!.seed) });
  return officialJobs.create(token, request, {
    cacheKey,
    noCache: isNoCache(body!.nocache),
    timeoutMs,
  });
}

async function saveOfficialImage(job: OfficialJob, image: GeneratedImage) {
  const request = job.request!;
  const id = createId('img');
  const file = await writeStoredImage(id, image);
  const saved = {
    id,
    token: job.owner,
    accountId: '',
    authMode: 'official',
    cacheKey: job.cacheKey,
    prompt: request.tag,
    fullPrompt: request.prompt,
    model: request.model,
    width: request.width,
    height: request.height,
    requestedSteps: request.requestedSteps ?? request.steps,
    routedSteps: request.steps,
    cost: 0,
    accountCost: 0,
    mimeType: image.mimeType,
    file,
    createdAt: new Date().toISOString(),
  };
  try {
    await store.update(
      (db) => {
        db.images.unshift(saved);
      },
      { collections: ['images'], dirtyRows: () => ({ images: [id] }) },
    );
  } catch (error) {
    await removeStoredImages([saved]);
    throw error;
  }
  const trimmed = await store.trimImageCache(null, { batchSize: imageCacheTrimBuffer() });
  await removeStoredImages(trimmed);
  return saved;
}

async function acceptOpenAiRequest(req: GatewayRequest) {
  const token = bearerToken(req);
  if (!token) {
    throw httpError(401, 'missing API key.');
  }
  const body = await readJson(req);
  if (req.generationIdempotencyKey) {
    let model!: string;
    const intent = generationIntent(
      req.generationIdempotencyKey,
      (settings) => {
        const parsed = parseOpenAiImageRequest(body, settings);
        model = parsed.model;
        const request: NormalizedImageRequest & Pick<ExecutionRequest, 'allowPurchasedAnlas'> =
          normalizeNovelAiRequest(parsed.request, settings, { maxSteps: DIRECT_URL_MAX_STEPS });
        request.allowPurchasedAnlas = parsed.request.allowPurchasedAnlas === true;
        return request;
      },
      {
        randomSeed: true,
        noCache: isNoCache((body.nai as Body | undefined)?.nocache ?? body!.nocache ?? '1'),
        claim: req.generationPayloadClaim,
      },
    );
    const job = await createJob(token, null, {
      source: 'openai',
      generationPayloadClaim: req.generationPayloadClaim,
      idempotency: intent,
    });
    scheduleQueueDrain();
    return { id: job.id, requestId: job.requestId, model, stream: body.stream === true };
  }
  const settings = await store.readSettings();
  const parsed = parseOpenAiImageRequest(body, settings);
  const job = await createJob(token, parsed.request, {
    source: 'openai',
    generationPayloadClaim: req.generationPayloadClaim,
  });
  scheduleQueueDrain();
  // 完整正文和解析参数只在受理作用域保留，同步/SSE 等待仅携带小元数据。
  return {
    id: job.id,
    requestId: job.requestId,
    model: parsed.model,
    stream: body.stream === true,
  };
}

async function handleOpenAiChatCompletion(req: GatewayRequest, res: GatewayResponse) {
  const job = await acceptOpenAiRequest(req);
  if (job.requestId) {
    res.setHeader('X-Nai-Request-Id', job.requestId);
  }
  if (res.destroyed) {
    return;
  }
  const deadline = Date.now() + openAiChatTimeoutMs;
  if (job.stream) {
    await streamOpenAiImageJob(req, res, job, job.model, deadline);
    return;
  }
  const result = await waitForJobResult(job.id, deadline, res);
  if (res.destroyed) {
    return;
  }
  if (!result) {
    throw httpError(504, 'direct generate timeout');
  }
  if (result.error) {
    throw Object.assign(
      httpError(
        result.statusCode || (isTimeoutResultMessage(result.error) ? 504 : 500),
        result.error,
      ),
      { cooldownUntil: result.cooldownUntil },
    );
  }
  sendJson(
    res,
    200,
    openAiChatCompletionResponse({
      model: job.model,
      content: openAiImageMarkdown(req, result.saved!),
      id: `chatcmpl-${job.id}`,
    }),
  );
}

function openAiModelsResponse() {
  return listOpenAiModels();
}

function parseOpenAiImageRequest(body: Body = {}, settings: Partial<Settings> = {}) {
  const modelParts = parseOpenAiModel(
    body.model || settings.defaultModel || 'nai-diffusion-4-5-full',
  );
  const messageText = lastUserMessageText(body.messages || []);
  const fields = parseChinesePromptFields(messageText);
  validateOpenAiPromptFormat(fields, messageText);
  const nai: Body = body.nai && typeof body.nai === 'object' ? (body.nai as Body) : {};
  const prompt = String(nai.tag || nai.prompt || fields.tag || '').trim();
  const negative =
    String(nai.negative ?? fields.negative ?? '').trim() || settings.defaultNegative || '';
  const sizeName = String(nai.size ?? fields.size ?? settings.defaults?.size ?? '竖图').trim();
  const tierSize = modelParts.tier?.sizes?.[sizeName];

  const request: Body & { tag: string; model: string } = {
    tag: prompt,
    model: modelParts.model,
    artist: nai.artist ?? fields.artist ?? settings.defaultArtist ?? '',
    size: sizeName,
    width: tierSize?.width ?? nai.width,
    height: tierSize?.height ?? nai.height,
    steps: openAiFixedSteps,
    scale: nai.scale ?? fields.scale ?? settings.defaults?.scale,
    cfg: nai.cfg ?? fields.cfg ?? settings.defaults?.cfg,
    sampler: nai.sampler ?? fields.sampler ?? modelParts.sampler ?? settings.defaults?.sampler,
    negative,
    nocache: nai.nocache ?? body!.nocache ?? '1',
    noise_schedule:
      nai.noise_schedule ?? nai.noiseSchedule ?? settings.defaults?.noiseSchedule ?? 'karras',
  };

  return {
    model: modelParts.original,
    request,
  };
}

function validateOpenAiPromptFormat(fields: Record<string, string>, messageText: string) {
  const requiredFields = ['tag', 'size', 'scale', 'cfg'];
  const missing = requiredFields.some((key) => !String(fields[key] ?? '').trim());
  const optionalFieldsPresent =
    Object.hasOwn(fields, 'artist') && Object.hasOwn(fields, 'negative');
  if (missing || !optionalFieldsPresent) {
    throw httpError(400, openAiPromptFormatError());
  }
}

function openAiPromptFormatError() {
  return '请求格式错误，请参考群内使用指南';
}

function parseOpenAiModel(modelValue: unknown) {
  const original = String(modelValue || 'nai-diffusion-4-5-full');
  const tierMatch = original.match(/^\[(2K|4K)\]\s*(.+)$/i);
  const tierName = tierMatch ? tierMatch[1].toUpperCase() : '';
  const modelWithSampler = tierMatch ? tierMatch[2] : original;
  const [model, sampler] = modelWithSampler.split(':');
  return {
    original,
    tierName,
    tier: openAiSizeTiers[tierName] || null,
    model: model || 'nai-diffusion-4-5-full',
    sampler: sampler || '',
  };
}

function lastUserMessageText(messages: unknown) {
  const list: { role?: unknown; content?: unknown }[] = Array.isArray(messages) ? messages : [];
  const message = [...list].reverse().find((item) => item?.role === 'user') || list.at(-1);
  return messageContentText(message?.content || '');
}

function messageContentText(content: unknown) {
  if (typeof content === 'string') {
    return content;
  }
  if (!Array.isArray(content)) {
    return String(content || '');
  }
  return content
    .map((part: string | { type?: unknown; text?: unknown } | null) => {
      if (typeof part === 'string') {
        return part;
      }
      if (part?.type === 'text') {
        return part.text || '';
      }
      return part?.text || '';
    })
    .filter(Boolean)
    .join('\n');
}

function parseChinesePromptFields(text: string) {
  const fieldNames: Record<string, string> = {
    提示词: 'tag',
    畫師串: 'artist',
    画师串: 'artist',
    尺寸: 'size',
    提示词引导值: 'scale',
    提示詞引導值: 'scale',
    缩放引导值: 'cfg',
    縮放引導值: 'cfg',
    负面提示词: 'negative',
    負面提示詞: 'negative',
    采样器: 'sampler',
    採樣器: 'sampler',
  };
  const fields: Record<string, string> = {};
  let currentKey = '';
  for (const rawLine of String(text || '').split(/\r?\n/)) {
    const line = rawLine.trimEnd();
    const match = line.match(/^([^:：]{1,16})\s*[:：]\s*(.*)$/);
    const key = match ? fieldNames[match[1].trim()] : '';
    if (key) {
      currentKey = key;
      fields[currentKey] = appendFieldValue(fields[currentKey], match![2]);
      continue;
    }
    if (currentKey && line.trim()) {
      fields[currentKey] = appendFieldValue(fields[currentKey], line);
    }
  }
  return fields;
}

function appendFieldValue(current: string, value: string) {
  const text = String(value || '').trim();
  if (!current) {
    return text;
  }
  if (!text) {
    return current;
  }
  return `${current}\n${text}`;
}

function streamOpenAiImageJob(
  req: GatewayRequest,
  res: GatewayResponse,
  job: JobMetadata,
  model: string,
  deadline = Date.now() + openAiChatTimeoutMs,
) {
  return jobDelivery.streamOpenAiImageJob(res, job, model, deadline, (snapshot) =>
    openAiImageMarkdown(req, snapshot),
  );
}

function openAiImageMarkdown(
  req: GatewayRequest,
  imageOrJob: { id?: string; imageId?: string; imageUrl?: string },
) {
  const imageId = imageOrJob.imageId || imageOrJob.id;
  return `![image](${absoluteUrl(req, publicImageUrl(imageId!))})`;
}

function openAiChatCompletionResponse({
  model,
  content,
  id,
}: {
  model: string;
  content: string;
  id: string;
}) {
  return {
    id,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [
      {
        index: 0,
        message: { role: 'assistant', content },
        finish_reason: 'stop',
      },
    ],
    usage: {
      prompt_tokens: 1,
      completion_tokens: 1,
      total_tokens: 2,
    },
  };
}

function sendOpenAiStreamHeaders(res: GatewayResponse) {
  res.writeHead(200, {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-cache, no-transform',
    connection: 'keep-alive',
    ...corsHeaders(),
  });
}

function absoluteUrl(req: GatewayRequest, urlPath: string) {
  const proto =
    req.headers['x-forwarded-proto'] ||
    ((req.socket as typeof req.socket & { encrypted?: boolean }).encrypted ? 'https' : 'http');
  const hostHeader = req.headers['x-forwarded-host'] || req.headers.host || `localhost:${port}`;
  return `${proto}://${hostHeader}${urlPath}`;
}

async function addAccount(body: Body) {
  const token = String(body.token || '').trim();
  if (!token) {
    throw httpError(400, 'NovelAI account token is required.');
  }
  return store.update(
    (db) => {
      const account = {
        id: createId('acct'),
        routeId: nextAccountRouteId(db.accounts),
        name: String(body.name || `NovelAI ${db.accounts.length + 1}`).slice(0, 80),
        token,
        proxyUrl: normalizeAccountProxyUrl(body.proxyUrl || body.socksProxy || body.proxy || ''),
        enabled: body.enabled !== false,
        weight: clamp(Number(body.weight || 1), 1, 100),
        inFlight: 0,
        total: 0,
        failures: 0,
        quotaPoints: null,
        quotaFixed: null,
        quotaPurchased: null,
        quotaTier: null,
        v5UsagePercent: null,
        v5UsageIsNegative: false,
        v5UsageTimeUntilNextPercent: null,
        quotaCheckedAt: '',
        quotaError: '',
        cooldownUntil: '',
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        lastUsedAt: '',
      };
      db.accounts.unshift(account);
      recordDomainEvent(db, {
        eventType: 'AccountRegistered',
        streamId: `account:${account.id}`,
        accountId: account.id,
        occurredAt: account.createdAt,
        payload: {
          accountId: account.id,
          name: account.name,
          routeId: account.routeId,
          enabled: account.enabled,
          weight: account.weight,
          v5UsageIsNegative: account.v5UsageIsNegative,
          createdAt: account.createdAt,
        },
      });
      return account;
    },
    { collections: ['accounts'], domainCommand: { actorId: 'local-admin', actorKind: 'admin' } },
  );
}

async function updateAccounts(body: Body) {
  return store.update(
    (db) => {
      const ids = new Set(collectValues(body.ids || body.accounts));
      if (!ids.size) {
        throw httpError(400, 'account ids are required.');
      }
      const accounts = db.accounts.filter((account) => ids.has(account.id));
      if (!accounts.length) {
        throw httpError(404, 'no matching accounts found.');
      }
      const now = new Date().toISOString();
      let note = `Updated ${accounts.length} account(s)`;
      if (body.enabled !== undefined) {
        const action = body.enabled ? 'Enabled' : 'Disabled';
        note = `${action} ${accounts.length} account(s)`;
      }
      const audit = {
        id: createId('log'),
        type: 'update-accounts',
        amount: accounts.length,
        at: now,
        note,
      };
      db.ledger.unshift(audit);
      accounts.forEach((account, index) =>
        applyAccountConfiguration(
          db,
          account,
          { enabled: body.enabled, weight: body.weight },
          now,
          index === 0 ? [audit] : undefined,
        ),
      );
      return accounts;
    },
    {
      collections: ['accounts', 'ledger'],
      domainCommand: { actorId: 'local-admin', actorKind: 'admin' },
    },
  );
}

function refreshAccountQuotas(
  body: Body,
  command?: Parameters<typeof accountQuotaRefresh.refresh>[1],
) {
  return accountQuotaRefresh.refresh(collectValues(body.ids || body.accounts), command);
}

function startAccountQuotaAutoRefresh() {
  accountQuotaRefresh.start();
}

function startGenerationReviews() {
  const run = async () => {
    if (runtimeClosing) {
      return;
    }
    generationReviewPromise = reconcileGenerationReviews();
    try {
      await generationReviewPromise;
    } catch (error) {
      console.error('[fair-quota] 本地消费核对失败，下次继续:', (error as GatewayError).message);
    } finally {
      generationReviewPromise = null;
      if (!runtimeClosing) {
        generationReviewTimer = setTimeout(run, 10000);
        generationReviewTimer.unref?.();
      }
    }
  };
  void run();
}

function startColdImages() {
  if (!r2Storage) {
    return;
  }
  const idle = () =>
    !runtimeClosing &&
    !queueDispatcher.draining &&
    runningJobControls.size === 0 &&
    !store.hasActiveJobs();
  const run = async () => {
    if (runtimeClosing) {
      return;
    }
    let moved = false;
    try {
      if (idle()) {
        coldImagePromise = archiveColdImage(store, r2Storage, idle);
        moved = (await coldImagePromise) === true;
      }
    } catch {
      console.error(
        '[storage] R2 cold image archival failed; existing copies retained, retry deferred.',
      );
    } finally {
      coldImagePromise = null;
      if (!runtimeClosing) {
        coldImageTimer = setTimeout(run, moved ? 1000 : 60000);
        coldImageTimer.unref?.();
      }
    }
  };
  coldImageTimer = setTimeout(run, 60000);
  coldImageTimer.unref?.();
}

function reconcileGenerationReviews(now = Date.now()) {
  return reconcileGenerationReviewsCommand(store, { now: () => now, readStoredImage });
}

function accountTierText(tier: unknown, fallback = '会员未查询') {
  if (tier === null || tier === undefined || tier === '') {
    return fallback;
  }
  const value = Number(tier);
  const tierNames: Record<number, string> = {
    0: '无订阅',
    1: 'Tablet 会员',
    2: 'Scroll 会员',
    3: 'Opus 会员',
  };
  if (Number.isFinite(value)) {
    return tierNames[value] || `Tier ${value}`;
  }
  const key = String(tier).trim().toLowerCase();
  return (
    {
      none: '无订阅',
      paper: '无订阅',
      tablet: 'Tablet 会员',
      scroll: 'Scroll 会员',
      opus: 'Opus 会员',
    }[key] || String(tier)
  );
}

async function cleanupInterruptedStartupJobs() {
  const startedAt = Date.now(),
    result = await failureHandlers.cleanupInterruptedStartupJobs();
  console.log(
    `[runtime] startup interrupted job cleanup completed in ${Date.now() - startedAt}ms: changed=${result.changed} running=${result.running} direct=${result.direct} openai=${result.openai} expired=${result.expired} queuedRemaining=${result.queuedRemaining} runningRemaining=${result.runningRemaining}`,
  );
  return result;
}

async function logStartupQueueState() {
  const counts = await store.readQueueStateCounts();
  console.log(
    `[runtime] startup queue resume state: queued=${counts.queued} running=${counts.running} directQueued=${counts.directQueued} openaiQueued=${counts.openAiQueued}`,
  );
}

async function cleanupImageStorage() {
  // 私有部署保护：禁止自动清理孤儿图片。
  if (process.env.NO_AUTO_DELETE === 'true') {
    return;
  }
  const startedAt = Date.now();
  const settings = await store.readSettings();
  const trimmedImages =
    (await store.trimImageCache(normalizeCacheImageLimit(settings.maxCacheImages), {
      force: true,
    })) || [];
  await removeStoredImages(trimmedImages);

  const imageFiles = await store.readImageFiles();
  if (!imageFiles) {
    if (trimmedImages.length) {
      console.log(
        `[runtime] image cache cleanup removed ${trimmedImages.length} expired records and skipped orphan scan because image cache is partially loaded in ${Date.now() - startedAt}ms`,
      );
    } else {
      console.log(
        `[runtime] image cache cleanup skipped orphan scan because image cache is partially loaded in ${Date.now() - startedAt}ms`,
      );
    }
    return;
  }
  const referencedFiles = new Set(imageFiles.map((file) => path.resolve(dataDir, file)));

  let entries: import('node:fs').Dirent[] = [];
  try {
    entries = await readdir(imageDir, { withFileTypes: true });
  } catch (error) {
    if ((error as GatewayError)?.code === 'ENOENT') {
      return;
    }
    throw error;
  }

  const orphanFiles = entries
    .filter((entry) => entry.isFile() && entry.name.startsWith('img_'))
    .map((entry) => path.join(imageDir, entry.name))
    .filter((file) => !referencedFiles.has(path.resolve(file)));

  await Promise.all(
    orphanFiles.map(async (file) => {
      try {
        await rm(file, { force: true });
      } catch (error) {
        console.error(`Failed to delete orphan cached image ${path.basename(file)}:`, error);
      }
    }),
  );

  if (trimmedImages.length || orphanFiles.length) {
    console.log(
      `[runtime] image cache cleanup removed ${trimmedImages.length} expired records and ${orphanFiles.length} orphan files in ${Date.now() - startedAt}ms`,
    );
  } else {
    runtimeSlowLog('image cache cleanup', startedAt, 'removed=0');
  }
}

function imageCacheTrimBuffer(maxCacheImages = 1) {
  return maxCacheImages > 0 ? 300 : 0;
}

function normalizeCacheImageLimit(value: unknown) {
  const number = Number(value ?? 500);
  if (!Number.isFinite(number)) {
    return 500;
  }
  return Math.max(0, Math.min(MAX_CACHE_IMAGES_LIMIT, Math.floor(number)));
}

async function updateAccount(
  id: string,
  body: Body,
  actorId = 'local-admin',
  command?: Pick<PersistenceOptions<StoreItem>, 'domainCommand' | 'authorize' | 'commandReceipt'>,
) {
  return store.update(
    (db) => {
      const account = db.accounts.find((item) => item.id === id);
      if (!account) {
        throw httpError(404, 'account not found.');
      }
      applyAccountConfiguration(db, account, body, new Date().toISOString());
      return account;
    },
    {
      collections: ['accounts'],
      ...command,
      domainCommand: command?.domainCommand || { actorId, actorKind: 'admin' },
    },
  );
}

function applyAccountConfiguration(
  db: StoreDatabase,
  account: StoreItem,
  body: Body,
  now: string,
  evidence?: Record<string, unknown>[],
) {
  // 更换密钥前先检查在途预扣，避免请求结算到另一账户。
  const changingToken = body.token !== undefined && String(body.token).trim() !== account.token;
  if (
    changingToken &&
    (Number(account.inFlight || 0) > 0 ||
      Object.values(db.settings.fairQuota?.reservations || {}).some(
        (r) => r.accountId === account.id && r.status === 'reserved',
      ))
  ) {
    throw httpError(409, '有在途生成任务，请等待完成后更新密钥');
  }
  if (changingToken && !String(body.token).trim().startsWith('pst-')) {
    throw httpError(400, '需要有效的 PST 格式密钥');
  }
  if (changingToken) {
    account.enabled = false;
    account.quotaCheckedAt = '';
    account.quotaError = '密钥已更新，请刷新额度后再启用';
  }
  if (body.name !== undefined) {
    account.name = String(body.name).slice(0, 80);
  }
  if (body.token !== undefined && body.token) {
    account.token = String(body.token).trim();
  }
  if (body.proxyUrl !== undefined || body.socksProxy !== undefined || body.proxy !== undefined) {
    account.proxyUrl = normalizeAccountProxyUrl(
      body.proxyUrl || body.socksProxy || body.proxy || '',
    );
  }
  if (body.enabled !== undefined && !changingToken) {
    account.enabled = Boolean(body.enabled);
  }
  if (body.weight !== undefined) {
    account.weight = clamp(Number(body.weight), 1, 100);
  }
  account.updatedAt = now;
  recordDomainEvent(db, {
    eventType: 'AvailabilityChanged',
    streamId: `account:${account.id}`,
    accountId: account.id,
    occurredAt: account.updatedAt,
    payload: {
      accountId: account.id,
      enabled: account.enabled,
      name: account.name,
      weight: account.weight,
      quotaCheckedAt: account.quotaCheckedAt,
      quotaError: account.quotaError,
      updatedAt: account.updatedAt,
      reason: changingToken ? 'credential_replaced' : 'administrator',
      ...(evidence ? { evidence } : {}),
    },
  });
}

function dirtyJobRows(jobId: string | undefined) {
  return jobId ? { jobs: [jobId] } : {};
}

async function createJob(
  token: string,
  body: Body | ExecutionRequest | null,
  options: JobOptions = {},
): Promise<JobMetadata> {
  const normalize = (settings: Partial<Settings>): ExecutionRequest => {
    if (options.native) {
      return structuredClone(body as ExecutionRequest);
    }
    const input = options.frontend
      ? { ...body, width: undefined, height: undefined, cost: undefined }
      : body;
    const request: NormalizedImageRequest & Pick<ExecutionRequest, 'allowPurchasedAnlas'> =
      normalizeNovelAiRequest(input!, settings, {
        maxSteps: options.frontend ? MAX_STEPS : DIRECT_URL_MAX_STEPS,
      });
    if (options.frontend) {
      request.steps = Math.floor(request.steps);
      request.cost = generationCost(request);
    }
    request.allowPurchasedAnlas = (body as Body).allowPurchasedAnlas === true;
    return request;
  };
  const intent =
    options.idempotency ||
    generationIntent(options.idempotencyKey, normalize, {
      randomSeed: body?.seed === undefined || body.seed === '',
      noCache: isNoCache(body?.nocache),
      claim: options.generationPayloadClaim,
    });
  return admitGeneration({
    identity: { token },
    normalize,
    intent,
    claim: options.generationPayloadClaim,
    admissionVersion: options.admissionVersion,
    source: options.source || 'web',
    deadlineAt: options.deadlineAt,
    noCache: isNoCache(body?.nocache),
    explicitSeed: body?.seed,
  });
}
async function ensureAccountRouteIds() {
  await store.update(
    (db) => {
      assignAccountRouteIds(db.accounts);
    },
    { collections: ['accounts'] },
  );
}

async function createDirectJob(
  token: string,
  request: ExecutionRequest | null,
  cacheKey: string,
  options: JobOptions = {},
): Promise<JobMetadata> {
  return admitGeneration({
    identity: { token },
    request,
    cacheKey,
    intent: options.idempotency,
    claim: options.generationPayloadClaim,
    source: options.source || 'direct',
    admissionVersion: options.admissionVersion,
    deadlineAt: options.deadlineAt,
    noCache: true,
    ...(options.status === 'done'
      ? { cached: { imageId: options.imageId || '', accountId: options.accountId } }
      : {}),
  });
}
async function getGenerationRequest(requestId: string, memberId: string) {
  return publicGenerationRequest(await readOwnedGenerationRequest(store, requestId, memberId), {
    imageUrl: publicImageUrl,
    errorMessage: (message) => publicErrorMessage(message, { generation: true }),
  });
}

function resetJobStreamProgress(jobId: string | undefined, request: RequestSummary = {}) {
  if (!jobId) {
    return;
  }
  jobPreviews.clear(jobId);
  const progress = {
    percent: 0,
    step: 0,
    total: Number(request?.steps || 0),
    updatedAt: new Date().toISOString(),
  };
  jobStreamProgress.set(jobId, progress);
  patchCachedJobStreamProgress(jobId, progress);
  jobStreamProgressPersistState.set(jobId, { percent: 0, at: 0 });
}

function updateJobStreamProgress(jobId: string | undefined, progress: Progress = {}) {
  if (!jobId) {
    return;
  }
  const previous = jobStreamProgress.get(jobId) || {};
  const percent = clamp(Number(progress.percent ?? previous.percent ?? 0), 0, 100);
  const next = {
    percent: Math.max(Number(previous.percent || 0), percent),
    step: Number(progress.step ?? previous.step ?? 0) || 0,
    total: Number(progress.total ?? previous.total ?? 0) || 0,
    accountId: progress.accountId || previous.accountId || '',
    accountRouteId: Number(progress.accountRouteId || previous.accountRouteId || 0),
    eventType: progress.eventType || previous.eventType || '',
    updatedAt: progress.updatedAt || new Date().toISOString(),
  };
  jobStreamProgress.set(jobId, next);
  const job = patchCachedJobStreamProgress(jobId, next);
  if (percent === 0) {
    jobPreviews.clear(jobId);
  }
  jobPreviews.update(job as Parameters<JobPreviews['update']>[0], progress);
  if (shouldPersistJobStreamProgress(jobId, next)) {
    persistJobStreamProgress(jobId, next).catch((error: unknown) => {
      console.error('[runtime] failed to persist job stream progress:', error);
    });
  }
}

function clearJobStreamProgress(jobId: string) {
  if (!jobId) {
    return;
  }
  jobPreviews.clear(jobId);
  jobStreamProgress.delete(jobId);
  jobStreamProgressPersistState.delete(jobId);
}

function patchCachedJobStreamProgress(jobId: string, progress: Progress = {}) {
  return store.updateGenerationProgress(jobId, publicProgressSnapshot(progress));
}

function shouldPersistJobStreamProgress(jobId: string, progress: Progress = {}) {
  const now = Date.now();
  const previous = jobStreamProgressPersistState.get(jobId) || { percent: 0, at: 0 };
  const percent = Number(progress.percent || 0);
  if (percent >= 100) {
    return true;
  }
  if (percent - Number(previous.percent || 0) >= 8) {
    return true;
  }
  return now - Number(previous.at || 0) >= 1500;
}

async function persistJobStreamProgress(jobId: string, progress: Progress = {}) {
  const now = Date.now();
  jobStreamProgressPersistState.set(jobId, { percent: Number(progress.percent || 0), at: now });
  await store.update(
    (db) => {
      const job = db.jobs.find((item) => item.id === jobId);
      if (!job || job.status !== 'running') {
        return null;
      }
      job.generationProgress = publicProgressSnapshot(progress, job);
      return job;
    },
    {
      collections: ['jobs'],
      dirtyRows: dirtyJobRows(jobId),
      shouldPersist: (job) => Boolean(job),
    },
  );
}

function publicProgressSnapshot(progress: Progress = {}, job: Partial<StoreItem> = {}) {
  return {
    percent: clamp(Number(progress.percent || 0), 0, 100),
    step: Number(progress.step || 0),
    total: Number(progress.total || job.request?.steps || 0),
    updatedAt: progress.updatedAt || new Date().toISOString(),
  };
}

async function reserveQueuedJob(jobId: string): Promise<QueueReservation> {
  const requestedAt = Date.now();
  const result = await reserveQueuedGeneration(store, jobId, {
    dispatchEnabled: () => queueDispatchEnabled,
    physicalAccountLoad,
  });
  if (result.job) {
    console.log(
      '[queue]',
      JSON.stringify({
        event: 'dispatch_commit',
        jobId,
        accountId: result.job.accountId,
        createdAt: result.job.createdAt,
        requestedAt,
        startedAt: result.job.startedAt,
        committedAt: Date.now(),
      }),
    );
  }
  return result;
}

async function runReservedJob(reservation: QueueReservation) {
  if (!reservation || reservation.skip || reservation.queued) {
    return;
  }
  const control = createRunningJobControl(reservation.job);
  // 清理超时任务可能先于执行 catch 取得事务；复用活执行证据，不把本地提交错误当网络故障。
  control.reservation = reservation;
  const useStreamProgress = shouldUseJobStreamProgress(reservation.job);
  if (useStreamProgress) {
    resetJobStreamProgress(reservation.job?.id, reservation.job?.request);
  }
  reservation.failurePhase = 'preflight';
  try {
    // 已取消的任务不再启动新的上游请求。
    const current = await store.findJobContext(reservation.job.id);
    if (current?.job?.status !== 'running' || control.controller.signal.aborted) {
      throw new Error('任务已取消');
    }
    const request = reservation.job.requestId
      ? singleGenerationRequest(
          store.readGenerationRequest(reservation.job.requestId)!,
          reservation.job.sampleIndex,
        )
      : (reservation.job.request as ExecutionRequest);
    if (!request) {
      throw new Error('持久生成请求不存在');
    }
    reservation.failurePhase = 'upstream';
    const image = await generateWithBoundAccount(reservation, request, {
      signal: control.controller.signal,
      forceStream: useStreamProgress,
      onProgress: useStreamProgress
        ? (progress) => updateJobStreamProgress(reservation.job?.id, progress)
        : null,
    });
    reservation.failurePhase = 'commit';
    // 已拿到验证完整的产物时，即使截止时间刚到，也应保存；退款是否发生由提交时状态决定。
    if (useStreamProgress) {
      updateJobStreamProgress(reservation.job?.id, {
        percent: 100,
        step: reservation.job?.request?.steps,
        total: reservation.job?.request?.steps,
      });
    }
    await completeGeneration(reservation, request, image, { jobId: reservation.job.id });
  } catch (error) {
    if (control.controller.signal.aborted || isAbortError(error)) {
      await cancelReservedJob(reservation, error);
      return;
    }
    await failGeneration(reservation, error);
  } finally {
    finishRunningJobControl(reservation.job?.id, control);
    if (!runtimeClosing) {
      void refreshEnabledAccountQuotas().catch(() =>
        console.error('[fair-quota] 余额刷新失败，将在定时刷新时重试'),
      );
    }
  }
}

function shouldUseJobStreamProgress(job: Partial<StoreItem> = {}) {
  return (
    (job.request?.operation || 'generate') === 'generate' &&
    ['web', 'direct', 'openai', 'novelai-native', 'member-generation'].includes(job.source || 'web')
  );
}

function runningJobTimeoutDelay(job: Partial<StoreItem> = {}) {
  const deadline = jobDeadlineTimestamp(job);
  const delays = [novelAiGenerateTimeoutMs()];
  if (deadline) {
    delays.push(Math.max(1, deadline - Date.now()));
  }
  return Math.max(1, Math.min(...delays));
}

function jobDeadlineTimestamp(job: Partial<StoreItem> = {}) {
  const deadline = Date.parse(job.deadlineAt || '');
  return Number.isFinite(deadline) && deadline > 0 ? deadline : 0;
}

function novelAiGenerateTimeoutMs() {
  const configured = Number(process.env.NOVELAI_GENERATE_TIMEOUT_MS || 0);
  if (Number.isFinite(configured) && configured > 0) {
    return Math.max(1000, Math.floor(configured));
  }
  return Math.max(1, accountInflightTimeoutMs() - 1000);
}

function accountInflightTimeoutMs() {
  const configured = Number(process.env.ACCOUNT_INFLIGHT_TIMEOUT_MS || 10 * 60 * 1000);
  return Number.isFinite(configured) && configured > 0
    ? Math.max(1000, Math.floor(configured))
    : 10 * 60 * 1000;
}

async function generateWithBoundAccount(
  reservation: Reservation,
  request: ExecutionRequest,
  options: BoundGenerationOptions = {},
) {
  if (options.signal?.aborted) {
    throw options.signal.reason || new Error('direct generate timeout');
  }
  const account = reservation.account;
  options.onProgress?.({
    percent: 0,
    step: 0,
    total: Number(request.steps || 0),
    accountId: account?.id || '',
    accountRouteId: account?.routeId || 0,
  });
  try {
    return await generateNovelAiImage(request, account, process.env, {
      signal: options.signal,
      forceStream: options.forceStream,
      onUpstreamTiming: (event) => {
        reservation.upstreamTiming ||= {};
        for (const name of [
          'upstreamStartedAt',
          'upstreamHeadersAt',
          'upstreamCompletedAt',
          'upstreamHttpStatus',
          'upstreamRetryAfterUntil',
        ] as const) {
          if (event[name] !== undefined) {
            (reservation.upstreamTiming[name] as (typeof event)[typeof name]) = event[name];
          }
        }
      },
      onProgress: (progress) =>
        options.onProgress?.({
          ...progress,
          accountId: account?.id || '',
          accountRouteId: account?.routeId || 0,
        }),
    });
  } catch (error) {
    logNovelAiGenerateError(error, request, account, reservation.job);
    throw error;
  }
}

async function recoverPendingGenerationResults() {
  const result = await recoverPendingGenerationResultsCommand({
    store,
    dataDir,
    publish: publishStoredResult,
    onRecovery: (outcome) => {
      let log = console.error;
      if (outcome.status === 'completed') {
        log = console.log;
      } else if (outcome.status === 'file_missing') {
        log = console.warn;
      }
      log('[recovery]', JSON.stringify(outcome));
    },
  });
  if (result.completed) {
    await removeStoredImages(
      await store.trimImageCache(null, { batchSize: imageCacheTrimBuffer() }),
    );
  }
  return result;
}

async function completeGeneration(
  reservation: Reservation,
  request: ExecutionRequest,
  image: GeneratedImage,
  meta: { jobId: string },
) {
  const requestedAt = Date.now();
  const saved = await completeGenerationCommand(
    { store, dataDir, publish: publishStoredResult },
    reservation,
    request,
    image,
    meta,
  );
  console.log(
    '[queue]',
    JSON.stringify({
      event: 'result_commit',
      jobId: meta.jobId,
      requestedAt,
      committedAt: Date.now(),
      upstreamCompletedAt: reservation.upstreamTiming?.upstreamCompletedAt,
    }),
  );
  clearJobStreamProgress(meta.jobId);
  await removeStoredImages(await store.trimImageCache(null, { batchSize: imageCacheTrimBuffer() }));
  scheduleQueueDrain();
  notifyJobWaiters(meta.jobId, { saved, image, seed: request.seed, balance: saved.balance });
  return saved;
}

function physicalAccountLoad(accountId: string) {
  return countPhysicalAccountLoad(runningJobControls, accountId);
}

function scheduleQueueDrain(delay = 0) {
  queueDispatcher.schedule(delay);
}

function resetStaleAccountLoads(accounts: StoreItem[]) {
  const staleAfterMs = accountInflightTimeoutMs();
  const now = Date.now();
  accounts.forEach((account) => {
    if (account.cooldownUntil && Date.parse(account.cooldownUntil) <= now) {
      account.cooldownUntil = '';
    }
    if (Number(account.inFlight || 0) <= 0) {
      return;
    }
    const lastUsed = Date.parse((account.lastUsedAt || 0) as string);
    if (!lastUsed || now - lastUsed > staleAfterMs) {
      account.inFlight = 0;
    }
  });
}

function assignAccountRouteIds(accounts: StoreItem[]) {
  const used = new Set<number>();
  let next = 1;
  accounts.forEach((account) => {
    const current = Number(account.routeId || 0);
    if (Number.isInteger(current) && current > 0 && !used.has(current)) {
      account.routeId = current;
      used.add(current);
      next = Math.max(next, current + 1);
      return;
    }
    while (used.has(next)) {
      next += 1;
    }
    account.routeId = next;
    used.add(next);
    next += 1;
  });
}

function nextAccountRouteId(accounts: StoreItem[]) {
  return accounts.reduce((max, account) => Math.max(max, Number(account.routeId || 0)), 0) + 1;
}

function normalizePublicBaseUrl(value: unknown = '') {
  const text = String(value || '')
    .trim()
    .replace(/\/+$/, '');
  if (!text) {
    return '';
  }
  try {
    const url = new URL(text);
    if (!['http:', 'https:'].includes(url.protocol)) {
      return '';
    }
    const pathname = url.pathname === '/' ? '' : url.pathname.replace(/\/+$/, '');
    return `${url.origin}${pathname}`;
  } catch {
    return '';
  }
}

function getUserOrThrow(db: Pick<StoreDatabase, 'users'>, token: string | undefined) {
  const user = db.users.find((item) => item.token === token);
  if (!user || user.enabled === false) {
    throw httpError(401, 'invalid token.');
  }
  return user;
}

function publicAccount(
  account: StoreItem,
  options: { revealToken?: boolean; stats1h?: ReturnType<typeof finalizeStats> } | number = {},
) {
  return {
    id: account.id,
    routeId: account.routeId || 0,
    name: account.name,
    token: (options as { revealToken?: boolean }).revealToken
      ? account.token
      : maskToken(account.token),
    proxyUrl: (options as { revealToken?: boolean }).revealToken
      ? account.proxyUrl || ''
      : maskProxyUrl(account.proxyUrl || ''),
    hasProxy: Boolean(account.proxyUrl),
    enabled: account.enabled !== false,
    weight: account.weight || 1,
    inFlight: account.inFlight || 0,
    total: account.total || 0,
    failures: account.failures || 0,
    quotaPoints: account.quotaPoints ?? null,
    quotaFixed: account.quotaFixed ?? null,
    quotaPurchased: account.quotaPurchased ?? null,
    quotaTier: account.quotaTier ?? null,
    quotaTierText: accountTierText(account.quotaTier),
    trialRemainingImages: account.trialRemainingImages ?? null,
    trialUsedImages: account.trialUsedImages ?? null,
    trialEligible: account.trialEligible === true,
    trialStatusKnown: account.trialStatusKnown === true,
    v5UsagePercent: account.v5UsagePercent ?? null,
    v5UsageIsNegative: Boolean(account.v5UsageIsNegative),
    v5UsageTimeUntilNextPercent: account.v5UsageTimeUntilNextPercent ?? null,
    quotaCheckedAt: account.quotaCheckedAt || '',
    quotaError: account.quotaError || '',
    cooldownUntil: account.cooldownUntil || '',
    stats1h: (options as { stats1h?: ReturnType<typeof finalizeStats> }).stats1h || {
      done: 0,
      failed: 0,
      total: 0,
      successRate: 0,
    },
    lastUsedAt: account.lastUsedAt || '',
  };
}

async function writeStoredImage(id: string, image: GeneratedImage) {
  const imageFile = imageStorageName(id, image.mimeType);
  await writeFile(path.join(dataDir, imageFile), image.buffer);
  return imageFile;
}

async function readStoredImage(image: StoredResult) {
  return imageStorage.read(image);
}

async function removeStoredImages(images: StoredResult[]) {
  // 私有部署保护：保留图片文件，删除前须获得用户同意。
  if (process.env.NO_AUTO_DELETE === 'true') {
    return;
  }
  for (let index = 0; index < images.length; index += 50) {
    const batch = images.slice(index, index + 50);
    await Promise.all(
      batch.map(async (image) => {
        if (!image.file) {
          return;
        }
        try {
          await rm(imageFilePath(image.file), { force: true });
        } catch (error) {
          console.error(`Failed to delete cached image ${image.id}:`, error);
        }
      }),
    );
  }
}

function imageStorageName(id: string, mimeType = '') {
  return path.posix.join('images', `${id}.${imageExtension(mimeType)}`);
}

function imageFilePath(file: string) {
  return storedImagePath(dataDir, file);
}

function publicGenerationProgress(
  job: Partial<Pick<StoreItem, 'id' | 'status' | 'request' | 'generationProgress'>> = {},
) {
  if (job.status === 'done') {
    return {
      percent: 100,
      step: Number(job.request?.steps || 0),
      total: Number(job.request?.steps || 0),
      active: false,
    };
  }
  if (job.status !== 'running') {
    return {
      percent: 0,
      step: 0,
      total: Number(job.request?.steps || 0),
      active: false,
    };
  }
  const progress = jobStreamProgress.get(job.id!) || job.generationProgress || {};
  return {
    percent: clamp(Number(progress.percent || 0), 0, 100),
    step: Number(progress.step || 0),
    total: Number(progress.total || job.request?.steps || 0),
    active: true,
    updatedAt: progress.updatedAt || '',
  };
}

function hourlyUsageStatsByDay(jobs: StoreItem[], days = usageChartDays) {
  const keys = recentBeijingDateKeys(days);
  const buckets = new Map(
    keys.map((key) => [
      key,
      {
        date: key,
        label: key.slice(5),
        done: 0,
        failed: 0,
        total: 0,
        credits: 0,
        successRate: 0,
        hours: Array.from({ length: 24 }, (_, hour) => ({
          hour,
          label: `${String(hour).padStart(2, '0')}:00`,
          done: 0,
          failed: 0,
          total: 0,
          credits: 0,
          successRate: 0,
        })),
      },
    ]),
  );

  jobs.forEach((job) => {
    if (!['done', 'failed'].includes(job.status!)) {
      return;
    }
    const timestamp = Date.parse(job.updatedAt || job.createdAt || '');
    if (!timestamp) {
      return;
    }
    const key = beijingDateKey(timestamp);
    const bucket = buckets.get(key)!;
    if (!bucket) {
      return;
    }
    const hourBucket = bucket.hours[beijingHour(timestamp)];
    if (!hourBucket) {
      return;
    }
    if (job.status === 'done') {
      const credits = Math.max(0, Number(job.cost || 0));
      const generated = credits >= 1 ? 1 : 0;
      bucket.done += generated;
      hourBucket.done += generated;
      bucket.credits += credits;
      hourBucket.credits += credits;
    }
    if (job.status === 'failed') {
      bucket.failed += 1;
    }
    if (job.status === 'failed') {
      hourBucket.failed += 1;
    }
  });

  return keys.map((key) => {
    const bucket = buckets.get(key)!;
    bucket.total = bucket.done + bucket.failed;
    bucket.successRate = bucket.total ? bucket.done / bucket.total : 0;
    bucket.hours.forEach((hour) => {
      hour.total = hour.done + hour.failed;
      hour.successRate = hour.total ? hour.done / hour.total : 0;
    });
    return bucket;
  });
}

function errorLogs(jobs: StoreItem[], db: Partial<StoreDatabase> = {}, limit = 100) {
  const cutoff = Date.now() - errorLogRetentionMs;
  return jobs
    .filter((job) => job.status === 'failed')
    .filter(isAccountErrorLogJob)
    .filter((job) => {
      const timestamp = Date.parse(job.updatedAt || job.createdAt || '');
      return timestamp && timestamp >= cutoff;
    })
    .sort(
      (a, b) =>
        Date.parse(b.updatedAt || b.createdAt || '') - Date.parse(a.updatedAt || a.createdAt || ''),
    )
    .slice(0, limit)
    .map((job) => publicErrorLog(job, db));
}

function isAccountErrorLogJob(job: StoreItem) {
  if (!job?.accountId) {
    return false;
  }
  const text = `${job.error || ''}\n${job.errorDetail || ''}`;
  if (isNovelAiCapacityError({ message: text })) {
    return false;
  }
  if (
    /all NovelAI accounts are busy|server busy|direct generate timeout|AbortError|operation was aborted/i.test(
      text,
    )
  ) {
    return false;
  }
  if (/job deadline expired|stale timeout|queued job exceeded|running job exceeded/i.test(text)) {
    return false;
  }
  if (/invalid user token|invalid STA1N|密钥额度不足|用户额度不足/i.test(text)) {
    return false;
  }
  return true;
}

function publicErrorLog(job: StoreItem, db: Partial<StoreDatabase> = {}) {
  const request = generationRequestForErrorLog(store, job);
  const account =
    job.accountId && Array.isArray(db.accounts)
      ? db.accounts!.find((item) => item.id === job.accountId)
      : null;
  return {
    id: job.id,
    source: job.source || 'web',
    userToken: maskToken(job.userToken || ''),
    accountId: job.accountId || '',
    accountRouteId: account?.routeId || 0,
    status: job.status,
    error: publicErrorMessage(job.error || ''),
    errorDetail: errorDetailMessage(job.errorDetail || job.error || ''),
    retryCount: Number(job.retryCount || 0),
    cost: Number(job.cost || 0),
    accountCost: jobAccountCost(job),
    queueTotal: Number(job.queueTotal || 0),
    durationMs: jobDurationMs(job),
    request: errorLogRequest(request),
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    completedAt: job.completedAt || (['done', 'failed'].includes(job.status!) ? job.updatedAt : ''),
    beijingDate: beijingDateKey(Date.parse(job.updatedAt || job.createdAt || '')),
  };
}

function errorLogRequest(request: RequestSummary = {}) {
  return {
    tag: request.tag || '',
    prompt: request.prompt || '',
    artist: request.artist || '',
    negative: request.negative || '',
    model: request.model || '',
    size: request.size || '',
    width: request.width || 0,
    height: request.height || 0,
    requestedSteps: request.requestedSteps ?? request.steps ?? 0,
    routedSteps: request.steps ?? 0,
    scale: request.scale ?? '',
    cfg: request.cfg ?? '',
    sampler: request.sampler || '',
    noiseSchedule: request.noiseSchedule || '',
    seed: request.seed ?? '',
  };
}

function recentBeijingDateKeys(days: number) {
  const now = new Date(Date.now() + beijingOffsetMs);
  const midnight = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  return Array.from({ length: days }, (_, index) => {
    const dayOffset = index - days + 1;
    return new Date(midnight + dayOffset * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  });
}

function beijingDateKey(timestamp: number) {
  const value = Number(timestamp);
  if (!Number.isFinite(value)) {
    return '';
  }
  return new Date(value + beijingOffsetMs).toISOString().slice(0, 10);
}

function beijingHour(timestamp: number) {
  const value = Number(timestamp);
  if (!Number.isFinite(value)) {
    return 0;
  }
  return new Date(value + beijingOffsetMs).getUTCHours();
}

function jobStatsSince(jobs: StoreItem[], rangeMs: number) {
  const since = Date.now() - rangeMs;
  return finalizeStats(
    jobs.reduce(
      (stats, job) => {
        if (isQuotaFailureJob(job)) {
          return stats;
        }
        const createdAt = Date.parse(job.createdAt || '');
        if (!createdAt || createdAt < since) {
          return stats;
        }
        if (job.status === 'done') {
          stats.done += 1;
        }
        if (job.status === 'failed') {
          stats.failed += 1;
        }
        return stats;
      },
      { done: 0, failed: 0 },
    ),
  );
}

function requestStatsSince(jobs: StoreItem[], rangeMs: number) {
  const since = Date.now() - rangeMs;
  const total = jobs.reduce((sum, job) => {
    const createdAt = Date.parse(job.createdAt || '');
    return createdAt && createdAt >= since ? sum + 1 : sum;
  }, 0);
  return { total };
}

function accountStatsMapSince(jobs: StoreItem[], rangeMs: number) {
  const since = Date.now() - rangeMs;
  const map = new Map<string, { done: number; failed: number }>();
  jobs.forEach((job) => {
    if (isQuotaFailureJob(job) || !job.accountId) {
      return;
    }
    const createdAt = Date.parse(job.createdAt || '');
    if (!createdAt || createdAt < since) {
      return;
    }
    let stats = map.get(job.accountId);
    if (!stats) {
      stats = { done: 0, failed: 0 };
      map.set(job.accountId, stats);
    }
    if (job.status === 'done') {
      stats.done += 1;
    }
    if (job.status === 'failed') {
      stats.failed += 1;
    }
  });
  map.forEach((stats, accountId) => {
    map.set(accountId, finalizeStats(stats));
  });
  return map;
}

function finalizeStats(stats: Stats) {
  const done = Number(stats.done || 0);
  const failed = Number(stats.failed || 0);
  const total = done + failed;
  return {
    done,
    failed,
    total,
    successRate: total ? done / total : 0,
  };
}

function generationCost(request: RequestSummary | null = null) {
  return generationDisplayCost();
}

// Retain historical metadata only. New jobs do not estimate or debit upstream points.
function jobAccountCost(job: Partial<Pick<StoreItem, 'request' | 'accountCost' | 'cost'>> = {}) {
  return normalizeAccountCost(job.accountCost);
}

function normalizeAccountCost(value: unknown) {
  const cost = Number(value);
  return Number.isFinite(cost) && cost > 0 ? Math.ceil(cost) : 0;
}

function isQuotaFailureJob(job: StoreItem) {
  return job?.status === 'failed' && isInsufficientBalanceError({ message: job.error });
}

function normalizeAccountProxyUrl(value: unknown = '') {
  const text = String(value || '').trim();
  if (!text) {
    return '';
  }
  const rawProxy = parseHostPortUserPassProxy(text);
  if (rawProxy) {
    return rawProxy;
  }
  const withScheme = /^[a-z][a-z0-9+.-]*:\/\//i.test(text) ? text : `socks5://${text}`;
  let url: URL;
  try {
    url = new URL(withScheme.replace(/^sock5:\/\//i, 'socks5://'));
  } catch {
    throw httpError(400, 'invalid SOCKS5 proxy URL.');
  }
  if (!['socks5:', 'socks5h:'].includes(url.protocol)) {
    throw httpError(400, 'only socks5:// and socks5h:// proxies are supported.');
  }
  if (!url.hostname) {
    throw httpError(400, 'SOCKS5 proxy host is required.');
  }
  if (!url.port) {
    url.port = '1080';
  }
  return url.toString();
}

function parseHostPortUserPassProxy(value: unknown = '') {
  const parts = String(value || '')
    .trim()
    .split(':');
  if (parts.length !== 4) {
    return '';
  }
  const [host, port, username, password] = parts.map((part) => part.trim());
  if (!host || !port || !username || !password || !/^\d{1,5}$/.test(port)) {
    return '';
  }
  const url = new URL(`socks5://${host}:${port}`);
  url.username = username;
  url.password = password;
  return url.toString();
}

function maskProxyUrl(value: unknown = '') {
  const text = String(value || '').trim();
  if (!text) {
    return '';
  }
  try {
    const url = new URL(text);
    if (url.password) {
      url.password = '******';
    }
    if (url.username) {
      url.username = `${url.username.slice(0, 2)}***`;
    }
    return url.toString();
  } catch {
    return text.replace(/:\/\/([^:@]+):([^@]+)@/, '://$1:******@');
  }
}

function adminRuntimeSettings(settings: Partial<Settings> = {}) {
  return {
    ...settings,
    promptApi: adminPromptApiConfig(settings.promptApi),
  };
}

function collectValues(value: unknown) {
  if (Array.isArray(value)) {
    return value.map((item: unknown) => String(item).trim()).filter(Boolean);
  }
  if (value === undefined || value === null || value === '') {
    return [];
  }
  return [String(value).trim()].filter(Boolean);
}

async function readJson(req: GatewayRequest, maxBytes = 2 * 1024 * 1024): Promise<Body> {
  // 公网守卫读取过的正文直接复用，避免重复消费请求流。
  if (req.publicParsedBody !== undefined) {
    return req.publicParsedBody;
  }
  const chunks: Buffer[] = [];
  let bytes = 0;
  if (Number(req.headers?.['content-length']) > maxBytes) {
    throw httpError(413, '请求体超过大小限制');
  }
  for await (const chunk of req) {
    bytes += chunk.length;
    if (bytes > maxBytes) {
      throw httpError(413, '请求体超过大小限制');
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
    throw httpError(400, 'invalid JSON body.');
  }
}

function sendJson(res: GatewayResponse, statusCode: number, payload: unknown) {
  const body = JSON.stringify(payload);
  res.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': Buffer.byteLength(body),
    ...corsHeaders(),
  });
  res.end(body);
}

function sendOpenAiError(
  res: GatewayResponse,
  statusCode: number,
  message: string,
  type = 'invalid_request_error',
) {
  sendJson(res, statusCode, {
    error: {
      message: publicErrorMessage(message),
      type,
      param: null,
      code: type,
    },
  });
}

function sendImage(
  res: GatewayResponse,
  statusCode: number,
  mimeType: string,
  buffer: Buffer,
  extraHeaders: OutgoingHttpHeaders = {},
) {
  res.writeHead(statusCode, {
    'content-type': mimeType,
    'cache-control': 'public, max-age=31536000, immutable',
    'content-length': buffer.length,
    ...corsHeaders(),
    ...extraHeaders,
  });
  res.end(buffer);
}

async function sendStoredImage(
  res: GatewayResponse,
  statusCode: number,
  image: StoredResult | null | undefined,
  extraHeaders: OutgoingHttpHeaders = {},
) {
  if (!image) {
    throw httpError(404, 'image content not found.');
  }
  const source = await imageStorage.open(image);
  if (!('handle' in source)) {
    sendImage(
      res,
      statusCode,
      image?.mimeType || 'image/png',
      await readStoredImage(image!),
      extraHeaders,
    );
    return;
  }

  try {
    const fileStat = await source.handle.stat();
    res.writeHead(statusCode, {
      'content-type': image.mimeType || 'image/png',
      'cache-control': 'public, max-age=31536000, immutable',
      'content-length': fileStat.size,
      ...corsHeaders(),
      ...extraHeaders,
    });

    await new Promise<void>((resolve, reject) => {
      const stream = source.handle.createReadStream();
      const cleanup = () => {
        stream.off('error', onStreamError);
        res.off('error', onResponseError);
        res.off('finish', onFinish);
        res.off('close', onClose);
      };
      const finish = () => {
        cleanup();
        resolve();
      };
      const onStreamError = (error: Error) => {
        cleanup();
        if (!res.destroyed) {
          res.destroy(error);
        }
        resolve();
      };
      const onResponseError = (error: Error) => {
        cleanup();
        stream.destroy();
        reject(error);
      };
      const onFinish = () => finish();
      const onClose = () => {
        cleanup();
        stream.destroy();
        resolve();
      };
      stream.once('error', onStreamError);
      res.once('error', onResponseError);
      res.once('finish', onFinish);
      res.once('close', onClose);
      stream.pipe(res);
    });
  } finally {
    await source.handle.close();
  }
}

function sendBusyImage(res: GatewayResponse, cooldownUntil?: string) {
  const image = generationErrorImage('busy', cooldownUntil);
  sendImage(res, image.statusCode, image.mimeType, image.buffer, image.headers);
}

function sendTimeoutImage(res: GatewayResponse, cooldownUntil?: string) {
  const image = generationErrorImage('timeout', cooldownUntil);
  sendImage(res, image.statusCode, image.mimeType, image.buffer, image.headers);
}

function sendCorsPreflight(res: GatewayResponse) {
  res.writeHead(204, {
    ...corsHeaders(),
    'access-control-max-age': '86400',
  });
  res.end();
}

function sendHead(res: GatewayResponse, statusCode: number, headers: OutgoingHttpHeaders = {}) {
  res.writeHead(statusCode, {
    'cache-control': 'no-store',
    ...corsHeaders(),
    ...headers,
  });
  res.end();
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

function tokenFrom(req: GatewayRequest, url: URL) {
  const header = req.headers.authorization || '';
  if (header.startsWith('Bearer ')) {
    return header.slice(7).trim();
  }
  return String(url.searchParams.get('token') || req.headers['x-user-token'] || '').trim();
}

function bearerToken(req: GatewayRequest) {
  const header = String(req.headers.authorization || '');
  const match = header.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : '';
}

function openAiErrorType(error: unknown) {
  const statusCode = Number((error as GatewayError)?.statusCode || 500);
  if (statusCode === 401 || statusCode === 403) {
    return 'authentication_error';
  }
  if (statusCode === 429) {
    return 'rate_limit_error';
  }
  if (statusCode >= 500) {
    return 'server_error';
  }
  return 'invalid_request_error';
}

function isAdmin(req: GatewayRequest, url: URL) {
  const header = String(req.headers['x-admin-token'] || '');
  const query =
    process.env.PUBLIC_MODE === 'true' ? '' : String(url.searchParams.get('adminToken') || '');
  return Boolean(adminToken) && (header === adminToken || query === adminToken);
}

function assertAdmin(req: GatewayRequest, url: URL) {
  if (isAdmin(req, url)) {
    return;
  }
  const suppliedToken = String(
    req.headers['x-admin-token'] || url.searchParams.get('adminToken') || '',
  ).trim();
  throw httpError(
    suppliedToken ? 401 : 403,
    suppliedToken ? 'invalid token.' : 'admin token required.',
  );
}

function clamp(value: number, min: number, max: number) {
  if (!Number.isFinite(value)) {
    return min;
  }
  return Math.max(min, Math.min(max, value));
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function runtimeSlowLog(label: string, startedAt: number, detail = '', thresholdMs = 500) {
  const duration = Date.now() - startedAt;
  if (duration < thresholdMs) {
    return;
  }
  console.log(`[runtime] slow ${label}: ${duration}ms${detail ? ` (${detail})` : ''}`);
}

function runtimeRequestLog(label: string, startedAt: number, detail: Body = {}) {
  const duration = Date.now() - startedAt;
  const parts = Object.entries(detail)
    .map(([key, value]) => `${key}=${value}`)
    .join(' ');
  console.log(`[runtime] ${label}: total=${duration}ms${parts ? ` ${parts}` : ''}`);
}

function logNovelAiGenerateError(
  error: unknown,
  request: RequestSummary = {},
  account: Partial<StoreItem> | null = {},
  job: Partial<StoreItem> = {},
) {
  const message = String((error as GatewayError)?.message || error || '');
  if (!/NovelAI returned 5\d\d/i.test(message)) {
    return;
  }
  const cid = message.match(/cid=([a-z0-9]+)/i)?.[1] || '-';
  console.error(
    [
      `[runtime] NovelAI generate error status=5xx`,
      `cid=${cid}`,
      `job=${shortRuntimeId(job?.id)}`,
      `route=${account?.routeId || '-'}`,
      `account=${shortRuntimeId(account?.id)}`,
      `proxy=${account?.proxyUrl ? 1 : 0}`,
      `model=${request.model || '-'}`,
      `size=${request.width || '-'}x${request.height || '-'}`,
      `steps=${request.steps || '-'}`,
      `sampler=${request.sampler || '-'}`,
      `noise=${request.noiseSchedule || '-'}`,
    ].join(' '),
  );
}

function shortRuntimeId(value: unknown = '') {
  const text = String(value || '');
  if (!text) {
    return '-';
  }
  if (text.length <= 12) {
    return text;
  }
  return `${text.slice(0, 8)}...${text.slice(-4)}`;
}

function installRuntimeSafetyHandlers() {
  if (
    (installRuntimeSafetyHandlers as typeof installRuntimeSafetyHandlers & { installed?: boolean })
      .installed
  ) {
    return;
  }
  (
    installRuntimeSafetyHandlers as typeof installRuntimeSafetyHandlers & { installed?: boolean }
  ).installed = true;
  process.on('uncaughtException', (error) => {
    if (isRecoverableRuntimeAbort(error)) {
      console.error(
        `[runtime] recovered from async abort/socket error: ${(error as GatewayError)?.message || error}`,
      );
      return;
    }
    console.error('[runtime] uncaught exception:', error);
    process.exit(1);
  });
  process.on('unhandledRejection', (reason) => {
    const error = reason instanceof Error ? reason : new Error(String(reason));
    if (isRecoverableRuntimeAbort(error)) {
      console.error(
        `[runtime] recovered from async abort/socket rejection: ${(error as GatewayError).message}`,
      );
      return;
    }
    console.error('[runtime] unhandled rejection:', reason);
    process.exit(1);
  });
}

function installShutdownHandlers(serverInstance: Server) {
  if (
    (installShutdownHandlers as typeof installShutdownHandlers & { installed?: boolean }).installed
  ) {
    return;
  }
  (installShutdownHandlers as typeof installShutdownHandlers & { installed?: boolean }).installed =
    true;
  const shutdown = async (signal: NodeJS.Signals) => {
    if (runtimeClosing) {
      return;
    }
    runtimeClosing = true;
    queueDispatchEnabled = false;
    // 长期订阅须先结束；否则 server.close 等连接，而订阅等 store.close，形成循环。
    stateEventsFor(store).closeSubscriptions();
    jobPreviews.closeSubscriptions();
    closeMemberGenerationStreams(store);
    const configured = Number(process.env.SHUTDOWN_GRACE_MS || 30000);
    const graceMs =
      Number.isFinite(configured) && configured > 0
        ? Math.min(600000, Math.floor(configured))
        : 30000;
    console.log(`[runtime] ${signal} received. Waiting up to ${graceMs}ms for active generation.`);
    queueDispatcher.stop();
    accountQuotaRefresh.stop();
    if (generationReviewTimer) {
      clearTimeout(generationReviewTimer);
    }
    if (coldImageTimer) {
      clearTimeout(coldImageTimer);
    }
    const connectionsClosed = new Promise((resolve) => serverInstance.close(resolve));
    // 宽限到期保留持久任务/产物证据；强制退出后的恢复不依赖信号回调。
    const forceTimer = setTimeout(() => process.exit(0), graceMs);
    try {
      // 已开始的派发事务先结束，随后控制对象的 done 才覆盖全部在途执行。
      while (queueDispatcher.draining) {
        await sleep(10);
      }
      await Promise.all([...runningJobControls.values()].map((control) => control.done));
      await Promise.allSettled(
        [accountQuotaRefresh.pending, generationReviewPromise, coldImagePromise].filter(Boolean),
      );
      await store.queue;
      store.flushSync();
      // 完成事务早于图片 HTTP 发送结束；宽限内也让现有响应自然结束。
      await connectionsClosed;
      await store.close();
    } catch (error) {
      console.error('[runtime] failed to flush SQLite changes during shutdown:', error);
      clearTimeout(forceTimer);
      process.exit(1);
      return;
    }
    clearTimeout(forceTimer);
    process.exit(0);
  };
  process.once('SIGTERM', () => shutdown('SIGTERM'));
  process.once('SIGINT', () => shutdown('SIGINT'));
}

function isRecoverableRuntimeAbort(error: unknown) {
  const text = String(
    (error as GatewayError)?.stack || (error as GatewayError)?.message || error || '',
  );
  return (
    isAbortError(error) ||
    /direct generate timeout|This operation was aborted|ECONNRESET|ERR_STREAM_DESTROYED|socket hang up/i.test(
      text,
    )
  );
}

function httpError(statusCode: number, message: string) {
  const error: GatewayError = new Error(message);
  (error as GatewayError).statusCode = statusCode;
  return error;
}
