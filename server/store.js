import { existsSync, statSync } from 'node:fs';
import { mkdir, readFile, unlink } from 'node:fs/promises';
import crypto from 'node:crypto';
import path from 'node:path';
import { Worker } from 'node:worker_threads';
import Database from 'better-sqlite3';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import { sql, eq, and, desc, count, getTableColumns } from 'drizzle-orm';
import { recordTables, appSettings, appRecords, appMeta, generationRequests, generationIdempotency } from './storage-schema.js';
import { migrateStorage } from './storage-migrations.js';
import { queuedDeadlineTimestamp } from './queue-policy.js';
import { queryFailureAccounting } from './failure-accounting.js';

export const MAX_CACHE_IMAGES_LIMIT = 4000000;
export const MAX_FREE_STEPS = 28;
export const defaultArtist2_5D =
  `0.9::misaka_12003-gou ::, dino_(dinoartforame), wanke, liduke, year 2025, realistic, 4k, -2::green ::, textless version, The image is highly intricate finished drawn. Only the character's face is in anime style, but their body is in realistic style. 1.35::A highly finished photo-style artwork that has lively color, graphic texture, realistic skin surface, and lifelike flesh with little obliques::. 1.63::photorealistic::, 1.63::photo(medium)::, \\n20::best quality, absurdres, very aesthetic, detailed, masterpiece::,, very aesthetic, masterpiece, no text,`;
export const legacyDefaultArtist =
  'artist:ningen_mame,, noyu_(noyu23386566),, toosaka asagi,, location,\n20::best quality, absurdres, very aesthetic, detailed, masterpiece::,:,, very aesthetic, masterpiece, no text,';

const collections = Object.keys(recordTables);
const largeRuntimeCollections = new Set(['jobs', 'images', 'ledger']);
const collectionTables = Object.fromEntries(collections.map(name => [name, name]));
const emptyRecordRows = Object.fromEntries(Object.entries(recordTables).map(([name, table]) => [name,
  Object.fromEntries(Object.keys(getTableColumns(table)).map(column => [column, null]))]));

const defaultSettings = {
  serviceName: 'Nai2API',
  costPerImage: 1,
  maxCacheImages: 500,
  accountConcurrency: 1,
  publicBaseUrl: '',
  mockWhenNoAccount: true,
  promptApi: {
    baseUrl: '',
    apiKey: '',
    model: ''
  },
  defaultModel: 'nai-diffusion-4-5-full',
  defaultArtist: defaultArtist2_5D,
  defaultNegative:
    '{{{{bad anatomy}}}},{bad feet},bad hands,{{{bad proportions}}},{blurry},cloned face,cropped,{{{deformed}}},{{{disfigured}}},error,{{{extra arms}}},{extra digit},{{{extra legs}}},extra limbs,{{extra limbs}},{fewer digits},{{{fused fingers}}},gross proportions,jpeg artifacts,{{{{long neck}}}},low quality,{malformed limbs},{{missing arms}},{missing fingers},{{missing legs}},mutated hands,{{{mutation}}},normal quality,poorly drawn face,poorly drawn hands,signature,text,{{too many fingers}},{{{ugly}}},username,watermark,worst quality',
  defaults: {
    size: '绔栧浘',
    width: 832,
    height: 1216,
    steps: MAX_FREE_STEPS,
    scale: 6,
    cfg: 0,
    sampler: 'k_dpmpp_2m_sde',
    noiseSchedule: 'karras'
  }
};

const defaultDb = {
  settings: defaultSettings,
  cards: [],
  users: [],
  accounts: [],
  jobs: [],
  images: [],
  ledger: []
};
const usageChartDays = 7;
const beijingOffsetHours = 8;

export class JsonStore {
  constructor(dataDir) {
    this.dataDir = dataDir;
    this.dbPath = path.join(dataDir, 'library.sqlite');
    this.legacyDbPath = path.join(dataDir, 'library.json');
    this.legacyBackupPath = `${this.legacyDbPath}.bak`;
    this.queue = Promise.resolve();
    this.sqlite = null;
    this.statements = null;
    this.db = null;
    this.rowState = emptyCollectionMaps();
    this.orderKeys = emptyCollectionMaps();
    this.settingsState = '';
    this.pendingPersistScope = null;
    this.pendingPersistTimer = null;
    this.partialCollections = new Set();
    this.statsReader = null;
    this.adminReader = null;
    this.adminStatsCache = null;
    this.adminStatsPromise = null;
    this.adminFreshStatsPromise = null;
    this.lastImageTrimAt = 0;
  }

  async init() {
    const startedAt = Date.now();
    runtimeLog(`SQLite store init started: ${this.dbPath}`);
    await mkdir(this.dataDir, { recursive: true });
    this.openSqlite();
    this.createSchema();
    this.prepareStatements();
    this.openReadWorkers();

    if (!this.hasSqliteData()) {
      runtimeLog(`No SQLite data found. Checking legacy JSON in ${this.dataDir}`);
      const legacyDb = await this.readLegacyOrDefault();
      const safeDb = trimDb(normalizeDb(legacyDb));
      safeDb.accounts.forEach((account) => {
        account.inFlight = 0;
      });
      runtimeLog(`Migrating legacy data to SQLite: ${formatDbCounts(safeDb)}`);
      this.replaceAll(safeDb);
      await this.markLegacyMigrated();
      this.db = safeDb;
      this.initializeQueueSequence();
      runtimeLog(`SQLite store init completed in ${Date.now() - startedAt}ms: ${formatDbCounts(this.db)}`);
      return;
    }

    runtimeLog('SQLite data found. Loading tables into runtime cache.');
    this.db = this.loadFromSqlite();
    this.initializeQueueSequence();
    this.db.accounts.forEach((account) => {
      account.inFlight = 0;
    });
    trimDb(this.db, { collections: ['accounts'] });
    this.persistIncremental(this.db, { collections: ['accounts'] });
    runtimeLog(`SQLite store init completed in ${Date.now() - startedAt}ms: ${formatDbCounts(this.db)}`);
  }

  async read() {
    await this.ensureLoaded();
    return cloneDb(this.db);
  }

  async readCollections(requestedCollections = []) {
    await this.ensureLoaded();
    const snapshot = {};
    for (const key of requestedCollections) {
      if (largeRuntimeCollections.has(key) && this.partialCollections.has(key)) {
        throw new Error(`Refusing to clone partial large collection: ${key}`);
      }
      if (key === 'settings') {
        snapshot.settings = structuredClone(this.db.settings);
      } else if (Array.isArray(this.db[key])) {
        snapshot[key] = structuredClone(this.db[key]);
      }
    }
    return snapshot;
  }

  async readQueueStateCounts() {
    await this.ensureLoaded();
    const rows = this.sqlite.prepare(`
      SELECT status, source, COUNT(*) AS count
      FROM jobs INDEXED BY idx_jobs_active_created
      WHERE status IN ('queued', 'pending', 'running')
      GROUP BY status, source
    `).all();
    return rows.reduce((counts, row) => {
      const count = Number(row.count || 0);
      if (row.status === 'queued') {
        counts.queued += count;
        if (row.source === 'direct') counts.directQueued += count;
        if (row.source === 'openai') counts.openAiQueued += count;
      }
      if (row.status === 'pending') counts.pending += count;
      if (row.status === 'running') counts.running += count;
      return counts;
    }, { queued: 0, pending: 0, running: 0, directQueued: 0, openAiQueued: 0 });
  }

  async readImageFiles() {
    await this.ensureLoaded();
    if (this.partialCollections.has('images')) return null;
    return (this.db.images || []).map((image) => image.file).filter(Boolean);
  }

  async trimImageCache(maxCacheImages = null, options = {}) {
    // 私有部署保护：禁止自动裁剪图片缓存记录。
    if (process.env.NO_AUTO_DELETE === 'true') return [];
    await this.ensureLoaded();
    if (options.force !== true && Date.now() - this.lastImageTrimAt < 1000) return [];
    this.lastImageTrimAt = Date.now();
    this.flushPendingPersistSync();

    const maxCache = clampNumber(maxCacheImages ?? this.db.settings.maxCacheImages, 0, MAX_CACHE_IMAGES_LIMIT);
    this.db.settings.maxCacheImages = maxCache;
    const total = this.countRecords('images');
    const force = options.force === true || maxCache <= 0;
    const trimBuffer = force ? 0 : Math.max(0, Math.floor(Number(options.batchSize ?? 100) || 0));
    const trimAt = maxCache + trimBuffer;
    if (total < trimAt) return [];

    const maxDelete = trimImageCacheDeleteLimit(options.maxDelete);
    const deleteCount = Math.min(Math.max(0, total - maxCache), maxDelete);
    if (!deleteCount) return [];

    const startedAt = Date.now();
    const removedRows = this.sqlite.prepare(`
      SELECT id, order_value AS orderValue, data
      FROM images
      ORDER BY order_value ASC
      LIMIT @deleteCount
    `).all({ deleteCount });
    const removedImages = removedRows
      .map((row) => ({ row, image: safeJson(row.data, null) }))
      .filter((entry) => entry.image)
      .map((entry) => ({ ...entry.image, id: entry.image.id || entry.row.id }));
    const removedIds = removedImages.map((image) => String(image.id || '')).filter(Boolean);
    if (!removedIds.length) return [];

    const affectedJobs = [];
    for (const chunk of chunks(removedIds, 500)) {
      const placeholders = chunk.map(() => '?').join(',');
      const rows = this.sqlite.prepare(`
        SELECT id, order_value AS orderValue, data
        FROM jobs
        WHERE image_id IN (${placeholders})
      `).all(...chunk);
      affectedJobs.push(...rows);
    }

    const jobUpdates = affectedJobs
      .map((row) => {
        const job = safeJson(row.data, null);
        if (!job) return null;
        if (!job.id) job.id = row.id;
        job.imageId = '';
        const data = JSON.stringify(job);
        const order = Number(row.orderValue);
        return {
          id: row.id,
          data,
          order,
          sql: sqlRecord('jobs', job, order, data)
        };
      })
      .filter(Boolean);

    const applyTrim = this.sqlite.transaction(() => {
      for (const update of jobUpdates) this.statements.upsertRecord.jobs.run(update.sql);
      for (const id of removedIds) this.statements.deleteRecord.images.run({ id });
    });
    applyTrim();

    const removedSet = new Set(removedIds);
    this.db.images = (this.db.images || []).filter((image) => !removedSet.has(String(image.id || '')));
    for (const id of removedIds) {
      this.rowState.images?.delete(id);
      this.orderKeys.images?.delete(id);
    }

    const affectedJobIds = [];
    for (const update of jobUpdates) {
      affectedJobIds.push(update.id);
      const cachedJob = (this.db.jobs || []).find((job) => job.id === update.id);
      if (cachedJob) cachedJob.imageId = '';
      if (this.rowState.jobs?.has(update.id)) {
        this.rowState.jobs.set(update.id, { data: update.data, order: update.order });
        this.orderKeys.jobs.set(update.id, update.order);
      }
    }
    removedImages.affectedJobIds = uniqueStrings(affectedJobIds);
    this.trimRuntimePartialCaches();

    runtimeLog(`SQLite image cache trimmed ${removedIds.length} image record(s), affectedJobs=${removedImages.affectedJobIds.length}, totalBefore=${total}, max=${maxCache}, maxDelete=${maxDelete}, duration=${Date.now() - startedAt}ms`);
    return removedImages;
  }

  async clearRequestLogs() {
    await this.ensureLoaded();
    const startedAt = Date.now();
    const upperRow = await this.update(() => {
      this.flushPendingPersistSync();
      return this.sqlite.prepare('SELECT MAX(rowid) AS value FROM jobs').get().value || 0;
    }, { shouldPersist: () => false });
    const selectBatch = this.sqlite.prepare(`
      SELECT rowid AS cursor, id, status FROM jobs
      WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT 200
    `);
    const deleteBatch = this.sqlite.transaction((ids) => {
      for (const id of ids) this.statements.deleteRecord.jobs.run({ id });
    });
    let cursor = 0;
    let removed = 0;
    while (cursor < upperRow) {
      await this.update((db) => {
        this.flushPendingPersistSync();
        const rows = selectBatch.all(cursor, upperRow);
        cursor = rows.at(-1)?.cursor || upperRow;
        const ids = new Set(rows.filter((row) => row.status && !['queued', 'pending', 'running'].includes(row.status)).map((row) => row.id));
        deleteBatch(ids);
        db.jobs = db.jobs.filter((job) => !ids.has(job.id));
        for (const id of ids) {
          this.rowState.jobs.delete(id);
          this.orderKeys.jobs.delete(id);
        }
        removed += ids.size;
      }, { shouldPersist: () => false });
      // Let generation, polling and balance writes run between small transactions.
      await new Promise((resolve) => setImmediate(resolve));
    }
    this.refreshPartialCollectionState('jobs');
    this.adminStatsCache = null;
    const remaining = this.countRecords('jobs');
    runtimeLog(`SQLite request logs cleared ${removed} job record(s), remaining=${remaining}, duration=${Date.now() - startedAt}ms`);
    return { removed, remaining };
  }

  async clearCachedImages(options = {}) {
    await this.ensureLoaded();
    this.flushPendingPersistSync();

    const ids = uniqueStrings(options.ids || options.images);
    const query = String(options.q || options.query || '').trim().toLowerCase();
    const clearAll = options.all === true || options.mode === 'all';
    const startedAt = Date.now();
    const totalBefore = this.countRecords('images');
    const imageRows = this.selectImagesForClear({ ids, query, clearAll });
    const removedImages = imageRows
      .map((row) => ({ row, image: safeJson(row.data, null) }))
      .filter((entry) => entry.image)
      .map((entry) => ({ ...entry.image, id: entry.image.id || entry.row.id }));
    const removedIds = uniqueStrings(removedImages.map((image) => image.id));
    if (!removedIds.length) {
      return { deleted: 0, remaining: totalBefore, images: [] };
    }

    const affectedJobs = [];
    for (const chunk of chunks(removedIds, 500)) {
      const placeholders = chunk.map(() => '?').join(',');
      affectedJobs.push(...this.sqlite.prepare(`
        SELECT id, order_value AS orderValue, data
        FROM jobs
        WHERE image_id IN (${placeholders})
      `).all(...chunk));
    }

    const jobUpdates = affectedJobs
      .map((row) => {
        const job = safeJson(row.data, null);
        if (!job) return null;
        if (!job.id) job.id = row.id;
        job.imageId = '';
        const data = JSON.stringify(job);
        const order = Number(row.orderValue);
        return {
          id: row.id,
          data,
          order,
          sql: sqlRecord('jobs', job, order, data)
        };
      })
      .filter(Boolean);

    const ledger = {
      id: createId('log'),
      type: 'clear-cache',
      amount: removedIds.length,
      at: new Date().toISOString(),
      note: clearAll ? 'Cleared all cached images' : query ? `Cleared cached images matching ${query}` : 'Cleared selected cached images'
    };
    const ledgerOrder = Number(this.sqlite.prepare('SELECT MAX(order_value) AS value FROM ledger').get()?.value || 0) + 1;
    const ledgerData = JSON.stringify(ledger);
    const ledgerRow = sqlRecord('ledger', ledger, ledgerOrder, ledgerData);

    const applyClear = this.sqlite.transaction(() => {
      for (const update of jobUpdates) this.statements.upsertRecord.jobs.run(update.sql);
      for (const chunk of chunks(removedIds, 500)) {
        const placeholders = chunk.map(() => '?').join(',');
        this.sqlite.prepare(`DELETE FROM images WHERE id IN (${placeholders})`).run(...chunk);
      }
      this.statements.upsertRecord.ledger.run(ledgerRow);
    });
    applyClear();

    const removedSet = new Set(removedIds);
    this.db.images = (this.db.images || []).filter((image) => !removedSet.has(String(image.id || '')));
    for (const id of removedIds) {
      this.rowState.images?.delete(id);
      this.orderKeys.images?.delete(id);
    }

    for (const update of jobUpdates) {
      const cachedJob = (this.db.jobs || []).find((job) => job.id === update.id);
      if (cachedJob) cachedJob.imageId = '';
      if (this.rowState.jobs?.has(update.id)) {
        this.rowState.jobs.set(update.id, { data: update.data, order: update.order });
        this.orderKeys.jobs.set(update.id, update.order);
      }
    }

    this.db.ledger.unshift(ledger);
    this.rowState.ledger?.set(ledger.id, { data: ledgerData, order: ledgerOrder });
    this.orderKeys.ledger?.set(ledger.id, ledgerOrder);
    this.refreshPartialCollectionState('images');
    this.trimRuntimePartialCaches();

    const remaining = this.countRecords('images');
    runtimeLog(`SQLite image cache cleared ${removedIds.length} image record(s), affectedJobs=${jobUpdates.length}, totalBefore=${totalBefore}, remaining=${remaining}, duration=${Date.now() - startedAt}ms`);
    return { deleted: removedIds.length, remaining, images: removedImages };
  }

  selectImagesForClear({ ids = [], query = '', clearAll = false } = {}) {
    if (clearAll) {
      return this.sqlite.prepare('SELECT id, data FROM images ORDER BY order_value ASC').all();
    }

    const rowsById = new Map();
    for (const chunk of chunks(uniqueStrings(ids), 500)) {
      const placeholders = chunk.map(() => '?').join(',');
      const rows = this.sqlite.prepare(`SELECT id, data FROM images WHERE id IN (${placeholders})`).all(...chunk);
      rows.forEach((row) => rowsById.set(row.id, row));
    }
    if (query) {
      const rows = this.sqlite.prepare(`
        SELECT id, data FROM images
        WHERE search_text LIKE @query
        ORDER BY order_value ASC
      `).all({ query: `%${query}%` });
      rows.forEach((row) => rowsById.set(row.id, row));
    }
    return [...rowsById.values()];
  }

  hasPartialCollection(collection) {
    return this.partialCollections.has(collection);
  }

  nextQueueSequence() {
    // 元数据保留历史最高号；热任务还包含同一事务内刚分配、尚未提交的号。
    let maximum = Number(this.statements.readQueueSequence.get()?.value || 0);
    for (const job of this.db.jobs) maximum = Math.max(maximum, Number(job.queueSeq) || 0);
    const next = maximum + 1;
    if (!Number.isSafeInteger(next)) throw new Error('排队序号已达到安全整数上限');
    return next;
  }

  initializeQueueSequence() {
    // 精确沿用升级前实际选择：运行缓存的任务数组过滤后 reverse。
    // 不猜测 createdAt/order_value 的排序等价关系，也不改历史 order_value。
    const active = this.db.jobs.filter(job => ['queued', 'pending', 'running'].includes(job.status)).reverse();
    const changed = [];
    for (const job of active) {
      let dirty = false;
      if (job.status !== 'pending' && !job.queueSeq) { job.queueSeq = this.nextQueueSequence(); dirty = true; }
      if (!job.requestId && !Number.isSafeInteger(job.payloadBytes)) {
        job.payloadBytes = Buffer.byteLength(JSON.stringify(job.request || {}));
        dirty = true;
      }
      if (dirty) changed.push(job.id);
    }
    if (changed.length) this.persistIncremental(this.db, { dirtyRows: { jobs: changed } });
  }

  queuedJobIds() {
    return this.sqlite.prepare(`SELECT id FROM jobs INDEXED BY idx_jobs_queued_sequence
      WHERE status = 'queued' ORDER BY queue_seq ASC`).all().map(row => row.id);
  }

  persistQueueSequence(rows) {
    let maximum = 0;
    for (const row of rows) maximum = Math.max(maximum, Number(row.queue_seq) || 0);
    if (maximum > Number(this.statements.readQueueSequence.get()?.value || 0)) {
      this.statements.setMeta.run({ key: 'queue_sequence', value: String(maximum) });
    }
  }

  // 只读一份执行参数；状态、列表与容量查询不加载历史 payload。
  readGenerationRequest(id) {
    const row = this.statements.selectGenerationRequest.get({ id });
    return row ? JSON.parse(row.payload) : null;
  }

  readGenerationIdempotency(memberId, operation, keyHash) {
    return this.statements.selectGenerationIdempotency.get({ memberId, operation, keyHash }) || null;
  }

  readGenerationRequestState(id, memberId) {
    const request = this.sqlite.prepare(`SELECT id, source, operation, created_at AS createdAt
      FROM generation_requests WHERE id = ? AND member_id = ?`).get(id, memberId);
    if (!request) return null;
    // 父归属使用稳定成员 ID；图片仍必须属于对应 child，不能靠可轮换的当前 Key 代认。
    const jobs = this.sqlite.prepare(`SELECT j.id, j.status, j.model, j.created_at AS createdAt, j.updated_at AS updatedAt,
      COALESCE(json_extract(j.data, '$.sampleIndex'), 0) AS sampleIndex,
      json_extract(j.data, '$.completedAt') AS completedAt, json_extract(j.data, '$.error') AS error,
      json_extract(j.data, '$.cancelledAt') AS cancelledAt,
      json_extract(j.data, '$.lateResultAvailable') AS lateResultAvailable,
      j.image_id AS referencedImageId, i.id AS imageId
      FROM jobs j LEFT JOIN images i ON i.id = j.image_id AND i.token = j.user_token
      WHERE j.request_id = ? ORDER BY sampleIndex, j.id`).all(id);
    return { ...request, jobs };
  }

  activeGenerationPayloadBytes(inFlight = []) {
    // 从有界活动索引出发，只取字节元数据；绝不读取所有历史 payload。
    const held = new Map(this.sqlite.prepare(`
      SELECT id, payload_bytes AS bytes FROM generation_requests
      WHERE id IN (SELECT request_id FROM jobs INDEXED BY idx_jobs_active_created
        WHERE status IN ('queued', 'pending', 'running') AND request_id IS NOT NULL)
    `).all().map(row => [`request:${row.id}`, row.bytes]));
    for (const row of this.sqlite.prepare(`SELECT id, payload_bytes AS bytes FROM jobs INDEXED BY idx_jobs_active_created
      WHERE status IN ('queued', 'pending', 'running') AND request_id IS NULL`).all()) {
      held.set(`job:${row.id}`, row.bytes);
    }
    // 逻辑 failed 不代表执行器已经释放输入；按父请求去重，直到 control.finish。
    for (const attempt of inFlight) {
      const key = attempt.requestId ? `request:${attempt.requestId}` : `job:${attempt.jobId}`;
      if (held.has(key)) continue;
      const bytes = attempt.requestId
        ? this.sqlite.prepare('SELECT payload_bytes AS bytes FROM generation_requests WHERE id = ?').get(attempt.requestId)?.bytes
        : attempt.payloadBytes;
      held.set(key, Number(bytes || 0));
    }
    let total = 0;
    for (const bytes of held.values()) total += Number(bytes || 0);
    return total;
  }

  async findJobContext(id) {
    await this.ensureLoaded();
    const job = this.db.jobs.find((item) => item.id === id) || this.selectItemById('jobs', id);
    if (!job) return null;
    const account = job.accountId
      ? this.db.accounts.find((item) => item.id === job.accountId) || null
      : null;
    return {
      job: structuredClone(job),
      account: account ? structuredClone(account) : null,
      queue: this.jobQueueProgress(job)
    };
  }

  async readSettings() {
    await this.ensureLoaded();
    return structuredClone(this.db.settings);
  }

  async findImage(id) {
    await this.ensureLoaded();
    const row = this.statements.selectById.images.get({ id });
    if (row) return safeJson(row.data, null);
    const image = this.db.images.find((item) => item.id === id);
    return image ? structuredClone(image) : null;
  }

  async findImageByCacheKey(cacheKey) {
    await this.ensureLoaded();
    const row = this.sqlite.prepare(`
      SELECT data FROM images
      WHERE cache_key = ? AND COALESCE(mock, 0) = 0 AND mime_type != 'image/svg+xml'
      ORDER BY order_value DESC
      LIMIT 1
    `).get(String(cacheKey || ''));
    if (row) return safeJson(row.data, null);
    const image = this.db.images.find((item) => item.cacheKey === cacheKey && !item.mock && item.mimeType !== 'image/svg+xml');
    return image ? structuredClone(image) : null;
  }

  async readImagePage(options = {}) {
    await this.ensureLoaded();
    const limit = Math.max(1, Math.min(200, Math.floor(Number(options.limit || 60))));
    const offset = Math.max(0, Math.floor(Number(options.offset || 0)));
    const q = String(options.q || '').trim().toLowerCase();
    const tier = String(options.tier || '').trim().toLowerCase();
    const model = String(options.model || '').trim();
    const page = await this.adminReader.request('imagePage', { limit, offset, q, tier, model }, 15_000);
    return { ...page, maxCacheImages: this.db.settings.maxCacheImages };
  }

  async readUserPage(options = {}) {
    await this.ensureLoaded();
    const limit = Math.max(1, Math.min(500, Math.floor(Number(options.limit || 300))));
    const offset = Math.max(0, Math.floor(Number(options.offset || 0)));
    const q = String(options.q || '').trim().toLowerCase();
    return this.adminReader.request('userPage', { limit, offset, q }, 15_000);
  }

  async readUserByToken(token) {
    await this.ensureLoaded();
    const user = this.db.users.find((item) => item.token === token);
    return user ? structuredClone(user) : null;
  }

  async readCounts() {
    await this.ensureLoaded();
    return {
      users: this.db.users.length,
      enabledAccounts: this.db.accounts.filter((account) => account.enabled !== false).length,
      cards: this.db.cards.length
    };
  }

  async readOwnerJobRows(since, until) {
    await this.ensureLoaded();
    await this.queue;
    this.flushPendingPersistSync();
    // 只取统计字段，不加载提示词、图片、Key 或 Discord 授权数据。
    return this.sqlite.prepare(`
      SELECT j.id, j.status, j.model, j.created_at AS createdAt,
        COALESCE(u.id, json_extract(j.data, '$.fairCharge.userId'), 'removed') AS memberId,
        COALESCE(json_extract(u.data, '$.memberAuth.username'), '已移除成员') AS username,
        json_extract(j.data, '$.startedAt') AS startedAt,
        json_extract(j.data, '$.upstreamStartedAt') AS upstreamStartedAt,
        json_extract(j.data, '$.upstreamCompletedAt') AS upstreamCompletedAt,
        json_extract(j.data, '$.errorStatus') AS errorStatus,
        json_extract(j.data, '$.fairCharge.resource') AS resource,
        json_extract(j.data, '$.fairCharge.status') AS chargeStatus,
        json_extract(j.data, '$.fairCharge.amount') AS amount,
        json_extract(j.data, '$.fairCharge.fixed') AS fixed,
        json_extract(j.data, '$.fairCharge.purchased') AS purchased
      FROM jobs j LEFT JOIN users u ON u.token = j.user_token
      WHERE j.created_at >= ? AND j.created_at <= ? ORDER BY j.created_at DESC
    `).all(since, until);
  }

  async readFailureAccounting(before, now = Date.now()) {
    await this.ensureLoaded(); await this.queue.catch(() => {}); this.flushPendingPersistSync();
    return this.sqlite.transaction(() => queryFailureAccounting(this.sqlite, before, now)).deferred();
  }

  async readMemberLateResults(user) {
    await this.ensureLoaded(); await this.queue.catch(() => {}); this.flushPendingPersistSync();
    return this.sqlite.prepare(`SELECT j.id, j.model, j.created_at AS createdAt, j.image_id AS imageId,
      json_extract(j.data, '$.lateResultAt') AS lateResultAt, json_extract(j.data, '$.fairCharge') AS charge
      FROM jobs j JOIN images i ON i.id = j.image_id
      JOIN users u ON u.id = @userId AND u.token = j.user_token AND u.enabled = 1
      WHERE j.user_token = @token AND i.token = @token
        AND json_extract(j.data, '$.lateResultAvailable') = 1 AND json_extract(j.data, '$.lateResultAt') > ''
      ORDER BY json_extract(j.data, '$.lateResultAt') DESC, j.id DESC LIMIT 10`).all({ userId: user.id, token: user.token })
      .map(row => ({ ...row, charge: safeJson(row.charge, null) }));
  }

  async readMemberImageIds(user, jobIds) {
    const ids = uniqueStrings(jobIds).slice(0, 30);
    if (!ids.length) return new Map();
    await this.ensureLoaded(); await this.queue.catch(() => {}); this.flushPendingPersistSync();
    const rows = this.sqlite.prepare(`SELECT j.id, j.image_id AS imageId FROM jobs j JOIN images i ON i.id = j.image_id
      JOIN users u ON u.id = ? AND u.token = j.user_token AND u.enabled = 1
      WHERE j.user_token = ? AND i.token = ? AND j.id IN (${ids.map(() => '?').join(',')})`).all(user.id, user.token, user.token, ...ids);
    return new Map(rows.map(row => [row.id, row.imageId]));
  }

  async readAdminSummary(options = {}) {
    await this.ensureLoaded();
    const stats = await this.readAdminSummaryStats(options);
    const statsCutoff = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    const imageCount = this.countRecords('images');
    return {
      settings: structuredClone(this.db.settings),
      userCount: this.db.users.length,
      accounts: structuredClone(this.db.accounts),
      jobs: this.selectItems('jobs', "WHERE status = 'failed' ORDER BY order_value DESC LIMIT 50"),
      ...stats,
      errorJobs: this.selectItems('jobs', `
        WHERE error_loggable = 1
          AND updated_at >= @cutoff
        ORDER BY updated_at DESC
        LIMIT 100
      `, { cutoff: statsCutoff }),
      imageCount,
      imageTotal: imageCount,
      cacheImageCount: imageCount
    };
  }

  async readAdminSummaryStats({ fresh = false } = {}) {
    if (fresh) return this.readFreshAdminStats();
    // Job writes must not invalidate this cache: traffic would disable caching entirely.
    const now = Date.now();
    if (this.adminStatsCache) {
      if (now - this.adminStatsCache.at >= 30_000 && !this.adminStatsPromise && !this.adminFreshStatsPromise) {
        this.refreshAdminStats(now).catch(() => {});
      }
      return structuredClone(this.adminStatsCache.value);
    }
    return (this.adminFreshStatsPromise || this.refreshAdminStats(now)).catch(() => emptyAdminStats(now));
  }

  async readFreshAdminStats() {
    if (!this.adminFreshStatsPromise) {
      this.adminFreshStatsPromise = (async () => {
        try {
          // An older background read may predate pending writes; run a new read after it.
          await this.adminStatsPromise?.catch(() => {});
          await this.queue.catch(() => {});
          this.flushPendingPersistSync();
          return await this.refreshAdminStats();
        } finally {
          this.adminFreshStatsPromise = null;
        }
      })();
    }
    return this.adminFreshStatsPromise;
  }

  async refreshAdminStats(now = Date.now()) {
    if (this.adminStatsPromise) return this.adminStatsPromise;
    this.adminStatsPromise = Promise.resolve()
      .then(() => this.statsReader.request('adminStats', { now, days: usageChartDays }, 3_000))
      .then((value) => {
        this.adminStatsCache = { at: now, value };
        return structuredClone(value);
      }).catch((error) => {
        runtimeLog(`Admin stats worker unavailable: ${error.message}`);
        throw error;
      }).finally(() => {
        this.adminStatsPromise = null;
      });
    return this.adminStatsPromise;
  }

  async ensureLoaded() {
    if (this.db) return;
    this.openSqlite();
    this.createSchema();
    this.prepareStatements();
    this.openReadWorkers();
    this.db = this.hasSqliteData() ? this.loadFromSqlite() : trimDb(normalizeDb(defaultDb));
    if (!this.hasSqliteData()) this.replaceAll(this.db);
  }

  async readRaw() {
    await this.ensureLoaded();
    if ([...this.partialCollections].some((collection) => largeRuntimeCollections.has(collection))) {
      throw new Error('Refusing to clone full database while large collections are partially loaded.');
    }
    return cloneDb(this.db);
  }

  async write(db) {
    await this.ensureLoaded();
    this.flushPendingPersistSync();
    const safeDb = trimDb(normalizeDb(db));
    this.replaceAll(safeDb);
    this.db = safeDb;
  }

  async update(mutator, options = {}) {
    this.queue = this.queue.catch(() => {}).then(async () => {
      await this.ensureLoaded();
      // 前一次延迟写只能包含前一次变更，不能把本次尚未提交的修改一起刷入。
      this.flushPendingPersistSync();
      try {
        const result = await mutator(this.db);
        const persistOptions = resolvePersistenceOptions(options, result, this.db);
        const snapshot = cloneValue(result);
        if (shouldPersistUpdate(result, persistOptions)) {
          const scope = persistenceScope(persistOptions);
          if (shouldDeferPersist(scope, persistOptions)) {
            this.schedulePersist(scope);
          } else {
            if (!scope.hasDirtyRows) trimDb(this.db, { collections: scope.full ? null : scope.collections });
            this.persistIncremental(this.db, scope);
          }
          // 新父记录已进入活动 SQL 统计；下一项串行受理开始前移交临时正文占用。
          persistOptions.generationPayloadClaim?.release();
        }
        return snapshot;
      } catch (error) {
        // SQL 回滚不会还原 JS 对象；只在失败时重载已提交数据，避免每次预扣复制整个热缓存。
        // 先清空引用，若重载也失败，后续请求不得继续使用污染的内存状态。
        this.clearPendingPersist();
        this.db = null;
        this.db = this.loadFromSqlite();
        throw error;
      }
    });
    return this.queue;
  }

  scheduleFlush() {
    this.schedulePersist({ full: true, includeSettings: true, collections, dirtyRows: {}, hasDirtyRows: false });
    return this.pendingPersistTimer;
  }

  async flush() {
    await this.ensureLoaded();
    this.flushPendingPersistSync();
    this.persistIncremental(this.db);
  }

  flushSync() {
    if (!this.db) return;
    this.flushPendingPersistSync();
  }

  async close() {
    if (this.sqlite) this.flushPendingPersistSync();
    await Promise.allSettled([this.statsReader?.close(), this.adminReader?.close()].filter(Boolean));
    this.statsReader = null;
    this.adminReader = null;
    this.sqlite?.close();
    this.sqlite = null;
    this.statements = null;
  }

  openReadWorkers() {
    this.statsReader ||= new SqliteReadWorker(this.dbPath, 'stats');
    this.adminReader ||= new SqliteReadWorker(this.dbPath, 'admin');
  }

  schedulePersist(scope) {
    this.pendingPersistScope = mergePersistenceScopes(this.pendingPersistScope, scope);
    if (this.pendingPersistTimer) return;
    this.pendingPersistTimer = setTimeout(() => {
      this.pendingPersistTimer = null;
      try {
        this.flushPendingPersistSync();
      } catch (error) {
        // 保留待写变更供下次更新/关闭重试；异步写失败不能变成未捕获异常。
        console.error('[runtime] SQLite deferred persist failed:', error);
      }
    }, sqliteWriteDebounceMs());
    this.pendingPersistTimer.unref?.();
  }

  flushPendingPersistSync() {
    if (!this.pendingPersistScope) return;
    if (this.pendingPersistTimer) {
      clearTimeout(this.pendingPersistTimer);
      this.pendingPersistTimer = null;
    }
    const scope = this.pendingPersistScope;
    if (!scope.hasDirtyRows) trimDb(this.db, { collections: scope.full ? null : scope.collections });
    this.persistIncremental(this.db, scope);
    // 写失败时保留待写范围；下一次更新必须先重试它，不能静默丢掉已接受的延迟变更。
    this.pendingPersistScope = null;
  }

  selectItems(collection, clause = 'ORDER BY order_value DESC', params = {}) {
    const table = collectionTables[collection];
    if (!table) return [];
    const rows = this.sqlite.prepare(`SELECT data FROM ${table} ${clause}`).all(params);
    return rows.map((row) => safeJson(row.data, null)).filter(Boolean);
  }

  selectItemById(collection, id) {
    const statement = this.statements.selectById[collection];
    if (!statement || !id) return null;
    const row = statement.get({ id: String(id) });
    return row ? safeJson(row.data, null) : null;
  }

  loadJobForUpdate(id) {
    // 仅在串行 update 内使用：冷任务仍是旧行，必须恢复原排序及已提交快照，不能按新行分配顺序。
    const cached = this.db.jobs.find(job => job.id === id);
    if (cached) return cached;
    const row = this.statements.selectById.jobs.get({ id });
    const job = row ? safeJson(row.data, null) : null;
    if (!job) return null;
    const order = Number(row.orderValue);
    this.rowState.jobs.set(id, { data: row.data, order });
    this.orderKeys.jobs.set(id, order);
    const index = this.db.jobs.findIndex(item => this.orderKeys.jobs.get(item.id) < order);
    this.db.jobs.splice(index < 0 ? this.db.jobs.length : index, 0, job);
    return job;
  }

  readOperatingLossSettlements(lossEventId) {
    // 仅在已串行化的管理写事务内调用，覆盖已退出热缓存的核销历史。
    return this.selectItems('ledger', "WHERE source = 'operating_loss_covered' AND json_extract(data, '$.lossEventId') = @id", { id: lossEventId });
  }

  async readDueGenerationReviews(now, limit = 100) {
    await this.ensureLoaded();
    await this.queue.catch(() => {});
    this.flushPendingPersistSync();
    // 谓词与部分索引保持一致；读取持久历史，不漏掉已退出热缓存的失败任务。
    return this.selectItems('jobs', `WHERE json_extract(data, '$.generationReview.status') = 'pending'
      AND json_extract(data, '$.generationReview.deadlineAt') <= @now
      ORDER BY json_extract(data, '$.generationReview.deadlineAt') LIMIT @limit`, { now, limit });
  }

  countRecords(collection) {
    const statement = this.statements.countRecords[collection];
    if (!statement) return 0;
    return Number(statement.get()?.count || 0);
  }

  jobQueueProgress(job) {
    // Use the exact in-memory order used by the scheduler, including unflushed writes.
    return jobQueueProgress(job, this.db.jobs);
  }

  selectJobStatRows(clause = 'ORDER BY order_value DESC', params = {}) {
    const rows = this.sqlite.prepare(`
      SELECT
        id,
        status,
        account_id AS accountId,
        created_at AS createdAt,
        updated_at AS updatedAt
      FROM jobs
      ${clause}
    `).all(params);
    return rows.map((row) => ({
      id: row.id || '',
      status: row.status || '',
      accountId: row.accountId || '',
      createdAt: row.createdAt || '',
      updatedAt: row.updatedAt || ''
    }));
  }

  openSqlite() {
    if (this.sqlite) return;
    const startedAt = Date.now();
    this.sqlite = new Database(this.dbPath);
    this.sqlite.pragma('journal_mode = WAL');
    this.sqlite.pragma('synchronous = NORMAL');
    this.sqlite.pragma('foreign_keys = ON');
    runtimeLog(`SQLite opened in ${Date.now() - startedAt}ms`);
  }

  createSchema() {
    const startedAt = Date.now();
    migrateStorage(this.sqlite);
    runtimeLog(`SQLite migrations checked in ${Date.now() - startedAt}ms`);
  }

  prepareStatements() {
    if (this.statements) return;
    const db = drizzle(this.sqlite);
    this.statements = {
      hasSettings: db.select({ value: sql`1` }).from(appSettings).where(eq(appSettings.key, 'settings')).limit(1).prepare(),
      hasGenericRecords: db.select({ value: sql`1` }).from(appRecords).limit(1).prepare(),
      readQueueSequence: db.select({ value: appMeta.value }).from(appMeta).where(eq(appMeta.key, 'queue_sequence')).prepare(),
      selectSettings: db.select({ value: appSettings.value }).from(appSettings).where(eq(appSettings.key, 'settings')).prepare(),
      selectGenericRecords: db.select({ id: appRecords.id, orderValue: appRecords.order_value, data: appRecords.data })
        .from(appRecords).where(eq(appRecords.collection, sql.placeholder('collection'))).orderBy(desc(appRecords.order_value)).prepare(),
      replaceSettings: db.insert(appSettings).values({ key: 'settings', value: sql.placeholder('value') })
        .onConflictDoUpdate({ target: appSettings.key, set: { value: sql`excluded.value` } }).prepare(),
      insertGenerationRequest: db.insert(generationRequests).values(Object.fromEntries(
        Object.keys(getTableColumns(generationRequests)).map(key => [key, sql.placeholder(key)])
      )).prepare(),
      selectGenerationRequest: db.select({ payload: generationRequests.payload }).from(generationRequests)
        .where(eq(generationRequests.id, sql.placeholder('id'))).prepare(),
      insertGenerationIdempotency: db.insert(generationIdempotency).values(Object.fromEntries(
        Object.keys(getTableColumns(generationIdempotency)).map(key => [key, sql.placeholder(key)])
      )).prepare(),
      selectGenerationIdempotency: db.select().from(generationIdempotency).where(and(
        eq(generationIdempotency.member_id, sql.placeholder('memberId')),
        eq(generationIdempotency.operation, sql.placeholder('operation')),
        eq(generationIdempotency.key_hash, sql.placeholder('keyHash'))
      )).prepare(),
      deleteAllSettings: db.delete(appSettings).prepare(),
      deleteGenericRecords: db.delete(appRecords).prepare(),
      setMeta: db.insert(appMeta).values({ key: sql.placeholder('key'), value: sql.placeholder('value') })
        .onConflictDoUpdate({ target: appMeta.key, set: { value: sql`excluded.value` } }).prepare(),
      hasTypedRecords: {}, selectRecords: {}, selectById: {}, countRecords: {},
      deleteAllRecords: {}, upsertRecord: {}, deleteRecord: {}
    };

    for (const [collection, table] of Object.entries(recordTables)) {
      const columns = Object.keys(getTableColumns(table));
      const values = Object.fromEntries(columns.map(column => [column, sql.placeholder(column)]));
      const updates = Object.fromEntries(columns.filter(column => column !== 'id')
        .map(column => [column, sql`excluded.${sql.identifier(column)}`]));
      this.statements.hasTypedRecords[collection] = db.select({ value: sql`1` }).from(table).limit(1).prepare();
      this.statements.selectRecords[collection] = db.select({ id: table.id, orderValue: table.order_value, data: table.data })
        .from(table).orderBy(desc(table.order_value)).prepare();
      this.statements.selectById[collection] = db.select({ data: table.data, orderValue: table.order_value }).from(table)
        .where(eq(table.id, sql.placeholder('id'))).limit(1).prepare();
      this.statements.countRecords[collection] = db.select({ count: count() }).from(table).prepare();
      this.statements.deleteAllRecords[collection] = db.delete(table).prepare();
      // 准备语句不写入；所有 run 仍由既有 persister 的同步 SQLite 事务统一提交。
      this.statements.upsertRecord[collection] = db.insert(table).values(values)
        .onConflictDoUpdate({ target: table.id, set: updates }).prepare();
      this.statements.deleteRecord[collection] = db.delete(table).where(eq(table.id, sql.placeholder('id'))).prepare();
    }
  }

  hasSqliteData() {
    return Boolean(this.statements.hasSettings.get() || this.hasTypedRecords() || this.statements.hasGenericRecords.get());
  }

  hasTypedRecords() {
    return collections.some((collection) => this.statements.hasTypedRecords[collection].get());
  }

  loadFromSqlite() {
    if (!this.hasTypedRecords() && this.statements.hasGenericRecords.get()) {
      const startedAt = Date.now();
      runtimeLog('Legacy SQLite app_records table found. Upgrading to typed tables.');
      const db = this.loadFromGenericRecords();
      runtimeLog(`Loaded legacy app_records in ${Date.now() - startedAt}ms: ${formatDbCounts(db)}`);
      this.replaceAll(db);
      this.statements.deleteGenericRecords.run();
      this.statements.setMeta.run({ key: 'generic_records_migrated_at', value: new Date().toISOString() });
      runtimeLog(`Upgraded app_records to typed tables in ${Date.now() - startedAt}ms`);
      return this.loadFromSqlite();
    }

    const startedAt = Date.now();
    const settingsRow = this.statements.selectSettings.get();
    const db = {
      ...defaultDb,
      settings: settingsRow ? safeJson(settingsRow.value, defaultSettings) : defaultSettings
    };
    const rowState = emptyCollectionMaps();
    const orderKeys = emptyCollectionMaps();
    const totalCounts = Object.fromEntries(collections.map((collection) => [collection, this.countRecords(collection)]));
    this.partialCollections = new Set();

    for (const collection of collections) {
      const rows = this.runtimeRowsForCollection(collection);
      if (rows.length < totalCounts[collection]) this.partialCollections.add(collection);
      db[collection] = rows.map((row) => {
        rowState[collection].set(row.id, { data: row.data, order: Number(row.orderValue) });
        orderKeys[collection].set(row.id, Number(row.orderValue));
        return safeJson(row.data, null);
      }).filter(Boolean);
    }

    const normalized = normalizeDb(db);
    this.rowState = rowState;
    this.orderKeys = orderKeys;
    this.settingsState = JSON.stringify(normalized.settings);
    runtimeLog(`Loaded typed SQLite runtime cache in ${Date.now() - startedAt}ms: ${formatDbCounts(normalized)} total=${formatDbCounts(totalCounts)} partial=${[...this.partialCollections].join(',') || 'none'}`);
    return normalized;
  }

  runtimeRowsForCollection(collection) {
    const table = collectionTables[collection];
    if (!table) return [];
    if (collection === 'jobs') {
      // Read the small active index separately; OR makes SQLite scan all history.
      const active = this.sqlite.prepare(`
        SELECT id, order_value AS orderValue, data FROM ${table} INDEXED BY idx_jobs_active_created
        WHERE status IN ('queued', 'pending', 'running')
      `).all();
      const recent = this.sqlite.prepare(`
        SELECT id, order_value AS orderValue, data FROM ${table}
        ORDER BY order_value DESC
        LIMIT @limit
      `).all({ limit: runtimeCacheLimit('jobs') });
      return [...new Map([...active, ...recent].map((row) => [row.id, row])).values()]
        .sort((a, b) => b.orderValue - a.orderValue);
    }
    if (collection === 'images' || collection === 'ledger') {
      return this.sqlite.prepare(`
        SELECT id, order_value AS orderValue, data FROM ${table}
        ORDER BY order_value DESC
        LIMIT @limit
      `).all({ limit: runtimeCacheLimit(collection) });
    }
    return this.statements.selectRecords[collection].all();
  }

  loadFromGenericRecords() {
    const settingsRow = this.statements.selectSettings.get();
    const db = {
      ...defaultDb,
      settings: settingsRow ? safeJson(settingsRow.value, defaultSettings) : defaultSettings
    };
    for (const collection of collections) {
      const rows = this.statements.selectGenericRecords.all({ collection });
      db[collection] = rows.map((row) => safeJson(row.data, null)).filter(Boolean);
    }
    return normalizeDb(db);
  }

  replaceAll(db) {
    const safeDb = trimDb(normalizeDb(db));
    const snapshot = buildSnapshotState(safeDb, emptyCollectionMaps(), { dense: true });
    const startedAt = Date.now();
    const writeAll = this.sqlite.transaction(() => {
      this.statements.deleteAllSettings.run();
      for (const collection of collections) {
        this.statements.deleteAllRecords[collection].run();
      }
      this.statements.replaceSettings.run({ value: snapshot.settingsData });
      for (const collection of collections) {
        for (const [id, row] of snapshot.rowState[collection]) {
          this.statements.upsertRecord[collection].run(row.sql);
        }
      }
      this.persistQueueSequence([...snapshot.rowState.jobs.values()].map(row => row.sql));
      // 显式全库替换才删除失去所有任务引用的父记录；普通完成不会删除历史。
      this.sqlite.exec('DELETE FROM generation_requests WHERE NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.request_id = generation_requests.id)');
    });
    writeAll();
    this.clearPendingPersist();
    this.partialCollections = new Set();
    this.adminStatsCache = null;
    this.settingsState = snapshot.settingsData;
    this.rowState = snapshot.rowState;
    this.orderKeys = snapshot.orderKeys;
    runtimeLog(`SQLite full replace wrote ${formatDbCounts(safeDb)} in ${Date.now() - startedAt}ms`);
  }

  clearPendingPersist() {
    if (this.pendingPersistTimer) {
      clearTimeout(this.pendingPersistTimer);
      this.pendingPersistTimer = null;
    }
    this.pendingPersistScope = null;
  }

  persistIncremental(db, options = {}) {
    const startedAt = Date.now();
    const scope = persistenceScope(options);
    if (!scope.full && !scope.includeSettings && !scope.collections.length && !scope.generationIdempotency) return;
    if (scope.hasDirtyRows) {
      const fullCollections = scope.collections.filter((collection) => !scope.dirtyRows[collection]?.size);
      this.assertFullCollectionsAreLoaded(fullCollections);
      if (fullCollections.length) trimDb(db, { collections: fullCollections });
      this.persistDirtyRows(db, scope, startedAt);
      return;
    }
    this.assertFullCollectionsAreLoaded(scope.full ? collections : scope.collections);
    const safeDb = trimDb(normalizeDb(db), { collections: scope.full ? null : scope.collections });
    const snapshot = buildSnapshotState(safeDb, this.orderKeys, {
      collections: scope.full ? null : scope.collections,
      includeSettings: scope.includeSettings,
      baseRowState: scope.full ? null : this.rowState,
      baseOrderKeys: scope.full ? null : this.orderKeys
    });
    const changes = collectPersistenceChanges({
      previousSettings: this.settingsState,
      previousRows: this.rowState,
      nextSettings: snapshot.settingsData,
      nextRows: snapshot.rowState,
      includeSettings: scope.includeSettings,
      collections: scope.full ? collections : scope.collections
    });
    if (!changes.settingsChanged && !changes.deletes.length && !changes.upserts.length && !scope.generationRequest && !scope.generationIdempotency) return;

    const applyChanges = this.sqlite.transaction(() => {
      // 父请求和子任务/预扣使用同一个同步事务；任一写入失败全部回滚。
      if (scope.generationRequest) this.statements.insertGenerationRequest.run(scope.generationRequest);
      if (changes.settingsChanged) this.statements.replaceSettings.run({ value: snapshot.settingsData });
      for (const change of changes.deletes) {
        this.statements.deleteRecord[change.collection].run({ id: change.id });
      }
      for (const change of changes.upserts) {
        this.statements.upsertRecord[change.collection].run(change.sql);
      }
      this.persistQueueSequence(changes.upserts.filter(change => change.collection === 'jobs').map(change => change.sql));
      if (scope.generationIdempotency) this.statements.insertGenerationIdempotency.run(scope.generationIdempotency);
    });
    applyChanges();
    if (scope.includeSettings) this.settingsState = snapshot.settingsData;
    for (const collection of (scope.full ? collections : scope.collections)) {
      this.rowState[collection] = snapshot.rowState[collection];
      this.orderKeys[collection] = snapshot.orderKeys[collection];
    }
    const duration = Date.now() - startedAt;
    if (duration >= sqliteSlowLogMs()) {
      const scopeText = scope.full ? 'full' : ['settings', ...scope.collections].filter((item, index, array) => {
        if (item !== 'settings') return true;
        return scope.includeSettings && array.indexOf(item) === index;
      }).join(',');
      runtimeLog(`SQLite incremental persist slow: ${duration}ms scope=${scopeText || 'none'} upserts=${changes.upserts.length} deletes=${changes.deletes.length} settings=${changes.settingsChanged ? 1 : 0}`);
    }
    this.trimRuntimePartialCaches();
  }

  persistDirtyRows(db, scope, startedAt = Date.now()) {
    const changes = {
      settingsChanged: false,
      deletes: [],
      upserts: []
    };
    const fullCollections = [];

    if (scope.includeSettings) {
      const nextSettings = JSON.stringify(db.settings);
      changes.settingsChanged = this.settingsState !== nextSettings;
      if (changes.settingsChanged) changes.nextSettings = nextSettings;
    }

    for (const collection of scope.collections) {
      const dirtyIds = scope.dirtyRows[collection];
      if (!dirtyIds || !dirtyIds.size) {
        fullCollections.push(collection);
        continue;
      }
      const previousRows = this.rowState[collection] || new Map();
      const previousOrders = this.orderKeys[collection] || new Map();
      const items = Array.isArray(db[collection]) ? db[collection] : [];
      const dirtyIndexes = dirtyItemIndexes(items, dirtyIds);
      for (const id of dirtyIds) {
        const index = dirtyIndexes.get(id) ?? -1;
        if (index < 0) {
          if (previousRows.has(id)) changes.deletes.push({ collection, id });
          continue;
        }
        const item = items[index];
        ensureItemIds(collection, [item]);
        const data = JSON.stringify(item);
        const order = dirtyRowOrder(items, index, previousOrders);
        const row = {
          data,
          order,
          sql: sqlRecord(collection, item, order, data)
        };
        const old = previousRows.get(id);
        if (!old || old.data !== row.data || old.order !== row.order) {
          changes.upserts.push({ collection, id, data: row.data, order: row.order, sql: row.sql });
        }
      }
    }

    if (fullCollections.length) {
      this.assertFullCollectionsAreLoaded(fullCollections);
      const snapshot = buildSnapshotState(db, this.orderKeys, {
        collections: fullCollections,
        includeSettings: false,
        baseRowState: this.rowState,
        baseOrderKeys: this.orderKeys
      });
      const fullChanges = collectPersistenceChanges({
        previousSettings: this.settingsState,
        previousRows: this.rowState,
        nextSettings: snapshot.settingsData,
        nextRows: snapshot.rowState,
        includeSettings: false,
        collections: fullCollections
      });
      changes.deletes.push(...fullChanges.deletes);
      changes.upserts.push(...fullChanges.upserts);
    }

    if (!changes.settingsChanged && !changes.deletes.length && !changes.upserts.length && !scope.generationRequest && !scope.generationIdempotency) return;

    const applyChanges = this.sqlite.transaction(() => {
      // 父请求和子任务/预扣使用同一个同步事务；任一写入失败全部回滚。
      if (scope.generationRequest) this.statements.insertGenerationRequest.run(scope.generationRequest);
      if (changes.settingsChanged) this.statements.replaceSettings.run({ value: changes.nextSettings });
      for (const change of changes.deletes) {
        this.statements.deleteRecord[change.collection].run({ id: change.id });
      }
      for (const change of changes.upserts) {
        this.statements.upsertRecord[change.collection].run(change.sql);
      }
      this.persistQueueSequence(changes.upserts.filter(change => change.collection === 'jobs').map(change => change.sql));
      if (scope.generationIdempotency) this.statements.insertGenerationIdempotency.run(scope.generationIdempotency);
    });
    applyChanges();

    if (changes.settingsChanged) this.settingsState = changes.nextSettings;
    for (const change of changes.deletes) {
      this.rowState[change.collection]?.delete(change.id);
      this.orderKeys[change.collection]?.delete(change.id);
    }
    for (const change of changes.upserts) {
      this.rowState[change.collection]?.set(change.id, { data: change.data, order: change.order });
      this.orderKeys[change.collection]?.set(change.id, change.order);
    }

    if (fullCollections.length) {
      const snapshot = buildSnapshotState(db, this.orderKeys, {
        collections: fullCollections,
        includeSettings: false,
        baseRowState: this.rowState,
        baseOrderKeys: this.orderKeys
      });
      for (const collection of fullCollections) {
        this.rowState[collection] = snapshot.rowState[collection];
        this.orderKeys[collection] = snapshot.orderKeys[collection];
      }
    }

    const duration = Date.now() - startedAt;
    if (duration >= sqliteSlowLogMs()) {
      runtimeLog(`SQLite dirty-row persist slow: ${duration}ms scope=${scope.collections.join(',') || 'none'} upserts=${changes.upserts.length} deletes=${changes.deletes.length} settings=${changes.settingsChanged ? 1 : 0}`);
    }
    this.trimRuntimePartialCaches();
  }

  assertFullCollectionsAreLoaded(collectionList = []) {
    const partial = collectionList.filter((collection) => this.partialCollections.has(collection) && ['jobs', 'images'].includes(collection));
    if (partial.length) {
      throw new Error(`Refusing full persistence for partially loaded collection(s): ${partial.join(',')}`);
    }
  }

  trimRuntimePartialCaches() {
    if (!this.db) return;
    if (this.trimRuntimeCollection('images', runtimeCacheLimit('images'))) {
      this.partialCollections.add('images');
    }
    if (this.trimRuntimeCollection('ledger', runtimeCacheLimit('ledger'))) {
      this.partialCollections.add('ledger');
    }

    const limit = runtimeCacheLimit('jobs');
    const retained = [];
    const removed = [];
    for (const [index, job] of (this.db.jobs || []).entries()) {
      if (index < limit || ['queued', 'pending', 'running'].includes(job.status)) {
        retained.push(job);
      } else {
        removed.push(job);
      }
    }
    if (removed.length) {
      this.db.jobs = retained;
      this.partialCollections.add('jobs');
      for (const job of removed) {
        if (!job?.id) continue;
        this.rowState.jobs?.delete(job.id);
        this.orderKeys.jobs?.delete(job.id);
      }
    }
  }

  trimRuntimeCollection(collection, limit) {
    const items = Array.isArray(this.db?.[collection]) ? this.db[collection] : [];
    if (items.length <= limit) return 0;
    const removed = items.slice(limit);
    this.db[collection] = items.slice(0, limit);
    for (const item of removed) {
      if (!item?.id) continue;
      this.rowState[collection]?.delete(item.id);
      this.orderKeys[collection]?.delete(item.id);
    }
    return removed.length;
  }

  refreshPartialCollectionState(collection) {
    if (!collectionTables[collection]) return;
    const loaded = Array.isArray(this.db?.[collection]) ? this.db[collection].length : 0;
    const total = this.countRecords(collection);
    if (loaded < total) this.partialCollections.add(collection);
    else this.partialCollections.delete(collection);
  }

  async readLegacyOrDefault() {
    const candidates = [this.legacyDbPath, this.legacyBackupPath];
    let lastError = null;
    for (const filePath of candidates) {
      if (!existsSync(filePath)) continue;
      try {
        runtimeLog(`Reading legacy JSON: ${filePath} (${formatFileSize(filePath)})`);
        const raw = await readFile(filePath, 'utf8');
        const parsed = normalizeDb(JSON.parse(raw));
        runtimeLog(`Legacy JSON loaded: ${formatDbCounts(parsed)}`);
        return parsed;
      } catch (error) {
        lastError = error;
        runtimeLog(`Failed to read legacy JSON ${filePath}: ${error.message}`);
      }
    }
    if (lastError) throw lastError;
    return normalizeDb(defaultDb);
  }

  async markLegacyMigrated() {
    const migratedAt = new Date().toISOString();
    this.statements.setMeta.run({ key: 'schema_version', value: '1' });
    this.statements.setMeta.run({ key: 'last_migrated_at', value: migratedAt });
      // 私有部署保护：迁移后保留旧版 JSON 文件。
      if (process.env.NO_AUTO_DELETE === 'true') return;
    const deleted = [];
    for (const filePath of [this.legacyDbPath, this.legacyBackupPath]) {
      if (!existsSync(filePath)) continue;
      try {
        await unlink(filePath);
        deleted.push(filePath);
        runtimeLog(`Deleted migrated legacy JSON: ${filePath}`);
      } catch (error) {
        console.error(`Failed to delete migrated legacy JSON ${filePath}:`, error);
      }
    }
    this.statements.setMeta.run({ key: 'legacy_json_deleted', value: JSON.stringify(deleted) });
    if (deleted.length) console.log(`Migrated legacy JSON data to SQLite and deleted ${deleted.length} old JSON file(s).`);
  }
}

function buildSnapshotState(db, previousOrderKeys, options = {}) {
  const settingsData = options.includeSettings === false ? '' : JSON.stringify(db.settings);
  const hasCollectionOption = options.collections !== undefined && options.collections !== null;
  const targetCollections = hasCollectionOption
    ? normalizeCollectionList(options.collections) || []
    : collections;
  const rowState = options.baseRowState ? cloneCollectionMaps(options.baseRowState) : emptyCollectionMaps();
  const orderKeys = options.baseOrderKeys ? cloneCollectionMaps(options.baseOrderKeys) : emptyCollectionMaps();

  for (const collection of targetCollections) {
    const items = Array.isArray(db[collection]) ? db[collection] : [];
    ensureItemIds(collection, items);
    rowState[collection] = new Map();
    const orders = options.dense
      ? denseOrderValues(items)
      : assignOrderValues(items, previousOrderKeys[collection] || new Map());
    orderKeys[collection] = orders;
    for (const item of items) {
      const id = String(item.id);
      const data = JSON.stringify(item);
      const order = Number(orders.get(id));
      rowState[collection].set(id, {
        data,
        order,
        sql: sqlRecord(collection, item, order, data)
      });
    }
  }

  return { settingsData, rowState, orderKeys };
}

function collectPersistenceChanges({ previousSettings, previousRows, nextSettings, nextRows, includeSettings = true, collections: targetCollections = collections }) {
  const deletes = [];
  const upserts = [];
  for (const collection of targetCollections) {
    const previous = previousRows[collection] || new Map();
    const next = nextRows[collection] || new Map();
    for (const id of previous.keys()) {
      if (!next.has(id)) deletes.push({ collection, id });
    }
    for (const [id, row] of next) {
      const old = previous.get(id);
      if (!old || old.data !== row.data || old.order !== row.order) {
        upserts.push({ collection, id, data: row.data, order: row.order, sql: row.sql });
      }
    }
  }
  return {
    settingsChanged: includeSettings && previousSettings !== nextSettings,
    deletes,
    upserts
  };
}

function shouldPersistUpdate(result, options = {}) {
  if (options.persist === false) return false;
  if (typeof options.shouldPersist === 'function') return Boolean(options.shouldPersist(result));
  return true;
}

function shouldDeferPersist(scope, options = {}) {
  if (!sqliteBatchWritesEnabled()) return false;
  if (options.immediate === true || options.defer === false || options.generationRequest || options.generationIdempotency || options.generationPayloadClaim) return false;
  return Boolean(scope?.hasDirtyRows && !scope.full);
}

function resolvePersistenceOptions(options = {}, result, db) {
  const resolved = { ...options };
  for (const key of ['dirtyRows', 'rows', 'changedRows', 'generationRequest', 'generationIdempotency']) {
    if (typeof resolved[key] === 'function') resolved[key] = resolved[key](result, db);
  }
  return resolved;
}

function mergePersistenceScopes(current, next) {
  if (!current) return clonePersistenceScope(next);
  if (!next) return clonePersistenceScope(current);
  if (current.full || next.full) {
    return { full: true, includeSettings: true, collections, dirtyRows: {}, hasDirtyRows: false };
  }

  const targetCollections = new Set([...(current.collections || []), ...(next.collections || [])]);
  const fullCollections = new Set();
  const dirtyRows = {};

  for (const scope of [current, next]) {
    for (const collection of scope.collections || []) {
      const ids = scope.dirtyRows?.[collection];
      if (!ids || !ids.size) {
        fullCollections.add(collection);
        delete dirtyRows[collection];
        continue;
      }
      if (fullCollections.has(collection)) continue;
      if (!dirtyRows[collection]) dirtyRows[collection] = new Set();
      for (const id of ids) dirtyRows[collection].add(id);
    }
  }

  return {
    full: false,
    includeSettings: Boolean(current.includeSettings || next.includeSettings),
    collections: [...targetCollections].filter((collection) => collections.includes(collection)),
    dirtyRows,
    hasDirtyRows: Object.keys(dirtyRows).length > 0
  };
}

function clonePersistenceScope(scope) {
  if (!scope) return null;
  if (scope.full) return { full: true, includeSettings: true, collections, dirtyRows: {}, hasDirtyRows: false };
  const dirtyRows = {};
  for (const [collection, ids] of Object.entries(scope.dirtyRows || {})) {
    dirtyRows[collection] = new Set(ids || []);
  }
  return {
    full: false,
    includeSettings: Boolean(scope.includeSettings),
    collections: normalizeCollectionList(scope.collections) || [],
    dirtyRows,
    hasDirtyRows: Object.keys(dirtyRows).length > 0
  };
}

function persistenceScope(options = {}) {
  if (options.full === true) return { full: true, includeSettings: true, collections, dirtyRows: {}, hasDirtyRows: false, generationRequest: options.generationRequest, generationIdempotency: options.generationIdempotency };
  const dirtyRows = normalizeDirtyRows(options.dirtyRows ?? options.rows ?? options.changedRows);
  const dirtyCollections = Object.keys(dirtyRows);
  const raw = options.collections ?? options.changedCollections ?? (dirtyCollections.length ? dirtyCollections : undefined);
  if (raw === undefined || raw === null) return { full: true, includeSettings: true, collections, dirtyRows: {}, hasDirtyRows: false, generationRequest: options.generationRequest, generationIdempotency: options.generationIdempotency };
  const values = Array.isArray(raw) ? raw : [raw];
  const includeSettings = values.includes('settings') || options.includeSettings === true;
  const targetCollections = normalizeCollectionList([...values, ...dirtyCollections]);
  return {
    full: false,
    includeSettings,
    collections: targetCollections || [],
    dirtyRows,
    hasDirtyRows: dirtyCollections.length > 0,
    generationRequest: options.generationRequest, generationIdempotency: options.generationIdempotency
  };
}

function normalizeCollectionList(value) {
  if (value === undefined || value === null) return null;
  const values = Array.isArray(value) ? value : [value];
  const selected = [];
  const seen = new Set();
  for (const item of values) {
    const collection = String(item || '');
    if (!collections.includes(collection) || seen.has(collection)) continue;
    seen.add(collection);
    selected.push(collection);
  }
  return selected.length ? selected : null;
}

function normalizeDirtyRows(value) {
  const dirtyRows = {};
  if (!value) return dirtyRows;
  const entries = value instanceof Map ? value.entries() : Object.entries(value);
  for (const [rawCollection, rawIds] of entries) {
    const collection = String(rawCollection || '');
    if (!collections.includes(collection)) continue;
    const ids = rawIds instanceof Set
      ? [...rawIds]
      : Array.isArray(rawIds)
        ? rawIds
        : [rawIds];
    const selected = ids
      .map((id) => String(id || '').trim())
      .filter(Boolean);
    if (!selected.length) continue;
    dirtyRows[collection] = new Set([...(dirtyRows[collection] || []), ...selected]);
  }
  return dirtyRows;
}

function cloneCollectionMaps(source = {}) {
  const maps = emptyCollectionMaps();
  for (const collection of collections) {
    maps[collection] = new Map(source[collection] || []);
  }
  return maps;
}

function sqlRecord(collection, item, order, data) {
  const createdAt = item.createdAt || '';
  const updatedAt = item.updatedAt || '';
  const row = {
    ...emptyRecordRows[collection],
    id: String(item.id), order_value: Number(order), data,
    created_at: createdAt || null, updated_at: updatedAt || null, at: item.at || null,
    enabled: item.enabled === undefined ? null : item.enabled !== false ? 1 : 0,
    balance: numberOrNull(item.balance), route_id: numberOrNull(item.routeId),
    width: numberOrNull(item.width), height: numberOrNull(item.height),
    mock: item.mock === undefined ? null : item.mock ? 1 : 0,
    mime_type: item.mimeType || null,
    model: item.model || item.request?.model || null,
    prompt: item.prompt || item.request?.tag || null,
    full_prompt: item.fullPrompt || item.request?.prompt || null
  };

  if (collection === 'cards') {
    row.code = item.code || null;
    row.token = item.token || null;
  }
  if (collection === 'users') {
    row.token = item.token || null;
  }
  if (collection === 'accounts') {
    row.token = item.token || null;
  }
  if (collection === 'jobs') {
    row.request_id = item.requestId || null;
    row.queue_seq = item.queueSeq ?? null;
    row.payload_bytes = item.requestId ? 0 : Number(item.payloadBytes || 0);
    row.user_token = item.userToken || null;
    row.account_id = item.accountId || null;
    row.status = item.status || null;
    row.source = item.source || 'web';
    row.cache_key = item.cacheKey || null;
    row.image_id = item.imageId || null;
    row.prompt = item.request?.tag || item.prompt || null;
    row.full_prompt = item.request?.prompt || item.fullPrompt || null;
    row.model = item.request?.model || item.model || null;
    row.cost = Math.max(0, numberOrNull(item.cost) || 0);
    row.stats_excluded = isStatsExcludedJob(item) ? 1 : 0;
    row.error_loggable = isErrorLoggableJob(item) ? 1 : 0;
  }
  if (collection === 'images') {
    row.token = item.token || null;
    row.account_id = item.accountId || null;
    row.cache_key = item.cacheKey || null;
    row.resolution_tier = imageResolutionTier(item).toLowerCase();
  }
  if (collection === 'ledger') {
    row.token = item.token || null;
    row.user_token = item.token || null;
    row.account_id = item.accountId || null;
    row.status = item.type || null;
    row.source = item.type || null;
    row.image_id = item.imageId || null;
  }

  row.search_text = searchableText(item, row);
  return row;
}

function searchableText(item, row) {
  return [
    item.id,
    row.token,
    row.user_token,
    row.account_id,
    row.status,
    row.source,
    row.code,
    row.cache_key,
    row.image_id,
    row.model,
    row.prompt,
    row.full_prompt,
    item.name,
    item.note,
    item.file
  ].filter(Boolean).join('\n').toLowerCase();
}

function numberOrNull(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function isStatsExcludedJob(job = {}) {
  if (job.status !== 'failed') return false;
  return /insufficient balance|额度不足|余额不足/i.test(`${job.error || ''}\n${job.errorDetail || ''}`);
}

function isErrorLoggableJob(job = {}) {
  if (job.status !== 'failed' || !job.accountId) return false;
  const text = `${job.error || ''}\n${job.errorDetail || ''}`;
  return !/all NovelAI accounts are busy|server busy|NovelAI returned 429|statusCode"\s*:\s*429|Concurrent generation is locked|concurrent generation|direct generate timeout|AbortError|operation was aborted|job deadline expired|stale timeout|queued job exceeded|running job exceeded|invalid user token|invalid STA1N|密钥额度不足|用户额度不足/i.test(text);
}

function ensureItemIds(collection, items) {
  const prefix = collection === 'accounts' ? 'acct' : collection.slice(0, 4);
  items.forEach((item) => {
    if (item && !item.id) item.id = createId(prefix);
  });
}

function denseOrderValues(items) {
  const orders = new Map();
  const total = items.length;
  items.forEach((item, index) => {
    if (item?.id) orders.set(String(item.id), total - index);
  });
  return orders;
}

function assignOrderValues(items, previousOrders) {
  if (!items.length) return new Map();
  if (!existingOrderSequenceIsStable(items, previousOrders)) return denseOrderValues(items);

  const orders = new Map();
  let index = 0;
  while (index < items.length) {
    const id = String(items[index]?.id || '');
    if (previousOrders.has(id)) {
      orders.set(id, previousOrders.get(id));
      index += 1;
      continue;
    }

    const runStart = index;
    while (index < items.length && !previousOrders.has(String(items[index]?.id || ''))) index += 1;
    const runLength = index - runStart;
    const previousItem = runStart > 0 ? items[runStart - 1] : null;
    const nextItem = index < items.length ? items[index] : null;
    const before = previousItem ? orders.get(String(previousItem.id)) : null;
    const after = nextItem ? previousOrders.get(String(nextItem.id)) : null;
    const values = orderValuesForRun(runLength, before, after);
    for (let offset = 0; offset < runLength; offset += 1) {
      orders.set(String(items[runStart + offset].id), values[offset]);
    }
  }
  return orders;
}

function dirtyRowOrder(items, index, previousOrders) {
  const id = String(items[index]?.id || '');
  if (previousOrders.has(id)) return Number(previousOrders.get(id));

  let before = null;
  for (let cursor = index - 1; cursor >= 0; cursor -= 1) {
    const previousId = String(items[cursor]?.id || '');
    if (!previousOrders.has(previousId)) continue;
    before = Number(previousOrders.get(previousId));
    break;
  }

  let after = null;
  for (let cursor = index + 1; cursor < items.length; cursor += 1) {
    const nextId = String(items[cursor]?.id || '');
    if (!previousOrders.has(nextId)) continue;
    after = Number(previousOrders.get(nextId));
    break;
  }

  return orderValuesForRun(1, before, after)[0];
}

function dirtyItemIndexes(items, dirtyIds) {
  const indexes = new Map();
  let remaining = dirtyIds.size;
  for (let index = 0; index < items.length && remaining > 0; index += 1) {
    const id = String(items[index]?.id || '');
    if (!dirtyIds.has(id) || indexes.has(id)) continue;
    indexes.set(id, index);
    remaining -= 1;
  }
  return indexes;
}

function chunks(values, size) {
  const selected = [];
  for (let index = 0; index < values.length; index += size) {
    selected.push(values.slice(index, index + size));
  }
  return selected;
}

function uniqueStrings(values = []) {
  return [...new Set(values.map((value) => String(value || '').trim()).filter(Boolean))];
}

function trimImageCacheDeleteLimit(value) {
  const number = Number(value ?? 25);
  if (!Number.isFinite(number) || number <= 0) return 25;
  return Math.max(1, Math.floor(number));
}

class SqliteReadWorker {
  constructor(dbPath, name) {
    this.dbPath = dbPath;
    this.name = name;
    this.worker = null;
    this.sequence = 0;
    this.pending = new Map();
    this.closed = false;
  }

  request(type, payload, timeoutMs) {
    if (this.closed) return Promise.reject(new Error(`${this.name} reader is closed`));
    this.open();
    const id = ++this.sequence;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.fail(new Error(`${this.name} reader timed out`));
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      this.worker.postMessage({ id, type, payload });
    });
  }

  open() {
    if (this.worker) return;
    const worker = new Worker(new URL('./sqlite-read-worker.js', import.meta.url), {
      workerData: { dbPath: this.dbPath },
      execArgv: process.execArgv.filter((value) => !value.startsWith('--input-type'))
    });
    worker.unref();
    worker.on('message', (message) => {
      const request = this.pending.get(message.id);
      if (!request) return;
      clearTimeout(request.timer);
      this.pending.delete(message.id);
      if (message.error) request.reject(new Error(message.error));
      else request.resolve(message.result);
    });
    worker.on('error', (error) => this.fail(error));
    worker.on('exit', (code) => {
      if (this.worker !== worker) return;
      this.worker = null;
      if (code && !this.closed) this.fail(new Error(`${this.name} reader exited with code ${code}`));
    });
    this.worker = worker;
  }

  fail(error) {
    for (const request of this.pending.values()) {
      clearTimeout(request.timer);
      request.reject(error);
    }
    this.pending.clear();
    this.reset();
  }

  reset() {
    const worker = this.worker;
    this.worker = null;
    worker?.terminate().catch(() => {});
  }

  close() {
    this.closed = true;
    const error = new Error(`${this.name} reader closed`);
    for (const request of this.pending.values()) {
      clearTimeout(request.timer);
      request.reject(error);
    }
    this.pending.clear();
    const worker = this.worker;
    this.worker = null;
    return worker?.terminate().catch(() => {});
  }
}

function emptyAdminStats(now = Date.now()) {
  const buckets = usageBuckets(now, usageChartDays);
  return {
    requestStats1m: { total: 0 },
    generationStats1m: { total: 0 },
    jobStats1h: { done: 0, failed: 0, total: 0, successRate: 0 },
    generationSpeed1h: {
      v45: { seconds: null, count: 0 },
      v5: { seconds: null, count: 0 }
    },
    accountStats1h: {},
    usageHourlyDays: buckets.keys.map((key) => buckets.map.get(key)),
    statsRowsRead: 0
  };
}

function usageBuckets(now = Date.now(), days = usageChartDays) {
  const current = new Date(now + beijingOffsetHours * 60 * 60 * 1000);
  const midnight = Date.UTC(current.getUTCFullYear(), current.getUTCMonth(), current.getUTCDate());
  const keys = Array.from({ length: days }, (_, index) => {
    const dayOffset = index - days + 1;
    return new Date(midnight + dayOffset * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  });
  return {
    keys,
    map: new Map(keys.map((key) => [key, {
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
        successRate: 0
      }))
    }]))
  };
}

function existingOrderSequenceIsStable(items, previousOrders) {
  let last = Number.POSITIVE_INFINITY;
  for (const item of items) {
    const id = String(item?.id || '');
    if (!previousOrders.has(id)) continue;
    const value = Number(previousOrders.get(id));
    if (!Number.isFinite(value) || value >= last) return false;
    last = value;
  }
  return true;
}

function orderValuesForRun(length, before, after) {
  if (before === null && after === null) {
    return Array.from({ length }, (_, index) => length - index);
  }
  if (before === null) {
    return Array.from({ length }, (_, index) => Number(after) + length - index);
  }
  if (after === null) {
    return Array.from({ length }, (_, index) => Number(before) - index - 1);
  }
  const gap = Number(before) - Number(after);
  if (!Number.isFinite(gap) || gap <= 0) {
    return Array.from({ length }, (_, index) => Number(before) - index - 1);
  }
  const step = gap / (length + 1);
  return Array.from({ length }, (_, index) => Number(before) - step * (index + 1));
}

function emptyCollectionMaps() {
  return Object.fromEntries(collections.map((collection) => [collection, new Map()]));
}

function safeJson(text, fallback) {
  try {
    return JSON.parse(text);
  } catch {
    return fallback;
  }
}

function runtimeLog(message) {
  console.log(`[runtime] ${message}`);
}

function sqliteSlowLogMs() {
  const configured = Number(process.env.SQLITE_SLOW_LOG_MS || 500);
  if (Number.isFinite(configured) && configured >= 0) return Math.floor(configured);
  return 500;
}

function sqliteBatchWritesEnabled() {
  return process.env.SQLITE_BATCH_WRITES !== 'false';
}

function sqliteWriteDebounceMs() {
  const configured = Number(process.env.SQLITE_WRITE_DEBOUNCE_MS || 500);
  if (Number.isFinite(configured) && configured >= 0) return Math.floor(configured);
  return 500;
}

function runtimeCacheLimit(collection) {
  const defaults = {
    jobs: 3000,
    images: 1000,
    ledger: 1000
  };
  const envName = `RUNTIME_${collection.toUpperCase()}_CACHE_LIMIT`;
  const configured = Number(process.env[envName] || defaults[collection] || 1000);
  if (Number.isFinite(configured) && configured >= 0) return Math.floor(configured);
  return defaults[collection] || 1000;
}

function formatDbCounts(db = {}) {
  const count = (value) => Array.isArray(value) ? value.length : Number(value || 0);
  return [
    `users=${count(db.users)}`,
    `accounts=${count(db.accounts)}`,
    `jobs=${count(db.jobs)}`,
    `images=${count(db.images)}`,
    `cards=${count(db.cards)}`,
    `ledger=${count(db.ledger)}`
  ].join(' ');
}

function formatFileSize(filePath) {
  try {
    const bytes = statSync(filePath).size;
    if (bytes >= 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MiB`;
    if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
    return `${bytes} B`;
  } catch {
    return 'unknown size';
  }
}

function imageResolutionTier(image) {
  const width = Number(image?.width || 0);
  const height = Number(image?.height || 0);
  if (width >= 1700 || height >= 1900) return '4K';
  if (width >= 1300 || height >= 1500) return '2K';
  return 'standard';
}

function jobQueueProgress(job, jobs) {
  if (!job || !['queued', 'running'].includes(job.status)) return { progress: 0, total: 0 };
  if (job.status === 'running') {
    const total = Math.max(1, Number(job.queueTotal) || 1);
    return { progress: total, total };
  }
  const now = Date.now();
  let ahead = 0;
  const ordered = [...jobs].reverse().sort((a, b) => (Number(a.queueSeq) || 0) - (Number(b.queueSeq) || 0));
  for (const item of ordered) {
    if (item.id === job.id) break;
    if (isQueueActiveJob(item, now)) ahead++;
  }
  const total = Math.max(1, Number(job.queueTotal) || ahead + 1);
  // A free queue position is not a reserved account; only running may reach N/N.
  return { progress: Math.max(1, Math.min(Math.max(1, total - 1), total - ahead)), total };
}

function isQueueActiveJob(job, now = Date.now()) {
  if (!job || !['queued', 'running'].includes(job.status)) return false;
  return !isStaleActiveJob(job, now);
}

function isStaleActiveJob(job, now = Date.now()) {
  if (!job || !['queued', 'pending', 'running'].includes(job.status)) return false;
  if (isExpiredJob(job, now)) return true;
  const queueDeadline = queuedDeadlineTimestamp(job);
  if (queueDeadline && now >= queueDeadline) return true;
  const updatedAt = Date.parse(job.updatedAt || job.createdAt || '');
  if (!Number.isFinite(updatedAt) || updatedAt <= 0) return false;
  if (job.status === 'running') return now - updatedAt > staleRunningJobMs();
  if (job.status === 'queued' && !jobDeadlineTimestamp(job)) return now - updatedAt > staleQueuedJobMs();
  return false;
}

function isExpiredJob(job, now = Date.now()) {
  const deadline = jobDeadlineTimestamp(job);
  return deadline > 0 && now >= deadline;
}

function jobDeadlineTimestamp(job = {}) {
  const deadline = Date.parse(job.deadlineAt || '');
  return Number.isFinite(deadline) && deadline > 0 ? deadline : 0;
}

function staleQueuedJobMs() {
  return configuredTimeoutMs('STALE_QUEUED_JOB_MS', configuredTimeoutMs('STALE_ACTIVE_JOB_MS', 30 * 60 * 1000));
}

function staleRunningJobMs() {
  return configuredTimeoutMs('STALE_RUNNING_JOB_MS', accountInflightTimeoutMs() + 60 * 1000);
}

function accountInflightTimeoutMs() {
  const configured = Number(process.env.ACCOUNT_INFLIGHT_TIMEOUT_MS || 10 * 60 * 1000);
  return Number.isFinite(configured) && configured > 0 ? Math.max(1000, Math.floor(configured)) : 10 * 60 * 1000;
}

function configuredTimeoutMs(name, fallback) {
  const configured = Number(process.env[name] || 0);
  if (Number.isFinite(configured) && configured > 0) return Math.max(60_000, Math.floor(configured));
  return Math.max(60_000, Math.floor(Number(fallback) || 60_000));
}

function cloneDb(db) {
  return normalizeDb(structuredClone(db));
}

function cloneValue(value) {
  if (value === undefined || value === null) return value;
  return structuredClone(value);
}

function trimDb(db, options = {}) {
  const hasCollectionOption = options.collections !== undefined && options.collections !== null;
  const targetCollections = hasCollectionOption ? normalizeCollectionList(options.collections) || [] : null;
  const shouldTrim = (collection) => !hasCollectionOption || targetCollections.includes(collection);
  const maxCacheImages = clampNumber(db.settings.maxCacheImages, 0, MAX_CACHE_IMAGES_LIMIT);
  db.settings.costPerImage = 1;
  db.settings.maxCacheImages = maxCacheImages;
  db.settings.accountConcurrency = 1;
    // 私有部署保护：保留历史、图片与账本记录。
    if (process.env.NO_AUTO_DELETE === 'true') return db;
  if (shouldTrim('jobs')) db.jobs = trimJobs(db.jobs);
  if (shouldTrim('images')) db.images = db.images.slice(0, maxCacheImages);
  if (shouldTrim('ledger')) db.ledger = db.ledger.slice(0, 1000);
  return db;
}

function trimJobs(jobs) {
  const now = Date.now();
  const retainMs = clampNumber(process.env.JOB_HISTORY_RETENTION_MS ?? 7 * 24 * 60 * 60 * 1000, 60_000, 7 * 24 * 60 * 60 * 1000);
  return jobs.filter((job) => {
    if (['queued', 'pending', 'running'].includes(job.status)) return true;
    const updatedAt = Date.parse(job.updatedAt || job.createdAt || '');
    if (!updatedAt || now - updatedAt > retainMs) return false;
    return true;
  });
}

export function normalizeDb(db = {}) {
  return {
    settings: {
      ...defaultSettings,
      ...(db.settings || {}),
      accountConcurrency: 1,
      promptApi: {
        ...defaultSettings.promptApi,
        ...(db.settings?.promptApi || {})
      },
      defaults: {
        ...defaultSettings.defaults,
        ...(db.settings?.defaults || {})
      }
    },
    cards: Array.isArray(db.cards) ? db.cards : [],
    users: Array.isArray(db.users) ? db.users : [],
    accounts: Array.isArray(db.accounts) ? db.accounts : [],
    jobs: Array.isArray(db.jobs) ? db.jobs : [],
    images: Array.isArray(db.images) ? db.images : [],
    ledger: Array.isArray(db.ledger) ? db.ledger : []
  };
}

export function createId(prefix = 'item') {
  const random = crypto.randomBytes(5).toString('hex');
  return `${prefix}_${Date.now().toString(36)}_${random}`;
}

export function createPublicToken(prefix = 'STD') {
  return `${prefix}-${crypto.randomBytes(18).toString('base64url')}`;
}

export function hashObject(value) {
  return crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
}

export function maskToken(token = '') {
  if (token.length <= 12) return token ? '******' : '';
  return `${token.slice(0, 6)}...${token.slice(-4)}`;
}

function clampNumber(value, min, max) {
  const number = Number(value);
  if (!Number.isFinite(number)) return max;
  return Math.max(min, Math.min(max, Math.floor(number)));
}
