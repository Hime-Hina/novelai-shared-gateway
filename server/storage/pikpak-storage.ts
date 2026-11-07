import { createHash } from 'node:crypto';
import { promisify } from 'node:util';
import { brotliCompress, brotliDecompress, constants } from 'node:zlib';
import { createClient, type WebDAVClient } from 'webdav';
import { MAX_STORED_IMAGE_BYTES, type R2Image } from './r2-storage.ts';

const compress = promisify(brotliCompress);
const decompress = promisify(brotliDecompress);
const maxArchiveBytes = MAX_STORED_IMAGE_BYTES + 1024 * 1024;

export interface PikPakConfig {
  url: string;
  username: string;
  password: string;
}

export function pikpakConfig(env: NodeJS.ProcessEnv = process.env): PikPakConfig | null {
  const {
    PIKPAK_WEBDAV_URL: address,
    PIKPAK_WEBDAV_USERNAME: username,
    PIKPAK_WEBDAV_PASSWORD: password,
  } = env;
  if (!address && !username && !password) {
    return null;
  }
  let url: URL;
  try {
    url = new URL(address!);
  } catch {
    throw new Error('PikPak WebDAV configuration is incomplete or invalid');
  }
  if (
    !username ||
    !password ||
    username.includes(':') ||
    url.protocol !== 'https:' ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  ) {
    throw new Error('PikPak WebDAV configuration is incomplete or invalid');
  }
  if (!url.pathname.endsWith('/')) {
    url.pathname += '/';
  }
  return { url: url.href, username, password };
}

type QuotaSnapshot = { usedBytes: number; totalBytes: number; checkedAt: string };
let quotaCache:
  | {
      token: string;
      nextRefreshAt: number;
      pending: boolean;
      failed: boolean;
      snapshot?: QuotaSnapshot;
    }
  | undefined;

function quotaBytes(value: unknown) {
  if (typeof value !== 'number' && (typeof value !== 'string' || !/^\d+$/.test(value))) {
    throw new Error('Invalid PikPak quota');
  }
  const bytes = Number(value);
  if (!Number.isSafeInteger(bytes) || bytes < 0) {
    throw new Error('Invalid PikPak quota');
  }
  return bytes;
}

export function readPikPakQuota(env: NodeJS.ProcessEnv = process.env) {
  const empty = { usedBytes: null, totalBytes: null, checkedAt: null };
  const token = env.PIKPAK_API_TOKEN;
  if (!token) {
    return { ...empty, status: 'unconfigured' as const };
  }
  if (!quotaCache || quotaCache.token !== token) {
    quotaCache = { token, nextRefreshAt: 0, pending: false, failed: false };
  }
  const entry = quotaCache;
  if (!entry.pending && Date.now() >= entry.nextRefreshAt) {
    entry.pending = true;
    void fetch('https://api-drive.mypikpak.com/drive/v1/about', {
      headers: { Authorization: `Bearer ${token}` },
      redirect: 'error',
      signal: AbortSignal.timeout(10_000),
    })
      .then(async (response) => {
        if (!response.ok) {
          await response.body?.cancel();
          throw new Error('PikPak quota unavailable');
        }
        const body = (await response.json()) as { quota?: { usage?: unknown; limit?: unknown } };
        entry.snapshot = {
          usedBytes: quotaBytes(body.quota?.usage),
          totalBytes: quotaBytes(body.quota?.limit),
          checkedAt: new Date().toISOString(),
        };
        entry.failed = false;
      })
      .catch(() => {
        entry.failed = true;
      })
      .finally(() => {
        entry.pending = false;
        entry.nextRefreshAt = Date.now() + 60_000;
      });
  }
  if (entry.snapshot) {
    return { ...entry.snapshot, status: entry.failed ? ('stale' as const) : ('ready' as const) };
  }
  return { ...empty, status: entry.failed ? ('unavailable' as const) : ('loading' as const) };
}

export class PikPakStorage {
  readonly config: PikPakConfig;
  readonly client: WebDAVClient;
  constructor(config: PikPakConfig) {
    this.config = config;
    this.client = createClient(config.url, {
      username: config.username,
      password: config.password,
    });
  }

  private filename(image: R2Image) {
    if (
      typeof image.key !== 'string' ||
      !/^images\/[A-Za-z0-9_-]{1,128}\/[a-f0-9]{64}$/.test(image.key) ||
      !/^[a-f0-9]{64}$/.test(image.sha256) ||
      !image.key.endsWith('/' + image.sha256) ||
      !Number.isSafeInteger(image.byteLength) ||
      image.byteLength < 1 ||
      image.byteLength > MAX_STORED_IMAGE_BYTES ||
      (image.pikpak && image.pikpak.url !== this.config.url)
    ) {
      throw new Error('Invalid PikPak image descriptor or archive directory');
    }
    return `${image.sha256}.br`;
  }

  async put(image: R2Image, buffer: Buffer): Promise<number> {
    const filename = this.filename(image);
    if (
      buffer.length !== image.byteLength ||
      createHash('sha256').update(buffer).digest('hex') !== image.sha256
    ) {
      throw new Error('PikPak upload source checksum mismatch');
    }
    const compressed = await compress(buffer, {
      params: { [constants.BROTLI_PARAM_QUALITY]: 11 },
      maxOutputLength: maxArchiveBytes,
    });
    // PikPak ignores conditional PUT; hash-named files make retries write the same original content.
    const uploaded = await this.client.putFileContents(filename, compressed, {
      signal: AbortSignal.timeout(30000),
    });
    if (!uploaded) {
      throw new Error('PikPak upload was not confirmed');
    }
    return compressed.length;
  }

  async get(image: R2Image): Promise<Buffer> {
    const filename = this.filename(image);
    const response = await this.client.customRequest(filename, {
      method: 'GET',
      signal: AbortSignal.timeout(30000),
    });
    const body = (response as typeof response & { body: AsyncIterable<Uint8Array> }).body;
    if (!body) {
      throw new Error('PikPak object body is missing');
    }
    const chunks: Uint8Array[] = [];
    let length = 0;
    for await (const chunk of body) {
      length += chunk.length;
      if (length > maxArchiveBytes) {
        throw new Error('PikPak archive exceeds maximum length');
      }
      chunks.push(chunk);
    }
    const buffer = await decompress(Buffer.concat(chunks), { maxOutputLength: image.byteLength });
    if (
      buffer.length !== image.byteLength ||
      createHash('sha256').update(buffer).digest('hex') !== image.sha256
    ) {
      throw new Error('PikPak object checksum mismatch');
    }
    return buffer;
  }
}
