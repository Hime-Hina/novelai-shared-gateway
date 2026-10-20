import { createHash, createHmac } from 'node:crypto';
import { open } from 'node:fs/promises';
import type { FileHandle } from 'node:fs/promises';
import path from 'node:path';

export const R2_IMAGE_LIMIT_BYTES = 9_500_000_000;
export const MAX_STORED_IMAGE_BYTES = 64 * 1024 * 1024;
export interface R2Object {
  bucket: string; key: string; byteLength: number; sha256: string;
}
export interface R2Image extends R2Object {
  verifiedAt?: string; localRemovedAt?: string;
}
export interface StoredImage {
  id?: string; file?: string; base64?: string; mimeType?: string;
  byteLength?: number; sha256?: string; r2?: R2Image;
}
export interface R2Config { accountId: string; bucket: string; accessKeyId: string; secretAccessKey: string }
const hash = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');
const hmac = (key: string | Buffer, value: string) => createHmac('sha256', key).update(value).digest();
const encode = (value: string) => encodeURIComponent(value).replace(/[!'()*]/g, char => '%' + char.charCodeAt(0).toString(16).toUpperCase());

export function r2Config(env: NodeJS.ProcessEnv = process.env): R2Config | null {
  const fields = [env.R2_ACCOUNT_ID, env.R2_BUCKET, env.R2_ACCESS_KEY_ID, env.R2_SECRET_ACCESS_KEY];
  if (fields.every(value => !value)) return null;
  if (fields.some(value => !value) || !/^[a-f0-9]{32}$/.test(fields[0]!)
    || !/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/.test(fields[1]!)) throw new Error('R2 configuration is incomplete or invalid');
  return { accountId: fields[0]!, bucket: fields[1]!, accessKeyId: fields[2]!, secretAccessKey: fields[3]! };
}

// Redirects and automatic retries are disabled; callers reconcile uncertain writes by checksum.
export function signedR2Request(config: R2Config, method: 'GET' | 'PUT' | 'DELETE', key: string, body?: Buffer, mimeType?: string, now = new Date(), query: Record<string, string> = {}) {
  const url = new URL(`https://${config.accountId}.r2.cloudflarestorage.com/${encode(config.bucket)}/${key.split('/').map(encode).join('/')}`);
  const timestamp = now.toISOString().replace(/[:-]|\.\d{3}/g, ''), date = timestamp.slice(0, 8);
  const scope = `${date}/auto/s3/aws4_request`, payloadHash = hash(body ?? '');
  const headers: Record<string, string> = { host: url.host, 'x-amz-date': timestamp, 'x-amz-content-sha256': payloadHash };
  if (body) Object.assign(headers, { 'content-type': mimeType || 'application/octet-stream',
    'content-md5': createHash('md5').update(body).digest('base64'), 'if-none-match': '*' });
  const names = Object.keys(headers).sort(), signedHeaders = names.join(';');
  const canonicalQuery = Object.keys(query).sort().map(name => `${encode(name)}=${encode(query[name]!)}`).join('&');
  url.search = canonicalQuery;
  const canonical = [method, url.pathname, canonicalQuery, names.map(name => `${name}:${headers[name]!.trim()}\n`).join(''), signedHeaders, payloadHash].join('\n');
  const signingKey = hmac(hmac(hmac(hmac('AWS4' + config.secretAccessKey, date), 'auto'), 's3'), 'aws4_request');
  const signature = hmac(signingKey, ['AWS4-HMAC-SHA256', timestamp, scope, hash(canonical)].join('\n')).toString('hex');
  headers.authorization = `AWS4-HMAC-SHA256 Credential=${config.accessKeyId}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;
  return { url, headers };
}

export class R2Storage {
  readonly config: R2Config;
  constructor(config: R2Config) { this.config = config; }

  private async request(method: 'GET' | 'PUT' | 'DELETE', key: string, body?: Buffer, mimeType?: string, query?: Record<string, string>) {
    const signed = signedR2Request(this.config, method, key, body, mimeType, new Date(), query);
    return fetch(signed.url, { method, headers: signed.headers, body: body as BodyInit | undefined,
      redirect: 'error', signal: AbortSignal.timeout(30000) });
  }

  async put(image: R2Object, buffer: Buffer, mimeType: string) {
    this.check(image);
    if (buffer.length !== image.byteLength || hash(buffer) !== image.sha256) throw new Error('R2 upload source checksum mismatch');
    const response = await this.request('PUT', image.key, buffer, mimeType);
    await response.body?.cancel();
    // A timed-out PUT may have committed; never overwrite it, verify its bytes with GET instead.
    if (!response.ok && response.status !== 412) throw new Error(`R2 upload failed (HTTP ${response.status})`);
  }

  async get(image: R2Object): Promise<Buffer> {
    this.check(image);
    const response = await this.request('GET', image.key);
    if (!response.ok) { await response.body?.cancel(); throw new Error(`R2 read failed (HTTP ${response.status})`); }
    if (response.headers.has('content-length') && Number(response.headers.get('content-length')) !== image.byteLength) {
      await response.body?.cancel(); throw new Error('R2 object length mismatch');
    }
    const reader = response.body?.getReader();
    if (!reader) throw new Error('R2 object body is missing');
    const chunks: Uint8Array[] = []; let length = 0;
    try {
      while (true) {
        const result = await reader.read();
        if (result.done) break;
        length += result.value.length;
        if (length > image.byteLength) throw new Error('R2 object exceeds recorded length');
        chunks.push(result.value);
      }
    } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
    const buffer = Buffer.concat(chunks);
    if (buffer.length !== image.byteLength || hash(buffer) !== image.sha256) throw new Error('R2 object checksum mismatch');
    return buffer;
  }

  async listBackups() {
    const response = await this.request('GET', '', undefined, undefined, { 'list-type': '2', prefix: 'backups/', 'max-keys': '1000' });
    if (!response.ok) { await response.body?.cancel(); throw new Error(`R2 backup listing failed (HTTP ${response.status})`); }
    const xml = await response.text();
    if (!xml.includes('<ListBucketResult') || !xml.includes('</ListBucketResult>')
      || !/<IsTruncated>false<\/IsTruncated>/.test(xml)) throw new Error('R2 backup listing is incomplete');
    return [...xml.matchAll(/<Contents>([\s\S]*?)<\/Contents>/g)].map(match => {
      const key = /<Key>([^<]+)<\/Key>/.exec(match[1]!)?.[1];
      const bytes = Number(/<Size>(\d+)<\/Size>/.exec(match[1]!)?.[1]);
      if (!key?.startsWith('backups/') || !Number.isSafeInteger(bytes) || bytes < 0) throw new Error('Invalid R2 backup listing');
      return { key, bytes };
    });
  }

  async removeBackup(image: R2Object) {
    this.check(image);
    if (!image.key.startsWith('backups/daily-')) throw new Error('Only daily backup retention may delete R2 objects');
    const response = await this.request('DELETE', image.key);
    await response.body?.cancel();
    if (!response.ok && response.status !== 404) throw new Error(`R2 backup retention failed (HTTP ${response.status})`);
  }

  private check(image: R2Object) {
    if (image.bucket !== this.config.bucket || !/^(?:images\/[A-Za-z0-9_-]{1,128}|backups\/daily-[0-9TZ.]+)\/[a-f0-9]{64}$/.test(image.key)
      || !/^[a-f0-9]{64}$/.test(image.sha256) || !Number.isSafeInteger(image.byteLength)
      || image.byteLength < 1 || image.byteLength > MAX_STORED_IMAGE_BYTES) throw new Error('Invalid R2 object descriptor');
  }
}

export function storedImagePath(dataDir: string, file: string) {
  const directory = path.resolve(dataDir, 'images'), resolved = path.resolve(dataDir, file);
  if (!resolved.startsWith(directory + path.sep)) throw Object.assign(new Error('invalid image path'), { statusCode: 403 });
  return resolved;
}

export class ImageStorage {
  readonly dataDir: string;
  readonly remote: R2Storage | null;
  readonly findImage: (id: string) => Promise<StoredImage | null>;
  constructor(dataDir: string, remote: R2Storage | null, findImage: (id: string) => Promise<StoredImage | null>) {
    this.dataDir = dataDir; this.remote = remote; this.findImage = findImage;
  }

  async open(image: StoredImage): Promise<{ handle: FileHandle } | { image: StoredImage }> {
    if (image.file) {
      try { return { handle: await open(storedImagePath(this.dataDir, image.file), 'r') }; }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
    }
    // A reader may hold the pre-archive row while the verified local copy is removed.
    const current = image.id ? await this.findImage(image.id) : image;
    if (current?.r2?.verifiedAt || current?.base64) return { image: current };
    throw Object.assign(new Error('image content not found'), { statusCode: 404 });
  }

  async read(image: StoredImage): Promise<Buffer> {
    const source = await this.open(image);
    if ('handle' in source) {
      try { return await source.handle.readFile(); } finally { await source.handle.close(); }
    }
    if (source.image.r2?.verifiedAt) {
      if (!this.remote) throw Object.assign(new Error('R2 storage is not configured'), { statusCode: 503 });
      if (!source.image.r2.key.startsWith('images/')) throw Object.assign(new Error('invalid image object'), { statusCode: 403 });
      return this.remote.get(source.image.r2).catch(() => { throw Object.assign(new Error('历史图片暂时无法从 R2 读取，请稍后重试'), { statusCode: 503 }); });
    }
    return Buffer.from(source.image.base64!, 'base64');
  }

  async available(image: StoredImage) {
    const source = await this.open(image);
    if ('handle' in source) {
      try { return (await source.handle.stat()).size > 0; } finally { await source.handle.close(); }
    }
    // Durable verification is enough for admission; downloading later verifies content without a HEAD per view.
    return Boolean(source.image.r2?.verifiedAt || source.image.base64);
  }
}
