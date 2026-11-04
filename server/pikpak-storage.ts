import { createHash } from 'node:crypto';
import { promisify } from 'node:util';
import { brotliCompress, brotliDecompress, constants } from 'node:zlib';
import { createClient, type WebDAVClient } from 'webdav';
import { MAX_STORED_IMAGE_BYTES, type R2Image } from './r2-storage.ts';

const compress = promisify(brotliCompress), decompress = promisify(brotliDecompress);
const maxArchiveBytes = MAX_STORED_IMAGE_BYTES + 1024 * 1024;

export interface PikPakConfig { url: string; username: string; password: string }

export function pikpakConfig(env: NodeJS.ProcessEnv = process.env): PikPakConfig | null {
  const { PIKPAK_WEBDAV_URL: address, PIKPAK_WEBDAV_USERNAME: username, PIKPAK_WEBDAV_PASSWORD: password } = env;
  if (!address && !username && !password) return null;
  let url: URL;
  try { url = new URL(address!); } catch { throw new Error('PikPak WebDAV configuration is incomplete or invalid'); }
  if (!username || !password || username.includes(':') || url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) {
    throw new Error('PikPak WebDAV configuration is incomplete or invalid');
  }
  if (!url.pathname.endsWith('/')) url.pathname += '/';
  return { url: url.href, username, password };
}

export class PikPakStorage {
  readonly config: PikPakConfig;
  readonly client: WebDAVClient;
  constructor(config: PikPakConfig) {
    this.config = config;
    this.client = createClient(config.url, { username: config.username, password: config.password });
  }

  private filename(image: R2Image) {
    if (!/^images\/[A-Za-z0-9_-]{1,128}\/[a-f0-9]{64}$/.test(image.key) || !/^[a-f0-9]{64}$/.test(image.sha256)
      || !image.key.endsWith('/' + image.sha256) || !Number.isSafeInteger(image.byteLength)
      || image.byteLength < 1 || image.byteLength > MAX_STORED_IMAGE_BYTES
      || image.pikpak && image.pikpak.url !== this.config.url) throw new Error('Invalid PikPak image descriptor or archive directory');
    return `${image.sha256}.br`;
  }

  async put(image: R2Image, buffer: Buffer): Promise<number> {
    const filename = this.filename(image);
    if (buffer.length !== image.byteLength || createHash('sha256').update(buffer).digest('hex') !== image.sha256) {
      throw new Error('PikPak upload source checksum mismatch');
    }
    const compressed = await compress(buffer, { params: { [constants.BROTLI_PARAM_QUALITY]: 11 }, maxOutputLength: maxArchiveBytes });
    // PikPak ignores conditional PUT; hash-named files make retries write the same original content.
    const uploaded = await this.client.putFileContents(filename, compressed, { signal: AbortSignal.timeout(30000) });
    if (!uploaded) throw new Error('PikPak upload was not confirmed');
    return compressed.length;
  }

  async get(image: R2Image): Promise<Buffer> {
    const filename = this.filename(image);
    const response = await this.client.customRequest(filename, { method: 'GET', signal: AbortSignal.timeout(30000) });
    const body = (response as typeof response & { body: AsyncIterable<Uint8Array> }).body;
    if (!body) throw new Error('PikPak object body is missing');
    const chunks: Uint8Array[] = []; let length = 0;
    for await (const chunk of body) {
      length += chunk.length;
      if (length > maxArchiveBytes) throw new Error('PikPak archive exceeds maximum length');
      chunks.push(chunk);
    }
    const buffer = await decompress(Buffer.concat(chunks), { maxOutputLength: image.byteLength });
    if (buffer.length !== image.byteLength || createHash('sha256').update(buffer).digest('hex') !== image.sha256) {
      throw new Error('PikPak object checksum mismatch');
    }
    return buffer;
  }
}
