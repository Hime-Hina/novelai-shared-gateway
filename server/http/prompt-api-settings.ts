interface PromptApiInput {
  baseUrl?: unknown;
  apiKey?: unknown;
  model?: unknown;
}
interface PromptApiConfig {
  baseUrl: string;
  apiKey: string;
  model: string;
}
function normalizePromptApiConfig(value: PromptApiInput = {}): PromptApiConfig {
  return {
    baseUrl: normalizePromptApiBaseUrl(value.baseUrl || ''),
    apiKey: String(value.apiKey || '').trim(),
    model: String(value.model || '').trim(),
  };
}

export function adminPromptApiConfig(value: PromptApiInput = {}) {
  const config = normalizePromptApiConfig(value);
  return {
    baseUrl: config.baseUrl,
    model: config.model,
    configured: isPromptApiConfigured(config),
    apiKeyConfigured: Boolean(config.apiKey),
  };
}

function isPromptApiConfigured(value: PromptApiInput = {}): boolean {
  const config = normalizePromptApiConfig(value);
  return Boolean(config.baseUrl && config.apiKey && config.model);
}

function normalizePromptApiBaseUrl(value: unknown = ''): string {
  const text = String(value || '').trim();
  if (!text) return '';
  let url;
  try {
    url = new URL(text);
  } catch {
    throw promptApiError(400, 'API 地址格式不正确');
  }
  if (!['http:', 'https:'].includes(url.protocol))
    throw promptApiError(400, 'API 地址仅支持 HTTP 或 HTTPS');
  url.search = '';
  url.hash = '';
  url.pathname = url.pathname
    .replace(/\/(?:models|chat\/completions)\/?$/i, '')
    .replace(/\/+$/, '');
  return url.toString().replace(/\/$/, '');
}

function promptApiError(statusCode: number, message: string): Error & { statusCode: number } {
  const error = new Error(message) as Error & { statusCode: number };
  error.statusCode = statusCode;
  return error;
}
