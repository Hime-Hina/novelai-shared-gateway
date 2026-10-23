interface PromptApiInput { baseUrl?: unknown; apiKey?: unknown; model?: unknown }
interface PromptApiConfig { baseUrl: string; apiKey: string; model: string }
type PromptFetch = (url: string, init: RequestInit) => Promise<Pick<Response, 'ok' | 'status' | 'text'>>;
interface PromptApiOptions { fetchImpl?: PromptFetch; timeoutMs?: number }
interface PromptRequestOptions extends PromptApiOptions {
  apiKey?: string; method?: string; serviceName?: string; body?: unknown;
}
// 只描述现有读取位置；外部 JSON 的业务值仍是 unknown，不能据此信任上游字段。
interface PromptPayload {
  data?: unknown; models?: unknown; message?: unknown;
  error?: { message?: unknown } | null;
  choices?: Array<{ message?: { content?: unknown } | null; text?: unknown }>;
}

const promptSystemMessage = `You are a specialist at converting Chinese image requests into precise NovelAI Diffusion prompts.

OUTPUT CONTRACT
- Return exactly one line of English, comma-separated NovelAI/booru-style tags. Never return prose, headings, Markdown, Chinese, JSON or a negative-prompt section.
- For an adult NSFW scene, the first tag must be nsfw.
- Use concise visual tags, not sentences. Split compound ideas into concrete tags; for example, 月下 becomes moonlight, night.
- Describe only people, objects, clothing, background, lighting, camera framing and physical actions that are objectively visible in the requested image. Never include thoughts, memories, metaphors, plans or story exposition.
- Do not invent artist names, model settings, unrelated details or sexual content that the user did not request.

TAG PRIORITY AND ORDER
1. If this is a known copyrighted/fandom character, put the official English character tag or widely used canonical character tag first, followed immediately by its defining appearance. Never fabricate a character identity. For an original character, use original instead of its personal name.
2. Subject count and identity: 1girl, 1boy, multiple girls, species, role or archetype; include age only when visually relevant or needed to establish an adult-only explicit scene.
3. Defining appearance: hairstyle, hair color, eye color, skin, body type and distinctive accessories. These are the highest-priority consistency tags.
4. Clothing and its exact current state: garment type, material and details, whether it is intact, lifted, open, torn, partially removed or absent.
5. Main pose and action: standing, kneeling, walking, sleeping, cooking and other concrete actions.
6. Fine action and interaction details: which hand does what, contact with self, another adult, a prop or the environment; distinguish one hand from both hands and use spatially precise tags.
7. Visible expression and gaze: looking at viewer, looking away, smile, open mouth, blush, tears and other observable reactions.
8. Camera and visible body region: from above, from below, from behind, upper body, lower body, full body, close-up, between legs, dutch angle and focal emphasis.
9. Location, props, time, weather, lighting and atmosphere: bedroom, beach, indoors, morning, night, moonlight, rim lighting and other visible scene information.

CONSISTENCY RULES
- The latest explicit state in the request wins. Remove every conflicting tag instead of outputting both states.
- Adapt features to what the camera can actually see. A lower-body-only frame must omit facial expression, eye color and other invisible upper-body details. A back view must omit invisible eye details; a covered face or blindfold must omit hidden eye details.
- Convert dialogue or narrative claims into visible actions only when the request makes the action visually clear; for example, “showing underwear” becomes lifting skirt, panties.
- Preserve exact relative positions, prop locations, clothing state, lighting and interaction partners. Never swap who performs or receives an action.
- Use explicit absence tags such as no bra or no panties only when the absence is visually important and directly requested; otherwise omit the element.

WEIGHTING
- Emphasize only the most important stable traits or focal actions with NovelAI braces: {tag}, {{tag}}, {{{tag}}}. Prefer defining appearance, then action, clothing and expression. Avoid excessive weighting and never weight every tag.
- De-emphasize minor background details with [tag] or [[tag]] only when needed.
- Keep logically related tags adjacent and allocate more tags to the visual focal point than to minor background details.

For multiple characters, keep each character's appearance and actions unambiguous and adjacent. `;

export function normalizePromptApiConfig(value: PromptApiInput = {}): PromptApiConfig {
  return {
    baseUrl: normalizePromptApiBaseUrl(value.baseUrl || ''),
    apiKey: String(value.apiKey || '').trim(),
    model: String(value.model || '').trim()
  };
}

export function publicPromptApiConfig(value: PromptApiInput = {}): { configured: boolean } {
  const config = normalizePromptApiConfig(value);
  return { configured: isPromptApiConfigured(config) };
}

export function adminPromptApiConfig(value: PromptApiInput = {}) {
  const config = normalizePromptApiConfig(value);
  return {
    baseUrl: config.baseUrl,
    model: config.model,
    configured: isPromptApiConfigured(config),
    apiKeyConfigured: Boolean(config.apiKey)
  };
}

export function isPromptApiConfigured(value: PromptApiInput = {}): boolean {
  const config = normalizePromptApiConfig(value);
  return Boolean(config.baseUrl && config.apiKey && config.model);
}

export function normalizePromptApiBaseUrl(value: unknown = ''): string {
  const text = String(value || '').trim();
  if (!text) return '';
  let url;
  try {
    url = new URL(text);
  } catch {
    throw promptApiError(400, 'API 地址格式不正确');
  }
  if (!['http:', 'https:'].includes(url.protocol)) throw promptApiError(400, 'API 地址仅支持 HTTP 或 HTTPS');
  url.search = '';
  url.hash = '';
  url.pathname = url.pathname
    .replace(/\/(?:models|chat\/completions)\/?$/i, '')
    .replace(/\/+$/, '');
  return url.toString().replace(/\/$/, '');
}

export async function fetchPromptApiModels(value: PromptApiInput, options: PromptApiOptions = {}): Promise<string[]> {
  const config = requirePromptApiCredentials(value);
  const payload = await requestPromptApi(`${config.baseUrl}/models`, {
    apiKey: config.apiKey,
    fetchImpl: options.fetchImpl,
    timeoutMs: options.timeoutMs || 15_000
  });
  const source: unknown[] = Array.isArray((payload as PromptPayload | null | undefined)?.data)
    ? (payload as PromptPayload).data as unknown[]
    : Array.isArray((payload as PromptPayload | null | undefined)?.models)
      ? (payload as PromptPayload).models as unknown[]
      : [];
  const models = source
    .map((item) => typeof item === 'string' ? item : (item as { id?: unknown; name?: unknown } | null | undefined)?.id || (item as { name?: unknown } | null | undefined)?.name)
    .map((item) => String(item || '').trim())
    .filter(Boolean)
    .filter((item, index, array) => array.indexOf(item) === index)
    .sort((a, b) => a.localeCompare(b));
  if (!models.length) throw promptApiError(502, 'API 没有返回可用模型');
  return models;
}

export async function convertChinesePrompt(value: PromptApiInput, prompt: unknown, options: PromptApiOptions = {}): Promise<string> {
  const config = normalizePromptApiConfig(value);
  if (!isPromptApiConfigured(config)) throw promptApiError(503, '管理员尚未完成提示词 API 配置');
  const input = String(prompt || '').trim();
  if (!input) throw promptApiError(400, '请输入中文画面描述');
  if (input.length > 3000) throw promptApiError(400, '中文画面描述不能超过 3000 字');
  const payload = await requestPromptApi(`${config.baseUrl}/chat/completions`, {
    method: 'POST',
    apiKey: config.apiKey,
    fetchImpl: options.fetchImpl,
    timeoutMs: options.timeoutMs || 60_000,
    body: {
      model: config.model,
      messages: [
        { role: 'system', content: promptSystemMessage },
        { role: 'user', content: input }
      ],
      temperature: 0.35,
      max_tokens: 1000,
      stream: false
    }
  });
  const content = promptApiContent(payload);
  if (!content) throw promptApiError(502, 'API 没有返回提示词');
  return content;
}

async function requestPromptApi(url: string, options: PromptRequestOptions = {}): Promise<unknown> {
  const fetchImpl = options.fetchImpl || fetch;
  const serviceName = options.serviceName || '提示词 API';
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), options.timeoutMs || 15_000);
  try {
    const response = await fetchImpl(url, {
      method: options.method || 'GET',
      headers: {
        accept: 'application/json',
        authorization: `Bearer ${options.apiKey}`,
        ...(options.body ? { 'content-type': 'application/json' } : {})
      },
      body: options.body ? JSON.stringify(options.body) : undefined,
      signal: controller.signal
    });
    const text = await response.text();
    const payload = safeJson(text);
    if (!response.ok) {
      const detail = String((payload as PromptPayload | null | undefined)?.error?.message || (payload as PromptPayload | null | undefined)?.message || '').trim();
      if (response.status === 401 || response.status === 403) throw promptApiError(502, 'API 密钥无效或没有权限');
      if (response.status === 429) throw promptApiError(502, 'API 请求过于频繁，请稍后重试');
      throw promptApiError(502, detail ? `上游 API：${detail}` : `上游 API 返回 HTTP ${response.status}`);
    }
    return payload;
  } catch (error) {
    if ((error as { statusCode?: unknown } | null | undefined)?.statusCode) throw error;
    if ((error as { name?: unknown } | null | undefined)?.name === 'AbortError') throw promptApiError(504, `${serviceName} 请求超时`);
    const code = String((error as { cause?: { code?: unknown } | null } | null | undefined)?.cause?.code || (error as { code?: unknown } | null | undefined)?.code || '').trim();
    throw promptApiError(502, `无法连接${serviceName}${code ? `（${code}）` : ''}`);
  } finally {
    clearTimeout(timer);
  }
}

function requirePromptApiCredentials(value: PromptApiInput = {}): PromptApiConfig {
  const config = normalizePromptApiConfig(value);
  if (!config.baseUrl) throw promptApiError(400, '请输入 API 地址');
  if (!config.apiKey) throw promptApiError(400, '请输入 API 密钥');
  return config;
}

function promptApiContent(payload: unknown = {}): string {
  const content: unknown = (payload as PromptPayload | null | undefined)?.choices?.[0]?.message?.content ?? (payload as PromptPayload | null | undefined)?.choices?.[0]?.text ?? '';
  const text = Array.isArray(content)
    ? content.map((item: unknown) => typeof item === 'string' ? item : (item as { text?: unknown } | null | undefined)?.text || '').join('')
    : String(content || '');
  return text
    .trim()
    .replace(/^```(?:\w+)?\s*/i, '')
    .replace(/\s*```$/, '')
    .replace(/^prompt\s*:\s*/i, '')
    .replace(/^(["'])|(["'])$/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function safeJson(text: string): unknown {
  try {
    return text ? JSON.parse(text) : {};
  } catch {
    throw promptApiError(502, '上游 API 返回了无效数据');
  }
}

function promptApiError(statusCode: number, message: string): Error & { statusCode: number } {
  const error = new Error(message) as Error & { statusCode: number };
  error.statusCode = statusCode;
  return error;
}
