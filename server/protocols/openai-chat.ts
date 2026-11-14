import { openAiFixedSteps, openAiSizeTiers } from './openai-models.ts';
import type { requestNormalizationDefaults } from '../generation/request-fingerprint.ts';

type Body = Record<string, unknown>;
export type OpenAiNormalizationDefaults = NonNullable<
  Parameters<typeof requestNormalizationDefaults>[0]
>;
const httpError = (statusCode: number, message: string) =>
  Object.assign(new Error(message), { statusCode });

export function parseOpenAiImageRequest(
  body: Body = {},
  settings: OpenAiNormalizationDefaults = {},
) {
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
