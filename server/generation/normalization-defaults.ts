import { canonicalJson, domainDigest } from '../events/domain-events.ts';
import { requestNormalizationDefaults } from './request-fingerprint.ts';

export interface NormalizationDefaultsProfile {
  kind: 'normalization-defaults';
  values: ReturnType<typeof requestNormalizationDefaults>;
}

export function configurationUnavailable(): never {
  throw Object.assign(new Error('当前网关配置或其受保护来源不可用。'), {
    statusCode: 503,
    code: 'configuration_unavailable',
  });
}

function object(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object' && Object.getPrototypeOf(value) === Object.prototype;
}

// 这是当前默认值的专用载体，不能作为缺少 Native 定义的生成首次上下文。
export function parseNormalizationDefaultsProfile(value: unknown): NormalizationDefaultsProfile {
  if (
    !object(value) ||
    value.kind !== 'normalization-defaults' ||
    Object.keys(value).length !== 2 ||
    !object(value.values) ||
    !object(value.values.defaults)
  ) {
    configurationUnavailable();
  }
  const selected = requestNormalizationDefaults(value.values);
  const primitive = (entry: unknown) =>
    entry === null ||
    typeof entry === 'string' ||
    typeof entry === 'boolean' ||
    (typeof entry === 'number' && Number.isFinite(entry));
  if (
    Object.keys(selected).length !== 4 ||
    Object.keys(selected.defaults).length !== 8 ||
    Object.entries(value.values).some(([key, entry]) => key !== 'defaults' && !primitive(entry)) ||
    Object.values(value.values.defaults).some((entry) => !primitive(entry)) ||
    canonicalJson(selected) !== canonicalJson(value.values)
  ) {
    configurationUnavailable();
  }
  return { kind: 'normalization-defaults', values: structuredClone(selected) };
}

export function prepareNormalizationDefaultsProfile(values: unknown) {
  const profile = parseNormalizationDefaultsProfile({ kind: 'normalization-defaults', values }),
    body = canonicalJson(profile),
    contextSha256 = domainDigest(profile);
  return { profile, body, contextId: `defaults_${contextSha256}`, contextSha256 };
}
