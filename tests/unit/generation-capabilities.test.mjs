import test from 'node:test';
import assert from 'node:assert/strict';
import { memberGenerationCapabilities } from '../../server/generation-capabilities.ts';
import { BASE_MODELS, INPAINT_MODELS, NATIVE_DEFAULTS, NATIVE_LIMITS, NATIVE_VIBE_MODELS } from '../../server/native-image.ts';
import { NATIVE_MULTIPART_MAX_PARTS } from '../../server/native-body.ts';

test('capabilities are generated from actual Native models, limits, defaults and Medium transformations', () => {
  const caps = memberGenerationCapabilities({}, { id: 'self' }, { env: {} });
  assert.equal(caps.contractVersion, 1);
  assert.deepEqual(caps.models.filter(model => model.operation === 'generate').map(model => model.id), [...BASE_MODELS, ...INPAINT_MODELS.keys()]);
  assert.deepEqual(caps.models.filter(model => model.operation === 'encode-vibe').map(model => model.id), NATIVE_VIBE_MODELS);
  assert.equal(new Set(caps.models.map(model => `${model.operation}:${model.id}`)).size, caps.models.length);
  const five = caps.models.find(model => model.operation === 'generate' && model.id === 'nai-diffusion-5-curated');
  assert.deepEqual(five.actions, ['generate', 'img2img']); assert.equal(five.features.vibe, false); assert.equal(five.features.preciseReference, false);
  const medium = caps.models.find(model => model.id === 'nai-diffusion-5-full-medium');
  assert.equal(medium.fields['parameters.steps'].fixed, 14); assert.equal(medium.fields['parameters.cfg_rescale'].fixed, 0);
  assert.equal(medium.fields['parameters.sampler'].fixed, 'k_euler_ancestral'); assert.ok(medium.transforms.some(transform => transform.code === 'medium_fixed_parameters'));
  assert.equal(five.fields['parameters.scale'].default, NATIVE_DEFAULTS.scale); assert.equal(five.fields['parameters.width'].max, NATIVE_LIMITS.dimensionMax);
  assert.equal(caps.limits.multipartParts, NATIVE_MULTIPART_MAX_PARTS); assert.equal(caps.billing.allowPurchasedAnlasDefault, false);
  assert.deepEqual(caps.models.find(model => model.operation === 'upscale').actions, []);
});

test('version changes track execution policy; balance, queue counts, warning labels and clocks do not invalidate quotes', () => {
  const db = { settings: { fairQuota: { enabled: true, autoAnlasFallback: false, groups: [{ id: 'g', name: 'first', members: ['self'], share: 1, memberWeights: { self: 1 } }],
    balances: { self: { v5Available: '1' } }, freeImagePolicy: { version: 1, enabled: true, warningThreshold: 2, hardLimit: 4 } } }, jobs: [], accounts: [{ id: 'private', token: 'secret' }] };
  const before = memberGenerationCapabilities(db, { id: 'self' }, { env: {} });
  db.settings.fairQuota.balances.self.v5Available = '0'; db.settings.fairQuota.lastRefillAt = 999; db.jobs.push({ id: 'j', status: 'queued' });
  db.settings.fairQuota.groups[0].name = 'renamed'; db.settings.fairQuota.freeImagePolicy.warningThreshold = 3; db.settings.fairQuota.freeImagePolicy.version++;
  assert.deepEqual(memberGenerationCapabilities(db, { id: 'self' }, { env: {} }).versions, before.versions);
  db.settings.fairQuota.autoAnlasFallback = true;
  const fallback = memberGenerationCapabilities(db, { id: 'self' }, { env: {} });
  assert.notEqual(fallback.versions.billingPolicy, before.versions.billingPolicy); assert.equal(fallback.versions.defaults, before.versions.defaults);
  const changedLimits = memberGenerationCapabilities(db, { id: 'self' }, { env: { QUEUE_MAX_ACTIVE_GLOBAL: '8' } });
  assert.notEqual(changedLimits.versions.capabilities, fallback.versions.capabilities); assert.notEqual(changedLimits.versions.billingPolicy, fallback.versions.billingPolicy);
  assert.ok(!JSON.stringify(fallback).includes('secret')); assert.ok(!JSON.stringify(fallback).includes('private'));
});
