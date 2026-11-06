import test from 'node:test';
import assert from 'node:assert/strict';
import { memberGenerationCapabilities } from '../../server/generation/capabilities.ts';
import { BASE_MODELS, INPAINT_MODELS, NATIVE_DEFAULTS, NATIVE_LIMITS, NATIVE_VIBE_MODELS } from '../../server/protocols/native-image.ts';
import { NATIVE_MULTIPART_MAX_PARTS } from '../../server/protocols/native-body.ts';
import { normalizeMemberGeneration } from '../../server/generation/member-contract.ts';

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

test('conditional capability requirements agree with Native validation for absent, plain and cached Vibe inputs', () => {
  const model = memberGenerationCapabilities({}).models.find(entry => entry.operation === 'generate' && entry.id === 'nai-diffusion-4-5-full');
  const encoded = Buffer.alloc(256, 42).toString('base64');
  const enabled = value => value !== undefined && value !== null && value !== '' && value !== false && (!Array.isArray(value) || value.length > 0);
  const allowedByRequirements = body => {
    const read = path => path.split('.').reduce((value, key) => value?.[key], body);
    return model.requirements.every(rule => rule.action && rule.action !== (body.action || 'generate') || rule.when && !enabled(read(rule.when))
      || rule.requires.every(path => enabled(read(path))) && rule.excludes.every(path => !enabled(read(path))));
  };
  for (const [parameters, expected] of [
    [{}, true],
    [{ reference_image_multiple: [], reference_image_multiple_cached: [] }, true],
    [{ reference_image_multiple: [encoded], reference_strength_multiple: [0.6] }, true],
    [{ reference_image_multiple_cached: [{ data: encoded }], reference_strength_multiple: [0.6] }, true],
    [{ reference_image_multiple: [], reference_image_multiple_cached: [{ data: encoded }], reference_strength_multiple: [0.6] }, true],
    [{ reference_image_multiple: [encoded], reference_image_multiple_cached: [{ data: encoded }], reference_strength_multiple: [0.6] }, false],
    [{ reference_image_multiple: [encoded], director_reference_images: ['synthetic'], reference_strength_multiple: [0.6] }, false],
  ]) {
    const body = { operation: 'generate', input: 'synthetic', model: model.id, parameters: { width: 128, height: 128, steps: 23, seed: 42, ...parameters } };
    assert.equal(allowedByRequirements(body), expected);
    if (expected) assert.doesNotThrow(() => normalizeMemberGeneration(body));
    else assert.throws(() => normalizeMemberGeneration(body), error => error.statusCode === 422);
  }
});

test('infill excludes every Native Vibe alias for both base and explicit inpainting model names', () => {
  const png = 'iVBORw0KGgoAAAANSUhEUgAAAIAAAACACAYAAADDPmHLAAAAVklEQVR4nO3BMQEAAADCoPVPbQwfoAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAOBvAI8AAT4ZY7sAAAAASUVORK5CYII=';
  const encoded = Buffer.alloc(256, 42).toString('base64'), caps = memberGenerationCapabilities({});
  const aliases = { reference_image: encoded, reference_image_multiple: [encoded], reference_image_multiple_cached: [{ data: encoded }] };
  for (const model of ['nai-diffusion-4-5-full', 'nai-diffusion-4-5-full-inpainting']) {
    const rule = caps.models.find(entry => entry.operation === 'generate' && entry.id === model).requirements.find(rule => rule.action === 'infill');
    const input = { operation: 'generate', input: 'synthetic infill', model, action: 'infill', parameters: { width: 128, height: 128, steps: 23, seed: 42, image: png, mask: png } };
    assert.doesNotThrow(() => normalizeMemberGeneration(input));
    for (const [field, value] of Object.entries(aliases)) {
      assert.ok(rule.excludes.includes(`parameters.${field}`), `${model} capability must exclude ${field}`);
      assert.throws(() => normalizeMemberGeneration({ ...input, parameters: { ...input.parameters, [field]: value } }), error => error.statusCode === 422);
    }
  }
});
