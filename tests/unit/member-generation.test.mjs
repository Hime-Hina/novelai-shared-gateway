import test from 'node:test';
import assert from 'node:assert/strict';
import { deflateSync } from 'node:zlib';
import { Readable } from 'node:stream';
import { crc32, BASE_MODELS, NATIVE_LIMITS, NATIVE_SAMPLERS, NATIVE_NOISE_SCHEDULES, NATIVE_VIBE_MODELS, NATIVE_BLUR_VALUES } from '../../server/native-image.ts';
import { configureFairQuota, syncFairQuota } from '../../server/fair-quota.ts';
import { readNativeBody } from '../../server/native-body.ts';
import { V5_MEDIUM_PROFILE } from '../../server/v5-medium.ts';
import { generateNovelAiImage } from '../../server/providers.ts';
import { memberGenerationCapabilities } from '../../server/generation-capabilities.ts';
import { normalizeMemberGeneration, materializeMemberGeneration, quoteMemberGeneration, parseMemberGenerationConfirmation,
  memberGenerationIntentFingerprint, assertMemberGenerationConfirmation, assertMemberGenerationCost, assertMemberGenerationVersions,
  addMemberGenerationResources, memberGenerationInput, memberGenerationInputAsset } from '../../server/member-generation.ts';

const NOW = 1800000000000;
const zero = () => ({ v5: '0', anlasFixed: '0', anlasPurchased: '0' });
function png(width = 128, height = 128) {
  const chunk = (type, bytes) => { const h = Buffer.alloc(8), t = Buffer.alloc(4); h.writeUInt32BE(bytes.length); h.write(type, 4); t.writeUInt32BE(crc32(Buffer.concat([Buffer.from(type), bytes]))); return Buffer.concat([h, bytes, t]); };
  const h = Buffer.alloc(13); h.writeUInt32BE(width); h.writeUInt32BE(height, 4); h[8] = 8; h[9] = 6;
  return Buffer.concat([Buffer.from('89504e470d0a1a0a', 'hex'), chunk('IHDR', h), chunk('IDAT', deflateSync(Buffer.alloc((width * 4 + 1) * height))), chunk('IEND', Buffer.alloc(0))]);
}
const image = png(), image64 = image.toString('base64');
const body = (parameters = {}, extra = {}) => ({ operation: 'generate', input: 'synthetic prompt', model: 'nai-diffusion-4-5-full', action: 'generate', parameters: { width: 1024, height: 1024, steps: 23, seed: -1, ...parameters }, ...extra });
function fixture({ v5 = 80, fixed = 10000, purchased = 0 } = {}) {
  const users = ['a', 'b', 'c', 'd'].map(id => ({ id, token: `token-${id}`, enabled: true }));
  const db = { settings: {}, users, jobs: [], ledger: [], accounts: [{ id: 'opus-private', token: 'must-never-leak', enabled: true, quotaTier: 3, subscriptionActive: true,
    quotaFixed: fixed, quotaPurchased: purchased, v5UsagePercent: v5, v5UsageIsNegative: false, v5UsageTimeUntilNextPercent: 600, quotaCheckedAt: NOW }] };
  configureFairQuota(db, users.map(user => user.id)); syncFairQuota(db, NOW); return db;
}
function confirmation(quote) {
  return { requestFingerprint: quote.requestFingerprint, versions: quote.versions,
    maxPerJob: quote.charges.perJob.reduce((max, entry) => Object.fromEntries(Object.keys(max).map(resource => [resource, Number(max[resource]) >= Number(entry.resources[resource]) ? max[resource] : entry.resources[resource]])), zero()), maxTotal: quote.charges.total };
}
function freeze(value) { if (value && typeof value === 'object') { Object.values(value).forEach(freeze); Object.freeze(value); } return value; }

test('member root unwraps once, rejects identity/credential fields and never mutates input', () => {
  const db = fixture(), input = body(), normalized = normalizeMemberGeneration(input), quote = quoteMemberGeneration(db, db.users[0], normalized, { now: NOW });
  input.confirmation = confirmation(quote); const before = structuredClone(input);
  const accepted = normalizeMemberGeneration(input);
  assert.equal(accepted.request.operation, undefined); assert.equal(accepted.request.nativeParameters.seed, 0);
  assert.equal(accepted.request.allowPurchasedAnlas, false); assert.equal(accepted.confirmation.maxTotal.v5, '0');
  assert.equal(accepted.requestFingerprint, normalized.requestFingerprint); assert.deepEqual(input, before);
  for (const key of ['memberId', 'accountId', 'fleetId', 'key', 'pst', 'token', 'source', 'clientContract', 'normalizationContext']) {
    assert.throws(() => normalizeMemberGeneration({ ...input, [key]: 'private-secret' }), error => error.code === 'invalid_parameters' && !error.message.includes('private-secret'));
  }
  assert.throws(() => normalizeMemberGeneration({ ...input, operation: 'unsupported' }), error => error.statusCode === 422);
});

test('random intent stays stable; first materialization selects exactly the saved seed', () => {
  const one = normalizeMemberGeneration(body()), two = normalizeMemberGeneration(body({ seed: undefined }));
  assert.equal(one.requestFingerprint, two.requestFingerprint); assert.equal(one.request.seed, 0);
  const actual = materializeMemberGeneration(one, 4294967295);
  assert.equal(actual.seed, 4294967295); assert.equal(actual.nativeParameters.seed, 4294967295); assert.equal(one.request.seed, 0);
  assert.notEqual(one.requestFingerprint, normalizeMemberGeneration(body({ seed: 0 })).requestFingerprint);
  const saved = one.normalizationContext, name = one.request.model;
  BASE_MODELS.delete(name);
  try {
    assert.throws(() => normalizeMemberGeneration(body()), error => error.statusCode === 422);
    assert.equal(normalizeMemberGeneration(body(), { normalizationContext: saved }).requestFingerprint, one.requestFingerprint);
    assert.equal(memberGenerationInput('req_old', actual).recoverable, true);
  } finally { BASE_MODELS.add(name); }
  const oldDefault = structuredClone(saved); oldDefault.native.defaults.scale = 6;
  assert.equal(normalizeMemberGeneration(body(), { normalizationContext: oldDefault }).request.scale, 6);
});

test('stored Medium profile survives rule changes and server-only confirmation/context never enter upstream payload', async () => {
  const input = body({}, { model: 'nai-diffusion-5-full-medium' }), normalized = normalizeMemberGeneration(input), oldProfile = { ...V5_MEDIUM_PROFILE }, originalFetch = globalThis.fetch;
  const versions = memberGenerationCapabilities({}).versions, calls = [];
  try {
    Object.assign(V5_MEDIUM_PROFILE, { steps: 12, sampler: 'k_euler', negativePreset: 'changed preset' });
    const current = normalizeMemberGeneration(input);
    assert.equal(current.request.steps, 12); assert.notEqual(current.requestFingerprint, normalized.requestFingerprint);
    assert.throws(() => assertMemberGenerationVersions({ versions }, memberGenerationCapabilities({}).versions), error => error.code === 'quote_changed');
    const replay = normalizeMemberGeneration(input, { normalizationContext: normalized.normalizationContext });
    assert.equal(replay.requestFingerprint, normalized.requestFingerprint); assert.equal(replay.request.steps, 14);
    assert.equal(replay.request.negative, normalized.request.negative); assert.notEqual(memberGenerationCapabilities({}).versions.defaults, versions.defaults);
    globalThis.fetch = async (_url, options) => { calls.push(JSON.parse(options.body)); return new Response(image, { headers: { 'content-type': 'image/png' } }); };
    const request = { ...materializeMemberGeneration(replay, 77), clientContract: 'member-generation-v1', confirmation: { private: 'must-not-reach-upstream' } };
    assert.deepEqual(request.normalizationContext, replay.normalizationContext);
    await generateNovelAiImage(request, { token: 'synthetic-local-token' }, { NOVELAI_API_URL: 'https://synthetic.invalid' }, { forceStream: false });
    assert.equal(calls.length, 1); assert.deepEqual(Object.keys(calls[0]).sort(), ['action', 'input', 'model', 'parameters', 'use_new_shared_trial']);
    assert.equal(calls[0].parameters.steps, 14); assert.equal(calls[0].parameters.sampler, 'k_euler_ancestral');
    assert.equal(calls[0].parameters.negative_prompt, normalized.request.negative);
    assert.ok(!JSON.stringify(calls).includes('must-not-reach-upstream')); assert.ok(!JSON.stringify(calls).includes('normalizationContext'));
  } finally { Object.assign(V5_MEDIUM_PROFILE, oldProfile); globalThis.fetch = originalFetch; }
});

test('historical normalization passes its saved limits to every reference, caption, image and auxiliary helper', () => {
  const encoded = Buffer.alloc(256, 42).toString('base64'), original = { ...NATIVE_LIMITS };
  const small = (parameters = {}, root = {}) => body({ width: 128, height: 128, ...parameters }, root);
  const character = { char_caption: 'character', centers: [{ x: 0.25, y: 0.5 }, { x: 0.75, y: 0.5 }] };
  const caption = { caption: { base_caption: 'base caption', char_captions: [character, character] }, use_coords: true };
  const imageInputs = [small({ image: image64 }, { action: 'img2img' }), small({ image: image64, mask: image64 }, { action: 'infill' }),
    { operation: 'upscale', image: image64 }, { operation: 'encode-vibe', image: image64, model: 'nai-diffusion-4-5-full' },
    small({ reference_image_multiple: [image64], reference_strength_multiple: [0.6] }, { model: 'nai-diffusion-3' })];
  const cases = [
    ['referencesMax', 1, [small({ reference_image_multiple: [encoded, encoded], reference_strength_multiple: [0.6, 0.6] }),
      small({ reference_image_multiple_cached: [{ data: encoded }, { data: encoded }], reference_strength_multiple: [0.6, 0.6] })]],
    ['charactersMax', 1, [small({ v4_prompt: caption }), small({ v4_prompt: caption, characterPrompts: [{}, {}] })]],
    ['centersMax', 1, [small({ v4_prompt: { ...caption, caption: { ...caption.caption, char_captions: [character] } } })]],
    ['characterPromptCharacters', 2, [small({ v4_prompt: caption })]],
    ['promptCharacters', 2, [small({ negative_prompt: 'negative' }, { input: 'x' }), small({ v4_prompt: { caption: { base_caption: 'long base', char_captions: [] } } }, { input: 'x' })]],
    ['imageDimension', 64, imageInputs], ['imageBytes', image.length - 1, imageInputs], ['imagePixels', 128 * 128 - 1, imageInputs],
    ['vibeEncodingBytes', 255, [small({ reference_image_multiple: [encoded], reference_strength_multiple: [0.6] })]],
    ['generationPixels', 128 * 128 - 1, [small(), { operation: 'upscale', image: image64 }]],
    ['dimensionMin', 256, [small()]], ['dimensionMax', 127, [small()]], ['stepsMin', 24, [small()]], ['stepsMax', 22, [small()]], ['samplesMax', 1, [small({ n_samples: 2 })]],
  ];
  for (const [limit, value, inputs] of cases) {
    const accepted = inputs.map(input => normalizeMemberGeneration(input));
    try {
      NATIVE_LIMITS[limit] = value;
      for (const [index, input] of inputs.entries()) {
        assert.throws(() => normalizeMemberGeneration(input), error => error.statusCode === 422, `new intent must use current ${limit}`);
        const replay = normalizeMemberGeneration(input, { normalizationContext: accepted[index].normalizationContext });
        assert.equal(replay.requestFingerprint, accepted[index].requestFingerprint, `replay must use first ${limit}`);
        if (imageInputs.includes(input)) {
          const recovered = memberGenerationInput('historical', materializeMemberGeneration(replay, 7));
          assert.equal(recovered.recoverable, true); assert.ok(recovered.assets.length > 0);
        }
      }
    } finally { Object.assign(NATIVE_LIMITS, original); }
  }
});

test('historical auxiliary model, blur, sampler and noise enumerations remain available only through the stored context', () => {
  const cases = [
    [NATIVE_VIBE_MODELS, 'nai-diffusion-4-5-full', { operation: 'encode-vibe', image: image64, model: 'nai-diffusion-4-5-full' }],
    [NATIVE_BLUR_VALUES, 0.3, { operation: 'upscale', image: image64, declared_blur_sigma: 0.3 }],
    [NATIVE_SAMPLERS, 'k_euler', body({ sampler: 'k_euler' })],
    [NATIVE_NOISE_SCHEDULES, 'karras', body({ noise_schedule: 'karras' })],
  ];
  for (const [values, removed, input] of cases) {
    const before = [...values], accepted = normalizeMemberGeneration(input);
    try {
      values.splice(values.indexOf(removed), 1);
      assert.throws(() => normalizeMemberGeneration(input), error => error.statusCode === 422);
      assert.equal(normalizeMemberGeneration(input, { normalizationContext: accepted.normalizationContext }).requestFingerprint, accepted.requestFingerprint);
    } finally { values.splice(0, values.length, ...before); }
  }
});

test('quotes use independent per-child prices and leave frozen wallets, ledger, reservations and jobs untouched', () => {
  const db = fixture(), before = structuredClone(db); freeze(db);
  const free = quoteMemberGeneration(db, db.users[0], normalizeMemberGeneration(body({ n_samples: 2 })), { now: NOW });
  assert.equal(free.admission.allowed, true); assert.deepEqual(free.charges.total, zero()); assert.equal(free.charges.perJob.length, 2);
  const v5 = quoteMemberGeneration(db, db.users[0], normalizeMemberGeneration(body({ n_samples: 2 }, { model: 'nai-diffusion-5-full' })), { now: NOW });
  assert.equal(v5.charges.perJob[0].resources.v5, '0.057803468209'); assert.equal(v5.charges.total.v5, '0.115606936418');
  assert.equal(v5.charges.perJob[0].estimated, true); assert.equal(v5.effective.seedMode, 'random'); assert.equal(Object.hasOwn(v5.effective, 'seed'), false);
  const paid = quoteMemberGeneration(db, db.users[0], normalizeMemberGeneration(body({ steps: 29 })), { now: NOW });
  assert.deepEqual(paid.charges.total, { v5: '0', anlasFixed: '21', anlasPurchased: '0' });
  assert.deepEqual(db, before); assert.ok(!JSON.stringify(v5).includes('must-never-leak')); assert.ok(!JSON.stringify(v5).includes('synthetic prompt'));
});

test('valid but unavailable requests return blocked quotes for balance, purchase, free limit and queue', () => {
  const empty = fixture({ v5: 0, fixed: 0, purchased: 100 });
  const v5 = quoteMemberGeneration(empty, empty.users[0], normalizeMemberGeneration(body({}, { model: 'nai-diffusion-5-full' })), { now: NOW });
  assert.equal(v5.admission.allowed, false); assert.ok(v5.admission.reasons.some(reason => reason.code === 'insufficient_balance'));
  const paidInput = { operation: 'upscale', image: image64 };
  const unpaid = quoteMemberGeneration(empty, empty.users[0], normalizeMemberGeneration(paidInput), { now: NOW });
  assert.deepEqual(unpaid.charges.total, { v5: '0', anlasFixed: '0', anlasPurchased: '1' });
  assert.ok(unpaid.admission.reasons.some(reason => reason.code === 'purchased_anlas_not_authorized'));
  assert.equal(quoteMemberGeneration(empty, empty.users[0], normalizeMemberGeneration({ ...paidInput, allowPurchasedAnlas: true }), { now: NOW }).admission.allowed, true);
  const db = fixture(); db.jobs = Array.from({ length: 4 }, (_, n) => ({ id: `job${n}`, userToken: db.users[0].token, status: 'queued' }));
  const blocked = quoteMemberGeneration(db, { ...db.users[0], acceptingJobs: false }, normalizeMemberGeneration(body()), { now: NOW, freeUsage: { enabled: true, successful: 1, reserved: 0, warningThreshold: 1, hardLimit: 1, warning: true, warnedAt: NOW, nextAvailableAt: NOW + 1000, waitingForSettlement: false } });
  assert.ok(blocked.admission.reasons.some(reason => reason.code === 'member_stopped'));
  assert.ok(blocked.admission.reasons.some(reason => reason.code === 'free_image_limit' && reason.retryAt === new Date(NOW + 1000).toISOString()));
  assert.ok(quoteMemberGeneration(db, db.users[0], normalizeMemberGeneration(body()), { now: NOW }).admission.reasons.some(reason => reason.code === 'queue_full'));
  assert.equal(quoteMemberGeneration({}, { id: 'a' }, normalizeMemberGeneration(body()), { now: NOW }).admission.allowed, false);
});

test('confirmation uses canonical decimal strings, verifies each resource and includes the whole authorization in idempotency', () => {
  const db = fixture(), normalized = normalizeMemberGeneration(body()), quote = quoteMemberGeneration(db, db.users[0], normalized, { now: NOW }), original = confirmation(quote);
  const equivalent = structuredClone(original); for (const resource of Object.keys(equivalent.maxTotal)) equivalent.maxTotal[resource] = equivalent.maxPerJob[resource] = '0.000';
  assert.equal(memberGenerationIntentFingerprint(normalized, original), memberGenerationIntentFingerprint(normalized, equivalent));
  assertMemberGenerationConfirmation(parseMemberGenerationConfirmation(equivalent), quote);
  for (const value of ['-1', '-0', '1e2', 'NaN', 'Infinity', '0.0000000000001', '01', '1'.repeat(65), 0]) {
    assert.throws(() => parseMemberGenerationConfirmation({ ...original, maxTotal: { ...zero(), v5: value } }), error => error.statusCode === 400);
  }
  assert.throws(() => parseMemberGenerationConfirmation(), error => error.code === 'confirmation_required');
  const changed = structuredClone(original); changed.versions.pricing += '-changed';
  assert.throws(() => assertMemberGenerationConfirmation(changed, quote), error => error.code === 'quote_changed');
  assert.notEqual(memberGenerationIntentFingerprint(normalized, changed), memberGenerationIntentFingerprint(normalized, original));
  assert.throws(() => assertMemberGenerationCost(original, { ...zero(), v5: '0.000000000001' }), error => error.code === 'cost_limit_exceeded');
  const cap = { ...original, maxPerJob: { ...zero(), v5: '1' }, maxTotal: { ...zero(), v5: '2' } };
  assertMemberGenerationCost(cap, { ...zero(), v5: '1' }, { ...zero(), v5: '2' });
  assert.throws(() => assertMemberGenerationCost(cap, { ...zero(), v5: '1' }, { ...zero(), v5: '2.000000000001' }), error => error.code === 'cost_limit_exceeded');
  assert.deepEqual(addMemberGenerationResources({ ...zero(), v5: '0.1' }, { ...zero(), v5: '0.2' }), { ...zero(), v5: '0.3' });
});

test('auxiliary operations and JSON/multipart share normalization, price and input asset slots', async () => {
  const db = fixture(), json = { operation: 'encode-vibe', model: 'nai-diffusion-4-5-full', image: image64, information_extracted: 0.5 };
  const form = new FormData(); form.set('request', JSON.stringify({ ...json, image: 'image' })); form.set('image', new Blob([image], { type: 'image/png' }), 'synthetic.png');
  const wire = new Request('http://localhost/', { method: 'POST', body: form }), req = Readable.from([Buffer.from(await wire.arrayBuffer())]); req.headers = { 'content-type': wire.headers.get('content-type') };
  const decoded = await readNativeBody(req), fromJson = normalizeMemberGeneration(json), fromForm = normalizeMemberGeneration(decoded);
  assert.equal(fromJson.requestFingerprint, fromForm.requestFingerprint);
  assert.deepEqual(quoteMemberGeneration(db, db.users[0], fromJson, { now: NOW }).charges.total, { v5: '0', anlasFixed: '2', anlasPurchased: '0' });
  const request = materializeMemberGeneration(fromJson), recovered = memberGenerationInput('req_own', request);
  assert.deepEqual(recovered.input.image, { slot: 'image' }); assert.equal(recovered.assets[0].mimeType, 'image/png'); assert.equal(recovered.assets[0].byteLength, image.length);
  assert.deepEqual(memberGenerationInputAsset(request, 'image').bytes, image);
  assert.throws(() => memberGenerationInputAsset(request, '../../.env'), error => error.code === 'input_not_found');
  assert.equal(memberGenerationInput('missing', { model: 'old' }).recoverable, false);
  assert.throws(() => normalizeMemberGeneration({ ...json, accountId: 'other' }), error => error.statusCode === 422);
});

test('recovered Native input preserves actual first seed, mask and opaque references without storing assets twice', () => {
  const input = body({ width: 128, height: 128, image: image64, mask: image64, n_samples: 2 }, { action: 'infill' });
  const request = materializeMemberGeneration(normalizeMemberGeneration(input), 71), before = structuredClone(request), recovered = memberGenerationInput('req_a', request);
  assert.equal(recovered.input.parameters.seed, 71); assert.equal(recovered.input.parameters.n_samples, 2);
  assert.equal(recovered.assets.length, 2); assert.deepEqual(memberGenerationInputAsset(request, 'parameters.mask').bytes, image); assert.deepEqual(request, before);
  const binary = Buffer.alloc(256, 37), vibe = normalizeMemberGeneration(body({ reference_image_multiple: [binary.toString('base64')], reference_strength_multiple: [0.6] }));
  assert.equal(memberGenerationInput('req_v', vibe.request).assets[0].mimeType, 'application/octet-stream');
  assert.deepEqual(memberGenerationInputAsset(vibe.request, 'parameters.reference_image_multiple.0').bytes, binary);
  const legacy = { model: 'nai-diffusion-4-5-full', prompt: 'old prompt', width: 1024, height: 1024, steps: 23, seed: 3, scale: 5, cfg: 0, sampler: 'k_euler', noiseSchedule: 'karras', negative: '' };
  assert.equal(memberGenerationInput('req_web', legacy).recoverable, true);
  assert.equal(memberGenerationInput('req_web', legacy).input.parameters.v4_prompt.caption.base_caption, 'old prompt');
  assert.equal(memberGenerationInput('req_web', { ...legacy, action: 'infill' }).recoverable, false);
});
