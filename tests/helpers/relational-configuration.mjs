import { RelationalConfigurationStore } from '../../server/storage/relational-configuration-store.ts';

export const fixtureNormalizationDefaults = {
  defaultModel: 'nai-diffusion-4-5-full',
  defaultArtist: 'synthetic configured artist',
  defaultNegative: 'synthetic configured negative',
  defaults: {
    size: '竖图',
    width: 832,
    height: 1216,
    steps: 28,
    scale: 6,
    cfg: 0,
    sampler: 'k_dpmpp_2m_sde',
    noiseSchedule: 'karras',
  },
};

export function initializeFixtureConfiguration(sqlite) {
  return sqlite.transaction(() =>
    new RelationalConfigurationStore(sqlite).initializeGatewayConfiguration(
      {
        commandId: 'initialize-fixture-configuration',
        actorKind: 'system',
        actorId: null,
        recordedAt: 1800000000000,
        correlationId: null,
        causationId: null,
      },
      {
        normalizationDefaults: fixtureNormalizationDefaults,
        publicBaseUrl: null,
        originRef: 'synthetic-fixture:current-configuration-v1',
      },
    ),
  )();
}
