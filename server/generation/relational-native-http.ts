import { relationalStateNotificationsFor } from '../events/relational-state-notifications.ts';
import type Database from 'better-sqlite3';
import type { EventEmitter } from 'node:events';
import type { ServerResponse } from 'node:http';
import { authenticateMemberApiKey } from '../members/authentication.ts';
import { requireDiscordMembership, type DiscordDependencies } from '../members/discord-auth.ts';
import { RelationalMemberStore } from '../storage/relational-member-store.ts';
import { RelationalInputStore } from '../storage/relational-input-store.ts';
import { RelationalGenerationStore } from '../storage/relational-generation-store.ts';
import { RelationalRequestStore } from '../storage/relational-request-store.ts';
import { assertRelationalStorageReady } from '../storage/relational-storage.ts';
import type { RelationalReplicaReadConfiguration } from '../storage/relational-replica-store.ts';
import {
  createNativeHttp,
  type NativeHttpRequest,
  type NativeHttpDependencies,
} from '../protocols/native-http.ts';
import { readGenerationIdempotencyKey } from './admission.ts';
import { createRelationalNativeAdmission } from './relational-native-admission.ts';
import { createRelationalApiDelivery } from './relational-api-delivery.ts';
import type { JobPreviews } from './job-previews.ts';
import type { createGenerationPayloadBudget } from './payload-budget.ts';

export function createRelationalNativeHttp(deps: {
  sqlite: Database.Database;
  events: EventEmitter;
  previews: JobPreviews;
  replicas: RelationalReplicaReadConfiguration;
  payloadBudget: ReturnType<typeof createGenerationPayloadBudget>;
  readJson(req: NativeHttpRequest, limit: number): Promise<unknown>;
  sendImage: NativeHttpDependencies['sendImage'];
  schedule(): void;
  timeoutMs: number;
  env?: NodeJS.ProcessEnv;
  discordFetch?: DiscordDependencies['discordFetch'];
  now?(): number;
  logDiagnostic?(id: string, event: string, details: object): void;
}) {
  const { sqlite, env = process.env } = deps;
  assertRelationalStorageReady(sqlite);
  const members = new RelationalMemberStore(
      sqlite,
      env,
      relationalStateNotificationsFor(sqlite, deps.events),
    ),
    inputs = new RelationalInputStore(sqlite, env),
    generations = new RelationalGenerationStore(sqlite, env),
    requests = new RelationalRequestStore(sqlite);
  const admit = createRelationalNativeAdmission({
    sqlite,
    members,
    inputs,
    generations,
    payloadBudget: deps.payloadBudget,
    replicas: deps.replicas,
    env,
    now: deps.now,
  });
  return async (req: NativeHttpRequest, res: ServerResponse, url: URL): Promise<boolean> => {
    if (
      req.method !== 'POST' ||
      !/^\/(?:v1\/)?ai\/(?:generate-image(?:-stream)?|encode-vibe|upscale)$/.test(url.pathname)
    ) {
      return false;
    }
    assertRelationalStorageReady(sqlite);
    const claim = req.generationPayloadClaim ?? deps.payloadBudget.claim();
    req.generationPayloadClaim = claim;
    try {
      req.generationIdempotencyKey = readGenerationIdempotencyKey(req);
      const token =
        String(req.headers.authorization || '')
          .match(/^Bearer\s+(.+)$/i)?.[1]
          ?.trim() || '';
      const delivery = createRelationalApiDelivery({
        sqlite,
        members,
        requests,
        token,
        env,
        replicas: deps.replicas,
        events: deps.events,
        now: deps.now,
      });
      try {
        const native = createNativeHttp({
          readMember: async (key) => {
            const member = await authenticateMemberApiKey(members, key);
            if (!member) {
              return null;
            }
            await requireDiscordMembership(member, {
              store: members,
              env,
              discordFetch: deps.discordFetch,
            });
            const current = members.readMember(member.id);
            return current?.status === 'active'
              ? { admissionVersion: current.admissionEpoch }
              : null;
          },
          admitNative: async (options) => {
            const batch = await admit(options);
            if (batch.replayed) {
              await delivery.validateReplay(batch.jobIds);
            }
            return batch;
          },
          readJson: deps.readJson,
          waitForJobResult: delivery.waitForJobResult,
          readStoredImage: async (image) => {
            const content = await delivery.readSavedImage(image);
            return content.buffer;
          },
          sendImage: deps.sendImage,
          previews: deps.previews,
          schedule: deps.schedule,
          timeoutMs: deps.timeoutMs,
          now: deps.now,
          logDiagnostic: deps.logDiagnostic,
        });
        return await native.handle(req, res, url);
      } finally {
        delivery.close();
      }
    } finally {
      claim.release();
      delete req.generationPayloadClaim;
    }
  };
}
