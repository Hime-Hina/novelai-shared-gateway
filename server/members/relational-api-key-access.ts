import type Database from 'better-sqlite3';
import type { RelationalMemberStore } from '../storage/relational-member-store.ts';
import { assertRelationalStorageReady } from '../storage/relational-storage.ts';
import {
  authenticateMemberApiKey,
  withAuthenticatedMemberApiKey,
  type MemberApiKeyStore,
} from './authentication.ts';
import { requireDiscordMembership, type DiscordDependencies } from './discord-auth.ts';

function fail(): never {
  throw Object.assign(new Error('invalid token.'), { statusCode: 401 });
}

// 复用实际 Key/Discord owner；IO 不持有 SQL，后续回调只授予原稳定成员和原 auth epoch。
export async function authorizeRelationalMemberApiKey({
  sqlite,
  members,
  token,
  env = process.env,
  discordFetch,
}: {
  sqlite: Database.Database;
  members: RelationalMemberStore;
  token: string;
  env?: NodeJS.ProcessEnv;
  discordFetch?: DiscordDependencies['discordFetch'];
}) {
  const authentication: MemberApiKeyStore = {
    memberApiKeyTransaction: (hash, authorized) =>
      members.memberApiKeyTransaction(hash, (member) => {
        assertRelationalStorageReady(sqlite);
        return authorized(member);
      }),
  };
  const original = await authenticateMemberApiKey(authentication, token);
  if (!original) {
    fail();
  }
  const memberId = original.id,
    authEpoch = original.memberAuth?.version;
  await requireDiscordMembership(original, {
    env,
    discordFetch,
    store: {
      discordAuthenticationTransaction: (mutate) =>
        members.discordAuthenticationTransaction((transaction) => {
          assertRelationalStorageReady(sqlite);
          return mutate(transaction);
        }),
    },
  });
  return async <T>(read: (memberId: string) => T): Promise<T> => {
    const result = await withAuthenticatedMemberApiKey(
      authentication,
      token,
      (member) => {
        if (member.id !== memberId || member.memberAuth?.version !== authEpoch) {
          fail();
        }
        return { value: read(member.id) };
      },
      env,
    );
    if (!result) {
      fail();
    }
    return result.value;
  };
}
