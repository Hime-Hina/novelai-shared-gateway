// Offline B conversion only. These are selected same-snapshot rows, never an HTTP body or replay fact.
import { createHash, randomBytes } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { isOwner } from '../server/members/authentication.ts';
import { normalizeSocksProxyUrl } from '../server/protocols/novelai-client.ts';
import {
  readRelationalCommit,
  assertRelationalOpeningSource,
} from '../server/events/relational-event-store.ts';
import { assertRelationalStorageFormat } from '../server/storage/relational-storage.ts';
import { assertRelationalAccountConnections } from '../server/storage/relational-account-connection.ts';
import {
  relationalPrivateStateEmpty,
  relationalPrivateTables,
  readRelationalRecoveryState,
} from '../server/storage/relational-private-restore.ts';

const hash = (value) => createHash('sha256').update(value).digest('hex');
const validHash = (value) => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);
const text = (value) => typeof value === 'string' && Buffer.from(value).toString('utf8') === value;
const time = (value) => Number.isSafeInteger(value) && value >= 0;
const object = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const fail = () => {
  throw Error('Selected legacy private state is incomplete or conflicts with its B opening.');
};
const requireValue = (condition) => {
  if (!condition) fail();
};

function optionalString(value) {
  if (value === undefined || value === null) return null;
  requireValue(text(value));
  return value;
}
function optionalTime(value) {
  if (value === undefined || value === null) return null;
  requireValue(time(value));
  return value;
}
function owner(value) {
  requireValue(object(value) && text(value.ownerUserId) && text(value.guildId));
  return { ownerUserId: value.ownerUserId, guildId: value.guildId };
}

// Validate the fixed old accountProxyUrl syntax before using the current canonical URL owner.
// Missing aliases meant an explicit direct connection in 3c; no current environment is consulted.
function legacyProxy(account) {
  const value = String(account.proxyUrl || account.socksProxy || account.proxy || '').trim();
  if (!value) return '';
  try {
    const scheme = /^[a-z][a-z0-9+.-]*:\/\//i.test(value) ? value : `socks5://${value}`;
    const parsed = new URL(scheme.replace(/^sock5:\/\//i, 'socks5://'));
    requireValue(['socks5:', 'socks5h:'].includes(parsed.protocol) && parsed.hostname);
    return normalizeSocksProxyUrl(parsed.toString());
  } catch {
    fail();
  }
}

/**
 * Caller supplies already parsed rows from its one selected readonly legacy connection.
 * effectiveAt is an explicitly selected validity check later than the capture window,
 * NOT the public commit time or a claim about the exact private snapshot instant.
 * configuredOwner comes from the selected environment; sessionOwner is only the stored revocation ack.
 * discordLoginEnabled is the selected old owner's Boolean(guild || clientId || secret), never current env.
 * Apply only to a new building staging DB. The CLI owns one savepoint around public + private conversion,
 * then seals that new-format source; public rebuild + the existing unique installer restore its private cut.
 */
export function migrateLegacyPrivate(target, normalized, refs) {
  requireValue(target.inTransaction && object(normalized) && object(refs));
  assertRelationalStorageFormat(target);
  requireValue(readRelationalRecoveryState(target).status === 'building');
  requireValue(relationalPrivateStateEmpty(target));
  requireValue(time(normalized.effectiveAt) && validHash(refs.sourceSha256));
  requireValue(
    Array.isArray(normalized.users) &&
      Array.isArray(normalized.accounts) &&
      Array.isArray(normalized.sessions),
  );
  const configured = owner(normalized.configuredOwner);
  requireValue(
    typeof normalized.discordLoginEnabled === 'boolean' &&
      (!configured.guildId || normalized.discordLoginEnabled),
  );
  const previous = normalized.sessionOwner === null ? null : owner(normalized.sessionOwner);
  const environment = {
    OWNER_USER_ID: configured.ownerUserId,
    DISCORD_GUILD_ID: configured.guildId,
  };
  const commits = new Map();
  const eventById = (eventId) => {
    const row = target
      .prepare('SELECT CAST(commit_seq AS TEXT) AS seq FROM domain_events WHERE event_id=?')
      .get(eventId);
    requireValue(row);
    if (!commits.has(row.seq)) commits.set(row.seq, readRelationalCommit(target, row.seq));
    const event = commits.get(row.seq).events.find((value) => value.eventId === eventId);
    requireValue(event);
    return event;
  };
  const registered = target
    .prepare(
      "SELECT event_id FROM domain_events WHERE event_type='ImportSourceRegistered' AND stream_id=? LIMIT 2",
    )
    .all(`import:${refs.sourceId}`);
  requireValue(registered.length === 1);
  const source = eventById(registered[0].event_id);
  assertRelationalOpeningSource(target, source);
  requireValue(source.payload.sourceSha256 === refs.sourceSha256 && source.payload.route === 'B');
  const opening = (type, id) => {
    const event = eventById(refs.eventId(type, id));
    requireValue(
      event.eventType === type &&
        event.payload.origin.sourceId === refs.sourceId &&
        event.payload.origin.originalId === id &&
        isDeepStrictEqual(event.payload.origin, refs.origin(id)),
    );
    assertRelationalOpeningSource(target, event);
    return event;
  };
  const rows = Object.fromEntries(relationalPrivateTables.map(([name]) => [name, []]));
  const members = [],
    users = new Map(),
    seenAccounts = new Set();
  const counts = {
    members: 0,
    credentials: 0,
    unavailablePasswords: 0,
    apiKeys: 0,
    newBaselineAuthEpochs: 0,
    discordBindings: 0,
    invites: 0,
    sessions: 0,
    omittedSessions: 0,
    upstreamCredentials: 0,
  };
  for (const row of normalized.users) {
    requireValue(
      object(row) &&
        text(row.id) &&
        row.id &&
        object(row.data) &&
        row.data.id === row.id &&
        !users.has(row.id) &&
        refs.memberIds.has(row.id),
    );
    const user = row.data,
      event = opening('OpeningMemberRecorded', row.id);
    const current = target
      .prepare('SELECT status,source_event_id FROM members WHERE id=?')
      .get(row.id);
    requireValue(event.payload.memberId === row.id && current?.source_event_id === event.eventId);
    const active =
      current.status === 'active' &&
      user.enabled !== false &&
      (!normalized.discordLoginEnabled || user.discord?.member !== false);
    const originalEpoch =
      text(user.memberAuth?.version) && user.memberAuth.version ? user.memberAuth.version : null;
    const key = user.token;
    const keyValid =
      active &&
      text(key) &&
      key.startsWith('STA1N-') &&
      key.trim() === key &&
      key.length <= 4096 &&
      !key.includes('\0');
    if (keyValid) requireValue(row.token === key);
    // A new current-cut baseline identity for an old API-only key is not an old session/version.
    const epoch = active
      ? originalEpoch || (keyValid ? randomBytes(16).toString('hex') : null)
      : null;
    if (epoch && !originalEpoch) counts.newBaselineAuthEpochs++;
    requireValue(user.note === undefined || user.note === null || typeof user.note === 'string');
    members.push({
      id: row.id,
      epoch,
      note: typeof user.note === 'string' ? JSON.stringify(user.note) : null,
    });
    users.set(row.id, { user, originalEpoch, active });
    counts.members++;
    const auth = user.memberAuth;
    if (auth?.username !== undefined) {
      requireValue(text(auth.username) && auth.username);
      const salt = optionalString(auth.salt),
        passwordHash = optionalString(auth.hash);
      requireValue((salt === null) === (passwordHash === null));
      if (salt !== null)
        requireValue(/^[a-f0-9]{48}$/.test(salt) && /^[a-f0-9]{128}$/.test(passwordHash));
      // The API-only baseline must not turn a password without its original epoch into a session identity.
      if (salt !== null && !originalEpoch) counts.unavailablePasswords++;
      rows.password_credentials.push({
        member_id: row.id,
        username: auth.username,
        salt: originalEpoch ? salt : null,
        password_hash: originalEpoch ? passwordHash : null,
      });
      counts.credentials++;
    } else requireValue(auth?.salt === undefined && auth?.hash === undefined);
    if (keyValid) {
      rows.api_keys.push({ member_id: row.id, key_hash: hash(key), recoverable_key: key });
      counts.apiKeys++;
    }
    const discord = user.discord;
    if (discord?.id) {
      requireValue(text(discord.id) && text(discord.guildId) && discord.guildId);
      requireValue(
        discord.member === undefined ||
          discord.member === null ||
          typeof discord.member === 'boolean',
      );
      rows.discord_bindings.push({
        member_id: row.id,
        discord_id: discord.id,
        guild_id: discord.guildId,
        access_token: active ? optionalString(discord.accessToken) : null,
        refresh_token: active ? optionalString(discord.refreshToken) : null,
        expires_at: optionalTime(discord.expiresAt),
        checked_at: optionalTime(discord.checkedAt),
        eligible: discord.member == null ? null : Number(discord.member),
      });
      counts.discordBindings++;
    }
    const invite = user.discordInvite;
    if (
      active &&
      !discord?.id &&
      invite &&
      validHash(invite.hash) &&
      time(invite.expiresAt) &&
      invite.expiresAt > normalized.effectiveAt
    ) {
      rows.discord_invites.push({
        member_id: row.id,
        code_hash: invite.hash,
        expires_at: invite.expiresAt,
      });
      counts.invites++;
    }
  }
  if (previous)
    rows.member_auth_state.push({
      id: 1,
      owner_member_id: previous.ownerUserId,
      guild_id: previous.guildId,
    });
  for (const session of normalized.sessions) {
    const identity = users.get(session.user_id),
      user = identity?.user;
    const binding =
      user && isOwner(user, environment)
        ? JSON.stringify([user.id, user.discord.id, configured.guildId])
        : null;
    const changed =
      previous &&
      (previous.ownerUserId !== configured.ownerUserId || previous.guildId !== configured.guildId);
    const affected =
      changed &&
      (session.owner_binding !== null ||
        session.user_id === previous.ownerUserId ||
        session.user_id === configured.ownerUserId);
    if (
      !previous ||
      !validHash(session.token_hash) ||
      !identity?.active ||
      user.enabled !== true ||
      !identity.originalEpoch ||
      session.auth_version !== identity.originalEpoch ||
      !time(session.created_at) ||
      !time(session.expires_at) ||
      session.expires_at <= session.created_at ||
      session.expires_at <= normalized.effectiveAt ||
      affected ||
      session.owner_binding !== binding
    ) {
      counts.omittedSessions++;
      continue;
    }
    rows.member_sessions.push({
      token_hash: session.token_hash,
      member_id: session.user_id,
      auth_epoch: session.auth_version,
      owner_binding: session.owner_binding,
      created_at: session.created_at,
      expires_at: session.expires_at,
    });
    counts.sessions++;
  }
  for (const row of normalized.accounts) {
    requireValue(
      object(row) &&
        text(row.id) &&
        row.id &&
        object(row.data) &&
        row.data.id === row.id &&
        !seenAccounts.has(row.id) &&
        refs.accountIds.has(row.id),
    );
    seenAccounts.add(row.id);
    const event = opening('OpeningAccountRecorded', row.id);
    const current = target
      .prepare('SELECT enabled,credential_epoch,source_event_id FROM upstream_accounts WHERE id=?')
      .get(row.id);
    requireValue(
      event.payload.accountId === row.id &&
        event.payload.credentialEpoch === 1 &&
        current?.credential_epoch === 1 &&
        current.source_event_id === event.eventId,
    );
    const pst = row.data.token;
    if (!pst) {
      requireValue(current.enabled === 0);
      continue;
    }
    requireValue(
      text(pst) && /^pst-[A-Za-z0-9_-]+$/.test(pst) && pst.length <= 4096 && row.token === pst,
    );
    rows.upstream_credentials.push({ account_id: row.id, pst, proxy_url: legacyProxy(row.data) });
    counts.upstreamCredentials++;
  }
  return target.transaction(() => {
    // Savepoint covers every private table/column even if the outer migration catches an SQL error.
    requireValue(relationalPrivateStateEmpty(target));
    const update = target.prepare('UPDATE members SET auth_epoch=?,note_json=? WHERE id=?');
    for (const member of members)
      requireValue(update.run(member.epoch, member.note, member.id).changes === 1);
    for (const [table] of relationalPrivateTables) {
      for (const row of rows[table]) {
        const fields = Object.keys(row);
        target
          .prepare(
            `INSERT INTO ${table}(${fields.join(',')}) VALUES(${fields.map(() => '?').join(',')})`,
          )
          .run(...fields.map((field) => row[field]));
      }
    }
    assertRelationalAccountConnections(target);
    requireValue(!target.prepare('PRAGMA foreign_key_check').get());
    return counts;
  })();
}
