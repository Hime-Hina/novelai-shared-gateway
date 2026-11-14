import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { randomUUID } from 'node:crypto';
import { fixture, keys, databaseState } from '../helpers/relational-generation.mjs';
import { readRelationalCommit } from '../../server/events/relational-event-store.ts';
import { RelationalInputStore } from '../../server/storage/relational-input-store.ts';
import { RelationalMemberStore } from '../../server/storage/relational-member-store.ts';
import { RelationalCommitReplayer } from '../../server/storage/relational-replay.ts';
import { withAuthenticatedMemberApiKey } from '../../server/members/authentication.ts';
import { initializeRelationalStorage } from '../../server/storage/relational-storage.ts';

function appendOriginal(replayer, stored, inputs) {
  return replayer.appendCommit(
    {
      ...stored.command,
      events: stored.command.events.map((event, index) => ({
        ...event,
        eventId: stored.events[index].eventId,
      })),
    },
    inputs,
  );
}

function rewrite(t, db, seq, mutate) {
  const source = new Database(db.serialize());
  t.after(() => source.close());
  source.pragma('foreign_keys=ON');
  const stored = readRelationalCommit(source, seq);
  mutate(stored);
  const guards = source
    .prepare(
      "SELECT name,sql FROM sqlite_schema WHERE type='trigger' AND tbl_name IN ('commits','domain_events')",
    )
    .all();
  source.pragma('foreign_keys=OFF');
  for (const guard of guards) source.exec(`DROP TRIGGER ${guard.name}`);
  source.prepare('DELETE FROM domain_events WHERE commit_seq=?').run(BigInt(seq));
  const versions = new Map();
  const insert = source.prepare(
    `INSERT INTO domain_events(event_id,commit_seq,event_index,stream_id,stream_kind,stream_version,event_type,schema_version,occurred_at,correlation_id,causation_id,account_id,member_id,policy_version,pricing_version,amount_version,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
  );
  stored.events.forEach((event, index) => {
    if (!versions.has(event.streamId)) {
      const before = source
        .prepare(
          'SELECT CAST(MAX(stream_version) AS TEXT) AS v FROM domain_events WHERE stream_id=? AND commit_seq<?',
        )
        .get(event.streamId, BigInt(seq)).v;
      versions.set(event.streamId, BigInt(before ?? '0'));
    }
    versions.set(event.streamId, versions.get(event.streamId) + 1n);
    insert.run(
      event.eventId,
      BigInt(seq),
      index,
      event.streamId,
      event.streamKind,
      versions.get(event.streamId),
      event.eventType,
      event.schemaVersion,
      event.occurredAt,
      event.correlationId,
      event.causationId,
      event.accountId,
      event.memberId,
      event.policyVersion,
      event.pricingVersion,
      event.amountVersion,
      JSON.stringify(event.payload),
    );
  });
  source
    .prepare('UPDATE commits SET event_count=? WHERE commit_seq=?')
    .run(stored.events.length, BigInt(seq));
  source.exec(
    'DELETE FROM stream_heads WHERE NOT EXISTS(SELECT 1 FROM domain_events WHERE domain_events.stream_id=stream_heads.stream_id)',
  );
  for (const streamId of new Set(stored.events.map((event) => event.streamId))) {
    const last = source
      .prepare('SELECT * FROM domain_events WHERE stream_id=? ORDER BY stream_version DESC LIMIT 1')
      .get(streamId);
    source
      .prepare(
        'INSERT INTO stream_heads(stream_id,stream_kind,stream_version,last_commit_seq,last_event_id) VALUES(?,?,?,?,?) ON CONFLICT(stream_id) DO UPDATE SET stream_version=excluded.stream_version,last_commit_seq=excluded.last_commit_seq,last_event_id=excluded.last_event_id',
      )
      .run(streamId, last.stream_kind, last.stream_version, last.commit_seq, last.event_id);
  }
  for (const guard of guards) source.exec(guard.sql);
  source.pragma('foreign_keys=ON');
  assert.deepEqual(source.pragma('foreign_key_check'), []);
  return source;
}

test('普通v3受理逐本次注册绑定保护内容和唯一原event，拒绝旧context/input重新登记且完整回滚', async (t) => {
  const f = await fixture(t, { seconds: 0 });
  const first = await f.accept('first'),
    prefix = f.db.serialize();
  const second = await f.accept('second');
  const firstCommit = readRelationalCommit(f.db, first.commitSeq),
    context = firstCommit.events.find((event) => event.eventType === 'ContextRegistered'),
    input = firstCommit.events.find((event) => event.eventType === 'InputRegistered');
  assert.ok(context);
  assert.ok(input);
  const cases = [
    [
      'context-wrong-sha',
      (stored) => {
        stored.events.unshift({
          ...structuredClone(context),
          eventId: randomUUID(),
          payload: { ...context.payload, contextSha256: 'f'.repeat(64) },
        });
      },
    ],
    [
      'context-identical-second-registration',
      (stored) => {
        stored.events.unshift({ ...structuredClone(context), eventId: randomUUID() });
      },
    ],
    ...[
      ['input-identical-second-registration', {}],
      ['input-second-registration-wrong-sha', { inputSha256: 'e'.repeat(64) }],
      ['input-second-registration-wrong-bytes', { logicalBytes: input.payload.logicalBytes + 1 }],
    ].map(([name, changes]) => [
      name,
      (stored) => {
        const registered = stored.events.find((event) => event.eventType === 'InputRegistered'),
          accepted = stored.events.find((event) => event.eventType === 'RequestAccepted');
        registered.streamId = input.streamId;
        registered.payload = { ...input.payload, ...changes };
        accepted.payload.inputId = input.payload.inputId;
      },
    ]),
  ];
  for (const [name, mutate] of cases) {
    await t.test(name, async (t) => {
      const source = rewrite(t, f.db, second.commitSeq, mutate),
        target = new Database(prefix);
      t.after(() => target.close());
      target.pragma('foreign_keys=ON');
      const replay = new RelationalCommitReplayer(target);
      for (const { seq } of f.db
        .prepare(
          'SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE commit_seq>? AND commit_seq<? ORDER BY commit_seq',
        )
        .all(BigInt(first.commitSeq), BigInt(second.commitSeq))) {
        target.transaction(() =>
          appendOriginal(replay, readRelationalCommit(f.db, seq), f.inputs),
        )();
      }
      const forged = readRelationalCommit(source, second.commitSeq),
        before = databaseState(target),
        members = new RelationalMemberStore(target);
      await withAuthenticatedMemberApiKey(
        members,
        keys.m1,
        () => {
          assert.throws(
            () => appendOriginal(replay, forged, new RelationalInputStore(source)),
            (error) => error.code === 'input_unavailable',
          );
          return true;
        },
        {},
      );
      assert.equal(databaseState(target), before);
      const restored = target.transaction(() =>
        appendOriginal(replay, readRelationalCommit(f.db, second.commitSeq), f.inputs),
      )();
      assert.equal(restored.replayed, false);
      assert.equal(databaseState(target), databaseState(f.db));
      const once = databaseState(target);
      assert.equal(
        target.transaction(() =>
          replay.replayCommit(readRelationalCommit(f.db, second.commitSeq), f.inputs),
        )().replayed,
        true,
      );
      assert.equal(databaseState(target), once);

      const offline = new Database(':memory:');
      t.after(() => offline.close());
      initializeRelationalStorage(offline, 'generation-history');
      const business = new RelationalCommitReplayer(offline);
      for (const { seq } of f.db
        .prepare(
          'SELECT CAST(commit_seq AS TEXT) AS seq FROM commits WHERE commit_seq<? ORDER BY commit_seq',
        )
        .all(BigInt(second.commitSeq))) {
        offline.transaction(() =>
          business.replayCommit(readRelationalCommit(f.db, seq), f.inputs),
        )();
      }
      const checkpoint = databaseState(offline);
      offline.transaction(() =>
        assert.throws(
          () => business.replayCommit(forged, new RelationalInputStore(source)),
          (error) => error.code === 'input_unavailable',
        ),
      )();
      assert.equal(databaseState(offline), checkpoint);
      assert.equal(
        offline.transaction(() =>
          business.replayCommit(readRelationalCommit(f.db, second.commitSeq), f.inputs),
        )().replayed,
        false,
      );
      const recovered = databaseState(offline);
      assert.equal(
        offline.transaction(() =>
          business.replayCommit(readRelationalCommit(f.db, second.commitSeq), f.inputs),
        )().replayed,
        true,
      );
      assert.equal(databaseState(offline), recovered);
    });
  }
});
