import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { bootstrap } from '../../scripts/bootstrap.mjs';

const root = fileURLToPath(new URL('../../', import.meta.url));
export async function childFixture(t, label) {
  const directory = await mkdtemp(path.join(tmpdir(), `nai-${label}-`)),
    dataDir = path.join(directory, 'data'),
    env = {
      DATA_DIR: dataDir,
      ADMIN_TOKEN: randomUUID() + randomUUID(),
      NOVELAI_TOKEN: 'pst-synthetic-child-fixture-only',
    },
    initialized = await bootstrap({ env, log() {} }),
    members = JSON.parse(await readFile(initialized.credentialPath, 'utf8')).accounts,
    fixture = { directory, dataDir, env, members, children: [] };
  fixture.filename = path.join(dataDir, 'library.sqlite');
  fixture.accountId = query(
    fixture,
    'SELECT bound_account_id FROM sharing_policy WHERE id=1',
  )[0].bound_account_id;
  assert.ok(fixture.accountId);
  assert.equal(
    query(fixture, 'SELECT enabled FROM upstream_accounts WHERE id=?', fixture.accountId)[0]
      .enabled,
    0,
  );
  t.after(async () => {
    for (const child of fixture.children) {
      await child.kill();
    }
    assert.equal(path.dirname(path.resolve(directory)), path.resolve(tmpdir()));
    assert.ok(path.basename(directory).startsWith(`nai-${label}-`));
    await rm(directory, { recursive: true, force: true });
  });
  return fixture;
}

// Each audit opens a read-only connection, so child shutdown/reopen owns all writers and WALs.
export function query(fixture, sql, ...parameters) {
  const db = new Database(fixture.filename, { readonly: true });
  try {
    return db.prepare(sql).all(...parameters);
  } finally {
    db.close();
  }
}

export async function launchGateway(fixture, upstreamOrigin, env = {}) {
  const origin = new URL(upstreamOrigin);
  assert.equal(origin.protocol, 'http:');
  assert.equal(origin.hostname, '127.0.0.1');
  const preload = `
    import { registerHooks } from 'node:module';
    registerHooks({load(url, context, next) {
      if (/\\/server\\/storage\\/(?:sqlite-store|sqlite-migrations)\\.(?:ts|js)(?:[?#]|$)/.test(url))
        throw new Error('relational child must not load legacy SQLite owners');
      return next(url, context);
    }});
    const actualFetch = globalThis.fetch;
    globalThis.fetch = (input, options) => {
      const url = new URL(typeof input === 'string' || input instanceof URL ? input : input.url);
      if (url.origin !== ${JSON.stringify(origin.origin)})
        throw new Error('child fixture denied upstream before IO');
      return actualFetch(input, options);
    };
    process.on('message', message => { if (message === 'shutdown') process.emit('SIGTERM'); });
  `;
  const child = spawn(
    process.execPath,
    ['--import', `data:text/javascript,${encodeURIComponent(preload)}`, 'server/index.ts'],
    {
      cwd: root,
      windowsHide: true,
      env: {
        ...Object.fromEntries(
          ['PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE']
            .filter((key) => process.env[key])
            .map((key) => [key, process.env[key]]),
        ),
        ...fixture.env,
        HOST: '127.0.0.1',
        PORT: '0',
        SHUTDOWN_GRACE_MS: '10000',
        ...env,
        NOVELAI_API_URL: origin.origin,
        NOVELAI_ACCOUNT_API_URL: origin.origin,
      },
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    },
  );
  const state = { child, output: '', exit: null, origin: null };
  const finished = once(child, 'exit').then(([code, signal]) => (state.exit = { code, signal }));
  child.stderr.on('data', (bytes) => (state.output += bytes));
  child.stdout.on('data', (bytes) => {
    state.output += bytes;
    const match = state.output.match(/Nai2API listening on http:\/\/127\.0\.0\.1:(\d+)/);
    if (match) state.origin = `http://127.0.0.1:${match[1]}`;
  });
  state.kill = async () => {
    if (!state.exit) child.kill('SIGKILL');
    await finished;
  };
  state.stop = async () => {
    // Windows IPC exercises the actual SIGTERM handler; it is not a POSIX signal claim.
    if (!state.exit) child.send('shutdown');
    await finished;
    assert.equal(state.exit.code, 0, state.output);
  };
  fixture.children.push(state);
  const deadline = Date.now() + 15000;
  while (!state.origin && !state.exit && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.ok(state.origin, `actual child startup failed: ${state.output}`);
  return state;
}

export async function enableAccount(fixture, child) {
  const response = await fetch(child.origin + `/api/admin/accounts/${fixture.accountId}`, {
    method: 'PATCH',
    headers: {
      origin: child.origin,
      'content-type': 'application/json',
      'x-admin-token': fixture.env.ADMIN_TOKEN,
    },
    body: JSON.stringify({ enabled: true }),
    signal: AbortSignal.timeout(5000),
  });
  const body = await response.json();
  assert.equal(response.status, 200, JSON.stringify(body));
  assert.equal(body.enabled, true);
}
