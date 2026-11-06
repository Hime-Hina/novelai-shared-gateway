import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import ts from 'typescript';

test('store callers cannot access persistence internals or mix member/job/account fields', () => {
  const filename = fileURLToPath(new URL('./store-boundary-types.fixture.ts', import.meta.url));
  const source = `
import type { JsonStore } from '../../server/storage/sqlite-store.ts';
declare const store: JsonStore;
// @ts-expect-error Raw SQLite is implementation-only.
store.sqlite?.prepare('DELETE FROM domain_events').run();
// @ts-expect-error Prepared statements are implementation-only.
store.statements?.deleteGenericRecords.run();
// @ts-expect-error The mutable cache is implementation-only.
store.db!.users[0].queueSeq = 123;
// @ts-expect-error Full replacement is implementation-only.
store.replaceAll({});
// @ts-expect-error Domain persistence is implementation-only.
store.persistDomainFacts({}, {}, {});
// @ts-expect-error Incremental persistence is implementation-only.
store.persistIncremental({});
store.update(db => {
  db.users[0].enabled = false;
  db.jobs[0].queueSeq = 123;
  // @ts-expect-error A member is not a queue job.
  db.users[0].queueSeq = 123;
  // @ts-expect-error Job commands cannot write member credentials.
  db.jobs[0].passwordHash = 'wrong-entity-field';
});
const user = await store.readUserByToken('token');
// @ts-expect-error Member read models do not contain job fields.
if (user) user.queueSeq = 123;
const context = await store.findJobContext('job');
// @ts-expect-error Job read models do not contain member credentials.
if (context) context.job.passwordHash = 'wrong-entity-field';
// @ts-expect-error Account read models do not contain job queue positions.
if (context?.account) context.account.queueSeq = 123;
declare const query: Pick<JsonStore, 'readSettings' | 'findJobContext'>;
// @ts-expect-error Queries only receive read capabilities.
query.update(() => null);
`;
  const options = {
    target: ts.ScriptTarget.ES2022,
    module: ts.ModuleKind.NodeNext,
    moduleResolution: ts.ModuleResolutionKind.NodeNext,
    strict: true,
    noEmit: true,
    allowJs: true,
    checkJs: false,
    allowImportingTsExtensions: true,
    skipLibCheck: true,
    types: ['node'],
  };
  const host = ts.createCompilerHost(options),
    read = host.readFile.bind(host),
    exists = host.fileExists.bind(host),
    get = host.getSourceFile.bind(host);
  const fixture = (name) =>
    path.resolve(name).toLowerCase() === path.resolve(filename).toLowerCase();
  host.readFile = (name) => (fixture(name) ? source : read(name));
  host.fileExists = (name) => fixture(name) || exists(name);
  host.getSourceFile = (name, version, onError, newFile) =>
    fixture(name)
      ? ts.createSourceFile(name, source, version, true)
      : get(name, version, onError, newFile);
  const diagnostics = ts.getPreEmitDiagnostics(ts.createProgram([filename], options, host));
  assert.equal(
    diagnostics.length,
    0,
    ts.formatDiagnosticsWithColorAndContext(diagnostics, {
      getCanonicalFileName: (name) => name,
      getCurrentDirectory: () => process.cwd(),
      getNewLine: () => '\n',
    }),
  );
});
