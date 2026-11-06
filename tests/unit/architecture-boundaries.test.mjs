import test from 'node:test';
import assert from 'node:assert/strict';
import { checkArchitecture, checkSource } from '../../scripts/check-architecture.mjs';

test('current production imports respect the concrete architecture boundaries', () => {
  assert.deepEqual(checkArchitecture(), []);
});

for (const [name, filename, source, alternative] of [
  [
    'reducer to IO',
    'server/events/quota-projections.ts',
    "import { readFile } from 'node:fs/promises';",
    /facts passed/,
  ],
  [
    'amount to HTTP',
    'server/quota/resource-amounts.ts',
    "fetch('https://example.invalid');",
    /event facts/,
  ],
  [
    'query to wallet mutation',
    'server/generation/request-queries.ts',
    "import { reserveFairQuota as reserve } from '../quota/fair-share.ts'; reserve(db, user, job, account);",
    /generation commands/,
  ],
  [
    'query to persistence',
    'server/generation/request-queries.ts',
    'store.update(db => db.users.pop());',
    /admission\/execution command/,
  ],
  [
    'protocol to persistence',
    'server/protocols/native-http.ts',
    'deps.store.update(db => db.jobs.pop());',
    /admission\/execution command/,
  ],
  [
    'web to backend',
    'web/server/api/unsafe.ts',
    "import { JsonStore } from '../../../server/storage/sqlite-store.ts';",
    /executor HTTP/,
  ],
  [
    'web dynamic backend import',
    'web/app/unsafe.ts',
    "await import('../../server/storage/sqlite-store.ts');",
    /executor HTTP/,
  ],
  [
    'web component to backend',
    'web/app/Unsafe.vue',
    '<template><p>Safe template</p></template><script setup lang="ts">import { JsonStore } from "../../server/storage/sqlite-store.ts";</script>',
    /executor HTTP/,
  ],
  [
    'module to runtime',
    'server/quota/unsafe.ts',
    "export { startGateway } from '../gateway-runtime.ts';",
    /owning generation/,
  ],
]) {
  test(`architecture check rejects ${name} and gives the allowed owner`, () => {
    const errors = checkSource(filename, source);
    assert.ok(errors.length, name);
    assert.ok(
      errors.every((error) => error.startsWith(`${filename}:`)),
      errors.join('\n'),
    );
    assert.match(errors.join('\n'), alternative);
  });
}

test('type-only dependencies, pure readers, transport guards and executor assembly remain legal', () => {
  for (const [filename, source] of [
    [
      'server/events/quota-projections.ts',
      "import type { JsonStore } from '../storage/sqlite-store.ts'; import { type AccountingJob } from '../generation/accounting.ts';",
    ],
    [
      'server/generation/request-queries.ts',
      "import { fairQuotaSummary } from '../quota/fair-share.ts'; createHash('sha256').update('safe');",
    ],
    [
      'web/app/types/member.ts',
      "export type { StateInvalidation } from '../../../server/events/state-notifications.ts';",
    ],
    [
      'web/server/utils/executor-client.ts',
      "import { checkGenerationPayloadSize } from '../../../server/generation/payload-budget.ts';",
    ],
    ['executor/start.mjs', "await import('novelai-shared-gateway/server/gateway-runtime.js');"],
    ['server/index.ts', "import { startGateway } from './gateway-runtime.ts';"],
  ]) {
    assert.deepEqual(checkSource(filename, source), [], filename);
  }
});
