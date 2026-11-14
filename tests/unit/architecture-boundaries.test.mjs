import test from 'node:test';
import assert from 'node:assert/strict';
import { checkArchitecture, checkSource } from '../../scripts/check-architecture.mjs';

const writerFactories = [
  ['createRelationalAccountQuotaRefresh', '../quota/relational-account-refresh.ts'],
  ['createRelationalMemberGenerationHttp', '../generation/relational-member-http.ts'],
  ['createLegacyMemberGenerationHttp', '../generation/legacy-member-http.ts'],
  ['createRelationalNativeAdmission', '../generation/relational-native-admission.ts'],
  ['createRelationalNativeHttp', '../generation/relational-native-http.ts'],
  ['createLegacyNativeHttp', '../generation/legacy-native-http.ts'],
  ['createRelationalMemberAuthenticationHttp', '../members/relational-authentication-http.ts'],
  ['handleMemberAuthenticationRoutes', '../members/authentication-http.ts'],
  ['createRelationalMemberReadHttp', '../members/relational-read-http.ts'],
  ['createRelationalApiGenerationAdmission', '../generation/relational-api-admission.ts'],
  ['createRelationalOpenAiHttp', '../generation/relational-openai-http.ts'],
  ['createRelationalDirectHttp', '../generation/relational-direct-http.ts'],
  ['relationalStateNotificationsFor', '../events/relational-state-notifications.ts'],
];

// 同一已登记写命令的混合来源；只限制整模块能力，保留明确的纯 named reader。
const mixedWriterSources = [
  ['ownerAccountCommand', '../members/owner-operations.ts'],
  ['applyOperatingFunding', '../quota/operating-accounting.ts'],
  ['withAuthenticatedMemberSession', '../members/authentication.ts'],
  ['setMemberAdmission', '../generation/queue-policy.ts'],
  ['prepareGenerationResult', '../generation/execution.ts'],
  ['closeGenerationReview', '../generation/accounting.ts'],
  ['configureFreeImages', '../quota/free-image-policy.ts'],
  ['updateLossAlertPolicy', '../quota/failure-accounting.ts'],
  ['appendDomainCommit', '../events/event-store.ts'],
  ['beginDomainCommand', '../events/domain-events.ts'],
  ['JsonStore', '../storage/sqlite-store.ts'],
];

for (const filename of [
  'server/generation/request-queries.ts',
  'server/protocols/native-http.ts',
]) {
  for (const source of [
    "import {relationalStateNotificationsFor as run} from '../events/relational-state-notifications.ts'; run(raw,events);",
    "const module=await import('../events/relational-state-notifications.js'); const commit=module.relationalStateNotificationsFor; commit(raw,events);",
    "export * from '../events/relational-state-notifications.ts';",
    "const module=require('../events/relational-state-notifications.js'); const run=module.relationalStateNotificationsFor; run(raw,events);",
    "import {emitStoreEvent as emit} from '../events/state-notifications.ts'; emit(events,'commit',{});",
    "import * as notifications from '../events/state-notifications.ts'; const emit=notifications.emitStoreEvent; emit(events,'commit',{});",
    "const notifications=await import('../events/state-notifications.js'); const emit=notifications.emitStoreEvent; emit(events,'commit',{});",
  ]) {
    test(`${filename} rejects notification transaction capability ${source}`, () => {
      assert.ok(checkSource(filename, source).some((error) => /write capability/.test(error)));
    });
  }
  test(`${filename} allows notification type-only read`, () => {
    assert.deepEqual(
      checkSource(
        filename,
        "import type {RelationalStateNotifications} from '../events/relational-state-notifications.ts';",
      ),
      [],
    );
  });
  test(`${filename} allows named existing read mapper`, () => {
    assert.deepEqual(
      checkSource(
        filename,
        "import {stateInvalidation} from '../events/state-notifications.ts'; stateInvalidation(event,id,false);",
      ),
      [],
    );
  });
}

for (const filename of [
  'server/generation/request-queries.ts',
  'server/protocols/native-http.ts',
]) {
  for (const [name, modulePath] of writerFactories) {
    test(`${filename} rejects namespace capture of ${name}`, () => {
      assert.ok(
        checkSource(
          filename,
          `import * as writer from '${modulePath}'; const run = writer.${name}; run(deps);`,
        ).some((error) => /write capability/.test(error)),
      );
    });
  }
  for (const [name, modulePath] of mixedWriterSources) {
    const invoke = name === 'JsonStore' ? 'new run(dataDir);' : 'run(deps);';
    for (const source of [
      `import * as writer from '${modulePath}'; const run = writer.${name}; ${invoke}`,
      `const writer = await import('${modulePath.replace('.ts', '.js')}'); const run = writer.${name}; ${invoke}`,
    ]) {
      test(`${filename} rejects captured mixed writer ${name}: ${source}`, () => {
        assert.ok(checkSource(filename, source).some((error) => /write capability/.test(error)));
      });
    }
  }
  for (const [name, source] of [
    [
      'named legacy store capture',
      "import { JsonStore as Ledger } from '../storage/sqlite-store.ts'; const ledger = new Ledger(dataDir); const run = ledger.memberSessionTransaction.bind(ledger); run(mutate);",
    ],
    [
      'named legacy store re-export',
      "export { JsonStore as ProtocolWriter } from '../storage/sqlite-store.ts';",
    ],
  ]) {
    test(`${filename} rejects ${name}`, () => {
      assert.ok(checkSource(filename, source).some((error) => /write capability/.test(error)));
    });
  }
  for (const [name, modulePath] of [
    ['createRelationalMemberReadHttp', '../members/relational-read-http.ts'],
    ['ownerOperation', '../members/owner-operations.ts'],
  ]) {
    test(`${filename} rejects named and injected ${name}`, () => {
      assert.ok(
        checkSource(filename, `import { ${name} as run } from '${modulePath}'; run(deps);`).length,
      );
      assert.ok(checkSource(filename, `deps.${name}(input);`).length);
    });
  }
  for (const [name, modulePath] of writerFactories.slice(-3)) {
    test(`${filename} rejects dynamic capture of ${name}`, () => {
      const source = `const writer = await import('${modulePath.replace('.ts', '.js')}'); const run = writer.${name}; run(deps);`;
      assert.ok(checkSource(filename, source).some((error) => /write capability/.test(error)));
    });
  }
  for (const modulePath of [
    '../members/relational-read-http.ts',
    '../members/owner-operations.ts',
    '../quota/operating-accounting.ts',
  ]) {
    test(`${filename} rejects writer star re-export ${modulePath}`, () => {
      assert.ok(
        checkSource(filename, `export * from '${modulePath}';`).some((error) =>
          /write capability/.test(error),
        ),
      );
    });
  }
  for (const [name, modulePath] of writerFactories.slice(0, 3)) {
    test(`${filename} rejects aliased ${name}`, () => {
      assert.ok(
        checkSource(filename, `import { ${name} as run } from '${modulePath}'; run(deps);`).some(
          (error) => /write capability/.test(error),
        ),
      );
    });
  }
  for (const [name, source] of [
    [
      'dynamic quota refresh',
      "const writer = await import('../quota/relational-account-refresh.js'); const run = writer.createRelationalAccountQuotaRefresh; run(deps);",
    ],
    ['quota refresh re-export', "export * from '../quota/relational-account-refresh.ts';"],
  ]) {
    test(`${filename} rejects ${name}`, () => {
      assert.ok(checkSource(filename, source).some((error) => /write capability/.test(error)));
    });
  }
}

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

for (const filename of [
  'server/generation/request-queries.ts',
  'server/protocols/native-http.ts',
]) {
  for (const [name, source] of [
    [
      'aliased append',
      "import { appendRelationalCommit as append } from '../events/relational-event-store.ts'; append(raw,c);",
    ],
    [
      'namespace replay',
      "import * as facts from '../events/relational-event-store.ts'; facts.replayRelationalCommit(raw,c);",
    ],
    ['dynamic writer import', "await import('../events/relational-event-store.js');"],
    [
      'writer require alias',
      "const { replayRelationalCommit: replay } = require('../events/relational-event-store.ts'); replay(raw,c);",
    ],
    [
      'input store construction',
      "import { RelationalInputStore as Store } from '../storage/relational-input-store.ts'; new Store(raw).persistAcceptedInput(c,a);",
    ],
    [
      'input store re-export',
      "export { RelationalInputStore as Store } from '../storage/relational-input-store.ts';",
    ],
    ['direct acceptance', 'inputStore.persistAcceptedInput(c,a);'],
    ['bound account synchronization', 'syncBoundFairQuota(view,account,now);'],
    [
      'operating funding owner',
      "import { applyOperatingFunding as apply } from '../quota/operating-accounting.ts'; apply(view,body,context,now);",
    ],
    [
      'aliased synchronization',
      "import { syncBoundFairQuota as sync } from '../quota/fair-share.ts'; sync(view,account,now);",
    ],
    [
      'captured quota namespace writer',
      "import * as quota from '../quota/fair-share.ts'; const configure=quota.configureFairGroups; configure(view,groups,now);",
    ],
    [
      'captured dynamic quota writer',
      "const quota=await import('../quota/fair-share.js'); const configure=quota.configureFairGroups; configure(view,groups,now);",
    ],
    ['quota namespace re-export', "export * from '../quota/fair-share.ts';"],
    ['direct input replay', 'inputStore.replayAcceptedInput(source,cursor);'],
    ['direct session mutation', 'authStore.memberSessionTransaction(fn,options);'],
    ['direct Discord mutation', 'authStore.discordAuthenticationTransaction(fn);'],
    ['direct Discord invitation', 'authStore.createMemberDiscordInvite(id);'],
    ['scoped Discord save', 'transaction.saveAuthentication(user);'],
    [
      'password HTTP writer',
      "import { handleMemberAuthenticationRoutes as login } from '../members/authentication-http.ts'; login(req,res,url,deps);",
    ],
    [
      'Discord HTTP writer',
      "import { handleMemberDiscordAuthenticationRoutes as login } from '../members/authentication-http.ts'; login(req,res,url,deps);",
    ],
    [
      'aliased session renewal HTTP writer',
      "import { renewMemberCookie as renew } from '../members/authentication-http.ts'; renew(req,res,store,env);",
    ],
    [
      'relational authentication HTTP writer factory',
      "import { createRelationalMemberAuthenticationHttp as create } from '../members/relational-authentication-http.ts'; create(deps);",
    ],
    ...[
      [
        'API raw admission writer',
        '../generation/relational-api-admission.ts',
        'createRelationalApiGenerationAdmission',
      ],
      [
        'relational Direct HTTP writer',
        '../generation/relational-direct-http.ts',
        'createRelationalDirectHttp',
      ],
      [
        'relational OpenAI HTTP writer',
        '../generation/relational-openai-http.ts',
        'createRelationalOpenAiHttp',
      ],
      [
        'Native raw admission writer',
        '../generation/relational-native-admission.ts',
        'createRelationalNativeAdmission',
      ],
      [
        'relational Native HTTP writer',
        '../generation/relational-native-http.ts',
        'createRelationalNativeHttp',
      ],
      [
        'legacy Native HTTP writer',
        '../generation/legacy-native-http.ts',
        'createLegacyNativeHttp',
      ],
    ].map(([label, path, name]) => [
      label,
      `import { ${name} as create } from '${path}'; create(deps);`,
    ]),
    ['direct key mutation', 'authStore.memberApiKeyTransaction(hash,fn);'],
    [
      'relational member store',
      "import { RelationalMemberStore as Store } from '../storage/relational-member-store.ts'; new Store(raw);",
    ],
    ['credential persistence', 'memberStore.setMemberCredentials(prepared);'],
    ['member admission command', 'memberStore.changeMemberAdmission(command,input,actor);'],
    ['member admission commit', 'memberStore.assertMemberAdmissionCommit(events,receipt);'],
    ['member removal commit', 'memberStore.assertMemberRemovalCommit(events);'],
    ['member quota decision owner', 'applyFairMemberChange(view,draft,input,now);'],
    [
      'aliased member decision owner',
      "import { applyFairMemberChange as change } from '../quota/fair-share.ts'; change(view,draft,input,now);",
    ],
    ['queue admission owner', 'setMemberAdmission(view,id,false,refund);'],
    [
      'business replayer construction',
      "import { RelationalCommitReplayer as Replayer } from '../storage/relational-replay.ts'; new Replayer(raw);",
    ],
    ['dynamic business replayer', "await import('../storage/relational-replay.js');"],
    ...[
      'createInputProjector',
      'createDiagnosticProjector',
      'stageGenerationDiagnostic',
      'stageGenerationReuse',
      'createGenerationProjector',
      'createQuotaProjector',
      'applyAccountEvent',
      'applyReplayedMemberEvent',
      'replayCommit',
      'appendCommit',
    ].map((name) => [`direct ${name}`, `store.${name}(input);`]),
    [
      'private snapshot import',
      "import { restoreRelationalPrivateSnapshot as restore } from '../storage/relational-private-restore.ts'; restore(raw,path,hash);",
    ],
    [
      'dynamic private snapshot import',
      "await import('../storage/relational-private-restore.js');",
    ],
    ['direct private snapshot restore', 'restoreRelationalPrivateSnapshot(raw,path,hash);'],
    [
      'ready installer alias',
      "import { verifyAndInstallRelationalStorage as install } from '../storage/relational-ready.ts'; install(raw,source);",
    ],
    ['dynamic ready installer', "await import('../storage/relational-ready.js');"],
    ['direct ready installation', 'verifyAndInstallRelationalStorage(raw,source);'],
    ['current configuration initialization', 'initializeGatewayConfiguration(metadata,input);'],
    ['protected defaults stage', 'stageNormalizationDefaultsProfile(values);'],
    ['opening input stage', 'stageOpeningInput(input);'],
    ['opening context stage', 'stageOpeningContext(id,context);'],
    ['protected defaults projection', 'createNormalizationDefaultsProjector(source,commit,false);'],
    ['configuration projection', 'createConfigurationProjector(commit,source,false);'],
    [
      'dynamic configuration writer',
      "await import('../storage/relational-configuration-store.js');",
    ],
    [
      'generation store construction',
      "import { RelationalGenerationStore as Store } from '../storage/relational-generation-store.ts'; new Store(raw);",
    ],
    ['dynamic generation writer', "await import('../storage/relational-generation-store.js');"],
    [
      'dynamic relational admission',
      "await import('../generation/relational-member-admission.js');",
    ],
    [
      'dynamic relational queue runtime',
      "await import('../generation/relational-queue-runtime.js');",
    ],
    [
      'namespace relational queue runtime',
      "import * as queue from '../generation/relational-queue-runtime.ts';",
    ],
    [
      'namespace relational admission',
      "import * as admission from '../generation/relational-member-admission.ts';",
    ],
    ['generation writer re-export', "export * from '../storage/relational-generation-store.ts';"],
    [
      'aliased result recovery',
      "import { recoverRelationalGenerationResults as recover } from '../storage/relational-result-recovery.ts'; recover(raw,dir);",
    ],
    ['dynamic result recovery', "await import('../storage/relational-result-recovery.js');"],
    ['direct result recovery', 'recoverRelationalGenerationResults(raw,dir);'],
    ['local publication fact planning', 'store.planLocalArtifactPublication(descriptor,now);'],
    ['job history retirement', 'store.retireJobHistory(metadata,ids,actor,controls);'],
    ['artifact history retirement', 'store.retireArtifactHistory(metadata,ids,actor,controls);'],
    [
      'replica store alias',
      "import { RelationalReplicaStore as Store } from '../storage/relational-replica-store.ts'; new Store(raw);",
    ],
    ['dynamic replica IO', "await import('../storage/relational-replica-io.js');"],
    [
      'replica IO alias',
      "import { copyRelationalArtifactReplica as copy } from '../storage/relational-replica-io.ts'; copy(raw,id,kind,config);",
    ],
    ...[
      'registerStorageTarget',
      'beginArtifactReplica',
      'recordArtifactReplica',
      'applyReplicaEvents',
      'replayReplicaCommit',
      'copyRelationalArtifactReplica',
      'verifyRelationalArtifactReplica',
    ].map((name) => [`direct ${name}`, `store.${name}(command,input);`]),
    ...[
      'acceptGeneration',
      'reuseGeneration',
      'createRelationalMemberGenerationAdmission',
      'createRelationalQueueRuntime',
      'admitRelationalMemberGeneration',
      'dispatchGeneration',
      'recordGenerationResponse',
      'prepareGenerationResult',
      'completeGeneration',
      'failGeneration',
      'cancelWaitingGeneration',
      'closeGenerationReview',
      'replayGenerationCommit',
    ].map((name) => [`direct ${name}`, `store.${name}(command,input);`]),
    ['element refund command', "store['failGeneration'](command,input);"],
    [
      'quota construction',
      "import { RelationalQuotaStore as Store } from '../storage/relational-quota-store.ts'; new Store(raw);",
    ],
    ['dynamic quota writer', "await import('../storage/relational-quota-store.js');"],
    [
      'account construction',
      "import { RelationalAccountStore as Store } from '../storage/relational-account-store.ts'; new Store(raw,q,controls);",
    ],
    ['dynamic account writer', "await import('../storage/relational-account-store.js');"],
    ['account re-export', "export * from '../storage/relational-account-store.ts';"],
    ['quota re-export', "export * from '../storage/relational-quota-store.ts';"],
    ...[
      'beginQuotaObservation',
      'recordQuotaObservation',
      'refillQuota',
      'manageQuotaFunding',
      'configureQuotaPolicy',
      'configureGroups',
      'manageMemberQuota',
      'assertGroupCommit',
      'configureAnlasFallback',
      'configureFreeImages',
      'updateLossAlertPolicy',
      'saveAccountQuota',
      'openAccountQuota',
      'applyQuotaEvents',
      'replayQuotaCommit',
      'prepareAccountCommand',
      'completeAccountEnable',
      'ownerAccountCommand',
      'replayAccountCommit',
      'planQuotaObservation',
      'planGenerationCredentialFailure',
      'registerAccountQuota',
    ].map((name) => [`direct ${name}`, `store.${name}(command,input);`]),
    [
      'authenticated command',
      "import { withAuthenticatedMemberSession as run } from '../members/authentication.ts'; run(store,token,env,fn);",
    ],
    ['element acceptance', "inputStore['persistAcceptedInput'](c,a);"],
    [
      'database initialization',
      "import { initializeRelationalStorage as init } from '../storage/relational-storage.ts'; init(raw,'history');",
    ],
  ]) {
    test(`architecture check rejects relational ${name} in ${filename}`, () => {
      const errors = checkSource(filename, source);
      assert.ok(errors.length, name);
      assert.match(errors.join('\n'), /generation.*command|offline import/);
    });
  }
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
      'server/protocols/native-http.ts',
      "import { fairQuotaSummary } from '../quota/fair-share.ts'; fairQuotaSummary(view,member);",
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
    [
      'server/generation/request-queries.ts',
      "import type { RelationalMemberStore } from '../storage/relational-member-store.ts'; type Reader = Pick<RelationalMemberStore, 'readMember'>; function read(store: Reader) { return store.readMember('m'); }",
    ],
    [
      'server/generation/request-queries.ts',
      "import type { RelationalInputStore } from '../storage/relational-input-store.ts'; type Reader = Pick<RelationalInputStore, 'readMemberInput' | 'readMemberAsset'>; function read(store: Reader) { return store.readMemberInput('r','m'); }",
    ],
    [
      'server/protocols/native-http.ts',
      "import { type RelationalInputStore } from '../storage/relational-input-store.ts'; type Reader = Pick<RelationalInputStore, 'readMemberContext'>; function read(inputStore: Reader) { return inputStore.readMemberContext('r','m'); }",
    ],
    [
      'server/protocols/native-http.ts',
      "import { prepareRelationalInput } from '../generation/relational-input.ts';",
    ],
    [
      'server/generation/request-queries.ts',
      "import type { RelationalGenerationStore } from '../storage/relational-generation-store.ts'; type Reader = Pick<RelationalGenerationStore, 'readWallet' | 'readCharge'>; function read(store: Reader) { return store.readCharge('job'); }",
    ],
    [
      'server/generation/request-queries.ts',
      "import type { RelationalQuotaStore } from '../storage/relational-quota-store.ts'; type Reader = Pick<RelationalQuotaStore, 'readAccountQuota'>; function read(store: Reader) { return store.readAccountQuota('account'); }",
    ],
    [
      'server/generation/execution.ts',
      "import { RelationalGenerationStore } from '../storage/relational-generation-store.ts'; store.dispatchGeneration(command,input,controls);",
    ],
    [
      'server/generation/execution.ts',
      "import { recoverRelationalGenerationResults } from '../storage/relational-result-recovery.ts';",
    ],
    [
      'scripts/offline-import.mjs',
      "import { RelationalGenerationStore } from '../server/storage/relational-generation-store.ts'; store.replayGenerationCommit(source);",
    ],
    [
      'server/generation/admission.ts',
      "import { appendRelationalCommit } from '../events/relational-event-store.ts'; appendRelationalCommit(raw,c); inputStore.persistAcceptedInput(c,a);",
    ],
    [
      'scripts/offline-import.mjs',
      "import { replayRelationalCommit } from '../server/events/relational-event-store.ts'; replayRelationalCommit(raw,c); inputStore.replayAcceptedInput(source,cursor);",
    ],
  ]) {
    assert.deepEqual(checkSource(filename, source), [], filename);
  }
});

test('mixed writer modules retain pure named readers and type-only capability descriptions', () => {
  for (const filename of [
    'server/generation/request-queries.ts',
    'server/protocols/native-http.ts',
  ]) {
    assert.deepEqual(
      checkSource(
        filename,
        "import type { JsonStore as Ledger } from '../storage/sqlite-store.ts'; type Shape = Pick<Ledger, 'memberSessionTransaction'>;",
      ),
      [],
    );
    for (const [name, modulePath] of [
      ['summarizeOwnerUsage', '../members/owner-operations.ts'],
      ['ownerAccountConfiguration', '../members/owner-operations.ts'],
      ['readOperatingAccount', '../quota/operating-accounting.ts'],
      ['isOwner', '../members/authentication.ts'],
      ['queueDeadline', '../generation/queue-policy.ts'],
      ['generationJobDeadline', '../generation/execution.ts'],
      ['generationChargeResources', '../generation/accounting.ts'],
      ['freeImageSuccessWarningThreshold', '../quota/free-image-policy.ts'],
      ['memberCharge', '../quota/failure-accounting.ts'],
    ]) {
      assert.deepEqual(
        checkSource(filename, `import { ${name} as read } from '${modulePath}'; read(input);`),
        [],
      );
    }
    for (const [name, modulePath] of [...writerFactories, ...mixedWriterSources]) {
      assert.deepEqual(
        checkSource(filename, `import { type ${name} as Capability } from '${modulePath}';`),
        [],
      );
      assert.deepEqual(checkSource(filename, `export type * from '${modulePath}';`), []);
    }
  }
  for (const [name, modulePath] of [
    ['canonicalJson', '../events/domain-events.ts'],
    ['readCommandReceipt', '../events/event-store.ts'],
    ['createId', '../storage/sqlite-store.ts'],
  ]) {
    assert.deepEqual(
      checkSource(
        'server/protocols/native-http.ts',
        `import { ${name} as read } from '${modulePath}'; read(input);`,
      ),
      [],
    );
  }
});
