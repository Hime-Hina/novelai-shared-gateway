import { readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import ts from 'typescript';

const pure = new Set(['server/quota/resource-amounts.ts', 'server/events/quota-projections.ts']);
const assembly = new Set(['server/index.ts', 'executor/start.mjs', 'executor/gateway-handler.ts']);
// Shared transport guards contain no database or command assembly.
const webTransport = new Set(['server/http/api-surface.ts', 'server/generation/payload-budget.ts']);
const mutations = new Set([
  'configureFairQuota',
  'configureFairGroups',
  'configureAnlasFallback',
  'configureFreeImages',
  'updateLossAlertPolicy',
  'configureQuotaPolicy',
  'configureGroups',
  'assertGroupCommit',
  'manageFairMember',
  'applyFairMemberChange',
  'manageMemberQuota',
  'assertMemberRemovalCommit',
  'manageOperatingFunding',
  'applyOperatingFunding',
  'syncFairQuota',
  'syncBoundFairQuota',
  'reserveFairQuota',
  'settleFairQuota',
  'recordDomainEvent',
  'beginDomainCommand',
  'appendDomainCommit',
  'appendRelationalCommit',
  'replayRelationalCommit',
  'initializeRelationalStorage',
  'persistAcceptedInput',
  'stageGenerationDiagnostic',
  'initializeGatewayConfiguration',
  'stageNormalizationDefaultsProfile',
  'createNormalizationDefaultsProjector',
  'createConfigurationProjector',
  'stageGenerationReuse',
  'replayAcceptedInput',
  'memberSessionTransaction',
  'discordAuthenticationTransaction',
  'createMemberDiscordInvite',
  'saveAuthentication',
  'handleMemberAuthenticationRoutes',
  'handleMemberDiscordAuthenticationRoutes',
  'renewMemberCookie',
  'createRelationalMemberAuthenticationHttp',
  'createRelationalMemberReadHttp',
  'createRelationalNativeAdmission',
  'createRelationalApiGenerationAdmission',
  'createRelationalOpenAiHttp',
  'createRelationalNativeHttp',
  'createLegacyNativeHttp',
  'memberApiKeyTransaction',
  'withAuthenticatedMemberSession',
  'withAuthenticatedMemberApiKey',
  'registerMember',
  'setMemberCredentials',
  'replaceMemberApiKey',
  'setMemberStatus',
  'setMemberAdmission',
  'changeMemberAdmission',
  'assertMemberAdmissionCommit',
  'replayMemberCommit',
  'acceptGeneration',
  'reuseGeneration',
  'createRelationalMemberGenerationAdmission',
  'createRelationalMemberGenerationHttp',
  'createLegacyMemberGenerationHttp',
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
  'recoverRelationalGenerationResults',
  'registerStorageTarget',
  'beginArtifactReplica',
  'recordArtifactReplica',
  'applyReplicaEvents',
  'replayReplicaCommit',
  'copyRelationalArtifactReplica',
  'verifyRelationalArtifactReplica',
  'planLocalArtifactPublication',
  'retireJobHistory',
  'retireArtifactHistory',
  'beginQuotaObservation',
  'createRelationalAccountQuotaRefresh',
  'recordQuotaObservation',
  'refillQuota',
  'manageQuotaFunding',
  'saveAccountQuota',
  'openAccountQuota',
  'applyQuotaEvents',
  'replayQuotaCommit',
  'restoreRelationalPrivateSnapshot',
  'verifyAndInstallRelationalStorage',
  'prepareAccountCommand',
  'completeAccountEnable',
  'ownerAccountCommand',
  'ownerOperation',
  'replayAccountCommit',
  'planQuotaObservation',
  'planGenerationCredentialFailure',
  'registerAccountQuota',
  'createInputProjector',
  'createDiagnosticProjector',
  'createGenerationProjector',
  'createQuotaProjector',
  'applyAccountEvent',
  'applyReplayedMemberEvent',
  'replayCommit',
  'appendCommit',
]);
const persistence = new Set([
  'update',
  'write',
  'replaceAll',
  'persistIncremental',
  'persistDirtyRows',
  'persistDomainFacts',
  'loadItemForUpdate',
  'loadJobForUpdate',
  'sqlite',
  'statements',
  'db',
]);
const slash = (value) => value.replaceAll('\\', '/');

// These are source boundaries, not an OS sandbox for an agent that can edit this check.
export function checkSource(filename, text) {
  filename = slash(filename);
  if (filename.endsWith('.vue')) {
    let scripts = text.replace(/[^\n\r]/g, ' ');
    for (const match of text.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)) {
      const start = match.index + match[0].indexOf('>') + 1;
      scripts = scripts.slice(0, start) + match[1] + scripts.slice(start + match[1].length);
    }
    text = scripts;
  }
  const source = ts.createSourceFile(filename, text, ts.ScriptTarget.Latest, true);
  const errors = [];
  const query = filename.startsWith('server/') && filename.endsWith('-queries.ts');
  const protocol = filename.startsWith('server/protocols/');
  const web = filename.startsWith('web/');
  const report = (node, dependency, alternative) => {
    const { line } = source.getLineAndCharacterOfPosition(node.getStart(source));
    errors.push(`${filename}:${line + 1}: ${dependency}; use ${alternative}.`);
  };
  function dependency(node, specifier, names = []) {
    const target = specifier.startsWith('.')
      ? path.posix.normalize(path.posix.join(path.posix.dirname(filename), specifier))
      : specifier;
    if (pure.has(filename) && !pure.has(target)) {
      report(
        node,
        `pure reducer/amount runtime dependency ${specifier}`,
        'facts passed by the command owner; IO belongs in event-store/account-refresh',
      );
    }
    if (
      web &&
      !webTransport.has(target) &&
      (target.startsWith('server/') || /novelai-shared-gateway\/server\//.test(target))
    ) {
      report(
        node,
        `web backend runtime dependency ${specifier}`,
        'the executor HTTP protocol (type-only DTO imports are allowed)',
      );
    }
    if (/\bgateway-runtime\.(?:ts|js)$/.test(target) && !assembly.has(filename)) {
      report(
        node,
        `reverse runtime dependency ${specifier}`,
        'an owning generation/quota/protocol module; only index and executor assembly may import runtime',
      );
    }
    if (
      ((query || protocol) &&
        (names.some((name) => mutations.has(name)) ||
          // 混合 owner 的整模块含已登记写命令；明确的纯 named reader 继续允许。
          (/(?:quota\/(?:fair-share|operating-accounting|free-image-policy|failure-accounting)|members\/(?:owner-operations|authentication)|generation\/(?:queue-policy|execution|accounting)|events\/(?:event-store|domain-events)|storage\/sqlite-store)\.(?:ts|js)$/.test(
            target,
          ) &&
            names.includes('*')) ||
          /(?:events\/relational-event-store|members\/(?:(?:relational-)?authentication-http|relational-read-http)|generation\/(?:relational-(?:member-(?:admission|http)|native-(?:admission|http)|api-admission|openai-http|queue-runtime)|legacy-(?:member|native)-http)|quota\/relational-account-refresh|storage\/relational-(?:configuration-store|input-store|member-store|generation-store|quota-store|account-store|result-recovery|private-restore|replay|replica-store|replica-io|ready|storage))\.(?:ts|js)$/.test(
            target,
          ))) ||
      (query &&
        /(?:storage\/sqlite-store|events\/(?:event-store|domain-events))\.(?:ts|js)$/.test(target))
    ) {
      report(
        node,
        `adapter/query write capability ${specifier} (${names.join(', ')})`,
        'a type-only read-only Pick of the owning store; mutations belong in generation commands or offline imports',
      );
    }
  }
  function visit(node) {
    if (ts.isImportDeclaration(node)) {
      const clause = node.importClause;
      const bindings = clause?.namedBindings;
      const named =
        bindings && ts.isNamedImports(bindings)
          ? bindings.elements.filter((item) => !item.isTypeOnly)
          : null;
      if (!clause?.isTypeOnly && !(named?.length === 0 && !clause?.name)) {
        dependency(
          node,
          node.moduleSpecifier.text,
          named ? named.map((item) => (item.propertyName || item.name).text) : ['*'],
        );
      }
    } else if (ts.isExportDeclaration(node) && node.moduleSpecifier && !node.isTypeOnly) {
      const named =
        node.exportClause && ts.isNamedExports(node.exportClause)
          ? node.exportClause.elements.filter((item) => !item.isTypeOnly)
          : null;
      if (!named || named.length) {
        dependency(
          node,
          node.moduleSpecifier.text,
          named ? named.map((item) => (item.propertyName || item.name).text) : ['*'],
        );
      }
    } else if (ts.isCallExpression(node)) {
      const callee = node.expression;
      let name = '';
      if (ts.isIdentifier(callee)) {
        name = callee.text;
      } else if (ts.isPropertyAccessExpression(callee)) {
        name = callee.name.text;
      } else if (
        ts.isElementAccessExpression(callee) &&
        ts.isStringLiteralLike(callee.argumentExpression)
      ) {
        name = callee.argumentExpression.text;
      }
      if (
        (callee.kind === ts.SyntaxKind.ImportKeyword || name === 'require') &&
        node.arguments[0] &&
        ts.isStringLiteralLike(node.arguments[0])
      ) {
        dependency(node, node.arguments[0].text, ['*']);
      }
      if (pure.has(filename) && ['fetch', 'require'].includes(name)) {
        report(node, `pure reducer/amount IO call ${name}`, 'observations passed as event facts');
      }
      const storeCall =
        ts.isPropertyAccessExpression(callee) &&
        /(?:^|\.)store$/i.test(callee.expression.getText(source));
      if ((query || protocol) && (mutations.has(name) || (storeCall && persistence.has(name)))) {
        report(
          node,
          `adapter/query write call ${name}`,
          'the existing generation admission/execution command',
        );
      }
    }
    if (
      (query || protocol) &&
      ts.isPropertyAccessExpression(node) &&
      ['sqlite', 'statements', 'db'].includes(node.name.text)
    ) {
      report(node, `raw store access ${node.name.text}`, 'a read-only store method');
    }
    ts.forEachChild(node, visit);
  }
  visit(source);
  return errors;
}

export function checkArchitecture(root = process.cwd()) {
  const errors = [];
  function walk(relative) {
    for (const entry of readdirSync(path.join(root, relative), { withFileTypes: true })) {
      if (['node_modules', 'tests', '.nuxt', '.output', 'dist'].includes(entry.name)) {
        continue;
      }
      const filename = `${relative}/${entry.name}`;
      if (entry.isDirectory()) {
        walk(filename);
      } else if (/\.(?:ts|js|mjs|vue)$/.test(entry.name) && !entry.name.endsWith('.d.ts')) {
        errors.push(...checkSource(filename, readFileSync(path.join(root, filename), 'utf8')));
      }
    }
  }
  for (const directory of ['server', 'web', 'executor']) {
    walk(directory);
  }
  return errors;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const errors = checkArchitecture();
  if (errors.length) {
    console.error(errors.join('\n'));
    process.exitCode = 1;
  } else {
    console.log('Architecture boundaries passed.');
  }
}
