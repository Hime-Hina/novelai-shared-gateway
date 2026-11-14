import { readFile, writeFile, readdir, rm, lstat } from 'node:fs/promises';
import { isBuiltin } from 'node:module';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import ts from 'typescript';

export async function packageBackend(root = fileURLToPath(new URL('../', import.meta.url))) {
  const output = path.resolve(root, '.output/backend'),
    manifest = JSON.parse(await readFile(path.join(root, 'package.json'), 'utf8')),
    pending = [
      'server/index.js',
      'scripts/bootstrap.mjs',
      'scripts/restore-relational.mjs',
      'scripts/r2-backup.mjs',
      'scripts/mock-novelai.mjs',
    ],
    files = new Set();
  while (pending.length) {
    const name = path.normalize(pending.pop());
    if (files.has(name)) continue;
    const filename = path.resolve(output, name);
    if (!filename.startsWith(output + path.sep) || !/\.(?:[cm]?js|json)$/.test(filename)) {
      throw new Error(`Invalid packaged runtime dependency: ${name}`);
    }
    if (!(await lstat(filename)).isFile()) {
      throw new Error(`Runtime dependency is not a regular file: ${name}`);
    }
    const source = await readFile(filename, 'utf8');
    files.add(name);
    if (name.endsWith('.json')) continue;
    const parsed = ts.createSourceFile(
      name,
      source,
      ts.ScriptTarget.Latest,
      true,
      ts.ScriptKind.JS,
    );
    if (parsed.parseDiagnostics.length) throw new Error(`Invalid emitted JavaScript: ${name}`);
    const dependency = (specifier) => {
      if (specifier.startsWith('.')) {
        pending.push(path.relative(output, path.resolve(path.dirname(filename), specifier)));
      } else if (!isBuiltin(specifier)) {
        const packageName = specifier.startsWith('@')
          ? specifier.split('/').slice(0, 2).join('/')
          : specifier.split('/')[0];
        if (!Object.hasOwn(manifest.dependencies, packageName)) {
          throw new Error(`Runtime dependency is not a production dependency: ${specifier}`);
        }
      }
    };
    const visit = (node) => {
      if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) {
        if (node.moduleSpecifier) dependency(node.moduleSpecifier.text);
      } else if (
        ts.isCallExpression(node) &&
        (node.expression.kind === ts.SyntaxKind.ImportKeyword ||
          (ts.isIdentifier(node.expression) && node.expression.text === 'require'))
      ) {
        if (node.arguments.length !== 1 || !ts.isStringLiteralLike(node.arguments[0])) {
          throw new Error(`Runtime import must have a literal dependency: ${name}`);
        }
        dependency(node.arguments[0].text);
      } else if (
        ts.isNewExpression(node) &&
        ts.isIdentifier(node.expression) &&
        node.expression.text === 'URL' &&
        node.arguments?.length === 2 &&
        ts.isPropertyAccessExpression(node.arguments[1]) &&
        node.arguments[1].name.text === 'url' &&
        ts.isMetaProperty(node.arguments[1].expression) &&
        node.arguments[1].expression.keywordToken === ts.SyntaxKind.ImportKeyword
      ) {
        if (!ts.isStringLiteralLike(node.arguments[0])) {
          throw new Error(`Runtime URL must have a literal dependency: ${name}`);
        }
        if (/\.(?:[cm]?js|json|ts)$/.test(node.arguments[0].text))
          dependency(node.arguments[0].text);
      }
      ts.forEachChild(node, visit);
    };
    visit(parsed);
  }
  // Type-only source dependencies are still emitted by tsc; only runtime edges belong in this package.
  const prune = async (directory) => {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const filename = path.join(directory, entry.name);
      if (entry.isDirectory()) {
        await prune(filename);
        if (!(await readdir(filename)).length) await rm(filename, { recursive: true });
      } else if (!files.has(path.relative(output, filename))) {
        await rm(filename);
      }
    }
  };
  await prune(output);
  manifest.main = 'server/index.js';
  manifest.scripts = {
    start: 'node server/index.js',
    bootstrap: 'node scripts/bootstrap.mjs',
    restore: 'node scripts/restore-relational.mjs',
  };
  delete manifest.devDependencies;
  await writeFile(path.join(output, 'package.json'), JSON.stringify(manifest, null, 2) + '\n');
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  await packageBackend();
}
