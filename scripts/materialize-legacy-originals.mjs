import { createHash } from 'node:crypto';
import { lstat, mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { R2Storage, r2Config } from '../server/storage/r2-storage.ts';
import { PikPakStorage, pikpakConfig } from '../server/storage/pikpak-storage.ts';
import {
  assertReadableStoredResultDescriptor,
  assertStoredResultContent,
  readableResultFile,
} from '../server/storage/result-files.ts';
import {
  readLegacyCutConfiguration,
  readRescuedOriginals,
  writeNewMigrationFile,
} from './migrate-legacy-database.mjs';

const hash = (bytes) => createHash('sha256').update(bytes).digest('hex');
const reject = () => {
  throw new Error('Selected original materialization proof is invalid.');
};

// Explicitly selected new recovery input, never an original remote promise. No DB, upload,
// deletion, alternate transport or retry. Every successful local input remains after a later failure.
export async function materializeLegacyOriginals(options) {
  const stat = await lstat(options.selection);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 16777216) {
    reject();
  }
  const bytes = await readFile(options.selection);
  if (
    !/^[a-f0-9]{64}$/.test(options.selectionSha256 ?? '') ||
    hash(bytes) !== options.selectionSha256
  ) {
    reject();
  }
  const selected = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)),
    configuration = await readLegacyCutConfiguration(
      options.cutConfig,
      options.cutConfigSha256,
      selected,
      Date.now(),
    ),
    selection = await readRescuedOriginals(
      options.selection,
      options.selectionSha256,
      selected,
      configuration,
    ),
    directory = path.dirname(selection.filename);
  if (selection.originals.some((row) => row.file === null && row.r2 === null)) {
    reject();
  }
  let storage, readMethod;
  if (selection.originals.some((row) => row.file === null)) {
    if (selection.readableSource === 'pikpak') {
      const config = pikpakConfig();
      if (!config || selection.namespace !== config.url) {
        reject();
      }
      storage = new PikPakStorage(config);
      readMethod = 'PikPakStorage.get';
    } else {
      const config = r2Config();
      if (!config || selection.namespace !== `r2://${config.accountId}/${config.bucket}`) {
        reject();
      }
      storage = new R2Storage(config);
      readMethod = 'R2Storage.getFile';
    }
  }
  const output = path.resolve(options.output),
    completed = [],
    toolSha256 = hash(await readFile(new URL(import.meta.url)));
  await mkdir(output, { mode: 0o700 });
  let failure;
  try {
    for (let index = 0; index < selection.originals.length; index++) {
      const row = selection.originals[index],
        descriptor = selection.descriptorsByArtifact.get(row.artifactId),
        pending = {
          imageId: row.artifactId,
          operation: descriptor.operation,
          mimeType: descriptor.mimeType,
          file: readableResultFile({
            imageId: row.artifactId,
            operation: descriptor.operation,
            mimeType: descriptor.mimeType,
          }),
          byteLength: row.byteLength,
          sha256: row.sha256,
        },
        file =
          row.file === null
            ? path.join(output, `${index + 1}.bin`)
            : path.resolve(directory, row.file);
      assertReadableStoredResultDescriptor(pending);
      if (row.file === null) {
        if (!row.r2 || !storage) {
          reject();
        }
        const image = { ...row.r2, byteLength: row.byteLength, sha256: row.sha256 };
        if (selection.readableSource === 'pikpak') {
          await writeNewMigrationFile(file, await storage.get(image));
        } else {
          await storage.getFile(image, file);
        }
      }
      const info = await lstat(file);
      if (!info.isFile() || info.isSymbolicLink() || info.size !== row.byteLength) {
        reject();
      }
      // Same MIME/length/SHA owner as B's canonical Local admission, including reused samples.
      assertStoredResultContent(pending, await readFile(file));
      const receipt = {
          format: 'novelai-rescued-original-read-v1',
          toolSha256,
          selectionSha256: selection.selectedSha256,
          sourceSha256: selection.sourceSha256,
          cut: selection.cut,
          auditSha256: selection.auditSha256,
          cutConfigurationSha256: selection.cutConfigurationSha256,
          artifactId: row.artifactId,
          memberId: row.memberId,
          inputId: row.inputId,
          byteLength: row.byteLength,
          sha256: row.sha256,
          method: row.file === null ? readMethod : 'selected-local-reuse',
          namespace: selection.namespace,
          r2: row.r2,
          file,
          readAt: Date.now(),
          isOriginalR2Promise: false,
          previousReceipt: row.downloadReceipt,
        },
        receiptBytes = Buffer.from(JSON.stringify(receipt)),
        receiptFile = path.join(output, `${index + 1}.receipt.private.json`);
      await writeNewMigrationFile(receiptFile, receiptBytes);
      completed.push({
        ...row,
        file,
        sourceArtifactRowPointer: {
          ...row.sourceArtifactRowPointer,
          file: path.resolve(directory, row.sourceArtifactRowPointer.file),
        },
        downloadReceipt: { file: receiptFile, sha256: hash(receiptBytes) },
      });
    }
  } catch (error) {
    failure = error;
  }
  const { filename, selectedSha256, descriptorsByArtifact, readableSource, ...manifest } =
      selection,
    manifestBytes = Buffer.from(
      JSON.stringify({
        ...manifest,
        namespaceReceipt: {
          ...manifest.namespaceReceipt,
          file: path.resolve(directory, manifest.namespaceReceipt.file),
        },
        originals: completed,
      }),
    );
  await writeNewMigrationFile(path.join(output, 'rescued-originals.private.json'), manifestBytes);
  const result = {
    completed: completed.length,
    requested: selection.originals.length,
    byteLength: completed.reduce((sum, row) => sum + row.byteLength, 0),
    manifestSha256: hash(manifestBytes),
  };
  if (failure) {
    Object.defineProperty(failure, 'materializationResult', { value: result });
    throw failure;
  }
  return result;
}

if (process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url) {
  try {
    const names = new Map([
        ['selection', 'selection'],
        ['selection-sha256', 'selectionSha256'],
        ['cut-config', 'cutConfig'],
        ['cut-config-sha256', 'cutConfigSha256'],
        ['output', 'output'],
      ]),
      options = {};
    for (const argument of process.argv.slice(2)) {
      const match = /^--([^=]+)=(.+)$/.exec(argument),
        key = match && names.get(match[1]);
      if (!key || Object.hasOwn(options, key)) {
        reject();
      }
      options[key] = match[2];
    }
    if (Object.keys(options).length !== names.size) {
      reject();
    }
    console.log(JSON.stringify(await materializeLegacyOriginals(options)));
  } catch (error) {
    console.error(
      JSON.stringify({
        code: 'LEGACY_ORIGINAL_MATERIALIZATION_FAILED',
        completed: error?.materializationResult?.completed ?? 0,
      }),
    );
    process.exitCode = 1;
  }
}
