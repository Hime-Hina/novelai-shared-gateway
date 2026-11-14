import hashlib
import importlib.util
import json
import os
import shutil
from contextlib import closing
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import quote

spec = importlib.util.spec_from_file_location('backup_production', Path(__file__).resolve().parents[2] / 'scripts/backup-production.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
REPOSITORY = Path(__file__).resolve().parents[2]
NODE = os.environ.get('NODE_BINARY', 'node')


def database_state(filename):
    with closing(sqlite3.connect(filename.as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
        tables = db.execute("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name").fetchall()
        state = {}
        for (name,) in tables:
            rows = sorted(repr(row) for row in db.execute('SELECT * FROM "' + name.replace('"', '""') + '"'))
            state[name] = {'count': len(rows), 'sha256': hashlib.sha256(repr(rows).encode()).hexdigest()}
        return state


class BackupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.source, self.destination = root / 'live', root / 'backups'
        (self.source / 'images').mkdir(parents=True)
        self.image = self.source / 'images/one.png'
        self.image.write_bytes(b'image fixture')
        self.config = root / 'gateway.env'
        self.config.write_text('SECRET=fixture-only')
        with closing(sqlite3.connect(self.source / 'library.sqlite')) as db, db:
            db.execute('CREATE TABLE images (data TEXT)')
            db.execute('INSERT INTO images VALUES (?)', (json.dumps({'file': 'images/one.png', 'r2': None,
                'sha256': hashlib.sha256(self.image.read_bytes()).hexdigest(), 'byteLength': self.image.stat().st_size}),))

    def run_backup(self, **options):
        return module.backup(self.source, self.destination, {'config/gateway.env': self.config}, **options)

    def test_business_backup_preserves_database_and_config_without_image_copies(self):
        first = self.run_backup()
        second = self.run_backup()
        module.verify(second)
        self.assertEqual((second / 'config/gateway.env').read_text(), self.config.read_text())
        self.assertFalse((second / 'data/images').exists())
        self.assertFalse(json.loads((second / 'manifest.json').read_text())['imagesIncluded'])
        with closing(sqlite3.connect(second / 'data/library.sqlite')) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM images').fetchone()[0], 1)
        self.assertEqual((second / 'config/gateway.env').stat().st_mode & 0o777, 0o600)
        self.image.write_bytes(b'changed live image')
        module.verify(second)
        (second / 'config/gateway.env').write_text('tampered')
        with self.assertRaises(ValueError): module.verify(second)

    def test_remote_or_missing_local_images_do_not_break_business_backups(self):
        self.image.unlink()
        snapshot = self.run_backup()
        module.verify(snapshot)
        self.assertFalse((snapshot / 'data/images').exists())

    def test_legacy_full_image_backup_still_checks_missing_images(self):
        snapshot = self.run_backup()
        manifest = json.loads((snapshot / 'manifest.json').read_text())
        del manifest['imagesIncluded']
        (snapshot / 'manifest.json').write_text(json.dumps(manifest))
        with self.assertRaises(ValueError): module.verify(snapshot)

    def test_retention_preserves_cutover_and_latest(self):
        pinned = self.run_backup(label='cutover')
        daily = [self.run_backup() for _ in range(8)]
        old, latest = daily[0], daily[-1]

        def upload(snapshot):
            self.assertEqual(snapshot, latest)
            self.assertTrue(old.exists(), 'Remote upload must precede expiry')
            (snapshot / 'r2-backup.json').write_text('{}')

        with (
            patch.object(module, 'backup', return_value=latest),
            patch.object(module, 'production_configuration', return_value={}),
            patch.object(module, 'upload_business_backup', side_effect=upload),
            patch('sys.argv', ['backup-production.py']),
        ):
            module.main()
        self.assertFalse(old.exists())
        self.assertTrue(pinned.exists())
        self.assertEqual(len(list(self.destination.glob('daily-*'))), 7)
        module.verify(latest)

    def test_failed_remote_upload_keeps_all_previous_local_backups(self):
        daily = [self.run_backup() for _ in range(8)]
        failures = (RuntimeError('synthetic upload failure'), subprocess.CalledProcessError(2, ['node']))
        for failure in failures:
            with (
                self.subTest(failure=type(failure).__name__),
                patch.object(module, 'backup', return_value=daily[-1]),
                patch.object(module, 'production_configuration', return_value={}),
                patch.object(module, 'upload_business_backup', side_effect=failure),
                patch('sys.argv', ['backup-production.py']),
            ):
                with self.assertRaises(type(failure)):
                    module.main()
                self.assertTrue(all(snapshot.exists() for snapshot in daily))

    def test_refuses_destination_inside_live_data(self):
        with self.assertRaises(ValueError):
            module.backup(self.source, self.source / 'backups', {})

    def test_cleanup_only_removes_verified_duplicate_and_keeps_restore_receipt(self):
        snapshot = self.run_backup(label='cutover')
        (snapshot / 'data/images').mkdir()
        copied = snapshot / 'data/images/one.png'
        shutil.copyfile(self.image, copied)
        manifest = json.loads((snapshot / 'manifest.json').read_text())
        manifest['imagesIncluded'] = True
        manifest['files']['data/images/one.png'] = module.digest(copied)
        (snapshot / 'manifest.json').write_text(json.dumps(manifest))
        self.assertEqual(module.clean_archived_images(self.source, self.destination), (0, 0))
        self.assertTrue(copied.exists())
        with closing(sqlite3.connect(self.source / 'library.sqlite')) as db, db:
            image = json.loads(db.execute('SELECT data FROM images').fetchone()[0])
            image['r2'] = {'bucket': 'synthetic-images', 'key': 'images/img_one/' + image['sha256'],
                          'sha256': image['sha256'], 'byteLength': image['byteLength'], 'verifiedAt': '2026-10-21T00:00:00Z'}
            db.execute('UPDATE images SET data=?', (json.dumps(image),))
        self.assertEqual(module.clean_archived_images(self.source, self.destination), (1, self.image.stat().st_size))
        self.assertFalse(copied.exists())
        self.assertTrue(self.image.exists(), 'live originals must never be cleaned here')
        self.assertEqual(module.verify(snapshot)['r2Images']['images/one.png'], image['r2'])
        self.assertEqual(module.clean_archived_images(self.source, self.destination), (0, 0))

    def test_default_daily_with_real_ready_database_and_verified_encrypted_transport(self):
        root = Path(self.temp.name)
        live, destination, config, cloud_fixture = (root / name for name in ('relational-live', 'relational-backups', 'config', 'transport'))
        config.mkdir()
        # Real offline bootstrap and the existing R2 transport fixture; neither backup nor cleanup is mocked.
        subprocess.run([NODE, '--input-type=module', '-e', '''
            import path from 'node:path';
            import { pathToFileURL } from 'node:url';
            import { mkdir, copyFile, writeFile } from 'node:fs/promises';
            const [repo, root] = process.argv.slice(1);
            const { bootstrap } = await import(pathToFileURL(path.join(repo, 'scripts/bootstrap.mjs')));
            const { backupFixture } = await import(pathToFileURL(path.join(repo, 'tests/helpers/r2-backup-fixture.mjs')));
            const live = path.join(root, 'relational-live'), config = path.join(root, 'config');
            await bootstrap({ env: { DATA_DIR: live, ADMIN_TOKEN: 'synthetic-backup-admin-'.repeat(3),
                NOVELAI_TOKEN: 'pst-' + 'synthetic-backup-token-'.repeat(3) }, log() {} });
            const transport = path.join(root, 'transport');
            await mkdir(transport);
            const fixture = await backupFixture({ after() {} }, transport);
            await copyFile(fixture.envFile, path.join(config, 'r2.env'));
            await copyFile(fixture.keyFile, path.join(config, 'backup-encryption.key'));
            await writeFile(path.join(config, 'gateway.env'), 'PUBLIC=1\\n');
        ''', str(REPOSITORY), str(root)], check=True, stdout=subprocess.DEVNULL)
        filename = live / 'library.sqlite'
        before_sha, before = module.digest(filename), database_state(filename)
        self.assertNotIn('images', before, 'The fixture must reach the new-format failure boundary')
        self.assertGreater(before['commits']['count'], 0)
        self.assertGreater(before['normalization_contexts']['count'], 0)
        self.assertEqual(before['password_credentials']['count'], 4)
        self.assertEqual(before['api_keys']['count'], 4)
        self.assertEqual(before['upstream_credentials']['count'], 1)
        with closing(sqlite3.connect(filename.as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
            history, cut, status = db.execute('SELECT history_id, last_commit_seq, status FROM projection_state').fetchone()
        self.assertEqual(status, 'ready')
        self.assertGreater(cut, 0)

        # Redirect production filesystem capabilities into this private fixture, preserving the real main flow.
        original_path, original_run = Path, subprocess.run
        paths = {
            '/var/lib/novelai-shared-gateway': live,
            '/var/backups/novelai-shared-gateway': destination,
            '/etc/novelai-shared-gateway': config,
            '/etc/systemd/system': root / 'systemd',
            '/opt/novelai-shared-gateway': root / 'runtime',
        }

        def production_path(value):
            value = str(value)
            for prefix, replacement in paths.items():
                if value == prefix or value.startswith(prefix + '/'):
                    return replacement / value[len(prefix):].lstrip('/')
            return original_path(value)

        uploads = []

        def upload_transport(command, **options):
            self.assertEqual(command[2], 'upload')
            self.assertTrue(command[1].endswith('/scripts/r2-backup.mjs'))
            uploads.append(command[2])
            # Only HTTP transport is replaced. Encryption, streaming upload and verified download are real owners.
            return original_run([NODE, '--import', str(cloud_fixture / 'preload.mjs'),
                str(REPOSITORY / 'scripts/r2-backup.mjs'), *command[2:]], **options)

        with (patch.object(module, 'Path', side_effect=production_path),
              patch.object(module.subprocess, 'run', side_effect=upload_transport),
              patch('sys.argv', ['backup-production.py'])):
            module.main()
        self.assertEqual(uploads, ['upload'])
        snapshots = list(destination.glob('daily-*'))
        self.assertEqual(len(snapshots), 1)
        snapshot = snapshots[0]
        manifest = module.verify(snapshot)
        self.assertFalse(manifest['imagesIncluded'])
        self.assertIn('data/bootstrap-credentials.json', manifest['files'])
        self.assertEqual((snapshot / 'config/gateway.env').read_bytes(), (config / 'gateway.env').read_bytes())
        receipt = json.loads((snapshot / 'r2-backup.json').read_text())
        self.assertEqual(receipt['bucket'], 'synthetic-images')
        encrypted = cloud_fixture / 'cloud' / quote(receipt['key'], safe='')
        self.assertTrue(encrypted.read_bytes().startswith(b'NAIR2B1\0'))
        self.assertEqual(module.digest(encrypted), receipt['sha256'])
        self.assertEqual(encrypted.stat().st_size, receipt['byteLength'])
        self.assertEqual(database_state(snapshot / 'data/library.sqlite'), before)
        self.assertEqual(database_state(filename), before)
        self.assertEqual(module.digest(filename), before_sha)

        # Full public replay + selected current-private restore + the unique installer, without setting ready by hand.
        backup_database, restored = snapshot / 'data/library.sqlite', root / 'restored.sqlite'
        subprocess.run([NODE, str(REPOSITORY / 'scripts/restore-relational.mjs'),
            '--source', str(backup_database), '--sha256', module.digest(backup_database),
            '--history', history, '--cut', str(cut), '--output', str(restored), '--data-dir', str(live)],
            check=True, stdout=subprocess.DEVNULL)
        self.assertEqual(database_state(restored), before)
        self.assertEqual(module.digest(filename), before_sha)


if __name__ == '__main__':
    unittest.main()
