import hashlib
import importlib.util
import json
import shutil
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('backup_production', Path(__file__).resolve().parents[2] / 'scripts/backup-production.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


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
            db.execute('INSERT INTO images VALUES (?)', (json.dumps({'file': 'images/one.png',
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
        old = self.run_backup(keep=1)
        latest = self.run_backup(keep=1)
        self.assertFalse(old.exists())
        self.assertTrue(pinned.exists())
        module.verify(latest)

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


if __name__ == '__main__':
    unittest.main()
