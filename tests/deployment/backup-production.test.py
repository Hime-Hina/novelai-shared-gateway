import hashlib
import importlib.util
import json
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

    def test_complete_restore_checksums_and_deduplication(self):
        first = self.run_backup()
        second = self.run_backup()
        module.verify(second)
        self.assertEqual((second / 'config/gateway.env').read_text(), self.config.read_text())
        self.assertEqual((first / 'data/images/one.png').stat().st_ino, (second / 'data/images/one.png').stat().st_ino)
        self.assertNotEqual(self.image.stat().st_ino, (second / 'data/images/one.png').stat().st_ino)
        self.assertEqual((second / 'config/gateway.env').stat().st_mode & 0o777, 0o600)
        self.image.write_bytes(b'changed live image')
        module.verify(second)
        (second / 'config/gateway.env').write_text('tampered')
        with self.assertRaises(ValueError): module.verify(second)

    def test_missing_image_or_unsafe_record_does_not_publish(self):
        self.image.unlink()
        with self.assertRaises(ValueError): self.run_backup()
        self.assertEqual(list(self.destination.iterdir()), [])
        with closing(sqlite3.connect(self.source / 'library.sqlite')) as db, db:
            db.execute('UPDATE images SET data=?', (json.dumps({'file': '../gateway.env'}),))
        with self.assertRaises(ValueError): self.run_backup()
        self.assertEqual(list(self.destination.iterdir()), [])

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


if __name__ == '__main__':
    unittest.main()
