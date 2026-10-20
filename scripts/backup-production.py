#!/usr/bin/env python3
"""Verified business-data snapshots. Images are archived separately in private R2 storage."""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import sqlite3
import subprocess
import tempfile
import tarfile


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def safe_file(root, relative):
    parts = PurePosixPath(relative).parts
    if not parts or '\\' in relative or PurePosixPath(relative).is_absolute() or '..' in parts:
        raise ValueError('Unsafe backup file path')
    path = root
    for part in parts:
        path = path / part
        if path.is_symlink():
            raise ValueError('Symlinks are not backup contents')
    if not path.is_file():
        raise ValueError('Required backup file is missing')
    return path


def verify(snapshot):
    manifest = json.loads((snapshot / 'manifest.json').read_text())
    for relative, expected in manifest['files'].items():
        if digest(safe_file(snapshot, relative)) != expected:
            raise ValueError('Backup checksum mismatch')
    with closing(sqlite3.connect((snapshot / 'data/library.sqlite').as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
        if db.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise ValueError('Backup database is not intact')
        for (raw,) in db.execute('SELECT data FROM images') if manifest.get('imagesIncluded', True) else []:
            image = json.loads(raw)
            if not image.get('file'):  # Legacy inline images remain in SQLite.
                continue
            relative = 'data/' + image['file']
            remote = manifest.get('r2Images', {}).get(image['file'])
            if remote:
                if not remote.get('verifiedAt') or not re.fullmatch(r'images/[A-Za-z0-9_-]{1,128}/[a-f0-9]{64}', remote.get('key', '')):
                    raise ValueError('Invalid archived image receipt')
                if image.get('sha256') and remote.get('sha256') != image['sha256']:
                    raise ValueError('Archived image checksum mismatch')
                if image.get('byteLength') is not None and remote.get('byteLength') != image['byteLength']:
                    raise ValueError('Archived image length mismatch')
                continue
            if PurePosixPath(image['file']).parts[0] != 'images' or relative not in manifest['files']:
                raise ValueError('Referenced image is absent from backup')
            file = safe_file(snapshot, relative)
            if image.get('sha256') and manifest['files'][relative] != image['sha256']:
                raise ValueError('Image differs from its stored result descriptor')
            if image.get('byteLength') is not None and file.stat().st_size != image['byteLength']:
                raise ValueError('Image length differs from its stored result descriptor')
    return manifest


def clean_archived_images(source, destination):
    source, destination = source.resolve(), destination.resolve()
    if source == destination or source in destination.parents or destination in source.parents:
        raise ValueError('Backup destination must be outside live data')
    with closing(sqlite3.connect((source / 'library.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
        images = [json.loads(row[0]) for row in db.execute('SELECT data FROM images')]
    archived = {image['file']: image['r2'] for image in images
                if image.get('file') and image.get('r2', {}).get('verifiedAt')}
    removed, released = 0, 0
    for snapshot in sorted(destination.iterdir()):
        if snapshot.is_symlink() or not snapshot.is_dir() or not snapshot.name.startswith(('daily-', 'cutover-')):
            continue
        if not (snapshot / 'manifest.json').is_file():
            continue
        manifest = verify(snapshot)
        external = manifest.setdefault('r2Images', {})
        for relative, checksum in list(manifest['files'].items()):
            if not relative.startswith('data/images/'):
                continue
            image_file = relative.removeprefix('data/')
            remote = archived.get(image_file)
            if remote and remote.get('sha256') == checksum:
                local = safe_file(snapshot, relative)
                if local.stat().st_size == remote.get('byteLength'):
                    external[image_file] = remote
                    del manifest['files'][relative]
        # Publish durable restore locations before removing any duplicate bytes; interrupted cleanup can resume.
        temporary = snapshot / '.manifest-r2.tmp'
        with temporary.open('w') as stream:
            json.dump(manifest, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(snapshot / 'manifest.json')
        directory = os.open(snapshot, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        for image_file, remote in external.items():
            relative = 'data/' + image_file
            candidate = snapshot / relative
            if not candidate.exists() and not candidate.is_symlink():
                continue
            file = safe_file(snapshot, relative)
            if not relative.startswith('data/images/') or digest(file) != remote['sha256']:
                raise ValueError('Refusing to remove an unverified backup image')
            released += file.stat().st_size
            file.unlink()
            removed += 1
        verify(snapshot)
    return removed, released


def backup(source, destination, configuration, *, label='daily', keep=7):
    source, destination = source.resolve(), destination.resolve()
    if source == destination or source in destination.parents or destination in source.parents:
        raise ValueError('Backup destination must be outside live data')
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(destination, 0o700)
    work = Path(tempfile.mkdtemp(prefix='.partial-', dir=destination))
    try:
        (work / 'data').mkdir(mode=0o700)
        with closing(sqlite3.connect((source / 'library.sqlite').as_uri() + '?mode=ro', uri=True)) as live:
            with closing(sqlite3.connect(work / 'data/library.sqlite')) as snapshot:
                live.backup(snapshot)
        for relative, original in configuration.items():
            output = work / relative
            output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            shutil.copyfile(original, output)
        files = {}
        for file in sorted(work.rglob('*')):
            if file.is_symlink():
                raise ValueError('Symlinks are not backup contents')
            if file.is_file():
                os.chmod(file, 0o600)
                files[file.relative_to(work).as_posix()] = digest(file)
                with file.open('rb') as stream:
                    os.fsync(stream.fileno())
        stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
        manifest = {'createdAt': stamp, 'source': str(source), 'imagesIncluded': False, 'files': files}
        with (work / 'manifest.json').open('x') as stream:
            json.dump(manifest, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        verify(work)
        result = destination / (label + '-' + stamp)
        work.rename(result)
        directory = os.open(destination, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        # Only this script's completed daily snapshots expire. Cutover/legacy backups stay pinned.
        daily = sorted(p for p in destination.glob('daily-*') if not p.is_symlink() and (p / 'manifest.json').is_file())
        for old in daily[:-keep]:
            shutil.rmtree(old)
        return result
    finally:
        if work.exists():
            shutil.rmtree(work)


def production_configuration():
    config = Path('/etc/novelai-shared-gateway')
    if not (config / 'gateway.env').is_file():
        raise ValueError('Production configuration is missing')
    files = {'config/' + p.name: p for p in config.iterdir() if p.is_file()}
    credentials = Path('/var/lib/novelai-shared-gateway/bootstrap-credentials.json')
    if credentials.is_file():
        files['data/bootstrap-credentials.json'] = credentials
    units = Path('/etc/systemd/system')
    for name in ['novelai-gateway.service', 'novelai-gateway-web.service', 'novelai-gateway-backup.service', 'novelai-gateway-backup.timer']:
        if (units / name).is_file():
            files['systemd/' + name] = units / name
        for p in (units / (name + '.d')).glob('*.conf'):
            files['systemd/' + name + '.d/' + p.name] = p
    root = Path('/opt/novelai-shared-gateway')
    for relative in ['current/RELEASE.txt', 'app/server/index.js', 'backup.py']:
        if (root / relative).is_file():
            files['runtime/' + relative.replace('/', '_')] = root / relative
    return files


def upload_business_backup(snapshot):
    env_file = Path('/etc/novelai-shared-gateway/r2.env')
    key_file = Path('/etc/novelai-shared-gateway/backup-encryption.key')
    if not env_file.exists():
        return
    if not key_file.is_file():
        raise ValueError('Backup encryption key is missing')
    verify(snapshot)
    runtime = Path('/opt/novelai-shared-gateway/current')
    with tempfile.TemporaryDirectory(prefix='.r2-upload-', dir=snapshot.parent) as temporary:
        archive = Path(temporary) / 'business.tar.gz'
        with tarfile.open(archive, 'w:gz') as bundle:
            bundle.add(snapshot, arcname=snapshot.name)
        subprocess.run([str(runtime / 'bin/node'),
                        str(runtime / 'executor/.output/server/node_modules/novelai-shared-gateway/scripts/r2-backup.mjs'),
                        'upload', str(archive), str(snapshot / 'r2-backup.json'),
                        str(env_file), str(key_file), snapshot.name],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify', type=Path, help='Read and verify one existing snapshot only')
    parser.add_argument('--label', choices=['daily', 'cutover'], default='daily')
    parser.add_argument('--clean-archived-images', action='store_true', help='Remove only backup image copies with verified live R2 receipts')
    args = parser.parse_args()
    os.umask(0o077)
    if args.verify:
        manifest = verify(args.verify.resolve())
        print('Backup verified:', len(manifest['files']), 'files')
    elif args.clean_archived_images:
        count, size = clean_archived_images(Path('/var/lib/novelai-shared-gateway'), Path('/var/backups/novelai-shared-gateway'))
        print('Verified duplicate backup images removed:', count, 'logical bytes:', size)
    else:
        result = backup(Path('/var/lib/novelai-shared-gateway'), Path('/var/backups/novelai-shared-gateway'),
                        production_configuration(), label=args.label)
        print('Business backup verified (images excluded):', result.name, flush=True)
        if args.label == 'daily':
            upload_business_backup(result)
            if (result / 'r2-backup.json').is_file():
                print('Encrypted R2 business backup verified:', result.name)
            count, size = clean_archived_images(Path('/var/lib/novelai-shared-gateway'), result.parent)
            print('Verified duplicate backup images removed:', count, 'logical bytes:', size)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Config files and SQL rows contain credentials; do not log exception arguments.
        print('Backup failed:', type(error).__name__, flush=True)
        raise SystemExit(1)
