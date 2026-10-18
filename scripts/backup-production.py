#!/usr/bin/env python3
"""Verified SQLite/image snapshots; rsync reuses files from earlier backups, never live data."""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import sqlite3
import subprocess
import tempfile


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
        for (raw,) in db.execute('SELECT data FROM images'):
            image = json.loads(raw)
            if not image.get('file'):  # Legacy inline images remain in SQLite.
                continue
            relative = 'data/' + image['file']
            if PurePosixPath(image['file']).parts[0] != 'images' or relative not in manifest['files']:
                raise ValueError('Referenced image is absent from backup')
            file = safe_file(snapshot, relative)
            if image.get('sha256') and manifest['files'][relative] != image['sha256']:
                raise ValueError('Image differs from its stored result descriptor')
            if image.get('byteLength') is not None and file.stat().st_size != image['byteLength']:
                raise ValueError('Image length differs from its stored result descriptor')
    return manifest


def backup(source, destination, configuration, *, label='daily', keep=7):
    source, destination = source.resolve(), destination.resolve()
    if source == destination or source in destination.parents or destination in source.parents:
        raise ValueError('Backup destination must be outside live data')
    if not (source / 'images').is_dir() or (source / 'images').is_symlink():
        raise ValueError('Expected a real images directory')
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(destination, 0o700)
    previous = sorted(p for p in destination.glob('daily-*') if not p.is_symlink() and (p / 'manifest.json').is_file())
    work = Path(tempfile.mkdtemp(prefix='.partial-', dir=destination))
    try:
        (work / 'data').mkdir(mode=0o700)
        with closing(sqlite3.connect((source / 'library.sqlite').as_uri() + '?mode=ro', uri=True)) as live:
            with closing(sqlite3.connect(work / 'data/library.sqlite')) as snapshot:
                live.backup(snapshot)
        command = ['rsync', '-rtp', '--checksum', '--fsync', '--chmod=Du=rwx,Dgo=,Fu=rw,Fgo=']
        if previous:
            command += ['--link-dest=' + str(previous[-1] / 'data/images')]
        subprocess.run(command + [str(source / 'images') + '/', str(work / 'data/images') + '/'],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
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
        manifest = {'createdAt': stamp, 'source': str(source), 'files': files}
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify', type=Path, help='Read and verify one existing snapshot only')
    parser.add_argument('--label', choices=['daily', 'cutover'], default='daily')
    args = parser.parse_args()
    os.umask(0o077)
    if args.verify:
        manifest = verify(args.verify.resolve())
        print('Backup verified:', len(manifest['files']), 'files')
    else:
        result = backup(Path('/var/lib/novelai-shared-gateway'), Path('/var/backups/novelai-shared-gateway'),
                        production_configuration(), label=args.label)
        print('Complete backup verified:', result.name)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Config files and SQL rows contain credentials; do not log exception arguments or rsync output.
        print('Backup failed:', type(error).__name__, flush=True)
        raise SystemExit(1)
