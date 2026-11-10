import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


CLASSIFIER = Path(__file__).resolve().parents[2] / '.github/ci-docs-only.sh'
BASH = shutil.which('bash') or 'bash'
PACKAGED_DOCS = (
    'NOTICE.md',
    'web/THIRD_PARTY_NOTICES.md',
    'docs/operations/test-deployment.md',
    'docs/operations/production-release.md',
)


class DocsOnlyTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.repo = Path(temp.name)
        self.git('init', '-q')
        self.git('config', 'user.name', 'CI fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        self.git('config', 'commit.gpgsign', 'false')
        for name in (*PACKAGED_DOCS, 'README.md', 'docs/guide.md', 'scripts/README.md'):
            self.write(name)
        self.commit()
        self.base = self.git('rev-parse', 'HEAD')

    def git(self, *args):
        return subprocess.run(
            ['git', *args], cwd=self.repo, check=True, capture_output=True, text=True
        ).stdout.strip()

    def write(self, name, text='fixture\n'):
        path = self.repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding='utf-8')

    def commit(self):
        self.git('add', '--all')
        self.git('commit', '-qm', 'fixture')

    def classify(self, base):
        return subprocess.run(
            [BASH, CLASSIFIER.as_posix(), base], cwd=self.repo,
            check=True, capture_output=True, text=True,
        ).stdout.strip()

    def test_packaged_document_changes_require_full_validation(self):
        for path in (*PACKAGED_DOCS, 'scripts/README.md'):
            for change in ('edit', 'delete', 'rename', 'move'):
                with self.subTest(path=path, change=change):
                    self.git('checkout', '--detach', '-q', self.base)
                    if change == 'edit':
                        self.write(path, 'updated\n')
                    elif change == 'delete':
                        self.git('rm', path)
                    else:
                        destination = (
                            Path(path).with_name('RENAMED.md').as_posix()
                            if change == 'rename' else 'docs/moved notes.md'
                        )
                        self.git('mv', path, destination)
                    self.commit()
                    self.assertEqual(self.classify(self.base), '')

    def test_new_packaged_markdown_requires_full_validation(self):
        self.write('scripts/guides/example.md')
        self.commit()
        self.assertEqual(self.classify(self.base), '')

    def test_ordinary_document_changes_use_fast_path(self):
        name = 'multi\nline notes.md' if os.name != 'nt' else 'multi word notes.md'
        self.git('mv', 'README.md', f'docs/{name}')
        self.write('docs/history/evidence.json', '{}\n')
        self.write('docs/guide.md', 'updated\n')
        self.commit()
        self.assertEqual(self.classify(self.base), 'docs_only=true')

    def test_mixed_changes_and_unavailable_diff_use_full_validation(self):
        self.write('README.md', 'updated\n')
        self.write('server/new.ts')
        self.commit()
        self.assertEqual(self.classify(self.base), '')
        self.assertEqual(self.classify('missing-ci-base'), '')


if __name__ == '__main__':
    unittest.main()
