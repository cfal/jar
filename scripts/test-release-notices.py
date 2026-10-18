#!/usr/bin/env python3
"""Regression tests for release archive notices."""

import importlib.util
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("notices", ROOT / "scripts/check-release-notices.py")
notices = importlib.util.module_from_spec(spec)
spec.loader.exec_module(notices)


class ReleaseNoticesTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=Path.home())
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.package = self.root / "package"
        for name in notices.NOTICES:
            destination = self.package / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, destination)

    def archive(self):
        path = self.root / "jar.tar.gz"
        with tarfile.open(path, "w:gz") as archive:
            for name in notices.NOTICES:
                source = self.package / name
                if source.exists():
                    archive.add(source, arcname=name)
        return path

    def test_matching_directory_and_archive(self):
        notices.check(self.package)
        notices.check(self.archive())

    def test_missing_notice(self):
        for name in notices.NOTICES:
            with self.subTest(name=name):
                path = self.package / name
                path.unlink()
                with self.assertRaises(FileNotFoundError):
                    notices.check(self.package)
                with self.assertRaises(KeyError):
                    notices.check(self.archive())
                shutil.copyfile(ROOT / name, path)

    def test_changed_notice(self):
        for name in notices.NOTICES:
            with self.subTest(name=name):
                path = self.package / name
                path.write_text("incorrect notice\n")
                for artifact in (self.package, self.archive()):
                    with self.assertRaisesRegex(ValueError, "incorrect"):
                        notices.check(artifact)
                shutil.copyfile(ROOT / name, path)


if __name__ == "__main__":
    unittest.main()
