#!/usr/bin/env python3
"""Checks that binary artifacts contain the exact repository license notices."""

import argparse
from pathlib import Path
import tarfile

ROOT = Path(__file__).resolve().parent.parent
NOTICES = ("LICENSE", "THIRD_PARTY_NOTICES.md", "licenses/dependencies.txt")


def check(artifact):
    if artifact.is_dir():
        actual = {name: (artifact / name).read_bytes() for name in NOTICES}
    else:
        with tarfile.open(artifact) as archive:
            actual = {name: archive.extractfile(name).read() for name in NOTICES}
    for name, contents in actual.items():
        if contents != (ROOT / name).read_bytes():
            raise ValueError(f"{artifact}: incorrect {name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    check(parser.parse_args().artifact)
