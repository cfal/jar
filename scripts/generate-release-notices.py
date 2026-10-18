#!/usr/bin/env python3
"""Generates dependency notices, rejecting cargo-about warning fallbacks."""

from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "licenses/dependencies.txt"


def main():
    OUTPUT.parent.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=OUTPUT.parent) as temporary:
        candidate = Path(temporary) / "dependencies.txt"
        result = subprocess.run(
            ["cargo", "about", "-L", "warn", "generate", "--locked", "--all-features",
             "--fail", "scripts/licenses.hbs", "-o", str(candidate)],
            cwd=ROOT, stderr=subprocess.PIPE, text=True,
        )
        if result.returncode or result.stderr:
            raise SystemExit(result.stderr or f"cargo-about exited with {result.returncode}")
        candidate.replace(OUTPUT)


if __name__ == "__main__":
    main()
