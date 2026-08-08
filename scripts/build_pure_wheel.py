#!/usr/bin/env python3
"""Build the pure-Python fallback wheel (``moofile-<v>-py3-none-any.whl``).

This is the floor under the platform wheels: musl/Alpine, Intel macOS and any
other platform CI does not build for. Without it ``pip install moofile`` does
not fail on those platforms — it silently resolves *backwards* to the newest
release that has a compatible artifact, which was 0.2.1, from before the Rust
core existed. A missing floor is therefore not a missing feature, it is a
silent fourteen-version downgrade.

The root ``pyproject.toml`` uses the maturin backend, which always compiles the
extension. Building a pure-Python wheel means temporarily swapping in the
setuptools backend, so this script rewrites ``pyproject.toml``, builds, then
restores it — including on failure.

Usage:  python scripts/build_pure_wheel.py [--outdir dist]
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"

MATURIN_BUILD_SYSTEM = """[build-system]
requires = ["maturin>=1.0,<2.0"]
build-backend = "maturin"
"""

SETUPTOOLS_BUILD_SYSTEM = """[build-system]
requires = ["setuptools>=61"]
build-backend = "setuptools.build_meta"
"""

# maturin discovers the package from `python-source`; setuptools needs telling.
SETUPTOOLS_PACKAGES = """
[tool.setuptools]
packages = ["moofile", "moofile.cli"]
"""


def to_pure_python(text: str) -> str:
    """Rewrite the maturin pyproject into an equivalent setuptools one."""
    if MATURIN_BUILD_SYSTEM not in text:
        raise SystemExit(
            "pyproject.toml does not contain the expected maturin [build-system] "
            "block — refusing to guess. Update scripts/build_pure_wheel.py."
        )
    text = text.replace(MATURIN_BUILD_SYSTEM, SETUPTOOLS_BUILD_SYSTEM, 1)

    # Drop [tool.maturin]; it runs to the end of the file today, but match only
    # up to the next section header so a later addition is not swallowed.
    text, n = re.subn(
        r"\n\[tool\.maturin\]\n(?:(?!\n\[).)*", "\n", text, flags=re.DOTALL
    )
    if n != 1:
        raise SystemExit(f"expected exactly one [tool.maturin] section, found {n}")

    return text.rstrip() + "\n" + SETUPTOOLS_PACKAGES


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="dist")
    args = ap.parse_args()

    original = PYPROJECT.read_text()
    try:
        PYPROJECT.write_text(to_pure_python(original))
        subprocess.run(
            [sys.executable, "-m", "build", "--wheel", "--outdir", args.outdir],
            cwd=ROOT,
            check=True,
        )
    finally:
        PYPROJECT.write_text(original)

    built = sorted(Path(ROOT / args.outdir).glob("moofile-*-py3-none-any.whl"))
    if not built:
        raise SystemExit(
            "build produced no py3-none-any wheel — the maturin backend was "
            "probably still in effect, which is the bug this script exists to "
            "prevent"
        )
    print(f"built pure-Python wheel: {built[-1].name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
