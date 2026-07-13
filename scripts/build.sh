#!/usr/bin/env bash
# Build MooFile wheels for the current platform.
#
# Usage:
#   ./scripts/build.sh              # Build native wheel (needs Rust + maturin)
#   ./scripts/build.sh --pure       # Build pure-Python wheel (no Rust needed)
#   ./scripts/build.sh --release    # Build native wheel with --release flag
#
# Prerequisites (native build):
#   - Rust: https://rustup.rs
#   - maturin: pip install maturin
#   - Python 3.10+

set -euo pipefail

MODE="native"
MATURIN_ARGS="--release"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pure)   MODE="pure"; shift ;;
        --debug)  MATURIN_ARGS=""; shift ;;
        --release) MATURIN_ARGS="--release"; shift ;;
        -h|--help)
            sed -n '2,12p' "$0"
            exit 0
            ;;
        *) echo "Unknown flag: $1"; exit 1 ;;
    esac
done

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

case "$MODE" in
    native)
        echo "==> Building native wheel (Rust core)..."
        if ! command -v maturin &>/dev/null; then
            echo "ERROR: maturin not found. Install it: pip install maturin"
            exit 1
        fi
        if ! command -v cargo &>/dev/null; then
            echo "ERROR: Rust not found. Install it: https://rustup.rs"
            exit 1
        fi

        # Allow forward compatibility for Python versions newer than PyO3 supports
        export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1

        maturin build $MATURIN_ARGS

        echo ""
        echo "==> Done. Wheels in: target/wheels/"
        ls -lh target/wheels/*.whl 2>/dev/null || echo "   (no wheels found)"
        ;;

    pure)
        echo "==> Building pure-Python wheel (no Rust needed)..."
        pip install -q build 2>/dev/null || true

        # Build using setuptools as fallback
        # We temporarily swap pyproject.toml because maturin is the default
        cp pyproject.toml pyproject.toml.maturin
        cat > pyproject.toml << 'PYEOF'
[build-system]
requires = ["setuptools>=61"]
build-backend = "setuptools.build_meta"

[project]
name = "moofile"
version = "0.4.0"
requires-python = ">=3.10"
dependencies = [
    "pymongo>=4.0",
    "sortedcontainers>=2.0",
    "numpy>=1.20",
    "snowballstemmer>=2.0",
]

[project.optional-dependencies]
pandas = ["pandas>=1.0"]

[project.scripts]
moosh      = "moofile.cli.repl:main"
moo2json   = "moofile.cli.json_tool:main"
moo2mongo  = "moofile.cli.mongo_tool:main"
moo2sqlite = "moofile.cli.sqlite_tool:main"

[tool.setuptools.packages.find]
where = ["."]
include = ["moofile*"]
PYEOF

        python -m build --wheel --outdir dist/
        mv pyproject.toml.maturin pyproject.toml

        echo ""
        echo "==> Done. Wheels in: dist/"
        ls -lh dist/*.whl 2>/dev/null || echo "   (no wheels found)"
        ;;
esac
