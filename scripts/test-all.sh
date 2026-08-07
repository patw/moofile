#!/usr/bin/env bash
#
# test-all.sh — Run every MooFile test suite and print a summary.
#
# Languages whose toolchain is missing are SKIPPED, not failed, so a partial
# install still gives useful results.  See BUILDING.md for what to install.
#
# Usage:
#   ./scripts/test-all.sh            # build the C library if needed, run all
#   ./scripts/test-all.sh --no-build # assume libmoofile is already built

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1

BUILD=1
[ "${1:-}" = "--no-build" ] && BUILD=0

# name -> PASS / FAIL / SKIP(reason)
declare -A RESULTS
ORDER=()
FAILED=0

record() {
    RESULTS["$1"]="$2"
    ORDER+=("$1")
    [ "${2%%:*}" = "FAIL" ] && FAILED=1
    return 0
}

# Run a suite, recording PASS or FAIL from its exit status.
run_suite() {
    local name="$1"; shift
    echo ""
    echo "=============================================="
    echo " $name"
    echo "=============================================="
    if "$@"; then
        record "$name" "PASS"
    else
        record "$name" "FAIL"
    fi
}

skip() {
    echo ""
    echo "--- $1: SKIPPED ($2) ---"
    record "$1" "SKIP: $2"
}

have() { command -v "$1" >/dev/null 2>&1; }

# ------------------------------------------------------------------
# Build the shared library everything but Python depends on
# ------------------------------------------------------------------

if ! have cargo; then
    echo "error: cargo not found — install Rust from https://rustup.rs" >&2
    exit 1
fi

if [ $BUILD -eq 1 ]; then
    echo "--- Building libmoofile (release) ---"
    cargo build --release -p moofile-c || exit 1
fi

if [ ! -f "$PROJECT_DIR/target/release/libmoofile.so" ] \
   && [ ! -f "$PROJECT_DIR/target/release/libmoofile.dylib" ]; then
    echo "error: libmoofile not built — run without --no-build" >&2
    exit 1
fi

# ------------------------------------------------------------------
# Rust core
# ------------------------------------------------------------------

run_suite "Rust core" cargo test --quiet

# The slim build is a supported configuration, so keep it compiling.
run_suite "Rust core (--no-default-features)" \
    cargo test --quiet -p moofile-core --no-default-features

# ------------------------------------------------------------------
# Python — both backends
# ------------------------------------------------------------------

if have python3 && python3 -c "import pytest, bson" >/dev/null 2>&1; then
    # PYTHONPATH=. or pytest silently tests the installed copy instead.
    run_suite "Python (tests/ + tests-cross/)" \
        env PYTHONPATH="$PROJECT_DIR" python3 -m pytest tests/ tests-cross/ -q
else
    skip "Python (tests/ + tests-cross/)" "needs pytest and pymongo"
fi

# ------------------------------------------------------------------
# C, C++ and cross-backend parity
# ------------------------------------------------------------------

if have cmake && have gcc && have g++; then
    run_suite "C / C++ / parity" \
        bash "$PROJECT_DIR/bindings/c/tests/run_tests.sh" --release
else
    skip "C / C++ / parity" "needs cmake, gcc and g++"
fi

# ------------------------------------------------------------------
# Node.js
# ------------------------------------------------------------------

if have node && have npm; then
    if [ ! -d "$PROJECT_DIR/bindings/node/node_modules" ]; then
        echo "--- Installing Node dependencies ---"
        (cd "$PROJECT_DIR/bindings/node" && npm install --silent)
    fi
    run_suite "Node.js" \
        bash -c "cd '$PROJECT_DIR/bindings/node' && node test.js"
else
    skip "Node.js" "needs nodejs and npm"
fi

# ------------------------------------------------------------------
# Go
# ------------------------------------------------------------------

if have go; then
    run_suite "Go" \
        bash -c "cd '$PROJECT_DIR/bindings/go' && CGO_ENABLED=1 go test ./moofile/"
else
    skip "Go" "needs golang-go"
fi

# ------------------------------------------------------------------
# Java — needs JDK 22+ for the Foreign Function & Memory API
# ------------------------------------------------------------------

if have javac; then
    JAVA_MAJOR="$(javac -version 2>&1 | sed -E 's/javac ([0-9]+).*/\1/')"
    if [ "${JAVA_MAJOR:-0}" -ge 22 ] 2>/dev/null; then
        run_suite "Java" bash "$PROJECT_DIR/bindings/java/build.sh" test
    else
        skip "Java" "JDK 22+ required, found $JAVA_MAJOR"
    fi
else
    skip "Java" "needs a JDK 22+ (javac not found)"
fi

# ------------------------------------------------------------------
# C#
# ------------------------------------------------------------------

if have dotnet; then
    run_suite "C#" \
        bash -c "cd '$PROJECT_DIR/bindings/csharp' && dotnet run --project Moofile.Tests"
else
    skip "C#" "needs the .NET SDK"
fi

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------

echo ""
echo "=============================================="
echo " Summary"
echo "=============================================="
for name in "${ORDER[@]}"; do
    printf "  %-36s %s\n" "$name" "${RESULTS[$name]}"
done
echo ""

if [ $FAILED -ne 0 ]; then
    echo "  SOME SUITES FAILED"
    exit 1
fi

echo "  All available suites passed."
