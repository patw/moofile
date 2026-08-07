#!/usr/bin/env bash
#
# run_tests.sh — Build and run the MooFile C/C++ test suite.
#
# Prerequisites:
#   - Rust toolchain (cargo, rustc)
#   - gcc / g++ with C11 and C++17 support
#   - cmake >= 3.16
#   - nlohmann/json (auto-downloaded if not found)
#
# Usage:
#   cd bindings/c/tests
#   ./run_tests.sh            # debug build
#   ./run_tests.sh --release  # release build
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINDING_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_DIR="$(cd "$BINDING_DIR/../.." && pwd)"

BUILD_TYPE="${1:-debug}"

echo "============================================"
echo " MooFile C/C++ Test Suite"
echo "============================================"
echo "Project dir:  $PROJECT_DIR"
echo "Binding dir:  $BINDING_DIR"
echo "Build type:   $BUILD_TYPE"
echo ""

# ------------------------------------------------------------------
# Step 1: Build the Rust C binding library
# ------------------------------------------------------------------
echo "--- Step 1: Building libmoofile (Rust cdylib) ---"

RUST_FLAGS=""
TARGET_DIR="debug"
if [ "$BUILD_TYPE" = "--release" ] || [ "$BUILD_TYPE" = "release" ]; then
    RUST_FLAGS="--release"
    TARGET_DIR="release"
fi

cd "$PROJECT_DIR"
cargo build -p moofile-c $RUST_FLAGS 2>&1

echo "libmoofile built: $PROJECT_DIR/target/$TARGET_DIR/libmoofile.so"
echo ""

# ------------------------------------------------------------------
# Step 2: Build C and C++ tests with CMake
# ------------------------------------------------------------------
echo "--- Step 2: Building test executables ---"

BUILD_DIR="$SCRIPT_DIR/build"
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

cmake "$SCRIPT_DIR" \
    -DMOOFILE_LIB_DIR="$PROJECT_DIR/target/$TARGET_DIR" \
    -DMOOFILE_INCLUDE_DIR="$BINDING_DIR/include" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
    2>&1

cmake --build . 2>&1

echo ""
echo "--- Test executables ---"
ls -la test_c_api test_cxx_api 2>/dev/null || true
echo ""

# ------------------------------------------------------------------
# Step 3: Run the tests
# ------------------------------------------------------------------
echo "--- Step 3: Running tests ---"
echo ""

C_EXIT=0
CXX_EXIT=0

echo "=== C API Test ==="
LD_LIBRARY_PATH="$PROJECT_DIR/target/$TARGET_DIR:${LD_LIBRARY_PATH:-}" \
    "$BUILD_DIR/test_c_api" \
    || C_EXIT=$?

echo ""
echo "=== C++ Wrapper Test ==="
LD_LIBRARY_PATH="$PROJECT_DIR/target/$TARGET_DIR:${LD_LIBRARY_PATH:-}" \
    "$BUILD_DIR/test_cxx_api" \
    || CXX_EXIT=$?

# Cross-backend parity: pure Python vs PyO3 vs the C library.  Skipped
# rather than failed when the Python package is not importable, so the
# C/C++ suites remain usable without a Python environment.
PARITY_EXIT=0
PARITY_STATUS="SKIP (moofile not importable)"
echo ""
echo "=== Cross-backend Parity ==="
if PYTHONPATH="$PROJECT_DIR" python3 -c "import moofile" >/dev/null 2>&1; then
    PYTHONPATH="$PROJECT_DIR" python3 "$SCRIPT_DIR/test_parity.py" \
        --c-lib "$PROJECT_DIR/target/$TARGET_DIR/libmoofile.so" \
        || PARITY_EXIT=$?
    PARITY_STATUS="$([ $PARITY_EXIT -eq 0 ] && echo 'PASS' || echo 'FAIL')"
else
    echo "  skipped — run from a checkout where 'import moofile' works"
fi

echo ""
echo "============================================"
echo " Results"
echo "============================================"
echo "  C API:        $([ $C_EXIT -eq 0 ] && echo 'PASS' || echo 'FAIL')"
echo "  C++ Wrapper:  $([ $CXX_EXIT -eq 0 ] && echo 'PASS' || echo 'FAIL')"
echo "  Parity:       $PARITY_STATUS"

if [ $C_EXIT -ne 0 ] || [ $CXX_EXIT -ne 0 ] || [ $PARITY_EXIT -ne 0 ]; then
    echo ""
    echo "  SOME TESTS FAILED!"
    exit 1
fi

echo ""
echo "  All tests passed!"
