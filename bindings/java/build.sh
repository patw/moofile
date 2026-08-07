#!/usr/bin/env bash
#
# build.sh — Compile and test the MooFile Java binding.
#
# The binding uses the JDK's Foreign Function & Memory API (JDK 22+), so it
# needs no third-party jars and no Maven or Gradle — just a JDK and the
# libmoofile shared library.
#
# Usage:
#   ./build.sh            # build, then run the tests
#   ./build.sh build      # build only
#   ./build.sh test       # build, then run the tests
#   ./build.sh example    # build, then run the examples
#   ./build.sh jar        # build a redistributable Java binding JAR

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$SCRIPT_DIR/build/classes"
MAIN_BUILD_DIR="$SCRIPT_DIR/build/main-classes"
JAR_DIR="$SCRIPT_DIR/build"
LIB_DIR="$PROJECT_DIR/target/release"
VERSION="$(sed -nE 's/^version = "([^"]+)"/\1/p' "$PROJECT_DIR/Cargo.toml" | head -n1)"
JAR_FILE="$JAR_DIR/moofile-java-$VERSION.jar"

ACTION="${1:-test}"

# ------------------------------------------------------------------
# Preflight
# ------------------------------------------------------------------

if ! command -v javac >/dev/null 2>&1; then
    echo "error: javac not found — install a JDK 22 or newer" >&2
    exit 1
fi

JAVA_MAJOR="$(javac -version 2>&1 | sed -E 's/javac ([0-9]+).*/\1/')"
if [ "$JAVA_MAJOR" -lt 22 ]; then
    echo "error: JDK 22+ required (found $JAVA_MAJOR)." >&2
    echo "       The binding uses the Foreign Function & Memory API." >&2
    exit 1
fi

if [ "$ACTION" != "jar" ] && [ ! -f "$LIB_DIR/libmoofile.so" ] \
   && [ ! -f "$LIB_DIR/libmoofile.dylib" ] \
   && [ ! -f "$LIB_DIR/moofile.dll" ]; then
    echo "--- libmoofile not found, building it ---"
    (cd "$PROJECT_DIR" && cargo build -p moofile-c --release)
fi

# ------------------------------------------------------------------
# Build
# ------------------------------------------------------------------

echo "--- Compiling ---"
mkdir -p "$BUILD_DIR"
javac -d "$BUILD_DIR" \
    "$SCRIPT_DIR"/src/main/java/com/moofile/*.java \
    "$SCRIPT_DIR"/src/test/java/com/moofile/*.java
echo "Classes written to $BUILD_DIR"

package_jar() {
    echo "--- Packaging $JAR_FILE ---"
    rm -rf "$MAIN_BUILD_DIR"
    mkdir -p "$MAIN_BUILD_DIR"
    javac -d "$MAIN_BUILD_DIR" "$SCRIPT_DIR"/src/main/java/com/moofile/*.java

    local manifest="$JAR_DIR/MANIFEST.MF"
    cat > "$manifest" <<'EOF'
Manifest-Version: 1.0
Automatic-Module-Name: com.moofile
EOF
    jar --create --file "$JAR_FILE" --manifest "$manifest" -C "$MAIN_BUILD_DIR" .
    echo "JAR written to $JAR_FILE"
}

# --enable-native-access silences the restricted-method warning;
# moofile.library.path tells the binding where the platform-specific library lives.
JAVA_ARGS=()
if [ "$ACTION" != "jar" ]; then
    if [ -f "$LIB_DIR/libmoofile.dylib" ]; then
        LIB_PATH="$LIB_DIR/libmoofile.dylib"
    elif [ -f "$LIB_DIR/moofile.dll" ]; then
        LIB_PATH="$LIB_DIR/moofile.dll"
    else
        LIB_PATH="$LIB_DIR/libmoofile.so"
    fi

    JAVA_ARGS=(
        --enable-native-access=ALL-UNNAMED
        "-Dmoofile.library.path=$LIB_PATH"
        -cp "$BUILD_DIR"
    )
fi

case "$ACTION" in
    build)
        ;;
    test)
        echo ""
        java "${JAVA_ARGS[@]}" com.moofile.CollectionTest
        ;;
    example)
        echo ""
        java "${JAVA_ARGS[@]}" com.moofile.Example
        ;;
    jar)
        package_jar
        ;;
    *)
        echo "usage: $0 [build|test|example|jar]" >&2
        exit 1
        ;;
esac
