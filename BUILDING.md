# Building and Testing MooFile

Everything you need to build MooFile and run the full test suite across all
seven language bindings.

If you only want to *use* MooFile from Python, you need none of this — `pip
install moofile` ships a pure-Python fallback that works on its own, and
platform wheels carry the Rust engine prebuilt.

---

## Quick start (Ubuntu)

One `apt install` covers every language:

```bash
sudo apt update && sudo apt install -y \
  build-essential \
  pkg-config \
  cmake \
  git \
  curl \
  python3 \
  python3-pip \
  python3-venv \
  python3-dev \
  nodejs \
  npm \
  golang-go \
  openjdk-25-jdk \
  dotnet-sdk-10.0 \
  nlohmann-json3-dev
```

Rust is **not** in that list — install it from rustup rather than apt, because
the distro package is usually well behind and this project needs a recent
toolchain:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"
```

Then Python's test dependencies:

```bash
pip install --user pytest pymongo maturin
```

> `pymongo` provides the `bson` module the Python implementation uses. On
> Ubuntu 24.04+ pip refuses to install into the system environment; either add
> `--break-system-packages`, or work inside a venv:
> `python3 -m venv .venv && . .venv/bin/activate && pip install pytest pymongo maturin`

Now build and test everything:

```bash
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1   # needed for any cargo command
cargo build --release -p moofile-c             # the shared library everything else needs
./scripts/test-all.sh                          # runs all nine suites
```

---

## What each package is for

| Package | Needed for | Why |
|---|---|---|
| `build-essential` | Rust, C, C++, Go | Provides gcc, g++, make and libc headers. Rust needs a linker; cgo needs a C compiler. |
| `pkg-config` | Rust | Locating system libraries during the build. |
| `cmake` | C / C++ tests | `bindings/c/tests` builds its two test binaries with cmake (≥ 3.16). |
| `git` | Rust | cargo fetches some dependencies over git. |
| `curl` | rustup | Downloading the Rust installer. |
| `python3`, `python3-dev` | Python binding | `python3-dev` supplies `Python.h`, which PyO3 links against. |
| `python3-pip`, `python3-venv` | Python tests | Installing pytest, pymongo, maturin. |
| `nodejs`, `npm` | Node binding | Node 18 or newer. koffi ships prebuilt binaries, so no node-gyp toolchain is required. |
| `golang-go` | Go binding | Go 1.21+ with cgo enabled (the default). |
| `openjdk-25-jdk` | Java binding | **JDK 22 or newer is required** — the binding uses the Foreign Function & Memory API. Any of `openjdk-22-jdk` through `openjdk-25-jdk` works. The JRE alone is not enough; you need `javac`. |
| `dotnet-sdk-10.0` | C# binding | The projects target `net10.0`. For .NET 8 or 9, see [Older .NET](#older-net) below. |
| `nlohmann-json3-dev` | C++ wrapper | The header-only JSON library `moofile.hpp` uses. Optional — cmake downloads a single-header copy if it is missing, but that needs network access. |

### Version floors

| Tool | Minimum | Checked with |
|---|---|---|
| Rust | 1.75 | `rustc --version` |
| CMake | 3.16 | `cmake --version` |
| gcc / g++ | C11 / C++17 | `gcc --version` |
| Python | 3.9 | `python3 --version` |
| Node | 18 | `node --version` |
| Go | 1.21 | `go version` |
| JDK | **22** | `javac -version` |
| .NET SDK | 8 | `dotnet --version` |

---

## Building

### The Rust core and Python binding

```bash
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1

cargo build --release -p moofile-py           # → target/release/lib_native.so
cp target/release/lib_native.so moofile/_native.cpython-*.so
```

This is a cargo **workspace**: artifacts land in the repo-root `target/`, not
in `bindings/python/target/`. A stale directory may linger there — copying
from it silently installs an old build.

> If `moofile/_native` is a symlink to `bindings/python/_native/`, that package
> shadows `moofile/_native.cpython-*.so`. Refresh the `.so` inside the symlink
> target too, or imports keep resolving to the old build.

Alternatively, with a venv active: `maturin develop --release`.

### The C shared library

Every binding except Python needs this:

```bash
cargo build --release -p moofile-c    # → target/release/libmoofile.so
```

Each binding finds that file by walking up from its own directory. Override
with the `MOOFILE_LIB` environment variable, or for Java
`-Dmoofile.library.path=/path/to/libmoofile.so`.

### The `embed` feature

Autoembedding — on-device GGUF inference plus HuggingFace model downloads — is
**on by default** and pulls in `llama-gguf` and roughly 300 transitive crates.
If you do not need it, turn it off for a much smaller, faster build:

```bash
cargo build --release -p moofile-c --no-default-features
```

|  | Default | `--no-default-features` |
|---|---|---|
| Dependencies | ~379 | ~96 |
| `libmoofile.so` | ~8.3 MB | ~2.8 MB |
| `auto_embed` / `semantic()` | works | fails with a clear error |

Without the feature, the configuration types still exist and ordinary use is
unaffected; opening a collection with `auto_embed` configured fails with
`autoembedding is not available: this build of moofile was compiled without
the 'embed' feature`.

---

## Running the tests

`./scripts/test-all.sh` runs everything and prints a summary. It skips any
language whose toolchain is absent rather than failing, so a partial install
still gives useful results. To run suites individually:

```bash
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1

# Rust core — 79 tests
cargo test

# Python, both backends — 307 tests
# ALWAYS set PYTHONPATH=. or you test the installed site-packages copy
# instead of this checkout (silent, and very confusing).
PYTHONPATH=. pytest tests/ tests-cross/ -v

# C (73), C++ (42) and cross-backend parity (8)
cd bindings/c/tests && ./run_tests.sh --release

# Node.js — 22 tests
cd bindings/node && npm install && node test.js

# Go — 22 tests
cd bindings/go && go test ./moofile/

# Java — 30 tests
cd bindings/java && ./build.sh test

# C# — 30 tests
cd bindings/csharp && dotnet run --project Moofile.Tests
```

Runnable examples for each binding:

```bash
node bindings/node/example.js
cd bindings/go && go run ./example/
cd bindings/java && ./build.sh example
cd bindings/csharp && dotnet run --project Moofile.Example
```

---

## Other Linux distributions

<details>
<summary>Fedora / RHEL</summary>

```bash
sudo dnf install -y \
  gcc gcc-c++ make pkgconf-pkg-config cmake git curl \
  python3 python3-pip python3-devel \
  nodejs npm golang \
  java-latest-openjdk-devel \
  dotnet-sdk-10.0 \
  json-devel
```
</details>

<details>
<summary>Arch</summary>

```bash
sudo pacman -S --needed \
  base-devel cmake git curl \
  python python-pip \
  nodejs npm go \
  jdk-openjdk \
  dotnet-sdk \
  nlohmann-json
```
</details>

<details>
<summary>Alpine</summary>

```bash
sudo apk add \
  build-base cmake git curl pkgconf \
  python3 python3-dev py3-pip \
  nodejs npm go \
  openjdk21   # note: too old for the Java binding, which needs JDK 22+
```

Alpine uses musl. The Rust build works, but you may need
`RUSTFLAGS="-C target-feature=-crt-static"` for the cdylib.
</details>

---

## macOS

```bash
brew install cmake node go openjdk dotnet nlohmann-json
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
xcode-select --install     # for the C/C++ toolchain
```

Homebrew's `openjdk` is current enough for the Java binding. Link it so
`javac` is on PATH:

```bash
sudo ln -sfn "$(brew --prefix)/opt/openjdk/libexec/openjdk.jdk" \
             /Library/Java/JavaVirtualMachines/openjdk.jdk
```

The shared library is `libmoofile.dylib` rather than `.so`; every binding
already looks for the right name per platform.

---

## Windows

> Best effort — the bindings are written to be portable and pick the right
> library name (`moofile.dll`), but the test suites are developed and verified
> on Linux. The C/C++ `run_tests.sh` is a bash script and expects a Unix
> shell; use WSL if you want that suite.

**The path of least resistance is WSL2**, where the Ubuntu instructions above
apply unchanged:

```powershell
wsl --install -d Ubuntu
```

For a native Windows build, install with winget:

```powershell
winget install --id Rustlang.Rustup -e
winget install --id Kitware.CMake -e
winget install --id Git.Git -e
winget install --id Python.Python.3.12 -e
winget install --id OpenJS.NodeJS.LTS -e
winget install --id GoLang.Go -e
winget install --id Microsoft.OpenJDK.25 -e
winget install --id Microsoft.DotNet.SDK.10 -e
```

You also need the **MSVC build tools** — Rust's default Windows toolchain
links with MSVC, and cgo needs a C compiler:

```powershell
winget install --id Microsoft.VisualStudio.2022.BuildTools -e `
  --override "--quiet --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended"
```

Then, from a *Developer PowerShell for VS* (so the MSVC environment is set up):

```powershell
$env:PYO3_USE_ABI3_FORWARD_COMPATIBILITY = "1"
cargo build --release -p moofile-c        # → target\release\moofile.dll
pip install pytest pymongo maturin
```

Per-language notes:

| Binding | Windows status |
|---|---|
| Python | Works. `cargo build --release -p moofile-py`, then copy `target\release\_native.dll` to `moofile\_native.pyd`. |
| C / C++ | The library builds. The cmake test project should configure, but `run_tests.sh` is bash — run it under WSL or Git Bash, or drive cmake directly. |
| Node.js | Works. koffi ships prebuilt Windows binaries. |
| Go | Needs gcc for cgo; MSVC alone is not enough. Install [MSYS2](https://www.msys2.org/) mingw-w64 and put it on PATH, or use WSL. |
| Java | Works. Panama is platform-independent; point at the DLL with `-Dmoofile.library.path=target\release\moofile.dll`. `build.sh` is bash, so compile by hand or use WSL. |
| C# | Works, and is the most natively comfortable of the set. `dotnet run --project Moofile.Tests`. |

If the loader cannot find the DLL, set `MOOFILE_LIB` to its full path, or copy
`moofile.dll` next to the executable — Windows does not have an rpath
equivalent.

---

## Troubleshooting

**`error: feature 'embed' does not exist`** — you are on an older checkout
where `moofile-core` had no feature table. Pull the latest.

**Python tests import the wrong code** — you forgot `PYTHONPATH=.` and are
testing the installed site-packages copy. This fails silently and confusingly.

**Java: `UnsupportedClassVersionError` or "FFM API not found"** — your JDK is
older than 22. Check with `javac -version`; `java -version` may report a
different JDK than the compiler.

**Java: warnings about restricted methods** — expected. Pass
`--enable-native-access=ALL-UNNAMED`, which `build.sh` already does.

**`libmoofile.so: cannot open shared object file`** — the library was not
built, or is somewhere unexpected. Run `cargo build --release -p moofile-c`,
or set `MOOFILE_LIB=/full/path/to/libmoofile.so`.

**C++ tests: `nlohmann/json.hpp: No such file`** — install
`nlohmann-json3-dev`, or let cmake download it (needs network access).

**Go: `could not determine kind of name for C.moofile_...`** — the C header
was not found. The cgo directives use `${SRCDIR}` relative paths and assume
the in-repo layout; if you vendored the package elsewhere, set `CGO_CFLAGS`
and `CGO_LDFLAGS` explicitly.
