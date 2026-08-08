# Distribution

How MooFile reaches users in each language, and why each choice was made.

## The shape of the problem

Python is self-contained: a wheel carries the compiled extension, and there is
a pure-Python fallback when no wheel matches. Nothing else works that way.

**Every other binding needs `libmoofile` present at runtime.** The binding
itself is a few hundred lines of source that could be distributed trivially;
the native library behind it is the entire difficulty. So the distribution
question for six languages is really one question — *how does the shared
library get onto the user's machine* — asked six ways.

That is why the first piece of work was building and publishing the library
itself, once, for every platform. Everything else layers on top.

---

## What ships today

### Native libraries — GitHub Releases

`.github/workflows/release-libs.yml` builds `libmoofile` on tag push for:

| Target | Runner | Artifact |
|---|---|---|
| linux-x86_64 | ubuntu-latest | `moofile-linux-x86_64.tar.gz` |
| linux-aarch64 | ubuntu-24.04-arm | `moofile-linux-aarch64.tar.gz` |
| macos-aarch64 | macos-14 | `moofile-macos-aarch64.tar.gz` |
| windows-x86_64 | windows-latest | `moofile-windows-x86_64.zip` |

ARM Linux builds on a native ARM runner rather than cross-compiling: autoembedding
pulls a C/C++ toolchain in through `llama-gguf`, and cross-builds of that are what
failed when the target was first attempted.

Each archive holds `include/` (both headers), `lib/` (the shared library, plus
the import `.lib` on Windows), and a README. `SHA256SUMS` is published
alongside. The job verifies ≥30 exported `moofile_*` symbols before packaging,
so a silently broken build cannot ship.

Every binding accepts **`MOOFILE_LIB`** pointing at the library (Java uses
`-Dmoofile.library.path`), so one download serves all of them:

```bash
tar xzf moofile-linux-x86_64.tar.gz
export MOOFILE_LIB=$PWD/moofile-linux-x86_64/lib/libmoofile.so
```

### Python — PyPI

`.github/workflows/build-wheels.yml` builds one **abi3** wheel per platform —
linux x86_64/aarch64, macOS arm64, Windows x86_64 — each covering CPython 3.10 and
up, plus a pure-Python wheel as the fallback for everything else. This is the best
experience of any language here.

Before abi3 each wheel was pinned to one CPython minor version, so most
platform/version combinations had no wheel at all; pip did not fail on those, it
silently installed 0.2.1, the last release from before the Rust core. The publish
job now refuses to ship an incomplete artifact set for that reason.

```bash
pip install moofile
```

### Go — module path

Go resolves a module by fetching its path over HTTPS, so the module path must
match the repository. It previously read `github.com/patw/moofile-go`, a repo
that does not exist, which made `go get` impossible. It is now:

```go
import "github.com/patw/moofile/bindings/go/moofile"
```

Go still needs `libmoofile` at build time for cgo. That is normal for a cgo
package wrapping a system library — the same as `go-oci8` or the GDAL
bindings — and is documented rather than worked around.

---

## Per-language recommendations

Weighed against a project with well under a hundred users, most of them on
Python. "Idiomatic" is the goal, but not at any cost.

| Language | Idiomatic channel | Verdict | Why |
|---|---|---|---|
| Python | PyPI wheels | **Done** | Already working, nothing to add. |
| C / C++ | Release tarball + headers | **Done** | There is no registry convention for C. A tarball with `include/` and `lib/` *is* the idiom. |
| Go | `go get` from the repo | **Done** | Module path fixed. Needing a system library is normal for cgo. |
| C# | NuGet with `runtimes/<rid>/native/` | **Done** | `dotnet add package MooFile`. The SDK copies the matching native file into the consumer's output automatically. |
| Node.js | npm | **Done** | `npm install moofile`. All four currently built platform binaries ship in the package; no compile step, no postinstall download. |
| Java | GitHub Release JAR + native archive | **Done, no Maven Central** | Maven Central is still disproportionate: Sonatype namespace verification, GPG signing, and staging. A versioned binding JAR and each platform's native archive attach to the same GitHub Release; [the Java guide](bindings/java/README.md) covers javac, Maven/Gradle local consumption, and fat-JAR deployment. |

### Node sizing

Two shapes are available:

1. **One package carrying all four platform libraries.** Simplest possible
   thing; never breaks. Measured at 3.3 MB compressed per platform, so about
   13 MB for the published package (8.3 MB unpacked each, because
   autoembedding pulls `llama-gguf` into every binary).
2. **Per-platform optional dependencies** (`@moofile/linux-x64` and friends,
   selected by npm through the `os`/`cpu` fields). This is what esbuild and
   swc do and it is the modern best practice — each user downloads ~8 MB. It
   costs six published packages instead of one.

At this scale option 1 is the right trade: less machinery, nothing to get out
of sync, and no `postinstall` download script — those break under
`--ignore-scripts`, behind proxies, and in air-gapped CI. Option 2 is a
mechanical upgrade later if size ever becomes a real complaint.

### Why not a `postinstall` downloader anywhere

It is a common pattern and it is the wrong one here. It fails with
`--ignore-scripts` (increasingly the default in security-conscious shops),
fails offline, fails behind restrictive proxies, and turns every install into
a network dependency on GitHub's availability. Bundling or a registry's own
native-asset convention avoids all of it.

---

## Consuming as an agent

Agents need the same things humans do, minus the tolerance for ambiguity:
one obvious install path, and documentation that states contracts rather than
implying them.

- `pip install moofile` is the single best entry point and needs no native
  setup at all.
- For other languages, the release archive plus `MOOFILE_LIB` is one download
  and one environment variable — no build step to reason about.
- `bindings/README.md` documents the ABI contract explicitly (error
  conventions, ownership, no-match semantics) precisely so a reader does not
  have to infer them from the source.
- `BUILDING.md` lists exact package names per platform.

---

## Publishing setup

Both registry jobs live in `release-libs.yml` and run on a `v*` tag, after the
native build matrix they depend on. Each refuses to publish if the tag does
not match the version in `package.json` / `Moofile.csproj`, so a mistagged
release fails loudly instead of shipping a mismatched package.

### NuGet — trusted publishing

Uses OIDC, so there is no long-lived key to leak. Requires one repository
variable:

| Kind | Name | Value |
|---|---|---|
| Variable | `NUGET_USER` | the nuget.org account name |

Set under *Settings → Secrets and variables → Actions → Variables*. The
trusted-publishing policy on nuget.org must name this repository and the
workflow.

### npm — token

npm's trusted publishing is newer and not yet wired up here, so this uses a
token:

| Kind | Name | Value |
|---|---|---|
| Secret | `NPM_TOKEN` | an npm automation token |

Set under *Settings → Secrets and variables → Actions → Secrets*. Use an
**automation** token (bypasses 2FA for CI) and rotate it if it is ever pasted
anywhere outside that box. Publishing uses `--provenance`, which attests the
package back to this repository and commit.

## Still open

**Java Maven Central.** Maven Central remains deliberately skipped: Sonatype
namespace verification, GPG signing, and the staging-repository dance are a
multi-day slog for what is currently a small user base. GitHub Releases now
ship the versioned Java binding JAR alongside the platform-native archives;
revisit a public Maven repository if demand justifies the operational cost.
