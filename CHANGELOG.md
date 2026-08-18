# Changelog

## v1.2.2 (2026-08-18)

### Text analyzer: digits are indexed, arrays are no longer skipped (re-index)

Two silent defects in the inverted index, both found by an accuracy audit of a
439-document corpus.

- **Digits were discarded.** The tokenizer matched `[a-zA-Z]+` and dropped
  single characters, so no numeric token ever reached the index or a query.
  `14900K` and `595.71.05` returned *zero* results against a corpus that
  contained both; `moofile v1.2.0` and `moofile v0.3.0` produced byte-identical
  queries and therefore identical rankings. The pattern is now
  `[a-zA-Z0-9]+(?:[._-][a-zA-Z0-9]+)*`, which also holds compound identifiers
  (`llama.cpp`, `x86-64-v3`) together; a compound additionally emits its parts,
  so `llama` still finds `llama.cpp`. Porter stemming is now applied only to
  purely alphabetic terms — it mangled identifiers to no benefit.
- **Array fields were never indexed.** The add path matched `Bson::String`
  only, so declaring a text index on an array field built an empty index in
  silence and every search against it returned nothing. Arrays are now squashed
  into one space-joined string and indexed like any other field (as Lucene does
  for multi-valued fields); nested arrays flatten, and numeric elements are
  stringified so they stay searchable.

Measured on the audit corpus, the previously-broken query classes go from no
results to the correct document at rank 1, while ordinary prose queries are
unchanged (Recall@5 0.880 → 0.900, NDCG@5 0.871 → 0.875 on a 25-query set).

**This changes index contents, not the cache layout**, so `CACHE_VERSION` is
bumped 2 → 3: a stale cache would otherwise load happily and keep serving the
old token set. Existing collections rebuild their index on next open. No
document data is touched and no migration is required.

### Embedding memory is now bounded by a token budget, not a document count

Re-embedding a few hundred ordinary documents could OOM a 16 GB machine.
`batch_size` defaulted to 32 documents, and peak inference memory tracks
`batch x padded_sequence_length` — so the peak depended entirely on how long
the caller's documents happened to be. Measured on voyage-4-nano, the model
weights are ~170 MiB while a batch of 32 x 1024 tokens peaks at **~9.2 GiB**
(scaling close to linearly: ~285 MiB per document at full sequence length).

- New `max_batch_tokens` (default **8192**) caps `batch x padded_len`, holding
  inference to roughly 2.5 GiB regardless of document length. `batch_size`
  remains as a count cap and still applies, so short documents keep batching
  widely and lose no throughput.
- Batches are now assembled **longest-first**. The tokenizer pads to the
  longest member, so one long document in an otherwise short batch previously
  inflated every row to its width.
- **`batch_size` and `max_batch_tokens` are now settable from the bindings.**
  Both were reachable only from Rust, which left Python and C callers with no
  way to cap a re-embed at all.


## v1.2.1 (2026-08-16)

### Windows CI/CD fix: drop tokenizers default features (esaxx-rs CRT mismatch)

The v1.2.0 release workflows both failed on `windows-x86_64` at link time:
`link.exe` aborted with LNK2038 / exit code 1169 because `esaxx-rs` (pulled in
by `tokenizers`'s default `esaxx_fast` feature) hardcodes `static_crt(true)`
(/MT) while ONNX Runtime's prebuilt binaries are /MD. `v4nano-embed` now
depends on `tokenizers` with `default-features = false, features = ["onig"]` —
the same configuration `fastembed` used — so the static-CRT C++ build is never
compiled. No other platform or runtime behavior changed.

## v1.2.0 (2026-08-15)

### Autoembedding now runs voyage-4-nano via a dedicated crate (breaking)

fastembed is gone. It was pulled in to run *any* registry model, but the only
model we ever wanted was voyage-4-nano — so the generic registry was replaced
with [`v4nano-embed`](v4nano-embed/), a single-purpose ONNX runner extracted
from fastembed's glue (~200 lines), depending on `ort` + `tokenizers` directly.

- The default (and only built-in) model is now **`voyage-4-nano`**
  (`onnx-community/voyage-4-nano-ONNX` int8 export): 180M+160M params, 2048
  dims, 32k context, MRL-truncatable to 1024/512/256. `model` may also be a
  path to a local directory holding `model_quantized.onnx` + `tokenizer.json`.
- **`model` is now optional** — modern configs omit it and get voyage-4-nano.
  An empty string (some bindings serialize unset fields as `""`) also means
  the default. `hf:` GGUF URIs and fastembed registry ids are rejected with
  guidance to use `voyage-4-nano`.
- **`max_length`** (new config field, default **1024**) caps tokenizer
  truncation per source field. The ONNX export materializes a full
  `[1, 16, T, T]` attention mask (16·T²·4 bytes), so 1024 is cheap (67 MB,
  ~0.7 s) while 32k needs ~64 GB and fails with a clean embedding error — raise
  it only for whole-document embedding on hardware that can afford it.
- The previous GGUF backend (~7 s/embed) is long gone; this replaces the
  fastembed backend (~4 ms/embed, 384 dims) with voyage-4-nano
  (~35 ms single, ~6 ms batched, 2048 dims).
- `v4nano-embed` is a workspace member and carries the Apache-2.0 license for
  the code adapted from fastembed; the rest of moofile stays MIT.
- `--no-default-features` still drops embedding entirely (~38 MB → ~2.8 MB
  `libmoofile`).

## v1.1.1 (2026-08-15)

### Wheel build fix: manylinux 2_17 → 2_28 (std::regex ABI segfault)

The Linux wheels were built in the 2014-era `manylinux_2_17` container (CentOS 7,
GCC 4.9 headers → `GLIBCXX <= 3.4.19`). `std::regex` has no stable ABI in
libstdc++, and ONNX Runtime's `DeviceDiscovery::GetPciBusId` uses it — so on hosts
with a modern libstdc++ (e.g. Ubuntu 22.04+, libstdc++ 6.0.3x) opening a
collection with `auto_embed` segfaulted inside `regex_traits::transform`.

- Wheels are now built with `manylinux_2_28` (AlmaLinux 8, glibc 2.28, GCC 12,
  `GLIBCXX 3.4.30`) — still covers every supported distro, but the compiled
  `std::regex` code is compatible with current libstdc++ runtimes.
- Docs updated (README, moofile-spec, workflow comments).

## v1.1.0 (2026-08-14)

### Autoembedding now runs ONNX models via fastembed (breaking)

The `llama-gguf` embedding backend is gone, replaced by
[`fastembed`](https://crates.io/crates/fastembed) (ONNX Runtime + HuggingFace
tokenizers). The default model is **`BAAI/bge-small-en-v1.5`** (33M params,
384 dims).

This was not a tuning problem. `llama-gguf` 0.14's `EmbeddingExtractor` ran a
full forward pass **per token** and used the vocabulary *logits* as a stand-in
for the hidden state — slicing the first `hidden_dim` values off a ~152k-wide
distribution. The stored vectors were not sentence embeddings, and each one
cost ~7 seconds.

| | before | after |
|---|---|---|
| embed, short sentence | ~7 000 ms | **~4 ms** |
| semantic search | ~6 500 ms | **~4 ms** |
| model load (warm) | ~500 ms | **~186 ms** |
| stored vector | 1024 dims | 384 dims |
| `libmoofile.so` | 8.3 MB | **38.5 MB** |

The size increase is the statically linked ONNX Runtime and is not avoidable
while embedding is compiled in; `--no-default-features` still drops the whole
feature (down to ~2.8 MB) and `auto_embed` then returns `EmbedDisabled`.
Prebuilt ONNX Runtime binaries exist for all four supported targets
(linux x86-64/aarch64, macOS arm64, Windows x86-64).

**Migrating.** `model` now names a fastembed registry entry rather than a GGUF
file. Three spellings resolve, case-insensitively: the canonical HuggingFace id
(`BAAI/bge-small-en-v1.5`), the exact registry `model_code`
(`Xenova/bge-small-en-v1.5`), or the bare name (`bge-small-en-v1.5`). The old
`hf:repo:file.gguf` syntax is rejected with a message pointing at the
replacement rather than a bare "unknown model". Local model paths are **not**
currently supported — they only ever pointed at `.gguf` files, which no longer
load at all, but the "bring your own model" escape hatch is a follow-up.

Config defaults moved with the model: `dims` 1024 → 384, `query_prefix` to
BGE's instruction, `doc_prefix` to empty (BGE is asymmetric), `batch_size`
1 → 32.

### Changing the embedding model no longer corrupts search silently

Vectors of different widths cannot be compared, and the vector index quietly
skips any vector whose length is not the declared dimension. That is right for
one malformed document and badly wrong for "the embedding model changed":
the entire collection dropped out of the index and every search returned
nothing, with no signal anywhere.

- At open, each autoembedded vector field is checked against both the model's
  output width and the widths actually stored. A mismatch logs a warning and
  **disables** that index; searching it raises `VectorIndexDisabled` naming the
  expected width, the found width and the affected document count.
- New `reembed(source_field)` rewrites every stored vector at the new width,
  retargets the index and its `.meta` entry, and clears the flag. Embedding is
  batched, so it is several times faster per document than re-inserting.
- Re-embedding is never implicit on `open()`. It is a whole-collection write
  that can take minutes, it would turn a read-only handle into a writer, and a
  typo in the model id would destroy the old vectors before anyone noticed.

Note that `merge_meta` keeps a vector index's existing width and ignores a
re-declaration, so simply changing `vector_indexes={...}` on an existing
database does not widen or narrow the index — `reembed()` is what updates it.

### Bulk insert embeds in batches

`insert_many` embedded one document per ONNX pass. It now collects the batch's
texts, embeds them `batch_size` at a time, and inserts with the vectors already
attached — **4.9× faster** per document (2.94 ms → 0.61 ms on a 64-document
batch), with byte-identical output to the per-document path.

A duplicate `_id` already present in the collection is now detected before any
embedding work, so a bad id in a 10 000-document batch no longer costs 10 000
forward passes before erroring. Duplicates *within* a batch are still caught
per document, by the running index.

Documents inserted inside a `batch()` still embed one at a time, since those
are buffered individually.

### `reembed()` in every binding

New across all seven bindings, following the C ABI as usual:

| Language | Signature |
|---|---|
| C | `int64_t moofile_reembed(MooFileCollection*, const char*, char**)` |
| C++ | `int64_t reembed(const std::string&)` |
| Python | `db.reembed(source_field) -> int` |
| Node.js | `db.reembed(sourceField) -> number` |
| Go | `db.Reembed(sourceField string) (int64, error)` |
| Java | `long reembed(String sourceField)` |
| C# | `long Reembed(string sourceField)` |

The pure-Python backend raises `NotImplementedError`, matching how it already
refuses `auto_embed` — a missing attribute would read as a version mismatch
rather than a missing capability.

Every binding's `auto_embed` example and default `dims` moved to bge-small/384.
The documented `vector_indexes` widths moved with them: left at 1024 they would
have tripped the new dimension guard.

### The Python test suites were never running against the Rust backend

`moofile/_native` was a **tracked symlink** to `bindings/python/_native/`, a
directory package. Directories win over extension modules in Python's import
order, so that package permanently shadowed `moofile/_native.cpython-*.so` and
`import moofile` always fell through to the pure-Python implementation — in
this checkout, on every machine, for anyone running from source.

Both Python suites still reported PASS, because both backends pass most of the
same tests. What they were not doing was exercising the Rust backend at all.
With the shadow removed, two genuine failures surfaced immediately in
`tests-cross/test_autoembed_parity.py` — assertions still expecting the GGUF
error text.

The symlink was added incidentally in an unrelated caching commit and was never
the intended layout. The `__init__.py` inside its target explains the fallback
it causes, which suggests someone hit this, documented the symptom, and left
the cause in place. Refreshing the `.so` inside the symlink target — the
previous advice in CLAUDE.md — cannot work, since that `__init__.py`
deliberately imports nothing.

Check which backend you have with
`python -c "import moofile; print(moofile._NATIVE_LOADED)"`.

### Repo hygiene

- Deleted `core/Cargo.lock` and `bindings/python/Cargo.lock`. Cargo only reads
  the workspace root lockfile, so these two sat tracked and untouched at
  **0.4.1** since July — stale enough to predate autoembedding entirely, and
  misleading to anyone who opened them. Subdirectory lockfiles are now
  gitignored (`Cargo.lock` with `!/Cargo.lock`) so they cannot drift back.

### Other

- Embedding engines are keyed on the configured model id instead of a resolved
  filesystem path. The old key meant re-resolving the model URI on **every
  insert** — for `hf:` models, a filesystem cache probe per document. Invisible
  behind a 7-second embed; not invisible at 4 ms.
- `semantic()` no longer holds the collection's read lock across the embedding
  forward pass; it clones the config and engine handle and releases the lock
  first, as the hybrid path already did.
- `EmbeddingEngine::dims()` reports the model's real output width from
  fastembed's registry. It previously returned a hardcoded `1024` with a
  "will be refined" comment.
- Truncation, normalization and quantization were duplicated across the insert
  and query paths; both now share one `finalize_embedding` helper.

## v1.0.4 (2026-08-08)

### Build fixes

- Fixed `CString` pointer type mismatch in the C binding (`*mut i8` → `*mut u8` cast)

- **`Collection(..., auto_embed={...})` now works from Python.** The PyO3 constructor
  never grew the parameter, so on-device embedding was reachable from every binding
  *except* the one most users are on. It accepted exactly the keys the C ABI parses,
  so an `auto_embed` block is now portable verbatim between Python and the other
  bindings. `model_cache_dir` is exposed alongside it.
- Malformed config is rejected with a message naming the offender — an unknown key,
  an unknown `precision`, a missing `model`, or a non-dict value. Silently ignoring a
  misspelled `precision` would have left vectors at f32 and quadrupled stored size.
- Autoembedding failures (missing model file, no config for a source field, a build
  without the `embed` feature) now raise `MooFileError` from the Python adapter
  instead of a bare `RuntimeError`.
- The pure-Python backend accepts `auto_embed` and raises `NotImplementedError`,
  rather than `TypeError`, so a portable config block no longer looks like a typo.

### The pure-Python fallback announces itself

- Falling back to the pure-Python implementation now emits a `RuntimeWarning` naming
  the underlying import error. Same import, same class name, different feature set —
  the silence is how the autoembedding gap above survived several releases.
  `MOOFILE_PURE_PYTHON=1` silences it; `moofile._NATIVE_IMPORT_ERROR` holds the reason.
- Fixed `Collection.__del__` raising `AttributeError` during GC when the constructor
  failed before assigning its attributes, which buried the real construction error.

### Wheels: abi3, Linux ARM, and a working pure-Python floor

- **Wheels are now `abi3` (`abi3-py310`)** — one wheel per platform covers every
  CPython from 3.10 up. Previously each wheel was pinned to a single minor version
  and only the versions CI happened to build could install. On everything else pip
  did not fail, it resolved *backwards*: `pip install moofile` on macOS 3.12 or
  Windows 3.11 silently installed **0.2.1**, the last release from before the Rust
  core existed. Five of fifteen common platform/version combinations were getting
  1.0.2; nine were getting 0.2.1.
- **Linux ARM64 wheels are built again**, natively on `ubuntu-24.04-arm` rather than
  cross-compiled — cross-builds are what failed here before, and the embedding engine
  pulls a C/C++ toolchain through `llama-gguf`. This covers Docker on Apple Silicon
  and ARM servers.
- **The pure-Python wheel actually ships again.** Its CI job wrote the setuptools
  build backend to `pyproject.toml.bak` and never moved it into place, so it built
  through maturin and produced a native wheel that the publish step then deleted; no
  `py3-none-any` wheel has reached PyPI since 0.3.1. Replaced with
  `scripts/build_pure_wheel.py`, which swaps the backend, builds, and restores.
- **The release refuses to publish an incomplete artifact set.** A missing wheel does
  not break a release, it quietly narrows coverage — which is how the above went
  unnoticed. CI also asserts each wheel is abi3, installs it, and re-checks it on a
  newer interpreter than it was built against.
- Corrected the build instructions: `maturin build` must run from the repo root. Run
  inside `bindings/python/` it produces a wheel containing the compiled module and no
  `moofile/` package at all.

### Native libraries: Linux ARM64

- **`libmoofile` is built for `linux-aarch64`**, natively on `ubuntu-24.04-arm`, and
  ships in the release archives, the npm package (`native/linux-arm64/`) and the
  NuGet package (`runtimes/linux-arm64/`). C, C++, Node, Go, Java and C# all reach
  ARM Linux now — Docker on Apple Silicon and ARM servers included.
- The Node binding's `SUPPORTED_PLATFORMS` list advertised `darwin-x64` and
  `linux-arm64`, neither of which shipped. That list exists to turn an unsupported
  platform into a clear message instead of a dlopen error, so listing a platform that
  is not there produced exactly the confusing failure it is meant to prevent. It now
  matches what CI stages, and both packaging jobs fail if a binary is missing.

### Docs

- Corrected every claim that autoembedding works "transparently in all languages", and
  the platform tables: the native-library archives and the npm/NuGet packages carry
  Linux x64, macOS ARM64 and Windows x64 only, not the Intel macOS and Linux ARM builds
  they advertised. (Python wheels do now cover Linux ARM again — see above.)
- Added `llms.txt`, a compressed full-engine reference for coding agents.

## v1.0.2 (2026-08-07)

### Python native binding fixes

- Repaired PyO3/Python adapter parity for BSON-rich documents, including datetimes, binary values, ObjectIds, Code, and Decimal128.
- Validate document IDs and filters in the Python adapter before crossing the native boundary, preventing malformed inputs from poisoning a native collection lock.
- Restored consistent missing-field range-filter behavior for native full scans and indexed queries.

## v1.0.1 (2026-08-07)

### Minor refinements to language bindings

- **C#**: Added optional `Builders<T>.Filter` property-expression filters for the document-oriented API, including typed overloads across CRUD and search pre-filters.
- **Java**: Added MongoDB-style static `Filters` factories, a versioned binding JAR release artifact, and consumer guidance for `javac`, Maven/Gradle local dependencies, and fat-JAR deployments.
- **Go**: Added `Document`, `Filter`, `Update`, and `SearchOptions` conveniences with named update/search methods that avoid positional `nil` arguments.
- **C++**: Cursors now work in range-for loops; `Batch::commit()` now commits immediately and batch cleanup safely rolls back uncommitted work.
- **Node/TypeScript**: Removed the unsupported `$regex` filter operator from declarations.
- **Release tooling and docs**: Native release re-runs now skip already-published npm versions; Java JARs are included in release checksums; stale test counts and examples were refreshed.

## v1.2.0

## v1.0.0 (2026-08-07)

Moofile finally hits 1.0 with proper multi language bindings. These all work on my machine, but are in a 1.0 state and may have rapid fixes in the coming weeks. We've finally escaped Python world and can Moo everywhere!

- Stable release of the shared Rust core and Python, C, C++, Go, Node.js, and C# bindings.
- GitHub Actions publish Python wheels to PyPI, native libraries as release artifacts, and packages to npm and NuGet.
- Java remains available as a buildable binding and release artifact, but is not published to a package registry yet.


### Language bindings

- **Node.js, Go, Java and C# bindings are now functional.** As first written
  none of them could execute: the Node binding required `ffi-napi` (which no
  longer builds on Node 18+), the Java binding declared JNI natives against
  symbols `libmoofile` does not export, and the C# binding did not compile and
  applied its error check to the result pointer rather than the error pointer.
- **Node.js ported from `ffi-napi` to [koffi](https://koffi.dev)** — prebuilt,
  no node-gyp. Returned strings are now freed, the library handle is cached
  across collections, and cursors are iterable and self-freeing.
- **Java rewritten on the Foreign Function & Memory API** (JDK 22+). No
  third-party jars and no Maven or Gradle; `build.sh` is the whole toolchain.
  Adds a correct JSON parser — the previous comma-splitting one corrupted any
  document containing a vector, a nested object, or a comma inside a string.
- **C# rewritten** on a working P/Invoke layer with UTF-8 marshalling and a
  `NativeLibrary` resolver. Documents now hold plain CLR values instead of
  `JsonElement`, so `doc["age"]` compares as expected. Targets `net10.0`.
- **Go: `auto_embed` now reaches the core.** `AutoEmbedConfig` fields lacked
  JSON tags, so they serialised as `Model`/`Target` while the C layer reads
  `model`/`target` — semantic search could never be configured from Go. Also
  fixes a `C.CString` leak on every call and adds `-L`/rpath link flags.
- **C++ wrapper now compiles.** `insert`, `insert_many` and `stats` referenced
  an undeclared `err`, so the header was unusable; missing `<optional>` and
  `<cstdint>` includes added. `db.count({})` no longer trips over nlohmann
  decaying `{}` to null.

### C ABI

- **New `moofile_find_ex()`** exposes the full query builder — sort, skip,
  limit, group, agg — which had no C entry point, leaving every non-Python
  language unable to sort or paginate. Surfaced in all six bindings.
  Unrecognised option keys and aggregation names are errors, so a typo cannot
  silently return the whole collection.
- **Documented the ABI contract** in `moofile.h`: error conventions, string
  and cursor ownership, no-match semantics, and `dead_records` accounting.
- **Corrected the header's no-match documentation.** `update_one` and
  `replace_one` fail when nothing matches (matching Rust and Python, which
  raise `DocumentNotFound`); the header claimed they return 0.

### Build

- **The `embed` feature is now real.** `bindings/c/Cargo.toml` referenced
  `moofile-core/embed`, a feature that never existed — the line was a hard
  build error, and `llama-gguf` was an unconditional dependency, so
  autoembedding was always compiled in with no way to opt out. `moofile-core`
  now has a genuine `embed` feature, **on by default** and forwarded by both
  bindings. `--no-default-features` drops `llama-gguf` and ~300 transitive
  crates (libmoofile ~8.3 MB → ~2.8 MB); the config types remain and any
  attempt to embed returns the new `MooFileError::EmbedDisabled`. Only
  `core/src/embed.rs` needed `cfg` attributes: `EmbeddingEngine` becomes an
  uninhabited stub, so the rest of the crate stays gate-free.
- **`BUILDING.md`** — toolchain setup for all seven languages: one
  `apt install` for Ubuntu, plus Fedora, Arch, Alpine, macOS and best-effort
  Windows notes, version floors, and troubleshooting.
- **`scripts/test-all.sh`** — runs every suite and prints a summary, skipping
  languages whose toolchain is absent rather than failing.

### Core

- **Fixed `group()` in the Rust backend.** Group keys were stringified via
  `Bson::to_string()`, so a group on a string field produced `"\"eng\""` and a
  group on an integer produced text — diverging from the Python backend for
  every type. Keys now keep their original BSON value, and first-seen ordering
  matches Python.
- **`semantic()` exposed in the PyO3 binding**, which lacked it even though
  the core and C ABI both had it. The pure-Python backend raises
  `NotImplementedError` explaining that autoembedding needs the Rust engine.

### Tests

- Test suites for every binding: C 73, C++ 42, parity 8, Node 22, Go 22,
  Java 30, C# 30 — plus runnable examples for Node, Go, Java and C#.
- **`run_tests.sh` now runs the cross-backend parity suite**, and the C/C++
  suites build again (they were failing on missing includes and an
  nlohmann download path that did not match the `#include`).
- **Fixed the parity harness**, which was aborting with `free(): invalid size`:
  it declared C string returns as `c_char_p`, so ctypes discarded the real
  pointer and handed Python's own buffer to `moofile_free_string`. Its error
  slot was also a NULL `char**`, which disabled error reporting entirely, and
  its Python/Rust backends never actually opened a batch — making their
  rollback a silent no-op.

## v0.6.0 (2026-07-28)

- **Datetime parity**: BSON datetime round-tripping fixed in Rust backend (naive ↔ timezone-aware handled consistently)
- **Normalised BSON values**: Documents are now round-tripped through BSON encode/decode on write, ensuring reads always return the same Python types regardless of pickle cache validity (`Binary` → `bytes`, tz-aware → naive `datetime`)
- **`_id` enforcement**: Both backends now reject non-string `_id` at write time with `InvalidIdError` (Rust backend silently skipped them on replay, causing silent data loss)
- **Filter BSON normalisation**: Query filter values are now BSON-normalised before matching, fixing type mismatch bugs (e.g. tz-aware datetime in query vs naive datetime on disk)
- **`_check_id`**: Shared validation ensures Rust and Python reject the same documents
- **API guard tests**: Cross-backend parity tests for BSON types, filter operators, and edge cases
- **`update_one`/`replace_one`**: Now raise `DocumentNotFoundError` (was silently no-oping in some paths)

## v0.5.2 (2026-07-14)

- **Multi-process locking redesigned**: No lock held during normal operation — exclusive `flock` only acquired briefly during writes. Multiple processes (e.g. web UI + bot runners) can now open the same BSON file simultaneously for reads
- **`_catch_up` reconciliation**: Long-lived readers now detect and replay another process's suffix appends without a full reload
- **Compact safety**: Compaction re-acquires the exclusive lock for the full rewrite, preventing interleaved appends from other processes

## v0.5.1 (2026-07-13)

- **Fix `hybrid_search` type mismatch**: Python/Rust backends now return identical (doc, score) tuple shapes
- **Dead code removal**: Cleaned up vestigial pure-Python paths no longer reachable with the native backend active
- **API guard tests**: Added cross-implementation compatibility tests in `tests-cross/`

## v0.5.0 (2026-07-13)

- **Autoembedding**: Local GGUF embedding models via `auto_embed` parameter — no external API needed
- **`.semantic()`**: New query method for autoembedding-based semantic search
- **Model URIs**: `hf:user/repo:filename.gguf` (HuggingFace Hub auto-download), local file paths, absolute paths
- **Precision options**: `f32`, `int8`, `uint8`, `binary` with QAT-trained models
- **Hybrid + autoembed**: Pass `None` for `query_vector` in `hybrid_search()` to auto-embed the query text
- **Multiple auto-embed sources**: Different source fields can target different vector fields with different models/precisions
- **MRL truncation**: Multi-vector-dimension support via `dims` parameter

## v0.4.1 (2026-07-13)

- **Docs updated**: Coding-agent-friendly documentation with full API reference, gotchas, and edge cases

## v0.4.0 (2026-07-13)

- **Index caching**: Disposable `.bson.cache` file for sub-second cold opens — pickled snapshot of in-memory indexes validated against data file fingerprint (size + mtime + config)
- **Cache invalidation**: Automatically invalidated on data file change, version mismatch, or config change — safe to delete at any time
- **`reindex()`**: New method to rebuild indexes from scratch
- **Durability modes**: `"none"` (no flush), `"os"` (flush to page cache, default), `"fsync"` (sync to disk)
- **`sync()`**: Force an fsync for batched durability patterns
- **Compact fsync**: Compaction now fsyncs both the replacement file and its parent directory
- **`ReadOnlyError`**: Write attempts on read-only collections now raise a proper exception
- **`InvalidFilterError`**: Structured error for malformed query filters

## v0.3.5 (2026-07-11)

- **Python 3.13 & 3.14 wheels**: Linux CI now builds and uploads wheels for the latest Python versions

## v0.3.4 (2026-07-10)

- **GitHub Releases**: Automated release creation on tag push

## v0.3.3 (2026-07-10)

- **Wheel stripping**: Strip `linux_x86_64` wheels before PyPI upload to reduce package size

## v0.3.2 (2026-07-10)

- **Manylinux tag fix**: Corrected platform tag for PyPI compatibility

## v0.3.1 (2026-07-10)

- **macOS re-added**: Restored macos-latest to the build matrix after temporary drop
- **CI hardening**: Fixed various build matrix issues

## v0.3.0 (2026-07-09)

- **Rust native backend**: 20× performance improvement on key operations via PyO3 + maturin
- **Dual-engine architecture**: Auto-selects Rust native extension when available, falls back to pure Python
- **`_rust_adapter.py`**: Wraps `NativeCollection` to match the Python `Collection` API, enabling transparent backend switching
- **Cross-platform wheels**: CI builds for Linux (manylinux), macOS, and Windows via maturin
- **`core/` crate**: Standalone Rust library with BSON storage engine, query engine, BM25 text search, and vector search
- **`bindings/python/`**: Python bindings crate using PyO3
- **Vector search optimisations**: Cosine similarity via normalised dot product, numpy-free in Rust path

## v0.2.1 (2026-03-08)

- **Complete pyproject.toml**: Added metadata for PyPI publication (classifiers, keywords, license)

## v0.2.0 (2026-03-08)

- **BM25 text search**: Full-text search with Porter stemming via `text_indexes=["field"]` and `.text_search()`
- **Vector similarity search**: Cosine similarity via `.vector_search()` with numpy backend
- **Hybrid RRF search**: Reciprocal Rank Fusion combining BM25 + vector scores via `.hybrid_search()`
- **Document search example**: `examples/document_search.py` — end-to-end search with all three modes
- **`TextIndex` class**: In-memory inverted index with BM25 scoring, Porter stemming, and pickle support
- **`IndexManager` enhancements**: Vector index arrays, text index management, `rebuild_vector_indexes()`
- **`VectorQuery`, `TextQuery`, `HybridQuery`**: Dedicated result classes returning `[(doc, score)]` tuples
- **`$elemMatch` filter operator**: Array element matching for nested document conditions
- **`$exists` filter operator**: Check field presence/absence
- **Aggregation pipeline**: `group().agg()` with `count`, `sum`, `mean`, `min`, `max`, `collect`, `first`, `last`

## v0.1.0 (2026-03-06)

- **Initial release**: "Something I needed for a home project"
- **Core CRUD**: `insert`, `find`, `find_one`, `update_one`, `update_many`, `replace_one`, `delete_one`, `delete_many`
- **MongoDB-style query filters**: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$and`, `$or`, `$not`
- **Sorted indexes**: O(log n) field lookups via `sortedcontainers.SortedDict`
- **Append-only BSON storage**: Record types `LIVE` (0x01), `TOMBSTONE` (0x02), `REPLACEMENT` (0x03)
- **Compaction**: `compact()` rewrites live documents to a fresh file, atomically renamed
- **Stats**: `stats()` returns document count, dead records, file size, and dead ratio
- **CLI tools**: `moosh` (interactive REPL), `moo2json` (BSON ↔ JSON), `moo2mongo` (BSON ↔ MongoDB), `moo2sqlite` (BSON ↔ SQLite)
- **Context manager**: `with Collection(...) as db:` for auto-close
- **Examples**: `basic_crud.py`, `contacts_app.py`, `analytics.py`, `event_log.py`
