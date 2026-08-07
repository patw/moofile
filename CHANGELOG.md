# Changelog

## Unreleased

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
