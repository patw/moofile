# Changelog

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
