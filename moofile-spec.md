# MooFile — Specification v1.0.0

> A lightweight, embedded, single-file document store with vector similarity search, BM25 text search, **on-device autoembedding**, and a developer-friendly query API.  
> No server. No infrastructure. Just a file and a library.  
> **Now with a Rust core — 2-24× faster than pure Python.**  
> **And on-device embeddings — semantic search without external APIs.**

---

## Motivation

SQLite is magical because it is a library, not a server. You get persistence, indexing, and querying with zero infrastructure. The tradeoff is SQL — a relational query language that does not map naturally to how most application code thinks about data.

JSON files are the other extreme. Perfect ergonomics, zero infrastructure, but no indexes. Once your dataset grows past a few thousand documents, every query is a full scan.

MongoDB's query language (MQL) is closer to how developers naturally think — queries look like the documents they describe. But MongoDB requires a server, which is heavy for local or embedded use cases. Vector databases like Pinecone provide semantic search but require infrastructure and often sacrifice document flexibility.

MooFile sits in the gap:

- **Embedded** like SQLite — a library, no daemon, no network
- **Document-oriented** like MongoDB — JSON-shaped data, flexible schema
- **Vector search** like modern vector databases — cosine similarity, semantic search
- **Text search** like Elasticsearch — BM25 ranking, full-text indexing
- **On-device autoembedding** — run local GGUF embedding models, no external APIs needed
- **Developer-friendly API** — method chains, not operator dicts
- **Single-file portability** — your database is a file you can copy, version control, or email
- **Rust core** (v0.3.0) — 18-24× faster cold open, 10× faster insert, transparent fallback to pure Python

Target dataset size: **megabytes to single-digit gigabytes**.  
If you need horizontal scaling, distributed search, or sub-millisecond vector lookups, use specialized infrastructure.

---

## Non-Goals

- No network interface or server mode — ever
- No replication or clustering
- No cross-process transactions or snapshot isolation — concurrent access is SQLite-style (see [Multi-Process Access](#multi-process-access-v052)), not MVCC
- No multi-writer scaling — writes are serialized by a file lock; the design target is one writer with many readers
- No `$lookup` / joins — denormalize your data
- No SQL compatibility
- No full MongoDB MQL parity — implement the 80%, skip the 20% nobody uses
- No HNSW, IVF, or advanced vector indexing — brute-force is sufficient for target scale
- No advanced NLP — BM25 + Porter stemming covers most use cases
- No persistent indexes — correctness over startup performance

---

## File Layout

A MooFile database is three files:

```
mydata.bson       ← append-only document store, source of truth
mydata.bson.meta  ← index configuration (regular, vector, and text indexes)
mydata.bson.lock  ← advisory lock file (prevents concurrent multi-process access)
```

The `.lock` file is disposable — safe to delete, it will be re-created on next open. It exists only to detect and reject concurrent access from another process.

Indexes are **never persisted**. They are rebuilt in memory on every open by scanning the BSON file. This includes regular field indexes, vector indexes for similarity search, and inverted text indexes for BM25 ranking. The BSON file is always the source of truth. If the meta file is lost or corrupt, delete it and reopen — it will be rebuilt.

### Why No Persistent Indexes?

- Eliminates crash recovery complexity entirely
- Eliminates WAL complexity entirely  
- Simplifies the codebase dramatically
- For the target dataset size (MBs to low GBs), rebuild on open is fast enough
- Correctness is guaranteed — indexes can never be out of sync with data
- Applies to all index types: regular field indexes, vector arrays, and inverted text indexes
- Vector rebuilds are O(n) but acceptable for datasets under ~100K documents
- Text index rebuilds include tokenization and stemming but complete in seconds

### Disposable Index Snapshot Cache (v0.4.0)

While indexes are never persisted as a source of truth, an optional **disposable cache** can dramatically accelerate cold opens for large datasets.

On close, MooFile may write `mydata.bson.cache` — a binary snapshot of the in-memory indexes plus the data file's length and modification time. On the next open, if the cache's fingerprint matches the data file exactly, the pre-built indexes are loaded directly — skipping the BSON scan, decode, tokenisation, stemming, and vector normalisation that a cold rebuild requires.

**Validation (all must pass or the cache is silently ignored):**
1. Cache file exists and deserialises successfully
2. Magic bytes + format version match
3. Data file byte length matches (catches all append-only writes)
4. Data file modification time matches (catches compaction, external edits)
5. Index configuration matches (catches schema changes)

The recorded length and mtime are the state the index **actually describes** — the point this handle last reconciled to — not the file's state at the moment the cache is written. With concurrent writers the two differ, and recording the current state yields a cache that validates on the next open while omitting another process's records.

**Properties:**
- **Never a source of truth** — the BSON file is always the source of truth. The cache is a memoisation of the rebuild, keyed on file identity.
- **Safe to delete at any time** — a missing cache simply triggers a normal rebuild.
- **Can never be wrong** — any mismatch (modified file, corrupt cache, version change, schema change) triggers a full rebuild. The worst case is a cache miss, which is exactly the pre-cache behavior.
- **Zero crash-recovery logic** — if the cache write is interrupted, the partial cache file is rejected on next open (deserialisation fails → rebuild).
- **Format-specific** — a cache written by the Rust engine (bincode) is rejected by the Python engine (pickle) and vice versa. Cross-implementation portability is maintained through the BSON file, not the cache.

**Option B write strategy:** The cache is written on close only when needed:
- Loaded from cache, no writes → skip (cache is still valid)
- Rebuilt from scan (no cache existed) → write cache so next open is fast
- Writes occurred → write a fresh cache

This avoids redundant cache writes when the collection is opened read-only or opened from a valid cache without modifications.

### BSON File Format

The data file is **append-only**. Documents are never updated or deleted in place.

Each entry in the file is a fixed-header record followed by a BSON payload:

```
[4 bytes: record length] [1 byte: record type] [BSON payload]
```

Record types:
- `0x01` — live document
- `0x02` — tombstone (delete marker)
- `0x03` — replacement (update marker, contains new document)

On open, the file is scanned once from start to finish. The last record for any given `_id` wins. Tombstones remove a document from the in-memory index.

### Meta File Format

A small JSON file — human readable by design:

```json
{
  "version": 1,
  "indexes": ["email", "age", "status"],
  "vector_indexes": {"embedding": 384, "profile_vec": 128},
  "text_indexes": ["title", "content", "description"],
  "created_at": "2025-01-01T00:00:00Z"
}
```

Vector indexes specify field name and vector dimensionality. Text indexes list fields that support BM25 full-text search.

---

## Rust Core (v0.3.0)

MooFile now ships with an optional Rust-native engine. When available, `import moofile` transparently uses the Rust core. If the native extension can't be loaded (no prebuilt wheel for your platform, Rust not installed), MooFile falls back to the pure-Python implementation — same API, same file format, zero configuration.

### Architecture

```
import moofile
    │
    ├─ try: from moofile._native import NativeCollection   ← Rust via PyO3
    │       ✓ 18-24× faster cold open, 10× faster insert
    │
    └─ except ImportError:
           from moofile.collection import Collection        ← Pure Python
           ✓ Always works, no build required
```

Both implementations share the exact same file format, meta file schema, and BSON encoding. A database written by the Rust engine can be read by the pure-Python implementation and vice versa. The Python implementation serves as both a fallback and a reference for correctness.

### Source Layout

```
moofile/
├── core/                    # Rust library crate
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs           # Collection, CollectionBuilder, public API
│       ├── storage.rs       # Append-only BSON file I/O
│       ├── index.rs         # BTreeMap + vector + text indexes
│       ├── query.rs         # Query/VectorQuery/TextQuery, filter eval, .semantic()
│       ├── text.rs          # BM25 + Porter stemming (rust-stemmers)
│       ├── cache.rs         # Disposable index snapshot cache (bincode)
│       ├── embed.rs         # Autoembedding engine (llama-gguf wrapper, quantization)
│       └── errors.rs        # MooFileError enum
│
├── bindings/
│   ├── python/              # PyO3 binding (maturin build)
│   ├── c/                   # C API (extern "C") + C++ RAII wrapper
│   ├── node/                # Node.js via koffi FFI
│   ├── go/                  # Go via cgo
│   ├── java/                # Java via the Foreign Function & Memory API
│   └── csharp/              # C# via P/Invoke
│
├── moofile/                 # Python package (both impls)
│   ├── __init__.py          # Auto-detects Rust, falls back to Python
│   ├── _rust_adapter.py     # Adapter: Rust NativeCollection → Python API
│   ├── collection.py        # Pure-Python implementation (reference)
│   ├── storage.py
│   ├── index.py
│   ├── query.py
│   ├── operators.py
│   ├── aggregation.py
│   ├── text_search.py
│   ├── errors.py
│   └── cli/                 # moosh, moo2json, moo2mongo, moo2sqlite
│
├── tests/                   # Python test suite (both impls)
├── tests-cross/             # Cross-implementation validation tests
├── docs/README.md           # Full Python API reference
├── moofile-spec.md          # This file
└── pyproject.toml           # maturin build config
```

### Dependencies

| Dependency | Python impl | Rust impl | Purpose |
|---|---|---|---|
| `bson` (crate) / `pymongo` (py) | pymongo≥4.0 | bson 2.x | BSON encode/decode |
| `sortedcontainers` | sortedcontainers≥2.0 | — | Sorted indexes (Python only) |
| `numpy` | numpy≥1.20 | — | Vector ops (Python only) |
| `snowballstemmer` | snowballstemmer≥2.0 | — | Stemming (Python only) |
| `rust-stemmers` | — | rust-stemmers 1.2 | Porter stemming (Rust) |
| `serde` / `serde_json` | — | serde 1.x | Meta file JSON |
| `thiserror` | — | thiserror 2.x | Error derive macros |
| `regex-lite` | — | regex-lite 0.1 | Text tokenisation (Rust) |
| `getrandom` | — | getrandom 0.3 | Random `_id` bytes (Rust) |
| `hex` | — | hex 0.4 | `_id` hex encoding (Rust) |
| `bincode` | — | bincode 1.x | Index snapshot cache serialisation (Rust) |
| `fs4` | — | fs4 0.12 | Advisory file locking (Rust) |
| `rayon` | — | rayon 1.x | Parallel index rebuild (Rust) |
| `log` | — | log 0.4 | Structured logging (Rust) |
| `pandas` (opt) | pandas≥1.0 | — | `.to_df()` method |
| **`llama-gguf`** | — | **llama-gguf 0.14** | **On-device embedding model inference (Rust)** |
| **`dirs`** | — | **dirs 6.x** | **Model cache directory detection (Rust)** |

### Performance (10K docs, 128d vectors)

> These figures predate copy-on-read and BSON normalisation in the Python
> implementation, which previously returned live references to its internal
> documents and stored callers' objects verbatim. Its read and write rows are
> therefore not comparable to Rust's. Re-run `bench_native.py` before quoting.

| Operation | Python | Rust (pure) | Speedup |
|---|---|---|---|
| Cold open (scan + index rebuild) | 4,194 ms | 175 ms | **24×** |
| Insert 10K docs | 3,622 ms | 174 ms | **21×** |
| find_one indexed (2,000×) | 2.3 ms | 4.9 ms | 0.5× |
| Full scan (200×) | 304 ms | 81 ms | **3.7×** |
| Update (500×) | 1,202 ms | 215 ms | **5.6×** |
| Delete (200×) | 287 ms | 82 ms | **3.5×** |
| Vector search (50×) | 60 ms | 47 ms | 1.3× |
| Text search (50×) | 73 ms | 32 ms | 2.3× |

---

## Language Bindings (v1.0.0)

MooFile now ships with bindings for **C, C++, Node.js, Go, Java, and C#** — all
consuming the same C shared library (`libmoofile.so` / `.dylib` / `.dll`).

### Architecture

```
                    ┌─ Python (PyO3) ──── Rust core (direct FFI)
                    │
Language code ──→ JSON strings ──→ C shared library ──→ Rust core
                    │               (libmoofile.so)
                    ├─ C / C++ (direct C API)
                    ├─ Node.js (koffi FFI)
                    ├─ Go (cgo)
                    ├─ Java (FFM API)
                    └─ C# (P/Invoke)
```

Every binding passes documents and configuration as **JSON strings** across the
FFI boundary. The Rust core handles all parsing, BSON conversion, indexing, and
search — the binding is just a thin translation layer.

### What's shared

All bindings support the complete API surface:
- **CRUD**: insert, insert_many, find, find_one, count, exists
- **Update**: update_one, update_many, replace_one ($set, $unset, $inc)
- **Delete**: delete_one, delete_many
- **Search**: vector_search, text_search, hybrid_search (RRF), semantic
- **Batch**: atomic batch commit with rollback on error
- **Utility**: stats, compact, sync, reindex
- **Autoembedding**: on-device GGUF models, transparent to all languages

### Test coverage

| Binding | Tests | Platform |
|---------|:-----:|----------|
| C | 62 | Linux, macOS, Windows |
| C++ | 39 | Linux, macOS, Windows |
| Node.js | 18 | Linux, macOS, Windows |
| Go | 14 | Linux, macOS |
| Java | (source) | Linux, macOS, Windows |
| C# | 12 | Linux, macOS, Windows |

A cross-backend parity test suite (`bindings/c/tests/test_parity.py`) validates
identical results across Python (pure), Python (Rust native), and C (shared library)
for every major operation.

---

## Storage Engine

### Append-Only Writes

```
insert  → append type=0x01 record to BSON file
update  → append type=0x03 record (new document version)
delete  → append type=0x02 tombstone record
```

The file only ever grows. Old versions of documents remain in the file as dead bytes until compaction.

### Compaction

Compaction rewrites the BSON file keeping only the latest live version of each document.

```python
db.compact()  # explicit, never automatic
```

Rules:
- Never runs automatically — the developer decides when
- Writes to a `.tmp` file first, fsyncs it, then atomically renames
- fsyncs the parent directory so the rename is durable across power loss
- Safe to interrupt — if it fails, original file is untouched
- Recommended when dead space exceeds ~30% of file size (check via `db.stats()`)
- Reconciles with the file under the exclusive lock before computing the live set, and other handles detect the resulting inode change and reopen — see [Multi-Process Access](#multi-process-access-v052)

### In-Memory Indexes

On open, MooFile scans the BSON file and builds three types of in-memory indexes.

**Python implementation:**
```python
self._indexes = {
    "email": SortedDict(),   # value → {_id: None}  (insertion-ordered set)
}
self._vector_indexes = {
    "embedding": np.array(),  # [n_docs × vector_dim] matrix
}
self._text_indexes = {
    "content": { "terms": {}, "doc_lengths": {}, "idf": {} }
}
self._documents = {}         # _id → document_dict
```

**Rust implementation:**
```rust
documents: BTreeMap<String, Arc<Document>>          // _id → shared doc
regular: BTreeMap<String, BTreeMap<Value, BTreeSet<String>>> // field → value → ids
vector_data: BTreeMap<String, (Vec<String>, Vec<f32>, usize)> // (ids, matrix, dim)
text_indexes: BTreeMap<String, TextIndex>           // BM25 inverted indexes
```

Documents are stored as `Arc<Document>` — reference-counted to avoid deep copies during queries. Regular indexes use `BTreeMap` with `Bound`-based range queries for O(log n + k) lookups. Pure equality and pure range queries on a single indexed field return `IndexResult::Exact` — no secondary `matches()` filter pass needed.

**Posting lists are sets, not lists.** Removal from a list is an O(k) scan over every id sharing that value, which makes writes to a low-cardinality indexed field (a `status` with three values across the whole collection) O(n) each and bulk operations quadratic. Python uses a dict as an insertion-ordered set so result ordering is unchanged.

**Vector indexes are maintained incrementally.** `add` appends a normalised row; `remove` swap-removes one in place, keeping the id list and the flat matrix in lockstep. Marking the whole matrix stale on every write instead — which is what a "lazy rebuild" flag does — forces a full O(n·d) renormalise on the next search, so a single update in an interleaved insert/search workload costs as much as rebuilding the index. The stale flag survives only as a fallback for `clear()` and initial load.

**Ranked results are a total order.** Vector and text search break score ties on `_id`. Without a tie-break, equal-scoring documents come back in `HashMap` iteration order in Rust (randomised per process) and insertion order in Python, so results differ run to run, differ between backends, and reshuffle as rows move within the vector matrix.

**Documents handed to callers are copies.** The index stores documents by reference and `close()` serialises the index into the cache, so returning a live reference lets a caller's mutation corrupt the index, persist across a reopen, and then vanish when the cache is invalidated. Python copies after `skip`/`limit`, so paginated reads pay nothing and only unbounded scans pay the copy.

### No WAL

WAL is explicitly excluded. The tradeoff:

- A crash mid-write may corrupt the final record in the BSON file
- On open, scan to the last complete record and truncate any partial trailing write
- All prior records are intact — you lose at most the last in-flight write

A partial trailing record is truncated **only on open**, never during a mid-session catch-up. With multiple processes a short tail usually means another writer is mid-append, and truncating it would destroy their record; the reconciler stops before the partial record and picks it up next time.

### Durability Modes

The default `durability="os"` flushes writes to the OS page cache, which survives process crashes but **not** power loss. With the default, the "you lose at most the last in-flight write" claim applies to process crashes only — a power loss can lose any writes still in the page cache.

For applications that need power-loss durability, MooFile offers three durability levels:

| Mode | Behavior | Survives | Equivalent |
|---|---|---|---|
| `durability="none"` | No flush — data in userspace buffer | Nothing | SQLite `synchronous=OFF` |
| `durability="os"` (default) | `flush()` → OS page cache | Process crash | SQLite `synchronous=NORMAL` |
| `durability="fsync"` | `sync_all()` after every write | Power loss | SQLite `synchronous=FULL` |

For batched durability (best of both worlds), use the default and call `db.sync()` after a batch of writes:

```python
db = Collection("data.bson", durability="os")
for doc in docs:
    db.insert(doc)
db.sync()  # fsync once — all inserts are now durable
```

Compaction always fsyncs the temporary file and parent directory regardless of the durability setting, because it is a destructive rewrite of the entire file.

### Multi-Process Access (v0.5.2)

MooFile targets SQLite-style multi-process access: **typically one writer, possibly many readers**, all with the file open at once. This is the shape real deployments take — a persistent worker writing events while a web front end reads and occasionally writes configuration.

No lock is held for the lifetime of a handle. An exclusive `flock` on `mydata.bson.lock` is taken only for the duration of a write, and covers the *whole* operation — match, append, and index update — so a bulk `update_many` cannot interleave with another process's writes, and `insert_many` and batches take it once rather than per document.

**A handle's in-memory index is a private snapshot, not the source of truth.** Every operation that reads the index reconciles first:

| Data file vs. last-seen state | Action |
|---|---|
| Unchanged (same length, mtime, inode) | One `stat`, no work — the common path |
| Grew | Replay only the new suffix |
| Shrank, or inode changed | Full reload **and** reopen the storage handle |

Replaying only the suffix is possible because the format is append-only: another process's records are always a contiguous tail. Catching up costs O(new bytes), not O(file).

Three consequences are load-bearing. Changing any of them silently loses data:

1. **The index cache is stamped with the file state the index actually describes**, not the file's length at the moment of writing. With concurrent writers those differ, and stamping the current length produces a cache that passes validation on the next open while permanently omitting the other writer's records.
2. **`compact()` reconciles before computing its live set**, under the exclusive lock. It rewrites the file wholesale, so a stale snapshot erases records it never saw, irrecoverably.
3. **An inode change forces the storage handle to reopen.** `compact()` renames a new file over the path; every other handle's append descriptor then refers to the unlinked old inode, and everything it writes is lost. Detected via `st_ino` on Unix. On platforms without a usable inode only the shrink case is caught, so `compact()` alongside concurrent writers is less safe there.

**Guarantees.** No writes are lost, readers converge on writers, and duplicate `_id`s are detected across processes. **Not** guaranteed: cross-process transactions, or a stable read view within one operation. A reader can observe a writer's records at any point.

`ConcurrentAccessError` still exists in the exception hierarchy but is no longer raised on open.

---

## Document Identity

Every document has an `_id` field. Rules:

- If not provided on insert, MooFile generates a random 24-char hex string (12 random bytes, hex-encoded). Both implementations produce the same format.
- `_id` is always indexed automatically
- `_id` must be unique — inserting a duplicate raises `DuplicateKeyError`
- **`_id` must be a BSON string.** Any other type raises `InvalidIdError` (a subclass of `TypeError`). This is enforced because the Rust loader skips records whose `_id` is not a string when replaying the file: a non-string `_id` would be written to disk and then silently vanish on the next open.

---

## BSON Type Support

Documents may contain any type the BSON specification defines and pymongo can encode: `str`, `int`, `float`, `bool`, `None`, `list`, `dict`, `bytes`, `datetime`, `Binary`, `ObjectId`, `Decimal128`, `Timestamp`, `Regex`, `Code`, `Int64`, `SON`.

Both implementations encode through **pymongo's encoder**, so the bytes they write are identical to each other and to `bson.encode()`. The native binding converts `None`/`bool`/`int`/`float`/`str`/`list`/`dict` directly and delegates everything else; hand-rolling per-type conversion would drift from the Python implementation, which has no such layer.

Two consequences worth knowing:

- **Exact type checks matter.** Several BSON types subclass a Python builtin — `Code` is a `str`, `Int64` is an `int`, `Binary` is `bytes`, `SON` is a `dict`. A conversion that tests with `isinstance` silently downgrades them (a `Code` stored as a plain string). The fast paths test exact types; everything else falls through to the encoder.
- **Values a caller stores are BSON-normalised.** A read returns what is on disk, not the original Python object: a tz-aware `datetime` reads back as naive UTC, and a subtype-0 `Binary` reads back as `bytes` — the same values `bson.decode()` produces. Filter values are normalised the same way, so a tz-aware datetime in a query matches the stored form.

A type pymongo refuses to encode (a bare `uuid.UUID`, for instance) raises pymongo's own error, identically from both backends.

Range operators work on `datetime`, `Timestamp`, `ObjectId`, and `Binary` in addition to numbers, strings, and booleans, and datetimes and ObjectIds are indexable.

---

## Python API

Identical across both implementations. See the [README](README.md) for full examples.

### Opening

```python
from moofile import Collection

db = Collection("mydata.bson", 
    indexes=["email", "age", "status"],
    vector_indexes={"embedding": 384},
    text_indexes=["title", "content"])
```

On open: scans BSON file, builds all in-memory indexes, ready. Index build time: ~175ms (Rust) or ~4s (Python) for 10K documents.

### Method Chain API

```python
db.find({"status": "active"})
  .sort("age", descending=True)
  .skip(20).limit(10)
  .to_list()

db.find({"category": "research"})
  .vector_search("embedding", query_vector, limit=20)
  .to_list()  # → [(doc, similarity), ...]

db.find({"published": True})
  .text_search("content", "machine learning", limit=10)
  .to_list()  # → [(doc, relevance), ...]
```

### Hybrid Search (RRF)

Reciprocal Rank Fusion combines BM25 text search and vector cosine similarity by fusing their **rank positions** rather than their raw scores. This avoids the score-normalization problem (BM25 scores are unbounded and can be negative; cosine similarity is in [-1, 1]) and produces a single ranked list that benefits from both lexical and semantic matching.

```python
db.find({"category": "research"})
  .hybrid_search("content", "embedding", query_text, query_vector, limit=10)
  .to_list()  # → [(doc, rrf_score), ...]
```

**How it works:**

1. Pull a wider candidate pool (`max(limit × 5, 50)`) from each ranker independently.
2. For each document, compute `RRF_score = Σ 1/(k + rank + 1)` where `k = 60` (the canonical RRF constant) and `rank` is the document's 0-based position in each ranker's result list.
3. Sort by RRF score descending, truncate to `limit`.

A document that appears in both the text and vector result lists receives contributions from both, boosting it above documents that appear in only one. The pre-filter from `find({...})` is applied to both legs before fusion.

No other embedded single-file document store offers BM25 + cosine + RRF fusion behind a single method call.

### Semantic Search (Autoembedding, v0.5.0)

MooFile v0.5.0 introduces **on-device autoembedding** — run local embedding models (GGUF format) directly from the Rust core, with no external API calls. Models are auto-downloaded from HuggingFace on first use and cached in `~/.cache/moofile/models/`.

**Configuration:**

```python
from moofile import Collection

db = Collection("docs.bson",
    vector_indexes={"embedding": 1024},
    auto_embed={
        "content": {                              # source text field
            "model": "hf:jsonMartin/voyage-4-nano-gguf:voyage-4-nano-q8_0.gguf",
            #         ^^ HuggingFace URI scheme: hf:<repo>:<filename>
            "target": "embedding",                # target vector field
            "dims": 1024,                         # embedding dimensions
            "precision": "int8",                  # f32 | int8 | uint8 | binary
            "normalize": True,
            "query_prefix": "Represent the query for retrieving supporting documents: ",
            "doc_prefix": "Represent the document for retrieval: ",
        },
    })
```

**On insert/update:** if the document has a `"content"` field, MooFile automatically prefixes it with the `doc_prefix`, runs the embedding model, quantizes the result to `int8`, dequantizes to f32 for BSON storage, and populates the `"embedding"` field. This happens transparently — the caller just inserts their data.

**On query — `.semantic()`:** the query text is prefixed with `query_prefix`, embedded with the same model, and used for vector search:

```python
# Auto-embeds "quantum algorithms" and searches the "embedding" field
results = db.find({"year": 2025}).semantic("content", "quantum algorithms", 5).to_list()
```

**On hybrid search — auto-embedding:** pass `None` for `query_vector` to auto-embed from `query_text`:

```python
# The vector leg auto-embeds "quantum" from query_text using the configured model
results = db.find({}).hybrid_search("content", "content", "quantum", None, 10).to_list()
```

**Multiple auto-embed sources:** you can configure multiple source fields with different models or the same model:

```python
auto_embed={
    "abstract": {
        "model": "hf:jsonMartin/voyage-4-nano-gguf:voyage-4-nano-q8_0.gguf",
        "target": "embedding",
        "dims": 1024,
        "precision": "int8",
    },
    "title": {
        "model": "hf:jsonMartin/voyage-4-nano-gguf:voyage-4-nano-q8_0.gguf",
        "target": "title_vec",
        "dims": 256,                         # MRL truncation
        "precision": "binary",               # 128 bytes per embedding
    },
}
```

**Precision comparison:**

| Precision | Bytes per 1024-dim | Quality vs f32 |
|-----------|-------------------|----------------|
| `f32`     | 4,096 (4.0 KB)    | Baseline       |
| `int8`    | 1,024 (1.0 KB)    | ~1.0000 cosine |
| `uint8`   | 1,024 (1.0 KB)    | ~1.0000 cosine |
| `binary`  | 128 (128 B)       | ~0.9999 cosine |

All precisions use **Quantization-Aware Training (QAT)** — the model was trained to produce good results even after quantization. int8 and uint8 retain essentially perfect quality at 75% storage reduction. Binary retains usable quality at 96.9% storage reduction.

**Model URI scheme:**

```
hf:user/repo:filename.gguf  → HuggingFace Hub (auto-download + cache)
./local/model.gguf          → local file path
/absolute/path/model.gguf   → absolute path
```

Models are downloaded on first use via `llama-gguf`'s HuggingFace client, cached in `~/.cache/huggingface/hub/` (the standard HF cache), and loaded once per collection open. Subsequent opens are instant.

**Rust implementation:**

The autoembedding engine lives in `core/src/embed.rs`:

- `EmbeddingEngine` — wraps `llama_gguf::Engine` for text → vector
- `AutoEmbedConfig` — per-source-field configuration (model, target, precision, prefixes)
- `EmbeddingPrecision` — `F32 | Int8 | Uint8 | Binary`
- `ModelUri` — parses `hf:...` URIs and resolves to local paths via `HfClient`
- `quantize()` / `dequantize()` — conversion between f32 and quantized formats
- `cosine_similarity_quantized()` — compute cosine directly on quantized bytes (XOR + popcount for binary)

The engine is integrated into `CollectionInner` and invoked during `insert()`, `update_one/many()`, and `replace_one()`. For queries, `.semantic()` on `Query` and the updated `.hybrid_search()` (with `None` query_vector) trigger auto-embedding.

### Atomic Batch Writes

The `batch()` context manager buffers all write operations (insert, update, delete) and applies them atomically on commit — a single storage append, a single flush/fsync, and all index mutations applied together.

```python
with db.batch() as b:
    db.insert({"name": "alice", "status": "active"})
    db.update_one({"name": "bob"}, set={"status": "active"})
    db.delete_one({"name": "charlie"})
# All three operations committed atomically here.
```

**Properties:**

- **Transactional visibility**: reads within the batch see the pre-batch state. Buffered writes become visible only after commit.
- **Batched I/O**: all records are appended in a single write with one flush/fsync, regardless of durability mode. In `durability="fsync"` mode this reduces N fsyncs to 1.
- **Rollback on exception**: if the `with` block raises, the batch is discarded entirely — no records are appended and no indexes are mutated.
- **Crash semantics**: a crash mid-batch may commit a prefix of the batch (same as per-record semantics — you lose at most the last in-flight record). For true all-or-nothing crash atomicity, a future version may add commit markers.

Batch operations support the full write API: `insert`, `insert_many`, `update_one`, `update_many`, `replace_one`, `delete_one`, `delete_many`. Validation (duplicate `_id` detection, `DocumentNotFoundError`) happens eagerly at buffer time, so the caller gets immediate feedback.

### Update Operators

`$set`, `$unset`, `$inc` — covers 95% of real usage.

---

## Filter Operators

| Comparison | Logical | Element | Array |
|---|---|---|---|
| `$eq`, `$ne` | `$and` | `$exists` | `$elemMatch` |
| `$gt`, `$gte` | `$or` | | |
| `$lt`, `$lte` | `$not` | | |
| `$in`, `$nin` | | | |

That is the complete filter surface. No `$regex`, no `$where`, no geospatial.

Filters are validated at every entry point (`find`, `count`, and all mutations). An unknown operator, a `$and`/`$or` whose elements are not documents, or a `$not` that is not a document raises `InvalidFilterError` (a subclass of `ValueError`) rather than silently matching nothing. Filter *evaluation* itself is total and never raises — malformed input encountered mid-scan is treated as non-matching, because a panic inside the Rust engine would poison the collection's lock and disable the handle.

**A missing field satisfies no range operator.** `{"age": {"$gt": 10}}` never matches a document without an `age` field. This has to hold on both the index path and the scan path, or the same query returns different rows depending on whether the field happens to be indexed.

---

## Query Execution

**Indexed queries:** When the filter references a single indexed field with a pure equality or pure range condition, MooFile returns results directly from the index — no secondary `matches()` pass needed (`IndexResult::Exact`).

**Mixed queries:** When filter has multiple fields or mixed operators, MooFile uses the index for candidate pre-filtering then runs `matches()` on each candidate (`IndexResult::Candidates`).

**Full scan:** Falls back to scanning all documents when no index applies.

---

## Error Handling

```python
from moofile import (
    Collection,
    DuplicateKeyError,
    DocumentNotFoundError,
    MooFileError,
    ReadOnlyError,
    ConcurrentAccessError,
)
from moofile.errors import InvalidIdError, InvalidFilterError
```

| Exception | Base | Raised when |
|---|---|---|
| `DuplicateKeyError` | `MooFileError` | inserting an `_id` that already exists |
| `DocumentNotFoundError` | `MooFileError` | `update_one` / `replace_one` matches nothing |
| `ReadOnlyError` | `MooFileError` | writing to a read-only collection, or a closed one |
| `ConcurrentAccessError` | `MooFileError` | retained for compatibility; not raised since v0.5.2 |
| `InvalidIdError` | `MooFileError`, `TypeError` | `_id` is not a string |
| `InvalidFilterError` | `MooFileError`, `ValueError` | filter is structurally malformed |

The two newer exceptions subclass a builtin so ordinary `except TypeError` / `except ValueError` handling still catches them.

---

## Build & Distribution

### Source install (needs Rust)

```bash
pip install maturin
maturin develop --release   # from the repo root, not bindings/python
```

### Prebuilt wheels

One **abi3** wheel per platform, built via GitHub Actions CI. Because they target the
stable ABI, a single wheel covers every CPython from 3.10 up:
- Linux x86_64 (manylinux 2_17)
- Linux aarch64 (manylinux 2_17)
- macOS ARM64 (Apple Silicon)
- Windows x86_64

Fallback: pure-Python wheel (`moofile-x.y.z-py3-none-any.whl`) for platforms without prebuilt native wheels — musl/Alpine and Intel macOS. Importing it emits a `RuntimeWarning`, since it silently lacks autoembedding.

---

## Version History

| Version | Changes |
|---|---|
| 0.1.0 | Initial release — pure-Python, basic CRUD, sorted indexes |
| 0.2.0 | Vector similarity search (cosine), BM25 text search (Porter stemming), CLI tools |
| 0.3.0 | **Rust core** — PyO3 binding, 2-24× faster, Arc-backed documents |
| 0.4.0 | **Hybrid search (RRF)**. **Atomic batch writes**. **Disposable index snapshot cache** |
| 0.5.0 | **On-device autoembedding** — local GGUF models via `llama-gguf`. `.semantic()` method |
| 0.5.1 | `hybrid_search` type fix, dead code removal, API guard tests |
| 0.5.2 | **Concurrent multi-process access** — brief exclusive lock per write, not lifetime hold |
| 0.5.3 | Multi-process safety, full BSON type support, `_id` enforcement, performance fixes |
| **0.6.0** | **Language bindings** — C, C++, Node.js, Go, Java, C# all consuming the same C shared library. **BSON normalisation** — all documents round-tripped through BSON encode/decode on write. **`InvalidIdError`/`InvalidFilterError`** — structured errors for non-string `_id` and malformed filters. **Cross-backend parity tests** — 8 automated scenarios validated across Python (pure), Python (Rust native), and C (shared library) |

---

## Name

**MooFile**. It is a cow. Cows are not fast or scalable but they are reliable, friendly, and everyone likes them. This is that. (The Rust core makes the cow surprisingly quick, though.)
