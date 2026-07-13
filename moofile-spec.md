# MooFile — Specification v0.3.0

> A lightweight, embedded, single-file document store with vector similarity search, BM25 text search, and a developer-friendly query API.  
> No server. No infrastructure. Just a file and a library.  
> **Now with a Rust core — 2-24× faster than pure Python.**

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
- **Developer-friendly API** — method chains, not operator dicts
- **Single-file portability** — your database is a file you can copy, version control, or email
- **Rust core** (v0.3.0) — 18-24× faster cold open, 10× faster insert, transparent fallback to pure Python

Target dataset size: **megabytes to single-digit gigabytes**.  
If you need horizontal scaling, distributed search, or sub-millisecond vector lookups, use specialized infrastructure.

---

## Non-Goals

- No network interface or server mode — ever
- No replication or clustering
- No multi-process concurrent writes
- No `$lookup` / joins — denormalize your data
- No SQL compatibility
- No full MongoDB MQL parity — implement the 80%, skip the 20% nobody uses
- No HNSW, IVF, or advanced vector indexing — brute-force is sufficient for target scale
- No advanced NLP — BM25 + Porter stemming covers most use cases
- No persistent indexes — correctness over startup performance

---

## File Layout

A MooFile database is two files:

```
mydata.bson       ← append-only document store, source of truth
mydata.bson.meta  ← index configuration (regular, vector, and text indexes)
```

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
│       ├── query.rs         # Query/VectorQuery/TextQuery, filter eval
│       ├── text.rs          # BM25 + Porter stemming (rust-stemmers)
│       └── errors.rs        # MooFileError enum
│
├── bindings/python/         # PyO3 binding (maturin build)
│   ├── Cargo.toml
│   ├── pyproject.toml
│   └── src/
│       └── lib.rs           # NativeCollection PyO3 wrapper
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
| `pandas` (opt) | pandas≥1.0 | — | `.to_df()` method |

### Performance (10K docs, 128d vectors)

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
- Writes to a `.tmp` file first, then atomically renames
- Safe to interrupt — if it fails, original file is untouched
- Recommended when dead space exceeds ~30% of file size (check via `db.stats()`)

### In-Memory Indexes

On open, MooFile scans the BSON file and builds three types of in-memory indexes.

**Python implementation:**
```python
self._indexes = {
    "email": SortedDict(),   # value → [list of _ids]
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
regular: BTreeMap<String, BTreeMap<Value, Vec<String>>> // field → value → ids
vector_data: BTreeMap<String, (Vec<String>, Vec<f32>, usize)> // (ids, matrix, dim)
text_indexes: BTreeMap<String, TextIndex>           // BM25 inverted indexes
```

Documents are stored as `Arc<Document>` — reference-counted to avoid deep copies during queries. Regular indexes use `BTreeMap` with `Bound`-based range queries for O(log n + k) lookups. Pure equality and pure range queries on a single indexed field return `IndexResult::Exact` — no secondary `matches()` filter pass needed.

### No WAL

WAL is explicitly excluded. The tradeoff:

- A crash mid-write may corrupt the final record in the BSON file
- On open, scan to the last complete record and truncate any partial trailing write
- All prior records are intact — you lose at most the last in-flight write

---

## Document Identity

Every document has an `_id` field. Rules:

- If not provided on insert, MooFile generates a random 16-byte hex string (Rust) or 24-char hex string (Python). Both are valid.
- `_id` is always indexed automatically
- `_id` must be unique — inserting a duplicate raises `DuplicateKeyError`

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
)
```

---

## Build & Distribution

### Source install (needs Rust)

```bash
pip install maturin
cd bindings/python && maturin develop --release
```

### Prebuilt wheels

Platform wheels built via GitHub Actions CI for:
- Linux x86_64 (manylinux)
- macOS x86_64 + ARM64
- Windows x86_64

Fallback: pure-Python wheel (`moofile-x.y.z-py3-none-any.whl`) for platforms without prebuilt native wheels.

---

## Version History

| Version | Changes |
|---|---|
| 0.1.0 | Initial release — pure-Python, basic CRUD, sorted indexes |
| 0.2.0 | Vector similarity search (cosine), BM25 text search (Porter stemming), CLI tools |
| 0.3.0 | **Rust core** — PyO3 binding, 2-24× faster, Arc-backed documents, Exact/Candidates index result classification, Range lookup via BTreeMap Bound API, Cross-implementation test suite, Native wheel build pipeline |

---

## Name

**MooFile**. It is a cow. Cows are not fast or scalable but they are reliable, friendly, and everyone likes them. This is that. (The Rust core makes the cow surprisingly quick, though.)
